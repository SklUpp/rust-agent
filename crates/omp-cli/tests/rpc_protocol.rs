//! Consumer-level tests for the RPC transport: frame chunking round-trip,
//! message paging cursors, host URI scheme validation, stdin framing, plus
//! live-session integration tests that drive [`RpcMode`] against the real
//! `PiAgentRunner` and a localhost OpenAI-compatible SSE model: prompt
//! streaming, abort, queued follow-up, EOF drain and `abort_bash`.

use std::{
	fs,
	io::{BufRead, Read, Write},
	net::{TcpListener, TcpStream},
	path::{Path, PathBuf},
	sync::{
		Arc, Mutex,
		mpsc::{self, Receiver, Sender},
	},
	thread,
	time::{Duration, Instant},
};

use serde_json::{json, Value};

use omp_cli::{
	approve::ApprovalMode,
	catalog::{Catalog, ModelInfo},
	config::{Config, ProviderConfig},
	prompts::Prompts,
	skills::{LoadOptions, SkillSnapshot},
	rpc::{
		auth::LOGIN_IN_PROGRESS,
		frame::{MAX_RPC_FRAME_BYTES, RpcFrameDecoder, RpcFrameEncoder},
		host_uris::HostUriBridge,
		input::{Inbound, Output, RpcInput},
		messages::{MessageSnapshot, page_messages},
		mode::RpcMode,
		session::{FrameSink, RpcLaunch},
		types::RpcHostUriSchemeDefinition,
	},
	plan_yolo::PlanYoloArm,
	prewalk::PrewalkArm,
	runtime::{CliThinking, ModelSwitch, PiAgentRunner},
	session::{
		ContentPart, MessageContent, MessageEntry, SessionMessage, SessionOptions, SessionStore,
		fork_session,
	},
};

#[test]
fn v2_chunks_round_trip_through_the_decoder() {
	let big = json!({
		"type": "notice",
		"message": "y".repeat(MAX_RPC_FRAME_BYTES + 64),
	});
	let mut encoder = RpcFrameEncoder::new();
	encoder.set_protocol_version(2).expect("v2");
	let lines: Vec<String> = encoder.encode_frames(&big).collect();
	assert!(lines.len() > 1, "oversized frame must be chunked");

	let mut decoder = RpcFrameDecoder::default();
	let mut assembled = Vec::new();
	for line in &lines {
		let value: Value = serde_json::from_str(line).expect("chunk line is JSON");
		if let Some(frame) = decoder.push(value).expect("valid chunk sequence") {
			assembled.push(frame);
		}
	}
	assert_eq!(assembled, vec![big]);

	// A chunk sequence with a hole must be rejected, not silently reassembled.
	let mut decoder = RpcFrameDecoder::default();
	let first: Value = serde_json::from_str(&lines[0]).expect("chunk line is JSON");
	assert!(decoder.push(first).expect("first chunk accepted").is_none());
	let second: Value = serde_json::from_str(&lines[1]).expect("chunk line is JSON");
	let mut gap = second.clone();
	gap["index"] = json!(9);
	assert!(decoder.push(gap).is_err(), "out-of-order chunk must fail");
}

#[test]
fn message_paging_cursor_continues_and_rejects_a_changed_snapshot() {
	let messages: Vec<Value> = (0..5)
		.map(|index| json!({ "role": "user", "content": format!("m{index}") }))
		.collect();
	let snapshot = MessageSnapshot {
		session_id:    "s1".to_string(),
		leaf_id:       Some("leaf".to_string()),
		message_count: messages.len(),
	};
	let first = page_messages(&messages, &snapshot, (None, Some(2))).expect("first page");
	assert_eq!(first.messages.len(), 2);
	let cursor = first.next_cursor.clone().expect("cursor for the next page");

	let second = page_messages(&messages, &snapshot, (Some(&cursor), Some(2))).expect("second page");
	assert_eq!(second.messages.len(), 2);
	assert_ne!(first.messages, second.messages);

	// A snapshot that no longer matches the live list must fail closed.
	let stale = MessageSnapshot { message_count: 4, ..snapshot };
	assert!(page_messages(&messages, &stale, (Some(&cursor), Some(2))).is_err());
}

#[test]
fn reserved_host_uri_schemes_are_rejected_before_registration() {
	let sink: Arc<dyn FrameSink> = Arc::new(RecordingSink::default());
	let bridge = Arc::new(HostUriBridge::new(sink));
	let reserved = RpcHostUriSchemeDefinition {
		scheme:      "security".to_string(),
		description: None,
		writable:    false,
		immutable:   true,
	};
	let error = bridge.set_schemes(&[reserved]).expect_err("security:// is reserved");
	assert!(error.contains("reserved"), "{error}");

	let custom = RpcHostUriSchemeDefinition {
		scheme:      "notes".to_string(),
		description: Some("host notes".to_string()),
		writable:    true,
		immutable:   false,
	};
	let schemes = bridge.set_schemes(&[custom]).expect("custom scheme accepted");
	assert_eq!(schemes, vec!["notes".to_string()]);
}

#[test]
fn stdin_reader_skips_blank_lines_and_reports_garbage() {
	let mut input = RpcInput::new(std::io::Cursor::new(
		b"\n   \n{\"type\":\"get_state\",\"id\":\"1\"}\nnot json\n".to_vec(),
	));
	let first = input.next().expect("io").expect("line");
	match first {
		Ok(Inbound::Command(value)) => assert_eq!(value["type"], "get_state"),
		other => panic!("expected a command, got {other:?}"),
	}
	let second = input.next().expect("io").expect("line");
	match second {
		Ok(_) => panic!("garbage must not parse as a command"),
		Err(error) => assert!(error.starts_with("Failed to parse command"), "{error}"),
	}
	assert!(input.next().expect("io").is_none(), "EOF is reported");
}

#[test]
fn writer_keeps_frame_order_and_flips_to_v2_after_negotiation() {
	#[derive(Clone, Default)]
	struct Buffer(Arc<Mutex<Vec<u8>>>);

	impl std::io::Write for Buffer {
		fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
			self.0.lock().expect("buffer").extend_from_slice(data);
			Ok(data.len())
		}

		fn flush(&mut self) -> std::io::Result<()> {
			Ok(())
		}
	}

	let buffer = Buffer::default();
	let output = Output::spawn(buffer.clone());
	output.send(json!({ "type": "ready", "protocolVersion": 1 }));
	output.send(json!({
		"id": "1",
		"type": "response",
		"command": "negotiate_protocol",
		"success": true,
		"data": { "protocolVersion": 2 },
	}));
	output.send(json!({ "type": "notice", "message": "z".repeat(MAX_RPC_FRAME_BYTES + 8) }));
	assert_eq!(output.finish(), None);

	let text = String::from_utf8(buffer.0.lock().expect("buffer").clone()).expect("utf8");
	let lines: Vec<&str> = text.lines().collect();
	assert_eq!(lines[0], r#"{"type":"ready","protocolVersion":1}"#);
	assert!(lines[1].contains(r#""command":"negotiate_protocol""#));
	assert!(lines[2].contains(r#""type":"rpc_chunk""#));
	assert!(
		lines
			.iter()
			.all(|line| line.len() < MAX_RPC_FRAME_BYTES),
		"no line may exceed the advertised frame bound"
	);
}

#[derive(Default)]
struct RecordingSink(Mutex<Vec<Value>>);

impl FrameSink for RecordingSink {
	fn send(&self, frame: Value) {
		self.0.lock().expect("sink").push(frame);
	}
}

// ------------------------------------------------------------ live RPC mode --

type SharedBytes = Arc<Mutex<Vec<u8>>>;

struct SharedWriter(SharedBytes);

impl Write for SharedWriter {
	fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
		self.0.lock().expect("writer").extend_from_slice(data);
		Ok(data.len())
	}

	fn flush(&mut self) -> std::io::Result<()> {
		Ok(())
	}
}

/// Stdin substitute: blocks for the next chunk and reports EOF when the test
/// drops its sender.
struct ChannelReader {
	rx:  Receiver<Vec<u8>>,
	buf: Vec<u8>,
	pos: usize,
	eof: bool,
}

impl ChannelReader {
	fn new(rx: Receiver<Vec<u8>>) -> Self {
		Self { rx, buf: Vec::new(), pos: 0, eof: false }
	}
}

impl Read for ChannelReader {
	fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
		let available = self.fill_buf()?;
		let len = available.len().min(out.len());
		out[..len].copy_from_slice(&available[..len]);
		self.consume(len);
		Ok(len)
	}
}

impl BufRead for ChannelReader {
	fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
		while self.pos >= self.buf.len() {
			if self.eof {
				return Ok(&[]);
			}
			match self.rx.recv() {
				Ok(chunk) => {
					self.buf = chunk;
					self.pos = 0;
				},
				Err(_) => {
					self.eof = true;
					return Ok(&[]);
				},
			}
		}
		Ok(&self.buf[self.pos..])
	}

	fn consume(&mut self, amount: usize) {
		self.pos += amount;
	}
}

fn parse_frames(bytes: &[u8]) -> Vec<Value> {
	let text = String::from_utf8_lossy(bytes);
	let Some(last_newline) = text.rfind('\n') else {
		return Vec::new();
	};
	text[..=last_newline]
		.lines()
		.filter(|line| !line.trim().is_empty())
		.filter_map(|line| serde_json::from_str(line).ok())
		.collect()
}

struct Harness {
	tx:       Mutex<Option<Sender<Vec<u8>>>>,
	out:      SharedBytes,
	handle:   Option<thread::JoinHandle<omp_cli::Result<()>>>,
	/// Per-test credential store; removed on drop so the suite leaves no
	/// `agent.db` files behind.
	agent_db: std::path::PathBuf,
}

impl Harness {
	fn start(launch: RpcLaunch) -> Self {
		let (tx, rx) = mpsc::channel();
		let out = Arc::new(Mutex::new(Vec::new()));
		let reader = ChannelReader::new(rx);
		let writer = SharedWriter(Arc::clone(&out));
		let agent_db = launch.agent_db.clone();
		let handle = thread::Builder::new()
			.name("rpc-test-mode".to_string())
			.spawn(move || RpcMode::run(launch, reader, writer))
			.expect("spawn rpc mode");
		Self { tx: Mutex::new(Some(tx)), out, handle: Some(handle), agent_db }
	}

	fn send(&self, frame: Value) {
		let guard = self.tx.lock().expect("tx");
		guard
			.as_ref()
			.expect("stdin still open")
			.send(format!("{frame}\n").into_bytes())
			.expect("send frame");
	}

	fn close_stdin(&self) {
		self.tx.lock().expect("tx").take();
	}

	fn frames(&self) -> Vec<Value> {
		parse_frames(&self.out.lock().expect("out"))
	}

	fn wait_for(&self, mut done: impl FnMut(&[Value]) -> bool, timeout: Duration) -> Vec<Value> {
		let deadline = Instant::now() + timeout;
		loop {
			let frames = self.frames();
			if done(&frames) {
				return frames;
			}
			assert!(
				Instant::now() < deadline,
				"timed out waiting for RPC frame; got {frames:#?}"
			);
			thread::sleep(Duration::from_millis(10));
		}
	}

	fn wait_frame(&self, kind: &str, timeout: Duration) -> Vec<Value> {
		self.wait_for(
			|frames| frames.iter().any(|frame| frame["type"] == kind),
			timeout,
		)
	}

	fn join(mut self) -> omp_cli::Result<()> {
		self.close_stdin();
		self
			.handle
			.take()
			.expect("handle")
			.join()
			.expect("rpc mode thread")
	}
}

impl Drop for Harness {
	fn drop(&mut self) {
		self.close_stdin();
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}
		// SQLite keeps `-wal`/`-shm` beside the file; drop the whole triple.
		for suffix in ["", "-wal", "-shm"] {
			let path = if suffix.is_empty() {
				self.agent_db.clone()
			} else {
				let mut path = self.agent_db.clone().into_os_string();
				path.push(suffix);
				std::path::PathBuf::from(path)
			};
			let _ = std::fs::remove_file(path);
		}
	}
}

/// Wait until the streamed turn is journaled as `aborted`.
///
/// `abort_and_prompt` retires the live turn's token inside the run lock and only
/// then writes its response, so this frame proves the old turn can no longer
/// complete with `stop`. Releasing a paused model script before it is a race the
/// fixture — not the mode — decides, which is exactly what made the replacement
/// test flaky.
fn wait_aborted(harness: &Harness) {
	harness.wait_for(
		|frames| {
			frames.iter().any(|frame| {
				frame["type"] == "message_end" && frame["message"]["stopReason"] == "aborted"
			})
		},
		timeout(),
	);
}

struct StreamScript {
	text:  String,
	/// When present the server holds the response open until the test signals.
	pause: Option<Receiver<()>>,
}

fn spawn_model(scripts: Vec<StreamScript>) -> u16 {
	spawn_model_recording(scripts).0
}

/// [`spawn_model`] plus the user prompt of every request the mock served.
///
/// The scripted answer text is positional, so it cannot identify the prompt that
/// asked for it: a test asserting *which* prompt reached the provider has to read
/// the request.
fn spawn_model_recording(scripts: Vec<StreamScript>) -> (u16, Arc<Mutex<Vec<String>>>) {
	let prompts: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
	let recorded = Arc::clone(&prompts);
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind model");
	let port = listener.local_addr().expect("addr").port();
	thread::spawn(move || {
		listener
			.set_nonblocking(false)
			.expect("blocking listener");
		for script in scripts {
			let Ok((mut stream, _)) = listener.accept() else {
				return;
			};
			stream
				.set_read_timeout(Some(Duration::from_secs(5)))
				.expect("read timeout");
			let Some(request) = read_request(&mut stream) else {
				continue;
			};
			if let Some(prompt) = user_prompt(&request) {
				recorded
					.lock()
					.unwrap_or_else(std::sync::PoisonError::into_inner)
					.push(prompt);
			}
			let body = serde_json::to_string(&script.text).expect("json text");
			let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
			            Cache-Control: no-cache\r\nConnection: close\r\n\r\n";
			let _ = stream.write_all(head.as_bytes());
			let _ = stream.write_all(
				format!(
					"data: {{\"id\":\"chatcmpl-test\",\"object\":\"chat.completion.chunk\",\
					 \"choices\":[{{\"index\":0,\"delta\":{{\"content\":{body}}},\
					 \"finish_reason\":null}}]}}\n\n"
				)
				.as_bytes(),
			);
			let _ = stream.flush();
			if let Some(pause) = script.pause {
				let _ = pause.recv_timeout(Duration::from_secs(5));
			}
			let _ = stream.write_all(b"data: [DONE]\n\n");
			let _ = stream.flush();
		}
	});
	(port, prompts)
}

/// One scripted assistant response that may also call a tool.
struct ToolScript {
	text: String,
	tool: Option<(&'static str, Value)>,
}

/// [`spawn_model`] variant that emits one tool call and records every request
/// body, so a test can assert exactly which conversation the provider saw.
fn spawn_tool_model(scripts: Vec<ToolScript>) -> (u16, Arc<Mutex<Vec<Value>>>) {
	let (port, requests, _authorizations) = spawn_tool_model_with_auth(scripts);
	(port, requests)
}

/// [`spawn_tool_model`] plus each request's `Authorization` header.
fn spawn_tool_model_with_auth(
	scripts: Vec<ToolScript>,
) -> (
	u16,
	Arc<Mutex<Vec<Value>>>,
	Arc<Mutex<Vec<Option<String>>>>,
) {
	let requests: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
	let recorded = Arc::clone(&requests);
	let authorizations: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
	let auths = Arc::clone(&authorizations);
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind model");
	let port = listener.local_addr().expect("addr").port();
	thread::spawn(move || {
		for (index, script) in scripts.into_iter().enumerate() {
			let Ok((mut stream, _)) = listener.accept() else {
				return;
			};
			stream
				.set_read_timeout(Some(Duration::from_secs(5)))
				.expect("read timeout");
			let Some(request) = read_request(&mut stream) else {
				continue;
			};
			let body_start = request
				.windows(4)
				.position(|window| window == b"\r\n\r\n")
				.map_or(0, |index| index + 4);
			if let Ok(value) = serde_json::from_slice::<Value>(&request[body_start..]) {
				recorded
					.lock()
					.unwrap_or_else(std::sync::PoisonError::into_inner)
					.push(value);
				let head = String::from_utf8_lossy(&request[..body_start]);
				let authorization = head.lines().find_map(|line| {
					let (name, value) = line.split_once(':')?;
					name.eq_ignore_ascii_case("authorization")
						.then(|| value.trim().to_string())
				});
				auths
					.lock()
					.unwrap_or_else(std::sync::PoisonError::into_inner)
					.push(authorization);
			}
			let _ = stream.write_all(
				b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
				  Cache-Control: no-cache\r\nConnection: close\r\n\r\n",
			);
			if !script.text.is_empty() {
				let body = serde_json::to_string(&script.text).expect("json text");
				let _ = stream.write_all(
					format!(
						"data: {{\"id\":\"chatcmpl-test\",\"object\":\"chat.completion.chunk\",\
						 \"choices\":[{{\"index\":0,\"delta\":{{\"content\":{body}}},\
						 \"finish_reason\":null}}]}}\n\n"
					)
					.as_bytes(),
				);
			}
			if let Some((name, arguments)) = script.tool {
				let chunk = json!({
					"id": "chatcmpl-test",
					"object": "chat.completion.chunk",
					"choices": [{
						"index": 0,
						"delta": {
							"tool_calls": [{
								"index": 0,
								"id": format!("call-{index}"),
								"type": "function",
								"function": {
									"name": name,
									"arguments": arguments.to_string(),
								}
							}]
						},
						"finish_reason": null
					}]
				});
				let _ = stream.write_all(format!("data: {chunk}\n\n").as_bytes());
			}
			let _ = stream.write_all(b"data: [DONE]\n\n");
			let _ = stream.flush();
		}
	});
	(port, requests, authorizations)
}

/// Text of the last `user` message in an OpenAI-compatible request body.
fn user_prompt(request: &[u8]) -> Option<String> {
	let body_start = request.windows(4).position(|window| window == b"\r\n\r\n")? + 4;
	let value: Value = serde_json::from_slice(&request[body_start..]).ok()?;
	let messages = value.get("messages")?.as_array()?;
	let last = messages
		.iter()
		.rev()
		.find(|message| message.get("role").and_then(Value::as_str) == Some("user"))?;
	let content = last.get("content")?;
	if let Some(text) = content.as_str() {
		return Some(text.to_string());
	}
	Some(
		content
			.as_array()?
			.iter()
			.filter_map(|part| part.get("text").and_then(Value::as_str))
			.collect::<Vec<_>>()
			.join("\n"),
	)
}

/// Read headers and body so the client never sees a reset mid-request.
fn read_request(stream: &mut TcpStream) -> Option<Vec<u8>> {
	let mut buffer = Vec::new();
	let mut chunk = [0u8; 4096];
	loop {
		let read = stream.read(&mut chunk).ok()?;
		if read == 0 {
			return None;
		}
		buffer.extend_from_slice(&chunk[..read]);
		let Some(head_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") else {
			if buffer.len() > 64 * 1024 {
				return None;
			}
			continue;
		};
		let head = String::from_utf8_lossy(&buffer[..head_end]).to_ascii_lowercase();
		let content_length = head
			.lines()
			.find_map(|line| line.strip_prefix("content-length:"))
			.and_then(|value| value.trim().parse::<usize>().ok())
			.unwrap_or(0);
		let body_start = head_end + 4;
		while buffer.len() < body_start + content_length {
			let read = stream.read(&mut chunk).ok()?;
			if read == 0 {
				break;
			}
			buffer.extend_from_slice(&chunk[..read]);
		}
		return Some(buffer);
	}
}

/// Per-test credential store path, unique per launch so parallel tests never
/// share one `agent.db`.
fn agent_db_path() -> std::path::PathBuf {
	static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
	let next = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	std::env::temp_dir().join(format!("omp-rpc-agent-{}-{next}.db", std::process::id()))
}

fn effort_catalog() -> Catalog {
	let mut catalog = Catalog::default();
	let mut models = std::collections::BTreeMap::new();
	models.insert(
		"model-a".to_string(),
		ModelInfo {
			provider: "kimi-code".to_string(),
			reasoning: true,
			efforts: Some(vec!["low".to_string(), "high".to_string()]),
			thinking: Some(json!({
				"efforts": ["low", "high"],
				"defaultLevel": "high"
			})),
			..ModelInfo::default()
		},
	);
	catalog.providers.insert("kimi-code".to_string(), models);
	catalog
}

fn launch(port: u16, model: &str, provider: &str) -> RpcLaunch {
	let mut config = Config::default();
	config.providers.insert(
		provider.to_string(),
		ProviderConfig {
			base_url: Some(format!("http://127.0.0.1:{port}/v1")),
			api_key:  Some("sk-test".to_string()),
			..Default::default()
		},
	);
	let cwd = std::env::temp_dir();
	RpcLaunch {
		runner: Arc::new(PiAgentRunner::new().expect("runner")),
		config,
		cwd: cwd.clone(),
		prompts: Prompts::default(),
		skill_snapshot: SkillSnapshot::new(),
		skill_options: LoadOptions::new(cwd.clone()),
		catalog: Arc::new(Catalog::default()),
		agent_db: agent_db_path(),
		service_tier_explicit: false,
		approval_mode: ApprovalMode::Yolo,
		allow: Vec::new(),
		model: Some(model.to_string()),
		provider: Some(provider.to_string()),
		api_key: Some("sk-test".to_string()),
		thinking: None,
		restore_model: false,
		prewalk: None,
		plan_yolo: None,
		max_time: Some(Duration::from_secs(10)),
		max_turns: 2,
		tools_enabled: false,
		prompt_cache_key: None,
		prompt_cache_key_inherited: false,
		provider_session_id: None,
		store_options: SessionOptions {
			enabled: false,
			cwd,
			model: None,
			provider: None,
			dir: None,
			id: None,
			continue_latest: false,
			parent_session: None,
			no_title: true,
		},
		steer: None,
	}
}

fn timeout() -> Duration {
	Duration::from_secs(10)
}

/// Unique per-test root so parallel checkpoint scenarios never share a log.
fn checkpoint_root(label: &str) -> PathBuf {
	static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
	let next = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	let root = std::env::temp_dir().join(format!(
		"omp-rpc-checkpoint-{}-{label}-{next}",
		std::process::id()
	));
	fs::create_dir_all(root.join("worktree")).expect("worktree dir");
	root
}

fn checkpoint_session_options(worktree: &Path, dir: &Path, id: Option<&str>) -> SessionOptions {
	SessionOptions {
		enabled: true,
		cwd: worktree.to_path_buf(),
		model: Some("local/mock-model".to_string()),
		provider: Some("local".to_string()),
		dir: Some(dir.to_path_buf()),
		id: id.map(str::to_string),
		continue_latest: id.is_none(),
		parent_session: None,
		no_title: true,
	}
}

/// [`launch`] with the checkpoint pair mounted and a persisted session log.
fn launch_checkpoint(
	port: u16,
	worktree: &Path,
	session_dir: &Path,
	id: Option<&str>,
) -> RpcLaunch {
	let mut launch = launch(port, "local/mock-model", "local");
	launch.cwd = worktree.to_path_buf();
	launch.config.checkpoint.enabled = Some(true);
	launch.tools_enabled = true;
	launch.max_turns = 8;
	launch.store_options = checkpoint_session_options(worktree, session_dir, id);
	launch
}

/// [`launch`] with the checkpoint pair mounted and no session log: the whole
/// conversation lives in the agent mirror, so rewinds cut it in memory.
fn launch_checkpoint_in_memory(port: u16) -> RpcLaunch {
	let mut launch = launch(port, "local/mock-model", "local");
	launch.config.checkpoint.enabled = Some(true);
	launch.tools_enabled = true;
	launch.max_turns = 8;
	launch
}

/// Only session log in `dir`; fails loudly when a scenario wrote none or two.
fn session_log(dir: &Path) -> PathBuf {
	let mut logs: Vec<PathBuf> = fs::read_dir(dir)
		.expect("session dir")
		.filter_map(|entry| entry.ok().map(|entry| entry.path()))
		.filter(|path| {
			path.extension().and_then(|extension| extension.to_str()) == Some("jsonl")
		})
		.collect();
	assert_eq!(logs.len(), 1, "expected one session log in {}", dir.display());
	logs.pop().expect("session log")
}

/// The exact scenario the parent asked for, run through `RpcMode`:
/// checkpoint tool call, an exploratory assistant turn, then `rewind`.
///
/// Returns the persisted log, every provider request body in order, and the
/// RPC frames emitted for the run.
fn run_checkpoint_rewind(root: &Path) -> (PathBuf, Vec<Value>, Vec<Value>) {
	let worktree = root.join("worktree");
	let sessions = root.join("sessions");
	let (port, requests) = spawn_tool_model(vec![
		ToolScript {
			text: String::new(),
			tool: Some(("checkpoint", json!({ "goal": "map it" }))),
		},
		ToolScript { text: "EXPLORATORY-SECRET".to_string(), tool: None },
		ToolScript {
			text: String::new(),
			tool: Some(("rewind", json!({ "report": "CONCISE-FINDINGS" }))),
		},
		ToolScript { text: "FINAL-ANSWER".to_string(), tool: None },
	]);
	let harness = Harness::start(launch_checkpoint(port, &worktree, &sessions, Some("cp-smoke")));
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "start investigation" }));
	let frames = harness.wait_frame("agent_end", timeout());
	let bodies = requests
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	let log = session_log(&sessions);
	drop(harness);
	(log, bodies, frames)
}

#[test]
fn prompt_streams_events_and_agent_end_matches_messages() {
	let port = spawn_model(vec![StreamScript { text: "hello".to_string(), pause: None }]);
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "hi" }));
	let frames = harness.wait_frame("agent_end", timeout());

	let index = |kind: &str| {
		frames
			.iter()
			.position(|frame| frame["type"] == kind)
			.unwrap_or_else(|| panic!("missing {kind}: {frames:#?}"))
	};
	assert!(index("turn_start") < index("message_start"));
	assert!(index("message_start") < index("message_update"));
	assert!(index("message_update") < index("message_end"));
	assert!(index("message_end") < index("turn_end"));
	assert!(index("turn_end") < index("agent_end"));
	assert_eq!(frames[0]["type"], "ready");
	assert!(index("agent_start") < index("turn_start"));

	let message_end = &frames[index("message_end")];
	assert_eq!(message_end["message"]["stopReason"], "stop");
	let update = &frames[index("message_update")];
	assert_eq!(update["assistantMessageEvent"]["delta"], "hello");

	let agent_end = &frames[index("agent_end")];
	assert_eq!(agent_end["willContinue"], false);
	let messages = agent_end["messages"].as_array().expect("messages").clone();
	assert_eq!(messages.len(), 2);
	assert_eq!(messages[0]["role"], "user");

	let prompt_response = frames
		.iter()
		.find(|frame| frame["id"] == "p1")
		.expect("prompt response");
	assert_eq!(prompt_response["success"], true);
	assert_eq!(prompt_response["command"], "prompt");

	harness.send(json!({ "type": "get_messages", "id": "m1" }));
	let frames = harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "m1"),
		timeout(),
	);
	let response = frames.iter().find(|frame| frame["id"] == "m1").expect("get_messages");
	assert_eq!(response["data"]["messages"], Value::Array(messages));
}

#[test]
fn abort_mid_stream_marks_message_aborted_and_clears_streaming() {
	let (pause_tx, pause_rx) = mpsc::channel();
	let port = spawn_model(vec![StreamScript {
		text:  "partial".to_string(),
		pause: Some(pause_rx),
	}]);
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "hi" }));
	harness.wait_frame("message_update", timeout());
	harness.send(json!({ "type": "abort", "id": "a1" }));
	let frames = harness.wait_frame("agent_end", timeout());
	let aborted = frames
		.iter()
		.find(|frame| {
			frame["type"] == "message_end" && frame["message"]["stopReason"] == "aborted"
		})
		.unwrap_or_else(|| panic!("no aborted message_end: {frames:#?}"));
	assert_eq!(aborted["message"]["role"], "assistant");

	harness.send(json!({ "type": "get_state", "id": "s1" }));
	let frames = harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "s1"),
		timeout(),
	);
	let state = frames.iter().find(|frame| frame["id"] == "s1").expect("state");
	assert_eq!(state["data"]["isStreaming"], false);
	drop(pause_tx);
}

#[test]
fn follow_up_queued_while_streaming_runs_in_the_same_agent_run() {
	let (pause_tx, pause_rx) = mpsc::channel();
	let port = spawn_model(vec![
		StreamScript { text: "first".to_string(), pause: Some(pause_rx) },
		StreamScript { text: "second".to_string(), pause: None },
	]);
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "one" }));
	harness.wait_frame("message_update", timeout());
	harness.send(json!({ "type": "follow_up", "id": "f1", "message": "two" }));
	pause_tx.send(()).expect("release first response");
	let frames = harness.wait_frame("agent_end", timeout());

	let message_ends: Vec<&Value> = frames
		.iter()
		.filter(|frame| frame["type"] == "message_end")
		.collect();
	assert_eq!(message_ends.len(), 2, "{frames:#?}");
	assert_eq!(message_ends[0]["message"]["stopReason"], "stop");
	assert_eq!(message_ends[1]["message"]["stopReason"], "stop");
	let updates: Vec<String> = frames
		.iter()
		.filter(|frame| frame["type"] == "message_update")
		.map(|frame| frame["assistantMessageEvent"]["delta"].as_str().unwrap_or("").to_string())
		.collect();
	assert!(updates.iter().any(|delta| delta == "first"));
	assert!(updates.iter().any(|delta| delta == "second"));
	assert_eq!(
		frames.iter().filter(|frame| frame["type"] == "agent_start").count(),
		1
	);
	assert_eq!(
		frames.iter().filter(|frame| frame["type"] == "agent_end").count(),
		1
	);
	let follow_up = frames.iter().find(|frame| frame["id"] == "f1").expect("follow_up response");
	assert_eq!(follow_up["success"], true);
}

#[test]
fn abort_and_prompt_while_streaming_runs_the_replacement_as_a_new_turn() {
	let (pause_tx, pause_rx) = mpsc::channel();
	let port = spawn_model(vec![
		StreamScript { text: "old".to_string(), pause: Some(pause_rx) },
		StreamScript { text: "replacement".to_string(), pause: None },
	]);
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "one" }));
	harness.wait_frame("message_update", timeout());
	harness.send(json!({ "type": "abort_and_prompt", "id": "ap1", "message": "two" }));
	// The abort is applied before the release, so the paused script cannot win
	// the race by completing the old turn with `stop`.
	wait_aborted(&harness);
	pause_tx.send(()).expect("release first response");
	let frames = harness.wait_frame("agent_end", timeout());

	let response = frames
		.iter()
		.find(|frame| frame["id"] == "ap1")
		.expect("abort_and_prompt response");
	assert_eq!(response["success"], true);
	assert_eq!(response["command"], "abort_and_prompt");

	// The aborted turn is journaled as `aborted`, and the replacement runs as a
	// real new turn: its text can only appear when a second provider request was
	// actually made (the mock serves the scripts in order).
	let message_ends: Vec<&Value> = frames
		.iter()
		.filter(|frame| frame["type"] == "message_end")
		.collect();
	assert_eq!(message_ends.len(), 2, "{frames:#?}");
	assert_eq!(message_ends[0]["message"]["stopReason"], "aborted");
	assert_eq!(message_ends[1]["message"]["stopReason"], "stop");
	let deltas: Vec<String> = frames
		.iter()
		.filter(|frame| frame["type"] == "message_update")
		.map(|frame| frame["assistantMessageEvent"]["delta"].as_str().unwrap_or("").to_string())
		.collect();
	assert!(deltas.iter().any(|delta| delta == "replacement"), "{deltas:?}");
	// Same agent run: one start, one end, no extra run for the replacement.
	assert_eq!(
		frames.iter().filter(|frame| frame["type"] == "agent_start").count(),
		1
	);
	assert_eq!(
		frames.iter().filter(|frame| frame["type"] == "agent_end").count(),
		1
	);
}

#[test]
fn abort_and_prompt_discards_the_queued_follow_ups_of_the_aborted_run() {
	let (pause_tx, pause_rx) = mpsc::channel();
	// The mock answers in request order, so a script named after the discarded
	// follow-up could never distinguish the cases: with a correct run only two
	// requests exist, and the second would answer with that script's text whatever
	// prompt it carried. The scripts therefore name the expected run (aborted
	// `old`, then the replacement) and the prompt itself is asserted below.
	let (port, prompts) = spawn_model_recording(vec![
		StreamScript { text: "old".to_string(), pause: Some(pause_rx) },
		StreamScript { text: "replacement".to_string(), pause: None },
	]);
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "one" }));
	harness.wait_frame("message_update", timeout());
	harness.send(json!({ "type": "follow_up", "id": "f1", "message": "stale" }));
	harness.send(json!({ "type": "abort_and_prompt", "id": "ap1", "message": "two" }));
	// Same barrier as the replacement test: the aborted turn must be journaled
	// before the paused script is released, or `stops` below is a coin flip.
	wait_aborted(&harness);
	pause_tx.send(()).expect("release first response");
	let frames = harness.wait_frame("agent_end", timeout());

	let deltas: Vec<String> = frames
		.iter()
		.filter(|frame| frame["type"] == "message_update")
		.map(|frame| frame["assistantMessageEvent"]["delta"].as_str().unwrap_or("").to_string())
		.collect();
	assert!(deltas.iter().any(|delta| delta == "replacement"), "{deltas:?}");
	// The queued follow-up belonged to the aborted run, so the provider saw exactly
	// two prompts and the second one is the replacement — the assertion the scripted
	// answer text cannot make.
	let prompts = prompts
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(prompts.len(), 2, "{prompts:?}");
	assert!(prompts[0].contains("one"), "{prompts:?}");
	assert!(
		prompts[1].contains("two"),
		"the replacement prompt reached the provider: {prompts:?}"
	);
	assert!(
		!prompts[1].contains("stale"),
		"the aborted run's queued follow-up was sent to the provider: {prompts:?}"
	);

	let stops: Vec<String> = frames
		.iter()
		.filter(|frame| frame["type"] == "message_end")
		.map(|frame| frame["message"]["stopReason"].as_str().unwrap_or("").to_string())
		.collect();
	assert_eq!(stops, vec!["aborted".to_string(), "stop".to_string()], "{frames:#?}");
}

#[test]
fn plain_abort_at_the_boundary_discards_the_replacement() {
	let (pause_tx, pause_rx) = mpsc::channel();
	let port = spawn_model(vec![
		StreamScript { text: "old".to_string(), pause: Some(pause_rx) },
		StreamScript { text: "replacement".to_string(), pause: None },
	]);
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "one" }));
	harness.wait_frame("message_update", timeout());
	harness.send(json!({ "type": "abort_and_prompt", "id": "ap1", "message": "two" }));
	// A plain abort supersedes the replacement. Whether it lands before or after
	// the worker takes it, the replacement must never complete: before the
	// provider call the run state stops it, after it the abort cancels its token.
	harness.send(json!({ "type": "abort", "id": "a1" }));
	let frames = harness.wait_frame("agent_end", timeout());
	drop(pause_tx);

	let completed_replacement = frames.iter().any(|frame| {
		frame["type"] == "message_end"
			&& frame["message"]["stopReason"] == "stop"
			&& frame["message"]["content"].to_string().contains("replacement")
	});
	assert!(!completed_replacement, "a plain abort let the replacement complete: {frames:#?}");
}

#[test]
fn eof_after_abort_and_prompt_never_completes_the_replacement() {
	let (pause_tx, pause_rx) = mpsc::channel();
	let port = spawn_model(vec![
		StreamScript { text: "old".to_string(), pause: Some(pause_rx) },
		StreamScript { text: "replacement".to_string(), pause: None },
	]);
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "one" }));
	harness.wait_frame("message_update", timeout());
	harness.send(json!({ "type": "abort_and_prompt", "id": "ap1", "message": "two" }));
	// EOF while the aborted turn is still draining. Whether the worker reaches
	// the replacement before EOF or after is a genuine race in both upstream and
	// here; what must hold in every interleaving is that shutdown never lets the
	// replacement finish.
	harness.close_stdin();
	let frames = harness.wait_frame("agent_end", timeout());
	let result = harness.join();
	assert!(result.is_ok(), "rpc run must exit cleanly: {result:?}");
	drop(pause_tx);

	let completed_replacement = frames.iter().any(|frame| {
		frame["type"] == "message_end"
			&& frame["message"]["stopReason"] == "stop"
			&& frame["message"]["content"]
				.to_string()
				.contains("replacement")
	});
	assert!(!completed_replacement, "EOF let the replacement complete: {frames:#?}");
	assert_eq!(
		frames.iter().filter(|frame| frame["type"] == "agent_start").count(),
		1
	);
}

#[test]
fn eof_drains_the_streaming_turn_and_exits_clean() {
	let (_pause_tx, pause_rx) = mpsc::channel();
	let port = spawn_model(vec![StreamScript {
		text:  "bye".to_string(),
		pause: Some(pause_rx),
	}]);
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "hi" }));
	harness.wait_frame("message_update", timeout());
	// EOF while the turn is still streaming: `run` must abort, drain and return.
	let result = harness.join();
	assert!(result.is_ok(), "rpc run must exit cleanly: {result:?}");
}

#[test]
fn abort_bash_cancels_the_running_command() {
	let port = spawn_model(Vec::new());
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.send(json!({
		"type": "bash",
		"id": "b1",
		"command": "sleep 30; echo never",
	}));
	harness.send(json!({ "type": "abort_bash", "id": "b2" }));
	let frames = harness.wait_for(
		|frames| {
			frames.iter().any(|frame| {
				frame["id"] == "b1" && frame["data"]["cancelled"] == true
			})
		},
		timeout(),
	);
	let bash = frames.iter().find(|frame| frame["id"] == "b1").expect("bash result");
	assert_eq!(bash["success"], true);
	assert_eq!(bash["data"]["cancelled"], true);
	assert!(
		!bash["data"]["output"].as_str().unwrap_or("").contains("never"),
		"the command must not finish after abort"
	);
}

#[test]
fn fast_mode_accepts_both_directions_where_the_provider_supports_it() {
	let port = spawn_model(Vec::new());
	let harness = Harness::start(launch(port, "openai/gpt-5", "openai"));
	harness.send(json!({ "type": "set_fast_mode", "id": "f1", "enabled": true }));
	let frames = harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "f1"),
		timeout(),
	);
	let response = frames.iter().find(|frame| frame["id"] == "f1").expect("fast on");
	assert_eq!(response["success"], true);
	assert_eq!(response["data"]["enabled"], true);
	assert_eq!(response["data"]["active"], true);

	harness.send(json!({ "type": "set_fast_mode", "id": "f2", "enabled": false }));
	let frames = harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "f2"),
		timeout(),
	);
	let response = frames.iter().find(|frame| frame["id"] == "f2").expect("fast off");
	assert_eq!(response["success"], true);
	assert_eq!(response["data"]["enabled"], false);
	assert_eq!(response["data"]["active"], false);
}

#[test]
fn fast_mode_true_is_rejected_for_models_without_a_tier_knob() {
	let port = spawn_model(Vec::new());
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.send(json!({ "type": "set_fast_mode", "id": "f1", "enabled": true }));
	let frames = harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "f1"),
		timeout(),
	);
	let response = frames.iter().find(|frame| frame["id"] == "f1").expect("fast");
	assert_eq!(response["success"], false);
	assert_eq!(response["error"], "Fast mode is unavailable for the current model.");
}

#[test]
fn login_providers_come_from_the_process_credential_store() {
	// The store is opened by `RpcMode::run` before `ready`. A fresh `agent.db` has
	// no credential rows, so a known native registry entry must be listed with
	// `authenticated: false`: the frame is the real registry joined with real
	// store state, not a placeholder array.
	let port = spawn_model(Vec::new());
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.wait_frame("ready", timeout());
	harness.send(json!({ "type": "get_login_providers", "id": "lp1" }));
	let frames = harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "lp1"),
		timeout(),
	);
	let response = frames.iter().find(|frame| frame["id"] == "lp1").expect("providers");
	assert_eq!(response["success"], true, "{response:#?}");
	let providers = response["data"]["providers"].as_array().expect("providers array");
	let gemini = providers
		.iter()
		.find(|provider| provider["id"] == "google-gemini-cli")
		.unwrap_or_else(|| {
			panic!("native registry must list google-gemini-cli: {response:#?}")
		});
	assert_eq!(
		gemini["authenticated"], false,
		"an empty credential store reports no login for google-gemini-cli: {gemini:#?}"
	);
}

#[test]
fn unreadable_agent_db_fails_startup_before_any_frame() {
	// A corrupted store is a startup failure, never a degraded run that resolves
	// a different credential: no `ready`, no session, no silent fallback.
	let path = agent_db_path();
	std::fs::write(&path, b"this is not a sqlite database").expect("write corrupt db");
	let mut launch = launch(0, "local/mock-model", "local");
	launch.agent_db = path.clone();
	let harness = Harness::start(launch);
	let out = Arc::clone(&harness.out);
	let result = harness.join();
	assert!(result.is_err(), "corrupt agent.db must fail startup: {result:?}");
	let frames = parse_frames(&out.lock().expect("out"));
	assert!(frames.is_empty(), "no frame may precede a failed auth open: {frames:#?}");
}

#[test]
fn compact_and_handoff_reject_while_a_login_owns_the_store() {
	// `cloudflare-ai-gateway` is an API-key login whose first steps are host
	// prompts, so the worker parks in the store-owning state without any network
	// traffic. Summarising for `compact`/`handoff` runs on the stdin thread and
	// resolves a key synchronously; it must be rejected immediately instead of
	// blocking every later frame behind the login's store lock.
	let port = spawn_model(Vec::new());
	let harness = Harness::start(launch(port, "local/mock-model", "local"));
	harness.wait_frame("ready", timeout());
	harness.send(json!({ "type": "login", "id": "l1", "providerId": "cloudflare-ai-gateway" }));
	let prompting = |frames: &[Value]| {
		frames.iter().any(|frame| {
			frame["type"] == "extension_ui_request" && frame["method"] == "input"
		})
	};
	let frames = harness.wait_for(|frames| prompting(frames), timeout());
	assert!(prompting(&frames), "login must reach a host prompt: {frames:#?}");

	for (id, command) in [("c1", "compact"), ("h1", "handoff")] {
		harness.send(json!({ "type": command, "id": id }));
		let frames = harness.wait_for(
			|frames| frames.iter().any(|frame| frame["id"] == id),
			timeout(),
		);
		let response = frames.iter().find(|frame| frame["id"] == id).expect("response");
		assert_eq!(response["success"], false, "{command} must be rejected: {response:#?}");
		assert_eq!(response["code"], LOGIN_IN_PROGRESS, "{command}: {response:#?}");
	}

	// EOF must not wait for the parked login: teardown cancels it, which withdraws
	// the prompt, so the mode thread joins instead of hanging on the store lock.
	assert!(harness.join().is_ok(), "EOF must finish while a login is open");
}

/// Plan C2e through the RPC transport: `checkpoint` → exploratory assistant
/// turn → `rewind` must cut the conversation the next provider request sees,
/// while the abandoned entries stay in the JSONL log and nothing on disk is
/// rolled back.
#[test]
fn rpc_checkpoint_rewind_replaces_context_and_keeps_abandoned_entries() {
	let root = checkpoint_root("rewind");
	let worktree = root.join("worktree");
	let tracked = worktree.join("plan.md");
	fs::write(&tracked, "untouched worktree file\n").expect("worktree file");
	let worktree_before: Vec<PathBuf> = {
		let mut entries: Vec<PathBuf> = fs::read_dir(&worktree)
			.expect("worktree")
			.filter_map(|entry| entry.ok().map(|entry| entry.path()))
			.collect();
		entries.sort();
		entries
	};

	let (log, bodies, frames) = run_checkpoint_rewind(&root);

	assert_eq!(bodies.len(), 4, "provider requests: {bodies:#?}");
	let body = |index: usize| bodies[index].to_string();
	assert!(body(0).contains("start investigation"));
	assert!(
		body(1).contains("Checkpoint: map it"),
		"checkpoint result missing from second request: {}",
		body(1)
	);
	assert!(body(2).contains("EXPLORATORY-SECRET"));
	assert!(
		body(2).contains("You MUST call rewind"),
		"pre-yield guard missing from exploratory turn: {}",
		body(2)
	);
	// The turn-end ack must carry the rebuilt conversation: the retained report
	// and branch summary replace the exploratory tail.
	assert!(
		body(3).contains("Branch-return summary"),
		"rewound context missing: {}",
		body(3)
	);
	assert!(body(3).contains("CONCISE-FINDINGS"));
	assert!(body(3).contains("Checkpoint called and rewound"));
	assert!(
		!body(3).contains("EXPLORATORY-SECRET"),
		"abandoned exploration leaked into the next request: {}",
		body(3)
	);

	let agent_end = frames
		.iter()
		.rev()
		.find(|frame| frame["type"] == "agent_end")
		.expect("agent_end");
	assert_eq!(agent_end["willContinue"], false);
	assert!(
		!agent_end.to_string().contains("EXPLORATORY-SECRET"),
		"abandoned turn leaked into agent_end: {agent_end}"
	);

	// Conversation-only rewind: the log keeps every abandoned entry.
	let persisted = fs::read_to_string(&log).expect("read session log");
	assert!(
		persisted.contains("EXPLORATORY-SECRET"),
		"abandoned assistant entry must stay persisted"
	);
	assert!(persisted.contains("CONCISE-FINDINGS"));
	assert!(persisted.contains("branch_summary"));
	assert!(persisted.contains("rewind-report"));

	// No filesystem rollback: the worktree is byte-identical and unchanged.
	assert_eq!(
		fs::read_to_string(&tracked).expect("tracked file"),
		"untouched worktree file\n"
	);
	let worktree_after: Vec<PathBuf> = {
		let mut entries: Vec<PathBuf> = fs::read_dir(&worktree)
			.expect("worktree")
			.filter_map(|entry| entry.ok().map(|entry| entry.path()))
			.collect();
		entries.sort();
		entries
	};
	assert_eq!(
		worktree_after, worktree_before,
		"rewind must not change the worktree"
	);
	fs::remove_dir_all(&root).ok();
}

/// No-session parity: a rewind must commit at successful turn end (so a later
/// `checkpoint` is accepted) and cut from the loop's authoritative messages,
/// not from the session's stale mirror.
#[test]
fn rpc_no_session_rewind_commits_and_handles_a_second_cycle() {
	let (port, requests) = spawn_tool_model(vec![
		ToolScript {
			text: String::new(),
			tool: Some(("checkpoint", json!({ "goal": "cycle one" }))),
		},
		ToolScript { text: "EXPLORATORY-ONE-SECRET".to_string(), tool: None },
		ToolScript {
			text: String::new(),
			tool: Some(("rewind", json!({ "report": "FINDINGS-ONE" }))),
		},
		ToolScript { text: "FINAL-ONE".to_string(), tool: None },
		ToolScript {
			text: String::new(),
			tool: Some(("checkpoint", json!({ "goal": "cycle two" }))),
		},
		ToolScript { text: "EXPLORATORY-TWO-SECRET".to_string(), tool: None },
		ToolScript {
			text: String::new(),
			tool: Some(("rewind", json!({ "report": "FINDINGS-TWO" }))),
		},
		ToolScript { text: "FINAL-TWO".to_string(), tool: None },
	]);
	let harness = Harness::start(launch_checkpoint_in_memory(port));
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "first investigation" }));
	harness.wait_frame("agent_end", timeout());
	// Round-trip a read-only command so the worker's `end_if` has run before the
	// second prompt; otherwise it could land as a steer on the finished run.
	harness.send(json!({ "type": "get_state", "id": "idle1" }));
	harness.wait_for(|frames| frames.iter().any(|frame| frame["id"] == "idle1"), timeout());
	harness.send(json!({ "type": "prompt", "id": "p2", "message": "second investigation" }));
	let frames = harness.wait_for(
		|frames| frames.iter().filter(|frame| frame["type"] == "agent_end").count() >= 2,
		timeout(),
	);

	let bodies = requests
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(bodies.len(), 8, "provider requests: {bodies:#?}");
	let body = |index: usize| bodies[index].to_string();

	// The cut keeps the checkpoint batch and drops the exploration; the summary
	// and report replace it for the continuation.
	assert!(body(3).contains("Checkpoint: cycle one"), "{}", body(3));
	assert!(body(3).contains("Branch-return summary"), "{}", body(3));
	assert!(body(3).contains("FINDINGS-ONE"), "{}", body(3));
	assert!(!body(3).contains("EXPLORATORY-ONE-SECRET"), "{}", body(3));

	// The committed controller accepts a second checkpoint in the next prompt,
	// and that prompt's history is the mirror of the cut branch: the retained
	// report, not the abandoned exploration.
	assert!(body(4).contains("FINAL-ONE"), "{}", body(4));
	assert!(body(4).contains("FINDINGS-ONE"), "{}", body(4));
	assert!(body(4).contains("Branch-return summary"), "{}", body(4));
	assert!(!body(4).contains("EXPLORATORY-ONE-SECRET"), "{}", body(4));
	assert!(body(5).contains("Checkpoint: cycle two"), "{}", body(5));
	assert!(body(7).contains("FINDINGS-TWO"), "{}", body(7));
	assert!(!body(7).contains("EXPLORATORY-TWO-SECRET"), "{}", body(7));

	// `turn_end` describes the turn that settled, never the retained checkpoint.
	let rewind_turn = frames
		.iter()
		.filter(|frame| frame["type"] == "turn_end")
		.find(|frame| {
			frame["message"]["content"].as_array().is_some_and(|parts| {
				parts.iter().any(|part| {
					part.get("type").and_then(Value::as_str) == Some("toolCall")
						&& part.get("name").and_then(Value::as_str) == Some("rewind")
				})
			})
		})
		.expect("rewind turn_end frame");
	assert_eq!(rewind_turn["toolResults"][0]["toolName"], "rewind", "{rewind_turn:#?}");

	let agent_ends: Vec<&Value> = frames
		.iter()
		.filter(|frame| frame["type"] == "agent_end")
		.collect();
	assert_eq!(agent_ends.len(), 2, "{frames:#?}");
	assert!(
		!agent_ends[0].to_string().contains("EXPLORATORY-ONE-SECRET"),
		"{}",
		agent_ends[0]
	);
	assert!(
		!agent_ends[1].to_string().contains("EXPLORATORY-TWO-SECRET"),
		"{}",
		agent_ends[1]
	);
	drop(harness);
}

/// The persisted rewind's own `turn_end` must describe the settled turn, not
/// the retained checkpoint batch that now ends the durable branch.
#[test]
fn rpc_rewind_turn_end_reports_the_settled_turn() {
	let root = checkpoint_root("turn-end");
	let (_, _, frames) = run_checkpoint_rewind(&root);
	let rewind_turn = frames
		.iter()
		.filter(|frame| frame["type"] == "turn_end")
		.find(|frame| {
			frame["message"]["content"].as_array().is_some_and(|parts| {
				parts.iter().any(|part| {
					part.get("type").and_then(Value::as_str) == Some("toolCall")
						&& part.get("name").and_then(Value::as_str) == Some("rewind")
				})
			})
		})
		.expect("rewind turn_end frame");
	let message = &rewind_turn["message"];
	assert_eq!(message["role"], "assistant", "{rewind_turn:#?}");
	let call = message["content"]
		.as_array()
		.expect("message content")
		.iter()
		.find(|part| part["name"] == "rewind")
		.expect("rewind tool call");
	let results = rewind_turn["toolResults"].as_array().expect("tool results");
	assert_eq!(results.len(), 1, "{rewind_turn:#?}");
	assert_eq!(results[0]["role"], "toolResult");
	assert_eq!(results[0]["toolName"], "rewind");
	assert_eq!(results[0]["toolCallId"], call["id"]);
	fs::remove_dir_all(&root).ok();
}

/// Todo state is branch-scoped: a rewind drops the abandoned tail's todo edits
/// from the session frame and from the next persisted request.
#[test]
fn rpc_rewind_todo_state_follows_the_active_branch() {
	let root = checkpoint_root("todo");
	let worktree = root.join("worktree");
	let sessions = root.join("sessions");
	let (port, requests) = spawn_tool_model(vec![
		ToolScript {
			text: String::new(),
			tool: Some((
				"todo",
				json!({ "op": "init", "list": [{ "phase": "kept", "items": ["RETAINED-TODO"] }] }),
			)),
		},
		ToolScript {
			text: String::new(),
			tool: Some(("checkpoint", json!({ "goal": "map it" }))),
		},
		ToolScript {
			text: String::new(),
			tool: Some((
				"todo",
				json!({
					"op": "init",
					"list": [{ "phase": "abandoned", "items": ["ABANDONED-TODO"] }],
				}),
			)),
		},
		ToolScript { text: "EXPLORATORY-SECRET".to_string(), tool: None },
		ToolScript {
			text: String::new(),
			tool: Some(("rewind", json!({ "report": "TODO-FINDINGS" }))),
		},
		ToolScript { text: "FINAL-TODO".to_string(), tool: None },
		ToolScript {
			text: String::new(),
			tool: Some(("todo", json!({ "op": "view" }))),
		},
		ToolScript { text: "AFTER-VIEW".to_string(), tool: None },
	]);
	let harness =
		Harness::start(launch_checkpoint(port, &worktree, &sessions, Some("todo-branch")));
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "todo investigation" }));
	harness.wait_frame("agent_end", timeout());

	harness.send(json!({ "type": "get_state", "id": "s1" }));
	let frames =
		harness.wait_for(|frames| frames.iter().any(|frame| frame["id"] == "s1"), timeout());
	let state = frames
		.iter()
		.find(|frame| frame["id"] == "s1")
		.expect("state response");
	let phases = state["data"]["todoPhases"].to_string();
	assert!(phases.contains("RETAINED-TODO"), "{state:#?}");
	assert!(
		!phases.contains("ABANDONED-TODO"),
		"abandoned todo leaked into state: {state:#?}"
	);

	// The next prompt's request restores the branch's todo state, so a `view`
	// call reports the retained task and never the abandoned one.
	harness.send(json!({ "type": "prompt", "id": "p2", "message": "inspect todos" }));
	harness.wait_for(
		|frames| frames.iter().filter(|frame| frame["type"] == "agent_end").count() >= 2,
		timeout(),
	);
	let bodies = requests
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(bodies.len(), 8, "provider requests: {bodies:#?}");
	let after_view = bodies[7].to_string();
	assert!(after_view.contains("RETAINED-TODO"), "{after_view}");
	assert!(!after_view.contains("ABANDONED-TODO"), "abandoned todo re-sent: {after_view}");
	drop(harness);
	fs::remove_dir_all(&root).ok();
}

fn first_header_id(path: &Path) -> String {
	let content = fs::read_to_string(path).expect("read forked session");
	content
		.lines()
		.filter_map(|line| serde_json::from_str::<Value>(line).ok())
		.find(|value| value["type"] == "session")
		.and_then(|value| value["id"].as_str().map(str::to_string))
		.expect("forked session header id")
}

/// A reopened log (restart) and a forked log must both restore the completed
/// rewind: the retained report is context, and `rewind` is rejected.
#[test]
fn rpc_completed_rewind_survives_restart_and_fork() {
	let root = checkpoint_root("resume");
	let (log, _, _) = run_checkpoint_rewind(&root);
	let worktree = root.join("worktree");
	let sessions = root.join("sessions");

	let reopen = |dir: &Path, id: &str, label: &str| {
		let (port, requests) = spawn_tool_model(vec![
			ToolScript {
				text: String::new(),
				tool: Some(("rewind", json!({ "report": "again" }))),
			},
			ToolScript { text: format!("{label}-DONE"), tool: None },
		]);
		let harness = Harness::start(launch_checkpoint(port, &worktree, dir, Some(id)));
		harness.send(json!({ "type": "prompt", "id": "p1", "message": "resume" }));
		harness.wait_frame("agent_end", timeout());
		let bodies = requests
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.clone();
		drop(harness);
		bodies
	};

	let restarted = reopen(&sessions, "cp-smoke", "restart");
	assert_eq!(restarted.len(), 2, "restart requests: {restarted:#?}");
	assert!(
		restarted[0].to_string().contains("CONCISE-FINDINGS"),
		"completed rewind report missing after restart: {}",
		restarted[0]
	);
	assert!(
		restarted[1].to_string().contains("Checkpoint already completed"),
		"completed guard not restored after restart: {}",
		restarted[1]
	);

	let fork_dir = root.join("forks");
	let forked = fork_session(&log, Some(&worktree), Some(&fork_dir)).expect("fork session");
	let fork_id = first_header_id(&forked);
	let forked_requests = reopen(&fork_dir, &fork_id, "fork");
	assert_eq!(forked_requests.len(), 2, "fork requests: {forked_requests:#?}");
	assert!(
		forked_requests[0].to_string().contains("CONCISE-FINDINGS"),
		"completed rewind report missing after fork: {}",
		forked_requests[0]
	);
	assert!(
		forked_requests[1].to_string().contains("Checkpoint already completed"),
		"completed guard not restored after fork: {}",
		forked_requests[1]
	);
	fs::remove_dir_all(&root).ok();
}

/// The checkpoint controller is branch-scoped: selecting an earlier entry must
/// drop the open checkpoint from the abandoned path, and selecting the
/// checkpoint entry again must restore it.
#[test]
fn rpc_branch_rehydrates_checkpoint_state_for_the_selected_branch() {
	const TS: &str = "2026-10-29T00:00:00.000Z";
	let root = checkpoint_root("branch");
	let worktree = root.join("worktree");
	let sessions = root.join("sessions");

	let mut store = SessionStore::open(&checkpoint_session_options(
		&worktree,
		&sessions,
		Some("branch-fixture"),
	));
	assert!(store.enabled(), "fixture store: {:?}", store.corrupt_reason());
	let mut call = ContentPart {
		kind: "toolCall".to_string(),
		text: None,
		data: None,
		mime_type: None,
		extra: serde_json::Map::new(),
	};
	call.extra.insert("id".to_string(), json!("call-cp"));
	call.extra.insert("name".to_string(), json!("checkpoint"));
	call.extra.insert("arguments".to_string(), json!({ "goal": "map it" }));
	let assistant = SessionMessage::assistant(
		MessageContent::Parts(vec![call]),
		"api",
		"local",
		"mock-model",
		None,
		"toolUse",
		2,
	);
	let mut result = SessionMessage::tool_result(
		"call-cp",
		"checkpoint",
		MessageContent::Text(
			"Checkpoint: map it\nFinish exploration and formulate findings.".to_string(),
		),
		false,
		3,
	);
	result.extra.insert(
		"details".to_string(),
		json!({ "goal": "map it", "startedAt": TS }),
	);
	store
		.append_messages(&[
			MessageEntry::new(
				"u1",
				None,
				TS,
				SessionMessage::user(MessageContent::Text("root question".to_string()), 1),
			),
			MessageEntry::new("cp-asst", Some("u1".to_string()), TS, assistant),
			MessageEntry::new("cp-result", Some("cp-asst".to_string()), TS, result),
		])
		.expect("append fixture turn");
	drop(store);

	let (port, requests) = spawn_tool_model(vec![
		ToolScript { text: "BRANCH-OK".to_string(), tool: None },
		ToolScript { text: "CHECKPOINT-OPEN".to_string(), tool: None },
		ToolScript {
			text: String::new(),
			tool: Some(("rewind", json!({ "report": "BRANCH-FINDINGS" }))),
		},
		ToolScript { text: "FINAL-BRANCH".to_string(), tool: None },
	]);
	let harness = Harness::start(launch_checkpoint(
		port,
		&worktree,
		&sessions,
		Some("branch-fixture"),
	));

	// Branch to the root user message: the abandoned checkpoint must not keep
	// the yield guard armed, so one provider request finishes the turn.
	harness.send(json!({ "type": "branch", "id": "b1", "entryId": "u1" }));
	let frames = harness.wait_for(|frames| frames.iter().any(|frame| frame["id"] == "b1"), timeout());
	let branch = frames.iter().find(|frame| frame["id"] == "b1").expect("branch response");
	assert_eq!(branch["success"], true, "branch response: {branch:#?}");
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "after branch" }));
	harness.wait_frame("agent_end", timeout());

	let after_root_branch = requests
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(
		after_root_branch.len(),
		1,
		"stale checkpoint forced a continuation: {after_root_branch:#?}"
	);
	let root_request = after_root_branch[0].to_string();
	assert!(!root_request.contains("You MUST call rewind"), "{root_request}");
	assert!(!root_request.contains("Checkpoint: map it"), "{root_request}");

	// Branch back to the checkpoint result: the open checkpoint returns and the
	// guard continues the turn with the rewind warning.
	harness.send(json!({ "type": "branch", "id": "b2", "entryId": "cp-result" }));
	let frames = harness.wait_for(|frames| frames.iter().any(|frame| frame["id"] == "b2"), timeout());
	let branch = frames.iter().find(|frame| frame["id"] == "b2").expect("branch response");
	assert_eq!(branch["success"], true, "branch response: {branch:#?}");
	harness.send(json!({ "type": "prompt", "id": "p2", "message": "with checkpoint" }));
	harness.wait_for(
		|frames| frames.iter().filter(|frame| frame["type"] == "agent_end").count() >= 2,
		timeout(),
	);

	let all = requests
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(all.len(), 4, "branch requests: {all:#?}");
	assert!(
		all[2].to_string().contains("You MUST call rewind"),
		"checkpoint state not restored after branching back: {}",
		all[2]
	);
	// The restored checkpoint is cut by the scripted rewind, and the abandoned
	// post-branch turn stays out of the final request.
	let final_request = all[3].to_string();
	assert!(final_request.contains("Branch-return summary"), "{final_request}");
	assert!(final_request.contains("BRANCH-FINDINGS"), "{final_request}");
	assert!(!final_request.contains("CHECKPOINT-OPEN"), "{final_request}");
	drop(harness);
	fs::remove_dir_all(&root).ok();
}

/// RPC transport, same provider id, two model endpoints: the explicit root
/// secret must stay on the root endpoint and the switch must resolve its own
/// configured key for the alternate endpoint.
#[test]
fn rpc_prewalk_does_not_carry_the_root_secret_to_an_alternate_endpoint() {
	let (a_port, a_requests, a_auth) = spawn_tool_model_with_auth(vec![ToolScript {
		text: String::new(),
		tool: Some(("write", json!({ "path": ".", "content": "x" }))),
	}]);
	let (b_port, b_requests, b_auth) = spawn_tool_model_with_auth(vec![ToolScript {
		text: "alternate endpoint".to_string(),
		tool: None,
	}]);
	let mut catalog = Catalog::default();
	let mut models = std::collections::BTreeMap::new();
	models.insert(
		"model-a".to_string(),
		ModelInfo {
			provider: "mock".to_string(),
			base_url: Some(format!("http://127.0.0.1:{a_port}/v1")),
			context_window: Some(128_000),
			..ModelInfo::default()
		},
	);
	models.insert(
		"model-b".to_string(),
		ModelInfo {
			provider: "mock".to_string(),
			base_url: Some(format!("http://127.0.0.1:{b_port}/v1")),
			context_window: Some(128_000),
			reasoning: true,
			efforts: Some(vec!["low".to_string(), "high".to_string()]),
			thinking: Some(json!({
				"efforts": ["low", "high"],
				"defaultLevel": "max"
			})),
			..ModelInfo::default()
		},
	);
	catalog.providers.insert("mock".to_string(), models);

	let mut launch = launch(a_port, "mock/model-a", "mock");
	launch.catalog = Arc::new(catalog);
	{
		let provider = launch
			.config
			.providers
			.get_mut("mock")
			.expect("mock provider");
		provider.api_key = Some("target-key".to_string());
		provider.base_url = None;
	}
	launch.api_key = Some("root-secret".to_string());
	launch.tools_enabled = true;
	// Same gate as the CLI fixture: todo is not offered, so the switch does not
	// wait for a todo call before the first write.
	launch.config.tools = Some(vec!["write".to_string(), "read".to_string()]);
	launch.max_turns = 8;
	launch.prewalk = Some(PrewalkArm {
		target: ModelSwitch {
			provider: "mock".to_string(),
			model: "model-b".to_string(),
			effort: None,
			thinking: None,
		},
		thinking: None,
	});
	let harness = Harness::start(launch);
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "write" }));
	let frames = harness.wait_frame("agent_end", timeout());

	let a_count = a_requests.lock().expect("lock").len();
	let b_count = b_requests.lock().expect("lock").len();
	assert_eq!(
		(a_count, b_count),
		(1, 1),
		"root/target requests before assertions; frames: {frames:#?}"
	);
	let root_auth = a_auth
		.lock()
		.expect("lock")
		.first()
		.cloned()
		.flatten()
		.expect("root authorization");
	assert!(root_auth.contains("root-secret"), "{root_auth}");
	let target_auth = b_auth
		.lock()
		.expect("lock")
		.first()
		.cloned()
		.flatten()
		.expect("target authorization");
	assert!(target_auth.contains("target-key"), "{target_auth}");
	assert!(
		!target_auth.contains("root-secret"),
		"RPC root secret leaked to the alternate endpoint: {target_auth}"
	);
	assert_eq!(
		b_requests.lock().expect("lock")[0]["reasoning_effort"],
		"high",
		"RPC switch must apply the target model defaultLevel"
	);
	drop(harness);
}

/// Prewalk through the RPC transport: the *same* session owner must journal
/// the ephemeral role, actually continue on the target provider, and keep the
/// switch out of the user's conversation (plan guidance scrubbed).
#[test]
fn rpc_prewalk_switches_after_todo_and_first_write() {
	let root = checkpoint_root("prewalk");
	let worktree = root.join("worktree");
	let sessions = root.join("sessions");
	let (port, requests) = spawn_tool_model(vec![
		ToolScript {
			text: "exploring".to_string(),
			tool: Some((
				"todo",
				json!({ "op": "init", "list": [{ "phase": "P", "items": ["Do it"] }] }),
			)),
		},
		ToolScript {
			text: String::new(),
			tool: Some(("write", json!({ "path": "probe.txt", "content": "done" }))),
		},
		ToolScript { text: "switched".to_string(), tool: None },
	]);
	let mut launch = launch(port, "local/mock-model", "local");
	launch.config.providers.insert(
		"fast".to_string(),
		ProviderConfig {
			base_url: Some(format!("http://127.0.0.1:{port}/v1")),
			api_key: Some("sk-fast".to_string()),
			..Default::default()
		},
	);
	launch.cwd = worktree.clone();
	launch.tools_enabled = true;
	launch.max_turns = 8;
	launch.store_options = checkpoint_session_options(&worktree, &sessions, Some("prewalk-rpc"));
	launch.prewalk = Some(PrewalkArm {
		target: ModelSwitch {
			provider: "fast".to_string(),
			model: "small".to_string(),
			effort: None,
			thinking: None,
		},
		thinking: None,
	});
	let harness = Harness::start(launch);
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "start" }));
	harness.wait_frame("agent_end", timeout());

	let bodies = requests
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(bodies.len(), 3, "prewalk request sequence: {bodies:#?}");
	let first_model = bodies[0]["model"].as_str().unwrap_or_default();
	assert_ne!(first_model, "small", "first turn must stay on the root model");
	let switched = bodies[2].to_string();
	assert!(
		switched.contains("\"model\":\"small\""),
		"turn after the write must run on the prewalk target: {switched}"
	);
	assert!(
		!switched.contains("STOP: In NEXT reply"),
		"consumed plan guidance must be scrubbed: {switched}"
	);

	let log = session_log(&sessions);
	let entries: Vec<Value> = fs::read_to_string(&log)
		.expect("session log")
		.lines()
		.filter_map(|line| serde_json::from_str(line).ok())
		.collect();
	let change = entries
		.iter()
		.find(|entry| entry["type"] == "model_change")
		.expect("prewalk model_change entry");
	assert_eq!(change["model"], "fast/small");
	assert_eq!(change["role"], "fallback", "switch must be ephemeral on resume");
	assert!(
		!entries.iter().any(|entry| entry["type"] == "message"
			&& entry.to_string().contains("STOP: In NEXT reply")),
		"plan nudge must never be journaled"
	);
	assert!(
		entries.iter().any(|entry| entry["type"] == "custom_message"
			&& entry["customType"] == "prewalk-checklist"),
		"checklist continuation must replay from the journal"
	);
	drop(harness);
	fs::remove_dir_all(&root).ok();
}

/// Plan-yolo through RPC: guard blocks the working tree, approval queues an
/// atomic switch, the target implements, and the journal uses the ephemeral
/// fallback role.
#[test]
fn rpc_plan_yolo_guards_proposes_switches_and_journals_fallback() {
	let root = checkpoint_root("plan-yolo");
	let worktree = root.join("worktree");
	let sessions = root.join("sessions");
	let (port, requests) = spawn_tool_model(vec![
		ToolScript {
			text: String::new(),
			tool: Some((
				"write",
				json!({ "path": "blocked.txt", "content": "must not land" }),
			)),
		},
		ToolScript {
			text: String::new(),
			tool: Some((
				"write",
				json!({
					"path": "local://plan-plan.md",
					"content": "# Plan\n\nImplement probe.\n"
				}),
			)),
		},
		ToolScript {
			text: String::new(),
			tool: Some((
				"write",
				json!({ "path": "xd://propose", "content": "Plan" }),
			)),
		},
		ToolScript {
			text: String::new(),
			tool: Some(("write", json!({ "path": "probe.txt", "content": "done" }))),
		},
		ToolScript { text: "implemented".to_string(), tool: None },
	]);
	let mut launch = launch(port, "local/mock-model", "local");
	launch.config.providers.insert(
		"fast".to_string(),
		ProviderConfig {
			base_url: Some(format!("http://127.0.0.1:{port}/v1")),
			api_key: Some("sk-fast".to_string()),
			..Default::default()
		},
	);
	launch.cwd = worktree.clone();
	launch.tools_enabled = true;
	launch.max_turns = 8;
	launch.store_options = checkpoint_session_options(
		&worktree,
		&sessions,
		Some("plan-yolo-rpc"),
	);
	launch.plan_yolo = Some(PlanYoloArm {
		target: ModelSwitch {
			provider: "fast".to_string(),
			model: "small".to_string(),
			effort: None,
			thinking: None,
		},
		thinking: None,
	});
	let harness = Harness::start(launch);
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "start" }));
	harness.wait_frame("agent_end", timeout());

	let bodies = requests
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(bodies.len(), 5, "plan-yolo request sequence: {bodies:#?}");
	assert!(
		!worktree.join("blocked.txt").exists(),
		"the guard must deny working-tree writes before approval"
	);
	for body in &bodies[..3] {
		assert_ne!(body["model"], "small", "planning stays on the root model");
	}
	assert_eq!(bodies[3]["model"], "small", "implementation uses the plan target");
	assert_eq!(
		bodies[3].to_string().matches("Plan approved: **Plan**").count(),
		1
	);
	assert_eq!(fs::read_to_string(worktree.join("probe.txt")).expect("probe"), "done");

	let log = session_log(&sessions);
	let entries: Vec<Value> = fs::read_to_string(&log)
		.expect("session log")
		.lines()
		.filter_map(|line| serde_json::from_str(line).ok())
		.collect();
	let change = entries
		.iter()
		.find(|entry| entry["type"] == "model_change")
		.expect("plan-yolo model_change entry");
	assert_eq!(change["model"], "fast/small");
	assert_eq!(change["role"], "fallback", "switch must be ephemeral on resume");
	drop(harness);
	fs::remove_dir_all(&root).ok();
}

/// Actual localhost wire: an inherited prompt-cache key survives a no-op or
/// rejected thinking selection and dies only when the effective level changes.
#[test]
fn rpc_failed_plan_switch_surfaces_an_error_instead_of_success() {
	let root = checkpoint_root("plan-yolo-build-error");
	let worktree = root.join("worktree");
	let sessions = root.join("sessions");
	let (port, requests) = spawn_tool_model(vec![
		ToolScript {
			text: String::new(),
			tool: Some((
				"write",
				json!({ "path": "blocked.txt", "content": "must not land" }),
			)),
		},
		ToolScript {
			text: String::new(),
			tool: Some((
				"write",
				json!({
					"path": "local://plan-plan.md",
					"content": "# Plan\n\nImplement probe.\n"
				}),
			)),
		},
		ToolScript {
			text: String::new(),
			tool: Some((
				"write",
				json!({ "path": "xd://propose", "content": "Plan" }),
			)),
		},
		ToolScript { text: "done".to_string(), tool: None },
	]);
	let mut launch = launch(port, "local/mock-model", "local");
	launch.cwd = worktree.clone();
	launch.tools_enabled = true;
	launch.max_turns = 8;
	launch.store_options = checkpoint_session_options(
		&worktree,
		&sessions,
		Some("plan-yolo-rpc-build-error"),
	);
	launch.plan_yolo = Some(PlanYoloArm {
		target: ModelSwitch {
			provider: "missing-provider".to_string(),
			model: "small".to_string(),
			effort: None,
			thinking: None,
		},
		thinking: None,
	});
	let harness = Harness::start(launch);
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "start" }));
	let frames = harness.wait_for(
		|frames| {
			frames.iter().any(|frame| {
				frame["type"] == "response"
					&& frame["id"] == "p1"
					&& frame["success"] == false
			}) && frames.iter().any(|frame| {
				frame["type"] == "agent_end" && frame["willContinue"] == false
			})
		},
		timeout(),
	);
	let error = frames
		.iter()
		.find(|frame| {
			frame["type"] == "response"
				&& frame["id"] == "p1"
				&& frame["success"] == false
		})
		.expect("prompt error response");
	assert!(
		error["error"]
			.as_str()
			.is_some_and(|message| message.contains("plan-yolo switch failed")),
		"error={error}"
	);
	assert!(
		frames.iter().any(|frame| {
			frame["type"] == "agent_end" && frame["willContinue"] == false
		}),
		"failed switch must terminate the run: {frames:#?}"
	);
	assert_eq!(
		requests
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.len(),
		4,
		"provider build fails before any implementation request"
	);
	assert!(!worktree.join("probe.txt").exists());
	let log = session_log(&sessions);
	let entries: Vec<Value> = fs::read_to_string(&log)
		.expect("session log")
		.lines()
		.filter_map(|line| serde_json::from_str(line).ok())
		.collect();
	let failure = entries
		.iter()
		.find(|entry| {
			entry["type"] == "custom_message"
				&& entry["customType"] == "plan-yolo-switch-error"
		})
		.expect("failed cutover must leave a durable journal entry");
	assert!(
		failure["content"]
			.as_str()
			.is_some_and(|content| content.contains("plan-yolo switch failed")),
		"failure entry must carry the consumer-visible error: {failure}"
	);
	let resumed = SessionStore::open(&checkpoint_session_options(
		&worktree,
		&sessions,
		Some("plan-yolo-rpc-build-error"),
	));
	assert!(
		resumed.branch_entries().iter().any(|entry| {
			entry.kind == "custom_message"
				&& entry.extra["customType"] == "plan-yolo-switch-error"
		}),
		"resuming must retain the failed cutover"
	);
	drop(harness);
	fs::remove_dir_all(&root).ok();
}

#[test]
fn rpc_thinking_change_drops_inherited_cache_key_but_noop_keeps_it() {
	let (port, requests, _) = spawn_tool_model_with_auth(vec![
		ToolScript { text: "one".to_string(), tool: None },
		ToolScript { text: "two".to_string(), tool: None },
		ToolScript { text: "three".to_string(), tool: None },
	]);
	let mut launch = launch(port, "model-a", "kimi-code");
	launch.catalog = Arc::new(effort_catalog());
	launch.thinking = Some(CliThinking::Effort("high"));
	launch.prompt_cache_key = Some("header-cache-1".to_string());
	launch.prompt_cache_key_inherited = true;
	launch.max_turns = 2;
	let harness = Harness::start(launch);

	harness.send(json!({ "type": "set_thinking_level", "id": "t1", "level": "high" }));
	let frames = harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "t1"),
		timeout(),
	);
	assert_eq!(
		frames
			.iter()
			.find(|frame| frame["id"] == "t1")
			.expect("same-level response")["success"],
		true
	);
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "one" }));
	harness.wait_for(
		|frames| {
			frames
				.iter()
				.filter(|frame| frame["type"] == "agent_end")
				.count()
				>= 1
		},
		timeout(),
	);

	harness.send(json!({ "type": "set_thinking_level", "id": "t2", "level": "bogus" }));
	let frames = harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "t2"),
		timeout(),
	);
	assert_eq!(
		frames
			.iter()
			.find(|frame| frame["id"] == "t2")
			.expect("invalid-level response")["success"],
		false
	);
	harness.send(json!({ "type": "prompt", "id": "p2", "message": "two" }));
	harness.wait_for(
		|frames| {
			frames
				.iter()
				.filter(|frame| frame["type"] == "agent_end")
				.count()
				>= 2
		},
		timeout(),
	);

	harness.send(json!({ "type": "set_thinking_level", "id": "t3", "level": "low" }));
	harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "t3"),
		timeout(),
	);
	harness.send(json!({ "type": "prompt", "id": "p3", "message": "three" }));
	harness.wait_for(
		|frames| {
			frames
				.iter()
				.filter(|frame| frame["type"] == "agent_end")
				.count()
				>= 3
		},
		timeout(),
	);

	let bodies = requests
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(bodies.len(), 3, "{bodies:#?}");
	assert_eq!(bodies[0]["prompt_cache_key"], "header-cache-1");
	assert_eq!(
		bodies[1]["prompt_cache_key"], "header-cache-1",
		"a rejected selection must not change the cache shape: {}",
		bodies[1]
	);
	assert!(
		bodies[2].get("prompt_cache_key").is_none(),
		"an actual thinking change must drop the inherited key: {}",
		bodies[2]
	);
	assert_eq!(bodies[2]["reasoning_effort"], "low");
	drop(harness);
}

/// A restored raw suffix can clamp to a different identifier: `max` resolves to
/// `high` on this catalog, so selecting `high` is a no-op and must keep the
/// inherited key; a real change to `low` then drops it.
#[test]
fn rpc_thinking_restored_clamped_noop_keeps_inherited_cache_key() {
	let (port, requests, _) = spawn_tool_model_with_auth(vec![
		ToolScript { text: "one".to_string(), tool: None },
		ToolScript { text: "two".to_string(), tool: None },
	]);
	let mut launch = launch(port, "model-a", "kimi-code");
	launch.catalog = Arc::new(effort_catalog());
	// Recorded as `:max`; this catalog only advertises low/high, so its
	// effective level is high both before and after selecting high.
	launch.thinking = Some(CliThinking::Effort("max"));
	launch.prompt_cache_key = Some("header-cache-1".to_string());
	launch.prompt_cache_key_inherited = true;
	launch.max_turns = 2;
	let harness = Harness::start(launch);

	harness.send(json!({ "type": "set_thinking_level", "id": "t1", "level": "high" }));
	harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "t1"),
		timeout(),
	);
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "one" }));
	harness.wait_for(
		|frames| {
			frames
				.iter()
				.filter(|frame| frame["type"] == "agent_end")
				.count()
				>= 1
		},
		timeout(),
	);

	harness.send(json!({ "type": "set_thinking_level", "id": "t2", "level": "low" }));
	harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "t2"),
		timeout(),
	);
	harness.send(json!({ "type": "prompt", "id": "p2", "message": "two" }));
	harness.wait_for(
		|frames| {
			frames
				.iter()
				.filter(|frame| frame["type"] == "agent_end")
				.count()
				>= 2
		},
		timeout(),
	);

	let bodies = requests
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(bodies.len(), 2, "{bodies:#?}");
	assert_eq!(
		bodies[0]["prompt_cache_key"], "header-cache-1",
		"restored max->high is the same effective level: {}",
		bodies[0]
	);
	assert_eq!(bodies[0]["reasoning_effort"], "high");
	assert!(
		bodies[1].get("prompt_cache_key").is_none(),
		"a real low change must drop the inherited key: {}",
		bodies[1]
	);
	assert_eq!(bodies[1]["reasoning_effort"], "low");
	drop(harness);
}

/// An explicit `--prompt-cache-key` is not inherited identity and survives an
/// actual thinking change.
#[test]
fn rpc_thinking_change_keeps_explicit_cache_key() {
	let (port, requests, _) = spawn_tool_model_with_auth(vec![ToolScript {
		text: "one".to_string(),
		tool: None,
	}]);
	let mut launch = launch(port, "model-a", "kimi-code");
	launch.catalog = Arc::new(effort_catalog());
	launch.thinking = Some(CliThinking::Effort("high"));
	launch.prompt_cache_key = Some("explicit-cache-1".to_string());
	launch.prompt_cache_key_inherited = false;
	launch.max_turns = 2;
	let harness = Harness::start(launch);
	harness.send(json!({ "type": "set_thinking_level", "id": "t1", "level": "low" }));
	harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "t1"),
		timeout(),
	);
	harness.send(json!({ "type": "prompt", "id": "p1", "message": "one" }));
	harness.wait_for(
		|frames| frames.iter().any(|frame| frame["type"] == "agent_end"),
		timeout(),
	);
	let bodies = requests
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(bodies.len(), 1, "{bodies:#?}");
	assert_eq!(bodies[0]["prompt_cache_key"], "explicit-cache-1");
	assert_eq!(bodies[0]["reasoning_effort"], "low");
	drop(harness);
}

/// Anthropic fast-mode rejection body (`matchesFastModeUnsupported` matches a
/// 400 mentioning `invalid_request_error`, the `speed` parameter and `not
/// support`).
const FAST_REJECTION: &str = r#"{"type":"error","error":{"type":"invalid_request_error","message":"speed is not supported for this model"}}"#;

const ANTHROPIC_TURN: &str = concat!(
	"event: message_start\n",
	"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n",
	"event: content_block_start\n",
	"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
	"event: content_block_delta\n",
	"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n",
	"event: content_block_stop\n",
	"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
	"event: message_delta\n",
	"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
	"event: message_stop\n",
	"data: {\"type\":\"message_stop\"}\n\n"
);

/// Serve two Anthropic turns. Each turn starts with a fast-mode 400 followed by
/// the downgraded 200 retry, so the even requests carry `speed: "fast"`.
fn spawn_anthropic_fast_rejection(requests: Arc<Mutex<Vec<Vec<u8>>>>) -> u16 {
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind anthropic");
	let port = listener.local_addr().expect("addr").port();
	thread::spawn(move || {
		for index in 0..4 {
			let (mut socket, _) = listener.accept().expect("accept anthropic");
			let request = read_request(&mut socket).expect("anthropic request");
			requests
				.lock()
				.unwrap_or_else(std::sync::PoisonError::into_inner)
				.push(request);
			if index % 2 == 0 {
				let _ = write!(
					socket,
					"HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
					FAST_REJECTION.len(),
					FAST_REJECTION
				);
			} else {
				let _ = write!(
					socket,
					"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
					ANTHROPIC_TURN.len(),
					ANTHROPIC_TURN
				);
			}
			let _ = socket.flush();
		}
	});
	port
}

fn anthropic_launch(port: u16) -> RpcLaunch {
	let mut launch = launch(port, "claude-priority", "anthropic");
	launch.config.providers.insert(
		"anthropic".to_string(),
		ProviderConfig {
			base_url: Some(format!("http://127.0.0.1:{port}/v1")),
			api_key:  Some("sk-test".to_string()),
			api:      Some("anthropic-messages".to_string()),
			..Default::default()
		},
	);
	let mut models = std::collections::BTreeMap::new();
	models.insert("claude-priority".to_string(), ModelInfo {
		provider: "anthropic".to_string(),
		api: Some("anthropic-messages".to_string()),
		max_tokens: Some(4096),
		..ModelInfo::default()
	});
	let mut catalog = Catalog::default();
	catalog.providers.insert("anthropic".to_string(), models);
	launch.catalog = Arc::new(catalog);
	launch
}

#[test]
fn fast_mode_toggle_rearms_priority_after_an_anthropic_downgrade() {
	let requests = Arc::new(Mutex::new(Vec::new()));
	let port = spawn_anthropic_fast_rejection(Arc::clone(&requests));
	let harness = Harness::start(anthropic_launch(port));

	// The real `/fast on` path (TS `setFastMode`), not a settings file.
	harness.send(json!({ "type": "set_fast_mode", "id": "f1", "enabled": true }));
	let frames = harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "f1"),
		timeout(),
	);
	let enabled = frames.iter().find(|frame| frame["id"] == "f1").expect("fast on");
	assert_eq!(enabled["data"]["enabled"], true, "tier selected before the turn");

	harness.send(json!({ "type": "prompt", "id": "p1", "message": "first" }));
	let frames = harness.wait_for(
		|frames| frames.iter().any(|frame| frame["type"] == "agent_end"),
		timeout(),
	);
	assert!(
		frames.iter().any(|frame| frame["type"] == "notice"
			&& frame["level"] == "warning"
			&& frame["message"].as_str().is_some_and(|text| text.contains("Priority/fast mode rejected"))),
		"the downgrade is announced on the protocol notice frame"
	);

	// Re-arm through the same toggle: fresh provider instance plus the restored
	// priority selection means the next request carries `speed: "fast"` again.
	harness.send(json!({ "type": "set_fast_mode", "id": "f2", "enabled": true }));
	let frames = harness.wait_for(
		|frames| frames.iter().any(|frame| frame["id"] == "f2"),
		timeout(),
	);
	let rearmed = frames.iter().find(|frame| frame["id"] == "f2").expect("fast rearm");
	assert_eq!(rearmed["data"]["enabled"], true, "toggle restores priority");

	harness.send(json!({ "type": "prompt", "id": "p2", "message": "second" }));
	let frames = harness.wait_for(
		|frames| {
			frames
				.iter()
				.filter(|frame| frame["type"] == "agent_end")
				.count()
				>= 2
		},
		timeout(),
	);
	assert!(frames.iter().any(|frame| frame["type"] == "agent_end"));

	let bodies = requests
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(bodies.len(), 4, "one rejected and one retried request per turn");
	let body = |raw: &[u8]| -> Value {
		let start = raw
			.windows(4)
			.position(|window| window == b"\r\n\r\n")
			.map_or(raw.len(), |head| head + 4);
		serde_json::from_slice(&raw[start..]).expect("anthropic json body")
	};
	assert_eq!(body(&bodies[0])["speed"].as_str(), Some("fast"), "first attempt requests fast mode");
	assert_eq!(body(&bodies[1])["speed"].as_str(), None, "the downgraded retry does not");
	assert_eq!(
		body(&bodies[2])["speed"].as_str(),
		Some("fast"),
		"an explicit toggle rearms fast mode after the downgrade"
	);
	assert_eq!(
		body(&bodies[3])["speed"].as_str(),
		None,
		"rejected again, retried without fast mode"
	);
}

/// RPC idle delivery and EOF must publish an advisor card that settles after
/// the primary turn, using the real `RpcMode`/`PiAgentRunner` harness.
#[test]
fn advisor_card_is_published_by_the_rpc_pump_before_eof() {
	let (port, _requests) = spawn_tool_model(vec![
		ToolScript { text: "done".to_string(), tool: None },
		ToolScript {
			text: String::new(),
			tool: Some(("advise", json!({"note": "late finding", "severity": "blocker"}))),
		},
		ToolScript { text: "noted".to_string(), tool: None },
	]);
	let root = std::env::temp_dir().join(format!(
		"omp-rpc-advisor-{}-{}",
		std::process::id(),
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap_or_default()
			.as_nanos()
	));
	fs::create_dir_all(root.join(".omp")).expect("project");
	fs::write(
		root.join(".omp/WATCHDOG.yml"),
		"advisors:\n  - name: reviewer\n    model: local/advisor\n    tools:\n      - read\n",
	)
	.expect("watchdog");
	let mut launch = launch(port, "local/mock-model", "local");
	launch.cwd = root.clone();
	launch.config.advisor.enabled = Some(true);
	launch.config.model_roles.insert("advisor".to_string(), "local/advisor".to_string());
	launch.config.model_roles.insert("slow".to_string(), "local/advisor".to_string());
	let mut models = std::collections::BTreeMap::new();
	for id in ["mock-model", "advisor"] {
		models.insert(
			id.to_string(),
			ModelInfo {
				provider: "local".to_string(),
				context_window: Some(128_000),
				..ModelInfo::default()
			},
		);
	}
	let mut catalog = Catalog::default();
	catalog.providers.insert("local".to_string(), models);
	launch.catalog = Arc::new(catalog);
	let harness = Harness::start(launch);
	harness.send(json!({"type": "prompt", "message": "check it"}));
	harness.wait_for(
		|frames| {
			frames.iter().any(|frame| {
				frame["type"] == "message_end"
					&& frame["message"]["customType"] == "advisor"
			})
		},
		timeout(),
	);
	assert!(harness.join().is_ok(), "EOF must publish the settled card");
}
