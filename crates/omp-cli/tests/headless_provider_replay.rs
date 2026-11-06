//! Headless provider-context boundaries owned by `main.rs`:
//! - a journaled turn persists its streamed blocks in order (thinking, text,
//!   tool call) without a duplicated `tool_calls` projection, and a resumed run
//!   replays that call exactly once;
//! - a stored conversation that cannot be rebuilt losslessly fails loudly
//!   before any provider request instead of replaying a degraded history.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcCommand, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

static COUNTER: AtomicU32 = AtomicU32::new(0);

const DEFAULT_SSE: &str =
	"data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";

fn temp_dir(prefix: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"omp-headless-replay-{prefix}-{}-{}",
		std::process::id(),
		COUNTER.fetch_add(1, Ordering::Relaxed)
	));
	fs::create_dir_all(&dir).expect("create temp dir");
	dir
}

struct MockWireServer {
	port: u16,
	requests: Arc<Mutex<Vec<serde_json::Value>>>,
	authorizations: Arc<Mutex<Vec<Option<String>>>>,
	stop_tx: mpsc::Sender<()>,
	handle: Option<thread::JoinHandle<()>>,
}

impl MockWireServer {
	fn start(bodies: Vec<String>) -> Self {
		let listener = TcpListener::bind("127.0.0.1:0").expect("bind tcp");
		let port = listener.local_addr().expect("local addr").port();
		listener.set_nonblocking(true).expect("set nonblocking");
		let (stop_tx, stop_rx) = mpsc::channel::<()>();
		let requests = Arc::new(Mutex::new(Vec::new()));
		let reqs_clone = Arc::clone(&requests);
		let authorizations = Arc::new(Mutex::new(Vec::new()));
		let auths_clone = Arc::clone(&authorizations);
		let bodies = Arc::new(bodies);

		let handle = thread::spawn(move || {
			loop {
				if stop_rx.try_recv().is_ok() {
					break;
				}
				match listener.accept() {
					Ok((mut stream, _)) => {
						let mut buf = Vec::new();
						let mut temp = [0u8; 4096];
						let mut header_pos = None;
						while header_pos.is_none() {
							match stream.read(&mut temp) {
								Ok(0) => break,
								Ok(n) => {
									buf.extend_from_slice(&temp[..n]);
									if let Some(pos) =
										buf.windows(4).position(|w| w == b"\r\n\r\n")
									{
										header_pos = Some(pos);
									}
								},
								Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
									thread::sleep(Duration::from_millis(5));
								},
								Err(_) => break,
							}
						}
						if let Some(pos) = header_pos {
							let head = String::from_utf8_lossy(&buf[..pos]);
							let authorization = head.lines().find_map(|line| {
								let (name, value) = line.split_once(':')?;
								name.eq_ignore_ascii_case("authorization")
									.then(|| value.trim().to_string())
							});
							let len: usize = head
								.lines()
								.find(|line| {
									line.to_ascii_lowercase().starts_with("content-length:")
								})
								.and_then(|line| line.split(':').nth(1))
								.and_then(|value| value.trim().parse().ok())
								.unwrap_or(0);
							while buf.len() < pos + 4 + len {
								match stream.read(&mut temp) {
									Ok(0) => break,
									Ok(n) => buf.extend_from_slice(&temp[..n]),
									Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
										thread::sleep(Duration::from_millis(5));
									},
									Err(_) => break,
								}
							}
							if buf.len() >= pos + 4 + len
								&& let Ok(json) =
									serde_json::from_slice::<serde_json::Value>(&buf[pos + 4..pos + 4 + len])
							{
								reqs_clone.lock().expect("lock").push(json);
								auths_clone
									.lock()
									.expect("lock")
									.push(authorization.clone());
							}
						}
						let index = reqs_clone.lock().expect("lock").len().saturating_sub(1);
						let body = bodies
							.get(index)
							.or_else(|| bodies.last())
							.map(String::as_str)
							.unwrap_or(DEFAULT_SSE);
						let response = format!(
							"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
							body.len(),
							body
						);
						let _ = stream.write_all(response.as_bytes());
						let _ = stream.flush();
					},
					Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
						thread::sleep(Duration::from_millis(10));
					},
					Err(_) => break,
				}
			}
		});

		Self { port, requests, authorizations, stop_tx, handle: Some(handle) }
	}

	fn authorization(&self, index: usize) -> Option<String> {
		self.authorizations
			.lock()
			.expect("lock")
			.get(index)
			.cloned()
			.flatten()
	}

	fn request_count(&self) -> usize {
		self.requests.lock().expect("lock").len()
	}

	fn request(&self, index: usize) -> serde_json::Value {
		self.requests
			.lock()
			.expect("lock")
			.get(index)
			.cloned()
			.unwrap_or_else(|| panic!("mock request {index} not recorded"))
	}

	fn last_request(&self) -> serde_json::Value {
		self.requests
			.lock()
			.expect("lock")
			.last()
			.cloned()
			.expect("mock request recorded")
	}

	fn stop(mut self) {
		let _ = self.stop_tx.send(());
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}
	}
}

fn setup(port: u16, base: &Path) -> (PathBuf, PathBuf, PathBuf) {
	let agent_dir = base.join("agent");
	let cwd = base.join("work");
	let sessions = base.join("sessions");
	fs::create_dir_all(&agent_dir).expect("agent dir");
	fs::create_dir_all(&cwd).expect("cwd");
	fs::create_dir_all(&sessions).expect("sessions dir");
	fs::write(
		agent_dir.join("config.yml"),
		format!(
			"defaultModel: mock/mock-model\nproviders:\n  mock:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n"
		),
	)
	.expect("config.yml");
	fs::write(
		agent_dir.join("models.yml"),
		format!(
			"providers:\n  mock:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n    models:\n      - id: mock-model\n        contextWindow: 128000\n"
		),
	)
	.expect("models.yml");
	(agent_dir, cwd, sessions)
}

fn omp(agent_dir: &Path, cwd: &Path, sessions: &Path) -> ProcCommand {
	let mut cmd = ProcCommand::new(env!("CARGO_BIN_EXE_omp"));
	cmd.env("PI_CODING_AGENT_DIR", agent_dir);
	cmd.args([
		"--cwd",
		cwd.to_str().expect("utf8 cwd"),
		"--session-dir",
		sessions.to_str().expect("utf8 sessions"),
		"--no-title",
	])
	.stdin(Stdio::null());
	cmd
}

fn session_file(sessions: &Path, id: &str) -> PathBuf {
	let mut files: Vec<PathBuf> = fs::read_dir(sessions)
		.expect("session dir")
		.flatten()
		.map(|entry| entry.path())
		.filter(|path| {
			path.extension().and_then(|ext| ext.to_str()) == Some("jsonl")
				&& path
					.file_stem()
					.and_then(|stem| stem.to_str())
					.is_some_and(|stem| stem.contains(id))
		})
		.collect();
	files.sort();
	assert_eq!(files.len(), 1, "expected one log for {id}: {files:?}");
	files.remove(0)
}

fn entries(path: &Path) -> Vec<serde_json::Value> {
	fs::read_to_string(path)
		.expect("session log")
		.lines()
		.filter(|line| !line.trim().is_empty())
		.map(|line| serde_json::from_str(line).expect("valid session jsonl"))
		.collect()
}

fn run(cmd: &mut ProcCommand) -> Output {
	cmd.output().expect("run omp")
}

/// Count literal prompt-token occurrences in a recorded provider request.
fn occurrences(body: &serde_json::Value, needle: &str) -> usize {
	serde_json::to_string(body)
		.expect("serialize request")
		.matches(needle)
		.count()
}

#[test]
fn headless_turn_persists_ordered_blocks_and_resume_replays_call_once() {
	let tool_sse = r#"data: {"choices":[{"delta":{"reasoning_content":"REASON-TOKEN-9f"},"finish_reason":null}]}

data: {"choices":[{"delta":{"content":"checking "},"finish_reason":null}]}

data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"read","arguments":"{\"path\":\"note.txt\"}"}}]},"finish_reason":null}]}

data: [DONE]

"#;
	let server = MockWireServer::start(vec![
		tool_sse.to_string(),
		DEFAULT_SSE.to_string(),
		DEFAULT_SSE.to_string(),
	]);
	let base = temp_dir("ordered-blocks");
	let (agent_dir, cwd, sessions) = setup(server.port, &base);
	fs::write(cwd.join("note.txt"), "note body").expect("note");

	let first = run(
		omp(&agent_dir, &cwd, &sessions)
			.args(["--session-id", "replay-1", "-p", "PROMPT-TOKEN-3c read note.txt"]),
	);
	assert!(
		first.status.success(),
		"first headless turn failed: {}",
		String::from_utf8_lossy(&first.stderr)
	);
	assert_eq!(server.request_count(), 2, "one turn plus its tool follow-up");
	// The typed rebuild must describe the journal *before* this turn's user
	// entry is appended, otherwise the loop prompt lands a second time.
	assert_eq!(
		occurrences(&server.request(0), "PROMPT-TOKEN-3c"),
		1,
		"first request duplicated the current prompt"
	);
	assert_eq!(
		occurrences(&server.request(1), "PROMPT-TOKEN-3c"),
		1,
		"tool follow-up duplicated the current prompt"
	);

	let log = session_file(&sessions, "replay-1");
	let stored = entries(&log);
	let assistant = stored
		.iter()
		.find(|entry| entry["type"] == "message" && entry["message"]["role"] == "assistant")
		.expect("assistant entry");
	let parts = assistant["message"]["content"]
		.as_array()
		.expect("content must be ordered parts");
	let kinds: Vec<&str> = parts
		.iter()
		.map(|part| part["type"].as_str().unwrap_or_default())
		.collect();
	assert_eq!(
		kinds,
		["thinking", "text", "toolCall"],
		"streamed block order must persist as-is: {assistant}"
	);
	assert_eq!(
		parts
			.iter()
			.filter(|part| part["type"] == "toolCall")
			.count(),
		1,
		"the tool call must not be duplicated beside its ordered block: {assistant}"
	);
	assert_eq!(parts[2]["id"], "call-1");
	assert_eq!(parts[2]["name"], "read");
	assert!(parts[0]["thinking"].as_str().unwrap_or_default().contains("REASON-TOKEN-9f"));
	assert_eq!(assistant["message"]["api"], "openai-completions");
	assert_eq!(assistant["message"]["provider"], "mock");
	assert_eq!(assistant["message"]["model"], "mock-model");
	// The OpenAI adapter reports no terminal reason, so this is the event's
	// tool-use status, not a normalized `stop`.
	assert_eq!(assistant["message"]["stopReason"], "toolUse");
	assert!(
		assistant["message"].get("errorMessage").is_none(),
		"a clean stop must not carry an error message: {assistant}"
	);
	assert!(
		stored.iter().any(|entry| {
			entry["type"] == "message"
				&& entry["message"]["role"] == "toolResult"
				&& entry["message"]["toolCallId"] == "call-1"
		}),
		"tool result must follow its call: {stored:?}"
	);

	let second = run(
		omp(&agent_dir, &cwd, &sessions).args(["--resume", "replay-1", "-p", "RESUME-PROMPT-8a continue"]),
	);
	assert!(
		second.status.success(),
		"resumed headless turn failed: {}",
		String::from_utf8_lossy(&second.stderr)
	);

	let body = server.last_request();
	assert_eq!(
		occurrences(&body, "RESUME-PROMPT-8a"),
		1,
		"resume duplicated the new prompt: {body}"
	);
	assert_eq!(
		occurrences(&body, "PROMPT-TOKEN-3c"),
		1,
		"resume duplicated the replayed prompt: {body}"
	);
	let wire = serde_json::to_string(&body).expect("serialize request");
	assert_eq!(
		wire.matches("call-1").count(),
		2,
		"call id appears once as the call and once on its result: {body}"
	);
	let messages = body["messages"].as_array().expect("messages");
	let assistant = messages
		.iter()
		.find(|message| message["role"] == "assistant")
		.expect("replayed assistant");
	let calls = assistant["tool_calls"].as_array().expect("tool_calls");
	assert_eq!(calls.len(), 1, "resumed call must replay once: {assistant}");
	assert_eq!(calls[0]["id"], "call-1");
	assert!(
		messages
			.iter()
			.any(|message| message["role"] == "tool" && message["tool_call_id"] == "call-1"),
		"tool result must replay after the call: {body}"
	);

	// Resume must not downgrade the stored state to a text-only turn.
	let after = entries(&session_file(&sessions, "replay-1"));
	let assistant = after
		.iter()
		.find(|entry| entry["type"] == "message" && entry["message"]["role"] == "assistant")
		.expect("assistant entry after resume");
	assert_eq!(assistant["message"]["content"][0]["type"], "thinking");
	assert!(assistant.to_string().contains("REASON-TOKEN-9f"));

	server.stop();
	fs::remove_dir_all(base).ok();
}

#[test]
fn corrupt_stored_context_fails_before_any_provider_request() {
	let server = MockWireServer::start(vec![DEFAULT_SSE.to_string()]);
	let base = temp_dir("corrupt-context");
	let (agent_dir, cwd, sessions) = setup(server.port, &base);

	// A `toolCall` block without its name parses as a valid entry but cannot be
	// rebuilt losslessly, so the CLI must stop instead of replaying the turn.
	let log = sessions.join("2026-11-02T00-00-00-000Z_broken-context.jsonl");
	let lines = [
		serde_json::json!({
			"type": "session",
			"version": 3,
			"id": "broken-context",
			"timestamp": "2026-11-02T00:00:00.000Z",
			"cwd": cwd.to_str().expect("cwd"),
		})
		.to_string(),
		serde_json::json!({
			"type": "message",
			"id": "msg-u",
			"parentId": null,
			"timestamp": "2026-11-02T00:00:01.000Z",
			"message": {
				"role": "user",
				"content": [{ "type": "text", "text": "prior turn" }],
				"timestamp": 1000,
			},
		})
		.to_string(),
		serde_json::json!({
			"type": "message",
			"id": "msg-a",
			"parentId": "msg-u",
			"timestamp": "2026-11-02T00:00:02.000Z",
			"message": {
				"role": "assistant",
				"content": [{ "type": "toolCall", "id": "call-1", "arguments": { "path": "note.txt" } }],
				"api": "openai-completions",
				"provider": "mock",
				"model": "mock-model",
				"stopReason": "toolUse",
				"timestamp": 2000,
			},
		})
		.to_string(),
	];
	fs::write(&log, format!("{}\n", lines.join("\n"))).expect("write corrupt log");

	let output = run(
		omp(&agent_dir, &cwd, &sessions).args(["--resume", "broken-context", "-p", "CORRUPT-PROMPT-5e"]),
	);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(
		!output.status.success(),
		"a corrupt stored conversation must fail the turn: {stderr}"
	);
	assert!(
		stderr.contains("toolCall part missing name"),
		"failure must name the unreplayable block: {stderr}"
	);
	assert!(
		!stderr.contains("cannot rebuild branch context"),
		"the turn must not continue after a downgraded rebuild: {stderr}"
	);
	assert_eq!(
		server.request_count(),
		0,
		"the provider must never be called with degraded context"
	);
	assert!(
		!entries(&log)
			.iter()
			.any(|entry| entry.to_string().contains("CORRUPT-PROMPT-5e")),
		"the request must be built before the journal gains this turn's prompt"
	);

	server.stop();
	fs::remove_dir_all(base).ok();
}

#[tokio::test]
async fn rewind_rebuild_failure_aborts_before_another_provider_request() {
	// One real checkpoint+rewind tool pair in the first turn: the turn-end
	// handler applies the pending rewind, then cannot rebuild the branch.
	let tool_sse = r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-cp","type":"function","function":{"name":"checkpoint","arguments":"{\"goal\":\"probe\"}"}}]},"finish_reason":null}]}

data: {"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call-rw","type":"function","function":{"name":"rewind","arguments":"{\"report\":\"probe report\"}"}}]},"finish_reason":null}]}

data: [DONE]

"#;
	let server = MockWireServer::start(vec![tool_sse.to_string(), DEFAULT_SSE.to_string()]);
	let base = temp_dir("rewind-failure");
	let (agent_dir, cwd, sessions) = setup(server.port, &base);
	let mut config = fs::read_to_string(agent_dir.join("config.yml")).expect("read config");
	config.push_str("checkpoint:\n  enabled: true\n");
	fs::write(agent_dir.join("config.yml"), config).expect("enable checkpoint");

	let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_omp"));
	command
		.env("PI_CODING_AGENT_DIR", &agent_dir)
		.env("OMP_TEST_FAIL_REWIND_REBUILD", "1")
		.args([
			"--cwd",
			cwd.to_str().expect("utf8 cwd"),
			"--session-dir",
			sessions.to_str().expect("utf8 sessions"),
			"--no-title",
			"--yolo",
			"--session-id",
			"rewind-1",
			"-p",
			"checkpoint then rewind",
		])
		.stdin(std::process::Stdio::null());
	let output = tokio::time::timeout(Duration::from_secs(30), command.output())
		.await
		.expect("a failed rewind rebuild must not park the turn-end ack")
		.expect("run omp");
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(
		!output.status.success(),
		"a failed rewind rebuild must fail the turn: {stderr}"
	);
	assert!(
		stderr.contains("cannot rebuild branch context after rewind"),
		"the failure must be reported: {stderr}"
	);
	assert!(
		stderr.contains("injected rewind rebuild failure"),
		"the debug injection must be the cause: {stderr}"
	);
	assert_eq!(
		server.request_count(),
		1,
		"no provider request may start after the failed rewind rebuild"
	);

	server.stop();
	fs::remove_dir_all(base).ok();
}

// ---------------------------------------------------------------------------
// Prewalk: one-way model switch at the first implementation action.
// ---------------------------------------------------------------------------

fn text_sse(text: &str) -> String {
	format!(
		"data: {}\n\ndata: [DONE]\n\n",
		serde_json::json!({"choices":[{"delta":{"content":text},"finish_reason":"stop"}]})
	)
}

fn tool_sse(calls: &[(&str, &str, serde_json::Value)]) -> String {
	let mut body = String::new();
	for (index, (id, name, args)) in calls.iter().enumerate() {
		let call = serde_json::json!({
			"index": index,
			"id": id,
			"type": "function",
			"function": {"name": name, "arguments": args.to_string()},
		});
		body.push_str(&format!(
			"data: {}\n\n",
			serde_json::json!({"choices":[{"delta":{"tool_calls":[call]},"finish_reason":null}]})
		));
	}
	body.push_str("data: [DONE]\n\n");
	body
}

/// Two independent provider endpoints so a model switch is observable by which
/// server receives the next request.
fn setup_two_providers(
	base: &Path,
	a_port: u16,
	b_port: u16,
	extra_config: &str,
) -> (PathBuf, PathBuf, PathBuf) {
	let agent_dir = base.join("agent");
	let cwd = base.join("work");
	let sessions = base.join("sessions");
	fs::create_dir_all(&agent_dir).expect("agent dir");
	fs::create_dir_all(&cwd).expect("cwd");
	fs::create_dir_all(&sessions).expect("sessions dir");
	let mut config = format!(
		"defaultModel: mockA/model-a\nproviders:\n  mockA:\n    baseUrl: \"http://127.0.0.1:{a_port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n  mockB:\n    baseUrl: \"http://127.0.0.1:{b_port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n"
	);
	config.push_str(extra_config);
	fs::write(agent_dir.join("config.yml"), config).expect("config.yml");
	fs::write(
		agent_dir.join("models.yml"),
		format!(
			"providers:\n  mockA:\n    baseUrl: \"http://127.0.0.1:{a_port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n    models:\n      - id: model-a\n        contextWindow: 128000\n  mockB:\n    baseUrl: \"http://127.0.0.1:{b_port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n    models:\n      - id: model-b\n        contextWindow: 128000\n"
		),
	)
	.expect("models.yml");
	(agent_dir, cwd, sessions)
}

/// One provider id with two per-model `baseUrl`s: the root key must not follow
/// the switch to the alternate origin.
fn setup_same_provider_two_endpoints(
	base: &Path,
	a_port: u16,
	b_port: u16,
) -> (PathBuf, PathBuf, PathBuf) {
	let agent_dir = base.join("agent");
	let cwd = base.join("work");
	let sessions = base.join("sessions");
	fs::create_dir_all(&agent_dir).expect("agent dir");
	fs::create_dir_all(&cwd).expect("cwd");
	fs::create_dir_all(&sessions).expect("sessions dir");
	fs::write(
		agent_dir.join("config.yml"),
		"defaultModel: mockA/model-a\nproviders:\n  mockA:\n    api: openai-completions\n    apiKey: target-key\n",
	)
	.expect("config.yml");
	fs::write(
		agent_dir.join("models.yml"),
		format!(
			"providers:\n  mockA:\n    api: openai-completions\n    apiKey: target-key\n    models:\n      - id: model-a\n        contextWindow: 128000\n        baseUrl: \"http://127.0.0.1:{a_port}/v1\"\n      - id: model-b\n        contextWindow: 128000\n        baseUrl: \"http://127.0.0.1:{b_port}/v1\"\n        reasoning: true\n        thinking:\n          efforts: [\"low\", \"high\"]\n          defaultLevel: max\n      - id: model-c\n        contextWindow: 128000\n        baseUrl: \"http://127.0.0.1:{b_port}/v1\"\n        reasoning: true\n        thinking:\n          efforts: [\"low\", \"high\"]\n          defaultLevel: auto\n"
		),
	)
	.expect("models.yml");
	(agent_dir, cwd, sessions)
}

fn selectable_extra() -> &'static str {
	"modelRoles:\n  smol: mockB/model-b\nprewalk:\n  enabled: true\n"
}

#[test]
fn prewalk_switches_to_smol_after_todo_and_first_write_then_resumes_on_default() {
	let a = MockWireServer::start(vec![
		text_sse("exploring"),
		tool_sse(&[(
			"call-todo",
			"todo",
			serde_json::json!({"op": "init", "list": [{"phase": "P", "items": ["Do it"]}]}),
		)]),
		tool_sse(&[(
			"call-write",
			"write",
			serde_json::json!({"path": "probe.txt", "content": "done"}),
		)]),
		text_sse("resumed follow-up"),
	]);
	let b = MockWireServer::start(vec![text_sse("switched response")]);
	let base = temp_dir("prewalk-switch");
	let (agent_dir, cwd, sessions) = setup_two_providers(&base, a.port, b.port, selectable_extra());

	let first = run(
		omp(&agent_dir, &cwd, &sessions)
			.args([
				"--yolo",
				"--prewalk",
				"--session-id",
				"prewalk-1",
				"-p",
				"PLAN-TOKEN start",
			]),
	);
	assert!(
		first.status.success(),
		"prewalk run failed: {}",
		String::from_utf8_lossy(&first.stderr)
	);
	assert_eq!(a.request_count(), 3, "three primary turns before the switch");
	assert_eq!(b.request_count(), 1, "the switched request reaches the fast endpoint");
	// The plan nudge is injected after the first text-only turn and reaches the
	// primary model, never the fast one.
	assert!(
		occurrences(&a.request(1), "STOP: In NEXT reply") >= 1,
		"primary follow-up must carry the deep-plan nudge"
	);
	let fast = b.request(0);
	assert_eq!(fast["model"], "model-b");
	assert_eq!(
		occurrences(&fast, "Before task complete, verify:"),
		1,
		"fast request carries exactly one checklist"
	);
	assert_eq!(
		occurrences(&fast, "STOP: In NEXT reply"),
		0,
		"consumed plan nudge must be scrubbed before the switch"
	);

	let log = session_file(&sessions, "prewalk-1");
	let stored = entries(&log);
	let model_change = stored
		.iter()
		.position(|entry| entry["type"] == "model_change")
		.expect("ephemeral model change must be journaled");
	assert_eq!(stored[model_change]["model"], "mockB/model-b");
	assert_eq!(
		stored[model_change]["role"], "fallback",
		"prewalk must use the ephemeral role, not `temporary`"
	);
	let checklist = stored
		.iter()
		.position(|entry| {
			entry["type"] == "custom_message" && entry["customType"] == "prewalk-checklist"
		})
		.expect("checklist guidance must be journaled");
	assert!(
		model_change < checklist,
		"model change is recorded before the checklist it introduces"
	);
	assert!(
		stored.iter().all(|entry| entry["customType"] != "prewalk-plan"),
		"the one-run plan nudge must never be persisted"
	);

	// Resume restores the default model (`fallback` is ephemeral) while the
	// persisted checklist still replays as hidden guidance.
	let resumed = run(
		omp(&agent_dir, &cwd, &sessions)
			.args(["--resume", "prewalk-1", "-p", "RESUME-TOKEN again"]),
	);
	assert!(
		resumed.status.success(),
		"resume failed: {}",
		String::from_utf8_lossy(&resumed.stderr)
	);
	assert_eq!(a.request_count(), 4, "resume stays on the default provider");
	assert_eq!(b.request_count(), 1, "an ephemeral switch must not survive resume");
	let replay = a.request(3);
	assert_eq!(occurrences(&replay, "RESUME-TOKEN"), 1);
	assert!(
		occurrences(&replay, "Before task complete, verify:") >= 1,
		"checklist guidance replays on resume"
	);

	a.stop();
	b.stop();
	fs::remove_dir_all(base).ok();
}

#[test]
fn plan_yolo_guards_then_switches_and_resumes_on_default() {
	let a = MockWireServer::start(vec![
		tool_sse(&[(
			"call-blocked",
			"write",
			serde_json::json!({"path": "blocked.txt", "content": "must not land"}),
		)]),
		tool_sse(&[(
			"call-plan",
			"write",
			serde_json::json!({
				"path": "local://plan-plan.md",
				"content": "# Plan\n\nImplement probe.\n"
			}),
		)]),
		tool_sse(&[(
			"call-propose",
			"write",
			serde_json::json!({"path": "xd://propose", "content": "Plan"}),
		)]),
		text_sse("resumed"),
	]);
	let b = MockWireServer::start(vec![
		tool_sse(&[(
			"call-probe",
			"write",
			serde_json::json!({"path": "probe.txt", "content": "done"}),
		)]),
		text_sse("implemented"),
	]);
	let base = temp_dir("plan-yolo-switch");
	let (agent_dir, cwd, sessions) =
		setup_two_providers(&base, a.port, b.port, "tools: [write, read]\n");
	let first = run(
		omp(&agent_dir, &cwd, &sessions).args([
			"--yolo",
			"--plan-yolo",
			"--plan-yolo-into",
			"mockB/model-b",
			"--session-id",
			"plan-yolo-1",
			"-p",
			"PLAN-YOLO-TOKEN",
		]),
	);
	assert!(
		first.status.success(),
		"plan-yolo run failed: {}",
		String::from_utf8_lossy(&first.stderr)
	);
	assert_eq!(a.request_count(), 3, "guard, plan write, then proposal");
	assert_eq!(b.request_count(), 2, "switch happens before implementation");
	assert!(!cwd.join("blocked.txt").exists(), "guard must block working-tree writes");
	let plan = b.request(0);
	assert_eq!(plan["model"], "model-b");
	assert_eq!(
		occurrences(&plan, "Plan approved: **Plan**"),
		1,
		"the typed plan-yolo handoff is injected exactly once"
	);
	let handoff = plan["messages"]
		.as_array()
		.expect("messages")
		.iter()
		.find(|message| {
			message
				.to_string()
				.contains("Plan approved: **Plan**")
		})
		.expect("plan-yolo handoff message");
	assert_eq!(
		occurrences(handoff, "local://plan-plan.md"),
		1,
		"the handoff points at the plan once; history may legitimately repeat the path"
	);
	assert_eq!(fs::read_to_string(cwd.join("probe.txt")).expect("probe"), "done");

	let log = session_file(&sessions, "plan-yolo-1");
	let stored = entries(&log);
	let change = stored
		.iter()
		.position(|entry| entry["type"] == "model_change")
		.expect("plan-yolo switch must be journaled");
	assert_eq!(stored[change]["model"], "mockB/model-b");
	assert_eq!(stored[change]["role"], "fallback");

	let resumed = run(
		omp(&agent_dir, &cwd, &sessions).args(["--resume", "plan-yolo-1", "-p", "RESUME-TOKEN"]),
	);
	assert!(
		resumed.status.success(),
		"resume failed: {}",
		String::from_utf8_lossy(&resumed.stderr)
	);
	assert_eq!(a.request_count(), 4, "resume restores the default provider");
	assert_eq!(b.request_count(), 2, "the ephemeral plan target must not survive");
	assert_eq!(a.request(3)["model"], "model-a");
	assert!(
		!entries(&log)
			.iter()
			.any(|entry| entry["customType"] == "plan-yolo-handoff"),
		"the live handoff steer is not persisted as a custom message"
	);

	a.stop();
	b.stop();
	fs::remove_dir_all(base).ok();
}

#[test]
fn prewalk_noop_target_disarms_with_a_notice() {
	let a = MockWireServer::start(vec![text_sse("working")]);
	let b = MockWireServer::start(vec![text_sse("unused")]);
	let base = temp_dir("prewalk-noop");
	let (agent_dir, cwd, sessions) = setup_two_providers(
		&base,
		a.port,
		b.port,
		"modelRoles:\n  smol: mockA/model-a\nprewalk:\n  enabled: true\n",
	);
	let run_out = run(
		omp(&agent_dir, &cwd, &sessions)
			.args(["--session-id", "prewalk-noop", "-p", "hi"]),
	);
	assert!(
		run_out.status.success(),
		"noop run failed: {}",
		String::from_utf8_lossy(&run_out.stderr)
	);
	assert_eq!(a.request_count(), 1);
	assert_eq!(b.request_count(), 0);
	assert!(
		String::from_utf8_lossy(&run_out.stderr).contains("nothing to switch"),
		"noop target must announce the disarm"
	);
	let stored = entries(&session_file(&sessions, "prewalk-noop"));
	assert!(stored.iter().all(|entry| entry["type"] != "model_change"));
	assert!(
		stored
			.iter()
			.all(|entry| entry["customType"] != "prewalk-plan"),
		"a noop target injects no plan nudge"
	);

	a.stop();
	b.stop();
	fs::remove_dir_all(base).ok();
}

#[test]
fn prewalk_read_only_tools_do_not_switch() {
	let a = MockWireServer::start(vec![
		text_sse("exploring"),
		tool_sse(&[(
			"call-todo",
			"todo",
			serde_json::json!({"op": "init", "list": [{"phase": "P", "items": ["Read"]}]}),
		)]),
		tool_sse(&[(
			"call-read",
			"read",
			serde_json::json!({"path": "note.txt"}),
		)]),
		text_sse("still exploring"),
		text_sse("done"),
	]);
	let b = MockWireServer::start(vec![text_sse("must stay unused")]);
	let base = temp_dir("prewalk-readonly");
	let (agent_dir, cwd, sessions) =
		setup_two_providers(&base, a.port, b.port, selectable_extra());
	fs::write(cwd.join("note.txt"), "note body").expect("note");

	let run_out = run(
		omp(&agent_dir, &cwd, &sessions)
			.args(["--session-id", "prewalk-readonly", "-p", "read first"]),
	);
	assert!(
		run_out.status.success(),
		"readonly run failed: {}",
		String::from_utf8_lossy(&run_out.stderr)
	);
	assert_eq!(a.request_count(), 5, "primary keeps the dialog after reads");
	assert_eq!(b.request_count(), 0, "read-only tools must never switch");
	let stored = entries(&session_file(&sessions, "prewalk-readonly"));
	assert!(stored.iter().all(|entry| entry["type"] != "model_change"));
	assert!(
		stored.iter().any(|entry| entry["customType"] == "prewalk-continue"),
		"the tool-free turn consumes the pending continue nudge"
	);
	assert!(
		stored
			.iter()
			.all(|entry| entry["customType"] != "prewalk-checklist"),
		"no checklist without a switch"
	);

	a.stop();
	b.stop();
	fs::remove_dir_all(base).ok();
}

#[test]
fn prewalk_conflict_exits_before_any_provider_request() {
	let a = MockWireServer::start(vec![text_sse("unused")]);
	let b = MockWireServer::start(vec![text_sse("unused")]);
	let base = temp_dir("prewalk-conflict");
	let (agent_dir, cwd, sessions) = setup_two_providers(&base, a.port, b.port, "");
	let run_out = run(
		omp(&agent_dir, &cwd, &sessions).args(["--no-prewalk", "--prewalk", "-p", "hi"]),
	);
	assert!(!run_out.status.success());
	assert!(
		String::from_utf8_lossy(&run_out.stderr)
			.contains("--no-prewalk cannot be combined with --prewalk or --prewalk-into"),
		"conflict must fail with the TS message: {}",
		String::from_utf8_lossy(&run_out.stderr)
	);
	assert_eq!(a.request_count(), 0);
	assert_eq!(b.request_count(), 0);

	a.stop();
	b.stop();
	fs::remove_dir_all(base).ok();
}

#[test]
fn prewalk_unresolved_target_warns_and_stays_disabled() {
	let a = MockWireServer::start(vec![text_sse("working")]);
	let b = MockWireServer::start(vec![text_sse("unused")]);
	let base = temp_dir("prewalk-unresolved");
	let (agent_dir, cwd, sessions) = setup_two_providers(&base, a.port, b.port, "");
	let run_out = run(
		omp(&agent_dir, &cwd, &sessions).args([
			"--prewalk",
			"--prewalk-into",
			"model-absent",
			"--session-id",
			"prewalk-unresolved",
			"-p",
			"hi",
		]),
	);
	assert!(
		run_out.status.success(),
		"an optional prewalk target must not abort startup: {}",
		String::from_utf8_lossy(&run_out.stderr)
	);
	assert!(
		String::from_utf8_lossy(&run_out.stderr).contains("prewalk disabled —"),
		"unresolved target must warn: {}",
		String::from_utf8_lossy(&run_out.stderr)
	);
	assert_eq!(a.request_count(), 1);
	assert_eq!(b.request_count(), 0);
	let stored = entries(&session_file(&sessions, "prewalk-unresolved"));
	assert!(stored.iter().all(|entry| entry["type"] != "model_change"));

	a.stop();
	b.stop();
	fs::remove_dir_all(base).ok();
}

#[test]
fn prewalk_config_setting_is_ignored_on_resume() {
	let a = MockWireServer::start(vec![
		text_sse("first session"),
		tool_sse(&[(
			"call-write",
			"write",
			serde_json::json!({"path": "probe.txt", "content": "x"}),
		)]),
		text_sse("resumed without prewalk"),
	]);
	let b = MockWireServer::start(vec![text_sse("must stay unused")]);
	let base = temp_dir("prewalk-restore");
	let (agent_dir, cwd, sessions) =
		setup_two_providers(&base, a.port, b.port, selectable_extra());

	let first = run(
		omp(&agent_dir, &cwd, &sessions)
			.args(["--no-prewalk", "--session-id", "prewalk-restore", "-p", "first"]),
	);
	assert!(
		first.status.success(),
		"first run failed: {}",
		String::from_utf8_lossy(&first.stderr)
	);
	assert_eq!(a.request_count(), 1);
	assert_eq!(b.request_count(), 0);

	let resumed = run(
		omp(&agent_dir, &cwd, &sessions)
			.args(["--resume", "prewalk-restore", "--yolo", "-p", "second"]),
	);
	assert!(
		resumed.status.success(),
		"resume failed: {}",
		String::from_utf8_lossy(&resumed.stderr)
	);
	assert!(
		!String::from_utf8_lossy(&resumed.stderr).contains("Prewalk:"),
		"settings enablement must not arm a restored session"
	);
	assert_eq!(b.request_count(), 0, "restored session stays on the default model");
	let stored = entries(&session_file(&sessions, "prewalk-restore"));
	assert!(stored.iter().all(|entry| entry["type"] != "model_change"));

	a.stop();
	b.stop();
	fs::remove_dir_all(base).ok();
}

#[test]
fn prewalk_explicit_into_switches_on_first_write_when_todo_is_not_offered() {
	let a = MockWireServer::start(vec![tool_sse(&[(
		"call-write",
		"write",
		serde_json::json!({"path": "probe.txt", "content": "x"}),
	)])]);
	let b = MockWireServer::start(vec![text_sse("switched immediately")]);
	let base = temp_dir("prewalk-explicit");
	let (agent_dir, cwd, sessions) = setup_two_providers(&base, a.port, b.port, "");
	let run_out = run(
		omp(&agent_dir, &cwd, &sessions).args([
			"--yolo",
			"--tools",
			"write,read",
			"--prewalk-into",
			"mockB/model-b",
			"--session-id",
			"prewalk-explicit",
			"-p",
			"write now",
		]),
	);
	assert!(
		run_out.status.success(),
		"explicit prewalk run failed: {}",
		String::from_utf8_lossy(&run_out.stderr)
	);
	assert_eq!(a.request_count(), 1, "no todo gate when todo is not offered");
	assert_eq!(b.request_count(), 1);
	let fast = b.request(0);
	assert_eq!(fast["model"], "model-b");
	assert_eq!(occurrences(&fast, "Before task complete, verify:"), 1);
	assert_eq!(
		occurrences(&fast, "STOP: In NEXT reply"),
		0,
		"an immediate switch never injects the plan nudge"
	);
	let stored = entries(&session_file(&sessions, "prewalk-explicit"));
	assert!(stored.iter().any(|entry| {
		entry["type"] == "model_change" && entry["role"] == "fallback"
	}));

	a.stop();
	b.stop();
	fs::remove_dir_all(base).ok();
}

/// Same provider id, different effective endpoints: the explicit root secret
/// must never reach the alternate origin; the independently configured target
/// key must.
#[test]
fn prewalk_does_not_carry_the_root_secret_to_an_alternate_endpoint() {
	let a = MockWireServer::start(vec![tool_sse(&[(
		"call-write",
		"write",
		serde_json::json!({"path": ".", "content": "x"}),
	)])]);
	let b = MockWireServer::start(vec![text_sse("alternate endpoint")]);
	let base = temp_dir("prewalk-alternate-endpoint");
	let (agent_dir, cwd, sessions) =
		setup_same_provider_two_endpoints(&base, a.port, b.port);
	let run_out = run(
		omp(&agent_dir, &cwd, &sessions).args([
			"--yolo",
			"--tools",
			"write,read",
			"--api-key",
			"root-secret",
			"--prewalk-into",
			"mockA/model-b",
			"--session-id",
			"prewalk-alternate-endpoint",
			"-p",
			"write now",
		]),
	);
	assert!(
		run_out.status.success(),
		"alternate-endpoint run failed: {}",
		String::from_utf8_lossy(&run_out.stderr)
	);
	assert_eq!(a.request_count(), 1);
	assert_eq!(b.request_count(), 1, "the switch must reach the alternate endpoint");
	assert!(
		a.authorization(0)
			.as_deref()
			.is_some_and(|value| value.contains("root-secret")),
		"root request keeps its explicit key: {:?}",
		a.authorization(0)
	);
	let target_auth = b.authorization(0).expect("target authorization header");
	assert!(
		target_auth.contains("target-key"),
		"target endpoint must resolve its own configured key: {target_auth}"
	);
	assert!(
		!target_auth.contains("root-secret"),
		"root secret leaked to the alternate endpoint: {target_auth}"
	);
	assert_eq!(
		b.request(0)["reasoning_effort"], "high",
		"target without a suffix must apply its configured defaultLevel"
	);

	a.stop();
	b.stop();
	fs::remove_dir_all(base).ok();
}

/// An explicit `:suffix` wins over the target model's configured default.
#[test]
fn prewalk_explicit_suffix_overrides_the_target_default_level() {
	let a = MockWireServer::start(vec![tool_sse(&[(
		"call-write",
		"write",
		serde_json::json!({"path": ".", "content": "x"}),
	)])]);
	let b = MockWireServer::start(vec![text_sse("suffix endpoint")]);
	let base = temp_dir("prewalk-suffix-effort");
	let (agent_dir, cwd, sessions) =
		setup_same_provider_two_endpoints(&base, a.port, b.port);
	let run_out = run(
		omp(&agent_dir, &cwd, &sessions).args([
			"--yolo",
			"--tools",
			"write,read",
			"--prewalk-into",
			"mockA/model-b:low",
			"--session-id",
			"prewalk-suffix-effort",
			"-p",
			"write now",
		]),
	);
	assert!(
		run_out.status.success(),
		"suffix-effort run failed: {}",
		String::from_utf8_lossy(&run_out.stderr)
	);
	assert_eq!(b.request_count(), 1);
	assert_eq!(
		b.request(0)["reasoning_effort"], "low",
		"the explicit suffix must win over defaultLevel=high"
	);

	a.stop();
	b.stop();
	fs::remove_dir_all(base).ok();
}

/// `defaultLevel: auto` must enter the same auto state and produce the same
/// request shape as an explicit `:auto` suffix (no fixed effort).
#[test]
fn prewalk_default_auto_matches_explicit_auto() {
	let run_target = |target: &str| -> (serde_json::Value, String, String) {
		let a = MockWireServer::start(vec![tool_sse(&[(
			"call-write",
			"write",
			serde_json::json!({"path": "probe.txt", "content": "x"}),
		)])]);
		let b = MockWireServer::start(vec![text_sse("auto endpoint")]);
		let base = temp_dir("prewalk-auto");
		let (agent_dir, cwd, sessions) =
			setup_same_provider_two_endpoints(&base, a.port, b.port);
		let run_out = run(
			omp(&agent_dir, &cwd, &sessions).args([
				"--yolo",
				"--tools",
				"write,read",
				"--prewalk-into",
				target,
				"--session-id",
				"prewalk-auto",
				"-p",
				"write now",
			]),
		);
		assert!(
			run_out.status.success(),
			"auto run failed for {target}: {}",
			String::from_utf8_lossy(&run_out.stderr)
		);
		assert_eq!(b.request_count(), 1, "target request for {target}");
		let body = b.request(0);
		let stderr = String::from_utf8_lossy(&run_out.stderr).to_string();
		let root = base.to_string_lossy().to_string();
		a.stop();
		b.stop();
		fs::remove_dir_all(base).ok();
		(body, stderr, root)
	};

	let (default_body, default_stderr, default_root) = run_target("mockA/model-c");
	let (explicit_body, explicit_stderr, explicit_root) =
		run_target("mockA/model-c:auto");
	assert!(
		default_stderr.contains("Prewalk: switched"),
		"{default_stderr}"
	);
	assert!(
		explicit_stderr.contains("Prewalk: switched"),
		"{explicit_stderr}"
	);
	assert_eq!(default_body["model"], explicit_body["model"]);
	// Success tool results still name their absolute target; normalize only the
	// fixture's own known temp root, then compare the full wire conversation.
	let default_messages = default_body["messages"]
		.to_string()
		.replace(&default_root, "<FIXTURE_ROOT>");
	let explicit_messages = explicit_body["messages"]
		.to_string()
		.replace(&explicit_root, "<FIXTURE_ROOT>");
	assert_eq!(
		default_messages, explicit_messages,
		"default auto and explicit auto must carry the same conversation"
	);
	assert!(
		default_body.get("reasoning_effort").is_none(),
		"auto is not a fixed effort: {default_body}"
	);
	assert!(
		explicit_body.get("reasoning_effort").is_none(),
		"explicit auto is not a fixed effort: {explicit_body}"
	);
}

/// The action check keys on the tool, not its success flag. A write that fails
/// (`is_error: true`) still arms the hand-off after `todo` was unavailable.
#[test]
fn prewalk_switches_after_a_failed_write() {
	let a = MockWireServer::start(vec![tool_sse(&[(
		"call-write",
		"write",
		serde_json::json!({"path": ".", "content": "x"}),
	)])]);
	let b = MockWireServer::start(vec![text_sse("switched after the failed write")]);
	let base = temp_dir("prewalk-failed-write");
	let (agent_dir, cwd, sessions) = setup_two_providers(&base, a.port, b.port, "");
	let run_out = run(
		omp(&agent_dir, &cwd, &sessions).args([
			"--yolo",
			"--tools",
			"write,read",
			"--prewalk-into",
			"mockB/model-b",
			"--session-id",
			"prewalk-failed-write",
			"-p",
			"write now",
		]),
	);
	assert!(
		run_out.status.success(),
		"failed-write prewalk run failed: {}",
		String::from_utf8_lossy(&run_out.stderr)
	);
	assert_eq!(a.request_count(), 1);
	assert_eq!(
		b.request_count(),
		1,
		"a failed write is still the implementation action"
	);
	assert_eq!(b.request(0)["model"], "model-b");
	assert!(
		String::from_utf8_lossy(&run_out.stderr).contains("Prewalk: switched"),
		"the switch must be announced: {}",
		String::from_utf8_lossy(&run_out.stderr)
	);

	a.stop();
	b.stop();
	fs::remove_dir_all(base).ok();
}
