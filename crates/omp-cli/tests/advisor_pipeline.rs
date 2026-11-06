//! Behavioral tests for the passive advisor pipeline.
//!
//! Each case drives the real `omp` binary against a scripted chat-completions
//! endpoint and asserts host-visible behavior, never source shape:
//! - `--advisor` enables the roster, the advisor runs on its own model/scope
//!   with only its configured tools, and `WATCHDOG.yml` instructions reach its
//!   system prompt;
//! - a blocker raised after the primary turn settled is preserved as a card
//!   (persisted `custom_message` + wire pair) without starting a hidden primary
//!   turn;
//! - `modelRoles.advisor` falls back to the `slow` role, and an advisor provider
//!   failure (quota) never fails the session.
//!
//! All files live under `$HOME/task-tmp` when available; the fallback is the
//! platform temp dir.

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

fn temp_dir(prefix: &str) -> PathBuf {
	let base = std::env::var_os("HOME")
		.map(PathBuf::from)
		.map(|home| home.join("task-tmp"))
		.filter(|dir| dir.is_dir() || fs::create_dir_all(dir).is_ok())
		.unwrap_or_else(std::env::temp_dir);
	let dir = base.join(format!(
		"omp-advisor-pipeline-{prefix}-{}-{}",
		std::process::id(),
		COUNTER.fetch_add(1, Ordering::Relaxed)
	));
	fs::create_dir_all(&dir).expect("create temp dir");
	dir
}

struct MockWireServer {
	port: u16,
	requests: Arc<Mutex<Vec<serde_json::Value>>>,
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
								&& let Ok(json) = serde_json::from_slice::<serde_json::Value>(
									&buf[pos + 4..pos + 4 + len],
								)
							{
								reqs_clone.lock().expect("lock").push(json);
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

		Self { port, requests, stop_tx, handle: Some(handle) }
	}

	fn count(&self) -> usize {
		self.requests.lock().expect("lock").len()
	}

	fn body(&self, index: usize) -> serde_json::Value {
		self
			.requests
			.lock()
			.expect("lock")
			.get(index)
			.cloned()
			.unwrap_or_else(|| panic!("mock request {index} not recorded"))
	}

	fn stop(mut self) {
		let _ = self.stop_tx.send(());
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}
	}
}

const DEFAULT_SSE: &str =
	"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";

fn primary_sse(text: &str) -> String {
	format!(
		"data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}\"}},\"finish_reason\":\"stop\"}}]}}\n\ndata: [DONE]\n\n"
	)
}

fn advise_tool_sse(note: &str, severity: &str) -> String {
	let args = serde_json::json!({ "note": note, "severity": severity }).to_string();
	format!(
		"data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{{\"index\":0,\"id\":\"call-adv\",\"type\":\"function\",\"function\":{{\"name\":\"advise\",\"arguments\":{args}}}}}]}},\"finish_reason\":null}}]}}\n\ndata: [DONE]\n\n",
		args = serde_json::to_string(&args).expect("json args")
	)
}

struct Setup {
	agent_dir: PathBuf,
	cwd: PathBuf,
	sessions: PathBuf,
}

fn setup(port: u16, base: &Path, roles: &str) -> Setup {
	let agent_dir = base.join("agent");
	let cwd = base.join("work");
	let sessions = base.join("sessions");
	fs::create_dir_all(&agent_dir).expect("agent dir");
	fs::create_dir_all(&cwd).expect("cwd");
	fs::create_dir_all(&sessions).expect("sessions dir");
	fs::write(
		agent_dir.join("config.yml"),
		format!(
			"defaultModel: mock/primary\n{roles}providers:\n  mock:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n"
		),
	)
	.expect("config.yml");
	fs::write(
		agent_dir.join("models.yml"),
		format!(
			"providers:\n  mock:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n    models:\n      - id: primary\n        contextWindow: 128000\n      - id: advisor\n        contextWindow: 128000\n      - id: slow\n        contextWindow: 128000\n"
		),
	)
	.expect("models.yml");
	Setup { agent_dir, cwd, sessions }
}

fn run_omp(setup: &Setup, extra: &[&str]) -> Output {
	let mut cmd = ProcCommand::new(env!("CARGO_BIN_EXE_omp"));
	cmd.env("PI_CODING_AGENT_DIR", &setup.agent_dir);
	cmd.env("HOME", setup.agent_dir.parent().expect("base"));
	cmd.stdin(Stdio::null());
	cmd.args([
		"--cwd",
		setup.cwd.to_str().expect("utf8 cwd"),
		"--session-dir",
		setup.sessions.to_str().expect("utf8 sessions"),
		"--no-tools",
		"--no-title",
		"--mode",
		"json",
	]);
	cmd.args(extra);
	cmd.arg("say hi");
	cmd.output().expect("run omp")
}

fn session_files(dir: &Path) -> Vec<PathBuf> {
	let mut files: Vec<PathBuf> = fs::read_dir(dir)
		.expect("read sessions dir")
		.flatten()
		.map(|entry| entry.path())
		.filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
		.collect();
	files.sort();
	files
}

/// The flag enables the roster, the advisor runs on its own route with only its
/// configured tools, and the project `WATCHDOG.yml` instructions reach its
/// system prompt. A blocker that arrives after the primary turn settled is
/// preserved as a card (wire + journal + transcript), never as a hidden turn.
#[test]
fn advisor_flag_runs_isolated_roster_and_preserves_late_blocker_card() {
	let server = MockWireServer::start(vec![
		primary_sse("done"),
		advise_tool_sse("write target deleted", "blocker"),
		primary_sse("noted"),
	]);
	let base = temp_dir("roster");
	let setup = setup(server.port, &base, "modelRoles:\n  advisor: mock/advisor\n  slow: mock/slow\n");
	fs::create_dir_all(setup.cwd.join(".omp")).expect("project dir");
	fs::write(
		setup.cwd.join(".omp/WATCHDOG.yml"),
		"instructions: FOCUS MARKER\nadvisors:\n  - name: reviewer\n    model: mock/advisor\n    tools:\n      - read\n",
	)
	.expect("watchdog");
	let out = run_omp(&setup, &["--advisor"]);
	assert!(
		out.status.success(),
		"omp failed: {}",
		String::from_utf8_lossy(&out.stderr)
	);

	// Primary request: the configured default model, untouched.
	assert_eq!(server.body(0)["model"], "primary");
	// Advisor request: its own route and the project attention block.
	let advisor = server.body(1);
	assert_eq!(advisor["model"], "advisor", "the roster model must reach the wire");
	let system = advisor["messages"][0]["content"].to_string();
	assert!(system.contains("passive advisor"), "advisor system prompt: {system}");
	assert!(system.contains("FOCUS MARKER"), "watchdog instructions must be appended");
	let tool_names: Vec<String> = advisor["tools"]
		.as_array()
		.expect("tools array")
		.iter()
		.filter_map(|tool| tool["function"]["name"].as_str().map(str::to_string))
		.collect();
	assert!(tool_names.iter().any(|name| name == "advise"), "advise tool: {tool_names:?}");
	assert!(tool_names.iter().any(|name| name == "read"), "configured read tool: {tool_names:?}");
	assert!(!tool_names.iter().any(|name| name == "bash"), "unconfigured tool leaked: {tool_names:?}");
	// The advisor sees the primary turn, not a copy of the primary's own route.
	let messages = advisor["messages"].to_string();
	assert!(messages.contains("say hi"));
	assert!(messages.contains("done"));

	// No hidden primary turn: exactly one primary request, one advise turn, and
	// the advisor's final settle.
	assert_eq!(server.count(), 3, "requests: {:?}", server.requests);

	let stdout = String::from_utf8_lossy(&out.stdout).to_string();
	assert!(
		stdout.contains("\"customType\":\"advisor\""),
		"card must reach the wire: {stdout}"
	);
	assert!(
		stdout.contains("write target deleted"),
		"blocker text must be visible, not silently dropped: {stdout}"
	);
	let card_messages = stdout
		.lines()
		.filter(|line| line.contains("\"type\":\"message_end\""))
		.filter(|line| line.contains("advisor"))
		.count();
	assert_eq!(card_messages, 1, "exactly one advisor card pair: {stdout}");

	// Persisted once as a `custom_message`, never replayed as a user prompt.
	let files = session_files(&setup.sessions);
	assert_eq!(files.len(), 1, "one session log: {files:?}");
	let log = fs::read_to_string(&files[0]).expect("session log");
	assert!(log.contains("\"type\":\"custom_message\""), "journal must carry the card");
	assert!(log.contains("\"customType\":\"advisor\""), "journal card type");
	assert!(log.contains("write target deleted"), "journal card body");

	// Per-advisor transcript inside the session stem directory, named for its
	// slug (TS `<session>/__advisor.<slug>.jsonl`).
	let transcript = files[0]
		.with_extension("")
		.join("__advisor.reviewer.jsonl");
	assert!(
		transcript.is_file(),
		"advisor transcript must be written: {}",
		transcript.display()
	);
	server.stop();
}

/// `modelRoles.advisor` falls back to the `slow` role; an advisor provider
/// failure (quota) never fails the session and never starts a hidden turn.
#[test]
fn advisor_role_falls_back_to_slow_and_quota_failure_is_contained() {
	let server = MockWireServer::start(vec![primary_sse("done"), primary_sse("noted")]);
	let base = temp_dir("slow");
	let first = setup(server.port, &base, "modelRoles:\n  slow: mock/slow\n");
	let out = run_omp(&first, &["--advisor"]);
	assert!(
		out.status.success(),
		"advisor failure must not fail the session: {}",
		String::from_utf8_lossy(&out.stderr)
	);
	assert_eq!(server.body(0)["model"], "primary");
	assert_eq!(
		server.body(1)["model"],
		"slow",
		"the advisor role resolves through `slow`, never the primary"
	);
	assert_eq!(server.count(), 2);

	// Same setup against a quota rejection: contained, no card, no extra turn.
	let quota = MockWireServer::start(vec![
		primary_sse("done"),
		"data: {\"error\":{\"type\":\"insufficient_quota\",\"message\":\"429 rate limit\"}}\n\ndata: [DONE]\n\n"
			.to_string(),
	]);
	let setup = setup(quota.port, &base, "modelRoles:\n  slow: mock/slow\n");
	let out = run_omp(&setup, &["--advisor"]);
	assert!(
		out.status.success(),
		"a quota-latched advisor must not fail the session: {}",
		String::from_utf8_lossy(&out.stderr)
	);
	let stdout = String::from_utf8_lossy(&out.stdout);
	assert!(
		!stdout.contains("\"customType\":\"advisor\""),
		"no card is fabricated from a failed advisor call: {stdout}"
	);
	assert!(quota.count() <= 3, "a quota latch must not retry forever: {}", quota.count());
	quota.stop();
	server.stop();
}

/// `--help` advertises the flag; no provider is contacted.
#[test]
fn advisor_flag_is_documented_in_help() {
	let out = ProcCommand::new(env!("CARGO_BIN_EXE_omp"))
		.arg("--help")
		.stdin(Stdio::null())
		.output()
		.expect("run omp --help");
	assert!(out.status.success());
	let help = String::from_utf8_lossy(&out.stdout);
	assert!(help.contains("--advisor"), "help must list --advisor: {help}");
}
