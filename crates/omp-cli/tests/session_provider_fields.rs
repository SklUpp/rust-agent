//! Behavioral tests for `--provider-session-id` / `--prompt-cache-key`:
//! actual OpenAI-compatible request body, resume/fork inheritance priority, and
//! the non-TTY foreign-import refusal. Nothing here asserts flag plumbing alone.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcCommand, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_dir(prefix: &str) -> PathBuf {
	let unique = format!(
		"omp-provider-fields-{prefix}-{}-{}",
		std::process::id(),
		COUNTER.fetch_add(1, Ordering::Relaxed)
	);
	let dir = std::env::temp_dir().join(unique);
	fs::create_dir_all(&dir).expect("create temp dir");
	dir
}

fn omp() -> ProcCommand {
	let mut cmd = ProcCommand::new(env!("CARGO_BIN_EXE_omp"));
	cmd.stdin(Stdio::null());
	cmd
}

fn setup_agent_dir(dir: &Path, port: u16, provider: &str) {
	let config = format!(
		"defaultModel: {provider}/mock-model\nproviders:\n  {provider}:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n"
	);
	fs::write(dir.join("config.yml"), config).expect("write config.yml");
	let models = format!(
		"providers:\n  {provider}:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n    models:\n      - id: mock-model\n        contextWindow: 128000\n      - id: second-model\n        contextWindow: 128000\n"
	);
	fs::write(dir.join("models.yml"), models).expect("write models.yml");
}

fn read_request_body(stream: &mut std::net::TcpStream) -> serde_json::Value {
	let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
	let mut buf = Vec::new();
	let mut header_end = None;
	let mut chunk = [0u8; 1024];
	let start = Instant::now();
	while header_end.is_none() && start.elapsed() < Duration::from_secs(5) {
		match stream.read(&mut chunk) {
			Ok(0) => break,
			Ok(n) => {
				buf.extend_from_slice(&chunk[..n]);
				header_end = buf.windows(4).position(|w| w == b"\r\n\r\n");
			},
			Err(_) => break,
		}
	}
	let Some(header_end) = header_end else {
		return serde_json::Value::Null;
	};
	let headers = String::from_utf8_lossy(&buf[..header_end]);
	let content_length = headers
		.lines()
		.find_map(|line| {
			line.to_ascii_lowercase()
				.strip_prefix("content-length:")
				.map(|value| value.trim().parse::<usize>().unwrap_or(0))
		})
		.unwrap_or(0);
	let body_start = header_end + 4;
	let mut body = buf[body_start..].to_vec();
	while body.len() < content_length && start.elapsed() < Duration::from_secs(5) {
		match stream.read(&mut chunk) {
			Ok(0) => break,
			Ok(n) => body.extend_from_slice(&chunk[..n]),
			Err(_) => break,
		}
	}
	serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null)
}

fn serve_one(listener: TcpListener) -> thread::JoinHandle<serde_json::Value> {
	thread::spawn(move || {
		listener.set_nonblocking(true).expect("nonblocking listener");
		let deadline = Instant::now() + Duration::from_secs(15);
		let mut stream = loop {
			match listener.accept() {
				Ok((stream, _)) => break stream,
				Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
					if Instant::now() >= deadline {
						return serde_json::Value::Null;
					}
					thread::sleep(Duration::from_millis(10));
				},
				Err(_) => return serde_json::Value::Null,
			}
		};
		let body = read_request_body(&mut stream);
		let sse = concat!(
			"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n",
			"data: [DONE]\n\n"
		);
		let response = format!(
			"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
			sse.len(),
			sse
		);
		let _ = stream.write_all(response.as_bytes());
		let _ = stream.flush();
		body
	})
}

struct Fixture {
	base: PathBuf,
	agent_dir: PathBuf,
	sessions_dir: PathBuf,
	project_dir: PathBuf,
}

impl Fixture {
	fn new(prefix: &str) -> Self {
		let base = temp_dir(prefix);
		let agent_dir = base.join("agent");
		let sessions_dir = base.join("sessions");
		let project_dir = base.join("project");
		fs::create_dir_all(&agent_dir).expect("agent dir");
		fs::create_dir_all(&sessions_dir).expect("sessions dir");
		fs::create_dir_all(&project_dir).expect("project dir");
		Self { base, agent_dir, sessions_dir, project_dir }
	}

	fn run_repl_capture(&self, extra: &[&str], input: &str) -> (serde_json::Value, Output) {
		let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
		let port = listener.local_addr().expect("addr").port();
		setup_agent_dir(&self.agent_dir, port, "kimi-code");
		let server = serve_one(listener);

		let mut cmd = omp();
		cmd.env("PI_CODING_AGENT_DIR", self.agent_dir.to_str().unwrap())
			.args(["--cwd", self.project_dir.to_str().unwrap()])
			.args(["--session-dir", self.sessions_dir.to_str().unwrap()])
			.args(extra)
			.arg("repl")
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.stderr(Stdio::piped());
		let mut child = cmd.spawn().expect("spawn omp repl");
		child
			.stdin
			.take()
			.expect("repl stdin")
			.write_all(input.as_bytes())
			.expect("write repl input");
		let output = child.wait_with_output().expect("wait for omp repl");
		let body = server.join().expect("join mock server");
		(body, output)
	}

	fn run_capture(&self, extra: &[&str], prompt: &str) -> (serde_json::Value, Output) {
		self.run_capture_as("kimi-code", extra, prompt)
	}

	fn run_capture_as(
		&self,
		provider: &str,
		extra: &[&str],
		prompt: &str,
	) -> (serde_json::Value, Output) {
		let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
		let port = listener.local_addr().expect("addr").port();
		setup_agent_dir(&self.agent_dir, port, provider);
		let server = serve_one(listener);

		let mut cmd = omp();
		cmd.env("PI_CODING_AGENT_DIR", self.agent_dir.to_str().unwrap())
			.args(["--cwd", self.project_dir.to_str().unwrap()])
			.args(["--session-dir", self.sessions_dir.to_str().unwrap()])
			.args(extra)
			.arg(prompt);
		let output = cmd.output().expect("run omp");
		let body = server.join().expect("join mock server");
		(body, output)
	}
}

impl Drop for Fixture {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.base);
	}
}

/// Seed the durable session the CLI will resume. Header state lives in SQLite;
/// the returned path is the virtual identity, not a journal file.
fn write_session_header(sessions_dir: &Path, id: &str, cache_key: Option<&str>) -> PathBuf {
	let options = omp_cli::session::SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/tmp"),
		model: None,
		provider: None,
		dir: Some(sessions_dir.to_path_buf()),
		id: Some(id.to_string()),
		continue_latest: false,
		no_title: true,
		parent_session: None,
	};
	let mut store = omp_cli::session::SessionStore::open(&options);
	assert!(store.enabled(), "seed session must open");
	if let Some(key) = cache_key {
		store
			.set_provider_prompt_cache_key(key)
			.expect("persist cache key");
	}
	store.file().expect("virtual session identity").to_path_buf()
}

fn assert_success(output: &Output) {
	assert!(
		output.status.success(),
		"omp must exit 0: stderr={}",
		String::from_utf8_lossy(&output.stderr)
	);
}

#[test]
fn explicit_prompt_cache_key_wins_on_chat_completions_wire() {
	let fx = Fixture::new("explicit");
	let (body, output) = fx.run_capture(
		&[
			"--provider-session-id",
			"provider-session-1",
			"--prompt-cache-key",
			"explicit-cache-1",
		],
		"hello",
	);
	assert_success(&output);
	assert_eq!(
		body["prompt_cache_key"], "explicit-cache-1",
		"explicit --prompt-cache-key must reach the provider body"
	);
}

#[test]
fn generic_openai_provider_omits_prompt_cache_key_on_the_wire() {
	let fx = Fixture::new("generic-omit");
	let (body, output) = fx.run_capture_as(
		"mock",
		&[
			"--provider-session-id",
			"provider-session-1",
			"--prompt-cache-key",
			"explicit-cache-1",
		],
		"hello",
	);
	assert_success(&output);
	assert!(
		body.get("prompt_cache_key").is_none(),
		"TS openai-completions sends prompt_cache_key only for kimi-code or a \
		 prompt-cache-enabled compat model; generic endpoint got: {body}"
	);
}

#[test]
fn provider_session_id_is_the_cache_fallback_on_the_wire() {
	let fx = Fixture::new("session-fallback");
	let (body, output) = fx.run_capture(&["--provider-session-id", "provider-session-1"], "hello");
	assert_success(&output);
	assert_eq!(
		body["prompt_cache_key"], "provider-session-1",
		"without an explicit cache key, the provider session id is the TS fallback"
	);
}

#[test]
fn resume_inherits_header_key_and_explicit_flag_overrides_it() {
	let fx = Fixture::new("resume");
	write_session_header(&fx.sessions_dir, "resume-session", Some("header-cache-1"));

	let (body, output) = fx.run_capture(&["--resume", "resume-session"], "first");
	assert_success(&output);
	assert_eq!(
		body["prompt_cache_key"], "header-cache-1",
		"resume without shaping flags must inherit providerPromptCacheKey"
	);

	let (body, output) = fx.run_capture(
		&["--resume", "resume-session", "--prompt-cache-key", "explicit-cache-1"],
		"second",
	);
	assert_success(&output);
	assert_eq!(
		body["prompt_cache_key"], "explicit-cache-1",
		"explicit --prompt-cache-key must override the inherited header key"
	);
}

#[test]
fn resume_old_header_without_cache_key_stays_compatible() {
	let fx = Fixture::new("legacy-header");
	write_session_header(&fx.sessions_dir, "legacy-session", None);

	let (body, output) = fx.run_capture(&["--resume", "legacy-session"], "hello");
	assert_success(&output);
	assert!(
		body.get("prompt_cache_key").is_none(),
		"an old session without providerPromptCacheKey must not invent one, got: {body}"
	);

	let reopened = omp_cli::session::SessionStore::open(&omp_cli::session::SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/tmp"),
		model: None,
		provider: None,
		dir: Some(fx.sessions_dir.clone()),
		id: Some("legacy-session".to_string()),
		continue_latest: false,
		no_title: true,
		parent_session: None,
	});
	let header = reopened.header().expect("session header");
	assert_eq!(header.id, "legacy-session");
	assert!(
		header.provider_prompt_cache_key.is_none(),
		"resume must not add a provider cache key to an old header"
	);
	assert!(
		!reopened.messages().is_empty(),
		"resumed old session must still append the new turn"
	);
}

#[test]
fn model_override_drops_inherited_header_cache_key() {
	let fx = Fixture::new("model-shape");
	write_session_header(&fx.sessions_dir, "shape-session", Some("header-cache-1"));

	let (body, output) = fx.run_capture(
		&["--resume", "shape-session", "--model", "kimi-code/mock-model"],
		"hello",
	);
	assert_success(&output);
	assert!(
		body.get("prompt_cache_key").is_none(),
		"changing the model route must drop inherited cache identity, got: {body}"
	);
}

#[test]
fn thinking_override_drops_inherited_header_cache_key() {
	let fx = Fixture::new("thinking-shape");
	write_session_header(&fx.sessions_dir, "thinking-session", Some("header-cache-1"));

	let (body, output) = fx.run_capture(
		&["--resume", "thinking-session", "--thinking", "high"],
		"hello",
	);
	assert_success(&output);
	assert!(
		body.get("prompt_cache_key").is_none(),
		"an explicit --thinking level changes the request shape and must drop the inherited key, got: {body}"
	);

	let (body, output) = fx.run_capture(
		&["--fork", "thinking-session", "--thinking", "low"],
		"forked turn",
	);
	assert_success(&output);
	assert!(
		body.get("prompt_cache_key").is_none(),
		"forking with an explicit --thinking level must drop the inherited key, got: {body}"
	);
}

#[test]
fn thinking_override_keeps_explicit_cache_key() {
	let fx = Fixture::new("thinking-explicit");
	write_session_header(&fx.sessions_dir, "thinking-explicit", Some("header-cache-1"));

	let (body, output) = fx.run_capture(
		&[
			"--resume",
			"thinking-explicit",
			"--thinking",
			"medium",
			"--prompt-cache-key",
			"explicit-cache-1",
		],
		"hello",
	);
	assert_success(&output);
	assert_eq!(
		body["prompt_cache_key"], "explicit-cache-1",
		"an explicit --prompt-cache-key must win over the thinking shape change"
	);
}

#[test]
fn invalid_thinking_keeps_inherited_header_cache_key() {
	let fx = Fixture::new("thinking-invalid");
	write_session_header(&fx.sessions_dir, "thinking-invalid", Some("header-cache-1"));

	let (body, output) = fx.run_capture(
		&["--resume", "thinking-invalid", "--thinking", "bogus"],
		"hello",
	);
	assert_success(&output);
	assert!(
		String::from_utf8_lossy(&output.stderr)
			.contains("Invalid thinking level passed to --thinking"),
		"an invalid level must only warn: {}",
		String::from_utf8_lossy(&output.stderr)
	);
	assert_eq!(
		body["prompt_cache_key"], "header-cache-1",
		"an ignored --thinking value must not change the cache shape"
	);
}

#[test]
fn repl_model_switch_drops_inherited_key_but_keeps_explicit_key() {
	let fx = Fixture::new("repl-switch");
	write_session_header(&fx.sessions_dir, "repl-session", Some("header-cache-1"));

	let input = "/model kimi-code/second-model\nhello\n/exit\n";
	let (body, output) = fx.run_repl_capture(&["--resume", "repl-session"], input);
	assert_success(&output);
	assert!(
		body.get("prompt_cache_key").is_none(),
		"an inherited key must be dropped after /model changes the route, got: {body}"
	);

	let (body, output) = fx.run_repl_capture(
		&["--resume", "repl-session", "--prompt-cache-key", "explicit-cache-1"],
		input,
	);
	assert_success(&output);
	assert_eq!(
		body["prompt_cache_key"], "explicit-cache-1",
		"an explicit key must survive a /model switch"
	);
}

#[test]
fn model_override_keeps_explicit_cache_key() {
	let fx = Fixture::new("model-explicit");
	let (body, output) = fx.run_capture(
		&[
			"--model",
			"kimi-code/mock-model",
			"--prompt-cache-key",
			"explicit-cache-1",
		],
		"hello",
	);
	assert_success(&output);
	assert_eq!(
		body["prompt_cache_key"], "explicit-cache-1",
		"an explicit --prompt-cache-key must survive a model selection change"
	);
}

#[test]
fn fork_inherits_source_header_cache_key() {
	let fx = Fixture::new("fork");
	write_session_header(&fx.sessions_dir, "fork-source", Some("source-cache-1"));

	let (body, output) = fx.run_capture(&["--fork", "fork-source"], "forked turn");
	assert_success(&output);
	assert_eq!(
		body["prompt_cache_key"], "source-cache-1",
		"fork must inherit the source header's provider prompt-cache key"
	);

	let summaries = omp_cli::durable_session::list_session_summaries(&fx.sessions_dir)
		.expect("list durable sessions");
	assert_eq!(summaries.len(), 2, "fork must create one new durable session");
	let fork_id = summaries
		.iter()
		.find(|summary| summary.id != "fork-source")
		.map(|summary| summary.id.clone())
		.expect("forked session id");
	let forked = omp_cli::session::SessionStore::open(&omp_cli::session::SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/tmp"),
		model: None,
		provider: None,
		dir: Some(fx.sessions_dir.clone()),
		id: Some(fork_id),
		continue_latest: false,
		no_title: true,
		parent_session: None,
	});
	assert_eq!(
		forked
			.header()
			.and_then(|header| header.provider_prompt_cache_key.as_deref()),
		Some("source-cache-1")
	);
}

#[test]
fn foreign_import_flags_fail_explicitly_without_a_tty() {
	let fx = Fixture::new("foreign-non-tty");
	for flag in ["--from-claude", "--from-codex"] {
		let output = omp()
			.env("PI_CODING_AGENT_DIR", fx.agent_dir.to_str().unwrap())
			.args(["--session-dir", fx.sessions_dir.to_str().unwrap(), flag, "-p", "hello"])
			.output()
			.expect("run omp");
		let stderr = String::from_utf8_lossy(&output.stderr);
		assert_eq!(
			output.status.code(),
			Some(2),
			"{flag} in non-TTY must be a usage error, got {:?}: {stderr}",
			output.status.code()
		);
		assert!(
			stderr.contains("Non-interactive environment cannot display session picker")
				&& stderr.contains(flag),
			"{flag} must name the unavailable interactive picker, got: {stderr}"
		);
	}
	let entries = fs::read_dir(&fx.sessions_dir)
		.expect("read sessions")
		.flatten()
		.count();
	assert_eq!(entries, 0, "refused foreign import must not create a session");
}
