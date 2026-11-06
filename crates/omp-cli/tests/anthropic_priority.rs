//! Anthropic priority (`serviceTier: "priority"`) wire contract for the real
//! runner: the direct provider requests fast mode, a server rejection retries
//! once without it, the terminal message reports the dropped feature, and the
//! following request stays downgraded.
//!
//! Source: TS `anthropic.ts:2764-2783,2846-2848,3478-3480` (request flag,
//! one-shot fast-mode fallback, terminal `disabledFeatures`) and
//! `error/flags.ts` `matchesFastModeUnsupported`.

use std::{
	collections::BTreeMap,
	io::{Read, Write},
	net::{TcpListener, TcpStream},
	path::PathBuf,
	sync::{Arc, mpsc},
	time::Duration,
};

use omp_cli::{
	agent::{AgentRunner, Event, RunRequest},
	approve::{ApprovalMode, Approvals},
	catalog::{Catalog, ModelInfo},
	config::{Config, ProviderConfig},
	output::OutputMode,
	runtime::PiAgentRunner,
	session::{SessionOptions, SessionStore},
};

const FAST_REJECTION: &str = r#"{"type":"error","error":{"type":"invalid_request_error","message":"speed is not supported for this model"}}"#;

/// Downgraded retry that asks for a real tool: the continuation assistant turn
/// must descend from the recorded reset entry, not from the sibling assistant.
const TOOL_TURN: &str = concat!(
	"event: message_start\n",
	"data: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n",
	"event: content_block_start\n",
	"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"bash\",\"input\":{}}}\n\n",
	"event: content_block_delta\n",
	"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"command\\\":\\\"echo hi\\\"}\"}}\n\n",
	"event: content_block_stop\n",
	"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
	"event: message_delta\n",
	"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":2}}\n\n",
	"event: message_stop\n",
	"data: {\"type\":\"message_stop\"}\n\n"
);

/// A 200 stream whose first event is an error (TS `streamFailure` before
/// `firstTokenTime`), which must take the same one-shot downgrade path as an
/// HTTP rejection.
const SSE_ERROR: &str = concat!(
	"event: error\n",
	"data: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"speed is not supported for this model\"}}\n\n"
);

const TURN: &str = concat!(
	"event: message_start\n",
	"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n",
	"event: content_block_start\n",
	"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
	"event: content_block_delta\n",
	"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"fast ok\"}}\n\n",
	"event: content_block_stop\n",
	"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
	"event: message_delta\n",
	"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
	"event: message_stop\n",
	"data: {\"type\":\"message_stop\"}\n\n"
);

fn catalog(addr: std::net::SocketAddr) -> Catalog {
	let mut models = BTreeMap::new();
	models.insert("claude-priority".to_string(), ModelInfo {
		provider: "anthropic".to_string(),
		api: Some("anthropic-messages".to_string()),
		base_url: Some(format!("http://{addr}/v1")),
		max_tokens: Some(4096),
		..ModelInfo::default()
	});
	let mut providers = BTreeMap::new();
	providers.insert("anthropic".to_string(), models);
	Catalog { providers, connections: BTreeMap::new(), source: PathBuf::from("models.json") }
}

fn request(catalog: Catalog, tiers: &[(&str, &str)]) -> RunRequest {
	let mut config = Config::default();
	config.providers.insert("anthropic".to_string(), ProviderConfig {
		api_key: Some("sk-ant-priority".to_string()),
		..ProviderConfig::default()
	});
	let mut service_tier_by_family = BTreeMap::new();
	for (family, tier) in tiers {
		service_tier_by_family.insert((*family).to_string(), (*tier).to_string());
	}
	RunRequest {
		prompt: "answer quickly".to_string(),
		parts: Vec::new(),
		cwd: std::env::temp_dir(),
		output_mode: OutputMode::Json,
		model: Some("anthropic/claude-priority".to_string()),
		provider: None,
		api_key: None,
		provider_session_id: None,
		prompt_cache_key: None,
		system_prompt: Some("base prompt".to_string()),
		append_system_prompt: None,
		context_files: Vec::new(),
		skills: Vec::new(),
		history: Vec::new(),
		history_messages: None,
		checkpoint_controller: None,
		turn_ack: None,
		todo_phases: Vec::new(),
		session_persisted: false,
		session_file: None,
		session_id: None,
		config,
		catalog: Arc::new(catalog),
		approvals: Approvals::new(ApprovalMode::Yolo, Vec::new()),
		max_time: Some(Duration::from_secs(30)),
		max_turns: 1,
		tools_enabled: false,
		cancellation_token: None,
		extra_tools: Vec::new(),
		acp_bridge: None,
		extra_mcp_servers: Vec::new(),
		internal_urls: None,
		url_scope: None,
		subagent_observer: None,
		child_session_root: None,
		goal: None,
		goal_context: None,
		credential_resolver: None,
		plan_mode: Default::default(),
		tool_ceiling: None,
		skill_autoload: Default::default(),
		continuation: false,
		plan_context: None,
		advisor_card_sink: None,
		service_tier_by_family,
		security_host: None,
		security_worker: false,
	}
}

fn read_request(socket: &mut TcpStream) -> (String, String) {
	let mut buffer = Vec::new();
	let mut chunk = [0u8; 4096];
	let header_end = loop {
		let read = socket.read(&mut chunk).expect("read request");
		assert!(read > 0, "client closed before the request headers");
		buffer.extend_from_slice(&chunk[..read]);
		if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
			break position + 4;
		}
	};
	let head = String::from_utf8_lossy(&buffer[..header_end]).to_string();
	let length = head
		.lines()
		.find_map(|line| {
			let (name, value) = line.split_once(':')?;
			name.eq_ignore_ascii_case("content-length")
				.then(|| value.trim().parse::<usize>().ok())?
		})
		.unwrap_or(0);
	while buffer.len() < header_end + length {
		let read = socket.read(&mut chunk).expect("read body");
		assert!(read > 0, "client closed before the request body");
		buffer.extend_from_slice(&chunk[..read]);
	}
	(
		head,
		String::from_utf8(buffer[header_end..header_end + length].to_vec()).expect("utf8 body"),
	)
}

fn respond(socket: &mut TcpStream, status: &str, content_type: &str, body: &str) {
	let response = format!(
		"HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
		 Connection: close\r\n\r\n{body}",
		body.len()
	);
	socket.write_all(response.as_bytes()).expect("write response");
	socket.flush().expect("flush response");
}

fn run(request: RunRequest) -> (Vec<String>, Option<Vec<String>>) {
	let runner = PiAgentRunner::new().expect("runner");
	let mut stops = Vec::new();
	let mut disabled = None;
	for event in runner.run(&request).expect("stream") {
		match event {
			Ok(Event::MessageEnd { message, .. }) => disabled = message.disabled_features,
			Ok(Event::TurnEnd { stop_reason, .. }) => stops.push(stop_reason),
			_ => {},
		}
	}
	(stops, disabled)
}

fn body_field(body: &str, name: &str) -> Option<serde_json::Value> {
	let value: serde_json::Value = serde_json::from_str(body).expect("json body");
	value.get(name).cloned()
}

#[test]
fn rejected_priority_retries_without_fast_mode_and_reports_the_drop() {
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
	let addr = listener.local_addr().expect("addr");
	let (captured_tx, captured_rx) = mpsc::channel::<(String, String)>();
	let server = std::thread::spawn(move || {
		// Priorität request: the server rejects the `speed` parameter.
		let (mut socket, _) = listener.accept().expect("accept");
		let captured = read_request(&mut socket);
		captured_tx.send(captured).expect("capture reject");
		respond(&mut socket, "400 Bad Request", "application/json", FAST_REJECTION);
		// Downgraded retry: same turn, no fast mode.
		let (mut socket, _) = listener.accept().expect("accept");
		let captured = read_request(&mut socket);
		captured_tx.send(captured).expect("capture retry");
		respond(&mut socket, "200 OK", "text/event-stream", TURN);
		// Following turn: the caller no longer asks for priority.
		let (mut socket, _) = listener.accept().expect("accept");
		let captured = read_request(&mut socket);
		captured_tx.send(captured).expect("capture follow-up");
		respond(&mut socket, "200 OK", "text/event-stream", TURN);
	});

	let first = request(catalog(addr), &[("anthropic", "priority")]);
	let (stops, disabled) = run(first);
	assert_eq!(stops.last().map(String::as_str), Some("stop"));
	assert_eq!(
		disabled.as_deref(),
		Some(["priority".to_string()].as_slice()),
		"the terminal message reports the dropped feature"
	);

	let (reject_head, reject_body) = captured_rx.recv().expect("reject request");
	assert!(reject_head.starts_with("POST /v1/messages HTTP/1.1"), "{reject_head}");
	assert!(
		reject_head
			.to_ascii_lowercase()
			.contains("anthropic-beta:") && reject_head.contains("fast-mode-2026-02-01"),
		"the priority request carries the fast-mode beta: {reject_head}"
	);
	assert_eq!(body_field(&reject_body, "speed"), Some(serde_json::json!("fast")));

	let (retry_head, retry_body) = captured_rx.recv().expect("retry request");
	let retry_lower = retry_head.to_ascii_lowercase();
	assert!(
		!retry_lower.contains("fast-mode-2026-02-01"),
		"the retry drops the beta: {retry_head}"
	);
	assert_eq!(body_field(&retry_body, "speed"), None, "the retry drops `speed`");
	assert_eq!(body_field(&retry_body, "stream"), Some(serde_json::json!(true)));

	let follow = request(catalog(addr), &[]);
	run(follow);
	let (_head, follow_body) = captured_rx.recv().expect("follow-up request");
	assert_eq!(body_field(&follow_body, "speed"), None, "no priority, no speed");
	server.join().expect("server");
}

fn tier_options(dir: std::path::PathBuf, id: &str) -> SessionOptions {
	SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work"),
		model: Some("anthropic/claude-priority".to_string()),
		provider: Some("anthropic".to_string()),
		dir: Some(dir),
		id: Some(id.to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: true,
	}
}

fn tiers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
	pairs
		.iter()
		.map(|(family, tier)| ((*family).to_string(), (*tier).to_string()))
		.collect()
}

/// TS `session-entries.ts:91-95` + `session-context.ts:263-264`: the entry is
/// the string-valued family map (or `null`), the last entry on the branch wins,
/// and an explicit reset is a recorded state — it must not fall back to config.
#[test]
fn service_tier_change_journals_strings_and_reset_survives_resume() {
	let dir = std::env::temp_dir().join(format!(
		"omp-anthropic-priority-{}-{}",
		std::process::id(),
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("clock")
			.as_nanos()
	));
	std::fs::create_dir_all(&dir).expect("temp dir");

	let mut store = SessionStore::open(&tier_options(dir.clone(), "priority-tier"));
	assert!(!store.has_service_tier_entry(), "a fresh session has no selection");
	store
		.append_service_tier_change(&tiers(&[("anthropic", "priority")]))
		.expect("append selection");

	let reopened = SessionStore::open(&tier_options(dir.clone(), "priority-tier"));
	assert!(reopened.has_service_tier_entry());
	assert_eq!(
		reopened.service_tier_by_family(),
		tiers(&[("anthropic", "priority")]),
		"resume restores the recorded selection"
	);

	// Explicit reset: a recorded empty map, not a missing entry.
	let mut reset = SessionStore::open(&tier_options(dir.clone(), "priority-tier"));
	reset
		.append_service_tier_change(&BTreeMap::new())
		.expect("append reset");
	let resumed = SessionStore::open(&tier_options(dir.clone(), "priority-tier"));
	assert!(resumed.has_service_tier_entry(), "the reset is itself recorded");
	assert!(
		resumed.service_tier_by_family().is_empty(),
		"the reset must not re-enable the configured priority"
	);

	// Explicit re-arm after the reset wins as the last branch entry.
	let mut rearm = SessionStore::open(&tier_options(dir.clone(), "priority-tier"));
	rearm
		.append_service_tier_change(&tiers(&[("anthropic", "priority")]))
		.expect("append re-arm");
	assert_eq!(
		SessionStore::open(&tier_options(dir.clone(), "priority-tier")).service_tier_by_family(),
		tiers(&[("anthropic", "priority")]),
		"an explicit re-arm restores priority"
	);

	let log = std::fs::read_dir(&dir)
		.expect("session dir")
		.flatten()
		.map(|entry| entry.path())
		.find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
		.expect("session log");
	let contents = std::fs::read_to_string(&log).expect("read log");
	let entries: Vec<serde_json::Value> = contents
		.lines()
		.filter(|line| !line.trim().is_empty())
		.map(|line| serde_json::from_str(line).expect("json entry"))
		.collect();
	let tiers_json: Vec<&serde_json::Value> = entries
		.iter()
		.filter(|entry| entry["type"] == "service_tier_change")
		.map(|entry| &entry["serviceTier"])
		.collect();
	assert_eq!(tiers_json.len(), 3, "selection, reset, re-arm were all journaled");
	assert_eq!(tiers_json[0]["anthropic"], "priority", "values are ServiceTier strings");
	assert!(tiers_json[1].is_null(), "the reset is null, never false");
	let _ = std::fs::remove_dir_all(&dir);
}

/// TS `forkSession` copies the branch log, so a fork restores the recorded
/// service-tier selection exactly like resume.
#[test]
fn fork_restores_the_recorded_service_tier() {
	let dir = std::env::temp_dir().join(format!(
		"omp-anthropic-priority-fork-{}-{}",
		std::process::id(),
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("clock")
			.as_nanos()
	));
	std::fs::create_dir_all(&dir).expect("temp dir");
	let mut store = SessionStore::open(&tier_options(dir.clone(), "tier-origin"));
	store
		.append_service_tier_change(&tiers(&[("anthropic", "priority")]))
		.expect("append selection");
	let source = store.file().map(std::path::Path::to_path_buf).expect("session file");
	let fork = omp_cli::session::fork_session(&source, None, Some(&dir)).expect("fork");
	let contents = std::fs::read_to_string(&fork).expect("fork log");
	assert!(
		contents.contains("\"type\":\"service_tier_change\""),
		"the fork carries the recorded selection"
	);
	let mut options = tier_options(dir.clone(), "unused");
	options.id = None;
	options.continue_latest = true;
	let forked = SessionStore::open(&options);
	assert_eq!(
		forked.service_tier_by_family(),
		tiers(&[("anthropic", "priority")]),
		"the fork restores the same selection"
	);
	let _ = std::fs::remove_dir_all(&dir);
}

/// End-to-end CLI: the configured `tier.anthropic` selection (project
/// settings) starts the request in fast mode, the rejection downgrade is
/// reported once on the live stderr sink, and the resumed session restores the
/// recorded reset so the next process sends no fast mode. The explicit toggle
/// rearm lives in the RPC fixture (`set_fast_mode`).
#[test]
fn cli_priority_downgrade_reports_once_and_persists_the_reset() {
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
	let addr = listener.local_addr().expect("addr");
	let (captured_tx, captured_rx) = mpsc::channel::<(String, String)>();
	let server = std::thread::spawn(move || {
		for index in 0..3 {
			let (mut socket, _) = listener.accept().expect("accept");
			let captured = read_request(&mut socket);
			captured_tx.send(captured).expect("capture");
			if index == 0 {
				respond(&mut socket, "400 Bad Request", "application/json", FAST_REJECTION);
			} else {
				respond(&mut socket, "200 OK", "text/event-stream", TURN);
			}
		}
	});

	let root = std::env::temp_dir().join(format!(
		"omp-anthropic-priority-cli-{}-{}",
		std::process::id(),
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("clock")
			.as_nanos()
	));
	let agent = root.join("agent");
	let cwd = root.join("work");
	let sessions = root.join("sessions");
	std::fs::create_dir_all(&agent).expect("agent dir");
	std::fs::create_dir_all(&cwd).expect("cwd");
	std::fs::write(
		agent.join("config.yml"),
		format!(
			"defaultModel: anthropic/claude-priority\nproviders:\n  anthropic:\n    \
			 baseUrl: \"http://{addr}/v1\"\n    api: anthropic-messages\n    apiKey: test-key\n"
		),
	)
	.expect("config.yml");
	std::fs::write(
		agent.join("models.yml"),
		format!(
			"providers:\n  anthropic:\n    baseUrl: \"http://{addr}/v1\"\n    \
			 api: anthropic-messages\n    apiKey: test-key\n    models:\n      - id: claude-priority\n\
			 \x20       contextWindow: 200000\n"
		),
	)
	.expect("models.yml");
	// Project settings.json is always merged (the legacy global file is skipped
	// once a main YAML exists), so the fast-mode selection lives here.
	std::fs::create_dir_all(cwd.join(".omp")).expect("project config dir");
	std::fs::write(
		cwd.join(".omp").join("settings.json"),
		"{\"tier\":{\"anthropic\":\"priority\"}}",
	)
	.expect("project settings.json");

	let run = |extra: &[&str]| {
		let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_omp"));
		command
			.env("HOME", root.join("home"))
			.env("PI_CODING_AGENT_DIR", &agent)
			.args(["--cwd", cwd.to_str().expect("cwd")])
			.args(["--session-dir", sessions.to_str().expect("sessions")])
			.args(["--no-title", "--approval-mode", "yolo"])
			.args(extra)
			.args(["-p", "answer quickly"])
			.stdin(std::process::Stdio::null());
		command.output().expect("run omp")
	};

	let first = run(&[]);
	assert!(first.status.success(), "first run failed: {}", String::from_utf8_lossy(&first.stderr));
	let stderr = String::from_utf8_lossy(&first.stderr);
	const NOTICE: &str =
		"Priority/fast mode rejected for this model; retried without it. Fast mode is now off.";
	assert_eq!(stderr.matches(NOTICE).count(), 1, "reported exactly once: {stderr}");

	let (first_head, first_body) = captured_rx.recv().expect("first request");
	assert!(first_head.to_ascii_lowercase().contains("anthropic-beta:"));
	assert_eq!(body_field(&first_body, "speed"), Some(serde_json::json!("fast")));
	let (retry_head, retry_body) = captured_rx.recv().expect("retry request");
	assert!(!retry_head.to_ascii_lowercase().contains("fast-mode-2026-02-01"));
	assert_eq!(body_field(&retry_body, "speed"), None);

	// Resume with the same settings: the recorded reset still wins over configuration.
	let second = run(&["--continue"]);
	assert!(second.status.success(), "second run failed: {}", String::from_utf8_lossy(&second.stderr));
	let (_head, body) = captured_rx.recv().expect("resumed request");
	assert_eq!(body_field(&body, "speed"), None, "the reset survived resume");
	server.join().expect("server");
	let _ = std::fs::remove_dir_all(&root);
}

/// TS `matchesFastModeUnsupported` needs a 400/429 status. An SSE `error`
/// event carries none, so it is surfaced to the caller, never silently retried.
#[test]
fn streamed_rejection_is_surfaced_without_retry() {
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
	let addr = listener.local_addr().expect("addr");
	let (captured_tx, captured_rx) = mpsc::channel::<(String, String)>();
	let server = std::thread::spawn(move || {
		let (mut socket, _) = listener.accept().expect("accept");
		let captured = read_request(&mut socket);
		captured_tx.send(captured).expect("capture");
		respond(&mut socket, "200 OK", "text/event-stream", SSE_ERROR);
	});

	let runner = PiAgentRunner::new().expect("runner");
	let mut errors = 0usize;
	let mut disabled = None;
	for event in runner.run(&request(catalog(addr), &[("anthropic", "priority")])).expect("stream") {
		match event {
			Ok(Event::MessageEnd { message, .. }) => disabled = message.disabled_features,
			Err(_) => errors += 1,
			_ => {},
		}
	}
	assert!(errors > 0, "the stream error must reach the caller");
	assert!(disabled.is_none(), "no downgrade annotation without a status gate");
	let _first = captured_rx.recv().expect("first request");
	assert!(
		captured_rx.try_recv().is_err(),
		"no retry request is issued for a statusless stream error"
	);
	server.join().expect("server");
}

/// 70 usage-only `message_delta` records precede the first text: they are
/// coalesced into one metadata chunk, so the terminal turn keeps the initial
/// input/cache counters and the final output count instead of dropping them.
#[test]
fn many_metadata_records_coalesce_without_losing_usage() {
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
	let addr = listener.local_addr().expect("addr");
	let server = std::thread::spawn(move || {
		let (mut socket, _) = listener.accept().expect("accept");
		let _ = read_request(&mut socket);
		let mut body = String::from(
			"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"usage\":{\"input_tokens\":11,\"cache_read_input_tokens\":5,\"cache_creation_input_tokens\":3,\"output_tokens\":0}}}\n\n",
		);
		for count in 1..=70 {
			body.push_str(&format!(
				"event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{}},\"usage\":{{\"output_tokens\":{count}}}}}\n\n"
			));
		}
		body.push_str(concat!(
			"event: content_block_start\n",
			"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
			"event: content_block_delta\n",
			"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"fast ok\"}}\n\n",
			"event: content_block_stop\n",
			"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
			"event: message_delta\n",
			"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7}}\n\n",
			"event: message_stop\n",
			"data: {\"type\":\"message_stop\"}\n\n"
		));
		respond(&mut socket, "200 OK", "text/event-stream", &body);
	});

	let runner = PiAgentRunner::new().expect("runner");
	let mut turn = None;
	for event in runner.run(&request(catalog(addr), &[])).expect("stream") {
		if let Ok(Event::MessageEnd { message, .. }) = event {
			turn = Some(message);
		}
	}
	let message = turn.expect("assistant turn");
	assert_eq!(message.content, "fast ok");
	let usage = message.usage.expect("usage survives the coalescing");
	assert_eq!(usage.input, 11, "initial input tokens are preserved");
	assert_eq!(usage.cache_read, 5, "initial cache read is preserved");
	assert_eq!(usage.cache_write, 3, "initial cache write is preserved");
	assert_eq!(usage.output, 7, "the final output count wins");
	server.join().expect("server");
}

/// A downgraded turn that continues with a tool call must keep the recorded
/// reset on the ancestry path: the follow-up assistant turn and the resumed
/// process both stay downgraded.
#[test]
fn downgrade_survives_a_tool_continuation_and_resume() {
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
	let addr = listener.local_addr().expect("addr");
	let (captured_tx, captured_rx) = mpsc::channel::<(String, String)>();
	let server = std::thread::spawn(move || {
		for index in 0..4 {
			let (mut socket, _) = listener.accept().expect("accept");
			let captured = read_request(&mut socket);
			captured_tx.send(captured).expect("capture");
			match index {
				0 => respond(&mut socket, "400 Bad Request", "application/json", FAST_REJECTION),
				1 => respond(&mut socket, "200 OK", "text/event-stream", TOOL_TURN),
				_ => respond(&mut socket, "200 OK", "text/event-stream", TURN),
			}
		}
	});

	let root = std::env::temp_dir().join(format!(
		"omp-anthropic-priority-tool-{}-{}",
		std::process::id(),
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("clock")
			.as_nanos()
	));
	let agent = root.join("agent");
	let cwd = root.join("work");
	let sessions = root.join("sessions");
	std::fs::create_dir_all(&agent).expect("agent dir");
	std::fs::create_dir_all(&cwd).expect("cwd");
	std::fs::write(
		agent.join("config.yml"),
		format!(
			"defaultModel: anthropic/claude-priority\nproviders:\n  anthropic:\n    \
			 baseUrl: \"http://{addr}/v1\"\n    api: anthropic-messages\n    apiKey: test-key\n"
		),
	)
	.expect("config.yml");
	std::fs::write(
		agent.join("models.yml"),
		format!(
			"providers:\n  anthropic:\n    baseUrl: \"http://{addr}/v1\"\n    \
			 api: anthropic-messages\n    apiKey: test-key\n    models:\n      - id: claude-priority\n\
			 \x20       contextWindow: 200000\n"
		),
	)
	.expect("models.yml");
	// Project settings.json is always merged (the legacy global file is skipped
	// once a main YAML exists), so the fast-mode selection lives here.
	std::fs::create_dir_all(cwd.join(".omp")).expect("project config dir");
	std::fs::write(
		cwd.join(".omp").join("settings.json"),
		"{\"tier\":{\"anthropic\":\"priority\"}}",
	)
	.expect("project settings.json");

	let run = |extra: &[&str]| {
		let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_omp"));
		command
			.env("HOME", root.join("home"))
			.env("PI_CODING_AGENT_DIR", &agent)
			.args(["--cwd", cwd.to_str().expect("cwd")])
			.args(["--session-dir", sessions.to_str().expect("sessions")])
			.args(["--no-title", "--approval-mode", "yolo"])
			.args(extra)
			.args(["-p", "run echo hi"])
			.stdin(std::process::Stdio::null());
		command.output().expect("run omp")
	};

	let first = run(&[]);
	assert!(
		first.status.success(),
		"first run failed: {}",
		String::from_utf8_lossy(&first.stderr)
	);
	let (_reject, _tool) = captured_rx.recv().expect("reject request");
	// Both the retried tool turn and the continuation omit fast mode.
	for label in ["retried tool turn", "continuation turn"] {
		let (head, body) = captured_rx.recv().expect("provider request");
		assert!(
			!head.to_ascii_lowercase().contains("fast-mode-2026-02-01"),
			"{label} must stay downgraded: {head}"
		);
		assert_eq!(body_field(&body, "speed"), None, "{label} must omit speed");
	}

	let second = run(&["--continue"]);
	assert!(
		second.status.success(),
		"second run failed: {}",
		String::from_utf8_lossy(&second.stderr)
	);
	let (_head, resumed) = captured_rx.recv().expect("resumed request");
	assert_eq!(
		body_field(&resumed, "speed"),
		None,
		"resume walks the reset entry and stays downgraded"
	);
	server.join().expect("server");
	let _ = std::fs::remove_dir_all(&root);
}

/// Project-level `tier.anthropic: none` clears a global `priority` selection,
/// matching TS `deepMerge` (settings.ts:2650) where an explicit "none" clears
/// the lower layer. Consumer-observable check: no fast mode is requested.
#[test]
fn project_layer_explicit_none_clears_global_priority_and_omits_fast_mode() {
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
	let addr = listener.local_addr().expect("addr");
	let (captured_tx, captured_rx) = mpsc::channel::<(String, String)>();
	let server = std::thread::spawn(move || {
		let (mut socket, _) = listener.accept().expect("accept");
		let captured = read_request(&mut socket);
		captured_tx.send(captured).expect("capture");
		respond(&mut socket, "200 OK", "text/event-stream", TURN);
	});

	let root = std::env::temp_dir().join(format!(
		"omp-anthropic-priority-project-clear-{}-{}",
		std::process::id(),
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("clock")
			.as_nanos()
	));
	let agent = root.join("agent");
	let cwd = root.join("work");
	let sessions = root.join("sessions");
	std::fs::create_dir_all(&agent).expect("agent dir");
	std::fs::create_dir_all(&cwd).expect("cwd");

	// Global/agent layer sets priority.
	std::fs::write(
		agent.join("config.yml"),
		format!(
			"defaultModel: anthropic/claude-priority\n\
			 tier:\n  anthropic: priority\n\
			 providers:\n  anthropic:\n    \
			 baseUrl: \"http://{addr}/v1\"\n    api: anthropic-messages\n    apiKey: test-key\n"
		),
	)
	.expect("config.yml");
	std::fs::write(
		agent.join("models.yml"),
		format!(
			"providers:\n  anthropic:\n    baseUrl: \"http://{addr}/v1\"\n    \
			 api: anthropic-messages\n    apiKey: test-key\n    models:\n      - id: claude-priority\n\
			 \x20       contextWindow: 200000\n"
		),
	)
	.expect("models.yml");

	// Project layer explicitly clears priority with "none".
	std::fs::create_dir_all(cwd.join(".omp")).expect("project config dir");
	std::fs::write(
		cwd.join(".omp").join("settings.json"),
		"{\"tier\":{\"anthropic\":\"none\"}}",
	)
	.expect("project settings.json");

	let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_omp"));
	command
		.env("HOME", root.join("home"))
		.env("PI_CODING_AGENT_DIR", &agent)
		.args(["--cwd", cwd.to_str().expect("cwd")])
		.args(["--session-dir", sessions.to_str().expect("sessions")])
		.args(["--no-title", "--approval-mode", "yolo"])
		.args(["-p", "answer quickly"])
		.stdin(std::process::Stdio::null());
	let output = command.output().expect("run omp");
	assert!(
		output.status.success(),
		"run failed: {}",
		String::from_utf8_lossy(&output.stderr)
	);

	let (head, body) = captured_rx.recv().expect("captured request");
	assert!(
		!head.to_ascii_lowercase().contains("fast-mode-2026-02-01"),
		"fast-mode beta must be omitted when project layer clears priority: {head}"
	);
	assert_eq!(
		body_field(&body, "speed"),
		None,
		"project layer 'none' must clear global priority tier"
	);
	server.join().expect("server");
	let _ = std::fs::remove_dir_all(&root);
}
