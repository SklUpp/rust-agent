//! Consumer check for the C7 capture capability ceiling.
//!
//! A normal turn still mounts file-discovered MCP tools; the auto-continue
//! capture turn must not mount them nor spawn the server (marker stays at one
//! line). Nothing here changes ordinary MCP selection: the same binary, config
//! and session serve both requests.

use std::{
	fs,
	io::{Read, Write},
	net::{TcpListener, TcpStream},
	path::PathBuf,
	process::{Command as ProcCommand, Stdio},
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, AtomicU32, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_dir(prefix: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"omp-capture-mcp-{prefix}-{}-{}",
		std::process::id(),
		COUNTER.fetch_add(1, Ordering::Relaxed)
	));
	fs::create_dir_all(&dir).expect("temp dir");
	dir
}

fn read_request(stream: &mut TcpStream) -> Vec<u8> {
	let mut buf = Vec::new();
	let mut chunk = [0u8; 1024];
	let mut header_end = None;
	while header_end.is_none() {
		match stream.read(&mut chunk) {
			Ok(0) | Err(_) => return buf,
			Ok(n) => {
				buf.extend_from_slice(&chunk[..n]);
				header_end = buf.windows(4).position(|window| window == b"\r\n\r\n");
			},
		}
	}
	let header_end = header_end.expect("headers");
	let headers = String::from_utf8_lossy(&buf[..header_end]).into_owned();
	let content_length = headers
		.lines()
		.find_map(|line| {
			let (name, value) = line.split_once(':')?;
			name.trim()
				.eq_ignore_ascii_case("content-length")
				.then(|| value.trim().parse::<usize>().ok())?
		})
		.unwrap_or(0);
	let mut body = buf[header_end + 4..].to_vec();
	while body.len() < content_length {
		match stream.read(&mut chunk) {
			Ok(0) | Err(_) => break,
			Ok(n) => body.extend_from_slice(&chunk[..n]),
		}
	}
	body
}

fn text_body(text: &str) -> String {
	format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}\"}}}}]}}\n\ndata: [DONE]\n\n")
}

fn learn_call() -> String {
	let arguments = serde_json::to_string(&serde_json::json!({
		"memory": "Capture ceiling marker",
		"context": "capture-mcp",
	})
	.to_string())
	.expect("encode");
	format!(
		"data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{{\"index\":0,\"id\":\"call-learn\",\"type\":\"function\",\"function\":{{\"name\":\"learn\",\"arguments\":{arguments}}}}}]}},\"finish_reason\":null}}]}}\n\ndata: [DONE]\n\n"
	)
}

fn message_text(message: &serde_json::Value) -> String {
	match message.get("content") {
		Some(serde_json::Value::String(text)) => text.clone(),
		Some(serde_json::Value::Array(parts)) => parts
			.iter()
			.filter_map(|part| part.get("text").and_then(serde_json::Value::as_str))
			.collect::<Vec<_>>()
			.join("\n"),
		_ => String::new(),
	}
}

fn request_mentions(request: &serde_json::Value, needle: &str) -> bool {
	request
		.get("messages")
		.and_then(serde_json::Value::as_array)
		.is_some_and(|messages| {
			messages
				.iter()
				.any(|message| message_text(message).contains(needle))
		})
}

fn tool_names(request: &serde_json::Value) -> Vec<String> {
	request
		.get("tools")
		.and_then(serde_json::Value::as_array)
		.map(|tools| {
			tools
				.iter()
				.filter_map(|tool| {
					tool
						.get("function")
						.and_then(|function| function.get("name"))
						.and_then(serde_json::Value::as_str)
				})
				.map(str::to_string)
				.collect()
		})
		.unwrap_or_default()
}

#[test]
fn capture_turn_excludes_mcp_while_normal_turn_keeps_it() {
	let temp = temp_dir("ceiling");
	let agent_dir = temp.join("agent");
	let project = temp.join("project");
	let sessions = temp.join("sessions");
	let share = temp.join("share");
	let config_dir = temp.join("config");
	let marker = temp.join("mcp-started.txt");
	let script = temp.join("mcp_server.py");
	for dir in [&agent_dir, &project, &sessions, &share, &config_dir] {
		fs::create_dir_all(dir).expect("dir");
	}
	fs::write(
		&script,
		r#"
import json, sys
open(sys.argv[1], "a").write("started\n")
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        result = {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "mock", "version": "1"}}
    elif method == "tools/list":
        result = {"tools": [{"name": "echo", "description": "echo", "inputSchema": {"type": "object", "properties": {}}}]}
    else:
        continue
    open(sys.argv[1], "a").write(method + "\n")
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": message["id"], "result": result}) + "\n")
    sys.stdout.flush()
"#,
	)
	.expect("mcp script");
	fs::create_dir_all(project.join(".omp")).expect("omp");
	fs::write(
		project.join(".omp/mcp.json"),
		serde_json::json!({
			"mcpServers": {
				"mock": {
					"command": "python3",
					"args": [script.to_str().unwrap(), marker.to_str().unwrap()],
				}
			}
		})
		.to_string(),
	)
	.expect("mcp.json");

	// Mock provider records every request; one learn tool call in the first turn.
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
	let port = listener.local_addr().expect("addr").port();
	listener.set_nonblocking(true).expect("nonblocking");
	let stop = Arc::new(AtomicBool::new(false));
	let requests = Arc::new(Mutex::new(Vec::new()));
	let accept = {
		let stop = Arc::clone(&stop);
		let requests = Arc::clone(&requests);
		thread::spawn(move || {
			let deadline = Instant::now() + Duration::from_secs(40);
			while !stop.load(Ordering::SeqCst) && Instant::now() < deadline {
				match listener.accept() {
					Ok((mut stream, _)) => {
						let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
						let body = read_request(&mut stream);
						let Ok(body) = serde_json::from_slice::<serde_json::Value>(&body) else {
							continue;
						};
						requests.lock().expect("requests").push(body.clone());
						let last = body
							.get("messages")
							.and_then(serde_json::Value::as_array)
							.and_then(|messages| messages.last());
						let data = match last {
							Some(last)
								if last.get("role").and_then(serde_json::Value::as_str) == Some("user")
									&& message_text(last).contains("TOOL") =>
							{
								learn_call()
							},
							_ => text_body("ack"),
						};
						let _ = stream.write_all(
							format!(
								"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
								data.len(),
								data
							)
							.as_bytes(),
						);
					},
					Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
						thread::sleep(Duration::from_millis(10));
					},
					Err(_) => break,
				}
			}
		})
	};

	fs::write(
		agent_dir.join("config.yml"),
		format!(
			"defaultModel: mock/mock-model\nmodelRoles:\n  advisor: mock/advisor\nadvisor:\n  enabled: true\nmemory:\n  backend: local\nautolearn:\n  enabled: true\n  autoContinue: true\n  minToolCalls: 1\nproviders:\n  mock:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n"
		),
	)
	.expect("config");
	fs::write(
		agent_dir.join("models.yml"),
		format!(
			"providers:\n  mock:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n    models:\n      - id: mock-model\n        contextWindow: 128000\n      - id: advisor\n        contextWindow: 128000\n"
		),
	)
	.expect("models");

	let output = ProcCommand::new(env!("CARGO_BIN_EXE_omp"))
		.env("PI_CODING_AGENT_DIR", &agent_dir)
		.env("PI_CONFIG_DIR", &config_dir)
		.env("OMP_SHARE_DIR", &share)
		.env("PI_NO_TITLE", "1")
		.stdin(Stdio::null())
		.args(["--cwd", project.to_str().unwrap()])
		.args(["--session-dir", sessions.to_str().unwrap()])
		.args(["--no-session", "--advisor", "--tools", "read,learn,manage_skill,mcp__mock_echo", "--yolo"])
		.args(["--max-time", "30s", "--mode", "text", "-p"])
		.args(["TOOL first"])
		.output()
		.expect("run omp capture");

	stop.store(true, Ordering::SeqCst);
	let _ = accept.join();
	let diagnostics = format!(
		"stderr:\n{}\nmcp.json:\n{}\nconfig.yml:\n{}\nmarker exists: {}\nmarker:\n{}",
		String::from_utf8_lossy(&output.stderr),
		fs::read_to_string(project.join(".omp/mcp.json")).unwrap_or_default(),
		fs::read_to_string(agent_dir.join("config.yml")).unwrap_or_default(),
		marker.exists(),
		fs::read_to_string(&marker).unwrap_or_default(),
	);
	assert_eq!(output.status.code(), Some(0), "{diagnostics}");

	let requests = requests.lock().expect("requests").clone();
	assert!(requests.len() >= 3, "tool turn, continuation and capture calls");
	// The advisor is asynchronous, so request order is not a stable identity:
	// find the capture by its nudge and a normal turn by model/non-capture.
	let nudge = omp_cli::autolearn::auto_learn_nudge();
	let capture_request = requests
		.iter()
		.find(|request| request_mentions(request, nudge))
		.expect("capture request");
	let capture = tool_names(capture_request);
	assert!(
		capture.iter().all(|name| !name.starts_with("mcp__")),
		"capture turn must not mount MCP tools: {capture:?}"
	);
	assert!(capture.iter().any(|name| name == "learn"), "{capture:?}");
	let normal_request = requests
		.iter()
		.find(|request| {
			request.get("model").and_then(serde_json::Value::as_str) == Some("mock-model")
				&& !request_mentions(request, nudge)
		})
		.expect("normal primary request");
	let normal = tool_names(normal_request);
	assert!(
		normal.iter().any(|name| name.starts_with("mcp__")),
		"normal turn must keep MCP tools: {normal:?}"
	);
	// The advisor is enabled for the session, so the normal turn must still
	// reach its own provider route; the capture must not start one.
	let advisor_requests: Vec<&serde_json::Value> = requests
		.iter()
		.filter(|request| {
			request.get("model").and_then(serde_json::Value::as_str) == Some("advisor")
		})
		.collect();
	assert!(
		!advisor_requests.is_empty(),
		"normal root advisor must still run with --advisor"
	);
	assert!(
		advisor_requests
			.iter()
			.all(|request| !request_mentions(request, nudge)),
		"capture must not call the advisor executor/provider"
	);
	let started = fs::read_to_string(&marker).unwrap_or_default();
	let lines: Vec<&str> = started.lines().collect();
	assert_eq!(
		lines.iter().filter(|line| **line == "started").count(),
		1,
		"capture must not spawn a second MCP server: {started}"
	);
	assert!(lines.contains(&"initialize"), "MCP initialize was not observed: {started}");
	assert!(lines.contains(&"tools/list"), "MCP tools/list was not observed: {started}");
}
