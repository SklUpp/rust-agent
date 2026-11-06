//! Consumer regression for C7 local learning: a real `omp` process with
//! `memory.backend: local` and `autolearn.enabled`, talking to a mock provider.
//!
//! Turn 1 captures a lesson plus a managed skill through the `learn` tool.
//! Turn 2 invokes `/skill:<name>` and must see the user-invocation template.
//! Turn 3 reads `skill://<name>` and must resolve through this session's
//! snapshot. Every turn also proves the prompt suffixes: the auto-learn
//! guidance from the mounted tools and the learned-lessons memory block.

use std::{
	fs,
	io::{Read, Write},
	net::{TcpListener, TcpStream},
	path::{Path, PathBuf},
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
		"omp-local-learning-{prefix}-{}-{}",
		std::process::id(),
		COUNTER.fetch_add(1, Ordering::Relaxed)
	));
	fs::create_dir_all(&dir).expect("create temp dir");
	dir
}

fn setup_agent_dir(dir: &Path, port: u16) {
	fs::create_dir_all(dir).expect("agent dir");
	fs::write(
		dir.join("config.yml"),
		format!(
			"defaultModel: mock/mock-model\nmemory:\n  backend: local\nautolearn:\n  enabled: true\nproviders:\n  mock:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n"
		),
	)
	.expect("config.yml");
	fs::write(
		dir.join("models.yml"),
		format!(
			"providers:\n  mock:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n    models:\n      - id: mock-model\n        contextWindow: 128000\n"
		),
	)
	.expect("models.yml");
}

struct MockModel {
	port: u16,
	stop: Arc<AtomicBool>,
	requests: Arc<Mutex<Vec<serde_json::Value>>>,
	accept: Option<thread::JoinHandle<()>>,
}

impl MockModel {
	fn start() -> Self {
		let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
		let port = listener.local_addr().expect("addr").port();
		listener.set_nonblocking(true).expect("nonblocking");
		let stop = Arc::new(AtomicBool::new(false));
		let requests = Arc::new(Mutex::new(Vec::new()));

		let accept = {
			let stop = Arc::clone(&stop);
			let requests = Arc::clone(&requests);
			thread::spawn(move || {
				let deadline = Instant::now() + Duration::from_secs(30);
				while !stop.load(Ordering::SeqCst) && Instant::now() < deadline {
					match listener.accept() {
						Ok((mut stream, _)) => {
							let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
							let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
							let body = read_request(&mut stream);
							if let Ok(body) = serde_json::from_slice::<serde_json::Value>(&body) {
								requests.lock().expect("requests").push(body.clone());
								let response = respond(&body);
								let _ = stream.write_all(
									format!(
										"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
										response.len(),
										response
									)
									.as_bytes(),
								);
								let _ = stream.flush();
							}
						},
						Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
							thread::sleep(Duration::from_millis(10));
						},
						Err(_) => break,
					}
				}
			})
		};

		Self { port, stop, requests, accept: Some(accept) }
	}
}

impl Drop for MockModel {
	fn drop(&mut self) {
		self.stop.store(true, Ordering::SeqCst);
		if let Some(handle) = self.accept.take() {
			let _ = handle.join();
		}
	}
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
	let header_end = header_end.expect("header terminator");
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

fn respond(request: &serde_json::Value) -> String {
	let last = request
		.get("messages")
		.and_then(serde_json::Value::as_array)
		.and_then(|messages| messages.last());
	let Some(last) = last else {
		return text_body("ack");
	};
	if last.get("role").and_then(serde_json::Value::as_str) != Some("user") {
		return text_body("ack");
	}
	let text = message_text(last);
	if text.contains("CAPTURE") {
		tool_call_body(
			"call-learn",
			"learn",
			&serde_json::json!({
				"memory": "Always run the formatter before committing",
				"context": "review turn",
			}),
		)
	} else if text.contains("CREATE THE SKILL") {
		tool_call_body(
			"call-manage",
			"manage_skill",
			&serde_json::json!({
				"action": "create",
				"name": "captured",
				"description": "Captured procedure",
				"body": "# Captured\nrun it",
			}),
		)
	} else if text.contains("READ THE SKILL") {
		tool_call_body("call-read", "read", &serde_json::json!({ "path": "skill://captured" }))
	} else if text.contains("READ MEMORY") {
		tool_call_body("call-memory", "read", &serde_json::json!({ "path": "memory://root" }))
	} else if text.contains("READ LEARNED") {
		tool_call_body(
			"call-learned",
			"read",
			&serde_json::json!({ "path": "memory://root/learned.md" }),
		)
	} else if text.contains("SPAWN CHILD") {
		tool_call_body(
			"call-task",
			"task",
			&serde_json::json!({
				"name": "child",
				"agent": "child",
				"task": "inspect the repo",
			}),
		)
	} else {
		text_body("ack")
	}
}

fn tool_call_body(id: &str, name: &str, args: &serde_json::Value) -> String {
	let arguments = serde_json::to_string(&args.to_string()).expect("encode arguments");
	format!(
		"data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{{\"index\":0,\"id\":\"{id}\",\"type\":\"function\",\"function\":{{\"name\":\"{name}\",\"arguments\":{arguments}}}}}]}},\"finish_reason\":null}}]}}\n\ndata: [DONE]\n\n"
	)
}

fn text_body(text: &str) -> String {
	format!(
		"data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}\"}}}}]}}\n\ndata: [DONE]\n\n"
	)
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
						.or_else(|| tool.get("name"))
						.and_then(serde_json::Value::as_str)
						.map(str::to_string)
				})
				.collect()
		})
		.unwrap_or_default()
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

fn system_text(request: &serde_json::Value) -> String {
	request
		.get("messages")
		.and_then(serde_json::Value::as_array)
		.map(|messages| {
			messages
				.iter()
				.filter(|message| {
					matches!(
						message.get("role").and_then(serde_json::Value::as_str),
						Some("system" | "developer")
					)
				})
				.map(message_text)
				.collect::<Vec<_>>()
				.join("\n")
		})
		.unwrap_or_default()
}

fn request_diagnostics(requests: &[serde_json::Value], stderr: &str) -> String {
	let mut out = format!("stderr:\n{stderr}\nrequests: {}\n", requests.len());
	for (index, request) in requests.iter().enumerate() {
		out.push_str(&format!("request {index}: {:?}\n", tool_names(request)));
		if let Some(messages) = request.get("messages").and_then(serde_json::Value::as_array) {
			for message in messages {
				let role = message.get("role").and_then(serde_json::Value::as_str).unwrap_or("?");
				let text = message_text(message);
				let excerpt: String = text.chars().take(240).collect();
				out.push_str(&format!("  {role}: {excerpt}\n"));
			}
		}
	}
	out
}

fn find_file(root: &Path, name: &str) -> Option<PathBuf> {
	let entries = fs::read_dir(root).ok()?;
	for entry in entries.flatten() {
		let path = entry.path();
		if path.is_dir() {
			if let Some(found) = find_file(&path, name) {
				return Some(found);
			}
		} else if path.file_name().and_then(|value| value.to_str()) == Some(name) {
			return Some(path);
		}
	}
	None
}

#[test]
fn local_learning_capture_skill_command_and_uri_resolve() {
	let server = MockModel::start();
	let temp = temp_dir("wiring");
	let agent_dir = temp.join("agent");
	let sessions_dir = temp.join("sessions");
	let share_dir = temp.join("share");
	let config_dir = temp.join("config");
	let cwd = temp.join("project");
	for dir in [&sessions_dir, &share_dir, &config_dir, &cwd] {
		fs::create_dir_all(dir).expect("fixture dir");
	}
	setup_agent_dir(&agent_dir, server.port);

	let output = ProcCommand::new(env!("CARGO_BIN_EXE_omp"))
		.env("PI_CODING_AGENT_DIR", &agent_dir)
		.env("PI_CONFIG_DIR", &config_dir)
		.env("OMP_SHARE_DIR", &share_dir)
		.env("PI_NO_TITLE", "1")
		.stdin(Stdio::null())
		.args(["--cwd", cwd.to_str().unwrap()])
		.args(["--session-dir", sessions_dir.to_str().unwrap()])
		.args(["--no-session", "--tools", "read,learn,manage_skill", "--yolo"])
		.args(["--max-time", "20s", "--mode", "text", "-p"])
		.args([
			"CAPTURE the lesson",
			"CREATE THE SKILL now",
			"USE /skill:captured now",
			"READ THE SKILL",
		])
		.output()
		.expect("run omp with four prompts");

	let stderr = String::from_utf8_lossy(&output.stderr);
	assert_eq!(output.status.code(), Some(0), "prompts must succeed: {stderr}");

	// Capture side effects: learned.md and the managed skill exist under the
	// isolated HOME.
	let learned = find_file(&agent_dir.join("memories"), "learned.md")
		.expect("learn writes learned.md");
	let learned_text = fs::read_to_string(&learned).expect("learned.md");
	assert!(learned_text.contains("Always run the formatter before committing"));
	let skill_path = agent_dir.join("managed-skills/captured/SKILL.md");
	let skill_text = fs::read_to_string(&skill_path).expect("managed skill written");
	assert!(skill_text.contains("# Captured"));

	let requests = server.requests.lock().expect("requests").clone();
	assert_eq!(
		requests.len(),
		7,
		"four prompts: capture+skill tool round-trips plus two final turns"
	);

	let system = |index: usize| {
		requests[index]
			.get("messages")
			.and_then(serde_json::Value::as_array)
			.and_then(|messages| messages.first())
			.map(message_text)
			.unwrap_or_default()
	};
	// The auto-learn guidance follows the mounted tools from turn one.
	assert!(
		system(0).contains("## Auto-Learn (experimental)"),
		"manage_skill/learn guidance missing: {}",
		system(0)
	);
	// Learned instructions are re-read between turns, so turn two sees them.
	assert!(
		system(2).contains("# Memory Guidance"),
		"learned instructions missing from the next turn: {}",
		system(2)
	);
	assert!(system(2).contains("Always run the formatter before committing"));

	// `/skill:captured` is expanded to the user-invocation template before the
	// provider call, including a fresh read of the managed file.
	let turn_three_user = requests[4]
		.get("messages")
		.and_then(serde_json::Value::as_array)
		.and_then(|messages| messages.last())
		.map(message_text)
		.unwrap_or_default();
	assert!(
		turn_three_user.contains("User invoked the \"captured\" skill"),
		"skill command was not expanded: {turn_three_user}"
	);
	assert!(turn_three_user.contains("# Captured\nrun it"));

	// `skill://captured` resolves through the session snapshot and returns the
	// current file body.
	let read_result = requests[6]
		.get("messages")
		.and_then(serde_json::Value::as_array)
		.and_then(|messages| messages.last())
		.map(message_text)
		.unwrap_or_default();
	assert_eq!(
		requests[6]
			.get("messages")
			.and_then(serde_json::Value::as_array)
			.and_then(|messages| messages.last())
			.and_then(|message| message.get("name"))
			.and_then(serde_json::Value::as_str),
		Some("read")
	);
	assert!(
		read_result.contains("# Captured\nrun it"),
		"skill:// did not resolve to the managed skill: {read_result}"
	);
}

#[test]
fn memory_root_reads_and_exact_guidance() {
	let server = MockModel::start();
	let temp = temp_dir("memory");
	let agent_dir = temp.join("agent");
	let cwd = temp.join("project");
	let sessions_dir = temp.join("sessions");
	let share_dir = temp.join("share");
	let config_dir = temp.join("config");
	for dir in [&sessions_dir, &share_dir, &config_dir, &cwd] {
		fs::create_dir_all(dir).expect("fixture dir");
	}
	setup_agent_dir(&agent_dir, server.port);
	let memory_root = omp_cli::local_memory::memory_root(&agent_dir, &cwd);
	fs::create_dir_all(&memory_root).expect("memory root");
	fs::write(memory_root.join("memory_summary.md"), "summary text").expect("summary");
	fs::write(memory_root.join("learned.md"), "lesson from disk").expect("learned");

	let output = ProcCommand::new(env!("CARGO_BIN_EXE_omp"))
		.env("PI_CODING_AGENT_DIR", &agent_dir)
		.env("PI_CONFIG_DIR", &config_dir)
		.env("OMP_SHARE_DIR", &share_dir)
		.env("PI_NO_TITLE", "1")
		.stdin(Stdio::null())
		.args(["--cwd", cwd.to_str().unwrap()])
		.args(["--session-dir", sessions_dir.to_str().unwrap()])
		.args(["--no-session", "--tools", "read", "--yolo"])
		.args(["--max-time", "20s", "--mode", "text", "-p"])
		.args(["READ MEMORY", "READ LEARNED"])
		.output()
		.expect("run omp memory reads");
	assert_eq!(
		output.status.code(),
		Some(0),
		"{}",
		String::from_utf8_lossy(&output.stderr)
	);

	let requests = server.requests.lock().expect("requests").clone();
	let system = requests[0]
		.get("messages")
		.and_then(serde_json::Value::as_array)
		.and_then(|messages| messages.first())
		.map(message_text)
		.unwrap_or_default();
	assert!(system.contains("# Memory Guidance"), "{system}");
	assert!(system.contains("Root: memory://root"), "{system}");
	assert!(system.contains("1. Read `memory://root/memory_summary.md` first."), "{system}");
	assert!(system.contains("Memory summary:\nsummary text"), "{system}");
	assert!(system.contains("Learned lessons"), "{system}");
	assert!(system.contains("lesson from disk"), "{system}");

	let summary_result = requests[1]
		.get("messages")
		.and_then(serde_json::Value::as_array)
		.and_then(|messages| messages.last())
		.map(message_text)
		.unwrap_or_default();
	assert!(summary_result.contains("summary text"), "{summary_result}");
	let learned_result = requests[3]
		.get("messages")
		.and_then(serde_json::Value::as_array)
		.and_then(|messages| messages.last())
		.map(message_text)
		.unwrap_or_default();
	assert!(learned_result.contains("lesson from disk"), "{learned_result}");
}

#[test]
fn autoload_skills_reach_the_child_conversation_once() {
	let server = MockModel::start();
	let temp = temp_dir("autoload");
	let agent_dir = temp.join("agent");
	let cwd = temp.join("project");
	let sessions_dir = temp.join("sessions");
	let share_dir = temp.join("share");
	let config_dir = temp.join("config");
	for dir in [&sessions_dir, &share_dir, &config_dir, &cwd] {
		fs::create_dir_all(dir).expect("fixture dir");
	}
	setup_agent_dir(&agent_dir, server.port);
	fs::create_dir_all(cwd.join(".omp/agents")).expect("agents dir");
	fs::write(
		cwd.join(".omp/agents/child.md"),
		"---\nname: child\ndescription: Child agent for autoload tests\nautoloadSkills: [captured]\n---\nChild marker instructions.\n",
	)
	.expect("agent md");

	let output = ProcCommand::new(env!("CARGO_BIN_EXE_omp"))
		.env("PI_CODING_AGENT_DIR", &agent_dir)
		.env("PI_CONFIG_DIR", &config_dir)
		.env("OMP_SHARE_DIR", &share_dir)
		.env("PI_NO_TITLE", "1")
		.stdin(Stdio::null())
		.args(["--cwd", cwd.to_str().unwrap()])
		.args(["--session-dir", sessions_dir.to_str().unwrap()])
		.args(["--no-session", "--tools", "task,read,learn,manage_skill", "--yolo"])
		.args(["--max-time", "30s", "--mode", "text", "-p"])
		.args(["CREATE THE SKILL now", "SPAWN CHILD now"])
		.output()
		.expect("run omp autoload");
	assert_eq!(
		output.status.code(),
		Some(0),
		"{}",
		String::from_utf8_lossy(&output.stderr)
	);

	let requests = server.requests.lock().expect("requests").clone();
	let stderr = String::from_utf8_lossy(&output.stderr);
	let child_request = requests
		.iter()
		.find(|request| system_text(request).contains("Child marker instructions."))
		.unwrap_or_else(|| {
			panic!(
				"child request reached the provider; {}",
				request_diagnostics(&requests, &stderr)
			)
		});
	let child_tools = tool_names(child_request);
	assert!(
		!child_tools.iter().any(|name| name == "learn" || name == "manage_skill"),
		"top-level autolearn tools must not mount for children: {child_tools:?}"
	);
	let messages = child_request
		.get("messages")
		.and_then(serde_json::Value::as_array)
		.expect("messages");
	let autoload_index = messages
		.iter()
		.position(|message| message_text(message).contains("# Captured\nrun it"))
		.expect("autoload message in child conversation");
	let assignment_index = messages
		.iter()
		.position(|message| message_text(message).contains("Complete assignment thoroughly:"))
		.expect("assignment message");
	assert!(
		autoload_index < assignment_index,
		"autoload must precede the assignment"
	);
	assert_eq!(
		messages[autoload_index].get("role").and_then(serde_json::Value::as_str),
		Some("developer"),
		"autoload is hidden developer guidance, not a user turn"
	);
	assert_eq!(
		messages[assignment_index].get("role").and_then(serde_json::Value::as_str),
		Some("user"),
		"the assignment is the only user message"
	);
	assert_eq!(
		messages
			.iter()
			.filter(|message| {
				message.get("role").and_then(serde_json::Value::as_str) == Some("user")
			})
			.count(),
		1,
		"autoload must not add a second user turn"
	);
	assert_eq!(
		messages
			.iter()
			.filter(|message| message_text(message).contains("Complete assignment thoroughly:"))
			.count(),
		1,
		"assignment must not be duplicated"
	);
	assert_eq!(
		messages
			.iter()
			.filter(|message| message_text(message).contains("# Captured\nrun it"))
			.count(),
		1,
		"autoload body must appear once"
	);
}

#[test]
fn local_autolearn_tools_validate_only_when_mounted() {
	let server = MockModel::start();
	let temp = temp_dir("autolearn-tools");
	let agent_dir = temp.join("agent");
	let cwd = temp.join("project");
	let sessions = temp.join("sessions");
	let share = temp.join("share");
	let config_dir = temp.join("config");
	for dir in [&sessions, &share, &config_dir, &cwd] {
		fs::create_dir_all(dir).expect("fixture dir");
	}
	setup_agent_dir(&agent_dir, server.port);

	let run = |tools: &str, prompt: &str| {
		ProcCommand::new(env!("CARGO_BIN_EXE_omp"))
			.env("PI_CODING_AGENT_DIR", &agent_dir)
			.env("PI_CONFIG_DIR", &config_dir)
			.env("OMP_SHARE_DIR", &share)
			.env("PI_NO_TITLE", "1")
			.stdin(Stdio::null())
			.args(["--cwd", cwd.to_str().unwrap()])
			.args(["--session-dir", sessions.to_str().unwrap()])
			.args(["--no-session", "--tools", tools, "--yolo"])
			.args(["--max-time", "20s", "--mode", "text", "-p", prompt])
			.output()
			.expect("run omp")
	};

	let valid = run("read,learn,manage_skill", "hello");
	assert_eq!(
		valid.status.code(),
		Some(0),
		"{}",
		String::from_utf8_lossy(&valid.stderr)
	);
	let standalone = run("read,learn", "CAPTURE now");
	assert_eq!(
		standalone.status.code(),
		Some(0),
		"learn mounts without manage_skill upstream: {}",
		String::from_utf8_lossy(&standalone.stderr)
	);
	let requests = server.requests.lock().expect("requests").clone();
	let standalone_tools = tool_names(requests.last().expect("standalone request"));
	assert!(
		standalone_tools.iter().any(|name| name == "learn"),
		"standalone learn must be mounted: {standalone_tools:?}"
	);
	assert!(
		!standalone_tools.iter().any(|name| name == "manage_skill"),
		"manage_skill was not selected: {standalone_tools:?}"
	);
	let learned = find_file(&agent_dir, "learned.md").expect("standalone learn wrote learned.md");
	assert!(
		fs::read_to_string(learned)
			.expect("learned")
			.contains("Always run the formatter before committing"),
		"standalone learn call must persist the lesson"
	);

	let config = fs::read_to_string(agent_dir.join("config.yml")).expect("config");
	fs::write(
		agent_dir.join("config.yml"),
		config.replace("backend: local", "backend: off"),
	)
	.expect("disable memory");
	let disabled = run("read,learn,manage_skill", "hello");
	assert_ne!(disabled.status.code(), Some(0), "learn must not run without local memory");
	let stderr = String::from_utf8_lossy(&disabled.stderr);
	assert!(stderr.contains("learn"), "{stderr}");
	assert!(stderr.contains("memory backend"), "{stderr}");
	let manage_only = run("read,manage_skill", "hello");
	assert_eq!(
		manage_only.status.code(),
		Some(0),
		"manage_skill stays valid without a memory backend: {}",
		String::from_utf8_lossy(&manage_only.stderr)
	);

	let config = fs::read_to_string(agent_dir.join("config.yml")).expect("config");
	fs::write(
		agent_dir.join("config.yml"),
		config.replace("autolearn:\n  enabled: true", "autolearn:\n  enabled: false"),
	)
	.expect("disable autolearn");
	let no_autolearn = run("read,manage_skill", "hello");
	assert_ne!(
		no_autolearn.status.code(),
		Some(0),
		"manage_skill must report disabled autolearn"
	);
	let stderr = String::from_utf8_lossy(&no_autolearn.stderr);
	assert!(stderr.contains("autolearn.enabled"), "{stderr}");
}

#[test]
fn disabled_extensions_remove_a_skill_from_every_surface() {
	let server = MockModel::start();
	let temp = temp_dir("disabled-skill");
	let agent_dir = temp.join("agent");
	let cwd = temp.join("project");
	let sessions_dir = temp.join("sessions");
	let share_dir = temp.join("share");
	let config_dir = temp.join("config");
	for dir in [&sessions_dir, &share_dir, &config_dir, &cwd] {
		fs::create_dir_all(dir).expect("fixture dir");
	}
	setup_agent_dir(&agent_dir, server.port);
	let config = fs::read_to_string(agent_dir.join("config.yml")).expect("config.yml");
	fs::write(
		agent_dir.join("config.yml"),
		format!("{config}disabledExtensions:\n  - skill:blocked\n"),
	)
	.expect("disabledExtensions");
	fs::create_dir_all(cwd.join(".omp/skills/blocked")).expect("skill dir");
	fs::write(
		cwd.join(".omp/skills/blocked/SKILL.md"),
		"---\nname: blocked\ndescription: Disabled project skill\n---\nBLOCKED BODY MARKER\n",
	)
	.expect("skill md");
	fs::create_dir_all(cwd.join(".omp/skills/visible")).expect("visible skill dir");
	fs::write(
		cwd.join(".omp/skills/visible/SKILL.md"),
		"---\nname: visible\ndescription: Enabled project skill\n---\nVISIBLE BODY MARKER\n",
	)
	.expect("visible skill md");

	let output = ProcCommand::new(env!("CARGO_BIN_EXE_omp"))
		.env("PI_CODING_AGENT_DIR", &agent_dir)
		.env("PI_CONFIG_DIR", &config_dir)
		.env("OMP_SHARE_DIR", &share_dir)
		.env("PI_NO_TITLE", "1")
		.stdin(Stdio::null())
		.args(["--cwd", cwd.to_str().unwrap()])
		.args(["--session-dir", sessions_dir.to_str().unwrap()])
		.args(["--no-session", "--tools", "read", "--yolo"])
		.args(["--max-time", "20s", "--mode", "text", "-p"])
		.args(["/skill:blocked now"])
		.output()
		.expect("run omp with a disabled skill");
	assert_eq!(
		output.status.code(),
		Some(0),
		"{}",
		String::from_utf8_lossy(&output.stderr)
	);
	let requests = server.requests.lock().expect("requests").clone();
	assert!(!requests.is_empty(), "provider was reached");
	let system = system_text(&requests[0]);
	assert!(!system.is_empty(), "prompt listing/system prompt was not observed");
	assert!(
		system.contains("visible"),
		"enabled sibling skill must appear in the prompt listing: {system}"
	);
	assert!(!system.contains("blocked"), "prompt listing must omit disabled skill: {system}");
	let user = requests[0]
		.get("messages")
		.and_then(serde_json::Value::as_array)
		.and_then(|messages| {
			messages
				.iter()
				.rev()
				.find(|message| message.get("role").and_then(serde_json::Value::as_str) == Some("user"))
		})
		.map(message_text)
		.unwrap_or_default();
	assert_eq!(user, "/skill:blocked now", "disabled slash command stays literal");
	assert!(
		!requests
			.iter()
			.any(|request| serde_json::to_string(request)
				.expect("request json")
				.contains("BLOCKED BODY MARKER")),
		"disabled skill body must never reach the provider"
	);
	assert!(
		!requests
			.iter()
			.any(|request| serde_json::to_string(request)
				.expect("request json")
				.contains("skill://blocked")),
		"disabled skill URI must stay unavailable"
	);
}
