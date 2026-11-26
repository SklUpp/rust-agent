//! Durable-runtime crash tests with a real `arag` process.
//!
//! The fixture stores the exact pre-crash state a SIGKILL can leave behind:
//! an accepted peer delivery with no persisted message, and an open child
//! admission. A live `arag --resume` is then killed while its provider request
//! is in flight, and a second resume must deliver/abandon exactly once.
#![cfg(unix)]

use std::{
	io::{Read, Write},
	net::TcpListener,
	path::{Path, PathBuf},
	process::{Child, Command, Stdio},
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, Ordering},
	},
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use aragent::{
	durable_session::DurableSession,
	output,
	session::{CURRENT_SESSION_VERSION, SessionHeader},
	session_store::SqliteStore,
};

fn temp_dir(label: &str) -> PathBuf {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_nanos())
		.unwrap_or_default();
	let dir = std::env::temp_dir().join(format!(
		"omp-task-kill-{label}-{}-{nanos}",
		std::process::id()
	));
	std::fs::create_dir_all(&dir).expect("create temp dir");
	dir
}

fn header(id: &str, cwd: &Path) -> SessionHeader {
	SessionHeader {
		kind: "session".to_string(),
		version: CURRENT_SESSION_VERSION,
		id: id.to_string(),
		timestamp: output::iso8601(SystemTime::now()),
		cwd: cwd.display().to_string(),
		model: None,
		provider: None,
		title: None,
		title_source: None,
		additional_directories: Vec::new(),
		parent_session: None,
		previous_session_files: Vec::new(),
		provider_prompt_cache_key: None,
	}
}

/// Minimal OpenAI-compatible SSE endpoint. In blocking mode it records the
/// request and never answers, so the test can kill a real in-flight turn.
struct MockProvider {
	port: u16,
	requests: Arc<Mutex<Vec<String>>>,
	stop: Arc<AtomicBool>,
	handle: Option<std::thread::JoinHandle<()>>,
}

impl MockProvider {
	fn start(respond: bool) -> Self {
		let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock provider");
		listener
			.set_nonblocking(true)
			.expect("nonblocking mock provider");
		let port = listener.local_addr().expect("mock addr").port();
		let requests = Arc::new(Mutex::new(Vec::new()));
		let stop = Arc::new(AtomicBool::new(false));
		let requests_for_thread = Arc::clone(&requests);
		let stop_for_thread = Arc::clone(&stop);
		let handle = std::thread::spawn(move || {
			while !stop_for_thread.load(Ordering::SeqCst) {
				match listener.accept() {
					Ok((mut stream, _)) => {
						let requests = Arc::clone(&requests_for_thread);
						let stop = Arc::clone(&stop_for_thread);
						std::thread::spawn(move || {
							let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
							let mut raw = Vec::new();
							let mut buffer = [0u8; 4096];
							let mut body = String::new();
							loop {
								let read = stream.read(&mut buffer).unwrap_or(0);
								if read == 0 {
									break;
								}
								raw.extend_from_slice(&buffer[..read]);
								if let Some(split) =
									raw.windows(4).position(|window| window == b"\r\n\r\n")
								{
									let head = String::from_utf8_lossy(&raw[..split]).to_string();
									let length = head
										.lines()
										.find_map(|line| {
											let (name, value) = line.split_once(':')?;
											name.eq_ignore_ascii_case("content-length")
												.then(|| value.trim().parse::<usize>().ok())?
										})
										.unwrap_or(0);
									let start = split + 4;
									while raw.len() < start + length {
										let read = stream.read(&mut buffer).unwrap_or(0);
										if read == 0 {
											break;
										}
										raw.extend_from_slice(&buffer[..read]);
									}
									body = String::from_utf8_lossy(&raw[start..]).to_string();
									break;
								}
							}
							requests.lock().expect("requests").push(body);
							if respond {
								let payload = concat!(
									"data: {\"choices\":[{\"delta\":{\"content\":\"mock answer\"}}]}\n\n",
									"data: [DONE]\n\n"
								);
								let response = format!(
									"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
									payload.len(),
									payload
								);
								let _ = stream.write_all(response.as_bytes());
								let _ = stream.flush();
							} else {
								// Hold the turn open until the test kills the process.
								while !stop.load(Ordering::SeqCst) {
									std::thread::sleep(Duration::from_millis(20));
								}
							}
						});
					},
					Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
						std::thread::sleep(Duration::from_millis(10));
					},
					Err(_) => break,
				}
			}
		});
		Self { port, requests, stop, handle: Some(handle) }
	}

	fn wait_requests(&self, count: usize) {
		let deadline = Instant::now() + Duration::from_secs(20);
		loop {
			if self.requests.lock().expect("requests").len() >= count {
				return;
			}
			assert!(Instant::now() < deadline, "mock provider received no request");
			std::thread::sleep(Duration::from_millis(20));
		}
	}

	fn bodies(&self) -> Vec<String> {
		self.requests.lock().expect("requests").clone()
	}
}

impl Drop for MockProvider {
	fn drop(&mut self) {
		self.stop.store(true, Ordering::SeqCst);
		// Wake a pending `accept` on the nonblocking listener.
		let _ = std::net::TcpStream::connect(("127.0.0.1", self.port));
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}
	}
}

fn write_config(agent_dir: &Path, port: u16) {
	let config = format!(
		"defaultModel: mock/mock-model\nproviders:\n  mock:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n"
	);
	std::fs::write(agent_dir.join("config.yml"), config).expect("config.yml");
	let models = format!(
		"providers:\n  mock:\n    baseUrl: \"http://127.0.0.1:{port}/v1\"\n    api: openai-completions\n    apiKey: test-key\n    models:\n      - id: mock-model\n        contextWindow: 128000\n"
	);
	std::fs::write(agent_dir.join("models.yml"), models).expect("models.yml");
}

fn spawn_arag(agent_dir: &Path, sessions_dir: &Path, id: &str) -> Child {
	let mut command = Command::new(env!("CARGO_BIN_EXE_arag"));
	command
		.env("PI_CODING_AGENT_DIR", agent_dir)
		.args([
			"--session-dir",
			sessions_dir.to_str().expect("sessions dir"),
			"--resume",
			id,
			"--no-tools",
			"--no-title",
			"-p",
			"continue",
		])
		.stdin(Stdio::null())
		.stdout(Stdio::null())
		.stderr(Stdio::null());
	command.spawn().expect("spawn arag")
}

fn run_arag(agent_dir: &Path, sessions_dir: &Path, id: &str) -> std::process::Output {
	Command::new(env!("CARGO_BIN_EXE_arag"))
		.env("PI_CODING_AGENT_DIR", agent_dir)
		.args([
			"--session-dir",
			sessions_dir.to_str().expect("sessions dir"),
			"--resume",
			id,
			"--no-tools",
			"--no-title",
			"-p",
			"continue",
		])
		.stdin(Stdio::null())
		.output()
		.expect("run arag")
}

fn branch_marker_count(db: &Path, branch_id: &str, marker: &str) -> usize {
	let store = SqliteStore::open_read_only(db).expect("open read-only store");
	let messages = store.branch_messages(branch_id).expect("branch messages");
	messages
		.iter()
		.filter(|message| {
			serde_json::to_string(&message.payload)
				.map(|payload| payload.contains(marker))
				.unwrap_or(false)
		})
		.count()
}

#[test]
fn sigkill_during_turn_replays_peer_message_exactly_once() {
	let dir = temp_dir("peer");
	let agent_dir = dir.join("agent");
	let sessions_dir = dir.join("sessions");
	std::fs::create_dir_all(&agent_dir).expect("agent dir");
	std::fs::create_dir_all(&sessions_dir).expect("sessions dir");
	let db = sessions_dir.join("sessions.db");
	let id = "kill-peer-root";
	let marker = "marker-kill-once";
	let text = format!("[msg-1] peer-a: {marker}");

	let (delivery_id, branch_id);
	{
		let root = Arc::new(
			DurableSession::create(&db, header(id, &dir)).expect("create root session"),
		);
		let factory = root.child_durability_factory();
		delivery_id = factory
			.enqueue_peer_delivery(None, "peer-a", "msg-1", &text)
			.expect("accept peer delivery");
		let store = SqliteStore::open_read_only(&db).expect("read store");
		branch_id = store
			.session(id)
			.expect("session")
			.expect("present")
			.active_branch_id
			.expect("active branch");
	}
	{
		let store = SqliteStore::open_read_only(&db).expect("read store");
		assert_eq!(
			store.pending_peer_deliveries(id).expect("pending").len(),
			1,
			"the fixture starts with an accepted, unpersisted delivery"
		);
	}

	// First run: the boundary consumes and persists the delivery, then blocks in
	// the provider. Killing here proves a resume cannot repeat it.
	let blocking = MockProvider::start(false);
	write_config(&agent_dir, blocking.port);
	let mut child = spawn_arag(&agent_dir, &sessions_dir, id);
	blocking.wait_requests(1);
	assert!(
		blocking.bodies()[0].contains(marker),
		"the model request must carry the accepted peer message"
	);
	child.kill().expect("SIGKILL arag");
	let _ = child.wait();
	assert_eq!(
		branch_marker_count(&db, &branch_id, marker),
		1,
		"the killed turn persisted the message once"
	);
	{
		let store = SqliteStore::open_read_only(&db).expect("read store");
		assert!(
			store
				.pending_peer_deliveries(id)
				.expect("pending")
				.is_empty(),
			"the delivery row was consumed atomically with the append"
		);
		let row = store
			.delivery(&delivery_id)
			.expect("delivery")
			.expect("present");
		assert!(row.delivered_at_ms.is_some());
	}

	// Second run: replay from durable history only; no duplicate append and the
	// marker reaches the provider exactly once in this run.
	let responding = MockProvider::start(true);
	write_config(&agent_dir, responding.port);
	let output = run_arag(&agent_dir, &sessions_dir, id);
	assert!(
		output.status.success(),
		"resume must succeed: {}",
		String::from_utf8_lossy(&output.stderr)
	);
	let bodies = responding.bodies();
	assert!(!bodies.is_empty(), "resume issued a provider request");
	assert_eq!(
		bodies[0].matches(marker).count(),
		1,
		"the replayed history contains the marker exactly once"
	);
	assert_eq!(
		branch_marker_count(&db, &branch_id, marker),
		1,
		"a resume must not append a second copy"
	);

	let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn sigkill_after_admission_abandons_once_and_does_not_reexecute() {
	let dir = temp_dir("admission");
	let agent_dir = dir.join("agent");
	let sessions_dir = dir.join("sessions");
	std::fs::create_dir_all(&agent_dir).expect("agent dir");
	std::fs::create_dir_all(&sessions_dir).expect("sessions dir");
	let db = sessions_dir.join("sessions.db");
	let id = "kill-admission-root";
	let worker = "ns:task-kill";
	let task_id;

	{
		let root = Arc::new(
			DurableSession::create(&db, header(id, &dir)).expect("create root session"),
		);
		let store = SqliteStore::open_read_only(&db).expect("read store");
		let generation = store
			.session_producer_generation(id)
			.expect("producer generation")
			.expect("recorded generation");
		task_id = format!("{worker}@{generation}");
		let factory = root.durable_child_factory();
		factory
			.admit_child(worker, &serde_json::json!({"task": "write the report"}))
			.expect("admit child");
		let child = factory.open_child(worker).expect("open child scope");
		factory.start_child(worker).expect("start child");
		drop(child);
	}
	{
		let store = SqliteStore::open_read_only(&db).expect("read store");
		let row = store
			.task_admission(&task_id)
			.expect("admission")
			.expect("present");
		assert_eq!(row.state, "started", "fixture is an in-flight admission");
	}

	// First run: startup recovers the orphaned admission, then blocks in the
	// provider. SIGKILL leaves a clean abandoned row.
	let blocking = MockProvider::start(false);
	write_config(&agent_dir, blocking.port);
	let mut child = spawn_arag(&agent_dir, &sessions_dir, id);
	let deadline = Instant::now() + Duration::from_secs(20);
	loop {
		let abandoned = {
			let store = SqliteStore::open_read_only(&db).expect("read store");
			let row = store
				.task_admission(&task_id)
				.expect("admission")
				.expect("present");
			row.state == "abandoned"
				&& store
					.open_task_admissions(id)
					.expect("open admissions")
					.is_empty()
		};
		if abandoned {
			break;
		}
		assert!(Instant::now() < deadline, "recovery did not abandon the admission");
		std::thread::sleep(Duration::from_millis(20));
	}
	child.kill().expect("SIGKILL arag");
	let _ = child.wait();

	{
		let store = SqliteStore::open_read_only(&db).expect("read store");
		let row = store
			.task_admission(&task_id)
			.expect("admission")
			.expect("present");
		assert_eq!(row.state, "abandoned");
		assert!(
			store
				.open_task_admissions(id)
				.expect("open admissions")
				.is_empty()
		);
	}

	// Second run must not resurrect or re-run the abandoned task.
	let responding = MockProvider::start(true);
	write_config(&agent_dir, responding.port);
	let output = run_arag(&agent_dir, &sessions_dir, id);
	assert!(
		output.status.success(),
		"clean resume must succeed: {}",
		String::from_utf8_lossy(&output.stderr)
	);
	{
		let store = SqliteStore::open_read_only(&db).expect("read store");
		let row = store
			.task_admission(&task_id)
			.expect("admission")
			.expect("present");
		assert_eq!(row.state, "abandoned", "an abandoned task stays abandoned");
		assert!(
			store
				.open_task_admissions(id)
				.expect("open admissions")
				.is_empty(),
			"no admission is reopened by a resume"
		);
	}
	let _ = std::fs::remove_dir_all(dir);
}
