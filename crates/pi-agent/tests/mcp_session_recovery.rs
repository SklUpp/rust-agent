//! Streamable-HTTP MCP session recovery against real localhost servers.
//!
//! Two wire transitions are asserted end to end:
//!
//! 1. A later response may rotate `mcp-session-id`; the consumer must send the
//!    rotated value on every following request without re-running `initialize`.
//! 2. A stale-session `404` on an idempotent request must reconnect, re-run
//!    `initialize`/`notifications/initialized`, and continue on the replacement
//!    session — never on the stale one.
//!
//! Retry policy is taken from the client, not invented here: only safe
//! operations (pagination/listing) reconnect-and-retry once, while a mutating
//! `tools/call` is reported unchanged and never replayed. Existing `503`
//! reconnect coverage and accepted-stream no-replay coverage live in
//! `mcp_boundaries.rs` / `mcp.rs` and are deliberately not duplicated.
//!
//! The fixture serves connections one at a time from a single accept loop, so
//! request count is bounded and no detached per-connection task can outlive
//! the test; `Drop` reaps the loop even when an assertion panics.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pi_agent::mcp::{MCPServerConfig, MCPHttpServerConfig, McpError, connect_to_server};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Upper bound on requests a misbehaving client may make before the fixture
/// stops accepting. Every test asserts an exact, much smaller count.
const MAX_REQUESTS: usize = 32;

/// One request as the server observed it: raw header block plus parsed body.
#[derive(Clone)]
struct Seen {
	headers: String,
	body: Value,
}

struct Reply {
	status: u16,
	headers: Vec<(&'static str, String)>,
	body: String,
}

impl Reply {
	fn json(body: String) -> Self {
		Self {
			status: 200,
			headers: vec![("Content-Type", "application/json".to_string())],
			body,
		}
	}

	fn with_header(mut self, name: &'static str, value: &str) -> Self {
		self.headers.push((name, value.to_string()));
		self
	}
}

struct MockServer {
	url: String,
	seen: Arc<Mutex<Vec<Seen>>>,
	handle: tokio::task::JoinHandle<()>,
}

impl MockServer {
	fn requests(&self) -> Vec<Seen> {
		self.seen.lock().unwrap().clone()
	}
}

// Reaps the accept loop even when a test panics before `close`.
impl Drop for MockServer {
	fn drop(&mut self) {
		self.handle.abort();
	}
}

fn reason(status: u16) -> &'static str {
	match status {
		200 => "OK",
		404 => "Not Found",
		_ => "Unknown",
	}
}

/// Read exactly one HTTP/1.1 request. Every read is deadline-bounded so a
/// malformed client cannot hang the fixture.
async fn read_seen(socket: &mut tokio::net::TcpStream) -> Option<Seen> {
	let mut buf = Vec::new();
	let mut header_end = None;
	let mut chunk = [0u8; 1024];
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	while header_end.is_none() && tokio::time::Instant::now() < deadline {
		let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut chunk))
			.await
			.ok()?
			.ok()?;
		if n == 0 {
			return None;
		}
		buf.extend_from_slice(&chunk[..n]);
		header_end = buf.windows(4).position(|w| w == b"\r\n\r\n");
	}
	let header_end = header_end?;
	let headers = String::from_utf8_lossy(&buf[..header_end]).into_owned();
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
	while body.len() < content_length {
		let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut chunk))
			.await
			.ok()?
			.ok()?;
		if n == 0 {
			break;
		}
		body.extend_from_slice(&chunk[..n]);
	}
	Some(Seen { headers, body: serde_json::from_slice(&body).unwrap_or(Value::Null) })
}

async fn spawn_mock(
	handler: impl Fn(usize, &Seen) -> Reply + Send + Sync + 'static,
) -> MockServer {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	let seen = Arc::new(Mutex::new(Vec::new()));
	let seen_for_server = Arc::clone(&seen);
	let handle = tokio::spawn(async move {
		for _ in 0..MAX_REQUESTS {
			let Ok((mut socket, _)) = listener.accept().await else {
				return;
			};
			let Some(request) = read_seen(&mut socket).await else {
				return;
			};
			let index = {
				let mut requests = seen_for_server.lock().unwrap();
				let index = requests.len();
				requests.push(request.clone());
				index
			};
			let reply = handler(index, &request);
			let mut response = format!(
				"HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
				reply.status,
				reason(reply.status),
				reply.body.len()
			);
			for (name, value) in reply.headers {
				response.push_str(&format!("{name}: {value}\r\n"));
			}
			response.push_str("\r\n");
			response.push_str(&reply.body);
			let _ = socket.write_all(response.as_bytes()).await;
			let _ = socket.flush().await;
		}
	});
	MockServer { url: format!("http://{addr}/mcp"), seen, handle }
}

fn rpc_result(body: &Value, result: Value) -> String {
	json!({
		"jsonrpc": "2.0",
		"id": body.get("id").cloned().unwrap_or(Value::Null),
		"result": result,
	})
	.to_string()
}

fn initialize_result() -> Value {
	json!({
		"protocolVersion": "2025-11-25",
		"capabilities": { "tools": {} },
		"serverInfo": { "name": "mock", "version": "1.0.0" },
	})
}

fn http_config(url: &str) -> MCPHttpServerConfig {
	MCPHttpServerConfig {
		url: url.to_string(),
		headers: HashMap::new(),
		timeout: Some(5_000),
		request_id_format: None,
		header_policy: None,
		auth: None,
		oauth: None,
	}
}

fn method(seen: &Seen) -> Option<&str> {
	seen.body.get("method").and_then(Value::as_str)
}

fn header(seen: &Seen, name: &str) -> Option<String> {
	seen.headers.lines().skip(1).find_map(|line| {
		let (key, value) = line.split_once(':')?;
		key.trim().eq_ignore_ascii_case(name).then(|| value.trim().to_string())
	})
}

fn count_method(requests: &[Seen], wanted: &str) -> usize {
	requests.iter().filter(|seen| method(seen) == Some(wanted)).count()
}

fn tool_names(tools: &[pi_agent::mcp::MCPToolDefinition]) -> Vec<&str> {
	tools.iter().map(|tool| tool.name.as_str()).collect()
}

fn list_calls(requests: &[Seen]) -> Vec<&Seen> {
	requests.iter().filter(|seen| method(seen) == Some("tools/list")).collect()
}

#[tokio::test]
async fn rotated_session_header_is_adopted_by_the_next_request() {
	let server = spawn_mock(|_, seen| match method(seen) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result()))
			.with_header("mcp-session-id", "sess-1"),
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		Some("tools/list") => match seen.body["params"].get("cursor").and_then(Value::as_str) {
			None => Reply::json(rpc_result(
				&seen.body,
				json!({
					"tools": [{ "name": "alpha", "inputSchema": {} }],
					"nextCursor": "page-2",
				}),
			))
			.with_header("mcp-session-id", "sess-2"),
			Some("page-2") => Reply::json(rpc_result(
				&seen.body,
				json!({
					"tools": [{ "name": "beta", "inputSchema": {} }],
					"nextCursor": "page-3",
				}),
			))
			.with_header("mcp-session-id", "sess-3"),
			Some("page-3") => Reply::json(rpc_result(
				&seen.body,
				json!({ "tools": [{ "name": "gamma", "inputSchema": {} }] }),
			)),
			Some(other) => panic!("unexpected cursor {other}"),
		},
		Some(other) => panic!("unexpected method {other}"),
		None => panic!("unexpected notification payload"),
	})
	.await;

	let connection = connect_to_server("rotation", MCPServerConfig::Http(http_config(&server.url)))
		.await
		.expect("connect");
	let tools = connection.list_tools().await.expect("tools across rotated sessions");
	assert_eq!(
		tool_names(&tools),
		["alpha", "beta", "gamma"],
		"consumer must see every page"
	);

	let requests = server.requests();
	let lists = list_calls(&requests);
	assert_eq!(lists.len(), 3, "one request per page, no hidden retry");
	assert_eq!(
		header(lists[0], "mcp-session-id").as_deref(),
		Some("sess-1"),
		"initialize session must be sent first: {}",
		lists[0].headers
	);
	assert_eq!(
		header(lists[1], "mcp-session-id").as_deref(),
		Some("sess-2"),
		"rotation on page 1 must be adopted by page 2: {}",
		lists[1].headers
	);
	assert_eq!(
		header(lists[2], "mcp-session-id").as_deref(),
		Some("sess-3"),
		"rotation on page 2 must be adopted by page 3: {}",
		lists[2].headers
	);
	assert_eq!(
		count_method(&requests, "initialize"),
		1,
		"session rotation is not a reconnect and must not re-run the handshake"
	);
	assert_eq!(
		count_method(&requests, "notifications/initialized"),
		1,
		"rotation must not repeat the initialized notification"
	);
	assert_eq!(requests.len(), 5, "bounded request count: init, notify, three pages");

	connection.close().await.expect("close");
}

#[tokio::test]
async fn stale_session_404_reconnects_and_uses_the_replacement_session() {
	let initialize_calls = Arc::new(AtomicUsize::new(0));
	let list_attempts = Arc::new(AtomicUsize::new(0));
	let init_for_server = Arc::clone(&initialize_calls);
	let list_for_server = Arc::clone(&list_attempts);

	let server = spawn_mock(move |_, seen| match method(seen) {
		Some("initialize") => {
			let attempt = init_for_server.fetch_add(1, Ordering::SeqCst);
			let session = if attempt == 0 { "sess-stale" } else { "sess-fresh" };
			Reply::json(rpc_result(&seen.body, initialize_result()))
				.with_header("mcp-session-id", session)
		}
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		Some("tools/list") => {
			let attempt = list_for_server.fetch_add(1, Ordering::SeqCst);
			if attempt == 0 {
				// The first session is no longer valid server-side.
				Reply {
					status: 404,
					headers: vec![("Content-Type", "text/plain".to_string())],
					body: "session expired".to_string(),
				}
			} else {
				Reply::json(rpc_result(
					&seen.body,
					json!({ "tools": [{ "name": "recovered", "inputSchema": {} }] }),
				))
			}
		}
		Some(other) => panic!("unexpected method {other}"),
		None => panic!("unexpected notification payload"),
	})
	.await;

	let connection = connect_to_server("stale", MCPServerConfig::Http(http_config(&server.url)))
		.await
		.expect("connect");
	let tools = connection.list_tools().await.expect("idempotent list after stale session");
	assert_eq!(
		tool_names(&tools),
		["recovered"],
		"retry must succeed on the replacement session"
	);

	// The replacement session must persist beyond the retry itself, otherwise
	// the next consumer call would fall back to the stale session.
	let again = connection.list_tools().await.expect("later call on the replacement session");
	assert_eq!(again.len(), 1, "consumer stays usable after recovery");

	let requests = server.requests();
	let lists = list_calls(&requests);
	assert_eq!(
		list_attempts.load(Ordering::SeqCst),
		3,
		"one bounded retry, then one normal call"
	);
	assert_eq!(lists.len(), 3, "no request storm after the 404");
	assert_eq!(
		header(lists[0], "mcp-session-id").as_deref(),
		Some("sess-stale"),
		"the failing attempt must carry the stale session: {}",
		lists[0].headers
	);
	assert_eq!(
		header(lists[1], "mcp-session-id").as_deref(),
		Some("sess-fresh"),
		"the retry must carry the replacement session: {}",
		lists[1].headers
	);
	assert_ne!(
		header(lists[1], "mcp-session-id").as_deref(),
		Some("sess-stale"),
		"the stale session must never be reused after reconnect"
	);
	assert_eq!(
		header(lists[2], "mcp-session-id").as_deref(),
		Some("sess-fresh"),
		"the replacement session must persist for later calls: {}",
		lists[2].headers
	);
	assert_eq!(
		initialize_calls.load(Ordering::SeqCst),
		2,
		"a stale session must be replaced by a real handshake"
	);
	let inits: Vec<&Seen> = requests
		.iter()
		.filter(|seen| method(seen) == Some("initialize"))
		.collect();
	assert_eq!(inits.len(), 2, "a replacement handshake must run `initialize`");
	assert!(
		header(inits[0], "mcp-session-id").is_none(),
		"the first handshake must start without a session: {}",
		inits[0].headers
	);
	assert!(
		header(inits[1], "mcp-session-id").is_none(),
		"the replacement handshake must start a new session, not present the stale one: {}",
		inits[1].headers
	);

	let notifications: Vec<&Seen> = requests
		.iter()
		.filter(|seen| method(seen) == Some("notifications/initialized"))
		.collect();
	assert_eq!(
		notifications.len(),
		2,
		"the reconnected transport must complete `notifications/initialized`"
	);
	assert_eq!(
		header(notifications[1], "mcp-session-id").as_deref(),
		Some("sess-fresh"),
		"the replacement handshake must announce itself on the fresh session: {}",
		notifications[1].headers
	);
	assert_eq!(
		requests.len(),
		7,
		"bounded request count: init, notify, stale list, init, notify, retry, later call"
	);

	connection.close().await.expect("close");
}

#[tokio::test]
async fn mutating_404_is_surfaced_without_replay_or_rehandshake() {
	let initialize_calls = Arc::new(AtomicUsize::new(0));
	let call_attempts = Arc::new(AtomicUsize::new(0));
	let init_for_server = Arc::clone(&initialize_calls);
	let call_for_server = Arc::clone(&call_attempts);

	let server = spawn_mock(move |_, seen| match method(seen) {
		Some("initialize") => {
			init_for_server.fetch_add(1, Ordering::SeqCst);
			Reply::json(rpc_result(&seen.body, initialize_result()))
				.with_header("mcp-session-id", "sess-1")
		}
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		Some("tools/call") => {
			call_for_server.fetch_add(1, Ordering::SeqCst);
			Reply {
				status: 404,
				headers: vec![("Content-Type", "text/plain".to_string())],
				body: "session expired".to_string(),
			}
		}
		Some(other) => panic!("unexpected method {other}"),
		None => panic!("unexpected notification payload"),
	})
	.await;

	let connection = connect_to_server("mutating", MCPServerConfig::Http(http_config(&server.url)))
		.await
		.expect("connect");
	let err = connection
		.call_tool("delete_everything", json!({}))
		.await
		.expect_err("stale-session 404 on a mutation must surface");
	match err {
		McpError::Http { status: 404, message } => assert_eq!(message, "session expired"),
		other => panic!("expected the server 404 unchanged, got {other:?}"),
	}
	assert_eq!(call_attempts.load(Ordering::SeqCst), 1, "mutating call must not be replayed");

	let requests = server.requests();
	assert_eq!(
		initialize_calls.load(Ordering::SeqCst),
		1,
		"safe-operation recovery must not silently rehandshake after a mutating failure"
	);
	assert_eq!(requests.len(), 3, "bounded request count: init, notify, one tools/call");
	let call = requests
		.iter()
		.find(|seen| method(seen) == Some("tools/call"))
		.expect("tools/call was observed");
	assert_eq!(
		header(call, "mcp-session-id").as_deref(),
		Some("sess-1"),
		"the single attempt must carry the original session: {}",
		call.headers
	);

	connection.close().await.expect("close");
}
