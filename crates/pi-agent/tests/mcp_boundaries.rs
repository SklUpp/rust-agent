//! Supported MCP consumer boundaries against real localhost servers:
//! cursor pagination, header precedence and session/protocol propagation,
//! idempotent reconnect after a retriable failure, origin-locked redirect
//! refusal, and closing while a stdio request is still pending.
//!
//! OAuth credential storage is rejected by config validation and is not
//! exercised here; that remains an explicit unsupported feature, not a gap
//! this file can paper over.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pi_agent::mcp::{
	MCPHttpServerConfig, MCPServerConfig, MCPStdioServerConfig, McpError, connect_to_server,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
		Self { status: 200, headers: vec![("Content-Type", "application/json".to_string())], body }
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

impl Drop for MockServer {
	fn drop(&mut self) {
		self.handle.abort();
	}
}

fn reason(status: u16) -> &'static str {
	match status {
		200 => "OK",
		302 => "Found",
		503 => "Service Unavailable",
		_ => "Unknown",
	}
}

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
			line
				.to_ascii_lowercase()
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

async fn spawn_mock(handler: impl Fn(usize, &Seen) -> Reply + Send + Sync + 'static) -> MockServer {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	let seen = Arc::new(Mutex::new(Vec::new()));
	let handler = Arc::new(handler);
	let seen_for_server = Arc::clone(&seen);
	let handle = tokio::spawn(async move {
		loop {
			let Ok((mut socket, _)) = listener.accept().await else {
				return;
			};
			let handler = Arc::clone(&handler);
			let seen = Arc::clone(&seen_for_server);
			tokio::spawn(async move {
				let Some(request) = read_seen(&mut socket).await else {
					return;
				};
				let index = {
					let mut requests = seen.lock().unwrap();
					let index = requests.len();
					requests.push(request.clone());
					index
				};
				let reply = (*handler)(index, &request);
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
			});
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

#[tokio::test]
async fn tools_list_follows_cursor_pages_in_order() {
	let server = spawn_mock(|_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result()))
			.with_header("mcp-session-id", "sess-1"),
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		Some("tools/list") => {
			let cursor = seen
				.body
				.get("params")
				.and_then(|params| params.get("cursor"));
			if cursor.is_none() {
				Reply::json(rpc_result(
					&seen.body,
					json!({
						"tools": [{ "name": "alpha", "inputSchema": {} }],
						"nextCursor": "page-2",
					}),
				))
			} else {
				Reply::json(rpc_result(
					&seen.body,
					json!({ "tools": [{ "name": "beta", "inputSchema": {} }] }),
				))
			}
		},
		_ => Reply::json(rpc_result(&seen.body, json!({}))),
	})
	.await;

	let connection = connect_to_server("pages", MCPServerConfig::Http(http_config(&server.url)))
		.await
		.expect("connect");
	let tools = connection.list_tools().await.expect("paginated tools");
	let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_str()).collect();
	assert_eq!(names, ["alpha", "beta"]);

	let requests = server.seen.lock().unwrap().clone();
	let list_calls: Vec<&Seen> = requests
		.iter()
		.filter(|seen| seen.body.get("method").and_then(Value::as_str) == Some("tools/list"))
		.collect();
	assert_eq!(list_calls.len(), 2, "one request per cursor page");
	assert!(list_calls[0].body["params"].get("cursor").is_none());
	assert!(list_calls[0].body["params"]["_meta"]["progressToken"].is_number());
	assert_eq!(list_calls[1].body["params"]["cursor"], "page-2");

	connection.close().await.expect("close");
	connection
		.close()
		.await
		.expect("close is idempotent for HTTP");
}

#[tokio::test]
async fn tools_list_rejects_repeated_cursor() {
	let server = spawn_mock(|_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result())),
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		Some("tools/list") => {
			Reply::json(rpc_result(&seen.body, json!({ "tools": [], "nextCursor": "same" })))
		},
		_ => Reply::json(rpc_result(&seen.body, json!({}))),
	})
	.await;
	let connection =
		connect_to_server("repeated-cursor", MCPServerConfig::Http(http_config(&server.url)))
			.await
			.expect("connect");
	let error = connection
		.list_tools()
		.await
		.expect_err("repeated cursor must fail");
	assert!(matches!(error, McpError::Protocol(message) if message.contains("repeated cursor")));
	let calls = server
		.seen
		.lock()
		.unwrap()
		.iter()
		.filter(|seen| seen.body.get("method").and_then(Value::as_str) == Some("tools/list"))
		.count();
	assert_eq!(calls, 2, "stop when cursor repeats");
}

#[tokio::test]
async fn tools_list_rejects_page_limit_without_truncating() {
	let server = spawn_mock(|_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result())),
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		Some("tools/list") => {
			let cursor = seen
				.body
				.get("params")
				.and_then(|params| params.get("cursor"));
			let page = cursor
				.and_then(Value::as_str)
				.and_then(|value| value.strip_prefix("page-"))
				.and_then(|value| value.parse::<usize>().ok())
				.unwrap_or(1);
			Reply::json(rpc_result(
				&seen.body,
				json!({
					"tools": [], "nextCursor": format!("page-{}", page + 1),
				}),
			))
		},
		_ => Reply::json(rpc_result(&seen.body, json!({}))),
	})
	.await;
	let connection =
		connect_to_server("page-limit", MCPServerConfig::Http(http_config(&server.url)))
			.await
			.expect("connect");
	let error = connection
		.list_tools()
		.await
		.expect_err("page limit must fail");
	assert!(matches!(error, McpError::Protocol(message) if message.contains("pagination bounds")));
	let calls = server
		.seen
		.lock()
		.unwrap()
		.iter()
		.filter(|seen| seen.body.get("method").and_then(Value::as_str) == Some("tools/list"))
		.count();
	assert_eq!(calls, 64, "must stop at page limit without returning partial list");
}

#[tokio::test]
async fn tools_list_rejects_entry_limit_without_truncating() {
	let server = spawn_mock(|_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result())),
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		Some("tools/list") => Reply::json(rpc_result(
			&seen.body,
			json!({ "tools": vec![json!({ "name": "tool", "inputSchema": {} }); 10_001] }),
		)),
		_ => Reply::json(rpc_result(&seen.body, json!({}))),
	})
	.await;
	let connection =
		connect_to_server("entry-limit", MCPServerConfig::Http(http_config(&server.url)))
			.await
			.expect("connect");
	let error = connection
		.list_tools()
		.await
		.expect_err("entry limit must fail");
	assert!(matches!(error, McpError::Protocol(message) if message.contains("pagination bounds")));
}

#[tokio::test]
async fn configured_headers_lose_to_generated_and_session_headers_follow() {
	let server = spawn_mock(|_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result()))
			.with_header("mcp-session-id", "sess-1"),
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		_ => Reply::json(rpc_result(&seen.body, json!({}))),
	})
	.await;

	let mut config = http_config(&server.url);
	config.headers = HashMap::from([
		("authorization".to_string(), "Bearer tok".to_string()),
		("x-api-key".to_string(), "k-1".to_string()),
		("Content-Type".to_string(), "text/plain".to_string()),
		("mcp-protocol-version".to_string(), "spoofed".to_string()),
	]);
	connect_to_server("headers", MCPServerConfig::Http(config))
		.await
		.expect("connect");

	let requests = server.seen.lock().unwrap().clone();
	let init_headers = requests[0].headers.to_ascii_lowercase();
	assert!(init_headers.contains("authorization: bearer tok"), "{init_headers}");
	assert!(init_headers.contains("x-api-key: k-1"), "{init_headers}");
	assert!(
		init_headers.contains("content-type: application/json"),
		"SDK must generate JSON content type: {init_headers}"
	);
	assert!(
		!init_headers.contains("text/plain"),
		"configured content type must be rejected: {init_headers}"
	);
	assert!(
		!init_headers.contains("mcp-protocol-version: spoofed"),
		"configured protocol version must not override SDK negotiation: {init_headers}"
	);

	let notify_headers = requests[1].headers.to_ascii_lowercase();
	assert!(
		notify_headers.contains("mcp-session-id: sess-1"),
		"server session id must be replayed: {notify_headers}"
	);
	assert!(
		notify_headers.contains("mcp-protocol-version: 2025-11-25"),
		"negotiated protocol version must be sent after initialize: {notify_headers}"
	);
}

#[tokio::test]
async fn retriable_tools_list_failure_reconnects_and_retries_once() {
	let list_attempts = Arc::new(AtomicUsize::new(0));
	let attempts = Arc::clone(&list_attempts);
	let server = spawn_mock(move |_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result())),
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		Some("tools/list") => {
			let attempt = attempts.fetch_add(1, Ordering::SeqCst);
			if attempt == 0 {
				Reply {
					status: 503,
					headers: vec![("Content-Type", "text/plain".to_string())],
					body: "temporarily unavailable".to_string(),
				}
			} else {
				Reply::json(rpc_result(
					&seen.body,
					json!({ "tools": [{ "name": "after-retry", "inputSchema": {} }] }),
				))
			}
		},
		_ => Reply::json(rpc_result(&seen.body, json!({}))),
	})
	.await;

	let connection = connect_to_server("retry", MCPServerConfig::Http(http_config(&server.url)))
		.await
		.expect("connect");
	let tools = connection.list_tools().await.expect("retried tools");
	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name, "after-retry");
	assert_eq!(list_attempts.load(Ordering::SeqCst), 2);

	let requests = server.seen.lock().unwrap().clone();
	let initialize_calls = requests
		.iter()
		.filter(|seen| seen.body.get("method").and_then(Value::as_str) == Some("initialize"))
		.count();
	assert_eq!(initialize_calls, 2, "reconnect must re-run the handshake");
}

#[tokio::test]
async fn http_redirect_is_refused_before_second_origin_contact() {
	let target = spawn_mock(|_, seen| Reply::json(rpc_result(&seen.body, json!({})))).await;
	let target_hits = Arc::clone(&target.seen);
	let redirector = spawn_mock(move |_, _| Reply {
		status: 302,
		headers: vec![("Location", target.url.clone())],
		body: String::new(),
	})
	.await;
	let config = http_config(&redirector.url);
	let err = connect_to_server("locked", MCPServerConfig::Http(config))
		.await
		.expect_err("Policy::none must refuse all redirects");
	assert!(
		matches!(&err, McpError::Protocol(message) if message.contains("HTTP 302")),
		"expected rmcp Protocol redirect refusal, got {err:?}"
	);
	assert!(
		target_hits.lock().unwrap().is_empty(),
		"refused redirect must never reach target origin"
	);
}

#[tokio::test]
async fn close_drains_a_pending_stdio_request_and_kills_the_child() {
	let dir = std::env::temp_dir();
	let unique = format!("omp-mcp-close-{}", std::process::id());
	let script = dir.join(format!("{unique}.py"));
	let pid_file = dir.join(format!("{unique}.pid"));
	let marker = dir.join(format!("{unique}.called"));
	std::fs::write(
		&script,
		r#"
import json, os, sys, time

open(sys.argv[1], "w").write(str(os.getpid()))
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    mid = msg.get("id")
    if method == "tools/call":
        open(sys.argv[2], "w").write("called")
        time.sleep(300)
        continue
    if mid is not None:
        result = {
            "protocolVersion": "2025-11-25",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mock-close", "version": "1.0.0"},
        }
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": mid, "result": result}) + "\n")
        sys.stdout.flush()
"#,
	)
	.expect("write script");

	let config = MCPServerConfig::Stdio(MCPStdioServerConfig {
		command: "python3".to_string(),
		args: vec![
			script.to_string_lossy().into_owned(),
			pid_file.to_string_lossy().into_owned(),
			marker.to_string_lossy().into_owned(),
		],
		env: HashMap::new(),
		cwd: None,
		timeout: Some(5_000),
		request_id_format: None,
	});
	let connection = Arc::new(connect_to_server("close", config).await.expect("connect"));
	let pid: i32 = std::fs::read_to_string(&pid_file)
		.expect("pid file")
		.trim()
		.parse()
		.expect("pid");

	let caller = Arc::clone(&connection);
	let pending = tokio::spawn(async move { caller.call_tool("wait_forever", json!({})).await });
	let start = tokio::time::Instant::now();
	while !marker.exists() {
		assert!(start.elapsed() < Duration::from_secs(3), "server never observed tools/call");
		tokio::time::sleep(Duration::from_millis(10)).await;
	}

	connection.close().await.expect("close");
	let result = tokio::time::timeout(Duration::from_secs(2), pending)
		.await
		.expect("pending request must settle on close")
		.expect("task");
	match result {
		Err(McpError::UnknownOutcome(message)) => {
			assert!(message.contains("unverified") || message.contains("unknown"), "{message}")
		},
		other => panic!("pending mutating call must report unknown outcome, got {other:?}"),
	}

	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	let mut alive = true;
	while alive && tokio::time::Instant::now() < deadline {
		alive = std::process::Command::new("kill")
			.args(["-0", &pid.to_string()])
			.status()
			.is_ok_and(|status| status.success());
		if alive {
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}
	assert!(!alive, "close must terminate the stdio child");

	let _ = std::fs::remove_file(&script);
	let _ = std::fs::remove_file(&pid_file);
	let _ = std::fs::remove_file(&marker);
}

/// Transport that fails every request with one prepared error class. Used to
/// drive the client's error classification and transport-blame bookkeeping
/// without a live server.
struct FailingTransport {
	failure: Failure,
}

#[derive(Clone, Copy, Debug)]
enum Failure {
	/// Ambiguous: the request may have been accepted.
	Transport,
	/// Definite: never dispatched.
	Closed,
	/// Definite: the server answered with an error.
	JsonRpc,
}

#[async_trait::async_trait]
impl pi_agent::mcp::McpTransport for FailingTransport {
	async fn request(
		&self,
		_method: &str,
		_params: Option<Value>,
		_timeout: Option<Duration>,
	) -> Result<Value, McpError> {
		Err(match self.failure {
			Failure::Transport => McpError::Transport("mock transport broke".to_string()),
			Failure::Closed => McpError::Closed,
			Failure::JsonRpc => {
				McpError::JsonRpc { code: -32000, message: "rejected".to_string(), data: None }
			},
		})
	}

	async fn notify(&self, _method: &str, _params: Option<Value>) -> Result<(), McpError> {
		Ok(())
	}

	async fn close(&self) -> Result<(), McpError> {
		Ok(())
	}
}

/// A connection whose installed transport is the given fake. `config` must
/// point at a live mock server, because a reconnect builds a real transport
/// from it.
fn fake_connection(failure: Failure, config: MCPServerConfig) -> Arc<pi_agent::mcp::McpConnection> {
	Arc::new(pi_agent::mcp::McpConnection {
		name: "fake".to_string(),
		config,
		transport: Arc::new(tokio::sync::RwLock::new(
			Arc::new(FailingTransport { failure }) as Arc<dyn pi_agent::mcp::McpTransport>
		)),
		server_info: pi_agent::mcp::MCPServerInfo { name: "fake".to_string(), version: None },
		capabilities: pi_agent::mcp::MCPServerCapabilities::default(),
		instructions: None,
		closed: std::sync::atomic::AtomicBool::new(false),
		transport_epoch: std::sync::atomic::AtomicU64::new(1),
		failed_epoch: std::sync::atomic::AtomicU64::new(0),
		close_completed: std::sync::atomic::AtomicBool::new(false),
		close_barrier: Arc::new(tokio::sync::Mutex::new(())),
		state_barrier: Arc::new(tokio::sync::Mutex::new(())),
	})
}

fn method_count(server: &MockServer, method: &str) -> usize {
	server
		.seen
		.lock()
		.unwrap()
		.iter()
		.filter(|seen| seen.body.get("method").and_then(Value::as_str) == Some(method))
		.count()
}

fn tools_call_result(body: &Value) -> String {
	rpc_result(body, json!({ "content": [{ "type": "text", "text": "ok" }], "isError": false }))
}

#[tokio::test]
async fn definite_errors_never_condemn_the_transport() {
	let server = spawn_mock(|_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result())),
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		_ => Reply::json(tools_call_result(&seen.body)),
	})
	.await;

	// A JSON-RPC error response means the server answered: the transport is
	// healthy, so neither this call nor the next may trigger a handshake.
	let connection =
		fake_connection(Failure::JsonRpc, MCPServerConfig::Http(http_config(&server.url)));
	let error = connection
		.call_tool("tool", json!({}))
		.await
		.expect_err("fake always fails");
	assert!(matches!(error, McpError::JsonRpc { .. }), "{error:?}");
	connection
		.call_tool("tool", json!({}))
		.await
		.expect_err("fake always fails");
	assert_eq!(
		method_count(&server, "initialize"),
		0,
		"a server answer must not mark the transport broken"
	);
}

#[tokio::test]
async fn closed_transport_is_replaced_on_the_next_call() {
	let server = spawn_mock(|_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result())),
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		_ => Reply::json(tools_call_result(&seen.body)),
	})
	.await;

	let connection =
		fake_connection(Failure::Closed, MCPServerConfig::Http(http_config(&server.url)));
	// Definite: the request never left the client, so it is reported as `Closed`
	// rather than as an unknown outcome.
	let error = connection
		.call_tool("tool", json!({}))
		.await
		.expect_err("fake fails");
	assert!(matches!(error, McpError::Closed), "{error:?}");

	// A transport that reports `Closed` is gone, so the next call replaces it.
	connection
		.call_tool("tool", json!({}))
		.await
		.expect("reconnected call succeeds");
	assert_eq!(method_count(&server, "initialize"), 1, "closed transport is replaced once");
}

#[tokio::test]
async fn unverified_delivery_retires_only_its_own_epoch() {
	let server = spawn_mock(|_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result())),
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		_ => Reply::json(tools_call_result(&seen.body)),
	})
	.await;

	let connection =
		fake_connection(Failure::Transport, MCPServerConfig::Http(http_config(&server.url)));

	// An unverified delivery blames the transport that produced it.
	let error = connection
		.call_tool("tool", json!({}))
		.await
		.expect_err("fake fails");
	assert!(matches!(error, McpError::Transport(_)), "{error:?}");

	// The next call reconnects (one handshake) and the replacement serves it.
	connection
		.call_tool("tool", json!({}))
		.await
		.expect("reconnected call succeeds");
	assert_eq!(method_count(&server, "initialize"), 1, "exactly one reconnect");
	assert_eq!(
		connection.transport_epoch.load(Ordering::SeqCst),
		2,
		"reconnect installs a new epoch"
	);

	// A late failure from the retired transport must not mark the healthy one:
	// record it against epoch 1, then prove no further handshake happens.
	connection.failed_epoch.fetch_max(1, Ordering::AcqRel);
	connection
		.call_tool("tool", json!({}))
		.await
		.expect("healthy transport keeps serving");
	assert_eq!(method_count(&server, "initialize"), 1, "stale epoch must be ignored");

	// A failure on the installed epoch is genuine and must survive: it is the
	// epoch reconnect has actually observed, so the next call reconnects again.
	connection.failed_epoch.fetch_max(2, Ordering::AcqRel);
	connection
		.call_tool("tool", json!({}))
		.await
		.expect("second reconnect succeeds");
	assert_eq!(method_count(&server, "initialize"), 2, "current-epoch failure is honoured");
}

#[tokio::test]
async fn transient_gateway_status_on_a_mutating_call_is_unverified() {
	// 502/503 can arrive after the tool already ran. Reporting a definite HTTP
	// failure would invite the model to replay a non-idempotent call, so the
	// error has to keep the unverified wording and must not be retried.
	let calls = Arc::new(AtomicUsize::new(0));
	let seen_calls = Arc::clone(&calls);
	let server = spawn_mock(move |_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result())),
		Some("tools/call") => {
			seen_calls.fetch_add(1, Ordering::SeqCst);
			Reply { status: 503, headers: Vec::new(), body: "unavailable".to_string() }
		},
		_ => Reply::json(rpc_result(&seen.body, json!({}))),
	})
	.await;
	let connection =
		connect_to_server("mutating-503", MCPServerConfig::Http(http_config(&server.url)))
			.await
			.expect("connect");

	let error = connection
		.call_tool("tool", json!({}))
		.await
		.expect_err("503 fails the call");
	match error {
		McpError::UnknownOutcome(message) => {
			assert!(message.contains("unverified"), "wording keeps the replay warning: {message}");
		},
		other => panic!("mutating 503 must be UnknownOutcome, got {other:?}"),
	}
	assert_eq!(calls.load(Ordering::SeqCst), 1, "an unverified call is never replayed");
	connection.close().await.expect("close");
}

#[tokio::test]
async fn concurrent_calls_share_one_reconnect() {
	let call_attempts = Arc::new(AtomicUsize::new(0));
	let attempts = Arc::clone(&call_attempts);
	let server = spawn_mock(move |_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result())),
		Some("tools/call") => {
			if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
				Reply { status: 503, headers: Vec::new(), body: "unavailable".to_string() }
			} else {
				Reply::json(tools_call_result(&seen.body))
			}
		},
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		_ => Reply::json(rpc_result(&seen.body, json!({}))),
	})
	.await;

	let connection = connect_to_server("shared", MCPServerConfig::Http(http_config(&server.url)))
		.await
		.expect("connect");
	assert!(connection.call_tool("tool", json!({})).await.is_err(), "first call breaks");

	// Both callers observe the broken epoch; exactly one of them may rebuild it.
	let (left, right) = tokio::join!(
		connection.call_tool("tool", json!({})),
		connection.call_tool("tool", json!({}))
	);
	left.expect("left call succeeds after reconnect");
	right.expect("right call succeeds on the reconnected transport");

	assert_eq!(call_attempts.load(Ordering::SeqCst), 3, "the failed call is never replayed");
	assert_eq!(
		method_count(&server, "initialize"),
		2,
		"one reconnect shared by both concurrent callers"
	);
	connection.close().await.expect("close");
}

#[tokio::test]
async fn close_wins_over_concurrent_reconnect() {
	let server = spawn_mock(|_, seen| match seen.body.get("method").and_then(Value::as_str) {
		Some("initialize") => Reply::json(rpc_result(&seen.body, initialize_result())),
		Some("notifications/initialized") => Reply::json("{}".to_string()),
		_ => Reply::json(tools_call_result(&seen.body)),
	})
	.await;

	let connection = connect_to_server("race", MCPServerConfig::Http(http_config(&server.url)))
		.await
		.expect("connect");
	let (reconnected, closed) = tokio::join!(connection.reconnect(), connection.close());
	// Whichever order the two win, neither may panic and teardown must finish.
	assert!(
		reconnected.is_ok() || matches!(reconnected, Err(McpError::Closed)),
		"reconnect after close is Closed, never a half-installed transport: {reconnected:?}"
	);
	closed.expect("close completes");

	// Post-close requests are definite: nothing reached a transport.
	let error = connection
		.call_tool("tool", json!({}))
		.await
		.expect_err("closed session");
	assert!(matches!(error, McpError::Closed), "{error:?}");
	connection.close().await.expect("close is idempotent");
}
