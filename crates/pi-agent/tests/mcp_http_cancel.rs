//! HTTP MCP cancellation wire contract.
//!
//! A `tools/list` whose SSE response stays open must surface
//! [`McpError::Timeout`] and then send a *separate* `notifications/cancelled`
//! POST whose `params.requestId` is exactly the original request ID, with its
//! JSON type preserved. A fixture that only echoes fields would not catch a
//! missing cancellation or a guard that captured the wrong ID.
//!
//! Everything runs against a real loopback TCP listener; no production code is
//! modified and no global environment variables are touched.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pi_agent::mcp::{
	MCPServerConfig, MCPHttpServerConfig, McpError, RequestIdFormat, connect_to_server,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Per-request client deadline; short enough to keep the test fast, long enough
/// that loopback handshakes never race it.
const REQUEST_TIMEOUT: Duration = Duration::from_millis(750);
/// Bound every fixture/assertion wait so a hang fails instead of blocking CI.
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);
/// `Connection: close` responses remain readable for this long before the
/// fixture task gives up.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// The wire exchanges a test can observe are fixed (initialize, initialized,
/// tools/list, cancelled); a small bound keeps a runaway client from growing
/// fixture memory without ever blocking the happy path.
const SEEN_CAPACITY: usize = 16;

struct Fixture {
	url: String,
	seen: mpsc::Receiver<Value>,
	_server: Server,
}

struct Server {
	accept: JoinHandle<()>,
	connections: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Drop for Server {
	fn drop(&mut self) {
		self.accept.abort();
		for handle in self.connections.lock().unwrap().drain(..) {
			handle.abort();
		}
	}
}

async fn spawn_fixture() -> Fixture {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
	let addr = listener.local_addr().expect("fixture address");
	let (tx, seen) = mpsc::channel(SEEN_CAPACITY);
	let connections = Arc::new(Mutex::new(Vec::new()));
	let connections_for_accept = Arc::clone(&connections);
	let accept = tokio::spawn(async move {
		loop {
			let Ok((mut socket, _)) = listener.accept().await else {
				return;
			};
			let tx = tx.clone();
			let handle = tokio::spawn(async move {
				serve(&mut socket, tx).await;
			});
			connections_for_accept.lock().unwrap().push(handle);
		}
	});
	Fixture {
		url: format!("http://{addr}/mcp"),
		seen,
		_server: Server { accept, connections },
	}
}

async fn serve(socket: &mut TcpStream, seen: mpsc::Sender<Value>) {
	let Some(body) = read_request(socket).await else {
		return;
	};
	let method = body
		.get("method")
		.and_then(Value::as_str)
		.unwrap_or_default()
		.to_string();
	let _ = seen.send(body.clone()).await;
	match method.as_str() {
		"initialize" => write_json(socket, &rpc_result(&body, initialize_result())).await,
		"notifications/initialized" | "notifications/cancelled" => {
			write_json(socket, &json!({})).await;
		},
		"tools/list" => hold_sse_open(socket).await,
		_ => write_json(socket, &rpc_result(&body, json!({}))).await,
	}
}

/// Send SSE headers, then keep the body pending and never emit a JSON-RPC
/// frame carrying the request ID.
async fn hold_sse_open(socket: &mut TcpStream) {
	let _ = socket
		.write_all(
			b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n",
		)
		.await;
	let _ = socket.flush().await;
	let mut buf = [0u8; 1024];
	let _ = tokio::time::timeout(Duration::from_secs(30), async {
		loop {
			match socket.read(&mut buf).await {
				Ok(0) | Err(_) => break,
				Ok(_) => {},
			}
		}
	})
	.await;
}

async fn write_json(socket: &mut TcpStream, body: &Value) {
	let payload = body.to_string();
	let head = format!(
		"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
		payload.len()
	);
	let _ = socket.write_all(head.as_bytes()).await;
	let _ = socket.write_all(b"Connection: close\r\n\r\n").await;
	let _ = socket.write_all(payload.as_bytes()).await;
	let _ = socket.flush().await;
}

fn rpc_result(request: &Value, result: Value) -> Value {
	json!({
		"jsonrpc": "2.0",
		"id": request.get("id").cloned().unwrap_or(Value::Null),
		"result": result,
	})
}

fn initialize_result() -> Value {
	json!({
		"protocolVersion": "2025-11-25",
		"capabilities": { "tools": {} },
		"serverInfo": { "name": "cancel-fixture", "version": "1.0.0" },
	})
}

/// Read one HTTP request (headers plus Content-Length body) off a connection.
async fn read_request(socket: &mut TcpStream) -> Option<Value> {
	let mut buf = Vec::new();
	let mut chunk = [0u8; 1024];
	let header_end = loop {
		if let Some(pos) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
			break pos;
		}
		let n = tokio::time::timeout(READ_TIMEOUT, socket.read(&mut chunk)).await.ok()?.ok()?;
		if n == 0 {
			return None;
		}
		buf.extend_from_slice(&chunk[..n]);
	};
	let content_length = String::from_utf8_lossy(&buf[..header_end])
		.lines()
		.find_map(|line| {
			line.to_ascii_lowercase()
				.strip_prefix("content-length:")
				.and_then(|value| value.trim().parse::<usize>().ok())
		})
		.unwrap_or(0);
	let mut body = buf[header_end + 4..].to_vec();
	while body.len() < content_length {
		let n = tokio::time::timeout(READ_TIMEOUT, socket.read(&mut chunk)).await.ok()?.ok()?;
		if n == 0 {
			return None;
		}
		body.extend_from_slice(&chunk[..n]);
	}
	serde_json::from_slice(&body).ok()
}

async fn expect_request(fixture: &mut Fixture, method: &str) -> Value {
	let body = tokio::time::timeout(WAIT_TIMEOUT, fixture.seen.recv())
		.await
		.unwrap_or_else(|_| panic!("timed out waiting for {method}"))
		.unwrap_or_else(|| panic!("fixture closed before {method}"));
	assert_eq!(
		body.get("method").and_then(Value::as_str),
		Some(method),
		"unexpected wire request: {body}"
	);
	body
}

async fn assert_cancel_names_original_request(format: RequestIdFormat) {
	let mut fixture = spawn_fixture().await;
	let config = MCPHttpServerConfig {
		url: fixture.url.clone(),
		headers: HashMap::new(),
		timeout: Some(REQUEST_TIMEOUT.as_millis() as u64),
		request_id_format: Some(format),
		header_policy: None,
		auth: None,
		oauth: None,
	};
	let connection = connect_to_server("cancel", MCPServerConfig::Http(config))
		.await
		.expect("initialize and initialized must complete");

	let initialize = expect_request(&mut fixture, "initialize").await;
	expect_request(&mut fixture, "notifications/initialized").await;
	assert!(
		fixture.seen.try_recv().is_err(),
		"no cancellation may be sent before tools/list is even in flight"
	);

	let error = connection.list_tools().await.expect_err("pending tools/list must time out");
	assert!(matches!(error, McpError::Timeout(_)), "expected McpError::Timeout, got {error:?}");

	let list = expect_request(&mut fixture, "tools/list").await;
	let cancel = expect_request(&mut fixture, "notifications/cancelled").await;
	assert!(
		fixture.seen.try_recv().is_err(),
		"fixture saw an extra request after the cancellation: {format:?}"
	);

	let original_id = list.get("id").expect("tools/list carries an id");
	let cancelled_id = cancel
		.get("params")
		.and_then(|params| params.get("requestId"))
		.expect("notifications/cancelled must carry params.requestId");

	assert_eq!(
		cancelled_id, original_id,
		"cancellation must name the timed-out tools/list ID, not another request"
	);
	assert_ne!(
		Some(cancelled_id),
		initialize.get("id"),
		"cancellation must not name the initialize ID"
	);
	assert!(
		cancel.get("id").is_none(),
		"notifications/cancelled is a notification and must omit id: {cancel}"
	);
	match format {
		RequestIdFormat::Number => {
			assert!(original_id.is_u64(), "numeric request ID must stay numeric: {original_id}");
		},
		RequestIdFormat::String => {
			assert!(original_id.is_string(), "string request ID must stay a string: {original_id}");
		},
	}
}

#[tokio::test]
async fn pending_tools_list_timeout_cancels_numeric_request_id() {
	tokio::time::timeout(
		Duration::from_secs(20),
		assert_cancel_names_original_request(RequestIdFormat::Number),
	)
	.await
	.expect("cancel contract completed within the test deadline");
}

#[tokio::test]
async fn pending_tools_list_timeout_cancels_string_request_id() {
	tokio::time::timeout(
		Duration::from_secs(20),
		assert_cancel_names_original_request(RequestIdFormat::String),
	)
	.await
	.expect("cancel contract completed within the test deadline");
}
