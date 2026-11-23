//! HTTP MCP hard-deadline wire contract.
//!
//! A stalled Streamable HTTP response must return `McpError::Timeout` by the
//! caller deadline. rmcp cancellation and socket closure are best effort;
//! tests do not require `notifications/cancelled` or remote rollback.
//!
//! Everything runs against a real loopback TCP listener; no production code is
//! modified and no global environment variables are touched.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pi_agent::mcp::{
	MCPServerConfig, MCPHttpServerConfig, MCPSseServerConfig, McpError, connect_to_server,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Per-request client deadline; short enough to keep the test fast, long enough
/// that loopback handshakes never race it.
const REQUEST_TIMEOUT: Duration = Duration::from_millis(750);
/// Scheduled slack on top of a deadline: the client may return later only by
/// this much (wakeup latency, loopback teardown), never by a second deadline.
const SLACK: Duration = Duration::from_secs(1);
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
	post_closed: Arc<AtomicBool>,
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
	let post_closed = Arc::new(AtomicBool::new(false));
	let post_closed_for_accept = Arc::clone(&post_closed);
	let connections = Arc::new(Mutex::new(Vec::new()));
	let connections_for_accept = Arc::clone(&connections);
	let accept = tokio::spawn(async move {
		loop {
			let Ok((mut socket, _)) = listener.accept().await else { return; };
			let tx = tx.clone();
			let post_closed = Arc::clone(&post_closed_for_accept);
			let handle = tokio::spawn(async move { serve(&mut socket, tx, post_closed).await; });
			connections_for_accept.lock().unwrap().push(handle);
		}
	});
	Fixture { url: format!("http://{addr}/mcp"), seen, post_closed, _server: Server { accept, connections } }
}

async fn serve(socket: &mut TcpStream, seen: mpsc::Sender<Value>, post_closed: Arc<AtomicBool>) {
	let Some(body) = read_request(socket).await else { return; };
	let method = body.get("method").and_then(Value::as_str).unwrap_or_default().to_string();
	let _ = seen.send(body.clone()).await;
	match method.as_str() {
        "initialize" => write_json(socket, &rpc_result(&body, initialize_result())).await,
        "notifications/initialized" | "notifications/cancelled" => write_json(socket, &json!({})).await,
        "tools/list" | "prompts/get" => hold_sse_open(socket, post_closed).await,
        _ => write_json(socket, &rpc_result(&body, json!({}))).await,
	}
}

/// Send SSE headers, then keep body pending. Flag socket close after client cancellation.
async fn hold_sse_open(socket: &mut TcpStream, post_closed: Arc<AtomicBool>) {
	let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n").await;
	let _ = socket.flush().await;
	let mut buf = [0u8; 1024];
	let _ = tokio::time::timeout(Duration::from_secs(30), async {
		loop {
			match socket.read(&mut buf).await {
				Ok(0) | Err(_) => { post_closed.store(true, Ordering::Release); break; },
				Ok(_) => {},
			}
		}
	}).await;
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
        "capabilities": { "tools": {}, "prompts": {} },
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

async fn assert_timed_out_request_is_bounded() {
    let mut fixture = spawn_fixture().await;
    let config = MCPHttpServerConfig {
        url: fixture.url.clone(), headers: HashMap::new(),
        timeout: Some(REQUEST_TIMEOUT.as_millis() as u64), request_id_format: None,
        header_policy: None, auth: None, oauth: None,
    };
    let connection = connect_to_server("cancel", MCPServerConfig::Http(config)).await.expect("initialize and initialized must complete");
    expect_request(&mut fixture, "initialize").await;
    expect_request(&mut fixture, "notifications/initialized").await;
    let started = std::time::Instant::now();
    let error = connection.list_tools().await.expect_err("pending tools/list must time out");
    assert!(matches!(error, McpError::Timeout(_)), "expected timeout, got {error:?}");
    assert!(
        started.elapsed() < REQUEST_TIMEOUT + SLACK,
        "a stalled tools/list must return by its deadline: {:?}",
        started.elapsed()
    );
    let list = expect_request(&mut fixture, "tools/list").await;
    assert!(list.get("id").is_some_and(Value::is_u64), "rmcp HTTP request ID must be numeric");
}

#[tokio::test]
async fn pending_tools_list_timeout_is_bounded() {
	tokio::time::timeout(Duration::from_secs(20), assert_timed_out_request_is_bounded())
		.await
		.expect("deadline contract completed within test deadline");
}

#[tokio::test]
async fn timed_out_streamable_http_post_is_bounded() {
	let mut fixture = spawn_fixture().await;
	let config = MCPHttpServerConfig {
		url: fixture.url.clone(), headers: HashMap::new(),
		timeout: Some(REQUEST_TIMEOUT.as_millis() as u64), request_id_format: None,
		header_policy: None, auth: None, oauth: None,
	};
	let connection = connect_to_server("deadline-close", MCPServerConfig::Http(config)).await.expect("initialize");
	expect_request(&mut fixture, "initialize").await;
	expect_request(&mut fixture, "notifications/initialized").await;
	let started = std::time::Instant::now();
	let _ = connection.list_tools().await.expect_err("stalled list must time out");
	assert!(
		started.elapsed() < REQUEST_TIMEOUT + SLACK,
		"a stalled POST must return by its deadline: {:?}",
		started.elapsed()
	);
	let list = expect_request(&mut fixture, "tools/list").await;
	assert!(list.get("id").is_some_and(Value::is_u64));
}

#[tokio::test]
async fn timed_out_prompts_get_is_bounded() {
	let mut fixture = spawn_fixture().await;
	let config = MCPHttpServerConfig {
		url: fixture.url.clone(), headers: HashMap::new(),
		timeout: Some(REQUEST_TIMEOUT.as_millis() as u64), request_id_format: None,
		header_policy: None, auth: None, oauth: None,
	};
	let connection = connect_to_server("deadline-prompt", MCPServerConfig::Http(config)).await.expect("initialize");
	expect_request(&mut fixture, "initialize").await;
	expect_request(&mut fixture, "notifications/initialized").await;
	let mut args = BTreeMap::new();
	args.insert("name".to_owned(), "slow".to_owned());
	let started = std::time::Instant::now();
	let result = connection.get_prompt("slow", args).await;
	assert!(matches!(result, Err(McpError::Timeout(_))), "expected prompt timeout, got {result:?}");
	assert!(
		started.elapsed() < REQUEST_TIMEOUT + SLACK,
		"a stalled prompts/get must return by its deadline: {:?}",
		started.elapsed()
	);
	let prompt = expect_request(&mut fixture, "prompts/get").await;
	assert!(prompt.get("id").is_some_and(Value::is_u64), "rmcp raw prompt ID must be numeric");
}
async fn spawn_delayed_init_fixture() -> Fixture {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
	let addr = listener.local_addr().expect("fixture address");
	let (tx, seen) = mpsc::channel(SEEN_CAPACITY);
	let post_closed = Arc::new(AtomicBool::new(false));
	let post_closed_for_accept = Arc::clone(&post_closed);
	let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
	let attempts_for_accept = Arc::clone(&attempts);
	let connections = Arc::new(Mutex::new(Vec::new()));
	let connections_for_accept = Arc::clone(&connections);
	let accept = tokio::spawn(async move {
		loop {
			let Ok((mut socket, _)) = listener.accept().await else { return; };
			let tx = tx.clone();
			let post_closed = Arc::clone(&post_closed_for_accept);
			let attempts = Arc::clone(&attempts_for_accept);
			let handle = tokio::spawn(async move {
				let Some(body) = read_request(&mut socket).await else { return; };
				let method = body.get("method").and_then(Value::as_str).unwrap_or_default().to_string();
				let _ = tx.send(body.clone()).await;
				if method == "initialize" && attempts.fetch_add(1, Ordering::AcqRel) == 0 {
					hold_sse_open(&mut socket, post_closed).await;
				} else if method == "initialize" {
					write_json(&mut socket, &rpc_result(&body, initialize_result())).await;
				} else if method == "tools/list" {
					write_json(&mut socket, &rpc_result(&body, json!({"tools": [{"name": "retry_tool", "description": "retry", "inputSchema": {"type": "object"}}]}))).await;
				} else {
					write_json(&mut socket, &rpc_result(&body, json!({}))).await;
				}
			});
			connections_for_accept.lock().unwrap().push(handle);
		}
	});
	Fixture { url: format!("http://{addr}/mcp"), seen, post_closed, _server: Server { accept, connections } }
}

async fn spawn_long_sse_fixture() -> Fixture {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
	let addr = listener.local_addr().expect("fixture address");
	let (tx, seen) = mpsc::channel(SEEN_CAPACITY);
	let post_closed = Arc::new(AtomicBool::new(false));
	let connections = Arc::new(Mutex::new(Vec::new()));
	let connections_for_accept = Arc::clone(&connections);
	let accept = tokio::spawn(async move {
		loop {
			let Ok((mut socket, _)) = listener.accept().await else { return; };
			let tx = tx.clone();
			let handle = tokio::spawn(async move {
				let Some(body) = read_request(&mut socket).await else { return; };
				let method = body.get("method").and_then(Value::as_str).unwrap_or_default().to_string();
				let _ = tx.send(body.clone()).await;
				match method.as_str() {
					"initialize" => write_json(&mut socket, &rpc_result(&body, initialize_result())).await,
					"notifications/initialized" | "notifications/cancelled" => write_json(&mut socket, &json!({})).await,
					"tools/list" => {
						let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n").await;
						let _ = socket.flush().await;
						let progress = json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"test","progress":1}});
						let _ = socket.write_all(format!("event: message\r\ndata: {progress}\r\n\r\n").as_bytes()).await;
						tokio::time::sleep(Duration::from_millis(1500)).await;
						let result = json!({"tools": [
							{"name": "preserved_tool", "description": "idle-safe", "inputSchema": {"type": "object"}},
							{"name": "second_tool", "description": "idle-safe", "inputSchema": {"type": "object"}}
						]});
						let event = format!("event: message\r\ndata: {}\r\n\r\n", rpc_result(&body, result));
						let _ = socket.write_all(event.as_bytes()).await;
						let _ = socket.flush().await;
					},
					_ => write_json(&mut socket, &rpc_result(&body, json!({}))).await,
				}
			});
			connections_for_accept.lock().unwrap().push(handle);
		}
	});
	Fixture { url: format!("http://{addr}/mcp"), seen, post_closed, _server: Server { accept, connections } }
}

async fn spawn_legacy_sse_fixture() -> Fixture {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
	let addr = listener.local_addr().expect("fixture address");
	let (tx, seen) = mpsc::channel(SEEN_CAPACITY);
	let post_closed = Arc::new(AtomicBool::new(false));
	let sse_socket: Arc<tokio::sync::Mutex<Option<TcpStream>>> = Arc::new(tokio::sync::Mutex::new(None));
	let connections = Arc::new(Mutex::new(Vec::new()));
	let connections_for_accept = Arc::clone(&connections);
	let sse_socket_for_accept = Arc::clone(&sse_socket);
	let accept = tokio::spawn(async move {
		loop {
			let Ok((mut socket, _)) = listener.accept().await else { return; };
			let tx = tx.clone();
			let sse_socket = Arc::clone(&sse_socket_for_accept);
			let handle = tokio::spawn(async move {
				let mut head_buf = [0u8; 1];
				let n = socket.peek(&mut head_buf).await.unwrap_or(0);
				if n == 1 && head_buf[0] == b'G' {
					let endpoint = format!("http://{addr}/post");
					let response = format!(
						"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n\
						event: endpoint\r\ndata: {endpoint}\r\n\r\n"
					);
					let mut guard = sse_socket.lock().await;
					let _ = socket.write_all(response.as_bytes()).await;
					let _ = socket.flush().await;
					*guard = Some(socket);
				} else if n == 1 && head_buf[0] == b'P' {
					let Some(body) = read_request(&mut socket).await else { return; };
					let method = body.get("method").and_then(Value::as_str).unwrap_or_default().to_string();
					let _ = tx.send(body.clone()).await;
					let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
					let _ = socket.flush().await;
					match method.as_str() {
						"initialize" => {
							let event = format!(
								"event: message\r\ndata: {}\r\n\r\n",
								rpc_result(&body, initialize_result())
							);
							if let Some(sse) = &mut *sse_socket.lock().await {
								let _ = sse.write_all(event.as_bytes()).await;
								let _ = sse.flush().await;
							}
						},
						"tools/list" => {
							if let Some(sse) = &mut *sse_socket.lock().await {
								let progress = json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"test","progress":1}});
								let _ = sse.write_all(format!("event: message\r\ndata: {progress}\r\n\r\n").as_bytes()).await;
							}
							tokio::time::sleep(Duration::from_millis(1500)).await;
							let result = json!({
								"tools": [{
									"name": "legacy_tool",
									"description": "legacy sse tool",
									"inputSchema": { "type": "object" }
								}]
							});
							let event = format!(
								"event: message\r\ndata: {}\r\n\r\n",
								rpc_result(&body, result)
							);
							if let Some(sse) = &mut *sse_socket.lock().await {
								let _ = sse.write_all(event.as_bytes()).await;
								let _ = sse.flush().await;
							}
						},
						_ => {},
					}
				}
			});
			connections_for_accept.lock().unwrap().push(handle);
		}
	});
	Fixture { url: format!("http://{addr}/sse"), seen, post_closed, _server: Server { accept, connections } }
}

#[tokio::test]
async fn initialize_timeout_releases_socket_within_five_seconds() {
	let mut fixture = spawn_delayed_init_fixture().await;
	let config = MCPHttpServerConfig {
		url: fixture.url.clone(),
		headers: HashMap::new(),
		timeout: Some(REQUEST_TIMEOUT.as_millis() as u64),
		request_id_format: None,
		header_policy: None,
		auth: None,
		oauth: None,
	};
	let started = std::time::Instant::now();
	let result = connect_to_server("delay-init", MCPServerConfig::Http(config)).await;
	assert!(matches!(result, Err(McpError::Timeout(_))), "expected initialize timeout");
	assert!(started.elapsed() < REQUEST_TIMEOUT + SLACK, "initialize timeout must be bounded: {:?}", started.elapsed());
	let init_req = expect_request(&mut fixture, "initialize").await;
	assert!(init_req.get("id").is_some());

	let deadline = std::time::Instant::now() + Duration::from_secs(5);
	while !fixture.post_closed.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	assert!(fixture.post_closed.load(Ordering::Acquire), "cancelled initialize must close socket within 5 seconds");
}

#[tokio::test]
async fn dropped_initialize_future_releases_socket_within_five_seconds() {
	let mut fixture = spawn_delayed_init_fixture().await;
	let config = MCPHttpServerConfig {
		url: fixture.url.clone(), headers: HashMap::new(), timeout: Some(30_000),
		request_id_format: None, header_policy: None, auth: None, oauth: None,
	};
	let task = tokio::spawn(async move {
		connect_to_server("drop-init", MCPServerConfig::Http(config)).await
	});
	expect_request(&mut fixture, "initialize").await;
	task.abort();
	let _ = task.await;

	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	while !fixture.post_closed.load(Ordering::Acquire) && tokio::time::Instant::now() < deadline {
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	assert!(fixture.post_closed.load(Ordering::Acquire), "dropped initialize must close its socket");
}

#[tokio::test]
async fn repeated_initialize_succeeds_after_cancelled_handshake() {
	let mut fixture = spawn_delayed_init_fixture().await;
	let config = MCPHttpServerConfig {
		url: fixture.url.clone(),
		headers: HashMap::new(),
		timeout: Some(REQUEST_TIMEOUT.as_millis() as u64),
		request_id_format: None,
		header_policy: None,
		auth: None,
		oauth: None,
	};
	let result = connect_to_server("retry", MCPServerConfig::Http(config)).await;
	assert!(matches!(result, Err(McpError::Timeout(_))));
	let _ = expect_request(&mut fixture, "initialize").await;
	let deadline = std::time::Instant::now() + Duration::from_secs(5);
	while !fixture.post_closed.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	assert!(fixture.post_closed.load(Ordering::Acquire));
	let config = MCPHttpServerConfig {
		url: fixture.url.clone(),
		headers: HashMap::new(),
		timeout: Some(Duration::from_secs(5).as_millis() as u64),
		request_id_format: None,
		header_policy: None,
		auth: None,
		oauth: None,
	};
	let connection = connect_to_server("retry", MCPServerConfig::Http(config)).await.expect("same server reinitialize must succeed");
	expect_request(&mut fixture, "initialize").await;
	expect_request(&mut fixture, "notifications/initialized").await;
	let tools = connection.list_tools().await.expect("same-server tools/list must succeed");
	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name, "retry_tool");
}

#[tokio::test]
async fn rmcp_long_sse_stream_survives_idle_intervals_beyond_handshake_timeout() {
	let mut fixture = spawn_long_sse_fixture().await;
	let config = MCPHttpServerConfig {
		url: fixture.url.clone(),
		headers: HashMap::new(),
		timeout: Some(REQUEST_TIMEOUT.as_millis() as u64),
		request_id_format: None,
		header_policy: None,
		auth: None,
		oauth: None,
	};
	let connection = connect_to_server("rmcp-sse", MCPServerConfig::Http(config)).await.expect("initialize must succeed");
	expect_request(&mut fixture, "initialize").await;
	expect_request(&mut fixture, "notifications/initialized").await;
	let started = std::time::Instant::now();
	let transport = connection.transport.read().await.clone();
	let result = transport.request("tools/list", Some(json!({})), Some(Duration::from_secs(5))).await.expect("stream survives idle interval");
	assert!(started.elapsed() >= Duration::from_millis(1200));
	assert_eq!(result["tools"][0]["name"], "preserved_tool");
	assert_eq!(result["tools"][1]["name"], "second_tool");
}

#[tokio::test]
async fn legacy_sse_stream_survives_idle_intervals_beyond_handshake_timeout() {
	let mut fixture = spawn_legacy_sse_fixture().await;
	let config = MCPSseServerConfig {
		url: fixture.url.clone(),
		headers: HashMap::new(),
		timeout: Some(REQUEST_TIMEOUT.as_millis() as u64),
		request_id_format: None,
		header_policy: None,
		auth: None,
		oauth: None,
	};
	let connection = connect_to_server("legacy-sse", MCPServerConfig::Sse(config)).await.expect("legacy sse initialize must succeed");
	expect_request(&mut fixture, "initialize").await;
	expect_request(&mut fixture, "notifications/initialized").await;
	let started = std::time::Instant::now();
	let transport = connection.transport.read().await.clone();
	let result = transport.request("tools/list", Some(json!({})), Some(Duration::from_secs(5))).await.expect("legacy stream survives idle interval");
	assert!(started.elapsed() >= Duration::from_millis(1200));
	assert_eq!(result["tools"][0]["name"], "legacy_tool");
}
