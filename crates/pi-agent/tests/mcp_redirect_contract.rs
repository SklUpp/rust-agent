//! MCP redirect contracts over real localhost sockets: a same-origin relative
//! `Location` must be followed with the configured credential and negotiated
//! session/protocol headers intact while returning the actual JSON-RPC result,
//! and a legacy SSE server advertising an absolute endpoint on another origin
//! must be refused before any credential or request body reaches it. A
//! missing-`Location` redirect is covered as the one uncovered boundary.
//!
//! Cross-origin HTTP redirect refusal is already covered by `mcp_boundaries.rs`
//! and `mcp.rs`; it is deliberately not repeated here.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pi_agent::mcp::{
	MCPServerConfig, MCPHttpServerConfig, MCPSseServerConfig, McpError, connect_to_server,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone)]
struct Captured {
	request_line: String,
	headers: String,
	body: Vec<u8>,
}

impl Captured {
	fn method(&self) -> &str {
		self.request_line.split_whitespace().next().unwrap_or("")
	}

	fn path(&self) -> &str {
		self.request_line.split_whitespace().nth(1).unwrap_or("")
	}

	fn header(&self, name: &str) -> Option<String> {
		self.headers.lines().skip(1).find_map(|line| {
			let (key, value) = line.split_once(':')?;
			key.eq_ignore_ascii_case(name).then(|| value.trim().to_string())
		})
	}

	fn json_body(&self) -> Value {
		serde_json::from_slice(&self.body).unwrap_or(Value::Null)
	}
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

	fn redirect(status: u16, location: Option<&str>) -> Self {
		let mut headers = Vec::new();
		if let Some(location) = location {
			headers.push(("Location", location.to_string()));
		}
		Self {
			status,
			headers,
			body: String::new(),
		}
	}

	fn sse(body: String) -> Self {
		Self {
			status: 200,
			headers: vec![("Content-Type", "text/event-stream".to_string())],
			body,
		}
	}

	fn with_header(mut self, name: &'static str, value: &str) -> Self {
		self.headers.push((name, value.to_string()));
		self
	}
}

fn reason(status: u16) -> &'static str {
	match status {
		200 => "OK",
		307 => "Temporary Redirect",
		404 => "Not Found",
		_ => "Unknown",
	}
}

async fn read_request(socket: &mut TcpStream) -> Option<Captured> {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	let mut buf = Vec::new();
	let mut chunk = [0u8; 1024];
	let header_end = loop {
		if let Some(position) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
			break position;
		}
		if tokio::time::Instant::now() >= deadline {
			return None;
		}
		let read = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut chunk))
			.await
			.ok()?
			.ok()?;
		if read == 0 {
			return None;
		}
		buf.extend_from_slice(&chunk[..read]);
	};

	let headers = String::from_utf8_lossy(&buf[..header_end]).into_owned();
	let content_length = headers
		.lines()
		.find_map(|line| {
			line.to_ascii_lowercase()
				.strip_prefix("content-length:")
				.map(|value| value.trim().parse::<usize>().unwrap_or(0))
		})
		.unwrap_or(0);

	let mut body = buf[header_end + 4..].to_vec();
	while body.len() < content_length {
		let read = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut chunk))
			.await
			.ok()?
			.ok()?;
		if read == 0 {
			break;
		}
		body.extend_from_slice(&chunk[..read]);
	}

	Some(Captured {
		request_line: headers.lines().next().unwrap_or("").to_string(),
		headers,
		body,
	})
}

struct MockServer {
	url: String,
	captured: Arc<Mutex<Vec<Captured>>>,
	handle: tokio::task::JoinHandle<()>,
}

impl Drop for MockServer {
	fn drop(&mut self) {
		self.handle.abort();
	}
}

async fn spawn_mock(
	handler: impl Fn(&Captured) -> Reply + Send + Sync + 'static,
) -> MockServer {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock listener");
	let addr = listener.local_addr().expect("mock listener address");
	let captured = Arc::new(Mutex::new(Vec::new()));
	let handler = Arc::new(handler);
	let captured_for_server = Arc::clone(&captured);

	let handle = tokio::spawn(async move {
		loop {
			let Ok((mut socket, _)) = listener.accept().await else {
				return;
			};
			let handler = Arc::clone(&handler);
			let captured = Arc::clone(&captured_for_server);
			tokio::spawn(async move {
				let Some(request) = read_request(&mut socket).await else {
					return;
				};
				captured.lock().unwrap().push(request.clone());
				let reply = (*handler)(&request);
				let mut response = format!(
					"HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
					reply.status,
					reason(reply.status),
					reply.body.len()
				);
				for (name, value) in &reply.headers {
					response.push_str(&format!("{name}: {value}\r\n"));
				}
				response.push_str("\r\n");
				response.push_str(&reply.body);
				let _ = socket.write_all(response.as_bytes()).await;
				let _ = socket.flush().await;
			});
		}
	});

	MockServer {
		url: format!("http://{addr}"),
		captured,
		handle,
	}
}

fn rpc_result(request: &Captured, result: Value) -> String {
	json!({
		"jsonrpc": "2.0",
		"id": request.json_body().get("id").cloned().unwrap_or(Value::Null),
		"result": result,
	})
	.to_string()
}

fn initialize_result() -> Value {
	json!({
		"protocolVersion": "2025-11-25",
		"capabilities": { "tools": {} },
		"serverInfo": { "name": "mock-relative-redirect", "version": "1.0.0" },
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

fn sse_config(url: &str) -> MCPSseServerConfig {
	MCPSseServerConfig {
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
async fn same_origin_relative_redirect_keeps_auth_session_and_protocol_headers() {
	const AUTH: &str = "Bearer same-origin-contract-secret";

	let server = spawn_mock(|request| {
		let body = request.json_body();
		let method = body
			.get("method")
			.and_then(Value::as_str)
			.unwrap_or("");
		match (request.path(), method) {
			("/mcp", "initialize") => Reply::redirect(307, Some("/moved-init")),
			("/moved-init", "initialize") => {
				Reply::json(rpc_result(request, initialize_result()))
					.with_header("Mcp-Session-Id", "relative-session")
			},
			("/mcp", "notifications/initialized") => Reply::json("{}".to_string()),
			("/mcp", "tools/list") => Reply::redirect(307, Some("/moved-tools")),
			("/moved-tools", "tools/list") => Reply::json(rpc_result(
				request,
				json!({
					"tools": [{ "name": "via-relative-redirect", "inputSchema": {} }],
				}),
			)),
			_ => Reply {
				status: 404,
				headers: Vec::new(),
				body: String::new(),
			},
		}
	})
	.await;

	let mut config = http_config(&format!("{}/mcp", server.url));
	config.header_policy = Some("origin-locked".to_string());
	config.headers.insert("Authorization".to_string(), AUTH.to_string());

	let connection = connect_to_server("relative_redirect", MCPServerConfig::Http(config))
		.await
		.expect("same-origin relative redirects are followed");
	let tools = connection
		.list_tools()
		.await
		.expect("tools/list must succeed through the relative redirect");
	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name, "via-relative-redirect");

	let captured = server.captured.lock().unwrap().clone();
	let redirected: Vec<&Captured> = captured
		.iter()
		.filter(|request| request.path() == "/moved-tools")
		.collect();
	assert_eq!(
		redirected.len(),
		1,
		"tools/list must be re-sent to the relative redirect target"
	);

	let request = redirected[0];
	assert_eq!(request.method(), "POST");
	assert_eq!(
		request.header("authorization").as_deref(),
		Some(AUTH),
		"configured credential must survive a same-origin redirect"
	);
	assert_eq!(
		request.header("mcp-session-id").as_deref(),
		Some("relative-session"),
		"negotiated session must survive a same-origin redirect"
	);
	assert_eq!(
		request.header("mcp-protocol-version").as_deref(),
		Some("2025-11-25"),
		"negotiated protocol version must survive a same-origin redirect"
	);
	assert_eq!(
		request.header("content-type").as_deref(),
		Some("application/json")
	);
	assert_eq!(
		request.json_body().get("method").and_then(Value::as_str),
		Some("tools/list")
	);
}

#[tokio::test]
async fn legacy_sse_cross_origin_endpoint_is_refused_before_contact() {
	const AUTH: &str = "Bearer legacy-sse-contract-secret";

	let hostile_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind hostile listener");
	let hostile_addr = hostile_listener.local_addr().expect("hostile address");
	let hostile_hits = Arc::new(AtomicUsize::new(0));
	let hits = Arc::clone(&hostile_hits);
	let hostile_handle = tokio::spawn(async move {
		loop {
			let Ok((mut socket, _)) = hostile_listener.accept().await else {
				return;
			};
			hits.fetch_add(1, Ordering::SeqCst);
			let _ = socket
				.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
				.await;
		}
	});

	let endpoint = format!("http://{hostile_addr}/steal");
	let server = spawn_mock(move |_| {
		Reply::sse(format!("event: endpoint\ndata: {endpoint}\n\n"))
	})
	.await;

	let mut config = sse_config(&format!("{}/sse", server.url));
	config.headers.insert("Authorization".to_string(), AUTH.to_string());

	let error = connect_to_server("sse_foreign_endpoint", MCPServerConfig::Sse(config))
		.await
		.expect_err("an advertised cross-origin message endpoint must be refused");
	assert!(
		matches!(error, McpError::Transport(_)),
		"expected a typed transport refusal, got {error:?}"
	);

	// Give any erroneous follow-up request time to reach the second listener.
	tokio::time::sleep(Duration::from_millis(100)).await;
	assert_eq!(
		hostile_hits.load(Ordering::SeqCst),
		0,
		"credentials or request bodies must never reach the advertised cross-origin endpoint"
	);

	let captured = server.captured.lock().unwrap().clone();
	assert_eq!(
		captured.len(),
		1,
		"only the advertised SSE stream may be contacted"
	);
	assert_eq!(captured[0].method(), "GET");
	assert_eq!(
		captured[0].header("authorization").as_deref(),
		Some(AUTH),
		"the fixture credential must be present before the refusal, so the negative check is meaningful"
	);
	assert!(
		captured[0].body.is_empty(),
		"the SSE discovery request carries no JSON-RPC body"
	);

	hostile_handle.abort();
}

#[tokio::test]
async fn redirect_without_location_is_a_typed_transport_error() {
	let server = spawn_mock(|_| Reply::redirect(307, None)).await;

	let mut config = http_config(&format!("{}/mcp", server.url));
	config.header_policy = Some("origin-locked".to_string());

	let error = connect_to_server("missing_location", MCPServerConfig::Http(config))
		.await
		.expect_err("a redirect without Location must fail");
	assert!(
		matches!(error, McpError::Transport(_)),
		"expected a typed transport error, got {error:?}"
	);

	let captured = server.captured.lock().unwrap().clone();
	assert_eq!(
		captured.len(),
		1,
		"a redirect without Location must not trigger another hop"
	);
}
