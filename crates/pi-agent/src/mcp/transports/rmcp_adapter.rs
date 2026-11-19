use std::{
	collections::HashMap,
	future::Future,
	sync::atomic::{AtomicBool, Ordering},
	time::Duration,
};

use rmcp_http::header::{HeaderName, HeaderValue};
use serde_json::{Value, json};
use tokio::process::Command;

use crate::mcp::{
	error::McpError,
	transports::{McpTransport, root_uri},
	types::{MCPHttpServerConfig, MCPStdioServerConfig},
};

pub(crate) struct RmcpService {
	service: tokio::sync::Mutex<Option<rmcp::service::RunningService<rmcp::RoleClient, Handler>>>,
	/// This transport's own teardown flag. It is deliberately independent of
	/// `McpConnection::closed`, which lives one level up and governs the
	/// session; here it only decides whether an in-flight result is still
	/// trustworthy after `close()`.
	closed: AtomicBool,
}

struct Handler {
	config: rmcp::model::ClientConfig,
	root: Result<String, String>,
}

impl rmcp::ClientHandler for Handler {
	fn get_info(&self) -> rmcp::model::ClientConfig {
		self.config.clone()
	}

	fn list_roots(
		&self,
		_: rmcp::service::RequestContext<rmcp::RoleClient>,
	) -> impl Future<Output = Result<rmcp::model::ListRootsResult, rmcp::model::ErrorData>> + Send + '_
	{
		std::future::ready(
			self
				.root
				.as_ref()
				.map(|uri| {
					rmcp::model::ListRootsResult::new(vec![
						rmcp::model::Root::new(uri.clone()).with_name("workspace"),
					])
				})
				.map_err(|error| rmcp::model::ErrorData::internal_error(error.clone(), None)),
		)
	}
}

type RunningClient = rmcp::service::RunningService<rmcp::RoleClient, Handler>;

impl RmcpService {
	async fn from_handshake<F>(handshake: F, timeout_ms: u64) -> Result<Self, McpError>
	where
		F: Future<Output = Result<RunningClient, rmcp::service::ClientInitializeError>> + Send,
	{
		let service = if timeout_ms > 0 {
			tokio::time::timeout(Duration::from_millis(timeout_ms), handshake)
				.await
				.map_err(|_| {
					McpError::Timeout("MCP initialization timed out; transport dropped".into())
				})?
		} else {
			handshake.await
		}
		.map_err(map_initialize_error)?;
		Ok(Self { service: tokio::sync::Mutex::new(Some(service)), closed: AtomicBool::new(false) })
	}

	pub(crate) async fn connect_stdio(config: &MCPStdioServerConfig) -> Result<Self, McpError> {
		use rmcp::transport::child_process::TokioChildProcess;
		use rmcp::{ClientLifecycleMode, ClientServiceExt};

		let timeout_ms = crate::mcp::timeout::resolve_mcp_timeout_ms(config.timeout);
		let mut command = Command::new(&config.command);
		command.args(&config.args);
		if let Some(cwd) = &config.cwd {
			command.current_dir(cwd);
		}
		super::stdio::apply_isolated_env(&mut command, &config.env);
		let (transport, _) = TokioChildProcess::builder(command)
			.stderr(std::process::Stdio::null())
			.spawn()
			.map_err(|error| {
				McpError::Transport(format!(
					"Failed to spawn stdio process '{}': {error}",
					config.command
				))
			})?;
		let capabilities =
			serde_json::from_value(json!({"roots":{"listChanged":false}})).map_err(McpError::Json)?;
		let client = rmcp::model::InitializeRequestParams::new(
			capabilities,
			rmcp::model::Implementation::new("arag", "1.0.0"),
		)
		.with_protocol_version(rmcp::model::ProtocolVersion::LATEST_WITH_INITIALIZE);
		let root = match config.cwd.as_deref() {
			Some(path) => root_uri(std::path::Path::new(path)),
			None => std::env::current_dir()
				.map_err(|error| error.to_string())
				.and_then(|path| root_uri(&path)),
		};
		let handshake = Handler { config: client, root }
			.serve_with_lifecycle(transport, ClientLifecycleMode::Initialize);
		Self::from_handshake(handshake, timeout_ms).await
	}

	pub(crate) async fn connect_http(config: &MCPHttpServerConfig) -> Result<Self, McpError> {
		use rmcp::transport::{
			StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
		};
		use rmcp::{ClientLifecycleMode, ClientServiceExt};

		let timeout_ms = crate::mcp::timeout::resolve_mcp_timeout_ms(config.timeout);
		let headers = http_headers(&config.headers)?;
		let client = rmcp_http::Client::builder()
			.pool_max_idle_per_host(0)
			.redirect(rmcp_http::redirect::Policy::none())
			.build()
			.map_err(|error| {
				McpError::Transport(format!("Failed to build MCP HTTP client: {error}"))
			})?;
		let transport = StreamableHttpClientTransport::with_client(
			client,
			StreamableHttpClientTransportConfig::with_uri(config.url.clone())
				.custom_headers(headers)
				.max_sse_event_size(16 * 1024 * 1024)
				.reinit_on_expired_session(false),
		);
		let capabilities =
			serde_json::from_value(json!({"roots":{"listChanged":false}})).map_err(McpError::Json)?;
		let client_config = rmcp::model::InitializeRequestParams::new(
			capabilities,
			rmcp::model::Implementation::new("arag", "1.0.0"),
		)
		.with_protocol_version(rmcp::model::ProtocolVersion::LATEST_WITH_INITIALIZE);
		let root = std::env::current_dir()
			.map_err(|error| error.to_string())
			.and_then(|path| root_uri(&path));
		let handshake = Handler { config: client_config, root }
			.serve_with_lifecycle(transport, ClientLifecycleMode::Initialize);
		Self::from_handshake(handshake, timeout_ms).await
	}

	pub(crate) async fn initialized_result(
		&self,
	) -> Result<crate::mcp::types::MCPInitializeResult, McpError> {
		let service = self.service.lock().await;
		let info = service
			.as_ref()
			.ok_or(McpError::Closed)?
			.peer()
			.peer_info()
			.ok_or_else(|| McpError::Protocol("RMCP initialization omitted peer info".into()))?;
		let implementation = info.server_info.as_ref().ok_or_else(|| {
			McpError::Protocol("Server initialize response omitted serverInfo".into())
		})?;
		let mut capabilities = serde_json::to_value(&info.capabilities).map_err(McpError::Json)?;
		if let Value::Object(map) = &mut capabilities {
			map.remove("completions");
		}
		serde_json::from_value(json!({
			 "protocolVersion": info.protocol_version.as_str(),
			 "capabilities": capabilities,
			 "serverInfo": {"name": implementation.name, "version": implementation.version},
			 "instructions": info.instructions,
		}))
		.map_err(McpError::Json)
	}
}

fn http_headers(
	configured: &HashMap<String, String>,
) -> Result<HashMap<HeaderName, HeaderValue>, McpError> {
	configured
		.iter()
		.filter(|(name, _)| {
			!matches!(
				name.to_ascii_lowercase().as_str(),
				"accept" | "content-type" | "mcp-session-id" | "last-event-id" | "mcp-protocol-version"
			)
		})
		.map(|(name, value)| {
			let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
				McpError::Config(format!("Invalid MCP HTTP header name '{name}': {error}"))
			})?;
			let value = HeaderValue::from_str(value).map_err(|error| {
				McpError::Config(format!("Invalid MCP HTTP header value for '{name}': {error}"))
			})?;
			Ok((name, value))
		})
		.collect()
}

#[async_trait::async_trait]
impl McpTransport for RmcpService {
	async fn request(
		&self,
		method: &str,
		params: Option<Value>,
		timeout: Option<Duration>,
	) -> Result<Value, McpError> {
		let deadline = timeout.map(|duration| tokio::time::Instant::now() + duration);
		let raw_method = matches!(method, "prompts/get" | "ping");
		let peer = {
			let service = self.service.lock().await;
			service.as_ref().ok_or(McpError::Closed)?.peer().clone()
		};
		let run = async {
			let params = params.unwrap_or_else(|| json!({}));
			match method {
				"tools/list" => encode(peer.list_tools(parse_optional(params)?).await, false),
				"tools/call" => encode(peer.call_tool(parse_params(params)?).await, true),
				"resources/list" => encode(peer.list_resources(parse_optional(params)?).await, false),
				"resources/templates/list" => {
					encode(peer.list_resource_templates(parse_optional(params)?).await, false)
				},
				"resources/read" => encode(peer.read_resource(parse_params(params)?).await, false),
				"prompts/list" => encode(peer.list_prompts(parse_optional(params)?).await, false),
				"prompts/get" => encode(
					cancellable_request(
						&peer,
						rmcp::model::ClientRequest::CustomRequest(rmcp::model::CustomRequest::new(
							"prompts/get",
							Some(params),
						)),
						deadline,
					)
					.await,
					false,
				),
				"ping" => encode(
					cancellable_request(
						&peer,
						rmcp::model::ClientRequest::PingRequest(rmcp::model::PingRequest::default()),
						deadline,
					)
					.await,
					false,
				),
				_ => Err(McpError::Protocol(format!("Unsupported MCP method: {method}"))),
			}
		};
		let result = match (timeout, raw_method) {
			(_, true) => run.await,
			(Some(duration), false) => tokio::time::timeout(duration, run).await.map_err(|_| {
				if method == "tools/call" {
					McpError::UnknownOutcome(format!(
						"{method} timed out; verify server state before retry"
					))
				} else {
					McpError::Timeout(format!("{method} timed out"))
				}
			})?,
			(None, false) => run.await,
		};
		// Faithful classification for a `tools/call` that raced `close()`:
		// `Closed` was produced before dispatch (definitely not executed) and a
		// JSON-RPC error is a server answer (definitely executed and rejected),
		// so both keep their meaning. Only a transport failure can mean
		// "accepted, then the stream broke", which is the unknown outcome.
		if method == "tools/call"
			&& self.closed.load(Ordering::Acquire)
			&& matches!(&result, Err(McpError::Transport(_)))
		{
			return Err(McpError::UnknownOutcome(
				"tools/call interrupted by close; remote outcome unknown".into(),
			));
		}
		result
	}

	async fn notify(&self, method: &str, params: Option<Value>) -> Result<(), McpError> {
		let peer = {
			let service = self.service.lock().await;
			service.as_ref().ok_or(McpError::Closed)?.peer().clone()
		};
		peer
			.send_notification(rmcp::model::ClientNotification::CustomNotification(
				rmcp::model::CustomNotification::new(method, params),
			))
			.await
			.map_err(|_| McpError::Transport("MCP notification transport failure".into()))
	}

	async fn close(&self) -> Result<(), McpError> {
		self.closed.store(true, Ordering::Release);
		let service = {
			let mut guard = self.service.lock().await;
			guard.take()
		};
		let Some(mut service) = service else {
			return Ok(());
		};
		match service.close_with_timeout(Duration::from_secs(8)).await {
			Ok(Some(_)) => Ok(()),
			Ok(None) => Err(McpError::Transport("RMCP shutdown timed out; cleanup incomplete".into())),
			Err(_) => Err(McpError::Transport("RMCP shutdown transport failure".into())),
		}
	}
}

async fn cancellable_request(
	peer: &rmcp::service::Peer<rmcp::RoleClient>,
	request: rmcp::model::ClientRequest,
	deadline: Option<tokio::time::Instant>,
) -> Result<rmcp::model::ServerResult, rmcp::ServiceError> {
	// Caller deadline bounds registration and response wait. Dropping handle
	// future gives worker cancellation best effort; public rmcp has no
	// nonblocking cancel API, so remote cancellation is not guaranteed.
	let registration = async {
		let remaining =
			deadline.map(|instant| instant.saturating_duration_since(tokio::time::Instant::now()));
		let mut options = rmcp::service::PeerRequestOptions::no_options();
		options.timeout = remaining;
		peer.send_cancellable_request(request, options).await
	};
	let handle = match deadline {
		Some(deadline) => tokio::time::timeout_at(deadline, registration)
			.await
			.map_err(|_| rmcp::ServiceError::Timeout { timeout: Duration::ZERO })??,
		None => registration.await?,
	};
	let response = handle.await_response();
	match deadline {
		Some(deadline) => tokio::time::timeout_at(deadline, response)
			.await
			.map_err(|_| rmcp::ServiceError::Timeout { timeout: Duration::ZERO })?,
		None => response.await,
	}
}

fn parse_params<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, McpError> {
	serde_json::from_value(value)
		.map_err(|error| McpError::Protocol(format!("Invalid MCP request params: {error}")))
}

fn parse_optional<T: serde::de::DeserializeOwned>(value: Value) -> Result<Option<T>, McpError> {
	if value.as_object().is_some_and(serde_json::Map::is_empty) {
		Ok(None)
	} else {
		parse_params(value).map(Some)
	}
}

fn encode<T: serde::Serialize>(
	result: Result<T, rmcp::ServiceError>,
	mutating: bool,
) -> Result<Value, McpError> {
	serde_json::to_value(result.map_err(|error| map_service_error(error, mutating))?)
		.map_err(McpError::Json)
}

fn map_service_error(error: rmcp::ServiceError, mutating: bool) -> McpError {
	match error {
		rmcp::ServiceError::McpError(error) => McpError::JsonRpc {
			code: i64::from(error.code.0),
			message: error.message.into_owned(),
			data: error.data,
		},
		rmcp::ServiceError::Timeout { timeout } => {
			if mutating {
				McpError::UnknownOutcome(format!("RMCP request timed out after {timeout:?}"))
			} else {
				McpError::Timeout(format!("RMCP request timed out after {timeout:?}"))
			}
		},
		// A send that failed may have reached the server, so a mutating call is
		// unverified. Messages stay category-only: rmcp's transport error embeds
		// the request URL (and with it any credential in the path or query), so
		// it must not reach a tool result or a log line.
		rmcp::ServiceError::TransportSend(error) => match http_status(&error) {
			// A gateway 502/503 can arrive after the tool already ran, so a
			// mutating call keeps the unverified wording: `Http` renders as a
			// definite failure and would invite a replay.
			Some(status) if mutating && matches!(status, 502 | 503) => McpError::UnknownOutcome(
				"RMCP tools/call delivery is unverified: upstream answered with a transient error"
					.to_string(),
			),
			// The server answered with a definite error status; rmcp keeps the
			// status and no body, so surfacing it carries no secret material.
			Some(status) => McpError::Http { status, message: "RMCP transport failure".to_string() },
			None if mutating => {
				McpError::UnknownOutcome("RMCP tools/call delivery is unverified".to_string())
			},
			None => McpError::Transport("RMCP transport failure".to_string()),
		},
		rmcp::ServiceError::TransportClosed if mutating => {
			McpError::UnknownOutcome("RMCP tools/call delivery is unverified: transport closed".into())
		},
		rmcp::ServiceError::TransportClosed => {
			McpError::Transport("RMCP transport failure".to_string())
		},
		// Neither the variant name nor the body preview is echoed: both can
		// carry a response body, which is not ours to copy into a transcript.
		_ => McpError::Transport("RMCP transport failure".to_string()),
	}
}

/// HTTP status carried by an rmcp transport failure, when the server answered
/// with an error response instead of the stream breaking. rmcp keeps the status
/// and discards the body, so surfacing it cannot leak a response body secret.
///
/// rmcp's `HttpStatus` variant is transparent, so it never appears as a link in
/// `source()`; the typed error itself has to be downcast. rmcp and this crate
/// resolve the same `reqwest 0.13`, which makes the concrete type nameable; the
/// display fallback covers a future rmcp whose error type changes, because the
/// status variant is the only one whose whole message is `HTTP <code>`.
fn http_status(error: &rmcp::transport::DynamicTransportError) -> Option<u16> {
	use rmcp::transport::streamable_http_client::{HttpStatusError, StreamableHttpError};
	let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error.error.as_ref());
	while let Some(err) = current {
		if let Some(status) = err.downcast_ref::<HttpStatusError>() {
			return Some(status.status);
		}
		if let Some(StreamableHttpError::HttpStatus(status)) =
			err.downcast_ref::<StreamableHttpError<rmcp_http::Error>>()
		{
			return Some(status.status);
		}
		if let Some(code) = err
			.to_string()
			.strip_prefix("HTTP ")
			.filter(|tail| tail.len() == 3 && tail.bytes().all(|byte| byte.is_ascii_digit()))
		{
			return code.parse().ok();
		}
		current = err.source();
	}
	None
}

fn map_initialize_error(error: rmcp::service::ClientInitializeError) -> McpError {
	match error {
		rmcp::service::ClientInitializeError::JsonRpcError(error) => McpError::JsonRpc {
			code: i64::from(error.code.0),
			message: error.message.into_owned(),
			data: error.data,
		},
		// `error` is the transport's own failure (for the streamable HTTP
		// client, e.g. "Transport channel closed"); it is the only diagnostic
		// that tells a broken stream from a rejected handshake, so it is kept.
		rmcp::service::ClientInitializeError::TransportError { context, error } => {
			McpError::Protocol(format!("MCP initialization failed: {context}: {error}"))
		},
		other => McpError::Protocol(format!("MCP initialization failed: {other}")),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use rmcp::transport::DynamicTransportError;
	use rmcp::transport::streamable_http_client::{HttpStatusError, StreamableHttpError};

	fn transport_error(inner: StreamableHttpError<rmcp_http::Error>) -> DynamicTransportError {
		DynamicTransportError::from_parts("test", std::any::TypeId::of::<()>(), Box::new(inner))
	}

	#[test]
	fn http_status_reads_the_status_variant_of_a_transport_failure() {
		let error = transport_error(StreamableHttpError::HttpStatus(HttpStatusError::new(500, None)));
		assert_eq!(http_status(&error), Some(500));
	}

	#[test]
	fn mutating_transient_http_status_stays_unverified() {
		// A 502/503 can arrive after the tool ran; reporting a definite HTTP
		// failure would invite the model to replay a non-idempotent call.
		let error = transport_error(StreamableHttpError::HttpStatus(HttpStatusError::new(503, None)));
		assert!(matches!(
			map_service_error(rmcp::ServiceError::TransportSend(error), true),
			McpError::UnknownOutcome(_)
		));
		let error = transport_error(StreamableHttpError::HttpStatus(HttpStatusError::new(500, None)));
		assert!(matches!(
			map_service_error(rmcp::ServiceError::TransportSend(error), true),
			McpError::Http { status: 500, .. }
		));
		// A safe call never claims an unknown outcome.
		let error = transport_error(StreamableHttpError::HttpStatus(HttpStatusError::new(503, None)));
		assert!(matches!(
			map_service_error(rmcp::ServiceError::TransportSend(error), false),
			McpError::Http { status: 503, .. }
		));
	}

	#[test]
	fn http_status_is_none_for_a_broken_stream() {
		let error = transport_error(StreamableHttpError::TransportChannelClosed);
		assert_eq!(http_status(&error), None);
	}
}
