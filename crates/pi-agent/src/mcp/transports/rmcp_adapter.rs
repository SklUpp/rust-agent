use std::{
	collections::HashMap,
	future::Future,
	pin::Pin,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use reqwest::header::{HeaderName, HeaderValue};
use rmcp::{
	model::ClientJsonRpcMessage,
	transport::streamable_http_client::{
		StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
	},
};
use serde_json::{Value, json};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::mcp::{
	error::McpError,
	transports::{McpTransport, root_uri},
	types::{MCPHttpServerConfig, MCPStdioServerConfig},
};
#[cfg(feature = "test-support")]
pub type ChildKillHook = Arc<dyn Fn(rmcp::transport::child_process::ChildKillController) + Send + Sync>;

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
		#[cfg(feature = "test-support")]
		{
			Self::connect_stdio_with_kill_hook(config, None).await
		}
		#[cfg(not(feature = "test-support"))]
		{
			Self::connect_stdio_impl(config, None).await
		}
	}

	#[cfg(feature = "test-support")]
	pub(crate) async fn connect_stdio_with_kill_hook(
		config: &MCPStdioServerConfig,
		kill_hook: Option<ChildKillHook>,
	) -> Result<Self, McpError> {
		Self::connect_stdio_impl(config, kill_hook).await
	}

	async fn connect_stdio_impl(
		config: &MCPStdioServerConfig,
		#[cfg(feature = "test-support")]
		kill_hook: Option<ChildKillHook>,
		#[cfg(not(feature = "test-support"))]
		_kill_hook: Option<()>,
	) -> Result<Self, McpError> {
		use rmcp::transport::child_process::TokioChildProcess;
		use rmcp::{ClientLifecycleMode, ClientServiceExt};

		let timeout_ms = crate::mcp::timeout::resolve_mcp_timeout_ms(config.timeout);
		let mut command = Command::new(&config.command);
		command.args(&config.args);
		if let Some(cwd) = &config.cwd {
			command.current_dir(cwd);
		}
		super::stdio::apply_isolated_env(&mut command, &config.env);
		let mut builder = TokioChildProcess::builder(command)
			.stderr(std::process::Stdio::null());
		#[cfg(feature = "test-support")]
		if let Some(hook) = kill_hook {
			builder = builder.on_kill_controller(move |ctrl| hook(ctrl));
		}
		let (transport, _) = builder
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
		Self::connect_http_with_exit_hook(config, None).await
	}

	pub(crate) async fn connect_http_with_exit_hook(
		config: &MCPHttpServerConfig,
		on_exit: Option<tokio::sync::oneshot::Sender<()>>,
	) -> Result<Self, McpError> {
		Self::connect_http_with_hooks(config, on_exit, None).await
	}

	pub(crate) async fn connect_http_with_hooks(
		config: &MCPHttpServerConfig,
		on_exit: Option<tokio::sync::oneshot::Sender<()>>,
		session_recorded: Option<Arc<tokio::sync::Notify>>,
	) -> Result<Self, McpError> {
		Self::connect_http_with_all_hooks(config, on_exit, session_recorded, None, None).await
	}

	pub(crate) async fn connect_http_with_all_hooks(
		config: &MCPHttpServerConfig,
		on_exit: Option<tokio::sync::oneshot::Sender<()>>,
		session_recorded: Option<Arc<tokio::sync::Notify>>,
		main_loop_entered: Option<Arc<tokio::sync::Notify>>,
		pre_disarm_pause: Option<Arc<tokio::sync::Notify>>,
	) -> Result<Self, McpError> {
		use rmcp::transport::{
			StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
		};
		use rmcp::{ClientLifecycleMode, ClientServiceExt};
		let timeout_ms = crate::mcp::timeout::resolve_mcp_timeout_ms(config.timeout);
		let cancel_token = CancellationToken::new();

		let headers = http_headers(&config.headers)?;
		let client = crate::tls::http_client_builder()
			.pool_max_idle_per_host(0)
			.redirect(reqwest::redirect::Policy::none())
			.build()
			.map_err(|error| {
				McpError::Transport(format!("Failed to build MCP HTTP client: {error}"))
			})?;
		let guarded_client = CancellableHttpClient {
			inner: client,
			init_cancel_token: cancel_token.clone(),
		};
		let handshake_complete = Arc::new(AtomicBool::new(false));
		let mut transport_config = StreamableHttpClientTransportConfig::with_uri(config.url.clone())
			.custom_headers(headers)
			.max_sse_event_size(16 * 1024 * 1024)
			.reinit_on_expired_session(false)
			.handshake_complete(Arc::clone(&handshake_complete));
		if let Some(notify) = session_recorded {
			transport_config = transport_config.session_recorded_notify(notify);
		}
		if let Some(notify) = main_loop_entered {
			transport_config = transport_config.main_loop_entered_notify(notify);
		}
		let transport = StreamableHttpClientTransport::with_client_ct_and_exit_hook(
			guarded_client,
			transport_config,
			cancel_token.clone(),
			on_exit,
		);
		let mut handshake_guard = HandshakeDropGuard::new(cancel_token);
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
		let service = Self::from_handshake(handshake, timeout_ms).await?;
		if let Some(pause) = pre_disarm_pause {
			pause.notified().await;
		}
		handshake_complete.store(true, Ordering::Release);
		handshake_guard.disarm();
		Ok(service)
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
// ponytail: cancellation waiter remains registered across polls; rmcp's RunningService DropGuard handles teardown without spawning here.
struct CancellableSseStream<S> {
	inner: S,
	cancelled: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
}

impl<S> CancellableSseStream<S> {
	fn new(inner: S, token: CancellationToken) -> Self {
		Self { inner, cancelled: Box::pin(token.cancelled_owned()) }
	}
}

impl<S: futures::Stream + Unpin> futures::Stream for CancellableSseStream<S> {
	type Item = S::Item;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return std::task::Poll::Ready(None);
        }
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

fn is_handshake_message(message: &ClientJsonRpcMessage) -> bool {
	matches!(
		message,
		ClientJsonRpcMessage::Request(r)
			if matches!(
				&r.request,
				rmcp::model::ClientRequest::InitializeRequest(_)
					| rmcp::model::ClientRequest::DiscoverRequest(_)
			)
	) || matches!(
		message,
		ClientJsonRpcMessage::Notification(n)
			if matches!(
				&n.notification,
				rmcp::model::ClientNotification::InitializedNotification(_)
			)
	)
}

#[derive(Clone)]
struct CancellableHttpClient {
	inner: reqwest::Client,
	init_cancel_token: CancellationToken,
}

impl StreamableHttpClient for CancellableHttpClient {
	type Error = reqwest::Error;

	async fn post_message(
		&self,
		uri: Arc<str>,
		message: ClientJsonRpcMessage,
		session_id: Option<Arc<str>>,
		auth_header: Option<String>,
		custom_headers: HashMap<HeaderName, HeaderValue>,
	) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
		self.post_message_with_max_sse_event_size(
			uri,
			message,
			session_id,
			auth_header,
			custom_headers,
            16 * 1024 * 1024,
		)
		.await
	}

	async fn post_message_with_max_sse_event_size(
		&self,
		uri: Arc<str>,
		message: ClientJsonRpcMessage,
		session_id: Option<Arc<str>>,
		auth_header: Option<String>,
		custom_headers: HashMap<HeaderName, HeaderValue>,
		max_sse_event_size: usize,
	) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
		let is_handshake = is_handshake_message(&message);
		let send_fut = self.inner.post_message_with_max_sse_event_size(
			uri,
			message,
			session_id,
			auth_header,
			custom_headers,
			max_sse_event_size,
		);
		if is_handshake {
			if self.init_cancel_token.is_cancelled() {
				return Err(StreamableHttpError::TransportChannelClosed);
			}
			tokio::select! {
				biased;
				res = send_fut => {
					match res {
						Ok(StreamableHttpPostResponse::Sse(stream, session_id)) => {
							let token = self.init_cancel_token.clone();
							let cancellable = CancellableSseStream::new(stream, token);
							Ok(StreamableHttpPostResponse::Sse(Box::pin(cancellable), session_id))
						}
						other => other,
					}
				}
				_ = self.init_cancel_token.cancelled() => {
					Err(StreamableHttpError::TransportChannelClosed)
				}
			}
		} else {
			send_fut.await
		}
	}

	async fn delete_session(
		&self,
		uri: Arc<str>,
		session_id: Arc<str>,
		auth_header: Option<String>,
		custom_headers: HashMap<HeaderName, HeaderValue>,
	) -> Result<(), StreamableHttpError<Self::Error>> {
		self.inner.delete_session(uri, session_id, auth_header, custom_headers).await
	}

	async fn get_stream(
		&self,
		uri: Arc<str>,
		session_id: Option<Arc<str>>,
		last_event_id: Option<String>,
		auth_header: Option<String>,
		custom_headers: HashMap<HeaderName, HeaderValue>,
	) -> Result<rmcp::transport::common::client_side_sse::BoxedSseResponse, StreamableHttpError<Self::Error>> {
		self.inner.get_stream(uri, session_id, last_event_id, auth_header, custom_headers).await
	}

	async fn get_stream_with_max_sse_event_size(
		&self,
		uri: Arc<str>,
		session_id: Option<Arc<str>>,
		last_event_id: Option<String>,
		auth_header: Option<String>,
		custom_headers: HashMap<HeaderName, HeaderValue>,
		max_sse_event_size: usize,
	) -> Result<rmcp::transport::common::client_side_sse::BoxedSseResponse, StreamableHttpError<Self::Error>> {
		self.inner
			.get_stream_with_max_sse_event_size(
				uri,
				session_id,
				last_event_id,
				auth_header,
				custom_headers,
				max_sse_event_size,
			)
			.await
	}
}

struct HandshakeDropGuard {
	token: CancellationToken,
	armed: bool,
}

impl HandshakeDropGuard {
	fn new(token: CancellationToken) -> Self {
		Self { token, armed: true }
	}

	fn disarm(&mut self) {
		self.armed = false;
	}
}

impl Drop for HandshakeDropGuard {
	fn drop(&mut self) {
		if self.armed {
			self.token.cancel();
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

/// Reads `HTTP <code> ...` without keeping the trailing text; used only for
/// rmcp-authored status messages and the redirect response text.
fn parse_http_status_prefix(text: &str) -> Option<u16> {
	text
		.strip_prefix("HTTP ")
		.and_then(|rest| rest.get(..3))
		.filter(|code| code.bytes().all(|byte| byte.is_ascii_digit()))
		.and_then(|code| code.parse().ok())
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
			err.downcast_ref::<StreamableHttpError<reqwest::Error>>()
		{
			return Some(status.status);
		}
		// `error_for_status` keeps the status on the reqwest error even for a
		// status rmcp does not classify (for example a refused 302 redirect).
		// Downcast rather than parse text: the message also carries a URL.
		if let Some(StreamableHttpError::Client(error)) =
			err.downcast_ref::<StreamableHttpError<reqwest::Error>>()
			&& let Some(status) = error.status()
		{
			return Some(status.as_u16());
		}
		if let Some(error) = err.downcast_ref::<reqwest::Error>()
			&& let Some(status) = error.status()
		{
			return Some(status.as_u16());
		}
		if let Some(StreamableHttpError::UnexpectedServerResponse(text)) =
			err.downcast_ref::<StreamableHttpError<reqwest::Error>>()
			&& let Some(status) = parse_http_status_prefix(text.as_ref())
		{
			return Some(status);
		}
		// Keep the fallback for a status variant whose message starts with the
		// status. Only the three digits are read; the rest never leaves here.
		if let Some(code) = parse_http_status_prefix(&err.to_string()) {
			return Some(code);
		}
		current = err.source();
	}
	None
}

fn initialize_transport_detail(error: &rmcp::transport::DynamicTransportError) -> String {
	use rmcp::transport::streamable_http_client::StreamableHttpError;
	if let Some(status) = http_status(error) {
		return format!("HTTP {status}");
	}
	match error
		.error
		.downcast_ref::<StreamableHttpError<reqwest::Error>>()
	{
		Some(StreamableHttpError::TransportChannelClosed) => "Transport channel closed".to_string(),
		Some(StreamableHttpError::UnexpectedEndOfStream) => "unexpected end of stream".to_string(),
		Some(StreamableHttpError::ServerDoesNotSupportSse) => {
			"server does not support SSE".to_string()
		},
		_ => "transport failure".to_string(),
	}
}

fn map_initialize_error(error: rmcp::service::ClientInitializeError) -> McpError {
	use rmcp::service::ClientInitializeError as InitError;
	match error {
		InitError::JsonRpcError(error) => McpError::JsonRpc {
			code: i64::from(error.code.0),
			message: error.message.into_owned(),
			data: error.data,
		},
		// `context` is rmcp-authored and names the failed step. The nested
		// error's `Display` can carry the request URL and a body preview, so
		// only fixed rmcp variants or a numeric status are quoted.
		InitError::TransportError { error, context } => McpError::Protocol(format!(
			"MCP initialization failed: {context}: {}",
			initialize_transport_detail(&error)
		)),
		InitError::ConnectionClosed(_) => {
			McpError::Protocol("MCP initialization failed: connection closed".to_string())
		},
		InitError::NoCompatibleProtocolVersion { client_supported, server_supported } => {
			McpError::Protocol(format!(
				"MCP initialization failed: no compatible protocol version (client: \
				 {client_supported:?}, server: {server_supported:?})"
			))
		},
		InitError::NoPreferredProtocolVersion => {
			McpError::Protocol("MCP initialization failed: no preferred protocol version".to_string())
		},
		_ => {
			McpError::Protocol("MCP initialization failed: unexpected handshake response".to_string())
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use rmcp::transport::DynamicTransportError;
	use rmcp::transport::streamable_http_client::{HttpStatusError, StreamableHttpError};

	fn transport_error(inner: StreamableHttpError<reqwest::Error>) -> DynamicTransportError {
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

	#[test]
	fn http_status_accepts_a_prefixed_status_with_trailing_text() {
		let error =
			transport_error(StreamableHttpError::UnexpectedServerResponse("HTTP 302 Found".into()));
		assert_eq!(http_status(&error), Some(302));
		let error = transport_error(StreamableHttpError::UnexpectedServerResponse(
			"HTTP 302 https://example.test/?token=leak".into(),
		));
		assert_eq!(http_status(&error), Some(302));
	}

	#[derive(Debug)]
	struct LeakyTransportError;

	impl std::fmt::Display for LeakyTransportError {
		fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			f.write_str("failed to GET https://example.test/mcp?token=init-leak-secret")
		}
	}

	impl std::error::Error for LeakyTransportError {}

	#[test]
	fn initialize_transport_error_does_not_echo_the_transport_text() {
		// rmcp's transport text carries the request URL and a body preview.
		// Both must stay inside the adapter: `Display` reaches logs and the UI.
		let error = rmcp::service::ClientInitializeError::TransportError {
			error: DynamicTransportError::from_parts(
				"test",
				std::any::TypeId::of::<()>(),
				Box::new(LeakyTransportError),
			),
			context: "connect".into(),
		};
		let mapped = map_initialize_error(error).to_string();
		assert!(!mapped.contains("init-leak-secret"), "transport text leaked: {mapped}");
		assert!(!mapped.contains("example.test"), "URL leaked: {mapped}");
		assert!(mapped.contains("connect"), "rmcp step context is safe and useful: {mapped}");
		assert!(mapped.contains("transport failure"), "{mapped}");
	}

	#[test]
	fn initialize_http_status_is_reported_without_the_body() {
		let error = rmcp::service::ClientInitializeError::TransportError {
			error: transport_error(StreamableHttpError::HttpStatus(HttpStatusError::new(503, None))),
			context: "connect".into(),
		};
		let mapped = map_initialize_error(error).to_string();
		assert!(mapped.contains("HTTP 503"), "{mapped}");
	}

	#[test]
	fn initialize_http_status_excludes_secret_response_body() {
		let error = rmcp::service::ClientInitializeError::TransportError {
			error: transport_error(StreamableHttpError::UnexpectedServerResponse(
				"HTTP 503 secret-token-body-leak-xyz".into(),
			)),
			context: "process initialize response".into(),
		};
		let mapped = map_initialize_error(error).to_string();
		assert!(mapped.contains("HTTP 503"), "expected status HTTP 503");
		assert!(!mapped.contains("secret-token-body-leak-xyz"), "response body leaked");
	}

	async fn read_http_headers(socket: &mut tokio::net::TcpStream) -> Option<String> {
		use tokio::io::AsyncReadExt;
		let mut buf = Vec::new();
		let mut chunk = [0u8; 1024];
		loop {
			if let Some(pos) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
				return Some(String::from_utf8_lossy(&buf[..pos]).to_string());
			}
			let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut chunk)).await.ok()?.ok()?;
			if n == 0 {
				if buf.is_empty() {
					return None;
				}
				return Some(String::from_utf8_lossy(&buf).to_string());
			}
			buf.extend_from_slice(&chunk[..n]);
		}
	}

	async fn read_framed_request(socket: &mut tokio::net::TcpStream) -> Option<(String, serde_json::Value)> {
		use tokio::io::AsyncReadExt;
		let mut buf = Vec::new();
		let mut chunk = [0u8; 1024];
		let header_end = loop {
			if let Some(pos) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
				break pos;
			}
			let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut chunk)).await.ok()?.ok()?;
			if n == 0 {
				return None;
			}
			buf.extend_from_slice(&chunk[..n]);
		};
		let head_str = String::from_utf8_lossy(&buf[..header_end]).to_string();
		let content_length = head_str
			.lines()
			.find_map(|line| {
				line.to_ascii_lowercase()
					.strip_prefix("content-length:")
					.and_then(|val| val.trim().parse::<usize>().ok())
			})
			.unwrap_or(0);
		let mut body = buf[header_end + 4..].to_vec();
		while body.len() < content_length {
			let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut chunk)).await.ok()?.ok()?;
			if n == 0 {
				break;
			}
			buf.extend_from_slice(&chunk[..n]);
			body = buf[header_end + 4..].to_vec();
		}
		let json_val = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
		Some((head_str, json_val))
	}

	#[tokio::test]
	async fn connect_http_timeout_and_drop_terminate_worker_task_within_five_seconds() {
		use tokio::io::{AsyncReadExt, AsyncWriteExt};
		for external_drop in [false, true] {
			let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
			let address = listener.local_addr().unwrap();
			let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
			let server = tokio::spawn(async move {
				let (mut socket, _) = listener.accept().await.unwrap();
				let mut buf = [0; 4096];
				assert!(socket.read(&mut buf).await.unwrap() > 0);
				socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n").await.unwrap();
				let _ = ready_tx.send(());
				loop {
					if matches!(socket.read(&mut buf).await, Ok(0) | Err(_)) {
						break;
					}
				}
			});
			let config = MCPHttpServerConfig {
				url: format!("http://{address}/mcp"),
				headers: HashMap::new(),
				timeout: Some(if external_drop { 30_000 } else { 750 }),
				request_id_format: None,
				header_policy: None,
				auth: None,
				oauth: None,
			};
			let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
			let handle = tokio::spawn(async move {
				RmcpService::connect_http_with_exit_hook(&config, Some(exit_tx)).await
			});
			tokio::time::timeout(Duration::from_secs(5), ready_rx)
				.await
				.expect("server must be reached")
				.unwrap();

			let cancel_start = tokio::time::Instant::now();
			if external_drop {
				handle.abort();
			}
			let _ = tokio::time::timeout(Duration::from_secs(5), handle)
				.await
				.expect("connect handle must return within 5 seconds");

			// Single 5-second deadline for both worker exit and socket closure starting at cancellation
			tokio::time::timeout_at(
				cancel_start + Duration::from_secs(5),
				async {
					exit_rx.await.expect("worker exit signal sent");
					server.await.expect("server task join");
				},
			)
			.await
			.expect("both worker exit and socket closure must finish within five seconds of cancellation");
		}
	}

	#[tokio::test]
	async fn cancellation_before_sse_result_deletes_known_session_id_within_five_seconds() {
		use tokio::io::{AsyncReadExt, AsyncWriteExt};
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap();
		let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
		let (delete_tx, delete_rx) = tokio::sync::oneshot::channel();
		let (eof1_tx, eof1_rx) = tokio::sync::oneshot::channel();
		let client_eof = Arc::new(AtomicBool::new(false));
		let client_eof_monitor = Arc::clone(&client_eof);

		let server = tokio::spawn(async move {
			// 1. Accept initialize POST using framed reader
			let (mut socket1, _) = listener.accept().await.unwrap();
			let (_, _req_body) = read_framed_request(&mut socket1).await.expect("read framed init request");
			// Respond with headers containing Mcp-Session-Id, but no SSE event data yet
			let response = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nMcp-Session-Id: early-session-456\r\n\r\n";
			socket1.write_all(response.as_bytes()).await.unwrap();
			socket1.flush().await.unwrap();
			let _ = ready_tx.send(());

			// Monitor socket1 for client disconnect
			let socket1_monitor = tokio::spawn(async move {
				let mut check_buf = [0u8; 128];
				loop {
					match socket1.read(&mut check_buf).await {
						Ok(0) | Err(_) => {
							client_eof_monitor.store(true, Ordering::Release);
							let _ = eof1_tx.send(());
							break;
						}
						Ok(_) => {},
					}
				}
			});

			// 2. Accept DELETE request sent by worker cancellation cleanup (read complete headers)
			if let Ok((mut socket2, _)) = listener.accept().await {
				let headers = read_http_headers(&mut socket2).await.unwrap_or_default();
				if headers.starts_with("DELETE") && headers.contains("early-session-456") {
					let _ = delete_tx.send(());
				}
				let _ = socket2.write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n").await;
			}
			let _ = socket1_monitor.await;
		});

		let config = MCPHttpServerConfig {
			url: format!("http://{address}/mcp"),
			headers: HashMap::new(),
			timeout: Some(30_000),
			request_id_format: None,
			header_policy: None,
			auth: None,
			oauth: None,
		};
		let session_recorded = Arc::new(tokio::sync::Notify::new());
		let session_recorded_for_connect = Arc::clone(&session_recorded);
		let session_recorded_fut = session_recorded.notified();
		tokio::pin!(session_recorded_fut);
		let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
		let handle = tokio::spawn(async move {
			RmcpService::connect_http_with_hooks(&config, Some(exit_tx), Some(session_recorded_for_connect)).await
		});

		tokio::time::timeout(Duration::from_secs(5), ready_rx)
			.await
			.expect("server must be reached with initialize headers")
			.unwrap();

		// Synchronize: wait until client worker has parsed headers and recorded early session ID
		tokio::time::timeout(Duration::from_secs(5), &mut session_recorded_fut)
			.await
			.expect("client worker must record early session ID before cancellation");
		let cancel_start = tokio::time::Instant::now();
		handle.abort();
		let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

		tokio::time::timeout_at(
			cancel_start + Duration::from_secs(5),
			async {
				delete_rx.await.expect("DELETE request with early session ID must arrive at server");
				exit_rx.await.expect("worker task exit signal must be sent");
				eof1_rx.await.expect("socket1 must observe client EOF within 5 seconds");
			},
		)
		.await
		.expect("both DELETE arrival, worker termination, and socket closure must finish within 5 seconds of cancellation");

		tokio::time::timeout(Duration::from_secs(5), server)
			.await
			.expect("server task must complete within 5s");

		assert!(client_eof.load(Ordering::Acquire), "client socket must close on cancellation");
	}

	#[tokio::test]
	async fn cancellation_with_active_session_and_stalled_delete_completes_within_five_seconds() {
		use tokio::io::{AsyncReadExt, AsyncWriteExt};
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap();
		let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
		let (delete_tx, delete_rx) = tokio::sync::oneshot::channel();
		let (eof1_tx, eof1_rx) = tokio::sync::oneshot::channel();
		let (eof2_tx, eof2_rx) = tokio::sync::oneshot::channel();
		let (eof3_tx, eof3_rx) = tokio::sync::oneshot::channel();
		let client_eof1 = Arc::new(AtomicBool::new(false));
		let client_eof1_monitor = Arc::clone(&client_eof1);
		let client_eof2 = Arc::new(AtomicBool::new(false));
		let client_eof2_monitor = Arc::clone(&client_eof2);
		let client_eof3 = Arc::new(AtomicBool::new(false));
		let client_eof3_monitor = Arc::clone(&client_eof3);
		let server = tokio::spawn(async move {
			// 1. Accept initialize POST using framed reader
			let (mut socket1, _) = listener.accept().await.unwrap();
			let (_, req_body) = read_framed_request(&mut socket1).await.expect("read framed init request");
			let id_val = req_body.get("id").unwrap_or(&serde_json::Value::Null).to_string();
			let init_event = format!(
				"event: message\r\ndata: {{\"jsonrpc\":\"2.0\",\"id\":{id_val},\"result\":{{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{{}},\"serverInfo\":{{\"name\":\"test\",\"version\":\"1\"}}}}}}\r\n\r\n"
			);
			let response = format!(
				"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nMcp-Session-Id: test-session-123\r\n\r\n{init_event}"
			);
			socket1.write_all(response.as_bytes()).await.unwrap();
			socket1.flush().await.unwrap();

			// Monitor socket1 for client closure
			let socket1_monitor = tokio::spawn(async move {
				let mut check_buf = [0u8; 128];
				loop {
					match socket1.read(&mut check_buf).await {
						Ok(0) | Err(_) => {
							client_eof1_monitor.store(true, Ordering::Release);
							let _ = eof1_tx.send(());
							break;
						}
						Ok(_) => {},
					}
				}
			});

			// 2. Accept initialized notification POST, notify test that session is active
			let (mut socket2, _) = listener.accept().await.unwrap();
			let (_, _notif_body) = read_framed_request(&mut socket2).await.expect("read framed initialized notification");
			let _ = ready_tx.send(());

			// Monitor socket2 for client closure
			let socket2_monitor = tokio::spawn(async move {
				let mut check_buf = [0u8; 128];
				loop {
					match socket2.read(&mut check_buf).await {
						Ok(0) | Err(_) => {
							client_eof2_monitor.store(true, Ordering::Release);
							let _ = eof2_tx.send(());
							break;
						}
						Ok(_) => {},
					}
				}
			});

			// 3. Accept DELETE request sent by worker cancellation cleanup; stall response (read complete headers)
			let socket3_monitor = tokio::spawn(async move {
				if let Ok((mut socket3, _)) = listener.accept().await {
					let headers = read_http_headers(&mut socket3).await.unwrap_or_default();
					if headers.starts_with("DELETE") && headers.contains("test-session-123") {
						let _ = delete_tx.send(());
					}
					// Stalled DELETE: hold connection without replying, observe client EOF upon client-side cleanup timeout
					let mut check_buf = [0u8; 128];
					loop {
						match socket3.read(&mut check_buf).await {
							Ok(0) | Err(_) => {
								client_eof3_monitor.store(true, Ordering::Release);
								let _ = eof3_tx.send(());
								break;
							}
							Ok(_) => {},
						}
					}
				}
			});
			let _ = socket1_monitor.await;
			let _ = socket2_monitor.await;
			let _ = socket3_monitor.await;
		});

		let config = MCPHttpServerConfig {
			url: format!("http://{address}/mcp"),
			headers: HashMap::new(),
			timeout: Some(30_000),
			request_id_format: None,
			header_policy: None,
			auth: None,
			oauth: None,
		};
		let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
		let handle = tokio::spawn(async move {
			RmcpService::connect_http_with_exit_hook(&config, Some(exit_tx)).await
		});

		tokio::time::timeout(Duration::from_secs(5), ready_rx)
			.await
			.expect("server must be reached with initialized notification")
			.unwrap();

		let cancel_start = tokio::time::Instant::now();
		handle.abort();
		let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

		// Assert DELETE arrival, worker termination, and all socket closures under one shared 5s deadline
		tokio::time::timeout_at(
			cancel_start + Duration::from_secs(5),
			async {
				delete_rx.await.expect("DELETE request must arrive at server");
				exit_rx.await.expect("worker task exit signal must be sent");
				eof1_rx.await.expect("socket1 must observe client EOF within 5 seconds");
				eof2_rx.await.expect("socket2 must observe client EOF within 5 seconds");
				eof3_rx.await.expect("socket3 must observe client EOF within 5 seconds");
			},
		)
		.await
		.expect("both DELETE arrival, worker termination, and all socket closures must finish within 5 seconds of cancellation");

		assert!(client_eof1.load(Ordering::Acquire), "socket1 must close on cancellation");
		assert!(client_eof2.load(Ordering::Acquire), "socket2 must close on cancellation");
		assert!(client_eof3.load(Ordering::Acquire), "socket3 must close on cancellation");
		tokio::time::timeout(Duration::from_secs(6), server)
			.await
			.expect("server must complete within bound");
	}

	#[tokio::test]
	async fn cancellation_after_initialized_post_ack_with_stalled_delete_completes_within_five_seconds() {
		use tokio::io::{AsyncReadExt, AsyncWriteExt};
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap();
		let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
		let (delete_tx, delete_rx) = tokio::sync::oneshot::channel();
		let (eof1_tx, eof1_rx) = tokio::sync::oneshot::channel();
		let (eof3_tx, eof3_rx) = tokio::sync::oneshot::channel();
		let (eof_get_tx, eof_get_rx) = tokio::sync::oneshot::channel();
		let (get_accepted_tx, get_accepted_rx) = tokio::sync::oneshot::channel();
		let client_eof1 = Arc::new(AtomicBool::new(false));
		let client_eof1_monitor = Arc::clone(&client_eof1);
		let client_eof_get = Arc::new(AtomicBool::new(false));
		let client_eof_get_monitor = Arc::clone(&client_eof_get);
		let client_eof3 = Arc::new(AtomicBool::new(false));
		let client_eof3_monitor = Arc::clone(&client_eof3);

		let server = tokio::spawn(async move {
			// 1. Accept initialize POST using framed reader
			let (mut socket1, _) = listener.accept().await.unwrap();
			let (_, req_body) = read_framed_request(&mut socket1).await.expect("read framed init request");
			let id_val = req_body.get("id").unwrap_or(&serde_json::Value::Null).to_string();
			let init_event = format!(
				"event: message\r\ndata: {{\"jsonrpc\":\"2.0\",\"id\":{id_val},\"result\":{{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{{}},\"serverInfo\":{{\"name\":\"test\",\"version\":\"1\"}}}}}}\r\n\r\n"
			);
			let response = format!(
				"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nMcp-Session-Id: test-ack-session-456\r\n\r\n{init_event}"
			);
			socket1.write_all(response.as_bytes()).await.unwrap();
			socket1.flush().await.unwrap();

			// Monitor socket1 for client closure
			let socket1_monitor = tokio::spawn(async move {
				let mut check_buf = [0u8; 128];
				loop {
					match socket1.read(&mut check_buf).await {
						Ok(0) | Err(_) => {
							client_eof1_monitor.store(true, Ordering::Release);
							let _ = eof1_tx.send(());
							break;
						}
						Ok(_) => {},
					}
				}
			});

			// 2. Accept initialized notification POST and acknowledge it (200 OK) so worker enters main loop
			let (mut socket2, _) = listener.accept().await.unwrap();
			let (_, _notif_body) = read_framed_request(&mut socket2).await.expect("read framed initialized notification");
			socket2.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
			socket2.flush().await.unwrap();
			drop(socket2);
			let _ = ready_tx.send(());

			// 3. Dispatch incoming connections by method: route common GET separately and accept DELETE
			let socket3_monitor = tokio::spawn(async move {
				let mut eof_get_tx_opt = Some(eof_get_tx);
				let mut get_accepted_tx_opt = Some(get_accepted_tx);
				let mut get_monitor = None;
				loop {
					let Ok((mut sock, _)) = listener.accept().await else { break; };
					let headers = read_http_headers(&mut sock).await.unwrap_or_default();
					if headers.starts_with("DELETE") && headers.contains("test-ack-session-456") {
						let _ = delete_tx.send(());
						// Stalled DELETE: hold connection without replying, observe client EOF upon client-side cleanup timeout
						let mut check_buf = [0u8; 128];
						loop {
							match sock.read(&mut check_buf).await {
								Ok(0) | Err(_) => {
									client_eof3_monitor.store(true, Ordering::Release);
									let _ = eof3_tx.send(());
									break;
								}
								Ok(_) => {},
							}
						}
						break;
					} else if headers.starts_with("GET") {
						// Common SSE GET: return text/event-stream headers, hold socket open and monitor for client EOF
						let _ = sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n").await;
						let _ = sock.flush().await;
						if let Some(tx) = get_accepted_tx_opt.take() {
							let _ = tx.send(());
						}
						let client_eof_get_task = Arc::clone(&client_eof_get_monitor);
						let eof_get_tx_task = eof_get_tx_opt.take();
						get_monitor = Some(tokio::spawn(async move {
							let mut check_buf = [0u8; 128];
							loop {
								match sock.read(&mut check_buf).await {
									Ok(0) | Err(_) => {
										client_eof_get_task.store(true, Ordering::Release);
										if let Some(tx) = eof_get_tx_task {
											let _ = tx.send(());
										}
										break;
									}
									Ok(_) => {},
								}
							}
						}));
					}
				}
				if let Some(m) = get_monitor {
					let _ = m.await;
				}
			});
			let _ = socket1_monitor.await;
			let _ = socket3_monitor.await;
		});

		let config = MCPHttpServerConfig {
			url: format!("http://{address}/mcp"),
			headers: HashMap::new(),
			timeout: Some(30_000),
			request_id_format: None,
			header_policy: None,
			auth: None,
			oauth: None,
		};
		let main_loop_entered = Arc::new(tokio::sync::Notify::new());
		let main_loop_entered_for_connect = Arc::clone(&main_loop_entered);
		let main_loop_entered_fut = main_loop_entered.notified();
		tokio::pin!(main_loop_entered_fut);
		let pre_disarm_pause = Arc::new(tokio::sync::Notify::new());
		let pre_disarm_pause_for_connect = Arc::clone(&pre_disarm_pause);
		let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
		let handle = tokio::spawn(async move {
			RmcpService::connect_http_with_all_hooks(
				&config,
				Some(exit_tx),
				None,
				Some(main_loop_entered_for_connect),
				Some(pre_disarm_pause_for_connect),
			)
			.await
		});
		tokio::time::timeout(Duration::from_secs(5), ready_rx)
			.await
			.expect("server must be reached with initialized notification ack")
			.unwrap();

		// Synchronize: wait until worker has processed initialized POST ack and entered main loop
		tokio::time::timeout(Duration::from_secs(5), &mut main_loop_entered_fut)
			.await
			.expect("worker must enter main loop after initialized POST ack before cancellation");

		// Synchronize: wait until fixture has accepted and framed the common GET stream before cancellation
		tokio::time::timeout(Duration::from_secs(5), get_accepted_rx)
			.await
			.expect("fixture must accept common GET stream before cancellation")
			.unwrap();
		let cancel_start = tokio::time::Instant::now();
		handle.abort();
		let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

		// Assert DELETE arrival, worker termination, and socket closures under one shared 5s deadline
		tokio::time::timeout_at(
			cancel_start + Duration::from_secs(5),
			async {
				delete_rx.await.expect("DELETE request must arrive at server");
				exit_rx.await.expect("worker task exit signal must be sent");
				eof1_rx.await.expect("socket1 must observe client EOF within 5 seconds");
				eof_get_rx.await.expect("GET socket must observe client EOF within 5 seconds");
				eof3_rx.await.expect("socket3 must observe client EOF within 5 seconds");
			},
		)
		.await
		.expect("both DELETE arrival, worker termination, and all socket closures must finish within 5 seconds of cancellation");

		assert!(client_eof1.load(Ordering::Acquire), "socket1 must close on cancellation");
		assert!(client_eof_get.load(Ordering::Acquire), "GET socket must close on cancellation");
		assert!(client_eof3.load(Ordering::Acquire), "socket3 must close on cancellation");
		tokio::time::timeout(Duration::from_secs(6), server)
			.await
			.expect("server must complete within bound");
	}
}
