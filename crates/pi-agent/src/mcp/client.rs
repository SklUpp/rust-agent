use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
use std::sync::{
	Arc, LazyLock, Mutex,
	atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, RwLock};

use crate::mcp::config::validate_server_config;
use crate::mcp::error::McpError;
use crate::mcp::timeout::resolve_mcp_timeout_ms;
use crate::mcp::transports::McpTransport;
use crate::mcp::transports::rmcp_adapter::RmcpService;
use crate::mcp::transports::sse::LegacySseTransport;
use crate::mcp::types::{
	MCP_PROTOCOL_VERSION, MCPClientCapabilities, MCPContent, MCPGetPromptParams, MCPGetPromptResult,
	MCPInitializeParams, MCPInitializeResult, MCPPrompt, MCPPromptsListResult, MCPResource,
	MCPResourceReadParams, MCPResourceReadResult, MCPResourceTemplate,
	MCPResourceTemplatesListResult, MCPResourcesListResult, MCPRootsCapability,
	MCPServerCapabilities, MCPServerConfig, MCPServerInfo, MCPToolCallResult, MCPToolDefinition,
	MCPToolsListResult,
};
// Conservative pagination bounds for untrusted servers; shared by every
// cursor-paginated `*/list` method (resources, resource templates, prompts).
// 64 pages x 10k entries keeps a broken cursor from becoming unbounded
// memory/time while never limiting a real server.
const MAX_LIST_PAGES: usize = 64;
const MAX_LIST_ENTRIES: usize = 10_000;

pub fn is_retriable_connection_error(error: &McpError) -> bool {
	match error {
		McpError::AcceptedStreamError(_) | McpError::UnknownOutcome(_) => false,
		McpError::Http { status, .. } => *status == 404 || *status == 502 || *status == 503,
		McpError::Transport(msg) | McpError::Timeout(msg) | McpError::Tool(msg) => {
			let lower = msg.to_lowercase();
			lower.contains("econnrefused")
				|| lower.contains("econnreset")
				|| lower.contains("epipe")
				|| lower.contains("enetunreach")
				|| lower.contains("ehostunreach")
				|| lower.contains("fetch failed")
				|| lower.contains("transport not connected")
				|| lower.contains("transport closed")
				|| lower.contains("network error")
				|| lower.contains("connection refused")
				|| lower.contains("broken pipe")
				|| lower.contains("connection reset")
		},
		McpError::Io(io_err) => {
			use std::io::ErrorKind;
			matches!(
				io_err.kind(),
				ErrorKind::ConnectionRefused
					| ErrorKind::ConnectionReset
					| ErrorKind::BrokenPipe
					| ErrorKind::NotConnected
			)
		},
		McpError::Closed => true,
		_ => false,
	}
}

/// `tools/call` failures that leave delivery unverified: the request was on the
/// wire (or a gateway saw it and answered 502/503) when the result was lost, so a
/// replay could execute a mutating call twice. [`McpError::Closed`] is excluded —
/// transports report it only when the request never left the client.
pub fn is_unverified_delivery(error: &McpError) -> bool {
	matches!(
		error,
		McpError::Transport(_)
			| McpError::UnknownOutcome(_)
			| McpError::Http { status: 502 | 503, .. }
	)
}

/// Failures that prove the transport which produced them can no longer serve
/// requests, so the next call must reconnect. This is about transport health,
/// not replay safety: [`McpError::Closed`] is included here even though it is a
/// definite not-executed failure, because a transport that reports it has
/// already been torn down. A caller-side `Closed` never reaches this predicate
/// (it is checked before dispatch); a JSON-RPC error is a healthy answer and a
/// timeout leaves the transport intact.
fn is_transport_failure(error: &McpError) -> bool {
	is_unverified_delivery(error) || matches!(error, McpError::Closed)
}

/// Bound for every teardown step: `close()`, the retired-transport cleanup in
/// reconnect, and closing a transport that was connected but never installed.
/// A wedged transport must not hold a caller or a reconnect hostage; the rmcp
/// adapter applies its own shorter bound underneath.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct McpConnection {
	pub name: String,
	pub config: MCPServerConfig,
	pub transport: Arc<RwLock<Arc<dyn McpTransport>>>,
	pub server_info: MCPServerInfo,
	pub capabilities: MCPServerCapabilities,
	pub instructions: Option<String>,
	/// Teardown has started. Every request path checks this before dispatch, so
	/// nothing new reaches a transport that is going away and a rejected
	/// request is a definite "never executed".
	pub closed: AtomicBool,
	/// Epoch of the transport currently installed in `transport`. Bumped under
	/// the `transport` write lock, so `(transport, epoch)` read together under
	/// the read lock always name the same instance.
	pub transport_epoch: AtomicU64,
	/// Highest epoch that reported a transport failure
	/// ([`is_transport_failure`]). Monotonic.
	///
	/// Invariant: `failed_epoch >= transport_epoch` means "the installed
	/// transport is known broken". A failure records the epoch of the transport
	/// that produced it ([`McpConnection::mark_transport_failed`], `fetch_max`,
	/// so it happens exactly once); reconnect retires that state by installing
	/// `transport_epoch + 1`. Nothing is cleared, so there is no window in which
	/// reconnect can swallow a failure it never observed, and a late failure
	/// from a retired transport cannot mark a healthy successor.
	pub failed_epoch: AtomicU64,
	pub close_completed: AtomicBool,
	pub close_barrier: Arc<AsyncMutex<()>>,
	pub state_barrier: Arc<AsyncMutex<()>>,
}
impl std::fmt::Debug for McpConnection {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("McpConnection")
			.field("name", &self.name)
			.field("config", &self.config)
			.field("server_info", &self.server_info)
			.field("capabilities", &self.capabilities)
			.field("instructions", &self.instructions)
			.finish()
	}
}

async fn create_and_init_transport(
	config: &MCPServerConfig,
) -> Result<(Arc<dyn McpTransport>, MCPInitializeResult), McpError> {
	let (transport, rmcp_init_result): (Arc<dyn McpTransport>, Option<MCPInitializeResult>) =
		match config {
			MCPServerConfig::Stdio(stdio) => {
				let service = RmcpService::connect_stdio(stdio).await?;
				published_rmcp(service).await?
			},
			MCPServerConfig::Http(http) => {
				let service = RmcpService::connect_http(http).await?;
				published_rmcp(service).await?
			},
			MCPServerConfig::Sse(s) => {
				let sse = LegacySseTransport::new(s);
				sse.connect().await?;
				(Arc::new(sse), None)
			},
		};
	if let Some(result) = rmcp_init_result {
		return Ok((transport, result));
	}
	let timeout_ms = resolve_mcp_timeout_ms(config.timeout());
	let timeout_dur = if timeout_ms > 0 {
		Some(Duration::from_millis(timeout_ms))
	} else {
		None
	};
	let init_params = MCPInitializeParams {
		protocol_version: MCP_PROTOCOL_VERSION.to_string(),
		capabilities: MCPClientCapabilities {
			roots: Some(MCPRootsCapability { list_changed: false }),
		},
		client_info: crate::mcp::types::MCPClientInfo {
			name: "arag".to_string(),
			version: "1.0.0".to_string(),
		},
	};
	let initialised = async {
		let init_res_val = transport
			.request("initialize", Some(serde_json::to_value(&init_params)?), timeout_dur)
			.await?;
		let init_result: MCPInitializeResult = serde_json::from_value(init_res_val)
			.map_err(|e| McpError::Protocol(format!("Invalid initialize response: {e}")))?;
		transport.set_protocol_version(&init_result.protocol_version);
		transport
			.notify("notifications/initialized", Some(json!({})))
			.await?;
		Ok::<_, McpError>(init_result)
	};
	match initialised.await {
		Ok(init_result) => Ok((transport, init_result)),
		// A connected transport owns a live child or socket; dropping it here
		// would leak both, so close it and keep the caller's error.
		Err(error) => {
			let _ = tokio::time::timeout(CLOSE_TIMEOUT, transport.close()).await;
			Err(error)
		},
	}
}

/// Take the negotiated server info out of a finished rmcp handshake. A service
/// that handshook but cannot describe itself still owns a live child or socket,
/// so it is closed rather than dropped.
async fn published_rmcp(
	service: RmcpService,
) -> Result<(Arc<dyn McpTransport>, Option<MCPInitializeResult>), McpError> {
	match service.initialized_result().await {
		Ok(result) => Ok((Arc::new(service), Some(result))),
		Err(error) => {
			let _ = tokio::time::timeout(CLOSE_TIMEOUT, service.close()).await;
			Err(error)
		},
	}
}

/// Close a connected transport that was never published, bounded by
/// [`CLOSE_TIMEOUT`]. Failures are dropped: the caller already holds the error
/// that matters.
async fn close_uninstalled(transport: &Arc<dyn McpTransport>) {
	let _ = tokio::time::timeout(CLOSE_TIMEOUT, transport.close()).await;
}

pub async fn connect_to_server(
	name: &str,
	config: MCPServerConfig,
) -> Result<McpConnection, McpError> {
	if let Err(errors) = validate_server_config(name, &config) {
		return Err(McpError::Config(errors.join("; ")));
	}

	let (transport, init_result) = create_and_init_transport(&config).await?;

	Ok(McpConnection {
		name: name.to_string(),
		config,
		transport: Arc::new(RwLock::new(transport)),
		server_info: init_result.server_info,
		capabilities: init_result.capabilities,
		instructions: init_result.instructions,
		closed: AtomicBool::new(false),
		transport_epoch: AtomicU64::new(1),
		failed_epoch: AtomicU64::new(0),
		close_completed: AtomicBool::new(false),
		close_barrier: Arc::new(AsyncMutex::new(())),
		state_barrier: Arc::new(AsyncMutex::new(())),
	})
}

/// JSON-RPC `-32601` ("Method not found"). Servers may advertise the
/// `resources` capability without implementing the optional
/// `resources/templates/list` method.
fn is_method_not_found_error(error: &McpError) -> bool {
	if let McpError::JsonRpc { code, .. } = error {
		if *code == -32601 {
			return true;
		}
	}
	let message = error.to_string().to_lowercase();
	message.contains("-32601") || message.contains("method not found")
}

impl McpConnection {
	fn request_timeout(&self) -> Option<Duration> {
		let timeout_ms = resolve_mcp_timeout_ms(self.config.timeout());
		(timeout_ms > 0).then(|| Duration::from_millis(timeout_ms))
	}

	fn remaining(deadline: Option<Instant>) -> Option<Duration> {
		deadline.map(|at| at.saturating_duration_since(Instant::now()))
	}

	/// Take the state barrier, bounded by the caller's deadline when there is
	/// one, so waiting behind a reconnect can never outlive the request.
	async fn lock_state(
		&self,
		deadline: Option<Instant>,
		what: &str,
	) -> Result<tokio::sync::MutexGuard<'_, ()>, McpError> {
		match deadline {
			Some(at) => {
				tokio::time::timeout_at(tokio::time::Instant::from_std(at), self.state_barrier.lock())
					.await
					.map_err(|_| {
						McpError::Timeout(format!("MCP request deadline exceeded waiting for {what}"))
					})
			},
			None => Ok(self.state_barrier.lock().await),
		}
	}

	/// The installed transport is known broken: it reported an unverified
	/// delivery and has not been replaced since. See [`Self::failed_epoch`].
	fn transport_broken(&self) -> bool {
		self.failed_epoch.load(Ordering::Acquire) >= self.transport_epoch.load(Ordering::Acquire)
	}

	/// Record a delivery failure against the transport that produced it. The
	/// epoch, not a shared flag, identifies the victim: `fetch_max` keeps the
	/// highest failed epoch, so a stale failure from a retired transport is
	/// ignored by [`Self::transport_broken`] and a repeated one is idempotent.
	fn mark_transport_failed(&self, epoch: u64) {
		self.failed_epoch.fetch_max(epoch, Ordering::AcqRel);
	}

	async fn acquire_transport(
		&self,
		deadline: Option<Instant>,
	) -> Result<(Arc<dyn McpTransport>, u64), McpError> {
		let broken = {
			let _state = self.lock_state(deadline, "connection state").await?;
			if self.closed.load(Ordering::Acquire) {
				return Err(McpError::Closed);
			}
			self.transport_broken()
		};
		if broken {
			// One reconnect per breakage: concurrent callers queue on the state
			// barrier and re-check, so the second one finds the epoch already
			// retired instead of reconnecting again.
			let _state = self.lock_state(deadline, "reconnect").await?;
			if self.transport_broken() {
				self.reconnect_locked(deadline).await?;
			}
		}
		if self.closed.load(Ordering::Acquire) {
			return Err(McpError::Closed);
		}
		// Snapshot the transport and its epoch while the read guard is held:
		// the write lock installs both, so a pair read across the guard could
		// name a retired transport with a successor's epoch and then blame a
		// healthy replacement for the old one's failure.
		let (transport, epoch) = match deadline {
			Some(at) => {
				let guard =
					tokio::time::timeout_at(tokio::time::Instant::from_std(at), self.transport.read())
						.await
						.map_err(|_| {
							McpError::Timeout(
								"MCP request deadline exceeded waiting for transport lock".into(),
							)
						})?;
				let transport = guard.clone();
				let epoch = self.transport_epoch.load(Ordering::Acquire);
				drop(guard);
				(transport, epoch)
			},
			None => {
				let guard = self.transport.read().await;
				let transport = guard.clone();
				let epoch = self.transport_epoch.load(Ordering::Acquire);
				drop(guard);
				(transport, epoch)
			},
		};
		Ok((transport, epoch))
	}

	async fn request_value(
		&self,
		method: &str,
		params: Option<Value>,
		deadline: Option<Instant>,
	) -> Result<Value, McpError> {
		let (transport, epoch) = self.acquire_transport(deadline).await?;
		match transport
			.request(method, params, Self::remaining(deadline))
			.await
		{
			Ok(value) => Ok(value),
			Err(error) => {
				// See `failed_epoch`: blame the transport that carried the
				// request, exactly once, without deciding for a healthy one.
				if is_transport_failure(&error) {
					self.mark_transport_failed(epoch);
				}
				Err(error)
			},
		}
	}

	async fn reconnect_locked(&self, deadline: Option<Instant>) -> Result<(), McpError> {
		if self.closed.load(Ordering::Acquire) {
			return Err(McpError::Closed);
		}
		let handshake = create_and_init_transport(&self.config);
		let (new_transport, _) = match deadline {
			Some(at) => tokio::time::timeout_at(tokio::time::Instant::from_std(at), handshake)
				.await
				.map_err(|_| McpError::Timeout("MCP reconnect deadline exceeded".into()))??,
			None => handshake.await?,
		};
		// Publish first, clean up second. The caller's deadline covers the
		// handshake and the install; retiring the old transport gets its own
		// bounded budget, because the replacement is already healthy and a slow
		// teardown must not fail the reconnect.
		let retired = {
			let mut transport = match deadline {
				Some(at) => match tokio::time::timeout_at(
					tokio::time::Instant::from_std(at),
					self.transport.write(),
				)
				.await
				{
					Ok(guard) => guard,
					Err(_) => {
						// The replacement is fully handshaken and cannot be
						// installed: close it instead of dropping a live child
						// or HTTP session.
						close_uninstalled(&new_transport).await;
						return Err(McpError::Timeout(
							"MCP reconnect deadline exceeded waiting for transport lock".into(),
						));
					},
				},
				None => self.transport.write().await,
			};
			if self.closed.load(Ordering::Acquire) {
				drop(transport);
				close_uninstalled(&new_transport).await;
				return Err(McpError::Closed);
			}
			let retired = std::mem::replace(&mut *transport, new_transport);
			// Installing epoch + 1 retires every failure recorded against the
			// old transport; nothing else needs clearing (see `failed_epoch`).
			self.transport_epoch.fetch_add(1, Ordering::AcqRel);
			retired
		};
		// Cleanup runs detached: it must not spend the caller's remaining
		// deadline and it cannot fail the reconnect. A failure is recorded on
		// stderr, because the replacement is already serving requests.
		let name = self.name.clone();
		tokio::spawn(async move {
			match tokio::time::timeout(CLOSE_TIMEOUT, retired.close()).await {
				Ok(Ok(())) => {},
				Ok(Err(error)) => {
					eprintln!("MCP reconnect for {name}: retired transport close failed: {error}");
				},
				Err(_) => {
					eprintln!(
						"MCP reconnect for {name}: retired transport close timed out after {CLOSE_TIMEOUT:?}"
					);
				},
			}
		});
		Ok(())
	}

	pub async fn reconnect(&self) -> Result<(), McpError> {
		let _guard = self.state_barrier.lock().await;
		self.reconnect_locked(None).await
	}

	pub async fn list_tools(&self) -> Result<Vec<MCPToolDefinition>, McpError> {
		let timeout_dur = self.request_timeout();
		let deadline = timeout_dur.map(|timeout| Instant::now() + timeout);
		let mut all_tools = Vec::new();
		let mut cursor: Option<String> = None;
		let mut seen = HashSet::new();
		let mut pages = 0usize;

		loop {
			pages += 1;
			if pages > MAX_LIST_PAGES {
				return Err(McpError::Protocol("tools/list exceeded pagination bounds".to_string()));
			}
			let mut params = json!({});
			if let Some(c) = &cursor {
				params["cursor"] = json!(c);
			}
			let remaining = match deadline {
				Some(deadline) => {
					let remaining = deadline.saturating_duration_since(Instant::now());
					if remaining.is_zero() {
						return Err(McpError::Timeout(
							"tools/list pagination deadline exceeded".to_string(),
						));
					}
					Some(remaining)
				},
				None => None,
			};
			let call_res = self
				.request_value("tools/list", Some(params.clone()), deadline)
				.await;

			let res_val = match call_res {
				Ok(v) => v,
				Err(e) if is_retriable_connection_error(&e) || is_unverified_delivery(&e) => {
					// `request_value` records unverified deliveries and
					// transport failures against the transport that produced
					// them, and the retry retires that epoch. The retriable
					// classes outside that predicate (404, resets, timeouts)
					// leave the epoch healthy, so they need an explicit
					// reconnect or the retry would resend on the same broken
					// session.
					if !self.transport_broken() {
						self.reconnect().await?;
					}
					self
						.request_value("tools/list", Some(params), deadline)
						.await?
				},
				Err(e) => return Err(e),
			};
			let page: MCPToolsListResult = serde_json::from_value(res_val)
				.map_err(|e| McpError::Protocol(format!("Invalid tools/list response: {}", e)))?;

			if page.tools.len() > MAX_LIST_ENTRIES.saturating_sub(all_tools.len()) {
				return Err(McpError::Protocol("tools/list exceeded pagination bounds".to_string()));
			}
			all_tools.extend(page.tools);

			match page.next_cursor {
				Some(next) if !next.is_empty() => {
					if !seen.insert(next.clone()) {
						return Err(McpError::Protocol(
							"tools/list returned a repeated cursor".to_string(),
						));
					}
					cursor = Some(next);
				},
				_ => break,
			}
		}

		Ok(all_tools)
	}

	/// List resources, following `nextCursor` until exhausted.
	///
	/// Returns `[]` when the server did not advertise the `resources`
	/// capability (no request is sent), matching the TS client. Reads are
	/// idempotent, but nothing is cached or replayed here: the TS side caches
	/// per connection, and a stale cache is worse than one request.
	pub async fn list_resources(&self) -> Result<Vec<MCPResource>, McpError> {
		if self.capabilities.resources.is_none() {
			return Ok(Vec::new());
		}
		let timeout_dur = self.request_timeout();
		let deadline = timeout_dur.map(|timeout| Instant::now() + timeout);
		let mut all_resources = Vec::new();
		let mut cursor: Option<String> = None;
		let mut seen = HashSet::new();
		let mut pages = 0usize;

		loop {
			pages += 1;
			if pages > MAX_LIST_PAGES {
				return Err(McpError::Protocol(
					"resources/list exceeded pagination bounds".to_string(),
				));
			}
			let mut params = json!({});
			if let Some(c) = &cursor {
				params["cursor"] = json!(c);
			}
			let remaining = match deadline {
				Some(deadline) => {
					let remaining = deadline.saturating_duration_since(Instant::now());
					if remaining.is_zero() {
						return Err(McpError::Timeout(
							"resources/list pagination deadline exceeded".to_string(),
						));
					}
					Some(remaining)
				},
				None => None,
			};
			let res_val = self
				.request_value("resources/list", Some(params), deadline)
				.await?;
			let page: MCPResourcesListResult = serde_json::from_value(res_val)
				.map_err(|e| McpError::Protocol(format!("Invalid resources/list response: {}", e)))?;

			if page.resources.len() > MAX_LIST_ENTRIES.saturating_sub(all_resources.len()) {
				return Err(McpError::Protocol(
					"resources/list exceeded pagination bounds".to_string(),
				));
			}
			all_resources.extend(page.resources);

			match page.next_cursor {
				Some(next) if !next.is_empty() => {
					if !seen.insert(next.clone()) {
						return Err(McpError::Protocol(
							"resources/list returned a repeated cursor".to_string(),
						));
					}
					cursor = Some(next);
				},
				_ => break,
			}
		}

		Ok(all_resources)
	}

	/// List resource templates, following `nextCursor` until exhausted.
	///
	/// A server MAY advertise `resources` without implementing the optional
	/// `resources/templates/list` method and answer JSON-RPC `-32601`. That is
	/// treated as "no templates" so it cannot discard the server's concrete
	/// resources; every other error propagates.
	pub async fn list_resource_templates(&self) -> Result<Vec<MCPResourceTemplate>, McpError> {
		if self.capabilities.resources.is_none() {
			return Ok(Vec::new());
		}
		let timeout_dur = self.request_timeout();
		let deadline = timeout_dur.map(|timeout| Instant::now() + timeout);
		let mut all_templates = Vec::new();
		let mut cursor: Option<String> = None;
		let mut seen = HashSet::new();
		let mut pages = 0usize;

		loop {
			pages += 1;
			if pages > MAX_LIST_PAGES {
				return Err(McpError::Protocol(
					"resources/templates/list exceeded pagination bounds".to_string(),
				));
			}
			let mut params = json!({});
			if let Some(c) = &cursor {
				params["cursor"] = json!(c);
			}
			let remaining = match deadline {
				Some(deadline) => {
					let remaining = deadline.saturating_duration_since(Instant::now());
					if remaining.is_zero() {
						return Err(McpError::Timeout(
							"resources/templates/list pagination deadline exceeded".to_string(),
						));
					}
					Some(remaining)
				},
				None => None,
			};
			let res_val = self
				.request_value("resources/templates/list", Some(params), deadline)
				.await;
			let res_val = match res_val {
				Ok(v) => v,
				Err(e) if is_method_not_found_error(&e) => return Ok(Vec::new()),
				Err(e) => return Err(e),
			};
			let page: MCPResourceTemplatesListResult =
				serde_json::from_value(res_val).map_err(|e| {
					McpError::Protocol(format!("Invalid resources/templates/list response: {}", e))
				})?;

			if page.resource_templates.len() > MAX_LIST_ENTRIES.saturating_sub(all_templates.len()) {
				return Err(McpError::Protocol(
					"resources/templates/list exceeded pagination bounds".to_string(),
				));
			}
			all_templates.extend(page.resource_templates);

			match page.next_cursor {
				Some(next) if !next.is_empty() => {
					if !seen.insert(next.clone()) {
						return Err(McpError::Protocol(
							"resources/templates/list returned a repeated cursor".to_string(),
						));
					}
					cursor = Some(next);
				},
				_ => break,
			}
		}

		Ok(all_templates)
	}

	/// Read a resource by URI. The URI is sent byte-for-byte: MCP resource URIs
	/// are opaque and must not be normalized (colons, query strings, and
	/// fragments are all significant). Failures, including JSON-RPC errors,
	/// are surfaced unchanged — no synthetic content is ever produced.
	pub async fn read_resource(&self, uri: &str) -> Result<MCPResourceReadResult, McpError> {
		let timeout_dur = self.request_timeout();
		let deadline = timeout_dur.map(|timeout| Instant::now() + timeout);
		let params = serde_json::to_value(MCPResourceReadParams { uri: uri.to_string() })?;
		let res_val = self
			.request_value("resources/read", Some(params), deadline)
			.await?;

		serde_json::from_value(res_val)
			.map_err(|e| McpError::Protocol(format!("Invalid resources/read response: {}", e)))
	}

	/// Whether the server negotiated the `prompts` capability.
	#[must_use]
	pub fn supports_prompts(&self) -> bool {
		self.capabilities.prompts.is_some()
	}

	/// List prompt templates, following `nextCursor` until exhausted.
	///
	/// Unlike `list_resources`, an unnegotiated `prompts` capability is an
	/// explicit [`McpError::UnsupportedCapability`]: a caller that asked for
	/// prompts is told the server cannot serve them rather than being handed a
	/// misleading empty catalog. A server that advertised `prompts` but fails
	/// the method surfaces its own JSON-RPC error unchanged. Nothing is cached
	/// or retried, and the capability check sends no request.
	pub async fn list_prompts(&self) -> Result<Vec<MCPPrompt>, McpError> {
		if !self.supports_prompts() {
			return Err(McpError::UnsupportedCapability(
				"prompts (server did not advertise prompts/list)".to_string(),
			));
		}
		let timeout_dur = self.request_timeout();
		let deadline = timeout_dur.map(|timeout| Instant::now() + timeout);
		let mut all_prompts = Vec::new();
		let mut cursor: Option<String> = None;
		let mut seen = HashSet::new();
		let mut pages = 0usize;

		loop {
			pages += 1;
			if pages > MAX_LIST_PAGES {
				return Err(McpError::Protocol("prompts/list exceeded pagination bounds".to_string()));
			}
			let mut params = json!({});
			if let Some(c) = &cursor {
				params["cursor"] = json!(c);
			}
			let remaining = match deadline {
				Some(deadline) => {
					let remaining = deadline.saturating_duration_since(Instant::now());
					if remaining.is_zero() {
						return Err(McpError::Timeout(
							"prompts/list pagination deadline exceeded".to_string(),
						));
					}
					Some(remaining)
				},
				None => None,
			};
			let res_val = self
				.request_value("prompts/list", Some(params), deadline)
				.await?;
			let page: MCPPromptsListResult = serde_json::from_value(res_val)
				.map_err(|e| McpError::Protocol(format!("Invalid prompts/list response: {}", e)))?;

			if page.prompts.len() > MAX_LIST_ENTRIES.saturating_sub(all_prompts.len()) {
				return Err(McpError::Protocol("prompts/list exceeded pagination bounds".to_string()));
			}
			all_prompts.extend(page.prompts);

			match page.next_cursor {
				Some(next) if !next.is_empty() => {
					if !seen.insert(next.clone()) {
						return Err(McpError::Protocol(
							"prompts/list returned a repeated cursor".to_string(),
						));
					}
					cursor = Some(next);
				},
				_ => break,
			}
		}

		Ok(all_prompts)
	}

	/// Render one prompt template by name.
	///
	/// `arguments` are sent only when non-empty; a missing required argument is
	/// left to the server, whose JSON-RPC error propagates unchanged. The call
	/// is single-shot: rendering is server-side work, so a lost response is
	/// never replayed.
	pub async fn get_prompt(
		&self,
		name: &str,
		arguments: BTreeMap<String, String>,
	) -> Result<MCPGetPromptResult, McpError> {
		if !self.supports_prompts() {
			return Err(McpError::UnsupportedCapability(
				"prompts (server did not advertise prompts/get)".to_string(),
			));
		}
		let timeout_dur = self.request_timeout();
		let deadline = timeout_dur.map(|timeout| Instant::now() + timeout);
		let params = serde_json::to_value(MCPGetPromptParams {
			name: name.to_string(),
			arguments: (!arguments.is_empty()).then_some(arguments),
		})?;
		let res_val = self
			.request_value("prompts/get", Some(params), deadline)
			.await?;

		serde_json::from_value(res_val)
			.map_err(|e| McpError::Protocol(format!("Invalid prompts/get response: {}", e)))
	}

	pub async fn call_tool(
		&self,
		name: &str,
		arguments: Value,
	) -> Result<MCPToolCallResult, McpError> {
		let timeout_dur = self.request_timeout();
		let deadline = timeout_dur.map(|timeout| Instant::now() + timeout);
		let (transport, epoch) = self.acquire_transport(deadline).await?;
		let params = json!({ "name": name, "arguments": arguments });
		let call_res = transport
			.request("tools/call", Some(params), Self::remaining(deadline))
			.await;
		// Every failure is surfaced unchanged: a JSON-RPC error is a server
		// answer, a closed session never left the client, and a broken stream is
		// already `UnknownOutcome` (see the adapter). Only transport blame is
		// recorded here.
		let res_val = match call_res {
			Ok(value) => value,
			Err(error) => {
				if is_transport_failure(&error) {
					self.mark_transport_failed(epoch);
				}
				return Err(error);
			},
		};

		let mut tool_result: MCPToolCallResult = serde_json::from_value(res_val)
			.map_err(|e| McpError::Protocol(format!("Invalid tools/call response: {}", e)))?;

		if tool_result.is_error == Some(true) {
			if let Some(first) = tool_result.content.first_mut() {
				match first {
					MCPContent::Text { text } => {
						if !text.starts_with("Error:") {
							*text = format!("Error: {}", text);
						}
					},
					_ => {
						tool_result
							.content
							.insert(0, MCPContent::Text { text: "Error:".to_string() });
					},
				}
			} else {
				tool_result
					.content
					.push(MCPContent::Text { text: "Error: tool execution failed".to_string() });
			}
		}

		Ok(tool_result)
	}

	pub async fn close(&self) -> Result<(), McpError> {
		unregister_active_session_instance(self);
		let _close = tokio::time::timeout(CLOSE_TIMEOUT, self.close_barrier.lock())
			.await
			.map_err(|_| {
				McpError::Timeout("MCP close deadline exceeded waiting for a concurrent close".into())
			})?;
		if self.close_completed.load(Ordering::Acquire) {
			return Ok(());
		}
		// Checked by every request path before dispatch: from here on a request
		// is rejected as `Closed` rather than raced against the teardown.
		self.closed.store(true, Ordering::Release);
		let result = {
			let guard = tokio::time::timeout(CLOSE_TIMEOUT, self.transport.read())
				.await
				.map_err(|_| {
					McpError::Timeout("MCP close deadline exceeded waiting for transport lock".into())
				})?;
			match tokio::time::timeout(CLOSE_TIMEOUT, guard.close()).await {
				Ok(result) => result,
				Err(_) => Err(McpError::Timeout("MCP close deadline exceeded".into())),
			}
		};
		if result.is_ok() {
			self.close_completed.store(true, Ordering::Release);
		}
		result
	}
}
/// Process-wide live MCP connections for `mcp://` URI resolution.
///
/// Mirrors TS `MCPManager.#connections` / `MCPManager.instance()`: the URI
/// handler is stateless and looks sessions up here. Runtime code registers a
/// connection after `tools/list` succeeds and unregisters it on teardown; this
/// module never connects on its own.
static ACTIVE_SESSIONS: LazyLock<Mutex<BTreeMap<String, Arc<McpConnection>>>> =
	LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn lock_sessions() -> std::sync::MutexGuard<'static, BTreeMap<String, Arc<McpConnection>>> {
	ACTIVE_SESSIONS
		.lock()
		.unwrap_or_else(|poison| poison.into_inner())
}

/// Install or replace one live connection under its server name.
///
/// A replaced connection is not closed: its tools may still be executing.
/// Closing that superseded instance later leaves this registration untouched.
pub fn register_active_session(connection: Arc<McpConnection>) {
	let name = connection.name.clone();
	lock_sessions().insert(name, connection);
}

/// Drop one live connection by server name.
///
/// Name-addressed removal is a registry utility; connection teardown uses
/// [`unregister_active_session_instance`] so a same-name successor survives.
pub fn unregister_active_session(name: &str) -> Option<Arc<McpConnection>> {
	lock_sessions().remove(name)
}

/// Drop `connection` only while it is still the instance registered under its
/// name.
///
/// Registration is keyed by server name, and a later session with the same
/// name replaces the entry. Removing by name on teardown would detach that
/// successor and leave its consumers pointing at a closed session; ownership
/// is therefore decided by pointer identity.
fn unregister_active_session_instance(connection: &McpConnection) {
	let mut sessions = lock_sessions();
	let registered = sessions.get(&connection.name).is_some_and(|registered| {
		std::ptr::eq(Arc::as_ptr(registered), connection as *const McpConnection)
	});
	if registered {
		sessions.remove(&connection.name);
	}
}

/// Look up one live connection by server name.
#[must_use]
pub fn get_active_session(name: &str) -> Option<Arc<McpConnection>> {
	lock_sessions().get(name).cloned()
}

/// Sorted names of every live MCP server.
#[must_use]
pub fn list_active_sessions() -> Vec<String> {
	lock_sessions().keys().cloned().collect()
}

/// Clear every live connection. Test-only.
#[cfg(test)]
pub fn clear_active_sessions() {
	lock_sessions().clear();
}
