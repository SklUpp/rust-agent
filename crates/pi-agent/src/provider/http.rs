//! OpenAI-compatible chat-completions HTTP transport.
//!
//! Isolated from [`super`] so the provider trait, stream chunk types and the
//! stream assembler carry no concrete HTTP client dependency.

use std::time::Duration;

use async_trait::async_trait;

use super::*;

/// TCP/TLS connect budget for a provider endpoint.
///
/// Deliberately no total-request timeout: a streamed completion legitimately
/// stays open for minutes, so the caller (CLI, agent loop) owns cancellation.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Ceiling for a non-streaming success body; a larger body is rejected rather
/// than buffered, because a truncated turn must never be parsed as complete.
const MAX_RESPONSE_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Ceiling for an error body kept for diagnostics; the rest is discarded so a
/// hostile gateway cannot make the client allocate an unbounded string.
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

/// Chat-completions provider for any OpenAI-compatible HTTP endpoint.
///
/// Base URL, API key and model are supplied by the caller (CLI/config): nothing
/// about a specific vendor, model id or prompt is compiled in.
pub struct OpenAiCompatibleProvider {
	http: reqwest::Client,
	base_url: String,
	api_key: String,
	model: String,
	effort: Option<String>,
	supports_images: bool,
	/// Provider-side sticky session id (TS `sessionId`).
	session_id: Option<String>,
	/// Explicit prompt-cache identity (TS `promptCacheKey`).
	prompt_cache_key: Option<String>,
	/// Whether this provider's dialect accepts `prompt_cache_key` on the wire.
	///
	/// TS `openai-completions.ts` sends it for `kimi-code`, or for a model whose
	/// `compat.supportsPromptCacheBreakpoints` is set *and* prompt caching was
	/// requested. Rust models no prompt-cache mode, so callers enable this only
	/// for the kimi-code dialect.
	prompt_cache_enabled: bool,
	/// Whether the streaming dialect accepts `stream_options.include_usage`
	/// (TS compat `supportsUsageInStreaming`, true unless a dialect opts out).
	///
	/// Enabled by default because that is the TS default; only dialects known to
	/// reject the field should turn it off.
	usage_in_streaming: bool,
	/// When set, force native reasoning off (`reasoning_effort: "none"`).
	force_reasoning_off: bool,
	/// OpenAI `service_tier`; omitted when unset/`none`/empty.
	service_tier: Option<String>,
	/// Configured provider id (catalog key) stamped on produced messages so
	/// signed reasoning is only replayed to the route that issued it.
	provider_id: Option<String>,
	/// Per-request credential callback; `None` uses `api_key`.
	credential: Option<CredentialCallback>,
}

impl OpenAiCompatibleProvider {
	/// Build a provider with a rustls HTTP client.
	pub fn new(
		base_url: impl Into<String>,
		api_key: impl Into<String>,
		model: impl Into<String>,
	) -> Result<Self, ProviderError> {
		let base_url = base_url.into();
		let parsed = reqwest::Url::parse(&base_url)
			.map_err(|err| ProviderError::InvalidBaseUrl(err.to_string()))?;
		if parsed.scheme() != "http" && parsed.scheme() != "https" {
			return Err(ProviderError::InvalidBaseUrl(format!(
				"unsupported scheme `{}`",
				parsed.scheme()
			)));
		}
		let http = crate::tls::http_client_builder()
			.connect_timeout(CONNECT_TIMEOUT)
			.build()
			.map_err(network_error)?;
		Ok(Self::with_client(http, base_url, api_key, model))
	}

	/// Build a provider around a caller-configured HTTP client.
	pub fn with_client(
		http: reqwest::Client,
		base_url: impl Into<String>,
		api_key: impl Into<String>,
		model: impl Into<String>,
	) -> Self {
		Self {
			http,
			base_url: base_url.into(),
			api_key: api_key.into(),
			model: model.into(),
			effort: None,
			supports_images: true,
			session_id: None,
			prompt_cache_key: None,
			prompt_cache_enabled: false,
			usage_in_streaming: true,
			force_reasoning_off: false,
			service_tier: None,
			provider_id: None,
			credential: None,
		}
	}

	/// Install the per-request credential callback (C6 exact OAuth).
	#[must_use]
	pub fn with_credential_callback(mut self, callback: Option<CredentialCallback>) -> Self {
		self.credential = callback;
		self
	}

	/// Stamp the configured provider/catalog id on produced messages.
	pub fn with_provider_id(mut self, provider_id: impl Into<String>) -> Self {
		let provider_id = provider_id.into();
		self.provider_id = (!provider_id.trim().is_empty()).then_some(provider_id);
		self
	}

	/// Enable or disable multimodal image support.
	pub fn with_images(mut self, enabled: bool) -> Self {
		self.supports_images = enabled;
		self
	}

	/// Whether multimodal image inputs are accepted.
	pub fn supports_images(&self) -> bool {
		self.supports_images
	}

	/// Set the reasoning effort sent as `reasoning_effort`.
	///
	/// `None` and blank strings both mean "let the endpoint decide", so the field
	/// is omitted from the request instead of being sent empty.
	pub fn with_effort(mut self, effort: Option<String>) -> Self {
		self.effort = effort.filter(|value| !value.trim().is_empty());
		self
	}

	/// Force native reasoning off (TS `forceReasoningOff` / external thinking).
	///
	/// Completions wire: `reasoning_effort: "none"` (lowest disable). Responses/
	/// Anthropic dialects are not implemented in this provider yet.
	pub fn with_force_reasoning_off(mut self, enabled: bool) -> Self {
		self.force_reasoning_off = enabled;
		self
	}

	/// OpenAI `service_tier`. `"none"` and blank strings omit the wire field.
	pub fn with_service_tier(mut self, tier: Option<String>) -> Self {
		self.service_tier = tier.filter(|value| {
			let trimmed = value.trim();
			!trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("none")
		});
		self
	}


	/// Reasoning effort currently configured for this provider.
	pub fn effort(&self) -> Option<&str> {
		self.effort.as_deref()
	}

	/// Set the provider-side session id (TS `sessionId`).
	///
	/// Empty values are treated as unset, matching the TS truthy check.
	pub fn with_session_id(mut self, session_id: Option<String>) -> Self {
		self.session_id = session_id.filter(|value| !value.is_empty());
		self
	}

	/// Pin the prompt-cache identity (TS `promptCacheKey`).
	///
	/// Empty values are treated as unset, matching the TS truthy check.
	pub fn with_prompt_cache_key(mut self, key: Option<String>) -> Self {
		self.prompt_cache_key = key.filter(|value| !value.is_empty());
		self
	}

	/// Enable `prompt_cache_key` on the wire for dialects that accept it.
	///
	/// Disabled by default so a generic OpenAI-compatible endpoint never sees a
	/// cache identity it may reject. `kimi-code` is the dialect wired by the CLI.
	pub fn with_prompt_cache_enabled(mut self, enabled: bool) -> Self {
		self.prompt_cache_enabled = enabled;
		self
	}

	/// Enable or disable `stream_options.include_usage` on streamed requests.
	///
	/// On by default (TS default). Dialects that reject unknown request fields
	/// must disable it; the provider then reports no streaming usage instead of
	/// breaking the stream.
	pub fn with_usage_in_streaming(mut self, enabled: bool) -> Self {
		self.usage_in_streaming = enabled;
		self
	}

	/// Effective prompt-cache identity: explicit key, else provider session id.
	///
	/// Mirrors `getOpenAIPromptCacheKey`: `promptCacheKey ?? sessionId`, normalized
	/// to the 64-character OpenAI limit with the `pc_` hash fallback. Whether it
	/// is serialized is decided by [`Self::with_prompt_cache_enabled`].
	pub fn prompt_cache_key(&self) -> Option<String> {
		self
			.prompt_cache_key
			.as_deref()
			.or(self.session_id.as_deref())
			.and_then(normalize_prompt_cache_key)
	}

	/// `{base_url}/chat/completions`, tolerating a trailing slash in `base_url`.
	pub fn endpoint(&self) -> String {
		format!("{}/chat/completions", self.base_url.trim_end_matches('/'))
	}

	fn request(
		&self,
		messages: &[Message],
		tools: &[serde_json::Value],
		tool_choice: &ToolChoice,
		stream: bool,
	) -> Result<OpenAiChatRequest, ProviderError> {
		let tool_choice = match tool_choice {
			ToolChoice::Auto => None,
			ToolChoice::None => Some(OpenAiToolChoice::Mode(OpenAiToolChoiceMode::None)),
			ToolChoice::Required => Some(OpenAiToolChoice::Mode(OpenAiToolChoiceMode::Required)),
			ToolChoice::Tool { name } => {
				if name.trim().is_empty() {
					return Err(ProviderError::InvalidToolChoice(
						"forced tool name must not be empty".to_string(),
					));
				}
				if !tools.iter().any(|tool| {
					tool.get("type").and_then(serde_json::Value::as_str) == Some("function")
						&& tool
							.get("function")
							.and_then(|function| function.get("name"))
							.and_then(serde_json::Value::as_str)
							== Some(name.as_str())
				}) {
					return Err(ProviderError::InvalidToolChoice(format!(
						"forced tool `{name}` is not offered in this request"
					)));
				}
				Some(OpenAiToolChoice::Function {
					kind:     OpenAiToolChoiceKind::Function,
					function: OpenAiToolChoiceFunction { name: name.clone() },
				})
			},
		};
		Ok(OpenAiChatRequest {
			model: self.model.clone(),
			messages: to_openai_messages(messages)?,
			tools: tools.to_vec(),
			tool_choice,
			stream,
			stream_options: (stream && self.usage_in_streaming)
				.then_some(OpenAiStreamOptions { include_usage: true }),
			reasoning_effort: if self.force_reasoning_off {
				Some("none".to_string())
			} else {
				self.effort.clone()
			},
			prompt_cache_key: self.prompt_cache_enabled.then(|| self.prompt_cache_key()).flatten(),
			service_tier: self.service_tier.clone(),
		})
	}
}

impl OpenAiCompatibleProvider {
	/// Send one request with the per-request credential and the exact auth-retry
	/// contract: refresh only on 401/403 + parsed `invalid_key`, then reissue the
	/// same body once. Any other failure is returned unchanged. The bearer key
	/// that actually produced the response is returned so diagnostics can be
	/// redacted with it (a callback may have rotated the configured key).
	async fn post_authenticated(
		&self,
		body: &OpenAiChatRequest,
		stream: bool,
	) -> Result<(reqwest::Response, String), ProviderError> {
		let mut force_refresh = false;
		let mut key = match &self.credential {
			Some(callback) => callback(false).map_err(ProviderError::AuthRefresh)?,
			None => self.api_key.clone(),
		};
		loop {
			let mut request = self.http.post(self.endpoint()).bearer_auth(&key);
			if stream {
				request = request.header(reqwest::header::ACCEPT, "text/event-stream");
			}
			let response = request.json(body).send().await.map_err(network_error)?;
			let status = response.status();
			if status.is_success() {
				return Ok((response, key));
			}
			if !force_refresh && matches!(status.as_u16(), 401 | 403) {
				let body = read_body_bounded(response, MAX_ERROR_BODY_BYTES).await?;
				if is_invalid_key_body(&body.text) {
					if let Some(callback) = &self.credential {
						key = callback(true).map_err(ProviderError::AuthRefresh)?;
						force_refresh = true;
						continue;
					}
				}
				return Err(ProviderError::HttpStatus(
					status.as_u16(),
					diagnostic_body(&body.text, body.truncated, &self.diagnostic_secrets(&key)),
				));
			}
			let body = read_body_bounded(response, MAX_ERROR_BODY_BYTES).await?;
			return Err(ProviderError::HttpStatus(
				status.as_u16(),
				diagnostic_body(&body.text, body.truncated, &self.diagnostic_secrets(&key)),
			));
		}
	}

	/// Credential values that must not surface in diagnostics: the configured
	/// key and the bearer actually sent (a callback may have rotated it).
	fn diagnostic_secrets(&self, active_key: &str) -> Vec<String> {
		vec![self.api_key.clone(), active_key.to_string()]
	}
}

#[async_trait]
impl ModelProvider for OpenAiCompatibleProvider {
	fn api(&self) -> Option<&str> {
		Some("openai-completions")
	}

	fn provider(&self) -> Option<&str> {
		self.provider_id.as_deref()
	}

	fn model(&self) -> Option<&str> {
		Some(&self.model)
	}

	fn supports_images(&self) -> bool {
		self.supports_images
	}

	async fn complete(
		&self,
		messages: &[Message],
		tools: &[serde_json::Value],
		tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		self.check_support(messages)?;
		let body = self.request(messages, tools, tool_choice, false)?;
		let (response, key) = self.post_authenticated(&body, false).await?;

		let body = read_body_bounded(response, MAX_RESPONSE_BODY_BYTES).await?;
		if body.truncated {
			return Err(ProviderError::Protocol(format!(
				"chat completion body exceeded {MAX_RESPONSE_BODY_BYTES} bytes"
			)));
		}
		let text = body.text;
		let parsed: OpenAiChatResponse = serde_json::from_str(&text).map_err(|err| {
			let secrets = self.diagnostic_secrets(&key);
			// Serde embeds offending response values in its message, so redact
			// the whole diagnostic, not just the body snippet.
			ProviderError::Protocol(crate::provider::redact_with(
				&format!(
					"invalid chat completion body: {err}; body: {}",
					diagnostic_body(&text, false, &secrets)
				),
				&secrets,
			))
		})?;
		let usage = parsed.usage.as_ref().map(normalize_usage);
		let choice = parsed.choices.into_iter().next().ok_or_else(|| {
			ProviderError::Protocol("chat completion contained no choices".to_string())
		})?;
		let finish_reason = choice.finish_reason.clone();
		let mut message = from_openai_response(choice.message);
		message.usage = usage;
		if let Some(reason) = finish_reason.as_deref() {
			let (stop_reason, error_message) = map_openai_finish_reason(reason);
			message.stop_reason = Some(stop_reason);
			message.error_message = error_message;
		}
		Ok(message)
	}

	async fn stream(
		&self,
		messages: &[Message],
		tools: &[serde_json::Value],
		tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		self.check_support(messages)?;
		let body = self.request(messages, tools, tool_choice, true)?;
		let (response, key) = self.post_authenticated(&body, true).await?;

		let body = response
			.bytes_stream()
			.map(|item| item.map_err(|err| err.to_string()));
		let secrets = self.diagnostic_secrets(&key);
		Ok(sse_stream(Box::pin(body))
			.map(move |item| {
				item.map_err(|error| match error {
					ProviderError::Network(text) => {
						ProviderError::Network(redact_with(&text, &secrets))
					},
					ProviderError::Protocol(text) => {
						ProviderError::Protocol(redact_with(&text, &secrets))
					},
					// A serde error carries the offending text in its message
					// and cannot be rebuilt redacted, so surface it as protocol.
					ProviderError::Serialization(err) => {
						ProviderError::Protocol(redact_with(&err.to_string(), &secrets))
					},
					other => other,
				})
			})
			.boxed())
	}
}

/// Normalize a cache identity to the OpenAI wire limit (TS `normalizeOpenAIPromptCacheKey`).
fn normalize_prompt_cache_key(value: &str) -> Option<String> {
	if value.is_empty() {
		return None;
	}
	// JS `.length` counts UTF-16 code units; ASCII cache ids are the common case.
	if value.encode_utf16().count() <= 64 {
		return Some(value.to_string());
	}
	let hash = crate::tools::naming::bun_hash_base36(value.as_bytes());
	Some(format!("pc_{hash}"))
}

/// Body text capped at a byte limit, plus whether bytes were dropped.
struct BoundedBody {
	text:      String,
	truncated: bool,
}

/// Read at most `limit` body bytes, then stop; never buffers past the ceiling
/// even for an endless response.
async fn read_body_bounded(
	response: reqwest::Response,
	limit: usize,
) -> Result<BoundedBody, ProviderError> {
	let mut stream = response.bytes_stream();
	let mut bytes: Vec<u8> = Vec::new();
	let mut truncated = false;
	while let Some(item) = stream.next().await {
		let chunk = item.map_err(network_error)?;
		if bytes.len() + chunk.len() > limit {
			let room = limit.saturating_sub(bytes.len());
			bytes.extend_from_slice(&chunk[..room]);
			truncated = true;
			break;
		}
		bytes.extend_from_slice(&chunk);
	}
	let mut text = String::from_utf8_lossy(&bytes).into_owned();
	if truncated {
		trim_partial_token(&mut text);
	}
	Ok(BoundedBody { text, truncated })
}

/// Best effort for a byte-capped read: drop the incomplete trailing token so a
/// credential sliced across the cap is unlikely to survive as a prefix.
fn trim_partial_token(text: &mut String) {
	let boundary = text
		.char_indices()
		.rev()
		.find(|(_, ch)| {
			ch.is_whitespace() || matches!(ch, '"' | '\'' | ',' | ':' | '{' | '}' | '[' | ']')
		})
		.map(|(index, ch)| index + ch.len_utf8());
	if let Some(boundary) = boundary
		&& boundary < text.len()
	{
		text.truncate(boundary);
	}
}

/// Redact first, then bound to the diagnostic character limit; `truncated`
/// marks a byte-capped read so the diagnostic says the tail was dropped.
fn diagnostic_body(text: &str, truncated: bool, secrets: &[String]) -> String {
	let mut bounded = snippet(&crate::provider::redact_with(text, secrets));
	if truncated {
		bounded.push_str("\n[truncated]");
	}
	bounded
}

fn network_error(err: reqwest::Error) -> ProviderError {
	ProviderError::Network(err.to_string())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::tools::registry::tool_schema_envelope;
	use tokio::io::{AsyncReadExt, AsyncWriteExt};
	use tokio::net::TcpListener;

	#[test]
	fn chat_request_uses_explicit_prompt_cache_key_before_session_id() {
		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "key", "model-x")
			.expect("build")
			.with_prompt_cache_enabled(true)
			.with_session_id(Some("provider-session-1".to_string()))
			.with_prompt_cache_key(Some("explicit-cache-1".to_string()));
		assert_eq!(provider.prompt_cache_key().as_deref(), Some("explicit-cache-1"));
		let request = provider.request(&[], &[], &ToolChoice::Auto, true).expect("request");
		assert_eq!(request.prompt_cache_key.as_deref(), Some("explicit-cache-1"));

		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "key", "model-x")
			.expect("build")
			.with_prompt_cache_enabled(true)
			.with_session_id(Some("provider-session-1".to_string()));
		assert_eq!(provider.prompt_cache_key().as_deref(), Some("provider-session-1"));

		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "key", "model-x")
			.expect("build")
			.with_prompt_cache_enabled(true)
			.with_session_id(Some(String::new()))
			.with_prompt_cache_key(Some(String::new()));
		assert_eq!(provider.prompt_cache_key(), None);
		let payload = serde_json::to_value(provider.request(&[], &[], &ToolChoice::Auto, true).expect("request"))
			.expect("serialize");
		assert!(payload.get("prompt_cache_key").is_none(), "got: {payload}");
	}

	#[test]
	fn required_tool_choice_serializes_as_bare_mode_string() {
		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "key", "model-x")
			.expect("build");
		let tools = vec![tool_schema_envelope(
			"read",
			"Read a file",
			serde_json::json!({ "type": "object" }),
		)];
		let request = provider
			.request(&[], &tools, &ToolChoice::Required, true)
			.expect("request");
		let payload = serde_json::to_value(request).expect("serialize");
		assert_eq!(payload["tool_choice"], serde_json::json!("required"));
	}

	#[test]
	fn generic_endpoint_omits_prompt_cache_key_unless_enabled() {
		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "key", "model-x")
			.expect("build")
			.with_session_id(Some("provider-session-1".to_string()))
			.with_prompt_cache_key(Some("explicit-cache-1".to_string()));
		assert_eq!(provider.prompt_cache_key().as_deref(), Some("explicit-cache-1"));
		let payload = serde_json::to_value(provider.request(&[], &[], &ToolChoice::Auto, true).expect("request"))
			.expect("serialize");
		assert!(
			payload.get("prompt_cache_key").is_none(),
			"generic openai-completions must not receive prompt_cache_key: {payload}"
		);
	}

	#[test]
	fn overlong_prompt_cache_key_uses_ts_hash_form() {
		let long = "cache-key-".repeat(8);
		assert!(long.encode_utf16().count() > 64);
		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "key", "model-x")
			.expect("build")
			.with_prompt_cache_key(Some(long.clone()));
		let key = provider.prompt_cache_key().expect("normalized key");
		// `Bun.hash("cache-key-".repeat(8)).toString(36)` = 2jtb5bhjsyy44.
		assert_eq!(key, "pc_2jtb5bhjsyy44");
		assert_ne!(key, long);
	}

	/// Serve exactly one HTTP/1.1 request with `status`/`body`, then close.
	async fn serve_once(status: u16, body: Vec<u8>) -> String {
		let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
		let addr = listener.local_addr().expect("addr");
		tokio::spawn(async move {
			let (mut socket, _) = listener.accept().await.expect("accept");
			let mut request = Vec::new();
			let mut chunk = [0u8; 1024];
			loop {
				let read = socket.read(&mut chunk).await.expect("read request");
				if read == 0 {
					break;
				}
				request.extend_from_slice(&chunk[..read]);
				let Some(head_end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
					continue;
				};
				let head = String::from_utf8_lossy(&request[..head_end]).to_lowercase();
				let content_length = head
					.lines()
					.find_map(|line| line.strip_prefix("content-length:"))
					.and_then(|value| value.trim().parse::<usize>().ok())
					.unwrap_or(0);
				if request.len() >= head_end + 4 + content_length {
					break;
				}
			}
			let head = format!(
				"HTTP/1.1 {status} TEST\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
				body.len()
			);
			socket.write_all(head.as_bytes()).await.expect("write head");
			let _ = socket.write_all(&body).await;
			let _ = socket.shutdown().await;
		});
		format!("http://{addr}/v1")
	}

	#[tokio::test]
	async fn http_status_diagnostics_redact_the_active_key() {
		let base_url = serve_once(500, br#"{"error":{"message":"bad key sk-secret-123"}}"#.to_vec())
			.await;
		let provider = OpenAiCompatibleProvider::new(base_url, "sk-secret-123", "model-x")
			.expect("build");
		let err = provider.complete(&[], &[], &ToolChoice::Auto).await.unwrap_err();
		let text = match err {
			ProviderError::HttpStatus(status, text) => {
				assert_eq!(status, 500);
				text
			},
			other => panic!("expected HttpStatus, got {other:?}"),
		};
		assert!(!text.contains("sk-secret-123"), "leaked key: {text}");
		assert!(text.contains("[redacted]"), "not redacted: {text}");
	}

	#[tokio::test]
	async fn http_status_diagnostics_redact_the_rotated_credential() {
		let base_url = serve_once(500, b"rotated-key-456 rejected".to_vec()).await;
		let callback: CredentialCallback =
			std::sync::Arc::new(|_: bool| -> Result<String, String> {
				Ok("rotated-key-456".to_string())
			});
		let provider = OpenAiCompatibleProvider::new(base_url, "configured-key", "model-x")
			.expect("build")
			.with_credential_callback(Some(callback));
		let err = provider.complete(&[], &[], &ToolChoice::Auto).await.unwrap_err();
		let text = match err {
			ProviderError::HttpStatus(_, text) => text,
			other => panic!("expected HttpStatus, got {other:?}"),
		};
		assert!(!text.contains("rotated-key-456"), "leaked key: {text}");
		assert!(text.contains("[redacted]"), "not redacted: {text}");
	}

	#[tokio::test]
	async fn protocol_diagnostics_redact_the_active_key() {
		let base_url = serve_once(200, b"not-json sk-secret-123".to_vec()).await;
		let provider = OpenAiCompatibleProvider::new(base_url, "sk-secret-123", "model-x")
			.expect("build");
		let err = provider.complete(&[], &[], &ToolChoice::Auto).await.unwrap_err();
		let text = match err {
			ProviderError::Protocol(text) => text,
			other => panic!("expected Protocol, got {other:?}"),
		};
		assert!(!text.contains("sk-secret-123"), "leaked key: {text}");
		assert!(text.contains("[redacted]"), "not redacted: {text}");
	}

	#[tokio::test]
	async fn serde_diagnostics_redact_response_values() {
		let base_url = serve_once(200, br#"{"choices":"sk-secret-123"}"#.to_vec()).await;
		let provider = OpenAiCompatibleProvider::new(base_url, "sk-secret-123", "model-x")
			.expect("build");
		let err = provider.complete(&[], &[], &ToolChoice::Auto).await.unwrap_err();
		let text = match err {
			ProviderError::Protocol(text) => text,
			other => panic!("expected Protocol, got {other:?}"),
		};
		assert!(!text.contains("sk-secret-123"), "leaked key: {text}");
		assert!(text.contains("[redacted]"), "not redacted: {text}");
	}

	#[test]
	fn trim_partial_token_drops_the_cut_tail() {
		let mut text = "error: sk-secret".to_string();
		trim_partial_token(&mut text);
		assert_eq!(text, "error: ");
	}

	#[tokio::test]
	async fn oversized_success_body_is_rejected() {
		let body = vec![b' '; MAX_RESPONSE_BODY_BYTES + 1];
		let base_url = serve_once(200, body).await;
		let provider = OpenAiCompatibleProvider::new(base_url, "key", "model-x").expect("build");
		let err = provider.complete(&[], &[], &ToolChoice::Auto).await.unwrap_err();
		assert!(
			matches!(&err, ProviderError::Protocol(text) if text.contains("exceeded")),
			"got {err:?}"
		);
	}
}
