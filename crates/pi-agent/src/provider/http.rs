//! OpenAI-compatible chat-completions HTTP transport.
//!
//! Isolated from [`super`] so the provider trait, stream chunk types and the
//! stream assembler carry no concrete HTTP client dependency.

use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;

use super::*;
use crate::provider::{
	body::{bounded_redaction, read_body_bounded},
	retry::{
		RetryPolicy, is_concurrency_admission_rejection, should_retry_response, sleep_before_retry,
	},
};

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
	/// Accept the `developer` role instead of folding it into `system`.
	developer_role: bool,
	/// Accept `reasoning_effort` at all; when false the field is omitted.
	supports_reasoning_effort: bool,
	/// Per-model effort remap applied to `reasoning_effort`.
	reasoning_effort_map: BTreeMap<String, String>,
	/// Wire field carrying the output-token cap.
	max_tokens_field: MaxTokensField,
	/// Output-token cap sent on every request (catalog `maxTokens`).
	max_tokens: Option<u64>,
	/// Accept OpenAI strict-mode tool schemas; when false `function.strict`
	/// is stripped from every offered tool.
	supports_strict_mode: bool,
	/// Accept the OpenAI `store` request field.
	supports_store: bool,
	/// Sampling temperature; omitted when unset.
	temperature: Option<f64>,
	/// Nucleus-sampling probability mass; omitted when unset.
	top_p: Option<f64>,
	/// Stop sequence(s), passed through verbatim; omitted when unset.
	stop: Option<serde_json::Value>,
	/// End-user identifier for abuse monitoring; omitted when unset.
	user: Option<String>,
	/// Request metadata object, passed through verbatim; omitted when unset.
	metadata: Option<serde_json::Value>,
	/// Configured provider id (catalog key) stamped on produced messages so
	/// signed reasoning is only replayed to the route that issued it.
	provider_id: Option<String>,
	/// Per-request credential callback; `None` uses `api_key`.
	credential: Option<CredentialCallback>,
	/// Caller-supplied diagnostic scrubber, applied after credential redaction.
	diagnostic_redactor: Option<DiagnosticRedactor>,
	/// Retry budget and delay schedule for transient HTTP failures.
	retry: RetryPolicy,
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
			developer_role: true,
			supports_reasoning_effort: true,
			reasoning_effort_map: BTreeMap::new(),
			max_tokens_field: MaxTokensField::default(),
			max_tokens: None,
			supports_strict_mode: true,
			supports_store: true,
			temperature: None,
			top_p: None,
			stop: None,
			user: None,
			metadata: None,
			provider_id: None,
			credential: None,
			diagnostic_redactor: None,
			retry: RetryPolicy::default(),
		}
	}

	/// Install the per-request credential callback (C6 exact OAuth).
	#[must_use]
	pub fn with_credential_callback(mut self, callback: Option<CredentialCallback>) -> Self {
		self.credential = callback;
		self
	}

	/// Install a caller-supplied diagnostic scrubber. It runs after the
	/// built-in credential redaction on every provider error and streamed
	/// error message.
	#[must_use]
	pub fn with_diagnostic_redactor(mut self, redactor: Option<DiagnosticRedactor>) -> Self {
		self.diagnostic_redactor = redactor;
		self
	}

	/// Replace the transient-HTTP retry budget. `0` makes every failure
	/// terminal (the pre-retry contract used by transport-boundary tests).
	#[must_use]
	pub fn with_max_retries(mut self, retries: u32) -> Self {
		self.retry.max_retries = retries;
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

	/// Accept the `developer` role; when false it folds into `system`.
	pub fn with_developer_role(mut self, enabled: bool) -> Self {
		self.developer_role = enabled;
		self
	}

	/// Accept `reasoning_effort`; when false the field is omitted entirely.
	pub fn with_reasoning_effort_support(mut self, enabled: bool) -> Self {
		self.supports_reasoning_effort = enabled;
		self
	}

	/// Install the per-model `reasoning_effort` remap.
	pub fn with_effort_map(mut self, map: BTreeMap<String, String>) -> Self {
		self.reasoning_effort_map = map;
		self
	}

	/// Choose the wire field carrying the output-token cap.
	pub fn with_max_tokens_field(mut self, field: MaxTokensField) -> Self {
		self.max_tokens_field = field;
		self
	}

	/// Set the output-token cap sent on every request.
	pub fn with_max_tokens(mut self, max_tokens: Option<u64>) -> Self {
		self.max_tokens = max_tokens;
		self
	}

	/// Accept OpenAI strict-mode tool schemas; when false `function.strict`
	/// is stripped from every offered tool.
	pub fn with_strict_mode(mut self, enabled: bool) -> Self {
		self.supports_strict_mode = enabled;
		self
	}

	/// Accept the OpenAI `store` field; when false it is omitted.
	pub fn with_store(mut self, enabled: bool) -> Self {
		self.supports_store = enabled;
		self
	}

	/// Sampling temperature; `None` omits the field.
	pub fn with_temperature(mut self, temperature: Option<f64>) -> Self {
		self.temperature = temperature;
		self
	}

	/// Nucleus-sampling probability mass; `None` omits the field.
	pub fn with_top_p(mut self, top_p: Option<f64>) -> Self {
		self.top_p = top_p;
		self
	}

	/// Stop sequence(s), passed through verbatim; `None` omits the field.
	pub fn with_stop(mut self, stop: Option<serde_json::Value>) -> Self {
		self.stop = stop;
		self
	}

	/// End-user identifier; `None` omits the field.
	pub fn with_user(mut self, user: Option<String>) -> Self {
		self.user = user;
		self
	}

	/// Request metadata object, passed through verbatim; `None` omits it.
	pub fn with_metadata(mut self, metadata: Option<serde_json::Value>) -> Self {
		self.metadata = metadata;
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
		let tools = if self.supports_strict_mode {
			tools.to_vec()
		} else {
			tools.iter().map(strip_strict).collect()
		};
		let (max_tokens, max_completion_tokens) = match (self.max_tokens, self.max_tokens_field) {
			(Some(cap), MaxTokensField::MaxTokens) => (Some(cap), None),
			(Some(cap), MaxTokensField::MaxCompletionTokens) => (None, Some(cap)),
			(None, _) => (None, None),
		};
		Ok(OpenAiChatRequest {
			model: self.model.clone(),
			messages: to_openai_messages_with_developer_role(messages, self.developer_role)?,
			tools,
			tool_choice,
			stream,
			stream_options: (stream && self.usage_in_streaming)
				.then_some(OpenAiStreamOptions { include_usage: true }),
			store: self.supports_store.then_some(true),
			reasoning_effort: if !self.supports_reasoning_effort {
				None
			} else if self.force_reasoning_off {
				Some("none".to_string())
			} else {
				self.effort.as_deref().map(|effort| {
					self.reasoning_effort_map
						.get(effort)
						.cloned()
						.unwrap_or_else(|| effort.to_string())
				})
			},
			prompt_cache_key: self.prompt_cache_enabled.then(|| self.prompt_cache_key()).flatten(),
			service_tier: self.service_tier.clone(),
			max_tokens,
			max_completion_tokens,
			temperature: self.temperature,
			top_p: self.top_p,
			stop: self.stop.clone(),
			user: self.user.clone(),
			metadata: self.metadata.clone(),
		})
	}
}

/// Drop `function.strict` for dialects that reject OpenAI strict-mode schemas.
fn strip_strict(tool: &serde_json::Value) -> serde_json::Value {
	let mut tool = tool.clone();
	if let Some(function) = tool.get_mut("function").and_then(serde_json::Value::as_object_mut) {
		function.remove("strict");
	}
	tool
}

impl OpenAiCompatibleProvider {
	/// Send one request with the per-request credential and the exact auth-retry
	/// contract: refresh only on 401/403 + parsed `invalid_key`, then reissue the
	/// same body once. Transient HTTP failures (408/409/429/5xx) are retried
	/// inside this method, before a response is handed to a stream, so no chunk
	/// from a failed attempt can ever be observed (`fetchWithRetry` parity).
	/// The bearer key that actually produced the response is returned so
	/// diagnostics can be redacted with it (a callback may have rotated the
	/// configured key).
	async fn post_authenticated(
		&self,
		body: &OpenAiChatRequest,
		stream: bool,
	) -> Result<(reqwest::Response, String), ProviderError> {
		let mut force_refresh = false;
		let mut attempt = 0u32;
		let mut key = match &self.credential {
			Some(callback) => callback(false)
				.map_err(|err| self.auth_refresh_error(err, &self.api_key))?,
			None => self.api_key.clone(),
		};
		loop {
			let mut request = self.http.post(self.endpoint()).bearer_auth(&key);
			if stream {
				request = request.header(reqwest::header::ACCEPT, "text/event-stream");
			}
			let response = request
				.json(body)
				.send()
				.await
				.map_err(|err| self.network_error(err, &key))?;
			let status = response.status();
			if status.is_success() {
				return Ok((response, key));
			}
			let headers = response.headers().clone();
			let bounded = read_body_bounded(response, MAX_ERROR_BODY_BYTES)
				.await
				.map_err(|err| self.diagnostic_error(err, &key))?;
			if !force_refresh && matches!(status.as_u16(), 401 | 403) {
				if is_invalid_key_body(&bounded.text) {
					if let Some(callback) = &self.credential {
						key = callback(true)
							.map_err(|err| self.auth_refresh_error(err, &key))?;
						force_refresh = true;
						continue;
					}
				}
				return Err(ProviderError::HttpStatus(
					status.as_u16(),
					self.bounded_diagnostic(&bounded.text, &key, bounded.truncated),
				));
			}
			// A concurrency-admission 429 is a proxy shed, not backpressure:
			// the session layer owns its retry (`openai-http.ts` issue #8854).
			if attempt < self.retry.max_retries
				&& should_retry_response(&headers, status.as_u16())
				&& !is_concurrency_admission_rejection(&headers, &bounded.text)
				&& let Some(delay) = self.retry.delay(&headers, &bounded.text, attempt)
			{
				attempt += 1;
				sleep_before_retry(delay).await;
				continue;
			}
			return Err(ProviderError::HttpStatus(
				status.as_u16(),
				self.bounded_diagnostic(&bounded.text, &key, bounded.truncated),
			));
		}
	}

	/// Credential values that must not surface in diagnostics: the configured
	/// key and the bearer actually sent (a callback may have rotated it).
	///
	/// Longest first so a key that is a prefix of another cannot leave a
	/// fragment behind; deduplicated because both are often the same string.
	fn diagnostic_secrets(&self, active_key: &str) -> Vec<String> {
		let mut secrets = vec![self.api_key.clone(), active_key.to_string()];
		secrets.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
		secrets.dedup();
		secrets
	}

	/// Scrub free-form diagnostics with both credential redaction and the
	/// installed caller scrubber.
	fn redact_diagnostic(&self, text: &str, active_key: &str) -> String {
		redact_diagnostic_with(
			text,
			&self.diagnostic_secrets(active_key),
			self.diagnostic_redactor.as_ref(),
		)
	}

	/// Same as [`Self::redact_diagnostic`], bounded for an error body.
	fn bounded_diagnostic(&self, text: &str, active_key: &str, truncated: bool) -> String {
		bounded_redaction(
			text,
			&self.diagnostic_secrets(active_key),
			self.diagnostic_redactor.as_ref(),
			truncated,
		)
	}

	fn network_error(&self, err: reqwest::Error, active_key: &str) -> ProviderError {
		ProviderError::Network(self.redact_diagnostic(&err.to_string(), active_key))
	}

	fn auth_refresh_error(&self, err: String, active_key: &str) -> ProviderError {
		ProviderError::AuthRefresh(self.redact_diagnostic(&err, active_key))
	}

	/// Apply both scrubbers to any error text the transport can surface.
	fn diagnostic_error(&self, error: ProviderError, active_key: &str) -> ProviderError {
		match error {
			ProviderError::Network(text) => {
				ProviderError::Network(self.redact_diagnostic(&text, active_key))
			},
			ProviderError::Protocol(text) => {
				ProviderError::Protocol(self.redact_diagnostic(&text, active_key))
			},
			ProviderError::HttpStatus(code, text) => {
				ProviderError::HttpStatus(code, self.redact_diagnostic(&text, active_key))
			},
			ProviderError::AuthRefresh(text) => {
				ProviderError::AuthRefresh(self.redact_diagnostic(&text, active_key))
			},
			// Serde embeds the offending text in its own error and cannot be
			// rebuilt redacted; callers convert it to `Protocol` instead.
			other => other,
		}
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

		let body = read_body_bounded(response, MAX_RESPONSE_BODY_BYTES)
			.await
			.map_err(|err| self.diagnostic_error(err, &key))?;
		if body.truncated {
			return Err(ProviderError::Protocol(format!(
				"chat completion body exceeded {MAX_RESPONSE_BODY_BYTES} bytes"
			)));
		}
		let text = body.text;
		let parsed: OpenAiChatResponse = serde_json::from_str(&text).map_err(|err| {
			// Serde embeds offending response values in its message, so redact
			// both components. Each is redacted before it is bounded, so a
			// credential cut by the snippet cap cannot survive.
			ProviderError::Protocol(format!(
				"invalid chat completion body: {}; body: {}",
				self.redact_diagnostic(&err.to_string(), &key),
				self.bounded_diagnostic(&text, &key, false),
			))
		})?;
		let usage = parsed.usage.as_ref().map(normalize_usage);
		let choice = parsed.choices.into_iter().next().ok_or_else(|| {
			ProviderError::Protocol("chat completion contained no choices".to_string())
		})?;
		let finish_reason = choice.finish_reason.clone();
		let mut message = from_openai_response(choice.message);
		message.usage = usage;
		// A refusal already surfaced as a terminal error must not be downgraded
		// by the host's benign `finish_reason: "stop"`.
		if let Some(reason) = finish_reason.as_deref()
			&& message.stop_reason.is_none()
		{
			let (stop_reason, error_message) = map_openai_finish_reason(reason);
			message.stop_reason = Some(stop_reason);
			message.error_message = error_message;
		}
		if let Some(error_message) = message.error_message.as_mut() {
			*error_message = self.redact_diagnostic(error_message, &key);
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
		let secrets: std::sync::Arc<[String]> = self.diagnostic_secrets(&key).into();
		let redactor = self.diagnostic_redactor.clone();
		let inner = sse_stream(Box::pin(body));
		let chunks = futures::stream::unfold(
			(inner, String::new(), None::<ProviderError>, false),
			move |(mut inner, mut refusal_acc, pending, finished): (
				ChunkStream,
				String,
				Option<ProviderError>,
				bool,
			)| {
				let secrets = std::sync::Arc::clone(&secrets);
				let redactor = redactor.clone();
				async move {
					let secrets = secrets.as_ref();
					if finished {
						return None;
					}
					if let Some(error) = pending {
						return Some((Err(error), (inner, refusal_acc, None, true)));
					}
					match inner.next().await {
						Some(item) => {
							let mut chunk =
								match sanitize_stream_item(item, secrets, redactor.as_ref()) {
									Ok(chunk) => chunk,
									Err(error) => {
										if let Some(chunk) = flush_refusal(
											&mut refusal_acc,
											secrets,
											redactor.as_ref(),
										) {
											return Some((
												Ok(chunk),
												(inner, refusal_acc, Some(error), false),
											));
										}
										return Some((
											Err(error),
											(inner, refusal_acc, None, true),
										));
									},
								};
							if let Some(fragment) = chunk.refusal.take() {
								if refusal_acc
									.len()
									.saturating_add(fragment.len())
									> MAX_ERROR_BODY_BYTES
								{
									refusal_acc.clear();
									return Some((
										Err(ProviderError::Protocol(
											"streamed refusal exceeded maximum size".to_string(),
										)),
										(inner, refusal_acc, None, true),
									));
								}
								refusal_acc.push_str(&fragment);
							}
							// A refusal split across deltas only matches a secret
							// once joined, so scrub the assembled text on the
							// terminal chunk (`finish_reason` or `[DONE]`).
							if (chunk.stop_reason.is_some() || chunk.is_done)
								&& !refusal_acc.is_empty()
							{
								chunk.refusal = Some(redact_diagnostic_with(
									&std::mem::take(&mut refusal_acc),
									secrets,
									redactor.as_ref(),
								));
							}
							Some((Ok(chunk), (inner, refusal_acc, None, false)))
						},
						None => flush_refusal(&mut refusal_acc, secrets, redactor.as_ref())
							.map(|chunk| (Ok(chunk), (inner, refusal_acc, None, false))),
					}
				}
			},
		)
		.boxed();
		Ok(super::thinking_loop::guard_chunk_stream(chunks, &self.model))
	}
}

/// Scrub every provider-supplied string a stream can surface: transport and
/// protocol errors, plus a terminal chunk's `error_message`.
fn sanitize_stream_item(
	item: Result<StreamChunk, ProviderError>,
	secrets: &[String],
	redactor: Option<&DiagnosticRedactor>,
) -> Result<StreamChunk, ProviderError> {
	let scrubbed = |text: &str| redact_diagnostic_with(text, secrets, redactor);
	match item {
		Ok(mut chunk) => {
			if let Some(message) = chunk.error_message.as_mut() {
				*message = scrubbed(message);
			}
			if let Some(refusal) = chunk.refusal.as_mut() {
				*refusal = scrubbed(refusal);
			}
			Ok(chunk)
		},
		Err(error) => Err(match error {
			ProviderError::Network(text) => ProviderError::Network(scrubbed(&text)),
			ProviderError::Protocol(text) => ProviderError::Protocol(scrubbed(&text)),
			// A serde error carries the offending text in its message and
			// cannot be rebuilt redacted, so surface it as protocol.
			ProviderError::Serialization(err) => {
				ProviderError::Protocol(scrubbed(&err.to_string()))
			},
			ProviderError::HttpStatus(code, text) => {
				ProviderError::HttpStatus(code, scrubbed(&text))
			},
			ProviderError::AuthRefresh(text) => ProviderError::AuthRefresh(scrubbed(&text)),
			other => other,
		}),
	}
}

/// Emit buffered refusal fragments as one already-scrubbed chunk. Joining
/// before redaction catches a secret split across separate SSE deltas.
fn flush_refusal(
	refusal_acc: &mut String,
	secrets: &[String],
	redactor: Option<&DiagnosticRedactor>,
) -> Option<StreamChunk> {
	if refusal_acc.is_empty() {
		return None;
	}
	let refusal = redact_diagnostic_with(&std::mem::take(refusal_acc), secrets, redactor);
	Some(StreamChunk {
		refusal: Some(refusal),
		content: None,
		reasoning: None,
		tool_call_deltas: Vec::new(),
		usage: None,
		is_done: false,
		disabled_features: None,
		content_index: None,
		signature: None,
		redacted: None,
		stop_reason: None,
		error_message: None,
	})
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

fn network_error(err: reqwest::Error) -> ProviderError {
	ProviderError::Network(err.to_string())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::provider::timeout::{RequestTimeoutConfig, TimeoutProvider};
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
	fn diagnostic_secrets_are_longest_first_and_deduplicated() {
		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "sk-short", "m")
			.expect("build");
		assert_eq!(
			provider.diagnostic_secrets("sk-short-long"),
			vec!["sk-short-long".to_string(), "sk-short".to_string()]
		);
		// Replacing the shorter key first would leave "-long" behind.
		let secrets = provider.diagnostic_secrets("sk-short-long");
		assert_eq!(redact_with("sk-short-long value", &secrets), "[redacted] value");
		assert_eq!(provider.diagnostic_secrets("sk-short"), vec!["sk-short".to_string()]);
	}

	#[test]
	fn stream_chunk_error_messages_use_both_scrubbers() {
		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "sk-key", "m")
			.expect("build")
			.with_diagnostic_redactor(Some(std::sync::Arc::new(|text: &str| {
				text.replace("secret-token", "[scrubbed]")
			})));
		let secrets = provider.diagnostic_secrets("sk-key");
		let chunk: StreamChunk = serde_json::from_value(serde_json::json!({
			"is_done": true,
			"error_message": "boom sk-key secret-token",
		}))
		.expect("chunk");
		let sanitized =
			sanitize_stream_item(Ok(chunk), &secrets, provider.diagnostic_redactor.as_ref())
				.expect("chunk stays ok");
		assert_eq!(sanitized.error_message.as_deref(), Some("boom [redacted] [scrubbed]"));
	}

	#[test]
	fn network_and_auth_refresh_errors_use_both_scrubbers() {
		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "sk-key", "m")
			.expect("build")
			.with_diagnostic_redactor(Some(std::sync::Arc::new(|text: &str| {
				text.replace("secret-token", "[scrubbed]")
			})));
		let secrets = provider.diagnostic_secrets("sk-key");
		let network = sanitize_stream_item(
			Err(ProviderError::Network("boom sk-key secret-token".to_string())),
			&secrets,
			provider.diagnostic_redactor.as_ref(),
		)
		.expect_err("network error");
		match network {
			ProviderError::Network(text) => assert_eq!(text, "boom [redacted] [scrubbed]"),
			other => panic!("expected network error, got {other:?}"),
		}
		match provider.auth_refresh_error("refresh sk-key secret-token".to_string(), "sk-key") {
			ProviderError::AuthRefresh(text) => assert_eq!(text, "refresh [redacted] [scrubbed]"),
			other => panic!("expected auth refresh error, got {other:?}"),
		}
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

	/// Serve one request per entry, then close; `retry-after-ms: 0` keeps
	/// retry tests free of real backoff sleeps.
	async fn serve_sequence(responses: Vec<(u16, Vec<u8>)>) -> String {
		let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
		let addr = listener.local_addr().expect("addr");
		tokio::spawn(async move {
			for (status, body) in responses {
				let Ok((mut socket, _)) = listener.accept().await else {
					return;
				};
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
					"HTTP/1.1 {status} TEST\r\ncontent-type: application/json\r\nretry-after-ms: 0\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
					body.len()
				);
				socket.write_all(head.as_bytes()).await.expect("write head");
				let _ = socket.write_all(&body).await;
				let _ = socket.shutdown().await;
			}
		});
		format!("http://{addr}/v1")
	}

	/// Serve exactly one HTTP/1.1 request with `status`/`body`, then close.
	async fn serve_once(status: u16, body: Vec<u8>) -> String {
		serve_sequence(vec![(status, body)]).await
	}

	#[tokio::test]
	async fn http_status_diagnostics_redact_the_active_key() {
		let base_url = serve_once(500, br#"{"error":{"message":"bad key sk-secret-123"}}"#.to_vec())
			.await;
		let provider = OpenAiCompatibleProvider::new(base_url, "sk-secret-123", "model-x")
			.expect("build")
			.with_max_retries(0);
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
			.with_credential_callback(Some(callback))
			.with_max_retries(0);
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

	#[tokio::test]
	async fn retries_transient_429_before_returning_the_response() {
		let success = br#"{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}"#.to_vec();
		let base_url = serve_sequence(vec![
			(429, br#"{"error":"slow down"}"#.to_vec()),
			(200, success),
		])
		.await;
		let provider = OpenAiCompatibleProvider::new(base_url, "key", "model-x")
			.expect("build")
			.with_max_retries(1);
		let message = provider
			.complete(&[], &[], &ToolChoice::Auto)
			.await
			.expect("retry succeeds");
		assert_eq!(message.content, "ok");
	}

	#[tokio::test]
	async fn exhausted_retries_surface_the_last_http_status() {
		let base_url = serve_sequence(vec![
			(503, b"busy".to_vec()),
			(503, b"still busy".to_vec()),
		])
		.await;
		let provider = OpenAiCompatibleProvider::new(base_url, "key", "model-x")
			.expect("build")
			.with_max_retries(1);
		let err = provider.complete(&[], &[], &ToolChoice::Auto).await.unwrap_err();
		match err {
			ProviderError::HttpStatus(503, text) => assert!(text.contains("still busy"), "{text}"),
			other => panic!("expected HttpStatus(503), got {other:?}"),
		}
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

	#[test]
	fn compat_builders_shape_the_chat_request() {
		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "key", "model-x")
			.expect("build")
			.with_developer_role(false)
			.with_effort(Some("high".to_string()))
			.with_effort_map(BTreeMap::from([(
				"high".to_string(),
				"medium".to_string(),
			)]))
			.with_max_tokens_field(MaxTokensField::MaxTokens)
			.with_max_tokens(Some(1234))
			.with_strict_mode(false)
			.with_store(false)
			.with_temperature(Some(0.2))
			.with_top_p(Some(0.9))
			.with_stop(Some(serde_json::json!(["END"])))
			.with_user(Some("user-1".to_string()))
			.with_metadata(Some(serde_json::json!({"trace": "t1"})));
		let tools = vec![serde_json::json!({
			"type": "function",
			"function": {
				"name": "read",
				"description": "Read a file",
				"parameters": {"type": "object"},
				"strict": true
			}
		})];
		let request = provider
			.request(&[Message::developer("policy")], &tools, &ToolChoice::Auto, false)
			.expect("request");
		let payload = serde_json::to_value(request).expect("serialize");
		assert_eq!(payload["messages"][0]["role"], "system", "developer folds: {payload}");
		assert_eq!(payload["reasoning_effort"], "medium");
		assert_eq!(payload["max_tokens"], 1234);
		assert!(payload.get("max_completion_tokens").is_none());
		assert!(payload.get("store").is_none(), "store omitted: {payload}");
		assert!(payload["tools"][0]["function"].get("strict").is_none());
		assert_eq!(payload["temperature"], 0.2);
		assert_eq!(payload["top_p"], 0.9);
		assert_eq!(payload["stop"], serde_json::json!(["END"]));
		assert_eq!(payload["user"], "user-1");
		assert_eq!(payload["metadata"], serde_json::json!({"trace": "t1"}));
	}

	#[test]
	fn compat_defaults_send_store_and_keep_strict_schemas() {
		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "key", "model-x")
			.expect("build");
		let tools = vec![serde_json::json!({
			"type": "function",
			"function": {
				"name": "read",
				"description": "Read a file",
				"parameters": {"type": "object"},
				"strict": true
			}
		})];
		let payload = serde_json::to_value(
			provider
				.request(&[], &tools, &ToolChoice::Auto, true)
				.expect("request"),
		)
		.expect("serialize");
		assert_eq!(payload["store"], true);
		assert_eq!(payload["tools"][0]["function"]["strict"], true);
	}

	#[test]
	fn max_completion_tokens_and_unsupported_effort_shape_the_body() {
		let provider = OpenAiCompatibleProvider::new("https://example.test/v1", "key", "model-x")
			.expect("build")
			.with_effort(Some("high".to_string()))
			.with_force_reasoning_off(true)
			.with_reasoning_effort_support(false)
			.with_max_tokens_field(MaxTokensField::MaxCompletionTokens)
			.with_max_tokens(Some(7));
		let payload = serde_json::to_value(
			provider
				.request(&[], &[], &ToolChoice::Auto, false)
				.expect("request"),
		)
		.expect("serialize");
		assert!(
			payload.get("reasoning_effort").is_none(),
			"unsupported effort is omitted: {payload}"
		);
		assert_eq!(payload["max_completion_tokens"], 7);
		assert!(payload.get("max_tokens").is_none());
	}

	#[tokio::test]
	async fn complete_preserves_refusal_error_over_stop_finish_reason() {
		let body = br#"{"choices":[{"message":{"role":"assistant","content":null,"refusal":"blocked by safety"},"finish_reason":"stop"}]}"#.to_vec();
		let base_url = serve_once(200, body).await;
		let provider = OpenAiCompatibleProvider::new(base_url, "key", "model-x").expect("build");
		let message = provider
			.complete(&[], &[], &ToolChoice::Auto)
			.await
			.expect("complete");
		assert!(message.content.is_empty());
		assert_eq!(message.stop_reason, Some(StopReason::Error));
		assert_eq!(message.error_message.as_deref(), Some("blocked by safety"));
	}

	#[tokio::test]
	async fn complete_scrubs_secrets_from_refusal_and_unknown_finish_reason() {
		let cases: [(&[u8], &str); 2] = [
			(
				br#"{"choices":[{"message":{"role":"assistant","content":null,"refusal":"blocked sk-key"},"finish_reason":"stop"}]}"#,
				"blocked [redacted]",
			),
			(
				br#"{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"weird sk-key"}]}"#,
				"Provider finish_reason: weird [redacted]",
			),
		];
		for (body, expected) in cases {
			let base_url = serve_once(200, body.to_vec()).await;
			let provider =
				OpenAiCompatibleProvider::new(base_url, "sk-key", "model-x").expect("build");
			let message = provider
				.complete(&[], &[], &ToolChoice::Auto)
				.await
				.expect("complete");
			assert_eq!(message.error_message.as_deref(), Some(expected));
		}
	}

	#[tokio::test]
	async fn stream_scrubs_secrets_from_refusal_deltas() {
		let sse = concat!(
			"data: {\"choices\":[{\"delta\":{\"refusal\":\"blocked sk-key\"}}]}\n\n",
			"data: [DONE]\n\n",
		)
		.as_bytes()
		.to_vec();
		let base_url = serve_once(200, sse).await;
		let provider = OpenAiCompatibleProvider::new(base_url, "sk-key", "model-x").expect("build");
		let mut stream = provider.stream(&[], &[], &ToolChoice::Auto).await.expect("stream");
		let mut chunks = Vec::new();
		while let Some(item) = stream.next().await {
			chunks.push(item.expect("chunk"));
		}
		let done = chunks.iter().find(|chunk| chunk.is_done).expect("done chunk");
		assert_eq!(done.refusal.as_deref(), Some("blocked [redacted]"));
		let refusals: String = chunks.iter().filter_map(|chunk| chunk.refusal.as_deref()).collect();
		assert_eq!(refusals, "blocked [redacted]");
		assert!(!refusals.contains("sk-key"));
	}

	#[tokio::test]
	async fn oversized_streamed_refusal_fails_closed() {
		let fragment = "x".repeat(MAX_ERROR_BODY_BYTES / 2 + 1);
		let sse = format!(
			"data: {{\"choices\":[{{\"delta\":{{\"refusal\":\"{fragment}\"}}}}]}}\n\n\
			 data: {{\"choices\":[{{\"delta\":{{\"refusal\":\"{fragment}\"}}}}]}}\n\n"
		);
		let base_url = serve_once(200, sse.into_bytes()).await;
		let provider = OpenAiCompatibleProvider::new(base_url, "key", "model-x").expect("build");
		let mut stream = provider.stream(&[], &[], &ToolChoice::Auto).await.expect("stream");
		let error = loop {
			match stream.next().await {
				Some(Err(error)) => break error,
				Some(Ok(_)) => {},
				None => panic!("oversized refusal must fail"),
			}
		};
		assert!(matches!(
			error,
			ProviderError::Protocol(text) if text == "streamed refusal exceeded maximum size"
		));
		assert!(stream.next().await.is_none());
	}

	#[tokio::test]
	async fn split_refusal_secret_is_scrubbed_after_joining_across_chunks() {
		let sse = concat!(
			"data: {\"choices\":[{\"delta\":{\"refusal\":\"sk-\"}}]}\n\n",
			"data: {\"choices\":[{\"delta\":{\"refusal\":\"key\"}}]}\n\n",
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
			"data: [DONE]\n\n",
		)
		.as_bytes()
		.to_vec();
		for via_timeout in [false, true] {
			let base_url = serve_once(200, sse.clone()).await;
			let inner: std::sync::Arc<dyn ModelProvider> = std::sync::Arc::new(
				OpenAiCompatibleProvider::new(base_url, "sk-key", "model-x").expect("build"),
			);
			let mut stream = if via_timeout {
				let timeout = TimeoutProvider::new(inner, RequestTimeoutConfig::disabled());
				timeout
					.stream(&[], &[], &ToolChoice::Auto)
					.await
					.expect("stream")
			} else {
				inner.stream(&[], &[], &ToolChoice::Auto).await.expect("stream")
			};
			let mut assembler = StreamAssembler::new();
			while let Some(item) = stream.next().await {
				assembler.push(&item.expect("chunk")).expect("push");
			}
			let message = assembler.finish().expect("finish");
			assert_eq!(
				message.error_message.as_deref(),
				Some("[redacted]"),
				"via_timeout={via_timeout}"
			);
		}
	}

	#[tokio::test]
	async fn refusal_secret_is_scrubbed_on_eof_flush_without_terminal_reason() {
		let sse = concat!(
			"data: {\"choices\":[{\"delta\":{\"refusal\":\"sk-\"}}]}\n\n",
			"data: {\"choices\":[{\"delta\":{\"refusal\":\"key\"}}]}\n\n",
		)
		.as_bytes()
		.to_vec();
		let base_url = serve_once(200, sse).await;
		let provider = OpenAiCompatibleProvider::new(base_url, "sk-key", "model-x").expect("build");
		let mut stream = provider.stream(&[], &[], &ToolChoice::Auto).await.expect("stream");
		let mut assembler = StreamAssembler::new();
		while let Some(item) = stream.next().await {
			assembler.push(&item.expect("chunk")).expect("push");
		}
		let message = assembler.finish().expect("finish");
		assert_eq!(message.error_message.as_deref(), Some("[redacted]"));
	}

	#[tokio::test]
	async fn stream_preserves_refusal_error_after_trailing_stop_chunk() {
		let sse = concat!(
			"data: {\"choices\":[{\"delta\":{\"refusal\":\"blocked by safety\"}}]}\n\n",
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
			"data: [DONE]\n\n",
		)
		.as_bytes()
		.to_vec();
		let base_url = serve_once(200, sse).await;
		let provider = OpenAiCompatibleProvider::new(base_url, "key", "model-x").expect("build");
		let mut stream = provider.stream(&[], &[], &ToolChoice::Auto).await.expect("stream");
		let mut assembler = StreamAssembler::new();
		while let Some(item) = stream.next().await {
			assembler.push(&item.expect("chunk")).expect("push");
		}
		let message = assembler.finish().expect("finish");
		assert!(message.content.is_empty());
		assert_eq!(message.stop_reason, Some(StopReason::Error));
		assert_eq!(message.error_message.as_deref(), Some("blocked by safety"));
	}

	#[tokio::test]
	async fn stream_accumulates_split_refusal_deltas() {
		let sse = concat!(
			"data: {\"choices\":[{\"delta\":{\"refusal\":\"I cannot\"}}]}\n\n",
			"data: {\"choices\":[{\"delta\":{\"refusal\":\" \"}}]}\n\n",
			"data: {\"choices\":[{\"delta\":{\"refusal\":\"help with that\"}}]}\n\n",
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
			"data: [DONE]\n\n",
		)
		.as_bytes()
		.to_vec();
		let base_url = serve_once(200, sse).await;
		let provider = OpenAiCompatibleProvider::new(base_url, "key", "model-x").expect("build");
		let mut stream = provider.stream(&[], &[], &ToolChoice::Auto).await.expect("stream");
		let mut assembler = StreamAssembler::new();
		while let Some(item) = stream.next().await {
			assembler.push(&item.expect("chunk")).expect("push");
		}
		let message = assembler.finish().expect("finish");
		assert!(message.content.is_empty());
		assert_eq!(message.stop_reason, Some(StopReason::Error));
		assert_eq!(message.error_message.as_deref(), Some("I cannot help with that"));
	}

	#[tokio::test]
	async fn stream_runs_tool_calls_when_refusal_delta_precedes_them() {
		let sse = concat!(
			"data: {\"choices\":[{\"delta\":{\"refusal\":\"nope\"}}]}\n\n",
			"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"read\",\"arguments\":\"{}\"}}]}}]}\n\n",
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
			"data: [DONE]\n\n",
		)
		.as_bytes()
		.to_vec();
		let base_url = serve_once(200, sse).await;
		let provider = OpenAiCompatibleProvider::new(base_url, "key", "model-x").expect("build");
		let mut stream = provider.stream(&[], &[], &ToolChoice::Auto).await.expect("stream");
		let mut assembler = StreamAssembler::new();
		while let Some(item) = stream.next().await {
			assembler.push(&item.expect("chunk")).expect("push");
		}
		let message = assembler.finish().expect("finish");
		assert_eq!(message.stop_reason, Some(StopReason::ToolUse));
		assert!(message.error_message.is_none());
		assert_eq!(message.tool_calls.len(), 1);
		assert_eq!(message.tool_calls[0].name, "read");
	}
}
