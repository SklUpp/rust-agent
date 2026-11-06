//! Anthropic Messages API (`anthropic-messages`) provider.
//!
//! Port of the wire/stream contract in
//! `packages/ai/src/providers/anthropic.ts`, `anthropic-wire.ts` and
//! `anthropic-client.ts`. The adapter owns HTTP, request translation and the
//! SSE event state machine; it deliberately omits OAuth account flows, Claude
//! Code fingerprint/billing headers and model-catalog compatibility switches
//! (see the module notes at the end).
//!
//! Authentication follows the upstream split:
//! - official endpoint + API key: `x-api-key`;
//! - non-official endpoint + API key: `Authorization: Bearer`;
//! - OAuth token (`sk-ant-oat`): `Authorization: Bearer` and `?beta=true`,
//!   with the OAuth beta profile. The credential is never placed in a body or
//!   an error message.

use std::{
	sync::atomic::{AtomicBool, Ordering},
	collections::{HashMap, VecDeque},
	fmt::Display,
	time::Duration,
};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
	provider::{ChunkStream, ModelProvider, ProviderError, StreamChunk, ToolCallDelta, ToolChoice, redact_with},
	state::{
		CacheTtlUsage, ContentPart, Message, Role, ServerToolUsage, StopReason, TokenUsage, ToolCall,
	},
};

/// Anthropic API version header value.
pub const ANTHROPIC_VERSION: &str = "2023-06-01";
/// Claude Code OAuth beta profile version marker.
pub const ANTHROPIC_OAUTH_BETA: &str = "oauth-2025-04-20";
/// Context-management beta required by `clear_thinking_20251015`.
pub const ANTHROPIC_CONTEXT_MANAGEMENT_BETA: &str = "context-management-2025-06-27";
/// Fast mode (`serviceTier: "priority"` on the direct Anthropic provider).
const FAST_MODE_BETA: &str = "fast-mode-2026-02-01";

/// Interleaved-thinking beta for non-adaptive models.
pub const ANTHROPIC_INTERLEAVED_THINKING_BETA: &str = "interleaved-thinking-2025-05-14";
/// Adaptive-thinking effort beta.
pub const ANTHROPIC_EFFORT_BETA: &str = "effort-2025-11-24";
/// Default `max_tokens` when the caller does not configure a model ceiling.
const DEFAULT_MAX_TOKENS: u32 = 8192;
/// TCP/TLS connect budget; the caller owns stream cancellation.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum characters of an error response body kept for diagnostics.
const ERROR_BODY_CHARS: usize = 2048;
/// Hard ceiling on bytes read from an error body before diagnostics.
const ERROR_BODY_BYTES: usize = 64 * 1024;
/// Hard ceiling on a non-streaming success body. Overflow is an error: a
/// truncated success would silently hand back an incomplete turn.
const MAX_RESPONSE_BODY_BYTES: usize = 8 * 1024 * 1024;
/// Hard ceiling on one unterminated SSE line.
const MAX_SSE_LINE_BYTES: usize = 1024 * 1024;
/// Hard ceiling on the accumulated `data` payload of one SSE event.
const MAX_SSE_EVENT_BYTES: usize = 8 * 1024 * 1024;
/// Prefix Anthropic applies to custom OAuth tool names.
const CLAUDE_TOOL_PREFIX: char = '_';
/// Built-in Anthropic tool names that must not be prefixed.
const BUILTIN_TOOL_NAMES: [&str; 4] = ["web_search", "code_execution", "text_editor", "computer"];
/// Upstream placeholder for images sent to a text-only endpoint.
const NON_VISION_IMAGE_PLACEHOLDER: &str = "[image omitted: model does not support vision]";
/// Upstream text used when an error tool result has no output.
const EMPTY_ERROR_TOOL_RESULT_TEXT: &str = "Tool failed with no output.";
/// Claude Code runtime/SDK fingerprint constants (`claude-code-fingerprint.ts`).
const CLAUDE_CODE_VERSION: &str = "2.1.246";
const CLAUDE_CODE_SDK_VERSION: &str = "0.112.1";
const CLAUDE_CODE_SYSTEM_INSTRUCTION: &str =
	"You are a Claude agent, built on Anthropic's Claude Agent SDK.";
const CLAUDE_CODE_USER_AGENT: &str = "claude-cli/2.1.246 (external, claude-desktop)";
/// CCH attestation seed (`anthropic.ts` `CCH_SEED`).
const CCH_SEED: u64 = 0x4d65_9218_e32a_3268;
/// Output headroom kept above an enabled thinking budget (`OUTPUT_FALLBACK_BUFFER`).
const OUTPUT_FALLBACK_BUFFER: u32 = 4000;
/// Server-directed retry wait cap (TS `maxRetryDelayMs` default).
const MAX_RETRY_DELAY_MS: u64 = 60_000;

/// Thinking request shape (TS `ThinkingConfigParam`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicThinkingConfig {
	Enabled {
		budget_tokens: u32,
		#[serde(skip_serializing_if = "Option::is_none")]
		display: Option<String>,
	},
	Adaptive {
		#[serde(skip_serializing_if = "Option::is_none")]
		display: Option<String>,
	},
	Disabled,
}

impl AnthropicThinkingConfig {
	fn keeps_context(&self) -> bool {
		!matches!(self, Self::Disabled)
	}

	fn is_adaptive(&self) -> bool {
		matches!(self, Self::Adaptive { .. })
	}
}

/// `output_config` request field. Only the adaptive-effort pin is modeled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AnthropicOutputConfig {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub effort: Option<String>,
}

/// `context_management` request field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AnthropicContextManagement {
	pub edits: Vec<AnthropicContextEdit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AnthropicContextEdit {
	#[serde(rename = "type")]
	pub kind: String,
	pub keep: String,
}

/// Anthropic `tool_choice` object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicToolChoice {
	Auto,
	Any,
	Tool { name: String },
	None,
}

/// System prompt block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicTextBlock {
	Text { text: String },
}

/// Ephemeral prefix-cache breakpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AnthropicCacheControl {
	#[serde(rename = "type")]
	pub kind: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ttl: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub scope: Option<String>,
}

/// Image source accepted by `image` content blocks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicImageSource {
	Base64 { media_type: String, data: String },
	Url { url: String },
	File { file_id: String },
}

/// Request content: a plain string or ordered blocks.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum AnthropicContent {
	Text(String),
	Blocks(Vec<AnthropicBlock>),
}

/// Request content block.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicBlock {
	Text {
		text: String,
		#[serde(skip_serializing_if = "Option::is_none")]
		cache_control: Option<AnthropicCacheControl>,
	},
	Image {
		source: AnthropicImageSource,
		#[serde(skip_serializing_if = "Option::is_none")]
		cache_control: Option<AnthropicCacheControl>,
	},
	ToolUse {
		id: String,
		name: String,
		input: serde_json::Value,
		#[serde(skip_serializing_if = "Option::is_none")]
		cache_control: Option<AnthropicCacheControl>,
	},
	ToolResult {
		tool_use_id: String,
		content: AnthropicContent,
		#[serde(skip_serializing_if = "Option::is_none")]
		is_error: Option<bool>,
		#[serde(skip_serializing_if = "Option::is_none")]
		id: Option<String>,
		#[serde(skip_serializing_if = "Option::is_none")]
		cache_control: Option<AnthropicCacheControl>,
	},
	Thinking {
		thinking: String,
		signature: String,
	},
	RedactedThinking {
		data: String,
	},
}

/// One conversation turn on the wire.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AnthropicMessage {
	pub role: String,
	pub content: AnthropicContent,
}

/// Tool declaration sent to the Messages API.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AnthropicTool {
	pub name: String,
	pub description: String,
	pub input_schema: serde_json::Value,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub eager_input_streaming: Option<bool>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub strict: Option<bool>,
}

/// Request body for `POST /v1/messages`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AnthropicMessagesRequest {
	pub model: String,
	pub messages: Vec<AnthropicMessage>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub system: Option<Vec<AnthropicTextBlock>>,
	pub max_tokens: u32,
	#[serde(skip_serializing_if = "Vec::is_empty")]
	pub tools: Vec<AnthropicTool>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub tool_choice: Option<AnthropicToolChoice>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub thinking: Option<AnthropicThinkingConfig>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub context_management: Option<AnthropicContextManagement>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub output_config: Option<AnthropicOutputConfig>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub temperature: Option<f64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub top_p: Option<f64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub top_k: Option<u32>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub stop_sequences: Option<Vec<String>>,
	/// Anthropic fast mode (`serviceTier: "priority"`).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub speed: Option<String>,
	pub stream: bool,
}

/// Provider for the Anthropic Messages API.
///
/// Base URL, credential and model come from the caller; no vendor default or
/// model id is compiled in beyond the protocol constants.
pub struct AnthropicMessagesProvider {
	http: reqwest::Client,
	base_url: String,
	api_key: String,
	/// Per-request credential callback; `None` uses `api_key`.
	credential: Option<crate::provider::CredentialCallback>,
	model: String,
	/// Model output ceiling; also the default requested `max_tokens`.
	max_tokens: u32,
	/// Caller-requested `max_tokens`; thinking may raise it up to `max_tokens`.
	request_max_tokens: Option<u32>,
	thinking: Option<AnthropicThinkingConfig>,
	output_effort: Option<String>,
	extra_betas: Vec<String>,
	interleaved_thinking: bool,
	/// `None` sniffs `sk-ant-oat` from the credential, matching TS.
	is_oauth: Option<bool>,
	supports_images: bool,
	/// Replay unsigned thinking as a `thinking` block instead of demoting it to
	/// text. Signed blocks always replay with their signature; official
	/// Anthropic endpoints reject *unsigned* thinking blocks, hence the
	/// conservative default for the signature-less case.
	replay_unsigned_thinking: bool,
	supports_sampling_params: bool,
	adaptive_only_thinking: bool,
	supports_forced_tool_choice: bool,
	temperature: Option<f64>,
	top_p: Option<f64>,
	top_k: Option<u32>,
	stop_sequences: Vec<String>,
	cache_retention: CacheRetention,
	supports_long_cache_retention: bool,
	max_retries: u32,
	first_event_timeout: Option<Duration>,
	idle_timeout: Option<Duration>,
	claude_code_session_id: Option<String>,
	custom_headers: Vec<(String, String)>,
	allow_anthropic_header_overrides: bool,
	provider_id: Option<String>,
	/// TS `options.serviceTier`; only `priority` is realized on direct Anthropic.
	service_tier: Option<String>,
	/// Per-request fast-mode downgrade latch: set once the server rejects
	/// `speed`, so the retried request omits it and the terminal chunk reports
	/// the drop. Never sticky across requests (the session owns that).
	fast_mode_disabled: AtomicBool,
	requires_tool_result_id: bool,
	escape_builtin_tool_names: bool,
	supports_mid_conversation_system: bool,
	cache_scope: Option<String>,
	strict_tools: bool,
	disable_strict_tools: bool,
}

/// Prompt-cache retention policy (TS `CacheRetention`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheRetention {
	/// No `cache_control` breakpoints.
	#[default]
	None,
	Short,
	Long,
}

/// Catalog-dispatch configuration for [`AnthropicMessagesProvider`].
///
/// Every field maps to a `Model<"anthropic-messages">` / caller option; the
/// parent registry can fill it from the catalog and call [`from_config`].
#[derive(Debug, Clone)]
pub struct AnthropicConfig {
	pub base_url: String,
	pub api_key: String,
	pub model: String,
	pub max_tokens: Option<u32>,
	pub request_max_tokens: Option<u32>,
	pub thinking: Option<AnthropicThinkingConfig>,
	pub output_effort: Option<String>,
	pub extra_betas: Vec<String>,
	pub interleaved_thinking: bool,
	pub is_oauth: Option<bool>,
	pub supports_images: bool,
	pub replay_unsigned_thinking: bool,
	pub supports_sampling_params: bool,
	pub adaptive_only_thinking: bool,
	pub supports_forced_tool_choice: bool,
	pub temperature: Option<f64>,
	pub top_p: Option<f64>,
	pub top_k: Option<u32>,
	pub stop_sequences: Vec<String>,
	pub cache_retention: CacheRetention,
	pub supports_long_cache_retention: bool,
	pub max_retries: u32,
	pub first_event_timeout: Option<Duration>,
	pub idle_timeout: Option<Duration>,
	pub claude_code_session_id: Option<String>,
	/// Caller/model headers; enforced credential/beta headers win as in TS.
	pub custom_headers: Vec<(String, String)>,
	/// OAuth-only opt-in for anthropic-version/beta/x-app overrides.
	pub allow_anthropic_header_overrides: bool,
	/// Configured provider id (gateway/proxy); defaults to `anthropic`.
	pub provider_id: Option<String>,
	pub requires_tool_result_id: bool,
	pub escape_builtin_tool_names: bool,
	pub supports_mid_conversation_system: bool,
	pub cache_scope: Option<String>,
	pub strict_tools: bool,
	pub disable_strict_tools: bool,
}

impl AnthropicConfig {
	pub fn new(
		base_url: impl Into<String>,
		api_key: impl Into<String>,
		model: impl Into<String>,
	) -> Self {
		Self {
			base_url: base_url.into(),
			api_key: api_key.into(),
			model: model.into(),
			max_tokens: None,
			request_max_tokens: None,
			thinking: None,
			output_effort: None,
			extra_betas: Vec::new(),
			interleaved_thinking: true,
			is_oauth: None,
			supports_images: true,
			replay_unsigned_thinking: false,
			supports_sampling_params: true,
			adaptive_only_thinking: false,
			supports_forced_tool_choice: true,
			temperature: None,
			top_p: None,
			top_k: None,
			stop_sequences: Vec::new(),
			cache_retention: CacheRetention::None,
			supports_long_cache_retention: false,
			max_retries: 2,
			first_event_timeout: None,
			idle_timeout: None,
			claude_code_session_id: None,
			custom_headers: Vec::new(),
			allow_anthropic_header_overrides: false,
			provider_id: None,
			requires_tool_result_id: false,
			escape_builtin_tool_names: false,
			supports_mid_conversation_system: false,
			cache_scope: None,
			strict_tools: false,
			disable_strict_tools: false,
		}
	}
}

impl AnthropicMessagesProvider {
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
		let http = reqwest::Client::builder()
			.connect_timeout(CONNECT_TIMEOUT)
			// Never replay credentials across an unintended redirect.
			.redirect(reqwest::redirect::Policy::none())
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
			credential: None,
			model: model.into(),
			max_tokens: DEFAULT_MAX_TOKENS,
			thinking: None,
			output_effort: None,
			extra_betas: Vec::new(),
			interleaved_thinking: true,
			is_oauth: None,
			supports_images: true,
			replay_unsigned_thinking: false,
			supports_sampling_params: true,
			adaptive_only_thinking: false,
			supports_forced_tool_choice: true,
			temperature: None,
			top_p: None,
			top_k: None,
			stop_sequences: Vec::new(),
			cache_retention: CacheRetention::None,
			supports_long_cache_retention: false,
			max_retries: 2,
			first_event_timeout: None,
			idle_timeout: None,
			claude_code_session_id: None,
			custom_headers: Vec::new(),
			allow_anthropic_header_overrides: false,
			provider_id: None,
			requires_tool_result_id: false,
			escape_builtin_tool_names: false,
			supports_mid_conversation_system: false,
			cache_scope: None,
			strict_tools: false,
			disable_strict_tools: false,
			request_max_tokens: None,
			service_tier: None,
			fast_mode_disabled: AtomicBool::new(false),
		}
	}

	/// Build from a catalog-dispatch [`AnthropicConfig`].
	pub fn from_config(config: AnthropicConfig) -> Result<Self, ProviderError> {
		let mut provider = Self::new(config.base_url, config.api_key, config.model)?;
		if let Some(max_tokens) = config.max_tokens {
			provider = provider.with_max_tokens(max_tokens);
		}
		Ok(provider
			.with_request_max_tokens(config.request_max_tokens)
			.with_thinking(config.thinking)
			.with_output_effort(config.output_effort)
			.with_extra_betas(config.extra_betas)
			.with_interleaved_thinking(config.interleaved_thinking)
			.with_oauth(config.is_oauth)
			.with_images(config.supports_images)
			.with_replay_unsigned_thinking(config.replay_unsigned_thinking)
			.with_sampling_params(config.supports_sampling_params)
			.with_adaptive_only_thinking(config.adaptive_only_thinking)
			.with_forced_tool_choice(config.supports_forced_tool_choice)
			.with_temperature(config.temperature)
			.with_top_p(config.top_p)
			.with_top_k(config.top_k)
			.with_stop_sequences(config.stop_sequences)
			.with_cache_retention(
				config.cache_retention,
				config.supports_long_cache_retention,
			)
			.with_max_retries(config.max_retries)
			.with_stream_timeouts(config.first_event_timeout, config.idle_timeout)
			.with_claude_code_session_id(config.claude_code_session_id)
			.with_custom_headers(config.custom_headers)
			.with_anthropic_header_overrides(config.allow_anthropic_header_overrides)
			.with_provider_id(config.provider_id)
			.with_requires_tool_result_id(config.requires_tool_result_id)
			.with_escape_builtin_tool_names(config.escape_builtin_tool_names)
			.with_mid_conversation_system(config.supports_mid_conversation_system)
			.with_cache_scope(config.cache_scope)
			.with_strict_tools(config.strict_tools, config.disable_strict_tools))
	}

	/// `max_tokens` for every request. Anthropic requires the field.
	pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
		self.max_tokens = max_tokens.max(1);
		self
	}

	/// Requested `max_tokens`; thinking may raise it up to [`Self::with_max_tokens`].
	pub fn with_request_max_tokens(mut self, max_tokens: Option<u32>) -> Self {
		self.request_max_tokens = max_tokens.map(|value| value.max(1));
		self
	}

	/// Enable or disable temperature/top_p/top_k (TS `supportsSamplingParams`).
	pub fn with_sampling_params(mut self, enabled: bool) -> Self {
		self.supports_sampling_params = enabled;
		self
	}

	/// Temperature sent when sampling is allowed.
	pub fn with_temperature(mut self, temperature: Option<f64>) -> Self {
		self.temperature = temperature;
		self
	}

	/// `top_p` sent when sampling is allowed.
	pub fn with_top_p(mut self, top_p: Option<f64>) -> Self {
		self.top_p = top_p;
		self
	}

	/// `top_k` sent when sampling is allowed.
	pub fn with_top_k(mut self, top_k: Option<u32>) -> Self {
		self.top_k = top_k;
		self
	}

	/// Stop sequences, trimmed to Anthropic's four-entry wire limit.
	pub fn with_stop_sequences(mut self, sequences: impl IntoIterator<Item = String>) -> Self {
		self.stop_sequences = sequences.into_iter().take(4).collect();
		self
	}

	/// Whether the model accepts forced `tool_choice` (TS `supportsForcedToolChoice`).
	pub fn with_forced_tool_choice(mut self, supported: bool) -> Self {
		self.supports_forced_tool_choice = supported;
		self
	}

	/// Adaptive-only models reject `thinking.type: "disabled"`; pin low effort.
	pub fn with_adaptive_only_thinking(mut self, adaptive_only: bool) -> Self {
		self.adaptive_only_thinking = adaptive_only;
		self
	}

	/// Prompt-cache retention and long-TTL support.
	pub fn with_cache_retention(
		mut self,
		retention: CacheRetention,
		supports_long_ttl: bool,
	) -> Self {
		self.cache_retention = retention;
		self.supports_long_cache_retention = supports_long_ttl;
		self
	}

	/// Retry budget for transient HTTP/transport failures (TS default 2).
	pub fn with_max_retries(mut self, retries: u32) -> Self {
		self.max_retries = retries;
		self
	}

	/// First-event and idle watchdog budgets for streamed responses.
	pub fn with_stream_timeouts(
		mut self,
		first_event: Option<Duration>,
		idle: Option<Duration>,
	) -> Self {
		self.first_event_timeout = first_event.filter(|value| !value.is_zero());
		self.idle_timeout = idle.filter(|value| !value.is_zero());
		self
	}

	/// Claude Code session header value for OAuth requests.
	pub fn with_claude_code_session_id(mut self, session_id: Option<String>) -> Self {
		self.claude_code_session_id = session_id.filter(|value| !value.is_empty());
		self
	}

	/// Caller/model headers. Enforced credential and beta headers still win,
	/// matching TS `buildAnthropicHeaders` precedence.
	pub fn with_custom_headers(mut self, headers: impl IntoIterator<Item = (String, String)>) -> Self {
		self.custom_headers = headers.into_iter().collect();
		self
	}

	/// Allow OAuth requests to override anthropic-version/beta/x-app (TS
	/// `allowAnthropicHeaderOverrides`, never on the official endpoint).
	pub fn with_anthropic_header_overrides(mut self, allowed: bool) -> Self {
		self.allow_anthropic_header_overrides = allowed;
		self
	}

	/// Configured provider id used in `ModelProvider::provider` provenance.
	pub fn with_provider_id(mut self, provider_id: Option<String>) -> Self {
		self.provider_id = provider_id.filter(|value| !value.trim().is_empty());
		self
	}

	/// TS `options.serviceTier`: `priority` realizes fast mode on the direct
	/// Anthropic provider (`speed: "fast"` plus [`FAST_MODE_BETA`]).
	pub fn with_service_tier(mut self, service_tier: Option<String>) -> Self {
		self.service_tier = service_tier.filter(|tier| !tier.trim().is_empty());
		self
	}

	/// User asked for priority on a direct Anthropic route.
	fn wants_priority(&self) -> bool {
		self.service_tier.as_deref() == Some("priority")
	}

	/// TS `model.provider === "anthropic" && serviceTier === "priority"`.
	fn fast_mode_active(&self) -> bool {
		!self.fast_mode_disabled.load(Ordering::Relaxed)
			&& self.provider_id.as_deref().unwrap_or("anthropic") == "anthropic"
			&& self.wants_priority()
	}

	/// Z.AI-style gateways require `id` aliased to `tool_use_id` on tool results.
	pub fn with_requires_tool_result_id(mut self, required: bool) -> Self {
		self.requires_tool_result_id = required;
		self
	}

	/// Prefix built-in-looking tool names for non-OAuth gateways.
	pub fn with_escape_builtin_tool_names(mut self, enabled: bool) -> Self {
		self.escape_builtin_tool_names = enabled;
		self
	}

	/// First-party models that accept mid-conversation `system` turns.
	pub fn with_mid_conversation_system(mut self, supported: bool) -> Self {
		self.supports_mid_conversation_system = supported;
		self
	}

	/// Prompt-cache scope (`global` requires its own beta on some models).
	pub fn with_cache_scope(mut self, scope: Option<String>) -> Self {
		self.cache_scope = scope.filter(|value| !value.trim().is_empty());
		self
	}

	/// Enable Anthropic strict tool schemas unless the model/gateway disables them.
	pub fn with_strict_tools(mut self, enabled: bool, disabled: bool) -> Self {
		self.strict_tools = enabled;
		self.disable_strict_tools = disabled;
		self
	}

	/// Set the thinking request shape. `None` omits `thinking` entirely.
	pub fn with_thinking(mut self, thinking: Option<AnthropicThinkingConfig>) -> Self {
		self.thinking = thinking;
		self
	}

	/// Adaptive-thinking effort sent as `output_config.effort`.
	pub fn with_output_effort(mut self, effort: Option<String>) -> Self {
		self.output_effort = effort.filter(|value| !value.trim().is_empty());
		self
	}

	/// Additional `anthropic-beta` entries, deduplicated with the profile.
	pub fn with_extra_betas(mut self, betas: impl IntoIterator<Item = String>) -> Self {
		self.extra_betas = betas
			.into_iter()
			.map(|beta| beta.trim().to_string())
			.filter(|beta| !beta.is_empty())
			.collect();
		self
	}

	/// Enable or disable the interleaved-thinking beta for non-adaptive models.
	pub fn with_interleaved_thinking(mut self, enabled: bool) -> Self {
		self.interleaved_thinking = enabled;
		self
	}

	/// Force OAuth or API-key auth; `None` sniffs the credential.
	pub fn with_oauth(mut self, oauth: Option<bool>) -> Self {
		self.is_oauth = oauth;
		self
	}

	/// Enable or disable multimodal image inputs.
	pub fn with_images(mut self, enabled: bool) -> Self {
		self.supports_images = enabled;
		self
	}

	/// Replay unsigned thinking blocks instead of demoting them to text.
	pub fn with_replay_unsigned_thinking(mut self, enabled: bool) -> Self {
		self.replay_unsigned_thinking = enabled;
		self
	}

	/// Whether multimodal image inputs are accepted.
	pub fn supports_images(&self) -> bool {
		self.supports_images
	}

	/// Whether requests use OAuth bearer auth.
	pub fn is_oauth(&self) -> bool {
		self
			.is_oauth
			.unwrap_or_else(|| self.api_key.contains("sk-ant-oat"))
	}

	/// Official Anthropic API host, which uses `x-api-key` for API-key auth.
	pub fn is_official_endpoint(&self) -> bool {
		reqwest::Url::parse(&self.base_url)
			.ok()
			.and_then(|url| url.host_str().map(str::to_ascii_lowercase))
			.as_deref()
			== Some("api.anthropic.com")
	}

	/// `{base_url}/v1/messages`, plus `?beta=true` for OAuth requests.
	pub fn endpoint(&self) -> String {
		let path = format!("{}/v1/messages", self.base_url.trim_end_matches('/'));
		if self.is_oauth() {
			format!("{path}?beta=true")
		} else {
			path
		}
	}

	/// Request headers for one call. `stream` selects the SSE accept type for
	/// API-key requests; OAuth requests always accept JSON, matching TS.
	pub fn request_headers(&self, stream: bool) -> Result<reqwest::header::HeaderMap, ProviderError> {
		self.request_headers_for(stream, false)
	}

	/// Request headers with knowledge of whether tools are offered.
	///
	/// The OAuth beta profile distinguishes agent requests (tools/thinking)
	/// from utility requests, matching `buildCoworkBetas`.
	pub fn request_headers_for(
		&self,
		stream: bool,
		has_tools: bool,
	) -> Result<reqwest::header::HeaderMap, ProviderError> {
		use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, USER_AGENT};

		let mut headers = HeaderMap::new();
		let oauth = self.is_oauth();
		let allow_overrides =
			oauth && self.allow_anthropic_header_overrides && !self.is_official_endpoint();
		let mut model_headers: Vec<(String, String)> = Vec::new();
		let mut overrides: Vec<(String, String)> = Vec::new();
		let mut incoming_user_agent = None;
		let mut incoming_authorization = None;
		let mut incoming_api_key = None;
		for (name, value) in &self.custom_headers {
			let lower = name.to_ascii_lowercase();
			match lower.as_str() {
				"user-agent" => incoming_user_agent = Some(value.clone()),
				"authorization" => incoming_authorization = Some(value.clone()),
				"x-api-key" => incoming_api_key = Some(value.clone()),
				_ => {},
			}
			if is_enforced_header(&lower) {
				if allow_overrides && is_overridable_header(&lower) {
					overrides.push((name.clone(), value.clone()));
				}
			} else {
				model_headers.push((name.clone(), value.clone()));
			}
		}
		for (name, value) in &model_headers {
			insert_header(&mut headers, parse_header_name(name)?, value)?;
		}

		let accept = if oauth {
			"application/json"
		} else if stream {
			"text/event-stream"
		} else {
			"application/json"
		};
		insert_header(&mut headers, ACCEPT, accept)?;
		insert_header(&mut headers, CONTENT_TYPE, "application/json")?;
		insert_header(
			&mut headers,
			reqwest::header::HeaderName::from_static("connection"),
			"keep-alive",
		)?;
		insert_header(
			&mut headers,
			reqwest::header::HeaderName::from_static("accept-encoding"),
			"gzip",
		)?;
		insert_header(
			&mut headers,
			reqwest::header::HeaderName::from_static("anthropic-version"),
			ANTHROPIC_VERSION,
		)?;
		insert_header(
			&mut headers,
			reqwest::header::HeaderName::from_static("anthropic-dangerous-direct-browser-access"),
			"true",
		)?;
		insert_header(
			&mut headers,
			reqwest::header::HeaderName::from_static("x-app"),
			"cli",
		)?;

		if oauth {
			insert_header(
				&mut headers,
				AUTHORIZATION,
				&format!("Bearer {}", self.api_key),
			)?;
			let user_agent = incoming_user_agent
				.as_deref()
				.filter(|value| value.to_ascii_lowercase().starts_with("claude-cli"))
				.unwrap_or(CLAUDE_CODE_USER_AGENT);
			insert_header(&mut headers, USER_AGENT, user_agent)?;
			if let Some(session_id) = &self.claude_code_session_id {
				insert_header(
					&mut headers,
					reqwest::header::HeaderName::from_static("x-claude-code-session-id"),
					session_id,
				)?;
			}
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("x-stainless-arch"),
				&map_stainless_arch(std::env::consts::ARCH),
			)?;
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("x-stainless-lang"),
				"js",
			)?;
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("x-stainless-os"),
				"Linux",
			)?;
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("x-stainless-package-version"),
				CLAUDE_CODE_SDK_VERSION,
			)?;
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("x-stainless-retry-count"),
				"0",
			)?;
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("x-stainless-runtime"),
				"node",
			)?;
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("x-stainless-runtime-version"),
				"v26.3.0",
			)?;
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("x-stainless-timeout"),
				"600",
			)?;
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("x-client-request-id"),
				&random_uuid_v4(),
			)?;
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("connection"),
				"keep-alive",
			)?;
			if let Some(api_key) = &incoming_api_key {
				insert_header(
					&mut headers,
					reqwest::header::HeaderName::from_static("x-api-key"),
					api_key,
				)?;
			}
		} else if self.is_official_endpoint() {
			if let Some(user_agent) = &incoming_user_agent {
				insert_header(&mut headers, USER_AGENT, user_agent)?;
			}
			if let Some(authorization) = &incoming_authorization {
				insert_header(&mut headers, AUTHORIZATION, authorization)?;
			}
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("x-api-key"),
				incoming_api_key.as_deref().unwrap_or(&self.api_key),
			)?;
		} else {
			insert_header(
				&mut headers,
				AUTHORIZATION,
				incoming_authorization
					.as_deref()
					.unwrap_or(&format!("Bearer {}", self.api_key)),
			)?;
			if let Some(user_agent) = &incoming_user_agent {
				insert_header(&mut headers, USER_AGENT, user_agent)?;
			}
			if let Some(api_key) = &incoming_api_key {
				insert_header(
					&mut headers,
					reqwest::header::HeaderName::from_static("x-api-key"),
					api_key,
				)?;
			}
		}

		// TS re-applies caller overrides last, OAuth non-official only.
		for (name, value) in &overrides {
			insert_header(&mut headers, parse_header_name(name)?, value)?;
		}

		let betas = self.betas_for(has_tools);
		if !betas.is_empty() {
			insert_header(
				&mut headers,
				reqwest::header::HeaderName::from_static("anthropic-beta"),
				&betas.join(","),
			)?;
		}
		Ok(headers)
	}

	/// Deduplicated beta profile without tool awareness (utility profile for
	/// OAuth requests).
	pub fn betas(&self) -> Vec<String> {
		self.betas_for(false)
	}

	/// Deduplicated beta profile for this request.
	pub fn betas_for(&self, has_tools: bool) -> Vec<String> {
		let mut betas: Vec<String> = Vec::new();
		if self.is_oauth() {
			let agent_request = has_tools || self.thinking.is_some();
			let profile: &[&str] = if agent_request {
				&[
					"claude-code-20250219",
					ANTHROPIC_OAUTH_BETA,
					ANTHROPIC_INTERLEAVED_THINKING_BETA,
					"thinking-token-count-2026-05-13",
					ANTHROPIC_CONTEXT_MANAGEMENT_BETA,
					"prompt-caching-scope-2026-01-05",
					"mid-conversation-system-2026-04-07",
					"advanced-tool-use-2025-11-20",
				]
			} else {
				&[
					ANTHROPIC_OAUTH_BETA,
					ANTHROPIC_INTERLEAVED_THINKING_BETA,
					"thinking-token-count-2026-05-13",
					ANTHROPIC_CONTEXT_MANAGEMENT_BETA,
					"prompt-caching-scope-2026-01-05",
					"structured-outputs-2025-12-15",
				]
			};
			betas.extend(profile.iter().map(|beta| (*beta).to_string()));
			if agent_request {
				if self.thinking.is_some() || self.output_effort.is_some() {
					betas.push(ANTHROPIC_EFFORT_BETA.to_string());
				}
				betas.push("fallback-credit-2026-06-01".to_string());
			}
		} else {
			if let Some(thinking) = &self.thinking {
				if self.interleaved_thinking && !thinking.is_adaptive() {
					betas.push(ANTHROPIC_INTERLEAVED_THINKING_BETA.to_string());
				}
				betas.push(ANTHROPIC_CONTEXT_MANAGEMENT_BETA.to_string());
			}
			if self.output_effort.is_some() {
				betas.push(ANTHROPIC_EFFORT_BETA.to_string());
			}
			if self.cache_retention == CacheRetention::Long
				&& self.supports_long_cache_retention
			{
				betas.push("extended-cache-ttl-2025-04-11".to_string());
			}
		}
		if self.fast_mode_active() {
			betas.push(FAST_MODE_BETA.to_string());
		}
		betas.extend(self.extra_betas.iter().cloned());
		dedupe(betas)
	}

	/// Build the request body without performing I/O.
	///
	/// `tool_choice` is validated here, before any request leaves the client, so
	/// an unoffered/empty forced tool fails with [`ProviderError::InvalidToolChoice`].
	pub fn build_request(
		&self,
		messages: &[Message],
		tools: &[serde_json::Value],
		tool_choice: &ToolChoice,
		stream: bool,
	) -> Result<AnthropicMessagesRequest, ProviderError> {
		let oauth = self.is_oauth();
		let first_user_text = first_user_text(messages);
		let (mut system, mut converted_messages) = self.convert_messages(messages)?;
		let converted_tools = self.convert_tools(tools)?;
		let mut choice = self.convert_tool_choice(tool_choice, tools)?;
		let requested_forced =
			matches!(tool_choice, ToolChoice::Required | ToolChoice::Tool { .. });
		if requested_forced && !self.supports_forced_tool_choice {
			choice = Some(AnthropicToolChoice::Auto);
		}
		let forced = requested_forced && self.supports_forced_tool_choice;

		// OAuth requests carry the Claude Code identity + billing header blocks.
		let has_billing_header = system.as_ref().is_some_and(|blocks| {
			blocks.iter().any(|block| {
				matches!(block, AnthropicTextBlock::Text { text } if text.starts_with("x-anthropic-billing-header:"))
			})
		});
		if oauth && !self.model.starts_with("claude-3-5-haiku") && !has_billing_header {
			let mut blocks = vec![
				AnthropicTextBlock::Text {
					text: create_claude_billing_header(&first_user_text),
				},
				AnthropicTextBlock::Text {
					text: CLAUDE_CODE_SYSTEM_INSTRUCTION.to_string(),
				},
			];
			if let Some(existing) = system.take() {
				blocks.extend(existing);
			}
			system = Some(blocks);
		}

		let mut thinking = if forced { None } else { self.thinking.clone() };
		let mut output_effort = self.output_effort.clone();
		if forced {
			output_effort = if self.adaptive_only_thinking {
				Some("low".to_string())
			} else {
				None
			};
		} else if matches!(thinking, Some(AnthropicThinkingConfig::Disabled))
			&& self.adaptive_only_thinking
		{
			// Adaptive-only models reject `thinking.type: "disabled"`; omit it
			// and pin the lowest adaptive effort instead.
			thinking = None;
			output_effort = Some("low".to_string());
		}

		let context_management = thinking
			.as_ref()
			.filter(|thinking| thinking.keeps_context())
			.map(|_| AnthropicContextManagement {
				edits: vec![AnthropicContextEdit {
					kind: "clear_thinking_20251015".to_string(),
					keep: "all".to_string(),
				}],
			});
		let output_config = output_effort
			.clone()
			.map(|effort| AnthropicOutputConfig { effort: Some(effort) });

		let requested_max_tokens = self
			.request_max_tokens
			.unwrap_or(self.max_tokens)
			.min(self.max_tokens);
		let max_tokens = ensure_max_tokens_for_thinking(
			requested_max_tokens,
			self.max_tokens,
			&mut thinking,
		)?;

		let allow_sampling = self.supports_sampling_params
			&& thinking
				.as_ref()
				.is_none_or(|thinking| matches!(thinking, AnthropicThinkingConfig::Disabled));
		let temperature = allow_sampling.then_some(self.temperature).flatten();
		let top_p = allow_sampling.then_some(self.top_p).flatten();
		let top_k = allow_sampling.then_some(self.top_k).flatten();

		if let Some(cache_control) = self.cache_control() {
			apply_prompt_caching(&mut converted_messages, &cache_control);
		}

		Ok(AnthropicMessagesRequest {
			model: self.model.clone(),
			messages: converted_messages,
			system,
			max_tokens,
			tools: converted_tools,
			tool_choice: choice,
			thinking,
			context_management,
			output_config,
			temperature,
			top_p,
			top_k,
			stop_sequences: (!self.stop_sequences.is_empty())
				.then(|| self.stop_sequences.clone()),
			speed: self.fast_mode_active().then(|| "fast".to_string()),
			stream,
		})
	}

	fn cache_control(&self) -> Option<AnthropicCacheControl> {
		match self.cache_retention {
			CacheRetention::None => None,
			CacheRetention::Short => Some(AnthropicCacheControl {
				kind: "ephemeral".to_string(),
				ttl: None,
				scope: self.cache_scope.clone(),
			}),
			CacheRetention::Long => Some(AnthropicCacheControl {
				kind: "ephemeral".to_string(),
				ttl: self.supports_long_cache_retention.then(|| "1h".to_string()),
				scope: self.cache_scope.clone(),
			}),
		}
	}

	fn convert_tools(
		&self,
		tools: &[serde_json::Value],
	) -> Result<Vec<AnthropicTool>, ProviderError> {
		let mut converted = Vec::with_capacity(tools.len());
		let mut strict_tools_used = 0usize;
		let mut strict_optional_used = 0usize;
		for (index, tool) in tools.iter().enumerate() {
			let function = tool.get("function").unwrap_or(tool);
			let name = function
				.get("name")
				.and_then(serde_json::Value::as_str)
				.filter(|name| !name.trim().is_empty())
				.ok_or_else(|| {
					ProviderError::Protocol(format!("tool at index {index} is missing a name"))
				})?;
			let description = function
				.get("description")
				.and_then(serde_json::Value::as_str)
				.unwrap_or("")
				.to_string();
			let parameters = function
				.get("parameters")
				.or_else(|| function.get("input_schema"))
				.cloned()
				.unwrap_or_else(|| serde_json::json!({ "type": "object", "properties": {} }));
			let mut input_schema = normalize_tool_schema(parameters, index)?;
			let mut strict = false;
			if self.strict_tools
				&& !self.disable_strict_tools
				&& strict_tools_used < 20
				&& matches!(name, "bash" | "python" | "edit" | "find")
			{
				if let Some((schema, optional_count)) = strict_tool_schema(
					input_schema.clone(),
					24 - strict_optional_used,
				) {
					strict_tools_used += 1;
					strict_optional_used += optional_count;
					input_schema = schema;
					strict = true;
				}
			}
			converted.push(AnthropicTool {
				name: self.encode_tool_name(name),
				description,
				input_schema,
				eager_input_streaming: self.is_official_endpoint().then_some(true),
				strict: strict.then_some(true),
			});
		}
		Ok(converted)
	}

	fn convert_tool_choice(
		&self,
		tool_choice: &ToolChoice,
		tools: &[serde_json::Value],
	) -> Result<Option<AnthropicToolChoice>, ProviderError> {
		Ok(match tool_choice {
			ToolChoice::Auto => None,
			ToolChoice::None => Some(AnthropicToolChoice::None),
			ToolChoice::Required => Some(AnthropicToolChoice::Any),
			ToolChoice::Tool { name } => {
				if name.trim().is_empty() {
					return Err(ProviderError::InvalidToolChoice(
						"forced tool name must not be empty".to_string(),
					));
				}
				let offered = tools.iter().any(|tool| {
					tool
						.get("function")
						.unwrap_or(tool)
						.get("name")
						.and_then(serde_json::Value::as_str)
						== Some(name.as_str())
				});
				if !offered {
					return Err(ProviderError::InvalidToolChoice(format!(
						"forced tool `{name}` is not offered in this request"
					)));
				}
				Some(AnthropicToolChoice::Tool {
					name: self.encode_tool_name(name),
				})
			},
		})
	}

	fn encode_tool_name(&self, name: &str) -> String {
		// `escape_builtin_tool_names` is an explicit caller opt-in and prefixes
		// every name, built-ins included; OAuth alone leaves built-ins bare.
		let escape = self.escape_builtin_tool_names
			|| (self.is_oauth()
				&& !BUILTIN_TOOL_NAMES.contains(&name.to_ascii_lowercase().as_str()));
		if escape {
			format!("{CLAUDE_TOOL_PREFIX}{name}")
		} else {
			name.to_string()
		}
	}

	fn decode_tool_name(&self, name: &str) -> String {
		if self.is_oauth() || self.escape_builtin_tool_names {
			if let Some(stripped) = name.strip_prefix(CLAUDE_TOOL_PREFIX) {
				return stripped.to_string();
			}
		}
		name.to_string()
	}

	fn convert_messages(
		&self,
		messages: &[Message],
	) -> Result<(Option<Vec<AnthropicTextBlock>>, Vec<AnthropicMessage>), ProviderError> {
		let mut system = Vec::new();
		let mut params: Vec<AnthropicMessage> = Vec::new();
		let mut developer_indices = Vec::new();
		let mut index = 0;

		while index < messages.len() {
			let message = &messages[index];
			match message.role {
				Role::System => {
					let text = message_text(message);
					if !text.trim().is_empty() {
						system.push(AnthropicTextBlock::Text { text });
					}
					index += 1;
				},
				Role::User | Role::Developer => {
					if let Some(content) = self.user_content(message)? {
						params.push(AnthropicMessage {
							role: "user".to_string(),
							content,
						});
						if message.role == Role::Developer && self.supports_mid_conversation_system {
							developer_indices.push(params.len() - 1);
						}
					}
					index += 1;
				},
				Role::Assistant => {
					let blocks = self.assistant_blocks(message);
					if !blocks.is_empty() {
						params.push(AnthropicMessage {
							role: "assistant".to_string(),
							content: AnthropicContent::Blocks(blocks),
						});
					}
					index += 1;
				},
				Role::Tool => {
					let mut results = Vec::new();
					let mut hoisted_images = Vec::new();
					while index < messages.len() && messages[index].role == Role::Tool {
						let message = &messages[index];
						let content =
							self.tool_result_content(message, &mut hoisted_images)?;
						let tool_use_id = message.tool_call_id.clone().unwrap_or_default();
						results.push(AnthropicBlock::ToolResult {
							tool_use_id: tool_use_id.clone(),
							content,
							is_error: message.is_error.then_some(true),
							id: self.requires_tool_result_id.then_some(tool_use_id),
							cache_control: None,
						});
						index += 1;
					}
					if !hoisted_images.is_empty() {
						results.push(AnthropicBlock::Text {
							text: "Attached image(s) from the tool result(s) above:".to_string(),
							cache_control: None,
						});
						results.extend(hoisted_images);
					}
					if !results.is_empty() {
						params.push(AnthropicMessage {
							role: "user".to_string(),
							content: AnthropicContent::Blocks(results),
						});
					}
				},
			}
		}

		// Mid-conversation system placement (first-party Opus/Fable/Mythos):
		// must follow a user turn and be last or followed by an assistant.
		for idx in developer_indices {
			let follows_user = idx > 0 && params[idx - 1].role == "user";
			let last_or_before_assistant =
				idx == params.len() - 1 || params[idx + 1].role == "assistant";
			let text_only = match &params[idx].content {
				AnthropicContent::Text(_) => true,
				AnthropicContent::Blocks(blocks) => blocks
					.iter()
					.all(|block| matches!(block, AnthropicBlock::Text { .. })),
			};
			if follows_user && last_or_before_assistant && text_only {
				params[idx].role = "system".to_string();
			}
		}

		// The Messages API rejects consecutive assistant turns; upstream repairs
		// them with the same neutral nudge and never ends a request on an
		// assistant prefill.
		let mut repaired: Vec<AnthropicMessage> = Vec::with_capacity(params.len());
		for message in params {
			if message.role == "assistant"
				&& repaired.last().is_some_and(|previous| previous.role == "assistant")
			{
				repaired.push(AnthropicMessage {
					role: "user".to_string(),
					content: AnthropicContent::Text("Continue.".to_string()),
				});
			}
			repaired.push(message);
		}
		if repaired.last().is_some_and(|message| message.role == "assistant") {
			repaired.push(AnthropicMessage {
				role: "user".to_string(),
				content: AnthropicContent::Text("Continue.".to_string()),
			});
		}

		Ok(((!system.is_empty()).then_some(system), repaired))
	}

	fn user_content(&self, message: &Message) -> Result<Option<AnthropicContent>, ProviderError> {
		if !message.has_image() {
			return Ok((!message.content.trim().is_empty())
				.then(|| AnthropicContent::Text(message.content.clone())));
		}
		if !self.supports_images {
			let mut texts = Vec::new();
			for part in &message.parts {
				match part {
					ContentPart::Text { text } if !text.trim().is_empty() => texts.push(text.clone()),
					ContentPart::Image { .. } => {
						texts.push(NON_VISION_IMAGE_PLACEHOLDER.to_string());
					},
					_ => {},
				}
			}
			if texts.is_empty() && !message.content.trim().is_empty() {
				texts.push(message.content.clone());
			}
			return Ok((!texts.is_empty()).then(|| AnthropicContent::Text(texts.join("\n"))));
		}

		let mut blocks = Vec::new();
		let mut saw_text = false;
		let mut saw_image = false;
		for part in &message.parts {
			match part {
				ContentPart::Text { text } => {
					if !text.trim().is_empty() {
						saw_text = true;
						blocks.push(AnthropicBlock::Text { text: text.clone(), cache_control: None });
					}
				},
				ContentPart::Image { data, mime_type } => {
					blocks.push(AnthropicBlock::Image {
						source: image_source(data, mime_type)?,
						cache_control: None,
					});
					saw_image = true;
				},
				ContentPart::Thinking { .. }
				| ContentPart::RedactedThinking { .. }
				| ContentPart::ToolCall { .. } => {},
			}
		}
		if !saw_text && !message.content.trim().is_empty() {
			saw_text = true;
			blocks.insert(0, AnthropicBlock::Text { text: message.content.clone(), cache_control: None });
		}
		if saw_image && !saw_text {
			blocks.insert(
				0,
				AnthropicBlock::Text { text: "(see attached image)".to_string(), cache_control: None },
			);
		}
		Ok((!blocks.is_empty()).then(|| AnthropicContent::Blocks(blocks)))
	}

	fn assistant_blocks(&self, message: &Message) -> Vec<AnthropicBlock> {
		let mut blocks = Vec::new();
		let mut tool_blocks = Vec::new();
		let mut saw_text = false;
		let mut saw_tool_call_part = false;
		// A message mixing signed and unsigned reasoning cannot replay the
		// unsigned half as a thinking block: only a turn with no signed block at
		// all may fall back to unsigned replay.
		let has_signed = message.parts.iter().any(|part| {
			matches!(
				part,
				ContentPart::Thinking { thinking_signature: Some(signature), .. }
					if !signature.trim().is_empty()
			)
		});
		let replay_unsigned = self.replay_unsigned_thinking && !has_signed;
		for part in &message.parts {
			match part {
				ContentPart::Text { text } => {
					if !text.trim().is_empty() {
						saw_text = true;
						blocks.push(AnthropicBlock::Text { text: text.clone(), cache_control: None });
					}
				},
				ContentPart::Thinking { thinking, thinking_signature } => {
					if thinking.trim().is_empty() {
						continue;
					}
					if let Some(signature) = thinking_signature
						.as_deref()
						.filter(|signature| !signature.trim().is_empty())
					{
						blocks.push(AnthropicBlock::Thinking {
							thinking: thinking.clone(),
							signature: signature.to_string(),
						});
					} else if replay_unsigned {
						blocks.push(AnthropicBlock::Thinking {
							thinking: thinking.clone(),
							signature: String::new(),
						});
					} else {
						// Official Anthropic endpoints reject unsigned thinking
						// blocks on replay; upstream demotes them to plain prose.
						blocks.push(AnthropicBlock::Text { text: thinking.clone(), cache_control: None });
					}
				},
				ContentPart::RedactedThinking { data } => {
					if !data.trim().is_empty() {
						blocks.push(AnthropicBlock::RedactedThinking { data: data.clone() });
					}
				},
				ContentPart::ToolCall { id, name, arguments } => {
					saw_tool_call_part = true;
					tool_blocks.push(AnthropicBlock::ToolUse {
						id: id.clone(),
						name: self.encode_tool_name(name),
						input: arguments.clone(),
						cache_control: None,
					});
				},
				ContentPart::Image { .. } => {},
			}
		}
		if !saw_text && !message.parts.is_empty() && !message.content.trim().is_empty() {
			blocks.push(AnthropicBlock::Text { text: message.content.clone(), cache_control: None });
		} else if message.parts.is_empty() && !message.content.trim().is_empty() {
			blocks.push(AnthropicBlock::Text { text: message.content.clone(), cache_control: None });
		}
		if !saw_tool_call_part {
			for call in &message.tool_calls {
				tool_blocks.push(AnthropicBlock::ToolUse {
					id: call.id.clone(),
					name: self.encode_tool_name(&call.name),
					input: call.arguments.clone(),
					cache_control: None,
				});
			}
		}
		// Anthropic requires tool_use blocks at the tail of an assistant turn.
		blocks.extend(tool_blocks);
		blocks
	}

	fn tool_result_content(
		&self,
		message: &Message,
		hoisted_images: &mut Vec<AnthropicBlock>,
	) -> Result<AnthropicContent, ProviderError> {
		let mut blocks = Vec::new();
		let mut text = String::new();
		let mut saw_text_part = false;
		let mut saw_image = false;
		for part in &message.parts {
			match part {
				ContentPart::Text { text: value } => {
					if !value.is_empty() {
						saw_text_part = true;
						if !text.is_empty() {
							text.push('\n');
						}
						text.push_str(value);
					}
				},
				ContentPart::Image { data, mime_type } => {
					if !self.supports_images {
						if !text.is_empty() {
							text.push('\n');
						}
						text.push_str(NON_VISION_IMAGE_PLACEHOLDER);
						continue;
					}
					let block = AnthropicBlock::Image {
						source: image_source(data, mime_type)?,
						cache_control: None,
					};
					if message.is_error {
						// Error tool results may only carry text; replay images
						// after the tool-result run instead.
						hoisted_images.push(block);
						continue;
					}
					blocks.push(block);
					saw_image = true;
				},
				ContentPart::Thinking { .. }
				| ContentPart::RedactedThinking { .. }
				| ContentPart::ToolCall { .. } => {},
			}
		}
		if !saw_text_part && !message.content.trim().is_empty() {
			if text.is_empty() {
				text = message.content.clone();
			} else {
				text = format!("{}\n{text}", message.content);
			}
		}
		if message.is_error && text.trim().is_empty() && !saw_image {
			text = EMPTY_ERROR_TOOL_RESULT_TEXT.to_string();
		}
		if !saw_image {
			return Ok(AnthropicContent::Text(text));
		}
		if text.is_empty() {
			text = "(see attached image)".to_string();
		}
		let mut content = Vec::with_capacity(blocks.len() + 1);
		if !text.is_empty() {
			content.push(AnthropicBlock::Text { text, cache_control: None });
		}
		content.extend(blocks);
		Ok(AnthropicContent::Blocks(content))
	}

	fn from_response(&self, response: AnthropicResponse) -> Message {
		let raw_stop = response.stop_reason.clone();
		let stop_reason = raw_stop.as_deref().map(map_stop_reason);
		let error_message = matches!(&stop_reason, Some(StopReason::Error)).then(|| {
			stop_error_message(
				raw_stop.as_deref(),
				response.stop_details.as_ref(),
				&self.credential_values(),
			)
		});
		let mut parts = Vec::new();
		let mut text = String::new();
		let mut tool_calls = Vec::new();
		for block in response.content {
			match block {
				AnthropicResponseBlock::Text { text: value } => {
					if !value.is_empty() {
						text.push_str(&value);
						parts.push(ContentPart::Text { text: value });
					}
				},
				AnthropicResponseBlock::Thinking { thinking, signature } => {
					if !thinking.is_empty() {
						parts.push(match signature.filter(|signature| !signature.is_empty()) {
							Some(signature) => {
								ContentPart::thinking_with_signature(thinking, signature)
							},
							None => ContentPart::thinking(thinking),
						});
					}
				},
				AnthropicResponseBlock::RedactedThinking { data } => {
					if !data.is_empty() {
						parts.push(ContentPart::redacted_thinking(data));
					}
				},
				AnthropicResponseBlock::ToolUse { id, name, input } => {
					let call = ToolCall {
						id,
						name: self.decode_tool_name(&name),
						arguments: if input.is_null() {
							serde_json::Value::Object(serde_json::Map::new())
						} else {
							input
						},
					};
					parts.push(ContentPart::ToolCall {
						id: call.id.clone(),
						name: call.name.clone(),
						arguments: call.arguments.clone(),
					});
					tool_calls.push(call);
				},
				AnthropicResponseBlock::Unknown => {},
			}
		}

		let mut message = Message::assistant_with_tools(text, tool_calls);
		if parts.iter().any(|part| {
			matches!(
				part,
				ContentPart::Thinking { .. }
					| ContentPart::RedactedThinking { .. }
					| ContentPart::ToolCall { .. }
			)
		}) {
			message.parts = parts;
		}
		message.usage = response.usage.map(AnthropicUsage::into_token_usage);
		message.stop_reason = stop_reason;
		message.error_message = error_message;
		message.api = Some("anthropic-messages".to_string());
		message.provider = Some(
			self.provider_id
				.clone()
				.unwrap_or_else(|| "anthropic".to_string()),
		);
		message.model = Some(self.model.clone());
		message
	}
}

#[async_trait]
impl ModelProvider for AnthropicMessagesProvider {
	fn api(&self) -> Option<&str> {
		Some("anthropic-messages")
	}

	fn provider(&self) -> Option<&str> {
		self.provider_id.as_deref().or(Some("anthropic"))
	}

	fn model(&self) -> Option<&str> {
		Some(&self.model)
	}

	fn supports_images(&self) -> bool {
		self.supports_images
	}

	fn check_support(&self, messages: &[Message]) -> Result<(), ProviderError> {
		// TS replaces unsupported images with a text placeholder instead of
		// rejecting the turn; only malformed payloads are errors here.
		for message in messages {
			for image in message.image_parts() {
				image.validate().map_err(|err| {
					ProviderError::UnsupportedImage(format!("invalid image payload: {err}"))
				})?;
			}
		}
		Ok(())
	}

	async fn complete(
		&self,
		messages: &[Message],
		tools: &[serde_json::Value],
		tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		self.check_support(messages)?;
		let body = self.build_request(messages, tools, tool_choice, false)?;
		let (response, key) = self
			.send_with_retry(&body, false, !tools.is_empty())
			.await?;

		let status = response.status();
		let limit = if status.is_success() {
			MAX_RESPONSE_BODY_BYTES
		} else {
			ERROR_BODY_BYTES
		};
		let body = read_body_bounded(response, limit).await?;
		if !status.is_success() {
			return Err(ProviderError::HttpStatus(
				status.as_u16(),
				self.redact_snippet_with_key(&body.text, key.as_deref(), body.truncated),
			));
		}
		if body.truncated {
			return Err(ProviderError::Protocol(format!(
				"Anthropic response body exceeded {MAX_RESPONSE_BODY_BYTES} bytes"
			)));
		}

		let text = body.text;
		let parsed: AnthropicResponse = serde_json::from_str(&text).map_err(|err| {
			ProviderError::Protocol(format!(
				"invalid Anthropic message body: {err}; body: {}",
				self.redact_snippet_with_key(&text, key.as_deref(), false)
			))
		})?;
		Ok(self.from_response(parsed))
	}

	async fn stream(
		&self,
		messages: &[Message],
		tools: &[serde_json::Value],
		tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		self.check_support(messages)?;
		let body = self.build_request(messages, tools, tool_choice, true)?;
		let (response, key) = self
			.send_with_retry(&body, true, !tools.is_empty())
			.await?;

		let status = response.status();
		let response = if status.is_success() {
			(response, key)
		} else {
			let text = match read_body_bounded(response, ERROR_BODY_BYTES).await {
				Ok(body) => {
					self.redact_snippet_with_key(&body.text, key.as_deref(), body.truncated)
				},
				Err(err) => format!("<failed to read error body: {err}>"),
			};
			// TS `anthropic.ts:2764-2783`: only a priority request whose fast
			// mode is still active retries, only before any output exists (the
			// rejection is the whole response here), and only when the exact
			// fast-mode classifier matches. The generic retry budget is not
			// touched.
			if !(self.fast_mode_active() && is_fast_mode_unsupported(status.as_u16(), &text)) {
				return Err(ProviderError::HttpStatus(status.as_u16(), text));
			}
			self.fast_mode_disabled.store(true, Ordering::Relaxed);
			let retry_body = self.build_request(messages, tools, tool_choice, true)?;
			let (retry, retry_key) = self
				.send_with_retry(&retry_body, true, !tools.is_empty())
				.await?;
			let retry_status = retry.status();
			if !retry_status.is_success() {
				let retry_text = match read_body_bounded(retry, ERROR_BODY_BYTES).await {
					Ok(body) => self.redact_snippet_with_key(
						&body.text,
						retry_key.as_deref(),
						body.truncated,
					),
					Err(err) => format!("<failed to read error body: {err}>"),
				};
				return Err(ProviderError::HttpStatus(retry_status.as_u16(), retry_text));
			}
			(retry, retry_key)
		};
		let (response, key) = response;
		let disabled_features = (self.wants_priority()
			&& self.fast_mode_disabled.load(Ordering::Relaxed))
		.then(|| vec!["priority".to_string()]);

		let mut secrets = self.credential_values();
		if let Some(key) = key.as_deref().filter(|key| !key.is_empty()) {
			secrets.push(key.to_string());
		}
		let body = response
			.bytes_stream()
			.map(|item| item.map_err(|err| err.to_string()));
		let mut stream = with_disabled_features(
			anthropic_sse_stream(
				Box::pin(body),
				secrets,
				self.first_event_timeout,
				self.idle_timeout,
			),
			disabled_features.clone(),
		);
		// TS `firstTokenTime === undefined`: a fast-mode failure before any
		// observable output (text, reasoning, tool delta, redacted/signature
		// block or terminal stop) retries once without `speed` and the beta.
		// Metadata-only records are coalesced into one aggregate chunk instead
		// of being buffered one by one.
		let mut metadata: Option<StreamChunk> = None;
		let mut observable: Option<StreamChunk> = None;
		loop {
			match stream.next().await {
				Some(Ok(chunk)) => {
					let is_observable = chunk
						.content
						.as_deref()
						.is_some_and(|text| !text.is_empty())
						|| chunk
							.reasoning
							.as_deref()
							.is_some_and(|text| !text.is_empty())
						|| !chunk.tool_call_deltas.is_empty()
						|| chunk
							.redacted
							.as_deref()
							.is_some_and(|data| !data.is_empty())
						|| chunk.signature.is_some()
						|| chunk.is_done;
					if is_observable {
						observable = Some(chunk);
						break;
					}
					match &mut metadata {
						Some(aggregate) => merge_metadata(aggregate, chunk),
						None => metadata = Some(chunk),
					}
				},
				Some(Err(error)) => {
					if self.fast_mode_active()
						&& stream_error_is_fast_mode_unsupported(&error)
					{
						self.fast_mode_disabled.store(true, Ordering::Relaxed);
						let retry_body =
							self.build_request(messages, tools, tool_choice, true)?;
						let (retry, retry_key) = self
							.send_with_retry(&retry_body, true, !tools.is_empty())
							.await?;
						let retry_status = retry.status();
						if !retry_status.is_success() {
							let text = match read_body_bounded(retry, ERROR_BODY_BYTES).await {
								Ok(body) => self.redact_snippet_with_key(
									&body.text,
									retry_key.as_deref(),
									body.truncated,
								),
								Err(err) => format!("<failed to read error body: {err}>"),
							};
							return Err(ProviderError::HttpStatus(
								retry_status.as_u16(),
								text,
							));
						}
						let mut secrets = self.credential_values();
						if let Some(key) = retry_key.as_deref().filter(|key| !key.is_empty()) {
							secrets.push(key.to_string());
						}
						let retry_body = retry
							.bytes_stream()
							.map(|item| item.map_err(|err| err.to_string()));
						return Ok(with_disabled_features(
							anthropic_sse_stream(
								Box::pin(retry_body),
								secrets,
								self.first_event_timeout,
								self.idle_timeout,
							),
							Some(vec!["priority".to_string()]),
						));
					}
					return Ok(Box::pin(
						futures::stream::iter(metadata.map(Ok))
							.chain(futures::stream::iter([Err(error)]))
							.chain(stream),
					));
				},
				None => break,
			}
		}
		Ok(Box::pin(
			futures::stream::iter(metadata.map(Ok))
				.chain(futures::stream::iter(observable.map(Ok)))
				.chain(stream),
		))
	}
}

impl AnthropicMessagesProvider {
	async fn send_with_retry(
		&self,
		body: &AnthropicMessagesRequest,
		stream: bool,
		has_tools: bool,
	) -> Result<(reqwest::Response, Option<String>), ProviderError> {
		let mut attempt = 0u32;
		let mut refreshed = false;
		loop {
			// The callback runs before every POST, including 5xx retries, so a
			// pinned row disabled mid-review stops the next request. Only the
			// 401/403 invalid-key branch forces one refresh.
			let key = match &self.credential {
				Some(callback) => Some(callback(false).map_err(ProviderError::AuthRefresh)?),
				None => None,
			};
			let request = match &key {
				Some(key) => self.http_request_with_key(body, stream, has_tools, key)?,
				None => self.http_request(body, stream, has_tools)?,
			};
			match request.send().await {
				Ok(response) => {
					if response.status().is_success() {
						return Ok((response, key));
					}
					if matches!(response.status().as_u16(), 401 | 403) {
						let status = response.status();
						let body = read_body_bounded(response, ERROR_BODY_BYTES).await?;
						if !refreshed
							&& crate::provider::is_invalid_key_body(&body.text)
							&& let Some(callback) = &self.credential
						{
							callback(true).map_err(ProviderError::AuthRefresh)?;
							refreshed = true;
							continue;
						}
						return Err(ProviderError::HttpStatus(
							status.as_u16(),
							self.redact_snippet_with_key(
								&body.text,
								key.as_deref(),
								body.truncated,
							),
						));
					}
					let Some(delay) = (attempt < self.max_retries
						&& should_retry_response(&response))
					.then(|| retry_delay(&response, attempt))
					.flatten()
					else {
						return Ok((response, key));
					};
					drop(response);
					tokio::time::sleep(delay).await;
					attempt += 1;
				},
				Err(err) => {
					if attempt >= self.max_retries {
						return Err(network_error(err));
					}
					let delay = retry_delay_from_attempt(attempt);
					attempt += 1;
					tokio::time::sleep(delay).await;
				},
			}
		}
	}

	/// Install the per-request credential callback (C6 exact OAuth).
	#[must_use]
	pub fn with_credential_callback(
		mut self,
		callback: Option<crate::provider::CredentialCallback>,
	) -> Self {
		self.credential = callback;
		self
	}

	fn http_request_with_key(
		&self,
		body: &AnthropicMessagesRequest,
		stream: bool,
		has_tools: bool,
		key: &str,
	) -> Result<reqwest::RequestBuilder, ProviderError> {
		let mut headers = self.request_headers_for(stream, has_tools)?;
		let header_value = |value: String| {
			reqwest::header::HeaderValue::from_str(&value)
				.map_err(|err| ProviderError::Protocol(format!("invalid credential header: {err}")))
		};
		if headers.contains_key(reqwest::header::AUTHORIZATION) {
			headers.insert(
				reqwest::header::AUTHORIZATION,
				header_value(format!("Bearer {key}"))?,
			);
		}
		if headers.contains_key("x-api-key") {
			headers.insert(
				reqwest::header::HeaderName::from_static("x-api-key"),
				header_value(key.to_string())?,
			);
		}
		let request = self.http.post(self.endpoint()).headers(headers);
		if self.is_oauth() {
			let serialized = serde_json::to_string(body)?;
			let patched = patch_cch_placeholder(&serialized).unwrap_or(serialized);
			Ok(request.body(patched))
		} else {
			Ok(request.json(body))
		}
	}

	fn http_request(
		&self,
		body: &AnthropicMessagesRequest,
		stream: bool,
		has_tools: bool,
	) -> Result<reqwest::RequestBuilder, ProviderError> {
		let request = self
			.http
			.post(self.endpoint())
			.headers(self.request_headers_for(stream, has_tools)?);
		if self.is_oauth() {
			let serialized = serde_json::to_string(body)?;
			let patched = patch_cch_placeholder(&serialized).unwrap_or(serialized);
			Ok(request.body(patched))
		} else {
			Ok(request.json(body))
		}
	}

	fn credential_values(&self) -> Vec<String> {
		credential_values(&self.api_key, &self.custom_headers)
	}

	fn redact_snippet(&self, text: &str, truncated: bool) -> String {
		bounded_redaction(text, &self.credential_values(), truncated)
	}

	/// Redact a body against the static credential plus the exact token this
	/// request actually used (including a refreshed callback token, which is
	/// deliberately absent from `credential_values`).
	fn redact_snippet_with_key(
		&self,
		text: &str,
		key: Option<&str>,
		truncated: bool,
	) -> String {
		let mut secrets = self.credential_values();
		if let Some(key) = key.filter(|key| !key.is_empty()) {
			secrets.push(key.to_string());
		}
		bounded_redaction(text, &secrets, truncated)
	}
}

fn message_text(message: &Message) -> String {
	let mut text = message.content.clone();
	if message.parts.iter().any(|part| matches!(part, ContentPart::Text { .. })) {
		text = message
			.parts
			.iter()
			.filter_map(|part| match part {
				ContentPart::Text { text } => Some(text.as_str()),
				_ => None,
			})
			.collect::<Vec<_>>()
			.join("\n");
	}
	text
}

fn image_source(data: &str, mime_type: &str) -> Result<AnthropicImageSource, ProviderError> {
	let (clean_b64, mime) = crate::state::normalize_image_payload(data, mime_type)
		.map_err(|err| ProviderError::UnsupportedImage(format!("invalid image payload: {err}")))?;
	Ok(AnthropicImageSource::Base64 {
		media_type: mime,
		data: clean_b64,
	})
}

fn first_user_text(messages: &[Message]) -> String {
	for message in messages {
		if message.role != Role::User {
			continue;
		}
		if !message.content.is_empty() {
			return message.content.clone();
		}
		for part in &message.parts {
			if let ContentPart::Text { text } = part {
				return text.clone();
			}
		}
		return String::new();
	}
	String::new()
}

fn create_claude_billing_header(first_user_message_text: &str) -> String {
	let units: Vec<u16> = first_user_message_text.encode_utf16().collect();
	let key: String = [4usize, 7, 20]
		.iter()
		.map(|index| {
			units
				.get(*index)
				.copied()
				.and_then(|unit| char::from_u32(u32::from(unit)))
				.unwrap_or('0')
		})
		.collect();
	let digest = sha256_hex(format!("59cf53e54c78{key}{CLAUDE_CODE_VERSION}").as_bytes());
	format!(
		"x-anthropic-billing-header: cc_version={CLAUDE_CODE_VERSION}.{}; cc_entrypoint=claude-desktop; cch=00000;",
		&digest[..3]
	)
}

fn map_stainless_arch(arch: &str) -> String {
	match arch.to_ascii_lowercase().as_str() {
		"x86_64" | "amd64" => "x64".to_string(),
		"aarch64" | "arm64" => "arm64".to_string(),
		"x86" | "i686" => "x86".to_string(),
		other => format!("other::{other}"),
	}
}

fn sha256_hex(input: &[u8]) -> String {
	const K: [u32; 64] = [
		0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
		0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
		0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
		0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
		0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
		0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
		0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
		0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
	];
	let mut h: [u32; 8] = [
		0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
	];
	let mut message = input.to_vec();
	let bit_len = (message.len() as u64).wrapping_mul(8);
	message.push(0x80);
	while message.len() % 64 != 56 {
		message.push(0);
	}
	message.extend_from_slice(&bit_len.to_be_bytes());
	for chunk in message.chunks_exact(64) {
		let mut w = [0u32; 64];
		for index in 0..16 {
			w[index] = u32::from_be_bytes([
				chunk[index * 4],
				chunk[index * 4 + 1],
				chunk[index * 4 + 2],
				chunk[index * 4 + 3],
			]);
		}
		for index in 16..64 {
			let s0 = w[index - 15].rotate_right(7)
				^ w[index - 15].rotate_right(18)
				^ (w[index - 15] >> 3);
			let s1 = w[index - 2].rotate_right(17)
				^ w[index - 2].rotate_right(19)
				^ (w[index - 2] >> 10);
			w[index] = w[index - 16]
				.wrapping_add(s0)
				.wrapping_add(w[index - 7])
				.wrapping_add(s1);
		}
		let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
		for index in 0..64 {
			let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
			let ch = (e & f) ^ (!e & g);
			let temp1 = hh
				.wrapping_add(s1)
				.wrapping_add(ch)
				.wrapping_add(K[index])
				.wrapping_add(w[index]);
			let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
			let maj = (a & b) ^ (a & c) ^ (b & c);
			let temp2 = s0.wrapping_add(maj);
			hh = g;
			g = f;
			f = e;
			e = d.wrapping_add(temp1);
			d = c;
			c = b;
			b = a;
			a = temp1.wrapping_add(temp2);
		}
		h[0] = h[0].wrapping_add(a);
		h[1] = h[1].wrapping_add(b);
		h[2] = h[2].wrapping_add(c);
		h[3] = h[3].wrapping_add(d);
		h[4] = h[4].wrapping_add(e);
		h[5] = h[5].wrapping_add(f);
		h[6] = h[6].wrapping_add(g);
		h[7] = h[7].wrapping_add(hh);
	}
	h.iter().map(|word| format!("{word:08x}")).collect()
}

fn patch_cch_placeholder(body: &str) -> Option<String> {
	const PLACEHOLDER: &str = "cch=00000";
	let index = body.find(PLACEHOLDER)?;
	let hash = xxhash_rust::xxh64::xxh64(body.as_bytes(), CCH_SEED);
	let digits = format!("{:05x}", hash & 0xf_ffff);
	let start = index + "cch=".len();
	let mut patched = String::with_capacity(body.len());
	patched.push_str(&body[..start]);
	patched.push_str(&digits);
	patched.push_str(&body[start + 5..]);
	Some(patched)
}

fn random_uuid_v4() -> String {
	let mut bytes = [0u8; 16];
	let mut filled = false;
	if let Ok(mut file) = std::fs::File::open("/dev/urandom") {
		use std::io::Read;
		filled = file.read_exact(&mut bytes).is_ok();
	}
	if !filled {
		let mut state = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|duration| duration.as_nanos() as u64)
			.unwrap_or(0)
			^ u64::from(std::process::id());
		for byte in &mut bytes {
			state = state
				.wrapping_mul(6_364_136_223_846_793_005)
				.wrapping_add(1_442_695_040_888_963_407);
			*byte = (state >> 33) as u8;
		}
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x40;
	bytes[8] = (bytes[8] & 0x3f) | 0x80;
	format!(
		"{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
		bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
		bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
	)
}

fn ensure_max_tokens_for_thinking(
	current: u32,
	ceiling: u32,
	thinking: &mut Option<AnthropicThinkingConfig>,
) -> Result<u32, ProviderError> {
	let Some(AnthropicThinkingConfig::Enabled { budget_tokens, .. }) = thinking else {
		return Ok(current);
	};
	if *budget_tokens == 0 {
		return Ok(current);
	}
	let raised = current
		.max(budget_tokens.saturating_add(OUTPUT_FALLBACK_BUFFER))
		.min(ceiling);
	if budget_tokens.saturating_add(OUTPUT_FALLBACK_BUFFER) <= raised {
		return Ok(raised);
	}
	let clamped = raised.saturating_sub(OUTPUT_FALLBACK_BUFFER);
	if clamped == 0 {
		return Err(ProviderError::Protocol(format!(
			"Anthropic thinking budget requires max_tokens greater than {OUTPUT_FALLBACK_BUFFER}; got {raised}"
		)));
	}
	if let Some(AnthropicThinkingConfig::Enabled { budget_tokens, .. }) = thinking {
		*budget_tokens = clamped;
	}
	Ok(raised)
}

fn apply_prompt_caching(messages: &mut [AnthropicMessage], cache_control: &AnthropicCacheControl) {
	if messages.is_empty() {
		return;
	}
	let mut message_end = messages.len() - 1;
	if messages[message_end].role == "user"
		&& matches!(&messages[message_end].content, AnthropicContent::Text(text) if text.as_str() == "Continue.")
		&& message_end > 0
		&& messages[message_end - 1].role == "assistant"
	{
		message_end -= 1;
	}
	let start = message_end.saturating_sub(1);
	for index in (start..=message_end).rev() {
		let replacement = match &mut messages[index].content {
			AnthropicContent::Text(text) => {
				let text = std::mem::take(text);
				Some(AnthropicContent::Blocks(vec![AnthropicBlock::Text {
					text,
					cache_control: Some(cache_control.clone()),
				}]))
			},
			AnthropicContent::Blocks(blocks) => {
				apply_cache_control_to_last_block(blocks, cache_control);
				None
			},
		};
		if let Some(content) = replacement {
			messages[index].content = content;
		}
	}
}

fn apply_cache_control_to_last_block(
	blocks: &mut [AnthropicBlock],
	cache_control: &AnthropicCacheControl,
) -> bool {
	for block in blocks.iter_mut().rev() {
		let slot = match block {
			AnthropicBlock::Thinking { .. } | AnthropicBlock::RedactedThinking { .. } => continue,
			AnthropicBlock::Text { cache_control, .. }
			| AnthropicBlock::Image { cache_control, .. }
			| AnthropicBlock::ToolUse { cache_control, .. }
			| AnthropicBlock::ToolResult { cache_control, .. } => cache_control,
		};
		if slot.is_some() {
			return false;
		}
		*slot = Some(cache_control.clone());
		return true;
	}
	false
}

fn normalize_tool_schema(
	parameters: serde_json::Value,
	index: usize,
) -> Result<serde_json::Value, ProviderError> {
	let Some(mut object) = parameters.as_object().cloned() else {
		return Err(ProviderError::Protocol(format!(
			"tool at index {index} has a non-object input schema"
		)));
	};
	// TS `buildAnthropicBaseToolInputSchema` overrides these root fields.
	object.insert(
		"type".to_string(),
		serde_json::Value::String("object".to_string()),
	);
	if !object.get("properties").is_some_and(serde_json::Value::is_object) {
		object.insert(
			"properties".to_string(),
			serde_json::Value::Object(serde_json::Map::new()),
		);
	}
	let required = object
		.get("required")
		.and_then(serde_json::Value::as_array)
		.map(|entries| {
			serde_json::Value::Array(
				entries
					.iter()
					.filter_map(serde_json::Value::as_str)
					.map(|entry| serde_json::Value::String(entry.to_string()))
					.collect(),
			)
		})
		.unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
	object.insert("required".to_string(), required);
	Ok(normalize_anthropic_schema_node(
		serde_json::Value::Object(object),
		true,
	))
}

fn strict_tool_schema(
	schema: serde_json::Value,
	optional_budget: usize,
) -> Option<(serde_json::Value, usize)> {
	let mut schema = schema;
	let mut optional = 0usize;
	if !make_strict_schema(&mut schema, &mut optional, optional_budget) {
		return None;
	}
	Some((schema, optional))
}

fn make_strict_schema(
	value: &mut serde_json::Value,
	optional: &mut usize,
	optional_budget: usize,
) -> bool {
	let Some(object) = value.as_object_mut() else {
		return true;
	};
	if object.contains_key("$ref") || object.contains_key("anyOf") || object.contains_key("oneOf") {
		return false;
	}
	let mut required: Vec<String> = object
		.get("required")
		.and_then(serde_json::Value::as_array)
		.map(|entries| {
			entries
				.iter()
				.filter_map(serde_json::Value::as_str)
				.map(str::to_string)
				.collect()
		})
		.unwrap_or_default();
	let mut had_properties = false;
	if let Some(properties) = object.get_mut("properties").and_then(serde_json::Value::as_object_mut) {
		had_properties = true;
		let keys: Vec<String> = properties.keys().cloned().collect();
		for key in keys {
			let Some(property) = properties.get_mut(&key) else {
				continue;
			};
			if !make_strict_schema(property, optional, optional_budget) {
				return false;
			}
			if !required.iter().any(|entry| entry == &key) {
				*optional += 1;
				if *optional > optional_budget {
					return false;
				}
				make_schema_nullable(property);
				required.push(key);
			}
		}
	}
	if had_properties {
		object.insert(
			"required".to_string(),
			serde_json::Value::Array(
				required
					.into_iter()
					.map(serde_json::Value::String)
					.collect(),
			),
		);
		object.insert(
			"additionalProperties".to_string(),
			serde_json::Value::Bool(false),
		);
	}
	if let Some(items) = object.get_mut("items") {
		if let serde_json::Value::Array(entries) = items {
			for entry in entries {
				if !make_strict_schema(entry, optional, optional_budget) {
					return false;
				}
			}
		} else if !make_strict_schema(items, optional, optional_budget) {
			return false;
		}
	}
	true
}

fn make_schema_nullable(value: &mut serde_json::Value) {
	let Some(object) = value.as_object_mut() else {
		return;
	};
	let replace = match object.get("type") {
		Some(serde_json::Value::String(kind)) if kind != "null" => {
			Some(serde_json::Value::Array(vec![
				serde_json::Value::String(kind.clone()),
				serde_json::Value::String("null".to_string()),
			]))
		},
		_ => None,
	};
	if let Some(replacement) = replace {
		object.insert("type".to_string(), replacement);
		return;
	}
	if let Some(serde_json::Value::Array(kinds)) = object.get_mut("type") {
		if !kinds.iter().any(|kind| kind.as_str() == Some("null")) {
			kinds.push(serde_json::Value::String("null".to_string()));
		}
	}
}

fn normalize_anthropic_schema_node(schema: serde_json::Value, is_root: bool) -> serde_json::Value {
	match schema {
		serde_json::Value::Array(entries) => serde_json::Value::Array(
			entries
				.into_iter()
				.map(|entry| normalize_anthropic_schema_node(entry, false))
				.collect(),
		),
		serde_json::Value::Object(mut object) => {
			let scalar_type = anthropic_effective_scalar_type(&object).map(str::to_string);
			let mut kept = serde_json::Map::new();
			let mut spilled: Vec<(String, serde_json::Value)> = Vec::new();
			for (key, value) in std::mem::take(&mut object) {
				let root_combinator =
					is_root && matches!(key.as_str(), "anyOf" | "allOf" | "oneOf");
				if !root_combinator
					&& (ANTHROPIC_SCHEMA_UNIVERSAL_KEEP.contains(&key.as_str())
						|| anthropic_per_type_keep(scalar_type.as_deref(), &key))
				{
					kept.insert(key, value);
				} else {
					spilled.push((key, value));
				}
			}

			if scalar_type.as_deref() == Some("string") {
				if let Some(format) = kept.get("format").and_then(serde_json::Value::as_str) {
					if !ANTHROPIC_STRING_FORMATS.contains(&format) {
						let value = kept.remove("format").expect("just read");
						spilled.push(("format".to_string(), value));
					}
				}
			}
			if scalar_type.as_deref() == Some("array") {
				if let Some(min_items) = kept.get("minItems").and_then(serde_json::Value::as_u64) {
					if min_items != 0 && min_items != 1 {
						let value = kept.remove("minItems").expect("just read");
						spilled.push(("minItems".to_string(), value));
					}
				}
			}
			if scalar_type.as_deref() == Some("object") && !kept.contains_key("additionalProperties") {
				kept.insert(
					"additionalProperties".to_string(),
					serde_json::Value::Bool(false),
				);
			}

			if let Some(properties) = kept.get_mut("properties").and_then(serde_json::Value::as_object_mut) {
				for value in properties.values_mut() {
					*value = normalize_anthropic_schema_node(value.clone(), false);
				}
			}
			if let Some(additional) = kept.get_mut("additionalProperties") {
				if additional.is_object() {
					let normalized = normalize_anthropic_schema_node(additional.clone(), false);
					if normalized.as_object().is_some_and(serde_json::Map::is_empty) {
						*additional = serde_json::Value::Bool(true);
					} else {
						*additional = normalized;
					}
				}
			}
			for key in ["items", "prefixItems"] {
				if let Some(value) = kept.get_mut(key) {
					*value = normalize_anthropic_schema_node(value.clone(), false);
				}
			}
			for key in ["anyOf", "allOf", "oneOf"] {
				if let Some(value) = kept.get_mut(key) {
					*value = normalize_anthropic_schema_node(value.clone(), false);
				}
			}
			for key in ["$defs", "definitions"] {
				if let Some(serde_json::Value::Object(definitions)) = kept.get_mut(key) {
					for value in definitions.values_mut() {
						*value = normalize_anthropic_schema_node(value.clone(), false);
					}
				}
			}
			spill_into_description(&mut kept, spilled);
			serde_json::Value::Object(kept)
		},
		other => other,
	}
}

const ANTHROPIC_SCHEMA_UNIVERSAL_KEEP: [&str; 13] = [
	"$ref",
	"$defs",
	"$schema",
	"definitions",
	"type",
	"anyOf",
	"allOf",
	"enum",
	"const",
	"description",
	"title",
	"default",
	"nullable",
];

const ANTHROPIC_STRING_FORMATS: [&str; 10] = [
	"date-time", "time", "date", "duration", "email", "hostname", "uri", "ipv4", "ipv6", "uuid",
];

fn anthropic_per_type_keep(scalar_type: Option<&str>, key: &str) -> bool {
	match scalar_type {
		Some("object") => matches!(key, "properties" | "required" | "additionalProperties"),
		Some("array") => matches!(key, "items" | "prefixItems" | "minItems"),
		Some("string") => key == "format",
		_ => false,
	}
}

fn anthropic_effective_scalar_type(object: &serde_json::Map<String, serde_json::Value>) -> Option<&str> {
	match object.get("type") {
		Some(serde_json::Value::String(value)) => Some(value.as_str()),
		Some(serde_json::Value::Array(entries)) => entries
			.iter()
			.filter_map(serde_json::Value::as_str)
			.find(|entry| *entry != "null"),
		_ => {
			if object.get("properties").is_some_and(serde_json::Value::is_object) {
				Some("object")
			} else if object.contains_key("items") || object.contains_key("prefixItems") {
				Some("array")
			} else {
				None
			}
		},
	}
}

fn spill_into_description(
	object: &mut serde_json::Map<String, serde_json::Value>,
	spilled: Vec<(String, serde_json::Value)>,
) {
	if spilled.is_empty() {
		return;
	}
	let formatted = format!(
		"{{{}}}",
		spilled
			.iter()
			.map(|(key, value)| format!(
				"{key}: {}",
				serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
			))
			.collect::<Vec<_>>()
			.join(", ")
	);
	let existing = object
		.get("description")
		.and_then(serde_json::Value::as_str)
		.unwrap_or("");
	let description = if existing.is_empty() {
		formatted
	} else {
		format!("{existing}\n\n{formatted}")
	};
	object.insert(
		"description".to_string(),
		serde_json::Value::String(description),
	);
}

fn dedupe(values: Vec<String>) -> Vec<String> {
	let mut seen = std::collections::HashSet::new();
	let mut out = Vec::with_capacity(values.len());
	for value in values {
		let trimmed = value.trim();
		if trimmed.is_empty() {
			continue;
		}
		if seen.insert(trimmed.to_string()) {
			out.push(trimmed.to_string());
		}
	}
	out
}

fn parse_header_name(name: &str) -> Result<reqwest::header::HeaderName, ProviderError> {
	reqwest::header::HeaderName::from_bytes(name.as_bytes())
		.map_err(|_| ProviderError::Network(format!("invalid header name `{name}`")))
}

fn is_enforced_header(lower: &str) -> bool {
	matches!(
		lower,
		"accept"
			| "accept-encoding"
			| "connection"
			| "content-type"
			| "anthropic-version"
			| "anthropic-dangerous-direct-browser-access"
			| "anthropic-beta"
			| "user-agent"
			| "x-app"
			| "authorization"
			| "x-api-key"
			| "x-claude-code-session-id"
			| "x-client-request-id"
			| "cf-aig-authorization"
			| "x-stainless-arch"
			| "x-stainless-lang"
			| "x-stainless-os"
			| "x-stainless-package-version"
			| "x-stainless-retry-count"
			| "x-stainless-runtime"
			| "x-stainless-runtime-version"
			| "x-stainless-timeout"
	)
}

fn is_overridable_header(lower: &str) -> bool {
	matches!(
		lower,
		"anthropic-beta"
			| "user-agent"
			| "x-app"
			| "x-stainless-arch"
			| "x-stainless-lang"
			| "x-stainless-os"
			| "x-stainless-package-version"
			| "x-stainless-retry-count"
			| "x-stainless-runtime"
			| "x-stainless-runtime-version"
			| "x-stainless-timeout"
	)
}

fn insert_header(
	headers: &mut reqwest::header::HeaderMap,
	name: reqwest::header::HeaderName,
	value: &str,
) -> Result<(), ProviderError> {
	let value = reqwest::header::HeaderValue::from_str(value).map_err(|_| {
		ProviderError::Network(format!("invalid value for header {}", name.as_str()))
	})?;
	headers.insert(name, value);
	Ok(())
}

/// TS `matchesFastModeUnsupported` (`packages/ai/src/error/flags.ts`): a 400
/// that rejects the `speed` parameter, or a 429 because the account lacks the
/// fast-mode rate-limit entitlement.
fn is_fast_mode_unsupported(status: u16, message: &str) -> bool {
	let lower = message.to_ascii_lowercase();
	if status == 400 {
		return lower.contains("invalid_request_error")
			&& contains_word(&lower, "speed")
			&& lower.contains("not support");
	}
	status == 429 && lower.contains("rate_limit_error") && lower.contains("fast mode")
}

/// `\bspeed\b` over an already-lowercased message: never matches `speedy` or
/// `speed_limit`.
fn contains_word(text: &str, word: &str) -> bool {
	let mut offset = 0;
	while let Some(position) = text[offset..].find(word) {
		let start = offset + position;
		let end = start + word.len();
		let before = text[..start].chars().next_back();
		let after = text[end..].chars().next();
		let boundary = |ch: Option<char>| ch.is_none_or(|ch| !ch.is_alphanumeric() && ch != '_');
		if boundary(before) && boundary(after) {
			return true;
		}
		offset = end;
	}
	false
}

/// Annotate the terminal chunk with the features this turn dropped. Errors and
/// non-terminal chunks pass through untouched.
fn with_disabled_features(
	stream: ChunkStream,
	features: Option<Vec<String>>,
) -> ChunkStream {
	let Some(features) = features else {
		return stream;
	};
	Box::pin(stream.map(move |item| {
		item.map(|mut chunk| {
			if chunk.is_done {
				chunk.disabled_features = Some(features.clone());
			}
			chunk
		})
	}))
}

/// Metadata-only pre-output records are coalesced into one chunk: usage
/// snapshots are cumulative, so the newest one wins whole; the remaining
/// fields keep their latest present value. No record is dropped silently.
fn merge_metadata(into: &mut StreamChunk, chunk: StreamChunk) {
	if chunk.content_index.is_some() {
		into.content_index = chunk.content_index;
	}
	if chunk.usage.is_some() {
		into.usage = chunk.usage;
	}
	if chunk.stop_reason.is_some() {
		into.stop_reason = chunk.stop_reason;
	}
	if chunk.error_message.is_some() {
		into.error_message = chunk.error_message;
	}
	if chunk.disabled_features.is_some() {
		into.disabled_features = chunk.disabled_features;
	}
}

/// TS `matchesFastModeUnsupported` requires a concrete 400/429 status. An SSE
/// `error` event carries no status, so it is surfaced, never retried.
fn stream_error_is_fast_mode_unsupported(error: &ProviderError) -> bool {
	match error {
		ProviderError::HttpStatus(status, text) => is_fast_mode_unsupported(*status, text),
		_ => false,
	}
}

fn should_retry_response(response: &reqwest::Response) -> bool {
	if let Some(value) = response.headers().get("x-should-retry") {
		if value.as_bytes() == b"true" {
			return true;
		}
		if value.as_bytes() == b"false" {
			return false;
		}
	}
	let status = response.status().as_u16();
	matches!(status, 408 | 409 | 429 | 500..=599)
}

fn retry_delay(response: &reqwest::Response, attempt: u32) -> Option<Duration> {
	let headers = response.headers();
	if let Some(value) = headers.get("retry-after-ms") {
		if let Ok(text) = value.to_str() {
			if let Ok(ms) = text.trim().parse::<u64>() {
				// TS declines an over-cap server-directed wait instead of
				// sleeping past the configured budget.
				return (ms <= MAX_RETRY_DELAY_MS).then(|| Duration::from_millis(ms));
			}
		}
	}
	if let Some(value) = headers.get("retry-after") {
		if let Ok(text) = value.to_str() {
			if let Ok(seconds) = text.trim().parse::<u64>() {
				let ms = seconds.saturating_mul(1000);
				return (ms <= MAX_RETRY_DELAY_MS).then(|| Duration::from_millis(ms));
			}
			if let Some(target) = parse_http_date_epoch(text.trim()) {
				let now = std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.map(|duration| duration.as_secs())
					.unwrap_or(0);
				let seconds = target.saturating_sub(now);
				let ms = seconds.saturating_mul(1000);
				return (ms <= MAX_RETRY_DELAY_MS).then(|| Duration::from_millis(ms));
			}
		}
	}
	Some(retry_delay_from_attempt(attempt))
}

fn parse_http_date_epoch(value: &str) -> Option<u64> {
	// IMF-fixdate: `Sun, 06 Nov 1994 08:49:37 GMT`.
	let parts: Vec<&str> = value.split_whitespace().collect();
	if parts.len() != 6 || !parts[5].eq_ignore_ascii_case("GMT") {
		return None;
	}
	let day: u32 = parts[1].parse().ok()?;
	let month = match parts[2].to_ascii_lowercase().as_str() {
		"jan" => 1,
		"feb" => 2,
		"mar" => 3,
		"apr" => 4,
		"may" => 5,
		"jun" => 6,
		"jul" => 7,
		"aug" => 8,
		"sep" => 9,
		"oct" => 10,
		"nov" => 11,
		"dec" => 12,
		_ => return None,
	};
	let year: i32 = parts[3].parse().ok()?;
	let time: Vec<&str> = parts[4].split(':').collect();
	if time.len() != 3 {
		return None;
	}
	let hour: u64 = time[0].parse().ok()?;
	let minute: u64 = time[1].parse().ok()?;
	let second: u64 = time[2].parse().ok()?;
	let days = days_from_civil(year, month, day);
	let seconds = days
		.checked_mul(86_400)?
		.checked_add((hour * 3_600 + minute * 60 + second) as i64)?;
	u64::try_from(seconds).ok()
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
	let year = year - i32::from(month <= 2);
	let era = if year >= 0 { year } else { year - 399 } / 400;
	let year_of_era = year - era * 400;
	let month_prime = if month > 2 { month - 3 } else { month + 9 };
	let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
	let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year as i32;
	i64::from(era * 146_097 + day_of_era - 719_468)
}

fn retry_delay_from_attempt(attempt: u32) -> Duration {
	let base_ms = 500u64.saturating_mul(1 << attempt.min(4)).min(8_000);
	let nanos = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map(|duration| duration.subsec_nanos())
		.unwrap_or(0);
	let jitter = 1.0 - (f64::from(nanos % 1000) / 1000.0) * 0.25;
	Duration::from_millis((base_ms as f64 * jitter) as u64)
}

fn network_error(err: reqwest::Error) -> ProviderError {
	ProviderError::Network(err.to_string())
}

/// Every credential value this provider can actually put on the wire.
///
/// Custom `Authorization`/`x-api-key` headers may differ from the configured
/// API key; a gateway echoing either must not leak it through diagnostics.
fn credential_values(api_key: &str, custom_headers: &[(String, String)]) -> Vec<String> {
	let mut values: Vec<String> = Vec::new();
	let mut push = |value: &str| {
		let value = value.trim();
		if value.is_empty() || values.iter().any(|existing| existing == value) {
			return;
		}
		values.push(value.to_string());
	};
	push(api_key);
	for (name, value) in custom_headers {
		if !matches!(name.to_ascii_lowercase().as_str(), "authorization" | "x-api-key") {
			continue;
		}
		push(value);
		// Scheme spelling varies by gateway ("Bearer"/"BEARER"). The credential
		// is the token after the scheme; collect it independently of its casing.
		let trimmed = value.trim();
		if let Some(index) = trimmed.find(char::is_whitespace) {
			push(trimmed[index..].trim());
		}
	}
	// Longest first so a bearer token is replaced before a shorter overlap.
	values.sort_by_key(|value| std::cmp::Reverse(value.len()));
	values
}

/// Redact before bounding so a credential cut by the character limit cannot
/// survive as a prefix; `truncated` marks a byte-capped read.
fn bounded_redaction(text: &str, secrets: &[String], truncated: bool) -> String {
	let mut bounded: String = redact_with(text, secrets)
		.chars()
		.take(ERROR_BODY_CHARS)
		.collect();
	if truncated {
		bounded.push_str("\n[truncated]");
	}
	bounded
}

/// Best effort for byte-capped reads: drop the incomplete trailing token so a
/// credential cut mid-token is unlikely to survive. Diagnostics stay bounded
/// and redacted regardless; this is a heuristic, not a prefix guarantee.
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

struct BoundedBody {
	text: String,
	truncated: bool,
}

/// Read at most `limit` body bytes; never grows past the ceiling even for an
/// endless or huge response.
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

// ─── Response / usage ───────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
struct AnthropicUsage {
	input_tokens: Option<u64>,
	output_tokens: Option<u64>,
	cache_read_input_tokens: Option<u64>,
	cache_creation_input_tokens: Option<u64>,
	#[serde(default)]
	cache_creation: Option<AnthropicCacheCreation>,
	#[serde(default)]
	server_tool_use: Option<AnthropicServerToolUse>,
}

#[derive(Debug, Default, Deserialize)]
struct AnthropicCacheCreation {
	ephemeral_5m_input_tokens: Option<u64>,
	ephemeral_1h_input_tokens: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct AnthropicServerToolUse {
	web_search_requests: Option<u64>,
	web_fetch_requests: Option<u64>,
}

impl AnthropicUsage {
	fn has_any(&self) -> bool {
		self.input_tokens.is_some()
			|| self.output_tokens.is_some()
			|| self.cache_read_input_tokens.is_some()
			|| self.cache_creation_input_tokens.is_some()
			|| self.cache_creation.is_some()
			|| self.server_tool_use.is_some()
	}

	fn apply(&self, usage: &mut TokenUsage) {
		if let Some(value) = self.input_tokens {
			usage.input = value;
		}
		if let Some(value) = self.output_tokens {
			usage.output = value;
		}
		if let Some(value) = self.cache_read_input_tokens {
			usage.cache_read = value;
		}
		if let Some(value) = self.cache_creation_input_tokens {
			usage.cache_write = value;
		}
		if let Some(cache_creation) = &self.cache_creation {
			let five_minute = cache_creation.ephemeral_5m_input_tokens.unwrap_or(0);
			let one_hour = cache_creation.ephemeral_1h_input_tokens.unwrap_or(0);
			usage.cttl = (five_minute > 0 || one_hour > 0).then(|| CacheTtlUsage {
				ephemeral_5m: (five_minute > 0).then_some(five_minute),
				ephemeral_1h: (one_hour > 0).then_some(one_hour),
			});
		}
		if let Some(server_tool_use) = &self.server_tool_use {
			let web_search = server_tool_use.web_search_requests.unwrap_or(0);
			let web_fetch = server_tool_use.web_fetch_requests.unwrap_or(0);
			usage.server = (web_search > 0 || web_fetch > 0).then(|| ServerToolUsage {
				web_search: (web_search > 0).then_some(web_search),
				web_fetch: (web_fetch > 0).then_some(web_fetch),
			});
		}
		usage.total_tokens =
			usage.input + usage.output + usage.cache_read + usage.cache_write;
	}

	fn into_token_usage(self) -> TokenUsage {
		let mut usage = TokenUsage::default();
		self.apply(&mut usage);
		usage
	}
}

#[derive(Debug, Deserialize)]
struct AnthropicResponse {
	#[serde(default)]
	content: Vec<AnthropicResponseBlock>,
	#[serde(default)]
	usage: Option<AnthropicUsage>,
	#[serde(default)]
	stop_reason: Option<String>,
	#[serde(default)]
	stop_details: Option<StopDetails>,
}

#[derive(Debug, Deserialize)]
struct StopDetails {
	#[serde(rename = "type")]
	kind: String,
	#[serde(default)]
	category: Option<String>,
	#[serde(default)]
	explanation: Option<String>,
}

/// TS `mapStopReason`: unknown server-side reasons degrade to a normal stop.
fn map_stop_reason(reason: &str) -> StopReason {
	match reason {
		"max_tokens" | "model_context_window_exceeded" => StopReason::Length,
		"tool_use" => StopReason::ToolUse,
		"refusal" | "sensitive" => StopReason::Error,
		_ => StopReason::Stop,
	}
}

fn stop_error_message(
	raw: Option<&str>,
	details: Option<&StopDetails>,
	secrets: &[String],
) -> String {
	let bound = |text: String| -> String {
		redact_with(&text, secrets)
			.chars()
			.take(ERROR_BODY_CHARS)
			.collect()
	};
	if let Some(details) = details {
		if details.kind == "refusal" {
			let label = match details
				.category
				.as_deref()
				.map(str::trim)
				.filter(|value| !value.is_empty())
			{
				Some(category) => format!("Refusal ({})", redact_with(category, secrets)),
				None => "Refusal".to_string(),
			};
			return match details
				.explanation
				.as_deref()
				.map(str::trim)
				.filter(|value| !value.is_empty())
			{
				Some(explanation) => {
					bound(format!("{label}: {}", redact_with(explanation, secrets)))
				},
				None => bound(label),
			};
		}
	}
	bound(match raw {
		Some("refusal") => "Refusal (no details provided)".to_string(),
		Some("sensitive") => "Content flagged by safety filters".to_string(),
		Some(reason) => format!("Anthropic stream ended with stop_reason: {reason}"),
		None => "Anthropic stream ended with stop_reason: unknown".to_string(),
	})
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AnthropicResponseBlock {
	Text {
		#[serde(default)]
		text: String,
	},
	Thinking {
		#[serde(default)]
		thinking: String,
		#[serde(default)]
		signature: Option<String>,
	},
	RedactedThinking {
		#[serde(default)]
		data: String,
	},
	ToolUse {
		id: String,
		name: String,
		#[serde(default)]
		input: serde_json::Value,
	},
	#[serde(other)]
	Unknown,
}

// ─── SSE stream ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum BlockKind {
	Text,
	Thinking,
	RedactedThinking,
	Tool(usize),
	Ignored,
}

#[derive(Default)]
struct StreamState {
	usage: TokenUsage,
	usage_reported: bool,
	started: bool,
	terminal: bool,
	terminal_reason: Option<StopReason>,
	terminal_error: Option<String>,
	blocks: HashMap<u64, BlockKind>,
	/// Signature fragments per thinking content block; emitted once, finalized,
	/// at `content_block_stop` so split deltas cannot race a shared append path.
	signatures: HashMap<u64, String>,
	next_tool_index: usize,
}

struct SseRecord {
	event: Option<String>,
	data: String,
}

/// Parse Anthropic SSE events into provider chunks.
///
/// Unlike the OpenAI dialect the event type is carried by the `event:` field
/// and the JSON envelope; both are used, and unknown event names are ignored
/// without inventing content. Malformed JSON in a known event is a protocol
/// error, and EOF before `message_start` / a terminal stop signal is a
/// truncated stream.
fn anthropic_sse_stream<S, B, E>(
	body: S,
	secrets: Vec<String>,
	first_event_timeout: Option<Duration>,
	idle_timeout: Option<Duration>,
) -> ChunkStream
where
	S: Stream<Item = Result<B, E>> + Send + Unpin + 'static,
	B: AsRef<[u8]> + Send + 'static,
	E: Display + Send + 'static,
{
	struct State<S> {
		body: S,
		buf: Vec<u8>,
		/// Bytes after the last newline currently buffered; checked before any
		/// segment is copied so a completed line cannot exceed the ceiling.
		pending_line: usize,
		event: Option<String>,
		data: String,
		pending: VecDeque<Result<StreamChunk, ProviderError>>,
		done: bool,
		eof: bool,
		saw_record: bool,
		stream: StreamState,
		secrets: Vec<String>,
		/// Absolute instant by which the next complete record must arrive; reset
		/// on a record boundary, never on a raw transport chunk.
		deadline: Option<tokio::time::Instant>,
		idle_timeout: Option<Duration>,
	}

	futures::stream::unfold(
		State {
			body,
			buf: Vec::new(),
			pending_line: 0,
			event: None,
			data: String::new(),
			pending: VecDeque::new(),
			done: false,
			eof: false,
			saw_record: false,
			stream: StreamState::default(),
			secrets,
			deadline: first_event_timeout
				.map(|budget| tokio::time::Instant::now() + budget),
			idle_timeout,
		},
		|mut st| async move {
			loop {
				if let Some(item) = st.pending.pop_front() {
					let terminal = item.as_ref().map(|chunk| chunk.is_done).unwrap_or(true);
					if terminal {
						st.done = true;
					}
					return Some((item, st));
				}
				if st.done {
					return None;
				}
				if st.eof {
					st.done = true;
					if !st.stream.started {
						return Some((
							Err(ProviderError::Protocol(
								"Anthropic stream ended before message_start".to_string(),
							)),
							st,
						));
					}
					if !st.stream.terminal {
						return Some((
							Err(ProviderError::Protocol(
								"Anthropic stream ended before message_stop".to_string(),
							)),
							st,
						));
					}
					let usage = st.stream.usage_reported.then_some(st.stream.usage);
					let mut chunk = empty_chunk();
					chunk.usage = usage;
					chunk.is_done = true;
					chunk.stop_reason = st.stream.terminal_reason.clone();
					chunk.error_message = st.stream.terminal_error.clone();
					return Some((Ok(chunk), st));
				}

				if let Some(pos) = st.buf.iter().position(|byte| *byte == b'\n') {
					let line: Vec<u8> = st.buf.drain(..=pos).collect();
					let line = String::from_utf8_lossy(&line);
					let line = line.trim_end_matches(|c| c == '\r' || c == '\n');
					if line.is_empty() {
						if let Some(record) = take_record(&mut st.event, &mut st.data) {
							// Only a complete non-ping record is progress: resetting
							// on transport bytes would let a drip extend the budget.
							if record.event.as_deref() != Some("ping") {
								st.saw_record = true;
								st.deadline = st
									.idle_timeout
									.map(|budget| tokio::time::Instant::now() + budget);
							}
							let results = handle_event(&record, &mut st.stream, &st.secrets);
							st.pending.extend(results);
						}
						continue;
					}
					if line.starts_with(':') {
						continue;
					}
					if let Some(value) = line.strip_prefix("event:") {
						st.event = Some(value.trim().to_string());
					} else if let Some(value) = line.strip_prefix("data:") {
						let value = value.trim_start();
						let separator = usize::from(!st.data.is_empty());
						if st.data.len() + separator + value.len() > MAX_SSE_EVENT_BYTES {
							st.done = true;
							return Some((
								Err(ProviderError::Protocol(format!(
									"Anthropic stream event exceeded {MAX_SSE_EVENT_BYTES} bytes"
								))),
								st,
							));
						}
						if separator == 1 {
							st.data.push('\n');
						}
						st.data.push_str(value);
					}
					continue;
				}

				let item = match st.deadline {
					Some(deadline) => match tokio::time::timeout_at(deadline, st.body.next()).await {
						Ok(item) => item,
						Err(_) => {
							st.done = true;
							let message = if st.saw_record {
								"Anthropic stream stalled while waiting for the next event"
							} else {
								"Anthropic stream timed out while waiting for the first event"
							};
							return Some((Err(ProviderError::Network(message.to_string())), st));
						},
					},
					None => st.body.next().await,
				};
				match item {
					Some(Ok(bytes)) => {
						// Validate every segment before copying it: a chunk carrying a
						// newline must not smuggle a complete line past the ceiling.
						let mut rest = bytes.as_ref();
						loop {
							let newline = rest.iter().position(|byte| *byte == b'\n');
							let (segment, tail) = match newline {
								Some(position) => (&rest[..position], &rest[position + 1..]),
								None => (rest, &rest[rest.len()..]),
							};
							if st.pending_line + segment.len() > MAX_SSE_LINE_BYTES {
								st.done = true;
								return Some((
									Err(ProviderError::Protocol(format!(
										"Anthropic stream line exceeded {MAX_SSE_LINE_BYTES} bytes"
									))),
									st,
								));
							}
							st.buf.extend_from_slice(segment);
							st.pending_line += segment.len();
							if newline.is_none() {
								break;
							}
							st.buf.push(b'\n');
							st.pending_line = 0;
							rest = tail;
							if rest.is_empty() {
								break;
							}
						}
					},
					Some(Err(err)) => {
						st.done = true;
						return Some((Err(ProviderError::Network(err.to_string())), st));
					},
					None => {
						st.eof = true;
						if let Some(record) = take_record(&mut st.event, &mut st.data) {
							// Only a complete non-ping record is progress: resetting
							// on transport bytes would let a drip extend the budget.
							if record.event.as_deref() != Some("ping") {
								st.saw_record = true;
								st.deadline = st
									.idle_timeout
									.map(|budget| tokio::time::Instant::now() + budget);
							}
							let results = handle_event(&record, &mut st.stream, &st.secrets);
							st.pending.extend(results);
						}
						continue;
					},
				}
			}
		},
	)
	.boxed()
}

fn take_record(event: &mut Option<String>, data: &mut String) -> Option<SseRecord> {
	if data.is_empty() {
		*event = None;
		return None;
	}
	let record = SseRecord {
		event: event.take(),
		data: std::mem::take(data),
	};
	Some(record)
}

fn handle_event(
	record: &SseRecord,
	state: &mut StreamState,
	secrets: &[String],
) -> Vec<Result<StreamChunk, ProviderError>> {
	if record.event.as_deref() == Some("ping") {
		return Vec::new();
	}
	let parsed: serde_json::Value = match serde_json::from_str(&record.data) {
		Ok(value) => value,
		Err(err) => {
			let known = match record.event.as_deref() {
				None => true,
				Some(event) => is_known_event(event),
			};
			return if known {
				vec![Err(ProviderError::Protocol(format!(
					"invalid Anthropic stream event JSON: {err}"
				)))]
			} else {
				Vec::new()
			};
		},
	};
	let kind = parsed
		.get("type")
		.and_then(serde_json::Value::as_str)
		.or(record.event.as_deref())
		.unwrap_or("");
	if record.event.as_deref() == Some("error") || kind == "error" {
		return vec![Err(stream_error(&parsed, secrets))];
	}
	if !is_known_event(kind) {
		return Vec::new();
	}
	if !state.started && kind != "message_start" {
		return vec![Err(ProviderError::Protocol(format!(
			"received {} before message_start",
			redact_with(kind, secrets)
		)))];
	}

	match kind {
		"message_start" => {
			if state.started {
				return Vec::new();
			}
			state.started = true;
			let usage = parsed
				.get("message")
				.and_then(|message| message.get("usage"))
				.and_then(|usage| serde_json::from_value::<AnthropicUsage>(usage.clone()).ok());
			if let Some(usage) = usage.filter(AnthropicUsage::has_any) {
				usage.apply(&mut state.usage);
				state.usage_reported = true;
				let mut chunk = empty_chunk();
				chunk.usage = Some(state.usage);
				vec![Ok(chunk)]
			} else {
				Vec::new()
			}
		},
		"content_block_start" => {
			let Some(index) = parsed.get("index").and_then(serde_json::Value::as_u64) else {
				return Vec::new();
			};
			let Some(block) = parsed.get("content_block") else {
				return Vec::new();
			};
			let Some(block_type) = block.get("type").and_then(serde_json::Value::as_str) else {
				return Vec::new();
			};
			match block_type {
				"text" => {
					state.blocks.insert(index, BlockKind::Text);
					block
						.get("text")
						.and_then(serde_json::Value::as_str)
						.filter(|text| !text.is_empty())
						.map(|text| vec![Ok(content_chunk(text, index))])
						.unwrap_or_default()
				},
				"thinking" => {
					state.blocks.insert(index, BlockKind::Thinking);
					// Open the indexed thinking block before any signature_delta;
					// the assembler rejects a signature without a block.
					let thinking = block
						.get("thinking")
						.and_then(serde_json::Value::as_str)
						.unwrap_or("");
					vec![Ok(reasoning_chunk(thinking, index))]
				},
				"redacted_thinking" => {
					state.blocks.insert(index, BlockKind::RedactedThinking);
					block
						.get("data")
						.and_then(serde_json::Value::as_str)
						.map(|data| vec![Ok(redacted_chunk(data, index))])
						.unwrap_or_default()
				},
				"tool_use" => {
					let Some(id) = block.get("id").and_then(serde_json::Value::as_str) else {
						state.blocks.insert(index, BlockKind::Ignored);
						return Vec::new();
					};
					let Some(name) = block.get("name").and_then(serde_json::Value::as_str) else {
						state.blocks.insert(index, BlockKind::Ignored);
						return Vec::new();
					};
					let tool_index = state.next_tool_index;
					state.next_tool_index += 1;
					state.blocks.insert(index, BlockKind::Tool(tool_index));
					let arguments_delta = block
						.get("input")
						.filter(|input| {
							!input.is_null()
								&& input
									.as_object()
									.map(|object| !object.is_empty())
									.unwrap_or(true)
						})
						.and_then(|input| serde_json::to_string(input).ok());
					let mut chunk = empty_chunk();
					chunk.content_index = Some(index as usize);
					chunk.tool_call_deltas = vec![ToolCallDelta {
						index: tool_index,
						id: Some(id.to_string()),
						name: Some(name.to_string()),
						arguments_delta,
						content_index: Some(index as usize),
					}];
					vec![Ok(chunk)]
				},
				_ => {
					state.blocks.insert(index, BlockKind::Ignored);
					Vec::new()
				},
			}
		},
		"content_block_delta" => {
			let Some(index) = parsed.get("index").and_then(serde_json::Value::as_u64) else {
				return Vec::new();
			};
			let Some(kind) = state.blocks.get(&index).copied() else {
				return Vec::new();
			};
			let Some(delta) = parsed.get("delta") else {
				return Vec::new();
			};
			let Some(delta_type) = delta.get("type").and_then(serde_json::Value::as_str) else {
				return Vec::new();
			};
			match (kind, delta_type) {
				(BlockKind::Text, "text_delta") => delta
					.get("text")
					.and_then(serde_json::Value::as_str)
					.filter(|text| !text.is_empty())
					.map(|text| vec![Ok(content_chunk(text, index))])
					.unwrap_or_default(),
				(BlockKind::Thinking, "thinking_delta") => delta
					.get("thinking")
					.and_then(serde_json::Value::as_str)
					.filter(|text| !text.is_empty())
					.map(|text| vec![Ok(reasoning_chunk(text, index))])
					.unwrap_or_default(),
				(BlockKind::Thinking, "signature_delta") => {
					match delta
						.get("signature")
						.and_then(serde_json::Value::as_str)
						.filter(|signature| !signature.is_empty())
					{
						// Deltas are fragments of one signature: concatenate here
						// and emit the finished value once at `content_block_stop`.
						Some(signature) => {
							let slot = state.signatures.entry(index).or_default();
							if slot.len() + signature.len() > MAX_SSE_LINE_BYTES {
								vec![Err(ProviderError::Protocol(format!(
									"Anthropic thinking signature exceeded {MAX_SSE_LINE_BYTES} bytes"
								)))]
							} else {
								slot.push_str(signature);
								Vec::new()
							}
						},
						None => Vec::new(),
					}
				},
				(BlockKind::Tool(tool_index), "input_json_delta") => delta
					.get("partial_json")
					.and_then(serde_json::Value::as_str)
					.filter(|text| !text.is_empty())
					.map(|text| {
						let mut chunk = empty_chunk();
						chunk.content_index = Some(index as usize);
						chunk.tool_call_deltas = vec![ToolCallDelta {
							index: tool_index,
							id: None,
							name: None,
							arguments_delta: Some(text.to_string()),
							content_index: Some(index as usize),
						}];
						vec![Ok(chunk)]
					})
					.unwrap_or_default(),
				_ => Vec::new(),
			}
		},
		"content_block_stop" => {
			let Some(index) = parsed.get("index").and_then(serde_json::Value::as_u64) else {
				return Vec::new();
			};
			state.blocks.remove(&index);
			match state.signatures.remove(&index) {
				Some(signature) if !signature.is_empty() => {
					vec![Ok(signature_chunk(&signature, index))]
				},
				_ => Vec::new(),
			}
		},
		"message_delta" => {
			let delta = parsed.get("delta");
			let raw_stop = delta
				.and_then(|delta| delta.get("stop_reason"))
				.and_then(serde_json::Value::as_str);
			if let Some(raw_stop) = raw_stop {
				// A usage-only delta is not a terminal signal: the stream must
				// still carry `message_stop` or EOF reports truncation.
				state.terminal = true;
				let reason = map_stop_reason(raw_stop);
				if matches!(&reason, StopReason::Error) {
					let details = delta
						.and_then(|delta| delta.get("stop_details"))
						.and_then(|value| serde_json::from_value::<StopDetails>(value.clone()).ok());
					state.terminal_error =
						Some(stop_error_message(Some(raw_stop), details.as_ref(), secrets));
				}
				state.terminal_reason = Some(reason);
			}
			let usage = parsed
				.get("usage")
				.and_then(|usage| serde_json::from_value::<AnthropicUsage>(usage.clone()).ok())
				.filter(AnthropicUsage::has_any);
			if let Some(usage) = &usage {
				usage.apply(&mut state.usage);
				state.usage_reported = true;
			}
			if usage.is_none() && raw_stop.is_none() {
				return Vec::new();
			}
			let mut chunk = empty_chunk();
			chunk.usage = state.usage_reported.then_some(state.usage);
			chunk.stop_reason = state.terminal_reason.clone();
			chunk.error_message = state.terminal_error.clone();
			vec![Ok(chunk)]
		},
		"message_stop" => {
			state.terminal = true;
			let usage = state.usage_reported.then_some(state.usage);
			let mut chunk = empty_chunk();
			chunk.usage = usage;
			chunk.is_done = true;
			chunk.stop_reason = state.terminal_reason.clone();
			chunk.error_message = state.terminal_error.clone();
			vec![Ok(chunk)]
		},
		_ => Vec::new(),
	}
}

fn is_known_event(kind: &str) -> bool {
	matches!(
		kind,
		"message_start"
			| "content_block_start"
			| "content_block_delta"
			| "content_block_stop"
			| "message_delta"
			| "message_stop"
			| "error"
			| "ping"
	)
}

fn empty_chunk() -> StreamChunk {
	StreamChunk {
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
	}
}

fn content_chunk(text: &str, index: u64) -> StreamChunk {
	let mut chunk = empty_chunk();
	chunk.content = Some(text.to_string());
	chunk.content_index = Some(index as usize);
	chunk
}

fn reasoning_chunk(text: &str, index: u64) -> StreamChunk {
	let mut chunk = empty_chunk();
	chunk.reasoning = Some(text.to_string());
	chunk.content_index = Some(index as usize);
	chunk
}

fn signature_chunk(signature: &str, index: u64) -> StreamChunk {
	let mut chunk = empty_chunk();
	chunk.signature = Some(signature.to_string());
	chunk.content_index = Some(index as usize);
	chunk
}

fn redacted_chunk(data: &str, index: u64) -> StreamChunk {
	let mut chunk = empty_chunk();
	chunk.redacted = Some(data.to_string());
	chunk.content_index = Some(index as usize);
	chunk
}

fn stream_error(event: &serde_json::Value, secrets: &[String]) -> ProviderError {
	let error_type = event
		.get("error")
		.and_then(|error| error.get("type"))
		.and_then(serde_json::Value::as_str)
		.map(|error_type| redact_with(error_type, secrets));
	let message = event
		.get("error")
		.and_then(|error| error.get("message"))
		.and_then(serde_json::Value::as_str)
		.unwrap_or("Anthropic stream error");
	let message = redact_with(message, secrets);
	let rendered = match error_type {
		Some(error_type) => format!("Anthropic stream error ({error_type}): {message}"),
		None => format!("Anthropic stream error: {message}"),
	};
	ProviderError::Protocol(rendered.chars().take(ERROR_BODY_CHARS).collect())
}

// Explicit limits relative to the TS adapter (reported, not silently dropped):
// - Signed thinking/redacted blocks, ordered tool content parts, streamed
//   signatures, stop reasons and cache/server usage extras round-trip through
//   the shared state/provider types.
// - Strict schemas implement the four-tool allowlist with optional-nullable
//   normalization and the 20/24 caps, but not the full TS grammar/union
//   analyzer, so a shape that TS would reject may remain non-strict here.
// - Cloudflare/Foundry/copilot/OpenCode gateway special cases, server-side
//   fallback content blocks/`usage.iterations`, service tier, global cache
//   scope beta, and OAuth TLS/fingerprint account flows are not modeled.
// - `Accept-Encoding` is `gzip` only because reqwest is built without
//   br/zstd/deflate decoders; TS advertises all four.
// - OAuth request ids come from `/dev/urandom` (time/pid fallback); the CCH
//   attestation is xxHash64 over the serialized body.
