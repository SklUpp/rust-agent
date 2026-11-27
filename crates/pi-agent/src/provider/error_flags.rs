//! Semantic provider-error classification shared by every HTTP adapter.
//!
//! Ports the text/status core of:
//! - `packages/ai/src/error/flags.ts` (bitmask, `classify`, `classifyMessage`,
//!   `retriable`, grammar/fast-mode/usage/OAuth accessors),
//! - `packages/ai/src/error/rate-limit.ts` (`RateLimitReason`, reason parser,
//!   backoff schedule, usage-limit outcome and concurrency exclusion),
//! - `packages/ai/src/error/auth-classify.ts` (`isDefinitiveOAuthFailure`,
//!   `isInvalidatedOAuthTokenError`, `isAuthRetryableError`),
//! - `packages/ai/src/error/finalize.ts` (`FinalizeResult` bundle).
//!
//! Deliberately text-first: the Rust adapters carry an HTTP status and a body
//! string, not the donor's mutable `AIError` graph. Cause-chain walking, the
//! `AwsCredentialsError` / `ProviderHttpError` type switches and
//! `isUnexpectedSocketCloseMessage` (already covered by the transport regex)
//! stay with the call sites that own those types.

use std::sync::LazyLock;

use regex::Regex;

/// Bitmask of provider-error kinds plus the `CLASSIFIED` marker.
///
/// Flag values mirror `flags.ts` exactly so persisted ids stay compatible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ErrorFlags(u32);

impl ErrorFlags {
	/// No bit set.
	pub const NONE: Self = Self(0);
	/// Marker proving the low bits are kinds, not an HTTP status.
	pub const CLASSIFIED: Self = Self(0x0000_1000);
	/// Empty model response that was retried (`thought-only`).
	pub const EMPTY_RESPONSE: Self = Self(0x0000_2000);
	/// Account-scoped provider policy denial; another credential may succeed.
	pub const ACCOUNT_POLICY: Self = Self(0x0000_4000);
	pub const CONTENT_BLOCKED: Self = Self(0x0000_8000);
	pub const THINKING_LOOP: Self = Self(0x0001_0000);
	pub const TRANSIENT: Self = Self(0x0002_0000);
	pub const TIMEOUT: Self = Self(0x0004_0000);
	/// Persistent account usage/quota cap; rotate credentials, do not back off.
	pub const USAGE_LIMIT: Self = Self(0x0008_0000);
	pub const STALE_RESPONSES_ITEM: Self = Self(0x0010_0000);
	pub const MALFORMED_FUNCTION_CALL: Self = Self(0x0020_0000);
	pub const PROVIDER_FINISH_ERROR: Self = Self(0x0040_0000);
	/// Prompt exceeded the model context window; compaction owns the fix.
	pub const CONTEXT_OVERFLOW: Self = Self(0x0080_0000);
	pub const AUTH_FAILED: Self = Self(0x0100_0000);
	pub const SILENT_ABORT: Self = Self(0x0200_0000);
	pub const USER_INTERRUPT: Self = Self(0x0400_0000);
	pub const ABORT: Self = Self(0x0800_0000);
	/// Strict-tool rejection: grammar too large, schema too complex or
	/// structured outputs unsupported by the endpoint.
	pub const GRAMMAR: Self = Self(0x1000_0000);
	/// Model/account does not support fast mode or the `speed` parameter.
	pub const FAST_MODE_UNSUPPORTED: Self = Self(0x2000_0000);
	/// OAuth refresh failed definitively; the stored grant is dead.
	pub const OAUTH_EXPIRY: Self = Self(0x4000_0000);
	/// HTTP 413 byte/media rejection; compaction cannot shrink byte budgets.
	pub const PAYLOAD_REJECTED: Self = Self(0x8000_0000);

	/// Wrap raw bits.
	#[must_use]
	pub const fn from_bits(bits: u32) -> Self {
		Self(bits)
	}

	/// Raw bits.
	#[must_use]
	pub const fn bits(self) -> u32 {
		self.0
	}

	/// No bit set.
	#[must_use]
	pub const fn is_empty(self) -> bool {
		self.0 == 0
	}

	/// `other` is fully contained in `self`.
	#[must_use]
	pub const fn contains(self, other: Self) -> bool {
		(self.0 & other.0) == other.0
	}

	/// Any overlap between `self` and `other`.
	#[must_use]
	pub const fn intersects(self, other: Self) -> bool {
		(self.0 & other.0) != 0
	}

	#[must_use]
	pub const fn union(self, other: Self) -> Self {
		Self(self.0 | other.0)
	}

	#[must_use]
	pub const fn intersection(self, other: Self) -> Self {
		Self(self.0 & other.0)
	}

	#[must_use]
	pub const fn without(self, other: Self) -> Self {
		Self(self.0 & !other.0)
	}

	/// Mark [kinds] as classified (`create()` in the donor).
	#[must_use]
	pub const fn create(kinds: Self) -> Self {
		Self(kinds.0 | Self::CLASSIFIED.0)
	}

	/// Whether the id carries the classification marker.
	#[must_use]
	pub const fn is_classified(self) -> bool {
		self.contains(Self::CLASSIFIED)
	}

	/// The raw HTTP status for an unclassified id, else `None`.
	#[must_use]
	pub const fn status(self) -> Option<u16> {
		if self.0 != 0 && !self.is_classified() {
			Some(self.0 as u16)
		} else {
			None
		}
	}

	/// Whether a replay can succeed (`retriable()` with `replayUnsafe = false`).
	#[must_use]
	pub const fn retriable(self) -> bool {
		retriable(self, false)
	}
}

impl std::ops::BitOr for ErrorFlags {
	type Output = Self;

	fn bitor(self, rhs: Self) -> Self {
		self.union(rhs)
	}
}

impl std::ops::BitOrAssign for ErrorFlags {
	fn bitor_assign(&mut self, rhs: Self) {
		*self = self.union(rhs);
	}
}

impl std::ops::BitAnd for ErrorFlags {
	type Output = Self;

	fn bitand(self, rhs: Self) -> Self {
		self.intersection(rhs)
	}
}

impl std::ops::BitAndAssign for ErrorFlags {
	fn bitand_assign(&mut self, rhs: Self) {
		*self = self.intersection(rhs);
	}
}

/// Every kind bit; excludes [`ErrorFlags::CLASSIFIED`] (`KIND_MASK`).
const KIND_MASK: ErrorFlags = ErrorFlags(0xFFFF_E000);

/// Kinds a plain (replay-safe) retry may succeed on.
const RETRIABLE_KINDS: ErrorFlags = ErrorFlags(
	ErrorFlags::TRANSIENT.bits()
		| ErrorFlags::USAGE_LIMIT.bits()
		| ErrorFlags::THINKING_LOOP.bits()
		| ErrorFlags::STALE_RESPONSES_ITEM.bits()
		| ErrorFlags::PROVIDER_FINISH_ERROR.bits()
		| ErrorFlags::EMPTY_RESPONSE.bits(),
);

/// `true` when `flag` is set on `id`.
#[must_use]
pub const fn is(id: ErrorFlags, flag: ErrorFlags) -> bool {
	id.contains(flag)
}

/// Retry decision for a classified id.
///
/// `replay_unsafe` mirrors the donor option: a turn that already emitted
/// partial content must not be replayed, whatever the kind.
#[must_use]
pub const fn retriable(id: ErrorFlags, replay_unsafe: bool) -> bool {
	if id.contains(ErrorFlags::CONTENT_BLOCKED) {
		return false;
	}
	if id.contains(ErrorFlags::PAYLOAD_REJECTED) {
		return false;
	}
	if replay_unsafe {
		return false;
	}
	if id.contains(ErrorFlags::MALFORMED_FUNCTION_CALL) {
		return true;
	}
	id.intersects(RETRIABLE_KINDS)
}

/// Reason category for a rate-limit/throttle failure (`rate-limit.ts`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RateLimitReason {
	QuotaExhausted,
	InsufficientG1CreditsBalance,
	RateLimitExceeded,
	ConcurrentLimit,
	ModelCapacityExhausted,
	ServerError,
	Unknown,
}

const QUOTA_EXHAUSTED_BACKOFF_MS: u64 = 30 * 60 * 1000;
const RATE_LIMIT_EXCEEDED_BACKOFF_MS: u64 = 30 * 1000;
const CONCURRENT_LIMIT_BACKOFF_MS: u64 = 5 * 1000;
const MODEL_CAPACITY_BASE_MS: u64 = 45 * 1000;
const MODEL_CAPACITY_JITTER_MS: u64 = 30 * 1000;
const SERVER_ERROR_BACKOFF_MS: u64 = 20 * 1000;
const LONG_RATE_LIMIT_DELAY_MS: u64 = 5 * 60 * 1000;
const GOOGLE_RPC_ERROR_INFO_TYPE: &str = "type.googleapis.com/google.rpc.ErrorInfo";

/// Every compiled pattern, built once (`regex` replaces the donor literals).
struct Patterns {
	overflow_evidence:           Vec<Regex>,
	generic_limit_overflow:      Regex,
	overflow_no_body:            Regex,
	payload_rejection:           Vec<Regex>,
	timeout:                     Regex,
	transient_envelope:          Regex,
	transient_envelope_before:   Regex,
	transient_transport:         Regex,
	auth_failure:                Regex,
	malformed_function_call:     Regex,
	provider_finish_error:       Regex,
	empty_response:              Regex,
	content_filter:              Regex,
	account_policy:              Regex,
	codex_account_model:         Regex,
	cursor_plan_marker:          Regex,
	cursor_plan:                 Regex,
	stale_item_id:               Regex,
	stale_previous_response:     Regex,
	stale_detail:                Regex,
	llama_tool_call_parse:       Regex,
	copilot_transient_model:     Regex,
	generation_nan:              Regex,
	grammar_too_large:           Regex,
	grammar_too_large_detail:    Regex,
	schema:                      Regex,
	schema_too_complex:          Regex,
	compile:                     Regex,
	invalid_request:             Regex,
	structured_outputs:          Regex,
	feature_not_supported:       Regex,
	anthropic_strict_field:      Regex,
	extra_inputs_not_permitted:  Regex,
	fast_speed:                  Regex,
	fast_not_supported:          Regex,
	fast_rate_limit:             Regex,
	fast_entitlement:            Regex,
	oauth_definitive:            Regex,
	oauth_transient:             Regex,
	oauth_http_auth:             Regex,
	invalidated_oauth_token:     Regex,
	status_patterns:             Vec<Regex>,
	account_rate_limit:          Regex,
	insufficient_balance:        Regex,
	spend_limit:                 Regex,
	subscription_cap:            Regex,
	transient_interval:          Regex,
	openrouter_daily_free:       Regex,
	resource_exhausted:          Regex,
	concurrent_limit:            Regex,
	account_scoped_403:          Regex,
	cn_quota:                    Regex,
	cn_transient_cap:            Regex,
	cn_throttle:                 Regex,
	dashscope_doc:               Regex,
	dashscope_message:           Regex,
	usage_limit:                 Regex,
	opaque_status_digits:        Regex,
	opaque_status_words:         Regex,
	ascii_word:                  Regex,
	retry_delay:                 Regex,
	thinking_loop:               Regex,
}

fn re(pattern: &str) -> Regex {
	Regex::new(pattern).expect("error_flags pattern compiles")
}

fn re_i(pattern: &str) -> Regex {
	re(&format!("(?i){pattern}"))
}

static PATTERNS: LazyLock<Patterns> = LazyLock::new(|| Patterns {
	overflow_evidence:           vec![
		re_i(r"prompt is too long"),
		re_i(r"input is too long for requested model"),
		re_i(r"exceeds the context window"),
		re_i(r"input token count.*exceeds the maximum"),
		re_i(r"maximum prompt length is \d+"),
		re_i(r"reduce the length of the messages"),
		re_i(r"maximum context length is \d+ tokens"),
		re_i(r"exceeds the available context size"),
		re_i(r"requested tokens?.*exceed.*context (window|length|size)"),
		re_i(r"context (window|length|size).*(exceeded|overflow|too small)"),
		re_i(r"(prompt|input).*(too long|too large).*(context|n_ctx)"),
		re_i(r"requested tokens?.*(exceeds?|greater than).*(n_ctx|context)"),
		re_i(r"greater than the context length"),
		re_i(r"context window exceeds limit"),
		re_i(r"exceeded model token limit"),
		re_i(r"context[_ ]length[_ ]exceeded"),
		re_i(r"too many tokens"),
		re_i(r"token limit exceeded"),
		re_i(r"request_too_large[^\n]*\btokens?\b"),
		re_i(r"\btokens?\b[^\n]*request_too_large"),
		re_i(r"model_context_window_exceeded"),
		re_i(r"prompt filled the context window"),
		re_i(r"exceeds the limit of \d+ tokens?\b"),
		re_i(r"chat history exceeds the \d+-message limit"),
	],
	generic_limit_overflow:      re_i(r"exceeds the limit of \d+"),
	overflow_no_body:            re_i(r"\b4(00|13)\s*(status code)?\s*\(no body\)"),
	payload_rejection:           vec![
		re_i(r"\b413\s*(?:status code\s*)?\(no body\)"),
		re_i(r"\b413\b[^.\n]{0,120}\b(?:request|payload|entity|body)\b[^.\n]{0,60}\b(?:exceed|too large|limit)"),
		re_i(r"request_too_large"),
		re_i(r"(?:payload|entity) too large"),
		re_i(r"request exceeds the maximum (?:size|number of bytes)"),
	],
	timeout:                     re_i(r"\b(?:operation\s+)?timed?\s*out\b|\btimeout\b|\bstream stall\b"),
	transient_envelope:          re_i(r"anthropic stream envelope error:"),
	transient_envelope_before:   re_i(r"before message_start"),
	transient_transport:         re_i(
		r"\b(?:no[_ -]?capacity|(?:high|peak)[ _-]?demand|(?:at|over|insufficient)[ _-]?capacity|capacity[ _-]?(?:exceeded|exhausted)|peak[ _-]?load)\b|overloaded|provider.?returned.?error|rate.?limit|too many requests|\b(?:429|500|502|503|504)\b|service.?unavailable|server.?error|internal.?error|retry your request|network.?error|connection.?error|connection.?refused|unable.?to.?connect\.\s*is the computer able to access the url\?|other side closed|fetch failed|upstream.?connect|upstream.?request.?failed|reset before headers|socket hang up|timed? out|timeout|terminated|retry delay|connection reset|stream ended before completion|stream stall|no error details in response|HTTP2(?:StreamReset|RefusedStream|EnhanceYourCalm)|nghttp2_(?:internal_error|refused_stream)|stream closed with error code nghttp2_(?:internal_error|refused_stream)|malformed.?function.?call",
	),
	auth_failure:                re_i(
		r"\b(?:401|403|unauthorized|forbidden|authentication|auth[_ ]?unavailable|no auth available|(?:invalid|no)[_ ]?api[_ ]?key)\b",
	),
	malformed_function_call:     re_i(r"\bmalformed.?function.?call\b"),
	provider_finish_error:      re_i(r"\bProvider (?:returned error finish_reason|finish_reason:\s*error)\b"),
	empty_response:             re_i(r"\bthought-only response without final output\b"),
	content_filter:             re_i(r"\b(?:incomplete:\s*)?content_filter\b"),
	account_policy:             re_i(r"\bcyber_policy\b|trusted access for cyber"),
	codex_account_model:        re_i(r#"\bThe ['"]([^'"\r\n]+)['"] model is not supported when using Codex with a ChatGPT account\."#),
	cursor_plan_marker:         re_i(r"\bERROR_RATE_LIMITED_CHANGEABLE\b"),
	cursor_plan:                re_i(r"\bNamed models unavailable\b|\bModel unavailable on\b|\bFree plans can only use\b"),
	stale_item_id:              re_i(r#"\bItem with id ['"][^'"]+['"] not found\.?"#),
	stale_previous_response:    re_i(r"previous[ _]?response"),
	stale_detail:               re_i(r"not[ _]?found|invalid|expired|stale|zero[ _-]?data[ _-]?retention"),
	llama_tool_call_parse:      re_i(r"failed to parse tool call arguments as json|\[json\.exception\.parse_error\.101\]"),
	copilot_transient_model:    re_i(r"model_not_supported"),
	generation_nan:             re(r"(?is)floating[ _-]?point nan\b.*\bdetected in generation"),
	grammar_too_large:          re_i(r"compiled grammar"),
	grammar_too_large_detail:   re_i(r"too large"),
	schema:                     re_i(r"schema"),
	schema_too_complex:         re_i(r"too complex"),
	compile:                    re_i(r"compil"),
	invalid_request:            re_i(r"invalid_request_error"),
	structured_outputs:         re_i(r"structured[_ -]?outputs?"),
	feature_not_supported:      re_i(r"not (?:supported|available|enabled)|unsupported|does(?: not|n'?t) support"),
	anthropic_strict_field:     re_i(r"\btools\.\d+\.custom\.strict\b"),
	extra_inputs_not_permitted: re_i(r"extra inputs? (?:are|is) not permitted"),
	fast_speed:                 re_i(r"\bspeed\b"),
	fast_not_supported:         re_i(r"not support"),
	fast_rate_limit:            re_i(r"rate_limit_error"),
	fast_entitlement:           re_i(r"fast mode"),
	oauth_definitive:           re_i(
		r"\binvalid_grant\b|\binvalid_token\b|\bunauthorized_client\b|\brevoked\b|refresh[\s_]?token.*expired",
	),
	oauth_transient:            re_i(
		r"timeout|network|fetch failed|ECONN(?:REFUSED|RESET)|ETIMEDOUT|EAI_AGAIN|socket hang up|\b(?:408|425|429|5\d{2})\b|rate.?limit|too many requests|temporar|unavailable|forbidden|permission_denied|cloudflare|captcha",
	),
	oauth_http_auth:            re_i(r"\b401\b"),
	invalidated_oauth_token:    re_i(r"\binvalidated oauth token\b"),
	status_patterns:            vec![
		re_i(r"\bstatus(?:_code)?[:=]\s*(\d{3})\b"),
		re_i(r"\bstatus\s+(\d{3})\b"),
		re_i(r"\bHTTP\s+(\d{3})\b"),
		re_i(r"\b(?:error|failed)\s*[:=]?\s*(\d{3})\b"),
		re(r"(?:^|\s)(\d{3})\s+(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)"),
	],
	account_rate_limit:         re_i(r"\baccount(?:'s)?\b[^\n]{0,80}\brate.?limit\b|\brate.?limit\b[^\n]{0,80}\baccount\b"),
	insufficient_balance:       re_i(r"insufficient.?balance"),
	spend_limit:                re_i(r"spend.?limit"),
	subscription_cap:           re_i(
		r"\b(?:subscription|plan|membership)\b[^\n]{0,80}\b(?:rate.?limits?|quota|cap)\b|\b(?:rate.?limits?|quota|cap)\b[^\n]{0,80}\b(?:subscription|plan|membership)\b",
	),
	transient_interval:         re_i(r"\bper\s+(?:second|minute)\b"),
	openrouter_daily_free:      re_i(r"\bfree[-_ ]models[-_ ]per[-_ ]day\b"),
	resource_exhausted:         re_i(r"resource.?exhausted"),
	concurrent_limit:           re_i(
		r"\btoo many\s+concurren\w*\s+(?:requests?|invocations?)\b|\bconcurren\w*\b[^\n]{0,60}\b(?:limit|quota|exceed\w*|reach\w*)\b|\b(?:limit|quota|exceed\w*|reach\w*)\b[^\n]{0,60}\bconcurren\w*\b|\bconcurren[a-z]*[-_](?:[a-z]+[_-])*(?:limit|quota|exceed\w*|reach\w*)",
	),
	account_scoped_403:         re_i(
		r"\b(?:overall|account|organization|team|workspace)\b[^\n]{0,40}\b(?:message |request )?rate.?limit\b|\byour\b[^\n]{0,30}\b(?:limit )?will reset\b",
	),
	cn_quota:                   re(r"使用.{0,30}?上限|(?:额度|配额)已?(?:用|耗)(?:完|尽)|限额.{0,30}重置|余额不足"),
	cn_transient_cap:           re(r"速率.{0,30}上限|频率.{0,30}上限|每分钟.{0,30}上限|并发.{0,30}上限|使用.{0,30}(?:速率|频率|每分钟|并发).{0,30}上限"),
	cn_throttle:                re(r"速率(?:限制|过快)|频率(?:过高|过快)|过于频繁|稍后[重再]试"),
	dashscope_doc:              re_i(r"error-code[^()\s]*#token-limit"),
	dashscope_message:          re_i(r"\byou exceeded your current quota, please check your plan and billing details\b"),
	usage_limit:                re_i(
		r"usage.?limit|usage_limit_reached|usage_not_included|limit_reached|quota.?(?:exceeded|reached|insufficient)|额度不足|额度耗尽|resource.?exhausted|exhausted your capacity|quota will reset|insufficient.?(?:balance|quota)|balance.?exhausted|run out of credits|out of credits|spending[- _]?limit|personal-team-blocked",
	),
	opaque_status_digits:       re_i(r"\b(?:429|402)\b"),
	opaque_status_words:        re_i(r"\b(?:http|https|status|error|code|response|message)\b"),
	ascii_word:                 re_i(r"[a-z\d]{3,}"),
	retry_delay:                re(r#"(?i)"retryDelay"\s*:\s*"([0-9.]+)(ms|s)""#),
	thinking_loop:              re_i(r"thinking loop detected"),
});

/// Token-context evidence only; excludes the generic numeric limit which media
/// budgets also match (#9235).
#[must_use]
pub fn has_token_context_overflow_evidence(text: &str) -> bool {
	PATTERNS.overflow_evidence.iter().any(|pattern| pattern.is_match(text))
}

fn matches_overflow_text(text: &str) -> bool {
	has_token_context_overflow_evidence(text)
		|| PATTERNS.generic_limit_overflow.is_match(text)
		|| PATTERNS.overflow_no_body.is_match(text)
}

fn matches_payload_rejection_text(text: &str) -> bool {
	if !PATTERNS.payload_rejection.iter().any(|pattern| pattern.is_match(text)) {
		return false;
	}
	!has_token_context_overflow_evidence(text)
}

fn is_timeout_text(text: &str) -> bool {
	PATTERNS.timeout.is_match(text)
}

fn is_transient_error_text(text: &str) -> bool {
	(PATTERNS.transient_envelope.is_match(text) && PATTERNS.transient_envelope_before.is_match(text))
		|| PATTERNS.transient_transport.is_match(text)
}

fn is_auth_failure_text(text: &str) -> bool {
	PATTERNS.auth_failure.is_match(text)
}

fn is_stale_responses_text(text: &str) -> bool {
	PATTERNS.stale_item_id.is_match(text)
		|| (PATTERNS.stale_previous_response.is_match(text) && PATTERNS.stale_detail.is_match(text))
}

fn matches_strict_tools_rejection(message: &str, status: Option<u16>) -> bool {
	if status != Some(400) {
		return false;
	}
	if PATTERNS.anthropic_strict_field.is_match(message)
		&& PATTERNS.extra_inputs_not_permitted.is_match(message)
	{
		return true;
	}
	if PATTERNS.structured_outputs.is_match(message) && PATTERNS.feature_not_supported.is_match(message) {
		return true;
	}
	if !PATTERNS.invalid_request.is_match(message) {
		return false;
	}
	let grammar_too_large =
		PATTERNS.grammar_too_large.is_match(message) && PATTERNS.grammar_too_large_detail.is_match(message);
	let schema_too_complex = PATTERNS.schema.is_match(message)
		&& PATTERNS.schema_too_complex.is_match(message)
		&& PATTERNS.compile.is_match(message);
	grammar_too_large || schema_too_complex
}

fn matches_fast_mode_unsupported(message: &str, status: Option<u16>) -> bool {
	if status != Some(400) && status != Some(429) {
		return false;
	}
	if status == Some(400)
		&& PATTERNS.invalid_request.is_match(message)
		&& PATTERNS.fast_speed.is_match(message)
		&& PATTERNS.fast_not_supported.is_match(message)
	{
		return true;
	}
	status == Some(429)
		&& PATTERNS.fast_rate_limit.is_match(message)
		&& PATTERNS.fast_entitlement.is_match(message)
}

/// Provider id for the Codex ChatGPT-account entitlement denial (`None` when
/// the message is a generic unsupported-model rejection).
fn codex_account_policy_model_from_text(text: &str) -> Option<String> {
	let model = PATTERNS
		.codex_account_model
		.captures(text)?
		.get(1)?
		.as_str()
		.trim()
		.to_string();
	normalize_codex_model(&model).map(|_| model)
}

fn normalize_codex_model(model: &str) -> Option<String> {
	let bare = model.rsplit('/').next()?.trim().to_lowercase();
	if bare.is_empty() || bare.chars().count() > 256 || bare.contains('\0') {
		return None;
	}
	Some(bare)
}

fn is_codex_account_policy_text(text: &str, provider: Option<&str>, model: Option<&str>) -> bool {
	if provider != Some("openai-codex") {
		return false;
	}
	let denied = codex_account_policy_model_from_text(text);
	let denied_identity = denied.as_deref().and_then(normalize_codex_model);
	let requested_identity = model.and_then(normalize_codex_model);
	denied_identity.is_some() && denied_identity == requested_identity
}

fn is_cursor_plan_policy_text(text: &str) -> bool {
	PATTERNS.cursor_plan_marker.is_match(text) && PATTERNS.cursor_plan.is_match(text)
}

/// First HTTP status mentioned in free-form error text (`status()` donor).
#[must_use]
pub fn status_from_message(message: &str) -> Option<u16> {
	for pattern in &PATTERNS.status_patterns {
		if let Some(code) = pattern
			.captures(message)
			.and_then(|captures| captures.get(1))
			.and_then(|value| value.as_str().parse::<u16>().ok())
			&& (100..=599).contains(&code)
		{
			return Some(code);
		}
	}
	None
}

/// HTTP statuses that, absent richer body classification, are account caps.
#[must_use]
pub const fn is_usage_limit_status(status: Option<u16>) -> bool {
	matches!(status, Some(402 | 429))
}

/// True for DashScope/Bailian's documented OpenAI-compatible TPM/TPS throttle.
#[must_use]
pub fn is_dashscope_token_limit_text(message: &str) -> bool {
	PATTERNS.dashscope_doc.is_match(message) && PATTERNS.dashscope_message.is_match(message)
}

fn matches_subscription_cap_text(message: &str) -> bool {
	PATTERNS.subscription_cap.is_match(message) && !PATTERNS.transient_interval.is_match(message)
}

const fn is_quota_exhausted_reason(reason: RateLimitReason) -> bool {
	matches!(
		reason,
		RateLimitReason::QuotaExhausted | RateLimitReason::InsufficientG1CreditsBalance
	)
}

/// Retry-hint milliseconds carried as `"retryDelay": "30s"` (`extractRetryHint`).
fn retry_delay_ms(message: &str) -> Option<u64> {
	let captures = PATTERNS.retry_delay.captures(message)?;
	let value: f64 = captures.get(1)?.as_str().parse().ok()?;
	let unit = captures.get(2)?.as_str();
	#[allow(
		clippy::cast_possible_truncation,
		clippy::cast_sign_loss,
		reason = "retry hints are small positive millisecond counts"
	)]
	let millis = if unit == "ms" {
		value as u64
	} else {
		(value * 1000.0) as u64
	};
	Some(millis)
}

/// Structured Google `RESOURCE_EXHAUSTED` body classification.
fn parse_google_rpc_rate_limit_reason(message: &str) -> Option<RateLimitReason> {
	let start = message.find('{')?;
	let end = message.rfind('}')?;
	if end < start {
		return None;
	}
	let body: serde_json::Value = serde_json::from_str(&message[start..=end]).ok()?;
	let error = body.get("error")?;
	if error.get("status").and_then(serde_json::Value::as_str)?.trim().to_ascii_uppercase()
		!= "RESOURCE_EXHAUSTED"
	{
		return None;
	}
	for detail in error.get("details")?.as_array()? {
		if detail.get("@type").and_then(serde_json::Value::as_str) != Some(GOOGLE_RPC_ERROR_INFO_TYPE) {
			continue;
		}
		let Some(reason) = detail.get("reason").and_then(serde_json::Value::as_str) else {
			continue;
		};
		match reason.trim().to_ascii_uppercase().as_str() {
			"QUOTA_EXHAUSTED" => return Some(RateLimitReason::QuotaExhausted),
			"INSUFFICIENT_G1_CREDITS_BALANCE" => return Some(RateLimitReason::InsufficientG1CreditsBalance),
			"RATE_LIMIT_EXCEEDED" => {
				return Some(match retry_delay_ms(message) {
					Some(delay) if delay >= LONG_RATE_LIMIT_DELAY_MS => RateLimitReason::QuotaExhausted,
					_ => RateLimitReason::RateLimitExceeded,
				});
			}
			_ => {}
		}
	}
	None
}

/// Classify a rate-limit message (`parseRateLimitReason`).
///
/// Priority: structured Google detail > `quota will reset` > CN quota >
/// DashScope token-limit > concurrency > model capacity > account/subscription/
/// free-tier quota > rate limit > generic quota > server error > bare
/// `resource exhausted` (model capacity) > unknown.
#[must_use]
pub fn parse_rate_limit_reason(error_message: &str) -> RateLimitReason {
	if let Some(reason) = parse_google_rpc_rate_limit_reason(error_message) {
		return reason;
	}
	let lower_with_status = error_message.to_lowercase();
	let lower = PATTERNS.resource_exhausted.replace_all(&lower_with_status, "");
	let has_resource_exhausted_status = lower.as_ref() != lower_with_status.as_str();

	if lower.contains("quota will reset") || lower.contains("exhausted your capacity") {
		return RateLimitReason::QuotaExhausted;
	}
	if PATTERNS.cn_quota.is_match(error_message) && !PATTERNS.cn_transient_cap.is_match(error_message) {
		return RateLimitReason::QuotaExhausted;
	}
	if is_dashscope_token_limit_text(error_message) {
		return RateLimitReason::RateLimitExceeded;
	}
	if PATTERNS.concurrent_limit.is_match(error_message) {
		return RateLimitReason::ConcurrentLimit;
	}
	if lower.contains("capacity")
		|| lower.contains("overloaded")
		|| lower.contains("529")
		|| lower.contains("503")
	{
		return RateLimitReason::ModelCapacityExhausted;
	}
	if PATTERNS.account_rate_limit.is_match(error_message) {
		return RateLimitReason::QuotaExhausted;
	}
	if PATTERNS.spend_limit.is_match(error_message) {
		return RateLimitReason::QuotaExhausted;
	}
	if matches_subscription_cap_text(error_message) {
		return RateLimitReason::QuotaExhausted;
	}
	if PATTERNS.openrouter_daily_free.is_match(error_message) {
		return RateLimitReason::QuotaExhausted;
	}
	if lower.contains("per minute")
		|| lower.contains("rate limit")
		|| lower.contains("too many requests")
		|| lower.contains("presque")
	{
		return RateLimitReason::RateLimitExceeded;
	}
	if lower.contains("exhausted")
		|| lower.contains("quota")
		|| lower.contains("usage limit")
		|| lower.contains("run out of credits")
		|| lower.contains("out of credits")
		|| lower.contains("spending-limit")
		|| lower.contains("spending limit")
		|| PATTERNS.insufficient_balance.is_match(error_message)
	{
		return RateLimitReason::QuotaExhausted;
	}
	if lower.contains("500") || lower.contains("internal error") || lower.contains("internal server error") {
		return RateLimitReason::ServerError;
	}
	if has_resource_exhausted_status {
		return RateLimitReason::ModelCapacityExhausted;
	}
	RateLimitReason::Unknown
}

/// Backoff delay in ms for a reason, with clock-derived jitter for model
/// capacity (`calculateRateLimitBackoffMs`).
#[must_use]
pub fn calculate_rate_limit_backoff_ms(reason: RateLimitReason) -> u64 {
	let jitter = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |duration| u64::from(duration.subsec_nanos()) % MODEL_CAPACITY_JITTER_MS);
	calculate_rate_limit_backoff_ms_with_jitter(reason, jitter)
}

/// Deterministic form of [`calculate_rate_limit_backoff_ms`] for tests and
/// replay: `jitter_ms` is applied modulo the donor's 30s band.
#[must_use]
pub const fn calculate_rate_limit_backoff_ms_with_jitter(reason: RateLimitReason, jitter_ms: u64) -> u64 {
	match reason {
		RateLimitReason::QuotaExhausted | RateLimitReason::InsufficientG1CreditsBalance => {
			QUOTA_EXHAUSTED_BACKOFF_MS
		}
		RateLimitReason::RateLimitExceeded => RATE_LIMIT_EXCEEDED_BACKOFF_MS,
		RateLimitReason::ConcurrentLimit => CONCURRENT_LIMIT_BACKOFF_MS,
		RateLimitReason::ModelCapacityExhausted => {
			MODEL_CAPACITY_BASE_MS + jitter_ms % MODEL_CAPACITY_JITTER_MS
		}
		RateLimitReason::ServerError => SERVER_ERROR_BACKOFF_MS,
		RateLimitReason::Unknown => QUOTA_EXHAUSTED_BACKOFF_MS,
	}
}

/// A usage-limit status body is opaque when it only repeats the status.
#[must_use]
pub fn is_opaque_status_body(message: &str) -> bool {
	let digits = PATTERNS.opaque_status_digits.replace_all(message, "");
	let cleaned = PATTERNS.opaque_status_words.replace_all(&digits, "");
	!PATTERNS.ascii_word.is_match(&cleaned)
		&& !PATTERNS.cn_quota.is_match(&cleaned)
		&& !PATTERNS.cn_transient_cap.is_match(&cleaned)
		&& !PATTERNS.cn_throttle.is_match(&cleaned)
}

/// Internal text matcher for persistent usage/quota phrasing
/// (`matchesUsageLimitText`).
#[must_use]
pub fn matches_usage_limit_text(error_message: &str) -> bool {
	if let Some(reason) = parse_google_rpc_rate_limit_reason(error_message) {
		return is_quota_exhausted_reason(reason);
	}
	if is_dashscope_token_limit_text(error_message) {
		return false;
	}
	PATTERNS.usage_limit.is_match(error_message)
		|| (PATTERNS.cn_quota.is_match(error_message) && !PATTERNS.cn_transient_cap.is_match(error_message))
		|| PATTERNS.spend_limit.is_match(error_message)
		|| PATTERNS.account_rate_limit.is_match(error_message)
		|| matches_subscription_cap_text(error_message)
		|| PATTERNS.openrouter_daily_free.is_match(error_message)
}

/// Account-scoped cap phrasing delivered on a 403 or statusless trailer.
#[must_use]
pub fn is_account_scoped_cap_text(message: &str) -> bool {
	PATTERNS.account_scoped_403.is_match(message)
}

/// A concurrency cap on a non-billing status is shed-and-backoff, not
/// credential-rotatable (`isConcurrencyCapExclusion`).
#[must_use]
pub fn is_concurrency_cap_exclusion(status: Option<u16>, message: Option<&str>) -> bool {
	message.is_some_and(|message| {
		parse_rate_limit_reason(message) == RateLimitReason::ConcurrentLimit && status != Some(402)
	})
}

/// Whether a failure should burn a credential and rotate (`isUsageLimitOutcome`).
#[must_use]
pub fn is_usage_limit_outcome(status: Option<u16>, message: Option<&str>) -> bool {
	if let Some(reason) = message.and_then(parse_google_rpc_rate_limit_reason) {
		return is_quota_exhausted_reason(reason);
	}
	if is_concurrency_cap_exclusion(status, message) {
		return false;
	}
	if let Some(message) = message {
		if matches_usage_limit_text(message) {
			return true;
		}
		if matches!(status, Some(403) | None) && is_account_scoped_cap_text(message) {
			return true;
		}
	}
	if !is_usage_limit_status(status) {
		return false;
	}
	let Some(message) = message else {
		return true;
	};
	if is_opaque_status_body(message) {
		return true;
	}
	let reason = parse_rate_limit_reason(message);
	is_quota_exhausted_reason(reason) || (status == Some(402) && reason == RateLimitReason::ConcurrentLimit)
}

/// Text/status classification core (`classifyText`).
fn classify_text(
	error_message: Option<&str>,
	error_status: Option<u16>,
	prior_token_overflow_evidence: bool,
	api: Option<&str>,
	provider: Option<&str>,
	model: Option<&str>,
) -> ErrorFlags {
	let mut kinds = ErrorFlags::NONE;
	if let Some(text) = error_message {
		if matches_overflow_text(text) {
			kinds |= ErrorFlags::CONTEXT_OVERFLOW;
		}
		if matches_payload_rejection_text(text) {
			kinds |= ErrorFlags::PAYLOAD_REJECTED;
		}
		if PATTERNS.malformed_function_call.is_match(text) {
			kinds |= ErrorFlags::MALFORMED_FUNCTION_CALL;
		}
		if PATTERNS.provider_finish_error.is_match(text) {
			kinds |= ErrorFlags::PROVIDER_FINISH_ERROR;
		}
		if PATTERNS.empty_response.is_match(text) {
			kinds |= ErrorFlags::EMPTY_RESPONSE | ErrorFlags::TRANSIENT;
		}
		if PATTERNS.thinking_loop.is_match(text) {
			// The thinking-loop guard's own marker: retryable like the other
			// transient kinds, but distinguishable for diagnostics/telemetry.
			kinds |= ErrorFlags::THINKING_LOOP | ErrorFlags::TRANSIENT;
		}
		if PATTERNS.content_filter.is_match(text) {
			kinds |= ErrorFlags::CONTENT_BLOCKED;
		}
		if PATTERNS.account_policy.is_match(text)
			|| is_codex_account_policy_text(text, provider, model)
			|| (provider == Some("cursor") && is_cursor_plan_policy_text(text))
		{
			kinds |= ErrorFlags::ACCOUNT_POLICY | ErrorFlags::CONTENT_BLOCKED;
		}
		if is_auth_failure_text(text) {
			kinds |= ErrorFlags::AUTH_FAILED;
		}

		let status_clean = error_status.or_else(|| status_from_message(text));
		let is_opaque = is_opaque_status_body(text);
		let is_limit_status = is_usage_limit_status(status_clean);
		let reason = parse_rate_limit_reason(text);
		// A 402 is categorically a billing cap: a concurrency-worded body still
		// rotates. For 429 and other statuses a concurrency cap is transient.
		let is_billing_cap_status = status_clean == Some(402);
		let concurrency_excluded = reason == RateLimitReason::ConcurrentLimit && !is_billing_cap_status;
		if !concurrency_excluded
			&& (matches_usage_limit_text(text)
				|| (matches!(status_clean, Some(403) | None) && is_account_scoped_cap_text(text))
				|| (is_limit_status
					&& (is_opaque
						|| reason == RateLimitReason::QuotaExhausted
						|| (is_billing_cap_status && reason == RateLimitReason::ConcurrentLimit))))
		{
			kinds |= ErrorFlags::USAGE_LIMIT;
		}

		if is_timeout_text(text) {
			kinds |= ErrorFlags::TRANSIENT | ErrorFlags::TIMEOUT;
		} else if is_transient_error_text(text) {
			kinds |= ErrorFlags::TRANSIENT;
		}
		if reason == RateLimitReason::ConcurrentLimit {
			kinds |= ErrorFlags::TRANSIENT;
		}
		if matches!(api, Some("openai-responses" | "openai-codex-responses")) && is_stale_responses_text(text) {
			kinds |= ErrorFlags::STALE_RESPONSES_ITEM;
		}

		if status_clean == Some(400) && PATTERNS.copilot_transient_model.is_match(text) {
			kinds |= ErrorFlags::TRANSIENT;
		}
		if status_clean == Some(400) && PATTERNS.generation_nan.is_match(text) {
			kinds |= ErrorFlags::TRANSIENT;
		}
		if matches_strict_tools_rejection(text, status_clean) {
			kinds |= ErrorFlags::GRAMMAR;
		}
		if matches_fast_mode_unsupported(text, status_clean) {
			kinds |= ErrorFlags::FAST_MODE_UNSUPPORTED;
		}
	}

	// Status-only 413: infer payload rejection unless token evidence exists.
	let status_evidence = error_status.or_else(|| error_message.and_then(status_from_message));
	if status_evidence == Some(413)
		&& !prior_token_overflow_evidence
		&& !error_message.is_some_and(has_token_context_overflow_evidence)
	{
		kinds |= ErrorFlags::PAYLOAD_REJECTED;
	}
	if !kinds.is_empty() {
		return ErrorFlags::create(kinds);
	}
	match status_evidence {
		Some(401 | 403) => ErrorFlags::create(ErrorFlags::AUTH_FAILED),
		Some(status) => ErrorFlags::from_bits(u32::from(status)),
		None => ErrorFlags::NONE,
	}
}

/// Classify an HTTP status + body (donor `classify`, text-only form).
#[must_use]
pub fn classify(
	status: Option<u16>,
	api: Option<&str>,
	provider: Option<&str>,
	model: Option<&str>,
	body: &str,
) -> ErrorFlags {
	let id = classify_text(Some(body), status, false, api, provider, model);
	if id.is_empty() {
		ErrorFlags::from_bits(u32::from(status.unwrap_or(0)))
	} else {
		id
	}
}

/// Classify a persisted error message (`classifyMessage`).
#[must_use]
pub fn classify_message(body: &str) -> ErrorFlags {
	classify_message_with(body, ErrorFlags::NONE, None, None, None, None)
}

/// [`classify_message`] merged onto an existing id/status, with provider
/// context (`classifyMessage`).
#[must_use]
pub fn classify_message_with(
	body: &str,
	existing: ErrorFlags,
	status: Option<u16>,
	api: Option<&str>,
	provider: Option<&str>,
	model: Option<&str>,
) -> ErrorFlags {
	let current_status = status.or_else(|| existing.status());
	let existing_overflow_only = existing.contains(ErrorFlags::CONTEXT_OVERFLOW)
		&& !existing.contains(ErrorFlags::PAYLOAD_REJECTED);
	let text_id = classify_text(Some(body), current_status, existing_overflow_only, api, provider, model);

	let mut kinds = existing.union(text_id).intersection(KIND_MASK);
	// Two-phase finalization: final token-overflow text drops a stale 413 bit.
	if current_status == Some(413)
		&& has_token_context_overflow_evidence(body)
		&& !text_id.contains(ErrorFlags::PAYLOAD_REJECTED)
	{
		kinds = kinds.without(ErrorFlags::PAYLOAD_REJECTED);
	}
	// Deterministic local-model tool-call JSON parse failure: replay loops.
	if PATTERNS.llama_tool_call_parse.is_match(body) {
		kinds = kinds.without(ErrorFlags::TRANSIENT);
	}
	if !kinds.is_empty() {
		return ErrorFlags::create(kinds);
	}
	text_id
		.status()
		.or_else(|| existing.status())
		.or(current_status)
		.map_or(ErrorFlags::NONE, |status| ErrorFlags::from_bits(u32::from(status)))
}

/// Whether an id classifies as a persistent usage/quota cap.
#[must_use]
pub const fn is_usage_limit(id: ErrorFlags) -> bool {
	id.contains(ErrorFlags::USAGE_LIMIT)
}

/// Whether an id classifies as an account-scoped policy denial.
#[must_use]
pub const fn is_account_policy_error(id: ErrorFlags) -> bool {
	id.contains(ErrorFlags::ACCOUNT_POLICY)
}

/// Whether an id classifies as a strict-tool grammar/schema rejection.
#[must_use]
pub const fn is_grammar_error(id: ErrorFlags) -> bool {
	id.contains(ErrorFlags::GRAMMAR)
}

/// Whether an id classifies as unsupported fast mode / `speed` parameter.
#[must_use]
pub const fn is_fast_mode_unsupported(id: ErrorFlags) -> bool {
	id.contains(ErrorFlags::FAST_MODE_UNSUPPORTED)
}

/// Whether an OAuth refresh message means the grant is definitively dead
/// (`isOAuthExpiry`).
#[must_use]
pub fn is_oauth_expiry(error_message: &str) -> bool {
	if PATTERNS.oauth_definitive.is_match(error_message) {
		return true;
	}
	PATTERNS.oauth_http_auth.is_match(error_message) && !PATTERNS.oauth_transient.is_match(error_message)
}

/// Alias kept for the donor's public name (`isDefinitiveOAuthFailure`).
#[must_use]
pub fn is_definitive_oauth_failure(error_message: &str) -> bool {
	is_oauth_expiry(error_message)
}

/// Marker-only OAuth death test: an explicit revocation/dead-grant identifier
/// (`invalid_grant`, `invalid_token`, `revoked`, expired refresh token).
///
/// Unlike [`is_oauth_expiry`] this never treats a bare HTTP `401` as definitive:
/// `HttpResponse::error_for()` embeds the status in the message, so a transient
/// 401 refresh failure must not fence the stored credential. Credential
/// disabling uses this accessor; the donor 401 heuristic stays available for
/// callers that own a real transport error.
#[must_use]
pub fn is_oauth_revocation_message(message: &str) -> bool {
	PATTERNS.oauth_definitive.is_match(message)
}

/// Whether the provider explicitly invalidated the supplied OAuth bearer.
#[must_use]
pub fn is_invalidated_oauth_token_error(message: &str) -> bool {
	PATTERNS.invalidated_oauth_token.is_match(message)
}

/// Whether a failure should retry through the credential resolver
/// (`isAuthRetryableError`).
#[must_use]
pub fn is_auth_retryable_error(status: Option<u16>, message: &str) -> bool {
	let id = classify(status, None, None, None, message);
	if is_usage_limit(id) || is_account_policy_error(id) || is_invalidated_oauth_token_error(message) {
		return true;
	}
	let embedded_status = status.or_else(|| status_from_message(message));
	if is_concurrency_cap_exclusion(embedded_status, Some(message)) {
		return false;
	}
	if matches!(embedded_status, Some(401 | 403)) {
		return true;
	}
	is_usage_limit_outcome(embedded_status, Some(message))
}

/// Stop reason for a finalized bundle (`finalize.ts`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizedStopReason {
	Aborted,
	Error,
}

/// Context a provider catch block hands to [`finalize`].
#[derive(Debug, Clone, Default)]
pub struct FinalizeOptions<'a> {
	/// Wire API, for api-specific text classification.
	pub api:          Option<&'a str>,
	/// Provider id (Codex/Cursor policy classification).
	pub provider:     Option<&'a str>,
	/// Requested model id (model-entitlement classification).
	pub model:        Option<&'a str>,
	/// Caller-visible abort flag; a caller abort dominates classification.
	pub aborted:      bool,
	/// Captured HTTP status, when the adapter has one.
	pub error_status: Option<u16>,
}

/// The bundle a provider assigns onto an assistant error (`FinalizeResult`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedError {
	pub id:          ErrorFlags,
	pub status:      Option<u16>,
	pub stop_reason: FinalizedStopReason,
	pub message:     String,
}

/// Build the complete error bundle for a provider catch block.
///
/// Donor `formatMessage` is not ported: the Rust adapters already carry the
/// user-facing text, so the caller passes it in.
#[must_use]
pub fn finalize(message: &str, opts: &FinalizeOptions<'_>) -> FinalizedError {
	let status = opts.error_status.or_else(|| status_from_message(message));
	let id = classify_message_with(message, ErrorFlags::NONE, status, opts.api, opts.provider, opts.model);
	FinalizedError {
		id,
		status,
		stop_reason: if opts.aborted {
			FinalizedStopReason::Aborted
		} else {
			FinalizedStopReason::Error
		},
		message: message.to_string(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn flags(s: u32) -> ErrorFlags {
		ErrorFlags::from_bits(s)
	}

	#[test]
	fn flag_bits_match_the_donor() {
		assert_eq!(ErrorFlags::CLASSIFIED.bits(), 0x1000);
		assert_eq!(ErrorFlags::EMPTY_RESPONSE.bits(), 0x2000);
		assert_eq!(ErrorFlags::ACCOUNT_POLICY.bits(), 0x4000);
		assert_eq!(ErrorFlags::CONTENT_BLOCKED.bits(), 0x8000);
		assert_eq!(ErrorFlags::THINKING_LOOP.bits(), 0x1_0000);
		assert_eq!(ErrorFlags::TRANSIENT.bits(), 0x2_0000);
		assert_eq!(ErrorFlags::TIMEOUT.bits(), 0x4_0000);
		assert_eq!(ErrorFlags::USAGE_LIMIT.bits(), 0x8_0000);
		assert_eq!(ErrorFlags::STALE_RESPONSES_ITEM.bits(), 0x10_0000);
		assert_eq!(ErrorFlags::MALFORMED_FUNCTION_CALL.bits(), 0x20_0000);
		assert_eq!(ErrorFlags::PROVIDER_FINISH_ERROR.bits(), 0x40_0000);
		assert_eq!(ErrorFlags::CONTEXT_OVERFLOW.bits(), 0x80_0000);
		assert_eq!(ErrorFlags::AUTH_FAILED.bits(), 0x100_0000);
		assert_eq!(ErrorFlags::SILENT_ABORT.bits(), 0x200_0000);
		assert_eq!(ErrorFlags::USER_INTERRUPT.bits(), 0x400_0000);
		assert_eq!(ErrorFlags::ABORT.bits(), 0x800_0000);
		assert_eq!(ErrorFlags::GRAMMAR.bits(), 0x1000_0000);
		assert_eq!(ErrorFlags::FAST_MODE_UNSUPPORTED.bits(), 0x2000_0000);
		assert_eq!(ErrorFlags::OAUTH_EXPIRY.bits(), 0x4000_0000);
		assert_eq!(ErrorFlags::PAYLOAD_REJECTED.bits(), 0x8000_0000);
	}

	#[test]
	fn create_sets_the_classified_marker() {
		let id = ErrorFlags::create(ErrorFlags::TRANSIENT);
		assert!(id.is_classified());
		assert!(id.contains(ErrorFlags::TRANSIENT));
		assert!(id.status().is_none());
		assert_eq!(ErrorFlags::from_bits(429).status(), Some(429));
		assert_eq!(ErrorFlags::NONE.status(), None);
	}

	#[test]
	fn retriable_kinds_and_vetoes() {
		assert!(ErrorFlags::create(ErrorFlags::TRANSIENT).retriable());
		assert!(ErrorFlags::create(ErrorFlags::EMPTY_RESPONSE).retriable());
		assert!(ErrorFlags::create(ErrorFlags::MALFORMED_FUNCTION_CALL).retriable());
		assert!(!ErrorFlags::create(ErrorFlags::CONTENT_BLOCKED).retriable());
		assert!(!ErrorFlags::create(ErrorFlags::PAYLOAD_REJECTED).retriable());
		assert!(!retriable(ErrorFlags::create(ErrorFlags::TRANSIENT), true));
		assert!(!ErrorFlags::create(ErrorFlags::AUTH_FAILED).retriable());
	}

	#[test]
	fn http_400_generation_nan_is_transient_not_grammar() {
		let id = classify(
			Some(400),
			Some("openai-completions"),
			Some("fireworks"),
			None,
			"invalid_request_error: Floating point NaN (not-a-number) is detected in generation",
		);
		assert!(id.contains(ErrorFlags::TRANSIENT), "{id:?}");
		assert!(!id.contains(ErrorFlags::GRAMMAR));
		assert!(!id.contains(ErrorFlags::AUTH_FAILED));
	}

	#[test]
	fn http_400_grammar_and_schema_rejections() {
		let grammar = classify(
			Some(400),
			Some("openai-responses"),
			Some("anthropic"),
			None,
			"invalid_request_error: compiled grammar is too large",
		);
		assert!(is_grammar_error(grammar), "{grammar:?}");

		let schema = classify(
			Some(400),
			None,
			None,
			None,
			"invalid_request_error: schema is too complex to compile",
		);
		assert!(is_grammar_error(schema), "{schema:?}");

		let structured = classify(
			Some(400),
			None,
			Some("baseten"),
			None,
			"structured_outputs not supported on this endpoint",
		);
		assert!(is_grammar_error(structured), "{structured:?}");

		let strict_field = classify(
			Some(400),
			None,
			None,
			None,
			"invalid_request_error: tools.0.custom.strict: Extra inputs are not permitted",
		);
		assert!(is_grammar_error(strict_field), "{strict_field:?}");
	}

	#[test]
	fn http_400_fast_mode_and_429_entitlement() {
		let param = classify(
			Some(400),
			None,
			Some("anthropic"),
			None,
			"invalid_request_error: speed parameter is not supported",
		);
		assert!(is_fast_mode_unsupported(param), "{param:?}");

		let entitlement = classify(
			Some(429),
			None,
			Some("anthropic"),
			None,
			"rate_limit_error: fast mode requires extra usage",
		);
		assert!(is_fast_mode_unsupported(entitlement), "{entitlement:?}");
	}

	#[test]
	fn concurrency_caps_are_transient_not_usage_limits() {
		let msg = "Too many concurrent requests";
		let r429 = classify(Some(429), None, None, None, msg);
		assert!(!r429.contains(ErrorFlags::USAGE_LIMIT), "{r429:?}");
		assert!(r429.contains(ErrorFlags::TRANSIENT), "{r429:?}");
		assert_eq!(parse_rate_limit_reason(msg), RateLimitReason::ConcurrentLimit);
		assert!(is_concurrency_cap_exclusion(Some(429), Some(msg)));
		assert!(!is_usage_limit_outcome(Some(429), Some(msg)));

		// A 402 is categorically a billing cap: concurrency wording still rotates.
		let r402 = classify(Some(402), None, None, None, msg);
		assert!(r402.contains(ErrorFlags::USAGE_LIMIT), "{r402:?}");
		assert!(!is_concurrency_cap_exclusion(Some(402), Some(msg)));
		assert!(is_usage_limit_outcome(Some(402), Some(msg)));
	}

	#[test]
	fn dashscope_token_limit_stays_a_transient_throttle() {
		let msg = "You exceeded your current quota, please check your plan and billing details. \
		           See https://help.aliyun.com/zh/model-studio/error-code#token-limit";
		assert!(is_dashscope_token_limit_text(msg));
		assert_eq!(parse_rate_limit_reason(msg), RateLimitReason::RateLimitExceeded);
		assert!(!matches_usage_limit_text(msg));
		assert!(!is_usage_limit_outcome(Some(429), Some(msg)));
		let id = classify(Some(429), None, None, None, msg);
		assert!(!id.contains(ErrorFlags::USAGE_LIMIT), "{id:?}");
	}

	#[test]
	fn english_and_chinese_quotas_rotate() {
		let english = "You exceeded your current quota, please check your plan and billing details";
		assert_eq!(parse_rate_limit_reason(english), RateLimitReason::QuotaExhausted);
		let id = classify(Some(429), None, None, None, english);
		assert!(is_usage_limit(id), "{id:?}");
		assert!(is_usage_limit_outcome(Some(429), Some(english)));

		let chinese = "429 已达到 5 小时的使用上限。您的限额将在 2026-08-06 20:06:00 重置。";
		assert_eq!(parse_rate_limit_reason(chinese), RateLimitReason::QuotaExhausted);
		assert!(matches_usage_limit_text(chinese));
		let cn_id = classify(Some(429), None, None, None, chinese);
		assert!(is_usage_limit(cn_id), "{cn_id:?}");

		let cn_spent = "额度已用完";
		assert_eq!(parse_rate_limit_reason(cn_spent), RateLimitReason::QuotaExhausted);

		// Per-minute CN wording is transient, never an account quota.
		let cn_transient = "每分钟请求数已达上限";
		assert_ne!(parse_rate_limit_reason(cn_transient), RateLimitReason::QuotaExhausted);
		assert!(!matches_usage_limit_text(cn_transient));
	}

	#[test]
	fn subscription_cap_and_per_minute_split() {
		let capped = "Your plan rate limit has been reached";
		assert_eq!(parse_rate_limit_reason(capped), RateLimitReason::QuotaExhausted);

		let per_minute = "Rate limit is 20 requests per minute";
		assert_eq!(parse_rate_limit_reason(per_minute), RateLimitReason::RateLimitExceeded);
	}

	#[test]
	fn auth_statuses_and_account_policy() {
		let invalid_key = classify(Some(401), None, None, None, r#"{"error":{"code":"invalid_key"}}"#);
		assert!(invalid_key.contains(ErrorFlags::AUTH_FAILED), "{invalid_key:?}");

		let api_key = classify(Some(401), None, None, None, "invalid api key");
		assert!(api_key.contains(ErrorFlags::AUTH_FAILED), "{api_key:?}");

		let forbidden = classify(Some(403), None, None, None, "forbidden");
		assert!(forbidden.contains(ErrorFlags::AUTH_FAILED), "{forbidden:?}");

		let cyber = classify(Some(403), None, Some("openai-codex"), None, "cyber_policy");
		assert!(is_account_policy_error(cyber), "{cyber:?}");
		assert!(cyber.contains(ErrorFlags::CONTENT_BLOCKED), "{cyber:?}");
	}

	#[test]
	fn codex_model_entitlement_matches_only_the_requested_model() {
		let msg = "The 'gpt-5-codex' model is not supported when using Codex with a ChatGPT account.";
		let matching = classify(Some(400), None, Some("openai-codex"), Some("openai/gpt-5-codex"), msg);
		assert!(is_account_policy_error(matching), "{matching:?}");

		let other = classify(Some(400), None, Some("openai-codex"), Some("gpt-4.1"), msg);
		assert!(!is_account_policy_error(other), "{other:?}");
	}

	#[test]
	fn context_overflow_versus_payload_rejection() {
		let overflow = classify(Some(400), None, None, None, "prompt is too long");
		assert!(overflow.contains(ErrorFlags::CONTEXT_OVERFLOW), "{overflow:?}");
		assert!(!overflow.contains(ErrorFlags::PAYLOAD_REJECTED), "{overflow:?}");

		let payload = classify(Some(413), None, None, None, "request entity too large");
		assert!(payload.contains(ErrorFlags::PAYLOAD_REJECTED), "{payload:?}");
		assert!(!payload.contains(ErrorFlags::CONTEXT_OVERFLOW), "{payload:?}");

		// Bare 413 (no body) is deliberately dual-flagged: local headroom decides.
		let ambiguous = classify(Some(413), None, None, None, "413 status code (no body)");
		assert!(ambiguous.contains(ErrorFlags::PAYLOAD_REJECTED), "{ambiguous:?}");
		assert!(ambiguous.contains(ErrorFlags::CONTEXT_OVERFLOW), "{ambiguous:?}");

		// A 413 naming the token budget stays overflow-only.
		let token = classify(Some(413), None, None, None, "input is too long for requested model");
		assert!(token.contains(ErrorFlags::CONTEXT_OVERFLOW), "{token:?}");
		assert!(!token.contains(ErrorFlags::PAYLOAD_REJECTED), "{token:?}");
	}

	#[test]
	fn classify_message_two_phase_finalization() {
		let overflow = classify_message("input is too long for requested model");
		assert!(overflow.contains(ErrorFlags::CONTEXT_OVERFLOW), "{overflow:?}");

		// Pre-existing status-inferred payload bit is dropped once text proves overflow.
		let merged = classify_message_with(
			"input is too long for requested model",
			ErrorFlags::from_bits(413),
			Some(413),
			None,
			None,
			None,
		);
		assert!(merged.contains(ErrorFlags::CONTEXT_OVERFLOW), "{merged:?}");
		assert!(!merged.contains(ErrorFlags::PAYLOAD_REJECTED), "{merged:?}");

		// Deterministic local-model tool-call JSON failure never retries.
		let llama = classify_message_with(
			"failed to parse tool call arguments as json",
			ErrorFlags::create(ErrorFlags::TRANSIENT),
			Some(500),
			None,
			None,
			None,
		);
		assert!(!llama.contains(ErrorFlags::TRANSIENT), "{llama:?}");
	}

	#[test]
	fn opaque_status_bodies() {
		assert!(is_opaque_status_body("429"));
		assert!(is_opaque_status_body("HTTP 429 error"));
		assert!(!is_opaque_status_body("Too many requests"));
		assert!(is_opaque_status_body("额度不足"));
	}

	#[test]
	fn oauth_expiry_and_invalidated_tokens() {
		assert!(is_oauth_expiry("invalid_grant"));
		assert!(is_definitive_oauth_failure("refresh token has expired"));
		assert!(is_oauth_expiry("token revoked"));
		assert!(is_oauth_expiry("401"));
		assert!(!is_oauth_expiry("401 network error"));
		assert!(!is_oauth_expiry("request timeout"));
		assert!(!is_oauth_expiry("429 too many requests"));

		assert!(is_invalidated_oauth_token_error("provider: invalidated oauth token"));
		assert!(!is_invalidated_oauth_token_error("invalid token"));

		// Exact identifier match: `invalid_grant_type` is a distinct, live grant
		// state and must never be read as `invalid_grant`.
		assert!(!is_definitive_oauth_failure(r#"{"error":"invalid_grant_type"}"#));
		assert!(is_definitive_oauth_failure(r#"{"error":"invalid_grant"}"#));
		assert!(!is_oauth_revocation_message(r#"{"error":"invalid_grant_type"}"#));

		// Credential fencing uses the marker-only accessor: the bare status that
		// `HttpResponse::error_for()` embeds never disables a stored grant.
		assert!(!is_oauth_revocation_message("token refresh failed (HTTP 401)"));
		assert!(is_oauth_revocation_message("TokenRefresh failed (HTTP 400): invalid_grant"));
		assert!(is_oauth_revocation_message("refresh token has expired"));
		// The donor 401 heuristic remains on `is_oauth_expiry` for transport-owning
		// callers; it must not be used for credential fencing.
		assert!(is_oauth_expiry("token refresh failed (HTTP 401)"));
	}

	#[test]
	fn auth_retryability() {
		assert!(is_auth_retryable_error(Some(401), "unauthorized"));
		assert!(is_auth_retryable_error(Some(403), "forbidden"));
		assert!(is_auth_retryable_error(Some(429), "usage_limit_reached"));
		assert!(is_auth_retryable_error(Some(402), "insufficient_quota"));
		assert!(is_auth_retryable_error(None, "invalidated oauth token"));
		assert!(!is_auth_retryable_error(Some(429), "Too many requests"));
		assert!(!is_auth_retryable_error(Some(500), "internal server error"));
		assert!(!is_auth_retryable_error(Some(429), "Too many concurrent requests"));
	}

	#[test]
	fn structured_google_rate_limit_details_win() {
		let body = r#"{"error":{"code":429,"message":"quota","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"}]}}"#;
		assert_eq!(parse_rate_limit_reason(body), RateLimitReason::QuotaExhausted);
		assert!(is_usage_limit_outcome(Some(429), Some(body)));

		let credits = body.replace("QUOTA_EXHAUSTED", "INSUFFICIENT_G1_CREDITS_BALANCE");
		assert_eq!(
			parse_rate_limit_reason(&credits),
			RateLimitReason::InsufficientG1CreditsBalance
		);
		assert!(is_usage_limit_outcome(Some(429), Some(credits.as_str())));

		let short = body.replace("QUOTA_EXHAUSTED", "RATE_LIMIT_EXCEEDED");
		assert_eq!(parse_rate_limit_reason(&short), RateLimitReason::RateLimitExceeded);
		assert!(!is_usage_limit_outcome(Some(429), Some(short.as_str())));

		let long = format!(r#"{short} "retryDelay":"600s""#);
		assert_eq!(parse_rate_limit_reason(&long), RateLimitReason::QuotaExhausted);
	}

	#[test]
	fn backoff_schedule_and_jitter_band() {
		assert_eq!(
			calculate_rate_limit_backoff_ms(RateLimitReason::QuotaExhausted),
			1_800_000
		);
		assert_eq!(
			calculate_rate_limit_backoff_ms(RateLimitReason::RateLimitExceeded),
			30_000
		);
		assert_eq!(
			calculate_rate_limit_backoff_ms(RateLimitReason::ConcurrentLimit),
			5_000
		);
		assert_eq!(calculate_rate_limit_backoff_ms(RateLimitReason::ServerError), 20_000);
		assert_eq!(calculate_rate_limit_backoff_ms(RateLimitReason::Unknown), 1_800_000);
		for jitter in [0, 1, 29_999, 30_000, 60_000] {
			let delay = calculate_rate_limit_backoff_ms_with_jitter(RateLimitReason::ModelCapacityExhausted, jitter);
			assert!((45_000..75_000).contains(&delay), "jitter {jitter} -> {delay}");
		}
	}

	#[test]
	fn finalize_bundle_and_stop_reason() {
		let finished = finalize(
			"rate_limit_error: fast mode requires extra usage",
			&FinalizeOptions {
				api:          None,
				provider:     Some("anthropic"),
				model:        None,
				aborted:      false,
				error_status: Some(429),
			},
		);
		assert!(is_fast_mode_unsupported(finished.id), "{finished:?}");
		assert_eq!(finished.status, Some(429));
		assert_eq!(finished.stop_reason, FinalizedStopReason::Error);

		let aborted = finalize(
			"aborted",
			&FinalizeOptions {
				api:          None,
				provider:     None,
				model:        None,
				aborted:      true,
				error_status: None,
			},
		);
		assert_eq!(aborted.stop_reason, FinalizedStopReason::Aborted);
	}

	#[test]
	fn status_parsing_from_text() {
		assert_eq!(status_from_message("HTTP 503 service unavailable"), Some(503));
		assert_eq!(status_from_message("status_code: 401"), Some(401));
		assert_eq!(status_from_message("429 Too Many Requests"), Some(429));
		assert_eq!(status_from_message("plain text"), None);
	}

	#[test]
	fn transient_transport_and_timeout() {
		let timeout = classify(None, None, None, None, "operation timed out");
		assert!(timeout.contains(ErrorFlags::TIMEOUT), "{timeout:?}");
		assert!(timeout.contains(ErrorFlags::TRANSIENT), "{timeout:?}");

		let reset = classify(None, None, None, None, "connection reset before headers");
		assert!(reset.contains(ErrorFlags::TRANSIENT), "{reset:?}");
		assert!(!reset.contains(ErrorFlags::TIMEOUT), "{reset:?}");
	}

	#[test]
	fn bit_operations_compose() {
		let combined = flags(0x2_0000) | flags(0x4_0000);
		assert!(combined.contains(ErrorFlags::TRANSIENT));
		assert!(combined.contains(ErrorFlags::TIMEOUT));
		assert!(combined.intersects(ErrorFlags::TRANSIENT | ErrorFlags::AUTH_FAILED));
		assert_eq!(combined.without(ErrorFlags::TRANSIENT), flags(0x4_0000));
		assert_eq!(combined & ErrorFlags::TIMEOUT, ErrorFlags::TIMEOUT);
	}
}
