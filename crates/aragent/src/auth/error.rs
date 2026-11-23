//! Typed login/refresh errors.
//!
//! Anchors: `packages/ai/src/error/oauth.ts:1` (`OAuthError{kind,…}`),
//! `packages/ai/src/error/auth-classify.ts:1` (`isDefinitiveOAuthFailure`).
//!
//! Two jobs beyond carrying a message:
//!
//! - **Redaction.** No token, code or secret ever reaches a formatted error.
//!   Token-endpoint bodies are summarised to their `error`/`message` fields and
//!   every long credential-looking run is masked, because the same message may
//!   land in stderr, an RPC frame or a log file.
//! - **Classification.** `is_definitive()` (a stale grant must be disabled) is
//!   distinct from `is_transient()` (a retry may succeed), mirroring
//!   `isDefinitiveOAuthFailure`/`isAuthRetryable`.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthErrorKind {
	/// Non-2xx from an endpoint that is not the token endpoint.
	Http,
	/// Response body/shape did not satisfy the flow.
	Validation,
	TokenExchange,
	TokenRefresh,
	Polling,
	Timeout,
	DeviceAuth,
	Configuration,
	Provisioning,
	Discovery,
	/// User (or host) cancelled; never a login fallback.
	Cancelled,
	/// Provider id absent from the registry: fail closed.
	UnknownProvider,
	ApiKeyRequired,
	PromptRequired,
	Storage,
}

impl AuthErrorKind {
	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Http => "http",
			Self::Validation => "validation",
			Self::TokenExchange => "token-exchange",
			Self::TokenRefresh => "token-refresh",
			Self::Polling => "polling",
			Self::Timeout => "timeout",
			Self::DeviceAuth => "device-auth",
			Self::Configuration => "configuration",
			Self::Provisioning => "provisioning",
			Self::Discovery => "discovery",
			Self::Cancelled => "cancelled",
			Self::UnknownProvider => "unknown-provider",
			Self::ApiKeyRequired => "api-key-required",
			Self::PromptRequired => "prompt-required",
			Self::Storage => "storage",
		}
	}
}

#[derive(Debug)]
pub struct AuthError {
	pub kind: AuthErrorKind,
	pub provider: Option<String>,
	pub status: Option<u16>,
	pub message: String,
	/// The provider explicitly said the grant is dead (`invalid_grant`,
	/// `invalid_token`, expired refresh token). Only then may a stored
	/// credential be disabled: a 429/5xx/WAF/parse/timeout answer never is.
	pub grant_dead: bool,
}

impl AuthError {
	pub fn new(kind: AuthErrorKind, message: impl AsRef<str>) -> Self {
		Self {
			kind,
			provider: None,
			status: None,
			message: redact(message.as_ref()),
			grant_dead: false,
		}
	}

	pub fn for_provider(mut self, provider: &str) -> Self {
		self.provider = Some(provider.to_string());
		self
	}

	pub fn with_status(mut self, status: u16) -> Self {
		self.status = Some(status);
		self
	}

	/// Mark this failure as evidence that the stored grant is unusable.
	pub fn with_grant_dead(mut self) -> Self {
		self.grant_dead = true;
		self
	}

	#[must_use]
	pub fn cancelled(provider: &str) -> Self {
		Self::new(AuthErrorKind::Cancelled, "Login cancelled").for_provider(provider)
	}

	#[must_use]
	pub fn unknown_provider(provider: &str) -> Self {
		Self::new(AuthErrorKind::UnknownProvider, format!("Unknown login provider: {provider}"))
			.for_provider(provider)
	}

	#[must_use]
	pub fn config(provider: &str, message: impl AsRef<str>) -> Self {
		Self::new(AuthErrorKind::Configuration, message).for_provider(provider)
	}

	#[must_use]
	pub fn api_key_required(provider: &str) -> Self {
		Self::new(AuthErrorKind::ApiKeyRequired, "An API key is required").for_provider(provider)
	}

	#[must_use]
	pub fn prompt_required(provider: &str) -> Self {
		Self::new(AuthErrorKind::PromptRequired, "This login requires interactive input")
			.for_provider(provider)
	}

	/// A provider-declared dead grant (`invalid_grant`-class answer on the token
	/// endpoint) or a rejected credential (`401`/`403` with such an identifier).
	///
	/// Rate limits, provider outages, WAF denials, malformed bodies, network and
	/// cancellation failures are **not** definitive: the stored credential must
	/// survive them (`auth-classify.ts`, `flags.ts:253-257`).
	#[must_use]
	pub fn is_definitive(&self) -> bool {
		self.grant_dead
			&& !matches!(
				self.kind,
				AuthErrorKind::Cancelled | AuthErrorKind::Timeout | AuthErrorKind::Polling
			)
	}

	#[must_use]
	pub fn is_transient(&self) -> bool {
		!self.is_definitive()
			&& matches!(
				self.kind,
				AuthErrorKind::Timeout
					| AuthErrorKind::Polling
					| AuthErrorKind::Http
					| AuthErrorKind::Validation
			)
	}
}

impl fmt::Display for AuthError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.kind.as_str())?;
		if let Some(provider) = &self.provider {
			write!(f, " ({provider})")?;
		}
		if let Some(status) = self.status {
			write!(f, " HTTP {status}")?;
		}
		write!(f, ": {}", self.message)
	}
}

impl std::error::Error for AuthError {}

impl From<rusqlite::Error> for AuthError {
	fn from(err: rusqlite::Error) -> Self {
		Self::new(AuthErrorKind::Storage, format!("credential store: {}", redact(&err.to_string())))
	}
}

impl From<std::io::Error> for AuthError {
	fn from(err: std::io::Error) -> Self {
		Self::new(AuthErrorKind::Storage, format!("io: {}", redact(&err.to_string())))
	}
}

/// Mask credentials in free-form text.
///
/// Covers the two shapes that actually leak: JSON fields whose name says
/// token/secret/key, and bare bearer/API-key-looking tokens in log lines.
pub fn redact(text: &str) -> String {
	const SECRET_FIELDS: &[&str] = &[
		"access_token",
		"refresh_token",
		"id_token",
		"token",
		"secret",
		"password",
		"code",
		"code_verifier",
		"key",
	];

	// Exact known values first: a field-shaped scan cannot see a credential
	// embedded in a message string.
	let known = crate::auth::known_secrets::redact(text);
	let text: &str = &known;
	let mut out = String::with_capacity(text.len());
	let mut cursor = 0;
	while let Some(rel) = text[cursor..].find('"') {
		let open = cursor + rel;
		out.push_str(&text[cursor..=open]);
		let Some(close_rel) = text[open + 1..].find('"') else {
			out.push_str(&text[open + 1..]);
			return redact_bearer_tokens(&out);
		};
		let close = open + 1 + close_rel;
		let quoted = &text[open + 1..close];
		out.push_str(quoted);
		out.push('"');
		cursor = close + 1;
		if !SECRET_FIELDS.contains(&quoted.to_ascii_lowercase().as_str()) {
			continue;
		}
		// Mask the next quoted string, keeping the separators between them.
		let tail = &text[cursor..];
		let Some(next_open_rel) = tail.find('"') else {
			// A secret field with no following quoted value: nothing to mask,
			// and `cursor` cannot advance, so stop instead of re-scanning.
			out.push_str(&text[cursor..]);
			cursor = text.len();
			break;
		};
		out.push_str(&tail[..next_open_rel]);
		let value_open = cursor + next_open_rel;
		let Some(value_close_rel) = text[value_open + 1..].find('"') else {
			out.push_str("[REDACTED]");
			return redact_bearer_tokens(&out);
		};
		out.push_str("[REDACTED]");
		cursor = value_open + 1 + value_close_rel + 1;
	}

	// Quote-free text never entered the loop above: keep the whole remainder.
	out.push_str(&text[cursor..]);
	redact_bearer_tokens(&redact_url_queries(&redact_jwts(&out)))
}

/// Mask secret query parameters (`?code=`, `&access_token=`) in free-form text:
/// a redirect URL echoed by a provider must not ship the grant.
fn redact_url_queries(text: &str) -> String {
	const SECRET_QUERY_KEYS: &[&str] = &[
		"access_token",
		"refresh_token",
		"id_token",
		"token",
		"code",
		"code_verifier",
		"client_secret",
		"secret",
		"password",
		"key",
		"api_key",
		"auth",
	];

	// Only `scheme://…` spans carry query strings. Redacting a bare `?key=` in
	// prose or code (`cond ? key = value`) would corrupt ordinary text.
	let mut out = String::with_capacity(text.len());
	let mut index = 0usize;
	while let Some(rel) = text[index..].find("://") {
		let url_start = index + rel + 3;
		let url_end = text[url_start..]
			// `]`/`}` are not terminators: `[REDACTED]` already sits inside the
			// span and cutting there would split the marker.
			.find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>' | ')' | '`'))
			.map(|rel| url_start + rel)
			.unwrap_or(text.len());
		out.push_str(&text[index..url_start]);
		out.push_str(&redact_query_span(&text[url_start..url_end], SECRET_QUERY_KEYS));
		index = url_end;
	}
	out.push_str(&text[index..]);
	out
}

fn redact_query_span(text: &str, secret_keys: &[&str]) -> String {
	let mut out = String::with_capacity(text.len());
	let mut index = 0usize;
	while index < text.len() {
		let Some(ch) = text[index..].chars().next() else {
			break;
		};
		if ch != '?' && ch != '&' {
			out.push(ch);
			index += ch.len_utf8();
			continue;
		}
		out.push(ch);
		let start = index + 1;
		let Some(eq_rel) = text[start..].find('=') else {
			out.push_str(&text[start..]);
			break;
		};
		let eq = start + eq_rel;
		let key = &text[start..eq];
		out.push_str(key);
		out.push('=');
		let value_start = eq + 1;
		let value_end = text[value_start..]
			.find(|c: char| c == '&' || c == '#' || c.is_whitespace() || matches!(c, '"' | '\'' | ')'))
			.map(|rel| value_start + rel)
			.unwrap_or(text.len());
		if secret_keys
			.iter()
			.any(|known| known.eq_ignore_ascii_case(key.trim()))
		{
			out.push_str("[REDACTED]");
		} else {
			out.push_str(&text[value_start..value_end]);
		}
		index = value_end;
	}
	out
}

/// Mask JWT-shaped runs (`eyJ…`) wherever they appear.
fn redact_jwts(text: &str) -> String {
	let mut out = String::with_capacity(text.len());
	let mut rest = text;
	while let Some(position) = rest.find("eyJ") {
		let (head, tail) = rest.split_at(position);
		out.push_str(head);
		let end = tail
			.find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ',' | ';' | ')' | '}'))
			.unwrap_or(tail.len());
		if end >= 20 {
			out.push_str("[REDACTED-JWT]");
		} else {
			out.push_str(&tail[..end]);
		}
		rest = &tail[end..];
	}
	out.push_str(rest);
	out
}

fn redact_bearer_tokens(text: &str) -> String {
	const SCHEMES: &[&str] =
		&["Bearer ", "sk-", "sk_", "xai-", "nvapi-", "gho_", "csk-", "fw_", "bt_", "fpk_"];

	let mut out = String::with_capacity(text.len());
	let mut rest = text;
	'outer: while !rest.is_empty() {
		for scheme in SCHEMES {
			if let Some(tail) = rest.strip_prefix(scheme) {
				out.push_str(scheme);
				out.push_str("[REDACTED]");
				let end = tail
					.find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ',' | ';' | ')'))
					.unwrap_or(tail.len());
				rest = &tail[end..];
				continue 'outer;
			}
		}
		let mut chars = rest.chars();
		let Some(first) = chars.next() else { break };
		out.push(first);
		rest = chars.as_str();
	}
	out
}

/// Mask bearer tokens, known provider secret prefixes, JWTs, and secret URL
/// query parameters in arbitrary assistant prose/code without corrupting
/// ordinary JSON keys (`"code"`, `"key"`) or identifiers (`skip_serializing_if`).
/// Credentials this process resolved are masked first, by exact value: shape
/// heuristics cannot see an opaque token, and that layer has no off switch.
pub fn redact_free_text(text: &str) -> String {
	let known = crate::auth::known_secrets::redact(text);
	let after_queries = redact_url_queries(&known);
	let after_jwt = redact_strict_jwts(&after_queries);
	redact_boundary_tokens(&after_jwt)
}

/// Mask every string value and dynamic JSON key inside an arbitrary JSON payload.
///
/// Unconditional generic traversal over JSON values. Does NOT inspect keys
/// (such as `"type"`, `"id"`, or `"role"`) to select privileged schemas,
/// preventing untrusted input (e.g. tool arguments, tool results, execution
/// intents, worker deliveries) from spoofing structural schemas.
pub fn redact_json_strings(value: &mut serde_json::Value) {
	match value {
		serde_json::Value::String(text) => {
			let redacted = redact_free_text(text);
			if redacted != *text {
				*text = redacted;
			}
		},
		serde_json::Value::Array(items) => {
			items.iter_mut().for_each(redact_json_strings);
		},
		serde_json::Value::Object(map) => {
			redact_dynamic_object(map);
		},
		_ => {},
	}
}

// ponytail: collision disambiguation uses explicit `[REDACTED_COLLISION:{counter}]` so unredacted keys are never mutated
pub(crate) fn insert_dynamic_entries_two_phase(
	target: &mut serde_json::Map<String, serde_json::Value>,
	entries: Vec<(String, serde_json::Value)>,
) {
	let mut unredacted = Vec::new();
	let mut modified = Vec::new();
	for (key, mut val) in entries {
		redact_json_strings(&mut val);
		let redacted_key = redact_free_text(&key);
		if redacted_key == key {
			unredacted.push((key, val));
		} else {
			modified.push((redacted_key, val));
		}
	}
	// Phase 1: Insert all unredacted keys first. Literal keys that never had secrets
	// (including literal [REDACTED] or [REDACTED_COLLISION:N]) are never displaced or overwritten.
	for (key, val) in unredacted {
		target.insert(key, val);
	}
	// Phase 2: Insert modified keys. If a redacted key collides with an existing key,
	// assign an explicit documented collision marker [REDACTED_COLLISION:N] without data loss.
	for (redacted_key, val) in modified {
		let mut final_key = redacted_key;
		let mut counter = 1usize;
		while target.contains_key(&final_key) {
			final_key = format!("[REDACTED_COLLISION:{counter}]");
			counter += 1;
		}
		target.insert(final_key, val);
	}
}

pub(crate) fn redact_dynamic_object(map: &mut serde_json::Map<String, serde_json::Value>) {
	let old_entries: Vec<(String, serde_json::Value)> = std::mem::take(map).into_iter().collect();
	insert_dynamic_entries_two_phase(map, old_entries);
}

/// Redact a session header payload, preserving structural `id`, `parentSession`, and
/// projection kept/anchor references while masking metadata and synthetic messages.
pub fn redact_session_payload(payload: &mut serde_json::Value) {
	if let Some(map) = payload.as_object_mut() {
		redact_session_payload_map(map);
	} else {
		redact_json_strings(payload);
	}
}

fn redact_session_payload_map(map: &mut serde_json::Map<String, serde_json::Value>) {
	let old_entries: Vec<(String, serde_json::Value)> = std::mem::take(map).into_iter().collect();
	let mut dynamic_entries = Vec::new();
	for (key, mut val) in old_entries {
		match key.as_str() {
			"id" | "parentSession" | "type" | "version" | "timestamp" => {
				map.insert(key, val);
			},
			"durableProjection" => {
				redact_stored_projection_value(&mut val);
				map.insert(key, val);
			},
			"title" | "cwd" | "model" | "provider" | "titleSource"
			| "providerPromptCacheKey" => {
				redact_json_strings(&mut val);
				map.insert(key, val);
			},
			"additionalDirectories" | "previousSessionFiles" => {
				redact_json_strings(&mut val);
				map.insert(key, val);
			},
			_ => {
				dynamic_entries.push((key, val));
			},
		}
	}
	insert_dynamic_entries_two_phase(map, dynamic_entries);
}

/// Redact stored projection, preserving `kept` message IDs and `anchor` while
/// masking native synthetic compaction messages.
pub fn redact_stored_projection_value(proj_val: &mut serde_json::Value) {
	let Some(map) = proj_val.as_object_mut() else {
		return;
	};
	if let Some(synthetic) = map.get_mut("synthetic").and_then(serde_json::Value::as_array_mut) {
		for msg in synthetic {
			redact_synthetic_message_value(msg);
		}
	}
	let old_entries: Vec<(String, serde_json::Value)> = std::mem::take(map).into_iter().collect();
	let mut dynamic_entries = Vec::new();
	for (key, val) in old_entries {
		match key.as_str() {
			"kept" | "anchor" | "firstKeptEntryId" | "synthetic" => {
				map.insert(key, val);
			},
			_ => {
				dynamic_entries.push((key, val));
			},
		}
	}
	insert_dynamic_entries_two_phase(map, dynamic_entries);
}

/// Redact a synthetic projection message (native `pi_agent::Message` or wrapped `MessageEntry`).
pub fn redact_synthetic_message_value(value: &mut serde_json::Value) {
	let Some(map) = value.as_object_mut() else {
		redact_json_strings(value);
		return;
	};
	if map.get("type").and_then(serde_json::Value::as_str) == Some("message")
		&& map.contains_key("message")
	{
		redact_message_payload_map(map);
		return;
	}

	if let Some(serde_json::Value::String(content)) = map.get_mut("content") {
		let redacted = redact_free_text(content);
		if redacted != *content {
			*content = redacted;
		}
	}
	if let Some(parts) = map.get_mut("parts").and_then(serde_json::Value::as_array_mut) {
		for part in parts {
			redact_content_part_value(part);
		}
	}
	for key in ["tool_calls", "toolCalls"] {
		if let Some(calls) = map.get_mut(key).and_then(serde_json::Value::as_array_mut) {
			for call in calls {
				if let Some(call_map) = call.as_object_mut() {
					if let Some(args) = call_map.get_mut("arguments") {
						redact_json_strings(args);
					}
					let old_call: Vec<(String, serde_json::Value)> =
						std::mem::take(call_map).into_iter().collect();
					let mut dynamic_call = Vec::new();
					for (k, v) in old_call {
						match k.as_str() {
							"id" | "name" | "arguments" => {
								call_map.insert(k, v);
							},
							_ => {
								dynamic_call.push((k, v));
							},
						}
					}
					insert_dynamic_entries_two_phase(call_map, dynamic_call);
				} else {
					redact_json_strings(call);
				}
			}
		}
	}
	for key in ["errorMessage", "error_message"] {
		if let Some(serde_json::Value::String(err)) = map.get_mut(key) {
			let redacted = redact_free_text(err);
			if redacted != *err {
				*err = redacted;
			}
		}
	}
	for key in ["name", "api", "provider", "model", "customType", "custom_type", "attribution"] {
		if let Some(serde_json::Value::String(s)) = map.get_mut(key) {
			let redacted = redact_free_text(s);
			if redacted != *s {
				*s = redacted;
			}
		}
	}
	for key in ["disabledFeatures", "disabled_features"] {
		if let Some(features) = map.get_mut(key).and_then(serde_json::Value::as_array_mut) {
			for f in features {
				if let serde_json::Value::String(s) = f {
					let redacted = redact_free_text(s);
					if redacted != *s {
						*s = redacted;
					}
				}
			}
		}
	}
	let old_entries: Vec<(String, serde_json::Value)> = std::mem::take(map).into_iter().collect();
	let mut dynamic_entries = Vec::new();
	for (key, val) in old_entries {
		match key.as_str() {
			"role" | "tool_call_id" | "toolCallId" | "is_error" | "isError" | "usage"
			| "stopReason" | "stop_reason" | "streamAttempt" | "stream_attempt"
			| "content" | "parts" | "tool_calls" | "toolCalls" | "errorMessage"
			| "error_message" | "name" | "api" | "provider" | "model" | "customType"
			| "custom_type" | "attribution" | "disabledFeatures" | "disabled_features" => {
				map.insert(key, val);
			},
			_ => {
				dynamic_entries.push((key, val));
			},
		}
	}
	insert_dynamic_entries_two_phase(map, dynamic_entries);
}

/// Redact a message entry payload, preserving structural `id` and `parentId` while
/// masking message contents, tool arguments, and dynamic details.
pub fn redact_message_payload(payload: &mut serde_json::Value) {
	let Some(map) = payload.as_object_mut() else {
		redact_json_strings(payload);
		return;
	};
	if map.contains_key("message") {
		redact_message_payload_map(map);
	} else if map.contains_key("role") {
		redact_session_message_map(map);
	} else {
		redact_message_payload_map(map);
	}
}

fn redact_message_payload_map(map: &mut serde_json::Map<String, serde_json::Value>) {
	if let Some(msg_val) = map.get_mut("message") {
		redact_session_message_value(msg_val);
	}
	let old_entries: Vec<(String, serde_json::Value)> = std::mem::take(map).into_iter().collect();
	let mut dynamic_entries = Vec::new();
	for (key, val) in old_entries {
		match key.as_str() {
			"id" | "parentId" | "type" | "timestamp" | "message" => {
				map.insert(key, val);
			},
			_ => {
				dynamic_entries.push((key, val));
			},
		}
	}
	insert_dynamic_entries_two_phase(map, dynamic_entries);
}

/// Redact an inner session message, preserving `toolCallId` and tool-call `id` while
/// masking tool arguments (including user fields named `id`), prose, extras, and details.
pub fn redact_session_message_value(message: &mut serde_json::Value) {
	if let Some(map) = message.as_object_mut() {
		redact_session_message_map(map);
	} else {
		redact_json_strings(message);
	}
}

fn redact_session_message_map(map: &mut serde_json::Map<String, serde_json::Value>) {
	if let Some(content) = map.get_mut("content") {
		match content {
			serde_json::Value::String(text) => {
				let redacted = redact_free_text(text);
				if redacted != *text {
					*text = redacted;
				}
			},
			serde_json::Value::Array(parts) => {
				for part in parts {
					redact_content_part_value(part);
				}
			},
			_ => {
				redact_json_strings(content);
			},
		}
	}
	if let Some(serde_json::Value::String(err)) = map.get_mut("errorMessage") {
		let redacted = redact_free_text(err);
		if redacted != *err {
			*err = redacted;
		}
	}
	for key in ["api", "provider", "model", "attribution"] {
		if let Some(serde_json::Value::String(s)) = map.get_mut(key) {
			let redacted = redact_free_text(s);
			if redacted != *s {
				*s = redacted;
			}
		}
	}
	if let Some(features) = map.get_mut("disabledFeatures").and_then(serde_json::Value::as_array_mut) {
		for f in features {
			if let serde_json::Value::String(s) = f {
				let redacted = redact_free_text(s);
				if redacted != *s {
					*s = redacted;
				}
			}
		}
	}
	let old_entries: Vec<(String, serde_json::Value)> = std::mem::take(map).into_iter().collect();
	let mut dynamic_entries = Vec::new();
	for (key, val) in old_entries {
		match key.as_str() {
			"toolCallId" | "toolName" | "role" | "isError" | "timestamp" | "usage"
			| "stopReason" | "content" | "errorMessage" | "api" | "provider" | "model"
			| "attribution" | "disabledFeatures" => {
				map.insert(key, val);
			},
			_ => {
				// Passthrough fields on SessionMessage (details, providerPayload, etc.)
				dynamic_entries.push((key, val));
			},
		}
	}
	insert_dynamic_entries_two_phase(map, dynamic_entries);
}

fn redact_content_part_value(part: &mut serde_json::Value) {
	let Some(part_map) = part.as_object_mut() else {
		redact_json_strings(part);
		return;
	};
	let kind = part_map.get("type").and_then(serde_json::Value::as_str);
	match kind {
		Some("toolCall") => {
			if let Some(args) = part_map.get_mut("arguments") {
				redact_json_strings(args);
			}
			let old_part: Vec<(String, serde_json::Value)> =
				std::mem::take(part_map).into_iter().collect();
			let mut dynamic_entries = Vec::new();
			for (k, v) in old_part {
				match k.as_str() {
					"id" | "name" | "type" | "arguments" => {
						part_map.insert(k, v);
					},
					_ => {
						dynamic_entries.push((k, v));
					},
				}
			}
			insert_dynamic_entries_two_phase(part_map, dynamic_entries);
		},
		Some("text") => {
			if let Some(serde_json::Value::String(text)) = part_map.get_mut("text") {
				let redacted = redact_free_text(text);
				if redacted != *text {
					*text = redacted;
				}
			}
			let old_part: Vec<(String, serde_json::Value)> =
				std::mem::take(part_map).into_iter().collect();
			let mut dynamic_entries = Vec::new();
			for (k, v) in old_part {
				match k.as_str() {
					"type" | "text" => {
						part_map.insert(k, v);
					},
					_ => {
						dynamic_entries.push((k, v));
					},
				}
			}
			insert_dynamic_entries_two_phase(part_map, dynamic_entries);
		},
		Some("thinking") => {
			let mut text_changed = false;
			if let Some(serde_json::Value::String(text)) = part_map.get_mut("thinking") {
				let redacted = redact_free_text(text);
				if redacted != *text {
					*text = redacted;
					text_changed = true;
				}
			}
			let old_part: Vec<(String, serde_json::Value)> =
				std::mem::take(part_map).into_iter().collect();
			let mut dynamic_entries = Vec::new();
			for (k, v) in old_part {
				match k.as_str() {
					"thinkingSignature" => {
						// Reasoning signature proves the exact original thinking text.
						// If text changed by redaction, signature is invalid.
						// Dropping the signature prevents replay of an invalid signature
						// and safely demotes the thinking block to unsigned replay.
						if !text_changed {
							part_map.insert(k, v);
						}
					},
					"type" | "thinking" => {
						part_map.insert(k, v);
					},
					_ => {
						dynamic_entries.push((k, v));
					},
				}
			}
			insert_dynamic_entries_two_phase(part_map, dynamic_entries);
		},
		Some("image") => {
			let old_part: Vec<(String, serde_json::Value)> =
				std::mem::take(part_map).into_iter().collect();
			let mut dynamic_entries = Vec::new();
			for (k, v) in old_part {
				match k.as_str() {
					"type" | "data" | "mimeType" | "mime_type" => {
						part_map.insert(k, v);
					},
					_ => {
						dynamic_entries.push((k, v));
					},
				}
			}
			insert_dynamic_entries_two_phase(part_map, dynamic_entries);
		},
		Some("redactedThinking") => {
			let old_part: Vec<(String, serde_json::Value)> =
				std::mem::take(part_map).into_iter().collect();
			let mut dynamic_entries = Vec::new();
			for (k, v) in old_part {
				match k.as_str() {
					"type" | "data" => {
						part_map.insert(k, v);
					},
					_ => {
						dynamic_entries.push((k, v));
					},
				}
			}
			insert_dynamic_entries_two_phase(part_map, dynamic_entries);
		},
		_ => {
			redact_json_strings(part);
		},
	}
}
fn redact_strict_jwts(text: &str) -> String {
	let bytes = text.as_bytes();
	let is_jwt_char = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_');
	let mut out = String::with_capacity(text.len());
	let mut i = 0usize;
	while i < bytes.len() {
		if text[i..].starts_with("eyJ")
			&& (i == 0 || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_'))
		{
			let mut p = i;
			while p < bytes.len() && is_jwt_char(bytes[p]) {
				p += 1;
			}
			if p - i >= 16 && bytes.get(p) == Some(&b'.') {
				let seg2_start = p + 1;
				let mut q = seg2_start;
				while q < bytes.len() && is_jwt_char(bytes[q]) {
					q += 1;
				}
				if q - seg2_start >= 16 && bytes.get(q) == Some(&b'.') {
					let seg3_start = q + 1;
					let mut r = seg3_start;
					while r < bytes.len() && is_jwt_char(bytes[r]) {
						r += 1;
					}
					if r - seg3_start >= 16 {
						out.push_str("[REDACTED-JWT]");
						i = r;
						continue;
					}
				}
			}
		}
		let ch = text[i..].chars().next().expect("char boundary");
		out.push(ch);
		i += ch.len_utf8();
	}
	out
}

fn redact_boundary_tokens(text: &str) -> String {
	// (prefix, minimum run length). Real credentials are long and mixed, so a
	// short run is prose or an identifier, not a secret.
	const PREFIXES: &[(&str, usize)] = &[
		("Bearer ", 20),
		("github_pat_", 20),
		("xoxb-", 20),
		("xoxa-", 20),
		("xoxp-", 20),
		("xoxr-", 20),
		("xoxs-", 20),
		("nvapi-", 20),
		("xai-", 20),
		("csk-", 20),
		("fpk-", 20),
		("fpk_", 20),
		("ghp_", 20),
		("gho_", 20),
		("ghu_", 20),
		("ghs_", 20),
		("ghr_", 20),
		("npm_", 20),
		("AIza", 20),
		("AKIA", 16),
		("ASIA", 16),
		("sk-", 20),
		("sk_", 20),
		("fw_", 20),
		("bt_", 20),
	];

	let bytes = text.as_bytes();
	let mut out = String::with_capacity(text.len());
	let mut i = 0usize;
	'outer: while i < bytes.len() {
		let at_boundary = i == 0 || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_');
		if at_boundary {
			for &(prefix, min_tail) in PREFIXES {
				if let Some(tail) = text[i..].strip_prefix(prefix) {
					if prefix == "Bearer " && tail.starts_with("[REDACTED") {
						break;
					}
					let run = tail
						.find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
						.unwrap_or(tail.len());
					// Real credentials are long and mixed: `Bearer authentication`
					// (a prose phrase) and `sk-learn-pipeline` (an identifier) stay.
					if run >= min_tail && tail[..run].bytes().any(|b| b.is_ascii_digit()) {
						if prefix == "Bearer " {
							out.push_str(prefix);
						}
						out.push_str("[REDACTED]");
						i += prefix.len() + run;
						continue 'outer;
					}
				}
			}
		}
		let ch = text[i..].chars().next().expect("char boundary");
		out.push(ch);
		i += ch.len_utf8();
	}
	out
}

/// `invalid_grant`-class OAuth identifiers: the provider rejected the stored
/// grant itself, so disabling the row is safe (`auth-classify.ts`).
#[must_use]
pub fn is_invalid_grant_body(body: &str) -> bool {
	const DEAD: &[&str] = &[
		"invalid_grant",
		"invalid_token",
		"invalid_refresh_token",
		"expired_token",
		"invalid_client",
		"unauthorized_client",
	];
	let Ok(json) = serde_json::from_str::<serde_json::Value>(body.trim()) else {
		return false;
	};
	["error", "error_code", "type"].iter().any(|field| {
		json
			.get(*field)
			.and_then(serde_json::Value::as_str)
			.map(|code| code.trim().to_ascii_lowercase())
			.is_some_and(|code| DEAD.contains(&code.as_str()))
	})
}

/// OAuth identifiers this client understands. Anything else in an error body
/// is treated as untrusted text: an attacker-shaped "identifier" must not be
/// copied into a message, and a grant is never mistaken for one by shape.
const KNOWN_OAUTH_ERROR_IDENTIFIERS: &[&str] = &[
	"invalid_request",
	"invalid_client",
	"invalid_grant",
	"invalid_grant_type",
	"invalid_token",
	"invalid_refresh_token",
	"expired_token",
	"invalid_scope",
	"invalid_target",
	"unauthorized_client",
	"unsupported_grant_type",
	"unsupported_response_type",
	"invalid_redirect_uri",
	"invalid_code_verifier",
	"invalid_state",
	"access_denied",
	"authorization_pending",
	"slow_down",
	"incorrect_device_code",
	"expired_device_code",
	"device_flow_disabled",
	"login_required",
	"consent_required",
	"interaction_required",
	"server_error",
	"temporarily_unavailable",
	"rate_limit_exceeded",
	"not_found",
	"forbidden",
];

/// The allowlisted lowercase identifier for `text`, if it is one of ours.
pub(crate) fn oauth_error_identifier(text: &str) -> Option<String> {
	let candidate = text.trim().to_ascii_lowercase();
	KNOWN_OAUTH_ERROR_IDENTIFIERS
		.contains(&candidate.as_str())
		.then_some(candidate)
}

/// Summarise a token-endpoint error: only allowlisted OAuth identifiers
/// survive. `error_description` is provider prose — it may quote the grant, so
/// it is dropped rather than heuristically filtered. A non-JSON or free-form
/// body is never echoed; the caller reports the operation plus HTTP status.
#[must_use]
pub fn summarize_error_body(body: &str) -> String {
	let trimmed = body.trim();
	if trimmed.is_empty() {
		return String::new();
	}
	let Ok(json) = serde_json::from_str::<serde_json::Value>(trimmed) else {
		return String::new();
	};
	let mut parts: Vec<String> = Vec::new();
	for field in ["error", "error_code", "type"] {
		let Some(code) = json
			.get(field)
			.and_then(serde_json::Value::as_str)
			.and_then(oauth_error_identifier)
		else {
			continue;
		};
		if !parts.contains(&code) {
			parts.push(code);
		}
	}
	parts.join(": ")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn redacts_json_token_fields() {
		let text = r#"{"access_token":"sk-abc123","refresh_token":"r","expires_in":10}"#;
		let redacted = redact(text);
		assert!(!redacted.contains("abc123"));
		assert!(redacted.contains("[REDACTED]"));
		assert!(redacted.contains("expires_in"));
	}

	#[test]
	fn redacts_bearer_and_prefixed_keys() {
		let redacted = redact("Authorization: Bearer eyJhbGciOi.payload.sig next");
		assert!(!redacted.contains("eyJhbGciOi"));
		assert!(redacted.contains("Bearer [REDACTED]"));
	}

	#[test]
	fn summary_keeps_only_error_shape() {
		let body = r#"{"error":"invalid_grant","error_description":"Refresh token expired","access_token":"secret"}"#;
		let summary = summarize_error_body(body);
		assert_eq!(summary, "invalid_grant");
		assert!(!summary.contains("secret"));
	}

	#[test]
	fn only_provider_declared_dead_grants_are_definitive() {
		// Nothing disables a credential unless the provider said it is dead.
		assert!(!AuthError::new(AuthErrorKind::TokenRefresh, "invalid response").is_definitive());
		assert!(!AuthError::new(AuthErrorKind::Validation, "malformed body").is_definitive());
		assert!(
			!AuthError::new(AuthErrorKind::Http, "busy")
				.with_status(429)
				.is_definitive()
		);
		assert!(
			!AuthError::new(AuthErrorKind::Http, "down")
				.with_status(503)
				.is_definitive()
		);
		assert!(
			!AuthError::new(AuthErrorKind::Http, "waf")
				.with_status(403)
				.is_definitive()
		);
		assert!(
			AuthError::new(AuthErrorKind::TokenRefresh, "revoked")
				.with_grant_dead()
				.is_definitive()
		);
		assert!(
			!AuthError::cancelled("zai")
				.with_grant_dead()
				.is_definitive()
		);

		assert!(
			AuthError::new(AuthErrorKind::Http, "busy")
				.with_status(429)
				.is_transient()
		);
		assert!(AuthError::new(AuthErrorKind::Timeout, "slow").is_transient());
	}

	#[test]
	fn invalid_grant_bodies_are_recognised() {
		assert!(is_invalid_grant_body(r#"{"error":"invalid_grant"}"#));
		assert!(is_invalid_grant_body(r#"{"error":"expired_token"}"#));
		assert!(!is_invalid_grant_body(r#"{"error":"rate_limit_exceeded"}"#));
		assert!(!is_invalid_grant_body("<html>blocked</html>"));
	}

	#[test]
	fn free_form_bodies_are_never_echoed() {
		assert_eq!(summarize_error_body("<html>token=abc123</html>"), "");
		assert_eq!(summarize_error_body("plain text secret"), "");
		// A machine-shaped identifier survives; an opaque token does not.
		let body = r#"{"error":"invalid_token","error_description":"handler returned 0123456789abcdef0123456789abcdef"}"#;
		let summary = summarize_error_body(body);
		assert_eq!(summary, "invalid_token");

		let prefixed = r#"{"error":"invalid_grant","error_description":"token sk-abc123 rejected"}"#;
		assert!(!summarize_error_body(prefixed).contains("sk-abc123"));
	}

	#[test]
	fn redacts_pasted_redirect_urls_and_jwts() {
		let pasted = "vscode://gitlab.gitlab-workflow/authentication?code=secret-code&state=nonce";
		let redacted = redact(pasted);
		assert!(!redacted.contains("secret-code"));
		assert!(redacted.contains("state=nonce"));

		let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl";
		assert!(!redact(jwt).contains("eyJhbGciOiJIUzI1NiJ9"));
	}

	#[test]
	fn redact_free_text_masks_secrets_without_corrupting_identifiers() {
		let input = "serde(skip_serializing_if) disk-cache-key sk-proj-abcdefghij1234567890 Bearer abcdefghij1234567890abcdef ghp_1234567890abcdefghij AKIAIOSFODNN7EXAMPLE https://x/cb?access_token=tok123&ok=1 eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJlMTIzNDU2";
		let redacted = redact_free_text(input);
		assert!(redacted.contains("skip_serializing_if"));
		assert!(redacted.contains("disk-cache-key"));
		assert!(redacted.contains("ok=1"));
		assert!(!redacted.contains("sk-proj-abcdefghij1234567890"));
		assert!(!redacted.contains("abcdefghij1234567890abcdef"));
		assert!(!redacted.contains("ghp_1234567890abcdefghij"));
		assert!(!redacted.contains("AKIAIOSFODNN7EXAMPLE"));
		assert!(!redacted.contains("tok123"));
		assert!(!redacted.contains("eyJhbGciOiJIUzI1NiJ9"));
		assert_eq!(redact_free_text(&redacted), redacted);
	}

	#[test]
	fn redact_free_text_masks_registered_credentials_exactly() {
		// A credential with no recognisable shape: only exact-value matching
		// finds it, and the registry has to cover the encoded carriers too.
		let secret = "Kq7fWpz2LmRt9XvB4nC";
		crate::auth::known_secrets::register(secret);
		let text = format!("request body: key={secret} retry=1");
		let redacted = redact_free_text(&text);
		assert!(!redacted.contains(secret), "registered value must be masked: {redacted}");
		assert!(redacted.contains("retry=1"), "surrounding text survives: {redacted}");
		assert_eq!(redact_free_text(&redacted), redacted);

		let encoded =
			base64::Engine::encode(&base64::engine::general_purpose::STANDARD, secret.as_bytes());
		let carrier = format!("blob {encoded} end");
		let redacted = redact_free_text(&carrier);
		assert!(!redacted.contains(&encoded), "base64 carrier must be masked: {redacted}");
	}

	#[test]
	fn redact_free_text_leaves_prose_and_code_untouched() {
		// Ordinary prose and code must survive: a recovered fragment is model
		// context, and corrupting it is a permanent loss of what the user saw.
		for text in [
			"Bearer authentication is negotiated per request",
			"sk-learn-pipeline is our feature name",
			"let v = cond ? key = value : None;",
			"the disk token bucket refills every minute",
			"skip_serializing_if",
		] {
			assert_eq!(redact_free_text(text), text, "unchanged: {text}");
		}
	}

	#[test]
	fn redact_json_strings_handles_dynamic_keys_and_disambiguates_collisions() {
		let mut obj = serde_json::json!({
			"sk-ant-api03-secret1-123456789012": "value1",
			"sk-ant-api03-secret2-123456789012": "value2",
			"[REDACTED]": "literal_unredacted_val",
			"normal_key": "normal_val"
		});
		redact_json_strings(&mut obj);
		let map = obj.as_object().expect("object");
		assert!(map.get("normal_key") == Some(&serde_json::json!("normal_val")));
		assert!(
			map.get("[REDACTED]") == Some(&serde_json::json!("literal_unredacted_val")),
			"literal unredacted key preserved"
		);
		assert!(map.contains_key("[REDACTED_COLLISION:1]"), "first collision marker present");
		assert!(map.contains_key("[REDACTED_COLLISION:2]"), "second collision marker present");
		let v1 = map.get("[REDACTED_COLLISION:1]").unwrap();
		let v2 = map.get("[REDACTED_COLLISION:2]").unwrap();
		assert!(v1 == "value1" || v1 == "value2");
		assert!(v2 == "value1" || v2 == "value2");
		assert!(v1 != v2);
	}

	#[test]
	fn redact_json_strings_redacts_user_field_named_id() {
		let secret = "sk-ant-api03-user-id-secret-12345";
		let mut obj = serde_json::json!({
			"id": secret,
			"nested": {
				"id": secret,
				"other": "hello"
			}
		});
		redact_json_strings(&mut obj);
		assert!(obj["id"] == "[REDACTED]", "id must be masked");
		assert!(obj["nested"]["id"] == "[REDACTED]", "nested id must be masked");
		assert!(obj["nested"]["other"] == "hello");
		let dump = obj.to_string();
		assert!(!dump.contains(secret), "secret must not appear in cleartext");
	}

	#[test]
	fn redact_message_payload_preserves_structural_ids_and_tool_call_ids() {
		let secret = "sk-ant-api03-tool-arg-secret-1234";
		let mut msg = serde_json::json!({
			"type": "message",
			"id": "msg-struct-id-123",
			"parentId": "msg-parent-id-000",
			"timestamp": "2026-01-01T00:00:00Z",
			"message": {
				"role": "assistant",
				"content": [
					{
						"type": "toolCall",
						"id": "call-struct-id-999",
						"name": "calc",
						"arguments": {
							"id": secret,
							"expr": format!("eval {secret}")
						}
					},
					{
						"type": "text",
						"text": format!("thought about {secret}")
					}
				],
				"details": {
					"id": secret
				}
			}
		});
		redact_message_payload(&mut msg);
		assert!(msg["id"] == "msg-struct-id-123", "structural id must be preserved");
		assert!(msg["parentId"] == "msg-parent-id-000", "structural parentId must be preserved");
		let parts = msg["message"]["content"].as_array().expect("parts");
		assert!(parts[0]["id"] == "call-struct-id-999", "tool call id preserved");
		assert!(parts[0]["name"] == "calc");
		assert!(parts[0]["arguments"]["id"] == "[REDACTED]", "tool argument id masked");
		assert!(parts[0]["arguments"]["expr"] == "eval [REDACTED]", "tool argument expr masked");
		assert!(parts[1]["text"] == "thought about [REDACTED]", "text part masked");
		assert!(msg["message"]["details"]["id"] == "[REDACTED]", "details id masked");
		let dump = msg.to_string();
		assert!(!dump.contains(secret), "secret must not appear in cleartext");
	}

	#[test]
	fn redact_session_payload_preserves_structural_ids_and_projection_kept() {
		let secret = "sk-ant-api03-proj-synth-secret-12";
		let mut sess = serde_json::json!({
			"type": "session",
			"version": 3,
			"id": "s-struct-id-456",
			"parentSession": "s-parent-id-789",
			"timestamp": "2026-01-01T00:00:00Z",
			"cwd": "/tmp",
			"title": format!("session title with {secret}"),
			"durableProjection": {
				"kept": ["msg-kept-1", "msg-kept-2"],
				"anchor": "msg-kept-2",
				"synthetic": [
					{
						"type": "message",
						"id": "synth-msg-1",
						"parentId": "msg-kept-2",
						"message": {
							"role": "user",
							"content": format!("synthetic summary {secret}")
						}
					}
				]
			}
		});
		redact_session_payload(&mut sess);
		assert!(sess["id"] == "s-struct-id-456", "session id preserved");
		assert!(sess["parentSession"] == "s-parent-id-789", "parentSession preserved");
		assert!(sess["title"] == "session title with [REDACTED]", "title masked");
		let proj = &sess["durableProjection"];
		assert!(proj["kept"] == serde_json::json!(["msg-kept-1", "msg-kept-2"]));
		assert!(proj["anchor"] == "msg-kept-2");
		assert!(proj["synthetic"][0]["id"] == "synth-msg-1");
		assert!(
			proj["synthetic"][0]["message"]["content"] == "synthetic summary [REDACTED]"
		);
		let dump = sess.to_string();
		assert!(!dump.contains(secret), "secret must not appear in cleartext");
	}

	#[test]
	fn generic_redact_json_strings_does_not_spoof_schemas() {
		let secret = "sk-ant-api03-spoofed-schema-secret-1234";
		let mut spoofed_session = serde_json::json!({
			"type": "session",
			"id": secret,
			"parentSession": secret
		});
		redact_json_strings(&mut spoofed_session);
		let s_id = spoofed_session["id"].as_str().unwrap_or_default();
		assert!(s_id == "[REDACTED]", "generic redaction must mask id even if type is session");
		let s_parent = spoofed_session["parentSession"].as_str().unwrap_or_default();
		assert!(s_parent == "[REDACTED]");
		assert!(!spoofed_session.to_string().contains(secret));

		let mut spoofed_msg = serde_json::json!({
			"type": "message",
			"id": secret,
			"parentId": secret,
			"message": {
				"role": "assistant",
				"toolCallId": secret
			}
		});
		redact_json_strings(&mut spoofed_msg);
		let m_id = spoofed_msg["id"].as_str().unwrap_or_default();
		assert!(m_id == "[REDACTED]", "generic redaction must mask id even if type is message");
		let m_parent = spoofed_msg["parentId"].as_str().unwrap_or_default();
		assert!(m_parent == "[REDACTED]");
		let m_call = spoofed_msg["message"]["toolCallId"].as_str().unwrap_or_default();
		assert!(m_call == "[REDACTED]");
		assert!(!spoofed_msg.to_string().contains(secret));
	}

	#[test]
	fn content_parts_redact_extras_while_preserving_signatures_and_image_bytes() {
		let secret = "sk-ant-api03-content-part-secret-5678";
		let sig = "opaque-cryptographic-provider-signature-abc123";
		let b64 = "aW1hZ2UtYnl0ZXMtcGF5bG9hZA==";

		let mut text_part = serde_json::json!({
			"type": "text",
			"text": format!("thought {secret}"),
			"customExtra": format!("extra-val-{secret}")
		});
		redact_content_part_value(&mut text_part);
		assert!(text_part["text"] == "thought [REDACTED]");
		assert!(text_part["customExtra"] == "extra-val-[REDACTED]");
		assert!(!text_part.to_string().contains(secret));

		// When thinking text contains a secret, redaction modifies the text, so the
		// cryptographic signature is invalid and MUST be removed.
		let mut modified_thinking = serde_json::json!({
			"type": "thinking",
			"thinking": format!("deep reasoning {secret}"),
			"thinkingSignature": sig,
			"extraField": secret
		});
		redact_content_part_value(&mut modified_thinking);
		assert!(modified_thinking["thinking"] == "deep reasoning [REDACTED]");
		assert!(
			modified_thinking.get("thinkingSignature").is_none(),
			"invalidated signature must be removed when text modified"
		);
		assert!(modified_thinking["extraField"] == "[REDACTED]");
		assert!(!modified_thinking.to_string().contains(secret));

		// When thinking text does NOT contain a secret, text is unchanged and signature is preserved.
		let mut unredacted_thinking = serde_json::json!({
			"type": "thinking",
			"thinking": "clean reasoning without any secret",
			"thinkingSignature": sig,
			"extraField": secret
		});
		redact_content_part_value(&mut unredacted_thinking);
		assert!(unredacted_thinking["thinking"] == "clean reasoning without any secret");
		assert!(
			unredacted_thinking["thinkingSignature"] == sig,
			"signature preserved when thinking text is unchanged"
		);
		assert!(unredacted_thinking["extraField"] == "[REDACTED]");
		assert!(!unredacted_thinking.to_string().contains(secret));

		let mut image_part = serde_json::json!({
			"type": "image",
			"data": b64,
			"mimeType": "image/png",
			"extraMeta": secret
		});
		redact_content_part_value(&mut image_part);
		assert!(image_part["data"] == b64, "image data preserved");
		assert!(image_part["mimeType"] == "image/png");
		assert!(image_part["extraMeta"] == "[REDACTED]");
		assert!(!image_part.to_string().contains(secret));
	}

	#[test]
	fn stored_projection_preserves_native_synthetic_tool_ids_and_masks_args() {
		let secret = "sk-ant-api03-synth-native-secret-9999";
		let call_id = "call-synthetic-id-123";
		let mut proj = serde_json::json!({
			"kept": ["m1", "m2"],
			"anchor": "m2",
			"synthetic": [
				{
					"role": "assistant",
					"content": format!("synthetic summary {secret}"),
					"tool_calls": [
						{
							"id": call_id,
							"name": "bash",
							"arguments": {
								"id": format!("user-id-{secret}"),
								"cmd": format!("run {secret}")
							}
						}
					],
					"tool_call_id": call_id
				}
			]
		});
		redact_stored_projection_value(&mut proj);
		assert!(proj["kept"] == serde_json::json!(["m1", "m2"]));
		assert!(proj["anchor"] == "m2");
		let synth = &proj["synthetic"][0];
		assert!(synth["content"] == "synthetic summary [REDACTED]");
		assert!(synth["tool_call_id"] == call_id, "tool_call_id preserved");
		assert!(synth["tool_calls"][0]["id"] == call_id, "tool_call id preserved");
		assert!(synth["tool_calls"][0]["name"] == "bash");
		assert!(synth["tool_calls"][0]["arguments"]["id"] == "user-id-[REDACTED]");
		assert!(synth["tool_calls"][0]["arguments"]["cmd"] == "run [REDACTED]");
		assert!(!proj.to_string().contains(secret));
	}
	#[test]
	fn content_part_text_extras_two_phase_preserves_literals_and_counts() {
		let secret = "sk-ant-api03-extra-secret-key-12345";
		// Order matters: secret key first, followed by literal [REDACTED], then literal [REDACTED_COLLISION:1]
		let mut text_part = serde_json::json!({
			"type": "text",
			"text": "plain text",
			format!("sk-ant-api03-{secret}"): "secret-key-val",
			"[REDACTED]": "literal-redacted-val",
			"[REDACTED_COLLISION:1]": "literal-collision-val",
			"normal_extra": "normal-val"
		});
		redact_content_part_value(&mut text_part);
		let map = text_part.as_object().expect("object");
		// 1. Entry count MUST be exactly preserved: 6 entries in, 6 entries out!
		assert!(map.len() == 6, "entry count must be preserved without loss");
		// 2. Structural fields preserved
		assert!(map.get("type") == Some(&serde_json::json!("text")));
		assert!(map.get("text") == Some(&serde_json::json!("plain text")));
		// 3. Unredacted normal extra preserved
		assert!(map.get("normal_extra") == Some(&serde_json::json!("normal-val")));
		// 4. Literal unredacted keys MUST NOT be overwritten by the secret key!
		assert!(
			map.get("[REDACTED]") == Some(&serde_json::json!("literal-redacted-val")),
			"literal [REDACTED] key must preserve its own value and not be overwritten"
		);
		assert!(
			map.get("[REDACTED_COLLISION:1]") == Some(&serde_json::json!("literal-collision-val")),
			"literal collision marker must preserve its own value"
		);
		// 5. The secret-bearing key collides with both [REDACTED] and [REDACTED_COLLISION:1],
		// so it must receive [REDACTED_COLLISION:2] without losing data!
		assert!(
			map.get("[REDACTED_COLLISION:2]") == Some(&serde_json::json!("secret-key-val")),
			"secret key value must be preserved under the next available collision marker"
		);
		assert!(!text_part.to_string().contains(secret), "secret must not appear in cleartext");

		// 6. Repeated redaction MUST be idempotent and stable
		let mut second_pass = text_part.clone();
		redact_content_part_value(&mut second_pass);
		assert!(second_pass == text_part, "repeated redaction must be idempotent and stable");
	}

	#[test]
	fn session_message_extras_two_phase_preserves_literals_and_repeated_redaction() {
		let secret = "sk-ant-api03-msg-extra-secret-99999";
		let mut msg = serde_json::json!({
			"role": "assistant",
			"content": "message text",
			format!("sk-prefix-{secret}"): "secret-val-1",
			format!("sk-another-{secret}"): "secret-val-2",
			"[REDACTED]": "literal-val",
			"details": {
				"detail_key": "detail_val"
			}
		});
		redact_session_message_value(&mut msg);
		let map = msg.as_object().expect("object");
		// 6 entries in, 6 entries out!
		assert!(map.len() == 6, "entry count must be preserved in message extras");
		assert!(map.get("role") == Some(&serde_json::json!("assistant")));
		assert!(map.get("content") == Some(&serde_json::json!("message text")));
		assert!(map.get("[REDACTED]") == Some(&serde_json::json!("literal-val")));
		assert!(map.contains_key("[REDACTED_COLLISION:1]"));
		assert!(map.contains_key("[REDACTED_COLLISION:2]"));
		let v1 = map.get("[REDACTED_COLLISION:1]").unwrap();
		let v2 = map.get("[REDACTED_COLLISION:2]").unwrap();
		assert!(v1 == "secret-val-1" || v1 == "secret-val-2");
		assert!(v2 == "secret-val-1" || v2 == "secret-val-2");
		assert!(v1 != v2);
		assert!(!msg.to_string().contains(secret), "secret must not appear in cleartext");

		// Repeated redaction stability
		let mut second = msg.clone();
		redact_session_message_value(&mut second);
		assert!(second == msg, "message redaction must be idempotent");
	}
}
