//! Incremental primary-transcript rendering for the advisor model.
//!
//! The advisor sees what the primary model saw plus what it produced: user
//! turns, assistant text/reasoning/tool calls, and tool results. Each update is
//! one `### Session update` block. Re-injected primary context (plan/goal rules)
//! collapses to a one-line marker when its body is unchanged, so the advisor is
//! not re-fed the same ~1k-token block every turn.
//!
//! `ponytail:` the TS split renderer emits one user message per source message
//! so provider prompt caches grow incrementally. This build sends one block per
//! update; upgrade by feeding the chunk list as consecutive user messages once
//! the advisor loop accepts a multi-message prompt.

use std::collections::BTreeMap;

use pi_agent::{ContentPart, Message, Role};

/// Custom types whose bodies are re-injected verbatim by the primary.
const PRIMARY_CONTEXT_TYPES: [&str; 2] = ["plan-mode-context", "goal-mode-context"];

const HEADING: &str = "### Session update";

/// Field-selective identity: only fields the renderer reads feed the hash, so a
/// provider clone with a different timestamp still matches its predecessor.
#[must_use]
pub fn fingerprint(message: &Message) -> u64 {
	let mut hash = 0xcbf2_9ce4_8422_2325_u64;
	let mut feed = |text: &str| {
		for byte in text.as_bytes() {
			hash ^= u64::from(*byte);
			hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
		}
		hash ^= 0xff;
		hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
	};
	feed(match message.role {
		Role::System => "system",
		Role::User => "user",
		Role::Assistant => "assistant",
		Role::Tool => "tool",
		Role::Developer => "developer",
	});
	feed(message.content.as_str());
	feed(message.custom_type.as_deref().unwrap_or(""));
	feed(if message.is_error { "error" } else { "ok" });
	for part in &message.parts {
		match part {
			ContentPart::Text { text } => {
				feed("text");
				feed(text);
			},
			ContentPart::Thinking { thinking, .. } => {
				feed("thinking");
				feed(thinking);
			},
			ContentPart::RedactedThinking { data } => {
				feed("redacted");
				feed(data);
			},
			ContentPart::Image { mime_type, .. } => {
				feed("image");
				feed(mime_type);
			},
			ContentPart::ToolCall { id, name, arguments } => {
				feed("call");
				feed(id);
				feed(name);
				feed(&arguments.to_string());
			},
		}
	}
	for call in &message.tool_calls {
		feed("call");
		feed(&call.id);
		feed(&call.name);
		feed(&call.arguments.to_string());
	}
	if let Some(tool_call_id) = &message.tool_call_id {
		feed("result");
		feed(tool_call_id);
		feed(message.name.as_deref().unwrap_or(""));
	}
	hash
}

fn is_primary_context(message: &Message) -> Option<&str> {
	message
		.custom_type
		.as_deref()
		.filter(|custom_type| PRIMARY_CONTEXT_TYPES.contains(custom_type))
}

/// Render one message body. Returns `None` when the message carries nothing the
/// advisor can read.
fn render_message(message: &Message, include_thinking: bool) -> Option<String> {
	let mut out = String::new();
	let mut wrote_content = false;
	match message.role {
		Role::User => out.push_str("### User\n"),
		Role::Assistant => out.push_str("### Assistant\n"),
		Role::Tool => {
			let name = message.name.as_deref().unwrap_or("tool");
			let status = if message.is_error { " (error)" } else { "" };
			out.push_str(&format!("### Tool result: {name}{status}\n"));
		},
		Role::System | Role::Developer => out.push_str("### Context\n"),
	}
	if !message.content.is_empty() {
		wrote_content = true;
		out.push_str(message.content.trim_end());
		out.push('\n');
	}
	let mut wrote_part = false;
	for part in &message.parts {
		match part {
			ContentPart::Text { text } if !text.is_empty() => {
				if !wrote_part && message.content.is_empty() {
					out.push_str("### Assistant\n");
				}
				wrote_part = true;
				wrote_content = true;
				out.push_str(text.trim_end());
				out.push('\n');
			},
			ContentPart::Thinking { thinking, .. } if include_thinking && !thinking.is_empty() => {
				wrote_part = true;
				out.push_str("[thinking] ");
				out.push_str(thinking.trim_end());
				out.push('\n');
			},
			ContentPart::ToolCall { id, name, arguments } => {
				wrote_part = true;
				out.push_str(&format!("[tool call {name} id={id}] {}\n", compact_json(arguments)));
			},
			ContentPart::RedactedThinking { .. } => {},
			ContentPart::Image { mime_type, .. } => {
				wrote_part = true;
				out.push_str(&format!("[image {mime_type}]\n"));
			},
			_ => {},
		}
	}
	if !message.tool_calls.is_empty() && message.parts.is_empty() {
		for call in &message.tool_calls {
			wrote_part = true;
			out.push_str(&format!(
				"[tool call {} id={}] {}\n",
				call.name,
				call.id,
				compact_json(&call.arguments)
			));
		}
	}
	if !wrote_content && !wrote_part {
		return None;
	}
	Some(out)
}

fn compact_json(value: &serde_json::Value) -> String {
	let text = value.to_string();
	const LIMIT: usize = 400;
	if text.len() <= LIMIT {
		text
	} else {
		let mut end = LIMIT;
		while end > 0 && !text.is_char_boundary(end) {
			end -= 1;
		}
		format!("{}…", &text[..end])
	}
}

/// Render the incremental messages as one update block.
///
/// `seen_context` is advanced for first-time primary-context messages; callers
/// that must not mutate it (a preview render) pass a throwaway clone, and a
/// failed batch restores the pre-batch map.
pub fn render_delta(
	messages: &[Message],
	wip: bool,
	include_thinking: bool,
	seen_context: &mut BTreeMap<String, String>,
) -> Option<String> {
	let mut body = String::new();
	for message in messages {
		let collapse = is_primary_context(message).and_then(|custom_type| {
			if !message.content.is_empty() {
				let seen = seen_context
					.get(custom_type)
					.is_some_and(|seen| seen == &message.content);
				seen_context.insert(custom_type.to_string(), message.content.clone());
				Some(seen)
			} else {
				None
			}
		});
		if collapse == Some(true) {
			body.push_str(&format!("### {}\n(unchanged — still in effect)\n", message.custom_type.as_deref().unwrap_or("Context")));
			continue;
		}
		if let Some(rendered) = render_message(message, include_thinking) {
			body.push_str(&rendered);
			body.push('\n');
		}
	}
	if body.trim().is_empty() {
		return None;
	}
	let mut rendered = format!("{HEADING}\n\n{}", body.trim_end());
	if wip {
		// The marker stays at the END so a wip→final flip never changes the
		// provider-cache-relevant prefix.
		rendered.push_str("\n\n---\n\n[in progress — more steps follow]");
	}
	Some(rendered)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn user(text: &str) -> Message {
		Message::user(text)
	}

	#[test]
	fn fingerprint_ignores_timestamp_but_tracks_content() {
		let mut first = user("hello");
		first.usage = Some(pi_agent::TokenUsage::default());
		let second = user("hello");
		assert_eq!(fingerprint(&first), fingerprint(&second));
		assert_ne!(fingerprint(&user("hello")), fingerprint(&user("world")));
	}

	#[test]
	fn render_includes_roles_tool_intent_and_wip_marker_at_tail() {
		let mut assistant = Message::assistant("Checking the guard.");
		assistant.tool_calls.push(pi_agent::ToolCall {
			id:   "call-1".to_string(),
			name: "read".to_string(),
			arguments: serde_json::json!({ "path": "src/lib.rs" }),
		});
		let mut seen = BTreeMap::new();
		let rendered = render_delta(&[user("fix it"), assistant], true, true, &mut seen)
			.expect("rendered");
		assert!(rendered.starts_with("### Session update"));
		assert!(rendered.contains("### User\nfix it"));
		assert!(rendered.contains("### Assistant"));
		assert!(rendered.contains("[tool call read id=call-1]"));
		assert!(rendered.ends_with("[in progress — more steps follow]"));
		assert!(!rendered.contains("---\n\n---"));
	}

	#[test]
	fn repeated_primary_context_collapses_to_marker() {
		let mut context = Message::developer("PLAN RULES");
		context.custom_type = Some("plan-mode-context".to_string());
		let first = render_delta(std::slice::from_ref(&context), false, true, &mut BTreeMap::new())
			.expect("first");
		assert!(first.contains("PLAN RULES"));
		let mut seen = BTreeMap::new();
		assert!(render_delta(std::slice::from_ref(&context), false, true, &mut seen)
			.expect("first")
			.contains("PLAN RULES"));
		let second = render_delta(std::slice::from_ref(&context), false, true, &mut seen)
			.expect("second");
		assert!(second.contains("(unchanged — still in effect)"));
		assert!(!second.contains("PLAN RULES"));
	}

	#[test]
	fn empty_delta_renders_nothing() {
		assert_eq!(
			render_delta(&[], false, true, &mut BTreeMap::new()),
			None
		);
		assert_eq!(
			render_delta(&[Message::assistant("")], false, true, &mut BTreeMap::new()),
			None
		);
	}
}
