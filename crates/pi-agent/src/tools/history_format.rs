//! Concise markdown transcript serializer for `history://` URLs.
//!
//! Mirrors `packages/coding-agent/src/session/session-history-format.ts` for
//! the message shapes the Rust transcript log can contain: full
//! user/assistant/developer text, one line per tool call paired with its
//! result, thinking elided, system/catalog/config sections excluded. Raw JSONL
//! is never handed to a caller.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::state::{ContentPart, Message, Role};

/// Max length of the primary-arg summary inside `→ tool(...)` lines.
const PRIMARY_ARG_MAX: usize = 120;

/// Per-tool preference order for the most informative scalar argument.
const PRIMARY_ARG_KEYS: [&str; 15] = [
	"path",
	"file_path",
	"filePath",
	"command",
	"cmd",
	"pattern",
	"url",
	"query",
	"prompt",
	"assignment",
	"note",
	"message",
	"op",
	"name",
	"id",
];

/// Tool-call intent field (never a useful summary on its own).
const INTENT_FIELD: &str = "i";

/// Collapse whitespace runs and truncate to `max` chars with an ellipsis.
fn one_line(text: &str, max: usize) -> String {
	let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
	if flat.chars().count() > max {
		let mut truncated: String = flat.chars().take(max.saturating_sub(1)).collect();
		truncated.push('…');
		truncated
	} else {
		flat
	}
}

/// Text body of a message: typed text parts when present, else `content`.
fn message_text(message: &Message) -> String {
	let mut parts: Vec<&str> = Vec::new();
	for part in &message.parts {
		if let ContentPart::Text { text } = part {
			parts.push(text.as_str());
		}
	}
	if parts.is_empty() {
		message.content.clone()
	} else {
		parts.join("\n")
	}
}

fn line_count(text: &str) -> usize {
	if text.is_empty() {
		return 0;
	}
	text.split('\n').count()
}

fn primary_arg_value(value: &Value) -> String {
	match value {
		Value::String(text) => text.clone(),
		Value::Array(items) if !items.is_empty() => {
			let strings: Option<Vec<&str>> = items
				.iter()
				.map(|item| item.as_str())
				.collect();
			strings.map(|values| values.join(", ")).unwrap_or_default()
		},
		_ => String::new(),
	}
}

/// Pick the most informative scalar argument of a tool call.
fn primary_arg(name: &str, arguments: Option<&Value>) -> String {
	let Some(Value::Object(args)) = arguments else {
		return String::new();
	};
	if name == "advise" {
		let note = primary_arg_value(args.get("note").unwrap_or(&Value::Null));
		let severity = primary_arg_value(args.get("severity").unwrap_or(&Value::Null));
		if !note.is_empty() && !severity.is_empty() {
			return one_line(&format!("{severity}: {note}"), PRIMARY_ARG_MAX);
		}
		if !note.is_empty() {
			return one_line(&note, PRIMARY_ARG_MAX);
		}
		if !severity.is_empty() {
			return one_line(&severity, PRIMARY_ARG_MAX);
		}
	}
	if name == "grep" {
		let pattern = primary_arg_value(args.get("pattern").unwrap_or(&Value::Null));
		let paths = primary_arg_value(args.get("path").unwrap_or(&Value::Null));
		let paths = if paths.is_empty() {
			primary_arg_value(args.get("paths").unwrap_or(&Value::Null))
		} else {
			paths
		};
		if !pattern.is_empty() && !paths.is_empty() {
			return one_line(&format!("{pattern} @ {paths}"), PRIMARY_ARG_MAX);
		}
		if !pattern.is_empty() {
			return one_line(&pattern, PRIMARY_ARG_MAX);
		}
		if !paths.is_empty() {
			return one_line(&paths, PRIMARY_ARG_MAX);
		}
	}
	if name == "ast_grep" {
		let pattern = primary_arg_value(args.get("pat").unwrap_or(&Value::Null));
		if !pattern.is_empty() {
			return one_line(&pattern, PRIMARY_ARG_MAX);
		}
	}
	if name == "glob" {
		let paths = primary_arg_value(args.get("path").unwrap_or(&Value::Null));
		let paths = if paths.is_empty() {
			primary_arg_value(args.get("paths").unwrap_or(&Value::Null))
		} else {
			paths
		};
		if !paths.is_empty() {
			return one_line(&paths, PRIMARY_ARG_MAX);
		}
	}
	for key in PRIMARY_ARG_KEYS {
		let summary = primary_arg_value(args.get(key).unwrap_or(&Value::Null));
		if !summary.is_empty() {
			return one_line(&summary, PRIMARY_ARG_MAX);
		}
	}
	// Fallback: first non-intent string arg, then a compact JSON of the args.
	let mut rest = serde_json::Map::new();
	for (key, value) in args {
		if key == INTENT_FIELD {
			continue;
		}
		if let Value::String(text) = value
			&& !text.is_empty()
		{
			return one_line(text, PRIMARY_ARG_MAX);
		}
		rest.insert(key.clone(), value.clone());
	}
	if rest.is_empty() {
		return "{}".to_string();
	}
	one_line(&Value::Object(rest).to_string(), PRIMARY_ARG_MAX)
}

/// One line per tool call: `→ read(src/foo.ts:50-80) ⇒ ok · 31 lines`.
fn tool_call_line(name: &str, arguments: Option<&Value>, result: Option<&Message>) -> String {
	let head = format!("→ {name}({})", primary_arg(name, arguments));
	let Some(result) = result else {
		return format!("{head} ⇒ pending");
	};
	let text = result.content.as_str();
	let lines = line_count(text);
	let count = format!("{lines} {}", if lines == 1 { "line" } else { "lines" });
	if result.is_error {
		let first_line = one_line(text.split('\n').next().unwrap_or_default(), PRIMARY_ARG_MAX);
		if first_line.is_empty() {
			format!("{head} ⇒ error · {count}")
		} else {
			format!("{head} ⇒ error · {count} — {first_line}")
		}
	} else {
		format!("{head} ⇒ ok · {count}")
	}
}

/// Format a transcript as a concise markdown document.
///
/// Tool results are collapsed into their originating call; an unanswered call
/// stays `pending`, and an orphan result (truncated history) gets its own line.
/// Thinking parts are never rendered.
#[must_use]
pub fn format_history_markdown(title: &str, messages: &[Message]) -> String {
	let mut results: HashMap<&str, &Message> = HashMap::new();
	for message in messages {
		if message.role == Role::Tool
			&& let Some(id) = message.tool_call_id.as_deref()
		{
			results.insert(id, message);
		}
	}
	let mut consumed: HashSet<String> = HashSet::new();
	let mut lines: Vec<String> = vec![format!("# {title}"), String::new()];

	for message in messages {
		match message.role {
			Role::User | Role::Developer => {
				let text = message_text(message);
				if text.trim().is_empty() {
					continue;
				}
				let role = if message.role == Role::User { "user" } else { "developer" };
				lines.push(format!("## {role}"));
				lines.push(String::new());
				lines.push(text);
				lines.push(String::new());
			},
			Role::Assistant => {
				let mut body: Vec<String> = Vec::new();
				let text = message_text(message);
				if !text.trim().is_empty() {
					body.push(text);
				}
				// `tool_calls` is the execution projection; a transcript that only
				// carries streamed `toolCall` parts still renders those calls.
				let part_calls: Vec<(String, String, Value)> = message
					.parts
					.iter()
					.filter_map(|part| match part {
						ContentPart::ToolCall { id, name, arguments } => {
							Some((id.clone(), name.clone(), arguments.clone()))
						},
						_ => None,
					})
					.collect();
				let calls: Vec<(String, String, Value)> = if message.tool_calls.is_empty() {
					part_calls
				} else {
					message
						.tool_calls
						.iter()
						.map(|call| (call.id.clone(), call.name.clone(), call.arguments.clone()))
						.collect()
				};
				for (id, name, arguments) in calls {
					let result = results.get(id.as_str()).copied();
					if result.is_some() {
						consumed.insert(id.clone());
					}
					body.push(tool_call_line(&name, Some(&arguments), result));
				}
				if body.is_empty() {
					continue;
				}
				lines.push("## assistant".to_string());
				lines.push(String::new());
				lines.extend(body);
				lines.push(String::new());
			},
			Role::Tool => {
				let Some(id) = message.tool_call_id.as_deref() else {
					continue;
				};
				if consumed.contains(id) {
					continue;
				}
				let name = message.name.as_deref().unwrap_or("tool");
				lines.push(tool_call_line(name, None, Some(message)));
				lines.push(String::new());
			},
			Role::System => {},
		}
	}

	let mut document = lines.join("\n").trim().to_string();
	document.push('\n');
	document
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;
	use crate::state::{Message, ToolCall};

	fn assistant_with_parts(content: &str, parts: Vec<ContentPart>, calls: Vec<ToolCall>) -> Message {
		let mut message = Message::assistant_with_tools(content, calls);
		message.parts = parts;
		message
	}

	#[test]
	fn renders_user_and_assistant_text_with_a_title() {
		let messages = vec![Message::user("hello"), Message::assistant("hi there")];
		let markdown = format_history_markdown("Worker (parked)", &messages);
		assert!(markdown.starts_with("# Worker (parked)\n"));
		assert!(markdown.contains("## user\n\nhello"));
		assert!(markdown.contains("## assistant\n\nhi there"));
	}

	#[test]
	fn collapses_tool_call_and_result_into_one_line() {
		let call = ToolCall {
			id:     "call-1".to_string(),
			name:   "read".to_string(),
			arguments: json!({ "path": "src/foo.ts", "i": "reading file" }),
		};
		let mut result = Message::tool("call-1", "line one\nline two\nline three");
		result.name = Some("read".to_string());
		let messages = vec![
			Message::user("inspect"),
			Message::assistant_with_tools("", vec![call]),
			result,
		];
		let markdown = format_history_markdown("Worker", &messages);
		assert!(
			markdown.contains("→ read(src/foo.ts) ⇒ ok · 3 lines"),
			"{markdown}"
		);
		assert!(
			!markdown.contains("## tool"),
			"the paired result must not render as a section: {markdown}"
		);
		assert_eq!(markdown.matches("→ read(").count(), 1);
	}

	#[test]
	fn elides_thinking_and_redacted_thinking() {
		let messages = vec![assistant_with_parts(
			"visible answer",
			vec![
				ContentPart::thinking("private reasoning"),
				ContentPart::thinking_with_signature("signed reasoning", "sig"),
				ContentPart::redacted_thinking("opaque"),
				ContentPart::text("visible answer"),
			],
			Vec::new(),
		)];
		let markdown = format_history_markdown("Worker", &messages);
		assert!(markdown.contains("visible answer"));
		assert!(!markdown.contains("private reasoning"), "{markdown}");
		assert!(!markdown.contains("signed reasoning"), "{markdown}");
		assert!(!markdown.contains("opaque"), "{markdown}");
	}

	#[test]
	fn renders_unanswered_calls_and_orphan_results() {
		let call = ToolCall {
			id:     "call-pending".to_string(),
			name:   "grep".to_string(),
			arguments: json!({ "pattern": "fn main", "path": "src" }),
		};
		let mut orphan = Message::tool("call-orphan", "one line\nboom");
		orphan.name = Some("bash".to_string());
		orphan.is_error = true;
		let messages = vec![
			Message::assistant_with_tools("", vec![call]),
			orphan,
		];
		let markdown = format_history_markdown("Worker", &messages);
		assert!(markdown.contains("→ grep(fn main @ src) ⇒ pending"), "{markdown}");
		assert!(
			markdown.contains("→ bash() ⇒ error · 2 lines — one line"),
			"{markdown}"
		);
	}

	#[test]
	fn empty_tool_result_reports_zero_lines_and_specials_pick_primary_arg() {
		let call = ToolCall {
			id:     "call-empty".to_string(),
			name:   "bash".to_string(),
			arguments: json!({ "command": "true" }),
		};
		let mut empty = Message::tool("call-empty", "");
		empty.name = Some("bash".to_string());
		let markdown = format_history_markdown(
			"Worker",
			&[Message::assistant_with_tools("", vec![call]), empty],
		);
		assert!(markdown.contains("→ bash(true) ⇒ ok · 0 lines"), "{markdown}");

		assert_eq!(
			super::primary_arg("advise", Some(&json!({ "severity": "error", "note": "blocking" }))),
			"error: blocking"
		);
		assert_eq!(
			super::primary_arg("ast_grep", Some(&json!({ "pat": "fn $F($$$)", "path": "src" }))),
			"fn $F($$$)"
		);
	}

	#[test]
	fn excludes_system_prompt_messages() {
		let messages = vec![
			Message::system("SECRET SYSTEM PROMPT"),
			Message::user("visible"),
		];
		let markdown = format_history_markdown("Worker", &messages);
		assert!(!markdown.contains("SECRET SYSTEM PROMPT"));
		assert!(markdown.contains("visible"));
	}
}
