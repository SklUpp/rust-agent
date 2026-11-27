//! OpenAI-wire message hygiene: malformed tool-call removal, tool-call id
//! clamping and duplicate-id disambiguation.
//!
//! Port of `packages/ai/src/providers/transform-messages.ts`
//! (`sanitizeMalformedToolCalls`, `deduplicateToolCallIds`). Messages are
//! transformed before the OpenAI projection so every replay leaves with ids the
//! endpoint accepts: tool calls with an empty name/id are dropped together with
//! their paired result, ids are capped at [`MAX_TOOL_CALL_ID_LENGTH`], and a
//! repeated id gets a `_dupN` suffix mirrored onto its result.

use std::{
	borrow::Cow,
	collections::{BTreeMap, BTreeSet, VecDeque},
};

use crate::state::{ContentPart, Message, Role, StopReason, ToolCall};

/// Maximum tool-call id length the strictest replay provider accepts.
pub(crate) const MAX_TOOL_CALL_ID_LENGTH: usize = 64;

/// Suffix prefix for a colliding tool-call id (TS `duplicateSuffixPrefix`).
pub(crate) const DUPLICATE_SUFFIX_PREFIX: &str = "_dup";

/// Apply malformed-call sanitization, id clamping and duplicate rewriting.
///
/// Borrows the input when nothing changes so a clean history costs no clone.
pub(crate) fn transform_messages(messages: &[Message]) -> Cow<'_, [Message]> {
	let sanitized = sanitize_malformed_tool_calls(messages);
	let input = sanitized.as_deref().unwrap_or(messages);
	match normalize_tool_call_ids(input) {
		Some(owned) => Cow::Owned(owned),
		None => match sanitized {
			Some(owned) => Cow::Owned(owned),
			None => Cow::Borrowed(messages),
		},
	}
}

/// Drop malformed tool calls, their paired results and truncated
/// thinking-only assistant turns.
///
/// Pairing is positional per id within one assistant→result window: a repeated
/// id may have one malformed occurrence whose result must not consume the
/// surviving valid occurrence's output. Queues are cleared at every boundary
/// (assistant turn start, non-result message) so a missing malformed result
/// cannot leak into a later turn that reuses the id.
fn sanitize_malformed_tool_calls(messages: &[Message]) -> Option<Vec<Message>> {
	let needs_pass = messages.iter().any(|message| {
		should_drop_truncated_thinking_only(message)
			|| (message.role == Role::Assistant
				&& (message.tool_calls.iter().any(is_malformed_tool_call)
					|| message.parts.iter().any(is_malformed_tool_call_part)))
	});
	if !needs_pass {
		return None;
	}

	let mut drop_queues: BTreeMap<String, VecDeque<bool>> = BTreeMap::new();
	let mut out = Vec::with_capacity(messages.len());
	for message in messages {
		match message.role {
			Role::Assistant => {
				drop_queues.clear();
				let mut kept_calls = Vec::with_capacity(message.tool_calls.len());
				let mut dropped_call = false;
				for call in &message.tool_calls {
					let malformed = is_malformed_tool_call(call);
					drop_queues.entry(call.id.clone()).or_default().push_back(malformed);
					if malformed {
						dropped_call = true;
					} else {
						kept_calls.push(call.clone());
					}
				}
				let malformed_part = message.parts.iter().any(is_malformed_tool_call_part);
				let mut filtered = message.clone();
				if dropped_call {
					filtered.tool_calls = kept_calls;
				}
				if malformed_part {
					// The execution projection owns result pairing when it
					// exists; otherwise the streamed blocks are the only call
					// record and every one needs a queue slot.
					if message.tool_calls.is_empty() {
						for part in &message.parts {
							if let ContentPart::ToolCall { id, .. } = part {
								drop_queues
									.entry(id.clone())
									.or_default()
									.push_back(is_malformed_tool_call_part(part));
							}
						}
					}
					let mut kept_parts = Vec::with_capacity(message.parts.len());
					for part in &message.parts {
						if is_malformed_tool_call_part(part) {
							continue;
						}
						kept_parts.push(part.clone());
					}
					filtered.parts = kept_parts;
				}
				if should_drop_truncated_thinking_only(&filtered) {
					continue;
				}
				// An assistant turn whose only replayable content was the
				// malformed call must not leave an empty shell behind.
				if (dropped_call || malformed_part)
					&& filtered.tool_calls.is_empty()
					&& !has_visible_text(&filtered)
					&& !has_replayable_part(&filtered)
				{
					continue;
				}
				out.push(filtered);
			},
			Role::Tool => {
				let id = message.tool_call_id.as_deref().unwrap_or_default();
				let mut queue_empty = false;
				let dropped = match drop_queues.get_mut(id) {
					Some(queue) => {
						let dropped = queue.pop_front() == Some(true);
						queue_empty = queue.is_empty();
						dropped
					},
					None => false,
				};
				if queue_empty {
					drop_queues.remove(id);
				}
				if dropped {
					continue;
				}
				out.push(message.clone());
			},
			_ => {
				drop_queues.clear();
				out.push(message.clone());
			},
		}
	}
	Some(out)
}

/// `true` when at least one tool-call id must be clamped or renamed. Pure
/// containment check: no clones, no rewrite planning.
fn history_needs_id_rewrite(messages: &[Message]) -> bool {
	let mut seen: BTreeSet<String> = BTreeSet::new();
	for message in messages {
		if message.role != Role::Assistant {
			continue;
		}
		for call in &message.tool_calls {
			let base = clamp_tool_call_id(&call.id);
			if base != call.id || !seen.insert(base) {
				return true;
			}
		}
	}
	false
}

/// Clamp every tool-call id and give colliding ids a unique `_dupN` suffix.
///
/// The replacement is recorded positionally per original id so the paired
/// [`Role::Tool`] message is rewritten to the same wire id (a result pointing at
/// the unrenamed id would be an orphan the endpoint rejects).
fn normalize_tool_call_ids(messages: &[Message]) -> Option<Vec<Message>> {
	// Fast path: no overlong id and no collision means every id already round
	// trips, so the history is returned untouched without cloning it.
	if !history_needs_id_rewrite(messages) {
		return None;
	}
	let mut seen: BTreeMap<String, usize> = BTreeMap::new();
	let mut pending: BTreeMap<String, VecDeque<Option<String>>> = BTreeMap::new();
	let mut changed = false;
	let mut out = Vec::with_capacity(messages.len());
	for message in messages {
		match message.role {
			Role::Tool => {
				let original = message.tool_call_id.clone().unwrap_or_default();
				let Some(queue) = pending.get_mut(&original) else {
					out.push(message.clone());
					continue;
				};
				let rewrite = queue.pop_front();
				if queue.is_empty() {
					pending.remove(&original);
				}
				match rewrite {
					Some(Some(id)) => {
						changed = true;
						let mut rewritten = message.clone();
						rewritten.tool_call_id = Some(id);
						out.push(rewritten);
					},
					_ => out.push(message.clone()),
				}
			},
			Role::Assistant => {
				let mut touched_in_turn: BTreeSet<String> = BTreeSet::new();
				let mut calls_changed = false;
				let mut calls = Vec::with_capacity(message.tool_calls.len());
				for call in &message.tool_calls {
					let base = clamp_tool_call_id(&call.id);
					// Drop stale rewrites carried from an earlier turn on this
					// id's first appearance; otherwise the old `_dup` id would
					// steal the result that belongs to this turn's call.
					if touched_in_turn.insert(call.id.clone()) {
						pending.remove(&call.id);
					}
					let previous = seen.get(&base).copied().unwrap_or(0);
					let replacement = if previous == 0 {
						seen.insert(base.clone(), 1);
						base.clone()
					} else {
						let mut index = previous;
						let mut candidate = append_duplicate_suffix(&base, index);
						while seen.contains_key(&candidate) {
							index += 1;
							candidate = append_duplicate_suffix(&base, index);
						}
						seen.insert(base.clone(), index + 1);
						seen.insert(candidate.clone(), 1);
						candidate
					};
					if replacement != call.id {
						calls_changed = true;
					}
					let rewrite = (replacement != call.id).then(|| replacement.clone());
					pending
						.entry(call.id.clone())
						.or_default()
						.push_back(rewrite);
					let mut normalized = call.clone();
					normalized.id = replacement;
					calls.push(normalized);
				}
				if calls_changed {
					changed = true;
					let mut rewritten = message.clone();
					rewritten.tool_calls = calls;
					out.push(rewritten);
				} else {
					out.push(message.clone());
				}
			},
			_ => out.push(message.clone()),
		}
	}
	changed.then_some(out)
}

fn is_malformed_tool_call(call: &ToolCall) -> bool {
	call.id.trim().is_empty() || call.name.trim().is_empty()
}

fn is_malformed_tool_call_part(part: &ContentPart) -> bool {
	matches!(
		part,
		ContentPart::ToolCall { id, name, .. } if id.trim().is_empty() || name.trim().is_empty()
	)
}

fn is_truncated_stop(reason: Option<StopReason>) -> bool {
	matches!(reason, Some(StopReason::Length | StopReason::Error | StopReason::Aborted))
}

/// A truncated turn with no tool call and no visible text replayed is stale
/// thinking garbage, never a valid empty answer (TS
/// `shouldDropTruncatedThinkingOnlyAssistant`).
fn should_drop_truncated_thinking_only(message: &Message) -> bool {
	message.role == Role::Assistant
		&& is_truncated_stop(message.stop_reason)
		&& message.tool_calls.is_empty()
		&& !has_visible_text(message)
}

fn has_visible_text(message: &Message) -> bool {
	!message.content.trim().is_empty()
		|| message.parts.iter().any(|part| {
			matches!(part, ContentPart::Text { text } if !text.trim().is_empty())
		})
}

/// Parts that survive replay to an OpenAI-compatible endpoint. Reasoning
/// blocks are print/persist only, and empty text contributes no content.
fn has_replayable_part(message: &Message) -> bool {
	message.parts.iter().any(|part| match part {
		ContentPart::Text { text } => !text.trim().is_empty(),
		ContentPart::Image { .. } | ContentPart::ToolCall { .. } => true,
		ContentPart::Thinking { .. } | ContentPart::RedactedThinking { .. } => false,
	})
}

/// Clamp an id to [`MAX_TOOL_CALL_ID_LENGTH`] per segment. Responses-family
/// composite ids (`callId|itemId`) are capped per segment because the wire
/// extractors split on `|` and apply the bound per segment.
fn clamp_tool_call_id(id: &str) -> String {
	if id.contains('|') {
		id
			.split('|')
			.map(clamp_tool_call_id_segment)
			.collect::<Vec<_>>()
			.join("|")
	} else {
		clamp_tool_call_id_segment(id)
	}
}

fn clamp_tool_call_id_segment(segment: &str) -> String {
	if segment.chars().count() <= MAX_TOOL_CALL_ID_LENGTH {
		return segment.to_string();
	}
	segment.chars().take(MAX_TOOL_CALL_ID_LENGTH).collect()
}

/// Append `_dupN`, truncating the id prefix so the result stays within the
/// wire bound. Composite ids suffix every segment for the same reason
/// [`clamp_tool_call_id`] clamps each one.
fn append_duplicate_suffix(base: &str, index: usize) -> String {
	let suffix = format!("{DUPLICATE_SUFFIX_PREFIX}{index}");
	if base.contains('|') {
		base
			.split('|')
			.map(|segment| append_segment_suffix(segment, &suffix))
			.collect::<Vec<_>>()
			.join("|")
	} else {
		append_segment_suffix(base, &suffix)
	}
}

fn append_segment_suffix(segment: &str, suffix: &str) -> String {
	let segment_length = segment.chars().count();
	let suffix_length = suffix.chars().count();
	if segment_length + suffix_length <= MAX_TOOL_CALL_ID_LENGTH {
		return format!("{segment}{suffix}");
	}
	let budget = MAX_TOOL_CALL_ID_LENGTH.saturating_sub(suffix_length);
	let prefix: String = segment.chars().take(budget).collect();
	format!("{prefix}{suffix}")
}

#[cfg(test)]
mod tests {
	use super::*;

	fn call(id: &str, name: &str) -> ToolCall {
		ToolCall {
			id:          id.to_string(),
			name:        name.to_string(),
			arguments:   serde_json::json!({}),
		}
	}

	fn tool_call_ids(messages: &[Message]) -> Vec<String> {
		messages.iter().flat_map(|message| &message.tool_calls).map(|call| call.id.clone()).collect()
	}

	#[test]
	fn clean_history_is_borrowed_unchanged() {
		let messages = [Message::user("hi")];
		assert!(matches!(transform_messages(&messages), Cow::Borrowed(_)));
	}

	#[test]
	fn clamps_overlong_tool_call_id_on_call_and_paired_result() {
		let long = "tool_".repeat(16);
		assert_eq!(long.chars().count(), 80);
		let expected: String = long.chars().take(MAX_TOOL_CALL_ID_LENGTH).collect();
		let messages = [
			Message::assistant_with_tools("", vec![call(&long, "read")]),
			Message::tool(&long, "ok"),
		];
		let transformed = transform_messages(&messages);
		assert_eq!(tool_call_ids(&transformed), vec![expected.clone()]);
		assert_eq!(transformed[1].tool_call_id.as_deref(), Some(expected.as_str()));

		let wire = crate::provider::to_openai_messages(&messages).expect("wire");
		assert_eq!(wire[0].tool_calls.as_ref().expect("calls")[0].id, expected);
		assert_eq!(wire[1].tool_call_id.as_deref(), Some(expected.as_str()));
	}

	#[test]
	fn duplicate_tool_call_ids_get_suffix_and_rewrite_paired_results() {
		let messages = [
			Message::assistant_with_tools("", vec![call("call_1", "read")]),
			Message::tool("call_1", "one"),
			Message::assistant_with_tools("", vec![call("call_1", "read")]),
			Message::tool("call_1", "two"),
		];
		let transformed = transform_messages(&messages);
		assert_eq!(tool_call_ids(&transformed), vec!["call_1", "call_1_dup1"]);
		let result_ids: Vec<_> = transformed
			.iter()
			.filter(|message| message.role == Role::Tool)
			.map(|message| message.tool_call_id.clone().unwrap_or_default())
			.collect();
		assert_eq!(result_ids, vec!["call_1", "call_1_dup1"]);
		assert_eq!(transformed[1].content, "one");
		assert_eq!(transformed[3].content, "two");
	}

	#[test]
	fn duplicates_within_one_assistant_turn_pair_in_order() {
		let messages = [
			Message::assistant_with_tools(
				"",
				vec![call("call_same", "read"), call("call_same", "read")],
			),
			Message::tool("call_same", "first"),
			Message::tool("call_same", "second"),
		];
		let transformed = transform_messages(&messages);
		assert_eq!(tool_call_ids(&transformed), vec!["call_same", "call_same_dup1"]);
		assert_eq!(transformed[1].tool_call_id.as_deref(), Some("call_same"));
		assert_eq!(transformed[2].tool_call_id.as_deref(), Some("call_same_dup1"));
		assert_eq!(transformed[1].content, "first");
		assert_eq!(transformed[2].content, "second");
	}

	#[test]
	fn duplicate_suffix_never_exceeds_the_wire_bound() {
		let base = "a".repeat(MAX_TOOL_CALL_ID_LENGTH);
		let messages = [
			Message::assistant_with_tools("", vec![call(&base, "read")]),
			Message::tool(&base, "one"),
			Message::assistant_with_tools("", vec![call(&base, "read")]),
			Message::tool(&base, "two"),
		];
		let transformed = transform_messages(&messages);
		let ids = tool_call_ids(&transformed);
		assert_eq!(ids[0], base);
		assert_eq!(ids[1].chars().count(), MAX_TOOL_CALL_ID_LENGTH);
		assert!(ids[1].ends_with("_dup1"));
	}

	#[test]
	fn composite_response_ids_suffix_each_segment() {
		let composite = "call_x|fc_y";
		let messages = [
			Message::assistant_with_tools("", vec![call(composite, "read")]),
			Message::tool(composite, "first"),
			Message::assistant_with_tools("", vec![call(composite, "read")]),
			Message::tool(composite, "second"),
		];
		let transformed = transform_messages(&messages);
		assert_eq!(tool_call_ids(&transformed), vec![composite, "call_x_dup1|fc_y_dup1"]);
		assert_eq!(transformed[3].tool_call_id.as_deref(), Some("call_x_dup1|fc_y_dup1"));
	}

	#[test]
	fn drops_malformed_call_and_its_paired_result_but_keeps_visible_text() {
		let mut assistant = Message::assistant_with_tools(
			"Working on it.",
			vec![call("call_empty", "")],
		);
		assistant.parts = vec![ContentPart::text("Working on it.")];
		let messages = [
			Message::user("go"),
			assistant,
			Message::tool("call_empty", "Tool not found"),
			Message::user("continue"),
		];
		let transformed = transform_messages(&messages);
		assert_eq!(transformed.len(), 3);
		assert_eq!(transformed[1].role, Role::Assistant);
		assert!(transformed[1].tool_calls.is_empty());
		assert_eq!(transformed[1].parts, vec![ContentPart::text("Working on it.")]);
		assert!(transformed.iter().all(|message| message.role != Role::Tool));
	}

	#[test]
	fn drops_turn_and_result_when_only_a_malformed_call_was_emitted() {
		let messages = [
			Message::user("go"),
			Message::assistant_with_tools("", vec![call("call_bad", "   ")]),
			Message::tool("call_bad", "Tool not found"),
			Message::user("continue"),
		];
		let transformed = transform_messages(&messages);
		assert_eq!(transformed.iter().map(|message| message.role).collect::<Vec<_>>(), vec![
			Role::User,
			Role::User
		]);
	}

	#[test]
	fn drops_assistant_shell_when_only_a_malformed_tool_call_part_remains() {
		for (id, name) in [("", "read"), ("call_bad", "   ")] {
			let mut assistant = Message::assistant("");
			assistant.parts =
				vec![ContentPart::tool_call(id, name, serde_json::json!({}))];
			let messages = [
				Message::user("go"),
				assistant,
				Message::tool(id, "Tool not found"),
				Message::user("continue"),
			];
			let transformed = transform_messages(&messages);
			assert_eq!(
				transformed.iter().map(|message| message.role).collect::<Vec<_>>(),
				vec![Role::User, Role::User],
				"case id={id:?} name={name:?}"
			);
		}
	}

	#[test]
	fn projected_malformed_call_is_not_queued_twice_from_parts() {
		let mut assistant =
			Message::assistant_with_tools("", vec![call("call_bad", "")]);
		assistant.parts =
			vec![ContentPart::tool_call("call_bad", "", serde_json::json!({}))];
		let messages = [
			Message::user("go"),
			assistant,
			Message::tool("call_bad", "Tool not found"),
			Message::assistant_with_tools("", vec![call("call_bad", "read")]),
			Message::tool("call_bad", "contents"),
		];
		let transformed = transform_messages(&messages);
		assert_eq!(
			transformed.iter().map(|message| message.role).collect::<Vec<_>>(),
			vec![Role::User, Role::Assistant, Role::Tool]
		);
		assert_eq!(transformed[2].content, "contents");
	}

	#[test]
	fn drops_assistant_shell_with_thinking_and_malformed_call() {
		let mut assistant =
			Message::assistant_with_tools("", vec![call("call_bad", "")]);
		assistant.parts = vec![
			ContentPart::thinking("reasoning"),
			ContentPart::tool_call("call_bad", "", serde_json::json!({})),
		];
		let messages = [
			Message::user("go"),
			assistant,
			Message::tool("call_bad", "Tool not found"),
			Message::user("continue"),
		];
		let transformed = transform_messages(&messages);
		assert_eq!(
			transformed.iter().map(|message| message.role).collect::<Vec<_>>(),
			vec![Role::User, Role::User]
		);
	}

	#[test]
	fn empty_tool_call_id_is_malformed_and_its_result_is_dropped() {
		let messages = [
			Message::user("go"),
			Message::assistant_with_tools(
				"Reading.",
				vec![call("", "read"), call("call_keep", "read")],
			),
			Message::tool("", "cannot match"),
			Message::tool("call_keep", "contents"),
		];
		let transformed = transform_messages(&messages);
		assert_eq!(tool_call_ids(&transformed), vec!["call_keep"]);
		let results: Vec<_> = transformed
			.iter()
			.filter(|message| message.role == Role::Tool)
			.map(|message| message.tool_call_id.clone().unwrap_or_default())
			.collect();
		assert_eq!(results, vec!["call_keep"]);
		assert_eq!(transformed[2].content, "contents");
	}

	#[test]
	fn malformed_occurrence_does_not_drop_the_valid_reused_id_result() {
		let messages = [
			Message::user("first"),
			Message::assistant_with_tools("", vec![call("shared", "")]),
			Message::tool("shared", "not found"),
			Message::user("second"),
			Message::assistant_with_tools("", vec![call("shared", "read")]),
			Message::tool("shared", "real contents"),
		];
		let transformed = transform_messages(&messages);
		assert_eq!(tool_call_ids(&transformed), vec!["shared"]);
		let results: Vec<_> = transformed
			.iter()
			.filter(|message| message.role == Role::Tool)
			.map(|message| message.content.clone())
			.collect();
		assert_eq!(results, vec!["real contents"]);
	}

	#[test]
	fn missing_malformed_result_cannot_consume_a_later_valid_result() {
		let messages = [
			Message::user("first"),
			Message::assistant_with_tools("", vec![call("shared", "")]),
			Message::user("second"),
			Message::assistant_with_tools("", vec![call("shared", "read")]),
			Message::tool("shared", "real contents"),
		];
		let transformed = transform_messages(&messages);
		assert_eq!(tool_call_ids(&transformed), vec!["shared"]);
		let results: Vec<_> = transformed
			.iter()
			.filter(|message| message.role == Role::Tool)
			.map(|message| message.content.clone())
			.collect();
		assert_eq!(results, vec!["real contents"]);
	}

	#[test]
	fn drops_truncated_thinking_only_turn_but_keeps_valid_empty_turns() {
		let mut truncated = Message::assistant("");
		truncated.parts = vec![ContentPart::thinking("half a thought")];
		truncated.stop_reason = Some(StopReason::Length);
		let valid_empty = Message::assistant("");
		let messages = [truncated, valid_empty];
		let transformed = transform_messages(&messages);
		assert_eq!(transformed.len(), 1);
		assert_eq!(transformed[0].stop_reason, None);

		let mut answered_truncated = Message::assistant("visible");
		answered_truncated.stop_reason = Some(StopReason::Error);
		let transformed = transform_messages(&[answered_truncated]);
		assert_eq!(transformed.len(), 1);
	}
}
