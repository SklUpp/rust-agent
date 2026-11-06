use crate::{
	state::{Message, Role},
	tokenizer::Tokenizer,
};

/// Prefix of the system notice [`Compactor::compact`] inserts when it prunes the
/// head. Callers that journal a conversation by index use it to find the new
/// start: after compaction the old indices name different messages.
pub const COMPACTION_NOTICE_PREFIX: &str = "[System notice: ";

/// Whether `message` is the prune notice inserted by [`Compactor::compact`].
pub fn is_compaction_notice(message: &Message) -> bool {
	message.role == Role::System && message.content.starts_with(COMPACTION_NOTICE_PREFIX)
}

/// Compactor preserves system prompt and recent turns while pruning older messages
/// when estimated token count exceeds the budget.
pub struct Compactor;

/// What [`Compactor::compact`] dropped, for owners that journal incrementally.
///
/// `kept_from` is the old index where the retained conversation resumes, and
/// `kept_at` is that same message's index after the rewrite (right after the
/// prune notice). An owner remaps its write cursor with [`Self::remap`] instead
/// of guessing from lengths: lengths can match again after new messages and
/// then silently skip or duplicate accepted turns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionReport {
	/// Conversation length before the rewrite.
	pub old_len:    usize,
	/// Old index of the first retained conversation message (after the leading
	/// system messages and the pruned prefix).
	pub kept_from:  usize,
	/// Index of that first retained message in the compacted conversation.
	pub kept_at:    usize,
}

impl CompactionReport {
	/// Translate a cursor in the old index space into the compacted one.
	///
	/// A cursor at or before the pruned prefix resumes at the first kept
	/// message; a later cursor keeps its distance from the kept boundary, so
	/// everything after it stays unwritten and nothing old is appended twice.
	#[must_use]
	pub fn remap(&self, written: usize) -> usize {
		if written <= self.kept_from {
			self.kept_at
		} else {
			self.kept_at + (written - self.kept_from)
		}
	}
}

impl Compactor {
	/// Compact messages slice to fit within token_budget.
	/// Guarantees:
	/// 1. System messages at the head are preserved.
	/// 2. At least min_recent_messages (default e.g. 4) are kept if possible.
	/// 3. Older user/assistant/tool messages are pruned or summarized.
	/// 4. Never orphan Tool messages: keeps assistant message with tool_calls together with its Tool response messages.
	pub fn compact(messages: &[Message], token_budget: usize, min_recent: usize) -> Vec<Message> {
		Self::compact_report(messages, token_budget, min_recent).0
	}

	/// [`Self::compact`] plus the index mapping it applied, when it pruned.
	pub fn compact_report(
		messages: &[Message],
		token_budget: usize,
		min_recent: usize,
	) -> (Vec<Message>, Option<CompactionReport>) {
		if messages.is_empty() {
			return (Vec::new(), None);
		}

		let total_tokens = Tokenizer::count_messages(messages);
		if total_tokens <= token_budget {
			return (messages.to_vec(), None);
		}

		// Partition system messages at the start
		let mut system_messages = Vec::new();
		let mut conversation = Vec::new();

		let mut in_leading_system = true;
		for msg in messages {
			if in_leading_system && msg.role == Role::System {
				system_messages.push(msg.clone());
			} else {
				in_leading_system = false;
				conversation.push(msg.clone());
			}
		}

		let system_tokens = Tokenizer::count_messages(&system_messages);
		let remaining_budget = token_budget.saturating_sub(system_tokens);

		if conversation.is_empty() {
			return (system_messages, None);
		}

		// We need to keep the most recent messages from conversation
		let mut kept_reverse = Vec::new();
		let mut current_tokens = 0;

		// Walk backwards from newest to oldest
		for (idx, msg) in conversation.iter().rev().enumerate() {
			let msg_tokens = Tokenizer::count_message(msg);
			if idx < min_recent || (current_tokens + msg_tokens <= remaining_budget) {
				current_tokens += msg_tokens;
				kept_reverse.push(msg.clone());
			} else {
				break;
			}
		}

		kept_reverse.reverse();

		// Clean up boundary to avoid orphaning tool messages:
		// If the first message in kept_reverse is Role::Tool, it is missing its predecessor assistant message.
		// We must drop leading orphan Tool messages until the sequence starts cleanly at a user or assistant message.
		while let Some(first) = kept_reverse.first() {
			if first.role == Role::Tool {
				kept_reverse.remove(0);
			} else {
				break;
			}
		}

		let pruned_count = conversation.len().saturating_sub(kept_reverse.len());

		let leading = system_messages.len();
		let mut result = system_messages;
		if pruned_count > 0 {
			let summary_notice = format!(
				"{COMPACTION_NOTICE_PREFIX}{} earlier turn(s) were pruned to fit the context limit]",
				pruned_count
			);
			result.push(Message::system(summary_notice));
		}
		result.extend(kept_reverse);
		if pruned_count == 0 {
			return (result, None);
		}
		let report = CompactionReport {
			old_len:   messages.len(),
			kept_from: leading + pruned_count,
			kept_at:   leading + 1,
		};
		(result, Some(report))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::state::ToolCall;

	#[test]
	fn test_compaction_under_budget() {
		let msgs = vec![
			Message::system("System instructions"),
			Message::user("Hello"),
			Message::assistant("Hi there!"),
		];
		let compacted = Compactor::compact(&msgs, 10_000, 2);
		assert_eq!(compacted.len(), 3);
	}

	#[test]
	fn test_compaction_over_budget() {
		let mut msgs = vec![Message::system("System prompt")];
		for i in 0..20 {
			msgs.push(Message::user(format!("Turn {i} question with some length")));
			msgs.push(Message::assistant(format!("Turn {i} response with some length")));
		}

		let compacted = Compactor::compact(&msgs, 150, 4);
		assert!(compacted.len() < msgs.len());
		assert_eq!(compacted[0].role, Role::System);
		assert_eq!(compacted[0].content, "System prompt");
		assert!(
			compacted
				.iter()
				.any(|m| m.content.contains("earlier turn(s) were pruned"))
		);
	}

	/// The report must describe exactly where the retained conversation starts:
	/// new length = kept_at + (old_len - kept_from). Owners remap their journal
	/// cursor with it, so a length that later matches the old one cannot hide
	/// accepted turns or re-append journaled ones.
	#[test]
	fn compaction_report_maps_the_retained_tail() {
		let mut msgs = vec![Message::system("System prompt")];
		for i in 0..20 {
			msgs.push(Message::user(format!("Turn {i} question with some length")));
			msgs.push(Message::assistant(format!("Turn {i} response with some length")));
		}
		let (compacted, report) = Compactor::compact_report(&msgs, 150, 4);
		let report = report.expect("over-budget compaction always reports");
		assert_eq!(report.old_len, msgs.len());
		assert_eq!(report.kept_at, 2, "system prompt + prune notice");
		assert_eq!(
			compacted.len(),
			report.kept_at + (report.old_len - report.kept_from),
			"every retained message maps one to one"
		);
		// The first retained message is the one the report points at.
		assert_eq!(compacted[report.kept_at].content, msgs[report.kept_from].content);
	}

	/// A cursor inside the pruned prefix resumes at the first kept message; a
	/// cursor past it keeps its distance, so nothing old is appended twice.
	#[test]
	fn compaction_report_remaps_cursors_on_both_sides_of_the_prune() {
		let report = CompactionReport { old_len: 12, kept_from: 7, kept_at: 2 };
		assert_eq!(report.remap(0), 2);
		assert_eq!(report.remap(7), 2);
		assert_eq!(report.remap(8), 3);
		assert_eq!(report.remap(12), 7);
	}

	#[test]
	fn test_compaction_prevents_orphan_tool_message() {
		let msgs = vec![
			Message::system("System prompt"),
			Message::user("Please do task"),
			Message::assistant_with_tools(
				"running tool",
				vec![ToolCall {
					id: "call_1".to_string(),
					name: "read".to_string(),
					arguments: serde_json::json!({"path": "foo"}),
				}],
			),
			Message::tool("call_1", "file content"),
			Message::user("Next user query"),
			Message::assistant("Done!"),
		];

		// Budget enough only for the last 3 messages (Tool, User, Assistant), which would orphan Tool
		let compacted = Compactor::compact(&msgs, 25, 1);
		assert_ne!(compacted[1].role, Role::Tool, "First non-system message must not be orphan Tool");
	}
}
