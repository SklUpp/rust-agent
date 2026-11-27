use std::collections::HashSet;

use crate::{
	state::{ContentPart, Message, Role},
	tokenizer::Tokenizer,
};

/// Prefix of the system notice [`Compactor::compact`] inserts when it prunes the
/// head. Callers that journal a conversation by index use it to find the new
/// start: after compaction the old indices name different messages.
pub const COMPACTION_NOTICE_PREFIX: &str = "[System notice: ";

/// Exact placeholder written over a stale `read` result superseded by a newer
/// read/write/edit on the same path. The text is stable because journals and
/// tests compare it; never fold a token count into it.
pub const SUPERSEDED_NOTICE: &str = "[Pruned: superseded by newer operation on <path>]";

/// Whether `message` is the prune notice inserted by [`Compactor::compact`].
pub fn is_compaction_notice(message: &Message) -> bool {
	message.role == Role::System && message.content.starts_with(COMPACTION_NOTICE_PREFIX)
}

/// One local context-reduction method, in the donor's `methodOrder` vocabulary.
///
/// `Handoff` and `Remote` are summarizer-class methods: this module has no model
/// handle, so they are accepted in an order but never executed here. `Soft`
/// drops whole old turns; `Shake` truncates oversized tool output first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionMethod {
	Soft,
	Shake,
	Handoff,
	Remote,
}

impl CompactionMethod {
	/// Parse one `compaction.methodOrder` entry. Unknown methods (for example
	/// `snapcompact`, which this crate does not implement) return `None`.
	#[must_use]
	pub fn parse(raw: &str) -> Option<Self> {
		match raw {
			"soft" => Some(Self::Soft),
			"shake" => Some(Self::Shake),
			"handoff" => Some(Self::Handoff),
			"remote" | "context-full" => Some(Self::Remote),
			_ => None,
		}
	}

	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Soft => "soft",
			Self::Shake => "shake",
			Self::Handoff => "handoff",
			Self::Remote => "remote",
		}
	}
}

/// Tunables for one compaction pass.
///
/// The defaults mirror the TS donor settings where this crate can honour them:
/// `keepRecentTokens = 20_000`, supersede pruning and shake enabled, and shake
/// before the soft cut (the donor schema's fallback order ends
/// `handoff → shake → soft`).
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionOptions {
	/// Master switch for compaction; disabled is a no-op.
	pub enabled:                 bool,
	/// Allow the local loop to compact between provider requests.
	pub mid_turn_enabled:        bool,
	/// Fixed trigger threshold from `compaction.thresholdTokens`; wins over the
	/// percentage. `None` keeps the legacy budget/reserve path.
	pub threshold_tokens:        Option<usize>,
	/// Trigger threshold as a percentage of the context window from
	/// `compaction.thresholdPercent`. Applied only without `threshold_tokens`.
	pub threshold_percent:       Option<f64>,
	/// Keep at least this many recent tokens even when the budget is tighter.
	/// `0` disables the token floor and keeps the legacy budget-only behavior.
	pub keep_recent_tokens:      usize,
	/// Tokens held back below the caller's budget for the next request.
	pub reserve_tokens:          usize,
	/// Messages that survive a soft cut even when they exceed the budget.
	pub min_recent_messages:     usize,
	/// Replace older read results superseded by a newer read of the same file.
	pub prune_superseded_reads:  bool,
	/// Truncate oversized old tool results before dropping whole turns.
	pub shake_tool_outputs:      bool,
	/// Tool-result text longer than this (in chars) is a shake candidate.
	pub shake_max_chars:         usize,
	/// Local methods to try, in order. Empty disables every method.
	pub method_order:            Vec<CompactionMethod>,
}

impl Default for CompactionOptions {
	fn default() -> Self {
		Self {
			enabled:                true,
			mid_turn_enabled:       true,
			threshold_tokens:       None,
			threshold_percent:      None,
			keep_recent_tokens:     20_000,
			reserve_tokens:         0,
			min_recent_messages:    4,
			prune_superseded_reads: true,
			shake_tool_outputs:     true,
			shake_max_chars:        4_000,
			method_order:           vec![CompactionMethod::Shake, CompactionMethod::Soft],
		}
	}
}

impl CompactionOptions {
	/// Legacy `compact(…, min_recent)` behavior: budget-only soft cut, no
	/// supersede/shake passes, no recent-token floor.
	fn legacy(min_recent: usize) -> Self {
		Self {
			keep_recent_tokens: 0,
			min_recent_messages: min_recent,
			prune_superseded_reads: false,
			shake_tool_outputs: false,
			method_order: vec![CompactionMethod::Soft],
			..Self::default()
		}
	}

	/// Effective trigger tokens for a context window, mirroring
	/// `resolveThresholdTokens`: a fixed token limit wins, then a positive
	/// percentage, otherwise the window itself (the reserve is applied later by
	/// the legacy path).
	#[must_use]
	pub fn resolve_threshold_tokens(&self, context_window: usize) -> usize {
		if context_window == 0 {
			return 0;
		}
		if let Some(tokens) = self.threshold_tokens {
			return tokens.max(1).min(context_window - 1);
		}
		if let Some(percent) = self.threshold_percent.filter(|p| p.is_finite() && *p > 0.0) {
			let clamped = percent.clamp(1.0, 99.0);
			return ((context_window as f64) * (clamped / 100.0)).floor() as usize;
		}
		context_window
	}

	/// Whether a configured threshold replaces the reserve-derived budget.
	fn has_configured_threshold(&self) -> bool {
		self.threshold_tokens.is_some()
			|| self.threshold_percent.is_some_and(|percent| percent.is_finite() && percent > 0.0)
	}
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
	pub old_len: usize,
	/// Number of leading system messages retained verbatim by the cut.
	pub leading_system: usize,
	/// Old index of the first retained conversation message (after the leading
	/// system messages and the pruned prefix).
	pub kept_from: usize,
	/// Index of that first retained message in the compacted conversation.
	pub kept_at: usize,
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

/// Result of a full compaction pass beyond the index mapping.
///
/// `rewritten_indices` name tool results in the final compacted slice whose
/// content was replaced in place (supersede prune or shake) without changing
/// message order or length. Owners that journal incrementally must persist
/// those rewrites even when no soft cut happened and `report` is `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionOutcome {
	/// Present exactly when a soft cut dropped a prefix.
	pub report: Option<CompactionReport>,
	/// Final indices of tool results whose content changed in place.
	pub rewritten_indices: Vec<usize>,
}

/// A tool call resolved from an assistant message, keyed by call id.
#[derive(Debug, Clone)]
struct ToolCallMeta {
	name: String,
	path: Option<String>,
}

/// Pair each message with the tool call it originated from: results attach to
/// the nearest preceding call with the same id. Keeping this positional means
/// interleaved or repeated batches cannot make an earlier call win merely
/// because a map stores its metadata last.
fn pair_tool_calls(messages: &[Message]) -> Vec<Option<ToolCallMeta>> {
	let mut calls: Vec<Option<ToolCallMeta>> = vec![None; messages.len()];
	let mut pending: Vec<(&str, ToolCallMeta)> = Vec::new();
	for (index, message) in messages.iter().enumerate() {
		if message.role == Role::Assistant {
			for call in &message.tool_calls {
				pending.push((
					call.id.as_str(),
					ToolCallMeta {
						name: call.name.clone(),
						path: tool_path(&call.arguments),
					},
				));
			}
			continue;
		}
		if message.role != Role::Tool {
			continue;
		}
		let Some(id) = message.tool_call_id.as_deref() else {
			continue;
		};
		if let Some(position) = pending.iter().rposition(|(call_id, _)| *call_id == id) {
			let (_, meta) = pending.remove(position);
			calls[index] = Some(meta);
		}
	}
	calls
}

/// `path` argument shared by `read` and `write`; `edit` variants also use it
/// except the hashline form, which embeds paths in its input syntax.
fn tool_path(arguments: &serde_json::Value) -> Option<String> {
	["path", "file_path", "filePath"]
		.iter()
		.find_map(|key| arguments.get(key).and_then(serde_json::Value::as_str))
		.filter(|path| !path.is_empty())
		.map(str::to_string)
}

/// Split a read path into its base and trailing selector using the donor
/// grammar (`packages/agent/src/compaction/utils.ts`, `splitReadSelector`).
/// `path:50-200`, `path:raw`, `path:conflicts`, and the `range:raw` compounds
/// are selectors; a bare `db.sqlite:users` stays one path.
#[must_use]
pub fn split_read_selector(path: &str) -> (String, Option<String>) {
	// `str::rfind` on a byte index is safe here because `:` is ASCII and the
	// candidate is validated before either side is sliced.
	let Some(colon) = path.rfind(':').filter(|colon| *colon > 0) else {
		return (path.to_string(), None);
	};
	let candidate = &path[colon + 1..];
	if !is_read_selector(candidate) {
		return (path.to_string(), None);
	}
	let mut base = &path[..colon];
	let mut selector = candidate.to_string();
	if let Some(inner) = base.rfind(':').filter(|inner| *inner > 0) {
		let inner_candidate = &base[inner + 1..];
		let inner_is_raw = inner_candidate.eq_ignore_ascii_case("raw");
		let outer_is_raw = candidate.eq_ignore_ascii_case("raw");
		let inner_is_range = is_read_range(inner_candidate);
		let outer_is_range = is_read_range(candidate);
		if (inner_is_raw && outer_is_range) || (inner_is_range && outer_is_raw) {
			selector = format!("{inner_candidate}:{candidate}");
			base = &base[..inner];
		}
	}
	(base.to_string(), Some(selector))
}

fn is_read_selector(candidate: &str) -> bool {
	candidate.eq_ignore_ascii_case("raw")
		|| candidate.eq_ignore_ascii_case("conflicts")
		|| is_read_range(candidate)
}

/// One comma-separated range chunk: `L?\d+` with an optional `-N`, `+N`, `..N`,
/// bare `-`, or bare `..` suffix.
fn is_read_range(value: &str) -> bool {
	if value.is_empty() {
		return false;
	}
	value.split(',').all(is_read_range_chunk)
}

fn is_read_range_chunk(chunk: &str) -> bool {
	let after_line = chunk.strip_prefix(['L', 'l']).unwrap_or(chunk);
	let digits = after_line.bytes().take_while(u8::is_ascii_digit).count();
	if digits == 0 {
		return false;
	}
	let rest = &after_line[digits..];
	if rest.is_empty() {
		return true;
	}
	if rest == "-" || rest == ".." {
		return true;
	}
	let after_op = rest
		.strip_prefix('-')
		.or_else(|| rest.strip_prefix('+'))
		.or_else(|| rest.strip_prefix(".."));
	let Some(after_op) = after_op else {
		return false;
	};
	let after_op = after_op.strip_prefix(['L', 'l']).unwrap_or(after_op);
	!after_op.is_empty() && after_op.bytes().all(|byte| byte.is_ascii_digit())
}

/// Supersede key for a `read` call (donor `readToolSupersedeKey`): the selector
/// is stripped from the base path and retained after a NUL so only equal
/// selectors collide. A selector-free read keys on the bare path and therefore
/// supersedes every selector-carrying read of that file. URLs and internal
/// schemes are exempt because they are not files the agent can re-ground on.
fn read_supersede_key(meta: &ToolCallMeta) -> Option<(String, String)> {
	if meta.name != "read" {
		return None;
	}
	let path = meta.path.as_deref()?;
	if path.is_empty() || path.contains("://") {
		return None;
	}
	let (base, selector) = split_read_selector(path);
	let key = match selector {
		Some(selector) => format!("{base}\u{0}{selector}"),
		None => base.clone(),
	};
	Some((key, base))
}

/// Supersede-prune protection from the donor's `tool-protection.ts`: the whole
/// `skill` tool, plus any `read` of a `skill://` resource.
fn is_supersede_protected(meta: &ToolCallMeta) -> bool {
	meta.name == "skill"
		|| (meta.name == "read"
			&& meta
				.path
				.as_deref()
				.is_some_and(|path| path.starts_with("skill://")))
}

/// Shake protection additionally keeps `artifact://` recovery reads: eliding
/// one merely mints another artifact and can repeat indefinitely.
fn is_shake_protected(meta: &ToolCallMeta) -> bool {
	is_supersede_protected(meta)
		|| (meta.name == "read"
			&& meta
				.path
				.as_deref()
				.is_some_and(|path| path.starts_with("artifact://")))
}

fn is_write_like(name: &str) -> bool {
	matches!(name, "write" | "edit" | "apply_patch")
}

/// Cut start that keeps at least `min_recent_messages` and
/// `keep_recent_tokens`, walking back over an assistant tool call when the
/// budget boundary lands inside its tool-result run. Returning the assistant
/// (not merely dropping the orphan results) preserves the call/result pair.
fn protected_start(
	conversation: &[Message],
	options: &CompactionOptions,
	count_message: impl Fn(&Message) -> usize,
) -> usize {
	let mut start = conversation.len();
	let mut tokens = 0;
	while start > 0 {
		let kept = conversation.len() - start;
		let must_keep = kept < options.min_recent_messages
			|| (options.keep_recent_tokens > 0 && tokens < options.keep_recent_tokens);
		if !must_keep {
			break;
		}
		tokens += count_message(&conversation[start - 1]);
		start -= 1;
	}
	while start < conversation.len() && start > 0 && conversation[start].role == Role::Tool {
		start -= 1;
	}
	start
}

/// Budget cut start for the soft pass. Keeps as many recent messages as fit,
/// then extends the tail to `min_recent_messages` / `keep_recent_tokens` and
/// finally walks back to the assistant owning a split tool-result run.
fn soft_cut_start(
	conversation: &[Message],
	budget: usize,
	options: &CompactionOptions,
	count_message: impl Fn(&Message) -> usize,
) -> usize {
	let mut start = conversation.len();
	let mut tokens = 0;
	while start > 0 {
		let kept = conversation.len() - start;
		let message_tokens = count_message(&conversation[start - 1]);
		let must_keep = kept < options.min_recent_messages
			|| (options.keep_recent_tokens > 0 && tokens < options.keep_recent_tokens);
		if must_keep || tokens.saturating_add(message_tokens) <= budget {
			tokens = tokens.saturating_add(message_tokens);
			start -= 1;
		} else {
			break;
		}
	}
	while start < conversation.len() && start > 0 && conversation[start].role == Role::Tool {
		start -= 1;
	}
	start
}

/// Replace superseded read results outside the protected tail. Scans backwards
/// so the newest operation of a path is the one that survives; a newer
/// `write`/`edit` on the same base path also invalidates an older read.
/// Successful calls are keyed by occurrence `(assistant message index, call
/// index)` so a reused call id cannot borrow another turn's result.
///
/// Supersede pruning must only trust an operation that actually succeeded: a
/// denied or failed write leaves the older successful read authoritative.
fn succeeded_tool_calls(conversation: &[Message]) -> HashSet<(usize, usize)> {
	let mut pending: Vec<(&str, usize, usize)> = Vec::new();
	let mut succeeded = HashSet::new();
	for (message_index, message) in conversation.iter().enumerate() {
		if message.role == Role::Assistant {
			for (call_index, call) in message.tool_calls.iter().enumerate() {
				pending.push((call.id.as_str(), message_index, call_index));
			}
			continue;
		}
		if message.role != Role::Tool {
			continue;
		}
		let Some(id) = message.tool_call_id.as_deref() else {
			continue;
		};
		let Some(position) = pending.iter().rposition(|(call_id, _, _)| *call_id == id) else {
			continue;
		};
		let (_, assistant_index, call_index) = pending.remove(position);
		// A staged write/edit reports success but leaves the workspace untouched.
		// It cannot invalidate an older read, so only an applied write counts.
		if !message.is_error && !is_staged_tool_result(message) {
			succeeded.insert((assistant_index, call_index));
		}
	}
	succeeded
}

/// Whether a successful tool result only staged its effect.
///
/// `Message` keeps the model-facing text but not typed tool `details`, so the
/// stable notices emitted by the staged write/edit variants are the signal.
fn is_staged_tool_result(message: &Message) -> bool {
	message.content.contains("Workspace NOT modified yet")
		|| message.content.contains("files NOT modified yet")
}

/// Donor `effectiveReserveTokens`: never reserve more than a quarter of the
/// window, and always keep a little headroom for the response. A fixed reserve
/// (16 384 by default) must not consume a small local model's whole budget.
fn effective_reserve_tokens(reserve_tokens: usize, context_window: usize) -> usize {
	if context_window == 0 {
		return reserve_tokens;
	}
	let proportional_cap = context_window / 4;
	let safety_headroom = (context_window / 20).max(256);
	let max_reserve = context_window.saturating_sub(safety_headroom);
	reserve_tokens.min(proportional_cap).min(max_reserve)
}

fn prune_superseded_reads(
	conversation: &mut [Message],
	protected_start: usize,
) -> Vec<usize> {
	let calls = pair_tool_calls(conversation);
	let succeeded = succeeded_tool_calls(conversation);
	let mut seen_keys: HashSet<String> = HashSet::new();
	let mut seen_bare_paths: HashSet<String> = HashSet::new();
	let mut newer_writes: HashSet<String> = HashSet::new();
	let mut rewritten = Vec::new();

	for index in (0..conversation.len()).rev() {
		match conversation[index].role {
			Role::Assistant => {
				for (call_index, call) in conversation[index].tool_calls.iter().enumerate() {
					let meta = ToolCallMeta {
						name: call.name.clone(),
						path: tool_path(&call.arguments),
					};
					if is_write_like(&meta.name)
						&& succeeded.contains(&(index, call_index))
						&& let Some(path) = meta.path
					{
						newer_writes.insert(path);
					}
				}
			},
			Role::Tool => {
				let Some(meta) = calls[index].as_ref() else {
					continue;
				};
				if conversation[index].is_error {
					continue;
				}
				if is_supersede_protected(meta) {
					continue;
				}
				let Some((key, base)) = read_supersede_key(meta) else {
					continue;
				};
				let carries_selector = key.contains('\u{0}');
				let superseded = seen_keys.contains(&key)
					|| (carries_selector && seen_bare_paths.contains(&base))
					|| newer_writes.contains(&base);
				if superseded && index < protected_start {
					let before = conversation[index].content.clone();
					conversation[index].content = SUPERSEDED_NOTICE.to_string();
					// Keep image/non-text parts (the donor keeps non-text content
					// for shake; a read result can carry an image).
					conversation[index]
						.parts
						.retain(|part| !matches!(part, ContentPart::Text { .. }));
					if conversation[index].content != before {
						rewritten.push(index);
					}
				}
				seen_keys.insert(key);
				if !carries_selector {
					seen_bare_paths.insert(base);
				}
			},
			_ => {},
		}
	}
	rewritten
}

/// Head+tail excerpt for one oversized tool result. The marker keeps the exact
/// `shaken` word callers grep for while reporting the original character count.
fn shake_text(text: &str, shake_max_chars: usize) -> String {
	let total_chars = text.chars().count();
	let head_len = shake_max_chars / 2;
	let tail_len = shake_max_chars.saturating_sub(head_len);
	let head: String = text.chars().take(head_len).collect();
	let tail: String = text.chars().skip(total_chars.saturating_sub(tail_len)).collect();
	format!("{head}\n[... shaken {total_chars} chars ...]\n{tail}")
}

/// Shake oversized tool results outside the protected tail. The donor applies a
/// recent-token window (`protectTokens`), skips skill/artifact reads, and never
/// touches tool-call blocks; this slice version has no timestamps and never
/// rewrites an assistant tool call.
fn shake_tool_outputs(
	conversation: &mut [Message],
	protected_start: usize,
	options: &CompactionOptions,
) -> Vec<usize> {
	let calls = pair_tool_calls(conversation);
	let mut rewritten = Vec::new();
	for index in 0..protected_start.min(conversation.len()) {
		if conversation[index].role != Role::Tool {
			continue;
		}
		if calls[index].as_ref().is_some_and(is_shake_protected) {
			continue;
		}
		let total_chars = conversation[index].content.chars().count();
		if total_chars <= options.shake_max_chars {
			continue;
		}
		let before = conversation[index].content.clone();
		let original = std::mem::take(&mut conversation[index].content);
		conversation[index].content = shake_text(&original, options.shake_max_chars);
		conversation[index]
			.parts
			.retain(|part| !matches!(part, ContentPart::Text { .. }));
		if conversation[index].content != before {
			rewritten.push(index);
		}
	}
	rewritten
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

	/// [`Self::compact`] with token counts from `model`'s family.
	pub fn compact_for_model(
		model: &str,
		messages: &[Message],
		token_budget: usize,
		min_recent: usize,
	) -> Vec<Message> {
		Self::compact_report_for_model(model, messages, token_budget, min_recent).0
	}

	/// [`Self::compact`] plus the index mapping it applied, when it pruned.
	pub fn compact_report(
		messages: &[Message],
		token_budget: usize,
		min_recent: usize,
	) -> (Vec<Message>, Option<CompactionReport>) {
		Self::compact_report_with(
			messages,
			token_budget,
			min_recent,
			|msg| Tokenizer::count_message(msg),
			|messages| Tokenizer::count_messages(messages),
		)
	}

	/// [`Self::compact_report`] with token counts from `model`'s family.
	pub fn compact_report_for_model(
		model: &str,
		messages: &[Message],
		token_budget: usize,
		min_recent: usize,
	) -> (Vec<Message>, Option<CompactionReport>) {
		Self::compact_report_with(
			messages,
			token_budget,
			min_recent,
			|msg| Tokenizer::count_message_for_model(model, msg),
			|messages| Tokenizer::count_messages_for_model(model, messages),
		)
	}

	/// Full compaction policy: supersede prune → `method_order` passes.
	pub fn compact_report_with_options(
		messages: &[Message],
		token_budget: usize,
		options: &CompactionOptions,
	) -> (Vec<Message>, Option<CompactionReport>) {
		let (messages, outcome) =
			Self::compact_outcome_with_options(messages, token_budget, options);
		(messages, outcome.report)
	}

	/// [`Self::compact_report_with_options`] with the in-place rewrite indices.
	pub fn compact_outcome_with_options(
		messages: &[Message],
		token_budget: usize,
		options: &CompactionOptions,
	) -> (Vec<Message>, CompactionOutcome) {
		Self::compact_outcome_with_counters(
			messages,
			token_budget,
			options,
			|msg| Tokenizer::count_message(msg),
			|messages| Tokenizer::count_messages(messages),
		)
	}

	/// [`Self::compact_outcome_with_options`] with `model`'s tokenizer family.
	pub fn compact_outcome_for_model_with_options(
		model: &str,
		messages: &[Message],
		token_budget: usize,
		options: &CompactionOptions,
	) -> (Vec<Message>, CompactionOutcome) {
		Self::compact_outcome_with_counters(
			messages,
			token_budget,
			options,
			|msg| Tokenizer::count_message_for_model(model, msg),
			|messages| Tokenizer::count_messages_for_model(model, messages),
		)
	}

	/// [`Self::compact_report_with_options`] with `model`'s tokenizer family.
	pub fn compact_report_for_model_with_options(
		model: &str,
		messages: &[Message],
		token_budget: usize,
		options: &CompactionOptions,
	) -> (Vec<Message>, Option<CompactionReport>) {
		let (messages, outcome) =
			Self::compact_outcome_for_model_with_options(model, messages, token_budget, options);
		(messages, outcome.report)
	}

	fn compact_report_with(
		messages: &[Message],
		token_budget: usize,
		min_recent: usize,
		count_message: impl Fn(&Message) -> usize,
		count_messages: impl Fn(&[Message]) -> usize,
	) -> (Vec<Message>, Option<CompactionReport>) {
		let (messages, outcome) = Self::compact_outcome_with_counters(
			messages,
			token_budget,
			&CompactionOptions::legacy(min_recent),
			count_message,
			count_messages,
		);
		(messages, outcome.report)
	}

	fn compact_outcome_with_counters(
		messages: &[Message],
		token_budget: usize,
		options: &CompactionOptions,
		count_message: impl Fn(&Message) -> usize,
		count_messages: impl Fn(&[Message]) -> usize,
	) -> (Vec<Message>, CompactionOutcome) {
		if !options.enabled {
			return (messages.to_vec(), CompactionOutcome::default());
		}
		if messages.is_empty() {
			return (Vec::new(), CompactionOutcome::default());
		}

		let token_budget = if options.has_configured_threshold() {
			// An explicit settings threshold is the trigger and the budget; the
			// reserve must not push a 50%-of-window threshold back to ~84%.
			options.resolve_threshold_tokens(token_budget)
		} else {
			token_budget
				.saturating_sub(effective_reserve_tokens(options.reserve_tokens, token_budget))
		};
		let total_tokens = count_messages(messages);
		if total_tokens <= token_budget {
			return (messages.to_vec(), CompactionOutcome::default());
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

		let system_tokens = count_messages(&system_messages);
		let budget = token_budget.saturating_sub(system_tokens);

		if conversation.is_empty() {
			return (system_messages, CompactionOutcome::default());
		}

		// A default 20k floor must never protect the whole conversation when
		// the provider window leaves less than that for the conversation.
		let options = CompactionOptions {
			keep_recent_tokens: options.keep_recent_tokens.min(budget),
			..options.clone()
		};

		// Supersede pruning is correctness work, not a fallback: a stale read is
		// dead weight whether or not the soft cut would later drop it.
		let mut rewritten = Vec::new();
		if options.prune_superseded_reads {
			let protected = protected_start(&conversation, &options, &count_message);
			rewritten.extend(prune_superseded_reads(&mut conversation, protected));
		}

		let mut current_tokens = count_messages(&system_messages) + count_messages(&conversation);
		for method in &options.method_order {
			if current_tokens <= token_budget {
				break;
			}
			match method {
				CompactionMethod::Shake if options.shake_tool_outputs => {
					let protected = protected_start(&conversation, &options, &count_message);
					let shaken = shake_tool_outputs(&mut conversation, protected, &options);
					if !shaken.is_empty() {
						rewritten.extend(shaken);
						current_tokens =
							count_messages(&system_messages) + count_messages(&conversation);
					}
				},
				CompactionMethod::Soft => {
					let cut = soft_cut_start(&conversation, budget, &options, &count_message);
					if cut == 0 && rewritten.is_empty() {
						return (messages.to_vec(), CompactionOutcome::default());
					}
					let leading = system_messages.len();
					let report = (cut > 0).then_some(CompactionReport {
						old_len: messages.len(),
						leading_system: leading,
						kept_from: leading + cut,
						kept_at: leading + 1,
					});
					let rewritten_indices = rewritten
						.into_iter()
						.filter(|index| *index >= cut)
						.map(|index| if cut > 0 {
							leading + 1 + (index - cut)
						} else {
							leading + index
						})
						.collect();
					let mut result = system_messages;
					if cut > 0 {
						let summary_notice = format!(
							"{COMPACTION_NOTICE_PREFIX}{cut} earlier turn(s) were pruned to fit the context limit]"
						);
						result.push(Message::system(summary_notice));
					}
					result.extend(conversation.into_iter().skip(cut));
					return (
						result,
						CompactionOutcome { report, rewritten_indices },
					);
				},
				// Summarizer-class methods need a provider; this module prunes only.
				CompactionMethod::Shake
				| CompactionMethod::Handoff
				| CompactionMethod::Remote => {},
			}
		}

		if rewritten.is_empty() {
			return (messages.to_vec(), CompactionOutcome::default());
		}
		let leading = system_messages.len();
		let rewritten_indices = rewritten
			.into_iter()
			.map(|index| leading + index)
			.collect();
		let mut result = system_messages;
		result.extend(conversation);
		(
			result,
			CompactionOutcome { report: None, rewritten_indices },
		)
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
		let report =
			CompactionReport { old_len: 12, leading_system: 1, kept_from: 7, kept_at: 2 };
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

	fn read_call(id: &str, path: &str) -> Message {
		Message::assistant_with_tools(
			"reading",
			vec![ToolCall {
				id: id.to_string(),
				name: "read".to_string(),
				arguments: serde_json::json!({ "path": path }),
			}],
		)
	}

	fn write_call(id: &str, path: &str) -> Message {
		Message::assistant_with_tools(
			"writing",
			vec![ToolCall {
				id: id.to_string(),
				name: "write".to_string(),
				arguments: serde_json::json!({ "path": path, "content": "new" }),
			}],
		)
	}

	#[test]
	fn tool_call_cut_point_keeps_the_assistant_and_its_results() {
		let msgs = vec![
			Message::system("System prompt"),
			Message::user("Please do task"),
			Message::assistant_with_tools(
				"running tool",
				vec![ToolCall {
					id: "call_1".to_string(),
					name: "read".to_string(),
					arguments: serde_json::json!({ "path": "foo" }),
				}],
			),
			Message::tool("call_1", "file content"),
			Message::user("Next user query"),
			Message::assistant("Done!"),
		];
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 3,
			prune_superseded_reads: false,
			shake_tool_outputs: false,
			method_order: vec![CompactionMethod::Soft],
			..CompactionOptions::default()
		};
		let (compacted, report) = Compactor::compact_report_with_options(&msgs, 20, &options);
		let report = report.expect("turn dropped");
		assert_eq!(compacted[report.kept_at].role, Role::Assistant);
		assert!(
			!compacted[report.kept_at].tool_calls.is_empty(),
			"the kept assistant owns the tool call"
		);
		assert_eq!(compacted[report.kept_at + 1].role, Role::Tool);
		assert_eq!(compacted[report.kept_at + 1].tool_call_id.as_deref(), Some("call_1"));
	}

	#[test]
	fn keep_recent_tokens_floor_is_clamped_to_the_context_budget() {
		let filler = "alpha beta gamma delta ".repeat(600);
		let mut msgs = Vec::new();
		for index in 0..12 {
			msgs.push(Message::user(format!("question {index} {filler}")));
			msgs.push(Message::assistant(format!("answer {index} {filler}")));
		}
		let options = CompactionOptions {
			keep_recent_tokens: 20_000,
			min_recent_messages: 0,
			prune_superseded_reads: false,
			shake_tool_outputs: false,
			method_order: vec![CompactionMethod::Soft],
			..CompactionOptions::default()
		};
		let (compacted, report) = Compactor::compact_report_with_options(&msgs, 16_000, &options);
		assert!(
			report.is_some(),
			"a 20k floor on a 16k conversation must not protect every message"
		);
		assert!(compacted.len() < msgs.len());
	}

	#[test]
	fn duplicate_tool_call_ids_pair_results_to_the_nearest_call() {
		let msgs = vec![
			read_call("dup", "src/a.ts"),
			Message::tool("dup", "a body ".repeat(200)),
			read_call("dup", "src/b.ts"),
			Message::tool("dup", "b body"),
			Message::user("continue"),
		];
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 0,
			prune_superseded_reads: true,
			shake_tool_outputs: false,
			method_order: vec![],
			..CompactionOptions::default()
		};
		let (compacted, outcome) =
			Compactor::compact_outcome_with_options(&msgs, 10, &options);
		assert!(
			outcome.rewritten_indices.is_empty(),
			"distinct paths must not collapse through a duplicate call id"
		);
		assert!(compacted[1].content.starts_with("a body"));
		assert_eq!(compacted[3].content, "b body");
	}

	#[test]
	fn errored_read_result_does_not_mark_the_path_seen() {
		let msgs = vec![
			read_call("read_old", "src/foo.ts"),
			Message::tool("read_old", "old body"),
			read_call("read_failed", "src/foo.ts"),
			Message::tool_with_status("read_failed", "read", "permission denied", true),
			Message::user("continue"),
		];
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 0,
			prune_superseded_reads: true,
			shake_tool_outputs: false,
			method_order: vec![],
			..CompactionOptions::default()
		};
		let (compacted, outcome) =
			Compactor::compact_outcome_with_options(&msgs, 10, &options);
		assert!(
			outcome.rewritten_indices.is_empty(),
			"a failed read is not a newer read of the path"
		);
		assert_eq!(compacted[1].content, "old body");
	}

	#[test]
	fn fixed_reserve_is_clamped_for_a_small_window() {
		let mut msgs = Vec::new();
		for index in 0..10 {
			msgs.push(Message::user(format!("message {index}")));
		}
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 0,
			prune_superseded_reads: false,
			shake_tool_outputs: false,
			reserve_tokens: 16_384,
			method_order: vec![CompactionMethod::Soft],
			..CompactionOptions::default()
		};
		let (compacted, report) = Compactor::compact_report_with_options(&msgs, 1_000, &options);
		assert_eq!(report, None, "the 16k reserve must shrink to a quarter of a 1k window");
		assert_eq!(compacted.len(), msgs.len());
	}

	#[test]
	fn configured_threshold_percent_replaces_the_reserve_budget() {
		let msgs = vec![
			Message::user("alpha ".repeat(50)),
			Message::assistant("beta ".repeat(50)),
			Message::user("gamma ".repeat(50)),
			Message::assistant("delta ".repeat(50)),
			Message::user("epsilon ".repeat(50)),
			Message::assistant("zeta ".repeat(50)),
		];
		let total = Tokenizer::count_messages(&msgs);
		assert!(total > 10);
		// 50% of this window is just below the conversation, so only the
		// configured threshold triggers; the full-window budget does not.
		let window = total.saturating_mul(2).saturating_sub(10);
		let options = CompactionOptions {
			threshold_percent: Some(50.0),
			keep_recent_tokens: 0,
			min_recent_messages: 0,
			prune_superseded_reads: false,
			shake_tool_outputs: false,
			method_order: vec![CompactionMethod::Soft],
			..CompactionOptions::default()
		};
		let (_compacted, outcome) =
			Compactor::compact_outcome_with_options(&msgs, window, &options);
		assert!(
			outcome.report.is_some(),
			"50% threshold must trigger before the full context window"
		);
		let legacy = CompactionOptions { threshold_percent: None, ..options };
		let (unchanged, outcome) =
			Compactor::compact_outcome_with_options(&msgs, window, &legacy);
		assert_eq!(outcome, CompactionOutcome::default());
		assert_eq!(unchanged.len(), msgs.len());
	}

	#[test]
	fn fixed_threshold_tokens_win_over_percent() {
		let options = CompactionOptions {
			threshold_tokens: Some(1),
			threshold_percent: Some(99.0),
			..CompactionOptions::default()
		};
		assert_eq!(options.resolve_threshold_tokens(1_000), 1);
	}

	#[test]
	fn disabled_compaction_is_a_noop() {
		let msgs = vec![
			Message::user("one"),
			Message::assistant("two"),
			Message::user("three"),
		];
		let options = CompactionOptions {
			enabled: false,
			method_order: vec![CompactionMethod::Shake, CompactionMethod::Soft],
			..CompactionOptions::default()
		};
		let (compacted, outcome) =
			Compactor::compact_outcome_with_options(&msgs, 0, &options);
		assert_eq!(compacted, msgs);
		assert_eq!(outcome, CompactionOutcome::default());
	}

	#[test]
	fn supersede_pruning_replaces_only_the_older_read() {
		let old_text = "old file body ".repeat(400);
		let newer_text = "new file body";
		let msgs = vec![
			Message::system("System prompt"),
			read_call("read_old", "src/foo.ts"),
			Message::tool("read_old", old_text),
			read_call("read_new", "src/foo.ts"),
			Message::tool("read_new", newer_text),
			Message::user("continue"),
		];
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 0,
			prune_superseded_reads: true,
			shake_tool_outputs: false,
			method_order: vec![],
			..CompactionOptions::default()
		};
		let (compacted, outcome) =
			Compactor::compact_outcome_with_options(&msgs, 10, &options);
		assert_eq!(outcome.report, None, "content-only pruning does not shift indices");
		assert_eq!(outcome.rewritten_indices, vec![2]);
		assert_eq!(compacted[2].content, SUPERSEDED_NOTICE);
		assert_eq!(compacted[4].content, newer_text);
	}

	#[test]
	fn failed_write_does_not_supersede_an_earlier_read() {
		let msgs = vec![
			read_call("read_old", "src/foo.ts"),
			Message::tool("read_old", "old file body ".repeat(400)),
			write_call("write_new", "src/foo.ts"),
			Message::tool_with_status("write_new", "write", "permission denied", true),
			Message::user("continue"),
		];
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 0,
			prune_superseded_reads: true,
			shake_tool_outputs: false,
			method_order: vec![],
			..CompactionOptions::default()
		};
		let (compacted, outcome) =
			Compactor::compact_outcome_with_options(&msgs, 10, &options);
		assert_eq!(
			compacted[1].content,
			"old file body ".repeat(400),
			"a failed write must leave the successful read authoritative"
		);
		assert!(outcome.rewritten_indices.is_empty());
		assert_eq!(outcome.report, None);
	}

	#[test]
	fn failed_write_with_reused_call_id_does_not_supersede_the_read() {
		let msgs = vec![
			read_call("dup", "src/foo.ts"),
			Message::tool("dup", "old file body"),
			write_call("dup", "src/foo.ts"),
			Message::tool_with_status("dup", "write", "permission denied", true),
			Message::user("continue"),
		];
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 0,
			prune_superseded_reads: true,
			shake_tool_outputs: false,
			method_order: vec![],
			..CompactionOptions::default()
		};
		let (compacted, outcome) =
			Compactor::compact_outcome_with_options(&msgs, 10, &options);
		assert!(
			outcome.rewritten_indices.is_empty(),
			"a read result must not make a later failed write succeed"
		);
		assert_eq!(compacted[1].content, "old file body");
	}

	#[test]
	fn staged_writes_never_supersede_an_earlier_read() {
		for (call_id, staged) in [
			(
				"write_staged",
				"Staged file operation #7: update src/foo.ts (12 bytes). \
				 Workspace NOT modified yet.\nWrite to xd://resolve to apply or xd://reject to discard.",
			),
			(
				"edit_staged",
				"Staged as a proposal — files NOT modified yet. To apply: write a one-sentence reason \
				 to xd://resolve. To discard: write to xd://reject.",
			),
		] {
			let msgs = vec![
				read_call("read_old", "src/foo.ts"),
				Message::tool("read_old", "old file body ".repeat(400)),
				write_call(call_id, "src/foo.ts"),
				Message::tool_with_status(call_id, "write", staged, false),
				Message::user("continue"),
			];
			let options = CompactionOptions {
				keep_recent_tokens: 0,
				min_recent_messages: 0,
				prune_superseded_reads: true,
				shake_tool_outputs: false,
				method_order: vec![],
				..CompactionOptions::default()
			};
			let (compacted, outcome) =
				Compactor::compact_outcome_with_options(&msgs, 10, &options);
			assert!(
				outcome.rewritten_indices.is_empty(),
				"staged {call_id} must not supersede the read"
			);
			assert_eq!(compacted[1].content, "old file body ".repeat(400));
		}
	}

	#[test]
	fn a_newer_write_supersedes_an_older_read_of_the_same_path() {
		let msgs = vec![
			read_call("read_old", "src/foo.ts"),
			Message::tool("read_old", "old file body ".repeat(400)),
			write_call("write_new", "src/foo.ts"),
			Message::tool("write_new", "staged"),
			Message::user("continue"),
		];
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 0,
			prune_superseded_reads: true,
			shake_tool_outputs: false,
			method_order: vec![],
			..CompactionOptions::default()
		};
		let (compacted, _) = Compactor::compact_report_with_options(&msgs, 10, &options);
		assert_eq!(compacted[1].content, SUPERSEDED_NOTICE);
	}

	#[test]
	fn read_selectors_key_like_the_donor() {
		assert_eq!(split_read_selector("src/foo.ts"), ("src/foo.ts".to_string(), None));
		assert_eq!(
			split_read_selector("src/foo.ts:50-200"),
			("src/foo.ts".to_string(), Some("50-200".to_string()))
		);
		assert_eq!(
			split_read_selector("src/foo.ts:2-4:raw"),
			("src/foo.ts".to_string(), Some("2-4:raw".to_string()))
		);
		assert_eq!(
			split_read_selector("src/foo.ts:5-16,960-973"),
			("src/foo.ts".to_string(), Some("5-16,960-973".to_string()))
		);
		assert_eq!(
			split_read_selector("src/foo.ts:50+150"),
			("src/foo.ts".to_string(), Some("50+150".to_string()))
		);
		assert_eq!(
			split_read_selector("db.sqlite:users"),
			("db.sqlite:users".to_string(), None)
		);
		assert_eq!(
			split_read_selector("db.sqlite:users:42"),
			("db.sqlite:users".to_string(), Some("42".to_string()))
		);
	}

	#[test]
	fn supersede_respects_distinct_selectors_and_bare_read_prefix_rule() {
		let msgs = vec![
			read_call("a", "src/foo.ts:50-200"),
			Message::tool("a", "range a ".repeat(200)),
			read_call("b", "src/foo.ts:10-20"),
			Message::tool("b", "range b"),
			read_call("full", "src/foo.ts"),
			Message::tool("full", "full file"),
			Message::user("continue"),
		];
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 0,
			prune_superseded_reads: true,
			shake_tool_outputs: false,
			method_order: vec![],
			..CompactionOptions::default()
		};
		let (compacted, _) = Compactor::compact_report_with_options(&msgs, 10, &options);
		assert_eq!(compacted[1].content, SUPERSEDED_NOTICE);
		assert_eq!(compacted[3].content, SUPERSEDED_NOTICE);
		assert_eq!(compacted[5].content, "full file");
	}

	#[test]
	fn shake_truncates_only_old_oversized_tool_output() {
		let old_text = "old output line\n".repeat(2_000);
		let recent_text = "recent output";
		let msgs = vec![
			Message::user("first"),
			Message::tool("bash_old", old_text),
			Message::user("second"),
			Message::tool("bash_recent", recent_text),
		];
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 1,
			prune_superseded_reads: false,
			shake_tool_outputs: true,
			shake_max_chars: 200,
			method_order: vec![CompactionMethod::Shake],
			..CompactionOptions::default()
		};
		let (compacted, outcome) =
			Compactor::compact_outcome_with_options(&msgs, 10, &options);
		assert_eq!(outcome.report, None);
		assert_eq!(outcome.rewritten_indices, vec![1]);
		assert!(compacted[1].content.contains("[... shaken "));
		assert!(compacted[1].content.starts_with("old output line"));
		assert!(compacted[1].content.ends_with("old output line\n"));
		assert_eq!(compacted[3].content, recent_text, "protected recent tail unchanged");
	}

	#[test]
	fn shake_keeps_skill_and_artifact_reads() {
		let long = "protected output ".repeat(500);
		let msgs = vec![
			Message::user("first"),
			read_call("skill", "skill://react"),
			Message::tool("skill", long.clone()),
			read_call("artifact", "artifact://7"),
			Message::tool("artifact", long.clone()),
		];
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 0,
			prune_superseded_reads: false,
			shake_tool_outputs: true,
			shake_max_chars: 50,
			method_order: vec![CompactionMethod::Shake],
			..CompactionOptions::default()
		};
		let (compacted, _) = Compactor::compact_report_with_options(&msgs, 1, &options);
		assert_eq!(compacted[2].content, long);
		assert_eq!(compacted[4].content, long);
	}

	#[test]
	fn shake_and_soft_cut_remap_the_rewritten_index() {
		let msgs = vec![
			Message::user("old question"),
			Message::user("keep before the tool"),
			Message::tool("tool_old", "0123456789"),
			Message::assistant("a1"),
			Message::user("q2"),
			Message::assistant("a2"),
			Message::user("q3"),
		];
		let options = CompactionOptions {
			keep_recent_tokens: 3,
			min_recent_messages: 1,
			prune_superseded_reads: false,
			shake_tool_outputs: true,
			shake_max_chars: 5,
			method_order: vec![CompactionMethod::Shake, CompactionMethod::Soft],
			..CompactionOptions::default()
		};
		// The cut lands on "keep before the tool": one token per message and a
		// six-token budget keep the shaken tool result while dropping only the
		// first user turn.
		let (compacted, outcome) = Compactor::compact_outcome_with_counters(
			&msgs,
			6,
			&options,
			|_| 1,
			|messages| messages.len(),
		);
		let report = outcome.report.expect("soft cut drops the first message");
		assert_eq!((report.old_len, report.kept_from, report.kept_at), (7, 1, 1));
		assert_eq!(outcome.rewritten_indices, vec![2]);
		assert!(compacted[2].content.contains("[... shaken "));
		assert!(
			compacted[2].content.ends_with("789"),
			"rewrite index points at the retained shaken result"
		);
	}

	#[test]
	fn shake_then_soft_cut_reports_the_retained_tail() {
		let mut msgs = vec![Message::system("System prompt")];
		msgs.push(Message::user("old"));
		msgs.push(Message::tool("old_bash", "x".repeat(8_000)));
		for index in 0..10 {
			msgs.push(Message::user(format!("question {index} with words")));
			msgs.push(Message::assistant(format!("answer {index} with words")));
		}
		let options = CompactionOptions {
			keep_recent_tokens: 0,
			min_recent_messages: 2,
			prune_superseded_reads: false,
			shake_tool_outputs: true,
			shake_max_chars: 100,
			method_order: vec![CompactionMethod::Shake, CompactionMethod::Soft],
			..CompactionOptions::default()
		};
		let (compacted, report) = Compactor::compact_report_with_options(&msgs, 60, &options);
		let report = report.expect("soft cut drops turns");
		assert_eq!(report.old_len, msgs.len());
		assert_eq!(
			compacted.len(),
			report.kept_at + (report.old_len - report.kept_from)
		);
		assert_eq!(compacted[report.kept_at].content, msgs[report.kept_from].content);
	}
}
