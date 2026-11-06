//! Advice rendering, delivery-channel policy, and the per-session emission gate.
//!
//! Ports `advisor/advise-tool.ts` (`resolveAdvisorDeliveryChannel`,
//! `formatAdvisorBatchContent`, `isAdvisorInterruptImmuneTurnActive`) and
//! `advisor/emission-guard.ts` (`normalizeAdvisorNote`, noise list, FIFO dedupe,
//! one accepted note per update). The gate is intentionally invisible to the
//! advisor model: a suppressed call still answers `Recorded.`, so the model is
//! not tempted to rephrase the same useless note past the dedupe.

use std::collections::{HashSet, VecDeque};

/// How strongly the primary should weigh one note.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
	Nit,
	Concern,
	Blocker,
}

impl Severity {
	#[must_use]
	pub fn parse(value: &str) -> Option<Self> {
		match value {
			"nit" => Some(Self::Nit),
			"concern" => Some(Self::Concern),
			"blocker" => Some(Self::Blocker),
			_ => None,
		}
	}

	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Nit => "nit",
			Self::Concern => "concern",
			Self::Blocker => "blocker",
		}
	}

	/// Omitted severity defers to `nit`, exactly like the tool schema.
	#[must_use]
	pub fn rank(self) -> u8 {
		match self {
			Self::Nit => 1,
			Self::Concern => 2,
			Self::Blocker => 3,
		}
	}
}

/// One advice note, with the producing advisor's name when it has one (the
/// legacy/default advisor stamps no name, keeping the bytes identical to the
/// single-advisor path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
	pub note:     String,
	pub severity: Option<Severity>,
	pub advisor:  Option<String>,
}

impl Note {
	#[must_use]
	pub fn details(&self) -> serde_json::Value {
		let mut map = serde_json::Map::new();
		map.insert("note".to_string(), serde_json::Value::String(self.note.clone()));
		if let Some(severity) = self.severity {
			map.insert(
				"severity".to_string(),
				serde_json::Value::String(severity.as_str().to_string()),
			);
		}
		if let Some(advisor) = &self.advisor {
			map.insert(
				"advisor".to_string(),
				serde_json::Value::String(advisor.clone()),
			);
		}
		serde_json::Value::Object(map)
	}
}

/// Details payload on the batched `advisor` custom message.
#[must_use]
pub fn details_json(notes: &[Note]) -> serde_json::Value {
	serde_json::json!({
		"notes": notes.iter().map(Note::details).collect::<Vec<_>>(),
	})
}

/// Behavioral framing for the watched agent — advice, not orders.
pub const GUIDANCE: &str = "weigh, don't blindly obey";

fn escape_text(text: &str) -> String {
	let mut out = String::with_capacity(text.len());
	for ch in text.chars() {
		match ch {
			'&' => out.push_str("&amp;"),
			'<' => out.push_str("&lt;"),
			'>' => out.push_str("&gt;"),
			_ => out.push(ch),
		}
	}
	out
}

fn escape_attribute(text: &str) -> String {
	let mut out = String::with_capacity(text.len());
	for ch in text.chars() {
		match ch {
			'&' => out.push_str("&amp;"),
			'<' => out.push_str("&lt;"),
			'>' => out.push_str("&gt;"),
			'"' => out.push_str("&quot;"),
			_ => out.push(ch),
		}
	}
	out
}

/// Agent-facing batch body: one `<advisory>` element per note. Shared by the
/// non-interrupting aside dispatcher and the interrupting steer path, so both
/// branches are byte-identical.
#[must_use]
pub fn format_batch(notes: &[Note]) -> String {
	notes
		.iter()
		.map(|note| {
			let severity = note
				.severity
				.map(|severity| format!(" severity=\"{}\"", severity.as_str()))
				.unwrap_or_default();
			let advisor = note
				.advisor
				.as_deref()
				.map(|advisor| format!(" advisor=\"{}\"", escape_attribute(advisor)))
				.unwrap_or_default();
			format!(
				"<advisory{advisor}{severity} guidance=\"{GUIDANCE}\">\n{}\n</advisory>",
				escape_text(&note.note)
			)
		})
		.collect::<Vec<_>>()
		.join("\n")
}

#[must_use]
pub fn is_interrupting(severity: Option<Severity>) -> bool {
	matches!(severity, Some(Severity::Concern | Severity::Blocker))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
	Aside,
	Steer,
	Preserve,
}

/// Inputs that decide how one note reaches the primary.
#[derive(Debug, Clone, Copy)]
pub struct ChannelInputs {
	pub severity:                   Option<Severity>,
	pub auto_resume_suppressed:     bool,
	pub streaming:                  bool,
	pub aborting:                   bool,
	pub terminal_answer_no_queued:  bool,
	pub interrupt_immune_turn:      bool,
	pub preserve_only:              bool,
}

/// Exact port of `resolveAdvisorDeliveryChannel`. Idle triggering is not
/// available in this build: callers map an idle `Steer` to `Preserve`, never to
/// a hidden primary turn.
#[must_use]
pub fn resolve_channel(inputs: ChannelInputs) -> Channel {
	if inputs.preserve_only && !inputs.streaming {
		return Channel::Preserve;
	}
	if !is_interrupting(inputs.severity) {
		return Channel::Aside;
	}
	if inputs.auto_resume_suppressed && (inputs.aborting || !inputs.streaming) {
		return Channel::Preserve;
	}
	if inputs.terminal_answer_no_queued
		&& inputs.severity != Some(Severity::Blocker)
		&& !inputs.streaming
		&& !inputs.aborting
	{
		return Channel::Preserve;
	}
	if inputs.interrupt_immune_turn && inputs.severity != Some(Severity::Blocker) {
		return Channel::Aside;
	}
	Channel::Steer
}

/// Post-interrupt cooldown fence.
#[must_use]
pub fn immune_active(completed_turns: u64, immune_start: Option<u64>, immune_turns: i64) -> bool {
	let Some(start) = immune_start else {
		return false;
	};
	if immune_turns <= 0 {
		return false;
	}
	completed_turns < start.saturating_add(immune_turns as u64)
}

/// Fold the compatibility forms this build can handle without a Unicode
/// normalization dependency (fullwidth ASCII + ideographic space), lowercase,
/// then collapse every non-letter/non-digit run to one space.
///
/// `ponytail:` NFKC compatibility folding beyond the fullwidth block is absent;
/// add `unicode-normalization` if the tree ever takes that dependency.
#[must_use]
pub fn normalize_note(note: &str) -> String {
	let folded: String = note
		.chars()
		.map(|ch| match ch as u32 {
			0x3000 => ' ',
			0xFF01..=0xFF5E => char::from_u32(ch as u32 - 0xFF01 + 0x21).unwrap_or(ch),
			_ => ch,
		})
		.flat_map(char::to_lowercase)
		.collect();
	let mut out = String::with_capacity(folded.len());
	let mut pending_space = false;
	for ch in folded.chars() {
		if ch.is_alphanumeric() {
			if pending_space && !out.is_empty() {
				out.push(' ');
			}
			pending_space = false;
			out.push(ch);
		} else {
			pending_space = true;
		}
	}
	out
}

const SUPPRESSED: [&str; 37] = [
	"stop",
	"stop here",
	"stop now",
	"halt",
	"abort",
	"done",
	"task done",
	"task complete",
	"complete",
	"finished",
	"ok",
	"okay",
	"ok done",
	"no issue",
	"no issues",
	"no issue continue",
	"no concerns",
	"no concern",
	"nothing to add",
	"nothing to flag",
	"nothing to report",
	"no notes",
	"no further input",
	"no further input needed",
	"no further input required",
	"no further watcher input",
	"no further watcher input needed",
	"no further advice",
	"no further advice needed",
	"lgtm",
	"looks good",
	"all good",
	"agent is on track",
	"agent on track",
	"on track",
	"continue",
	"carry on",
];

const HISTORY_CAPACITY: usize = 4096;

/// Session-scoped gate: noise filter, exact-text dedupe with FIFO eviction, and
/// a per-update budget of one accepted note.
#[derive(Debug)]
pub struct EmissionGuard {
	seen:       HashSet<String>,
	order:      VecDeque<String>,
	consumed:   bool,
	capacity:   usize,
}

impl Default for EmissionGuard {
	fn default() -> Self {
		Self::new()
	}
}

impl EmissionGuard {
	#[must_use]
	pub fn new() -> Self {
		Self { seen: HashSet::new(), order: VecDeque::new(), consumed: false, capacity: HISTORY_CAPACITY }
	}

	pub fn reset(&mut self) {
		self.seen.clear();
		self.order.clear();
		self.consumed = false;
	}

	pub fn begin_update(&mut self) {
		self.consumed = false;
	}

	/// `true` when the note should reach the primary; the gate has then already
	/// consumed the update budget and recorded the note.
	pub fn accept(&mut self, note: &str) -> bool {
		let key = normalize_note(note);
		if key.is_empty() || SUPPRESSED.contains(&key.as_str()) {
			return false;
		}
		if self.seen.contains(&key) || self.consumed {
			return false;
		}
		self.consumed = true;
		self.seen.insert(key.clone());
		self.order.push_back(key);
		if self.order.len() > self.capacity
			&& let Some(stale) = self.order.pop_front()
		{
			self.seen.remove(&stale);
		}
		true
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn normalization_folds_punctuation_case_and_fullwidth() {
		assert_eq!(normalize_note("Stop."), "stop");
		assert_eq!(normalize_note("  No issue; continue. "), "no issue continue");
		assert_eq!(normalize_note("Ｆｕｌｌ　ｗｉｄｔｈ"), "full width");
		assert_eq!(normalize_note("***"), "");
	}

	#[test]
	fn noise_dedupe_and_one_per_update() {
		let mut guard = EmissionGuard::new();
		assert!(!guard.accept("Stop."));
		assert!(guard.accept("Check the await on writeStream.end()."));
		assert!(!guard.accept("Check the await on writeStream.end()."));
		assert!(!guard.accept("A different concern in the same update."));
		guard.begin_update();
		assert!(guard.accept("A different concern in the next update."));
		guard.reset();
		assert!(guard.accept("Check the await on writeStream.end()."));
	}

	#[test]
	fn batch_escapes_text_and_attributes() {
		let notes = vec![Note {
			note:     "Use <x> & \"y\"".to_string(),
			severity: Some(Severity::Blocker),
			advisor:  Some("review&er".to_string()),
		}];
		let rendered = format_batch(&notes);
		assert!(rendered.contains("severity=\"blocker\""));
		assert!(rendered.contains("advisor=\"review&amp;er\""));
		assert!(rendered.contains("Use &lt;x&gt; &amp; \"y\""));
		assert!(rendered.contains("guidance=\"weigh, don't blindly obey\""));
	}

	#[test]
	fn delivery_channel_branches_match_source() {
		let base = ChannelInputs {
			severity:                  None,
			auto_resume_suppressed:    false,
			streaming:                 true,
			aborting:                  false,
			terminal_answer_no_queued: false,
			interrupt_immune_turn:     false,
			preserve_only:             false,
		};
		assert_eq!(resolve_channel(base), Channel::Aside);
		assert_eq!(
			resolve_channel(ChannelInputs { severity: Some(Severity::Concern), ..base }),
			Channel::Steer
		);
		// DURING a headless drain every severity is preserved, blocker included.
		assert_eq!(
			resolve_channel(ChannelInputs {
				severity: Some(Severity::Blocker),
				streaming: false,
				preserve_only: true,
				..base
			}),
			Channel::Preserve
		);
		// BEFORE the drain a blocker steers a live turn.
		assert_eq!(
			resolve_channel(ChannelInputs {
				severity: Some(Severity::Blocker),
				preserve_only: false,
				..base
			}),
			Channel::Steer
		);
		assert_eq!(
			resolve_channel(ChannelInputs {
				severity: Some(Severity::Concern),
				streaming: false,
				terminal_answer_no_queued: true,
				..base
			}),
			Channel::Preserve
		);
		assert_eq!(
			resolve_channel(ChannelInputs {
				severity: Some(Severity::Concern),
				interrupt_immune_turn: true,
				..base
			}),
			Channel::Aside
		);
		assert_eq!(
			resolve_channel(ChannelInputs {
				severity: Some(Severity::Blocker),
				interrupt_immune_turn: true,
				..base
			}),
			Channel::Steer
		);
	}

	#[test]
	fn immune_window_is_half_open_on_completed_turns() {
		assert!(immune_active(3, Some(2), 3));
		assert!(!immune_active(5, Some(2), 3));
		assert!(!immune_active(3, Some(2), 0));
		assert!(!immune_active(0, None, 3));
	}
}
