//! Thinking-loop guard for streamed reasoning deltas.
//!
//! Port of `packages/ai/src/utils/thinking-loop.ts`: an exact suffix-cycle
//! detector (Z-array over the reversed tail), a word-trigram Jaccard segment
//! cluster detector, a progress-lexicon stall detector and the Gemini
//! reasoning-summary header-run counter. The semantic detectors run only for
//! guarded model families; exact repetition applies to every stream.
//!
//! A detected loop terminates the chunk stream with a retryable
//! [`ProviderError::Protocol`] whose text carries the thinking-loop marker and
//! "stream stall", so the provider-error classifier flags it
//! [`ErrorFlags::THINKING_LOOP`](crate::provider::error_flags::ErrorFlags::THINKING_LOOP)
//! and the session retries it as transient.

use std::{
	collections::{HashSet, VecDeque},
	sync::LazyLock,
};

use futures::{StreamExt, stream};
use regex::Regex;

use crate::provider::{ChunkStream, ProviderError};

/// Stable lead phrase of the guard's error message.
pub(crate) const THINKING_LOOP_ERROR_MARKER: &str = "Thinking loop detected";

/// Rolling tail retained for exact suffix-cycle detection.
const EXACT_TAIL_WINDOW: usize = 4096;
/// Longest exact cycle length considered.
const EXACT_MAX_UNIT: usize = 1024;
/// New characters between scans. Large deltas are scanned immediately.
const EXACT_CHECK_STRIDE: usize = 128;
/// Short cycles need four repeats covering at least this many characters.
const EXACT_SHORT_MAX_UNIT: usize = 60;
const EXACT_SHORT_MIN_REPEATED_CHARS: usize = 180;
/// Long cycles need at least three repeats covering at least this many chars.
const EXACT_LONG_MIN_REPEATED_CHARS: usize = 1024;

/// Char cap for an unterminated segment; forces a flush so a wall-of-text loop
/// still segments.
const SEGMENT_CHAR_CAP: usize = 700;
/// Normalized-length floor below which a segment is ignored.
const SEGMENT_MIN_NORM_CHARS: usize = 60;
/// How many recent substantial segments are kept for similarity comparison.
const SEGMENT_WINDOW: usize = 16;
/// Word-trigram Jaccard at/above which two segments count as near-duplicates.
const SEGMENT_SIMILARITY: f64 = 0.8;
/// Substantial segments required before detection may fire (warm-up).
const SEGMENT_MIN_COUNT: usize = 8;
/// Near-duplicate cluster size (current + matches) that trips the loop.
const SEGMENT_MIN_CLUSTER: usize = 4;

/// Recent segments whose pooled unigram vocabulary is the novelty baseline.
const LEX_NOVELTY_WINDOW: usize = 8;
/// Novelty at/below which a segment counts as recycling earlier wording.
const LEX_STALL_NOVELTY_FLOOR: f64 = 0.2;
/// Consecutive low-information segments that trip a progress-lexicon stall.
const LEX_STALL_MIN_RUN: usize = 8;

/// Consecutive Gemini thought-summary headers that trip the run detector.
pub(crate) const GEMINI_HEADER_RUNAWAY_THRESHOLD: usize = 24;

/// A concrete reference the model is actually reasoning about: a code span, a
/// dotted member, a multi-segment path, a snake/camel/Pascal identifier. A
/// segment that introduces a NEW one resets the lexical-stall run.
static CONCRETE_ANCHOR: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(concat!(
		r"`[^`]+`",
		r"|\b[A-Za-z0-9_]{2,}\.[A-Za-z][A-Za-z0-9_]{0,4}\b",
		r"|[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+){2,}",
		r"|\b[A-Za-z0-9_]+_[A-Za-z0-9_]+\b",
		r"|\b[a-z]+[A-Z][A-Za-z0-9_]*\b",
		r"|\b[A-Z][a-z]+[A-Z][A-Za-z0-9_]*\b",
	))
	.expect("concrete anchor pattern compiles")
});

/// True when the model id belongs to a family guarded by the semantic
/// heuristics. Exact suffix-cycle detection applies independently.
pub(crate) fn is_loop_guarded_model(model: &str) -> bool {
	let model = model.to_ascii_lowercase();
	["gemini", "deepseek", "grok"].iter().any(|family| model.contains(family))
}

/// Stateful detector fed the streamed thinking deltas.
#[derive(Debug)]
pub(crate) struct ThinkingLoopDetector {
	/// Rolling char tail for exact suffix-cycle detection.
	tail:               String,
	/// Total characters received when the exact detector last scanned.
	exact_scanned_at:   usize,
	/// Pending thinking text not yet split into completed segments.
	pending:            String,
	/// Fingerprints of the most recent substantial segments.
	window:             VecDeque<HashSet<String>>,
	/// Count of substantial segments seen so far (warm-up gate).
	count:              usize,
	/// Unigram word sets of the most recent segments.
	word_window:        VecDeque<HashSet<String>>,
	/// Consecutive low-information (low-novelty, anchor-free) segments.
	lex_stall_run:      usize,
	/// Concrete anchors seen per recent segment.
	anchor_window:      VecDeque<HashSet<String>>,
	/// Whether the semantic heuristics are enabled for this stream.
	semantic_heuristics: bool,
}

impl ThinkingLoopDetector {
	pub(crate) fn new(semantic_heuristics: bool) -> Self {
		Self {
			tail: String::new(),
			exact_scanned_at: 0,
			pending: String::new(),
			window: VecDeque::new(),
			count: 0,
			word_window: VecDeque::new(),
			lex_stall_run: 0,
			anchor_window: VecDeque::new(),
			semantic_heuristics,
		}
	}

	/// Feed a reasoning delta. Returns the first recognized loop reason.
	pub(crate) fn push(&mut self, delta: &str) -> Option<String> {
		if delta.is_empty() {
			return None;
		}

		// 1. Exact suffix cycles, scanned at a bounded cadence rather than
		// quadratically for every token-sized delta.
		let delta_chars = delta.chars().count();
		self.tail.push_str(delta);
		truncate_front_chars(&mut self.tail, EXACT_TAIL_WINDOW);
		self.exact_scanned_at += delta_chars;
		if self.exact_scanned_at >= EXACT_CHECK_STRIDE || delta_chars >= EXACT_CHECK_STRIDE {
			self.exact_scanned_at = 0;
			if let Some((unit_length, times)) = detect_exact_suffix_cycle(&self.tail) {
				return Some(exact_cycle_reason(unit_length, times));
			}
		}

		if !self.semantic_heuristics {
			return None;
		}

		// 2. Near-duplicate paragraph loop. Append, then drain completed segments.
		self.pending.push_str(delta);
		loop {
			let (raw, rest) = if let Some((start, end)) = segment_boundary(&self.pending) {
				(self.pending[..start].to_string(), self.pending[end..].to_string())
			} else if self.pending.chars().count() > SEGMENT_CHAR_CAP {
				// No boundary yet but the segment is runaway-long: force a flush.
				split_at_chars(&self.pending, SEGMENT_CHAR_CAP)
			} else {
				return None;
			};
			self.pending = rest;
			let mut remaining = raw;
			while !remaining.is_empty() {
				let (chunk, next) = split_at_chars(&remaining, SEGMENT_CHAR_CAP);
				remaining = next;
				if let Some(hit) = self.consume_segment(&chunk) {
					return Some(hit);
				}
			}
		}
	}

	/// Process the buffered trailing paragraph when the thinking block ends.
	pub(crate) fn flush(&mut self) -> Option<String> {
		if let Some((unit_length, times)) = detect_exact_suffix_cycle(&self.tail) {
			return Some(exact_cycle_reason(unit_length, times));
		}
		if !self.semantic_heuristics || self.pending.is_empty() {
			return None;
		}
		let mut remaining = std::mem::take(&mut self.pending);
		while !remaining.is_empty() {
			let (chunk, next) = split_at_chars(&remaining, SEGMENT_CHAR_CAP);
			remaining = next;
			if let Some(hit) = self.consume_segment(&chunk) {
				return Some(hit);
			}
		}
		None
	}

	fn consume_segment(&mut self, raw: &str) -> Option<String> {
		// Summary titles are per-thought formatting, not chain-of-thought;
		// stripping them keeps their ever-changing wording from inflating
		// novelty and masking a loop.
		let segment = strip_summary_titles(raw);
		let normalized = normalize_segment(&segment);
		if normalized.len() < SEGMENT_MIN_NORM_CHARS {
			return None;
		}

		// (a) Near-duplicate trigram cluster: the same paragraph reused with
		// cosmetic wording drift (high word-trigram overlap).
		let fingerprint = trigram_shingles(&normalized);
		let mut cluster = 1;
		for previous in &self.window {
			if jaccard(&fingerprint, previous) >= SEGMENT_SIMILARITY {
				cluster += 1;
			}
		}

		// (b) Progress-lexicon stall: paragraphs that recycle the recent
		// vocabulary and add no NEW concrete reference.
		let words: HashSet<String> = normalized.split(' ').map(str::to_string).collect();
		let mut prior_vocab: HashSet<&str> = HashSet::new();
		for set in &self.word_window {
			prior_vocab.extend(set.iter().map(String::as_str));
		}
		let unseen = words.iter().filter(|word| !prior_vocab.contains(word.as_str())).count();
		let novelty = if prior_vocab.is_empty() {
			1.0
		} else {
			unseen as f64 / words.len() as f64
		};

		let anchors: HashSet<String> = CONCRETE_ANCHOR
			.find_iter(&segment)
			.map(|matched| matched.as_str().replace('`', "").to_lowercase())
			.collect();
		let new_anchor = anchors
			.iter()
			.any(|anchor| self.anchor_window.iter().all(|seen| !seen.contains(anchor)));

		if novelty <= LEX_STALL_NOVELTY_FLOOR && !new_anchor {
			self.lex_stall_run += 1;
		} else {
			self.lex_stall_run = 0;
		}

		self.window.push_back(fingerprint);
		if self.window.len() > SEGMENT_WINDOW {
			self.window.pop_front();
		}
		self.word_window.push_back(words);
		if self.word_window.len() > LEX_NOVELTY_WINDOW {
			self.word_window.pop_front();
		}
		self.anchor_window.push_back(anchors);
		if self.anchor_window.len() > LEX_NOVELTY_WINDOW {
			self.anchor_window.pop_front();
		}
		self.count += 1;

		if self.count >= SEGMENT_MIN_COUNT {
			if cluster >= SEGMENT_MIN_CLUSTER {
				return Some(format!(
					"{cluster} near-identical segments within the last {SEGMENT_WINDOW}"
				));
			}
			if self.lex_stall_run >= LEX_STALL_MIN_RUN {
				let run = self.lex_stall_run;
				return Some(format!("{run} low-information segments recycling recent wording"));
			}
		}
		None
	}
}

/// Counts consecutive Gemini reasoning-summary headers across a streamed
/// thinking block; fires once per run at the threshold.
#[derive(Debug, Default)]
pub(crate) struct GeminiHeaderRunDetector {
	/// Thinking text not yet split into completed lines.
	pending: String,
	/// Summary-title lines seen in the current run.
	count:   usize,
	/// Latches after the first threshold hit so each run fires at most once.
	fired:   bool,
}

impl GeminiHeaderRunDetector {
	/// Feed a thinking delta. Returns `true` the first time the run hits the
	/// threshold.
	pub(crate) fn push(&mut self, delta: &str) -> bool {
		if self.fired || delta.is_empty() {
			return false;
		}
		self.pending.push_str(delta);
		while let Some(newline) = self.pending.find('\n') {
			let line = self.pending[..newline].trim().to_string();
			self.pending.drain(..=newline);
			if !line.is_empty() && is_reasoning_summary_header(&line) {
				self.count += 1;
				if self.count >= GEMINI_HEADER_RUNAWAY_THRESHOLD {
					self.fired = true;
					return true;
				}
			}
		}
		false
	}

	/// Re-arm for a fresh reasoning block.
	pub(crate) fn reset(&mut self) {
		self.pending.clear();
		self.count = 0;
		self.fired = false;
	}
}

/// True when a trimmed line is a Gemini reasoning-summary title: a markdown ATX
/// heading or a whole-line bold / bold-italic run.
pub(crate) fn is_reasoning_summary_header(line: &str) -> bool {
	let line = line.trim();
	let hashes = line.bytes().take_while(|byte| *byte == b'#').count();
	if (1..=6).contains(&hashes) {
		let rest = &line[hashes..];
		if rest.starts_with(' ') || rest.starts_with('\t') {
			if !rest.trim_start_matches(|c: char| c == ' ' || c == '\t').is_empty() {
				return true;
			}
		}
	}
	let leading_stars = line.bytes().take_while(|byte| *byte == b'*').count();
	if (2..=3).contains(&leading_stars) {
		let trailing_stars = line.bytes().rev().take_while(|byte| *byte == b'*').count();
		let middle = line.len().saturating_sub(leading_stars + trailing_stars);
		return (2..=3).contains(&trailing_stars) && middle > 0;
	}
	false
}

/// Wrap an OpenAI-compatible chunk stream with the loop guard.
pub(crate) fn guard_chunk_stream(inner: ChunkStream, model: &str) -> ChunkStream {
	struct GuardState {
		stream:         ChunkStream,
		thinking:       ThinkingLoopDetector,
		headers:        GeminiHeaderRunDetector,
		check_headers:  bool,
		thinking_armed: bool,
	}

	let state = GuardState {
		stream: inner,
		thinking: ThinkingLoopDetector::new(is_loop_guarded_model(model)),
		headers: GeminiHeaderRunDetector::default(),
		check_headers: model.to_ascii_lowercase().contains("gemini"),
		thinking_armed: true,
	};

	stream::unfold(Some(state), |state| async move {
		let mut state = state?;
		let item = state.stream.next().await?;
		match item {
			Err(error) => return Some((Err(error), None)),
			Ok(chunk) => {
				let mut detail = None;
				if let Some(delta) = chunk.reasoning.as_deref().filter(|delta| !delta.is_empty())
					&& state.thinking_armed
				{
					detail = state.thinking.push(delta);
					if detail.is_none() && state.check_headers && state.headers.push(delta) {
						detail = Some(format!(
							"{GEMINI_HEADER_RUNAWAY_THRESHOLD} consecutive reasoning-summary headers without progress"
						));
					}
				}
				if chunk.content.as_deref().is_some_and(|delta| !delta.is_empty()) {
					// Visible answer text latches semantic detection off and
					// leaves the reasoning channel for the header counter.
					state.thinking_armed = false;
					state.headers.reset();
				}
				if !chunk.tool_call_deltas.is_empty() {
					// A tool call closes the Gemini reasoning-summary run, but
					// Grok/xAI Responses can keep reasoning afterwards, so the
					// thinking detector stays armed (TS guard parity).
					state.headers.reset();
				}
				if detail.is_none() && chunk.is_done && state.thinking_armed {
					detail = state.thinking.flush();
				}
				if let Some(detail) = detail {
					return Some((
						Err(ProviderError::Protocol(thinking_loop_error_message(&detail))),
						None,
					));
				}
				Some((Ok(chunk), Some(state)))
			},
		}
	})
	.boxed()
}

/// Retryable error text: the marker makes the classifier set
/// [`ErrorFlags::THINKING_LOOP`](crate::provider::error_flags::ErrorFlags::THINKING_LOOP)
/// and "stream stall" keeps the turn transient so the session re-samples it.
pub fn thinking_loop_error_message(detail: &str) -> String {
	format!(
		"{THINKING_LOOP_ERROR_MARKER}: the model repeated near-identical content ({detail}). \
		 Treating as a stream stall and retrying."
	)
}

fn exact_cycle_reason(unit_length: usize, times: usize) -> String {
	format!("repeated an exact {unit_length}-character cycle {times}× back-to-back")
}

/// Detect an exact cycle at the text suffix. A Z-array over the reversed tail
/// finds every suffix period in linear time without substring churn.
fn detect_exact_suffix_cycle(text: &str) -> Option<(usize, usize)> {
	let chars: Vec<char> = text.chars().collect();
	if chars.len() < EXACT_SHORT_MIN_REPEATED_CHARS {
		return None;
	}
	let reversed: Vec<char> = chars.iter().rev().copied().collect();
	let mut z = vec![0usize; reversed.len()];
	let (mut left, mut right) = (0usize, 0usize);
	for i in 1..reversed.len() {
		if i <= right {
			z[i] = z[i - left].min(right - i + 1);
		}
		while i + z[i] < reversed.len() && reversed[z[i]] == reversed[i + z[i]] {
			z[i] += 1;
		}
		if i + z[i] > 0 && i + z[i] - 1 > right {
			left = i;
			right = i + z[i] - 1;
		}
	}

	let max_unit = EXACT_MAX_UNIT.min(reversed.len() / 3);
	for unit_length in 2..=max_unit {
		let count = 1 + z[unit_length] / unit_length;
		let (min_count, min_chars) = if unit_length <= EXACT_SHORT_MAX_UNIT {
			(4, EXACT_SHORT_MIN_REPEATED_CHARS)
		} else {
			(3, EXACT_LONG_MIN_REPEATED_CHARS)
		};
		if count < min_count || unit_length * count < min_chars {
			continue;
		}
		let unit = &chars[chars.len() - unit_length..];
		if unit.iter().any(|c| c.is_alphabetic() || is_pictographic(*c)) {
			return Some((unit_length, count));
		}
	}
	None
}

/// Lowercase and tokenize prose plus code/path payloads, dropping pure numbers.
fn normalize_segment(segment: &str) -> String {
	segment
		.to_ascii_lowercase()
		.split(|c: char| !c.is_ascii_alphanumeric())
		.filter(|token| !token.is_empty() && token.bytes().any(|byte| byte.is_ascii_lowercase()))
		.collect::<Vec<_>>()
		.join(" ")
}

/// Word-trigram shingle set of a normalized segment.
fn trigram_shingles(normalized: &str) -> HashSet<String> {
	let words: Vec<&str> = normalized.split(' ').filter(|word| !word.is_empty()).collect();
	if words.len() < 3 {
		return if words.is_empty() {
			HashSet::new()
		} else {
			HashSet::from([words.join(" ")])
		};
	}
	words.windows(3).map(|window| window.join(" ")).collect()
}

fn jaccard(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
	if a.is_empty() || b.is_empty() {
		return 0.0;
	}
	let intersection = a.iter().filter(|item| b.contains(*item)).count();
	let union = a.len() + b.len() - intersection;
	if union == 0 {
		0.0
	} else {
		intersection as f64 / union as f64
	}
}

/// Strip reasoning-summary title lines before similarity analysis.
fn strip_summary_titles(segment: &str) -> String {
	let mut stripped = String::with_capacity(segment.len());
	for (index, line) in segment.split('\n').enumerate() {
		if index > 0 {
			stripped.push('\n');
		}
		if !is_reasoning_summary_header(line) {
			stripped.push_str(line);
		}
	}
	stripped
}

/// First `\n\s*\n` boundary as `(start, end)` byte offsets.
fn segment_boundary(pending: &str) -> Option<(usize, usize)> {
	let bytes = pending.as_bytes();
	for (index, byte) in bytes.iter().enumerate() {
		if *byte != b'\n' {
			continue;
		}
		let mut cursor = index + 1;
		let mut last_newline = None;
		while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
			if bytes[cursor] == b'\n' {
				last_newline = Some(cursor);
			}
			cursor += 1;
		}
		if let Some(end) = last_newline {
			return Some((index, end + 1));
		}
	}
	None
}

/// Split at the first `max_chars` characters, returning `(head, tail)`.
fn split_at_chars(text: &str, max_chars: usize) -> (String, String) {
	match text.char_indices().nth(max_chars) {
		Some((index, _)) => (text[..index].to_string(), text[index..].to_string()),
		None => (text.to_string(), String::new()),
	}
}

fn truncate_front_chars(text: &mut String, max_chars: usize) {
	let length = text.chars().count();
	if length <= max_chars {
		return;
	}
	let skip = length - max_chars;
	if let Some((index, _)) = text.char_indices().nth(skip) {
		text.drain(..index);
	}
}

const fn is_pictographic(c: char) -> bool {
	matches!(
		c,
		'\u{1F000}'..='\u{1F2FF}' | '\u{1F300}'..='\u{1FAFF}' | '\u{2600}'..='\u{27BF}'
	)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::provider::StreamChunk;

	fn feed(text: &str, step: usize, semantic: bool) -> Option<String> {
		let mut detector = ThinkingLoopDetector::new(semantic);
		let chars: Vec<char> = text.chars().collect();
		let mut index = 0;
		while index < chars.len() {
			let end = (index + step).min(chars.len());
			let delta: String = chars[index..end].iter().collect();
			if let Some(detail) = detector.push(&delta) {
				return Some(detail);
			}
			index = end;
		}
		detector.flush()
	}

	fn near_duplicate_loop(paragraphs: usize) -> String {
		let variants = [
			"I am now verifying the test module to guarantee there are no compile errors and the \
			 code is completely safe.",
			"I am now verifying the test module once more to ensure there are no compile errors and \
			 the code stays completely safe.",
			"I am now re-verifying the test module to confirm there are no compile errors and the \
			 code remains completely safe.",
		];
		let mut out = Vec::with_capacity(paragraphs);
		for index in 0..paragraphs {
			out.push(format!("**Confirming Safety {index}**\n\n{}", variants[index % 3]));
		}
		out.join("\n\n\n")
	}

	fn tight_duplicate_loop(paragraphs: usize) -> String {
		(0..paragraphs)
			.map(|index| {
				format!(
					"Confirming the change is safe and the whole suite is green; pass number {index} \
					 of the final review sweep."
				)
			})
			.collect::<Vec<_>>()
			.join("\n\n\n")
	}

	fn distinct_reasoning() -> String {
		[
			"First I read the agent loop to understand how the stream wrapper commits the final \
			 assistant message.",
			"Next I traced the retry classifier to see which error shapes are treated as transient \
			 by the session.",
			"The Vertex transport needs ADC, so its credential path differs from the OpenRouter \
			 completions route entirely.",
			"I should add a regression covering the empty-content terminal so the auto-retry gate \
			 stays satisfied later.",
			"The tokenizer counts code points, which matters for the wide-emoji case in the \
			 truncation helper above here.",
			"Compaction journals are per-request, so they never persist into the on-disk session \
			 transcript at all ever.",
			"The mock provider emits one delta per block, so streaming nuances need a direct \
			 detector unit test instead.",
			"Finally I will run the focused package suite and the type checker before touching the \
			 changelog entries now.",
			"Telemetry spans wrap each provider call, so failing one must still close its span in \
			 the catch branch too.",
			"A device default of CPU keeps the tiny model worker from crashing Bun on teardown \
			 across every platform now.",
		]
		.join("\n\n\n")
	}

	#[test]
	fn trips_on_verbatim_back_to_back_repetition() {
		let mut detector = ThinkingLoopDetector::new(true);
		let detail = detector.push(&"🌊 ".repeat(120)).expect("cycle");
		assert!(detail.contains("back-to-back"), "{detail}");
	}

	#[test]
	fn does_not_trip_on_legitimate_repetitive_numeric_output() {
		let mut detector = ThinkingLoopDetector::new(true);
		assert!(detector.push(&"00 ".repeat(200)).is_none());
	}

	#[test]
	fn does_not_trip_on_short_requested_repetitive_text() {
		let mut detector = ThinkingLoopDetector::new(true);
		assert!(detector.push(&"🌊 ".repeat(26)).is_none());
	}

	#[test]
	fn exact_cycle_applies_even_without_semantic_heuristics() {
		let text = "the quick brown fox jumps. ".repeat(10);
		let detail = feed(&text, 17, false).expect("cycle");
		assert!(detail.contains("back-to-back"), "{detail}");
	}

	#[test]
	fn trips_on_near_duplicate_paragraph_cluster() {
		let detail = feed(&tight_duplicate_loop(12), 17, true).expect("loop");
		assert!(detail.contains("near-identical segments"), "{detail}");
	}

	#[test]
	fn flush_catches_a_final_unterminated_duplicate_paragraph() {
		let mut detector = ThinkingLoopDetector::new(true);
		let block = format!(
			"{}\n\n\nI am now verifying the test module to guarantee there are no compile errors \
			 and the code is completely safe.",
			near_duplicate_loop(7)
		);
		assert!(detector.push(&block).is_none());
		let detail = detector.flush().expect("loop");
		assert!(detail.contains("near-identical segments"), "{detail}");
	}

	#[test]
	fn does_not_trip_on_genuinely_distinct_reasoning() {
		assert!(feed(&distinct_reasoning(), 23, true).is_none());
	}

	#[test]
	fn reasoning_summary_header_shapes() {
		assert!(is_reasoning_summary_header("## Examining Result Handling"));
		assert!(is_reasoning_summary_header("**Maintaining Momentum**"));
		assert!(is_reasoning_summary_header("***Bold Italic Title***"));
		assert!(!is_reasoning_summary_header("inline **bold** prose"));
		assert!(!is_reasoning_summary_header("plain paragraph"));
	}

	#[test]
	fn gemini_header_run_fires_at_threshold_once_and_resets() {
		let mut detector = GeminiHeaderRunDetector::default();
		let mut fired_at = None;
		for index in 1..=GEMINI_HEADER_RUNAWAY_THRESHOLD {
			let delta = format!("## Thought {index}\nsome paragraph text\n");
			if detector.push(&delta) {
				fired_at = Some(index);
			}
		}
		assert_eq!(fired_at, Some(GEMINI_HEADER_RUNAWAY_THRESHOLD));
		assert!(!detector.push("## Another\n"));
		detector.reset();
		assert!(!detector.push("## After reset\n"));
	}

	#[test]
	fn gemini_paragraph_lines_do_not_reset_the_run() {
		let mut detector = GeminiHeaderRunDetector::default();
		let mut fired = false;
		for index in 1..GEMINI_HEADER_RUNAWAY_THRESHOLD {
			fired |= detector.push(&format!("## Thought {index}\nbody\n\n"));
		}
		assert!(!fired);
		assert!(detector.push("## Final Thought\n"));
	}

	#[tokio::test]
	async fn guard_terminates_a_guarded_stream_with_retryable_loop_error() {
		let text = near_duplicate_loop(12);
		let chunks: Vec<Result<StreamChunk, ProviderError>> = text
			.chars()
			.collect::<Vec<_>>()
			.chunks(40)
			.map(|chunk| {
				Ok(StreamChunk {
					reasoning: Some(chunk.iter().collect()),
					content: None,
					tool_call_deltas: Vec::new(),
					usage: None,
					is_done: false,
					content_index: None,
					signature: None,
					redacted: None,
					stop_reason: None,
					error_message: None,
					disabled_features: None,
				})
			})
			.collect();
		let mut guarded = guard_chunk_stream(stream::iter(chunks).boxed(), "google/gemini-3.5-flash");
		let error = loop {
			match guarded.next().await {
				Some(Err(error)) => break error,
				Some(Ok(_)) => {},
				None => panic!("guard ended without an error"),
			}
		};
		let text = error.to_string();
		assert!(text.contains(THINKING_LOOP_ERROR_MARKER), "{text}");
		assert!(text.contains("stream stall"), "{text}");
		let id = crate::provider::error_flags::classify_message(&text);
		assert!(
			crate::provider::error_flags::is(
				id,
				crate::provider::error_flags::ErrorFlags::THINKING_LOOP,
			),
			"{id:?}"
		);
		assert!(crate::provider::error_flags::retriable(id, false), "{id:?}");
		assert!(guarded.next().await.is_none());
	}
}
