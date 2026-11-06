//! Git merge-conflict detection and resolution, ported from
//! `packages/coding-agent/src/tools/conflict-detect.ts`.
//!
//! `read` scans the lines it already returned for well-formed column-0
//! marker blocks and registers each completed block in the session's
//! [`ConflictHistory`]. `write({ path: "conflict://<N>" })` then splices the
//! recorded region by content, so out-of-band edits that shift line numbers do
//! not break resolution, while an altered marker block is refused outright.
//!
//! The history is in-memory and session-scoped. Nothing here persists, and
//! nothing is process-global: a child agent gets its own history with its own
//! ids.

use std::{
	collections::BTreeMap,
	path::{Path, PathBuf},
	sync::{Arc, Mutex},
};

use tokio::io::AsyncReadExt;

const OURS_PREFIX: &str = "<<<<<<<";
const BASE_PREFIX: &str = "|||||||";
const SEPARATOR: &str = "=======";
const THEIRS_PREFIX: &str = ">>>>>>>";

/// Byte cap for a whole-file conflict scan (`read <file>:conflicts` and the
/// window footer's total count). Matches upstream `SCAN_FILE_DEFAULT_MAX_BYTES`.
pub const SCAN_FILE_DEFAULT_MAX_BYTES: usize = 10 * 1024 * 1024;

/// Replacement lines dropped because they duplicated adjacent context.
const MAX_ECHO_LINES: usize = 12;

/// One completed conflict block. Lines are LF-normalized; a trailing `\r` from
/// a CRLF checkout is stripped during scanning and re-applied on write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictBlock {
	/// 1-indexed line of the `<<<<<<<` marker.
	pub start_line:     u64,
	/// 1-indexed line of the `=======` separator.
	pub separator_line: u64,
	/// 1-indexed line of the `>>>>>>>` marker.
	pub end_line:       u64,
	/// 1-indexed line of the `|||||||` marker (diff3 only).
	pub base_line:      Option<u64>,
	pub ours_label:     Option<String>,
	pub base_label:     Option<String>,
	pub theirs_label:   Option<String>,
	pub ours_lines:     Vec<String>,
	pub base_lines:     Option<Vec<String>>,
	pub theirs_lines:   Vec<String>,
}

/// A block recorded under a session-stable id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictEntry {
	pub id:            u64,
	pub absolute_path: PathBuf,
	pub display_path:  String,
	pub block:         ConflictBlock,
	/// Whether the source that surfaced this block grants write authority.
	/// A `local://` artifact read has a physical `absolute_path` for snapshots
	/// but its handler is read-only, so `conflict://` must not become a write
	/// capability the source URL never had.
	pub writable:      bool,
}

impl ConflictEntry {
	pub fn is_three_way(&self) -> bool {
		self.block.base_lines.is_some()
	}
}

#[derive(Default)]
struct HistoryInner {
	next_id: u64,
	entries: BTreeMap<u64, ConflictEntry>,
}

/// Per-session log of conflict regions surfaced by `read`.
///
/// Ids are append-only: resolving one block never renumbers the others, so
/// several writes can be issued in one turn. Re-registering the same
/// `absolutePath` + `startLine` reuses the earlier id and overwrites the
/// recorded region, so a re-read reflects current marker positions.
pub struct ConflictHistory {
	inner: Mutex<HistoryInner>,
}

impl Default for ConflictHistory {
	fn default() -> Self {
		Self::new()
	}
}

impl std::fmt::Debug for ConflictHistory {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("ConflictHistory")
			.field("entries", &self.entries().len())
			.finish()
	}
}

impl ConflictHistory {
	pub fn new() -> Self {
		Self {
			inner: Mutex::new(HistoryInner { next_id: 1, entries: BTreeMap::new() }),
		}
	}

	/// Register a block, reusing the id of an entry recorded at the same
	/// absolute path and start line. `writable` carries the source's mutation
	/// capability; a physical path alone never confers it.
	pub fn register(
		&self,
		absolute_path: &Path,
		display_path: impl Into<String>,
		block: ConflictBlock,
		writable: bool,
	) -> ConflictEntry {
		let mut inner = self.inner.lock().unwrap_or_else(|poison| poison.into_inner());
		let existing = inner
			.entries
			.values()
			.find(|entry| {
				entry.absolute_path == absolute_path && entry.block.start_line == block.start_line
			})
			.map(|entry| entry.id);
		let id = match existing {
			Some(id) => id,
			None => {
				let id = inner.next_id;
				inner.next_id += 1;
				id
			},
		};
		let entry = ConflictEntry {
			id,
			absolute_path: absolute_path.to_path_buf(),
			display_path: display_path.into(),
			block,
			writable,
		};
		inner.entries.insert(id, entry.clone());
		entry
	}

	pub fn get(&self, id: u64) -> Option<ConflictEntry> {
		self
			.inner
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.entries
			.get(&id)
			.cloned()
	}

	/// Every registered entry in id order.
	pub fn entries(&self) -> Vec<ConflictEntry> {
		self
			.inner
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.entries
			.values()
			.cloned()
			.collect()
	}

	pub fn len(&self) -> usize {
		self
			.inner
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.entries
			.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn invalidate(&self, id: u64) {
		self
			.inner
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.entries
			.remove(&id);
	}

	pub fn invalidate_path(&self, absolute_path: &Path) {
		let mut inner = self.inner.lock().unwrap_or_else(|poison| poison.into_inner());
		inner
			.entries
			.retain(|_, entry| entry.absolute_path != absolute_path);
	}
}

/// A side of a conflict block renderable via `conflict://N/<scope>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictScope {
	Ours,
	Theirs,
	Base,
}

impl ConflictScope {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Ours => "ours",
			Self::Theirs => "theirs",
			Self::Base => "base",
		}
	}

	fn parse(raw: &str) -> Option<Self> {
		match raw {
			"ours" => Some(Self::Ours),
			"theirs" => Some(Self::Theirs),
			"base" => Some(Self::Base),
			_ => None,
		}
	}
}

/// `conflict://*` selects every currently-registered block (bulk write only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictId {
	Id(u64),
	Wildcard,
}

/// Parsed `conflict://<N>`, `conflict://<N>/<scope>`, or `conflict://*` URI.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedConflictUri {
	pub id:               ConflictId,
	pub scope:            Option<ConflictScope>,
	/// Stripped `<file-prefix>:` for malformed `<file>:conflict://…` input, so
	/// callers can add a gentle "you don't need the file path" note.
	pub recovered_prefix: Option<String>,
}

/// Parse a conflict URI.
///
/// Returns `Ok(None)` for non-conflict paths and `Err` for a well-formed
/// scheme with an invalid id or scope, so the agent gets an actionable message
/// instead of a confusing "not found" later.
pub fn parse_conflict_uri(raw: &str) -> Result<Option<ParsedConflictUri>, String> {
	const SCHEME: &str = "conflict://";
	let (tail, recovered_prefix) = if let Some(tail) = raw.strip_prefix(SCHEME) {
		(tail, None)
	} else if let Some(pos) = raw.rfind(&format!(":{SCHEME}")) {
		if pos == 0 {
			return Ok(None);
		}
		(&raw[pos + 1 + SCHEME.len()..], Some(raw[..pos].to_string()))
	} else {
		return Ok(None);
	};
	if tail.is_empty() {
		return Ok(None);
	}

	let (id_part, scope_part) = match tail.split_once('/') {
		Some((id, scope)) => (id, Some(scope)),
		None => (tail, None),
	};

	let id = if id_part == "*" {
		if let Some(scope) = scope_part {
			return Err(format!(
				"Invalid conflict URI '{raw}': wildcard 'conflict://*' does not accept a scope segment. Drop '/{scope}' or use a numeric id."
			));
		}
		ConflictId::Wildcard
	} else {
		if id_part.is_empty() || !id_part.bytes().all(|byte| byte.is_ascii_digit()) {
			return Err(format!(
				"Invalid conflict URI '{raw}': must be 'conflict://<N>', 'conflict://<N>/<scope>', or 'conflict://*' where N is a positive integer surfaced by a prior `read`."
			));
		}
		let id = id_part.parse::<u64>().map_err(|_| {
			format!(
				"Invalid conflict URI '{raw}': must be 'conflict://<N>', 'conflict://<N>/<scope>', or 'conflict://*' where N is a positive integer surfaced by a prior `read`."
			)
		})?;
		if id < 1 {
			return Err(format!("Invalid conflict URI '{raw}': id must be ≥ 1."));
		}
		ConflictId::Id(id)
	};

	let scope = match scope_part {
		None => None,
		Some(scope) => Some(ConflictScope::parse(scope).ok_or_else(|| {
			let id = match id {
				ConflictId::Id(id) => id.to_string(),
				ConflictId::Wildcard => "*".to_string(),
			};
			format!(
				"Invalid conflict URI '{raw}': scope must be one of 'ours', 'theirs', 'base', or omitted (e.g. 'conflict://{id}/theirs')."
			)
		})?),
	};

	Ok(Some(ParsedConflictUri { id, scope, recovered_prefix }))
}

/// Scan already-collected lines for completed conflict blocks.
///
/// `first_line_number` is the 1-indexed number of `lines[0]`. Only blocks whose
/// opener, separator, and closer are all inside the window are returned; a
/// block whose closer is past the tail is dropped.
pub fn scan_conflict_lines(lines: &[String], first_line_number: u64) -> Vec<ConflictBlock> {
	#[derive(PartialEq)]
	enum Phase {
		Idle,
		Ours,
		Base,
		Theirs,
	}

	struct Partial {
		start_line:     u64,
		ours_label:     Option<String>,
		ours_lines:     Vec<String>,
		base_line:      Option<u64>,
		base_label:     Option<String>,
		base_lines:     Option<Vec<String>>,
		separator_line: Option<u64>,
		theirs_lines:   Option<Vec<String>>,
	}

	let mut blocks = Vec::new();
	let mut phase = Phase::Idle;
	let mut partial: Option<Partial> = None;

	for (index, raw_line) in lines.iter().enumerate() {
		let line = strip_trailing_cr(raw_line);
		let line_number = first_line_number + index as u64;

		if let Some(label) = match_marker(line, OURS_PREFIX) {
			partial = Some(Partial {
				start_line: line_number,
				ours_label: non_empty(label),
				ours_lines: Vec::new(),
				base_line: None,
				base_label: None,
				base_lines: None,
				separator_line: None,
				theirs_lines: None,
			});
			phase = Phase::Ours;
			continue;
		}

		let Some(state) = partial.as_mut() else { continue };
		if phase == Phase::Idle {
			continue;
		}

		if let Some(label) = match_marker(line, BASE_PREFIX) {
			if phase != Phase::Ours {
				partial = None;
				phase = Phase::Idle;
				continue;
			}
			state.base_line = Some(line_number);
			state.base_label = non_empty(label);
			state.base_lines = Some(Vec::new());
			phase = Phase::Base;
			continue;
		}

		if line == SEPARATOR {
			if phase == Phase::Ours || phase == Phase::Base {
				state.separator_line = Some(line_number);
				state.theirs_lines = Some(Vec::new());
				phase = Phase::Theirs;
			} else {
				partial = None;
				phase = Phase::Idle;
			}
			continue;
		}

		if let Some(label) = match_marker(line, THEIRS_PREFIX) {
			let complete = phase == Phase::Theirs && state.separator_line.is_some() && state.theirs_lines.is_some();
			if complete {
				blocks.push(ConflictBlock {
					start_line: state.start_line,
					separator_line: state.separator_line.expect("checked"),
					end_line: line_number,
					base_line: state.base_line,
					ours_label: state.ours_label.clone(),
					base_label: state.base_label.clone(),
					theirs_label: non_empty(label),
					ours_lines: std::mem::take(&mut state.ours_lines),
					base_lines: state.base_lines.take(),
					theirs_lines: state.theirs_lines.take().unwrap_or_default(),
				});
			}
			partial = None;
			phase = Phase::Idle;
			continue;
		}

		match phase {
			Phase::Ours => state.ours_lines.push(line.to_string()),
			Phase::Base => {
				if let Some(base_lines) = state.base_lines.as_mut() {
					base_lines.push(line.to_string());
				}
			},
			Phase::Theirs => {
				if let Some(theirs_lines) = state.theirs_lines.as_mut() {
					theirs_lines.push(line.to_string());
				}
			},
			Phase::Idle => {},
		}
	}

	blocks
}

/// Scan a whole file for unresolved blocks, capped at `max_bytes`.
///
/// Returns the complete blocks found in the scanned prefix plus whether the
/// byte cap truncated the file. A partial trailing marker never invents a
/// block: the scanner only emits fully-closed ones.
pub async fn scan_file_for_conflicts(
	absolute_path: &Path,
	max_bytes: usize,
) -> std::io::Result<(Vec<ConflictBlock>, bool)> {
	let mut file = tokio::fs::File::open(absolute_path).await?;
	let len = file.metadata().await?.len();
	let truncated = len > max_bytes as u64;
	let mut buffer = Vec::with_capacity(len.min(max_bytes as u64) as usize);
	file.take(max_bytes as u64).read_to_end(&mut buffer).await?;
	let text = String::from_utf8_lossy(&buffer);
	let lines: Vec<String> = text.split('\n').map(str::to_string).collect();
	Ok((scan_conflict_lines(&lines, 1), truncated))
}

/// Strict marker match: prefix alone, or prefix + one space + label.
fn match_marker(line: &str, prefix: &str) -> Option<String> {
	let rest = line.strip_prefix(prefix)?;
	if rest.is_empty() {
		return Some(String::new());
	}
	rest.strip_prefix(' ').map(str::to_string)
}

fn non_empty(label: String) -> Option<String> {
	if label.is_empty() { None } else { Some(label) }
}

fn strip_trailing_cr(line: &str) -> &str {
	line.strip_suffix('\r').unwrap_or(line)
}

/// Result of [`splice_conflict`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictSplice {
	pub text:             String,
	/// Replacement lines dropped because they duplicated context above.
	pub trimmed_leading:  usize,
	/// Replacement lines dropped because they duplicated context below.
	pub trimmed_trailing: usize,
}

/// Reconstruct the recorded marker block as it should appear in the file.
pub fn build_recorded_region(block: &ConflictBlock) -> Vec<String> {
	let mut out = Vec::new();
	out.push(marker_line(OURS_PREFIX, block.ours_label.as_deref()));
	out.extend(block.ours_lines.iter().cloned());
	if let Some(base_lines) = &block.base_lines {
		out.push(marker_line(BASE_PREFIX, block.base_label.as_deref()));
		out.extend(base_lines.iter().cloned());
	}
	out.push(SEPARATOR.to_string());
	out.extend(block.theirs_lines.iter().cloned());
	out.push(marker_line(THEIRS_PREFIX, block.theirs_label.as_deref()));
	out
}

fn marker_line(prefix: &str, label: Option<&str>) -> String {
	match label.filter(|label| !label.is_empty()) {
		Some(label) => format!("{prefix} {label}"),
		None => prefix.to_string(),
	}
}

/// Splice the recorded marker block out of `original` and replace it with
/// `replacement` (markers and all sides included, already token-expanded).
///
/// The block is located by content, anchored to the recorded start line, so
/// earlier out-of-band edits that shift line numbers still resolve. A changed
/// or removed block is a hard error: the caller tells the agent to re-read.
pub fn splice_conflict(
	original: &str,
	entry: &ConflictEntry,
	replacement: &str,
) -> Result<ConflictSplice, String> {
	let lines: Vec<&str> = original.split('\n').collect();
	let expected = build_recorded_region(&entry.block);
	let preferred = entry.block.start_line.saturating_sub(1) as usize;
	let Some((start_idx, end_idx)) = locate_region(&lines, &expected, preferred) else {
		return Err(format!(
			"Conflict #{} no longer present in '{}': the recorded marker block can't be located. The file changed since the conflict was registered — re-read it to re-register conflicts.",
			entry.id, entry.display_path
		));
	};

	let trimmed = normalize_trailing_newline(replacement);
	let mut replacement_lines: Vec<String> = trimmed
		.split('\n')
		.map(|line| strip_trailing_cr(line).to_string())
		.collect();
	let echo = trim_boundary_echo(&mut replacement_lines, &lines, start_idx, end_idx, &entry.block);

	if lines[start_idx].ends_with('\r') {
		let has_following_line = end_idx + 1 < lines.len();
		let last = replacement_lines.len().saturating_sub(1);
		for (index, line) in replacement_lines.iter_mut().enumerate() {
			if index < last || has_following_line {
				line.push('\r');
			}
		}
	}

	let mut next: Vec<&str> = Vec::with_capacity(lines.len() + replacement_lines.len());
	next.extend_from_slice(&lines[..start_idx]);
	next.extend(replacement_lines.iter().map(String::as_str));
	next.extend_from_slice(&lines[end_idx + 1..]);
	Ok(ConflictSplice {
		text: next.join("\n"),
		trimmed_leading: echo.leading,
		trimmed_trailing: echo.trailing,
	})
}

fn normalize_trailing_newline(replacement: &str) -> &str {
	if let Some(stripped) = replacement.strip_suffix("\r\n") {
		return stripped;
	}
	replacement.strip_suffix('\n').unwrap_or(replacement)
}

/// Net `{}`/`()`/`[]` count. String/comment-blind; only corroborates a
/// single-line echo trim, never decides one alone.
fn delimiter_balance(lines: &[String]) -> i64 {
	let mut balance = 0i64;
	for line in lines {
		for ch in line.chars() {
			match ch {
				'{' | '(' | '[' => balance += 1,
				'}' | ')' | ']' => balance -= 1,
				_ => {},
			}
		}
	}
	balance
}

fn trim_boundary_echo(
	lines: &mut Vec<String>,
	file_lines: &[&str],
	start_idx: usize,
	end_idx: usize,
	block: &ConflictBlock,
) -> EchoTrim {
	let ours_balance = delimiter_balance(&block.ours_lines);
	let expected = if ours_balance == delimiter_balance(&block.theirs_lines) {
		Some(ours_balance)
	} else {
		None
	};
	let justified = |lines: &[String], without: &[String]| {
		expected.is_some()
			&& delimiter_balance(lines) != expected.unwrap_or(0)
			&& delimiter_balance(without) == expected.unwrap_or(0)
	};

	let mut trailing = 0;
	let after: Vec<String> = file_lines
		.iter()
		.skip(end_idx + 1)
		.take(MAX_ECHO_LINES)
		.map(|line| strip_trailing_cr(line).to_string())
		.collect();
	'after: for k in (1..=after.len().min(lines.len().saturating_sub(1))).rev() {
		let tail = &lines[lines.len() - k..];
		if tail.iter().zip(after.iter()).any(|(left, right)| left != right) {
			continue;
		}
		if k >= 2 {
			trailing = k;
			lines.truncate(lines.len() - k);
		} else {
			let without = lines[..lines.len() - 1].to_vec();
			if justified(lines, &without) {
				trailing = k;
				lines.truncate(lines.len() - k);
			}
		}
		break 'after;
	}

	let mut leading = 0;
	let before: Vec<String> = file_lines[..start_idx]
		.iter()
		.rev()
		.take(MAX_ECHO_LINES)
		.rev()
		.map(|line| strip_trailing_cr(line).to_string())
		.collect();
	'before: for k in (1..=before.len().min(lines.len().saturating_sub(1))).rev() {
		let head = &lines[..k];
		let before_tail = &before[before.len() - k..];
		if head.iter().zip(before_tail.iter()).any(|(left, right)| left != right) {
			continue;
		}
		if k >= 2 {
			leading = k;
			lines.drain(..k);
		} else {
			let without = lines[1..].to_vec();
			if justified(lines, &without) {
				leading = k;
				lines.drain(..k);
			}
		}
		break 'before;
	}

	EchoTrim { leading, trailing }
}

struct EchoTrim {
	leading:  usize,
	trailing: usize,
}

/// Find `expected` in `lines`, preferring the occurrence closest to
/// `preferred_idx` when an identical block appears more than once.
fn locate_region(lines: &[&str], expected: &[String], preferred_idx: usize) -> Option<(usize, usize)> {
	if expected.is_empty() || expected.len() > lines.len() {
		return None;
	}
	if matches_at(lines, preferred_idx, expected) {
		return Some((preferred_idx, preferred_idx + expected.len() - 1));
	}
	let mut best: Option<usize> = None;
	let mut best_distance = usize::MAX;
	for index in 0..=lines.len() - expected.len() {
		if !matches_at(lines, index, expected) {
			continue;
		}
		let distance = index.abs_diff(preferred_idx);
		if distance < best_distance {
			best = Some(index);
			best_distance = distance;
		}
	}
	best.map(|start| (start, start + expected.len() - 1))
}

fn matches_at(lines: &[&str], start_idx: usize, expected: &[String]) -> bool {
	if start_idx + expected.len() > lines.len() {
		return false;
	}
	expected
		.iter()
		.enumerate()
		.all(|(offset, expected)| strip_trailing_cr(lines[start_idx + offset]) == expected)
}

/// True when two registered blocks record the same marker-block content.
pub fn conflict_regions_equal(left: &ConflictBlock, right: &ConflictBlock) -> bool {
	build_recorded_region(left) == build_recorded_region(right)
}

/// True when the entry's recorded marker block still occurs in `content`
/// (LF-normalized). A stale re-registration whose region is gone must not be
/// confused with a distinct but byte-identical block still present elsewhere.
pub fn conflict_region_present(content: &str, block: &ConflictBlock) -> bool {
	let region = build_recorded_region(block).join("\n");
	if content.contains('\r') {
		content.replace("\r\n", "\n").contains(&region)
	} else {
		content.contains(&region)
	}
}

/// Expand `@ours` / `@theirs` / `@base` / `@both` lines against the recorded
/// sections. A token only triggers when it is an entire line; other lines pass
/// through verbatim.
pub fn expand_content_tokens(content: &str, entry: &ConflictEntry) -> Result<String, String> {
	let mut out: Vec<String> = Vec::new();
	for raw_line in content.split('\n') {
		let line = strip_trailing_cr(raw_line);
		match line {
			"@ours" => out.extend(entry.block.ours_lines.iter().cloned()),
			"@theirs" => out.extend(entry.block.theirs_lines.iter().cloned()),
			"@base" => {
				let Some(base_lines) = &entry.block.base_lines else {
					return Err(format!(
						"Conflict #{} has no base section (2-way merge). `@base` is only valid for diff3 conflicts.",
						entry.id
					));
				};
				out.extend(base_lines.iter().cloned());
			},
			"@both" => {
				out.extend(entry.block.ours_lines.iter().cloned());
				out.extend(entry.block.theirs_lines.iter().cloned());
			},
			_ => out.push(raw_line.to_string()),
		}
	}
	Ok(out.join("\n"))
}

/// Materialise a block for `conflict://<N>` reads.
///
/// Bare (no scope) returns the full block including markers. A scoped view
/// returns only that side's body; `base` errors for a 2-way conflict. The
/// returned start line is the 1-indexed file position of the first line, so
/// numbering lines up with the source file.
pub fn render_conflict_region(
	entry: &ConflictEntry,
	scope: Option<ConflictScope>,
) -> Result<(Vec<String>, u64), String> {
	match scope {
		Some(ConflictScope::Ours) => {
			Ok((entry.block.ours_lines.clone(), entry.block.start_line + 1))
		},
		Some(ConflictScope::Theirs) => {
			Ok((entry.block.theirs_lines.clone(), entry.block.separator_line + 1))
		},
		Some(ConflictScope::Base) => {
			let (Some(base_lines), Some(base_line)) = (&entry.block.base_lines, entry.block.base_line)
			else {
				return Err(format!(
					"Conflict #{} has no base section (2-way merge). 'conflict://{}/base' is only valid for diff3 conflicts.",
					entry.id, entry.id
				));
			};
			Ok((base_lines.clone(), base_line + 1))
		},
		None => {
			let mut out = Vec::new();
			out.push(marker_line(OURS_PREFIX, entry.block.ours_label.as_deref()));
			out.extend(entry.block.ours_lines.iter().cloned());
			if let Some(base_lines) = &entry.block.base_lines {
				out.push(marker_line(BASE_PREFIX, entry.block.base_label.as_deref()));
				out.extend(base_lines.iter().cloned());
			}
			out.push(SEPARATOR.to_string());
			out.extend(entry.block.theirs_lines.iter().cloned());
			out.push(marker_line(THEIRS_PREFIX, entry.block.theirs_label.as_deref()));
			Ok((out, entry.block.start_line))
		},
	}
}

const PREVIEW_SIDE_LINES: usize = 6;

/// Compact diff-style footer appended to a read that surfaced conflicts.
///
/// `total_in_file` may exceed `entries.len()` when only a window was read; the
/// header then points at `<path>:conflicts` for the full list.
pub fn format_conflict_warning(
	entries: &[ConflictEntry],
	total_in_file: usize,
	display_path: Option<&str>,
	scan_truncated: bool,
) -> String {
	if entries.is_empty() {
		return String::new();
	}
	let total = total_in_file.max(entries.len());
	let partial = total > entries.len();
	let mut out = vec![String::new()];
	let word = if total == 1 { "conflict" } else { "conflicts" };
	if partial {
		let hint = display_path.unwrap_or("<file>");
		out.push(format!(
			"⚠ {} of {total} unresolved {word} visible in this window (read `{hint}:conflicts` for the full list).",
			entries.len()
		));
	} else {
		out.push(format!("⚠ {total} unresolved {word} detected"));
	}
	if scan_truncated {
		out.push("- note: file scan hit the byte cap; additional conflicts may exist beyond the scanned prefix.".to_string());
	}

	if let Some(label) = pick_label(entries, |entry| entry.block.ours_label.as_deref()) {
		out.push(format!("- ours = {label}"));
	}
	if let Some(label) = pick_label(entries, |entry| entry.block.theirs_label.as_deref()) {
		out.push(format!("- theirs = {label}"));
	}
	if entries.iter().any(ConflictEntry::is_three_way) {
		let label = pick_label(entries, |entry| {
			entry.block.base_lines.as_ref().and(entry.block.base_label.as_deref())
		});
		out.push(format!("- base = {}", label.unwrap_or("(no label)")));
	}
	out.push(
		"NOTICE: Inspect a block by reading `conflict://<N>` (add `/ours` / `/theirs` / `/base` to render a single side). Resolve with `write({ path: \"conflict://<N>\", content })`, or bulk-resolve every registered conflict with `write({ path: \"conflict://*\", content })`. Writes replace ONLY the marker block (markers + all sides) — never repeat the lines before/after it; they stay in place.".to_string(),
	);
	out.push(
		"`content` shorthand: a line that is exactly `@ours` / `@theirs` / `@base` / `@both` expands to that recorded section. `@both` is ours-then-theirs with no separator — only for additive conflicts where each side adds something different; NEVER for competing edits of the same lines (pick a side or write the combined text). Lines that are not a token pass through verbatim, so `\"// keep both\\n@ours\\n@theirs\"` literally writes the comment, then ours, then theirs.".to_string(),
	);
	out.push(
		"Per-id bulk: `write({ path: \"conflict://*\", content: \"1: @ours\\n2: @theirs\\n…\" })` resolves each listed id with that side in ONE call — the cheapest way through many pick-one conflicts; unlisted ids stay registered.".to_string(),
	);
	out.push(
		"Resolve each block faithfully: keep one side (`@ours`/`@theirs`), or combine them when both intents apply — never invent content beyond the recorded sides, and never stack both sides of competing edits. Resolve several conflicts in a single turn by issuing multiple `write` calls at once; ids stay valid as earlier blocks are resolved.".to_string(),
	);

	for entry in entries {
		let range = if entry.block.start_line == entry.block.end_line {
			format!("L{}", entry.block.start_line)
		} else {
			format!("L{}-{}", entry.block.start_line, entry.block.end_line)
		};
		out.push(String::new());
		out.push(format!("──── #{}  {range} ────", entry.id));

		let base_equals_ours = entry
			.block
			.base_lines
			.as_ref()
			.is_some_and(|base| sections_equal(base, &entry.block.ours_lines));
		let base_equals_theirs = entry
			.block
			.base_lines
			.as_ref()
			.is_some_and(|base| sections_equal(base, &entry.block.theirs_lines));
		let theirs_equals_ours = sections_equal(&entry.block.theirs_lines, &entry.block.ours_lines);

		out.push("<<< ours".to_string());
		append_body(&mut out, &entry.block.ours_lines);

		if let Some(base_lines) = &entry.block.base_lines {
			if base_equals_ours {
				out.push("=== base ≡ ours".to_string());
			} else if base_equals_theirs {
				out.push("=== base ≡ theirs".to_string());
			} else {
				out.push("=== base".to_string());
				append_body(&mut out, base_lines);
			}
		}

		if theirs_equals_ours {
			out.push(">>> theirs ≡ ours".to_string());
		} else {
			out.push(">>> theirs".to_string());
			append_body(&mut out, &entry.block.theirs_lines);
		}
	}
	out.join("\n")
}

/// One-line-per-block index returned by `<path>:conflicts`.
pub fn format_conflict_summary(
	entries: &[ConflictEntry],
	display_path: &str,
	scan_truncated: bool,
) -> String {
	let total = entries.len();
	let word = if total == 1 { "conflict" } else { "conflicts" };
	let mut lines = vec![format!(
		"⚠ {total} unresolved {word} in {}",
		if display_path.is_empty() { "<file>" } else { display_path }
	)];
	if scan_truncated {
		lines.push("- note: file scan hit the byte cap; additional conflicts may exist beyond the scanned prefix.".to_string());
	}
	if let Some(label) = pick_label(entries, |entry| entry.block.ours_label.as_deref()) {
		lines.push(format!("- ours = {label}"));
	}
	if let Some(label) = pick_label(entries, |entry| entry.block.theirs_label.as_deref()) {
		lines.push(format!("- theirs = {label}"));
	}
	if entries.iter().any(ConflictEntry::is_three_way) {
		let label = pick_label(entries, |entry| {
			entry.block.base_lines.as_ref().and(entry.block.base_label.as_deref())
		});
		lines.push(format!("- base = {}", label.unwrap_or("(no label)")));
	}
	lines.push(
		"NOTICE: Bulk-resolve with `write({ path: \"conflict://*\", content })`, or address a single block with `write({ path: \"conflict://<N>\", content })`. Inspect a block by reading `conflict://<N>` (add `/ours` / `/theirs` / `/base` for a single side).".to_string(),
	);
	lines.push(
		"`content` shorthand: `@ours` / `@theirs` / `@base` / `@both` lines expand to the recorded sections; `@both` = ours-then-theirs (additive conflicts only — never for competing edits of the same lines). Per-id bulk: content of `<id>: @side` lines (e.g. \"1: @ours\\n2: @theirs\") resolves each listed id in one call. Non-token lines pass through verbatim. Writes replace ONLY the marker block — never repeat the surrounding lines. Keep one side or combine faithfully; never invent content beyond the recorded sides.".to_string(),
	);
	lines.push(String::new());
	let id_width = entries.last().map_or(1, |entry| entry.id.to_string().len());
	for entry in entries {
		let range = if entry.block.start_line == entry.block.end_line {
			format!("L{}", entry.block.start_line)
		} else {
			format!("L{}-{}", entry.block.start_line, entry.block.end_line)
		};
		let kind = if entry.is_three_way() { "  (3-way)" } else { "" };
		lines.push(format!("#{:>id_width$}  {range}{kind}", entry.id));
	}
	lines.join("\n")
}

fn pick_label<'a>(
	entries: &'a [ConflictEntry],
	get: impl Fn(&'a ConflictEntry) -> Option<&'a str>,
) -> Option<&'a str> {
	entries
		.iter()
		.filter_map(get)
		.find(|label| !label.trim().is_empty())
}

fn sections_equal(left: &[String], right: &[String]) -> bool {
	left == right
}

fn append_body(out: &mut Vec<String>, section: &[String]) {
	if section.is_empty() {
		out.push("(empty)".to_string());
		return;
	}
	let shown = section.len().min(PREVIEW_SIDE_LINES);
	out.extend(section[..shown].iter().cloned());
	let hidden = section.len() - shown;
	if hidden > 0 {
		out.push(format!(
			"… ({hidden} more line{})",
			if hidden == 1 { "" } else { "s" }
		));
	}
}

/// Parse `conflict://*` per-id directives: every non-empty line must be
/// `<id>: @side` (also `#<id> = @side`).
///
/// Returns `Ok(None)` only when no line is directive-shaped (uniform bulk
/// mode). A partial block — valid directives mixed with anything else — is an
/// error, because falling through to uniform mode would paste the directive
/// text verbatim into every block.
pub fn parse_bulk_directives(content: &str) -> Result<Option<BTreeMap<u64, String>>, String> {
	let mut directives = BTreeMap::new();
	let mut stray: Vec<&str> = Vec::new();
	let mut saw_directive = false;
	for raw in content.split('\n') {
		let line = raw.trim();
		if line.is_empty() {
			continue;
		}
		match parse_directive_line(line) {
			Some((id, side)) => {
				saw_directive = true;
				if directives.insert(id, side).is_some() {
					return Err(format!(
						"Bulk directive lists conflict #{id} twice — each id may appear once."
					));
				}
			},
			None => stray.push(line),
		}
	}
	if !saw_directive {
		return Ok(None);
	}
	if let Some(sample) = stray.first() {
		let head_shaped = directive_head_shaped(sample);
		let token_hint = if head_shaped {
			"Per-id bulk only accepts the tokens @ours/@theirs/@base/@both — one side per id, single line. "
		} else {
			""
		};
		let sample = truncate_directive(sample);
		return Err(format!(
			"Malformed `conflict://*` per-id block: {} line(s) are not `<id>: @side` directives (first: `{sample}`). {token_hint}Literal or multi-line replacement content isn't supported in a per-id block — resolve those blocks with individual `write({{ path: \"conflict://<N>\", content }})` calls (you can issue several at once). For a pure pick-a-side pass, make every non-empty line `<id>: @ours` (or @theirs/@base/@both).", 
			stray.len()
		));
	}
	Ok(Some(directives))
}

fn parse_directive_line(line: &str) -> Option<(u64, String)> {
	let rest = line.strip_prefix('#').unwrap_or(line);
	let split = rest.find([':', '='])?;
	let (head, value) = rest.split_at(split);
	let head = head.trim();
	if head.is_empty() || !head.bytes().all(|byte| byte.is_ascii_digit()) {
		return None;
	}
	let id = head.parse::<u64>().ok()?;
	let side = value[1..].trim();
	if !matches!(side, "@ours" | "@theirs" | "@base" | "@both") {
		return None;
	}
	Some((id, side.to_string()))
}

fn directive_head_shaped(line: &str) -> bool {
	let rest = line.strip_prefix('#').unwrap_or(line);
	let Some(split) = rest.find([':', '=']) else {
		return false;
	};
	rest[..split].trim().bytes().all(|byte| byte.is_ascii_digit()) && !rest[..split].trim().is_empty()
}

fn truncate_directive(line: &str) -> String {
	if line.chars().count() > 60 {
		let prefix: String = line.chars().take(57).collect();
		format!("{prefix}…")
	} else {
		line.to_string()
	}
}

/// Shared per-context handle. Clones of a context share one history; a fresh
/// context (child agent, embedder) gets its own.
pub type SharedConflictHistory = Arc<ConflictHistory>;

#[cfg(test)]
mod tests {
	use super::*;

	fn block_2way() -> ConflictBlock {
		ConflictBlock {
			start_line: 2,
			separator_line: 4,
			end_line: 6,
			base_line: None,
			ours_label: Some("HEAD".to_string()),
			base_label: None,
			theirs_label: Some("feature".to_string()),
			ours_lines: vec!["a".to_string(), "b".to_string()],
			base_lines: None,
			theirs_lines: vec!["c".to_string()],
		}
	}

	#[test]
	fn scans_diff3_and_2way_blocks() {
		let lines: Vec<String> = [
			"ctx",
			"<<<<<<< HEAD",
			"a",
			"||||||| base",
			"z",
			"=======",
			"c",
			">>>>>>> feature",
			"<<<<<<< HEAD",
			"x",
			"=======",
			"y",
			">>>>>>> feature",
		]
		.iter()
		.map(|line| line.to_string())
		.collect();
		let blocks = scan_conflict_lines(&lines, 1);
		assert_eq!(blocks.len(), 2);
		assert_eq!(blocks[0].base_lines.as_deref(), Some(&["z".to_string()][..]));
		assert_eq!(blocks[1].base_lines, None);
		assert_eq!(blocks[1].start_line, 9);
	}

	#[test]
	fn strict_marker_shape_only() {
		let lines: Vec<String> = ["<<<<<<<x", "a", "=======", "b", ">>>>>>> y"]
			.iter()
			.map(|line| line.to_string())
			.collect();
		assert!(scan_conflict_lines(&lines, 1).is_empty());
	}

	#[test]
	fn splice_shifts_and_preserves_crlf() {
		let original = "top\r\n<<<<<<< HEAD\r\na\r\nb\r\n=======\r\nc\r\n>>>>>>> feature\r\nbottom\r\n";
		let mut history = ConflictHistory::new();
		let entry = history.register(
			Path::new("/tmp/f.txt"),
			"f.txt",
			ConflictBlock {
				start_line: 2,
				..block_2way()
			},
			true,
		);
		let expanded = expand_content_tokens("@ours", &entry).expect("expand");
		let splice = splice_conflict(original, &entry, &expanded).expect("splice");
		assert_eq!(splice.text, "top\r\na\r\nb\r\nbottom\r\n");
	}

	#[test]
	fn splice_refuses_altered_region() {
		let original = "<<<<<<< HEAD\nCHANGED\n=======\nc\n>>>>>>> feature\n";
		let mut history = ConflictHistory::new();
		let entry = history.register(Path::new("/tmp/f.txt"), "f.txt", block_2way(), true);
		assert!(splice_conflict(original, &entry, "@ours").is_err());
	}

	#[test]
	fn tokens_expand_and_base_requires_diff3() {
		let mut history = ConflictHistory::new();
		let entry = history.register(Path::new("/tmp/f.txt"), "f.txt", block_2way(), true);
		assert_eq!(
			expand_content_tokens("// keep\n@ours\n@both", &entry).unwrap(),
			"// keep\na\nb\na\nb\nc"
		);
		assert!(expand_content_tokens("@base", &entry).is_err());
	}

	#[test]
	fn parse_uris_and_prefixes() {
		assert_eq!(
			parse_conflict_uri("conflict://3/theirs").unwrap(),
			Some(ParsedConflictUri {
				id: ConflictId::Id(3),
				scope: Some(ConflictScope::Theirs),
				recovered_prefix: None,
			})
		);
		assert!(parse_conflict_uri("conflict://*").unwrap().is_some());
		assert!(parse_conflict_uri("file.rs:conflict://2").unwrap().unwrap().recovered_prefix.is_some());
		assert!(parse_conflict_uri("conflict://0").is_err());
		assert!(parse_conflict_uri("conflict://*/ours").is_err());
		assert!(parse_conflict_uri("src/x.rs").unwrap().is_none());
	}

	#[test]
	fn bulk_directives_reject_partial_blocks() {
		let parsed = parse_bulk_directives("1: @ours\n#2 = @theirs").unwrap().unwrap();
		assert_eq!(parsed.get(&1).map(String::as_str), Some("@ours"));
		assert!(parse_bulk_directives("@ours").unwrap().is_none());
		assert!(parse_bulk_directives("1: @ours\n2: literal").is_err());
		assert!(parse_bulk_directives("1: @ours\n1: @theirs").is_err());
	}

	#[test]
	fn splice_tolerates_shifted_line_numbers() {
		let original = "header\nmore\ntop\n<<<<<<< HEAD\na\nb\n=======\nc\n>>>>>>> feature\nbottom\n";
		let mut history = ConflictHistory::new();
		let entry = history.register(Path::new("/tmp/f.txt"), "f.txt", block_2way(), true);
		let expanded = expand_content_tokens("@ours", &entry).expect("expand");
		let splice = splice_conflict(original, &entry, &expanded).expect("shifted match");
		assert_eq!(splice.text, "header\nmore\ntop\na\nb\nbottom\n");
	}

	#[test]
	fn scoped_base_requires_a_diff3_block() {
		let mut history = ConflictHistory::new();
		let entry = history.register(Path::new("/tmp/f.txt"), "f.txt", block_2way(), true);
		assert!(render_conflict_region(&entry, Some(ConflictScope::Base)).is_err());
		assert!(render_conflict_region(&entry, Some(ConflictScope::Ours)).is_ok());
	}

	#[test]
	fn region_presence_ignores_crlf() {
		let block = block_2way();
		let lf = "<<<<<<< HEAD\na\nb\n=======\nc\n>>>>>>> feature\n";
		assert!(conflict_region_present(lf, &block));
		assert!(conflict_region_present(&lf.replace('\n', "\r\n"), &block));
		assert!(!conflict_region_present("nothing\n", &block));
	}

	#[tokio::test]
	async fn file_scan_reports_the_byte_cap() {
		let dir = std::env::temp_dir().join(format!(
			"omp-conflict-scan-{}-{:?}",
			std::process::id(),
			std::thread::current().id()
		));
		std::fs::create_dir_all(&dir).expect("mkdir");
		let path = dir.join("big.txt");
		let text = format!("{}<<<<<<< HEAD\na\n=======\nb\n>>>>>>> feature\n", "x".repeat(64));
		std::fs::write(&path, text).expect("seed");
		let (blocks, truncated) = scan_file_for_conflicts(&path, 32).await.expect("scan");
		assert!(truncated);
		assert!(blocks.is_empty(), "marker block starts past the cap");
		let _ = std::fs::remove_dir_all(&dir);
	}
}
