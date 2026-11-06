//! Native hashline contract: content tags, bounded snapshots, the patch
//! grammar, clipboard registers, block anchors, and the `edit` tool.
//!
//! One [`SnapshotStore`] per agent session is shared by `read` (via
//! [`NativeSnapshotSink`]) and `edit`; the store is injected, never global. A
//! session's tags are content-derived, so what the store scopes is read
//! provenance (which lines this session displayed) and `CUT` registers — a
//! child registry must rebind its own store so neither leaks between agents.

pub mod apply;
pub mod budget;
pub mod block;
pub mod clipboard;
pub mod edit;
pub mod hash;
pub mod mismatch;
pub mod parser;
pub mod recovery;
pub mod syntax;
pub mod snapshot;

use std::{path::Path, sync::Arc};

pub use edit::execute;
pub use snapshot::{Snapshot, SnapshotStore, canonical_key, format_header};

/// Read-side sink state: the session store this agent's reads and edits share.
#[derive(Debug, Clone)]
pub struct NativeSnapshotSink {
	store: Arc<SnapshotStore>,
}

impl NativeSnapshotSink {
	pub fn new(store: Arc<SnapshotStore>) -> Self {
		Self { store }
	}

	/// Record the full normalized text a read observed; returns its 4-hex tag
	/// (`None` above the 4 MiB snapshot cap).
	pub fn record_file(&self, absolute_path: &Path, full_text: &str) -> Option<String> {
		self.store.record(&canonical_key(absolute_path), full_text, None)
	}

	/// Attach the 1-indexed source lines the read actually displayed.
	pub fn record_seen_lines(&self, absolute_path: &Path, tag: &str, lines: &[u32]) {
		if lines.is_empty() {
			return;
		}
		self
			.store
			.record_seen_lines(&canonical_key(absolute_path), tag, lines);
	}

	/// Section header line (`[path#TAG]`), with the path shown relative to
	/// `cwd` when it lives underneath it.
	pub fn header(&self, absolute_path: &Path, tag: &str, cwd: &Path) -> String {
		format_header(&display_path(absolute_path, cwd), tag)
	}
}

/// Present a read path relative to the session cwd (`/` separators).
pub fn display_path(path: &Path, cwd: &Path) -> String {
	match path.strip_prefix(cwd) {
		Ok(relative) => {
			let text = relative.to_string_lossy().replace('\\', "/");
			if text.is_empty() { ".".to_string() } else { text }
		}
		Err(_) => path.to_string_lossy().into_owned(),
	}
}

/// Whether the first non-empty line of `content` is a `[PATH#TAG]` display
/// header. Callers use this as display evidence before stripping hashline
/// echoes in a write body.
pub fn has_write_display_header(content: &str) -> bool {
	content
		.split('\n')
		.find(|line| !line.trim().is_empty())
		.is_some_and(|line| parser::is_display_header(line.trim()))
}

/// Strip the `[PATH#TAG]` header and uniform read line-number prefixes a model
/// copied from hashline display output. Mirrors upstream
/// `stripWriteContentWithPotentialLooseHeader`: strict stripping succeeds only
/// when every non-empty content line carries a prefix; otherwise a first-line
/// header is removed and the remainder is retried.
pub fn strip_write_display_prefixes(content: &str) -> (String, bool) {
	let lines: Vec<&str> = content.split('\n').collect();
	if let Some(cleaned) = strip_uniform_display_prefixes(&lines) {
		return (cleaned, true);
	}
	let Some(header_index) = lines.iter().position(|line| !line.trim().is_empty()) else {
		return (content.to_string(), false);
	};
	if !parser::is_display_header(lines[header_index].trim()) {
		return (content.to_string(), false);
	}
	let without_header: Vec<&str> = lines
		.iter()
		.enumerate()
		.filter(|(index, _)| *index != header_index)
		.map(|(_, line)| *line)
		.collect();
	match strip_uniform_display_prefixes(&without_header) {
		Some(cleaned) => (cleaned, true),
		None => (content.to_string(), false),
	}
}

/// Strict pass: every non-empty non-header line must carry one prefix.
fn strip_uniform_display_prefixes(lines: &[&str]) -> Option<String> {
	let mut non_empty = 0usize;
	let mut headers = 0usize;
	let mut prefixed = 0usize;
	for line in lines {
		if line.is_empty() {
			continue;
		}
		if parser::is_display_header((*line).trim()) {
			non_empty += 1;
			headers += 1;
			continue;
		}
		non_empty += 1;
		if parser::strip_one_leading_prefix(*line).is_some() {
			prefixed += 1;
		}
	}
	let content_lines = non_empty - headers;
	if content_lines == 0 || prefixed != content_lines {
		return None;
	}
	Some(
		lines
			.iter()
			.filter(|line| !parser::is_display_header((*line).trim()))
			.map(|line| {
				let line = *line;
				if line.is_empty() {
					String::new()
				} else {
					strip_all_leading_prefixes(line).to_string()
				}
			})
			.collect::<Vec<_>>()
			.join("\n"),
	)
}

fn strip_all_leading_prefixes(mut line: &str) -> &str {
	while let Some(stripped) = parser::strip_one_leading_prefix(line) {
		line = stripped;
	}
	line
}
