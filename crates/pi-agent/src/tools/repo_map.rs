//! `read <directory>:map` — a bounded, deterministic, syntax-only map of
//! declarations in the files under a directory.
//!
//! The map deliberately stops at syntax. It lists what tree-sitter parsed
//! (`kind name` plus the declaration's 1-based line), never bodies, never
//! reference resolution. Traversal uses the shared walker with ignore files,
//! `FollowLinks::Never`, sorted paths, and a fixed file/byte/symbol budget so
//! the same tree produces the same output. A symlinked root is rejected
//! explicitly rather than returning an empty map. Files in languages without a
//! map rule are counted and skipped rather than guessed at.

use std::{
	fs, io,
	path::{Path, PathBuf},
};

use pi_ast::map::{
	MAP_MAX_SYMBOLS_PER_FILE, RankedFile, RepoMapBuilder, RepoMapOptions, repo_map_supported_path,
};
use pi_walker::{
	DirectoryErrorMode, FollowLinks, WalkDetail, WalkFilter, WalkOrder, WalkRank, WalkRequest,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use super::read::{DEFAULT_MAX_BYTES, result_with_details, truncate_head};
use crate::{
	state::{AgentContext, ToolResult},
	tokenizer::Tokenizer,
	tool::tool_result,
};

/// Files examined by one map call.
const MAP_MAX_FILES: usize = 500;
/// Bytes read from one file before it is skipped as oversized.
const MAP_MAX_FILE_BYTES: u64 = 512 * 1024;
/// Declarations emitted by one map call across all files.
const MAP_MAX_SYMBOLS: usize = 2000;
/// Token budget for one rendered map (Aider's default repo-map size).
const MAP_MAX_TOKENS: usize = 1024;

#[derive(Clone, Copy)]
struct MapLimits {
	max_files: usize,
	max_file_bytes: u64,
	max_symbols: usize,
	max_tokens: usize,
}

impl Default for MapLimits {
	fn default() -> Self {
		Self {
			max_files: MAP_MAX_FILES,
			max_file_bytes: MAP_MAX_FILE_BYTES,
			max_symbols: MAP_MAX_SYMBOLS,
			max_tokens: MAP_MAX_TOKENS,
		}
	}
}

struct MapReport {
	rendered: String,
	cancelled: bool,
	files: usize,
	symbols: usize,
	scanned: usize,
	skipped_unsupported: usize,
	skipped_large: usize,
	skipped_unreadable: usize,
	truncated: bool,
}

impl MapReport {
	fn cancelled() -> Self {
		Self {
			rendered: String::new(),
			cancelled: true,
			files: 0,
			symbols: 0,
			scanned: 0,
			skipped_unsupported: 0,
			skipped_large: 0,
			skipped_unreadable: 0,
			truncated: false,
		}
	}
}

pub(super) async fn read_directory_map(target: &Path, ctx: &AgentContext) -> ToolResult {
	if ctx.cancellation_token.is_cancelled() {
		return tool_result("Read cancelled", true);
	}
	let root = target.to_path_buf();
	let token = ctx.cancellation_token.clone();
	let worker_token = token.clone();
	let built = match tokio::task::spawn_blocking(move || {
		build_map(&root, &worker_token, MapLimits::default())
	})
	.await
	{
		Ok(Ok(built)) => built,
		Ok(Err(err)) => return tool_result(format!("Cannot map directory: {err}"), true),
		Err(err) => return tool_result(format!("Cannot map directory: {err}"), true),
	};
	if token.is_cancelled() || built.cancelled {
		return tool_result("Read cancelled", true);
	}

	let details = json!({
		"isDirectory": true,
		"resolvedPath": target.display().to_string(),
		"repoMap": {
			"syntaxOnly": true,
			"ranked": true,
			"tokens": Tokenizer::count_text(&built.rendered),
			"maxTokens": MAP_MAX_TOKENS,
			"files": built.files,
			"symbols": built.symbols,
			"scannedFiles": built.scanned,
			"truncated": built.truncated,
			"skipped": {
				"unsupported": built.skipped_unsupported,
				"oversized": built.skipped_large,
				"unreadable": built.skipped_unreadable,
			},
		},
	});
	result_with_details(built.rendered, details, false)
}

fn build_map(root: &Path, token: &CancellationToken, limits: MapLimits) -> io::Result<MapReport> {
	if token.is_cancelled() {
		return Ok(MapReport::cancelled());
	}
	// Trailing separators make `symlink_metadata` follow a symlinked root, so
	// normalize first; a link root is rejected rather than silently emptied.
	let normalized: PathBuf = root.components().collect();
	match fs::symlink_metadata(&normalized) {
		Ok(metadata) if metadata.file_type().is_symlink() => {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				format!(
					"refusing to map symlinked directory root '{}'; pass the real directory path",
					normalized.display()
				),
			));
		},
		Ok(metadata) if !metadata.is_dir() => {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				format!("'{}' is not a directory", normalized.display()),
			));
		},
		Ok(_) => {},
		Err(err) => return Err(err),
	}
	let request = WalkRequest::new(normalized.clone())
		.hidden(true)
		.gitignore(true)
		.skip_git(true)
		.follow_links(FollowLinks::Never)
		.detail(WalkDetail::Full)
		.order(WalkOrder::Path)
		.depth(1, usize::MAX)
		.directory_errors(DirectoryErrorMode::SkipSkippable)
		.cache(false)
		.filter(WalkFilter::files_only());
	let heartbeat = || {
		if token.is_cancelled() {
			Err("cancelled")
		} else {
			Ok(())
		}
	};
	let outcome =
		match request.collect_ranked_with_heartbeat(WalkRank::PathAsc, limits.max_files, heartbeat) {
			Ok(outcome) => outcome,
			Err(_) if token.is_cancelled() => return Ok(MapReport::cancelled()),
			Err(err) => return Err(io::Error::other(err.to_string())),
		};

	let scan_truncated = outcome.stats.limited_entries > 0;
	let mut builder = RepoMapBuilder::new(RepoMapOptions {
		max_symbols: limits.max_symbols,
		per_file_symbols: MAP_MAX_SYMBOLS_PER_FILE,
		chat_files: Vec::new(),
		mentioned_idents: Vec::new(),
	});
	let mut scanned = 0usize;
	let mut skipped_unsupported = 0usize;
	let mut skipped_large = 0usize;
	let mut skipped_unreadable = 0usize;

	for entry in outcome.entries {
		if token.is_cancelled() {
			return Ok(MapReport::cancelled());
		}
		let absolute = entry.absolute_path(&normalized);
		let relative = entry.path;
		let Some(language) = repo_map_supported_path(&absolute) else {
			skipped_unsupported += 1;
			continue;
		};
		let size = match entry.size {
			Some(size) => size as u64,
			None => match fs::metadata(&absolute) {
				Ok(metadata) => metadata.len(),
				Err(_) => {
					skipped_unreadable += 1;
					continue;
				},
			},
		};
		if size > limits.max_file_bytes {
			skipped_large += 1;
			continue;
		}
		let bytes = match fs::read(&absolute) {
			Ok(bytes) => bytes,
			Err(_) => {
				skipped_unreadable += 1;
				continue;
			},
		};
		if bytes.len() as u64 > limits.max_file_bytes {
			skipped_large += 1;
			continue;
		}
		scanned += 1;
		let code = String::from_utf8_lossy(&bytes).into_owned();
		builder.add(&relative, &code, language);
	}

	let ranking = builder.finish();
	let symbol_truncated = ranking.symbol_truncated;
	let per_file_truncated = ranking.per_file_truncated;
	let files: Vec<RankedFile> = ranking
		.files
		.into_iter()
		.filter(|file| !file.symbols.is_empty() || !file.parsed)
		.collect();
	let symbols = files.iter().map(|file| file.symbols.len()).sum();
	let (rendered, output_truncated) = render_map(
		&files,
		scanned,
		symbols,
		skipped_unsupported,
		skipped_large,
		skipped_unreadable,
		scan_truncated,
		symbol_truncated,
		per_file_truncated,
		limits,
	);
	let truncated = scan_truncated || symbol_truncated || per_file_truncated || output_truncated;
	Ok(MapReport {
		rendered,
		cancelled: false,
		files: files.len(),
		symbols,
		scanned,
		skipped_unsupported,
		skipped_large,
		skipped_unreadable,
		truncated,
	})
}

#[allow(
	clippy::too_many_arguments,
	reason = "one rendering pass; grouping the counters into a struct would not clarify the call"
)]
fn render_map(
	files: &[RankedFile],
	scanned: usize,
	symbols: usize,
	skipped_unsupported: usize,
	skipped_large: usize,
	skipped_unreadable: usize,
	scan_truncated: bool,
	symbol_truncated: bool,
	per_file_truncated: bool,
	limits: MapLimits,
) -> (String, bool) {
	let mut out = String::new();
	out.push_str(
		"[map: syntax-only declarations from tree-sitter tags; heuristic PageRank ranking; bodies omitted]\n",
	);
	out.push_str(&format!(
		"[map: {} file(s), {} symbol(s); scanned {} supported file(s), skipped {} unsupported, {} oversized, {} unreadable]\n",
		files.len(),
		symbols,
		scanned,
		skipped_unsupported,
		skipped_large,
		skipped_unreadable,
	));
	if files.is_empty() {
		out.push_str("(no supported declarations found)\n");
	}
	let mut tokens = Tokenizer::count_text(&out);
	let mut token_truncated = false;
	let mut byte_truncated = false;
	let mut emitted_file = false;
	let mut trimmable_starts: Vec<usize> = Vec::new();
	'files: for file in files {
		let mut header = String::new();
		header.push_str(&file.path);
		if !file.parsed {
			header.push_str(" [parse errors: map may be incomplete]");
		}
		header.push('\n');
		let header_tokens = Tokenizer::count_text(&header);
		// The first ranked file is always named: an empty map is worse than a
		// header that eats most of a tight budget.
		if emitted_file && tokens + header_tokens > limits.max_tokens {
			token_truncated = true;
			break;
		}
		if (out.len() + header.len()) as u64 > DEFAULT_MAX_BYTES {
			byte_truncated = true;
			break;
		}
		trimmable_starts.push(out.len());
		out.push_str(&header);
		tokens += header_tokens;
		emitted_file = true;
		for symbol in &file.symbols {
			let line = format!("  {}: {} {}\n", symbol.line, symbol.kind, symbol.name);
			let line_tokens = Tokenizer::count_text(&line);
			if tokens + line_tokens > limits.max_tokens {
				token_truncated = true;
				break 'files;
			}
			if (out.len() + line.len()) as u64 > DEFAULT_MAX_BYTES {
				byte_truncated = true;
				break 'files;
			}
			trimmable_starts.push(out.len());
			out.push_str(&line);
			tokens += line_tokens;
		}
	}
	let mut notice = String::new();
	loop {
		let mut reasons = Vec::new();
		if scan_truncated {
			reasons.push(format!("file scan limited to {}", limits.max_files));
		}
		if symbol_truncated {
			reasons.push(format!("symbol output limited to {}", limits.max_symbols));
		}
		if per_file_truncated {
			reasons.push(format!("per-file symbol limit {MAP_MAX_SYMBOLS_PER_FILE} reached"));
		}
		if skipped_large > 0 {
			reasons.push(format!("{} oversized file(s) skipped", skipped_large));
		}
		if token_truncated {
			reasons.push(format!("token budget {} reached", limits.max_tokens));
		}
		if byte_truncated {
			reasons.push("byte budget reached".to_string());
		}
		notice = if reasons.is_empty() {
			String::new()
		} else {
			format!("[map truncated: {}]\n", reasons.join("; "))
		};
		// The notice counts: drop emitted lines until the whole rendered map
		// (global header + file/symbol lines + notice) fits the token budget.
		// Oversized output remains possible only when the minimum header plus
		// notice alone exceeds the caller's budget.
		let total = Tokenizer::count_text(&out) + Tokenizer::count_text(&notice);
		if total <= limits.max_tokens {
			break;
		}
		let Some(start) = trimmable_starts.pop() else {
			break;
		};
		out.truncate(start);
		token_truncated = true;
	}
	if !notice.is_empty() {
		out.push_str(&notice);
	}
	let output_truncated = out.len() as u64 > DEFAULT_MAX_BYTES;
	if output_truncated {
		let mut cut = truncate_head(&out, DEFAULT_MAX_BYTES);
		cut.content.push_str("\n[map output truncated at 50 KiB]\n");
		(cut.content, true)
	} else {
		(out, token_truncated || byte_truncated)
	}
}

#[cfg(test)]
mod tests {
	use std::{
		fs,
		path::PathBuf,
		time::{SystemTime, UNIX_EPOCH},
	};

	use serde_json::json;
	use tokio_util::sync::CancellationToken;

	use super::*;
	use crate::{tool::Tool as _, tools::read::ReadTool};

	struct TempDir(PathBuf);

	impl TempDir {
		fn new(label: &str) -> Self {
			let nanos = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.map(|duration| duration.as_nanos())
				.unwrap_or_default();
			let path = std::env::temp_dir()
				.join(format!("omp-repo-map-{label}-{}-{nanos}", std::process::id()));
			fs::create_dir_all(&path).expect("create temp dir");
			Self(path)
		}

		fn write(&self, relative: &str, contents: &str) {
			let path = self.0.join(relative);
			if let Some(parent) = path.parent() {
				fs::create_dir_all(parent).expect("create parent");
			}
			fs::write(path, contents).expect("write fixture");
		}
	}

	impl Drop for TempDir {
		fn drop(&mut self) {
			let _ = fs::remove_dir_all(&self.0);
		}
	}

	fn report(root: &Path) -> MapReport {
		build_map(root, &CancellationToken::new(), MapLimits::default()).expect("map builds")
	}

	#[tokio::test]
	async fn map_lists_declarations_respects_ignore_and_skips_unsupported() {
		let dir = TempDir::new("basic");
		dir.write(".ignore", "ignored.rs\n");
		dir.write("src/a.rs", "pub fn alpha() {\n    let body_marker = 1;\n    body_marker\n}\n");
		dir.write("src/b.py", "def beta():\n    pass\n");
		dir.write("ignored.rs", "fn ignored() {}\n");
		dir.write("notes.txt", "not code\n");
		let ctx = AgentContext::new(dir.0.clone());
		let result = super::read_directory_map(&dir.0, &ctx).await;
		assert!(!result.is_error, "{}", result.content);
		assert!(result.content.starts_with("[map: syntax-only"), "{}", result.content);
		assert!(result.content.contains("src/a.rs"), "{}", result.content);
		assert!(result.content.contains("alpha"), "{}", result.content);
		assert!(result.content.contains("src/b.py"), "{}", result.content);
		assert!(result.content.contains("beta"), "{}", result.content);
		assert!(!result.content.contains("body_marker"), "{}", result.content);
		assert!(!result.content.contains("ignored"), "{}", result.content);
		assert!(!result.content.contains("notes.txt"), "{}", result.content);

		let whole = report(&dir.0);
		assert!(whole.skipped_unsupported >= 1, "notes.txt is not mappable");
	}

	#[tokio::test]
	async fn map_selector_on_a_file_is_an_explicit_error() {
		let dir = TempDir::new("file-map");
		dir.write("single.rs", "fn one() {}\n");
		let tool = ReadTool::new();
		let result = tool
			.execute(&json!({ "path": "single.rs:map" }), &AgentContext::new(dir.0.clone()))
			.await
			.expect("read executes");
		assert!(result.is_error, "{}", result.content);
		assert!(
			result.content.contains("only") && result.content.contains("director"),
			"{}",
			result.content
		);
	}

	#[tokio::test]
	async fn read_tool_routes_directory_map_selector() {
		let dir = TempDir::new("dir-map-route");
		dir.write("src/a.rs", "fn alpha() {}\n");
		let tool = ReadTool::new();
		let result = tool
			.execute(&json!({ "path": ".:map" }), &AgentContext::new(dir.0.clone()))
			.await
			.expect("read executes");
		assert!(!result.is_error, "{}", result.content);
		assert!(result.content.starts_with("[map: syntax-only"), "{}", result.content);
		assert!(result.content.contains("src/a.rs"), "{}", result.content);
		assert!(result.content.contains("alpha"), "{}", result.content);
	}

	#[test]
	fn map_bounds_the_file_scan_and_marks_truncation() {
		let dir = TempDir::new("file-bound");
		dir.write("a.rs", "fn a() {}\n");
		dir.write("b.rs", "fn b() {}\n");
		let limits = MapLimits { max_files: 1, ..MapLimits::default() };
		let report = build_map(&dir.0, &CancellationToken::new(), limits).expect("map builds");
		assert!(report.truncated);
		assert!(report.rendered.contains("[map truncated:"), "{}", report.rendered);
		assert!(report.files <= 1);
	}

	#[test]
	fn map_bounds_symbols_and_marks_truncation() {
		let dir = TempDir::new("symbol-bound");
		dir.write("a.rs", "fn a() {}\nfn b() {}\n");
		let limits = MapLimits { max_symbols: 1, ..MapLimits::default() };
		let report = build_map(&dir.0, &CancellationToken::new(), limits).expect("map builds");
		assert_eq!(report.symbols, 1);
		assert!(report.truncated);
		assert!(report.rendered.contains("symbol output limited to 1"), "{}", report.rendered);
	}

	#[test]
	fn map_applies_the_per_file_symbol_cap() {
		let dir = TempDir::new("per-file-bound");
		let mut code = String::new();
		for index in 0..=MAP_MAX_SYMBOLS_PER_FILE {
			code.push_str(&format!("fn f{index}() {{}}\n"));
		}
		dir.write("big.rs", &code);
		let report =
			build_map(&dir.0, &CancellationToken::new(), MapLimits::default()).expect("map builds");
		assert_eq!(report.symbols, MAP_MAX_SYMBOLS_PER_FILE);
		assert!(report.truncated);
		assert!(
			report
				.rendered
				.contains(&format!("per-file symbol limit {MAP_MAX_SYMBOLS_PER_FILE}")),
			"{}",
			report.rendered
		);
	}

	#[test]
	fn parse_error_files_are_reported_even_without_symbols() {
		let dir = TempDir::new("parse-error");
		dir.write("broken.rs", "@@@ not rust\n");
		let report =
			build_map(&dir.0, &CancellationToken::new(), MapLimits::default()).expect("map builds");
		assert!(report.rendered.contains("broken.rs [parse errors"), "{}", report.rendered);
	}

	#[test]
	fn tight_token_budget_keeps_partial_top_file() {
		use pi_ast::map::RankedSymbol;

		let symbols: Vec<RankedSymbol> = (0..200)
			.map(|index| RankedSymbol {
				kind: "fn".to_string(),
				name: format!("sym_{index}"),
				line: index as u32 + 1,
				refs: 0,
			})
			.collect();
		let files = [RankedFile {
			path: "top.rs".to_string(),
			parsed: true,
			score: 1.0,
			used_tags: true,
			symbols,
		}];
		let limits = MapLimits { max_tokens: 160, ..MapLimits::default() };
		let (rendered, truncated) = render_map(&files, 1, 200, 0, 0, 0, false, false, false, limits);
		assert!(truncated);
		assert!(Tokenizer::count_text(&rendered) <= 160, "{rendered}");
		assert!(rendered.contains("top.rs"), "{rendered}");
		assert!(rendered.contains("sym_0"), "{rendered}");
		assert!(!rendered.contains("sym_150"), "{rendered}");
		assert!(
			rendered
				.lines()
				.filter(|line| line.starts_with("  "))
				.count() < 200,
			"{rendered}"
		);
	}

	#[test]
	fn oversized_file_header_is_trimmed_to_budget() {
		let long_path = format!("{}file.rs", "nested/".repeat(300));
		let files = [RankedFile {
			path: long_path,
			parsed: true,
			score: 1.0,
			used_tags: false,
			symbols: Vec::new(),
		}];
		let limits = MapLimits { max_tokens: 160, ..MapLimits::default() };
		let (rendered, truncated) = render_map(&files, 1, 0, 0, 0, 0, false, false, false, limits);
		assert!(truncated);
		assert!(Tokenizer::count_text(&rendered) <= 160, "{rendered}");
		assert!(!rendered.contains("nested/"), "{rendered}");
	}

	#[test]
	fn token_budget_truncated_render_is_reported() {
		use pi_ast::map::RankedSymbol;

		let symbols: Vec<RankedSymbol> = (0..3000)
			.map(|index| RankedSymbol {
				kind: "function".to_string(),
				name: format!("symbol_{index:04}_{}", "x".repeat(30)),
				line: index as u32 + 1,
				refs: 0,
			})
			.collect();
		let files = [RankedFile {
			path: "big.rs".to_string(),
			parsed: true,
			score: 1.0,
			used_tags: true,
			symbols,
		}];
		let (rendered, truncated) =
			render_map(&files, 1, 3000, 0, 0, 0, false, false, false, MapLimits::default());
		assert!(truncated);
		assert!(Tokenizer::count_text(&rendered) <= 1024, "{rendered}");
		assert!(rendered.contains("token budget 1024 reached"), "{rendered}");
	}

	#[tokio::test]
	async fn output_token_truncation_sets_details_flag() {
		let dir = TempDir::new("byte-details");
		for file in 0..5 {
			let mut code = String::new();
			for index in 0..200 {
				code.push_str(&format!("fn symbol_{file}_{index}_{}() {{}}\n", "x".repeat(40)));
			}
			dir.write(&format!("f{file}.rs"), &code);
		}
		let result = super::read_directory_map(&dir.0, &AgentContext::new(dir.0.clone())).await;
		assert!(!result.is_error, "{}", result.content);
		let details = result.details.expect("details");
		assert_eq!(details["repoMap"]["truncated"], json!(true), "{details}");
		assert!(result.content.contains("token budget 1024 reached"), "{}", result.content);
	}

	#[cfg(unix)]
	#[tokio::test]
	async fn symlinked_directory_root_is_rejected() {
		let target = TempDir::new("symlink-root-target");
		target.write("a.rs", "fn a() {}\n");
		let container = TempDir::new("symlink-root");
		let link = container.0.join("link");
		if std::os::unix::fs::symlink(&target.0, &link).is_err() {
			return; // Filesystem without symlink support; nothing to assert.
		}
		let result = super::read_directory_map(&link, &AgentContext::new(container.0.clone())).await;
		assert!(result.is_error, "{}", result.content);
		assert!(result.content.contains("symlink"), "{}", result.content);

		let with_slash = PathBuf::from(format!("{}/", link.display()));
		let result =
			super::read_directory_map(&with_slash, &AgentContext::new(container.0.clone())).await;
		assert!(result.is_error, "{}", result.content);
		assert!(result.content.contains("symlink"), "{}", result.content);
	}

	#[test]
	fn cancelled_map_returns_no_partial_report() {
		let dir = TempDir::new("cancelled");
		dir.write("a.rs", "fn a() {}\n");
		let token = CancellationToken::new();
		token.cancel();
		let report = build_map(&dir.0, &token, MapLimits::default()).expect("map builds");
		assert!(report.cancelled);
		assert!(report.rendered.is_empty());
	}

	#[cfg(unix)]
	#[test]
	fn symlinked_directories_are_not_traversed() {
		let dir = TempDir::new("symlink");
		let outside = TempDir::new("symlink-outside");
		outside.write("secret.rs", "fn secret() {}\n");
		dir.write("visible.rs", "fn visible() {}\n");
		let link = dir.0.join("linked");
		if std::os::unix::fs::symlink(&outside.0, &link).is_err() {
			return; // Filesystem without symlink support; nothing to assert.
		}
		let report = report(&dir.0);
		assert!(!report.rendered.contains("secret"), "{}", report.rendered);
		assert!(report.rendered.contains("visible"), "{}", report.rendered);
	}
}
