//! Deterministic conventional-commit inference for `arag commit`.
//!
//! Upstream anchors: `packages/coding-agent/src/commit/conventional/`
//! (`commit-types.ts`, `normalization.ts`, `validation.ts`) and
//! `conventional/inference.ts`. Upstream asks a model to choose the type and
//! summary; this build has no commit-message LLM, so it infers from the staged
//! file set and an optional `--context` hint, then normalizes and validates the
//! result with the same shape so a later model-backed path can reuse it.

use super::exclusions;

/// Conventional commit types this build emits, in upstream priority order.
pub const COMMIT_TYPES: &[&str] = &[
	"feat", "fix", "refactor", "docs", "test", "chore", "build", "perf", "deps", "style", "security",
];

/// Hard limit for the summary line, in bytes.
pub const SUMMARY_LIMIT: usize = 72;

/// A conventional commit message before it is formatted for `git commit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConventionalCommit {
	pub commit_type: String,
	pub scope:       Option<String>,
	pub summary:     String,
	pub body:        Vec<String>,
}

/// Canonical type for a raw token (`bugfix`, `feature`, `FEAT`, ...).
pub fn canonical_type(raw: &str) -> Option<&'static str> {
	let lower = raw
		.trim_matches(|c: char| !c.is_ascii_alphanumeric())
		.to_ascii_lowercase();
	if let Some(exact) = COMMIT_TYPES
		.iter()
		.copied()
		.find(|known| *known == lower.as_str())
	{
		return Some(exact);
	}
	Some(match lower.as_str() {
		"feature" | "features" | "enhancement" | "enhancements" => "feat",
		"bug" | "bugs" | "bugfix" | "bugfixes" | "hotfix" => "fix",
		"refactoring" | "refactorings" | "cleanup" | "rewrite" | "rework" => "refactor",
		"doc" | "documentation" => "docs",
		"tests" | "testing" => "test",
		"dependencies" | "dependency" | "dep" | "bump" => "deps",
		"ci" | "build-system" => "build",
		"performance" | "optimization" | "optimisation" => "perf",
		"hardening" | "cve" => "security",
		_ => return None,
	})
}

/// Whether `commit_type` is one of [`COMMIT_TYPES`].
pub fn is_commit_type(commit_type: &str) -> bool {
	COMMIT_TYPES.contains(&commit_type)
}

/// Infer the commit type from staged paths, with `--context` winning.
pub fn infer_type(files: &[String], context: Option<&str>) -> &'static str {
	if let Some(commit_type) = context.and_then(|text| parse_context(text).0) {
		return commit_type;
	}
	if files.is_empty() {
		return "chore";
	}
	if files.iter().all(|file| is_docs_path(file)) {
		return "docs";
	}
	if files.iter().all(|file| is_test_path(file)) {
		return "test";
	}
	if files.iter().all(|file| is_deps_path(file)) {
		return "deps";
	}
	if files.iter().all(|file| is_build_path(file)) {
		return "build";
	}
	"feat"
}

/// Infer a single scope shared by every path, or `None` when they disagree.
///
/// `crates/aragent/src/x.rs` yields `aragent`; root files and bare code roots
/// such as `src/x.rs` yield no scope.
pub fn infer_scope(files: &[String]) -> Option<String> {
	let mut scope: Option<String> = None;
	for file in files {
		let Some(candidate) = scope_for_path(file) else {
			return None;
		};
		match &scope {
			None => scope = Some(candidate),
			Some(existing) if *existing == candidate => {},
			Some(_) => return None,
		}
	}
	scope
}

/// Split `--context` into `(type, scope, summary)`.
///
/// A leading conventional prefix (`fix: ...`, `feat(commit): ...`) is honored;
/// otherwise the whole text is a summary hint.
pub fn parse_context(context: &str) -> (Option<&'static str>, Option<String>, Option<String>) {
	let trimmed = context.trim();
	if let Some((prefix, rest)) = trimmed.split_once(':') {
		let mut type_token = prefix.trim();
		let mut scope = None;
		if let Some((head, tail)) = type_token.split_once('(') {
			if let Some(scope_raw) = tail.strip_suffix(')') {
				scope = sanitize_scope(scope_raw);
				type_token = head.trim();
			}
		}
		if let Some(commit_type) = canonical_type(type_token) {
			let summary = Some(rest.trim().to_string()).filter(|text| !text.is_empty());
			return (Some(commit_type), scope, summary);
		}
	}
	(None, None, Some(trimmed.to_string()).filter(|text| !text.is_empty()))
}

/// Build a conventional message for `files`.
///
/// `stat` is an optional `git diff --stat` tail used as a body detail.
/// `legacy` keeps the old deterministic `update N files` summary.
pub fn build_message(
	files: &[String],
	context: Option<&str>,
	stat: Option<&str>,
	legacy: bool,
) -> ConventionalCommit {
	let parsed = context.map(parse_context);
	let context_type = parsed.as_ref().and_then(|entry| entry.0);
	let context_scope = parsed.as_ref().and_then(|entry| entry.1.clone());
	let context_summary = parsed.as_ref().and_then(|entry| entry.2.clone());

	let commit_type = context_type
		.unwrap_or_else(|| infer_type(files, None))
		.to_string();
	let scope = context_scope.or_else(|| infer_scope(files));
	let summary = context_summary
		.map(|text| normalize_summary(&text))
		.filter(|text| !text.is_empty())
		.unwrap_or_else(|| {
			if legacy {
				legacy_summary(files)
			} else {
				default_summary(&commit_type, files)
			}
		});

	let mut body = Vec::new();
	if let Some(stat) = stat.map(str::trim).filter(|text| !text.is_empty()) {
		body.push(normalize_detail(stat));
	}
	if !files.is_empty() {
		body.push(normalize_detail(&format!("files: {}", join_limited(files, 10))));
	}

	ConventionalCommit { commit_type, scope, summary, body }
}

/// Collapse whitespace, strip trailing punctuation, lowercase the first token,
/// and cap the result at [`SUMMARY_LIMIT`] bytes.
pub fn normalize_summary(summary: &str) -> String {
	let collapsed = summary.split_whitespace().collect::<Vec<_>>().join(" ");
	let trimmed = collapsed.trim_end_matches(['.', ',', ';', ':']).trim();
	truncate_summary(&lowercase_first_token(trimmed))
}

/// Normalize one body detail: collapse whitespace, drop bullet prefixes and
/// trailing punctuation, then capitalize and terminate with a period.
pub fn normalize_detail(detail: &str) -> String {
	let collapsed = detail.split_whitespace().collect::<Vec<_>>().join(" ");
	let cleaned = collapsed
		.trim()
		.trim_start_matches(['-', '*', '+', '•'])
		.trim()
		.trim_end_matches(['.', ',', ';'])
		.trim();
	let capitalized = capitalize_first(cleaned);
	if capitalized.ends_with('.') {
		capitalized
	} else {
		format!("{capitalized}.")
	}
}

/// Validate a generated message. Explicit `-m` text is never validated.
pub fn validate_message(commit: &ConventionalCommit) -> Result<(), String> {
	if !is_commit_type(&commit.commit_type) {
		return Err(format!("invalid commit type: {}", commit.commit_type));
	}
	if let Some(scope) = &commit.scope {
		let valid = !scope.is_empty()
			&& scope
				.chars()
				.all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
		if !valid {
			return Err(format!("invalid commit scope: {scope}"));
		}
	}
	if commit.summary.trim().is_empty() {
		return Err("commit summary is empty".to_string());
	}
	if commit.summary.contains('\n') {
		return Err("commit summary must be a single line".to_string());
	}
	if commit.summary.len() > SUMMARY_LIMIT {
		return Err(format!("commit summary exceeds {SUMMARY_LIMIT} bytes"));
	}
	if commit.summary.ends_with('.') {
		return Err("commit summary must not end with a period".to_string());
	}
	if commit
		.summary
		.chars()
		.next()
		.is_some_and(char::is_uppercase)
	{
		return Err("commit summary must start lowercase".to_string());
	}
	Ok(())
}

/// Render a conventional commit as git commit-message text.
pub fn format_commit(commit: &ConventionalCommit) -> String {
	let scope = commit
		.scope
		.as_deref()
		.map(|scope| format!("({scope})"))
		.unwrap_or_default();
	let mut message = format!("{}{scope}: {}", commit.commit_type, commit.summary);
	if !commit.body.is_empty() {
		message.push_str("\n\n");
		message.push_str(
			&commit
				.body
				.iter()
				.map(|item| format!("- {item}"))
				.collect::<Vec<_>>()
				.join("\n"),
		);
	}
	message
}

fn default_summary(commit_type: &str, files: &[String]) -> String {
	let verb = match commit_type {
		"feat" => "add",
		"fix" => "fix",
		"docs" => "document",
		"test" => "test",
		"refactor" => "refactor",
		"deps" => "bump dependencies in",
		"build" => "update build for",
		"perf" => "optimize",
		"security" => "harden",
		"style" => "restyle",
		_ => "update",
	};
	match files {
		[] => "update working tree".to_string(),
		[only] => normalize_summary(&format!("{verb} {only}")),
		_ => normalize_summary(&format!("{verb} {} files", files.len())),
	}
}

fn legacy_summary(files: &[String]) -> String {
	match files {
		[] => "update working tree".to_string(),
		[only] => normalize_summary(&format!("update {only}")),
		_ => normalize_summary(&format!("update {} files", files.len())),
	}
}

fn scope_for_path(path: &str) -> Option<String> {
	let mut parts = path.split('/');
	let first = parts.next()?;
	let raw = match first {
		"crates" | "packages" | "apps" | "plugins" => parts.next()?,
		"src" | "tests" | "docs" | "scripts" | "ci" => return None,
		_ if parts.next().is_some() => first,
		_ => return None,
	};
	sanitize_scope(raw)
}

fn sanitize_scope(raw: &str) -> Option<String> {
	let scope: String = raw
		.chars()
		.map(|c| c.to_ascii_lowercase())
		.filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
		.collect();
	let scope = scope.trim_matches(['.', '-', '_']).to_string();
	(!scope.is_empty()).then_some(scope)
}

fn is_docs_path(path: &str) -> bool {
	let lower = path.to_ascii_lowercase();
	let name = lower.rsplit('/').next().unwrap_or(&lower);
	lower.starts_with("docs/")
		|| lower.contains("/docs/")
		|| matches!(
			lower.rsplit_once('.').map(|(_, extension)| extension),
			Some("md" | "mdx" | "rst" | "adoc")
		) || matches!(name, "readme" | "changelog" | "contributing" | "license")
		|| name.starts_with("readme.")
		|| name.starts_with("changelog.")
		|| name.starts_with("contributing.")
}

fn is_test_path(path: &str) -> bool {
	let lower = path.to_ascii_lowercase();
	let name = lower.rsplit('/').next().unwrap_or(&lower);
	lower.starts_with("tests/")
		|| lower.contains("/tests/")
		|| lower.contains("/test/")
		|| name.starts_with("test_")
		|| name.ends_with("_test.rs")
		|| name.ends_with("_test.py")
		|| name.contains(".test.")
		|| name.contains(".spec.")
}

fn is_deps_path(path: &str) -> bool {
	if exclusions::is_excluded_file(path) {
		return true;
	}
	let lower = path.to_ascii_lowercase();
	let name = lower.rsplit('/').next().unwrap_or(&lower);
	matches!(
		name,
		"cargo.toml"
			| "package.json"
			| "pyproject.toml"
			| "requirements.txt"
			| "go.mod"
			| "gemfile"
			| "pom.xml"
			| "build.gradle"
			| "composer.json"
			| "packages.lock.json"
	)
}

fn is_build_path(path: &str) -> bool {
	let lower = path.to_ascii_lowercase();
	let name = lower.rsplit('/').next().unwrap_or(&lower);
	lower.starts_with(".github/")
		|| lower.starts_with(".gitlab/")
		|| lower.starts_with("scripts/")
		|| lower.starts_with("ci/")
		|| lower.contains("/.cargo/")
		|| matches!(
			name,
			"makefile" | "justfile" | "dockerfile" | "build.rs" | ".gitlab-ci.yml" | "deny.toml"
		)
}

fn lowercase_first_token(text: &str) -> String {
	let token_end = text.find(char::is_whitespace).unwrap_or(text.len());
	let token = &text[..token_end];
	let letters: Vec<char> = token.chars().filter(|c| c.is_alphabetic()).collect();
	if letters.is_empty() || letters.iter().all(|c| c.is_uppercase()) {
		return text.to_string();
	}
	let mut chars = text.chars();
	let Some(first) = chars.next() else {
		return String::new();
	};
	if first.is_uppercase() {
		format!("{}{}", first.to_lowercase(), chars.as_str())
	} else {
		text.to_string()
	}
}

fn capitalize_first(text: &str) -> String {
	let mut chars = text.chars();
	let Some(first) = chars.next() else {
		return String::new();
	};
	format!("{}{}", first.to_uppercase(), chars.as_str())
}

fn truncate_summary(text: &str) -> String {
	if text.len() <= SUMMARY_LIMIT {
		return text.to_string();
	}
	let mut end = SUMMARY_LIMIT;
	while !text.is_char_boundary(end) {
		end -= 1;
	}
	let cut = text[..end].trim_end();
	let cut = cut
		.rsplit_once(' ')
		.map_or(cut, |(head, _)| head.trim_end());
	cut.trim_end_matches(['.', ',', ';', ':'])
		.trim()
		.to_string()
}

fn join_limited(items: &[String], limit: usize) -> String {
	if items.len() <= limit {
		return items.join(", ");
	}
	let mut joined = items[..limit].join(", ");
	joined.push_str(&format!(", +{} more", items.len() - limit));
	joined
}

#[cfg(test)]
mod tests {
	use super::*;

	fn paths(items: &[&str]) -> Vec<String> {
		items.iter().map(|item| (*item).to_string()).collect()
	}

	#[test]
	fn type_inference_follows_the_changed_paths() {
		assert_eq!(infer_type(&paths(&["README.md"]), None), "docs");
		assert_eq!(infer_type(&paths(&["docs/commit.md", "docs/usage.md"]), None), "docs");
		assert_eq!(infer_type(&paths(&["crates/a/tests/fixture.rs"]), None), "test");
		assert_eq!(infer_type(&paths(&["Cargo.toml"]), None), "deps");
		assert_eq!(infer_type(&paths(&["Cargo.lock"]), None), "deps");
		assert_eq!(infer_type(&paths(&[".github/workflows/ci.yml"]), None), "build");
		assert_eq!(infer_type(&paths(&["crates/aragent/src/main.rs"]), None), "feat");
		// A docs file next to code is a behavior change, not docs-only.
		assert_eq!(infer_type(&paths(&["README.md", "src/lib.rs"]), None), "feat");
		assert_eq!(infer_type(&[], None), "chore");
	}

	#[test]
	fn context_type_overrides_path_inference() {
		assert_eq!(infer_type(&paths(&["src/lib.rs"]), Some("fix: crash on empty input")), "fix");
		assert_eq!(infer_type(&paths(&["src/lib.rs"]), Some("feature")), "feat");
		assert_eq!(infer_type(&paths(&["README.md"]), Some("update wording")), "docs");
	}

	#[test]
	fn scope_is_shared_or_absent() {
		assert_eq!(
			infer_scope(&paths(&["crates/aragent/src/commit/mod.rs", "crates/aragent/Cargo.toml"])),
			Some("aragent".to_string())
		);
		assert_eq!(
			infer_scope(&paths(&["crates/aragent/src/a.rs", "crates/pi-agent/src/b.rs"])),
			None
		);
		assert_eq!(infer_scope(&paths(&["src/main.rs"])), None);
		assert_eq!(infer_scope(&paths(&["README.md"])), None);
		assert_eq!(infer_scope(&paths(&[".github/workflows/ci.yml"])), Some("github".to_string()));
	}

	#[test]
	fn context_parses_type_scope_and_summary() {
		let (commit_type, scope, summary) = parse_context("feat(commit): add dry run");
		assert_eq!(commit_type, Some("feat"));
		assert_eq!(scope.as_deref(), Some("commit"));
		assert_eq!(summary.as_deref(), Some("add dry run"));

		let (commit_type, scope, summary) = parse_context("bugfix: guard empty input");
		assert_eq!(commit_type, Some("fix"));
		assert_eq!(scope, None);
		assert_eq!(summary.as_deref(), Some("guard empty input"));

		let (commit_type, scope, summary) = parse_context("tighten the wording");
		assert_eq!(commit_type, None);
		assert_eq!(scope, None);
		assert_eq!(summary.as_deref(), Some("tighten the wording"));
	}

	#[test]
	fn summary_normalization_lowercases_and_strips_punctuation() {
		assert_eq!(normalize_summary("Added the Thing."), "added the Thing");
		assert_eq!(normalize_summary("  FIX   crash;; "), "FIX crash");
		assert_eq!(normalize_summary("fix: guard input:"), "fix: guard input");
		assert_eq!(normalize_summary(""), "");
	}

	#[test]
	fn summary_truncation_is_byte_safe_and_word_aware() {
		let long = "a".repeat(100);
		let truncated = normalize_summary(&long);
		assert!(truncated.len() <= SUMMARY_LIMIT);
		let unicode = format!("é {}", "x".repeat(100));
		let truncated = normalize_summary(&unicode);
		assert!(truncated.len() <= SUMMARY_LIMIT);
		assert!(truncated.starts_with('é'));
	}

	#[test]
	fn inferred_messages_validate_and_format() {
		let files =
			paths(&["crates/aragent/src/commit/mod.rs", "crates/aragent/src/commit/exclusions.rs"]);
		let mut commit =
			build_message(&files, None, Some("2 files changed, 10 insertions(+)"), false);
		assert_eq!(commit.commit_type, "feat");
		assert_eq!(commit.scope.as_deref(), Some("aragent"));
		assert_eq!(commit.summary, "add 2 files");
		assert!(validate_message(&commit).is_ok());

		let formatted = format_commit(&commit);
		assert!(formatted.starts_with("feat(aragent): add 2 files\n\n- "));
		assert!(formatted.contains("- Files: crates/aragent/src/commit/mod.rs"));

		commit.scope = None;
		assert_eq!(format_commit(&commit), formatted.replace("(aragent)", ""));
	}

	#[test]
	fn explicit_context_summary_is_used_verbatim_after_normalization() {
		let commit =
			build_message(&paths(&["src/lib.rs"]), Some("fix: Guard empty input."), None, false);
		assert_eq!(commit.commit_type, "fix");
		assert_eq!(commit.summary, "guard empty input");
		assert!(validate_message(&commit).is_ok());
	}

	#[test]
	fn legacy_summary_stays_deterministic() {
		let commit = build_message(&paths(&["a.rs", "b.rs"]), None, None, true);
		assert_eq!(commit.summary, "update 2 files");
		let single = build_message(&paths(&["a.rs"]), None, None, true);
		assert_eq!(single.summary, "update a.rs");
	}

	#[test]
	fn validation_rejects_malformed_messages() {
		let mut commit = ConventionalCommit {
			commit_type: "bogus".to_string(),
			scope: None,
			summary: "do it".to_string(),
			body: Vec::new(),
		};
		assert!(validate_message(&commit).is_err());

		commit.commit_type = "feat".to_string();
		commit.summary = String::new();
		assert!(validate_message(&commit).is_err());

		commit.summary = "Added a thing".to_string();
		assert!(validate_message(&commit).is_err());

		commit.summary = "add a thing.".to_string();
		assert!(validate_message(&commit).is_err());

		commit.summary = "add a thing".to_string();
		commit.scope = Some("Bad Scope".to_string());
		assert!(validate_message(&commit).is_err());

		commit.scope = Some("ok-scope".to_string());
		assert!(validate_message(&commit).is_ok());
	}

	#[test]
	fn body_details_capitalize_and_terminate() {
		assert_eq!(normalize_detail("* files: a.rs, b.rs"), "Files: a.rs, b.rs.");
		assert_eq!(normalize_detail("2 files changed. "), "2 files changed.");
	}
}
