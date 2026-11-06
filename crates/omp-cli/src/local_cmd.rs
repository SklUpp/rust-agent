//! Local C5 commands: `ttsr`, `gallery`, `grievances list`, `gc` (dry-run and
//! `--apply`), and `setup` probes.
//!
//! Anchors are the TypeScript CLI handlers in
//! `packages/coding-agent/src/cli/{ttsr,gallery,grievances,gc,setup}-cli.ts`.
//! `gc --apply` mutates local agent storage; every other command here is
//! read-only and none reaches the network, starts a daemon, or downloads models. `setup python` does execute the
//! configured/discovered interpreter (with startup hooks) for its availability
//! probe, so it is limited to trusted projects; the CLI itself performs no
//! install/download/mutation.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[cfg(not(target_os = "linux"))]
use std::os::unix::io::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::linux::net::SocketAddrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
#[cfg(target_os = "linux")]
use std::os::unix::net::{SocketAddr, UnixDatagram};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::dirs::{self, AgentDirCategory};
use crate::{Error, Result, config_cmd, yaml};

const DAY_MS: u64 = 86_400_000;
const GC_WRITE_GRACE_MS: u64 = 5 * 60_000;
const DEFAULT_MAX_SCAN_BYTES: u64 = 5 * 1024 * 1024;
const BINARY_PROBE_BYTES: usize = 8192;
const SESSION_PREFIX_BYTES: u64 = 4096;
const SESSION_SUFFIX_BYTES: u64 = 32_768;

// ───────────────────────────────────────────────────────────────────────────
// Shared helpers
// ───────────────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

fn modified_ms(meta: &fs::Metadata) -> u64 {
	meta.modified()
		.ok()
		.and_then(|time| time.duration_since(UNIX_EPOCH).ok())
		.map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(0))
}

fn resolve_path(cwd: &Path, path: &Path) -> PathBuf {
	dirs::absolute(cwd, path)
}

fn normalize_slashes(value: &str) -> String {
	value.replace('\\', "/")
}

fn display_path(path: &Path) -> String {
	path.display().to_string()
}

fn write_line(out: &mut dyn Write, line: &str) -> Result<()> {
	writeln!(out, "{line}")?;
	Ok(())
}

fn setting_value(cwd: &Path, key: &str) -> Option<Value> {
	config_cmd::effective_setting(cwd, &[], key).ok().flatten()
}

fn setting_bool(cwd: &Path, key: &str, default: bool) -> bool {
	match setting_value(cwd, key) {
		Some(Value::Bool(value)) => value,
		Some(Value::String(value)) => value.eq_ignore_ascii_case("true"),
		_ => default,
	}
}

fn setting_strings(cwd: &Path, key: &str) -> Vec<String> {
	match setting_value(cwd, key) {
		Some(Value::Array(items)) => items
			.into_iter()
			.filter_map(|item| item.as_str().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string))
			.collect(),
		Some(Value::String(value)) => value
			.split(',')
			.map(str::trim)
			.filter(|s| !s.is_empty())
			.map(str::to_string)
			.collect(),
		_ => Vec::new(),
	}
}

fn setting_string(cwd: &Path, key: &str) -> Option<String> {
	setting_value(cwd, key)
		.and_then(|value| value.as_str().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string))
}

// ───────────────────────────────────────────────────────────────────────────
// Rules (TTSR)
// ───────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Rule {
	name:            String,
	path:            PathBuf,
	content:         String,
	globs:           Option<Vec<String>>,
	always_apply:    bool,
	description:     Option<String>,
	condition:       Option<Vec<String>>,
	ast_condition:   Option<Vec<String>>,
	scope:           Option<Vec<String>>,
	provider:        String,
}

fn split_frontmatter(content: &str) -> (Option<&str>, &str) {
	let body = content.strip_prefix('\u{feff}').unwrap_or(content);
	if let Some(rest) = body.strip_prefix("---") {
		let rest = rest.strip_prefix('\r').unwrap_or(rest);
		let rest = rest.strip_prefix('\n').unwrap_or(rest);
		if let Some(end) = rest.find("\n---") {
			let front = &rest[..end];
			let after = &rest[end + 4..];
			let after = after.strip_prefix('\r').unwrap_or(after);
			let after = after.strip_prefix('\n').unwrap_or(after);
			return (Some(front), after);
		}
	}
	(None, body)
}

fn parse_rule_name(path: &Path, suffix: Option<&str>) -> String {
	let base = path.file_name().and_then(|name| name.to_str()).unwrap_or("rule");
	if let Some(suffix) = suffix {
		return base.strip_suffix(suffix).unwrap_or(base).to_string();
	}
	base.strip_suffix(".mdc")
		.or_else(|| base.strip_suffix(".md"))
		.unwrap_or(base)
		.to_string()
}

fn strings_from_frontmatter(value: Option<&yaml::Value>) -> Option<Vec<String>> {
	let Some(value) = value else {
		return None;
	};
	let mut tokens = Vec::new();
	match value {
		yaml::Value::Str(text) => {
			let trimmed = text.trim();
			if !trimmed.is_empty() {
				tokens.push(trimmed.to_string());
			}
		},
		yaml::Value::List(items) => {
			for item in items {
				if let Some(text) = item.as_str() {
					let trimmed = text.trim();
					if !trimmed.is_empty() {
						tokens.push(trimmed.to_string());
					}
				}
			}
		},
		_ => {},
	}
	if tokens.is_empty() {
		return None;
	}
	tokens.dedup();
	Some(tokens)
}

fn split_scope_tokens(value: &str) -> Vec<String> {
	let mut tokens = Vec::new();
	let mut current = String::new();
	let (mut paren, mut bracket, mut brace) = (0i32, 0i32, 0i32);
	let mut quote: Option<char> = None;
	let mut chars = value.chars().peekable();
	while let Some(ch) = chars.next() {
		if let Some(active) = quote {
			current.push(ch);
			if ch == active && !current.ends_with(&format!("\\{ch}")) {
				quote = None;
			}
			continue;
		}
		match ch {
			'"' | '\'' => {
				quote = Some(ch);
				current.push(ch);
			},
			'(' => {
				paren += 1;
				current.push(ch);
			},
			')' => {
				paren = (paren - 1).max(0);
				current.push(ch);
			},
			'[' => {
				bracket += 1;
				current.push(ch);
			},
			']' => {
				bracket = (bracket - 1).max(0);
				current.push(ch);
			},
			'{' => {
				brace += 1;
				current.push(ch);
			},
			'}' => {
				brace = (brace - 1).max(0);
				current.push(ch);
			},
			',' if paren == 0 && bracket == 0 && brace == 0 => {
				let token = current.trim().to_string();
				if !token.is_empty() {
					tokens.push(token);
				}
				current.clear();
			},
			_ => current.push(ch),
		}
	}
	let tail = current.trim();
	if !tail.is_empty() {
		tokens.push(tail.to_string());
	}
	tokens
}

fn normalize_scope_field(value: Option<&yaml::Value>) -> Option<Vec<String>> {
	let raw = strings_from_frontmatter(value)?;
	let mut tokens: Vec<String> = Vec::new();
	for token in raw {
		for part in split_scope_tokens(&token) {
			let quote = part.chars().next();
			let stripped = match quote {
				Some(q @ ('"' | '\'')) if part.len() >= 2 && part.ends_with(q) => {
					part[1..part.len() - 1].trim().to_string()
				},
				_ => part,
			};
			let stripped = stripped.trim().to_string();
			if !stripped.is_empty() && !tokens.contains(&stripped) {
				tokens.push(stripped);
			}
		}
	}
	if tokens.is_empty() {
		None
	} else {
		Some(tokens)
	}
}

fn is_likely_file_glob(value: &str) -> bool {
	let token = value.trim();
	if token.is_empty() || token.chars().any(|ch| matches!(ch, '\\' | '^' | '$' | '+' | '|' | '(' | ')')) {
		return false;
	}
	if !token.chars().any(|ch| matches!(ch, '?' | '*' | '[' | ']' | '{' | '}')) {
		return false;
	}
	if token.contains('/') {
		return true;
	}
	let Some(rest) = token.strip_prefix("*.") else {
		return false;
	};
	!rest.is_empty() && !rest.chars().any(char::is_whitespace) && !rest.contains('/')
}

fn parse_condition_and_scope(front: &yaml::Value) -> (Option<Vec<String>>, Option<Vec<String>>, Option<Vec<String>>) {
	let raw_condition = front
		.get("condition")
		.or_else(|| front.get("ttsr_trigger"))
		.or_else(|| front.get("ttsrTrigger"));
	let parsed_condition = strings_from_frontmatter(raw_condition);
	let ast_condition = strings_from_frontmatter(front.get("astCondition"));
	let parsed_scope = normalize_scope_field(front.get("scope"));

	let mut inferred_scope = Vec::new();
	let mut condition = Vec::new();
	for token in parsed_condition.unwrap_or_default() {
		if is_likely_file_glob(&token) {
			for tool in ["edit", "write"] {
				let scope = format!("tool:{tool}({token})");
				if !inferred_scope.contains(&scope) {
					inferred_scope.push(scope);
				}
			}
			continue;
		}
		if !condition.contains(&token) {
			condition.push(token);
		}
	}
	if condition.is_empty() && !inferred_scope.is_empty() {
		condition.push(".*".to_string());
	}

	let mut scope = parsed_scope.unwrap_or_default();
	for token in inferred_scope {
		if !scope.contains(&token) {
			scope.push(token);
		}
	}
	(
		if condition.is_empty() { None } else { Some(condition) },
		ast_condition,
		if scope.is_empty() { None } else { Some(scope) },
	)
}

fn build_rule(
	name: &str,
	content: &str,
	path: &Path,
	provider: &str,
	rule_name: Option<&str>,
	suffix: Option<&str>,
) -> Rule {
	let (front, body) = split_frontmatter(content);
	let front = front
		.and_then(|text| yaml::parse(text).ok())
		.unwrap_or(yaml::Value::Map(Vec::new()));
	let (condition, ast_condition, scope) = parse_condition_and_scope(&front);

	let globs = match front.get("globs") {
		Some(yaml::Value::List(items)) => {
			let globs: Vec<String> = items
				.iter()
				.filter_map(|item| item.as_str().map(str::to_string))
				.collect();
			(!globs.is_empty()).then_some(globs)
		},
		Some(yaml::Value::Str(text)) => Some(vec![text.clone()]),
		_ => None,
	};

	let resolved_name = rule_name
		.map(str::to_string)
		.unwrap_or_else(|| parse_rule_name(path, suffix));
	Rule {
		name: resolved_name,
		path: path.to_path_buf(),
		content: body.to_string(),
		globs,
		always_apply: front.get("alwaysApply").and_then(yaml::Value::as_bool).unwrap_or(false),
		description: front.get("description").and_then(yaml::Value::as_str).map(str::to_string),
		condition,
		ast_condition,
		scope,
		provider: provider.to_string(),
	}
}

/// Bundled `builtin-defaults` rule sources, embedded from the same markdown
/// assets the TypeScript package compiles in. Provider priority 1 means every
/// on-disk/user rule of the same name overrides these, matching the TS
/// `ruleCapability` funnel.
const BUILTIN_DEFAULT_RULES: [(&str, &str); 27] = [
	("go-add-cleanup", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/go-add-cleanup.md")),
	("go-bench-loop", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/go-bench-loop.md")),
	("go-exp-promoted", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/go-exp-promoted.md")),
	("go-ioutil", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/go-ioutil.md")),
	("go-join-hostport", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/go-join-hostport.md")),
	("go-new-expr", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/go-new-expr.md")),
	("go-rand-v2", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/go-rand-v2.md")),
	("go-range-int", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/go-range-int.md")),
	("rs-box-leak", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/rs-box-leak.md")),
	("rs-future-prelude", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/rs-future-prelude.md")),
	("rs-lazylock", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/rs-lazylock.md")),
	("rs-match-ergonomics", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/rs-match-ergonomics.md")),
	("rs-parking-lot", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/rs-parking-lot.md")),
	("rs-result-type", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/rs-result-type.md")),
	("ts-bare-catch", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-bare-catch.md")),
	("ts-import-type", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-import-type.md")),
	("ts-no-any", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-no-any.md")),
	("ts-no-deprecated-leftovers", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-no-deprecated-leftovers.md")),
	("ts-no-dynamic-import", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-no-dynamic-import.md")),
	("ts-no-inline-cast-access", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-no-inline-cast-access.md")),
	("ts-no-local-is-record", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-no-local-is-record.md")),
	("ts-no-return-type", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-no-return-type.md")),
	("ts-no-test-timers", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-no-test-timers.md")),
	("ts-no-tiny-functions", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-no-tiny-functions.md")),
	("ts-promise-with-resolvers", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-promise-with-resolvers.md")),
	("ts-redundant-clear-guard", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-redundant-clear-guard.md")),
	("ts-set-map", include_str!("../../../packages/coding-agent/src/discovery/builtin-rules/ts-set-map.md")),
];

fn builtin_default_rules() -> Vec<Rule> {
	BUILTIN_DEFAULT_RULES
		.iter()
		.map(|(name, content)| {
			let path = PathBuf::from(format!("builtin-defaults:{name}.md"));
			build_rule(name, content, &path, "builtin-defaults", Some(name), None)
		})
		.collect()
}

fn load_rule_file(path: &Path, provider: &str, name: Option<&str>, suffix: Option<&str>) -> Option<Rule> {
	let content = fs::read_to_string(path).ok()?;
	Some(build_rule(
		name.unwrap_or_else(|| path.file_name().and_then(|n| n.to_str()).unwrap_or("rule")),
		&content,
		path,
		provider,
		name,
		suffix,
	))
}

fn read_dir_files(dir: &Path, extensions: &[&str]) -> Vec<PathBuf> {
	let Ok(entries) = fs::read_dir(dir) else {
		return Vec::new();
	};
	let mut files: Vec<PathBuf> = entries
		.filter_map(|entry| entry.ok())
		.map(|entry| entry.path())
		.filter(|path| path.is_file())
		.filter(|path| {
			path.extension()
				.and_then(|ext| ext.to_str())
				.is_some_and(|ext| extensions.iter().any(|allowed| ext.eq_ignore_ascii_case(allowed)))
		})
		.collect();
	files.sort();
	files
}

fn canonical_dir_key(path: &Path) -> Option<PathBuf> {
	fs::canonicalize(path).ok()
}

fn read_dir_files_recursive(dir: &Path, suffix: &str) -> Vec<PathBuf> {
	let mut files = Vec::new();
	let mut visited = HashSet::new();
	if let Some(key) = canonical_dir_key(dir) {
		visited.insert(key);
	}
	let mut stack = vec![dir.to_path_buf()];
	while let Some(current) = stack.pop() {
		let Ok(entries) = fs::read_dir(&current) else {
			continue;
		};
		for entry in entries.flatten() {
			let path = entry.path();
			if path.is_dir() {
				// Follow symlinks but never revisit a directory target, so a
				// cycle cannot make discovery unbounded.
				let Some(key) = canonical_dir_key(&path) else {
					continue;
				};
				if visited.insert(key) {
					stack.push(path);
				}
			} else if path
				.file_name()
				.and_then(|name| name.to_str())
				.is_some_and(|name| name.ends_with(suffix))
			{
				files.push(path);
			}
		}
	}
	files.sort();
	files
}

fn home_dir() -> Option<PathBuf> {
	dirs::home_dir()
}

fn repo_root_or_home(cwd: &Path) -> PathBuf {
	let home = home_dir();
	let mut current = cwd.to_path_buf();
	loop {
		if current.join(".git").exists() || home.as_deref() == Some(current.as_path()) {
			return current;
		}
		let Some(parent) = current.parent() else {
			return current;
		};
		if parent == current {
			return current;
		}
		current = parent.to_path_buf();
	}
}

fn discover_rules(cwd: &Path, agent_dir: &Path) -> Vec<Rule> {
	let home = home_dir();
	let repo_root = repo_root_or_home(cwd);
	let mut rules: Vec<(u8, Rule)> = Vec::new(); // (priority, rule)
	let mut push = |priority: u8, rule: Rule| rules.push((priority, rule));

	// Native OMP: <cwd>/.omp/rules + <agent>/rules (priority 100).
	for path in read_dir_files(&cwd.join(".omp").join("rules"), &["md", "mdc"]) {
		if let Some(rule) = load_rule_file(&path, "native", None, None) {
			push(100, rule);
		}
	}
	for path in read_dir_files(&agent_dir.join("rules"), &["md", "mdc"]) {
		if let Some(rule) = load_rule_file(&path, "native", None, None) {
			push(100, rule);
		}
	}

	// `.agent/rules` and `.agents/rules`: walk up cwd → repo root, then ~.
	let mut current = cwd.to_path_buf();
	loop {
		if home.as_deref() != Some(current.as_path()) {
			for base in [".agent", ".agents"] {
				for path in read_dir_files(&current.join(base).join("rules"), &["md", "mdc"]) {
					if let Some(rule) = load_rule_file(&path, "agents", None, None) {
						push(70, rule);
					}
				}
			}
		}
		if current == repo_root {
			break;
		}
		let Some(parent) = current.parent() else {
			break;
		};
		current = parent.to_path_buf();
	}
	if let Some(home) = &home {
		for base in [".agent", ".agents"] {
			for path in read_dir_files(&home.join(base).join("rules"), &["md", "mdc"]) {
				if let Some(rule) = load_rule_file(&path, "agents", None, None) {
					push(70, rule);
				}
			}
		}
		// Cursor: ~/.cursor/rules + <cwd>/.cursor/rules (priority 60).
		for path in read_dir_files(&home.join(".cursor").join("rules"), &["mdc", "md"]) {
			if let Some(rule) = load_rule_file(&path, "cursor", None, None) {
				push(60, rule);
			}
		}
	}
	for path in read_dir_files(&cwd.join(".cursor").join("rules"), &["mdc", "md"]) {
		if let Some(rule) = load_rule_file(&path, "cursor", None, None) {
			push(60, rule);
		}
	}

	// Windsurf: ~/.codeium/windsurf/memories/global_rules.md + project rules.
	if let Some(home) = &home {
		let global = home.join(".codeium").join("windsurf").join("memories").join("global_rules.md");
		if let Some(rule) = load_rule_file(&global, "windsurf", Some("global_rules"), None) {
			push(50, rule);
		}
	}
	for path in read_dir_files(&cwd.join(".windsurf").join("rules"), &["md"]) {
		if let Some(rule) = load_rule_file(&path, "windsurf", None, None) {
			push(50, rule);
		}
	}

	// Cline: nearest `.clinerules` file or directory walking up from cwd.
	let mut current = cwd.to_path_buf();
	loop {
		let candidate = current.join(".clinerules");
		if candidate.is_file() {
			if let Some(rule) = load_rule_file(&candidate, "cline", Some("clinerules"), None) {
				push(40, rule);
			}
			break;
		}
		if candidate.is_dir() {
			for path in read_dir_files(&candidate, &["md"]) {
				if let Some(rule) = load_rule_file(&path, "cline", None, None) {
					push(40, rule);
				}
			}
			break;
		}
		let Some(parent) = current.parent() else {
			break;
		};
		current = parent.to_path_buf();
	}

	// GitHub Copilot: <cwd>/.github/instructions/**/*.instructions.md.
	for path in read_dir_files_recursive(&cwd.join(".github").join("instructions"), ".instructions.md") {
		let Some(content) = fs::read_to_string(&path).ok() else {
			continue;
		};
		let (front, _) = split_frontmatter(&content);
		let front = front
			.and_then(|text| yaml::parse(text).ok())
			.unwrap_or(yaml::Value::Map(Vec::new()));
		let apply_to = normalize_apply_to(front.get("applyTo"));
		let name = parse_rule_name(&path, Some(".instructions.md"));
		let mut rule = build_rule(&name, &content, &path, "github", Some(&name), Some(".instructions.md"));
		if apply_to.as_ref().is_some_and(|globs| globs.iter().any(|glob| is_always_apply_glob(glob))) {
			rule.always_apply = true;
			rule.globs = None;
		} else {
			rule.always_apply = false;
			rule.globs = apply_to.clone();
			rule.description = apply_to.as_ref().map_or_else(
				|| Some("GitHub Copilot instructions without applyTo metadata".to_string()),
				|globs| Some(format!("GitHub Copilot instructions for {}", globs.join(", "))),
			);
		}
		push(30, rule);
	}

	// `config.extensions` roots follow the existing native agent convention:
	// `<extension-root>/rules/{*.md,*.mdc}` as the `omp-plugins` provider at
	// priority 90. Marketplace-installed roots are still an unsupported source.
	let extension_roots = crate::config::Config::load_run(&[], Some(cwd))
		.ok()
		.and_then(|config| config.extensions)
		.unwrap_or_default();
	for root in extension_roots {
		let rules_dir = crate::dirs::absolute(cwd, &root).join("rules");
		for path in read_dir_files(&rules_dir, &["md", "mdc"]) {
			let suffix = path
				.extension()
				.and_then(|ext| ext.to_str())
				.map(|ext| format!(".{ext}"));
			if let Some(rule) = load_rule_file(&path, "omp-plugins", None, suffix.as_deref()) {
				push(90, rule);
			}
		}
	}

	// TypeScript `loadCapability<Rule>` includes the embedded `builtin-defaults`
	// provider at priority 1 unless `ttsr.builtinRules` is disabled.
	if setting_bool(cwd, "ttsr.builtinRules", true) {
		for rule in builtin_default_rules() {
			push(1, rule);
		}
	}

	// Capability discovery keeps the highest-priority provider for a duplicate
	// rule name, and earlier project scans outrank user scans within one level.
	rules.sort_by(|left, right| right.0.cmp(&left.0));
	let mut seen = HashSet::new();
	let mut out = Vec::new();
	for (_, rule) in rules {
		if seen.insert(rule.name.clone()) {
			out.push(rule);
		}
	}
	out
}

fn normalize_apply_to(value: Option<&yaml::Value>) -> Option<Vec<String>> {
	let mut raw: Vec<String> = Vec::new();
	match value {
		Some(yaml::Value::Str(text)) => raw.push(text.clone()),
		Some(yaml::Value::List(items)) => {
			for item in items {
				if let Some(text) = item.as_str() {
					raw.push(text.to_string());
				}
			}
		},
		_ => {},
	}
	let globs: Vec<String> = raw
		.into_iter()
		.flat_map(|text| text.split(',').map(str::trim).map(str::to_string).collect::<Vec<_>>())
		.filter(|glob| !glob.is_empty())
		.collect();
	(!globs.is_empty()).then_some(globs)
}

fn is_always_apply_glob(glob: &str) -> bool {
	matches!(glob, "*" | "**" | "**/*")
}

fn isolated_rule(rule_path: &Path, cwd: &Path) -> Result<Rule> {
	let resolved = resolve_path(cwd, rule_path);
	if !resolved.is_file() {
		return Err(Error::new(format!("Rule file not found: {}", resolved.display())));
	}
	let content = fs::read_to_string(&resolved)?;
	let name = parse_rule_name(&resolved, None);
	Ok(build_rule(
		&name,
		&content,
		&resolved,
		"ttsr-cli",
		Some(&name),
		None,
	))
}

// ── scope / glob matching ─────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct ToolScope {
	tool_name:  Option<String>,
	path_glob:  Option<String>,
}

#[derive(Clone, Debug)]
struct ScopePlan {
	allow_text:     bool,
	allow_thinking: bool,
	allow_any_tool: bool,
	tool_scopes:    Vec<ToolScope>,
}

fn parse_tool_scope_token(token: &str) -> Option<ToolScope> {
	let (head, path_pattern) = match token.find('(') {
		Some(index) => {
			let head = &token[..index];
			let rest = &token[index + 1..];
			let path = rest.strip_suffix(')')?;
			(head, Some(path.trim()).filter(|p| !p.is_empty()))
		},
		None => (token, None),
	};
	let head = head.trim();
	let lower = head.to_ascii_lowercase();
	let (has_tool_prefix, raw_name) = match lower.strip_prefix("tool:") {
		Some(name) => (true, Some(name)),
		None if lower == "tool" => (true, None),
		None => (false, Some(lower.as_str())),
	};
	let tool_name = raw_name.map(str::trim).filter(|name| {
		!name.is_empty()
			&& name
				.chars()
				.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
	});
	if has_tool_prefix && tool_name.is_none() && path_pattern.is_none() {
		return None;
	}
	if !has_tool_prefix && tool_name.is_none() {
		return None;
	}
	Some(ToolScope {
		tool_name: tool_name.map(str::to_string),
		path_glob: path_pattern.map(str::to_string),
	})
}

fn build_scope(rule: &Rule) -> ScopePlan {
	let Some(tokens) = &rule.scope else {
		return ScopePlan {
			allow_text:     true,
			allow_thinking: false,
			allow_any_tool: true,
			tool_scopes:    Vec::new(),
		};
	};
	let mut scope = ScopePlan {
		allow_text:     false,
		allow_thinking: false,
		allow_any_tool: false,
		tool_scopes:    Vec::new(),
	};
	for raw_token in tokens {
		let token = raw_token.trim();
		let lower = token.to_ascii_lowercase();
		if token.is_empty() {
			continue;
		}
		if lower == "text" {
			scope.allow_text = true;
			continue;
		}
		if lower == "thinking" {
			scope.allow_thinking = true;
			continue;
		}
		if lower == "tool" || lower == "toolcall" {
			scope.allow_any_tool = true;
			continue;
		}
		let Some(tool_scope) = parse_tool_scope_token(token) else {
			continue;
		};
		if tool_scope.tool_name.is_none() && tool_scope.path_glob.is_none() {
			scope.allow_any_tool = true;
			continue;
		}
		scope.tool_scopes.push(tool_scope);
	}
	scope
}

fn scope_is_reachable(scope: &ScopePlan) -> bool {
	scope.allow_text || scope.allow_thinking || scope.allow_any_tool || !scope.tool_scopes.is_empty()
}

fn glob_matches(pattern: &str, file_paths: &[String]) -> bool {
	let Ok(glob) = globset::Glob::new(pattern.trim()) else {
		return false;
	};
	for file_path in file_paths {
		let normalized = normalize_slashes(file_path);
		if glob.compile_matcher().is_match(&normalized) {
			return true;
		}
		let basename = normalized.rsplit('/').next().unwrap_or(&normalized);
		if basename != normalized && glob.compile_matcher().is_match(basename) {
			return true;
		}
	}
	false
}

fn globs_match(globs: &Option<Vec<String>>, file_paths: &[String]) -> bool {
	let Some(globs) = globs else {
		return false;
	};
	globs.iter().any(|glob| glob_matches(glob, file_paths))
}

fn global_paths_match(global_globs: &Option<Vec<String>>, file_paths: &[String]) -> bool {
	match global_globs {
		None => true,
		Some(globs) if globs.is_empty() => true,
		Some(globs) => globs.iter().any(|glob| glob_matches(glob, file_paths)),
	}
}

fn scope_matches(scope: &ScopePlan, source: &str, tool: Option<&str>, file_paths: &[String]) -> bool {
	match source {
		"text" => scope.allow_text,
		"thinking" => scope.allow_thinking,
		_ => {
			if scope.allow_any_tool {
				return true;
			}
			let tool_name = tool.map(|value| value.trim().to_ascii_lowercase());
			scope.tool_scopes.iter().any(|tool_scope| {
				if let Some(expected) = &tool_scope.tool_name
					&& tool_name.as_deref() != Some(expected.as_str())
				{
					return false;
				}
				if let Some(pattern) = &tool_scope.path_glob
					&& !glob_matches(pattern, file_paths)
				{
					return false;
				}
				true
			})
		},
	}
}

fn derive_lang(file_paths: &[String]) -> Option<String> {
	for file_path in file_paths {
		let path = normalize_slashes(file_path);
		if let Some(ext) = Path::new(&path).extension().and_then(|ext| ext.to_str()) {
			if !ext.is_empty() {
				return Some(ext.to_ascii_lowercase());
			}
		}
	}
	None
}

fn ast_lang(lang: &str) -> Option<pi_ast::SupportLang> {
	pi_ast::SupportLang::from_alias(lang).or_else(|| {
		pi_ast::SupportLang::from_alias(&format!("*.{lang}"))
			.or_else(|| pi_ast::SupportLang::from_path(Path::new(&format!("x.{lang}"))))
	})
}

fn ast_pattern_matches(pattern: &str, source: &str, lang: pi_ast::SupportLang) -> bool {
	match pi_ast::ops::compile_search_patterns(pattern, lang) {
		Ok(patterns) => !pi_ast::ops::collect_matches(source, lang, &patterns).is_empty(),
		Err(_) => false,
	}
}

fn ast_matches(rule: &Rule, source: &str, lang: pi_ast::SupportLang) -> Vec<String> {
	let mut matched = Vec::new();
	for pattern in rule.ast_condition.clone().unwrap_or_default() {
		if pattern.trim().is_empty() {
			continue;
		}
		if ast_pattern_matches(&pattern, source, lang) {
			matched.push(pattern);
		}
	}
	matched
}

// ── TTSR manager equivalent ───────────────────────────────────────────────

#[derive(Clone, Debug)]
struct TtsrEntry {
	rule:            Rule,
	conditions:      Vec<String>,
	ast_conditions:  Vec<String>,
	scope:           ScopePlan,
	global_globs:    Option<Vec<String>>,
}

fn compile_rule_rules(cwd: &Path, agent_dir: &Path) -> Vec<Rule> {
	// TS pipeline: loadCapability<Rule> providers (builtin, agents, cursor,
	// windsurf, cline, github, builtin-defaults, claude-plugins, omp-plugins)
	// -> name dedupe by provider priority -> TtsrManager.addRule.
	// This native runtime embeds builtin-defaults, on-disk providers, and
	// configured `config.extensions` rule roots. Marketplace-installed plugin
	// roots remain an explicit unsupported source (no catalog loader), and are
	// never partially merged.
	let enabled = setting_bool(cwd, "ttsr.enabled", true);
	if !enabled {
		return Vec::new();
	}
	let disabled: HashSet<String> = setting_strings(cwd, "ttsr.disabledRules").into_iter().collect();
	let mut registered: Vec<Rule> = Vec::new();
	let mut seen = HashSet::new();
	for rule in discover_rules(cwd, agent_dir) {
		if disabled.contains(&rule.name) || !seen.insert(rule.name.clone()) {
			continue;
		}
		if register_rule(&rule).is_some() {
			registered.push(rule);
		}
	}
	registered
}

fn register_rule(rule: &Rule) -> Option<TtsrEntry> {
	let conditions: Vec<String> = rule
		.condition
		.clone()
		.unwrap_or_default()
		.into_iter()
		.filter(|pattern| pi_grep_pattern::compile_strict(pattern, false, false).is_ok())
		.collect();
	let ast_conditions: Vec<String> = rule
		.ast_condition
		.clone()
		.unwrap_or_default()
		.into_iter()
		.map(|pattern| pattern.trim().to_string())
		.filter(|pattern| !pattern.is_empty())
		.collect();
	if conditions.is_empty() && ast_conditions.is_empty() {
		return None;
	}
	let scope = build_scope(rule);
	if !scope_is_reachable(&scope) {
		return None;
	}
	Some(TtsrEntry {
		rule: rule.clone(),
		conditions,
		ast_conditions,
		scope,
		global_globs: rule.globs.clone(),
	})
}

fn entry_matches_snapshot(entry: &TtsrEntry, snapshot: &str, source: &str, tool: Option<&str>, file_paths: &[String]) -> bool {
	if !scope_matches(&entry.scope, source, tool, file_paths) {
		return false;
	}
	if !global_paths_match(&entry.global_globs, file_paths) {
		return false;
	}
	entry
		.conditions
		.iter()
		.any(|pattern| pi_grep_pattern::pattern_matches(pattern, snapshot, false, false))
}

fn entry_matches_ast(entry: &TtsrEntry, snapshot: &str, source: &str, tool: Option<&str>, file_paths: &[String]) -> bool {
	if entry.ast_conditions.is_empty() || source != "tool" {
		return false;
	}
	if !scope_matches(&entry.scope, source, tool, file_paths) || !global_paths_match(&entry.global_globs, file_paths) {
		return false;
	}
	let Some(lang) = derive_lang(file_paths).and_then(|lang| ast_lang(&lang)) else {
		return false;
	};
	entry
		.ast_conditions
		.iter()
		.any(|pattern| ast_pattern_matches(pattern, snapshot, lang))
}

// ───────────────────────────────────────────────────────────────────────────
// `omp ttsr`
// ───────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct TtsrOptions {
	pub action:       Option<String>,
	pub snippet:      Option<String>,
	pub file:         Option<String>,
	pub rule:         Option<PathBuf>,
	pub source:       Option<String>,
	pub tool:         Option<String>,
	pub path:         Option<String>,
	pub verbose:      bool,
	pub json:         bool,
	pub no_gitignore: bool,
	pub max_bytes:    Option<u64>,
}

const TTSR_SOURCES: [&str; 3] = ["text", "thinking", "tool"];

/// Escape terminal control characters from project/DB-provided text before
/// human-readable rendering. ESC/OSC/CSI and other C0/C1 controls are emitted
/// as visible `\u{..}` escapes; JSON serialization is untouched.
fn escape_terminal_text(input: &str, keep_newlines: bool) -> String {
	let mut out = String::with_capacity(input.len());
	for ch in input.chars() {
		if ch.is_control() {
			if keep_newlines && ch == '\n' {
				out.push('\n');
			} else if keep_newlines && ch == '\t' {
				out.push('\t');
			} else {
				use std::fmt::Write as _;
				let _ = write!(out, "\\u{{{:x}}}", ch as u32);
			}
		} else {
			out.push(ch);
		}
	}
	out
}

fn preview_snippet(text: &str) -> String {
	let single = text.split_whitespace().collect::<Vec<_>>().join(" ");
	if single.chars().count() > 80 {
		format!("{}…", single.chars().take(77).collect::<String>())
	} else {
		single
	}
}

fn read_snippet(opts: &TtsrOptions, cwd: &Path) -> Result<String> {
	if let Some(file) = &opts.file {
		if file == "-" {
			let mut buffer = String::new();
			io::stdin().read_to_string(&mut buffer)?;
			return Ok(buffer);
		}
		let resolved = resolve_path(cwd, Path::new(file));
		if !resolved.is_file() {
			return Err(Error::new(format!("Snippet file not found: {}", resolved.display())));
		}
		return fs::read_to_string(resolved).map_err(Into::into);
	}
	if let Some(snippet) = &opts.snippet {
		return Ok(snippet.clone());
	}
	if !io::stdin().is_terminal() {
		let mut buffer = String::new();
		io::stdin().read_to_string(&mut buffer)?;
		return Ok(buffer);
	}
	return Err(Error::new(
		"No snippet provided. Pass inline text, --file <path>, or pipe via --file -.",
	));
}

fn source_file_extension(path: &str) -> bool {
	let ext = Path::new(path)
		.extension()
		.and_then(|ext| ext.to_str())
		.map(str::to_ascii_lowercase)
		.unwrap_or_default();
	matches!(
		ext.as_str(),
		"ts" | "tsx"
			| "js" | "jsx"
			| "mjs" | "cjs"
			| "rs" | "py"
			| "go" | "java"
			| "kt" | "swift"
			| "c" | "cc"
			| "cpp" | "h"
			| "hpp" | "rb"
			| "php" | "lua"
			| "css" | "scss"
			| "html" | "json"
			| "yaml" | "yml"
			| "toml" | "md"
			| "mdc" | "cs"
			| "razor" | "cshtml"
			| "fs" | "fsx"
			| "vb" | "sh"
			| "bash" | "sql"
			| "zig" | "dart"
			| "scala" | "ex"
			| "exs" | "proto"
			| "tf"
	)
}

fn rule_detail(entry: &Rule, matched_regex: Vec<String>, matched_ast: Vec<String>, skipped_ast: Option<&str>) -> Value {
	let mut detail = json!({
		"name": entry.name,
		"path": display_path(&entry.path),
		"matched": { "regex": matched_regex, "ast": matched_ast },
		"defined": {
			"regex": entry.condition.clone().unwrap_or_default(),
			"ast": entry.ast_condition.clone().unwrap_or_default(),
		},
		"sourceProvider": entry.provider,
	});
	if let Some(skipped) = skipped_ast {
		detail["skippedAst"] = json!(skipped);
	}
	detail
}

fn render_rule_detail(out: &mut dyn Write, detail: &Value, hit: bool) -> Result<()> {
	let mark = if hit { "✓" } else { "✗" };
	let name = escape_terminal_text(detail["name"].as_str().unwrap_or("?"), false);
	let mut parts: Vec<String> = Vec::new();
	let regex = detail[if hit { "matched" } else { "defined" }]
		.get("regex")
		.and_then(Value::as_array);
	let ast = detail[if hit { "matched" } else { "defined" }]
		.get("ast")
		.and_then(Value::as_array);
	if let Some(regex) = regex {
		let patterns: Vec<String> = regex
			.iter()
			.filter_map(Value::as_str)
			.map(|pattern| format!("/{pattern}/"))
			.collect();
		if !patterns.is_empty() {
			parts.push(format!("condition: {}", patterns.join(", ")));
		}
	}
	if let Some(ast) = ast {
		let patterns: Vec<String> = ast.iter().filter_map(Value::as_str).map(str::to_string).collect();
		if !patterns.is_empty() {
			parts.push(format!("astCondition: {}", patterns.join(", ")));
		}
	}
	if let Some(skipped) = detail["skippedAst"].as_str() {
		parts.push(format!("astCondition: {skipped}"));
	}
	let label = if parts.is_empty() {
		"no active conditions".to_string()
	} else {
		escape_terminal_text(&parts.join("  "), false)
	};
	let provider = detail["sourceProvider"]
		.as_str()
		.map(|provider| format!(" [{}]", escape_terminal_text(provider, false)))
		.unwrap_or_default();
	write_line(out, &format!("  {mark} {name}  {label}{provider}"))
}

fn run_ttsr_test(opts: &TtsrOptions, cwd: &Path, out: &mut dyn Write, err: &mut dyn Write) -> Result<u8> {
	// `ttsr test <path>` treats an existing positional path as `--file`, like
	// the TypeScript command adapter does before calling the test runner.
	let mut effective = opts.clone();
	if effective.file.is_none() {
		let candidate = effective.snippet.clone();
		if let Some(candidate) = candidate {
			let resolved = resolve_path(cwd, Path::new(&candidate));
			if resolved.exists() {
				effective.file = Some(display_path(&resolved));
				effective.snippet = None;
			}
		}
	}
	let opts = &effective;
	if let Some(source) = &opts.source
		&& !TTSR_SOURCES.contains(&source.as_str())
	{
		return Err(Error::new(format!(
			"Invalid --source: {source}. Expected one of: {}",
			TTSR_SOURCES.join(", ")
		)));
	}

	let snippet = read_snippet(opts, cwd)?;
	let file_path = opts.path.clone().or_else(|| {
		opts.file
			.as_deref()
			.filter(|file| *file != "-")
			.map(|file| display_path(&resolve_path(cwd, Path::new(file))))
	});
	let source = opts.source.clone().unwrap_or_else(|| {
		if file_path.as_deref().is_some_and(source_file_extension) {
			"tool".to_string()
		} else {
			"text".to_string()
		}
	});
	let tool = opts
		.tool
		.clone()
		.or_else(|| (source == "tool").then(|| "edit".to_string()));
	let inference_note = if opts.source.is_none() && file_path.is_some() && source == "text" {
		let ext = file_path
			.as_deref()
			.and_then(|file| Path::new(file).extension().and_then(|ext| ext.to_str()))
			.map(|ext| format!(".{ext}"))
			.unwrap_or_else(|| file_path.clone().unwrap_or_default());
		Some(format!(
			"inferred --source text from '{ext}' (not in the source-file extension set); pass --source tool --tool edit to evaluate tool-scoped rules"
		))
	} else {
		None
	};

	let agent_dir = dirs::agent_dir();
	let (rules, entries) = if let Some(rule_path) = &opts.rule {
		let rule = isolated_rule(rule_path, cwd)?;
		let Some(entry) = register_rule(&rule) else {
			return Err(Error::new(format!(
				"Rule \"{}\" has no usable TTSR condition. Add a `condition` (regex) or `astCondition` (ast-grep pattern) to its frontmatter.",
				rule.name
			)));
		};
		(vec![rule], vec![entry])
	} else {
		let mut rules = Vec::new();
		let mut entries = Vec::new();
		for rule in compile_rule_rules(cwd, &agent_dir) {
			if let Some(entry) = register_rule(&rule) {
				rules.push(rule);
				entries.push(entry);
			}
		}
		(rules, entries)
	};

	if rules.is_empty() {
		let message = if opts.rule.is_some() {
			"Rule registered but produced no TTSR entry."
		} else {
			"No TTSR rules registered for this project. Add a `condition` or `astCondition` to a rule file, then re-run."
		};
		if opts.json {
			write_line(out, &serde_json::to_string(&json!({ "error": message }))?)?;
		} else {
			write_line(err, message)?;
		}
		return Ok(1);
	}

	let file_paths: Vec<String> = file_path.iter().cloned().collect();
	let mut hit_names: HashSet<String> = HashSet::new();
	for entry in &entries {
		if entry_matches_snapshot(entry, &snippet, &source, tool.as_deref(), &file_paths) {
			hit_names.insert(entry.rule.name.clone());
		}
	}
	if source == "tool" && !file_paths.is_empty() {
		for entry in &entries {
			if entry_matches_ast(entry, &snippet, &source, tool.as_deref(), &file_paths) {
				hit_names.insert(entry.rule.name.clone());
			}
		}
	}

	let lang_name = derive_lang(&file_paths);
	let lang = lang_name.as_deref().and_then(ast_lang);
	let ast_eligible = source == "tool" && lang_name.is_some();
	let mut triggered = Vec::new();
	let mut not_triggered = Vec::new();
	for rule in &rules {
		let mut matched_regex = Vec::new();
		for pattern in rule.condition.clone().unwrap_or_default() {
			if pi_grep_pattern::pattern_matches(&pattern, &snippet, false, false) {
				matched_regex.push(pattern);
			}
		}
		let matched_ast = if ast_eligible {
			lang.map_or_else(Vec::new, |lang| ast_matches(rule, &snippet, lang))
		} else {
			Vec::new()
		};
		let skipped = if !ast_eligible && rule.ast_condition.as_ref().is_some_and(|items| !items.is_empty()) {
			Some("astCondition requires --source tool and a --path with a file extension")
		} else {
			None
		};
		let detail = rule_detail(rule, matched_regex, matched_ast, skipped);
		if hit_names.contains(&rule.name) {
			triggered.push(detail);
		} else {
			not_triggered.push(detail);
		}
	}

	// `JSON.stringify` drops undefined keys, so `tool`/`filePath`/`inferenceNote`
	// are omitted rather than emitted as null.
	let mut report = serde_json::Map::new();
	report.insert("source".to_string(), json!(source));
	if let Some(tool) = &tool {
		report.insert("tool".to_string(), json!(tool));
	}
	if let Some(path) = &file_path {
		report.insert("filePath".to_string(), json!(path));
	}
	report.insert("snippetPreview".to_string(), json!(preview_snippet(&snippet)));
	report.insert("snippetBytes".to_string(), json!(snippet.encode_utf16().count()));
	report.insert("evaluated".to_string(), json!(rules.len()));
	report.insert("triggered".to_string(), json!(triggered));
	report.insert("notTriggered".to_string(), json!(not_triggered));
	if let Some(note) = &inference_note {
		report.insert("inferenceNote".to_string(), json!(note));
	}
	let report = Value::Object(report);

	if opts.json {
		write_line(out, &serde_json::to_string(&report)?)?;
		return Ok(0);
	}

	let ctx_label = if source == "tool" {
		format!(
			"tool:{}",
			escape_terminal_text(tool.as_deref().unwrap_or("?"), false)
		)
	} else {
		escape_terminal_text(&source, false)
	};
	let path_label = file_path
		.as_deref()
		.map(|path| format!(" path={}", escape_terminal_text(path, false)))
		.unwrap_or_default();
	write_line(
		out,
		&format!("TTSR test — source={ctx_label}{path_label} snippet={}b", report["snippetBytes"]),
	)?;
	write_line(
		out,
		&format!(
			"  \"{}\"\n",
			escape_terminal_text(&preview_snippet(&snippet), false)
		),
	)?;
	if let Some(note) = inference_note {
		write_line(out, &format!("note: {}\n", escape_terminal_text(&note, false)))?;
	}

	if triggered.is_empty() {
		write_line(out, &format!("No rules triggered. (evaluated {})", rules.len()))?;
	} else {
		write_line(out, &format!("Triggered ({})\n", triggered.len()))?;
		for detail in &triggered {
			render_rule_detail(out, detail, true)?;
		}
	}
	if opts.verbose && !not_triggered.is_empty() {
		write_line(out, &format!("\nNot triggered ({})", not_triggered.len()))?;
		for detail in &not_triggered {
			render_rule_detail(out, detail, false)?;
		}
	}
	Ok(if opts.rule.is_some() && triggered.is_empty() { 1 } else { 0 })
}

fn run_ttsr_list(cwd: &Path, out: &mut dyn Write, json_mode: bool) -> Result<u8> {
	// TS `loadProjectTtsrRules` returns `manager.getRules()`, i.e. only rules
	// `TtsrManager.addRule` accepted (non-empty and valid TTSR conditions).
	let rules: Vec<Rule> = compile_rule_rules(cwd, &dirs::agent_dir())
		.into_iter()
		.filter(|rule| register_rule(rule).is_some())
		.collect();
	if json_mode {
		let items: Vec<Value> = rules
			.iter()
			.map(|rule| {
				let mut item = serde_json::Map::new();
				item.insert("name".to_string(), json!(rule.name));
				item.insert("path".to_string(), json!(display_path(&rule.path)));
				item.insert("provider".to_string(), json!(rule.provider));
				item.insert("condition".to_string(), json!(rule.condition.clone().unwrap_or_default()));
				item.insert("astCondition".to_string(), json!(rule.ast_condition.clone().unwrap_or_default()));
				item.insert("scope".to_string(), json!(rule.scope.clone().unwrap_or_default()));
				item.insert("globs".to_string(), json!(rule.globs.clone().unwrap_or_default()));
				if let Some(description) = &rule.description {
					item.insert("description".to_string(), json!(description));
				}
				Value::Object(item)
			})
			.collect();
		write_line(out, &serde_json::to_string(&items)?)?;
		return Ok(0);
	}
	if rules.is_empty() {
		write_line(out, "No TTSR rules registered for this project.")?;
		return Ok(0);
	}
	write_line(out, &format!("TTSR rules ({})", rules.len()))?;
	for rule in &rules {
		let mut parts = Vec::new();
		if let Some(condition) = rule.condition.as_ref().filter(|items| !items.is_empty()) {
			parts.push(format!("condition: {}", condition.join(", ")));
		}
		if let Some(ast) = rule.ast_condition.as_ref().filter(|items| !items.is_empty()) {
			parts.push(format!("astCondition: {}", ast.join(", ")));
		}
		if let Some(scope) = rule.scope.as_ref().filter(|items| !items.is_empty()) {
			parts.push(format!("scope: {}", scope.join(", ")));
		}
		if let Some(globs) = rule.globs.as_ref().filter(|items| !items.is_empty()) {
			parts.push(format!("globs: {}", globs.join(", ")));
		}
		let cond = if parts.is_empty() {
			"no conditions".to_string()
		} else {
			parts.join("  ")
		};
		write_line(
			out,
			&format!(
				"  {} [{}] {}",
				escape_terminal_text(&rule.name, false),
				escape_terminal_text(&rule.provider, false),
				escape_terminal_text(&cond, false)
			),
		)?;
		if let Some(description) = &rule.description {
			write_line(out, &format!("    {}", escape_terminal_text(description, false)))?;
		}
	}
	Ok(0)
}

fn scan_rule_plans(rules: &[Rule]) -> Vec<(Rule, ScopePlan, Vec<String>, Vec<String>, Option<Vec<String>>)> {
	let mut plans = Vec::new();
	for rule in rules {
		let scope = build_scope(rule);
		let default_tool_scope = rule.scope.as_ref().is_none_or(|tokens| tokens.is_empty());
		// Scan plans treat text/thinking tokens as irrelevant and only care about
		// tool scopes; a rule without conditions cannot match anything.
		let mut scopes = Vec::new();
		let mut plan_default = default_tool_scope;
		for raw in rule.scope.clone().unwrap_or_default() {
			let token = raw.trim().to_string();
			let lower = token.to_ascii_lowercase();
			if token.is_empty() || lower == "text" || lower == "thinking" {
				continue;
			}
			if lower == "tool" || lower == "toolcall" {
				scopes.push(ToolScope { tool_name: None, path_glob: None });
				continue;
			}
			let Some(tool_scope) = parse_tool_scope_token(&token) else {
				continue;
			};
			if tool_scope.tool_name.is_none() && tool_scope.path_glob.is_none() {
				plan_default = true;
				continue;
			}
			scopes.push(tool_scope);
		}
		let regex_conditions: Vec<String> = rule
			.condition
			.clone()
			.unwrap_or_default()
			.into_iter()
			.filter(|pattern| pi_grep_pattern::compile_strict(pattern, false, false).is_ok())
			.collect();
		let ast_conditions: Vec<String> = rule
			.ast_condition
			.clone()
			.unwrap_or_default()
			.into_iter()
			.map(|pattern| pattern.trim().to_string())
			.filter(|pattern| !pattern.is_empty())
			.collect();
		if regex_conditions.is_empty() && ast_conditions.is_empty() {
			continue;
		}
		let plan_scope = ScopePlan {
			allow_text:     true,
			allow_thinking: true,
			allow_any_tool: plan_default,
			tool_scopes:    scopes,
		};
		plans.push((rule.clone(), plan_scope, regex_conditions, ast_conditions, rule.globs.clone()));
	}
	plans
}

fn scan_plan_matches_path(global_globs: &Option<Vec<String>>, file_paths: &[String]) -> bool {
	global_globs.is_none() || globs_match(global_globs, file_paths)
}

fn scan_plan_matches_tool_scope(scope: &ScopePlan, globs: &Option<Vec<String>>, file_paths: &[String]) -> bool {
	if !scan_plan_matches_path(globs, file_paths) {
		return false;
	}
	if scope.allow_any_tool {
		return true;
	}
	for tool_scope in &scope.tool_scopes {
		if let Some(pattern) = &tool_scope.path_glob
			&& !glob_matches(pattern, file_paths)
		{
			continue;
		}
		if tool_scope.tool_name.is_none()
			|| tool_scope.tool_name.as_deref() == Some("edit")
			|| tool_scope.tool_name.as_deref() == Some("write")
		{
			return true;
		}
	}
	false
}

#[derive(Default, Clone, Copy)]
struct ScanSkipped {
	binary:           u64,
	large:            u64,
	unreadable:       u64,
	no_relevant_rules: u64,
}

impl ScanSkipped {
	fn total(self) -> u64 {
		self.binary + self.large + self.unreadable + self.no_relevant_rules
	}
}

fn discover_scan_files(scan_dir: &Path, gitignore: bool) -> Vec<pi_walker::FileCandidate> {
	let request = pi_walker::WalkRequest::new(scan_dir)
		.hidden(true)
		.gitignore(gitignore)
		.skip_git(true)
		.detail(pi_walker::WalkDetail::Full);
	let Ok(mut files) = request.collect_file_candidates() else {
		return Vec::new();
	};
	files.sort_by(|left, right| left.relative.cmp(&right.relative));
	files
}

fn read_scan_file(path: &Path, max_bytes: u64, known_size: Option<f64>) -> std::result::Result<String, &'static str> {
	let metadata = fs::metadata(path).map_err(|_| "unreadable")?;
	let size = known_size.map_or(metadata.len(), |size| size.max(0.0) as u64);
	if max_bytes > 0 && size > max_bytes {
		return Err("large");
	}
	let mut file = fs::File::open(path).map_err(|_| "unreadable")?;
	let mut bytes = Vec::new();
	file.read_to_end(&mut bytes).map_err(|_| "unreadable")?;
	let probe_end = bytes.len().min(BINARY_PROBE_BYTES);
	if bytes[..probe_end].contains(&0) {
		return Err("binary");
	}
	String::from_utf8(bytes).map_err(|_| "unreadable")
}

fn run_ttsr_scan(opts: &TtsrOptions, cwd: &Path, out: &mut dyn Write, err: &mut dyn Write) -> Result<u8> {
	let scan_dir = opts
		.snippet
		.as_deref()
		.map_or_else(|| cwd.to_path_buf(), |directory| resolve_path(cwd, Path::new(directory)));
	if !scan_dir.exists() {
		if opts.json {
			write_line(out, &serde_json::to_string(&json!({ "error": format!("Directory not found: {}", scan_dir.display()) }))?)?;
		} else {
			write_line(err, &format!("error: scan directory not found: {}", scan_dir.display()))?;
		}
		return Ok(1);
	}

	let rules = if let Some(rule_path) = &opts.rule {
		let rule = isolated_rule(rule_path, cwd)?;
		let filtered: Vec<Rule> = if rule.condition.as_ref().is_some_and(|items| !items.is_empty())
			|| rule.ast_condition.as_ref().is_some_and(|items| !items.is_empty())
		{
			vec![rule]
		} else {
			Vec::new()
		};
		filtered
	} else {
		let enabled = setting_bool(cwd, "ttsr.enabled", true);
		if !enabled {
			Vec::new()
		} else {
			let disabled: HashSet<String> = setting_strings(cwd, "ttsr.disabledRules").into_iter().collect();
			discover_rules(cwd, &dirs::agent_dir())
				.into_iter()
				.filter(|rule| !disabled.contains(&rule.name))
				.filter(|rule| {
					rule.condition.as_ref().is_some_and(|items| !items.is_empty())
						|| rule.ast_condition.as_ref().is_some_and(|items| !items.is_empty())
				})
				.collect()
		}
	};

	if rules.is_empty() {
		let message = if opts.rule.is_some() {
			"Rule registered but produced no TTSR entry."
		} else {
			"No TTSR rules registered for this project."
		};
		if opts.json {
			write_line(out, &serde_json::to_string(&json!({ "error": message }))?)?;
		} else {
			write_line(err, message)?;
		}
		return Ok(1);
	}

	let plans = scan_rule_plans(&rules);
	if plans.is_empty() {
		let message = if opts.rule.is_some() {
			"Rule registered but produced no usable TTSR condition."
		} else {
			"No usable TTSR rules registered for this project."
		};
		if opts.json {
			write_line(out, &serde_json::to_string(&json!({ "error": message }))?)?;
		} else {
			write_line(err, message)?;
		}
		return Ok(1);
	}

	let gitignore = !opts.no_gitignore;
	let max_bytes = opts.max_bytes.unwrap_or(DEFAULT_MAX_SCAN_BYTES);
	let include_details = opts.json || opts.verbose;
	let files = discover_scan_files(&scan_dir, gitignore);

	if files.is_empty() {
		let scan_label = escape_terminal_text(&scan_dir.to_string_lossy(), false);
		let message = format!("No files found to scan in {scan_label}");
		if opts.json {
			write_line(
				out,
				&serde_json::to_string(&json!({
					"files": [],
					"summary": {
						"totalFiles": 0,
						"scannedFiles": 0,
						"matchedFiles": 0,
						"totalMatches": 0,
						"evaluatedRules": plans.len(),
						"skippedFiles": 0,
						"skipped": { "binary": 0, "large": 0, "unreadable": 0, "noRelevantRules": 0 },
						"gitignore": gitignore,
						"maxBytes": max_bytes,
					},
				}))?,
			)?;
		} else {
			write_line(out, &message)?;
		}
		return Ok(0);
	}

	let mut file_results: Vec<Value> = Vec::new();
	let mut skipped = ScanSkipped::default();
	let mut scanned_files = 0usize;
	let mut matched_files = 0usize;
	let mut total_matches = 0usize;

	for candidate in &files {
		let abs_path = scan_dir.join(&candidate.relative);
		let rel_to_project = normalize_slashes(
			&abs_path
				.strip_prefix(cwd)
				.map_or_else(|_| abs_path.clone(), Path::to_path_buf)
				.display()
				.to_string(),
		);
		let basename = abs_path
			.file_name()
			.and_then(|name| name.to_str())
			.unwrap_or_default()
			.to_string();
		let file_paths = vec![
			normalize_slashes(&display_path(&abs_path)),
			rel_to_project.clone(),
			basename.clone(),
		];

		let relevant: Vec<&(Rule, ScopePlan, Vec<String>, Vec<String>, Option<Vec<String>>)> = plans
			.iter()
			.filter(|plan| scan_plan_matches_tool_scope(&plan.1, &plan.4, &file_paths))
			.collect();
		if relevant.is_empty() {
			skipped.no_relevant_rules += 1;
			continue;
		}

		let content = match read_scan_file(&abs_path, max_bytes, candidate.size) {
			Ok(content) => content,
			Err(reason) => {
				match reason {
					"binary" => skipped.binary += 1,
					"large" => skipped.large += 1,
					_ => skipped.unreadable += 1,
				}
				continue;
			},
		};
		let lang = derive_lang(&file_paths).and_then(|lang| ast_lang(&lang));
		scanned_files += 1;

		let mut details: Vec<Value> = Vec::new();
		let mut file_match_count = 0usize;
		for plan in &relevant {
			let rule = &plan.0;
			let regex_conditions = &plan.2;
			let ast_conditions = &plan.3;
			let regex_hit = regex_conditions
				.iter()
				.any(|pattern| pi_grep_pattern::pattern_matches(pattern, &content, false, false));
			let mut matched_regex = Vec::new();
			if include_details {
				matched_regex = regex_conditions
					.iter()
					.filter(|pattern| pi_grep_pattern::pattern_matches(pattern, &content, false, false))
					.cloned()
					.collect();
			}
			let mut matched_ast = Vec::new();
			let mut ast_hit = false;
			if (include_details || !regex_hit) && !ast_conditions.is_empty() {
				if let Some(lang) = lang {
					if include_details {
						matched_ast = ast_conditions
							.iter()
							.filter(|pattern| ast_pattern_matches(pattern, &content, lang))
							.cloned()
							.collect();
						ast_hit = !matched_ast.is_empty();
					} else {
						ast_hit = ast_conditions
							.iter()
							.any(|pattern| ast_pattern_matches(pattern, &content, lang));
					}
				}
			}
			if !regex_hit && !ast_hit {
				continue;
			}
			file_match_count += 1;
			if include_details {
				let mut detail = json!({
					"name": rule.name,
					"path": display_path(&rule.path),
					"matched": { "regex": matched_regex, "ast": matched_ast },
					"defined": {
						"regex": rule.condition.clone().unwrap_or_default(),
						"ast": rule.ast_condition.clone().unwrap_or_default(),
					},
				});
				detail["sourceProvider"] = json!(rule.provider);
				details.push(detail);
			}
		}

		if file_match_count > 0 {
			matched_files += 1;
			total_matches += file_match_count;
			if include_details {
				let matches: Vec<Value> = details
					.iter()
					.map(|detail| {
						json!({
							"name": detail["name"],
							"path": detail["path"],
							"matched": detail["matched"],
						})
					})
					.collect();
				file_results.push(json!({ "filePath": rel_to_project, "matches": matches }));
			}
		}
	}

	if opts.json {
		write_line(
			out,
			&serde_json::to_string(&json!({
				"files": file_results,
				"summary": {
					"totalFiles": files.len(),
					"scannedFiles": scanned_files,
					"matchedFiles": matched_files,
					"totalMatches": total_matches,
					"evaluatedRules": plans.len(),
					"skippedFiles": skipped.total(),
					"skipped": {
						"binary": skipped.binary,
						"large": skipped.large,
						"unreadable": skipped.unreadable,
						"noRelevantRules": skipped.no_relevant_rules,
					},
					"gitignore": gitignore,
					"maxBytes": max_bytes,
				},
			}))?,
		)?;
		return Ok(0);
	}

	write_line(
		out,
		&format!(
			"TTSR scan — directory={} files={} scanned={} rules={} gitignore={} max-bytes={}",
			escape_terminal_text(&scan_dir.to_string_lossy(), false),
			files.len(),
			scanned_files,
			plans.len(),
			if gitignore { "on" } else { "off" },
			if max_bytes == 0 { "off".to_string() } else { max_bytes.to_string() },
		),
	)?;
	if skipped.total() > 0 {
		write_line(
			out,
			&format!(
				"  skipped: binary={} large={} unreadable={} no-relevant-rules={}",
				skipped.binary, skipped.large, skipped.unreadable, skipped.no_relevant_rules
			),
		)?;
	}
	if matched_files == 0 {
		write_line(
			out,
			&format!(
				"No rule matches found. (evaluated {} rules on {}/{} files)",
				rules.len(),
				scanned_files,
				files.len()
			),
		)?;
		return Ok(0);
	}
	write_line(
		out,
		&format!("Found violations/matches: ({total_matches} matches across {matched_files} files)"),
	)?;
	if !include_details {
		write_line(out, "  rerun with --verbose to list matched files and conditions")?;
		return Ok(0);
	}
	write_line(out, "")?;
	for result in &file_results {
		write_line(
			out,
			&escape_terminal_text(result["filePath"].as_str().unwrap_or_default(), false),
		)?;
		if let Some(matches) = result["matches"].as_array() {
			for detail in matches {
				render_rule_detail(out, detail, true)?;
			}
		}
		write_line(out, "")?;
	}
	Ok(0)
}

pub fn run_ttsr(opts: &TtsrOptions, cwd: &Path, out: &mut dyn Write, err: &mut dyn Write) -> Result<u8> {
	match opts.action.as_deref().unwrap_or("list") {
		"test" => run_ttsr_test(opts, cwd, out, err),
		"list" => run_ttsr_list(cwd, out, opts.json),
		"scan" => run_ttsr_scan(opts, cwd, out, err),
		other => Err(Error::new(format!("error: unknown ttsr action: {other}"))),
	}
}

// ───────────────────────────────────────────────────────────────────────────
// `omp gallery`
// ───────────────────────────────────────────────────────────────────────────

pub struct GalleryOptions {
	pub tool:     Option<String>,
	pub states:   Vec<String>,
	pub width:    Option<usize>,
	pub expanded: bool,
	pub plain:    bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GalleryState {
	Streaming,
	Progress,
	Success,
	Error,
}

const GALLERY_STATES: [GalleryState; 4] = [
	GalleryState::Streaming,
	GalleryState::Progress,
	GalleryState::Success,
	GalleryState::Error,
];

const GALLERY_STATE_TOKENS: [&str; 8] = [
	"streaming",
	"streaming args",
	"progress",
	"in progress",
	"success",
	"done",
	"error",
	"failed",
];

fn parse_gallery_states(raw: &[String]) -> Result<Vec<GalleryState>> {
	if raw.is_empty() {
		return Ok(GALLERY_STATES.to_vec());
	}
	let mut parsed = Vec::new();
	for item in raw {
		let token = item.trim().to_ascii_lowercase();
		let state = match token.as_str() {
			"streaming" | "streaming args" => GalleryState::Streaming,
			"progress" | "in progress" => GalleryState::Progress,
			"success" | "done" => GalleryState::Success,
			"error" | "failed" => GalleryState::Error,
			_ => {
				return Err(Error::new(format!(
					"Invalid --state '{}'. Valid values: {}",
					item,
					GALLERY_STATE_TOKENS.join(", ")
				)));
			},
		};
		if !parsed.contains(&state) {
			parsed.push(state);
		}
	}
	Ok(parsed)
}

fn gallery_state_label(state: GalleryState) -> &'static str {
	match state {
		GalleryState::Streaming => "streaming args",
		GalleryState::Progress => "in progress",
		GalleryState::Success => "done",
		GalleryState::Error => "failed",
	}
}

struct GalleryTool {
	name:   &'static str,
	args:   &'static str,
	result: &'static str,
}

/// Headless renderer fixtures for every tool this native runtime can register:
/// `BUILTIN_TOOLS`, `DELEGATION_TOOLS`, and the conditionally mounted
/// `checkpoint`/`rewind`/`goal`/`think` tools. No TypeScript-only tool is
/// listed.
///
/// Contract: deterministic ANSI/`--plain` lifecycle preview from call args and
/// result text. It is **not** a byte-for-byte port of the TypeScript TUI
/// `ToolExecutionComponent`; this binary has no TUI renderer layer. Add a
/// per-tool renderer here if tool-specific framing is required.
const GALLERY_TOOLS: [GalleryTool; 20] = [
	GalleryTool { name: "read", args: r#"{"path":"src/main.ts","offset":1,"limit":40}"#, result: "     1\tconst app = createApp();" },
	GalleryTool { name: "glob", args: r#"{"pattern":"src/**/*.ts"}"#, result: "src/main.ts\nsrc/util.ts" },
	GalleryTool { name: "grep", args: r#"{"pattern":"TODO","path":"src"}"#, result: "src/main.ts:12: // TODO: cache" },
	GalleryTool { name: "ast_grep", args: r#"{"pattern":"console.log($$$)","path":"src"}"#, result: "src/main.ts:14: console.log(app)" },
	GalleryTool { name: "ast_edit", args: r#"{"pattern":"console.log($$$)","rewrite":"logger.info($$$)"}"#, result: "Edited 1 match in src/main.ts" },
	GalleryTool { name: "write", args: r##"{"path":"notes.md","content":"# Notes\n"}"##, result: "Wrote 8 bytes to notes.md" },
	GalleryTool { name: "edit", args: r#"{"path":"src/main.ts","old_string":"let a = 1","new_string":"let a = 2"}"#, result: "Edited src/main.ts" },
	GalleryTool { name: "bash", args: r#"{"command":"git status --short"}"#, result: " M src/main.ts" },
	GalleryTool { name: "eval", args: r#"{"language":"py","code":"print(6 * 7)"}"#, result: "42" },
	GalleryTool { name: "web_search", args: r#"{"query":"rust cli parity"}"#, result: "1. Rust CLI parity plan — example.com" },
	GalleryTool { name: "todo", args: r#"{"phases":[{"name":"Build","tasks":["Ship"]}]}"#, result: "Updated todo list (1 phase)" },
	GalleryTool { name: "ask", args: r#"{"question":"Proceed with the migration?"}"#, result: "User answered: yes" },
	GalleryTool { name: "lsp", args: r#"{"action":"diagnostics","file":"src/main.ts"}"#, result: "No diagnostics" },
	GalleryTool { name: "debug", args: r#"{"action":"threads"}"#, result: "thread #1 main" },
	GalleryTool { name: "task", args: r#"{"task":"Add tests","agent":"task"}"#, result: "Subagent completed: 3 files changed" },
	GalleryTool { name: "hub", args: r#"{"action":"list"}"#, result: "No background agents" },
	GalleryTool { name: "goal", args: r#"{"goal":"Ship the headless preview"}"#, result: "Goal updated" },
	GalleryTool { name: "think", args: r#"{"thoughts":"Check the cache path"}"#, result: "------" },
	GalleryTool { name: "checkpoint", args: r#"{"goal":"Investigate the failing test"}"#, result: "Checkpoint: Investigate the failing test" },
	GalleryTool { name: "rewind", args: r#"{"report":"Explored the edit path; ready to rewind."}"#, result: "Rewind requested." },
];

fn gallery_known_tools() -> Vec<String> {
	let mut names: Vec<String> = GALLERY_TOOLS.iter().map(|tool| tool.name.to_string()).collect();
	names.sort();
	names
}

fn ansi(code: &str, text: &str, plain: bool) -> String {
	if plain {
		text.to_string()
	} else {
		format!("\x1b[{code}m{text}\x1b[0m")
	}
}

fn section_rule(label: &str, width: usize, plain: bool) -> String {
	let prefix = format!("── {label} ");
	let fill = width.saturating_sub(prefix.chars().count());
	ansi("1;36", &format!("{prefix}{}", "─".repeat(fill)), plain)
}

fn wrap_text(text: &str, width: usize) -> Vec<String> {
	let width = width.max(1);
	let mut lines = Vec::new();
	for paragraph in text.lines() {
		if paragraph.chars().count() <= width {
			lines.push(paragraph.to_string());
			continue;
		}
		let mut current = String::new();
		for word in paragraph.split_whitespace() {
			let candidate = if current.is_empty() {
				word.to_string()
			} else {
				format!("{current} {word}")
			};
			if candidate.chars().count() > width && !current.is_empty() {
				lines.push(std::mem::take(&mut current));
				current = word.to_string();
			} else {
				current = candidate;
			}
		}
		if !current.is_empty() {
			lines.push(current);
		}
	}
	if lines.is_empty() {
		lines.push(String::new());
	}
	lines
}

fn gallery_body(tool: &GalleryTool, state: GalleryState, width: usize, expanded: bool, plain: bool) -> Vec<String> {
	let mut lines = Vec::new();
	match state {
		GalleryState::Streaming | GalleryState::Progress => {
			let call = format!("→ {}", tool.name);
			lines.push(format!("  {}", ansi("36", &call, plain)));
			let args = if state == GalleryState::Streaming && !expanded {
				let half = tool.args.chars().count() / 2;
				format!("{}…", tool.args.chars().take(half).collect::<String>())
			} else {
				tool.args.to_string()
			};
			for line in wrap_text(&args, width.saturating_sub(4)) {
				lines.push(format!("    {}", ansi("2", &line, plain)));
			}
		},
		GalleryState::Success => {
			let call = format!("→ {}", tool.name);
			lines.push(format!("  {}", ansi("36", &call, plain)));
			let limit = if expanded { usize::MAX } else { 3 };
			for line in wrap_text(tool.result, width.saturating_sub(4)).into_iter().take(limit) {
				lines.push(format!("    {line}"));
			}
		},
		GalleryState::Error => {
			lines.push(format!(
				"  {}",
				ansi("31", &format!("✗ {}: Error: operation failed", tool.name), plain)
			));
		},
	}
	lines
}

pub fn run_gallery(opts: &GalleryOptions, out: &mut dyn Write) -> Result<u8> {
	let states = parse_gallery_states(&opts.states)?;
	let fallback = std::env::var("COLUMNS")
		.ok()
		.and_then(|value| value.trim().parse::<usize>().ok())
		.unwrap_or(100);
	let width = opts.width.unwrap_or(fallback).clamp(40, 200);
	let mut names = gallery_known_tools();
	if let Some(tool) = &opts.tool {
		if !names.iter().any(|name| name.as_str() == tool.as_str()) {
			write_line(
				out,
				&format!("Unknown tool '{tool}'. Known tools: {}", names.join(", ")),
			)?;
			return Ok(0);
		}
		names = vec![tool.clone()];
	}

	for name in &names {
		let Some(tool) = GALLERY_TOOLS.iter().find(|tool| tool.name == name.as_str()) else {
			continue;
		};
		write_line(out, "")?;
		write_line(out, &section_rule(name, width, opts.plain))?;
		for state in &states {
			write_line(out, "")?;
			write_line(
				out,
				&format!("  {}", ansi("2", &format!("· {}", gallery_state_label(*state)), opts.plain)),
			)?;
			for line in gallery_body(tool, *state, width, opts.expanded, opts.plain) {
				write_line(out, &line)?;
			}
		}
	}
	write_line(out, "")?;
	Ok(0)
}

// ───────────────────────────────────────────────────────────────────────────
// `omp grievances list`
// ───────────────────────────────────────────────────────────────────────────

pub struct GrievancesOptions {
	pub limit: u32,
	pub tool:  Option<String>,
	pub json:  bool,
}

fn autoqa_db_path() -> PathBuf {
	dirs::root_subdir("autoqa.db", AgentDirCategory::Data)
}

fn grievance_table_missing(error: &rusqlite::Error) -> bool {
	matches!(
		error,
		rusqlite::Error::SqliteFailure(_, Some(message))
			if message.contains("no such table: grievances")
	)
}

fn list_grievances_in(db_path: &Path, opts: &GrievancesOptions, out: &mut dyn Write) -> Result<u8> {
	if !db_path.exists() {
		if opts.json {
			write_line(out, "[]")?;
		} else {
			write_line(
				out,
				"No grievances database found. Auto-QA has not recorded any reports yet (or was disabled).",
			)?;
		}
		return Ok(0);
	}

	let connection = rusqlite::Connection::open_with_flags(
		db_path,
		rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
	)
	.map_err(|error| Error::new(error.to_string()))?;
	connection
		.busy_timeout(std::time::Duration::from_secs(5))
		.map_err(|error| Error::new(error.to_string()))?;

	let mut rows = Vec::new();
	let query = if opts.tool.is_some() {
		"SELECT id, model, version, tool, report FROM grievances WHERE tool = ? ORDER BY id DESC LIMIT ?"
	} else {
		"SELECT id, model, version, tool, report FROM grievances ORDER BY id DESC LIMIT ?"
	};
	let map_row = |row: &rusqlite::Row<'_>| {
		Ok((
			row.get::<_, i64>(0)? as u64,
			row.get::<_, String>(1)?,
			row.get::<_, String>(2)?,
			row.get::<_, String>(3)?,
			row.get::<_, String>(4)?,
		))
	};
	let limit = i64::from(opts.limit);
	let read_rows = || -> std::result::Result<Vec<_>, rusqlite::Error> {
		let mut statement = connection.prepare(query)?;
		match &opts.tool {
			Some(tool) => statement.query_map(rusqlite::params![tool, limit], map_row)?.collect(),
			None => statement.query_map(rusqlite::params![limit], map_row)?.collect(),
		}
	};
	let mapped = match read_rows() {
		Ok(rows) => rows,
		// Only a database without the auto-QA table is an empty report. Busy,
		// corrupt, or otherwise unreadable databases fail closed.
		Err(error) if grievance_table_missing(&error) => Vec::new(),
		Err(error) => return Err(Error::new(error.to_string())),
	};
	for row in mapped {
		rows.push(row);
	}

	if opts.json {
		let items: Vec<Value> = rows
			.iter()
			.map(|(id, model, version, tool, report)| {
				json!({
					"id": id,
					"model": model,
					"version": version,
					"tool": tool,
					"report": report,
				})
			})
			.collect();
		write_line(out, &serde_json::to_string_pretty(&items)?)?;
		return Ok(0);
	}

	if rows.is_empty() {
		write_line(out, "No grievances recorded yet.")?;
		return Ok(0);
	}
	for (id, model, version, tool, report) in &rows {
		write_line(
			out,
			&format!(
				"#{id} {} ({} v{})",
				escape_terminal_text(tool, false),
				escape_terminal_text(model, false),
				escape_terminal_text(version, false)
			),
		)?;
		write_line(out, &format!("  {}", escape_terminal_text(report, true)))?;
		write_line(out, "")?;
	}
	write_line(
		out,
		&format!(
			"Showing {} most recent{}",
			rows.len(),
			opts.tool
				.as_ref()
				.map(|tool| format!(" for {tool}"))
				.unwrap_or_default()
		),
	)?;
	Ok(0)
}

pub fn run_grievances(opts: &GrievancesOptions, out: &mut dyn Write) -> Result<u8> {
	list_grievances_in(&autoqa_db_path(), opts, out)
}

// ───────────────────────────────────────────────────────────────────────────
// `omp gc` (dry-run unless `--apply`)
// ───────────────────────────────────────────────────────────────────────────

pub struct GcOptions {
	pub apply:                  bool,
	pub json:                   bool,
	pub agent_dir:              Option<PathBuf>,
	pub blobs:                  bool,
	pub archive:                bool,
	pub wal:                    bool,
	pub cold_archive_after_days: Option<u32>,
	pub retain_newest_global:   Option<u32>,
	pub retain_newest_per_cwd:  Option<u32>,
}

struct GcPaths {
	agent_dir: PathBuf,
	sessions:  PathBuf,
	blobs:     PathBuf,
	archive:   PathBuf,
	history_db: PathBuf,
	models_db: PathBuf,
	lock_path: PathBuf,
}

impl GcPaths {
	fn resolve(explicit: Option<&Path>, cwd: &Path) -> Self {
		if let Some(agent_dir) = explicit {
			let agent_dir = resolve_path(cwd, agent_dir);
			return Self {
				sessions: agent_dir.join("sessions"),
				blobs: agent_dir.join("blobs"),
				archive: agent_dir.join("archive").join("sessions"),
				history_db: agent_dir.join("history.db"),
				models_db: agent_dir.join("models.db"),
				lock_path: agent_dir.join("gc.lock"),
				agent_dir,
			};
		}
		let sessions = dirs::agent_subdir("sessions", AgentDirCategory::Data);
		let archive = sessions
			.parent()
			.map_or_else(|| dirs::config_root().join("archive").join("sessions"), |parent| parent.join("archive").join("sessions"));
		Self {
			agent_dir: dirs::agent_dir(),
			blobs: dirs::agent_subdir("blobs", AgentDirCategory::Data),
			history_db: dirs::agent_subdir("history.db", AgentDirCategory::Data),
			models_db: dirs::agent_subdir("models.db", AgentDirCategory::Data),
			lock_path: dirs::agent_dir().join("gc.lock"),
			sessions,
			archive,
		}
	}
}

fn format_bytes(bytes: u64) -> String {
	if bytes < 1024 {
		return format!("{bytes} B");
	}
	let kib = bytes as f64 / 1024.0;
	if bytes < 1024 * 1024 {
		return format!("{kib:.1} KiB");
	}
	let mib = kib / 1024.0;
	if bytes < 1024 * 1024 * 1024 {
		return format!("{mib:.1} MiB");
	}
	format!("{:.1} GiB", mib / 1024.0)
}

fn is_blob_file(name: &str) -> Option<&str> {
	let bytes = name.as_bytes();
	if bytes.len() < 64 || !name.is_char_boundary(64) {
		return None;
	}
	let (hash, rest) = name.split_at(64);
	if !hash.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')) {
		return None;
	}
	if rest.is_empty() {
		return Some(hash);
	}
	let suffix = rest.strip_prefix('.')?;
	if suffix.is_empty() || suffix.len() > 32 {
		return None;
	}
	let mut chars = suffix.chars();
	let first = chars.next()?;
	if !first.is_ascii_alphanumeric() {
		return None;
	}
	if !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')) {
		return None;
	}
	Some(hash)
}

fn collect_jsonl_files(root: &Path, suffix: &str) -> Vec<PathBuf> {
	let mut files = Vec::new();
	let mut visited = HashSet::new();
	if let Some(key) = canonical_dir_key(root) {
		visited.insert(key);
	}
	let mut stack = vec![root.to_path_buf()];
	while let Some(current) = stack.pop() {
		let Ok(entries) = fs::read_dir(&current) else {
			continue;
		};
		for entry in entries.flatten() {
			let path = entry.path();
			if path.is_dir() {
				let Some(key) = canonical_dir_key(&path) else {
					continue;
				};
				if visited.insert(key) {
					stack.push(path);
				}
			} else if path
				.file_name()
				.and_then(|name| name.to_str())
				.is_some_and(|name| name.ends_with(suffix))
			{
				files.push(path);
			}
		}
	}
	files.sort();
	files
}

fn is_jsonl_name(name: &str) -> bool {
	name.ends_with(".jsonl")
}

fn is_compressed_jsonl_name(name: &str) -> bool {
	name.ends_with(".jsonl.gz")
}

fn is_backup_jsonl_name(name: &str) -> bool {
	name.ends_with(".bak") && name.contains(".jsonl.")
}

/// `Bun.Glob("**/*.jsonl")` semantics: recursive, dotfiles/dot-dirs excluded.
fn walk_visible_files(root: &Path, matches: fn(&str) -> bool) -> Vec<PathBuf> {
	let mut files = Vec::new();
	let mut visited = HashSet::new();
	if let Some(key) = canonical_dir_key(root) {
		visited.insert(key);
	}
	let mut stack = vec![root.to_path_buf()];
	while let Some(current) = stack.pop() {
		let Ok(entries) = fs::read_dir(&current) else {
			continue;
		};
		for entry in entries.flatten() {
			let Some(name) = entry.file_name().to_str().map(str::to_string) else {
				continue;
			};
			if name.starts_with('.') {
				continue;
			}
			let path = entry.path();
			if path.is_dir() {
				let Some(key) = canonical_dir_key(&path) else {
					continue;
				};
				if visited.insert(key) {
					stack.push(path);
				}
			} else if matches(&name) {
				files.push(path);
			}
		}
	}
	files.sort();
	files
}

fn read_text_window(path: &Path, offset: u64, max_bytes: u64) -> Option<String> {
	let mut file = fs::File::open(path).ok()?;
	file.seek(SeekFrom::Start(offset)).ok()?;
	let mut bytes = Vec::new();
	file.take(max_bytes).read_to_end(&mut bytes).ok()?;
	Some(String::from_utf8_lossy(&bytes).into_owned())
}

fn read_text_prefix(path: &Path, max_bytes: u64) -> Option<String> {
	read_text_window(path, 0, max_bytes)
}

fn read_text_suffix(path: &Path, max_bytes: u64) -> Option<String> {
	let size = fs::metadata(path).ok()?.len();
	read_text_window(path, size.saturating_sub(max_bytes), max_bytes)
}

fn is_word_byte(byte: u8) -> bool {
	byte.is_ascii_alphanumeric() || byte == b'_'
}

fn ascii_eq_ignore_case(left: &[u8], right: &[u8]) -> bool {
	left.len() == right.len()
		&& left
			.iter()
			.zip(right)
			.all(|(left, right)| left.eq_ignore_ascii_case(right))
}

const BLOB_REF_NEEDLE: &[u8] = b"blob:sha256:";
const BLOB_REF_LEN: usize = 12 + 64;
/// Retain the byte before a possible trailing match so a reference spanning a
/// chunk boundary is rescanned with its leading word boundary intact.
const BLOB_SCAN_OVERLAP: usize = BLOB_REF_LEN + 1;
const BLOB_SCAN_CHUNK: usize = 64 * 1024;

/// Equivalent of `/blob:sha256:([a-f0-9]{64})/gi` + lowercase + hash test,
/// restricted to one rolling window. A match ending exactly at a non-EOF window
/// boundary is deferred to the next window instead of being accepted early.
fn extract_blob_refs_window(
	bytes: &[u8],
	scan_start: usize,
	eof: bool,
	hashes: &mut HashSet<String>,
) {
	for start in scan_start..bytes.len() {
		let hash_start = start + BLOB_REF_NEEDLE.len();
		let hash_end = hash_start + 64;
		if hash_end > bytes.len() || (hash_end == bytes.len() && !eof) {
			continue;
		}
		if !ascii_eq_ignore_case(&bytes[start..hash_start], BLOB_REF_NEEDLE) {
			continue;
		}
		if start > 0 && is_word_byte(bytes[start - 1]) {
			continue;
		}
		if hash_end < bytes.len() && is_word_byte(bytes[hash_end]) {
			continue;
		}
		let hash = &bytes[hash_start..hash_end];
		if !hash.iter().all(u8::is_ascii_hexdigit) {
			continue;
		}
		let hash: String = hash
			.iter()
			.map(|byte| char::from(byte.to_ascii_lowercase()))
			.collect();
		hashes.insert(hash);
	}
}

/// Stream a reference log through a bounded rolling window: peak memory is one
/// chunk plus the overlap, independent of decompressed archive size.
fn scan_blob_refs_reader<R: Read>(
	reader: &mut R,
	path: &Path,
	hashes: &mut HashSet<String>,
) -> std::result::Result<(), String> {
	let mut buffer = vec![0u8; BLOB_SCAN_CHUNK];
	let mut carry: Vec<u8> = Vec::new();
	let mut data: Vec<u8> = Vec::new();
	let mut data_start = 0usize;
	loop {
		let read = reader
			.read(&mut buffer)
			.map_err(|error| format!("{}: {error}", path.display()))?;
		let eof = read == 0;
		data.clear();
		data.reserve(carry.len() + read);
		data.extend_from_slice(&carry);
		data.extend_from_slice(&buffer[..read]);
		// When the window starts after byte 0 its first byte is only the
		// leading-boundary byte; the first possible match starts at index 1 and
		// retains its preceding byte. A window that still starts at byte 0 can
		// scan index 0 because there is no preceding stream byte at all.
		let scan_start = usize::from(data_start > 0);
		extract_blob_refs_window(&data, scan_start, eof, hashes);
		if eof {
			break;
		}
		let keep = data.len().min(BLOB_SCAN_OVERLAP);
		data_start += data.len() - keep;
		carry.clear();
		carry.extend_from_slice(&data[data.len() - keep..]);
	}
	Ok(())
}

fn scan_blob_ref_file(
	path: &Path,
	gzip: bool,
	hashes: &mut HashSet<String>,
) -> std::result::Result<(), String> {
	let file = match fs::File::open(path) {
		Ok(file) => file,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(error) => return Err(format!("{}: {error}", path.display())),
	};
	if gzip {
		let mut decoder = GzDecoder::new(file);
		scan_blob_refs_reader(&mut decoder, path, hashes)
	} else {
		let mut reader = io::BufReader::new(file);
		scan_blob_refs_reader(&mut reader, path, hashes)
	}
}

fn collect_referenced_blob_hashes(
	roots: &[&Path],
) -> std::result::Result<HashSet<String>, String> {
	let mut files = Vec::new();
	for root in roots {
		files.extend(walk_visible_files(root, is_jsonl_name));
		files.extend(walk_visible_files(root, is_compressed_jsonl_name));
		files.extend(walk_visible_files(root, is_backup_jsonl_name));
	}
	files.sort();
	files.dedup();
	let mut hashes = HashSet::new();
	for file in files {
		let gzip = file
			.file_name()
			.and_then(|name| name.to_str())
			.is_some_and(is_compressed_jsonl_name);
		scan_blob_ref_file(&file, gzip, &mut hashes)?;
	}
	Ok(hashes)
}

fn run_blob_gc(paths: &GcPaths, apply: bool) -> std::result::Result<Value, String> {
	let referenced = collect_referenced_blob_hashes(&[&paths.sessions, &paths.archive])?;
	let mut by_hash: BTreeMap<String, (Vec<PathBuf>, u64, u64)> = BTreeMap::new();
	let entries = match fs::read_dir(&paths.blobs) {
		Ok(entries) => entries
			.collect::<std::result::Result<Vec<_>, _>>()
			.map_err(|error| format!("{}: {error}", paths.blobs.display()))?,
		Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
		Err(error) => return Err(format!("{}: {error}", paths.blobs.display())),
	};
	for entry in entries {
		let path = entry.path();
		let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
			continue;
		};
		let Some(hash) = is_blob_file(name) else {
			continue;
		};
		let meta = match fs::metadata(&path) {
			Ok(meta) => meta,
			Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
			Err(error) => return Err(format!("{}: {error}", path.display())),
		};
		if !meta.is_file() {
			continue;
		}
		let candidate = by_hash.entry(hash.to_string()).or_insert_with(|| (Vec::new(), 0, 0));
		candidate.0.push(path);
		candidate.1 += meta.len();
		candidate.2 = candidate.2.max(modified_ms(&meta));
	}
	let delete_before = now_ms().saturating_sub(GC_WRITE_GRACE_MS);
	let mut would_delete = 0u64;
	let mut deleted = 0u64;
	let mut bytes = 0u64;
	let mut errors: Vec<String> = Vec::new();
	for (hash, (files, group_bytes, mtime)) in &by_hash {
		if referenced.contains(hash) || *mtime > delete_before {
			continue;
		}
		would_delete += files.len() as u64;
		bytes += group_bytes;
		if !apply {
			continue;
		}
		for file in files {
			match fs::remove_file(file) {
				Ok(()) => deleted += 1,
				Err(error) if error.kind() == io::ErrorKind::NotFound => {},
				Err(error) => errors.push(format!("{}: {error}", file.display())),
			}
		}
	}
	Ok(json!({
		"referenced": referenced.len(),
		"candidates": by_hash.len(),
		"wouldDelete": would_delete,
		"deleted": deleted,
		"bytes": bytes,
		"errors": errors,
	}))
}

struct SessionHeader {
	id:                     String,
	cwd:                    Option<String>,
	parent_session:         Option<String>,
	previous_session_files: Vec<String>,
}

/// TS `parseSessionListHeader`: optional leading `title` slot, then a
/// `type: "session"` record with a non-empty `id`.
fn parse_session_header(content: &str) -> Option<SessionHeader> {
	let mut saw_title = false;
	for raw in content.split('\n') {
		let trimmed = raw.trim();
		if trimmed.is_empty() {
			continue;
		}
		if !saw_title {
			if let Ok(record) = serde_json::from_str::<Value>(trimmed) {
				if record.get("type").and_then(Value::as_str) == Some("title") {
					saw_title = true;
					continue;
				}
			}
		}
		let record = serde_json::from_str::<Value>(trimmed).ok()?;
		if record.get("type").and_then(Value::as_str) != Some("session") {
			return None;
		}
		let id = record.get("id").and_then(Value::as_str)?;
		if id.is_empty() {
			return None;
		}
		let parent_session = record
			.get("parentSession")
			.and_then(Value::as_str)
			.map(str::to_string);
		let previous_session_files = record
			.get("previousSessionFiles")
			.and_then(Value::as_array)
			.map(|items| {
				items
					.iter()
					.filter_map(Value::as_str)
					.filter(|value| !value.is_empty())
					.map(str::to_string)
					.collect()
			})
			.unwrap_or_default();
		return Some(SessionHeader {
			id: id.to_string(),
			cwd: record.get("cwd").and_then(Value::as_str).map(str::to_string),
			parent_session,
			previous_session_files,
		});
	}
	None
}

fn status_from_tail_message(message: &Value) -> &'static str {
	match message.get("role").and_then(Value::as_str).unwrap_or_default() {
		"assistant" => match message.get("stopReason").and_then(Value::as_str).unwrap_or_default() {
			"error" => "error",
			"aborted" => "aborted",
			"length" => "interrupted",
			_ => {
				let has_tool_call = message
					.get("content")
					.and_then(Value::as_array)
					.is_some_and(|blocks| {
						blocks.iter().any(|block| {
							block.get("type").and_then(Value::as_str) == Some("toolCall")
						})
					});
				if has_tool_call { "interrupted" } else { "complete" }
			},
		},
		"toolResult" => "interrupted",
		"user" => "pending",
		_ => "unknown",
	}
}

/// TS `deriveSessionStatus`: reverse-scan the tail window, skip non-`{` lines,
/// and classify the last parsable `type: "message"` entry.
fn session_status_text(suffix: &str) -> &'static str {
	if suffix.is_empty() {
		return "unknown";
	}
	for line in suffix.split('\n').rev() {
		if line.as_bytes().first() != Some(&b'{') {
			continue;
		}
		let Ok(entry) = serde_json::from_str::<Value>(line) else {
			continue;
		};
		if entry.get("type").and_then(Value::as_str) != Some("message") {
			continue;
		}
		let Some(message) = entry.get("message") else {
			continue;
		};
		return status_from_tail_message(message);
	}
	"unknown"
}

#[derive(Clone)]
struct SessionMeta {
	path:                   PathBuf,
	modified:               u64,
	status:                 &'static str,
	cwd:            Option<String>,
	id:             String,
	parent_session: Option<String>,
}

/// TS `listSessionsReadOnly(dir)`: non-recursive visible `*.jsonl` files with
/// valid session headers; unreadable/unparseable files are skipped.
fn list_sessions_in_dir(dir: &Path) -> Vec<SessionMeta> {
	let Ok(entries) = fs::read_dir(dir) else {
		return Vec::new();
	};
	let mut sessions = Vec::new();
	for entry in entries.flatten() {
		let Some(name) = entry.file_name().to_str().map(str::to_string) else {
			continue;
		};
		if name.starts_with('.') {
			continue;
		}
		let path = entry.path();
		if !name.ends_with(".jsonl") || !path.is_file() {
			continue;
		}
		let Ok(meta) = fs::metadata(&path) else {
			continue;
		};
		let Some(prefix) = read_text_prefix(&path, SESSION_PREFIX_BYTES) else {
			continue;
		};
		let Some(header) = parse_session_header(&prefix) else {
			continue;
		};
		let status = read_text_suffix(&path, SESSION_SUFFIX_BYTES)
			.map_or("unknown", |suffix| session_status_text(&suffix));
		sessions.push(SessionMeta {
			path,
			modified: modified_ms(&meta),
			status,
			cwd: header.cwd,
			id: header.id,
			parent_session: header.parent_session,
		});
	}
	sessions.sort_by(|left, right| right.modified.cmp(&left.modified));
	sessions
}

fn session_artifacts_path(session_path: &Path) -> PathBuf {
	let text = session_path.to_string_lossy();
	PathBuf::from(
		text.strip_suffix(".jsonl.gz")
			.or_else(|| text.strip_suffix(".jsonl"))
			.unwrap_or(text.as_ref())
			.to_string(),
	)
}

fn has_live_nested_sessions(session: &SessionMeta, archive_before: u64) -> bool {
	let root = session_artifacts_path(&session.path);
	let mut dirs = std::collections::BTreeSet::new();
	for file in walk_visible_files(&root, is_jsonl_name) {
		if let Some(parent) = file.parent() {
			dirs.insert(parent.to_path_buf());
		}
	}
	for dir in dirs {
		for nested in list_sessions_in_dir(&dir) {
			if matches!(nested.status, "pending" | "interrupted" | "unknown") {
				return true;
			}
			if nested.modified > archive_before {
				return true;
			}
		}
	}
	false
}

fn session_cwd_key(sessions_root: &Path, session: &SessionMeta) -> String {
	let Ok(relative) = session.path.strip_prefix(sessions_root) else {
		return session.cwd.clone().unwrap_or_else(|| ".".to_string());
	};
	let Some(parent) = relative.parent() else {
		return session.cwd.clone().unwrap_or_else(|| ".".to_string());
	};
	let dirname = normalize_slashes(&parent.display().to_string());
	if dirname.is_empty() || dirname == "." {
		session.cwd.clone().unwrap_or_else(|| ".".to_string())
	} else {
		dirname
	}
}

fn list_active_sessions_checked(
	sessions_root: &Path,
) -> std::result::Result<Vec<SessionMeta>, String> {
	let entries = match fs::read_dir(sessions_root) {
		Ok(entries) => entries,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
		Err(error) => {
			return Err(format!("{}: {error}", sessions_root.display()));
		},
	};
	let mut sessions = Vec::new();
	for entry in entries {
		let entry = entry.map_err(|error| format!("{}: {error}", sessions_root.display()))?;
		let dir = entry.path();
		if !dir.is_dir() {
			continue;
		}
		sessions.extend(list_sessions_in_dir(&dir));
	}
	sessions.sort_by(|left, right| right.modified.cmp(&left.modified));
	Ok(sessions)
}

struct ArchiveCandidate {
	session:     SessionMeta,
	destination: PathBuf,
}

fn archive_destination(
	archive_root: &Path,
	sessions_root: &Path,
	session: &SessionMeta,
) -> Option<PathBuf> {
	let relative = session.path.strip_prefix(sessions_root).ok()?;
	if relative.as_os_str().is_empty()
		|| relative.is_absolute()
		|| relative
			.components()
			.any(|component| matches!(component, Component::ParentDir | Component::RootDir | Component::Prefix(_)))
	{
		return None;
	}
	let relative_text = normalize_slashes(&relative.display().to_string());
	if !relative_text.ends_with(".jsonl") {
		return None;
	}
	Some(archive_root.join(format!("{relative_text}.gz")))
}

fn run_archive_gc(
	paths: &GcPaths,
	cwd: &Path,
	apply: bool,
	cold_archive_after_days: u64,
	retain_newest_global: u64,
	retain_newest_per_cwd: u64,
) -> std::result::Result<Value, String> {
	let sessions = list_active_sessions_checked(&paths.sessions)?;
	let cutoff = now_ms().saturating_sub(cold_archive_after_days.saturating_mul(DAY_MS));
	let archive_before = now_ms().saturating_sub(GC_WRITE_GRACE_MS);
	let mut skipped_active = 0u64;
	let mut kept_newest_global = 0u64;
	let mut kept_newest_per_cwd = 0u64;
	let mut inactive_seen = 0u64;
	let mut inactive_by_cwd: HashMap<String, u64> = HashMap::new();
	let mut candidates: Vec<ArchiveCandidate> = Vec::new();

	for session in &sessions {
		if matches!(session.status, "pending" | "interrupted" | "unknown") {
			skipped_active += 1;
			continue;
		}
		if session.modified > archive_before {
			skipped_active += 1;
			continue;
		}
		if has_live_nested_sessions(session, archive_before) {
			skipped_active += 1;
			continue;
		}
		let cwd_key = session_cwd_key(&paths.sessions, session);
		let cwd_seen = inactive_by_cwd.get(&cwd_key).copied().unwrap_or(0);
		let keep_global = inactive_seen < retain_newest_global;
		let keep_per_cwd = cwd_seen < retain_newest_per_cwd;
		inactive_seen += 1;
		inactive_by_cwd.insert(cwd_key, cwd_seen + 1);
		if keep_global {
			kept_newest_global += 1;
			continue;
		}
		if keep_per_cwd {
			kept_newest_per_cwd += 1;
			continue;
		}
		if cold_archive_after_days > 0 && session.modified > cutoff {
			continue;
		}
		let Some(destination) = archive_destination(&paths.archive, &paths.sessions, session) else {
			continue;
		};
		candidates.push(ArchiveCandidate { session: session.clone(), destination });
	}

	let would_archive = candidates.len() as u64;
	let mut archived = 0u64;
	let mut history_rows_deleted = 0u64;
	let mut stats_rows_deleted = 0u64;
	let mut fts_rebuilt = false;
	let mut errors: Vec<String> = Vec::new();
	let mut archived_sessions: Vec<SessionMeta> = Vec::new();
	let mut archived_session_ids: Vec<String> = Vec::new();

	if apply {
		for candidate in &candidates {
			match move_session_with_artifacts(candidate, &paths.sessions, &paths.archive) {
				Ok(()) => {
					archived += 1;
					archived_session_ids.push(candidate.session.id.clone());
					archived_sessions.push(candidate.session.clone());
				},
				Err(message) => {
					errors.push(format!("{}: {message}", candidate.session.path.display()));
				},
			}
		}
		cleanup_history_rows_for_archived_sessions(
			paths,
			&archived_session_ids,
			&mut history_rows_deleted,
			&mut fts_rebuilt,
			&mut errors,
		);
		stats_rows_deleted = cleanup_stats_rows_for_archived_sessions(
			paths,
			cwd,
			&archived_sessions,
			&mut errors,
		);
	}

	Ok(json!({
		"scanned": sessions.len(),
		"skippedActive": skipped_active,
		"keptNewestGlobal": kept_newest_global,
		"keptNewestPerCwd": kept_newest_per_cwd,
		"wouldArchive": would_archive,
		"archived": archived,
		"historyRowsDeleted": history_rows_deleted,
		"statsRowsDeleted": stats_rows_deleted,
		"ftsRebuilt": fts_rebuilt,
		"errors": errors,
	}))
}

fn wal_file_bytes(wal_path: &Path) -> std::result::Result<u64, String> {
	match fs::metadata(wal_path) {
		Ok(meta) => Ok(meta.len()),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
		Err(error) => Err(format!("{}: {error}", wal_path.display())),
	}
}

fn path_exists_checked(path: &Path) -> std::result::Result<bool, String> {
	match fs::metadata(path) {
		Ok(_) => Ok(true),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
		Err(error) => Err(format!("{}: {error}", path.display())),
	}
}

fn checkpoint_wal(
	db_path: &Path,
	apply: bool,
) -> std::result::Result<Value, String> {
	let wal_path = PathBuf::from(format!("{}-wal", db_path.display()));
	let mut wal_bytes = wal_file_bytes(&wal_path)?;
	let would_checkpoint = wal_bytes > 0;
	let mut checkpointed = false;
	let mut busy = 0i64;
	let mut log = 0i64;
	let mut checkpointed_frames = 0i64;
	if apply && path_exists_checked(db_path)? {
		let connection = rusqlite::Connection::open(db_path)
			.map_err(|error| format!("{}: {error}", db_path.display()))?;
		connection
			.busy_timeout(Duration::from_millis(5_000))
			.map_err(|error| format!("{}: {error}", db_path.display()))?;
		let row = match connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
			Ok((
				row.get::<_, i64>(0)?,
				row.get::<_, i64>(1)?,
				row.get::<_, i64>(2)?,
			))
		}) {
			Ok(row) => Some(row),
			Err(rusqlite::Error::QueryReturnedNoRows) => None,
			Err(error) => return Err(format!("{}: {error}", db_path.display())),
		};
		if let Some((next_busy, next_log, next_frames)) = row {
			busy = next_busy;
			log = next_log;
			checkpointed_frames = next_frames;
		}
		drop(connection);
		wal_bytes = wal_file_bytes(&wal_path)?;
		if busy > 0 || wal_bytes > 0 {
			return Err(format!(
				"WAL checkpoint failed for {}: busy={busy}, walBytes={wal_bytes}",
				db_path.display()
			));
		}
		checkpointed = true;
	}
	Ok(json!({
		"dbPath": display_path(db_path),
		"walBytes": wal_bytes,
		"wouldCheckpoint": would_checkpoint,
		"checkpointed": checkpointed,
		"busy": busy,
		"log": log,
		"checkpointedFrames": checkpointed_frames,
	}))
}

fn run_wal_gc(
	paths: &GcPaths,
	apply: bool,
) -> std::result::Result<Value, String> {
	// Match TS `Promise.all`: attempt both databases, then propagate the first
	// failure, so a busy history WAL never hides the models WAL.
	let history = checkpoint_wal(&paths.history_db, apply);
	let models = checkpoint_wal(&paths.models_db, apply);
	let databases = vec![history?, models?];
	let wal_bytes: u64 = databases
		.iter()
		.filter_map(|database| database["walBytes"].as_u64())
		.sum();
	let would_checkpoint = databases
		.iter()
		.any(|database| database["wouldCheckpoint"].as_bool().unwrap_or(false));
	let checkpointed = databases
		.iter()
		.any(|database| database["checkpointed"].as_bool().unwrap_or(false));
	Ok(json!({
		"databases": databases,
		"walBytes": wal_bytes,
		"wouldCheckpoint": would_checkpoint,
		"checkpointed": checkpointed,
	}))
}

fn gc_path_key(cwd: &Path, path: &Path) -> PathBuf {
	crate::config::normalize_lexically(&dirs::absolute(cwd, path))
}

fn gc_temp_path(destination: &Path) -> PathBuf {
	static COUNTER: AtomicU64 = AtomicU64::new(0);
	let nonce = COUNTER.fetch_add(1, Ordering::Relaxed);
	PathBuf::from(format!(
		"{}.{}.{}.{}.tmp",
		destination.display(),
		std::process::id(),
		now_ms(),
		nonce
	))
}

/// Create missing destination parents while refusing to leave the archive root.
///
/// The configured archive root is trusted (it may be a symlink), but every
/// descendant is opened with `O_NOFOLLOW` and missing directories are created
/// with `mkdirat`, so an escaping symlink is rejected before any foreign
/// directory is created.
fn ensure_destination_parent(
	archive_root: &Path,
	destination: &Path,
) -> std::result::Result<(), String> {
	use std::ffi::CString;
	use std::os::unix::ffi::OsStrExt;
	use std::os::unix::io::{AsRawFd, FromRawFd};

	let parent = destination
		.parent()
		.ok_or_else(|| format!("invalid archive destination: {}", destination.display()))?;
	let escape = || format!("archive destination escapes root: {}", destination.display());
	let relative = parent.strip_prefix(archive_root).map_err(|_| escape())?;
	fs::create_dir_all(archive_root)
		.map_err(|error| format!("{}: {error}", archive_root.display()))?;
	let mut directory = fs::File::open(archive_root)
		.map_err(|error| format!("{}: {error}", archive_root.display()))?;
	for component in relative.components() {
		let Component::Normal(name) = component else {
			return Err(escape());
		};
		let name = CString::new(name.as_bytes()).map_err(|_| escape())?;
		let open_child = |directory: &fs::File| unsafe {
			libc::openat(
				directory.as_raw_fd(),
				name.as_ptr(),
				libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
			)
		};
		let mut fd = open_child(&directory);
		if fd < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT) {
			let created = unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), 0o777) };
			if created != 0 {
				let error = io::Error::last_os_error();
				if error.kind() != io::ErrorKind::AlreadyExists {
					return Err(format!("{}: {error}", parent.display()));
				}
			}
			fd = open_child(&directory);
		}
		if fd < 0 {
			return Err(escape());
		}
		// SAFETY: `fd` was returned by `openat` and is owned from here on.
		directory = unsafe { fs::File::from_raw_fd(fd) };
	}
	Ok(())
}

fn ensure_source_within(
	root: &Path,
	source: &Path,
) -> std::result::Result<(), String> {
	let meta = fs::symlink_metadata(source)
		.map_err(|error| format!("{}: {error}", source.display()))?;
	if meta.file_type().is_symlink() {
		return Err(format!("refusing to move symlink: {}", source.display()));
	}
	let root = fs::canonicalize(root)
		.map_err(|error| format!("{}: {error}", root.display()))?;
	let resolved = fs::canonicalize(source)
		.map_err(|error| format!("{}: {error}", source.display()))?;
	if !resolved.starts_with(&root) {
		return Err(format!("source escapes sessions root: {}", source.display()));
	}
	Ok(())
}

#[cfg(test)]
thread_local! {
	static FAIL_GC_PUBLISH: std::cell::Cell<bool> = std::cell::Cell::new(false);
}

fn gzip_session_file(
	source: &Path,
	destination: &Path,
	archive_root: &Path,
) -> std::result::Result<(), String> {
	ensure_destination_parent(archive_root, destination)?;
	if path_exists_checked(destination)? {
		return Err(format!(
			"archive destination exists: {}",
			destination.display()
		));
	}
	let temp = gc_temp_path(destination);
	let written = (|| -> io::Result<()> {
		let input = fs::File::open(source)?;
		let output = OpenOptions::new()
			.write(true)
			.create_new(true)
			.mode(0o600)
			.open(&temp)?;
		let mut encoder = GzEncoder::new(output, Compression::best());
		io::copy(&mut BufReader::new(input), &mut encoder)?;
		let output = encoder.finish()?;
		output.sync_all()?;
		Ok(())
	})();
	if let Err(error) = written {
		let _ = fs::remove_file(&temp);
		return Err(format!("{}: {error}", source.display()));
	}
	// The staging file is complete and synced; publish it atomically without
	// ever exposing a partial archive or replacing a foreign destination. On
	// Android `hard_link` can fail with EACCES, so the native
	// `renameat2(RENAME_NOREPLACE)` fallback is a normal platform path.
	#[cfg(test)]
	let inject_publish_failure = FAIL_GC_PUBLISH.with(|flag| flag.replace(false));
	#[cfg(not(test))]
	let inject_publish_failure = false;
	let published = if inject_publish_failure {
		Err("injected publish failure".to_string())
	} else {
		pi_agent::tools::atomic_publish::publish_staged_file(&temp, destination)
	};
	// The staging file is ours on every outcome; remove it before propagating a
	// failure so a rejected archive never leaks the session temp.
	let _ = fs::remove_file(&temp);
	let published =
		published.map_err(|error| format!("{}: {error}", destination.display()))?;
	if !published {
		return Err(format!(
			"archive destination exists: {}",
			destination.display()
		));
	}
	if let Err(error) = fs::remove_file(source) {
		let _ = fs::remove_file(destination);
		return Err(format!("{}: {error}", source.display()));
	}
	Ok(())
}

fn restore_gzip_session_file(
	source: &Path,
	destination: &Path,
) -> std::result::Result<(), String> {
	if let Some(parent) = destination.parent() {
		fs::create_dir_all(parent)
			.map_err(|error| format!("{}: {error}", parent.display()))?;
	}
	let input = fs::File::open(source)
		.map_err(|error| format!("{}: {error}", source.display()))?;
	let mut decoder = GzDecoder::new(input);
	let mut output = OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(0o600)
		.open(destination)
		.map_err(|error| format!("{}: {error}", destination.display()))?;
	let copied = io::copy(&mut decoder, &mut output)
		.and_then(|_| output.sync_all())
		.map_err(|error| format!("{}: {error}", destination.display()));
	drop(output);
	if let Err(error) = copied {
		let _ = fs::remove_file(destination);
		return Err(error);
	}
	fs::remove_file(source).map_err(|error| format!("{}: {error}", source.display()))
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
fn path_cstring(path: &Path) -> io::Result<std::ffi::CString> {
	use std::os::unix::ffi::OsStrExt;
	std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
		io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("path contains a NUL: {}", path.display()),
		)
	})
}

/// Atomically rename `from` onto `to` only when `to` does not exist.
///
/// `Ok(false)` means another writer owns the destination. Linux and Android
/// implement this natively as `renameat2(..., RENAME_NOREPLACE)`; macOS uses
/// `renamex_np(RENAME_EXCL)`. The fallback copies only when the filesystem
/// reports `EXDEV`.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn rename_noreplace_native(from: &Path, to: &Path) -> io::Result<bool> {
	let from = path_cstring(from)?;
	let to = path_cstring(to)?;
	// SAFETY: both C strings are NUL-terminated and outlive the call; the
	// variadic arguments are the `renameat2` signature with LP64-wide integers.
	let result = unsafe {
		libc::syscall(
			libc::SYS_renameat2,
			libc::AT_FDCWD as libc::c_long,
			from.as_ptr(),
			libc::AT_FDCWD as libc::c_long,
			to.as_ptr(),
			libc::RENAME_NOREPLACE as libc::c_long,
		)
	};
	if result == 0 {
		return Ok(true);
	}
	let error = io::Error::last_os_error();
	match error.raw_os_error() {
		Some(libc::EEXIST) => Ok(false),
		_ => Err(error),
	}
}

#[cfg(target_os = "macos")]
fn rename_noreplace_native(from: &Path, to: &Path) -> io::Result<bool> {
	let from = path_cstring(from)?;
	let to = path_cstring(to)?;
	// SAFETY: both C strings are NUL-terminated and outlive the call.
	let result = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
	if result == 0 {
		return Ok(true);
	}
	let error = io::Error::last_os_error();
	match error.raw_os_error() {
		Some(libc::EEXIST) => Ok(false),
		_ => Err(error),
	}
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
fn rename_noreplace_native(from: &Path, to: &Path) -> io::Result<bool> {
	match fs::symlink_metadata(from) {
		Ok(meta) if !meta.is_dir() => {},
		_ => {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"atomic no-replace move needs renameat2 on this platform",
			));
		},
	}
	match fs::hard_link(from, to) {
		Ok(()) => {
			fs::remove_file(from)?;
			Ok(true)
		},
		Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
		Err(error) => Err(error),
	}
}

fn existing_device(path: &Path) -> std::result::Result<u64, String> {
	let mut current = Some(path);
	while let Some(path) = current {
		match fs::metadata(path) {
			Ok(meta) => return Ok(meta.dev()),
			Err(error) if error.kind() == io::ErrorKind::NotFound => current = path.parent(),
			Err(error) => return Err(format!("{}: {error}", path.display())),
		}
	}
	Err(format!("no existing ancestor for {}", path.display()))
}

/// Cross-filesystem artifact movement cannot be done losslessly without a
/// cooperative writer protocol: copying and then deleting the source loses
/// appends made through an already-open descriptor. Refuse before mutating
/// anything instead of porting the TS copy/delete fallback.
fn ensure_same_filesystem(
	source: &Path,
	destination: &Path,
) -> std::result::Result<(), String> {
	let source_dev = fs::metadata(source)
		.map_err(|error| format!("{}: {error}", source.display()))?
		.dev();
	let destination_dev = existing_device(destination)?;
	if source_dev != destination_dev {
		return Err(format!(
			"archive artifacts move across filesystems is unsupported: {} -> {}",
			source.display(),
			destination.display()
		));
	}
	Ok(())
}

fn move_tree_no_replace(
	source: &Path,
	destination: &Path,
) -> std::result::Result<(), String> {
	let exists = || {
		format!(
			"archive artifacts destination exists: {}",
			destination.display()
		)
	};
	if path_exists_checked(destination)? {
		return Err(exists());
	}
	match rename_noreplace_native(source, destination) {
		Ok(true) => Ok(()),
		Ok(false) => Err(exists()),
		Err(error) if error.raw_os_error() == Some(libc::EXDEV) => Err(format!(
			"archive artifacts move across filesystems is unsupported: {} -> {}",
			source.display(),
			destination.display()
		)),
		Err(error) => Err(format!("{}: {error}", source.display())),
	}
}

fn rollback_moves(moves: &[(PathBuf, PathBuf, bool)]) {
	for (source, destination, compressed) in moves.iter().rev() {
		if *compressed {
			let _ = restore_gzip_session_file(destination, source);
		} else {
			let _ = move_tree_no_replace(destination, source);
		}
	}
}

fn move_session_with_artifacts(
	candidate: &ArchiveCandidate,
	sessions_root: &Path,
	archive_root: &Path,
) -> std::result::Result<(), String> {
	let source = &candidate.session.path;
	let destination = &candidate.destination;
	ensure_source_within(sessions_root, source)?;
	let destination_text = destination.to_string_lossy();
	let legacy_destination = PathBuf::from(
		destination_text
			.strip_suffix(".gz")
			.unwrap_or(destination_text.as_ref())
			.to_string(),
	);
	let source_artifacts = session_artifacts_path(source);
	let destination_artifacts = session_artifacts_path(destination);
	let has_artifacts = path_exists_checked(&source_artifacts)?;
	if has_artifacts {
		ensure_source_within(sessions_root, &source_artifacts)?;
		ensure_same_filesystem(&source_artifacts, &destination_artifacts)?;
	}
	ensure_destination_parent(archive_root, destination)?;
	if path_exists_checked(destination)? {
		return Err(format!(
			"archive destination exists: {}",
			destination.display()
		));
	}
	if path_exists_checked(&legacy_destination)? {
		return Err(format!(
			"archive destination exists: {}",
			legacy_destination.display()
		));
	}
	if has_artifacts {
		if path_exists_checked(&destination_artifacts)? {
			return Err(format!(
				"archive artifacts destination exists: {}",
				destination_artifacts.display()
			));
		}
	}
	let mut moves: Vec<(PathBuf, PathBuf, bool)> = Vec::new();
	gzip_session_file(source, destination, archive_root)?;
	moves.push((source.clone(), destination.clone(), true));
	if has_artifacts {
		if let Err(error) = move_tree_no_replace(&source_artifacts, &destination_artifacts) {
			rollback_moves(&moves);
			return Err(error);
		}
	}
	Ok(())
}

fn read_text_if_present(path: &Path) -> std::result::Result<String, String> {
	let bytes = match fs::File::open(path) {
		Ok(file) => {
			if path
				.file_name()
				.and_then(|name| name.to_str())
				.is_some_and(is_compressed_jsonl_name)
			{
				let mut decoder = GzDecoder::new(file);
				let mut bytes = Vec::new();
				decoder.read_to_end(&mut bytes).map_err(|error| error.to_string())?;
				bytes
			} else {
				let mut bytes = Vec::new();
				io::BufReader::new(file)
					.read_to_end(&mut bytes)
					.map_err(|error| error.to_string())?;
				bytes
			}
		},
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(String::new()),
		Err(error) => return Err(error.to_string()),
	};
	Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn collect_archived_session_ids(archive_root: &Path) -> std::result::Result<Vec<String>, String> {
	let mut ids = BTreeSet::new();
	for file in walk_visible_files(archive_root, is_compressed_jsonl_name) {
		let text = read_text_if_present(&file)?;
		if let Some(header) = parse_session_header(&text) {
			ids.insert(header.id);
		}
	}
	Ok(ids.into_iter().collect())
}

fn sqlite_table_exists(
	connection: &rusqlite::Connection,
	table: &str,
) -> std::result::Result<bool, String> {
	use rusqlite::OptionalExtension as _;
	connection
		.query_row(
			"SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?1",
			[table],
			|_| Ok(()),
		)
		.optional()
		.map(|row| row.is_some())
		.map_err(|error| error.to_string())
}

fn sqlite_table_has_column(
	connection: &rusqlite::Connection,
	table: &str,
	column: &str,
) -> std::result::Result<bool, String> {
	let sql = format!("PRAGMA table_info({table})");
	let mut statement = connection.prepare(&sql).map_err(|error| error.to_string())?;
	let mut rows = statement.query([]).map_err(|error| error.to_string())?;
	while let Some(row) = rows.next().map_err(|error| error.to_string())? {
		let name = row.get::<_, Option<String>>(1).map_err(|error| error.to_string())?;
		if name.as_deref() == Some(column) {
			return Ok(true);
		}
	}
	Ok(false)
}

fn delete_history_rows_for_sessions(
	db_path: &Path,
	session_ids: &[String],
) -> std::result::Result<(u64, bool), String> {
	if session_ids.is_empty() {
		return Ok((0, false));
	}
	let mut connection =
		rusqlite::Connection::open(db_path).map_err(|error| error.to_string())?;
	connection
		.busy_timeout(Duration::from_millis(5_000))
		.map_err(|error| error.to_string())?;
	if !sqlite_table_exists(&connection, "history")? {
		return Ok((0, false));
	}
	if !sqlite_table_has_column(&connection, "history", "session_id")? {
		return Ok((0, false));
	}
	let has_fts = sqlite_table_exists(&connection, "history_fts")?;
	let transaction = connection
		.transaction()
		.map_err(|error| error.to_string())?;
	let mut deleted = 0u64;
	{
		let mut statement = transaction
			.prepare("DELETE FROM history WHERE session_id = ?")
			.map_err(|error| error.to_string())?;
		for id in session_ids {
			deleted += statement
				.execute([id.as_str()])
				.map_err(|error| error.to_string())? as u64;
		}
	}
	if deleted > 0 && has_fts {
		transaction
			.execute("INSERT INTO history_fts(history_fts) VALUES('rebuild')", [])
			.map_err(|error| error.to_string())?;
	}
	transaction.commit().map_err(|error| error.to_string())?;
	Ok((deleted, deleted > 0 && has_fts))
}

fn cleanup_history_rows_for_archived_sessions(
	paths: &GcPaths,
	newly_archived_ids: &[String],
	deleted_rows: &mut u64,
	fts_rebuilt: &mut bool,
	errors: &mut Vec<String>,
) {
	match path_exists_checked(&paths.history_db) {
		Ok(false) => return,
		Ok(true) => {},
		Err(message) => {
			errors.push(format!("history cleanup: {message}"));
			return;
		},
	}
	let mut cleanup_ids: BTreeSet<String> = newly_archived_ids.iter().cloned().collect();
	match collect_archived_session_ids(&paths.archive) {
		Ok(ids) => cleanup_ids.extend(ids),
		Err(message) => errors.push(format!("history cleanup scan: {message}")),
	}
	match delete_history_rows_for_sessions(&paths.history_db, &cleanup_ids.into_iter().collect::<Vec<_>>())
	{
		Ok((deleted, rebuilt)) => {
			*deleted_rows = deleted;
			*fts_rebuilt = rebuilt;
		},
		Err(message) => errors.push(format!("history cleanup: {message}")),
	}
}

#[derive(Clone, Default)]
struct StatsIdentities {
	messages:      Vec<StatsIdentity>,
	user_messages: Vec<StatsIdentity>,
	tool_calls:    Vec<StatsIdentity>,
}

#[derive(Clone)]
struct StatsIdentity {
	entry_id:     String,
	timestamp:    i64,
	tool_call_id: String,
}

#[derive(Clone)]
struct StatsSession {
	path:              PathBuf,
	id:                String,
	parent_session:    Option<String>,
	historical_paths:  Vec<PathBuf>,
	identities:        StatsIdentities,
}

#[derive(Clone)]
struct StatsNode {
	session:       StatsSession,
	stats_paths:   BTreeSet<PathBuf>,
	identity_keys: HashSet<String>,
}

#[derive(Clone)]
struct StatsTransfer {
	path:       PathBuf,
	identities: StatsIdentities,
}

struct StatsPlan {
	session_paths:           Vec<PathBuf>,
	retained:                Vec<usize>,
	transfers:               Vec<StatsTransfer>,
	archived_identity_keys:  HashSet<String>,
	preserve_all:            bool,
}

struct StatsCleanupContext {
	plans:               Vec<StatsPlan>,
	incomplete_retained: Vec<usize>,
}

fn stats_identity_key(table: &str, identity: &StatsIdentity) -> String {
	format!(
		"{table}\0{}\0{}\0{}",
		identity.entry_id, identity.timestamp, identity.tool_call_id
	)
}

fn stats_identity_keys(identities: &StatsIdentities) -> HashSet<String> {
	let mut keys = HashSet::new();
	for identity in &identities.messages {
		keys.insert(stats_identity_key("messages", identity));
	}
	for identity in &identities.user_messages {
		keys.insert(stats_identity_key("user_messages", identity));
	}
	for identity in &identities.tool_calls {
		keys.insert(stats_identity_key("tool_calls", identity));
	}
	keys
}

fn add_session_stats_identity(line: &str, identities: &mut StatsIdentities) {
	if line.is_empty() {
		return;
	}
	let Ok(record) = serde_json::from_str::<Value>(line) else {
		return;
	};
	if record.get("type").and_then(Value::as_str) != Some("message") {
		return;
	}
	let Some(entry_id) = record
		.get("id")
		.and_then(Value::as_str)
		.filter(|id| !id.is_empty())
	else {
		return;
	};
	let Some(message) = record.get("message").filter(|message| message.is_object()) else {
		return;
	};
	let parsed_entry_timestamp = record
		.get("timestamp")
		.and_then(Value::as_str)
		.and_then(crate::foreign_session::parse_iso_timestamp_ms)
		.map_or(0, |value| value as i64);
	match message.get("role").and_then(Value::as_str) {
		Some("user") => identities.user_messages.push(StatsIdentity {
			entry_id: entry_id.to_string(),
			timestamp: parsed_entry_timestamp,
			tool_call_id: String::new(),
		}),
		Some("assistant") => {
			let timestamp = message
				.get("timestamp")
				.and_then(Value::as_f64)
				.filter(|value| value.is_finite())
				.map_or(parsed_entry_timestamp, |value| value as i64);
			identities.messages.push(StatsIdentity {
				entry_id: entry_id.to_string(),
				timestamp,
				tool_call_id: String::new(),
			});
			if let Some(blocks) = message.get("content").and_then(Value::as_array) {
				for block in blocks {
					let Some(tool_call_id) = block
						.get("type")
						.filter(|kind| kind.as_str() == Some("toolCall"))
						.and_then(|_| block.get("id"))
						.and_then(Value::as_str)
					else {
						continue;
					};
					identities.tool_calls.push(StatsIdentity {
						entry_id: entry_id.to_string(),
						timestamp,
						tool_call_id: tool_call_id.to_string(),
					});
				}
			}
		},
		_ => {},
	}
}

fn collect_session_stats_identities_from_text(text: &str) -> StatsIdentities {
	let mut identities = StatsIdentities::default();
	for line in text.split('\n') {
		add_session_stats_identity(line, &mut identities);
	}
	identities
}

fn collect_session_stats_identities(
	path: &Path,
) -> std::result::Result<StatsIdentities, String> {
	let mut identities = StatsIdentities::default();
	let file = fs::File::open(path).map_err(|error| error.to_string())?;
	let mut reader = BufReader::new(file);
	let mut buffer = Vec::new();
	loop {
		buffer.clear();
		let read = reader
			.read_until(b'\n', &mut buffer)
			.map_err(|error| error.to_string())?;
		if read == 0 {
			break;
		}
		let line = String::from_utf8_lossy(&buffer);
		add_session_stats_identity(&line, &mut identities);
	}
	Ok(identities)
}

fn read_session_lineage_header(
	path: &Path,
) -> std::result::Result<Option<SessionHeader>, String> {
	let file = fs::File::open(path).map_err(|error| error.to_string())?;
	let mut reader = BufReader::new(file);
	let mut lines: Vec<String> = Vec::new();
	let mut buffer = String::new();
	loop {
		buffer.clear();
		let read = reader
			.read_line(&mut buffer)
			.map_err(|error| error.to_string())?;
		if read == 0 {
			break;
		}
		let line = buffer.trim();
		if line.is_empty() {
			continue;
		}
		lines.push(line.to_string());
		if let Some(header) = parse_session_header(&lines.join("\n")) {
			return Ok(Some(header));
		}
		if lines.len() >= 2 {
			return Ok(None);
		}
	}
	Ok(None)
}

fn managed_historical_session_paths(
	header: &SessionHeader,
	current_session_path: &Path,
	sessions_root: &Path,
	cwd: &Path,
) -> Vec<PathBuf> {
	let Some(filename) = current_session_path.file_name() else {
		return Vec::new();
	};
	let root = gc_path_key(cwd, sessions_root);
	let mut paths = BTreeSet::new();
	for previous in &header.previous_session_files {
		let previous_path = Path::new(previous);
		if !previous_path.is_absolute() || previous_path.file_name() != Some(filename) {
			continue;
		}
		let resolved = gc_path_key(cwd, previous_path);
		let Ok(relative) = resolved.strip_prefix(&root) else {
			continue;
		};
		if relative.as_os_str().is_empty()
			|| relative
				.components()
				.any(|component| matches!(component, Component::ParentDir))
			|| !resolved.to_string_lossy().ends_with(".jsonl")
		{
			continue;
		}
		paths.insert(resolved);
	}
	paths.into_iter().collect()
}

fn logical_session_root_for_stats_path(
	stats_path: &Path,
	session_path: &Path,
	cwd: &Path,
) -> Option<PathBuf> {
	let stats_path = gc_path_key(cwd, stats_path);
	if stats_path.file_name() == session_path.file_name() {
		return Some(stats_path);
	}
	let session_filename = session_path.file_name()?.to_str()?;
	let artifacts_dirname = session_filename.strip_suffix(".jsonl")?;
	let mut directory = stats_path.parent()?.to_path_buf();
	loop {
		if directory.file_name().and_then(|name| name.to_str()) == Some(artifacts_dirname) {
			return Some(PathBuf::from(format!("{}.jsonl", directory.display())));
		}
		let parent = directory.parent()?.to_path_buf();
		if parent == directory {
			return None;
		}
		directory = parent;
	}
}

fn session_path_encodes_id(session_path: &Path, session_id: &str) -> bool {
	let Some(name) = session_path.file_name().and_then(|name| name.to_str()) else {
		return false;
	};
	let stem = name.strip_suffix(".jsonl").unwrap_or(name);
	stem == session_id || stem.ends_with(&format!("_{session_id}"))
}

fn resolve_logical_session_match(
	stats_path: &Path,
	nodes: &[StatsNode],
	cwd: &Path,
) -> Option<(usize, PathBuf)> {
	let mut matches = Vec::new();
	for (index, node) in nodes.iter().enumerate() {
		if let Some(logical_root) =
			logical_session_root_for_stats_path(stats_path, &node.session.path, cwd)
		{
			matches.push((index, logical_root));
		}
	}
	let exact: Vec<&(usize, PathBuf)> = matches
		.iter()
		.filter(|(index, logical_root)| {
			gc_path_key(cwd, logical_root) == gc_path_key(cwd, &nodes[*index].session.path)
		})
		.collect();
	if exact.len() == 1 {
		return Some((exact[0].0, exact[0].1.clone()));
	}
	if exact.len() > 1 {
		return None;
	}
	let known: Vec<&(usize, PathBuf)> = matches
		.iter()
		.filter(|(index, logical_root)| {
			nodes[*index]
				.stats_paths
				.contains(&gc_path_key(cwd, logical_root))
		})
		.collect();
	if known.len() == 1 {
		return Some((known[0].0, known[0].1.clone()));
	}
	if known.len() > 1 {
		return None;
	}
	let by_id: Vec<&(usize, PathBuf)> = matches
		.iter()
		.filter(|(index, logical_root)| {
			session_path_encodes_id(logical_root, &nodes[*index].session.id)
		})
		.collect();
	if by_id.len() == 1 {
		return Some((by_id[0].0, by_id[0].1.clone()));
	}
	None
}

fn collect_stored_stats_session_paths(
	connection: &rusqlite::Connection,
) -> std::result::Result<Vec<String>, String> {
	let mut paths = BTreeSet::new();
	for table in ["messages", "user_messages", "tool_calls", "file_offsets"] {
		if !sqlite_table_exists(connection, table)?
			|| !sqlite_table_has_column(connection, table, "session_file")?
		{
			continue;
		}
		let sql = format!("SELECT DISTINCT session_file FROM {table}");
		let mut statement = connection.prepare(&sql).map_err(|error| error.to_string())?;
		let mut rows = statement.query([]).map_err(|error| error.to_string())?;
		while let Some(row) = rows.next().map_err(|error| error.to_string())? {
			if let Ok(Some(path)) = row.get::<_, Option<String>>(0) {
				paths.insert(path);
			}
		}
	}
	Ok(paths.into_iter().collect())
}

fn build_stats_cleanup_plans(
	archived_sessions: &[StatsSession],
	retained_sessions: &[StatsSession],
	db_path: &Path,
	cwd: &Path,
) -> std::result::Result<StatsCleanupContext, String> {
	let mut nodes: Vec<StatsNode> = archived_sessions
		.iter()
		.chain(retained_sessions)
		.map(|session| {
			let mut stats_paths = BTreeSet::new();
			stats_paths.insert(gc_path_key(cwd, &session.path));
			for path in &session.historical_paths {
				stats_paths.insert(gc_path_key(cwd, path));
			}
			StatsNode {
				session: session.clone(),
				stats_paths,
				identity_keys: stats_identity_keys(&session.identities),
			}
		})
		.collect();

	{
		let connection =
			rusqlite::Connection::open(db_path).map_err(|error| error.to_string())?;
		connection
			.busy_timeout(Duration::from_millis(5_000))
			.map_err(|error| error.to_string())?;
		for stored_path in collect_stored_stats_session_paths(&connection)? {
			let stored_path = PathBuf::from(stored_path);
			if let Some((index, logical_root)) =
				resolve_logical_session_match(&stored_path, &nodes, cwd)
			{
				nodes[index]
					.stats_paths
					.insert(gc_path_key(cwd, &logical_root));
			}
		}
	}

	for index in 0..nodes.len() {
		let Some(parent_session) = nodes[index].session.parent_session.clone() else {
			continue;
		};
		let parent_path = Path::new(&parent_session);
		if !parent_path.is_absolute() && !parent_session.ends_with(".jsonl") {
			continue;
		}
		if let Some((parent_index, logical_root)) =
			resolve_logical_session_match(parent_path, &nodes, cwd)
		{
			nodes[parent_index]
				.stats_paths
				.insert(gc_path_key(cwd, &logical_root));
		}
	}

	let archived_count = archived_sessions.len();
	let mut claim_counts: HashMap<PathBuf, usize> = HashMap::new();
	for node in nodes.iter().take(archived_count) {
		for path in &node.stats_paths {
			*claim_counts.entry(path.clone()).or_default() += 1;
		}
	}
	let ambiguous: HashSet<PathBuf> = claim_counts
		.into_iter()
		.filter_map(|(path, count)| (count > 1).then_some(path))
		.collect();

	let alias_keys_by_node: Vec<HashSet<String>> = nodes
		.iter()
		.map(|node| {
			let mut keys = HashSet::new();
			keys.insert(format!("id:{}", node.session.id));
			for path in &node.stats_paths {
				keys.insert(format!("path:{}", path.display()));
			}
			keys
		})
		.collect();
	let mut aliases: HashMap<String, Option<usize>> = HashMap::new();
	for (index, keys) in alias_keys_by_node.iter().enumerate() {
		for key in keys {
			match aliases.get(key) {
				None => {
					aliases.insert(key.clone(), Some(index));
				},
				Some(Some(existing)) if *existing == index => {},
				Some(_) => {
					aliases.insert(key.clone(), None);
				},
			}
		}
	}

	let mut incomplete: HashSet<usize> = HashSet::new();
	let mut lineage_keys: HashMap<usize, HashSet<String>> = HashMap::new();
	for index in 0..nodes.len() {
		let mut keys = alias_keys_by_node[index].clone();
		let mut current = Some(index);
		let mut seen = HashSet::new();
		while let Some(node_index) = current {
			if !seen.insert(node_index) {
				incomplete.insert(index);
				break;
			}
			let Some(parent_session) = nodes[node_index].session.parent_session.clone() else {
				break;
			};
			let parent_ref = Path::new(&parent_session);
			let parent_key = if parent_ref.is_absolute() || parent_session.ends_with(".jsonl") {
				format!("path:{}", gc_path_key(cwd, parent_ref).display())
			} else {
				format!("id:{parent_session}")
			};
			keys.insert(parent_key.clone());
			let Some(Some(parent_index)) = aliases.get(&parent_key).copied() else {
				incomplete.insert(index);
				break;
			};
			keys.extend(alias_keys_by_node[parent_index].iter().cloned());
			current = Some(parent_index);
		}
		lineage_keys.insert(index, keys);
	}

	let retained_path_keys: HashSet<PathBuf> = nodes
		.iter()
		.skip(archived_count)
		.flat_map(|node| node.stats_paths.iter().cloned())
		.collect();
	let mut plans = Vec::new();
	for index in 0..archived_count {
		let archived_lineage: HashSet<String> =
			lineage_keys.get(&index).cloned().unwrap_or_default();
		let session_paths: Vec<PathBuf> = nodes[index]
			.stats_paths
			.iter()
			.filter(|path| !retained_path_keys.contains(*path) && !ambiguous.contains(*path))
			.cloned()
			.collect();
		let retained: Vec<usize> = (archived_count..nodes.len())
			.filter(|retained_index| {
				!incomplete.contains(retained_index)
					&& lineage_keys.get(retained_index).is_some_and(|keys| {
						keys.iter().any(|key| archived_lineage.contains(key))
					})
			})
			.collect();
		plans.push(StatsPlan {
			session_paths,
			retained,
			transfers: Vec::new(),
			archived_identity_keys: nodes[index].identity_keys.clone(),
			preserve_all: false,
		});
	}
	let incomplete_retained = (archived_count..nodes.len())
		.filter(|index| incomplete.contains(index))
		.collect();
	Ok(StatsCleanupContext { plans, incomplete_retained })
}

fn cached_stats_identities(
	cache: &mut HashMap<PathBuf, StatsIdentities>,
	path: &Path,
) -> std::result::Result<StatsIdentities, String> {
	if let Some(identities) = cache.get(path) {
		return Ok(identities.clone());
	}
	let identities = collect_session_stats_identities(path)?;
	cache.insert(path.to_path_buf(), identities.clone());
	Ok(identities)
}

fn populate_stats_transfer_targets(
	context: &mut StatsCleanupContext,
	nodes: &[StatsNode],
) -> std::result::Result<(), String> {
	let mut cache: HashMap<PathBuf, StatsIdentities> = HashMap::new();
	let mut incomplete_identity_keys: HashSet<String> = HashSet::new();
	let mut has_unidentifiable_incomplete_session = false;
	for index in &context.incomplete_retained {
		let path = &nodes[*index].session.path;
		let identities = cached_stats_identities(&mut cache, path)?;
		let keys = stats_identity_keys(&identities);
		if keys.is_empty() {
			has_unidentifiable_incomplete_session = true;
		}
		incomplete_identity_keys.extend(keys);
	}
	for plan_index in 0..context.plans.len() {
		let retained = context.plans[plan_index].retained.clone();
		if !context.incomplete_retained.is_empty() {
			let preserve = {
				let archived_keys = &context.plans[plan_index].archived_identity_keys;
				has_unidentifiable_incomplete_session
					|| archived_keys.is_empty()
					|| archived_keys
						.iter()
						.any(|key| incomplete_identity_keys.contains(key))
			};
			context.plans[plan_index].preserve_all = preserve;
		}
		for index in retained {
			let path = &nodes[index].session.path;
			let identities = cached_stats_identities(&mut cache, path)?;
			context.plans[plan_index].transfers.push(StatsTransfer {
				path: path.clone(),
				identities,
			});
		}
	}
	Ok(())
}

fn reconcile_stats_rows_for_sessions(
	db_path: &Path,
	plans: &[StatsPlan],
) -> std::result::Result<u64, String> {
	if plans.is_empty() {
		return Ok(0);
	}
	let mut connection =
		rusqlite::Connection::open(db_path).map_err(|error| error.to_string())?;
	connection
		.busy_timeout(Duration::from_millis(5_000))
		.map_err(|error| error.to_string())?;
	let session_tables: Vec<&str> = ["messages", "user_messages", "tool_calls", "file_offsets"]
		.into_iter()
		.filter(|table| {
			sqlite_table_exists(&connection, table).unwrap_or(false)
				&& sqlite_table_has_column(&connection, table, "session_file").unwrap_or(false)
		})
		.collect();
	let entry_tables: Vec<&str> = ["messages", "user_messages", "tool_calls"]
		.into_iter()
		.filter(|table| {
			session_tables.contains(table)
				&& match *table {
					"tool_calls" => {
						["entry_id", "timestamp", "tool_call_id"]
							.iter()
							.all(|column| {
								sqlite_table_has_column(&connection, table, column)
									.unwrap_or(false)
							})
					},
					_ => ["entry_id", "timestamp"].iter().all(|column| {
						sqlite_table_has_column(&connection, table, column).unwrap_or(false)
					}),
				}
		})
		.collect();
	connection
		.execute(
			"CREATE TEMP TABLE gc_retained_entries (
				table_name TEXT NOT NULL,
				entry_id TEXT NOT NULL,
				timestamp INTEGER NOT NULL,
				tool_call_id TEXT NOT NULL,
				target_session_file TEXT NOT NULL,
				PRIMARY KEY (table_name, entry_id, timestamp, tool_call_id)
			)",
			[],
		)
		.map_err(|error| error.to_string())?;
	let transaction = connection
		.transaction()
		.map_err(|error| error.to_string())?;
	let mut deleted = 0u64;
	for plan in plans {
		if plan.preserve_all {
			continue;
		}
		transaction
			.execute("DELETE FROM gc_retained_entries", [])
			.map_err(|error| error.to_string())?;
		{
			let mut insert = transaction
				.prepare(
					"INSERT OR IGNORE INTO gc_retained_entries (
						table_name, entry_id, timestamp, tool_call_id, target_session_file
					) VALUES (?, ?, ?, ?, ?)",
				)
				.map_err(|error| error.to_string())?;
			for transfer in &plan.transfers {
				let target = transfer.path.to_string_lossy().into_owned();
				for identity in &transfer.identities.messages {
					insert
						.execute(rusqlite::params![
							"messages",
							identity.entry_id,
							identity.timestamp,
							identity.tool_call_id,
							target
						])
						.map_err(|error| error.to_string())?;
				}
				for identity in &transfer.identities.user_messages {
					insert
						.execute(rusqlite::params![
							"user_messages",
							identity.entry_id,
							identity.timestamp,
							identity.tool_call_id,
							target
						])
						.map_err(|error| error.to_string())?;
				}
				for identity in &transfer.identities.tool_calls {
					insert
						.execute(rusqlite::params![
							"tool_calls",
							identity.entry_id,
							identity.timestamp,
							identity.tool_call_id,
							target
						])
						.map_err(|error| error.to_string())?;
				}
			}
		}
		let mut session_paths = BTreeSet::new();
		for path in &plan.session_paths {
			session_paths.insert(path.to_string_lossy().into_owned());
		}
		for table in &entry_tables {
			let tool_call_match = if *table == "tool_calls" {
				format!("retained.tool_call_id = {table}.tool_call_id")
			} else {
				"retained.tool_call_id = ''".to_string()
			};
			let identity_match = format!(
				"retained.table_name = '{table}'
					AND retained.entry_id = {table}.entry_id
					AND retained.timestamp = {table}.timestamp
					AND {tool_call_match}"
			);
			let sql = format!(
				"UPDATE OR IGNORE {table}
					SET session_file = (
						SELECT retained.target_session_file
						FROM gc_retained_entries AS retained
						WHERE {identity_match}
					)
					WHERE session_file = ?
						AND EXISTS (
							SELECT 1
							FROM gc_retained_entries AS retained
							WHERE {identity_match}
						)"
			);
			let mut update = transaction
				.prepare(&sql)
				.map_err(|error| error.to_string())?;
			for path in &session_paths {
				update
					.execute([path.as_str()])
					.map_err(|error| error.to_string())?;
			}
		}
		for path in &session_paths {
			let nested_prefix = format!(
				"{}{}",
				session_artifacts_path(Path::new(path)).display(),
				std::path::MAIN_SEPARATOR
			);
			for table in &session_tables {
				let requires_transfer = !plan.retained.is_empty()
					&& *table != "file_offsets"
					&& !entry_tables.contains(table);
				if requires_transfer {
					continue;
				}
				let sql = format!(
					"DELETE FROM {table} WHERE session_file = ? OR instr(session_file, ?) = 1"
				);
				deleted += (transaction
					.execute(&sql, rusqlite::params![path.as_str(), nested_prefix.as_str()])
					.map_err(|error| error.to_string())?) as u64;
			}
		}
	}
	transaction.commit().map_err(|error| error.to_string())?;
	Ok(deleted)
}

fn collect_archived_stats_sessions(
	archive_root: &Path,
	sessions_root: &Path,
	cwd: &Path,
	errors: &mut Vec<String>,
) -> Vec<StatsSession> {
	let mut sessions = Vec::new();
	for file in walk_visible_files(archive_root, is_compressed_jsonl_name) {
		let Ok(relative) = file.strip_prefix(archive_root) else {
			continue;
		};
		if relative.as_os_str().is_empty()
			|| relative.is_absolute()
			|| relative
				.components()
				.any(|component| matches!(component, Component::ParentDir))
		{
			continue;
		}
		let relative_text = relative.to_string_lossy();
		let Some(without_gz) = relative_text.strip_suffix(".gz") else {
			continue;
		};
		let source_path = sessions_root.join(without_gz);
		let text = match read_text_if_present(&file) {
			Ok(text) => text,
			Err(error) => {
				errors.push(format!("stats cleanup scan {}: {error}", file.display()));
				continue;
			},
		};
		let Some(header) = parse_session_header(&text) else {
			errors.push(format!(
				"stats cleanup scan {}: archive is missing a valid session header",
				file.display()
			));
			continue;
		};
		let historical_paths =
			managed_historical_session_paths(&header, &source_path, sessions_root, cwd);
		let identities = collect_session_stats_identities_from_text(&text);
		sessions.push(StatsSession {
			path: source_path,
			id: header.id,
			parent_session: header.parent_session,
			historical_paths,
			identities,
		});
	}
	sessions
}

fn stats_db_path(paths: &GcPaths, cwd: &Path) -> PathBuf {
	if gc_path_key(cwd, &paths.agent_dir) == gc_path_key(cwd, &dirs::agent_dir()) {
		dirs::root_subdir("stats.db", AgentDirCategory::Data)
	} else {
		paths.agent_dir.join("stats.db")
	}
}

fn with_stats_sync_lock<T>(
	db_path: &Path,
	run: impl FnOnce() -> std::result::Result<T, String>,
) -> std::result::Result<T, String> {
	if let Some(parent) = db_path.parent() {
		fs::create_dir_all(parent)
			.map_err(|error| format!("{}: {error}", parent.display()))?;
	}
	let _lock = StatsSyncLock::acquire(db_path)?;
	run()
}

enum StatsSyncHandle {
	#[cfg(target_os = "linux")]
	Socket(UnixDatagram),
	#[cfg(not(target_os = "linux"))]
	File(fs::File),
}

struct StatsSyncLock {
	_handle: StatsSyncHandle,
}

impl StatsSyncLock {
	const RETRIES: u64 = 60 * 60 * 1_000 / 25;
	const RETRY_DELAY: Duration = Duration::from_millis(25);

	fn acquire(db_path: &Path) -> std::result::Result<Self, String> {
		let target = format!("{}.sync", db_path.display());
		let lock_path = format!("{target}.lock");
		for attempt in 0..Self::RETRIES {
			if let Some(handle) = try_stats_sync_lock(&lock_path)? {
				return Ok(Self { _handle: handle });
			}
			if attempt + 1 < Self::RETRIES {
				std::thread::sleep(Self::RETRY_DELAY);
			}
		}
		Err(format!(
			"Failed to acquire lock for {target} after {} attempts",
			Self::RETRIES
		))
	}
}

#[cfg(target_os = "linux")]
fn try_stats_sync_lock(
	lock_path: &str,
) -> std::result::Result<Option<StatsSyncHandle>, String> {
	const HIGH_SEED: u64 = 0x4f4d_502d_4c4f_434b;
	const LOW_SEED: u64 = 0x5049_2d46_494c_454c;
	let bytes = lock_path.as_bytes();
	let name = format!(
		"omp-file-lock-{:016x}{:016x}",
		xxhash_rust::xxh64::xxh64(bytes, HIGH_SEED),
		xxhash_rust::xxh64::xxh64(bytes, LOW_SEED)
	);
	let address = SocketAddr::from_abstract_name(name.as_bytes()).map_err(|error| error.to_string())?;
	match UnixDatagram::bind_addr(&address) {
		Ok(socket) => Ok(Some(StatsSyncHandle::Socket(socket))),
		Err(error) if error.kind() == io::ErrorKind::AddrInUse => Ok(None),
		Err(error) => Err(error.to_string()),
	}
}

#[cfg(not(target_os = "linux"))]
fn try_stats_sync_lock(
	lock_path: &str,
) -> std::result::Result<Option<StatsSyncHandle>, String> {
	let file = OpenOptions::new()
		.read(true)
		.write(true)
		.create(true)
		.mode(0o600)
		.open(lock_path)
		.map_err(|error| error.to_string())?;
	// SAFETY: `file` owns a live descriptor for the duration of this call.
	let status = unsafe {
		libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB)
	};
	if status == 0 {
		return Ok(Some(StatsSyncHandle::File(file)));
	}
	let error = io::Error::last_os_error();
	if error.kind() == io::ErrorKind::WouldBlock {
		return Ok(None);
	}
	Err(error.to_string())
}

type LockStat = (u64, u64, u64, i64, i64, i64, i64);

fn lock_stat(meta: &fs::Metadata) -> LockStat {
	(
		meta.dev(),
		meta.ino(),
		meta.len(),
		meta.mtime(),
		meta.mtime_nsec(),
		meta.ctime(),
		meta.ctime_nsec(),
	)
}

fn read_gc_lock_snapshot(
	lock_path: &Path,
) -> std::result::Result<Option<(String, LockStat)>, String> {
	let before = match fs::metadata(lock_path) {
		Ok(meta) => meta,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
		Err(error) => return Err(error.to_string()),
	};
	let text = match fs::read_to_string(lock_path) {
		Ok(text) => text,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
		Err(error) => return Err(error.to_string()),
	};
	let after = match fs::metadata(lock_path) {
		Ok(meta) => meta,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
		Err(error) => return Err(error.to_string()),
	};
	if lock_stat(&before) != lock_stat(&after) {
		return Ok(None);
	}
	Ok(Some((text, lock_stat(&after))))
}

fn gc_lock_pid(text: &str) -> Option<i64> {
	let first = text.split('\n').next()?.trim();
	let pid = first.parse::<i64>().ok()?;
	(pid > 0).then_some(pid)
}

fn process_exists(pid: i64) -> bool {
	if pid <= 0 || pid > i64::from(i32::MAX) {
		return false;
	}
	// SAFETY: `kill(pid, 0)` performs only an existence/permission check.
	let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
	if result == 0 {
		return true;
	}
	let error = io::Error::last_os_error();
	!matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::EINVAL))
}

fn should_break_gc_lock(text: &str, stat: LockStat) -> bool {
	if let Some(pid) = gc_lock_pid(text) {
		return !process_exists(pid);
	}
	let created_at = text
		.split('\n')
		.nth(1)
		.map(str::trim)
		.and_then(crate::foreign_session::parse_iso_timestamp_ms);
	let created_at = created_at.unwrap_or_else(|| {
		// `LockStat.4` is nanoseconds; lock timestamps are compared in millis.
		let millis = stat.4.max(0) as u64 / 1_000_000;
		(stat.3.max(0) as u64)
			.saturating_mul(1_000)
			.saturating_add(millis)
	});
	now_ms().saturating_sub(created_at) > GC_WRITE_GRACE_MS
}

fn gc_lock_snapshot_still_current(
	lock_path: &Path,
	snapshot: &LockStat,
) -> std::result::Result<bool, String> {
	match fs::metadata(lock_path) {
		Ok(meta) => Ok(lock_stat(&meta) == *snapshot),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
		Err(error) => Err(error.to_string()),
	}
}

fn remove_stale_gc_lock(lock_path: &Path) -> std::result::Result<bool, String> {
	let Some((text, snapshot)) = read_gc_lock_snapshot(lock_path)? else {
		return Ok(false);
	};
	if !should_break_gc_lock(&text, snapshot) {
		return Ok(false);
	}
	if !gc_lock_snapshot_still_current(lock_path, &snapshot)? {
		return Ok(false);
	}
	match fs::remove_file(lock_path) {
		Ok(()) => Ok(true),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
		Err(error) => Err(error.to_string()),
	}
}

fn gc_breaker_owner_live(lock_path: &Path) -> std::result::Result<bool, String> {
	let breaker_path = PathBuf::from(format!("{}.break", lock_path.display()));
	match read_gc_lock_snapshot(&breaker_path)? {
		Some((text, _)) => Ok(gc_lock_pid(&text).is_some_and(process_exists)),
		None => Ok(false),
	}
}

/// Serialize every Rust GC acquisition, stale recovery and release on a stable
/// guard inode that is created once and never unlinked. `flock` is released by
/// the kernel on exit, so a crashed GC cannot wedge later runs.
fn open_gc_guard(lock_path: &Path) -> std::result::Result<fs::File, String> {
	use std::os::unix::io::AsRawFd;

	let guard_path = PathBuf::from(format!("{}.guard", lock_path.display()));
	let file = OpenOptions::new()
		.read(true)
		.write(true)
		.create(true)
		.mode(0o600)
		.custom_flags(libc::O_NOFOLLOW)
		.open(&guard_path)
		.map_err(|error| format!("{}: {error}", guard_path.display()))?;
	// SAFETY: `flock` operates on an owned, open descriptor.
	if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
		return Ok(file);
	}
	let error = io::Error::last_os_error();
	match error.raw_os_error() {
		Some(libc::EWOULDBLOCK) => Err(format!(
			"GC already running: {}",
			lock_path.display()
		)),
		_ => Err(format!("{}: {error}", guard_path.display())),
	}
}

fn open_new_gc_lock(
	lock_path: &Path,
) -> std::result::Result<Option<fs::File>, String> {
	match OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(0o600)
		.open(lock_path)
	{
		Ok(file) => Ok(Some(file)),
		Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(None),
		Err(error) => Err(error.to_string()),
	}
}

fn write_gc_lock_owner(file: &mut fs::File) -> std::result::Result<(), String> {
	let content = format!(
		"{}\n{}\n",
		std::process::id(),
		crate::foreign_session::format_iso_timestamp(now_ms())
	);
	file.write_all(content.as_bytes())
		.and_then(|()| file.flush())
		.map_err(|error| error.to_string())
}

fn release_lock_file(lock_path: &Path, file: fs::File) {
	let identity = file.metadata().ok().map(|meta| (meta.dev(), meta.ino()));
	drop(file);
	let Some(identity) = identity else {
		return;
	};
	if let Ok(meta) = fs::metadata(lock_path) {
		if (meta.dev(), meta.ino()) == identity {
			let _ = fs::remove_file(lock_path);
		}
	}
}

fn open_gc_lock(lock_path: &Path) -> std::result::Result<fs::File, String> {
	// A live TS breaker means TS recovery is in progress; never take over
	// underneath it, even when the main lock currently looks absent. Stale
	// breakers are ignored, and Rust no longer creates one.
	if gc_breaker_owner_live(lock_path)? {
		return Err(format!("GC already running: {}", lock_path.display()));
	}
	if let Some(file) = open_new_gc_lock(lock_path)? {
		return Ok(file);
	}
	if !remove_stale_gc_lock(lock_path)? {
		return Err(format!("GC already running: {}", lock_path.display()));
	}
	open_new_gc_lock(lock_path)?
		.ok_or_else(|| format!("GC already running: {}", lock_path.display()))
}

struct GcLock {
	path:   PathBuf,
	_file:  Option<fs::File>,
	_guard: Option<fs::File>,
}

impl Drop for GcLock {
	fn drop(&mut self) {
		if let Some(file) = self._file.take() {
			release_lock_file(&self.path, file);
		}
		// Release the guard only after the TS-visible lock is gone.
		let _ = self._guard.take();
	}
}

fn acquire_gc_lock(agent_dir: &Path) -> std::result::Result<GcLock, String> {
	fs::create_dir_all(agent_dir)
		.map_err(|error| format!("{}: {error}", agent_dir.display()))?;
	let lock_path = agent_dir.join("gc.lock");
	let guard = open_gc_guard(&lock_path)?;
	for _ in 0..2 {
		let mut file = open_gc_lock(&lock_path)?;
		if let Err(error) = write_gc_lock_owner(&mut file) {
			release_lock_file(&lock_path, file);
			return Err(error);
		}
		let owned = match (file.metadata(), fs::metadata(&lock_path)) {
			(Ok(mine), Ok(current)) => {
				(mine.dev(), mine.ino()) == (current.dev(), current.ino())
			},
			_ => false,
		};
		if owned {
			return Ok(GcLock {
				path:   lock_path,
				_file:  Some(file),
				_guard: Some(guard),
			});
		}
		// Only a non-cooperating TS recovery can displace our inode; retry
		// once while still holding the Rust guard.
		release_lock_file(&lock_path, file);
	}
	Err(format!("GC already running: {}", lock_path.display()))
}

fn cleanup_stats_rows_for_archived_sessions(
	paths: &GcPaths,
	cwd: &Path,
	newly_archived: &[SessionMeta],
	errors: &mut Vec<String>,
) -> u64 {
	let db_path = stats_db_path(paths, cwd);
	match path_exists_checked(&db_path) {
		Ok(false) => return 0,
		Ok(true) => {},
		Err(message) => {
			errors.push(format!("stats cleanup: {message}"));
			return 0;
		},
	}
	let mut archived_by_path: BTreeMap<PathBuf, StatsSession> = BTreeMap::new();
	for session in newly_archived {
		archived_by_path.insert(
			gc_path_key(cwd, &session.path),
			StatsSession {
				path: session.path.clone(),
				id: session.id.clone(),
				parent_session: session.parent_session.clone(),
				historical_paths: Vec::new(),
				identities: StatsIdentities::default(),
			},
		);
	}
	for session in collect_archived_stats_sessions(&paths.archive, &paths.sessions, cwd, errors) {
		archived_by_path.insert(gc_path_key(cwd, &session.path), session);
	}
	let retained_sessions = match list_active_sessions_checked(&paths.sessions) {
		Ok(sessions) => sessions,
		Err(message) => {
			errors.push(format!("stats cleanup scan: {message}"));
			return 0;
		},
	};
	let archived_sessions: Vec<StatsSession> = archived_by_path.into_values().collect();
	let result = with_stats_sync_lock(&db_path, || {
		let mut retained_stats = Vec::new();
		for session in &retained_sessions {
			let header = read_session_lineage_header(&session.path)?;
			let Some(header) = header else {
				return Err(format!(
					"session header changed during stats cleanup: {}",
					session.path.display()
				));
			};
			if header.id != session.id {
				return Err(format!(
					"session header changed during stats cleanup: {}",
					session.path.display()
				));
			}
			let historical_paths =
				managed_historical_session_paths(&header, &session.path, &paths.sessions, cwd);
			retained_stats.push(StatsSession {
				path: session.path.clone(),
				id: header.id,
				parent_session: header.parent_session,
				historical_paths,
				identities: StatsIdentities::default(),
			});
		}
		let mut context =
			build_stats_cleanup_plans(&archived_sessions, &retained_stats, &db_path, cwd)?;
		let nodes: Vec<StatsNode> = archived_sessions
			.iter()
			.chain(retained_stats.iter())
			.map(|session| {
				let mut stats_paths = BTreeSet::new();
				stats_paths.insert(gc_path_key(cwd, &session.path));
				for path in &session.historical_paths {
					stats_paths.insert(gc_path_key(cwd, path));
				}
				StatsNode {
					session: session.clone(),
					stats_paths,
					identity_keys: stats_identity_keys(&session.identities),
				}
			})
			.collect();
		populate_stats_transfer_targets(&mut context, &nodes)?;
		reconcile_stats_rows_for_sessions(&db_path, &context.plans)
	});
	match result {
		Ok(deleted) => deleted,
		Err(message) => {
			errors.push(format!("stats cleanup: {message}"));
			0
		},
	}
}

fn gc_text(result: &Value) -> String {
	let agent_dir = escape_terminal_text(result["agentDir"].as_str().unwrap_or_default(), false);
	let mode = if result["apply"].as_bool().unwrap_or(false) {
		"applied"
	} else {
		"dry-run"
	};
	let mut lines = vec![format!("GC {mode} ({agent_dir})")];
	if let Some(blobs) = result.get("blobs") {
		lines.push(format!(
			"blobs: {}/{} files, {}, {} refs",
			blobs["deleted"].as_u64().unwrap_or(0),
			blobs["wouldDelete"].as_u64().unwrap_or(0),
			format_bytes(blobs["bytes"].as_u64().unwrap_or(0)),
			blobs["referenced"].as_u64().unwrap_or(0),
		));
		if blobs["errors"].as_array().is_some_and(|errors| !errors.is_empty()) {
			lines.push(format!("blob errors: {}", blobs["errors"].as_array().map_or(0, Vec::len)));
		}
	}
	if let Some(archive) = result.get("archive") {
		lines.push(format!(
			"sessions: {}/{} archived, {} history rows and {} stats rows removed",
			archive["archived"].as_u64().unwrap_or(0),
			archive["wouldArchive"].as_u64().unwrap_or(0),
			archive["historyRowsDeleted"].as_u64().unwrap_or(0),
			archive["statsRowsDeleted"].as_u64().unwrap_or(0),
		));
		if archive["skippedActive"].as_u64().unwrap_or(0) > 0 {
			lines.push(format!(
				"sessions skipped active: {}",
				archive["skippedActive"].as_u64().unwrap_or(0)
			));
		}
		if archive["errors"].as_array().is_some_and(|errors| !errors.is_empty()) {
			lines.push(format!("session errors: {}", archive["errors"].as_array().map_or(0, Vec::len)));
		}
	}
	if let Some(wal) = result.get("wal") {
		let state = if wal["checkpointed"].as_bool().unwrap_or(false) {
			"checkpointed"
		} else {
			"checkpoint dry-run"
		};
		lines.push(format!(
			"wal: {state}, {} across {} dbs",
			format_bytes(wal["walBytes"].as_u64().unwrap_or(0)),
			wal["databases"].as_array().map_or(0, Vec::len)
		));
	}
	format!("{}\n", lines.join("\n"))
}

enum ConfigLayer {
	Yaml(yaml::Value),
	Json(Value),
}

fn yaml_lookup<'a>(mut value: &'a yaml::Value, segments: &[&str]) -> Option<&'a yaml::Value> {
	for segment in segments {
		value = value.get(segment)?;
	}
	Some(value)
}

fn json_lookup<'a>(mut value: &'a Value, segments: &[&str]) -> Option<&'a Value> {
	for segment in segments {
		value = value.get(*segment)?;
	}
	Some(value)
}

fn read_yaml_layer(path: &Path) -> Option<ConfigLayer> {
	let text = fs::read_to_string(path).ok()?;
	yaml::parse(&text).ok().map(ConfigLayer::Yaml)
}

fn read_json_layer(path: &Path) -> Option<ConfigLayer> {
	let text = fs::read_to_string(path).ok()?;
	serde_json::from_str(&text).ok().map(ConfigLayer::Json)
}

fn first_layer(paths: &[PathBuf], json_last: bool) -> Option<ConfigLayer> {
	for path in paths {
		if let Some(layer) = read_yaml_layer(path) {
			return Some(layer);
		}
	}
	if json_last {
		for path in paths {
			if let Some(layer) = read_json_layer(path) {
				return Some(layer);
			}
		}
	}
	None
}

fn global_gc_layer(agent_dir: &Path) -> Option<ConfigLayer> {
	first_layer(
		&[
			agent_dir.join("config.yml"),
			agent_dir.join("config.yaml"),
			agent_dir.join("settings.json"),
		],
		true,
	)
}

fn project_gc_layers(cwd: &Path) -> Vec<ConfigLayer> {
	let root = cwd.join(".omp");
	[
		root.join("config.yml"),
		root.join("config.yaml"),
		root.join("settings.json"),
	]
	.iter()
	.filter_map(|path| read_yaml_layer(path).or_else(|| read_json_layer(path)))
	.collect()
}

fn layer_bool(layer: &ConfigLayer, segments: &[&str]) -> Option<bool> {
	match layer {
		ConfigLayer::Yaml(value) => {
			let value = yaml_lookup(value, segments)?;
			value
				.as_bool()
				.or_else(|| value.as_str()?.parse::<bool>().ok())
		},
		ConfigLayer::Json(value) => {
			let value = json_lookup(value, segments)?;
			value
				.as_bool()
				.or_else(|| value.as_str()?.parse::<bool>().ok())
		},
	}
}

fn layer_number(layer: &ConfigLayer, segments: &[&str]) -> Option<f64> {
	match layer {
		ConfigLayer::Yaml(value) => {
			let value = yaml_lookup(value, segments)?;
			match value {
				yaml::Value::Int(number) => Some(*number as f64),
				yaml::Value::Float(number) => Some(*number),
				yaml::Value::Str(text) => text.parse::<f64>().ok(),
				_ => None,
			}
		},
		ConfigLayer::Json(value) => {
			let value = json_lookup(value, segments)?;
			value
				.as_f64()
				.or_else(|| value.as_str()?.parse::<f64>().ok())
		},
	}
}


fn gc_config_bool(cwd: &Path, agent_dir: &Path, key: &str, default: bool) -> bool {
	let segments: Vec<&str> = key.split('.').collect();
	for layer in project_gc_layers(cwd) {
		if let Some(value) = layer_bool(&layer, &segments) {
			return value;
		}
	}
	global_gc_layer(agent_dir)
		.and_then(|layer| layer_bool(&layer, &segments))
		.unwrap_or(default)
}

fn gc_config_number(cwd: &Path, agent_dir: &Path, key: &str, default: u64) -> u64 {
	let segments: Vec<&str> = key.split('.').collect();
	for layer in project_gc_layers(cwd) {
		if let Some(value) = layer_number(&layer, &segments) {
			return value.max(0.0) as u64;
		}
	}
	global_gc_layer(agent_dir)
		.and_then(|layer| layer_number(&layer, &segments))
		.map(|number| number.max(0.0) as u64)
		.unwrap_or(default)
}

pub fn run_gc(opts: &GcOptions, cwd: &Path, out: &mut dyn Write, err: &mut dyn Write) -> Result<u8> {
	let paths = GcPaths::resolve(opts.agent_dir.as_deref(), cwd);
	let selected = opts.blobs || opts.archive || opts.wal;
	let (run_blobs, run_archive, run_wal) = if selected {
		(opts.blobs, opts.archive, opts.wal)
	} else {
		(
			gc_config_bool(cwd, &paths.agent_dir, "gc.blobs", true),
			gc_config_bool(cwd, &paths.agent_dir, "gc.archive", true),
			gc_config_bool(cwd, &paths.agent_dir, "gc.wal", true),
		)
	};
	let cold_days = opts
		.cold_archive_after_days
		.map(u64::from)
		.unwrap_or_else(|| gc_config_number(cwd, &paths.agent_dir, "gc.coldArchiveAfterDays", 30));
	let retain_global = opts
		.retain_newest_global
		.map(u64::from)
		.unwrap_or_else(|| gc_config_number(cwd, &paths.agent_dir, "gc.retainNewestGlobal", 20));
	let retain_cwd = opts
		.retain_newest_per_cwd
		.map(u64::from)
		.unwrap_or_else(|| gc_config_number(cwd, &paths.agent_dir, "gc.retainNewestPerCwd", 10));

	let _lock = acquire_gc_lock(&paths.agent_dir).map_err(Error::new)?;
	let mut result = json!({
		"agentDir": display_path(&paths.agent_dir),
		"apply": opts.apply,
		"lockPath": display_path(&paths.lock_path),
	});
	if run_blobs {
		result["blobs"] = run_blob_gc(&paths, opts.apply).map_err(Error::new)?;
	}
	if run_archive {
		result["archive"] = run_archive_gc(
			&paths,
			cwd,
			opts.apply,
			cold_days,
			retain_global,
			retain_cwd,
		)
		.map_err(Error::new)?;
	}
	if run_wal {
		result["wal"] = run_wal_gc(&paths, opts.apply).map_err(Error::new)?;
	}

	if opts.json {
		write_line(out, &serde_json::to_string_pretty(&result)?)?;
	} else {
		out.write_all(gc_text(&result).as_bytes())?;
	}

	let mut gc_errors: Vec<String> = Vec::new();
	if let Some(blobs) = result.get("blobs").and_then(|blobs| blobs["errors"].as_array()) {
		gc_errors.extend(
			blobs
				.iter()
				.filter_map(Value::as_str)
				.map(|error| format!("blobs: {error}")),
		);
	}
	if let Some(archive) = result.get("archive").and_then(|archive| archive["errors"].as_array()) {
		gc_errors.extend(
			archive
				.iter()
				.filter_map(Value::as_str)
				.map(|error| format!("archive: {error}")),
		);
	}
	if !gc_errors.is_empty() {
		write_line(
			err,
			&format!(
				"GC completed with {} error{}:",
				gc_errors.len(),
				if gc_errors.len() == 1 { "" } else { "s" }
			),
		)?;
		for error in gc_errors {
			write_line(err, &format!("- {error}"))?;
		}
		return Ok(1);
	}
	Ok(0)
}

// ───────────────────────────────────────────────────────────────────────────
// `omp setup` probes
// ───────────────────────────────────────────────────────────────────────────

pub struct SetupOptions {
	pub component: Option<String>,
	pub check:     bool,
	pub json:      bool,
}

fn python_env_dir() -> PathBuf {
	dirs::root_subdir("python-env", AgentDirCategory::Data)
}

fn managed_python_binary(env_dir: &Path) -> PathBuf {
	if cfg!(windows) {
		env_dir.join("Scripts").join("python.exe")
	} else {
		env_dir.join("bin").join("python")
	}
}

fn managed_python_path() -> PathBuf {
	managed_python_binary(&python_env_dir())
}

fn venv_runtime(venv: &Path, base: &HashMap<String, String>) -> Option<pi_agent::eval::env::InterpreterRuntime> {
	let bin = if cfg!(windows) {
		venv.join("Scripts")
	} else {
		venv.join("bin")
	};
	let program = if cfg!(windows) {
		bin.join("python.exe")
	} else {
		bin.join("python")
	};
	if !program.is_file() {
		return None;
	}
	let mut env = base.clone();
	env.insert("VIRTUAL_ENV".to_string(), venv.display().to_string());
	let separator = if cfg!(windows) { ";" } else { ":" };
	let bin_text = bin.display().to_string();
	let path = match env.get("PATH") {
		Some(current) if !current.is_empty() => format!("{bin_text}{separator}{current}"),
		_ => bin_text,
	};
	env.insert("PATH".to_string(), path);
	Some(pi_agent::eval::env::InterpreterRuntime {
		program: program.clone(),
		env,
		label: program.display().to_string(),
		venv: Some(venv.to_path_buf()),
	})
}

/// Mirrors TS setup/runtime candidate order while substituting the reported
/// profile/XDG-aware managed env for the shared helper's `$HOME/.omp` default:
/// explicit interpreter, active/project venv, managed env, PATH.
fn python_candidates(
	cwd: &Path,
	base: &HashMap<String, String>,
	interpreter: Option<&str>,
	managed_env: &Path,
) -> Vec<pi_agent::eval::env::InterpreterRuntime> {
	if let Some(interpreter) = interpreter {
		return vec![pi_agent::eval::env::explicit_python(interpreter, cwd, base)];
	}
	let mut candidates = Vec::new();
	let mut seen = HashSet::new();
	let mut push = |runtime: pi_agent::eval::env::InterpreterRuntime| {
		if seen.insert(runtime.program.clone()) {
			candidates.push(runtime);
		}
	};
	let venv = base
		.get("VIRTUAL_ENV")
		.map(PathBuf::from)
		.or_else(|| base.get("CONDA_PREFIX").map(PathBuf::from))
		.or_else(|| {
			[cwd.join(".venv"), cwd.join("venv")]
				.into_iter()
				.find(|candidate| candidate.is_dir())
		});
	if let Some(venv) = venv {
		if let Some(runtime) = venv_runtime(&venv, base) {
			push(runtime);
		}
	}
	if let Some(runtime) = venv_runtime(managed_env, base) {
		push(runtime);
	}
	for binary in ["python", "python3"] {
		if let Some(program) = pi_agent::eval::env::which(binary, base) {
			push(pi_agent::eval::env::InterpreterRuntime::new(program, base.clone()));
		}
	}
	candidates
}

async fn probe_python(
	cwd: &Path,
	interpreter: Option<&str>,
	managed_env: &Path,
) -> (bool, Option<PathBuf>) {
	let env: HashMap<String, String> = std::env::vars_os()
		.filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
		.collect();
	let base = pi_agent::eval::env::filter_env(pi_agent::eval::types::EvalLanguage::Py, &env);
	let runtimes = python_candidates(cwd, &base, interpreter, managed_env);
	if runtimes.is_empty() {
		return (false, None);
	}
	let candidates: Vec<pi_agent::eval::probe::ProbeCandidate> = runtimes
		.iter()
		.map(|runtime| pi_agent::eval::probe::ProbeCandidate {
			command: vec![
				runtime.program.display().to_string(),
				"-c".to_string(),
				"import sys;sys.exit(0)".to_string(),
			],
			env: runtime.env.clone(),
			label: runtime.label.clone(),
		})
		.collect();
	let cancellation = CancellationToken::new();
	match pi_agent::eval::probe::probe_candidates(&candidates, cwd, &cancellation, None).await {
		pi_agent::eval::probe::CandidateProbe::Ok(index) => {
			(true, runtimes.get(index).map(|runtime| runtime.program.clone()))
		},
		_ => (false, None),
	}
}

/// Preserve the TypeScript selection/argv contract: effective
/// `python.interpreter`, else active/project venv, managed env, then PATH; probe
/// with `-c` in `cwd`. This is a trust decision, not a sandbox: the selected
/// interpreter can run project startup hooks and code with user permissions.
fn run_setup_python(opts: &SetupOptions, cwd: &Path, out: &mut dyn Write, err: &mut dyn Write) -> Result<u8> {
	let interpreter = setting_string(cwd, "python.interpreter");
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|error| Error::new(error.to_string()))?;
	let managed = managed_python_path();
	let (available, python_path) =
		runtime.block_on(probe_python(cwd, interpreter.as_deref(), &python_env_dir()));
	let using_managed = python_path.as_deref() == Some(managed.as_path());

	if opts.json {
		let mut report = json!({ "available": available });
		if let Some(path) = &python_path {
			report["pythonPath"] = json!(display_path(path));
		}
		report["usingManagedEnv"] = json!(using_managed);
		report["managedEnvPath"] = json!(display_path(&python_env_dir()));
		write_line(out, &serde_json::to_string_pretty(&report)?)?;
		return Ok(if available { 0 } else { 1 });
	}

	let Some(python_path) = python_path else {
		write_line(err, "✘ Python not found")?;
		write_line(err, "Install Python 3.8+ or set python.interpreter to its executable path")?;
		return Ok(1);
	};

	write_line(out, &format!("Python: {}", python_path.display()))?;
	if using_managed {
		write_line(out, &format!("Using managed environment: {}", python_env_dir().display()))?;
	}
	if available {
		write_line(out, "\n✔ Python execution is ready")?;
		Ok(0)
	} else {
		write_line(err, "\n✘ Python interpreter reported failure")?;
		Ok(1)
	}
}

fn tiny_models_cache_dir() -> PathBuf {
	dirs::agent_subdir("cache/tiny-models", AgentDirCategory::Cache)
}

fn regular_nonempty_file(path: &Path) -> bool {
	fs::metadata(path).is_ok_and(|meta| meta.is_file() && meta.len() > 0)
}

fn stt_model_cached(cache_dir: &Path, model_key: &str) -> bool {
	let (repo, spec) = match model_key {
		"fast" => ("onnx-community/whisper-base", None),
		"balanced" => ("onnx-community/whisper-small", None),
		"turbo" => ("onnx-community/whisper-large-v3-turbo", None),
		"parakeet" => (
			"csukuangfj/sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8",
			Some(["encoder.int8.onnx", "decoder.int8.onnx", "joiner.int8.onnx", "tokens.txt"]),
		),
		// Unknown/stale keys resolve to the SoTA default, like resolveSttModelSpec.
		_ => (
			"csukuangfj/sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8",
			Some(["encoder.int8.onnx", "decoder.int8.onnx", "joiner.int8.onnx", "tokens.txt"]),
		),
	};
	let repo_dir = cache_dir.join(repo);
	if let Some(required) = spec {
		let Ok(entries) = fs::read_dir(&repo_dir) else {
			return false;
		};
		let names: HashSet<String> = entries
			.flatten()
			.filter_map(|entry| entry.file_name().to_str().map(str::to_string))
			.collect();
		return required
			.iter()
			.all(|file| names.contains(*file) && regular_nonempty_file(&repo_dir.join(file)));
	}
	let Ok(entries) = fs::read_dir(&repo_dir) else {
		return false;
	};
	let names: HashSet<String> = entries
		.flatten()
		.filter_map(|entry| entry.file_name().to_str().map(str::to_string))
		.collect();
	if !names.contains("config.json") || !regular_nonempty_file(&repo_dir.join("config.json")) {
		return false;
	}
	let Ok(onnx_entries) = fs::read_dir(repo_dir.join("onnx")) else {
		return false;
	};
	let mut has_encoder = false;
	let mut has_decoder = false;
	for entry in onnx_entries.flatten() {
		let Some(name) = entry.file_name().to_str().map(str::to_string) else {
			continue;
		};
		if name.starts_with("encoder")
			&& name.ends_with(".onnx")
			&& regular_nonempty_file(&entry.path())
		{
			has_encoder = true;
		}
		if name.starts_with("decoder")
			&& name.ends_with(".onnx")
			&& regular_nonempty_file(&entry.path())
		{
			has_decoder = true;
		}
	}
	has_encoder && has_decoder
}

fn tts_runtime_cached(tiny: &Path) -> bool {
	let Some(parent) = tiny.parent() else {
		return false;
	};
	let runtime_dir = parent
		.join("tts-runtime")
		.join("kokoro-1.2.1");
	let package = runtime_dir
		.join("node_modules")
		.join("kokoro-js")
		.join("package.json");
	if !regular_nonempty_file(&package) {
		return false;
	}
	let Ok(text) = fs::read_to_string(package) else {
		return false;
	};
	let Ok(value) = serde_json::from_str::<Value>(&text) else {
		return false;
	};
	value.get("version").and_then(Value::as_str) == Some("1.2.1")
}

fn tts_model_cached(tiny: &Path, model_key: &str) -> bool {
	if model_key != "kokoro" {
		return false;
	}
	let repo_dir = tiny.join("onnx-community").join("Kokoro-82M-v1.0-ONNX");
	let Ok(entries) = fs::read_dir(&repo_dir) else {
		return false;
	};
	let mut has_weights = false;
	for entry in entries.flatten() {
		let path = entry.path();
		if path.is_dir() {
			if collect_jsonl_files(&path, ".onnx")
				.iter()
				.any(|file| regular_nonempty_file(file))
			{
				has_weights = true;
			}
			continue;
		}
		if path.extension().and_then(|ext| ext.to_str()) == Some("onnx")
			&& regular_nonempty_file(&path)
		{
			has_weights = true;
		}
	}
	has_weights && tts_runtime_cached(tiny)
}

fn speech_report(cwd: &Path) -> Vec<Value> {
	speech_report_with_cache(cwd, &tiny_models_cache_dir())
}

fn speech_report_with_cache(cwd: &Path, tiny: &Path) -> Vec<Value> {
	let stt_key = setting_string(cwd, "stt.modelName").unwrap_or_else(|| "parakeet".to_string());
	let tts_key = setting_string(cwd, "tts.localModel").unwrap_or_else(|| "kokoro".to_string());
	let stt_cached = stt_model_cached(tiny, &stt_key);
	let tts_cached = tts_model_cached(tiny, &tts_key);
	// Cache presence is not execution readiness: this binary has no verified
	// STT/TTS backend, so `ready` stays false until a real runtime probe exists.
	vec![
		json!({
			"name": "Speech-to-Text model",
			"cached": stt_cached,
			"ready": false,
			"status": if stt_cached {
				format!("{stt_key} — cached; execution runtime not verified")
			} else {
				format!("{stt_key} — not downloaded")
			},
		}),
		json!({
			"name": "Text-to-Speech model",
			"cached": tts_cached,
			"ready": false,
			"status": if tts_cached {
				format!("{tts_key} — cached; execution runtime not verified")
			} else {
				format!("{tts_key} — model/runtime not installed")
			},
		}),
	]
}

fn run_setup_speech(opts: &SetupOptions, cwd: &Path, out: &mut dyn Write) -> Result<u8> {
	run_setup_speech_with_cache(opts, cwd, &tiny_models_cache_dir(), out)
}

fn run_setup_speech_with_cache(
	opts: &SetupOptions,
	cwd: &Path,
	tiny: &Path,
	out: &mut dyn Write,
) -> Result<u8> {
	let report = speech_report_with_cache(cwd, tiny);
	let all_ready = report.iter().all(|component| component["ready"].as_bool().unwrap_or(false));
	if opts.json {
		let mut object = serde_json::Map::new();
		for component in &report {
			let name = component["name"].as_str().unwrap_or_default().to_string();
			object.insert(
				name,
				json!({
					"cached": component["cached"],
					"ready": component["ready"],
					"status": component["status"],
				}),
			);
		}
		write_line(out, &serde_json::to_string_pretty(&Value::Object(object))?)?;
		return Ok(if all_ready { 0 } else { 1 });
	}
	if opts.check {
		write_line(out, "Speech dependencies:")?;
		for component in &report {
			let mark = if component["ready"].as_bool().unwrap_or(false) {
				"[ok]"
			} else {
				"[missing]"
			};
			write_line(
				out,
				&format!(
					"  {mark} {}: {}",
					component["name"].as_str().unwrap_or_default(),
					component["status"].as_str().unwrap_or_default()
				),
			)?;
		}
		return Ok(if all_ready { 0 } else { 1 });
	}
	Err(Error::new(
		"setup speech requires --check or --json in this headless build (model downloads are not available).",
	))
}

pub fn run_setup(opts: &SetupOptions, cwd: &Path, out: &mut dyn Write, err: &mut dyn Write) -> Result<u8> {
	let Some(component) = opts.component.as_deref().map(str::trim).filter(|c| !c.is_empty()) else {
		if opts.check || opts.json {
			return Err(Error::new("setup --check/--json requires a COMPONENT (python|speech)"));
		}
		return Err(Error::new("omp setup requires an interactive TTY."));
	};
	match component {
		"python" => run_setup_python(opts, cwd, out, err),
		"speech" => run_setup_speech(opts, cwd, out),
		other => Err(Error::new(format!(
			"Unknown component: {other}\nValid components: python, speech"
		))),
	}
}

#[cfg(test)]
mod tests;
