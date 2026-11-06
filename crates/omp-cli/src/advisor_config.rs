//! `WATCHDOG.yml`/`WATCHDOG.yaml` discovery for the advisor roster.
//!
//! Mirrors `packages/coding-agent/src/advisor/config.ts` +
//! `advisor/watchdog.ts` on the same user/project search path as `WATCHDOG.md`:
//! the user agent dir plus every directory from `cwd` up to `$HOME`, probing
//! both `<F>` and `.omp/<F>`. Advisors are keyed by slug; a more specific file
//! (project leaf > project ancestor > user) replaces an earlier entry with the
//! same slug. A malformed file is warned about and skipped — a bad project
//! config never kills the session.
//!
//! Boundaries: `@import` expansion is not implemented in this build (the
//! Rust config reader has no import expander); instructions are returned
//! verbatim. `ponytail:` add expansion if the Rust tree ever gains it.

use std::path::{Path, PathBuf};

use crate::runtime::BUILTIN_TOOLS;
use crate::yaml::{self, Value};

/// One advisor declared in a `WATCHDOG.yml` file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdvisorEntry {
	pub name:         String,
	pub model:        Option<String>,
	/// `None` means the default read/grep/glob subset; `Some(vec![])` means no
	/// tools; a non-empty list is already normalized/filtered.
	pub tools:        Option<Vec<String>>,
	pub instructions: Option<String>,
	pub enabled:      Option<bool>,
}

/// Deduped roster plus the concatenated top-level `instructions` baseline.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiscoveredAdvisors {
	pub advisors:            Vec<AdvisorEntry>,
	pub shared_instructions: Option<String>,
}

/// Lowercase, collapse non-alphanumerics to `-`, trim leading/trailing `-`.
/// Falls back to `advisor` when nothing survives; callers dedupe collisions.
#[must_use]
pub fn slugify(name: &str) -> String {
	let mut slug = String::with_capacity(name.len());
	let mut pending_separator = false;
	for ch in name.chars().flat_map(char::to_lowercase) {
		if ch.is_ascii_alphanumeric() {
			if pending_separator && !slug.is_empty() {
				slug.push('-');
			}
			pending_separator = false;
			slug.push(ch);
		} else {
			pending_separator = true;
		}
	}
	if slug.is_empty() {
		"advisor".to_string()
	} else {
		slug
	}
}

/// Normalize legacy aliases and drop names this build cannot mount.
///
/// `undefined` keeps the default subset; an explicit empty list grants no
/// tools; a list whose every entry is unknown falls back to the default subset
/// (TS `filterAdvisorTools`). Unknown names are warned about, never fatal.
#[must_use]
pub fn filter_tools(tools: Option<&[String]>, source: &Path) -> Option<Vec<String>> {
	let tools = tools?;
	if tools.is_empty() {
		return Some(Vec::new());
	}
	let mut filtered: Vec<String> = Vec::new();
	for raw in tools {
		let name = match raw.as_str() {
			"search" => "grep",
			"find" => "glob",
			other => other,
		};
		if !BUILTIN_TOOLS.iter().any(|(known, _)| *known == name) {
			eprintln!(
				"warning: Advisor config {}: dropping unknown tool `{name}`",
				source.display()
			);
			continue;
		}
		if !filtered.iter().any(|kept| kept == name) {
			filtered.push(name.to_string());
		}
	}
	if filtered.is_empty() { None } else { Some(filtered) }
}

/// One readable config candidate with the position metadata that decides merge
/// priority.
struct Candidate {
	path:   PathBuf,
	level:  Level,
	depth:  usize,
	order:  usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Level {
	User,
	Project,
}

/// Number of path steps between two lexical paths (nested or ancestral).
fn relative_depth(left: &Path, right: &Path) -> usize {
	let left: Vec<_> = left.components().collect();
	let right: Vec<_> = right.components().collect();
	let common = left
		.iter()
		.zip(&right)
		.take_while(|(a, b)| a == b)
		.count();
	(left.len() - common) + (right.len() - common)
}

/// Walk the advisor search path. Returns candidates user-first, then project
/// ancestors→leaf (depth descending). Insertion order is preserved by the
/// stable sort, so at one depth `.omp/<F>` precedes `<F>` and the bare file —
/// inserted last — wins the slug map.
fn collect_paths(
	cwd: &Path,
	agent_dir: Option<&Path>,
	filenames: &[&str],
) -> Vec<Candidate> {
	let cwd = crate::dirs::absolute(
		&std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
		cwd,
	);
	let home = crate::dirs::home_dir();
	let mut paths: Vec<PathBuf> = Vec::new();
	let mut user_paths: Vec<PathBuf> = Vec::new();

	if let Some(agent_dir) = agent_dir {
		for filename in filenames {
			let path = crate::dirs::absolute(agent_dir, Path::new(filename));
			if !paths.contains(&path) {
				paths.push(path.clone());
				user_paths.push(path);
			}
		}
	}

	let mut current = cwd.clone();
	loop {
		for filename in filenames {
			for path in [
				current.join(crate::dirs::CONFIG_DIR_NAME).join(filename),
				current.join(filename),
			] {
				if !paths.contains(&path) {
					paths.push(path);
				}
			}
		}
		let stop = home.as_deref().is_some_and(|home| current == home);
		let parent = current.parent().map(Path::to_path_buf);
		match parent {
			Some(parent) if !stop => current = parent,
			_ => break,
		}
	}

	let mut candidates: Vec<Candidate> = Vec::new();
	for (order, path) in paths.into_iter().enumerate() {
		if !path.is_file() {
			continue;
		}
		let is_user = user_paths.contains(&path);
		let owner = if path
			.parent()
			.and_then(Path::file_name)
			.is_some_and(|name| name == crate::dirs::CONFIG_DIR_NAME)
		{
			path.parent().and_then(Path::parent).map(Path::to_path_buf)
		} else {
			path.parent().map(Path::to_path_buf)
		};
		let depth = owner
			.as_deref()
			.map_or(0, |owner| relative_depth(&cwd, owner));
		candidates.push(Candidate {
			path,
			level: if is_user { Level::User } else { Level::Project },
			depth,
			order,
		});
	}

	candidates.sort_by(|left, right| {
		match (left.level, right.level) {
			(Level::User, Level::Project) => return std::cmp::Ordering::Less,
			(Level::Project, Level::User) => return std::cmp::Ordering::Greater,
			_ => {},
		}
		right
			.depth
			.cmp(&left.depth)
			.then(left.order.cmp(&right.order))
	});
	candidates
}

/// Discover advisor configs from `WATCHDOG.yml`/`WATCHDOG.yaml`.
#[must_use]
pub fn discover(cwd: &Path, agent_dir: Option<&Path>) -> DiscoveredAdvisors {
	let candidates = collect_paths(cwd, agent_dir, &["WATCHDOG.yml", "WATCHDOG.yaml"]);
	let mut roster: Vec<AdvisorEntry> = Vec::new();
	let mut shared: Vec<String> = Vec::new();

	for candidate in candidates {
		let Ok(text) = std::fs::read_to_string(&candidate.path) else {
			continue;
		};
		let parsed = match yaml::parse(&text) {
			Ok(parsed) => parsed,
			Err(error) => {
				eprintln!(
					"warning: Advisor config {}: failed to parse YAML: {error}",
					candidate.path.display()
				);
				continue;
			},
		};
		let Some(pairs) = parsed.as_map() else {
			if !matches!(parsed, Value::Null) {
				eprintln!(
					"warning: Advisor config {}: expected a YAML mapping",
					candidate.path.display()
				);
			}
			continue;
		};

		let instructions = pairs
			.iter()
			.find(|(key, _)| key == "instructions")
			.and_then(|(_, value)| value.to_text())
			.map(|text| text.trim().to_string())
			.filter(|text| !text.is_empty());
		if let Some(instructions) = instructions {
			shared.push(instructions);
		}

		let entries = pairs
			.iter()
			.find(|(key, _)| key == "advisors")
			.and_then(|(_, value)| value.as_list());
		let Some(entries) = entries else {
			continue;
		};
		let mut parsed_entries: Vec<AdvisorEntry> = Vec::new();
		let mut invalid = false;
		for entry in entries {
			match parse_entry(entry, &candidate.path) {
				Ok(entry) => parsed_entries.push(entry),
				Err(message) => {
					eprintln!(
						"warning: Advisor config {}: invalid schema: {message}",
						candidate.path.display()
					);
					invalid = true;
					break;
				},
			}
		}
		if invalid {
			continue;
		}
		for entry in parsed_entries {
			let slug = slugify(&entry.name);
			match roster.iter_mut().find(|existing| slugify(&existing.name) == slug) {
				Some(existing) => *existing = entry,
				None => roster.push(entry),
			}
		}
	}

	DiscoveredAdvisors {
		advisors: roster,
		shared_instructions: if shared.is_empty() {
			None
		} else {
			Some(shared.join("\n\n"))
		},
	}
}

fn parse_entry(value: &Value, source: &Path) -> std::result::Result<AdvisorEntry, String> {
	let Some(pairs) = value.as_map() else {
		return Err("advisor entry must be a mapping".to_string());
	};
	let field = |key: &str| pairs.iter().find(|(name, _)| name == key).map(|(_, value)| value);
	let name = field("name")
		.and_then(Value::to_text)
		.filter(|name| !name.trim().is_empty())
		.ok_or_else(|| "`name` must be a non-empty string".to_string())?;
	let model = field("model")
		.and_then(Value::to_text)
		.map(|model| model.trim().to_string())
		.filter(|model| !model.is_empty());
	let tools = match field("tools") {
		None => None,
		Some(value) => {
			let items = value
				.as_list()
				.ok_or_else(|| "`tools` must be a sequence of strings".to_string())?;
			let mut names = Vec::with_capacity(items.len());
			for item in items {
				let name = item
					.to_text()
					.ok_or_else(|| "`tools` must be a sequence of strings".to_string())?;
				names.push(name);
			}
			filter_tools(Some(&names), source)
		},
	};
	let instructions = field("instructions")
		.and_then(Value::to_text)
		.map(|instructions| instructions.trim().to_string())
		.filter(|instructions| !instructions.is_empty());
	let enabled = match field("enabled") {
		None => None,
		Some(value) => Some(
			value
				.as_bool()
				.ok_or_else(|| "`enabled` must be a boolean".to_string())?,
		),
	};
	Ok(AdvisorEntry { name, model, tools, instructions, enabled })
}

/// `WATCHDOG.md` blocks, in the same user→project order, ready to append to the
/// advisor system prompt.
#[must_use]
pub fn discover_watchdog_docs(cwd: &Path, agent_dir: Option<&Path>) -> Vec<String> {
	collect_paths(cwd, agent_dir, &["WATCHDOG.md"])
		.into_iter()
		.filter_map(|candidate| std::fs::read_to_string(&candidate.path).ok())
		.map(|content| {
			format!(
				"Especially pay attention to:\n<attention>\n{}\n</attention>",
				content.trim_end()
			)
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn temp_dir(prefix: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"omp-advisor-config-{prefix}-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap_or_default()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	#[test]
	fn slugify_collapses_and_falls_back() {
		assert_eq!(slugify("Security Reviewer"), "security-reviewer");
		assert_eq!(slugify("  --A__B--  "), "a-b");
		assert_eq!(slugify("***"), "advisor");
	}

	#[test]
	fn unknown_tools_are_dropped_and_empty_result_means_default() {
		let source = Path::new("WATCHDOG.yml");
		let tools = vec!["search".to_string(), "find".to_string(), "nope".to_string()];
		assert_eq!(
			filter_tools(Some(&tools), source),
			Some(vec!["grep".to_string(), "glob".to_string()])
		);
		let bogus = vec!["definitely-not-a-tool".to_string()];
		assert_eq!(filter_tools(Some(&bogus), source), None);
		assert_eq!(filter_tools(Some(&[]), source), Some(Vec::new()));
	}

	#[test]
	fn bare_file_wins_over_dot_omp_at_same_depth_and_project_wins_over_user() {
		let base = temp_dir("merge");
		let agent = base.join("agent");
		let project = base.join("work");
		std::fs::create_dir_all(agent.join(".omp")).expect("agent .omp");
		std::fs::create_dir_all(project.join(".omp")).expect("project .omp");
		std::fs::write(
			agent.join("WATCHDOG.yml"),
			"instructions: shared-user\nadvisors:\n  - name: reviewer\n    model: mock/user\n",
		)
		.expect("user config");
		std::fs::write(
			project.join(".omp").join("WATCHDOG.yml"),
			"advisors:\n  - name: reviewer\n    model: mock/dot-omp\n",
		)
		.expect("project dot config");
		std::fs::write(
			project.join("WATCHDOG.yml"),
			"instructions: shared-project\nadvisors:\n  - name: reviewer\n    model: mock/bare\n  - name: second\n",
		)
		.expect("project bare config");

		let discovered = discover(&project, Some(&agent));
		assert_eq!(discovered.advisors.len(), 2);
		assert_eq!(discovered.advisors[0].name, "reviewer");
		assert_eq!(
			discovered.advisors[0].model.as_deref(),
			Some("mock/bare"),
			"the bare project file is most specific"
		);
		assert_eq!(discovered.advisors[1].name, "second");
		assert_eq!(
			discovered.shared_instructions.as_deref(),
			Some("shared-user\n\nshared-project")
		);
	}

	#[test]
	fn malformed_file_is_skipped_not_fatal() {
		let base = temp_dir("malformed");
		let project = base.join("work");
		std::fs::create_dir_all(project.join(".omp")).expect("project .omp");
		std::fs::write(project.join("WATCHDOG.yml"), "advisors: [\n").expect("broken yaml");
		let discovered = discover(&project, Some(&base.join("agent")));
		assert!(discovered.advisors.is_empty());
	}

	#[test]
	fn watchdog_docs_are_wrapped_for_the_prompt() {
		let base = temp_dir("docs");
		let project = base.join("work");
		std::fs::create_dir_all(project.join(".omp")).expect("project .omp");
		std::fs::write(project.join("WATCHDOG.md"), "Check the auth boundary.").expect("watchdog md");
		let docs = discover_watchdog_docs(&project, Some(&base.join("agent")));
		assert_eq!(docs.len(), 1);
		assert!(docs[0].contains("Check the auth boundary."));
		assert!(docs[0].starts_with("Especially pay attention to:"));
	}
}
