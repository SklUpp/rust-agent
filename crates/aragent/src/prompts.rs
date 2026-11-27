//! Prompt resolution from files.
//!
//! Order (CLI input first, most specific file next):
//! `--system-prompt` > `<cwd>/.omp/SYSTEM.md` > `~/.aragent/agent/SYSTEM.md`
//! > legacy `~/.omp/agent/SYSTEM.md` > `share/aragent/system-prompt.md` (with
//! `share/omp` as a read-only fallback) > the bundled `prompts/system-prompt.md`.
//! `APPEND_SYSTEM.md` is additive on the same
//! roots; `PERSONALITY.md`, `RULES.md` and `AGENTS.md` contribute context
//! files. A CLI value is a file path when that file exists (unless it contains
//! a newline, which makes it a literal prompt) — `resolvePromptInput`
//! semantics from `packages/coding-agent/src/system-prompt.ts`.

use std::fs;
use std::path::{Path, PathBuf};

use crate::{Result, dirs};

/// Bundled default system prompt, used when no CLI or file prompt exists.
const DEFAULT_SYSTEM_PROMPT: &str = include_str!("../prompts/system-prompt.md");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextFile {
	pub path: PathBuf,
	pub content: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Prompts {
	/// Fully resolved system prompt, if any source provided one.
	pub system: Option<String>,
	/// Additive append-system prompt.
	pub append: Option<String>,
	/// `PERSONALITY.md` / `RULES.md` / `AGENTS.md` contents, project first.
	pub files: Vec<ContextFile>,
}

/// CLI prompt input: a literal when it contains a newline, otherwise file
/// contents when readable, otherwise the literal string.
#[must_use]
pub fn resolve_input(input: &str) -> String {
	if input.contains('\n') {
		return input.to_string();
	}
	fs::read_to_string(input).unwrap_or_else(|_| input.to_string())
}

pub fn resolve(cli_system: Option<&str>, cli_append: Option<&str>, cwd: &Path) -> Result<Prompts> {
	resolve_with_share_dirs(cli_system, cli_append, cwd, &dirs::share_dirs())
}

/// `resolve` with explicit share roots, so tests never depend on installed
/// executable-relative or `/usr/share` prompt files.
fn resolve_with_share_dirs(
	cli_system: Option<&str>,
	cli_append: Option<&str>,
	cwd: &Path,
	share_dirs: &[PathBuf],
) -> Result<Prompts> {
	let project = dirs::project_config_dir(cwd);
	let agent = dirs::agent_dir();
	let legacy_agent = dirs::legacy_agent_dir().filter(|dir| *dir != agent);

	let system = match cli_system {
		Some(input) => Some(resolve_input(input)),
		None => {
			let mut candidates = vec![project.join("SYSTEM.md"), agent.join("SYSTEM.md")];
			candidates.extend(legacy_agent.as_ref().map(|dir| dir.join("SYSTEM.md")));
			candidates.extend(share_dirs.iter().map(|dir| dir.join("system-prompt.md")));
			first_file(candidates).or_else(|| Some(DEFAULT_SYSTEM_PROMPT.to_string()))
		},
	};

	let append = match cli_append {
		Some(input) => Some(resolve_input(input)),
		None => first_file(
			[
				Some(project.join("APPEND_SYSTEM.md")),
				Some(agent.join("APPEND_SYSTEM.md")),
				legacy_agent.as_ref().map(|dir| dir.join("APPEND_SYSTEM.md")),
			]
			.into_iter()
			.flatten(),
		),
	};

	let mut files = Vec::new();
	for name in ["PERSONALITY.md", "RULES.md", "AGENTS.md"] {
		// Project + current user always contribute; the legacy user file only
		// fills the user slot when the current agent dir has no such file.
		let current_user = agent.join(name);
		let user = if current_user.is_file() {
			current_user
		} else {
			legacy_agent.as_ref().map_or(current_user, |dir| dir.join(name))
		};
		for path in [project.join(name), user] {
			if let Ok(content) = fs::read_to_string(&path)
				&& !content.trim().is_empty()
			{
				files.push(ContextFile { path, content });
			}
		}
	}
	Ok(Prompts { system, append, files })
}

/// First readable non-empty file among `candidates`.
fn first_file(candidates: impl IntoIterator<Item = PathBuf>) -> Option<String> {
	for path in candidates {
		if let Ok(content) = fs::read_to_string(&path)
			&& !content.trim().is_empty()
		{
			return Some(content);
		}
	}
	None
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::env;
	use std::io::Write as _;
	use std::sync::atomic::{AtomicU32, Ordering};

	static COUNTER: AtomicU32 = AtomicU32::new(0);

	fn temp_dir() -> PathBuf {
		let unique = format!(
			"aragent-prompts-{}-{}",
			std::process::id(),
			COUNTER.fetch_add(1, Ordering::Relaxed)
		);
		let dir = env::temp_dir().join(unique);
		fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	fn write(path: &Path, text: &str) {
		fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
		let mut file = fs::File::create(path).expect("create");
		file.write_all(text.as_bytes()).expect("write");
	}

	#[test]
	fn cli_input_with_newline_is_literal() {
		assert_eq!(resolve_input("line one\nline two"), "line one\nline two");
	}

	#[test]
	fn cli_input_reads_existing_file_else_literal() {
		let dir = temp_dir();
		let path = dir.join("prompt.md");
		write(&path, "from file");
		assert_eq!(resolve_input(path.to_str().expect("utf8")), "from file");
		assert_eq!(resolve_input("not a file"), "not a file");
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn project_files_win_over_agent_files_and_append_is_additive() {
		let _env = crate::TEST_ENV_LOCK.lock().expect("env lock");
		let dir = temp_dir();
		let project = dir.join(".omp");
		let agent = dir.join("agent");
		// Isolate the real user config: the agent dir is derived from env.
		// SAFETY: single-threaded unit test; restored below.
		let previous_agent = env::var_os("PI_CODING_AGENT_DIR");
		let previous_config = env::var_os("PI_CONFIG_DIR");
		unsafe {
			env::set_var("PI_CONFIG_DIR", &dir);
			env::set_var("PI_CODING_AGENT_DIR", &agent);
		}
		write(&project.join("SYSTEM.md"), "project system");
		write(&agent.join("SYSTEM.md"), "agent system");
		write(&agent.join("APPEND_SYSTEM.md"), "append tail");
		write(&project.join("AGENTS.md"), "project agents");
		write(&agent.join("AGENTS.md"), "agent agents");

		let prompts = resolve(None, None, &dir).expect("resolve");
		assert_eq!(prompts.system.as_deref(), Some("project system"));
		assert_eq!(prompts.append.as_deref(), Some("append tail"));
		let contents: Vec<&str> = prompts
			.files
			.iter()
			.map(|file| file.content.as_str())
			.collect();
		assert_eq!(contents, ["project agents", "agent agents"]);

		fs::remove_dir_all(&dir).ok();
		unsafe {
			match previous_agent {
				Some(value) => env::set_var("PI_CODING_AGENT_DIR", value),
				None => env::remove_var("PI_CODING_AGENT_DIR"),
			}
			match previous_config {
				Some(value) => env::set_var("PI_CONFIG_DIR", value),
				None => env::remove_var("PI_CONFIG_DIR"),
			}
		}
	}

	/// Isolates the env-derived agent roots; restores them on drop.
	struct IsolatedEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);

	impl IsolatedEnv {
		fn new(root: &Path) -> Self {
			let keys = [
				"PI_CONFIG_DIR",
				"PI_CODING_AGENT_DIR",
				"ARAGENT_PROFILE",
				"OMP_PROFILE",
				"PI_PROFILE",
			];
			let saved: Vec<_> = keys
				.into_iter()
				.map(|key| (key, env::var_os(key)))
				.collect();
			// SAFETY: guarded by TEST_ENV_LOCK for the test's lifetime.
			unsafe {
				env::set_var("PI_CONFIG_DIR", root.join("config"));
				env::set_var("PI_CODING_AGENT_DIR", root.join("config/agent"));
				for key in ["ARAGENT_PROFILE", "OMP_PROFILE", "PI_PROFILE"] {
					env::remove_var(key);
				}
			}
			Self(saved)
		}
	}

	impl Drop for IsolatedEnv {
		fn drop(&mut self) {
			// SAFETY: guarded by TEST_ENV_LOCK for the test's lifetime.
			unsafe {
				for (key, value) in self.0.drain(..) {
					match value {
						Some(value) => env::set_var(key, value),
						None => env::remove_var(key),
					}
				}
			}
		}
	}

	#[test]
	fn bundled_default_fills_prompt_when_no_files_exist() {
		let _env = crate::TEST_ENV_LOCK.lock().expect("env lock");
		let dir = temp_dir();
		let _isolated = IsolatedEnv::new(&dir);

		let prompts = resolve_with_share_dirs(None, None, &dir.join("work"), &[]).expect("resolve");
		assert_eq!(prompts.system.as_deref(), Some(DEFAULT_SYSTEM_PROMPT));
		assert_eq!(prompts.append, None);
		assert!(prompts.files.is_empty());

		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn project_agent_share_and_cli_prompts_override_bundled_default() {
		let _env = crate::TEST_ENV_LOCK.lock().expect("env lock");
		let dir = temp_dir();
		let _isolated = IsolatedEnv::new(&dir);
		let work = dir.join("work");
		let agent = dir.join("config/agent");
		let shares = [dir.join("share")];

		let project = work.join(".omp/SYSTEM.md");
		write(&project, "project system");
		assert_eq!(
			resolve_with_share_dirs(None, None, &work, &shares)
				.expect("resolve")
				.system
				.as_deref(),
			Some("project system")
		);
		fs::remove_file(&project).expect("remove project system");

		let agent_system = agent.join("SYSTEM.md");
		write(&agent_system, "agent system");
		assert_eq!(
			resolve_with_share_dirs(None, None, &work, &shares)
				.expect("resolve")
				.system
				.as_deref(),
			Some("agent system")
		);
		fs::remove_file(&agent_system).expect("remove agent system");

		write(&dir.join("share/system-prompt.md"), "share system");
		assert_eq!(
			resolve_with_share_dirs(None, None, &work, &shares)
				.expect("resolve")
				.system
				.as_deref(),
			Some("share system")
		);

		assert_eq!(
			resolve_with_share_dirs(Some("cli system"), None, &work, &shares)
				.expect("resolve")
				.system
				.as_deref(),
			Some("cli system")
		);

		fs::remove_dir_all(dir).ok();
	}
}
