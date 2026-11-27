//! Safe commit pipeline for `arag commit` (D28).
//!
//! The old command ran `git add -A` over the whole working tree, so any dirty
//! or untracked user file was swept into the commit. This module never does
//! that implicitly:
//!
//! - an occupied index is committed as-is (only what the user staged);
//! - otherwise only *tracked, modified* files are staged, one explicit
//!   `git add -- <paths>` at a time;
//! - lockfiles are excluded from that auto-staging (`exclusions.rs`) and
//!   reported instead of committed;
//! - untracked files are listed but never staged unless the user passes
//!   `--all` or names them with `--files <path>`.
//!
//! `--dry-run` plans everything and mutates nothing. Upstream anchor:
//! `packages/coding-agent/src/commit/{cli,pipeline,execute}.ts`.
//!
//! Path handling is byte-exact: every name comes from `git -z` output and is
//! kept as an [`OsString`], and staging adds with `--literal-pathspecs` so a
//! file named `a*b.txt` or `[tag].txt` can never expand into other paths. All
//! git commands run from the repository root, resolved once, so a `cwd` inside
//! a subdirectory still stages the repo-relative paths git reports.

pub mod conventional;
pub mod exclusions;

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use conventional::ConventionalCommit;

/// Everything `arag commit` accepts. Mirrors `CommitCommandArgs` upstream.
#[derive(Debug, Default, Clone)]
pub struct CommitOptions {
	/// Explicit message; skips conventional inference.
	pub message:      Option<String>,
	/// Explicit paths to stage instead of every tracked change.
	pub files:        Vec<PathBuf>,
	/// Stage every change, untracked files included.
	pub all:          bool,
	/// Print the plan and proposed message without mutating anything.
	pub dry_run:      bool,
	/// Push the current branch after committing.
	pub push:         bool,
	/// Accepted for CLI parity. This build never writes a changelog.
	pub no_changelog: bool,
	/// Keep the old deterministic `update N files` summary.
	pub legacy:       bool,
	/// Extra context for the inferred message.
	pub context:      Option<String>,
	/// Accepted for CLI parity. Inference stays deterministic and local.
	pub model:        Option<String>,
}

/// Plan derived from the repository state before any mutation.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Plan {
	/// Already-staged files. When non-empty, only these are committed.
	staged:            Vec<OsString>,
	/// Tracked changes (or `--files` entries) this run will stage explicitly.
	files:             Vec<OsString>,
	/// `--all` was passed: stage every change.
	all:               bool,
	/// Files used to infer the message (superset of `staged`/`files`).
	candidates:        Vec<OsString>,
	/// Lockfiles skipped by auto-staging.
	skipped_excluded:  Vec<OsString>,
	/// Untracked paths left alone by auto-staging.
	skipped_untracked: Vec<OsString>,
	/// Lockfiles the user named explicitly with `--files`.
	explicit_excluded: Vec<OsString>,
}

/// Run the commit pipeline in `cwd`.
pub fn run_commit(options: &CommitOptions, cwd: &Path) -> Result<(), String> {
	let root = discover_repo_root(cwd)?;
	let git = Git { cwd: &root };

	let unmerged = git.checked("git ls-files -u", ["ls-files", "-u"])?;
	if !unmerged.stdout.is_empty() {
		return Err(
			"cannot commit: unresolved merge conflicts exist; resolve them before committing."
				.to_string(),
		);
	}
	let status = git.checked("git status --porcelain", ["status", "--porcelain"])?;
	if status.stdout.is_empty() {
		println!("Nothing to commit, working tree clean.");
		return Ok(());
	}

	let plan = build_plan(options, &git, cwd)?;
	if plan.staged.is_empty() && plan.files.is_empty() && !plan.all {
		println!("Nothing staged to commit.");
		print_skips(&plan);
		return Ok(());
	}
	if !plan.staged.is_empty() && (options.all || !options.files.is_empty()) {
		println!("Note: the index already has staged work; committing only those files.");
	}
	if !plan.explicit_excluded.is_empty() {
		println!(
			"Note: staging explicitly requested lockfile(s): {}",
			join_list(&plan.explicit_excluded)
		);
	}
	if !plan.staged.is_empty() && !plan.skipped_excluded.is_empty() {
		println!(
			"Note: committing explicitly staged lockfile(s): {}",
			join_list(&plan.skipped_excluded)
		);
	}

	let targets = if plan.staged.is_empty() {
		&plan.candidates
	} else {
		&plan.staged
	};
	let target_names = display_paths(targets);
	let message = build_message_text(options, &target_names)?;

	if options.dry_run {
		println!("Dry run: nothing staged or committed.");
		if !plan.staged.is_empty() {
			println!("Would commit already staged: {}", join_list(&plan.staged));
		} else if plan.all {
			println!("Would stage every change (`--all`), untracked files included.");
		} else {
			println!("Would stage: {}", join_list(&plan.files));
		}
		print_skips(&plan);
		println!("Proposed commit message:\n\n{message}");
		if options.push {
			println!("Dry run: skipping push.");
		}
		return Ok(());
	}

	if plan.staged.is_empty() {
		if plan.all {
			git.add_all()?;
		} else {
			git.add(&plan.files)?;
		}
	}
	let staged_now =
		git.nul_paths("git diff --cached -z --name-only", ["diff", "--cached", "-z", "--name-only"])?;
	if staged_now.is_empty() {
		println!("Nothing staged to commit.");
		print_skips(&plan);
		return Ok(());
	}
	git.commit(&message)?;
	println!("Committed: {}", message.lines().next().unwrap_or_default());
	if options.push {
		git.push()?;
		println!("Pushed to remote.");
	}
	Ok(())
}

fn build_plan(
	options: &CommitOptions,
	git: &Git<'_>,
	invocation_dir: &Path,
) -> Result<Plan, String> {
	let staged =
		git.nul_paths("git diff --cached -z --name-only", ["diff", "--cached", "-z", "--name-only"])?;
	if !staged.is_empty() {
		let (_, skipped_excluded) = split_excluded(staged.clone());
		return Ok(Plan { candidates: staged.clone(), staged, skipped_excluded, ..Plan::default() });
	}
	if options.all {
		let mut candidates =
			git.nul_paths("git diff -z --name-only", ["diff", "-z", "--name-only"])?;
		candidates.extend(git.nul_paths(
			"git ls-files -z --others --exclude-standard",
			["ls-files", "-z", "--others", "--exclude-standard"],
		)?);
		candidates.sort();
		candidates.dedup();
		return Ok(Plan { all: true, candidates, ..Plan::default() });
	}
	if !options.files.is_empty() {
		let mut files: Vec<OsString> = options
			.files
			.iter()
			.map(|file| resolve_repo_path(invocation_dir, git.cwd, file))
			.collect();
		files.sort();
		files.dedup();
		let explicit_excluded = files
			.iter()
			.filter(|file| is_excluded_path(file))
			.cloned()
			.collect();
		return Ok(Plan { candidates: files.clone(), files, explicit_excluded, ..Plan::default() });
	}
	let modified = git.nul_paths("git diff -z --name-only", ["diff", "-z", "--name-only"])?;
	let (files, mut skipped_excluded) = split_excluded(modified);
	let untracked = git.nul_paths(
		"git ls-files -z --others --exclude-standard",
		["ls-files", "-z", "--others", "--exclude-standard"],
	)?;
	let (skipped_untracked, untracked_excluded) = split_excluded(untracked);
	skipped_excluded.extend(untracked_excluded);
	Ok(Plan {
		candidates: files.clone(),
		files,
		skipped_excluded,
		skipped_untracked,
		..Plan::default()
	})
}

fn build_message_text(options: &CommitOptions, files: &[String]) -> Result<String, String> {
	match options.message.as_deref() {
		Some(raw) => {
			let trimmed = raw.trim();
			if trimmed.is_empty() {
				return Err("commit message is empty".to_string());
			}
			Ok(trimmed.to_string())
		},
		None => {
			if options.model.is_some() {
				println!(
					"Note: --model is accepted for CLI parity; this build infers the message locally."
				);
			}
			let commit: ConventionalCommit =
				conventional::build_message(files, options.context.as_deref(), None, options.legacy);
			conventional::validate_message(&commit)?;
			Ok(conventional::format_commit(&commit))
		},
	}
}

fn print_skips(plan: &Plan) {
	if !plan.skipped_excluded.is_empty() {
		println!(
			"Skipped lockfiles (excluded from auto-staging): {}",
			join_list(&plan.skipped_excluded)
		);
		println!(
			"Stage them explicitly with `git add -- <path>` if the bump belongs in this commit."
		);
	}
	if !plan.skipped_untracked.is_empty() {
		println!("Left untracked (not staged automatically): {}", join_list(&plan.skipped_untracked));
		println!("Pass `--all` or `--files <path>` to include them.");
	}
}

/// Resolve `--files` entries, which are relative to the invocation directory,
/// to paths relative to the repository root git commands run from.
fn resolve_repo_path(invocation_dir: &Path, root: &Path, file: &Path) -> OsString {
	let absolute = if file.is_absolute() {
		file.to_path_buf()
	} else {
		let directory =
			std::fs::canonicalize(invocation_dir).unwrap_or_else(|_| invocation_dir.to_path_buf());
		directory.join(file)
	};
	let root_real = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
	match absolute.strip_prefix(&root_real) {
		Ok(relative) if !relative.as_os_str().is_empty() => relative.as_os_str().to_os_string(),
		// Off-root or lexical mismatch: hand the path to git unchanged.
		_ => file.as_os_str().to_os_string(),
	}
}

fn is_excluded_path(path: &OsStr) -> bool {
	exclusions::is_excluded_file(&path.to_string_lossy())
}

fn split_excluded(files: Vec<OsString>) -> (Vec<OsString>, Vec<OsString>) {
	let mut stageable = Vec::new();
	let mut excluded = Vec::new();
	for file in files {
		if is_excluded_path(&file) {
			excluded.push(file);
		} else {
			stageable.push(file);
		}
	}
	(stageable, excluded)
}

fn display_paths(paths: &[OsString]) -> Vec<String> {
	paths
		.iter()
		.map(|path| path.to_string_lossy().into_owned())
		.collect()
}

fn join_list(items: &[OsString]) -> String {
	const LIMIT: usize = 20;
	let shown = items.len().min(LIMIT);
	let mut joined = display_paths(&items[..shown]).join(", ");
	if items.len() > LIMIT {
		joined.push_str(&format!(", +{} more", items.len() - LIMIT));
	}
	joined
}

/// Resolve the work-tree root git paths are reported against.
fn discover_repo_root(cwd: &Path) -> Result<PathBuf, String> {
	let status = Command::new("git")
		.args(["rev-parse", "--is-inside-work-tree"])
		.current_dir(cwd)
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.status()
		.map_err(|err| format!("git not available: {err}"))?;
	if !status.success() {
		return Err(format!("not a git repository: {}", cwd.display()));
	}
	let git = Git { cwd };
	let output = git.checked("git rev-parse --show-toplevel", ["rev-parse", "--show-toplevel"])?;
	let root = os_from_bytes(trim_line_end(&output.stdout));
	if root.is_empty() {
		return Ok(cwd.to_path_buf());
	}
	let root = PathBuf::from(root);
	Ok(if root.is_absolute() {
		root
	} else {
		cwd.join(root)
	})
}

/// Strip exactly one line terminator: `\n` on Unix, `\r\n`/`\n` on Windows.
/// A directory name that really ends in `\r` (or another `\n`) is preserved.
fn trim_line_end(bytes: &[u8]) -> &[u8] {
	#[cfg(not(windows))]
	{
		bytes.strip_suffix(b"\n").unwrap_or(bytes)
	}
	#[cfg(windows)]
	{
		match bytes.strip_suffix(b"\r\n") {
			Some(rest) => rest,
			None => bytes.strip_suffix(b"\n").unwrap_or(bytes),
		}
	}
}

fn os_from_bytes(bytes: &[u8]) -> OsString {
	#[cfg(unix)]
	{
		use std::os::unix::ffi::OsStringExt;
		OsString::from_vec(bytes.to_vec())
	}
	#[cfg(not(unix))]
	{
		OsString::from(String::from_utf8_lossy(bytes).into_owned())
	}
}

/// Thin `git` CLI wrapper; the aragent binary already shells out to git.
struct Git<'a> {
	cwd: &'a Path,
}

impl Git<'_> {
	fn checked<I, S>(&self, label: &str, args: I) -> Result<Output, String>
	where
		I: IntoIterator<Item = S>,
		S: AsRef<OsStr>,
	{
		let output = Command::new("git")
			.args(args)
			.current_dir(self.cwd)
			.output()
			.map_err(|err| format!("{label} failed: {err}"))?;
		if !output.status.success() {
			let stderr = String::from_utf8_lossy(&output.stderr);
			let detail = stderr.trim();
			let code = output.status.code().unwrap_or(-1);
			return Err(if detail.is_empty() {
				format!("{label} exited {code}")
			} else {
				format!("{label} exited {code}: {detail}")
			});
		}
		Ok(output)
	}

	/// NUL-delimited path listing; names stay byte-exact.
	fn nul_paths<I, S>(&self, label: &str, args: I) -> Result<Vec<OsString>, String>
	where
		I: IntoIterator<Item = S>,
		S: AsRef<OsStr>,
	{
		let output = self.checked(label, args)?;
		Ok(output
			.stdout
			.split(|byte| *byte == 0)
			.filter(|chunk| !chunk.is_empty())
			.map(os_from_bytes)
			.collect())
	}

	fn add(&self, files: &[OsString]) -> Result<(), String> {
		let mut args: Vec<OsString> = vec!["--literal-pathspecs".into(), "add".into(), "--".into()];
		args.extend(files.iter().cloned());
		self.checked("git add", args).map(|_| ())
	}

	fn add_all(&self) -> Result<(), String> {
		self.checked("git add -A", ["add", "-A"]).map(|_| ())
	}

	fn commit(&self, message: &str) -> Result<(), String> {
		self
			.checked("git commit", ["commit", "-m", message])
			.map(|_| ())
	}

	fn push(&self) -> Result<(), String> {
		self.checked("git push", ["push"]).map(|_| ())
	}
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use super::*;

	fn git_output(dir: &Path, args: &[&str]) -> Output {
		Command::new("git")
			.args(args)
			.current_dir(dir)
			.output()
			.expect("git must be available for commit tests")
	}

	fn git_ok(dir: &Path, args: &[&str]) -> String {
		let output = git_output(dir, args);
		assert!(
			output.status.success(),
			"git {args:?} failed: {}",
			String::from_utf8_lossy(&output.stderr)
		);
		String::from_utf8_lossy(&output.stdout).trim().to_string()
	}

	fn temp_repo(name: &str) -> Option<PathBuf> {
		let dir = std::env::temp_dir().join(format!("aragent-commit-{name}-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		std::fs::create_dir_all(&dir).ok()?;
		let init = Command::new("git")
			.args(["init", "-q", "."])
			.current_dir(&dir)
			.status()
			.ok()?;
		if !init.success() {
			let _ = std::fs::remove_dir_all(&dir);
			return None;
		}
		git_ok(&dir, &["config", "user.email", "test@example.com"]);
		git_ok(&dir, &["config", "user.name", "Test"]);
		git_ok(&dir, &["config", "commit.gpgsign", "false"]);
		Some(dir)
	}

	fn write(dir: &Path, path: &str, contents: &str) {
		let full = dir.join(path);
		if let Some(parent) = full.parent() {
			std::fs::create_dir_all(parent).expect("create parent dir");
		}
		std::fs::write(full, contents).expect("write fixture file");
	}

	fn seed_and_commit(dir: &Path, files: &[(&str, &str)]) {
		for (path, contents) in files {
			write(dir, path, contents);
		}
		git_ok(dir, &["add", "-A"]);
		git_ok(dir, &["commit", "-q", "-m", "initial"]);
	}

	fn commit_count(dir: &Path) -> usize {
		let output = git_output(dir, &["rev-list", "--count", "HEAD"]);
		if !output.status.success() {
			return 0;
		}
		String::from_utf8_lossy(&output.stdout)
			.trim()
			.parse()
			.unwrap_or(0)
	}

	fn head_files(dir: &Path) -> Vec<String> {
		let output = git_output(dir, &["show", "-z", "--name-only", "--format=", "HEAD"]);
		assert!(
			output.status.success(),
			"git show failed: {}",
			String::from_utf8_lossy(&output.stderr)
		);
		String::from_utf8_lossy(&output.stdout)
			.split('\0')
			.filter(|part| !part.is_empty())
			.map(str::to_string)
			.collect()
	}

	fn head_subject(dir: &Path) -> String {
		git_ok(dir, &["show", "-s", "--format=%s", "HEAD"])
	}

	fn status_lines(dir: &Path) -> Vec<String> {
		git_ok(dir, &["status", "--porcelain"])
			.lines()
			.map(str::to_string)
			.filter(|line| !line.is_empty())
			.collect()
	}

	#[test]
	fn auto_stage_skips_lockfiles_and_untracked_files() {
		let Some(dir) = temp_repo("skip-locks") else {
			return;
		};
		seed_and_commit(&dir, &[("src/lib.rs", "a"), ("Cargo.lock", "v1")]);
		write(&dir, "src/lib.rs", "b");
		write(&dir, "Cargo.lock", "v2");
		write(&dir, "notes.txt", "untracked");

		run_commit(&CommitOptions::default(), &dir).expect("commit succeeds");

		assert_eq!(commit_count(&dir), 2);
		assert_eq!(head_files(&dir), vec!["src/lib.rs"]);
		let status = status_lines(&dir);
		assert!(status.iter().any(|line| line.contains("Cargo.lock")), "status: {status:?}");
		assert!(status.iter().any(|line| line.contains("notes.txt")), "status: {status:?}");

		// Nothing else is stageable: a second run must not invent a commit.
		run_commit(&CommitOptions::default(), &dir).expect("second run succeeds quietly");
		assert_eq!(commit_count(&dir), 2);
	}

	#[test]
	fn dry_run_never_mutates_index_or_history() {
		let Some(dir) = temp_repo("dry-run") else {
			return;
		};
		seed_and_commit(&dir, &[("src/lib.rs", "a")]);
		write(&dir, "src/lib.rs", "b");

		let index_before = git_ok(&dir, &["ls-files", "-s"]);
		let head_before = git_ok(&dir, &["rev-parse", "HEAD"]);
		let options = CommitOptions { dry_run: true, ..CommitOptions::default() };
		run_commit(&options, &dir).expect("dry run succeeds");

		assert_eq!(git_ok(&dir, &["ls-files", "-s"]), index_before);
		assert_eq!(git_ok(&dir, &["rev-parse", "HEAD"]), head_before);
		assert_eq!(commit_count(&dir), 1);

		let plan = build_plan(&options, &Git { cwd: &dir }, &dir).expect("plan");
		assert_eq!(plan.files, vec![OsString::from("src/lib.rs")]);
		assert!(plan.skipped_untracked.is_empty());
	}

	#[test]
	fn explicit_message_is_used_verbatim() {
		let Some(dir) = temp_repo("explicit-message") else {
			return;
		};
		seed_and_commit(&dir, &[("src/lib.rs", "a")]);
		write(&dir, "src/lib.rs", "b");

		let options = CommitOptions {
			message: Some("chore: release 1.0".to_string()),
			..CommitOptions::default()
		};
		run_commit(&options, &dir).expect("commit succeeds");
		assert_eq!(head_subject(&dir), "chore: release 1.0");
		assert_eq!(head_files(&dir), vec!["src/lib.rs"]);
	}

	#[test]
	fn already_staged_work_is_committed_alone() {
		let Some(dir) = temp_repo("staged-only") else {
			return;
		};
		seed_and_commit(&dir, &[("a.rs", "a"), ("b.rs", "b")]);
		write(&dir, "a.rs", "a2");
		write(&dir, "b.rs", "b2");
		git_ok(&dir, &["add", "b.rs"]);

		run_commit(&CommitOptions::default(), &dir).expect("commit succeeds");
		assert_eq!(head_files(&dir), vec!["b.rs"]);
		let status = status_lines(&dir);
		assert!(status.iter().any(|line| line.contains("a.rs")), "status: {status:?}");
	}

	#[test]
	fn explicit_files_limit_the_commit() {
		let Some(dir) = temp_repo("explicit-files") else {
			return;
		};
		seed_and_commit(&dir, &[("a.rs", "a"), ("b.rs", "b")]);
		write(&dir, "a.rs", "a2");
		write(&dir, "b.rs", "b2");

		let options =
			CommitOptions { files: vec![PathBuf::from("a.rs")], ..CommitOptions::default() };
		run_commit(&options, &dir).expect("commit succeeds");
		assert_eq!(head_files(&dir), vec!["a.rs"]);
		let status = status_lines(&dir);
		assert!(status.iter().any(|line| line.contains("b.rs")), "status: {status:?}");
	}

	#[test]
	fn untracked_only_change_does_not_commit() {
		let Some(dir) = temp_repo("untracked-only") else {
			return;
		};
		seed_and_commit(&dir, &[("tracked.rs", "a")]);
		write(&dir, "untracked.txt", "junk");

		run_commit(&CommitOptions::default(), &dir).expect("nothing to do is not an error");
		assert_eq!(commit_count(&dir), 1);
		assert!(
			status_lines(&dir)
				.iter()
				.any(|line| line.contains("untracked.txt"))
		);
	}

	#[test]
	fn all_flag_sweeps_untracked_files_on_request() {
		let Some(dir) = temp_repo("all-flag") else {
			return;
		};
		seed_and_commit(&dir, &[("tracked.rs", "a")]);
		write(&dir, "tracked.rs", "b");
		write(&dir, "fresh.txt", "new");

		let options = CommitOptions { all: true, ..CommitOptions::default() };
		run_commit(&options, &dir).expect("commit succeeds");
		let files = head_files(&dir);
		assert!(files.contains(&"tracked.rs".to_string()), "files: {files:?}");
		assert!(files.contains(&"fresh.txt".to_string()), "files: {files:?}");
	}

	#[test]
	fn lockfile_only_change_reports_and_does_not_commit() {
		let Some(dir) = temp_repo("lockfile-only") else {
			return;
		};
		seed_and_commit(&dir, &[("Cargo.lock", "v1")]);
		write(&dir, "Cargo.lock", "v2");

		run_commit(&CommitOptions::default(), &dir).expect("skipping is not an error");
		assert_eq!(commit_count(&dir), 1);

		let options = CommitOptions::default();
		let plan = build_plan(&options, &Git { cwd: &dir }, &dir).expect("plan");
		assert_eq!(plan.skipped_excluded, vec![OsString::from("Cargo.lock")]);
		assert!(plan.files.is_empty());
	}

	#[test]
	fn clean_tree_reports_ok_without_committing() {
		let Some(dir) = temp_repo("clean-tree") else {
			return;
		};
		run_commit(&CommitOptions::default(), &dir).expect("a clean tree is not an error");
		assert_eq!(commit_count(&dir), 0);
	}

	#[test]
	fn failing_git_probe_is_surfaced() {
		let Some(dir) = temp_repo("corrupt-index") else {
			return;
		};
		write(&dir, "tracked.txt", "x");
		git_ok(&dir, &["add", "-A"]);
		std::fs::write(dir.join(".git/index"), b"garbage").expect("corrupt index");

		let error = run_commit(&CommitOptions::default(), &dir).expect_err("broken index must fail");
		assert!(error.starts_with("git ls-files -u exited"), "unexpected message: {error}");
	}

	#[test]
	fn legacy_and_context_are_deterministic() {
		let Some(dir) = temp_repo("legacy-context") else {
			return;
		};
		seed_and_commit(&dir, &[("src/lib.rs", "a")]);
		write(&dir, "src/lib.rs", "b");

		let options = CommitOptions {
			legacy: true,
			context: Some("fix: guard empty input".to_string()),
			..CommitOptions::default()
		};
		run_commit(&options, &dir).expect("commit succeeds");
		assert_eq!(head_subject(&dir), "fix: guard empty input");
	}

	#[test]
	fn subdirectory_cwd_commits_repo_relative_paths() {
		let Some(dir) = temp_repo("subdir-cwd") else {
			return;
		};
		seed_and_commit(&dir, &[("src/lib.rs", "a"), ("root.rs", "r")]);
		write(&dir, "src/lib.rs", "b");

		run_commit(&CommitOptions::default(), &dir.join("src")).expect("commit from a subdirectory");
		assert_eq!(head_files(&dir), vec!["src/lib.rs"]);
		let status = status_lines(&dir);
		assert!(status.is_empty(), "a clean tree is expected, status: {status:?}");
	}

	#[test]
	fn subdirectory_cwd_resolves_explicit_files() {
		let Some(dir) = temp_repo("subdir-files") else {
			return;
		};
		seed_and_commit(&dir, &[("src/lib.rs", "a"), ("root.rs", "r")]);
		write(&dir, "src/lib.rs", "b");
		write(&dir, "root.rs", "r2");

		let options =
			CommitOptions { files: vec![PathBuf::from("lib.rs")], ..CommitOptions::default() };
		run_commit(&options, &dir.join("src")).expect("explicit file resolves against cwd");
		assert_eq!(head_files(&dir), vec!["src/lib.rs"]);
	}

	#[test]
	fn literal_pathspecs_do_not_expand_globs() {
		let Some(dir) = temp_repo("literal-pathspecs") else {
			return;
		};
		seed_and_commit(&dir, &[("a*b.txt", "x"), ("[tag].txt", "x"), ("keep.txt", "k")]);
		write(&dir, "a*b.txt", "y");
		write(&dir, "[tag].txt", "y");
		// Untracked decoys: a pathspec glob would sweep these in.
		write(&dir, "anb.txt", "decoy");
		write(&dir, "t.txt", "decoy");

		run_commit(&CommitOptions::default(), &dir).expect("commit succeeds");
		assert_eq!(head_files(&dir), vec!["[tag].txt", "a*b.txt"]);
		let status = status_lines(&dir);
		assert!(status.iter().any(|line| line.contains("anb.txt")), "status: {status:?}");
		assert!(status.iter().any(|line| line.contains("t.txt")), "status: {status:?}");
	}

	#[test]
	fn nul_delimited_names_keep_non_ascii_and_whitespace() {
		let Some(dir) = temp_repo("odd-names") else {
			return;
		};
		let names = ["café.txt", "line\nname.txt", "sp ace.txt", "tab\tname.txt"];
		let mut seed = vec![("base.txt", "b")];
		seed.extend(names.iter().map(|name| (*name, "x")));
		seed_and_commit(&dir, &seed);
		for name in names {
			write(&dir, name, "y");
		}

		run_commit(&CommitOptions::default(), &dir).expect("commit succeeds");
		let mut expected: Vec<String> = names.iter().map(|name| (*name).to_string()).collect();
		expected.sort();
		assert_eq!(head_files(&dir), expected);
	}

	#[test]
	fn repo_root_trim_preserves_name_bytes() {
		assert_eq!(trim_line_end(b"/repo\n"), b"/repo");
		#[cfg(windows)]
		assert_eq!(trim_line_end(b"/repo\r\n"), b"/repo");
		#[cfg(not(windows))]
		{
			// Unix git writes `\n`; a name byte before it is not a terminator.
			assert_eq!(trim_line_end(b"/repo\r\n"), b"/repo\r");
			assert_eq!(trim_line_end(b"/repo\r"), b"/repo\r");
		}
		assert_eq!(trim_line_end(b"/repo\n\n"), b"/repo\n");
		assert_eq!(trim_line_end(b"/re\rpo"), b"/re\rpo");
		assert_eq!(trim_line_end(b"/repo"), b"/repo");
	}

	#[cfg(unix)]
	#[test]
	fn explicit_files_accept_non_utf8_names() {
		use std::os::unix::ffi::OsStringExt;

		let Some(dir) = temp_repo("non-utf8-files") else {
			return;
		};
		let name = OsString::from_vec(b"bad-\xff.txt".to_vec());
		std::fs::write(dir.join(&name), "a").expect("write fixture file");
		git_ok(&dir, &["add", "-A"]);
		git_ok(&dir, &["commit", "-q", "-m", "initial"]);
		std::fs::write(dir.join(&name), "b").expect("write fixture file");

		let options =
			CommitOptions { files: vec![PathBuf::from(name.as_os_str())], ..CommitOptions::default() };
		run_commit(&options, &dir).expect("non-UTF-8 explicit path commits");
		assert_eq!(head_files(&dir), vec!["bad-\u{fffd}.txt"]);
	}
}
