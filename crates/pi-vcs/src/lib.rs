//! In-process version control for the coding agent.
//!
//! Wraps gitoxide behind a small repository handle. The git binary survives
//! only where an in-process implementation cannot reach parity: credential-bound
//! network transfers (push/fetch/clone reuse the user's ssh config and
//! credential helpers) and reftable repositories (no library implementation of
//! reftable exists yet).
//!
//! Discovery ([`GitRepo::discover`]) is a pure filesystem walk, cheap enough to
//! call from synchronous render paths.
//!
//! [`GitRepo::discover`]: git::GitRepo::discover

pub mod error;
pub mod git;
pub mod types;

use std::{
	path::{Path, PathBuf},
	sync::Arc,
};

pub use error::{Error, Result};
pub use types::*;

/// A discovered repository.
#[derive(Debug, Clone)]
pub struct Repo(Arc<git::GitRepo>);

impl Repo {
	/// Checkout root.
	pub fn root(&self) -> &Path {
		self.0.root()
	}

	/// Primary checkout root.
	pub fn primary_root(&self) -> PathBuf {
		self.0.primary_root()
	}

	/// Path of `dir` relative to the checkout root.
	pub fn prefix_of(&self, dir: &Path) -> Option<String> {
		self.0.prefix_of(dir)
	}

	/// Human label for the working copy: the current branch.
	pub fn label(&self) -> Result<Option<String>> {
		self.0.current_branch()
	}

	/// Commit id currently checked out by the working copy.
	pub fn head_id(&self) -> Result<Option<String>> {
		self.0.head_sha()
	}

	/// Count the current staged, unstaged, and untracked changes.
	pub fn status_summary(&self) -> Result<StatusSummary> {
		self.0.status_summary()
	}

	/// Render current changes in git porcelain-v1 form.
	pub fn status_porcelain(&self, options: &StatusOptions) -> Result<String> {
		self.0.status_porcelain(options)
	}

	/// Render the selected changes as a git-format patch.
	pub fn diff_text(&self, options: &DiffOptions) -> Result<String> {
		self.0.diff_text(options)
	}

	/// Return changed paths, using destination paths for renames.
	pub fn changed_files(&self, options: &DiffOptions) -> Result<Vec<String>> {
		self.0.changed_files(options)
	}

	/// Return per-file added and removed line counts.
	pub fn numstat(&self, options: &DiffOptions) -> Result<Vec<NumstatEntry>> {
		self.0.numstat(options)
	}

	/// Render every working-copy change since the last commit.
	pub fn uncommitted_diff(&self, files: &[String]) -> Result<String> {
		if self.0.head_sha()?.is_none() {
			let unstaged = self
				.0
				.diff_text(&DiffOptions { files: files.to_vec(), ..DiffOptions::default() })?;
			let staged = self.0.diff_text(&DiffOptions {
				cached: true,
				files: files.to_vec(),
				..DiffOptions::default()
			})?;
			Ok(git::join_patches(&[unstaged, staged]))
		} else {
			self.0.diff_text(&DiffOptions {
				base: Some("HEAD".to_owned()),
				files: files.to_vec(),
				..DiffOptions::default()
			})
		}
	}

	/// Return recent commit subjects, newest first.
	pub fn log_subjects(&self, count: usize) -> Result<Vec<String>> {
		self.0.log_subjects(count)
	}

	/// Return recent commits in compact one-line form.
	pub fn log_onelines(&self, count: usize) -> Result<Vec<String>> {
		self.0.log_onelines(count)
	}

	/// Read commit identity, parents, author, date, and full message.
	pub fn commit_details(&self, rev: &str) -> Result<CommitDetails> {
		self.0.commit_details(rev)
	}

	/// List tracked paths, or untracked paths when `others` is true.
	pub fn ls_files(&self, others: bool, exclude_standard: bool) -> Result<Vec<String>> {
		self.0.ls_files(others, exclude_standard)
	}

	/// Filesystem target whose metadata changes when the repository head moves.
	pub fn watch_target(&self) -> PathBuf {
		self.0.head_watch_target()
	}

	/// Access the underlying git handle.
	pub const fn as_git(&self) -> &Arc<git::GitRepo> {
		&self.0
	}
}

/// Detect the repository containing `dir`.
///
/// Returns `Ok(None)` for directories outside any git repository.
pub fn detect(dir: &Path) -> Result<Option<Repo>> {
	Ok(git::GitRepo::discover(dir)?.map(|repo| Repo(Arc::new(repo))))
}

#[cfg(test)]
mod tests {
	use std::{fs, path::Path, process::Command};

	use super::*;

	fn run_git(root: &Path, args: &[&str]) -> String {
		let output = Command::new("git")
			.current_dir(root)
			.args(args)
			.output()
			.unwrap();
		assert!(output.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&output.stderr));
		String::from_utf8(output.stdout).unwrap()
	}

	fn init_git(root: &Path) {
		run_git(root, &["init", "-q"]);
		run_git(root, &["config", "user.name", "VCS Test"]);
		run_git(root, &["config", "user.email", "vcs@example.com"]);
	}

	#[test]
	fn detect_finds_git_checkout() {
		let dir = tempfile::tempdir().unwrap();
		init_git(dir.path());
		let repo = detect(dir.path()).unwrap().unwrap();
		assert_eq!(repo.root(), dir.path().canonicalize().unwrap());
	}

	#[test]
	fn detect_returns_none_outside_repository() {
		let dir = tempfile::tempdir().unwrap();
		assert!(detect(dir.path()).unwrap().is_none());
	}

	#[test]
	fn git_uncommitted_diff_matches_head_with_staged_and_unstaged_changes() {
		let temp = tempfile::tempdir().unwrap();
		init_git(temp.path());
		fs::write(temp.path().join("tracked.txt"), "base\n").unwrap();
		run_git(temp.path(), &["add", "tracked.txt"]);
		run_git(temp.path(), &["commit", "-q", "-m", "base"]);

		fs::write(temp.path().join("tracked.txt"), "base\nunstaged\n").unwrap();
		fs::write(temp.path().join("staged.txt"), "staged\n").unwrap();
		run_git(temp.path(), &["add", "staged.txt"]);

		let expected = run_git(temp.path(), &["diff", "HEAD"]);
		let actual = detect(temp.path())
			.unwrap()
			.unwrap()
			.uncommitted_diff(&[])
			.unwrap();
		assert_eq!(actual, expected);
	}

	#[test]
	fn git_uncommitted_diff_matches_both_unborn_head_comparisons() {
		let temp = tempfile::tempdir().unwrap();
		init_git(temp.path());
		fs::write(temp.path().join("new.txt"), "staged\n").unwrap();
		run_git(temp.path(), &["add", "new.txt"]);
		fs::write(temp.path().join("new.txt"), "staged\nunstaged\n").unwrap();

		let expected = git::join_patches(&[
			run_git(temp.path(), &["diff"]),
			run_git(temp.path(), &["diff", "--cached"]),
		]);
		let actual = detect(temp.path())
			.unwrap()
			.unwrap()
			.uncommitted_diff(&[])
			.unwrap();
		assert_eq!(actual, expected);
	}
}
