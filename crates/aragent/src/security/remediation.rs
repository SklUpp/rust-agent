//! Isolated workspaces for security remediation.
//!
//! Anchors: `packages/coding-agent/src/security/remediation.ts`.
//!
//! Remediation starts from a clean parent checkout because the isolated
//! worktree is materialised from `HEAD`: inheriting staged, unstaged, or
//! untracked changes would make the remediation result ambiguous. The
//! workspace itself reuses the task-isolation lifecycle, so backend fallback,
//! git-metadata detachment, ownership markers, and cleanup stay identical to
//! the rest of the runtime.

use std::{
	collections::HashSet,
	fs,
	path::{Path, PathBuf},
};

use pi_agent::tools::task_isolation::{
	self, IsolationCommitStyle, IsolationMergeMode, IsolationPolicy, IsolationSandbox, RepoBaseline,
	WorktreeBaseline,
};

/// Request for one isolated remediation workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityRemediationRequest {
	/// Directory inside the repository to remediate.
	pub cwd: PathBuf,
	/// Findings the workspace is allowed to address; blanks are dropped and
	/// duplicates collapse to the first occurrence.
	pub finding_ids: Vec<String>,
	/// Stable isolation id; defaults to `security-remediation-<uuid>`.
	pub isolation_id: Option<String>,
	/// Task-isolation base directory. Defaults to `OMP_WORKTREE_DIR` or the
	/// standard worktree root. Kept explicit so tests and embedders never write
	/// outside their own temporary root.
	pub worktree_base: Option<PathBuf>,
}

/// A materialised remediation worktree. Dropping it tears the worktree down;
/// [`SecurityRemediationWorkspace::cleanup`] makes that explicit and reports
/// teardown failures.
#[derive(Debug)]
pub struct SecurityRemediationWorkspace {
	/// Isolation id, stable across the workspace lifetime.
	pub id: String,
	/// Canonical root of the source checkout.
	pub repository_root: PathBuf,
	/// Writable isolated checkout path.
	pub worktree_path: PathBuf,
	/// Findings this workspace was prepared for.
	pub finding_ids: Vec<String>,
	/// Selected isolation backend (`rcopy`, `overlayfs`, ...).
	pub backend: String,
	/// Whether the requested/default backend fell back.
	pub fell_back: bool,
	/// Why fallback happened, when known.
	pub fallback_reason: Option<String>,
	sandbox: IsolationSandbox,
}

impl SecurityRemediationWorkspace {
	/// Tear down the worktree and its isolation base. Safe to call repeatedly;
	/// a teardown failure leaves the sandbox retained for `arag worktree clear`.
	pub fn cleanup(&self) -> crate::Result<()> {
		match self.sandbox.cleanup() {
			None => Ok(()),
			Some(error) => Err(crate::Error::new(error)),
		}
	}
}

/// Prepare a clean, isolated worktree for repairing security findings.
///
/// Fails before creating any directory when the parent checkout or any nested
/// repository is dirty, when the path is not a Git checkout with a commit, or
/// when no finding id was supplied.
pub async fn prepare_security_remediation_workspace(
	request: &SecurityRemediationRequest,
) -> crate::Result<SecurityRemediationWorkspace> {
	let finding_ids = normalize_finding_ids(&request.finding_ids);
	if finding_ids.is_empty() {
		return Err(crate::Error::new("Security remediation requires at least one finding id"));
	}
	let id = request
		.isolation_id
		.as_deref()
		.map(str::trim)
		.filter(|id| !id.is_empty())
		.map_or_else(create_remediation_id, str::to_string);
	let worktree_base = match request.worktree_base.as_deref() {
		Some(base) => std::path::absolute(base).map_err(|error| {
			crate::Error::new(format!(
				"cannot resolve security remediation worktree base {}: {error}",
				base.display()
			))
		})?,
		None => crate::worktree_cmd::resolve_base(None).map_err(crate::Error::new)?,
	};
	let baseline = task_isolation::prepare_context(request.cwd.clone())
		.await
		.map_err(crate::Error::new)?;
	assert_security_remediation_baseline_clean(&baseline)?;
	let repository_root = baseline.root.repo_root.clone();
	assert_security_remediation_tree_inspectable(&repository_root)?;
	let policy = IsolationPolicy {
		merge: IsolationMergeMode::Patch,
		apply: false,
		commits: IsolationCommitStyle::Generic,
		preferred: None,
	};
	let sandbox =
		task_isolation::ensure(repository_root.clone(), id.clone(), policy, Some(worktree_base))
			.await
			.map_err(crate::Error::new)?;
	if let Err(error) = task_isolation::prepare_sandbox(&sandbox, &baseline).await {
		let cleanup = sandbox.cleanup();
		let cleanup =
			cleanup.map_or_else(String::new, |cleanup| format!("; sandbox cleanup failed: {cleanup}"));
		return Err(crate::Error::new(format!(
			"cannot prepare security remediation sandbox: {error}{cleanup}"
		)));
	}
	let worktree_path = sandbox.merged_dir().to_path_buf();
	let backend = sandbox.handle().backend.as_str().to_string();
	let fell_back = sandbox.handle().fell_back;
	let fallback_reason = sandbox.handle().fallback_reason.clone();
	Ok(SecurityRemediationWorkspace {
		id,
		repository_root,
		worktree_path,
		finding_ids,
		backend,
		fell_back,
		fallback_reason,
		sandbox,
	})
}

/// Refuse a dirty baseline before an isolated worktree is materialised.
///
/// The parent must be a Git checkout with a born `HEAD`; a plain snapshot
/// cannot be represented as a detached remediation worktree.
fn assert_security_remediation_baseline_clean(baseline: &WorktreeBaseline) -> crate::Result<()> {
	if baseline.plain.is_some() {
		return Err(crate::Error::new(
			"Security remediation requires a Git checkout; the workspace is not a Git repository",
		));
	}
	if baseline.root.head_commit.is_empty() {
		return Err(crate::Error::new(
			"Security remediation requires a Git checkout with at least one commit",
		));
	}
	let dirty = baseline_dirty_reasons(baseline);
	if dirty.is_empty() {
		return Ok(());
	}
	Err(crate::Error::new(format!(
		"Security remediation refuses a dirty working tree ({}). Commit or stash the changes before creating an isolated remediation workspace.",
		dirty.join(", ")
	)))
}

/// Refuse a repository whose nested directories cannot be inspected.
///
/// Nested-repository discovery skips unreadable directories, which could hide a
/// nested Git repository from both the clean-baseline check and sandbox
/// preparation. Refuse that case before materialising a remediation worktree.
fn assert_security_remediation_tree_inspectable(root: &Path) -> crate::Result<()> {
	fn walk(dir: &Path) -> crate::Result<()> {
		let entries = fs::read_dir(dir).map_err(|error| {
			crate::Error::new(format!(
				"Security remediation cannot inspect directory {}: {error}",
				dir.display()
			))
		})?;
		for entry in entries {
			let entry = entry.map_err(|error| {
				crate::Error::new(format!(
					"Security remediation cannot inspect directory {}: {error}",
					dir.display()
				))
			})?;
			let name = entry.file_name();
			let name = name.to_string_lossy();
			if name == ".git" || name == "node_modules" {
				continue;
			}
			let file_type = entry.file_type().map_err(|error| {
				crate::Error::new(format!(
					"Security remediation cannot inspect {}: {error}",
					entry.path().display()
				))
			})?;
			if !file_type.is_dir() || file_type.is_symlink() {
				continue;
			}
			let path = entry.path();
			if path.join(".git").exists() {
				continue;
			}
			walk(&path)?;
		}
		Ok(())
	}
	walk(root)
}

fn baseline_dirty_reasons(baseline: &WorktreeBaseline) -> Vec<String> {
	let mut dirty = Vec::new();
	append_repo_dirty_reasons(&mut dirty, &baseline.root);
	for nested in &baseline.nested {
		let before = dirty.len();
		append_repo_dirty_reasons(&mut dirty, &nested.baseline);
		if dirty.len() != before {
			dirty.truncate(before);
			dirty.push(format!("dirty nested repository {}", nested.relative_path));
		}
	}
	dirty
}

fn append_repo_dirty_reasons(dirty: &mut Vec<String>, repo: &RepoBaseline) {
	if !patch_is_blank(&repo.staged) {
		dirty.push("staged changes".to_string());
	}
	if !patch_is_blank(&repo.unstaged) {
		dirty.push("unstaged changes".to_string());
	}
	if !repo.untracked.is_empty() || !patch_is_blank(&repo.untracked_patch) {
		dirty.push("untracked files".to_string());
	}
}

fn patch_is_blank(bytes: &[u8]) -> bool {
	bytes.iter().all(u8::is_ascii_whitespace)
}

fn normalize_finding_ids(ids: &[String]) -> Vec<String> {
	let mut seen = HashSet::new();
	let mut normalized = Vec::new();
	for id in ids {
		let id = id.trim();
		if id.is_empty() || !seen.insert(id.to_string()) {
			continue;
		}
		normalized.push(id.to_string());
	}
	normalized
}

fn create_remediation_id() -> String {
	format!("security-remediation-{}", crate::advisor_identity::uuid_v7().replace('-', ""))
}

#[cfg(test)]
mod tests {
	use std::{
		fs,
		path::{Path, PathBuf},
		process::Command,
		sync::atomic::{AtomicU64, Ordering},
	};

	use super::*;

	fn temp_root(label: &str) -> PathBuf {
		static NEXT: AtomicU64 = AtomicU64::new(0);
		let path = std::env::var_os("HOME")
			.map_or_else(std::env::temp_dir, PathBuf::from)
			.join(format!(
				"omp-security-remediation-{label}-{}-{}",
				std::process::id(),
				NEXT.fetch_add(1, Ordering::Relaxed)
			));
		fs::create_dir_all(&path).expect("create temp root");
		path
	}

	fn git(cwd: &Path, args: &[&str]) -> String {
		let output = Command::new("git")
			.arg("-C")
			.arg(cwd)
			.args(args)
			.output()
			.expect("run git");
		assert!(
			output.status.success(),
			"git {args:?} in {} failed: {}",
			cwd.display(),
			String::from_utf8_lossy(&output.stderr)
		);
		String::from_utf8_lossy(&output.stdout).into_owned()
	}

	fn init_repository(label: &str) -> (PathBuf, PathBuf) {
		let root = temp_root(label);
		let repo = root.join("repo");
		fs::create_dir_all(&repo).expect("create repository");
		git(&repo, &["init", "-q"]);
		git(&repo, &["config", "user.name", "Security Test"]);
		git(&repo, &["config", "user.email", "security@example.com"]);
		git(&repo, &["config", "commit.gpgsign", "false"]);
		fs::write(repo.join("tracked.txt"), "base\n").expect("write tracked file");
		git(&repo, &["add", "tracked.txt"]);
		git(&repo, &["commit", "-qm", "initial"]);
		(root, repo)
	}

	fn request(repo: &Path, base: &Path, finding_ids: &[&str]) -> SecurityRemediationRequest {
		SecurityRemediationRequest {
			cwd: repo.to_path_buf(),
			finding_ids: finding_ids.iter().map(|id| (*id).to_string()).collect(),
			isolation_id: Some("fix-1".to_string()),
			worktree_base: Some(base.to_path_buf()),
		}
	}

	fn baseline_with(head: &str) -> RepoBaseline {
		RepoBaseline {
			repo_root: PathBuf::from("/repo"),
			head_commit: head.to_string(),
			staged: Vec::new(),
			unstaged: Vec::new(),
			untracked: Vec::new(),
			untracked_patch: Vec::new(),
			submodules: Vec::new(),
		}
	}

	#[test]
	fn refuses_dirty_baseline_and_reports_each_reason() {
		use pi_agent::tools::task_isolation::NestedBaseline;

		let mut root = baseline_with("root-head");
		root.staged = b"diff --git a/file b/file\n".to_vec();
		let mut nested = baseline_with("nested-head");
		nested.untracked = vec!["src/nested.ts".to_string()];
		let baseline = WorktreeBaseline {
			root,
			nested: vec![NestedBaseline { relative_path: "nested".to_string(), baseline: nested }],
			plain: None,
		};
		let error = assert_security_remediation_baseline_clean(&baseline).expect_err("dirty");
		let message = error.to_string();
		assert!(message.contains("staged changes"), "{message}");
		assert!(message.contains("dirty nested repository nested"), "{message}");

		let clean =
			WorktreeBaseline { root: baseline_with("root-head"), nested: Vec::new(), plain: None };
		assert!(assert_security_remediation_baseline_clean(&clean).is_ok());
	}

	#[tokio::test]
	async fn prepares_clean_workspace_and_cleans_up() {
		let (root, repo) = init_repository("clean");
		let base = root.join("worktrees");
		let request = SecurityRemediationRequest {
			finding_ids: vec![
				" finding-a ".to_string(),
				String::new(),
				"finding-b".to_string(),
				"finding-a".to_string(),
			],
			..request(&repo, &base, &[])
		};
		let workspace = prepare_security_remediation_workspace(&request)
			.await
			.expect("prepare workspace");
		assert_eq!(workspace.id, "fix-1");
		assert_eq!(workspace.finding_ids, vec!["finding-a".to_string(), "finding-b".to_string()]);
		assert_eq!(workspace.repository_root, fs::canonicalize(&repo).expect("canonical repository"));
		assert!(workspace.worktree_path.join("tracked.txt").exists());
		assert!(workspace.worktree_path.join(".git").exists());
		assert!(!workspace.backend.is_empty());
		assert!(
			git(&workspace.worktree_path, &["status", "--porcelain"])
				.trim()
				.is_empty(),
			"remediation worktree must start clean"
		);
		let worktree_path = workspace.worktree_path.clone();
		let base_dir = worktree_path
			.parent()
			.expect("worktree base dir")
			.to_path_buf();
		workspace.cleanup().expect("cleanup");
		assert!(!worktree_path.exists());
		assert!(!base_dir.exists());
		assert!(workspace.cleanup().is_ok(), "cleanup must be idempotent");
		let _ = fs::remove_dir_all(root);
	}

	#[tokio::test]
	async fn refuses_dirty_working_tree_without_creating_a_worktree() {
		let (root, repo) = init_repository("dirty");
		let base = root.join("worktrees");
		fs::write(repo.join("tracked.txt"), "changed\n").expect("dirty tracked file");
		let error = prepare_security_remediation_workspace(&request(&repo, &base, &["finding-a"]))
			.await
			.expect_err("dirty working tree");
		let message = error.to_string();
		assert!(message.contains("dirty working tree"), "{message}");
		assert!(message.contains("unstaged changes"), "{message}");
		assert!(!base.exists(), "dirty refusal must not create the worktree base");

		git(&repo, &["checkout", "--", "tracked.txt"]);
		fs::write(repo.join("staged.txt"), "staged\n").expect("staged file");
		git(&repo, &["add", "staged.txt"]);
		let error = prepare_security_remediation_workspace(&request(&repo, &base, &["finding-a"]))
			.await
			.expect_err("staged changes");
		assert!(error.to_string().contains("staged changes"), "{error}");
		assert!(!base.exists(), "staged refusal must not create the worktree base");

		git(&repo, &["reset", "-q"]);
		let error = prepare_security_remediation_workspace(&request(&repo, &base, &["finding-a"]))
			.await
			.expect_err("untracked files");
		assert!(error.to_string().contains("untracked files"), "{error}");
		assert!(!base.exists(), "untracked refusal must not create the worktree base");
		let _ = fs::remove_dir_all(root);
	}

	#[tokio::test]
	async fn refuses_dirty_nested_repository() {
		let (root, repo) = init_repository("nested");
		fs::write(repo.join(".gitignore"), "nested/\n").expect("gitignore");
		git(&repo, &["add", ".gitignore"]);
		git(&repo, &["commit", "-qm", "ignore nested"]);
		let nested = repo.join("nested");
		fs::create_dir_all(&nested).expect("nested repository");
		git(&nested, &["init", "-q"]);
		git(&nested, &["config", "user.name", "Security Test"]);
		git(&nested, &["config", "user.email", "security@example.com"]);
		git(&nested, &["config", "commit.gpgsign", "false"]);
		fs::write(nested.join("inner.txt"), "inner\n").expect("nested file");
		git(&nested, &["add", "inner.txt"]);
		git(&nested, &["commit", "-qm", "nested initial"]);
		fs::write(nested.join("inner.txt"), "dirty\n").expect("dirty nested file");
		let error = prepare_security_remediation_workspace(&request(
			&repo,
			&root.join("worktrees"),
			&["finding-a"],
		))
		.await
		.expect_err("dirty nested repository");
		assert!(error.to_string().contains("dirty nested repository nested"), "{error}");
		let _ = fs::remove_dir_all(root);
	}

	#[tokio::test]
	async fn rejects_missing_findings_and_non_git_workspaces() {
		let root = temp_root("reject");
		let plain = root.join("plain");
		fs::create_dir_all(&plain).expect("plain directory");
		let error = prepare_security_remediation_workspace(&SecurityRemediationRequest {
			cwd: plain.clone(),
			finding_ids: vec!["   ".to_string()],
			isolation_id: None,
			worktree_base: Some(root.join("worktrees")),
		})
		.await
		.expect_err("missing finding id");
		assert!(error.to_string().contains("at least one finding id"), "{error}");

		let error = prepare_security_remediation_workspace(&SecurityRemediationRequest {
			cwd: plain,
			finding_ids: vec!["finding-a".to_string()],
			isolation_id: None,
			worktree_base: Some(root.join("worktrees")),
		})
		.await
		.expect_err("non-git workspace");
		assert!(error.to_string().contains("Git"), "{error}");
		let _ = fs::remove_dir_all(root);
	}
}
