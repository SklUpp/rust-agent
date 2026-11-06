//! C2c acceptance: task-isolation data-loss / recovery / leak guarantees.
//!
//! These tests drive the *public* isolation lifecycle (`prepare_context` →
//! `ensure` → `prepare_sandbox` → child edits → `finalize`) instead of the
//! crate-private helpers used by the unit tests in `task_isolation.rs`, so a
//! guarantee only holds here if the shipped path holds it.
//!
//! One test per parity-plan scenario:
//! 1. dirty baseline is preserved and the task delta stays distinguishable,
//! 2. binary deltas round-trip byte-exactly,
//! 3. committed + uncommitted child work both survive the merge,
//! 4. a conflicting merge retains the recoverable patch and leaves the parent
//!    worktree untouched,
//! 5. cancellation before merge produces no partial merge and no sandbox,
//! 6. the sandbox is owned by the task and is cleaned up without orphans or
//!    collateral deletion.

use std::{
	fs,
	path::{Path, PathBuf},
	process::Command,
	sync::atomic::{AtomicU64, Ordering},
	time::{SystemTime, UNIX_EPOCH},
};

use pi_agent::{
	CancellationToken,
	tools::task_isolation::{
		self as iso, ISOLATION_OWNER_FILE, IsolationMergeMode, IsolationSandbox, WorktreeBaseline,
	},
};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
	fn new(label: &str) -> Self {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_or(0, |elapsed| elapsed.as_nanos());
		let path = std::env::temp_dir().join(format!(
			"omp-iso-accept-{label}-{}-{nanos}-{}",
			std::process::id(),
			SEQ.fetch_add(1, Ordering::SeqCst)
		));
		fs::create_dir_all(&path).expect("temp dir");
		Self(path)
	}

	fn join(&self, name: &str) -> PathBuf {
		self.0.join(name)
	}
}

impl Drop for TempDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}

fn git(dir: &Path, args: &[&str]) -> String {
	let output = Command::new("git")
		.arg("-C")
		.arg(dir)
		.args(args)
		.output()
		.expect("git");
	assert!(
		output.status.success(),
		"git {args:?} in {}: {}",
		dir.display(),
		String::from_utf8_lossy(&output.stderr)
	);
	String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn git_ok(dir: &Path, args: &[&str]) -> bool {
	Command::new("git")
		.arg("-C")
		.arg(dir)
		.args(args)
		.output()
		.expect("git")
		.status
		.success()
}

fn init_repo(dir: &Path) {
	fs::create_dir_all(dir).expect("repo dir");
	git(dir, &["init", "-q"]);
	git(dir, &["config", "user.email", "test@example.com"]);
	git(dir, &["config", "user.name", "test"]);
	fs::write(dir.join("tracked.txt"), "base\n").expect("write");
	git(dir, &["add", "tracked.txt"]);
	git(dir, &["commit", "-qm", "base"]);
}

fn read(path: &Path) -> String {
	fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

/// Rcopy is the always-available backend; `resolve_policy` is the production
/// entry point, so the policy under test is the one a task actually receives.
fn policy(merge: IsolationMergeMode) -> iso::IsolationPolicy {
	let settings = iso::TaskIsolationSettings {
		mode: iso::IsolationMode::Rcopy,
		apply: true,
		merge,
		commits: iso::IsolationCommitStyle::Generic,
		worktree_base: None,
	};
	let request = iso::IsolationRequest {
		isolated: Some(true),
		..Default::default()
	};
	iso::resolve_policy(&settings, &request, true)
		.expect("policy")
		.expect("isolation requested")
}

async fn start_sandbox(
	parent: &Path,
	worktree_base: &Path,
	policy: iso::IsolationPolicy,
	id: &str,
) -> (WorktreeBaseline, IsolationSandbox) {
	let baseline = iso::prepare_context(parent.to_path_buf())
		.await
		.expect("baseline");
	let sandbox = iso::ensure(
		parent.to_path_buf(),
		id.to_string(),
		policy,
		Some(worktree_base.to_path_buf()),
	)
	.await
	.expect("sandbox");
	iso::prepare_sandbox(&sandbox, &baseline)
		.await
		.expect("prepare sandbox");
	(baseline, sandbox)
}

async fn finish(
	sandbox: &IsolationSandbox,
	baseline: &WorktreeBaseline,
	policy: iso::IsolationPolicy,
	id: &str,
	artifacts: &Path,
	cancel: &CancellationToken,
) -> iso::IsolationFinalize {
	iso::finalize(
		sandbox,
		baseline,
		policy,
		id,
		artifacts.to_path_buf(),
		true,
		cancel,
		None,
	)
	.await
}

// ─── 1. dirty baseline ──────────────────────────────────────────────────────

/// A task spawned over uncommitted work must not drop that work, and its own
/// delta must not be contaminated by the inherited dirty state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dirty_baseline_is_preserved_and_task_delta_is_distinct() {
	let temp = TempDir::new("dirty");
	let parent = temp.join("repo");
	init_repo(&parent);
	fs::write(parent.join("tracked.txt"), "base\ndirty\n").expect("dirty");
	fs::write(parent.join("parent-wip.txt"), "parent wip\n").expect("wip");

	let policy = policy(IsolationMergeMode::Patch);
	let (baseline, sandbox) =
		start_sandbox(&parent, &temp.join("wt"), policy, "task-dirty").await;

	fs::write(sandbox.merged_dir().join("tracked.txt"), "base\ndirty\nagent\n")
		.expect("child edit");
	fs::write(sandbox.merged_dir().join("agent.txt"), "agent file\n").expect("child add");

	let out = finish(
		&sandbox,
		&baseline,
		policy,
		"task-dirty",
		&temp.join("artifacts"),
		&CancellationToken::new(),
	)
	.await;
	assert!(out.cleanup_error.is_none(), "{out:?}");
	let report = out.report.clone().expect("report");
	assert_eq!(report.changes_applied, Some(true), "{}", out.summary);

	// Both halves of the new file survive: the user's dirty line and the task's.
	assert_eq!(read(&parent.join("tracked.txt")), "base\ndirty\nagent\n");
	// Pre-existing untracked work is untouched.
	assert_eq!(read(&parent.join("parent-wip.txt")), "parent wip\n");
	assert_eq!(read(&parent.join("agent.txt")), "agent file\n");

	// The captured delta names only the task's change, so a reviewer/merger can
	// tell task output from the baseline WIP it inherited.
	let patch_path = report.patch_path.expect("patch artifact");
	let patch = String::from_utf8_lossy(&fs::read(&patch_path).expect("patch")).into_owned();
	assert!(patch.contains("+agent"), "{patch}");
	assert!(
		!patch.contains("parent wip"),
		"task patch must exclude inherited baseline WIP: {patch}"
	);

	assert!(!sandbox.handle().base_dir.exists(), "sandbox cleaned");
}

// ─── 2. binary deltas ───────────────────────────────────────────────────────

/// Binary content must merge byte-exactly; any lossy text conversion would
/// corrupt the file (and silently destroy user data).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_deltas_round_trip_without_corruption() {
	let temp = TempDir::new("binary");
	let parent = temp.join("repo");
	init_repo(&parent);
	fs::write(parent.join("data.bin"), [0_u8, 1, 2, 3, 0, 255]).expect("bin");
	git(&parent, &["add", "data.bin"]);
	git(&parent, &["commit", "-qm", "add binary"]);

	let child_bin = [0_u8, 1, 2, 255, 0, 128, b'\n', 0xFE];
	let child_new_bin = [0x89_u8, 0x50, 0x4E, 0x47, 0, 0xFF, 0x00, 0x80];

	let policy = policy(IsolationMergeMode::Patch);
	let (baseline, sandbox) =
		start_sandbox(&parent, &temp.join("wt"), policy, "task-binary").await;
	fs::write(sandbox.merged_dir().join("data.bin"), child_bin).expect("child bin");
	fs::write(sandbox.merged_dir().join("new.bin"), child_new_bin).expect("child new bin");

	let out = finish(
		&sandbox,
		&baseline,
		policy,
		"task-binary",
		&temp.join("artifacts"),
		&CancellationToken::new(),
	)
	.await;
	assert!(out.cleanup_error.is_none(), "{out:?}");
	let report = out.report.clone().expect("report");
	assert_eq!(report.changes_applied, Some(true), "{}", out.summary);

	assert_eq!(
		fs::read(parent.join("data.bin")).expect("merged bin"),
		child_bin,
		"modified binary must merge byte-exactly"
	);
	assert_eq!(
		fs::read(parent.join("new.bin")).expect("merged new bin"),
		child_new_bin,
		"new binary must merge byte-exactly"
	);
	// No text-conversion sidecar (e.g. a UTF-8 mangled copy) was introduced.
	let mut names = fs::read_dir(&parent)
		.expect("parent dir")
		.map(|entry| entry.expect("entry").file_name())
		.collect::<Vec<_>>();
	names.sort();
	assert_eq!(
		names,
		vec![
			std::ffi::OsString::from(".git"),
			std::ffi::OsString::from("data.bin"),
			std::ffi::OsString::from("new.bin"),
			std::ffi::OsString::from("tracked.txt"),
		]
	);
	assert!(!sandbox.handle().base_dir.exists(), "sandbox cleaned");
}

// ─── 3. committed + uncommitted ─────────────────────────────────────────────

/// When a task commits part of its work and leaves the rest uncommitted, the
/// merge must preserve both: the commit history and the trailing edits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_and_uncommitted_work_both_survive_merge() {
	let temp = TempDir::new("mixed");
	let parent = temp.join("repo");
	init_repo(&parent);
	let base_head = git(&parent, &["rev-parse", "HEAD"]);

	let policy = policy(IsolationMergeMode::Branch);
	let (baseline, sandbox) =
		start_sandbox(&parent, &temp.join("wt"), policy, "task-mixed").await;
	fs::write(sandbox.merged_dir().join("tracked.txt"), "base\ncommitted\n").expect("commit half");
	git(sandbox.merged_dir(), &["add", "-A"]);
	git(sandbox.merged_dir(), &["commit", "-qm", "task committed half"]);
	fs::write(
		sandbox.merged_dir().join("tracked.txt"),
		"base\ncommitted\nleftover\n",
	)
	.expect("leftover");
	fs::write(sandbox.merged_dir().join("leftover.txt"), "leftover file\n").expect("leftover file");

	let out = finish(
		&sandbox,
		&baseline,
		policy,
		"task-mixed",
		&temp.join("artifacts"),
		&CancellationToken::new(),
	)
	.await;
	assert!(out.cleanup_error.is_none(), "{out:?}");
	let report = out.report.clone().expect("report");
	assert_eq!(report.changes_applied, Some(true), "{}", out.summary);

	assert_ne!(git(&parent, &["rev-parse", "HEAD"]), base_head, "history moved");
	assert_eq!(read(&parent.join("tracked.txt")), "base\ncommitted\nleftover\n");
	assert_eq!(read(&parent.join("leftover.txt")), "leftover file\n");
	// The intermediate committed state is preserved as its own commit, and the
	// leftover edit as the commit on top: neither half was folded away or lost.
	assert_eq!(git(&parent, &["show", "HEAD~1:tracked.txt"]), "base\ncommitted");
	assert_eq!(
		git(&parent, &["show", "HEAD:tracked.txt"]),
		"base\ncommitted\nleftover"
	);
	assert_eq!(git(&parent, &["show", "HEAD:leftover.txt"]), "leftover file");
	assert!(
		git(&parent, &["status", "--porcelain"]).is_empty(),
		"nothing may be left unmerged or stashed behind"
	);
	assert!(
		!git_ok(
			&parent,
			&["rev-parse", "--verify", "--quiet", "omp/task/task-mixed"]
		),
		"task branch removed after a successful merge"
	);
	assert!(!sandbox.handle().base_dir.exists(), "sandbox cleaned");
}

// ─── 4. conflicts retain recoverable output ─────────────────────────────────

/// When the parent drifts after the task's baseline was taken, the merge must
/// refuse to apply, keep the task's patch recoverable, and leave the parent
/// worktree exactly as the user left it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_retains_recoverable_patch_and_leaves_parent_intact() {
	let temp = TempDir::new("conflict");
	let parent = temp.join("repo");
	init_repo(&parent);

	let policy = policy(IsolationMergeMode::Patch);
	// Baseline is snapshotted here, at spawn time, exactly like production.
	let (baseline, sandbox) =
		start_sandbox(&parent, &temp.join("wt"), policy, "task-conflict").await;
	fs::write(sandbox.merged_dir().join("tracked.txt"), "base\nagent\n").expect("child edit");

	// The user keeps editing the parent while the task runs: the captured delta
	// no longer applies.
	fs::write(parent.join("tracked.txt"), "parent conflict\n").expect("parent drift");

	let out = finish(
		&sandbox,
		&baseline,
		policy,
		"task-conflict",
		&temp.join("artifacts"),
		&CancellationToken::new(),
	)
	.await;
	assert!(out.cleanup_error.is_none(), "{out:?}");
	let report = out.report.clone().expect("report");
	assert_eq!(
		report.changes_applied,
		Some(false),
		"conflicting merge must report not applied: {}",
		out.summary
	);

	// The user's drifting edit is intact; no conflict markers were written.
	assert_eq!(read(&parent.join("tracked.txt")), "parent conflict\n");
	assert!(!read(&parent.join("tracked.txt")).contains("<<<<<<<"));

	// The task's output is retained and is actually replayable: apply the
	// artifact to a fresh copy of the pre-conflict state and recover it.
	let patch_path = report.patch_path.clone().expect("patch artifact path");
	assert!(
		Path::new(&patch_path).exists(),
		"patch artifact retained on conflict"
	);
	assert!(
		out.summary.contains(patch_path.as_str()),
		"the retained patch must be surfaced to the caller: {}",
		out.summary
	);
	let patch_arg = patch_path.clone();
	assert!(
		fs::read(&patch_path).expect("patch").windows(6).any(|w| w == b"+agent"),
		"retained patch must carry the task's change"
	);
	let recover = temp.join("recover");
	init_repo(&recover);
	git(&recover, &["apply", "--binary", patch_arg.as_str()]);
	assert_eq!(
		read(&recover.join("tracked.txt")),
		"base\nagent\n",
		"retained patch must reproduce the task output"
	);

	assert!(!sandbox.handle().base_dir.exists(), "sandbox cleaned");
}

// ─── 5. cancellation before merge ───────────────────────────────────────────

/// Cancelling a task before it merges must leave no partial merge, no sandbox,
/// and the parent's baseline exactly as it was.
///
/// ponytail: the production cancel is delivered through this token and
/// `finalize` short-circuits before capture/merge for a pre-cancelled token;
/// that is the deterministic shape of "cancel before merge". Upgrade path: add
/// a gated merge seam to `task_isolation` to cancel mid-`spawn_blocking`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_before_merge_leaves_no_partial_merge_or_sandbox() {
	let temp = TempDir::new("cancel");
	let parent = temp.join("repo");
	init_repo(&parent);
	let head_before = git(&parent, &["rev-parse", "HEAD"]);
	let artifacts = temp.join("artifacts");

	let policy = policy(IsolationMergeMode::Patch);
	let (baseline, sandbox) =
		start_sandbox(&parent, &temp.join("wt"), policy, "task-cancel").await;
	fs::write(sandbox.merged_dir().join("tracked.txt"), "base\nagent\n").expect("child edit");
	let base_dir = sandbox.handle().base_dir.clone();
	assert!(base_dir.is_dir());

	let cancel = CancellationToken::new();
	cancel.cancel();
	let out = finish(&sandbox, &baseline, policy, "task-cancel", &artifacts, &cancel).await;

	assert!(out.cancelled, "finalize must report the cancellation");
	assert!(out.report.is_none(), "no merge report for a cancelled task");
	assert!(out.cleanup_error.is_none(), "{out:?}");

	assert_eq!(read(&parent.join("tracked.txt")), "base\n");
	assert!(
		git(&parent, &["status", "--porcelain"]).is_empty(),
		"cancel must not leave a partial merge or dirt behind"
	);
	assert_eq!(git(&parent, &["rev-parse", "HEAD"]), head_before);
	assert!(!base_dir.exists(), "owned sandbox removed on cancel");
}

// ─── 6. owned sandbox cleanup ───────────────────────────────────────────────

/// The sandbox is owned by the task (owner marker names it) and cleanup removes
/// exactly that base dir — no orphan worktree, no collateral deletion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_sandbox_is_cleaned_up_without_orphans() {
	let temp = TempDir::new("owned");
	let parent = temp.join("repo");
	init_repo(&parent);
	let worktree_base = temp.join("wt");

	let sandbox = iso::ensure(
		parent.clone(),
		"task-owned".to_string(),
		policy(IsolationMergeMode::Patch),
		Some(worktree_base.clone()),
	)
	.await
	.expect("sandbox");
	let base_dir = sandbox.handle().base_dir.clone();
	assert!(base_dir.is_dir(), "sandbox base created");
	assert!(
		sandbox.merged_dir().starts_with(&base_dir),
		"mount lives inside the owned base dir"
	);

	// Ownership is recorded on disk so a later scan can attribute the sandbox.
	let marker: serde_json::Value =
		serde_json::from_slice(&fs::read(base_dir.join(ISOLATION_OWNER_FILE)).expect("owner marker"))
			.expect("owner marker json");
	assert_eq!(marker["id"], "task-owned", "owner marker names the task");

	// Unrelated state in the shared base dir must survive cleanup.
	let root = base_dir.parent().expect("base root").to_path_buf();
	let foreign = root.join("foreign-checkout");
	fs::create_dir_all(&foreign).expect("foreign");
	fs::write(foreign.join("keep.txt"), "keep\n").expect("foreign file");

	assert!(sandbox.cleanup().is_none(), "cleanup reports no error");
	assert!(!base_dir.exists(), "owned sandbox base dir removed");
	assert_eq!(read(&foreign.join("keep.txt")), "keep\n", "foreign dir untouched");
	assert!(sandbox.cleanup().is_none(), "cleanup is idempotent");
	assert!(!sandbox.handle().base_dir.exists());
	// Nothing orphaned: only the foreign dir remains in the shared base root.
	let leftover = fs::read_dir(&root)
		.expect("wt root")
		.map(|entry| entry.expect("entry").file_name())
		.collect::<Vec<_>>();
	assert_eq!(leftover, vec![std::ffi::OsString::from("foreign-checkout")]);
	assert_eq!(read(&parent.join("tracked.txt")), "base\n", "parent intact");
}
