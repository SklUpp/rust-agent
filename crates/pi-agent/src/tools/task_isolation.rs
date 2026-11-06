//! Task-isolation lifecycle for `task` subagents.
//!
//! Two backends share one contract:
//!
//! - **Git checkout.** Mirrors the TypeScript `task/worktree.ts` +
//!   `task/isolation-runner.ts` contract while reusing the existing [`pi_iso`]
//!   PAL for the writable view and the `git` CLI for everything the PAL does
//!   not model:
//!
//!   - [`prepare_context`] snapshots the parent's HEAD + staged + unstaged +
//!     untracked content (size-capped) and discovers nested repositories.
//!   - [`ensure`] resolves a backend, claims an owned base dir before the mount,
//!     materialises `m/`, then severs the sandbox's git metadata from the parent
//!     so a child's git operations can never mutate the parent checkout.
//!   - [`finalize`] captures the child delta against the *baseline snapshot*
//!     (never `HEAD`, which would include inherited WIP and hide agent commits),
//!     writes a binary-safe patch artifact, optionally merges it or fetches a
//!     preserved task branch, then tears the sandbox down.
//!
//! - **No Git checkout.** [`prepare_context`] copies the live filesystem
//!   (including uncommitted/untracked files) as the three-way base, `ensure`
//!   forces the `Rcopy` backend, and [`finalize`] integrates with a real
//!   base/current/result file merge. Conflicts are never written into the
//!   parent: the whole integration is refused and all three versions are
//!   preserved under the artifacts dir. Path confinement refuses symlinked
//!   parent directories and re-checks content before every publish, but it is
//!   best-effort (not an atomic compare-and-swap and not a sandbox).
//!
//! Every mutation that touches the parent repo runs under one process-wide
//! async mutex: pi-agent has no per-repo lock, and two concurrent tasks
//! applying nested patches must not fold a sibling delta into their commit or
//! pop a sibling's stash entry.

use std::{
	collections::{BTreeMap, BTreeSet, HashSet},
	fs,
	io::{Read as _, Write as _},
	path::{Path, PathBuf},
	process::{Command, Stdio},
	sync::{
		Arc, LazyLock, Mutex as StdMutex,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	time::{SystemTime, UNIX_EPOCH},
};

use pi_iso::{BackendKind, IsoError};
use serde::{Deserialize, Serialize};
use similar::TextMerge;
use tokio_util::sync::CancellationToken;

use crate::{
	provider::{ModelProvider, ToolChoice},
	state::Message,
	workspace_writer::{
		FileVersion, LockError, VersionError, WorkspaceWriter, replace_if_unchanged,
	},
};

/// Ownership marker written into a task-isolation base dir; byte-compatible
/// with `packages/coding-agent/src/task/isolation-ownership.ts` so
/// `omp worktree clear` sees live sandboxes from either runtime.
pub const ISOLATION_OWNER_FILE: &str = ".omp-isolation-owner.json";

/// Mount dir name under the owned base dir. `merged` is a legacy alias read by
/// the CLI scanner; new sandboxes always use `m`.
pub const ISOLATION_MOUNT_DIR: &str = "m";

/// Ceiling on buffered uncommitted content, matching the TS baseline guard.
pub const ISOLATION_BASELINE_MAX_CONTENT_BYTES: u64 = 1024 * 1024 * 1024;

const WORKTREE_DIR_PREFIX: &str = "t";

/// Largest regular file the plain (non-git) integration will merge as text.
/// Larger files are preserved as conflicts instead of buffered in memory.
const PLAIN_MERGE_MAX_TEXT_BYTES: u64 = 8 * 1024 * 1024;

/// Serializes every parent-repo mutation: root patch apply, cherry-pick, and
/// nested stash/apply/commit/pop sequences.
static MERGE_LOCK: StdMutex<()> = StdMutex::new(());

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Per-process component of the sandbox directory digest. Including it means a
/// restart never reuses a crashed run's sandbox path, so `ensure` can refuse an
/// existing directory instead of recursively deleting state it does not own.
static PROCESS_NONCE: LazyLock<String> = LazyLock::new(|| {
	let token = process_start_token(std::process::id())
		.unwrap_or_else(|| format!("{}", now_nanos()));
	format!("{}-{token}", std::process::id())
});

/// `task.isolation.mode` values accepted by the settings file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IsolationMode {
	#[default]
	None,
	Auto,
	Apfs,
	Btrfs,
	Zfs,
	Reflink,
	Overlayfs,
	Projfs,
	BlockClone,
	Rcopy,
}

impl IsolationMode {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::None => "none",
			Self::Auto => "auto",
			Self::Apfs => "apfs",
			Self::Btrfs => "btrfs",
			Self::Zfs => "zfs",
			Self::Reflink => "reflink",
			Self::Overlayfs => "overlayfs",
			Self::Projfs => "projfs",
			Self::BlockClone => "block-clone",
			Self::Rcopy => "rcopy",
		}
	}

	/// Parse a settings value, including the legacy aliases the TS runtime
	/// still accepts (`worktree`, `fuse-overlay`, `fuse-projfs`). Unknown text
	/// is an error: silently ignoring it would run a different isolation mode
	/// than the user configured.
	pub fn parse(value: &str) -> Result<Self, String> {
		match value.trim() {
			"none" => Ok(Self::None),
			"auto" => Ok(Self::Auto),
			"apfs" => Ok(Self::Apfs),
			"btrfs" => Ok(Self::Btrfs),
			"zfs" => Ok(Self::Zfs),
			"reflink" => Ok(Self::Reflink),
			"overlayfs" | "fuse-overlay" => Ok(Self::Overlayfs),
			"projfs" | "fuse-projfs" => Ok(Self::Projfs),
			"block-clone" => Ok(Self::BlockClone),
			"rcopy" | "worktree" => Ok(Self::Rcopy),
			other => Err(format!(
				"invalid task.isolation.mode '{other}': expected none|auto|apfs|btrfs|zfs|reflink|overlayfs|projfs|block-clone|rcopy"
			)),
		}
	}

	const fn backend(self) -> Option<BackendKind> {
		match self {
			Self::None | Self::Auto => None,
			Self::Apfs => Some(BackendKind::Apfs),
			Self::Btrfs => Some(BackendKind::Btrfs),
			Self::Zfs => Some(BackendKind::Zfs),
			Self::Reflink => Some(BackendKind::LinuxReflink),
			Self::Overlayfs => Some(BackendKind::Overlayfs),
			Self::Projfs => Some(BackendKind::Projfs),
			Self::BlockClone => Some(BackendKind::WindowsBlockClone),
			Self::Rcopy => Some(BackendKind::Rcopy),
		}
	}
}

/// `task.isolation.merge`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IsolationMergeMode {
	#[default]
	Patch,
	Branch,
}

impl IsolationMergeMode {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Patch => "patch",
			Self::Branch => "branch",
		}
	}

	pub fn parse(value: &str) -> Result<Self, String> {
		match value.trim() {
			"patch" => Ok(Self::Patch),
			"branch" => Ok(Self::Branch),
			other => Err(format!(
				"invalid task.isolation.merge '{other}': expected patch|branch"
			)),
		}
	}
}

/// `task.isolation.commits`. `Ai` generates a validated subject through the
/// resolved child model (`ModelProvider::complete`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IsolationCommitStyle {
	#[default]
	Generic,
	Ai,
}

impl IsolationCommitStyle {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Generic => "generic",
			Self::Ai => "ai",
		}
	}

	pub fn parse(value: &str) -> Result<Self, String> {
		match value.trim() {
			"generic" => Ok(Self::Generic),
			"ai" => Ok(Self::Ai),
			other => Err(format!(
				"invalid task.isolation.commits '{other}': expected generic|ai"
			)),
		}
	}
}

/// Session-scoped isolation policy threaded from the CLI settings into every
/// `TaskTool` through [`crate::supervisor::Supervisor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskIsolationSettings {
	pub mode: IsolationMode,
	pub apply: bool,
	pub merge: IsolationMergeMode,
	pub commits: IsolationCommitStyle,
	/// Absolute base directory resolved by the CLI (`OMP_WORKTREE_DIR` →
	/// `worktree.base` → data root). `None` is only reachable from embedders
	/// and tests; the task tool then refuses isolation instead of guessing a
	/// base the CLI `worktree clear` command cannot scan.
	pub worktree_base: Option<PathBuf>,
}

impl Default for TaskIsolationSettings {
	fn default() -> Self {
		Self {
			mode: IsolationMode::None,
			apply: true,
			merge: IsolationMergeMode::Patch,
			commits: IsolationCommitStyle::Generic,
			worktree_base: None,
		}
	}
}

impl TaskIsolationSettings {
	fn preferred_backend(&self) -> Result<Option<BackendKind>, String> {
		match self.mode {
			IsolationMode::None => Err(
				"Subagent isolated execution requires task.isolation.mode to be set; current mode is \"none\"."
					.to_string(),
			),
			_ => Ok(self.mode.backend()),
		}
	}
}

/// One spawn's per-call isolation request. `isolated` is `None` when the model
/// omitted the key; `apply`/`merge` are only reachable through the eval
/// `agent()` bridge, matching the TS request shape.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IsolationRequest {
	pub isolated: Option<bool>,
	pub apply: Option<bool>,
	pub merge: Option<IsolationMergeMode>,
}

impl IsolationRequest {
	pub const fn requested(self) -> bool {
		matches!(self.isolated, Some(true))
	}
}

/// Effective policy for one spawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsolationPolicy {
	pub merge: IsolationMergeMode,
	pub apply: bool,
	pub commits: IsolationCommitStyle,
	pub preferred: Option<BackendKind>,
}

/// Resolve settings + request. Returns `Ok(None)` for the unchanged,
/// non-isolated path.
pub fn resolve_policy(
	settings: &TaskIsolationSettings,
	request: &IsolationRequest,
	invocation_task: bool,
) -> Result<Option<IsolationPolicy>, String> {
	if !request.requested() {
		return Ok(None);
	}
	let preferred = settings.preferred_backend()?;
	Ok(Some(IsolationPolicy {
		merge: request.merge.unwrap_or(settings.merge),
		apply: request.apply.unwrap_or(if invocation_task {
			settings.apply
		} else {
			true
		}),
		commits: settings.commits,
		preferred,
	}))
}

/// Baseline of one repository: everything `git status` would report, buffered
/// as byte-exact patches so the build is independent of the live parent worktree.
#[derive(Debug, Clone)]
pub struct RepoBaseline {
	pub repo_root: PathBuf,
	pub head_commit: String,
	pub staged: Vec<u8>,
	pub unstaged: Vec<u8>,
	pub untracked: Vec<String>,
	pub untracked_patch: Vec<u8>,
	/// Gitlink paths from the index; submodule checkouts can share metadata
	/// with the parent and must be rejected before a child runs.
	pub submodules: Vec<String>,
}

impl RepoBaseline {
	fn is_clean(&self) -> bool {
		!self.head_commit.is_empty()
			&& self.staged.iter().all(u8::is_ascii_whitespace)
			&& self.unstaged.iter().all(u8::is_ascii_whitespace)
			&& self.untracked.is_empty()
	}
}

/// Parent repo plus every nested repo discovered at spawn time.
#[derive(Debug, Clone)]
pub struct WorktreeBaseline {
	pub root: RepoBaseline,
	pub nested: Vec<NestedBaseline>,
	/// Set when the workspace is not a Git checkout. Isolation then snapshots
	/// the live filesystem (including uncommitted and untracked files) and
	/// integrates with a three-way file merge instead of Git.
	pub plain: Option<PlainBaseline>,
}

#[derive(Debug, Clone)]
pub struct NestedBaseline {
	pub relative_path: String,
	pub baseline: RepoBaseline,
}

/// One path observed in a plain (non-git) workspace snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlainEntry {
	pub kind: PlainKind,
	pub mode: u32,
	pub len: u64,
	/// `xxh64` of the file bytes, or of the link target for symlinks.
	pub hash: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlainKind {
	File,
	Symlink,
}

impl PlainEntry {
	fn same(self, other: Self) -> bool {
		self.kind == other.kind
			&& self.hash == other.hash
			&& self.len == other.len
			&& (self.kind != PlainKind::File || self.mode == other.mode)
	}
}

/// Byte-exact copy of a non-Git workspace taken before the child starts.
///
/// The copy is the merge base. It is intentionally a real copy (not a hardlink
/// farm): a hardlinked base would silently change when an external editor
/// writes the parent file in place, which would turn a conflicting edit into a
/// silent overwrite.
#[derive(Debug, Clone)]
pub struct PlainBaseline {
	pub root: PathBuf,
	entries: Arc<BTreeMap<String, PlainEntry>>,
	snapshot: Arc<PlainSnapshotDir>,
}

impl PlainBaseline {
	/// Directory holding the frozen baseline tree.
	pub fn snapshot_dir(&self) -> &Path {
		&self.snapshot.dir
	}
}

#[derive(Debug)]
struct PlainSnapshotDir {
	dir: PathBuf,
}

impl Drop for PlainSnapshotDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.dir);
	}
}

/// Isolated child output for a plain workspace, independent of the sandbox
/// lifetime so integration can run from the artifact copy alone.
#[derive(Debug, Clone)]
pub struct PlainCapture {
	pub base_dir: PathBuf,
	pub result_dir: PathBuf,
	pub artifacts_dir: PathBuf,
	pub base: BTreeMap<String, PlainEntry>,
	pub result: BTreeMap<String, PlainEntry>,
	/// Number of paths added, modified, or removed by the child.
	pub changed: usize,
}

/// Materialised sandbox. Cleanup is owned by [`IsolationSandbox`], never by the
/// caller's future.
#[derive(Debug, Clone)]
pub struct IsolationHandle {
	pub base_dir: PathBuf,
	pub merged_dir: PathBuf,
	pub backend: BackendKind,
	pub fell_back: bool,
	pub fallback_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NestedPatchArtifact {
	pub relative_path: String,
	pub patch: Vec<u8>,
	pub patch_path: Option<PathBuf>,
}

/// Result of capturing the child delta.
#[derive(Debug, Clone, Default)]
pub struct CapturedChanges {
	pub root_patch: Vec<u8>,
	pub patch_path: Option<PathBuf>,
	pub branch: Option<String>,
	pub base_sha: Option<String>,
	pub nested: Vec<NestedPatchArtifact>,
	/// Set for a non-Git workspace: the frozen base tree plus the child
	/// output copied into the artifacts dir.
	pub plain: Option<PlainCapture>,
	/// Why branch mode fell back to the patch artifact (dirty parent baseline).
	pub branch_fallback_reason: Option<String>,
	/// Capture failure that still left a recoverable patch artifact behind.
	pub error: Option<String>,
}

impl CapturedChanges {
	pub fn is_empty(&self) -> bool {
		if let Some(plain) = &self.plain {
			return plain.changed == 0;
		}
		self.root_patch.iter().all(u8::is_ascii_whitespace)
			&& self.nested.iter().all(|nested| nested.patch.iter().all(u8::is_ascii_whitespace))
	}
}

#[derive(Debug, Clone, Default)]
pub struct CommitMessages {
	pub root: Option<String>,
	pub nested: Vec<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct MergeOutcome {
	pub applied: bool,
	pub had_changes: bool,
	pub summary: String,
	pub merged_branch_for_nested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NestedRepoPatch {
	pub relative_path: String,
	pub patch: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IsolationArtifacts {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub patch_path: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub branch_name: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub branch_base_sha: Option<String>,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub nested_patches: Vec<NestedRepoPatch>,
}

/// Model/UI-facing isolation report carried on [`crate::tools::task::SingleResult`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IsolationReport {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub patch_path: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub branch_name: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub base_sha: Option<String>,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub nested_patches: Vec<NestedRepoPatch>,
	/// `None` means the caller skipped merging (`apply=false`).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub changes_applied: Option<bool>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub backend: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub error: Option<String>,
	/// Human-facing merge/capture summary (including notifications).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub summary: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct IsolationFinalize {
	pub report: Option<IsolationReport>,
	pub summary: String,
	pub cleanup_error: Option<String>,
	/// Cancellation arrived before/during finalize; callers must not mark the
	/// child as completed.
	pub cancelled: bool,
}

/// Owned sandbox whose `Drop` tears the mount down. `ops` is held by every
/// blocking capture/cleanup pair so a force-aborted future cannot delete the
/// merged view while a `spawn_blocking` capture is still reading it.
pub struct IsolationSandbox {
	inner: Arc<SandboxInner>,
}

struct SandboxInner {
	handle: IsolationHandle,
	ops: StdMutex<()>,
	cleaned: AtomicBool,
}

impl std::fmt::Debug for IsolationSandbox {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("IsolationSandbox")
			.field("merged_dir", &self.inner.handle.merged_dir)
			.field("backend", &self.inner.handle.backend)
			.finish()
	}
}

impl IsolationSandbox {
	fn new(handle: IsolationHandle) -> Self {
		Self {
			inner: Arc::new(SandboxInner {
				handle,
				ops: StdMutex::new(()),
				cleaned: AtomicBool::new(false),
			}),
		}
	}

	pub fn merged_dir(&self) -> &Path {
		&self.inner.handle.merged_dir
	}

	pub fn handle(&self) -> &IsolationHandle {
		&self.inner.handle
	}

	/// Explicit teardown. Safe to call more than once.
	pub fn cleanup(&self) -> Option<String> {
		let inner = Arc::clone(&self.inner);
		cleanup_inner(&inner)
	}

	/// Keep the sandbox on disk (uncaptured child work). `Drop` becomes a no-op.
	/// Callers must surface the retained path to the user.
	pub fn retain(&self) {
		self.inner.cleaned.store(true, Ordering::SeqCst);
	}
}

impl Drop for IsolationSandbox {
	fn drop(&mut self) {
		let _ = cleanup_inner(&self.inner);
	}
}

fn cleanup_inner(inner: &SandboxInner) -> Option<String> {
	if inner.cleaned.swap(true, Ordering::SeqCst) {
		return None;
	}
	// Wait for any in-flight capture that already cloned this sandbox.
	let _ops = inner
		.ops
		.lock()
		.unwrap_or_else(|poison| poison.into_inner());
	match cleanup_blocking(&inner.handle) {
		Ok(()) => None,
		Err(error) => {
			eprintln!("warning: {error}");
			Some(error)
		},
	}
}

// ─── git plumbing ───────────────────────────────────────────────────────────

struct GitOutput {
	stdout: Vec<u8>,
	#[allow(dead_code, reason = "kept for diagnostics in error paths")]
	stderr: Vec<u8>,
	code: i32,
}

fn run_git(
	cwd: &Path,
	args: &[&str],
	envs: &[(&str, std::ffi::OsString)],
	allow_exit1: bool,
) -> Result<GitOutput, String> {
	let mut command = Command::new("git");
	command
		.arg("-C")
		.arg(cwd)
		.args(args)
		.env("GIT_TERMINAL_PROMPT", "0")
		.stdin(Stdio::null())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped());
	for (key, value) in envs {
		command.env(key, value);
	}
	let output = command.output().map_err(|error| {
		format!(
			"cannot run git {} in {}: {error}",
			args.join(" "),
			cwd.display()
		)
	})?;
	let code = output.status.code().unwrap_or(-1);
	if !output.status.success() && !(allow_exit1 && code == 1) {
		return Err(format!(
			"git {} failed in {} (exit {code}): {}",
			args.join(" "),
			cwd.display(),
			String::from_utf8_lossy(&output.stderr).trim()
		));
	}
	Ok(GitOutput {
		stdout: output.stdout,
		stderr: output.stderr,
		code,
	})
}

fn git_ok(cwd: &Path, args: &[&str]) -> Result<Vec<u8>, String> {
	Ok(run_git(cwd, args, &[], false)?.stdout)
}

fn git_text(cwd: &Path, args: &[&str]) -> Result<String, String> {
	let bytes = git_ok(cwd, args)?;
	String::from_utf8(bytes)
		.map(|text| text.trim().to_string())
		.map_err(|error| format!("git {} output is not UTF-8: {error}", args.join(" ")))
}

fn split_nul(bytes: &[u8]) -> Result<Vec<String>, String> {
	bytes
		.split(|byte| *byte == 0)
		.filter(|chunk| !chunk.is_empty())
		.map(|chunk| {
			String::from_utf8(chunk.to_vec())
				.map_err(|error| format!("git path is not UTF-8: {error}"))
		})
		.collect()
}

fn temp_path(label: &str) -> PathBuf {
	let seq = TEMP_SEQ.fetch_add(1, Ordering::SeqCst);
	std::env::temp_dir().join(format!(
		"omp-iso-{label}-{}-{seq}-{}",
		std::process::id(),
		now_nanos()
	))
}

fn write_temp(label: &str, bytes: &[u8]) -> Result<PathBuf, String> {
	let path = temp_path(label);
	fs::write(&path, bytes).map_err(|error| format!("write {}: {error}", path.display()))?;
	Ok(path)
}

fn apply_patch(repo: &Path, patch: &[u8], extra: &[&str]) -> Result<(), String> {
	if patch.iter().all(u8::is_ascii_whitespace) {
		return Ok(());
	}
	let path = write_temp("patch", patch)?;
	let path_text = path.to_string_lossy().into_owned();
	let mut args = vec!["apply", "--binary", "--whitespace=nowarn"];
	args.extend_from_slice(extra);
	args.push(path_text.as_str());
	let result = git_ok(repo, &args).map(|_| ());
	let _ = fs::remove_file(&path);
	result
}

fn patch_applies(repo: &Path, patch: &[u8], reverse: bool) -> bool {
	if patch.iter().all(u8::is_ascii_whitespace) {
		return false;
	}
	let Ok(path) = write_temp("check", patch) else {
		return false;
	};
	let path_text = path.to_string_lossy().into_owned();
	let mut args = vec!["apply", "--check", "--binary"];
	if reverse {
		args.push("--reverse");
	}
	args.push(path_text.as_str());
	let applies = git_ok(repo, &args).is_ok();
	let _ = fs::remove_file(&path);
	applies
}

fn resolve_relative(base: &Path, value: &str) -> PathBuf {
	let path = Path::new(value);
	if path.is_absolute() {
		path.to_path_buf()
	} else {
		base.join(path)
	}
}

fn canonical_dir(path: &Path) -> Result<PathBuf, String> {
	fs::canonicalize(path).map_err(|error| format!("canonicalize {}: {error}", path.display()))
}

fn ensure_absolute_dir(path: &Path) -> Result<PathBuf, String> {
	if path.exists() {
		let meta = fs::symlink_metadata(path)
			.map_err(|error| format!("stat {}: {error}", path.display()))?;
		if !meta.is_dir() {
			return Err(format!("{} is not a directory", path.display()));
		}
		return canonical_dir(path);
	}
	fs::create_dir_all(path).map_err(|error| format!("create {}: {error}", path.display()))?;
	canonical_dir(path)
}

fn now_nanos() -> u128 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |elapsed| elapsed.as_nanos())
}

/// Boot-stable start-time token, byte-compatible with the TS marker reader.
fn process_start_token(pid: u32) -> Option<String> {
	if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
		let rest = stat.rsplit_once(')')?.1;
		let token = rest.split_whitespace().nth(19)?;
		return (!token.is_empty()).then(|| token.to_string());
	}
	let pid_text = pid.to_string();
	let output = Command::new("ps")
		.args(["-o", "lstart=", "-p", pid_text.as_str()])
		.output()
		.ok()?;
	if !output.status.success() {
		return None;
	}
	let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
	(!token.is_empty()).then_some(token)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OwnerMarker<'a> {
	pid: u32,
	id: &'a str,
	#[serde(skip_serializing_if = "Option::is_none")]
	start_token: Option<String>,
}

/// Publish the owner marker atomically before the mount is materialised.
fn write_owner(base_dir: &Path, id: &str) -> Result<(), String> {
	let marker = OwnerMarker {
		pid: std::process::id(),
		id,
		start_token: process_start_token(std::process::id()),
	};
	let bytes = serde_json::to_vec(&marker)
		.map_err(|error| format!("serialize isolation owner marker: {error}"))?;
	let tmp = base_dir.join(format!(
		"{ISOLATION_OWNER_FILE}.tmp-{}",
		std::process::id()
	));
	fs::write(&tmp, bytes).map_err(|error| format!("write {}: {error}", tmp.display()))?;
	let final_path = base_dir.join(ISOLATION_OWNER_FILE);
	fs::rename(&tmp, &final_path).map_err(|error| {
		let _ = fs::remove_file(&tmp);
		format!("publish {}: {error}", final_path.display())
	})
}

fn base_segment(repo_root: &Path, id: &str) -> String {
	let key = format!("{}\0{id}\0{}", repo_root.display(), *PROCESS_NONCE);
	let digest = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0) & 0xF_FFFF_FFFF;
	format!("{}{digest:09x}", WORKTREE_DIR_PREFIX)
}

fn check_disjoint(
	base: &Path,
	repo_root: &Path,
	common_dir: Option<&Path>,
) -> Result<(), String> {
	let overlaps = |a: &Path, b: &Path| a.starts_with(b) || b.starts_with(a);
	if overlaps(base, repo_root) {
		return Err(format!(
			"refusing isolation sandbox {}: it overlaps the source checkout {}",
			base.display(),
			repo_root.display()
		));
	}
	if let Some(common_dir) = common_dir
		&& overlaps(base, common_dir)
	{
		return Err(format!(
			"refusing isolation sandbox {}: it overlaps the Git dir {}",
			base.display(),
			common_dir.display()
		));
	}
	Ok(())
}

// ─── baseline ───────────────────────────────────────────────────────────────

/// Snapshot the parent working tree before any sandbox exists.
pub async fn prepare_context(cwd: PathBuf) -> Result<WorktreeBaseline, String> {
	tokio::task::spawn_blocking(move || prepare_context_blocking(&cwd))
		.await
		.map_err(|error| format!("baseline task failed: {error}"))?
}

fn prepare_context_blocking(cwd: &Path) -> Result<WorktreeBaseline, String> {
	let cwd = canonical_dir(cwd)?;
	let root = match git_text(&cwd, &["rev-parse", "--show-toplevel"]) {
		Ok(root) => canonical_dir(Path::new(&root))?,
		Err(error) => {
			if has_jj_marker(&cwd) {
				return Err(
					"Isolated task execution requires a Git checkout, but this workspace is pure Jujutsu (`.jj/` without a colocated `.git/`). Run `jj git init --colocate` to add a Git checkout, or set `task.isolation.mode: none` to disable task isolation."
						.to_string(),
				);
			}
			if has_git_marker(&cwd) {
				// Git metadata exists, so the probe failed for a real reason
				// (corrupt repo, dubious ownership, missing `git`). Never
				// downgrade a real checkout to the filesystem merge.
				if error.starts_with("cannot run git") {
					return Err(format!(
						"Isolated task execution found Git metadata at {} but cannot run the `git` binary: {error}. Install Git, or set `task.isolation.mode: none` to run tasks without isolation.",
						cwd.display()
					));
				}
				return Err(format!(
					"Isolated task execution could not read the Git checkout at {}: {error}. Fix the repository, or set `task.isolation.mode: none` to run tasks without isolation; the workspace is not downgraded to the non-Git merge automatically.",
					cwd.display()
				));
			}
			// No `.git` marker anywhere up the tree: a filesystem workspace.
			// The snapshot is the three-way merge base and includes every
			// uncommitted/untracked file as it exists right now.
			let plain = capture_plain_baseline(&cwd)?;
			return Ok(WorktreeBaseline {
				root: RepoBaseline {
					repo_root: cwd.clone(),
					head_commit: String::new(),
					staged: Vec::new(),
					unstaged: Vec::new(),
					untracked: Vec::new(),
					untracked_patch: Vec::new(),
					submodules: Vec::new(),
				},
				nested: Vec::new(),
				plain: Some(plain),
			});
		},
	};
	let submodules = submodule_paths(&root)?;
	let mut root_baseline = capture_repo_baseline(&root)?;
	root_baseline.submodules = submodules.iter().cloned().collect();
	root_baseline.submodules.sort();
	let nested = discover_nested_repos(&root, &submodules)
		.into_iter()
		.map(|relative_path| {
			let baseline = capture_repo_baseline(&root.join(&relative_path))?;
			Ok(NestedBaseline {
				relative_path,
				baseline,
			})
		})
		.collect::<Result<Vec<_>, String>>()?;
	Ok(WorktreeBaseline {
		root: root_baseline,
		nested,
		plain: None,
	})
}

fn has_jj_marker(start: &Path) -> bool {
	let mut current = Some(start);
	while let Some(dir) = current {
		if dir.join(".jj").is_dir() {
			return true;
		}
		current = dir.parent();
	}
	false
}

/// `.git` (directory or linked-worktree pointer) at `start` or any ancestor.
/// Used to distinguish "not a repository" from a broken repository when the
/// `git` probe fails.
fn has_git_marker(start: &Path) -> bool {
	let mut current = Some(start);
	while let Some(dir) = current {
		if fs::symlink_metadata(dir.join(".git")).is_ok() {
			return true;
		}
		current = dir.parent();
	}
	false
}

fn submodule_paths(repo: &Path) -> Result<HashSet<String>, String> {
	let bytes = git_ok(repo, &["ls-files", "--stage", "-z"])?;
	let mut paths = HashSet::new();
	for entry in bytes.split(|byte| *byte == 0) {
		let Some(tab) = entry.iter().position(|byte| *byte == b'\t') else {
			continue;
		};
		let meta = String::from_utf8_lossy(&entry[..tab]);
		if meta.starts_with("160000 ") {
			let path = String::from_utf8_lossy(&entry[tab + 1..]).into_owned();
			paths.insert(path);
		}
	}
	Ok(paths)
}

fn capture_repo_baseline(repo_root: &Path) -> Result<RepoBaseline, String> {
	let repo_root = canonical_dir(repo_root)?;
	let head_commit = git_text(&repo_root, &["rev-parse", "--verify", "HEAD"]).unwrap_or_default();
	let staged = git_ok(
		&repo_root,
		&["diff", "--binary", "--no-color", "--cached"],
	)?;
	let unstaged = git_ok(&repo_root, &["diff", "--binary", "--no-color"])?;
	let untracked = split_nul(&git_ok(
		&repo_root,
		&["ls-files", "--others", "--exclude-standard", "-z"],
	)?)?;

	let mut untracked_bytes = 0_u64;
	for relative in &untracked {
		if let Ok(meta) = fs::symlink_metadata(repo_root.join(relative))
			&& meta.is_file()
		{
			untracked_bytes = untracked_bytes.saturating_add(meta.len());
		}
	}
	let content_bytes = untracked_bytes
		.saturating_add(staged.len() as u64)
		.saturating_add(unstaged.len() as u64);
	if content_bytes > ISOLATION_BASELINE_MAX_CONTENT_BYTES {
		return Err(format!(
			"Working tree at {} carries {} of uncommitted content, over the {} isolation-snapshot budget. Commit or gitignore the bulk (untracked files that are not ignored are the usual culprit), or set `task.isolation.mode: none` to run tasks without isolation.",
			repo_root.display(),
			format_bytes(content_bytes),
			format_bytes(ISOLATION_BASELINE_MAX_CONTENT_BYTES)
		));
	}

	let mut untracked_patch = Vec::new();
	for relative in &untracked {
		let patch = no_index_patch(&repo_root, relative)?;
		if !patch.is_empty() {
			if !untracked_patch.is_empty() && !untracked_patch.ends_with(b"\n") {
				untracked_patch.push(b'\n');
			}
			untracked_patch.extend_from_slice(&patch);
		}
	}

	Ok(RepoBaseline {
		repo_root,
		head_commit,
		staged,
		unstaged,
		untracked,
		untracked_patch,
		submodules: Vec::new(),
	})
}

fn format_bytes(bytes: u64) -> String {
	const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
	let mut value = bytes as f64;
	let mut unit = 0;
	while value >= 1024.0 && unit + 1 < UNITS.len() {
		value /= 1024.0;
		unit += 1;
	}
	if unit == 0 {
		format!("{bytes} B")
	} else {
		format!("{value:.1} {}", UNITS[unit])
	}
}

fn no_index_patch(repo: &Path, relative: &str) -> Result<Vec<u8>, String> {
	let null = if cfg!(windows) { "NUL" } else { "/dev/null" };
	let output = run_git(
		repo,
		&[
			"diff",
			"--no-index",
			"--binary",
			"--no-color",
			"--",
			null,
			relative,
		],
		&[],
		true,
	)?;
	Ok(if output.code == 1 { output.stdout } else { Vec::new() })
}

fn discover_nested_repos(root: &Path, submodules: &HashSet<String>) -> Vec<String> {
	fn walk(root: &Path, dir: &Path, submodules: &HashSet<String>, out: &mut Vec<String>) {
		let Ok(entries) = fs::read_dir(dir) else {
			return;
		};
		let mut entries = entries.filter_map(Result::ok).collect::<Vec<_>>();
		entries.sort_by_key(|entry| entry.file_name());
		for entry in entries {
			let name = entry.file_name();
			let name = name.to_string_lossy();
			if name == ".git" || name == "node_modules" {
				continue;
			}
			let Ok(file_type) = entry.file_type() else {
				continue;
			};
			if !file_type.is_dir() || file_type.is_symlink() {
				continue;
			}
			let full = entry.path();
			let Ok(relative) = full.strip_prefix(root) else {
				continue;
			};
			let relative = relative.to_string_lossy().replace('\\', "/");
			if full.join(".git").exists() {
				if !submodules.contains(&relative) {
					out.push(relative);
				}
				continue;
			}
			walk(root, &full, submodules, out);
		}
	}
	let mut out = Vec::new();
	walk(root, root, submodules, &mut out);
	out
}

fn write_synthetic_tree(repo: &Path, baseline: &RepoBaseline) -> Result<String, String> {
	let index = temp_path("index");
	if baseline.head_commit.is_empty() {
		run_git(
			repo,
			&["read-tree", "--empty"],
			&[("GIT_INDEX_FILE", index.clone().into_os_string())],
			false,
		)?;
	} else {
		run_git(
			repo,
			&["read-tree", baseline.head_commit.as_str()],
			&[("GIT_INDEX_FILE", index.clone().into_os_string())],
			false,
		)?;
	}
	let envs = [("GIT_INDEX_FILE", index.clone().into_os_string())];
	for patch in [&baseline.staged, &baseline.unstaged, &baseline.untracked_patch] {
		if patch.iter().all(u8::is_ascii_whitespace) {
			continue;
		}
		let path = write_temp("baseline", patch)?;
		let path_text = path.to_string_lossy().into_owned();
		let result = run_git(
			repo,
			&[
				"apply",
				"--cached",
				"--binary",
				"--whitespace=nowarn",
				path_text.as_str(),
			],
			&envs,
			false,
		);
		let _ = fs::remove_file(&path);
		if let Err(err) = result {
			let _ = fs::remove_file(&index);
			let _ = fs::remove_file(index.with_extension("lock"));
			return Err(err);
		}
	}
	let tree = run_git(repo, &["write-tree"], &envs, false).and_then(|output| {
		String::from_utf8(output.stdout)
			.map(|text| text.trim().to_string())
			.map_err(|error| format!("git write-tree output is not UTF-8: {error}"))
	});
	let _ = fs::remove_file(&index);
	let _ = fs::remove_file(index.with_extension("lock"));
	tree
}

fn synthesize_worktree(repo: &Path) -> Result<RepoBaseline, String> {
	let head_commit = git_text(repo, &["rev-parse", "--verify", "HEAD"]).unwrap_or_default();
	let staged = git_ok(repo, &["diff", "--binary", "--no-color", "--cached"])?;
	let unstaged = git_ok(repo, &["diff", "--binary", "--no-color"])?;
	let untracked = split_nul(&git_ok(
		repo,
		&["ls-files", "--others", "--exclude-standard", "-z"],
	)?)?;
	let mut untracked_patch = Vec::new();
	for relative in &untracked {
		let patch = no_index_patch(repo, relative)?;
		if patch.is_empty() {
			continue;
		}
		if !untracked_patch.is_empty() && !untracked_patch.ends_with(b"\n") {
			untracked_patch.push(b'\n');
		}
		untracked_patch.extend_from_slice(&patch);
	}
	Ok(RepoBaseline {
		repo_root: repo.to_path_buf(),
		head_commit,
		staged,
		unstaged,
		untracked,
		untracked_patch,
		submodules: Vec::new(),
	})
}

fn capture_repo_delta(worktree: &Path, baseline: &RepoBaseline) -> Result<Vec<u8>, String> {
	// Build BOTH synthetic trees in the child ODB. Copied backends (APFS,
	// btrfs, reflink, block-clone) have a private object DB with no alternates
	// back to the parent, so a baseline tree created in the parent ODB would be
	// invisible here. The baseline patches are frozen bytes and apply against
	// baseline HEAD, which the child copy/alternates always has.
	let baseline_tree = write_synthetic_tree(worktree, baseline)?;
	let current = synthesize_worktree(worktree)?;
	let current_tree = write_synthetic_tree(worktree, &current)?;
	git_ok(
		worktree,
		&[
			"diff",
			"--binary",
			"--no-color",
			baseline_tree.as_str(),
			current_tree.as_str(),
		],
	)
}

// ─── plain (non-git) workspace ─────────────────────────────────────────────

/// Snapshot a workspace that is not a Git checkout.
///
/// The baseline keeps a byte-exact copy of every regular file and symlink
/// (excluding `.git` metadata). Integration later compares three real trees —
/// frozen base, live parent, isolated result — so an external writer is
/// detected by content, never by mtime.
fn capture_plain_baseline(root: &Path) -> Result<PlainBaseline, String> {
	let entries = plain_manifest(root)?;
	let total = entries
		.values()
		.filter(|entry| entry.kind == PlainKind::File)
		.fold(0_u64, |sum, entry| sum.saturating_add(entry.len));
	if total > ISOLATION_BASELINE_MAX_CONTENT_BYTES {
		return Err(format!(
			"Workspace at {} carries {} of files, over the {} isolation-snapshot budget. Reduce the tree (generated output and caches are the usual culprit), or set `task.isolation.mode: none` to run tasks without isolation.",
			root.display(),
			format_bytes(total),
			format_bytes(ISOLATION_BASELINE_MAX_CONTENT_BYTES)
		));
	}
	let dir = temp_path("baseline");
	if let Err(error) = copy_plain_tree(root, &dir, &entries) {
		let _ = fs::remove_dir_all(&dir);
		return Err(error);
	}
	Ok(PlainBaseline {
		root: root.to_path_buf(),
		entries: Arc::new(entries),
		snapshot: Arc::new(PlainSnapshotDir { dir }),
	})
}

/// Content manifest of a plain tree. `.git` metadata is excluded from every
/// snapshot, capture, and merge: nested repositories keep their own metadata,
/// and merging its internals would corrupt them.
fn plain_manifest(root: &Path) -> Result<BTreeMap<String, PlainEntry>, String> {
	fn walk(
		root: &Path,
		dir: &Path,
		out: &mut BTreeMap<String, PlainEntry>,
	) -> Result<(), String> {
		let entries = fs::read_dir(dir)
			.map_err(|error| format!("read_dir {}: {error}", dir.display()))?;
		let mut entries = entries.filter_map(Result::ok).collect::<Vec<_>>();
		entries.sort_by_key(|entry| entry.file_name());
		for entry in entries {
			let name = entry.file_name();
			if name.to_string_lossy() == ".git" {
				let meta = fs::symlink_metadata(entry.path()).map_err(|error| {
					format!("stat {}: {error}", entry.path().display())
				})?;
				if !meta.is_dir() {
					return Err(format!(
						"Nested Git metadata at {} is a linked worktree/submodule pointer, so an isolated copy would still mutate the source repository. Replace it with a full checkout, or set `task.isolation.mode: none` to run tasks without isolation.",
						entry.path().display()
					));
				}
				continue;
			}
			let path = entry.path();
			let meta = fs::symlink_metadata(&path)
				.map_err(|error| format!("stat {}: {error}", path.display()))?;
			let relative = plain_relative(root, &path)?;
			if meta.file_type().is_symlink() {
				let target = fs::read_link(&path)
					.map_err(|error| format!("read_link {}: {error}", path.display()))?;
				let target = target.to_string_lossy();
				out.insert(
					relative,
					PlainEntry {
						kind:  PlainKind::Symlink,
						mode:  plain_mode(&meta),
						len:   target.len() as u64,
						hash:  xxhash_rust::xxh64::xxh64(target.as_bytes(), 0),
					},
				);
			} else if meta.is_dir() {
				walk(root, &path, out)?;
			} else if meta.is_file() {
				out.insert(
					relative,
					PlainEntry {
						kind: PlainKind::File,
						mode: plain_mode(&meta),
						len:  meta.len(),
						hash: hash_file(&path)?,
					},
				);
			}
			// FIFOs, sockets, and devices have no mergeable content.
		}
		Ok(())
	}
	let mut out = BTreeMap::new();
	if !root.exists() {
		return Ok(out);
	}
	walk(root, root, &mut out)?;
	Ok(out)
}

fn plain_relative(root: &Path, path: &Path) -> Result<String, String> {
	let relative = path
		.strip_prefix(root)
		.map_err(|_| format!("{} is outside {}", path.display(), root.display()))?;
	relative
		.to_str()
		.map(|text| text.replace('\\', "/"))
		.ok_or_else(|| format!("workspace path {} is not valid UTF-8", path.display()))
}

fn plain_mode(meta: &fs::Metadata) -> u32 {
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		meta.permissions().mode() & 0o7777
	}
	#[cfg(not(unix))]
	{
		let _ = meta;
		0
	}
}

fn hash_file(path: &Path) -> Result<u64, String> {
	let mut file =
		fs::File::open(path).map_err(|error| format!("open {}: {error}", path.display()))?;
	let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);
	let mut buffer = vec![0_u8; 64 * 1024];
	loop {
		let read = file
			.read(&mut buffer)
			.map_err(|error| format!("read {}: {error}", path.display()))?;
		if read == 0 {
			return Ok(hasher.digest());
		}
		hasher.update(&buffer[..read]);
	}
}

fn copy_plain_tree(
	root: &Path,
	dir: &Path,
	entries: &BTreeMap<String, PlainEntry>,
) -> Result<(), String> {
	fs::create_dir_all(dir).map_err(|error| format!("create {}: {error}", dir.display()))?;
	for (relative, entry) in entries {
		let source = root.join(relative);
		let destination = dir.join(relative);
		if let Some(parent) = destination.parent() {
			fs::create_dir_all(parent)
				.map_err(|error| format!("create {}: {error}", parent.display()))?;
		}
		copy_plain_node(&source, &destination, *entry)?;
	}
	Ok(())
}

fn copy_plain_node(source: &Path, destination: &Path, entry: PlainEntry) -> Result<(), String> {
	remove_plain_node(destination)?;
	match entry.kind {
		PlainKind::File => {
			fs::copy(source, destination).map_err(|error| {
				format!("copy {} -> {}: {error}", source.display(), destination.display())
			})?;
			copy_plain_mtime(source, destination);
			set_plain_mode(destination, entry.mode)?;
			Ok(())
		},
		PlainKind::Symlink => {
			let target = fs::read_link(source)
				.map_err(|error| format!("read_link {}: {error}", source.display()))?;
			create_plain_symlink(&target, destination).map_err(|error| {
				format!("symlink {} -> {}: {error}", destination.display(), target.display())
			})
		},
	}
}

/// Mirror the source mtime onto a snapshot copy. pi-iso's plain diff uses
/// `(size, mtime)` as its fast path, so keeping it makes the patch artifact
/// name only files the child actually touched. Best effort: mtime is a diff
/// hint, never a correctness input (the manifest hashes bytes).
fn copy_plain_mtime(source: &Path, destination: &Path) {
	let Ok(modified) = fs::metadata(source).and_then(|meta| meta.modified()) else {
		return;
	};
	if let Ok(file) = fs::OpenOptions::new().write(true).open(destination) {
		let _ = file.set_modified(modified);
	}
}

fn remove_plain_node(path: &Path) -> Result<(), String> {
	match fs::symlink_metadata(path) {
		Ok(meta) if meta.is_dir() => fs::remove_dir_all(path)
			.map_err(|error| format!("remove {}: {error}", path.display())),
		Ok(_) => fs::remove_file(path).map_err(|error| format!("remove {}: {error}", path.display())),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(error) => Err(format!("stat {}: {error}", path.display())),
	}
}

fn set_plain_mode(path: &Path, mode: u32) -> Result<(), String> {
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		fs::set_permissions(path, fs::Permissions::from_mode(mode & 0o7777))
			.map_err(|error| format!("chmod {}: {error}", path.display()))
	}
	#[cfg(not(unix))]
	{
		let _ = (path, mode);
		Ok(())
	}
}

fn create_plain_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
	#[cfg(unix)]
	{
		std::os::unix::fs::symlink(target, link)
	}
	#[cfg(windows)]
	{
		std::os::windows::fs::symlink_file(target, link)
	}
	#[cfg(not(any(unix, windows)))]
	{
		let _ = (target, link);
		Err(std::io::Error::new(
			std::io::ErrorKind::Unsupported,
			"symlink integration is unsupported on this platform",
		))
	}
}

fn plain_entry_at(path: &Path) -> Result<Option<PlainEntry>, String> {
	match fs::symlink_metadata(path) {
		Ok(meta) if meta.file_type().is_symlink() => {
			let target = fs::read_link(path)
				.map_err(|error| format!("read_link {}: {error}", path.display()))?;
			let target = target.to_string_lossy();
			Ok(Some(PlainEntry {
				kind:  PlainKind::Symlink,
				mode:  plain_mode(&meta),
				len:   target.len() as u64,
				hash:  xxhash_rust::xxh64::xxh64(target.as_bytes(), 0),
			}))
		},
		Ok(meta) if meta.is_file() => Ok(Some(PlainEntry {
			kind: PlainKind::File,
			mode: plain_mode(&meta),
			len:  meta.len(),
			hash: hash_file(path)?,
		})),
		Ok(_) => Err(format!(
			"{} is not a regular file or symlink; refusing to integrate over it",
			path.display()
		)),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
		Err(error) => Err(format!("stat {}: {error}", path.display())),
	}
}

fn capture_plain(
	merged: &Path,
	baseline: &PlainBaseline,
	id: &str,
	artifacts_dir: &Path,
) -> Result<CapturedChanges, String> {
	let result = plain_manifest(merged)?;
	fs::create_dir_all(artifacts_dir)
		.map_err(|error| format!("create artifacts dir {}: {error}", artifacts_dir.display()))?;
	let result_dir = artifacts_dir.join(format!("{id}.isolated"));
	fs::create_dir_all(&result_dir)
		.map_err(|error| format!("create {}: {error}", result_dir.display()))?;
	let mut changed = 0_usize;
	for (relative, entry) in &result {
		if baseline
			.entries
			.get(relative)
			.is_some_and(|base| base.same(*entry))
		{
			continue;
		}
		let destination = result_dir.join(relative);
		if let Some(parent) = destination.parent() {
			fs::create_dir_all(parent)
				.map_err(|error| format!("create {}: {error}", parent.display()))?;
		}
		copy_plain_node(&merged.join(relative), &destination, *entry)?;
		changed += 1;
	}
	for relative in baseline.entries.keys() {
		if !result.contains_key(relative) {
			changed += 1;
		}
	}

	let mut root_patch = Vec::new();
	if changed > 0 {
		let diff = pi_iso::plain_diff_blocking(baseline.snapshot_dir(), merged)
			.map_err(|error| format!("diff {}: {error}", baseline.root.display()))?;
		for change in diff.files {
			if change
				.path
				.components()
				.any(|component| component.as_os_str() == ".git")
			{
				continue;
			}
			match change.diff {
				Some(text) => root_patch.extend_from_slice(text.as_bytes()),
				None => root_patch.extend_from_slice(
					format!(
						"Binary files a/{0} and b/{0} differ\n",
						change.path.display()
					)
					.as_bytes(),
				),
			}
		}
	}
	let patch_path = artifacts_dir.join(format!("{id}.patch"));
	fs::write(&patch_path, &root_patch)
		.map_err(|error| format!("write patch {}: {error}", patch_path.display()))?;

	Ok(CapturedChanges {
		root_patch,
		patch_path: Some(patch_path),
		plain: Some(PlainCapture {
			base_dir: baseline.snapshot_dir().to_path_buf(),
			result_dir,
			artifacts_dir: artifacts_dir.to_path_buf(),
			base: (*baseline.entries).clone(),
			result,
			changed,
		}),
		..CapturedChanges::default()
	})
}

fn plain_same(a: Option<PlainEntry>, b: Option<PlainEntry>) -> bool {
	match (a, b) {
		(None, None) => true,
		(Some(a), Some(b)) => a.same(b),
		_ => false,
	}
}

/// Line-oriented three-way merge from the installed `similar` crate.
/// `None` means unresolved conflict; the caller then preserves all versions
/// instead of writing conflict markers into the user's workspace.
fn merge_plain_text(base: &[u8], current: &[u8], result: &[u8]) -> Option<Vec<u8>> {
	if base.contains(&0) || current.contains(&0) || result.contains(&0) {
		return None;
	}
	let base = std::str::from_utf8(base).ok()?;
	let current = std::str::from_utf8(current).ok()?;
	let result = std::str::from_utf8(result).ok()?;
	let merge = TextMerge::from_lines(base, current, result);
	(!merge.is_conflicted()).then(|| merge.to_string().into_bytes())
}

enum PlainAction {
	Write {
		relative: String,
		bytes:    Vec<u8>,
		mode:     u32,
		expected: Option<PlainEntry>,
	},
	Symlink {
		relative: String,
		target:   PathBuf,
		expected: Option<PlainEntry>,
	},
	Delete {
		relative: String,
		expected: Option<PlainEntry>,
	},
}

impl PlainAction {
	fn relative(&self) -> &str {
		match self {
			Self::Write { relative, .. }
			| Self::Symlink { relative, .. }
			| Self::Delete { relative, .. } => relative,
		}
	}
}

enum PlainPlan {
	Action(PlainAction),
	Skip,
	Conflict(String),
}

enum PlainApplyError {
	/// Another omp writer holds the parent lease, or the path appeared after
	/// preflight. Nothing was written.
	External(String),
	/// The target no longer matches the content preflight observed.
	Stale(String),
	Io(String),
}

impl PlainApplyError {
	fn message(&self) -> &str {
		match self {
			Self::External(message) | Self::Stale(message) | Self::Io(message) => message,
		}
	}
}

fn merge_plain(
	repo_root: &Path,
	id: &str,
	captured: &CapturedChanges,
	mode: IsolationMergeMode,
) -> MergeOutcome {
	let Some(plain) = &captured.plain else {
		return MergeOutcome {
			applied:                 false,
			had_changes:             false,
			summary:                 "\n\n<system-notification>Plain integration was requested without a plain capture.</system-notification>".to_string(),
			merged_branch_for_nested: false,
		};
	};
	let current = match plain_manifest(repo_root) {
		Ok(manifest) => manifest,
		Err(error) => {
			return MergeOutcome {
				applied:                 false,
				had_changes:             false,
				summary:                 format!(
					"\n\n<system-notification>Could not read the parent workspace for three-way merge: {error}\nThe isolated output was retained at {}.</system-notification>",
					plain.result_dir.display()
				),
				merged_branch_for_nested: false,
			};
		},
	};

	let mut actions = Vec::new();
	let mut conflicts = Vec::new();
	let mut paths = BTreeSet::new();
	paths.extend(plain.base.keys().cloned());
	paths.extend(plain.result.keys().cloned());
	paths.extend(current.keys().cloned());
	for relative in paths {
		let base = plain.base.get(&relative).copied();
		let result = plain.result.get(&relative).copied();
		let now = current.get(&relative).copied();
		if plain_same(base, result) {
			continue;
		}
		if plain_same(base, now) {
			match plain_result_action(plain, &relative, result, now) {
				Ok(action) => actions.push(action),
				Err(reason) => conflicts.push((relative, reason)),
			}
			continue;
		}
		if plain_same(now, result) {
			continue;
		}
		match plan_plain_merge(repo_root, plain, &relative, base, result, now) {
			Ok(PlainPlan::Action(action)) => actions.push(action),
			Ok(PlainPlan::Skip) => {},
			Ok(PlainPlan::Conflict(reason)) => conflicts.push((relative, reason)),
			Err(error) => conflicts.push((relative, error)),
		}
	}

	let writer = WorkspaceWriter::new();
	let mut warnings = Vec::new();
	// The root session owns the workspace-scope lease, so taking it here would
	// deadlock every merge. Integration takes the narrower per-file lease and
	// re-checks content before publishing.
	if let Err(error) = &writer {
		warnings.push(format!(
			"Parent workspace writer lease is unavailable ({error}); integration ran without the omp writer lease. External changes detected before each write are reported, but this is not an atomic compare-and-swap."
		));
	}

	if !conflicts.is_empty() {
		// Refuse the whole integration: a partial write would mix task output
		// into a workspace the user is actively editing.
		let mut summary = format!(
			"\n\n<system-notification>Three-way merge refused: {} path(s) conflict; the parent workspace was left unchanged.</system-notification>",
			conflicts.len()
		);
		for (relative, reason) in &conflicts {
			match preserve_plain_conflict(repo_root, plain, id, relative) {
				Ok(Some(dir)) => summary.push_str(&format!(
					"\n- {relative}: {reason}\n  base/current/result: {}",
					dir.display()
				)),
				Ok(None) => summary.push_str(&format!("\n- {relative}: {reason}")),
				Err(error) => summary.push_str(&format!(
					"\n- {relative}: {reason}\n  could not preserve the three versions: {error}"
				)),
			}
		}
		summary.push_str(&format!(
			"\nIsolated artifact: {}",
			plain.result_dir.display()
		));
		if let Some(path) = &captured.patch_path {
			summary.push_str(&format!("\nPatch artifact:\n- {}", path.display()));
		}
		for warning in warnings {
			summary.push_str(&format!("\n\n<system-notification>{warning}</system-notification>"));
		}
		return MergeOutcome {
			applied: false,
			had_changes: false,
			summary,
			merged_branch_for_nested: false,
		};
	}

	let writer = writer.as_ref().ok();
	let mut applied = 0_usize;
	let mut failures = Vec::new();
	for action in &actions {
		match apply_plain_action(writer, repo_root, action) {
			Ok(()) => applied += 1,
			Err(error) => failures.push(format!("{}: {}", action.relative(), error.message())),
		}
	}

	let mut summary = format!(
		"\n\nNon-Git workspace: filesystem three-way merge applied {applied} path(s)."
	);
	if mode == IsolationMergeMode::Branch {
		summary.push_str(" A non-Git workspace has no branch history to merge.");
	}
	if let Some(path) = &captured.patch_path {
		summary.push_str(&format!("\nPatch artifact:\n- {}", path.display()));
	}
	summary.push_str(&format!("\nIsolated artifact: {}", plain.result_dir.display()));
	if !failures.is_empty() {
		summary.push_str(&format!(
			"\n\n<system-notification>{} path(s) could not be integrated; the parent workspace was left unchanged for them.</system-notification>",
			failures.len()
		));
		for failure in &failures {
			summary.push_str(&format!("\n- {failure}"));
		}
	}
	for warning in warnings {
		summary.push_str(&format!("\n\n<system-notification>{warning}</system-notification>"));
	}
	MergeOutcome {
		applied: failures.is_empty(),
		had_changes: applied > 0,
		summary,
		merged_branch_for_nested: false,
	}
}

fn plain_result_action(
	plain: &PlainCapture,
	relative: &str,
	result: Option<PlainEntry>,
	expected: Option<PlainEntry>,
) -> Result<PlainAction, String> {
	let Some(entry) = result else {
		return Ok(PlainAction::Delete {
			relative: relative.to_string(),
			expected,
		});
	};
	match entry.kind {
		PlainKind::File => Ok(PlainAction::Write {
			relative: relative.to_string(),
			bytes: fs::read(plain.result_dir.join(relative))
				.map_err(|error| format!("read isolated artifact {relative}: {error}"))?,
			mode: entry.mode,
			expected,
		}),
		PlainKind::Symlink => Ok(PlainAction::Symlink {
			relative: relative.to_string(),
			target: fs::read_link(plain.result_dir.join(relative))
				.map_err(|error| format!("read isolated symlink {relative}: {error}"))?,
			expected,
		}),
	}
}

fn plan_plain_merge(
	repo_root: &Path,
	plain: &PlainCapture,
	relative: &str,
	base: Option<PlainEntry>,
	result: Option<PlainEntry>,
	now: Option<PlainEntry>,
) -> Result<PlainPlan, String> {
	let (Some(base), Some(result), Some(now)) = (base, result, now) else {
		return Ok(PlainPlan::Conflict(
			"the path was added or removed on both sides".to_string(),
		));
	};
	if base.kind != PlainKind::File || result.kind != PlainKind::File || now.kind != PlainKind::File {
		return Ok(PlainPlan::Conflict(
			"node kind changed on both sides; no text base to merge".to_string(),
		));
	}
	if base.len > PLAIN_MERGE_MAX_TEXT_BYTES
		|| result.len > PLAIN_MERGE_MAX_TEXT_BYTES
		|| now.len > PLAIN_MERGE_MAX_TEXT_BYTES
	{
		return Ok(PlainPlan::Conflict(format!(
			"file is larger than the {PLAIN_MERGE_MAX_TEXT_BYTES}-byte text-merge limit"
		)));
	}
	let base_bytes = fs::read(plain.base_dir.join(relative))
		.map_err(|error| format!("read baseline {relative}: {error}"))?;
	let current_bytes = fs::read(repo_root.join(relative))
		.map_err(|error| format!("read parent {relative}: {error}"))?;
	let result_bytes = fs::read(plain.result_dir.join(relative))
		.map_err(|error| format!("read isolated artifact {relative}: {error}"))?;
	let Some(merged) = merge_plain_text(&base_bytes, &current_bytes, &result_bytes) else {
		return Ok(PlainPlan::Conflict(
			"overlapping edits; preserved base/current/result".to_string(),
		));
	};
	let merged_hash = xxhash_rust::xxh64::xxh64(&merged, 0);
	let mode = if result.mode == base.mode {
		now.mode
	} else if now.mode == base.mode || now.mode == result.mode {
		result.mode
	} else {
		return Ok(PlainPlan::Conflict(
			"file mode changed on both sides".to_string(),
		));
	};
	if merged_hash == now.hash && merged.len() as u64 == now.len && mode == now.mode {
		return Ok(PlainPlan::Skip);
	}
	Ok(PlainPlan::Action(PlainAction::Write {
		relative: relative.to_string(),
		bytes: merged,
		mode,
		expected: Some(now),
	}))
}

fn preserve_plain_conflict(
	repo_root: &Path,
	plain: &PlainCapture,
	id: &str,
	relative: &str,
) -> Result<Option<PathBuf>, String> {
	let sanitized = relative.replace(['/', '\\'], "__");
	let dir = plain
		.artifacts_dir
		.join(format!("{id}.conflicts"))
		.join(&sanitized);
	fs::create_dir_all(&dir).map_err(|error| format!("create {}: {error}", dir.display()))?;
	let mut copied = false;
	for (label, root) in [
		("base", plain.base_dir.as_path()),
		("current", repo_root),
		("result", plain.result_dir.as_path()),
	] {
		let source = root.join(relative);
		let Some(entry) = plain_entry_at(&source).map_err(|error| format!("{label}: {error}"))?
		else {
			continue;
		};
		let destination = dir.join(label);
		remove_plain_node(&destination)?;
		copy_plain_node(&source, &destination, entry)?;
		copied = true;
	}
	Ok(copied.then_some(dir))
}

fn plain_target(root: &Path, relative: &str) -> Result<PathBuf, PlainApplyError> {
	let path = Path::new(relative);
	let safe = !path.is_absolute()
		&& path
			.components()
			.all(|component| matches!(component, std::path::Component::Normal(_)));
	if !safe {
		return Err(PlainApplyError::Io(format!(
			"refusing unsafe isolation path {relative}"
		)));
	}
	let target = root.join(path);
	validate_plain_ancestors(root, &target)?;
	Ok(target)
}

/// Reject a parent component that is a symlink.
///
/// This is best-effort confinement, not a sandbox: the check is not atomic
/// with the later write, so an external process can still swap a directory
/// between the check and the publish. That residual race is reported as a
/// stale/external change by the publish path, never claimed as CAS.
fn validate_plain_ancestors(root: &Path, target: &Path) -> Result<(), PlainApplyError> {
	let Ok(relative) = target.strip_prefix(root) else {
		return Err(PlainApplyError::Io(format!(
			"{} is outside {}",
			target.display(),
			root.display()
		)));
	};
	let mut probe = root.to_path_buf();
	let mut components = relative.components().peekable();
	while let Some(component) = components.next() {
		if components.peek().is_none() {
			// The final component is the node itself; the publish path handles
			// (and replaces) it without following it.
			break;
		}
		let std::path::Component::Normal(name) = component else {
			return Err(PlainApplyError::Io(format!(
				"refusing unsafe isolation path {}",
				target.display()
			)));
		};
		probe.push(name);
		match fs::symlink_metadata(&probe) {
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
			Err(error) => {
				return Err(PlainApplyError::Io(format!(
					"stat {}: {error}",
					probe.display()
				)));
			},
			Ok(meta) if meta.file_type().is_symlink() => {
				return Err(PlainApplyError::External(format!(
					"refusing to write {} through the symlinked directory {}; the parent must not redirect task output",
					target.display(),
					probe.display()
				)));
			},
			Ok(_) => {},
		}
	}
	Ok(())
}

/// Create missing parent directories one component at a time with `create_dir`
/// (never `create_dir_all`, which would silently accept an existing symlink).
fn ensure_plain_parents(root: &Path, target: &Path) -> Result<(), PlainApplyError> {
	let parent = target.parent().unwrap_or(root);
	let Ok(relative) = parent.strip_prefix(root) else {
		return Err(PlainApplyError::Io(format!(
			"{} is outside {}",
			target.display(),
			root.display()
		)));
	};
	let mut current = root.to_path_buf();
	for component in relative.components() {
		let std::path::Component::Normal(name) = component else {
			return Err(PlainApplyError::Io(format!(
				"refusing unsafe isolation path {}",
				target.display()
			)));
		};
		current.push(name);
		match fs::symlink_metadata(&current) {
			Ok(meta) if meta.file_type().is_symlink() => {
				return Err(PlainApplyError::External(format!(
					"refusing to write {} through the symlinked directory {}; the parent must not redirect task output",
					target.display(),
					current.display()
				)));
			},
			Ok(meta) if meta.is_dir() => {},
			Ok(_) => {
				return Err(PlainApplyError::External(format!(
					"refusing to write {}: {} is not a directory",
					target.display(),
					current.display()
				)));
			},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				match fs::create_dir(&current) {
					Ok(()) => {},
					Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
						let meta = fs::symlink_metadata(&current).map_err(|error| {
							PlainApplyError::Io(format!("stat {}: {error}", current.display()))
						})?;
						if meta.file_type().is_symlink() || !meta.is_dir() {
							return Err(PlainApplyError::External(format!(
								"refusing to write {}: {} appeared and is not a directory",
								target.display(),
								current.display()
							)));
						}
					},
					Err(error) => {
						return Err(PlainApplyError::Io(format!(
							"create {}: {error}",
							current.display()
						)));
					},
				}
			},
			Err(error) => {
				return Err(PlainApplyError::Io(format!(
					"stat {}: {error}",
					current.display()
				)));
			},
		}
	}
	Ok(())
}

fn apply_plain_action(
	writer: Option<&WorkspaceWriter>,
	root: &Path,
	action: &PlainAction,
) -> Result<(), PlainApplyError> {
	let relative = action.relative();
	let target = plain_target(root, relative)?;
	let _lease = match writer {
		Some(writer) => Some(writer.acquire(&target).map_err(|error| match error {
			LockError::Occupied { .. } => PlainApplyError::External(format!(
				"another omp writer holds the lease for {}",
				target.display()
			)),
			LockError::Io(error) => PlainApplyError::Io(format!(
				"workspace writer lease failed for {}: {error}",
				target.display()
			)),
		})?),
		None => None,
	};
	ensure_plain_parents(root, &target)?;
	let observed = plain_entry_at(&target)
		.map_err(|error| PlainApplyError::Io(format!("{error}")))?;
	let expected = match action {
		PlainAction::Write { expected, .. }
		| PlainAction::Symlink { expected, .. }
		| PlainAction::Delete { expected, .. } => *expected,
	};
	if !plain_same(observed, expected) {
		return Err(PlainApplyError::Stale(format!(
			"{} changed while the task ran; refusing to overwrite it",
			target.display()
		)));
	}
	match action {
		PlainAction::Delete { .. } => fs::remove_file(&target)
			.map_err(|error| PlainApplyError::Io(format!("remove {}: {error}", target.display()))),
		PlainAction::Symlink { target: link, .. } => {
			plain_publish_symlink(&target, link, expected)
		},
		PlainAction::Write { bytes, mode, .. } => plain_publish_file(&target, bytes, *mode, expected),
	}
}

/// Publish file bytes over the parent path.
///
/// - A symlink currently at the path is replaced atomically with `rename(2)`
///   on a same-directory temporary; `rename` replaces the link itself and
///   never follows it.
/// - An existing regular file goes through [`replace_if_unchanged`] when small
///   enough for a [`FileVersion`]; larger files use the same temporary publish.
/// - A new path is created with exclusive create, chmodded through the open
///   descriptor, and removed on failure only when the inode is still the one
///   this call created.
///
/// None of these is an atomic compare-and-swap: an external process can still
/// race the final rename. Every failure is reported, and nothing that is not
/// provably this call's own output is deleted.
fn plain_publish_file(
	target: &Path,
	bytes: &[u8],
	mode: u32,
	expected: Option<PlainEntry>,
) -> Result<(), PlainApplyError> {
	if matches!(expected, Some(entry) if entry.kind == PlainKind::Symlink) {
		return plain_publish_temp(target, bytes, mode, expected);
	}
	match expected {
		Some(entry) => {
			// Never follow a link that appeared after the preflight.
			match fs::symlink_metadata(target) {
				Ok(meta) if meta.file_type().is_symlink() => {
					return Err(PlainApplyError::Stale(format!(
						"{} became a symlink during integration; refusing to write through it",
						target.display()
					)));
				},
				Ok(meta) if meta.is_file() => {},
				Ok(_) => {
					return Err(PlainApplyError::Stale(format!(
						"{} is no longer a regular file; refusing to overwrite it",
						target.display()
					)));
				},
				Err(error) => {
					return Err(PlainApplyError::Io(format!(
						"stat {}: {error}",
						target.display()
					)));
				},
			}
			match FileVersion::read(target) {
				Ok(version) => {
					if xxhash_rust::xxh64::xxh64(version.raw(), 0) != entry.hash
						|| version.raw().len() as u64 != entry.len
					{
						return Err(PlainApplyError::Stale(format!(
							"{} changed while the task ran; refusing to overwrite it",
							target.display()
						)));
					}
					replace_if_unchanged(target, &version, bytes, Some(mode)).map_err(|error| {
						if error.is_stale() {
							PlainApplyError::Stale(format!(
								"{} changed during the write; refusing to overwrite it",
								target.display()
							))
						} else {
							PlainApplyError::Io(format!("write {}: {error}", target.display()))
						}
					})
				},
				Err(VersionError::TooLarge { .. }) => {
					plain_publish_temp(target, bytes, mode, expected)
				},
				Err(VersionError::Stale { .. }) => Err(PlainApplyError::Stale(format!(
					"{} changed while the task ran; refusing to overwrite it",
					target.display()
				))),
				Err(error) => Err(PlainApplyError::Io(format!(
					"read {}: {error}",
					target.display()
				))),
			}
		},
		None => {
			// New file: exclusive create reports a concurrently appearing path
			// instead of overwriting it.
			let mut options = fs::OpenOptions::new();
			options.write(true).create_new(true);
			#[cfg(unix)]
			{
				use std::os::unix::fs::OpenOptionsExt;
				options.mode(mode & 0o7777);
			}
			let mut file = options.open(target).map_err(|error| {
				if error.kind() == std::io::ErrorKind::AlreadyExists {
					PlainApplyError::External(format!(
						"{} appeared while the task ran; refusing to overwrite it",
						target.display()
					))
				} else {
					PlainApplyError::Io(format!("create {}: {error}", target.display()))
				}
			})?;
			let identity = file.metadata().ok().as_ref().and_then(file_identity);
			let written = file
				.write_all(bytes)
				.and_then(|()| file.flush())
				.and_then(|()| set_file_mode(&file, mode));
			if let Err(error) = written {
				drop(file);
				remove_if_same_inode(target, identity);
				return Err(PlainApplyError::Io(format!(
					"write {}: {error}",
					target.display()
				)));
			}
			Ok(())
		},
	}
}

/// Publish through a same-directory temporary and `rename(2)`.
///
/// Used for files that cannot go through [`FileVersion`] and for replacing a
/// symlink: `rename` replaces the directory entry itself, never the link
/// target. The expected entry is re-checked immediately before the rename;
/// this narrows, but does not close, the external-writer race.
fn plain_publish_temp(
	target: &Path,
	bytes: &[u8],
	mode: u32,
	expected: Option<PlainEntry>,
) -> Result<(), PlainApplyError> {
	let parent = target.parent().unwrap_or_else(|| Path::new("."));
	let name = target
		.file_name()
		.map(|name| name.to_string_lossy().into_owned())
		.unwrap_or_else(|| "file".to_string());
	let temp = parent.join(format!(
		".{name}.omp-iso-{}-{}",
		std::process::id(),
		TEMP_SEQ.fetch_add(1, Ordering::SeqCst)
	));
	let result = (|| {
		let mut options = fs::OpenOptions::new();
		options.write(true).create_new(true);
		#[cfg(unix)]
		{
			use std::os::unix::fs::OpenOptionsExt;
			options.mode(mode & 0o7777);
		}
		let mut file = options
			.open(&temp)
			.map_err(|error| PlainApplyError::Io(format!("create {}: {error}", temp.display())))?;
		file.write_all(bytes)
			.map_err(|error| PlainApplyError::Io(format!("write {}: {error}", temp.display())))?;
		file.flush()
			.map_err(|error| PlainApplyError::Io(format!("flush {}: {error}", temp.display())))?;
		set_file_mode(&file, mode)
			.map_err(|error| PlainApplyError::Io(format!("chmod {}: {error}", temp.display())))?;
		let observed = plain_entry_at(target)
			.map_err(|error| PlainApplyError::Io(format!("{error}")))?;
		if !plain_same(observed, expected) {
			return Err(PlainApplyError::Stale(format!(
				"{} changed during the write; refusing to overwrite it",
				target.display()
			)));
		}
		fs::rename(&temp, target)
			.map_err(|error| PlainApplyError::Io(format!("rename {}: {error}", target.display())))
	})();
	if result.is_err() {
		let _ = fs::remove_file(&temp);
	}
	result
}

/// Atomically replace the directory entry with a symlink. `rename` removes the
/// old file/link without following it; a concurrently appearing directory makes
/// the rename fail without deleting anything.
fn plain_publish_symlink(
	target: &Path,
	link: &Path,
	expected: Option<PlainEntry>,
) -> Result<(), PlainApplyError> {
	let parent = target.parent().unwrap_or_else(|| Path::new("."));
	let name = target
		.file_name()
		.map(|name| name.to_string_lossy().into_owned())
		.unwrap_or_else(|| "link".to_string());
	let temp = parent.join(format!(
		".{name}.omp-iso-link-{}-{}",
		std::process::id(),
		TEMP_SEQ.fetch_add(1, Ordering::SeqCst)
	));
	let result = (|| {
		create_plain_symlink(link, &temp).map_err(|error| {
			PlainApplyError::Io(format!(
				"symlink {} -> {}: {error}",
				temp.display(),
				link.display()
			))
		})?;
		let observed = plain_entry_at(target)
			.map_err(|error| PlainApplyError::Io(format!("{error}")))?;
		if !plain_same(observed, expected) {
			return Err(PlainApplyError::Stale(format!(
				"{} changed during the write; refusing to overwrite it",
				target.display()
			)));
		}
		fs::rename(&temp, target)
			.map_err(|error| PlainApplyError::Io(format!("rename {}: {error}", target.display())))
	})();
	if result.is_err() {
		let _ = fs::remove_file(&temp);
	}
	result
}

fn set_file_mode(file: &fs::File, mode: u32) -> std::io::Result<()> {
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		file.set_permissions(fs::Permissions::from_mode(mode & 0o7777))
	}
	#[cfg(not(unix))]
	{
		let _ = (file, mode);
		Ok(())
	}
}

fn file_identity(meta: &fs::Metadata) -> Option<(u64, u64)> {
	#[cfg(unix)]
	{
		use std::os::unix::fs::MetadataExt;
		Some((meta.dev(), meta.ino()))
	}
	#[cfg(not(unix))]
	{
		let _ = meta;
		None
	}
}

/// Best-effort removal of this call's own failed output. An external writer
/// that replaced the inode keeps its file.
fn remove_if_same_inode(path: &Path, identity: Option<(u64, u64)>) {
	#[cfg(unix)]
	{
		use std::os::unix::fs::MetadataExt;
		let Some(expected) = identity else { return };
		if let Ok(meta) = fs::symlink_metadata(path)
			&& (meta.dev(), meta.ino()) == expected
		{
			let _ = fs::remove_file(path);
		}
	}
	#[cfg(not(unix))]
	{
		let _ = (path, identity);
	}
}

// ─── sandbox creation ───────────────────────────────────────────────────────

/// Materialise a sandbox. The returned value owns cleanup: if the awaiting
/// future is dropped, the `spawn_blocking` task still returns a sandbox whose
/// `Drop` tears the mount down.
pub async fn ensure(
	repo_root: PathBuf,
	id: String,
	policy: IsolationPolicy,
	worktree_base: Option<PathBuf>,
) -> Result<IsolationSandbox, String> {
	let base = worktree_base.ok_or_else(|| {
		"task isolation has no worktree base configured; the CLI must resolve `worktree.base`".to_string()
	})?;
	let sandbox = tokio::task::spawn_blocking(move || {
		ensure_blocking(&repo_root, &id, policy, &base).map(IsolationSandbox::new)
	})
	.await
	.map_err(|error| format!("isolation setup task failed: {error}"))??;
	Ok(sandbox)
}

fn ensure_blocking(
	repo_root: &Path,
	id: &str,
	policy: IsolationPolicy,
	worktree_base: &Path,
) -> Result<IsolationHandle, String> {
	let repo_root = canonical_dir(repo_root)?;
	let is_git = has_git_marker(&repo_root);
	let common = if is_git {
		let raw = git_text(&repo_root, &["rev-parse", "--git-common-dir"])?;
		Some(canonical_dir(&resolve_relative(&repo_root, &raw))?)
	} else {
		None
	};
	let base_root = ensure_absolute_dir(worktree_base)?;
	check_disjoint(&base_root, &repo_root, common.as_deref())?;
	let base_dir = base_root.join(base_segment(&repo_root, id));
	check_disjoint(&base_dir, &repo_root, common.as_deref())?;
	if base_dir.exists() {
		return Err(format!(
			"isolation sandbox {} already exists; run `omp worktree clear` and retry",
			base_dir.display()
		));
	}
	let merged_dir = base_dir.join(ISOLATION_MOUNT_DIR);

	let resolution = pi_iso::resolve(policy.preferred);
	let mut fallback_reason = resolution.reason;
	let mut candidates = resolution.candidates;
	if candidates.is_empty() {
		candidates.push(BackendKind::Rcopy);
	}
	if common.is_none() {
		// Non-Git source: Rcopy is the only correct backend. Its recursive copy
		// freezes the live tree, whereas a live-lower view would fold external
		// parent edits into the child result. pi-iso's git path also requires
		// `.git`.
		fallback_reason.get_or_insert_with(|| {
			"workspace is not a Git checkout; using rcopy so the child sees a frozen copy of the live files".to_string()
		});
		candidates = vec![BackendKind::Rcopy];
	}

	for candidate in candidates {
		let staging = base_root.join(format!(
			".isolation-claim-{}-{}",
			std::process::id(),
			TEMP_SEQ.fetch_add(1, Ordering::SeqCst)
		));
		if let Err(error) = fs::create_dir(&staging) {
			return Err(format!("claim {}: {error}", staging.display()));
		}
		if let Err(error) = write_owner(&staging, id) {
			let _ = fs::remove_dir_all(&staging);
			return Err(error);
		}
		// Publish the fully-formed base (dir + owner marker) atomically, so the
		// CLI's orphan scan can never observe an unclaimed live sandbox.
		if let Err(error) = fs::rename(&staging, &base_dir) {
			let _ = fs::remove_dir_all(&staging);
			return Err(format!("publish {}: {error}", base_dir.display()));
		}
		match pi_iso::backend(candidate).start(&repo_root, &merged_dir) {
			Ok(()) => {
				let detached = match common.as_deref() {
					Some(common) => detach_git_metadata(&merged_dir, common),
					None => Ok(()),
				};
				if let Err(error) = detached {
					if pi_iso::backend(candidate).stop(&merged_dir).is_ok() {
						let _ = fs::remove_dir_all(&base_dir);
					}
					return Err(format!(
						"isolation backend {} started but the sandbox could not be detached from {}: {error}",
						candidate.as_str(),
						repo_root.display()
					));
				}
				return Ok(IsolationHandle {
					base_dir,
					merged_dir,
					backend: candidate,
					fell_back: resolution.fell_back || candidate != resolution.kind,
					fallback_reason,
				});
			},
			Err(IsoError::Unavailable(reason)) => {
				fallback_reason.get_or_insert(reason);
				stop_and_discard(candidate, &merged_dir, &base_dir)?;
			},
			Err(error) => {
				stop_and_discard(candidate, &merged_dir, &base_dir)?;
				return Err(format!(
					"isolation backend {} failed for {}: {error}",
					candidate.as_str(),
					repo_root.display()
				));
			},
		}
	}
	Err(fallback_reason
		.unwrap_or_else(|| "No isolation backend is available.".to_string()))
}

fn stop_and_discard(
	backend: BackendKind,
	merged_dir: &Path,
	base_dir: &Path,
) -> Result<(), String> {
	match pi_iso::backend(backend).stop(merged_dir) {
		Ok(()) => fs::remove_dir_all(base_dir).map_err(|error| {
			format!("remove failed sandbox {}: {error}", base_dir.display())
		}),
		Err(error) => Err(format!(
			"isolation backend {} stop failed after setup error ({error}); sandbox retained at {} for `omp worktree clear`",
			backend.as_str(),
			base_dir.display()
		)),
	}
}

/// Harden a freshly materialised sandbox before any child starts:
/// - reject submodule checkouts whose git metadata would stay shared,
/// - detach every nested repo's linked metadata from its source admin dir,
/// - materialise the frozen baseline trees in the child object DB.
///
/// Failure here is a setup failure (no child ran), so the caller may discard
/// the sandbox; the parent baseline itself is untouched.
pub async fn prepare_sandbox(
	sandbox: &IsolationSandbox,
	baseline: &WorktreeBaseline,
) -> Result<(), String> {
	let inner = Arc::clone(&sandbox.inner);
	let baseline = baseline.clone();
	tokio::task::spawn_blocking(move || prepare_sandbox_blocking(&inner, &baseline))
		.await
		.map_err(|error| format!("isolation preparation task failed: {error}"))?
}

fn prepare_sandbox_blocking(
	inner: &SandboxInner,
	baseline: &WorktreeBaseline,
) -> Result<(), String> {
	let _ops = inner
		.ops
		.lock()
		.unwrap_or_else(|poison| poison.into_inner());
	let merged = &inner.handle.merged_dir;
	if baseline.plain.is_some() {
		// Plain workspace: rcopy already froze the live tree and there is no
		// git metadata to sever or synthetic tree to write.
		return Ok(());
	}

	for submodule in &baseline.root.submodules {
		let source = baseline.root.repo_root.join(submodule);
		if !source.join(".git").exists() {
			continue;
		}
		let destination = merged.join(submodule);
		if !destination.exists() {
			fs::create_dir_all(&destination).map_err(|error| {
				format!("create submodule sandbox {}: {error}", destination.display())
			})?;
		}
		let admin = git_admin_dir(&source)?;
		let common = git_common_dir(&source)?;
		point_gitdir(&destination, &admin)?;
		detach_git_metadata(&destination, &common)?;
	}

	for entry in &baseline.nested {
		let destination = merged.join(&entry.relative_path);
		if !destination.exists() {
			fs::create_dir_all(&destination).map_err(|error| {
				format!("create nested sandbox {}: {error}", destination.display())
			})?;
		}
		let admin = git_admin_dir(&entry.baseline.repo_root)?;
		let common = git_common_dir(&entry.baseline.repo_root)?;
		point_gitdir(&destination, &admin)?;
		detach_git_metadata(&destination, &common)?;
		write_synthetic_tree(&destination, &entry.baseline)?;
	}
	write_synthetic_tree(merged, &baseline.root)?;
	Ok(())
}

fn git_common_dir(repo: &Path) -> Result<PathBuf, String> {
	let raw = git_text(repo, &["rev-parse", "--git-common-dir"])?;
	canonical_dir(&resolve_relative(repo, &raw))
}

/// Absolute git admin dir for `repo` (`.git` for a normal repo, the worktree
/// admin dir for a linked worktree/submodule).
fn git_admin_dir(repo: &Path) -> Result<PathBuf, String> {
	let raw = git_text(repo, &["rev-parse", "--absolute-git-dir"])?;
	canonical_dir(Path::new(&raw))
}

/// Point `destination/.git` at `admin` without touching `admin` itself. The
/// copied sandbox may carry a directory, a broken relative pointer, or a
/// pointer into the parent; replacing it here lets `detach_git_metadata` copy
/// the admin privately before severing the alternates edge.
fn point_gitdir(destination: &Path, admin: &Path) -> Result<(), String> {
	let entry = destination.join(".git");
	match fs::symlink_metadata(&entry) {
		Ok(meta) if meta.is_dir() => fs::remove_dir_all(&entry)
			.map_err(|error| format!("remove {}: {error}", entry.display()))?,
		Ok(_) => fs::remove_file(&entry)
			.map_err(|error| format!("remove {}: {error}", entry.display()))?,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
		Err(error) => return Err(format!("stat {}: {error}", entry.display())),
	}
	fs::create_dir_all(destination)
		.map_err(|error| format!("create {}: {error}", destination.display()))?;
	fs::write(&entry, format!("gitdir: {}\n", admin.display()))
		.map_err(|error| format!("write {}: {error}", entry.display()))
}

/// Remove the sandbox. Never recurses into a still-mounted view: a stop error
/// leaves the owned base dir in place for `omp worktree clear`.
pub fn cleanup_blocking(handle: &IsolationHandle) -> Result<(), String> {
	match pi_iso::backend(handle.backend).stop(&handle.merged_dir) {
		Ok(()) => match fs::remove_dir_all(&handle.base_dir) {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
			Err(error) => Err(format!(
				"isolation sandbox {} retained: {error}",
				handle.base_dir.display()
			)),
		},
		Err(error) => Err(format!(
			"isolation backend {} stop failed ({error}); sandbox retained at {} for `omp worktree clear`",
			handle.backend.as_str(),
			handle.base_dir.display()
		)),
	}
}

// ─── git metadata detach ────────────────────────────────────────────────────

fn registered_admin(git_entry: &Path) -> Result<Option<PathBuf>, String> {
	let text = fs::read_to_string(git_entry)
		.map_err(|error| format!("read {}: {error}", git_entry.display()))?;
	let Some(raw) = text.trim().strip_prefix("gitdir:") else {
		return Ok(None);
	};
	let admin = normalize_path(&git_entry.parent().unwrap_or(Path::new(".")).join(raw.trim()));
	let back = fs::read_to_string(admin.join("gitdir")).unwrap_or_default();
	let back = fs::canonicalize(back.trim()).unwrap_or_else(|_| normalize_path(Path::new(back.trim())));
	let entry = fs::canonicalize(git_entry).unwrap_or_else(|_| normalize_path(git_entry));
	Ok((back == entry).then_some(admin))
}

fn normalize_path(path: &Path) -> PathBuf {
	let mut out = PathBuf::new();
	for component in path.components() {
		match component {
			std::path::Component::CurDir => {},
			std::path::Component::ParentDir => {
				out.pop();
			},
			other => out.push(other.as_os_str()),
		}
	}
	out
}

fn read_prefixed(dir: &Path, prefix: &str) -> Vec<(String, Vec<u8>)> {
	let Ok(entries) = fs::read_dir(dir) else {
		return Vec::new();
	};
	entries
		.filter_map(Result::ok)
		.filter_map(|entry| {
			let name = entry.file_name().to_string_lossy().into_owned();
			if !name.starts_with(prefix) {
				return None;
			}
			let bytes = fs::read(entry.path()).ok()?;
			Some((name, bytes))
		})
		.collect()
}

fn snapshot_refs(merged: &Path) -> Result<Vec<(String, String)>, String> {
	let text = git_text(
		merged,
		&["for-each-ref", "--format=%(refname) %(objectname)"],
	)?;
	Ok(text
		.lines()
		.filter_map(|line| {
			let (name, id) = line.split_once(' ')?;
			Some((name.to_string(), id.to_string()))
		})
		.collect())
}

/// Sever a copied/linked worktree from shared git metadata while borrowing the
/// source object database through `alternates`. Mirrors
/// `pi-vcs::git::detach_git_dir` with the git CLI so pi-agent needs no gix/jj
/// dependency.
fn detach_git_metadata(merged: &Path, source_common: &Path) -> Result<(), String> {
	let entry = merged.join(".git");
	let meta = match fs::symlink_metadata(&entry) {
		Ok(meta) => meta,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
		Err(error) => return Err(format!("stat {}: {error}", entry.display())),
	};
	let iso_common = resolve_relative(
		merged,
		&git_text(merged, &["rev-parse", "--git-common-dir"])?,
	);
	let iso_common = canonical_dir(&iso_common)?;
	let source_common = canonical_dir(source_common)?;
	if iso_common != source_common {
		return Ok(());
	}

	let git_dir = if meta.is_dir() {
		entry.clone()
	} else {
		let text = fs::read_to_string(&entry)
			.map_err(|error| format!("read {}: {error}", entry.display()))?;
		let raw = text
			.trim()
			.strip_prefix("gitdir:")
			.ok_or_else(|| format!("{} is not a gitdir pointer", entry.display()))?;
		resolve_relative(merged, raw.trim())
	};
	let admin = if meta.is_file() {
		registered_admin(&entry)?
	} else {
		None
	};

	let head = fs::read(git_dir.join("HEAD")).ok();
	let index = fs::read(git_dir.join("index")).ok();
	let shared = read_prefixed(&git_dir, "sharedindex.");
	let sparse = fs::read(git_dir.join("info/sparse-checkout")).ok();
	let shallow = fs::read(source_common.join("shallow")).ok();
	let refs = snapshot_refs(merged)?;
	let configs = [
		"user.name",
		"user.email",
		"core.fileMode",
		"core.splitIndex",
		"core.sparseCheckout",
		"core.sparseCheckoutCone",
		"commit.gpgsign",
	]
	.into_iter()
	.filter_map(|key| {
		git_text(merged, &["config", "--get", key])
			.ok()
			.filter(|value| !value.is_empty())
			.map(|value| (key, value))
	})
	.collect::<Vec<_>>();

	if meta.is_dir() {
		fs::remove_dir_all(&entry)
			.map_err(|error| format!("remove {}: {error}", entry.display()))?;
	} else {
		fs::remove_file(&entry)
			.map_err(|error| format!("remove {}: {error}", entry.display()))?;
	}
	if let Some(admin) = admin
		&& admin.exists()
	{
		fs::remove_dir_all(&admin)
			.map_err(|error| format!("remove worktree admin {}: {error}", admin.display()))?;
	}

	git_ok(merged, &["init", "-q"])?;
	let git_dir = merged.join(".git");
	let objects_info = git_dir.join("objects/info");
	fs::create_dir_all(&objects_info)
		.map_err(|error| format!("create {}: {error}", objects_info.display()))?;
	let mut alternates = vec![source_common.join("objects")];
	if let Ok(chained) = fs::read_to_string(source_common.join("objects/info/alternates")) {
		for line in chained.lines().map(str::trim).filter(|line| !line.is_empty()) {
			let path = Path::new(line);
			alternates.push(if path.is_absolute() {
				path.to_path_buf()
			} else {
				source_common.join("objects").join(path)
			});
		}
	}
	let alternate_text = alternates
		.iter()
		.map(|path| path.to_string_lossy())
		.collect::<Vec<_>>()
		.join("\n")
		+ "\n";
	fs::write(objects_info.join("alternates"), alternate_text)
		.map_err(|error| format!("write alternates: {error}"))?;
	for (name, id) in refs {
		let path = git_dir.join(&name);
		if let Some(parent) = path.parent() {
			fs::create_dir_all(parent)
				.map_err(|error| format!("create {}: {error}", parent.display()))?;
		}
		fs::write(&path, format!("{id}\n"))
			.map_err(|error| format!("write ref {}: {error}", path.display()))?;
	}
	if let Some(head) = head {
		fs::write(git_dir.join("HEAD"), head)
			.map_err(|error| format!("write HEAD: {error}"))?;
	}
	for (key, value) in configs {
		git_ok(merged, &["config", "--local", key, value.as_str()])?;
	}
	if let Some(bytes) = shallow {
		fs::write(git_dir.join("shallow"), bytes)
			.map_err(|error| format!("write shallow: {error}"))?;
	}
	if let Some(bytes) = sparse {
		fs::create_dir_all(git_dir.join("info"))
			.map_err(|error| format!("create info: {error}"))?;
		fs::write(git_dir.join("info/sparse-checkout"), bytes)
			.map_err(|error| format!("write sparse-checkout: {error}"))?;
	}
	for (name, bytes) in shared {
		fs::write(git_dir.join(name), bytes)
			.map_err(|error| format!("write shared index: {error}"))?;
	}
	if let Some(bytes) = index {
		fs::write(git_dir.join("index"), bytes)
			.map_err(|error| format!("write index: {error}"))?;
	} else {
		let _ = git_ok(merged, &["read-tree", "HEAD"]);
	}
	Ok(())
}

// ─── capture ────────────────────────────────────────────────────────────────

fn capture_blocking(
	sandbox: &SandboxInner,
	baseline: &WorktreeBaseline,
	id: &str,
	artifacts_dir: &Path,
	_merge: IsolationMergeMode,
) -> Result<CapturedChanges, String> {
	let _ops = sandbox
		.ops
		.lock()
		.unwrap_or_else(|poison| poison.into_inner());
	let merged = &sandbox.handle.merged_dir;
	if let Some(plain) = &baseline.plain {
		return capture_plain(merged, plain, id, artifacts_dir);
	}

	let root_patch = capture_repo_delta(merged, &baseline.root)?;
	fs::create_dir_all(artifacts_dir)
		.map_err(|error| format!("create artifacts dir {}: {error}", artifacts_dir.display()))?;
	let patch_path = artifacts_dir.join(format!("{id}.patch"));
	fs::write(&patch_path, &root_patch)
		.map_err(|error| format!("write patch {}: {error}", patch_path.display()))?;

	let mut nested = Vec::new();
	for entry in &baseline.nested {
		let nested_dir = merged.join(&entry.relative_path);
		if !nested_dir.join(".git").exists() {
			continue;
		}
		let patch = capture_repo_delta(&nested_dir, &entry.baseline)?;
		if patch.iter().all(u8::is_ascii_whitespace) {
			continue;
		}
		let sanitized = entry.relative_path.replace(['/', '\\'], "__");
		let nested_path = artifacts_dir.join(format!("{id}.nested.{sanitized}.patch"));
		fs::write(&nested_path, &patch)
			.map_err(|error| format!("write nested patch {}: {error}", nested_path.display()))?;
		nested.push(NestedPatchArtifact {
			relative_path: entry.relative_path.clone(),
			patch,
			patch_path: Some(nested_path),
		});
	}

	let captured = CapturedChanges {
		root_patch,
		patch_path: Some(patch_path),
		nested,
		..CapturedChanges::default()
	};
	Ok(captured)
}

/// Prepare a parent task branch without ever applying baseline WIP to the
/// parent history.
///
/// - Clean baseline: fetch `HEAD` (plus a trailing commit for uncommitted
///   child work) and cherry-pick `baselineHead..branch`.
/// - Dirty baseline with agent commits: create a synthetic baseline commit `B`
///   whose tree is HEAD+WIP in the child ODB, rebase the agent commits onto
///   `B` with git's 3-way merge machinery (WIP-only hunks collapse), then fetch
///   `B..HEAD`; the parent cherry-picks only filtered agent commits.
/// - Dirty baseline without agent commits: no commit history to preserve, so
///   the exact baseline→current delta patch is applied instead.
fn prepare_branch_capture(
	merged: &Path,
	baseline: &RepoBaseline,
	id: &str,
	root_message: Option<&str>,
) -> Result<Option<(String, String)>, String> {
	if baseline.head_commit.is_empty() {
		return Ok(None);
	}
	let parent = &baseline.repo_root;
	let branch = format!("omp/task/{id}");
	let branch_ref = format!("refs/heads/{branch}");
	if git_ok(parent, &["rev-parse", "--verify", "--quiet", branch_ref.as_str()]).is_ok() {
		return Err(format!(
			"task branch {branch} already exists; changes kept in the patch artifact"
		));
	}
	let head = git_text(merged, &["rev-parse", "--verify", "HEAD"]).unwrap_or_default();
	if head.is_empty() {
		return Ok(None);
	}
	let committed = head != baseline.head_commit;
	let dirty_baseline = !baseline.is_clean();
	if dirty_baseline && !committed {
		return Ok(None);
	}

	// Gate on the real worktree, never the net baseline→final patch: a child
	// may commit A then leave an uncommitted revert, leaving net delta empty
	// while HEAD still carries A.
	ensure_trailing_commit(merged, root_message)?;

	let mut base_sha = baseline.head_commit.clone();
	if committed && dirty_baseline {
		let synthetic = create_baseline_commit(merged, baseline, id)?;
		rebase_onto_baseline(merged, &synthetic, &baseline.head_commit)?;
		base_sha = synthetic;
	}

	let current_head = git_text(merged, &["rev-parse", "HEAD"])?;
	if current_head == baseline.head_commit {
		return Ok(None);
	}
	fetch_task_branch(parent, merged, &branch)?;
	Ok(Some((branch, base_sha)))
}

fn ensure_trailing_commit(merged: &Path, message: Option<&str>) -> Result<bool, String> {
	let status = git_ok(merged, &["status", "--porcelain"])?;
	if status.is_empty() {
		return Ok(false);
	}
	git_ok(merged, &["add", "-A"])?;
	let message = message.unwrap_or("omp: apply isolated changes");
	git_ok(
		merged,
		&["-c", "commit.gpgsign=false", "commit", "--no-verify", "-m", message],
	)
	.or_else(|_| {
		git_ok(
			merged,
			&[
				"-c",
				"user.name=omp",
				"-c",
				"user.email=omp@localhost",
				"-c",
				"commit.gpgsign=false",
				"commit",
				"--no-verify",
				"-m",
				message,
			],
		)
	})?;
	Ok(true)
}

fn create_baseline_commit(
	merged: &Path,
	baseline: &RepoBaseline,
	id: &str,
) -> Result<String, String> {
	let tree = write_synthetic_tree(merged, baseline)?;
	let message = format!("omp: isolation baseline {id}");
	git_text(
		merged,
		&[
			"-c",
			"user.name=omp",
			"-c",
			"user.email=omp@localhost",
			"commit-tree",
			tree.as_str(),
			"-p",
			baseline.head_commit.as_str(),
			"-m",
			message.as_str(),
		],
	)
}

fn rebase_onto_baseline(
	merged: &Path,
	baseline_commit: &str,
	baseline_head: &str,
) -> Result<(), String> {
	let result = git_ok(
		merged,
		&[
			"-c",
			"user.name=omp",
			"-c",
			"user.email=omp@localhost",
			"-c",
			"commit.gpgsign=false",
			"rebase",
			"--onto",
			baseline_commit,
			baseline_head,
			"HEAD",
		],
	);
	if let Err(error) = result {
		let _ = git_ok(merged, &["rebase", "--abort"]);
		return Err(format!(
			"filtering agent commits over the dirty baseline failed: {error}"
		));
	}
	Ok(())
}

fn fetch_task_branch(parent: &Path, merged: &Path, branch: &str) -> Result<(), String> {
	let merged_text = merged.to_string_lossy().into_owned();
	let refspec = format!("HEAD:refs/heads/{branch}");
	git_ok(
		parent,
		&[
			"fetch",
			"--no-tags",
			"--no-write-fetch-head",
			merged_text.as_str(),
			refspec.as_str(),
		],
	)
	.map(|_| ())
}

// ─── merge ──────────────────────────────────────────────────────────────────

fn merge_blocking(
	repo_root: &Path,
	captured: &CapturedChanges,
	mode: IsolationMergeMode,
	id: &str,
	commits: IsolationCommitStyle,
	messages: &CommitMessages,
	cancel: &CancellationToken,
) -> (MergeOutcome, bool) {
	if cancel.is_cancelled() {
		return (
			MergeOutcome {
				applied: false,
				had_changes: false,
				summary: "\n\n<system-notification>Isolation merge cancelled before any parent mutation.</system-notification>"
					.to_string(),
				merged_branch_for_nested: false,
			},
			true,
		);
	}
	if captured.plain.is_some() {
		return (merge_plain(repo_root, id, captured, mode), false);
	}
	let outcome = match mode {
		IsolationMergeMode::Patch => merge_patch(repo_root, captured, messages, commits),
		IsolationMergeMode::Branch => merge_branch(repo_root, captured, messages, commits),
	};
	(outcome, false)
}

fn merge_patch(
	repo_root: &Path,
	captured: &CapturedChanges,
	messages: &CommitMessages,
	commits: IsolationCommitStyle,
) -> MergeOutcome {
	let root_empty = captured.root_patch.iter().all(u8::is_ascii_whitespace);
	let nested_empty = captured
		.nested
		.iter()
		.all(|nested| nested.patch.iter().all(u8::is_ascii_whitespace));
	if root_empty && nested_empty {
		return MergeOutcome {
			applied: true,
			had_changes: false,
			summary: "\n\nNo changes to apply.".to_string(),
			merged_branch_for_nested: false,
		};
	}
	let already_applied = !root_empty && patch_applies(repo_root, &captured.root_patch, true);
	let forward_applies = !root_empty && patch_applies(repo_root, &captured.root_patch, false);
	let mut had_changes = false;
	let applied = if root_empty {
		true
	} else if already_applied && !forward_applies {
		true
	} else if forward_applies {
		match apply_patch(repo_root, &captured.root_patch, &[]) {
			Ok(()) => {
				had_changes = true;
				true
			},
			Err(_) => false,
		}
	} else {
		false
	};

	if applied {
		let warnings = apply_nested(repo_root, &captured.nested, messages, commits);
		let mut summary = if root_empty {
			"\n\nNo root changes; nested repository patches applied.".to_string()
		} else if had_changes {
			"\n\nApplied patches: yes".to_string()
		} else {
			"\n\nNo changes to apply.".to_string()
		};
		for warning in warnings {
			summary.push_str(&format!("\n\n<system-notification>{warning}</system-notification>"));
		}
		MergeOutcome {
			applied,
			had_changes: had_changes || !nested_empty,
			summary,
			merged_branch_for_nested: !nested_empty,
		}
	} else {
		let patch_list = captured
			.patch_path
			.as_ref()
			.map_or_else(String::new, |path| {
				format!("\n\nPatch artifact:\n- {}", path.display())
			});
		MergeOutcome {
			applied: false,
			had_changes: false,
			summary: format!(
				"\n\n<system-notification>Patches were not applied and must be handled manually.</system-notification>{patch_list}"
			),
			merged_branch_for_nested: false,
		}
	}
}

fn merge_branch(
	repo_root: &Path,
	captured: &CapturedChanges,
	messages: &CommitMessages,
	commits: IsolationCommitStyle,
) -> MergeOutcome {
	if let Some(error) = &captured.error
		&& captured.branch.is_none()
	{
		let patch_list = captured
			.patch_path
			.as_ref()
			.map_or_else(String::new, |path| {
				format!("\nPatch artifact:\n- {}", path.display())
			});
		return MergeOutcome {
			applied: false,
			had_changes: false,
			summary: format!(
				"\n\n<system-notification>Branch merge failed while capturing the task branch: {error}\nTask outputs are preserved but changes were not applied.{patch_list}</system-notification>"
			),
			merged_branch_for_nested: false,
		};
	}
	let Some(branch) = &captured.branch else {
		if !captured.root_patch.is_empty() {
			let mut outcome = merge_patch(repo_root, captured, messages, commits);
			if let Some(reason) = &captured.branch_fallback_reason {
				outcome.summary = format!(
					"\n\n<system-notification>{reason}</system-notification>{}",
					outcome.summary
				);
			}
			return outcome;
		}
		let nested_only = !captured.nested.is_empty();
		let mut warnings = Vec::new();
		if nested_only {
			warnings = apply_nested(repo_root, &captured.nested, messages, commits);
		}
		let mut summary = if nested_only {
			"\n\nNo root changes to apply; nested repository patches captured.".to_string()
		} else {
			"\n\nNo changes to apply.".to_string()
		};
		for warning in warnings {
			summary.push_str(&format!("\n\n<system-notification>{warning}</system-notification>"));
		}
		return MergeOutcome {
			applied: true,
			had_changes: nested_only,
			summary,
			merged_branch_for_nested: nested_only,
		};
	};
	let base = captured.base_sha.as_deref().unwrap_or_default();
	if base.is_empty() {
		return MergeOutcome {
			applied: false,
			had_changes: false,
			summary: format!(
				"\n\n<system-notification>Branch {branch} has no base commit to cherry-pick from; the branch remains for manual resolution.</system-notification>"
			),
			merged_branch_for_nested: false,
		};
	}
	let range = format!("{base}..{branch}");
	let dirty = git_ok(repo_root, &["status", "--porcelain"])
		.map(|bytes| !bytes.is_empty())
		.unwrap_or(false);
	let stashed = dirty
		&& git_ok(
			repo_root,
			&[
				"stash",
				"push",
				"--include-untracked",
				"-m",
				"omp-task-merge",
			],
		)
		.is_ok();
	if dirty && !stashed {
		return MergeOutcome {
			applied: false,
			had_changes: false,
			summary: format!(
				"\n\n<system-notification>Could not stash the parent worktree; branch {branch} was not merged and remains for manual resolution.</system-notification>"
			),
			merged_branch_for_nested: false,
		};
	}
	if let Err(error) = git_ok(repo_root, &["cherry-pick", range.as_str()]) {
		let _ = git_ok(repo_root, &["cherry-pick", "--abort"]);
		if stashed {
			let _ = git_ok(repo_root, &["stash", "pop", "--index"]);
		}
		return MergeOutcome {
			applied: false,
			had_changes: false,
			summary: format!(
				"\n\n<system-notification>Branch merge failed: {branch}.\nConflict: {error}\nThe unmerged branch remains for manual resolution.</system-notification>"
			),
			merged_branch_for_nested: false,
		};
	}
	let mut warnings = Vec::new();
	if stashed && git_ok(repo_root, &["stash", "pop", "--index"]).is_err() {
		warnings.push(
			"Pre-existing parent changes could not be auto-restored after the cherry-pick; stash entry preserved. Run `git stash pop` and resolve manually."
				.to_string(),
		);
	}
	warnings.extend(apply_nested(repo_root, &captured.nested, messages, commits));
	let _ = git_ok(repo_root, &["branch", "-D", branch]);
	let mut summary = format!("\n\nMerged branch: {branch}");
	for warning in warnings {
		summary.push_str(&format!("\n\n<system-notification>{warning}</system-notification>"));
	}
	MergeOutcome {
		applied: true,
		had_changes: true,
		summary,
		merged_branch_for_nested: true,
	}
}

fn apply_nested(
	repo_root: &Path,
	nested: &[NestedPatchArtifact],
	messages: &CommitMessages,
	commits: IsolationCommitStyle,
) -> Vec<String> {
	let mut warnings = Vec::new();
	for (index, artifact) in nested.iter().enumerate() {
		if artifact.patch.iter().all(u8::is_ascii_whitespace) {
			continue;
		}
		let repo = repo_root.join(&artifact.relative_path);
		if !repo.join(".git").exists() {
			warnings.push(format!(
				"Nested repository `{}` is missing; its patch was not applied.",
				artifact.relative_path
			));
			continue;
		}
		let dirty = git_ok(&repo, &["status", "--porcelain"])
			.map(|bytes| !bytes.is_empty())
			.unwrap_or(false);
		let stash_message = format!(
			"omp isolation pre-merge {}",
			artifact.relative_path
		);
		let stashed = dirty
			&& git_ok(
				&repo,
				&[
					"stash",
					"push",
					"--include-untracked",
					"-m",
					stash_message.as_str(),
				],
			)
			.is_ok();
		if dirty && !stashed {
			warnings.push(format!(
				"Could not stash pre-existing changes in nested repo `{}`; patch not applied.",
				artifact.relative_path
			));
			continue;
		}
		let message = messages
			.nested
			.get(index)
			.and_then(Clone::clone)
			.unwrap_or_else(|| match commits {
				IsolationCommitStyle::Generic => format!(
					"omp: apply isolated changes in {}",
					artifact.relative_path
				),
				IsolationCommitStyle::Ai => String::new(),
			});
		if message.is_empty() {
			if stashed {
				let _ = git_ok(&repo, &["stash", "pop", "--index"]);
			}
			warnings.push(format!(
				"No generated commit message was available for nested repo `{}`; its patch was not applied.",
				artifact.relative_path
			));
			continue;
		}
		let applied = apply_patch(&repo, &artifact.patch, &[]).is_ok();
		if !applied {
			if stashed {
				let _ = git_ok(&repo, &["stash", "pop", "--index"]);
			}
			warnings.push(format!(
				"Patch for nested repo `{}` did not apply; pre-existing state restored.",
				artifact.relative_path
			));
			continue;
		}
		let _ = git_ok(&repo, &["add", "-A"]);
		let committed = git_ok(
			&repo,
			&[
				"-c",
				"commit.gpgsign=false",
				"commit",
				"--no-verify",
				"-m",
				message.as_str(),
			],
		)
		.or_else(|_| {
			git_ok(
					&repo,
					&[
						"-c",
						"user.name=omp",
						"-c",
						"user.email=omp@localhost",
						"-c",
						"commit.gpgsign=false",
						"commit",
						"--no-verify",
						"-m",
						message.as_str(),
					],
				)
			})
			.is_ok();
		if !committed {
			warnings.push(format!(
				"Applied the patch to nested repo `{}` but could not commit it.",
				artifact.relative_path
			));
		}
		if stashed && git_ok(&repo, &["stash", "pop", "--index"]).is_err() {
			warnings.push(format!(
				"Pre-existing dirty state in nested repo `{}` could not be auto-restored after the agent commit; stash entry preserved.",
				artifact.relative_path
			));
		}
	}
	warnings
}

// ─── finalize ───────────────────────────────────────────────────────────────

async fn generate_commit_messages(
	captured: &CapturedChanges,
	provider: &std::sync::Arc<dyn ModelProvider>,
	merge: IsolationMergeMode,
) -> Result<CommitMessages, String> {
	let mut messages = CommitMessages::default();
	// Patch mode never commits the root delta; only nested repositories get
	// commits, so an AI root subject would be a wasted provider call.
	if merge == IsolationMergeMode::Branch
		&& !captured.root_patch.iter().all(u8::is_ascii_whitespace)
	{
		messages.root = Some(generate_one_message(provider, &captured.root_patch).await?);
	}
	for artifact in &captured.nested {
		if artifact.patch.iter().all(u8::is_ascii_whitespace) {
			messages.nested.push(None);
			continue;
		}
		messages
			.nested
			.push(Some(generate_one_message(provider, &artifact.patch).await?));
	}
	Ok(messages)
}

async fn generate_one_message(
	provider: &std::sync::Arc<dyn ModelProvider>,
	diff: &[u8],
) -> Result<String, String> {
	let diff = String::from_utf8_lossy(diff);
	let truncated = diff.chars().take(24_000).collect::<String>();
	let messages = [
		Message::system(
			"Write one git commit subject (max 72 characters) that summarizes this diff. Return only the subject: no quotes, no markdown, no trailing period, no prefix.",
		),
		Message::user(truncated),
	];
	let response = provider
		.complete(&messages, &[], &ToolChoice::Auto)
		.await
		.map_err(|error| format!("commit message generation failed: {error}"))?;
	let subject = response
		.content
		.lines()
		.map(str::trim)
		.find(|line| !line.is_empty())
		.unwrap_or_default()
		.trim_matches('"')
		.trim();
	if subject.is_empty() {
		return Err("commit message generation returned an empty subject".to_string());
	}
	Ok(subject.chars().take(72).collect())
}

/// Capture, optionally merge, and clean up. `succeeded` must be `false` for a
/// failed or aborted run: its sandbox is torn down without capturing changes.
pub async fn finalize(
	sandbox: &IsolationSandbox,
	baseline: &WorktreeBaseline,
	policy: IsolationPolicy,
	id: &str,
	artifacts_dir: PathBuf,
	succeeded: bool,
	cancel: &CancellationToken,
	commit_provider: Option<Arc<dyn ModelProvider>>,
) -> IsolationFinalize {
	if !succeeded || cancel.is_cancelled() {
		let cancelled = cancel.is_cancelled();
		let cleanup_error = sandbox.cleanup();
		return IsolationFinalize {
			report: None,
			summary: String::new(),
			cleanup_error,
			cancelled,
		};
	}
	let inner = Arc::clone(&sandbox.inner);
	let baseline_owned = baseline.clone();
	let id_owned = id.to_string();
	let artifacts = artifacts_dir.clone();
	let merge_mode = policy.merge;
	let captured = tokio::task::spawn_blocking(move || {
		capture_blocking(&inner, &baseline_owned, &id_owned, &artifacts, merge_mode)
	})
	.await;

	let mut finalize = IsolationFinalize::default();
	let mut captured = match captured {
		Ok(Ok(captured)) => captured,
		Ok(Err(error)) => {
			finalize.report = Some(IsolationReport {
				backend: Some(sandbox.handle().backend.as_str().to_string()),
				error: Some(error.clone()),
				..IsolationReport::default()
			});
			finalize.summary = format!(
				"\n\n<system-notification>Isolation capture failed: {error}\nThe sandbox and child work were retained at {} for manual recovery.</system-notification>",
				sandbox.handle().base_dir.display()
			);
			sandbox.retain();
			if let Some(report) = finalize.report.as_mut() {
				report.summary = Some(finalize.summary.trim().to_string());
			}
			return finalize;
		},
		Err(error) => {
			let message = format!("isolation capture task failed: {error}");
			finalize.report = Some(IsolationReport {
				backend: Some(sandbox.handle().backend.as_str().to_string()),
				error: Some(message.clone()),
				..IsolationReport::default()
			});
			finalize.summary = format!(
				"\n\n<system-notification>Isolation capture failed: {message}\nThe sandbox and child work were retained at {} for manual recovery.</system-notification>",
				sandbox.handle().base_dir.display()
			);
			sandbox.retain();
			if let Some(report) = finalize.report.as_mut() {
				report.summary = Some(finalize.summary.trim().to_string());
			}
			return finalize;
		},
	};

	let mut commit_messages = CommitMessages::default();
	if policy.commits == IsolationCommitStyle::Ai
		&& baseline.plain.is_none()
		&& (policy.apply || policy.merge == IsolationMergeMode::Branch)
	{
		let Some(provider) = commit_provider.clone() else {
			finalize.summary = "\n\n<system-notification>task.isolation.commits: ai requires a resolved child model; no commit message was generated and no merge was applied.</system-notification>".to_string();
			finalize.report = Some(IsolationReport {
				patch_path: captured
					.patch_path
					.as_ref()
					.map(|path| path.display().to_string()),
				changes_applied: Some(false),
				backend: Some(sandbox.handle().backend.as_str().to_string()),
				error: Some("missing child model for ai commit messages".to_string()),
				summary: Some(finalize.summary.trim().to_string()),
				..IsolationReport::default()
			});
			sandbox.retain();
			return finalize;
		};
		match generate_commit_messages(&captured, &provider, policy.merge).await {
			Ok(mut messages) => {
				if policy.merge == IsolationMergeMode::Branch && messages.root.is_none() {
					let merged = sandbox.handle().merged_dir.clone();
					let trailing = tokio::task::spawn_blocking(move || {
						let head = git_text(&merged, &["rev-parse", "--verify", "HEAD"])
							.unwrap_or_default();
						if head.is_empty() {
							return Ok::<_, String>(Vec::new());
						}
						let baseline = RepoBaseline {
							repo_root: merged.clone(),
							head_commit: head,
							staged: Vec::new(),
							unstaged: Vec::new(),
							untracked: Vec::new(),
							untracked_patch: Vec::new(),
							submodules: Vec::new(),
						};
						capture_repo_delta(&merged, &baseline)
					})
					.await;
					let trailing_patch = match trailing {
						Ok(Ok(patch)) if !patch.iter().all(u8::is_ascii_whitespace) => Some(patch),
						Ok(Ok(_)) => None,
						Ok(Err(error)) => {
							finalize.summary = format!(
								"\n\n<system-notification>AI commit message generation failed: {error}\nNo merge was applied; the sandbox was retained at {}.</system-notification>",
								sandbox.handle().base_dir.display()
							);
							finalize.report = Some(IsolationReport {
								patch_path: captured
									.patch_path
									.as_ref()
									.map(|path| path.display().to_string()),
								changes_applied: Some(false),
								backend: Some(sandbox.handle().backend.as_str().to_string()),
								error: Some(error),
								summary: Some(finalize.summary.trim().to_string()),
								..IsolationReport::default()
							});
							sandbox.retain();
							return finalize;
						},
						Err(error) => {
							let error = format!("trailing diff task failed: {error}");
							finalize.summary = format!(
								"\n\n<system-notification>AI commit message generation failed: {error}\nNo merge was applied; the sandbox was retained at {}.</system-notification>",
								sandbox.handle().base_dir.display()
							);
							finalize.report = Some(IsolationReport {
								patch_path: captured
									.patch_path
									.as_ref()
									.map(|path| path.display().to_string()),
								changes_applied: Some(false),
								backend: Some(sandbox.handle().backend.as_str().to_string()),
								error: Some(error),
								summary: Some(finalize.summary.trim().to_string()),
								..IsolationReport::default()
							});
							sandbox.retain();
							return finalize;
						},
					};
					if let Some(patch) = trailing_patch {
						match generate_one_message(&provider, &patch).await {
							Ok(message) => messages.root = Some(message),
							Err(error) => {
								finalize.summary = format!(
									"\n\n<system-notification>AI commit message generation failed: {error}\nNo merge was applied; the sandbox was retained at {}.</system-notification>",
									sandbox.handle().base_dir.display()
								);
								finalize.report = Some(IsolationReport {
									patch_path: captured
										.patch_path
										.as_ref()
										.map(|path| path.display().to_string()),
									changes_applied: Some(false),
									backend: Some(sandbox.handle().backend.as_str().to_string()),
									error: Some(error),
									summary: Some(finalize.summary.trim().to_string()),
									..IsolationReport::default()
								});
								sandbox.retain();
								return finalize;
							},
						}
					}
				}
				commit_messages = messages;
			},
			Err(error) => {
				finalize.summary = format!(
					"\n\n<system-notification>AI commit message generation failed: {error}\nNo merge was applied; the sandbox and child work were retained at {}.</system-notification>",
					sandbox.handle().base_dir.display()
				);
				finalize.report = Some(IsolationReport {
					patch_path: captured
						.patch_path
						.as_ref()
						.map(|path| path.display().to_string()),
					changes_applied: Some(false),
					backend: Some(sandbox.handle().backend.as_str().to_string()),
					error: Some(error),
					summary: Some(finalize.summary.trim().to_string()),
					..IsolationReport::default()
				});
				sandbox.retain();
				return finalize;
			},
		}
	}

	if policy.merge == IsolationMergeMode::Branch && baseline.plain.is_none() {
		let merged = sandbox.handle().merged_dir.clone();
		let baseline_root = baseline.root.clone();
		let id_owned = id.to_string();
		let root_message = commit_messages.root.clone();
		let branch = tokio::task::spawn_blocking(move || {
			prepare_branch_capture(&merged, &baseline_root, &id_owned, root_message.as_deref())
		})
		.await;
		match branch {
			Ok(Ok(Some((branch, base_sha)))) => {
				captured.branch = Some(branch);
				captured.base_sha = Some(base_sha);
			},
			Ok(Ok(None)) => {
				if !baseline.root.is_clean()
					&& !captured.root_patch.iter().all(u8::is_ascii_whitespace)
				{
					captured.branch_fallback_reason = Some(
						"No agent commits needed preservation and the parent worktree was dirty; applied the captured child-delta patch instead of creating a branch."
							.to_string(),
					);
				}
			},
			Ok(Err(error)) => captured.error = Some(error),
			Err(error) => captured.error = Some(format!("branch capture task failed: {error}")),
		}
	}

	let mut report = IsolationReport {
		patch_path: captured
			.patch_path
			.as_ref()
			.map(|path| path.display().to_string()),
		branch_name: captured.branch.clone(),
		base_sha: captured.base_sha.clone(),
		nested_patches: captured
			.nested
			.iter()
			.map(|nested| NestedRepoPatch {
				relative_path: nested.relative_path.clone(),
				patch: String::from_utf8_lossy(&nested.patch).into_owned(),
			})
			.collect(),
		changes_applied: None,
		backend: Some(sandbox.handle().backend.as_str().to_string()),
		error: captured.error.clone(),
		summary: None,
	};
	if cancel.is_cancelled() {
		let artifact = report
			.patch_path
			.clone()
			.map(|path| format!("patch artifact {path}"))
			.or_else(|| {
				report
					.branch_name
					.clone()
					.map(|branch| format!("task branch {branch}"))
			})
			.unwrap_or_else(|| format!("sandbox {}", sandbox.handle().base_dir.display()));
		finalize.summary = format!(
			"\n\n<system-notification>Isolation merge cancelled; captured artifacts were kept at {artifact}.</system-notification>"
		);
		finalize.cancelled = true;
		finalize.report = Some(report);
		finalize.cleanup_error = sandbox.cleanup();
		return finalize;
	}

	let mut keep_sandbox = false;
	if !policy.apply {
		let mut summary = String::from("\n\nIsolated changes retained; merge skipped (apply=false).");
		if let Some(path) = &report.patch_path {
			summary.push_str(&format!("\nPatch artifact:\n- {path}"));
		}
		if let Some(branch) = &report.branch_name {
			summary.push_str(&format!(
				"\nTask branch: {branch} (base {})",
				report.base_sha.as_deref().unwrap_or("")
			));
		}
		for nested in &captured.nested {
			if let Some(path) = &nested.patch_path {
				summary.push_str(&format!(
					"\nNested patch ({}):\n- {}",
					nested.relative_path,
					path.display()
				));
			}
		}
		if let Some(plain) = &captured.plain {
			summary.push_str(&format!(
				"\nIsolated artifact: {}",
				plain.result_dir.display()
			));
		}
		finalize.summary = summary;
	} else if let Some(error) = &captured.error
		&& captured.branch.is_none()
		&& policy.merge == IsolationMergeMode::Branch
	{
		finalize.summary = format!(
			"\n\n<system-notification>Branch merge failed while capturing the task branch: {error}\nTask outputs and the full sandbox were retained at {} for manual resolution.</system-notification>",
			sandbox.handle().base_dir.display()
		);
		report.changes_applied = Some(false);
		keep_sandbox = true;
	} else {
		let repo_root = baseline.root.repo_root.clone();
		let captured_for_merge = captured.clone();
		let id_owned = id.to_string();
		let commits = policy.commits;
		let messages = commit_messages.clone();
		let cancel_for_merge = cancel.clone();
		let outcome = tokio::task::spawn_blocking(move || {
			// The guard lives inside the blocking worker: a cancelled async
			// waiter cannot release it while the git mutation is still running.
			let _merge = MERGE_LOCK
				.lock()
				.unwrap_or_else(|poison| poison.into_inner());
			merge_blocking(
				&repo_root,
				&captured_for_merge,
				merge_mode,
				&id_owned,
				commits,
				&messages,
				&cancel_for_merge,
			)
		})
		.await;
		match outcome {
			Ok((outcome, cancelled_before_merge)) => {
				finalize.summary = outcome.summary;
				if cancelled_before_merge {
					finalize.cancelled = true;
					report.changes_applied = None;
				} else {
					report.changes_applied = Some(outcome.applied);
				}
			},
			Err(error) => {
				report.changes_applied = Some(false);
				finalize.summary = format!(
					"\n\n<system-notification>Merge phase failed: {error}\nTask outputs are preserved but changes were not applied.</system-notification>"
				);
			},
		}
	}
	if cancel.is_cancelled() {
		finalize.cancelled = true;
		finalize
			.summary
			.push_str("\n\n<system-notification>Isolation finalize cancelled.</system-notification>");
	}

	finalize.report = Some(report);
	if keep_sandbox {
		sandbox.retain();
	} else {
		finalize.cleanup_error = sandbox.cleanup();
	}
	if let Some(error) = &finalize.cleanup_error {
		finalize.summary.push_str(&format!(
			"\n\n<system-notification>Isolation cleanup failed: {error}</system-notification>"
		));
	}
	if let Some(report) = finalize.report.as_mut()
		&& !finalize.summary.is_empty()
	{
		report.summary = Some(finalize.summary.trim().to_string());
	}
	finalize
}

#[cfg(test)]
mod tests {
	use std::process::Command;

	use super::*;

	struct TempDir(PathBuf);

	impl TempDir {
		fn new(label: &str) -> Self {
			let path = std::env::temp_dir().join(format!(
				"omp-iso-test-{label}-{}-{}",
				std::process::id(),
				now_nanos()
			));
			fs::create_dir_all(&path).expect("temp dir");
			Self(path)
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
			"git {args:?}: {}",
			String::from_utf8_lossy(&output.stderr)
		);
		String::from_utf8_lossy(&output.stdout).trim().to_string()
	}

	fn repo(dir: &Path) {
		git(dir, &["init", "-q"]);
		git(dir, &["config", "user.email", "test@example.com"]);
		git(dir, &["config", "user.name", "test"]);
		fs::write(dir.join("tracked.txt"), "base\n").expect("write");
		git(dir, &["add", "tracked.txt"]);
		git(dir, &["commit", "-qm", "base"]);
	}

	fn capture_branch_for_test(
		sandbox: &IsolationSandbox,
		baseline: &WorktreeBaseline,
		id: &str,
		artifacts: &Path,
	) -> CapturedChanges {
		let mut captured = capture_blocking(
			&sandbox.inner,
			baseline,
			id,
			artifacts,
			IsolationMergeMode::Branch,
		)
		.expect("capture");
		let branch = prepare_branch_capture(sandbox.merged_dir(), &baseline.root, id, None)
			.expect("branch");
		if let Some((branch, base_sha)) = branch {
			captured.branch = Some(branch);
			captured.base_sha = Some(base_sha);
		}
		captured
	}

	#[test]
	fn baseline_delta_excludes_dirty_parent_and_keeps_binary() {
		let temp = TempDir::new("delta");
		let parent = temp.0.join("repo");
		fs::create_dir_all(&parent).expect("repo dir");
		repo(&parent);
		fs::write(parent.join("tracked.txt"), "base\ndirty\n").expect("dirty");
		fs::write(parent.join("untracked.txt"), "parent wip\n").expect("wip");

		let baseline = prepare_context_blocking(&parent).expect("baseline");
		let base = temp.0.join("wt");
		fs::create_dir_all(&base).expect("wt");
		// Force the always-available backend so this test needs no mount privileges.
		let policy = IsolationPolicy {
			merge: IsolationMergeMode::Patch,
			apply: true,
			commits: IsolationCommitStyle::Generic,
			preferred: Some(BackendKind::Rcopy),
		};
		let handle =
			ensure_blocking(&parent, "task-1", policy, &base).expect("sandbox");
		let sandbox = IsolationSandbox::new(handle);

		fs::write(sandbox.merged_dir().join("tracked.txt"), "base\ndirty\nagent\n")
			.expect("agent edit");
		fs::write(sandbox.merged_dir().join("agent.bin"), [0, 1, 2, 3]).expect("binary");
		let artifacts = temp.0.join("artifacts");
		let captured = capture_blocking(
			&sandbox.inner,
			&baseline,
			"task-1",
			&artifacts,
			IsolationMergeMode::Patch,
		)
		.expect("capture");

		let patch = String::from_utf8_lossy(&captured.root_patch);
		assert!(patch.contains("+agent"), "{patch}");
		assert!(patch.contains("agent.bin"), "{patch}");
		assert!(!patch.contains("+parent wip"), "{patch}");
		assert!(
			fs::read(parent.join("tracked.txt")).expect("parent") == b"base\ndirty\n",
			"parent must stay unchanged before merge"
		);

		let outcome = merge_patch(&parent, &captured, &CommitMessages::default(), IsolationCommitStyle::Generic);
		assert!(outcome.applied);
		assert_eq!(
			fs::read_to_string(parent.join("tracked.txt")).expect("merged"),
			"base\ndirty\nagent\n"
		);
		assert_eq!(fs::read(parent.join("agent.bin")).expect("bin"), [0, 1, 2, 3]);
		assert!(sandbox.cleanup().is_none());
		assert!(!sandbox.handle().base_dir.exists());
	}

	#[test]
	fn cancel_cleanup_removes_owned_sandbox_only() {
		let temp = TempDir::new("cancel");
		let parent = temp.0.join("repo");
		fs::create_dir_all(&parent).expect("repo dir");
		repo(&parent);
		let baseline = prepare_context_blocking(&parent).expect("baseline");
		let base = temp.0.join("wt");
		fs::create_dir_all(&base).expect("wt");
		let policy = IsolationPolicy {
			merge: IsolationMergeMode::Patch,
			apply: true,
			commits: IsolationCommitStyle::Generic,
			preferred: Some(BackendKind::Rcopy),
		};
		let handle =
			ensure_blocking(&parent, "task-2", policy, &base).expect("sandbox");
		let sandbox = IsolationSandbox::new(handle);
		let owned = sandbox.handle().base_dir.clone();
		drop(sandbox);
		assert!(!owned.exists(), "sandbox base removed");
		assert!(parent.join("tracked.txt").exists(), "source checkout intact");
		assert!(!baseline.root.head_commit.is_empty());
	}

	#[test]
	fn branch_mode_preserves_agent_commits_and_applies_them() {
		let temp = TempDir::new("branch");
		let parent = temp.0.join("repo");
		fs::create_dir_all(&parent).expect("repo dir");
		repo(&parent);
		let baseline = prepare_context_blocking(&parent).expect("baseline");
		let base = temp.0.join("wt");
		fs::create_dir_all(&base).expect("wt");
		let policy = IsolationPolicy {
			merge: IsolationMergeMode::Branch,
			apply: true,
			commits: IsolationCommitStyle::Generic,
			preferred: Some(BackendKind::Rcopy),
		};
		let sandbox = IsolationSandbox::new(
			ensure_blocking(&parent, "task-4", policy, &base).expect("sandbox"),
		);
		fs::write(sandbox.merged_dir().join("tracked.txt"), "base\ncommitted\n")
			.expect("agent");
		git(sandbox.merged_dir(), &["add", "-A"]);
		git(sandbox.merged_dir(), &["commit", "-qm", "agent commit"]);
		// Uncommitted B on top of committed A must become a trailing branch
		// commit, not be lost by the cherry-pick.
		fs::write(sandbox.merged_dir().join("tracked.txt"), "base\ncommitted\nleftover\n")
			.expect("leftover");
		let captured = capture_branch_for_test(
			&sandbox,
			&baseline,
			"task-4",
			&temp.0.join("artifacts"),
		);
		let branch = captured.branch.as_deref().expect("branch");
		assert_eq!(
			git(&parent, &["rev-parse", branch]),
			git(sandbox.merged_dir(), &["rev-parse", "HEAD"])
		);
		let outcome = merge_branch(&parent, &captured, &CommitMessages::default(), IsolationCommitStyle::Generic);
		assert!(outcome.applied, "{}", outcome.summary);
		assert_eq!(
			fs::read_to_string(parent.join("tracked.txt")).expect("merged"),
			"base\ncommitted\nleftover\n"
		);
		let branch_exists = Command::new("git")
			.arg("-C")
			.arg(&parent)
			.args(["rev-parse", "--verify", "--quiet", branch])
			.output()
			.expect("git");
		assert!(
			!branch_exists.status.success(),
			"task branch is deleted after a successful cherry-pick"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[test]
	fn merge_conflict_keeps_patch_and_parent_intact() {
		let temp = TempDir::new("conflict");
		let parent = temp.0.join("repo");
		fs::create_dir_all(&parent).expect("repo dir");
		repo(&parent);
		let baseline = prepare_context_blocking(&parent).expect("baseline");
		let base = temp.0.join("wt");
		fs::create_dir_all(&base).expect("wt");
		let policy = IsolationPolicy {
			merge: IsolationMergeMode::Patch,
			apply: true,
			commits: IsolationCommitStyle::Generic,
			preferred: Some(BackendKind::Rcopy),
		};
		let sandbox = IsolationSandbox::new(
			ensure_blocking(&parent, "task-3", policy, &base).expect("sandbox"),
		);
		fs::write(sandbox.merged_dir().join("tracked.txt"), "base\nagent\n").expect("agent");
		let captured = capture_blocking(
			&sandbox.inner,
			&baseline,
			"task-3",
			&temp.0.join("artifacts"),
			IsolationMergeMode::Patch,
		)
		.expect("capture");
		// Parent drifts after capture; the patch must not overwrite it.
		fs::write(parent.join("tracked.txt"), "parent conflict\n").expect("parent drift");
		let outcome = merge_patch(&parent, &captured, &CommitMessages::default(), IsolationCommitStyle::Generic);
		assert!(!outcome.applied);
		assert_eq!(
			fs::read_to_string(parent.join("tracked.txt")).expect("parent"),
			"parent conflict\n"
		);
		assert!(
			captured
				.patch_path
				.as_ref()
				.is_some_and(|path| path.exists()),
			"patch artifact retained"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[test]
	fn nested_repo_metadata_is_detached_and_parent_untouched() {
		let temp = TempDir::new("nested");
		let parent = temp.0.join("repo");
		fs::create_dir_all(&parent).expect("repo dir");
		repo(&parent);
		let nested = parent.join("nested");
		fs::create_dir_all(&nested).expect("nested dir");
		repo(&nested);
		fs::write(nested.join("inner.txt"), "inner\n").expect("inner");
		git(&nested, &["add", "inner.txt"]);
		git(&nested, &["commit", "-qm", "inner"]);
		let parent_head = git(&nested, &["rev-parse", "HEAD"]);
		let baseline = prepare_context_blocking(&parent).expect("baseline");
		assert_eq!(baseline.nested.len(), 1);

		let base = temp.0.join("wt");
		fs::create_dir_all(&base).expect("wt");
		let policy = IsolationPolicy {
			merge: IsolationMergeMode::Patch,
			apply: false,
			commits: IsolationCommitStyle::Generic,
			preferred: Some(BackendKind::Rcopy),
		};
		let sandbox = IsolationSandbox::new(
			ensure_blocking(&parent, "task-5", policy, &base).expect("sandbox"),
		);
		prepare_sandbox_blocking(&sandbox.inner, &baseline).expect("prepare");
		let child_nested = sandbox.merged_dir().join("nested");
		fs::write(child_nested.join("inner.txt"), "inner\nchild\n").expect("child");
		git(&child_nested, &["add", "inner.txt"]);
		git(&child_nested, &["commit", "-qm", "child commit"]);

		assert_eq!(
			git(&nested, &["rev-parse", "HEAD"]),
			parent_head,
			"parent nested HEAD must not move"
		);
		assert!(
			git(&nested, &["status", "--porcelain"]).is_empty(),
			"parent nested index must stay clean"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn merge_lock_survives_dropped_async_waiter() {
		let (acquired_tx, mut acquired_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
		let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
		let held = tokio::task::spawn_blocking(move || {
			let _guard = MERGE_LOCK
				.lock()
				.unwrap_or_else(|poison| poison.into_inner());
			let _ = acquired_tx.send(());
			let _ = release_rx.recv();
		});
		tokio::time::timeout(std::time::Duration::from_secs(5), acquired_rx.recv())
			.await
			.expect("worker acquired lock")
			.expect("signal");
		// Dropping the async waiter (simulating a cancelled finalize) must not
		// release the blocking lock while the mutation still runs.
		drop(held);

		let (second_tx, mut second_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
		let second = tokio::task::spawn_blocking(move || {
			let _guard = MERGE_LOCK
				.lock()
				.unwrap_or_else(|poison| poison.into_inner());
			let _ = second_tx.send(());
		});
		assert!(
			tokio::time::timeout(std::time::Duration::from_millis(150), second_rx.recv())
				.await
				.is_err(),
			"second merge entered while the first worker still held the lock"
		);
		let _ = release_tx.send(());
		tokio::time::timeout(std::time::Duration::from_secs(5), second_rx.recv())
			.await
			.expect("second merge after release")
			.expect("signal");
		second.await.expect("second worker");
	}

	#[test]
	fn branch_committed_then_uncommitted_revert_keeps_net_zero() {
		let temp = TempDir::new("revert");
		let parent = temp.0.join("repo");
		fs::create_dir_all(&parent).expect("repo dir");
		repo(&parent);
		let baseline = prepare_context_blocking(&parent).expect("baseline");
		let base = temp.0.join("wt");
		fs::create_dir_all(&base).expect("wt");
		let policy = IsolationPolicy {
			merge: IsolationMergeMode::Branch,
			apply: true,
			commits: IsolationCommitStyle::Generic,
			preferred: Some(BackendKind::Rcopy),
		};
		let sandbox = IsolationSandbox::new(
			ensure_blocking(&parent, "task-6", policy, &base).expect("sandbox"),
		);
		fs::write(sandbox.merged_dir().join("tracked.txt"), "base\nagent\n").expect("agent");
		git(sandbox.merged_dir(), &["add", "-A"]);
		git(sandbox.merged_dir(), &["commit", "-qm", "agent commit"]);
		fs::write(sandbox.merged_dir().join("tracked.txt"), "base\n").expect("revert");
		let captured = capture_branch_for_test(
			&sandbox,
			&baseline,
			"task-6",
			&temp.0.join("artifacts"),
		);
		assert!(
			captured.root_patch.iter().all(u8::is_ascii_whitespace),
			"net delta is empty"
		);
		assert!(captured.branch.is_some(), "committed history preserved");
		let outcome = merge_branch(&parent, &captured, &CommitMessages::default(), IsolationCommitStyle::Generic);
		assert!(outcome.applied, "{}", outcome.summary);
		assert_eq!(
			fs::read_to_string(parent.join("tracked.txt")).expect("parent"),
			"base\n"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[test]
	fn cancelled_merge_never_touches_parent() {
		let temp = TempDir::new("cancelmerge");
		let parent = temp.0.join("repo");
		fs::create_dir_all(&parent).expect("repo dir");
		repo(&parent);
		let baseline = prepare_context_blocking(&parent).expect("baseline");
		let base = temp.0.join("wt");
		fs::create_dir_all(&base).expect("wt");
		let policy = IsolationPolicy {
			merge: IsolationMergeMode::Patch,
			apply: true,
			commits: IsolationCommitStyle::Generic,
			preferred: Some(BackendKind::Rcopy),
		};
		let sandbox = IsolationSandbox::new(
			ensure_blocking(&parent, "task-7", policy, &base).expect("sandbox"),
		);
		fs::write(sandbox.merged_dir().join("tracked.txt"), "base\nagent\n").expect("agent");
		let captured = capture_blocking(
			&sandbox.inner,
			&baseline,
			"task-7",
			&temp.0.join("artifacts"),
			IsolationMergeMode::Patch,
		)
		.expect("capture");
		let cancel = CancellationToken::new();
		cancel.cancel();
		let (outcome, cancelled) =
			merge_blocking(&parent, &captured, IsolationMergeMode::Patch, "task-7", IsolationCommitStyle::Generic, &CommitMessages::default(), &cancel);
		assert!(cancelled);
		assert!(!outcome.applied);
		assert_eq!(
			fs::read_to_string(parent.join("tracked.txt")).expect("parent"),
			"base\n"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[test]
	fn submodule_metadata_is_private_and_parent_untouched() {
		let temp = TempDir::new("submodule");
		let source = temp.0.join("source");
		fs::create_dir_all(&source).expect("source dir");
		repo(&source);
		let parent = temp.0.join("repo");
		fs::create_dir_all(&parent).expect("repo dir");
		repo(&parent);
		let sub_url = source.to_string_lossy().into_owned();
		git(
			&parent,
			&[
				"-c",
				"protocol.file.allow=always",
				"submodule",
				"add",
				sub_url.as_str(),
				"sub",
			],
		);
		git(&parent, &["commit", "-qm", "add submodule"]);
		let parent_sub_head = git(&parent.join("sub"), &["rev-parse", "HEAD"]);
		let baseline = prepare_context_blocking(&parent).expect("baseline");
		assert_eq!(baseline.root.submodules, vec!["sub".to_string()]);

		let base = temp.0.join("wt");
		fs::create_dir_all(&base).expect("wt");
		let policy = IsolationPolicy {
			merge: IsolationMergeMode::Patch,
			apply: false,
			commits: IsolationCommitStyle::Generic,
			preferred: Some(BackendKind::Rcopy),
		};
		let sandbox = IsolationSandbox::new(
			ensure_blocking(&parent, "task-8", policy, &base).expect("sandbox"),
		);
		prepare_sandbox_blocking(&sandbox.inner, &baseline).expect("prepare");
		let child_sub = sandbox.merged_dir().join("sub");
		fs::write(child_sub.join("child.txt"), "child\n").expect("child");
		git(&child_sub, &["add", "-A"]);
		git(
			&child_sub,
			&[
				"-c",
				"user.name=test",
				"-c",
				"user.email=test@example.com",
				"commit",
				"-qm",
				"child submodule",
			],
		);
		assert_eq!(
			git(&parent.join("sub"), &["rev-parse", "HEAD"]),
			parent_sub_head,
			"parent submodule HEAD must not move"
		);
		assert!(
			git(&parent.join("sub"), &["status", "--porcelain"]).is_empty(),
			"parent submodule index must stay clean"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[test]
	fn policy_none_rejects_requested_isolation() {
		let settings = TaskIsolationSettings::default();
		let request = IsolationRequest {
			isolated: Some(true),
			..IsolationRequest::default()
		};
		assert!(resolve_policy(&settings, &request, true).is_err());
		let request = IsolationRequest::default();
		assert_eq!(resolve_policy(&settings, &request, true).expect("none"), None);
	}

	#[test]
	fn invalid_mode_values_are_rejected() {
		assert!(IsolationMode::parse("bogus").is_err());
		assert_eq!(IsolationMode::parse("worktree").expect("legacy"), IsolationMode::Rcopy);
		assert!(IsolationMergeMode::parse("squash").is_err());
		assert!(IsolationCommitStyle::parse("ai").is_ok());
	}

	// ─── non-git workspace ──────────────────────────────────────────────

	fn plain_policy(merge: IsolationMergeMode) -> IsolationPolicy {
		IsolationPolicy {
			merge,
			apply: true,
			commits: IsolationCommitStyle::Generic,
			preferred: Some(BackendKind::Rcopy),
		}
	}

	fn plain_project(dir: &Path) {
		fs::create_dir_all(dir.join("src")).expect("src dir");
		fs::write(dir.join("src/main.txt"), "one\ntwo\nthree\n").expect("main");
		fs::write(dir.join("keep.bin"), [0_u8, 1, 2, 3]).expect("bin");
	}

	fn plain_sandbox(
		temp: &TempDir,
		parent: &Path,
		id: &str,
		merge: IsolationMergeMode,
	) -> (WorktreeBaseline, IsolationSandbox) {
		let baseline = prepare_context_blocking(parent).expect("baseline");
		assert!(baseline.plain.is_some(), "non-git baseline is plain");
		assert!(baseline.root.head_commit.is_empty());
		let base = temp.0.join("wt");
		fs::create_dir_all(&base).expect("wt");
		let sandbox = IsolationSandbox::new(
			ensure_blocking(&baseline.root.repo_root, id, plain_policy(merge), &base)
				.expect("sandbox"),
		);
		prepare_sandbox_blocking(&sandbox.inner, &baseline).expect("prepare");
		(baseline, sandbox)
	}

	#[test]
	fn plain_workspace_task_delta_applies_without_git() {
		let temp = TempDir::new("plain");
		let parent = temp.0.join("project");
		plain_project(&parent);
		// Nested git metadata is copied for the child but must never be
		// captured or merged.
		fs::create_dir_all(parent.join("vendor/.git")).expect("nested git");
		fs::write(parent.join("vendor/.git/config"), "base\n").expect("git metadata");

		let (baseline, sandbox) =
			plain_sandbox(&temp, &parent, "task-plain", IsolationMergeMode::Patch);
		let merged = sandbox.merged_dir().to_path_buf();
		fs::write(merged.join("src/main.txt"), "one\ntwo\nthree\nagent\n").expect("edit");
		fs::write(merged.join("src/new.sh"), "#!/bin/sh\n").expect("new file");
		set_plain_mode(&merged.join("src/new.sh"), 0o755).expect("mode");
		fs::write(merged.join("vendor/.git/config"), "child\n").expect("git edit");

		let artifacts = temp.0.join("artifacts");
		let captured = capture_blocking(
			&sandbox.inner,
			&baseline,
			"task-plain",
			&artifacts,
			IsolationMergeMode::Patch,
		)
		.expect("capture");
		assert_eq!(
			captured.plain.as_ref().expect("plain capture").changed,
			2,
			"only the agent's own edits count"
		);
		let patch = String::from_utf8_lossy(&captured.root_patch).into_owned();
		assert!(patch.contains("+agent"), "{patch}");
		assert!(!patch.contains("vendor/.git"), "git metadata excluded: {patch}");

		let (outcome, cancelled) = merge_blocking(
			&baseline.root.repo_root,
			&captured,
			IsolationMergeMode::Patch,
			"task-plain",
			IsolationCommitStyle::Generic,
			&CommitMessages::default(),
			&CancellationToken::new(),
		);
		assert!(!cancelled);
		assert!(outcome.applied, "{}", outcome.summary);
		assert_eq!(
			fs::read_to_string(parent.join("src/main.txt")).expect("main"),
			"one\ntwo\nthree\nagent\n"
		);
		assert_eq!(
			fs::read_to_string(parent.join("src/new.sh")).expect("new"),
			"#!/bin/sh\n"
		);
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt;
			assert_eq!(
				fs::metadata(parent.join("src/new.sh"))
					.expect("mode")
					.permissions()
					.mode()
					& 0o777,
				0o755,
				"executable bit is preserved"
			);
		}
		assert_eq!(
			fs::read_to_string(parent.join("vendor/.git/config")).expect("git metadata"),
			"base\n",
			"nested .git metadata is never merged"
		);
		assert!(
			captured
				.patch_path
				.as_ref()
				.is_some_and(|path| path.exists()),
			"patch artifact retained"
		);
		assert!(
			captured.plain.as_ref().expect("plain").result_dir.is_dir(),
			"isolated artifact retained"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[test]
	fn plain_three_way_merges_disjoint_parent_and_child_edits() {
		let temp = TempDir::new("threeway");
		let parent = temp.0.join("project");
		plain_project(&parent);
		let (baseline, sandbox) =
			plain_sandbox(&temp, &parent, "task-3way", IsolationMergeMode::Patch);
		fs::write(
			sandbox.merged_dir().join("src/main.txt"),
			"one\ntwo\nthree\nchild\n",
		)
		.expect("child");
		let captured = capture_blocking(
			&sandbox.inner,
			&baseline,
			"task-3way",
			&temp.0.join("artifacts"),
			IsolationMergeMode::Patch,
		)
		.expect("capture");
		// The user edits a different region after the baseline was taken.
		fs::write(parent.join("src/main.txt"), "parent\ntwo\nthree\n").expect("parent drift");

		let (outcome, _) = merge_blocking(
			&parent,
			&captured,
			IsolationMergeMode::Patch,
			"task-3way",
			IsolationCommitStyle::Generic,
			&CommitMessages::default(),
			&CancellationToken::new(),
		);
		assert!(outcome.applied, "{}", outcome.summary);
		assert_eq!(
			fs::read_to_string(parent.join("src/main.txt")).expect("merged"),
			"parent\ntwo\nthree\nchild\n",
			"non-overlapping parent and child edits are both kept"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[test]
	fn plain_conflict_preserves_versions_and_leaves_parent_unchanged() {
		let temp = TempDir::new("plainconflict");
		let parent = temp.0.join("project");
		plain_project(&parent);
		let (baseline, sandbox) =
			plain_sandbox(&temp, &parent, "task-conflict", IsolationMergeMode::Patch);
		fs::write(
			sandbox.merged_dir().join("src/main.txt"),
			"one\nchild\nthree\n",
		)
		.expect("child");
		let artifacts = temp.0.join("artifacts");
		let captured = capture_blocking(
			&sandbox.inner,
			&baseline,
			"task-conflict",
			&artifacts,
			IsolationMergeMode::Patch,
		)
		.expect("capture");
		fs::write(parent.join("src/main.txt"), "one\nparent\nthree\n").expect("parent drift");

		let (outcome, _) = merge_blocking(
			&parent,
			&captured,
			IsolationMergeMode::Patch,
			"task-conflict",
			IsolationCommitStyle::Generic,
			&CommitMessages::default(),
			&CancellationToken::new(),
		);
		assert!(!outcome.applied);
		assert!(
			!fs::read_to_string(parent.join("src/main.txt"))
				.expect("parent")
				.contains("<<<<<<<"),
			"conflict markers are never written into the workspace"
		);
		assert_eq!(
			fs::read_to_string(parent.join("src/main.txt")).expect("parent"),
			"one\nparent\nthree\n",
			"the user's edit is untouched"
		);
		let bundle = artifacts
			.join("task-conflict.conflicts")
			.join("src__main.txt");
		assert_eq!(
			fs::read_to_string(bundle.join("base")).expect("base"),
			"one\ntwo\nthree\n"
		);
		assert_eq!(
			fs::read_to_string(bundle.join("current")).expect("current"),
			"one\nparent\nthree\n"
		);
		assert_eq!(
			fs::read_to_string(bundle.join("result")).expect("result"),
			"one\nchild\nthree\n"
		);
		assert!(outcome.summary.contains("conflict"), "{}", outcome.summary);
		assert!(sandbox.cleanup().is_none());
	}

	#[test]
	fn plain_merge_defers_to_a_held_parent_lease() {
		let temp = TempDir::new("plainlease");
		let parent = temp.0.join("project");
		plain_project(&parent);
		let (baseline, sandbox) =
			plain_sandbox(&temp, &parent, "task-lease", IsolationMergeMode::Patch);
		fs::write(
			sandbox.merged_dir().join("src/main.txt"),
			"one\ntwo\nthree\nchild\n",
		)
		.expect("child");
		let captured = capture_blocking(
			&sandbox.inner,
			&baseline,
			"task-lease",
			&temp.0.join("artifacts"),
			IsolationMergeMode::Patch,
		)
		.expect("capture");

		// Another writer (the parent session) holds the workspace lease for the
		// target; integration must report it instead of clobbering.
		let writer = WorkspaceWriter::new().expect("writer lease root");
		let _held = writer
			.acquire(&parent.join("src/main.txt"))
			.expect("hold parent lease");
		let (outcome, _) = merge_blocking(
			&parent,
			&captured,
			IsolationMergeMode::Patch,
			"task-lease",
			IsolationCommitStyle::Generic,
			&CommitMessages::default(),
			&CancellationToken::new(),
		);
		assert!(!outcome.applied, "{}", outcome.summary);
		assert!(
			outcome.summary.contains("lease"),
			"the held lease is reported: {}",
			outcome.summary
		);
		assert_eq!(
			fs::read_to_string(parent.join("src/main.txt")).expect("parent"),
			"one\ntwo\nthree\n"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[cfg(unix)]
	#[test]
	fn plain_symlinked_parent_directory_cannot_redirect_task_output() {
		let temp = TempDir::new("plainlinkdir");
		let parent = temp.0.join("project");
		fs::create_dir_all(&parent).expect("project");
		fs::write(parent.join("keep.txt"), "keep\n").expect("keep");
		let (baseline, sandbox) =
			plain_sandbox(&temp, &parent, "task-linkdir", IsolationMergeMode::Patch);
		fs::create_dir_all(sandbox.merged_dir().join("out")).expect("out");
		fs::write(sandbox.merged_dir().join("out/report.txt"), "report\n").expect("report");
		let captured = capture_blocking(
			&sandbox.inner,
			&baseline,
			"task-linkdir",
			&temp.0.join("artifacts"),
			IsolationMergeMode::Patch,
		)
		.expect("capture");
		// The parent redirects `out` outside the workspace after the baseline.
		let outside = temp.0.join("outside");
		fs::create_dir_all(&outside).expect("outside");
		std::os::unix::fs::symlink(&outside, parent.join("out")).expect("symlink");

		let (outcome, _) = merge_blocking(
			&parent,
			&captured,
			IsolationMergeMode::Patch,
			"task-linkdir",
			IsolationCommitStyle::Generic,
			&CommitMessages::default(),
			&CancellationToken::new(),
		);
		assert!(!outcome.applied, "{}", outcome.summary);
		assert!(
			!outside.join("report.txt").exists(),
			"nothing may be written through a symlinked parent directory"
		);
		assert!(
			fs::symlink_metadata(parent.join("out"))
				.expect("symlink")
				.file_type()
				.is_symlink(),
			"the parent's symlink is not replaced"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[cfg(unix)]
	#[test]
	fn plain_symlink_replaced_by_file_is_applied() {
		let temp = TempDir::new("plainlinkfile");
		let parent = temp.0.join("project");
		fs::create_dir_all(&parent).expect("project");
		fs::write(parent.join("target.txt"), "target\n").expect("target");
		std::os::unix::fs::symlink("target.txt", parent.join("link.txt")).expect("link");
		let (baseline, sandbox) =
			plain_sandbox(&temp, &parent, "task-linkfile", IsolationMergeMode::Patch);
		fs::remove_file(sandbox.merged_dir().join("link.txt")).expect("remove link");
		fs::write(sandbox.merged_dir().join("link.txt"), "now a file\n").expect("file");
		let captured = capture_blocking(
			&sandbox.inner,
			&baseline,
			"task-linkfile",
			&temp.0.join("artifacts"),
			IsolationMergeMode::Patch,
		)
		.expect("capture");
		assert!(
			fs::symlink_metadata(parent.join("link.txt"))
				.expect("parent link")
				.file_type()
				.is_symlink(),
			"parent still has the link before merge"
		);

		let (outcome, _) = merge_blocking(
			&parent,
			&captured,
			IsolationMergeMode::Patch,
			"task-linkfile",
			IsolationCommitStyle::Generic,
			&CommitMessages::default(),
			&CancellationToken::new(),
		);
		assert!(outcome.applied, "{}", outcome.summary);
		let meta = fs::symlink_metadata(parent.join("link.txt")).expect("link");
		assert!(!meta.file_type().is_symlink(), "link became a regular file");
		assert_eq!(
			fs::read_to_string(parent.join("link.txt")).expect("link file"),
			"now a file\n"
		);
		assert_eq!(
			fs::read_to_string(parent.join("target.txt")).expect("target"),
			"target\n",
			"the link target is never written through"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[test]
	fn plain_new_paths_are_never_overwritten_when_they_appear() {
		let temp = TempDir::new("plainappear");
		let parent = temp.0.join("project");
		fs::create_dir_all(&parent).expect("project");
		fs::write(parent.join("keep.txt"), "keep\n").expect("keep");
		let (baseline, sandbox) =
			plain_sandbox(&temp, &parent, "task-appear", IsolationMergeMode::Patch);
		fs::write(sandbox.merged_dir().join("new.txt"), "task\n").expect("new");
		fs::write(sandbox.merged_dir().join("blocked.txt"), "task\n").expect("blocked");
		let captured = capture_blocking(
			&sandbox.inner,
			&baseline,
			"task-appear",
			&temp.0.join("artifacts"),
			IsolationMergeMode::Patch,
		)
		.expect("capture");
		// An external writer creates the same new file and a directory where the
		// task wanted a file, both after capture.
		fs::write(parent.join("new.txt"), "user\n").expect("user file");
		fs::create_dir_all(parent.join("blocked.txt")).expect("user dir");

		let (outcome, _) = merge_blocking(
			&parent,
			&captured,
			IsolationMergeMode::Patch,
			"task-appear",
			IsolationCommitStyle::Generic,
			&CommitMessages::default(),
			&CancellationToken::new(),
		);
		assert!(!outcome.applied, "{}", outcome.summary);
		assert_eq!(
			fs::read_to_string(parent.join("new.txt")).expect("user file"),
			"user\n",
			"an externally created file is not overwritten"
		);
		assert!(
			parent.join("blocked.txt").is_dir(),
			"an externally created directory is not replaced"
		);
		assert!(sandbox.cleanup().is_none());
	}

	#[test]
	fn broken_git_checkout_is_not_downgraded_to_plain() {
		let temp = TempDir::new("brokengit");
		let parent = temp.0.join("repo");
		fs::create_dir_all(parent.join(".git")).expect(".git dir");
		let result = prepare_context_blocking(&parent);
		let message = result.expect_err("broken checkout must not become a plain workspace");
		assert!(
			message.contains("not downgraded") || message.contains("could not read the Git checkout"),
			"{message}"
		);
	}

	#[test]
	fn plain_empty_workspace_creates_the_task_files() {
		let temp = TempDir::new("plainempty");
		let parent = temp.0.join("project");
		fs::create_dir_all(&parent).expect("project");
		let (baseline, sandbox) =
			plain_sandbox(&temp, &parent, "task-empty", IsolationMergeMode::Branch);
		fs::write(sandbox.merged_dir().join("new.txt"), "created\n").expect("new");
		let captured = capture_blocking(
			&sandbox.inner,
			&baseline,
			"task-empty",
			&temp.0.join("artifacts"),
			IsolationMergeMode::Branch,
		)
		.expect("capture");
		let (outcome, _) = merge_blocking(
			&parent,
			&captured,
			IsolationMergeMode::Branch,
			"task-empty",
			IsolationCommitStyle::Generic,
			&CommitMessages::default(),
			&CancellationToken::new(),
		);
		assert!(outcome.applied, "{}", outcome.summary);
		assert_eq!(
			fs::read_to_string(parent.join("new.txt")).expect("new"),
			"created\n"
		);
		assert!(sandbox.cleanup().is_none());
	}
}
