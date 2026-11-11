//! Immutable scan plans and freshness checks.
//!
//! Anchors: `packages/coding-agent/src/security/preflight.ts`.
//!
//! A plan pins the repository snapshot (working-tree digest or resolved diff),
//! include/exclude scope, knowledge-base hashes, output directory state, model,
//! exact OAuth account, config snapshot, and workflow fingerprint. Starting a
//! scan re-runs the exact same material computation; any drift is a typed stale
//! plan error carrying expected/actual fingerprints.

use std::{
	fs,
	path::{Path, PathBuf},
	process::Command,
};

use sha2::{Digest as _, Sha256};

use super::{
	ids::{canonical_security_json, create_security_plan_id, sha256_hex},
	provenance,
	types::{
		SecurityAccountRef, SecurityKnowledgeBaseRef, SecurityModelRef, SecurityOutputPlan, SecurityScanPlan,
		SecurityTarget,
	},
};

/// Target request before normalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecurityTargetRequest {
	Repository { include_paths: Vec<String>, exclude_paths: Vec<String> },
	ScopedPath { include_paths: Vec<String>, exclude_paths: Vec<String> },
	RefDiff {
		base_revision: String,
		head_revision: String,
		include_paths: Vec<String>,
		exclude_paths: Vec<String>,
	},
	WorkingTree { include_paths: Vec<String>, exclude_paths: Vec<String> },
}

impl SecurityTargetRequest {
	#[must_use]
	pub fn parts(&self) -> (&str, &[String], &[String]) {
		match self {
			Self::Repository { include_paths, exclude_paths } => ("repository", include_paths, exclude_paths),
			Self::ScopedPath { include_paths, exclude_paths } => ("scoped_path", include_paths, exclude_paths),
			Self::RefDiff { include_paths, exclude_paths, .. } => ("ref_diff", include_paths, exclude_paths),
			Self::WorkingTree { include_paths, exclude_paths } => ("working_tree", include_paths, exclude_paths),
		}
	}
}

/// Git surface used by preflight and the coordinator. Synchronous because every
/// implementation is a bounded local operation.
pub trait SecurityGitAdapter: Send + Sync {
	fn root(&self, cwd: &Path) -> Result<Option<PathBuf>, String>;
	fn head_sha(&self, cwd: &Path) -> Result<Option<String>, String>;
	fn resolve_ref(&self, cwd: &Path, ref_name: &str) -> Result<Option<String>, String>;
	fn diff_tree(&self, cwd: &Path, base: &str, head: &str) -> Result<String, String>;
	fn status(&self, cwd: &Path) -> Result<String, String>;
	fn files(&self, cwd: &Path) -> Result<Vec<String>, String>;
	fn untracked(&self, cwd: &Path) -> Result<Vec<String>, String>;
	fn worktree_add(&self, cwd: &Path, path: &Path, reference: &str) -> Result<(), String>;
	fn worktree_remove(&self, cwd: &Path, path: &Path) -> Result<bool, String>;
}

/// `DEFAULT_SECURITY_GIT_ADAPTER`, implemented with the git CLI (the same
/// dependency boundary `pi-agent` uses for worktrees).
#[derive(Debug, Default, Clone, Copy)]
pub struct NativeSecurityGitAdapter;

impl SecurityGitAdapter for NativeSecurityGitAdapter {
	fn root(&self, cwd: &Path) -> Result<Option<PathBuf>, String> {
		match git_text(cwd, &["rev-parse", "--show-toplevel"]) {
			Ok(text) if !text.trim().is_empty() => Ok(Some(PathBuf::from(text.trim()))),
			Ok(_) => Ok(None),
			Err(_) => Ok(None),
		}
	}

	fn head_sha(&self, cwd: &Path) -> Result<Option<String>, String> {
		match git_text(cwd, &["rev-parse", "--verify", "HEAD"]) {
			Ok(text) => Ok(Some(text.trim().to_string()).filter(|value| !value.is_empty())),
			Err(_) => Ok(None),
		}
	}

	fn resolve_ref(&self, cwd: &Path, ref_name: &str) -> Result<Option<String>, String> {
		match git_text(cwd, &["rev-parse", "--verify", ref_name]) {
			Ok(text) => Ok(Some(text.trim().to_string()).filter(|value| !value.is_empty())),
			Err(_) => Ok(None),
		}
	}

	fn diff_tree(&self, cwd: &Path, base: &str, head: &str) -> Result<String, String> {
		git_text(cwd, &["diff-tree", "-p", "--no-ext-diff", base, head])
	}

	fn status(&self, cwd: &Path) -> Result<String, String> {
		git_text(cwd, &["status", "--porcelain", "--untracked-files=all"])
	}

	fn files(&self, cwd: &Path) -> Result<Vec<String>, String> {
		Ok(git_text(cwd, &["ls-files"])?.lines().map(str::to_string).collect())
	}

	fn untracked(&self, cwd: &Path) -> Result<Vec<String>, String> {
		Ok(git_text(cwd, &["ls-files", "--others", "--exclude-standard"])?
			.lines()
			.map(str::to_string)
			.collect())
	}

	fn worktree_add(&self, cwd: &Path, path: &Path, reference: &str) -> Result<(), String> {
		git_text(
			cwd,
			&[
				"worktree",
				"add",
				"--detach",
				path.to_string_lossy().as_ref(),
				reference,
			],
		)
		.map(|_| ())
	}

	fn worktree_remove(&self, cwd: &Path, path: &Path) -> Result<bool, String> {
		match git_text(
			cwd,
			&["worktree", "remove", "--force", path.to_string_lossy().as_ref()],
		) {
			Ok(_) => Ok(true),
			Err(error) if error.contains("is not a working tree") => Ok(false),
			Err(_) => Ok(false),
		}
	}
}

fn git_text(cwd: &Path, args: &[&str]) -> Result<String, String> {
	let output = Command::new("git")
		.arg("-C")
		.arg(cwd)
		.args(args)
		.output()
		.map_err(|error| format!("cannot run git in {}: {error}", cwd.display()))?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
		return Err(if stderr.is_empty() {
			format!("git {} failed with {}", args.join(" "), output.status)
		} else {
			stderr
		});
	}
	Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Input for plan creation.
pub struct SecurityPlanRequest<'a> {
	pub cwd: &'a Path,
	pub target: SecurityTargetRequest,
	pub knowledge_base_paths: &'a [String],
	pub output_root: &'a Path,
	pub archive_existing: bool,
	pub model: SecurityModelRef,
	pub account: SecurityAccountRef,
	pub config: serde_json::Value,
	pub workflow_fingerprint: &'a str,
	pub created_at: &'a str,
}

/// Typed stale-plan failure; the message is the user-facing contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleSecurityScanPlanError {
	pub expected: String,
	pub actual: String,
}

impl std::fmt::Display for StaleSecurityScanPlanError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"Security scan plan is stale: expected {}, got {}. Run security preflight again.",
			self.expected, self.actual
		)
	}
}

impl std::error::Error for StaleSecurityScanPlanError {}

fn path_is_within(candidate: &Path, root: &Path) -> bool {
	candidate == root || candidate.starts_with(root)
}

fn normalize_relative_path(input: &str) -> Result<String, String> {
	if input.contains('\0') {
		return Err(format!("Security scope path contains a null byte: {input}"));
	}
	let slashed = input.replace('\\', "/");
	if slashed.is_empty() {
		return Ok(String::new());
	}
	let raw_segments: Vec<&str> = slashed.split('/').collect();
	let mut parts: Vec<&str> = Vec::new();
	for segment in &raw_segments {
		match *segment {
			"" | "." => {},
			".." => {
				return Err(format!("Security scope path must be repository-relative: {input}"));
			},
			other => parts.push(other),
		}
	}
	if raw_segments.first() == Some(&"..") || slashed.starts_with('/') {
		return Err(format!("Security scope path must be repository-relative: {input}"));
	}
	let normalized = parts.join("/");
	if normalized.is_empty() {
		return Ok(".".to_string());
	}
	if normalized.starts_with("../") || normalized == ".." {
		return Err(format!("Security scope path must be repository-relative: {input}"));
	}
	if slashed.len() >= 2 && slashed.as_bytes()[1] == b':' {
		return Err(format!("Security scope path must be repository-relative: {input}"));
	}
	Ok(normalized)
}

fn normalize_scope_paths(values: &[String]) -> Result<Vec<String>, String> {
	let mut out: Vec<String> = Vec::new();
	for value in values {
		let normalized = normalize_relative_path(value)?;
		if !out.contains(&normalized) {
			out.push(normalized);
		}
	}
	out.sort();
	Ok(out)
}

fn scope_contains_path(candidate: &str, normalized_path: &str) -> bool {
	candidate.is_empty()
		|| candidate == "."
		|| normalized_path == candidate
		|| normalized_path.starts_with(&format!("{candidate}/"))
}

#[must_use]
pub fn path_matches_security_scope(
	relative_path: &str,
	include_paths: &[String],
	exclude_paths: &[String],
) -> bool {
	let Ok(normalized) = normalize_relative_path(relative_path) else {
		return false;
	};
	let included = include_paths.is_empty()
		|| include_paths
			.iter()
			.any(|candidate| scope_contains_path(candidate, &normalized));
	let excluded = exclude_paths
		.iter()
		.any(|candidate| scope_contains_path(candidate, &normalized));
	included && !excluded
}

fn validate_scope_paths(repository_root: &Path, paths: &[String]) -> Result<(), String> {
	for relative in paths {
		if relative.is_empty() {
			continue;
		}
		let absolute = repository_root.join(relative);
		if !path_is_within(&absolute, repository_root) {
			return Err(format!("Security scope escapes repository: {relative}"));
		}
		let canonical = fs::canonicalize(&absolute)
			.map_err(|_| format!("Security scope path does not exist: {relative}"))?;
		if !path_is_within(&canonical, repository_root) {
			return Err(format!("Security scope resolves outside repository: {relative}"));
		}
	}
	Ok(())
}

fn digest_working_tree(
	repository_root: &Path,
	include_paths: &[String],
	exclude_paths: &[String],
	adapter: &dyn SecurityGitAdapter,
) -> Result<String, String> {
	let mut files: Vec<String> = adapter
		.files(repository_root)?
		.into_iter()
		.chain(adapter.untracked(repository_root)?)
		.filter_map(|path| normalize_relative_path(&path).ok())
		.filter(|path| path_matches_security_scope(path, include_paths, exclude_paths))
		.collect();
	files.sort();
	files.dedup();
	let mut hasher = Sha256::new();
	for relative in files {
		let absolute = repository_root.join(&relative);
		if !path_is_within(&absolute, repository_root) {
			return Err(format!("Git path escapes repository: {relative}"));
		}
		let metadata = fs::symlink_metadata(&absolute).ok();
		hasher.update(relative.as_bytes());
		hasher.update(b"\0");
		let Some(metadata) = metadata else {
			hasher.update(b"missing\0");
			continue;
		};
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt as _;
			hasher.update(format!("mode:{}\0", metadata.permissions().mode() & 0o111).as_bytes());
		}
		#[cfg(not(unix))]
		hasher.update(b"mode:0\0");
		if metadata.file_type().is_symlink() {
			hasher.update(b"symlink\0");
			let target = fs::read_link(&absolute).map_err(|error| {
				format!("cannot read symlink {}: {error}", absolute.display())
			})?;
			hasher.update(target.to_string_lossy().as_bytes());
		} else if metadata.is_file() {
			let bytes = fs::read(&absolute)
				.map_err(|error| format!("cannot read {}: {error}", absolute.display()))?;
			hasher.update(bytes);
		} else {
			hasher.update(b"unsupported\0");
		}
		hasher.update(b"\0");
	}
	let head = adapter.head_sha(repository_root)?.unwrap_or_else(|| "unborn".to_string());
	hasher.update(head.as_bytes());
	Ok(format!("omp-security-tree/v1:sha256:{}", hex(&hasher.finalize())))
}

fn hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		out.push_str(&format!("{byte:02x}"));
	}
	out
}

fn normalize_target(
	repository_root: &Path,
	request: &SecurityTargetRequest,
	adapter: &dyn SecurityGitAdapter,
) -> Result<SecurityTarget, String> {
	let (kind, include_paths, exclude_paths) = request.parts();
	if kind == "scoped_path" && !include_paths.iter().any(|value| !value.trim().is_empty()) {
		return Err("scoped_path security scans require at least one include path".to_string());
	}
	let include_paths = normalize_scope_paths(include_paths)?;
	let exclude_paths = normalize_scope_paths(exclude_paths)?;
	validate_scope_paths(repository_root, &include_paths)?;
	validate_scope_paths(repository_root, &exclude_paths)?;
	let display_name = repository_root
		.file_name()
		.map(|name| name.to_string_lossy().to_string())
		.unwrap_or_else(|| repository_root.to_string_lossy().to_string());
	if let SecurityTargetRequest::RefDiff { base_revision, head_revision, .. } = request {
		let base = adapter
			.resolve_ref(repository_root, base_revision)?
			.ok_or_else(|| format!("Unknown security scan base revision: {base_revision}"))?;
		let head = adapter
			.resolve_ref(repository_root, head_revision)?
			.ok_or_else(|| format!("Unknown security scan head revision: {head_revision}"))?;
		let raw_diff = adapter.diff_tree(repository_root, &base, &head)?;
		let value = serde_json::json!({
			"baseRevision": base,
			"headRevision": head,
			"includePaths": include_paths,
			"excludePaths": exclude_paths,
			"rawDiff": raw_diff,
		});
		return Ok(SecurityTarget {
			kind: "ref_diff".to_string(),
			repository_root: repository_root.to_string_lossy().to_string(),
			display_name,
			revision: None,
			base_revision: Some(base),
			head_revision: Some(head),
			include_paths,
			exclude_paths,
			tree_digest: format!(
				"omp-security-diff/v1:sha256:{}",
				sha256_hex(canonical_security_json(&value).as_bytes())
			),
		});
	}
	let revision = adapter.head_sha(repository_root)?;
	Ok(SecurityTarget {
		kind: kind.to_string(),
		repository_root: repository_root.to_string_lossy().to_string(),
		display_name,
		revision,
		base_revision: None,
		head_revision: None,
		include_paths: include_paths.clone(),
		exclude_paths: exclude_paths.clone(),
		tree_digest: digest_working_tree(repository_root, &include_paths, &exclude_paths, adapter)?,
	})
}

fn hash_file(path: &Path) -> Result<(String, u64), String> {
	let bytes = fs::read(path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
	let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
	Ok((sha256_hex(&bytes), size))
}

fn normalize_knowledge_bases(
	paths: &[String],
	base_directory: &Path,
) -> Result<Vec<SecurityKnowledgeBaseRef>, String> {
	let mut results = Vec::new();
	for input in paths {
		let canonical = fs::canonicalize(base_directory.join(input))
			.map_err(|error| format!("Security knowledge base is unavailable: {input}: {error}"))?;
		let metadata =
			fs::metadata(&canonical).map_err(|error| format!("cannot stat {input}: {error}"))?;
		if !metadata.is_file() {
			return Err(format!("Security knowledge base is not a file: {input}"));
		}
		let (sha256, size) = hash_file(&canonical)?;
		results.push(SecurityKnowledgeBaseRef {
			path: canonical.to_string_lossy().to_string(),
			sha256,
			size,
		});
	}
	results.sort_by(|left, right| left.path.cmp(&right.path));
	Ok(results)
}

fn normalize_output(
	repository_root: &Path,
	output_root: &Path,
	archive_existing: bool,
) -> Result<SecurityOutputPlan, String> {
	let requested = if output_root.is_absolute() {
		output_root.to_path_buf()
	} else {
		std::env::current_dir()
			.map_err(|error| format!("cannot resolve output directory: {error}"))?
			.join(output_root)
	};
	let parent = requested
		.parent()
		.ok_or_else(|| "Security output directory has no parent".to_string())?;
	let canonical_parent = fs::canonicalize(parent).map_err(|error| {
		format!(
			"Security output directory parent is unavailable: {}: {error}",
			parent.display()
		)
	})?;
	let candidate = canonical_parent.join(
		requested
			.file_name()
			.ok_or_else(|| "Security output directory has no name".to_string())?,
	);
	if path_is_within(&candidate, repository_root) {
		return Err("Security output directory must be outside the scanned repository".to_string());
	}
	let mut existing_state = "absent".to_string();
	match fs::symlink_metadata(&candidate) {
		Ok(metadata) => {
			if metadata.file_type().is_symlink() {
				return Err("Security output directory must not be a symbolic link".to_string());
			}
			if !metadata.is_dir() {
				return Err("Security output path exists and is not a directory".to_string());
			}
			let canonical = fs::canonicalize(&candidate)
				.map_err(|error| format!("cannot canonicalize output: {error}"))?;
			if canonical != candidate {
				return Err("Security output directory does not have a canonical identity".to_string());
			}
			let entries = fs::read_dir(&candidate)
				.map_err(|error| format!("cannot list output directory: {error}"))?
				.count();
			existing_state = if entries == 0 { "empty".to_string() } else { "archivable".to_string() };
			if entries > 0 && !archive_existing {
				return Err(
					"Security output directory is not empty; enable archiveExisting or choose another directory"
						.to_string(),
				);
			}
		},
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
			fs::create_dir(&candidate)
				.map_err(|error| format!("cannot create output directory: {error}"))?;
			existing_state = "empty".to_string();
		},
		Err(error) => return Err(format!("cannot inspect output directory: {error}")),
	}
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt as _;
		fs::set_permissions(&candidate, fs::Permissions::from_mode(0o700))
			.map_err(|error| format!("cannot chmod output directory: {error}"))?;
	}
	Ok(SecurityOutputPlan {
		root: candidate.to_string_lossy().to_string(),
		archive_existing,
		existing_state,
	})
}

struct PlanMaterial {
	repository_root: String,
	target: SecurityTarget,
	knowledge_bases: Vec<SecurityKnowledgeBaseRef>,
	output: SecurityOutputPlan,
	model: SecurityModelRef,
	account: SecurityAccountRef,
	config_fingerprint: String,
	workflow_fingerprint: String,
}

fn build_plan_material(
	request: &SecurityPlanRequest<'_>,
	adapter: &dyn SecurityGitAdapter,
) -> Result<PlanMaterial, String> {
	let repository_root = adapter
		.root(request.cwd)?
		.ok_or_else(|| format!("Security scans require a Git repository: {}", request.cwd.display()))?;
	let canonical_root =
		fs::canonicalize(&repository_root).map_err(|error| format!("cannot canonicalize repo: {error}"))?;
	let target = normalize_target(&canonical_root, &request.target, adapter)?;
	let knowledge_bases = normalize_knowledge_bases(request.knowledge_base_paths, &canonical_root)?;
	let output = normalize_output(&canonical_root, request.output_root, request.archive_existing)?;
	Ok(PlanMaterial {
		repository_root: canonical_root.to_string_lossy().to_string(),
		target,
		knowledge_bases,
		output,
		model: request.model.clone(),
		account: request.account.clone(),
		config_fingerprint: format!(
			"omp-security-config/v1:sha256:{}",
			sha256_hex(canonical_security_json(&request.config).as_bytes())
		),
		workflow_fingerprint: request.workflow_fingerprint.to_string(),
	})
}

pub fn create_security_scan_plan(
	request: &SecurityPlanRequest<'_>,
	adapter: &dyn SecurityGitAdapter,
) -> Result<SecurityScanPlan, String> {
	let material = build_plan_material(request, adapter)?;
	let value = serde_json::json!({
		"repositoryRoot": &material.repository_root,
		"target": &material.target,
		"knowledgeBases": &material.knowledge_bases,
		"output": &material.output,
		"model": &material.model,
		"account": &material.account,
		"configFingerprint": &material.config_fingerprint,
		"workflowFingerprint": &material.workflow_fingerprint,
	});
	let fingerprint = format!(
		"omp-security-plan/v1:sha256:{}",
		sha256_hex(canonical_security_json(&value).as_bytes())
	);
	Ok(SecurityScanPlan {
		document_type: "omp-security.scan-plan".to_string(),
		schema_version: "1.0".to_string(),
		id: create_security_plan_id(&fingerprint),
		created_at: request.created_at.to_string(),
		repository_root: material.repository_root,
		target: material.target,
		knowledge_bases: material.knowledge_bases,
		output: material.output,
		model: material.model,
		account: material.account,
		config_fingerprint: material.config_fingerprint,
		workflow_fingerprint: material.workflow_fingerprint,
		fingerprint,
	})
}

fn target_request_from_plan(plan: &SecurityScanPlan) -> SecurityTargetRequest {
	let include_paths = plan.target.include_paths.clone();
	let exclude_paths = plan.target.exclude_paths.clone();
	match plan.target.kind.as_str() {
		"ref_diff" => SecurityTargetRequest::RefDiff {
			base_revision: plan.target.base_revision.clone().unwrap_or_default(),
			head_revision: plan.target.head_revision.clone().unwrap_or_default(),
			include_paths,
			exclude_paths,
		},
		"scoped_path" => SecurityTargetRequest::ScopedPath { include_paths, exclude_paths },
		"working_tree" => SecurityTargetRequest::WorkingTree { include_paths, exclude_paths },
		_ => SecurityTargetRequest::Repository { include_paths, exclude_paths },
	}
}

/// Recompute the plan fingerprint against the current tree/config/workflow.
pub fn assert_security_scan_plan_fresh(
	plan: &SecurityScanPlan,
	config: &serde_json::Value,
	workflow_fingerprint: &str,
	adapter: &dyn SecurityGitAdapter,
) -> Result<(), Box<StaleSecurityScanPlanError>> {
	let knowledge_base_paths: Vec<String> = plan
		.knowledge_bases
		.iter()
		.map(|item| item.path.clone())
		.collect();
	let request = SecurityPlanRequest {
		cwd: Path::new(&plan.repository_root),
		target: target_request_from_plan(plan),
		knowledge_base_paths: &knowledge_base_paths,
		output_root: Path::new(&plan.output.root),
		archive_existing: plan.output.archive_existing,
		model: plan.model.clone(),
		account: plan.account.clone(),
		config: config.clone(),
		workflow_fingerprint,
		created_at: &plan.created_at,
	};
	let current = create_security_scan_plan(&request, adapter).map_err(|message| {
		Box::new(StaleSecurityScanPlanError {
			expected: plan.fingerprint.clone(),
			actual: format!("uncomputable: {message}"),
		})
	})?;
	if current.fingerprint != plan.fingerprint {
		return Err(Box::new(StaleSecurityScanPlanError {
			expected: plan.fingerprint.clone(),
			actual: current.fingerprint,
		}));
	}
	Ok(())
}

/// Result of preparing the user-visible output directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSecurityOutput {
	pub root: String,
	pub archived_to: Option<String>,
}

/// Rotate a non-empty output directory aside when the plan allows archiving.
pub fn prepare_security_output_directory(
	output: &SecurityOutputPlan,
	archive_suffix: &str,
) -> Result<PreparedSecurityOutput, String> {
	let root = PathBuf::from(&output.root);
	let metadata =
		fs::symlink_metadata(&root).map_err(|error| format!("output directory unavailable: {error}"))?;
	if metadata.file_type().is_symlink() {
		return Err("Security output directory must not be a symbolic link".to_string());
	}
	if !metadata.is_dir() {
		return Err("Security output path exists and is not a directory".to_string());
	}
	let canonical = fs::canonicalize(&root).map_err(|error| format!("cannot canonicalize output: {error}"))?;
	if canonical != root {
		return Err("Security output directory does not have a canonical identity".to_string());
	}
	let entries = fs::read_dir(&root)
		.map_err(|error| format!("cannot list output directory: {error}"))?
		.count();
	let mut archived_to = None;
	if entries > 0 {
		if !output.archive_existing {
			return Err(
				"Security output directory is not empty; enable archiveExisting or choose another directory"
					.to_string(),
			);
		}
		let safe_suffix: String = archive_suffix
			.chars()
			.map(|ch| if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') { ch } else { '-' })
			.collect();
		let archive = root.with_file_name(format!(
			"{}.archive-{safe_suffix}",
			root.file_name().and_then(|name| name.to_str()).unwrap_or("security-output")
		));
		fs::rename(&root, &archive)
			.map_err(|error| format!("cannot archive output directory: {error}"))?;
		fs::create_dir(&root).map_err(|error| format!("cannot recreate output directory: {error}"))?;
		archived_to = Some(archive.to_string_lossy().to_string());
	}
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt as _;
		fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
			.map_err(|error| format!("cannot chmod output directory: {error}"))?;
	}
	Ok(PreparedSecurityOutput { root: root.to_string_lossy().to_string(), archived_to })
}

/// Workflow fingerprint inputs are the exact prompt texts the coordinator
/// ships, in coordinator order (`coordinator.ts` passes the four prompt
/// strings). Editing a prompt must invalidate stored plans, so the contents —
/// not the file names — are hashed.
pub const WORKFLOW_PROMPT_INPUTS: [&str; 4] = [
	include_str!("../../../../packages/coding-agent/src/prompts/security/scan-coordinator.md"),
	include_str!("../../../../packages/coding-agent/src/prompts/security/scan-request.md"),
	include_str!("../../../../packages/coding-agent/src/prompts/agents/security-reviewer.md"),
	include_str!("../../../../packages/coding-agent/src/prompts/tools/security-publish.md"),
];

#[must_use]
pub fn security_workflow_fingerprint() -> String {
	provenance::create_security_workflow_fingerprint(&WORKFLOW_PROMPT_INPUTS)
}
