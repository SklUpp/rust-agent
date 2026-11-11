//! Project-scoped, crash-safe security store.
//!
//! Anchors: `packages/coding-agent/src/security/store.ts`.
//!
//! Layout (`~/.omp/security/<project-key>` unless a state root is injected):
//! - `index.json` lists plan/scan ids; guarded by an in-process mutex plus a
//!   cross-process `flock`;
//! - `plans/<secplan_id>.json`;
//! - `scans/<secscan_id>/{findings.json,report.md,results.sarif,scan.json}`;
//! - `targets/<secscan_id>` is the ref-diff worktree owned by a run.
//!
//! `scan.json` is written LAST and is the commit marker: a reader that sees it
//! can rely on findings and optional artifacts already being present. Every
//! file is written through a `*.tmp` sibling and renamed; directories are 0700
//! and files 0600.

use std::{
	collections::HashMap,
	fs::{self, File, OpenOptions},
	io::Write as _,
	path::{Path, PathBuf},
	sync::{Arc, LazyLock, Mutex},
};

use super::{
	ids::{encode_security_project_key, sha256_hex},
	preflight::{NativeSecurityGitAdapter, SecurityGitAdapter as _},
	sarif::export_security_bundle_to_sarif,
	types::{
		self, SecurityDisposition, SecurityEvidence, SecurityFinding, SecurityScan, SecurityScanBundle,
		SecurityScanPlan, SecurityValidation,
	},
};

const STORE_SCHEMA_VERSION: u32 = 1;
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;

static STORE_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
	LazyLock::new(|| Mutex::new(HashMap::new()));

fn store_lock(project_directory: &Path) -> Arc<Mutex<()>> {
	let mut locks = STORE_LOCKS
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner);
	Arc::clone(
		locks
			.entry(project_directory.to_path_buf())
			.or_insert_with(|| Arc::new(Mutex::new(()))),
	)
}

/// Cross-process advisory lock on `<project>/index.lock`.
///
/// The descriptor stays open for the guard's lifetime; the kernel releases the
/// lock if the process dies, so a crash cannot strand the store.
struct StoreFileLock {
	_file: File,
}

impl StoreFileLock {
	fn acquire(path: &Path) -> crate::Result<Self> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(path)
			.map_err(|error| {
				crate::Error::new(format!("cannot open security store lock {}: {error}", path.display()))
			})?;
		#[cfg(unix)]
		{
			use std::os::unix::io::AsRawFd as _;
			let status = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
			if status != 0 {
				return Err(crate::Error::new(format!(
					"cannot lock security store {}: {}",
					path.display(),
					std::io::Error::last_os_error()
				)));
			}
		}
		Ok(Self { _file: file })
	}
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SecurityStoreIndex {
	schema_version: u32,
	project_key: String,
	repository_root: String,
	scan_ids: Vec<String>,
	plan_ids: Vec<String>,
	updated_at: String,
}

/// `listScans` projection; findings are counted from the bundle.
#[derive(Debug, Clone, PartialEq)]
pub struct SecurityScanSummary {
	pub id: String,
	pub status: String,
	pub created_at: String,
	pub completed_at: Option<String>,
	pub producer: types::SecurityProducer,
	pub finding_count: usize,
	pub target: types::SecurityTarget,
}

/// One project's store handle. Cheap to clone conceptually but kept unique so
/// writers serialize on `project_directory`.
#[derive(Debug)]
pub struct SecurityStore {
	repository_root: String,
	project_key: String,
	project_directory: PathBuf,
}

/// `openForCwd` root resolution: the enclosing git worktree root when `cwd` is
/// inside one, else `cwd` itself, canonicalized.
///
/// Resolution lives here rather than at the call site so no caller can pass a
/// subdirectory and silently split a repository into two project stores.
fn resolve_repository_root(cwd: &Path) -> PathBuf {
	let resolved = fs::canonicalize(cwd).unwrap_or_else(|_| cwd.to_path_buf());
	match NativeSecurityGitAdapter.root(&resolved).ok().flatten() {
		Some(root) => fs::canonicalize(&root).unwrap_or(root),
		None => resolved,
	}
}

impl SecurityStore {
	/// Open (or create) the project store for `repository_root` or, when it is a
	/// directory inside a worktree, for that worktree's root (TS `openForCwd`).
	/// A non-repository path keeps its own store; preflight is what later
	/// requires a Git repository.
	pub fn open(repository_root: &Path, state_root: Option<&Path>) -> crate::Result<Self> {
		let canonical = resolve_repository_root(repository_root);
		let canonical_text = canonical.to_string_lossy().to_string();
		let project_key = encode_security_project_key(&canonical_text);
		let project_directory = match state_root {
			Some(root) => root.join(&project_key),
			None => crate::dirs::root_subdir("security", crate::dirs::AgentDirCategory::State)
				.join(&project_key),
		};
		ensure_private_directory(&project_directory)?;
		let store = Self {
			repository_root: canonical_text,
			project_key,
			project_directory,
		};
		store.with_write(|| store.ensure_index())?;
		Ok(store)
	}

	/// Alias kept for callers that already speak in terms of a cwd. It performs
	/// the exact same root resolution as [`Self::open`]; there is no bypass for
	/// an "already resolved" path.
	pub fn open_for_root(cwd: &Path, state_root: Option<&Path>) -> crate::Result<Self> {
		Self::open(cwd, state_root)
	}

	#[must_use]
	pub fn repository_root(&self) -> &str {
		&self.repository_root
	}

	#[must_use]
	pub fn project_key(&self) -> &str {
		&self.project_key
	}

	#[must_use]
	pub fn project_directory(&self) -> &Path {
		&self.project_directory
	}

	#[must_use]
	pub fn work_directory(&self) -> PathBuf {
		self.project_directory.join("work")
	}

	#[must_use]
	pub fn targets_directory(&self) -> PathBuf {
		self.project_directory.join("targets")
	}

	#[must_use]
	pub fn sessions_directory(&self) -> PathBuf {
		self.project_directory.join("sessions")
	}

	fn scan_directory(&self, scan_id: &str) -> crate::Result<PathBuf> {
		if !types::is_scan_id(scan_id) {
			return Err(crate::Error::new(format!("Invalid security scan id: {scan_id}")));
		}
		Ok(self.project_directory.join("scans").join(scan_id))
	}

	fn plan_path(&self, plan_id: &str) -> crate::Result<PathBuf> {
		if !types::is_scan_plan_id(plan_id) {
			return Err(crate::Error::new(format!("Invalid security plan id: {plan_id}")));
		}
		Ok(self.project_directory.join("plans").join(format!("{plan_id}.json")))
	}

	fn index_path(&self) -> PathBuf {
		self.project_directory.join("index.json")
	}

	/// Run one read-modify-write transaction under both locks. The in-process
	/// mutex prevents same-process writers from stacking `flock` calls (which
	/// would deadlock a non-reentrant advisory lock held from two threads).
	pub(crate) fn with_write<T>(&self, operation: impl FnOnce() -> crate::Result<T>) -> crate::Result<T> {
		let local = store_lock(&self.project_directory);
		let _local = local.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
		let _file = StoreFileLock::acquire(&self.project_directory.join("index.lock"))?;
		operation()
	}

	fn ensure_index(&self) -> crate::Result<()> {
		if self.index_path().is_file() {
			self.read_index()?;
			return Ok(());
		}
		self.write_index(&SecurityStoreIndex {
			schema_version: STORE_SCHEMA_VERSION,
			project_key: self.project_key.clone(),
			repository_root: self.repository_root.clone(),
			scan_ids: Vec::new(),
			plan_ids: Vec::new(),
			updated_at: crate::output::iso8601(std::time::SystemTime::now()),
		})
	}

	fn read_index(&self) -> crate::Result<SecurityStoreIndex> {
		let path = self.index_path();
		let bytes = fs::read(&path).map_err(|error| {
			crate::Error::new(format!("cannot read security store index {}: {error}", path.display()))
		})?;
		let index: SecurityStoreIndex = types::parse_document(&bytes, "Security store index")?;
		if index.schema_version != STORE_SCHEMA_VERSION || index.project_key != self.project_key {
			return Err(crate::Error::new(format!(
				"Unsupported security store index at {}",
				path.display()
			)));
		}
		if index.scan_ids.iter().any(|id| !types::is_scan_id(id)) {
			return Err(crate::Error::new(format!(
				"Invalid security store scan index at {}",
				path.display()
			)));
		}
		if index.plan_ids.iter().any(|id| !types::is_scan_plan_id(id)) {
			return Err(crate::Error::new(format!(
				"Invalid security store plan index at {}",
				path.display()
			)));
		}
		Ok(index)
	}

	fn write_index(&self, index: &SecurityStoreIndex) -> crate::Result<()> {
		let text = serde_json::to_string_pretty(index)
			.map_err(|error| crate::Error::new(format!("cannot encode security index: {error}")))?;
		write_security_file_atomic(&self.index_path(), &format!("{text}\n"))
	}

	pub fn put_plan(&self, plan: &SecurityScanPlan) -> crate::Result<()> {
		self.with_write(|| {
			if plan.repository_root != self.repository_root {
				return Err(crate::Error::new(format!(
					"Security plan repository {} does not match {}",
					plan.repository_root, self.repository_root
				)));
			}
			let text = serde_json::to_string_pretty(plan)
				.map_err(|error| crate::Error::new(format!("cannot encode security plan: {error}")))?;
			write_security_file_atomic(&self.plan_path(&plan.id)?, &format!("{text}\n"))?;
			let mut index = self.read_index()?;
			if !index.plan_ids.iter().any(|id| id == &plan.id) {
				index.plan_ids.push(plan.id.clone());
			}
			index.updated_at = crate::output::iso8601(std::time::SystemTime::now());
			self.write_index(&index)
		})
	}

	pub fn get_plan(&self, plan_id: &str) -> crate::Result<Option<SecurityScanPlan>> {
		let path = self.plan_path(plan_id)?;
		let bytes = match fs::read(&path) {
			Ok(bytes) => bytes,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(error) => {
				return Err(crate::Error::new(format!(
					"cannot read security plan {}: {error}",
					path.display()
				)))
			},
		};
		types::parse_plan(&bytes).map(Some)
	}

	pub fn list_plans(&self) -> crate::Result<Vec<SecurityScanPlan>> {
		let index = self.read_index()?;
		let mut plans = Vec::new();
		for plan_id in index.plan_ids.iter().rev() {
			if let Some(plan) = self.get_plan(plan_id)? {
				plans.push(plan);
			}
		}
		Ok(plans)
	}

	fn put_bundle_unlocked(&self, bundle: &SecurityScanBundle) -> crate::Result<()> {
		types::validate_bundle(bundle)?;
		if bundle.scan.project_key != self.project_key {
			return Err(crate::Error::new(format!(
				"Security scan project key {} does not match {}",
				bundle.scan.project_key, self.project_key
			)));
		}
		let scan_directory = self.scan_directory(&bundle.scan.id)?;
		ensure_private_directory(&scan_directory)?;
		let findings = serde_json::to_string_pretty(&bundle.findings)
			.map_err(|error| crate::Error::new(format!("cannot encode findings: {error}")))?;
		write_security_file_atomic(&scan_directory.join("findings.json"), &format!("{findings}\n"))?;
		write_optional(&scan_directory.join("report.md"), bundle.report.as_deref())?;
		match bundle.sarif.as_ref() {
			Some(sarif) => {
				let text = serde_json::to_string_pretty(sarif)
					.map_err(|error| crate::Error::new(format!("cannot encode SARIF: {error}")))?;
				write_security_file_atomic(&scan_directory.join("results.sarif"), &format!("{text}\n"))?;
			},
			None => remove_if_exists(&scan_directory.join("results.sarif"))?,
		}
		// Commit marker: readers must never observe scan.json before artifacts.
		let scan = serde_json::to_string_pretty(&bundle.scan)
			.map_err(|error| crate::Error::new(format!("cannot encode security scan: {error}")))?;
		write_security_file_atomic(&scan_directory.join("scan.json"), &format!("{scan}\n"))?;
		let mut index = self.read_index()?;
		if !index.scan_ids.iter().any(|id| id == &bundle.scan.id) {
			index.scan_ids.push(bundle.scan.id.clone());
		}
		index.updated_at = crate::output::iso8601(std::time::SystemTime::now());
		self.write_index(&index)
	}

	pub fn put_bundle(&self, bundle: &SecurityScanBundle) -> crate::Result<()> {
		self.with_write(|| self.put_bundle_unlocked(bundle))
	}

	pub fn get_scan(&self, scan_id: &str) -> crate::Result<Option<SecurityScan>> {
		let path = self.scan_directory(scan_id)?.join("scan.json");
		let bytes = match fs::read(&path) {
			Ok(bytes) => bytes,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(error) => {
				return Err(crate::Error::new(format!(
					"cannot read security scan {}: {error}",
					path.display()
				)))
			},
		};
		types::parse_scan(&bytes).map(Some)
	}

	fn get_bundle_unlocked(&self, scan_id: &str) -> crate::Result<Option<SecurityScanBundle>> {
		let Some(scan) = self.get_scan(scan_id)? else {
			return Ok(None);
		};
		let directory = self.scan_directory(scan_id)?;
		let findings_bytes = fs::read(directory.join("findings.json")).map_err(|error| {
			crate::Error::new(format!("cannot read findings for {scan_id}: {error}"))
		})?;
		let findings: Vec<SecurityFinding> = types::parse_document(&findings_bytes, "Security findings")?;
		let report = read_optional_text(&directory.join("report.md"))?;
		let sarif = match read_optional_text(&directory.join("results.sarif"))? {
			Some(text) => Some(serde_json::from_str(&text).map_err(|error| {
				crate::Error::new(format!("invalid SARIF for {scan_id}: {error}"))
			})?),
			None => None,
		};
		let bundle = SecurityScanBundle { scan, findings, report, sarif };
		types::validate_bundle(&bundle)?;
		Ok(Some(bundle))
	}

	pub fn get_bundle(&self, scan_id: &str) -> crate::Result<Option<SecurityScanBundle>> {
		self.with_write(|| self.get_bundle_unlocked(scan_id))
	}

	pub fn list_scans(&self) -> crate::Result<Vec<SecurityScanSummary>> {
		let index = self.read_index()?;
		let mut summaries = Vec::new();
		for scan_id in index.scan_ids.iter().rev() {
			let Some(bundle) = self.get_bundle(scan_id)? else {
				continue;
			};
			summaries.push(SecurityScanSummary {
				id: bundle.scan.id.clone(),
				status: bundle.scan.status.clone(),
				created_at: bundle.scan.created_at.clone(),
				completed_at: bundle.scan.completed_at.clone(),
				producer: bundle.scan.producer.clone(),
				finding_count: bundle.findings.len(),
				target: bundle.scan.target.clone(),
			});
		}
		Ok(summaries)
	}

	pub fn get_finding(&self, scan_id: &str, finding_id: &str) -> crate::Result<Option<SecurityFinding>> {
		let bundle = self.get_bundle(scan_id)?;
		Ok(bundle.and_then(|bundle| bundle.finding(finding_id).cloned()))
	}

	pub fn update_validation(
		&self,
		scan_id: &str,
		finding_id: &str,
		validation: SecurityValidation,
		evidence: &[SecurityEvidence],
	) -> crate::Result<SecurityFinding> {
		self.with_write(|| {
			let mut bundle = self
				.get_bundle_unlocked(scan_id)?
				.ok_or_else(|| crate::Error::new(format!("Unknown security scan: {scan_id}")))?;
			let index = bundle
				.findings
				.iter()
				.position(|finding| finding.id == finding_id)
				.ok_or_else(|| {
					crate::Error::new(format!("Unknown security finding: {finding_id}"))
				})?;
			let finding = bundle.findings[index].clone();
			let mut evidence_by_id: std::collections::BTreeMap<String, SecurityEvidence> =
				finding.evidence.iter().cloned().map(|item| (item.id.clone(), item)).collect();
			for item in evidence {
				evidence_by_id.insert(item.id.clone(), item.clone());
			}
			let mut canonical = SecurityValidation {
				status: validation.status,
				summary: validation.summary,
				evidence_ids: {
					let mut ids: Vec<String> = validation.evidence_ids;
					ids.sort();
					ids.dedup();
					ids
				},
				validated_at: validation.validated_at,
			};
			for evidence_id in &canonical.evidence_ids {
				if !evidence_by_id.contains_key(evidence_id) {
					return Err(crate::Error::new(format!(
						"Unknown security validation evidence: {evidence_id}"
					)));
				}
			}
			if canonical.summary.as_deref() == Some("") {
				canonical.summary = None;
			}
			let mut updated = finding;
			updated.evidence = evidence_by_id.into_values().collect();
			updated.validation = canonical;
			bundle.findings[index] = updated;
			if bundle.sarif.is_some() {
				bundle.sarif = Some(export_security_bundle_to_sarif(&bundle));
			}
			self.put_bundle_unlocked(&bundle)?;
			Ok(bundle.findings[index].clone())
		})
	}

	pub fn update_disposition(
		&self,
		scan_id: &str,
		finding_id: &str,
		disposition: SecurityDisposition,
	) -> crate::Result<SecurityFinding> {
		self.with_write(|| {
			let mut bundle = self
				.get_bundle_unlocked(scan_id)?
				.ok_or_else(|| crate::Error::new(format!("Unknown security scan: {scan_id}")))?;
			let index = bundle
				.findings
				.iter()
				.position(|finding| finding.id == finding_id)
				.ok_or_else(|| {
					crate::Error::new(format!("Unknown security finding: {finding_id}"))
				})?;
			bundle.findings[index].disposition = disposition;
			if bundle.sarif.is_some() {
				bundle.sarif = Some(export_security_bundle_to_sarif(&bundle));
			}
			self.put_bundle_unlocked(&bundle)?;
			Ok(bundle.findings[index].clone())
		})
	}

	/// `storeDigest`: hash of the index document (diagnostics only).
	pub fn store_digest(&self) -> crate::Result<String> {
		let index = self.read_index()?;
		let text = serde_json::to_string(&index)
			.map_err(|error| crate::Error::new(format!("cannot encode security index: {error}")))?;
		Ok(sha256_hex(text.as_bytes()))
	}
}

/// Write one file atomically inside a 0700 directory. The temporary file is
/// created exclusively and removed on every failure path.
pub fn write_security_file_atomic(path: &Path, content: &str) -> crate::Result<()> {
	if let Some(parent) = path.parent() {
		ensure_private_directory(parent)?;
	}
	static NEXT_TEMP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
	let nonce = NEXT_TEMP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	let temporary = path.with_file_name(format!(
		".{}.{}.{}.tmp",
		path.file_name().and_then(|name| name.to_str()).unwrap_or("security"),
		std::process::id(),
		nonce
	));
	let result = (|| -> crate::Result<()> {
		let mut options = OpenOptions::new();
		options.write(true).create_new(true);
		#[cfg(unix)]
		{
			use std::os::unix::fs::OpenOptionsExt as _;
			options.mode(PRIVATE_FILE_MODE);
		}
		let mut file = options.open(&temporary).map_err(|error| {
			crate::Error::new(format!("cannot create {}: {error}", temporary.display()))
		})?;
		file.write_all(content.as_bytes()).and_then(|()| file.sync_all()).map_err(|error| {
			crate::Error::new(format!("cannot write {}: {error}", temporary.display()))
		})?;
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt as _;
			fs::set_permissions(&temporary, fs::Permissions::from_mode(PRIVATE_FILE_MODE)).map_err(
				|error| crate::Error::new(format!("cannot chmod {}: {error}", temporary.display())),
			)?;
		}
		fs::rename(&temporary, path).map_err(|error| {
			crate::Error::new(format!(
				"cannot publish {} -> {}: {error}",
				temporary.display(),
				path.display()
			))
		})
	})();
	if result.is_err() {
		let _ = fs::remove_file(&temporary);
	}
	result
}

pub fn ensure_private_directory(directory: &Path) -> crate::Result<()> {
	fs::create_dir_all(directory).map_err(|error| {
		crate::Error::new(format!("cannot create {}: {error}", directory.display()))
	})?;
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt as _;
		fs::set_permissions(directory, fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE)).map_err(
			|error| crate::Error::new(format!("cannot chmod {}: {error}", directory.display())),
		)?;
	}
	Ok(())
}

fn write_optional(path: &Path, content: Option<&str>) -> crate::Result<()> {
	match content {
		Some(text) => write_security_file_atomic(path, text),
		None => remove_if_exists(path),
	}
}

fn remove_if_exists(path: &Path) -> crate::Result<()> {
	match fs::remove_file(path) {
		Ok(()) => Ok(()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(error) => Err(crate::Error::new(format!(
			"cannot remove {}: {error}",
			path.display()
		))),
	}
}

fn read_optional_text(path: &Path) -> crate::Result<Option<String>> {
	match fs::read_to_string(path) {
		Ok(text) => Ok(Some(text)),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
		Err(error) => Err(crate::Error::new(format!(
			"cannot read {}: {error}",
			path.display()
		))),
	}
}

/// Write a completed bundle to a user-visible output directory, using the same
/// last-writer commit marker as the store.
pub fn write_security_bundle_to_directory(directory: &Path, bundle: &SecurityScanBundle) -> crate::Result<()> {
	types::validate_bundle(bundle)?;
	ensure_private_directory(directory)?;
	let findings = serde_json::to_string_pretty(&bundle.findings)
		.map_err(|error| crate::Error::new(format!("cannot encode findings: {error}")))?;
	write_security_file_atomic(&directory.join("findings.json"), &format!("{findings}\n"))?;
	write_optional(&directory.join("report.md"), bundle.report.as_deref())?;
	match bundle.sarif.as_ref() {
		Some(sarif) => {
			let text = serde_json::to_string_pretty(sarif)
				.map_err(|error| crate::Error::new(format!("cannot encode SARIF: {error}")))?;
			write_security_file_atomic(&directory.join("results.sarif"), &format!("{text}\n"))?;
		},
		None => remove_if_exists(&directory.join("results.sarif"))?,
	}
	let provenance = super::provenance::redact_private_security_metadata(
		&serde_json::to_value(&bundle.scan.provenance)
			.map_err(|error| crate::Error::new(format!("cannot encode provenance: {error}")))?,
	);
	let provenance = serde_json::to_string_pretty(&provenance)
		.map_err(|error| crate::Error::new(format!("cannot encode provenance: {error}")))?;
	write_security_file_atomic(&directory.join("provenance.json"), &format!("{provenance}\n"))?;
	let scan = super::provenance::create_public_security_scan(&bundle.scan, false)?;
	let scan = serde_json::to_string_pretty(&scan)
		.map_err(|error| crate::Error::new(format!("cannot encode public scan: {error}")))?;
	write_security_file_atomic(&directory.join("scan.json"), &format!("{scan}\n"))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn temp_root(label: &str) -> PathBuf {
		static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
		let path = std::env::var_os("HOME")
			.map(PathBuf::from)
			.unwrap_or_else(std::env::temp_dir)
			.join(format!(
				"omp-security-store-{label}-{}-{}",
				std::process::id(),
				NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
			));
		fs::create_dir_all(&path).expect("create temp root");
		path
	}

	#[test]
	fn index_is_private_and_project_keyed() {
		let root = temp_root("index");
		let repo = root.join("repo");
		fs::create_dir_all(&repo).expect("repo");
		let store = SecurityStore::open(&repo, Some(&root.join("state"))).expect("store");
		assert!(store
			.project_key()
			.ends_with(&sha256_hex(store.repository_root().as_bytes())[..12]));
		assert!(store.index_path().is_file());
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt as _;
			let mode = fs::metadata(store.project_directory()).expect("meta").permissions().mode();
			assert_eq!(mode & 0o777, 0o700);
		}
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn unicode_roots_keep_distinct_stable_store_identity() {
		fn init_repo(path: &Path) {
			// The store resolves an enclosing worktree; make each fixture its own
			// repository so the identity under test is the path, not a parent repo.
			let status = std::process::Command::new("git")
				.arg("init")
				.arg("-q")
				.arg(path)
				.status()
				.expect("run git init");
			assert!(status.success(), "git init failed for {}", path.display());
		}

		let root = temp_root("unicode");
		let state = root.join("state");
		let left = root.join("aπππb");
		let right = root.join("aπππc");
		fs::create_dir_all(&left).expect("left");
		fs::create_dir_all(&right).expect("right");
		init_repo(&left);
		init_repo(&right);

		let left_store = SecurityStore::open(&left, Some(&state)).expect("left store");
		let right_store = SecurityStore::open(&right, Some(&state)).expect("right store");
		let (left_slug, left_hash) =
			left_store.project_key().rsplit_once('-').expect("slug and hash");
		let (right_slug, right_hash) =
			right_store.project_key().rsplit_once('-').expect("slug and hash");

		// A run of Unicode characters collapses to exactly one separator, and the
		// store's directory name is that key, so the identity is consumer-visible.
		assert!(left_slug.ends_with("a-b"), "{left_slug}");
		assert!(right_slug.ends_with("a-c"), "{right_slug}");
		assert_eq!(
			left_store.project_directory().file_name().and_then(std::ffi::OsStr::to_str),
			Some(left_store.project_key())
		);

		// Neighbouring roots that read the same stay separate: the 12 hex suffix
		// covers the raw path, not the collapsed slug.
		assert_eq!(left_hash.len(), 12);
		assert_eq!(right_hash.len(), 12);
		assert_ne!(left_hash, right_hash);
		assert_ne!(left_store.project_key(), right_store.project_key());
		assert_ne!(left_store.project_directory(), right_store.project_directory());

		// Re-opening one root reuses the same identity and directory.
		let reopened = SecurityStore::open(&left, Some(&state)).expect("reopen");
		assert_eq!(reopened.project_key(), left_store.project_key());
		assert_eq!(reopened.project_directory(), left_store.project_directory());

		let _ = fs::remove_dir_all(root);
	}
}
