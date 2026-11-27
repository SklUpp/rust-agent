//! Copy-only migration from the legacy `~/.omp` user root to `~/.aragent`.
//!
//! The migration is a one-time, destination-wins merge:
//! - nothing in `~/.omp` is modified, moved, or deleted;
//! - existing destination files always win, so a partially migrated tree can
//!   be re-merged without data loss;
//! - symlinks are copied as symlinks (never followed into external trees);
//! - `run/` (runtime locks/daemons) is always skipped at the source root;
//!   the legacy user root additionally skips transient `wt/`, `cache/`,
//!   `webcache/`, `logs/` and `natives/` trees;
//! - sockets, FIFOs, devices and `*.lock` files are skipped;
//! - private directories are created `0700`, files `0600`, and executables
//!   `0700`;
//! - `.migrated-from-omp-v1` is written atomically only when no copy error was
//!   reported. Non-fatal errors are printed to stderr and the marker stays
//!   absent so the next start retries the idempotent merge.
//!
//! The same gate also copies the legacy XDG `omp` app trees to `aragent`.
//! Explicit `PI_CONFIG_DIR`/`PI_CODING_AGENT_DIR` overrides disable migration
//! entirely and there is no dual-write: the legacy tree is only ever a read
//! source.

use std::{
	fs,
	io::{self, Write as _},
	path::{Path, PathBuf},
};

use pi_agent::state::lexical_normalize;
use pi_agent::tools::atomic_publish::publish_staged_file_io;

use crate::dirs::{self, AgentDirCategory};

/// Marker written into the new user root after a clean migration.
pub const MARKER_FILE: &str = dirs::MIGRATION_MARKER;

/// Result counters for one tree merge (used by tests and diagnostics).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MergeReport {
	pub copied_files:   usize,
	pub copied_dirs:    usize,
	pub copied_links:   usize,
	pub skipped_existing: usize,
	pub skipped_special:  usize,
	pub warnings:       Vec<String>,
}

impl MergeReport {
	#[must_use]
	pub fn is_clean(&self) -> bool {
		self.warnings.is_empty()
	}
}

/// Run the startup migration when its preconditions hold.
///
/// Never fatal: a missing legacy root, an explicit override, or an existing
/// marker is a silent no-op. Copy errors are reported on stderr.
pub fn migrate_if_needed() {
	if dirs::explicit_storage_override() {
		return;
	}
	let Some(home) = dirs::home_dir() else {
		return;
	};
	let legacy = home.join(dirs::LEGACY_CONFIG_DIR_NAME);
	let current = dirs::base_config_root();
	if !migration_pending(&legacy, &current) {
		return;
	}
	let marker = current.join(MARKER_FILE);

	eprintln!(
		"migrating legacy user state from {} to {} (copy-only; destination wins)",
		legacy.display(),
		current.display()
	);
	let mut report = copy_merge_legacy_root(&legacy, &current);
	for category in [AgentDirCategory::Data, AgentDirCategory::State, AgentDirCategory::Cache] {
		let Some(base) = category.xdg_home() else {
			continue;
		};
		let legacy_xdg = base.join(dirs::XDG_LEGACY_APP_DIR_NAME);
		if !legacy_xdg.is_dir() {
			continue;
		}
		let current_xdg = base.join(dirs::XDG_APP_DIR_NAME);
		if same_path(&legacy_xdg, &current_xdg) {
			continue;
		}
		let xdg_report = copy_merge(&legacy_xdg, &current_xdg);
		report.copied_files += xdg_report.copied_files;
		report.copied_dirs += xdg_report.copied_dirs;
		report.copied_links += xdg_report.copied_links;
		report.skipped_existing += xdg_report.skipped_existing;
		report.skipped_special += xdg_report.skipped_special;
		report.warnings.extend(xdg_report.warnings);
	}

	for warning in &report.warnings {
		eprintln!("warning: migration: {warning}");
	}
	if !report.is_clean() {
		eprintln!(
			"warning: migration incomplete ({} issue(s)); {} stays absent so the next start retries",
			report.warnings.len(),
			marker.display()
		);
		return;
	}
	if let Err(error) = write_marker(&current) {
		eprintln!("warning: cannot write migration marker {}: {error}", marker.display());
	}
}

/// Gate shared by startup and tests: the legacy root exists, it is not the
/// destination, and the marker is still absent.
#[must_use]
pub fn migration_pending(legacy: &Path, current: &Path) -> bool {
	legacy.exists() && !same_path(legacy, current) && !current.join(MARKER_FILE).exists()
}

/// Recursively merge `src` into `dst` without overwriting destination entries.
///
/// `src` is never written. Returns warnings instead of failing so one
/// unreadable file cannot abort the whole migration. Only `run/` is skipped at
/// the source root; other same-named directories are preserved.
#[must_use]
pub fn copy_merge(src: &Path, dst: &Path) -> MergeReport {
	copy_merge_with(src, dst, false)
}

/// Merge the implicit legacy user root (`~/.omp`), skipping runtime and
/// transient top-level trees that the current process recreates. Other merged
/// trees (XDG, sessions, security stores) keep those directories.
fn copy_merge_legacy_root(src: &Path, dst: &Path) -> MergeReport {
	copy_merge_with(src, dst, true)
}

fn copy_merge_with(src: &Path, dst: &Path, legacy_root: bool) -> MergeReport {
	let mut report = MergeReport::default();
	// The root itself may be a symlink to the user's data root; follow it, but
	// entries below are handled with `symlink_metadata` and never followed.
	let Ok(metadata) = fs::metadata(src) else {
		report.warnings.push(format!("cannot read {}: source is missing", src.display()));
		return report;
	};
	if !metadata.is_dir() {
		report.warnings.push(format!("cannot migrate {}: source is not a directory", src.display()));
		return report;
	}
	let src_root = fs::canonicalize(src).unwrap_or_else(|_| normalize_path(src));
	let dst_norm = normalize_path(dst);
	if dst_norm == src_root || dst_norm.starts_with(&src_root) {
		report.warnings.push(format!(
			"refusing to copy {} into itself or a nested destination {}",
			src.display(),
			dst.display()
		));
		return report;
	}
	merge_dir(src, dst, true, legacy_root, &src_root, &dst_norm, &mut report);
	report
}

fn merge_dir(
	src: &Path,
	dst: &Path,
	root: bool,
	legacy_root: bool,
	src_root: &Path,
	dst_root: &Path,
	report: &mut MergeReport,
) {
	let existed = fs::symlink_metadata(dst).is_ok();
	if let Err(error) = fs::create_dir_all(dst) {
		report.warnings.push(format!("cannot create {}: {error}", dst.display()));
		return;
	}
	if !existed
		&& let Err(error) = set_dir_mode(dst)
	{
		report.warnings.push(format!("cannot set mode on {}: {error}", dst.display()));
	}
	let entries = match fs::read_dir(src) {
		Ok(entries) => entries,
		Err(error) => {
			report.warnings.push(format!("cannot read {}: {error}", src.display()));
			return;
		},
	};
	for entry in entries {
		let entry = match entry {
			Ok(entry) => entry,
			Err(error) => {
				report.warnings.push(format!("cannot read {}: {error}", src.display()));
				continue;
			},
		};
		let name = entry.file_name();
		if is_skipped_root_dir(root, legacy_root, &name) {
			report.skipped_special += 1;
			continue;
		}
		let src_path = entry.path();
		let dst_path = dst.join(&name);
		let metadata = match fs::symlink_metadata(&src_path) {
			Ok(metadata) => metadata,
			Err(error) => {
				report.warnings.push(format!("cannot stat {}: {error}", src_path.display()));
				continue;
			},
		};
		let file_type = metadata.file_type();
		if let Ok(dst_metadata) = fs::symlink_metadata(&dst_path) {
			// Existing files (and symlinks) always win, but an existing
			// directory is merged so a partial tree still receives new files.
			if file_type.is_dir() && dst_metadata.is_dir() {
				merge_dir(&src_path, &dst_path, false, false, src_root, dst_root, report);
			} else {
				report.skipped_existing += 1;
			}
			continue;
		}
		if file_type.is_symlink() {
			let target = match fs::read_link(&src_path) {
				Ok(target) => target,
				Err(error) => {
					report.warnings.push(format!("cannot read link {}: {error}", src_path.display()));
					continue;
				},
			};
			// An absolute link into the legacy root, or a relative link that
			// escapes the destination and lands back inside it, would keep
			// sending readers to `~/.omp`. Rewrite those into the new tree;
			// links to external trees stay verbatim and are never followed.
			let target =
				internal_symlink_target(&target, &src_path, &dst_path, src_root, dst_root)
					.unwrap_or(target);
			match copy_symlink(&target, &dst_path) {
				Ok(true) => report.copied_links += 1,
				Ok(false) => report.skipped_existing += 1,
				Err(error) => report
					.warnings
					.push(format!("cannot copy link {}: {error}", src_path.display())),
			}
			continue;
		}
		if file_type.is_dir() {
			merge_dir(&src_path, &dst_path, false, false, src_root, dst_root, report);
			report.copied_dirs += 1;
			continue;
		}
		if !file_type.is_file() {
			report.skipped_special += 1;
			continue;
		}
		if is_lock_file(&name) {
			report.skipped_special += 1;
			continue;
		}
		if skip_sqlite_sidecar(&name, src) {
			report.skipped_special += 1;
			continue;
		}
		let snapshot = is_sqlite_source(&src_path);
		let copied = if snapshot {
			snapshot_sqlite(&dst_path, &src_path)
		} else {
			copy_private_file(&src_path, &dst_path)
		};
		match copied {
			Ok(true) => report.copied_files += 1,
			Ok(false) => report.skipped_existing += 1,
			Err(error) => report
				.warnings
				.push(format!("cannot copy {}: {error}", src_path.display())),
		}
	}
}

/// `run/` is runtime state and never migrated from any source root. The other
/// transient trees are skipped only for the implicit legacy user root; XDG,
/// session and security merges keep legitimate `cache/`, `logs/` etc.
fn is_skipped_root_dir(root: bool, legacy_root: bool, name: &std::ffi::OsStr) -> bool {
	if !root {
		return false;
	}
	match name.to_str() {
		Some("run") => true,
		Some("wt" | "cache" | "webcache" | "logs" | "natives") => legacy_root,
		_ => false,
	}
}

/// Rewrite a link target that points inside `src_root` to its place in the
/// destination tree. Absolute links are checked directly; relative links are
/// checked from the destination location so `../../.omp/...` escapes are
/// caught while ordinary `shared.txt` links stay verbatim.
fn internal_symlink_target(
	target: &Path,
	_src_path: &Path,
	dst_path: &Path,
	src_root: &Path,
	dst_root: &Path,
) -> Option<PathBuf> {
	if target.is_absolute() {
		let relative = normalize_path(target).strip_prefix(src_root).ok()?.to_path_buf();
		return Some(dst_root.join(relative));
	}
	let destination_target = dst_path.parent().unwrap_or(dst_root).join(target);
	let resolved = normalize_path(&destination_target);
	let relative = resolved.strip_prefix(src_root).ok()?;
	Some(dst_root.join(relative))
}

/// Absolute path whose deepest existing ancestor is canonicalized and whose
/// missing suffix is re-attached, then cleaned by [`lexical_normalize`] so
/// `..` and `.` never reach [`fs::canonicalize`] comparisons.
fn normalize_path(path: &Path) -> PathBuf {
	let absolute = if path.is_absolute() {
		path.to_path_buf()
	} else {
		std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(path)
	};
	let mut existing = absolute.clone();
	let mut suffix = Vec::new();
	while !existing.exists() {
		let Some(name) = existing.file_name().map(std::ffi::OsString::from) else {
			break;
		};
		suffix.push(name);
		if !existing.pop() {
			break;
		}
	}
	let mut normalized = fs::canonicalize(&existing).unwrap_or(existing);
	for component in suffix.into_iter().rev() {
		normalized.push(component);
	}
	lexical_normalize(&normalized)
}

/// `<name>-wal` / `<name>-shm` / `<name>-journal` belonging to a `.db` in the
/// same directory are already represented by the snapshot of that database.
fn skip_sqlite_sidecar(name: &std::ffi::OsStr, src_dir: &Path) -> bool {
	let Some(base) = sqlite_sidecar_base(name) else {
		return false;
	};
	let base = Path::new(&base);
	base.extension().and_then(|extension| extension.to_str()) == Some("db")
		&& src_dir.join(base).is_file()
}

fn sqlite_sidecar_base(name: &std::ffi::OsStr) -> Option<std::ffi::OsString> {
	let name = name.to_string_lossy();
	for suffix in ["-wal", "-shm", "-journal"] {
		if let Some(base) = name.strip_suffix(suffix) {
			return Some(std::ffi::OsString::from(base));
		}
	}
	None
}

/// Snapshot a legacy SQLite database without ever writing to it: committed
/// WAL rows are included by `VACUUM INTO`, and the destination never sees a
/// half-copied file or the source's SQLite sidecars (`-wal`, `-shm`,
/// `-journal`).
fn is_sqlite_source(path: &Path) -> bool {
	if path.extension().and_then(|extension| extension.to_str()) != Some("db") {
		return false;
	}
	if has_sqlite_header(path) {
		return true;
	}
	let mut sidecar = path.as_os_str().to_os_string();
	for suffix in ["-wal", "-shm", "-journal"] {
		sidecar.push(suffix);
		if Path::new(&sidecar).is_file() {
			return true;
		}
		sidecar = path.as_os_str().to_os_string();
	}
	false
}

fn has_sqlite_header(path: &Path) -> bool {
	let Ok(mut file) = fs::File::open(path) else {
		return false;
	};
	let mut header = [0_u8; 16];
	io::Read::read_exact(&mut file, &mut header).is_ok() && &header == b"SQLite format 3\0"
}

fn is_lock_file(name: &std::ffi::OsStr) -> bool {
	let name = name.to_string_lossy();
	name.ends_with(".lock") || name.ends_with(".lock.guard")
}

fn same_path(left: &Path, right: &Path) -> bool {
	if left == right {
		return true;
	}
	match (fs::canonicalize(left), fs::canonicalize(right)) {
		(Ok(left), Ok(right)) => left == right,
		_ => false,
	}
}

fn write_marker(root: &Path) -> io::Result<()> {
	let existed = root.exists();
	fs::create_dir_all(root)?;
	if !existed {
		set_dir_mode(root)?;
	}
	let marker = root.join(MARKER_FILE);
	let temporary = root.join(format!(".{MARKER_FILE}.{}.tmp", std::process::id()));
	let mut file = fs::File::create(&temporary)?;
	file.write_all(b"version=1\nsource=.omp\n")?;
	file.sync_all()?;
	drop(file);
	set_file_mode(&temporary, 0o600)?;
	if let Err(error) = fs::rename(&temporary, &marker) {
		let _ = fs::remove_file(&temporary);
		return Err(error);
	}
	set_file_mode(&marker, 0o600)
}

fn next_copy_path(parent: &Path) -> PathBuf {
	static COPY_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
	let sequence = COPY_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	parent.join(format!(".aragent-copy.{}.{sequence}", std::process::id()))
}

/// Create `parent` when missing and make a newly created leaf private.
fn create_private_parent(parent: &Path) -> io::Result<()> {
	let existed = parent.exists();
	fs::create_dir_all(parent)?;
	if !existed {
		set_dir_mode(parent)?;
	}
	Ok(())
}

/// Drop a private staging copy after publication.
///
/// The publication primitive already removes it on success, so `NotFound` is
/// the normal case; any other removal failure is surfaced instead of silently
/// leaving a private copy behind while migration reports a clean pass.
fn remove_staged(path: &Path) -> io::Result<bool> {
	match fs::remove_file(path) {
		Ok(()) => Ok(true),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
		Err(error) => Err(error),
	}
}

fn copy_private_file(src: &Path, dst: &Path) -> io::Result<bool> {
	let parent = dst.parent().unwrap_or_else(|| Path::new("."));
	create_private_parent(parent)?;
	let temporary = next_copy_path(parent);
	let mode = file_mode_for(src)?;
	// Copy to a sibling temp then publish it, so a crash never leaves a
	// partial destination that destination-wins would treat as authoritative.
	let result = fs::copy(src, &temporary)
		.and_then(|_| set_file_mode(&temporary, mode))
		.and_then(|()| publish_staged_file_io(&temporary, dst));
	// The shared primitive keeps the staged file on a lost race, so clean it up
	// on anything short of a published destination.
	match result {
		Ok(true) => remove_staged(&temporary),
		other => {
			let _ = fs::remove_file(&temporary);
			other
		},
	}
}

#[cfg(unix)]
fn file_mode_for(src: &Path) -> io::Result<u32> {
	use std::os::unix::fs::PermissionsExt as _;
	let mode = fs::metadata(src)?.permissions().mode();
	Ok(if mode & 0o111 == 0 { 0o600 } else { 0o700 })
}

#[cfg(not(unix))]
fn file_mode_for(_src: &Path) -> io::Result<u32> {
	Ok(0o600)
}

#[cfg(unix)]
fn copy_symlink(target: &Path, dst: &Path) -> io::Result<bool> {
	match std::os::unix::fs::symlink(target, dst) {
		Ok(()) => Ok(true),
		Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
		Err(error) => Err(error),
	}
}

#[cfg(not(unix))]
fn copy_symlink(_target: &Path, _dst: &Path) -> io::Result<bool> {
	Err(io::Error::new(io::ErrorKind::Unsupported, "symlinks are not supported on this platform"))
}

#[cfg(unix)]
fn set_dir_mode(path: &Path) -> io::Result<()> {
	use std::os::unix::fs::PermissionsExt as _;
	let metadata = fs::symlink_metadata(path)?;
	if !metadata.file_type().is_symlink() {
		fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
	}
	Ok(())
}

#[cfg(not(unix))]
fn set_dir_mode(_path: &Path) -> io::Result<()> {
	Ok(())
}

#[cfg(unix)]
fn set_file_mode(path: &Path, mode: u32) -> io::Result<()> {
	use std::os::unix::fs::PermissionsExt as _;
	if fs::symlink_metadata(path)?.is_file() {
		fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
	}
	Ok(())
}

#[cfg(not(unix))]
fn set_file_mode(_path: &Path, _mode: u32) -> io::Result<()> {
	Ok(())
}

/// Marker path for a user root (tests and doctor).
#[must_use]
pub fn marker_path(root: &Path) -> PathBuf {
	root.join(MARKER_FILE)
}

/// Snapshot a legacy SQLite database (its committed WAL content included) into
/// `dst` when `dst` is absent. The legacy database and sidecars are only ever
/// opened read-only; no `-wal`/`-shm`/`-journal` files are published.
///
/// # Errors
///
/// Fails when the destination directory cannot be created, the snapshot fails,
/// or the destination cannot be published.
pub fn adopt_legacy_sqlite(dst: &Path, legacy: &Path) -> io::Result<bool> {
	if fs::symlink_metadata(dst).is_ok() || !legacy.is_file() || same_path(dst, legacy) {
		return Ok(false);
	}
	snapshot_sqlite(dst, legacy)
}

/// Snapshot `legacy` with `VACUUM INTO` and publish it no-clobber.
fn snapshot_sqlite(dst: &Path, legacy: &Path) -> io::Result<bool> {
	if fs::symlink_metadata(dst).is_ok() || same_path(dst, legacy) {
		return Ok(false);
	}
	let parent = dst.parent().unwrap_or_else(|| Path::new("."));
	create_private_parent(parent)?;
	let temporary = next_copy_path(parent);
	let _ = fs::remove_file(&temporary);
	let snapshot = (|| -> io::Result<()> {
		let connection = rusqlite::Connection::open_with_flags(
			legacy,
			rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
		)
		.map_err(|error| io::Error::other(error.to_string()))?;
		connection
			.busy_timeout(std::time::Duration::from_millis(500))
			.map_err(|error| io::Error::other(error.to_string()))?;
		connection
			.execute(
				"VACUUM INTO ?1",
				rusqlite::params![temporary.to_string_lossy().to_string()],
			)
			.map_err(|error| io::Error::other(error.to_string()))?;
		drop(connection);
		set_file_mode(&temporary, 0o600)
	})();
	if let Err(error) = snapshot {
		let _ = fs::remove_file(&temporary);
		return Err(error);
	}
	let published = publish_staged_file_io(&temporary, dst);
	match published {
		Ok(true) => remove_staged(&temporary),
		other => {
			let _ = fs::remove_file(&temporary);
			other
		},
	}
}

/// Copy one legacy file into `dst` when `dst` is absent.
///
/// # Errors
///
/// Fails when the destination cannot be written.
pub fn adopt_legacy_file(dst: &Path, legacy: &Path) -> io::Result<bool> {
	if fs::symlink_metadata(dst).is_ok() || !legacy.is_file() || same_path(dst, legacy) {
		return Ok(false);
	}
	copy_private_file(legacy, dst)
}

#[cfg(all(test, unix))]
mod tests {
	use std::{
		os::unix::fs::PermissionsExt as _,
		sync::atomic::{AtomicU32, Ordering},
	};

	use super::*;

	static NEXT: AtomicU32 = AtomicU32::new(0);

	fn temp_dir(label: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"aragent-migration-{}-{}-{label}",
			std::process::id(),
			NEXT.fetch_add(1, Ordering::Relaxed)
		));
		let _ = fs::remove_dir_all(&dir);
		fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	fn write(path: &Path, text: &str) {
		fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
		fs::write(path, text).expect("write");
	}

	#[test]
	fn copy_merge_is_copy_only_and_destination_wins() {
		let base = temp_dir("wins");
		let legacy = base.join("legacy");
		let current = base.join("current");
		write(&legacy.join("keep.txt"), "legacy");
		write(&legacy.join("nested/child.txt"), "child");
		write(&current.join("keep.txt"), "current");
		let report = copy_merge(&legacy, &current);
		assert!(report.is_clean(), "{:?}", report.warnings);
		assert_eq!(fs::read_to_string(current.join("keep.txt")).unwrap(), "current");
		assert_eq!(fs::read_to_string(current.join("nested/child.txt")).unwrap(), "child");
		assert_eq!(fs::read_to_string(legacy.join("keep.txt")).unwrap(), "legacy");
		assert_eq!(fs::read_to_string(legacy.join("nested/child.txt")).unwrap(), "child");
		assert_eq!(report.copied_files, 1);
		assert_eq!(report.skipped_existing, 1);
		assert_eq!(report.warnings, Vec::<String>::new());
	}

	#[test]
	fn copy_merge_skips_run_sockets_and_locks() {
		let base = temp_dir("skip");
		let legacy = base.join("legacy");
		let current = base.join("current");
		write(&legacy.join("run/session-writer/a.lock"), "lock");
		write(&legacy.join("gc.lock"), "lock");
		write(&legacy.join("data.txt"), "data");
		let socket = legacy.join("broker.sock");
		fs::create_dir_all(&legacy).expect("legacy");
		let _listener = std::os::unix::net::UnixListener::bind(&socket).expect("socket");
		let report = copy_merge(&legacy, &current);
		assert!(report.is_clean(), "{:?}", report.warnings);
		assert!(!current.join("run").exists(), "run/ must be skipped");
		assert!(!current.join("gc.lock").exists(), "lock files must be skipped");
		assert!(!current.join("broker.sock").exists(), "sockets must be skipped");
		assert_eq!(fs::read_to_string(current.join("data.txt")).unwrap(), "data");
		assert!(legacy.join("run/session-writer/a.lock").is_file(), "legacy run/ stays intact");
		assert!(legacy.join("broker.sock").exists(), "legacy sockets stay intact");
		assert!(report.skipped_special >= 3);
	}

	#[test]
	fn copy_merge_skips_transient_root_dirs_but_keeps_nested_ones() {
		let base = temp_dir("transient-root");
		let legacy = base.join("legacy");
		let current = base.join("current");
		let external = base.join("external");
		write(&external.join("secret.txt"), "outside");
		for name in ["run", "wt", "webcache", "logs", "natives"] {
			write(&legacy.join(name).join("state.txt"), name);
		}
		std::os::unix::fs::symlink(&external, legacy.join("cache")).expect("cache link");
		write(&legacy.join("agent/state.txt"), "agent");
		write(&legacy.join("agent/cache/cached.txt"), "nested cache");
		write(&legacy.join("agent/logs/trace.log"), "nested logs");
		write(&legacy.join("profiles/work/agent/state.txt"), "profile");

		let report = copy_merge_legacy_root(&legacy, &current);

		assert!(report.is_clean(), "{:?}", report.warnings);
		for name in ["run", "wt", "cache", "webcache", "logs", "natives"] {
			assert!(!current.join(name).exists(), "legacy top-level {name} must be skipped");
		}
		assert!(legacy.join("run/state.txt").is_file(), "legacy run/ stays intact");
		assert!(
			fs::symlink_metadata(legacy.join("cache")).unwrap().file_type().is_symlink(),
			"a skipped transient symlink is never followed or replaced"
		);
		assert_eq!(fs::read_to_string(external.join("secret.txt")).unwrap(), "outside");
		assert_eq!(fs::read_to_string(current.join("agent/state.txt")).unwrap(), "agent");
		assert_eq!(
			fs::read_to_string(current.join("agent/cache/cached.txt")).unwrap(),
			"nested cache"
		);
		assert_eq!(
			fs::read_to_string(current.join("agent/logs/trace.log")).unwrap(),
			"nested logs"
		);
		assert_eq!(
			fs::read_to_string(current.join("profiles/work/agent/state.txt")).unwrap(),
			"profile"
		);
		assert_eq!(report.skipped_special, 6);

		// `copy_merge` also migrates XDG, session-artifact and security-store
		// trees, where same-named directories are legitimate.
		let base = temp_dir("transient-generic");
		let source = base.join("source");
		let destination = base.join("destination");
		for name in ["wt", "cache", "webcache", "logs", "natives"] {
			write(&source.join(name).join("state.txt"), name);
		}
		write(&source.join("run/state.txt"), "run");
		let report = copy_merge(&source, &destination);
		assert!(report.is_clean(), "{:?}", report.warnings);
		for name in ["wt", "cache", "webcache", "logs", "natives"] {
			assert_eq!(
				fs::read_to_string(destination.join(name).join("state.txt")).unwrap(),
				name,
				"copy_merge must preserve {name}/"
			);
		}
		assert!(!destination.join("run").exists(), "copy_merge still skips root run/");
	}

	#[test]
	fn copy_merge_preserves_symlinks_without_following() {
		let base = temp_dir("links");
		let legacy = base.join("legacy");
		let current = base.join("current");
		let external = base.join("external");
		write(&external.join("secret.txt"), "outside");
		fs::create_dir_all(&legacy).expect("legacy");
		std::os::unix::fs::symlink(&external, legacy.join("outside")).expect("symlink");
		let report = copy_merge(&legacy, &current);
		assert!(report.is_clean(), "{:?}", report.warnings);
		let copied = current.join("outside");
		let metadata = fs::symlink_metadata(&copied).expect("symlink copied");
		assert!(metadata.file_type().is_symlink());
		assert_eq!(fs::read_link(&copied).unwrap(), external);
		assert_eq!(report.copied_files, 0);
		assert_eq!(report.copied_links, 1);
		assert_eq!(fs::read_to_string(external.join("secret.txt")).unwrap(), "outside");
	}

	#[test]
	fn copy_merge_rewrites_links_into_the_legacy_tree() {
		let base = temp_dir("internal-links");
		let legacy = base.join("legacy");
		let current = base.join("current");
		write(&legacy.join("agent/shared.txt"), "shared");
		std::os::unix::fs::symlink(
			legacy.join("agent/shared.txt"),
			legacy.join("agent/absolute-link"),
		)
		.expect("absolute link");
		std::os::unix::fs::symlink("shared.txt", legacy.join("agent/relative-link"))
			.expect("relative link");
		std::os::unix::fs::symlink(
			"../../legacy/agent/shared.txt",
			legacy.join("agent/escaping-link"),
		)
		.expect("escaping link");
		let report = copy_merge(&legacy, &current);
		assert!(report.is_clean(), "{:?}", report.warnings);
		let absolute_copy = current.join("agent/absolute-link");
		assert!(fs::symlink_metadata(&absolute_copy).unwrap().file_type().is_symlink());
		assert_eq!(
			fs::read_link(&absolute_copy).unwrap(),
			fs::canonicalize(&current).unwrap().join("agent/shared.txt"),
			"an absolute link must target the migrated tree"
		);
		let relative_copy = current.join("agent/relative-link");
		assert!(fs::symlink_metadata(&relative_copy).unwrap().file_type().is_symlink());
		assert_eq!(
			fs::read_link(&relative_copy).unwrap(),
			Path::new("shared.txt"),
			"a relative link keeps its link text and mirrors the layout"
		);
		assert_eq!(fs::read_to_string(relative_copy).unwrap(), "shared");
		let escaping_copy = current.join("agent/escaping-link");
		assert!(fs::symlink_metadata(&escaping_copy).unwrap().file_type().is_symlink());
		assert_eq!(
			fs::read_link(&escaping_copy).unwrap(),
			current.join("agent/shared.txt"),
			"an escaping relative link must target the migrated tree"
		);
		assert_eq!(fs::read_to_string(escaping_copy).unwrap(), "shared");
		assert_eq!(fs::read_to_string(current.join("agent/shared.txt")).unwrap(), "shared");
	}

	#[test]
	fn publish_staged_file_io_is_no_clobber_and_publishes_when_absent() {
		let base = temp_dir("publish-noreplace");
		let source = base.join("source.txt");
		let destination = base.join("destination.txt");
		write(&source, "temp");
		write(&destination, "current");
		assert!(!publish_staged_file_io(&source, &destination).expect("existing"));
		assert_eq!(fs::read_to_string(&destination).unwrap(), "current");
		assert!(source.exists(), "a lost race leaves the staged file in place");
		fs::remove_file(&destination).expect("remove");
		assert!(publish_staged_file_io(&source, &destination).expect("publish"));
		assert_eq!(fs::read_to_string(&destination).unwrap(), "temp");
		assert!(!source.exists(), "publication consumes the staged file");
	}

	#[test]
	fn copy_private_file_never_overwrites_an_existing_destination() {
		let base = temp_dir("publish");
		let source = base.join("source.txt");
		let destination = base.join("destination.txt");
		write(&source, "legacy");
		write(&destination, "current");
		assert!(!copy_private_file(&source, &destination).expect("copy"));
		assert_eq!(fs::read_to_string(&destination).unwrap(), "current");
		assert_eq!(fs::read_to_string(&source).unwrap(), "legacy");
		let leftovers: Vec<_> = fs::read_dir(&base)
			.expect("base")
			.flatten()
			.filter(|entry| entry.file_name().to_string_lossy().starts_with(".aragent-copy."))
			.collect();
		assert!(leftovers.is_empty(), "publish removes its temp file");
	}

	#[test]
	fn copy_merge_refuses_itself_and_nested_destinations() {
		let base = temp_dir("nested");
		let legacy = base.join("legacy");
		write(&legacy.join("data.txt"), "x");
		assert!(!copy_merge(&legacy, &legacy).is_clean());
		let nested = legacy.join("nested");
		let report = copy_merge(&legacy, &nested);
		assert!(!report.is_clean(), "a nested destination must be refused");
		assert!(!nested.exists(), "nothing may be created inside the source");
		assert!(legacy.join("data.txt").is_file());
	}

	#[test]
	fn copy_merge_creates_private_modes() {
		let base = temp_dir("modes");
		let legacy = base.join("legacy");
		let current = base.join("current");
		write(&legacy.join("dir/file.txt"), "x");
		write(&legacy.join("dir/tool.sh"), "#!/bin/sh\n");
		fs::set_permissions(legacy.join("dir/file.txt"), fs::Permissions::from_mode(0o644)).expect("chmod");
		fs::set_permissions(legacy.join("dir/tool.sh"), fs::Permissions::from_mode(0o755)).expect("chmod");
		fs::set_permissions(legacy.join("dir"), fs::Permissions::from_mode(0o755)).expect("chmod");
		copy_merge(&legacy, &current);
		assert_eq!(fs::metadata(current.join("dir")).unwrap().permissions().mode() & 0o777, 0o700);
		assert_eq!(
			fs::metadata(current.join("dir/file.txt")).unwrap().permissions().mode() & 0o777,
			0o600
		);
		assert_eq!(
			fs::metadata(current.join("dir/tool.sh")).unwrap().permissions().mode() & 0o777,
			0o700,
			"executables keep an executable private mode"
		);
		assert_eq!(fs::metadata(legacy.join("dir")).unwrap().permissions().mode() & 0o777, 0o755);
		assert_eq!(
			fs::metadata(legacy.join("dir/file.txt")).unwrap().permissions().mode() & 0o777,
			0o644
		);
		assert_eq!(
			fs::metadata(legacy.join("dir/tool.sh")).unwrap().permissions().mode() & 0o777,
			0o755
		);
	}

	#[test]
	fn marker_is_atomic_and_idempotent() {
		let base = temp_dir("marker");
		let root = base.join("root");
		write(&base.join("source/data.txt"), "x");
		fs::create_dir_all(&root).expect("root");
		write_marker(&root).expect("marker");
		assert_eq!(fs::read_to_string(root.join(MARKER_FILE)).unwrap(), "version=1\nsource=.omp\n");
		assert_eq!(fs::metadata(root.join(MARKER_FILE)).unwrap().permissions().mode() & 0o777, 0o600);
		let report = copy_merge(&base.join("source"), &root.join("other"));
		assert!(report.is_clean(), "{:?}", report.warnings);
		assert_eq!(fs::read_to_string(root.join("other/data.txt")).unwrap(), "x");
		write_marker(&root).expect("idempotent marker");
		assert_eq!(fs::read_to_string(root.join(MARKER_FILE)).unwrap(), "version=1\nsource=.omp\n");
	}

	#[test]
	fn migration_gate_skips_marker_explicit_override_and_same_root() {
		let base = temp_dir("gate");
		let legacy = base.join("legacy");
		let current = base.join("current");
		fs::create_dir_all(&legacy).expect("legacy");
		assert!(migration_pending(&legacy, &current));
		write(&current.join(MARKER_FILE), "done");
		assert!(!migration_pending(&legacy, &current), "marker stops the copy");
		let _ = fs::remove_file(current.join(MARKER_FILE));
		assert!(!migration_pending(&legacy, &legacy), "source == dest is a no-op");
		assert!(!migration_pending(&base.join("missing"), &current));
	}

	fn with_migration_env<T>(home: &Path, overrides: &[(&str, &Path)], run: impl FnOnce() -> T) -> T {
		let _env = crate::TEST_ENV_LOCK.lock().expect("env lock");
		let keys = [
			"HOME",
			"PI_CONFIG_DIR",
			"PI_CODING_AGENT_DIR",
			"ARAGENT_PROFILE",
			"OMP_PROFILE",
			"PI_PROFILE",
			"XDG_DATA_HOME",
			"XDG_STATE_HOME",
			"XDG_CACHE_HOME",
		];
		let saved = keys.map(std::env::var_os);
		// SAFETY: guarded by TEST_ENV_LOCK.
		unsafe {
			std::env::set_var("HOME", home);
			for key in &keys[1..] {
				std::env::remove_var(*key);
			}
			for &(key, value) in overrides {
				std::env::set_var(key, value);
			}
		}
		let result = run();
		// SAFETY: guarded by TEST_ENV_LOCK.
		unsafe {
			for (key, value) in keys.into_iter().zip(saved) {
				match value {
					Some(value) => std::env::set_var(key, value),
					None => std::env::remove_var(key),
				}
			}
		}
		result
	}

	#[test]
	fn startup_migration_copies_once_and_writes_the_marker() {
		let base = temp_dir("startup");
		let home = base.join("home");
		write(&home.join(".omp/agent/state.txt"), "legacy");
		write(&home.join(".omp/run/session-writer/a.lock"), "lock");
		with_migration_env(&home, &[], migrate_if_needed);
		let current = home.join(".aragent");
		assert_eq!(fs::read_to_string(current.join("agent/state.txt")).unwrap(), "legacy");
		assert!(current.join(MARKER_FILE).is_file());
		assert!(!current.join("run").exists(), "runtime locks are not migrated");
		assert!(home.join(".omp/agent/state.txt").is_file(), "legacy tree is retained");
		write(&current.join("agent/state.txt"), "current");
		write(&home.join(".omp/agent/other.txt"), "late");
		with_migration_env(&home, &[], migrate_if_needed);
		assert_eq!(fs::read_to_string(current.join("agent/state.txt")).unwrap(), "current");
		assert!(!current.join("agent/other.txt").exists(), "the marker stops re-merges");
	}

	#[test]
	fn startup_migration_copies_the_legacy_xdg_tree() {
		let base = temp_dir("xdg");
		let home = base.join("home");
		let data = base.join("data");
		write(&home.join(".omp/agent/state.txt"), "legacy");
		write(&data.join("omp/state.txt"), "xdg-legacy");
		with_migration_env(
			&home,
			&[("XDG_DATA_HOME", data.as_path())],
			migrate_if_needed,
		);
		assert_eq!(fs::read_to_string(data.join("aragent/state.txt")).unwrap(), "xdg-legacy");
		assert_eq!(fs::read_to_string(data.join("omp/state.txt")).unwrap(), "xdg-legacy");
		assert!(home.join(".aragent").join(MARKER_FILE).is_file());
	}

	#[test]
	fn profile_derived_agent_dir_does_not_block_migration() {
		let base = temp_dir("profile");
		let home = base.join("home");
		write(&home.join(".omp/agent/state.txt"), "legacy");
		let profile_agent = home.join(".aragent/profiles/work/agent");
		with_migration_env(
			&home,
			&[
				("ARAGENT_PROFILE", Path::new("work")),
				("PI_CODING_AGENT_DIR", profile_agent.as_path()),
			],
			migrate_if_needed,
		);
		assert_eq!(
			fs::read_to_string(home.join(".aragent/agent/state.txt")).unwrap(),
			"legacy"
		);
		assert!(home.join(".aragent").join(MARKER_FILE).is_file());
	}

	#[test]
	fn explicit_override_skips_migration_entirely() {
		let base = temp_dir("explicit");
		let home = base.join("home");
		write(&home.join(".omp/agent/state.txt"), "legacy");
		let explicit = base.join("explicit");
		with_migration_env(&home, &[("PI_CONFIG_DIR", explicit.as_path())], migrate_if_needed);
		assert!(!home.join(".aragent").exists(), "no implicit migration under an override");
		assert!(!explicit.join(MARKER_FILE).exists(), "the override root is untouched");
		let agent_override = base.join("agent-override");
		with_migration_env(
			&home,
			&[("PI_CODING_AGENT_DIR", agent_override.as_path())],
			migrate_if_needed,
		);
		assert!(!home.join(".aragent").exists(), "PI_CODING_AGENT_DIR also disables migration");
		assert!(!agent_override.join(MARKER_FILE).exists());
	}

	#[test]
	fn adopt_legacy_sqlite_snapshots_committed_wal_rows_without_sidecars() {
		let base = temp_dir("sqlite");
		let legacy = base.join("legacy.db");
		let current = base.join("current.db");
		let legacy_db = rusqlite::Connection::open(&legacy).expect("legacy db");
		legacy_db.pragma_update(None, "journal_mode", "WAL").expect("wal");
		legacy_db.pragma_update(None, "wal_autocheckpoint", 0).expect("no checkpoint");
		legacy_db
			.execute_batch("CREATE TABLE probe(value TEXT); INSERT INTO probe VALUES ('wal');")
			.expect("seed");
		assert!(base.join("legacy.db-wal").is_file());
		assert!(adopt_legacy_sqlite(&current, &legacy).expect("adopt"));
		assert!(!base.join("current.db-wal").exists(), "snapshot must not copy sidecars");
		assert!(!base.join("current.db-shm").exists());
		let snapshot = rusqlite::Connection::open(&current).expect("snapshot");
		let value: String = snapshot
			.query_row("SELECT value FROM probe", [], |row| row.get(0))
			.expect("committed row");
		assert_eq!(value, "wal");
		drop(snapshot);
		assert!(base.join("legacy.db-wal").is_file(), "legacy sidecars stay untouched");
		write(&current, "current");
		assert!(!adopt_legacy_sqlite(&current, &legacy).expect("no overwrite"));
		assert_eq!(fs::read_to_string(&current).unwrap(), "current");
		drop(legacy_db);
	}

	#[test]
	fn copy_merge_snapshots_a_plain_sqlite_file_without_sidecars() {
		let base = temp_dir("sqlite-plain");
		let legacy = base.join("legacy");
		let current = base.join("current");
		fs::create_dir_all(&legacy).expect("legacy");
		let database = legacy.join("plain.db");
		{
			let connection = rusqlite::Connection::open(&database).expect("db");
			connection
				.execute_batch("CREATE TABLE probe(value TEXT); INSERT INTO probe VALUES ('row');")
				.expect("seed");
		}
		assert!(!legacy.join("plain.db-wal").exists());
		assert!(!legacy.join("plain.db-shm").exists());
		assert!(is_sqlite_source(&database), "the real SQLite header is recognized");
		let report = copy_merge(&legacy, &current);
		assert!(report.is_clean(), "{:?}", report.warnings);
		assert_eq!(report.copied_files, 1);
		assert!(!current.join("plain.db-wal").exists());
		let snapshot = rusqlite::Connection::open(current.join("plain.db")).expect("snapshot");
		let value: String = snapshot
			.query_row("SELECT value FROM probe", [], |row| row.get(0))
			.expect("row");
		assert_eq!(value, "row");
	}

	#[test]
	fn copy_merge_snapshots_databases_and_skips_their_sidecars() {
		let base = temp_dir("sqlite-merge");
		let legacy = base.join("legacy");
		let current = base.join("current");
		fs::create_dir_all(&legacy).expect("legacy");
		let db = rusqlite::Connection::open(legacy.join("agent.db")).expect("db");
		db.pragma_update(None, "journal_mode", "WAL").expect("wal");
		db.pragma_update(None, "wal_autocheckpoint", 0).expect("no checkpoint");
		db.execute_batch("CREATE TABLE probe(value TEXT); INSERT INTO probe VALUES ('wal');")
			.expect("seed");
		let report = copy_merge(&legacy, &current);
		assert!(report.is_clean(), "{:?}", report.warnings);
		assert!(!current.join("agent.db-wal").exists());
		assert!(!current.join("agent.db-shm").exists());
		let snapshot = rusqlite::Connection::open(current.join("agent.db")).expect("snapshot");
		let value: String = snapshot
			.query_row("SELECT value FROM probe", [], |row| row.get(0))
			.expect("committed row");
		assert_eq!(value, "wal");
		drop(snapshot);
		drop(db);
	}
}
