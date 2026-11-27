//! Private filesystem primitives for owner-only state.
//!
//! Credential databases, diagnostics logs, memory exports and daemon sockets
//! all live in per-user paths that must not be replaced or read by another
//! process. This module holds the shared pieces so each store does not grow its
//! own subtly different mode, symlink or ancestor checks:
//!
//! - [`ensure_private_dir`] creates (or tightens) an owner-only `0700`
//!   directory, refusing symlinks and foreign owners.
//! - [`write_private_atomic`] writes a `0600` file through a same-directory
//!   temporary plus `sync_all` and an atomic rename.
//! - [`tighten_sqlite_sidecars`] brings an existing database and its `-wal` /
//!   `-shm` sidecars back to `0600` after SQLite opened them.
//! - [`verify_ancestor_chain`] proves every existing ancestor can be trusted
//!   not to let another user rename our entries, including the Android
//!   platform `system` uid (`1000`) and app uid (`>= 10000`) identities.
//!
//! Non-Unix builds keep the same API with mode checks compiled out.

use std::{
	fs,
	io::{self, Write as _},
	path::Path,
	sync::atomic::{AtomicU64, Ordering},
};

/// Owner-only file mode for private state.
pub const PRIVATE_FILE_MODE: u32 = 0o600;
/// Owner-only directory mode for private state.
pub const PRIVATE_DIR_MODE: u32 = 0o700;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

/// Current effective uid, or `0` where ownership is not modelled.
#[cfg(unix)]
#[must_use]
pub fn current_euid() -> u32 {
	// SAFETY: geteuid has no preconditions and cannot fail.
	unsafe { libc::geteuid() }
}

#[cfg(not(unix))]
#[must_use]
pub const fn current_euid() -> u32 {
	0
}

/// An ancestor owner we trust not to substitute entries: root, the current
/// user, or (on Android) the platform `system` uid.
#[cfg(unix)]
#[must_use]
pub const fn trusted_owner(uid: u32, euid: u32) -> bool {
	#[cfg(target_os = "android")]
	{
		if uid == 1000 {
			return true;
		}
	}
	uid == 0 || uid == euid
}

#[cfg(not(unix))]
#[must_use]
pub const fn trusted_owner(_uid: u32, _euid: u32) -> bool {
	true
}

/// Prove that a group-writable directory's writable group is exclusive to a
/// trusted principal. GID 0 is not exclusive (wheel/root groups may contain
/// other users), so only established Android identities qualify: the platform
/// `system` uid/gid or an app's own uid/gid pair.
#[cfg(unix)]
#[must_use]
pub const fn exclusive_group(_uid: u32, _euid: u32, _gid: u32) -> bool {
	#[cfg(target_os = "android")]
	{
		// Verified on device: `/data` and `/data/data` are `system:system`
		// (uid/gid 1000), mode 0771. Older platform layouts use `root:system`
		// (uid 0, gid 1000); the writable principal is still the platform-only
		// gid 1000 either way.
		if _gid == 1000 && (_uid == 1000 || _uid == 0) {
			return true;
		}
		// App ids start at 10000 and use the app uid as its own private
		// primary gid; require the directory to match this process exactly.
		if _euid >= 10_000 && _uid == _euid && _gid == _euid {
			return true;
		}
	}
	false
}

#[cfg(not(unix))]
#[must_use]
pub const fn exclusive_group(_uid: u32, _euid: u32, _gid: u32) -> bool {
	false
}

/// A directory ancestor is trusted when its owner is trusted and other users
/// cannot create/rename inside it, or when sticky semantics protect our owned
/// entries, or when a writable group is provably platform-exclusive.
#[must_use]
pub fn trusted_ancestor(metadata: &fs::Metadata, euid: u32) -> bool {
	#[cfg(unix)]
	{
		use std::os::unix::fs::{MetadataExt, PermissionsExt};
		if metadata.file_type().is_symlink() || !metadata.is_dir() {
			return false;
		}
		let mode = metadata.permissions().mode();
		let uid = metadata.uid();
		let gid = metadata.gid();
		let owner_trusted = trusted_owner(uid, euid);
		if mode & 0o022 == 0 {
			return owner_trusted;
		}
		if mode & 0o1000 != 0 {
			// Sticky: rename of our entries is limited to root/current user.
			return owner_trusted;
		}
		if mode & 0o002 != 0 {
			// World-writable without sticky semantics.
			return false;
		}
		// Group-writable only: reject unless the writable group is exclusive.
		exclusive_group(uid, euid, gid)
	}
	#[cfg(not(unix))]
	{
		let _ = euid;
		!metadata.file_type().is_symlink() && metadata.is_dir()
	}
}

/// Reject a relative path and prove every existing ancestor is trusted.
/// `missing_ok` allows trailing components that do not exist yet.
pub fn verify_ancestor_chain(dir: &Path, euid: u32, missing_ok: bool) -> io::Result<()> {
	if !dir.is_absolute() {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("{} is not an absolute path", dir.display()),
		));
	}
	for ancestor in dir.ancestors() {
		if ancestor.as_os_str().is_empty() {
			continue;
		}
		match fs::symlink_metadata(ancestor) {
			Ok(metadata) => {
				if !trusted_ancestor(&metadata, euid) {
					return Err(io::Error::new(
						io::ErrorKind::PermissionDenied,
						format!("{} is not a trusted ancestor", ancestor.display()),
					));
				}
			},
			Err(error) if missing_ok && error.kind() == io::ErrorKind::NotFound => {},
			Err(error) => return Err(error),
		}
	}
	Ok(())
}

/// Create an owner-only `0700` directory, tightening an existing one.
///
/// On Unix the directory is opened with `O_NOFOLLOW | O_DIRECTORY` and the mode
/// is changed through that descriptor (`fchmod`), so a symlink planted between
/// the existence check and the mode change is never followed. A symlink, a
/// non-directory, or a directory owned by another uid is refused. Missing
/// parents are created recursively with the same mode; an unsafe ancestor is
/// not this function's concern (see [`verify_ancestor_chain`]).
pub fn ensure_private_dir(dir: &Path) -> io::Result<()> {
	#[cfg(unix)]
	{
		ensure_private_dir_handle(dir)
	}
	#[cfg(not(unix))]
	{
		ensure_private_dir_portable(dir)
	}
}

#[cfg(unix)]
fn ensure_private_dir_handle(dir: &Path) -> io::Result<()> {
	use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

	fn open(dir: &Path) -> io::Result<fs::File> {
		fs::OpenOptions::new()
			.read(true)
			.custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
			.open(dir)
	}

	let handle = match open(dir) {
		Ok(handle) => handle,
		Err(error) if error.kind() == io::ErrorKind::NotFound => {
			use std::os::unix::fs::DirBuilderExt as _;
			let mut builder = fs::DirBuilder::new();
			builder.recursive(true).mode(PRIVATE_DIR_MODE);
			builder.create(dir)?;
			// Re-open instead of trusting the path we just created: a racing
			// process may have replaced it with a symlink.
			open(dir)?
		},
		Err(error) => return Err(error),
	};
	let metadata = handle.metadata()?;
	if !metadata.is_dir() {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("{} is not a directory", dir.display()),
		));
	}
	if metadata.uid() != current_euid() {
		return Err(io::Error::new(
			io::ErrorKind::PermissionDenied,
			format!("{} is owned by another user", dir.display()),
		));
	}
	// `fchmod` on the descriptor: no path lookup, so no symlink follows.
	if metadata.permissions().mode() & 0o7777 != PRIVATE_DIR_MODE {
		handle.set_permissions(fs::Permissions::from_mode(PRIVATE_DIR_MODE))?;
	}
	Ok(())
}

#[cfg(not(unix))]
fn ensure_private_dir_portable(dir: &Path) -> io::Result<()> {
	match fs::symlink_metadata(dir) {
		Ok(metadata) => {
			if metadata.file_type().is_symlink() || !metadata.is_dir() {
				return Err(io::Error::new(
					io::ErrorKind::InvalidInput,
					format!("{} is not a directory", dir.display()),
				));
			}
			Ok(())
		},
		Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir_all(dir),
		Err(error) => Err(error),
	}
}

/// Best-effort chmod: a filesystem that refuses mode changes must not fail the
/// caller that only tightens an already open file.
pub fn set_mode_best_effort(path: &Path, mode: u32) {
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt as _;
		let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
	}
	#[cfg(not(unix))]
	{
		let _ = (path, mode);
	}
}

/// Write `bytes` to `path` atomically with the given owner-only `mode`.
///
/// The bytes land in a temporary file in the same directory (mode forced, then
/// `sync_all`), and only a successful fsync is followed by the rename. A
/// failure or panic before the rename leaves the destination untouched.
pub fn write_private_atomic(path: &Path, bytes: &[u8], mode: u32) -> io::Result<()> {
	let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty()).ok_or_else(
		|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent directory"),
	)?;
	let name = path
		.file_name()
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
	fs::create_dir_all(parent)?;
	let mut temp_name = std::ffi::OsString::from(".");
	temp_name.push(name);
	temp_name.push(format!(
		".{}.{}.tmp",
		std::process::id(),
		NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
	));
	let temp = parent.join(temp_name);
	// `create_new` failing means the path is not ours: never delete it.
	let mut file = match open_temp_file(&temp, mode) {
		Ok(file) => file,
		Err(error) => return Err(error),
	};
	let written = write_and_sync(&mut file, bytes, mode);
	drop(file);
	let result = written.and_then(|()| fs::rename(&temp, path));
	if result.is_err() {
		let _ = fs::remove_file(&temp);
	}
	result
}

/// Open the same-directory temporary with `create_new`, mode and no symlinks.
fn open_temp_file(temp: &Path, mode: u32) -> io::Result<fs::File> {
	#[cfg(not(unix))]
	let _ = mode;
	let mut options = fs::OpenOptions::new();
	options.write(true).create_new(true);
	#[cfg(unix)]
	{
		use std::os::unix::fs::OpenOptionsExt as _;
		options.mode(mode).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
	}
	options.open(temp)
}

/// Write, force the caller's exact mode, and fsync the owned temporary.
fn write_and_sync(file: &mut fs::File, bytes: &[u8], mode: u32) -> io::Result<()> {
	#[cfg(not(unix))]
	let _ = mode;
	file.write_all(bytes)?;
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt as _;
		file.set_permissions(fs::Permissions::from_mode(mode))?;
	}
	file.sync_all()
}

/// Open `path` for writing and truncate it only after proving it is ours.
///
/// The file is opened without `O_TRUNC` first, so a foreign-owned or
/// non-regular target is rejected before any byte is lost. On Unix the opened
/// descriptor must be a regular file owned by the current uid; its mode is
/// forced to `0600` through the descriptor (failure is fatal).
pub fn open_private_file_trunc(path: &Path) -> io::Result<fs::File> {
	let mut options = fs::OpenOptions::new();
	options.write(true).create(true);
	#[cfg(unix)]
	{
		use std::os::unix::fs::OpenOptionsExt as _;
		options
			.mode(PRIVATE_FILE_MODE)
			.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
	}
	let file = options.open(path)?;
	#[cfg(unix)]
	{
		use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
		let metadata = file.metadata()?;
		if !metadata.is_file() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				format!("{} is not a regular file", path.display()),
			));
		}
		if metadata.uid() != current_euid() {
			return Err(io::Error::new(
				io::ErrorKind::PermissionDenied,
				format!("{} is owned by another user", path.display()),
			));
		}
		file.set_permissions(fs::Permissions::from_mode(PRIVATE_FILE_MODE))?;
	}
	file.set_len(0)?;
	Ok(file)
}

/// Force an existing database and its `-wal` / `-shm` sidecars to `0600`.
/// Missing files (SQLite creates them lazily) are ignored.
pub fn tighten_sqlite_sidecars(db_path: &Path) {
	#[cfg(not(unix))]
	let _ = db_path;
	#[cfg(unix)]
	{
		set_mode_best_effort(db_path, PRIVATE_FILE_MODE);
		for suffix in ["-wal", "-shm"] {
			let mut os = db_path.as_os_str().to_os_string();
			os.push(suffix);
			set_mode_best_effort(Path::new(&os), PRIVATE_FILE_MODE);
		}
	}
}

#[cfg(all(test, unix))]
mod tests {
	use std::{
		fs,
		os::unix::fs::{PermissionsExt as _, symlink},
		path::{Path, PathBuf},
		sync::atomic::{AtomicU32, Ordering},
	};

	use super::{
		PRIVATE_DIR_MODE, PRIVATE_FILE_MODE, current_euid, ensure_private_dir,
		open_private_file_trunc, verify_ancestor_chain, write_private_atomic,
	};

	static COUNTER: AtomicU32 = AtomicU32::new(0);

	fn temp_dir(label: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"pi-private-fs-{label}-{}-{}",
			std::process::id(),
			COUNTER.fetch_add(1, Ordering::Relaxed)
		));
		fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	fn mode(path: &Path) -> u32 {
		fs::symlink_metadata(path).expect("metadata").permissions().mode() & 0o777
	}

	#[test]
	fn ensure_private_dir_tightens_existing_directory() {
		let dir = temp_dir("tighten");
		fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).expect("chmod");
		ensure_private_dir(&dir).expect("private dir");
		assert_eq!(mode(&dir), PRIVATE_DIR_MODE);
		fs::remove_dir_all(&dir).expect("cleanup");
	}

	#[test]
	fn ensure_private_dir_rejects_symlink() {
		let root = temp_dir("symlink");
		let target = root.join("target");
		fs::create_dir(&target).expect("target");
		let link = root.join("link");
		symlink(&target, &link).expect("symlink");
		assert!(ensure_private_dir(&link).is_err());
		fs::remove_dir_all(&root).expect("cleanup");
	}

	#[test]
	fn write_private_atomic_creates_owner_only_file() {
		let dir = temp_dir("atomic");
		let path = dir.join("secret");
		write_private_atomic(&path, b"token", PRIVATE_FILE_MODE).expect("write");
		assert_eq!(mode(&path), PRIVATE_FILE_MODE);
		assert_eq!(fs::read(&path).expect("read"), b"token".to_vec());
		fs::remove_dir_all(&dir).expect("cleanup");
	}

	#[test]
	fn open_private_file_trunc_tightens_and_truncates_owned_file() {
		let dir = temp_dir("trunc");
		let path = dir.join("log");
		fs::write(&path, b"old").expect("seed");
		fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("chmod");
		let file = open_private_file_trunc(&path).expect("open");
		drop(file);
		assert_eq!(mode(&path), PRIVATE_FILE_MODE);
		assert_eq!(fs::metadata(&path).expect("metadata").len(), 0);
		fs::remove_dir_all(&dir).expect("cleanup");
	}

	#[test]
	fn ancestor_chain_accepts_temp_dir_and_rejects_relative_path() {
		let dir = temp_dir("chain");
		let euid = current_euid();
		assert!(verify_ancestor_chain(&dir, euid, false).is_ok());
		assert!(verify_ancestor_chain(Path::new("relative/dir"), euid, true).is_err());
		fs::remove_dir_all(&dir).expect("cleanup");
	}
}
