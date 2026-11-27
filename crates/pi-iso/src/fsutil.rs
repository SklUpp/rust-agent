//! Filesystem primitives shared by the isolation backends.
//!
//! Path resolution, source validation, C-string conversion, symlink copying,
//! and mtime preservation were duplicated in every backend. They live here so
//! the error wording and the per-platform `cfg` arms cannot drift apart.

#[cfg(unix)]
use std::ffi::CString;
use std::path::{Path, PathBuf};

use crate::IsoError;

/// Make `path` absolute against the process working directory.
///
/// An unreadable cwd leaves a relative path untouched, which is the behaviour
/// every backend had before this helper was shared. The `Result` mirrors
/// [`canonical_existing_dir`] so callers thread both through `?`.
pub(crate) fn absolutize(path: &Path) -> Result<PathBuf, IsoError> {
	Ok(if path.is_absolute() {
		path.to_path_buf()
	} else {
		std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
	})
}

/// Canonicalize `path` and prove it is a directory.
///
/// `label` is the whole noun used in the error text (`"rcopy source"`,
/// `"overlay lower"`, …) so each backend keeps its historical wording.
pub(crate) fn canonical_existing_dir(path: &Path, label: &str) -> Result<PathBuf, IsoError> {
	canonical_dir(path, label, IsoError::other)
}

/// [`canonical_existing_dir`] for a backend that classifies a bad source path
/// as [`IsoError::Unavailable`]: ZFS falls back to another backend when the
/// path is not a dataset mountpoint.
#[cfg(unix)]
pub(crate) fn canonical_existing_dir_unavailable(
	path: &Path,
	label: &str,
) -> Result<PathBuf, IsoError> {
	canonical_dir(path, label, IsoError::unavailable)
}

fn canonical_dir(
	path: &Path,
	label: &str,
	error: fn(String) -> IsoError,
) -> Result<PathBuf, IsoError> {
	let resolved = absolutize(path)?;
	let meta = std::fs::metadata(&resolved)
		.map_err(|err| error(format!("invalid {label} {}: {err}", resolved.display())))?;
	if !meta.is_dir() {
		return Err(error(format!("{label} {} is not a directory", resolved.display())));
	}
	Ok(std::fs::canonicalize(&resolved).unwrap_or(resolved))
}

/// NUL-checked C string for a syscall target or a mount option list.
///
/// Takes bytes rather than a `Path` because overlayfs feeds its `-o` string
/// through the same check; the `label` keeps each call site's message.
#[cfg(unix)]
pub(crate) fn to_cstring(bytes: &[u8], label: &str) -> Result<CString, IsoError> {
	CString::new(bytes)
		.map_err(|err| IsoError::other(format!("{label} path contains NUL byte: {err}")))
}

/// Recreate `src` as a symlink at `dst`, unless it resolves outside `root`.
///
/// An escaping link returns `Ok(())` on purpose: recursive copy and plain diff
/// both omit such links, and an omission on one side must not read as a
/// deletion on the other.
pub(crate) fn copy_symlink_confined(src: &Path, dst: &Path, root: &Path) -> Result<(), IsoError> {
	if crate::symlink_escapes(root, src) {
		return Ok(());
	}
	copy_symlink(src, dst)
}

#[cfg(unix)]
fn copy_symlink(src: &Path, dst: &Path) -> Result<(), IsoError> {
	let target = std::fs::read_link(src)
		.map_err(|err| IsoError::other(format!("read_link {}: {err}", src.display())))?;
	std::os::unix::fs::symlink(target, dst)
		.map_err(|err| IsoError::other(format!("symlink {}: {err}", dst.display())))
}

#[cfg(windows)]
fn copy_symlink(src: &Path, dst: &Path) -> Result<(), IsoError> {
	let target = std::fs::read_link(src)
		.map_err(|err| IsoError::other(format!("read_link {}: {err}", src.display())))?;
	let meta = std::fs::symlink_metadata(src)
		.map_err(|err| IsoError::other(format!("symlink_metadata {}: {err}", src.display())))?;
	let res = if meta.file_type().is_dir() {
		std::os::windows::fs::symlink_dir(target, dst)
	} else {
		std::os::windows::fs::symlink_file(target, dst)
	};
	res.map_err(|err| IsoError::other(format!("symlink {}: {err}", dst.display())))
}

#[cfg(not(any(unix, windows)))]
fn copy_symlink(_src: &Path, _dst: &Path) -> Result<(), IsoError> {
	Err(IsoError::other("symlink copy unsupported on this platform"))
}

/// Mirror `src`'s access and modification times onto `dst` without following a
/// symlink at `dst`.
///
/// Callers treat failure as a non-event: the timestamps only feed the cheap
/// `(size, mtime)` short-circuit in [`crate::diff`], never correctness.
#[cfg(unix)]
pub(crate) fn set_times_nofollow(path: &Path, meta: &std::fs::Metadata) -> std::io::Result<()> {
	use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};

	let times = [
		libc::timespec { tv_sec: meta.atime() as _, tv_nsec: meta.atime_nsec() as libc::c_long },
		libc::timespec { tv_sec: meta.mtime() as _, tv_nsec: meta.mtime_nsec() as libc::c_long },
	];
	let c_path = CString::new(path.as_os_str().as_bytes())?;
	// SAFETY: `c_path` and `times` live until the syscall returns; the kernel
	// retains neither pointer. `AT_SYMLINK_NOFOLLOW` stamps the link itself
	// instead of mutating its target.
	let rc = unsafe {
		libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), libc::AT_SYMLINK_NOFOLLOW)
	};
	if rc == 0 {
		Ok(())
	} else {
		Err(std::io::Error::last_os_error())
	}
}

/// Windows arm: set the modification time only, opening through
/// `FILE_FLAG_BACKUP_SEMANTICS` so directories work too. A link at `dst` is
/// stamped through to its target, matching the previous rcopy behaviour.
#[cfg(windows)]
pub(crate) fn set_times_nofollow(path: &Path, meta: &std::fs::Metadata) -> std::io::Result<()> {
	use std::{
		fs::OpenOptions,
		os::windows::{fs::OpenOptionsExt, io::AsRawHandle},
	};

	use windows_sys::Win32::{
		Foundation::FILETIME,
		Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, SetFileTime},
	};

	let Ok(mtime) = meta.modified() else {
		return Ok(());
	};
	let dur = mtime
		.duration_since(std::time::UNIX_EPOCH)
		.map_err(|err| std::io::Error::other(err.to_string()))?;
	// Windows FILETIME = 100-ns ticks since 1601-01-01.
	const EPOCH_DIFF_100NS: u64 = 116_444_736_000_000_000;
	let ticks = EPOCH_DIFF_100NS + dur.as_secs() * 10_000_000 + u64::from(dur.subsec_nanos() / 100);
	let ft = FILETIME {
		dwLowDateTime:  (ticks & 0xffff_ffff) as u32,
		dwHighDateTime: (ticks >> 32) as u32,
	};

	let mut opts = OpenOptions::new();
	opts.write(true);
	opts.custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
	let file = opts.open(path)?;
	// SAFETY: `file` owns the HANDLE for the duration of the call.
	let ok = unsafe {
		SetFileTime(file.as_raw_handle() as _, std::ptr::null(), std::ptr::null(), &raw const ft)
	};
	if ok != 0 {
		Ok(())
	} else {
		Err(std::io::Error::last_os_error())
	}
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn set_times_nofollow(_path: &Path, _meta: &std::fs::Metadata) -> std::io::Result<()> {
	Ok(())
}
