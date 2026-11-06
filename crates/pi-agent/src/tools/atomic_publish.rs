//! Atomic no-clobber publication of an already complete staging file.
//!
//! Extracted from `crates/omp-cli/src/eval_host.rs`, where spilled eval
//! artifacts and agent outputs both need the same guarantee: a public name is
//! only ever created by [`publish_staged_file`], so a reader can never observe
//! a partial file, and an existing file is never replaced. The staged payload
//! lives under an exclusive sibling name until the atomic publish succeeds.
//!
//! `hard_link` is the primary atomic no-clobber operation. Android app-private
//! storage can refuse hard links (EACCES/EPERM) and some filesystems do not
//! support them, so the fallback is the native `renameat2(RENAME_NOREPLACE)`
//! syscall. `exists()` + `rename` is deliberately not used: that pair races and
//! `rename` replaces the destination.
//!
//! Callers that own a legitimate update of the same name (a follow-up turn
//! rewriting its own `<id>.md`) must use an explicit replace path instead of
//! this primitive; no-clobber is for first publication of a new identity.

use std::path::Path;

/// Publish an already complete staging file under `final_path` without
/// replacing an existing file.
///
/// `Ok(false)` means another writer owns this destination; the caller must
/// keep the existing file untouched and choose another identity or retry.
pub fn publish_staged_file(staged: &Path, final_path: &Path) -> Result<bool, String> {
	match std::fs::hard_link(staged, final_path) {
		Ok(()) => Ok(true),
		Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
		Err(error) => match rename_noreplace(staged, final_path) {
			Ok(published) => Ok(published),
			Err(fallback) => Err(format!(
				"hard link failed ({error}); atomic no-clobber rename failed ({fallback})"
			)),
		},
	}
}

/// Atomically move `from` onto `to` only when `to` does not exist.
///
/// `Ok(false)` means another writer owns the destination. Linux and Android
/// implement this natively as `renameat2(..., RENAME_NOREPLACE)`. The raw
/// syscall avoids a link-time dependency on a bionic `renameat2` symbol that
/// older API levels do not export; kernels without `renameat2` fail with
/// `ENOSYS` and publication reports both errors instead of silently
/// overwriting.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn rename_noreplace(from: &Path, to: &Path) -> std::io::Result<bool> {
	use std::{ffi::CString, os::unix::ffi::OsStrExt};

	fn cstring(path: &Path) -> std::io::Result<CString> {
		CString::new(path.as_os_str().as_bytes()).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				format!("path contains a NUL: {}", path.display()),
			)
		})
	}

	let from = cstring(from)?;
	let to = cstring(to)?;
	// SAFETY: both C strings are NUL-terminated and outlive the call; the
	// variadic arguments are the `renameat2` signature with LP64-wide integers.
	let result = unsafe {
		libc::syscall(
			libc::SYS_renameat2,
			libc::AT_FDCWD as libc::c_long,
			from.as_ptr(),
			libc::AT_FDCWD as libc::c_long,
			to.as_ptr(),
			libc::RENAME_NOREPLACE as libc::c_long,
		)
	};
	if result == 0 {
		return Ok(true);
	}
	let error = std::io::Error::last_os_error();
	match error.raw_os_error() {
		Some(libc::EEXIST) => Ok(false),
		_ => Err(error),
	}
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn rename_noreplace(_from: &Path, _to: &Path) -> std::io::Result<bool> {
	Err(std::io::Error::new(
		std::io::ErrorKind::Unsupported,
		"atomic no-clobber rename needs renameat2 on this platform",
	))
}
