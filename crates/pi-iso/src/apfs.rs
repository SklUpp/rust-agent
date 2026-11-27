//! macOS APFS clonefile-based isolation.
//!
//! `clonefile(2)` recursively reflinks an entire directory tree in a single
//! syscall. Both paths share the same on-disk blocks until either side is
//! modified; the kernel handles per-block copy-on-write. The destination is
//! a fully independent directory tree from the caller's perspective — there
//! is no mount to undo, so [`stop`](IsolationBackend::stop) is a recursive
//! remove.

use std::path::Path;

use async_trait::async_trait;

#[cfg(not(target_os = "macos"))]
use crate::IsoError;
use crate::{BackendKind, IsoResult, IsolationBackend, ProbeResult};

pub struct ApfsBackend;

pub fn backend() -> &'static dyn IsolationBackend {
	&ApfsBackend
}

#[async_trait]
impl IsolationBackend for ApfsBackend {
	fn kind(&self) -> BackendKind {
		BackendKind::Apfs
	}

	fn probe(&self) -> ProbeResult {
		#[cfg(target_os = "macos")]
		{
			ProbeResult::available()
		}
		#[cfg(not(target_os = "macos"))]
		{
			ProbeResult::unavailable("APFS clonefile isolation is only available on macOS")
		}
	}

	fn start(&self, lower: &Path, merged: &Path) -> IsoResult<()> {
		#[cfg(target_os = "macos")]
		{
			imp::start(lower, merged)
		}
		#[cfg(not(target_os = "macos"))]
		{
			let _ = (lower, merged);
			Err(IsoError::unavailable("APFS clonefile isolation is only available on macOS"))
		}
	}

	fn stop(&self, merged: &Path) -> IsoResult<()> {
		#[cfg(target_os = "macos")]
		{
			imp::stop(merged)
		}
		#[cfg(not(target_os = "macos"))]
		{
			let _ = merged;
			Ok(())
		}
	}
}

#[cfg(target_os = "macos")]
mod imp {
	use std::{
		fs,
		os::unix::ffi::OsStrExt,
		path::Path,
	};

	use crate::{IsoError, IsoResult, fsutil};

	pub fn start(lower: &Path, merged: &Path) -> IsoResult<()> {
		let lower = fsutil::canonical_existing_dir(lower, "clone source")?;
		if let Some(parent) = merged.parent() {
			fs::create_dir_all(parent).map_err(|err| {
				IsoError::other(format!("unable to create parent of {}: {err}", merged.display()))
			})?;
		}
		// `clonefile` refuses to overwrite. Drop any stale tree first.
		if merged.exists() {
			fs::remove_dir_all(merged).map_err(|err| {
				IsoError::other(format!("unable to clear {} before clone: {err}", merged.display()))
			})?;
		}

		let src_c = fsutil::to_cstring(lower.as_os_str().as_bytes(), "lower")?;
		let dst_c = fsutil::to_cstring(merged.as_os_str().as_bytes(), "merged")?;

		// SAFETY: both pointers are valid CStrings whose backing storage lives
		// until after the call. `clonefile` with `flags = 0` performs a
		// recursive reflink clone and does not retain the pointers past the
		// syscall.
		let rc = unsafe { libc::clonefile(src_c.as_ptr(), dst_c.as_ptr(), 0) };
		if rc == 0 {
			return Ok(());
		}
		let err = std::io::Error::last_os_error();
		if let Some(code) = err.raw_os_error()
			&& matches!(code, libc::ENOTSUP | libc::EOPNOTSUPP | libc::EXDEV)
		{
			return Err(IsoError::unavailable(format!(
				"APFS clonefile unsupported on this volume ({err}); {} -> {}",
				lower.display(),
				merged.display()
			)));
		}
		Err(IsoError::other(format!("clonefile {} -> {}: {err}", lower.display(), merged.display())))
	}

	pub fn stop(merged: &Path) -> IsoResult<()> {
		match fs::remove_dir_all(merged) {
			Ok(()) => Ok(()),
			Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
			Err(err) => Err(IsoError::other(format!(
				"unable to remove cloned tree {}: {err}",
				merged.display()
			))),
		}
	}
}
