//! Windows block-clone based isolation.
//!
//! `FSCTL_DUPLICATE_EXTENTS_TO_FILE` asks ReFS to share file extents
//! copy-on-write between a source file and a destination file. NTFS does not
//! implement this control: it rejects the request with `ERROR_INVALID_FUNCTION`.
//! The backend recursively materializes the directory tree and block-clones
//! each regular file. There is no mount/session state to undo, so
//! [`stop`](IsolationBackend::stop) is a recursive remove.

use std::path::Path;

use async_trait::async_trait;

#[cfg(not(windows))]
use crate::IsoError;
use crate::{BackendKind, IsoResult, IsolationBackend, ProbeResult};

pub struct WindowsBlockCloneBackend;

pub fn backend() -> &'static dyn IsolationBackend {
	&WindowsBlockCloneBackend
}

/// Whether `path` resides on a ReFS volume.
///
/// `FSCTL_DUPLICATE_EXTENTS_TO_FILE` is implemented only by ReFS; NTFS and
/// every other Windows filesystem reject it with `ERROR_INVALID_FUNCTION`. The
/// path itself need not exist: `GetVolumePathNameW` reduces it to the volume
/// mount point, which must exist.
#[cfg(windows)]
pub(crate) fn is_refs_path(path: &Path) -> Result<bool, String> {
	use std::{io, os::windows::ffi::OsStrExt as _};

	use windows_sys::Win32::{
		Foundation::{
			ERROR_FILENAME_EXCED_RANGE, ERROR_INSUFFICIENT_BUFFER, ERROR_MORE_DATA, MAX_PATH,
		},
		Storage::FileSystem::{GetVolumeInformationW, GetVolumePathNameW},
	};

	let requested: Vec<u16> = path
		.as_os_str()
		.encode_wide()
		.chain(std::iter::once(0))
		.collect();
	// Extended-length and mounted-folder paths, and short junctions that point
	// at one, can make GetVolumePathNameW return more units than the input
	// path. Start with room for the trailing separator and NUL, then grow up to
	// Windows' maximum path length.
	const MAX_VOLUME_UNITS: usize = 32_768;
	let mut capacity = (requested.len().max(MAX_PATH as usize + 1) + 2).min(MAX_VOLUME_UNITS);
	let mut volume = loop {
		let mut buffer = vec![0u16; capacity];
		// SAFETY: `requested` is NUL-terminated and `buffer` is a writable
		// buffer whose capacity in UTF-16 units is passed alongside it.
		let ok = unsafe {
			GetVolumePathNameW(requested.as_ptr(), buffer.as_mut_ptr(), buffer.len() as u32)
		};
		let err = io::Error::last_os_error();
		let volume_len = buffer.iter().position(|&unit| unit == 0);
		let ends_with_separator = volume_len.is_some_and(|len| {
			len > 0 && (buffer[len - 1] == u16::from(b'\\') || buffer[len - 1] == u16::from(b'/'))
		});
		// A 1-unit-short buffer succeeds without the trailing separator, so a
		// missing separator is also a truncation signal until the cap.
		let retry = capacity < MAX_VOLUME_UNITS
			&& if ok == 0 {
				matches!(
					err.raw_os_error().map(|code| code as u32),
					Some(ERROR_FILENAME_EXCED_RANGE | ERROR_INSUFFICIENT_BUFFER | ERROR_MORE_DATA)
				)
			} else {
				!ends_with_separator
			};
		if !retry {
			if ok == 0 {
				return Err(format!("GetVolumePathNameW({}): {err}", path.display()));
			}
			break buffer;
		}
		capacity = (capacity * 2).min(MAX_VOLUME_UNITS);
	};
	let volume_len = volume
		.iter()
		.position(|&unit| unit == 0)
		.unwrap_or(volume.len());
	if volume_len == 0 {
		return Err(format!("GetVolumePathNameW({}) returned an empty volume", path.display()));
	}
	let mut volume = volume[..volume_len].to_vec();
	// GetVolumeInformationW requires a trailing backslash on the root path.
	if let Some(unit) = volume.last_mut() {
		if *unit == u16::from(b'/') {
			*unit = u16::from(b'\\');
		}
	}
	if volume.last() != Some(&u16::from(b'\\')) {
		volume.push(u16::from(b'\\'));
	}
	volume.push(0);

	let mut fs_name = [0u16; 32];
	// SAFETY: `volume` is NUL-terminated. The optional volume-name, serial,
	// component-length, and flags outputs are null/zero, which the API accepts,
	// and `fs_name` is writable with its capacity passed in UTF-16 units.
	let ok = unsafe {
		GetVolumeInformationW(
			volume.as_ptr(),
			std::ptr::null_mut(),
			0,
			std::ptr::null_mut(),
			std::ptr::null_mut(),
			std::ptr::null_mut(),
			fs_name.as_mut_ptr(),
			fs_name.len() as u32,
		)
	};
	if ok == 0 {
		return Err(format!(
			"GetVolumeInformationW({}): {}",
			path.display(),
			io::Error::last_os_error()
		));
	}
	let name_len = fs_name
		.iter()
		.position(|&unit| unit == 0)
		.unwrap_or(fs_name.len());
	Ok(String::from_utf16_lossy(&fs_name[..name_len]).eq_ignore_ascii_case("refs"))
}

/// Nearest ancestor of `path` that exists, so a not-yet-created destination
/// can still be attributed to its volume.
///
/// Returns the path itself when it exists.
#[cfg(any(windows, test))]
fn nearest_existing_ancestor(path: &Path) -> std::path::PathBuf {
	let mut current = if path.is_absolute() {
		path.to_path_buf()
	} else {
		std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
	};
	while !current.exists() && current.pop() {}
	current
}

#[async_trait]
impl IsolationBackend for WindowsBlockCloneBackend {
	fn kind(&self) -> BackendKind {
		BackendKind::WindowsBlockClone
	}

	fn probe(&self) -> ProbeResult {
		#[cfg(windows)]
		{
			// Volume capability is checked per path in `start`: `lower`/`merged`
			// may be on ReFS while the process cwd or temp dir is on NTFS.
			ProbeResult::available()
		}
		#[cfg(not(windows))]
		{
			ProbeResult::unavailable("Windows block-clone isolation is only available on Windows")
		}
	}

	fn start(&self, lower: &Path, merged: &Path) -> IsoResult<()> {
		#[cfg(windows)]
		{
			imp::start(lower, merged)
		}
		#[cfg(not(windows))]
		{
			let _ = (lower, merged);
			Err(IsoError::unavailable("Windows block-clone isolation is only available on Windows"))
		}
	}

	fn stop(&self, merged: &Path) -> IsoResult<()> {
		#[cfg(windows)]
		{
			imp::stop(merged)
		}
		#[cfg(not(windows))]
		{
			let _ = merged;
			Ok(())
		}
	}
}

#[cfg(windows)]
mod imp {
	use std::{
		fs::{self, File, OpenOptions},
		io,
		os::windows::{
			fs::{FileTypeExt, OpenOptionsExt},
			io::AsRawHandle,
		},
		path::{Path, PathBuf},
	};

	use windows_sys::Win32::{
		Foundation::{
			ERROR_ACCESS_DENIED, ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER,
			ERROR_NOT_SAME_DEVICE, ERROR_NOT_SUPPORTED, FILETIME,
		},
		Storage::FileSystem::{
			FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, SetFileTime,
		},
		System::{
			IO::DeviceIoControl,
			Ioctl::{DUPLICATE_EXTENTS_DATA, FSCTL_DUPLICATE_EXTENTS_TO_FILE},
		},
	};

	use crate::{IsoError, IsoResult};

	pub fn start(lower: &Path, merged: &Path) -> IsoResult<()> {
		let lower = canonical_existing_dir(lower)?;
		require_refs(&lower, "block-clone source")?;
		require_refs(&super::nearest_existing_ancestor(merged), "block-clone destination")?;
		prepare_destination(merged)?;

		let result = recursive_block_clone(&lower, merged);
		if result.is_err() {
			let _ = remove_path(merged);
		}
		result
	}

	pub fn stop(merged: &Path) -> IsoResult<()> {
		remove_path(merged).map_err(|err| {
			IsoError::other(format!("unable to remove block-cloned tree {}: {err}", merged.display()))
		})
	}

	fn canonical_existing_dir(path: &Path) -> IsoResult<PathBuf> {
		let resolved = if path.is_absolute() {
			path.to_path_buf()
		} else {
			std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
		};
		let meta = fs::metadata(&resolved).map_err(|err| {
			IsoError::other(format!("invalid block-clone source {}: {err}", resolved.display()))
		})?;
		if !meta.is_dir() {
			return Err(IsoError::other(format!(
				"block-clone source {} is not a directory",
				resolved.display()
			)));
		}
		Ok(fs::canonicalize(&resolved).unwrap_or(resolved))
	}

	fn prepare_destination(merged: &Path) -> IsoResult<()> {
		if let Some(parent) = merged.parent() {
			fs::create_dir_all(parent).map_err(|err| {
				IsoError::other(format!("create parent of {}: {err}", merged.display()))
			})?;
		}
		remove_path(merged).map_err(|err| {
			IsoError::other(format!("unable to clear {} before block clone: {err}", merged.display()))
		})?;
		Ok(())
	}

	fn require_refs(path: &Path, role: &str) -> IsoResult<()> {
		match super::is_refs_path(path) {
			Ok(true) => Ok(()),
			Ok(false) => Err(IsoError::unavailable(format!(
				"{role} {} is not on a ReFS volume; FSCTL_DUPLICATE_EXTENTS_TO_FILE is ReFS-only",
				path.display()
			))),
			Err(err) => Err(IsoError::unavailable(format!(
				"unable to determine the filesystem of {role} {}: {err}",
				path.display()
			))),
		}
	}

	fn remove_path(path: &Path) -> io::Result<()> {
		let meta = match fs::symlink_metadata(path) {
			Ok(meta) => meta,
			Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
			Err(err) => return Err(err),
		};
		let file_type = meta.file_type();
		if file_type.is_dir() && !file_type.is_symlink() {
			for entry in fs::read_dir(path)? {
				remove_path(&entry?.path())?;
			}
			clear_readonly(path, &meta);
			fs::remove_dir(path)
		} else {
			clear_readonly(path, &meta);
			fs::remove_file(path)
		}
	}

	fn clear_readonly(path: &Path, meta: &fs::Metadata) {
		if meta.file_type().is_symlink() {
			return;
		}
		let mut permissions = meta.permissions();
		if permissions.readonly() {
			// This backend only removes a temporary Windows block-clone tree; clearing
			// the readonly file attribute is required so removal can proceed.
			#[allow(
				clippy::permissions_set_readonly_false,
				reason = "Windows block-clone cleanup must clear the readonly file attribute before \
				          deletion"
			)]
			permissions.set_readonly(false);
			let _ = fs::set_permissions(path, permissions);
		}
	}

	fn recursive_block_clone(lower: &Path, merged: &Path) -> IsoResult<()> {
		fs::create_dir_all(merged)
			.map_err(|err| IsoError::other(format!("create {}: {err}", merged.display())))?;
		clone_dir_contents(lower, merged)?;
		copy_metadata_best_effort(lower, merged);
		Ok(())
	}

	fn clone_dir_contents(src: &Path, dst: &Path) -> IsoResult<()> {
		let entries = fs::read_dir(src)
			.map_err(|err| IsoError::other(format!("read_dir {}: {err}", src.display())))?;
		for entry in entries {
			let entry = entry
				.map_err(|err| IsoError::other(format!("dir entry in {}: {err}", src.display())))?;
			let file_type = entry.file_type().map_err(|err| {
				IsoError::other(format!("file_type {}: {err}", entry.path().display()))
			})?;
			let src_path = entry.path();
			let dst_path = dst.join(entry.file_name());

			if file_type.is_symlink() {
				clone_symlink(&src_path, &dst_path)?;
				copy_metadata_best_effort(&src_path, &dst_path);
			} else if file_type.is_dir() {
				fs::create_dir_all(&dst_path)
					.map_err(|err| IsoError::other(format!("create {}: {err}", dst_path.display())))?;
				clone_dir_contents(&src_path, &dst_path)?;
				copy_metadata_best_effort(&src_path, &dst_path);
			} else if file_type.is_file() {
				clone_regular_file(&src_path, &dst_path)?;
				copy_metadata_best_effort(&src_path, &dst_path);
			} else {
				return Err(IsoError::other(format!(
					"unsupported filesystem entry for block clone: {}",
					src_path.display()
				)));
			}
		}
		Ok(())
	}

	fn clone_symlink(src: &Path, dst: &Path) -> IsoResult<()> {
		let target = fs::read_link(src)
			.map_err(|err| IsoError::other(format!("read_link {}: {err}", src.display())))?;
		let file_type = fs::symlink_metadata(src)
			.map_err(|err| IsoError::other(format!("symlink_metadata {}: {err}", src.display())))?
			.file_type();
		let res = if file_type.is_symlink_dir() {
			std::os::windows::fs::symlink_dir(target, dst)
		} else {
			std::os::windows::fs::symlink_file(target, dst)
		};
		res.map_err(|err| IsoError::other(format!("symlink {}: {err}", dst.display())))
	}

	fn clone_regular_file(src: &Path, dst: &Path) -> IsoResult<()> {
		let src_meta = fs::metadata(src)
			.map_err(|err| IsoError::other(format!("metadata {}: {err}", src.display())))?;
		let len = src_meta.len();

		let src_file = OpenOptions::new().read(true).open(src).map_err(|err| {
			IsoError::other(format!("open block-clone source {}: {err}", src.display()))
		})?;
		let dst_file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(dst)
			.map_err(|err| {
				IsoError::other(format!("create block-clone destination {}: {err}", dst.display()))
			})?;
		dst_file
			.set_len(len)
			.map_err(|err| IsoError::other(format!("set_len {} to {len}: {err}", dst.display())))?;

		if len != 0 {
			duplicate_extents(&src_file, &dst_file, len, src, dst)?;
		}
		Ok(())
	}

	fn duplicate_extents(
		src_file: &File,
		dst_file: &File,
		len: u64,
		src: &Path,
		dst: &Path,
	) -> IsoResult<()> {
		let byte_count = i64::try_from(len).map_err(|_| {
			IsoError::other(format!("{} is too large for Windows block clone", src.display()))
		})?;
		let data = DUPLICATE_EXTENTS_DATA {
			FileHandle:       src_file.as_raw_handle() as _,
			SourceFileOffset: 0,
			TargetFileOffset: 0,
			ByteCount:        byte_count,
		};
		let mut returned = 0u32;
		let in_size = u32::try_from(std::mem::size_of::<DUPLICATE_EXTENTS_DATA>())
			.expect("DUPLICATE_EXTENTS_DATA size fits u32");

		// SAFETY: `dst_file` and `src_file` own valid handles for the duration of
		// the call. `data` points to an initialized DUPLICATE_EXTENTS_DATA buffer,
		// and no output buffer is required by FSCTL_DUPLICATE_EXTENTS_TO_FILE.
		let ok = unsafe {
			DeviceIoControl(
				dst_file.as_raw_handle() as _,
				FSCTL_DUPLICATE_EXTENTS_TO_FILE,
				&raw const data as *const _,
				in_size,
				std::ptr::null_mut(),
				0,
				&raw mut returned,
				std::ptr::null_mut(),
			)
		};
		if ok != 0 {
			return Ok(());
		}

		let err = io::Error::last_os_error();
		if is_unavailable_error(&err) {
			Err(IsoError::unavailable(format!(
				"Windows block clone unsupported for {} -> {}: {err}",
				src.display(),
				dst.display()
			)))
		} else {
			Err(IsoError::other(format!(
				"FSCTL_DUPLICATE_EXTENTS_TO_FILE {} -> {}: {err}",
				src.display(),
				dst.display()
			)))
		}
	}

	fn is_unavailable_error(err: &io::Error) -> bool {
		let Some(code) = err.raw_os_error() else {
			return false;
		};
		let code = code as u32;
		matches!(
			code,
			ERROR_INVALID_FUNCTION
				| ERROR_NOT_SUPPORTED
				| ERROR_NOT_SAME_DEVICE
				| ERROR_INVALID_PARAMETER
				| ERROR_ACCESS_DENIED
		)
	}

	fn copy_metadata_best_effort(src: &Path, dst: &Path) {
		let Ok(meta) = fs::symlink_metadata(src) else {
			return;
		};
		set_times_best_effort(dst, &meta);
		if !meta.file_type().is_symlink() {
			let _ = fs::set_permissions(dst, meta.permissions());
		}
	}

	fn set_times_best_effort(path: &Path, meta: &fs::Metadata) {
		let created = meta.created().ok().and_then(system_time_to_filetime);
		let accessed = meta.accessed().ok().and_then(system_time_to_filetime);
		let modified = meta.modified().ok().and_then(system_time_to_filetime);
		if created.is_none() && accessed.is_none() && modified.is_none() {
			return;
		}

		let mut opts = OpenOptions::new();
		opts.write(true);
		opts.custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
		let Ok(file) = opts.open(path) else { return };
		// SAFETY: `file` owns the HANDLE for the duration of the call. The optional
		// FILETIME pointers either reference stack locals that outlive the call or
		// are null when the corresponding timestamp is unavailable.
		let _ = unsafe {
			SetFileTime(
				file.as_raw_handle() as _,
				created
					.as_ref()
					.map_or(std::ptr::null(), |ft| ft as *const FILETIME),
				accessed
					.as_ref()
					.map_or(std::ptr::null(), |ft| ft as *const FILETIME),
				modified
					.as_ref()
					.map_or(std::ptr::null(), |ft| ft as *const FILETIME),
			)
		};
	}

	fn system_time_to_filetime(time: std::time::SystemTime) -> Option<FILETIME> {
		let dur = time.duration_since(std::time::UNIX_EPOCH).ok()?;
		// Windows FILETIME = 100-ns ticks since 1601-01-01.
		const EPOCH_DIFF_100NS: u64 = 116_444_736_000_000_000;
		let ticks = EPOCH_DIFF_100NS
			.checked_add(dur.as_secs().checked_mul(10_000_000)?)?
			.checked_add(u64::from(dur.subsec_nanos() / 100))?;
		Some(FILETIME {
			dwLowDateTime:  (ticks & 0xffff_ffff) as u32,
			dwHighDateTime: (ticks >> 32) as u32,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::fs;

	use super::nearest_existing_ancestor;

	#[test]
	fn nearest_existing_ancestor_returns_self_and_walks_up() {
		let root = std::env::temp_dir().join(format!("pi-iso-ancestor-{}", std::process::id()));
		fs::create_dir_all(&root).expect("create temp dir");
		assert_eq!(nearest_existing_ancestor(&root), root);
		assert_eq!(nearest_existing_ancestor(&root.join("missing").join("deeper")), root);
		fs::remove_dir_all(&root).expect("remove temp dir");
	}
}
