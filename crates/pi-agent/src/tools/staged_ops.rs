//! Session-scoped staged workspace file operations with explicit apply.
//!
//! Tools (`write`, `edit`, `ast_edit`) stage operations here as a [`FileOpsAction`]
//! instead of mutating the workspace directly. A later write to `xd://resolve`
//! executes the staged operations using descriptor-relative operations
//! (`openat`, `fstatat`, `unlinkat`, `linkat`, `renameat2`) pinned to held
//! directory and file descriptors under `WorkspaceWriter` leases.
//!
//! # POSIX Semantics and Residual Race Windows
//!
//! Atomic multi-file integrity, filesystem-level CAS, or kernel transactional guarantees
//! are explicitly **not** claimed. Standard POSIX filesystem semantics lack atomic conditional
//! unlink (`unlink_if_unchanged`) and fd-to-name publication (`frenameat`/`flinkat`).
//!
//! Two residual windows inherent to POSIX are mitigated but cannot be eliminated against
//! concurrent processes operating with the same UID and write access to the directory:
//! 1. **Named-temp check-to-publication race**: Between verifying `dir_pin.stat_file(&temp_name) == temp_dev_ino`
//!    and the kernel `linkat` / `renameat` call, an uncooperating process could unlink and replace `temp_name`.
//! 2. **Named-temp cleanup race**: Between `dir_pin.stat_file(&temp_name) == expected_dev_ino` and `unlinkat_file`,
//!    an uncooperating process could substitute the temporary entry.
//!
//! The design narrows these windows to sub-microsecond descriptor-pinned checks and fails closed upon
//! any discrepancy, but absolute transactional isolation without OS kernel sandboxing/namespaces is impossible.
use std::{
	ffi::CString,
	io::{Read, Seek, SeekFrom, Write},
	path::{Path, PathBuf},
	sync::{Arc, Mutex},
};

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::{
	state::{AgentContext, ToolResult},
	tool::{ToolError, tool_result},
	tools::{
		hashline::{SnapshotStore, canonical_key, format_header},
		pending::{ActionTier, PendingAction},
		read::normalize_snapshot_text,
	},
	workspace_writer::{canonical_identity, WorkspaceWriter},
};

/// Operation kind for a staged file change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
	Create,
	Update,
	Delete,
	Move,
}

impl OpKind {
	#[allow(dead_code)]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Create => "create",
			Self::Update => "update",
			Self::Delete => "delete",
			Self::Move => "move",
		}
	}
}

#[cfg(unix)]
pub fn dev_ino(metadata: &std::fs::Metadata) -> (u64, u64) {
	use std::os::unix::fs::MetadataExt;
	(metadata.dev(), metadata.ino())
}

#[cfg(not(unix))]
pub fn dev_ino(_metadata: &std::fs::Metadata) -> (u64, u64) {
	(0, 0)
}

#[cfg(unix)]
pub fn file_mode(metadata: &std::fs::Metadata) -> u32 {
	use std::os::unix::fs::MetadataExt;
	metadata.mode() & 0o7777
}

#[cfg(not(unix))]
pub fn file_mode(_metadata: &std::fs::Metadata) -> u32 {
	0o600
}

fn to_cstring(s: &str) -> Result<CString, String> {
	CString::new(s).map_err(|_| "path contains NUL byte".to_string())
}

/// A held directory descriptor that pins a parent directory against symlink retargeting attacks.
pub struct DirPin {
	pub path: PathBuf,
	pub dev_ino: Option<(u64, u64)>,
	#[cfg(unix)]
	pub fd: std::os::unix::io::RawFd,
}

#[cfg(unix)]
impl Drop for DirPin {
	fn drop(&mut self) {
		if self.fd >= 0 {
			unsafe { libc::close(self.fd); }
		}
	}
}

impl DirPin {
	pub fn try_clone(&self) -> Result<Self, String> {
		#[cfg(unix)]
		{
			let new_fd = unsafe { libc::fcntl(self.fd, libc::F_DUPFD_CLOEXEC, 0) };
			if new_fd < 0 {
				return Err(format!("dup directory descriptor failed: {}", std::io::Error::last_os_error()));
			}
			Ok(Self {
				path: self.path.clone(),
				dev_ino: self.dev_ino,
				fd: new_fd,
			})
		}
		#[cfg(not(unix))]
		{
			Self::open(&self.path)
		}
	}
	pub fn open(dir_path: &Path) -> Result<Self, String> {
		let canonical = canonical_identity(dir_path);
		#[cfg(unix)]
		{
			use std::os::unix::ffi::OsStrExt;
			let c_path = CString::new(canonical.as_os_str().as_bytes())
				.map_err(|_| format!("invalid path: {}", canonical.display()))?;
			let fd = unsafe {
				libc::open(
					c_path.as_ptr(),
					libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
				)
			};
			if fd < 0 {
				return Err(format!(
					"cannot open parent directory descriptor for {}: {}",
					canonical.display(),
					std::io::Error::last_os_error()
				));
			}
			let mut stat: libc::stat = unsafe { std::mem::zeroed() };
			if unsafe { libc::fstat(fd, &mut stat) } != 0 {
				let err = std::io::Error::last_os_error();
				unsafe { libc::close(fd); }
				return Err(format!("fstat directory {}: {err}", canonical.display()));
			}
			let dev_ino_val = (stat.st_dev as u64, stat.st_ino as u64);
			Ok(Self { path: canonical, dev_ino: Some(dev_ino_val), fd })
		}
		#[cfg(not(unix))]
		{
			let meta = std::fs::metadata(&canonical)
				.map_err(|e| format!("cannot stat directory {}: {e}", canonical.display()))?;
			let dev_ino_val = dev_ino(&meta);
			Ok(Self { path: canonical, dev_ino: Some(dev_ino_val) })
		}
	}

	pub fn verify_against(&self, expected_path: &Path, expected_dev_ino: Option<(u64, u64)>) -> Result<(), String> {
		if self.path != expected_path {
			return Err(format!(
				"directory identity changed (symlink retarget detected: expected {}, got {})",
				expected_path.display(),
				self.path.display()
			));
		}
		if let Some(expected) = expected_dev_ino {
			if self.dev_ino != Some(expected) {
				return Err(format!(
					"directory device/inode changed for {}: expected {expected:?}, got {:?}",
					self.path.display(),
					self.dev_ino
				));
			}
		}
		Ok(())
	}

	#[cfg(unix)]
	pub fn stat_file(&self, file_name: &str) -> std::io::Result<Option<(u64, u64)>> {
		let c_name = to_cstring(file_name).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
		let mut stat: libc::stat = unsafe { std::mem::zeroed() };
		let res = unsafe { libc::fstatat(self.fd, c_name.as_ptr(), &mut stat, libc::AT_SYMLINK_NOFOLLOW) };
		if res == 0 {
			Ok(Some((stat.st_dev as u64, stat.st_ino as u64)))
		} else {
			let err = std::io::Error::last_os_error();
			if err.kind() == std::io::ErrorKind::NotFound {
				Ok(None)
			} else {
				Err(err)
			}
		}
	}

	#[cfg(not(unix))]
	pub fn stat_file(&self, file_name: &str) -> std::io::Result<Option<(u64, u64)>> {
		match std::fs::symlink_metadata(self.path.join(file_name)) {
			Ok(m) => Ok(Some(dev_ino(&m))),
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
			Err(e) => Err(e),
		}
	}

	#[cfg(unix)]
	pub fn unlinkat_file(&self, file_name: &str) -> std::io::Result<()> {
		let c_name = to_cstring(file_name).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
		let res = unsafe { libc::unlinkat(self.fd, c_name.as_ptr(), 0) };
		if res == 0 {
			Ok(())
		} else {
			Err(std::io::Error::last_os_error())
		}
	}

	#[cfg(not(unix))]
	pub fn unlinkat_file(&self, file_name: &str) -> std::io::Result<()> {
		std::fs::remove_file(self.path.join(file_name))
	}
}

fn get_crypto_random_bytes(buf: &mut [u8]) -> Result<(), String> {
	#[cfg(unix)]
	{
		// 1. Try SYS_getrandom with loop handling EINTR and short reads
		let mut offset = 0;
		while offset < buf.len() {
			let res = unsafe {
				libc::syscall(
					libc::SYS_getrandom,
					buf[offset..].as_mut_ptr() as *mut libc::c_void,
					buf.len() - offset,
					0,
				)
			};
			if res > 0 {
				offset += res as usize;
			} else if res < 0 {
				let err = std::io::Error::last_os_error();
				if err.raw_os_error() == Some(libc::EINTR) {
					continue;
				}
				break;
			} else {
				break;
			}
		}
		if offset == buf.len() {
			return Ok(());
		}

		// 2. Fallback to /dev/urandom with read_exact
		if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
			if f.read_exact(buf).is_ok() {
				return Ok(());
			}
		}
	}
	#[cfg(unix)]
	return Err("cryptographic entropy source unavailable (SYS_getrandom and /dev/urandom failed); failing closed".to_string());
	#[cfg(not(unix))]
	Err("cryptographic entropy source unsupported on non-Unix platform; failing closed".to_string())
}

fn generate_temp_name(file_name: &str) -> Result<String, String> {
	let mut random_bytes = [0u8; 16];
	get_crypto_random_bytes(&mut random_bytes)?;
	let hex: String = random_bytes.iter().map(|b| format!("{b:02x}")).collect();
	Ok(format!(".{file_name}.tmp-{hex}"))
}

/// RAII temp file cleanup guard for descriptor-relative operations.
/// Armed IMMEDIATELY upon openat(O_CREAT | O_EXCL) success so any failure during
/// subsequent fchmod or fstat unlinks the newly created temp file.
/// Verifies leaf entry inode before unlink if dev_ino was recorded.
struct TempDescriptorGuard<'a> {
	dir_pin: &'a DirPin,
	temp_info: Option<(String, Option<(u64, u64)>)>,
	cleanup_warnings: Arc<Mutex<Vec<String>>>,
}

impl<'a> TempDescriptorGuard<'a> {
	fn new(dir_pin: &'a DirPin, cleanup_warnings: Arc<Mutex<Vec<String>>>) -> Self {
		Self {
			dir_pin,
			temp_info: None,
			cleanup_warnings,
		}
	}

	/// Arm immediately upon openat(O_CREAT | O_EXCL) success.
	fn arm(&mut self, temp_name: String) {
		self.temp_info = Some((temp_name, None));
	}

	/// Record verified inode once fstat succeeds.
	fn set_dev_ino(&mut self, dev_ino: (u64, u64)) {
		if let Some((_, dev_ino_slot)) = self.temp_info.as_mut() {
			*dev_ino_slot = Some(dev_ino);
		}
	}

	fn cleanup_now(&mut self) {
		if let Some((temp_name, expected_dev_ino_opt)) = self.temp_info.take() {
			// NOTE on residual POSIX named-temp cleanup race:
			// POSIX lacks a descriptor-relative `unlink_by_fd(dirfd, file_fd)`. The kernel `unlinkat(dirfd, name, 0)`
			// syscall resolves the path string `name` relative to `dirfd`.
			// We verify that `dir_pin.stat_file(&temp_name) == expected_dev_ino` immediately before `unlinkat`.
			// However, an uncooperative concurrent process operating under the same UID with write access to the
			// directory could theoretically replace the entry in the sub-microsecond gap between `stat_file` and
			// `unlinkat`. True transactional multi-file atomicity is not claimed.
			if let Some(expected_dev_ino) = expected_dev_ino_opt {
				match self.dir_pin.stat_file(&temp_name) {
					Ok(Some(current_dev_ino)) => {
						if current_dev_ino != expected_dev_ino {
							self.cleanup_warnings.lock().unwrap().push(format!(
								"temp file {temp_name} inode mismatch (expected {expected_dev_ino:?}, found {current_dev_ino:?}); aborting unlink to avoid deleting foreign file"
							));
							return;
						}
					}
					Ok(None) => {
						// Entry already unlinked or consumed by renameat; nothing to remove.
						return;
					}
					Err(err) => {
						self.cleanup_warnings.lock().unwrap().push(format!("stat temp file {temp_name} failed before cleanup: {err}"));
						return;
					}
				}
			}

			match self.dir_pin.unlinkat_file(&temp_name) {
				Ok(()) => {},
				// After renameat, temp_name was consumed by the kernel; ENOENT is expected and not an error.
				Err(err) if err.kind() == std::io::ErrorKind::NotFound => {},
				Err(err) => {
					self.cleanup_warnings.lock().unwrap().push(format!("failed to unlink temp {temp_name}: {err}"));
				}
			}
		}
	}
}

impl<'a> Drop for TempDescriptorGuard<'a> {
	fn drop(&mut self) {
		self.cleanup_now();
	}
}

/// Create a temporary file with mode 0600 inside the pinned directory.
/// Holds mode 0600 throughout intermediate writing to prevent access leak.
fn create_temp_file<'a>(
	dir_pin: &'a DirPin,
	file_name: &str,
	cleanup_warnings: &Arc<Mutex<Vec<String>>>,
) -> Result<(std::fs::File, String, (u64, u64), TempDescriptorGuard<'a>), String> {
	let mut temp_guard = TempDescriptorGuard::new(dir_pin, Arc::clone(cleanup_warnings));
	for _ in 0..10 {
		let candidate_name = generate_temp_name(file_name)?;
		#[cfg(unix)]
		{
			let c_temp = to_cstring(&candidate_name)?;
			let fd = unsafe {
				libc::openat(
					dir_pin.fd,
					c_temp.as_ptr(),
					libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC,
					0o600 as libc::mode_t,
				)
			};
			if fd >= 0 {
				// CRITICAL: Arm guard IMMEDIATELY after openat succeeds, so any subsequent failure
				// in fchmod or fstat will unlink the newly created temp file!
				temp_guard.arm(candidate_name.clone());

				if unsafe { libc::fchmod(fd, 0o600 as libc::mode_t) } != 0 {
					let err = std::io::Error::last_os_error();
					unsafe { libc::close(fd); }
					temp_guard.cleanup_now();
					return Err(format!("fchmod 0600 temp file failed: {err}"));
				}
				let mut stat: libc::stat = unsafe { std::mem::zeroed() };
				if unsafe { libc::fstat(fd, &mut stat) } != 0 {
					let err = std::io::Error::last_os_error();
					unsafe { libc::close(fd); }
					temp_guard.cleanup_now();
					return Err(format!("fstat temp file failed: {err}"));
				}
				let dev_ino = (stat.st_dev as u64, stat.st_ino as u64);
				temp_guard.set_dev_ino(dev_ino);

				use std::os::unix::io::FromRawFd;
				let file = unsafe { std::fs::File::from_raw_fd(fd) };
				return Ok((file, candidate_name, dev_ino, temp_guard));
			} else {
				let err = std::io::Error::last_os_error();
				if err.raw_os_error() == Some(libc::EEXIST) {
					continue;
				} else {
					return Err(format!("openat temp file failed: {err}"));
				}
			}
		}
		#[cfg(not(unix))]
		{
			use std::fs::OpenOptions;
			match OpenOptions::new().read(true).write(true).create_new(true).open(dir_pin.path.join(&candidate_name)) {
				Ok(file) => {
					temp_guard.arm(candidate_name.clone());
					return Ok((file, candidate_name, (0, 0), temp_guard));
				}
				Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
				Err(e) => return Err(format!("create temp file failed: {e}")),
			}
		}
	}
	Err("could not create unique temporary file after 10 attempts".to_string())
}

pub fn nearest_existing_ancestor(path: &Path) -> (Option<PathBuf>, Option<(u64, u64)>, Vec<String>) {
	let parent = path.parent().unwrap_or_else(|| Path::new("."));
	let mut current = parent;
	let mut subdirs = Vec::new();
	loop {
		if let Ok(pin) = DirPin::open(current) {
			subdirs.reverse();
			return (Some(pin.path.clone()), pin.dev_ino, subdirs);
		}
		if let Some(name) = current.file_name().and_then(|n| n.to_str()) {
			subdirs.push(name.to_string());
			if let Some(p) = current.parent() {
				if p == current || p == Path::new("") {
					break;
				}
				current = p;
			} else {
				break;
			}
		} else {
			break;
		}
	}
	let dot = Path::new(".");
	if let Ok(pin) = DirPin::open(dot) {
		subdirs.reverse();
		return (Some(pin.path.clone()), pin.dev_ino, subdirs);
	}
	let canonical = canonical_identity(parent);
	let d_i = std::fs::symlink_metadata(&canonical).ok().map(|m| dev_ino(&m));
	(Some(canonical), d_i, Vec::new())
}

fn open_or_create_subdirs(
	ancestor_pin: &DirPin,
	subdirs: &[String],
	created_dirs: &mut Vec<String>,
) -> Result<DirPin, String> {
	let mut current_pin = ancestor_pin.try_clone()?;
	for subdir in subdirs {
		current_pin = open_or_create_subdir(&current_pin, subdir, created_dirs)?;
	}
	Ok(current_pin)
}

fn open_or_create_subdir(
	parent_pin: &DirPin,
	component: &str,
	created_dirs: &mut Vec<String>,
) -> Result<DirPin, String> {
	#[cfg(unix)]
	{
		let c_name = to_cstring(component)?;
		// 1. Try to open existing subdir with O_NOFOLLOW
		let fd = unsafe {
			libc::openat(
				parent_pin.fd,
				c_name.as_ptr(),
				libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
			)
		};
		if fd >= 0 {
			let mut stat: libc::stat = unsafe { std::mem::zeroed() };
			if unsafe { libc::fstat(fd, &mut stat) } != 0 {
				let err = std::io::Error::last_os_error();
				unsafe { libc::close(fd); }
				return Err(format!("fstat {component} failed: {err}"));
			}
			let dev_ino = Some((stat.st_dev as u64, stat.st_ino as u64));
			let sub_path = parent_pin.path.join(component);
			return Ok(DirPin {
				path: sub_path,
				dev_ino,
				fd,
			});
		}
		let err = std::io::Error::last_os_error();
		if err.raw_os_error() == Some(libc::ENOENT) {
			// 2. Create directory descriptor-relatively!
			let mkdir_res = unsafe { libc::mkdirat(parent_pin.fd, c_name.as_ptr(), 0o755) };
			let sub_path = parent_pin.path.join(component);
			if mkdir_res == 0 {
				created_dirs.push(sub_path.display().to_string());
			} else {
				let mkdir_err = std::io::Error::last_os_error();
				if mkdir_err.raw_os_error() != Some(libc::EEXIST) {
					return Err(format!("mkdirat {component} failed: {mkdir_err}"));
				}
				// EEXIST means someone else created it concurrently; do not claim it.
			}

			// 3. Open the newly created directory with O_NOFOLLOW
			let fd = unsafe {
				libc::openat(
					parent_pin.fd,
					c_name.as_ptr(),
					libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
				)
			};
			if fd < 0 {
				return Err(format!("openat newly created directory {component} failed: {}", std::io::Error::last_os_error()));
			}
			let mut stat: libc::stat = unsafe { std::mem::zeroed() };
			if unsafe { libc::fstat(fd, &mut stat) } != 0 {
				let err = std::io::Error::last_os_error();
				unsafe { libc::close(fd); }
				return Err(format!("fstat {component} failed: {err}"));
			}
			let dev_ino = Some((stat.st_dev as u64, stat.st_ino as u64));
			return Ok(DirPin {
				path: sub_path,
				dev_ino,
				fd,
			});
		}
		Err(format!("openat {component} failed: {err}"))
	}
	#[cfg(not(unix))]
	{
		let sub_path = parent_pin.path.join(component);
		if !sub_path.exists() {
			std::fs::create_dir(&sub_path).map_err(|e| e.to_string())?;
			created_dirs.push(sub_path.display().to_string());
		}
		DirPin::open(&sub_path)
	}
}

/// A single prepared file operation with its captured base version and directory identity.
#[derive(Debug, Clone)]
pub struct PreparedOp {
	pub kind: OpKind,
	pub path: PathBuf,
	pub display: String,
	/// Exact raw bytes of the file at staging time. `None` for Create.
	pub base: Option<Vec<u8>>,
	/// Inode identity (dev, ino) of the file at staging time. `None` for Create.
	pub base_dev_ino: Option<(u64, u64)>,
	/// File permissions mode at staging time. `None` for Create.
	pub base_mode: Option<u32>,
	/// Canonical path of the nearest existing parent directory at staging time.
	pub parent_identity: Option<PathBuf>,
	/// Inode identity (dev, ino) of the nearest existing parent directory at staging time.
	pub parent_dev_ino: Option<(u64, u64)>,
	/// Subdirectory components to traverse/create under parent_identity.
	pub parent_subdirs: Vec<String>,
	/// Content to write. Empty for Delete.
	pub content: Vec<u8>,
	/// Explicit Unix mode permissions, if requested or preserved from metadata.
	pub mode: Option<u32>,
	/// Rename target path for Move operations.
	pub rename_to: Option<PathBuf>,
	/// Rename target display string.
	pub rename_to_display: Option<String>,
	/// Canonical path of rename target nearest existing parent directory at staging time.
	pub rename_parent_identity: Option<PathBuf>,
	/// Inode identity of rename target nearest existing parent directory at staging time.
	pub rename_parent_dev_ino: Option<(u64, u64)>,
	/// Subdirectory components to traverse/create under rename_parent_identity.
	pub rename_parent_subdirs: Vec<String>,
	/// Per-operation status / description line (e.g. "Updated note.txt (first changed line 2)", "Deleted gone.txt", "Moved a -> b")
	pub description: Option<String>,
}

impl PreparedOp {
	pub fn new_create(path: PathBuf, display: String, content: Vec<u8>, mode: Option<u32>) -> Self {
		let (parent_identity, parent_dev_ino, parent_subdirs) = nearest_existing_ancestor(&path);
		Self {
			kind: OpKind::Create,
			path,
			display,
			base: None,
			base_dev_ino: None,
			base_mode: None,
			parent_identity,
			parent_dev_ino,
			parent_subdirs,
			content,
			mode,
			rename_to: None,
			rename_to_display: None,
			rename_parent_identity: None,
			rename_parent_dev_ino: None,
			rename_parent_subdirs: Vec::new(),
			description: None,
		}
	}

	pub fn new_update(
		path: PathBuf,
		display: String,
		base: Vec<u8>,
		base_dev_ino: Option<(u64, u64)>,
		content: Vec<u8>,
		mode: Option<u32>,
	) -> Self {
		let (parent_identity, parent_dev_ino, parent_subdirs) = nearest_existing_ancestor(&path);
		let base_mode = std::fs::symlink_metadata(&path).ok().map(|m| file_mode(&m));
		Self {
			kind: OpKind::Update,
			path,
			display,
			base: Some(base),
			base_dev_ino,
			base_mode,
			parent_identity,
			parent_dev_ino,
			parent_subdirs,
			content,
			mode,
			rename_to: None,
			rename_to_display: None,
			rename_parent_identity: None,
			rename_parent_dev_ino: None,
			rename_parent_subdirs: Vec::new(),
			description: None,
		}
	}

	pub fn new_delete(
		path: PathBuf,
		display: String,
		base: Option<Vec<u8>>,
		base_dev_ino: Option<(u64, u64)>,
	) -> Self {
		let (parent_identity, parent_dev_ino, parent_subdirs) = nearest_existing_ancestor(&path);
		let base_mode = std::fs::symlink_metadata(&path).ok().map(|m| file_mode(&m));
		Self {
			kind: OpKind::Delete,
			path,
			display,
			base,
			base_dev_ino,
			base_mode,
			parent_identity,
			parent_dev_ino,
			parent_subdirs,
			content: Vec::new(),
			mode: None,
			rename_to: None,
			rename_to_display: None,
			rename_parent_identity: None,
			rename_parent_dev_ino: None,
			rename_parent_subdirs: Vec::new(),
			description: None,
		}
	}

	pub fn new_move(
		source_path: PathBuf,
		source_display: String,
		dest_path: PathBuf,
		dest_display: String,
		base: Vec<u8>,
		base_dev_ino: Option<(u64, u64)>,
		content: Vec<u8>,
		mode: Option<u32>,
	) -> Self {
		let (parent_identity, parent_dev_ino, parent_subdirs) = nearest_existing_ancestor(&source_path);
		let (dest_parent_id, dest_parent_dev_ino, dest_parent_subdirs) = nearest_existing_ancestor(&dest_path);
		let base_mode = std::fs::symlink_metadata(&source_path).ok().map(|m| file_mode(&m));
		Self {
			kind: OpKind::Move,
			path: source_path,
			display: source_display,
			base: Some(base),
			base_dev_ino,
			base_mode,
			parent_identity,
			parent_dev_ino,
			parent_subdirs,
			content,
			mode,
			rename_to: Some(dest_path),
			rename_to_display: Some(dest_display),
			rename_parent_identity: dest_parent_id,
			rename_parent_dev_ino: dest_parent_dev_ino,
			rename_parent_subdirs: dest_parent_subdirs,
			description: None,
		}
	}

	pub fn with_description(mut self, desc: impl Into<String>) -> Self {
		self.description = Some(desc.into());
		self
	}

	#[allow(dead_code)]
	pub fn with_base_mode(mut self, base_mode: u32) -> Self {
		self.base_mode = Some(base_mode);
		self
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpStatus {
	Pending,
	DestPublished {
		dest_dev_ino: (u64, u64),
	},
	Completed,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct AttemptWrites {
	pub written: Vec<String>,
	pub invalidated: Vec<String>,
}

/// A staged batch of file operations implementing [`PendingAction`].
pub struct FileOpsAction {
	source_tool: String,
	label: String,
	ops: Vec<PreparedOp>,
	snapshots: Arc<SnapshotStore>,
	op_statuses: Arc<Mutex<Vec<OpStatus>>>,
	cumulative_written: Arc<Mutex<Vec<String>>>,
	cumulative_invalidated: Arc<Mutex<Vec<String>>>,
	created_dirs: Arc<Mutex<Vec<String>>>,
	cleanup_warnings: Arc<Mutex<Vec<String>>>,
	conflict_ids: Vec<u64>,
	extra_details: Option<Value>,
}

impl FileOpsAction {
	pub fn new(
		source_tool: impl Into<String>,
		label: impl Into<String>,
		ops: Vec<PreparedOp>,
		snapshots: Arc<SnapshotStore>,
	) -> Self {
		let op_count = ops.len();
		Self {
			source_tool: source_tool.into(),
			label: label.into(),
			ops,
			snapshots,
			op_statuses: Arc::new(Mutex::new(vec![OpStatus::Pending; op_count])),
			cumulative_written: Arc::new(Mutex::new(Vec::new())),
			cumulative_invalidated: Arc::new(Mutex::new(Vec::new())),
			created_dirs: Arc::new(Mutex::new(Vec::new())),
			cleanup_warnings: Arc::new(Mutex::new(Vec::new())),
			conflict_ids: Vec::new(),
			extra_details: None,
		}
	}

	pub fn with_conflict_ids(mut self, ids: Vec<u64>) -> Self {
		self.conflict_ids = ids;
		self
	}

	#[allow(dead_code)]
	pub fn attempts(&self) -> AttemptWrites {
		AttemptWrites {
			written: self.cumulative_written.lock().unwrap().clone(),
			invalidated: self.cumulative_invalidated.lock().unwrap().clone(),
		}
	}

	pub fn has_any_partial_effects(&self) -> bool {
		!self.cumulative_written.lock().unwrap().is_empty()
			|| !self.cumulative_invalidated.lock().unwrap().is_empty()
			|| !self.created_dirs.lock().unwrap().is_empty()
	}

	pub fn with_extra_details(mut self, extra: Value) -> Self {
		self.extra_details = Some(extra);
		self
	}

	pub fn ops(&self) -> &[PreparedOp] {
		&self.ops
	}

	fn partial_result(
		&self,
		message: String,
		attempt_written: &[String],
		attempt_invalidated: &[String],
		attempt_created_dirs: &[String],
		fresh_tags: &[String],
		partial: bool,
		stale: bool,
		failed_target: Option<String>,
		cleanup_warnings: Vec<String>,
	) -> ToolResult {
		let cum_written = self.cumulative_written.lock().unwrap().clone();
		let cum_invalidated = self.cumulative_invalidated.lock().unwrap().clone();
		let cum_created_dirs = self.created_dirs.lock().unwrap().clone();
		let mut all_cleanup_warnings = self.cleanup_warnings.lock().unwrap().clone();
		for w in cleanup_warnings {
			if !all_cleanup_warnings.contains(&w) {
				all_cleanup_warnings.push(w);
			}
		}
		let is_partial = partial || self.has_any_partial_effects() || !attempt_written.is_empty() || !attempt_created_dirs.is_empty();
		let is_stale = stale || (!cum_written.is_empty() && attempt_written.is_empty());

		let mut text = message;
		if attempt_written.is_empty() && !cum_written.is_empty() {
			text.push_str("\nthis attempt applied nothing.\nPreviously written by this staged action: ");
			text.push_str(&cum_written.join(", "));
		} else if !cum_written.is_empty() {
			text.push_str("\nWritten: ");
			text.push_str(&cum_written.join(", "));
		}
		if !cum_invalidated.is_empty() {
			text.push_str("\nSnapshot invalidations: ");
			text.push_str(&cum_invalidated.join(", "));
		}
		if !cum_created_dirs.is_empty() {
			text.push_str("\nCreated directories: ");
			text.push_str(&cum_created_dirs.join(", "));
		}
		if !all_cleanup_warnings.is_empty() {
			text.push_str("\nCleanup warnings: ");
			text.push_str(&all_cleanup_warnings.join(", "));
		}

		let statuses = self.op_statuses.lock().unwrap().clone();
		let total_ops = self.ops.len();
		let landed_ops: Vec<String> = self.ops.iter().enumerate()
			.filter(|(idx, _)| statuses[*idx] == OpStatus::Completed)
			.map(|(_, op)| {
				if let Some(d) = &op.description {
					d.clone()
				} else {
					match op.kind {
						OpKind::Create => format!("Created {}", op.display),
						OpKind::Update => format!("Updated {}", op.display),
						OpKind::Delete => format!("Deleted {}", op.display),
						OpKind::Move => format!("Moved {} -> {}", op.display, op.rename_to_display.as_deref().unwrap_or("dest")),
					}
				}
			})
			.collect();
		if !landed_ops.is_empty() {
			text.push_str("\n\nAlready written by this call:");
			for line in &landed_ops {
				text.push_str("\n- ");
				text.push_str(line);
			}
			let not_written = total_ops.saturating_sub(landed_ops.len());
			if not_written > 0 {
				text.push_str(&format!(
					"\n\n{not_written} section(s) starting with the failing one did not complete; any partial \
					 mutation the message above reports still stands. Re-read their current content before retrying."
				));
			}
		}

		let mut details = json!({
			"stale": is_stale,
			"partial": is_partial,
			"writtenFiles": cum_written,
			"attemptWrittenFiles": attempt_written,
			"snapshotInvalidations": cum_invalidated,
			"attemptSnapshotInvalidations": attempt_invalidated,
			"createdDirectories": cum_created_dirs,
			"attemptCreatedDirectories": attempt_created_dirs,
			"freshTags": fresh_tags,
		});
		if let Some(target) = failed_target {
			details["failedFile"] = json!(target.clone());
			details["failedTarget"] = json!(target);
		}
		if !all_cleanup_warnings.is_empty() {
			details["cleanupWarnings"] = json!(all_cleanup_warnings);
		}
		if let Some(extra) = &self.extra_details {
			if let Some(obj) = details.as_object_mut() {
				if let Some(extra_obj) = extra.as_object() {
					for (k, v) in extra_obj {
						obj.insert(k.clone(), v.clone());
					}
				}
			}
		}
		let mut res = tool_result(text, true);
		res.details = Some(details);
		res
	}

	/// Validate parent directory pinning using held directory descriptors.
	/// Strictly READ-ONLY. No directories are created during Gate 1.
	fn check_parent_pin(op: &PreparedOp) -> Result<(), String> {
		if let Some(expected_ancestor) = &op.parent_identity {
			let pin = DirPin::open(expected_ancestor).map_err(|e| format!("{}: {e}", op.display))?;
			pin.verify_against(expected_ancestor, op.parent_dev_ino)
				.map_err(|e| format!("{}: {e}", op.display))?;
		}

		if op.kind == OpKind::Move {
			if let Some(_dest) = &op.rename_to {
				if let Some(expected_dest_ancestor) = &op.rename_parent_identity {
					let dest_pin = DirPin::open(expected_dest_ancestor)
						.map_err(|e| format!("destination for {}: {e}", op.display))?;
					dest_pin.verify_against(expected_dest_ancestor, op.rename_parent_dev_ino)
						.map_err(|e| format!("destination for {}: {e}", op.display))?;
				}
			}
		}
		Ok(())
	}

	/// Re-verify file identity and content immediately before mutating.
	async fn check_file_identity(op: &PreparedOp) -> Result<(), String> {
		match op.kind {
			OpKind::Create => {
				if tokio::fs::symlink_metadata(&op.path).await.is_ok() {
					return Err(format!("{}: file already exists", op.display));
				}
			}
			OpKind::Update => {
				let meta = tokio::fs::symlink_metadata(&op.path)
					.await
					.map_err(|e| format!("{}: {e}", op.display))?;
				if let Some(expected_dev_ino) = op.base_dev_ino {
					if dev_ino(&meta) != expected_dev_ino {
						return Err(format!(
							"{}: file identity (dev/ino) changed since preview",
							op.display
						));
					}
				}
				if let Some(expected_base_mode) = op.base_mode {
					if file_mode(&meta) != expected_base_mode {
						return Err(format!(
							"{}: file mode changed from 0o{:o} to 0o{:o} since preview",
							op.display,
							expected_base_mode,
							file_mode(&meta),
						));
					}
				}
				let bytes = tokio::fs::read(&op.path)
					.await
					.map_err(|e| format!("{}: {e}", op.display))?;
				if let Some(base) = &op.base {
					if &bytes != base {
						return Err(format!("{}: content changed since preview", op.display));
					}
				}
			}
			OpKind::Delete => {
				let meta = tokio::fs::symlink_metadata(&op.path)
					.await
					.map_err(|e| format!("{}: {e}", op.display))?;
				let ft = meta.file_type();
				if !ft.is_file() && !ft.is_symlink() {
					return Err(format!("{}: not a regular file or symlink", op.display));
				}
				if let Some(expected_dev_ino) = op.base_dev_ino {
					if dev_ino(&meta) != expected_dev_ino {
						return Err(format!(
							"{}: file identity (dev/ino) changed since preview",
							op.display
						));
					}
				}
				if let Some(expected_base_mode) = op.base_mode {
					if file_mode(&meta) != expected_base_mode {
						return Err(format!(
							"{}: file mode changed from 0o{:o} to 0o{:o} since preview",
							op.display,
							expected_base_mode,
							file_mode(&meta),
						));
					}
				}
				if let Some(base) = &op.base {
					let bytes = tokio::fs::read(&op.path)
						.await
						.map_err(|e| format!("{}: {e}", op.display))?;
					if &bytes != base {
						return Err(format!("{}: content changed since preview", op.display));
					}
				}
			}
			OpKind::Move => {
				if let Some(dest) = &op.rename_to {
					if tokio::fs::symlink_metadata(dest).await.is_ok() {
						let dest_name = op.rename_to_display.as_deref().unwrap_or("destination");
						return Err(format!("{dest_name}: destination already exists"));
					}
				}
				let meta = tokio::fs::symlink_metadata(&op.path)
					.await
					.map_err(|e| format!("{}: {e}", op.display))?;
				if let Some(expected_dev_ino) = op.base_dev_ino {
					if dev_ino(&meta) != expected_dev_ino {
						return Err(format!(
							"{}: source identity (dev/ino) changed since preview",
							op.display
						));
					}
				}
				if let Some(expected_base_mode) = op.base_mode {
					if file_mode(&meta) != expected_base_mode {
						return Err(format!(
							"{}: source file mode changed from 0o{:o} to 0o{:o} since preview",
							op.display,
							expected_base_mode,
							file_mode(&meta),
						));
					}
				}
				let bytes = tokio::fs::read(&op.path)
					.await
					.map_err(|e| format!("{}: {e}", op.display))?;
				if let Some(base) = &op.base {
					if &bytes != base {
						return Err(format!("{}: source content changed since preview", op.display));
					}
				}
			}
		}
		Ok(())
	}
}

#[async_trait]
impl PendingAction for FileOpsAction {
	fn label(&self) -> &str {
		&self.label
	}

	fn source_tool(&self) -> &str {
		&self.source_tool
	}

	fn tier(&self) -> Option<ActionTier> {
		Some(ActionTier::Write)
	}

	fn targets(&self) -> Vec<String> {
		let mut list = Vec::new();
		for op in &self.ops {
			list.push(op.display.clone());
			if let Some(dest) = &op.rename_to_display {
				list.push(dest.clone());
			}
		}
		list
	}

	async fn apply(&self, ctx: &AgentContext, _reason: &str) -> Result<ToolResult, ToolError> {
		if ctx.cancellation_token.is_cancelled() {
			return Err(ToolError::Aborted("File operations cancelled".to_string()));
		}

		let statuses = self.op_statuses.lock().unwrap().clone();

		// Gate 1: Check ALL affected files and parent directories before ANY write begins
		let mut gate_failures = Vec::new();
		for (idx, op) in self.ops.iter().enumerate() {
			if ctx.cancellation_token.is_cancelled() {
				return Err(ToolError::Aborted("File operations cancelled".to_string()));
			}
			if statuses[idx] == OpStatus::Completed || matches!(statuses[idx], OpStatus::DestPublished { .. }) {
				continue;
			}
			if let Err(err) = Self::check_parent_pin(op) {
				gate_failures.push(err);
			}
			if let Err(err) = Self::check_file_identity(op).await {
				gate_failures.push(err);
			}
		}

		if !gate_failures.is_empty() {
			return Ok(self.partial_result(
				format!(
					"Preview is stale / no longer matches; this attempt applied nothing.\n{}",
					gate_failures.join("\n")
				),
				&[],
				&[],
				&[],
				&[],
				!self.cumulative_written.lock().unwrap().is_empty(),
				true,
				None,
				Vec::new(),
			));
		}

		let total_ops = self.ops.len();
		let mut attempt_written: Vec<String> = Vec::new();
		let mut attempt_invalidated: Vec<String> = Vec::new();
		let mut attempt_created_dirs: Vec<String> = Vec::new();
		let mut fresh_tags: Vec<String> = Vec::new();
		let mut cleanup_warnings: Vec<String> = Vec::new();

		for (idx, op) in self.ops.iter().enumerate() {
			let current_status = self.op_statuses.lock().unwrap()[idx].clone();
			if current_status == OpStatus::Completed {
				continue;
			}

			if ctx.cancellation_token.is_cancelled() {
				return Ok(self.partial_result(
					format!("Cancelled after applying {} of {total_ops} operations; remaining operations were not performed.", self.cumulative_written.lock().unwrap().len()),
					&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, None, cleanup_warnings,
				));
			}

			// Open held directory descriptor for parent pin throughout the entire operation
			// using descriptor-relative traversal from nearest existing ancestor
			let ancestor_path = op.parent_identity.as_ref().map(|p| p.as_path()).unwrap_or_else(|| Path::new("."));
			let ancestor_pin = match DirPin::open(ancestor_path) {
				Ok(p) => {
					if let Err(e) = p.verify_against(ancestor_path, op.parent_dev_ino) {
						return Ok(self.partial_result(
							format!("Parent directory ancestor verification failed at {}: {e}. {} of {total_ops} operations were performed.", op.display, self.cumulative_written.lock().unwrap().len()),
							&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
						));
					}
					p
				}
				Err(err) => {
					return Ok(self.partial_result(
						format!("Parent directory ancestor pinning failed at {}: {err}. {} of {total_ops} operations were performed.", op.display, self.cumulative_written.lock().unwrap().len()),
						&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
					));
				}
			};

			let mut created_dirs = Vec::new();
			let dir_pin = match open_or_create_subdirs(&ancestor_pin, &op.parent_subdirs, &mut created_dirs) {
				Ok(p) => {
					attempt_created_dirs.extend(created_dirs.clone());
					self.created_dirs.lock().unwrap().extend(created_dirs);
					p
				}
				Err(err) => {
					attempt_created_dirs.extend(created_dirs.clone());
					self.created_dirs.lock().unwrap().extend(created_dirs);
					return Ok(self.partial_result(
						format!("Parent directory subdirectory creation failed at {}: {err}. {} of {total_ops} operations were performed.", op.display, self.cumulative_written.lock().unwrap().len()),
						&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
					));
				}
			};

			if !matches!(current_status, OpStatus::DestPublished { .. }) {
				if let Err(err) = Self::check_file_identity(op).await {
					return Ok(self.partial_result(
						format!("Preview is stale / no longer matches at {}: {err}. {} of {total_ops} operations were performed.", op.display, self.cumulative_written.lock().unwrap().len()),
						&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
					));
				}
			}
			if ctx.cancellation_token.is_cancelled() {
				return Ok(self.partial_result(
					format!("Cancelled after applying {} of {total_ops} operations; remaining operations were not performed.", self.cumulative_written.lock().unwrap().len()),
					&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, None, cleanup_warnings,
				));
			}

			let file_name = op.path.file_name().and_then(|n| n.to_str()).unwrap_or("file");

			match op.kind {
				OpKind::Create => {
					// Check target does not exist via held dirfd
					match dir_pin.stat_file(file_name) {
						Ok(Some(_)) => {
							return Ok(self.partial_result(
								format!("Cannot create {}: file already exists under held directory descriptor. {} of {total_ops} operations performed.", op.display, self.cumulative_written.lock().unwrap().len()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
							));
						}
						Err(e) => {
							return Ok(self.partial_result(
								format!("Cannot stat target {}: {e}. {} of {total_ops} operations performed.", op.display, self.cumulative_written.lock().unwrap().len()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						Ok(None) => {}
					}
					let (mut temp_file, temp_name, temp_dev_ino, mut temp_guard) = match create_temp_file(&dir_pin, file_name, &self.cleanup_warnings) {
						Ok(res) => res,
						Err(err) => {
							return Ok(self.partial_result(
								format!("Failed to create temp file for {}: {err}. {} of {total_ops} operations performed.", op.display, self.cumulative_written.lock().unwrap().len()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					};

					if let Err(e) = temp_file.write_all(&op.content) {
						drop(temp_file);
						drop(temp_guard);
						return Ok(self.partial_result(
							format!("write {}: {e}. {} of {total_ops} operations performed.", op.display, self.cumulative_written.lock().unwrap().len()),
							&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
						));
					}
					if let Err(e) = temp_file.flush() {
						drop(temp_file);
						drop(temp_guard);
						return Ok(self.partial_result(
							format!("flush {}: {e}. {} of {total_ops} operations performed.", op.display, self.cumulative_written.lock().unwrap().len()),
							&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
						));
					}

					let target_mode = op.mode.unwrap_or(0o600) & 0o7777;
					#[cfg(unix)]
					if target_mode != 0o600 {
						use std::os::unix::io::AsRawFd;
						if unsafe { libc::fchmod(temp_file.as_raw_fd(), target_mode as libc::mode_t) } != 0 {
							let err = std::io::Error::last_os_error();
							drop(temp_file);
							drop(temp_guard);
							return Ok(self.partial_result(
								format!("fchmod target mode failed for {}: {err}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					}

					// Verify temp file entry dev_ino immediately before publication to guard against substitution
					#[cfg(unix)]
					{
						match dir_pin.stat_file(&temp_name) {
							Ok(Some(entry_dev_ino)) if entry_dev_ino == temp_dev_ino => {},
							Ok(Some(other_dev_ino)) => {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Temp file {temp_name} entry in directory was replaced concurrently (expected {temp_dev_ino:?}, found {other_dev_ino:?}). Publication aborted."),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
								));
							}
							_ => {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Temp file {temp_name} missing from directory before publication. Publication aborted."),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
					}
					// NOTE on residual POSIX named-temp check-to-publication race:
					// POSIX lacks an fd-to-name atomic publication syscall (such as frenameat or flinkat).
					// Publication must pass the temporary file's name string relative to dir_pin.fd.
					// We keep temp_file open and verify that dir_pin.stat_file(&temp_name) == Some(temp_dev_ino)
					// immediately before invoking linkat/renameat2. An uncooperating same-UID process could
					// theoretically replace the entry in the gap between stat_file and publication.
					// True transactional multi-file atomicity is not claimed.
					#[cfg(unix)]
					let publish_res = {
						let c_temp = match to_cstring(&temp_name) {
							Ok(c) => c,
							Err(e) => {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Invalid temp file name: {e}"),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
								));
							}
						};
						let c_target = match to_cstring(file_name) {
							Ok(c) => c,
							Err(e) => {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Invalid target file name: {e}"),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
								));
							}
						};
						let res = unsafe { libc::linkat(dir_pin.fd, c_temp.as_ptr(), dir_pin.fd, c_target.as_ptr(), 0) };
						if res == 0 {
							Ok(true)
						} else {
							let err = std::io::Error::last_os_error();
							if err.raw_os_error() == Some(libc::EEXIST) {
								Ok(false)
							} else {
								// Fallback to renameat2(RENAME_NOREPLACE) through dirfd
								let ren = unsafe {
									libc::syscall(
										libc::SYS_renameat2,
										dir_pin.fd as libc::c_long,
										c_temp.as_ptr(),
										dir_pin.fd as libc::c_long,
										c_target.as_ptr(),
										libc::RENAME_NOREPLACE as libc::c_long,
									)
								};
								if ren == 0 {
									Ok(true)
								} else {
									let ren_err = std::io::Error::last_os_error();
									if ren_err.raw_os_error() == Some(libc::EEXIST) {
										Ok(false)
									} else {
										Err(format!("linkat failed ({err}); renameat2 failed ({ren_err})"))
									}
								}
							}
						}
					};
					#[cfg(not(unix))]
					let publish_res = match std::fs::hard_link(dir_pin.path.join(&temp_name), &op.path) {
						Ok(()) => Ok(true),
						Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
						Err(e) => Err(e.to_string()),
					};

					drop(temp_file);
					// Unlink temp via held dirfd immediately
					drop(temp_guard);
					match publish_res {
						Ok(true) => {
							attempt_written.push(op.display.clone());
							self.cumulative_written.lock().unwrap().push(op.display.clone());
							self.op_statuses.lock().unwrap()[idx] = OpStatus::Completed;
							let key = canonical_key(&op.path);
							self.snapshots.invalidate(&key);
							let text = normalize_snapshot_text(&op.content);
							let tag = self.snapshots.record_with_raw(&key, &op.content, &text, None);
							fresh_tags.extend(tag.map(|t| format_header(&op.display, &t)));
						}
						Ok(false) => {
							return Ok(self.partial_result(
								format!("Cannot create {}: file was created concurrently. {} of {total_ops} operations performed.", op.display, self.cumulative_written.lock().unwrap().len()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
							));
						}
						Err(e) => {
							return Ok(self.partial_result(
								format!("Publish {}: {e}. {} of {total_ops} operations performed.", op.display, self.cumulative_written.lock().unwrap().len()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					}
				}
				OpKind::Update => {
					// Acquire WorkspaceWriter lease in lock directory namespace
					let canonical = canonical_identity(&op.path);
					let writer = match WorkspaceWriter::new() {
						Ok(w) => w,
						Err(e) => {
							return Ok(self.partial_result(
								format!("workspace writer {}: {e}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					};
					let _lease = match writer.acquire(&canonical) {
						Ok(l) => l,
						Err(e) => {
							return Ok(self.partial_result(
								format!("lease {}: {e}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					};

					// Source pin before checks: open source file descriptor directly and keep open until after rename
					#[cfg(unix)]
					let (mut source_file, source_mode) = {
						let c_target = match to_cstring(file_name) {
							Ok(c) => c,
							Err(e) => {
								return Ok(self.partial_result(
									format!("Invalid source path: {e}"),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
								));
							}
						};
						let fd = unsafe {
							libc::openat(dir_pin.fd, c_target.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW)
						};
						if fd < 0 {
							return Ok(self.partial_result(
								format!("Cannot pin source file {}: {}. Source was preserved.", op.display, std::io::Error::last_os_error()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
							));
						}
						use std::os::unix::io::FromRawFd;
						let file = unsafe { std::fs::File::from_raw_fd(fd) };

						use std::os::unix::io::AsRawFd;
						let mut stat: libc::stat = unsafe { std::mem::zeroed() };
						if unsafe { libc::fstat(file.as_raw_fd(), &mut stat) } != 0 {
							return Ok(self.partial_result(
								format!("Cannot fstat source {}: {}. Source preserved.", op.display, std::io::Error::last_os_error()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
							));
						}
						if let Some(expected_dev_ino) = op.base_dev_ino {
							if (stat.st_dev as u64, stat.st_ino as u64) != expected_dev_ino {
								return Ok(self.partial_result(
									format!("Source {} identity (dev/ino) changed concurrently. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
						let current_mode = stat.st_mode as u32 & 0o7777;
						if let Some(expected_base_mode) = op.base_mode {
							if current_mode != expected_base_mode {
								return Ok(self.partial_result(
									format!("Source {} mode changed from 0o{expected_base_mode:o} to 0o{current_mode:o} concurrently. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
						// Read initial exact bytes from pinned descriptor
						let mut current_bytes = Vec::new();
						let mut rdr = &file;
						if let Err(e) = rdr.read_to_end(&mut current_bytes) {
							return Ok(self.partial_result(
								format!("Cannot read source {}: {e}. Source preserved.", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						if let Some(base) = &op.base {
							if &current_bytes != base {
								return Ok(self.partial_result(
									format!("Source {} content changed concurrently. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
						(file, stat.st_mode as u32 & 0o7777)
					};
					#[cfg(not(unix))]
					let source_mode = 0o600;

					let (mut temp_file, temp_name, temp_dev_ino, mut temp_guard) = match create_temp_file(&dir_pin, file_name, &self.cleanup_warnings) {
						Ok(res) => res,
						Err(err) => {
							return Ok(self.partial_result(
								format!("Failed to create temp file for {}: {err}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					};

					if let Err(e) = temp_file.write_all(&op.content) {
						drop(temp_file);
						drop(temp_guard);
						return Ok(self.partial_result(
							format!("write {}: {e}", op.display),
							&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
						));
					}
					if let Err(e) = temp_file.flush() {
						drop(temp_file);
						drop(temp_guard);
						return Ok(self.partial_result(
							format!("flush {}: {e}", op.display),
							&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
						));
					}

					// Check source mode after writing temp before fchmod on temp!
					#[cfg(unix)]
					{
						use std::os::unix::io::AsRawFd;
						let mut mid_source_stat: libc::stat = unsafe { std::mem::zeroed() };
						if unsafe { libc::fstat(source_file.as_raw_fd(), &mut mid_source_stat) } != 0 {
							drop(temp_file);
							drop(temp_guard);
							return Ok(self.partial_result(
								format!("Failed to fstat source {} before fchmod temp: {}. Source preserved.", op.display, std::io::Error::last_os_error()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						if let Some(expected_base_mode) = op.base_mode {
							let mid_mode = mid_source_stat.st_mode as u32 & 0o7777;
							if mid_mode != expected_base_mode {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Source {} mode changed from 0o{expected_base_mode:o} to 0o{mid_mode:o} after writing temp. Update aborted. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
					}

					let target_mode = op.mode.unwrap_or(source_mode) & 0o7777;
					#[cfg(unix)]
					if target_mode != 0o600 {
						use std::os::unix::io::AsRawFd;
						if unsafe { libc::fchmod(temp_file.as_raw_fd(), target_mode as libc::mode_t) } != 0 {
							let err = std::io::Error::last_os_error();
							drop(temp_file);
							drop(temp_guard);
							return Ok(self.partial_result(
								format!("fchmod target mode failed for {}: {err}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					}

					#[cfg(unix)]
					{
						// Re-verify source descriptor and leaf identity immediately before rename
						use std::os::unix::io::AsRawFd;
						let mut final_stat: libc::stat = unsafe { std::mem::zeroed() };
						if unsafe { libc::fstat(source_file.as_raw_fd(), &mut final_stat) } != 0 {
							drop(temp_file);
							drop(temp_guard);
							return Ok(self.partial_result(
								format!("Re-fstat source {} immediately before rename failed: {}. Source preserved.", op.display, std::io::Error::last_os_error()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
							));
						}
						if let Some(expected_dev_ino) = op.base_dev_ino {
							if (final_stat.st_dev as u64, final_stat.st_ino as u64) != expected_dev_ino {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Source {} dev/ino changed immediately before rename. Update aborted.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
						if let Some(expected_base_mode) = op.base_mode {
							let final_mode = final_stat.st_mode as u32 & 0o7777;
							if final_mode != expected_base_mode {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Source {} mode changed from 0o{expected_base_mode:o} to 0o{final_mode:o} immediately before rename. Update aborted. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
						// HIGH review defect fix: re-read exact bytes from source_file after writing temp / immediately before rename!
						if let Err(e) = source_file.seek(SeekFrom::Start(0)) {
							drop(temp_file);
							drop(temp_guard);
							return Ok(self.partial_result(
								format!("Failed to seek source {}: {e}. Source preserved.", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						let mut final_source_bytes = Vec::new();
						if let Err(e) = source_file.read_to_end(&mut final_source_bytes) {
							drop(temp_file);
							drop(temp_guard);
							return Ok(self.partial_result(
								format!("Failed to re-read source {}: {e}. Source preserved.", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						if let Some(base) = &op.base {
							if &final_source_bytes != base {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Source {} content was modified in-place immediately before rename. Update aborted. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}

						// Re-verify source leaf entry in directory descriptor
						match dir_pin.stat_file(file_name) {
							Ok(Some(leaf_dev_ino)) => {
								if let Some(expected_dev_ino) = op.base_dev_ino {
									if leaf_dev_ino != expected_dev_ino {
										drop(temp_file);
										drop(temp_guard);
										return Ok(self.partial_result(
											format!("Source {} leaf entry in directory replaced immediately before rename. Update aborted.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
							}
							_ => {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Source {} leaf entry missing or invalid before rename. Update aborted.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}

						// Re-verify temp leaf entry in directory descriptor to guard against substitution
						match dir_pin.stat_file(&temp_name) {
							Ok(Some(entry_dev_ino)) if entry_dev_ino == temp_dev_ino => {},
							Ok(Some(other_dev_ino)) => {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Temp file {temp_name} entry was replaced concurrently (expected {temp_dev_ino:?}, found {other_dev_ino:?}) before rename. Update aborted. Source preserved."),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
								));
							}
							_ => {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Temp file {temp_name} missing from directory before rename. Update aborted. Source preserved."),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
						// NOTE on residual POSIX named-temp check-to-publication race:
						// POSIX lacks frenameat. We verify dir_pin.stat_file(&temp_name) == temp_dev_ino
						// immediately before renameat. A concurrent same-UID process could race in the gap.
						// Atomic filesystem transactions are explicitly not claimed.
						// Atomic replace relative to held dirfd
						let c_target = match to_cstring(file_name) {
							Ok(c) => c,
							Err(e) => {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Invalid target file name: {e}"),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
								));
							}
						};
						let c_temp = match to_cstring(&temp_name) {
							Ok(c) => c,
							Err(e) => {
								drop(temp_file);
								drop(temp_guard);
								return Ok(self.partial_result(
									format!("Invalid temp file name: {e}"),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
								));
							}
						};
						let ren = unsafe {
							libc::renameat(dir_pin.fd, c_temp.as_ptr(), dir_pin.fd, c_target.as_ptr())
						};
						drop(temp_file);
						drop(source_file);
						if ren != 0 {
							let err = std::io::Error::last_os_error();
							drop(temp_guard);
							return Ok(self.partial_result(
								format!("Failed to rename temp over {}: {err}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					}
					#[cfg(not(unix))]
					{
						if let Err(e) = std::fs::write(&op.path, &op.content) {
							drop(temp_guard);
							return Ok(self.partial_result(
								format!("Failed to write {}: {e}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					}

					drop(temp_guard);
					attempt_written.push(op.display.clone());
					attempt_invalidated.push(op.display.clone());
					self.cumulative_written.lock().unwrap().push(op.display.clone());
					self.cumulative_invalidated.lock().unwrap().push(op.display.clone());
					self.op_statuses.lock().unwrap()[idx] = OpStatus::Completed;
					let key = canonical_key(&op.path);
					self.snapshots.invalidate(&key);
					let text = normalize_snapshot_text(&op.content);
					let tag = self.snapshots.record_with_raw(&key, &op.content, &text, None);
					fresh_tags.extend(tag.map(|t| format_header(&op.display, &t)));
				}
				OpKind::Delete => {
					// NOTE on residual noncooperating TOCTOU:
					// Operations are descriptor-relative (`unlinkat(dirfd, name, 0)`) under held dirfd.
					// Source is pinned via openat descriptor and verified under WorkspaceWriter lease.
					// However, POSIX lacks an atomic conditional unlink (`unlink_if_unchanged`); an
					// uncooperative external process operating concurrently outside flock could replace the
					// file in the sub-millisecond gap between verification and unlinkat. Atomic filesystem transactions are not claimed.
					let canonical = canonical_identity(&op.path);
					let writer = match WorkspaceWriter::new() {
						Ok(w) => w,
						Err(e) => {
							return Ok(self.partial_result(
								format!("workspace writer {}: {e}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					};
					let _lease = match writer.acquire(&canonical) {
						Ok(l) => l,
						Err(e) => {
							return Ok(self.partial_result(
								format!("delete lease {}: {e}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					};

					#[cfg(unix)]
					let mut source_file = {
						let c_target = match to_cstring(file_name) {
							Ok(c) => c,
							Err(e) => {
								return Ok(self.partial_result(
									format!("Invalid file name: {e}"),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
								));
							}
						};
						let fd = unsafe {
							libc::openat(dir_pin.fd, c_target.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW)
						};
						if fd < 0 {
							let err = std::io::Error::last_os_error();
							if err.raw_os_error() == Some(libc::ELOOP) {
								None
							} else {
								return Ok(self.partial_result(
									format!("Cannot delete {}: failed to open pinned descriptor: {err}. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
								));
							}
						} else {
							use std::os::unix::io::FromRawFd;
							Some(unsafe { std::fs::File::from_raw_fd(fd) })
						}
					};

					#[cfg(unix)]
					if let Some(mut source_file) = source_file {
						use std::os::unix::io::AsRawFd;
						let mut stat: libc::stat = unsafe { std::mem::zeroed() };
						if unsafe { libc::fstat(source_file.as_raw_fd(), &mut stat) } != 0 {
							return Ok(self.partial_result(
								format!("Cannot delete {}: fstat failed: {}. Source preserved.", op.display, std::io::Error::last_os_error()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						if let Some(expected_dev_ino) = op.base_dev_ino {
							if (stat.st_dev as u64, stat.st_ino as u64) != expected_dev_ino {
								return Ok(self.partial_result(
									format!("Cannot delete {}: file identity (dev/ino) changed concurrently. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
						if let Some(expected_base_mode) = op.base_mode {
							let current_mode = stat.st_mode as u32 & 0o7777;
							if current_mode != expected_base_mode {
								return Ok(self.partial_result(
									format!("Cannot delete {}: file mode changed from 0o{expected_base_mode:o} to 0o{current_mode:o} concurrently. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
						let mut current_bytes = Vec::new();
						if let Err(e) = source_file.read_to_end(&mut current_bytes) {
							return Ok(self.partial_result(
								format!("Cannot delete {}: failed to read file: {e}. Source preserved.", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						if let Some(base) = &op.base {
							if &current_bytes != base {
								return Ok(self.partial_result(
									format!("Cannot delete {}: file content changed concurrently. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}

						// Re-read exact bytes immediately before unlink
						if let Err(e) = source_file.seek(SeekFrom::Start(0)) {
							return Ok(self.partial_result(
								format!("Cannot delete {}: failed to seek source: {e}. Source preserved.", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						let mut final_delete_bytes = Vec::new();
						if let Err(e) = source_file.read_to_end(&mut final_delete_bytes) {
							return Ok(self.partial_result(
								format!("Cannot delete {}: failed to re-read source: {e}. Source preserved.", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						if let Some(base) = &op.base {
							if &final_delete_bytes != base {
								return Ok(self.partial_result(
									format!("Cannot delete {}: file content changed in-place immediately before delete. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
						if let Some(expected_base_mode) = op.base_mode {
							let mut final_delete_stat: libc::stat = unsafe { std::mem::zeroed() };
							if unsafe { libc::fstat(source_file.as_raw_fd(), &mut final_delete_stat) } == 0 {
								let final_mode = final_delete_stat.st_mode as u32 & 0o7777;
								if final_mode != expected_base_mode {
									return Ok(self.partial_result(
										format!("Cannot delete {}: file mode changed immediately before delete. Source preserved.", op.display),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
									));
								}
							}
						}

						// Re-verify leaf entry immediately before unlink
						match dir_pin.stat_file(file_name) {
							Ok(Some(leaf_dev_ino)) => {
								if let Some(expected_dev_ino) = op.base_dev_ino {
									if leaf_dev_ino != expected_dev_ino {
										return Ok(self.partial_result(
											format!("Source {} leaf entry in directory replaced before delete. Source preserved.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
							}
							_ => {
								return Ok(self.partial_result(
									format!("Source {} leaf entry missing before delete. Source preserved.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
						drop(source_file);
					} else {
						// Symlink delete: verify leaf entry dev_ino matches expected dev_ino
						if let Some(expected_dev_ino) = op.base_dev_ino {
							match dir_pin.stat_file(file_name) {
								Ok(Some(leaf_dev_ino)) => {
									if leaf_dev_ino != expected_dev_ino {
										return Ok(self.partial_result(
											format!("Symlink {} leaf entry in directory replaced before delete. Symlink preserved.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
								_ => {
									return Ok(self.partial_result(
										format!("Symlink {} leaf entry missing before delete. Symlink preserved.", op.display),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
									));
								}
							}
						}
					}

					// Descriptor-relative unlink under held dirfd
					if let Err(e) = dir_pin.unlinkat_file(file_name) {
						return Ok(self.partial_result(
							format!("Failed to delete {}: {e}. Source preserved.", op.display),
							&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
						));
					}
					attempt_written.push(op.display.clone());
					attempt_invalidated.push(op.display.clone());
					self.cumulative_written.lock().unwrap().push(op.display.clone());
					self.cumulative_invalidated.lock().unwrap().push(op.display.clone());
					self.op_statuses.lock().unwrap()[idx] = OpStatus::Completed;
					let key = canonical_key(&op.path);
					self.snapshots.invalidate(&key);
				}
				// Symlink leaf policy for Move:
				// Move opens source descriptor with O_NOFOLLOW. If source is a symlink leaf,
				// openat returns ELOOP and fails closed with an explicit error. This prevents
				// silently transforming a symlink into a regular file while orphaning its target.
				// To move a symlink, users should delete and recreate the link.
				OpKind::Move => {
					let dest = match op.rename_to.as_ref() {
						Some(d) => d,
						None => {
							return Ok(self.partial_result(
								format!("Missing rename destination for {}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					};
					let dest_display = op.rename_to_display.as_deref().unwrap_or("destination").to_string();

					let source_canonical = canonical_identity(&op.path);
					let dest_canonical = canonical_identity(dest);

					// Acquire WorkspaceWriter lease on BOTH source and destination for the entire move!
					let writer = match WorkspaceWriter::new() {
						Ok(w) => w,
						Err(e) => {
							return Ok(self.partial_result(
								format!("workspace writer {}: {e}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					};
					let _source_lease = match writer.acquire(&source_canonical) {
						Ok(l) => l,
						Err(e) => {
							return Ok(self.partial_result(
								format!("source lease {}: {e}", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
							));
						}
					};
					let _dest_lease = match writer.acquire(&dest_canonical) {
						Ok(l) => l,
						Err(e) => {
							return Ok(self.partial_result(
								format!("destination lease {dest_display}: {e}"),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(dest_display), cleanup_warnings,
							));
						}
					};

					let dest_file_name = dest.file_name().and_then(|n| n.to_str()).unwrap_or("dest");

					let dest_ancestor = op.rename_parent_identity.as_ref().map(|p| p.as_path()).unwrap_or_else(|| Path::new("."));
					let dest_ancestor_pin = match DirPin::open(dest_ancestor) {
						Ok(p) => {
							if let Err(e) = p.verify_against(dest_ancestor, op.rename_parent_dev_ino) {
								return Ok(self.partial_result(
									format!("Destination ancestor verification failed at {dest_display}: {e}"),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags,
									!self.cumulative_written.lock().unwrap().is_empty(), true, Some(dest_display), cleanup_warnings,
								));
							}
							p
						}
						Err(e) => {
							return Ok(self.partial_result(
								format!("Cannot open destination directory ancestor {}: {e}", dest_ancestor.display()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags,
								!self.cumulative_written.lock().unwrap().is_empty(), true, Some(dest_display), cleanup_warnings,
							));
						}
					};
					let mut dest_created_dirs = Vec::new();
					let dest_dir_pin = match open_or_create_subdirs(&dest_ancestor_pin, &op.rename_parent_subdirs, &mut dest_created_dirs) {
						Ok(p) => {
							attempt_created_dirs.extend(dest_created_dirs.clone());
							self.created_dirs.lock().unwrap().extend(dest_created_dirs);
							p
						}
						Err(e) => {
							attempt_created_dirs.extend(dest_created_dirs.clone());
							self.created_dirs.lock().unwrap().extend(dest_created_dirs);
							return Ok(self.partial_result(
								format!("Cannot open or create destination subdirectories for {dest_display}: {e}"),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags,
								!self.cumulative_written.lock().unwrap().is_empty(), true, Some(dest_display), cleanup_warnings,
							));
						}
					};

					let dest_dev_ino = match current_status {
						OpStatus::Pending => {
							// Verify destination does not exist under held dest_dir_pin
							match dest_dir_pin.stat_file(dest_file_name) {
								Ok(Some(_)) => {
									return Ok(self.partial_result(
										format!("Cannot move to {dest_display}: destination already exists under held directory descriptor. {} of {total_ops} operations performed.", self.cumulative_written.lock().unwrap().len()),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(dest_display), cleanup_warnings,
									));
								}
								Err(e) => {
									return Ok(self.partial_result(
										format!("Cannot stat destination {dest_display}: {e}. {} of {total_ops} operations performed.", self.cumulative_written.lock().unwrap().len()),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(dest_display), cleanup_warnings,
									));
								}
								Ok(None) => {}
							}

							// Pin source descriptor before any write and keep open until after unlink
							#[cfg(unix)]
							let (mut source_file, source_mode) = {
								let c_source = match to_cstring(file_name) {
									Ok(c) => c,
									Err(e) => {
										return Ok(self.partial_result(
											format!("Invalid source path: {e}"),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
										));
									}
								};
								let fd = unsafe {
									libc::openat(dir_pin.fd, c_source.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW)
								};
								if fd < 0 {
									let err = std::io::Error::last_os_error();
									let msg = if err.raw_os_error() == Some(libc::ELOOP) {
										format!("Cannot move symlink {}: moving symlinks is not supported. Source preserved.", op.display)
									} else {
										format!("Cannot pin source {}: {err}. Source preserved.", op.display)
									};
									return Ok(self.partial_result(
										msg,
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
									));
								}
								use std::os::unix::io::FromRawFd;
								let file = unsafe { std::fs::File::from_raw_fd(fd) };

								use std::os::unix::io::AsRawFd;
								let mut stat: libc::stat = unsafe { std::mem::zeroed() };
								if unsafe { libc::fstat(file.as_raw_fd(), &mut stat) } != 0 {
									return Ok(self.partial_result(
										format!("Cannot stat source {}: {}. Source preserved.", op.display, std::io::Error::last_os_error()),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
									));
								}
								if let Some(expected_dev_ino) = op.base_dev_ino {
									if (stat.st_dev as u64, stat.st_ino as u64) != expected_dev_ino {
										return Ok(self.partial_result(
											format!("Source {} identity (dev/ino) changed concurrently. Source preserved.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
								let current_mode = stat.st_mode as u32 & 0o7777;
								if let Some(expected_base_mode) = op.base_mode {
									if current_mode != expected_base_mode {
										return Ok(self.partial_result(
											format!("Source {} mode changed from 0o{expected_base_mode:o} to 0o{current_mode:o} concurrently. Source preserved.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
								let mut current_bytes = Vec::new();
								let mut rdr = &file;
								if let Err(e) = rdr.read_to_end(&mut current_bytes) {
									return Ok(self.partial_result(
										format!("Cannot read source {}: {e}. Source preserved.", op.display),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
									));
								}
								if let Some(base) = &op.base {
									if &current_bytes != base {
										return Ok(self.partial_result(
											format!("Source {} content changed concurrently. Source preserved.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
								(file, stat.st_mode as u32 & 0o7777)
							};
							#[cfg(not(unix))]
							let source_mode = 0o600;

							// Create temp file in dest_dir_pin with private 0600
							let (mut temp_dest_file, temp_dest_name, temp_dest_dev_ino, mut temp_dest_guard) = match create_temp_file(&dest_dir_pin, dest_file_name, &self.cleanup_warnings) {
								Ok(res) => res,
								Err(err) => {
									return Ok(self.partial_result(
										format!("Failed to create destination temp file for {dest_display}: {err}"),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(dest_display), cleanup_warnings,
									));
								}
							};

							if let Err(e) = temp_dest_file.write_all(&op.content) {
								drop(temp_dest_file);
								drop(temp_dest_guard);
								return Ok(self.partial_result(
									format!("write {dest_display}: {e}"),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(dest_display), cleanup_warnings,
								));
							}
							if let Err(e) = temp_dest_file.flush() {
								drop(temp_dest_file);
								drop(temp_dest_guard);
								return Ok(self.partial_result(
									format!("flush {dest_display}: {e}"),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(dest_display), cleanup_warnings,
								));
							}

							// Check source mode after writing temp before fchmod on destination temp!
							#[cfg(unix)]
							{
								use std::os::unix::io::AsRawFd;
								let mut mid_source_stat: libc::stat = unsafe { std::mem::zeroed() };
								if unsafe { libc::fstat(source_file.as_raw_fd(), &mut mid_source_stat) } != 0 {
									drop(temp_dest_file);
									drop(temp_dest_guard);
									return Ok(self.partial_result(
										format!("Failed to fstat source {} before fchmod temp: {}. Source preserved.", op.display, std::io::Error::last_os_error()),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
									));
								}
								if let Some(expected_base_mode) = op.base_mode {
									let mid_mode = mid_source_stat.st_mode as u32 & 0o7777;
									if mid_mode != expected_base_mode {
										drop(temp_dest_file);
										drop(temp_dest_guard);
										return Ok(self.partial_result(
											format!("Source {} mode changed from 0o{expected_base_mode:o} to 0o{mid_mode:o} after writing temp. Move aborted. Source preserved.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
							}

							let target_mode = op.mode.unwrap_or(source_mode) & 0o7777;
							#[cfg(unix)]
							if target_mode != 0o600 {
								use std::os::unix::io::AsRawFd;
								if unsafe { libc::fchmod(temp_dest_file.as_raw_fd(), target_mode as libc::mode_t) } != 0 {
									let err = std::io::Error::last_os_error();
									drop(temp_dest_file);
									drop(temp_dest_guard);
									return Ok(self.partial_result(
										format!("fchmod target mode failed for {dest_display}: {err}"),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(dest_display), cleanup_warnings,
									));
								}
							}

							// Re-verify source BEFORE publication of destination!
							#[cfg(unix)]
							{
								use std::os::unix::io::AsRawFd;
								let mut pre_source_stat: libc::stat = unsafe { std::mem::zeroed() };
								if unsafe { libc::fstat(source_file.as_raw_fd(), &mut pre_source_stat) } != 0 {
									drop(temp_dest_file);
									drop(temp_dest_guard);
									return Ok(self.partial_result(
										format!("Failed to fstat source {} before destination publish: {}. Source preserved.", op.display, std::io::Error::last_os_error()),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
									));
								}
								if let Some(expected_dev_ino) = op.base_dev_ino {
									if (pre_source_stat.st_dev as u64, pre_source_stat.st_ino as u64) != expected_dev_ino {
										drop(temp_dest_file);
										drop(temp_dest_guard);
										return Ok(self.partial_result(
											format!("Source {} dev/ino changed before destination publish. Move aborted. Source preserved.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
								if let Some(expected_base_mode) = op.base_mode {
									let pre_mode = pre_source_stat.st_mode as u32 & 0o7777;
									if pre_mode != expected_base_mode {
										drop(temp_dest_file);
										drop(temp_dest_guard);
										return Ok(self.partial_result(
											format!("Source {} mode changed from 0o{expected_base_mode:o} to 0o{pre_mode:o} before destination publish. Move aborted. Source preserved.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
								if let Err(e) = source_file.seek(SeekFrom::Start(0)) {
									drop(temp_dest_file);
									drop(temp_dest_guard);
									return Ok(self.partial_result(
										format!("Failed to seek source {}: {e}. Move aborted.", op.display),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
									));
								}
								let mut pre_source_bytes = Vec::new();
								if let Err(e) = source_file.read_to_end(&mut pre_source_bytes) {
									drop(temp_dest_file);
									drop(temp_dest_guard);
									return Ok(self.partial_result(
										format!("Failed to re-read source {}: {e}. Move aborted.", op.display),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(op.display.clone()), cleanup_warnings,
									));
								}
								if let Some(base) = &op.base {
									if &pre_source_bytes != base {
										drop(temp_dest_file);
										drop(temp_dest_guard);
										return Ok(self.partial_result(
											format!("Source {} content changed in-place before destination publish. Move aborted. Source preserved.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
								match dir_pin.stat_file(file_name) {
									Ok(Some(leaf_dev_ino)) => {
										if let Some(expected_dev_ino) = op.base_dev_ino {
											if leaf_dev_ino != expected_dev_ino {
												drop(temp_dest_file);
												drop(temp_dest_guard);
												return Ok(self.partial_result(
													format!("Source {} leaf entry in directory replaced before destination publish. Move aborted. Source preserved.", op.display),
													&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
												));
											}
										}
									}
									_ => {
										drop(temp_dest_file);
										drop(temp_dest_guard);
										return Ok(self.partial_result(
											format!("Source {} leaf entry missing before destination publish. Move aborted. Source preserved.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
								// Verify dest temp leaf entry dev_ino immediately before publication
								match dest_dir_pin.stat_file(&temp_dest_name) {
									Ok(Some(entry_dev_ino)) if entry_dev_ino == temp_dest_dev_ino => {},
									Ok(Some(other_dev_ino)) => {
										drop(temp_dest_file);
										drop(temp_dest_guard);
										return Ok(self.partial_result(
											format!("Destination temp file {temp_dest_name} replaced concurrently (expected {temp_dest_dev_ino:?}, found {other_dev_ino:?}) before publication. Move aborted."),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(dest_display), cleanup_warnings,
										));
									}
									_ => {
										drop(temp_dest_file);
										drop(temp_dest_guard);
										return Ok(self.partial_result(
											format!("Destination temp file {temp_dest_name} missing from directory before publication. Move aborted."),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(dest_display), cleanup_warnings,
										));
									}
								}
							}
							// NOTE on residual POSIX named-temp check-to-publication race:
							// POSIX lacks frenameat/flinkat. We verify dest_dir_pin.stat_file(&temp_dest_name) == temp_dest_dev_ino
							// immediately before publication. A concurrent same-UID process could race in the gap.
							// Atomic filesystem transactions are explicitly not claimed.
							// Atomic publication at destination relative to dest_dir_pin
							#[cfg(unix)]
							let dest_publish_res = {
								let c_temp = match to_cstring(&temp_dest_name) {
									Ok(c) => c,
									Err(e) => {
										drop(temp_dest_file);
										drop(temp_dest_guard);
										return Ok(self.partial_result(
											format!("Invalid temp destination name: {e}"),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(dest_display), cleanup_warnings,
										));
									}
								};
								let c_dest = match to_cstring(dest_file_name) {
									Ok(c) => c,
									Err(e) => {
										drop(temp_dest_file);
										drop(temp_dest_guard);
										return Ok(self.partial_result(
											format!("Invalid destination name: {e}"),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(dest_display), cleanup_warnings,
										));
									}
								};
								let res = unsafe { libc::linkat(dest_dir_pin.fd, c_temp.as_ptr(), dest_dir_pin.fd, c_dest.as_ptr(), 0) };
								drop(temp_dest_file);
								if res == 0 {
									Ok(true)
								} else {
									let err = std::io::Error::last_os_error();
									if err.raw_os_error() == Some(libc::EEXIST) {
										Ok(false)
									} else {
										let ren = unsafe {
											libc::syscall(
												libc::SYS_renameat2,
												dest_dir_pin.fd as libc::c_long,
												c_temp.as_ptr(),
												dest_dir_pin.fd as libc::c_long,
												c_dest.as_ptr(),
												libc::RENAME_NOREPLACE as libc::c_long,
											)
										};
										if ren == 0 {
											Ok(true)
										} else {
											let ren_err = std::io::Error::last_os_error();
											if ren_err.raw_os_error() == Some(libc::EEXIST) {
												Ok(false)
											} else {
												Err(format!("linkat failed ({err}); renameat2 failed ({ren_err})"))
											}
										}
									}
								}
							};
							#[cfg(not(unix))]
							let dest_publish_res = match std::fs::hard_link(dest_dir_pin.path.join(&temp_dest_name), dest) {
								Ok(()) => Ok(true),
								Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
								Err(e) => Err(e.to_string()),
							};

							// Unlink temp via held dest_dir_pin
							drop(temp_dest_guard);

							match dest_publish_res {
								Ok(true) => {
									#[cfg(unix)]
									let published_dev_ino = temp_dest_dev_ino;
									#[cfg(not(unix))]
									let published_dev_ino = (0, 0);

									attempt_written.push(dest_display.clone());
									self.cumulative_written.lock().unwrap().push(dest_display.clone());
									self.op_statuses.lock().unwrap()[idx] = OpStatus::DestPublished { dest_dev_ino: published_dev_ino };
									let dest_key = canonical_key(dest);
									let text = normalize_snapshot_text(&op.content);
									let tag = self.snapshots.record_with_raw(&dest_key, &op.content, &text, None);
									fresh_tags.extend(tag.map(|t| format_header(&dest_display, &t)));
									published_dev_ino
								}
								Ok(false) => {
									return Ok(self.partial_result(
										format!("Cannot move to {dest_display}: destination already exists. {} of {total_ops} operations performed.", self.cumulative_written.lock().unwrap().len()),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), true, Some(dest_display), cleanup_warnings,
									));
								}
								Err(e) => {
									return Ok(self.partial_result(
										format!("Failed to publish move destination {dest_display}: {e}. {} of {total_ops} operations performed.", self.cumulative_written.lock().unwrap().len()),
										&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, !self.cumulative_written.lock().unwrap().is_empty(), false, Some(dest_display), cleanup_warnings,
									));
								}
							}
						}
						OpStatus::DestPublished { dest_dev_ino } => {
							dest_dev_ino
						}
						OpStatus::Completed => unreachable!(),
					};

					// Verify destination identity, directory entry, and exact bytes for BOTH fresh publication and retry!
					// Hold owning destination descriptor across source verification!
					#[cfg(unix)]
					let mut dest_file = {
						let c_dest = match to_cstring(dest_file_name) {
							Ok(c) => c,
							Err(e) => {
								return Ok(self.partial_result(
									format!("Invalid destination name: {e}"),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, false, Some(dest_display), cleanup_warnings,
								));
							}
						};
						let dest_fd = unsafe {
							libc::openat(dest_dir_pin.fd, c_dest.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW)
						};
						if dest_fd < 0 {
							return Ok(self.partial_result(
								format!("Destination {dest_display} was published, but cannot be opened for verification: {}. Source preserved. Move outcome is partial.", std::io::Error::last_os_error()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
							));
						}
						use std::os::unix::io::FromRawFd;
						let file = unsafe { std::fs::File::from_raw_fd(dest_fd) };

						use std::os::unix::io::AsRawFd;
						let mut dest_stat: libc::stat = unsafe { std::mem::zeroed() };
						if unsafe { libc::fstat(file.as_raw_fd(), &mut dest_stat) } != 0 {
							return Ok(self.partial_result(
								format!("Destination {dest_display} was published, but fstat failed: {}. Source preserved. Move outcome is partial.", std::io::Error::last_os_error()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
							));
						}
						let current_dest_dev_ino = (dest_stat.st_dev as u64, dest_stat.st_ino as u64);
						if current_dest_dev_ino != dest_dev_ino {
							return Ok(self.partial_result(
								format!("Destination {dest_display} was published, but its identity (dev/ino) does not match expected {dest_dev_ino:?} (found {current_dest_dev_ino:?}). Source preserved. Move outcome is partial."),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
							));
						}
						match dest_dir_pin.stat_file(dest_file_name) {
							Ok(Some(leaf_dev_ino)) if leaf_dev_ino == dest_dev_ino => {},
							Ok(Some(other)) => {
								return Ok(self.partial_result(
									format!("Destination {dest_display} directory entry does not match expected inode {dest_dev_ino:?} (found {other:?}). Source preserved. Move outcome is partial."),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
								));
							}
							_ => {
								return Ok(self.partial_result(
									format!("Destination {dest_display} directory entry missing before source unlink. Source preserved. Move outcome is partial."),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
								));
							}
						}
						let mut dest_bytes = Vec::new();
						let mut dest_rdr = &file;
						if let Err(e) = dest_rdr.read_to_end(&mut dest_bytes) {
							return Ok(self.partial_result(
								format!("Failed to read destination {dest_display} bytes: {e}. Source preserved. Move outcome is partial."),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
							));
						}
						if dest_bytes != op.content {
							return Ok(self.partial_result(
								format!("Destination {dest_display} content changed or does not match expected content. Source preserved. Move outcome is partial."),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
							));
						}
						file
					};

					// Destination is verified and held open! Now open, verify source file descriptor and leaf entry immediately before unlink!
					#[cfg(unix)]
					{
						let c_source = match to_cstring(file_name) {
							Ok(c) => c,
							Err(e) => {
								drop(dest_file);
								return Ok(self.partial_result(
									format!("Invalid source path: {e}"),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, false, Some(op.display.clone()), cleanup_warnings,
								));
							}
						};
						let fd = unsafe {
							libc::openat(dir_pin.fd, c_source.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW)
						};
						if fd < 0 {
							drop(dest_file);
							let err = std::io::Error::last_os_error();
							let msg = if err.raw_os_error() == Some(libc::ELOOP) {
								format!("Destination {dest_display} was published, but cannot move symlink {}: moving symlinks is not supported. Source preserved. Move outcome is partial.", op.display)
							} else {
								format!("Destination {dest_display} was published, but cannot pin source {}: {err}. Source preserved. Move outcome is partial.", op.display)
							};
							return Ok(self.partial_result(
								msg,
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(op.display.clone()), cleanup_warnings,
							));
						}
						use std::os::unix::io::FromRawFd;
						let mut source_file = unsafe { std::fs::File::from_raw_fd(fd) };

						use std::os::unix::io::AsRawFd;
						let mut final_source_stat: libc::stat = unsafe { std::mem::zeroed() };
						if unsafe { libc::fstat(source_file.as_raw_fd(), &mut final_source_stat) } != 0 {
							drop(dest_file);
							return Ok(self.partial_result(
								format!("Destination {dest_display} was published, but failed to re-fstat source {}: {}. Source was preserved. Move outcome is partial.", op.display, std::io::Error::last_os_error()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						if let Some(expected_dev_ino) = op.base_dev_ino {
							if (final_source_stat.st_dev as u64, final_source_stat.st_ino as u64) != expected_dev_ino {
								drop(dest_file);
								return Ok(self.partial_result(
									format!("Destination {dest_display} was published, but source {} dev/ino changed concurrently. Source was preserved. Move outcome is partial.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}
						if let Some(expected_base_mode) = op.base_mode {
							let final_mode = final_source_stat.st_mode as u32 & 0o7777;
							if final_mode != expected_base_mode {
								drop(dest_file);
								return Ok(self.partial_result(
									format!("Destination {dest_display} was published, but source {} mode changed concurrently. Source was preserved. Move outcome is partial.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}

						if let Err(e) = source_file.seek(SeekFrom::Start(0)) {
							drop(dest_file);
							return Ok(self.partial_result(
								format!("Destination {dest_display} was published, but failed to seek source {}: {e}. Source preserved. Move outcome is partial.", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						let mut final_source_bytes = Vec::new();
						if let Err(e) = source_file.read_to_end(&mut final_source_bytes) {
							drop(dest_file);
							return Ok(self.partial_result(
								format!("Destination {dest_display} was published, but failed to re-read source {}: {e}. Source preserved. Move outcome is partial.", op.display),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, false, Some(op.display.clone()), cleanup_warnings,
							));
						}
						if let Some(base) = &op.base {
							if &final_source_bytes != base {
								drop(dest_file);
								return Ok(self.partial_result(
									format!("Destination {dest_display} was published, but source {} content was modified in-place concurrently. Source preserved. Move outcome is partial.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}

						// Re-verify source leaf entry immediately before unlink
						match dir_pin.stat_file(file_name) {
							Ok(Some(leaf_dev_ino)) => {
								if let Some(expected_dev_ino) = op.base_dev_ino {
									if leaf_dev_ino != expected_dev_ino {
										drop(dest_file);
										return Ok(self.partial_result(
											format!("Destination {dest_display} was published, but source {} leaf entry in directory replaced. Source preserved. Move outcome is partial.", op.display),
											&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(op.display.clone()), cleanup_warnings,
										));
									}
								}
							}
							_ => {
								drop(dest_file);
								return Ok(self.partial_result(
									format!("Destination {dest_display} was published, but source {} leaf entry missing before unlink. Source preserved. Move outcome is partial.", op.display),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(op.display.clone()), cleanup_warnings,
								));
							}
						}

						// HIGH review fix: re-verify destination exact bytes from held dest_file fd
						// AFTER reading source to catch in-place destination corruption/truncation!
						let mut post_source_dest_stat: libc::stat = unsafe { std::mem::zeroed() };
						if unsafe { libc::fstat(dest_file.as_raw_fd(), &mut post_source_dest_stat) } != 0 {
							drop(dest_file);
							drop(source_file);
							return Ok(self.partial_result(
								format!("Destination {dest_display} fstat failed after source verification: {}. Source preserved. Move outcome is partial.", std::io::Error::last_os_error()),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
							));
						}
						let post_dest_dev_ino = (post_source_dest_stat.st_dev as u64, post_source_dest_stat.st_ino as u64);
						if post_dest_dev_ino != dest_dev_ino {
							drop(dest_file);
							drop(source_file);
							return Ok(self.partial_result(
								format!("Destination {dest_display} identity changed after source verification. Expected {dest_dev_ino:?}, found {post_dest_dev_ino:?}. Source preserved. Move outcome is partial."),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
							));
						}
						if let Err(e) = dest_file.seek(SeekFrom::Start(0)) {
							drop(dest_file);
							drop(source_file);
							return Ok(self.partial_result(
								format!("Failed to seek destination {dest_display} after source verification: {e}. Source preserved. Move outcome is partial."),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, false, Some(dest_display), cleanup_warnings,
							));
						}
						let mut final_dest_bytes = Vec::new();
						if let Err(e) = dest_file.read_to_end(&mut final_dest_bytes) {
							drop(dest_file);
							drop(source_file);
							return Ok(self.partial_result(
								format!("Failed to re-read destination {dest_display} bytes after source verification: {e}. Source preserved. Move outcome is partial."),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, false, Some(dest_display), cleanup_warnings,
							));
						}
						if final_dest_bytes != op.content {
							drop(dest_file);
							drop(source_file);
							return Ok(self.partial_result(
								format!("Destination {dest_display} content was modified in-place during source verification. Source preserved. Move outcome is partial."),
								&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
							));
						}

						// Re-verify destination directory entry immediately before source unlink to shrink race window
						match dest_dir_pin.stat_file(dest_file_name) {
							Ok(Some(leaf_dev_ino)) if leaf_dev_ino == dest_dev_ino => {},
							Ok(Some(other)) => {
								drop(dest_file);
								drop(source_file);
								return Ok(self.partial_result(
									format!("Destination {dest_display} directory entry replaced immediately before source unlink (expected {dest_dev_ino:?}, found {other:?}). Source preserved. Move outcome is partial."),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
								));
							}
							_ => {
								drop(dest_file);
								drop(source_file);
								return Ok(self.partial_result(
									format!("Destination {dest_display} directory entry missing immediately before source unlink. Source preserved. Move outcome is partial."),
									&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, true, Some(dest_display), cleanup_warnings,
								));
							}
						}
						// NOTE on residual noncooperating cross-file race:
						// Destination bytes are verified through held destination descriptor, and destination
						// directory entry is verified immediately prior to source unlinkat. However, POSIX
						// lacks a multi-file atomic move/unlink primitive; an uncooperative concurrent process
						// operating outside flock could mutate destination in the sub-microsecond window between
						// final verification and unlinkat. Absolute two-file atomicity is explicitly not claimed.
						drop(dest_file);
						drop(source_file);
					}

					// Unlink source relative to held dir_pin
					if let Err(e) = dir_pin.unlinkat_file(file_name) {
						return Ok(self.partial_result(
							format!(
								"Destination {dest_display} was published, but failed to unlink source {}: {e}. Source was preserved. Move outcome is partial.",
								op.display
							),
							&attempt_written, &attempt_invalidated, &attempt_created_dirs, &fresh_tags, true, false, Some(op.display.clone()), cleanup_warnings,
						));
					}

					attempt_invalidated.push(op.display.clone());
					self.cumulative_invalidated.lock().unwrap().push(op.display.clone());
					self.op_statuses.lock().unwrap()[idx] = OpStatus::Completed;
					let key = canonical_key(&op.path);
					self.snapshots.invalidate(&key);
				}
			}
		}
		for id in &self.conflict_ids {
			ctx.conflicts.invalidate(*id);
		}
		for op in &self.ops {
			ctx.conflicts.invalidate_path(&op.path);
		}

		let cum_written = self.cumulative_written.lock().unwrap().clone();
		let cum_invalidated = self.cumulative_invalidated.lock().unwrap().clone();
		let created_dirs = self.created_dirs.lock().unwrap().clone();

		let mut message = format!("Applied {} staged file operation{}.", total_ops, if total_ops == 1 { "" } else { "s" });
		for op in &self.ops {
			let line = if let Some(d) = &op.description {
				d.clone()
			} else {
				match op.kind {
					OpKind::Create => format!("Created {}", op.display),
					OpKind::Update => format!("Updated {}", op.display),
					OpKind::Delete => format!("Deleted {}", op.display),
					OpKind::Move => format!("Moved {} -> {}", op.display, op.rename_to_display.as_deref().unwrap_or("dest")),
				}
			};
			message.push('\n');
			message.push_str(&line);
		}
		if self.source_tool == "ast_edit" {
			if let Some(extra) = &self.extra_details {
				if let (Some(repls), Some(files)) = (extra.get("totalReplacements").and_then(|v| v.as_u64()), extra.get("filesTouched").and_then(|v| v.as_u64())) {
					message.push('\n');
					message.push_str(&format!("Applied {repls} replacement{} in {files} file{}.", if repls == 1 { "" } else { "s" }, if files == 1 { "" } else { "s" }));
				}
			}
		}
		if !fresh_tags.is_empty() {
			message.push_str("\n\n");
			message.push_str(&fresh_tags.join("\n"));
		}
		let mut details = json!({
			"applied": true,
			"writtenFiles": cum_written,
			"attemptWrittenFiles": attempt_written,
			"snapshotInvalidations": cum_invalidated,
			"attemptSnapshotInvalidations": attempt_invalidated,
			"createdDirectories": created_dirs,
			"attemptCreatedDirectories": attempt_created_dirs,
			"freshTags": fresh_tags,
		});
		let mut all_cleanup_warnings = self.cleanup_warnings.lock().unwrap().clone();
		for w in cleanup_warnings {
			if !all_cleanup_warnings.contains(&w) {
				all_cleanup_warnings.push(w);
			}
		}
		if !all_cleanup_warnings.is_empty() {
			details["cleanupWarnings"] = json!(all_cleanup_warnings);
		}
		if let Some(extra) = &self.extra_details {
			if let Some(obj) = details.as_object_mut() {
				if let Some(extra_obj) = extra.as_object() {
					for (k, v) in extra_obj {
						obj.insert(k.clone(), v.clone());
					}
				}
			}
		}
		let mut res = tool_result(message, false);
		res.details = Some(details);
		Ok(res)
	}

	async fn reject(&self, _ctx: &AgentContext, reason: &str) -> Result<ToolResult, ToolError> {
		let cum_written = self.cumulative_written.lock().unwrap().clone();
		let cum_created_dirs = self.created_dirs.lock().unwrap().clone();
		let mut message = format!("Discarded: {}. Reason: {reason}", self.label());
		if !cum_written.is_empty() {
			message.push_str(&format!(
				" (Note: {} file(s) were previously written by an earlier attempt and remain modified: {})",
				cum_written.len(),
				cum_written.join(", ")
			));
		}
		if !cum_created_dirs.is_empty() {
			message.push_str(&format!(
				" (Note: {} director{} previously created by an earlier attempt remain in workspace; discard does not roll back directory creation: {})",
				cum_created_dirs.len(),
				if cum_created_dirs.len() == 1 { "y" } else { "ies" },
				cum_created_dirs.join(", ")
			));
		}
		let mut res = tool_result(message, false);
		res.details = Some(json!({
			"action": "discard",
			"reason": reason,
			"previouslyWritten": cum_written,
			"createdDirectories": cum_created_dirs,
			"directoriesRolledBack": false,
		}));
		Ok(res)
	}
}
