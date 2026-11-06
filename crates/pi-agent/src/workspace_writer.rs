//! Cross-process writer lease and strict full-version guard for file mutation.
//!
//! The lease is one `flock` on a lock file in a profile-independent per-user
//! coordination root. A second writer on the same path (another process or
//! another descriptor in this process) gets an explicit
//! [`LockError::Occupied`] instead of blocking; the host can queue and retry.
//! Symlink aliases resolve to the same canonical identity and therefore the
//! same file lock. [`WorkspaceWriter::acquire_workspace`] adds the wider
//! workspace-owner scope that D23 needs; it is not claimed as active until the
//! host actually holds it for the agent lifetime.
//!
//! This coordinator is not a sandbox and does not exclude an external editor:
//! `flock` only serializes writers that use this API. A commit therefore also
//! re-reads the full raw file and its `(dev, ino)` immediately before the final
//! `rename`. That window is still **not** an atomic compare-and-swap: another
//! process can replace the file between the check and the rename. The guard is
//! best-effort stale detection, not a claim of CAS or a multi-file transaction.
//!
//! A [`FileVersion`] carries the exact raw bytes that a caller read, not only a
//! short content tag. Overwrites must pass that version to
//! [`replace_if_unchanged`], which rejects different raw bytes even when their
//! normalized 4-hex UI tag collides.

use std::{
	fs::{File, OpenOptions},
	io::{self, Read, Write},
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

/// Largest file a version/guarded write will read or publish.
pub const VERSION_MAX_BYTES: u64 = 4 * 1024 * 1024;
/// Env override for the lock directory (tests and profile-aware hosts).
pub const LOCK_DIR_ENV: &str = "OMP_WORKSPACE_WRITER_LOCK_DIR";

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Resolve a path to its actual workspace identity: the symlink target when it
/// exists, otherwise the resolved parent plus file name, otherwise an absolute
/// path against the current directory. Two aliases of one file share a key.
pub fn canonical_identity(path: &Path) -> PathBuf {
	if let Ok(resolved) = std::fs::canonicalize(path) {
		return resolved;
	}
	if let (Some(parent), Some(name)) = (path.parent(), path.file_name()) {
		let parent = if parent.as_os_str().is_empty() { Path::new(".") } else { parent };
		if let Ok(resolved) = std::fs::canonicalize(parent) {
			return resolved.join(name);
		}
	}
	if path.is_absolute() {
		path.to_path_buf()
	} else {
		std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
	}
}

/// Deterministic, profile-independent lock root under the current user's home.
///
/// The root is one coordination namespace for every profile and data root of
/// this UID: `$HOME/.omp/run/workspace-writer`. `PI_CONFIG_DIR` and profile
/// variables are deliberately ignored, so CLI/TUI/headless runs of the same
/// project cannot split into parallel writer namespaces. The explicit
/// [`LOCK_DIR_ENV`] override replaces that one root; it does not create a
/// second namespace. There is no `/tmp` assumption: without a home, an
/// absolute `TMPDIR` is the only fallback, and resolution otherwise fails
/// closed.
pub fn default_lock_dir() -> io::Result<PathBuf> {
	if let Some(exact) = env_nonempty(LOCK_DIR_ENV) {
		return Ok(exact);
	}
	if let Some(home) = env_nonempty("HOME").or_else(|| env_nonempty("USERPROFILE")) {
		return Ok(home.join(".omp").join("run").join("workspace-writer"));
	}
	if let Some(temp) = env_nonempty("TMPDIR").filter(|path| path.is_absolute()) {
		return Ok(temp.join(format!("omp-workspace-writer-{}", current_uid())));
	}
	Err(io::Error::new(
		io::ErrorKind::NotFound,
		"cannot resolve a per-user coordination root; set OMP_WORKSPACE_WRITER_LOCK_DIR",
	))
}

fn env_nonempty(key: &str) -> Option<PathBuf> {
	std::env::var_os(key).map(PathBuf::from).filter(|path| !path.as_os_str().is_empty())
}

fn current_uid() -> u32 {
	#[cfg(unix)]
	{
		// SAFETY: geteuid has no preconditions and cannot fail.
		unsafe { libc::geteuid() }
	}
	#[cfg(not(unix))]
	{
		0
	}
}

/// Cross-process writer coordinator. Cheap to construct; the lock directory is
/// created on first acquire.
#[derive(Debug, Clone)]
pub struct WorkspaceWriter {
	lock_dir: PathBuf,
}

impl WorkspaceWriter {
	/// Open the configured runtime lock root. An unresolvable or unwritable
	/// root fails closed; it never falls back to the source tree.
	pub fn new() -> io::Result<Self> {
		let lock_dir = default_lock_dir()?;
		ensure_private_lock_dir(&lock_dir)?;
		Ok(Self { lock_dir })
	}

	/// Use an explicit lock root (tests, profile-aware host). The same
	/// ownership/0700/no-symlink validation runs on first acquire.
	pub fn with_lock_dir(lock_dir: PathBuf) -> Self {
		Self { lock_dir }
	}

	pub fn lock_dir(&self) -> &Path {
		&self.lock_dir
	}

	/// Try to take the exclusive file lease for `path` without blocking.
	///
	/// The canonical identity is resolved first, so symlink aliases map to one
	/// lock. The returned lease releases the kernel lock on drop; a crashed
	/// process releases it too, leaving no stale lock state.
	pub fn acquire(&self, path: &Path) -> Result<WorkspaceLease, LockError> {
		let canonical = canonical_identity(path);
		let name = file_lock_name(&canonical);
		self.acquire_named(canonical, name)
	}

	/// Try to take the workspace-wide lease for `root` without blocking.
	///
	/// Host integration holds this for the agent/workspace owner lifetime; it
	/// serializes every file writer in that workspace, not one pathname. D23
	/// remains pending until the host actually keeps this lease.
	pub fn acquire_workspace(&self, root: &Path) -> Result<WorkspaceScopeLease, LockError> {
		let canonical = canonical_identity(root);
		let name = workspace_key(&canonical);
		self.acquire_named(canonical, name)
	}

	fn acquire_named(
		&self,
		canonical: PathBuf,
		name: String,
	) -> Result<WorkspaceLease, LockError> {
		ensure_private_lock_dir(&self.lock_dir).map_err(LockError::Io)?;
		let lock_path = self.lock_dir.join(name);
		let file = open_lock_file(&lock_path).map_err(LockError::Io)?;
		match try_lock_exclusive(&file) {
			Ok(()) => Ok(WorkspaceLease { file, canonical }),
			Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
				Err(LockError::Occupied { canonical })
			},
			Err(error) => Err(LockError::Io(error)),
		}
	}
}

/// Stable workspace lock key for `root`. Hosts can persist/log this identity
/// without depending on path spelling or symlink aliases.
pub fn workspace_key(root: &Path) -> String {
	format!("workspace-{}.lock", lock_hash(&canonical_identity(root)))
}

/// Held exclusive lease. Dropping it unlocks (`flock` is also released when the
/// descriptor closes).
#[derive(Debug)]
pub struct WorkspaceLease {
	file: File,
	canonical: PathBuf,
}

/// Workspace-owner lease: the host holds this value for the agent/workspace
/// lifetime. Dropping it ends the D23 scope.
pub type WorkspaceScopeLease = WorkspaceLease;

impl WorkspaceLease {
	pub fn canonical(&self) -> &Path {
		&self.canonical
	}
}

impl Drop for WorkspaceLease {
	fn drop(&mut self) {
		#[cfg(unix)]
		{
			use std::os::unix::io::AsRawFd;
			// SAFETY: the descriptor is valid until `file` drops below; `flock`
			// has no memory-safety preconditions.
			unsafe {
				libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
			}
		}
	}
}

/// Exclusive non-blocking `flock`.
#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> io::Result<()> {
	use std::os::unix::io::AsRawFd;
	// SAFETY: the descriptor is owned by `file` and valid for the call.
	if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
		Ok(())
	} else {
		Err(io::Error::last_os_error())
	}
}

#[cfg(not(unix))]
fn try_lock_exclusive(_file: &File) -> io::Result<()> {
	Err(io::Error::new(
		io::ErrorKind::Unsupported,
		"workspace writer leases need POSIX flock on this platform",
	))
}

fn open_lock_file(path: &Path) -> io::Result<File> {
	let mut options = OpenOptions::new();
	options.read(true).write(true).create(true);
	#[cfg(unix)]
	{
		use std::os::unix::fs::OpenOptionsExt;
		options.mode(0o600).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
	}
	options.open(path)
}

fn file_lock_name(canonical: &Path) -> String {
	format!("file-{}.lock", lock_hash(canonical))
}

fn lock_hash(canonical: &Path) -> String {
	#[cfg(unix)]
	{
		use std::os::unix::ffi::OsStrExt;
		format!("{:016x}", xxhash_rust::xxh64::xxh64(canonical.as_os_str().as_bytes(), 0))
	}
	#[cfg(not(unix))]
	{
		format!(
			"{:016x}",
			xxhash_rust::xxh64::xxh64(canonical.to_string_lossy().as_bytes(), 0)
		)
	}
}

/// Create/validate the coordination directory: existing directory, owned by
/// this user, not a symlink, mode 0700. Any mismatch fails closed.
fn ensure_private_lock_dir(dir: &Path) -> io::Result<()> {
	std::fs::create_dir_all(dir)?;
	let metadata = std::fs::symlink_metadata(dir)?;
	if metadata.file_type().is_symlink() {
		return Err(io::Error::new(
			io::ErrorKind::PermissionDenied,
			format!("{} is a symlink; refusing to use it as a lock root", dir.display()),
		));
	}
	if !metadata.is_dir() {
		return Err(io::Error::new(
			io::ErrorKind::NotADirectory,
			format!("{} is not a directory", dir.display()),
		));
	}
	#[cfg(unix)]
	{
		use std::os::unix::fs::{MetadataExt, PermissionsExt};
		if metadata.uid() != current_uid() {
			return Err(io::Error::new(
				io::ErrorKind::PermissionDenied,
				format!(
					"{} is owned by uid {} not the current user",
					dir.display(),
					metadata.uid()
				),
			));
		}
		if metadata.permissions().mode() & 0o777 != 0o700 {
			std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
		}
	}
	Ok(())
}

/// Exact raw version of one file as observed by a read: full bytes plus the
/// inode identity where the platform exposes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileVersion {
	canonical: PathBuf,
	raw: Arc<[u8]>,
	dev_ino: Option<(u64, u64)>,
}

impl FileVersion {
	/// Read the current full raw bytes and inode identity.
	pub fn read(path: &Path) -> Result<Self, VersionError> {
		let canonical = canonical_identity(path);
		let metadata = std::fs::metadata(&canonical).map_err(VersionError::Io)?;
		if !metadata.is_file() {
			return Err(VersionError::NotRegular { path: canonical });
		}
		if metadata.len() > VERSION_MAX_BYTES {
			return Err(VersionError::TooLarge { path: canonical, size: metadata.len() });
		}
		let mut file = File::open(&canonical).map_err(VersionError::Io)?;
		let mut raw = Vec::new();
		(&mut file)
			.take(VERSION_MAX_BYTES + 1)
			.read_to_end(&mut raw)
			.map_err(VersionError::Io)?;
		if raw.len() as u64 > VERSION_MAX_BYTES {
			return Err(VersionError::TooLarge {
				path: canonical,
				size: raw.len() as u64,
			});
		}
		Ok(Self { canonical, raw: Arc::from(raw), dev_ino: dev_ino(&metadata) })
	}

	/// Build a version from bytes a caller already read (for example the exact
	/// source bytes captured while preparing a hashline edit). The [`Arc`] is
	/// shared with the snapshot store, not copied.
	pub fn from_parts(canonical: PathBuf, raw: Arc<[u8]>, dev_ino: Option<(u64, u64)>) -> Self {
		Self { canonical, raw, dev_ino }
	}

	pub fn canonical(&self) -> &Path {
		&self.canonical
	}

	pub fn raw(&self) -> &[u8] {
		self.raw.as_ref()
	}

	pub fn dev_ino(&self) -> Option<(u64, u64)> {
		self.dev_ino
	}

	/// Re-read the target and require identical raw bytes and inode identity.
	pub fn verify(&self) -> Result<(), VersionError> {
		match Self::read(&self.canonical) {
			Ok(current) => {
				if current.raw == self.raw && current.dev_ino == self.dev_ino {
					Ok(())
				} else {
					Err(VersionError::Stale { path: self.canonical.clone() })
				}
			},
			Err(VersionError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
				Err(VersionError::Stale { path: self.canonical.clone() })
			},
			Err(error) => Err(error),
		}
	}
}

#[cfg(unix)]
fn dev_ino(metadata: &std::fs::Metadata) -> Option<(u64, u64)> {
	use std::os::unix::fs::MetadataExt;
	Some((metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn dev_ino(_metadata: &std::fs::Metadata) -> Option<(u64, u64)> {
	None
}

/// Publish `bytes` over `path` only while the target still matches `expected`.
///
/// The bytes go to a same-directory temporary file first; the expected raw
/// version and inode are re-read after that write and immediately before the
/// final `rename`. This narrows, but does not close, the external-editor race:
/// it is deliberately not advertised as atomic CAS.
pub fn replace_if_unchanged(
	path: &Path,
	expected: &FileVersion,
	bytes: &[u8],
	mode: Option<u32>,
) -> Result<(), WriteError> {
	let target = canonical_identity(path);
	let metadata = match std::fs::metadata(&target) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == io::ErrorKind::NotFound => {
			return Err(WriteError::Version(VersionError::Stale { path: target }));
		},
		Err(error) => return Err(WriteError::Io(error)),
	};
	if metadata.is_dir() {
		return Err(WriteError::Io(io::Error::new(
			io::ErrorKind::IsADirectory,
			format!("{} is a directory", target.display()),
		)));
	}
	let parent = target.parent().unwrap_or_else(|| Path::new("."));
	let base = target
		.file_name()
		.map(|name| name.to_string_lossy().into_owned())
		.unwrap_or_else(|| "file".to_string());
	let effective_mode = mode.or_else(|| existing_mode(&metadata));

	let (temp_path, mut temp_file) = create_exclusive_temp(parent, &base, effective_mode)?;
	let mut guard = TempGuard::new(temp_path);
	temp_file.write_all(bytes).map_err(WriteError::Io)?;
	temp_file.flush().map_err(WriteError::Io)?;
	// Restore the exact published mode explicitly: the temp file was created
	// under the process umask, and that must not leak into the result.
	if let Some(mode) = effective_mode {
		set_explicit_mode(guard.path(), mode).map_err(WriteError::Io)?;
	}
	expected.verify().map_err(WriteError::Version)?;
	std::fs::rename(guard.path(), &target).map_err(WriteError::Io)?;
	guard.disarm();
	Ok(())
}

#[cfg(unix)]
fn set_explicit_mode(path: &Path, mode: u32) -> io::Result<()> {
	use std::os::unix::fs::PermissionsExt;
	std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode & 0o7777))
}

#[cfg(not(unix))]
fn set_explicit_mode(_path: &Path, _mode: u32) -> io::Result<()> {
	Ok(())
}

#[cfg(unix)]
fn existing_mode(metadata: &std::fs::Metadata) -> Option<u32> {
	use std::os::unix::fs::PermissionsExt;
	Some(metadata.permissions().mode() & 0o7777)
}

#[cfg(not(unix))]
fn existing_mode(_metadata: &std::fs::Metadata) -> Option<u32> {
	None
}

fn create_exclusive_temp(
	parent: &Path,
	base: &str,
	mode: Option<u32>,
) -> Result<(PathBuf, File), WriteError> {
	for _ in 0..64 {
		let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
		let path = parent.join(format!(
			".{base}.writer-{}-{counter:016x}",
			std::process::id()
		));
		let mut options = OpenOptions::new();
		options.write(true).create_new(true);
		#[cfg(unix)]
		{
			use std::os::unix::fs::OpenOptionsExt;
			options.mode(mode.unwrap_or(0o666));
		}
		match options.open(&path) {
			Ok(file) => return Ok((path, file)),
			Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {},
			Err(error) => return Err(WriteError::Io(error)),
		}
	}
	Err(WriteError::Io(io::Error::new(
		io::ErrorKind::AlreadyExists,
		"could not create a writer temporary file after retries",
	)))
}

struct TempGuard {
	path: Option<PathBuf>,
}

impl TempGuard {
	fn new(path: PathBuf) -> Self {
		Self { path: Some(path) }
	}

	fn path(&self) -> &Path {
		self.path.as_deref().unwrap_or_else(|| Path::new("."))
	}

	fn disarm(&mut self) {
		self.path = None;
	}
}

impl Drop for TempGuard {
	fn drop(&mut self) {
		if let Some(path) = &self.path {
			let _ = std::fs::remove_file(path);
		}
	}
}

/// Lease acquisition failure. `Occupied` is an expected state for the async
/// queue host, not an I/O error.
#[derive(Debug)]
pub enum LockError {
	/// Another writer holds the lease for this canonical identity.
	Occupied { canonical: PathBuf },
	Io(io::Error),
}

impl std::fmt::Display for LockError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Occupied { canonical } => {
				write!(formatter, "another writer holds the lease for {}", canonical.display())
			},
			Self::Io(error) => write!(formatter, "workspace writer lease failed: {error}"),
		}
	}
}

impl std::error::Error for LockError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Io(error) => Some(error),
			Self::Occupied { .. } => None,
		}
	}
}

/// Full-version read/verification failure.
#[derive(Debug)]
pub enum VersionError {
	Io(io::Error),
	NotRegular { path: PathBuf },
	TooLarge { path: PathBuf, size: u64 },
	/// The file no longer matches the recorded raw bytes or inode.
	Stale { path: PathBuf },
}

impl std::fmt::Display for VersionError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Io(error) => write!(formatter, "version read failed: {error}"),
			Self::NotRegular { path } => write!(formatter, "{} is not a regular file", path.display()),
			Self::TooLarge { path, size } => {
				write!(formatter, "{} is {size} bytes, above the {VERSION_MAX_BYTES} limit", path.display())
			},
			Self::Stale { path } => write!(
				formatter,
				"{} changed since it was read (raw content or inode no longer matches)",
				path.display()
			),
		}
	}
}

impl std::error::Error for VersionError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Io(error) => Some(error),
			_ => None,
		}
	}
}

/// Guarded write failure.
#[derive(Debug)]
pub enum WriteError {
	Version(VersionError),
	Io(io::Error),
}

impl WriteError {
	/// Whether the target no longer matched the read version.
	pub fn is_stale(&self) -> bool {
		matches!(self, Self::Version(VersionError::Stale { .. }))
	}
}

impl std::fmt::Display for WriteError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Version(error) => std::fmt::Display::fmt(error, formatter),
			Self::Io(error) => write!(formatter, "guarded write failed: {error}"),
		}
	}
}

impl std::error::Error for WriteError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Version(error) => Some(error),
			Self::Io(error) => Some(error),
		}
	}
}

impl From<io::Error> for WriteError {
	fn from(error: io::Error) -> Self {
		Self::Io(error)
	}
}
