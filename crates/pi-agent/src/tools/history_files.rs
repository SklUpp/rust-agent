//! Descriptor-relative, no-symlink filesystem access for `history://`.
//!
//! Authorization anchors at `/` and opens EVERY path component with
//! `O_NOFOLLOW|O_DIRECTORY`, so a symlink in any ancestor (or the caller root
//! itself) cannot redirect scanning or reading. Empty names, `..`, and
//! non-normal components are rejected rather than normalized.

#[cfg(unix)]
mod imp {
	use std::{
		ffi::{CStr, CString, OsStr, OsString},
		fs::File,
		io,
		os::{
			fd::{AsRawFd, FromRawFd, OwnedFd},
			unix::ffi::{OsStrExt, OsStringExt},
		},
		path::{Component, Path, PathBuf},
	};

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	pub enum EntryKind {
		Dir,
		File,
		Other,
	}

	/// One directory opened without following any symlink on the way in.
	#[derive(Debug)]
	pub struct PinnedDir {
		fd:      OwnedFd,
		display: PathBuf,
		dev:     u64,
		ino:     u64,
	}

	fn last_error() -> io::Error {
		io::Error::last_os_error()
	}

	/// Directory traversal flags. Linux/Android `O_PATH` opens a directory
	/// anchor without read permission on its ancestors (Termux app sandboxes
	/// expose `/data/data` as execute-only), while `O_DIRECTORY|O_NOFOLLOW`
	/// still rejects a symlink at every component. Other Unix targets keep the
	/// readable flags: their authorized roots are traversable that way.
	#[cfg(any(target_os = "linux", target_os = "android"))]
	const TRAVERSE_FLAGS: libc::c_int = libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
	#[cfg(not(any(target_os = "linux", target_os = "android")))]
	const TRAVERSE_FLAGS: libc::c_int = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;

	fn open_dir_at(dir: &OwnedFd, name: &OsStr) -> io::Result<OwnedFd> {
		let name_c = CString::new(name.as_bytes())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "history:// path component contains a NUL"))?;
		// SAFETY: `dir` is an open directory fd and `name_c` is NUL-terminated.
		let fd = unsafe {
			libc::openat(
				dir.as_raw_fd(),
				name_c.as_ptr(),
				libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
			)
		};
		if fd < 0 {
			return Err(last_error());
		}
		// SAFETY: fresh descriptor owned by this call.
		Ok(unsafe { OwnedFd::from_raw_fd(fd) })
	}

	fn open_traverse_at(dir: &OwnedFd, name: &OsStr) -> io::Result<OwnedFd> {
		let name_c = CString::new(name.as_bytes())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "history:// path component contains a NUL"))?;
		// SAFETY: `dir` is an open directory fd and `name_c` is NUL-terminated.
		let fd = unsafe { libc::openat(dir.as_raw_fd(), name_c.as_ptr(), TRAVERSE_FLAGS) };
		if fd < 0 {
			return Err(last_error());
		}
		// SAFETY: fresh descriptor owned by this call.
		Ok(unsafe { OwnedFd::from_raw_fd(fd) })
	}

	/// Reopen a pinned directory readably through its descriptor (`.`), never
	/// by pathname, so enumeration sees the exact inode that was pinned.
	fn open_readable(dir: &OwnedFd) -> io::Result<OwnedFd> {
		// SAFETY: `.` is a literal NUL-terminated path relative to a pinned dir.
		let fd = unsafe {
			libc::openat(
				dir.as_raw_fd(),
				c".".as_ptr(),
				libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
			)
		};
		if fd < 0 {
			return Err(last_error());
		}
		// SAFETY: fresh descriptor owned by this call.
		Ok(unsafe { OwnedFd::from_raw_fd(fd) })
	}

	fn stat_fd(fd: &OwnedFd) -> io::Result<(u64, u64)> {
		// SAFETY: `stat` is zeroed and the fd is open.
		let mut stat: libc::stat = unsafe { std::mem::zeroed() };
		// SAFETY: valid fd and out-pointer.
		if unsafe { libc::fstat(fd.as_raw_fd(), &mut stat) } != 0 {
			return Err(last_error());
		}
		Ok((stat.st_dev as u64, stat.st_ino as u64))
	}

	impl PinnedDir {
		/// Open an absolute path from `/`, rejecting a symlink at any component.
		///
		/// Components are pinned with [`TRAVERSE_FLAGS`] (Linux/Android
		/// `O_PATH`), so an execute-only platform ancestor such as `/data/data`
		/// is traversable without being readable. Only the authorized final
		/// root is reopened readably via `.` on that pinned descriptor.
		pub fn open(path: &Path) -> io::Result<Self> {
			if !path.is_absolute() {
				return Err(io::Error::new(
					io::ErrorKind::InvalidInput,
					format!("history:// root is not absolute: {}", path.display()),
				));
			}
			// SAFETY: literal NUL-terminated path.
			let mut current = unsafe {
				let root = libc::open(c"/".as_ptr(), TRAVERSE_FLAGS);
				if root < 0 {
					return Err(last_error());
				}
				OwnedFd::from_raw_fd(root)
			};
			let mut seen_component = false;
			for component in path.components() {
				match component {
					Component::RootDir => {},
					Component::CurDir => {},
					Component::Normal(name) => {
						seen_component = true;
						current = open_traverse_at(&current, name)?;
					},
					Component::ParentDir | Component::Prefix(_) => {
						return Err(io::Error::new(
							io::ErrorKind::InvalidInput,
							format!("history:// root contains a non-normal component: {}", path.display()),
						));
					},
				}
			}
			if !seen_component {
				return Err(io::Error::new(
					io::ErrorKind::InvalidInput,
					format!("history:// root is empty: {}", path.display()),
				));
			}
			let pinned = stat_fd(&current)?;
			let readable = open_readable(&current)?;
			let (dev, ino) = stat_fd(&readable)?;
			if (dev, ino) != pinned {
				return Err(io::Error::new(
					io::ErrorKind::InvalidData,
					format!("history:// root changed while pinning: {}", path.display()),
				));
			}
			Ok(Self {
				fd: readable,
				display: path.to_path_buf(),
				dev,
				ino,
			})
		}

		#[must_use]
		pub fn display(&self) -> &Path {
			&self.display
		}

		#[must_use]
		pub fn same_file(&self, other: &Self) -> bool {
			self.dev == other.dev && self.ino == other.ino
		}

		pub fn list(&self) -> io::Result<Vec<OsString>> {
			let fd = unsafe { libc::dup(self.fd.as_raw_fd()) };
			if fd < 0 {
				return Err(last_error());
			}
			// SAFETY: `fdopendir` takes ownership of `fd` on success.
			let directory = unsafe { libc::fdopendir(fd) };
			if directory.is_null() {
				unsafe { libc::close(fd) };
				return Err(last_error());
			}
			let mut names = Vec::new();
			loop {
				// SAFETY: `directory` is open until `closedir` below.
				let entry = unsafe { libc::readdir(directory) };
				if entry.is_null() {
					break;
				}
				// SAFETY: `d_name` is a NUL-terminated string in the dirent.
				let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
				let bytes = name.to_bytes();
				if bytes != b"." && bytes != b".." {
					names.push(OsString::from_vec(bytes.to_vec()));
				}
			}
			// SAFETY: closes the directory stream exactly once.
			unsafe { libc::closedir(directory) };
			names.sort();
			Ok(names)
		}

		pub fn kind(&self, name: &OsStr) -> io::Result<EntryKind> {
			let name_c = CString::new(name.as_bytes())
				.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "history:// path component contains a NUL"))?;
			// SAFETY: `stat` is zeroed; fd/name are valid; nofollow.
			let mut stat: libc::stat = unsafe { std::mem::zeroed() };
			if unsafe {
				libc::fstatat(
					self.fd.as_raw_fd(),
					name_c.as_ptr(),
					&mut stat,
					libc::AT_SYMLINK_NOFOLLOW,
				)
			} != 0
			{
				return Err(last_error());
			}
			let mode = stat.st_mode & libc::S_IFMT;
			if mode == libc::S_IFDIR {
				Ok(EntryKind::Dir)
			} else if mode == libc::S_IFREG {
				Ok(EntryKind::File)
			} else {
				Ok(EntryKind::Other)
			}
		}

		pub fn open_dir(&self, name: &OsStr) -> io::Result<Self> {
			let fd = open_dir_at(&self.fd, name)?;
			let (dev, ino) = stat_fd(&fd)?;
			Ok(Self {
				fd,
				display: self.display.join(name),
				dev,
				ino,
			})
		}

		/// Open one regular leaf below this directory, never following a symlink.
		pub fn open_file(&self, name: &OsStr) -> io::Result<File> {
			let name_c = CString::new(name.as_bytes())
				.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "history:// path component contains a NUL"))?;
			// SAFETY: valid directory fd and NUL-terminated name.
			let fd = unsafe {
				libc::openat(
					self.fd.as_raw_fd(),
					name_c.as_ptr(),
					libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
				)
			};
			if fd < 0 {
				return Err(last_error());
			}
			// SAFETY: fresh descriptor moved into `File`.
			let file = File::from(unsafe { OwnedFd::from_raw_fd(fd) });
			if !file.metadata()?.is_file() {
				return Err(io::Error::new(
					io::ErrorKind::InvalidInput,
					"history:// transcript is not a regular file",
				));
			}
			Ok(file)
		}

		/// Open a relative component list from this pinned root, nofollow each.
		pub fn open_relative(&self, components: &[OsString]) -> io::Result<File> {
			let Some((leaf, parents)) = components.split_last() else {
				return Err(io::Error::new(
					io::ErrorKind::InvalidInput,
					"history:// transcript path is empty",
				));
			};
			let mut current = self.open_dir_or_self(parents.first())?;
			for component in parents.iter().skip(1) {
				current = current.open_dir(component)?;
			}
			current.open_file(leaf)
		}

		fn open_dir_or_self(&self, first: Option<&OsString>) -> io::Result<Self> {
			match first {
				Some(name) => self.open_dir(name),
				None => {
					let fd = unsafe { libc::dup(self.fd.as_raw_fd()) };
					if fd < 0 {
						return Err(last_error());
					}
					let fd = unsafe { OwnedFd::from_raw_fd(fd) };
					let (dev, ino) = stat_fd(&fd)?;
					Ok(Self {
						fd,
						display: self.display.clone(),
						dev,
						ino,
					})
				},
			}
		}
	}
}

#[cfg(not(unix))]
mod imp {
	use std::{
		ffi::{OsStr, OsString},
		fs::File,
		io,
		path::Path,
	};

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	pub enum EntryKind {
		Dir,
		File,
		Other,
	}

	#[derive(Debug)]
	pub struct PinnedDir;

	impl PinnedDir {
		pub fn open(_path: &Path) -> io::Result<Self> {
			Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"history:// needs descriptor-relative traversal; unsupported on this platform",
			))
		}

		#[must_use]
		pub fn display(&self) -> &Path {
			Path::new("")
		}

		#[must_use]
		pub fn same_file(&self, _other: &Self) -> bool {
			false
		}

		pub fn list(&self) -> io::Result<Vec<OsString>> {
			Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"history:// needs descriptor-relative traversal; unsupported on this platform",
			))
		}

		pub fn kind(&self, _name: &OsStr) -> io::Result<EntryKind> {
			Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"history:// needs descriptor-relative traversal; unsupported on this platform",
			))
		}

		pub fn open_dir(&self, _name: &OsStr) -> io::Result<Self> {
			Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"history:// needs descriptor-relative traversal; unsupported on this platform",
			))
		}

		pub fn open_file(&self, _name: &OsStr) -> io::Result<File> {
			Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"history:// needs descriptor-relative traversal; unsupported on this platform",
			))
		}

		pub fn open_relative(&self, _components: &[OsString]) -> io::Result<File> {
			Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"history:// needs descriptor-relative traversal; unsupported on this platform",
			))
		}
	}
}

pub use imp::{EntryKind, PinnedDir};
