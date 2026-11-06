//! Saved subagent output IDs and first-publication of `<id>.md`.
//!
//! Mirrors `packages/coding-agent/src/task/output-manager.ts` for the ID
//! ownership half that `agent://` needs: a requested name is used verbatim the
//! first time, a repeat gets `-2`, `-3`, …; nested children are allocated
//! under their parent output id (`Parent.Child`); the id set is seeded from
//! `<artifact>.md` and `<child>.jsonl` files already on disk so a restarted
//! session never reuses a name; and `__advisor` is reserved because a child
//! allocated that stem would write `__advisor.jsonl` over the advisor
//! transcript.
//!
//! IDs are deliberately separate from the supervisor's `task-N` lifecycle
//! identity and from `result_key`: those keep their existing sequencing and
//! accounting meaning. `output_id` is the stable `agent://` handle.
//!
//! Publication is no-clobber for the initial run for an id: it owns the name,
//! and a competing writer must choose another candidate rather than replace
//! bytes. A follow-up turn that produced a real `yield` deliberately replaces
//! its own `<id>.md` through [`publish_agent_output_replace`], matching the TS
//! `replaceFileAtomically` path; a yield-less wake leaves the completed
//! artifact untouched.

use std::{
	collections::HashSet,
	fs::{self, OpenOptions},
	io::Write,
	path::{Path, PathBuf},
	sync::{
		Mutex,
		atomic::{AtomicU64, Ordering},
	},
	time::{SystemTime, UNIX_EPOCH},
};

use super::atomic_publish::publish_staged_file;

/// Advisor transcript stem reserved in every allocation scope.
pub const ADVISOR_OUTPUT_STEM: &str = "__advisor";

/// One output-id allocation scope (one supervisor / session tree).
#[derive(Default)]
pub(crate) struct OutputIdAllocator {
	state: Mutex<OutputIdState>,
}

#[derive(Default)]
struct OutputIdState {
	/// Directory whose `.md`/`.jsonl` entries seeded `taken`, if any.
	seeded_root: Option<PathBuf>,
	taken:       HashSet<String>,
}

impl OutputIdAllocator {
	/// Allocate a unique output id.
	///
	/// `prefix` is the parent's full output id for a nested child; `None` at
	/// the root. `root` is the artifacts directory, scanned once so a restart
	/// cannot hand out an id whose file already exists.
	pub fn allocate(
		&self,
		root: Option<&Path>,
		requested: &str,
		prefix: Option<&str>,
	) -> String {
		let mut state = self
			.state
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		Self::seed(&mut state, root);
		let requested = if requested.is_empty() { "task" } else { requested };
		let mut candidate = requested.to_string();
		let mut suffix = 2u64;
		while Self::taken(&state.taken, prefix, &candidate) {
			candidate = format!("{requested}-{suffix}");
			suffix += 1;
		}
		let full = match prefix.filter(|prefix| !prefix.is_empty()) {
			Some(prefix) => format!("{prefix}.{candidate}"),
			None => candidate,
		};
		state.taken.insert(full.clone());
		full
	}

	/// Mark a full id as taken after a publication race lost to another writer.
	pub fn reserve(&self, id: &str) {
		let mut state = self
			.state
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		state.taken.insert(id.to_string());
	}

	fn seed(state: &mut OutputIdState, root: Option<&Path>) {
		let Some(root) = root else {
			return;
		};
		if state.seeded_root.as_deref() == Some(root) {
			return;
		}
		if let Ok(entries) = fs::read_dir(root) {
			for entry in entries.flatten() {
				let Some(name) = entry.file_name().to_str().map(str::to_string) else {
					continue;
				};
				let Some(stem) = output_stem(&name) else {
					continue;
				};
				if !stem.is_empty() {
					state.taken.insert(stem.to_string());
				}
			}
		}
		state.seeded_root = Some(root.to_path_buf());
	}

	/// TS `AgentOutputManager` seeds from both `.md` output artifacts and
	/// `.jsonl` child transcripts; a dotted file is a nested child, and only
	/// the first segment belongs to this scope.
	fn taken(taken: &HashSet<String>, prefix: Option<&str>, candidate: &str) -> bool {
		if candidate == ADVISOR_OUTPUT_STEM {
			return true;
		}
		let prefix = prefix
			.filter(|prefix| !prefix.is_empty())
			.map(|prefix| format!("{prefix}."));
		for id in taken {
			let rest = match &prefix {
				Some(prefix) => match id.strip_prefix(prefix) {
					Some(rest) => rest,
					None => continue,
				},
				None => id.as_str(),
			};
			if rest.split('.').next() == Some(candidate) {
				return true;
			}
		}
		false
	}
}

fn output_stem(name: &str) -> Option<&str> {
	name.strip_suffix(".md")
		.or_else(|| name.strip_suffix(".jsonl"))
}

/// An output id is a single filesystem-safe name. Requested task names are
/// already `[A-Za-z0-9_-]+`; the extra dot allowance is only for the nested
/// `Parent.Child` form. Rejecting empty segments and `..` keeps a malicious
/// persisted contract from addressing a path outside the artifacts directory.
pub(crate) fn valid_output_id(id: &str) -> bool {
	!id.is_empty()
		&& !id.starts_with('.')
		&& !id.ends_with('.')
		&& !id.contains("..")
		&& id
			.chars()
			.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// Publish `<root>/<output_id>.md` for the first time.
///
/// `Ok(None)` means the name is already owned by an existing file; the caller
/// must allocate another id and leave the existing output untouched.
/// The body is staged completely, verified, and only then atomically linked
/// into place, so `agent://` never resolves a partial artifact.
pub(crate) fn publish_agent_output(
	root: &Path,
	output_id: &str,
	body: &str,
) -> Result<Option<PathBuf>, String> {
	if !valid_output_id(output_id) {
		return Err(format!("invalid agent output id: {output_id:?}"));
	}
	fs::create_dir_all(root)
		.map_err(|error| format!("artifacts directory {}: {error}", root.display()))?;
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		// Best effort: the session directory is already private.
		let _ = fs::set_permissions(root, fs::Permissions::from_mode(0o700));
	}

	let staged = stage_body(root, body)?;
	let final_path = root.join(format!("{output_id}.md"));
	let published = publish_staged_file(&staged, &final_path);
	let _ = fs::remove_file(&staged);
	match published {
		Ok(true) => Ok(Some(final_path)),
		Ok(false) => Ok(None),
		Err(error) => Err(format!("publish {}: {error}", final_path.display())),
	}
}

/// Stage and verify one complete payload under an exclusive private name.
fn stage_body(root: &Path, body: &str) -> Result<PathBuf, String> {
	let (staged, mut file) = create_stage(root)?;
	let bytes = body.as_bytes();
	let written = (|| -> std::io::Result<()> {
		file.write_all(bytes)?;
		file.flush()?;
		file.sync_all()
	})();
	drop(file);
	if let Err(error) = written {
		let _ = fs::remove_file(&staged);
		return Err(format!("stage {}: {error}", staged.display()));
	}
	match fs::metadata(&staged) {
		Ok(metadata) if metadata.len() == bytes.len() as u64 => Ok(staged),
		Ok(metadata) => {
			let _ = fs::remove_file(&staged);
			Err(format!(
				"stage {}: wrote {} of {} bytes",
				staged.display(),
				metadata.len(),
				bytes.len()
			))
		},
		Err(error) => {
			let _ = fs::remove_file(&staged);
			Err(format!("verify {}: {error}", staged.display()))
		},
	}
}

/// Replace an existing saved output for a **follow-up turn that produced a
/// real yield**. TS follow-up semantics (`executor.ts:2266`) intentionally
/// overwrite the same `<id>.md` through `replaceFileAtomically`; the initial
/// publication path above remains no-clobber.
///
/// The root must be the owning `UrlScope` producer root. The final name is
/// validated; a symlink at the target is refused so an atomic rename cannot
/// swap a link or redirect the write through it.
pub(crate) fn publish_agent_output_replace(
	root: &Path,
	output_id: &str,
	body: &str,
) -> Result<PathBuf, String> {
	if !valid_output_id(output_id) {
		return Err(format!("invalid agent output id: {output_id:?}"));
	}
	fs::create_dir_all(root)
		.map_err(|error| format!("artifacts directory {}: {error}", root.display()))?;
	let root = fs::canonicalize(root)
		.map_err(|error| format!("artifacts directory {}: {error}", root.display()))?;
	let final_path = root.join(format!("{output_id}.md"));
	match fs::symlink_metadata(&final_path) {
		Ok(metadata) if metadata.file_type().is_symlink() => {
			return Err(format!("refusing to replace symlink {}", final_path.display()));
		},
		Ok(metadata) if metadata.is_dir() => {
			return Err(format!("output target is a directory: {}", final_path.display()));
		},
		Ok(_) => {},
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
		Err(error) => {
			return Err(format!("inspect {}: {error}", final_path.display()));
		},
	}
	let staged = stage_body(&root, body)?;
	// `rename` replaces the directory entry atomically; the symlink check above
	// guarantees it replaces a regular file (or creates the first one), never a
	// link target.
	match fs::rename(&staged, &final_path) {
		Ok(()) => Ok(final_path),
		Err(error) => {
			let _ = fs::remove_file(&staged);
			Err(format!("replace {}: {error}", final_path.display()))
		},
	}
}

/// Create one exclusive staging sibling. The public `<id>.md` name is never
/// opened directly, so a reader cannot observe an empty/partial artifact.
fn create_stage(directory: &Path) -> Result<(PathBuf, fs::File), String> {
	static SEQUENCE: AtomicU64 = AtomicU64::new(0);
	for _ in 0..16 {
		let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map(|duration| duration.as_nanos())
			.unwrap_or(0);
		let path = directory.join(format!(
			".agent-output.md.stage-{}-{nanos}-{sequence}",
			std::process::id()
		));
		match OpenOptions::new().write(true).create_new(true).open(&path) {
			Ok(file) => return Ok((path, file)),
			Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {},
			Err(error) => return Err(format!("stage {}: {error}", path.display())),
		}
	}
	Err("could not create an exclusive agent output staging file".to_string())
}

#[cfg(test)]
mod tests {
	use std::{
		collections::HashSet,
		path::PathBuf,
		sync::Arc,
		time::{SystemTime, UNIX_EPOCH},
	};

	use super::*;

	struct TempRoot(PathBuf);

	impl TempRoot {
		fn new(label: &str) -> Self {
			let nanos = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.map(|duration| duration.as_nanos())
				.unwrap_or_default();
			let home = std::env::var_os("HOME")
				.map(PathBuf::from)
				.expect("HOME must be set for agent output tests");
			let path = home.join(format!(
				".omp-agent-output-{label}-{}-{nanos}",
				std::process::id()
			));
			fs::create_dir_all(&path).expect("create root");
			Self(path)
		}
	}

	impl Drop for TempRoot {
		fn drop(&mut self) {
			let _ = fs::remove_dir_all(&self.0);
		}
	}

	#[test]
	fn allocator_uses_requested_name_then_suffixes_and_reserves_advisor() {
		let root = TempRoot::new("names");
		let allocator = OutputIdAllocator::default();
		assert_eq!(allocator.allocate(Some(&root.0), "Anna", None), "Anna");
		assert_eq!(allocator.allocate(Some(&root.0), "Anna", None), "Anna-2");
		assert_eq!(allocator.allocate(Some(&root.0), "Anna", None), "Anna-3");
		assert_eq!(
			allocator.allocate(Some(&root.0), ADVISOR_OUTPUT_STEM, None),
			"__advisor-2"
		);
	}

	#[test]
	fn allocator_seeds_from_md_jsonl_and_nested_prefix() {
		let root = TempRoot::new("seed");
		fs::write(root.0.join("Saved.md"), "old").expect("md seed");
		fs::write(root.0.join("task-1.jsonl"), "{}").expect("jsonl seed");
		fs::write(root.0.join("Parent.Child.md"), "nested").expect("nested seed");
		let allocator = OutputIdAllocator::default();

		assert_eq!(allocator.allocate(Some(&root.0), "Saved", None), "Saved-2");
		assert_eq!(allocator.allocate(Some(&root.0), "Parent", None), "Parent-2");
		assert_eq!(
			allocator.allocate(Some(&root.0), "Child", Some("Parent")),
			"Parent.Child-2"
		);
		assert_eq!(
			allocator.allocate(Some(&root.0), "Other", Some("Parent")),
			"Parent.Other"
		);
	}

	#[test]
	fn allocator_restart_does_not_reuse_names() {
		let root = TempRoot::new("restart");
		fs::write(root.0.join("Saved.md"), "old").expect("seed");
		let fresh_process = OutputIdAllocator::default();
		assert_eq!(
			fresh_process.allocate(Some(&root.0), "Saved", None),
			"Saved-2",
			"a restarted allocator seeds from disk instead of overwriting"
		);
	}

	#[test]
	fn root_and_nested_allocations_share_one_exact_name_scope() {
		let root = TempRoot::new("concurrent");
		let allocator = Arc::new(OutputIdAllocator::default());
		let mut handles = Vec::new();
		for _ in 0..8 {
			let allocator = Arc::clone(&allocator);
			let root = root.0.clone();
			handles.push(std::thread::spawn(move || {
				allocator.allocate(Some(&root), "Same", None)
			}));
		}
		let mut ids = HashSet::new();
		for handle in handles {
			ids.insert(handle.join().expect("thread"));
		}
		assert_eq!(ids.len(), 8, "every concurrent allocation is distinct");
		assert!(ids.contains("Same"));
		assert!(ids.contains("Same-8"));
	}

	#[test]
	fn publish_refuses_to_replace_existing_output() {
		let root = TempRoot::new("publish");
		let first = publish_agent_output(&root.0, "Saved", "first").expect("publish");
		assert_eq!(first, Some(root.0.join("Saved.md")));
		assert_eq!(
			publish_agent_output(&root.0, "Saved", "second").expect("publish"),
			None
		);
		assert_eq!(
			fs::read_to_string(root.0.join("Saved.md")).expect("read"),
			"first"
		);
	}

	#[test]
	fn publish_writes_empty_aborted_output() {
		let root = TempRoot::new("empty");
		let path = publish_agent_output(&root.0, "Aborted", "")
			.expect("publish")
			.expect("path");
		assert_eq!(fs::read_to_string(path).expect("read"), "");
	}

	#[test]
	fn followup_replace_updates_existing_regular_output() {
		let root = TempRoot::new("followup-replace");
		publish_agent_output(&root.0, "Saved", "initial").expect("initial");
		let path = publish_agent_output_replace(&root.0, "Saved", "follow-up")
			.expect("replace");
		assert_eq!(fs::read_to_string(path).expect("read"), "follow-up");
	}

	#[cfg(unix)]
	#[test]
	fn followup_replace_refuses_a_symlink_target() {
		let root = TempRoot::new("followup-symlink");
		let outside = root.0.join("outside");
		fs::write(&outside, "keep").expect("outside");
		std::os::unix::fs::symlink(&outside, root.0.join("Saved.md")).expect("symlink");
		let error = publish_agent_output_replace(&root.0, "Saved", "follow-up")
			.expect_err("symlink target");
		assert!(error.contains("refusing to replace symlink"), "{error}");
		assert_eq!(fs::read_to_string(&outside).expect("outside intact"), "keep");
	}

	#[test]
	fn invalid_output_ids_never_address_a_path() {
		for id in ["", ".", "..", "a/b", "a..b", "../escape", "a\\b"] {
			assert!(!valid_output_id(id), "{id:?} must be rejected");
		}
		assert!(valid_output_id("Parent.Child-2"));
	}
}
