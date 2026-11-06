//! Writer lease and strict full-version guard behavior.
//!
//! These are real filesystem checks: lease exclusion (two descriptors in one
//! process have the same `flock` semantics as two processes), symlink identity,
//! and a guarded overwrite that must refuse different raw bytes carrying the
//! same normalized 4-hex tag.

use std::path::{Path, PathBuf};

use pi_agent::{
	tools::hashline::hash::compute_file_hash,
	workspace_writer::{FileVersion, LockError, WorkspaceWriter, replace_if_unchanged},
};

fn temp_dir(label: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!("omp-writer-{label}-{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&dir);
	std::fs::create_dir_all(&dir).expect("temp dir");
	dir
}

fn canonical(path: &Path) -> PathBuf {
	std::fs::canonicalize(path).expect("canonical path")
}

#[cfg(unix)]
#[test]
fn workspace_lease_is_shared_across_files_and_stable_by_root() {
	let dir = temp_dir("workspace");
	let file = dir.join("note.txt");
	std::fs::write(&file, "alpha\n").expect("seed");
	let writer = WorkspaceWriter::with_lock_dir(dir.join("locks"));
	let key = pi_agent::workspace_writer::workspace_key(&dir);
	assert_eq!(key, pi_agent::workspace_writer::workspace_key(&canonical(&dir)));

	let lease = writer.acquire_workspace(&dir).expect("workspace lease");
	match writer.acquire_workspace(&dir) {
		Err(LockError::Occupied { canonical: blocked }) => assert_eq!(blocked, canonical(&dir)),
		Err(other) => panic!("expected Occupied, got {other:?}"),
		Ok(_) => panic!("a second workspace lease must conflict"),
	}
	// A file lease is a different scope: the host still holds the workspace
	// owner lease, so this must not block the file-level primitive itself.
	writer.acquire(&file).expect("file lease is independent");

	drop(lease);
	let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn guarded_replace_preserves_and_sets_exact_mode() {
	use std::os::unix::fs::PermissionsExt;

	let dir = temp_dir("mode");
	let file = dir.join("note.txt");
	std::fs::write(&file, "alpha\n").expect("seed");
	std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o640)).expect("mode");
	let version = FileVersion::read(&file).expect("read version");

	replace_if_unchanged(&file, &version, b"ALPHA\n", None).expect("preserve mode");
	let preserved = std::fs::metadata(&file).expect("metadata").permissions().mode() & 0o777;
	assert_eq!(preserved, 0o640, "existing mode survives the umask-restoring publish");

	let version = FileVersion::read(&file).expect("read version");
	replace_if_unchanged(&file, &version, b"alpha\n", Some(0o600)).expect("set mode");
	let explicit = std::fs::metadata(&file).expect("metadata").permissions().mode() & 0o777;
	assert_eq!(explicit, 0o600, "explicit mode is applied before publish");

	let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn unwritable_lock_root_fails_closed() {
	let dir = temp_dir("unwritable");
	let not_a_dir = dir.join("not-a-dir");
	std::fs::write(&not_a_dir, "x").expect("seed");
	let writer = WorkspaceWriter::with_lock_dir(not_a_dir.join("locks"));

	match writer.acquire(&not_a_dir) {
		Err(LockError::Io(_)) => {},
		Err(other) => panic!("expected an I/O refusal, got {other:?}"),
		Ok(_) => panic!("an unusable coordination root must not fail open"),
	}

	let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn lease_is_exclusive_and_released_on_drop() {
	let dir = temp_dir("lease");
	let file = dir.join("note.txt");
	std::fs::write(&file, "alpha\n").expect("seed");
	let writer = WorkspaceWriter::with_lock_dir(dir.join("locks"));

	let lease = writer.acquire(&file).expect("first lease");
	assert_eq!(lease.canonical(), canonical(&file).as_path());

	match writer.acquire(&file) {
		Err(LockError::Occupied { canonical: blocked }) => {
			assert_eq!(blocked, canonical(&file));
		},
		Err(other) => panic!("expected Occupied, got {other:?}"),
		Ok(_) => panic!("a second descriptor must not share the lease"),
	}

	drop(lease);
	writer.acquire(&file).expect("lease released on drop");

	let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn symlink_alias_shares_the_lease() {
	let dir = temp_dir("symlink");
	let file = dir.join("note.txt");
	let link = dir.join("alias.txt");
	std::fs::write(&file, "alpha\n").expect("seed");
	std::os::unix::fs::symlink(&file, &link).expect("symlink");
	let writer = WorkspaceWriter::with_lock_dir(dir.join("locks"));

	let lease = writer.acquire(&link).expect("lease through alias");
	assert_eq!(lease.canonical(), canonical(&file).as_path());
	match writer.acquire(&file) {
		Err(LockError::Occupied { canonical: blocked }) => assert_eq!(blocked, canonical(&file)),
		Err(other) => panic!("expected Occupied, got {other:?}"),
		Ok(_) => panic!("the symlink alias must share the target lease"),
	}

	let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn stale_raw_change_is_not_overwritten_even_with_the_same_tag() {
	let dir = temp_dir("stale");
	let file = dir.join("note.txt");
	std::fs::write(&file, "alpha\n").expect("seed");
	let version = FileVersion::read(&file).expect("read version");
	let tag = compute_file_hash("alpha\n");

	let mut collision = None;
	for index in 0..1_000_000u32 {
		let candidate = format!("alpha\n#{}", index);
		if compute_file_hash(&candidate) == tag {
			collision = Some(candidate);
			break;
		}
	}
	let collision = collision.expect("find a 16-bit tag collision");
	assert_eq!(compute_file_hash(&collision), tag);
	std::fs::write(&file, &collision).expect("external change");

	let result = replace_if_unchanged(&file, &version, b"replacement\n", None);
	match result {
		Err(error) => assert!(error.is_stale(), "expected a stale refusal, got {error}"),
		Ok(()) => panic!("different raw bytes with the same tag must not be overwritten"),
	}
	assert_eq!(std::fs::read_to_string(&file).expect("content"), collision);

	let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn unchanged_raw_version_is_replaced() {
	let dir = temp_dir("replace");
	let file = dir.join("note.txt");
	std::fs::write(&file, "alpha\n").expect("seed");
	let version = FileVersion::read(&file).expect("read version");

	replace_if_unchanged(&file, &version, b"ALPHA\n", None).expect("guarded replace");
	assert_eq!(std::fs::read_to_string(&file).expect("content"), "ALPHA\n");
	assert_eq!(
		std::fs::read_dir(&dir)
			.expect("dir")
			.filter_map(Result::ok)
			.filter(|entry| entry.file_name().to_string_lossy().contains(".writer-"))
			.count(),
		0,
		"no temporary writer file may remain"
	);

	let _ = std::fs::remove_dir_all(&dir);
}
