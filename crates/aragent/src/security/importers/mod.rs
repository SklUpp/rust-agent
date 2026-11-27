//! Importers for external security result documents.
//!
//! Anchors: `packages/coding-agent/src/security/importers/*`.
//!
//! Imported bundles enter the same store, `security://` namespace, and
//! remediation path as native scans; the importer only translates producer
//! output into the canonical contract and assigns Aragent-owned ids.

pub mod codex_security;
pub mod sarif;

use std::{fs, io::Read, path::Path};

use crate::security::types::MAX_SECURITY_DOCUMENT_BYTES;

/// Reject any symlink component that is dangling or resolves outside `root`.
///
/// `canonicalize_nearest_existing` deliberately stops at the first missing
/// component, which would otherwise let `repo/link -> /outside/missing` pass as
/// an ordinary missing path. Callers must invoke this before nearest-existing
/// canonicalization.
pub(crate) fn ensure_path_components_inside(path: &Path, root: &Path) -> Result<(), String> {
	let relative = path
		.strip_prefix(root)
		.map_err(|_| format!("{} is outside {}", path.display(), root.display()))?;
	let mut prefix = root.to_path_buf();
	for component in relative.components() {
		prefix.push(component.as_os_str());
		match fs::symlink_metadata(&prefix) {
			Ok(metadata) if metadata.file_type().is_symlink() => {
				let canonical = fs::canonicalize(&prefix)
					.map_err(|error| format!("dangling symbolic link {}: {error}", prefix.display()))?;
				if !(canonical.as_path() == root || canonical.starts_with(root)) {
					return Err(format!(
						"symbolic link {} resolves outside the repository {}",
						prefix.display(),
						root.display()
					));
				}
			},
			Ok(_) => {},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
			Err(error) => {
				return Err(format!("cannot inspect {}: {error}", prefix.display()));
			},
		}
	}
	Ok(())
}

fn document_too_large(path: &Path, label: &str) -> crate::Error {
	crate::Error::new(format!(
		"{label} {} exceeds the {MAX_SECURITY_DOCUMENT_BYTES}-byte security document limit",
		path.display()
	))
}

/// Read a security document without ever buffering more than the shared limit.
pub(crate) fn read_security_document(path: &Path, label: &str) -> crate::Result<Vec<u8>> {
	let metadata = fs::metadata(path).map_err(|error| {
		crate::Error::new(format!("cannot read {label} {}: {error}", path.display()))
	})?;
	if metadata.len() > MAX_SECURITY_DOCUMENT_BYTES as u64 {
		return Err(document_too_large(path, label));
	}
	let file = fs::File::open(path).map_err(|error| {
		crate::Error::new(format!("cannot read {label} {}: {error}", path.display()))
	})?;
	let mut bytes = Vec::new();
	file
		.take(MAX_SECURITY_DOCUMENT_BYTES as u64 + 1)
		.read_to_end(&mut bytes)
		.map_err(|error| {
			crate::Error::new(format!("cannot read {label} {}: {error}", path.display()))
		})?;
	if bytes.len() > MAX_SECURITY_DOCUMENT_BYTES {
		return Err(document_too_large(path, label));
	}
	Ok(bytes)
}

/// Optional artifact reader: only `NotFound` means absent; every other IO or
/// size error is surfaced to the importer.
pub(crate) fn read_optional_security_document(
	path: &Path,
	label: &str,
) -> crate::Result<Option<Vec<u8>>> {
	match fs::metadata(path) {
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
		Err(error) => {
			Err(crate::Error::new(format!("cannot read {label} {}: {error}", path.display())))
		},
		Ok(metadata) if metadata.len() > MAX_SECURITY_DOCUMENT_BYTES as u64 => {
			Err(document_too_large(path, label))
		},
		Ok(_) => read_security_document(path, label).map(Some),
	}
}
