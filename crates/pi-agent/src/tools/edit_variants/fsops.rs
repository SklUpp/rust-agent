//! Atomic per-file operations for the edit engines.
//!
//! Every commit re-checks the bytes it read before publishing, writes through
//! the shared temp-file+rename helper (symlink-safe, mode-preserving), and
//! leaves the destination untouched when anything fails. Moves are a
//! non-overwriting write of the destination followed by the source unlink;
//! delete and move are only reachable from an explicit mode operation.

use std::path::Path;
use crate::{
	tool::ToolError,
	tools::{MAX_EDIT_BYTES, read_capped},
};
/// Read an existing UTF-8 file under the edit size cap.
pub async fn read_text(path: &Path, display: &str) -> Result<String, ToolError> {
	let meta = tokio::fs::metadata(path)
		.await
		.map_err(|err| map_read_error(err, display))?;
	if !meta.is_file() {
		return Err(ToolError::ExecutionFailed(format!("{display} is not a regular file")));
	}
	let (bytes, size) = read_capped(path, MAX_EDIT_BYTES)
		.await
		.map_err(|err| map_read_error(err, display))?;
	if size > bytes.len() as u64 {
		return Err(ToolError::ExecutionFailed(format!(
			"{display} is {size} bytes, the edit limit is {MAX_EDIT_BYTES}"
		)));
	}
	String::from_utf8(bytes)
		.map_err(|_| ToolError::ExecutionFailed(format!("{display} is not valid UTF-8")))
}

fn map_read_error(err: std::io::Error, display: &str) -> ToolError {
	if err.kind() == std::io::ErrorKind::NotFound {
		return ToolError::ExecutionFailed(format!("File not found: {display}"));
	}
	ToolError::ExecutionFailed(format!("Failed to read {display}: {err}"))
}
