//! Read-only snapshot of a persistent [`Shell`] session.
//!
//! The host needs the effective working directory and exported environment
//! when it starts an asynchronous process that should continue the same
//! logical shell state. Reading the host process environment (`std::env`) is
//! not equivalent: it misses `cd` and `export` performed inside the session.

use std::collections::HashMap;

use brush_core::variables::ShellValue;

use crate::shell::Shell;

/// Effective state of a materialized persistent shell session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShellStateSnapshot {
	/// The session's current working directory.
	pub cwd: String,
	/// Variables exported to child processes. Non-string shell values
	/// (arrays, dynamic values) have no single child-process string form and
	/// are omitted.
	pub env: HashMap<String, String>,
}

impl Shell {
	/// Snapshot the session's working directory and exported environment.
	///
	/// Returns `None` when the session has not been materialized yet or while
	/// a command owns it; the caller must not treat either case as an empty
	/// shell state.
	pub async fn snapshot_state(&self) -> Option<ShellStateSnapshot> {
		let guard = self.session.lock().await;
		let core = guard.as_ref()?;
		if core.in_flight {
			return None;
		}
		let cwd = core.shell.working_dir().to_string_lossy().into_owned();
		let env: HashMap<String, String> = core
			.shell
			.env()
			.iter_exported()
			.filter_map(|(name, variable)| match variable.value() {
				ShellValue::String(value) => Some((name.clone(), value.clone())),
				_ => None,
			})
			.collect();
		Some(ShellStateSnapshot { cwd, env })
	}
}
