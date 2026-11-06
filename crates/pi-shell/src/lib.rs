//! Shell execution for pi.
//!
//! [`shell::Shell`] owns a persistent, per-instance session: `cd`, exported
//! variables, and functions survive between calls, and concurrent calls are
//! serialized instead of falling back to a fresh shell. A call cancelled
//! before it dispatches leaves the session untouched; a call cancelled
//! mid-command discards it, reported by `ShellRunResult::session_reset`.
//! One-shot helpers ([`execute_shell`], [`execute_shell_streams`]) stay
//! stateless.
pub mod cancel;
pub mod minimizer;
pub mod process;
pub mod shell;
#[cfg(windows)]
pub mod windows;

pub use brush_core::commands::{ChildSessionAction, child_session_action};
// Re-exported for `pi-natives`: the builtins live in `pi-builtins`,
// but the native layer only ever depends on the shell.
pub use pi_builtins::{
	panic_scope_active, rayon_global_pool_available, set_rayon_global_pool_available,
};
pub use shell::{
	MinimizerResult, Shell, ShellExecuteOptions, ShellExecuteResult, ShellOptions, ShellRunOptions,
	ShellRunResult, StreamSinks, execute_shell, execute_shell_streams,
};
