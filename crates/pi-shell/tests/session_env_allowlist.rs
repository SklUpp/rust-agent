//! Regression: shell sessions never inherit arbitrary ambient credentials.
//! Only platform essentials (PATH, HOME, temp dirs, locale) are copied on
//! every pathway — persistent sessions, the one-shot fallback, and streams —
//! while an explicit `session_env` or per-command `env` is still applied and
//! reaches a real child process.
//!
//! This lives in its own test binary because it mutates the process-global
//! environment. It is a single `#[test]` on purpose: the variable is set
//! before the Tokio runtime (and any shell thread) exists, so nothing can read
//! the environment concurrently. A second test in this binary would race a
//! shell's `std::env::vars_os()` read against this mutation.
#![cfg(unix)]

use std::collections::HashMap;

use pi_shell::{ShellExecuteOptions, cancel::CancelToken, execute_shell};

async fn capture(options: ShellExecuteOptions) -> String {
	let (tx, rx) = flume::unbounded::<String>();
	let result = execute_shell(options, Some(tx), CancelToken::default())
		.await
		.expect("shell run");
	assert_eq!(result.exit_code, Some(0), "shell command failed");
	rx.try_iter().collect()
}

#[test]
fn shells_do_not_inherit_ambient_credentials_and_pass_explicit_env_to_children() {
	// SAFETY: this binary contains exactly one test, and the variable is set
	// before the Tokio runtime (and any shell thread) is created, so no thread
	// is reading or writing the process environment concurrently. It is never
	// restored: the test process ends after this test.
	unsafe { std::env::set_var("PI_TEST_AMBIENT_SECRET", "leaked") };
	let host_path = std::env::var("PATH").expect("PATH in the test environment");
	assert!(!host_path.is_empty(), "the test host must expose a usable PATH");

	let runtime = tokio::runtime::Builder::new_multi_thread()
		.worker_threads(2)
		.enable_all()
		.build()
		.expect("test runtime");
	runtime.block_on(async {
		// An explicit `session_env` overlays the filtered host environment.
		let mut session_env = HashMap::new();
		session_env.insert("PI_TEST_SESSION_ONLY".to_string(), "present".to_string());
		let output = capture(ShellExecuteOptions {
			command: "printf '%s|%s' \"${PI_TEST_AMBIENT_SECRET:-absent}\" \
			          \"$PI_TEST_SESSION_ONLY\""
				.to_string(),
			session_env: Some(session_env),
			..Default::default()
		})
		.await;
		assert_eq!(output.trim(), "absent|present", "session env leaked ambient state: {output}");

		// The one-shot fallback (`session_env: None`, used by stateless
		// embedders and `BashTool::legacy_one_shot`) applies the same allowlist.
		// It also proves the contract in a real external child: `sh` is resolved
		// through the inherited PATH, and the secret is absent from *its*
		// environment, not merely from the shell's own expansion.
		let mut explicit = HashMap::new();
		explicit.insert("PI_TEST_EXPLICIT".to_string(), "authorized".to_string());
		let output = capture(ShellExecuteOptions {
			command: "printf '%s|%s|%s|' \
			          \"${PI_TEST_AMBIENT_SECRET:-absent}\" \
			          \"$PI_TEST_EXPLICIT\" \
			          \"$PATH\"; \
			          sh -c 'printf \"%s|%s|%s\" \
			                 \"${PI_TEST_AMBIENT_SECRET:-absent}\" \
			                 \"$PI_TEST_EXPLICIT\" \
			                 \"$PATH\"'"
				.to_string(),
			env: Some(explicit),
			..Default::default()
		})
		.await;

		let fields: Vec<&str> = output.trim().split('|').collect();
		assert_eq!(fields.len(), 6, "unexpected output shape: {output:?}");
		assert_eq!(fields[0], "absent", "one-shot shell inherited an ambient secret");
		assert_eq!(fields[1], "authorized", "explicit per-command env was dropped");
		assert_eq!(fields[2], host_path, "PATH must be inherited unchanged");
		assert_eq!(fields[3], "absent", "child process inherited an ambient secret");
		assert_eq!(fields[4], "authorized", "child process lost the explicit env");
		assert_eq!(fields[5], host_path, "child process PATH must be inherited unchanged");
	});
}
