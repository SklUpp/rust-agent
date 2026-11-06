//! Regression: an explicit `session_env` must not inherit arbitrary ambient
//! credentials. Only platform essentials (PATH, HOME, temp dirs, locale) are
//! copied, and the caller's entries are overlaid on top.
//!
//! This lives in its own test binary because it mutates the process-global
//! environment.
#![cfg(unix)]

use std::collections::HashMap;

use pi_shell::{ShellExecuteOptions, cancel::CancelToken, execute_shell};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_env_does_not_inherit_ambient_credentials() {
	// SAFETY: this dedicated test process mutates the environment only here,
	// before any shell is created, and never restores it.
	unsafe { std::env::set_var("PI_TEST_AMBIENT_SECRET", "leaked") };

	let mut session_env = HashMap::new();
	session_env.insert("PI_TEST_SESSION_ONLY".to_string(), "present".to_string());

	let (tx, rx) = flume::unbounded::<String>();
	let result = execute_shell(
		ShellExecuteOptions {
			command: "printf '%s|%s' \"${PI_TEST_AMBIENT_SECRET:-absent}\" \
			          \"$PI_TEST_SESSION_ONLY\""
				.to_string(),
			session_env: Some(session_env),
			..Default::default()
		},
		Some(tx),
		CancelToken::default(),
	)
	.await
	.expect("shell run");

	assert_eq!(result.exit_code, Some(0));
	let output: String = rx.try_iter().collect();
	assert_eq!(output.trim(), "absent|present", "session env leaked ambient state: {output}");
}
