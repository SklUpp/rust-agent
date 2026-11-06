//! Regression coverage for the per-instance persistent session:
//! `cd`/`export`/functions survive between calls, instances stay independent,
//! and cancelling a call that is still waiting for the session neither runs
//! its command nor discards the owner's state.
#![cfg(unix)]

use std::{path::Path, sync::Arc, time::Duration};

use pi_shell::{
	Shell, ShellRunOptions, ShellRunResult,
	cancel::{AbortReason, CancelToken},
};

/// Wait for a condition file instead of guessing a fixed sleep duration.
async fn wait_for(path: &Path) {
	for _ in 0..400 {
		if path.exists() {
			return;
		}
		tokio::time::sleep(Duration::from_millis(25)).await;
	}
	panic!("condition file never appeared: {}", path.display());
}

async fn run(shell: &Shell, command: &str, token: CancelToken) -> (ShellRunResult, String) {
	let (tx, rx) = flume::unbounded::<String>();
	let result = shell
		.run(
			ShellRunOptions { command: command.to_string(), ..Default::default() },
			Some(tx),
			token,
		)
		.await
		.expect("shell run");
	(result, rx.try_iter().collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_session_keeps_cd_env_and_functions() {
	let shell = Shell::new(None);
	let dir = tempfile::tempdir().expect("temp dir");
	let dir_path = dir.path().to_string_lossy().into_owned();

	let (first, _) = run(
		&shell,
		&format!(
			"cd '{dir_path}' && export PI_PERSIST=kept && pi_greet() {{ echo fn-ok; }}"
		),
		CancelToken::default(),
	)
	.await;
	assert_eq!(first.exit_code, Some(0));

	let (second, output) = run(
		&shell,
		"pwd; printf '|%s|' \"$PI_PERSIST\"; pi_greet",
		CancelToken::default(),
	)
	.await;
	assert_eq!(second.exit_code, Some(0));
	assert!(output.contains(&dir_path), "cwd not retained: {output}");
	assert!(output.contains("|kept|"), "export not retained: {output}");
	assert!(output.contains("fn-ok"), "function not retained: {output}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_instances_are_independent() {
	let a = Shell::new(None);
	let b = Shell::new(None);
	let dir = tempfile::tempdir().expect("temp dir");
	let dir_path = dir.path().to_string_lossy().into_owned();

	let (first, _) = run(
		&a,
		&format!("cd '{dir_path}'; export PI_ONLY_A=1"),
		CancelToken::default(),
	)
	.await;
	assert_eq!(first.exit_code, Some(0));

	let (second, output) = run(
		&b,
		"printf '%s' \"${PI_ONLY_A:-unset}\"; pwd",
		CancelToken::default(),
	)
	.await;
	assert_eq!(second.exit_code, Some(0));
	assert!(output.contains("unset"), "instance B inherited A's env: {output}");
	assert!(!output.contains(&dir_path), "instance B inherited A's cwd: {output}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_waiting_call_runs_nothing_and_keeps_session() {
	let shell = Arc::new(Shell::new(None));
	let dir = tempfile::tempdir().expect("temp dir");
	let started = dir.path().join("started");
	let release = dir.path().join("release");
	let marker = dir.path().join("waited-command-ran");
	let started_arg = started.to_string_lossy().into_owned();
	let release_arg = release.to_string_lossy().into_owned();
	let marker_arg = marker.to_string_lossy().into_owned();

	let first_shell = Arc::clone(&shell);
	let first = tokio::spawn(async move {
		run(
			&first_shell,
			&format!(
				"export PI_BEFORE_WAIT=kept; : > '{started_arg}'; \
				 while [ ! -e '{release_arg}' ]; do sleep 0.01; done"
			),
			CancelToken::new(Some(30_000)),
		)
		.await
	});

	// The first call owns the mutex from here until `release` exists.
	wait_for(&started).await;

	let mut cancel = CancelToken::default();
	let abort = cancel.emplace_abort_token();
	let waiter_shell = Arc::clone(&shell);
	let waiter = tokio::spawn(async move {
		run(&waiter_shell, &format!(": > '{marker_arg}'"), cancel).await
	});
	// The first call still holds the session, so this abort always lands while
	// the waiter is queued - deterministic, independent of scheduler timing.
	abort.abort(AbortReason::Signal);

	let (waited, _) = waiter.await.expect("waiter joins");
	assert!(waited.cancelled, "queued cancellation must report cancelled");
	assert!(
		!waited.session_reset,
		"cancelling a waiting call must not discard the owner's session"
	);
	assert!(!marker.exists(), "cancelled waiting call executed its command");

	std::fs::write(&release, b"go").expect("release first call");
	let (first_result, _) = first.await.expect("first call joins");
	assert_eq!(first_result.exit_code, Some(0));
	assert!(!marker.exists(), "waiting call ran after the session was released");

	let (_, output) = run(&shell, "printf '%s' \"$PI_BEFORE_WAIT\"", CancelToken::default()).await;
	assert_eq!(output.trim(), "kept", "waiting cancellation lost session state: {output}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_run_generation_is_discarded_before_reuse() {
	let shell = Arc::new(Shell::new(None));
	let dir = tempfile::tempdir().expect("temp dir");
	let started = dir.path().join("started");
	let started_arg = started.to_string_lossy().into_owned();

	let blocked_shell = Arc::clone(&shell);
	let blocked = tokio::spawn(async move {
		run(
			&blocked_shell,
			&format!("export PI_STALE=1; : > '{started_arg}'; sleep 5"),
			CancelToken::default(),
		)
		.await
	});
	wait_for(&started).await;

	// Drop the caller future mid-command; the run task is aborted with its
	// in-flight mark left on the session slot.
	blocked.abort();
	let _ = blocked.await;

	let (fresh, output) = run(
		&shell,
		"printf '%s' \"${PI_STALE:-clean}\"",
		CancelToken::default(),
	)
	.await;
	assert_eq!(fresh.exit_code, Some(0));
	assert_eq!(output.trim(), "clean", "abandoned generation was reused: {output}");
}
