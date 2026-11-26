//! Process-tree and session-recovery guarantees for a cancelled command.
//!
//! Real processes: a timed-out run must escalate `TERM -> KILL` until the
//! whole tree (including a TERM-ignoring grandchild) is gone, and the discarded
//! session must not leak `cd`/exports into the next call.
#![cfg(unix)]

use std::{
	path::Path,
	time::{Duration, Instant},
};

use pi_shell::{
	Shell, ShellRunOptions,
	cancel::CancelToken,
	process::{Process, ProcessStatus},
};

fn temp_dir(label: &str) -> std::path::PathBuf {
	let dir = std::env::temp_dir().join(format!("omp-tree-{label}-{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&dir);
	std::fs::create_dir_all(&dir).expect("temp dir");
	dir
}

/// Single-quote a path for the shell; temp paths contain no quotes, but escape
/// anyway so the helper cannot be mangled by an unusual `TMPDIR`.
fn quote(path: &Path) -> String {
	format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

fn read_pid(path: &Path) -> i32 {
	let raw = std::fs::read_to_string(path).expect("pid file");
	raw.trim().parse().expect("numeric pid")
}

async fn wait_until_dead(pid: i32) -> bool {
	let deadline = Instant::now() + Duration::from_secs(5);
	while Instant::now() < deadline {
		let alive = Process::from_pid(pid)
			.is_some_and(|process| process.status() == ProcessStatus::Running);
		if !alive {
			return true;
		}
		tokio::time::sleep(Duration::from_millis(25)).await;
	}
	false
}

/// Last-resort cleanup when an assertion fails before the tree is dead.
struct PidCleanup(i32);

impl Drop for PidCleanup {
	fn drop(&mut self) {
		if let Some(process) = Process::from_pid(self.0) {
			let _ = process.kill_tree(None);
		}
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_command_kills_term_ignoring_grandchild() {
	let root = temp_dir("kill");
	let script = root.join("tree.sh");
	let pidfile = root.join("grandchild.pid");
	// Both the script and its background grandchild ignore SIGTERM, so only
	// the KILL escalation can reap them.
	std::fs::write(
		&script,
		"trap '' TERM\nsh -c 'trap \"\" TERM; exec sleep 60' &\necho $! > \"$1\"\nwait\n",
	)
	.expect("write script");

	let shell = Shell::new(None);
	let result = shell
		.run(
			ShellRunOptions {
				command:    format!("sh {} {}", quote(&script), quote(&pidfile)),
				cwd:        Some(root.to_string_lossy().into_owned()),
				timeout_ms: Some(1200),
				..Default::default()
			},
			None,
			CancelToken::default(),
		)
		.await
		.expect("run timing-out tree");

	assert!(result.timed_out, "expected a timeout, got {result:?}");
	let pid = read_pid(&pidfile);
	let _cleanup = PidCleanup(pid);
	assert!(
		wait_until_dead(pid).await,
		"grandchild {pid} survived the TERM -> KILL escalation"
	);

	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_command_discards_session_state_for_next_call() {
	let root = temp_dir("cwd");
	let nested = root.join("nested");
	std::fs::create_dir_all(&nested).expect("nested dir");
	let root_string = root.to_string_lossy().into_owned();
	let nested_string = nested.to_string_lossy().into_owned();

	let shell = Shell::new(None);
	let setup = shell
		.run(
			ShellRunOptions {
				command: format!("cd {} && export PI_SURVIVES=1", quote(&nested)),
				cwd:     Some(root_string.clone()),
				..Default::default()
			},
			None,
			CancelToken::default(),
		)
		.await
		.expect("setup run");
	assert_eq!(setup.exit_code, Some(0), "setup failed: {setup:?}");

	let before = root.join("before.txt");
	shell
		.run(
			ShellRunOptions {
				command: format!("pwd > {}", quote(&before)),
				..Default::default()
			},
			None,
			CancelToken::default(),
		)
		.await
		.expect("pwd before");
	assert_eq!(
		std::fs::canonicalize(std::fs::read_to_string(&before).expect("before").trim())
			.expect("canonical before"),
		std::fs::canonicalize(&nested).expect("canonical nested"),
		"cd must persist into the next call"
	);

	let timed_out = shell
		.run(
			ShellRunOptions {
				command:    "sh -c 'sleep 60'".to_string(),
				cwd:        Some(nested_string.clone()),
				timeout_ms: Some(300),
				..Default::default()
			},
			None,
			CancelToken::default(),
		)
		.await
		.expect("timed-out run");
	assert!(timed_out.timed_out, "expected a timeout, got {timed_out:?}");
	assert!(
		timed_out.session_reset,
		"a timed-out command must discard the session generation"
	);

	let after = root.join("after.txt");
	let env_after = root.join("env-after.txt");
	let recovery = shell
		.run(
			ShellRunOptions {
				command: format!(
					"pwd > {} && printf '%s' \"${{PI_SURVIVES-unset}}\" > {}",
					quote(&after),
					quote(&env_after)
				),
				cwd:     Some(root_string.clone()),
				..Default::default()
			},
			None,
			CancelToken::default(),
		)
		.await
		.expect("recovery run");
	assert_eq!(recovery.exit_code, Some(0), "recovery failed: {recovery:?}");
	assert_eq!(
		std::fs::canonicalize(std::fs::read_to_string(&after).expect("after").trim())
			.expect("canonical after"),
		std::fs::canonicalize(&root).expect("canonical root"),
		"the next call must start from the host-provided cwd"
	);
	assert_eq!(
		std::fs::read_to_string(&env_after).expect("env after"),
		"unset",
		"an export from the discarded session must not survive"
	);

	let _ = std::fs::remove_dir_all(&root);
}
