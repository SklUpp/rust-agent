//! Consumer-visible contract for `BashTool`: per-call `env`/`cwd`/`timeout`,
//! foreground-PTY and legacy `timeout_ms` rejection, background delegation to
//! the session process supervisor, the output cap, and timeout/cancellation
//! error results.

use std::{
	path::PathBuf,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::Duration,
};

use async_trait::async_trait;
use pi_agent::{
	AcpClientBridge, AcpClientError, AgentContext, BashTool, CancellationToken, Tool, ToolError,
};
use serde_json::{Value, json};

struct TempDir {
	path: PathBuf,
}

impl TempDir {
	fn new(label: &str) -> Self {
		static NEXT: AtomicUsize = AtomicUsize::new(0);
		let unique =
			format!("pi-agent-{label}-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::SeqCst));
		let path = std::env::temp_dir().join(unique);
		std::fs::create_dir_all(&path).expect("create temp dir");
		Self { path }
	}

	fn join(&self, child: &str) -> PathBuf {
		self.path.join(child)
	}
}

impl Drop for TempDir {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.path);
	}
}

/// First line of a bash result, i.e. the captured output without the status
/// trailer the tool appends.
fn first_line(result: &pi_agent::ToolResult) -> &str {
	result.content.lines().next().unwrap_or("")
}

#[tokio::test]
async fn env_overlay_applies_and_values_stay_literal() {
	let dir = TempDir::new("bash-env");
	let mut ctx = AgentContext::new(dir.path.clone());
	ctx.env.insert("BASH_CONTRACT".to_string(), "base".to_string());

	let result = BashTool::legacy_one_shot()
		.execute(
			&json!({
				"command": "echo \"$BASH_CONTRACT\"; echo \"$BASH_LITERAL\"",
				"env": { "BASH_CONTRACT": "overlay", "BASH_LITERAL": "$(id)" }
			}),
			&ctx,
		)
		.await
		.expect("bash");

	assert!(!result.is_error, "got: {}", result.content);
	assert!(result.content.contains("overlay"), "overlay missing: {}", result.content);
	assert!(
		result.content.contains("$(id)"),
		"env value was interpreted as shell code: {}",
		result.content
	);
	assert_eq!(
		ctx.env.get("BASH_CONTRACT").map(String::as_str),
		Some("base"),
		"per-call env must not mutate the agent environment"
	);
}

#[tokio::test]
async fn invalid_env_names_and_non_string_values_are_rejected() {
	let dir = TempDir::new("bash-env-invalid");
	let ctx = AgentContext::new(dir.path.clone());

	let bad_name = BashTool::legacy_one_shot()
		.execute(&json!({"command": "true", "env": {"BAD-NAME": "x"}}), &ctx)
		.await
		.expect_err("invalid name must be rejected");
	assert!(matches!(&bad_name, ToolError::InvalidArguments(_)));
	assert!(bad_name.to_string().contains("BAD-NAME"));

	let leading_digit = BashTool::legacy_one_shot()
		.execute(&json!({"command": "true", "env": {"1LEAD": "x"}}), &ctx)
		.await
		.expect_err("leading digit must be rejected");
	assert!(leading_digit.to_string().contains("1LEAD"));

	let bad_value = BashTool::legacy_one_shot()
		.execute(&json!({"command": "true", "env": {"OK": 1}}), &ctx)
		.await
		.expect_err("non-string value must be rejected");
	assert!(matches!(&bad_value, ToolError::InvalidArguments(_)));

	let bad_shape = BashTool::legacy_one_shot()
		.execute(&json!({"command": "true", "env": "FOO=bar"}), &ctx)
		.await
		.expect_err("string env must be rejected");
	assert!(matches!(&bad_shape, ToolError::InvalidArguments(_)));
}

#[tokio::test]
async fn cwd_resolves_relative_absolute_and_tilde() {
	let dir = TempDir::new("bash-cwd");
	let sub = dir.join("sub");
	std::fs::create_dir_all(&sub).expect("mkdir sub");
	let home = dir.join("home");
	let nested = home.join("nested");
	std::fs::create_dir_all(&nested).expect("mkdir nested");

	let mut ctx = AgentContext::new(dir.path.clone());
	ctx.env.insert("HOME".to_string(), home.display().to_string());

	let relative = BashTool::legacy_one_shot()
		.execute(&json!({"command": "pwd", "cwd": "sub"}), &ctx)
		.await
		.expect("bash");
	assert!(!relative.is_error, "got: {}", relative.content);
	assert_same_dir(first_line(&relative), &sub);

	let absolute = BashTool::legacy_one_shot()
		.execute(&json!({"command": "pwd", "cwd": sub.display().to_string()}), &ctx)
		.await
		.expect("bash");
	assert_same_dir(first_line(&absolute), &sub);

	let tilde = BashTool::legacy_one_shot()
		.execute(&json!({"command": "pwd", "cwd": "~/nested"}), &ctx)
		.await
		.expect("bash");
	assert_same_dir(first_line(&tilde), &nested);

	let bare_tilde = BashTool::legacy_one_shot()
		.execute(&json!({"command": "pwd", "cwd": "~"}), &ctx)
		.await
		.expect("bash");
	assert_same_dir(first_line(&bare_tilde), &home);
}

fn assert_same_dir(reported: &str, expected: &std::path::Path) {
	let reported = std::path::Path::new(reported.trim());
	let resolved = std::fs::canonicalize(reported).unwrap_or_else(|_| reported.to_path_buf());
	let expected = std::fs::canonicalize(expected).expect("canonical expected dir");
	assert_eq!(resolved, expected, "reported cwd {reported:?}");
}

#[tokio::test]
async fn invalid_cwd_is_an_error_result_and_runs_nothing() {
	let dir = TempDir::new("bash-cwd-invalid");
	let marker = dir.join("marker.txt");
	let ctx = AgentContext::new(dir.path.clone());

	let missing = BashTool::legacy_one_shot()
		.execute(
			&json!({
				"command": format!("touch {}", marker.display()),
				"cwd": "/nonexistent/bash-contract-directory"
			}),
			&ctx,
		)
		.await
		.expect("bash");
	assert!(missing.is_error);
	assert!(
		missing.content.contains("Working directory does not exist: /nonexistent/bash-contract-directory"),
		"got: {}",
		missing.content
	);

	let file = dir.join("plain-file");
	std::fs::write(&file, "x").expect("write file");
	let not_a_directory = BashTool::legacy_one_shot()
		.execute(
			&json!({
				"command": format!("touch {}", marker.display()),
				"cwd": file.display().to_string()
			}),
			&ctx,
		)
		.await
		.expect("bash");
	assert!(not_a_directory.is_error);
	assert!(
		not_a_directory.content.contains("Working directory is not a directory"),
		"got: {}",
		not_a_directory.content
	);

	let numeric = BashTool::legacy_one_shot()
		.execute(&json!({"command": "true", "cwd": 7}), &ctx)
		.await
		.expect_err("non-string cwd must be rejected");
	assert!(matches!(&numeric, ToolError::InvalidArguments(_)));

	assert!(!marker.exists(), "command ran despite an invalid cwd");
}

#[tokio::test]
async fn timeout_zero_disables_the_deadline() {
	let dir = TempDir::new("bash-timeout-zero");
	let ctx = AgentContext::new(dir.path.clone());

	let result = BashTool::legacy_one_shot()
		.execute(&json!({"command": "sleep 1.5; echo done", "timeout": 0}), &ctx)
		.await
		.expect("bash");

	assert!(!result.is_error, "timeout 0 must not impose a deadline: {}", result.content);
	assert!(result.content.contains("done"), "got: {}", result.content);
}

#[tokio::test]
async fn small_timeouts_clamp_up_to_one_second() {
	let dir = TempDir::new("bash-timeout-clamp");
	let ctx = AgentContext::new(dir.path.clone());

	// A 1 ms deadline would kill this; the documented floor is one second.
	let result = BashTool::legacy_one_shot()
		.execute(&json!({"command": "sleep 0.3; echo clamped", "timeout": 0.001}), &ctx)
		.await
		.expect("bash");

	assert!(!result.is_error, "timeout was not clamped to the 1s floor: {}", result.content);
	assert!(result.content.contains("clamped"), "got: {}", result.content);

	let non_finite = BashTool::legacy_one_shot()
		.execute(&json!({"command": "true", "timeout": "soon"}), &ctx)
		.await
		.expect_err("non-numeric timeout must be rejected");
	assert!(matches!(&non_finite, ToolError::InvalidArguments(_)));
}

#[tokio::test]
async fn timeout_kills_a_long_command_and_reports_a_timeout() {
	let dir = TempDir::new("bash-timeout-kill");
	let marker = dir.join("finished.txt");
	let ctx = AgentContext::new(dir.path.clone());

	let result = BashTool::legacy_one_shot()
		.execute(
			&json!({
				"command": format!("sleep 5; touch {}", marker.display()),
				"timeout": 1
			}),
			&ctx,
		)
		.await
		.expect("bash");

	assert!(result.is_error);
	assert!(result.content.contains("timed out: true"), "got: {}", result.content);
	assert!(!marker.exists(), "command ran past its timeout");
}

#[tokio::test]
async fn cancellation_aborts_the_run_before_its_deadline() {
	let dir = TempDir::new("bash-cancel");
	let marker = dir.join("finished.txt");
	let token = CancellationToken::new();
	let ctx = AgentContext::with_cancellation(dir.path.clone(), token.clone());

	let args = json!({"command": format!("sleep 5; touch {}", marker.display())});
	let handle = tokio::spawn(async move { BashTool::legacy_one_shot().execute(&args, &ctx).await });
	tokio::time::sleep(Duration::from_millis(200)).await;
	token.cancel();

	let result = tokio::time::timeout(Duration::from_secs(10), handle)
		.await
		.expect("cancelled run must settle")
		.expect("join")
		.expect("bash");

	assert!(result.is_error);
	assert!(result.content.contains("cancelled: true"), "got: {}", result.content);
	assert!(!result.content.contains("timed out: true"), "got: {}", result.content);
	assert!(!marker.exists(), "cancelled command kept running");
}

#[tokio::test]
async fn invalid_run_modes_are_rejected_not_silently_ignored() {
	let dir = TempDir::new("bash-modes");
	let ctx = AgentContext::new(dir.path.clone());

	let pty = BashTool::legacy_one_shot()
		.execute(&json!({"command": "true", "pty": true}), &ctx)
		.await
		.expect_err("pty must be rejected");
	assert!(matches!(&pty, ToolError::InvalidArguments(_)));
	assert!(pty.to_string().contains("pty"));

	let async_marker = dir.path.join("async_ran");
	let background = BashTool::legacy_one_shot()
		.execute(
			&json!({"command": format!("touch {}", async_marker.display()), "async": true}),
			&ctx,
		)
		.await
		.expect_err("async needs a supervisor");
	assert!(matches!(&background, ToolError::InvalidArguments(_)));
	assert!(
		!async_marker.exists(),
		"rejected async command must not start"
	);

	let legacy = BashTool::legacy_one_shot()
		.execute(&json!({"command": "true", "timeout_ms": 1000}), &ctx)
		.await
		.expect_err("timeout_ms was removed");
	assert!(matches!(&legacy, ToolError::InvalidArguments(_)));
	assert!(legacy.to_string().contains("timeout"), "got: {legacy}");

	// The legacy spelling is rejected before the async branch can start a job.
	let backend: std::sync::Arc<dyn pi_agent::tools::process::ProcessBackend> =
		std::sync::Arc::new(pi_agent::tools::process::ProcessManager::new(dir.join("processes")));
	let legacy_async_marker = dir.path.join("legacy_async_ran");
	let legacy_async = BashTool::with_processes(backend, None)
		.execute(
			&json!({
				"command": format!("touch {}", legacy_async_marker.display()),
				"async": true,
				"timeout_ms": 1
			}),
			&ctx,
		)
		.await
		.expect_err("async with timeout_ms was removed");
	assert!(matches!(&legacy_async, ToolError::InvalidArguments(_)));
	assert!(
		!legacy_async_marker.exists(),
		"legacy timeout_ms must not start a supervised job"
	);

	let explicit_off = BashTool::legacy_one_shot()
		.execute(&json!({"command": "echo off", "pty": false, "async": false}), &ctx)
		.await
		.expect("explicit false stays accepted");
	assert!(!explicit_off.is_error);
	assert!(explicit_off.content.contains("off"));
}

#[tokio::test]
async fn output_stays_capped_and_the_status_trailer_survives() {
	let dir = TempDir::new("bash-cap");
	let ctx = AgentContext::new(dir.path.clone());

	let result = BashTool::legacy_one_shot()
		.execute(
			&json!({"command": "head -c 200000 /dev/zero | tr '\\0' 'a'"}),
			&ctx,
		)
		.await
		.expect("bash");

	assert!(!result.is_error, "got: {}", result.content.len());
	assert!(result.content.contains("output truncated:"), "cap missing: {}", result.content.len());
	assert!(result.content.len() < 100 * 1024, "output escaped the cap: {}", result.content.len());
	assert!(result.content.contains("exit code: 0"), "trailer lost: {}", result.content);
}

#[tokio::test]
async fn async_bash_shares_the_supervisor_process_table() {
	use std::sync::Arc;

	use pi_agent::tools::process::{
		LogOptions, ProcessBackend, ProcessManager, WaitRequest,
	};

	let dir = TempDir::new("bash-async");
	let backend: Arc<dyn ProcessBackend> = Arc::new(ProcessManager::new(dir.join("processes")));
	let tool = BashTool::with_processes(Arc::clone(&backend), None);
	let ctx = AgentContext::new(dir.path.clone());

	let started = tool
		.execute(&json!({"command": "sleep 1; echo async-done", "async": true}), &ctx)
		.await
		.expect("async bash");
	assert!(!started.is_error, "got: {}", started.content);
	assert!(
		started.content.contains("Backgrounded as job bash-1"),
		"job id missing: {}",
		started.content
	);

	let logs = Arc::clone(&backend)
		.logs(
			"bash-1".to_string(),
			LogOptions {
				lines: 20,
				head: false,
				grep: None,
				follow: true,
				cursor: Some(0),
				timeout_ms: 5_000,
			},
			None,
		)
		.await
		.expect("shared logs");
	assert!(logs.text.contains("async-done"), "got: {}", logs.text);

	let waited = Arc::clone(&backend)
		.wait(
			"bash-1".to_string(),
			WaitRequest { for_exit: true, pattern: None },
			5_000,
			None,
		)
		.await
		.expect("shared wait");
	assert_eq!(waited.snapshot.state.as_str(), "exited");
	let listed = Arc::clone(&backend).list(None).await.expect("shared list");
	assert!(listed.iter().any(|item| item.name == "bash-1"));
}

#[tokio::test]
async fn persistent_shell_keeps_cwd_exports_and_functions_per_agent() {
	let dir = TempDir::new("bash-persistent");
	let sub = dir.join("sub");
	std::fs::create_dir_all(&sub).expect("create sub");

	let shell = Arc::new(pi_agent::AgentShell::new());
	let ctx = AgentContext::new(dir.path.clone());
	let env = ctx.env.clone();
	let ctx = ctx.with_shell(Some(shell.handle(&env)));
	let tool = BashTool::new();

	let first = tool
		.execute(
			&json!({"command": "cd sub && export KEEP=kept && keep() { echo kept; }"}),
			&ctx,
		)
		.await
		.expect("first bash");
	assert!(!first.is_error, "got: {}", first.content);

	let second = tool
		.execute(&json!({"command": "pwd; printf '%s\\n' \"$KEEP\"; keep"}), &ctx)
		.await
		.expect("second bash");
	assert!(!second.is_error, "got: {}", second.content);
	assert!(second.content.contains("kept"), "export/function lost: {}", second.content);
	let state = ctx
		.shell
		.as_ref()
		.expect("shell")
		.snapshot_state()
		.await
		.expect("materialized session");
	assert_eq!(state.cwd, sub.to_string_lossy().into_owned(), "cd did not persist");
	assert_eq!(state.env.get("KEEP").map(String::as_str), Some("kept"));

	let child_shell = Arc::new(pi_agent::AgentShell::new());
	let child_ctx = ctx.clone().with_shell(Some(child_shell.handle(&ctx.env)));
	let child = tool
		.execute(
			&json!({"command": "printf '%s\\n' \"${KEEP-}\"; type keep >/dev/null 2>&1; echo fn:$?"}),
			&child_ctx,
		)
		.await
		.expect("child bash");
	assert!(!child.is_error, "got: {}", child.content);
	assert!(child.content.contains("fn:1"), "child inherited function: {}", child.content);
	let child_state = child_ctx
		.shell
		.as_ref()
		.expect("child shell")
		.snapshot_state()
		.await
		.expect("child session");
	assert_eq!(child_state.cwd, dir.path.to_string_lossy().into_owned(), "child inherited parent cwd");
	assert!(!child_state.env.contains_key("KEEP"), "child inherited parent export");
}

#[derive(Default)]
struct TerminalBridge {
	creates: AtomicUsize,
}

#[async_trait]
impl AcpClientBridge for TerminalBridge {
	fn supports_terminal(&self) -> bool {
		true
	}

	async fn read_text_file(
		&self,
		_path: &str,
		_line: Option<u64>,
		_limit: Option<u64>,
		_cancel: &CancellationToken,
	) -> Result<String, AcpClientError> {
		Err(AcpClientError::Unsupported)
	}

	async fn write_text_file(
		&self,
		_path: &str,
		_content: &str,
		_cancel: &CancellationToken,
	) -> Result<(), AcpClientError> {
		Err(AcpClientError::Unsupported)
	}

	async fn terminal_create(
		&self,
		_command: &str,
		_args: Vec<String>,
		_env: Vec<(String, String)>,
		_cwd: Option<&str>,
		_output_byte_limit: Option<u64>,
		_cancel: &CancellationToken,
	) -> Result<String, AcpClientError> {
		self.creates.fetch_add(1, Ordering::SeqCst);
		Ok("terminal-1".to_string())
	}

	async fn terminal_output(
		&self,
		_terminal_id: &str,
		_cancel: &CancellationToken,
	) -> Result<Value, AcpClientError> {
		Ok(json!({ "output": "remote-output\n" }))
	}

	async fn terminal_wait_for_exit(
		&self,
		_terminal_id: &str,
		_cancel: &CancellationToken,
	) -> Result<Value, AcpClientError> {
		Ok(json!({ "exitCode": 0 }))
	}

	async fn terminal_kill(
		&self,
		_terminal_id: &str,
		_cancel: &CancellationToken,
	) -> Result<Value, AcpClientError> {
		Ok(Value::Null)
	}

	async fn terminal_release(
		&self,
		_terminal_id: &str,
		_cancel: &CancellationToken,
	) -> Result<Value, AcpClientError> {
		Ok(Value::Null)
	}
}

#[tokio::test]
async fn acp_terminal_is_used_only_by_the_explicit_legacy_engine() {
	let dir = TempDir::new("bash-acp-legacy");
	let bridge = Arc::new(TerminalBridge::default());
	let tool = BashTool::legacy_one_shot().with_acp_bridge(Some(bridge.clone()));

	let result = tool
		.execute(&json!({"command": "echo local"}), &AgentContext::new(dir.path.clone()))
		.await
		.expect("legacy acp bash");
	assert!(!result.is_error, "got: {}", result.content);
	assert!(result.content.contains("remote-output"), "got: {}", result.content);
	assert_eq!(bridge.creates.load(Ordering::SeqCst), 1, "legacy engine must use the ACP terminal");
}

#[tokio::test]
async fn persistent_engine_keeps_local_state_when_acp_advertises_terminal() {
	let dir = TempDir::new("bash-acp-persistent");
	let sub = dir.join("sub");
	std::fs::create_dir_all(&sub).expect("create sub");
	let bridge = Arc::new(TerminalBridge::default());
	let shell = Arc::new(pi_agent::AgentShell::new());
	let ctx = AgentContext::new(dir.path.clone());
	let env = ctx.env.clone();
	let ctx = ctx.with_shell(Some(shell.handle(&env)));
	let tool = BashTool::new().with_acp_bridge(Some(bridge.clone()));

	let first = tool
		.execute(&json!({"command": "cd sub && export KEEP=kept"}), &ctx)
		.await
		.expect("first persistent bash");
	assert!(!first.is_error, "got: {}", first.content);

	let second = tool
		.execute(&json!({"command": "pwd; printf '%s\\n' \"$KEEP\""}), &ctx)
		.await
		.expect("second persistent bash");
	assert!(!second.is_error, "got: {}", second.content);
	assert!(second.content.contains("kept"), "state lost: {}", second.content);
	assert!(
		second.content.contains("remote terminal was not used"),
		"missing local-routing note: {}",
		second.content
	);
	assert!(!second.content.contains("remote-output"), "ACP terminal was used: {}", second.content);
	assert_eq!(bridge.creates.load(Ordering::SeqCst), 0, "persistent engine must stay local");
	assert_eq!(
		ctx.shell.as_ref().expect("shell").snapshot_state().await.expect("session").cwd,
		sub.to_string_lossy().into_owned()
	);
}

#[tokio::test]
async fn exit_reports_session_reset_and_next_call_starts_at_project() {
	let dir = TempDir::new("bash-exit-reset");
	let sub = dir.join("sub");
	std::fs::create_dir_all(&sub).expect("create sub");
	let shell = Arc::new(pi_agent::AgentShell::new());
	let ctx = AgentContext::new(dir.path.clone());
	let env = ctx.env.clone();
	let ctx = ctx.with_shell(Some(shell.handle(&env)));
	let tool = BashTool::new();

	let moved = tool
		.execute(&json!({"command": "cd sub && export KEEP=kept"}), &ctx)
		.await
		.expect("move shell");
	assert!(!moved.is_error, "got: {}", moved.content);

	let exited = tool
		.execute(&json!({"command": "exit 0"}), &ctx)
		.await
		.expect("exit bash");
	assert!(!exited.is_error, "exit 0 must stay successful: {}", exited.content);
	assert!(
		exited.content.contains("session reset: true"),
		"exit must report the discarded session: {}",
		exited.content
	);
	assert!(
		exited.content.contains("persistent shell session was reset"),
		"missing neutral reset banner: {}",
		exited.content
	);

	let next = tool
		.execute(&json!({"command": "pwd; printf '%s\\n' \"${KEEP-}\""}), &ctx)
		.await
		.expect("next bash");
	assert!(!next.is_error, "got: {}", next.content);
	assert_eq!(
		first_line(&next).to_owned(),
		dir.path.to_string_lossy().into_owned(),
		"next call must start at the project cwd: {}",
		next.content
	);
	assert!(!next.content.contains("kept"), "exit leaked shell state: {}", next.content);
}
