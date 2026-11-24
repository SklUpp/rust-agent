#![cfg(all(feature = "test-support", any(target_os = "linux", target_os = "android")))]

//! Live external filesystem MCP server smoke test for Termux / Linux (R0.4).
//!
//! Validates:
//! 1. `initialize` against an external stdio filesystem MCP server.
//! 2. `tools/list` enumerating real filesystem tools.
//! 3. `tools/call` executing filesystem operations (read_file) with exact byte payload validation.
//! 4. Sudden process termination (`SIGKILL`) via direct child seam controller, confirmed by wait.
//! 5. Post-kill call fails with transport error, triggering automatic reconnection on subsequent call.
//! 6. Post-reconnect generation 2 payload validation and kill controller issuance.
//! 7. Clean `close()` confirmed by direct reap of Generation 2 child.
//! 8. Bounded cleanup executed within live Tokio runtime across success, timeout, and panic paths, explicitly surfacing any unconfirmed reap or platform failure.
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::FutureExt;
use pi_agent::mcp::client::connect_to_stdio_server_with_kill_seam;
use pi_agent::mcp::types::{MCPContent, MCPStdioServerConfig, MCPToolCallResult};
use pi_agent::mcp::{ChildKillController, ChildKillHook, ReapError};
use serde_json::json;

struct TempDir(PathBuf);

impl TempDir {
	fn new(prefix: &str) -> Self {
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|d| d.as_nanos())
			.unwrap_or(0);
		let path = std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
		fs::create_dir_all(&path).expect("create temp dir");
		Self(path)
	}

	fn path(&self) -> &Path {
		&self.0
	}
}

impl Drop for TempDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}

fn resolve_server_target(scratch_dir: &Path) -> Option<(String, Vec<String>)> {
	let prefix = std::env::var("PREFIX").unwrap_or_else(|_| "/data/data/com.termux/files/usr".to_string());
	if let Ok(cmd) = std::env::var("MCP_FILESYSTEM_CMD") {
		let trimmed = cmd.trim().to_string();
		if !trimmed.is_empty() {
			return Some((trimmed, vec![scratch_dir.to_string_lossy().to_string()]));
		}
	}

	let candidate_bins = [
		format!("{prefix}/lib/node_modules/omniroute/node_modules/.bin/mcp-server-filesystem"),
		format!("{prefix}/bin/mcp-server-filesystem"),
		"/data/data/com.termux/files/usr/lib/node_modules/omniroute/node_modules/.bin/mcp-server-filesystem".to_string(),
	];
	for bin in &candidate_bins {
		if Path::new(bin).exists() {
			return Some((bin.clone(), vec![scratch_dir.to_string_lossy().to_string()]));
		}
	}

	let candidate_js = [
		format!("{prefix}/lib/node_modules/omniroute/node_modules/@modelcontextprotocol/server-filesystem/dist/index.js"),
		format!("{prefix}/lib/node_modules/@modelcontextprotocol/server-filesystem/dist/index.js"),
		"/data/data/com.termux/files/usr/lib/node_modules/omniroute/node_modules/@modelcontextprotocol/server-filesystem/dist/index.js".to_string(),
	];
	for js in &candidate_js {
		if Path::new(js).exists() {
			return Some(("node".to_string(), vec![js.clone(), scratch_dir.to_string_lossy().to_string()]));
		}
	}

	None
}

fn extract_payload_text(result: &MCPToolCallResult) -> &str {
	assert_ne!(result.is_error, Some(true), "tool call reported error");
	assert!(!result.content.is_empty(), "tool result content must not be empty");
	match &result.content[0] {
		MCPContent::Text { text } => text.as_str(),
		other => panic!("expected text content from read_file, got: {other:?}"),
	}
}

#[tokio::test]
async fn live_filesystem_mcp_smoke() {
	let opt_in = std::env::var("ARAGENT_LIVE_FILESYSTEM_SMOKE").is_ok()
		|| std::env::var("LIVE_MCP_FILESYSTEM").is_ok();
	if !opt_in {
		eprintln!("SKIP: live filesystem MCP server smoke test (set ARAGENT_LIVE_FILESYSTEM_SMOKE=1 to run)");
		return;
	}

	let temp_dir = TempDir::new("mcp-fs-smoke");
	let scratch = temp_dir.path();

	let (command, args) = match resolve_server_target(scratch) {
		Some(target) => target,
		None => {
			panic!("ARAGENT_LIVE_FILESYSTEM_SMOKE set but no preinstalled filesystem MCP server found");
		}
	};

	let probe_file = scratch.join("probe.txt");
	let probe_payload = format!("live-smoke-test-payload-{}", std::process::id());
	fs::write(&probe_file, &probe_payload).expect("write probe file");

	let config = MCPStdioServerConfig {
		command,
		args,
		env: Default::default(),
		cwd: Some(scratch.to_string_lossy().to_string()),
		timeout: Some(15_000), // 15_000 ms explicit budget
		request_id_format: None,
	};

	let (track_tx, mut track_rx) = tokio::sync::mpsc::unbounded_channel::<ChildKillController>();
	let pre_hook: ChildKillHook = std::sync::Arc::new(move |ctrl| {
		let _ = track_tx.send(ctrl);
	});

	let (conn_tx, mut conn_rx) = tokio::sync::oneshot::channel::<std::sync::Arc<pi_agent::mcp::McpConnection>>();

	let probe_file_path = probe_file.to_string_lossy().to_string();
	let expected_payload = probe_payload.clone();

	let scenario = std::panic::AssertUnwindSafe(async move {
		let (connection, mut kill_rx) =
			connect_to_stdio_server_with_kill_seam("fs-live", config, Some(pre_hook))
				.await
				.expect("connect to filesystem mcp server");

		let connection = std::sync::Arc::new(connection);
		let _ = conn_tx.send(std::sync::Arc::clone(&connection));

		let kill_controller_1 = tokio::time::timeout(Duration::from_secs(10), kill_rx.recv())
			.await
			.expect("recv gen 1 controller timed out")
			.expect("receive gen 1 kill controller");
		// 1. tools/list
		let tools = connection.list_tools().await.expect("list tools");
		assert!(
			tools.iter().any(|t| t.name == "read_file"),
			"read_file tool must be available"
		);

		// 2. tools/call read_file (exact byte payload validation)
		let read_args = json!({
			"path": probe_file_path,
		});
		let tool_res_1 = connection
			.call_tool("read_file", read_args.clone())
			.await
			.expect("call read_file gen 1");
		let read_text_1 = extract_payload_text(&tool_res_1);
		assert_eq!(read_text_1, expected_payload, "exact payload mismatch in gen 1 read");

		// 3. Forced SIGKILL via seam controller, confirmed by wait
		#[cfg(unix)]
		let sig = libc::SIGKILL;
		#[cfg(not(unix))]
		let sig = 9;

		let exit_status = tokio::time::timeout(Duration::from_secs(10), kill_controller_1.kill(sig))
			.await
			.expect("kill operation timed out")
			.expect("kill child process");

		#[cfg(unix)]
		{
			use std::os::unix::process::ExitStatusExt;
			assert_eq!(
				exit_status.signal(),
				Some(libc::SIGKILL),
				"child process must be terminated specifically by SIGKILL"
			);
		}
		#[cfg(not(unix))]
		assert!(!exit_status.success(), "killed process must report non-zero exit status");

		// 4. First post-kill call fails with transport failure
		let post_kill_err = connection
			.call_tool("read_file", read_args.clone())
			.await
			.expect_err("call immediately after kill must fail with transport failure");
		assert!(
			matches!(
				post_kill_err,
				pi_agent::mcp::error::McpError::UnknownOutcome(_)
					| pi_agent::mcp::error::McpError::Transport(_)
					| pi_agent::mcp::error::McpError::Closed
			),
			"expected transport failure after kill, got: {post_kill_err:?}"
		);

		// 5. Automatic reconnect & exact payload validation on Generation 2
		let tool_res_2 = connection
			.call_tool("read_file", read_args.clone())
			.await
			.expect("call read_file after reconnect (gen 2)");
		let read_text_2 = extract_payload_text(&tool_res_2);
		assert_eq!(read_text_2, expected_payload, "exact payload mismatch in gen 2 read");

		let mut kill_controller_2 = tokio::time::timeout(Duration::from_secs(10), kill_rx.recv())
			.await
			.expect("recv gen 2 controller timed out")
			.expect("receive gen 2 kill controller");

		// 6. Clean close without zombies, confirmed by waiting on Generation 2 reap
		tokio::time::timeout(Duration::from_secs(10), connection.close())
			.await
			.expect("close timed out")
			.expect("clean connection close");

		let reap_status = kill_controller_2
			.wait_reaped(Duration::from_secs(10))
			.await
			.expect("child 2 must be reaped on clean close");
		assert!(
			reap_status.success() || reap_status.code() == Some(0),
			"gracefully closed child 2 must exit cleanly, got: {reap_status:?}"
		);
	})
	.catch_unwind();

	let overall_timeout = Duration::from_secs(45);
	let scenario_outcome = tokio::time::timeout(overall_timeout, scenario).await;

	// Bounded cleanup phase: runs inside the live Tokio runtime before test exit
	let mut cleanup_errors = Vec::new();

	if let Ok(conn) = conn_rx.try_recv() {
		if !conn.close_completed.load(Ordering::Acquire) {
			match tokio::time::timeout(Duration::from_secs(10), conn.close()).await {
				Ok(Ok(())) => {},
				Ok(Err(e)) => cleanup_errors.push(format!("connection.close reported error: {e:?}")),
				Err(e) => cleanup_errors.push(format!("connection.close timed out: {e:?}")),
			}
		}
	}

	while let Ok(mut ctrl) = track_rx.try_recv() {
		match ctrl.wait_reaped(Duration::from_millis(50)).await {
			Ok(_) => {},
			Err(ReapError::PendingTimeout) => {
				#[cfg(unix)]
				let sig = libc::SIGKILL;
				#[cfg(not(unix))]
				let sig = 9;
				match tokio::time::timeout(Duration::from_secs(3), ctrl.kill(sig)).await {
					Ok(Ok(_)) => {},
					Ok(Err(kill_err)) => cleanup_errors.push(format!("cleanup kill failed: {kill_err:?}")),
					Err(timeout_err) => cleanup_errors.push(format!("cleanup kill timed out: {timeout_err:?}")),
				}
				match ctrl.wait_reaped(Duration::from_secs(10)).await {
					Ok(_) => {},
					Err(reap_err) => {
						cleanup_errors.push(format!(
							"child wait_reaped failed after kill (unavoidable platform limit): {reap_err:?}"
						));
					}
				}
			},
			Err(ReapError::Terminal(terminal_err)) => {
				cleanup_errors.push(format!("terminal reap error during cleanup: {terminal_err:?}"));
			}
		}
	}
	assert!(
		cleanup_errors.is_empty(),
		"cleanup errors surfaced during test teardown: {cleanup_errors:?}"
	);

	match scenario_outcome {
		Ok(Ok(())) => {},
		Ok(Err(panic_payload)) => {
			std::panic::resume_unwind(panic_payload);
		},
		Err(_) => {
			panic!("live filesystem smoke test timed out after {overall_timeout:?}");
		},
	}
}
