//! Tests for stdio MCP launch approval gate, --tools filtering before spawn,
//! truthful connection errors, and early discovery cancellation.

use std::fs;
use std::path::Path;

use omp_cli::approve::{ApprovalMode, Approvals};
use omp_cli::config::{Config, DiscoveredMcpServer};
use omp_cli::runtime::filter_and_spawn_mcp_servers;
use pi_agent::mcp::{MCPServerConfig, MCPStdioServerConfig, create_mcp_tool_name};
struct TempDir(std::path::PathBuf);

impl TempDir {
	fn new() -> Self {
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("time before epoch")
			.as_nanos();
		let path =
			std::env::temp_dir().join(format!("omp-test-mcp-launch-{}-{nanos}", std::process::id()));
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

fn make_mock_stdio_server(dir: &Path, server_name: &str) -> (DiscoveredMcpServer, std::path::PathBuf) {
	let marker_file = dir.join(format!("{server_name}_spawned.marker"));
	let script_path = dir.join(format!("{server_name}_mcp.py"));

	fs::write(
		&script_path,
		format!(
			r#"import sys, json
open("{}", "w").write("spawned")
for line in sys.stdin:
    if not line: break
    req = json.loads(line)
    mid = req.get("id")
    method = req.get("method")
    if method == "initialize":
        res = {{"jsonrpc":"2.0","id":mid,"result":{{"protocolVersion":"2024-11-05","capabilities":{{"tools":{{}}}},"serverInfo":{{"name":"{}","version":"1.0"}}}}}}
    elif method == "tools/list":
        res = {{"jsonrpc":"2.0","id":mid,"result":{{"tools":[{{"name":"echo","description":"Echo","inputSchema":{{"type":"object"}}}}]}}}}
    else:
        res = {{"jsonrpc":"2.0","id":mid,"result":{{}}}}
    sys.stdout.write(json.dumps(res) + "\n")
    sys.stdout.flush()
"#,
			marker_file.display(),
			server_name,
		),
	)
	.expect("write mock script");

	let config = MCPServerConfig::Stdio(MCPStdioServerConfig {
		command: "python3".to_string(),
		args: vec![script_path.to_str().unwrap().to_string()],
		env: std::collections::HashMap::new(),
		cwd: None,
		timeout: Some(5000),
		request_id_format: None,
	});

	let server = DiscoveredMcpServer {
		name: server_name.to_string(),
		config,
		source_path: dir.to_path_buf(),
	};

	(server, marker_file)
}

/// Stdio server whose `tools/list` behaviour is selectable: `ok`, `error` or
/// `hang`. It records its pid and every lifecycle marker so the test can prove
/// the child dies with its session.
fn make_scripted_stdio_server(
	dir: &Path,
	server_name: &str,
	mode: &str,
) -> (DiscoveredMcpServer, std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
	let spawned = dir.join(format!("{server_name}_spawned.marker"));
	let listed = dir.join(format!("{server_name}_listed.marker"));
	let pid_file = dir.join(format!("{server_name}_pid.txt"));
	let script_path = dir.join(format!("{server_name}_scripted_mcp.py"));

	fs::write(
		&script_path,
		r#"import sys, json, os, time
marker, list_marker, pid_path, mode, server_name = sys.argv[1:6]
open(marker, "w").write("spawned")
open(pid_path, "w").write(str(os.getpid()))
for line in sys.stdin:
    if not line.strip():
        continue
    req = json.loads(line)
    mid = req.get("id")
    method = req.get("method")
    if method == "initialize":
        caps = {"resources": {}} if mode == "resources_only" else {"tools": {}}
        res = {"jsonrpc":"2.0","id":mid,"result":{"protocolVersion":"2024-11-05","capabilities":caps,"serverInfo":{"name":server_name,"version":"1.0"}}}
    elif method == "resources/list":
        res = {"jsonrpc":"2.0","id":mid,"result":{"resources":[{"uri":"test://notes","name":"notes","mimeType":"text/plain"}]}}
    elif method == "resources/read":
        uri = (req.get("params") or {}).get("uri", "")
        res = {"jsonrpc":"2.0","id":mid,"result":{"contents":[{"uri":uri,"mimeType":"text/plain","text":"hello from resource-only"}]}}
    elif method == "tools/list":
        open(list_marker, "w").write("listed")
        if mode == "error":
            res = {"jsonrpc":"2.0","id":mid,"error":{"code":-32603,"message":"boom"}}
        elif mode == "hang":
            time.sleep(3600)
            continue
        elif mode == "resources_only":
            res = {"jsonrpc":"2.0","id":mid,"error":{"code":-32601,"message":"method not found"}}
        else:
            res = {"jsonrpc":"2.0","id":mid,"result":{"tools":[{"name":"echo","description":"Echo","inputSchema":{"type":"object"}}]}}
    elif mid is None:
        continue
    else:
        res = {"jsonrpc":"2.0","id":mid,"result":{}}
    sys.stdout.write(json.dumps(res) + "\n")
    sys.stdout.flush()
"#,
	)
	.expect("write scripted mock script");

	let config = MCPServerConfig::Stdio(MCPStdioServerConfig {
		command: "python3".to_string(),
		args: vec![
			script_path.to_str().unwrap().to_string(),
			spawned.to_str().unwrap().to_string(),
			listed.to_str().unwrap().to_string(),
			pid_file.to_str().unwrap().to_string(),
			mode.to_string(),
			server_name.to_string(),
		],
		env: std::collections::HashMap::new(),
		cwd: None,
		timeout: Some(5000),
		request_id_format: None,
	});

	let server = DiscoveredMcpServer {
		name: server_name.to_string(),
		config,
		source_path: dir.to_path_buf(),
	};

	(server, spawned, listed, pid_file)
}

fn mock_child_pid(pid_file: &Path) -> i32 {
	let text = fs::read_to_string(pid_file).expect("server writes its pid");
	text.trim().parse().expect("numeric child pid")
}

fn mock_child_alive(pid: i32) -> bool {
	std::process::Command::new("kill")
		.args(["-0", &pid.to_string()])
		.status()
		.map(|status| status.success())
		.unwrap_or_else(|_| Path::new(&format!("/proc/{pid}")).exists())
}

/// Kill delivery is awaited by `close`, but the child is reaped by a task on
/// the runner runtime; drive it while polling so a zombie is not mistaken for a
/// live server.
fn wait_for_mock_child_exit(rt: &tokio::runtime::Runtime, pid_file: &Path) {
	let pid = mock_child_pid(pid_file);
	for _ in 0..100 {
		if !mock_child_alive(pid) {
			return;
		}
		rt.block_on(tokio::time::sleep(std::time::Duration::from_millis(20)));
	}
	panic!("MCP child {pid} still alive");
}

#[test]
fn test_stdio_mcp_launch_denied_without_allow_or_yolo() {
	let temp_dir = TempDir::new();
	let (server, marker_file) = make_mock_stdio_server(temp_dir.path(), "mock");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery must not abort runs when server is skipped");

	assert!(tools.is_empty(), "unapproved stdio server tools must not be registered");
	assert!(
		!marker_file.exists(),
		"stdio MCP server must NOT be spawned when launch approval is denied"
	);
}

#[test]
fn test_stdio_mcp_launch_denied_under_write_mode_without_allow() {
	let temp_dir = TempDir::new();
	let (server, marker_file) = make_mock_stdio_server(temp_dir.path(), "mock");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::Write, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery must not abort runs under write mode when server is skipped");

	assert!(tools.is_empty());
	assert!(
		!marker_file.exists(),
		"stdio MCP server must NOT be spawned under write mode without explicit allow"
	);
}

#[test]
fn test_stdio_mcp_launch_allowed_with_yolo() {
	let temp_dir = TempDir::new();
	let (server, marker_file) = make_mock_stdio_server(temp_dir.path(), "mock");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("yolo must permit stdio launch");

	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name(), "mcp__mock_echo");
	assert!(
		marker_file.exists(),
		"stdio MCP server must be spawned under yolo"
	);
}

#[test]
fn test_stdio_mcp_launch_allowed_with_explicit_server_allow() {
	let temp_dir = TempDir::new();
	let (server, marker_file) = make_mock_stdio_server(temp_dir.path(), "mock");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec!["mcp:mock".to_string()]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("explicit server allow must permit stdio launch");

	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name(), "mcp__mock_echo");
	assert!(
		marker_file.exists(),
		"stdio MCP server must be spawned when server is explicitly allowed"
	);
}

#[test]
fn test_stdio_mcp_launch_allowed_with_explicit_tool_allow() {
	let temp_dir = TempDir::new();
	let (server, marker_file) = make_mock_stdio_server(temp_dir.path(), "mock");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec!["mcp__mock_echo".to_string()]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("explicit tool allow must permit stdio launch");

	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name(), "mcp__mock_echo");
	assert!(
		marker_file.exists(),
		"stdio MCP server must be spawned when tool is explicitly allowed"
	);
}

#[test]
fn test_stdio_mcp_skipped_when_tools_filter_excludes_it() {
	let temp_dir = TempDir::new();
	let (server, marker_file) = make_mock_stdio_server(temp_dir.path(), "mock");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	// Even under Yolo, if --tools restricts to builtin "read", mock server must NOT be spawned
	let mut config = Config::default();
	config.tools = Some(vec!["read".to_string()]);
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("excluding server via --tools must skip spawn cleanly");

	assert!(tools.is_empty());
	assert!(
		!marker_file.exists(),
		"stdio MCP server must NOT be spawned when excluded by --tools filter"
	);
}

#[test]
fn test_truthful_connection_error_when_server_fails() {
	let temp_dir = TempDir::new();
	let broken_config = MCPServerConfig::Stdio(MCPStdioServerConfig {
		command: "/nonexistent/binary_path_that_fails".to_string(),
		args: Vec::new(),
		env: std::collections::HashMap::new(),
		cwd: None,
		timeout: Some(1000),
		request_id_format: None,
	});

	let server = DiscoveredMcpServer {
		name: "broken_srv".to_string(),
		config: broken_config,
		source_path: temp_dir.path().to_path_buf(),
	};

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let mut config = Config::default();
	config.tools = Some(vec!["mcp__broken_srv_echo".to_string()]);
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let res = rt.block_on(filter_and_spawn_mcp_servers(
		vec![server],
		temp_dir.path(),
		&config,
		&approvals,
		&cancel_token,
	));

	let err = res.expect_err("connecting to broken server must fail");
	let err_msg = err.to_string();
	assert!(
		err_msg.contains("Failed to connect to MCP server \"broken_srv\""),
		"must report truthful connection error, got: {err_msg}"
	);
	assert!(
		!err_msg.contains("Unknown tool in --tools"),
		"must NOT report misleading Unknown tool error when server connection failed, got: {err_msg}"
	);
}

#[test]
fn test_truthful_error_when_denied_server_tool_requested_in_tools() {
	let temp_dir = TempDir::new();
	let (server, marker_file) = make_mock_stdio_server(temp_dir.path(), "mock");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let mut config = Config::default();
	config.tools = Some(vec!["mcp__mock_echo".to_string()]);
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let res = rt.block_on(filter_and_spawn_mcp_servers(
		vec![server],
		temp_dir.path(),
		&config,
		&approvals,
		&cancel_token,
	));

	let err = res.expect_err("requesting denied server tool must fail with truthful launch denial");
	let err_msg = err.to_string();
	assert!(
		err_msg.contains("MCP server \"mock\" was not launched"),
		"must report truthful launch denial error, got: {err_msg}"
	);
	assert!(
		!err_msg.contains("Unknown tool in --tools"),
		"must NOT report misleading Unknown tool error when server launch was denied, got: {err_msg}"
	);
	assert!(
		!marker_file.exists(),
		"stdio MCP server must NOT be spawned when launch approval is denied"
	);
}

#[test]
fn test_exact_server_identity_prevents_sanitized_collision() {
	let temp_dir = TempDir::new();
	let (server_dash, marker_dash) = make_mock_stdio_server(temp_dir.path(), "foo-bar");
	let (server_dot, marker_dot) = make_mock_stdio_server(temp_dir.path(), "foo.bar");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	// Explicitly allow ONLY "foo-bar", NOT "foo.bar"
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec!["mcp:foo-bar".to_string()]);
	let cancel_token = pi_agent::CancellationToken::new();

	let _tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_dash, server_dot],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert!(marker_dash.exists(), "foo-bar was explicitly allowed and must spawn");
	assert!(!marker_dot.exists(), "foo.bar was NOT allowed and must NOT spawn despite sanitized name similarity");
}

#[test]
fn test_tool_level_allow_ambiguous_prefix_denies_both_servers() {
	let temp_dir = TempDir::new();
	let (server_foo, marker_foo) = make_mock_stdio_server(temp_dir.path(), "foo");
	let (server_foo_bar, marker_foo_bar) = make_mock_stdio_server(temp_dir.path(), "foo_bar");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	// Ambiguous tool allow: could be tool "bar_echo" on "foo", or tool "echo" on "foo_bar"
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec!["mcp__foo_bar_echo".to_string()]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_foo, server_foo_bar],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert!(tools.is_empty(), "neither server should register tools without explicit server allow when ambiguous");
	assert!(!marker_foo.exists(), "unrelated prefix server foo must NOT spawn for ambiguous tool allow");
	assert!(!marker_foo_bar.exists(), "server foo_bar must NOT spawn on ambiguous tool allow without explicit server-level allow");
}

#[test]
fn test_explicit_server_allow_resolves_ambiguity_between_prefix_servers() {
	let temp_dir = TempDir::new();
	let (server_foo, marker_foo) = make_mock_stdio_server(temp_dir.path(), "foo");
	let (server_foo_bar, marker_foo_bar) = make_mock_stdio_server(temp_dir.path(), "foo_bar");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	// Explicit server-level allow resolves the ambiguity conservatively
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec!["mcp:foo_bar".to_string()]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_foo, server_foo_bar],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name(), "mcp__foo_bar_echo");
	assert!(marker_foo_bar.exists(), "foo_bar was explicitly allowed at server level and must spawn");
	assert!(!marker_foo.exists(), "foo was NOT allowed and must NOT spawn");
}

#[test]
fn test_tools_prefilter_does_not_launch_unrelated_prefix_server() {
	let temp_dir = TempDir::new();
	let (server_foo, marker_foo) = make_mock_stdio_server(temp_dir.path(), "foo");
	let (server_foo_bar, marker_foo_bar) = make_mock_stdio_server(temp_dir.path(), "foo_bar");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	// Even under Yolo, --tools specifying foo_bar's tool must NOT launch unrelated server foo
	let mut config = Config::default();
	config.tools = Some(vec!["mcp__foo_bar_echo".to_string()]);
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_foo, server_foo_bar],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name(), "mcp__foo_bar_echo");
	assert!(!marker_foo.exists(), "unrelated server foo must NOT spawn when --tools requested foo_bar's tool");
	assert!(marker_foo_bar.exists(), "foo_bar must spawn when its tool is requested in --tools");
}

#[test]
fn test_tools_prefilter_matches_bounded_64_char_tool_name() {
	let temp_dir = TempDir::new();
	let long_name = "a_very_long_mcp_server_name_that_exceeds_fifty_characters_in_total";
	let (server, marker) = make_mock_stdio_server(temp_dir.path(), long_name);

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let tool_name = create_mcp_tool_name(long_name, "echo");
	assert_eq!(tool_name.len(), 64, "tool name must be capped at 64 chars");

	let mut config = Config::default();
	config.tools = Some(vec![tool_name.clone()]);
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name(), tool_name);
	assert!(marker.exists(), "server with bounded 64-char tool name must be matched by --tools and spawned");
}

#[test]
fn test_cancellation_aborts_discovery_without_spawning() {
	let temp_dir = TempDir::new();
	let (server, marker) = make_mock_stdio_server(temp_dir.path(), "mock");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();
	cancel_token.cancel();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("cancelled discovery returns cleanly");

	assert!(tools.is_empty(), "cancelled discovery must not return tools");
	assert!(!marker.exists(), "cancelled discovery must not spawn servers or leave readers");
}

/// Regression: the connection used to be registered before `tools/list`, so a
/// failing list left a live `mcp://` entry and its stdio child behind.
#[test]
fn test_failed_tools_list_leaves_no_registry_entry_or_child() {
	let temp_dir = TempDir::new();
	let (server, _spawned, _listed, pid_file) =
		make_scripted_stdio_server(temp_dir.path(), "list_fail", "error");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("a failed tools/list is a server error, not a fatal discovery error");

	assert!(tools.is_empty(), "a failed tools/list must expose no tools");
	assert!(
		pi_agent::mcp::get_active_session("list_fail").is_none(),
		"a failed tools/list must not stay registered for mcp:// resolution"
	);
	wait_for_mock_child_exit(&rt, &pid_file);
}

/// Regression: cancelling while `tools/list` is in flight abandoned a
/// registered session whose transport (and child process) stayed alive. An
/// already-registered server from the same discovery must be closed too.
#[test]
fn test_cancelled_tools_list_leaves_no_registry_entry_or_child() {
	let temp_dir = TempDir::new();
	let (first, _first_spawned, _first_listed, first_pid) =
		make_scripted_stdio_server(temp_dir.path(), "list_first_ok", "ok");
	let (hanging, _spawned, listed, hanging_pid) =
		make_scripted_stdio_server(temp_dir.path(), "list_hang", "hang");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();
	let cancel_thread = {
		let cancel_token = cancel_token.clone();
		std::thread::spawn(move || {
			for _ in 0..500 {
				if listed.exists() {
					cancel_token.cancel();
					return;
				}
				std::thread::sleep(std::time::Duration::from_millis(10));
			}
			panic!("mock server never reached tools/list");
		})
	};

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![first, hanging],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("cancelled discovery returns cleanly");
	cancel_thread.join().expect("cancel thread");

	assert!(tools.is_empty(), "cancelled discovery must return no tools");
	assert!(
		pi_agent::mcp::get_active_session("list_first_ok").is_none(),
		"a session registered before the cancellation must be closed"
	);
	assert!(
		pi_agent::mcp::get_active_session("list_hang").is_none(),
		"cancellation during tools/list must not leave a registry entry"
	);
	wait_for_mock_child_exit(&rt, &first_pid);
	wait_for_mock_child_exit(&rt, &hanging_pid);
}

/// A successful `tools/list` registers the session, and teardown of that
/// session (the owner) removes the entry and reaps the child.
#[test]
fn test_successful_discovery_registers_and_owning_close_reaps_session() {
	let temp_dir = TempDir::new();
	let (server, _spawned, _listed, pid_file) =
		make_scripted_stdio_server(temp_dir.path(), "owner_ok", "ok");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery succeeds");
	assert_eq!(tools.len(), 1, "one tool from the mock server");

	let connection = pi_agent::mcp::get_active_session("owner_ok")
		.expect("a successful tools/list registers the session");

	rt.block_on(connection.close()).expect("close owning session");

	assert!(
		pi_agent::mcp::get_active_session("owner_ok").is_none(),
		"teardown removes the registry entry"
	);
	wait_for_mock_child_exit(&rt, &pid_file);
}

/// Two discoveries can register the same server name (a later turn replaces
/// the registry entry). Teardown of the superseded session must not unregister
/// the successor that consumers now resolve.
#[test]
fn test_closing_superseded_same_name_session_keeps_newer_registration() {
	let temp_dir = TempDir::new();
	let dir_a = temp_dir.path().join("a");
	let dir_b = temp_dir.path().join("b");
	fs::create_dir_all(&dir_a).expect("create dir a");
	fs::create_dir_all(&dir_b).expect("create dir b");
	let (server_a, _spawned_a, _listed_a, _pid_a) =
		make_scripted_stdio_server(&dir_a, "overlap", "ok");
	let (server_b, _spawned_b, _listed_b, _pid_b) =
		make_scripted_stdio_server(&dir_b, "overlap", "ok");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let spawn = |server| {
		rt.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&pi_agent::CancellationToken::new(),
		))
		.expect("discovery succeeds")
	};

	let first_tools = spawn(server_a);
	let first = pi_agent::mcp::get_active_session("overlap").expect("first registration");
	let second_tools = spawn(server_b);
	let second = pi_agent::mcp::get_active_session("overlap").expect("second registration");
	assert!(!std::sync::Arc::ptr_eq(&first, &second), "second discovery replaces the entry");

	rt.block_on(first.close()).expect("close superseded session");

	let registered = pi_agent::mcp::get_active_session("overlap")
		.expect("successor registration must survive the older session's teardown");
	assert!(std::sync::Arc::ptr_eq(&registered, &second));

	rt.block_on(second.close()).expect("close successor");
	assert!(pi_agent::mcp::get_active_session("overlap").is_none());
	drop((first_tools, second_tools));
}

/// Resource-only servers omit the `tools` capability. `tools/list` must not be
/// attempted (TS client.ts:214), the session must stay registered for `mcp://`
/// consumers, and owning-session teardown must remove it.
#[test]
fn test_resource_only_server_reachable_via_mcp_url_until_teardown() {
	let temp_dir = TempDir::new();
	let (server, _spawned, listed, pid_file) =
		make_scripted_stdio_server(temp_dir.path(), "resources_only", "resources_only");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("resource-only discovery succeeds");
	assert!(tools.is_empty(), "resource-only server exposes no tools");
	assert!(
		!listed.exists(),
		"a server without the tools capability must not receive tools/list"
	);

	let connection = pi_agent::mcp::get_active_session("resources_only")
		.expect("resource-only session is registered");
	let router = pi_agent::tools::internal_urls::InternalUrlRouter::new();
	let scope = pi_agent::tools::internal_urls::UrlScope::default();
	let context = scope.resolve_context(temp_dir.path(), &cancel_token);

	let listing = rt
		.block_on(router.resolve("mcp://resources_only", &context))
		.expect("mcp:// lists the resource-only server's resources");
	assert!(listing.content.contains("test://notes"), "{}", listing.content);
	let body = rt
		.block_on(router.resolve("mcp://resources_only/test://notes", &context))
		.expect("mcp:// reads the resource-only server's resource");
	assert_eq!(body.content, "hello from resource-only");

	rt.block_on(connection.close()).expect("teardown owning session");
	let error = rt
		.block_on(router.resolve("mcp://resources_only", &context))
		.expect_err("teardown must unregister the session");
	assert!(
		error.contains("Unknown MCP server \"resources_only\"")
			|| error.contains("No active MCP sessions"),
		"{error}"
	);
	wait_for_mock_child_exit(&rt, &pid_file);
}

#[test]
fn test_two_long_servers_differing_in_prefix_spawns_only_requested_server() {
	let temp_dir = TempDir::new();
	let name_a = "long_server_alpha_name_exceeding_fifty_characters_in_total";
	let name_b = "long_server_beta_name_exceeding_fifty_characters_in_total";
	let (server_a, marker_a) = make_mock_stdio_server(temp_dir.path(), name_a);
	let (server_b, marker_b) = make_mock_stdio_server(temp_dir.path(), name_b);

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let tool_a = create_mcp_tool_name(name_a, "echo");
	assert_eq!(tool_a.len(), 64, "tool name must be capped at 64 chars");

	let mut config = Config::default();
	config.tools = Some(vec![tool_a.clone()]);
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_a, server_b],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name(), tool_a);
	assert!(marker_a.exists(), "server A must spawn when its 64-char capped tool is requested");
	assert!(!marker_b.exists(), "server B must NOT spawn when server A's capped tool is requested");
}

#[test]
fn test_two_long_servers_uncapped_exact_length_64_prefilter_spawns_only_requested() {
	let temp_dir = TempDir::new();
	let name_a = "shared_prefix_exceeding_fifty_characters_in_total_aaaa";
	let name_b = "shared_prefix_exceeding_fifty_characters_in_total_bbbb";
	let (server_a, marker_a) = make_mock_stdio_server(temp_dir.path(), name_a);
	let (server_b, marker_b) = make_mock_stdio_server(temp_dir.path(), name_b);

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let tool_a = create_mcp_tool_name(name_a, "echo");
	assert_eq!(tool_a.len(), 64, "tool name must be exactly 64 chars");

	let mut config = Config::default();
	config.tools = Some(vec![tool_a.clone()]);
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_a, server_b],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name(), tool_a);
	assert!(marker_a.exists(), "server A must spawn when its tool is requested");
	assert!(!marker_b.exists(), "server B must NOT spawn when server A's tool is requested");
}

#[test]
fn test_two_long_servers_sharing_first_50_chars_tool_allow_spawns_only_requested() {
	let temp_dir = TempDir::new();
	let name_a = "shared_prefix_exceeding_fifty_characters_in_total_aaaa";
	let name_b = "shared_prefix_exceeding_fifty_characters_in_total_bbbb";
	let (server_a, marker_a) = make_mock_stdio_server(temp_dir.path(), name_a);
	let (server_b, marker_b) = make_mock_stdio_server(temp_dir.path(), name_b);

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let tool_a = create_mcp_tool_name(name_a, "echo");
	assert_eq!(tool_a.len(), 64, "tool name must be exactly 64 chars");

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec![tool_a.clone()]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_a, server_b],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name(), tool_a);
	assert!(marker_a.exists(), "server A must spawn when its unambiguous 64-char tool is allowed");
	assert!(!marker_b.exists(), "server B must NOT spawn when server A's tool is allowed");
}

#[test]
fn test_real_capped_tie_tool_allow_ambiguous_denies_both() {
	let temp_dir = TempDir::new();
	let prefix = "common_prefix_long_enough_to_exceed_the_fifty_limit_";
	let name_a = format!("{prefix}xxxxxx");
	let name_b = format!("{prefix}yyyyyy");
	let (server_a, marker_a) = make_mock_stdio_server(temp_dir.path(), &name_a);
	let (server_b, marker_b) = make_mock_stdio_server(temp_dir.path(), &name_b);

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let tool_a = create_mcp_tool_name(&name_a, "echo");
	assert_eq!(tool_a.len(), 64, "capped tool name must be 64 chars");

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec![tool_a]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_a, server_b],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert!(tools.is_empty(), "tied capped tool allow must deny launch for both servers without explicit server allow");
	assert!(!marker_a.exists(), "server A must NOT spawn under ambiguous capped tool allow");
	assert!(!marker_b.exists(), "server B must NOT spawn under ambiguous capped tool allow");
}

#[test]
fn test_real_capped_tie_prefilter_returns_ambiguity_error_under_yolo() {
	let temp_dir = TempDir::new();
	let prefix = "common_prefix_long_enough_to_exceed_the_fifty_limit_";
	let name_a = format!("{prefix}xxxxxx");
	let name_b = format!("{prefix}yyyyyy");
	let (server_a, marker_a) = make_mock_stdio_server(temp_dir.path(), &name_a);
	let (server_b, marker_b) = make_mock_stdio_server(temp_dir.path(), &name_b);

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let tool_a = create_mcp_tool_name(&name_a, "echo");
	assert_eq!(tool_a.len(), 64, "capped tool name must be 64 chars");

	let mut config = Config::default();
	config.tools = Some(vec![tool_a.clone()]);
	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let res = rt.block_on(filter_and_spawn_mcp_servers(
		vec![server_a, server_b],
		temp_dir.path(),
		&config,
		&approvals,
		&cancel_token,
	));

	let err = res.expect_err("tied capped prefix under Yolo must return ambiguity error");
	let err_msg = err.to_string();
	assert!(
		err_msg.contains("Ambiguous tool") && err_msg.contains("matches multiple servers"),
		"must return precise ambiguity error naming servers, got: {err_msg}"
	);
	assert!(!marker_a.exists(), "tied capped prefix must not spawn A");
	assert!(!marker_b.exists(), "tied capped prefix must not spawn B");
}

#[test]
fn test_real_capped_tie_disambiguated_by_server_allow_spawns_only_a() {
	let temp_dir = TempDir::new();
	let prefix = "common_prefix_long_enough_to_exceed_the_fifty_limit_";
	let name_a = format!("{prefix}xxxxxx");
	let name_b = format!("{prefix}yyyyyy");
	let (server_a, marker_a) = make_mock_stdio_server(temp_dir.path(), &name_a);
	let (server_b, marker_b) = make_mock_stdio_server(temp_dir.path(), &name_b);

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let tool_a = create_mcp_tool_name(&name_a, "echo");
	assert_eq!(tool_a.len(), 64, "capped tool name must be 64 chars");

	let mut config = Config::default();
	config.tools = Some(vec![tool_a.clone()]);
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec![format!("mcp:{name_a}")]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_a, server_b],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("explicit server allow must disambiguate capped tie");

	assert_eq!(tools.len(), 1);
	assert_eq!(tools[0].name(), tool_a);
	assert!(marker_a.exists(), "server A must spawn when disambiguated by server allow");
	assert!(!marker_b.exists(), "unrelated server B must NOT spawn");
}

#[test]
fn test_tool_level_allow_with_exact_tool_name_does_not_launch_distinct_server() {
	let temp_dir = TempDir::new();
	let (server_foo, marker_foo) = make_mock_stdio_server(temp_dir.path(), "foo");
	let (server_foo_bar, marker_foo_bar) = make_mock_stdio_server(temp_dir.path(), "foo_bar");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	// User allowed mcp__foo_bar (tool "bar" on server "foo").
	// When server "foo_bar" is also discovered, mcp__foo_bar matches bare server form
	// for foo_bar and tool on foo: ambiguous, fail-closed, neither server spawns.
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec!["mcp__foo_bar".to_string()]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_foo, server_foo_bar],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert!(tools.is_empty(), "ambiguous bare server mcp__ entry must register no tools");
	assert!(!marker_foo_bar.exists(), "server foo_bar must NOT spawn from mcp__foo_bar tool allow");
	assert!(!marker_foo.exists(), "server foo must NOT spawn because identity is ambiguous with foo_bar");
}

#[test]
fn test_tool_level_allow_with_matching_tool_launches_when_server_identity_unambiguous() {
	let temp_dir = TempDir::new();
	// Discovery-shaping resistance: foo_bar is configured in .omp/mcp.json but disabled.
	// A naive check seeing only discovered server "foo" would mistake `mcp__foo_bar` for tool
	// "bar" on "foo" and launch foo. omp inspects all config layers, detects collision with
	// configured server foo_bar, and fails closed.
	let mcp_json = temp_dir.path().join(".omp").join("mcp.json");
	fs::create_dir_all(mcp_json.parent().unwrap()).unwrap();
	fs::write(
		&mcp_json,
		r#"{"mcpServers":{"foo_bar":{"command":"true","enabled":false}}}"#,
	)
	.unwrap();

	let (server_foo, marker_foo) = make_mock_stdio_server(temp_dir.path(), "foo");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec!["mcp__foo_bar".to_string()]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_foo],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert!(tools.is_empty(), "structurally ambiguous mcp__ allow must register no tools");
	assert!(!marker_foo.exists(), "server foo must NOT spawn from structurally ambiguous mcp__ allow");

	// Unambiguous colon syntax mcp:foo:echo explicitly and safely authorizes the tool.
	let (server_foo_2, marker_foo_2) = make_mock_stdio_server(temp_dir.path(), "foo");
	let approvals_colon = Approvals::new(ApprovalMode::AlwaysAsk, vec!["mcp:foo:echo".to_string()]);
	let tools_colon = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server_foo_2],
			temp_dir.path(),
			&config,
			&approvals_colon,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert_eq!(tools_colon.len(), 1);
	assert!(marker_foo_2.exists(), "server foo must spawn when authorized via unambiguous colon syntax");
}

#[test]
fn test_agent_dir_disabled_server_participates_in_ambiguity_check() {
	let temp_dir = TempDir::new();
	let agent_dir = TempDir::new();
	let agent_mcp_json = agent_dir.path().join("mcp.json");
	fs::write(
		&agent_mcp_json,
		r#"{"mcpServers":{"foo_bar":{"command":"true","enabled":false}}}"#,
	)
	.unwrap();

	let (server_foo, marker_foo) = make_mock_stdio_server(temp_dir.path(), "foo");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec!["mcp__foo_bar".to_string()]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(omp_cli::runtime::filter_and_spawn_mcp_servers_with_paths_detailed(
			vec![server_foo],
			temp_dir.path(),
			agent_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert!(tools.tools.is_empty(), "agent_dir disabled server must prevent ambiguous launch");
	assert!(!marker_foo.exists(), "server foo must NOT spawn when agent_dir contains ambiguous foo_bar");
}

#[test]
fn test_bare_server_name_read_does_not_launch_server() {
	let temp_dir = TempDir::new();
	let (server, marker_file) = make_mock_stdio_server(temp_dir.path(), "read");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec!["read".to_string()]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			vec![server],
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert!(tools.is_empty(), "bare allow 'read' must NOT spawn server named 'read'");
	assert!(!marker_file.exists(), "marker must NOT exist when bare server allow is denied");
}

#[test]
fn test_bare_server_name_mcp_prefixed_does_not_launch_server() {
	let temp_dir = TempDir::new();
	let agent_dir = TempDir::new();
	let agent_mcp_json = agent_dir.path().join("mcp.json");
	fs::write(
		&agent_mcp_json,
		r#"{"mcpServers":{"foo_bar":{"command":"true","enabled":false}}}"#,
	)
	.unwrap();

	let (server, marker_file) = make_mock_stdio_server(temp_dir.path(), "mcp__foo_bar");

	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let config = Config::default();
	let approvals = Approvals::new(ApprovalMode::AlwaysAsk, vec!["mcp__foo_bar".to_string()]);
	let cancel_token = pi_agent::CancellationToken::new();

	let tools = rt
		.block_on(omp_cli::runtime::filter_and_spawn_mcp_servers_with_paths_detailed(
			vec![server],
			temp_dir.path(),
			agent_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("discovery should proceed");

	assert!(tools.tools.is_empty(), "server named mcp__foo_bar must NOT spawn from bare allow mcp__foo_bar");
	assert!(!marker_file.exists(), "marker must NOT exist when bare mcp__foo_bar allow is denied");
}

#[test]
fn checkpoint_tools_are_known_tools_selection_names() {
	let temp_dir = TempDir::new();
	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let approvals = Approvals::new(ApprovalMode::Yolo, Vec::new());
	let cancel_token = pi_agent::CancellationToken::new();

	let mut config = Config::default();
	config.tools = Some(vec!["checkpoint".to_string(), "rewind".to_string()]);
	let tools = rt
		.block_on(filter_and_spawn_mcp_servers(
			Vec::new(),
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect("checkpoint/rewind are selectable builtin names");
	assert!(tools.is_empty(), "no MCP server was discovered");

	config.tools = Some(vec!["checkpoint".to_string(), "bogus_tool".to_string()]);
	let err = rt
		.block_on(filter_and_spawn_mcp_servers(
			Vec::new(),
			temp_dir.path(),
			&config,
			&approvals,
			&cancel_token,
		))
		.expect_err("unknown tool names must still fail");
	assert!(err.to_string().contains("Unknown tool in --tools: bogus_tool"));
}
