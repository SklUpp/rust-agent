//! D20 workspace trust store: persistence, config-change invalidation, config
//! integration, MCP discovery and the CLI handler.

use std::{fs, path::PathBuf};

use aragent::{
	config::Config,
	trust::{TrustStatus, TrustStore, run_trust_command},
};
use pi_agent::hooks::HookEvent;

struct TempDir(PathBuf);

impl TempDir {
	fn new(label: &str) -> Self {
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("clock")
			.as_nanos();
		let path = std::env::temp_dir().join(format!(
			"omp-trust-{label}-{}-{nanos}",
			std::process::id()
		));
		fs::create_dir_all(&path).expect("create temp dir");
		Self(path)
	}

	fn path(&self) -> &std::path::Path {
		&self.0
	}
}

impl Drop for TempDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}

fn write_file(path: &std::path::Path, contents: &str) {
	if let Some(parent) = path.parent() {
		fs::create_dir_all(parent).expect("create parent");
	}
	fs::write(path, contents).expect("write file");
}

fn project_workspace(root: &std::path::Path) -> PathBuf {
	let project = root.join("project");
	fs::create_dir_all(&project).expect("project dir");
	write_file(
		&project.join(".claude/settings.json"),
		r#"{"hooks":{"PreToolUse":[{"matcher":"Bash","hooks":[{"type":"command","command":"printf project"}]}]}}"#,
	);
	project
}

#[test]
fn grant_list_status_and_revoke_round_trip() {
	let temp = TempDir::new("roundtrip");
	let agent = temp.path().join("agent");
	let project = project_workspace(temp.path());

	let mut store = TrustStore::load_from_agent_dir(&agent);
	assert_eq!(store.status(&project), TrustStatus::Untrusted);
	assert!(!store.is_trusted(&project));
	assert!(store.list().is_empty());

	let report = store.grant(&project).expect("grant");
	assert_eq!(report.path, fs::canonicalize(&project).expect("canonical project"));
	assert_eq!(report.config_hashes.len(), 1);
	assert!(report.granted_at_ms > 0);
	assert!(matches!(
		store.status(&project),
		TrustStatus::Trusted { granted_at_ms } if granted_at_ms == report.granted_at_ms
	));

	let entries = store.list();
	assert_eq!(entries.len(), 1);
	assert_eq!(entries[0].config_files, 1);
	assert_eq!(
		entries[0].path,
		fs::canonicalize(&project).expect("canonical").display().to_string()
	);

	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		let file_mode = fs::metadata(agent.join("trust.json"))
			.expect("trust file")
			.permissions()
			.mode()
			& 0o777;
		assert_eq!(file_mode, 0o600, "trust.json must be private");
		let dir_mode = fs::metadata(&agent).expect("agent dir").permissions().mode() & 0o777;
		assert_eq!(dir_mode, 0o700, "agent dir must be private");
	}

	// Reload sees the persisted grant.
	let reloaded = TrustStore::load(&agent.join("trust.json"));
	assert!(reloaded.is_trusted(&project));

	let mut store = reloaded;
	assert!(store.revoke(&project).expect("revoke"));
	assert!(!store.is_trusted(&project));
	assert!(store.list().is_empty());
	assert!(!store.revoke(&project).expect("second revoke is a no-op"));
}

#[test]
fn project_hooks_require_trust_but_policy_stays_stripped() {
	let temp = TempDir::new("hooks");
	let home = temp.path().join("home");
	fs::create_dir_all(&home).expect("home");
	let project = temp.path().join("project");
	write_file(
		&project.join(".claude/settings.json"),
		r#"{"hooks":{"PreToolUse":[{"matcher":"Bash","hooks":[{"type":"command","command":"printf claude"}]}]}}"#,
	);
	write_file(
		&project.join(".codex/hooks.json"),
		r#"{"SessionStart":[{"command":"printf codex"}]}"#,
	);
	write_file(
		&project.join(".omp/config.yml"),
		r#"hooks:
  Stop:
    - command: printf yaml
allow:
  - bash
approvalMode: yolo
bash:
  patterns:
    - match: "rm *"
      approval: allow
searxng:
  endpoint: http://127.0.0.1:9999
"#,
	);

	let untrusted_hooks = Config::discover_hooks(&project, Some(&home), false);
	assert!(untrusted_hooks.rules().is_empty());

	let mut untrusted = Config::default();
	Config::apply_project_layers(&mut untrusted, &project, false).expect("untrusted layers");
	assert!(untrusted.hooks.is_empty());
	assert!(untrusted.allow.is_empty());
	assert!(untrusted.approval_mode.is_none());
	assert!(untrusted.searxng.endpoint.is_none());

	let trusted_hooks = Config::discover_hooks(&project, Some(&home), true);
	assert_eq!(trusted_hooks.rules().len(), 2);
	assert_eq!(trusted_hooks.rules()[0].event, HookEvent::PreToolUse);
	assert_eq!(trusted_hooks.rules()[1].event, HookEvent::SessionStart);

	let mut trusted = Config::default();
	Config::apply_project_layers(&mut trusted, &project, true).expect("trusted layers");
	assert_eq!(trusted.hooks.rules().len(), 1);
	assert_eq!(trusted.hooks.rules()[0].event, HookEvent::Stop);
	// D20: trust enables hooks only; approvals and endpoints stay stripped.
	assert!(trusted.allow.is_empty(), "allow must stay stripped when trusted");
	assert!(trusted.approval_mode.is_none(), "yolo must stay stripped when trusted");
	assert!(
		trusted.searxng.endpoint.is_none(),
		"searxng must stay stripped when trusted"
	);
	assert!(
		trusted.bash.patterns.as_ref().is_none_or(Vec::is_empty),
		"bash allow rules must stay stripped when trusted"
	);

	// A symlinked project config is an alias: its hooks never become trusted,
	// even with a workspace grant.
	#[cfg(unix)]
	{
		use std::os::unix::fs::symlink;
		let outside = temp.path().join("outside-config.yml");
		fs::write(&outside, "hooks:\n  Stop:\n    - command: printf outside\n").expect("outside");
		let config_path = project.join(".omp/config.yml");
		fs::remove_file(&config_path).expect("remove config");
		symlink(&outside, &config_path).expect("symlink config");
		let mut aliased = Config::default();
		Config::apply_project_layers(&mut aliased, &project, true).expect("aliased layers");
		assert!(aliased.hooks.is_empty(), "symlinked config hooks stay untrusted");
	}
}

#[cfg(unix)]
#[test]
fn symlinked_project_config_dirs_are_not_trusted() {
	use std::os::unix::fs::symlink;

	let temp = TempDir::new("symlink-dirs");
	let project = temp.path().join("project");
	fs::create_dir_all(&project).expect("project");

	// `.omp` -> outside: the config.yml is readable but outside the checkout.
	let outside_omp = temp.path().join("outside-omp");
	write_file(
		&outside_omp.join("config.yml"),
		"hooks:\n  Stop:\n    - command: printf outside\n",
	);
	symlink(&outside_omp, project.join(".omp")).expect("symlink .omp");
	let mut config = Config::default();
	Config::apply_project_layers(&mut config, &project, true).expect("layers");
	assert!(
		config.hooks.is_empty(),
		"`.omp` parent symlink escaping the root must not supply hooks"
	);

	// `.claude` -> outside: project hook discovery rejects the canonical path.
	let outside_claude = temp.path().join("outside-claude");
	write_file(
		&outside_claude.join("settings.json"),
		r#"{"hooks":{"Stop":[{"command":"printf outside"}]}}"#,
	);
	symlink(&outside_claude, project.join(".claude")).expect("symlink .claude");
	let hooks = Config::discover_hooks(&project, None, true);
	assert!(
		hooks.rules().is_empty(),
		"`.claude` parent symlink escaping the root must not supply hooks"
	);
}

#[test]
fn config_change_invalidates_until_regranted() {
	let temp = TempDir::new("invalidate");
	let agent = temp.path().join("agent");
	let project = project_workspace(temp.path());
	let settings = project.join(".claude/settings.json");

	let mut store = TrustStore::load_from_agent_dir(&agent);
	store.grant(&project).expect("grant");
	assert!(store.is_trusted(&project));

	fs::write(
		&settings,
		r#"{"hooks":{"PreToolUse":[{"matcher":"Bash","hooks":[{"type":"command","command":"printf changed"}]}]}}"#,
	)
	.expect("edit settings");
	let status = store.status(&project);
	match status {
		TrustStatus::Invalidated { reason } => {
			assert!(reason.contains(".claude/settings.json"), "reason: {reason}");
		},
		other => panic!("expected invalidation, got {other:?}"),
	}
	assert!(!store.is_trusted(&project));

	// Re-granting records the new fingerprint.
	store.grant(&project).expect("re-grant");
	assert!(store.status(&project).is_trusted());

	// A brand-new project MCP file also fails closed.
	write_file(&project.join(".omp/mcp.json"), r#"{"mcpServers":{}}"#);
	match store.status(&project) {
		TrustStatus::Invalidated { reason } => {
			assert!(reason.contains("added .omp/mcp.json"), "reason: {reason}");
		},
		other => panic!("expected invalidation after adding a file, got {other:?}"),
	}
}

#[test]
fn unrelated_config_keys_do_not_invalidate() {
	let temp = TempDir::new("unrelated");
	let agent = temp.path().join("agent");
	let project = temp.path().join("project");
	let config_path = project.join(".omp/config.yml");
	write_file(
		&config_path,
		"defaultModel: openai/gpt-5\nhooks:\n  Stop:\n    - command: printf one\n",
	);

	let mut store = TrustStore::load_from_agent_dir(&agent);
	store.grant(&project).expect("grant");
	assert!(store.status(&project).is_trusted());

	// Only `hooks` is fingerprinted for this file.
	write_file(
		&config_path,
		"defaultModel: openai/gpt-6\nhooks:\n  Stop:\n    - command: printf one\n",
	);
	assert!(
		store.status(&project).is_trusted(),
		"unrelated keys must not invalidate the grant"
	);

	write_file(
		&config_path,
		"defaultModel: openai/gpt-6\nhooks:\n  Stop:\n    - command: printf two\n",
	);
	assert!(!store.status(&project).is_trusted(), "hook edits invalidate");
}

#[test]
fn project_mcp_keeps_host_provenance_while_reporting_trust() {
	let temp = TempDir::new("mcp");
	let agent = temp.path().join("agent");
	let project = temp.path().join("project");
	let mcp_path = project.join(".omp/mcp.json");
	write_file(
		&mcp_path,
		r#"{"mcpServers":{"proj":{"command":"echo","args":["hi"]}}}"#,
	);

	let project_server_trusted = |report: &aragent::config::McpDiscoveryReport| {
		report
			.servers
			.iter()
			.find(|server| server.name == "proj")
			.expect("proj server")
			.trusted
	};

	let report = aragent::config::discover_mcp_servers_with_paths(&project, &agent);
	assert!(!report.workspace_trusted, "workspace is untrusted by default");
	assert!(!project_server_trusted(&report), "project MCP is never host-trusted");

	let mut store = TrustStore::load_from_agent_dir(&agent);
	store.grant(&project).expect("grant");
	let report = aragent::config::discover_mcp_servers_with_paths(&project, &agent);
	assert!(report.workspace_trusted, "granted workspace reports trust");
	assert!(
		!project_server_trusted(&report),
		"workspace trust must not bypass the project MCP launch gate"
	);

	store.revoke(&project).expect("revoke");
	let report = aragent::config::discover_mcp_servers_with_paths(&project, &agent);
	assert!(!report.workspace_trusted, "revoke returns to untrusted");
	assert!(!project_server_trusted(&report));

	// A symlink or hardlink alias is repository-controlled bytes: it disables
	// the workspace-trust flag even while the grant itself fingerprints fine.
	#[cfg(unix)]
	{
		use std::os::unix::fs::symlink;
		let outside = temp.path().join("outside-mcp.json");
		fs::write(
			&outside,
			r#"{"mcpServers":{"proj":{"command":"echo","args":["hi"]}}}"#,
		)
		.expect("outside mcp");
		store.grant(&project).expect("re-grant");
		fs::remove_file(&mcp_path).expect("remove mcp");
		symlink(&outside, &mcp_path).expect("symlink mcp");
		let report = aragent::config::discover_mcp_servers_with_paths(&project, &agent);
		assert!(!report.workspace_trusted, "symlinked MCP disables workspace trust");
		assert!(!project_server_trusted(&report), "symlinked MCP stays untrusted");
		assert!(
			report.warnings.iter().any(|warning| warning.contains("escapes the project root")),
			"warnings: {:?}",
			report.warnings
		);

		fs::remove_file(&mcp_path).expect("remove symlink");
		fs::hard_link(&outside, &mcp_path).expect("hardlink mcp");
		let report = aragent::config::discover_mcp_servers_with_paths(&project, &agent);
		assert!(!report.workspace_trusted, "hardlinked MCP disables workspace trust");
		assert!(!project_server_trusted(&report), "hardlinked MCP stays untrusted");
	}
}

#[test]
fn incomplete_trust_json_fails_closed() {
	let temp = TempDir::new("incomplete");
	let agent = temp.path().join("agent");
	let project = temp.path().join("project");
	fs::create_dir_all(&agent).expect("agent");
	fs::create_dir_all(&project).expect("project");
	let trust_path = agent.join("trust.json");
	let key = fs::canonicalize(&project).expect("canonical").display().to_string();

	let mut workspaces = serde_json::Map::new();
	workspaces.insert(
		key.clone(),
		serde_json::json!({"granted_at_ms": 1, "config_hashes": {}}),
	);
	let missing_version = serde_json::json!({ "workspaces": workspaces });
	fs::write(&trust_path, serde_json::to_string(&missing_version).expect("json"))
		.expect("write");
	let store = TrustStore::load(&trust_path);
	assert!(store.list().is_empty(), "missing version must drop the store");
	assert!(!store.is_trusted(&project));

	let mut workspaces = serde_json::Map::new();
	workspaces.insert(key, serde_json::json!({"granted_at_ms": 1}));
	let missing_hashes = serde_json::json!({ "version": 1, "workspaces": workspaces });
	fs::write(&trust_path, serde_json::to_string(&missing_hashes).expect("json"))
		.expect("write");
	let store = TrustStore::load(&trust_path);
	assert!(
		store.list().is_empty(),
		"missing config_hashes must drop the store"
	);
	assert!(!store.is_trusted(&project));
}

#[cfg(unix)]
#[test]
fn save_tightens_existing_agent_dir_permissions() {
	use std::os::unix::fs::PermissionsExt;

	let temp = TempDir::new("tighten");
	let agent = temp.path().join("agent");
	fs::create_dir_all(&agent).expect("agent");
	fs::set_permissions(&agent, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
	assert_eq!(fs::metadata(&agent).expect("meta").permissions().mode() & 0o777, 0o755);

	let project = project_workspace(temp.path());
	let mut store = TrustStore::load_from_agent_dir(&agent);
	store.grant(&project).expect("grant");
	assert_eq!(
		fs::metadata(&agent).expect("meta").permissions().mode() & 0o777,
		0o700,
		"save must tighten an existing agent dir to 0700"
	);
}

#[test]
fn run_trust_command_covers_grant_list_status_revoke() {
	let temp = TempDir::new("cli");
	let agent = temp.path().join("agent");
	let project = project_workspace(temp.path());

	let mut out = Vec::new();
	run_trust_command(Some("grant"), None, false, &project, &agent, &mut out).expect("grant");
	assert!(String::from_utf8(out).expect("utf8").contains("trusted"));

	let mut out = Vec::new();
	run_trust_command(Some("list"), None, true, &project, &agent, &mut out).expect("list json");
	let listed: serde_json::Value =
		serde_json::from_slice(&out).expect("parse list json");
	let workspaces = listed["workspaces"].as_array().expect("workspaces array");
	assert_eq!(workspaces.len(), 1);
	assert_eq!(
		workspaces[0]["path"].as_str().expect("path"),
		fs::canonicalize(&project).expect("canonical").display().to_string()
	);

	let mut out = Vec::new();
	run_trust_command(Some("status"), None, true, &project, &agent, &mut out).expect("status json");
	let status: serde_json::Value = serde_json::from_slice(&out).expect("parse status json");
	assert_eq!(status["trusted"].as_bool(), Some(true));
	assert_eq!(status["status"].as_str(), Some("trusted"));

	let mut out = Vec::new();
	run_trust_command(Some("revoke"), None, false, &project, &agent, &mut out).expect("revoke");
	assert!(String::from_utf8(out).expect("utf8").contains("revoked"));

	let mut out = Vec::new();
	run_trust_command(Some("status"), None, true, &project, &agent, &mut out).expect("status json");
	let status: serde_json::Value = serde_json::from_slice(&out).expect("parse status json");
	assert_eq!(status["trusted"].as_bool(), Some(false));
	assert_eq!(status["status"].as_str(), Some("untrusted"));

	let mut out = Vec::new();
	run_trust_command(Some("list"), None, true, &project, &agent, &mut out).expect("list json");
	let listed: serde_json::Value = serde_json::from_slice(&out).expect("parse list json");
	assert!(listed["workspaces"].as_array().expect("array").is_empty());

	assert!(
		run_trust_command(Some("bogus"), None, false, &project, &agent, &mut Vec::<u8>::new())
			.is_err()
	);
}

#[test]
fn ssh_and_lsp_surfaces_invalidate_a_grant() {
	let temp = TempDir::new("ssh-lsp");
	let agent = temp.path().join("agent");
	let project = temp.path().join("project");
	fs::create_dir_all(&project).expect("project dir");
	let ssh_config = project.join(".omp/ssh.json");
	write_file(&ssh_config, r#"{"hosts":{"prod":{"host":"10.0.0.1"}}}"#);

	let mut store = TrustStore::load_from_agent_dir(&agent);
	let report = store.grant(&project).expect("grant");
	assert!(
		report.config_hashes.contains_key(".omp/ssh.json"),
		"project ssh.json must be fingerprinted: {:?}",
		report.config_hashes.keys().collect::<Vec<_>>()
	);
	assert!(store.is_trusted(&project));

	// Changing the SSH destination after the grant must fail closed.
	write_file(&ssh_config, r#"{"hosts":{"prod":{"host":"10.0.0.9"}}}"#);
	match store.status(&project) {
		TrustStatus::Invalidated { reason } => {
			assert!(reason.contains(".omp/ssh.json"), "reason: {reason}");
		},
		other => panic!("expected invalidation after ssh.json edit, got {other:?}"),
	}

	store.grant(&project).expect("re-grant");
	// LSP configs launch server commands, so a new project LSP file invalidates.
	write_file(&project.join(".omp/lsp.json"), r#"{"servers":{}}"#);
	match store.status(&project) {
		TrustStatus::Invalidated { reason } => {
			assert!(reason.contains("added .omp/lsp.json"), "reason: {reason}");
		},
		other => panic!("expected invalidation after adding lsp.json, got {other:?}"),
	}
}

#[test]
fn untrusted_router_hides_project_ssh_hosts() {
	use pi_agent::{
		CancellationToken,
		tools::internal_urls::{InternalUrlRouter, UrlScope},
	};

	let temp = TempDir::new("ssh-router");
	let project = temp.path().join("project");
	write_file(
		&project.join(".omp/ssh.json"),
		r#"{"hosts":{"omp-trust-ssh-fixture":{"host":"10.0.0.1"}}}"#,
	);
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.expect("runtime");
	let scope = UrlScope::default();
	let context = scope.resolve_context(&project, &CancellationToken::new());

	let untrusted = InternalUrlRouter::with_ssh_project_config(false);
	let index = runtime
		.block_on(untrusted.resolve("ssh://", &context))
		.expect("index");
	assert!(
		!index.content.contains("omp-trust-ssh-fixture"),
		"untrusted router must not list project ssh hosts: {}",
		index.content
	);

	let trusted = InternalUrlRouter::with_ssh_project_config(true);
	let index = runtime
		.block_on(trusted.resolve("ssh://", &context))
		.expect("index");
	assert!(
		index.content.contains("omp-trust-ssh-fixture"),
		"trusted router must list project ssh hosts: {}",
		index.content
	);
}

#[test]
fn legacy_trust_store_is_read_only_and_writes_move_to_aragent() {
	let home = TempDir::new("legacy-home");
	let legacy = home.path().join(".omp/agent/trust.json");
	write_file(
		&legacy,
		r#"{"version":1,"workspaces":{"/legacy/ws":{"granted_at_ms":7,"config_hashes":{}}}}"#,
	);
	let saved: Vec<_> = ["HOME", "PI_CONFIG_DIR", "PI_CODING_AGENT_DIR", "ARAGENT_PROFILE", "OMP_PROFILE", "PI_PROFILE"]
		.iter()
		.map(|key| (*key, std::env::var_os(key)))
		.collect();
	// SAFETY: this test binary's other cases use explicit agent dirs and never
	// read HOME while this value is installed.
	unsafe {
		std::env::set_var("HOME", home.path());
		for key in ["PI_CONFIG_DIR", "PI_CODING_AGENT_DIR", "ARAGENT_PROFILE", "OMP_PROFILE", "PI_PROFILE"] {
			std::env::remove_var(key);
		}
	}
	let mut store = TrustStore::load_from_agent_dir(&aragent::dirs::agent_dir());
	let entries = store.list();
	let current = home.path().join(".aragent/agent");
	let project = project_workspace(home.path());
	let grant = store.grant(&project);
	unsafe {
		for (key, value) in saved {
			match value {
				Some(value) => std::env::set_var(key, value),
				None => std::env::remove_var(key),
			}
		}
	}
	assert_eq!(store.path(), current.join("trust.json"));
	assert_eq!(entries.len(), 1, "legacy grants stay readable");
	assert_eq!(entries[0].path, "/legacy/ws");
	grant.expect("grant writes the current root");
	assert!(current.join("trust.json").is_file());
	assert_eq!(
		fs::read_to_string(&legacy).expect("legacy untouched"),
		r#"{"version":1,"workspaces":{"/legacy/ws":{"granted_at_ms":7,"config_hashes":{}}}}"#
	);
}
