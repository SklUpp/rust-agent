//! End-to-end contract tests for OMP-native security scans.
//!
//! Deterministic and offline: a real git fixture repository under `$HOME`, a
//! real SQLite auth store seeded with one pinned OAuth row, and an in-process
//! host whose "model session" calls the mounted `security_publish` tool. No
//! provider account, no network, no ambient config.
//!
//! Covered: the disabled gate, preflight input/scope/KB/output errors, exact
//! OAuth pinning and identity assertions, plan determinism and staleness,
//! start/status/cancel, once-only publication, partial scans, restart recovery,
//! store commit-marker ordering, SARIF/provenance redaction, validation, and
//! the read-only `security://` namespace.

use std::{
	fs,
	path::{Path, PathBuf},
	process::Command,
	sync::{
		Arc, Mutex,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use serde_json::{Value, json};

use omp_cli::{
	auth::{
		AuthBackend,
		http::Cancel,
		security::{ExactSecurityOAuthResolver, resolve_security_oauth_access, select_security_account},
		store::{Credential, CredentialStore, OAuthCredentials},
	},
	security::{
		INTERRUPTED_MESSAGE, PARTIAL_MESSAGE, SECURITY_DISABLED_TOOL, SecurityCoordinator,
		SecurityPreflightInput, SecurityScanTool,
		coordinator::SECURITY_DISABLED_PREFLIGHT,
		host::{SecurityHost, SecurityScanRequest, SecurityScanSession, SecurityScanStats},
		preflight::SecurityTargetRequest,
		publication::{SecurityPublisher, SecurityPublisherOptions},
		resource::{SECURITY_DISABLED_MESSAGE, SecurityUrlHandler},
		store::SecurityStore,
		types::SecurityModelRef,
	},
};
use pi_agent::{
	state::AgentContext,
	tools::{Tool as _, internal_urls::InternalUrl},
};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

struct TempRoot(PathBuf);

impl TempRoot {
	fn new(label: &str) -> Self {
		static NEXT: AtomicU64 = AtomicU64::new(0);
		let home = std::env::var_os("HOME").map(PathBuf::from).expect("HOME is set");
		let path = home.join(format!(
			"omp-security-contract-{label}-{}-{}",
			std::process::id(),
			NEXT.fetch_add(1, Ordering::Relaxed)
		));
		fs::create_dir_all(&path).expect("create temp root");
		Self(path)
	}

	fn path(&self) -> &Path {
		&self.0
	}
}

impl Drop for TempRoot {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}

fn run_git(cwd: &Path, args: &[&str]) {
	let output = Command::new("git")
		.arg("-C")
		.arg(cwd)
		.args(args)
		.output()
		.expect("run git");
	assert!(
		output.status.success(),
		"git {args:?} failed: {}",
		String::from_utf8_lossy(&output.stderr)
	);
}

fn git_fixture(root: &Path) -> PathBuf {
	let repo = root.join("repo");
	fs::create_dir_all(repo.join("src")).expect("repo dirs");
	run_git(&repo, &["init", "-q"]);
	run_git(&repo, &["config", "user.email", "security-fixture@example.test"]);
	run_git(&repo, &["config", "user.name", "Security Fixture"]);
	run_git(&repo, &["config", "commit.gpgsign", "false"]);
	fs::write(repo.join("src/lib.rs"), "pub fn a() {}\n").expect("write src");
	fs::write(repo.join("README.md"), "fixture\n").expect("write readme");
	run_git(&repo, &["add", "."]);
	run_git(&repo, &["commit", "-q", "-m", "one"]);
	fs::write(repo.join("src/lib.rs"), "pub fn a() {}\npub fn b() {}\n").expect("write src 2");
	run_git(&repo, &["commit", "-q", "-am", "two"]);
	repo
}

fn oauth(sequence: u64) -> Credential {
	let expires = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
		.unwrap_or(0)
		+ 3_600_000;
	Credential::OAuth(OAuthCredentials {
		access: format!("access-{sequence}"),
		refresh: format!("refresh-{sequence}"),
		expires,
		enterprise_url: None,
		project_id: None,
		email: Some(format!("user{sequence}@example.test")),
		account_id: Some(format!("acct-{sequence}")),
		api_endpoint: None,
		org_id: Some(format!("org-{sequence}")),
		org_name: Some(format!("Workspace {sequence}")),
		authorized_at: None,
	})
}

fn seed_oauth(db: &Path, sequence: u64) -> i64 {
	let store = CredentialStore::open(db).expect("auth store");
	let id = store
		.upsert("openai-codex", &oauth(sequence))
		.expect("seed oauth row");
	drop(store);
	id
}

#[derive(Clone)]
enum Behavior {
	Publish(Value),
	PublishTwice(Value),
	Partial,
	WaitCancel,
}

struct TestHost {
	cwd: PathBuf,
	state_root: PathBuf,
	auth_db: PathBuf,
	enabled: bool,
	model: Option<SecurityModelRef>,
	config: Value,
	behavior: Behavior,
	execution_roots: Mutex<Vec<PathBuf>>,
}

impl TestHost {
	fn new(
		cwd: &Path,
		root: &Path,
		model: Option<SecurityModelRef>,
		behavior: Behavior,
	) -> Arc<Self> {
		let auth_db = root.join("agent.db");
		if !auth_db.exists() {
			seed_oauth(&auth_db, 1);
		}
		Arc::new(Self {
			cwd: cwd.to_path_buf(),
			state_root: root.join("security-state"),
			auth_db,
			enabled: true,
			model,
			config: json!({"securityEnabled": true}),
			behavior,
			execution_roots: Mutex::new(Vec::new()),
		})
	}

	fn disabled(cwd: &Path, root: &Path) -> Arc<Self> {
		let auth_db = root.join("agent.db");
		if !auth_db.exists() {
			seed_oauth(&auth_db, 1);
		}
		Arc::new(Self {
			cwd: cwd.to_path_buf(),
			state_root: root.join("security-state"),
			auth_db,
			enabled: false,
			model: Some(model()),
			config: json!({"securityEnabled": false}),
			behavior: Behavior::Partial,
			execution_roots: Mutex::new(Vec::new()),
		})
	}
}

fn model() -> SecurityModelRef {
	SecurityModelRef {
		provider: "openai-codex".to_string(),
		model_id: "gpt-5-codex".to_string(),
		thinking_level: None,
	}
}

impl SecurityHost for TestHost {
	fn security_enabled(&self) -> bool {
		self.enabled
	}

	fn cwd(&self) -> PathBuf {
		self.cwd.clone()
	}

	fn session_id(&self) -> Option<String> {
		Some("security-contract-session".to_string())
	}

	fn agent_id(&self) -> Option<String> {
		None
	}

	fn active_model(&self) -> Option<SecurityModelRef> {
		self.model.clone()
	}

	fn config_snapshot(&self) -> Value {
		self.config.clone()
	}

	fn state_root(&self) -> Option<PathBuf> {
		Some(self.state_root.clone())
	}

	fn auth_db(&self) -> PathBuf {
		self.auth_db.clone()
	}

	fn ensure_model(&self, model: &SecurityModelRef) -> Result<(), String> {
		if model.provider == "openai-codex" && model.model_id == "gpt-5-codex" {
			return Ok(());
		}
		Err(format!(
			"Security scan model is unavailable: {}/{}",
			model.provider, model.model_id
		))
	}

	fn create_scan_session(
		&self,
		request: SecurityScanRequest,
	) -> Result<Arc<dyn SecurityScanSession>, String> {
		self.execution_roots
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.push(request.execution_root.clone());
		Ok(Arc::new(FakeSession {
			request,
			behavior: self.behavior.clone(),
		}))
	}
}

struct FakeSession {
	request: SecurityScanRequest,
	behavior: Behavior,
}

impl FakeSession {
	fn publish(&self, params: &Value) -> Result<(), pi_agent::tools::ToolError> {
		let context = AgentContext::new(self.request.execution_root.clone());
		let runtime = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.expect("session runtime");
		runtime
			.block_on(self.request.publication_tool.execute(params, &context))
			.map(|_| ())
	}
}

impl SecurityScanSession for FakeSession {
	fn run(&self, _request_text: &str) -> Result<Option<SecurityScanStats>, String> {
		match &self.behavior {
			Behavior::Publish(params) => {
				self.publish(params).map_err(|error| error.to_string())?;
				Ok(Some(stats()))
			},
			Behavior::PublishTwice(params) => {
				self.publish(params).map_err(|error| error.to_string())?;
				let error = self
					.publish(params)
					.err()
					.expect("second publication must be refused");
				assert!(
					error.to_string().contains("has already been published"),
					"{error}"
				);
				Ok(Some(stats()))
			},
			Behavior::Partial => Ok(None),
			Behavior::WaitCancel => {
				while !self.request.cancel.is_cancelled() {
					std::thread::sleep(Duration::from_millis(5));
				}
				Ok(None)
			},
		}
	}

	fn session_file(&self) -> Option<PathBuf> {
		None
	}

	fn abort(&self) {}
}

fn stats() -> SecurityScanStats {
	SecurityScanStats {
		token_usage: None,
		cost: Some(0.5),
		premium_requests: Some(1),
	}
}

fn publish_params() -> Value {
	json!({
		"findings": [{
			"rule_id": "rust/unsafe-deref",
			"title": "Unsafe dereference",
			"summary": "Dereferences a raw pointer without validation.",
			"severity": "high",
			"confidence": "high",
			"category": "memory-safety",
			"cwe": ["CWE-476"],
			"locations": [{"path": "src/lib.rs", "start_line": 1, "end_line": 2}],
			"evidence": [{"label": "sink", "explanation": "raw pointer deref"}],
			"remediation": "Validate before dereferencing.",
			"validation": "validated"
		}],
		"coverage": {"completeness": "complete"},
		"report": "# Security report\n"
	})
}

fn preflight_input(output: &Path) -> SecurityPreflightInput {
	SecurityPreflightInput {
		output_root: Some(output.to_path_buf()),
		..SecurityPreflightInput::default()
	}
}

async fn call(
	tool: &SecurityScanTool,
	params: Value,
	cwd: &Path,
) -> Result<(String, Value), String> {
	let context = AgentContext::new(cwd.to_path_buf());
	match tool.execute(&params, &context).await {
		Ok(result) => Ok((result.content, result.details.unwrap_or(Value::Null))),
		Err(error) => Err(error.to_string()),
	}
}

fn tool(host: Arc<dyn SecurityHost>) -> (Arc<SecurityCoordinator>, SecurityScanTool) {
	let coordinator = Arc::new(SecurityCoordinator::new(Arc::clone(&host)));
	let tool = SecurityScanTool::with_coordinator(Arc::clone(&host), Arc::clone(&coordinator));
	(coordinator, tool)
}

fn iso_now() -> String {
	omp_cli::output::iso8601(std::time::SystemTime::now())
}

// ---------------------------------------------------------------------------
// Disabled gate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn disabled_gate_blocks_tool_and_preflight() {
	let root = TempRoot::new("disabled-gate");
	let repo = git_fixture(root.path());
	let host = TestHost::disabled(&repo, root.path());
	let (coordinator, tool) = tool(host.clone());

	let error = call(
		&tool,
		json!({"action": "preflight", "target_kind": "repository"}),
		&repo,
	)
	.await
	.expect_err("disabled tool must reject");
	assert_eq!(error, SECURITY_DISABLED_TOOL);

	let error = coordinator
		.preflight(SecurityPreflightInput::default())
		.expect_err("disabled preflight must fail");
	assert_eq!(error.to_string(), SECURITY_DISABLED_PREFLIGHT);

	let error = coordinator
		.start("secplan_deadbeef")
		.expect_err("disabled start must fail");
	assert_eq!(
		error.to_string(),
		omp_cli::security::coordinator::SECURITY_DISABLED_START
	);
}

// ---------------------------------------------------------------------------
// Preflight input / scope / knowledge-base / output errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn preflight_rejects_non_git_scope_kb_output_and_symlinks() {
	let root = TempRoot::new("preflight-errors");
	let repo = git_fixture(root.path());
	let host = TestHost::new(&repo, root.path(), Some(model()), Behavior::Partial);
	let (coordinator, _tool) = tool(host);

	let plain = root.path().join("plain");
	fs::create_dir_all(&plain).expect("plain dir");
	let plain_host = TestHost::new(&plain, root.path(), Some(model()), Behavior::Partial);
	let plain_coordinator = SecurityCoordinator::new(plain_host);
	let error = plain_coordinator
		.preflight(SecurityPreflightInput::default())
		.expect_err("non-git cwd");
	assert!(
		error.to_string().contains("require a Git repository"),
		"unexpected: {error}"
	);

	let error = coordinator
		.preflight(SecurityPreflightInput {
			target: SecurityTargetRequest::ScopedPath {
				include_paths: vec!["../outside".to_string()],
				exclude_paths: Vec::new(),
			},
			..SecurityPreflightInput::default()
		})
		.expect_err("escaping scope");
	assert!(
		error.to_string().contains("repository-relative"),
		"unexpected: {error}"
	);

	let outside = root.path().join("outside");
	fs::create_dir_all(&outside).expect("outside dir");
	std::os::unix::fs::symlink(&outside, repo.join("escape-link")).expect("symlink");
	let error = coordinator
		.preflight(SecurityPreflightInput {
			target: SecurityTargetRequest::ScopedPath {
				include_paths: vec!["escape-link".to_string()],
				exclude_paths: Vec::new(),
			},
			..SecurityPreflightInput::default()
		})
		.expect_err("symlink scope escape");
	assert!(
		error.to_string().contains("outside repository"),
		"unexpected: {error}"
	);

	let error = coordinator
		.preflight(SecurityPreflightInput {
			knowledge_base_paths: vec!["missing-kb.md".to_string()],
			..SecurityPreflightInput::default()
		})
		.expect_err("missing knowledge base");
	assert!(
		error.to_string().contains("Security knowledge base is unavailable"),
		"unexpected: {error}"
	);

	let error = coordinator
		.preflight(SecurityPreflightInput {
			output_root: Some(repo.join("inside-output")),
			..SecurityPreflightInput::default()
		})
		.expect_err("output inside repository");
	assert!(
		error.to_string().contains("outside the scanned repository"),
		"unexpected: {error}"
	);

	let real_output = root.path().join("real-output");
	fs::create_dir_all(&real_output).expect("real output");
	let link_output = root.path().join("output-link");
	std::os::unix::fs::symlink(&real_output, &link_output).expect("output symlink");
	let error = coordinator
		.preflight(SecurityPreflightInput {
			output_root: Some(link_output),
			..SecurityPreflightInput::default()
		})
		.expect_err("symlink output");
	assert!(
		error.to_string().contains("must not be a symbolic link"),
		"unexpected: {error}"
	);

	let busy = root.path().join("busy-output");
	fs::create_dir_all(&busy).expect("busy output");
	fs::write(busy.join("stale.json"), "{}").expect("busy file");
	let error = coordinator
		.preflight(SecurityPreflightInput {
			output_root: Some(busy),
			..SecurityPreflightInput::default()
		})
		.expect_err("non-empty output");
	assert!(
		error.to_string().contains("is not empty"),
		"unexpected: {error}"
	);
}

// ---------------------------------------------------------------------------
// Exact OAuth pin + idempotent planning
// ---------------------------------------------------------------------------

#[tokio::test]
async fn preflight_pins_exact_oauth_row_and_is_deterministic() {
	let root = TempRoot::new("preflight-pin");
	let repo = git_fixture(root.path());
	let credential_id = seed_oauth(&root.path().join("agent.db"), 7);
	let host = TestHost::new(&repo, root.path(), Some(model()), Behavior::Partial);
	let (coordinator, _tool) = tool(host.clone());
	let output = root.path().join("security-output");

	let first = coordinator
		.preflight(preflight_input(&output))
		.expect("first preflight");
	let second = coordinator
		.preflight(preflight_input(&output))
		.expect("second preflight");

	assert_eq!(first.id, second.id);
	assert_eq!(first.fingerprint, second.fingerprint);
	assert_eq!(first.account.provider, "openai-codex");
	assert_eq!(first.account.credential_id, credential_id);
	assert_eq!(first.model.model_id, "gpt-5-codex");
	assert!(first.fingerprint.starts_with("omp-security-plan/v1:sha256:"));
	assert!(first.config_fingerprint.starts_with("omp-security-config/v1:sha256:"));

	let state = SecurityStore::open(&repo, Some(&host.state_root().expect("state root"))).expect("store");
	assert!(state.get_plan(&first.id).expect("read plan").is_some());

	let model_less = TestHost::new(&repo, root.path(), None, Behavior::Partial);
	let error = SecurityCoordinator::new(model_less)
		.preflight(SecurityPreflightInput::default())
		.expect_err("missing active model");
	assert!(
		error
			.to_string()
			.contains("Security scan preflight requires an active model"),
		"unexpected: {error}"
	);
}

// ---------------------------------------------------------------------------
// Unknown plan + stale plan
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_rejects_unknown_plan_and_stale_plan() {
	let root = TempRoot::new("stale");
	let repo = git_fixture(root.path());
	let host = TestHost::new(&repo, root.path(), Some(model()), Behavior::Publish(publish_params()));
	let (coordinator, _tool) = tool(host);
	let output = root.path().join("security-output");

	let error = coordinator
		.start("secplan_deadbeef")
		.expect_err("unknown plan");
	assert!(
		error.to_string().contains("Unknown security scan plan"),
		"unexpected: {error}"
	);

	let plan = coordinator
		.preflight(preflight_input(&output))
		.expect("preflight");
	fs::write(repo.join("src/lib.rs"), "pub fn drifted() {}\n").expect("drift the tree");
	let error = coordinator.start(&plan.id).expect_err("stale plan");
	assert!(
		error.to_string().contains("Security scan plan is stale"),
		"unexpected: {error}"
	);
	assert!(error.to_string().contains("expected "), "unexpected: {error}");
}

// ---------------------------------------------------------------------------
// Publication end to end
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_scan_completes_store_output_sarif_and_redacts_identity() {
	let root = TempRoot::new("publish");
	let repo = git_fixture(root.path());
	let host = TestHost::new(
		&repo,
		root.path(),
		Some(model()),
		Behavior::Publish(publish_params()),
	);
	let state_root = host.state_root().expect("state root");
	let (coordinator, tool) = tool(host.clone());
	let output = root.path().join("security-output");

	let plan = coordinator
		.preflight(preflight_input(&output))
		.expect("preflight");
	let (text, details) = call(&tool, json!({"action": "start", "plan_id": plan.id}), &repo)
		.await
		.expect("start");
	assert!(text.contains("started as"), "unexpected: {text}");
	let operation_id = details["operation"]["operationId"]
		.as_str()
		.expect("operation id")
		.to_string();
	let scan_id = details["operation"]["scanId"]
		.as_str()
		.expect("scan id")
		.to_string();

	let snapshot = coordinator.wait(&operation_id).expect("wait");
	assert_eq!(snapshot.phase, "completed", "{snapshot:?}");
	assert_eq!(snapshot.finding_count, 1);

	let (text, details) = call(&tool, json!({"action": "status", "operation_id": operation_id}), &repo)
		.await
		.expect("status");
	assert_eq!(text, format!("Security scan {scan_id}: completed; 1 finding(s)."));
	assert_eq!(details["operation"]["phase"], "completed");

	let store = SecurityStore::open(&repo, Some(&state_root)).expect("store");
	let bundle = store
		.get_bundle(&scan_id)
		.expect("read bundle")
		.expect("bundle exists");
	assert_eq!(bundle.scan.status, "completed");
	assert_eq!(bundle.findings.len(), 1);
	assert_eq!(bundle.scan.finding_ids, vec![bundle.findings[0].id.clone()]);
	let metrics = bundle.scan.metrics.as_ref().expect("metrics");
	assert_eq!(metrics.cost, Some(0.5));
	assert_eq!(metrics.premium_requests, Some(1));
	assert!(bundle.report.as_deref().is_some_and(|report| report.contains("Security report")));

	for name in ["findings.json", "report.md", "results.sarif", "provenance.json", "scan.json"] {
		assert!(output.join(name).is_file(), "missing output artifact {name}");
	}
	let sarif: Value =
		serde_json::from_str(&fs::read_to_string(output.join("results.sarif")).expect("sarif"))
			.expect("sarif json");
	assert_eq!(sarif["version"], "2.1.0");
	assert_eq!(sarif["runs"][0]["results"].as_array().expect("results").len(), 1);
	assert_eq!(sarif["runs"][0]["results"][0]["fingerprints"]["omp-security/v1"], bundle.findings[0].fingerprint);

	let provenance = fs::read_to_string(output.join("provenance.json")).expect("provenance");
	assert!(!provenance.contains("user1@example.test"), "{provenance}");
	assert!(!provenance.contains("credentialId"), "{provenance}");
	assert!(!provenance.contains("access-1"), "{provenance}");
	let public_scan = fs::read_to_string(output.join("scan.json")).expect("scan.json");
	assert!(!public_scan.contains("user1@example.test"), "{public_scan}");
	assert!(!public_scan.contains("access-1"), "{public_scan}");
}

// ---------------------------------------------------------------------------
// Once-only publication + scope enforcement + reopen after failure
// ---------------------------------------------------------------------------

#[tokio::test]
async fn security_publish_is_once_only_and_reopens_after_failure() {
	let root = TempRoot::new("once");
	let repo = git_fixture(root.path());
	let host = TestHost::new(
		&repo,
		root.path(),
		Some(model()),
		Behavior::PublishTwice(publish_params()),
	);
	let state_root = host.state_root().expect("state root");
	let (coordinator, _tool) = tool(host);
	let output = root.path().join("security-output");

	let plan = coordinator
		.preflight(SecurityPreflightInput {
			target: SecurityTargetRequest::ScopedPath {
				include_paths: vec!["src".to_string()],
				exclude_paths: Vec::new(),
			},
			output_root: Some(output.clone()),
			..SecurityPreflightInput::default()
		})
		.expect("preflight");
	let operation = coordinator.start(&plan.id).expect("start");
	let snapshot = coordinator.wait(&operation.operation_id).expect("wait");
	assert_eq!(snapshot.phase, "completed", "{snapshot:?}");

	let store = Arc::new(SecurityStore::open(&repo, Some(&state_root)).expect("store"));
	let publisher = SecurityPublisher::new(SecurityPublisherOptions {
		plan: plan.clone(),
		scan_id: "secscan_reopenchain".to_string(),
		store,
		started_at: iso_now(),
		session_id: Some("security:secscan_reopenchain".to_string()),
		operation_id: Some("secop_reopenchain".to_string()),
		on_published: None,
	});

	let mut out_of_scope = publish_params();
	out_of_scope["findings"][0]["locations"][0]["path"] = json!("README.md");
	let error = publisher.publish(&out_of_scope).expect_err("scope violation");
	assert!(
		error.to_string().contains("outside the immutable scan scope"),
		"unexpected: {error}"
	);
	assert!(publisher.published_bundle().is_none());

	let mut escaping = publish_params();
	escaping["findings"][0]["locations"][0]["path"] = json!("../escape.rs");
	let error = publisher.publish(&escaping).expect_err("path escape");
	assert!(
		error.to_string().contains("repository-relative"),
		"unexpected: {error}"
	);
	assert!(publisher.published_bundle().is_none());

	let bundle = publisher.publish(&publish_params()).expect("publish after failures");
	assert_eq!(bundle.findings.len(), 1);
	assert!(publisher.published_bundle().is_some());
	let error = publisher
		.publish(&publish_params())
		.expect_err("second publication");
	assert!(
		error.to_string().contains("has already been published"),
		"unexpected: {error}"
	);
}

// ---------------------------------------------------------------------------
// Partial scan: session ended without publishing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn session_without_publish_is_partial() {
	let root = TempRoot::new("partial");
	let repo = git_fixture(root.path());
	let host = TestHost::new(&repo, root.path(), Some(model()), Behavior::Partial);
	let state_root = host.state_root().expect("state root");
	let (coordinator, tool) = tool(host.clone());
	let output = root.path().join("security-output");

	let plan = coordinator
		.preflight(preflight_input(&output))
		.expect("preflight");
	let (_text, details) = call(&tool, json!({"action": "start", "plan_id": plan.id}), &repo)
		.await
		.expect("start");
	let operation_id = details["operation"]["operationId"].as_str().expect("op id");
	let snapshot = coordinator.wait(operation_id).expect("wait");
	assert_eq!(snapshot.phase, "partial", "{snapshot:?}");
	assert_eq!(snapshot.error.as_deref(), Some(PARTIAL_MESSAGE));

	let store = SecurityStore::open(&repo, Some(&state_root)).expect("store");
	let scan = store.get_scan(&snapshot.scan_id).expect("scan").expect("some");
	assert_eq!(scan.status, "partial");
	assert_eq!(scan.error.as_deref(), Some(PARTIAL_MESSAGE));
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_marks_terminal_cancelled_once() {
	let root = TempRoot::new("cancel");
	let repo = git_fixture(root.path());
	let host = TestHost::new(&repo, root.path(), Some(model()), Behavior::WaitCancel);
	let (coordinator, tool) = tool(host);
	let output = root.path().join("security-output");

	let plan = coordinator
		.preflight(preflight_input(&output))
		.expect("preflight");
	let (_text, details) = call(&tool, json!({"action": "start", "plan_id": plan.id}), &repo)
		.await
		.expect("start");
	let operation_id = details["operation"]["operationId"]
		.as_str()
		.expect("op id")
		.to_string();
	let scan_id = details["operation"]["scanId"]
		.as_str()
		.expect("scan id")
		.to_string();

	let (text, details) = call(
		&tool,
		json!({"action": "cancel", "operation_id": operation_id}),
		&repo,
	)
	.await
	.expect("cancel");
	assert_eq!(text, format!("Cancellation requested for {operation_id}."));
	assert_eq!(details["cancelled"], true);

	let snapshot = coordinator.wait(&operation_id).expect("wait");
	assert_eq!(snapshot.phase, "cancelled", "{snapshot:?}");

	let (text, details) = call(
		&tool,
		json!({"action": "cancel", "operation_id": operation_id}),
		&repo,
	)
	.await
	.expect("second cancel");
	assert_eq!(text, format!("No running operation {operation_id}."));
	assert_eq!(details["cancelled"], false);
	assert_eq!(details["operation"]["scanId"], scan_id);

	let error = call(
		&tool,
		json!({"action": "status", "operation_id": "secop_unknown"}),
		&repo,
	)
	.await
	.expect_err("unknown operation");
	assert!(error.contains("Unknown security operation"), "unexpected: {error}");
}

// ---------------------------------------------------------------------------
// Restart recovery + commit-marker ordering
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restart_marks_running_scan_failed_and_keeps_marker_order() {
	let root = TempRoot::new("restart");
	let repo = git_fixture(root.path());
	let host = TestHost::new(
		&repo,
		root.path(),
		Some(model()),
		Behavior::Publish(publish_params()),
	);
	let state_root = host.state_root().expect("state root");
	let (coordinator, _tool) = tool(host.clone());
	let output = root.path().join("security-output");

	let plan = coordinator
		.preflight(preflight_input(&output))
		.expect("preflight");
	let operation = coordinator.start(&plan.id).expect("start");
	let snapshot = coordinator.wait(&operation.operation_id).expect("wait");
	assert_eq!(snapshot.phase, "completed", "{snapshot:?}");

	let store = SecurityStore::open(&repo, Some(&state_root)).expect("store");
	let mut bundle = store
		.get_bundle(&snapshot.scan_id)
		.expect("read")
		.expect("bundle");
	bundle.scan.status = "running".to_string();
	bundle.scan.completed_at = None;
	bundle.scan.error = None;
	store.put_bundle(&bundle).expect("seed running bundle");

	// A fresh coordinator has no in-memory operation record: restart recovery
	// must mark the persisted running scan failed.
	let restarted = SecurityCoordinator::new(host.clone());
	let recovered = restarted
		.status(&operation.operation_id)
		.expect("status")
		.expect("recovered operation");
	assert_eq!(recovered.phase, "failed");
	assert_eq!(recovered.error.as_deref(), Some(INTERRUPTED_MESSAGE));
	assert_eq!(recovered.scan_id, snapshot.scan_id);
	let scan = store
		.get_scan(&snapshot.scan_id)
		.expect("scan")
		.expect("some");
	assert_eq!(scan.status, "failed");
	assert_eq!(scan.error.as_deref(), Some(INTERRUPTED_MESSAGE));

	// `scan.json` is the commit marker: artifacts without it are not a scan.
	let orphan = store
		.project_directory()
		.join("scans")
		.join("secscan_orphanmarker");
	fs::create_dir_all(&orphan).expect("orphan dir");
	fs::write(orphan.join("findings.json"), "[]\n").expect("orphan findings");
	fs::write(orphan.join("report.md"), "# incomplete\n").expect("orphan report");
	assert_eq!(
		store.get_scan("secscan_orphanmarker").expect("scan lookup"),
		None
	);
	assert_eq!(
		store.get_bundle("secscan_orphanmarker").expect("bundle lookup"),
		None
	);
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn validate_updates_finding_and_rejects_unknown_ids() {
	let root = TempRoot::new("validate");
	let repo = git_fixture(root.path());
	let host = TestHost::new(
		&repo,
		root.path(),
		Some(model()),
		Behavior::Publish(publish_params()),
	);
	let state_root = host.state_root().expect("state root");
	let (coordinator, tool) = tool(host);
	let output = root.path().join("security-output");

	let plan = coordinator
		.preflight(preflight_input(&output))
		.expect("preflight");
	let operation = coordinator.start(&plan.id).expect("start");
	let snapshot = coordinator.wait(&operation.operation_id).expect("wait");
	assert_eq!(snapshot.phase, "completed", "{snapshot:?}");
	let store = SecurityStore::open(&repo, Some(&state_root)).expect("store");
	let bundle = store
		.get_bundle(&snapshot.scan_id)
		.expect("read")
		.expect("bundle");
	let finding_id = bundle.findings[0].id.clone();

	let error = call(
		&tool,
		json!({
			"action": "validate",
			"scan_id": snapshot.scan_id,
			"finding_id": finding_id,
			"validation_status": "validated"
		}),
		&repo,
	)
	.await
	.expect_err("blank summary");
	assert!(error.contains("validation_summary"), "unexpected: {error}");

	let before = store
		.get_finding(&snapshot.scan_id, &finding_id)
		.expect("read finding")
		.expect("finding");
	call(
		&tool,
		json!({
			"action": "validate",
			"scan_id": "secscan_unknownscan",
			"finding_id": finding_id,
			"validation_status": "validated",
			"validation_summary": "checked"
		}),
		&repo,
	)
	.await
	.expect_err("unknown scan id");
	let after = store
		.get_finding(&snapshot.scan_id, &finding_id)
		.expect("read finding")
		.expect("finding");
	assert_eq!(
		after.validation, before.validation,
		"a rejected validate must not mutate the finding"
	);

	let error = call(
		&tool,
		json!({
			"action": "validate",
			"scan_id": snapshot.scan_id,
			"finding_id": "secf_unknownfinding",
			"validation_status": "validated",
			"validation_summary": "checked"
		}),
		&repo,
	)
	.await
	.expect_err("unknown finding");
	assert!(error.contains("Unknown security finding"), "unexpected: {error}");

	let (text, details) = call(
		&tool,
		json!({
			"action": "validate",
			"scan_id": snapshot.scan_id,
			"finding_id": finding_id,
			"validation_status": "validated",
			"validation_summary": "Reproduced with a local harness.",
			"validation_evidence": [{"label": "repro", "explanation": "crash reproduced"}]
		}),
		&repo,
	)
	.await
	.expect("validate");
	assert!(text.contains("validation is now validated"), "unexpected: {text}");
	assert_eq!(details["finding"]["validationStatus"], "validated");

	let updated = store
		.get_finding(&snapshot.scan_id, &finding_id)
		.expect("read finding")
		.expect("finding");
	assert_eq!(updated.validation.status, "validated");
	assert_eq!(updated.validation.evidence_ids.len(), 1);
	assert!(updated.validation.summary.is_some());
}

// ---------------------------------------------------------------------------
// Repository root vs subdirectory: one project store
// ---------------------------------------------------------------------------

#[tokio::test]
async fn repository_subdirectory_shares_one_security_store() {
	let root = TempRoot::new("subdir");
	let repo = git_fixture(root.path());
	let host = TestHost::new(
		&repo,
		root.path(),
		Some(model()),
		Behavior::Publish(publish_params()),
	);
	let state_root = host.state_root().expect("state root");
	let (coordinator, _tool) = tool(host.clone());
	let output = root.path().join("security-output");

	let plan = coordinator
		.preflight(preflight_input(&output))
		.expect("preflight");
	let operation = coordinator.start(&plan.id).expect("start");
	let snapshot = coordinator.wait(&operation.operation_id).expect("wait");
	assert_eq!(snapshot.phase, "completed", "{snapshot:?}");

	// The root and a directory inside it resolve to ONE project store.
	let at_root = SecurityStore::open(&repo, Some(&state_root)).expect("root store");
	let in_subdir =
		SecurityStore::open(&repo.join("src"), Some(&state_root)).expect("subdirectory store");
	assert_eq!(at_root.project_key(), in_subdir.project_key());
	assert_eq!(at_root.project_directory(), in_subdir.project_directory());

	// A coordinator started from the subdirectory sees the same operation.
	let subdir_host = TestHost::new(
		&repo.join("src"),
		root.path(),
		Some(model()),
		Behavior::Partial,
	);
	let subdir = SecurityCoordinator::new(subdir_host);
	let recovered = subdir
		.status(&operation.operation_id)
		.expect("status")
		.expect("known operation");
	assert_eq!(recovered.phase, "completed");
	assert_eq!(recovered.scan_id, snapshot.scan_id);

	let bundle = at_root
		.get_bundle(&snapshot.scan_id)
		.expect("read")
		.expect("bundle");
	let finding_id = bundle.findings[0].id.clone();
	let updated = subdir
		.validate(
			&snapshot.scan_id,
			&finding_id,
			"validated",
			"Subdirectory reader reaches the same finding.",
			&[],
		)
		.expect("validate from subdirectory");
	assert_eq!(updated.validation.status, "validated");

	// The read-only namespace resolves that same project from the subdirectory.
	let handler = SecurityUrlHandler::new(true, Some(state_root));
	let context = pi_agent::tools::internal_urls::ResolveContext {
		cwd: repo.join("src"),
		session_file: None,
		local_roots: std::collections::BTreeMap::new(),
		signal: pi_agent::CancellationToken::new(),
	};
	let listing = pi_agent::tools::internal_urls::ProtocolHandler::resolve(
		&handler,
		&InternalUrl::parse("security://scans").expect("parse url"),
		&context,
	)
	.await
	.expect("resolve scans from subdirectory");
	assert!(
		listing.content.contains(&snapshot.scan_id),
		"unexpected: {}",
		listing.content
	);
}

// ---------------------------------------------------------------------------
// Starting from a subdirectory runs the scan at the repository root
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_from_subdirectory_uses_the_repository_root() {
	let root = TempRoot::new("subdir-start");
	let repo = git_fixture(root.path());
	let canonical_repo = fs::canonicalize(&repo).expect("canonical repository");
	let host = TestHost::new(
		&repo.join("src"),
		root.path(),
		Some(model()),
		Behavior::Publish(publish_params()),
	);
	let output = root.path().join("security-output");
	let (coordinator, _tool) = tool(host.clone());

	let plan = coordinator
		.preflight(preflight_input(&output))
		.expect("preflight");
	assert_eq!(
		fs::canonicalize(&plan.repository_root).expect("canonical plan root"),
		canonical_repo,
		"the plan is anchored at the repository root"
	);
	let operation = coordinator.start(&plan.id).expect("start");
	let snapshot = coordinator.wait(&operation.operation_id).expect("wait");
	assert_eq!(snapshot.phase, "completed", "{snapshot:?}");

	let roots = host
		.execution_roots
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone();
	assert_eq!(roots.len(), 1, "exactly one session was created");
	assert_eq!(
		fs::canonicalize(&roots[0]).expect("canonical execution root"),
		canonical_repo,
		"the restricted session runs at the repository root, not the caller cwd"
	);

	let findings = fs::read_to_string(output.join("findings.json")).expect("findings");
	assert!(
		findings.contains("\"path\": \"src/lib.rs\""),
		"evidence paths stay repository-relative: {findings}"
	);
}

// ---------------------------------------------------------------------------
// `security://` namespace
// ---------------------------------------------------------------------------

#[tokio::test]
async fn security_url_lists_scans_and_respects_disabled_gate() {
	let root = TempRoot::new("resource");
	let repo = git_fixture(root.path());
	let host = TestHost::new(
		&repo,
		root.path(),
		Some(model()),
		Behavior::Publish(publish_params()),
	);
	let state_root = host.state_root().expect("state root");
	let (coordinator, _tool) = tool(host.clone());
	let output = root.path().join("security-output");

	let plan = coordinator
		.preflight(preflight_input(&output))
		.expect("preflight");
	let operation = coordinator.start(&plan.id).expect("start");
	let snapshot = coordinator.wait(&operation.operation_id).expect("wait");
	assert_eq!(snapshot.phase, "completed", "{snapshot:?}");

	let handler = SecurityUrlHandler::new(true, Some(state_root.clone()));
	let context = pi_agent::tools::internal_urls::ResolveContext {
		cwd: repo.clone(),
		session_file: None,
		local_roots: std::collections::BTreeMap::new(),
		signal: pi_agent::CancellationToken::new(),
	};
	let listing = pi_agent::tools::internal_urls::ProtocolHandler::resolve(
		&handler,
		&InternalUrl::parse("security://scans").expect("parse url"),
		&context,
	)
	.await
	.expect("resolve scans");
	assert!(
		listing.content.contains(&snapshot.scan_id),
		"unexpected: {}",
		listing.content
	);
	assert!(listing.content.contains("completed"));

	let finding_id = {
		let store = SecurityStore::open(&repo, Some(&state_root)).expect("store");
		store
			.get_bundle(&snapshot.scan_id)
			.expect("read")
			.expect("bundle")
			.findings[0]
			.id
			.clone()
	};
	let finding = pi_agent::tools::internal_urls::ProtocolHandler::resolve(
		&handler,
		&InternalUrl::parse(&format!("security://scans/{}/findings/{finding_id}", snapshot.scan_id))
			.expect("parse finding url"),
		&context,
	)
	.await
	.expect("resolve finding");
	assert!(finding.content.contains(&finding_id));
	assert!(finding.content.contains("Unsafe dereference"), "{}", finding.content);
	assert!(finding.content.contains("src/lib.rs"));

	let disabled = SecurityUrlHandler::new(false, Some(state_root));
	let error = pi_agent::tools::internal_urls::ProtocolHandler::resolve(
		&disabled,
		&InternalUrl::parse("security://scans").expect("parse url"),
		&context,
	)
	.await
	.expect_err("disabled namespace");
	assert_eq!(error, SECURITY_DISABLED_MESSAGE);
}

// ---------------------------------------------------------------------------
// Exact OAuth selection / identity / no sibling fallback
// ---------------------------------------------------------------------------

#[test]
fn exact_oauth_selector_refuses_ambiguous_and_foreign_accounts() {
	let root = TempRoot::new("auth-pin");
	let db = root.path().join("agent.db");
	seed_oauth(&db, 1);
	seed_oauth(&db, 2);
	let backend = AuthBackend::open_at(&db).expect("backend");

	let error = select_security_account(&backend, "openai-codex", None, None)
		.expect_err("ambiguous accounts must fail");
	assert!(
		error.to_string().contains("Multiple OAuth accounts"),
		"unexpected: {error}"
	);

	// This backend records no session→credential pin: a session id must not pick
	// an account, so the ambiguity error stands with one supplied.
	let error = select_security_account(
		&backend,
		"openai-codex",
		None,
		Some("security-contract-session"),
	)
	.expect_err("a session id is not a credential pin");
	assert!(
		error.to_string().contains("Multiple OAuth accounts"),
		"unexpected: {error}"
	);

	// An explicit credential id still pins even with a session id present.
	let pinned = select_security_account(
		&backend,
		"openai-codex",
		Some(2),
		Some("security-contract-session"),
	)
	.expect("explicit credential id pins");
	assert_eq!(pinned.credential_id, 2);

	let second = select_security_account(&backend, "openai-codex", Some(2), None)
		.expect("explicit credential id");
	assert_eq!(second.credential_id, 2);
	assert_eq!(second.email.as_deref(), Some("user2@example.test"));

	let error = select_security_account(&backend, "openai-codex", Some(9999), None)
		.expect_err("unknown credential id");
	assert!(error.to_string().contains("is not available"), "unexpected: {error}");

	let error = select_security_account(&backend, "anthropic", None, None)
		.expect_err("no stored oauth row");
	assert!(
		error.to_string().contains("require a stored OAuth account"),
		"unexpected: {error}"
	);

	// Identity drift on the pinned row is rejected by name.
	let mut drifted = second.clone();
	drifted.email = Some("someone-else@example.test".to_string());
	let error = resolve_security_oauth_access(&backend, &drifted, false, &Cancel::new())
		.expect_err("identity mismatch");
	assert!(
		error
			.to_string()
			.contains("Security scan authentication identity mismatch"),
		"unexpected: {error}"
	);

	let resolver = ExactSecurityOAuthResolver::new(
		Arc::new(Mutex::new(backend)),
		second.clone(),
	);
	assert_eq!(resolver.last_chance(), None);
	let error = resolver
		.api_key_for("anthropic", false, &Cancel::new())
		.expect_err("provider mismatch");
	assert!(
		error.to_string().contains("provider mismatch"),
		"unexpected: {error}"
	);
	let token = resolver
		.api_key_for("openai-codex", false, &Cancel::new())
		.expect("pinned access token")
		.expect("some token");
	assert_eq!(token, "access-2");
}
