//! Scan planning, asynchronous execution, recovery, and validation.
//!
//! Anchors: `packages/coding-agent/src/security/coordinator.ts`.
//!
//! Five public actions: `preflight`, `start`, `status`, `cancel`, `validate`.
//! `start` returns a queued operation immediately and runs the restricted scan
//! on a dedicated thread. Terminal phases are `completed`, `partial`,
//! `cancelled`, `failed`; `partial` means the real model session ended without
//! calling `security_publish`. Restart recovery marks `running|planned` scans
//! failed and removes the worktree each operation owned.

use std::{
	collections::HashMap,
	fs,
	path::{Path, PathBuf},
	sync::{
		Arc, LazyLock, Mutex,
		atomic::{AtomicBool, Ordering},
	},
	thread::JoinHandle,
};

use super::{
	host::{exact_oauth_resolver, SecurityHost, SecurityScanRequest, SecurityScanStats, SecurityScanSession},
	ids::{create_security_operation_id, create_security_scan_id},
	preflight::{
		assert_security_scan_plan_fresh, create_security_scan_plan, prepare_security_output_directory,
		security_workflow_fingerprint, NativeSecurityGitAdapter, SecurityGitAdapter, SecurityPlanRequest,
		SecurityTargetRequest,
	},
	provenance::create_native_security_provenance,
	publication::{SecurityPublishTool, SecurityPublisher, SecurityPublisherOptions},
	store::{ensure_private_directory, write_security_bundle_to_directory, SecurityStore},
	types::{
		SecurityAccountRef, SecurityCoverage, SecurityCoverageDeferred, SecurityModelRef, SecurityScan,
		SecurityScanBundle, SecurityScanMetrics, SecurityScanPlan, SecurityTokenUsage,
	},
};

/// Restricted scan-session tools from TS `SECURITY_SESSION_TOOLS`.
pub const SECURITY_SESSION_TOOLS: [&str; 7] =
	["read", "grep", "glob", "lsp", "ast_grep", "task", "security_publish"];

pub const SECURITY_DISABLED_PREFLIGHT: &str =
	"Security is disabled; enable security.enabled before planning a scan";
pub const SECURITY_DISABLED_START: &str =
	"Security is disabled; enable security.enabled before starting a scan";
pub const INTERRUPTED_MESSAGE: &str = "Security scan was interrupted by a process restart";
pub const PARTIAL_MESSAGE: &str = "The scan session ended without publishing a canonical result";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityOperationSnapshot {
	pub operation_id: String,
	pub plan_id: String,
	pub scan_id: String,
	pub phase: String,
	pub created_at: String,
	pub updated_at: String,
	pub job_id: Option<String>,
	pub session_file: Option<PathBuf>,
	pub finding_count: usize,
	pub error: Option<String>,
}

pub struct SecurityPreflightInput {
	pub target: SecurityTargetRequest,
	pub knowledge_base_paths: Vec<String>,
	pub output_root: Option<PathBuf>,
	pub archive_existing: bool,
	pub credential_id: Option<i64>,
	pub model: Option<SecurityModelRef>,
	pub thinking_level: Option<String>,
}

impl Default for SecurityPreflightInput {
	fn default() -> Self {
		Self {
			target: SecurityTargetRequest::Repository {
				include_paths: Vec::new(),
				exclude_paths: Vec::new(),
			},
			knowledge_base_paths: Vec::new(),
			output_root: None,
			archive_existing: false,
			credential_id: None,
			model: None,
			thinking_level: None,
		}
	}
}

struct OperationRecord {
	snapshot: Mutex<SecurityOperationSnapshot>,
	cancel: pi_agent::CancellationToken,
	thread: Mutex<Option<JoinHandle<()>>>,
	finished: AtomicBool,
}

impl OperationRecord {
	fn snapshot(&self) -> SecurityOperationSnapshot {
		self.snapshot
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.clone()
	}

	fn update(&self, phase: &str, error: Option<String>, now: fn() -> String) {
		let mut snapshot = self
			.snapshot
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		snapshot.phase = phase.to_string();
		snapshot.updated_at = now();
		snapshot.error = error;
	}

	fn set_findings(&self, count: usize) {
		let mut snapshot = self
			.snapshot
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		snapshot.finding_count = count;
	}

	fn set_session_file(&self, session_file: Option<PathBuf>) {
		let mut snapshot = self
			.snapshot
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		snapshot.session_file = session_file;
	}
}

fn now_iso() -> String {
	crate::output::iso8601(std::time::SystemTime::now())
}

/// Unique, alphanumeric token for scan/operation ids and default work dirs.
///
/// A UUID is preferred; the fallback is a zero-padded hex clock so an RNG
/// failure can never produce an id the store validators reject.
fn unique_token() -> String {
	crate::auth::pkce::random_uuid_v4().unwrap_or_else(|_| {
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|elapsed| elapsed.as_nanos())
			.unwrap_or(0);
		format!("{nanos:032x}")
	})
}

/// Operations running anywhere in this process (TS
/// `ACTIVE_SECURITY_OPERATIONS`).
///
/// Restart recovery runs per coordinator, but a scan started by another
/// session's coordinator is still live: the set is process-wide so recovery
/// never marks a running scan failed.
static ACTIVE_OPERATIONS: LazyLock<Mutex<std::collections::BTreeSet<String>>> =
	LazyLock::new(|| Mutex::new(std::collections::BTreeSet::new()));

fn mark_active(operation_id: &str) {
	ACTIVE_OPERATIONS
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.insert(operation_id.to_string());
}

fn clear_active(operation_id: &str) {
	ACTIVE_OPERATIONS
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.remove(operation_id);
}

fn is_active(operation_id: &str) -> bool {
	ACTIVE_OPERATIONS
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.contains(operation_id)
}

/// One coordinator per host/session.
pub struct SecurityCoordinator {
	host: Arc<dyn SecurityHost>,
	adapter: Arc<dyn SecurityGitAdapter>,
	operations: Mutex<HashMap<String, Arc<OperationRecord>>>,
	recovered: Mutex<bool>,
}

impl SecurityCoordinator {
	pub fn new(host: Arc<dyn SecurityHost>) -> Self {
		Self {
			host,
			adapter: Arc::new(NativeSecurityGitAdapter),
			operations: Mutex::new(HashMap::new()),
			recovered: Mutex::new(false),
		}
	}

	#[must_use]
	pub fn with_adapter(host: Arc<dyn SecurityHost>, adapter: Arc<dyn SecurityGitAdapter>) -> Self {
		let mut coordinator = Self::new(host);
		coordinator.adapter = adapter;
		coordinator
	}

	/// Test seam: deterministic operation ids and clock.
	pub fn operations(&self) -> Vec<SecurityOperationSnapshot> {
		let mut snapshots: Vec<SecurityOperationSnapshot> = self
			.operations
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.values()
			.map(|record| record.snapshot())
			.collect();
		snapshots.sort_by(|left, right| right.created_at.cmp(&left.created_at));
		snapshots
	}

	fn store(&self) -> crate::Result<SecurityStore> {
		SecurityStore::open(&self.host.cwd(), self.host.state_root().as_deref())
	}

	fn ensure_recovered(&self) -> crate::Result<()> {
		let mut recovered = self
			.recovered
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		if *recovered {
			return Ok(());
		}
		self.recover_interrupted()?;
		*recovered = true;
		Ok(())
	}

	fn recover_interrupted(&self) -> crate::Result<()> {
		let store = self.store()?;
		for summary in store.list_scans()? {
			let Some(mut bundle) = store.get_bundle(&summary.id)? else {
				continue;
			};
			let operation_id = bundle
				.scan
				.provenance
				.metadata
				.as_ref()
				.and_then(|metadata| metadata.get("operationId"))
				.and_then(serde_json::Value::as_str)
				.map(str::to_string);
			let Some(operation_id) = operation_id else { continue };
			let operations = self
				.operations
				.lock()
				.unwrap_or_else(std::sync::PoisonError::into_inner);
			if operations.contains_key(&operation_id) {
				continue;
			}
			drop(operations);
			if is_active(&operation_id) {
				continue;
			}
			if bundle.scan.status == "running" || bundle.scan.status == "planned" {
				bundle.scan.status = "failed".to_string();
				bundle.scan.completed_at = Some(now_iso());
				bundle.scan.error = Some(INTERRUPTED_MESSAGE.to_string());
				store.put_bundle(&bundle)?;
				if bundle.scan.target.kind == "ref_diff" {
					let target_path = store.targets_directory().join(&bundle.scan.id);
					let repo_root = Path::new(&bundle.scan.target.repository_root);
					let _ = self.adapter.worktree_remove(repo_root, &target_path);
					if target_path.exists() {
						let _ = fs::remove_dir_all(&target_path);
					}
				}
			}
			let phase = operation_phase_from_status(&bundle.scan.status);
			let snapshot = SecurityOperationSnapshot {
				operation_id: operation_id.clone(),
				plan_id: bundle.scan.plan.as_ref().map(|plan| plan.id.clone()).unwrap_or_default(),
				scan_id: bundle.scan.id.clone(),
				phase: phase.to_string(),
				created_at: bundle.scan.created_at.clone(),
				updated_at: bundle
					.scan
					.completed_at
					.clone()
					.or_else(|| bundle.scan.started_at.clone())
					.unwrap_or_else(|| bundle.scan.created_at.clone()),
				job_id: None,
				session_file: None,
				finding_count: bundle.findings.len(),
				error: bundle.scan.error.clone(),
			};
			self.operations
				.lock()
				.unwrap_or_else(std::sync::PoisonError::into_inner)
				.insert(
					operation_id,
					Arc::new(OperationRecord {
						snapshot: Mutex::new(snapshot),
						cancel: pi_agent::CancellationToken::new(),
						thread: Mutex::new(None),
						finished: AtomicBool::new(true),
					}),
				);
		}
		Ok(())
	}

	pub fn preflight(&self, input: SecurityPreflightInput) -> crate::Result<SecurityScanPlan> {
		if !self.host.security_enabled() {
			return Err(crate::Error::new(SECURITY_DISABLED_PREFLIGHT));
		}
		let model = input.model.or_else(|| self.host.active_model()).ok_or_else(|| {
			crate::Error::new("Security scan preflight requires an active model")
		})?;
		let store = self.store()?;
		let backend = crate::auth::AuthBackend::open_at(self.host.auth_db())
			.map_err(|error| crate::Error::new(error.to_string()))?;
		let account = crate::auth::security::select_security_account(
			&backend,
			&model.provider,
			input.credential_id,
			self.host.session_id().as_deref(),
		)
		.map_err(|error| crate::Error::new(error.to_string()))?;
		let work_root = store.work_directory();
		ensure_private_directory(&work_root)?;
		let mut model_ref = model;
		if input.thinking_level.is_some() {
			model_ref.thinking_level = input.thinking_level;
		}
		let output_root = input.output_root.unwrap_or_else(|| work_root.join(unique_token()));
		let workflow = security_workflow_fingerprint();
		let created_at = now_iso();
		let request = SecurityPlanRequest {
			cwd: &self.host.cwd(),
			target: input.target,
			knowledge_base_paths: &input.knowledge_base_paths,
			output_root: &output_root,
			archive_existing: input.archive_existing,
			model: model_ref,
			account: SecurityAccountRef::from(account),
			config: self.host.config_snapshot(),
			workflow_fingerprint: &workflow,
			created_at: &created_at,
		};
		let plan = create_security_scan_plan(&request, self.adapter.as_ref())
			.map_err(crate::Error::new)?;
		store.put_plan(&plan)?;
		Ok(plan)
	}

	pub fn start(&self, plan_id: &str) -> crate::Result<SecurityOperationSnapshot> {
		if !self.host.security_enabled() {
			return Err(crate::Error::new(SECURITY_DISABLED_START));
		}
		self.ensure_recovered()?;
		let store = Arc::new(self.store()?);
		let plan = store
			.get_plan(plan_id)?
			.ok_or_else(|| crate::Error::new(format!("Unknown security scan plan: {plan_id}")))?;
		let workflow = security_workflow_fingerprint();
		assert_security_scan_plan_fresh(
			&plan,
			&self.host.config_snapshot(),
			&workflow,
			self.adapter.as_ref(),
		)
		.map_err(|error| crate::Error::new(error.to_string()))?;
		let operation_id = create_security_operation_id(&unique_token());
		let scan_id = create_security_scan_id(&unique_token());
		let created_at = now_iso();
		let record = Arc::new(OperationRecord {
			snapshot: Mutex::new(SecurityOperationSnapshot {
				operation_id: operation_id.clone(),
				plan_id: plan.id.clone(),
				scan_id: scan_id.clone(),
				phase: "queued".to_string(),
				created_at: created_at.clone(),
				updated_at: created_at,
				job_id: None,
				session_file: None,
				finding_count: 0,
				error: None,
			}),
			cancel: pi_agent::CancellationToken::new(),
			thread: Mutex::new(None),
			finished: AtomicBool::new(false),
		});
		self.operations
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.insert(operation_id.clone(), Arc::clone(&record));
		mark_active(&operation_id);
		let host = Arc::clone(&self.host);
		let adapter = Arc::clone(&self.adapter);
		let op_record = Arc::clone(&record);
		let store_for_run = Arc::clone(&store);
		let plan_for_run = plan.clone();
		let scan_for_run = scan_id.clone();
		let op_for_run = operation_id.clone();
		let thread = match std::thread::Builder::new()
			.name(format!("security-scan-{scan_id}"))
			.spawn(move || {
				run_operation(
					host,
					adapter,
					store_for_run,
					plan_for_run,
					scan_for_run,
					op_for_run,
					op_record,
				);
			}) {
			Ok(thread) => thread,
			Err(error) => {
				self.operations
					.lock()
					.unwrap_or_else(std::sync::PoisonError::into_inner)
					.remove(&operation_id);
				clear_active(&operation_id);
				return Err(crate::Error::new(format!(
					"cannot start security scan thread: {error}"
				)));
			},
		};
		*record
			.thread
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner) = Some(thread);
		Ok(record.snapshot())
	}

	pub fn status(&self, operation_id: &str) -> crate::Result<Option<SecurityOperationSnapshot>> {
		self.ensure_recovered()?;
		let known = self
			.operations
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.get(operation_id)
			.map(|record| record.snapshot());
		if known.is_some() {
			return Ok(known);
		}
		if is_active(operation_id) {
			return Ok(None);
		}
		// The operation may belong to another session's coordinator, or to a
		// process that has since exited: its bundle is on disk, so rescan before
		// reporting unknown.
		self.recover_interrupted()?;
		Ok(self
			.operations
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.get(operation_id)
			.map(|record| record.snapshot()))
	}

	pub fn cancel(&self, operation_id: &str) -> crate::Result<bool> {
		self.ensure_recovered()?;
		let record = self
			.operations
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.get(operation_id)
			.cloned();
		let Some(record) = record else { return Ok(false) };
		if matches!(
			record.snapshot().phase.as_str(),
			"completed" | "partial" | "cancelled" | "failed"
		) {
			return Ok(false);
		}
		record.cancel.cancel();
		Ok(true)
	}

	pub fn wait(&self, operation_id: &str) -> crate::Result<SecurityOperationSnapshot> {
		self.ensure_recovered()?;
		let record = self
			.operations
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.get(operation_id)
			.cloned()
			.ok_or_else(|| crate::Error::new(format!("Unknown security operation: {operation_id}")))?;
		loop {
			if record.finished.load(Ordering::SeqCst) {
				break;
			}
			if record.cancel.is_cancelled() {
				// The worker observes the token and finishes shortly; keep joining
				// rather than returning a non-terminal snapshot.
			}
			std::thread::sleep(std::time::Duration::from_millis(10));
		}
		let mut thread = record
			.thread
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		if let Some(handle) = thread.take() {
			let _ = handle.join();
		}
		Ok(record.snapshot())
	}

	pub fn validate(
		&self,
		scan_id: &str,
		finding_id: &str,
		status: &str,
		summary: &str,
		evidence: &[super::types::SecurityEvidence],
	) -> crate::Result<super::types::SecurityFinding> {
		let store = self.store()?;
		let finding = store
			.get_finding(scan_id, finding_id)?
			.ok_or_else(|| crate::Error::new(format!("Unknown security finding: {finding_id}")))?;
		let mut evidence = evidence.to_vec();
		let offset = finding.evidence.len();
		for (index, item) in evidence.iter_mut().enumerate() {
			if item.id.is_empty() {
				item.id = super::ids::create_security_evidence_id(
					&finding.fingerprint,
					&format!("validation:{}", item.label),
					offset + index,
				);
				item.kind = "validation".to_string();
			}
		}
		store.update_validation(
			scan_id,
			finding_id,
			super::types::SecurityValidation {
				status: status.to_string(),
				summary: Some(summary.to_string()),
				evidence_ids: evidence.iter().map(|item| item.id.clone()).collect(),
				validated_at: Some(now_iso()),
			},
			&evidence,
		)
	}
}

fn operation_phase_from_status(status: &str) -> &'static str {
	match status {
		"running" | "planned" => "failed",
		"completed" => "completed",
		"partial" => "partial",
		"cancelled" => "cancelled",
		_ => "failed",
	}
}

fn initial_coverage(plan: &SecurityScanPlan) -> SecurityCoverage {
	SecurityCoverage {
		mode: match plan.target.kind.as_str() {
			"ref_diff" => "diff",
			"working_tree" => "working_tree",
			"scoped_path" => "scoped_path",
			"imported" => "imported",
			_ => "repository",
		}
		.to_string(),
		completeness: "unknown".to_string(),
		inventory_strategy: match plan.target.kind.as_str() {
			"ref_diff" => "diff",
			"scoped_path" => "scoped_path",
			_ => "repository",
		}
		.to_string(),
		include_paths: plan.target.include_paths.clone(),
		exclude_paths: plan.target.exclude_paths.clone(),
		surfaces: Vec::new(),
		explicit_exclusions: Vec::new(),
		deferred: vec![SecurityCoverageDeferred {
			id: "scan-pending".to_string(),
			reason: "Security review has not completed".to_string(),
			paths: None,
			surface_ids: None,
		}],
		open_questions: None,
	}
}

fn initial_bundle(
	store: &SecurityStore,
	plan: &SecurityScanPlan,
	scan_id: &str,
	operation_id: &str,
	started_at: &str,
	status: &str,
) -> SecurityScanBundle {
	SecurityScanBundle {
		scan: SecurityScan {
			document_type: "omp-security.scan".to_string(),
			schema_version: "1.0".to_string(),
			id: scan_id.to_string(),
			project_key: store.project_key().to_string(),
			status: status.to_string(),
			created_at: plan.created_at.clone(),
			started_at: Some(started_at.to_string()),
			completed_at: None,
			plan: Some(plan.clone()),
			target: plan.target.clone(),
			producer: super::provenance::create_native_security_producer(),
			provenance: create_native_security_provenance(
				started_at,
				&plan.account,
				&plan.fingerprint,
				&plan.workflow_fingerprint,
				None,
				Some(operation_id),
			),
			finding_ids: Vec::new(),
			coverage: initial_coverage(plan),
			report_ref: None,
			sarif_ref: None,
			error: None,
			metrics: None,
		},
		findings: Vec::new(),
		report: None,
		sarif: None,
	}
}

fn request_text(plan: &SecurityScanPlan, execution_root: &Path, diff_text: Option<&str>) -> String {
	let include = if plan.target.include_paths.is_empty() {
		"all in-scope paths".to_string()
	} else {
		plan.target.include_paths.join(", ")
	};
	let exclude = if plan.target.exclude_paths.is_empty() {
		"none".to_string()
	} else {
		plan.target.exclude_paths.join(", ")
	};
	let knowledge = if plan.knowledge_bases.is_empty() {
		"none".to_string()
	} else {
		plan.knowledge_bases
			.iter()
			.map(|item| item.path.clone())
			.collect::<Vec<_>>()
			.join(", ")
	};
	let mut text = format!(
		"Run the immutable security plan below.\n\nRepository: {}\nTarget kind: {}\nRevision: {}\nBase revision: {}\nHead revision: {}\nInclude paths: {}\nExclude paths: {}\nKnowledge bases: {}\nPlan fingerprint: {}\n",
		execution_root.display(),
		plan.target.kind,
		plan.target.revision.as_deref().unwrap_or(""),
		plan.target.base_revision.as_deref().unwrap_or(""),
		plan.target.head_revision.as_deref().unwrap_or(""),
		include,
		exclude,
		knowledge,
		plan.fingerprint
	);
	if let Some(diff) = diff_text.filter(|diff| !diff.is_empty()) {
		text.push_str("\nRequested base-to-head diff:\n\n```diff\n");
		text.push_str(diff);
		text.push_str("\n```\n");
	}
	text.push_str(
		"\nFirst inventory the exact scope. Delegate disjoint review assignments to `security-reviewer` through `task`. Reconcile all worker output, inspect any evidence needed to resolve uncertainty, then call `security_publish` once with findings, honest coverage, and the final report.",
	);
	text
}

struct PreparedTarget {
	cwd: PathBuf,
	diff_text: Option<String>,
}

fn prepare_execution_target(
	adapter: &dyn SecurityGitAdapter,
	store: &SecurityStore,
	plan: &SecurityScanPlan,
	scan_id: &str,
) -> Result<PreparedTarget, String> {
	if plan.target.kind != "ref_diff" {
		// Every scope path is repository-relative, so the session must run at the
		// planned root even when the caller's cwd is a subdirectory.
		return Ok(PreparedTarget { cwd: PathBuf::from(&plan.repository_root), diff_text: None });
	}
	let head = plan
		.target
		.head_revision
		.clone()
		.ok_or_else(|| "ref_diff security plan is missing resolved revisions".to_string())?;
	let base = plan
		.target
		.base_revision
		.clone()
		.ok_or_else(|| "ref_diff security plan is missing resolved revisions".to_string())?;
	let targets = store.targets_directory();
	ensure_private_directory(&targets).map_err(|error| error.to_string())?;
	let cwd = targets.join(scan_id);
	adapter.worktree_add(Path::new(&plan.repository_root), &cwd, &head)?;
	let diff_text = match adapter.diff_tree(Path::new(&plan.repository_root), &base, &head) {
		Ok(diff) => diff,
		Err(error) => {
			cleanup_target(adapter, plan, &cwd);
			return Err(error);
		},
	};
	Ok(PreparedTarget { cwd, diff_text: Some(diff_text) })
}

fn cleanup_target(adapter: &dyn SecurityGitAdapter, plan: &SecurityScanPlan, cwd: &Path) {
	if plan.target.kind != "ref_diff" {
		return;
	}
	let _ = adapter.worktree_remove(Path::new(&plan.repository_root), cwd);
	if cwd.exists() {
		let _ = fs::remove_dir_all(cwd);
	}
}

fn metrics_from(started_ms: u128, stats: Option<SecurityScanStats>) -> SecurityScanMetrics {
	let runtime_ms = u64::try_from(
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|duration| duration.as_millis())
			.unwrap_or(started_ms)
			.saturating_sub(started_ms),
	)
	.unwrap_or(0);
	let mut metrics = SecurityScanMetrics {
		runtime_ms: Some(runtime_ms),
		token_usage: None,
		cost: None,
		premium_requests: None,
	};
	if let Some(stats) = stats {
		metrics.token_usage = stats.token_usage.map(|usage: SecurityTokenUsage| usage);
		metrics.cost = stats.cost;
		metrics.premium_requests = stats.premium_requests;
	}
	metrics
}

#[allow(clippy::too_many_arguments)]
fn run_operation(
	host: Arc<dyn SecurityHost>,
	adapter: Arc<dyn SecurityGitAdapter>,
	store: Arc<SecurityStore>,
	plan: SecurityScanPlan,
	scan_id: String,
	operation_id: String,
	record: Arc<OperationRecord>,
) {
	let started_at = now_iso();
	let started_ms = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map(|duration| duration.as_millis())
		.unwrap_or(0);
	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		let mut prepared: Option<PreparedTarget> = None;
		let mut session: Option<Arc<dyn SecurityScanSession>> = None;
		let mut publisher: Option<Arc<SecurityPublisher>> = None;
		let mut published: Option<SecurityScanBundle> = None;
		let outcome: crate::Result<()> = (|| {
			store.put_bundle(&initial_bundle(
				&store,
				&plan,
				&scan_id,
				&operation_id,
				&started_at,
				"running",
			))?;
			if record.cancel.is_cancelled() {
				return Err(crate::Error::new("Security scan cancelled"));
			}
			prepare_security_output_directory(&plan.output, &scan_id)
				.map_err(crate::Error::new)?;
			record.update("preparing", None, now_iso);
			host.ensure_model(&plan.model).map_err(crate::Error::new)?;
			let target = prepare_execution_target(adapter.as_ref(), &store, &plan, &scan_id)
			.map_err(crate::Error::new)?;
			let execution_root = target.cwd.clone();
			let diff_text = target.diff_text.clone();
			prepared = Some(target);
			ensure_private_directory(&store.sessions_directory())?;
			let resolver = exact_oauth_resolver(&host.auth_db(), &plan.account)?;
			let record_for_publish = Arc::clone(&record);
			let publisher_instance = Arc::new(SecurityPublisher::new(SecurityPublisherOptions {
				plan: plan.clone(),
				scan_id: scan_id.clone(),
				store: Arc::clone(&store),
				started_at: started_at.clone(),
				session_id: Some(format!("security:{scan_id}")),
				operation_id: Some(operation_id.clone()),
				on_published: Some(Arc::new(move |bundle: &SecurityScanBundle| {
					record_for_publish.set_findings(bundle.findings.len());
					record_for_publish.update("publishing", None, now_iso);
				})),
			}));
			let publisher_tool: Arc<dyn pi_agent::tools::Tool> =
				Arc::new(SecurityPublishTool::new(Arc::clone(&publisher_instance)));
			publisher = Some(Arc::clone(&publisher_instance));
			let request = SecurityScanRequest {
				plan: plan.clone(),
				scan_id: scan_id.clone(),
				execution_root: execution_root.clone(),
				model: plan.model.clone(),
				publication_tool: publisher_tool,
				cancel: record.cancel.clone(),
				resolver,
			};
			let created = host
				.create_scan_session(request)
				.map_err(crate::Error::new)?;
			record.set_session_file(created.session_file());
			if record.cancel.is_cancelled() {
				created.abort();
				return Err(crate::Error::new("Security scan cancelled"));
			}
			session = Some(Arc::clone(&created));
			record.update("reviewing", None, now_iso);
			let stats = created
				.run(&request_text(&plan, &execution_root, diff_text.as_deref()))
				.map_err(crate::Error::new)?;
			record.set_session_file(created.session_file());
			if let Some(bundle) = publisher_instance.published_bundle() {
				published = Some(bundle);
				let mut bundle = published.take().expect("published");
				bundle.scan.metrics = Some(metrics_from(started_ms, stats));
				write_security_bundle_to_directory(Path::new(&plan.output.root), &bundle)?;
				store.put_bundle(&bundle)?;
				published = Some(bundle);
			}
			if record.cancel.is_cancelled() {
				return Err(crate::Error::new("Security scan cancelled"));
			}
			if published.is_some() {
				record.update("completed", None, now_iso);
				return Ok(());
			}
			let mut partial = initial_bundle(
				&store,
				&plan,
				&scan_id,
				&operation_id,
				&started_at,
				"partial",
			);
			partial.scan.completed_at = Some(now_iso());
			partial.scan.error = Some(PARTIAL_MESSAGE.to_string());
			record.update("partial", Some(PARTIAL_MESSAGE.to_string()), now_iso);
			store.put_bundle(&partial)?;
			Ok(())
		})();
		if let Err(error) = outcome {
			if let Some(bundle) = publisher
				.as_ref()
				.and_then(|publisher| publisher.published_bundle())
				.or(published)
			{
				// The canonical bundle is already persisted; a late failure
				// degrades to completed instead of invalidating it.
				record.set_findings(bundle.findings.len());
				record.update("completed", None, now_iso);
			} else {
				let cancelled = record.cancel.is_cancelled();
				let message = error.to_string();
				let mut terminal = initial_bundle(
					&store,
					&plan,
					&scan_id,
					&operation_id,
					&started_at,
					if cancelled { "cancelled" } else { "failed" },
				);
				terminal.scan.completed_at = Some(now_iso());
				terminal.scan.error = Some(message.clone());
				record.update(if cancelled { "cancelled" } else { "failed" }, Some(message), now_iso);
				let _ = store.put_bundle(&terminal);
			}
		}
		if let Some(session) = session {
			session.abort();
		}
		if let Some(target) = prepared {
			cleanup_target(adapter.as_ref(), &plan, &target.cwd);
		}
	})).is_err();
	if result {
		record.update("failed", Some("Security scan worker panicked".to_string()), now_iso);
	}
	// Clear the process-wide mark before publishing `finished`, so a waiter that
	// observes the terminal snapshot can never race restart recovery into
	// treating this operation as still live.
	clear_active(&operation_id);
	record.finished.store(true, Ordering::SeqCst);
}
