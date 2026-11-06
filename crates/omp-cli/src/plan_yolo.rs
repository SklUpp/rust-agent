//! Plan-yolo host core: one-shot plan mode armed before the first prompt, an
//! auto-approving `xd://propose` handler, and a two-phase hand-off that keeps
//! the write guard active until the execution provider was actually built.
//!
//! TS reference: `session/prewalk.ts:252-348`. The host owns tool-set
//! augmentation and provider assignment; this module owns the shared gate and
//! the pending approval so no caller can exit plan mode before a successful
//! switch is guaranteed.

use std::{
	cell::RefCell,
	path::{Path, PathBuf},
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

use pi_agent::{AgentContext, ToolError, ToolResult};
use serde_json::json;

use crate::{
	acp::plan::{self, AcpPlanState, DEFAULT_PLAN_FILE_URL, PlanGate, PlanWorkflow},
	agent::PlanModeHooks,
	args::Cli,
	config::Config,
	runtime::{self, CliThinking, ModelSwitch},
};

/// TS `DEFAULT_PLAN_YOLO_TARGET`.
pub const DEFAULT_TARGET: &str = "@smol";
/// TS `PLAN_YOLO_HANDOFF_MESSAGE_TYPE`.
pub const PLAN_YOLO_HANDOFF_TYPE: &str = "plan-yolo-handoff";

const PLAN_YOLO_HANDOFF_PROMPT: &str = include_str!(
	"../../../packages/coding-agent/src/prompts/system/plan-yolo-handoff.md"
);

/// Mirrors the `write` tool's `MAX_WRITE_BYTES`; inline proposal content must
/// not take a private path around that public tool limit.
const MAX_INLINE_PLAN_BYTES: usize = 1024 * 1024;

/// Defense in depth: the shared reader rejects capped/truncated artifacts
/// before UTF-8 decoding, so approval never sees a prefix. Keep this check so
/// a caller that substitutes a different reader still cannot approve a plan
/// whose reviewed text is at or beyond the bounded read size.
fn ensure_plan_fully_read(path: &str, content: &str) -> Result<(), String> {
	if content.len() > pi_agent::tools::hashline::snapshot::SNAPSHOT_MAX_BYTES {
		return Err(format!(
			"Plan file {path} exceeds the approval read limit ({} bytes); shorten it before proposing.",
			pi_agent::tools::hashline::snapshot::SNAPSHOT_MAX_BYTES
		));
	}
	Ok(())
}

fn ensure_inline_plan_size(content: &str) -> Result<(), String> {
	if content.len() > MAX_INLINE_PLAN_BYTES {
		return Err(format!(
			"Inline plan content is {} bytes; the write limit is {MAX_INLINE_PLAN_BYTES} bytes.",
			content.len()
		));
	}
	Ok(())
}

/// One resolved plan-yolo arm. Credentials and the endpoint are re-derived when
/// the switch actually fires, exactly like prewalk.
#[derive(Debug, Clone)]
pub struct PlanYoloArm {
	pub target: ModelSwitch,
	/// Selector the target's `:suffix` spelled, if any.
	pub thinking: Option<CliThinking>,
}

/// Resolve the explicit startup flags. Unlike prewalk, a missing target or
/// missing configured auth is a hard startup error (TS `main.ts:1267-1283`).
pub fn resolve_arm(
	cli: &Cli,
	config: &Config,
	catalog: &crate::catalog::Catalog,
	explicit_api_key: Option<&str>,
) -> Result<Option<PlanYoloArm>, String> {
	if !cli.plan_yolo {
		if cli.plan_yolo_into.is_some() {
			return Err("--plan-yolo-into requires --plan-yolo".to_string());
		}
		return Ok(None);
	}
	if cli.no_tools {
		return Err(
			"--plan-yolo requires tools; --no-tools cannot arm an unapprovable plan"
				.to_string(),
		);
	}
	if cli.no_session {
		return Err(
			"--plan-yolo requires a persisted session for plan artifacts"
				.to_string(),
		);
	}
	let requested = cli.plan_yolo_into.as_deref().unwrap_or(DEFAULT_TARGET);
	let target = runtime::resolve_model_switch(catalog, config, requested, None)
		.map_err(|error| format!("plan-yolo target: {error}"))?;
	// `--api-key` belongs to the root route; reuse it only for that provider.
	let api_key_override = explicit_api_key.filter(|_| {
		cli.provider
			.as_deref()
			.is_some_and(|provider| provider == target.provider.as_str())
	});
	if !runtime::provider_has_configured_auth(
		catalog,
		config,
		&target.provider,
		api_key_override,
	) {
		return Err(format!(
			"plan-yolo target {}/{} has no configured API key",
			target.provider, target.model
		));
	}
	let thinking = target.thinking;
	Ok(Some(PlanYoloArm { target, thinking }))
}

/// Snapshot taken before plan mode is published. The host rolls this back when
/// applying the augmented tool set fails; a failed arm must not leave the
/// session permanently in plan mode.
#[derive(Debug)]
pub struct PlanYoloArming {
	previous: PlanGate,
}

/// Shared one-shot state. Hosts keep one `Arc<PlanYoloShared>`; the write guard,
/// proposal handler and reminder policy all read the same gate.
#[derive(Debug)]
pub struct PlanYoloShared {
	gate: Arc<Mutex<PlanGate>>,
	arm: PlanYoloArm,
	pending: Mutex<Option<PlanYoloPending>>,
	armed: AtomicBool,
	completed: AtomicBool,
	abandoned: AtomicBool,
	/// Gate generation published by [`PlanYoloShared::arm`]. A manual plan
	/// entry after arm/commit changes it, so the stale handler cannot approve.
	arm_generation: AtomicU64,
}

/// A validated plan whose execution switch has not been applied yet. Review
/// content is retained so [`PlanYoloShared::commit_pending`] can prove the
/// artifact did not change between approval and switch.
#[derive(Debug, Clone)]
pub struct PlanYoloPending {
	target: ModelSwitch,
	thinking: Option<CliThinking>,
	plan_file_path: String,
	title: String,
	reviewed_content: String,
}

impl PlanYoloPending {
	#[must_use]
	pub fn target(&self) -> &ModelSwitch {
		&self.target
	}

	#[must_use]
	pub fn thinking(&self) -> Option<CliThinking> {
		self.thinking
	}

	#[must_use]
	pub fn plan_file_path(&self) -> &str {
		&self.plan_file_path
	}

	#[must_use]
	pub fn title(&self) -> &str {
		&self.title
	}
}

/// Host-facing hand-off material returned only after the provider build
/// succeeded and plan mode was exited atomically.
#[derive(Debug, Clone)]
pub struct PlanYoloCommit {
	pub target: ModelSwitch,
	pub thinking: Option<CliThinking>,
	pub handoff: String,
	pub notice: String,
	pub plan_file_path: String,
}

impl PlanYoloShared {
	#[must_use]
	pub fn new(gate: Arc<Mutex<PlanGate>>, arm: PlanYoloArm) -> Self {
		Self {
			gate,
			arm,
			pending: Mutex::new(None),
			armed: AtomicBool::new(false),
			completed: AtomicBool::new(false),
			abandoned: AtomicBool::new(false),
			arm_generation: AtomicU64::new(0),
		}
	}

	#[must_use]
	pub fn gate(&self) -> Arc<Mutex<PlanGate>> {
		Arc::clone(&self.gate)
	}

	#[must_use]
	pub fn is_armed(&self) -> bool {
		self.armed.load(Ordering::Acquire) && !self.completed.load(Ordering::Acquire)
	}

	#[must_use]
	pub fn is_completed(&self) -> bool {
		self.completed.load(Ordering::Acquire)
	}

	#[must_use]
	pub fn is_abandoned(&self) -> bool {
		self.abandoned.load(Ordering::Acquire)
	}

	fn approval_active(&self) -> bool {
		self.is_armed()
	}

	#[must_use]
	pub fn has_pending(&self) -> bool {
		self.pending
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.is_some()
	}

	/// Validate that the armed plan generation still owns plan mode before a
	/// proposal mutates its artifact.
	fn ensure_current_generation(&self) -> Result<(), String> {
		let gate = self
			.gate
			.lock()
			.unwrap_or_else(|poison| poison.into_inner());
		if !gate.state.as_ref().is_some_and(|state| state.enabled)
			|| gate.generation != self.arm_generation.load(Ordering::Acquire)
		{
			return Err("plan-yolo plan mode changed before approval".to_string());
		}
		Ok(())
	}

	/// Publish plan mode before the first prompt. Returns the snapshot to pass
	/// to [`Self::rollback_arm`] if the host's tool-set update fails.
	///
	/// The mode state is published first, matching TS: `SessionTools` must see
	/// plan mode before `write` is added to a restricted selection.
	pub fn arm(&self) -> Option<PlanYoloArming> {
		if self.completed.load(Ordering::Acquire)
			|| self.abandoned.load(Ordering::Acquire)
			|| self.armed.swap(true, Ordering::AcqRel)
		{
			return None;
		}
		let mut gate = self
			.gate
			.lock()
			.unwrap_or_else(|poison| poison.into_inner());
		let previous = gate.clone();
		let plan_file_path = gate
			.plan_reference_for_injection()
			.unwrap_or_else(|| DEFAULT_PLAN_FILE_URL.to_string());
		// TS plan-yolo sets this state directly, independently of
		// `plan.enabled`; the explicit flag is the grant.
		gate.state = Some(AcpPlanState {
			enabled: true,
			plan_file_path,
			workflow: PlanWorkflow::Parallel,
			reentry: previous.state.is_some(),
		});
		gate.settle = plan::PlanSettleState::default();
		gate.discard_queued_continuation();
		gate.generation = gate.generation.wrapping_add(1);
		self.arm_generation.store(gate.generation, Ordering::Release);
		Some(PlanYoloArming { previous })
	}

	/// Restore the pre-arm gate state after a failed tool-set publication.
	pub fn rollback_arm(&self, arming: PlanYoloArming) {
		// Same pending -> gate order as approval/commit, so a failed arm cannot
		// deadlock a concurrent proposal that already holds the pending slot.
		self.pending
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.take();
		*self
			.gate
			.lock()
			.unwrap_or_else(|poison| poison.into_inner()) = arming.previous;
		self.arm_generation.store(0, Ordering::Release);
		self.armed.store(false, Ordering::Release);
	}

	/// Discard a queued approval without touching plan mode. A cancelled or
	/// failed turn must be able to ask for a fresh proposal.
	pub fn cancel_pending(&self) {
		self.pending
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.take();
	}

	/// Abandon an incomplete one-shot arm. It can never autoapprove later, and
	/// the host can safely restore ordinary, non-autoapproving plan hooks.
	pub fn abandon(&self) {
		self.cancel_pending();
		self.armed.store(false, Ordering::Release);
		self.abandoned.store(true, Ordering::Release);
	}

	/// Atomically validate, build and commit the execution switch.
	///
	/// The guard is exited only after `build` succeeds, so an auth, config or
	/// wire-build failure leaves plan mode active with the pending approval
	/// intact. The artifact is re-read through the contained reader and must
	/// byte-match the reviewed version. The caller must not independently exit
	/// the gate; this is the only transition to execution.
	///
	/// `build` runs synchronously with the pending and gate locks held; it must
	/// not call back into this [`PlanYoloShared`]. This is what makes the
	/// build/exit sequence atomic without a speculative intermediate state.
	pub fn commit_pending<T>(
		&self,
		artifacts: Option<&Path>,
		build: impl FnOnce(&PlanYoloPending) -> Result<T, String>,
	) -> Result<(PlanYoloCommit, T), String> {
		let mut pending_slot = self
			.pending
			.lock()
			.unwrap_or_else(|poison| poison.into_inner());
		let Some(pending) = pending_slot.take() else {
			return Err("plan-yolo has no approved plan pending a switch".to_string());
		};
		if !self.approval_active() {
			return Err("plan-yolo is no longer armed for this session".to_string());
		}
		if let Err(error) =
			ensure_plan_fully_read(&pending.plan_file_path, &pending.reviewed_content)
		{
			return Err(error);
		}
		let mut gate = self
			.gate
			.lock()
			.unwrap_or_else(|poison| poison.into_inner());
		if !gate.state.as_ref().is_some_and(|state| state.enabled)
			|| gate.generation != self.arm_generation.load(Ordering::Acquire)
		{
			return Err("plan-yolo plan mode changed before the execution switch".to_string());
		}
		let Some(root) = artifacts else {
			*pending_slot = Some(pending);
			return Err("plan-yolo cannot switch: the session has no artifact directory".to_string());
		};
		let Some(relative) = pi_agent::tools::local_artifact_relative(&pending.plan_file_path)
		else {
			let message = format!(
				"plan-yolo cannot switch: {} is not a local artifact path",
				pending.plan_file_path
			);
			*pending_slot = Some(pending);
			return Err(message);
		};
		let current = match pi_agent::tools::read_local_artifact(root, &relative) {
			Ok(current) => current,
			Err(error) => {
				let message = format!(
					"plan-yolo cannot revalidate {}: {error}",
					pending.plan_file_path
				);
				*pending_slot = Some(pending);
				return Err(message);
			},
		};
		if let Err(error) = ensure_plan_fully_read(&pending.plan_file_path, &current) {
			return Err(error);
		}
		if current != pending.reviewed_content {
			return Err(format!(
				"plan-yolo: {} changed after approval; re-propose the updated plan",
				pending.plan_file_path
			));
		}
		// Build while the gate is still active. `Err` returns with both the
		// pending approval and the guard untouched.
		let built = match build(&pending) {
			Ok(built) => built,
			Err(error) => {
				let message = format!(
					"plan-yolo could not build the execution provider for {}/{}: {error}",
					pending.target.provider, pending.target.model
				);
				*pending_slot = Some(pending);
				return Err(message);
			},
		};
		let commit = PlanYoloCommit {
			target: pending.target.clone(),
			thinking: pending.thinking,
			handoff: render_handoff(&pending.plan_file_path, &pending.title),
			notice: format!(
				"Plan-yolo: plan approved, switched to {}/{} to implement \"{}\".",
				pending.target.provider, pending.target.model, pending.title
			),
			plan_file_path: pending.plan_file_path.clone(),
		};
		gate.exit_after_approval(&pending.plan_file_path);
		*pending_slot = None;
		self.armed.store(false, Ordering::Release);
		self.completed.store(true, Ordering::Release);
		Ok((commit, built))
	}

	/// Queue the reviewed plan for the host's turn-boundary switch. The gate
	/// stays active here; only [`Self::commit_pending`] exits it.
	fn queue_pending(&self, plan: plan::ResolvedPlan) -> Result<(), String> {
		let mut pending = self
			.pending
			.lock()
			.unwrap_or_else(|poison| poison.into_inner());
		if pending.is_some() {
			return Err(
				"plan-yolo approval is already pending the execution switch".to_string(),
			);
		}
		if !self.approval_active() {
			return Err("plan-yolo is no longer armed for this session".to_string());
		}
		let gate = self
			.gate
			.lock()
			.unwrap_or_else(|poison| poison.into_inner());
		if !gate.state.as_ref().is_some_and(|state| state.enabled)
			|| gate.generation != self.arm_generation.load(Ordering::Acquire)
		{
			return Err("plan-yolo plan mode changed before approval".to_string());
		}
		*pending = Some(PlanYoloPending {
			target: self.arm.target.clone(),
			thinking: self.arm.thinking,
			plan_file_path: plan.plan_file_path,
			title: plan.title,
			reviewed_content: plan.plan_content,
		});
		Ok(())
	}
}

/// Host plan-mode wiring. `plan_hooks` reuses the existing guard, reminder
/// policy and proposal trait instead of adding a parallel enforcement path.
#[must_use]
pub fn plan_hooks(
	shared: &Arc<PlanYoloShared>,
	artifacts: Option<PathBuf>,
) -> PlanModeHooks {
	let gate = shared.gate();
	PlanModeHooks {
		guard:   Some(Arc::new(plan::PlanWriteGuard::new(
			Arc::clone(&gate),
			artifacts.clone(),
		))),
		propose: Some(Arc::new(PlanYoloProposalHandler {
			shared:    Arc::clone(shared),
			artifacts,
		})),
		policy:  Some(Arc::new(plan::PlanReminderPolicy::new(gate))),
	}
}

/// Tool plan for the planning turn plus the selection the host must restore
/// after approval or arm failure. Keeping both in one value prevents a host
/// from losing the pre-plan selection when it only calls [`augment_tools`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanYoloToolSelection {
	selected: Option<Vec<String>>,
	previous: Option<Vec<String>>,
}

impl PlanYoloToolSelection {
	#[must_use]
	pub fn selected(&self) -> Option<&[String]> {
		self.selected.as_deref()
	}

	#[must_use]
	pub fn previous(&self) -> Option<&[String]> {
		self.previous.as_deref()
	}

	/// Selection to publish again after the plan turn or a failed arm.
	#[must_use]
	pub fn restore(&self) -> Option<Vec<String>> {
		self.previous.clone()
	}

	/// Tools added only for planning. Hiding this delta at the boundary
	/// restores the previous builtin allowlist without filtering mounted MCP
	/// or host tools.
	#[must_use]
	pub fn hidden_by_plan(&self) -> Vec<String> {
		let Some(previous) = self.previous.as_deref() else {
			return Vec::new();
		};
		self
			.selected
			.as_deref()
			.unwrap_or_default()
			.iter()
			.filter(|name| !previous.contains(*name))
			.cloned()
			.collect()
	}
}

/// Add `write` to a restricted selection only when tools are enabled and the
/// builtin is actually absent. `None` means "all builtins", which already
/// includes `write`; `--no-tools` never gains a transport tool.
#[must_use]
pub fn augment_tools(
	tools_enabled: bool,
	selected: Option<&[String]>,
) -> Option<Vec<String>> {
	let mut tools = selected?.to_vec();
	if !tools_enabled || tools.iter().any(|tool| tool == "write") {
		return Some(tools);
	}
	tools.push("write".to_string());
	Some(tools)
}

/// Host-facing planning-tool handle. Rejects `--no-tools` instead of arming a
/// plan the model cannot write or approve; the caller applies
/// [`PlanYoloToolSelection::selected`] and later restores
/// [`PlanYoloToolSelection::restore`].
pub fn plan_tools(
	tools_enabled: bool,
	selected: Option<&[String]>,
) -> Result<PlanYoloToolSelection, String> {
	if !tools_enabled {
		return Err(
			"plan-yolo requires tools; --no-tools cannot arm an unapprovable plan"
				.to_string(),
		);
	}
	Ok(PlanYoloToolSelection {
		selected: augment_tools(tools_enabled, selected),
		previous: selected.map(|tools| tools.to_vec()),
	})
}

/// Auto-approving proposal handler. It validates and queues, but deliberately
/// never exits plan mode: cancellation or a failed switch must leave the guard
/// in place.
pub struct PlanYoloProposalHandler {
	shared:    Arc<PlanYoloShared>,
	artifacts: Option<PathBuf>,
}

#[async_trait::async_trait]
impl pi_agent::state::ProposeHandler for PlanYoloProposalHandler {
	async fn propose(
		&self,
		body: &str,
		ctx: &AgentContext,
	) -> Result<ToolResult, ToolError> {
		if !self.shared.approval_active() {
			return Err(ToolError::Rejected(
				"Plan-yolo is no longer armed for this session; xd://propose has no auto-approval here."
					.to_string(),
			));
		}
		if self.shared.has_pending() {
			return Err(ToolError::Rejected(
				"Plan approval is already pending the execution switch.".to_string(),
			));
		}
		let state_path = {
			let gate = self
				.shared
				.gate
				.lock()
				.unwrap_or_else(|poison| poison.into_inner());
			match gate.state.as_ref().filter(|state| state.enabled) {
				Some(state) => state.plan_file_path.clone(),
				None => {
					return Err(ToolError::Rejected(
						"No plan is awaiting approval — xd://propose only accepts a plan title while plan mode is active."
							.to_string(),
					));
				},
			}
		};
		let root = self.artifacts.as_deref().ok_or_else(|| {
			ToolError::ExecutionFailed(
				"Plan artifacts need a persisted session directory".to_string(),
			)
		})?;
		let proposal = plan::parse_plan_proposal(body);
		let target_url = proposal
			.slug
			.as_deref()
			.map(plan::plan_file_url_for_slug)
			.unwrap_or(state_path);
		self.shared
			.ensure_current_generation()
			.map_err(ToolError::Rejected)?;
		// `local://` is read-only for the model, so the one inline-content path
		// goes through the same fd-contained artifact writer as plan mode.
		if let Some(content) = &proposal.content {
			ensure_inline_plan_size(content).map_err(ToolError::Rejected)?;
			let relative = pi_agent::tools::local_artifact_relative(&target_url)
				.ok_or_else(|| {
					ToolError::ExecutionFailed(format!(
						"{target_url} is not a local artifact path"
					))
				})?;
			pi_agent::tools::write_artifact(
				root,
				&relative,
				content.as_bytes(),
				&ctx.cancellation_token,
			)
			.await
			.map_err(|error| {
				ToolError::ExecutionFailed(format!(
					"Failed to write {target_url}: {error}"
				))
			})?;
		}
		let resolved = resolve_reviewed_plan(&proposal, &target_url, root)?;
		ensure_plan_fully_read(&resolved.plan_file_path, &resolved.plan_content)
			.map_err(ToolError::Rejected)?;
		self.shared
			.queue_pending(resolved.clone())
			.map_err(ToolError::Rejected)?;
		let details = json!({
			"planFilePath": resolved.plan_file_path,
			"title": resolved.title,
			"planExists": true,
			"executionSwitchPending": true,
		});
		Ok(ToolResult {
			tool_call_id: String::new(),
			content: format!(
				"Plan approved at {}. The execution switch is pending; finish this turn and wait for the mode change before implementing.",
				resolved.plan_file_path
			),
			is_error: false,
			terminate: false,
			details: Some(details),
		})
	}
}

/// Contained `resolveApprovedPlan`: plan content is read through the artifact
/// fd walk, never through the working tree.
fn read_plan_candidate(root: &Path, url: &str) -> Result<Option<String>, String> {
	let Some(relative) = pi_agent::tools::local_artifact_relative(url) else {
		return Ok(None);
	};
	match pi_agent::tools::read_local_artifact(root, &relative) {
		Ok(content) => Ok(Some(content)),
		Err(error) if error.is_truncated() => Err(error.to_string()),
		Err(_) => Ok(None),
	}
}

fn resolve_reviewed_plan(
	proposal: &plan::PlanProposal,
	target_url: &str,
	root: &Path,
) -> Result<plan::ResolvedPlan, ToolError> {
	// Surface an oversized target as its actionable size error instead of
	// converting it to the resolver's generic "Plan file not found".
	if let Err(error) = read_plan_candidate(root, target_url) {
		return Err(ToolError::Rejected(error));
	}
	let read_error = RefCell::new(None::<String>);
	let resolved = plan::resolve_approved_plan(
		proposal.title.as_deref(),
		target_url,
		&|url| match read_plan_candidate(root, url) {
			Ok(content) => content,
			Err(error) => {
				if read_error.borrow().is_none() {
					*read_error.borrow_mut() = Some(error);
				}
				None
			},
		},
		&|| plan::list_local_plan_files(root),
	);
	if resolved.is_ok() {
		return resolved.map_err(ToolError::Rejected);
	}
	if let Some(error) = read_error.into_inner() {
		return Err(ToolError::Rejected(error));
	}
	resolved.map_err(ToolError::Rejected)
}

fn render_handoff(plan_file_path: &str, title: &str) -> String {
	PLAN_YOLO_HANDOFF_PROMPT
		.replace("{{title}}", title)
		.replace("{{planFilePath}}", plan_file_path)
}

#[cfg(test)]
mod tests {
	use std::{fs::File, io::Write};

	use super::*;

	fn target() -> ModelSwitch {
		ModelSwitch {
			provider: "mock".to_string(),
			model:    "fast".to_string(),
			effort:   None,
			thinking: None,
		}
	}

	fn shared() -> Arc<PlanYoloShared> {
		Arc::new(PlanYoloShared::new(
			Arc::new(Mutex::new(PlanGate::new(false))),
			PlanYoloArm { target: target(), thinking: None },
		))
	}

	fn temp_root(name: &str) -> PathBuf {
		let path = std::env::temp_dir().join(format!(
			"omp-plan-yolo-{name}-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.expect("clock")
				.as_nanos()
		));
		std::fs::create_dir_all(&path).expect("temp root");
		path
	}

	#[test]
	fn arm_publishes_plan_mode_and_rollback_restores_the_gate() {
		let shared = shared();
		let arming = shared.arm().expect("arm");
		{
			let gate = shared.gate();
			let gate = gate.lock().unwrap();
			let state = gate.state.as_ref().expect("plan mode");
			assert!(state.enabled);
			assert_eq!(state.plan_file_path, DEFAULT_PLAN_FILE_URL);
		}
		shared.rollback_arm(arming);
		assert!(!shared.is_armed());
		assert!(shared.gate().lock().unwrap().state.is_none());
		// TS resets `#planYoloArmed` on tool-set failure, so a retry is legal.
		assert!(shared.arm().is_some());
	}

	#[test]
	fn augment_tools_adds_write_only_to_an_enabled_restricted_set() {
		assert_eq!(augment_tools(true, None), None);
		assert_eq!(
			augment_tools(true, Some(&["read".to_string()])),
			Some(vec!["read".to_string(), "write".to_string()])
		);
		assert_eq!(
			augment_tools(true, Some(&["read".to_string(), "write".to_string()])),
			Some(vec!["read".to_string(), "write".to_string()])
		);
		assert_eq!(
			augment_tools(false, Some(&["read".to_string()])),
			Some(vec!["read".to_string()])
		);
	}

	#[test]
	fn truncated_approval_read_cannot_become_a_prefix_approval() {
		let over = "x".repeat(pi_agent::tools::hashline::snapshot::SNAPSHOT_MAX_BYTES + 1);
		assert!(ensure_plan_fully_read("local://PLAN.md", &over).is_err());
		let exact = "x".repeat(pi_agent::tools::hashline::snapshot::SNAPSHOT_MAX_BYTES);
		assert!(ensure_plan_fully_read("local://PLAN.md", &exact).is_ok());
	}

	#[test]
	fn inline_proposal_uses_the_write_tool_limit() {
		let over = "x".repeat(MAX_INLINE_PLAN_BYTES + 1);
		assert!(ensure_inline_plan_size(&over).is_err());
		let exact = "x".repeat(MAX_INLINE_PLAN_BYTES);
		assert!(ensure_inline_plan_size(&exact).is_ok());
	}

	#[test]
	fn plan_tools_keep_the_previous_selection_for_restore() {
		let read = vec!["read".to_string()];
		let tools = plan_tools(true, Some(&read)).expect("tools enabled");
		assert_eq!(
			tools.selected().map(|selected| selected.to_vec()),
			Some(vec!["read".to_string(), "write".to_string()])
		);
		assert_eq!(tools.previous().map(|previous| previous.to_vec()), Some(read.clone()));
		assert_eq!(tools.hidden_by_plan(), vec!["write".to_string()]);
		assert_eq!(tools.restore(), Some(read));
		assert!(plan_tools(false, None).is_err(), "--no-tools must not arm");
	}

	#[test]
	fn oversized_plan_surfaces_the_reader_error() {
		let root = temp_root("oversize-resolve");
		let path = root.join("PLAN.md");
		File::create(&path)
			.expect("plan")
			.set_len(pi_agent::tools::hashline::snapshot::SNAPSHOT_MAX_BYTES as u64 + 1)
			.expect("plan size");
		let proposal = plan::PlanProposal {
			title:   Some("Plan".to_string()),
			slug:    None,
			content: None,
		};
		let error = resolve_reviewed_plan(&proposal, "local://PLAN.md", &root)
			.expect_err("oversized plan");
		assert!(error.to_string().contains("full-text artifact limit"), "{error}");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn successful_resolution_ignores_an_unrelated_truncated_candidate() {
		let root = temp_root("candidate-error");
		std::fs::write(root.join("PLAN.md"), "# Plan\n\nDo it.\n").expect("valid plan");
		File::create(root.join("A-plan.md"))
			.expect("oversized candidate")
			.set_len(pi_agent::tools::hashline::snapshot::SNAPSHOT_MAX_BYTES as u64 + 1)
			.expect("candidate size");
		let proposal = plan::PlanProposal {
			title:   None,
			slug:    None,
			content: None,
		};
		let resolved = resolve_reviewed_plan(&proposal, "local://PLAN.md", &root)
			.expect("the valid plan must win even if an earlier candidate was truncated");
		assert_eq!(resolved.plan_file_path, "local://PLAN.md");
		let _ = std::fs::remove_dir_all(root);
	}

	#[tokio::test]
	async fn proposal_rejects_a_changed_generation_before_inline_write() {
		let shared = shared();
		let _arming = shared.arm().expect("arm");
		let root = temp_root("stale-generation");
		{
			let gate = shared.gate();
			let mut gate = gate.lock().unwrap();
			gate.generation = gate.generation.wrapping_add(1);
		}
		let handler = PlanYoloProposalHandler {
			shared:    Arc::clone(&shared),
			artifacts: Some(root.clone()),
		};
		let ctx = AgentContext::new(root.clone());
		let error = pi_agent::state::ProposeHandler::propose(
			&handler,
			r##"{"title":"Plan","content":"# Plan\n"}"##,
			&ctx,
		)
		.await
		.expect_err("a stale plan generation must reject before writing");
		assert!(error.to_string().contains("plan mode changed"), "{error}");
		assert!(!root.join("PLAN.md").exists(), "the inline artifact must not be written");
		let _ = std::fs::remove_dir_all(root);
	}

	#[tokio::test]
	async fn stale_handler_refuses_autoapproval_after_commit() {
		let shared = shared();
		let _arming = shared.arm().expect("arm");
		let root = temp_root("stale-handler");
		let content = "# Plan\n\nDo it.\n";
		File::create(root.join("PLAN.md"))
			.expect("plan")
			.write_all(content.as_bytes())
			.expect("plan body");
		*shared.pending.lock().unwrap() = Some(PlanYoloPending {
			target: target(),
			thinking: None,
			plan_file_path: "local://PLAN.md".to_string(),
			title: "Plan".to_string(),
			reviewed_content: content.to_string(),
		});
		shared
			.commit_pending(Some(&root), |_| Ok(()))
			.expect("initial approval");

		// A later manual plan entry keeps the handler installed. It must not
		// inherit plan-yolo's auto-approval.
		{
			let gate = shared.gate();
			let mut gate = gate.lock().unwrap();
			gate.enabled = true;
			gate.set_mode(plan::ACP_PLAN_MODE_ID).expect("manual plan");
		}
		let handler = PlanYoloProposalHandler {
			shared:    Arc::clone(&shared),
			artifacts: Some(root.clone()),
		};
		let ctx = AgentContext::new(root.clone());
		let error = pi_agent::state::ProposeHandler::propose(&handler, "Plan", &ctx)
			.await
			.expect_err("stale handler must reject");
		assert!(error.to_string().contains("no longer armed"), "{error}");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn oversized_reviewed_plan_keeps_the_guard_and_clears_pending() {
		let shared = shared();
		let _arming = shared.arm().expect("arm");
		let root = temp_root("oversized-plan");
		let over = "x".repeat(pi_agent::tools::hashline::snapshot::SNAPSHOT_MAX_BYTES + 1);
		std::fs::write(root.join("PLAN.md"), &over).expect("plan");
		*shared.pending.lock().unwrap() = Some(PlanYoloPending {
			target: target(),
			thinking: None,
			plan_file_path: "local://PLAN.md".to_string(),
			title: "Plan".to_string(),
			reviewed_content: over,
		});

		let error = shared
			.commit_pending(Some(&root), |_| Ok(()))
			.expect_err("truncated approval");
		assert!(error.contains("approval read limit"));
		assert!(shared.gate().lock().unwrap().state.as_ref().is_some_and(|state| state.enabled));
		assert!(!shared.has_pending());
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn failed_switch_keeps_the_guard_and_pending_approval() {
		let shared = shared();
		let _arming = shared.arm().expect("arm");
		let root = temp_root("failed-switch");
		std::fs::write(root.join("PLAN.md"), "# Plan\n\nDo it.\n").expect("plan");
		*shared.pending.lock().unwrap() = Some(PlanYoloPending {
			target: target(),
			thinking: None,
			plan_file_path: "local://PLAN.md".to_string(),
			title: "Plan".to_string(),
			reviewed_content: "# Plan\n\nDo it.\n".to_string(),
		});

		let error = shared
			.commit_pending(Some(&root), |_| Err::<(), _>("no key".to_string()))
			.expect_err("build must fail");
		assert!(error.contains("no key"));
		assert!(shared.gate().lock().unwrap().state.as_ref().is_some_and(|state| state.enabled));
		assert!(shared.has_pending());
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn successful_switch_is_atomic_and_renders_the_handoff() {
		let shared = shared();
		let _arming = shared.arm().expect("arm");
		let root = temp_root("successful-switch");
		let content = "# Plan\n\nDo it.\n";
		std::fs::write(root.join("PLAN.md"), content).expect("plan");
		*shared.pending.lock().unwrap() = Some(PlanYoloPending {
			target: target(),
			thinking: None,
			plan_file_path: "local://PLAN.md".to_string(),
			title: "Plan".to_string(),
			reviewed_content: content.to_string(),
		});

		let (commit, built) = shared
			.commit_pending(Some(&root), |_| Ok(7))
			.expect("switch");
		assert_eq!(built, 7);
		assert!(commit.handoff.contains("local://PLAN.md"));
		assert!(commit.handoff.contains("Plan"));
		assert!(commit.notice.contains("mock/fast"));
		assert!(shared.gate().lock().unwrap().state.is_none());
		assert!(!shared.has_pending());
		// One-shot: a completed plan-yolo never re-arms.
		assert!(shared.arm().is_none());
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn changed_plan_fails_revalidation_without_exiting_the_guard() {
		let shared = shared();
		let _arming = shared.arm().expect("arm");
		let root = temp_root("changed-plan");
		std::fs::write(root.join("PLAN.md"), "# Changed\n").expect("plan");
		*shared.pending.lock().unwrap() = Some(PlanYoloPending {
			target: target(),
			thinking: None,
			plan_file_path: "local://PLAN.md".to_string(),
			title: "Plan".to_string(),
			reviewed_content: "# Original\n".to_string(),
		});

		let error = shared
			.commit_pending(Some(&root), |_| Ok(()))
			.expect_err("changed plan");
		assert!(error.contains("changed after approval"));
		assert!(shared.gate().lock().unwrap().state.as_ref().is_some_and(|state| state.enabled));
		assert!(!shared.has_pending());
		let _ = std::fs::remove_dir_all(root);
	}
}
