use std::sync::Arc;

use futures::future::join_all;

use super::registry::{ToolRegistry, excluded_tool_reason};
use crate::durable::{
	DURABILITY_ADMISSION_FAILURE_CODE, DURABILITY_FAILURE_CODE, DURABILITY_UNKNOWN_OUTCOME_CODE,
	DurableBoundary, DurableError,
};
use crate::hooks::HookEngine;
use crate::tool::ToolError;
use crate::state::{AgentContext, ApprovalHint, ToolApprover, ToolCall, ToolResult};

/// Upper bound on tool executions in flight inside one concurrent group.
pub const DEFAULT_MAX_CONCURRENT_TOOLS: usize = 8;

/// Scheduler partitions a tool batch into concurrent non-exclusive runs and
/// sequential exclusive ones, preserving call order in the results.
pub struct ToolScheduler {
	registry: ToolRegistry,
	approver: Option<Arc<dyn ToolApprover>>,
	/// Internal tools hidden at a turn boundary. A delta preserves mounted
	/// MCP/host tools that were never in the config allowlist.
	hidden: std::sync::Mutex<Vec<String>>,
	/// Host durability boundary: admission writes an intent before the effect
	/// and an outcome after it. `None` means the host has no durable session.
	durable: Option<Arc<dyn DurableBoundary>>,
	/// Trusted subprocess hooks for `PreToolUse`/`PostToolUse`.
	hooks: Option<Arc<HookEngine>>,
	/// Cap on non-exclusive tools executing at the same time in one batch.
	max_concurrent: usize,
}

impl ToolScheduler {
	pub fn new(registry: ToolRegistry) -> Self {
		Self {
			registry,
			approver: None,
			hidden: std::sync::Mutex::new(Vec::new()),
			durable: None,
			hooks: None,
			max_concurrent: DEFAULT_MAX_CONCURRENT_TOOLS,
		}
	}

	pub fn with_approver(registry: ToolRegistry, approver: Arc<dyn ToolApprover>) -> Self {
		Self {
			registry,
			approver: Some(approver),
			hidden: std::sync::Mutex::new(Vec::new()),
			durable: None,
			hooks: None,
			max_concurrent: DEFAULT_MAX_CONCURRENT_TOOLS,
		}
	}

	/// Cap how many non-exclusive tools run at once. Clamped to
	/// `1..=Semaphore::MAX_PERMITS` so the semaphore constructor cannot panic.
	#[must_use]
	pub const fn with_max_concurrent(mut self, limit: usize) -> Self {
		// `Ord::clamp` is not const-callable; keep the bounds explicit.
		self.max_concurrent = if limit == 0 {
			1
		} else if limit > tokio::sync::Semaphore::MAX_PERMITS {
			tokio::sync::Semaphore::MAX_PERMITS
		} else {
			limit
		};
		self
	}

	pub fn approver(&self) -> Option<&Arc<dyn ToolApprover>> {
		self.approver.as_ref()
	}

	/// Install or clear the host durability boundary. The loop attaches this
	/// when the session owner supplies a store.
	pub fn set_durable(&mut self, durable: Option<Arc<dyn DurableBoundary>>) {
		self.durable = durable;
	}

	/// Install or clear the trusted subprocess hook engine.
	pub fn set_hooks(&mut self, hooks: Option<Arc<HookEngine>>) {
		self.hooks = hooks;
	}

	/// Hide internal tool names before the next provider request.
	pub fn hide_tools(&self, names: Vec<String>) {
		let mut hidden = self
			.hidden
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		hidden.extend(names);
	}

	#[must_use]
	pub fn hidden(&self) -> Vec<String> {
		self
			.hidden
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.clone()
	}
	/// Execute a batch of tool calls:
	/// - Non-exclusive tools (read, grep, glob) run concurrently.
	/// - Exclusive tools (bash, write, todo) run one at a time.
	///
	/// Always returns exactly one result per call, in call order.
	pub async fn execute_batch(&self, calls: &[ToolCall], ctx: &AgentContext) -> Vec<ToolResult> {
		let mut results = Vec::with_capacity(calls.len());
		let mut concurrent: Vec<&ToolCall> = Vec::new();

		for call in calls {
			let is_exclusive = self
				.registry
				.get(&call.name)
				.is_some_and(|tool| tool.is_exclusive());
			if !is_exclusive {
				concurrent.push(call);
				continue;
			}
			if !concurrent.is_empty() {
				results.extend(self.run_concurrent(&concurrent, ctx).await);
				concurrent.clear();
			}
			results.push(self.run_single(call, ctx, None, None).await);
		}

		if !concurrent.is_empty() {
			results.extend(self.run_concurrent(&concurrent, ctx).await);
		}
		results
	}

	async fn run_single(
		&self,
		call: &ToolCall,
		ctx: &AgentContext,
		exec_permits: Option<&Arc<tokio::sync::Semaphore>>,
		hook_permits: Option<&Arc<tokio::sync::Semaphore>>,
	) -> ToolResult {
		let hidden = self.hidden();
		let is_hidden = hidden.iter().any(|name| name == &call.name)
			|| self
				.registry
				.get(&call.name)
				.is_some_and(|tool| hidden.iter().any(|name| name == tool.name()));
		if is_hidden {
			return ToolResult {
				tool_call_id: call.id.clone(),
				content: format!(
					"Tool execution denied: {} is not available in the active tool selection.",
					call.name
				),
				is_error: true,
				terminate: false,
				details: None,
			};
		}
		let Some(tool) = self.registry.get(&call.name) else {
			// A known-but-excluded tool carries a machine-readable boundary marker
			// in `details`; unknown names keep the empty diagnostic shape.
			let details = excluded_tool_reason(&call.name).map(|reason| {
				serde_json::json!({
					"code": "platform_boundary",
					"tool": &call.name,
					"reason": reason,
				})
			});
			return ToolResult {
				tool_call_id: call.id.clone(),
				content: self.registry.missing_tool_diagnostic(&call.name),
				is_error: true,
				terminate: false,
				details,
			};
		};

		// `PreToolUse` runs before approval and execution: it may deny the call
		// or rewrite its input. `PermissionRequest` (inside the approver) owns
		// the allow decision, so a hook cannot bypass the bash policy here.
		let mut effective_call = call.clone();
		let mut hook_context = Vec::new();
		let mut approval_hint = ApprovalHint::default();
		if let Some(hooks) = &self.hooks {
			// Hook subprocesses are bounded too. A call queued behind another
			// call's hook is cancelled here, before any effect exists.
			let _hook_permit = match hook_permits {
				Some(permits) => {
					let permit = tokio::select! {
						biased;
						() = ctx.cancellation_token.cancelled() => {
							return self
								.abort_before_start(call, &effective_call, None)
								.await;
						},
						permit = Arc::clone(permits).acquire_owned() => {
							permit.expect("scheduler semaphore is never closed")
						},
					};
					Some(permit)
				},
				None => None,
			};
			let decision = hooks.pre_tool_use(&call.name, &call.arguments).await;
			hook_context.extend(decision.additional_context);
			hook_context.extend(decision.system_messages);
			approval_hint.force_ask = decision.forced_ask;
			if let Some(reason) = decision.denied {
				return ToolResult {
					tool_call_id: call.id.clone(),
					content: format!("Tool execution denied: {reason}"),
					is_error: true,
					terminate: false,
					details: None,
				};
			}
			if let Some(updated) = decision.updated_input {
				effective_call.arguments = updated;
			}
		}

		let approver = self.approver.as_ref().or(ctx.approver.as_ref());
		if approval_hint.force_ask
			&& approver.is_none_or(|approver| !approver.supports_approval_hints())
		{
			let content = if approver.is_some() {
				"Approval required by PreToolUse hook, but this approver cannot force a prompt"
			} else {
				"Approval required by PreToolUse hook, but no approver is available"
			};
			return ToolResult {
				tool_call_id: call.id.clone(),
				content: content.to_string(),
				is_error: true,
				terminate: false,
				details: None,
			};
		}
		if let Some(approver) = approver {
			// A pending prompt must not outlive the turn: dropping this future
			// aborts the approver's blocking read, and no approved-but-cancelled
			// call reaches `execute`.
			let decision = tokio::select! {
				biased;
				decision = approver.approve_with_hint(&effective_call.name, &effective_call.arguments, approval_hint) => decision,
				_ = ctx.cancellation_token.cancelled() => Err("approval cancelled".to_string()),
			};
			if let Err(denial) = decision {
				return ToolResult {
					tool_call_id: call.id.clone(),
					content: format!("Tool execution denied: {denial}"),
					is_error: true,
					terminate: false,
					details: None,
				};
			}
			if ctx.cancellation_token.is_cancelled() {
				return ToolResult {
					tool_call_id: call.id.clone(),
					content: "Tool execution aborted before start".to_string(),
					is_error: true,
					terminate: false,
					details: None,
				};
			}
		}

		// Final cancellation check at the admission boundary, with or without an
		// approver: no intent/effect starts after a cancel.
		if ctx.cancellation_token.is_cancelled() {
			return ToolResult {
				tool_call_id: call.id.clone(),
				content: "Tool execution aborted before start".to_string(),
				is_error: true,
				terminate: false,
				details: None,
			};
		}

		// Worker extension is S4: child agent turns are not yet covered by the
		// durable boundary, so the spawn tool is explicitly gated rather than
		// running unpersisted effects.
		if self.durable.is_some() && call.name == "task" {
			return ToolResult {
				tool_call_id: call.id.clone(),
				content: "The task tool is unavailable while durable session persistence is \
					active: child agent turns are not yet part of the persistence boundary."
					.to_string(),
				is_error: true,
				terminate: false,
				details: Some(serde_json::json!({
					"code": "worker_durability_pending",
				})),
			};
		}

		// The execution slot is taken after `PreToolUse` rewriting, so the
		// exemption decision sees the effective arguments and a hub wait never
		// holds a slot while blocked. It is taken before the durable intent: a
		// turn killed while queued for a slot must leave no unresolved intents
		// for tools that never started.
		let exempt = (call.name == "hub" || tool.name() == "hub")
			&& hub_op_is_control(&effective_call.arguments);
		let exec_permit = match exec_permits {
			Some(permits) if !exempt => {
				// A cancel while queued must not let the effect start later.
				let permit = tokio::select! {
					biased;
					() = ctx.cancellation_token.cancelled() => {
						return self
							.abort_before_start(call, &effective_call, None)
							.await;
					},
					permit = Arc::clone(permits).acquire_owned() => {
						permit.expect("scheduler semaphore is never closed")
					},
				};
				// The permit wait may have raced a cancel that no longer wakes
				// this future: recheck before the intent or effect starts.
				if ctx.cancellation_token.is_cancelled() {
					drop(permit);
					return self
						.abort_before_start(call, &effective_call, None)
						.await;
				}
				Some(permit)
			},
			_ => None,
		};

		let execution_id = match &self.durable {
			Some(durable) => match durable.begin_execution(&effective_call).await {
				Ok(id) => Some(id),
				Err(error) => {
					// Admission failed: the effect must not start. An unknown write
					// outcome may still have recorded the intent, so the message must
					// not claim the intent write cleanly failed.
					let message = if error.is_unknown_outcome() {
						format!("durable intent write outcome unknown: {error}")
					} else {
						format!("tool not started: durable intent write failed: {error}")
					};
					return durable_admission_failure_result(&call.id, &error, message);
				},
			},
			None => None,
		};

		// The durable admission await may have raced a cancel (or a policy
		// change observed by the caller): recheck before any effect starts.
		if ctx.cancellation_token.is_cancelled() {
			return self
				.abort_before_start(call, &effective_call, execution_id.as_deref())
				.await;
		}

		let result = match tool.execute_with_call_id(&effective_call.id, &effective_call.arguments, ctx).await {
			Ok(result) => result,
			Err(err) => tool_error_result(&effective_call.id, err),
		};
		let mut result = ToolResult { tool_call_id: call.id.clone(), ..result };
		// Hook subprocesses have their own semaphore; release the execution
		// slot first so permits are never held in a nested order.
		drop(exec_permit);
		if let Some(hooks) = &self.hooks {
			// A completed tool always reports to `PostToolUse`, even after a
			// cancel: the hook spawn is bounded, never dropped.
			let _post_hook_permit = match hook_permits {
				Some(permits) => Some(
					Arc::clone(permits)
						.acquire_owned()
						.await
						.expect("scheduler semaphore is never closed"),
				),
				None => None,
			};
			let post = hooks
				.post_tool_use(
					&effective_call.name,
					&effective_call.arguments,
					&result.content,
					result.is_error,
				)
				.await;
			if let Some(reason) = post.blocked {
				append_hook_text(&mut result, "Hook feedback", &reason);
			}
			hook_context.extend(post.additional_context);
			hook_context.extend(post.system_messages);
		}
		for text in hook_context {
			append_hook_text(&mut result, "Hook context", &text);
		}
		if let (Some(durable), Some(execution_id)) = (&self.durable, execution_id.as_deref())
			&& let Err(error) = durable.finish_execution(execution_id, &effective_call, &result).await
		{
			// The effect may already have run. The intent stays unresolved; the
			// loop terminates without another model request and never retries.
			return durable_failure_result(
				&call.id,
				&error,
				format!("tool effect outcome could not be persisted: {error}"),
			);
		}
		result
	}

	/// Terminal result for a call aborted before its effect starts. Rewrites the
	/// durable intent to the aborted outcome; a cancellation outcome that could
	/// not be persisted becomes a durability failure marker.
	async fn abort_before_start(
		&self,
		call: &ToolCall,
		effective_call: &ToolCall,
		execution_id: Option<&str>,
	) -> ToolResult {
		let aborted = ToolResult {
			tool_call_id: call.id.clone(),
			content: "Tool execution aborted before start".to_string(),
			is_error: true,
			terminate: false,
			details: None,
		};
		if let (Some(durable), Some(execution_id)) = (&self.durable, execution_id)
			&& let Err(error) = durable
				.finish_execution(execution_id, effective_call, &aborted)
				.await
		{
			return durable_failure_result(
				&call.id,
				&error,
				format!("tool cancellation outcome could not be persisted: {error}"),
			);
		}
		aborted
	}

	async fn run_concurrent(&self, calls: &[&ToolCall], ctx: &AgentContext) -> Vec<ToolResult> {
		// `join_all` polls in input order and collects in that order, so the
		// results stay index-aligned with `calls` however the permits interleave.
		let exec_permits = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent));
		let hook_permits = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent));
		join_all(calls.iter().map(|call| {
			let exec_permits = Arc::clone(&exec_permits);
			let hook_permits = Arc::clone(&hook_permits);
			async move {
				self.run_single(call, ctx, Some(&exec_permits), Some(&hook_permits))
					.await
			}
		}))
		.await
	}
}

/// `hub` ops that only observe or unblock other calls. Work-starting ops
/// (`start`, `restart`) and unknown/missing ops deliberately keep their
/// concurrency permit so a batch cannot spawn unbounded work.
fn hub_op_is_control(args: &serde_json::Value) -> bool {
	// `HubTool` resolves the op with `require_non_empty_str`, which trims, so
	// the exemption must trim the same way or a padded op silently escapes it.
	matches!(
		args.get("op").and_then(serde_json::Value::as_str).map(str::trim),
		Some(
			"jobs" | "cancel" | "wait" | "send" | "inbox" | "list" | "ps" | "logs" | "stop"
				| "describe"
		)
	)
}

fn append_hook_text(result: &mut ToolResult, label: &str, text: &str) {
	if text.trim().is_empty() {
		return;
	}
	result.content.push_str("\n\n");
	result.content.push_str(label);
	result.content.push_str(": ");
	result.content.push_str(text.trim());
}

fn tool_error_result(tool_call_id: &str, err: ToolError) -> ToolResult {
	ToolResult {
		tool_call_id: tool_call_id.to_string(),
		content: format!("Tool error: {err}"),
		is_error: true,
		terminate: false,
		details: None,
	}
}

/// Marker code for a durability write failure. An unknown outcome must never be
/// reported as a clean failure: the write may have committed.
fn durability_failure_code(error: &DurableError, clean: &'static str) -> &'static str {
	if error.is_unknown_outcome() {
		DURABILITY_UNKNOWN_OUTCOME_CODE
	} else {
		clean
	}
}

/// Terminal result for a durability failure. The loop detects the marker and
/// converts the batch into an [`crate::AgentError`] before any further model
/// request; `terminate` also stops a host that only reads results.
fn durable_failure_result(
	tool_call_id: &str,
	error: &DurableError,
	message: String,
) -> ToolResult {
	durable_terminal_result(
		tool_call_id,
		message,
		durability_failure_code(error, DURABILITY_FAILURE_CODE),
	)
}

/// Terminal result for an admission durability failure where the effect was not started.
fn durable_admission_failure_result(
	tool_call_id: &str,
	error: &DurableError,
	message: String,
) -> ToolResult {
	durable_terminal_result(
		tool_call_id,
		message,
		durability_failure_code(error, DURABILITY_ADMISSION_FAILURE_CODE),
	)
}

fn durable_terminal_result(tool_call_id: &str, message: String, code: &'static str) -> ToolResult {
	ToolResult {
		tool_call_id: tool_call_id.to_string(),
		content: message,
		is_error: true,
		terminate: true,
		details: Some(serde_json::json!({
			"code": code,
		})),
	}
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;
	use std::sync::Arc;
	use std::time::Duration;

	use async_trait::async_trait;
	use serde_json::json;

	use super::*;
	use crate::CancellationToken;
	use crate::tools::{BashTool, registry::ToolRegistry};

	#[test]
	fn unknown_outcome_never_uses_a_clean_failure_marker() {
		let unknown = DurableError::unknown_outcome("writer stopped before replying");
		let admission =
			durable_admission_failure_result("call-1", &unknown, "lost".to_string());
		assert_eq!(
			admission.details.expect("details")["code"],
			json!(DURABILITY_UNKNOWN_OUTCOME_CODE)
		);
		let effect = durable_failure_result("call-2", &unknown, "lost".to_string());
		assert_eq!(
			effect.details.expect("details")["code"],
			json!(DURABILITY_UNKNOWN_OUTCOME_CODE)
		);

		let clean = DurableError::new("session store busy");
		let admission =
			durable_admission_failure_result("call-3", &clean, "failed".to_string());
		assert_eq!(
			admission.details.expect("details")["code"],
			json!(DURABILITY_ADMISSION_FAILURE_CODE)
		);
		let effect = durable_failure_result("call-4", &clean, "failed".to_string());
		assert_eq!(effect.details.expect("details")["code"], json!(DURABILITY_FAILURE_CODE));
	}

	struct PendingApprover {
		started: tokio::sync::mpsc::Sender<()>,
	}

	#[async_trait]
	impl ToolApprover for PendingApprover {
		async fn approve(&self, _name: &str, _args: &serde_json::Value) -> Result<(), String> {
			let _ = self.started.send(()).await;
			std::future::pending::<()>().await;
			Ok(())
		}
	}

	struct AllowApprover;

	#[async_trait]
	impl ToolApprover for AllowApprover {
		async fn approve(&self, _name: &str, _args: &serde_json::Value) -> Result<(), String> {
			Ok(())
		}
	}

	/// Counts approval prompts so a test can prove none was asked.
	struct CountingApprover(Arc<std::sync::atomic::AtomicUsize>);

	#[async_trait]
	impl ToolApprover for CountingApprover {
		async fn approve(&self, _name: &str, _args: &serde_json::Value) -> Result<(), String> {
			self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			Ok(())
		}
	}

	fn temp_dir(label: &str) -> PathBuf {
		static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
		let path = std::env::temp_dir().join(format!(
			"pi-agent-scheduler-{label}-{}-{}",
			std::process::id(),
			NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
		));
		std::fs::create_dir_all(&path).expect("create temp dir");
		path
	}

	fn bash_call(marker: &std::path::Path) -> Vec<ToolCall> {
		vec![ToolCall {
			id: "call_1".to_string(),
			name: "bash".to_string(),
			arguments: json!({"command": format!("touch {}", marker.display())}),
		}]
	}

	fn bash_scheduler(approver: Arc<dyn ToolApprover>) -> ToolScheduler {
		let mut registry = ToolRegistry::new();
		registry.register(Arc::new(BashTool::legacy_one_shot()));
		ToolScheduler::with_approver(registry, approver)
	}

	#[tokio::test]
	async fn hidden_tool_is_denied_before_execution() {
		let dir = temp_dir("hidden");
		let marker = dir.join("ran");
		let scheduler = ToolScheduler::new({
			let mut registry = ToolRegistry::new();
			registry.register(Arc::new(BashTool::legacy_one_shot()));
			registry
		});
		scheduler.hide_tools(vec!["bash".to_string()]);
		let results = scheduler.execute_batch(&bash_call(&marker), &AgentContext::new(dir)).await;
		assert_eq!(results.len(), 1);
		assert!(results[0].is_error, "a hidden tool must not run");
		assert!(results[0].content.contains("not available in the active tool selection"));
		assert!(!marker.exists(), "the denied command must not execute");
	}

	#[tokio::test]
	async fn excluded_tool_never_prompts_or_runs() {
		let approvals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let scheduler = ToolScheduler::with_approver(
			ToolRegistry::new(),
			Arc::new(CountingApprover(approvals.clone())),
		);
		let ctx = AgentContext::new(temp_dir("computer-boundary"));
		let calls = vec![ToolCall {
			id:        "call_1".to_string(),
			name:      "computer".to_string(),
			arguments: json!({ "action": "screenshot" }),
		}];

		let results = scheduler.execute_batch(&calls, &ctx).await;
		assert_eq!(results.len(), 1);
		assert!(results[0].is_error, "an excluded tool must not succeed");
		assert!(
			results[0].content.contains("excluded by the platform boundary"),
			"{}",
			results[0].content
		);
		let details = results[0].details.as_ref().expect("typed boundary details");
		assert_eq!(details["code"], "platform_boundary");
		assert_eq!(details["tool"], "computer");
		assert_eq!(
			details["reason"].as_str(),
			excluded_tool_reason("computer"),
			"details must reuse the exclusion table"
		);
		assert_eq!(
			approvals.load(std::sync::atomic::Ordering::SeqCst),
			0,
			"an excluded tool must not ask for approval"
		);
	}

	#[tokio::test]
	async fn cancelled_approval_never_executes_the_tool() {
		let dir = temp_dir("approval-cancel");
		let marker = dir.join("ran");
		let (started_tx, mut started_rx) = tokio::sync::mpsc::channel(1);
		let scheduler = bash_scheduler(Arc::new(PendingApprover { started: started_tx }));
		let token = CancellationToken::new();
		let ctx = AgentContext::with_cancellation(dir.clone(), token.clone());
		let calls = bash_call(&marker);

		let batch = tokio::spawn(async move { scheduler.execute_batch(&calls, &ctx).await });
		started_rx
			.recv()
			.await
			.expect("approval prompt must be pending");
		token.cancel();

		let results = tokio::time::timeout(Duration::from_secs(5), batch)
			.await
			.expect("cancelled approval must settle")
			.expect("join");
		assert_eq!(results.len(), 1);
		assert!(results[0].is_error, "cancelled approval must not succeed");
		assert!(!marker.exists(), "cancelled approval still ran the command");
	}

	#[tokio::test]
	async fn approval_after_cancellation_does_not_execute_the_tool() {
		let dir = temp_dir("approval-late");
		let marker = dir.join("ran");
		let scheduler = bash_scheduler(Arc::new(AllowApprover));
		let token = CancellationToken::new();
		token.cancel();
		let ctx = AgentContext::with_cancellation(dir.clone(), token);
		let calls = bash_call(&marker);

		let results = scheduler.execute_batch(&calls, &ctx).await;
		assert_eq!(results.len(), 1);
		assert!(results[0].is_error, "cancelled turn must not execute the tool");
		assert!(!marker.exists(), "approved call ran after cancellation");
	}

	struct ConcurrencyProbe {
		active: Arc<std::sync::atomic::AtomicUsize>,
		peak:   Arc<std::sync::atomic::AtomicUsize>,
	}

	#[async_trait]
	impl crate::tool::Tool for ConcurrencyProbe {
		fn name(&self) -> &str {
			"probe"
		}

		fn description(&self) -> &str {
			"concurrency probe"
		}

		fn schema(&self) -> serde_json::Value {
			json!({"type": "object"})
		}

		async fn execute(
			&self,
			args: &serde_json::Value,
			_ctx: &AgentContext,
		) -> Result<crate::state::ToolResult, ToolError> {
			use std::sync::atomic::Ordering;
			let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
			self.peak.fetch_max(now, Ordering::SeqCst);
			let sleep_ms = args.get("sleep_ms").and_then(serde_json::Value::as_u64).unwrap_or(0);
			tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
			self.active.fetch_sub(1, Ordering::SeqCst);
			if args.get("fail").and_then(serde_json::Value::as_bool) == Some(true) {
				return Err(ToolError::ExecutionFailed(format!("probe {sleep_ms} failed")));
			}
			Ok(crate::state::ToolResult {
				tool_call_id: String::new(),
				content: format!("probe {sleep_ms}"),
				is_error: false,
				terminate: false,
				details: None,
			})
		}
	}

	fn probe_scheduler(
		limit: usize,
		active: &Arc<std::sync::atomic::AtomicUsize>,
		peak: &Arc<std::sync::atomic::AtomicUsize>,
	) -> ToolScheduler {
		let mut registry = ToolRegistry::new();
		registry.register(Arc::new(ConcurrencyProbe {
			active: Arc::clone(active),
			peak:   Arc::clone(peak),
		}));
		ToolScheduler::new(registry).with_max_concurrent(limit)
	}

	/// Test double for the real `hub` tool: `wait` blocks until a `send` in the
	/// same batch notifies the shared gate.
	struct HubGate {
		gate: Arc<tokio::sync::Notify>,
	}

	#[async_trait]
	impl crate::tool::Tool for HubGate {
		fn name(&self) -> &str {
			"hub"
		}

		fn description(&self) -> &str {
			"hub test double"
		}

		fn schema(&self) -> serde_json::Value {
			json!({"type": "object"})
		}

		async fn execute(
			&self,
			args: &serde_json::Value,
			_ctx: &AgentContext,
		) -> Result<crate::state::ToolResult, ToolError> {
			match args.get("op").and_then(serde_json::Value::as_str) {
				Some("wait") => {
					self.gate.notified().await;
				},
				Some("send") => self.gate.notify_one(),
				_ => {},
			}
			Ok(crate::state::ToolResult {
				tool_call_id: String::new(),
				content: "hub".to_string(),
				is_error: false,
				terminate: false,
				details: None,
			})
		}
	}

	fn hub_call(id: &str, op: &str) -> ToolCall {
		ToolCall {
			id: id.to_string(),
			name: "hub".to_string(),
			arguments: json!({"op": op}),
		}
	}

	/// Test double for hub work-starting ops: `start` sleeps, then marks done
	/// so a later probe can prove it only ran after the permit was released.
	/// `started` is written at execute entry, i.e. after the scheduler acquired
	/// the permit, so a hook can wait for the effect boundary deterministically.
	struct HubStart {
		done:    Arc<std::sync::atomic::AtomicUsize>,
		started: Option<Arc<std::path::PathBuf>>,
	}

	#[async_trait]
	impl crate::tool::Tool for HubStart {
		fn name(&self) -> &str {
			"hub"
		}

		fn description(&self) -> &str {
			"hub start test double"
		}

		fn schema(&self) -> serde_json::Value {
			json!({"type": "object"})
		}

		async fn execute(
			&self,
			args: &serde_json::Value,
			_ctx: &AgentContext,
		) -> Result<crate::state::ToolResult, ToolError> {
			if args.get("op").and_then(serde_json::Value::as_str) == Some("start") {
				if let Some(path) = &self.started {
					let _ = std::fs::write(path.as_path(), "1");
				}
				tokio::time::sleep(Duration::from_millis(30)).await;
				self.done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			}
			Ok(crate::state::ToolResult {
				tool_call_id: String::new(),
				content: "hub start".to_string(),
				is_error: false,
				terminate: false,
				details: None,
			})
		}
	}

	/// Records how many hub `start`s had finished when it was allowed to run.
	struct StartOrderProbe {
		done:     Arc<std::sync::atomic::AtomicUsize>,
		observed: Arc<std::sync::atomic::AtomicUsize>,
	}

	#[async_trait]
	impl crate::tool::Tool for StartOrderProbe {
		fn name(&self) -> &str {
			"probe"
		}

		fn description(&self) -> &str {
			"start-order probe"
		}

		fn schema(&self) -> serde_json::Value {
			json!({"type": "object"})
		}

		async fn execute(
			&self,
			_args: &serde_json::Value,
			_ctx: &AgentContext,
		) -> Result<crate::state::ToolResult, ToolError> {
			let done = self.done.load(std::sync::atomic::Ordering::SeqCst);
			self.observed.store(done, std::sync::atomic::Ordering::SeqCst);
			Ok(crate::state::ToolResult {
				tool_call_id: String::new(),
				content: format!("saw {done} starts"),
				is_error: false,
				terminate: false,
				details: None,
			})
		}
	}

	fn probe_call(index: usize, sleep_ms: u64) -> ToolCall {
		probe_call_with(index, sleep_ms, false)
	}

	fn probe_call_with(index: usize, sleep_ms: u64, fail: bool) -> ToolCall {
		ToolCall {
			id: format!("call_{index}"),
			name: "probe".to_string(),
			arguments: json!({"sleep_ms": sleep_ms, "fail": fail}),
		}
	}

	#[tokio::test]
	async fn concurrent_group_never_exceeds_max_concurrent() {
		use std::sync::atomic::Ordering;
		let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let scheduler = probe_scheduler(2, &active, &peak);

		let calls: Vec<ToolCall> = (0..6).map(|i| probe_call(i, 10)).collect();
		let results = scheduler
			.execute_batch(&calls, &AgentContext::new(temp_dir("max-concurrent")))
			.await;

		assert_eq!(results.len(), calls.len());
		let observed = peak.load(Ordering::SeqCst);
		assert!(observed <= 2, "peak {observed} exceeded max_concurrent 2");
		assert_eq!(observed, 2, "the semaphore should still allow two overlapping tools");
		assert_eq!(active.load(Ordering::SeqCst), 0, "every probe released its slot");
	}

	#[tokio::test]
	async fn max_concurrent_zero_is_clamped_to_one() {
		use std::sync::atomic::Ordering;
		let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let scheduler = probe_scheduler(0, &active, &peak);

		let calls: Vec<ToolCall> = (0..3).map(|i| probe_call(i, 5)).collect();
		let results = scheduler
			.execute_batch(&calls, &AgentContext::new(temp_dir("clamped")))
			.await;

		assert_eq!(results.len(), 3);
		assert_eq!(peak.load(Ordering::SeqCst), 1, "a zero limit must clamp to one");
	}

	#[test]
	fn with_max_concurrent_clamps_to_the_semaphore_limits() {
		let clamped = |limit: usize| {
			ToolScheduler::new(ToolRegistry::new()).with_max_concurrent(limit).max_concurrent
		};
		assert_eq!(
			clamped(usize::MAX),
			tokio::sync::Semaphore::MAX_PERMITS,
			"the semaphore constructor panics above MAX_PERMITS"
		);
		assert_eq!(clamped(3), 3);
	}

	#[tokio::test]
	async fn hub_wait_does_not_starve_its_unblocking_send() {
		let mut registry = ToolRegistry::new();
		registry.register(Arc::new(HubGate { gate: Arc::new(tokio::sync::Notify::new()) }));
		// One permit would deadlock if the wait held it: the send that releases
		// the wait only runs after the batch's later calls get a turn.
		let scheduler = ToolScheduler::new(registry).with_max_concurrent(1);
		let calls = vec![hub_call("call_wait", "wait"), hub_call("call_send", "send")];

		let results = tokio::time::timeout(
			Duration::from_secs(5),
			scheduler.execute_batch(&calls, &AgentContext::new(temp_dir("hub-exemption"))),
		)
		.await
		.expect("hub wait must not starve the send that unblocks it");

		let ids: Vec<&str> = results.iter().map(|result| result.tool_call_id.as_str()).collect();
		assert_eq!(ids, ["call_wait", "call_send"]);
		assert!(results.iter().all(|result| !result.is_error));
	}

	#[tokio::test]
	async fn hub_start_stays_bounded_by_max_concurrent() {
		use std::sync::atomic::{AtomicUsize, Ordering};
		let done = Arc::new(AtomicUsize::new(0));
		let observed = Arc::new(AtomicUsize::new(0));
		let mut registry = ToolRegistry::new();
		registry.register(Arc::new(HubStart { done: Arc::clone(&done), started: None }));
		registry.register(Arc::new(StartOrderProbe {
			done:     Arc::clone(&done),
			observed: Arc::clone(&observed),
		}));
		let scheduler = ToolScheduler::new(registry).with_max_concurrent(1);

		let calls = vec![hub_call("call_start", "start"), probe_call(0, 0)];
		let results = tokio::time::timeout(
			Duration::from_secs(5),
			scheduler.execute_batch(&calls, &AgentContext::new(temp_dir("hub-start-bounded"))),
		)
		.await
		.expect("a work-starting hub op must finish, not deadlock");

		assert_eq!(results.len(), 2);
		assert_eq!(
			observed.load(Ordering::SeqCst),
			1,
			"hub start must hold the only permit until it finishes"
		);
	}

	#[test]
	fn hub_permit_exemption_covers_only_observe_and_unblock_ops() {
		for op in [
			"send", "wait", "inbox", "list", "jobs", "cancel", "ps", "logs", "stop", "describe",
		] {
			assert!(hub_op_is_control(&json!({"op": op})), "op {op} must skip the permit");
		}
		for op in ["start", "restart", "spawn", "unknown"] {
			assert!(!hub_op_is_control(&json!({"op": op})), "op {op} must stay bounded");
		}
		// `HubTool` trims the op, so padded spellings must classify identically.
		for op in [" wait ", " send ", "  jobs  "] {
			assert!(hub_op_is_control(&json!({"op": op})), "padded op {op:?} must skip the permit");
		}
		for op in [" start ", " restart "] {
			assert!(!hub_op_is_control(&json!({"op": op})), "padded op {op:?} must stay bounded");
		}
		assert!(!hub_op_is_control(&json!({})), "a missing op must stay bounded");
		assert!(!hub_op_is_control(&json!({"op": 1})), "a non-string op must stay bounded");
	}

	#[cfg(unix)]
	#[tokio::test]
	async fn pre_tool_use_rewrite_decides_the_hub_permit() {
		use std::sync::atomic::{AtomicUsize, Ordering};

		use crate::hooks::{HookConfig, HookEngine, HookEvent, HookInvocation, HookRule};

		let dir = temp_dir("hook-rewrite");
		let ready = dir.join("hook.ready");
		let started = dir.join("tool.started");
		let done = Arc::new(AtomicUsize::new(0));
		let observed = Arc::new(AtomicUsize::new(0));
		let mut registry = ToolRegistry::new();
		registry.register(Arc::new(HubStart {
			done:    Arc::clone(&done),
			started: Some(Arc::new(started.clone())),
		}));
		registry.register(Arc::new(StartOrderProbe {
			done:     Arc::clone(&done),
			observed: Arc::clone(&observed),
		}));
		let mut scheduler = ToolScheduler::new(registry).with_max_concurrent(1);
		// The hub hook rewrites the exempt `wait` into work-starting `start`
		// and drops `hook.ready` only after writing its reply. The probe hook
		// waits for that file, then for `tool.started`, which the hub double
		// writes only after the scheduler acquired the single permit. That
		// closes the child-reap race: the probe cannot reach `acquire_owned`
		// before the rewritten `start` holds the permit. Evaluating the
		// pre-hook arguments would let the probe run first (observed == 0).
		let hub_hook = HookRule {
			event:      HookEvent::PreToolUse,
			matcher:    Some("hub".to_string()),
			invocation: HookInvocation::Shell(format!(
				r#"cat >/dev/null; printf '%s' '{{"hookSpecificOutput":{{"updatedInput":{{"op":"start"}}}}}}'; : > '{}'"#,
				ready.display()
			)),
			timeout_ms: 2_000,
			fail_closed: true,
		};
		let probe_hook = HookRule {
			event:      HookEvent::PreToolUse,
			matcher:    Some("probe".to_string()),
			invocation: HookInvocation::Shell(format!(
				r#"cat >/dev/null; i=0; while [ ! -e '{}' ] && [ "$i" -lt 200 ]; do i=$((i+1)); sleep 0.01; done; j=0; while [ ! -e '{}' ] && [ "$j" -lt 200 ]; do j=$((j+1)); sleep 0.01; done"#,
				ready.display(),
				started.display()
			)),
			timeout_ms: 2_000,
			fail_closed: false,
		};
		scheduler.set_hooks(Some(Arc::new(HookEngine::new(
			HookConfig::new(vec![hub_hook, probe_hook]),
			"scheduler-hook-test",
			dir.clone(),
		))));

		let calls = vec![hub_call("call_rewritten", "wait"), probe_call(0, 0)];
		let results = tokio::time::timeout(
			Duration::from_secs(5),
			scheduler.execute_batch(&calls, &AgentContext::new(dir)),
		)
		.await
		.expect("the rewritten start must finish, not deadlock");

		assert_eq!(results.len(), 2);
		assert_eq!(results[0].tool_call_id, "call_rewritten");
		assert_eq!(
			observed.load(Ordering::SeqCst),
			1,
			"the post-hook `start` must hold the permit, not the pre-hook `wait`"
		);
	}

	#[tokio::test]
	async fn cancellation_while_queued_for_a_permit_never_starts_the_effect() {
		use std::sync::atomic::Ordering;
		let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let scheduler = probe_scheduler(1, &active, &peak);
		let calls = vec![probe_call(0, 200), probe_call(1, 0)];

		let token = CancellationToken::new();
		let ctx = AgentContext::with_cancellation(temp_dir("permit-cancel"), token.clone());
		let batch = tokio::spawn(async move { scheduler.execute_batch(&calls, &ctx).await });
		tokio::time::sleep(Duration::from_millis(50)).await;
		token.cancel();

		let results = tokio::time::timeout(Duration::from_secs(5), batch)
			.await
			.expect("a cancelled queued call must settle")
			.expect("join");
		assert_eq!(results.len(), 2);
		assert_eq!(results[0].tool_call_id, "call_0");
		assert_eq!(results[1].tool_call_id, "call_1");
		assert!(!results[0].is_error, "the in-flight call completes normally");
		assert!(results[1].is_error, "a cancelled queued call must not run");
		assert!(
			results[1].content.contains("aborted before start"),
			"{}",
			results[1].content
		);
		assert_eq!(peak.load(Ordering::SeqCst), 1);
	}

	#[tokio::test]
	async fn concurrent_errors_and_cancellations_keep_call_order() {
		let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let scheduler = probe_scheduler(2, &active, &peak);

		// Slow failures finish after fast successes: the error flag must stay
		// on its own index instead of following completion order.
		let calls = vec![
			probe_call_with(0, 30, true),
			probe_call_with(1, 0, true),
			probe_call(2, 20),
			probe_call(3, 0),
		];
		let results = scheduler
			.execute_batch(&calls, &AgentContext::new(temp_dir("error-order")))
			.await;
		let ids: Vec<&str> = results.iter().map(|result| result.tool_call_id.as_str()).collect();
		let expected: Vec<&str> = calls.iter().map(|call| call.id.as_str()).collect();
		assert_eq!(ids, expected);
		assert_eq!(
			results.iter().map(|result| result.is_error).collect::<Vec<_>>(),
			[true, true, false, false]
		);

		// A batch cancelled before start still yields one aborted result per
		// call, in call order.
		let token = CancellationToken::new();
		token.cancel();
		let ctx = AgentContext::with_cancellation(temp_dir("cancel-order"), token);
		let results = scheduler.execute_batch(&calls, &ctx).await;
		let ids: Vec<&str> = results.iter().map(|result| result.tool_call_id.as_str()).collect();
		assert_eq!(ids, expected);
		assert!(results.iter().all(|result| result.is_error));
		assert!(results.iter().all(|result| result.content.contains("aborted before start")));
	}

	#[tokio::test]
	async fn concurrent_results_keep_call_order_when_earlier_calls_are_slower() {
		let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let scheduler = probe_scheduler(DEFAULT_MAX_CONCURRENT_TOOLS, &active, &peak);

		// Descending sleeps: later calls finish first, so completion order is
		// the reverse of call order. Results must still be call-ordered.
		let sleeps = [40, 30, 20, 10, 0];
		let calls: Vec<ToolCall> =
			sleeps.iter().enumerate().map(|(i, ms)| probe_call(i, *ms)).collect();
		let results = scheduler
			.execute_batch(&calls, &AgentContext::new(temp_dir("order")))
			.await;

		let ids: Vec<&str> = results.iter().map(|result| result.tool_call_id.as_str()).collect();
		let expected: Vec<&str> = calls.iter().map(|call| call.id.as_str()).collect();
		assert_eq!(ids, expected);
	}
}
