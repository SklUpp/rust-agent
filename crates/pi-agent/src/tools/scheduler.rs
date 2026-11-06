use std::sync::Arc;

use futures::future::join_all;

use super::registry::{ToolRegistry, excluded_tool_reason};
use crate::durable::{DURABILITY_FAILURE_CODE, DurableBoundary};
use crate::tool::ToolError;
use crate::state::{AgentContext, ToolApprover, ToolCall, ToolResult};
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
}

impl ToolScheduler {
	pub fn new(registry: ToolRegistry) -> Self {
		Self {
			registry,
			approver: None,
			hidden: std::sync::Mutex::new(Vec::new()),
			durable: None,
		}
	}

	pub fn with_approver(registry: ToolRegistry, approver: Arc<dyn ToolApprover>) -> Self {
		Self {
			registry,
			approver: Some(approver),
			hidden: std::sync::Mutex::new(Vec::new()),
			durable: None,
		}
	}

	pub fn approver(&self) -> Option<&Arc<dyn ToolApprover>> {
		self.approver.as_ref()
	}

	/// Install or clear the host durability boundary. The loop attaches this
	/// when the session owner supplies a store.
	pub fn set_durable(&mut self, durable: Option<Arc<dyn DurableBoundary>>) {
		self.durable = durable;
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
			results.push(self.run_single(call, ctx).await);
		}

		if !concurrent.is_empty() {
			results.extend(self.run_concurrent(&concurrent, ctx).await);
		}
		results
	}

	async fn run_single(&self, call: &ToolCall, ctx: &AgentContext) -> ToolResult {
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

		let approver = self.approver.as_ref().or(ctx.approver.as_ref());
		if let Some(approver) = approver {
			// A pending prompt must not outlive the turn: dropping this future
			// aborts the approver's blocking read, and no approved-but-cancelled
			// call reaches `execute`.
			let decision = tokio::select! {
				biased;
				decision = approver.approve(&call.name, &call.arguments) => decision,
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

		let execution_id = match &self.durable {
			Some(durable) => match durable.begin_execution(call).await {
				Ok(id) => Some(id),
				Err(error) => {
					// Admission failed: the effect must not start. The durable
					// intent row is the only evidence; there is nothing to replay.
					return durable_failure_result(
						&call.id,
						format!(
							"tool not started: durable intent write failed: {error}"
						),
					);
				},
			},
			None => None,
		};

		// The durable admission await may have raced a cancel (or a policy
		// change observed by the caller): recheck before any effect starts.
		if ctx.cancellation_token.is_cancelled() {
			let aborted = ToolResult {
				tool_call_id: call.id.clone(),
				content: "Tool execution aborted before start".to_string(),
				is_error: true,
				terminate: false,
				details: None,
			};
			if let (Some(durable), Some(execution_id)) = (&self.durable, execution_id.as_deref())
				&& let Err(error) = durable.finish_execution(execution_id, call, &aborted).await
			{
				return durable_failure_result(
					&call.id,
					format!("tool cancellation outcome could not be persisted: {error}"),
				);
			}
			return aborted;
		}

		let result = match tool.execute_with_call_id(&call.id, &call.arguments, ctx).await {
			Ok(result) => result,
			Err(err) => tool_error_result(&call.id, err),
		};
		let result = ToolResult { tool_call_id: call.id.clone(), ..result };
		if let (Some(durable), Some(execution_id)) = (&self.durable, execution_id.as_deref())
			&& let Err(error) = durable.finish_execution(execution_id, call, &result).await
		{
			// The effect may already have run. The intent stays unresolved; the
			// loop terminates without another model request and never retries.
			return durable_failure_result(
				&call.id,
				format!("tool effect outcome could not be persisted: {error}"),
			);
		}
		result
	}

	async fn run_concurrent(&self, calls: &[&ToolCall], ctx: &AgentContext) -> Vec<ToolResult> {
		join_all(calls.iter().map(|call| self.run_single(call, ctx))).await
	}
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

/// Terminal result for a durability failure. The loop detects the marker and
/// converts the batch into [`crate::AgentError::Durable`] before any further
/// model request; `terminate` also stops a host that only reads results.
fn durable_failure_result(tool_call_id: &str, message: String) -> ToolResult {
	ToolResult {
		tool_call_id: tool_call_id.to_string(),
		content: message,
		is_error: true,
		terminate: true,
		details: Some(serde_json::json!({
			"code": DURABILITY_FAILURE_CODE,
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
}
