//! Neutral tool contract.
//!
//! Tool implementations live under [`crate::tools`]; this module holds only the
//! execution interface and its error/result helpers so the loop, the scheduler
//! and host adapters do not depend on the builtin tool tree.

use async_trait::async_trait;
use serde_json::Value;

use crate::state::{AgentContext, ToolResult};

#[derive(Debug, thiserror::Error)]
pub enum ToolError {
	#[error("Invalid arguments: {0}")]
	InvalidArguments(String),
	#[error("Execution failed: {0}")]
	ExecutionFailed(String),
	#[error("Operation aborted: {0}")]
	Aborted(String),
	/// Upstream `ToolError.render()` text, surfaced verbatim (no prefix).
	#[error("{0}")]
	Rejected(String),
}

/// Dynamic trait implemented by all agent tools.
#[async_trait]
pub trait Tool: Send + Sync {
	fn name(&self) -> &str;
	/// Model-facing name when it differs from the registry key (upstream
	/// `customWireName`); `None` advertises [`Tool::name`].
	fn wire_name(&self) -> Option<&str> {
		None
	}
	fn description(&self) -> &str;
	fn schema(&self) -> Value;
	fn is_exclusive(&self) -> bool {
		false
	}
	async fn execute(&self, args: &Value, ctx: &AgentContext) -> Result<ToolResult, ToolError>;
	/// Execute with the model's tool-call id.
	///
	/// Host-bridged tools need the real id (`host_tool_call.toolCallId`), but
	/// the scheduler's later result-id rewrite cannot repair a frame that was
	/// already emitted. Defaults to [`Tool::execute`], so every existing
	/// implementer is unchanged.
	async fn execute_with_call_id(
		&self,
		_id: &str,
		args: &Value,
		ctx: &AgentContext,
	) -> Result<ToolResult, ToolError> {
		self.execute(args, ctx).await
	}
}

pub(crate) fn tool_result(content: impl Into<String>, is_error: bool) -> ToolResult {
	ToolResult {
		tool_call_id: String::new(),
		content: content.into(),
		is_error,
		terminate: false,
		details: None,
	}
}
