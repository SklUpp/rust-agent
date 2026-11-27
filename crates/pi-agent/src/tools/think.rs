//! Hidden `think` tool: records private scratchpad thoughts.
//!
//! Upstream registers it as a hidden tool so a model can keep reasoning notes
//! out of the visible transcript while native reasoning is disabled. The
//! result is intentionally opaque (`------`) and never shown to the user.

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::state::{AgentContext, ToolResult};
use crate::tool::{Tool, ToolError, args_object, reject_unknown, require_str};

pub struct ThinkTool;

impl ThinkTool {
	pub fn new() -> Self {
		Self
	}
}

impl Default for ThinkTool {
	fn default() -> Self {
		Self::new()
	}
}

#[async_trait]
impl Tool for ThinkTool {
	fn name(&self) -> &str {
		"think"
	}

	fn description(&self) -> &str {
		"private scratchpad; not shown to user"
	}

	fn schema(&self) -> Value {
		json!({
			"type": "object",
			"properties": {
				"thoughts": {
					"type": "string",
					"description": "private scratchpad; not shown to user"
				}
			},
			"required": ["thoughts"],
			"additionalProperties": false
		})
	}

	async fn execute(&self, args: &Value, _ctx: &AgentContext) -> Result<ToolResult, ToolError> {
		let obj = args_object(args)?;
		require_str(obj, "thoughts")?;
		reject_unknown(obj, &["thoughts"])?;
		Ok(ToolResult {
			tool_call_id: String::new(),
			content: "------".to_string(),
			is_error: false,
			terminate: false,
			details: Some(json!({ "recorded": true })),
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn records_only_thoughts_and_rejects_extra_args() {
		let tool = ThinkTool::new();
		let ctx = AgentContext::new(std::env::temp_dir());
		let result = tool
			.execute(&json!({ "thoughts": "draft" }), &ctx)
			.await
			.expect("think");
		assert_eq!(result.content, "------");
		assert_eq!(result.details, Some(json!({ "recorded": true })));
		let error = tool
			.execute(&json!({ "thoughts": "draft", "extra": true }), &ctx)
			.await;
		assert!(matches!(error, Err(ToolError::InvalidArguments(_))));
	}
}
