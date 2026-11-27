//! Neutral tool contract.
//!
//! Tool implementations live under [`crate::tools`]; this module holds only the
//! execution interface and its error/result helpers so the loop, the scheduler
//! and host adapters do not depend on the builtin tool tree.

use async_trait::async_trait;
use serde_json::{Map, Value};

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

/// Borrow the arguments object, rejecting non-object tool arguments.
pub(crate) fn args_object(args: &Value) -> Result<&Map<String, Value>, ToolError> {
	args
		.as_object()
		.ok_or_else(|| ToolError::InvalidArguments("Arguments must be an object".to_string()))
}

/// Require a string parameter; an empty string still counts as present.
pub(crate) fn require_str<'a>(
	map: &'a Map<String, Value>,
	key: &str,
) -> Result<&'a str, ToolError> {
	map.get(key)
		.and_then(Value::as_str)
		.ok_or_else(|| ToolError::InvalidArguments(format!("Missing required parameter '{key}'")))
}

/// Require a string parameter whose trimmed value is non-blank.
pub(crate) fn require_non_empty_str<'a>(
	map: &'a Map<String, Value>,
	key: &str,
) -> Result<&'a str, ToolError> {
	let value = require_str(map, key)?.trim();
	if value.is_empty() {
		return Err(ToolError::InvalidArguments(format!("Parameter '{key}' must not be empty")));
	}
	Ok(value)
}

/// Reject the first key outside `allowed`, naming it and the accepted keys.
pub(crate) fn reject_unknown(map: &Map<String, Value>, allowed: &[&str]) -> Result<(), ToolError> {
	let Some(key) = map.keys().find(|key| !allowed.contains(&key.as_str())) else {
		return Ok(());
	};
	Err(ToolError::InvalidArguments(format!(
		"Unknown parameter '{key}': accepted keys are {}",
		allowed.join(", ")
	)))
}

/// Abort the run with [`ToolError::Aborted`], the house cancellation signal.
pub(crate) fn aborted_result(message: impl Into<String>) -> Result<ToolResult, ToolError> {
	Err(ToolError::Aborted(message.into()))
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	#[test]
	fn args_object_accepts_objects_only() {
		let args = json!({ "a": 1 });
		assert_eq!(args_object(&args).expect("object").get("a"), Some(&json!(1)));
		let error = args_object(&json!([1])).expect_err("array");
		assert!(
			matches!(error, ToolError::InvalidArguments(message) if message == "Arguments must be an object")
		);
	}

	#[test]
	fn require_str_separates_missing_from_empty() {
		let map = json!({ "empty": "", "text": "x", "number": 3 });
		let map = map.as_object().expect("object");
		assert_eq!(require_str(map, "text").expect("text"), "x");
		assert_eq!(require_str(map, "empty").expect("present empty"), "");
		let missing = require_str(map, "absent").expect_err("missing");
		assert!(
			matches!(missing, ToolError::InvalidArguments(message) if message == "Missing required parameter 'absent'")
		);
		let wrong_type = require_str(map, "number").expect_err("wrong type");
		assert!(matches!(wrong_type, ToolError::InvalidArguments(_)));
	}

	#[test]
	fn require_non_empty_str_rejects_blank_values() {
		let map = json!({ "blank": "   ", "text": " x " });
		let map = map.as_object().expect("object");
		assert_eq!(require_non_empty_str(map, "text").expect("text"), "x");
		let blank = require_non_empty_str(map, "blank").expect_err("blank");
		assert!(
			matches!(blank, ToolError::InvalidArguments(message) if message == "Parameter 'blank' must not be empty")
		);
		assert!(require_non_empty_str(map, "absent").is_err());
	}

	#[test]
	fn reject_unknown_names_the_offending_key() {
		let map = json!({ "thoughts": "draft", "extra": true });
		let map = map.as_object().expect("object");
		assert!(reject_unknown(map, &["thoughts"]).is_err_and(|error| {
			matches!(error, ToolError::InvalidArguments(message) if message.contains("'extra'"))
		}));
		let map = json!({ "thoughts": "draft" });
		let map = map.as_object().expect("object");
		reject_unknown(map, &["thoughts"]).expect("known key");
	}

	#[test]
	fn aborted_result_is_an_aborted_error() {
		let error = aborted_result("cancelled").expect_err("aborted");
		assert!(matches!(error, ToolError::Aborted(message) if message == "cancelled"));
		let error = aborted_result(String::from("owned")).expect_err("aborted");
		assert!(matches!(error, ToolError::Aborted(message) if message == "owned"));
	}
}
