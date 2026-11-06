//! `security_scan` model-facing tool: five local actions.
//!
//! Anchors: `packages/coding-agent/src/tools/security-scan.ts`.
//! Cloud actions remain in the documented schema but are unavailable until the
//! paused C8 cloud client lands; they return an explicit capability error and
//! never fall back to a native scan.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use super::{
	coordinator::{SecurityCoordinator, SecurityPreflightInput},
	host::SecurityHost,
	preflight::SecurityTargetRequest,
	types::{SecurityEvidence, SecurityScanPlan},
};

pub const SECURITY_SCAN_TOOL_NAME: &str = "security_scan";
pub const SECURITY_DISABLED_TOOL: &str =
	"Security is disabled. Enable security.enabled before using security_scan.";
pub const CLOUD_PAUSED: &str =
	"Codex Security cloud actions are not available in the native runtime (cloud support is paused).";

const DESCRIPTION: &str =
	include_str!("../../../../packages/coding-agent/src/prompts/tools/security-scan.md");

#[must_use]
pub fn security_scan_schema() -> Value {
	json!({
		"type": "object",
		"additionalProperties": false,
		"required": ["action"],
		"properties": {
			"action": {"enum": [
				"preflight", "start", "status", "cancel", "validate",
				"cloud_scans", "cloud_start", "cloud_status", "cloud_pull"
			]},
			"plan_id": {"type": "string"},
			"operation_id": {"type": "string"},
			"target_kind": {"enum": ["repository", "scoped_path", "ref_diff", "working_tree"]},
			"include_paths": {"type": "array", "items": {"type": "string"}},
			"exclude_paths": {"type": "array", "items": {"type": "string"}},
			"base_revision": {"type": "string"},
			"head_revision": {"type": "string"},
			"knowledge_base_paths": {"type": "array", "items": {"type": "string"}},
			"output_root": {"type": "string"},
			"archive_existing": {"type": "boolean"},
			"credential_id": {"type": "integer", "minimum": 1},
			"scan_id": {"type": "string"},
			"finding_id": {"type": "string"},
			"validation_status": {"enum": ["unvalidated", "validated", "rejected", "partial", "error"]},
			"validation_summary": {"type": "string"},
			"validation_evidence": {
				"type": "array",
				"items": {
					"type": "object",
					"additionalProperties": false,
					"required": ["label", "explanation"],
					"properties": {
						"label": {"type": "string", "minLength": 1},
						"explanation": {"type": "string"}
					}
				}
			},
			"cloud_configuration_id": {"type": "string"},
			"repository_id": {"type": "string"},
			"repository_url": {"type": "string"},
			"environment_id": {"type": "string"},
			"lookback_days": {"type": "integer", "minimum": 1}
		}
	})
}

pub struct SecurityScanTool {
	host: Arc<dyn SecurityHost>,
	coordinator: Arc<SecurityCoordinator>,
}

impl SecurityScanTool {
	#[must_use]
	pub fn new(host: Arc<dyn SecurityHost>) -> Self {
		let coordinator = Arc::new(SecurityCoordinator::new(Arc::clone(&host)));
		Self { host, coordinator }
	}

	/// Test/embedder seam: share an existing coordinator.
	#[must_use]
	pub fn with_coordinator(host: Arc<dyn SecurityHost>, coordinator: Arc<SecurityCoordinator>) -> Self {
		Self { host, coordinator }
	}

	fn target_from_params(params: &Value) -> crate::Result<SecurityTargetRequest> {
		let include_paths = string_array(params, "include_paths");
		let exclude_paths = string_array(params, "exclude_paths");
		match optional_str(params, "target_kind").as_deref().unwrap_or("repository") {
			"scoped_path" => {
				if !include_paths.iter().any(|value| !value.trim().is_empty()) {
					return Err(crate::Error::new(
						"scoped_path security scans require at least one include path",
					));
				}
				Ok(SecurityTargetRequest::ScopedPath { include_paths, exclude_paths })
			},
			"working_tree" => {
				Ok(SecurityTargetRequest::WorkingTree { include_paths, exclude_paths })
			},
			"ref_diff" => {
				let base = optional_str(params, "base_revision").unwrap_or_default();
				let head = optional_str(params, "head_revision").unwrap_or_default();
				if base.is_empty() || head.is_empty() {
					return Err(crate::Error::new(
						"ref_diff preflight requires base_revision and head_revision",
					));
				}
				Ok(SecurityTargetRequest::RefDiff {
					base_revision: base,
					head_revision: head,
					include_paths,
					exclude_paths,
				})
			},
			_ => Ok(SecurityTargetRequest::Repository { include_paths, exclude_paths }),
		}
	}

	fn action(&self, params: &Value) -> crate::Result<(String, Value)> {
		let action = required_str(params, "action")?;
		match action.as_str() {
			"preflight" => {
				let input = SecurityPreflightInput {
					target: Self::target_from_params(params)?,
					knowledge_base_paths: string_array(params, "knowledge_base_paths"),
					output_root: optional_str(params, "output_root").map(std::path::PathBuf::from),
					archive_existing: params
						.get("archive_existing")
						.and_then(Value::as_bool)
						.unwrap_or(false),
					credential_id: params
						.get("credential_id")
						.and_then(Value::as_i64)
						.filter(|id| *id >= 1),
					model: None,
					thinking_level: None,
				};
				let plan = self.coordinator.preflight(input)?;
				let text = format!(
					"Security plan {} is ready. Fingerprint: {}. Start it with action=start and plan_id={}.",
					plan.id, plan.fingerprint, plan.id
				);
				Ok((
					text,
					json!({
						"action": "preflight",
						"plan": {"id": plan.id, "fingerprint": plan.fingerprint}
					}),
				))
			},
			"start" => {
				let plan_id = required_str(params, "plan_id")?;
				let operation = self.coordinator.start(&plan_id)?;
				Ok((
					format!(
						"Security scan {} started as {}.",
						operation.scan_id, operation.operation_id
					),
					json!({"action": "start", "operation": snapshot_json(&operation)}),
				))
			},
			"status" => {
				let operation_id = required_str(params, "operation_id")?;
				let operation = self
					.coordinator
					.status(&operation_id)?
					.ok_or_else(|| {
						crate::Error::new(format!("Unknown security operation: {operation_id}"))
					})?;
				Ok((
					format!(
						"Security scan {}: {}; {} finding(s).",
						operation.scan_id, operation.phase, operation.finding_count
					),
					json!({"action": "status", "operation": snapshot_json(&operation)}),
				))
			},
			"cancel" => {
				let operation_id = required_str(params, "operation_id")?;
				let cancelled = self.coordinator.cancel(&operation_id)?;
				let operation = self.coordinator.status(&operation_id)?;
				let text = if cancelled {
					format!("Cancellation requested for {operation_id}.")
				} else {
					format!("No running operation {operation_id}.")
				};
				let mut details = json!({"action": "cancel", "cancelled": cancelled});
				if let Some(operation) = operation {
					details["operation"] = snapshot_json(&operation);
				}
				Ok((text, details))
			},
			"validate" => {
				let scan_id = required_str(params, "scan_id")?;
				let finding_id = required_str(params, "finding_id")?;
				let status = required_str(params, "validation_status")?;
				let summary = required_str(params, "validation_summary")?;
				// `getFinding` semantics: an unknown scan surfaces as an unknown
				// finding, exactly like the TS tool.
				let evidence: Vec<SecurityEvidence> = params
					.get("validation_evidence")
					.and_then(Value::as_array)
					.map(Vec::as_slice)
					.unwrap_or(&[])
					.iter()
					.map(|item| -> crate::Result<SecurityEvidence> {
						Ok(SecurityEvidence {
							id: String::new(),
							kind: "validation".to_string(),
							label: required_str(item, "label")?,
							explanation: optional_str(item, "explanation").unwrap_or_default(),
							location: None,
							excerpt: None,
						})
					})
					.collect::<crate::Result<Vec<_>>>()?;
				let updated =
					self.coordinator
						.validate(&scan_id, &finding_id, &status, &summary, &evidence)?;
				Ok((
					format!(
						"Finding {} validation is now {}.",
						updated.id, updated.validation.status
					),
					json!({
						"action": "validate",
						"finding": {"id": updated.id, "validationStatus": updated.validation.status}
					}),
				))
			},
			"cloud_scans" | "cloud_start" | "cloud_status" | "cloud_pull" => {
				Err(crate::Error::new(CLOUD_PAUSED))
			},
			other => Err(crate::Error::new(format!("Unknown security action: {other}"))),
		}
	}
}

#[async_trait]
impl pi_agent::tools::Tool for SecurityScanTool {
	fn name(&self) -> &str {
		SECURITY_SCAN_TOOL_NAME
	}

	fn description(&self) -> &str {
		DESCRIPTION.trim()
	}

	fn schema(&self) -> Value {
		security_scan_schema()
	}

	async fn execute(
		&self,
		args: &Value,
		_ctx: &pi_agent::state::AgentContext,
	) -> Result<pi_agent::state::ToolResult, pi_agent::tools::ToolError> {
		if !self.host.security_enabled() {
			return Err(pi_agent::tools::ToolError::Rejected(
				SECURITY_DISABLED_TOOL.to_string(),
			));
		}
		let (text, details) = self
			.action(args)
			.map_err(|error| pi_agent::tools::ToolError::ExecutionFailed(error.to_string()))?;
		Ok(pi_agent::state::ToolResult {
			tool_call_id: String::new(),
			content: text,
			is_error: false,
			terminate: false,
			details: Some(details),
		})
	}
}

fn snapshot_json(snapshot: &super::coordinator::SecurityOperationSnapshot) -> Value {
	json!({
		"operationId": snapshot.operation_id,
		"planId": snapshot.plan_id,
		"scanId": snapshot.scan_id,
		"phase": snapshot.phase,
		"createdAt": snapshot.created_at,
		"updatedAt": snapshot.updated_at,
		"findingCount": snapshot.finding_count,
		"sessionFile": snapshot.session_file,
		"error": snapshot.error,
	})
}

fn required_str(value: &Value, field: &str) -> crate::Result<String> {
	let text = value
		.get(field)
		.and_then(Value::as_str)
		.ok_or_else(|| crate::Error::new(format!("{field} is required for this action")))?;
	if text.trim().is_empty() {
		return Err(crate::Error::new(format!("{field} is required for this action")));
	}
	Ok(text.trim().to_string())
}

fn optional_str(value: &Value, field: &str) -> Option<String> {
	value.get(field).and_then(Value::as_str).map(str::to_string)
}

fn string_array(value: &Value, field: &str) -> Vec<String> {
	value
		.get(field)
		.and_then(Value::as_array)
		.map(|items| {
			items
				.iter()
				.filter_map(Value::as_str)
				.map(str::to_string)
				.collect()
		})
		.unwrap_or_default()
}

/// Plan projection for hosts that need it (tests/diagnostics).
#[must_use]
pub fn plan_summary(plan: &SecurityScanPlan) -> Value {
	json!({"id": plan.id, "fingerprint": plan.fingerprint})
}
