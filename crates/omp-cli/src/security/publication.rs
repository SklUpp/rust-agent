//! `security_publish`: the scan session's once-only canonical result writer.
//!
//! Anchors: `packages/coding-agent/src/security/publication.ts`.
//!
//! The tool validates paths against the immutable plan scope, assigns OMP-owned
//! ids/fingerprints, writes the output directory and project store, then emits
//! SARIF. It accepts exactly one successful publication per scan; a failure
//! before the store commit re-opens the tool so the model may correct itself.

use std::{
	path::Path,
	sync::{Arc, Mutex},
};

use async_trait::async_trait;
use serde_json::{Map, Value, json};

use super::{
	ids::{
		create_security_evidence_id, create_security_finding_fingerprint, create_security_finding_id,
		create_security_occurrence_id,
	},
	preflight::path_matches_security_scope,
	provenance::{
		create_native_security_producer, create_native_security_provenance,
		create_security_credential_affinity,
	},
	sarif::export_security_bundle_to_sarif,
	store::{SecurityStore, write_security_bundle_to_directory},
	types::{
		self, SecurityCoverage, SecurityCoverageDeferred, SecurityCoverageSurface, SecurityEvidence,
		SecurityExplicitExclusion, SecurityFinding, SecurityLocation, SecurityOpenQuestion, SecurityScan,
		SecurityScanBundle, SecurityScanPlan,
	},
};

pub const SECURITY_PUBLISH_TOOL_NAME: &str = "security_publish";

const DESCRIPTION: &str = include_str!(
	"../../../../packages/coding-agent/src/prompts/tools/security-publish.md"
);

/// `securityPublishSchema` (snake_case tool arguments).
///
/// Nested pieces are built as locals: one `json!` literal this deep exceeds the
/// macro expansion budget, and the composed value is byte-identical.
#[must_use]
pub fn security_publish_schema() -> Value {
	let location = json!({
		"type": "object",
		"additionalProperties": false,
		"required": ["path", "start_line"],
		"properties": {
			"path": {"type": "string", "minLength": 1},
			"start_line": {"type": "integer", "minimum": 1},
			"end_line": {"type": "integer", "minimum": 1},
			"start_column": {"type": "integer", "minimum": 1},
			"end_column": {"type": "integer", "minimum": 1},
			"role": {"type": "string"}
		}
	});
	let evidence = json!({
		"type": "object",
		"additionalProperties": false,
		"required": ["label", "explanation"],
		"properties": {
			"label": {"type": "string", "minLength": 1},
			"explanation": {"type": "string"},
			"excerpt": {"type": "string"},
			"location": location.clone()
		}
	});
	let finding = json!({
		"type": "object",
		"additionalProperties": false,
		"required": ["rule_id", "title", "summary", "severity", "confidence", "category", "locations"],
		"properties": {
			"rule_id": {"type": "string", "minLength": 1},
			"title": {"type": "string", "minLength": 1},
			"summary": {"type": "string"},
			"severity": {"enum": ["critical", "high", "medium", "low", "informational"]},
			"confidence": {"enum": ["high", "medium", "low"]},
			"category": {"type": "string", "minLength": 1},
			"anchor": {"type": "string"},
			"cwe": {"type": "array", "items": {"type": "string"}},
			"locations": {"type": "array", "minItems": 1, "items": location},
			"evidence": {"type": "array", "items": evidence},
			"remediation": {"type": "string"},
			"validation": {"enum": ["unvalidated", "validated", "partial"]}
		}
	});
	let coverage = json!({
		"type": "object",
		"additionalProperties": false,
		"required": ["completeness"],
		"properties": {
			"completeness": {"enum": ["complete", "partial", "unknown"]},
			"surfaces": {"type": "array", "items": {"type": "object"}},
			"explicit_exclusions": {"type": "array", "items": {"type": "object"}},
			"deferred": {"type": "array", "items": {"type": "object"}},
			"open_questions": {"type": "array", "items": {"type": "object"}}
		}
	});
	json!({
		"type": "object",
		"additionalProperties": false,
		"required": ["findings", "coverage", "report"],
		"properties": {
			"findings": {"type": "array", "items": finding},
			"coverage": coverage,
			"report": {"type": "string"}
		}
	})
}

#[derive(Debug, Clone)]
pub struct SecurityPublishDetails {
	pub scan_id: String,
	pub finding_count: usize,
	pub status: String,
}

/// Options captured when the scan session starts.
pub struct SecurityPublisherOptions {
	pub plan: SecurityScanPlan,
	pub scan_id: String,
	pub store: Arc<SecurityStore>,
	pub started_at: String,
	pub session_id: Option<String>,
	pub operation_id: Option<String>,
	pub on_published: Option<Arc<dyn Fn(&SecurityScanBundle) + Send + Sync>>,
}

/// One publication attempt per scan. Holds the produced bundle for the
/// coordinator to attach metrics and re-persist after the session ends.
pub struct SecurityPublisher {
	plan: SecurityScanPlan,
	scan_id: String,
	store: Arc<SecurityStore>,
	started_at: String,
	session_id: Option<String>,
	operation_id: Option<String>,
	published: Mutex<bool>,
	bundle: Mutex<Option<SecurityScanBundle>>,
	on_published: Option<Arc<dyn Fn(&SecurityScanBundle) + Send + Sync>>,
}

impl SecurityPublisher {
	pub fn new(options: SecurityPublisherOptions) -> Self {
		Self {
			plan: options.plan,
			scan_id: options.scan_id,
			store: options.store,
			started_at: options.started_at,
			session_id: options.session_id,
			operation_id: options.operation_id,
			published: Mutex::new(false),
			bundle: Mutex::new(None),
			on_published: options.on_published,
		}
	}

	#[must_use]
	pub fn scan_id(&self) -> &str {
		&self.scan_id
	}

	/// `Some(bundle)` only after the store accepted the canonical result.
	#[must_use]
	pub fn published_bundle(&self) -> Option<SecurityScanBundle> {
		self.bundle.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
	}

	/// Build/persist one publication. Called by the tool; tests may call it with
	/// the same JSON shape the model sends.
	pub fn publish(&self, params: &Value) -> crate::Result<SecurityScanBundle> {
		let mut published = self
			.published
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		if *published {
			return Err(crate::Error::new(format!(
				"Security scan {} has already been published",
				self.scan_id
			)));
		}
		*published = true;
		drop(published);
		let mut persisted = false;
		let result = (|| -> crate::Result<SecurityScanBundle> {
			let completed_at = crate::output::iso8601(std::time::SystemTime::now());
			let findings_input = params
				.get("findings")
				.and_then(Value::as_array)
				.ok_or_else(|| crate::Error::new("findings must be an array"))?;
			let mut by_fingerprint: Vec<SecurityFinding> = Vec::new();
			for input in findings_input {
				let finding = self.build_finding(input, &completed_at)?;
				if !by_fingerprint
					.iter()
					.any(|existing| existing.fingerprint == finding.fingerprint)
				{
					by_fingerprint.push(finding);
				}
			}
			let findings = by_fingerprint;
			let provenance = create_native_security_provenance(
				&self.started_at,
				&self.plan.account,
				&self.plan.fingerprint,
				&self.plan.workflow_fingerprint,
				self.session_id.as_deref(),
				self.operation_id.as_deref(),
			);
			let scan = SecurityScan {
				document_type: "omp-security.scan".to_string(),
				schema_version: "1.0".to_string(),
				id: self.scan_id.clone(),
				project_key: self.store.project_key().to_string(),
				status: "completed".to_string(),
				created_at: self.plan.created_at.clone(),
				started_at: Some(self.started_at.clone()),
				completed_at: Some(completed_at),
				plan: Some(self.plan.clone()),
				target: self.plan.target.clone(),
				producer: create_native_security_producer(),
				provenance,
				finding_ids: findings.iter().map(|finding| finding.id.clone()).collect(),
				coverage: self.build_coverage(params)?,
				report_ref: Some("report.md".to_string()),
				sarif_ref: Some("results.sarif".to_string()),
				error: None,
				metrics: None,
			};
			let report = params
				.get("report")
				.and_then(Value::as_str)
				.ok_or_else(|| crate::Error::new("report must be a string"))?
				.to_string();
			let provisional = SecurityScanBundle { scan, findings, report: Some(report), sarif: None };
			let mut bundle = provisional;
			bundle.sarif = Some(export_security_bundle_to_sarif(&bundle));
			types::validate_bundle(&bundle)?;
			write_security_bundle_to_directory(Path::new(&self.plan.output.root), &bundle)?;
			self.store.put_bundle(&bundle)?;
			persisted = true;
			*self.bundle.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
				Some(bundle.clone());
			if let Some(callback) = &self.on_published {
				callback(&bundle);
			}
			Ok(bundle)
		})();
		if result.is_err() && !persisted {
			*self.published.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = false;
		}
		result
	}

	fn build_finding(&self, input: &Value, created_at: &str) -> crate::Result<SecurityFinding> {
		let rule_id = required_str(input, "rule_id")?;
		let title = required_str(input, "title")?;
		let summary = optional_str(input, "summary").unwrap_or_default();
		let severity = required_str(input, "severity")?;
		let confidence = required_str(input, "confidence")?;
		let category = required_str(input, "category")?;
		let anchor = optional_str(input, "anchor");
		let raw_locations = input
			.get("locations")
			.and_then(Value::as_array)
			.ok_or_else(|| crate::Error::new("locations must be an array"))?;
		if raw_locations.is_empty() {
			return Err(crate::Error::new("locations must contain at least one entry"));
		}
		let locations = raw_locations
			.iter()
			.map(|location| self.to_location(location))
			.collect::<crate::Result<Vec<_>>>()?;
		let fingerprint = create_security_finding_fingerprint(
			&rule_id,
			&category,
			anchor.as_deref(),
			&locations,
		);
		let evidence: Vec<SecurityEvidence> = input
			.get("evidence")
			.and_then(Value::as_array)
			.map(Vec::as_slice)
			.unwrap_or(&[])
			.iter()
			.enumerate()
			.map(|(index, item)| -> crate::Result<SecurityEvidence> {
				let label = required_str(item, "label")?;
				let explanation = optional_str(item, "explanation").unwrap_or_default();
				let location = item
					.get("location")
					.map(|location| self.to_location(location))
					.transpose()?;
				Ok(SecurityEvidence {
					id: create_security_evidence_id(&fingerprint, &label, index),
					kind: "code".to_string(),
					label,
					explanation,
					location,
					excerpt: optional_str(item, "excerpt"),
				})
			})
			.collect::<crate::Result<Vec<_>>>()?;
		let cwe: Vec<String> = input
			.get("cwe")
			.and_then(Value::as_array)
			.map(|items| {
				items
					.iter()
					.filter_map(Value::as_str)
					.map(str::to_string)
					.collect()
			})
			.unwrap_or_default();
		let validation_status = optional_str(input, "validation").unwrap_or_else(|| "unvalidated".to_string());
		let occurrence = super::types::SecurityOccurrence {
			id: create_security_occurrence_id(&fingerprint, &locations),
			locations: locations.clone(),
			evidence_ids: evidence.iter().map(|item| item.id.clone()).collect(),
		};
		Ok(SecurityFinding {
			id: create_security_finding_id(&fingerprint),
			scan_id: self.scan_id.clone(),
			fingerprint,
			rule_id,
			anchor,
			title,
			summary,
			severity: types::SecuritySeverity {
				level: severity,
				score: None,
				scoring_system: None,
				vector: None,
				rationale: None,
			},
			confidence: types::SecurityConfidence { level: confidence, rationale: None },
			taxonomy: types::SecurityTaxonomy { category, cwe, tags: None },
			occurrences: vec![occurrence],
			evidence,
			remediation: optional_str(input, "remediation"),
			validation: types::SecurityValidation {
				status: validation_status,
				summary: None,
				evidence_ids: Vec::new(),
				validated_at: None,
			},
			disposition: types::SecurityDisposition {
				status: "open".to_string(),
				rationale: None,
				updated_at: None,
				actor: None,
			},
			provenance: create_native_security_provenance(
				created_at,
				&self.plan.account,
				&self.plan.fingerprint,
				&self.plan.workflow_fingerprint,
				self.session_id.as_deref(),
				self.operation_id.as_deref(),
			),
			extensions: None,
		})
	}

	fn to_location(&self, input: &Value) -> crate::Result<SecurityLocation> {
		let raw_path = required_str(input, "path")?;
		let normalized = normalize_published_path(&raw_path)?;
		if !path_matches_security_scope(
			&normalized,
			&self.plan.target.include_paths,
			&self.plan.target.exclude_paths,
		) {
			return Err(crate::Error::new(format!(
				"Security finding path is outside the immutable scan scope: {raw_path}"
			)));
		}
		let start_line = required_u32(input, "start_line")?;
		Ok(SecurityLocation {
			path: normalized,
			start_line,
			end_line: optional_u32(input, "end_line"),
			start_column: optional_u32(input, "start_column"),
			end_column: optional_u32(input, "end_column"),
			role: optional_str(input, "role"),
		})
	}

	fn build_coverage(&self, params: &Value) -> crate::Result<SecurityCoverage> {
		let coverage = params
			.get("coverage")
			.and_then(Value::as_object)
			.ok_or_else(|| crate::Error::new("coverage must be an object"))?;
		let completeness = coverage
			.get("completeness")
			.and_then(Value::as_str)
			.ok_or_else(|| crate::Error::new("coverage.completeness is required"))?;
		let surfaces: Vec<SecurityCoverageSurface> = coverage
			.get("surfaces")
			.and_then(Value::as_array)
			.map(Vec::as_slice)
			.unwrap_or(&[])
			.iter()
			.enumerate()
			.map(|(index, surface)| -> crate::Result<SecurityCoverageSurface> {
				Ok(SecurityCoverageSurface {
					id: format!("surface-{}", index + 1),
					label: required_str(surface, "label")?,
					disposition: required_str(surface, "disposition")?,
					receipt_refs: string_array(surface, "receipt_refs"),
					risk_area: optional_str(surface, "risk_area"),
					notes: optional_str(surface, "notes"),
				})
			})
			.collect::<crate::Result<Vec<_>>>()?;
		let deferred: Vec<SecurityCoverageDeferred> = coverage
			.get("deferred")
			.and_then(Value::as_array)
			.map(Vec::as_slice)
			.unwrap_or(&[])
			.iter()
			.enumerate()
			.map(|(index, item)| -> crate::Result<SecurityCoverageDeferred> {
				Ok(SecurityCoverageDeferred {
					id: format!("deferred-{}", index + 1),
					reason: required_str(item, "reason")?,
					paths: item.get("paths").and_then(Value::as_array).map(|paths| {
						paths.iter().filter_map(Value::as_str).map(str::to_string).collect()
					}),
					surface_ids: item.get("surface_ids").and_then(Value::as_array).map(|ids| {
						ids.iter().filter_map(Value::as_str).map(str::to_string).collect()
					}),
				})
			})
			.collect::<crate::Result<Vec<_>>>()?;
		let explicit_exclusions: Vec<SecurityExplicitExclusion> = coverage
			.get("explicit_exclusions")
			.and_then(Value::as_array)
			.map(Vec::as_slice)
			.unwrap_or(&[])
			.iter()
			.map(|item| -> crate::Result<SecurityExplicitExclusion> {
				Ok(SecurityExplicitExclusion {
					pattern: required_str(item, "pattern")?,
					reason: required_str(item, "reason")?,
				})
			})
			.collect::<crate::Result<Vec<_>>>()?;
		let open_questions = coverage
			.get("open_questions")
			.and_then(Value::as_array)
			.map(|items| -> crate::Result<Vec<SecurityOpenQuestion>> {
				items
					.iter()
					.map(|item| -> crate::Result<SecurityOpenQuestion> {
						Ok(SecurityOpenQuestion {
							question: required_str(item, "question")?,
							follow_up_prompt: optional_str(item, "follow_up_prompt"),
						})
					})
					.collect()
			})
			.transpose()?;
		Ok(SecurityCoverage {
			mode: coverage_mode(&self.plan),
			completeness: completeness.to_string(),
			inventory_strategy: inventory_strategy(&self.plan),
			include_paths: self.plan.target.include_paths.clone(),
			exclude_paths: self.plan.target.exclude_paths.clone(),
			surfaces,
			explicit_exclusions,
			deferred,
			open_questions,
		})
	}
}

fn coverage_mode(plan: &SecurityScanPlan) -> String {
	match plan.target.kind.as_str() {
		"ref_diff" => "diff",
		"working_tree" => "working_tree",
		"scoped_path" => "scoped_path",
		_ => "repository",
	}
	.to_string()
}

fn inventory_strategy(plan: &SecurityScanPlan) -> String {
	match plan.target.kind.as_str() {
		"ref_diff" => "diff",
		"scoped_path" => "scoped_path",
		_ => "repository",
	}
	.to_string()
}

fn normalize_published_path(input: &str) -> crate::Result<String> {
	let normalized = input.replace('\\', "/");
	let normalized = normalized.strip_prefix("./").unwrap_or(&normalized).to_string();
	let segments: Vec<&str> = normalized.split('/').collect();
	if normalized.is_empty()
		|| normalized.starts_with('/')
		|| (normalized.len() >= 2 && normalized.as_bytes()[1] == b':')
		|| segments.iter().any(|segment| *segment == "..")
	{
		return Err(crate::Error::new(format!(
			"Security finding paths must be repository-relative: {input}"
		)));
	}
	Ok(normalized)
}

fn required_str(value: &Value, field: &str) -> crate::Result<String> {
	let text = value
		.get(field)
		.and_then(Value::as_str)
		.ok_or_else(|| crate::Error::new(format!("{field} must be a string")))?;
	if text.is_empty() {
		return Err(crate::Error::new(format!("{field} must not be empty")));
	}
	Ok(text.to_string())
}

fn optional_str(value: &Value, field: &str) -> Option<String> {
	value.get(field).and_then(Value::as_str).map(str::to_string)
}

fn required_u32(value: &Value, field: &str) -> crate::Result<u32> {
	let number = value
		.get(field)
		.and_then(Value::as_u64)
		.ok_or_else(|| crate::Error::new(format!("{field} must be a positive integer")))?;
	u32::try_from(number)
		.ok()
		.filter(|number| *number >= 1)
		.ok_or_else(|| crate::Error::new(format!("{field} must be a positive integer")))
}

fn optional_u32(value: &Value, field: &str) -> Option<u32> {
	value
		.get(field)
		.and_then(Value::as_u64)
		.and_then(|number| u32::try_from(number).ok())
		.filter(|number| *number >= 1)
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

/// Model-facing tool wrapper.
pub struct SecurityPublishTool {
	publisher: Arc<SecurityPublisher>,
}

impl SecurityPublishTool {
	#[must_use]
	pub fn new(publisher: Arc<SecurityPublisher>) -> Self {
		Self { publisher }
	}
}

#[async_trait]
impl pi_agent::tools::Tool for SecurityPublishTool {
	fn name(&self) -> &str {
		SECURITY_PUBLISH_TOOL_NAME
	}

	fn description(&self) -> &str {
		DESCRIPTION.trim()
	}

	fn schema(&self) -> Value {
		security_publish_schema()
	}

	async fn execute(
		&self,
		args: &Value,
		_ctx: &pi_agent::state::AgentContext,
	) -> Result<pi_agent::state::ToolResult, pi_agent::tools::ToolError> {
		let bundle = self
			.publisher
			.publish(args)
			.map_err(|error| pi_agent::tools::ToolError::ExecutionFailed(error.to_string()))?;
		let details = SecurityPublishDetails {
			scan_id: self.publisher.scan_id().to_string(),
			finding_count: bundle.findings.len(),
			status: "completed".to_string(),
		};
		let mut payload = Map::new();
		payload.insert("scanId".to_string(), json!(details.scan_id));
		payload.insert("findingCount".to_string(), json!(details.finding_count));
		payload.insert("status".to_string(), json!(details.status));
		Ok(pi_agent::state::ToolResult {
			tool_call_id: String::new(),
			content: format!(
				"Published security scan {} with {} finding(s).",
				self.publisher.scan_id(),
				bundle.findings.len()
			),
			is_error: false,
			terminate: false,
			details: Some(Value::Object(payload)),
		})
	}
}

/// Public plan projection used by `security://.../manifest`: credential
/// identity becomes an affinity hash.
#[must_use]
pub fn public_plan(plan: &SecurityScanPlan) -> Value {
	let mut account = Map::new();
	account.insert("provider".to_string(), json!(plan.account.provider));
	account.insert(
		"credentialAffinity".to_string(),
		json!(create_security_credential_affinity(&plan.account)),
	);
	let mut value = serde_json::to_value(plan).unwrap_or(Value::Null);
	if let Some(object) = value.as_object_mut() {
		object.insert("account".to_string(), Value::Object(account));
	}
	value
}
