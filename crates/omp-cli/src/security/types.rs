//! Security scan contracts.
//!
//! Anchors: `packages/coding-agent/src/security/contracts/types.ts` and
//! `schemas.ts`. Field names are camelCase because the same JSON documents are
//! written by the TypeScript runtime; nothing here is Rust-only.
//!
//! Validation is intentionally explicit at store boundaries: unknown ids,
//! duplicate references, a finding from another scan, or a manifest that omits
//! a finding are hard errors, never repaired silently.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Maximum serialized document size accepted by a store parse. Findings are
/// model-produced; an unbounded document is a memory/DoS boundary.
pub const MAX_SECURITY_DOCUMENT_BYTES: usize = 32 * 1024 * 1024;

pub type SecurityScanStatus = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityProducer {
	pub kind: String,
	pub name: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub version: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vendor: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub revision: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub plugin_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityUpstreamProvenance {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub repository: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub revision: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub package_version: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub plugin_version: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub archive_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityProvenance {
	pub producer: SecurityProducer,
	pub created_at: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub imported_at: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub source_ids: Option<serde_json::Map<String, serde_json::Value>>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vendor_fingerprints: Option<serde_json::Map<String, serde_json::Value>>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub upstream: Option<SecurityUpstreamProvenance>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityLocation {
	pub path: String,
	pub start_line: u32,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub end_line: Option<u32>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub start_column: Option<u32>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub end_column: Option<u32>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub role: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityEvidence {
	pub id: String,
	pub kind: String,
	pub label: String,
	pub explanation: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<SecurityLocation>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub excerpt: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityOccurrence {
	pub id: String,
	pub locations: Vec<SecurityLocation>,
	pub evidence_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecuritySeverity {
	pub level: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub score: Option<f64>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub scoring_system: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vector: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub rationale: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityConfidence {
	pub level: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub rationale: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityTaxonomy {
	pub category: String,
	pub cwe: Vec<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tags: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityValidation {
	pub status: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub summary: Option<String>,
	pub evidence_ids: Vec<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub validated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityDisposition {
	pub status: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub rationale: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub updated_at: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub actor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityFinding {
	pub id: String,
	pub scan_id: String,
	pub fingerprint: String,
	pub rule_id: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub anchor: Option<String>,
	pub title: String,
	pub summary: String,
	pub severity: SecuritySeverity,
	pub confidence: SecurityConfidence,
	pub taxonomy: SecurityTaxonomy,
	pub occurrences: Vec<SecurityOccurrence>,
	pub evidence: Vec<SecurityEvidence>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remediation: Option<String>,
	pub validation: SecurityValidation,
	pub disposition: SecurityDisposition,
	pub provenance: SecurityProvenance,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub extensions: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityCoverageSurface {
	pub id: String,
	pub label: String,
	pub disposition: String,
	pub receipt_refs: Vec<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub risk_area: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub notes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityCoverageDeferred {
	pub id: String,
	pub reason: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub paths: Option<Vec<String>>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub surface_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityOpenQuestion {
	pub question: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub follow_up_prompt: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityExplicitExclusion {
	pub pattern: String,
	pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityCoverage {
	pub mode: String,
	pub completeness: String,
	pub inventory_strategy: String,
	pub include_paths: Vec<String>,
	pub exclude_paths: Vec<String>,
	pub surfaces: Vec<SecurityCoverageSurface>,
	pub explicit_exclusions: Vec<SecurityExplicitExclusion>,
	pub deferred: Vec<SecurityCoverageDeferred>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub open_questions: Option<Vec<SecurityOpenQuestion>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityTarget {
	pub kind: String,
	pub repository_root: String,
	pub display_name: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub revision: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub base_revision: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub head_revision: Option<String>,
	pub include_paths: Vec<String>,
	pub exclude_paths: Vec<String>,
	pub tree_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityModelRef {
	pub provider: String,
	pub model_id: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub thinking_level: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityAccountRef {
	pub provider: String,
	pub credential_id: i64,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub account_id: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub email: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub organization_id: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub organization_name: Option<String>,
}

impl From<crate::auth::security::SecurityOAuthAccount> for SecurityAccountRef {
	fn from(account: crate::auth::security::SecurityOAuthAccount) -> Self {
		Self {
			provider: account.provider,
			credential_id: account.credential_id,
			account_id: account.account_id,
			email: account.email,
			organization_id: account.org_id,
			organization_name: account.org_name,
		}
	}
}

impl From<&SecurityAccountRef> for crate::auth::security::SecurityOAuthAccount {
	fn from(account: &SecurityAccountRef) -> Self {
		Self {
			provider: account.provider.clone(),
			credential_id: account.credential_id,
			account_id: account.account_id.clone(),
			email: account.email.clone(),
			org_id: account.organization_id.clone(),
			org_name: account.organization_name.clone(),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityKnowledgeBaseRef {
	pub path: String,
	pub sha256: String,
	pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityOutputPlan {
	pub root: String,
	pub archive_existing: bool,
	pub existing_state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityScanPlan {
	pub document_type: String,
	pub schema_version: String,
	pub id: String,
	pub created_at: String,
	pub repository_root: String,
	pub target: SecurityTarget,
	pub knowledge_bases: Vec<SecurityKnowledgeBaseRef>,
	pub output: SecurityOutputPlan,
	pub model: SecurityModelRef,
	pub account: SecurityAccountRef,
	pub config_fingerprint: String,
	pub workflow_fingerprint: String,
	pub fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityTokenUsage {
	pub input: u64,
	pub output: u64,
	pub reasoning: u64,
	pub cache_read: u64,
	pub cache_write: u64,
	pub total: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityScanMetrics {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub runtime_ms: Option<u64>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token_usage: Option<SecurityTokenUsage>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cost: Option<f64>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub premium_requests: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityScan {
	pub document_type: String,
	pub schema_version: String,
	pub id: String,
	pub project_key: String,
	pub status: String,
	pub created_at: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub started_at: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub completed_at: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub plan: Option<SecurityScanPlan>,
	pub target: SecurityTarget,
	pub producer: SecurityProducer,
	pub provenance: SecurityProvenance,
	pub finding_ids: Vec<String>,
	pub coverage: SecurityCoverage,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub report_ref: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sarif_ref: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub metrics: Option<SecurityScanMetrics>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityScanBundle {
	pub scan: SecurityScan,
	pub findings: Vec<SecurityFinding>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub report: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sarif: Option<serde_json::Value>,
}

impl SecurityScanBundle {
	pub fn finding_count(&self) -> usize {
		self.findings.len()
	}

	pub fn finding(&self, finding_id: &str) -> Option<&SecurityFinding> {
		self.findings.iter().find(|finding| finding.id == finding_id)
	}
}

/// Parse one stored document, enforcing the size ceiling before deserializing.
pub fn parse_document<T: serde::de::DeserializeOwned>(bytes: &[u8], label: &str) -> crate::Result<T> {
	if bytes.len() > MAX_SECURITY_DOCUMENT_BYTES {
		return Err(crate::Error::new(format!(
			"{label} exceeds the {MAX_SECURITY_DOCUMENT_BYTES}-byte security document limit"
		)));
	}
	serde_json::from_slice(bytes)
		.map_err(|error| crate::Error::new(format!("{label} failed schema validation: {error}")))
}

pub fn parse_plan(bytes: &[u8]) -> crate::Result<SecurityScanPlan> {
	let plan: SecurityScanPlan = parse_document(bytes, "Security scan plan")?;
	validate_plan(&plan)?;
	Ok(plan)
}

pub fn parse_scan(bytes: &[u8]) -> crate::Result<SecurityScan> {
	let scan: SecurityScan = parse_document(bytes, "Security scan")?;
	validate_scan(&scan)?;
	Ok(scan)
}

pub fn parse_bundle(bytes: &[u8]) -> crate::Result<SecurityScanBundle> {
	let bundle: SecurityScanBundle = parse_document(bytes, "Security scan bundle")?;
	validate_bundle(&bundle)?;
	Ok(bundle)
}

fn required(document: &str, field: &str, value: &str) -> crate::Result<()> {
	if value.is_empty() {
		return Err(crate::Error::new(format!(
			"{document} failed schema validation: {field} must not be empty"
		)));
	}
	Ok(())
}

fn validate_plan(plan: &SecurityScanPlan) -> crate::Result<()> {
	if plan.document_type != "omp-security.scan-plan" || plan.schema_version != "1.0" {
		return Err(crate::Error::new(
			"Security scan plan failed schema validation: unsupported document type or schema version",
		));
	}
	for (field, value) in [
		("id", plan.id.as_str()),
		("repositoryRoot", plan.repository_root.as_str()),
		("fingerprint", plan.fingerprint.as_str()),
		("model.provider", plan.model.provider.as_str()),
		("model.modelId", plan.model.model_id.as_str()),
		("account.provider", plan.account.provider.as_str()),
		("target.repositoryRoot", plan.target.repository_root.as_str()),
		("target.treeDigest", plan.target.tree_digest.as_str()),
		("output.root", plan.output.root.as_str()),
	] {
		required("Security scan plan", field, value)?;
	}
	if plan.account.credential_id < 1 {
		return Err(crate::Error::new(
			"Security scan plan failed schema validation: account.credentialId must be positive",
		));
	}
	if !is_scan_plan_id(&plan.id) {
		return Err(crate::Error::new(format!(
			"Invalid security plan id: {}",
			plan.id
		)));
	}
	validate_target(&plan.target)?;
	Ok(())
}

fn validate_target(target: &SecurityTarget) -> crate::Result<()> {
	const KINDS: [&str; 5] = ["repository", "scoped_path", "ref_diff", "working_tree", "imported"];
	if !KINDS.contains(&target.kind.as_str()) {
		return Err(crate::Error::new(format!(
			"Security scan target failed schema validation: unknown kind {}",
			target.kind
		)));
	}
	required("Security scan target", "repositoryRoot", &target.repository_root)?;
	required("Security scan target", "displayName", &target.display_name)?;
	required("Security scan target", "treeDigest", &target.tree_digest)?;
	Ok(())
}

fn validate_scan(scan: &SecurityScan) -> crate::Result<()> {
	if scan.document_type != "omp-security.scan" || scan.schema_version != "1.0" {
		return Err(crate::Error::new(
			"Security scan failed schema validation: unsupported document type or schema version",
		));
	}
	const STATUSES: [&str; 6] = ["planned", "running", "completed", "partial", "cancelled", "failed"];
	if !STATUSES.contains(&scan.status.as_str()) {
		return Err(crate::Error::new(format!(
			"Security scan failed schema validation: unknown status {}",
			scan.status
		)));
	}
	required("Security scan", "id", &scan.id)?;
	required("Security scan", "projectKey", &scan.project_key)?;
	required("Security scan", "createdAt", &scan.created_at)?;
	if !is_scan_id(&scan.id) {
		return Err(crate::Error::new(format!("Invalid security scan id: {}", scan.id)));
	}
	validate_target(&scan.target)?;
	if scan.producer.name.is_empty() {
		return Err(crate::Error::new(
			"Security scan failed schema validation: producer.name must not be empty",
		));
	}
	Ok(())
}

/// Bundle invariants from `parseSecurityScanBundle`: ids are unique and the
/// manifest and finding list agree in both directions.
pub fn validate_bundle(bundle: &SecurityScanBundle) -> crate::Result<()> {
	validate_scan(&bundle.scan)?;
	let mut finding_ids = BTreeSet::new();
	for finding in &bundle.findings {
		if !finding_ids.insert(finding.id.as_str()) {
			return Err(crate::Error::new("Security scan contains duplicate finding ids"));
		}
		if finding.scan_id != bundle.scan.id {
			return Err(crate::Error::new(format!(
				"Finding {} belongs to {}, expected {}",
				finding.id, finding.scan_id, bundle.scan.id
			)));
		}
		let mut evidence_ids = BTreeSet::new();
		for evidence in &finding.evidence {
			if !evidence_ids.insert(evidence.id.as_str()) {
				return Err(crate::Error::new(format!(
					"Finding {} contains duplicate evidence ids",
					finding.id
				)));
			}
		}
		let mut occurrence_ids = BTreeSet::new();
		for occurrence in &finding.occurrences {
			if !occurrence_ids.insert(occurrence.id.as_str()) {
				return Err(crate::Error::new(format!(
					"Finding {} contains duplicate occurrence ids",
					finding.id
				)));
			}
			for evidence_id in &occurrence.evidence_ids {
				if !evidence_ids.contains(evidence_id.as_str()) {
					return Err(crate::Error::new(format!(
						"Occurrence {} references missing evidence: {evidence_id}",
						occurrence.id
					)));
				}
			}
		}
		if finding.occurrences.is_empty() {
			return Err(crate::Error::new(format!(
				"Security finding {} has no occurrences",
				finding.id
			)));
		}
	}
	let mut manifest = BTreeSet::new();
	for finding_id in &bundle.scan.finding_ids {
		if !manifest.insert(finding_id.as_str()) {
			return Err(crate::Error::new(
				"Security scan manifest contains duplicate finding references",
			));
		}
		if !finding_ids.contains(finding_id.as_str()) {
			return Err(crate::Error::new(format!(
				"Security scan references missing finding: {finding_id}"
			)));
		}
	}
	for finding_id in &finding_ids {
		if !manifest.contains(finding_id) {
			return Err(crate::Error::new(format!(
				"Security scan omits finding from manifest: {finding_id}"
			)));
		}
	}
	Ok(())
}

#[must_use]
pub fn is_scan_id(value: &str) -> bool {
	let Some(suffix) = value.strip_prefix("secscan_") else {
		return false;
	};
	!suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_alphanumeric())
}

#[must_use]
pub fn is_scan_plan_id(value: &str) -> bool {
	let Some(suffix) = value.strip_prefix("secplan_") else {
		return false;
	};
	!suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_alphanumeric())
}

#[must_use]
pub fn is_finding_id(value: &str) -> bool {
	let Some(suffix) = value.strip_prefix("secf_") else {
		return false;
	};
	!suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ids_reject_unknown_prefixes_and_path_separators() {
		assert!(is_scan_id("secscan_abc123"));
		assert!(!is_scan_id("secscan_../x"));
		assert!(!is_scan_id("scan_abc123"));
		assert!(is_scan_plan_id("secplan_abc123"));
		assert!(!is_scan_plan_id("secplan_"));
		assert!(is_finding_id("secf_abc123"));
	}
}
