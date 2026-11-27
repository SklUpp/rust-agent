//! Codex Security bundle importer.
//!
//! Anchors: `packages/coding-agent/src/security/importers/codex-security.ts`.
//!
//! A bundle is a directory holding `manifest.json` (or the upstream
//! `scan-manifest.json`), required `findings.json`, optional `coverage.json`,
//! optional `PROVENANCE.json`, `report.md`, and `exports/results.sarif`.
//! Location and code-evidence paths must be repository-relative; every path is
//! lexically checked and then canonically contained inside the repository root
//! so a symlinked bundle cannot direct findings outside it.
//!
//! [`import_codex_security_bundle_into_store`] imports and persists through the
//! project store; [`import_codex_security_bundle`] only builds the bundle.

use std::{
	collections::HashSet,
	ffi::OsString,
	fs,
	path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::security::{
	ids::{
		create_security_evidence_id, create_security_finding_fingerprint, create_security_finding_id,
		create_security_occurrence_id, create_security_scan_id, encode_security_project_key,
		sha256_hex,
	},
	store::SecurityStore,
	types::{
		self, SecurityConfidence, SecurityCoverage, SecurityCoverageDeferred, SecurityDisposition,
		SecurityEvidence, SecurityExplicitExclusion, SecurityFinding, SecurityLocation,
		SecurityOccurrence, SecurityOpenQuestion, SecurityProducer, SecurityProvenance,
		SecurityScanBundle, SecuritySeverity, SecurityTaxonomy, SecurityUpstreamProvenance,
		SecurityValidation,
	},
};

const VALIDATION_STATUSES: [&str; 5] = ["unvalidated", "validated", "rejected", "partial", "error"];
const SEVERITY_LEVELS: [&str; 5] = ["critical", "high", "medium", "low", "informational"];
const CONFIDENCE_LEVELS: [&str; 3] = ["high", "medium", "low"];

/// Options for importing one Codex Security bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexSecurityImportOptions {
	/// Repository every bundle location must resolve inside; canonicalized on use.
	pub repository_root: PathBuf,
	/// Fixed scan timestamp; defaults to the manifest `startedAt` or now.
	pub created_at: Option<String>,
	/// Fixed scan id; defaults to a random `secscan_<uuid-without-dashes>`.
	pub scan_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexManifest {
	document_type: Option<String>,
	schema_version: Option<String>,
	scan: Option<CodexManifestScan>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexManifestScan {
	id: Option<String>,
	producer: Option<CodexProducer>,
	started_at: Option<String>,
	completed_at: Option<String>,
	target: Option<Map<String, Value>>,
	scope: Option<CodexScope>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexProducer {
	name: Option<String>,
	version: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexScope {
	#[serde(default)]
	include_paths: Value,
	#[serde(default)]
	exclude_paths: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexFindingsDocument {
	document_type: Option<String>,
	schema_version: Option<String>,
	scan_id: Option<String>,
	findings: Option<Vec<CodexFinding>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexFinding {
	finding_id: Option<String>,
	occurrence_id: Option<String>,
	rule_id: Option<String>,
	identity: Option<CodexIdentity>,
	fingerprints: Option<CodexFingerprints>,
	title: Option<String>,
	summary: Option<String>,
	severity: Option<CodexSeverity>,
	confidence: Option<CodexConfidence>,
	taxonomy: Option<CodexTaxonomy>,
	locations: Option<Vec<CodexLocation>>,
	code_evidence: Option<Vec<CodexEvidence>>,
	remediation: Option<String>,
	validation: Option<Map<String, Value>>,
	provenance: Option<Map<String, Value>>,
	extensions: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexIdentity {
	anchor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexFingerprints {
	algorithm: Option<String>,
	primary: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexSeverity {
	level: Option<String>,
	score: Option<f64>,
	scoring_system: Option<String>,
	vector: Option<String>,
	rationale: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexConfidence {
	level: Option<String>,
	rationale: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexTaxonomy {
	category: Option<String>,
	#[serde(default)]
	cwe: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexLocation {
	path: Option<String>,
	start_line: Option<u32>,
	end_line: Option<u32>,
	role: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexEvidence {
	id: Option<String>,
	label: Option<String>,
	path: Option<String>,
	start_line: Option<u32>,
	end_line: Option<u32>,
	role: Option<String>,
	code: Option<String>,
	explanation: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexCoverageDocument {
	document_type: Option<String>,
	schema_version: Option<String>,
	scan_id: Option<String>,
	mode: Option<String>,
	completeness: Option<String>,
	inventory_strategy: Option<String>,
	#[serde(default)]
	include_paths: Value,
	#[serde(default)]
	exclude_paths: Value,
	surfaces: Option<Vec<types::SecurityCoverageSurface>>,
	explicit_exclusions: Option<Vec<SecurityExplicitExclusion>>,
	deferred: Option<Vec<SecurityCoverageDeferred>>,
	open_questions: Option<Vec<SecurityOpenQuestion>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexFixtureProvenance {
	repository: Option<String>,
	revision: Option<String>,
	package_version: Option<String>,
	plugin_version: Option<String>,
	archive_sha256: Option<String>,
}

/// Import a Codex Security bundle directory into a completed scan bundle.
pub fn import_codex_security_bundle(
	bundle_directory: &Path,
	options: &CodexSecurityImportOptions,
) -> crate::Result<SecurityScanBundle> {
	let root = std::path::absolute(bundle_directory).map_err(|error| {
		crate::Error::new(format!(
			"cannot resolve Codex Security bundle {}: {error}",
			bundle_directory.display()
		))
	})?;
	let manifest = read_manifest(&root)?;
	let findings_document = read_required_document::<CodexFindingsDocument>(
		&root.join("findings.json"),
		"findings document",
	)?;
	let coverage_document = read_optional_document::<CodexCoverageDocument>(
		&root.join("coverage.json"),
		"coverage document",
	)?;
	validate_documents(&manifest, &findings_document, coverage_document.as_ref())?;
	let Some(manifest_scan) = manifest.scan.as_ref() else {
		return Err(crate::Error::new("Codex Security bundle scan IDs do not agree"));
	};
	let Some(source_scan_id) = manifest_scan.id.as_deref().filter(|id| !id.is_empty()) else {
		return Err(crate::Error::new("Codex Security bundle scan IDs do not agree"));
	};
	let fixture_provenance = read_optional_document::<CodexFixtureProvenance>(
		&root.join("PROVENANCE.json"),
		"provenance",
	)?
	.unwrap_or_default();
	let scan_id = options
		.scan_id
		.clone()
		.unwrap_or_else(|| create_security_scan_id(&crate::advisor_identity::uuid_v7()));
	let created_at = options
		.created_at
		.clone()
		.or_else(|| manifest_scan.started_at.clone())
		.unwrap_or_else(now_iso);
	let completed_at = manifest_scan
		.completed_at
		.clone()
		.unwrap_or_else(|| created_at.clone());
	let canonical_root = fs::canonicalize(&options.repository_root).map_err(|error| {
		crate::Error::new(format!(
			"cannot resolve Codex Security repository root {}: {error}",
			options.repository_root.display()
		))
	})?;
	let producer = codex_producer(manifest_scan, &fixture_provenance);
	let upstream = codex_upstream(&fixture_provenance);

	let mut findings = Vec::new();
	let mut seen_fingerprints = HashSet::new();
	for source in findings_document.findings.iter().flatten() {
		let finding = import_finding(
			source,
			&canonical_root,
			source_scan_id,
			&scan_id,
			&created_at,
			&producer,
			&upstream,
		)?;
		if seen_fingerprints.insert(finding.fingerprint.clone()) {
			findings.push(finding);
		}
	}

	let scope = manifest_scan.scope.as_ref();
	let include_paths_value = scope.map(|scope| &scope.include_paths);
	let exclude_paths_value = scope.map(|scope| &scope.exclude_paths);
	let include_paths = string_array(include_paths_value);
	let exclude_paths = string_array(exclude_paths_value);
	let target = manifest_scan.target.as_ref();
	let target_kind = match target
		.and_then(|target| target.get("kind"))
		.and_then(Value::as_str)
	{
		Some("git_diff") => "ref_diff",
		Some("git_worktree") => "working_tree",
		_ => "imported",
	};
	let display_name = target
		.and_then(|target| target.get("displayName"))
		.and_then(Value::as_str)
		.map_or_else(
			|| {
				canonical_root
					.file_name()
					.map_or_else(String::new, |name| name.to_string_lossy().into_owned())
			},
			str::to_string,
		);
	let tree_digest = target
		.and_then(|target| target.get("snapshotDigest"))
		.and_then(Value::as_str)
		.map_or_else(
			|| {
				let material = json!({
						"manifest": &manifest,
						"findingsDocument": &findings_document,
					"coverageDocument": &coverage_document,
				});
				sha256_hex(
					serde_json::to_string(&material)
						.unwrap_or_default()
						.as_bytes(),
				)
			},
			str::to_string,
		);
	let mut canonical_target = types::SecurityTarget {
		kind: target_kind.to_string(),
		repository_root: canonical_root.to_string_lossy().into_owned(),
		display_name,
		revision: None,
		base_revision: None,
		head_revision: None,
		include_paths,
		exclude_paths,
		tree_digest,
	};
	if let Some(revision) = target_string(target, "revision") {
		canonical_target.revision = Some(revision);
	}
	if let Some(baseline) = target_string(target, "baseRevision") {
		canonical_target.base_revision = Some(baseline);
	}
	if let Some(head) = target_string(target, "headRevision") {
		canonical_target.head_revision = Some(head);
	}

	let scan_provenance = SecurityProvenance {
		producer: producer.clone(),
		created_at: created_at.clone(),
		imported_at: Some(now_iso()),
		source_ids: Some(scan_source_ids(source_scan_id)),
		vendor_fingerprints: None,
		upstream: Some(upstream),
		metadata: Some(bundle_metadata(&root)),
	};
	let report = match super::read_optional_security_document(
		&root.join("report.md"),
		"Codex Security report",
	)? {
		Some(bytes) => Some(String::from_utf8(bytes).map_err(|error| {
			crate::Error::new(format!("Codex Security report is not valid UTF-8: {error}"))
		})?),
		None => None,
	};
	let sarif_path = root.join("exports").join("results.sarif");
	let sarif =
		match super::read_optional_security_document(&sarif_path, "Codex Security SARIF artifact")? {
			Some(bytes) => Some(serde_json::from_slice(&bytes).map_err(|error| {
				crate::Error::new(format!("cannot parse Codex Security SARIF artifact: {error}"))
			})?),
			None => None,
		};
	let mut scan = types::SecurityScan {
		document_type: "omp-security.scan".to_string(),
		schema_version: "1.0".to_string(),
		id: scan_id,
		project_key: encode_security_project_key(&canonical_root.to_string_lossy()),
		status: "completed".to_string(),
		created_at: created_at.clone(),
		started_at: manifest_scan.started_at.clone(),
		completed_at: Some(completed_at.clone()),
		plan: None,
		target: canonical_target,
		producer: producer.clone(),
		provenance: scan_provenance,
		finding_ids: findings.iter().map(|finding| finding.id.clone()).collect(),
		coverage: map_coverage(coverage_document.as_ref(), include_paths_value, exclude_paths_value),
		report_ref: report.as_ref().map(|_| "report.md".to_string()),
		sarif_ref: sarif.as_ref().map(|_| "results.sarif".to_string()),
		error: None,
		metrics: None,
	};
	if let (Some(start), Some(end)) = (
		scan
			.started_at
			.as_deref()
			.and_then(crate::foreign_session::parse_iso_timestamp_ms),
		crate::foreign_session::parse_iso_timestamp_ms(&completed_at),
	) {
		if let Some(runtime_ms) = end.checked_sub(start) {
			scan.metrics = Some(types::SecurityScanMetrics {
				runtime_ms: Some(runtime_ms),
				token_usage: None,
				cost: None,
				premium_requests: None,
			});
		}
	}
	let bundle = SecurityScanBundle { scan, findings, report, sarif };
	types::validate_bundle(&bundle)?;
	Ok(bundle)
}

/// Import a Codex Security bundle and persist it in the project store.
pub fn import_codex_security_bundle_into_store(
	bundle_directory: &Path,
	store: &SecurityStore,
	options: &CodexSecurityImportOptions,
) -> crate::Result<SecurityScanBundle> {
	let bundle = import_codex_security_bundle(bundle_directory, options)?;
	store.put_bundle(&bundle)?;
	Ok(bundle)
}

fn validate_documents(
	manifest: &CodexManifest,
	findings: &CodexFindingsDocument,
	coverage: Option<&CodexCoverageDocument>,
) -> crate::Result<()> {
	if manifest.document_type.as_deref() != Some("codex-security.scan-manifest")
		|| manifest.schema_version.as_deref() != Some("1.0")
	{
		return Err(crate::Error::new("Unsupported Codex Security scan manifest"));
	}
	if findings.document_type.as_deref() != Some("codex-security.findings")
		|| findings.schema_version.as_deref() != Some("1.0")
	{
		return Err(crate::Error::new("Unsupported Codex Security findings document"));
	}
	if findings.findings.is_none() {
		return Err(crate::Error::new(
			"Codex Security findings document is missing its findings array",
		));
	}
	if let Some(coverage) = coverage {
		if coverage.document_type.as_deref() != Some("codex-security.coverage")
			|| coverage.schema_version.as_deref() != Some("1.0")
		{
			return Err(crate::Error::new("Unsupported Codex Security coverage document"));
		}
	}
	let Some(scan_id) = manifest
		.scan
		.as_ref()
		.and_then(|scan| scan.id.as_deref())
		.filter(|id| !id.is_empty())
	else {
		return Err(crate::Error::new("Codex Security bundle scan IDs do not agree"));
	};
	if findings.scan_id.as_deref() != Some(scan_id)
		|| coverage.is_some_and(|coverage| coverage.scan_id.as_deref() != Some(scan_id))
	{
		return Err(crate::Error::new("Codex Security bundle scan IDs do not agree"));
	}
	Ok(())
}

fn read_manifest(root: &Path) -> crate::Result<CodexManifest> {
	if let Some(manifest) =
		read_optional_document::<CodexManifest>(&root.join("manifest.json"), "scan manifest")?
	{
		return Ok(manifest);
	}
	read_required_document::<CodexManifest>(&root.join("scan-manifest.json"), "scan manifest")
}

fn read_required_document<T: serde::de::DeserializeOwned>(
	path: &Path,
	label: &str,
) -> crate::Result<T> {
	read_optional_document(path, label)?.ok_or_else(|| {
		crate::Error::new(format!("Codex Security bundle is missing {} ({label})", path.display()))
	})
}

fn read_optional_document<T: serde::de::DeserializeOwned>(
	path: &Path,
	label: &str,
) -> crate::Result<Option<T>> {
	let Some(bytes) =
		super::read_optional_security_document(path, &format!("Codex Security {label}"))?
	else {
		return Ok(None);
	};
	let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
		crate::Error::new(format!("cannot parse Codex Security {label} {}: {error}", path.display()))
	})?;
	serde_json::from_value(value).map(Some).map_err(|error| {
		crate::Error::new(format!(
			"Codex Security {label} {} failed schema validation: {error}",
			path.display()
		))
	})
}

fn codex_producer(scan: &CodexManifestScan, fixture: &CodexFixtureProvenance) -> SecurityProducer {
	let name = scan
		.producer
		.as_ref()
		.and_then(|producer| producer.name.as_deref())
		.filter(|name| !name.is_empty())
		.unwrap_or("codex-security")
		.to_string();
	let mut producer = SecurityProducer {
		kind: "codex-security-bundle".to_string(),
		name,
		version: None,
		vendor: Some("openai".to_string()),
		revision: None,
		plugin_version: None,
	};
	if let Some(version) = scan
		.producer
		.as_ref()
		.and_then(|producer| producer.version.clone())
	{
		producer.version = Some(version);
	}
	if let Some(revision) = fixture.revision.clone() {
		producer.revision = Some(revision);
	}
	if let Some(plugin_version) = fixture.plugin_version.clone() {
		producer.plugin_version = Some(plugin_version);
	}
	producer
}

fn codex_upstream(fixture: &CodexFixtureProvenance) -> SecurityUpstreamProvenance {
	SecurityUpstreamProvenance {
		repository: fixture.repository.clone(),
		revision: fixture.revision.clone(),
		package_version: fixture.package_version.clone(),
		plugin_version: fixture.plugin_version.clone(),
		archive_sha256: fixture.archive_sha256.clone(),
	}
}

fn import_finding(
	source: &CodexFinding,
	root: &Path,
	source_scan_id: &str,
	scan_id: &str,
	created_at: &str,
	producer: &SecurityProducer,
	upstream: &SecurityUpstreamProvenance,
) -> crate::Result<SecurityFinding> {
	let rule_id = source
		.rule_id
		.as_deref()
		.filter(|rule_id| !rule_id.is_empty())
		.unwrap_or("codex-security.unknown");
	let category = source
		.taxonomy
		.as_ref()
		.and_then(|taxonomy| taxonomy.category.as_deref())
		.filter(|category| !category.is_empty())
		.map_or_else(|| rule_category(rule_id), str::to_string);
	let has_source_locations = source
		.locations
		.as_deref()
		.unwrap_or_default()
		.iter()
		.any(|location| location.path.is_some() && location.start_line.is_some());
	let locations = locations_for_finding(root, source.locations.as_deref().unwrap_or_default())?;
	let anchor = source
		.identity
		.as_ref()
		.and_then(|identity| identity.anchor.as_deref())
		.filter(|anchor| !anchor.is_empty())
		.map(str::to_string)
		.or_else(|| {
			source
				.fingerprints
				.as_ref()
				.and_then(|fingerprints| fingerprints.primary.as_deref())
				.filter(|primary| !primary.is_empty())
				.map(str::to_string)
		})
		.or_else(|| {
			if has_source_locations {
				None
			} else {
				source
					.finding_id
					.as_deref()
					.filter(|id| !id.is_empty())
					.map(str::to_string)
			}
		});
	let fingerprint =
		create_security_finding_fingerprint(rule_id, &category, anchor.as_deref(), &locations);
	let evidence = evidence_for_finding(root, source, &fingerprint)?;
	let evidence_ids = evidence.iter().map(|item| item.id.clone()).collect();
	let mut provenance = SecurityProvenance {
		producer: producer.clone(),
		created_at: created_at.to_string(),
		imported_at: Some(now_iso()),
		source_ids: Some(finding_source_ids(
			source_scan_id,
			source.finding_id.as_deref(),
			source.occurrence_id.as_deref(),
		)),
		vendor_fingerprints: None,
		upstream: Some(upstream.clone()),
		metadata: source.provenance.clone(),
	};
	if let Some(primary) = source
		.fingerprints
		.as_ref()
		.and_then(|fingerprints| fingerprints.primary.as_deref())
		.filter(|primary| !primary.is_empty())
	{
		let algorithm = source
			.fingerprints
			.as_ref()
			.and_then(|fingerprints| fingerprints.algorithm.as_deref())
			.filter(|algorithm| !algorithm.is_empty())
			.unwrap_or("codex-security/v1");
		let mut vendor_fingerprints = Map::new();
		vendor_fingerprints.insert(algorithm.to_string(), Value::String(primary.to_string()));
		provenance.vendor_fingerprints = Some(vendor_fingerprints);
	}
	let mut severity = SecuritySeverity {
		level: source
			.severity
			.as_ref()
			.and_then(|severity| severity.level.as_deref())
			.filter(|level| SEVERITY_LEVELS.contains(level))
			.unwrap_or("informational")
			.to_string(),
		score: None,
		scoring_system: None,
		vector: None,
		rationale: None,
	};
	if let Some(severity_source) = source.severity.as_ref() {
		severity.score = severity_source.score;
		severity
			.scoring_system
			.clone_from(&severity_source.scoring_system);
		severity.vector.clone_from(&severity_source.vector);
		severity.rationale.clone_from(&severity_source.rationale);
	}
	let confidence = SecurityConfidence {
		level: source
			.confidence
			.as_ref()
			.and_then(|confidence| confidence.level.as_deref())
			.filter(|level| CONFIDENCE_LEVELS.contains(level))
			.unwrap_or("medium")
			.to_string(),
		rationale: source
			.confidence
			.as_ref()
			.and_then(|confidence| confidence.rationale.clone()),
	};
	let validation_summary = source
		.validation
		.as_ref()
		.map(serde_json::to_string)
		.transpose()
		.map_err(|error| {
			crate::Error::new(format!("cannot encode Codex Security validation: {error}"))
		})?;
	let finding = SecurityFinding {
		id: create_security_finding_id(&fingerprint),
		scan_id: scan_id.to_string(),
		fingerprint: fingerprint.clone(),
		rule_id: rule_id.to_string(),
		anchor: anchor.clone(),
		title: source
			.title
			.as_deref()
			.filter(|title| !title.is_empty())
			.unwrap_or(rule_id)
			.to_string(),
		summary: source.summary.clone().unwrap_or_default(),
		severity,
		confidence,
		taxonomy: SecurityTaxonomy {
			category,
			cwe: string_array(source.taxonomy.as_ref().map(|taxonomy| &taxonomy.cwe)),
			tags: None,
		},
		occurrences: vec![SecurityOccurrence {
			id: create_security_occurrence_id(&fingerprint, &locations),
			locations,
			evidence_ids,
		}],
		evidence,
		remediation: source.remediation.clone(),
		validation: SecurityValidation {
			status: source
				.validation
				.as_ref()
				.and_then(|validation| validation.get("status"))
				.and_then(Value::as_str)
				.filter(|status| VALIDATION_STATUSES.contains(status))
				.unwrap_or("unvalidated")
				.to_string(),
			summary: validation_summary,
			evidence_ids: Vec::new(),
			validated_at: None,
		},
		disposition: SecurityDisposition {
			status: "open".to_string(),
			rationale: None,
			updated_at: None,
			actor: None,
		},
		provenance,
		extensions: source.extensions.clone(),
	};
	Ok(finding)
}

fn validate_codex_lines(
	start_line: Option<u32>,
	end_line: Option<u32>,
	label: &str,
) -> crate::Result<()> {
	if start_line == Some(0) {
		return Err(crate::Error::new(format!("Codex Security {label} startLine must be >= 1")));
	}
	if end_line == Some(0) {
		return Err(crate::Error::new(format!("Codex Security {label} endLine must be >= 1")));
	}
	Ok(())
}

fn locations_for_finding(
	root: &Path,
	source: &[CodexLocation],
) -> crate::Result<Vec<SecurityLocation>> {
	let mut locations = Vec::new();
	for location in source {
		validate_codex_lines(location.start_line, location.end_line, "finding location")?;
		let (Some(path), Some(start_line)) = (&location.path, location.start_line) else {
			continue;
		};
		locations.push(SecurityLocation {
			path: repository_location(root, path)?,
			start_line,
			end_line: location.end_line,
			start_column: None,
			end_column: None,
			role: location.role.clone(),
		});
	}
	Ok(if locations.is_empty() {
		vec![SecurityLocation {
			path: "unknown".to_string(),
			start_line: 1,
			end_line: None,
			start_column: None,
			end_column: None,
			role: Some("unknown".to_string()),
		}]
	} else {
		locations
	})
}

fn evidence_for_finding(
	root: &Path,
	source: &CodexFinding,
	fingerprint: &str,
) -> crate::Result<Vec<SecurityEvidence>> {
	let mut evidence = Vec::new();
	for (index, item) in source
		.code_evidence
		.as_deref()
		.unwrap_or_default()
		.iter()
		.enumerate()
	{
		let label_source = item
			.label
			.as_deref()
			.filter(|label| !label.is_empty())
			.or_else(|| item.id.as_deref().filter(|id| !id.is_empty()))
			.unwrap_or("code evidence");
		validate_codex_lines(item.start_line, item.end_line, "evidence location")?;
		let label = item
			.label
			.as_deref()
			.filter(|label| !label.is_empty())
			.or_else(|| item.id.as_deref().filter(|id| !id.is_empty()))
			.map_or_else(|| format!("Evidence {}", index + 1), str::to_string);
		let mut entry = SecurityEvidence {
			id: create_security_evidence_id(fingerprint, label_source, index),
			kind: "code".to_string(),
			label,
			explanation: item.explanation.clone().unwrap_or_default(),
			location: None,
			excerpt: item.code.clone(),
		};
		if let (Some(path), Some(start_line)) = (&item.path, item.start_line) {
			entry.location = Some(SecurityLocation {
				path: repository_location(root, path)?,
				start_line,
				end_line: item.end_line,
				start_column: None,
				end_column: None,
				role: item.role.clone(),
			});
		}
		evidence.push(entry);
	}
	Ok(evidence)
}

fn map_coverage(
	document: Option<&CodexCoverageDocument>,
	manifest_include_paths: Option<&Value>,
	manifest_exclude_paths: Option<&Value>,
) -> SecurityCoverage {
	let Some(document) = document else {
		return SecurityCoverage {
			mode: "imported".to_string(),
			completeness: "unknown".to_string(),
			inventory_strategy: "imported".to_string(),
			include_paths: string_array(manifest_include_paths),
			exclude_paths: string_array(manifest_exclude_paths),
			surfaces: Vec::new(),
			explicit_exclusions: Vec::new(),
			deferred: vec![SecurityCoverageDeferred {
				id: "codex-coverage".to_string(),
				reason: "Codex Security bundle has no coverage document".to_string(),
				paths: None,
				surface_ids: None,
			}],
			open_questions: None,
		};
	};
	let mode = match document.mode.as_deref() {
		Some(mode @ ("repository" | "scoped_path" | "diff" | "working_tree" | "deep_repository")) => {
			mode.to_string()
		},
		Some("commit" | "branch_diff") => "diff".to_string(),
		_ => "imported".to_string(),
	};
	let completeness = match document.completeness.as_deref() {
		Some(completeness @ ("complete" | "partial" | "unknown")) => completeness.to_string(),
		_ => "unknown".to_string(),
	};
	let inventory_strategy = match document.inventory_strategy.as_deref() {
		Some(strategy @ ("repository" | "scoped_path" | "diff" | "directory" | "custom")) => {
			strategy.to_string()
		},
		_ => "imported".to_string(),
	};
	SecurityCoverage {
		mode,
		completeness,
		inventory_strategy,
		include_paths: string_array(Some(&document.include_paths)),
		exclude_paths: string_array(Some(&document.exclude_paths)),
		surfaces: document.surfaces.clone().unwrap_or_default(),
		explicit_exclusions: document.explicit_exclusions.clone().unwrap_or_default(),
		deferred: document.deferred.clone().unwrap_or_default(),
		open_questions: document.open_questions.clone(),
	}
}

fn repository_location(root: &Path, value: &str) -> crate::Result<String> {
	let mut normalized = value.replace('\\', "/");
	if let Some(stripped) = normalized.strip_prefix("./") {
		normalized = stripped.to_string();
	}
	let invalid = normalized.is_empty()
		|| normalized.starts_with('/')
		|| has_drive_prefix(&normalized)
		|| normalized.split('/').any(|segment| segment == "..")
		|| normalized.contains('\0');
	if invalid {
		return Err(crate::Error::new(format!(
			"Codex Security bundle locations must be repository-relative: {value}"
		)));
	}
	let candidate = root.join(&normalized);
	super::ensure_path_components_inside(&candidate, root).map_err(|error| {
		crate::Error::new(format!("Codex Security bundle location {value}: {error}"))
	})?;
	let canonical = canonicalize_nearest_existing(&candidate).map_err(|error| {
		crate::Error::new(format!("cannot resolve Codex Security bundle location {value}: {error}"))
	})?;
	if !path_is_within(&canonical, root) {
		return Err(crate::Error::new(format!(
			"Codex Security bundle location resolves outside the repository: {value}"
		)));
	}
	Ok(normalized)
}

fn has_drive_prefix(value: &str) -> bool {
	let bytes = value.as_bytes();
	bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn canonicalize_nearest_existing(path: &Path) -> std::io::Result<PathBuf> {
	let mut missing: Vec<OsString> = Vec::new();
	let mut candidate = path.to_path_buf();
	loop {
		match fs::canonicalize(&candidate) {
			Ok(mut resolved) => {
				for part in missing.iter().rev() {
					resolved.push(Path::new(part));
				}
				return Ok(resolved);
			},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				let Some(name) = candidate.file_name().map(OsString::from) else {
					return Err(error);
				};
				missing.push(name);
				let Some(parent) = candidate.parent() else {
					return Err(error);
				};
				if parent == candidate.as_path() {
					return Err(error);
				}
				candidate = parent.to_path_buf();
			},
			Err(error) => return Err(error),
		}
	}
}

fn path_is_within(candidate: &Path, root: &Path) -> bool {
	candidate == root || candidate.starts_with(root)
}

fn string_array(value: Option<&Value>) -> Vec<String> {
	value
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

fn target_string(target: Option<&Map<String, Value>>, key: &str) -> Option<String> {
	target
		.and_then(|target| target.get(key))
		.and_then(Value::as_str)
		.map(str::to_string)
}

fn scan_source_ids(scan_id: &str) -> Map<String, Value> {
	let mut ids = Map::new();
	ids.insert("scanId".to_string(), Value::String(scan_id.to_string()));
	ids
}

fn finding_source_ids(
	scan_id: &str,
	finding_id: Option<&str>,
	occurrence_id: Option<&str>,
) -> Map<String, Value> {
	let mut ids = scan_source_ids(scan_id);
	if let Some(finding_id) = finding_id.filter(|id| !id.is_empty()) {
		ids.insert("findingId".to_string(), Value::String(finding_id.to_string()));
	}
	if let Some(occurrence_id) = occurrence_id.filter(|id| !id.is_empty()) {
		ids.insert("occurrenceId".to_string(), Value::String(occurrence_id.to_string()));
	}
	ids
}

fn bundle_metadata(root: &Path) -> Map<String, Value> {
	let mut metadata = Map::new();
	metadata
		.insert("bundleDirectory".to_string(), Value::String(root.to_string_lossy().into_owned()));
	metadata
}

fn rule_category(rule_id: &str) -> String {
	rule_id
		.split(['.', '/', '-'])
		.next()
		.filter(|segment| !segment.is_empty())
		.unwrap_or("security")
		.to_string()
}

fn now_iso() -> String {
	crate::output::iso8601(std::time::SystemTime::now())
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicU64, Ordering};

	use serde_json::json;

	use super::*;

	fn temp_root(label: &str) -> PathBuf {
		static NEXT: AtomicU64 = AtomicU64::new(0);
		let path = std::env::var_os("HOME")
			.map_or_else(std::env::temp_dir, PathBuf::from)
			.join(format!(
				"omp-security-codex-{label}-{}-{}",
				std::process::id(),
				NEXT.fetch_add(1, Ordering::Relaxed)
			));
		fs::create_dir_all(&path).expect("create temp root");
		path
	}

	fn repository(label: &str) -> (PathBuf, PathBuf) {
		let root = temp_root(label);
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("create repository");
		(root, repository)
	}

	fn touch(repository: &Path, relative: &str) {
		let path = repository.join(relative);
		fs::create_dir_all(path.parent().expect("parent")).expect("create parent");
		fs::write(path, "SECRET\n").expect("write fixture");
	}

	fn write_json(path: &Path, value: &Value) {
		if let Some(parent) = path.parent() {
			fs::create_dir_all(parent).expect("create json parent");
		}
		fs::write(path, serde_json::to_vec_pretty(value).expect("encode json")).expect("write json");
	}

	fn manifest(scan_id: &str) -> Value {
		json!({
			"documentType": "codex-security.scan-manifest",
			"schemaVersion": "1.0",
			"scan": {
				"id": scan_id,
				"producer": {"name": "codex-security-plugin", "version": "0.1.0"},
				"status": "completed",
				"startedAt": "2026-05-31T18:00:00Z",
				"completedAt": "2026-05-31T18:09:00Z",
				"scope": {"includePaths": ["src/"], "excludePaths": ["vendor/"]},
				"target": {
					"kind": "git_worktree",
					"displayName": "example/repo",
					"revision": "deadbeef",
					"baseRevision": "aaa111",
					"headRevision": "bbb222",
					"snapshotDigest": "codex-security-snapshot/v1:sha256:snapshot"
				}
			}
		})
	}

	fn findings_document(scan_id: &str, findings: Value) -> Value {
		json!({
			"documentType": "codex-security.findings",
			"schemaVersion": "1.0",
			"scanId": scan_id,
			"findings": findings
		})
	}

	fn one_finding(locations: Value) -> Value {
		json!({
			"findingId": "csf_1",
			"occurrenceId": "occ_1",
			"ruleId": "path.traversal",
			"title": "Title",
			"summary": "Summary",
			"locations": locations
		})
	}

	fn coverage_document(scan_id: &str) -> Value {
		json!({
			"documentType": "codex-security.coverage",
			"schemaVersion": "1.0",
			"scanId": scan_id,
			"mode": "repository",
			"completeness": "complete",
			"inventoryStrategy": "repository",
			"includePaths": ["src/"],
			"excludePaths": ["vendor/"],
			"surfaces": [{
				"id": "surface_archive_extraction",
				"label": "Archive extraction",
				"disposition": "reported",
				"receiptRefs": ["receipt_1"],
				"riskArea": "extraction",
				"notes": "checked"
			}],
			"explicitExclusions": [{"pattern": "vendor/**", "reason": "third-party"}],
			"deferred": [{
				"id": "deferred_1",
				"reason": "later",
				"paths": ["src/other.py"],
				"surfaceIds": ["surface_archive_extraction"]
			}],
			"openQuestions": [{"question": "what about symlinks?", "followUpPrompt": "inspect"}]
		})
	}

	fn write_bundle(
		root: &Path,
		manifest_name: &str,
		manifest: &Value,
		findings: &Value,
		coverage: Option<&Value>,
	) {
		write_json(&root.join(manifest_name), manifest);
		write_json(&root.join("findings.json"), findings);
		if let Some(coverage) = coverage {
			write_json(&root.join("coverage.json"), coverage);
		}
	}

	fn options(repository: &Path, scan_id: &str) -> CodexSecurityImportOptions {
		CodexSecurityImportOptions {
			repository_root: repository.to_path_buf(),
			created_at: Some("2026-07-29T00:00:00.000Z".to_string()),
			scan_id: Some(scan_id.to_string()),
		}
	}

	#[test]
	fn imports_manifest_findings_coverage_and_optional_artifacts() {
		let (root, repository) = repository("rich");
		touch(&repository, "src/extract.py");
		let bundle_root = root.join("bundle");
		let findings = json!([{
			"findingId": "csf_852f90d6e1177502ff113d4a",
			"occurrenceId": "occ_e79cb19591e696572a1c22be",
			"ruleId": "path-traversal.archive-extraction",
			"identity": {"anchor": "archive-entry-write-without-containment"},
			"fingerprints": {
				"algorithm": "codex-security/v1",
				"primary": "codex-security/v1:sha256:990a"
			},
			"title": "Unsafe archive extraction can escape the output directory",
			"summary": "An attacker-controlled path reaches a filesystem write.",
			"severity": {
				"level": "high",
				"score": 8.1,
				"scoringSystem": "CVSS:3.1",
				"vector": "CVSS:3.1/AV:N",
				"rationale": "write outside root"
			},
			"confidence": {"level": "high", "rationale": "direct trace"},
			"taxonomy": {"category": "path-traversal", "cwe": ["CWE-22"]},
			"locations": [{
				"path": "src/extract.py",
				"startLine": 41,
				"endLine": 44,
				"role": "sink"
			}],
			"codeEvidence": [{
				"id": "ev_1",
				"label": "Sink",
				"path": "src/extract.py",
				"startLine": 41,
				"endLine": 44,
				"role": "sink",
				"code": "open(destination, \"w\")",
				"explanation": "writes the archive entry"
			}],
			"remediation": "Normalize destinations.",
			"validation": {"status": "validated", "summary": "reproduced"},
			"provenance": {"source": "local_plugin"},
			"extensions": {"extra": true}
		}]);
		write_bundle(
			&bundle_root,
			"manifest.json",
			&manifest("scan_example_001"),
			&findings_document("scan_example_001", findings),
			Some(&coverage_document("scan_example_001")),
		);
		write_json(
			&bundle_root.join("PROVENANCE.json"),
			&json!({
				"repository": "https://github.com/openai/codex-security",
				"revision": "f22d4a36",
				"packageVersion": "0.1.1",
				"pluginVersion": "0.1.14",
				"archiveSha256": "13745c49"
			}),
		);
		fs::write(bundle_root.join("report.md"), "# report\n").expect("report");
		write_json(
			&bundle_root.join("exports/results.sarif"),
			&json!({"version": "2.1.0", "runs": []}),
		);

		let bundle = import_codex_security_bundle(
			&bundle_root,
			&CodexSecurityImportOptions {
				repository_root: repository.clone(),
				created_at: None,
				scan_id: Some("secscan_codexfixture".to_string()),
			},
		)
		.expect("import bundle");
		assert_eq!(bundle.scan.id, "secscan_codexfixture");
		assert_eq!(bundle.scan.status, "completed");
		assert_eq!(bundle.scan.created_at, "2026-05-31T18:00:00Z");
		assert_eq!(bundle.scan.completed_at.as_deref(), Some("2026-05-31T18:09:00Z"));
		assert_eq!(bundle.scan.started_at.as_deref(), Some("2026-05-31T18:00:00Z"));
		assert_eq!(
			bundle
				.scan
				.metrics
				.as_ref()
				.and_then(|metrics| metrics.runtime_ms),
			Some(540_000)
		);
		assert_eq!(bundle.scan.target.kind, "working_tree");
		assert_eq!(bundle.scan.target.display_name, "example/repo");
		assert_eq!(bundle.scan.target.revision.as_deref(), Some("deadbeef"));
		assert_eq!(bundle.scan.target.base_revision.as_deref(), Some("aaa111"));
		assert_eq!(bundle.scan.target.head_revision.as_deref(), Some("bbb222"));
		assert_eq!(bundle.scan.target.include_paths, vec!["src/".to_string()]);
		assert_eq!(bundle.scan.target.exclude_paths, vec!["vendor/".to_string()]);
		assert_eq!(bundle.scan.target.tree_digest, "codex-security-snapshot/v1:sha256:snapshot");
		assert_eq!(bundle.scan.producer.kind, "codex-security-bundle");
		assert_eq!(bundle.scan.producer.name, "codex-security-plugin");
		assert_eq!(bundle.scan.producer.vendor.as_deref(), Some("openai"));
		assert_eq!(bundle.scan.producer.version.as_deref(), Some("0.1.0"));
		assert_eq!(bundle.scan.producer.revision.as_deref(), Some("f22d4a36"));
		assert_eq!(bundle.scan.producer.plugin_version.as_deref(), Some("0.1.14"));
		assert_eq!(bundle.scan.report_ref.as_deref(), Some("report.md"));
		assert_eq!(bundle.scan.sarif_ref.as_deref(), Some("results.sarif"));
		assert_eq!(bundle.report.as_deref(), Some("# report\n"));
		assert_eq!(
			bundle
				.sarif
				.as_ref()
				.and_then(|sarif| sarif.get("version"))
				.and_then(Value::as_str),
			Some("2.1.0")
		);
		let scan_ids = bundle
			.scan
			.provenance
			.source_ids
			.as_ref()
			.expect("scan source ids");
		assert_eq!(scan_ids.get("scanId").and_then(Value::as_str), Some("scan_example_001"));
		let upstream = bundle.scan.provenance.upstream.as_ref().expect("upstream");
		assert_eq!(upstream.repository.as_deref(), Some("https://github.com/openai/codex-security"));
		assert_eq!(upstream.revision.as_deref(), Some("f22d4a36"));
		assert_eq!(upstream.package_version.as_deref(), Some("0.1.1"));
		assert_eq!(upstream.plugin_version.as_deref(), Some("0.1.14"));
		assert_eq!(upstream.archive_sha256.as_deref(), Some("13745c49"));
		let bundle_directory = bundle
			.scan
			.provenance
			.metadata
			.as_ref()
			.and_then(|metadata| metadata.get("bundleDirectory"))
			.and_then(Value::as_str)
			.expect("bundle directory metadata");
		let expected_bundle_root = std::path::absolute(&bundle_root).expect("absolute");
		assert_eq!(Path::new(bundle_directory), expected_bundle_root.as_path());
		let coverage = &bundle.scan.coverage;
		assert_eq!(coverage.mode, "repository");
		assert_eq!(coverage.completeness, "complete");
		assert_eq!(coverage.inventory_strategy, "repository");
		assert_eq!(coverage.surfaces.len(), 1);
		assert_eq!(coverage.surfaces[0].label, "Archive extraction");
		assert_eq!(coverage.explicit_exclusions[0].pattern, "vendor/**");
		assert_eq!(coverage.deferred[0].id, "deferred_1");
		assert_eq!(
			coverage
				.open_questions
				.as_ref()
				.and_then(|questions| questions.first())
				.map(|question| question.question.as_str()),
			Some("what about symlinks?")
		);

		assert_eq!(bundle.findings.len(), 1);
		let finding = &bundle.findings[0];
		assert_eq!(finding.rule_id, "path-traversal.archive-extraction");
		assert_eq!(finding.title, "Unsafe archive extraction can escape the output directory");
		assert_eq!(finding.summary, "An attacker-controlled path reaches a filesystem write.");
		assert_eq!(finding.anchor.as_deref(), Some("archive-entry-write-without-containment"));
		assert_eq!(finding.severity.level, "high");
		assert_eq!(finding.severity.score, Some(8.1));
		assert_eq!(finding.severity.scoring_system.as_deref(), Some("CVSS:3.1"));
		assert_eq!(finding.severity.vector.as_deref(), Some("CVSS:3.1/AV:N"));
		assert_eq!(finding.severity.rationale.as_deref(), Some("write outside root"));
		assert_eq!(finding.confidence.level, "high");
		assert_eq!(finding.confidence.rationale.as_deref(), Some("direct trace"));
		assert_eq!(finding.taxonomy.category, "path-traversal");
		assert_eq!(finding.taxonomy.cwe, vec!["CWE-22".to_string()]);
		assert_eq!(finding.remediation.as_deref(), Some("Normalize destinations."));
		assert_eq!(finding.validation.status, "validated");
		assert_eq!(
			finding.validation.summary.as_deref(),
			Some("{\"status\":\"validated\",\"summary\":\"reproduced\"}")
		);
		assert_eq!(finding.disposition.status, "open");
		assert_eq!(
			finding
				.extensions
				.as_ref()
				.and_then(|extensions| extensions.get("extra"))
				.and_then(Value::as_bool),
			Some(true)
		);
		let location = &finding.occurrences[0].locations[0];
		assert_eq!(location.path, "src/extract.py");
		assert_eq!(location.start_line, 41);
		assert_eq!(location.end_line, Some(44));
		assert_eq!(location.role.as_deref(), Some("sink"));
		assert_eq!(finding.evidence.len(), 1);
		assert_eq!(finding.evidence[0].kind, "code");
		assert_eq!(finding.evidence[0].label, "Sink");
		assert_eq!(finding.evidence[0].explanation, "writes the archive entry");
		assert_eq!(finding.evidence[0].excerpt.as_deref(), Some("open(destination, \"w\")"));
		assert_eq!(
			finding.evidence[0]
				.location
				.as_ref()
				.map(|location| location.path.as_str()),
			Some("src/extract.py")
		);
		assert_eq!(finding.occurrences[0].evidence_ids, vec![finding.evidence[0].id.clone()]);
		let source_ids = finding
			.provenance
			.source_ids
			.as_ref()
			.expect("finding source ids");
		assert_eq!(source_ids.get("scanId").and_then(Value::as_str), Some("scan_example_001"));
		assert_eq!(
			source_ids.get("findingId").and_then(Value::as_str),
			Some("csf_852f90d6e1177502ff113d4a")
		);
		assert_eq!(
			source_ids.get("occurrenceId").and_then(Value::as_str),
			Some("occ_e79cb19591e696572a1c22be")
		);
		let vendor = finding
			.provenance
			.vendor_fingerprints
			.as_ref()
			.expect("vendor fingerprints");
		assert_eq!(
			vendor.get("codex-security/v1").and_then(Value::as_str),
			Some("codex-security/v1:sha256:990a")
		);
		assert_eq!(
			finding
				.provenance
				.metadata
				.as_ref()
				.and_then(|metadata| metadata.get("source"))
				.and_then(Value::as_str),
			Some("local_plugin")
		);
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn accepts_scan_manifest_fallback_and_missing_coverage() {
		let (root, repository) = repository("fallback");
		touch(&repository, "src/extract.py");
		let bundle_root = root.join("bundle");
		let mut fallback_manifest = manifest("scan_example_002");
		fallback_manifest["scan"]["target"]["kind"] = json!("git_diff");
		fallback_manifest["scan"]["target"]
			.as_object_mut()
			.expect("target object")
			.remove("snapshotDigest");
		write_bundle(
			&bundle_root,
			"scan-manifest.json",
			&fallback_manifest,
			&findings_document(
				"scan_example_002",
				json!([one_finding(json!([{
					"path": "./src\\extract.py",
					"startLine": 1
				}]))]),
			),
			None,
		);
		let bundle =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_fallback"))
				.expect("import fallback");
		assert_eq!(bundle.scan.target.kind, "ref_diff");
		assert_eq!(bundle.scan.target.display_name, "example/repo");
		assert_eq!(bundle.scan.target.tree_digest.len(), 64);
		assert_eq!(bundle.scan.coverage.mode, "imported");
		assert_eq!(bundle.scan.coverage.completeness, "unknown");
		assert_eq!(bundle.scan.coverage.inventory_strategy, "imported");
		assert_eq!(bundle.scan.coverage.include_paths, vec!["src/".to_string()]);
		assert_eq!(bundle.scan.coverage.exclude_paths, vec!["vendor/".to_string()]);
		assert_eq!(bundle.scan.coverage.deferred[0].id, "codex-coverage");
		assert_eq!(bundle.findings[0].occurrences[0].locations[0].path, "src/extract.py");
		assert_eq!(bundle.findings[0].severity.level, "informational");
		assert_eq!(bundle.findings[0].confidence.level, "medium");
		assert!(bundle.report.is_none());
		assert!(bundle.sarif.is_none());
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn rejects_locations_and_evidence_outside_the_repository() {
		let (root, repository) = repository("locations");
		let bundle_root = root.join("bundle");
		write_bundle(
			&bundle_root,
			"manifest.json",
			&manifest("scan_paths"),
			&findings_document("scan_paths", json!([])),
			None,
		);
		for path in
			["../outside.ts", "src/../../outside.ts", "/etc/passwd", "C:/Windows/system.ini", ""]
		{
			write_json(
				&bundle_root.join("findings.json"),
				&findings_document(
					"scan_paths",
					json!([one_finding(json!([{"path": path, "startLine": 1}]))]),
				),
			);
			let error =
				import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_paths"))
					.expect_err("reject unsafe location");
			assert!(
				error.to_string().contains("repository-relative"),
				"unexpected error for {path}: {error}"
			);
		}
		write_json(
			&bundle_root.join("findings.json"),
			&findings_document(
				"scan_paths",
				json!([{
					"findingId": "csf_1",
					"ruleId": "path.traversal",
					"locations": [],
					"codeEvidence": [{"label": "escape", "path": "../outside.ts", "startLine": 1}]
				}]),
			),
		);
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_paths"))
				.expect_err("reject unsafe evidence");
		assert!(error.to_string().contains("repository-relative"));
		write_json(
			&bundle_root.join("findings.json"),
			&findings_document(
				"scan_paths",
				json!([one_finding(json!([{"path": "src/missing.ts", "startLine": 1}]))]),
			),
		);
		let bundle =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_paths"))
				.expect("nonexistent path inside repository is allowed");
		assert_eq!(bundle.findings[0].occurrences[0].locations[0].path, "src/missing.ts");
		let _ = fs::remove_dir_all(root);
	}

	#[cfg(unix)]
	#[test]
	fn rejects_symbolic_links_that_escape_the_repository() {
		use std::os::unix::fs::symlink;

		let (root, repository) = repository("symlink");
		let outside = root.join("outside");
		fs::create_dir_all(&outside).expect("outside");
		touch(&outside, "secret.ts");
		symlink(&outside, repository.join("link")).expect("symlink");
		symlink(outside.join("missing.ts"), repository.join("dangling.ts"))
			.expect("dangling symlink");
		let bundle_root = root.join("bundle");
		write_bundle(
			&bundle_root,
			"manifest.json",
			&manifest("scan_symlink"),
			&findings_document(
				"scan_symlink",
				json!([one_finding(json!([{"path": "link/secret.ts", "startLine": 1}]))]),
			),
			None,
		);
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_symlink"))
				.expect_err("reject symlink escape");
		assert!(error.to_string().contains("outside the repository"), "{error}");

		write_json(
			&bundle_root.join("findings.json"),
			&findings_document(
				"scan_symlink",
				json!([one_finding(json!([{"path": "dangling.ts", "startLine": 1}]))]),
			),
		);
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_symlink"))
				.expect_err("reject dangling symlink");
		assert!(error.to_string().contains("dangling symbolic link"), "{error}");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn rejects_zero_line_numbers() {
		let (root, repository) = repository("zero-lines");
		touch(&repository, "src/extract.py");
		let bundle_root = root.join("bundle");
		write_bundle(
			&bundle_root,
			"manifest.json",
			&manifest("scan_zero"),
			&findings_document(
				"scan_zero",
				json!([one_finding(json!([{"path": "src/extract.py", "startLine": 0}]))]),
			),
			None,
		);
		let error = import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_zero"))
			.expect_err("zero finding start line");
		assert!(error.to_string().contains("startLine"), "{error}");

		write_json(
			&bundle_root.join("findings.json"),
			&findings_document(
				"scan_zero",
				json!([one_finding(json!([{"path": "src/extract.py", "startLine": 1, "endLine": 0}]))]),
			),
		);
		let error = import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_zero"))
			.expect_err("zero finding end line");
		assert!(error.to_string().contains("endLine"), "{error}");

		let mut evidence = one_finding(json!([{"path": "src/extract.py", "startLine": 1}]));
		evidence["codeEvidence"] = json!([{"path": "src/extract.py", "startLine": 0}]);
		write_json(
			&bundle_root.join("findings.json"),
			&findings_document("scan_zero", json!([evidence])),
		);
		let error = import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_zero"))
			.expect_err("zero evidence start line");
		assert!(error.to_string().contains("startLine"), "{error}");

		let mut evidence = one_finding(json!([{"path": "src/extract.py", "startLine": 1}]));
		evidence["codeEvidence"] = json!([{"path": "src/extract.py", "startLine": 1, "endLine": 0}]);
		write_json(
			&bundle_root.join("findings.json"),
			&findings_document("scan_zero", json!([evidence])),
		);
		let error = import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_zero"))
			.expect_err("zero evidence end line");
		assert!(error.to_string().contains("endLine"), "{error}");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn validates_document_schema_and_scan_id_agreement() {
		let (root, repository) = repository("schema");
		let bundle_root = root.join("bundle");
		let mut bad_manifest = manifest("scan_schema");
		bad_manifest["documentType"] = json!("codex-security.other");
		write_bundle(
			&bundle_root,
			"manifest.json",
			&bad_manifest,
			&findings_document("scan_schema", json!([])),
			None,
		);
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_schema"))
				.expect_err("bad manifest");
		assert!(
			error
				.to_string()
				.contains("Unsupported Codex Security scan manifest")
		);

		write_bundle(
			&bundle_root,
			"manifest.json",
			&manifest("scan_schema"),
			&findings_document("scan_schema", json!([])),
			None,
		);
		write_json(
			&bundle_root.join("findings.json"),
			&json!({
				"documentType": "codex-security.findings",
				"schemaVersion": "2.0",
				"scanId": "scan_schema",
				"findings": []
			}),
		);
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_schema"))
				.expect_err("bad findings version");
		assert!(
			error
				.to_string()
				.contains("Unsupported Codex Security findings document")
		);

		write_json(
			&bundle_root.join("findings.json"),
			&findings_document("different_scan", json!([])),
		);
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_schema"))
				.expect_err("scan id mismatch");
		assert!(error.to_string().contains("scan IDs do not agree"));

		write_json(&bundle_root.join("findings.json"), &findings_document("scan_schema", json!([])));
		write_json(
			&bundle_root.join("coverage.json"),
			&json!({
				"documentType": "codex-security.coverage",
				"schemaVersion": "1.0",
				"scanId": "different_scan"
			}),
		);
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_schema"))
				.expect_err("coverage id mismatch");
		assert!(error.to_string().contains("scan IDs do not agree"));

		fs::write(bundle_root.join("coverage.json"), b"{not json").expect("bad coverage");
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_schema"))
				.expect_err("malformed coverage");
		assert!(
			error
				.to_string()
				.contains("cannot parse Codex Security coverage document")
		);
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn rejects_missing_malformed_and_unsupported_manifest_documents() {
		let (root, repository) = repository("manifest-errors");
		let bundle_root = root.join("bundle");
		fs::create_dir_all(&bundle_root).expect("bundle directory");

		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_errors"))
				.expect_err("missing manifest");
		let message = error.to_string();
		assert!(message.contains("missing"), "{message}");
		assert!(message.contains("scan manifest"), "{message}");

		fs::write(bundle_root.join("manifest.json"), b"{not json").expect("malformed manifest");
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_errors"))
				.expect_err("malformed manifest");
		assert!(
			error
				.to_string()
				.contains("cannot parse Codex Security scan manifest"),
			"{error}"
		);

		let bad_version = {
			let mut value = manifest("scan_errors");
			value["schemaVersion"] = json!("2.0");
			value
		};
		write_json(&bundle_root.join("manifest.json"), &bad_version);
		write_json(&bundle_root.join("findings.json"), &findings_document("scan_errors", json!([])));
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_errors"))
				.expect_err("unsupported manifest version");
		assert!(
			error
				.to_string()
				.contains("Unsupported Codex Security scan manifest"),
			"{error}"
		);

		write_json(&bundle_root.join("manifest.json"), &manifest("scan_errors"));
		fs::remove_file(bundle_root.join("findings.json")).expect("remove findings");
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_errors"))
				.expect_err("missing findings document");
		let message = error.to_string();
		assert!(message.contains("missing"), "{message}");
		assert!(message.contains("findings document"), "{message}");

		fs::write(bundle_root.join("findings.json"), b"[not json").expect("malformed findings");
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_errors"))
				.expect_err("malformed findings document");
		assert!(
			error
				.to_string()
				.contains("cannot parse Codex Security findings document"),
			"{error}"
		);

		write_json(
			&bundle_root.join("findings.json"),
			&json!({
				"documentType": "codex-security.findings",
				"schemaVersion": "1.0",
				"scanId": "scan_errors"
			}),
		);
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_errors"))
				.expect_err("missing findings array");
		assert!(error.to_string().contains("missing its findings array"), "{error}");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn deduplicates_equivalent_findings() {
		let (root, repository) = repository("dedupe");
		touch(&repository, "src/extract.py");
		let bundle_root = root.join("bundle");
		let duplicate = one_finding(json!([{"path": "src/extract.py", "startLine": 1}]));
		write_bundle(
			&bundle_root,
			"manifest.json",
			&manifest("scan_dedupe"),
			&findings_document("scan_dedupe", json!([duplicate.clone(), duplicate])),
			None,
		);
		let bundle =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_dedupe"))
				.expect("import duplicate findings");
		assert_eq!(bundle.findings.len(), 1);
		assert_eq!(bundle.scan.finding_ids, vec![bundle.findings[0].id.clone()]);
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn rejects_malformed_optional_artifacts() {
		let (root, repository) = repository("optional-errors");
		touch(&repository, "src/extract.py");
		let bundle_root = root.join("bundle");
		write_bundle(
			&bundle_root,
			"manifest.json",
			&manifest("scan_optional"),
			&findings_document(
				"scan_optional",
				json!([one_finding(json!([{"path": "src/extract.py", "startLine": 1}]))]),
			),
			None,
		);

		fs::write(bundle_root.join("PROVENANCE.json"), b"{not json").expect("bad provenance");
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_optional"))
				.expect_err("malformed provenance");
		assert!(
			error
				.to_string()
				.contains("cannot parse Codex Security provenance"),
			"{error}"
		);

		fs::remove_file(bundle_root.join("PROVENANCE.json")).expect("remove provenance");
		fs::write(bundle_root.join("report.md"), b"\xff").expect("invalid report UTF-8");
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_optional"))
				.expect_err("invalid report UTF-8");
		assert!(error.to_string().contains("not valid UTF-8"), "{error}");

		fs::remove_file(bundle_root.join("report.md")).expect("remove report");
		fs::create_dir_all(bundle_root.join("exports")).expect("exports directory");
		fs::write(bundle_root.join("exports/results.sarif"), b"{not json").expect("bad sarif");
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_optional"))
				.expect_err("malformed sarif");
		assert!(
			error
				.to_string()
				.contains("cannot parse Codex Security SARIF artifact"),
			"{error}"
		);
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn rejects_oversized_bundle_artifacts() {
		let (root, repository) = repository("oversize");
		touch(&repository, "src/extract.py");
		let bundle_root = root.join("bundle");
		write_bundle(
			&bundle_root,
			"manifest.json",
			&manifest("scan_oversize"),
			&findings_document(
				"scan_oversize",
				json!([one_finding(json!([{"path": "src/extract.py", "startLine": 1}]))]),
			),
			None,
		);
		let report = bundle_root.join("report.md");
		let file = fs::File::create(&report).expect("oversized report");
		file
			.set_len(crate::security::types::MAX_SECURITY_DOCUMENT_BYTES as u64 + 1)
			.expect("extend report");
		let error =
			import_codex_security_bundle(&bundle_root, &options(&repository, "secscan_oversize"))
				.expect_err("oversized report");
		assert!(error.to_string().contains("security document limit"), "{error}");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn persists_imported_bundle_in_the_project_store() {
		let (root, repository) = repository("store");
		touch(&repository, "src/extract.py");
		let bundle_root = root.join("bundle");
		write_bundle(
			&bundle_root,
			"manifest.json",
			&manifest("scan_store"),
			&findings_document(
				"scan_store",
				json!([one_finding(json!([{"path": "src/extract.py", "startLine": 1}]))]),
			),
			None,
		);
		let state_root = root.join("state");
		let store = SecurityStore::open(&repository, Some(state_root.as_path())).expect("open store");
		let mut store_options = options(&repository, "secscan_store");
		store_options.repository_root = PathBuf::from(store.repository_root());
		let bundle = import_codex_security_bundle_into_store(&bundle_root, &store, &store_options)
			.expect("import and persist");
		assert_eq!(bundle.scan.id, "secscan_store");
		let stored = store
			.get_bundle("secscan_store")
			.expect("read bundle")
			.expect("stored bundle");
		assert_eq!(stored.findings.len(), 1);
		assert_eq!(stored.scan.producer.kind, "codex-security-bundle");
		assert_eq!(stored.findings[0].rule_id, "path.traversal");
		assert!(
			store
				.list_scans()
				.expect("list scans")
				.iter()
				.any(|scan| scan.id == "secscan_store")
		);
		let _ = fs::remove_dir_all(root);
	}
}
