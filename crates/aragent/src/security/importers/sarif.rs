//! SARIF 2.1.0 importer for security scan bundles.
//!
//! Anchors: `packages/coding-agent/src/security/importers/sarif.ts`.
//!
//! Producer input is untrusted: a result location becomes a repository path
//! only when it is a `file://` URI or a repository-relative URI with no `..`
//! component, and the canonical (symlink-resolved) target must stay inside the
//! repository root. Rules, fingerprints, severities, and dispositions follow
//! the donor importer so generic SARIF and native scans land in one store.

use std::{
	collections::{BTreeMap, HashSet},
	ffi::OsString,
	fs,
	path::{Component, Path, PathBuf},
};

use serde_json::{Map, Value};

use crate::security::{
	ids::{
		canonical_security_json, create_security_finding_fingerprint, create_security_finding_id,
		create_security_occurrence_id, create_security_scan_id, encode_security_project_key,
		sha256_hex,
	},
	types::{
		self, SecurityConfidence, SecurityCoverage, SecurityCoverageDeferred, SecurityDisposition,
		SecurityFinding, SecurityLocation, SecurityOccurrence, SecurityProducer, SecurityProvenance,
		SecurityScanBundle, SecuritySeverity, SecurityTaxonomy, SecurityValidation,
	},
};

const DEFAULT_PRODUCER_NAME: &str = "SARIF importer";
const VALIDATION_STATUSES: [&str; 5] = ["unvalidated", "validated", "rejected", "partial", "error"];
const DISPOSITION_STATUSES: [&str; 5] =
	["open", "false_positive", "accepted_risk", "fixed", "wont_fix"];

/// Options for [`import_sarif`] and [`import_sarif_file`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SarifImportOptions {
	/// Repository the SARIF locations must resolve inside; canonicalized on use.
	pub repository_root: PathBuf,
	/// Imported document path, recorded as `provenance.metadata.sourcePath`.
	pub source_path: Option<PathBuf>,
	/// Fixed scan timestamp for reproducible imports; defaults to now.
	pub created_at: Option<String>,
	/// Fixed scan id; defaults to a random `secscan_<uuid-without-dashes>`.
	pub scan_id: Option<String>,
}

struct ImportContext<'a> {
	root: &'a Path,
	scan_id: &'a str,
	created_at: &'a str,
	producer: &'a SecurityProducer,
	source_metadata: Option<&'a Map<String, Value>>,
}

/// Import a parsed SARIF 2.1.0 document as a completed, imported scan bundle.
pub fn import_sarif(
	input: &Value,
	options: &SarifImportOptions,
) -> crate::Result<SecurityScanBundle> {
	if input.get("version").and_then(Value::as_str) != Some("2.1.0") {
		return Err(crate::Error::new("Expected SARIF 2.1.0 input"));
	}
	let Some(runs) = input.get("runs").and_then(Value::as_array) else {
		return Err(crate::Error::new("Expected SARIF 2.1.0 input"));
	};
	let root = canonical_repository_root(&options.repository_root)?;
	let scan_id = options
		.scan_id
		.clone()
		.unwrap_or_else(|| create_security_scan_id(&crate::advisor_identity::uuid_v7()));
	let created_at = options.created_at.clone().unwrap_or_else(now_iso);
	let source_metadata = options.source_path.as_ref().map(|path| {
		let mut metadata = Map::new();
		metadata.insert("sourcePath".to_string(), Value::String(path.to_string_lossy().into_owned()));
		metadata
	});

	let mut findings = Vec::new();
	let mut seen_fingerprints = HashSet::new();
	let mut producer_name = DEFAULT_PRODUCER_NAME.to_string();
	let mut producer_version: Option<String> = None;

	for run in runs {
		let driver = run.get("tool").and_then(|tool| tool.get("driver"));
		if let Some(name) = driver
			.and_then(|driver| driver.get("name"))
			.and_then(Value::as_str)
		{
			if !name.is_empty() {
				producer_name = name.to_string();
			}
		}
		if let Some(version) = driver
			.and_then(|driver| driver.get("version"))
			.and_then(Value::as_str)
		{
			producer_version = Some(version.to_string());
		}
		let producer = SecurityProducer {
			kind: "sarif-import".to_string(),
			name: producer_name.clone(),
			version: producer_version.clone(),
			vendor: None,
			revision: None,
			plugin_version: None,
		};
		let context = ImportContext {
			root: &root,
			scan_id: &scan_id,
			created_at: &created_at,
			producer: &producer,
			source_metadata: source_metadata.as_ref(),
		};
		for result in run
			.get("results")
			.and_then(Value::as_array)
			.into_iter()
			.flatten()
		{
			let finding = finding_from_result(result, run, &context)?;
			if seen_fingerprints.insert(finding.fingerprint.clone()) {
				findings.push(finding);
			}
		}
	}

	let producer = SecurityProducer {
		kind: "sarif-import".to_string(),
		name: producer_name,
		version: producer_version,
		vendor: None,
		revision: None,
		plugin_version: None,
	};
	let scan_provenance = SecurityProvenance {
		producer: producer.clone(),
		created_at: created_at.clone(),
		imported_at: Some(now_iso()),
		source_ids: None,
		vendor_fingerprints: None,
		upstream: None,
		metadata: source_metadata,
	};
	let coverage = imported_coverage();
	let finding_ids = findings.iter().map(|finding| finding.id.clone()).collect();
	let report = format!(
		"# Imported SARIF security results\n\nProducer: {}\n\nFindings: {}\n",
		producer.name,
		findings.len()
	);
	let bundle = SecurityScanBundle {
		scan: types::SecurityScan {
			document_type: "omp-security.scan".to_string(),
			schema_version: "1.0".to_string(),
			id: scan_id,
			project_key: encode_security_project_key(&root.to_string_lossy()),
			status: "completed".to_string(),
			created_at: created_at.clone(),
			started_at: None,
			completed_at: Some(created_at),
			plan: None,
			target: types::SecurityTarget {
				kind: "imported".to_string(),
				repository_root: root.to_string_lossy().into_owned(),
				display_name: root
					.file_name()
					.map_or_else(String::new, |name| name.to_string_lossy().into_owned()),
				revision: None,
				base_revision: None,
				head_revision: None,
				include_paths: Vec::new(),
				exclude_paths: Vec::new(),
				tree_digest: sha256_hex(serde_json::to_string(input).unwrap_or_default().as_bytes()),
			},
			producer,
			provenance: scan_provenance,
			finding_ids,
			coverage,
			report_ref: Some("report.md".to_string()),
			sarif_ref: Some("results.sarif".to_string()),
			error: None,
			metrics: None,
		},
		findings,
		report: Some(report),
		sarif: Some(input.clone()),
	};
	types::validate_bundle(&bundle)?;
	Ok(bundle)
}

/// Read and import a SARIF document, recording the resolved file as the source.
pub fn import_sarif_file(
	path: &Path,
	options: &SarifImportOptions,
) -> crate::Result<SecurityScanBundle> {
	let bytes = super::read_security_document(path, "SARIF file")?;
	let input: Value = serde_json::from_slice(&bytes).map_err(|error| {
		crate::Error::new(format!("cannot parse SARIF file {}: {error}", path.display()))
	})?;
	let source_path = std::path::absolute(path).map_err(|error| {
		crate::Error::new(format!("cannot resolve SARIF file {}: {error}", path.display()))
	})?;
	let options = SarifImportOptions { source_path: Some(source_path), ..options.clone() };
	import_sarif(&input, &options)
}

fn now_iso() -> String {
	crate::output::iso8601(std::time::SystemTime::now())
}

fn canonical_repository_root(path: &Path) -> crate::Result<PathBuf> {
	fs::canonicalize(path).map_err(|error| {
		crate::Error::new(format!("cannot resolve SARIF repository root {}: {error}", path.display()))
	})
}

fn tool_components<'a>(tool: &'a Value) -> Vec<&'a Value> {
	let mut components = Vec::new();
	if let Some(driver) = tool.get("driver") {
		components.push(driver);
	}
	if let Some(extensions) = tool.get("extensions").and_then(Value::as_array) {
		components.extend(extensions);
	}
	components
}

fn rule_owning_component<'a>(run: &'a Value, rule: &Value) -> Option<&'a Value> {
	let tool = run.get("tool")?;
	tool_components(tool).into_iter().find(|component| {
		component
			.get("rules")
			.and_then(Value::as_array)
			.is_some_and(|rules| rules.iter().any(|candidate| std::ptr::eq(candidate, rule)))
	})
}

fn rule_by_id_in_component<'a>(component: &'a Value, id: &str) -> Option<&'a Value> {
	component
		.get("rules")
		.and_then(Value::as_array)
		.into_iter()
		.flatten()
		.find(|rule| rule.get("id").and_then(Value::as_str) == Some(id))
}

fn rule_by_index_in_component<'a>(component: &'a Value, index: usize) -> Option<&'a Value> {
	component
		.get("rules")
		.and_then(Value::as_array)
		.and_then(|rules| rules.get(index))
}

fn rule_by_id<'a>(run: &'a Value, id: &str) -> Option<&'a Value> {
	let tool = run.get("tool")?;
	tool_components(tool)
		.into_iter()
		.find_map(|component| rule_by_id_in_component(component, id))
}

fn rule_by_index<'a>(run: &'a Value, index: usize) -> Option<&'a Value> {
	let tool = run.get("tool")?;
	tool_components(tool)
		.into_iter()
		.find_map(|component| rule_by_index_in_component(component, index))
}

fn rule_component<'a>(run: &'a Value, reference: &Value) -> Option<&'a Value> {
	let tool = run.get("tool")?;
	let driver = tool.get("driver");
	let Some(component) = reference.get("toolComponent") else {
		return driver;
	};
	if let Some(index) = component
		.get("index")
		.and_then(Value::as_u64)
		.and_then(|index| usize::try_from(index).ok())
	{
		if let Some(extension) = tool
			.get("extensions")
			.and_then(Value::as_array)
			.and_then(|extensions| extensions.get(index))
		{
			return Some(extension);
		}
	}
	if let Some(name) = component
		.get("name")
		.and_then(Value::as_str)
		.filter(|name| !name.is_empty())
	{
		if let Some(matched) = tool_components(tool)
			.into_iter()
			.find(|candidate| candidate.get("name").and_then(Value::as_str) == Some(name))
		{
			return Some(matched);
		}
	}
	if let Some(guid) = component
		.get("guid")
		.and_then(Value::as_str)
		.filter(|guid| !guid.is_empty())
	{
		if let Some(matched) = tool_components(tool)
			.into_iter()
			.find(|candidate| candidate.get("guid").and_then(Value::as_str) == Some(guid))
		{
			return Some(matched);
		}
	}
	if component.get("index").is_some()
		|| component.get("name").is_some()
		|| component.get("guid").is_some()
	{
		return None;
	}
	driver
}

fn rule_component_is_explicit(reference: &Value) -> bool {
	reference.get("toolComponent").is_some_and(|component| {
		component.get("index").is_some()
			|| component.get("guid").is_some()
			|| component.get("name").is_some()
	})
}

fn rule_reference_rule<'a>(run: &'a Value, reference: &Value) -> Option<&'a Value> {
	let id = rule_id_from_value(reference);
	let index = reference
		.get("index")
		.and_then(Value::as_u64)
		.and_then(|index| usize::try_from(index).ok());
	if let Some(component) = rule_component(run, reference) {
		if let Some(rule) = id
			.and_then(|id| rule_by_id_in_component(component, id))
			.or_else(|| index.and_then(|index| rule_by_index_in_component(component, index)))
		{
			return Some(rule);
		}
	}
	if rule_component_is_explicit(reference) {
		return None;
	}
	id.and_then(|id| rule_by_id(run, id))
		.or_else(|| index.and_then(|index| rule_by_index(run, index)))
}

fn rule_id_from_value(rule: &Value) -> Option<&str> {
	rule
		.get("id")
		.and_then(Value::as_str)
		.filter(|id| !id.is_empty())
}

fn message_string(value: &Value) -> Option<&str> {
	value.as_str().or_else(|| {
		value
			.get("text")
			.and_then(Value::as_str)
			.or_else(|| value.get("markdown").and_then(Value::as_str))
	})
}

fn message_template(
	rule: Option<&Value>,
	component: Option<&Value>,
	run: &Value,
	id: &str,
) -> Option<String> {
	if let Some(template) = rule
		.and_then(|rule| rule.get("messageStrings"))
		.and_then(|strings| strings.get(id))
		.and_then(message_string)
	{
		return Some(template.to_string());
	}
	if let Some(component) = component {
		return component
			.get("globalMessageStrings")
			.and_then(|strings| strings.get(id))
			.and_then(message_string)
			.map(str::to_string);
	}
	let tool = run.get("tool")?;
	tool_components(tool).into_iter().find_map(|component| {
		component
			.get("globalMessageStrings")
			.and_then(|strings| strings.get(id))
			.and_then(message_string)
			.map(str::to_string)
	})
}

fn interpolate_message(template: &str, arguments: Option<&Vec<Value>>) -> String {
	let arguments = arguments.map(|arguments| {
		arguments
			.iter()
			.map(|argument| match argument {
				Value::String(value) => value.clone(),
				value => value.to_string(),
			})
			.collect::<Vec<_>>()
	});
	let mut message = String::with_capacity(template.len());
	let mut characters = template.chars().peekable();
	while let Some(character) = characters.next() {
		match character {
			'{' => {
				if characters.peek() == Some(&'{') {
					characters.next();
					message.push('{');
					continue;
				}
				let mut digits = String::new();
				let mut closed = false;
				while let Some(next) = characters.peek().copied() {
					if next == '}' {
						characters.next();
						closed = true;
						break;
					}
					if !next.is_ascii_digit() {
						break;
					}
					digits.push(next);
					characters.next();
				}
				if closed {
					if let Some(argument) = digits.parse::<usize>().ok().and_then(|index| {
						arguments
							.as_ref()
							.and_then(|arguments| arguments.get(index))
					}) {
						message.push_str(argument);
						continue;
					}
					message.push('{');
					message.push_str(&digits);
					message.push('}');
				} else {
					message.push('{');
					message.push_str(&digits);
				}
			},
			'}' => {
				if characters.peek() == Some(&'}') {
					characters.next();
				}
				message.push('}');
			},
			_ => message.push(character),
		}
	}
	message
}

fn resolve_message(
	message: &Value,
	rule: Option<&Value>,
	component: Option<&Value>,
	run: &Value,
) -> Option<String> {
	let id = message
		.get("id")
		.and_then(Value::as_str)
		.filter(|id| !id.is_empty())?;
	let template = message_template(rule, component, run, id)?;
	Some(interpolate_message(&template, message.get("arguments").and_then(Value::as_array)))
}

fn finding_from_result(
	result: &Value,
	run: &Value,
	context: &ImportContext<'_>,
) -> crate::Result<SecurityFinding> {
	let explicit_rule_id = result
		.get("ruleId")
		.and_then(Value::as_str)
		.filter(|rule_id| !rule_id.is_empty());
	let explicit_rule = explicit_rule_id.and_then(|rule_id| rule_by_id(run, rule_id));
	let rule_reference = result.get("rule");
	let referenced_rule = rule_reference.and_then(|reference| rule_reference_rule(run, reference));
	let referenced_rule_id = rule_reference.and_then(rule_id_from_value);
	let indexed_rule = result
		.get("ruleIndex")
		.and_then(Value::as_u64)
		.and_then(|index| usize::try_from(index).ok())
		.and_then(|index| rule_by_index(run, index));
	let reference_has_component =
		rule_reference.is_some_and(|reference| reference.get("toolComponent").is_some());
	let rule = if reference_has_component {
		referenced_rule
	} else {
		referenced_rule.or(explicit_rule).or(indexed_rule)
	};
	let rule_id = explicit_rule_id
		.or(referenced_rule_id)
		.or_else(|| referenced_rule.and_then(rule_id_from_value))
		.or_else(|| indexed_rule.and_then(rule_id_from_value))
		.unwrap_or("sarif.unknown");
	let message_component = rule
		.and_then(|rule| rule_owning_component(run, rule))
		.or_else(|| rule_reference.and_then(|reference| rule_component(run, reference)));
	let locations = normalize_sarif_locations(result, run, context.root)?;
	let vendor_fingerprints = vendor_fingerprints(result);
	let first_vendor_fingerprint = vendor_fingerprints
		.values()
		.find(|fingerprint| !fingerprint.is_empty())
		.cloned();
	let category = result
		.get("properties")
		.and_then(|properties| properties.get("category"))
		.and_then(Value::as_str)
		.map_or_else(|| rule_category(rule_id), str::to_string);
	let message_value = result.get("message");
	let message_arguments = message_value
		.and_then(|message| message.get("arguments"))
		.and_then(Value::as_array);
	let message = message_value
		.and_then(|message| {
			message
				.get("text")
				.and_then(Value::as_str)
				.or_else(|| message.get("markdown").and_then(Value::as_str))
		})
		.map(|text| interpolate_message(text, message_arguments))
		.or_else(|| {
			message_value.and_then(|message| resolve_message(message, rule, message_component, run))
		})
		.or_else(|| {
			rule
				.and_then(|rule| rule.get("shortDescription"))
				.and_then(|description| description.get("text"))
				.and_then(Value::as_str)
				.map(str::to_string)
		})
		.unwrap_or_else(|| rule_id.to_string());
	let anchor = first_vendor_fingerprint
		.unwrap_or_else(|| semantic_result_anchor(rule_id, &category, &message, &locations));
	let fingerprint =
		create_security_finding_fingerprint(rule_id, &category, Some(anchor.as_str()), &locations);
	let tags = rule_tags(rule);
	let cwe = tags.iter().filter_map(|tag| cwe_tag(tag)).collect();
	let title = rule
		.and_then(|rule| rule.get("shortDescription"))
		.and_then(|description| description.get("text"))
		.and_then(Value::as_str)
		.or_else(|| {
			rule
				.and_then(|rule| rule.get("name"))
				.and_then(Value::as_str)
		})
		.unwrap_or(rule_id)
		.to_string();
	let score = security_score(result);
	let severity = SecuritySeverity {
		level: severity_level(result, score),
		score,
		scoring_system: None,
		vector: None,
		rationale: None,
	};
	let provenance = SecurityProvenance {
		producer: context.producer.clone(),
		created_at: context.created_at.to_string(),
		imported_at: Some(now_iso()),
		source_ids: None,
		vendor_fingerprints: Some(vendor_fingerprint_map(&vendor_fingerprints)),
		upstream: None,
		metadata: context.source_metadata.cloned(),
	};
	let occurrence_id = create_security_occurrence_id(&fingerprint, &locations);
	Ok(SecurityFinding {
		id: create_security_finding_id(&fingerprint),
		scan_id: context.scan_id.to_string(),
		fingerprint,
		rule_id: rule_id.to_string(),
		anchor: Some(anchor),
		title,
		summary: message,
		severity,
		confidence: SecurityConfidence {
			level: "medium".to_string(),
			rationale: Some("Imported from a SARIF producer".to_string()),
		},
		taxonomy: SecurityTaxonomy { category, cwe, tags: Some(tags) },
		occurrences: vec![SecurityOccurrence {
			id: occurrence_id,
			locations,
			evidence_ids: Vec::new(),
		}],
		evidence: Vec::new(),
		remediation: None,
		validation: SecurityValidation {
			status: imported_status(result, "validation", &VALIDATION_STATUSES, "unvalidated"),
			summary: None,
			evidence_ids: Vec::new(),
			validated_at: None,
		},
		disposition: SecurityDisposition {
			status: imported_status(result, "disposition", &DISPOSITION_STATUSES, "open"),
			rationale: None,
			updated_at: None,
			actor: None,
		},
		provenance,
		extensions: None,
	})
}

fn rule_category(rule_id: &str) -> String {
	rule_id
		.split(['.', '/', '-'])
		.next()
		.filter(|segment| !segment.is_empty())
		.unwrap_or("security")
		.to_string()
}

fn rule_tags(rule: Option<&Value>) -> Vec<String> {
	rule
		.and_then(|rule| rule.get("properties"))
		.and_then(|properties| properties.get("tags"))
		.and_then(Value::as_array)
		.map_or_else(Vec::new, |tags| {
			tags
				.iter()
				.filter_map(Value::as_str)
				.map(str::to_string)
				.collect()
		})
}

fn cwe_tag(tag: &str) -> Option<String> {
	let (prefix, suffix) = tag.split_at_checked(4)?;
	if !prefix.eq_ignore_ascii_case("cwe-")
		|| suffix.is_empty()
		|| !suffix.bytes().all(|byte| byte.is_ascii_digit())
	{
		return None;
	}
	Some(tag.to_ascii_uppercase())
}

fn security_score(result: &Value) -> Option<f64> {
	let value = result.get("properties")?.get("security-severity")?;
	let score = match value {
		Value::Number(number) => number.as_f64()?,
		Value::String(text) => text.trim().parse::<f64>().ok()?,
		_ => return None,
	};
	score.is_finite().then_some(score)
}

fn severity_level(result: &Value, score: Option<f64>) -> String {
	if let Some(score) = score {
		if score >= 9.0 {
			return "critical".to_string();
		}
		if score >= 7.0 {
			return "high".to_string();
		}
		if score >= 4.0 {
			return "medium".to_string();
		}
		if score > 0.0 {
			return "low".to_string();
		}
	}
	match result.get("level").and_then(Value::as_str) {
		Some("error") => "high".to_string(),
		Some("warning") => "medium".to_string(),
		Some("note") => "low".to_string(),
		_ => "informational".to_string(),
	}
}

fn imported_status(result: &Value, field: &str, allowed: &[&str], fallback: &str) -> String {
	result
		.get("properties")
		.and_then(|properties| properties.get(field))
		.and_then(Value::as_str)
		.filter(|value| allowed.contains(value))
		.unwrap_or(fallback)
		.to_string()
}

fn vendor_fingerprints(result: &Value) -> BTreeMap<String, String> {
	let mut fingerprints = string_record(result.get("fingerprints"));
	// TS spreads `partialFingerprints` after `fingerprints`, so its keys win.
	fingerprints.extend(string_record(result.get("partialFingerprints")));
	fingerprints
}

fn string_record(value: Option<&Value>) -> BTreeMap<String, String> {
	let mut record = BTreeMap::new();
	for (key, value) in value.and_then(Value::as_object).into_iter().flatten() {
		if let Some(value) = value.as_str() {
			record.insert(key.clone(), value.to_string());
		}
	}
	record
}

fn vendor_fingerprint_map(fingerprints: &BTreeMap<String, String>) -> Map<String, Value> {
	fingerprints
		.iter()
		.map(|(key, value)| (key.clone(), Value::String(value.clone())))
		.collect()
}

fn semantic_result_anchor(
	rule_id: &str,
	category: &str,
	message: &str,
	locations: &[SecurityLocation],
) -> String {
	let material = serde_json::json!({
		"ruleId": rule_id,
		"category": category,
		"message": message,
		"locations": locations,
	});
	format!("sarif-result/v1:sha256:{}", sha256_hex(canonical_security_json(&material).as_bytes()))
}

fn normalize_sarif_locations(
	result: &Value,
	run: &Value,
	root: &Path,
) -> crate::Result<Vec<SecurityLocation>> {
	let mut locations = Vec::new();
	for item in result
		.get("locations")
		.and_then(Value::as_array)
		.into_iter()
		.flatten()
	{
		let Some(physical) = item.get("physicalLocation") else {
			continue;
		};
		let Some(artifact) = physical.get("artifactLocation") else {
			continue;
		};
		if artifact
			.get("uri")
			.and_then(Value::as_str)
			.is_none_or(str::is_empty)
		{
			continue;
		}
		let region = physical.get("region");
		let start_line = match region.and_then(|region| region.get("startLine")) {
			Some(value) if !value.is_null() => {
				positive_u32(value).ok_or_else(|| sarif_line_error("startLine", value))?
			},
			_ => 1,
		};
		let end_line = match region.and_then(|region| region.get("endLine")) {
			Some(value) if !value.is_null() => {
				Some(positive_u32(value).ok_or_else(|| sarif_line_error("endLine", value))?)
			},
			_ => None,
		};
		locations.push(SecurityLocation {
			path: resolve_sarif_artifact_path(artifact, run, root)?,
			start_line,
			end_line,
			start_column: region
				.and_then(|region| region.get("startColumn"))
				.and_then(positive_u32),
			end_column: region
				.and_then(|region| region.get("endColumn"))
				.and_then(positive_u32),
			role: Some("primary".to_string()),
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

fn sarif_line_error(field: &str, value: &Value) -> crate::Error {
	crate::Error::new(format!("SARIF location {field} must be a positive integer: {value}"))
}

fn positive_u32(value: &Value) -> Option<u32> {
	match value {
		Value::Number(number) => number
			.as_u64()
			.filter(|number| (1..=u64::from(u32::MAX)).contains(number))
			.map(|number| number as u32)
			.or_else(|| {
				let number = number.as_f64()?;
				(number.fract() == 0.0 && (1.0..=f64::from(u32::MAX)).contains(&number))
					.then_some(number as u32)
			}),
		Value::String(text) => text.trim().parse::<u32>().ok().filter(|number| *number > 0),
		_ => None,
	}
}

fn imported_coverage() -> SecurityCoverage {
	SecurityCoverage {
		mode: "imported".to_string(),
		completeness: "unknown".to_string(),
		inventory_strategy: "imported".to_string(),
		include_paths: Vec::new(),
		exclude_paths: Vec::new(),
		surfaces: Vec::new(),
		explicit_exclusions: Vec::new(),
		deferred: vec![SecurityCoverageDeferred {
			id: "sarif-coverage".to_string(),
			reason: "SARIF does not define repository coverage".to_string(),
			paths: None,
			surface_ids: None,
		}],
		open_questions: None,
	}
}

fn resolve_sarif_artifact_path(
	artifact: &Value,
	run: &Value,
	root: &Path,
) -> crate::Result<String> {
	let uri = artifact
		.get("uri")
		.and_then(Value::as_str)
		.filter(|uri| !uri.is_empty())
		.ok_or_else(|| crate::Error::new("SARIF artifact location is missing its URI"))?;
	let uri_base_id = match artifact.get("uriBaseId") {
		None | Some(Value::Null) => None,
		Some(Value::String(id)) if id.is_empty() => None,
		Some(Value::String(id)) => Some(id.as_str()),
		Some(_) => {
			return Err(crate::Error::new("SARIF artifact location has a non-string URI base id"));
		},
	};
	let base_directory = resolve_uri_base_directory(uri_base_id, run, root)?;
	let uri = uri.replace('\\', "/");
	let absolute = resolve_uri_path(&uri, &base_directory, root)?;
	if !path_is_within(&absolute, root) {
		return Err(crate::Error::new(format!(
			"SARIF artifact resolves outside the repository: {uri}"
		)));
	}
	super::ensure_path_components_inside(&absolute, root)
		.map_err(|error| crate::Error::new(format!("SARIF artifact {error}: {uri}")))?;
	let canonical = canonicalize_nearest_existing(&absolute).map_err(|error| {
		crate::Error::new(format!("cannot resolve SARIF artifact {uri}: {error}"))
	})?;
	if !path_is_within(&canonical, root) {
		return Err(crate::Error::new(format!(
			"SARIF artifact resolves outside the repository through a symbolic link: {uri}"
		)));
	}
	let relative = canonical.strip_prefix(root).map_err(|_| {
		crate::Error::new(format!("SARIF artifact resolves outside the repository: {uri}"))
	})?;
	Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn resolve_uri_base_directory(
	uri_base_id: Option<&str>,
	run: &Value,
	root: &Path,
) -> crate::Result<PathBuf> {
	let Some(uri_base_id) = uri_base_id else {
		return Ok(root.to_path_buf());
	};
	let declared = run
		.get("originalUriBaseIds")
		.and_then(|bases| bases.get(uri_base_id))
		.and_then(|base| base.get("uri"))
		.and_then(Value::as_str)
		.filter(|uri| !uri.is_empty());
	match declared {
		Some(declared) => base_directory_for_uri(declared, root),
		None if uri_base_id == "%SRCROOT%" => Ok(root.to_path_buf()),
		None => {
			Err(crate::Error::new(format!("SARIF artifact uses an unknown URI base: {uri_base_id}")))
		},
	}
}

fn base_directory_for_uri(uri: &str, root: &Path) -> crate::Result<PathBuf> {
	let uri = uri.replace('\\', "/");
	let (path, directory_hint) = if uri_scheme(&uri).is_some() {
		let decoded = parse_file_url(&uri, "SARIF URI base")?;
		let hint = uri_path_is_directory(&decoded);
		(lexical_normalize(Path::new(&decoded)), hint)
	} else {
		let decoded = percent_decode(&uri, "SARIF URI base")?;
		if decoded.starts_with('/') {
			return Err(crate::Error::new(format!(
				"SARIF URI base must be relative or a file:// URI: {uri}"
			)));
		}
		reject_parent_segments(&decoded, "SARIF URI base")?;
		let hint = uri_path_is_directory(&decoded);
		(lexical_normalize(&root.join(&decoded)), hint)
	};
	let directory = if directory_hint {
		path
	} else {
		path
			.parent()
			.map_or_else(|| path.clone(), Path::to_path_buf)
	};
	if !path_is_within(&directory, root) {
		return Err(crate::Error::new(format!(
			"SARIF URI base resolves outside the repository: {uri}"
		)));
	}
	Ok(directory)
}

fn resolve_uri_path(uri: &str, base_directory: &Path, root: &Path) -> crate::Result<PathBuf> {
	if let Some(scheme) = uri_scheme(uri) {
		if !scheme.eq_ignore_ascii_case("file") {
			return Err(crate::Error::new(format!(
				"SARIF artifact URI must resolve to a repository file: {uri}"
			)));
		}
		let decoded = parse_file_url(uri, "SARIF artifact")?;
		let absolute = lexical_normalize(Path::new(&decoded));
		if !absolute.is_absolute() {
			return Err(crate::Error::new(format!(
				"SARIF artifact URI must resolve to a repository file: {uri}"
			)));
		}
		return Ok(absolute);
	}
	let decoded = percent_decode(uri, "SARIF artifact")?;
	if decoded.is_empty() {
		return Err(crate::Error::new("SARIF artifact location is missing its URI"));
	}
	if decoded.starts_with('/') {
		return Err(crate::Error::new(format!(
			"SARIF artifact URI must be relative or a file:// URI: {uri}"
		)));
	}
	reject_parent_segments(&decoded, "SARIF artifact")?;
	let joined = lexical_normalize(&base_directory.join(&decoded));
	if !path_is_within(&joined, root) {
		return Err(crate::Error::new(format!(
			"SARIF artifact resolves outside the repository: {uri}"
		)));
	}
	Ok(joined)
}

fn uri_scheme(uri: &str) -> Option<&str> {
	let separator = uri.find(':')?;
	let candidate = &uri[..separator];
	let mut characters = candidate.chars();
	let first = characters.next()?;
	if !first.is_ascii_alphabetic() {
		return None;
	}
	characters
		.all(|character| character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.'))
		.then_some(candidate)
}

fn parse_file_url(uri: &str, label: &str) -> crate::Result<String> {
	let separator = uri
		.find(':')
		.ok_or_else(|| crate::Error::new(format!("{label} must be a file:// URI: {uri}")))?;
	if !uri[..separator].eq_ignore_ascii_case("file") {
		return Err(crate::Error::new(format!("{label} must be a file:// URI: {uri}")));
	}
	let rest = &uri[separator + 1..];
	let path = if let Some(after_slashes) = rest.strip_prefix("//") {
		let (authority, path) = match after_slashes.split_once('/') {
			Some((authority, path)) => (authority, path),
			None => (after_slashes, ""),
		};
		if !authority.is_empty() && !authority.eq_ignore_ascii_case("localhost") {
			return Err(crate::Error::new(format!("{label} URI must not name a remote host: {uri}")));
		}
		if path.is_empty() {
			"/".to_string()
		} else {
			format!("/{path}")
		}
	} else if rest.starts_with('/') {
		rest.to_string()
	} else {
		return Err(crate::Error::new(format!("{label} URI is missing an absolute path: {uri}")));
	};
	let decoded = percent_decode(&path, label)?;
	if !decoded.starts_with('/') {
		return Err(crate::Error::new(format!("{label} URI is missing an absolute path: {uri}")));
	}
	Ok(decoded)
}

fn percent_decode(value: &str, label: &str) -> crate::Result<String> {
	fn hex_digit(byte: u8) -> Option<u8> {
		match byte {
			b'0'..=b'9' => Some(byte - b'0'),
			b'a'..=b'f' => Some(byte - b'a' + 10),
			b'A'..=b'F' => Some(byte - b'A' + 10),
			_ => None,
		}
	}

	let bytes = value.as_bytes();
	let mut decoded = Vec::with_capacity(bytes.len());
	let mut index = 0;
	while index < bytes.len() {
		let byte = bytes[index];
		if byte == 0 {
			return Err(crate::Error::new(format!("{label} contains a NUL byte: {value}")));
		}
		if byte == b'%' {
			let high = bytes.get(index + 1).and_then(|byte| hex_digit(*byte));
			let low = bytes.get(index + 2).and_then(|byte| hex_digit(*byte));
			let Some(high) = high else {
				return Err(crate::Error::new(format!(
					"{label} contains an invalid percent escape: {value}"
				)));
			};
			let Some(low) = low else {
				return Err(crate::Error::new(format!(
					"{label} contains an invalid percent escape: {value}"
				)));
			};
			let decoded_byte = high * 16 + low;
			if decoded_byte == 0 {
				return Err(crate::Error::new(format!("{label} contains a NUL byte: {value}")));
			}
			if matches!(decoded_byte, b'/' | b'\\') {
				return Err(crate::Error::new(format!(
					"{label} must not contain an encoded path separator: {value}"
				)));
			}
			decoded.push(decoded_byte);
			index += 3;
			continue;
		}
		decoded.push(byte);
		index += 1;
	}
	String::from_utf8(decoded)
		.map_err(|_| crate::Error::new(format!("{label} is not valid UTF-8 after percent decoding")))
}

fn reject_parent_segments(value: &str, label: &str) -> crate::Result<()> {
	if value.split('/').any(|segment| segment == "..") {
		return Err(crate::Error::new(format!(
			"{label} must not traverse outside the repository: {value}"
		)));
	}
	Ok(())
}

fn uri_path_is_directory(value: &str) -> bool {
	value.is_empty() || value.ends_with('/') || matches!(value.rsplit('/').next(), Some("." | ".."))
}

fn lexical_normalize(path: &Path) -> PathBuf {
	let mut normalized = PathBuf::new();
	for component in path.components() {
		match component {
			Component::CurDir => {},
			Component::ParentDir => {
				normalized.pop();
			},
			Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
			Component::Normal(name) => normalized.push(name),
		}
	}
	normalized
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
				if parent == candidate {
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
				"omp-security-sarif-{label}-{}-{}",
				std::process::id(),
				NEXT.fetch_add(1, Ordering::Relaxed)
			));
		fs::create_dir_all(&path).expect("create temp root");
		path
	}

	fn repository(label: &str) -> PathBuf {
		let repository = temp_root(label).join("repo");
		fs::create_dir_all(&repository).expect("create repository");
		repository
	}

	fn touch(repository: &Path, relative: &str) {
		let path = repository.join(relative);
		fs::create_dir_all(path.parent().expect("parent")).expect("create parent");
		fs::write(path, "fn main() {}\n").expect("write fixture file");
	}

	fn import_options(repository: &Path) -> SarifImportOptions {
		SarifImportOptions {
			repository_root: repository.to_path_buf(),
			source_path: None,
			created_at: Some("2026-07-29T00:00:00.000Z".to_string()),
			scan_id: Some("secscan_fixture".to_string()),
		}
	}

	fn fixture() -> Value {
		json!({
			"$schema": "https://json.schemastore.org/sarif-2.1.0.json",
			"version": "2.1.0",
			"runs": [{
				"tool": {
					"driver": {
						"name": "FixtureScanner",
						"version": "1.2.3",
						"rules": [
							{
								"id": "sql-injection",
								"name": "SQL injection",
								"shortDescription": {"text": "Unsanitized SQL query"},
								"properties": {"tags": ["security", "CWE-89"]}
							},
							{
								"id": "ssrf",
								"name": "Server-side request forgery",
								"shortDescription": {"text": "Unvalidated outbound URL"},
								"properties": {"tags": ["security", "CWE-918"]}
							}
						]
					}
				},
				"results": [
					{
						"ruleId": "sql-injection",
						"level": "error",
						"message": {"text": "User input is concatenated into a SQL query."},
						"locations": [{
							"physicalLocation": {
								"artifactLocation": {"uri": "src/db.ts"},
								"region": {"startLine": 18, "endLine": 18, "startColumn": 12}
							}
						}],
						"partialFingerprints": {"primaryLocationLineHash": "fixture-sql-18"},
						"properties": {"security-severity": "8.5", "category": "injection"}
					},
					{
						"ruleId": "ssrf",
						"level": "warning",
						"message": {"text": "An untrusted URL reaches fetch without host validation."},
						"locations": [{
							"physicalLocation": {
								"artifactLocation": {"uri": "src/fetcher.ts"},
								"region": {"startLine": 29, "endLine": 31}
							}
						}],
						"partialFingerprints": {"primaryLocationLineHash": "fixture-ssrf-29"},
						"properties": {"security-severity": "6.5", "category": "ssrf"}
					}
				]
			}]
		})
	}

	fn one_result(uri: &str) -> Value {
		json!({
			"version": "2.1.0",
			"runs": [{
				"tool": {"driver": {"name": "Fixture scanner"}},
				"results": [{
					"ruleId": "fixture.rule",
					"message": {"text": "result"},
					"locations": [{
						"physicalLocation": {
							"artifactLocation": {"uri": uri},
							"region": {"startLine": 1}
						}
					}]
				}]
			}]
		})
	}

	fn one_result_with_base(uri: &str, base: &str) -> Value {
		json!({
			"version": "2.1.0",
			"runs": [{
				"tool": {"driver": {"name": "Fixture scanner"}},
				"results": [{
					"ruleId": "fixture.rule",
					"message": {"text": "result"},
					"locations": [{
						"physicalLocation": {
							"artifactLocation": {"uri": uri, "uriBaseId": base},
							"region": {"startLine": 1}
						}
					}]
				}]
			}]
		})
	}

	fn mapped_result(
		rule_id: Option<&str>,
		line: u64,
		level: Option<&str>,
		properties: Value,
	) -> Value {
		let mut result = json!({
			"message": {"text": format!("result at line {line}")},
			"locations": [{
				"physicalLocation": {
					"artifactLocation": {"uri": "src/main.ts"},
					"region": {"startLine": line}
				}
			}]
		});
		if let Some(rule_id) = rule_id {
			result["ruleId"] = json!(rule_id);
		}
		if let Some(level) = level {
			result["level"] = json!(level);
		}
		if !properties.is_null() {
			result["properties"] = properties;
		}
		result
	}

	fn mapped_sarif(rules: Vec<Value>, results: Vec<Value>) -> Value {
		json!({
			"version": "2.1.0",
			"runs": [{
				"tool": {"driver": {"name": "Mapping scanner", "rules": rules}},
				"results": results
			}]
		})
	}

	#[test]
	fn imports_results_rules_locations_and_severity() {
		let root = temp_root("fixture");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/db.ts");
		touch(&repository, "src/fetcher.ts");
		let input = fixture();
		let bundle = import_sarif(&input, &import_options(&repository)).expect("import fixture");
		assert_eq!(bundle.scan.id, "secscan_fixture");
		assert_eq!(bundle.scan.status, "completed");
		assert_eq!(bundle.scan.completed_at.as_deref(), Some("2026-07-29T00:00:00.000Z"));
		assert_eq!(bundle.scan.target.kind, "imported");
		assert_eq!(bundle.scan.target.repository_root, repository.to_string_lossy().into_owned());
		assert_eq!(bundle.scan.producer.kind, "sarif-import");
		assert_eq!(bundle.scan.producer.name, "FixtureScanner");
		assert_eq!(bundle.scan.producer.version.as_deref(), Some("1.2.3"));
		assert_eq!(bundle.findings.len(), 2);
		assert_eq!(
			bundle.scan.finding_ids,
			bundle
				.findings
				.iter()
				.map(|finding| finding.id.clone())
				.collect::<Vec<_>>()
		);
		assert_eq!(bundle.sarif.as_ref(), Some(&input));

		let finding = &bundle.findings[0];
		assert_eq!(finding.rule_id, "sql-injection");
		assert_eq!(finding.title, "Unsanitized SQL query");
		assert_eq!(finding.summary, "User input is concatenated into a SQL query.");
		assert_eq!(finding.severity.level, "high");
		assert_eq!(finding.severity.score, Some(8.5));
		assert_eq!(finding.taxonomy.category, "injection");
		assert_eq!(finding.taxonomy.cwe, vec!["CWE-89".to_string()]);
		assert_eq!(finding.taxonomy.tags, Some(vec!["security".to_string(), "CWE-89".to_string()]));
		assert_eq!(finding.anchor.as_deref(), Some("fixture-sql-18"));
		assert!(finding.fingerprint.starts_with("omp-security/v1:sha256:"));
		assert_eq!(finding.validation.status, "unvalidated");
		assert_eq!(finding.disposition.status, "open");
		assert_eq!(
			finding
				.provenance
				.vendor_fingerprints
				.as_ref()
				.and_then(|map| map.get("primaryLocationLineHash")),
			Some(&Value::String("fixture-sql-18".to_string()))
		);
		let location = &finding.occurrences[0].locations[0];
		assert_eq!(location.path, "src/db.ts");
		assert_eq!(location.start_line, 18);
		assert_eq!(location.end_line, Some(18));
		assert_eq!(location.start_column, Some(12));
		assert_eq!(location.role.as_deref(), Some("primary"));

		let second = &bundle.findings[1];
		assert_eq!(second.severity.level, "medium");
		assert_eq!(second.severity.score, Some(6.5));
		assert_eq!(second.occurrences[0].locations[0].path, "src/fetcher.ts");
		assert_eq!(second.occurrences[0].locations[0].end_line, Some(31));
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn maps_security_severity_scores_and_levels() {
		let root = temp_root("severity");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/main.ts");
		let options = import_options(&repository);
		let cases = [
			(json!({"security-severity": 9.5}), None, "critical", Some(9.5)),
			(json!({"security-severity": "9"}), None, "critical", Some(9.0)),
			(json!({"security-severity": 7.0}), Some("error"), "high", Some(7.0)),
			(json!({"security-severity": 4.0}), Some("note"), "medium", Some(4.0)),
			(json!({"security-severity": 0.1}), Some("note"), "low", Some(0.1)),
			(json!({"security-severity": 0.0}), Some("error"), "high", Some(0.0)),
			(json!({"security-severity": "not-a-number"}), Some("warning"), "medium", None),
			(json!({}), Some("note"), "low", None),
			(json!({}), Some("none"), "informational", None),
			(json!({}), None, "informational", None),
			(json!({"security-severity": "8.75"}), None, "high", Some(8.75)),
		];
		for (properties, level, expected_level, expected_score) in cases {
			let input = mapped_sarif(
				Vec::new(),
				vec![mapped_result(Some("severity.rule"), 1, level, properties)],
			);
			let bundle = import_sarif(&input, &options).expect("import severity case");
			assert_eq!(bundle.findings.len(), 1);
			let severity = &bundle.findings[0].severity;
			assert_eq!(severity.level, expected_level, "level for {expected_score:?}");
			assert_eq!(severity.score, expected_score, "score for {expected_level}");
			assert!(severity.scoring_system.is_none());
			assert!(severity.vector.is_none());
			assert!(severity.rationale.is_none());
		}
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn maps_rule_metadata_and_fallback_categories() {
		let root = temp_root("rules");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/main.ts");
		let options = import_options(&repository);
		let rules = vec![
			json!({
				"id": "sql-injection",
				"name": "SQL injection rule",
				"shortDescription": {"text": "SQL injection"},
				"properties": {"tags": ["security", "cwe-89", "CWE-22", "not-a-cwe", "cwe-abc"]}
			}),
			json!({"id": "fallback.rule", "name": "Fallback name", "properties": {"tags": []}}),
		];
		let results = vec![
			mapped_result(Some("sql-injection"), 10, None, json!({})),
			mapped_result(Some("fallback.rule"), 11, None, json!({})),
			mapped_result(Some("unknown.rule"), 12, None, json!({})),
			mapped_result(None, 13, None, json!({})),
		];
		let bundle = import_sarif(&mapped_sarif(rules, results), &options).expect("import rules");
		assert_eq!(bundle.findings.len(), 4);

		let first = &bundle.findings[0];
		assert_eq!(first.rule_id, "sql-injection");
		assert_eq!(first.title, "SQL injection");
		assert_eq!(first.summary, "result at line 10");
		assert_eq!(first.taxonomy.category, "sql");
		assert_eq!(first.taxonomy.cwe, vec!["CWE-89".to_string(), "CWE-22".to_string()]);
		assert_eq!(
			first.taxonomy.tags,
			Some(vec![
				"security".to_string(),
				"cwe-89".to_string(),
				"CWE-22".to_string(),
				"not-a-cwe".to_string(),
				"cwe-abc".to_string(),
			])
		);

		let second = &bundle.findings[1];
		assert_eq!(second.rule_id, "fallback.rule");
		assert_eq!(second.title, "Fallback name");
		assert_eq!(second.taxonomy.category, "fallback");
		assert!(second.taxonomy.cwe.is_empty());

		let third = &bundle.findings[2];
		assert_eq!(third.rule_id, "unknown.rule");
		assert_eq!(third.title, "unknown.rule");
		assert_eq!(third.taxonomy.category, "unknown");
		assert!(third.taxonomy.cwe.is_empty());

		let fourth = &bundle.findings[3];
		assert_eq!(fourth.rule_id, "sarif.unknown");
		assert_eq!(fourth.title, "sarif.unknown");
		assert_eq!(fourth.summary, "result at line 13");
		assert_eq!(fourth.taxonomy.category, "sarif");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn resolves_rule_index_and_extension_rule_metadata() {
		let root = temp_root("rule-index");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/main.ts");
		let mut indexed = mapped_result(None, 20, None, json!({}));
		indexed["ruleIndex"] = json!(0);
		let input = json!({
			"version": "2.1.0",
			"runs": [{
				"tool": {
					"driver": {
						"name": "Index scanner",
						"rules": [{
							"id": "driver.rule",
							"shortDescription": {"text": "Driver title"}
						}]
					},
					"extensions": [{
						"name": "plugin",
						"rules": [{
							"id": "extension.rule",
							"name": "Extension title",
							"properties": {"tags": ["cwe-79"]}
						}]
					}]
				},
				"results": [
					indexed,
					mapped_result(Some("extension.rule"), 21, None, json!({}))
				]
			}]
		});
		let bundle =
			import_sarif(&input, &import_options(&repository)).expect("import indexed rules");
		assert_eq!(bundle.findings.len(), 2);
		assert_eq!(bundle.findings[0].rule_id, "driver.rule");
		assert_eq!(bundle.findings[0].title, "Driver title");
		assert_eq!(bundle.findings[0].taxonomy.category, "driver");
		assert_eq!(bundle.findings[1].rule_id, "extension.rule");
		assert_eq!(bundle.findings[1].title, "Extension title");
		assert_eq!(bundle.findings[1].taxonomy.cwe, vec!["CWE-79".to_string()]);
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn keeps_file_level_locations_without_a_region_start_line() {
		let root = temp_root("file-level");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/main.ts");
		let input = json!({
			"version": "2.1.0",
			"runs": [{
				"tool": {"driver": {"name": "File scanner"}},
				"results": [{
					"ruleId": "file.rule",
					"message": {"text": "file-level finding"},
					"locations": [{
						"physicalLocation": {
							"artifactLocation": {"uri": "src/main.ts"}
						}
					}]
				}]
			}]
		});
		let options = import_options(&repository);
		let bundle = import_sarif(&input, &options).expect("file-level location");
		let location = &bundle.findings[0].occurrences[0].locations[0];
		assert_eq!(location.path, "src/main.ts");
		assert_eq!(location.start_line, 1);
		assert_eq!(location.end_line, None);
		assert_eq!(location.role.as_deref(), Some("primary"));

		let mut zero_start = input.clone();
		zero_start["runs"][0]["results"][0]["locations"][0]["physicalLocation"]["region"] =
			json!({"startLine": 0});
		let error = import_sarif(&zero_start, &options).expect_err("zero start line");
		assert!(error.to_string().contains("startLine"), "{error}");

		let mut zero_end = input.clone();
		zero_end["runs"][0]["results"][0]["locations"][0]["physicalLocation"]["region"] =
			json!({"startLine": 1, "endLine": 0});
		let error = import_sarif(&zero_end, &options).expect_err("zero end line");
		assert!(error.to_string().contains("endLine"), "{error}");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn resolves_rule_references_and_message_strings() {
		let root = temp_root("rule-reference");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/main.ts");
		let input = json!({
			"version": "2.1.0",
			"runs": [{
				"tool": {
					"driver": {
						"name": "Reference scanner",
						"globalMessageStrings": {
							"global.message": {"text": "Global {0}"}
						},
						"rules": [{
							"id": "driver.rule",
							"shortDescription": {"text": "Driver title"}
						}]
					},
					"extensions": [{
						"name": "plugin",
						"guid": "plugin-guid",
						"rules": [{
							"id": "extension.rule",
							"shortDescription": {"text": "Extension title"},
							"messageStrings": {
								"extension.message": {"text": "Extension {0} at {1}"}
							},
							"properties": {"tags": ["cwe-79"]}
						}]
					}]
				},
				"results": [
					{
						"rule": {
							"index": 0,
							"toolComponent": {"index": 0, "name": "plugin", "guid": "plugin-guid"}
						},
						"message": {
							"id": "extension.message",
							"arguments": ["value", "line 4"]
						},
						"locations": [{
							"physicalLocation": {
								"artifactLocation": {"uri": "src/main.ts"},
								"region": {"startLine": 4}
							}
						}]
					},
					{
						"rule": {"id": "driver.rule"},
						"message": {"id": "global.message", "arguments": ["value"]},
						"locations": [{
							"physicalLocation": {
								"artifactLocation": {"uri": "src/main.ts"},
								"region": {"startLine": 5}
							}
						}]
					}
				]
			}]
		});
		let bundle =
			import_sarif(&input, &import_options(&repository)).expect("rule reference import");
		assert_eq!(bundle.findings.len(), 2);
		let extension = &bundle.findings[0];
		assert_eq!(extension.rule_id, "extension.rule");
		assert_eq!(extension.title, "Extension title");
		assert_eq!(extension.summary, "Extension value at line 4");
		assert_eq!(extension.taxonomy.cwe, vec!["CWE-79".to_string()]);
		let driver = &bundle.findings[1];
		assert_eq!(driver.rule_id, "driver.rule");
		assert_eq!(driver.title, "Driver title");
		assert_eq!(driver.summary, "Global value");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn interpolates_inline_messages_in_one_pass() {
		let root = temp_root("inline-message");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/main.ts");
		let input = json!({
			"version": "2.1.0",
			"runs": [{
				"tool": {"driver": {"name": "Message scanner"}},
				"results": [
					{
						"ruleId": "inline.rule",
						"message": {"text": "{0} {1}", "arguments": ["{1}", "X"]},
						"locations": [{
							"physicalLocation": {
								"artifactLocation": {"uri": "src/main.ts"},
								"region": {"startLine": 1}
							}
						}]
					},
					{
						"ruleId": "inline.rule",
						"message": {"markdown": "Use {{0}} and {0}", "arguments": ["X"]},
						"locations": [{
							"physicalLocation": {
								"artifactLocation": {"uri": "src/main.ts"},
								"region": {"startLine": 2}
							}
						}]
					}
				]
			}]
		});
		let bundle =
			import_sarif(&input, &import_options(&repository)).expect("inline message import");
		assert_eq!(bundle.findings.len(), 2);
		assert_eq!(bundle.findings[0].summary, "{1} X");
		assert_eq!(bundle.findings[1].summary, "Use {0} and X");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn resolves_duplicate_rule_ids_per_component() {
		let root = temp_root("duplicate-rules");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/main.ts");
		let input = json!({
			"version": "2.1.0",
			"runs": [{
				"tool": {
					"driver": {
						"name": "Duplicate scanner",
						"rules": [{
							"id": "duplicate.rule",
							"shortDescription": {"text": "Driver title"}
						}]
					},
					"extensions": [{
						"name": "plugin",
						"rules": [{
							"id": "duplicate.rule",
							"shortDescription": {"text": "Extension title"}
						}]
					}]
				},
				"results": [
					{
						"ruleId": "duplicate.rule",
						"message": {"text": "driver result"},
						"locations": [{
							"physicalLocation": {
								"artifactLocation": {"uri": "src/main.ts"},
								"region": {"startLine": 1}
							}
						}]
					},
					{
						"ruleId": "duplicate.rule",
						"rule": {
							"id": "duplicate.rule",
							"toolComponent": {"index": 0}
						},
						"message": {"text": "extension result"},
						"locations": [{
							"physicalLocation": {
								"artifactLocation": {"uri": "src/main.ts"},
								"region": {"startLine": 2}
							}
						}]
					}
				]
			}]
		});
		let bundle =
			import_sarif(&input, &import_options(&repository)).expect("duplicate rule import");
		assert_eq!(bundle.findings.len(), 2);
		assert_eq!(bundle.findings[0].rule_id, "duplicate.rule");
		assert_eq!(bundle.findings[0].title, "Driver title");
		assert_eq!(bundle.findings[1].rule_id, "duplicate.rule");
		assert_eq!(bundle.findings[1].title, "Extension title");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn resolves_global_message_strings_from_owning_component() {
		let root = temp_root("global-messages");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/main.ts");
		let input = json!({
			"version": "2.1.0",
			"runs": [{
				"tool": {
					"driver": {
						"name": "Global scanner",
						"globalMessageStrings": {
							"shared.message": {"text": "Driver {0}"}
						},
						"rules": [{
							"id": "driver.rule",
							"shortDescription": {"text": "Driver title"}
						}]
					},
					"extensions": [{
						"name": "plugin",
						"globalMessageStrings": {
							"shared.message": {"text": "Extension {0}"}
						},
						"rules": [{
							"id": "extension.rule",
							"shortDescription": {"text": "Extension title"}
						}]
					}]
				},
				"results": [
					{
						"rule": {
							"id": "extension.rule",
							"toolComponent": {"index": 0}
						},
						"message": {"id": "shared.message", "arguments": ["value"]},
						"locations": [{
							"physicalLocation": {
								"artifactLocation": {"uri": "src/main.ts"},
								"region": {"startLine": 1}
							}
						}]
					},
					{
						"ruleId": "driver.rule",
						"message": {"id": "shared.message", "arguments": ["value"]},
						"locations": [{
							"physicalLocation": {
								"artifactLocation": {"uri": "src/main.ts"},
								"region": {"startLine": 2}
							}
						}]
					}
				]
			}]
		});
		let bundle =
			import_sarif(&input, &import_options(&repository)).expect("global message import");
		assert_eq!(bundle.findings.len(), 2);
		assert_eq!(bundle.findings[0].rule_id, "extension.rule");
		assert_eq!(bundle.findings[0].summary, "Extension value");
		assert_eq!(bundle.findings[1].rule_id, "driver.rule");
		assert_eq!(bundle.findings[1].summary, "Driver value");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn locationless_results_keep_distinct_anchors_and_deduplicate_repeats() {
		let root = temp_root("locationless");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		let input = json!({
			"version": "2.1.0",
			"runs": [{
				"tool": {"driver": {"name": "Fixture scanner"}},
				"results": [
					{"ruleId": "fixture.rule", "message": {"text": "first result"}},
					{"ruleId": "fixture.rule", "message": {"text": "second result"}},
					{"ruleId": "fixture.rule", "message": {"text": "second result"}}
				]
			}]
		});
		let bundle = import_sarif(&input, &import_options(&repository)).expect("import");
		assert_eq!(
			bundle
				.findings
				.iter()
				.map(|finding| finding.summary.as_str())
				.collect::<Vec<_>>(),
			vec!["first result", "second result"]
		);
		assert_ne!(bundle.findings[0].id, bundle.findings[1].id);
		for finding in &bundle.findings {
			let location = &finding.occurrences[0].locations[0];
			assert_eq!(location.path, "unknown");
			assert_eq!(location.start_line, 1);
			assert_eq!(location.role.as_deref(), Some("unknown"));
		}
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn rejects_non_file_absolute_and_traversing_uris() {
		let root = temp_root("rejections");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/db.ts");
		fs::write(root.join("outside.ts"), "secret\n").expect("outside target");
		for uri in [
			"../outside.ts",
			"src/../../outside.ts",
			"https://example.com/src/db.ts",
			"/etc/passwd",
			"C:/Windows/system.ini",
			"file:///etc/passwd",
			"src/%2e%2e/outside.ts",
			"src%2Fdb.ts",
			"src/%zz.ts",
		] {
			let error = import_sarif(&one_result(uri), &import_options(&repository))
				.expect_err("reject unsafe URI");
			let message = error.to_string();
			assert!(message.contains("SARIF"), "unexpected error for {uri}: {message}");
			if uri.contains("..") {
				assert!(message.contains("traverse"), "expected traversal error for {uri}: {message}");
			}
		}
		let outside_uri = format!("file://{}", root.join("outside.ts").display());
		let error = import_sarif(&one_result(&outside_uri), &import_options(&repository))
			.expect_err("existing file outside repository");
		assert!(error.to_string().contains("outside the repository"), "{error}");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn resolves_srcroot_relative_and_declared_file_bases() {
		let root = temp_root("bases");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/db.ts");
		let options = import_options(&repository);

		let bundle =
			import_sarif(&one_result_with_base("src/db.ts", "%SRCROOT%"), &options).expect("srcroot");
		assert_eq!(bundle.findings[0].occurrences[0].locations[0].path, "src/db.ts");

		let absolute_base = format!("file://{}/", repository.to_string_lossy());
		let mut input = one_result_with_base("src/db.ts", "ROOT");
		input["runs"][0]["originalUriBaseIds"] = json!({"ROOT": {"uri": absolute_base}});
		let bundle = import_sarif(&input, &options).expect("declared absolute base");
		assert_eq!(bundle.findings[0].occurrences[0].locations[0].path, "src/db.ts");

		let mut input = one_result_with_base("db.ts", "ROOT");
		input["runs"][0]["originalUriBaseIds"] = json!({"ROOT": {"uri": "src/"}});
		let bundle = import_sarif(&input, &options).expect("declared relative base");
		assert_eq!(bundle.findings[0].occurrences[0].locations[0].path, "src/db.ts");

		let mut input = one_result_with_base("src/db.ts", "ROOT");
		input["runs"][0]["originalUriBaseIds"] = json!({"ROOT": {"uri": "../"}});
		assert!(import_sarif(&input, &options).is_err());

		let error = import_sarif(&one_result_with_base("src/db.ts", "%UNKNOWN%"), &options)
			.expect_err("unknown base");
		assert!(error.to_string().contains("unknown URI base"));
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn decodes_percent_escapes_and_accepts_backslashes() {
		let root = temp_root("encoding");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/a b.ts");
		touch(&repository, "src/db.ts");
		let options = import_options(&repository);
		let bundle = import_sarif(&one_result("src/a%20b.ts"), &options).expect("encoded space");
		assert_eq!(bundle.findings[0].occurrences[0].locations[0].path, "src/a b.ts");
		let bundle = import_sarif(&one_result("src\\db.ts"), &options).expect("backslash");
		assert_eq!(bundle.findings[0].occurrences[0].locations[0].path, "src/db.ts");
		let _ = fs::remove_dir_all(root);
	}

	#[cfg(unix)]
	#[test]
	fn rejects_symbolic_links_that_resolve_outside_the_repository() {
		use std::os::unix::fs::symlink;

		let root = temp_root("symlink");
		let repository = root.join("repo");
		let outside = root.join("outside");
		fs::create_dir_all(&repository).expect("repository");
		fs::create_dir_all(&outside).expect("outside");
		touch(&outside, "secret.ts");
		touch(&repository, "real.ts");
		symlink(&outside, repository.join("link")).expect("directory symlink");
		symlink(outside.join("secret.ts"), repository.join("alias.ts")).expect("file symlink");
		symlink(repository.join("real.ts"), repository.join("inside.ts")).expect("inside symlink");
		symlink(outside.join("missing.ts"), repository.join("dangling.ts"))
			.expect("dangling symlink");
		symlink(outside.join("missing-dir"), repository.join("dangling-dir"))
			.expect("dangling directory symlink");
		let options = import_options(&repository);

		for uri in
			["link/secret.ts", "link/missing.ts", "alias.ts", "dangling.ts", "dangling-dir/file.ts"]
		{
			let error = import_sarif(&one_result(uri), &options).expect_err("escaping symlink");
			assert!(
				error.to_string().contains("symbolic link"),
				"unexpected error for {uri}: {error}"
			);
		}
		let bundle = import_sarif(&one_result("inside.ts"), &options).expect("inside symlink");
		assert_eq!(bundle.findings[0].occurrences[0].locations[0].path, "real.ts");
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn import_file_records_the_resolved_source_path() {
		let root = temp_root("file");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		touch(&repository, "src/db.ts");
		touch(&repository, "src/fetcher.ts");
		let file = root.join("results.sarif");
		fs::write(&file, serde_json::to_vec(&fixture()).expect("encode fixture")).expect("write");
		let bundle = import_sarif_file(&file, &import_options(&repository)).expect("import file");
		let expected = std::path::absolute(&file).expect("absolute source");
		for metadata in
			[bundle.scan.provenance.metadata.as_ref(), bundle.findings[0].provenance.metadata.as_ref()]
		{
			let source = metadata
				.and_then(|metadata| metadata.get("sourcePath"))
				.and_then(Value::as_str)
				.expect("source path metadata");
			assert_eq!(Path::new(source), expected.as_path());
		}
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn rejects_other_sarif_versions() {
		let root = temp_root("version");
		let repository = root.join("repo");
		fs::create_dir_all(&repository).expect("repository");
		let error =
			import_sarif(&json!({"version": "2.0.0", "runs": []}), &import_options(&repository))
				.expect_err("old version");
		assert!(error.to_string().contains("Expected SARIF 2.1.0 input"));
		let _ = fs::remove_dir_all(root);
	}
}
