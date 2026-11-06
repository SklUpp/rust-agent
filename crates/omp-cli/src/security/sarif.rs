//! SARIF 2.1.0 export for security bundles.
//!
//! Anchors: `packages/coding-agent/src/security/sarif.ts`.

use serde_json::{Map, Value, json};

use super::types::{SecurityFinding, SecurityScanBundle};

fn sarif_level(finding: &SecurityFinding) -> &'static str {
	match finding.severity.level.as_str() {
		"critical" | "high" => "error",
		"medium" => "warning",
		"low" => "note",
		_ => "none",
	}
}

fn file_uri(path: &std::path::Path) -> String {
	let mut text = path.to_string_lossy().replace('\\', "/");
	if !text.starts_with('/') {
		text.insert(0, '/');
	}
	if !text.ends_with('/') {
		text.push('/');
	}
	format!("file://{text}")
}

/// Build the SARIF document for one scan bundle. Rules are de-duplicated by
/// rule id in first-seen order; every finding is a result.
#[must_use]
pub fn export_security_bundle_to_sarif(bundle: &SecurityScanBundle) -> Value {
	let mut rules: Vec<&SecurityFinding> = Vec::new();
	let mut seen = std::collections::BTreeSet::new();
	for finding in &bundle.findings {
		if seen.insert(finding.rule_id.as_str()) {
			rules.push(finding);
		}
	}
	let rules: Vec<Value> = rules
		.iter()
		.map(|finding| {
			let mut tags = finding.taxonomy.cwe.clone();
			if let Some(extra) = &finding.taxonomy.tags {
				tags.extend(extra.iter().cloned());
			}
			let mut properties = Map::new();
			properties.insert("tags".to_string(), json!(tags));
			if let Some(score) = finding.severity.score {
				properties.insert("security-severity".to_string(), json!(score));
			}
			json!({
				"id": finding.rule_id,
				"name": finding.rule_id,
				"shortDescription": {"text": finding.title},
				"fullDescription": {"text": finding.summary},
				"properties": properties,
			})
		})
		.collect();
	let results: Vec<Value> = bundle
		.findings
		.iter()
		.map(|finding| {
			let locations: Vec<Value> = finding
				.occurrences
				.iter()
				.flat_map(|occurrence| occurrence.locations.iter())
				.map(|location| {
					json!({
						"physicalLocation": {
							"artifactLocation": {"uri": location.path, "uriBaseId": "%SRCROOT%"},
							"region": {
								"startLine": location.start_line,
								"endLine": location.end_line,
								"startColumn": location.start_column,
								"endColumn": location.end_column,
							}
						}
					})
				})
				.collect();
			let mut properties = Map::new();
			properties.insert("findingId".to_string(), json!(finding.id));
			properties.insert("confidence".to_string(), json!(finding.confidence.level));
			properties.insert("validation".to_string(), json!(finding.validation.status));
			properties.insert("disposition".to_string(), json!(finding.disposition.status));
			properties.insert("category".to_string(), json!(finding.taxonomy.category));
			if let Some(score) = finding.severity.score {
				properties.insert("security-severity".to_string(), json!(score));
			}
			json!({
				"ruleId": finding.rule_id,
				"level": sarif_level(finding),
				"message": {"text": finding.summary},
				"locations": locations,
				"fingerprints": {"omp-security/v1": finding.fingerprint},
				"properties": properties,
			})
		})
		.collect();
	json!({
		"$schema": "https://json.schemastore.org/sarif-2.1.0.json",
		"version": "2.1.0",
		"runs": [{
			"tool": {
				"driver": {
					"name": bundle.scan.producer.name,
					"version": bundle.scan.producer.version,
					"informationUri": "https://omp.sh",
					"rules": rules,
				}
			},
			"results": results,
			"originalUriBaseIds": {
				"%SRCROOT%": {"uri": file_uri(std::path::Path::new(&bundle.scan.target.repository_root))}
			}
		}]
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn sarif_level_mapping_matches_severity() {
		for (level, expected) in [
			("critical", "error"),
			("high", "error"),
			("medium", "warning"),
			("low", "note"),
			("informational", "none"),
		] {
			let finding = minimal_finding(level);
			assert_eq!(sarif_level(&finding), expected);
		}
	}

	fn minimal_finding(level: &str) -> SecurityFinding {
		SecurityFinding {
			id: "secf_x".to_string(),
			scan_id: "secscan_x".to_string(),
			fingerprint: "fp".to_string(),
			rule_id: "rule".to_string(),
			anchor: None,
			title: "t".to_string(),
			summary: "s".to_string(),
			severity: crate::security::types::SecuritySeverity {
				level: level.to_string(),
				score: None,
				scoring_system: None,
				vector: None,
				rationale: None,
			},
			confidence: crate::security::types::SecurityConfidence {
				level: "high".to_string(),
				rationale: None,
			},
			taxonomy: crate::security::types::SecurityTaxonomy {
				category: "c".to_string(),
				cwe: Vec::new(),
				tags: None,
			},
			occurrences: Vec::new(),
			evidence: Vec::new(),
			remediation: None,
			validation: crate::security::types::SecurityValidation {
				status: "unvalidated".to_string(),
				summary: None,
				evidence_ids: Vec::new(),
				validated_at: None,
			},
			disposition: crate::security::types::SecurityDisposition {
				status: "open".to_string(),
				rationale: None,
				updated_at: None,
				actor: None,
			},
			provenance: crate::security::provenance::create_native_security_provenance(
				"now",
				&crate::security::types::SecurityAccountRef {
					provider: "p".to_string(),
					credential_id: 1,
					account_id: None,
					email: None,
					organization_id: None,
					organization_name: None,
				},
				"plan",
				"workflow",
				None,
				None,
			),
			extensions: None,
		}
	}
}
