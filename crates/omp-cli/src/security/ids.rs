//! Stable security identifiers and fingerprints.
//!
//! Anchors: `packages/coding-agent/src/security/contracts/ids.ts`.
//! Canonical JSON sorts object keys and drops `undefined` equivalents (`None`)
//! so the same logical finding fingerprints identically across runs/languages.

use sha2::{Digest, Sha256};

use super::types::SecurityLocation;

/// Recursively sort object keys, dropping `null` object members. Arrays keep
/// their order (the contract sorts locations before hashing instead).
#[must_use]
pub fn canonicalize(value: &serde_json::Value) -> serde_json::Value {
	match value {
		serde_json::Value::Array(items) => {
			serde_json::Value::Array(items.iter().map(canonicalize).collect())
		},
		serde_json::Value::Object(map) => {
			let mut keys: Vec<&String> = map.keys().collect();
			keys.sort();
			let mut out = serde_json::Map::new();
			for key in keys {
				let item = &map[key];
				if item.is_null() {
					continue;
				}
				out.insert(key.clone(), canonicalize(item));
			}
			serde_json::Value::Object(out)
		},
		other => other.clone(),
	}
}

#[must_use]
pub fn canonical_security_json(value: &serde_json::Value) -> String {
	serde_json::to_string(&canonicalize(value)).unwrap_or_default()
}

#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
	let digest = Sha256::digest(bytes);
	let mut out = String::with_capacity(64);
	for byte in digest {
		out.push_str(&format!("{byte:02x}"));
	}
	out
}

fn sha256_canonical(value: &serde_json::Value) -> String {
	sha256_hex(canonical_security_json(value).as_bytes())
}

fn short_digest(material: &str) -> String {
	sha256_hex(material.as_bytes()).chars().take(24).collect()
}

fn normalize_fingerprint_path(value: &str) -> String {
	let slashed = value.replace('\\', "/");
	slashed.strip_prefix("./").unwrap_or(&slashed).to_string()
}

#[must_use]
pub fn create_security_finding_fingerprint(
	rule_id: &str,
	category: &str,
	anchor: Option<&str>,
	locations: &[SecurityLocation],
) -> String {
	let normalized = locations
		.iter()
		.map(|location| {
			serde_json::json!({
				"path": normalize_fingerprint_path(&location.path),
				"startLine": location.start_line,
				"endLine": location.end_line,
				"startColumn": location.start_column,
				"endColumn": location.end_column,
				"role": location.role,
			})
		})
		.collect::<Vec<_>>();
	let mut normalized = normalized;
	normalized.sort_by_cached_key(|location| canonical_security_json(location));
	let material = serde_json::json!({
		"ruleId": rule_id.trim().to_lowercase(),
		"category": category.trim().to_lowercase(),
		"anchor": anchor.map(str::trim).filter(|value| !value.is_empty()).map(str::to_lowercase),
		"locations": normalized,
	});
	format!("omp-security/v1:sha256:{}", sha256_canonical(&material))
}

#[must_use]
pub fn create_security_finding_id(fingerprint: &str) -> String {
	format!("secf_{}", short_digest(fingerprint))
}

#[must_use]
pub fn create_security_occurrence_id(fingerprint: &str, locations: &[SecurityLocation]) -> String {
	let normalized = locations
		.iter()
		.map(|location| {
			serde_json::json!({
				"path": normalize_fingerprint_path(&location.path),
				"startLine": location.start_line,
				"endLine": location.end_line,
				"startColumn": location.start_column,
				"endColumn": location.end_column,
				"role": location.role,
			})
		})
		.collect::<Vec<_>>();
	let material = serde_json::json!({ "fingerprint": fingerprint, "locations": normalized });
	format!("seco_{}", short_digest(&canonical_security_json(&material)))
}

#[must_use]
pub fn create_security_evidence_id(fingerprint: &str, label: &str, ordinal: usize) -> String {
	let material = serde_json::json!({ "fingerprint": fingerprint, "label": label, "ordinal": ordinal });
	format!("sece_{}", short_digest(&canonical_security_json(&material)))
}

/// `secscan_<uuid without dashes>`; a v4 UUID keeps the shape stable enough
/// for the store's id validator while needing no extra dependency.
#[must_use]
pub fn create_security_scan_id(uuid: &str) -> String {
	format!("secscan_{}", uuid.replace('-', ""))
}

#[must_use]
pub fn create_security_plan_id(fingerprint: &str) -> String {
	format!("secplan_{}", short_digest(fingerprint))
}

/// `secop_<uuid without dashes>`.
#[must_use]
pub fn create_security_operation_id(uuid: &str) -> String {
	format!("secop_{}", uuid.replace('-', ""))
}

/// Readable trailing slug plus 12 hex characters of the canonical root hash.
///
/// Mirrors TS `encodeSecurityProjectKey`: backslashes become `/`, exactly one
/// trailing `/` is dropped, a single leading `/` is stripped, each run of
/// characters outside `[A-Za-z0-9._-]` collapses to one `-`, surrounding `-`
/// are trimmed and only the last 80 characters of the slug survive. `project`
/// stands in for an empty slug, and the hash covers the normalized path.
#[must_use]
pub fn encode_security_project_key(repository_root: &str) -> String {
	let slashed = repository_root.replace('\\', "/");
	let normalized = slashed.strip_suffix('/').unwrap_or(&slashed);
	let slug = normalized.strip_prefix('/').unwrap_or(normalized);
	let mut collapsed = String::with_capacity(slug.len());
	let mut in_run = false;
	for ch in slug.chars() {
		if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
			collapsed.push(ch);
			in_run = false;
		} else if !in_run {
			collapsed.push('-');
			in_run = true;
		}
	}
	let readable = collapsed.trim_matches('-');
	let readable: String = if readable.chars().count() > 80 {
		readable.chars().rev().take(80).collect::<String>().chars().rev().collect()
	} else {
		readable.to_string()
	};
	let readable = if readable.is_empty() { "project".to_string() } else { readable };
	let digest = sha256_hex(normalized.as_bytes());
	format!("{readable}-{}", &digest[..12])
}

#[cfg(test)]
mod tests {
	use super::*;

	fn location(path: &str, line: u32) -> SecurityLocation {
		SecurityLocation {
			path: path.to_string(),
			start_line: line,
			end_line: None,
			start_column: None,
			end_column: None,
			role: None,
		}
	}

	#[test]
	fn fingerprint_is_canonical_across_path_spelling() {
		let left = create_security_finding_fingerprint(
			" Rule ",
			"CAT",
			None,
			&[location("./src/a.rs", 3)],
		);
		let right =
			create_security_finding_fingerprint("rule", "cat", Some(""), &[location("src\\a.rs", 3)]);
		assert_eq!(left, right);
		assert!(left.starts_with("omp-security/v1:sha256:"));
	}

	#[test]
	fn project_key_keeps_a_bounded_readable_suffix() {
		// TS `encodeSecurityProjectKey`: separators collapse to `-`, one trailing
		// `/` is dropped, and the suffix is 12 hex characters of that path's hash.
		let key = encode_security_project_key("/data/data/home/work/myrepo/");
		let digest = &sha256_hex(b"/data/data/home/work/myrepo")[..12];
		assert_eq!(key, format!("data-data-home-work-myrepo-{digest}"));

		// One leading `/` is stripped and surrounding `-` are trimmed.
		assert_eq!(
			encode_security_project_key("//data//home//"),
			format!("data-home-{}", &sha256_hex(b"//data//home/")[..12])
		);

		// Only the last 80 characters of the slug survive.
		let deep = format!("/{}", "segment/".repeat(12));
		let key = encode_security_project_key(&deep);
		let slug = key.rsplit_once('-').expect("slug and digest").0;
		assert_eq!(slug.chars().count(), 80);
		assert!(slug.ends_with("segment-segment-segment"), "{slug}");

		// Root has no readable part.
		assert_eq!(
			encode_security_project_key("/"),
			format!("project-{}", &sha256_hex(b"")[..12])
		);
	}

	#[test]
	fn canonical_json_drops_null_members_and_sorts_keys() {
		let value = serde_json::json!({"b": 1, "a": null, "c": [{"z": 1, "y": null}]});
		assert_eq!(canonical_security_json(&value), r#"{"b":1,"c":[{"z":1}]}"#);
	}
}
