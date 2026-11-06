//! Security provenance, public redaction, and workflow fingerprinting.
//!
//! Anchors: `packages/coding-agent/src/security/provenance.ts`.
//! Credential identity is represented only by a hash affinity in public
//! documents; tokens, emails, account ids, and org ids never reach a persisted
//! public bundle.

use serde_json::{Map, Value};

use super::{
	ids::{canonical_security_json, sha256_hex},
	types::{SecurityAccountRef, SecurityProducer, SecurityProvenance, SecurityScan, SecurityUpstreamProvenance},
};

/// `CODEX_SECURITY_UPSTREAM`: attribution for the ported workflow.
pub const CODEX_SECURITY_UPSTREAM_REPOSITORY: &str = "https://github.com/openai/codex-security";
pub const CODEX_SECURITY_UPSTREAM_REVISION: &str = "f22d4a36f26d16287bcdfd707b369116e02a08c3";
pub const CODEX_SECURITY_UPSTREAM_PACKAGE_VERSION: &str = "0.1.1";
pub const CODEX_SECURITY_UPSTREAM_PLUGIN_VERSION: &str = "0.1.14";
pub const CODEX_SECURITY_UPSTREAM_ARCHIVE_SHA256: &str =
	"13745c495b7c5cf5273cf2115df86b9c3ec3056f43151c869e004aa3f30bcffb";
pub const OMP_SECURITY_WORKFLOW_VERSION: &str = "1.0.0";

#[must_use]
pub fn create_native_security_producer() -> SecurityProducer {
	SecurityProducer {
		kind: "omp-native".to_string(),
		name: "OMP Native Security".to_string(),
		version: Some(OMP_SECURITY_WORKFLOW_VERSION.to_string()),
		vendor: None,
		revision: None,
		plugin_version: None,
	}
}

#[must_use]
pub fn upstream_provenance() -> SecurityUpstreamProvenance {
	SecurityUpstreamProvenance {
		repository: Some(CODEX_SECURITY_UPSTREAM_REPOSITORY.to_string()),
		revision: Some(CODEX_SECURITY_UPSTREAM_REVISION.to_string()),
		package_version: Some(CODEX_SECURITY_UPSTREAM_PACKAGE_VERSION.to_string()),
		plugin_version: Some(CODEX_SECURITY_UPSTREAM_PLUGIN_VERSION.to_string()),
		archive_sha256: Some(CODEX_SECURITY_UPSTREAM_ARCHIVE_SHA256.to_string()),
	}
}

#[must_use]
pub fn create_security_credential_affinity(account: &SecurityAccountRef) -> String {
	let value = serde_json::to_value(account).unwrap_or(Value::Null);
	format!(
		"omp-security-credential/v1:sha256:{}",
		sha256_hex(canonical_security_json(&value).as_bytes())
	)
}

pub fn create_native_security_provenance(
	created_at: &str,
	account: &SecurityAccountRef,
	plan_fingerprint: &str,
	workflow_fingerprint: &str,
	session_id: Option<&str>,
	operation_id: Option<&str>,
) -> SecurityProvenance {
	let mut metadata = Map::new();
	metadata.insert("planFingerprint".to_string(), Value::String(plan_fingerprint.to_string()));
	metadata.insert(
		"workflowFingerprint".to_string(),
		Value::String(workflow_fingerprint.to_string()),
	);
	metadata.insert(
		"credentialAffinity".to_string(),
		Value::String(create_security_credential_affinity(account)),
	);
	if let Some(session_id) = session_id {
		metadata.insert(
			"sessionAffinity".to_string(),
			Value::String(format!(
				"omp-security-session/v1:sha256:{}",
				sha256_hex(session_id.as_bytes())
			)),
		);
	}
	if let Some(operation_id) = operation_id {
		metadata.insert("operationId".to_string(), Value::String(operation_id.to_string()));
	}
	SecurityProvenance {
		producer: create_native_security_producer(),
		created_at: created_at.to_string(),
		imported_at: None,
		source_ids: None,
		vendor_fingerprints: None,
		upstream: Some(upstream_provenance()),
		metadata: Some(metadata),
	}
}

const PRIVATE_SECURITY_KEYS: [&str; 12] = [
	"account",
	"accountid",
	"accesstoken",
	"apikey",
	"credentialid",
	"email",
	"organizationid",
	"organizationname",
	"orgid",
	"orgname",
	"refreshtoken",
	"secret",
];

fn normalized_key(key: &str) -> String {
	key.chars()
		.filter(|ch| ch.is_ascii_alphanumeric())
		.map(|ch| ch.to_ascii_lowercase())
		.collect()
}

/// `redactPrivateSecurityMetadata`: recursive key redaction.
#[must_use]
pub fn redact_private_security_metadata(value: &Value) -> Value {
	match value {
		Value::Array(items) => Value::Array(items.iter().map(redact_private_security_metadata).collect()),
		Value::Object(map) => {
			let mut out = Map::new();
			for (key, item) in map {
				let key_norm = normalized_key(key);
				if PRIVATE_SECURITY_KEYS.contains(&key_norm.as_str()) || key_norm == "token" || key_norm == "sessionid" {
					continue;
				}
				out.insert(key.clone(), redact_private_security_metadata(item));
			}
			Value::Object(out)
		},
		other => other.clone(),
	}
}

/// `createPublicSecurityScan`: redact private metadata; when `include_plan`,
/// replace the plan account with a credential affinity hash.
pub fn create_public_security_scan(scan: &SecurityScan, include_plan: bool) -> crate::Result<Value> {
	let mut value = serde_json::to_value(scan)
		.map_err(|error| crate::Error::new(format!("cannot encode security scan: {error}")))?;
	if include_plan {
		if let Some(plan) = value.get_mut("plan").and_then(Value::as_object_mut) {
			let affinity = plan
				.get("account")
				.cloned()
				.and_then(|account| serde_json::from_value::<SecurityAccountRef>(account).ok())
				.map(|account| create_security_credential_affinity(&account));
			if let Some(affinity) = affinity {
				let provider = plan
					.get("account")
					.and_then(|account| account.get("provider"))
					.cloned()
					.unwrap_or(Value::String(String::new()));
				let mut public = Map::new();
				public.insert("provider".to_string(), provider);
				public.insert("credentialAffinity".to_string(), Value::String(affinity));
				plan.insert("account".to_string(), Value::Object(public));
			}
		}
	} else {
		value.as_object_mut().map(|object| object.remove("plan"));
	}
	Ok(redact_private_security_metadata(&value))
}

/// `createSecurityWorkflowFingerprint`: hash of workflow inputs in order.
#[must_use]
pub fn create_security_workflow_fingerprint(inputs: &[&str]) -> String {
	let value = serde_json::json!({
		"workflowVersion": OMP_SECURITY_WORKFLOW_VERSION,
		"upstream": {
			"repository": CODEX_SECURITY_UPSTREAM_REPOSITORY,
			"revision": CODEX_SECURITY_UPSTREAM_REVISION,
			"packageVersion": CODEX_SECURITY_UPSTREAM_PACKAGE_VERSION,
			"pluginVersion": CODEX_SECURITY_UPSTREAM_PLUGIN_VERSION,
			"archiveSha256": CODEX_SECURITY_UPSTREAM_ARCHIVE_SHA256,
		},
		"inputs": inputs,
	});
	format!(
		"omp-security-workflow/v1:sha256:{}",
		sha256_hex(canonical_security_json(&value).as_bytes())
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn account() -> SecurityAccountRef {
		SecurityAccountRef {
			provider: "openai-codex".to_string(),
			credential_id: 7,
			account_id: Some("acct-1".to_string()),
			email: Some("person@example.test".to_string()),
			organization_id: Some("org-1".to_string()),
			organization_name: Some("Workspace".to_string()),
		}
	}

	#[test]
	fn redaction_drops_identity_and_token_keys_at_any_depth() {
		let value = serde_json::json!({
			"metadata": {"credentialId": 7, "accessToken": "secret", "nested": {"email": "a@b"}},
			"keep": true,
			"producer": {"name": "OMP Native Security"}
		});
		let redacted = redact_private_security_metadata(&value);
		assert_eq!(
			redacted,
			serde_json::json!({"metadata": {"nested": {}}, "keep": true, "producer": {"name": "OMP Native Security"}})
		);
	}

	#[test]
	fn workflow_fingerprint_changes_with_inputs() {
		let a = create_security_workflow_fingerprint(&["a", "b"]);
		let b = create_security_workflow_fingerprint(&["a", "c"]);
		assert!(a.starts_with("omp-security-workflow/v1:sha256:"));
		assert_ne!(a, b);
	}
}
