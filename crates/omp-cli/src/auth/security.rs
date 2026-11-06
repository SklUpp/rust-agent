//! Exact OAuth pinning for OMP-native security scans.
//!
//! Anchors: `packages/coding-agent/src/security/auth.ts`.
//!
//! A security scan selects one stored OAuth row before planning and every
//! later credential resolution targets that same durable row. There is no
//! API-key, environment, static, or sibling-account fallback: an unavailable
//! account fails the scan instead of crossing an account/workspace boundary.
//!
//! Differences from [`super::AuthBackend::api_key_for_session`] are deliberate:
//! that method implements the normal login precedence (OAuth, API key, env,
//! static) and usage-ranked rotation. These functions never touch it.

use serde::{Deserialize, Serialize};

use super::error::{AuthError, AuthErrorKind};
use super::http::Cancel;
use super::store::{Credential, StoredCredential};
use super::{registry, AuthBackend};

/// Durable identity of the OAuth row a security scan is pinned to
/// (`SecurityAccountRef` in the security contracts).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityOAuthAccount {
	pub provider: String,
	pub credential_id: i64,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub account_id: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub email: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub org_id: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub org_name: Option<String>,
}

impl SecurityOAuthAccount {
	/// `assertSecurityIdentityMatches`: a resolved row MUST be the pinned row
	/// and MUST agree on every recorded identity field. A missing field on the
	/// pin is not a wildcard for a *different* credential id.
	pub fn assert_matches(&self, resolution: &SecurityOAuthAccount) -> Result<(), AuthError> {
		let matches = self.credential_id == resolution.credential_id
			&& self.provider == resolution.provider
			&& account_field_matches(&self.account_id, &resolution.account_id)
			&& account_field_matches(&self.email, &resolution.email)
			&& account_field_matches(&self.org_id, &resolution.org_id)
			&& account_field_matches(&self.org_name, &resolution.org_name);
		if matches {
			return Ok(());
		}
		Err(AuthError::new(
			AuthErrorKind::Configuration,
			"Security scan authentication identity mismatch",
		)
		.for_provider(&self.provider))
	}
}

fn account_field_matches(pinned: &Option<String>, resolved: &Option<String>) -> bool {
	pinned.as_ref().is_none_or(|pinned| Some(pinned) == resolved.as_ref())
}

/// Resolved access token plus the durable identity it came from.
#[derive(Debug, Clone)]
pub struct SecurityOAuthAccess {
	pub account: SecurityOAuthAccount,
	pub access_token: String,
}

/// `selectSecurityAccount`: pin one OAuth row for `model.provider`.
///
/// 1. an explicit `requested_credential_id` must exist among the provider's
///    active OAuth rows;
/// 2. a lone account is selected automatically;
/// 3. several accounts without an explicit id is an error — never a guess.
///
/// `session_id` is accepted for call-site parity with TS
/// `listOAuthAccounts(provider, sessionId).active`, but this backend records no
/// session→credential pin: [`SelectionState`](super::selection::SelectionState)
/// only holds round-robin counters and backoff windows, so a session id merely
/// shifts a selection order. Treating it as a pin would silently scan under a
/// hash-chosen account, which the security contract forbids; only a durable
/// `credentialId` can pin a multi-account provider here.
pub fn select_security_account(
	backend: &AuthBackend,
	provider: &str,
	requested_credential_id: Option<i64>,
	_session_id: Option<&str>,
) -> Result<SecurityOAuthAccount, AuthError> {
	let entry = registry::find(provider).ok_or_else(|| AuthError::unknown_provider(provider))?;
	let stored_as = registry::storage_provider(entry);
	let accounts = oauth_rows(backend, stored_as)?;
	if accounts.is_empty() {
		return Err(AuthError::config(
			provider,
			format!("Security scans require a stored OAuth account for {provider}"),
		));
	}
	let selected = match requested_credential_id {
		Some(requested) => accounts
			.iter()
			.find(|row| row.id == requested)
			.ok_or_else(|| {
				AuthError::config(
					provider,
					format!("Security OAuth credential {requested} is not available for {provider}"),
				)
			})?,
		None => match accounts.as_slice() {
			[only] => only,
			_ => {
				return Err(AuthError::config(
					provider,
					format!(
						"Multiple OAuth accounts are available for {provider}; supply credentialId to pin one exact account"
					),
				))
			},
		},
	};
	account_from_row(provider, selected)
}

fn oauth_rows(backend: &AuthBackend, stored_as: &str) -> Result<Vec<StoredCredential>, AuthError> {
	let mut rows = backend.store().list_active(stored_as)?;
	rows.retain(|row| matches!(row.credential, Credential::OAuth(_)));
	Ok(rows)
}

fn account_from_row(provider: &str, row: &StoredCredential) -> Result<SecurityOAuthAccount, AuthError> {
	let Credential::OAuth(creds) = &row.credential else {
		return Err(AuthError::config(provider, "selected credential is not an OAuth account"));
	};
	Ok(SecurityOAuthAccount {
		provider: provider.to_string(),
		credential_id: row.id,
		account_id: creds.account_id.clone(),
		email: creds.email.clone(),
		org_id: creds.org_id.clone(),
		org_name: creds.org_name.clone(),
	})
}

/// `getOAuthAccessByCredentialId` + `assertSecurityIdentityMatches`: refresh
/// ONLY the pinned row, then verify the resolved identity still matches.
///
/// `force_refresh` mirrors the auth-retry `lastChance` path: the normal retry
/// driver asks once more after an authentication error, still on this row.
pub fn resolve_security_oauth_access(
	backend: &AuthBackend,
	pinned: &SecurityOAuthAccount,
	force_refresh: bool,
	cancel: &Cancel,
) -> Result<SecurityOAuthAccess, AuthError> {
	let provider = pinned.provider.as_str();
	let (resolved, access_token) = backend.resolve_exact_oauth_access(
		provider,
		pinned.credential_id,
		force_refresh,
		cancel,
	)?;
	pinned.assert_matches(&resolved)?;
	Ok(SecurityOAuthAccess { account: resolved, access_token })
}

/// Model request credential: initial call and retries both target the pinned
/// row; the retry `lastChance` step returns `None` so the transport fails the
/// scan rather than rotating to a sibling account.
pub struct ExactSecurityOAuthResolver {
	backend: std::sync::Arc<std::sync::Mutex<AuthBackend>>,
	pinned: SecurityOAuthAccount,
}

/// Opaque: the auth backend is not `Debug` and must not be exposed through a
/// formatter; only the pinned, plan-visible identity is reported.
impl std::fmt::Debug for ExactSecurityOAuthResolver {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("ExactSecurityOAuthResolver")
			.field("provider", &self.pinned.provider)
			.field("credential_id", &self.pinned.credential_id)
			.field("backend", &"<pinned auth backend>")
			.finish()
	}
}

impl ExactSecurityOAuthResolver {
	pub fn new(
		backend: std::sync::Arc<std::sync::Mutex<AuthBackend>>,
		pinned: SecurityOAuthAccount,
	) -> Self {
		Self { backend, pinned }
	}

	#[must_use]
	pub fn pinned(&self) -> &SecurityOAuthAccount {
		&self.pinned
	}

	/// `getApiKey(model)`: provider mismatch is a hard error, never a silent
	/// cross-provider lookup.
	pub fn api_key_for(
		&self,
		provider: &str,
		force_refresh: bool,
		cancel: &Cancel,
	) -> Result<Option<String>, AuthError> {
		if provider != self.pinned.provider {
			return Err(AuthError::new(
				AuthErrorKind::Configuration,
				"Security scan authentication provider mismatch",
			)
			.for_provider(&self.pinned.provider));
		}
		let backend = self
			.backend
			.lock()
			.map_err(|_| AuthError::new(AuthErrorKind::Storage, "auth store lock poisoned"))?;
		let access = resolve_security_oauth_access(&backend, &self.pinned, force_refresh, cancel)?;
		Ok(Some(access.access_token))
	}

	/// The retry driver's final sibling-rotation step. `None` is the contract:
	/// no unaligned credential is ever returned.
	#[must_use]
	pub fn last_chance(&self) -> Option<String> {
		None
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn account(id: i64) -> SecurityOAuthAccount {
		SecurityOAuthAccount {
			provider: "openai-codex".to_string(),
			credential_id: id,
			account_id: Some(format!("acct-{id}")),
			email: Some(format!("user{id}@example.test")),
			org_id: Some("org-1".to_string()),
			org_name: Some("Workspace".to_string()),
		}
	}

	#[test]
	fn identity_mismatch_is_rejected_per_field() {
		let pinned = account(1);
		pinned.assert_matches(&account(1)).expect("same identity");
		let mut other_id = account(1);
		other_id.credential_id = 2;
		assert!(pinned.assert_matches(&other_id).is_err());
		let mut other_account = account(1);
		other_account.account_id = Some("acct-2".to_string());
		assert!(pinned.assert_matches(&other_account).is_err());
		let mut other_email = account(1);
		other_email.email = Some("other@example.test".to_string());
		assert!(pinned.assert_matches(&other_email).is_err());
		let mut other_org = account(1);
		other_org.org_id = Some("org-2".to_string());
		assert!(pinned.assert_matches(&other_org).is_err());
		let mut other_org_name = account(1);
		other_org_name.org_name = Some("Other".to_string());
		assert!(pinned.assert_matches(&other_org_name).is_err());
	}

	#[test]
	fn absent_pinned_field_accepts_absent_resolution_only() {
		let mut pinned = account(1);
		pinned.email = None;
		let mut resolved = account(1);
		resolved.email = None;
		pinned.assert_matches(&resolved).expect("both absent");
		resolved.email = Some("late@example.test".to_string());
		pinned.assert_matches(&resolved).expect("unrecorded field is not compared");
	}

	#[test]
	fn last_chance_never_rotates_to_a_sibling() {
		let backend = std::sync::Arc::new(std::sync::Mutex::new(
			AuthBackend::in_memory().expect("in-memory store"),
		));
		let resolver = ExactSecurityOAuthResolver::new(backend, account(1));
		assert_eq!(resolver.last_chance(), None);
	}
}
