//! Host seam for security scans.
//!
//! The CLI runtime owns provider construction and session lifecycle; the
//! security coordinator owns planning, persistence, recovery, and the exact
//! OAuth pin. A [`SecurityHost`] supplies only those two runtime facts plus the
//! active model/config snapshot. Nothing here constructs a second provider
//! engine.

use std::{
	fmt,
	path::PathBuf,
	sync::{Arc, Mutex},
};

use super::types::{SecurityModelRef, SecurityScanPlan, SecurityTokenUsage};

/// One side-session request. The publication tool is mounted ONLY inside this
/// session; `resolver` is called per model request so a refresh targets the
/// same pinned OAuth row.
#[derive(Clone)]
pub struct SecurityScanRequest {
	pub plan: SecurityScanPlan,
	pub scan_id: String,
	pub execution_root: PathBuf,
	pub model: SecurityModelRef,
	pub publication_tool: Arc<dyn pi_agent::tools::Tool>,
	pub cancel: pi_agent::CancellationToken,
	pub resolver: Arc<crate::auth::security::ExactSecurityOAuthResolver>,
}

/// Optional session statistics attached after a real model run.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SecurityScanStats {
	pub token_usage: Option<SecurityTokenUsage>,
	pub cost: Option<f64>,
	pub premium_requests: Option<u64>,
}

/// A running (or immediately runnable) scan session.
pub trait SecurityScanSession: Send + Sync {
	/// Prompt the restricted worker and wait for idle. `Err` fails the scan.
	fn run(&self, request_text: &str) -> Result<Option<SecurityScanStats>, String>;
	fn session_file(&self) -> Option<PathBuf>;
	fn abort(&self);
}

/// Runtime facts the C6 coordinator cannot derive from files.
pub trait SecurityHost: Send + Sync {
	fn security_enabled(&self) -> bool;
	fn cwd(&self) -> PathBuf;
	fn session_id(&self) -> Option<String>;
	fn agent_id(&self) -> Option<String>;
	fn active_model(&self) -> Option<SecurityModelRef>;
	/// TS `securityConfigSnapshot`: `{"securityEnabled": bool}`.
	fn config_snapshot(&self) -> serde_json::Value;
	fn state_root(&self) -> Option<PathBuf>;
	fn auth_db(&self) -> PathBuf;
	/// Pinned provider+model availability; NEVER a fallback chain.
	fn ensure_model(&self, model: &SecurityModelRef) -> Result<(), String>;
	fn create_scan_session(
		&self,
		request: SecurityScanRequest,
	) -> Result<Arc<dyn SecurityScanSession>, String>;
}

/// `RunRequest`-safe wrapper: `Arc<dyn SecurityHost>` has no `Debug`, and the
/// request must keep its derived `Debug`/`Clone`.
#[derive(Clone)]
pub struct SecurityHostHandle(pub Arc<dyn SecurityHost>);

impl SecurityHostHandle {
	#[must_use]
	pub fn new(host: Arc<dyn SecurityHost>) -> Self {
		Self(host)
	}

	#[must_use]
	pub fn host(&self) -> &Arc<dyn SecurityHost> {
		&self.0
	}
}

impl fmt::Debug for SecurityHostHandle {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("SecurityHostHandle")
	}
}

impl AsRef<dyn SecurityHost> for SecurityHostHandle {
	fn as_ref(&self) -> &(dyn SecurityHost + 'static) {
		self.0.as_ref()
	}
}

/// Open the exact-OAuth resolver for a planned account. Called by the runtime
/// factory; the store is shared through a mutex because SQLite connections are
/// not `Sync`.
pub fn exact_oauth_resolver(
	auth_db: &std::path::Path,
	account: &super::types::SecurityAccountRef,
) -> crate::Result<Arc<crate::auth::security::ExactSecurityOAuthResolver>> {
	let backend = crate::auth::AuthBackend::open_at(auth_db.to_path_buf())
		.map_err(|error| crate::Error::new(error.to_string()))?;
	Ok(Arc::new(crate::auth::security::ExactSecurityOAuthResolver::new(
		Arc::new(Mutex::new(backend)),
		account.into(),
	)))
}

/// Bridge a session cancellation token into the auth driver's cancellation.
#[must_use]
pub fn auth_cancel(token: &pi_agent::CancellationToken) -> crate::auth::http::Cancel {
	let token = token.clone();
	crate::auth::http::Cancel::with_predicate(Arc::new(move || token.is_cancelled()))
}
