//! OMP-native security scans: planning, restricted model review, once-only
//! publication, the project store, and the read-only `security://` namespace.
//!
//! Anchors: `packages/coding-agent/src/security/*` and
//! `packages/coding-agent/src/tools/security-scan.ts`.
//!
//! The runtime composes these modules; nothing here builds a second provider
//! engine. `SecurityHost` supplies the active model, config snapshot, auth
//! database, and the restricted session, so cost and provider policy stay in
//! the one runtime that already owns them.

use std::{path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};

pub mod coordinator;
pub mod host;
pub mod ids;
pub mod preflight;
pub mod provenance;
pub mod publication;
pub mod resource;
pub mod sarif;
pub mod store;
pub mod tool;
pub mod types;

pub use coordinator::{
	SecurityCoordinator, SecurityOperationSnapshot, SecurityPreflightInput, INTERRUPTED_MESSAGE,
	PARTIAL_MESSAGE, SECURITY_SESSION_TOOLS,
};
pub use host::{
	SecurityHost, SecurityHostHandle, SecurityScanRequest, SecurityScanSession, SecurityScanStats,
};
pub use preflight::{SecurityGitAdapter, SecurityPlanRequest, SecurityTargetRequest};
pub use publication::{SecurityPublisher, SecurityPublishTool, SecurityPublisherOptions};
pub use resource::{SecurityUrlHandler, SECURITY_SCHEME};
pub use store::SecurityStore;
pub use tool::{
	security_scan_schema, SecurityScanTool, CLOUD_PAUSED, SECURITY_DISABLED_TOOL,
	SECURITY_SCAN_TOOL_NAME,
};
pub use types::{
	SecurityAccountRef, SecurityEvidence, SecurityFinding, SecurityModelRef, SecurityScan,
	SecurityScanBundle, SecurityScanPlan,
};

/// `security` settings group.
///
/// The upstream schema leaf is `security.enabled` (flat dotted path, default
/// `false`); the nested group exists so `Config` and the settings loader have a
/// single typed home for it. Disabled keeps the tool, the coordinator, and the
/// `security://` scheme unmounted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityConfig {
	/// `security.enabled`; absent means [`Self::DEFAULT_ENABLED`].
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enabled: Option<bool>,
}

impl SecurityConfig {
	/// Upstream `security.enabled` default: security scans are opt-in.
	pub const DEFAULT_ENABLED: bool = false;

	#[must_use]
	pub fn enabled(&self) -> bool {
		self.enabled.unwrap_or(Self::DEFAULT_ENABLED)
	}

	/// Layered config merge: an explicit upper layer wins.
	pub fn merge(&mut self, overlay: Self) {
		if overlay.enabled.is_some() {
			self.enabled = overlay.enabled;
		}
	}

	/// `securityConfigSnapshot`: the object hashed into every plan fingerprint.
	#[must_use]
	pub fn snapshot(&self) -> serde_json::Value {
		serde_json::json!({"securityEnabled": self.enabled()})
	}
}

/// Mount seam for the runtime: the `security_scan` tool over a live host.
///
/// The caller registers it under [`tool::SECURITY_SCAN_TOOL_NAME`] and offers
/// the name in the tool list only when [`SecurityConfig::enabled`] is true.
#[must_use]
pub fn security_scan_tool(host: Arc<dyn SecurityHost>) -> Arc<dyn pi_agent::tools::Tool> {
	Arc::new(SecurityScanTool::new(host))
}

/// Mount seam for the runtime: the read-only `security://` protocol handler.
#[must_use]
pub fn security_url_handler(
	enabled: bool,
	state_root: Option<PathBuf>,
) -> Arc<dyn pi_agent::tools::internal_urls::ProtocolHandler> {
	Arc::new(SecurityUrlHandler::new(enabled, state_root))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn config_defaults_to_disabled_and_nests_under_security_enabled() {
		assert!(!SecurityConfig::default().enabled());
		assert_eq!(
			SecurityConfig::default().snapshot(),
			serde_json::json!({"securityEnabled": false})
		);
		let enabled = SecurityConfig { enabled: Some(true) };
		assert!(enabled.enabled());
		assert_eq!(enabled.snapshot(), serde_json::json!({"securityEnabled": true}));
		let mut merged = SecurityConfig::default();
		merged.merge(SecurityConfig { enabled: Some(false) });
		assert_eq!(merged.enabled, Some(false));
	}
}
