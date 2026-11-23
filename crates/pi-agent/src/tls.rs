//! Unified TLS and HTTP client configuration for `rustls` + `ring`.
//!
//! Configures reqwest clients to:
//! 1. Install ring as the default crypto provider.
//! 2. Preconfigure a rustls client with Mozilla's `webpki-roots` trust anchors,
//!    preserving name constraints (e.g. TUBITAK .tr) and maintaining strict
//!    certificate and hostname verification on standalone Android (Termux/bionic)
//!    without JVM / JNI dependencies and on headless Unix without host CA stores.

use std::sync::LazyLock;

static PROVIDER_INSTALLED: LazyLock<()> = LazyLock::new(|| {
	let _ = rustls::crypto::ring::default_provider().install_default();
});

/// Ensures ring crypto provider installed as process-wide default.
///
/// Safe to call concurrently from any thread; idempotent.
pub fn ensure_crypto_provider() {
	let _ = *PROVIDER_INSTALLED;
}

static SHARED_TLS_CONFIG: LazyLock<rustls::ClientConfig> = LazyLock::new(|| {
	ensure_crypto_provider();
	let mut root_store = rustls::RootCertStore::empty();
	root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
	let mut config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
		rustls::crypto::ring::default_provider(),
	))
	.with_safe_default_protocol_versions()
	.expect("safe default TLS versions")
	.with_root_certificates(root_store)
	.with_no_client_auth();
	config.alpn_protocols = vec![b"http/1.1".to_vec()];
	config
});

/// Returns clone of preconfigured rustls client config with Mozilla WebPKI trust anchors.
#[must_use]
pub fn shared_rustls_config() -> rustls::ClientConfig {
	SHARED_TLS_CONFIG.clone()
}

/// Creates a `reqwest::ClientBuilder` configured with ring crypto provider
/// and preconfigured rustls TLS client using Mozilla WebPKI trust anchors.
///
/// Preserves Mozilla trust anchor name constraints (e.g. TUBITAK .tr).
/// TLS certificate and hostname verification strictly enabled.
#[must_use]
pub fn http_client_builder() -> reqwest::ClientBuilder {
	ensure_crypto_provider();
	reqwest::Client::builder().use_preconfigured_tls(shared_rustls_config())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_tubitak_trust_anchor_has_name_constraint() {
		let tubitak = webpki_roots::TLS_SERVER_ROOTS
			.iter()
			.find(|ta| ta.subject.as_ref().windows(7).any(|w| w == b"TUBITAK"))
			.expect("TUBITAK root must be present in trust anchors");
		assert!(
			tubitak.name_constraints.is_some(),
			"TUBITAK root must retain name constraints"
		);
		let nc = tubitak.name_constraints.as_ref().unwrap().as_ref();
		assert!(
			nc.windows(3).any(|w| w == b".tr"),
			"TUBITAK root must be constrained to .tr"
		);
	}

	#[test]
	fn test_tls_client_builder_construction_and_alpn() {
		ensure_crypto_provider();
		let config = shared_rustls_config();
		assert_eq!(config.alpn_protocols, vec![b"http/1.1".to_vec()], "ALPN must be HTTP/1.1 only");
		let client = http_client_builder().build();
		assert!(client.is_ok(), "Client build must succeed with ring + preconfigured TLS");
	}
}
