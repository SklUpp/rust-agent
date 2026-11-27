//! PKCE, CSRF state and loopback-safe random material.
//!
//! Anchors: `packages/ai/src/registry/oauth/pkce.ts:1` (96-byte verifier,
//! base64url, S256 challenge) and `callback-server.ts#generateState`
//! (16 random bytes, lowercase hex) plus Devin's `crypto.randomUUID()` state.

use base64::Engine as _;
use sha2::{Digest, Sha256};

use super::error::{AuthError, AuthErrorKind};

const BASE64URL: base64::engine::GeneralPurpose = base64::engine::GeneralPurpose::new(
	&base64::alphabet::URL_SAFE,
	base64::engine::GeneralPurposeConfig::new().with_encode_padding(false),
);

#[derive(Clone, Debug)]
pub struct Pkce {
	pub verifier: String,
	pub challenge: String,
}

impl Pkce {
	pub fn generate() -> Result<Self, AuthError> {
		let bytes = random_bytes(96)?;
		let verifier = BASE64URL.encode(bytes);
		let challenge = BASE64URL.encode(Sha256::digest(verifier.as_bytes()));
		Ok(Self { verifier, challenge })
	}
}

/// 16 random bytes as lowercase hex — `OAuthCallbackFlow#generateState`.
pub fn random_state() -> Result<String, AuthError> {
	let bytes = random_bytes(16)?;
	Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// RFC 4122 v4 UUID for flows that use one as the CSRF state (Devin).
pub fn random_uuid_v4() -> Result<String, AuthError> {
	let mut bytes = random_bytes(16)?;
	bytes[6] = (bytes[6] & 0x0f) | 0x40;
	bytes[8] = (bytes[8] & 0x3f) | 0x80;
	let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
	Ok(format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32]))
}

/// Owner id for a refresh lease — the TS side uses `crypto.randomUUID()`.
pub fn random_owner_id() -> Result<String, AuthError> {
	random_uuid_v4()
}

/// `crypto.getRandomValues` equivalent. A failing OS CSPRNG is a hard error:
/// a weak fallback would weaken PKCE/state.
fn random_bytes(len: usize) -> Result<Vec<u8>, AuthError> {
	let mut bytes = vec![0u8; len];
	pi_agent::security_primitives::random_bytes(&mut bytes).map_err(|err| {
		AuthError::new(AuthErrorKind::Configuration, format!("cannot fill random bytes: {err}"))
	})?;
	Ok(bytes)
}

/// Constant-time equality for CSRF/state checks: a length or byte mismatch
/// takes the same path, so timing cannot reveal how much of the nonce matched.
#[must_use]
pub fn constant_time_eq(left: &str, right: &str) -> bool {
	pi_agent::security_primitives::constant_time_eq_str(left, right)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn pkce_matches_rfc7636_s256_vector() {
		// RFC 7636 Appendix B verifier → challenge.
		let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
		let challenge = BASE64URL.encode(Sha256::digest(verifier.as_bytes()));
		assert_eq!(challenge, "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
	}

	#[test]
	fn generated_pkce_is_url_safe_and_sized() {
		let pkce = Pkce::generate().expect("os randomness");
		assert!(pkce.verifier.len() >= 128, "96 bytes base64url");
		assert!(!pkce.verifier.contains('='));
		assert!(!pkce.challenge.contains('+') && !pkce.challenge.contains('/'));
		assert_ne!(Pkce::generate().expect("os randomness").verifier, pkce.verifier);
	}

	#[test]
	fn state_is_32_lowercase_hex() {
		let state = random_state().expect("os randomness");
		assert_eq!(state.len(), 32);
		assert!(
			state
				.chars()
				.all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
		);
	}

	#[test]
	fn uuid_is_v4_shaped() {
		let uuid = random_uuid_v4().expect("os randomness");
		assert_eq!(uuid.len(), 36);
		assert_eq!(&uuid[14..15], "4");
		assert!(matches!(&uuid[19..20], "8" | "9" | "a" | "b"));
	}

	#[test]
	fn constant_time_eq_behaves() {
		assert!(constant_time_eq("abc", "abc"));
		assert!(!constant_time_eq("abc", "abd"));
		assert!(!constant_time_eq("abc", "abcd"));
	}
}
