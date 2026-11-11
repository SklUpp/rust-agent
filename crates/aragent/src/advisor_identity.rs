//! Stable provider-facing identity for one advisor inside one primary session.
//!
//! Mirrors `advisor/config.ts` `getOrCreateAdvisorProviderSessionId` +
//! the per-advisor provider rebinding in `session-advisors.ts`: a UUIDv7 that is
//! stable for `(primary session, slug)`, a prompt-cache key that follows the
//! primary's explicit pin (else the advisor UUID), and `tier.advisor` selection.
//! The advisor never borrows the primary's credentials, session id, or tier.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Mutex;

/// Key separator used by the TS map; kept as NUL so no slug can collide.
const KEY_SEPARATOR: char = '\u{0}';

/// One UUIDv7 (48-bit unix ms, version 7, RFC 4122 variant, random tail).
#[must_use]
pub fn uuid_v7() -> String {
	static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
	let millis = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis() as u64;
	let mut random = [0u8; 10];
	let filled = std::fs::File::open("/dev/urandom")
		.and_then(|mut file| file.read_exact(&mut random))
		.is_ok();
	if !filled {
		// No entropy file (non-Unix test host): derive a unique-enough tail from
		// time, pid, a process-local counter and the caller's address space.
		let mut tail = [0u8; 10];
		let seed = format!(
			"{millis}:{}:{}:{:p}",
			std::process::id(),
			COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
			&random
		);
		let a = xxhash_rust::xxh64::xxh64(seed.as_bytes(), 0).to_be_bytes();
		let b = xxhash_rust::xxh64::xxh64(&a, 0).to_be_bytes();
		tail[..8].copy_from_slice(&a);
		tail[8..].copy_from_slice(&b[..2]);
		random = tail;
	}
	let mut bytes = [0u8; 16];
	bytes[..6].copy_from_slice(&millis.to_be_bytes()[2..]);
	bytes[6] = 0x70 | (random[0] & 0x0f);
	bytes[7] = random[1];
	bytes[8] = 0x80 | (random[2] & 0x3f);
	bytes[9..].copy_from_slice(&random[3..]);
	format!(
		"{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
		bytes[0],
		bytes[1],
		bytes[2],
		bytes[3],
		bytes[4],
		bytes[5],
		bytes[6],
		bytes[7],
		bytes[8],
		bytes[9],
		bytes[10],
		bytes[11],
		bytes[12],
		bytes[13],
		bytes[14],
		bytes[15]
	)
}

/// `^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
#[must_use]
pub fn is_uuid_v7(value: &str) -> bool {
	let bytes = value.as_bytes();
	if bytes.len() != 36 {
		return false;
	}
	for (index, byte) in bytes.iter().enumerate() {
		match index {
			8 | 13 | 18 | 23 => {
				if *byte != b'-' {
					return false;
				}
			},
			14 => {
				if *byte != b'7' {
					return false;
				}
			},
			19 => {
				if !matches!(*byte, b'8' | b'9' | b'a' | b'b') {
					return false;
				}
			},
			_ => {
				if !byte.is_ascii_hexdigit() {
					return false;
				}
			},
		}
	}
	true
}

/// Stable UUIDv7 per `(primary, slug)` within this process.
#[derive(Debug, Default)]
pub struct AdvisorIdentities {
	ids: Mutex<BTreeMap<String, String>>,
}

impl AdvisorIdentities {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	/// Stable identity. An in-memory primary (`None`) still gets one: the
	/// advisor's provider identity must never borrow the primary's default,
	/// and the contract requires a stable UUIDv7 scope per advisor.
	#[must_use]
	pub fn provider_session_id(&self, primary: Option<&str>, slug: &str) -> String {
		let key = format!(
			"{}{KEY_SEPARATOR}{slug}",
			primary.unwrap_or("anonymous")
		);
		let mut ids = self.ids.lock().unwrap_or_else(|poison| poison.into_inner());
		if let Some(existing) = ids.get(&key) {
			return existing.clone();
		}
		let next = loop {
			let candidate = uuid_v7();
			if is_uuid_v7(&candidate) {
				break candidate;
			}
		};
		ids.insert(key, next.clone());
		next
	}
}

/// An explicit primary cache pin wins; otherwise the advisor UUID keeps Codex
/// request identity UUID-shaped.
#[must_use]
pub fn prompt_cache_key(primary: Option<&str>, advisor_session_id: &str) -> String {
	primary
		.filter(|key| !key.trim().is_empty())
		.unwrap_or(advisor_session_id)
		.to_string()
}

/// `tier.advisor`: `none` runs standard processing, `inherit` tracks the live
/// per-family map (including `/fast` toggles), a concrete value broadcasts
/// across families. Unknown values yield an empty map (the config layer already
/// warned and dropped them).
#[must_use]
pub fn service_tier_map(
	setting: Option<&str>,
	primary: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
	match setting {
		Some("inherit") => primary.clone(),
		Some(value) if value != "none" => {
			let mut map = BTreeMap::new();
			for family in ["openai", "anthropic", "google"] {
				if let Some(tier) = crate::service_tier::family_value(family, value) {
					map.insert(family.to_string(), tier.to_string());
				}
			}
			map
		},
		_ => BTreeMap::new(),
	}
}

/// Local-only label for logs/telemetry (`<primary>-advisor-<slug>`); never sent
/// as a provider identity.
#[must_use]
pub fn session_label(primary: Option<&str>, slug: &str) -> String {
	match (primary, slug.is_empty()) {
		(Some(primary), false) => format!("{primary}-advisor-{slug}"),
		(Some(primary), true) => format!("{primary}-advisor"),
		(None, false) => format!("anonymous-advisor-{slug}"),
		(None, true) => "anonymous-advisor".to_string(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn uuid_v7_shape_and_uniqueness() {
		let first = uuid_v7();
		let second = uuid_v7();
		assert!(is_uuid_v7(&first), "{first}");
		assert!(is_uuid_v7(&second), "{second}");
		assert_ne!(first, second);
		assert!(!is_uuid_v7("00000000-0000-4000-8000-000000000000"));
	}

	#[test]
	fn identity_is_stable_per_primary_and_slug_and_distinct_across_slugs() {
		let ids = AdvisorIdentities::new();
		let a = ids.provider_session_id(Some("primary"), "reviewer");
		assert_eq!(a, ids.provider_session_id(Some("primary"), "reviewer"));
		let b = ids.provider_session_id(Some("primary"), "security");
		assert_ne!(a, b);
		let other = ids.provider_session_id(Some("other"), "reviewer");
		assert_ne!(a, other);
		assert!(is_uuid_v7(&a) && is_uuid_v7(&b) && is_uuid_v7(&other));
	}

	#[test]
	fn cache_key_follows_explicit_primary_pin_else_advisor_uuid() {
		let id = uuid_v7();
		assert_eq!(prompt_cache_key(Some("pin"), &id), "pin");
		assert_eq!(prompt_cache_key(None, &id), id);
		assert_eq!(prompt_cache_key(Some("  "), &id), id);
	}

	#[test]
	fn tier_advisor_none_inherit_and_broadcast() {
		let primary = BTreeMap::from([("openai".to_string(), "flex".to_string())]);
		assert!(service_tier_map(Some("none"), &primary).is_empty());
		assert_eq!(service_tier_map(Some("inherit"), &primary), primary);
		let broadcast = service_tier_map(Some("priority"), &primary);
		assert_eq!(broadcast.get("openai").map(String::as_str), Some("priority"));
		assert_eq!(broadcast.get("anthropic").map(String::as_str), Some("priority"));
		assert_eq!(broadcast.get("google").map(String::as_str), Some("priority"));
		assert!(service_tier_map(Some("bogus"), &primary).is_empty());
	}
}
