//! Exact redaction of credential values this process actually holds.
//!
//! `error::redact_free_text` recognises secret *shapes*. It cannot mask a
//! credential that has no recognisable prefix, no digits, or that a provider
//! hands out in an opaque format. This module is the layer above: every value
//! the process resolved is matched literally.
//!
//! Strategy: one `AhoCorasick` automaton, `MatchKind::LeftmostLongest`, over
//! the raw value plus its percent-encoded and base64 forms. Leftmost-longest
//! matters for correctness rather than speed: with the default `Standard`
//! kind a pattern that is a prefix of another can match first and leave the
//! rest of the longer secret in the output, so the residue leaks.
//!
//! Invariant: a value passed to [`register`] is masked for the rest of the
//! process lifetime in every later `redact_free_text` call. There is no switch
//! that turns this layer off.
//!
//! Coverage is the text that goes through `redact_free_text`: stream drafts,
//! message payloads at write time (and the durable revision digest), error
//! text and memory lessons. Live streaming frames, the in-process message
//! vector of the current turn, `executions`/`deliveries` payloads and rows
//! written before a value was registered are outside it.
//!
//! Cost: one linear scan per redacted string, skipped entirely (borrowed
//! output, no allocation) when no automaton is registered or nothing matches.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use aho_corasick::{AhoCorasick, MatchKind};

/// Values shorter than this are prose or identifiers, not credentials.
pub const MIN_SECRET_LEN: usize = 8;
/// Shared marker with the heuristic layer so output is uniform and idempotent.
pub const REDACTION: &str = "[REDACTED]";

/// Compiled matcher over a fixed set of known values.
pub struct KnownSecrets {
	automaton: AhoCorasick,
}

impl KnownSecrets {
	/// `None` when every value is too short or the set is empty.
	#[must_use]
	pub fn new<'a>(values: impl IntoIterator<Item = &'a str>) -> Option<Self> {
		let mut patterns: BTreeSet<String> = BTreeSet::new();
		for value in values {
			let trimmed = value.trim();
			if trimmed.chars().count() < MIN_SECRET_LEN {
				continue;
			}
			for variant in variants(trimmed) {
				patterns.insert(variant);
			}
		}
		if patterns.is_empty() {
			return None;
		}
		let automaton = AhoCorasick::builder()
			.match_kind(MatchKind::LeftmostLongest)
			.build(patterns)
			.ok()?;
		Some(Self { automaton })
	}

	/// Mask every known value in `text`; borrowed when nothing matches.
	#[must_use]
	pub fn redact<'a>(&self, text: &'a str) -> Cow<'a, str> {
		let mut out: Option<String> = None;
		let mut last = 0usize;
		for found in self.automaton.find_iter(text) {
			let out = out.get_or_insert_with(|| String::with_capacity(text.len()));
			out.push_str(&text[last..found.start()]);
			out.push_str(REDACTION);
			last = found.end();
		}
		match out {
			Some(mut out) => {
				out.push_str(&text[last..]);
				Cow::Owned(out)
			},
			None => Cow::Borrowed(text),
		}
	}
}

/// The carrier forms a credential shows up in: as issued, percent-encoded in a
/// pasted URL (either hex case), and base64 of the value itself, standard or
/// URL-safe, padded or not.
///
/// This matches an encoded blob that *is* the value. Base64 of a token embedded
/// at an arbitrary offset inside a larger structure (`Basic base64(user:token)`,
/// a JWT payload) is different bytes and needs the raw form or a decode step;
/// that is out of scope here and stated as such.
fn variants(value: &str) -> Vec<String> {
	let mut out = vec![value.to_string()];
	for lowercase in [false, true] {
		let percent = percent_encode(value, lowercase);
		if percent != value {
			out.push(percent);
		}
	}
	for engine in [
		base64::engine::general_purpose::STANDARD,
		base64::engine::general_purpose::STANDARD_NO_PAD,
		base64::engine::general_purpose::URL_SAFE,
		base64::engine::general_purpose::URL_SAFE_NO_PAD,
	] {
		let encoded = base64::Engine::encode(&engine, value.as_bytes());
		if encoded.len() >= MIN_SECRET_LEN {
			out.push(encoded);
		}
	}
	out
}

fn percent_encode(value: &str, lowercase: bool) -> String {
	const UNRESERVED: &[u8] = b"-._~";
	let mut out = String::with_capacity(value.len() * 3);
	for byte in value.as_bytes() {
		if byte.is_ascii_alphanumeric() || UNRESERVED.contains(byte) {
			out.push(char::from(*byte));
		} else {
			out.push('%');
			if lowercase {
				out.push_str(&format!("{byte:02x}"));
			} else {
				out.push_str(&format!("{byte:02X}"));
			}
		}
	}
	out
}

#[derive(Default)]
struct Registry {
	values: BTreeSet<String>,
	compiled: Option<Arc<KnownSecrets>>,
	dirty: bool,
}

static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(|| Mutex::new(Registry::default()));

/// Record a resolved credential. Monotonic: values are never removed, so a
/// rotated key stays masked for the lifetime of the process.
pub fn register(value: &str) {
	let trimmed = value.trim();
	if trimmed.chars().count() < MIN_SECRET_LEN {
		return;
	}
	let mut registry = REGISTRY.lock().unwrap_or_else(PoisonError::into_inner);
	if !registry.values.insert(trimmed.to_string()) {
		return;
	}
	registry.dirty = true;
}

fn compiled() -> Option<Arc<KnownSecrets>> {
	let mut registry = REGISTRY.lock().unwrap_or_else(PoisonError::into_inner);
	if registry.dirty {
		registry.compiled =
			KnownSecrets::new(registry.values.iter().map(String::as_str)).map(Arc::new);
		registry.dirty = false;
	}
	registry.compiled.clone()
}

/// Mask every registered value in `text`, borrowing when there is nothing to
/// mask. Called from `redact_free_text` before the shape heuristics.
#[must_use]
pub fn redact(text: &str) -> Cow<'_, str> {
	if text.is_empty() {
		return Cow::Borrowed(text);
	}
	match compiled() {
		Some(secrets) => secrets.redact(text),
		None => Cow::Borrowed(text),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn secrets(values: &[&str]) -> KnownSecrets {
		KnownSecrets::new(values.iter().copied()).expect("automaton")
	}

	#[test]
	fn masks_known_value_and_keeps_surrounding_text() {
		let matcher = secrets(&["sk-live-abcdef123456"]);
		let text = "request failed: Authorization=sk-live-abcdef123456 (retry later)";
		assert_eq!(matcher.redact(text), "request failed: Authorization=[REDACTED] (retry later)");
	}

	#[test]
	fn longer_value_wins_over_its_prefix() {
		// Standard match kind would mask the short pattern and leak `-suffix`.
		let matcher = secrets(&["sk-sharedprefix", "sk-sharedprefix-suffix"]);
		assert_eq!(matcher.redact("key sk-sharedprefix-suffix end"), "key [REDACTED] end");
	}

	#[test]
	fn masks_percent_encoded_and_base64_forms() {
		let secret = "tok/en+with space123";
		let matcher = secrets(&[secret]);
		let percent = percent_encode(secret, false);
		assert_ne!(percent, secret);
		assert_eq!(matcher.redact(&format!("u={percent}")), "u=[REDACTED]");
		let b64 =
			base64::Engine::encode(&base64::engine::general_purpose::STANDARD, secret.as_bytes());
		assert_eq!(matcher.redact(&format!("blob {b64}")), "blob [REDACTED]");
		let url_safe = base64::Engine::encode(
			&base64::engine::general_purpose::URL_SAFE_NO_PAD,
			secret.as_bytes(),
		);
		assert_eq!(matcher.redact(&format!("blob {url_safe}")), "blob [REDACTED]");
		let lower = percent_encode(secret, true);
		assert_eq!(matcher.redact(&format!("u={lower}")), "u=[REDACTED]");
	}

	#[test]
	fn untouched_text_is_borrowed_without_allocation() {
		let matcher = secrets(&["sk-live-abcdef123456"]);
		assert!(matches!(matcher.redact("plain prose, nothing secret"), Cow::Borrowed(_)));
	}

	#[test]
	fn redaction_is_idempotent() {
		let matcher = secrets(&["sk-live-abcdef123456"]);
		let once = matcher.redact("a sk-live-abcdef123456 b").into_owned();
		assert_eq!(matcher.redact(&once), once);
	}

	#[test]
	fn short_values_are_not_treated_as_secrets() {
		assert!(KnownSecrets::new(["abc"]).is_none());
	}
}
