//! Cross-platform random material and constant-time comparison helpers.
//!
//! Randomness comes from the rustls/ring OS CSPRNG, so no `/dev/urandom`
//! probing or weak fallback is needed on Linux, Android, macOS or Windows.

use rustls::crypto::SecureRandom as _;

/// Fills `buf` with cryptographically secure random bytes from the OS.
pub fn random_bytes(buf: &mut [u8]) -> std::io::Result<()> {
	rustls::crypto::ring::default_provider()
		.secure_random
		.fill(buf)
		.map_err(|_| std::io::Error::other("OS CSPRNG unavailable"))
}

/// `bytes` random bytes as lowercase hex.
pub fn random_hex(bytes: usize) -> std::io::Result<String> {
	let mut buffer = vec![0u8; bytes];
	random_bytes(&mut buffer)?;
	let mut text = String::with_capacity(bytes * 2);
	for byte in buffer {
		use std::fmt::Write as _;
		let _ = write!(text, "{byte:02x}");
	}
	Ok(text)
}

/// Length-independent comparison: a length or byte mismatch takes the same
/// path, so timing cannot reveal how much of the value matched.
#[must_use]
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
	let mut difference = left.len() ^ right.len();
	for index in 0..left.len().max(right.len()) {
		let a = left.get(index).copied().unwrap_or(0);
		let b = right.get(index).copied().unwrap_or(0);
		difference |= usize::from(a ^ b);
	}
	difference == 0
}

/// Constant-time comparison of two UTF-8 strings.
#[must_use]
pub fn constant_time_eq_str(left: &str, right: &str) -> bool {
	constant_time_eq(left.as_bytes(), right.as_bytes())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn random_bytes_fills_requested_length() {
		for len in [0usize, 1, 16, 96] {
			let mut buf = vec![0u8; len];
			random_bytes(&mut buf).expect("OS CSPRNG");
			assert_eq!(buf.len(), len);
		}
	}

	#[test]
	fn random_bytes_are_distinct() {
		let mut first = [0u8; 32];
		let mut second = [0u8; 32];
		random_bytes(&mut first).expect("OS CSPRNG");
		random_bytes(&mut second).expect("OS CSPRNG");
		assert_ne!(first, second);
	}

	#[test]
	fn random_hex_is_lowercase_hex_of_requested_size() {
		let hex = random_hex(16).expect("OS CSPRNG");
		assert_eq!(hex.len(), 32);
		assert!(
			hex.chars()
				.all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
		);
	}

	#[test]
	fn random_hex_outputs_are_distinct() {
		assert_ne!(random_hex(16).expect("OS CSPRNG"), random_hex(16).expect("OS CSPRNG"));
		assert_eq!(random_hex(0).expect("OS CSPRNG"), "");
	}

	#[test]
	fn constant_time_eq_handles_equal_diff_and_length_changes() {
		assert!(constant_time_eq(b"secret", b"secret"));
		assert!(!constant_time_eq(b"secret", b"secrez"));
		assert!(!constant_time_eq(b"secret", b"secret2"));
		assert!(!constant_time_eq(b"secret2", b"secret"));
		assert!(!constant_time_eq(b"", b"x"));
		assert!(constant_time_eq(b"", b""));
		assert!(constant_time_eq_str("abc", "abc"));
		assert!(!constant_time_eq_str("abc", "abd"));
	}
}
