//! Small dependency-free helpers shared by eval backends.

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 with `=` padding.
pub fn base64_encode(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
	for chunk in bytes.chunks(3) {
		let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
		let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
		out.push(ALPHABET[(n >> 18) as usize & 63] as char);
		out.push(ALPHABET[(n >> 12) as usize & 63] as char);
		out.push(if chunk.len() > 1 { ALPHABET[(n >> 6) as usize & 63] as char } else { '=' });
		out.push(if chunk.len() > 2 { ALPHABET[n as usize & 63] as char } else { '=' });
	}
	out
}

/// Strict-enough standard base64 decode; whitespace is ignored, other bytes reject.
pub fn base64_decode(data: &str) -> Option<Vec<u8>> {
	let mut out = Vec::with_capacity(data.len() / 4 * 3);
	let mut buffer = 0u32;
	let mut bits = 0u32;
	for byte in data.bytes() {
		let value = match byte {
			b'A'..=b'Z' => u32::from(byte - b'A'),
			b'a'..=b'z' => u32::from(byte - b'a') + 26,
			b'0'..=b'9' => u32::from(byte - b'0') + 52,
			b'+' => 62,
			b'/' => 63,
			b'=' | b'\n' | b'\r' | b' ' | b'\t' => continue,
			_ => return None,
		};
		buffer = (buffer << 6) | value;
		bits += 6;
		if bits >= 8 {
			bits -= 8;
			out.push((buffer >> bits) as u8);
		}
	}
	Some(out)
}

/// Random hex token for the loopback bridge from the OS CSPRNG.
///
/// Fails closed: a host without working OS randomness cannot serve the bridge.
pub fn random_token() -> String {
	crate::security_primitives::random_hex(16).expect("OS CSPRNG unavailable")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn base64_round_trip_pads_correctly() {
		for input in [b"".as_slice(), b"a", b"ab", b"abc", b"hello world"] {
			let encoded = base64_encode(input);
			assert_eq!(encoded.len() % 4, 0);
			assert_eq!(base64_decode(&encoded).unwrap(), input);
		}
	}

	#[test]
	fn tokens_are_hex_and_unique() {
		let a = random_token();
		let b = random_token();
		assert_eq!(a.len(), 32);
		assert!(a.bytes().all(|byte| byte.is_ascii_hexdigit()));
		assert_ne!(a, b);
	}
}
