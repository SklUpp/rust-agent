//! Incremental `Content-Length` framing for LSP stdio streams.
//!
//! Port of `packages/coding-agent/src/jsonrpc/message-framing.ts`, with explicit
//! bounds: a header block may not exceed [`MAX_HEADER_BYTES`] and one JSON
//! payload may not exceed [`MAX_CONTENT_BYTES`]. LSP uses the same base framing
//! as DAP (`Content-Length: N\r\n\r\n<JSON>`), *not* MCP's newline-delimited
//! transport.
//!
//! A header block without a parseable `Content-Length` is non-protocol noise
//! (a wrapper script printing to stdout). The framer drops through the bogus
//! terminator, reports the header text through the resync callback, and keeps
//! decoding instead of stalling on the same junk forever. An oversized payload
//! is a protocol violation that cannot be resynchronised safely, so it is
//! surfaced as an error and the reader tears the client down.

use std::{fmt, io::Write as _};

/// Maximum accepted header block, including the `\r\n\r\n` terminator.
pub const MAX_HEADER_BYTES: usize = 64 * 1024;
/// Maximum accepted JSON payload for one message.
pub const MAX_CONTENT_BYTES: usize = 64 * 1024 * 1024;

/// Encode one JSON value as a `Content-Length`-framed LSP/DAP message.
///
/// The header length is the UTF-8 byte count of the payload, never its
/// character count.
pub fn encode_frame(value: &serde_json::Value) -> Result<Vec<u8>, serde_json::Error> {
	let body = serde_json::to_vec(value)?;
	let mut frame = Vec::with_capacity(body.len() + 32);
	let _ = write!(frame, "Content-Length: {}\r\n\r\n", body.len());
	frame.extend_from_slice(&body);
	Ok(frame)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FramingError {
	HeaderTooLarge,
	ContentTooLarge(usize),
}

impl fmt::Display for FramingError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::HeaderTooLarge => write!(f, "LSP header exceeds {MAX_HEADER_BYTES} bytes"),
			Self::ContentTooLarge(len) => {
				write!(f, "LSP Content-Length {len} exceeds {MAX_CONTENT_BYTES} bytes")
			}
		}
	}
}

impl std::error::Error for FramingError {}

/// Incremental Content-Length decoder. Feed raw stdout chunks with
/// [`MessageFramer::push`], pull complete messages with [`MessageFramer::drain`].
#[derive(Debug, Default)]
pub struct MessageFramer {
	buffer: Vec<u8>,
}

impl MessageFramer {
	#[must_use]
	pub fn new() -> Self {
		Self { buffer: Vec::new() }
	}

	pub fn from_remainder(remainder: Vec<u8>) -> Self {
		Self { buffer: remainder }
	}

	pub fn push(&mut self, chunk: &[u8]) {
		self.buffer.extend_from_slice(chunk);
	}

	/// Drain every complete message currently buffered. `on_resync` receives the
	/// header block that lacked a valid `Content-Length`.
	pub fn drain(&mut self, mut on_resync: impl FnMut(&str)) -> Result<Vec<String>, FramingError> {
		let mut messages = Vec::new();
		loop {
			let Some(header_end) = find_header_end(&self.buffer) else {
				if self.buffer.len() > MAX_HEADER_BYTES {
					return Err(FramingError::HeaderTooLarge);
				}
				break;
			};
			if header_end + 4 > MAX_HEADER_BYTES {
				return Err(FramingError::HeaderTooLarge);
			}
			let header_bytes = &self.buffer[..header_end];
			let header_text = String::from_utf8_lossy(header_bytes);
			let message_start = header_end + 4;

			let Some(content_length) = parse_content_length(&header_text) else {
				on_resync(&header_text);
				self.buffer.drain(..message_start);
				continue;
			};
			if content_length > MAX_CONTENT_BYTES {
				return Err(FramingError::ContentTooLarge(content_length));
			}
			let message_end = message_start.saturating_add(content_length);
			if self.buffer.len() < message_end {
				break;
			}
			let payload = self.buffer[message_start..message_end].to_vec();
			self.buffer.drain(..message_end);
			match std::str::from_utf8(&payload) {
				Ok(text) => messages.push(text.to_owned()),
				Err(_) => {
					on_resync("<non-utf8 LSP payload>");
				}
			}
		}
		Ok(messages)
	}

	#[must_use]
	pub fn remainder(&self) -> Vec<u8> {
		self.buffer.clone()
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.buffer.is_empty()
	}
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
	buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

/// Match the upstream `/Content-Length: (\d+)/i` behaviour: the token may
/// appear on any header line, case-insensitively. A malformed length is treated
/// as missing so the caller resyncs past the bogus header.
fn parse_content_length(header: &str) -> Option<usize> {
	let lower = header.to_ascii_lowercase();
	let marker = "content-length:";
	let index = lower.find(marker)?;
	let rest = &header[index + marker.len()..];
	let digits: String = rest.trim_start().chars().take_while(char::is_ascii_digit).collect();
	if digits.is_empty() {
		return None;
	}
	digits.parse().ok()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn frame(text: &str) -> Vec<u8> {
		format!("Content-Length: {}\r\n\r\n{}", text.len(), text).into_bytes()
	}

	#[test]
	fn decodes_message_split_across_chunks() {
		let bytes = frame("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":null}");
		let mut framer = MessageFramer::new();
		let mut out = Vec::new();
		for chunk in bytes.chunks(5) {
			framer.push(chunk);
			out.extend(framer.drain(|_| {}).expect("valid frames"));
		}
		assert_eq!(out, vec!["{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":null}"]);
		assert!(framer.is_empty());
	}

	#[test]
	fn decodes_multiple_messages_in_one_chunk() {
		let mut bytes = frame("{\"id\":1}");
		bytes.extend(frame("{\"id\":2}"));
		let mut framer = MessageFramer::new();
		framer.push(&bytes);
		let out = framer.drain(|_| {}).expect("valid frames");
		assert_eq!(out, vec!["{\"id\":1}", "{\"id\":2}"]);
	}

	#[test]
	fn resyncs_past_header_without_content_length() {
		let mut bytes = b"wrapper noise\r\n\r\n".to_vec();
		bytes.extend(frame("{\"id\":7}"));
		let mut framer = MessageFramer::new();
		framer.push(&bytes);
		let mut resynced = Vec::new();
		let out = framer.drain(|header| resynced.push(header.to_owned())).expect("recoverable");
		assert_eq!(out, vec!["{\"id\":7}"]);
		assert_eq!(resynced.len(), 1);
	}

	#[test]
	fn malformed_content_length_is_reported_not_parsed() {
		let mut bytes = b"Content-Length: nope\r\n\r\n".to_vec();
		bytes.extend(frame("{\"id\":8}"));
		let mut framer = MessageFramer::new();
		framer.push(&bytes);
		let mut resynced = 0;
		let out = framer.drain(|_| resynced += 1).expect("recoverable");
		assert_eq!(resynced, 1);
		assert_eq!(out, vec!["{\"id\":8}"]);
	}

	#[test]
	fn oversized_content_length_is_an_error() {
		let mut framer = MessageFramer::new();
		framer.push(b"Content-Length: 999999999999\r\n\r\n");
		assert_eq!(
			framer.drain(|_| {}),
			Err(FramingError::ContentTooLarge(999_999_999_999))
		);
	}

	#[test]
	fn keeps_incomplete_payload_as_remainder() {
		let bytes = frame("{\"id\":9}");
		let mut framer = MessageFramer::new();
		framer.push(&bytes[..bytes.len() - 2]);
		assert!(framer.drain(|_| {}).expect("incomplete is not an error").is_empty());
		assert_eq!(framer.remainder(), bytes[..bytes.len() - 2].to_vec());
	}

	#[test]
	fn encoded_frame_round_trips_through_framer() {
		let value = serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": null});
		let bytes = encode_frame(&value).expect("encode");
		let mut framer = MessageFramer::new();
		framer.push(&bytes);
		let out = framer.drain(|_| {}).expect("decode");
		assert!(framer.is_empty());
		assert_eq!(out.len(), 1);
		assert_eq!(
			serde_json::from_str::<serde_json::Value>(&out[0]).expect("json"),
			value
		);
	}

	#[test]
	fn encoded_header_counts_utf8_bytes_not_chars() {
		let value = serde_json::json!({"text": "héllo—日本語"});
		let body = serde_json::to_vec(&value).expect("body");
		assert!(body.len() > value.to_string().chars().count());
		let bytes = encode_frame(&value).expect("encode");
		let header = std::str::from_utf8(&bytes[..bytes.len() - body.len()]).expect("utf8 header");
		assert_eq!(header, format!("Content-Length: {}\r\n\r\n", body.len()));
		let mut framer = MessageFramer::new();
		framer.push(&bytes);
		assert_eq!(
			framer.drain(|_| {}).expect("decode"),
			vec![String::from_utf8(body).expect("utf8 body")]
		);
	}
}
