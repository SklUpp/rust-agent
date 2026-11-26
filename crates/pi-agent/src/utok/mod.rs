//! Universal offline tokenizer.
//!
//! Exact token counting (and, where meaningful, encoding) for six model
//! families, with all vocabulary data zstd-compressed and embedded in the
//! binary. No network, no files, no external tokenizer runtimes.
//!
//! | [`Encoding`] | family | mechanism |
//! |---|---|---|
//! | `O200kBase` | GPT-4o / o1 / GPT-5+ | byte-level BPE (tiktoken ranks) |
//! | `Cl100kBase` | GPT-3.5 / GPT-4 | byte-level BPE |
//! | `ClaudeV3` / `ClaudeV47` / `ClaudeV5` / `ClaudeV5Sonnet` | Claude generations | ctok count reconstruction (count-only) |
//! | `Qwen3` | Qwen 3.5 / 3.6 / 3.8 (248k vocab) | byte-level BPE + NFC |
//! | `DeepSeekV3` | `DeepSeek` V3 … V4 | byte-level BPE, 3-stage split chain |
//! | `KimiK2` | Kimi K2 … K3 | byte-level BPE (tiktoken ranks) |
//! | `Glm5` | GLM-5.x (exact), GLM-4.x (near-exact) | byte-level BPE, `ignore_merges` |
//!
//! `o200k_base` and the Claude families are embedded in every build; the
//! remaining vocabularies (~3.4 MB compressed) sit behind the
//! `utok-all-families` cargo feature. A family that is compiled out falls
//! back to `o200k_base`; [`Encoding::compiled`] reports availability.
//!
//! Semantics are `encode_ordinary`: plain content, no special tokens, no
//! chat-template frame. This matches budget-estimation use where fragments
//! are summed.
//!
//! Input is encoding-generic ([`Utf`]): `&str`/`String` (UTF-8), `&[u16]`
//! (UTF-16, e.g. a JS string over napi) and `&[u32]` (UTF-32) tokenize
//! natively in their own code units — no UTF-8 transcode, no scratch
//! buffer. The pre-tokenizer scans a codepoint [`Cursor`], and rank tables
//! lazily expand per-flavor lookup views (`bpe.rs`). Valid text yields
//! flavor-invariant ids/counts; malformed units decode permissively as
//! U+FFFD (matching a lossy JS crossing).

mod bpe;
mod claude;
mod pretoken;
mod scan;
mod tables;
mod utf;
pub use self::{
	bpe::RankTable,
	utf::{Cursor, Unit, Utf},
};

/// Hard upper bound for any embedded zstd blob's decoded size.
///
/// All vocabulary blobs decode to well under 16 MiB; the cap keeps a corrupt
/// or hostile frame from allocating unbounded memory.
const MAX_DECODED_BYTES: usize = 16 * 1024 * 1024;

/// Decode a build-time-embedded zstd blob with the pure-Rust `ruzstd`
/// decoder: no C zstd, no build-script step.
///
/// The blob must be exactly one frame: oversized output, trailing bytes after
/// the frame, and a checksum mismatch all panic, because these blobs are
/// compiled into the binary and corruption is a build error.
pub(crate) fn decode_zstd(zst: &[u8], what: &str) -> Vec<u8> {
	use std::io::Read as _;
	let mut decoder = ruzstd::decoding::StreamingDecoder::new(zst)
		.unwrap_or_else(|e| panic!("utoken: {what} zstd header: {e}"));
	assert!(
		decoder.decoder.content_size() <= MAX_DECODED_BYTES as u64,
		"utoken: {what} decoded size exceeds {MAX_DECODED_BYTES} bytes (declared {})",
		decoder.decoder.content_size()
	);
	let mut raw = Vec::new();
	(&mut decoder)
		.take((MAX_DECODED_BYTES + 1) as u64)
		.read_to_end(&mut raw)
		.unwrap_or_else(|e| panic!("utoken: {what} zstd decode: {e}"));
	assert!(
		raw.len() <= MAX_DECODED_BYTES,
		"utoken: {what} decoded size exceeds {MAX_DECODED_BYTES} bytes"
	);
	let frame = decoder.into_frame_decoder();
	assert_eq!(
		frame.bytes_read_from_source(),
		zst.len() as u64,
		"utoken: {what} has trailing bytes after the zstd frame"
	);
	if let Some(expected) = frame.get_checksum_from_data() {
		let calculated = frame
			.get_calculated_checksum()
			.unwrap_or_else(|| panic!("utoken: {what} zstd checksum could not be calculated"));
		assert_eq!(calculated, expected, "utoken: {what} zstd checksum mismatch");
	}
	raw
}

/// A tokenizer family. Copy-cheap; all state is in lazily-initialized
/// process-wide tables (first use pays one zstd decode of ~0.5–1 MB).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Encoding {
	/// GPT-4o / o1 / GPT-5 (`OpenAI` default).
	O200kBase,
	/// GPT-3.5 / GPT-4 / older `OpenAI`.
	Cl100kBase,
	/// Claude 3 through Opus 4.6 (and every non-opus Claude < 5).
	ClaudeV3,
	/// Claude Opus 4.7–4.9.
	ClaudeV47,
	/// Claude Opus 5+.
	ClaudeV5,
	/// Claude Sonnet/Fable 5+ (non-opus 5-series frame variant).
	ClaudeV5Sonnet,
	/// Qwen 3.5 / 3.6 / 3.8 (248,044-token vocabulary, NFC input).
	Qwen3,
	/// `DeepSeek` V3 / V3.1 / V3.2 / R1 / V4 (identical base BPE).
	DeepSeekV3,
	/// Kimi K2 / K2.5 / K3 (163,584-token base vocabulary).
	KimiK2,
	/// GLM-5 (154,820-token vocabulary; ID-preserving superset of GLM-4.x).
	Glm5,
}

impl Encoding {
	/// Whether this family's vocabulary is embedded in the current build.
	///
	/// `o200k_base` and the Claude families always are; the rest require the
	/// `utok-all-families` feature.
	pub const fn compiled(self) -> bool {
		match self {
			Self::O200kBase
			| Self::ClaudeV3
			| Self::ClaudeV47
			| Self::ClaudeV5
			| Self::ClaudeV5Sonnet => true,
			_ => cfg!(feature = "utok-all-families"),
		}
	}

	/// Exact content token count of `text` (no specials, no message frame),
	/// in any UTF flavor.
	///
	/// A family compiled out of the build approximates with the always-present
	/// `o200k_base` vocabulary (see [`Encoding::compiled`]).
	pub fn count<T: Utf + ?Sized>(self, text: &T) -> u32 {
		match self {
			Self::ClaudeV3 => claude::content_token_count(text.units(), claude::Family::V3),
			Self::ClaudeV47 => claude::content_token_count(text.units(), claude::Family::V47),
			Self::ClaudeV5 => claude::content_token_count(text.units(), claude::Family::V5),
			Self::ClaudeV5Sonnet => {
				claude::content_token_count(text.units(), claude::Family::V5Sonnet)
			},
			_ => {
				let bpe = tables::bpe_for(self).unwrap_or(&tables::O200K_BASE);
				bpe.count(text.units())
			},
		}
	}

	/// Token ids for `text`, in any UTF flavor.
	///
	/// `None` for the Claude families: ctok reconstructs counts, not
	/// boundaries, so no id sequence exists. A family compiled out of the
	/// build falls back to the `o200k_base` ids.
	pub fn encode<T: Utf + ?Sized>(self, text: &T) -> Option<Vec<u32>> {
		match self {
			Self::ClaudeV3 | Self::ClaudeV47 | Self::ClaudeV5 | Self::ClaudeV5Sonnet => None,
			_ => {
				let bpe = tables::bpe_for(self).unwrap_or(&tables::O200K_BASE);
				Some(bpe.encode(text.units()))
			},
		}
	}
}

#[cfg(test)]
#[path = "tests"]
mod tests {
	#[path = "claude.rs"]
	mod claude;
	#[cfg(feature = "utok-all-families")]
	#[path = "deepseek.rs"]
	mod deepseek;
	#[cfg(feature = "utok-all-families")]
	#[path = "glm.rs"]
	mod glm;
	#[cfg(feature = "utok-all-families")]
	#[path = "kimi.rs"]
	mod kimi;
	#[path = "openai.rs"]
	mod openai;
	#[cfg(feature = "utok-all-families")]
	#[path = "qwen.rs"]
	mod qwen;
}

#[cfg(test)]
mod zstd_tests {
	use super::*;

	const TINY: &[u8] = include_bytes!("../../fixtures/zstd_tiny.bin.zst");
	const OVERSIZED: &[u8] = include_bytes!("../../fixtures/zstd_oversized.bin.zst");

	#[test]
	fn decodes_a_single_frame() {
		assert_eq!(decode_zstd(TINY, "tiny"), b"hello hello hello");
	}

	#[test]
	#[should_panic(expected = "zstd")]
	fn rejects_truncated_frame() {
		let _ = decode_zstd(&TINY[..TINY.len() - 1], "truncated");
	}

	#[test]
	#[should_panic(expected = "trailing bytes after the zstd frame")]
	fn rejects_trailing_garbage() {
		let mut corrupt = TINY.to_vec();
		corrupt.extend_from_slice(b"garbage");
		let _ = decode_zstd(&corrupt, "trailing");
	}

	#[test]
	#[should_panic(expected = "decoded size exceeds")]
	fn rejects_declared_oversized_frame() {
		let _ = decode_zstd(OVERSIZED, "oversized");
	}
}
