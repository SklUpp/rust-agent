//! Core byte-pair encoding engine over rank tables (tiktoken algorithm).
//!
//! A [`RankTable`] maps token byte sequences to ranks; merge priority is
//! rank order, so no merges list exists. Tables parse from the UTOK1
//! container (see `data/families.json` for the format) after zstd
//! decompression in [`tables`](crate::utok::tables).
//!
//! Input is encoding-generic: [`BpeEncoding::count`]/[`encode`]
//! (`BpeEncoding::encode`) take `&[U: Unit]`. Pre-tokenization scans the
//! units natively; each piece is then UTF-8-encoded into a reused buffer
//! for the byte-keyed rank table (`str` input skips that copy entirely,
//! non-UTF-8 flavors narrow ASCII runs 1:1). Steady state performs no
//! per-call allocation beyond one scratch buffer for non-UTF-8 flavors.
//!
//! Per-flavor native rank-table views (`HashMap<Box<[u16]>, u32>` etc.)
//! were considered and measured out: with the ASCII narrow path, u16
//! input already runs at 81-97% of the str path per codepoint (M4 Max,
//! english/CJK), so a second table per flavor (2x memory, plus a
//! ragged-token eligibility rule for tokens that split codepoints) buys
//! almost nothing. Revisit only with profile evidence.

use std::{
	borrow::Cow,
	cmp::Reverse,
	collections::{BinaryHeap, HashMap},
	hash::{BuildHasherDefault, Hasher},
};

use crate::utok::{
	pretoken::{self, Splitter},
	utf::Unit,
};

/// Firefox/rustc Fx hash: multiplicative word-at-a-time mixing. Rank
/// lookups hash short byte keys on every merge step; `SipHash` is the
/// dominant cost there (~30% end-to-end at the default hasher).
#[derive(Default)]
struct FxHasher(u64);

impl Hasher for FxHasher {
	#[inline]
	fn write(&mut self, bytes: &[u8]) {
		const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;
		let mut h = self.0;
		let mut b = bytes;
		while let Some(chunk) = b.first_chunk::<8>() {
			h = (h.rotate_left(5) ^ u64::from_le_bytes(*chunk)).wrapping_mul(SEED);
			b = &b[8..];
		}
		if let Some(chunk) = b.first_chunk::<4>() {
			h = (h.rotate_left(5) ^ u64::from(u32::from_le_bytes(*chunk))).wrapping_mul(SEED);
			b = &b[4..];
		}
		for &byte in b {
			h = (h.rotate_left(5) ^ u64::from(byte)).wrapping_mul(SEED);
		}
		self.0 = h;
	}

	#[inline]
	fn finish(&self) -> u64 {
		self.0
	}
}

type Fx = BuildHasherDefault<FxHasher>;
type FxMap = HashMap<Box<[u8]>, u32, Fx>;

/// Pack a key of ≤15 bytes losslessly into a `u128`: bytes little-endian
/// at bits 0..len*8, zero padding, length tag at bits 120..128 (a
/// 15-byte key leaves the top byte free, so equal packs imply equal keys
/// even across lengths and with NUL bytes). Built from two overlapping
/// unaligned reads — a variable-length memcpy here benched slower than
/// hashing the raw bytes; the overlap region ORs identical bits.
#[inline]
fn pack(key: &[u8]) -> Option<u128> {
	let n = key.len();
	if n > 15 {
		return None;
	}
	let v: u128 = if let (Some(lo), Some(hi)) = (key.first_chunk::<8>(), key.last_chunk::<8>()) {
		u128::from(u64::from_le_bytes(*lo)) | u128::from(u64::from_le_bytes(*hi)) << ((n - 8) * 8)
	} else if let (Some(lo), Some(hi)) = (key.first_chunk::<4>(), key.last_chunk::<4>()) {
		u128::from(u32::from_le_bytes(*lo)) | u128::from(u32::from_le_bytes(*hi)) << ((n - 4) * 8)
	} else if let (Some(lo), Some(hi)) = (key.first_chunk::<2>(), key.last_chunk::<2>()) {
		u128::from(u16::from_le_bytes(*lo)) | u128::from(u16::from_le_bytes(*hi)) << ((n - 2) * 8)
	} else if let [b] = key {
		u128::from(*b)
	} else {
		0
	};
	Some(v | (n as u128) << 120)
}

/// Token bytes → rank map decoded from a UTOK1 blob.
///
/// Split by key length into three stores, matching the merge loop's
/// query mix (measured on o200k, M4 Max: +18% english / +34% code /
/// +14% cjk end-to-end vs a single `FxMap<Box<[u8]>, u32>`):
///
/// - 2 bytes — direct-indexed table: the merge seed loop queries every adjacent
///   byte pair, so over half of all lookups land here as one array load.
/// - other ≤15 bytes — [`pack`]ed `u128` keys in an Fx map: KV inline in the
///   table, no `Box` pointer chase, no byte-wise compare.
/// - >15 bytes — plain byte-keyed Fx map (~3% of vocab; spans this long are
///   > almost always misses).
pub struct RankTable {
	/// Rank of 2-byte token `[a, b]` at `a << 8 | b`; `u32::MAX` where
	/// absent (ranks are vocab indices, far below the sentinel).
	pairs: Box<[u32; 65536]>,
	/// Tokens of 1 or 3..=15 bytes, keyed by [`pack`].
	short: HashMap<u128, u32, Fx>,
	/// Tokens longer than 15 bytes.
	long: FxMap,
	/// Longest token in bytes; callers may use it to bound scans.
	pub max_token_len: usize,
}

impl RankTable {
	/// Parse a zstd-compressed UTOK1 blob. Panics on malformed data — the
	/// blobs are compile-time embedded, so corruption is a build error.
	///
	/// Zero-length entries are *skipped*: packers emit merge-unreachable
	/// ("dead") vocab slots as empty strings to keep rank contiguity, and
	/// those ranks must never be produced.
	pub(crate) fn parse(zst: &[u8]) -> Self {
		/// Each entry starts with at least a one-byte length varint, so a
		/// header cannot claim more entries than bytes remain.
		const MIN_ENTRY_BYTES: usize = 1;
		/// Far above every real BPE vocabulary (~200k); rejects corrupt
		/// headers before `HashMap::with_capacity` can allocate unboundedly.
		const MAX_ENTRIES: usize = 512_000;

		let raw = super::decode_zstd(zst, "UTOK1");
		let mut p = &raw[..];
		assert_eq!(&p[..6], b"UTOK1\n", "utoken: bad magic");
		p = &p[6..];
		let n = u32::from_le_bytes(p[..4].try_into().unwrap()) as usize;
		p = &p[4..];
		assert!(n <= MAX_ENTRIES, "utoken: UTOK1 entry count {n} exceeds {MAX_ENTRIES}");
		assert!(
			n <= p.len() / MIN_ENTRY_BYTES,
			"utoken: UTOK1 entry count {n} exceeds remaining {} bytes",
			p.len()
		);
		let mut pairs: Box<[u32; 65536]> =
			vec![u32::MAX; 65536].into_boxed_slice().try_into().unwrap();
		let mut short = HashMap::with_capacity_and_hasher(n, Fx::default());
		let mut long = FxMap::default();
		let mut max_token_len = 0usize;
		for rank in 0..n as u32 {
			let mut len = 0usize;
			let mut shift = 0;
			loop {
				let b = p[0];
				p = &p[1..];
				len |= ((b & 0x7f) as usize) << shift;
				if b < 0x80 {
					break;
				}
				shift += 7;
			}
			if len > 0 {
				let key = &p[..len];
				if let [a, b] = key {
					pairs[usize::from(*a) << 8 | usize::from(*b)] = rank;
				} else if let Some(k) = pack(key) {
					short.insert(k, rank);
				} else {
					long.insert(key.into(), rank);
				}
				max_token_len = max_token_len.max(len);
				p = &p[len..];
			}
		}
		assert!(p.is_empty(), "utoken: trailing bytes in UTOK1 blob");
		Self { pairs, short, long, max_token_len }
	}

	/// Rank of an exact token byte sequence, if present.
	#[inline]
	pub fn rank(&self, piece: &[u8]) -> Option<u32> {
		if let [a, b] = piece {
			let r = self.pairs[usize::from(*a) << 8 | usize::from(*b)];
			return (r != u32::MAX).then_some(r);
		}
		match pack(piece) {
			Some(k) => self.short.get(&k).copied(),
			None => self.long.get(piece).copied(),
		}
	}

	/// Append the BPE token ids of one pre-tokenized piece to `out`.
	pub fn encode_piece(&self, piece: &[u8], out: &mut Vec<u32>) {
		if piece.is_empty() {
			return;
		}
		if let Some(rank) = self.rank(piece) {
			out.push(rank);
			return;
		}
		self.merge(piece, |start, end| {
			out.push(
				self
					.rank(&piece[start..end])
					.expect("utoken: unreachable merge state"),
			);
		});
	}

	/// Token count of one pre-tokenized piece without materializing ids.
	pub fn count_piece(&self, piece: &[u8]) -> u32 {
		if piece.is_empty() {
			return 0;
		}
		if self.rank(piece).is_some() {
			return 1;
		}
		let mut n = 0u32;
		self.merge(piece, |_, _| n += 1);
		n
	}

	/// tiktoken's `byte_pair_merge`: repeatedly merge the adjacent pair with
	/// the lowest rank, then emit each final span via `emit(start, end)`.
	///
	/// Indexed doubly-linked list over the piece's bytes plus a lazy
	/// `BinaryHeap` keyed `(rank, left_index)`: each merge invalidates at most
	/// three candidate pairs, so the whole pass is `O(N log N)` with the same
	/// lowest-rank/lowest-position tie-break as tiktoken's linear scan.
	fn merge(&self, piece: &[u8], mut emit: impl FnMut(usize, usize)) {
		const NONE: usize = usize::MAX;
		let n = piece.len();
		if n <= 1 {
			if n == 1 {
				emit(0, 1);
			}
			return;
		}
		let mut prev: Vec<usize> = (0..n)
			.map(|index| if index == 0 { NONE } else { index - 1 })
			.collect();
		let mut next: Vec<usize> = (0..n)
			.map(|index| if index + 1 == n { NONE } else { index + 1 })
			.collect();
		let mut start: Vec<usize> = (0..n).collect();
		let mut rank = vec![u32::MAX; n];
		let mut alive = vec![true; n];
		let mut heap: BinaryHeap<Reverse<(u32, usize)>> = BinaryHeap::with_capacity(n);
		for index in 0..n - 1 {
			let r = self.rank(&piece[index..index + 2]).unwrap_or(u32::MAX);
			rank[index] = r;
			if r != u32::MAX {
				heap.push(Reverse((r, index)));
			}
		}
		while let Some(Reverse((r, left))) = heap.pop() {
			// Stale entries are skipped: the node may be gone, lose its pair,
			// or have been re-ranked after a neighbouring merge.
			if !alive[left] || next[left] == NONE || rank[left] != r {
				continue;
			}
			let right = next[left];
			alive[right] = false;
			let after = next[right];
			next[left] = after;
			if after != NONE {
				prev[after] = left;
			}
			// `left`'s span now ends at `left_end`; its pair with `after`
			// spans through that node's span as well.
			let left_end = if after == NONE { n } else { start[after] };
			rank[left] = if after == NONE {
				u32::MAX
			} else {
				let pair_end = next[after];
				let pair_end = if pair_end == NONE { n } else { start[pair_end] };
				self.rank(&piece[start[left]..pair_end]).unwrap_or(u32::MAX)
			};
			if rank[left] != u32::MAX {
				heap.push(Reverse((rank[left], left)));
			}
			let before = prev[left];
			if before != NONE {
				rank[before] = self
					.rank(&piece[start[before]..left_end])
					.unwrap_or(u32::MAX);
				if rank[before] != u32::MAX {
					heap.push(Reverse((rank[before], before)));
				}
			}
		}
		let mut node = 0;
		while next[node] != NONE {
			let after = next[node];
			emit(start[node], start[after]);
			node = after;
		}
		emit(start[node], n);
	}
}

/// A full BPE tokenizer: piece splitter + rank table + family flags.
pub struct BpeEncoding {
	pub table: RankTable,
	pub splitter: Splitter,
	/// Apply Unicode NFC to input before splitting (Qwen3).
	pub nfc: bool,
	/// HF `ignore_merges`: whole-piece vocab hit bypasses the merge loop
	/// (GLM-5). The engine already short-circuits whole-piece hits, which
	/// is proven equivalent for GLM-5 (see GLM tests); flag kept for
	/// documentation and any future divergence.
	#[allow(dead_code, reason = "retained to document the GLM-5 tokenizer behavior")]
	pub ignore_merges: bool,
}

impl BpeEncoding {
	pub fn count<U: Unit>(&self, units: &[U]) -> u32 {
		let mut n = 0u32;
		self.run(units, &mut |t, p| n += t.count_piece(p));
		n
	}

	pub fn encode<U: Unit>(&self, units: &[U]) -> Vec<u32> {
		let mut out = Vec::new();
		self.run(units, &mut |t, p| t.encode_piece(p, &mut out));
		out
	}

	/// Normalize/transcode as required, split, and feed each piece's
	/// UTF-8 bytes to `f` alongside the rank table.
	fn run<U: Unit>(&self, units: &[U], f: &mut impl FnMut(&RankTable, &[u8])) {
		if let Some(bytes) = U::as_utf8(units) {
			// UTF-8 flavor: valid by construction (`str`/`String` input).
			if self.nfc
				&& let Ok(text) = std::str::from_utf8(bytes)
				&& let Cow::Owned(norm) = pretoken::nfc(text)
			{
				return self.scan(norm.as_bytes(), f);
			}
			return self.scan(bytes, f);
		}
		// Non-UTF-8 flavors: owned UTF-8 needed only when NFC actually has
		// work to do, or while the test-only regex oracle is active.
		#[cfg(test)]
		let regex_splitter = self.splitter.is_regex();
		#[cfg(not(test))]
		let regex_splitter = false;
		if (self.nfc && !nfc_quick(units)) || regex_splitter {
			let s = decode_lossy(units);
			let s = match pretoken::nfc(&s) {
				Cow::Owned(o) if self.nfc => o,
				_ => s,
			};
			return self.scan(s.as_bytes(), f);
		}
		self.scan(units, f);
	}

	fn scan<U: Unit>(&self, units: &[U], f: &mut impl FnMut(&RankTable, &[u8])) {
		let mut buf = Vec::new();
		self
			.splitter
			.for_each_piece(units, |piece| f(&self.table, piece_bytes(piece, &mut buf)));
	}
}

/// UTF-8 bytes of one piece: identity for `u8`, otherwise re-encoded into
/// `buf` (reused across pieces — one allocation per call, amortized nil).
fn piece_bytes<'a, U: Unit>(piece: &'a [U], buf: &'a mut Vec<u8>) -> &'a [u8] {
	if let Some(bytes) = U::as_utf8(piece) {
		return bytes;
	}
	buf.clear();
	buf.reserve(piece.len());
	let mut i = 0;
	while i < piece.len() {
		// ASCII runs narrow 1:1 without the decode/encode round-trip
		// (dominant for code/English u16 input, cf. xutf's ASCII kernels;
		// the trivial loop autovectorizes).
		if let Some(b) = piece[i].ascii() {
			buf.push(b);
			i += 1;
		} else {
			let (c, n) = U::decode(piece, i);
			i += n;
			let mut tmp = [0u8; 4];
			buf.extend_from_slice(c.encode_utf8(&mut tmp).as_bytes());
		}
	}
	buf
}

/// Permissive whole-input decode (malformed units → U+FFFD).
fn decode_lossy<U: Unit>(units: &[U]) -> String {
	let mut s = String::with_capacity(units.len());
	let mut i = 0;
	while i < units.len() {
		let (c, n) = U::decode(units, i);
		i += n;
		s.push(c);
	}
	s
}

/// NFC quick-check over the decoded codepoint stream, allocation-free
/// (conservative: `false` means "may need normalization").
fn nfc_quick<U: Unit>(units: &[U]) -> bool {
	struct Cps<'a, U: Unit>(&'a [U], usize);
	impl<U: Unit> Iterator for Cps<'_, U> {
		type Item = u32;

		fn next(&mut self) -> Option<u32> {
			(self.1 < self.0.len()).then(|| {
				let (c, n) = U::decode(self.0, self.1);
				self.1 += n;
				c as u32
			})
		}
	}
	xutf::is_nfc_codepoints(Cps(units, 0))
}

#[cfg(test)]
mod merge_tests {
	use std::time::{Duration, Instant};

	use super::*;

	fn table_over(tokens: &[&[u8]]) -> RankTable {
		let mut table = RankTable {
			pairs: vec![u32::MAX; 65536].into_boxed_slice().try_into().unwrap(),
			short: HashMap::default(),
			long: FxMap::default(),
			max_token_len: 0,
		};
		for (rank, token) in tokens.iter().enumerate() {
			let rank = rank as u32;
			table.max_token_len = table.max_token_len.max(token.len());
			if let [a, b] = *token {
				table.pairs[usize::from(a) << 8 | usize::from(b)] = rank;
			} else if let Some(key) = pack(token) {
				table.short.insert(key, rank);
			} else {
				table.long.insert((*token).into(), rank);
			}
		}
		table
	}

	/// Alphabet of `n` bytes, all tokens of length `1..=max_len`, rank = index.
	fn exhaustive_table(alphabet: &[u8], max_len: usize) -> RankTable {
		let mut tokens: Vec<Vec<u8>> = Vec::new();
		for len in 1..=max_len {
			let mut stack = vec![(Vec::new(), 0usize)];
			while let Some((prefix, depth)) = stack.pop() {
				if depth == len {
					tokens.push(prefix);
					continue;
				}
				for &byte in alphabet {
					let mut next = prefix.clone();
					next.push(byte);
					stack.push((next, depth + 1));
				}
			}
		}
		let refs: Vec<&[u8]> = tokens.iter().map(Vec::as_slice).collect();
		table_over(&refs)
	}

	fn pieces(alphabet: &[u8], len: usize) -> Vec<Vec<u8>> {
		let mut pieces = Vec::new();
		let mut stack = vec![(Vec::new(), 0usize)];
		while let Some((prefix, depth)) = stack.pop() {
			if depth == len {
				pieces.push(prefix);
				continue;
			}
			for &byte in alphabet {
				let mut next = prefix.clone();
				next.push(byte);
				stack.push((next, depth + 1));
			}
		}
		pieces
	}

	/// Direct reference implementation of tiktoken's linear algorithm.
	fn naive_ids(table: &RankTable, piece: &[u8]) -> Option<Vec<u32>> {
		if piece.is_empty() {
			return Some(Vec::new());
		}
		if let Some(rank) = table.rank(piece) {
			return Some(vec![rank]);
		}
		let mut bounds: Vec<usize> = (0..=piece.len()).collect();
		loop {
			let mut best: Option<(u32, usize)> = None;
			for k in 0..bounds.len().saturating_sub(2) {
				let rank = table
					.rank(&piece[bounds[k]..bounds[k + 2]])
					.unwrap_or(u32::MAX);
				if rank != u32::MAX
					&& best.is_none_or(|(best_rank, best_k)| {
						rank < best_rank || (rank == best_rank && k < best_k)
					}) {
					best = Some((rank, k));
				}
			}
			let Some((_, k)) = best else { break };
			bounds.remove(k + 1);
		}
		bounds
			.windows(2)
			.map(|window| table.rank(&piece[window[0]..window[1]]))
			.collect()
	}

	#[test]
	#[should_panic(expected = "entry count")]
	fn rejects_corrupt_utok1_entry_count() {
		let _ = RankTable::parse(include_bytes!("../../fixtures/utok_bad_count.bin.zst"));
	}

	#[test]
	fn merge_matches_tiktoken_linear_reference() {
		// Ranks cover every token of length 1..=4, so every length-5 final
		// span is rankable while the whole piece still takes the merge path.
		let table = exhaustive_table(b"ab", 4);
		for piece in pieces(b"ab", 5) {
			let naive = naive_ids(&table, &piece).expect("naive spans are ranked");
			let mut encoded = Vec::new();
			table.encode_piece(&piece, &mut encoded);
			assert_eq!(encoded, naive, "piece {:?}", String::from_utf8_lossy(&piece));
			assert_eq!(table.count_piece(&piece), naive.len() as u32);
		}
	}

	#[test]
	fn merge_handles_long_repetitive_pieces_quickly() {
		let table = table_over(&[&b"a"[..], &b"aa"[..]]);
		let piece = vec![b'a'; 50_000];
		let started = Instant::now();
		let mut spans = 0usize;
		table.merge(&piece, |_, _| spans += 1);
		let elapsed = started.elapsed();
		assert!(spans >= 1);
		assert!(elapsed < Duration::from_secs(5), "merge took {elapsed:?} for a 50 kB piece");
	}
}
