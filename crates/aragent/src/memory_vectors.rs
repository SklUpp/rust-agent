//! Pure-Rust vector kernels for memory recall and RepoMap ranking.
//!
//! Ported from `pi-natives`'s `vectors.rs` (N-API exports stripped) so recall
//! math needs no native addon. Accumulation order, non-finite handling, and
//! tie-breaking are preserved exactly: missing/non-finite cosine terms count
//! as `0`, zero norms score `0`, top-k ties keep the lower row first, and MMR
//! keeps the earliest remaining candidate on equal or all-`NaN` scores.
//!
//! Invalid shapes (mismatched lengths, a matrix length that is not a positive
//! multiple of the dimensions) are reported, never silently truncated.

#[inline]
const fn finite_or_zero(value: f64) -> f64 {
	if value.is_finite() { value } else { 0.0 }
}

/// Cosine similarity with the semantics of mnemopi's TS `cosineSimilarity`:
/// iterate `max(len_a, len_b)` elements, treat missing and non-finite entries
/// as `0`, return `0` when either norm is zero.
///
/// The shared prefix is accumulated separately from the tails so tail terms of
/// the shorter side only ever add `±0.0` to `dot` and `+0.0` to its own norm,
/// in the same index order as the TS loop.
#[inline]
#[allow(
	clippy::suboptimal_flops,
	reason = "mul_add rounds differently; bit-exact with the TS loops is the contract"
)]
pub fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
	if a.is_empty() && b.is_empty() {
		return 0.0;
	}
	let shared = a.len().min(b.len());
	let mut dot = 0.0f64;
	let mut norm_a = 0.0f64;
	let mut norm_b = 0.0f64;
	for i in 0..shared {
		let av = finite_or_zero(a[i]);
		let bv = finite_or_zero(b[i]);
		dot += av * bv;
		norm_a += av * av;
		norm_b += bv * bv;
	}
	for &raw in &a[shared..] {
		let av = finite_or_zero(raw);
		norm_a += av * av;
	}
	for &raw in &b[shared..] {
		let bv = finite_or_zero(raw);
		norm_b += bv * bv;
	}
	if norm_a == 0.0 || norm_b == 0.0 {
		return 0.0;
	}
	dot / (norm_a.sqrt() * norm_b.sqrt())
}

/// All pairs `(i, j)` with `i < j` whose cosine similarity meets `threshold`.
///
/// `vectors` is `count` vectors flattened row-major at `dim` `f64` elements per
/// row (zero padding reproduces the TS `?? 0` missing-element semantics).
/// Pairs are returned flattened as `[i0, j0, i1, j1, ...]` in the same
/// `(i, j)` visit order as the TS nested loop.
pub fn cosine_similarity_pairs(
	vectors: &[f64],
	count: usize,
	dim: usize,
	threshold: f64,
) -> Result<Vec<u32>, &'static str> {
	if vectors.len() != count * dim {
		return Err("vectors length must equal count * dim");
	}
	let mut pairs: Vec<u32> = Vec::new();
	for i in 0..count {
		let left = &vectors[i * dim..(i + 1) * dim];
		for j in (i + 1)..count {
			let right = &vectors[j * dim..(j + 1) * dim];
			if cosine_similarity(left, right) >= threshold {
				pairs.push(i as u32);
				pairs.push(j as u32);
			}
		}
	}
	Ok(pairs)
}

/// Top-k rows of a normalized vector matrix ranked by dot product with a
/// normalized query.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorTopK {
	/// Row indices of the selected hits, best score first.
	pub indices: Vec<u32>,
	/// Scores aligned with `indices`.
	pub scores:  Vec<f64>,
}

/// Score every row of a normalized `f32` matrix against `query` and return the
/// top `limit` rows.
///
/// Mirrors the TS `searchExactVectorIndex` loop exactly: the query is
/// normalized by the L2 norm of its *full* length, each row score sums
/// `matrix[row][col] * (query[col] / norm)` over
/// `min(query.len, dimensions)` columns in column order. Ranking matches the
/// TS stable sort: score descending, lower row index first on exact ties
/// (`-0.0` and `+0.0` compare equal). Callers are expected to enforce the TS
/// guards first (finite query with a positive norm, non-empty matrix).
#[allow(
	clippy::suboptimal_flops,
	reason = "mul_add rounds differently; bit-exact with the TS loops is the contract"
)]
pub fn vector_index_top_k(
	matrix: &[f32],
	dimensions: usize,
	query: &[f64],
	limit: usize,
) -> Result<VectorTopK, &'static str> {
	if dimensions == 0 || !matrix.len().is_multiple_of(dimensions) {
		return Err("matrix length must be a positive multiple of dimensions");
	}
	let count = matrix.len() / dimensions;
	let mut norm_sq = 0.0f64;
	for &value in query {
		norm_sq += value * value;
	}
	let norm = norm_sq.sqrt();
	// Hoisting the per-column division out of the row loop is bitwise
	// identical to the TS per-row `query[col] / queryNorm`.
	let query_dims = query.len().min(dimensions);
	let normalized: Vec<f64> = query[..query_dims].iter().map(|&v| v / norm).collect();

	let mut order: Vec<(f64, u32)> = Vec::with_capacity(count);
	for row in 0..count {
		let base = row * dimensions;
		let mut score = 0.0f64;
		for (col, &qv) in normalized.iter().enumerate() {
			score += f64::from(matrix[base + col]) * qv;
		}
		order.push((score, row as u32));
	}
	// JS comparator `(a, b) => b.score - a.score` under a stable sort: strict
	// score ordering, otherwise (equal, including ±0.0) original row order.
	order.sort_by(|a, b| {
		let diff = b.0 - a.0;
		if diff > 0.0 {
			core::cmp::Ordering::Greater
		} else if diff < 0.0 {
			core::cmp::Ordering::Less
		} else {
			a.1.cmp(&b.1)
		}
	});
	let take = limit.min(order.len());
	order.truncate(take);
	let indices: Vec<u32> = order.iter().map(|&(_, row)| row).collect();
	let scores: Vec<f64> = order.iter().map(|&(score, _)| score).collect();
	Ok(VectorTopK { indices, scores })
}

/// ECMA-262 `\s` (`WhiteSpace` ∪ `LineTerminator`), which differs from Rust's
/// `char::is_whitespace` (JS additionally includes U+FEFF).
#[inline]
const fn is_js_whitespace(c: char) -> bool {
	matches!(
		c,
		'\u{0009}'
			| '\u{000a}'
			| '\u{000b}'
			| '\u{000c}'
			| '\u{000d}'
			| '\u{0020}'
			| '\u{00a0}'
			| '\u{1680}'
			| '\u{2000}'
			..='\u{200a}'
				| '\u{2028}'
				| '\u{2029}'
				| '\u{202f}'
				| '\u{205f}'
				| '\u{3000}'
				| '\u{feff}'
	)
}

/// Lowercased word set per the TS `jaccardSimilarity` tokenizer:
/// `text.toLowerCase().split(/\s+/).filter(Boolean)` into a `Set`.
/// Returned sorted and deduplicated for merge-based intersection counting.
fn word_set(text: &str) -> Vec<Box<str>> {
	let lower = text.to_lowercase();
	let mut words: Vec<Box<str>> = lower
		.split(is_js_whitespace)
		.filter(|w| !w.is_empty())
		.map(Box::from)
		.collect();
	words.sort_unstable();
	words.dedup();
	words
}

/// Jaccard similarity of two sorted, deduplicated word sets. Matches the TS
/// `jaccardSimilarity`: `0` when either set is empty, otherwise
/// `|A ∩ B| / (|A| + |B| - |A ∩ B|)` with exact integer counts.
fn jaccard_sorted(a: &[Box<str>], b: &[Box<str>]) -> f64 {
	if a.is_empty() || b.is_empty() {
		return 0.0;
	}
	let mut intersection = 0usize;
	let (mut i, mut j) = (0usize, 0usize);
	while i < a.len() && j < b.len() {
		match a[i].cmp(&b[j]) {
			core::cmp::Ordering::Less => i += 1,
			core::cmp::Ordering::Greater => j += 1,
			core::cmp::Ordering::Equal => {
				intersection += 1;
				i += 1;
				j += 1;
			},
		}
	}
	intersection as f64 / (a.len() + b.len() - intersection) as f64
}

/// MMR selection over pre-sorted candidates using Jaccard word similarity.
///
/// `contents[i]` and `scores[i]` describe candidate `i`, already sorted by
/// relevance exactly as the TS `mmrRerank` sorts them (the JS stable sort stays
/// with the caller so its tie and `NaN` semantics are preserved). Candidate
/// `0` is always taken first; each round picks the remaining candidate
/// maximizing `lambda * score - (1 - lambda) * maxSimilarity(selected)` with
/// strict `>` comparisons, so ties keep the earliest remaining candidate — and
/// a round where every score is `NaN` picks the first remaining candidate,
/// matching the TS `bestIdx = 0` initialisation. Returns the selected indices
/// into the input order.
///
/// Word tokenization matches `text.toLowerCase().split(/\s+/)` (ECMA `\s`,
/// Unicode default full case conversion). Known divergence: unpaired
/// surrogates arrive here as U+FFFD, while JS keeps the lone surrogate; both
/// tokenize to a single non-whitespace word so Jaccard counts still agree
/// unless a text mixes U+FFFD words with lone-surrogate words.
#[allow(
	clippy::suboptimal_flops,
	reason = "mul_add rounds differently; bit-exact with the TS loops is the contract"
)]
pub fn mmr_rerank_indices(
	contents: &[impl AsRef<str>],
	scores: &[f64],
	lambda_param: f64,
	top_k: usize,
) -> Result<Vec<u32>, &'static str> {
	if scores.len() != contents.len() {
		return Err("scores length must equal contents length");
	}
	let count = contents.len();
	if top_k == 0 || count == 0 {
		return Ok(Vec::new());
	}
	let sets: Vec<Vec<Box<str>>> = contents.iter().map(|c| word_set(c.as_ref())).collect();
	let mut selected: Vec<u32> = Vec::with_capacity(top_k.min(count));
	selected.push(0);
	let mut remaining: Vec<u32> = (1..count as u32).collect();

	while !remaining.is_empty() && selected.len() < top_k {
		let mut best_idx = 0usize;
		let mut best_score = f64::NEG_INFINITY;
		for (idx, &candidate) in remaining.iter().enumerate() {
			let mut max_similarity = 0.0f64;
			for &picked in &selected {
				let similarity = jaccard_sorted(&sets[candidate as usize], &sets[picked as usize]);
				if similarity > max_similarity {
					max_similarity = similarity;
				}
			}
			let relevance = scores[candidate as usize];
			let mmr_score = lambda_param * relevance - (1.0 - lambda_param) * max_similarity;
			if mmr_score > best_score {
				best_score = mmr_score;
				best_idx = idx;
			}
		}
		selected.push(remaining.remove(best_idx));
	}
	if selected.len() < top_k {
		selected.extend(remaining);
		selected.truncate(top_k);
	}
	Ok(selected)
}

#[cfg(test)]
mod tests {
	use super::{
		VectorTopK, cosine_similarity, cosine_similarity_pairs, is_js_whitespace, jaccard_sorted,
		mmr_rerank_indices, vector_index_top_k, word_set,
	};

	fn approx(actual: f64, expected: f64) -> bool {
		(actual - expected).abs() < 1e-12
	}

	#[test]
	fn cosine_matches_reference_semantics() {
		assert_eq!(cosine_similarity(&[], &[]), 0.0);
		assert_eq!(cosine_similarity(&[1.0, 0.0], &[0.0, 0.0]), 0.0);
		let same = cosine_similarity(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]);
		assert!(approx(same, 1.0));
		// Non-finite entries are zeroed, mismatched lengths pad with zero.
		let sim = cosine_similarity(&[f64::NAN, 1.0], &[0.5, 1.0, 2.0]);
		let expect = 1.0 / (1.0f64.sqrt() * (0.25f64 + 1.0 + 4.0).sqrt());
		assert!(approx(sim, expect));
		assert_eq!(cosine_similarity(&[f64::INFINITY], &[1.0]), 0.0);
	}

	#[test]
	fn cosine_pairs_visit_order_and_threshold() {
		// Three unit rows 90° apart except rows 0 and 2, which are equal.
		let vectors = [1.0, 0.0, 0.0, 1.0, 1.0, 0.0];
		let pairs = cosine_similarity_pairs(&vectors, 3, 2, 0.5).unwrap();
		assert_eq!(pairs, vec![0, 2]);
		let all = cosine_similarity_pairs(&vectors, 3, 2, -1.0).unwrap();
		assert_eq!(all, vec![0, 1, 0, 2, 1, 2]);
		assert!(cosine_similarity_pairs(&vectors, 2, 4, 0.0).is_err());
		// Zero vectors score 0, so a 0 threshold includes them.
		assert_eq!(cosine_similarity_pairs(&[0.0, 0.0], 2, 1, 0.0).unwrap(), vec![0, 1]);
	}

	#[test]
	fn top_k_scores_and_breaks_ties_by_row() {
		let result = vector_index_top_k(&[1.0, 0.0, 0.5, 0.5, 0.5, 0.5], 2, &[1.0, 0.0], 3)
			.expect("valid matrix");
		assert_eq!(result.indices, vec![0, 1, 2]);
		assert!(approx(result.scores[0], 1.0));
		assert!(approx(result.scores[1], 0.5));
		assert!(approx(result.scores[2], 0.5));
		let ties = vector_index_top_k(&[1.0, 1.0, 1.0, 1.0], 1, &[1.0], 2).expect("valid matrix");
		assert_eq!(ties, VectorTopK { indices: vec![0, 1], scores: vec![1.0, 1.0] });
	}

	#[test]
	fn top_k_validates_shape_and_respects_limit() {
		assert!(vector_index_top_k(&[1.0, 2.0], 0, &[1.0], 1).is_err());
		assert!(vector_index_top_k(&[1.0, 2.0], 3, &[1.0], 1).is_err());
		let empty = vector_index_top_k(&[1.0, 2.0], 2, &[1.0, 0.0], 0).unwrap();
		assert_eq!(empty, VectorTopK { indices: vec![], scores: vec![] });
		let beyond = vector_index_top_k(&[1.0], 1, &[1.0], 9).unwrap();
		assert_eq!(beyond.indices, vec![0]);
	}

	#[test]
	fn word_set_matches_js_tokenizer() {
		let set = word_set("Hello\u{00a0}WORLD hello\u{feff}world");
		assert_eq!(set, vec![Box::from("hello"), Box::from("world")]);
		assert!(word_set("").is_empty());
		assert!(word_set(" \t\n").is_empty());
		assert!(!is_js_whitespace('\u{200b}')); // ZWSP is not JS \s
	}

	#[test]
	fn jaccard_matches_reference() {
		let a = word_set("the quick brown fox");
		let b = word_set("the lazy brown dog");
		let sim = jaccard_sorted(&a, &b);
		assert!(approx(sim, 2.0 / 6.0));
		assert_eq!(jaccard_sorted(&a, &word_set("")), 0.0);
	}

	#[test]
	fn mmr_takes_first_then_penalises_similar_text() {
		let contents = ["alpha beta", "alpha beta gamma", "delta epsilon"];
		let scores = [1.0, 0.9, 0.8];
		let picked = mmr_rerank_indices(&contents, &scores, 0.5, 3).unwrap();
		// Candidate 0 always leads; candidate 2 is dissimilar, so it beats the
		// near-duplicate 1 despite the lower relevance.
		assert_eq!(picked, vec![0, 2, 1]);
	}

	#[test]
	fn mmr_ties_keep_earliest_and_nan_picks_first() {
		let contents = ["same", "same", "same"];
		assert_eq!(mmr_rerank_indices(&contents, &[0.5, 0.5, 0.5], 0.5, 3).unwrap(), vec![0, 1, 2]);
		let nan_scores = [f64::NAN, f64::NAN, f64::NAN];
		assert_eq!(mmr_rerank_indices(&contents, &nan_scores, 0.5, 2).unwrap(), vec![0, 1]);
	}

	#[test]
	fn mmr_edge_cases() {
		let empty: &[&str] = &[];
		assert_eq!(mmr_rerank_indices(empty, &[], 0.5, 3).unwrap(), Vec::<u32>::new());
		assert_eq!(mmr_rerank_indices(&["a"], &[1.0], 0.5, 0).unwrap(), Vec::<u32>::new());
		assert!(mmr_rerank_indices(&["a"], &[1.0, 2.0], 0.5, 1).is_err());
		// top_k beyond the candidate count returns every candidate.
		assert_eq!(mmr_rerank_indices(&["a", "b"], &[1.0, 0.0], 0.5, 5).unwrap(), vec![0, 1]);
		// lambda 0 ignores relevance: pure diversity.
		assert_eq!(
			mmr_rerank_indices(&["a", "a b", "c"], &[1.0, 0.9, 0.1], 0.0, 3).unwrap(),
			vec![0, 2, 1]
		);
	}
}
