//! `OpenAI` family tests: golden fixtures from Python tiktoken.
//!
//! The upstream crate's tiktoken-rs differential test is not ported: this
//! workspace does not depend on tiktoken-rs, and the fixtures already pin
//! ids and counts for both OpenAI vocabularies.

use serde::Deserialize;

use crate::utok::Encoding;

#[derive(Deserialize)]
struct Fixture {
	#[allow(dead_code, reason = "fixture provenance is deserialized but not asserted")]
	generator: String,
	cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
	text: String,
	ids: Vec<u32>,
	count: u32,
}

fn check_fixture(enc: Encoding, json: &str) {
	let fx: Fixture = serde_json::from_str(json).unwrap();
	assert!(!fx.cases.is_empty());
	for (i, case) in fx.cases.iter().enumerate() {
		let ids = enc.encode(&case.text).expect("BPE family must encode");
		assert_eq!(
			ids,
			case.ids,
			"{enc:?} case {i} ids mismatch: {:?}…",
			&case.text[..case.text.len().min(60)]
		);
		assert_eq!(
			enc.count(&case.text),
			case.count,
			"{enc:?} case {i} count mismatch: {:?}…",
			&case.text[..case.text.len().min(60)]
		);
	}
}

#[test]
fn o200k_fixtures() {
	check_fixture(Encoding::O200kBase, include_str!("../../../fixtures/o200k_base.json"));
}

#[cfg(feature = "utok-all-families")]
#[test]
fn cl100k_fixtures() {
	check_fixture(Encoding::Cl100kBase, include_str!("../../../fixtures/cl100k_base.json"));
}
