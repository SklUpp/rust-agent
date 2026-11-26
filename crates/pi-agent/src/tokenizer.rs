use crate::state::Message;
pub use crate::utok;

/// Token counting: exact embedded-tokenizer counts per model family, plus
/// per-message envelope overhead.
pub struct Tokenizer;

/// Whether `haystack` contains any of `needles`.
fn contains_any(haystack: &str, needles: &[&str]) -> bool {
	needles.iter().any(|n| haystack.contains(n))
}

impl Tokenizer {
	/// Exact `o200k_base` token count, the always-embedded default family.
	#[must_use]
	pub fn count_text(text: &str) -> usize {
		utok::Encoding::O200kBase.count(text) as usize
	}

	/// Exact token count for a known model family.
	///
	/// Families not compiled into this build (non-default ones when
	/// `utok-all-families` is off) and unknown models fall back to the
	/// always-embedded `o200k_base` vocabulary.
	#[must_use]
	pub fn count_text_for_model(model: &str, text: &str) -> usize {
		let enc = Self::encoding_for_model(model)
			.filter(|enc| enc.compiled())
			.unwrap_or(utok::Encoding::O200kBase);
		enc.count(text) as usize
	}

	/// Map a model id to its utok family.
	fn encoding_for_model(model: &str) -> Option<utok::Encoding> {
		let m = model.to_ascii_lowercase();
		if m.contains("claude") {
			let enc = if m.contains("opus") {
				if contains_any(&m, &["opus-5", "opus5"]) {
					utok::Encoding::ClaudeV5
				} else if contains_any(
					&m,
					&["opus-4-7", "opus-4.7", "opus-4-8", "opus-4.8", "opus-4-9", "opus-4.9"],
				) {
					utok::Encoding::ClaudeV47
				} else {
					utok::Encoding::ClaudeV3
				}
			} else if contains_any(&m, &["sonnet-5", "sonnet5", "fable-5", "fable5"]) {
				utok::Encoding::ClaudeV5Sonnet
			} else {
				utok::Encoding::ClaudeV3
			};
			return Some(enc);
		}
		if m.contains("qwen") {
			return Some(utok::Encoding::Qwen3);
		}
		if m.contains("deepseek") {
			return Some(utok::Encoding::DeepSeekV3);
		}
		if m.contains("kimi") {
			return Some(utok::Encoding::KimiK2);
		}
		if m.contains("glm") {
			return Some(utok::Encoding::Glm5);
		}
		if m.contains("gpt-3.5")
			|| (m.contains("gpt-4") && !m.contains("4o") && !m.contains("4.1") && !m.contains("4-1"))
		{
			return Some(utok::Encoding::Cl100kBase);
		}
		if m.contains("gpt") || contains_any(&m, &["o1", "o3", "o4", "o5"]) {
			return Some(utok::Encoding::O200kBase);
		}
		None
	}

	/// Token count for a single message: exact content tokens plus envelope
	/// framing.
	#[must_use]
	pub fn count_message(msg: &Message) -> usize {
		Self::count_message_with(msg, |text| Self::count_text(text))
	}

	/// [`Self::count_message`] with the message content and metadata counted
	/// in `model`'s family.
	#[must_use]
	pub fn count_message_for_model(model: &str, msg: &Message) -> usize {
		Self::count_message_with(msg, |text| Self::count_text_for_model(model, text))
	}

	fn count_message_with(msg: &Message, count: impl Fn(&str) -> usize) -> usize {
		let mut tokens = 4; // Envelope overhead (<|im_start|>role\n ... <|im_end|>)
		tokens += count(&msg.content);

		if let Some(name) = &msg.name {
			tokens += 1 + count(name);
		}

		if let Some(id) = &msg.tool_call_id {
			tokens += 2 + count(id);
		}

		for call in &msg.tool_calls {
			tokens += 6; // Tool call overhead
			tokens += count(&call.id);
			tokens += count(&call.name);
			tokens += count(&call.arguments.to_string());
		}

		tokens
	}

	/// Total token count across messages, including per-message framing.
	#[must_use]
	pub fn count_messages(messages: &[Message]) -> usize {
		Self::count_messages_with(messages, |msg| Self::count_message(msg))
	}

	/// [`Self::count_messages`] with every message counted in `model`'s
	/// family.
	#[must_use]
	pub fn count_messages_for_model(model: &str, messages: &[Message]) -> usize {
		Self::count_messages_with(messages, |msg| Self::count_message_for_model(model, msg))
	}

	fn count_messages_with(messages: &[Message], count: impl Fn(&Message) -> usize) -> usize {
		if messages.is_empty() {
			return 0;
		}
		let mut sum = 3; // Prime conversation tokens
		for msg in messages {
			sum += count(msg);
		}
		sum
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn model_aware_message_counting_uses_the_model_family() {
		let samples = [
			"hello world",
			"fn main() { println!(\"hello, world\"); }",
			"The quick brown fox jumps over the lazy dog; pack my box with five dozen liquor jugs.",
		];
		assert!(
			samples
				.iter()
				.any(|text| Tokenizer::count_text_for_model("claude-opus-4-7", text)
					!= Tokenizer::count_text_for_model("gpt-4o", text)),
			"expected Claude and GPT families to disagree on at least one sample"
		);

		let messages = vec![Message::user(samples[2]), Message::assistant(samples[1])];
		let claude = Tokenizer::count_messages_for_model("claude-opus-4-7", &messages);
		let gpt = Tokenizer::count_messages_for_model("gpt-4o", &messages);
		assert_ne!(claude, gpt);
	}

	#[test]
	fn test_token_counting() {
		assert_eq!(Tokenizer::count_text(""), 0);
		assert!(Tokenizer::count_text("hello world") >= 2);

		let msg = Message::user("test prompt");
		assert!(Tokenizer::count_message(&msg) > 4);

		let msgs = vec![Message::system("sys"), Message::user("usr")];
		assert!(Tokenizer::count_messages(&msgs) > 10);
	}

	#[test]
	fn model_aware_counting_falls_back_cleanly() {
		assert!(Tokenizer::count_text_for_model("gpt-4o", "hello world") > 0);
		assert!(Tokenizer::count_text_for_model("claude-opus-4-7", "hello world") > 0);
		assert_eq!(
			Tokenizer::count_text_for_model("no-such-model", "hello world"),
			Tokenizer::count_text("hello world")
		);

		#[cfg(feature = "utok-all-families")]
		assert_eq!(
			Tokenizer::count_text_for_model("qwen3-max", "hello"),
			crate::utok::Encoding::Qwen3.count("hello") as usize
		);
		#[cfg(not(feature = "utok-all-families"))]
		assert_eq!(
			Tokenizer::count_text_for_model("qwen3-max", "hello"),
			crate::utok::Encoding::O200kBase.count("hello") as usize
		);
	}
}
