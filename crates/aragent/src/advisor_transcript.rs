//! Append-only advisor transcript (`<session>/__advisor[.<slug>].jsonl`).
//!
//! The advisor is a passive reviewer with its own model usage, so — like a task
//! subagent — its turns are written beside the primary session log for stats
//! attribution and observability. The file is derived from the *session file*
//! and resolved when a message finalizes, so a `/new` or resume in flight never
//! misattributes an advisor turn. The recorder never truncates: every billed
//! turn is appended even when the advisor's in-memory context resets.
//!
//! `ponytail:` the TS recorder queues async open/close/append through
//! `SessionManager`. This build appends synchronously with `OpenOptions` because
//! one advisor worker owns its file and messages are small; move to a writer
//! thread if transcripts ever block the advisor loop.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use pi_agent::{Message, Role};

use crate::catalog::Catalog;
use crate::session::{ContentPart, Cost, MessageContent, MessageEntry, SessionMessage, Usage};

pub const ADVISOR_TRANSCRIPT_STEM: &str = "__advisor";
pub const ADVISOR_TRANSCRIPT_FILENAME: &str = "__advisor.jsonl";

const JSONL_SUFFIX: &str = ".jsonl";

#[must_use]
pub fn advisor_transcript_filename(slug: &str) -> String {
	if slug.is_empty() {
		ADVISOR_TRANSCRIPT_FILENAME.to_string()
	} else {
		format!("{ADVISOR_TRANSCRIPT_STEM}.{slug}{JSONL_SUFFIX}")
	}
}

#[must_use]
pub fn is_advisor_transcript_name(name: &str) -> bool {
	name == ADVISOR_TRANSCRIPT_FILENAME
		|| (name.starts_with(&format!("{ADVISOR_TRANSCRIPT_STEM}.")) && name.ends_with(JSONL_SUFFIX))
}

/// `<session stem dir>/<filename>` for the owning session log: TS keeps
/// advisor transcripts inside `<session>/` (the `.jsonl` becomes a directory),
/// so two sessions never share advisor files or spend.
#[must_use]
pub fn transcript_path(session_file: &Path, slug: &str) -> Option<PathBuf> {
	if !session_file.to_string_lossy().ends_with(JSONL_SUFFIX) {
		return None;
	}
	let directory = session_file.with_extension("");
	Some(directory.join(advisor_transcript_filename(slug)))
}

fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis() as u64
}

/// Add catalog-priced cost to reported usage. Absent rates leave `cost = None`
/// (unknown), never a fabricated zero.
#[must_use]
pub fn priced_usage(
	message: &Message,
	catalog: &Catalog,
	provider: &str,
	model: &str,
) -> Option<Usage> {
	let mut usage = crate::session::reported_usage(message)?;
	let rates = catalog
		.providers
		.get(provider)
		.and_then(|models| models.get(model))
		.and_then(|model| model.cost.as_ref())
		.and_then(serde_json::Value::as_object);
	let Some(rates) = rates else {
		return Some(usage);
	};
	let rate = |key: &str| rates.get(key).and_then(serde_json::Value::as_f64).unwrap_or(0.0);
	let per_million = |tokens: u64, key: &str| (tokens as f64) * rate(key) / 1_000_000.0;
	let input = per_million(usage.input, "input");
	let output = per_million(usage.output, "output");
	let cache_read = per_million(usage.cache_read, "cacheRead");
	let cache_write = per_million(usage.cache_write, "cacheWrite");
	usage.cost = Some(Cost {
		input,
		output,
		cache_read,
		cache_write,
		total: input + output + cache_read + cache_write,
	});
	Some(usage)
}

/// Append-only writer for one advisor.
#[derive(Debug, Default)]
pub struct AdvisorTranscript {
	file:          Option<PathBuf>,
	/// Fingerprints of user deltas persisted since the last committed turn.
	replay_window: Vec<u64>,
	replay_cursor: usize,
	window_file:   Option<PathBuf>,
}

impl AdvisorTranscript {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	/// Persist one finalized message. User deltas are marked synthetic/agent so
	/// stats never count them as user prompts; assistant/tool turns are written
	/// verbatim (with their usage) so cost attribution stays intact.
	pub fn record(
		&mut self,
		message: &Message,
		session_file: Option<&Path>,
		slug: &str,
		cwd: &Path,
		catalog: &Catalog,
		provider: &str,
		model: &str,
	) {
		let Some(session_file) = session_file else {
			return;
		};
		let Some(file) = transcript_path(session_file, slug) else {
			return;
		};
		if self.window_file.as_deref() != Some(file.as_path()) {
			self.window_file = Some(file.clone());
			self.replay_window.clear();
			self.replay_cursor = 0;
		}
		if message.role == Role::User {
			let fingerprint = hash_text(&message.content);
			if self.replay_cursor < self.replay_window.len() {
				if self.replay_window[self.replay_cursor] == fingerprint {
					self.replay_cursor += 1;
					return;
				}
				self.replay_window.truncate(self.replay_cursor);
			}
			self.replay_window.push(fingerprint);
			self.replay_cursor = self.replay_window.len();
		}
		let timestamp = now_ms();
		let session_message = match message.role {
			Role::User => {
				let mut user = SessionMessage::user(MessageContent::Text(message.content.clone()), timestamp);
				user.attribution = Some("agent".to_string());
				user.extra.insert("synthetic".to_string(), serde_json::Value::Bool(true));
				user
			},
			Role::Assistant => {
				let usage = priced_usage(message, catalog, provider, model);
				let mut parts: Vec<ContentPart> = message
					.parts
					.iter()
					.filter_map(ContentPart::from_agent_part)
					.collect();
				if parts.is_empty() {
					if !message.tool_calls.is_empty() && !message.content.is_empty() {
						parts.push(ContentPart::text(message.content.clone()));
					}
				}
				let existing: BTreeSet<String> = parts
					.iter()
					.filter(|part| part.kind == "toolCall")
					.filter_map(|part| part.extra.get("id").and_then(serde_json::Value::as_str))
					.map(str::to_string)
					.collect();
				for call in &message.tool_calls {
					if !existing.contains(&call.id) {
						parts.push(ContentPart::tool_call(
							call.id.clone(),
							call.name.clone(),
							call.arguments.clone(),
						));
					}
				}
				let content = if parts.is_empty() {
					MessageContent::Text(message.content.clone())
				} else {
					MessageContent::Parts(parts)
				};
				SessionMessage::assistant(
					content,
					message.api.clone().unwrap_or_default(),
					message.provider.clone().unwrap_or_else(|| provider.to_string()),
					message.model.clone().unwrap_or_else(|| model.to_string()),
					usage,
					message
						.stop_reason
						.map(pi_agent::StopReason::as_str)
						.unwrap_or("stop"),
					timestamp,
				)
			},
			Role::Tool => SessionMessage::tool_result(
				message.tool_call_id.clone().unwrap_or_default(),
				message.name.clone().unwrap_or_else(|| "tool".to_string()),
				MessageContent::Text(message.content.clone()),
				message.is_error,
				timestamp,
			),
			// System/developer context is advisor plumbing, not a conversational
			// turn: persisting it would pollute message counts.
			Role::System | Role::Developer => return,
		};
		let entry = MessageEntry::new(
			format!("entry-{}", crate::output::session_id()),
			None,
			crate::output::iso8601(SystemTime::now()),
			session_message,
		);
		let Ok(line) = serde_json::to_string(&entry) else {
			return;
		};
		if let Err(error) = self.append(&file, &line, cwd, slug) {
			eprintln!("warning: advisor transcript {}: {error}", file.display());
		}
	}

	fn append(&mut self, file: &Path, line: &str, cwd: &Path, slug: &str) -> std::io::Result<()> {
		if let Some(parent) = file.parent() {
			std::fs::create_dir_all(parent)?;
		}
		let mut handle = std::fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(file)?;
		if handle.metadata()?.len() == 0 {
			let header = serde_json::json!({
				"type": "session",
				"version": crate::session::CURRENT_SESSION_VERSION,
				"id": format!("{}__advisor{}", crate::output::session_id(), if slug.is_empty() { String::new() } else { format!(".{slug}") }),
				"timestamp": crate::output::iso8601(SystemTime::now()),
				"cwd": cwd.display().to_string(),
				"model": serde_json::Value::Null,
				"provider": serde_json::Value::Null,
			});
			writeln!(handle, "{header}")?;
		}
		writeln!(handle, "{line}")?;
		Ok(())
	}

	/// Start one delivery attempt: rewind the replay cursor so a retried batch
	/// is skipped while the window itself is retained.
	pub fn begin_turn(&mut self) {
		self.replay_cursor = 0;
	}

	pub fn commit_turn(&mut self) {
		self.replay_window.clear();
		self.replay_cursor = 0;
	}

	pub fn abandon_turn(&mut self) {
		self.replay_window.clear();
		self.replay_cursor = 0;
	}
}

fn hash_text(text: &str) -> u64 {
	let mut hash = 0xcbf2_9ce4_8422_2325_u64;
	for byte in text.as_bytes() {
		hash ^= u64::from(*byte);
		hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
	}
	hash
}

/// Sum advisor spend already persisted in the session's stem directory, keyed
/// by slug. Subagent advisors write under `<session>/<SubId>/` and belong to
/// the child, so they can never contaminate the parent's reported spend.
#[must_use]
pub fn load_costs(session_file: Option<&Path>) -> BTreeMap<String, f64> {
	let Some(session_file) = session_file else {
		return BTreeMap::new();
	};
	let directory = session_file.with_extension("");
	let Ok(entries) = std::fs::read_dir(&directory) else {
		return BTreeMap::new();
	};
	let mut costs = BTreeMap::new();
	for entry in entries.flatten() {
		let Ok(file_type) = entry.file_type() else {
			continue;
		};
		if !file_type.is_file() {
			continue;
		}
		let name = entry.file_name();
		let Some(name) = name.to_str() else {
			continue;
		};
		if !is_advisor_transcript_name(name) {
			continue;
		}
		let slug = if name == ADVISOR_TRANSCRIPT_FILENAME {
			String::new()
		} else {
			name.trim_start_matches(&format!("{ADVISOR_TRANSCRIPT_STEM}."))
				.trim_end_matches(JSONL_SUFFIX)
				.to_string()
		};
		let Ok(content) = std::fs::read_to_string(entry.path()) else {
			continue;
		};
		let mut total = 0.0;
		for (index, line) in content.lines().enumerate() {
			let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
				continue;
			};
			if index == 0 {
				continue;
			}
			if value.get("type").and_then(serde_json::Value::as_str) != Some("message") {
				continue;
			}
			let Some(total_) = value
				.pointer("/message/usage/cost/total")
				.and_then(serde_json::Value::as_f64)
				.filter(|total| total.is_finite())
			else {
				continue;
			};
			total += total_;
		}
		if total > 0.0 {
			costs.insert(slug, total);
		}
	}
	costs
}

#[cfg(test)]
mod tests {
	use super::*;

	fn temp_dir(prefix: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"omp-advisor-transcript-{prefix}-{}-{}",
			std::process::id(),
			now_ms()
		));
		std::fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	#[test]
	fn filename_helpers_cover_default_and_named() {
		assert_eq!(advisor_transcript_filename(""), "__advisor.jsonl");
		assert_eq!(advisor_transcript_filename("reviewer"), "__advisor.reviewer.jsonl");
		assert!(is_advisor_transcript_name("__advisor.jsonl"));
		assert!(is_advisor_transcript_name("__advisor.reviewer.jsonl"));
		assert!(!is_advisor_transcript_name("__advisor-other.jsonl"));
		assert!(!is_advisor_transcript_name("task.jsonl"));
	}

	#[test]
	fn fallback_keeps_content_and_adds_tool_calls_once() {
		let dir = temp_dir("fallback");
		let session_file = dir.join("session.jsonl");
		std::fs::write(&session_file, "{}\n").expect("primary log");
		let arguments = serde_json::json!({
			"note": "child advice marker",
			"severity": "blocker"
		});
		let mut assistant = Message::assistant_with_tools(
			"preamble",
			vec![pi_agent::ToolCall {
				id: "call-adv".to_string(),
				name: "advise".to_string(),
				arguments: arguments.clone(),
			}],
		);
		assistant.usage = Some(pi_agent::TokenUsage {
			input: 100,
			output: 20,
			..pi_agent::TokenUsage::default()
		});
		let mut transcript = AdvisorTranscript::new();
		transcript.record(
			&assistant,
			Some(&session_file),
			"reviewer",
			&dir,
			&Catalog::default(),
			"mock",
			"model",
		);
		let file = transcript_path(&session_file, "reviewer").expect("path");
		let content = std::fs::read_to_string(file).expect("transcript");
		let line = content.lines().nth(1).expect("assistant line");
		let entry: MessageEntry = serde_json::from_str(line).expect("message entry");
		assert_eq!(entry.message.usage.expect("usage").input, 100);
		let MessageContent::Parts(parts) = entry.message.content else {
			panic!("expected parts: {line}");
		};
		assert_eq!(parts[0].kind, "text");
		assert_eq!(parts[0].text.as_deref(), Some("preamble"));
		assert_eq!(parts[1].kind, "toolCall");
		assert_eq!(parts[1].extra["id"], "call-adv");
		assert_eq!(parts[1].extra["name"], "advise");
		assert_eq!(parts[1].extra["arguments"], arguments);
		assert_eq!(parts.len(), 2, "tool call must not duplicate");
	}

	#[test]
	fn structured_parts_round_trip_signature_redaction_and_arguments() {
		let dir = temp_dir("parts");
		let session_file = dir.join("session.jsonl");
		std::fs::write(&session_file, "{}\n").expect("primary log");
		let arguments = serde_json::json!({
			"note": "child advice marker",
			"severity": "blocker"
		});
		let mut assistant = Message::assistant("");
		assistant.parts = vec![
			pi_agent::ContentPart::thinking_with_signature("plan", "signed-json"),
			pi_agent::ContentPart::redacted_thinking("redacted-blob"),
			pi_agent::ContentPart::tool_call("call-adv", "advise", arguments.clone()),
		];
		assistant.usage = Some(pi_agent::TokenUsage {
			input: 100,
			output: 20,
			..pi_agent::TokenUsage::default()
		});
		let mut transcript = AdvisorTranscript::new();
		transcript.record(
			&assistant,
			Some(&session_file),
			"reviewer",
			&dir,
			&Catalog::default(),
			"mock",
			"model",
		);
		let file = transcript_path(&session_file, "reviewer").expect("path");
		let content = std::fs::read_to_string(file).expect("transcript");
		let line = content.lines().nth(1).expect("assistant line");
		let entry: MessageEntry = serde_json::from_str(line).expect("message entry");
		assert_eq!(entry.message.usage.expect("usage").input, 100);
		let MessageContent::Parts(parts) = entry.message.content else {
			panic!("expected parts: {line}");
		};
		assert_eq!(parts[0].kind, "thinking");
		assert_eq!(parts[0].extra["thinking"], "plan");
		assert_eq!(parts[0].extra["thinkingSignature"], "signed-json");
		assert_eq!(parts[1].kind, "redactedThinking");
		assert_eq!(parts[1].data.as_deref(), Some("redacted-blob"));
		assert_eq!(parts[2].kind, "toolCall");
		assert_eq!(parts[2].extra["id"], "call-adv");
		assert_eq!(parts[2].extra["name"], "advise");
		assert_eq!(parts[2].extra["arguments"], arguments);
		assert_eq!(parts.len(), 3);
	}

	#[test]
	fn record_writes_header_usage_and_skips_system_and_replayed_deltas() {
		let dir = temp_dir("record");
		let session_file = dir.join("session.jsonl");
		std::fs::write(&session_file, "{}\n").expect("primary log");
		let catalog = Catalog::default();
		let mut transcript = AdvisorTranscript::new();
		let mut assistant = Message::assistant("advice");
		assistant.usage = Some(pi_agent::TokenUsage {
			input:  100,
			output: 20,
			..pi_agent::TokenUsage::default()
		});
		transcript.record(
			&Message::user("### Session update\n\nuser"),
			Some(&session_file),
			"reviewer",
			&dir,
			&catalog,
			"mock",
			"model",
		);
		transcript.record(
			&Message::system("system"),
			Some(&session_file),
			"reviewer",
			&dir,
			&catalog,
			"mock",
			"model",
		);
		transcript.record(
			&assistant,
			Some(&session_file),
			"reviewer",
			&dir,
			&catalog,
			"mock",
			"model",
		);
		// The identical delta is a retry replay, not new content: a retried
		// attempt rewinds the cursor, so the positional match skips it.
		transcript.begin_turn();
		transcript.record(
			&Message::user("### Session update\n\nuser"),
			Some(&session_file),
			"reviewer",
			&dir,
			&catalog,
			"mock",
			"model",
		);
		let file = transcript_path(&session_file, "reviewer").expect("path");
		let content = std::fs::read_to_string(file).expect("transcript");
		let lines: Vec<_> = content.lines().collect();
		assert_eq!(lines.len(), 3, "header + user + assistant: {content}");
		assert!(lines[0].contains("\"type\":\"session\""));
		assert!(lines[1].contains("\"synthetic\":true"));
		assert!(lines[2].contains("\"role\":\"assistant\""));
		assert!(lines[2].contains("\"input\":100"));
	}

	#[test]
	fn persisted_costs_are_summed_per_slug() {
		let dir = temp_dir("costs");
		let session_file = dir.join("session.jsonl");
		std::fs::write(&session_file, "{}\n").expect("primary log");
		let line = serde_json::json!({
			"type": "message",
			"id": "m1",
			"message": { "role": "assistant", "usage": { "cost": { "total": 0.25 } } }
		});
		let stem = session_file.with_extension("");
		std::fs::create_dir_all(&stem).expect("stem dir");
		std::fs::write(
			stem.join("__advisor.jsonl"),
			format!("{}\n{}\n", serde_json::json!({"type":"session","id":"x"}), line),
		)
		.expect("default transcript");
		std::fs::write(
			stem.join("__advisor.reviewer.jsonl"),
			format!("{}\n{}\n", serde_json::json!({"type":"session","id":"y"}), line),
		)
		.expect("named transcript");
		// A subagent's transcript below the top level must not count here.
		std::fs::create_dir_all(stem.join("sub")).expect("sub dir");
		std::fs::write(
			stem.join("sub").join("__advisor.jsonl"),
			format!("{}\n{}\n", serde_json::json!({"type":"session","id":"z"}), line),
		)
		.expect("sub transcript");

		let costs = load_costs(Some(&session_file));
		assert_eq!(costs.get("").copied(), Some(0.25));
		assert_eq!(costs.get("reviewer").copied(), Some(0.25));
		assert_eq!(costs.len(), 2);
	}

	#[test]
	fn two_sessions_never_share_advisor_transcripts_or_spend() {
		let dir = temp_dir("two-sessions");
		let first = dir.join("first.jsonl");
		let second = dir.join("second.jsonl");
		std::fs::write(&first, "{}\n").expect("first log");
		std::fs::write(&second, "{}\n").expect("second log");
		let line = serde_json::json!({
			"type": "message",
			"id": "m1",
			"message": { "role": "assistant", "usage": { "cost": { "total": 0.25 } } }
		});
		let first_stem = first.with_extension("");
		std::fs::create_dir_all(&first_stem).expect("first stem");
		std::fs::write(
			first_stem.join("__advisor.jsonl"),
			format!("{}\n{}\n", serde_json::json!({"type":"session","id":"a"}), line),
		)
		.expect("first transcript");
		let costs = load_costs(Some(&second));
		assert!(costs.is_empty(), "second session must not see first spend");
		assert_eq!(load_costs(Some(&first)).get("").copied(), Some(0.25));
	}
}
