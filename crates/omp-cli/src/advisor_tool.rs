//! `advise` — the only tool an advisor uses to reach the primary.
//!
//! Ports `advisor/advise-tool.ts`: a note emitted while the primary is mid-turn
//! is withheld (except `blocker`) and flushed deterministically when the turn
//! completes, and a verbatim repeat only passes when its severity escalates
//! (`nit → concern → blocker`). The tool answers the truth — `Deferred — …` for
//! a withheld note — so the advisor never believes a lost note reached the
//! primary.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use pi_agent::state::{AgentContext, ToolResult};
use pi_agent::tools::{Tool, ToolError};
use serde_json::{Value, json};

use crate::advisor_emit::{Note, Severity};

/// Sink that routes one accepted note to the primary.
pub type AdviceSink = Arc<dyn Fn(Note) + Send + Sync>;

#[derive(Default)]
struct State {
	delivered: HashMap<String, u8>,
	deferred:  Vec<Note>,
	in_progress: bool,
	source_text: String,
}

pub struct AdviseTool {
	sink:  AdviceSink,
	state: Mutex<State>,
}

impl AdviseTool {
	#[must_use]
	pub fn new(sink: AdviceSink) -> Self {
		Self { sink, state: Mutex::new(State::default()) }
	}

	/// Mark whether the next advisor prompt reviews an in-progress primary
	/// turn. A completed update flushes every withheld note, oldest first,
	/// through the normal escalation-ranked dedupe.
	pub fn begin_update(&self, in_progress: bool) {
		let mut state = self.state.lock().unwrap_or_else(|poison| poison.into_inner());
		let was = state.in_progress;
		state.in_progress = in_progress;
		let mut deliver = Vec::new();
		if was && !in_progress && !state.deferred.is_empty() {
			let pending = std::mem::take(&mut state.deferred);
			for note in pending {
				if reserve(&mut state, &note) {
					deliver.push(note);
				}
			}
		}
		drop(state);
		// The sink can route through the manager's epoch lock; never hold the
		// advise lock across it, or reset's epoch -> advise order can invert.
		for note in deliver {
			(self.sink)(note);
		}
	}

	/// Clear delivered-note memory at a conversation boundary.
	pub fn reset_delivered_notes(&self) {
		let mut state = self.state.lock().unwrap_or_else(|poison| poison.into_inner());
		state.delivered.clear();
		state.deferred.clear();
		state.in_progress = false;
	}

	pub fn set_source_text(&self, source: String) {
		self.state
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.source_text = source;
	}

	pub(crate) fn execute_note(&self, args: &Value) -> Result<ToolResult, ToolError> {
		let obj = args
			.as_object()
			.ok_or_else(|| ToolError::InvalidArguments("Arguments must be an object".to_string()))?;
		let note = obj
			.get("note")
			.and_then(Value::as_str)
			.filter(|note| !note.trim().is_empty())
			.ok_or_else(|| ToolError::InvalidArguments("Missing required parameter 'note'".to_string()))?
			.to_string();
		let severity = match obj.get("severity") {
			None | Some(Value::Null) => None,
			Some(Value::String(value)) => Some(Severity::parse(value).ok_or_else(|| {
				ToolError::InvalidArguments(
					"`severity` must be 'nit', 'concern', or 'blocker'".to_string(),
				)
			})?),
			Some(_) => {
				return Err(ToolError::InvalidArguments(
					"`severity` must be 'nit', 'concern', or 'blocker'".to_string(),
				));
			},
		};
		for key in obj.keys() {
			if key != "note" && key != "severity" {
				return Err(ToolError::InvalidArguments(format!(
					"Unknown parameter '{key}': advise accepts only 'note' and 'severity'"
				)));
			}
		}
		let entry = Note { note: note.clone(), severity, advisor: None };
		let delivery = {
			let mut state = self.state.lock().unwrap_or_else(|poison| poison.into_inner());
			if let Some(quarantined) =
				crate::advisor_runtime::quarantine_note(&note, &state.source_text)
			{
				return Err(ToolError::ExecutionFailed(quarantined));
			}
			if state.in_progress && severity != Some(Severity::Blocker) {
				let key = dedupe_key(&note);
				match state.deferred.iter_mut().find(|pending| dedupe_key(&pending.note) == key) {
					Some(pending) => {
						if severity.map(Severity::rank).unwrap_or(1)
							> pending.severity.map(Severity::rank).unwrap_or(1)
						{
							pending.severity = severity;
						}
					},
					None => state.deferred.push(entry.clone()),
				}
				Delivery::Deferred
			} else if reserve(&mut state, &entry) {
				Delivery::Now(entry.clone())
			} else {
				Delivery::Duplicate
			}
		};
		let content = match delivery {
			Delivery::Deferred => {
				"Deferred — primary is mid-turn; this note will be delivered automatically when \
				 the turn completes. Do not re-raise the same point."
					.to_string()
			},
			Delivery::Now(note) => {
				(self.sink)(note);
				"Recorded.".to_string()
			},
			Delivery::Duplicate => "Duplicate advice ignored.".to_string(),
		};
		Ok(ToolResult {
			tool_call_id: String::new(),
			content,
			is_error: false,
			terminate: false,
			details: Some(entry.details()),
		})
	}
}

enum Delivery {
	Deferred,
	Now(Note),
	Duplicate,
}

fn reserve(state: &mut State, note: &Note) -> bool {
	let key = dedupe_key(&note.note);
	let rank = note.severity.map(Severity::rank).unwrap_or(1);
	let previous = state.delivered.get(&key).copied().unwrap_or(0);
	if rank <= previous {
		return false;
	}
	state.delivered.insert(key, rank);
	true
}

fn dedupe_key(note: &str) -> String {
	note.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[async_trait::async_trait]
impl Tool for AdviseTool {
	fn name(&self) -> &str {
		"advise"
	}

	fn description(&self) -> &str {
		"Send one concrete piece of advice to the agent you are watching. At most one advise per \
		 update; never repeat advice you already gave."
	}

	fn schema(&self) -> Value {
		json!({
			"type": "object",
			"properties": {
				"note": {
					"type": "string",
					"description": "One concrete piece of advice for the agent you are watching. Terse, specific, actionable."
				},
				"severity": {
					"type": "string",
					"enum": ["nit", "concern", "blocker"],
					"description": "How strongly to weigh this. Omit for a plain nit."
				}
			},
			"required": ["note"],
			"additionalProperties": false
		})
	}

	async fn execute(&self, args: &Value, _ctx: &AgentContext) -> Result<ToolResult, ToolError> {
		self.execute_note(args)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::Mutex as StdMutex;
	use std::sync::atomic::{AtomicUsize, Ordering};

	fn recorder() -> (AdviceSink, Arc<StdMutex<Vec<Note>>>) {
		let notes = Arc::new(StdMutex::new(Vec::new()));
		let sink_notes = Arc::clone(&notes);
		let sink: AdviceSink = Arc::new(move |note| {
			sink_notes.lock().expect("notes").push(note);
		});
		(sink, notes)
	}

	#[test]
	fn mid_turn_notes_are_deferred_then_flushed_in_order() {
		let (sink, notes) = recorder();
		let tool = AdviseTool::new(sink);
		tool.begin_update(true);
		let args = json!({ "note": "The retry loop leaks the guard.", "severity": "concern" });
		let result = tool.execute_note(&args).expect("execute");
		assert!(result.content.starts_with("Deferred"));
		assert!(notes.lock().expect("notes").is_empty());
		// Same note twice while mid-turn does not pile up.
		tool.execute_note(&args).expect("execute");
		assert!(notes.lock().expect("notes").is_empty());
		tool.begin_update(false);
		let delivered = notes.lock().expect("notes");
		assert_eq!(delivered.len(), 1);
		assert_eq!(delivered[0].note, "The retry loop leaks the guard.");
	}

	#[test]
	fn blockers_bypass_mid_turn_deferral() {
		let (sink, notes) = recorder();
		let tool = AdviseTool::new(sink);
		tool.begin_update(true);
		let result = tool
			.execute_note(&json!({ "note": "Write target was deleted.", "severity": "blocker" }))
			.expect("execute");
		assert_eq!(result.content, "Recorded.");
		assert_eq!(notes.lock().expect("notes").len(), 1);
	}

	#[test]
	fn escalation_passes_but_repeat_at_same_rank_is_ignored() {
		let (sink, notes) = recorder();
		let tool = AdviseTool::new(sink);
		tool.begin_update(false);
		assert_eq!(
			tool.execute_note(&json!({ "note": "Guard is missing.", "severity": "nit" }))
				.expect("execute")
				.content,
			"Recorded."
		);
		assert_eq!(
			tool.execute_note(&json!({ "note": "Guard is missing.", "severity": "nit" }))
				.expect("execute")
				.content,
			"Duplicate advice ignored."
		);
		assert_eq!(
			tool.execute_note(&json!({ "note": "Guard   is missing.", "severity": "blocker" }))
				.expect("execute")
				.content,
			"Recorded."
		);
		assert_eq!(notes.lock().expect("notes").len(), 2);
		tool.reset_delivered_notes();
		assert_eq!(
			tool.execute_note(&json!({ "note": "Guard is missing.", "severity": "nit" }))
				.expect("execute")
				.content,
			"Recorded."
		);
	}

	#[test]
	fn invalid_severity_is_a_tool_error() {
		let (sink, _notes) = recorder();
		let tool = AdviseTool::new(sink);
		let error = tool
			.execute_note(&json!({ "note": "x", "severity": "urgent" }))
			.expect_err("invalid severity");
		assert!(matches!(error, ToolError::InvalidArguments(_)));
	}

	fn tool() -> (AdviseTool, Arc<AtomicUsize>) {
		let count = Arc::new(AtomicUsize::new(0));
		let sink_count = Arc::clone(&count);
		let tool = AdviseTool::new(Arc::new(move |_note: Note| {
			sink_count.fetch_add(1, Ordering::SeqCst);
		}));
		(tool, count)
	}

	#[test]
	fn sourced_destructive_quote_is_delivered() {
		let (tool, count) = tool();
		tool.set_source_text("tool output: deploy ran rm -rf build".to_string());
		let result = tool
			.execute_note(&json!({"note": "deploy ran rm -rf build", "severity": "concern"}))
			.expect("sourced quote must not be quarantined");
		assert_eq!(result.content, "Recorded.");
		assert_eq!(count.load(Ordering::SeqCst), 1);
	}

	#[test]
	fn output_only_destructive_advice_is_quarantined_before_dispatch() {
		let (tool, count) = tool();
		tool.set_source_text(String::new());
		let error = tool
			.execute_note(&json!({"note": "now run rm -rf build"}))
			.expect_err("unsafe output-only note must be rejected");
		assert!(error.to_string().contains("quarantined"));
		assert_eq!(count.load(Ordering::SeqCst), 0);
	}
}
