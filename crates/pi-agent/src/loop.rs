use std::sync::{Arc, Mutex};

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
	compaction::{CompactionReport, Compactor},
	mailbox::{Mailbox, format_incoming},
	provider::{ModelProvider, ProviderError, StreamAssembler, ToolChoice},
	state::{
		AgentConfig, AgentContext, AgentState, ContentPart, Exhaustion, Message, Role,
		SoftToolRequirement, StopReason, TokenUsage, ToolApprover, ToolCall, ToolPolicy, ToolResult,
		TurnResult, TurnStatus,
	},
	supervisor::BackgroundResultSink,
	tools::{registry::ToolRegistry, scheduler::ToolScheduler},
};

/// Messages that must survive compaction even when over budget.
const MIN_RECENT_MESSAGES: usize = 4;

/// Terminal status for an assistant message.
///
/// An explicit provider reason wins over the tool-call heuristic so truncation
/// (`length`), refusal/error text and abort survive instead of becoming `stop`.
fn assistant_status(message: &Message, tool_use: bool) -> TurnStatus {
	match message.stop_reason {
		Some(StopReason::Length) => TurnStatus::Length,
		Some(StopReason::Error) => TurnStatus::Error(
			message.error_message.clone().unwrap_or_else(|| "provider error".to_string()),
		),
		Some(StopReason::Aborted) => TurnStatus::Aborted,
		Some(StopReason::ToolUse) => TurnStatus::ToolUse,
		Some(StopReason::Stop) | None => {
			if tool_use {
				TurnStatus::ToolUse
			} else {
				TurnStatus::EndTurn
			}
		},
	}
}

/// Tool result recorded for calls that were cancelled before producing output.
///
/// OpenAI history requires every `tool_call` to be answered, so an interrupted
/// batch is closed with this marker instead of leaving orphan calls behind.
pub const INTERRUPTED_TOOL_RESULT: &str = "[Tool execution interrupted by user]";

/// Validate `details.images` into content parts for one tool result.
///
/// Single source of truth for the policy: the provider request, the live
/// `tool_execution_end` frame and the RPC child turn's `toolResults` all go
/// through it. `Err` names why a payload was rejected, so callers decide the
/// fallout: the loop fails the result closed, wire-only views drop the images.
pub fn tool_image_parts_of(result: &ToolResult) -> Result<Vec<ContentPart>, String> {
	let Some(images) = result
		.details
		.as_ref()
		.and_then(|details| details.get("images"))
		.and_then(serde_json::Value::as_array)
	else {
		return Ok(Vec::new());
	};
	let mut parts = Vec::with_capacity(images.len());
	for image in images {
		let data = image.get("data").and_then(serde_json::Value::as_str);
		let mime = image
			.get("mimeType")
			.or_else(|| image.get("mime_type"))
			.and_then(serde_json::Value::as_str);
		match (data, mime) {
			(Some(data), Some(mime)) => {
				parts.push(ContentPart::image(data, mime)?);
			},
			_ => return Err("image entry needs base64 `data` and `mimeType`".to_string()),
		}
	}
	Ok(parts)
}

/// Per-call image map for the provider request. Invalid payloads fail closed:
/// the tool result becomes an error and the image is not attached, so a bad
/// kernel payload cannot break every later request in the turn.
fn tool_image_parts(
	results: &mut [ToolResult],
) -> std::collections::HashMap<String, Vec<ContentPart>> {
	let mut by_call = std::collections::HashMap::new();
	for result in results {
		match tool_image_parts_of(result) {
			Ok(parts) if !parts.is_empty() => {
				by_call.insert(result.tool_call_id.clone(), parts);
			},
			Ok(_) => {},
			Err(error) => {
				result.is_error = true;
				result.content =
					format!("{}\n\n[image payload rejected: {error}]", result.content);
			},
		}
	}
	by_call
}

/// Metadata the loop hands the turn-end consumer with each ack request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TurnAckRequest {
	/// Message count before the finished assistant message was appended.
	pub turn_start: usize,
	/// Message count after the finished turn (assistant + tool results) was appended.
	pub message_count: usize,
}

/// In-memory branch cut for sessions without persistence.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InMemoryRewind {
	/// Live-loop messages to keep (checkpoint position, inclusive). Only the
	/// loop applies this count; the session mirror may be uncompacted.
	pub keep: usize,
	/// Provider tool-call id of the checkpoint result. Session mirrors locate
	/// the cut by this id so compaction/steering cannot shift it.
	pub checkpoint_tool_call_id: Option<String>,
	/// Rendered `branch_summary` context (user role).
	pub summary: String,
	/// Rendered retained rewind report (developer/system role).
	pub report: String,
}

/// Conversation rewrite requested by the turn-end consumer.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnRewrite {
	/// Replace the whole live conversation (persisted branch rebuild).
	Replace(Vec<Message>),
	/// Truncate at `keep` and append the summary/report (no-session mode).
	InMemory(InMemoryRewind),
}

/// Result of the async turn-end handshake. `Default` means unchanged.
///
/// Besides journaling/rewind, the session owner may steer the dialog at this
/// boundary: replace the provider for the rest of the dialog, inject hidden
/// guidance with a stable identity, or scrub guidance it consumed. The loop
/// keeps the replacement in a dialog-local variable; `AgentLoop::provider`
/// is never mutated, so concurrent turns cannot observe each other's switch.
#[derive(Clone)]
pub struct TurnAckDecision {
	pub rewrite: Option<TurnRewrite>,
	/// Run another provider request instead of ending the dialog.
	pub continue_turn: bool,
	/// Warning to append for the continuation turn.
	pub warning: Option<String>,
	/// Hidden guidance appended after the journaled batch and before the next
	/// provider request, in this order.
	pub guidance: Vec<Message>,
	/// Remove live guidance carrying this identity (consumed steering).
	pub scrub_custom_type: Option<String>,
	/// Provider for the remaining requests of this dialog.
	pub provider: Option<Arc<dyn ModelProvider>>,
	/// Context budget of the replacement model, when it differs from the one
	/// the run started with.
	pub context_limit: Option<usize>,
	/// Replace the live tool selection before the next provider request.
	/// Hiding is deliberately a delta: mounted MCP/host tools that were never
	/// in the config allowlist must not be filtered by an exhaustive snapshot.
	pub tool_selection: Option<TurnToolSelection>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnToolSelection {
	/// Internal tool names to remove from schemas and execution.
	Hide(Vec<String>),
}

impl Default for TurnAckDecision {
	fn default() -> Self {
		Self {
			rewrite: None,
			continue_turn: false,
			warning: None,
			guidance: Vec::new(),
			scrub_custom_type: None,
			provider: None,
			context_limit: None,
			tool_selection: None,
		}
	}
}

impl std::fmt::Debug for TurnAckDecision {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("TurnAckDecision")
			.field("rewrite", &self.rewrite)
			.field("continue_turn", &self.continue_turn)
			.field("warning", &self.warning)
			.field("guidance", &self.guidance)
			.field("scrub_custom_type", &self.scrub_custom_type)
			.field("provider", &self.provider.as_ref().map(|_| "<provider>"))
			.field("context_limit", &self.context_limit)
			.field("tool_selection", &self.tool_selection)
			.finish()
	}
}

#[derive(Debug)]
struct PendingTurnAck {
	request: TurnAckRequest,
	reply: oneshot::Sender<TurnAckDecision>,
}

/// One-shot per-turn rendezvous between the agent loop and the session owner.
///
/// The loop registers a request before emitting `TurnEnd`, then awaits the
/// decision while still inside its async task; the session consumer (on its own
/// thread) persists the batch and replies before the next provider request.
/// Dropping/never replying unblocks the loop via stream cancellation.
#[derive(Debug, Default)]
pub struct TurnAck {
	pending: Mutex<Option<PendingTurnAck>>,
}

impl TurnAck {
	#[must_use]
	pub fn new() -> Arc<Self> {
		Arc::new(Self::default())
	}

	/// Register the turn about to end and return the decision receiver.
	pub fn begin(&self, request: TurnAckRequest) -> oneshot::Receiver<TurnAckDecision> {
		let (reply, rx) = oneshot::channel();
		*self.pending.lock().unwrap_or_else(|poison| poison.into_inner()) =
			Some(PendingTurnAck { request, reply });
		rx
	}

	/// Take the pending request (consumer side) and its metadata.
	#[must_use]
	pub fn take(&self) -> Option<(TurnAckRequest, oneshot::Sender<TurnAckDecision>)> {
		self
			.pending
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.take()
			.map(|pending| (pending.request, pending.reply))
	}

	/// Reply to the pending turn. Returns `false` when there is none.
	#[must_use]
	pub fn finish(&self, decision: TurnAckDecision) -> bool {
		self
			.take()
			.is_some_and(|(_, reply)| reply.send(decision).is_ok())
	}

	/// Drop a pending reply so the loop stops waiting (cancellation/error path).
	pub fn cancel(&self) {
		drop(self.pending.lock().unwrap_or_else(|poison| poison.into_inner()).take());
	}
}

/// Live event emitted while a dialog runs.
///
/// Only assistant text is streamed: tool calls are delivered finalized, in the
/// `TurnResult` that closes the turn. Consumers accumulate `delta` themselves,
/// so no O(n²) full-text clones travel over the channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentEvent {
	/// Incremental assistant text, exactly as the provider streamed it.
	MessageUpdate { delta: String },
	/// Incremental public provider reasoning (TS `thinking_delta`), separate
	/// from [`Self::MessageUpdate`] so the answer text is never polluted.
	ThinkingUpdate { delta: String },
	/// Provider finalization (assembler.finish()), BEFORE tool batch runs. Once per assistant message.
	MessageEnd { message: Message, status: TurnStatus },
	/// Tool invocation started before batch execution.
	ToolStart { id: String, name: String, arguments: serde_json::Value },
	/// Partial tool output reported while the tool still runs (TS
	/// `tool_execution_update`). Sent by the tool through
	/// [`crate::state::AgentContext::report_progress`] on the same queue as
	/// `ToolStart`/`ToolEnd`, so an update always precedes its own end.
	ToolUpdate { id: String, name: String, args: serde_json::Value, partial: serde_json::Value },
	/// Tool invocation finished with result or error. `images` are the validated
	/// `details.images` parts the provider request will carry, so the host wire
	/// sees exactly what the model sees.
	ToolEnd {
		id: String,
		name: String,
		result: String,
		is_error: bool,
		images: Vec<ContentPart>,
		/// Structured result details (TS `AgentToolResult.details`), replayed on
		/// the wire verbatim and persisted with the tool result.
		details: Option<serde_json::Value>,
	},
	/// After tool results resolved (or immediately when tool_calls empty / on abort). Once per assistant message.
	TurnEnd { message: Message, tool_results: Vec<ToolResult>, status: TurnStatus },
	/// A user message accepted from the steering channel, exactly as it entered
	/// the history. Emitted only after acceptance — never for a queued message
	/// that was not injected — so the host can persist it at that position and
	/// mirror it instead of losing the accepted instruction.
	UserSteer { parts: Vec<ContentPart> },
	/// One finished background result was injected as a user message at this
	/// boundary. Emitted after the message is in history, exactly once per
	/// `id`: the supervisor's consumption mark is the only authority, and the
	/// same id can never arrive through auto-delivery and the hub snapshot path.
	/// `usage` is the child's summed provider report (`None` = unreported), so a
	/// host accounts it once per id instead of re-deriving it from the text.
	/// `result_key` is the durable accounting key (`resultKey` when present,
	/// else `id`); it is absent only for a sink with no supervisor behind it.
	BackgroundResult {
		id: String,
		result_key: Option<String>,
		text: String,
		usage: Option<TokenUsage>,
	},
}

/// Announce one accepted steering message to the host event stream.
async fn announce_user_steer(events: &Option<mpsc::Sender<AgentEvent>>, parts: &[ContentPart]) {
	if let Some(sink) = events {
		let _ = sink.send(AgentEvent::UserSteer { parts: parts.to_vec() }).await;
	}
}

#[derive(Debug, thiserror::Error)]
pub enum AgentError {
	#[error("Max turns exceeded: {0}")]
	MaxTurnsExceeded(usize),
	#[error("Execution interrupted by steering")]
	Interrupted,
	#[error("Model provider error: {0}")]
	Provider(#[from] ProviderError),
	#[error("Internal error: {0}")]
	Internal(String),
}

/// Control event to steer or interrupt a running dialog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SteeringEvent {
	Interrupt,
	/// Exact user parts (text and images) injected as one user message, so a
	/// steering image survives instead of being flattened to text.
	InjectMessage(Vec<ContentPart>),
}

/// Events drained from the steering channel at one point in time.
#[derive(Default)]
struct Steering {
	injections: Vec<Vec<ContentPart>>,
	interrupted: bool,
}

impl Steering {
	fn absorb(&mut self, event: SteeringEvent) {
		match event {
			SteeringEvent::Interrupt => self.interrupted = true,
			SteeringEvent::InjectMessage(message) => self.injections.push(message),
		}
	}

	/// Drain whatever is already queued, without awaiting new events.
	fn drain(rx: &mut mpsc::Receiver<SteeringEvent>) -> Self {
		let mut steering = Self::default();
		while let Ok(event) = rx.try_recv() {
			steering.absorb(event);
		}
		steering
	}
}

/// Outcome of one tool batch.
struct BatchOutcome {
	results: Vec<ToolResult>,
	injections: Vec<Vec<ContentPart>>,
	interrupted: bool,
}

/// Upstream `MAX_SOFT_TOOL_ESCALATIONS`: abort instead of forcing forever.
const MAX_SOFT_TOOL_ESCALATIONS: u32 = 3;

/// Loop-local soft requirement lifecycle (upstream `SoftToolRequirementState`).
///
/// The host only peeks the queue head; everything that must not leak between
/// prompts or child agents — the reminder sent for a head, the one-request
/// force, and the escalation budget — lives here for exactly one dialog run.
#[derive(Default)]
struct SoftRequirementState {
	head: Option<(u64, u64)>,
	force_next: bool,
	escalations: u32,
}

impl SoftRequirementState {
	/// Adopt the current head. Returns `true` when it changed (or first
	/// appeared), which is exactly when the reminder is injected.
	fn sync(&mut self, requirement: Option<&SoftToolRequirement>) -> bool {
		match requirement {
			Some(requirement) => {
				let head = (requirement.id, requirement.generation);
				if self.head == Some(head) {
					return false;
				}
				self.head = Some(head);
				self.force_next = false;
				self.escalations = 0;
				true
			},
			None => {
				self.head = None;
				self.force_next = false;
				self.escalations = 0;
				false
			},
		}
	}
}

/// Marks a loop's agent present in the shared mailbox while the dialog runs.
/// Root (`Main`) turns and detached child loops both register here, so a peer
/// `hub wait` can tell whether its counterpart still exists.
struct PresenceGuard {
	mailbox: Arc<Mailbox>,
	agent:   String,
}

impl PresenceGuard {
	fn install(peer_inbox: &Option<(Arc<Mailbox>, String)>) -> Option<Self> {
		let (mailbox, agent) = peer_inbox.as_ref()?;
		mailbox.set_agent_active(agent, true);
		Some(Self { mailbox: Arc::clone(mailbox), agent: agent.clone() })
	}
}

impl Drop for PresenceGuard {
	fn drop(&mut self) {
		self.mailbox.set_agent_active(&self.agent, false);
	}
}

/// Cut the live conversation after the checkpoint tool batch (all sibling
/// results of that assistant message). The anchor is located in the *current*
/// live messages, so it only works while compaction retained that result;
/// once pruned, the stale `keep` fallback runs (it may
/// clamp to a no-op or land inside the retained region). The fallback never
/// splits an assistant tool call from its results or leaves a dangling trailing
/// call, so the degraded live context is still structurally valid; the mirror
/// cut remains anchored and the next prompt self-heals.
fn apply_in_memory_cut(messages: &mut Vec<Message>, rewind: &InMemoryRewind) {
	let keep = rewind
		.checkpoint_tool_call_id
		.as_deref()
		.and_then(|id| tool_batch_end(messages, id))
		.unwrap_or_else(|| repair_tool_batch_boundary(messages, rewind.keep))
		.min(messages.len());
	messages.truncate(keep);
	if !rewind.summary.is_empty() {
		messages.push(Message::user(rewind.summary.clone()));
	}
	if !rewind.report.is_empty() {
		messages.push(Message::system(rewind.report.clone()));
	}
}

/// End (exclusive) of the tool-result group owned by the assistant message
/// that issued `tool_call_id`, or `None` when either side is absent.
#[must_use]
pub fn tool_batch_end(messages: &[Message], tool_call_id: &str) -> Option<usize> {
	let result_position = messages
		.iter()
		.rposition(|message| message.tool_call_id.as_deref() == Some(tool_call_id))?;
	let assistant_position = messages[..result_position].iter().rposition(|message| {
		message.role == Role::Assistant
			&& message.tool_calls.iter().any(|call| call.id == tool_call_id)
	})?;
	let call_ids: std::collections::HashSet<&str> = messages[assistant_position]
		.tool_calls
		.iter()
		.map(|call| call.id.as_str())
		.collect();
	let mut end = assistant_position + 1;
	while end < messages.len()
		&& messages[end].role == Role::Tool
		&& messages[end]
			.tool_call_id
			.as_deref()
			.is_some_and(|id| call_ids.contains(id))
	{
		end += 1;
	}
	Some(end)
}

/// Pair-safe fallback cut: finish a partially kept tool-result run and drop a
/// dangling trailing assistant tool call.
#[must_use]
pub fn repair_tool_batch_boundary(messages: &[Message], keep: usize) -> usize {
	let mut keep = keep.min(messages.len());
	// Cutting inside a tool-result run would strand the assistant tool call;
	// finish the whole batch instead.
	if keep > 0 && keep < messages.len() && messages[keep].role == Role::Tool {
		while keep < messages.len() && messages[keep].role == Role::Tool {
			keep += 1;
		}
	}
	// A kept assistant tool call whose results were cut away is also invalid.
	if keep > 0 {
		let last = &messages[keep - 1];
		let next = messages.get(keep);
		if last.role == Role::Assistant
			&& !last.tool_calls.is_empty()
			&& next.is_none_or(|next| next.role != Role::Tool)
		{
			keep -= 1;
		}
	}
	keep
}

/// Apply a session-owner reply to the live conversation. `Replace` restores
/// the owner's retained context wholesale (no dependency on a pruned anchor);
/// `InMemory` cuts at the checkpoint result when still present.
fn apply_turn_decision(
	messages: &mut Vec<Message>,
	rewrite: Option<TurnRewrite>,
	warning: Option<String>,
) -> bool {
	let mut continue_turn = false;
	match rewrite {
		Some(TurnRewrite::Replace(mut replacement)) => {
			if let Some(system) = messages
				.first()
				.filter(|message| message.role == Role::System)
				.cloned()
			{
				if replacement
					.first()
					.is_none_or(|message| message.role != Role::System)
				{
					replacement.insert(0, system);
				}
			}
			*messages = replacement;
			continue_turn = true;
		},
		Some(TurnRewrite::InMemory(rewind)) => {
			apply_in_memory_cut(messages, &rewind);
			continue_turn = true;
		},
		None => {},
	}
	if let Some(warning) = warning
		&& !warning.is_empty()
	{
		messages.push(Message::system(warning));
		continue_turn = true;
	}
	continue_turn
}

/// Apply the dialog-steering half of a turn-end reply: scrub consumed guidance
/// first (so a re-injected identity survives), then move to the replacement
/// provider, then append the new guidance. Provider replacement is local to
/// the running dialog; no shared loop state is mutated.
fn apply_turn_boundary(
	messages: &mut Vec<Message>,
	active_provider: &mut Arc<dyn ModelProvider>,
	context_limit: &mut usize,
	guidance: Vec<Message>,
	scrub_custom_type: Option<String>,
	provider: Option<Arc<dyn ModelProvider>>,
	replacement_context_limit: Option<usize>,
) {
	if let Some(custom_type) = scrub_custom_type.as_deref() {
		messages.retain(|message| message.custom_type.as_deref() != Some(custom_type));
	}
	if let Some(provider) = provider {
		*active_provider = provider;
	}
	if let Some(limit) = replacement_context_limit {
		*context_limit = limit;
	}
	messages.extend(guidance);
}

/// Synthetic result for a call that must never run (TS
/// `createSyntheticToolResultMessage`). The assistant's tool_use still needs a
/// paired tool_result, so truncated and terminal turns persist this instead of
/// executing partial arguments or leaving an orphan call.
fn synthetic_tool_result(call: &ToolCall, reason: &str, error_message: Option<&str>) -> ToolResult {
	let text = match reason {
		"aborted" => "Tool execution was aborted",
		"length" => "Tool call was not executed because the assistant hit its output token limit (stop_reason: length) before the arguments could complete; the recorded arguments are truncated and unsafe to run. Do NOT retry by re-emitting the same large payload — split the work into several smaller tool calls (e.g. for `write`/`edit`, write the first chunk then append the rest with subsequent `edit` insert ops, or break the file into multiple `write` targets)",
		"error" => "Tool call was not executed because the provider stream ended with an error before the tool could run",
		_ => "Tool call was not executed because the assistant ended its turn",
	};
	let content = match error_message {
		Some(error) => format!("{text}: {error}"),
		None => format!("{text}."),
	};
	let source = match reason {
		"aborted" => "assistant_stop_aborted",
		"length" => "assistant_stop_length",
		"error" => "assistant_stop_error",
		_ => "assistant_stop_skipped",
	};
	let mut details = serde_json::json!({
		"__synthetic": true,
		"source": source,
		"executed": false,
	});
	if reason == "error"
		&& let Some(error) = error_message
	{
		details["upstreamError"] = serde_json::Value::String(error.to_string());
	}
	ToolResult {
		tool_call_id: call.id.clone(),
		content,
		is_error: true,
		terminate: false,
		details: Some(details),
	}
}

/// Pair every assistant tool_call with a synthetic result, emitting the same
/// start/end frames a real execution would (TS `createAbortedToolResult`), so
/// the host's tool counters and history stay consistent without side effects.
async fn pair_skipped_tool_calls(
	assistant: &Message,
	reason: &str,
	error_message: Option<&str>,
	events: Option<&mpsc::Sender<AgentEvent>>,
	messages: &mut Vec<Message>,
) -> Vec<ToolResult> {
	let mut results = Vec::with_capacity(assistant.tool_calls.len());
	for call in &assistant.tool_calls {
		let result = synthetic_tool_result(call, reason, error_message);
		if let Some(events_sink) = events {
			let _ = events_sink
				.send(AgentEvent::ToolStart {
					id: call.id.clone(),
					name: call.name.clone(),
					arguments: call.arguments.clone(),
				})
				.await;
			let _ = events_sink
				.send(AgentEvent::ToolEnd {
					id: result.tool_call_id.clone(),
					name: call.name.clone(),
					result: result.content.clone(),
					is_error: true,
					images: Vec::new(),
					details: result.details.clone(),
				})
				.await;
		}
		messages.push(Message::tool_with_status(
			&result.tool_call_id,
			&call.name,
			&result.content,
			true,
		));
		results.push(result);
	}
	results
}

/// C4-specific pre-dispatch gate for one assistant response.
///
/// The advisor installs this so a quarantined turn never reaches `run_tools`.
/// The default loop has no guard, so every other caller keeps the exact
/// historical behavior. `history` is the provider history before this assistant
/// message; it already contains the tool results of the current update.
pub trait AssistantGuard: Send + Sync {
	fn guard(&self, message: &mut Message, history: &[Message]);
}

/// Agent execution engine: provider turn, tool batch, repeat.
pub struct AgentLoop {
	config: AgentConfig,
	provider: Arc<dyn ModelProvider>,
	scheduler: ToolScheduler,
	registry: ToolRegistry,
	/// Finished background results injected as user messages at turn boundaries.
	background_results: Option<Arc<dyn BackgroundResultSink>>,
	/// Peer mailbox drained into the same boundary messages.
	peer_inbox: Option<(Arc<Mailbox>, String)>,
	/// Session-owned staged-preview policy (peeked from the registry).
	policy: Option<Arc<dyn ToolPolicy>>,
	/// Turn-end persistence/rewind handshake with the session owner.
	turn_ack: Option<Arc<TurnAck>>,
	/// Session plan-mode guard copied into every tool call context of this run.
	plan: Option<Arc<dyn crate::state::PlanModeGuard>>,
	/// Session-owned conflict history, when the host outlives a single turn.
	/// Without it each run starts from an empty per-context history.
	conflicts: Option<Arc<crate::tools::conflict::ConflictHistory>>,
	/// Detached plan continuation: history already carries the persisted
	/// developer reminder, so no caller message is appended and the first
	/// request is the policy's one-shot forced choice.
	continuation: bool,
	/// Optional C4 assistant-response gate; `None` for every non-advisor caller.
	assistant_guard: Option<Arc<dyn AssistantGuard>>,
	/// Host hook invoked with the live conversation at every turn boundary and,
	/// after a prune, with the index mapping it applied. Owners journal there,
	/// so a compaction can only drop turns that are already on disk.
	turn_observer: Option<TurnObserver>,
}

/// Host hook for [`AgentLoop::with_turn_observer`].
///
/// `prune` is `Some` only for the call right after [`Compactor::compact_report`]
/// rewrote the conversation: the messages are the compacted ones and the owner
/// must remap its write cursor with [`CompactionReport::remap`].
///
/// The return value is durability: `false` means accepted messages are not yet
/// on disk, so the loop must not compact (an unwritten turn would be lost).
pub type TurnObserver =
	Arc<dyn Fn(&[Message], Option<CompactionReport>) -> bool + Send + Sync>;

impl AgentLoop {
	pub fn new(
		config: AgentConfig,
		provider: Arc<dyn ModelProvider>,
		registry: ToolRegistry,
	) -> Self {
		let scheduler = ToolScheduler::new(registry.clone());
		let policy = registry.policy();
		Self {
			config,
			provider,
			scheduler,
			registry,
			background_results: None,
			peer_inbox: None,
			policy,
			turn_ack: None,
			plan: None,
			conflicts: None,
			continuation: false,
			assistant_guard: None,
			turn_observer: None,
		}
	}

	pub fn with_approver(
		config: AgentConfig,
		provider: Arc<dyn ModelProvider>,
		registry: ToolRegistry,
		approver: Arc<dyn ToolApprover>,
	) -> Self {
		let scheduler = ToolScheduler::with_approver(registry.clone(), approver);
		let policy = registry.policy();
		Self {
			config,
			provider,
			scheduler,
			registry,
			background_results: None,
			peer_inbox: None,
			policy,
			turn_ack: None,
			plan: None,
			conflicts: None,
			continuation: false,
			assistant_guard: None,
			turn_observer: None,
		}
	}

	/// Install the C4 pre-dispatch assistant gate for this advisor loop only.
	#[must_use]
	pub fn with_assistant_guard(mut self, guard: Option<Arc<dyn AssistantGuard>>) -> Self {
		self.assistant_guard = guard;
		self
	}

	/// Install the session plan-mode guard for this run; it rides on every
	/// tool-call [`AgentContext`], so concurrent sessions never share it.
	#[must_use]
	pub fn with_plan(mut self, plan: Option<Arc<dyn crate::state::PlanModeGuard>>) -> Self {
		self.plan = plan;
		self
	}

	/// Mount the session-owned conflict history so `conflict://<N>` ids minted
	/// in one turn stay resolvable in later turns of the same session.
	#[must_use]
	pub fn with_conflicts(
		mut self,
		conflicts: Option<Arc<crate::tools::conflict::ConflictHistory>>,
	) -> Self {
		self.conflicts = conflicts;
		self
	}

	/// Mark this run as the detached plan-reminder continuation: no new caller
	/// message is appended and the first request uses the policy's forced
	/// choice once.
	#[must_use]
	pub fn with_continuation(mut self, continuation: bool) -> Self {
		self.continuation = continuation;
		self
	}

	/// Attach the per-turn ack channel used by the session owner for
	/// persistence and checkpoint/rewind context replacement.
	#[must_use]
	pub fn with_turn_ack(mut self, ack: Arc<TurnAck>) -> Self {
		self.turn_ack = Some(ack);
		self
	}

	/// Register the supervisor's exactly-once background result sink. Without
	/// this the task tool remains synchronous, because a detached job would have
	/// no one to deliver its result to.
	#[must_use]
	pub fn with_background_results(mut self, sink: Arc<dyn BackgroundResultSink>) -> Self {
		self.background_results = Some(sink);
		self
	}

	/// Drain peer mail addressed to `agent_id` into turn-boundary messages.
	#[must_use]
	pub fn with_peer_inbox(mut self, mailbox: Arc<Mailbox>, agent_id: impl Into<String>) -> Self {
		self.peer_inbox = Some((mailbox, agent_id.into()));
		self
	}

	/// Install a host hook called with the live conversation at every turn
	/// boundary and, after a prune, with the index mapping it applied. The owner
	/// journals accepted messages there, so every turn compaction removes is
	/// already durable and the cursor can be remapped exactly.
	#[must_use]
	pub fn with_turn_observer(mut self, observer: Option<TurnObserver>) -> Self {
		self.turn_observer = observer;
		self
	}

	/// Push undelivered background results and buffered peer mail as user
	/// messages. Called only between turns, so history never has an unanswered
	/// tool call. Each delivered result also reaches the host as
	/// [`AgentEvent::BackgroundResult`] once per id, carrying the child's
	/// reported usage so root accounting never parses the envelope.
	async fn inject_boundary_messages(
		&self,
		state: &mut AgentState,
		events: Option<&mpsc::Sender<AgentEvent>>,
	) {
		if let Some(sink) = &self.background_results {
			for delivery in sink.take_background_deliveries() {
				state.messages.push(Message::user(delivery.text.clone()));
				if let Some(events) = events {
					let _ = events
						.send(AgentEvent::BackgroundResult {
							id: delivery.id,
							result_key: delivery.result_key,
							text: delivery.text,
							usage: delivery.usage,
						})
						.await;
				}
			}
		}
		if let Some((mailbox, agent_id)) = &self.peer_inbox {
			for message in mailbox.inbox(agent_id, false) {
				state.messages.push(Message::user(format_incoming(&message)));
			}
		}
	}
	fn begin_turn_ack(
		&self,
		turn_start: usize,
		message_count: usize,
	) -> Option<oneshot::Receiver<TurnAckDecision>> {
		self
			.turn_ack
			.as_ref()
			.map(|ack| ack.begin(TurnAckRequest { turn_start, message_count }))
	}

	/// Await the session owner's reply. Cancellation or a dropped stream unblocks
	/// with the default (unchanged) decision so the loop can fail safely.
	async fn await_turn_ack(
		&self,
		receiver: Option<oneshot::Receiver<TurnAckDecision>>,
		cancellation_token: &CancellationToken,
	) -> TurnAckDecision {
		let Some(receiver) = receiver else {
			return TurnAckDecision::default();
		};
		tokio::select! {
			decision = receiver => decision.unwrap_or_default(),
			() = cancellation_token.cancelled() => TurnAckDecision::default(),
		}
	}

	/// Apply a session-owner rewrite to the live conversation. A replacement
	/// keeps the leading system message; an in-memory cut prefers the
	/// checkpoint tool-call anchor and falls back to a pair-safe `keep`.
	fn apply_turn_rewrite(
		&self,
		messages: &mut Vec<Message>,
		rewrite: Option<TurnRewrite>,
		warning: Option<String>,
	) -> bool {
		apply_turn_decision(messages, rewrite, warning)
	}

	/// Apply a typed tool-selection replacement at the turn boundary. It
	/// changes both the schemas and execution immediately, before the next
	/// provider request in this dialog.
	fn apply_turn_tools(&self, selection: Option<TurnToolSelection>) {
		if let Some(TurnToolSelection::Hide(names)) = selection {
			self.scheduler.hide_tools(names);
		}
	}

	/// Run turns without emitting live events.
	pub async fn run_dialog(
		&self,
		user_input: &str,
		state: &mut AgentState,
		steering_rx: Option<mpsc::Receiver<SteeringEvent>>,
	) -> Result<Vec<TurnResult>, AgentError> {
		self
			.run_dialog_with(user_input, state, steering_rx, None)
			.await
	}

	/// Run turns until the assistant answers without tool calls or the turn budget runs out.
	///
	/// With `events` set, the assistant turn is requested as a provider stream and
	/// every text delta is forwarded as [`AgentEvent::MessageUpdate`] the moment
	/// it arrives; tool calls still arrive only in the returned [`TurnResult`].
	/// The channel is bounded by the caller, so a slow consumer applies
	/// backpressure instead of growing memory. The consumer must therefore
	/// drain the channel concurrently (or size it for a full turn): a stalled
	/// consumer blocks the turn.
	///
	/// History invariants: every assistant `tool_call` is followed by exactly one
	/// tool message with the same id, and injected user messages are appended
	/// after a completed tool batch, never between an assistant call and its
	/// results.
	pub async fn run_dialog_with(
		&self,
		user_input: &str,
		state: &mut AgentState,
		steering_rx: Option<mpsc::Receiver<SteeringEvent>>,
		events: Option<mpsc::Sender<AgentEvent>>,
	) -> Result<Vec<TurnResult>, AgentError> {
		self.run_dialog_message(Message::user(user_input), state, steering_rx, events).await
	}

	/// Run turns with a pre-constructed user message (supporting text and image parts).
	pub async fn run_dialog_message(
		&self,
		user_message: Message,
		state: &mut AgentState,
		mut steering_rx: Option<mpsc::Receiver<SteeringEvent>>,
		events: Option<mpsc::Sender<AgentEvent>>,
	) -> Result<Vec<TurnResult>, AgentError> {
		self
			.run_dialog_message_with_cancel(
				user_message,
				state,
				steering_rx.as_mut(),
				events,
				CancellationToken::new(),
			)
			.await
	}

	/// Like [`Self::run_dialog_message`], but with a caller-owned cancellation
	/// token so a supervisor can stop the loop (and its in-flight tools) directly.
	///
	/// The steering receiver is borrowed, not moved: a long-lived owner (the
	/// revived child actor) keeps it across dialogs, so a message that missed
	/// its last step boundary is still readable instead of being dropped with
	/// the channel.
	pub async fn run_dialog_message_with_cancel(
		&self,
		user_message: Message,
		state: &mut AgentState,
		mut steering_rx: Option<&mut mpsc::Receiver<SteeringEvent>>,
		events: Option<mpsc::Sender<AgentEvent>>,
		cancellation_token: CancellationToken,
	) -> Result<Vec<TurnResult>, AgentError> {
		// The configured prompt belongs at the head of the history; resumed
		// sessions may already carry messages, so test for the role, not emptiness.
		if let Some(system) = &self.config.system_prompt
			&& state
				.messages
				.first()
				.is_none_or(|message| message.role != Role::System)
		{
			state.messages.insert(0, Message::system(system.clone()));
		}
		if !self.continuation {
			state.messages.push(user_message);
		}
		let _presence = PresenceGuard::install(&self.peer_inbox);
		self.inject_boundary_messages(state, events.as_ref()).await;
		let ctx = AgentContext::with_approver(
			self.config.cwd.clone(),
			cancellation_token.clone(),
			self.scheduler.approver().cloned(),
		)
		.with_progress(events.clone())
		.with_plan(self.plan.clone());
		let ctx = match &self.conflicts {
			Some(conflicts) => ctx.with_conflicts(Arc::clone(conflicts)),
			None => ctx,
		};
		let mut turn_results = Vec::new();
		let mut soft = SoftRequirementState::default();
		let mut continuation_pending = self.continuation;
		// Dialog-local provider: a turn-end decision may replace it for the rest
		// of this dialog. `self.provider` stays the initial value, so concurrent
		// turns of the same loop never observe one another's switch.
		let mut active_provider: Arc<dyn ModelProvider> = Arc::clone(&self.provider);
		// Context budget follows a provider replacement for the rest of the dialog.
		let mut context_limit = self.config.context_limit;

		while state.total_turns < self.config.max_turns {
			if let Some(rx) = steering_rx.as_deref_mut() {
				let steering = Steering::drain(rx);
				// Accepted sends were already receipted: hand them to the state
				// before an interrupt can end the dialog, or they would be lost
				// while their `injected` receipt stands.
				for parts in steering.injections {
					announce_user_steer(&events, &parts).await;
					state.messages.push(Message::user_with_parts(parts));
				}
				if steering.interrupted {
					cancellation_token.cancel();
					return Err(AgentError::Interrupted);
				}
			}
			// Results that settled while the previous assistant turn ran land
			// before this one, exactly once.
			self.inject_boundary_messages(state, events.as_ref()).await;

			// Peek the session queue before compaction so the reminder is part
			// of this request and stays in the newest history. A requirement
			// naming a tool this registry does not offer is dropped silently
			// (upstream `agent.ts` gate): no reminder and no invalid force.
			let soft_req = self
				.policy
				.as_ref()
				.and_then(|policy| policy.requirement())
				.filter(|requirement| self.registry.get(&requirement.tool_name).is_some());
			if continuation_pending {
				// The reminder is already persisted; adopt the head without
				// injecting it a second time.
				soft.sync(soft_req.as_ref());
			} else if !cancellation_token.is_cancelled()
				&& soft.sync(soft_req.as_ref())
				&& let Some(requirement) = &soft_req
			{
				state.messages.push(requirement.reminder.clone());
			}

			// Durable before destructive: the owner journals everything accepted
			// so far. If that write failed, the prune below is skipped — dropping
			// a turn the transcript does not hold would lose it silently. The next
			// boundary retries, and the run keeps every message meanwhile.
			let durable = self
				.turn_observer
				.as_ref()
				.is_none_or(|observer| observer(&state.messages, None));
			if durable {
				let (compacted, report) =
					Compactor::compact_report(&state.messages, context_limit, MIN_RECENT_MESSAGES);
				state.messages = compacted;
				// The rewrite is destructive even though its input is durable: the
				// owner must remap its write cursor to the new index space, never
				// infer the rewrite from lengths (they can match again).
				if let (Some(observer), Some(report)) = (&self.turn_observer, report) {
					observer(&state.messages, Some(report));
				}
			}

			if cancellation_token.is_cancelled() {
				return Err(AgentError::Interrupted);
			}
			let hidden = self.scheduler.hidden();
			let schemas = self.registry.schemas_excluding(&hidden);
			// A compliant turn pays zero tool_choice change; only the escalated
			// turn forces a choice, and only for the single request below. The
			// policy owns that choice: a staged preview names `write`, while the
			// plan gate requires any tool call.
			let mut tool_choice = match (&soft_req, soft.force_next) {
				(Some(requirement), true) => {
					soft.force_next = false;
					self.policy.as_ref().map_or(
						ToolChoice::Tool { name: requirement.tool_name.clone() },
						|policy| policy.choice(requirement),
					)
				},
				_ => ToolChoice::Auto,
			};
			if continuation_pending {
				continuation_pending = false;
				if let (Some(policy), Some(requirement)) = (self.policy.as_ref(), soft_req.as_ref())
				{
					// One-shot upstream `required`: consuming it also spends the
					// policy's escalation budget so an ignored reply yields.
					tool_choice = policy.choice(requirement);
					soft.escalations = policy.escalation_limit(requirement);
					soft.force_next = false;
				}
			}
			let mut pending_injections = Vec::new();
			let mut assistant = match self
				.next_assistant(
					&active_provider,
					&state.messages,
					&schemas,
					&tool_choice,
					events.as_ref(),
					&cancellation_token,
					steering_rx.as_deref_mut(),
					&mut pending_injections,
				)
				.await
			{
				Ok(assistant) => assistant,
				Err(error) => {
					// The hub receipted these sends as `injected`; a turn that dies
					// mid-request must still hand them to the caller's state instead
					// of dropping them with this frame.
					for parts in pending_injections {
						announce_user_steer(&events, &parts).await;
						state.messages.push(Message::user_with_parts(parts));
					}
					return Err(error);
				},
			};
			if let Some(guard) = &self.assistant_guard {
				guard.guard(&mut assistant, &state.messages);
			}
			let turn_start = state.messages.len();
			state.messages.push(assistant.clone());
			state.total_turns += 1;

			// A turn is compliant only when it calls the required tool and
			// nothing else, mirroring the forced request that can emit only
			// that tool. A required+detour batch is non-compliant so detour
			// side effects never run while the preview is still pending.
			let soft_non_compliant = soft_req.as_ref().is_some_and(|requirement| {
				assistant.tool_calls.is_empty()
					|| !assistant.tool_calls.iter().all(|call| {
						self
							.policy
							.as_ref()
							.is_some_and(|policy| policy.satisfies(requirement, call))
					})
			});

			if let Some(events_sink) = events.as_ref() {
				let status = assistant_status(
					&assistant,
					!assistant.tool_calls.is_empty() || soft_non_compliant,
				);
				let _ = events_sink
					.send(AgentEvent::MessageEnd { message: assistant.clone(), status })
					.await;
			}

			// TS `agent-loop.ts`: a terminal provider turn never runs tools.
			// `error`/`aborted` still pair every call with a synthetic result so
			// history stays answerable, but no side effect runs and no further
			// provider request is issued.
			if matches!(assistant.stop_reason, Some(StopReason::Error | StopReason::Aborted)) {
				let reason = if assistant.stop_reason == Some(StopReason::Aborted) {
					"aborted"
				} else {
					"error"
				};
				let results = pair_skipped_tool_calls(
					&assistant,
					reason,
					assistant.error_message.as_deref(),
					events.as_ref(),
					&mut state.messages,
				)
				.await;
				let ack_rx =
					self.begin_turn_ack(turn_start, state.messages.len() + pending_injections.len());
				if let Some(events_sink) = events.as_ref() {
					let _ = events_sink
						.send(AgentEvent::TurnEnd {
							message: assistant.clone(),
							tool_results: results.clone(),
							status: assistant_status(&assistant, false),
						})
						.await;
				}
				for parts in pending_injections {
					announce_user_steer(&events, &parts).await;
					state.messages.push(Message::user_with_parts(parts));
				}
				let decision = self.await_turn_ack(ack_rx, &cancellation_token).await;
				// A terminal provider turn ends the dialog: journaling/rewind still
				// apply, but a provider swap or guidance must not start another turn.
				let TurnAckDecision { rewrite, warning, .. } = decision;
				let _ = self.apply_turn_rewrite(&mut state.messages, rewrite, warning);
				turn_results.push(TurnResult {
					message: assistant,
					tool_results: results,
					is_completed: true,
				});
				return Ok(turn_results);
			}

			if soft_non_compliant
				&& let Some(requirement) = &soft_req
			{
				let limit = self
					.policy
					.as_ref()
					.map_or(MAX_SOFT_TOOL_ESCALATIONS, |policy| {
						policy.escalation_limit(requirement)
					});
				let exhausted = soft.escalations >= limit;
				let yield_to_user = exhausted
					&& assistant.tool_calls.is_empty()
					&& self.policy.as_ref().is_some_and(|policy| {
						policy.exhaustion(requirement) == Exhaustion::EndTurn
					});
				if yield_to_user {
					// The policy asks for a normal hand-back instead of an abort.
					// Dropping the head lets the end-turn path below run unchanged.
					soft.escalations = 0;
					soft.force_next = false;
				} else {
					if exhausted {
						// The assistant turn is already in `state`: pair every call
						// with a synthetic result (a replay must not see a dangling
						// tool call) and keep the asides this request absorbed, whose
						// `injected` receipts are already out, before aborting.
						pair_skipped_tool_calls(
							&assistant,
							"requirement",
							None,
							events.as_ref(),
							&mut state.messages,
						)
						.await;
						for parts in pending_injections {
							announce_user_steer(&events, &parts).await;
							state.messages.push(Message::user_with_parts(parts));
						}
						return Err(AgentError::Internal(format!(
							"Soft tool requirement '{}' was not satisfied after \
							 {limit} forced turns; aborting to avoid an unbounded force loop.",
							requirement.tool_name
						)));
					}
					let notice = format!(
					"Not executed: call the `{}` tool to resolve the pending action before using other tools.",
					requirement.tool_name
				);
				let mut results = Vec::with_capacity(assistant.tool_calls.len());
				for call in &assistant.tool_calls {
					if let Some(events_sink) = events.as_ref() {
						let _ = events_sink
							.send(AgentEvent::ToolStart {
								id: call.id.clone(),
								name: call.name.clone(),
								arguments: call.arguments.clone(),
							})
							.await;
						let _ = events_sink
							.send(AgentEvent::ToolEnd {
								id: call.id.clone(),
								name: call.name.clone(),
								result: notice.clone(),
								is_error: true,
								images: Vec::new(),
								details: None,
							})
							.await;
					}
					let result = ToolResult {
						tool_call_id: call.id.clone(),
						content: notice.clone(),
						is_error: true,
						terminate: false,
						details: None,
					};
					state.messages.push(Message::tool_with_status(
						&result.tool_call_id,
						&call.name,
						&result.content,
						true,
					));
					results.push(result);
				}
				soft.force_next = true;
				soft.escalations += 1;
				// The ack must exist before `TurnEnd` is published: the consumer
				// takes it while handling that event and the loop would wait
				// forever on a registration that came later. The boundary already
				// counts the injections journaled below, in this same block.
				let ack_rx = self.begin_turn_ack(
					turn_start,
					state.messages.len() + pending_injections.len(),
				);
				if let Some(events_sink) = events.as_ref() {
					let _ = events_sink
						.send(AgentEvent::TurnEnd {
							message: assistant.clone(),
							tool_results: results.clone(),
							status: assistant_status(&assistant, true),
						})
						.await;
				}
				// After `TurnEnd`, for the same reason as the end-turn path: the
				// assistant/tool tail is journaled first, the injected user chains
				// behind it.
				for parts in pending_injections {
					announce_user_steer(&events, &parts).await;
					state.messages.push(Message::user_with_parts(parts));
				}
				let decision = self.await_turn_ack(ack_rx, &cancellation_token).await;
				let TurnAckDecision {
					rewrite,
					warning,
					continue_turn: _,
					guidance,
					scrub_custom_type,
					provider,
					context_limit: replacement_context_limit,
					tool_selection: replacement_tools,
				} = decision;
				let _ = self.apply_turn_rewrite(&mut state.messages, rewrite, warning);
				self.apply_turn_tools(replacement_tools);
				apply_turn_boundary(
					&mut state.messages,
					&mut active_provider,
					&mut context_limit,
					guidance,
					scrub_custom_type,
					provider,
					replacement_context_limit,
				);
				turn_results.push(TurnResult {
					message: assistant,
					tool_results: results,
					is_completed: false,
				});
					continue;
				}
			}

			// A `length` truncation cannot be replayed: the trailing tool_use
			// arguments may be incomplete. Pair each call with a synthetic
			// skipped result and re-sample, exactly like TS `agent-loop.ts`.
			if assistant.stop_reason == Some(StopReason::Length)
				&& !assistant.tool_calls.is_empty()
			{
				let results = pair_skipped_tool_calls(
					&assistant,
					"length",
					None,
					events.as_ref(),
					&mut state.messages,
				)
				.await;
				let ack_rx =
					self.begin_turn_ack(turn_start, state.messages.len() + pending_injections.len());
				if let Some(events_sink) = events.as_ref() {
					let _ = events_sink
						.send(AgentEvent::TurnEnd {
							message: assistant.clone(),
							tool_results: results.clone(),
							status: assistant_status(&assistant, true),
						})
						.await;
				}
				for parts in pending_injections {
					announce_user_steer(&events, &parts).await;
					state.messages.push(Message::user_with_parts(parts));
				}
				let decision = self.await_turn_ack(ack_rx, &cancellation_token).await;
				let TurnAckDecision {
					rewrite,
					warning,
					continue_turn: _,
					guidance,
					scrub_custom_type,
					provider,
					context_limit: replacement_context_limit,
					tool_selection: replacement_tools,
				} = decision;
				let _ = self.apply_turn_rewrite(&mut state.messages, rewrite, warning);
				self.apply_turn_tools(replacement_tools);
				apply_turn_boundary(
					&mut state.messages,
					&mut active_provider,
					&mut context_limit,
					guidance,
					scrub_custom_type,
					provider,
					replacement_context_limit,
				);
				turn_results.push(TurnResult {
					message: assistant,
					tool_results: results,
					is_completed: false,
				});
				continue;
			}

			if assistant.tool_calls.is_empty() {
				// Register the ack before publishing `TurnEnd` (the consumer takes
				// it there), and count the injections this block journals next so
				// the boundary is final when the host sees it.
				let ack_rx = self.begin_turn_ack(
					turn_start,
					state.messages.len() + pending_injections.len(),
				);
				// The assistant entry reaches the journal through `TurnEnd`, so it
				// is emitted before the injected users: an accepted steering message
				// must chain after the message it interrupted, not before it.
				if let Some(events_sink) = events.as_ref() {
					let _ = events_sink
						.send(AgentEvent::TurnEnd {
							message: assistant.clone(),
							tool_results: Vec::new(),
							status: assistant_status(&assistant, false),
						})
						.await;
				}
				for parts in pending_injections {
					announce_user_steer(&events, &parts).await;
					state.messages.push(Message::user_with_parts(parts));
				}
				let decision = self.await_turn_ack(ack_rx, &cancellation_token).await;
				let TurnAckDecision {
					rewrite,
					warning,
					continue_turn,
					guidance,
					scrub_custom_type,
					provider,
					context_limit: replacement_context_limit,
					tool_selection: replacement_tools,
				} = decision;
				let mut continue_turn = continue_turn;
				continue_turn |= self.apply_turn_rewrite(&mut state.messages, rewrite, warning);
				self.apply_turn_tools(replacement_tools);
				apply_turn_boundary(
					&mut state.messages,
					&mut active_provider,
					&mut context_limit,
					guidance,
					scrub_custom_type,
					provider,
					replacement_context_limit,
				);
				turn_results.push(TurnResult {
					message: assistant,
					tool_results: Vec::new(),
					is_completed: !continue_turn,
				});
				if continue_turn && !cancellation_token.is_cancelled() {
					continue;
				}
				return Ok(turn_results);
			}

			if let Some(events_sink) = events.as_ref() {
				for call in &assistant.tool_calls {
					let _ = events_sink
						.send(AgentEvent::ToolStart {
							id: call.id.clone(),
							name: call.name.clone(),
							arguments: call.arguments.clone(),
						})
						.await;
				}
			}

			let mut outcome = self
				.run_tools(&assistant.tool_calls, &ctx, &cancellation_token, steering_rx.as_deref_mut())
				.await;
			let image_parts = tool_image_parts(&mut outcome.results);

			if let Some(events_sink) = events.as_ref() {
				let tool_names: std::collections::HashMap<&str, &str> = assistant
					.tool_calls
					.iter()
					.map(|c| (c.id.as_str(), c.name.as_str()))
					.collect();
				for result in &outcome.results {
					let name = tool_names
						.get(result.tool_call_id.as_str())
						.copied()
						.unwrap_or("tool");
					let _ = events_sink
						.send(AgentEvent::ToolEnd {
							id: result.tool_call_id.clone(),
							name: name.to_string(),
							result: result.content.clone(),
							is_error: result.is_error,
							images: image_parts
								.get(&result.tool_call_id)
								.cloned()
								.unwrap_or_default(),
							details: result.details.clone(),
						})
						.await;
				}
			}

			let tool_names: std::collections::HashMap<&str, &str> = assistant
				.tool_calls
				.iter()
				.map(|c| (c.id.as_str(), c.name.as_str()))
				.collect();
			for result in &outcome.results {
				let name = tool_names.get(result.tool_call_id.as_str()).copied().unwrap_or("tool");
				match image_parts.get(&result.tool_call_id) {
					Some(parts) => state.messages.push(Message::tool_with_status_and_parts(
						&result.tool_call_id,
						name,
						&result.content,
						parts.clone(),
						result.is_error,
					)),
					None => state.messages.push(Message::tool_with_status(
						&result.tool_call_id,
						name,
						&result.content,
						result.is_error,
					)),
				}
			}
			// A tool may cancel the turn itself (interactive `ask` cancel/abort).
			// TS ToolAbortError interrupts the whole turn; never issue another
			// provider request merely because the tool returned an Aborted result.
			let cancelled = outcome.interrupted || cancellation_token.is_cancelled();
			let status = if cancelled {
				TurnStatus::Aborted
			} else {
				assistant_status(&assistant, true)
			};

			// Ack first (the consumer takes it on `TurnEnd`), with the boundary
			// already counting the injections journaled right after it.
			let injection_count = pending_injections.len() + outcome.injections.len();
			let ack_rx =
				self.begin_turn_ack(turn_start, state.messages.len() + injection_count);
			if let Some(events_sink) = events.as_ref() {
				let _ = events_sink
					.send(AgentEvent::TurnEnd {
						message: assistant.clone(),
						tool_results: outcome.results.clone(),
						status,
					})
					.await;
			}
			// Injected messages land after the tool responses and after the turn
			// tail is journaled, so the history stays valid and the accepted user
			// chains behind the messages it interrupted.
			for parts in pending_injections.into_iter().chain(outcome.injections) {
				announce_user_steer(&events, &parts).await;
				state.messages.push(Message::user_with_parts(parts));
			}
			let decision = self.await_turn_ack(ack_rx, &cancellation_token).await;
			let TurnAckDecision {
				rewrite,
				warning,
				continue_turn: _,
				guidance,
				scrub_custom_type,
				provider,
				context_limit: replacement_context_limit,
				tool_selection: replacement_tools,
			} = decision;
			let _ = self.apply_turn_rewrite(&mut state.messages, rewrite, warning);
			self.apply_turn_tools(replacement_tools);
			apply_turn_boundary(
				&mut state.messages,
				&mut active_provider,
				&mut context_limit,
				guidance,
				scrub_custom_type,
				provider,
				replacement_context_limit,
			);

			let should_terminate = outcome.results.iter().any(|r| r.terminate);

			turn_results.push(TurnResult {
				message: assistant,
				tool_results: outcome.results,
				is_completed: should_terminate,
			});

			if cancelled {
				cancellation_token.cancel();
				return Err(AgentError::Interrupted);
			}

			if should_terminate {
				return Ok(turn_results);
			}


		}

		Err(AgentError::MaxTurnsExceeded(self.config.max_turns))
	}

	/// Execute one tool batch, honouring steering while it runs.
	///
	/// Results come back in call order, one per call: a call that produced no
	/// result (interrupted batch) gets a synthetic one so history never has an
	/// orphan `tool_call`.
	async fn run_tools(
		&self,
		calls: &[ToolCall],
		ctx: &AgentContext,
		cancellation_token: &CancellationToken,
		mut steering_rx: Option<&mut mpsc::Receiver<SteeringEvent>>,
	) -> BatchOutcome {
		let execute = self.scheduler.execute_batch(calls, ctx);
		tokio::pin!(execute);

		let mut outcome =
			BatchOutcome { results: Vec::new(), injections: Vec::new(), interrupted: false };
		let mut finished = false;
		let mut rx_open = steering_rx.is_some();

		while !finished {
			match steering_rx.as_deref_mut() {
				Some(rx) if rx_open => {
					tokio::select! {
						batch = execute.as_mut() => {
							outcome.results = batch;
							finished = true;
						},
						event = rx.recv() => match event {
							Some(SteeringEvent::InjectMessage(message)) => outcome.injections.push(message),
							Some(SteeringEvent::Interrupt) => outcome.interrupted = true,
							None => rx_open = false,
						},
					}
				},
				_ => {
					outcome.results = execute.as_mut().await;
					finished = true;
				},
			}

			if outcome.interrupted && !finished {
				// Abort in-flight work, then collect whatever the tools reported.
				cancellation_token.cancel();
				outcome.results = execute.as_mut().await;
				finished = true;
			}
		}

		let mut ordered = Vec::with_capacity(calls.len());
		for call in calls {
			match outcome
				.results
				.iter()
				.find(|result| result.tool_call_id == call.id)
			{
				Some(result) => ordered.push(result.clone()),
				None => ordered.push(ToolResult {
					tool_call_id: call.id.clone(),
					content: INTERRUPTED_TOOL_RESULT.to_string(),
					is_error: true,
					terminate: false,
					details: None,
				}),
			}
		}
		outcome.results = ordered;
		outcome
	}

	/// Stamp the provider's configured source route on a produced message.
	///
	/// Signed/redacted reasoning is only replayable to the provider/model that
	/// issued it, so the route must travel with the message instead of being
	/// inferred later from a session default. The provider is the dialog-local
	/// active one, not necessarily the one the loop was constructed with.
	fn with_source(provider: &Arc<dyn ModelProvider>, mut message: Message) -> Message {
		if message.api.is_none() {
			message.api = provider.api().map(str::to_string);
		}
		if message.provider.is_none() {
			message.provider = provider.provider().map(str::to_string);
		}
		if message.model.is_none() {
			message.model = provider.model().map(str::to_string);
		}
		message
	}

	/// Ask the provider for one assistant turn.
	///
	/// With an event sink the turn is streamed, so text reaches the consumer
	/// before the turn finishes; without one it is a single completion request
	/// and nothing but the final message is produced.
	async fn next_assistant(
		&self,
		provider: &Arc<dyn ModelProvider>,
		messages: &[Message],
		schemas: &[serde_json::Value],
		tool_choice: &ToolChoice,
		events: Option<&mpsc::Sender<AgentEvent>>,
		cancellation_token: &CancellationToken,
		mut steering_rx: Option<&mut mpsc::Receiver<SteeringEvent>>,
		injections: &mut Vec<Vec<ContentPart>>,
	) -> Result<Message, AgentError> {
		if cancellation_token.is_cancelled() {
			return Err(AgentError::Interrupted);
		}
		let Some(events) = events else {
			let complete_fut = provider.complete(messages, schemas, tool_choice);
			tokio::pin!(complete_fut);
			loop {
				match steering_rx.as_deref_mut() {
					Some(rx) => {
						tokio::select! {
							_ = cancellation_token.cancelled() => return Err(AgentError::Interrupted),
							event = rx.recv() => match event {
								Some(SteeringEvent::Interrupt) => {
									cancellation_token.cancel();
									return Err(AgentError::Interrupted);
								}
								Some(SteeringEvent::InjectMessage(msg)) => {
									injections.push(msg);
								}
								None => {
									tokio::select! {
										_ = cancellation_token.cancelled() => return Err(AgentError::Interrupted),
										res = &mut complete_fut => return Ok(Self::with_source(provider, res?)),
									}
								}
							},
							res = &mut complete_fut => return Ok(Self::with_source(provider, res?)),
						}
					}
					None => {
						tokio::select! {
							_ = cancellation_token.cancelled() => return Err(AgentError::Interrupted),
							res = &mut complete_fut => return Ok(Self::with_source(provider, res?)),
						}
					}
				}
			}
		};

		let stream_fut = provider.stream(messages, schemas, tool_choice);
		tokio::pin!(stream_fut);
		let mut stream = loop {
			match steering_rx.as_deref_mut() {
				Some(rx) => {
					tokio::select! {
						_ = cancellation_token.cancelled() => {
							let partial = Message::assistant("");
							let _ = events.send(AgentEvent::MessageEnd { message: partial.clone(), status: TurnStatus::Aborted }).await;
							let _ = events.send(AgentEvent::TurnEnd {
								message: partial,
								tool_results: Vec::new(),
								status: TurnStatus::Aborted,
							}).await;
							return Err(AgentError::Interrupted);
						},
						event = rx.recv() => match event {
							Some(SteeringEvent::Interrupt) => {
								cancellation_token.cancel();
								let partial = Message::assistant("");
								let _ = events.send(AgentEvent::MessageEnd { message: partial.clone(), status: TurnStatus::Aborted }).await;
								let _ = events.send(AgentEvent::TurnEnd {
									message: partial,
									tool_results: Vec::new(),
									status: TurnStatus::Aborted,
								}).await;
								return Err(AgentError::Interrupted);
							}
							Some(SteeringEvent::InjectMessage(msg)) => {
								injections.push(msg);
							}
							None => {
								tokio::select! {
									_ = cancellation_token.cancelled() => return Err(AgentError::Interrupted),
									res = &mut stream_fut => break res?,
								}
							}
						},
						res = &mut stream_fut => break res?,
					}
				}
				None => {
					tokio::select! {
						_ = cancellation_token.cancelled() => {
							let partial = Message::assistant("");
							let _ = events.send(AgentEvent::MessageEnd { message: partial.clone(), status: TurnStatus::Aborted }).await;
							let _ = events.send(AgentEvent::TurnEnd {
								message: partial,
								tool_results: Vec::new(),
								status: TurnStatus::Aborted,
							}).await;
							return Err(AgentError::Interrupted);
						},
						res = &mut stream_fut => break res?,
					}
				}
			}
		};

		let mut assembler = StreamAssembler::new();
		let mut done = false;
		let mut rx_open = steering_rx.is_some();

		while !done {
			let next_chunk = match steering_rx.as_deref_mut() {
				Some(rx) if rx_open => {
					tokio::select! {
						_ = cancellation_token.cancelled() => {
							let partial = Self::with_source(provider, assembler.finish_partial());
							let _ = events.send(AgentEvent::MessageEnd { message: partial.clone(), status: TurnStatus::Aborted }).await;
							let _ = events.send(AgentEvent::TurnEnd {
								message: partial,
								tool_results: Vec::new(),
								status: TurnStatus::Aborted,
							}).await;
							return Err(AgentError::Interrupted);
						},
						event = rx.recv() => match event {
							Some(SteeringEvent::Interrupt) => {
								cancellation_token.cancel();
								let partial = Self::with_source(provider, assembler.finish_partial());
								let _ = events.send(AgentEvent::MessageEnd { message: partial.clone(), status: TurnStatus::Aborted }).await;
								let _ = events.send(AgentEvent::TurnEnd {
									message: partial,
									tool_results: Vec::new(),
									status: TurnStatus::Aborted,
								}).await;
								return Err(AgentError::Interrupted);
							}
							Some(SteeringEvent::InjectMessage(msg)) => {
								injections.push(msg);
								continue;
							}
							None => {
								rx_open = false;
								continue;
							}
						},
						chunk = stream.next() => chunk,
					}
				}
				_ => {
					tokio::select! {
						_ = cancellation_token.cancelled() => {
							let partial = Self::with_source(provider, assembler.finish_partial());
							let _ = events.send(AgentEvent::MessageEnd { message: partial.clone(), status: TurnStatus::Aborted }).await;
							let _ = events.send(AgentEvent::TurnEnd {
								message: partial,
								tool_results: Vec::new(),
								status: TurnStatus::Aborted,
							}).await;
							return Err(AgentError::Interrupted);
						},
						chunk = stream.next() => chunk,
					}
				}
			};

			let Some(chunk) = next_chunk else {
				break;
			};
			let chunk = match chunk {
				Ok(c) => c,
				Err(err) => {
					let partial = Self::with_source(provider, assembler.finish_partial());
					let _ = events.send(AgentEvent::MessageEnd { message: partial.clone(), status: TurnStatus::Error(err.to_string()) }).await;
					let _ = events.send(AgentEvent::TurnEnd {
						message: partial,
						tool_results: Vec::new(),
						status: TurnStatus::Error(err.to_string()),
					}).await;
					return Err(err.into());
				}
			};
			// TS `openai-completions.ts:1160-1192` handles a chunk's reasoning
			// before its content, so reasoning-first here keeps block order and
			// `contentIndex` order identical for a chunk that carries both.
			if let Some(delta) = &chunk.reasoning
				&& !delta.is_empty()
			{
				let _ = events
					.send(AgentEvent::ThinkingUpdate { delta: delta.clone() })
					.await;
			}
			if let Some(delta) = &chunk.content
				&& !delta.is_empty()
			{
				let _ = events
					.send(AgentEvent::MessageUpdate { delta: delta.clone() })
					.await;
			}
			done = chunk.is_done;
			if let Err(err) = assembler.push(&chunk) {
				let partial = Self::with_source(provider, assembler.finish_partial());
				let _ = events.send(AgentEvent::MessageEnd { message: partial.clone(), status: TurnStatus::Error(err.to_string()) }).await;
				let _ = events.send(AgentEvent::TurnEnd {
					message: partial,
					tool_results: Vec::new(),
					status: TurnStatus::Error(err.to_string()),
				}).await;
				return Err(err.into());
			}
			if done {
				break;
			}
		}

		if !done {
			let partial = Self::with_source(provider, assembler.finish_partial());
			let err_msg = "stream ended before completion".to_string();
			let _ = events.send(AgentEvent::MessageEnd { message: partial.clone(), status: TurnStatus::Error(err_msg.clone()) }).await;
			let _ = events.send(AgentEvent::TurnEnd {
				message: partial,
				tool_results: Vec::new(),
				status: TurnStatus::Error(err_msg.clone()),
			}).await;
			return Err(ProviderError::Protocol(err_msg).into());
		}
		let partial = Self::with_source(provider, assembler.finish_partial());
		let message = match assembler.finish() {
			Ok(message) => Self::with_source(provider, message),
			Err(err) => {
				let _ = events.send(AgentEvent::MessageEnd { message: partial.clone(), status: TurnStatus::Error(err.to_string()) }).await;
				let _ = events.send(AgentEvent::TurnEnd {
					message: partial,
					tool_results: Vec::new(),
					status: TurnStatus::Error(err.to_string()),
				}).await;
				return Err(err.into());
			}
		};
		Ok(message)
	}
}

#[cfg(test)]
mod live_cut_tests {
	use super::*;
	use crate::state::ToolCall;

	fn call(id: &str, name: &str) -> ToolCall {
		ToolCall {
			id: id.to_string(),
			name: name.to_string(),
			arguments: serde_json::json!({}),
		}
	}

	#[test]
	fn anchor_cuts_after_checkpoint_ignoring_stale_keep() {
		let mut messages = vec![
			Message::system("sys"),
			Message::user("investigate"),
			Message::assistant_with_tools("", vec![call("cp", "checkpoint")]),
			Message::tool_with_status("cp", "checkpoint", "Checkpoint: map it", false),
			Message::user("injected boundary"),
			Message::assistant("steering answer"),
			Message::user("wrap up"),
			Message::assistant_with_tools("", vec![call("rw", "rewind")]),
			Message::tool_with_status("rw", "rewind", "Rewind requested.", false),
		];
		let rewind = InMemoryRewind {
			keep: 7,
			checkpoint_tool_call_id: Some("cp".to_string()),
			summary: "Summary".to_string(),
			report: "Report".to_string(),
		};
		apply_in_memory_cut(&mut messages, &rewind);
		let text = messages
			.iter()
			.map(|message| message.content.as_str())
			.collect::<Vec<_>>()
			.join("\n");
		assert!(text.contains("Checkpoint: map it"));
		assert!(text.contains("Report"));
		assert!(!text.contains("injected"));
		assert!(!text.contains("steering"));
		assert_eq!(messages.last().expect("report").role, Role::System);
	}

	#[test]
	fn missing_anchor_fallback_does_not_split_tool_batch() {
		let mut messages = vec![
			Message::system("sys"),
			Message::user("investigate"),
			Message::assistant_with_tools("", vec![call("a", "read")]),
			Message::tool_with_status("a", "read", "first", false),
			Message::tool_with_status("a", "read", "second", false),
		];
		let rewind = InMemoryRewind {
			keep: 3,
			checkpoint_tool_call_id: Some("compacted-away".to_string()),
			summary: String::new(),
			report: String::new(),
		};
		apply_in_memory_cut(&mut messages, &rewind);
		assert_eq!(messages.len(), 5, "whole tool batch retained");
		assert_eq!(messages[2].tool_calls[0].id, "a");
		assert_eq!(messages[3].tool_call_id.as_deref(), Some("a"));
		assert_eq!(messages[4].tool_call_id.as_deref(), Some("a"));
	}

	#[test]
	fn missing_anchor_fallback_drops_dangling_trailing_call() {
		let mut messages = vec![
			Message::system("sys"),
			Message::user("investigate"),
			Message::assistant_with_tools("", vec![call("x", "read")]),
		];
		let rewind = InMemoryRewind {
			keep: 3,
			checkpoint_tool_call_id: Some("compacted-away".to_string()),
			summary: String::new(),
			report: String::new(),
		};
		apply_in_memory_cut(&mut messages, &rewind);
		assert_eq!(messages.len(), 2);
		assert_eq!(messages.last().expect("user").role, Role::User);
	}

	#[test]
	fn anchored_cut_keeps_sibling_results_of_checkpoint_batch() {
		let mut messages = vec![
			Message::system("sys"),
			Message::user("investigate"),
			Message::assistant_with_tools(
				"",
				vec![call("cp", "checkpoint"), call("read", "read")],
			),
			Message::tool_with_status("cp", "checkpoint", "Checkpoint: map it", false),
			Message::tool_with_status("read", "read", "sibling contents", false),
			Message::assistant("secret exploration"),
		];
		let rewind = InMemoryRewind {
			keep: 3,
			checkpoint_tool_call_id: Some("cp".to_string()),
			summary: "Retained summary".to_string(),
			report: "Retained report".to_string(),
		};
		apply_in_memory_cut(&mut messages, &rewind);
		let text = messages
			.iter()
			.map(|message| message.content.as_str())
			.collect::<Vec<_>>()
			.join("\n");
		assert!(text.contains("sibling contents"), "sibling result kept");
		assert!(text.contains("Checkpoint: map it"));
		assert!(!text.contains("secret"), "later exploration cut");
		assert_eq!(messages.last().expect("report").content, "Retained report");
	}

	#[test]
	fn compacted_away_anchor_replaced_from_mirror_in_current_run() {
		// The owner's uncompacted retained branch (mirror after its cut).
		let retained = vec![
			Message::user("investigate"),
			Message::assistant_with_tools("", vec![call("cp", "checkpoint")]),
			Message::tool_with_status("cp", "checkpoint", "Checkpoint: map it", false),
			Message::user("Retained summary"),
			Message::system("Retained report"),
		];
		// Compaction pruned the checkpoint result: no live anchor remains.
		let mut live = vec![
			Message::system("sys"),
			Message::system("[System notice: earlier turns pruned]"),
			Message::user("exploration"),
			Message::assistant("secret exploration"),
		];
		assert!(
			live.iter()
				.all(|message| message.tool_call_id.as_deref() != Some("cp"))
		);
		let decision = TurnAckDecision {
			rewrite: Some(TurnRewrite::Replace(retained.clone())),
			..TurnAckDecision::default()
		};
		assert!(apply_turn_decision(&mut live, decision.rewrite, decision.warning));
		assert_eq!(live.first().expect("system").content, "sys");
		let text = live
			.iter()
			.map(|message| message.content.as_str())
			.collect::<Vec<_>>()
			.join("\n");
		assert!(text.contains("Checkpoint: map it"));
		assert!(text.contains("Retained report"));
		assert!(!text.contains("exploration"));
		assert!(!text.contains("secret"));
	}

	#[test]
	fn tool_images_are_validated_and_invalid_payloads_fail_closed() {
		let mut results = vec![
			ToolResult {
				tool_call_id: "ok".to_string(),
				content: "one image".to_string(),
				is_error: false,
				terminate: false,
				details: Some(serde_json::json!({
					"images": [{
						"type": "image",
						"data": "iVBORw0KGgo=",
						"mimeType": "image/png",
					}],
				})),
			},
			ToolResult {
				tool_call_id: "bad".to_string(),
				content: "broken".to_string(),
				is_error: false,
				terminate: false,
				details: Some(serde_json::json!({
					"images": [{ "data": "not base64!!", "mimeType": "image/png" }],
				})),
			},
		];
		let parts = tool_image_parts(&mut results);
		assert_eq!(parts.len(), 1);
		assert_eq!(parts["ok"].len(), 1);
		assert!(parts["ok"][0].is_image());
		assert!(!results[0].is_error);
		assert!(results[1].is_error, "invalid image fails closed");
		assert!(results[1].content.contains("image payload rejected"));
		assert!(!parts.contains_key("bad"));
	}

	#[test]
	fn turn_boundary_scrubs_consumed_guidance_then_injects_and_swaps_provider() {
		let first: Arc<dyn ModelProvider> = Arc::new(
			crate::provider::OpenAiCompatibleProvider::new(
				"http://127.0.0.1:9/v1",
				"key",
				"model-a",
			)
			.expect("first provider"),
		);
		let replacement: Arc<dyn ModelProvider> = Arc::new(
			crate::provider::OpenAiCompatibleProvider::new(
				"http://127.0.0.1:9/v1",
				"key",
				"model-b",
			)
			.expect("replacement provider"),
		);
		let mut active = Arc::clone(&first);
		let mut context_limit = 100_000;
		let mut messages = vec![
			Message::user("go"),
			Message::guidance("prewalk-plan", "consumed nudge"),
			Message::guidance("prewalk-continue", "keep me"),
		];
		apply_turn_boundary(
			&mut messages,
			&mut active,
			&mut context_limit,
			vec![
				Message::guidance("prewalk-checklist", "checklist"),
				Message::guidance("prewalk-plan", "fresh nudge"),
			],
			Some("prewalk-plan".to_string()),
			Some(Arc::clone(&replacement)),
			Some(32_000),
		);
		assert_eq!(context_limit, 32_000, "replacement budget follows the provider");
		assert!(Arc::ptr_eq(&active, &replacement), "dialog switched provider");
		assert!(!Arc::ptr_eq(&active, &first));
		let identities: Vec<Option<&str>> = messages
			.iter()
			.map(|message| message.custom_type.as_deref())
			.collect();
		assert_eq!(
			identities,
			vec![None, Some("prewalk-continue"), Some("prewalk-checklist"), Some("prewalk-plan")],
			"scrub precedes injection, so a refreshed identity survives"
		);
		assert_eq!(messages.last().expect("fresh nudge").content, "fresh nudge");
	}
}

#[cfg(test)]
mod steering_parts_tests {
	use super::*;

	/// A steering message must reach the model chat with its exact parts: an
	/// image is never flattened to text or dropped at the channel boundary.
	#[test]
	fn injected_message_keeps_text_and_image_parts() {
		let parts = vec![
			ContentPart::text("look at this"),
			ContentPart::image("aW1n", "image/png").expect("valid image part"),
		];
		let (tx, mut rx) = mpsc::channel(4);
		tx.try_send(SteeringEvent::InjectMessage(parts.clone()))
			.expect("steering channel must accept a message");
		let steering = Steering::drain(&mut rx);
		assert!(!steering.interrupted, "an injected message is not an interrupt");
		assert_eq!(steering.injections, vec![parts.clone()]);
		let injected = steering.injections.into_iter().next().expect("one injection");
		let message = Message::user_with_parts(injected);
		assert_eq!(message.parts, parts);
		assert!(message.has_image(), "steering image must survive into the chat");
	}
}
