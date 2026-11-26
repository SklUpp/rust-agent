//! Host-injected durability boundary for the agent loop.
//!
//! The loop owns ordering; the host owns storage. Everything here is an
//! ordinary awaited call: there is no acknowledgement protocol, no second
//! journal, and no silent downgrade of a write failure.
//!
//! Contract:
//! - [`DurableBoundary::persist_messages`] must return only after every
//!   complete user/assistant message in the slice is durable. The loop calls it
//!   before a model request and immediately after an assistant turn is
//!   complete, so a failed write stops the run before the next effect.
//! - [`DurableBoundary::begin_execution`] must return the durable execution id
//!   after tool selection/approval and the cancellation recheck, before the
//!   tool effect starts. A failure here means the effect must not run.
//! - [`DurableBoundary::finish_execution`] records the terminal outcome. A
//!   failure after the effect terminates the run; the host must still see the
//!   intent as unresolved and must never replay the effect automatically.
//!
//! Execution ids are store-owned and distinct from provider tool-call ids: a
//! provider may repeat a call id, so the two identities must never be
//! conflated.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::compaction::CompactionReport;
use crate::state::{Message, TokenUsage, ToolCall, ToolResult};

/// Detail marker the scheduler adds to a result whose durability write failed.
/// The loop turns any such result into [`crate::AgentError::Durable`] and stops
/// before the next provider request.
pub const DURABILITY_FAILURE_CODE: &str = "durability_failure";

/// Detail marker for tool admission failure where the effect was not started.
/// A safe error tool result is recorded so reopen/resume does not leave a dangling call.
pub const DURABILITY_ADMISSION_FAILURE_CODE: &str = "durability_admission_failure";

/// Detail marker for a durability write whose outcome is unknown: the write may
/// have committed even though no terminal status was observed. The loop stops
/// without recording a synthetic tool result and never retries the write.
pub const DURABILITY_UNKNOWN_OUTCOME_CODE: &str = "durability_unknown_outcome";

/// Durable write failure. The message is the storage error, never a generic
/// "persistence disabled" substitute.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct DurableError {
	message: String,
	/// A write may have committed although its outcome was never observed
	/// (cancelled/timed-out await, or a writer that stopped before replying).
	/// Callers must not retry such a write automatically.
	unknown_outcome: bool,
}

impl DurableError {
	#[must_use]
	pub fn new(message: impl Into<String>) -> Self {
		Self { message: message.into(), unknown_outcome: false }
	}

	/// A write whose outcome is unknowable: it may have committed. Never retry
	/// it automatically; surface it as an unresolved durable effect instead.
	#[must_use]
	pub fn unknown_outcome(message: impl Into<String>) -> Self {
		Self { message: message.into(), unknown_outcome: true }
	}

	#[must_use]
	pub fn message(&self) -> &str {
		&self.message
	}

	/// True when the write may have committed and its outcome is unknown.
	#[must_use]
	pub fn is_unknown_outcome(&self) -> bool {
		self.unknown_outcome
	}
}

impl From<String> for DurableError {
	fn from(value: String) -> Self {
		Self::new(value)
	}
}

impl From<&str> for DurableError {
	fn from(value: &str) -> Self {
		Self::new(value)
	}
}

/// Host persistence boundary. Implementations must be safe to call from the
/// concurrent tool scheduler: messages are persisted from the loop task, while
/// executions may be reserved and finished from several tool tasks.
#[async_trait]
pub trait DurableBoundary: Send + Sync + std::fmt::Debug {
	/// Persist every complete user/assistant message appended since the last
	/// call. Implementations may track a cursor over the growing history.
	async fn persist_messages(&self, messages: &[Message]) -> Result<(), DurableError>;

	/// Reserve a durable execution identity for `call` before its effect starts.
	async fn begin_execution(&self, call: &ToolCall) -> Result<String, DurableError>;

	/// Remap the persisted-message cursor after a compaction. The report names
	/// the retained tail; implementations must verify it against their own
	/// stable message revision, never guess from lengths.
	async fn compacted(
		&self,
		messages: &[Message],
		report: &CompactionReport,
	) -> Result<(), DurableError>;

	/// The host replaced the live conversation from durable state (checkpoint
	/// rewind / branch cut). Re-anchor the cursor; do not append a second copy.
	async fn resync_messages(&self, messages: &[Message]) -> Result<(), DurableError>;

	/// Record the terminal outcome for `execution_id`.
	async fn finish_execution(
		&self,
		execution_id: &str,
		call: &ToolCall,
		result: &ToolResult,
	) -> Result<(), DurableError>;

	/// Flush a branch-scoped draft of the in-progress assistant text.
	///
	/// `attempt` identifies the stream that produced the text
	/// (`{boot_nanos}-{counter}`, unique per loop boot). A flush whose
	/// attempt differs from the stored current draft preserves that draft as
	/// a prior fragment instead of overwriting it; a completed assistant
	/// message carrying the same attempt clears it atomically. Drafts carry
	/// no tool calls and are never replayed as executable history. Default
	/// no-op for ephemeral boundaries; durable hosts fail closed on write
	/// error.
	async fn persist_partial_assistant(
		&self,
		_text: &str,
		_attempt: &str,
	) -> Result<(), DurableError> {
		Ok(())
	}

	/// Drop the branch-scoped draft without persisting a message, for example
	/// after the completed assistant message cleared it already. Default
	/// no-op; durable hosts fail closed on write error.
	async fn clear_partial_assistant(&self) -> Result<(), DurableError> {
		Ok(())
	}
	/// Amend the active turn's assistant record with its terminal status.
	///
	/// When an assistant message requesting tools is persisted before tool
	/// execution, its initial stop reason is `toolUse`. If the turn terminates
	/// (e.g. cancelled/aborted by SIGINT during tool execution), the host must
	/// amend the pre-execution assistant row with its terminal `stop_reason`
	/// and optional `error_message` so that durable history and replay reflect
	/// the terminal state without losing completed tool effects or intents.
	async fn finish_turn(
		&self,
		stop_reason: &str,
		error_message: Option<&str>,
	) -> Result<(), DurableError>;

	/// Child scopes this root boundary can own. `None` is explicit ephemeral
	/// mode; a boundary backed by a session store returns a real factory.
	fn child_durability_factory(&self) -> Option<Arc<dyn ChildDurabilityFactory>> {
		None
	}

	/// Mark deliveries as durably present in the target conversation. The loop
	/// calls this only after the history append committed. Default no-op for
	/// boundaries that carry no delivery rows.
	async fn mark_deliveries_delivered(
		&self,
		_delivery_ids: &[String],
	) -> Result<(), DurableError> {
		Ok(())
	}

	/// Mark deliveries as consumed by the model. Called only after a provider
	/// request that included them returned a response — never on inbox take.
	async fn mark_deliveries_processed(
		&self,
		_delivery_ids: &[String],
	) -> Result<(), DurableError> {
		Ok(())
	}

	/// Atomically accept pending worker deliveries for the active session, appending
	/// them to durable branch history and marking them delivered in one transaction.
	///
	/// Returns the delivered worker completions so the loop can update in-memory state,
	/// emit events, and track delivery IDs for processed confirmation.
	async fn accept_pending_worker_deliveries(
		&self,
	) -> Result<Vec<DeliveredWorkerResult>, DurableError> {
		Ok(Vec::new())
	}

	/// Delivery IDs that are delivered on the active branch but not yet confirmed
	/// processed by a model response, and whose message is present in `messages`.
	async fn unconfirmed_delivery_ids(
		&self,
		_messages: &[Message],
	) -> Result<Vec<String>, DurableError> {
		Ok(Vec::new())
	}

	/// Atomically append one accepted peer message and mark its delivery row
	/// delivered, in a single transaction. `message` is exactly the message the
	/// loop will append to history, so the next `persist_messages` recognizes
	/// the stored row instead of appending a second copy.
	async fn consume_peer_delivery(
		&self,
		_delivery_id: &str,
		_message: &Message,
	) -> Result<(), DurableError> {
		Ok(())
	}

	/// Replay peer deliveries accepted by the host before `cutoff_ms` but never
	/// appended to the live conversation (process death between accept and
	/// persist). Rows accepted during the current turn stay pending for their
	/// in-memory queue and are not stolen by this drain.
	async fn accept_pending_peer_deliveries(
		&self,
		_cutoff_ms: i64,
	) -> Result<Vec<DeliveredPeerMessage>, DurableError> {
		Ok(Vec::new())
	}

	/// Drop a still-pending delivery whose send failed before acceptance.
	async fn withdraw_pending_delivery(&self, _delivery_id: &str) -> Result<(), DurableError> {
		Ok(())
	}
}

/// One replayed peer delivery. `message` is the exact user message the loop
/// must append; the row is already marked delivered.
#[derive(Debug, Clone)]
pub struct DeliveredPeerMessage {
	pub delivery_id: String,
	pub from: String,
	pub text: String,
	pub message: Message,
}

/// Immutable worker identity captured at spawn. `worker_id` is the spawn-time
/// result key, never a reusable `task-N` display index. `session_id` /
/// `origin_branch_id` name the root scope that owns the parent notification and
/// are frozen here so a later branch cut cannot redirect it.
/// `producer_generation` is the admitting parent's producer epoch at spawn; a
/// persisted contract records it and revive validates it against the parent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerIdentity {
	pub worker_id: String,
	pub session_id: String,
	pub origin_branch_id: String,
	pub producer_generation: u64,
}

/// Host completion writer: the worker completion and its parent delivery commit
/// in one transaction. A configured host must fail closed on error.
pub trait WorkerCompletionSink: Send + Sync + std::fmt::Debug {
	fn complete_worker(
		&self,
		identity: &WorkerIdentity,
		completion: Value,
		delivery: Value,
	) -> Result<(), DurableError>;
}

/// Result of a durably delivered worker completion.
#[derive(Debug, Clone)]
pub struct DeliveredWorkerResult {
	pub delivery_id: String,
	pub worker_id: String,
	pub message: Message,
	pub agent: String,
	pub status: String,
	pub text: String,
	pub duration_ms: u64,
	pub usage: Option<TokenUsage>,
	pub output_id: Option<String>,
	pub result_key: Option<String>,
}

/// Durable child scope handed to one worker loop.
#[derive(Debug, Clone)]
pub struct ChildDurability {
	identity: WorkerIdentity,
	boundary: Arc<dyn DurableBoundary>,
	completion: Arc<dyn WorkerCompletionSink>,
}

impl ChildDurability {
	#[must_use]
	pub fn new(
		identity: WorkerIdentity,
		boundary: Arc<dyn DurableBoundary>,
		completion: Arc<dyn WorkerCompletionSink>,
	) -> Self {
		Self { identity, boundary, completion }
	}

	#[must_use]
	pub fn identity(&self) -> &WorkerIdentity {
		&self.identity
	}

	/// Own saved session, messages, tool intents, and outcomes for this worker.
	#[must_use]
	pub fn boundary(&self) -> Arc<dyn DurableBoundary> {
		Arc::clone(&self.boundary)
	}

	/// Commit completion plus parent notification before the live queue sees it.
	pub fn complete(&self, completion: Value, delivery: Value) -> Result<(), DurableError> {
		self
			.completion
			.complete_worker(&self.identity, completion, delivery)
	}
}


/// Host factory for child scopes. Every method defaults to "unsupported".
pub trait ChildDurabilityFactory: Send + Sync + std::fmt::Debug {
	fn open_child(&self, _worker_id: &str) -> Result<ChildDurability, DurableError> {
		Err(DurableError::new("durable child scope is not implemented"))
	}

	/// Record that a child is about to be created. Must be durable before the
	/// host starts any effect; a failure aborts the spawn. The default keeps
	/// stores that predate task admissions working.
	fn admit_child(&self, _worker_id: &str, _spec: &Value) -> Result<(), DurableError> {
		Ok(())
	}

	/// Record that the child's effect was requested. Called once, after the
	/// durable scope is open and before the first provider request.
	fn start_child(&self, _worker_id: &str) -> Result<(), DurableError> {
		Ok(())
	}

	/// Close an admitted child with a terminal state. Called on settlement and
	/// on every early-abort path; repeating a terminal state is not an error,
	/// and an unknown admission is ignored.
	fn finish_child(&self, _worker_id: &str, _state: &str) -> Result<(), DurableError> {
		Ok(())
	}
    fn open_child_with_replay(&self, worker_id: &str) -> Result<(ChildDurability, Option<Vec<Message>>), DurableError> {
        let child = self.open_child(worker_id)?;
        let replay = self.load_child_messages_fresh(worker_id, &child)?;
        Ok((child, replay))
    }
    fn load_child_messages(&self, _worker_id: &str) -> Result<Option<Vec<Message>>, DurableError> { Ok(None) }

	fn load_child_messages_owned(
		&self,
		_worker_id: &str,
		_child: &ChildDurability,
	) -> Result<Option<Vec<Message>>, DurableError> {
		Err(DurableError::new("durable child replay requires a quiescent owner"))
	}

	/// Prove that a persisted child contract's producer epoch belongs to the
	/// durable parent before reopening it: the parent's recorded epoch must be
	/// at least the contract epoch, and the child session must be parented to
	/// that parent. The default refuses: a store that cannot verify the epoch
	/// must not replay the child.
	fn validate_child_generation(
		&self,
		_worker_id: &str,
		_producer_generation: u64,
	) -> Result<(), DurableError> {
		Err(DurableError::new(
			"durable child producer generation validation is not implemented",
		))
	}

    /// Load replay after this call acquired fresh exclusive child ownership.
    fn load_child_messages_fresh(
        &self,
        worker_id: &str,
        child: &ChildDurability,
    ) -> Result<Option<Vec<Message>>, DurableError> {
        self.load_child_messages_owned(worker_id, child)
    }

	/// Explicit one-time migration of a legacy JSONL child into a durable
	/// scope. A legacy child must be imported before it resumes; otherwise the
	/// caller must refuse to continue it rather than writing both stores.
	fn import_legacy_child(
		&self,
		_worker_id: &str,
		_messages: &[Message],
	) -> Result<ChildDurability, DurableError> {
		Err(DurableError::new("legacy child import is not implemented"))
	}

	/// Persist one peer message into the target agent's durable scope as a
	/// pending delivery. `target_session` is `None` for the root session.
	/// `message_id` is the transport message id and `text` is the exact
	/// model-facing line (`format_incoming`), so a replay renders identically.
	fn enqueue_peer_delivery(
		&self,
		_target_session: Option<&str>,
		_sender: &str,
		_message_id: &str,
		_text: &str,
	) -> Result<String, DurableError> {
		Err(DurableError::new("durable peer delivery is not implemented"))
	}

	/// Mark a peer delivery row delivered and processed synchronously. Used by
	/// `hub wait`/`hub inbox`, where the message leaves the mailbox as a tool
	/// result instead of entering the conversation.
	fn mark_peer_delivery_consumed(&self, _delivery_id: &str) -> Result<(), DurableError> {
		Ok(())
	}

	/// Withdraw a pending peer delivery whose send failed before acceptance.
	fn withdraw_peer_delivery(&self, _delivery_id: &str) -> Result<(), DurableError> {
		Ok(())
	}
}
