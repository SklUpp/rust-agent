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
use crate::state::{Message, ToolCall, ToolResult};

/// Detail marker the scheduler adds to a result whose durability write failed.
/// The loop turns any such result into [`crate::AgentError::Durable`] and stops
/// before the next provider request.
pub const DURABILITY_FAILURE_CODE: &str = "durability_failure";

/// Durable write failure. The message is the storage error, never a generic
/// "persistence disabled" substitute.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DurableError(String);

impl DurableError {
	#[must_use]
	pub fn new(message: impl Into<String>) -> Self {
		Self(message.into())
	}

	#[must_use]
	pub fn message(&self) -> &str {
		&self.0
	}
}

impl From<String> for DurableError {
	fn from(value: String) -> Self {
		Self(value)
	}
}

impl From<&str> for DurableError {
	fn from(value: &str) -> Self {
		Self(value.to_string())
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
}

/// Immutable worker identity captured at spawn. `worker_id` is the spawn-time
/// result key, never a reusable `task-N` display index. `session_id` /
/// `origin_branch_id` name the root scope that owns the parent notification and
/// are frozen here so a later branch cut cannot redirect it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerIdentity {
	pub worker_id: String,
	pub session_id: String,
	pub origin_branch_id: String,
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

/// Host factory for child scopes. Every method defaults to "unsupported": a
/// configured factory that cannot honour the request fails the spawn/revive
/// instead of silently falling back to an ephemeral child.
pub trait ChildDurabilityFactory: Send + Sync + std::fmt::Debug {
	fn open_child(&self, _worker_id: &str) -> Result<ChildDurability, DurableError> {
		Err(DurableError::new("durable child scope is not implemented"))
	}

	/// Durable child conversation for a cold revive. `None` means no durable
	/// child scope exists, so the caller may read a legacy transcript explicitly.
	fn load_child_messages(&self, _worker_id: &str) -> Result<Option<Vec<Message>>, DurableError> {
		Ok(None)
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
	fn enqueue_peer_delivery(
		&self,
		_target_session: Option<&str>,
		_sender: &str,
		_text: &str,
	) -> Result<String, DurableError> {
		Err(DurableError::new("durable peer delivery is not implemented"))
	}
}
