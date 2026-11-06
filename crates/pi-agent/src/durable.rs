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

use async_trait::async_trait;

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
}
