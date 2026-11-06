//! Host hook for the per-subagent advisor runtime.
//!
//! Mirrors the TypeScript session advisor trigger exactly:
//! `agent-session.ts:1305-1316` registers `setOnTurnEnd`, which skips an
//! aborted signal, applies turn-end side effects, and calls
//! `session-advisors.ts:337-354 onPrimaryTurnEnd(messages, willContinue, signal)`.
//! That method feeds every advisor the completed turn and then waits (30s,
//! `advisor.syncBacklog` threshold) for the backlog.
//!
//! The Rust child loop emits the same boundary as [`AgentEvent::TurnEnd`]
//! (assistant message plus resolved tool results). This hook is the host seam:
//! `task.rs` invokes it at the spawn, at each settled turn (never for an
//! aborted one), and when the child session ends. The callback is synchronous
//! and must schedule its own work; it never starts a hidden primary turn.

use std::path::Path;

use async_trait::async_trait;

use crate::{Message, ToolResult};

/// Identity of the child session owning one advisor runtime.
pub struct ChildAdvisorContext<'a> {
	pub child_id: &'a str,
	pub agent:    Option<&'a str>,
	/// Child JSONL transcript; advisor files are written beside it
	/// (`__advisor[.slug].jsonl`).
	pub session_file:       Option<&'a Path>,
	/// Agent frontmatter `advisor` enabled (`HandoffTarget::Default` or a
	/// `Pattern`); the host applies the `task.agentAdvisor` override first.
	pub definition_advisor: bool,
	/// Explicit frontmatter model pattern, when one was given.
	pub definition_pattern: Option<&'a str>,
	/// Live steering channel for this child; successful `try_send` is the only
	/// evidence that advice reached the next child request.
	pub steer: Option<tokio::sync::mpsc::Sender<crate::SteeringEvent>>,
}

/// One host-owned callback set. Errors are the host's to log; a failing hook
/// never fails the child. Async because the end hook must join its bounded
/// review drain before the host disposes the runtime.
#[async_trait]
pub trait ChildAdvisorHook: Send + Sync {
	/// The child conversation opened with this first user message.
	async fn on_child_start(&self, child: &ChildAdvisorContext<'_>, initial: &Message) {}

	/// One settled child turn, after its tool results resolved. `will_continue`
	/// is the TS `context.willContinue` equivalent.
	async fn on_child_turn(
		&self,
		child: &ChildAdvisorContext<'_>,
		message: &Message,
		tool_results: &[ToolResult],
		will_continue: bool,
	) {
	}

	/// The child session settled. `aborted` skips the review drain; every
	/// terminal path (success, failure, cancellation) must call this.
	async fn on_child_end(&self, child: &ChildAdvisorContext<'_>, aborted: bool) {}
}
