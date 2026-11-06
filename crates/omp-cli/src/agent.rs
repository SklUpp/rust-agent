//! Agent-runner seam.
//!
//! The binary registers the `pi-agent` implementation through [`install`]
//! (`runtime::PiAgentRunner`). A library caller that never registers one gets
//! `None` from [`runner`] and must fail with `agent runtime not linked` — there
//! is no stand-in that fabricates answers.

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use serde::Serialize;

use crate::{
	Result, approve::Approvals, catalog::Catalog, config::Config, output::OutputMode,
	prompts::ContextFile, skills::Skill,
};

pub const MISSING_RUNTIME: &str = "agent runtime not linked";

/// One streamed event of an agent turn.
///
/// Usage accounting rides on the embedded `pi_agent::Message`:
/// [`Event::MessageEnd`] fires exactly once per provider request, so its
/// `message.usage` is that request's report and is the only tick to sum.
/// [`Event::TurnEnd`] and [`Event::AgentEnd`] repeat the same messages (a loop
/// turn may hold several model calls), so accounting must never add them to the
/// `MessageEnd` totals. `usage: None` means the provider reported nothing.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
	MessageStart {
		role: String,
	},
	MessageUpdate {
		delta: String,
	},
	/// Incremental public provider reasoning (TS `thinking_delta`). Kept apart
	/// from [`Event::MessageUpdate`]; only text mode's final render gates it.
	ThinkingUpdate {
		delta: String,
	},
	MessageEnd {
		message: pi_agent::Message,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		stop_reason: Option<String>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		api: Option<String>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		provider: Option<String>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		model: Option<String>,
	},
	ToolStart {
		id: String,
		tool: String,
		args: serde_json::Value,
	},
	/// Partial tool output while it still runs (TS `tool_execution_update`).
	ToolUpdate {
		id: String,
		tool: String,
		args: serde_json::Value,
		partial: serde_json::Value,
	},
	ToolEnd {
		id: String,
		tool: String,
		result: String,
		error: Option<String>,
		/// Validated image parts the provider request carries for this result.
		#[serde(default, skip_serializing_if = "Vec::is_empty")]
		images: Vec<pi_agent::ContentPart>,
		/// Structured result details (TS `AgentToolResult.details`).
		#[serde(default, skip_serializing_if = "Option::is_none")]
		details: Option<serde_json::Value>,
	},
	TurnEnd {
		message: pi_agent::Message,
		tool_results: Vec<pi_agent::ToolResult>,
		stop_reason: String,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		api: Option<String>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		provider: Option<String>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		model: Option<String>,
	},
	AgentEnd {
		messages: Vec<pi_agent::Message>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		api: Option<String>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		provider: Option<String>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		model: Option<String>,
	},
	/// A user message accepted from the steering channel, exactly as it entered
	/// the loop's history. Emitted after acceptance, so the session can persist
	/// it at that position and mirror it on the wire.
	UserSteer {
		parts: Vec<pi_agent::ContentPart>,
	},
}

impl Event {
	pub fn message_end(message: pi_agent::Message) -> Self {
		let stop = if message.tool_calls.is_empty() { "stop" } else { "toolUse" };
		Self::MessageEnd {
			message,
			stop_reason: Some(stop.to_string()),
			api: None,
			provider: None,
			model: None,
		}
	}

	pub fn turn_end(
		message: pi_agent::Message,
		tool_results: Vec<pi_agent::ToolResult>,
		stop_reason: impl Into<String>,
	) -> Self {
		Self::TurnEnd {
			message,
			tool_results,
			stop_reason: stop_reason.into(),
			api: None,
			provider: None,
			model: None,
		}
	}

	pub fn agent_end(messages: Vec<pi_agent::Message>) -> Self {
		Self::AgentEnd { messages, api: None, provider: None, model: None }
	}
}

pub type EventStream = Box<dyn Iterator<Item = Result<Event>> + Send>;

/// Everything one turn needs. The runner owns retries, network and tools; the
/// CLI only resolves files into this request.
#[derive(Debug, Clone)]
pub struct RunRequest {
	pub prompt: String,
	pub parts: Vec<crate::session::ContentPart>,
	pub cwd: std::path::PathBuf,
	pub output_mode: OutputMode,
	pub model: Option<String>,
	pub provider: Option<String>,
	pub api_key: Option<String>,
	/// Provider-side sticky session id (`--provider-session-id`).
	pub provider_session_id: Option<String>,
	/// Effective prompt-cache identity for this run (`--prompt-cache-key`, else
	/// a key inherited from the resumed/forked session header).
	pub prompt_cache_key: Option<String>,
	pub system_prompt: Option<String>,
	pub append_system_prompt: Option<String>,
	pub context_files: Vec<ContextFile>,
	pub skills: Vec<Skill>,
	/// Earlier turns of this session (`--continue`/`--session-id`), oldest first.
	pub history: Vec<crate::session::TurnRecord>,
	/// Exact provider-ready branch context from the session (`branch_summary`
	/// and retained rewind reports included). When set it replaces `history`
	/// as the request prefix; the live runner appends the new user message.
	pub history_messages: Option<Vec<pi_agent::Message>>,
	/// Shared checkpoint controller when `checkpoint.enabled`; tool guards and
	/// the session owner must see the same state within a run.
	pub checkpoint_controller: Option<std::sync::Arc<pi_agent::tools::CheckpointController>>,
	/// Per-turn persistence/rewind handshake channel.
	pub turn_ack: Option<std::sync::Arc<pi_agent::TurnAck>>,
	/// Todo phases restored from the session branch (TS
	/// `getLatestTodoPhasesFromEntries`): the newest `user_todo_edit` custom
	/// entry or successful `todo` tool result, whichever comes last in the log.
	pub todo_phases: Vec<pi_agent::tools::todo::TodoPhase>,
	/// Whether turns are written to a session log; tools report
	/// `details.storage` as `session` instead of `memory`.
	pub session_persisted: bool,
	/// Session log path when persistence is enabled. Eval derives its artifact
	/// directory from this file and exposes it to kernels.
	pub session_file: Option<std::path::PathBuf>,
	/// Stable session identity for eval kernel/artifact scoping. `None` lets
	/// the runner generate one process-stable id for in-memory sessions.
	pub session_id: Option<String>,
	pub config: Config,
	pub catalog: Arc<Catalog>,
	pub approvals: Approvals,
	pub max_time: Option<Duration>,
	/// Assistant-turn budget handed to the agent loop.
	pub max_turns: usize,
	/// `false` when `--no-tools` was passed.
	pub tools_enabled: bool,
	/// Optional cancellation trigger from the consumer.
	pub cancellation_token: Option<pi_agent::CancellationToken>,
	/// Host-provided tools mounted into the per-turn registry after every
	/// builtin/MCP/ask/checkpoint/eval tool is in place. A name that is already
	/// registered — or repeated in this batch — is a hard error, never a silent
	/// replacement of a builtin.
	pub extra_tools: Vec<ExtraTool>,
	/// ACP reverse-call bridge mounted on this turn's `read`/`write`/`bash`
	/// tools and inherited by child registries. `None` keeps every tool local.
	pub acp_bridge: Option<AcpBridgeHandle>,
	/// ACP `session/new|load|resume|fork` client-supplied MCP servers, merged
	/// with file-discovered servers before the existing launch/approval gate.
	pub extra_mcp_servers: Vec<crate::config::DiscoveredMcpServer>,
	/// Session internal URL router: the RPC host `scheme://` handlers plus any
	/// scheme the runner installs. Shared with child agents so a child resolves
	/// exactly what the root does; `None` disables every non-filesystem source.
	pub internal_urls: Option<std::sync::Arc<pi_agent::tools::internal_urls::InternalUrlRouter>>,
	/// Immutable URL scope for this run (session file + `local://` root). The
	/// runner copies it into each tool call, so concurrent turns and children
	/// never share or overwrite one.
	pub url_scope: Option<std::sync::Arc<pi_agent::tools::internal_urls::UrlScope>>,
	/// Subagent event observer the runner subscribes once on its lazily built
	/// supervisor; a replaced supervisor reuses the same handle.
	pub subagent_observer: Option<ObserverHandle>,
	/// Directory child transcripts are written to; `None` leaves
	/// `get_subagent_messages` without a file to page.
	pub child_session_root: Option<std::path::PathBuf>,
	/// Session goal/budget controller. The live pump records `MessageEnd`/
	/// tool-end usage into it, sends the limit steer through the run's own
	/// steering channel, and the eval host reads its `budget()` snapshot.
	pub goal: Option<std::sync::Arc<crate::goal::GoalRuntime>>,
	/// Hidden goal-mode context for this request (TS `goal-mode-context`),
	/// inserted immediately before the user message and never journaled as a
	/// user turn.
	pub goal_context: Option<String>,
	/// Host-hidden Plan reference context, inserted once before the current user
	/// prompt (TS pre-LLM plan slot). Never persisted as a transcript turn.
	pub plan_context: Option<String>,
	/// Resolved per-family service-tier selection (TS `ServiceTierByFamily`,
	/// string values). Empty means "no selection" and must not re-enable the
	/// configured priority default.
	pub service_tier_by_family: std::collections::BTreeMap<String, String>,
	/// Per-request credential resolver installed by the RPC auth bridge. Key
	/// precedence stays TS `getApiKey` order: an explicit `--api-key`/config key
	/// wins, then this store (login, env, static), then the connection block.
	/// `None` keeps the non-RPC path unchanged.
	pub credential_resolver: Option<ResolverHandle>,
	/// Plan-mode runtime wiring: the write guard, the `xd://propose` handler and
	/// the plan side of the composite tool policy. Empty outside plan mode.
	pub plan_mode: PlanModeHooks,
	/// Request-specific capability ceiling. `None` keeps the full registry;
	/// isolated runs (auto-learn capture) exclude MCP and extension tools here
	/// instead of changing global MCP selection.
	pub tool_ceiling: Option<ToolCeiling>,
	/// Session-scoped `autoloadSkills` resolver for child agents; default means
	/// no session snapshot and declared names are skipped.
	pub skill_autoload: AutoloadHandle,
	/// Detached plan-reminder continuation: no caller message is appended and
	/// the first request uses the policy's one-shot forced choice.
	pub continuation: bool,
	/// Host adapter for the C6 security coordinator (runtime-owned glue). The
	/// runner mounts `security_scan` and `security://` only when
	/// `config.security.enabled()` and this is set.
	pub security_host: Option<crate::security::SecurityHostHandle>,
	/// Read-only security-scan worker: the `lsp` tool rejects every action
	/// outside `LSP_READONLY_ACTIONS` and delegated children inherit the same
	/// action ceiling, independently of the approval mode.
	pub security_worker: bool,
	/// Host-owned advisor card sink. The runner calls it exactly once per card
	/// (aside flush, preserve, or steered note) so each host persists/forwards
	/// the custom `advisor` message without duplicating it on the event wire.
	pub advisor_card_sink: Option<AdvisorCardHandle>,
}

/// Host plan-mode wiring for one run.
///
/// The guard stays mounted across turns (it reads the live mode state); the
/// proposal handler and reminder policy are `None` whenever plan mode is off,
/// so a plain session keeps the staged-preview policy alone.
#[derive(Clone, Default)]
pub struct PlanModeHooks {
	pub guard:   Option<std::sync::Arc<dyn pi_agent::state::PlanModeGuard>>,
	pub propose: Option<std::sync::Arc<dyn pi_agent::state::ProposeHandler>>,
	pub policy:  Option<std::sync::Arc<dyn pi_agent::state::ToolPolicy>>,
}

impl std::fmt::Debug for PlanModeHooks {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("PlanModeHooks")
			.field("guard", &self.guard.is_some())
			.field("propose", &self.propose.is_some())
			.field("policy", &self.policy.is_some())
			.finish()
	}
}

/// Request-specific tool capability ceiling.
///
/// Used by isolated runs (auto-learn capture) to keep MCP servers and extension
/// tools out of the registry without altering global MCP selection: every
/// ordinary request leaves this `None`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ToolCeiling {
	/// Mount no MCP tool and run no MCP discovery for this request.
	pub exclude_mcp:              bool,
	/// Bind no extension/eval tool for this request.
	pub exclude_extension_tools:  bool,
}

/// Keeps [`RunRequest`]'s derived `Debug` intact for the session-scoped
/// `autoloadSkills` resolver.
#[derive(Clone, Default)]
pub struct AutoloadHandle(pub Option<std::sync::Arc<dyn pi_agent::tools::task::AutoloadSkillResolver>>);

impl std::fmt::Debug for AutoloadHandle {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_tuple("AutoloadHandle").field(&self.0.is_some()).finish()
	}
}

impl AutoloadHandle {
	#[must_use]
	pub fn resolver(&self) -> Option<std::sync::Arc<dyn pi_agent::tools::task::AutoloadSkillResolver>> {
		self.0.clone()
	}
}

/// Advisor card sink that keeps [`RunRequest`]'s derived `Debug` intact.
#[derive(Clone)]
pub struct AdvisorCardHandle(pub crate::advisor_runtime::CardSink);

impl std::fmt::Debug for AdvisorCardHandle {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("AdvisorCardHandle")
	}
}

/// Resolver handle that keeps [`RunRequest`]'s derived `Debug` intact: the
/// resolver is a trait object, the handle only names that one was installed.
#[derive(Clone)]
pub struct ResolverHandle(pub std::sync::Arc<dyn crate::rpc::auth::CredentialResolver>);

impl std::fmt::Debug for ResolverHandle {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("ResolverHandle")
	}
}

/// Observer handle that keeps [`RunRequest`]'s derived `Debug` intact: the
/// trait object itself is not printable, the handle names only that one was
/// installed.
#[derive(Clone)]
pub struct ObserverHandle(pub std::sync::Arc<dyn pi_agent::supervisor::SubagentObserver>);

/// ACP reverse-call bridge for one turn. Wrapper keeps `RunRequest: Debug`.
#[derive(Clone)]
pub struct AcpBridgeHandle(pub std::sync::Arc<dyn pi_agent::AcpClientBridge>);

impl std::fmt::Debug for AcpBridgeHandle {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("AcpBridgeHandle")
	}
}

impl std::fmt::Debug for ObserverHandle {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("ObserverHandle")
	}
}

/// One host tool handed to the runner.
///
/// `Arc<dyn Tool>` is neither `Debug` nor name-aware at the field level, so the
/// wrapper supplies both: `RunRequest` stays `Debug` and a conflict message can
/// name the tool.
#[derive(Clone)]
pub struct ExtraTool(pub std::sync::Arc<dyn pi_agent::tools::Tool>);

impl std::fmt::Debug for ExtraTool {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_tuple("ExtraTool").field(&self.0.name()).finish()
	}
}

impl From<std::sync::Arc<dyn pi_agent::tools::Tool>> for ExtraTool {
	fn from(tool: std::sync::Arc<dyn pi_agent::tools::Tool>) -> Self {
		Self(tool)
	}
}

impl RunRequest {
	#[must_use]
	pub fn has_image(&self) -> bool {
		self.parts.iter().any(crate::session::ContentPart::is_image)
	}
}

pub trait AgentRunner: Send + Sync {
	fn run(&self, request: &RunRequest) -> Result<EventStream>;

	/// Release one root session's long-lived runtime resources. Hosts call this
	/// after a session closes or switches away; the selector is the same
	/// `session_id`/`session_file` pair the session's runs carried. The default
	/// is a no-op for runners that do not retain per-session state.
	fn close_session(
		&self,
		session_id: Option<&str>,
		session_file: Option<&std::path::Path>,
	) -> Option<u64> {
		let _ = (session_id, session_file);
		None
	}

	/// Release a closed tombstone only with the exact ticket returned by the
	/// close that created it. Default no-op; fast, no threads/joins.
	fn release_session_scope(
		&self,
		session_id: Option<&str>,
		session_file: Option<&std::path::Path>,
		ticket: u64,
	) {
		let _ = (session_id, session_file, ticket);
	}

	/// Clear a closed-identity tombstone when a host explicitly opens the same
	/// persisted session again (RPC switch/new target, ACP load/new/resume/fork).
	/// The default is a no-op for runners without per-session state.
	fn reopen_session(
		&self,
		session_id: Option<&str>,
		session_file: Option<&std::path::Path>,
	) {
		let _ = (session_id, session_file);
	}

	/// Enter the headless advisor drain: from here on every advice card is
	/// preserved (never a hidden primary turn). Hosts call this **after every
	/// primary prompt turn has settled**, immediately before waiting.
	fn advisor_prepare_headless_drain(
		&self,
		session_id: Option<&str>,
		session_file: Option<&std::path::Path>,
	) {
		let _ = (session_id, session_file);
	}

	/// Wait for the advisor backlog to settle. Returns `true` when every
	/// advisor is caught up or has stopped retrying (quota/error); a timeout
	/// returns `false` and must not abort the session.
	fn advisor_wait_catchup(
		&self,
		session_id: Option<&str>,
		session_file: Option<&std::path::Path>,
		timeout: std::time::Duration,
	) -> bool {
		let _ = (session_id, session_file, timeout);
		true
	}

	/// Stop advisor work for this session scope (idempotent).
	fn advisor_dispose(
		&self,
		session_id: Option<&str>,
		session_file: Option<&std::path::Path>,
	) {
		let _ = (session_id, session_file);
	}

	/// Re-prime every advisor after a conversation boundary (`/new`, branch
	/// switch, compaction/rewind rewrite). `history` is the exact post-boundary
	/// transcript; the old feed is replaced before the next advisor request.
	fn advisor_reset_boundary(
		&self,
		session_id: Option<&str>,
		session_file: Option<&std::path::Path>,
		history: &[pi_agent::Message],
	) {
		let _ = (session_id, session_file, history);
	}

	/// Rendered advisor status for this scope (`None` when the advisor never
	/// activated, i.e. disabled or no turn has run yet).
	fn advisor_status(
		&self,
		session_id: Option<&str>,
		session_file: Option<&std::path::Path>,
	) -> Option<String> {
		let _ = (session_id, session_file);
		None
	}
}

static RUNNER: OnceLock<Box<dyn AgentRunner>> = OnceLock::new();

/// Registration hook for the `pi-agent` integration.
///
/// ```ignore
/// omp_cli::agent::install(Box::new(pi_agent::OmpRunner::new()));
/// ```
pub fn install(runner: Box<dyn AgentRunner>) -> std::result::Result<(), Box<dyn AgentRunner>> {
	RUNNER.set(runner)
}

#[must_use]
pub fn runner() -> Option<&'static dyn AgentRunner> {
	RUNNER.get().map(|boxed| &**boxed)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn event_json_matches_print_mode_shape() {
		let json = serde_json::to_string(&Event::MessageUpdate { delta: "hi".into() }).expect("json");
		assert_eq!(json, r#"{"type":"message_update","delta":"hi"}"#);
		let end = serde_json::to_string(&Event::TurnEnd {
			message: pi_agent::Message::assistant("ok"),
			tool_results: Vec::new(),
			stop_reason: "end_turn".into(),
			api: None,
			provider: None,
			model: None,
		})
		.expect("json");
		assert!(end.contains(r#""type":"turn_end""#));
		assert!(end.contains(r#""stop_reason":"end_turn""#));
	}

	#[test]
	fn no_runner_is_registered_by_default() {
		// Unit tests are compiled without the pi-agent integration.
		assert!(runner().is_none());
	}
}
