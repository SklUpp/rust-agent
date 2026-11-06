//! Persisted child contract and cold-revive factory.
//!
//! Mirrors `packages/coding-agent/src/task/persisted-revive.ts`: a parked child
//! keeps only its transcript (`<session root>/<id>.jsonl`) and the
//! `session_init`-equivalent contract recorded beside it
//! (`<session root>/<id>.init.json`). Reviving reopens the JSONL, replays the
//! real messages into a fresh [`AgentState`], and rebuilds an [`AgentLoop`] from
//! the persisted contract instead of the current top-level session options — a
//! bare reopen would otherwise resurrect a wrong (root) session.
//!
//! No persisted contract, or a recorded workspace that no longer exists, means
//! the child stays transcript-only (`history://<id>`); [`PersistedChildSession::load`]
//! returns `None` in exactly those cases.

use std::{
	collections::{HashMap, HashSet},
	fs,
	io::{BufRead, BufReader, Write},
	path::{Path, PathBuf},
	sync::Arc,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
	loop_module::AgentLoop,
	provider::ModelProvider,
	state::{
		AgentConfig, AgentState, Message, PlanModeGuard, Role, ToolApprover, TurnResult,
	},
	supervisor::Supervisor,
	tools::{
		registry::ToolRegistry,
		task::TaskTool,
		task_catalog::{AgentDefinition, Spawns},
	},
};

/// Contract file suffix, next to the child transcript.
pub const SESSION_INIT_SUFFIX: &str = ".init.json";
/// Marker written when a hard kill tombstones an agent id.
pub const TOMBSTONE_SUFFIX: &str = ".tombstone";

const DEFAULT_MAX_TURNS: usize = 20;
const DEFAULT_CONTEXT_LIMIT: usize = 100_000;

fn default_max_turns() -> usize {
	DEFAULT_MAX_TURNS
}

fn default_context_limit() -> usize {
	DEFAULT_CONTEXT_LIMIT
}

/// Root children spawn at depth 1; a contract written before the field existed
/// is therefore a first-level child.
fn default_depth() -> i32 {
	1
}

/// One child transcript path in `root`.
#[must_use]
pub fn transcript_path(root: &Path, id: &str) -> PathBuf {
	root.join(format!("{id}.jsonl"))
}

/// One child contract path in `root`.
#[must_use]
pub fn init_path(root: &Path, id: &str) -> PathBuf {
	root.join(format!("{id}{SESSION_INIT_SUFFIX}"))
}

/// One tombstone marker path in `root`.
#[must_use]
pub fn tombstone_path(root: &Path, id: &str) -> PathBuf {
	root.join(format!("{id}{TOMBSTONE_SUFFIX}"))
}

fn valid_id(id: &str) -> bool {
	!id.is_empty()
		&& id
			.chars()
			.all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

/// Persisted `session_init` contract for one subagent.
///
/// Field names and meaning follow the TypeScript `SessionInitEntry`. The two
/// roster-only fields (`displayName`, `parentId`) and the workspace (`cwd`) are
/// extra: the TypeScript build reads them from the session header and the agent
/// registry, while a standalone transcript needs them recorded once to restore
/// a parked row after a process restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedChildSession {
	pub id: String,
	/// Agent definition name (for example `scout` or `reviewer`).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub agent: Option<String>,
	/// Initial task text this child ran.
	#[serde(default)]
	pub task: String,
	/// Full system prompt sent to the model.
	#[serde(default)]
	pub system_prompt: String,
	/// Tool names the child was offered (explicit grants on revive).
	#[serde(default)]
	pub tools: Vec<String>,
	/// Semantic model role declared by the agent, retained after resolution.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub model_role: Option<String>,
	/// Initially resolved `provider/model` selector.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub resolved_model: Option<String>,
	/// Structured output schema, when one was requested.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub output_schema: Option<Value>,
	/// Enforcement policy recorded with the output schema.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub output_schema_mode: Option<String>,
	/// Whether revival must retain only the explicitly persisted tool names.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub restrict_tool_names: Option<bool>,
	/// Spawn allowlist the child ran with (`""` = none, `"*"` = any, else CSV).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub spawns: Option<String>,
	/// The agent's `readSummarize` setting.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_summarize: Option<bool>,
	/// Effective advisor: `"on"` = advisor-role model, else an explicit pattern.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub advisor: Option<String>,
	/// Whether the agent definition is read-only.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_only: Option<bool>,
	/// Recorded workspace. A gone directory makes the contract unusable.
	pub cwd: PathBuf,
	/// Spawn depth this child ran at (root children are depth 1). A revived
	/// child is rebuilt at this depth so its own `task` tool obeys the same
	/// `max_depth`, self-spawn, and spawn-allowlist guards as the live child.
	#[serde(default = "default_depth")]
	pub depth: i32,
	#[serde(default = "default_max_turns")]
	pub max_turns: usize,
	#[serde(default = "default_context_limit")]
	pub context_limit: usize,
	/// Roster-only display name (`task.name`); never an identity.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub display_name: Option<String>,
	/// Roster-only parent agent id.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent_id: Option<String>,
	/// Stable saved-output identity (`agent://<output_id>`). Written only with
	/// [`Self::output_published`] after the artifact is confirmed, so a revived
	/// nested child keeps its prefix without gaining replacement authority from
	/// a spawn-time allocation. Older contracts omit it.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub output_id: Option<String>,
	/// Set only after `<output_id>.md` was actually published by this child.
	/// Spawn-time allocation and contracts written before confirmed
	/// publication (including older files with no field) are not authority to
	/// replace an existing artifact on a follow-up turn.
	#[serde(default)]
	pub output_published: bool,
}

impl PersistedChildSession {
	/// Published saved-output identity, if this contract owns one.
	#[must_use]
	pub fn published_output_id(&self) -> Option<&str> {
		if !self.output_published {
			return None;
		}
		self.output_id
			.as_deref()
			.filter(|id| crate::tools::agent_output::valid_output_id(id))
	}

	/// Write the contract next to the child transcript.
	pub fn write(&self, root: &Path) -> Result<PathBuf, String> {
		if !valid_id(&self.id) {
			return Err(format!("invalid child id for session contract: {:?}", self.id));
		}
		fs::create_dir_all(root)
			.map_err(|error| format!("cannot create {}: {error}", root.display()))?;
		let path = init_path(root, &self.id);
		let tmp = path.with_extension("json.tmp");
		let bytes = serde_json::to_vec_pretty(self)
			.map_err(|error| format!("cannot serialize child contract: {error}"))?;
		fs::write(&tmp, bytes)
			.map_err(|error| format!("cannot write {}: {error}", tmp.display()))?;
		fs::rename(&tmp, &path)
			.map_err(|error| format!("cannot commit {}: {error}", path.display()))?;
		Ok(path)
	}

	/// Load a usable contract for `id` from `root`.
	///
	/// `None` means the child is transcript-only: no contract was recorded, the
	/// contract does not belong to `id`, the transcript is missing, or the
	/// recorded workspace is gone (isolated/merged worktree, moved dir). A
	/// tombstoned id is not revivable either.
	pub fn load(root: &Path, id: &str) -> Option<Self> {
		if !valid_id(id) || tombstone_path(root, id).exists() {
			return None;
		}
		let raw = fs::read(init_path(root, id)).ok()?;
		let parsed: Self = serde_json::from_slice(&raw).ok()?;
		if parsed.id != id || !parsed.cwd.is_dir() {
			return None;
		}
		if !transcript_path(root, id).is_file() {
			return None;
		}
		Some(parsed)
	}

	/// Reopen the transcript and contract for one parked child.
	pub fn reopen(root: &Path, id: &str) -> Option<(Self, Vec<Message>)> {
		let contract = Self::load(root, id)?;
		let messages = load_transcript(&transcript_path(root, id)).ok()?;
		Some((contract, messages))
	}
}

/// Read a child transcript into real messages, without touching the replay.
///
/// The `history://` reader uses this: an unanswered call or an orphan result
/// is still part of what happened and must be visible (rendered as pending or
/// orphaned), so the display path never applies [`sanitize_replay`].
///
/// The executor writes `{"type":"message","message":{...}}` lines; a raw
/// message object is accepted too so older/foreign files are not fatal. A
/// malformed line is skipped rather than destroying the whole read, matching a
/// crash-truncated trailing write.
pub fn read_transcript(path: &Path) -> Result<Vec<Message>, String> {
	let file =
		fs::File::open(path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
	read_transcript_file(file)
}

/// Parse a transcript from an already-authorized, already-open regular file.
///
/// `history://` opens the leaf descriptor-relative with `O_NOFOLLOW` and parses
/// THAT descriptor, so a path swapped after the scan can never redirect the
/// read to another file.
pub fn read_transcript_file(file: fs::File) -> Result<Vec<Message>, String> {
	let mut messages = Vec::new();
	for line in BufReader::new(file).lines() {
		let Ok(line) = line else { continue };
		let line = line.trim();
		if line.is_empty() {
			continue;
		}
		let Ok(value) = serde_json::from_str::<Value>(line) else {
			continue;
		};
		let candidate = value.get("message").cloned().unwrap_or(value);
		if let Ok(message) = serde_json::from_value::<Message>(candidate) {
			messages.push(message);
		}
	}
	Ok(messages)
}

/// Read a child transcript and drop the turns a provider would reject.
///
/// Used by revive: a replay must be a valid model conversation.
pub fn load_transcript(path: &Path) -> Result<Vec<Message>, String> {
	Ok(sanitize_replay(read_transcript(path)?))
}

/// Drop replay turns the provider would reject.
///
/// Old transcripts (and a crash-truncated tail) can contain assistant turns
/// whose `tool_calls` have no matching `tool` message. Providers reject such a
/// history (`tool_use` without `tool_result`), so a revive would fail on its
/// first request. Unanswered calls are removed; an assistant turn left with no
/// content and no calls is dropped, as is any orphan `tool` message.
#[must_use]
pub fn sanitize_replay(messages: Vec<Message>) -> Vec<Message> {
	let mut kept: Vec<Message> = Vec::with_capacity(messages.len());
	let mut index = 0;
	while index < messages.len() {
		let message = &messages[index];
		if message.role == Role::Tool {
			// A tool result before any call cannot be paired: drop it.
			index += 1;
			continue;
		}
		if message.role != Role::Assistant || message.tool_calls.is_empty() {
			kept.push(message.clone());
			index += 1;
			continue;
		}
		let mut end = index + 1;
		let mut answered = HashSet::new();
		while end < messages.len() && messages[end].role == Role::Tool {
			if let Some(id) = &messages[end].tool_call_id {
				answered.insert(id.clone());
			}
			end += 1;
		}
		let calls: Vec<_> = message
			.tool_calls
			.iter()
			.filter(|call| answered.contains(&call.id))
			.cloned()
			.collect();
		if !calls.is_empty() || !message.content.trim().is_empty() {
			let mut assistant = message.clone();
			assistant.tool_calls = calls.clone();
			kept.push(assistant);
		}
		for result in &messages[index + 1..end] {
			if result
				.tool_call_id
				.as_ref()
				.is_some_and(|id| calls.iter().any(|call| &call.id == id))
			{
				kept.push(result.clone());
			}
		}
		index = end;
	}
	kept
}

/// Journal messages one finished turn contributes: the assistant message
/// followed by one `tool` message per result.
///
/// The executor used to journal assistant messages only, so a replay carried
/// `tool_calls` with no results and the revived child's first request was
/// rejected. Pairing the results at write time keeps the JSONL a valid model
/// conversation, so replay needs no reconstruction.
///
/// ponytail: tool image parts are not journaled (text results only); add them
/// to the message when a revived child must keep vision context.
#[must_use]
pub fn turn_journal_messages(turns: &[TurnResult]) -> Vec<Message> {
	let mut messages = Vec::new();
	for turn in turns {
		let names: HashMap<&str, &str> = turn
			.message
			.tool_calls
			.iter()
			.map(|call| (call.id.as_str(), call.name.as_str()))
			.collect();
		messages.push(turn.message.clone());
		for result in &turn.tool_results {
			let name = names
				.get(result.tool_call_id.as_str())
				.copied()
				.unwrap_or("tool");
			messages.push(Message::tool_with_status(
				&result.tool_call_id,
				name,
				&result.content,
				result.is_error,
			));
		}
	}
	messages
}

/// Append new messages to a child transcript in the executor's exact format.
pub fn append_messages(path: &Path, messages: &[&Message]) {
	if messages.is_empty() {
		return;
	}
	let mut file = match fs::OpenOptions::new().create(true).append(true).open(path) {
		Ok(file) => file,
		Err(error) => {
			eprintln!("warning: cannot open child transcript {}: {error}", path.display());
			return;
		},
	};
	for message in messages {
		let Ok(message) = serde_json::to_value(message) else {
			continue;
		};
		let mut line = serde_json::json!({ "type": "message", "message": message }).to_string();
		line.push('\n');
		if let Err(error) = file.write_all(line.as_bytes()) {
			eprintln!("warning: cannot append child transcript {}: {error}", path.display());
			return;
		}
	}
}

/// A rebuilt child: the loop and the replayed state it runs against.
pub struct RevivedChild {
	pub agent_loop: AgentLoop,
	pub state:     AgentState,
	/// Original definition when the live catalog still has it. A follow-up
	/// rewrite needs the agent output schema, so an unresolvable definition
	/// simply disables that rewrite rather than guessing.
	pub agent:     Option<AgentDefinition>,
	/// Contract is needed for the saved-output id and follow-up schema policy.
	pub contract:  PersistedChildSession,
}

/// Host-owned cold-revive factory (upstream
/// `PersistedSubagentReviverFactory`).
///
/// Provider/auth construction stays host-owned exactly as in the TypeScript
/// reviver context: the factory receives the persisted contract so it can honor
/// `modelRole`/`resolvedModel`. When a supervisor is passed, the default
/// implementation resolves the child's model through the supervisor catalog and
/// rebuilds a *child-bound* registry at the recorded depth (a root registry
/// would silently drop `yield` and bypass the spawn guards); otherwise it falls
/// back to its provider snapshot and template.
pub trait PersistedChildReviverFactory: Send + Sync {
	fn revive(
		&self,
		contract: &PersistedChildSession,
		replayed: Vec<Message>,
		supervisor: Option<&Arc<Supervisor>>,
	) -> Result<RevivedChild, String>;
}

/// Default factory: one provider snapshot plus the session tool template.
pub struct DefaultPersistedReviverFactory {
	provider: Arc<dyn ModelProvider>,
	registry: ToolRegistry,
	approver: Option<Arc<dyn ToolApprover>>,
	plan:     Option<Arc<dyn PlanModeGuard>>,
}

impl DefaultPersistedReviverFactory {
	#[must_use]
	pub fn new(provider: Arc<dyn ModelProvider>, registry: ToolRegistry) -> Self {
		Self { provider, registry, approver: None, plan: None }
	}

	#[must_use]
	pub fn with_approver(mut self, approver: Option<Arc<dyn ToolApprover>>) -> Self {
		self.approver = approver;
		self
	}

	#[must_use]
	pub fn with_plan(mut self, plan: Option<Arc<dyn PlanModeGuard>>) -> Self {
		self.plan = plan;
		self
	}
}

/// Apply the persisted explicit tool grants to the host template.
///
/// Mirrors the TypeScript replay: the enabled set recorded at spawn is an
/// explicit grant list, `yield` is the framework's mandatory return channel,
/// and a read-only agent never regains `write`.
fn apply_contract_tools(registry: &mut ToolRegistry, contract: &PersistedChildSession) {
	let mut allowed: std::collections::HashSet<&str> =
		contract.tools.iter().map(String::as_str).collect();
	allowed.insert("yield");
	if contract.read_only == Some(true) {
		allowed.remove("write");
	}
	for name in registry.names() {
		if !allowed.contains(name.as_str()) {
			registry.remove(&name);
		}
	}
}

/// Parse the recorded `spawns` text back into the agent's spawn policy.
///
/// ponytail: reverse-parses `Spawns::error_text`; record a structured policy
/// field if that format ever stops being the readable allowlist.
fn spawns_from_contract(value: Option<&str>) -> Option<Spawns> {
	let text = value?.trim();
	if text.is_empty() {
		return None;
	}
	if text == "*" {
		return Some(Spawns::All);
	}
	if text.starts_with("none") {
		return Some(Spawns::Only(Vec::new()));
	}
	Some(Spawns::Only(
		text.split(',')
			.map(str::trim)
			.filter(|name| !name.is_empty())
			.map(str::to_string)
			.collect(),
	))
}

/// Rebuild the registry a live child at `contract.depth` would have had:
/// child-scoped session tools, `yield`, an owner-bound `hub`, and a nested
/// `task` tool carrying the agent context and effective allowlist recorded at
/// spawn. `rebind_session_tools` is the caller's job, after this returns.
fn revived_child_registry(
	supervisor: &Arc<Supervisor>,
	contract: &PersistedChildSession,
) -> ToolRegistry {
	if let Some(output_id) = contract.published_output_id() {
		supervisor.set_revived_output_prefix(&contract.id, output_id);
	}
	let mut registry = supervisor.child_registry_unbound(contract.depth, Some(contract.id.as_str()));
	apply_contract_tools(&mut registry, contract);
	if registry.get("task").is_some() {
		registry.remove("task");
		let agent_name = contract
			.agent
			.clone()
			.unwrap_or_else(|| "task".to_string());
		let tools = contract.tools.iter().cloned().collect();
		registry.register(Arc::new(
			TaskTool::revived(
				Arc::clone(supervisor),
				contract.depth,
				contract.id.clone(),
				agent_name,
				spawns_from_contract(contract.spawns.as_deref()),
				tools,
			)
			.with_output_prefix(contract.published_output_id().map(str::to_string)),
		));
	}
	registry
}

/// Provider and active model for a revived child.
///
/// The per-agent model recorded at spawn wins when the supervisor's catalog can
/// still resolve it (the same path a live spawn takes). Otherwise the recorded
/// `resolvedModel`/`modelRole` is rebuilt through the catalog's model factory,
/// and only as a last resort does the factory's provider snapshot apply.
fn resolve_revived_provider(
	supervisor: Option<&Arc<Supervisor>>,
	contract: &PersistedChildSession,
	fallback: &Arc<dyn ModelProvider>,
) -> (Arc<dyn ModelProvider>, Option<String>) {
	if let Some(supervisor) = supervisor
		&& let Some(catalog) = supervisor.agent_catalog()
	{
		let factory = catalog.model_factory();
		if let Some(factory) = factory.as_deref() {
			if let Some(agent) = contract.agent.as_deref().and_then(|name| catalog.get(name))
				&& let Ok(Some(model)) = catalog.resolve_provider(agent, None, Some(factory))
			{
				return (model.provider, Some(model.model));
			}
			// `resolvedModel` is the spawn-time expansion; `modelRole` the
			// pre-expansion alias. Either can be rebuilt when the agent
			// definition changed or vanished since.
			for pattern in [
				contract.resolved_model.as_deref(),
				contract.model_role.as_deref(),
			]
			.into_iter()
			.flatten()
			{
				if factory.can_resolve(pattern)
					&& let Ok(model) = factory.resolve(pattern, None, None)
				{
					return (model.provider, Some(model.model));
				}
			}
		}
	}
	(Arc::clone(fallback), contract.resolved_model.clone())
}

impl PersistedChildReviverFactory for DefaultPersistedReviverFactory {
	fn revive(
		&self,
		contract: &PersistedChildSession,
		replayed: Vec<Message>,
		supervisor: Option<&Arc<Supervisor>>,
	) -> Result<RevivedChild, String> {
		if !contract.cwd.is_dir() {
			return Err(format!(
				"recorded workspace {} no longer exists; agent \"{}\" is transcript-only \
				 (history://{})",
				contract.cwd.display(),
				contract.id,
				contract.id
			));
		}
		let (provider, active_model) =
			resolve_revived_provider(supervisor, contract, &self.provider);
		let registry = match supervisor {
			Some(supervisor) => {
				let mut registry = revived_child_registry(supervisor, contract);
				// Exactly like a live child: finish the session tools with the
				// child's model, then bind eval's default completion to the
				// provider that child actually runs on.
				supervisor.rebind_session_tools(
					&mut registry,
					active_model.as_deref(),
					Some(contract.id.as_str()),
				);
				supervisor.rebind_eval_provider(&mut registry, &contract.id, Arc::clone(&provider));
				registry
			},
			None => {
				let mut registry = self.registry.clone();
				apply_contract_tools(&mut registry, contract);
				registry
			},
		};
		let replayed_turns = replayed.iter().filter(|m| m.role == Role::Assistant).count();
		let config = AgentConfig {
			max_turns:     contract.max_turns.saturating_add(replayed_turns),
			context_limit: contract.context_limit.max(1),
			system_prompt: if contract.system_prompt.is_empty() {
				None
			} else {
				Some(contract.system_prompt.clone())
			},
			cwd:           contract.cwd.clone(),
		};
		let mut agent_loop = match &self.approver {
			Some(approver) => AgentLoop::with_approver(
				config,
				Arc::clone(&provider),
				registry,
				Arc::clone(approver),
			),
			None => AgentLoop::new(config, Arc::clone(&provider), registry),
		};
		if let Some(plan) = &self.plan {
			agent_loop = agent_loop.with_plan(Some(Arc::clone(plan)));
		}
		if let Some(supervisor) = supervisor {
			agent_loop = agent_loop
				.with_background_results(supervisor.owner_results_sink(Some(&contract.id)))
				.with_peer_inbox(supervisor.mailbox(), contract.id.clone());
		}
		let agent = supervisor
			.and_then(|supervisor| supervisor.agent_catalog())
			.and_then(|catalog| {
				contract
					.agent
					.as_deref()
					.and_then(|name| catalog.get(name))
					.cloned()
			});
		Ok(RevivedChild {
			agent_loop,
			agent,
			contract: contract.clone(),
			state: AgentState {
				messages:    replayed,
				total_turns: replayed_turns,
			},
		})
	}
}
