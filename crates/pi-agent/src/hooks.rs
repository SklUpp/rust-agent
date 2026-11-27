//! Subprocess JSON hooks, compatible with Claude Code and Codex hook files.
//!
//! A hook is an external command that receives one JSON object on stdin and
//! answers through its exit status and stdout:
//!
//! - exit `0`: optional JSON reply (`continue`, `decision`, `reason`,
//!   `systemMessage`, and `hookSpecificOutput`);
//! - exit `2`: a blocking decision; stderr (or stdout) is fed back to the
//!   model;
//! - anything else (or a timeout): a hook failure. `failClosed` rules deny
//!   the action, otherwise the failure is advisory and only logged.
//!
//! Hooks are registered from trusted settings files only (see the aragent
//! loader); a project checkout cannot register an arbitrary subprocess.
//! Processes run without shell interpolation when configured as argv, and a
//! timed-out hook kills its whole process group so background children do not
//! leak past the decision.

use std::{
	path::{Path, PathBuf},
	process::Stdio,
	time::Duration,
};

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
	io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
	process::{ChildStdin, Command},
	sync::OnceCell,
};

/// Default per-hook timeout when the configuration does not set one.
pub const DEFAULT_HOOK_TIMEOUT_MS: u64 = 10_000;

/// Hard cap on each captured stream. A hook that floods stdout cannot grow
/// the parent's memory without bound; the timeout still reaps it.
pub const MAX_HOOK_OUTPUT_BYTES: usize = 1024 * 1024;

/// Hook surfaces the runtime invokes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookEvent {
	PreToolUse,
	PostToolUse,
	PermissionRequest,
	Stop,
	SessionStart,
}

impl HookEvent {
	pub const ALL: [Self; 5] = [
		Self::PreToolUse,
		Self::PostToolUse,
		Self::PermissionRequest,
		Self::Stop,
		Self::SessionStart,
	];

	/// Wire name used in `hook_event_name` and settings keys.
	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::PreToolUse => "PreToolUse",
			Self::PostToolUse => "PostToolUse",
			Self::PermissionRequest => "PermissionRequest",
			Self::Stop => "Stop",
			Self::SessionStart => "SessionStart",
		}
	}

	/// Parse a settings key case-insensitively, ignoring `_`/`-` separators.
	#[must_use]
	pub fn parse_key(raw: &str) -> Option<Self> {
		let normalized: String = raw
			.chars()
			.filter(|ch| *ch != '_' && *ch != '-')
			.flat_map(char::to_lowercase)
			.collect();
		match normalized.as_str() {
			"pretooluse" => Some(Self::PreToolUse),
			"posttooluse" => Some(Self::PostToolUse),
			"permissionrequest" => Some(Self::PermissionRequest),
			"stop" => Some(Self::Stop),
			"sessionstart" => Some(Self::SessionStart),
			_ => None,
		}
	}
}

/// How a hook is spawned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookInvocation {
	/// A command string. Claude settings store this form; it runs through
	/// `sh -c` (or `cmd /C`) because that is the documented contract.
	Shell(String),
	/// Direct argv, without any shell interpolation.
	Argv(Vec<String>),
}

/// One configured hook rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRule {
	pub event: HookEvent,
	/// Optional tool-name matcher for tool/permission events. `None`, `*`, the
	/// empty string and a regex match all tool names; an invalid regex falls
	/// back to exact equality.
	pub matcher: Option<String>,
	pub invocation: HookInvocation,
	pub timeout_ms: u64,
	/// Deny on failure/timeout instead of only logging it.
	pub fail_closed: bool,
}

impl HookRule {
	#[must_use]
	pub fn matches(&self, tool: Option<&str>) -> bool {
		let Some(matcher) = self.matcher.as_deref().map(str::trim) else {
			return true;
		};
		if matcher.is_empty() || matcher == "*" {
			return true;
		}
		let Some(tool) = tool else {
			return true;
		};
		match regex::Regex::new(matcher) {
			Ok(regex) => regex.is_match(tool),
			Err(_) => matcher == tool,
		}
	}
}

/// Validated hook configuration, normally assembled by the settings loader.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HookConfig {
	rules: Vec<HookRule>,
}

impl HookConfig {
	#[must_use]
	pub fn new(rules: Vec<HookRule>) -> Self {
		Self { rules }
	}

	#[must_use]
	pub fn rules(&self) -> &[HookRule] {
		&self.rules
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.rules.is_empty()
	}

	/// Append another layer. Later layers run after earlier ones.
	pub fn merge(&mut self, other: Self) {
		self.rules.extend(other.rules);
	}

	/// Parse a Claude Code `settings.json`, Codex `hooks.json`, or a bare
	/// event map. Unknown event names and non-command hook types are ignored so
	/// a newer host file does not break startup.
	pub fn from_json(value: &Value) -> Result<Self, HookConfigError> {
		let map = match value.get("hooks") {
			Some(hooks) if hooks.is_object() => hooks,
			_ => value,
		};
		let Some(pairs) = map.as_object() else {
			return Err(HookConfigError::new("hooks must be a JSON object keyed by event name"));
		};
		let mut rules = Vec::new();
		for (key, item) in pairs {
			let Some(event) = HookEvent::parse_key(key) else {
				continue;
			};
			let Some(entries) = item.as_array() else {
				return Err(HookConfigError::new(format!("hooks.{key} must be an array")));
			};
			for entry in entries {
				parse_entry(event, entry, None, &mut rules)?;
			}
		}
		Ok(Self { rules })
	}

	pub fn from_json_str(text: &str) -> Result<Self, HookConfigError> {
		let value: Value = serde_json::from_str(text)
			.map_err(|error| HookConfigError::new(format!("invalid hook JSON: {error}")))?;
		Self::from_json(&value)
	}

	/// Read a hook settings file. A malformed file is an error the caller can
	/// report or skip; a missing file is an empty configuration.
	pub fn from_file(path: &Path) -> Result<Self, HookConfigError> {
		if !path.is_file() {
			return Ok(Self::default());
		}
		let text = std::fs::read_to_string(path).map_err(|error| {
			HookConfigError::new(format!("cannot read {}: {error}", path.display()))
		})?;
		Self::from_json_str(&text)
			.map_err(|error| HookConfigError::new(format!("{}: {error}", path.display())))
	}
}

/// Configuration error with the offending key in the message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct HookConfigError {
	message: String,
}

impl HookConfigError {
	fn new(message: impl Into<String>) -> Self {
		Self { message: message.into() }
	}
}

fn parse_entry(
	event: HookEvent,
	entry: &Value,
	inherited_matcher: Option<&str>,
	rules: &mut Vec<HookRule>,
) -> Result<(), HookConfigError> {
	let Some(object) = entry.as_object() else {
		return Err(HookConfigError::new(format!(
			"hooks.{} entries must be objects",
			event.as_str()
		)));
	};
	let matcher = inherited_matcher
		.or_else(|| object.get("matcher").and_then(Value::as_str))
		.map(str::to_string);
	// Claude Code nests command definitions under `hooks` with a matcher on
	// the wrapper; the nested entries inherit it.
	if let Some(nested) = object.get("hooks").and_then(Value::as_array) {
		for child in nested {
			parse_entry(event, child, matcher.as_deref(), rules)?;
		}
		return Ok(());
	}
	let kind = object
		.get("type")
		.and_then(Value::as_str)
		.unwrap_or("command");
	if kind != "command" {
		return Ok(());
	}
	let invocation = if let Some(command) = object.get("command").and_then(Value::as_str) {
		HookInvocation::Shell(command.to_string())
	} else {
		let argv = object
			.get("argv")
			.or_else(|| object.get("command"))
			.and_then(Value::as_array)
			.ok_or_else(|| {
				HookConfigError::new(format!(
					"hooks.{} command needs a `command` string or an argv array",
					event.as_str()
				))
			})?;
		let argv: Vec<String> = argv
			.iter()
			.map(|item| {
				item
					.as_str()
					.map(str::to_string)
					.ok_or_else(|| HookConfigError::new("hook argv entries must be strings"))
			})
			.collect::<Result<_, _>>()?;
		if argv.is_empty() {
			return Err(HookConfigError::new("hook argv must not be empty"));
		}
		HookInvocation::Argv(argv)
	};
	let timeout_ms = object
		.get("timeoutMs")
		.or_else(|| object.get("timeout_ms"))
		.and_then(Value::as_u64)
		.or_else(|| {
			object
				.get("timeout")
				.and_then(Value::as_u64)
				.and_then(|seconds| seconds.checked_mul(1000))
		})
		.unwrap_or(DEFAULT_HOOK_TIMEOUT_MS)
		.max(1);
	let fail_closed = object
		.get("failClosed")
		.or_else(|| object.get("fail_closed"))
		.and_then(Value::as_bool)
		.unwrap_or(false);
	rules.push(HookRule { event, matcher, invocation, timeout_ms, fail_closed });
	Ok(())
}

/// Context returned by a `SessionStart` batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HookSummary {
	pub additional_context: Vec<String>,
	pub system_messages: Vec<String>,
}

/// Aggregated `PreToolUse` outcome.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PreToolUseDecision {
	/// First blocking reason; when set the tool must not run.
	pub denied: Option<String>,
	/// Final tool input after applying every `updatedInput` reply.
	pub updated_input: Option<Value>,
	pub additional_context: Vec<String>,
	pub system_messages: Vec<String>,
	/// A hook asked for an explicit approval prompt.
	pub forced_ask: bool,
	/// A hook allowed the call. The approval matrix still runs unless the
	/// caller opted into hook-driven approval; bash policy always runs.
	pub allowed: bool,
}

/// Aggregated `PostToolUse` outcome.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PostToolUseDecision {
	pub blocked: Option<String>,
	pub additional_context: Vec<String>,
	pub system_messages: Vec<String>,
}

/// Aggregated `PermissionRequest` outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionDecision {
	Allow,
	Deny(String),
	Ask,
}

/// Aggregated `Stop` outcome.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StopDecision {
	pub blocked: Option<String>,
	pub system_messages: Vec<String>,
}

/// Executes configured hooks for one session.
#[derive(Debug)]
pub struct HookEngine {
	rules: Vec<HookRule>,
	/// Mutable so a host that opens its durable store after constructing the
	/// approvals can replace the provisional id with the persisted session id.
	session_id: std::sync::Mutex<String>,
	cwd: PathBuf,
	/// First `SessionStart` result, replayed to later turns of the same engine.
	/// `OnceCell` only records it after the run finishes, so a cancelled run
	/// leaves the cell empty for the next caller instead of caching empty.
	session_summary: OnceCell<HookSummary>,
}

impl HookEngine {
	#[must_use]
	pub fn new(config: HookConfig, session_id: impl Into<String>, cwd: impl Into<PathBuf>) -> Self {
		Self {
			rules: config.rules,
			session_id: std::sync::Mutex::new(session_id.into()),
			cwd: cwd.into(),
			session_summary: OnceCell::new(),
		}
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.rules.is_empty()
	}

	#[must_use]
	pub fn has_event(&self, event: HookEvent) -> bool {
		self.rules.iter().any(|rule| rule.event == event)
	}

	#[must_use]
	pub fn session_id(&self) -> String {
		self
			.session_id
			.lock()
			.map_or_else(|_| String::new(), |id| id.clone())
	}

	/// Replace the provisional id once the durable store knows the real one.
	pub fn set_session_id(&self, session_id: impl Into<String>) {
		if let Ok(mut id) = self.session_id.lock() {
			*id = session_id.into();
		}
	}

	#[must_use]
	pub fn cwd(&self) -> &Path {
		&self.cwd
	}

	/// Fire `SessionStart` once per engine. Later turns get the cached result so
	/// a non-durable host can reinject context it did not persist.
	pub async fn session_start(&self) -> HookSummary {
		self.session_summary
			.get_or_init(|| async {
				let payload = json!({
					"hook_event_name": HookEvent::SessionStart.as_str(),
					"session_id": self.session_id(),
					"cwd": self.cwd,
					"source": "startup",
				});
				let runs = self.run_event(HookEvent::SessionStart, None, payload).await;
				summarize(&runs)
			})
			.await
			.clone()
	}

	pub async fn pre_tool_use(&self, tool: &str, input: &Value) -> PreToolUseDecision {
		let mut decision = PreToolUseDecision::default();
		let mut current = input.clone();
		// Each reply feeds the next hook's payload: validation hooks must see
		// the rewritten input, not the original call.
		for rule in self
			.rules
			.iter()
			.filter(|rule| rule.event == HookEvent::PreToolUse && rule.matches(Some(tool)))
		{
			let payload = json!({
				"hook_event_name": HookEvent::PreToolUse.as_str(),
				"session_id": self.session_id(),
				"cwd": self.cwd,
				"tool_name": tool,
				"tool_input": current,
			});
			let run = self.run_rule_logged(rule, &payload).await;
			if let Some(reason) = run.blocking_reason() {
				decision.denied = Some(reason);
				break;
			}
			if let Some(reason) = run.failure_reason() {
				if run.fail_closed {
					decision.denied = Some(reason);
					break;
				}
				continue;
			}
			let Some(reply) = run.reply else {
				if !run.stdout.trim().is_empty() {
					decision
						.additional_context
						.push(run.stdout.trim().to_string());
				}
				continue;
			};
			collect_messages(&reply, &mut decision.additional_context, &mut decision.system_messages);
			if let Some(spec) = &reply.hook_specific_output {
				match spec.permission_decision.as_deref() {
					Some("allow") => decision.allowed = true,
					Some("ask") => decision.forced_ask = true,
					_ => {},
				}
				if let Some(updated) = &spec.updated_input {
					merge_updated_input(&mut current, updated);
					decision.updated_input = Some(current.clone());
				}
			}
		}
		decision
	}

	pub async fn post_tool_use(
		&self,
		tool: &str,
		input: &Value,
		output: &str,
		is_error: bool,
	) -> PostToolUseDecision {
		let payload = json!({
			"hook_event_name": HookEvent::PostToolUse.as_str(),
			"session_id": self.session_id(),
			"cwd": self.cwd,
			"tool_name": tool,
			"tool_input": input,
			"tool_output": output,
			"tool_is_error": is_error,
		});
		let mut decision = PostToolUseDecision::default();
		for run in self
			.run_event(HookEvent::PostToolUse, Some(tool), payload)
			.await
		{
			if let Some(reason) = run.blocking_reason() {
				decision.blocked = Some(reason);
				break;
			}
			if let Some(reason) = run.failure_reason() {
				if run.fail_closed {
					decision.blocked = Some(reason);
					break;
				}
				continue;
			}
			let Some(reply) = run.reply else {
				if !run.stdout.trim().is_empty() {
					decision
						.additional_context
						.push(run.stdout.trim().to_string());
				}
				continue;
			};
			collect_messages(&reply, &mut decision.additional_context, &mut decision.system_messages);
		}
		decision
	}

	/// Run `PermissionRequest` hooks. `None` means no hook matched, so the
	/// caller keeps its normal approval matrix.
	pub async fn permission_request(&self, tool: &str, input: &Value) -> Option<PermissionDecision> {
		if !self.has_event(HookEvent::PermissionRequest) {
			return None;
		}
		let payload = json!({
			"hook_event_name": HookEvent::PermissionRequest.as_str(),
			"session_id": self.session_id(),
			"cwd": self.cwd,
			"tool_name": tool,
			"tool_input": input,
		});
		let mut matched = false;
		let mut allow = false;
		let mut ask = false;
		for run in self
			.run_event(HookEvent::PermissionRequest, Some(tool), payload)
			.await
		{
			matched = true;
			if let Some(reason) = run.blocking_reason() {
				return Some(PermissionDecision::Deny(reason));
			}
			if let Some(reason) = run.failure_reason() {
				if run.fail_closed {
					return Some(PermissionDecision::Deny(reason));
				}
				continue;
			}
			let Some(reply) = run.reply else {
				continue;
			};
			if let Some(spec) = &reply.hook_specific_output {
				match spec.permission_decision.as_deref() {
					Some("allow") => allow = true,
					Some("ask") => ask = true,
					_ => {},
				}
			}
		}
		if !matched {
			return None;
		}
		if allow && !ask {
			Some(PermissionDecision::Allow)
		} else {
			Some(PermissionDecision::Ask)
		}
	}

	pub async fn stop(&self, stop_hook_active: bool) -> StopDecision {
		let payload = json!({
			"hook_event_name": HookEvent::Stop.as_str(),
			"session_id": self.session_id(),
			"cwd": self.cwd,
			"stop_hook_active": stop_hook_active,
		});
		let mut decision = StopDecision::default();
		for run in self.run_event(HookEvent::Stop, None, payload).await {
			if let Some(reason) = run.blocking_reason() {
				decision.blocked = Some(reason);
				break;
			}
			if let Some(reason) = run.failure_reason() {
				if run.fail_closed {
					decision.blocked = Some(reason);
					break;
				}
				continue;
			}
			if let Some(reply) = &run.reply
				&& let Some(message) = &reply.system_message
			{
				decision.system_messages.push(message.clone());
			}
		}
		decision
	}

	async fn run_event(&self, event: HookEvent, tool: Option<&str>, payload: Value) -> Vec<RuleRun> {
		let mut runs = Vec::new();
		for rule in self
			.rules
			.iter()
			.filter(|rule| rule.event == event && rule.matches(tool))
		{
			runs.push(self.run_rule_logged(rule, &payload).await);
		}
		runs
	}

	async fn run_rule_logged(&self, rule: &HookRule, payload: &Value) -> RuleRun {
		let run = self.run_rule(rule, payload).await;
		if let Some(reason) = run.failure_reason()
			&& !run.fail_closed
		{
			eprintln!(
				"warning: {} hook failed: {reason}",
				rule.event.as_str()
			);
		}
		run
	}

	async fn run_rule(&self, rule: &HookRule, payload: &Value) -> RuleRun {
		let bytes = match serde_json::to_vec(payload) {
			Ok(bytes) => bytes,
			Err(error) => {
				return RuleRun::failed(rule, format!("cannot encode hook payload: {error}"));
			},
		};
		let mut command = match &rule.invocation {
			HookInvocation::Argv(argv) => {
				let mut command = Command::new(&argv[0]);
				command.args(&argv[1..]);
				command
			},
			HookInvocation::Shell(script) => {
				#[cfg(unix)]
				let command = {
					let mut command = Command::new("sh");
					command.args(["-c", script]);
					command
				};
				#[cfg(not(unix))]
				let command = {
					let mut command = Command::new("cmd");
					command.args(["/C", script]);
					command
				};
				command
			},
		};
		// Hooks inherit only the platform allowlist: provider keys, `PI_*`
		// knobs, and agent sockets must not leak into arbitrary hook scripts.
		command
			.env_clear()
			.envs(crate::state::platform_env(crate::state::utf8_env_vars()))
			.current_dir(&self.cwd)
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.kill_on_drop(true);
		#[cfg(unix)]
		command.process_group(0);

		let mut child = match command.spawn() {
			Ok(child) => child,
			Err(error) => {
				return RuleRun::failed(rule, format!("cannot start hook: {error}"));
			},
		};
		let pid = child.id();
		// Cancellation (turn abort) drops this future without reaching the
		// timeout arm; the guard then kills the whole process group.
		let mut group = ProcessGroupGuard::new(pid);
		let stdin = child.stdin.take();
		let stdout = child.stdout.take();
		let stderr = child.stderr.take();
		let io = async {
			let (_, stdout, stderr, status) = tokio::join!(
				write_payload(stdin, &bytes),
				read_limited(stdout),
				read_limited(stderr),
				child.wait(),
			);
			(stdout, stderr, status)
		};
		tokio::pin!(io);
		let deadline = Duration::from_millis(rule.timeout_ms.max(1));
		match tokio::time::timeout(deadline, &mut io).await {
			Ok((stdout, stderr, Ok(status))) => {
				group.disarm();
				RuleRun::from_output(rule, status.code(), false, stdout, stderr)
			},
			Ok((stdout, stderr, Err(error))) => {
				group.disarm();
				let mut run = RuleRun::from_output(rule, None, false, stdout, stderr);
				run.spawn_error = Some(format!("wait failed: {error}"));
				run
			},
			Err(_) => {
				group.kill();
				let mut run =
					RuleRun::failed(rule, format!("hook timed out after {} ms", rule.timeout_ms));
				run.timed_out = true;
				run
			},
		}
	}
}

#[derive(Debug)]
struct RuleRun {
	fail_closed: bool,
	status: Option<i32>,
	timed_out: bool,
	spawn_error: Option<String>,
	stdout: String,
	stderr: String,
	reply: Option<HookReply>,
}

impl RuleRun {
	fn from_output(
		rule: &HookRule,
		status: Option<i32>,
		timed_out: bool,
		stdout: String,
		stderr: String,
	) -> Self {
		let reply = if status == Some(0) && !stdout.trim().is_empty() {
			serde_json::from_str::<HookReply>(&stdout).ok()
		} else {
			None
		};
		Self {
			fail_closed: rule.fail_closed,
			status,
			timed_out,
			spawn_error: None,
			stdout,
			stderr,
			reply,
		}
	}

	fn failed(rule: &HookRule, error: String) -> Self {
		Self {
			fail_closed: rule.fail_closed,
			status: None,
			timed_out: false,
			spawn_error: Some(error),
			stdout: String::new(),
			stderr: String::new(),
			reply: None,
		}
	}

	/// Exit code 2, or a JSON reply that blocks/denies.
	fn blocking_reason(&self) -> Option<String> {
		if self.status == Some(2) {
			let stderr = self.stderr.trim();
			let stdout = self.stdout.trim();
			return Some(if stderr.is_empty() {
				if stdout.is_empty() {
					"blocked by hook".to_string()
				} else {
					stdout.to_string()
				}
			} else {
				stderr.to_string()
			});
		}
		if self.status != Some(0) {
			return None;
		}
		let reply = self.reply.as_ref()?;
		if reply.continue_ == Some(false) {
			return Some(
				reply
					.reason
					.clone()
					.unwrap_or_else(|| "blocked by hook".to_string()),
			);
		}
		if matches!(reply.decision.as_deref(), Some("block" | "deny")) {
			return Some(
				reply
					.reason
					.clone()
					.unwrap_or_else(|| "blocked by hook".to_string()),
			);
		}
		let spec = reply.hook_specific_output.as_ref()?;
		if matches!(spec.permission_decision.as_deref(), Some("deny" | "block")) {
			return Some(
				spec
					.permission_decision_reason
					.clone()
					.or_else(|| reply.reason.clone())
					.unwrap_or_else(|| "denied by hook".to_string()),
			);
		}
		None
	}

	fn failure_reason(&self) -> Option<String> {
		if self.timed_out {
			return Some("hook timed out".to_string());
		}
		if let Some(error) = &self.spawn_error {
			return Some(error.clone());
		}
		match self.status {
			Some(0 | 2) => None,
			Some(code) => Some(format!(
				"hook exited {code}{}",
				if self.stderr.trim().is_empty() {
					String::new()
				} else {
					format!(": {}", self.stderr.trim())
				}
			)),
			None => Some("hook terminated by signal".to_string()),
		}
	}
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HookReply {
	#[serde(default, rename = "continue", alias = "continue_")]
	continue_: Option<bool>,
	#[serde(default)]
	decision: Option<String>,
	#[serde(default)]
	reason: Option<String>,
	#[serde(default)]
	system_message: Option<String>,
	#[serde(default)]
	hook_specific_output: Option<HookSpecificOutput>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HookSpecificOutput {
	#[serde(default)]
	permission_decision: Option<String>,
	#[serde(default)]
	permission_decision_reason: Option<String>,
	#[serde(default)]
	updated_input: Option<Value>,
	#[serde(default)]
	additional_context: Option<String>,
}

fn summarize(runs: &[RuleRun]) -> HookSummary {
	let mut summary = HookSummary::default();
	for run in runs {
		if let Some(reply) = &run.reply {
			collect_messages(reply, &mut summary.additional_context, &mut summary.system_messages);
		} else if run.status == Some(0) {
			let text = run.stdout.trim();
			if !text.is_empty() {
				summary.additional_context.push(text.to_string());
			}
		}
	}
	summary
}

fn collect_messages(reply: &HookReply, context: &mut Vec<String>, system: &mut Vec<String>) {
	if let Some(spec) = &reply.hook_specific_output
		&& let Some(text) = spec.additional_context.as_deref().map(str::trim)
		&& !text.is_empty()
	{
		context.push(text.to_string());
	}
	if let Some(text) = reply.system_message.as_deref().map(str::trim)
		&& !text.is_empty()
	{
		system.push(text.to_string());
	}
	if let Some(text) = reply.reason.as_deref().map(str::trim)
		&& !text.is_empty()
		&& reply.decision.is_none()
	{
		context.push(text.to_string());
	}
}

/// `updatedInput` replaces a non-object input; object input receives a
/// shallow patch. Hooks therefore can add or replace known arguments without
/// dropping fields they did not mention.
fn merge_updated_input(current: &mut Value, updated: &Value) {
	match (current, updated) {
		(Value::Object(target), Value::Object(patch)) => {
			for (key, value) in patch {
				target.insert(key.clone(), value.clone());
			}
		},
		(target, replacement) => *target = replacement.clone(),
	}
}

async fn write_payload(stdin: Option<ChildStdin>, bytes: &[u8]) {
	if let Some(mut stdin) = stdin {
		let _ = stdin.write_all(bytes).await;
		let _ = stdin.shutdown().await;
	}
}

async fn read_limited<R>(reader: Option<R>) -> String
where
	R: AsyncRead + Unpin,
{
	let Some(reader) = reader else {
		return String::new();
	};
	let mut buffer = Vec::new();
	let mut limited = reader.take(MAX_HOOK_OUTPUT_BYTES as u64);
	let _ = limited.read_to_end(&mut buffer).await;
	String::from_utf8_lossy(&buffer).into_owned()
}

struct ProcessGroupGuard {
	pid: Option<u32>,
}

impl ProcessGroupGuard {
	fn new(pid: Option<u32>) -> Self {
		Self { pid }
	}

	fn disarm(&mut self) {
		self.pid = None;
	}

	fn kill(&mut self) {
		kill_group(self.pid.take());
	}
}

impl Drop for ProcessGroupGuard {
	fn drop(&mut self) {
		self.kill();
	}
}

fn kill_group(pid: Option<u32>) {
	#[cfg(unix)]
	if let Some(pid) = pid.and_then(|pid| i32::try_from(pid).ok()) {
		// `kill_on_drop` still reaps the direct child; the group signal also
		// reaches anything the hook spawned.
		// SAFETY: `kill` tolerates an already-reaped pid (ESRCH); the pid was
		// returned by `spawn` and only ever widened to its process group.
		unsafe {
			libc::kill(-pid, libc::SIGKILL);
		}
	}
	#[cfg(not(unix))]
	let _ = pid;
}

#[cfg(all(test, unix))]
mod tests {
	use serde_json::json;

	use super::*;

	fn shell(event: HookEvent, script: &str) -> HookConfig {
		HookConfig::new(vec![HookRule {
			event,
			matcher: None,
			invocation: HookInvocation::Shell(script.to_string()),
			timeout_ms: 2_000,
			fail_closed: false,
		}])
	}

	fn engine(config: HookConfig) -> HookEngine {
		HookEngine::new(config, "session-test", std::env::temp_dir())
	}

	#[test]
	fn parses_claude_matcher_groups_and_codex_flat_entries() {
		let config = HookConfig::from_json(&json!({
			"hooks": {
				"PreToolUse": [{
					"matcher": "Bash",
					"hooks": [{"type": "command", "command": "printf ok", "timeout": 2}]
				}],
				"Stop": [{"command": ["/bin/true"]}],
				"UnknownEvent": [{"command": "ignored"}]
			}
		}))
		.expect("hook config");
		assert_eq!(config.rules().len(), 2);
		assert_eq!(config.rules()[0].matcher.as_deref(), Some("Bash"));
		assert_eq!(config.rules()[0].timeout_ms, 2_000);
		assert!(matches!(config.rules()[1].invocation, HookInvocation::Argv(_)));
		assert!(config.rules()[0].matches(Some("Bash")));
		assert!(!config.rules()[0].matches(Some("Read")));
	}

	#[test]
	fn malformed_event_shape_is_rejected() {
		let error = HookConfig::from_json(&json!({"hooks": {"PreToolUse": "nope"}})).unwrap_err();
		assert!(error.to_string().contains("must be an array"));
	}

	#[tokio::test]
	async fn pre_tool_use_applies_updated_input_and_context() {
		let config = shell(
			HookEvent::PreToolUse,
			"cat >/dev/null; printf '%s' '{\"hookSpecificOutput\":{\"updatedInput\":{\"path\":\"/tmp/hooked\"},\"additionalContext\":\"seen\"}}'",
		);
		let decision = engine(config)
			.pre_tool_use("read", &json!({"path": "/tmp/original"}))
			.await;
		assert!(decision.denied.is_none());
		assert_eq!(decision.updated_input, Some(json!({"path": "/tmp/hooked"})));
		assert_eq!(decision.additional_context, ["seen"]);
	}

	#[tokio::test]
	async fn pre_tool_use_exit_two_blocks_with_stderr() {
		let config = shell(HookEvent::PreToolUse, "printf 'nope' >&2; exit 2");
		let decision = engine(config).pre_tool_use("bash", &json!({})).await;
		assert_eq!(decision.denied.as_deref(), Some("nope"));
	}

	#[tokio::test]
	async fn pre_tool_use_json_deny_blocks() {
		let config = shell(
			HookEvent::PreToolUse,
			"printf '%s' '{\"hookSpecificOutput\":{\"permissionDecision\":\"deny\",\"permissionDecisionReason\":\"policy\"}}'",
		);
		let decision = engine(config).pre_tool_use("bash", &json!({})).await;
		assert_eq!(decision.denied.as_deref(), Some("policy"));
	}

	#[tokio::test]
	async fn timeout_fails_open_by_default_and_closed_when_configured() {
		let mut open = shell(HookEvent::PreToolUse, "sleep 5");
		open.rules[0].timeout_ms = 50;
		assert!(
			engine(open)
				.pre_tool_use("bash", &json!({}))
				.await
				.denied
				.is_none()
		);

		let mut closed = shell(HookEvent::PreToolUse, "sleep 5");
		closed.rules[0].timeout_ms = 50;
		closed.rules[0].fail_closed = true;
		assert!(
			engine(closed)
				.pre_tool_use("bash", &json!({}))
				.await
				.denied
				.is_some()
		);
	}

	#[tokio::test]
	async fn post_tool_use_returns_additional_context() {
		let config = shell(
			HookEvent::PostToolUse,
			"cat >/dev/null; printf '%s' '{\"hookSpecificOutput\":{\"additionalContext\":\"post note\"}}'",
		);
		let decision = engine(config)
			.post_tool_use("read", &json!({}), "file body", false)
			.await;
		assert_eq!(decision.additional_context, ["post note"]);
		assert!(decision.blocked.is_none());
	}

	#[tokio::test]
	async fn stop_block_decision_is_surfaced() {
		let config =
			shell(HookEvent::Stop, "printf '%s' '{\"decision\":\"block\",\"reason\":\"keep going\"}'");
		let decision = engine(config).stop(false).await;
		assert_eq!(decision.blocked.as_deref(), Some("keep going"));
	}

	#[tokio::test]
	async fn stop_exit_two_blocks_with_stderr() {
		let config = shell(HookEvent::Stop, "printf 'run tests first' >&2; exit 2");
		let decision = engine(config).stop(false).await;
		assert_eq!(decision.blocked.as_deref(), Some("run tests first"));
	}

	#[tokio::test]
	async fn permission_request_allow_and_deny() {
		let allow = shell(
			HookEvent::PermissionRequest,
			"printf '%s' '{\"hookSpecificOutput\":{\"permissionDecision\":\"allow\"}}'",
		);
		assert_eq!(
			engine(allow).permission_request("write", &json!({})).await,
			Some(PermissionDecision::Allow)
		);
		let deny = shell(HookEvent::PermissionRequest, "exit 2; true");
		assert!(matches!(
			engine(deny).permission_request("write", &json!({})).await,
			Some(PermissionDecision::Deny(_))
		));
	}

	#[tokio::test]
	async fn session_start_runs_once_and_sees_event_payload() {
		let config = shell(
			HookEvent::SessionStart,
			"payload=$(cat); printf '%s' \"$payload\" | grep -q 'SessionStart' && printf '%s' '{\"hookSpecificOutput\":{\"additionalContext\":\"started\"}}'",
		);
		let engine = engine(config);
		assert_eq!(engine.session_start().await.additional_context, ["started"]);
		// Later turns receive the cached context so a non-durable host can
		// reinject it without replaying the hook process.
		assert_eq!(engine.session_start().await.additional_context, ["started"]);
	}

	#[tokio::test]
	async fn plain_stdout_is_accepted_as_context() {
		let config = shell(HookEvent::SessionStart, "printf 'plain note'");
		assert_eq!(engine(config).session_start().await.additional_context, ["plain note"]);
	}

	#[tokio::test]
	async fn argv_hook_does_not_use_a_shell() {
		let config = HookConfig::new(vec![HookRule {
			event: HookEvent::SessionStart,
			matcher: None,
			invocation: HookInvocation::Argv(vec![
				"sh".to_string(),
				"-c".to_string(),
				"printf 'argv ok'".to_string(),
			]),
			timeout_ms: 2_000,
			fail_closed: false,
		}]);
		assert_eq!(engine(config).session_start().await.additional_context, ["argv ok"]);
	}

	#[tokio::test]
	async fn updated_input_chains_across_pre_tool_use_hooks() {
		let config = HookConfig::new(vec![
			HookRule {
				event: HookEvent::PreToolUse,
				matcher: None,
				invocation: HookInvocation::Shell(
					r#"printf '%s' '{"hookSpecificOutput":{"updatedInput":{"path":"step1"}}}'"#
						.to_string(),
				),
				timeout_ms: 2_000,
				fail_closed: false,
			},
			HookRule {
				event: HookEvent::PreToolUse,
				matcher: None,
				invocation: HookInvocation::Shell(
					r#"payload=$(cat); printf '%s' "$payload" | grep -q '"path":"step1"' && printf '%s' '{"hookSpecificOutput":{"updatedInput":{"path":"step2"}}}'"#
						.to_string(),
				),
				timeout_ms: 2_000,
				fail_closed: false,
			},
		]);
		let decision = engine(config)
			.pre_tool_use("record", &json!({"path": "original"}))
			.await;
		assert_eq!(
			decision.updated_input,
			Some(json!({"path": "step2"})),
			"the second hook must see the first rewrite"
		);
	}

	#[tokio::test]
	async fn dropping_hook_future_kills_the_process_group() {
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("clock")
			.as_nanos();
		let marker = std::env::temp_dir().join(format!(
			"omp-hook-cancel-{}-{nanos}",
			std::process::id()
		));
		let ready = std::env::temp_dir().join(format!(
			"omp-hook-cancel-ready-{}-{nanos}",
			std::process::id()
		));
		let script = format!(
			"if [ -f '{}' ]; then printf '{{\"hookSpecificOutput\":{{\"additionalContext\":\"retried\"}}}}'; else touch '{}'; sleep 30 & echo $! > '{}'; wait; fi",
			ready.display(),
			ready.display(),
			marker.display()
		);
		let hook_engine = engine(shell(HookEvent::SessionStart, &script));
		let mut future = Box::pin(hook_engine.session_start());
		tokio::select! {
			_ = &mut future => panic!("hook completed before cancellation"),
			_ = tokio::time::sleep(Duration::from_millis(250)) => {},
		}
		let pid: i32 = std::fs::read_to_string(&marker)
			.expect("hook pid file")
			.trim()
			.parse()
			.expect("hook pid");
		drop(future);
		let mut alive = true;
		for _ in 0..40 {
			let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
			alive = !stat.is_empty() && stat.split_whitespace().nth(2) != Some("Z");
			if !alive {
				break;
			}
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
		let _ = std::fs::remove_file(&marker);
		assert!(!alive, "hook process group survived cancellation");
		// The cancelled initialization must not be cached as empty: the next
		// caller reruns `SessionStart` and receives its result.
		let summary = hook_engine.session_start().await;
		let _ = std::fs::remove_file(&ready);
		assert_eq!(summary.additional_context, ["retried"]);
	}

	#[tokio::test]
	async fn concurrent_session_start_waits_for_the_first_run() {
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("clock")
			.as_nanos();
		let count = std::env::temp_dir().join(format!(
			"omp-hook-once-{}-{nanos}",
			std::process::id()
		));
		let script = format!(
			"printf x >> '{}'; sleep 0.2; printf '%s' '{{\"hookSpecificOutput\":{{\"additionalContext\":\"once\"}}}}'",
			count.display()
		);
		let hook_engine = engine(shell(HookEvent::SessionStart, &script));
		let (first, second) = tokio::join!(
			hook_engine.session_start(),
			hook_engine.session_start()
		);
		assert_eq!(first.additional_context, ["once"]);
		assert_eq!(second.additional_context, ["once"]);
		let runs = std::fs::read_to_string(&count).unwrap_or_default();
		let _ = std::fs::remove_file(&count);
		assert_eq!(runs, "x", "SessionStart ran more than once");
	}
}
