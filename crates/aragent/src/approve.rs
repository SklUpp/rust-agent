//! Tool approval matrix (`--approval-mode`, `--allow`).
//!
//! `always-ask` auto-approves the read tier and prompts on stdin for writes and
//! execs, `write` auto-approves read-only tools, and `yolo` approves everything;
//! an explicit `--allow <tool>` always approves that tool. Mirrors
//! `packages/coding-agent` approval semantics.
//!
//! Bash command policy (`crate::config::BashConfig`) runs before this matrix:
//! `Forbidden` is a hard deny that `yolo`, `--allow bash`, and a remote
//! approver cannot override, and `Prompt` forces an ask even when the matrix
//! would auto-approve.

use std::collections::BTreeSet;
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use clap::ValueEnum;
use pi_agent::shell_policy::{BashPolicy, PolicyDecision, PolicyVerdict};

use crate::{Error, Result};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum ApprovalMode {
	/// Auto-approve read-tier tools; ask before write/exec calls.
	#[default]
	#[value(name = "always-ask")]
	AlwaysAsk,
	/// Auto-approve read-tier tools; ask before write/exec calls.
	/// Note: In TS `approval.ts`, `write` mode approves up to `write` tier.
	/// The Android headless parity plan explicitly defines `write` mode as
	/// read-only auto-permitted, requiring explicit approval for writes and execs.
	Write,
	/// Auto-approve everything.
	Yolo,
}

impl ApprovalMode {
	#[must_use]
	pub fn parse(raw: &str) -> Option<Self> {
		match raw {
			"always-ask" => Some(Self::AlwaysAsk),
			"write" => Some(Self::Write),
			"yolo" => Some(Self::Yolo),
			_ => None,
		}
	}

	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::AlwaysAsk => "always-ask",
			Self::Write => "write",
			Self::Yolo => "yolo",
		}
	}

	/// Higher means more auto-approval. Project config layers may only lower
	/// or keep this value; see `Config::apply_project_layers`.
	#[must_use]
	pub fn permissiveness(self) -> u8 {
		match self {
			Self::AlwaysAsk => 0,
			Self::Write => 1,
			Self::Yolo => 2,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
	/// Run without asking.
	Auto,
	/// Ask the operator on stdin.
	Ask,
}

/// Capability tier of a tool invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ToolTier {
	Read,
	Write,
	Exec,
}

/// Check if arguments target an ssh:// URI.
pub fn has_ssh_target(args: &serde_json::Value) -> bool {
	let is_ssh = |s: &str| s.starts_with("ssh://");
	if let Some(p) = args.get("path").and_then(|v| v.as_str()) {
		if is_ssh(p) {
			return true;
		}
	}
	if let Some(paths) = args.get("paths") {
		if let Some(s) = paths.as_str() {
			if is_ssh(s) {
				return true;
			}
		} else if let Some(arr) = paths.as_array() {
			for item in arr {
				if let Some(s) = item.as_str() {
					if is_ssh(s) {
						return true;
					}
				}
			}
		}
	}
	false
}

/// Classify tool invocation into a capability tier.
pub fn classify_tool(name: &str, args: &serde_json::Value) -> ToolTier {
	match name {
		"todo" | "glob" | "ask" | "ast_grep" | "web_search" => ToolTier::Read,
		"read" => {
			if has_ssh_target(args) {
				ToolTier::Exec
			} else {
				ToolTier::Read
			}
		},
		"grep" => {
			if has_ssh_target(args) {
				ToolTier::Exec
			} else {
				ToolTier::Read
			}
		},
		"write" => {
			let path = args.get("path").and_then(|v| v.as_str());
			if has_ssh_target(args) {
				ToolTier::Exec
			} else if path.is_some_and(pi_agent::tools::pending::is_resolution_device_path) {
				// `xd://resolve|reject` only finalizes an already-approved
				// staged preview: the queued action carries the approval that
				// staged it, so the dispatch itself is read tier (upstream
				// `write.ts` parity). Unknown xd:// targets stay write tier and
				// fail closed in the tool.
				ToolTier::Read
			} else {
				ToolTier::Write
			}
		},
		"edit" | "apply_patch" => {
			if has_ssh_target(args) {
				ToolTier::Exec
			} else {
				ToolTier::Write
			}
		},
		// Previewing an AST rewrite mutates files only after `write
		// xd://resolve`; the staging call itself is a write-tier action, and the
		// dispatch stays Read like `write`.
		"ast_edit" => ToolTier::Write,
		// Security publication mutates the results index / artifacts. The tool
		// only exists in the restricted scan worker, but its approval tier must
		// still be a write when a host inspects it.
		// The root coordinator tool only reads/preflights/starts; the worker
		// owns publication and is the only write surface.
		"security_scan" => ToolTier::Read,
		"security_publish" => ToolTier::Write,
		"bash" => ToolTier::Exec,
		// Upstream classifies LSP per action: rename/reload/raw `request`/format
		// mutate state, so they need explicit approval even though the tool is
		// mostly read-only.
		"lsp" => {
			let action = args
				.get("action")
				.and_then(|v| v.as_str())
				.unwrap_or_default()
				.to_ascii_lowercase();
			if pi_agent::lsp::is_readonly_lsp_action(&action) {
				ToolTier::Read
			} else {
				ToolTier::Write
			}
		},
		// Debug mutate/debuggee actions are exec-tier, matching `debug.ts`.
		"debug" => {
			let action = args.get("action").and_then(|v| v.as_str()).unwrap_or_default();
			if pi_agent::dap::is_readonly_action(action) {
				ToolTier::Read
			} else {
				ToolTier::Exec
			}
		},
		// Auto-learn mutates durable memory and the managed-skill library.
		// Upstream `learn` is write-tier for a skill payload or the local
		// backend (the only backend wired here); `manage_skill` is always write.
		"learn" | "manage_skill" => ToolTier::Write,
		// MCP tools and unknown tools default to conservative Exec
		_ => ToolTier::Exec,
	}
}

/// Extract detail description for approval prompts.
pub fn extract_tool_detail(name: &str, args: &serde_json::Value) -> Option<String> {
	match name {
		"bash" => args.get("command").and_then(|v| v.as_str()).map(|cmd| {
			let trimmed = cmd.trim();
			if trimmed.chars().count() > 60 {
				let prefix: String = trimmed.chars().take(57).collect();
				format!("{prefix}...")
			} else {
				trimmed.to_string()
			}
		}),
		"write" | "edit" | "read" => {
			args.get("path").and_then(|v| v.as_str()).map(|p| p.trim().to_string())
		},
		// Upstream `formatApprovalDetails`: the action (and file/target) is what
		// the operator needs to judge an `lsp`/`debug` prompt.
		"lsp" => {
			let action = args.get("action").and_then(|v| v.as_str()).unwrap_or("(missing)");
			match args.get("file").and_then(|v| v.as_str()).filter(|file| !file.is_empty()) {
				Some(file) => Some(format!("action: {action}, file: {file}")),
				None => Some(format!("action: {action}")),
			}
		},
		"debug" => {
			let action = args.get("action").and_then(|v| v.as_str()).unwrap_or("(missing)");
			match args
				.get("program")
				.and_then(|v| v.as_str())
				.filter(|program| !program.is_empty())
			{
				Some(program) => Some(format!("action: {action}, program: {program}")),
				None => Some(format!("action: {action}")),
			}
		},
		_ => None,
	}
}

pub type ApprovalInput = std::sync::Arc<std::sync::Mutex<Box<dyn BufRead + Send>>>;

#[derive(Clone, Default)]
pub struct Approvals {
	pub mode: ApprovalMode,
	pub allow: BTreeSet<String>,
	/// Interactive `ask` surface; absent in headless runs and child agents.
	pub ask: Option<std::sync::Arc<dyn pi_agent::tools::ask::AskChannel>>,
	/// Injected approval input (tests, embedders). `None` reads the process
	/// terminal, which stays interactive-only.
	input: Option<ApprovalInput>,
	/// Alternate approver for transports that ask a remote peer (ACP
	/// `session/request_permission`). The alternate owns the policy decision;
	/// this matrix is not consulted when it is installed.
	approver: Option<std::sync::Arc<dyn pi_agent::ToolApprover>>,
	/// Bash command policy. Built-in destructive rules are always active;
	/// `bash.patterns` / `bashInterceptor` from config layer on top.
	bash_policy: BashPolicy,
	/// Trusted subprocess hooks; only `PermissionRequest` is consulted here,
	/// after the bash policy has already rejected forbidden commands.
	hooks: Option<std::sync::Arc<pi_agent::hooks::HookEngine>>,
}

impl std::fmt::Debug for Approvals {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Approvals")
			.field("mode", &self.mode)
			.field("allow", &self.allow)
			.field("has_ask", &self.ask.is_some())
			.field("has_input", &self.input.is_some())
			.field("has_approver", &self.approver.is_some())
			.field("bash_policy_rules", &self.bash_policy.rules().len())
			.field("has_hooks", &self.hooks.is_some())
			.finish()
	}
}

impl Approvals {
	#[must_use]
	pub fn new(mode: ApprovalMode, allow: impl IntoIterator<Item = String>) -> Self {
		Self {
			mode,
			allow: allow.into_iter().collect(),
			ask: None,
			input: None,
			approver: None,
			bash_policy: BashPolicy::default(),
			hooks: None,
		}
	}

	/// Install the bash command policy. [`Approvals::new`] already enables the
	/// built-in destructive rules; hosts layer `Config::bash` rules on top.
	#[must_use]
	pub fn with_bash_policy(mut self, policy: BashPolicy) -> Self {
		self.bash_policy = policy;
		self
	}

	/// Install or clear the trusted subprocess hook engine. `PermissionRequest`
	/// runs only after the bash policy has allowed the command.
	pub fn set_hooks(&mut self, hooks: Option<std::sync::Arc<pi_agent::hooks::HookEngine>>) {
		self.hooks = hooks;
	}

	#[must_use]
	pub fn with_hooks(
		mut self,
		hooks: Option<std::sync::Arc<pi_agent::hooks::HookEngine>>,
	) -> Self {
		self.set_hooks(hooks);
		self
	}

	#[must_use]
	pub fn hooks(&self) -> Option<std::sync::Arc<pi_agent::hooks::HookEngine>> {
		self.hooks.clone()
	}

	/// Install or clear the alternate approver. The matrix still resolves
	/// explicit `Auto` decisions (`yolo`, `--allow`, per-run registered MCP
	/// names); the alternate decides every `Ask` outcome. This keeps a remote
	/// peer (ACP) and a local front-end (TUI) from re-implementing the allow
	/// list while neither can silently read the terminal.
	pub fn set_approver(&mut self, approver: Option<std::sync::Arc<dyn pi_agent::ToolApprover>>) {
		self.approver = approver;
	}

	/// The alternate approver, when a front-end installed one. Hosts that ask
	/// outside the tool-execution path (MCP stdio launch) must route through it;
	/// the front-end owns the terminal, so reading stdin here would race it.
	#[must_use]
	pub(crate) fn alternate_approver(
		&self,
	) -> Option<std::sync::Arc<dyn pi_agent::ToolApprover>> {
		self.approver.clone()
	}

	/// Install or clear an injected approval input. Injected input never consults
	/// the process terminal, so tests and embedders control the answer.
	pub fn set_input(&mut self, input: Option<ApprovalInput>) {
		self.input = input;
	}

	#[must_use]
	pub fn with_input(mut self, input: Option<ApprovalInput>) -> Self {
		self.set_input(input);
		self
	}

	/// Install or clear the interactive `ask` surface.
	pub fn set_ask(&mut self, ask: Option<std::sync::Arc<dyn pi_agent::tools::ask::AskChannel>>) {
		self.ask = ask;
	}

	/// Copy without the interactive surface, for delegated children.
	#[must_use]
	pub fn without_ask(&self) -> Self {
		let mut clone = self.clone();
		clone.ask = None;
		// A child must never inherit the parent's remote approver: that would
		// prompt the ACP client for delegated work. Without it the matrix
		// applies and Ask-tier work fails closed on closed stdin.
		clone.approver = None;
		clone
	}

	#[must_use]
	pub fn ask_channel(&self) -> Option<std::sync::Arc<dyn pi_agent::tools::ask::AskChannel>> {
		self
			.ask
			.clone()
			.or_else(|| self.approver.as_ref().and_then(|approver| approver.ask_channel()))
	}

	/// Authorize a registered MCP tool name if the exact colon tool allow
	/// `mcp:<server_name>:<tool_name>` was permitted.
	pub fn allow_mcp_tool_origin(&mut self, server_name: &str, tool_name: &str, registered_name: &str) {
		let colon_allow = format!("mcp:{server_name}:{tool_name}");
		if self.allow.contains(&colon_allow) {
			self.allow.insert(registered_name.to_string());
		}
	}

	/// Resolve approval decision given explicit tool tier.
	///
	/// Both non-yolo modes auto-approve the `read` tier, matching
	/// `APPROVAL_MODE_MAX_TIER` upstream: `always-ask` permits reads and prompts
	/// for writes/execs; this port's `write` mode deliberately stays
	/// read-only-auto and prompts for writes/execs.
	///
	/// The `apply_patch` wire name also matches an explicit allow for its
	/// internal tool name `edit`; the raw name is checked first so an explicit
	/// `--allow apply_patch` keeps working.
	#[must_use]
	pub fn decide_tier(&self, tool: &str, tier: ToolTier) -> Decision {
		let allowed =
			self.allow.contains(tool) || (tool == "apply_patch" && self.allow.contains("edit"));
		if allowed || self.mode == ApprovalMode::Yolo {
			return Decision::Auto;
		}
		if tier == ToolTier::Read {
			return Decision::Auto;
		}
		Decision::Ask
	}

	/// Pure matrix: explicit allow, then `yolo`, then the `read` tier.
	#[must_use]
	pub fn decide(&self, tool: &str, read_only: bool) -> Decision {
		let tier = if read_only { ToolTier::Read } else { ToolTier::Write };
		self.decide_tier(tool, tier)
	}

	/// Decide, prompting on `input`/`prompt` when the matrix says ask.
	pub fn confirm(
		&self,
		tool: &str,
		read_only: bool,
		detail: Option<&str>,
		input: &mut impl BufRead,
		prompt: &mut impl Write,
	) -> Result<bool> {
		self.confirm_decision(self.decide(tool, read_only), tool, detail, input, prompt)
	}

	/// Prompt for an already-resolved decision. Unlike [`Self::confirm`] this
	/// never re-consults the matrix, so a policy-forced `Ask` still prompts
	/// under `yolo` or `--allow bash`.
	fn confirm_decision(
		&self,
		decision: Decision,
		tool: &str,
		detail: Option<&str>,
		input: &mut impl BufRead,
		prompt: &mut impl Write,
	) -> Result<bool> {
		if decision == Decision::Auto {
			return Ok(true);
		}
		write!(prompt, "{}", approval_prompt(tool, detail))?;
		prompt.flush()?;
		let mut line = String::new();
		if input.read_line(&mut line)? == 0 {
			return Err(Error::new("approval required but stdin closed"));
		}
		let answer = line.trim().to_ascii_lowercase();
		Ok(matches!(answer.as_str(), "y" | "yes"))
	}

	/// Policy verdict for one tool call. Shell policy applies to `bash` and to
	/// `hub` `start`, whose `application`/`args` launch a child process.
	fn bash_verdict(&self, name: &str, args: &serde_json::Value) -> PolicyVerdict {
		if name == "hub" {
			if args.get("op").and_then(serde_json::Value::as_str) != Some("start") {
				return PolicyVerdict::allow();
			}
			let Some(command) = hub_start_command(args) else {
				return PolicyVerdict::allow();
			};
			let mut verdict = self.bash_policy.evaluate_for_hub(&command);
			// A `bash.patterns` allow rule approves a `bash` call, never a `hub`
			// `start`: that stays behind the matrix (`--allow hub`, yolo, prompt).
			verdict.explicit_allow = false;
			return verdict;
		}
		if name != "bash" {
			return PolicyVerdict::allow();
		}
		args
			.get("command")
			.and_then(|value| value.as_str())
			.map_or_else(PolicyVerdict::allow, |command| self.bash_policy.evaluate(command))
	}
}

/// Shell line for a `hub` `start` call so the bash policy inspects the same
/// executable and argv the process backend launches. `None` when
/// `application` is missing or blank.
fn hub_start_command(args: &serde_json::Value) -> Option<String> {
	let application = args
		.get("application")
		.and_then(serde_json::Value::as_str)
		.map(str::trim)
		.filter(|application| !application.is_empty())?;
	let mut line = shell_quote_token(application);
	for argument in args
		.get("args")
		.and_then(serde_json::Value::as_array)
		.into_iter()
		.flatten()
		.filter_map(serde_json::Value::as_str)
	{
		line.push(' ');
		line.push_str(&shell_quote_token(argument));
	}
	Some(line)
}

/// Leave a token bare when a POSIX shell would pass it through unchanged,
/// otherwise single-quote it (`'` becomes `'\''`) so the policy parser reads
/// one argv word and expansions stay inert.
fn shell_quote_token(token: &str) -> String {
	if !token.is_empty()
		&& token
			.chars()
			.all(|character| character.is_ascii_alphanumeric() || "%+,-./:=@_".contains(character))
	{
		return token.to_string();
	}
	let mut quoted = String::with_capacity(token.len() + 2);
	quoted.push('\'');
	for character in token.chars() {
		if character == '\'' {
			quoted.push_str("'\\''");
		} else {
			quoted.push(character);
		}
	}
	quoted.push('\'');
	quoted
}

#[async_trait]
impl pi_agent::ToolApprover for Approvals {
	fn ask_channel(&self) -> Option<std::sync::Arc<dyn pi_agent::tools::ask::AskChannel>> {
		Approvals::ask_channel(self)
	}

	fn supports_approval_hints(&self) -> bool {
		true
	}

	async fn approve(&self, name: &str, args: &serde_json::Value) -> std::result::Result<(), String> {
		self
			.approve_with_hint(name, args, pi_agent::ApprovalHint::default())
			.await
	}

	async fn approve_with_hint(
		&self,
		name: &str,
		args: &serde_json::Value,
		hint: pi_agent::ApprovalHint,
	) -> std::result::Result<(), String> {
		let tier = classify_tool(name, args);
		// Policy runs before the matrix and before an alternate approver:
		// `Forbidden` is a hard deny (yolo/`--allow bash`/ACP cannot override),
		// and `Prompt` forces an ask even when the matrix would auto-approve.
		let verdict = self.bash_verdict(name, args);
		if verdict.decision == PolicyDecision::Forbidden {
			return Err(
				verdict
					.reason
					.unwrap_or_else(|| format!("blocked by bash policy: {name}")),
			);
		}
		// A forced prompt is operator-owned: neither the matrix, `--allow`, a
		// hook, nor yolo may satisfy it.
		let forced_prompt = verdict.decision == PolicyDecision::Prompt || hint.force_ask;
		if !forced_prompt && verdict.decision == PolicyDecision::Allow && verdict.explicit_allow {
			return Ok(());
		}
		let decision = if forced_prompt {
			Decision::Ask
		} else {
			self.decide_tier(name, tier)
		};
		// A permission hook may answer a matrix prompt, but only when the prompt
		// was not forced by policy or a `PreToolUse` hook. A denial always wins.
		if decision == Decision::Ask
			&& let Some(hooks) = &self.hooks
			&& let Some(hook) = hooks.permission_request(name, args).await
		{
			match hook {
				pi_agent::hooks::PermissionDecision::Allow if !forced_prompt => return Ok(()),
				pi_agent::hooks::PermissionDecision::Allow => {},
				pi_agent::hooks::PermissionDecision::Deny(reason) => return Err(reason),
				pi_agent::hooks::PermissionDecision::Ask => {},
			}
		}
		if let Some(approver) = self.approver.as_ref() {
			// The matrix still owns the explicit auto decisions: `yolo`, `--allow`
			// and per-run registered MCP names all resolve before a front-end is
			// asked. The alternate decides every *ask* outcome.
			if decision == Decision::Auto {
				return Ok(());
			}
			// A legacy front-end cannot distinguish a forced prompt from an
			// ordinary ask, so it must not be allowed to auto-approve one.
			if forced_prompt && !approver.supports_approval_hints() {
				return Err(format!(
					"approval required for {name}, but active approver does not honor forced prompts"
				));
			}
			return approver
				.approve_with_hint(
					name,
					args,
					pi_agent::ApprovalHint { force_ask: forced_prompt },
				)
				.await;
		}
		match decision {
			Decision::Auto => Ok(()),
			Decision::Ask => {
				let detail = extract_tool_detail(name, args);
				if let Some(input) = self.input.clone() {
					// Injected input: never consults the process terminal and never
					// blocks a runtime worker waiting on a real operator.
					let mut input =
						input.lock().map_err(|_| "approval input poisoned".to_string())?;
					let mut prompt = std::io::stderr().lock();
					return match self.confirm_decision(
						decision,
						name,
						detail.as_deref(),
						&mut *input,
						&mut prompt,
					) {
						Ok(true) => Ok(()),
						Ok(false) => Err(format!("approval denied for {name}")),
						Err(err) => Err(err.to_string()),
					};
				}
				use std::io::IsTerminal;
				// In non-interactive environments (pipes, /dev/null, daemons, background),
				// closed stdin cannot prompt the operator; deny immediately before side effects.
				// MCP tool discovery already registered schemas via read-only tools/list;
				// this gate intercepts the execution before any side-effecting tools/call occurs.
				if !std::io::stdin().is_terminal() {
					return Err("approval required but stdin closed".to_string());
				}
				eprint!("{}", approval_prompt(name, detail.as_deref()));
				let _ = std::io::stderr().flush();
				let cancelled = std::sync::Arc::new(AtomicBool::new(false));
				// Dropping the approval future (turn cancellation) flips this flag;
				// the reader stops polling stdin instead of eating the next prompt.
				let _abort = PromptAbort(std::sync::Arc::clone(&cancelled));
				let answer = tokio::task::spawn_blocking(move || read_prompt_answer(cancelled)).await;
				match answer {
					Ok(Ok(true)) => Ok(()),
					Ok(Ok(false)) => Err(format!("approval denied for {name}")),
					Ok(Err(err)) => Err(err),
					Err(err) => Err(format!("approval prompt failed: {err}")),
				}
			},
		}
	}
}

fn approval_prompt(tool: &str, detail: Option<&str>) -> String {
	match detail {
		Some(detail) => format!("Approve {tool} ({detail})? [y/N] "),
		None => format!("Approve {tool}? [y/N] "),
	}
}

/// Flags a dropped approval future so its blocking stdin reader stops polling.
struct PromptAbort(std::sync::Arc<AtomicBool>);

impl Drop for PromptAbort {
	fn drop(&mut self) {
		self.0.store(true, Ordering::SeqCst);
	}
}

/// Read one approval line without blocking cancellation for the whole line.
/// Returns `Err` only when stdin is closed before an answer; an empty line is a
/// denial. The cancellation flag is checked before every read so a cancelled
/// prompt never consumes input intended for the next prompt.
fn read_prompt_answer(cancelled: std::sync::Arc<AtomicBool>) -> std::result::Result<bool, String> {
	let mut buf: Vec<u8> = Vec::new();
	loop {
		if cancelled.load(Ordering::SeqCst) {
			return Ok(false);
		}
		#[cfg(unix)]
		{
			if crate::runtime::poll_stdin_readable(100) {
				if cancelled.load(Ordering::SeqCst) {
					return Ok(false);
				}
				match crate::runtime::read_stdin_byte() {
					Some(b'\n') => break,
					Some(b'\r') => {},
					Some(byte) => buf.push(byte),
					None => return Err("approval required but stdin closed".to_string()),
				}
			}
		}
		#[cfg(not(unix))]
		{
			return Err("approval prompt requires a POSIX terminal".to_string());
		}
	}
	let answer = String::from_utf8_lossy(&buf).trim().to_ascii_lowercase();
	Ok(matches!(answer.as_str(), "y" | "yes"))
}
#[cfg(test)]
mod tests {
	use std::io::Cursor;

	use super::*;

	fn approvals(mode: ApprovalMode, allow: &[&str]) -> Approvals {
		Approvals::new(mode, allow.iter().map(ToString::to_string))
	}

	struct RecordingAlternate {
		supported: bool,
		seen:     std::sync::Mutex<Vec<pi_agent::ApprovalHint>>,
	}

	#[async_trait::async_trait]
	impl pi_agent::ToolApprover for RecordingAlternate {
		fn supports_approval_hints(&self) -> bool {
			self.supported
		}

		async fn approve(&self, _name: &str, _args: &serde_json::Value) -> Result<(), String> {
			Ok(())
		}

		async fn approve_with_hint(
			&self,
			_name: &str,
			_args: &serde_json::Value,
			hint: pi_agent::ApprovalHint,
		) -> Result<(), String> {
			self.seen.lock().expect("hint lock").push(hint);
			Ok(())
		}
	}

	#[test]
	fn matrix_auto_approves_read_and_prompts_side_effects() {
		let ask = approvals(ApprovalMode::AlwaysAsk, &[]);
		assert_eq!(ask.decide("read", true), Decision::Auto);
		assert_eq!(ask.decide("bash", false), Decision::Ask);

		let auto = approvals(ApprovalMode::AlwaysAsk, &["bash"]);
		assert_eq!(auto.decide("bash", false), Decision::Auto);
		assert_eq!(auto.decide("write", false), Decision::Ask);

		let write = approvals(ApprovalMode::Write, &[]);
		assert_eq!(write.decide("read", true), Decision::Auto);
		assert_eq!(write.decide("write", false), Decision::Ask);

		let yolo = approvals(ApprovalMode::Yolo, &[]);
		assert_eq!(yolo.decide("bash", false), Decision::Auto);
	}

	#[test]
	fn child_approvals_strip_the_ask_channel() {
		let mut approvals = approvals(ApprovalMode::AlwaysAsk, &[]);
		assert!(approvals.ask_channel().is_none());
		approvals.set_ask(Some(crate::ask::terminal_channel(None)));
		assert!(approvals.ask_channel().is_some());
		assert!(approvals.without_ask().ask_channel().is_none());
		assert_eq!(approvals.without_ask().mode, approvals.mode);
	}

	#[test]
	fn confirm_reads_yes_from_stdin() {
		let approvals = approvals(ApprovalMode::AlwaysAsk, &[]);
		let mut input = Cursor::new(b"y\n");
		let mut prompt = Vec::new();
		assert!(
			approvals
				.confirm("bash", false, Some("rm -rf build"), &mut input, &mut prompt)
				.expect("confirm")
		);
		assert!(
			String::from_utf8(prompt)
				.expect("utf8")
				.starts_with("Approve bash")
		);

		let mut input = Cursor::new(b"\n");
		let mut prompt = Vec::new();
		assert!(
			!approvals
				.confirm("bash", false, None, &mut input, &mut prompt)
				.expect("confirm")
		);
	}

	#[test]
	fn allowed_tool_skips_the_prompt() {
		let approvals = approvals(ApprovalMode::AlwaysAsk, &["read"]);
		let mut input = Cursor::new(Vec::new());
		let mut prompt = Vec::new();
		assert!(
			approvals
				.confirm("read", true, None, &mut input, &mut prompt)
				.expect("confirm")
		);
	}

	#[test]
	fn arg_sensitive_classification_and_decide_tier() {
		use serde_json::json;

		// Local vs SSH paths
		assert_eq!(classify_tool("todo", &json!({})), ToolTier::Read);
		assert_eq!(classify_tool("glob", &json!({})), ToolTier::Read);
		assert_eq!(classify_tool("ask", &json!({})), ToolTier::Read);
		assert_eq!(
			classify_tool("read", &json!({"path": "/local/file.txt"})),
			ToolTier::Read
		);
		assert_eq!(
			classify_tool("read", &json!({"path": "ssh://host/file.txt"})),
			ToolTier::Exec
		);
		assert_eq!(
			classify_tool("grep", &json!({"paths": ["local", "ssh://remote"]})),
			ToolTier::Exec
		);
		assert_eq!(
			classify_tool("write", &json!({"path": "file.txt"})),
			ToolTier::Write
		);
		assert_eq!(
			classify_tool("write", &json!({"path": "ssh://box/f.txt"})),
			ToolTier::Exec
		);
		// Staging an AST preview is a write; only the later `write xd://resolve`
		// is read tier.
		assert_eq!(
			classify_tool("ast_edit", &json!({"ops": [], "paths": ["src"]})),
			ToolTier::Write
		);
		assert_eq!(classify_tool("bash", &json!({"command": "ls"})), ToolTier::Exec);
		assert_eq!(classify_tool("mcp__server_tool", &json!({})), ToolTier::Exec);

		// LSP is per action: reads stay auto in the read tier, rename/format/raw
		// request need explicit approval.
		for action in [
			"diagnostics",
			"definition",
			"type_definition",
			"implementation",
			"references",
			"hover",
			"symbols",
			"status",
			"capabilities",
		] {
			assert_eq!(
				classify_tool("lsp", &json!({"action": action})),
				ToolTier::Read,
				"lsp action {action}"
			);
		}
		for action in ["rename", "rename_file", "format", "reload", "request", "code_actions"] {
			assert_eq!(
				classify_tool("lsp", &json!({"action": action})),
				ToolTier::Write,
				"lsp action {action}"
			);
		}
		// Debug mutates the debuggee or session: exec tier except pure reads.
		for action in ["output", "threads", "stack_trace", "scopes", "variables", "sessions"] {
			assert_eq!(
				classify_tool("debug", &json!({"action": action})),
				ToolTier::Read,
				"debug action {action}"
			);
		}
		for action in ["launch", "attach", "continue", "evaluate", "set_breakpoint", "terminate"] {
			assert_eq!(
				classify_tool("debug", &json!({"action": action})),
				ToolTier::Exec,
				"debug action {action}"
			);
		}

		assert_eq!(
			extract_tool_detail("lsp", &json!({"action": "rename", "file": "src/lib.rs"})),
			Some("action: rename, file: src/lib.rs".to_string())
		);
		assert_eq!(
			extract_tool_detail("debug", &json!({"action": "launch", "program": "./app"})),
			Some("action: launch, program: ./app".to_string())
		);

		// Tier decisions
		let write_mode = approvals(ApprovalMode::Write, &[]);
		assert_eq!(write_mode.decide_tier("read", ToolTier::Read), Decision::Auto);
		assert_eq!(write_mode.decide_tier("write", ToolTier::Write), Decision::Ask);
		assert_eq!(write_mode.decide_tier("bash", ToolTier::Exec), Decision::Ask);
		assert_eq!(write_mode.decide_tier("read", ToolTier::Exec), Decision::Ask);

		let allow_bash = approvals(ApprovalMode::AlwaysAsk, &["bash"]);
		assert_eq!(allow_bash.decide_tier("bash", ToolTier::Exec), Decision::Auto);
		assert_eq!(allow_bash.decide_tier("write", ToolTier::Write), Decision::Ask);
	}

	#[test]
	fn test_allow_mcp_tool_origin_matches_exact_colon_syntax() {
		let mut app = approvals(ApprovalMode::AlwaysAsk, &["mcp:my_server:my_tool"]);
		// Server-level allow does NOT auto-authorize all tools
		let mut server_level = approvals(ApprovalMode::AlwaysAsk, &["mcp:my_server"]);
		server_level.allow_mcp_tool_origin("my_server", "my_tool", "mcp__my_server_my_tool");
		assert_eq!(server_level.decide("mcp__my_server_my_tool", false), Decision::Ask);

		// Other tool on same server is NOT authorized
		app.allow_mcp_tool_origin("my_server", "other_tool", "mcp__my_server_other_tool");
		assert_eq!(app.decide("mcp__my_server_other_tool", false), Decision::Ask);

		// Same tool on other server is NOT authorized
		app.allow_mcp_tool_origin("other_server", "my_tool", "mcp__other_server_my_tool");
		assert_eq!(app.decide("mcp__other_server_my_tool", false), Decision::Ask);

		// Exact origin match authorizes execution
		app.allow_mcp_tool_origin("my_server", "my_tool", "mcp__my_server_my_tool");
		assert_eq!(app.decide("mcp__my_server_my_tool", false), Decision::Auto);
	}

	#[tokio::test]
	async fn bash_policy_forbidden_beats_yolo_and_allow_bash() {
		use pi_agent::ToolApprover;
		use serde_json::json;

		for app in [
			approvals(ApprovalMode::Yolo, &["bash"]),
			approvals(ApprovalMode::AlwaysAsk, &["bash"]),
		] {
			let error = app
				.approve("bash", &json!({"command": "rm -rf /"}))
				.await
				.expect_err("hard deny");
			assert!(error.contains("root"), "unexpected reason: {error}");
		}
	}

	#[tokio::test]
	async fn bash_policy_prompt_overrides_allow_bash_and_yolo() {
		use pi_agent::ToolApprover;
		use serde_json::json;

		let closed: Box<dyn std::io::BufRead + Send> =
			Box::new(Cursor::new(Vec::<u8>::new()));
		let input: ApprovalInput =
			std::sync::Arc::new(std::sync::Mutex::new(closed));
		let approvals = approvals(ApprovalMode::Yolo, &["bash"]).with_input(Some(input));
		let error = approvals
			.approve("bash", &json!({"command": "git push --force origin main"}))
			.await
			.expect_err("policy must force a prompt");
		assert!(
			error.contains("stdin closed") || error.contains("denied"),
			"unexpected reason: {error}"
		);
	}

	#[tokio::test]
	async fn bash_allow_rule_auto_approves_only_the_whole_line() {
		use pi_agent::ToolApprover;
		use pi_agent::shell_policy::{BashApproval, BashApprovalRule, BashPolicy};
		use serde_json::json;

		let policy = BashPolicy::new(vec![BashApprovalRule::new(
			"git status*",
			BashApproval::Allow,
		)]);
		let approvals = approvals(ApprovalMode::AlwaysAsk, &[]).with_bash_policy(policy);
		approvals
			.approve("bash", &json!({"command": "git status --short"}))
			.await
			.expect("allow rule matches the whole line");
	}

	#[cfg(unix)]
	fn permission_hook(script: &str) -> std::sync::Arc<pi_agent::hooks::HookEngine> {
		use pi_agent::hooks::{HookConfig, HookEngine, HookEvent, HookInvocation, HookRule};

		std::sync::Arc::new(HookEngine::new(
			HookConfig::new(vec![HookRule {
				event: HookEvent::PermissionRequest,
				matcher: None,
				invocation: HookInvocation::Shell(script.to_string()),
				timeout_ms: 2_000,
				fail_closed: false,
			}]),
			"approval-test",
			std::env::temp_dir(),
		))
	}

	#[cfg(unix)]
	#[tokio::test]
	async fn permission_hook_allow_answers_the_prompt() {
		use pi_agent::ToolApprover;
		use serde_json::json;

		let approvals = approvals(ApprovalMode::AlwaysAsk, &[]).with_hooks(Some(permission_hook(
			r#"printf '%s' '{"hookSpecificOutput":{"permissionDecision":"allow"}}'"#,
		)));
		approvals
			.approve("write", &json!({"path": "file.txt"}))
			.await
			.expect("hook allow answers Ask");
	}

	#[cfg(unix)]
	#[tokio::test]
	async fn permission_hook_deny_blocks_the_call() {
		use pi_agent::ToolApprover;
		use serde_json::json;

		let approvals = approvals(ApprovalMode::AlwaysAsk, &[])
			.with_hooks(Some(permission_hook("printf 'denied by hook' >&2; exit 2")));
		let error = approvals
			.approve("write", &json!({"path": "file.txt"}))
			.await
			.expect_err("hook denies");
		assert_eq!(error, "denied by hook");
	}

	#[cfg(unix)]
	#[tokio::test]
	async fn permission_hook_cannot_bypass_forbidden_bash() {
		use pi_agent::ToolApprover;
		use serde_json::json;

		let approvals = approvals(ApprovalMode::AlwaysAsk, &[]).with_hooks(Some(permission_hook(
			r#"printf '%s' '{"hookSpecificOutput":{"permissionDecision":"allow"}}'"#,
		)));
		let error = approvals
			.approve("bash", &json!({"command": "shutdown -h now"}))
			.await
			.expect_err("forbidden command must be denied before hooks");
		assert!(
			error.contains("power control") || error.contains("forbidden"),
			"unexpected reason: {error}"
		);
	}

	#[cfg(unix)]
	#[tokio::test]
	async fn permission_hook_cannot_bypass_policy_prompt() {
		use pi_agent::ToolApprover;
		use pi_agent::shell_policy::{BashApproval, BashApprovalRule, BashPolicy};
		use serde_json::json;

		let policy = BashPolicy::new(vec![BashApprovalRule::new(
			"git push*",
			BashApproval::Prompt,
		)]);
		let approvals = approvals(ApprovalMode::Yolo, &[])
			.with_bash_policy(policy)
			.with_hooks(Some(permission_hook(
				r#"printf '%s' '{"hookSpecificOutput":{"permissionDecision":"allow"}}'"#,
			)))
			.with_input(Some(std::sync::Arc::new(std::sync::Mutex::new(Box::new(
				Cursor::new(b"n\n".to_vec()),
			)))));
		approvals
			.approve("bash", &json!({"command": "git push origin main"}))
			.await
			.expect_err("policy prompt survives a hook allow");
	}

	#[cfg(unix)]
	#[tokio::test]
	async fn pre_tool_hint_forces_prompt_over_yolo_and_hook_allow() {
		use pi_agent::{ApprovalHint, ToolApprover};
		use serde_json::json;

		let approvals = approvals(ApprovalMode::Yolo, &[])
			.with_hooks(Some(permission_hook(
				r#"printf '%s' '{"hookSpecificOutput":{"permissionDecision":"allow"}}'"#,
			)))
			.with_input(Some(std::sync::Arc::new(std::sync::Mutex::new(Box::new(
				Cursor::new(b"n\n".to_vec()),
			)))));
		approvals
			.approve_with_hint(
				"write",
				&json!({"path": "file.txt"}),
				ApprovalHint { force_ask: true },
			)
			.await
			.expect_err("forced ask survives yolo and hook allow");
	}

	#[tokio::test]
	async fn legacy_alternate_cannot_auto_approve_a_policy_prompt() {
		use pi_agent::ToolApprover;
		use pi_agent::shell_policy::{BashApproval, BashApprovalRule, BashPolicy};
		use serde_json::json;

		let policy = BashPolicy::new(vec![BashApprovalRule::new(
			"git push*",
			BashApproval::Prompt,
		)]);
		let mut app = approvals(ApprovalMode::Yolo, &[]).with_bash_policy(policy);
		app.set_approver(Some(std::sync::Arc::new(RecordingAlternate {
			supported: false,
			seen: std::sync::Mutex::new(Vec::new()),
		})));
		let error = app
			.approve("bash", &json!({"command": "git push origin main"}))
			.await
			.expect_err("legacy approver must not swallow a policy prompt");
		assert!(error.contains("does not honor forced prompts"), "{error}");
	}

	#[tokio::test]
	async fn hub_start_enforces_forbidden_bash_policy() {
		use pi_agent::ToolApprover;
		use serde_json::json;

		let approvals = approvals(ApprovalMode::Yolo, &["hub"]);
		let error = approvals
			.approve(
				"hub",
				&json!({
					"op": "start",
					"name": "power",
					"application": "shutdown",
					"args": ["-h", "now"],
				}),
			)
			.await
			.expect_err("hub start must not bypass a hard deny");
		assert!(error.contains("power control"), "unexpected reason: {error}");
	}

	#[tokio::test]
	async fn hub_start_forced_prompt_and_non_start_ops_stay_unaffected() {
		use pi_agent::ToolApprover;
		use pi_agent::shell_policy::{BashApproval, BashApprovalRule, BashPolicy};
		use serde_json::json;

		let policy = BashPolicy::new(vec![BashApprovalRule::new(
			"git push*",
			BashApproval::Prompt,
		)]);
		let closed: Box<dyn std::io::BufRead + Send> =
			Box::new(Cursor::new(Vec::<u8>::new()));
		let input: ApprovalInput =
			std::sync::Arc::new(std::sync::Mutex::new(closed));
		let approvals = approvals(ApprovalMode::Yolo, &["hub"])
			.with_bash_policy(policy)
			.with_input(Some(input));
		let error = approvals
			.approve(
				"hub",
				&json!({
					"op": "start",
					"name": "push",
					"application": "git",
					"args": ["push", "origin", "main"],
				}),
			)
			.await
			.expect_err("policy prompt must survive yolo");
		assert!(
			error.contains("stdin closed") || error.contains("denied"),
			"unexpected reason: {error}"
		);

		// A start without `application` never reaches the policy.
		approvals
			.approve("hub", &json!({"op": "start", "name": "empty"}))
			.await
			.expect("missing application is allowed");

		// Non-start ops do not build a command line.
		let prompt_everything = BashPolicy::new(vec![BashApprovalRule::new(
			"*",
			BashApproval::Prompt,
		)]);
		let approvals = approvals(ApprovalMode::Yolo, &["hub"])
			.with_bash_policy(prompt_everything);
		approvals
			.approve("hub", &json!({"op": "ps"}))
			.await
			.expect("ps is unaffected");
		approvals
			.approve("hub", &json!({"op": "list"}))
			.await
			.expect("list is unaffected");
	}

	#[tokio::test]
	async fn hub_start_ignores_bash_allow_rules() {
		use pi_agent::ToolApprover;
		use pi_agent::shell_policy::{BashApproval, BashApprovalRule, BashPolicy};
		use serde_json::json;

		let policy = BashPolicy::new(vec![BashApprovalRule::new(
			"git status*",
			BashApproval::Allow,
		)]);
		let closed: Box<dyn std::io::BufRead + Send> =
			Box::new(Cursor::new(Vec::<u8>::new()));
		let input: ApprovalInput =
			std::sync::Arc::new(std::sync::Mutex::new(closed));
		let approvals = approvals(ApprovalMode::AlwaysAsk, &[])
			.with_bash_policy(policy)
			.with_input(Some(input));
		let error = approvals
			.approve(
				"hub",
				&json!({
					"op": "start",
					"name": "status",
					"application": "git",
					"args": ["status"],
				}),
			)
			.await
			.expect_err("a bash allow rule must not auto-approve a hub start");
		assert!(
			error.contains("stdin closed") || error.contains("denied"),
			"unexpected reason: {error}"
		);
	}

	#[tokio::test]
	async fn hub_start_skips_hub_directed_interceptors_but_keeps_other_checks() {
		use pi_agent::ToolApprover;
		use pi_agent::shell_policy::{BashInterceptor, BashPolicy, default_interceptor_rules};
		use serde_json::json;

		let interceptor = BashInterceptor {
			enabled: Some(true),
			rules:   default_interceptor_rules(),
		};
		let approvals = approvals(ApprovalMode::Yolo, &["hub"])
			.with_bash_policy(BashPolicy::new(Vec::new()).with_interceptor(interceptor));
		for args in [
			json!({"op": "start", "name": "vite", "application": "vite"}),
			json!({
				"op": "start",
				"name": "dev",
				"application": "npm",
				"args": ["run", "dev"],
			}),
		] {
			approvals
				.approve("hub", &args)
				.await
				.expect("hub-redirecting interceptor must not block hub start");
		}

		// Non-hub interceptors still apply.
		let error = approvals
			.approve(
				"hub",
				&json!({"op": "start", "name": "read", "application": "cat", "args": ["file"]}),
			)
			.await
			.expect_err("the read interceptor still applies");
		assert!(error.contains("cat/head/tail"), "unexpected reason: {error}");

		// Built-in hard denies still apply.
		let error = approvals
			.approve(
				"hub",
				&json!({"op": "start", "name": "power", "application": "shutdown"}),
			)
			.await
			.expect_err("hard deny still applies");
		assert!(error.contains("power control"), "unexpected reason: {error}");
	}

	#[tokio::test]
	async fn hint_aware_alternate_receives_force_ask() {
		use pi_agent::ApprovalHint;
		use pi_agent::ToolApprover;
		use serde_json::json;

		let recorder = std::sync::Arc::new(RecordingAlternate {
			supported: true,
			seen: std::sync::Mutex::new(Vec::new()),
		});
		let mut app = approvals(ApprovalMode::Yolo, &[]);
		app.set_approver(Some(recorder.clone()));
		app.approve_with_hint(
			"write",
			&json!({"path": "file.txt"}),
			ApprovalHint { force_ask: true },
		)
		.await
		.expect("hint-aware alternate is asked");
		assert_eq!(
			*recorder.seen.lock().expect("hint lock"),
			[ApprovalHint { force_ask: true }]
		);
	}
}
