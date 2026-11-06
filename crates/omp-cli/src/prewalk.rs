//! One-way prewalk: deep planning on the primary model, then a switch to a
//! fast model at the first implementation action.
//!
//! The state machine is host-owned (mirrors `session/prewalk.ts`): pi-agent
//! only carries the typed turn-boundary reply (guidance, scrub, provider). The
//! host journals the ephemeral model change and the persisted guidance, builds
//! the replacement provider with a reset provider session, and updates its own
//! model fields for later prompts.

use serde_json::Value;

use crate::{
	args::Cli,
	config::Config,
	runtime::{self, CliThinking, ModelSwitch},
};

/// TS `PREWALK_PLAN_MESSAGE_TYPE`: consumed within the live run, never
/// persisted, scrubbed before the switch.
pub const PREWALK_PLAN_TYPE: &str = "prewalk-plan";
pub const PREWALK_CONTINUE_TYPE: &str = "prewalk-continue";
pub const PREWALK_CHECKLIST_TYPE: &str = "prewalk-checklist";
/// TS `DEFAULT_PREWALK_TARGET`.
pub const DEFAULT_TARGET: &str = "@smol";

const PLAN_PROMPT: &str = include_str!(
	"../../../packages/coding-agent/src/prompts/system/prewalk-plan.md"
);
const CONTINUE_PROMPT: &str = include_str!(
	"../../../packages/coding-agent/src/prompts/system/prewalk-continue.md"
);
const CHECKLIST_PROMPT: &str = include_str!(
	"../../../packages/coding-agent/src/prompts/system/prewalk-checklist.md"
);

/// One resolved prewalk arm. The target carries the wire identity; credentials
/// and endpoint are re-derived when the switch actually fires.
#[derive(Debug, Clone)]
pub struct PrewalkArm {
	pub target: ModelSwitch,
	/// Selector the target's `:suffix` spelled, if any.
	pub thinking: Option<CliThinking>,
}

/// Startup policy result: an armed target plus warnings that must not abort the
/// process (TS leaves prewalk unarmed when its optional target is unusable).
#[derive(Debug, Default)]
pub struct PrewalkPolicy {
	pub arm: Option<PrewalkArm>,
	pub warnings: Vec<String>,
}

/// `--no-prewalk`/`--prewalk`/`--prewalk-into` plus `prewalk.enabled`.
///
/// Exact TS `main.ts` policy: the conflict is a hard error, an explicit flag
/// arms even on a resumed session, and the settings default applies to fresh
/// sessions only. A target that cannot be resolved or has no configured auth
/// warns and leaves prewalk unarmed instead of blocking startup.
pub fn resolve_policy(
	cli: &Cli,
	config: &Config,
	catalog: &crate::catalog::Catalog,
	restoring_session: bool,
	explicit_api_key: Option<&str>,
) -> Result<PrewalkPolicy, String> {
	if cli.no_prewalk && (cli.prewalk || cli.prewalk_into.is_some()) {
		return Err("--no-prewalk cannot be combined with --prewalk or --prewalk-into".to_string());
	}
	let explicit = cli.prewalk || cli.prewalk_into.is_some();
	let enabled = if cli.no_prewalk {
		false
	} else if explicit {
		true
	} else {
		!restoring_session && config.prewalk.enabled()
	};
	let mut policy = PrewalkPolicy::default();
	if !enabled {
		return Ok(policy);
	}
	let requested = cli.prewalk_into.as_deref().unwrap_or(DEFAULT_TARGET);
	let target = match runtime::resolve_model_switch(catalog, config, requested, None) {
		Ok(target) => target,
		Err(error) => {
			policy
				.warnings
				.push(format!("Warning: prewalk disabled — {error}"));
			return Ok(policy);
		},
	};
	// `--api-key` belongs to the root route; only reuse it when the target is
	// that same provider. Every other provider resolves its own configured key.
	let api_key_override = explicit_api_key.filter(|_| {
		cli.provider
			.as_deref()
			.is_some_and(|provider| provider == target.provider.as_str())
	});
	if !runtime::provider_has_configured_auth(catalog, config, &target.provider, api_key_override) {
		policy.warnings.push(format!(
			"Warning: prewalk disabled — no API key for {}/{}",
			target.provider, target.model
		));
		return Ok(policy);
	}
	let thinking = target.thinking;
	policy.arm = Some(PrewalkArm { target, thinking });
	Ok(policy)
}

/// Inputs a settled turn boundary offers the state machine.
#[derive(Debug)]
pub struct TurnBoundary<'a> {
	pub has_tool_results: bool,
	pub todo_succeeded: bool,
	/// First action tool of this batch that actually mutates (success flag is
	/// not part of the TS action check).
	pub action: Option<&'a str>,
	/// Whether the `todo` tool is offered to this run at all (TS gate).
	pub todo_available: bool,
	/// Target already equals the active model + thinking selector.
	pub noop: bool,
}

/// Host instructions produced at one boundary. The host journals `journal`,
/// builds the provider for `switch`, forwards `guidance`/`scrub_plan` in the
/// turn ack, and prints `notices`.
#[derive(Debug, Default)]
pub struct BoundaryAction {
	pub guidance: Vec<pi_agent::Message>,
	/// `(customType, content)` pairs that must be persisted as
	/// `custom_message` entries. The plan nudge is deliberately absent.
	pub journal: Vec<(&'static str, &'static str)>,
	pub scrub_plan: bool,
	pub switch: Option<ModelSwitch>,
	pub switch_thinking: Option<CliThinking>,
	pub notices: Vec<String>,
	pub continue_turn: bool,
}

#[derive(Debug, Default)]
pub struct PrewalkState {
	arm: Option<PrewalkArm>,
	plan_injected: bool,
	continue_pending: bool,
	todo_seen: bool,
}

impl PrewalkState {
	#[must_use]
	pub fn new(arm: Option<PrewalkArm>) -> Self {
		Self {
			arm,
			plan_injected: false,
			continue_pending: false,
			todo_seen: false,
		}
	}

	#[must_use]
	pub fn armed(&self) -> bool {
		self.arm.is_some()
	}

	#[must_use]
	pub fn target(&self) -> Option<&PrewalkArm> {
		self.arm.as_ref()
	}

	fn clear(&mut self) {
		self.arm = None;
		self.plan_injected = false;
		self.continue_pending = false;
		self.todo_seen = false;
	}

	/// Advance at a completed assistant-turn boundary (TS
	/// `PrewalkCoordinator.advanceAtTurnEnd`).
	pub fn advance(&mut self, boundary: &TurnBoundary<'_>) -> BoundaryAction {
		let mut action = BoundaryAction::default();
		let Some(arm) = self.arm.clone() else {
			return action;
		};
		if boundary.noop {
			self.clear();
			action.scrub_plan = true;
			action.notices.push(format!(
				"Prewalk: target {}/{} already matches the active model and thinking level; nothing to switch.",
				arm.target.provider, arm.target.model
			));
			return action;
		}
		if boundary.todo_succeeded {
			self.todo_seen = true;
		}
		if self.plan_injected && boundary.has_tool_results {
			self.continue_pending = true;
		} else if self.continue_pending {
			self.continue_pending = false;
			action
				.guidance
				.push(pi_agent::Message::guidance(PREWALK_CONTINUE_TYPE, CONTINUE_PROMPT));
			action.journal.push((PREWALK_CONTINUE_TYPE, CONTINUE_PROMPT));
			action.continue_turn = true;
		}

		let gate_open = self.todo_seen || !boundary.todo_available;
		let implementation = gate_open.then_some(boundary.action).flatten();
		let Some(tool) = implementation else {
			if !self.plan_injected {
				self.plan_injected = true;
				self.continue_pending = true;
				action
					.guidance
					.push(pi_agent::Message::guidance(PREWALK_PLAN_TYPE, PLAN_PROMPT));
				// The plan nudge is a one-run instruction: never journaled.
				action.continue_turn = true;
				action.notices.push("Prewalk: injected deep-plan nudge.".to_string());
			}
			return action;
		};

		self.clear();
		action.scrub_plan = true;
		action.switch = Some(arm.target.clone());
		action.switch_thinking = arm.thinking;
		action.guidance.push(pi_agent::Message::guidance(
			PREWALK_CHECKLIST_TYPE,
			CHECKLIST_PROMPT,
		));
		action
			.journal
			.push((PREWALK_CHECKLIST_TYPE, CHECKLIST_PROMPT));
		action.continue_turn = true;
		action.notices.push(format!(
			"Prewalk: switched to {}/{} after first {tool} call.",
			arm.target.provider, arm.target.model
		));
		action
	}
}

/// A switch the host actually applied during this turn. The host copies it into
/// its session fields after the run so later prompts stay on the fast model;
/// the journal records it as the ephemeral `fallback` role.
#[derive(Debug, Clone)]
pub struct AppliedSwitch {
	pub target: ModelSwitch,
	/// `None` preserves the previous selection; `Some(None)` clears it (the
	/// target default resolved to no concrete level); `Some(Some(level))` sets
	/// the explicit suffix or clamped target default.
	pub thinking: Option<Option<CliThinking>>,
	/// Credential the new provider was built with.
	pub api_key: Option<String>,
}

/// Whether `todo` is offered to this run. The gate must follow the actual tool
/// selection, not the tool's mere existence: `--no-tools` or a restricted
/// `--tools` list without `todo` opens the gate immediately.
#[must_use]
pub fn todo_available(tools_enabled: bool, selected: Option<&[String]>) -> bool {
	tools_enabled && selected.is_none_or(|tools| tools.iter().any(|tool| tool == "todo"))
}

/// Whether a completed `edit`/`write` result is the implementation action that
/// arms the hand-off (TS `isPrewalkImplementationAction`).
///
/// A direct edit/write always counts, regardless of its success flag (the TS
/// check does not consult `isError`). A `write` that dispatched an `xd://`
/// device counts only when the staged action declared a mutating capability:
/// `xdev.tier` is exactly `write` or `exec`. A missing tier (help lookup,
/// unresolved approval, read-only device) declines. The source tool name is
/// never consulted as a proxy.
#[must_use]
pub fn is_implementation_action(tool: &str, details: Option<&Value>) -> bool {
	match tool {
		"edit" => true,
		"write" => match details.and_then(|details| details.get("xdev")) {
			None => true,
			Some(xdev) => matches!(
				xdev.get("tier").and_then(Value::as_str),
				Some("write" | "exec")
			),
		},
		_ => false,
	}
}

/// Whether the target already equals the active route (TS
/// `prewalkWouldBeNoop`). Thinking is compared through
/// [`runtime::resolve_thinking_effort`]: `auto` is distinct from concrete
/// selectors, and both sides are clamped against the target model's declared
/// efforts before equality.
#[must_use]
pub fn would_be_noop(
	catalog: &crate::catalog::Catalog,
	target: &ModelSwitch,
	target_thinking: Option<CliThinking>,
	current_provider: Option<&str>,
	current_model: Option<&str>,
	current_thinking: Option<CliThinking>,
) -> bool {
	let Some(current_model) = current_model else {
		return false;
	};
	if current_provider != Some(target.provider.as_str()) {
		return false;
	}
	let (base, _) = runtime::split_effort(current_model);
	let prefix = format!("{}/", target.provider);
	let base = base.strip_prefix(&prefix).unwrap_or(base);
	if base != target.model.as_str() {
		return false;
	}
	// TS `concreteThinkingLevel`: auto resolves to `undefined` for comparison;
	// auto and a concrete selector are never no-ops of each other.
	let Some(target_level) = target_thinking.or(target.thinking) else {
		return true;
	};
	let current_level = current_thinking.or_else(|| suffix_selector(current_model));
	let target_auto = target_level == CliThinking::Auto;
	let current_auto = current_level == Some(CliThinking::Auto);
	if target_auto != current_auto {
		return false;
	}
	let target_concrete = (!target_auto).then_some(target_level);
	let current_concrete = if current_auto { None } else { current_level };
	runtime::resolve_thinking_effort(catalog, &target.provider, &target.model, target_concrete)
		== runtime::resolve_thinking_effort(
			catalog,
			&target.provider,
			&target.model,
			current_concrete,
		)
}

fn suffix_selector(model: &str) -> Option<CliThinking> {
	let (_, suffix) = model.rsplit_once(':')?;
	runtime::parse_cli_thinking(suffix)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn target() -> ModelSwitch {
		ModelSwitch {
			provider: "fast".to_string(),
			model: "small".to_string(),
			effort: None,
			thinking: None,
		}
	}

	fn boundary(action: Option<&str>, todo_succeeded: bool, has_tools: bool) -> TurnBoundary<'_> {
		TurnBoundary {
			has_tool_results: has_tools,
			todo_succeeded,
			action,
			todo_available: true,
			noop: false,
		}
	}

	#[test]
	fn plan_nudge_then_switch_requires_todo_and_an_action() {
		let mut state = PrewalkState::new(Some(PrewalkArm {
			target: target(),
			thinking: None,
		}));
		// Text-only turn: plan nudge, never journaled, loop must continue.
		let first = state.advance(&boundary(None, false, false));
		assert!(first.continue_turn);
		assert_eq!(first.guidance.len(), 1);
		assert_eq!(
			first.guidance[0].custom_type.as_deref(),
			Some(PREWALK_PLAN_TYPE)
		);
		assert!(first.journal.is_empty(), "plan nudge must never be persisted");

		// An edit before any successful todo does not switch (gate closed).
		let gated = state.advance(&boundary(Some("edit"), false, true));
		assert!(gated.switch.is_none());
		assert!(!gated.continue_turn);

		// Successful todo opens the gate; the next edit switches once.
		let todo = state.advance(&boundary(None, true, true));
		assert!(todo.switch.is_none());
		let switched = state.advance(&boundary(Some("edit"), false, true));
		let switch = switched.switch.expect("switch fired");
		assert_eq!(switch.provider, "fast");
		assert!(switched.scrub_plan, "consumed plan nudge is scrubbed");
		assert_eq!(
			switched.guidance[0].custom_type.as_deref(),
			Some(PREWALK_CHECKLIST_TYPE)
		);
		assert_eq!(switched.journal.len(), 1);
		assert_eq!(switched.journal[0].0, PREWALK_CHECKLIST_TYPE);
		assert!(!state.armed(), "prewalk is one-way");
	}

	#[test]
	fn noop_target_disarms_with_notice_and_no_guidance() {
		let mut state = PrewalkState::new(Some(PrewalkArm {
			target: target(),
			thinking: None,
		}));
		let mut ctx = boundary(None, false, false);
		ctx.noop = true;
		let action = state.advance(&ctx);
		assert!(action.guidance.is_empty());
		assert!(action.scrub_plan);
		assert!(action.notices[0].contains("nothing to switch"));
		assert!(!state.armed());
	}

	#[test]
	fn todo_absent_opens_the_gate_immediately() {
		let mut state = PrewalkState::new(Some(PrewalkArm {
			target: target(),
			thinking: None,
		}));
		let mut ctx = boundary(Some("write"), false, true);
		ctx.todo_available = false;
		let action = state.advance(&ctx);
		assert!(action.switch.is_some());
	}

	#[test]
	fn device_actions_switch_only_for_typed_mutating_tier() {
		// Tier is authoritative; the source tool name is deliberately misleading.
		let resolve_write = serde_json::json!({
			"xdev": { "tool": "resolve", "tier": "write", "inner": { "sourceToolName": "help" } }
		});
		let resolve_exec = serde_json::json!({
			"xdev": { "tool": "resolve", "tier": "exec", "inner": {} }
		});
		let resolve_read = serde_json::json!({
			"xdev": { "tool": "resolve", "tier": "read", "inner": { "sourceToolName": "ast_edit" } }
		});
		let resolve_unresolved = serde_json::json!({
			"xdev": { "tool": "resolve", "inner": { "sourceToolName": "ast_edit" } }
		});
		let reject = serde_json::json!({
			"xdev": { "tool": "reject", "inner": { "sourceToolName": "ast_edit" } }
		});
		assert!(is_implementation_action("write", Some(&resolve_write)));
		assert!(is_implementation_action("write", Some(&resolve_exec)));
		assert!(!is_implementation_action("write", Some(&resolve_read)));
		assert!(!is_implementation_action("write", Some(&resolve_unresolved)));
		assert!(!is_implementation_action("write", Some(&reject)));
		assert!(is_implementation_action("write", None));
		assert!(!is_implementation_action("read", Some(&resolve_write)));
		assert!(!is_implementation_action("lsp", None));
	}

	fn catalog_with_fast_small() -> crate::catalog::Catalog {
		let mut catalog = crate::catalog::Catalog::default();
		let mut models = std::collections::BTreeMap::new();
		models.insert(
			"small".to_string(),
			crate::catalog::ModelInfo {
				provider: "fast".to_string(),
				reasoning: true,
				efforts: Some(vec!["low".to_string(), "high".to_string()]),
				..crate::catalog::ModelInfo::default()
			},
		);
		models.insert(
			"plain".to_string(),
			crate::catalog::ModelInfo {
				provider: "fast".to_string(),
				..crate::catalog::ModelInfo::default()
			},
		);
		catalog.providers.insert("fast".to_string(), models);
		catalog
	}

	#[test]
	fn noop_compares_provider_model_and_clamped_thinking() {
		let catalog = catalog_with_fast_small();
		assert!(would_be_noop(
			&catalog,
			&target(),
			None,
			Some("fast"),
			Some("fast/small"),
			None
		));
		assert!(would_be_noop(
			&catalog,
			&target(),
			Some(CliThinking::Effort("high")),
			Some("fast"),
			Some("fast/small:high"),
			None
		));
		// Both selectors clamp to the target's highest declared effort.
		assert!(would_be_noop(
			&catalog,
			&target(),
			Some(CliThinking::Effort("max")),
			Some("fast"),
			Some("fast/small:xhigh"),
			None
		));
		assert!(!would_be_noop(
			&catalog,
			&target(),
			Some(CliThinking::Effort("low")),
			Some("fast"),
			Some("fast/small:high"),
			None
		));
		assert!(!would_be_noop(
			&catalog,
			&target(),
			Some(CliThinking::Auto),
			Some("fast"),
			Some("fast/small:high"),
			None
		));
		// A non-reasoning target resolves every concrete effort to none, so
		// different selectors collapse exactly like the TS resolver.
		let mut plain_target = target();
		plain_target.model = "plain".to_string();
		assert!(would_be_noop(
			&catalog,
			&plain_target,
			Some(CliThinking::Effort("high")),
			Some("fast"),
			Some("fast/plain:low"),
			None
		));
		assert!(!would_be_noop(
			&catalog,
			&target(),
			None,
			Some("slow"),
			Some("slow/small"),
			None
		));
	}

	#[test]
	fn policy_conflict_and_restore_rules() {
		let cli = |prewalk: bool, no_prewalk: bool, into: Option<&str>| {
			let mut cli = crate::args::parse_cli(vec!["omp".into()]);
			cli.prewalk = prewalk;
			cli.no_prewalk = no_prewalk;
			cli.prewalk_into = into.map(str::to_string);
			cli
		};
		let config = Config::default();
		let catalog = crate::catalog::Catalog::default();
		let conflict = resolve_policy(
			&cli(false, true, Some("@smol")),
			&config,
			&catalog,
			false,
			None,
		);
		assert_eq!(
			conflict.unwrap_err(),
			"--no-prewalk cannot be combined with --prewalk or --prewalk-into"
		);
		// Settings never arm a resumed session.
		let mut settings = Config::default();
		settings.prewalk.enabled = Some(true);
		let restored =
			resolve_policy(&cli(false, false, None), &settings, &catalog, true, None)
				.expect("policy");
		assert!(restored.arm.is_none());
		assert!(restored.warnings.is_empty());
	}
}
