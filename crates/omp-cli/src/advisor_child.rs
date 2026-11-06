//! Per-child advisor opt-in (frontmatter `advisor` / `task.agentAdvisor`).
//!
//! Ports `resolveAgentAdvisorSelection` from `config/model-resolver.ts`.
//! Subagents run unadvised by default; only an agent whose settings override is
//! `"on"` (or a concrete pattern) or whose frontmatter carries `advisor` gets a
//! runtime. A returned pattern lands on the spawned session's
//! `modelRoles.advisor`, so aliases and `:level` suffixes resolve there.
//!
//! This module is pure; the task executor owns mounting it (history baseline).

use pi_agent::tools::task_catalog::HandoffTarget;

use crate::config::Config;

/// Effective advisor for one spawned agent. Absent `model` resolves through the
/// `advisor` role (which falls back to the `slow` chain, never the primary).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentAdvisorSelection {
	pub model: Option<String>,
}

/// `settings_override` is `task.agentAdvisor[agent_name]`; `frontmatter` is the
/// agent definition's parsed `advisor` field. `"off"` always wins; `"on"`
/// enables with the frontmatter pattern (or the role); any other non-empty
/// override is a custom model pattern.
#[must_use]
pub fn resolve_agent_advisor(
	settings_override: Option<&str>,
	frontmatter: Option<&HandoffTarget>,
) -> Option<AgentAdvisorSelection> {
	let agent_pattern = match frontmatter {
		Some(HandoffTarget::Pattern(pattern)) if !pattern.trim().is_empty() => {
			Some(pattern.trim().to_string())
		},
		_ => None,
	};
	if let Some(override_value) = settings_override.filter(|value| !value.trim().is_empty()) {
		let lowered = override_value.trim().to_ascii_lowercase();
		if lowered == "off" || lowered == "false" {
			return None;
		}
		if lowered == "on" || lowered == "true" {
			return Some(AgentAdvisorSelection { model: agent_pattern });
		}
		return Some(AgentAdvisorSelection { model: Some(override_value.trim().to_string()) });
	}
	match frontmatter {
		Some(HandoffTarget::Default) => Some(AgentAdvisorSelection::default()),
		_ => agent_pattern.map(|model| AgentAdvisorSelection { model: Some(model) }),
	}
}

/// Apply a resolved opt-in to a child config: enable the advisor and pin the
/// explicit pattern on `modelRoles.advisor`.
pub fn apply_to_config(config: &mut Config, selection: &AgentAdvisorSelection) {
	config.advisor.enabled = Some(true);
	if let Some(model) = &selection.model {
		config.model_roles.insert("advisor".to_string(), model.clone());
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn off_override_wins_over_frontmatter() {
		assert_eq!(
			resolve_agent_advisor(Some("off"), Some(&HandoffTarget::Default)),
			None
		);
		assert_eq!(
			resolve_agent_advisor(Some("OFF"), Some(&HandoffTarget::Pattern("x/y".to_string()))),
			None
		);
	}

	#[test]
	fn on_override_uses_frontmatter_pattern_or_role() {
		assert_eq!(
			resolve_agent_advisor(Some("on"), Some(&HandoffTarget::Pattern("mock/x:high".to_string()))),
			Some(AgentAdvisorSelection { model: Some("mock/x:high".to_string()) })
		);
		assert_eq!(
			resolve_agent_advisor(Some("true"), None),
			Some(AgentAdvisorSelection::default())
		);
	}

	#[test]
	fn custom_override_is_a_model_pattern() {
		assert_eq!(
			resolve_agent_advisor(Some("mock/reviewer"), None),
			Some(AgentAdvisorSelection { model: Some("mock/reviewer".to_string()) })
		);
	}

	#[test]
	fn frontmatter_enables_without_override() {
		assert_eq!(
			resolve_agent_advisor(None, Some(&HandoffTarget::Default)),
			Some(AgentAdvisorSelection::default())
		);
		assert_eq!(
			resolve_agent_advisor(None, Some(&HandoffTarget::Pattern("mock/x".to_string()))),
			Some(AgentAdvisorSelection { model: Some("mock/x".to_string()) })
		);
		assert_eq!(resolve_agent_advisor(None, None), None);
	}

	#[test]
	fn apply_enables_and_pins_the_role_only_when_a_pattern_exists() {
		let mut config = Config::default();
		apply_to_config(&mut config, &AgentAdvisorSelection::default());
		assert_eq!(config.advisor.enabled, Some(true));
		assert_eq!(config.model_roles.get("advisor"), None);
		let mut config = Config::default();
		apply_to_config(
			&mut config,
			&AgentAdvisorSelection { model: Some("mock/x".to_string()) },
		);
		assert_eq!(config.model_roles.get("advisor").map(String::as_str), Some("mock/x"));
	}
}
