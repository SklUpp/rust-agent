//! Auto-learn tools: `learn` (local memory backend) and `manage_skill`.
//!
//! Anchors: `packages/coding-agent/src/tools/learn.ts:1-140`,
//! `packages/coding-agent/src/tools/manage-skill.ts:1-120`,
//! `packages/coding-agent/src/autolearn/controller.ts:23-39,112-132` and the
//! prompt assets under `packages/coding-agent/src/prompts/`.
//!
//! Gating (TS `createIf`):
//! - `learn` requires `autolearn.enabled` **and** a live memory backend. This
//!   slice owns the `local` backend only; with `memory.backend=off` (or a
//!   remote/SQLite backend this crate does not implement yet) no tool is built,
//!   so nothing can write `learned.md`.
//! - `manage_skill` requires `autolearn.enabled` only (the skill side is
//!   backend-independent).
//!
//! Both are write-tier tools and are mounted for top-level sessions only; the
//! registry/mount lives outside this module.
//!
//! Partial outcomes are honest: if the lesson is stored and the optional
//! managed-skill write then fails, the tool reports the memory result plus the
//! skill failure. Nothing is rolled back, and no "Created" is claimed for a
//! skill that cannot surface because an authored skill shadows it.

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use pi_agent::{
	state::{AgentContext, ToolResult},
	tools::{Tool, ToolError},
};
use serde_json::{Value, json};

use crate::{
	dirs,
	local_memory::{self, SaveInput},
	managed_skills::{
		WriteAction, WriteManagedSkillInput, delete_managed_skill, sanitize_skill_name,
		write_managed_skill,
	},
	skills::{LoadOptions, SkillSnapshot},
};

/// Tool names as advertised to the model.
pub const LEARN_TOOL_NAME: &str = "learn";
pub const MANAGE_SKILL_TOOL_NAME: &str = "manage_skill";

const LEARN_TOOL_DESCRIPTION: &str =
	include_str!("../../../packages/coding-agent/src/prompts/tools/learn.md");
const MANAGE_SKILL_TOOL_DESCRIPTION: &str =
	include_str!("../../../packages/coding-agent/src/prompts/tools/manage-skill.md");
const AUTOLEARN_GUIDANCE: &str =
	include_str!("../../../packages/coding-agent/src/prompts/system/autolearn-guidance.md");
const AUTOLEARN_GUIDANCE_LEARN: &str =
	include_str!("../../../packages/coding-agent/src/prompts/system/autolearn-guidance-learn.md");
const AUTOLEARN_NUDGE_AUTOCONTINUE: &str =
	include_str!("../../../packages/coding-agent/src/prompts/system/autolearn-nudge-autocontinue.md");

/// `memory.backend` selector values (`settings-schema.ts:2942-2945`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryBackend {
	Off,
	Local,
	Hindsight,
	Mnemopi,
}

impl MemoryBackend {
	/// Parse the settings value; unknown values are not invented.
	#[must_use]
	pub fn parse(raw: &str) -> Option<Self> {
		match raw {
			"off" => Some(Self::Off),
			"local" => Some(Self::Local),
			"hindsight" => Some(Self::Hindsight),
			"mnemopi" => Some(Self::Mnemopi),
			_ => None,
		}
	}
}

/// Standing auto-learn system guidance, or `None` when `manage_skill` is absent.
///
/// Driven by tool presence rather than live settings, matching TS: the registry
/// is built once per top-level session, so keying this on `autolearn.enabled`
/// could point at tools the session never built.
#[must_use]
pub fn build_auto_learn_instructions(manage_skill: bool, learn: bool) -> Option<String> {
	if !manage_skill {
		return None;
	}
	let mut parts = vec![AUTOLEARN_GUIDANCE.trim().to_string()];
	if learn {
		parts.push(AUTOLEARN_GUIDANCE_LEARN.trim().to_string());
	}
	Some(parts.join("\n\n"))
}

/// Prompt for the optional auto-continue capture turn (`autolearn.autoContinue`).
#[must_use]
pub fn auto_learn_nudge() -> &'static str {
	AUTOLEARN_NUDGE_AUTOCONTINUE.trim()
}

/// TS `AutoLearnController#onAgentEnd` gate for the auto-continue capture.
///
/// `goal_mode` covers both halves of the upstream rule: the turn started in
/// goal mode, or goal mode is still active when the turn ends. Capture tools
/// must exist for the run (`manage_skill`/`learn` mounted); otherwise the
/// capture request would have nothing to do.
#[must_use]
pub fn should_auto_capture(
	config: &crate::config::Config,
	tool_calls: usize,
	aborted: bool,
	plan_enabled: bool,
	goal_mode: bool,
	has_capture_tools: bool,
) -> bool {
	if aborted
		|| !has_capture_tools
		|| !config.autolearn.enabled()
		|| !config.autolearn.auto_continue()
	{
		return false;
	}
	if tool_calls < config.autolearn.min_tool_calls() {
		return false;
	}
	!plan_enabled && !goal_mode
}

/// Turn-level gate shared by CLI, RPC, and ACP.
///
/// `started_in_goal` is snapshotted before the turn starts; `goal_now` covers a
/// turn that completed the goal. Either blocks capture, matching
/// `controller.ts` (`startedInGoalMode || getGoalModeState().enabled`).
#[must_use]
pub fn should_capture_turn(
	config: &crate::config::Config,
	tool_calls: usize,
	aborted: bool,
	plan_active: bool,
	started_in_goal: bool,
	goal_now: bool,
	has_capture_tools: bool,
) -> bool {
	should_auto_capture(
		config,
		tool_calls,
		aborted,
		plan_active,
		started_in_goal || goal_now,
		has_capture_tools,
	)
}

fn learn_schema() -> Value {
	json!({
		"type": "object",
		"properties": {
			"memory": {
				"type": "string",
				"description": "the durable, self-contained lesson to remember (what, when, why)"
			},
			"context": {
				"type": "string",
				"description": "optional source context for the lesson"
			},
			"skill": {
				"type": "object",
				"description": "also create or enhance a managed skill in the same call",
				"properties": {
					"action": { "type": "string", "enum": ["create", "update"] },
					"name": { "type": "string", "description": "kebab-case skill name" },
					"description": {
						"type": "string",
						"description": "one-line description of when to use the skill"
					},
					"body": {
						"type": "string",
						"description": "the SKILL.md body in markdown (no frontmatter)"
					}
				},
				"required": ["action", "name", "description", "body"],
				"additionalProperties": false
			}
		},
		"required": ["memory"],
		"additionalProperties": false
	})
}

fn manage_skill_schema() -> Value {
	json!({
		"type": "object",
		"properties": {
			"action": { "type": "string", "enum": ["create", "update", "delete"] },
			"name": { "type": "string", "description": "kebab-case skill name" },
			"description": {
				"type": "string",
				"description": "one-line description of when to use the skill (required for create/update)"
			},
			"body": {
				"type": "string",
				"description": "the SKILL.md body in markdown, no frontmatter (required for create/update)"
			}
		},
		"required": ["action", "name"],
		"additionalProperties": false
	})
}

/// `learn` with the local (file) memory backend.
pub struct LocalLearnTool {
	agent_dir: PathBuf,
	snapshot:  SkillSnapshot,
	options:   LoadOptions,
}

impl LocalLearnTool {
	/// TS `LearnTool.createIf` for `memory.backend=local`.
	///
	/// Returns `None` for `off`/`hindsight`/`mnemopi`: this slice implements no
	/// backend that could store a lesson, and building the tool anyway would
	/// report success without writing anything.
	#[must_use]
	pub fn create_if(
		autolearn_enabled: bool,
		backend: MemoryBackend,
		options: LoadOptions,
		snapshot: SkillSnapshot,
	) -> Option<Self> {
		if !autolearn_enabled || backend != MemoryBackend::Local {
			return None;
		}
		let agent_dir = options.agent_dir.clone().unwrap_or_else(dirs::agent_dir);
		Some(Self { agent_dir, snapshot, options })
	}

	#[must_use]
	pub fn snapshot(&self) -> &SkillSnapshot {
		&self.snapshot
	}
}

#[async_trait]
impl Tool for LocalLearnTool {
	fn name(&self) -> &str {
		LEARN_TOOL_NAME
	}

	fn description(&self) -> &str {
		LEARN_TOOL_DESCRIPTION
	}

	fn schema(&self) -> Value {
		learn_schema()
	}

	async fn execute(&self, args: &Value, ctx: &AgentContext) -> Result<ToolResult, ToolError> {
		let memory = required_str(args, "memory")?;
		let context = optional_str(args, "context");
		if ctx.cancellation_token.is_cancelled() {
			return Err(ToolError::Aborted("learn canceled".to_string()));
		}

		// 1) Persist the lesson. A lesson that sanitizes to nothing is an error,
		// not a silent success (TS `stored === 0`).
		let cancel = || ctx.cancellation_token.is_cancelled();
		let result = local_memory::save_learned_lesson(
			&self.agent_dir,
			&ctx.cwd,
			&SaveInput { content: memory, context },
			Some(&cancel),
		)
		.map_err(|error| {
			if ctx.cancellation_token.is_cancelled() {
				ToolError::Aborted("learn canceled".to_string())
			} else {
				ToolError::ExecutionFailed(error.to_string())
			}
		})?;
		if result.stored == 0 {
			return Err(ToolError::ExecutionFailed(
				"Lesson was empty after sanitization; nothing stored.".to_string(),
			));
		}
		// TS `learn` keeps this fixed wording for the local backend.
		let memory_message = "Lesson stored";

		// 2) Optional managed skill. A failure here is a partial outcome: the
		// lesson is already stored, so report both instead of claiming the whole
		// call failed.
		let Some(skill) = args.get("skill").filter(|value| !value.is_null()) else {
			return Ok(ToolResult {
				tool_call_id: String::new(),
				content: format!("{memory_message}."),
				is_error: false,
				terminate: false,
				details: Some(json!({ "skill": null })),
			});
		};
		let action = required_str(skill, "action")?;
		let name = required_str(skill, "name")?;
		let description = required_str(skill, "description")?;
		let body = required_str(skill, "body")?;
		let write_action = match action {
			"create" => WriteAction::Create,
			"update" => WriteAction::Update,
			other => {
				return Err(ToolError::InvalidArguments(format!(
					"skill.action must be \"create\" or \"update\", got {other:?}"
				)));
			},
		};
		if write_action == WriteAction::Create
			&& sanitize_skill_name(name)
				.is_ok_and(|safe| self.snapshot.is_name_claimed_by_authored_skill(&safe))
		{
			return Ok(ToolResult {
				tool_call_id: String::new(),
				content: format!(
					"{memory_message}. Did not create managed skill \"{name}\": an authored skill of that name already exists, and managed skills cannot override authored ones. Choose a different name."
				),
				is_error: true,
				terminate: false,
				details: Some(json!({ "skill": null, "shadowed": true })),
			});
		}
		if ctx.cancellation_token.is_cancelled() {
			return Err(ToolError::Aborted("learn canceled".to_string()));
		}
		write_managed_skill(
			&self.agent_dir,
			&WriteManagedSkillInput { action: write_action, name, description, body },
		)
		.map_err(|error| {
			ToolError::ExecutionFailed(format!(
				"{memory_message}, but the managed skill could not be written: {error}"
			))
		})?;
		// Keep this caller's snapshot current; TS defers to the next load, which
		// would leave the freshly minted skill invisible to `skill://` for the
		// rest of the turn. A refresh failure must not fail the stored lesson.
		let _ = self.snapshot.refresh(&self.options);
		let verb = if write_action == WriteAction::Create { "Created" } else { "Updated" };
		Ok(ToolResult {
			tool_call_id: String::new(),
			content: format!("{memory_message}. {verb} managed skill \"{name}\"."),
			is_error: false,
			terminate: false,
			details: Some(json!({ "skill": name })),
		})
	}
}

/// Direct create/update/delete of isolated managed skills.
pub struct ManageSkillTool {
	agent_dir: PathBuf,
	snapshot:  SkillSnapshot,
	options:   LoadOptions,
}

impl ManageSkillTool {
	/// TS `ManageSkillTool.createIf`: `autolearn.enabled` only.
	#[must_use]
	pub fn create_if(
		autolearn_enabled: bool,
		options: LoadOptions,
		snapshot: SkillSnapshot,
	) -> Option<Self> {
		if !autolearn_enabled {
			return None;
		}
		let agent_dir = options.agent_dir.clone().unwrap_or_else(dirs::agent_dir);
		Some(Self { agent_dir, snapshot, options })
	}

	#[must_use]
	pub fn snapshot(&self) -> &SkillSnapshot {
		&self.snapshot
	}
}

#[async_trait]
impl Tool for ManageSkillTool {
	fn name(&self) -> &str {
		MANAGE_SKILL_TOOL_NAME
	}

	fn description(&self) -> &str {
		MANAGE_SKILL_TOOL_DESCRIPTION
	}

	fn schema(&self) -> Value {
		manage_skill_schema()
	}

	async fn execute(&self, args: &Value, ctx: &AgentContext) -> Result<ToolResult, ToolError> {
		let action = required_str(args, "action")?;
		let name = required_str(args, "name")?;
		if ctx.cancellation_token.is_cancelled() {
			return Err(ToolError::Aborted("manage_skill canceled".to_string()));
		}
		if action == "delete" {
			delete_managed_skill(&self.agent_dir, name)
				.map_err(|error| ToolError::ExecutionFailed(error.to_string()))?;
			self.refresh()?;
			return Ok(ToolResult {
				tool_call_id: String::new(),
				content: format!("Deleted managed skill \"{name}\"."),
				is_error: false,
				terminate: false,
				details: Some(json!({ "action": "delete", "name": name })),
			});
		}
		let write_action = match action {
			"create" => WriteAction::Create,
			"update" => WriteAction::Update,
			other => {
				return Err(ToolError::InvalidArguments(format!(
					"action must be \"create\", \"update\" or \"delete\", got {other:?}"
				)));
			},
		};
		// The schema already requires both fields for create/update; this is the
		// typed contract for `write_managed_skill`.
		let (Some(description), Some(body)) = (optional_str(args, "description"), optional_str(args, "body"))
		else {
			return Err(ToolError::InvalidArguments(format!(
				"\"{action}\" requires both \"description\" and \"body\"."
			)));
		};
		// Managed skills resolve below authored ones, so a create under a claimed
		// name would write a file that never surfaces: refuse up front instead of
		// reporting a false "Created".
		if write_action == WriteAction::Create
			&& sanitize_skill_name(name)
				.is_ok_and(|safe| self.snapshot.is_name_claimed_by_authored_skill(&safe))
		{
			return Ok(ToolResult {
				tool_call_id: String::new(),
				content: format!(
					"Cannot create managed skill \"{name}\": an authored skill of that name already exists, and managed skills cannot override authored ones. Choose a different name."
				),
				is_error: true,
				terminate: false,
				details: Some(json!({ "action": "create", "name": name, "shadowed": true })),
			});
		}
		let skill_path = write_managed_skill(
			&self.agent_dir,
			&WriteManagedSkillInput { action: write_action, name, description, body },
		)
		.map_err(|error| ToolError::ExecutionFailed(error.to_string()))?;
		self.refresh()?;
		let relative = skill_path
			.strip_prefix(crate::managed_skills::managed_skills_dir(&self.agent_dir))
			.map_or_else(|_| skill_path.clone(), std::path::Path::to_path_buf);
		let verb = if write_action == WriteAction::Create { "Created" } else { "Updated" };
		Ok(ToolResult {
			tool_call_id: String::new(),
			content: format!(
				"{verb} managed skill \"{name}\" (managed-skills/{}).",
				relative.display()
			),
			is_error: false,
			terminate: false,
			details: Some(json!({ "action": action, "name": name })),
		})
	}
}

impl ManageSkillTool {
	/// Refresh this caller's snapshot; a failure surfaces (TS `await refreshSkills`).
	fn refresh(&self) -> Result<(), ToolError> {
		self
			.snapshot
			.refresh(&self.options)
			.map(|_| ())
			.map_err(|error| ToolError::ExecutionFailed(error.to_string()))
	}
}

fn required_str<'a>(args: &'a Value, key: &str) -> Result<&'a str, ToolError> {
	args
		.get(key)
		.and_then(Value::as_str)
		.ok_or_else(|| ToolError::InvalidArguments(format!("missing string field \"{key}\"")))
}

fn optional_str<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
	args.get(key).and_then(Value::as_str)
}

/// Shared snapshot handle for a session that mounts both tools and the
/// `skill://` handler.
#[must_use]
pub fn session_snapshot(entries: Vec<crate::skills::SkillEntry>) -> Arc<SkillSnapshot> {
	let snapshot = SkillSnapshot::new();
	snapshot.set(entries);
	Arc::new(snapshot)
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::fs;
	use std::io::Write as _;
	use std::path::Path;
	use std::sync::atomic::{AtomicU32, Ordering};

	static COUNTER: AtomicU32 = AtomicU32::new(0);

	fn temp_dir() -> PathBuf {
		let unique = format!(
			"omp-cli-autolearn-{}-{}",
			std::process::id(),
			COUNTER.fetch_add(1, Ordering::Relaxed)
		);
		let dir = std::env::temp_dir().join(unique);
		fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	fn write(path: &Path, text: &str) {
		fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
		let mut file = fs::File::create(path).expect("create");
		file.write_all(text.as_bytes()).expect("write");
	}

	fn options(dir: &Path) -> LoadOptions {
		let mut options = LoadOptions::new(dir);
		options.home = Some(dir.to_path_buf());
		options.agent_dir = Some(dir.join("agent"));
		options
	}

	fn ctx() -> AgentContext {
		AgentContext::new(PathBuf::from("/work/project"))
	}

	fn authored_skill(dir: &Path, name: &str) {
		write(
			&dir.join(format!("agent/skills/{name}/SKILL.md")),
			&format!("---\nname: {name}\ndescription: authored\n---\nbody\n"),
		);
	}

	#[test]
	fn tools_are_gated_like_ts() {
		let dir = temp_dir();
		let options = options(&dir);
		let snapshot = SkillSnapshot::new();
		assert!(LocalLearnTool::create_if(false, MemoryBackend::Local, options.clone(), snapshot.clone()).is_none());
		for backend in [MemoryBackend::Off, MemoryBackend::Hindsight, MemoryBackend::Mnemopi] {
			assert!(
				LocalLearnTool::create_if(true, backend, options.clone(), snapshot.clone()).is_none(),
				"{backend:?} must not mount the local tool"
			);
		}
		assert!(LocalLearnTool::create_if(true, MemoryBackend::Local, options.clone(), snapshot.clone()).is_some());
		assert!(ManageSkillTool::create_if(false, options.clone(), snapshot.clone()).is_none());
		assert!(ManageSkillTool::create_if(true, options, snapshot).is_some());
		assert_eq!(MemoryBackend::parse("local"), Some(MemoryBackend::Local));
		assert_eq!(MemoryBackend::parse("bogus"), None);
	}

	#[tokio::test]
	async fn learn_writes_learned_md_and_mints_a_managed_skill() {
		let dir = temp_dir();
		let options = options(&dir);
		let snapshot = SkillSnapshot::new();
		snapshot.refresh(&options).expect("refresh");
		let tool = LocalLearnTool::create_if(true, MemoryBackend::Local, options.clone(), snapshot.clone())
			.expect("tool");
		let result = tool
			.execute(
				&json!({
					"memory": "Always run the formatter before committing",
					"context": "review turn",
					"skill": {
						"action": "create",
						"name": "format-first",
						"description": "Format <before> committing",
						"body": "# Format\nrun it"
					}
				}),
				&ctx(),
			)
			.await
			.expect("execute");
		assert!(!result.is_error);
		assert_eq!(result.details, Some(json!({ "skill": "format-first" })));

		let learned = local_memory::read_learned_lessons(&local_memory::memory_root(
			&dir.join("agent"),
			Path::new("/work/project"),
		));
		assert!(learned.contains("- Always run the formatter before committing _(context: review turn)_"));

		let skill = fs::read_to_string(dir.join("agent/managed-skills/format-first/SKILL.md")).expect("skill");
		assert!(skill.contains("name: format-first"));
		assert!(skill.contains("description: Format before committing"));
		// The caller's snapshot saw the new skill immediately.
		assert!(snapshot.find("format-first").is_some());
	}

	#[tokio::test]
	async fn learn_reports_empty_and_never_writes() {
		let dir = temp_dir();
		let options = options(&dir);
		let snapshot = SkillSnapshot::new();
		let tool = LocalLearnTool::create_if(true, MemoryBackend::Local, options, snapshot).expect("tool");
		let error = tool
			.execute(&json!({ "memory": "<>`" }), &ctx())
			.await
			.expect_err("empty lesson");
		assert_eq!(
			error.to_string(),
			"Execution failed: Lesson was empty after sanitization; nothing stored."
		);
		assert!(!dir.join("agent/memories").exists());
	}

	#[tokio::test]
	async fn learn_refuses_to_shadow_an_authored_skill_but_keeps_the_lesson() {
		let dir = temp_dir();
		authored_skill(&dir, "deploy");
		let options = options(&dir);
		let snapshot = SkillSnapshot::new();
		snapshot.refresh(&options).expect("refresh");
		let tool = LocalLearnTool::create_if(true, MemoryBackend::Local, options, snapshot).expect("tool");
		let result = tool
			.execute(
				&json!({
					"memory": "Deploy uses the staging account",
					"skill": {
						"action": "create",
						"name": "Deploy",
						"description": "deploy",
						"body": "body"
					}
				}),
				&ctx(),
			)
			.await
			.expect("execute");
		assert!(result.is_error);
		assert!(result.content.contains("Lesson stored. Did not create managed skill"));
		assert_eq!(result.details, Some(json!({ "skill": null, "shadowed": true })));
		assert!(local_memory::read_learned_lessons(&local_memory::memory_root(
			&dir.join("agent"),
			Path::new("/work/project")
		))
		.contains("Deploy uses the staging account"));
		assert!(!dir.join("agent/managed-skills/deploy").exists());
	}

	#[tokio::test]
	async fn manage_skill_create_update_delete_and_shadow_refusal() {
		let dir = temp_dir();
		authored_skill(&dir, "review");
		let options = options(&dir);
		let snapshot = SkillSnapshot::new();
		snapshot.refresh(&options).expect("refresh");
		let tool = ManageSkillTool::create_if(true, options, snapshot.clone()).expect("tool");

		let created = tool
			.execute(
				&json!({ "action": "create", "name": "deploy", "description": "Deploy prod", "body": "# Deploy" }),
				&ctx(),
			)
			.await
			.expect("create");
		assert_eq!(created.content, "Created managed skill \"deploy\" (managed-skills/deploy/SKILL.md).");
		assert!(snapshot.find("deploy").is_some());

		let updated = tool
			.execute(
				&json!({ "action": "update", "name": "deploy", "description": "Deploy staging", "body": "# Staging" }),
				&ctx(),
			)
			.await
			.expect("update");
		assert_eq!(updated.content, "Updated managed skill \"deploy\" (managed-skills/deploy/SKILL.md).");

		let shadowed = tool
			.execute(
				&json!({ "action": "create", "name": "review", "description": "dup", "body": "body" }),
				&ctx(),
			)
			.await
			.expect("shadowed");
		assert!(shadowed.is_error);
		assert!(shadowed.content.starts_with("Cannot create managed skill \"review\""));
		assert_eq!(
			shadowed.details,
			Some(json!({ "action": "create", "name": "review", "shadowed": true }))
		);

		let deleted = tool.execute(&json!({ "action": "delete", "name": "deploy" }), &ctx()).await.expect("delete");
		assert_eq!(deleted.content, "Deleted managed skill \"deploy\".");
		assert!(snapshot.find("deploy").is_none());
	}

	#[test]
	fn auto_continue_gate_requires_enabled_auto_continue_and_threshold() {
		let mut config = crate::config::Config::default();
		assert!(!should_auto_capture(&config, 99, false, false, false, true));
		config.autolearn.enabled = Some(true);
		assert!(!should_auto_capture(&config, 99, false, false, false, true));
		config.autolearn.auto_continue = Some(true);
		assert!(!should_auto_capture(&config, 4, false, false, false, true));
		assert!(should_auto_capture(&config, 5, false, false, false, true));
		assert!(!should_auto_capture(&config, 5, true, false, false, true));
		assert!(!should_auto_capture(&config, 5, false, true, false, true));
		assert!(!should_auto_capture(&config, 5, false, false, true, true));
		assert!(!should_auto_capture(&config, 5, false, false, false, false));
		config.autolearn.min_tool_calls = Some(1);
		assert!(should_auto_capture(&config, 1, false, false, false, true));
	}

	#[test]
	fn turn_gate_allows_idle_plan_but_blocks_cancel_active_plan_and_goals() {
		let mut config = crate::config::Config::default();
		assert!(config.plan.enabled(), "default PlanGate feature is enabled");
		config.autolearn.enabled = Some(true);
		config.autolearn.auto_continue = Some(true);
		config.autolearn.min_tool_calls = Some(1);
		assert!(
			should_capture_turn(&config, 1, false, false, false, false, true),
			"feature enabled with no active plan must capture"
		);
		assert!(!should_capture_turn(&config, 1, true, false, false, false, true));
		assert!(!should_capture_turn(&config, 1, false, true, false, false, true));
		assert!(
			!should_capture_turn(&config, 1, false, false, true, false, true),
			"a turn that started in goal mode blocks capture even after completing it"
		);
		assert!(!should_capture_turn(&config, 1, false, false, false, true, true));
	}

	#[tokio::test]
	async fn manage_skill_requires_description_and_body() {
		let dir = temp_dir();
		let options = options(&dir);
		let snapshot = SkillSnapshot::new();
		let tool = ManageSkillTool::create_if(true, options, snapshot).expect("tool");
		let error = tool
			.execute(&json!({ "action": "create", "name": "deploy" }), &ctx())
			.await
			.expect_err("missing fields");
		assert_eq!(
			error.to_string(),
			"Invalid arguments: \"create\" requires both \"description\" and \"body\"."
		);
	}

	#[tokio::test]
	async fn cancelled_tool_call_writes_nothing() {
		let dir = temp_dir();
		let options = options(&dir);
		let snapshot = SkillSnapshot::new();
		let tool = LocalLearnTool::create_if(true, MemoryBackend::Local, options, snapshot).expect("tool");
		let context = ctx();
		context.cancellation_token.cancel();
		let error = tool
			.execute(&json!({ "memory": "lesson" }), &context)
			.await
			.expect_err("canceled");
		assert!(matches!(error, ToolError::Aborted(_)));
		assert!(!dir.join("agent/memories").exists());
	}

	#[test]
	fn guidance_follows_tool_presence() {
		assert!(build_auto_learn_instructions(false, true).is_none());
		let guidance = build_auto_learn_instructions(true, false).expect("guidance");
		assert!(guidance.starts_with("## Auto-Learn (experimental)"));
		assert!(!guidance.contains("Durable fact"));
		let both = build_auto_learn_instructions(true, true).expect("guidance");
		assert!(both.contains("Durable fact"));
		assert!(auto_learn_nudge().starts_with("Automated capture turn"));
		assert!(auto_learn_nudge().ends_with("Yield; wait for the user's next prompt."));
	}

	#[test]
	fn session_snapshot_shares_one_view() {
		let dir = temp_dir();
		authored_skill(&dir, "deploy");
		let options = options(&dir);
		let result = crate::skills::load_with(&options).expect("skills");
		let snapshot = session_snapshot(result.entries);
		assert!(snapshot.find("deploy").is_some());
		assert_eq!(Arc::strong_count(&snapshot), 1);
	}
}
