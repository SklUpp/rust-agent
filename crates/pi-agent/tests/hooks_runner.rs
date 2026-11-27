//! End-to-end subprocess hook tests: a real `sh` hook receives the JSON
//! payload and the scheduler honours its decision.
#![cfg(unix)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pi_agent::hooks::{HookConfig, HookEngine, HookEvent, HookInvocation, HookRule};
use pi_agent::tools::registry::ToolRegistry;
use pi_agent::{
	AgentContext, ApprovalHint, Tool, ToolApprover, ToolCall, ToolError, ToolResult,
	ToolScheduler,
};
use serde_json::{Value, json};

fn engine(event: HookEvent, script: &str) -> Arc<HookEngine> {
	Arc::new(HookEngine::new(
		HookConfig::new(vec![HookRule {
			event,
			matcher: None,
			invocation: HookInvocation::Shell(script.to_string()),
			timeout_ms: 2_000,
			fail_closed: false,
		}]),
		"hooks-runner-test",
		std::env::temp_dir(),
	))
}

struct RecordingTool {
	seen: Arc<Mutex<Vec<Value>>>,
}

#[async_trait]
impl Tool for RecordingTool {
	fn name(&self) -> &str {
		"record"
	}

	fn description(&self) -> &str {
		"records its arguments"
	}

	fn schema(&self) -> Value {
		json!({"type": "object"})
	}

	async fn execute(&self, args: &Value, _ctx: &AgentContext) -> Result<ToolResult, ToolError> {
		self.seen.lock().expect("recording lock").push(args.clone());
		Ok(ToolResult {
			tool_call_id: String::new(),
			content: "recorded".to_string(),
			is_error: false,
			terminate: false,
			details: None,
		})
	}
}

fn scheduler(hooks: Option<Arc<HookEngine>>) -> (ToolScheduler, Arc<Mutex<Vec<Value>>>) {
	let seen = Arc::new(Mutex::new(Vec::new()));
	let mut registry = ToolRegistry::new();
	registry.register(Arc::new(RecordingTool { seen: Arc::clone(&seen) }));
	let mut scheduler = ToolScheduler::new(registry);
	scheduler.set_hooks(hooks);
	(scheduler, seen)
}

fn call(args: Value) -> ToolCall {
	ToolCall { id: "call-1".to_string(), name: "record".to_string(), arguments: args }
}

#[tokio::test]
async fn pre_tool_use_rewrites_arguments_before_execution() {
	let hook = engine(
		HookEvent::PreToolUse,
		"cat >/dev/null; printf '%s' '{\"hookSpecificOutput\":{\"updatedInput\":{\"path\":\"hooked\"}}}'",
	);
	let (scheduler, seen) = scheduler(Some(hook));
	let results = scheduler
		.execute_batch(&[call(json!({"path": "original"}))], &AgentContext::new(std::env::temp_dir()))
		.await;
	assert_eq!(results.len(), 1);
	assert!(!results[0].is_error);
	assert_eq!(
		*seen.lock().expect("seen lock"),
		[json!({"path": "hooked"})],
		"the tool sees the hook-rewritten input"
	);
}

#[tokio::test]
async fn pre_tool_use_exit_two_blocks_execution() {
	let hook = engine(HookEvent::PreToolUse, "printf 'blocked' >&2; exit 2");
	let (scheduler, seen) = scheduler(Some(hook));
	let results = scheduler
		.execute_batch(&[call(json!({}))], &AgentContext::new(std::env::temp_dir()))
		.await;
	assert_eq!(results.len(), 1);
	assert!(results[0].is_error);
	assert!(results[0].content.contains("blocked"));
	assert!(seen.lock().expect("seen lock").is_empty());
}

#[tokio::test]
async fn post_tool_use_context_reaches_the_result() {
	let hook = engine(
		HookEvent::PostToolUse,
		"cat >/dev/null; printf '%s' '{\"hookSpecificOutput\":{\"additionalContext\":\"post note\"}}'",
	);
	let (scheduler, _) = scheduler(Some(hook));
	let results = scheduler
		.execute_batch(&[call(json!({}))], &AgentContext::new(std::env::temp_dir()))
		.await;
	assert!(!results[0].is_error);
	assert!(results[0].content.contains("Hook context: post note"));
}

#[tokio::test]
async fn hook_stdin_carries_the_standard_payload() {
	let hook = engine(
		HookEvent::PreToolUse,
		"payload=$(cat); printf '%s' \"$payload\" | grep -q '\"hook_event_name\":\"PreToolUse\"' && printf '%s' \"$payload\" | grep -q '\"tool_name\":\"record\"' && printf '%s' '{\"hookSpecificOutput\":{\"additionalContext\":\"payload ok\"}}'",
	);
	let decision = hook.pre_tool_use("record", &json!({"path": "x"})).await;
	assert_eq!(decision.additional_context, ["payload ok"]);
}

struct RecordingApprover {
	seen: Arc<Mutex<Vec<ApprovalHint>>>,
}

#[async_trait]
impl ToolApprover for RecordingApprover {
	fn supports_approval_hints(&self) -> bool {
		true
	}

	async fn approve(&self, _name: &str, _args: &Value) -> Result<(), String> {
		Ok(())
	}

	async fn approve_with_hint(
		&self,
		_name: &str,
		_args: &Value,
		hint: ApprovalHint,
	) -> Result<(), String> {
		self.seen.lock().expect("hint lock").push(hint);
		Ok(())
	}
}

#[tokio::test]
async fn pre_tool_use_ask_hint_reaches_the_approver() {
	let hook = engine(
		HookEvent::PreToolUse,
		"printf '%s' '{\"hookSpecificOutput\":{\"permissionDecision\":\"ask\"}}'",
	);
	let seen = Arc::new(Mutex::new(Vec::new()));
	let mut registry = ToolRegistry::new();
	registry.register(Arc::new(RecordingTool {
		seen: Arc::new(Mutex::new(Vec::new())),
	}));
	let mut scheduler = ToolScheduler::with_approver(
		registry,
		Arc::new(RecordingApprover {
			seen: Arc::clone(&seen),
		}),
	);
	scheduler.set_hooks(Some(hook));
	scheduler
		.execute_batch(&[call(json!({}))], &AgentContext::new(std::env::temp_dir()))
		.await;
	let hints = seen.lock().expect("hint lock").clone();
	assert_eq!(hints, [ApprovalHint { force_ask: true }]);
}

#[tokio::test]
async fn force_ask_without_approver_fails_closed() {
	let hook = engine(
		HookEvent::PreToolUse,
		"printf '%s' '{\"hookSpecificOutput\":{\"permissionDecision\":\"ask\"}}'",
	);
	let (scheduler, seen) = scheduler(Some(hook));
	let results = scheduler
		.execute_batch(&[call(json!({}))], &AgentContext::new(std::env::temp_dir()))
		.await;
	assert!(results[0].is_error);
	assert!(
		results[0]
			.content
			.contains("no approver is available")
	);
	assert!(seen.lock().expect("seen lock").is_empty());
}

/// Ambient credentials must not reach hook subprocesses, while the platform
/// baseline (`PATH`) survives. The real assertion runs in a re-executed child
/// process with the credentials set explicitly, so no parallel test ever sees
/// a mutated process environment (and there is nothing to restore).
#[tokio::test]
async fn hook_subprocess_env_is_scrubbed() {
	const CHILD_MARKER: &str = "PI_HOOK_SCRUB_CHILD";
	if std::env::var_os(CHILD_MARKER).is_none() {
		let exe = std::env::current_exe().expect("current test executable");
		let output = std::process::Command::new(exe)
			.args(["--exact", "hook_subprocess_env_is_scrubbed", "--nocapture"])
			.env(CHILD_MARKER, "1")
			.env("OPENAI_API_KEY", "sk-ambient-leak-canary")
			.env("SSH_AUTH_SOCK", "/tmp/ambient-agent.sock")
			.output()
			.expect("re-exec hook env test");
		assert!(
			output.status.success(),
			"isolated hook env test failed: {}\nstdout:\n{}\nstderr:\n{}",
			output.status,
			String::from_utf8_lossy(&output.stdout),
			String::from_utf8_lossy(&output.stderr)
		);
		return;
	}

	let hook = engine(
		HookEvent::PreToolUse,
		r#"cat >/dev/null; leak=""; [ -n "${OPENAI_API_KEY+x}" ] && leak="$leak openai"; [ -n "${SSH_AUTH_SOCK+x}" ] && leak="$leak ssh"; [ -n "$PATH" ] || leak="$leak nopath"; printf '{"hookSpecificOutput":{"additionalContext":"%s"}}' "${leak:-clean}""#,
	);
	let decision = hook.pre_tool_use("record", &json!({})).await;
	assert_eq!(decision.additional_context, ["clean"], "hook saw ambient credentials or lost PATH");
}
