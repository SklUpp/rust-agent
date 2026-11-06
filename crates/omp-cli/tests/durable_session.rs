//! Behavioral durability tests for the live loop boundary.
//!
//! These use a scripted provider, a counting tool, and a real SQLite store on
//! disk. They assert ordering and recovery behavior, not source text.

use std::{
	collections::VecDeque,
	path::PathBuf,
	sync::{
		Arc, Mutex,
		atomic::{AtomicUsize, Ordering},
	},
};

use async_trait::async_trait;
use pi_agent::{
	AgentConfig, AgentError, AgentLoop, AgentState, ChunkStream, DurableBoundary, DurableError,
	Message, ModelProvider, ProviderError, StopReason, Tool, ToolCall, ToolChoice, ToolRegistry,
	ToolResult,
};
use serde_json::json;

use omp_cli::{
	durable_session::DurableSession,
	output,
	session::{
		CURRENT_SESSION_VERSION, MessageContent, MessageEntry, SessionHeader, SessionMessage,
		SessionOptions, SessionStore,
	},
};

struct CountingTool {
	runs: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for CountingTool {
	fn name(&self) -> &str {
		"effect"
	}

	fn description(&self) -> &str {
		"counts one effect"
	}

	fn schema(&self) -> serde_json::Value {
		json!({"type": "object", "properties": {}})
	}

	async fn execute(
		&self,
		_arguments: &serde_json::Value,
		_ctx: &pi_agent::AgentContext,
	) -> Result<ToolResult, pi_agent::ToolError> {
		self.runs.fetch_add(1, Ordering::SeqCst);
		Ok(ToolResult {
			tool_call_id: String::new(),
			content: "effect ran".to_string(),
			is_error: false,
			terminate: false,
			details: None,
		})
	}
}

struct ScriptedProvider {
	script: Mutex<VecDeque<Message>>,
	calls: AtomicUsize,
	/// Set on the second request: number of unresolved executions observed.
	unresolved_on_second: Mutex<Option<usize>>,
	session: Arc<DurableSession>,
}

#[async_trait]
impl ModelProvider for ScriptedProvider {
	async fn complete(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		let call = self.calls.fetch_add(1, Ordering::SeqCst);
		if call == 1 {
			let unresolved = self
				.session
				.unresolved_intents()
				.map_err(|error| ProviderError::Protocol(error.to_string()))?;
			*self.unresolved_on_second.lock().unwrap() = Some(unresolved.len());
		}
		self
			.script
			.lock()
			.unwrap()
			.pop_front()
			.ok_or_else(|| ProviderError::Protocol("script exhausted".to_string()))
	}

	async fn stream(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		Err(ProviderError::Protocol("scripted provider has no stream".to_string()))
	}
}

/// Delegates message/outcome writes to the real store but fails the intent
/// write, simulating a save failure before the effect.
#[derive(Debug)]
struct FailBegin(Arc<DurableSession>);

#[async_trait]
impl DurableBoundary for FailBegin {
	async fn persist_messages(&self, messages: &[Message]) -> Result<(), DurableError> {
		self.0.persist_messages(messages).await
	}

	async fn compacted(
		&self,
		messages: &[Message],
		report: &pi_agent::CompactionReport,
	) -> Result<(), DurableError> {
		self.0.compacted(messages, report).await
	}

	async fn resync_messages(&self, messages: &[Message]) -> Result<(), DurableError> {
		self.0.resync_messages(messages).await
	}

	async fn begin_execution(&self, _call: &ToolCall) -> Result<String, DurableError> {
		Err(DurableError::new("injected intent write failure"))
	}

	async fn finish_execution(
		&self,
		execution_id: &str,
		call: &ToolCall,
		result: &ToolResult,
	) -> Result<(), DurableError> {
		self.0.finish_execution(execution_id, call, result).await
	}
}

fn temp_dir(label: &str) -> PathBuf {
	static NEXT: AtomicUsize = AtomicUsize::new(0);
	let path = std::env::temp_dir().join(format!(
		"omp-durable-{label}-{}-{}",
		std::process::id(),
		NEXT.fetch_add(1, Ordering::SeqCst)
	));
	std::fs::create_dir_all(&path).expect("create temp dir");
	path
}

fn header(id: &str, cwd: &std::path::Path) -> SessionHeader {
	SessionHeader {
		kind: "session".to_string(),
		version: CURRENT_SESSION_VERSION,
		id: id.to_string(),
		timestamp: output::iso8601(std::time::SystemTime::now()),
		cwd: cwd.display().to_string(),
		model: None,
		provider: None,
		title: None,
		title_source: None,
		additional_directories: Vec::new(),
		parent_session: None,
		previous_session_files: Vec::new(),
		provider_prompt_cache_key: None,
	}
}

fn tool_call(id: &str) -> Message {
	Message::assistant_with_tools(
		"",
		vec![ToolCall {
			id: id.to_string(),
			name: "effect".to_string(),
			arguments: json!({}),
		}],
	)
}

fn config(cwd: &std::path::Path) -> AgentConfig {
	AgentConfig {
		max_turns: 4,
		context_limit: 100_000,
		system_prompt: None,
		cwd: cwd.to_path_buf(),
	}
}

fn registry(runs: Arc<AtomicUsize>) -> ToolRegistry {
	let mut registry = ToolRegistry::new();
	registry.register(Arc::new(CountingTool { runs }));
	registry
}

async fn run_loop(
	session: Arc<DurableSession>,
	script: Vec<Message>,
	runs: Arc<AtomicUsize>,
	cwd: &std::path::Path,
) -> (Result<Vec<pi_agent::TurnResult>, AgentError>, Arc<ScriptedProvider>) {
	let provider = Arc::new(ScriptedProvider {
		script: Mutex::new(script.into()),
		calls: AtomicUsize::new(0),
		unresolved_on_second: Mutex::new(None),
		session: Arc::clone(&session),
	});
	let boundary: Arc<dyn DurableBoundary> = session;
	let agent = AgentLoop::new(config(cwd), provider.clone(), registry(runs))
		.with_durable(Some(boundary));
	let mut state = AgentState { messages: Vec::new(), total_turns: 0 };
	let result = agent.run_dialog("go", &mut state, None).await;
	(result, provider)
}

#[tokio::test]
async fn intent_failure_prevents_the_effect() {
	let dir = temp_dir("intent-failure");
	let session = Arc::new(
		DurableSession::create(dir.join("sessions.db"), header("s1", &dir)).expect("create"),
	);
	let runs = Arc::new(AtomicUsize::new(0));
	let provider = Arc::new(ScriptedProvider {
		script: Mutex::new([tool_call("call_fail"), Message::assistant("done")].into()),
		calls: AtomicUsize::new(0),
		unresolved_on_second: Mutex::new(None),
		session: Arc::clone(&session),
	});
	let boundary: Arc<dyn DurableBoundary> = Arc::new(FailBegin(Arc::clone(&session)));
	let agent =
		AgentLoop::new(config(&dir), provider, registry(Arc::clone(&runs))).with_durable(Some(boundary));
	let mut state = AgentState { messages: Vec::new(), total_turns: 0 };
	let error = agent
		.run_dialog("go", &mut state, None)
		.await
		.expect_err("intent write failure must terminate the run");
	assert!(matches!(error, AgentError::Durable(_)), "{error:?}");
	assert_eq!(runs.load(Ordering::SeqCst), 0, "no effect may run after intent failure");
}

#[tokio::test]
async fn outcome_is_durable_before_the_next_model_request() {
	let dir = temp_dir("outcome-order");
	let session = Arc::new(
		DurableSession::create(dir.join("sessions.db"), header("s2", &dir)).expect("create"),
	);
	let runs = Arc::new(AtomicUsize::new(0));
	let (result, provider) = run_loop(
		Arc::clone(&session),
		vec![tool_call("call_1"), Message::assistant("done")],
		Arc::clone(&runs),
		&dir,
	)
	.await;
	result.expect("dialog");
	assert_eq!(runs.load(Ordering::SeqCst), 1);
	assert_eq!(
		*provider.unresolved_on_second.lock().unwrap(),
		Some(0),
		"the tool outcome must be committed before the second provider request"
	);
	let loaded = session.load().expect("reload");
	assert!(loaded
		.messages
		.iter()
		.any(|entry| entry.message.role == "toolResult"));
}

#[tokio::test]
async fn reopened_unresolved_intent_is_not_replayed() {
	let dir = temp_dir("unresolved");
	let db = dir.join("sessions.db");
	let session =
		DurableSession::create(db.clone(), header("s3", &dir)).expect("create");
	let call = ToolCall {
		id: "provider_duplicate".to_string(),
		name: "effect".to_string(),
		arguments: json!({}),
	};
	let execution = session.begin_execution(&call).await.expect("intent");
	// No finish: this models a crash after admission and before a durable
	// outcome. Reopening must surface the intent, not run the tool again.
	let reopened = DurableSession::open(db.clone(), "s3", header("s3", &dir))
		.expect("reopen")
		.0;
	let unresolved = reopened.unresolved_intents().expect("read intents");
	assert_eq!(unresolved.len(), 1);
	assert_eq!(unresolved[0].execution_id, execution);
	assert_eq!(unresolved[0].provider_call_id, call.id);
}

#[tokio::test]
async fn duplicate_provider_call_ids_get_distinct_execution_ids() {
	let dir = temp_dir("duplicate-ids");
	let session = Arc::new(
		DurableSession::create(dir.join("sessions.db"), header("s4", &dir)).expect("create"),
	);
	let call = ToolCall {
		id: "same_provider_id".to_string(),
		name: "effect".to_string(),
		arguments: json!({}),
	};
	let first = session.begin_execution(&call).await.expect("first intent");
	let second = session.begin_execution(&call).await.expect("second intent");
	assert_ne!(first, second, "execution identity must be store-owned");
	assert_ne!(first, call.id);
	assert_ne!(second, call.id);
	let result = ToolResult {
		tool_call_id: call.id.clone(),
		content: "ok".to_string(),
		is_error: false,
		terminate: false,
		details: None,
	};
	session
		.finish_execution(&first, &call, &result)
		.await
		.expect("first outcome");
	session
		.finish_execution(&second, &call, &result)
		.await
		.expect("second outcome");
	assert!(session.unresolved_intents().expect("intents").is_empty());

	// Two outcomes with the same provider id must both persist; only those
	// two occurrences are skipped. A third synthetic result with the same id is
	// a different execution and must still be written.
	let messages = vec![
		Message::assistant("one"),
		Message::tool_with_status(call.id.clone(), "effect", "one", false),
		Message::assistant("two"),
		Message::tool_with_status(call.id.clone(), "effect", "two", false),
		Message::assistant("three"),
		Message::tool_with_status(call.id.clone(), "effect", "synthetic", false),
	];
	session.persist_messages(&messages).await.expect("persist chain");
	let loaded = session.load().expect("load");
	let tools: Vec<_> = loaded
		.messages
		.iter()
		.filter(|entry| entry.message.role == "toolResult")
		.collect();
	assert_eq!(tools.len(), 3, "two outcomes plus the synthetic result");
	assert!(tools.iter().any(|entry| entry.message.content.text() == "synthetic"));
}

#[test]
fn session_store_uses_one_sqlite_authority_and_no_jsonl_journal() {
	let dir = temp_dir("store-authority");
	let options = SessionOptions {
		enabled: true,
		cwd: dir.clone(),
		model: None,
		provider: None,
		dir: Some(dir.clone()),
		id: Some("s5".to_string()),
		continue_latest: false,
		no_title: false,
		parent_session: None,
	};
	let mut store = SessionStore::open(&options);
	assert!(store.enabled(), "durable store must be enabled");
	let entry = MessageEntry::new(
		"msg-1",
		None,
		output::iso8601(std::time::SystemTime::now()),
		SessionMessage::user(MessageContent::Text("hello".to_string()), 1),
	);
	store.append_message(&entry).expect("append to sqlite");
	let reopened = SessionStore::open(&options);
	assert!(reopened.enabled());
	assert_eq!(reopened.messages().len(), 1);
	assert_eq!(reopened.messages()[0].id, "msg-1");
	assert!(
		!dir.join("s5.jsonl").exists(),
		"a durable session must not create a second live JSONL journal"
	);
	assert!(dir.join("sessions.db").exists());
}

#[tokio::test]
async fn compaction_remaps_by_stable_revision_not_by_length() {
	let dir = temp_dir("compaction-revision");
	let session = Arc::new(
		DurableSession::create(dir.join("sessions.db"), header("s6", &dir)).expect("create"),
	);
	let original = vec![Message::user("first"), Message::assistant("second")];
	session
		.persist_messages(&original)
		.await
		.expect("persist original");
	assert_eq!(session.load().expect("load").messages.len(), 2);

	// A compaction notice is a new leading message; the retained tail is the
	// same assistant revision at its new index.
	let compacted = vec![Message::user("notice"), Message::assistant("second")];
	let report = pi_agent::CompactionReport { old_len: 2, kept_from: 1, kept_at: 1 };
	session
		.compacted(&compacted, &report)
		.await
		.expect("retained tail matches its persisted revision");
	session
		.persist_messages(&compacted)
		.await
		.expect("compacted view is already durable");
	assert_eq!(
		session.load().expect("load").messages.len(),
		2,
		"compaction must not duplicate the retained tail"
	);
	let loaded = session.load().expect("load projection");
	let projection = loaded.projection.expect("compaction projection persisted");
	assert_eq!(projection.kept.len(), 1, "only the retained archive row");
	assert_eq!(projection.synthetic.len(), 1, "only the runtime-only notice");

	// The next prompt extends the selected projection, not the stale archive.
	let next = vec![
		Message::user("notice"),
		Message::assistant("second"),
		Message::user("third"),
	];
	session.persist_messages(&next).await.expect("append after compaction");
	let projection = session
		.load()
		.expect("load projection")
		.projection
		.expect("projection survives the next write");
	assert_eq!(
		projection.kept.len(),
		2,
		"the new turn joins the compacted projection"
	);

	// Same lengths, different retained content: revision check must refuse.
	let diverged = vec![Message::user("notice"), Message::assistant("rewritten")];
	let error = session
		.compacted(&diverged, &report)
		.await
		.expect_err("a rewritten retained message is not the persisted revision");
	assert!(error.to_string().contains("never persisted"), "{error}");
}

#[tokio::test]
async fn resume_seed_skips_terminal_error_turns_like_provider_replay() {
	let dir = temp_dir("resume-error-seed");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s7", &dir)).expect("create");
	let mut failed = Message::assistant("boom");
	failed.stop_reason = Some(StopReason::Error);
	session
		.persist_messages(&[Message::user("q"), failed])
		.await
		.expect("persist terminal error turn");
	drop(session);

	let reopened = DurableSession::open(db, "s7", header("s7", &dir))
		.expect("reopen")
		.0;
	// The provider replay drops the terminal error assistant, so the resume
	// seed must drop it too; otherwise the next write can never align.
	reopened
		.persist_messages(&[Message::user("q"), Message::user("next")])
		.await
		.expect("resume seed must match provider replay");
	let loaded = reopened.load().expect("load");
	assert!(
		loaded.messages.iter().any(|entry| {
			entry.message.role == "assistant"
				&& entry.message.stop_reason.as_deref() == Some("error")
		}),
		"archive must retain the terminal error turn"
	);
	assert_eq!(
		loaded
			.messages
			.iter()
			.filter(|entry| entry.message.role == "user")
			.count(),
		2,
		"the next request appends its new user turn"
	);
}

#[test]
fn replay_keeps_plain_text_tool_result_shape() {
	let entry: omp_cli::session::MessageEntry = serde_json::from_value(serde_json::json!({
		"type": "message",
		"id": "t1",
		"timestamp": "2026-01-01T00:00:00.000Z",
		"message": {
			"role": "toolResult",
			"toolCallId": "c1",
			"toolName": "bash",
			"content": "ok",
			"isError": false,
			"timestamp": 1
		}
	}))
	.expect("tool result fixture");
	let replayed = omp_cli::runtime::history_messages_from_entries(std::slice::from_ref(&entry))
		.expect("replay");
	assert_eq!(replayed.len(), 1);
	assert!(replayed[0].parts.is_empty(), "plain text stays a text content");
	assert_eq!(replayed[0].content, "ok");
}
