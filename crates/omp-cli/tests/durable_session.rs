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
	time::Duration,
};

use async_trait::async_trait;
use pi_agent::{
	AgentConfig, AgentError, AgentLoop, AgentState, ChunkStream, ContentPart, DurableBoundary,
	DurableError, Message, ModelProvider, ProviderError, Role, SteeringEvent, StopReason, Tool,
	ToolCall, ToolChoice, ToolRegistry, ToolResult,
};
use serde_json::{Value, json};

use omp_cli::{
	durable_session::DurableSession,
	output,
	session::{
		CURRENT_SESSION_VERSION, MessageContent, MessageEntry, SessionEntry, SessionHeader,
		SessionMessage, SessionOptions, SessionStore,
	},
	session_store::{NewIntent, SqliteStore, StoreError},
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

	// Same compacted length, different retained content: the report must name
	// the current three-message conversation (the appended turn included). Its
	// shape and length guards pass, so only the retained-revision check can
	// refuse it, before any write.
	let diverged = vec![Message::user("notice"), Message::assistant("rewritten")];
	let current = pi_agent::CompactionReport { old_len: 3, kept_from: 2, kept_at: 1 };
	session
		.compacted(&diverged, &current)
		.await
		.expect_err("a rewritten retained message is not the persisted revision");

	// A refused rewrite must leave the durable view untouched: the archive, the
	// selected projection, and the write cursor still describe the three
	// messages written above, and the next write stays a no-op.
	let refused = session.load().expect("load after refusal");
	assert_eq!(refused.messages.len(), 3, "a refused compaction appends nothing");
	assert_eq!(
		refused.projection.as_ref().map(|projection| projection.kept.len()),
		Some(2),
		"a refused compaction keeps the selected projection"
	);
	session
		.persist_messages(&next)
		.await
		.expect("the refusal must not disturb later writes");
	assert_eq!(
		session.load().expect("load after retry").messages.len(),
		3,
		"the durable view is unchanged by the refusal"
	);

	// A report from before the appended turn is refused too. Staleness is a
	// behavioral contract, not a message-text one.
	session
		.compacted(&compacted, &report)
		.await
		.expect_err("a report from before the appended turn is stale");
	assert_eq!(
		session.load().expect("load after stale refusal").messages.len(),
		3,
		"a stale report must not rewrite the durable view"
	);
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

/// Older durable/TS rows can archive tool images under `details.images`
/// instead of content parts; replay must restore them, never only the text.
#[test]
fn replay_restores_tool_images_archived_in_details() {
	let entry: omp_cli::session::MessageEntry = serde_json::from_value(serde_json::json!({
		"type": "message",
		"id": "t2",
		"timestamp": "2026-01-01T00:00:00.000Z",
		"message": {
			"role": "toolResult",
			"toolCallId": "c2",
			"toolName": "read",
			"content": "file content",
			"isError": false,
			"timestamp": 1,
			"details": {"images": [{"data": "aGVsbG8=", "mimeType": "image/png"}]}
		}
	}))
	.expect("legacy tool result fixture");
	let replayed = omp_cli::runtime::history_messages_from_entries(std::slice::from_ref(&entry))
		.expect("replay");
	assert_eq!(replayed.len(), 1);
	assert_eq!(replayed[0].content, "file content");
	assert!(
		replayed[0]
			.parts
			.iter()
			.any(|part| matches!(part, ContentPart::Image { .. })),
		"images archived under details must be replayed"
	);

	// The provider builds tool text from parts whenever any part exists, so the
	// replayed text must be visible on the wire next to the image attachment.
	use pi_agent::provider::{OpenAiContent, OpenAiContentPart, to_openai_messages};
	let wire = to_openai_messages(&replayed).expect("openai wire");
	let tool = wire
		.iter()
		.find(|message| message.role == "tool")
		.expect("tool wire message");
	assert!(
		matches!(&tool.content, Some(OpenAiContent::Text(text)) if text.as_str() == "file content"),
		"the tool answer text must reach the model, got {:?}",
		tool.content
	);
	assert!(
		wire.iter().any(|message| message.role == "user"
			&& matches!(&message.content, Some(OpenAiContent::Parts(parts))
				if parts.iter().any(|part| matches!(part, OpenAiContentPart::ImageUrl { .. })))),
		"the replayed image must be attached to the request"
	);
}

/// Tool outcomes are written before the loop builds the provider message, so
/// the outcome row must itself carry the text and image parts; otherwise the
/// live tracked revision and the next replay would disagree.
#[tokio::test]
async fn tool_outcome_archives_multimodal_content() {
	let dir = temp_dir("tool-outcome-images");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db, header("s14", &dir)).expect("create");
	let call = ToolCall {
		id: "c1".to_string(),
		name: "read".to_string(),
		arguments: json!({}),
	};
	let execution = session.begin_execution(&call).await.expect("intent");
	let result = ToolResult {
		tool_call_id: "c1".to_string(),
		content: "file content".to_string(),
		is_error: false,
		terminate: false,
		details: Some(json!({
			"images": [{"data": "aGVsbG8=", "mimeType": "image/png"}]
		})),
	};
	session
		.finish_execution(&execution, &call, &result)
		.await
		.expect("outcome");
	let loaded = session.load().expect("load");
	let entry = loaded
		.messages
		.iter()
		.find(|entry| entry.message.role == "toolResult")
		.expect("tool result row");
	assert_eq!(entry.message.content.text(), "file content");
	let parts = entry.message.content.parts();
	assert!(
		parts
			.iter()
			.any(|part| part.kind == "text" && part.text.as_deref() == Some("file content")),
		"text must be stored beside the image"
	);
	assert!(parts.iter().any(|part| part.kind == "image"), "image must be stored");
	let replayed =
		omp_cli::runtime::history_messages_from_entries(&loaded.branch_messages).expect("replay");
	assert!(
		replayed[0]
			.parts
			.iter()
			.any(|part| matches!(part, ContentPart::Image { .. })),
		"replay restores the multimodal tool result"
	);
	std::fs::remove_dir_all(dir).ok();
}

/// A terminal aborted turn stays archived but is absent from every replayed
/// view. The next REPL turn starts from that shorter history and must realign
/// instead of being rejected as divergence.
#[tokio::test]
async fn aborted_turn_does_not_block_the_next_turn() {
	let dir = temp_dir("aborted-next-turn");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db, header("s10", &dir)).expect("create");
	let mut aborted = Message::assistant("partial");
	aborted.stop_reason = Some(StopReason::Aborted);
	aborted.error_message = Some("Interrupted by user".to_string());
	session
		.persist_messages(&[Message::user("q"), aborted])
		.await
		.expect("persist aborted turn");

	// The next turn replays only the prompt, then appends its own user message.
	session
		.persist_messages(&[Message::user("q"), Message::user("next")])
		.await
		.expect("an aborted turn must not block the next turn");

	let loaded = session.load().expect("load");
	assert!(
		loaded.messages.iter().any(|entry| {
			entry.message.role == "assistant"
				&& entry.message.stop_reason.as_deref() == Some("aborted")
		}),
		"archive must retain the aborted turn"
	);
	assert_eq!(
		loaded
			.messages
			.iter()
			.filter(|entry| entry.message.role == "user")
			.count(),
		2,
		"both user turns are persisted"
	);
	std::fs::remove_dir_all(dir).ok();
}

/// A tool result can carry its answer text outside `parts` next to image
/// blocks; the persisted projection must keep both and replay to the same
/// revision.
#[tokio::test]
async fn tool_result_image_keeps_its_text_across_replay() {
	let dir = temp_dir("tool-image-text");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db, header("s11", &dir)).expect("create");
	let result = Message::tool_with_status_and_parts(
		"call_1",
		"read",
		"file content",
		vec![ContentPart::Image {
			data: "aGVsbG8=".to_string(),
			mime_type: "image/png".to_string(),
		}],
		false,
	);
	session
		.persist_messages(&[Message::user("read it"), result])
		.await
		.expect("persist tool result");
	let loaded = session.load().expect("load");
	let entry = loaded
		.messages
		.iter()
		.find(|entry| entry.message.role == "toolResult")
		.expect("tool result row");
	assert_eq!(entry.message.content.text(), "file content", "text must survive");
	let parts = entry.message.content.parts();
	assert!(
		parts
			.iter()
			.any(|part| part.kind == "text" && part.text.as_deref() == Some("file content")),
		"the text part must be stored beside the image"
	);
	assert!(parts.iter().any(|part| part.kind == "image"), "the image must survive");

	// The replay projection (the next turn's history) must hash to the stored
	// revision, so appending after it aligns instead of diverging.
	let mut next = omp_cli::runtime::history_messages_from_entries(&loaded.branch_messages)
		.expect("replay stored branch");
	next.push(Message::user("next"));
	session
		.persist_messages(&next)
		.await
		.expect("replayed history must align with the stored revision");
	std::fs::remove_dir_all(dir).ok();
}

/// A supervisor that streams nothing and never answers; an interrupt must
/// still journal the aborted turn with the exact reason.
struct HangingProvider {
	started: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl ModelProvider for HangingProvider {
	async fn complete(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		self.started.notify_one();
		tokio::time::sleep(Duration::from_secs(3_600)).await;
		Err(ProviderError::Protocol("unreachable".to_string()))
	}

	async fn stream(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		Err(ProviderError::Protocol("hanging provider has no stream".to_string()))
	}
}

/// Stalls the first request (so a steering interrupt can land mid-flight) and
/// answers every later one, recording exactly what each request saw.
struct HangThenAnswer {
	started: Arc<tokio::sync::Notify>,
	calls: AtomicUsize,
	seen: Mutex<Vec<Vec<Message>>>,
}

#[async_trait]
impl ModelProvider for HangThenAnswer {
	async fn complete(
		&self,
		messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		let call = self.calls.fetch_add(1, Ordering::SeqCst);
		self.seen.lock().unwrap().push(messages.to_vec());
		if call == 0 {
			self.started.notify_one();
			tokio::time::sleep(Duration::from_secs(3_600)).await;
		}
		Ok(Message::assistant("second".to_string()))
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

/// Records every provider request and always answers, so a test can assert the
/// exact history the model saw after a host reload.
struct RecordingProvider {
	seen: Mutex<Vec<Vec<Message>>>,
}

#[async_trait]
impl ModelProvider for RecordingProvider {
	async fn complete(
		&self,
		messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		self.seen.lock().unwrap().push(messages.to_vec());
		Ok(Message::assistant("answered".to_string()))
	}

	async fn stream(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		Err(ProviderError::Protocol("recording provider has no stream".to_string()))
	}
}

/// The `--max-time` deadline reaches the loop as a reasoned interrupt; the
/// durable aborted turn must record that reason, not the user-interrupt label.
#[tokio::test]
async fn timeout_interrupt_records_its_own_reason() {
	let (session, dir) = run_interrupted_stall(
		"timeout-reason",
		"s12",
		SteeringEvent::InterruptWithReason(omp_cli::deadline::TIMEOUT_MESSAGE.to_string()),
	)
	.await;
	let loaded = session.load().expect("load");
	let aborted = loaded
		.messages
		.iter()
		.find(|entry| entry.message.role == "assistant")
		.expect("aborted assistant row");
	assert_eq!(aborted.message.stop_reason.as_deref(), Some("aborted"));
	assert_eq!(
		aborted.message.error_message.as_deref(),
		Some(omp_cli::deadline::TIMEOUT_MESSAGE),
		"the deadline reason must not be rewritten as a user interrupt"
	);
	std::fs::remove_dir_all(dir).ok();
}

/// A bare interrupt stays a user interrupt even though it takes the same
/// journaling path as the reasoned deadline interrupt.
#[tokio::test]
async fn user_interrupt_keeps_the_user_label() {
	let (session, dir) = run_interrupted_stall("user-interrupt-label", "s13", SteeringEvent::Interrupt).await;
	let loaded = session.load().expect("load");
	let aborted = loaded
		.messages
		.iter()
		.find(|entry| entry.message.role == "assistant")
		.expect("aborted assistant row");
	assert_eq!(aborted.message.stop_reason.as_deref(), Some("aborted"));
	assert_eq!(
		aborted.message.error_message.as_deref(),
		Some("Interrupted by user")
	);
	std::fs::remove_dir_all(dir).ok();
}

/// An interrupt that lands during a provider request must journal the accepted
/// steering *and* the aborted turn before the dialog unwinds, and the next turn
/// on the same store must start from that exact history.
#[tokio::test]
async fn interrupt_journals_injections_before_the_next_turn() {
	let dir = temp_dir("interrupt-injection");
	let db = dir.join("sessions.db");
	let session = Arc::new(DurableSession::create(db, header("s14", &dir)).expect("create"));
	let started = Arc::new(tokio::sync::Notify::new());
	let provider = Arc::new(HangThenAnswer {
		started: Arc::clone(&started),
		calls: AtomicUsize::new(0),
		seen: Mutex::new(Vec::new()),
	});
	let provider_dyn: Arc<dyn ModelProvider> = provider.clone();
	let agent = AgentLoop::new(config(&dir), provider_dyn, ToolRegistry::new())
		.with_durable(Some(Arc::clone(&session) as Arc<dyn DurableBoundary>));
	let mut state = AgentState::default();
	let (tx, rx) = tokio::sync::mpsc::channel(4);
	{
		let first = agent.run_dialog("first", &mut state, Some(rx));
		tokio::pin!(first);
		tokio::select! {
			_ = started.notified() => {},
			res = &mut first => panic!("dialog ended before the provider request started: {res:?}"),
		}
		tx.send(SteeringEvent::InjectMessage(vec![ContentPart::text("please also note")]))
			.await
			.expect("accept injection");
		tx
			.send(SteeringEvent::InterruptWithReason(
				omp_cli::deadline::TIMEOUT_MESSAGE.to_string(),
			))
			.await
			.expect("accept interrupt");
		let result = tokio::time::timeout(Duration::from_secs(5), &mut first)
			.await
			.expect("the interrupt must stop the dialog");
		assert!(matches!(result, Err(AgentError::InterruptedReason(_))));
	}
	// Rows the host would reload on the abort boundary are already durable.
	let after_abort = session.load().expect("load after abort");
	assert!(
		after_abort
			.messages
			.iter()
			.any(|entry| entry.message.role == "user"
				&& entry.message.content.text().contains("please also note")),
		"the accepted steering must be durable before the interrupt surfaces"
	);

	let turns = agent
		.run_dialog("second", &mut state, None)
		.await
		.expect("the next turn proceeds on the same history");
	assert!(!turns.is_empty());
	{
		let seen = provider.seen.lock().unwrap();
		assert_eq!(seen.len(), 2, "the second request must carry the same history");
		assert!(
			seen[1]
				.iter()
				.any(|message| message.role == Role::User
					&& message.content.contains("please also note")),
			"the injected user message must precede the next request"
		);
	}
	let loaded = session.load().expect("load after the next turn");
	assert_eq!(
		loaded
			.messages
			.iter()
			.filter(|entry| entry.message.role == "assistant")
			.count(),
		2,
		"both assistant turns stay in the durable log"
	);
	std::fs::remove_dir_all(dir).ok();
}

/// A drain-boundary interrupt persists the accepted steering and returns
/// without any TurnEnd. The host rebuilds its next request from the durable
/// branch (that is what the CLI's error-path reload does); that request must
/// align with the seed and carry the injected user message.
#[tokio::test]
async fn drain_interrupt_survives_a_host_reload() {
	let dir = temp_dir("drain-reload");
	let db = dir.join("sessions.db");
	let session = Arc::new(DurableSession::create(db.clone(), header("s15", &dir)).expect("create"));
	let provider = Arc::new(RecordingProvider { seen: Mutex::new(Vec::new()) });
	let provider_dyn: Arc<dyn ModelProvider> = provider.clone();
	let agent = AgentLoop::new(config(&dir), provider_dyn, ToolRegistry::new())
		.with_durable(Some(Arc::clone(&session) as Arc<dyn DurableBoundary>));
	let mut state = AgentState::default();
	let (tx, rx) = tokio::sync::mpsc::channel(4);
	tx.send(SteeringEvent::InjectMessage(vec![ContentPart::text("steer note")]))
		.await
		.expect("queue injection");
	tx
		.send(SteeringEvent::InterruptWithReason(
			omp_cli::deadline::TIMEOUT_MESSAGE.to_string(),
		))
		.await
		.expect("queue interrupt");
	let result = agent.run_dialog("first", &mut state, Some(rx)).await;
	assert!(matches!(result, Err(AgentError::InterruptedReason(_))));
	assert!(
		provider.seen.lock().unwrap().is_empty(),
		"the drain aborts before any provider request"
	);
	drop(agent);
	drop(session);

	// The host's error path reloads and the next prompt replays the branch.
	let reopened = DurableSession::open(db, "s15", header("s15", &dir))
		.expect("reopen")
		.0;
	let loaded = reopened.load().expect("load persisted branch");
	let history = omp_cli::runtime::history_messages_from_entries(&loaded.messages)
		.expect("replay the reloaded branch");
	assert!(
		history
			.iter()
			.any(|message| message.role == Role::User && message.content.contains("steer note")),
		"the host reload must see the accepted steering row"
	);
	let provider_dyn: Arc<dyn ModelProvider> = provider.clone();
	let agent = AgentLoop::new(config(&dir), provider_dyn, ToolRegistry::new())
		.with_durable(Some(Arc::new(reopened) as Arc<dyn DurableBoundary>));
	let mut state = AgentState { messages: history, total_turns: 0 };
	let turns = agent
		.run_dialog("second", &mut state, None)
		.await
		.expect("the next turn must align with the durable branch");
	assert!(!turns.is_empty());
	{
		let seen = provider.seen.lock().unwrap();
		assert_eq!(seen.len(), 1, "exactly one provider request after the reload");
		assert!(
			seen[0]
				.iter()
				.any(|message| message.role == Role::User
					&& message.content.contains("steer note")),
			"the injected message must be in the next provider request"
		);
	}
	std::fs::remove_dir_all(dir).ok();
}

/// Start a dialog against a provider that never answers, interrupt it, and
/// return the store the loop journaled into.
async fn run_interrupted_stall(
	label: &str,
	id: &str,
	interrupt: SteeringEvent,
) -> (Arc<DurableSession>, PathBuf) {
	let dir = temp_dir(label);
	let db = dir.join("sessions.db");
	let session = Arc::new(DurableSession::create(db, header(id, &dir)).expect("create"));
	let started = Arc::new(tokio::sync::Notify::new());
	let provider = HangingProvider { started: Arc::clone(&started) };
	let agent = AgentLoop::new(config(&dir), Arc::new(provider), ToolRegistry::new())
		.with_durable(Some(Arc::clone(&session) as Arc<dyn DurableBoundary>));
	let mut state = AgentState::default();
	let (tx, rx) = tokio::sync::mpsc::channel(4);
	let run = tokio::spawn(async move { agent.run_dialog("stall", &mut state, Some(rx)).await });
	tokio::time::timeout(Duration::from_secs(5), started.notified())
		.await
		.expect("provider request must start");
	tx.send(interrupt).await.expect("send interrupt");
	let result = tokio::time::timeout(Duration::from_secs(5), run)
		.await
		.expect("the interrupt must stop the dialog")
		.expect("join dialog");
	assert!(matches!(result, Err(AgentError::Interrupted | AgentError::InterruptedReason(_))));
	(session, dir)
}

#[tokio::test]
async fn finish_execution_records_outcome_and_message_atomically() {
	let dir = temp_dir("atomic-finish");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-atomic", &dir)).expect("create");

	let call = ToolCall {
		id: "call-atomic-1".to_string(),
		name: "effect".to_string(),
		arguments: json!({}),
	};
	let execution_id = session.begin_execution(&call).await.expect("begin");

	let result = ToolResult {
		tool_call_id: call.id.clone(),
		content: "atomic output".to_string(),
		is_error: false,
		terminate: false,
		details: None,
	};
	session
		.finish_execution(&execution_id, &call, &result)
		.await
		.expect("finish");

	// Both outcome and message are durable.
	let loaded = session.load().expect("load");
	let tool_entry = loaded
		.messages
		.iter()
		.find(|m| m.message.role == "toolResult")
		.expect("toolResult message must be present");
	assert_eq!(tool_entry.message.content.text(), "atomic output");
	let details = tool_entry.message.extra.get("details").expect("details");
	assert_eq!(details.get("executionId").and_then(Value::as_str), Some(execution_id.as_str()));

	// No unresolved intents or unmessaged outcomes.
	assert!(session.unresolved_intents().expect("unresolved").is_empty());
	assert!(session.executions_needing_recovery().expect("needing recovery").is_empty());

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn reopened_session_recovers_legacy_outcome_without_message() {
	let dir = temp_dir("legacy-recover");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-legacy", &dir)).expect("create");

	// Persist an assistant message calling tool "call-leg-1".
	let asst_msg = Message::assistant_with_tools(
		"calling tool",
		vec![ToolCall {
			id: "call-leg-1".to_string(),
			name: "effect".to_string(),
			arguments: json!({}),
		}],
	);
	session
		.persist_messages(&[Message::user("hi"), asst_msg])
		.await
		.expect("persist assistant");
	drop(session);

	// Simulate legacy crash: open store directly, record intent and outcome, but no message.
	let execution_id;
	{
		let mut store = SqliteStore::open(&db).expect("open store");
		execution_id = store
			.record_intent(NewIntent {
				session_id: "s-legacy".to_string(),
				branch_id: "s-legacy.main".to_string(),
				provider_call_id: "call-leg-1".to_string(),
				tool_name: "effect".to_string(),
				payload: json!({}),
			})
			.expect("record intent");
		store
			.record_outcome(
				&execution_id,
				json!({ "content": "legacy output", "isError": false }),
			)
			.expect("record outcome");
		// Verify 1 execution needing recovery.
		assert_eq!(store.executions_needing_recovery("s-legacy").expect("needing").len(), 1);
	}

	// Reopen session with DurableSession::open.
	let (reopened, loaded) =
		DurableSession::open(db, "s-legacy", header("s-legacy", &dir)).expect("reopen");
	let loaded = loaded.expect("loaded session");

	// Verify full deserialization of recovered message:
	assert_eq!(loaded.messages.len(), 3);
	assert_eq!(loaded.branch_messages.len(), 3);

	let recovered_msg = loaded
		.branch_messages
		.iter()
		.find(|m| m.message.role == "toolResult")
		.expect("recovered toolResult message must be on branch");
	assert_eq!(recovered_msg.message.content.text(), "legacy output");
	assert_eq!(recovered_msg.message.tool_call_id.as_deref(), Some("call-leg-1"));
	assert_eq!(recovered_msg.message.tool_name.as_deref(), Some("effect"));
	assert_eq!(recovered_msg.message.is_error, Some(false));
	let details = recovered_msg.message.extra.get("details").expect("details");
	assert_eq!(details.get("executionId").and_then(Value::as_str), Some(execution_id.as_str()));
	assert_eq!(details.get("recovered").and_then(Value::as_bool), Some(true));

	// Replay / write boundary succeeds with the recovered turn!
	reopened
		.persist_messages(&[
			Message::user("hi"),
			Message::assistant_with_tools("calling tool", vec![ToolCall {
				id: "call-leg-1".to_string(),
				name: "effect".to_string(),
				arguments: json!({}),
			}]),
			Message::tool_with_status("call-leg-1", "effect", "legacy output", false),
			Message::assistant("next response"),
		])
		.await
		.expect("write boundary must succeed after recovery without dangling calls");

	// Reopening resolved it: no executions need recovery anymore.
	assert!(reopened.executions_needing_recovery().expect("needing").is_empty());
	assert!(reopened.unresolved_intents().expect("unresolved").is_empty());

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn finish_execution_is_idempotent_with_different_message_id() {
	let dir = temp_dir("atomic-idempotent");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-idem", &dir)).expect("create");

	let call = ToolCall {
		id: "call-idem-1".to_string(),
		name: "effect".to_string(),
		arguments: json!({}),
	};

	let asst = Message::assistant_with_tools("calling tool", vec![call.clone()]);
	session
		.persist_messages(&[Message::user("start"), asst.clone()])
		.await
		.expect("persist assistant");

	let execution_id = session.begin_execution(&call).await.expect("begin");

	let result = ToolResult {
		tool_call_id: call.id.clone(),
		content: "first output".to_string(),
		is_error: false,
		terminate: false,
		details: None,
	};

	// First finish:
	session
		.finish_execution(&execution_id, &call, &result)
		.await
		.expect("first finish");

	// Second finish on the live session WITHOUT intermediate load (intermediate load resets in-memory cursor):
	// to_tool_entry generates a fresh UUID for the message entry, but it must be an idempotent no-op!
	session
		.finish_execution(&execution_id, &call, &result)
		.await
		.expect("second finish must succeed idempotently on live cursor");

	// Сначала выполни следующую запись на живом курсоре (без промежуточного load!):
	session
		.persist_messages(&[
			Message::user("start"),
			asst,
			Message::tool_with_status("call-idem-1", "effect", "first output", false),
			Message::assistant("post-tool turn done"),
		])
		.await
		.expect("followup persist_messages on live hot cursor must succeed");

	// Затем проверяй цепочку:
	let loaded = session.load().expect("load");
	let tool_count = loaded
		.messages
		.iter()
		.filter(|m| m.message.role == "toolResult")
		.count();
	assert_eq!(tool_count, 1, "live duplicate finish must not append second toolResult message");
	assert_eq!(loaded.branch_messages.len(), 4, "User -> Asst -> ToolResult -> Asst");
	assert_eq!(loaded.branch_messages[0].message.role, "user");
	assert_eq!(loaded.branch_messages[1].message.role, "assistant");
	assert_eq!(loaded.branch_messages[2].message.role, "toolResult");
	assert_eq!(loaded.branch_messages[3].message.role, "assistant");
	assert_eq!(
		loaded.branch_messages[3].parent_id.as_deref(),
		Some(loaded.branch_messages[2].id.as_str()),
		"followup assistant must chain directly to the single tool result"
	);

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn reopened_session_skips_legacy_result_without_execution_id() {
	let dir = temp_dir("legacy-no-exec-id");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-leg-no-exec", &dir)).expect("create");

	let asst_msg = Message::assistant_with_tools(
		"calling legacy tool",
		vec![ToolCall {
			id: "call-leg-no-exec".to_string(),
			name: "effect".to_string(),
			arguments: json!({}),
		}],
	);
	session
		.persist_messages(&[Message::user("hi"), asst_msg])
		.await
		.expect("persist assistant");
	drop(session);

	// Simulate legacy session: toolResult is already in messages with matching toolCallId but NO details.executionId.
	// Executions row has terminal outcome.
	let execution_id;
	{
		let mut store = SqliteStore::open(&db).expect("open store");
		let branch = store.branch("s-leg-no-exec.main").expect("branch").expect("main branch");
		let asst_id = branch.leaf_message_id.expect("leaf");

		store
			.append_message(omp_cli::session_store::NewMessage {
				id: "msg-legacy-tool".to_string(),
				session_id: "s-leg-no-exec".to_string(),
				branch_id: "s-leg-no-exec.main".to_string(),
				parent_message_id: Some(asst_id),
				payload: json!({
					"type": "message",
					"id": "msg-legacy-tool",
					"parentId": "msg-u",
					"timestamp": "2026-11-07T00:00:00.000Z",
					"message": {
						"role": "toolResult",
						"toolCallId": "call-leg-no-exec",
						"toolName": "effect",
						"content": "legacy output",
						"isError": false,
						"timestamp": 123456789
					}
				}),
			})
			.expect("append legacy tool message");

		execution_id = store
			.record_intent(NewIntent {
				session_id: "s-leg-no-exec".to_string(),
				branch_id: "s-leg-no-exec.main".to_string(),
				provider_call_id: "call-leg-no-exec".to_string(),
				tool_name: "effect".to_string(),
				payload: json!({}),
			})
			.expect("record intent");
		store
			.record_outcome(
				&execution_id,
				json!({ "content": "legacy output", "isError": false }),
			)
			.expect("record outcome");

		// Crucial guarantee: execution is NOT marked as needing recovery because the legacy message exists!
		assert!(store.executions_needing_recovery("s-leg-no-exec").expect("needing").is_empty());
	}

	// Reopen session:
	let (reopened, loaded) =
		DurableSession::open(db, "s-leg-no-exec", header("s-leg-no-exec", &dir)).expect("reopen");
	let loaded = loaded.expect("loaded session");

	// Exactly 3 messages (User, Assistant, Legacy ToolResult). No duplicate recovered message appended!
	assert_eq!(loaded.branch_messages.len(), 3);
	assert!(reopened.executions_needing_recovery().expect("needing").is_empty());

	// Replay / next turn write succeeds cleanly:
	reopened
		.persist_messages(&[
			Message::user("hi"),
			Message::assistant_with_tools("calling legacy tool", vec![ToolCall {
				id: "call-leg-no-exec".to_string(),
				name: "effect".to_string(),
				arguments: json!({}),
			}]),
			Message::tool_with_status("call-leg-no-exec", "effect", "legacy output", false),
			Message::assistant("next turn response"),
		])
		.await
		.expect("next turn must align cleanly with legacy message");

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn reopened_session_recovers_missing_sibling_tool_result_when_first_already_persisted() {
	let dir = temp_dir("sibling-leaf-recover");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-sibling", &dir)).expect("create");

	// Assistant calls two tools: call-A and call-B in batch.
	let asst_msg = Message::assistant_with_tools(
		"calling batch",
		vec![
			ToolCall {
				id: "call-A".to_string(),
				name: "effect".to_string(),
				arguments: json!({}),
			},
			ToolCall {
				id: "call-B".to_string(),
				name: "effect".to_string(),
				arguments: json!({}),
			},
		],
	);
	session
		.persist_messages(&[Message::user("run batch"), asst_msg])
		.await
		.expect("persist assistant");
	drop(session);

	let exec_b_id;
	{
		let mut store = SqliteStore::open(&db).expect("open store");
		let branch = store.branch("s-sibling.main").expect("branch").expect("main branch");
		let asst_id = branch.leaf_message_id.expect("leaf");

		let exec_a = store
			.record_intent(NewIntent {
				session_id: "s-sibling".to_string(),
				branch_id: "s-sibling.main".to_string(),
				provider_call_id: "call-A".to_string(),
				tool_name: "effect".to_string(),
				payload: json!({}),
			})
			.expect("intent A");

		exec_b_id = store
			.record_intent(NewIntent {
				session_id: "s-sibling".to_string(),
				branch_id: "s-sibling.main".to_string(),
				provider_call_id: "call-B".to_string(),
				tool_name: "effect".to_string(),
				payload: json!({}),
			})
			.expect("intent B");

		// Tool A finished and its message was appended:
		store
			.record_outcome_with_message(
				&exec_a,
				json!({ "content": "output A", "isError": false }),
				omp_cli::session_store::NewMessage {
					id: "msg-tool-A".to_string(),
					session_id: "s-sibling".to_string(),
					branch_id: "s-sibling.main".to_string(),
					parent_message_id: Some(asst_id),
					payload: json!({
						"type": "message",
						"id": "msg-tool-A",
						"parentId": asst_id,
						"timestamp": "2026-11-07T00:00:00.000Z",
						"message": {
							"role": "toolResult",
							"toolCallId": "call-A",
							"toolName": "effect",
							"content": "output A",
							"isError": false,
							"timestamp": 1000,
							"details": { "executionId": exec_a }
						}
					}),
				},
			)
			.expect("record outcome A with message");

		// Tool B recorded outcome, but crash happened before its message was appended:
		store
			.record_outcome(
				&exec_b_id,
				json!({ "content": "output B", "isError": false }),
			)
			.expect("record outcome B");

		// Only B needs recovery:
		let needing = store.executions_needing_recovery("s-sibling").expect("needing");
		assert_eq!(needing.len(), 1);
		assert_eq!(needing[0].execution_id, exec_b_id);
	}

	// Reopen session:
	let (reopened, loaded) =
		DurableSession::open(db, "s-sibling", header("s-sibling", &dir)).expect("reopen");
	let loaded = loaded.expect("loaded session");

	// Exactly 4 messages on branch: User, Assistant, ToolResult A, ToolResult B (recovered)!
	assert_eq!(loaded.branch_messages.len(), 4);
	assert_eq!(loaded.branch_messages[2].id, "msg-tool-A");
	assert_eq!(loaded.branch_messages[2].message.content.text(), "output A");

	let recovered_b = &loaded.branch_messages[3];
	assert_eq!(recovered_b.message.role, "toolResult");
	assert_eq!(recovered_b.message.tool_call_id.as_deref(), Some("call-B"));
	assert_eq!(recovered_b.message.content.text(), "output B");
	assert_eq!(recovered_b.parent_id.as_deref(), Some("msg-tool-A"), "recovered B must chain to sibling leaf A");
	let details_b = recovered_b.message.extra.get("details").expect("details");
	assert_eq!(details_b.get("executionId").and_then(Value::as_str), Some(exec_b_id.as_str()));
	assert_eq!(details_b.get("recovered").and_then(Value::as_bool), Some(true));

	// Reopening resolved B:
	assert!(reopened.executions_needing_recovery().expect("needing").is_empty());

	// Next turn write boundary succeeds:
	reopened
		.persist_messages(&[
			Message::user("run batch"),
			Message::assistant_with_tools("calling batch", vec![
				ToolCall { id: "call-A".to_string(), name: "effect".to_string(), arguments: json!({}) },
				ToolCall { id: "call-B".to_string(), name: "effect".to_string(), arguments: json!({}) },
			]),
			Message::tool_with_status("call-A", "effect", "output A", false),
			Message::tool_with_status("call-B", "effect", "output B", false),
			Message::assistant("batch done"),
		])
		.await
		.expect("next turn must align cleanly with recovered sibling B");

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn reopened_session_recovers_multiple_batch_tool_results_across_sibling_leaves() {
	let dir = temp_dir("multi-recover");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-multi", &dir)).expect("create");

	let asst_msg = Message::assistant_with_tools(
		"calling batch multi",
		vec![
			ToolCall { id: "call-M1".to_string(), name: "effect".to_string(), arguments: json!({}) },
			ToolCall { id: "call-M2".to_string(), name: "effect".to_string(), arguments: json!({}) },
		],
	);
	session
		.persist_messages(&[Message::user("run multi"), asst_msg])
		.await
		.expect("persist assistant");
	drop(session);

	let (exec_1, exec_2);
	{
		let mut store = SqliteStore::open(&db).expect("open store");
		exec_1 = store
			.record_intent(NewIntent {
				session_id: "s-multi".to_string(),
				branch_id: "s-multi.main".to_string(),
				provider_call_id: "call-M1".to_string(),
				tool_name: "effect".to_string(),
				payload: json!({}),
			})
			.expect("intent 1");
		exec_2 = store
			.record_intent(NewIntent {
				session_id: "s-multi".to_string(),
				branch_id: "s-multi.main".to_string(),
				provider_call_id: "call-M2".to_string(),
				tool_name: "effect".to_string(),
				payload: json!({}),
			})
			.expect("intent 2");

		// Both recorded outcomes in SQLite, neither appended message before crash:
		store
			.record_outcome(&exec_1, json!({ "content": "out 1", "isError": false }))
			.expect("outcome 1");
		store
			.record_outcome(&exec_2, json!({ "content": "out 2", "isError": false }))
			.expect("outcome 2");

		assert_eq!(store.executions_needing_recovery("s-multi").expect("needing").len(), 2);
	}

	// Reopen session:
	let (reopened, loaded) =
		DurableSession::open(db, "s-multi", header("s-multi", &dir)).expect("reopen");
	let loaded = loaded.expect("loaded session");

	// Both tool results recovered in chain!
	assert_eq!(loaded.branch_messages.len(), 4);
	let rec_1 = &loaded.branch_messages[2];
	let rec_2 = &loaded.branch_messages[3];

	assert_eq!(rec_1.message.tool_call_id.as_deref(), Some("call-M1"));
	assert_eq!(rec_1.message.content.text(), "out 1");

	assert_eq!(rec_2.message.tool_call_id.as_deref(), Some("call-M2"));
	assert_eq!(rec_2.message.content.text(), "out 2");
	assert_eq!(rec_2.parent_id.as_deref(), Some(rec_1.id.as_str()), "M2 must chain to sibling M1");

	assert!(reopened.executions_needing_recovery().expect("needing").is_empty());

	// Next turn write boundary succeeds:
	reopened
		.persist_messages(&[
			Message::user("run multi"),
			Message::assistant_with_tools("calling batch multi", vec![
				ToolCall { id: "call-M1".to_string(), name: "effect".to_string(), arguments: json!({}) },
				ToolCall { id: "call-M2".to_string(), name: "effect".to_string(), arguments: json!({}) },
			]),
			Message::tool_with_status("call-M1", "effect", "out 1", false),
			Message::tool_with_status("call-M2", "effect", "out 2", false),
			Message::assistant("multi done"),
		])
		.await
		.expect("next turn must align cleanly with both recovered siblings");

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn finish_execution_validates_call_id_linkage() {
	let dir = temp_dir("call-id-link");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-link", &dir)).expect("create");

	let call = ToolCall {
		id: "call-correct".to_string(),
		name: "effect".to_string(),
		arguments: json!({}),
	};
	let execution_id = session.begin_execution(&call).await.expect("begin");

	// Finish with mismatched provider call ID
	let wrong_call = ToolCall {
		id: "call-WRONG".to_string(),
		name: "effect".to_string(),
		arguments: json!({}),
	};
	let result = ToolResult {
		tool_call_id: "call-WRONG".to_string(),
		content: "out".to_string(),
		is_error: false,
		terminate: false,
		details: None,
	};
	let err = session
		.finish_execution(&execution_id, &wrong_call, &result)
		.await
		.expect_err("mismatched call ID must fail validation");
	assert!(err.to_string().contains("call id"));

	// Neither outcome nor message committed
	assert_eq!(session.execution(&execution_id).expect("exec").unwrap().outcome, None);
	assert_eq!(session.unresolved_intents().expect("unresolved").len(), 1);

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn reopen_with_unresolved_intent_refuses_dangling_replay_fail_closed() {
	let dir = temp_dir("fail-closed");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-fail", &dir)).expect("create");

	// Persist an assistant message with a tool call.
	let asst_msg = Message::assistant_with_tools(
		"calling tool",
		vec![ToolCall {
			id: "call-dangling".to_string(),
			name: "effect".to_string(),
			arguments: json!({}),
		}],
	);
	session
		.persist_messages(&[Message::user("hi"), asst_msg])
		.await
		.expect("persist assistant");

	// Record intent, but NO outcome (simulating crash during effect).
	let call = ToolCall {
		id: "call-dangling".to_string(),
		name: "effect".to_string(),
		arguments: json!({}),
	};
	let execution_id = session.begin_execution(&call).await.expect("begin");
	drop(session);

	// Reopen session.
	let (reopened, loaded) =
		DurableSession::open(db, "s-fail", header("s-fail", &dir)).expect("reopen");
	assert!(loaded.is_some());

	// Intent remains Unknown in unresolved_intents.
	let unresolved = reopened.unresolved_intents().expect("unresolved");
	assert_eq!(unresolved.len(), 1);
	assert_eq!(unresolved[0].execution_id, execution_id);

	// Reading stays available: load succeeds and returns the raw branch messages.
	let loaded_again = reopened.load().expect("raw read of branch must stay available");
	assert_eq!(loaded_again.branch_messages.len(), 2);

	// But write boundary fails closed on dangling unresolvable tool call!
	let err = reopened
		.persist_messages(&[Message::user("next turn")])
		.await
		.expect_err("write boundary must fail closed on dangling call");
	assert!(err.to_string().contains("dangling tool call call-dangling"));

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn multiple_outcomes_keep_distinct_execution_ids_and_call_ids() {
	let dir = temp_dir("multi-exec-ids");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-multi-ids", &dir)).expect("create");

	let call1 = ToolCall {
		id: "call-1".to_string(),
		name: "effect".to_string(),
		arguments: json!({"n": 1}),
	};
	let call2 = ToolCall {
		id: "call-2".to_string(),
		name: "effect".to_string(),
		arguments: json!({"n": 2}),
	};

	let asst = Message::assistant_with_tools("calling two", vec![call1.clone(), call2.clone()]);
	session
		.persist_messages(&[Message::user("go"), asst.clone()])
		.await
		.expect("persist asst");

	let exec1 = session.begin_execution(&call1).await.expect("begin 1");
	let exec2 = session.begin_execution(&call2).await.expect("begin 2");
	assert_ne!(exec1, exec2, "execution IDs must be distinct");

	let res1 = ToolResult {
		tool_call_id: "call-1".to_string(),
		content: "result 1".to_string(),
		is_error: false,
		terminate: false,
		details: None,
	};
	let res2 = ToolResult {
		tool_call_id: "call-2".to_string(),
		content: "result 2".to_string(),
		is_error: false,
		terminate: false,
		details: None,
	};

	// Cross-finishing exec1 with call2 must be rejected by validation
	let bad_finish = session.finish_execution(&exec1, &call2, &res2).await;
	assert!(bad_finish.is_err(), "cross-finishing exec1 with call2 must be rejected");

	// Finish exec1 with call1
	session.finish_execution(&exec1, &call1, &res1).await.expect("finish 1");
	// Finish exec2 with call2
	session.finish_execution(&exec2, &call2, &res2).await.expect("finish 2");

	// Perform next write on live hot cursor:
	session
		.persist_messages(&[
			Message::user("go"),
			asst,
			Message::tool_with_status("call-1", "effect", "result 1", false),
			Message::tool_with_status("call-2", "effect", "result 2", false),
			Message::assistant("both done"),
		])
		.await
		.expect("persist batch on hot cursor");

	// Verify chain and executionId mapping
	let loaded = session.load().expect("load");
	let tool_entries: Vec<_> = loaded
		.messages
		.iter()
		.filter(|m| m.message.role == "toolResult")
		.collect();
	assert_eq!(tool_entries.len(), 2);

	let entry1 = tool_entries.iter().find(|m| m.message.tool_call_id.as_deref() == Some("call-1")).expect("tool 1");
	let entry2 = tool_entries.iter().find(|m| m.message.tool_call_id.as_deref() == Some("call-2")).expect("tool 2");

	let eid1 = entry1.message.extra.get("details").and_then(|d| d.get("executionId")).and_then(Value::as_str);
	let eid2 = entry2.message.extra.get("details").and_then(|d| d.get("executionId")).and_then(Value::as_str);

	assert_eq!(eid1, Some(exec1.as_str()));
	assert_eq!(eid2, Some(exec2.as_str()));
	assert_ne!(eid1, eid2);

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn reopened_session_with_compaction_recovers_unmessaged_outcome_and_preserves_context() {
	let dir = temp_dir("compact-crash-recover");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-comp", &dir)).expect("create");

	// 1. Initial turns before compaction
	let original = vec![Message::user("first turn"), Message::assistant("first answer")];
	session.persist_messages(&original).await.expect("persist original");

	// 2. Compaction: retained tail is assistant("first answer") with a real system notice
	let notice = Message::system(format!(
		"{}1 earlier turn(s) were pruned to fit the context limit]",
		pi_agent::compaction::COMPACTION_NOTICE_PREFIX
	));
	let compacted = vec![notice.clone(), Message::assistant("first answer")];
	let report = pi_agent::CompactionReport { old_len: 2, kept_from: 1, kept_at: 1 };
	session.compacted(&compacted, &report).await.expect("compacted");
	session.persist_messages(&compacted).await.expect("persist compacted");
	// 3. New assistant turn with a tool call after compaction
	let call = ToolCall {
		id: "call-compact-1".to_string(),
		name: "effect".to_string(),
		arguments: json!({}),
	};
	let asst_tool = Message::assistant_with_tools("calling tool after compaction", vec![call.clone()]);
	let conversation_with_tool = vec![
		notice.clone(),
		Message::assistant("first answer"),
		Message::user("trigger tool"),
		asst_tool.clone(),
	];
	session.persist_messages(&conversation_with_tool).await.expect("persist conversation with tool");

	// 4. Begin execution
	let execution_id = session.begin_execution(&call).await.expect("begin");
	drop(session);

	// 5. Simulate pre-atomic / legacy crash: outcome is recorded in SQLite executions, but no toolResult message on branch
	{
		let mut store = SqliteStore::open(&db).expect("open store");
		store
			.record_outcome(
				&execution_id,
				json!({ "content": "compact effect output", "isError": false }),
			)
			.expect("record outcome");
		assert_eq!(store.executions_needing_recovery("s-comp").expect("needing").len(), 1);
	}

	// 6. Reopen session: load_parts runs recover_unmessaged_outcomes, which inserts message AND updates durableProjection.kept atomically!
	let (reopened, loaded) =
		DurableSession::open(db, "s-comp", header("s-comp", &dir)).expect("reopen");
	let loaded = loaded.expect("loaded session");

	// Verify recovered message was inserted
	let recovered_msg = loaded
		.branch_messages
		.iter()
		.find(|m| m.message.role == "toolResult")
		.expect("recovered toolResult message must be present");
	assert_eq!(recovered_msg.message.content.text(), "compact effect output");
	let rec_id = recovered_msg.id.clone();

	// Verify durableProjection.kept was ATOMICALLY UPDATED to include the recovered message ID
	let projection = loaded.projection.expect("session must have durableProjection");
	assert!(
		projection.kept.contains(&rec_id),
		"durableProjection.kept must contain the recovered message ID {rec_id}, got: {:?}",
		projection.kept
	);

	// 7. Replay / next turn write boundary on reopened session must succeed without losing context!
	let next_turn = vec![
		notice,
		Message::assistant("first answer"),
		Message::user("trigger tool"),
		asst_tool,
		Message::tool_with_status("call-compact-1", "effect", "compact effect output", false),
		Message::assistant("final answer after tool"),
	];
	reopened
		.persist_messages(&next_turn)
		.await
		.expect("write boundary after compaction recovery must align and succeed");

	let final_loaded = reopened.load().expect("load final");
	let final_proj = final_loaded.projection.expect("final projection");
	assert!(final_proj.kept.contains(&rec_id));

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn reopened_session_with_compaction_backfills_unprojected_branch_suffix() {
	let dir = temp_dir("compact-backfill");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-backfill", &dir)).expect("create");

	let notice = Message::system(format!(
		"{}1 earlier turn(s) were pruned to fit the context limit]",
		pi_agent::compaction::COMPACTION_NOTICE_PREFIX
	));
	let original = vec![Message::user("first turn"), Message::assistant("first answer")];
	session.persist_messages(&original).await.expect("persist original");

	let compacted = vec![notice.clone(), Message::assistant("first answer")];
	let report = pi_agent::CompactionReport { old_len: 2, kept_from: 1, kept_at: 1 };
	session.compacted(&compacted, &report).await.expect("compacted");
	session.persist_messages(&compacted).await.expect("persist compacted");

	let call = ToolCall {
		id: "call-backfill-1".to_string(),
		name: "effect".to_string(),
		arguments: json!({}),
	};
	let asst_tool = Message::assistant_with_tools("calling tool", vec![call.clone()]);
	session
		.persist_messages(&[
			notice.clone(),
			Message::assistant("first answer"),
			Message::user("do tool"),
			asst_tool.clone(),
		])
		.await
		.expect("persist asst with tool");

	let execution_id = session.begin_execution(&call).await.expect("begin");
	let result = ToolResult {
		tool_call_id: call.id.clone(),
		content: "backfill tool output".to_string(),
		is_error: false,
		terminate: false,
		details: None,
	};
	session
		.finish_execution(&execution_id, &call, &result)
		.await
		.expect("finish");
	drop(session);

	// Simulate legacy crash state: tool result message is in SQLite `messages`,
	// but its ID is missing from `durableProjection.kept`.
	let tool_msg_id;
	{
		let mut store = SqliteStore::open(&db).expect("open store");
		let branch = store.branch("s-backfill.main").expect("branch").expect("main branch");
		tool_msg_id = branch.leaf_message_id.expect("toolResult leaf");

		// Strip tool_msg_id from durableProjection.kept in sessions.payload_json:
		let mut payload = store.session("s-backfill").expect("session").expect("s-backfill").payload;
		let kept_arr = payload
			.get_mut("durableProjection")
			.and_then(|p| p.get_mut("kept"))
			.and_then(Value::as_array_mut)
			.expect("kept array");
		kept_arr.retain(|id| id.as_str() != Some(&tool_msg_id));
		store.update_session_payload("s-backfill", payload).expect("update payload");

		// Verify it is missing:
		let payload_check = store.session("s-backfill").expect("session").expect("s-backfill").payload;
		let kept_check = payload_check["durableProjection"]["kept"].as_array().expect("kept");
		assert!(!kept_check.iter().any(|v| v.as_str() == Some(&tool_msg_id)));
	}

	// Reopen session: load_parts calls reconcile_durable_projection which safely backfills the suffix!
	let (reopened, loaded) =
		DurableSession::open(db, "s-backfill", header("s-backfill", &dir)).expect("reopen");
	let loaded = loaded.expect("loaded session");

	let projection = loaded.projection.expect("projection");
	assert!(
		projection.kept.contains(&tool_msg_id),
		"reconcile_durable_projection must backfill tool_msg_id into projection.kept"
	);

	// Next turn write boundary succeeds without dropping the tool result from context:
	let next_turn = vec![
		notice,
		Message::assistant("first answer"),
		Message::user("do tool"),
		asst_tool,
		Message::tool_with_status("call-backfill-1", "effect", "backfill tool output", false),
		Message::assistant("final response"),
	];
	reopened
		.persist_messages(&next_turn)
		.await
		.expect("next turn must align cleanly with reconciled projection suffix");

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn reopened_session_with_diverged_projection_discards_stale_projection_and_resumes() {
	let dir = temp_dir("compact-corrupt-branch");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-corrupt", &dir)).expect("create");

	let original = vec![Message::user("first"), Message::assistant("first answer")];
	session.persist_messages(&original).await.expect("persist");
	drop(session);

	// Invalidate durableProjection.kept with an ID completely absent from the active branch (legacy branch cut crash state):
	{
		let mut store = SqliteStore::open(&db).expect("open store");
		let mut payload = store.session("s-corrupt").expect("session").expect("s-corrupt").payload;
		payload.as_object_mut().unwrap().insert(
			"durableProjection".to_string(),
			json!({
				"kept": ["msg-nonexistent-unrelated-branch"],
				"synthetic": []
			}),
		);
		store.update_session_payload("s-corrupt", payload).expect("update payload");
	}

	// 1. Safe readability: opening the session succeeds, stale projection is discarded!
	let (reopened, loaded) = DurableSession::open(db, "s-corrupt", header("s-corrupt", &dir))
		.expect("reopen must succeed for safe readability even with diverged projection");
	let loaded = loaded.expect("loaded session");
	assert_eq!(loaded.branch_messages.len(), 2, "history messages must be fully preserved and readable");
	assert_eq!(loaded.branch_messages[0].message.role, "user");
	assert_eq!(loaded.branch_messages[1].message.role, "assistant");
	assert!(loaded.projection.is_none(), "stale diverged projection must be discarded, not exposed to reader");

	let load_again = reopened.load().expect("load must succeed for reading");
	assert_eq!(load_again.branch_messages.len(), 2);
	assert!(load_again.projection.is_none());

	// 2. Writing resumes against the real active branch without duplicating rows:
	reopened
		.persist_messages(&[Message::user("first"), Message::assistant("first answer"), Message::user("new turn")])
		.await
		.expect("write must succeed by aligning against the real branch");
	let final_loaded = reopened.load().expect("read final");
	assert_eq!(final_loaded.branch_messages.len(), 3, "must append exactly 1 new turn, not duplicate history");
	assert_eq!(final_loaded.branch_messages[2].message.content.text(), "new turn");

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn branch_cut_via_append_entry_clears_projection_in_sqlite_and_in_memory() {
	let dir = temp_dir("branch-cut-proj");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-cut", &dir)).expect("create");

	let notice = Message::system(format!(
		"{}1 earlier turn(s) were pruned to fit the context limit]",
		pi_agent::compaction::COMPACTION_NOTICE_PREFIX
	));
	let compacted = vec![notice, Message::assistant("first answer")];
	let report = pi_agent::CompactionReport { old_len: 2, kept_from: 1, kept_at: 1 };
	session.compacted(&compacted, &report).await.expect("compacted");
	session.persist_messages(&compacted).await.expect("persist compacted");

	let loaded_before = session.load().expect("load");
	assert!(loaded_before.projection.is_some());
	let parent_id = loaded_before.leaf.clone();

	// Perform branch cut via append_entry with valid existing parent
	let cut_entry = SessionEntry {
		kind: "branch_summary".to_string(),
		id: "msg-cut-1".to_string(),
		parent_id,
		timestamp: output::iso8601(std::time::SystemTime::now()),
		extra: json!({}),
	};
	session.append_entry(cut_entry).expect("append branch cut");

	// Both in SQLite and in-memory, projection is None:
	let loaded = session.load().expect("load after cut");
	assert!(loaded.projection.is_none(), "projection must be None after branch cut");

	{
		let store = SqliteStore::open(&db).expect("open store");
		let payload = store.session("s-cut").expect("session").expect("s-cut").payload;
		assert!(payload.get("durableProjection").is_none(), "SQLite payload must not contain durableProjection");
	}

	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn clear_projection_clears_both_sqlite_and_in_memory_state() {
	let dir = temp_dir("clear-proj");
	let db = dir.join("sessions.db");
	let session = DurableSession::create(db.clone(), header("s-clear", &dir)).expect("create");

	let notice = Message::system(format!(
		"{}1 earlier turn(s) were pruned to fit the context limit]",
		pi_agent::compaction::COMPACTION_NOTICE_PREFIX
	));
	let compacted = vec![notice, Message::assistant("first answer")];
	let report = pi_agent::CompactionReport { old_len: 2, kept_from: 1, kept_at: 1 };
	session.compacted(&compacted, &report).await.expect("compacted");
	session.persist_messages(&compacted).await.expect("persist compacted");

	assert!(session.load().expect("load").projection.is_some());

	session.clear_projection().expect("clear_projection");

	// Verified: both in memory and SQLite cleared
	assert!(session.load().expect("load after clear").projection.is_none());

	{
		let store = SqliteStore::open(&db).expect("open store");
		let payload = store.session("s-clear").expect("session").expect("s-clear").payload;
		assert!(payload.get("durableProjection").is_none());
	}

	std::fs::remove_dir_all(dir).ok();
}
