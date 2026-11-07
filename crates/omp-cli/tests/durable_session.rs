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
	let provider_dyn: Arc<dyn ModelProvider> = Arc::clone(&provider);
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
			.expect("the interrupt must stop the dialog")
			.expect("join dialog");
		assert!(matches!(result, Err(AgentError::InterruptedReason(_))));
	}
	// Rows the host would reload on the abort boundary are already durable.
	let after_abort = session.load().expect("load after abort");
	assert!(
		after_abort
			.messages
			.iter()
			.any(|entry| entry.message.role == "user"
				&& entry.message.content.contains("please also note")),
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
	let provider_dyn: Arc<dyn ModelProvider> = Arc::clone(&provider);
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
	let provider_dyn: Arc<dyn ModelProvider> = Arc::clone(&provider);
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
