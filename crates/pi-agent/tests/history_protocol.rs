//! Consumer-level `history://` regressions: index, live/parked/tombstoned/disk
//! readers, caller isolation, exact errors, cancellation, and read-tool
//! behavior. Fixtures live under `$HOME`, never a shared system temp dir.

use std::{
	collections::{BTreeMap, HashSet},
	fs,
	io::Write,
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::stream;
use pi_agent::{
	CancellationToken,
	loop_module::AgentEvent,
	provider::{ChunkStream, ModelProvider, ProviderError, StreamChunk, ToolCallDelta, ToolChoice},
	state::{AgentContext, ContentPart, Message, ToolCall, TurnStatus},
	subagent_lifecycle::{
		attach, DeliveryOutcome, LifecycleStatus, SubagentLifecycleManager,
	},
	supervisor::{ChildSpawn, DelegationLimits, Supervisor},
	task_persisted_revive::{
		append_messages, DefaultPersistedReviverFactory, PersistedChildSession, transcript_path,
	},
	tools::{
		hub_park,
		internal_urls::{InternalUrlRouter, UrlScope},
		registry::ToolRegistry,
		read::ReadTool,
		task::{TaskTool, TaskToolDetails},
		Tool,
	},
};
use serde_json::json;
use tokio::sync::{Notify, Semaphore};

struct TempRoot {
	path: PathBuf,
}

impl TempRoot {
	fn new(label: &str) -> Self {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map(|duration| duration.as_nanos())
			.unwrap_or_default();
		let home = std::env::var_os("HOME").expect("HOME must be set for history tests");
		let path = PathBuf::from(home).join(format!(
			".omp-history-{label}-{}-{nanos}",
			std::process::id()
		));
		fs::create_dir_all(path.join("session").join("subagents")).expect("fixture root");
		Self { path }
	}

	fn session_file(&self) -> PathBuf {
		self.path.join("session.jsonl")
	}

	fn artifacts(&self) -> PathBuf {
		self.path.join("session")
	}

	fn subagents(&self) -> PathBuf {
		self.artifacts().join("subagents")
	}

	fn scope(&self) -> UrlScope {
		UrlScope {
			session_file: Some(self.session_file()),
			local_roots:  BTreeMap::from([("local".to_string(), self.artifacts())]),
		}
	}
}

impl Drop for TempRoot {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.path);
	}
}

struct NoopProvider;

#[async_trait]
impl ModelProvider for NoopProvider {
	async fn complete(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		Err(ProviderError::Protocol(
			"history tests never revive a child".to_string(),
		))
	}

	async fn stream(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		Err(ProviderError::Protocol(
			"history tests never revive a child".to_string(),
		))
	}
}

fn text_chunk(content: String) -> StreamChunk {
	StreamChunk {
		content: Some(content),
		reasoning: None,
		tool_call_deltas: Vec::new(),
		usage: None,
		is_done: false,
		content_index: None,
		signature: None,
		redacted: None,
		stop_reason: None,
		error_message: None,
		disabled_features: None,
	}
}

fn done_chunk() -> StreamChunk {
	StreamChunk {
		content: None,
		reasoning: None,
		tool_call_deltas: Vec::new(),
		usage: None,
		is_done: true,
		content_index: None,
		signature: None,
		redacted: None,
		stop_reason: None,
		error_message: None,
		disabled_features: None,
	}
}

/// Provider that can block a streamed request until the test releases it, so
/// a history read can observe the live snapshot before the transcript flush.
///
/// `scripted` emits one assistant turn with `reply` plus a harmless `bash true`
/// tool call before gating the follow-up request. That keeps the dialog open and
/// makes the live marker absent from the journal (which only flushes at settle).
struct GatedProvider {
	reply:        String,
	entered:      Arc<Notify>,
	gate:         Arc<Semaphore>,
	scripted:     bool,
	stream_calls: AtomicUsize,
	/// Shell command the scripted first turn calls; the test uses it to hold the
	/// loop inside a tool (no turn boundary, so nothing is journaled yet).
	command:      String,
}

#[async_trait]
impl ModelProvider for GatedProvider {
	async fn complete(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		self.entered.notify_one();
		let _permit = self
			.gate
			.acquire()
			.await
			.map_err(|_| ProviderError::Protocol("gate closed".to_string()))?;
		Ok(Message::assistant(self.reply.clone()))
	}

	async fn stream(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		if self.scripted && self.stream_calls.fetch_add(1, Ordering::SeqCst) == 0 {
			let delta = ToolCallDelta {
				index:            0,
				id:               Some("call-live".to_string()),
				name:             Some("bash".to_string()),
				arguments_delta:  Some(
					serde_json::json!({ "command": self.command }).to_string(),
				),
				content_index:    None,
			};
			let reply = self.reply.clone();
			return Ok(Box::pin(stream::iter(vec![
				Ok(StreamChunk {
					content: Some(reply),
					reasoning: None,
					tool_call_deltas: vec![delta],
					usage: None,
					is_done: false,
					content_index: None,
					signature: None,
					redacted: None,
					stop_reason: None,
					error_message: None,
					disabled_features: None,
				}),
				Ok(done_chunk()),
			])));
		}
		self.entered.notify_one();
		let _permit = self
			.gate
			.acquire()
			.await
			.map_err(|_| ProviderError::Protocol("gate closed".to_string()))?;
		let reply = self.reply.clone();
		Ok(Box::pin(stream::iter(vec![
			Ok(text_chunk(reply)),
			Ok(done_chunk()),
		])))
	}
}

fn gated_provider(reply: &str) -> Arc<GatedProvider> {
	gated_provider_with_mode(reply, false)
}

fn gated_provider_with_mode(reply: &str, scripted: bool) -> Arc<GatedProvider> {
	Arc::new(GatedProvider {
		reply:        reply.to_string(),
		entered:      Arc::new(Notify::new()),
		gate:         Arc::new(Semaphore::new(0)),
		scripted,
		stream_calls: AtomicUsize::new(0),
		command:      "true".to_string(),
	})
}

/// Scripted provider whose first turn calls `command` instead of `true`, so a
/// test can hold the loop inside the tool call (no turn boundary, no journal
/// flush) while the live snapshot already has the answer.
fn gated_scripted_provider_with_command(reply: &str, command: &str) -> Arc<GatedProvider> {
	Arc::new(GatedProvider {
		reply:        reply.to_string(),
		entered:      Arc::new(Notify::new()),
		gate:         Arc::new(Semaphore::new(0)),
		scripted:     true,
		stream_calls: AtomicUsize::new(0),
		command:      command.to_string(),
	})
}

fn gated_manager(supervisor: &Arc<Supervisor>, provider: Arc<GatedProvider>) -> Arc<SubagentLifecycleManager> {
	let provider: Arc<dyn ModelProvider> = provider;
	attach(
		supervisor,
		0,
		Arc::new(DefaultPersistedReviverFactory::new(
			provider,
			ToolRegistry::with_builtins(),
		)),
	)
}

fn supervisor(root: &TempRoot) -> Arc<Supervisor> {
	let provider: Arc<dyn ModelProvider> = Arc::new(NoopProvider);
	let supervisor = Supervisor::new(
		ToolRegistry::with_builtins(),
		provider,
		None,
		DelegationLimits::default(),
	);
	supervisor.set_child_session_root(Some(root.subagents()));
	supervisor
}

fn supervisor_with(
	root: &TempRoot,
	provider: Arc<GatedProvider>,
) -> Arc<Supervisor> {
	let provider: Arc<dyn ModelProvider> = provider;
	let supervisor = Supervisor::new(
		ToolRegistry::with_builtins(),
		provider,
		None,
		DelegationLimits::default(),
	);
	supervisor.set_child_session_root(Some(root.subagents()));
	supervisor
}

fn manager(supervisor: &Arc<Supervisor>) -> Arc<SubagentLifecycleManager> {
	let provider: Arc<dyn ModelProvider> = Arc::new(NoopProvider);
	attach(
		supervisor,
		0,
		Arc::new(DefaultPersistedReviverFactory::new(
			provider,
			ToolRegistry::with_builtins(),
		)),
	)
}

fn reserve(supervisor: &Arc<Supervisor>, name: &str) -> String {
	let (id, _token) = supervisor
		.reserve_child_full(ChildSpawn {
			name,
			index: 1,
			agent: "scout",
			agent_source: "builtin",
			task: "investigate",
			parent_id: None,
			parent_tool_call_id: None,
			detached: false,
			output_prefix: None,
		})
		.expect("reserve child");
	id
}

fn append(supervisor: &Arc<Supervisor>, id: &str, messages: &[Message]) {
	let refs: Vec<&Message> = messages.iter().collect();
	supervisor.append_child_transcript(id, &refs);
}

fn contract(id: &str, root: &Path) -> PersistedChildSession {
	PersistedChildSession {
		id: id.to_string(),
		agent: Some("scout".to_string()),
		task: "investigate".to_string(),
		system_prompt: "child system prompt".to_string(),
		tools: vec!["read".to_string()],
		model_role: None,
		resolved_model: None,
		output_schema: None,
		output_schema_mode: None,
		restrict_tool_names: Some(true),
		spawns: Some("*".to_string()),
		read_summarize: None,
		advisor: None,
		read_only: None,
		cwd: root.to_path_buf(),
		depth: 1,
		max_turns: 20,
		context_limit: 100_000,
		display_name: Some(id.to_string()),
		parent_id: None,
		output_id: None,
		output_published: false,
	}
}

fn router(manager: &Arc<SubagentLifecycleManager>) -> Arc<InternalUrlRouter> {
	let router = Arc::new(InternalUrlRouter::new());
	hub_park::register_history(&router, Arc::clone(manager)).expect("history handler");
	router
}

async fn read(
	router: &InternalUrlRouter,
	root: &TempRoot,
	uri: &str,
) -> Result<pi_agent::tools::InternalResource, String> {
	router
		.resolve(uri, &root.scope().resolve_context(&root.path, &CancellationToken::new()))
		.await
}

struct HostHistory;

#[async_trait]
impl pi_agent::tools::ProtocolHandler for HostHistory {
	fn scheme(&self) -> &str {
		"history"
	}

	async fn resolve(
		&self,
		_url: &pi_agent::tools::InternalUrl,
		_context: &pi_agent::tools::ResolveContext,
	) -> Result<pi_agent::tools::InternalResource, String> {
		Ok(pi_agent::tools::InternalResource::text("history://x", "shadow".to_string()))
	}
}

#[tokio::test]
async fn index_lists_live_parked_tombstoned_and_disk_rows_without_advisors() {
	let root = TempRoot::new("index");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let router = router(&manager);

	let live = reserve(&supervisor, "live");
	append(&supervisor, &live, &[Message::user("live row")]);

	let parked = reserve(&supervisor, "parked");
	append(&supervisor, &parked, &[Message::user("parked row")]);
	manager
		.adopt(&parked, Some(contract(&parked, &root.path)))
		.expect("adopt");
	manager.park(&parked);

	let tombstoned = reserve(&supervisor, "tombstoned");
	append(&supervisor, &tombstoned, &[Message::user("tombstone row")]);
	assert!(manager.tombstone(&tombstoned, "hard kill"));

	append_messages(&root.subagents().join("Ghost.jsonl"), &[&Message::user("disk row")]);
	let nested = root.subagents().join("Parent");
	fs::create_dir_all(&nested).expect("nested dir");
	append_messages(
		&nested.join("Parent.Child.jsonl"),
		&[&Message::user("nested row")],
	);
	append_messages(
		&root.subagents().join("__advisor.jsonl"),
		&[&Message::user("advisor row")],
	);

	let index = read(&router, &root, "history://").await.expect("index");
	assert_eq!(index.content_type, "text/markdown");
	assert!(index.content.starts_with("# Agents"));
	assert!(index.content.contains("| id | status | kind | parent | last activity |"));
	for id in [&live, &parked, &tombstoned] {
		assert!(index.content.contains(id), "{id} missing:\n{}", index.content);
	}
	assert!(index.content.contains("| parked |"), "{}", index.content);
	assert!(index.content.contains("| tombstoned |"), "{}", index.content);
	assert!(index.content.contains("| running |"), "{}", index.content);
	assert!(index.content.contains("Ghost"), "{}", index.content);
	assert!(index.content.contains("Parent.Child"), "{}", index.content);
	assert!(!index.content.contains("__advisor"), "{}", index.content);
	assert!(index.content.contains("Read a transcript with `read history://<id>`."));
}

#[tokio::test]
async fn live_transcript_reads_appends_without_claiming_an_in_memory_session() {
	let root = TempRoot::new("live-append");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let router = router(&manager);
	let id = reserve(&supervisor, "live");
	append(&supervisor, &id, &[Message::user("first-answer")]);

	let first = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("live read");
	assert_eq!(first.content_type, "text/markdown");
	assert!(!first.content.starts_with('{'), "raw JSONL leaked: {}", first.content);
	assert!(first.content.starts_with(&format!("# {id} (running)")));
	assert!(first.content.contains("first-answer"));
	assert!(!first.immutable, "a live transcript snapshot may refresh");
	assert_eq!(
		first.source_path.as_deref(),
		Some(transcript_path(&root.subagents(), &id).as_path())
	);
	let notes = first.notes.expect("source note");
	assert!(
		notes.iter().any(|note| note == "Source: transcript file (read-only, running)"),
		"{notes:?}"
	);

	append(&supervisor, &id, &[Message::assistant("second-answer")]);
	let second = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("live reread");
	assert!(second.content.contains("first-answer"));
	assert!(second.content.contains("second-answer"));

	// Closing the live session keeps the transcript readable; it is the same
	// reader, only the reported status changes.
	manager
		.adopt(&id, Some(contract(&id, &root.path)))
		.expect("adopt live row");
	assert!(manager.park(&id));
	let closed = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("closed live read");
	assert!(closed.content.starts_with(&format!("# {id} (parked)")));
	assert!(closed.content.contains("second-answer"));
}

#[tokio::test]
async fn parked_and_tombstoned_transcripts_stay_readable() {
	let root = TempRoot::new("parked");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let router = router(&manager);

	let parked = reserve(&supervisor, "parked");
	append(&supervisor, &parked, &[Message::user("parked-prior-answer")]);
	manager
		.adopt(&parked, Some(contract(&parked, &root.path)))
		.expect("adopt");
	manager.park(&parked);
	let parked_read = read(&router, &root, &format!("history://{parked}"))
		.await
		.expect("parked read");
	assert!(parked_read.content.starts_with(&format!("# {parked} (parked)")));
	assert!(parked_read.content.contains("parked-prior-answer"));

	let tombstoned = reserve(&supervisor, "tombstoned");
	append(
		&supervisor,
		&tombstoned,
		&[Message::user("tombstoned-prior-answer")],
	);
	assert!(manager.tombstone(&tombstoned, "hard kill"));
	let tombstoned_read = read(&router, &root, &format!("history://{tombstoned}"))
		.await
		.expect("tombstoned read");
	assert!(tombstoned_read
		.content
		.starts_with(&format!("# {tombstoned} (tombstoned)")));
	assert!(tombstoned_read.content.contains("tombstoned-prior-answer"));
	assert_eq!(
		tombstoned_read.notes.expect("notes"),
		vec!["Source: session file (read-only, tombstoned)".to_string()]
	);

	// A parked/closed live session never removes the file reader.
	manager.park(&tombstoned);
	let still_readable = read(&router, &root, &format!("history://{tombstoned}"))
		.await
		.expect("closed live read");
	assert!(still_readable.content.contains("tombstoned-prior-answer"));
}

#[tokio::test]
async fn malformed_tail_is_tolerated_and_never_persisted_back() {
	let root = TempRoot::new("malformed");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let router = router(&manager);
	let id = reserve(&supervisor, "malformed");
	append(
		&supervisor,
		&id,
		&[Message::user("before-tail"), Message::assistant("also-before")],
	);
	let path = transcript_path(&root.subagents(), &id);
	let mut file = fs::OpenOptions::new()
		.append(true)
		.open(&path)
		.expect("open transcript");
	file.write_all(b"{\"type\":\"message\",\"message\":\n\nnot-json\n")
		.expect("append malformed tail");
	drop(file);
	let before = fs::read(&path).expect("transcript bytes");

	let resource = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("tolerant read");
	assert!(resource.content.contains("before-tail"));
	assert!(resource.content.contains("also-before"));
	assert!(!resource.content.contains("not-json"));
	let after = fs::read(&path).expect("transcript bytes after read");
	assert_eq!(before, after, "history reads must never rewrite the transcript");
}

#[tokio::test]
async fn unknown_and_missing_transcript_errors_stay_distinct() {
	let root = TempRoot::new("errors");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let router = router(&manager);
	let known = reserve(&supervisor, "known");
	append(&supervisor, &known, &[Message::user("known-body")]);

	let unknown = read(&router, &root, "history://Nope")
		.await
		.expect_err("unknown agent");
	assert_eq!(
		unknown,
		format!(
			"Unknown agent: Nope\nKnown agents: {known}\nList all with history://"
		)
	);

	let missing = reserve(&supervisor, "missing");
	let missing_path = transcript_path(&root.subagents(), &missing);
	fs::remove_file(&missing_path).expect("remove transcript");
	let error = read(&router, &root, &format!("history://{missing}"))
		.await
		.expect_err("missing transcript");
	assert_eq!(
		error,
		format!(
			"Agent {missing} has no transcript: session is gone and no session file was retained"
		)
	);
}

#[tokio::test]
async fn cancelled_reads_abort_before_touching_the_transcript() {
	let root = TempRoot::new("cancel");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let router = router(&manager);
	let id = reserve(&supervisor, "cancel");
	append(&supervisor, &id, &[Message::user("body")]);

	let signal = CancellationToken::new();
	signal.cancel();
	let error = router
		.resolve(
			&format!("history://{id}"),
			&root.scope().resolve_context(&root.path, &signal),
		)
		.await
		.expect_err("pre-cancelled resolve");
	assert_eq!(error, "aborted");

	let tool = ReadTool::new().with_internal_urls(
		Arc::clone(&router),
		Arc::new(root.scope()),
	);
	let cancelled = AgentContext::with_cancellation(root.path.clone(), {
		let signal = CancellationToken::new();
		signal.cancel();
		signal
	});
	let result = tool
		.execute(&json!({ "path": format!("history://{id}") }), &cancelled)
		.await
		.expect("read executes");
	assert!(result.is_error);
	assert_eq!(result.content, "Read cancelled");
}

#[tokio::test]
async fn advisor_transcripts_are_hidden_from_index_and_lookup() {
	let root = TempRoot::new("advisor");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let router = router(&manager);
	let visible = reserve(&supervisor, "visible");
	append(&supervisor, &visible, &[Message::user("visible-body")]);
	append_messages(
		&root.subagents().join("__advisor.jsonl"),
		&[&Message::user("advisor-secret")],
	);

	let index = read(&router, &root, "history://").await.expect("index");
	assert!(index.content.contains(&visible));
	assert!(!index.content.contains("__advisor"));
	assert!(!index.content.contains("advisor-secret"));

	let error = read(&router, &root, "history://__advisor")
		.await
		.expect_err("advisor hidden");
	assert!(error.starts_with("Unknown agent: __advisor"), "{error}");
	assert!(!error.contains("advisor-secret"));
}

#[tokio::test]
async fn caller_scope_isolates_two_sessions_sharing_an_agent_id() {
	let root_a = TempRoot::new("iso-a");
	let root_b = TempRoot::new("iso-b");
	let supervisor_a = supervisor(&root_a);
	let supervisor_b = supervisor(&root_b);
	let manager_b = manager(&supervisor_b);
	let id_a = reserve(&supervisor_a, "same");
	let id_b = reserve(&supervisor_b, "same");
	assert_eq!(id_a, id_b, "both sessions restart their own task ids");
	append(&supervisor_a, &id_a, &[Message::user("alpha-body")]);
	let second_b = reserve(&supervisor_b, "other");
	append(&supervisor_b, &id_b, &[Message::user("beta-body")]);
	append(&supervisor_b, &second_b, &[Message::user("gamma-body")]);

	// A shared router keeps only the newest manager: session A must still read
	// its own transcript and never session B's.
	let shared = router(&manager_b);
	let from_a = read(&shared, &root_a, &format!("history://{id_a}"))
		.await
		.expect("caller A read");
	assert!(from_a.content.contains("alpha-body"));
	assert!(!from_a.content.contains("beta-body"));

	let from_b = read(&shared, &root_b, &format!("history://{id_b}"))
		.await
		.expect("caller B read");
	assert!(from_b.content.contains("beta-body"));
	assert!(!from_b.content.contains("alpha-body"));

	let index_a = read(&shared, &root_a, "history://").await.expect("index A");
	assert!(index_a.content.contains(&id_a));
	assert!(
		!index_a.content.contains(&second_b),
		"session A must not list session B's ids:\n{}",
		index_a.content
	);
	let index_b = read(&shared, &root_b, "history://").await.expect("index B");
	assert!(index_b.content.contains(&second_b));

	// The bound manager still serves its own caller, proving the fallback above
	// came from caller scope rather than from the newest manager.
	let visible = read(&shared, &root_b, &format!("history://{second_b}"))
		.await
		.expect("manager B row");
	assert!(visible.content.contains("gamma-body"));
}

#[tokio::test]
async fn case_insensitive_and_nested_disk_ids_resolve() {
	let root = TempRoot::new("nested");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let router = router(&manager);
	let id = reserve(&supervisor, "Case");
	append(&supervisor, &id, &[Message::user("case-body")]);

	let upper = id.to_uppercase();
	let from_case = read(&router, &root, &format!("history://{upper}"))
		.await
		.expect("case-insensitive read");
	assert!(from_case.content.contains("case-body"));

	let nested = root.subagents().join("Parent");
	fs::create_dir_all(&nested).expect("nested dir");
	append_messages(
		&nested.join("Parent.Child.jsonl"),
		&[&Message::user("nested-body")],
	);
	let from_nested = read(&router, &root, "history://parent.child")
		.await
		.expect("nested case-insensitive read");
	assert!(from_nested.content.contains("nested-body"));
	assert!(from_nested.content.starts_with("# Parent.Child (on disk)"));
}

#[tokio::test]
async fn transcript_markdown_collapses_tool_pairs_and_elides_thinking() {
	let root = TempRoot::new("markdown");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let router = router(&manager);
	let id = reserve(&supervisor, "markdown");

	let mut assistant = Message::assistant_with_tools(
		"final answer",
		vec![ToolCall {
			id:     "call-1".to_string(),
			name:   "read".to_string(),
			arguments: json!({ "path": "src/lib.rs", "i": "checking" }),
		}],
	);
	assistant.parts = vec![
		ContentPart::thinking("private-reasoning"),
		ContentPart::redacted_thinking("opaque-reasoning"),
		ContentPart::text("final answer"),
	];
	let mut result = Message::tool("call-1", "one\ntwo\nthree");
	result.name = Some("read".to_string());
	append(
		&supervisor,
		&id,
		&[Message::user("please check"), assistant, result],
	);

	let resource = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("markdown read");
	assert!(resource.content.contains("## user\n\nplease check"));
	assert!(resource.content.contains("## assistant\n\nfinal answer"));
	assert!(
		resource
			.content
			.contains("→ read(src/lib.rs) ⇒ ok · 3 lines"),
		"{}",
		resource.content
	);
	assert_eq!(resource.content.matches("→ read(").count(), 1);
	assert!(!resource.content.contains("private-reasoning"));
	assert!(!resource.content.contains("opaque-reasoning"));
	assert!(!resource.content.contains("## tool"));
}

#[tokio::test]
async fn read_tool_pages_history_and_rejects_writes_anchors_and_overrides() {
	let root = TempRoot::new("read");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let router = router(&manager);
	let id = reserve(&supervisor, "paged");
	append(
		&supervisor,
		&id,
		&[Message::user("alpha\nbeta\ngamma\ndelta")],
	);

	let tool = ReadTool::new().with_internal_urls(
		Arc::clone(&router),
		Arc::new(root.scope()),
	);
	let ranged = tool
		.execute(
			&json!({ "path": format!("history://{id}:5-6") }),
			&AgentContext::new(root.path.clone()),
		)
		.await
		.expect("read executes");
	assert!(!ranged.is_error, "{}", ranged.content);
	assert!(ranged.content.contains("5|alpha"), "{}", ranged.content);
	assert!(ranged.content.contains("6|beta"), "{}", ranged.content);
	let details = ranged.details.expect("details");
	assert_eq!(details["internalUrl"]["contentType"], json!("text/markdown"));
	assert_eq!(details["internalUrl"]["immutable"], json!(false));
	assert_eq!(details["internalUrl"]["writable"], json!(false));

	let raw = tool
		.execute(
			&json!({ "path": format!("history://{id}:raw") }),
			&AgentContext::new(root.path.clone()),
		)
		.await
		.expect("read executes");
	assert!(!raw.is_error, "{}", raw.content);
	assert!(raw.content.starts_with(&format!("# {id} (running)")));
	assert!(!raw.content.contains("1|"), "{}", raw.content);

	let scope = root.scope();
	let write_context = scope.write_context(&root.path, &CancellationToken::new());
	let write_error = router
		.write(&format!("history://{id}"), "mutate", &write_context)
		.await
		.expect_err("history is read-only");
	assert_eq!(
		write_error,
		"history:// URLs are read-only for write; use the protocol-specific tool for mutations."
	);

	let fresh = InternalUrlRouter::new();
	let error = fresh
		.register(Arc::new(HostHistory))
		.err()
		.expect("reserved scheme");
	assert_eq!(error, "history:// is reserved for built-in handlers");
	assert!(!fresh.can_handle("history://x"));
}


#[tokio::test]
async fn live_snapshot_precedes_flush_and_handles_steer_tool_pending_and_rollback() {
	let root = TempRoot::new("live-events");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let router = router(&manager);
	let id = reserve(&supervisor, "live-events");
	let path = transcript_path(&root.subagents(), &id);

	// Real producer sequence: owned initial prompt, accepted steering, assistant
	// MessageEnd, then its ToolEnd. TurnEnd repeats the pair and must be ignored.
	manager.seed_live(&id, Message::user("initial owned prompt"));
	manager.apply_live_event(
		&id,
		&AgentEvent::UserSteer {
			parts: vec![ContentPart::text("steered-live")],
		},
	);
	let assistant = Message::assistant_with_tools(
		"live answer",
		vec![ToolCall {
			id:     "call-live".to_string(),
			name:   "read".to_string(),
			arguments: json!({ "path": "src/live.rs" }),
		}],
	);
	manager.apply_live_event(
		&id,
		&AgentEvent::MessageEnd {
			message: assistant.clone(),
			status:  TurnStatus::ToolUse,
		},
	);
	manager.apply_live_event(
		&id,
		&AgentEvent::ToolEnd {
			id:      "call-live".to_string(),
			name:    "read".to_string(),
			result:  "one\ntwo".to_string(),
			is_error: false,
			images:  Vec::new(),
			details: None,
		},
	);
	manager.apply_live_event(
		&id,
		&AgentEvent::TurnEnd {
			message:      assistant,
			tool_results: Vec::new(),
			status:       TurnStatus::ToolUse,
		},
	);

	// The producer has emitted nothing to the JSONL yet.
	assert_eq!(
		fs::read_to_string(&path).unwrap_or_default(),
		"",
		"fixture must prove the live view is not the transcript file"
	);
	let live = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("live snapshot read");
	assert!(live.content.contains("initial owned prompt"));
	assert!(live.content.contains("steered-live"));
	assert!(live.content.contains("live answer"));
	assert!(
		live.content.contains("→ read(src/live.rs) ⇒ ok · 2 lines"),
		"{}",
		live.content
	);
	assert_eq!(live.content.matches("## assistant").count(), 1, "{}", live.content);
	assert_eq!(
		live.notes.expect("notes"),
		vec!["Source: live session (read-only, running)".to_string()]
	);

	// Read-tool selectors operate on the live snapshot too.
	let tool = ReadTool::new().with_internal_urls(
		Arc::clone(&router),
		Arc::new(root.scope()),
	);
	let raw = tool
		.execute(
			&json!({ "path": format!("history://{id}:raw") }),
			&AgentContext::new(root.path.clone()),
		)
		.await
		.expect("read executes");
	assert!(!raw.is_error, "{}", raw.content);
	assert!(raw.content.contains("live answer"));

	// A cancelled turn shows its pending call; the error rollback replaces the
	// partial turn with the authoritative state before the request.
	manager.replace_live(&id, vec![Message::user("pending prompt")]);
	manager.apply_live_event(
		&id,
		&AgentEvent::MessageEnd {
			message: Message::assistant_with_tools(
				"",
				vec![ToolCall {
					id:     "call-pending".to_string(),
					name:   "bash".to_string(),
					arguments: json!({ "command": "sleep 5" }),
				}],
			),
			status:  TurnStatus::ToolUse,
		},
	);
	let pending = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("pending read");
	assert!(
		pending
			.content
			.contains("→ bash(sleep 5) ⇒ pending"),
		"{}",
		pending.content
	);
	manager.replace_live(&id, vec![Message::user("pending prompt")]);
	let rolled_back = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("rollback read");
	assert!(!rolled_back.content.contains("sleep 5"));

	// Live ownership end falls back to the persisted file, never a stale view.
	manager.drop_live(&id);
	let file_fallback = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("file fallback");
	assert!(!file_fallback.content.contains("initial owned prompt"));
	assert_eq!(
		file_fallback.notes.expect("notes"),
		vec!["Source: transcript file (read-only, running)".to_string()]
	);
}

#[tokio::test]
async fn revived_live_snapshot_precedes_flush_and_survives_idle_until_park() {
	let root = TempRoot::new("revived-live");
	let supervisor = supervisor(&root);
	let provider = gated_provider("gated-answer");
	let manager = gated_manager(&supervisor, Arc::clone(&provider));
	let router = router(&manager);
	let id = reserve(&supervisor, "revived-live");
	append(
		&supervisor,
		&id,
		&[Message::user("prior-question"), Message::assistant("prior-answer")],
	);
	manager
		.adopt(&id, Some(contract(&id, &root.path)))
		.expect("adopt");

	let outcome = manager
		.send(&id, "resume-prompt".to_string())
		.expect("revive");
	assert_eq!(outcome, DeliveryOutcome::Revived);
	tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
		.await
		.expect("provider entered");

	let transcript = transcript_path(&root.subagents(), &id);
	assert!(
		!fs::read_to_string(&transcript)
			.unwrap_or_default()
			.contains("gated-answer"),
		"the reply has not been flushed yet"
	);
	let before = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("live before flush");
	assert!(before.content.contains("prior-answer"));
	assert!(before.content.contains("resume-prompt"));
	assert!(
		!before.content.contains("gated-answer"),
		"the provider has not answered yet"
	);
	assert_eq!(
		before.notes.expect("notes"),
		vec!["Source: live session (read-only, running)".to_string()]
	);

	provider.gate.add_permits(1);
	let mut answered = false;
	for _ in 0..500 {
		if manager.status(&id) == Some(LifecycleStatus::Idle)
			&& manager
				.live_messages(&id)
				.is_some_and(|messages| messages.iter().any(|m| m.content.contains("gated-answer")))
		{
			answered = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(answered, "live snapshot must receive the turn result");

	let idle = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("live idle read");
	assert!(idle.content.contains("gated-answer"));
	assert_eq!(
		idle.notes.expect("notes"),
		vec!["Source: live session (read-only, idle)".to_string()]
	);

	let tool = ReadTool::new().with_internal_urls(
		Arc::clone(&router),
		Arc::new(root.scope()),
	);
	let raw = tool
		.execute(
			&json!({ "path": format!("history://{id}:raw") }),
			&AgentContext::new(root.path.clone()),
		)
		.await
		.expect("read executes");
	assert!(!raw.is_error, "{}", raw.content);
	assert!(raw.content.contains("gated-answer"));

	manager.park(&id);
	assert!(manager.live_messages(&id).is_none());
	let parked = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("parked file read");
	assert!(parked.content.contains("gated-answer"));
}


#[tokio::test]
async fn task_tool_live_snapshot_precedes_flush_then_hands_over_live() {
	let root = TempRoot::new("producer");
	let release = root.path.join("child-tool-release");
	let started = root.path.join("child-tool-started");
	// The first turn answers and then calls `bash` with a command that reports it
	// started and waits for the test. Holding the loop *inside* the tool is what
	// makes "live snapshot before the journal flush" observable: the journal is
	// flushed at every turn boundary (before compaction may prune), so only a
	// blocked tool keeps an accepted answer out of the transcript.
	let provider = gated_scripted_provider_with_command(
		"child-live-answer",
		&format!(
			"echo started > {}; i=0; while [ ! -e {} ] && [ $i -lt 400 ]; do i=$((i+1)); sleep 0.05; done; true",
			started.display(),
			release.display()
		),
	);
	let supervisor = supervisor_with(&root, Arc::clone(&provider));
	let manager = gated_manager(&supervisor, Arc::clone(&provider));
	let router = router(&manager);
	let _sink = supervisor.owner_results_sink(None);
	let tool = TaskTool::new(Arc::clone(&supervisor), 0);
	let ctx = AgentContext::new(root.path.clone());
	let result = tool
		.execute(
			&json!({ "name": "Live", "task": "LIVE_PROMPT" }),
			&ctx,
		)
		.await
		.expect("task start");
	let details: TaskToolDetails =
		serde_json::from_str(&result.content).expect("task details");
	let job = details.async_state.expect("background task");
	assert_eq!(job.state, "running");
	let mut live_child = None;
	for _ in 0..500 {
		if let Some(record) = manager.child_records().first() {
			live_child = Some(record.id.clone());
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	let id = live_child.expect("live child record");
	let mut answered = false;
	for _ in 0..500 {
		if started.is_file()
			&& manager.live_messages(&id).is_some_and(|messages| {
				messages
					.iter()
					.any(|message| message.content.contains("child-live-answer"))
			})
		{
			answered = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(
		answered,
		"the live snapshot must receive the turn result while the tool is still running"
	);
	let transcript = transcript_path(&root.subagents(), &id);
	let unjournaled = fs::read_to_string(&transcript).unwrap_or_default();
	assert!(
		!unjournaled.contains("child-live-answer"),
		"the answer must still be unjournaled inside the tool; transcript: {unjournaled}"
	);
	// A refinement accepted mid-turn: the loop appends it to the child's own
	// state, so it must also reach the journal and any later revive.
	supervisor
		.inject_peer_message(&id, "keep plantdb.toml read-only".to_string())
		.expect("steering accepted by the running child");

	let live = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("live history read");
	assert!(live.content.contains("LIVE_PROMPT"), "{}", live.content);
	assert!(
		live.content.contains("child-live-answer"),
		"live snapshot must include the producer turn before flush: {}",
		live.content
	);
	assert!(
		live.content.contains("→ bash(echo started"),
		"scripted child tool call must reach the live view: {}",
		live.content
	);
	assert_eq!(
		live.notes.expect("notes"),
		vec!["Source: live session (read-only, running)".to_string()]
	);

	// Release the tool: the next turn boundary journals the accepted turn before
	// the loop asks the provider again (that follow-up request is the gated one).
	fs::write(&release, b"go").expect("release the child tool");
	tokio::time::timeout(Duration::from_secs(10), provider.entered.notified())
		.await
		.expect("child follow-up request entered");
	let mut boundary_journaled = false;
	for _ in 0..500 {
		if fs::read_to_string(&transcript)
			.unwrap_or_default()
			.contains("child-live-answer")
		{
			boundary_journaled = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(
		boundary_journaled,
		"the turn boundary must journal the accepted answer before the next request"
	);

	provider.gate.add_permits(1);
	let mut delivered = false;
	for _ in 0..500 {
		if !supervisor.take_results_for(None).is_empty() {
			delivered = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(delivered, "background task must deliver its result");
	let mut settled = false;
	for _ in 0..500 {
		if manager.status(&id) == Some(LifecycleStatus::Idle) && manager.is_live(&id) {
			settled = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(
		settled,
		"keep-alive adoption must leave a live idle row (TS keeps the session)"
	);
	let mut journaled = false;
	let mut transcript_body = String::new();
	for _ in 0..500 {
		transcript_body = fs::read_to_string(&transcript).unwrap_or_default();
		if transcript_body.contains("child-live-answer") {
			journaled = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(journaled, "final answer must be journaled: {transcript_body}");
	assert!(
		transcript_body.contains("keep plantdb.toml read-only"),
		"accepted steering must be journaled with the turn it joined: {transcript_body}"
	);

	// The live row is still the authoritative view until the idle TTL parks it.
	let live_idle = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("live idle read");
	assert!(live_idle.content.contains("child-live-answer"));
	assert_eq!(
		live_idle.notes.expect("notes"),
		vec!["Source: live session (read-only, idle)".to_string()]
	);

	// Park drops the actor: the transcript file takes over from here.
	manager.park(&id);
	let disk = read(&router, &root, &format!("history://{id}"))
		.await
		.expect("disk fallback read");
	assert!(disk.content.contains("child-live-answer"), "{}", disk.content);
	assert_eq!(
		disk.notes.expect("notes"),
		vec!["Source: transcript file (read-only, parked)".to_string()]
	);
	assert!(
		disk.content.contains("keep plantdb.toml read-only"),
		"accepted steering must survive park on disk: {}",
		disk.content
	);

	// Revive replays that journal: the instruction is still in context.
	assert_eq!(
		manager.send(&id, "continue".to_string()).expect("revive"),
		DeliveryOutcome::Revived
	);
	tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
		.await
		.expect("revived turn entered provider");
	let revived = manager.live_messages(&id).expect("revived snapshot");
	assert!(
		revived
			.iter()
			.any(|message| message.content.contains("keep plantdb.toml read-only")),
		"revived state must include the accepted steering"
	);
	provider.gate.add_permits(1);
	manager.shutdown_actors().await;
	assert!(!manager.is_live(&id));
}

#[tokio::test]
async fn explicit_roots_without_session_file_deny_foreign_manager() {
	let root_a = TempRoot::new("explicit-a");
	let root_b = TempRoot::new("explicit-b");
	let supervisor_a = supervisor(&root_a);
	let supervisor_b = supervisor(&root_b);
	let manager_b = manager(&supervisor_b);
	let id_a = reserve(&supervisor_a, "a");
	let _first_b = reserve(&supervisor_b, "first-b");
	let id_b = reserve(&supervisor_b, "b");
	append(&supervisor_a, &id_a, &[Message::user("alpha-body")]);
	append(&supervisor_b, &id_b, &[Message::user("beta-body")]);
	manager_b.seed_live(&id_b, Message::user("beta-live-secret"));
	let shared = router(&manager_b);
	let mut scope = UrlScope::default();
	scope
		.local_roots
		.insert("local".to_string(), root_a.artifacts());
	let context = scope.resolve_context(&root_a.path, &CancellationToken::new());

	let own = shared
		.resolve(&format!("history://{id_a}"), &context)
		.await
		.expect("caller A disk read");
	assert!(own.content.contains("alpha-body"));
	assert!(!own.content.contains("beta-body"));

	let foreign = shared
		.resolve(&format!("history://{id_b}"), &context)
		.await
		.expect_err("foreign manager denied");
	assert!(foreign.starts_with(&format!("Unknown agent: {id_b}")), "{foreign}");
	assert!(!foreign.contains("beta-live-secret"));
}

#[tokio::test]
async fn empty_scope_denies_manager_rows_and_live_snapshots() {
	let root = TempRoot::new("empty-scope");
	let supervisor = supervisor(&root);
	let manager = manager(&supervisor);
	let id = reserve(&supervisor, "empty");
	append(&supervisor, &id, &[Message::user("file-secret")]);
	manager.seed_live(&id, Message::user("live-secret"));
	let router = router(&manager);
	let context = UrlScope::default().resolve_context(&root.path, &CancellationToken::new());

	let index = router
		.resolve("history://", &context)
		.await
		.expect("empty index");
	assert!(index.content.contains("No agents registered."));
	assert!(!index.content.contains(&id));

	let error = router
		.resolve(&format!("history://{id}"), &context)
		.await
		.expect_err("empty scope denial");
	assert!(error.starts_with(&format!("Unknown agent: {id}")), "{error}");
	assert!(!error.contains("live-secret"));
}

#[cfg(unix)]
mod symlink_scope {
	use std::os::unix::fs::symlink;

	use super::*;

	#[tokio::test]
	async fn root_symlink_denies_live_and_disk_before_visibility() {
		let root = TempRoot::new("root-link");
		let outside = root.path.join("outside");
		fs::create_dir_all(&outside).expect("outside");
		append_messages(
			&outside.join("task-1.jsonl"),
			&[&Message::user("outside-secret")],
		);
		let subagents = root.subagents();
		fs::remove_dir_all(&subagents).expect("remove subagents");
		symlink(&outside, &subagents).expect("root symlink");

		let supervisor = supervisor(&root);
		let manager = manager(&supervisor);
		manager.seed_live("task-1", Message::user("live-secret"));
		let router = router(&manager);
		let index = read(&router, &root, "history://").await.expect("index");
		assert!(!index.content.contains("task-1"), "{}", index.content);
		let error = read(&router, &root, "history://task-1")
			.await
			.expect_err("root symlink denied");
		assert!(error.starts_with("Unknown agent: task-1"), "{error}");
		assert!(!error.contains("outside-secret"));
		assert!(!error.contains("live-secret"));
	}

	#[tokio::test]
	async fn ancestor_symlink_denies_manager_before_live_visibility() {
		let root = TempRoot::new("ancestor-link");
		let real = root.path.join("real").join("session").join("subagents");
		fs::create_dir_all(&real).expect("real subagents");
		append_messages(
			&real.join("task-1.jsonl"),
			&[&Message::user("real-body")],
		);
		let linked = root.path.join("linked");
		symlink(root.path.join("real"), &linked).expect("ancestor symlink");

		let provider: Arc<dyn ModelProvider> = Arc::new(NoopProvider);
		let supervisor = Supervisor::new(
			ToolRegistry::with_builtins(),
			provider,
			None,
			DelegationLimits::default(),
		);
		supervisor.set_child_session_root(Some(real.clone()));
		let manager = manager(&supervisor);
		manager.seed_live("task-1", Message::user("live-secret"));
		let router = router(&manager);
		let mut scope = UrlScope::default();
		scope.session_file = Some(linked.join("session.jsonl"));
		scope
			.local_roots
			.insert("local".to_string(), linked.join("session"));
		let context = scope.resolve_context(&root.path, &CancellationToken::new());
		let error = router
			.resolve("history://task-1", &context)
			.await
			.expect_err("ancestor symlink denied");
		assert!(error.starts_with("Unknown agent: task-1"), "{error}");
		assert!(!error.contains("real-body"));
		assert!(!error.contains("live-secret"));
	}

	#[tokio::test]
	async fn nested_and_leaf_symlinks_are_never_read() {
		let root = TempRoot::new("nested-leaf-links");
		let supervisor = supervisor(&root);
		let manager = manager(&supervisor);
		let router = router(&manager);

		let outside_dir = root.path.join("outside-dir");
		fs::create_dir_all(&outside_dir).expect("outside dir");
		append_messages(
			&outside_dir.join("Parent.Child.jsonl"),
			&[&Message::user("nested-secret")],
		);
		let parent_link = root.subagents().join("Parent");
		symlink(&outside_dir, &parent_link).expect("nested dir symlink");

		let outside_leaf = root.path.join("outside-leaf.jsonl");
		append_messages(&outside_leaf, &[&Message::user("leaf-secret")]);
		let leaf_link = root.subagents().join("task-1.jsonl");
		symlink(&outside_leaf, &leaf_link).expect("leaf symlink");

		let index = read(&router, &root, "history://").await.expect("index");
		assert!(!index.content.contains("Parent.Child"), "{}", index.content);
		assert!(!index.content.contains("task-1"), "{}", index.content);
		for id in ["Parent.Child", "task-1"] {
			let error = read(&router, &root, &format!("history://{id}"))
				.await
				.expect_err("symlink denied");
			assert!(error.starts_with(&format!("Unknown agent: {id}")), "{error}");
			assert!(!error.contains("nested-secret"));
			assert!(!error.contains("leaf-secret"));
		}
	}

	#[tokio::test]
	async fn manager_row_leaf_symlink_is_denied_at_open() {
		let root = TempRoot::new("manager-leaf-link");
		let supervisor = supervisor(&root);
		let manager = manager(&supervisor);
		let id = reserve(&supervisor, "linked");
		let transcript = transcript_path(&root.subagents(), &id);
		fs::remove_file(&transcript).expect("remove transcript");
		let outside = root.path.join("outside-manager.jsonl");
		append_messages(&outside, &[&Message::user("manager-secret")]);
		symlink(&outside, &transcript).expect("manager leaf symlink");
		let router = router(&manager);
		let error = read(&router, &root, &format!("history://{id}"))
			.await
			.expect_err("leaf symlink denied");
		assert!(error.contains("cannot read transcript"), "{error}");
		assert!(!error.contains("manager-secret"));
	}
}

/// The caller isolation test must not accidentally pass because the two
/// supervisors picked the same instance key.
#[test]
fn distinct_supervisors_have_distinct_instance_keys() {
	let root = TempRoot::new("keys");
	let first = supervisor(&root);
	let second = supervisor(&root);
	let mut keys = HashSet::new();
	keys.insert(first.instance_key().to_string());
	keys.insert(second.instance_key().to_string());
	assert_eq!(keys.len(), 2);
}
