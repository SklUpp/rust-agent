//! Behavioral park/revive parity: keep-alive adoption, idle TTL parking, hub
//! roster visibility, JSONL-replayed revival, restart restoration, `history://`,
//! and tombstone refusal.

use std::{
	path::{Path, PathBuf},
	sync::{
		Arc, Mutex,
		atomic::{AtomicUsize, Ordering},
	},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::stream;
use pi_agent::{
	AgentConfig, AgentLoop, CancellationToken,
	compaction::{CompactionReport, is_compaction_notice},
	mailbox::MAIN_AGENT_ID,
	provider::{
		ChunkStream, ModelProvider, ProviderError, StreamChunk, ToolCallDelta, ToolChoice,
	},
	state::{AgentContext, AgentState, Message, Role, ToolCall, ToolResult, TurnResult},
	subagent_lifecycle::{
		DEFAULT_AGENT_IDLE_TTL_MS, DeliveryOutcome, LifecycleStatus,
		SubagentLifecycleManager, attach,
	},
	supervisor::{CancelAttempt, ChildSpawn, DelegationLimits, Supervisor},
	task_persisted_revive::{
		DefaultPersistedReviverFactory, PersistedChildReviverFactory,
		PersistedChildSession, RevivedChild, load_transcript, tombstone_path, transcript_path,
		turn_journal_messages,
	},
	tools::{
		Tool,
		hub::{HubTool, HubToolDetails},
		hub_park,
		internal_urls::{InternalUrlRouter, UrlScope},
		registry::ToolRegistry,
		task_catalog::{AgentCatalog, AgentDefinition, AgentSource},
	},
};
use serde_json::json;
use tokio::sync::{Notify, Semaphore, mpsc};

fn stream_text(content: &str) -> StreamChunk {
	StreamChunk {
		content: Some(content.to_string()),
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

fn stream_done() -> StreamChunk {
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

/// Convert a scripted assistant message into stream chunks. Tool calls use a
/// complete `arguments_delta`; the loop assembles the same message the
/// non-streaming path would have returned.
fn message_chunks(message: &Message) -> Vec<Result<StreamChunk, ProviderError>> {
	let mut chunks = Vec::new();
	if !message.content.is_empty() {
		chunks.push(Ok(stream_text(&message.content)));
	}
	for (index, call) in message.tool_calls.iter().enumerate() {
		chunks.push(Ok(StreamChunk {
			content: None,
			reasoning: None,
			tool_call_deltas: vec![ToolCallDelta {
				index,
				id: Some(call.id.clone()),
				name: Some(call.name.clone()),
				arguments_delta: Some(call.arguments.to_string()),
				content_index: None,
			}],
			usage: None,
			is_done: false,
			content_index: None,
			signature: None,
			redacted: None,
			stop_reason: None,
			error_message: None,
			disabled_features: None,
		}));
	}
	chunks.push(Ok(stream_done()));
	chunks
}

struct GatedProvider {
	reply:    String,
	captured: Arc<Mutex<Vec<Message>>>,
	entered:  Arc<Notify>,
	gate:     Arc<Semaphore>,
	calls:    AtomicUsize,
}

#[async_trait]
impl ModelProvider for GatedProvider {
	async fn complete(
		&self,
		messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		self.captured
			.lock()
			.expect("captured")
			.extend_from_slice(messages);
		self.calls.fetch_add(1, Ordering::SeqCst);
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
		messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		self.captured
			.lock()
			.expect("captured")
			.extend_from_slice(messages);
		self.calls.fetch_add(1, Ordering::SeqCst);
		self.entered.notify_one();
		let _permit = self
			.gate
			.acquire()
			.await
			.map_err(|_| ProviderError::Protocol("gate closed".to_string()))?;
		Ok(Box::pin(stream::iter(vec![
			Ok(stream_text(&self.reply)),
			Ok(stream_done()),
		])))
	}
}

fn gated_provider(reply: &str) -> Arc<GatedProvider> {
	Arc::new(GatedProvider {
		reply:    reply.to_string(),
		captured: Arc::new(Mutex::new(Vec::new())),
		entered:  Arc::new(Notify::new()),
		gate:     Arc::new(Semaphore::new(0)),
		calls:    AtomicUsize::new(0),
	})
}

/// Provider that lets the test observe the in-flight request and then fails it,
/// so a revive turn ends with `Err` (not a cancel) and the actor keeps running
/// with its reconciled state.
struct FailingProvider {
	captured: Arc<Mutex<Vec<Message>>>,
	entered:  Arc<Notify>,
	gate:     Arc<Semaphore>,
}

impl FailingProvider {
	fn new() -> Arc<Self> {
		Arc::new(Self {
			captured: Arc::new(Mutex::new(Vec::new())),
			entered:  Arc::new(Notify::new()),
			gate:     Arc::new(Semaphore::new(0)),
		})
	}

	async fn record_and_wait(
		&self,
		messages: &[Message],
	) -> Result<tokio::sync::SemaphorePermit<'_>, ProviderError> {
		self
			.captured
			.lock()
			.expect("captured")
			.extend_from_slice(messages);
		self.entered.notify_one();
		self
			.gate
			.acquire()
			.await
			.map_err(|_| ProviderError::Protocol("gate closed".to_string()))
	}
}

#[async_trait]
impl ModelProvider for FailingProvider {
	async fn complete(
		&self,
		messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		let _permit = self.record_and_wait(messages).await?;
		Err(ProviderError::Protocol(
			"scripted provider failure".to_string(),
		))
	}

	async fn stream(
		&self,
		messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		let _permit = self.record_and_wait(messages).await?;
		Err(ProviderError::Protocol(
			"scripted provider failure".to_string(),
		))
	}
}

/// Provider whose first call answers with one real tool step and whose later
/// calls fail once the test releases them. The test sees each call start, so an
/// aside can be accepted in the successful step or in the failing one.
struct ToolStepProvider {
	captured: Arc<Mutex<Vec<Message>>>,
	arrived:  mpsc::UnboundedSender<()>,
	gate:     Arc<Semaphore>,
	calls:    AtomicUsize,
}

impl ToolStepProvider {
	fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<()>) {
		let (arrived, calls) = mpsc::unbounded_channel();
		let provider = Arc::new(Self {
			captured: Arc::new(Mutex::new(Vec::new())),
			arrived,
			gate: Arc::new(Semaphore::new(0)),
			calls: AtomicUsize::new(0),
		});
		(provider, calls)
	}

	/// Record the request, tell the test this call started, and wait for it to
	/// be released; returns the zero-based call index.
	async fn step(&self, messages: &[Message]) -> Result<usize, ProviderError> {
		self
			.captured
			.lock()
			.expect("captured")
			.extend_from_slice(messages);
		let call = self.calls.fetch_add(1, Ordering::SeqCst);
		let _ = self.arrived.send(());
		self
			.gate
			.acquire()
			.await
			.map_err(|_| ProviderError::Protocol("gate closed".to_string()))?
			.forget();
		Ok(call)
	}

	fn answer() -> Message {
		Message::assistant_with_tools(
			"step one",
			vec![ToolCall {
				id:        "call-bash".to_string(),
				name:      "bash".to_string(),
				arguments: json!({ "command": "true" }),
			}],
		)
	}
}

#[async_trait]
impl ModelProvider for ToolStepProvider {
	async fn complete(
		&self,
		messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		match self.step(messages).await? {
			0 => Ok(Self::answer()),
			_ => Err(ProviderError::Protocol(
				"provider failed after the tool step".to_string(),
			)),
		}
	}

	async fn stream(
		&self,
		messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		match self.step(messages).await? {
			0 => Ok(Box::pin(stream::iter(message_chunks(&Self::answer())))),
			_ => Err(ProviderError::Protocol(
				"provider failed after the tool step".to_string(),
			)),
		}
	}
}

fn session_root(label: &str) -> PathBuf {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_nanos())
		.unwrap_or_default();
	let dir = std::env::temp_dir().join(format!(
		"omp-hub-park-{label}-{}-{nanos}",
		std::process::id()
	));
	std::fs::create_dir_all(&dir).expect("session root");
	dir
}

/// Caller scope that explicitly authorizes the manager root.
fn history_scope(root: &Path) -> UrlScope {
	let mut scope = UrlScope::default();
	scope
		.local_roots
		.insert("local".to_string(), root.to_path_buf());
	scope
}

fn test_supervisor(root: &PathBuf, provider: Arc<dyn ModelProvider>) -> Arc<Supervisor> {
	let supervisor = Supervisor::new(
		ToolRegistry::with_builtins(),
		provider,
		None,
		DelegationLimits::default(),
	);
	supervisor.set_child_session_root(Some(root.clone()));
	supervisor
}

fn adoptable_child(supervisor: &Arc<Supervisor>, root: &PathBuf) -> (String, PersistedChildSession) {
	let (id, contract) = reserved_child(supervisor, root);
	supervisor.mark_completed(&id, 7, "prior-answer");
	(id, contract)
}

/// Reserve a child whose spawn is still `running`, so a test can settle it as
/// aborted before deciding whether to publish a live actor.
fn reserved_child(supervisor: &Arc<Supervisor>, root: &PathBuf) -> (String, PersistedChildSession) {
	reserved_child_with_parent(supervisor, root, None)
}

fn reserved_child_with_parent(
	supervisor: &Arc<Supervisor>,
	root: &PathBuf,
	parent_id: Option<&str>,
) -> (String, PersistedChildSession) {
	let (id, _token) = supervisor
		.reserve_child_full(ChildSpawn {
			name:                "kept",
			index:               1,
			agent:               "scout",
			agent_source:        "builtin",
			task:                "investigate the repo",
			parent_id,
			parent_tool_call_id: Some("call-park"),
			detached:            true,
			output_prefix:       None,
		})
		.expect("reserve child");
	supervisor.append_child_transcript(
		&id,
		&[
			&Message::user("Complete assignment thoroughly:\n\ninvestigate the repo"),
			&Message::assistant("prior-answer"),
		],
	);
	let contract = PersistedChildSession {
		id: id.clone(),
		agent: Some("scout".to_string()),
		task: "investigate the repo".to_string(),
		system_prompt: "child system prompt".to_string(),
		tools: vec!["read".to_string(), "yield".to_string()],
		model_role: Some("smol".to_string()),
		resolved_model: Some("test/kept".to_string()),
		output_schema: None,
		output_schema_mode: None,
		restrict_tool_names: Some(true),
		spawns: Some("*".to_string()),
		read_summarize: None,
		advisor: None,
		read_only: None,
		cwd: root.clone(),
		depth: 1,
		max_turns: 20,
		context_limit: 100_000,
		display_name: Some("kept".to_string()),
		parent_id: parent_id.map(str::to_string),
		output_id: None,
		output_published: false,
	};
	contract.write(root).expect("write contract");
	(id, contract)
}

/// Build the still-live loop/state a finished child hands to `adopt_live`.
fn live_child(
	provider: Arc<dyn ModelProvider>,
	root: &Path,
	contract: PersistedChildSession,
	messages: Vec<Message>,
) -> RevivedChild {
	let agent_loop = AgentLoop::new(
		AgentConfig {
			max_turns: 20,
			context_limit: 100_000,
			system_prompt: Some("child system prompt".to_string()),
			cwd: root.to_path_buf(),
		},
		provider,
		ToolRegistry::with_builtins(),
	);
	RevivedChild {
		agent_loop,
		state: AgentState { messages, total_turns: 0 },
		agent: None,
		contract,
	}
}

fn child_messages() -> Vec<Message> {
	vec![
		Message::system("child system prompt"),
		Message::user("Complete assignment thoroughly:\n\ninvestigate the repo"),
		Message::assistant("prior-answer"),
	]
}

fn reviver_factory(provider: Arc<GatedProvider>) -> Arc<DefaultPersistedReviverFactory> {
	Arc::new(DefaultPersistedReviverFactory::new(
		provider,
		ToolRegistry::with_builtins(),
	))
}

fn hub_details(result: pi_agent::state::ToolResult) -> HubToolDetails {
	serde_json::from_value(result.details.expect("hub details"))
		.expect("hub details shape")
}

fn captured_has(provider: &GatedProvider, body: &str) -> bool {
	provider
		.captured
		.lock()
		.expect("captured")
		.iter()
		.any(|message| message.content.contains(body))
}

#[tokio::test]
async fn keep_alive_adopts_parks_revives_replays_and_survives_restart() {
	let root = session_root("full");
	let provider = gated_provider("revived-reply");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);

	let manager = attach(&supervisor, 120, reviver_factory(provider.clone()));
	manager.adopt(&id, Some(contract)).expect("adopt");
	assert_eq!(manager.status(&id), Some(LifecycleStatus::Idle));

	// Mail addressed to the idle agent waits in the mailbox; the parked row
	// exposes it as unread until a revive drains it through the peer inbox.
	supervisor
		.mailbox()
		.buffer(pi_agent::mailbox::IrcMessage::new(
			"Main",
			id.clone(),
			"queued note",
		));

	tokio::time::sleep(Duration::from_millis(350)).await;
	assert_eq!(manager.status(&id), Some(LifecycleStatus::Parked));

	let hub = HubTool::root(supervisor.clone());
	let ctx = AgentContext::new(root.clone());
	let listed = hub
		.execute(&json!({ "op": "list", "status": "parked" }), &ctx)
		.await
		.expect("hub list");
	let details = hub_details(listed);
	let peers = details.peers.expect("peers");
	assert_eq!(peers.len(), 1, "exactly the parked row");
	assert_eq!(peers[0].id, id);
	assert_eq!(peers[0].status, "parked");
	assert_eq!(peers[0].unread, 1, "buffered mail is visible as unread");
	let counts = details.counts.expect("counts");
	assert_eq!(counts.parked, 1);
	assert_eq!(counts.running, 0);

	// First send revives from the persisted contract + JSONL replay.
	let sent = hub
		.execute(
			&json!({ "op": "send", "to": id, "message": "wake up" }),
			&ctx,
		)
		.await
		.expect("hub send");
	let receipt = hub_details(sent).receipts.expect("receipts").remove(0);
	assert_eq!(receipt.outcome, "revived");
	assert!(receipt.error.is_none());

	tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
		.await
		.expect("revived turn entered provider");
	// The replay carries the previous turn, the buffered note, and the new send.
	assert!(captured_has(&provider, "investigate the repo"));
	assert!(captured_has(&provider, "prior-answer"));
	assert!(captured_has(&provider, "queued note"));
	assert!(captured_has(&provider, "wake up"));
	assert!(captured_has(&provider, "child system prompt"));

	// Second send goes to the live revived session, not another revive. The turn
	// is in flight, so it is a non-interrupting aside (TS `deliverIrcMessage`
	// streaming branch), never a second session.
	let again = hub
		.execute(
			&json!({ "op": "send", "to": id, "message": "follow-up" }),
			&ctx,
		)
		.await
		.expect("second hub send");
	let receipt = hub_details(again).receipts.expect("receipts").remove(0);
	assert_eq!(receipt.outcome, "injected");

	// Release the gated turn. The aside joins the running dialog, so it lands in
	// that turn's history and is journaled with it; it does not start a second
	// turn (TS `flushPending` for a record that missed a step boundary).
	provider.gate.add_permits(1);
	let transcript = hub_park::transcript_path(&supervisor, &id).expect("transcript path");
	let mut journaled = false;
	for _ in 0..200 {
		let messages = load_transcript(&transcript).expect("transcript");
		if messages.iter().any(|m| m.content == "revived-reply")
			&& messages.iter().any(|m| m.content.contains("follow-up"))
		{
			journaled = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(
		journaled,
		"the turn reply and the accepted aside must be appended to the child JSONL"
	);
	assert_eq!(
		provider.calls.load(Ordering::SeqCst),
		1,
		"a busy send is an aside, not a second turn"
	);

	// Back to idle: the next send wakes the same live session for a real turn.
	let mut idle = false;
	for _ in 0..200 {
		if manager.status(&id) == Some(LifecycleStatus::Idle) {
			idle = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(idle, "the actor must return to idle after its turn");
	let woke = hub
		.execute(
			&json!({ "op": "send", "to": id, "message": "wake again" }),
			&ctx,
		)
		.await
		.expect("idle wake send");
	let receipt = hub_details(woke).receipts.expect("receipts").remove(0);
	assert_eq!(receipt.outcome, "woken");
	tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
		.await
		.expect("idle wake entered provider");
	assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
	assert!(captured_has(&provider, "follow-up"));
	assert!(captured_has(&provider, "wake again"));
	provider.gate.add_permits(1);

	let router = InternalUrlRouter::new();
	hub_park::register_history(&router, manager.clone()).expect("history handler");
	let resource = router
		.resolve(
			&format!("history://{id}"),
			&history_scope(&root).resolve_context(&root, &CancellationToken::new()),
		)
		.await
		.expect("history readable");
	assert!(resource.content.contains("prior-answer"));
	assert!(resource.content.contains("revived-reply"));

	// Restart: a fresh supervisor + manager restores the same parked row from
	// disk without any in-memory adoption.
	drop(hub);
	drop(manager);
	drop(supervisor);
	let restarted_provider = gated_provider("unused-reply");
	let restarted = test_supervisor(&root, restarted_provider.clone());
	let restarted_manager = attach(&restarted, 120, reviver_factory(restarted_provider));
	let restarted_hub = HubTool::root(restarted.clone());
	let listed = restarted_hub
		.execute(
			&json!({ "op": "list", "status": "parked" }),
			&AgentContext::new(root.clone()),
		)
		.await
		.expect("restarted list");
	let details = hub_details(listed);
	let peers = details.peers.expect("peers");
	assert_eq!(peers.len(), 1, "restored parked row");
	assert_eq!(peers[0].id, id);
	assert_eq!(peers[0].status, "parked");

	// A hard kill tombstones the id: transcript stays readable, revive refuses.
	assert!(restarted_manager.tombstone(&id, "hard kill"));
	let refused = restarted_hub
		.execute(
			&json!({ "op": "send", "to": id, "message": "please come back" }),
			&AgentContext::new(root.clone()),
		)
		.await
		.expect("tombstoned send returns a receipt");
	let receipt = hub_details(refused).receipts.expect("receipts").remove(0);
	assert_eq!(receipt.outcome, "failed");
	assert!(
		receipt
			.error
			.as_deref()
			.is_some_and(|error| error.contains("cannot be messaged or revived")),
		"tombstoned id must refuse revive: {:?}",
		receipt.error
	);
	let after = restarted_hub
		.execute(
			&json!({ "op": "list", "status": "parked" }),
			&AgentContext::new(root.clone()),
		)
		.await
		.expect("post-tombstone list");
	assert_eq!(
		hub_details(after).peers.expect("peers").len(),
		0,
		"tombstoned row leaves the roster"
	);
	let router = InternalUrlRouter::new();
	hub_park::register_history(&router, restarted_manager).expect("history handler");
	let history = router
		.resolve(
			&format!("history://{id}"),
			&history_scope(&root).resolve_context(&root, &CancellationToken::new()),
		)
		.await
		.expect("tombstoned transcript readable");
	assert!(history.content.contains("prior-answer"));
}

/// Records cold revives so a live wake can prove the session was never rebuilt.
struct CountingReviver {
	inner: Arc<DefaultPersistedReviverFactory>,
	calls: AtomicUsize,
}

impl CountingReviver {
	fn new(provider: Arc<GatedProvider>) -> Arc<Self> {
		Arc::new(Self {
			inner: Arc::new(DefaultPersistedReviverFactory::new(
				provider,
				ToolRegistry::with_builtins(),
			)),
			calls: AtomicUsize::new(0),
		})
	}

	fn calls(&self) -> usize {
		self.calls.load(Ordering::SeqCst)
	}
}

impl PersistedChildReviverFactory for CountingReviver {
	fn revive(
		&self,
		contract: &PersistedChildSession,
		replayed: Vec<Message>,
		supervisor: Option<&Arc<Supervisor>>,
	) -> Result<RevivedChild, String> {
		self.calls.fetch_add(1, Ordering::SeqCst);
		self.inner.revive(contract, replayed, supervisor)
	}
}

/// Records what the supervisor knew at the instant the child registry is built.
///
/// The URL scope bound into `read`/`write` is chosen from the supervisor record,
/// so a revive that builds the registry before restoring the record silently
/// gives the child the parent's `local://` namespace.
struct OrderProbeReviver {
	inner:  Arc<DefaultPersistedReviverFactory>,
	probe:  Arc<Mutex<Option<Option<PathBuf>>>>,
}

impl PersistedChildReviverFactory for OrderProbeReviver {
	fn revive(
		&self,
		contract: &PersistedChildSession,
		replayed: Vec<Message>,
		supervisor: Option<&Arc<Supervisor>>,
	) -> Result<RevivedChild, String> {
		let session_file = supervisor.and_then(|supervisor| {
			supervisor.child_session_file(&contract.id)
		});
		*self.probe.lock().expect("probe") = Some(session_file);
		self.inner.revive(contract, replayed, supervisor)
	}
}

/// Blocks inside the reviver until a concurrent `cancel_child` has run, so the
/// cancel/revive interleaving is sequenced instead of raced.
struct CancelRaceReviver {
	inner:   Arc<DefaultPersistedReviverFactory>,
	reached: Arc<Notify>,
	release: Arc<std::sync::Barrier>,
}

impl PersistedChildReviverFactory for CancelRaceReviver {
	fn revive(
		&self,
		contract: &PersistedChildSession,
		replayed: Vec<Message>,
		supervisor: Option<&Arc<Supervisor>>,
	) -> Result<RevivedChild, String> {
		self.reached.notify_one();
		self.release.wait();
		self.inner.revive(contract, replayed, supervisor)
	}
}

#[tokio::test]
async fn completed_child_wakes_its_own_state_and_only_rebuilds_after_park() {
	let root = session_root("live-wake");
	let provider = gated_provider("woken-reply");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let reviver = CountingReviver::new(provider.clone());
	let manager = attach(&supervisor, DEFAULT_AGENT_IDLE_TTL_MS, reviver.clone());
	let transcript = transcript_path(&root, &id);
	let journaled_before = std::fs::read_to_string(&transcript).unwrap_or_default();
	assert!(!journaled_before.is_empty(), "fixture transcript");

	// The real loop/state a finished child still owns. The last message is not
	// in the JSONL: only the in-memory state carries it, so a replay-based
	// revive could not produce it.
	let mut messages = child_messages();
	messages.push(Message::user("in-memory-only marker"));
	manager
		.adopt_live(&id, live_child(provider.clone(), &root, contract, messages))
		.expect("live adopt");
	assert_eq!(manager.status(&id), Some(LifecycleStatus::Idle));
	assert!(manager.is_live(&id));

	// `history://` serves the live snapshot before any turn is journaled.
	let router = InternalUrlRouter::new();
	hub_park::register_history(&router, manager.clone()).expect("history handler");
	let live_view = router
		.resolve(
			&format!("history://{id}"),
			&history_scope(&root).resolve_context(&root, &CancellationToken::new()),
		)
		.await
		.expect("live history readable");
	assert!(
		live_view.content.contains("in-memory-only marker"),
		"history must serve the adopted live state, not the file"
	);

	let hub = HubTool::root(supervisor.clone());
	let ctx = AgentContext::new(root.clone());

	// Idle live actor: a real wake turn on the original state, no rebuild.
	let sent = hub
		.execute(
			&json!({ "op": "send", "to": id, "message": "wake up" }),
			&ctx,
		)
		.await
		.expect("hub send");
	let receipt = hub_details(sent).receipts.expect("receipts").remove(0);
	assert_eq!(receipt.outcome, "woken");
	assert!(receipt.error.is_none());
	assert_eq!(reviver.calls(), 0, "a live actor must never be rebuilt");
	tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
		.await
		.expect("wake turn entered provider");
	assert!(captured_has(&provider, "in-memory-only marker"));
	assert!(captured_has(&provider, "wake up"));

	// Second send while that turn is gated is a non-interrupting aside. The gate
	// is still closed, so the turn cannot finish before the aside lands.
	let sent = hub
		.execute(
			&json!({ "op": "send", "to": id, "message": "busy note" }),
			&ctx,
		)
		.await
		.expect("busy send");
	let receipt = hub_details(sent).receipts.expect("receipts").remove(0);
	assert_eq!(receipt.outcome, "injected");
	assert_eq!(reviver.calls(), 0);
	provider.gate.add_permits(1);

	let mut idle = false;
	for _ in 0..300 {
		if manager.status(&id) == Some(LifecycleStatus::Idle) {
			idle = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(idle, "actor returns to idle after the aside is resolved");

	// The aside joined the running dialog: it is context for the next turn, so a
	// later wake proves it was delivered rather than dropped.
	let sent = hub
		.execute(
			&json!({ "op": "send", "to": id, "message": "wake again" }),
			&ctx,
		)
		.await
		.expect("second wake");
	let receipt = hub_details(sent).receipts.expect("receipts").remove(0);
	assert_eq!(receipt.outcome, "woken");
	tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
		.await
		.expect("follow-up wake entered provider");
	assert!(captured_has(&provider, "busy note"), "the aside must not be lost");
	assert!(captured_has(&provider, "wake again"));
	provider.gate.add_permits(1);

	// The transcript is appended, never rebuilt from scratch.
	let mut appended = None;
	for _ in 0..300 {
		let text = std::fs::read_to_string(&transcript).unwrap_or_default();
		if text.matches("woken-reply").count() >= 2 {
			appended = Some(text);
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	let appended = appended.expect("wake turns must be journaled");
	assert!(
		appended.starts_with(&journaled_before),
		"live turns append to the existing JSONL instead of rebuilding it"
	);

	// Park drops the live actor; the next send is an honest cold revive.
	manager.park(&id);
	assert!(!manager.is_live(&id));
	assert_eq!(manager.status(&id), Some(LifecycleStatus::Parked));
	let sent = hub
		.execute(
			&json!({ "op": "send", "to": id, "message": "after park" }),
			&ctx,
		)
		.await
		.expect("post-park send");
	let receipt = hub_details(sent).receipts.expect("receipts").remove(0);
	assert_eq!(receipt.outcome, "revived");
	assert_eq!(reviver.calls(), 1, "park forces exactly one rebuild");
	provider.gate.add_permits(1);

	// A hard kill cancels the live actor and refuses every later send.
	assert!(manager.tombstone(&id, "hard kill"));
	assert!(!manager.is_live(&id));
	let refused = hub
		.execute(
			&json!({ "op": "send", "to": id, "message": "never" }),
			&ctx,
		)
		.await
		.expect("tombstoned send");
	let receipt = hub_details(refused).receipts.expect("receipts").remove(0);
	assert_eq!(receipt.outcome, "failed");
	assert!(
		receipt
			.error
			.as_deref()
			.is_some_and(|error| error.contains("cannot be messaged or revived")),
		"{:?}",
		receipt.error
	);
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn settled_child_reports_and_stops_hub_cancel_for_its_live_actor() {
	let root = session_root("cancel-live");
	let provider = gated_provider("cancel-reply");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	manager
		.adopt_live(&id, live_child(provider.clone(), &root, contract, child_messages()))
		.expect("live adopt");
	// The spawn already settled, but the follow-up actor is real work: a cancel
	// must reach it instead of reporting the finished assignment.
	assert_eq!(
		supervisor.cancel_child(&id, None).await,
		CancelAttempt::Cancelled
	);
	tokio::time::timeout(Duration::from_secs(2), async {
		while manager.is_live(&id) {
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.expect("cancelled idle actor must stop without another message");
	// TS parity: cancel releases the registration, so the id is neither woken
	// nor revived afterwards.
	let refused = manager
		.send(&id, "after cancel".to_string())
		.expect_err("a cancelled live session must not be revived");
	assert!(refused.contains("hard-aborted"), "{refused}");
	assert!(!manager.is_live(&id));
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn cancel_drains_accepted_messages_before_the_actor_stops() {
	let root = session_root("cancel-drain");
	let provider = gated_provider("drained-reply");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	manager
		.adopt_live(&id, live_child(provider.clone(), &root, contract.clone(), child_messages()))
		.expect("live adopt");
	assert_eq!(
		manager.send(&id, "first turn".to_string()).expect("wake"),
		DeliveryOutcome::Woken
	);
	tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
		.await
		.expect("first turn entered provider");
	// Accepted into the running turn; its receipt is already `injected`, so it
	// may not disappear with the actor.
	assert_eq!(
		manager.send(&id, "keep this aside".to_string()).expect("aside"),
		DeliveryOutcome::Injected
	);
	assert_eq!(
		supervisor.cancel_child(&id, None).await,
		CancelAttempt::Cancelled
	);
	assert!(!manager.is_live(&id), "the live slot is dropped under the manager lock");
	// `cancel_live` tombstones the row; the actor journals every accepted
	// message before it exits, so `history://` still carries it.
	let mut transcript = String::new();
	for _ in 0..500 {
		transcript =
			std::fs::read_to_string(transcript_path(&root, &id)).unwrap_or_default();
		if transcript.contains("keep this aside") {
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(
		transcript.contains("keep this aside"),
		"an accepted message must survive the cancel: {transcript}"
	);
	let refused = manager
		.send(&id, "after cancel".to_string())
		.expect_err("a cancelled live session must not be revived");
	assert!(refused.contains("hard-aborted"), "{refused}");
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn cold_revive_restores_ownership_and_the_journal() {
	let root = session_root("cold-restore");
	let provider = gated_provider("restored-reply");
	// Settle a child, then throw the process state away: only the persisted
	// contract and transcript survive.
	let old = test_supervisor(&root, provider.clone());
	let (id, _contract) = adoptable_child(&old, &root);
	drop(old);
	let restarted = test_supervisor(&root, provider.clone());
	let manager = attach(
		&restarted,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	assert!(
		restarted.child_session_file(&id).is_none(),
		"a restarted supervisor starts without the settled record"
	);
	assert_eq!(
		manager.send(&id, "after restart".to_string()).expect("revive"),
		DeliveryOutcome::Revived
	);
	tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
		.await
		.expect("revived turn entered provider");
	// The restore is what puts the journal path and the cancel handle back.
	assert!(
		restarted.child_session_file(&id).is_some(),
		"cold revive must restore the supervisor record"
	);
	let mut transcript = String::new();
	for _ in 0..500 {
		transcript =
			std::fs::read_to_string(transcript_path(&root, &id)).unwrap_or_default();
		if transcript.contains("after restart") {
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(
		transcript.contains("after restart"),
		"a revived actor must journal through the restored record: {transcript}"
	);
	// Ownership comes from the persisted contract: a foreign owner is refused,
	// the root owner can cancel the live actor.
	assert_eq!(
		restarted.cancel_child(&id, Some("someone-else")).await,
		CancelAttempt::NotFound
	);
	assert_eq!(
		restarted.cancel_child(&id, None).await,
		CancelAttempt::Cancelled
	);
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn cold_revive_binds_the_child_url_scope_and_restores_before_the_reviver() {
	let root = session_root("cold-scope");
	let parent = session_root("cold-scope-parent");
	std::fs::write(parent.join("probe.txt"), "parent-file").expect("parent probe");
	std::fs::write(root.join("probe.txt"), "child-file").expect("child probe");
	let provider = gated_provider("scope-reply");
	let old = test_supervisor(&root, provider.clone());
	let (id, _contract) = adoptable_child(&old, &root);
	drop(old);
	let restarted = test_supervisor(&root, provider.clone());
	restarted.set_internal_urls(Some(Arc::new(InternalUrlRouter::new())));
	let mut scope = UrlScope::default();
	scope.local_roots.insert("local".to_string(), parent.clone());
	restarted.set_url_scope(Arc::new(scope));
	let probe = Arc::new(Mutex::new(None));
	let manager = attach(
		&restarted,
		DEFAULT_AGENT_IDLE_TTL_MS,
		Arc::new(OrderProbeReviver {
			inner: Arc::new(DefaultPersistedReviverFactory::new(
				provider.clone(),
				ToolRegistry::with_builtins(),
			)),
			probe: probe.clone(),
		}),
	);
	assert_eq!(
		manager.send(&id, "after restart".to_string()).expect("revive"),
		DeliveryOutcome::Revived
	);
	// The child registry is built inside the reviver, so the record must already
	// exist then; otherwise `url_scope_for(Some(child))` falls back to the parent.
	let at_revive = probe.lock().expect("probe").clone();
	assert_eq!(
		at_revive,
		Some(Some(transcript_path(&root, &id))),
		"the supervisor record must exist before the reviver builds the registry"
	);
	let mut registry = ToolRegistry::with_builtins();
	restarted.rebind_session_tools(&mut registry, None, Some(&id));
	let read = registry.get("read").expect("read tool");
	let result = read
		.execute(&json!({ "path": "local://probe.txt" }), &AgentContext::new(root.clone()))
		.await
		.expect("child read");
	assert!(!result.is_error, "{}", result.content);
	assert!(
		result.content.contains("child-file"),
		"a revived child must keep its own local:// root, got: {}",
		result.content
	);
	manager.shutdown_actors().await;
	let _ = std::fs::remove_dir_all(&parent);
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_that_lands_during_the_revive_refuses_the_actor() {
	let root = session_root("cancel-revive-race");
	let provider = gated_provider("never");
	let old = test_supervisor(&root, provider.clone());
	let (id, _contract) = adoptable_child(&old, &root);
	drop(old);
	let restarted = test_supervisor(&root, provider.clone());
	let reached = Arc::new(Notify::new());
	let release = Arc::new(std::sync::Barrier::new(2));
	let manager = attach(
		&restarted,
		DEFAULT_AGENT_IDLE_TTL_MS,
		Arc::new(CancelRaceReviver {
			inner: Arc::new(DefaultPersistedReviverFactory::new(
				provider.clone(),
				ToolRegistry::with_builtins(),
			)),
			reached: reached.clone(),
			release: release.clone(),
		}),
	);
	let cancel = {
		let restarted = restarted.clone();
		let id = id.clone();
		tokio::spawn(async move {
			tokio::time::timeout(Duration::from_secs(5), reached.notified())
				.await
				.expect("revive reached the reviver");
			// Settled, restored, and not yet published: this is the one outcome
			// that must still stop the pending revive.
			let attempt = restarted.cancel_child(&id, None).await;
			release.wait();
			attempt
		})
	};
	let result = manager.send(&id, "start".to_string());
	let attempt = tokio::time::timeout(Duration::from_secs(5), cancel)
		.await
		.expect("cancel task")
		.expect("cancel join");
	assert_eq!(attempt, CancelAttempt::AlreadyCompleted);
	assert!(
		result.is_err(),
		"a cancel between restore and publication must refuse the revive: {result:?}"
	);
	assert!(!manager.is_live(&id), "no live actor may be published");
	assert_eq!(
		provider.calls.load(Ordering::SeqCst),
		0,
		"the refused actor must never run a turn"
	);
	manager.shutdown_actors().await;
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn await_does_not_report_a_woken_actor_as_stopped() {
	let root = session_root("await-woken");
	let provider = gated_provider("late");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	manager
		.adopt_live(
			&id,
			live_child(provider.clone(), &root, contract, child_messages()),
		)
		.expect("live adopt");
	let hub = HubTool::root(supervisor.clone());
	let ctx = AgentContext::new(root.clone());
	let sent = hub
		.execute(
			&json!({ "op": "send", "to": id, "message": "ping", "await": true, "timeoutMs": 40 }),
			&ctx,
		)
		.await
		.expect("hub send");
	assert!(
		!sent.content.contains("stopped without replying"),
		"a woken actor is live even before its turn starts: {}",
		sent.content
	);
	assert!(
		sent.content.contains("No reply from"),
		"the await must run to its own timeout: {}",
		sent.content
	);
	manager.shutdown_actors().await;
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn await_stops_when_the_woken_turn_ends_without_reply() {
	let root = session_root("await-ended");
	let provider = gated_provider("no-reply");
	// One permit: the wake turn completes instead of hanging on the gate.
	provider.gate.add_permits(1);
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	manager
		.adopt_live(
			&id,
			live_child(provider.clone(), &root, contract, child_messages()),
		)
		.expect("live adopt");
	let hub = HubTool::root(supervisor.clone());
	let ctx = AgentContext::new(root.clone());
	let started = std::time::Instant::now();
	let sent = hub
		.execute(
			&json!({ "op": "send", "to": id, "message": "ping", "await": true, "timeoutMs": 30_000 }),
			&ctx,
		)
		.await
		.expect("hub send");
	assert!(
		sent.content.contains("stopped without replying"),
		"the turn ended without a reply: {}",
		sent.content
	);
	assert!(
		started.elapsed() < Duration::from_secs(10),
		"a terminal agent_end must settle the waiter, not the timeout"
	);
	manager.shutdown_actors().await;
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn wait_does_not_hold_on_idle_keep_alive_peers() {
	let root = session_root("wait-idle");
	let provider = gated_provider("never");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	manager
		.adopt_live(
			&id,
			live_child(provider.clone(), &root, contract, child_messages()),
		)
		.expect("live adopt");
	let hub = HubTool::root(supervisor.clone());
	let ctx = AgentContext::new(root.clone());
	let started = std::time::Instant::now();
	let waited = hub
		.execute(&json!({ "op": "wait", "timeoutMs": 30_000 }), &ctx)
		.await
		.expect("hub wait");
	assert!(
		waited.content.contains("No running peers to wait for."),
		"an idle keep-alive peer is not a running peer: {}",
		waited.content
	);
	assert!(
		started.elapsed() < Duration::from_secs(10),
		"idle peers must not hold a wait open"
	);
	manager.shutdown_actors().await;
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn broadcast_from_a_child_reaches_idle_main() {
	let root = session_root("broadcast-main");
	let provider = gated_provider("never");
	let supervisor = test_supervisor(&root, provider.clone());
	// No lifecycle actor at all: the root session exists and is idle between turns.
	let child = HubTool::with_owner(supervisor.clone(), Some("some-child".to_string()));
	let ctx = AgentContext::new(root.clone());
	let sent = child
		.execute(
			&json!({ "op": "send", "to": "all", "message": "to root" }),
			&ctx,
		)
		.await
		.expect("child broadcast");
	assert!(
		sent.content.contains(&format!("- {MAIN_AGENT_ID}: injected")),
		"an idle Main is still a broadcast target (TS flat roster): {}",
		sent.content
	);
	assert!(!sent.is_error, "the buffered delivery is not a failure: {}", sent.content);
	let buffered = supervisor
		.mailbox()
		.take(MAIN_AGENT_ID, None)
		.expect("the body must be buffered for the next root turn");
	assert!(buffered.body.contains("to root"), "{}", buffered.body);
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_sends_coalesce_into_one_revive() {
	let root = session_root("revive-coalesce");
	let provider = gated_provider("never");
	let old = test_supervisor(&root, provider.clone());
	let (id, _contract) = adoptable_child(&old, &root);
	drop(old);
	let restarted = test_supervisor(&root, provider.clone());
	let reached = Arc::new(Notify::new());
	let release = Arc::new(std::sync::Barrier::new(2));
	let manager = attach(
		&restarted,
		DEFAULT_AGENT_IDLE_TTL_MS,
		Arc::new(CancelRaceReviver {
			inner: Arc::new(DefaultPersistedReviverFactory::new(
				provider.clone(),
				ToolRegistry::with_builtins(),
			)),
			reached: reached.clone(),
			release: release.clone(),
		}),
	);
	let first = {
		let manager = manager.clone();
		let id = id.clone();
		tokio::task::spawn_blocking(move || manager.send(&id, "first".to_string()))
	};
	tokio::time::timeout(Duration::from_secs(5), reached.notified())
		.await
		.expect("the first revive started");
	// The second send arrives while the revive is in flight and must wait for it
	// instead of starting a second revive or failing with a dropped message.
	let second = {
		let manager = manager.clone();
		let id = id.clone();
		tokio::task::spawn_blocking(move || manager.send(&id, "second".to_string()))
	};
	release.wait();
	let first = first.await.expect("first join");
	let second = second.await.expect("second join");
	assert_eq!(first.expect("first send"), DeliveryOutcome::Revived);
	let second = second.expect("second send");
	assert!(
		matches!(second, DeliveryOutcome::Woken | DeliveryOutcome::Injected),
		"the coalesced send must deliver into the revived session, got {second:?}"
	);
	assert!(manager.is_live(&id));
	provider.gate.add_permits(8);
	let mut both = false;
	for _ in 0..500 {
		both = captured_has(&provider, "first") && captured_has(&provider, "second");
		if both {
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(both, "both coalesced messages must reach the revived session");
	manager.shutdown_actors().await;
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn broadcast_reaches_a_live_keep_alive_child() {
	let root = session_root("broadcast-live");
	let provider = gated_provider("broadcast-reply");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	manager
		.adopt_live(
			&id,
			live_child(provider.clone(), &root, contract, child_messages()),
		)
		.expect("live adopt");
	// Messaging is a flat namespace: a live child of another parent is a peer too.
	let (nested, nested_contract) =
		reserved_child_with_parent(&supervisor, &root, Some("nest-parent"));
	supervisor.mark_completed(&nested, 7, "prior-answer");
	manager
		.adopt_live(
			&nested,
			live_child(provider.clone(), &root, nested_contract, child_messages()),
		)
		.expect("nested live adopt");
	let hub = HubTool::root(supervisor.clone());
	let ctx = AgentContext::new(root.clone());
	let sent = hub
		.execute(
			&json!({ "op": "send", "to": "all", "message": "hello peers" }),
			&ctx,
		)
		.await
		.expect("hub broadcast");
	// A settled child with an idle keep-alive session is `idle` in TS terms: it
	// must be a broadcast target, not skipped for having no job or mailbox row.
	assert!(
		sent.content.contains(&format!("- {id}: woken")),
		"the live keep-alive child must receive the broadcast: {}",
		sent.content
	);
	assert!(
		sent.content.contains(&format!("- {nested}: woken")),
		"a live peer outside this hub's owner scope is still a broadcast target: {}",
		sent.content
	);
	tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
		.await
		.expect("the woken actor entered its turn");
	assert!(
		captured_has(&provider, "hello peers"),
		"the broadcast body must reach the live session"
	);
	manager.shutdown_actors().await;
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn a_failed_first_turn_after_revive_keeps_the_shifted_history() {
	let root = session_root("failed-first-turn");
	let provider = FailingProvider::new();
	let supervisor: Arc<Supervisor> = Supervisor::new(
		ToolRegistry::with_builtins(),
		provider.clone(),
		None,
		DelegationLimits::default(),
	);
	supervisor.set_child_session_root(Some(root.clone()));
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		Arc::new(DefaultPersistedReviverFactory::new(
			provider.clone(),
			ToolRegistry::with_builtins(),
		)),
	);
	// A revived state without a system prompt: the loop inserts one at index 0, so
	// every index after it shifts; a failure must still keep every replayed
	// message and the accepted aside in order, with the request echoed once.
	manager
		.adopt_live(
			&id,
			live_child(
				provider.clone(),
				&root,
				contract,
				vec![
					Message::user("autoload"),
					Message::assistant("prior-answer"),
				],
			),
		)
		.expect("live adopt");
	// The fixture seeds the persisted replay; the actor appends after it.
	let baseline = load_transcript(&transcript_path(&root, &id)).expect("baseline transcript");
	assert_eq!(
		manager.send(&id, "first turn".to_string()).expect("wake"),
		DeliveryOutcome::Woken
	);
	tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
		.await
		.expect("the revived turn reached the provider");
	assert_eq!(
		manager
			.send(&id, "keep this aside".to_string())
			.expect("aside"),
		DeliveryOutcome::Injected
	);
	// The provider fails: the turn is interrupted after the loop already absorbed
	// the aside, and the reconciled state must keep the replayed history plus it
	// without rewriting anything.
	provider.gate.add_permits(1);
	let mut state = None;
	for _ in 0..500 {
		// The reconciled state starts with the system prompt the loop inserted;
		// an intermediate live event (the absorbed aside) has no system yet, so its
		// arrival alone must not end the wait.
		if let Some(live) = manager.live_messages(&id)
			&& live.first().is_some_and(|message| message.role == Role::System)
			&& live
				.iter()
				.any(|message| message.content == "keep this aside")
		{
			state = Some(live);
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	let state = state.expect("the failed turn must reconcile the live state");
	let rendered: Vec<(Role, String)> = state
		.iter()
		.map(|message| (message.role, message.content.clone()))
		.collect();
	assert_eq!(
		rendered,
		vec![
			(Role::System, "child system prompt".to_string()),
			(Role::User, "autoload".to_string()),
			(Role::Assistant, "prior-answer".to_string()),
			(Role::User, "first turn".to_string()),
			(Role::User, "keep this aside".to_string()),
		],
		"history must keep order, drop no replayed message and duplicate nothing"
	);
	// The failed turn is never on disk: only the accepted tail (the request and
	// the aside) is appended after the seeded replay, once and in order.
	wait_for_transcript(&root, &id, baseline.len() + 2).await;
	let replay = load_transcript(&transcript_path(&root, &id)).expect("replay");
	assert_eq!(
		&replay[..baseline.len()],
		baseline.as_slice(),
		"the replayed prefix must not be rewritten: {replay:?}"
	);
	let tail = &replay[baseline.len()..];
	assert_eq!(
		tail.len(),
		2,
		"only the request and the aside may be appended: {replay:?}"
	);
	assert!(
		tail.iter().all(|message| message.role != Role::Assistant),
		"an unaccepted partial turn must not be journaled: {replay:?}"
	);
	let users: Vec<&str> = tail
		.iter()
		.filter(|message| message.role == Role::User)
		.map(|message| message.content.as_str())
		.collect();
	assert_eq!(
		users,
		vec!["first turn", "keep this aside"],
		"the accepted tail must be journaled exactly once, in order: {replay:?}"
	);
	manager.shutdown_actors().await;
	let _ = std::fs::remove_dir_all(&root);
}

/// Poll the reconciled live snapshot: it starts with the loop's system prompt
/// (an intermediate live event is not the authoritative state) and ends with
/// the aside the hub receipted.
async fn reconciled_after_aside(
	manager: &SubagentLifecycleManager,
	id: &str,
	aside: &str,
) -> Vec<Message> {
	for _ in 0..500 {
		if let Some(live) = manager.live_messages(id)
			&& live.first().is_some_and(|message| message.role == Role::System)
			&& live.last().is_some_and(|message| message.content == aside)
		{
			return live;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	panic!("the failed turn must reconcile the live state");
}

/// Bounded wait for the transcript to reach `expected` lines. The actor writes
/// the file before it publishes the reconciled state, so this only covers the
/// observation itself; the exact line count is still asserted afterwards.
async fn wait_for_transcript(root: &PathBuf, id: &str, expected: usize) {
	for _ in 0..500 {
		let lines = std::fs::read_to_string(transcript_path(root, id))
			.map(|transcript| transcript.lines().count())
			.unwrap_or_default();
		if lines >= expected {
			return;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	panic!(
		"the transcript did not reach {expected} lines: {:?}",
		std::fs::read_to_string(transcript_path(root, id))
	);
}

/// The transcript must keep the seeded replay untouched and append exactly the
/// accepted tail (the state minus its replayed system message): nothing
/// rewritten, no duplicate aside, and a replay that hands a revive the aside
/// once with its tool pair intact.
fn assert_accepted_tail_is_journaled_once(
	root: &PathBuf,
	id: &str,
	state: &[Message],
	aside: &str,
	baseline: &[Message],
) {
	let replay = load_transcript(&transcript_path(root, id)).expect("replay");
	assert_eq!(
		&replay[..baseline.len()],
		baseline,
		"the replayed prefix must not be rewritten: {replay:?}"
	);
	let tail = &replay[baseline.len()..];
	assert_eq!(
		tail.len(),
		state.len() - 1,
		"the journal must hold exactly the accepted tail: {replay:?}"
	);
	assert_eq!(
		tail
			.iter()
			.filter(|message| message.content == aside)
			.count(),
		1,
		"a revive must see the accepted aside exactly once: {replay:?}"
	);
	assert_eq!(
		tail
			.iter()
			.filter(|message| message.role == Role::Assistant)
			.count(),
		1,
		"the completed tool step stays on disk once: {replay:?}"
	);
	assert_eq!(
		tail.iter().filter(|message| message.role == Role::Tool).count(),
		1,
		"the tool result stays paired once: {replay:?}"
	);
}

/// P1: the step-boundary observer journals the completed tool pair, so a state
/// that later fails must not be rewritten shorter — the cursor already covers
/// it. The aside accepted by the failing step is appended after the pair.
#[tokio::test]
async fn an_aside_accepted_in_the_failing_step_after_a_tool_step_is_journaled_once() {
	let root = session_root("aside-in-failing-step");
	let (provider, mut arrived) = ToolStepProvider::new();
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		Arc::new(DefaultPersistedReviverFactory::new(
			provider.clone(),
			ToolRegistry::with_builtins(),
		)),
	);
	// The fixture seeds the persisted replay (assignment plus the settled answer):
	// the actor appends after it, so the file before the turn is the baseline the
	// assertions compare against.
	manager
		.adopt_live(
			&id,
			live_child(
				provider.clone(),
				&root,
				contract,
				vec![Message::system("child system prompt")],
			),
		)
		.expect("live adopt");
	let baseline = load_transcript(&transcript_path(&root, &id)).expect("baseline transcript");
	assert_eq!(
		manager.send(&id, "first turn".to_string()).expect("wake"),
		DeliveryOutcome::Woken
	);
	// Step one answers with a real tool call; bash runs and the next boundary
	// journals the pair.
	arrived.recv().await.expect("first provider call");
	provider.gate.add_permits(1);
	// Step two starts after the tool result and fails; the aside is accepted
	// while it is in flight, so the hub receipts `injected` and the loop hands
	// it back on its error path.
	arrived.recv().await.expect("second provider call");
	assert_eq!(
		manager
			.send(&id, "keep this aside".to_string())
			.expect("aside"),
		DeliveryOutcome::Injected
	);
	provider.gate.add_permits(1);
	let state = reconciled_after_aside(&manager, &id, "keep this aside").await;
	let roles: Vec<Role> = state.iter().map(|message| message.role).collect();
	assert_eq!(
		roles,
		vec![
			Role::System,
			Role::User,
			Role::Assistant,
			Role::Tool,
			Role::User,
		],
		"the completed tool step stays and the aside lands last: {state:?}"
	);
	assert_eq!(state[1].content, "first turn");
	assert_eq!(state[2].content, "step one");
	assert_eq!(state[2].tool_calls.len(), 1);
	assert_eq!(state[3].tool_call_id.as_deref(), Some("call-bash"));
	assert_eq!(state[4].content, "keep this aside");
	wait_for_transcript(&root, &id, baseline.len() + state.len() - 1).await;
	assert_accepted_tail_is_journaled_once(&root, &id, &state, "keep this aside", &baseline);
	manager.shutdown_actors().await;
	let _ = std::fs::remove_dir_all(&root);
}

/// An aside accepted by the *successful* step is journaled by the next boundary;
/// the failing step that follows must not write it again.
#[tokio::test]
async fn an_aside_accepted_in_a_successful_step_is_not_duplicated_by_a_later_failure() {
	let root = session_root("aside-in-successful-step");
	let (provider, mut arrived) = ToolStepProvider::new();
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		Arc::new(DefaultPersistedReviverFactory::new(
			provider.clone(),
			ToolRegistry::with_builtins(),
		)),
	);
	manager
		.adopt_live(
			&id,
			live_child(
				provider.clone(),
				&root,
				contract,
				vec![Message::system("child system prompt")],
			),
		)
		.expect("live adopt");
	let baseline = load_transcript(&transcript_path(&root, &id)).expect("baseline transcript");
	assert_eq!(
		manager.send(&id, "first turn".to_string()).expect("wake"),
		DeliveryOutcome::Woken
	);
	// The aside arrives while the first step is in flight and rides with its
	// successful result.
	arrived.recv().await.expect("first provider call");
	assert_eq!(
		manager
			.send(&id, "keep this aside".to_string())
			.expect("aside"),
		DeliveryOutcome::Injected
	);
	provider.gate.add_permits(1);
	// The next step fails after the aside is already on disk: appending nothing
	// is the only correct outcome.
	arrived.recv().await.expect("second provider call");
	provider.gate.add_permits(1);
	let state = reconciled_after_aside(&manager, &id, "keep this aside").await;
	let roles: Vec<Role> = state.iter().map(|message| message.role).collect();
	assert_eq!(
		roles,
		vec![
			Role::System,
			Role::User,
			Role::Assistant,
			Role::Tool,
			Role::User,
		],
		"the aside stays after the completed tool step: {state:?}"
	);
	assert_eq!(state[4].content, "keep this aside");
	wait_for_transcript(&root, &id, baseline.len() + state.len() - 1).await;
	assert_accepted_tail_is_journaled_once(&root, &id, &state, "keep this aside", &baseline);
	manager.shutdown_actors().await;
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn an_undurable_journal_blocks_destructive_compaction() {
	let root = session_root("durable-gate");
	let provider = gated_provider("turn-reply");
	provider.gate.add_permits(8);
	let seen: Arc<Mutex<Vec<(usize, bool)>>> = Arc::new(Mutex::new(Vec::new()));
	let observer = {
		let seen = Arc::clone(&seen);
		Arc::new(move |messages: &[Message], prune: Option<CompactionReport>| {
			seen.lock().expect("seen").push((messages.len(), prune.is_some()));
			// The transcript is not durable: nothing may be pruned.
			false
		}) as pi_agent::loop_module::TurnObserver
	};
	let agent_loop = AgentLoop::new(
		AgentConfig {
			max_turns: 4,
			// Any conversation is over budget, so a durable journal would prune.
			context_limit: 1,
			system_prompt: Some("child system prompt".to_string()),
			cwd: root.clone(),
		},
		provider,
		ToolRegistry::with_builtins(),
	)
	.with_turn_observer(Some(observer));
	let mut state = AgentState { messages: vec![Message::user("first question")], total_turns: 0 };
	agent_loop
		.run_dialog_message(Message::user("second question"), &mut state, None, None)
		.await
		.expect("dialog");
	let contents: Vec<&str> = state.messages.iter().map(|m| m.content.as_str()).collect();
	assert!(contents.contains(&"first question"));
	assert!(contents.contains(&"second question"));
	assert!(
		state.messages.iter().all(|message| !is_compaction_notice(message)),
		"a non-durable journal must not be compacted: {contents:?}"
	);
	let seen = seen.lock().expect("seen");
	assert_eq!(seen.len(), 1, "one boundary before the provider call: {seen:?}");
	assert_eq!(seen[0].1, false, "no compaction may be applied");
	assert_eq!(seen[0].0, 3, "the observer sees every accepted message");
}

#[tokio::test]
async fn a_failed_tombstone_is_reported_and_retryable() {
	let root = session_root("tombstone-retry");
	let provider = gated_provider("never-reply");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	manager
		.adopt_live(&id, live_child(provider.clone(), &root, contract, child_messages()))
		.expect("live adopt");
	// Make the session root unwritable: the durable marker cannot be created.
	std::fs::remove_dir_all(&root).expect("remove root");
	std::fs::write(&root, b"not a directory").expect("replace root with a file");
	match supervisor.cancel_child(&id, None).await {
		CancelAttempt::TombstoneFailed(error) => {
			assert!(!error.is_empty(), "the failure must be explained");
		},
		other => panic!("a failed durable cancel must not report success: {other:?}"),
	}
	assert!(!manager.is_live(&id), "the actor is stopped regardless");
	assert!(
		!manager.tombstone(&id, "hard abort"),
		"an unwritable root must not report a durable abort marker"
	);
	assert!(
		manager.is_tombstoned(&id),
		"the in-memory tombstone still blocks revocation in this process"
	);
	assert!(manager.send(&id, "resume".to_string()).is_err());
	manager.shutdown_actors().await;
	// Recovery: restore the root and retry the same cancel without an actor.
	std::fs::remove_file(&root).expect("remove placeholder");
	std::fs::create_dir_all(&root).expect("restore root");
	assert_eq!(
		supervisor.cancel_child(&id, None).await,
		CancelAttempt::Cancelled,
		"a repeated cancel must retry the durable marker"
	);
	assert!(tombstone_path(&root, &id).is_file(), "the marker is durable now");
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn retention_keeps_a_live_actor_record_and_its_journal() {
	let root = session_root("retention-live");
	let provider = gated_provider("never-reply");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	manager
		.adopt_live(&id, live_child(provider.clone(), &root, contract, child_messages()))
		.expect("live adopt");
	// Churn past the settled-record cap. The live actor's record owns the journal
	// path and the cancel handle, so evicting it would silently stop persistence
	// and make the actor unstoppable through the owner-scoped `hub cancel`.
	for index in 0..120 {
		let (filler, _token) = supervisor
			.reserve_child_full(ChildSpawn {
				name:                "filler",
				index:               index + 2,
				agent:               "scout",
				agent_source:        "builtin",
				task:                "filler",
				parent_id:           None,
				parent_tool_call_id: None,
				detached:            true,
				output_prefix:       None,
			})
			.expect("reserve filler");
		supervisor.unregister_active_child(&filler);
		supervisor.mark_completed(&filler, 1, "done");
		// Free the admission slot: a settled filler with no delivery still counts
		// toward `MAX_OUTSTANDING_RESULTS` until it is consumed, so churn without
		// this stops at the admission cap instead of reaching the record cap.
		supervisor.consume_results(std::slice::from_ref(&filler));
	}
	assert!(
		supervisor.children().iter().any(|record| record.id == id),
		"a registered live actor's record must survive retention"
	);
	assert_eq!(
		supervisor.append_child_transcript(&id, &[&Message::user("journal check")]),
		1,
		"a surviving live actor must still have a transcript path"
	);
	assert_eq!(
		supervisor.cancel_child(&id, None).await,
		CancelAttempt::Cancelled,
		"the owner-scoped cancel must still reach the live actor"
	);
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn cancel_tombstone_survives_a_restart_scan() {
	let root = session_root("cancel-durable");
	let provider = gated_provider("never-reply");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	manager
		.adopt_live(&id, live_child(provider.clone(), &root, contract, child_messages()))
		.expect("live adopt");
	assert_eq!(
		supervisor.cancel_child(&id, None).await,
		CancelAttempt::Cancelled
	);
	assert!(
		tombstone_path(&root, &id).is_file(),
		"cancel must persist its tombstone before reporting success"
	);
	// A restarted process scans the same root: the cancelled id must not come
	// back as a parked row that a sibling can revive.
	let restarted = test_supervisor(&root, provider.clone());
	let restarted_manager = attach(
		&restarted,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider),
	);
	assert!(restarted_manager.knows(&id), "the tombstone must be discovered");
	assert!(
		restarted_manager
			.send(&id, "resume".to_string())
			.is_err(),
		"a cancelled id must stay unmessageable after a restart"
	);
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn a_request_taken_before_the_stop_still_reaches_the_transcript() {
	// The receipt is already out when the stop lands. Whether the actor takes the
	// queued request before observing the cancellation (loop check) or leaves it
	// in the queue (exit drain), it may not disappear. Several rounds make the
	// select's branch choice irrelevant to the outcome.
	for round in 0..8 {
		let root = session_root(&format!("cancel-taken-{round}"));
		let provider = gated_provider("never-reply");
		let supervisor = test_supervisor(&root, provider.clone());
		let (id, contract) = adoptable_child(&supervisor, &root);
		let manager = attach(
			&supervisor,
			DEFAULT_AGENT_IDLE_TTL_MS,
			CountingReviver::new(provider.clone()),
		);
		manager
			.adopt_live(&id, live_child(provider.clone(), &root, contract, child_messages()))
			.expect("live adopt");
		// `send` does not yield, so the message is queued before the actor is
		// polled; the cancel then lands while the request is still live.
		assert_eq!(
			manager.send(&id, "taken before stop".to_string()).expect("wake"),
			DeliveryOutcome::Woken
		);
		manager.shutdown_actors().await;
		let mut transcript = String::new();
		for _ in 0..500 {
			transcript = std::fs::read_to_string(transcript_path(&root, &id))
				.unwrap_or_default();
			if transcript.contains("taken before stop") {
				break;
			}
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		assert!(
			transcript.contains("taken before stop"),
			"an accepted request must survive the stop (round {round}): {transcript}"
		);
		let _ = std::fs::remove_dir_all(&root);
	}
}

#[tokio::test]
async fn shutdown_actors_stops_idle_and_in_flight_follow_ups() {
	let root = session_root("shutdown-actor");
	let provider = gated_provider("shutdown-reply");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	manager
		.adopt_live(&id, live_child(provider.clone(), &root, contract, child_messages()))
		.expect("live adopt");

	// Idle actor: teardown must not wait for another message to observe it.
	tokio::time::timeout(Duration::from_secs(2), manager.shutdown_actors())
		.await
		.expect("idle actor must stop on teardown");
	assert!(!manager.is_live(&id));

	// Mid-turn actor: the follow-up request is gated, so only the session
	// cancellation can end it.
	assert_eq!(
		manager.send(&id, "wake".to_string()).expect("wake"),
		DeliveryOutcome::Revived
	);
	tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
		.await
		.expect("revived turn entered provider");
	tokio::time::timeout(Duration::from_secs(2), manager.shutdown_actors())
		.await
		.expect("in-flight actor must be cancelled by teardown");
	assert!(!manager.is_live(&id));
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn closing_session_refuses_sends_and_live_adoption() {
	let root = session_root("closing");
	let provider = gated_provider("never-reply");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);
	supervisor.shutdown();
	// No receipt may claim work for a session whose provider/registry are
	// already being released.
	let error = manager
		.send(&id, "wake".to_string())
		.expect_err("a closed session must refuse a send");
	assert!(error.contains("shutting down"), "{error}");
	assert!(!manager.is_live(&id));
	assert!(
		manager
			.adopt_live(&id, live_child(provider, &root, contract, child_messages()))
			.is_err(),
		"a closing session must not publish a live actor"
	);
	assert!(!manager.is_live(&id));
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn aborted_or_tombstoned_child_never_gets_a_live_actor() {
	let root = session_root("abort-gate");
	let provider = gated_provider("never-reply");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = reserved_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		CountingReviver::new(provider.clone()),
	);

	// The yield-abort is decided by the caller before publication, so no actor
	// may exist for it even though the loop returned a full turn list.
	supervisor.mark_aborted(&id, "yield aborted");
	assert!(
		manager
			.adopt_live(
				&id,
				live_child(provider.clone(), &root, contract.clone(), child_messages())
			)
			.is_err(),
		"an aborted child must not be published live"
	);
	assert!(!manager.is_live(&id));

	// A durable tombstone keeps refusing the same hand-over.
	manager.tombstone(&id, "hard kill");
	assert!(
		manager
			.adopt_live(&id, live_child(provider.clone(), &root, contract, child_messages()))
			.is_err(),
		"a tombstoned child must not be published live"
	);
	assert!(!manager.is_live(&id));
	let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn gone_workspace_is_transcript_only_and_refuses_revive() {
	let root = session_root("gone");
	let provider = gated_provider("never");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, mut contract) = adoptable_child(&supervisor, &root);
	contract.cwd = root.join("workspace-that-was-removed");
	contract.write(&root).expect("write invalid contract");
	assert!(
		PersistedChildSession::load(&root, &id).is_none(),
		"a gone workspace is not a usable contract"
	);

	let manager = attach(&supervisor, DEFAULT_AGENT_IDLE_TTL_MS, reviver_factory(provider));
	manager.adopt(&id, Some(contract)).expect("adopt");
	manager.park(&id);
	let outcome: Result<DeliveryOutcome, String> = manager.send(&id, "hello".to_string());
	let error = outcome.expect_err("revive must refuse a gone workspace");
	assert!(
		error.contains("no longer exists"),
		"exact refusal should name the workspace: {error}"
	);

	let router = InternalUrlRouter::new();
	hub_park::register_history(&router, manager).expect("history handler");
	let resource = router
		.resolve(
			&format!("history://{id}"),
			&history_scope(&root).resolve_context(&root, &CancellationToken::new()),
		)
		.await
		.expect("transcript remains readable");
	assert!(resource.content.contains("prior-answer"));
}


fn home_session_root(label: &str) -> PathBuf {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_nanos())
		.unwrap_or_default();
	let home = std::env::var_os("HOME")
		.map(PathBuf::from)
		.expect("HOME must be set");
	let dir = home.join(format!(
		".omp-hub-park-{label}-{}-{nanos}",
		std::process::id()
	));
	std::fs::create_dir_all(&dir).expect("session root");
	dir
}

fn scout_definition() -> AgentDefinition {
	AgentDefinition {
		name: "scout".to_string(),
		description: "test scout".to_string(),
		tools: None,
		spawns: None,
		model: vec!["@task".to_string()],
		output: None,
		thinking_level: None,
		blocking: false,
		read_summarize: None,
		prewalk: None,
		advisor: None,
		autoload_skills: Vec::new(),
		system_prompt: String::new(),
		source: AgentSource::Bundled,
		file_path: "embedded:scout.md".to_string(),
	}
}

#[tokio::test]
async fn actor_rewrites_only_yielding_follow_up_artifacts() {
	let root = home_session_root("actor-artifact");
	let provider = ScriptProvider::new(vec![
		Message::assistant_with_tools(
			"",
			vec![ToolCall {
				id:     "call-yield".to_string(),
				name:   "yield".to_string(),
				arguments: json!({ "result": { "data": { "value": "yielded" } } }),
			}],
		),
		Message::assistant("plain reply"),
	]);
	let supervisor = one_turn(provider.clone(), 2);
	supervisor.set_child_session_root(Some(root.clone()));
	let mut scope = UrlScope::default();
	scope
		.local_roots
		.insert("local".to_string(), root.clone());
	supervisor.set_url_scope(Arc::new(scope));
	supervisor.set_agent_catalog(Arc::new(AgentCatalog::new(
		vec![scout_definition()],
		None,
	)));
	let (id, mut contract) = adoptable_child(&supervisor, &root);
	contract.output_id = Some("Saved".to_string());
	contract.output_published = true;
	contract.write(&root).expect("published contract");
	std::fs::write(root.join("Saved.md"), "initial artifact").expect("initial artifact");
	let reviver: Arc<dyn PersistedChildReviverFactory> = Arc::new(
		DefaultPersistedReviverFactory::new(
			provider,
			ToolRegistry::with_builtins(),
		),
	);
	let manager = attach(&supervisor, 120, reviver);
	manager.adopt(&id, Some(contract)).expect("adopt");
	let hub = HubTool::root(supervisor.clone());
	let ctx = AgentContext::new(root.clone());

	let sent = hub
		.execute(&json!({ "op": "send", "to": id, "message": "yield" }), &ctx)
		.await
		.expect("hub send");
	let receipt = hub_details(sent).receipts.expect("receipts").remove(0);
	assert!(receipt.error.is_none(), "{:?}", receipt.error);
	let mut yielded = None;
	for _ in 0..300 {
		let body = std::fs::read_to_string(root.join("Saved.md")).unwrap_or_default();
		if body.contains("yielded") {
			yielded = Some(body);
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	let yielded = yielded.expect("yielding follow-up must rewrite Saved.md");

	let sent = hub
		.execute(&json!({ "op": "send", "to": id, "message": "plain" }), &ctx)
		.await
		.expect("second hub send");
	let receipt = hub_details(sent).receipts.expect("receipts").remove(0);
	assert!(receipt.error.is_none(), "{:?}", receipt.error);
	let mut replied = false;
	for _ in 0..300 {
		let messages = load_transcript(&transcript_path(&root, &id)).unwrap_or_default();
		if messages.iter().any(|message| message.content.contains("plain reply")) {
			replied = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(replied, "plain follow-up must run");
	assert_eq!(
		std::fs::read_to_string(root.join("Saved.md")).expect("artifact"),
		yielded,
		"yield-less follow-up must leave the prior artifact intact"
	);
	let _ = std::fs::remove_dir_all(&root);
}

fn contract_for(
	id: &str,
	root: &PathBuf,
	task: &str,
	tools: Vec<String>,
	agent: &str,
	depth: i32,
) -> PersistedChildSession {
	PersistedChildSession {
		id: id.to_string(),
		agent: Some(agent.to_string()),
		task: task.to_string(),
		system_prompt: "child system prompt".to_string(),
		tools,
		model_role: None,
		resolved_model: None,
		output_schema: None,
		output_schema_mode: None,
		restrict_tool_names: Some(true),
		spawns: Some("*".to_string()),
		read_summarize: None,
		advisor: None,
		read_only: None,
		cwd: root.clone(),
		depth,
		max_turns: 20,
		context_limit: 100_000,
		display_name: Some(id.to_string()),
		parent_id: Some("Main".to_string()),
		output_id: None,
		output_published: false,
	}
}

/// Replies from a fixed script; running out of replies fails the turn so a
/// surprise extra request is visible instead of hanging.
struct ScriptProvider {
	replies: Mutex<Vec<Message>>,
}

impl ScriptProvider {
	fn new(mut replies: Vec<Message>) -> Arc<Self> {
		replies.reverse();
		Arc::new(Self {
			replies: Mutex::new(replies),
		})
	}
}

#[async_trait]
impl ModelProvider for ScriptProvider {
	async fn complete(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		self.replies
			.lock()
			.expect("replies")
			.pop()
			.ok_or_else(|| ProviderError::Protocol("script exhausted".to_string()))
	}

	async fn stream(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		let message = self
			.replies
			.lock()
			.expect("replies")
			.pop()
			.ok_or_else(|| ProviderError::Protocol("script exhausted".to_string()))?;
		Ok(Box::pin(stream::iter(message_chunks(&message))))
	}
}

/// Enforces the provider-side rule a broken replay violates: every assistant
/// `tool_calls` id must be answered by the immediately following tool results.
struct PairingProvider {
	seen: Mutex<Vec<Message>>,
}

impl PairingProvider {
	fn new() -> Arc<Self> {
		Arc::new(Self {
			seen: Mutex::new(Vec::new()),
		})
	}

	fn saw(&self, body: &str) -> bool {
		self.seen
			.lock()
			.expect("seen")
			.iter()
			.any(|message| message.content.contains(body))
	}
}

#[async_trait]
impl ModelProvider for PairingProvider {
	async fn complete(
		&self,
		messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		for (index, message) in messages.iter().enumerate() {
			if message.role != Role::Assistant || message.tool_calls.is_empty() {
				continue;
			}
			for call in &message.tool_calls {
				let answered = messages[index + 1..]
					.iter()
					.take_while(|next| next.role == Role::Tool)
					.any(|next| next.tool_call_id.as_deref() == Some(call.id.as_str()));
				if !answered {
					return Err(ProviderError::Protocol(format!(
						"tool_use {} without tool_result",
						call.id
					)));
				}
			}
		}
		self.seen
			.lock()
			.expect("seen")
			.extend_from_slice(messages);
		Ok(Message::assistant("revived-ok"))
	}

	async fn stream(
		&self,
		_messages: &[Message],
		_tools: &[serde_json::Value],
		_tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		Err(ProviderError::Protocol(
			"revived runs use complete".to_string(),
		))
	}
}

fn one_turn(provider: Arc<dyn ModelProvider>, depth_limit: i32) -> Arc<Supervisor> {
	let supervisor = Supervisor::new(
		ToolRegistry::with_builtins(),
		provider,
		None,
		DelegationLimits {
			max_depth: depth_limit,
			max_concurrency: 32,
		},
	);
	supervisor
}

fn reserve_in(supervisor: &Arc<Supervisor>, name: &str) -> String {
	let (id, _token) = supervisor
		.reserve_child_full(ChildSpawn {
			name,
			index: 1,
			agent: "scout",
			agent_source: "builtin",
			task: "investigate",
			parent_id: None,
			parent_tool_call_id: Some("call-depth"),
			detached: false,
			output_prefix: None,
		})
		.expect("reserve child");
	id
}

fn task_call() -> Message {
	Message::assistant_with_tools(
		"",
		vec![ToolCall {
			id:     "call-task".to_string(),
			name:   "task".to_string(),
			arguments: json!({ "agent": "scout", "task": "nested" }),
		}],
	)
}

/// Blocker 1: a revived child must be rebuilt with the child-bound registry at
/// its recorded depth, not from the root registry.
#[tokio::test]
async fn revived_child_is_bound_at_its_recorded_depth() {
	// At max depth the child's registry has no `task` tool at all; a root
	// registry would still register one.
	let root = session_root("depth-max");
	let provider = ScriptProvider::new(vec![task_call(), Message::assistant("done")]);
	let supervisor = one_turn(provider.clone(), 1);
	supervisor.set_child_session_root(Some(root.clone()));
	let id = reserve_in(&supervisor, "depth-max");
	supervisor.append_child_transcript(
		&id,
		&[&Message::user("prior"), &Message::assistant("ok")],
	);
	let contract = contract_for(
		&id,
		&root,
		"investigate",
		vec!["task".to_string(), "yield".to_string()],
		"scout",
		1,
	);
	contract.write(&root).expect("write contract");
	let replayed = load_transcript(&transcript_path(&root, &id)).expect("transcript");
	let mut revived = DefaultPersistedReviverFactory::new(provider, ToolRegistry::with_builtins())
		.revive(&contract, replayed, Some(&supervisor))
		.expect("revive");
	let turns = revived
		.agent_loop
		.run_dialog_message(Message::user("go"), &mut revived.state, None, None)
		.await
		.expect("revived turn");
	assert!(!turns.is_empty());
	let result = &turns[0].tool_results[0];
	assert!(result.is_error, "task must not run at max depth");
	assert!(
		result.content.contains("Tool not found: task"),
		"a revived child at max depth keeps the depth guard: {}",
		result.content
	);

	// Below max depth the nested `task` tool keeps the agent context, so the
	// self-spawn guard fires instead of the root tool accepting the spawn.
	let root = session_root("depth-agent");
	let provider = ScriptProvider::new(vec![task_call(), Message::assistant("done")]);
	let supervisor = one_turn(provider.clone(), 2);
	supervisor.set_child_session_root(Some(root.clone()));
	let id = reserve_in(&supervisor, "depth-agent");
	supervisor.append_child_transcript(
		&id,
		&[&Message::user("prior"), &Message::assistant("ok")],
	);
	let contract = contract_for(
		&id,
		&root,
		"investigate",
		vec!["task".to_string(), "yield".to_string()],
		"scout",
		1,
	);
	let replayed = load_transcript(&transcript_path(&root, &id)).expect("transcript");
	let mut revived = DefaultPersistedReviverFactory::new(provider, ToolRegistry::with_builtins())
		.revive(&contract, replayed, Some(&supervisor))
		.expect("revive");
	let turns = revived
		.agent_loop
		.run_dialog_message(Message::user("go"), &mut revived.state, None, None)
		.await
		.expect("revived turn");
	let result = &turns[0].tool_results[0];
	assert!(
		result.content.contains("from within itself"),
		"the revived task tool must carry the child agent context: {}",
		result.content
	);
}

/// Blocker 1 (return channel): a revived child can actually call `yield`, the
/// tool only a child-bound registry mounts.
#[tokio::test]
async fn revived_child_can_yield() {
	let root = session_root("yield");
	let provider = ScriptProvider::new(vec![
		Message::assistant_with_tools(
			"",
			vec![ToolCall {
				id:     "call-yield".to_string(),
				name:   "yield".to_string(),
				arguments: json!({ "result": { "data": "done" } }),
			}],
		),
		Message::assistant("done"),
	]);
	let supervisor = one_turn(provider.clone(), 2);
	supervisor.set_child_session_root(Some(root.clone()));
	let id = reserve_in(&supervisor, "yield");
	supervisor.append_child_transcript(
		&id,
		&[&Message::user("prior"), &Message::assistant("ok")],
	);
	let contract = contract_for(&id, &root, "investigate", vec!["yield".to_string()], "scout", 1);
	let replayed = load_transcript(&transcript_path(&root, &id)).expect("transcript");
	let mut revived = DefaultPersistedReviverFactory::new(provider, ToolRegistry::with_builtins())
		.revive(&contract, replayed, Some(&supervisor))
		.expect("revive");
	let turns = revived
		.agent_loop
		.run_dialog_message(Message::user("go"), &mut revived.state, None, None)
		.await
		.expect("revived turn");
	let result = &turns[0].tool_results[0];
	assert!(!result.is_error, "yield must exist: {}", result.content);
	assert!(result.terminate, "yield terminates the turn");
}

/// Blocker 2: the journal records tool results with their calls, and a legacy
/// transcript with dangling calls is stripped so the first revived request is
/// not rejected.
#[tokio::test]
async fn revived_replay_pairs_tool_calls_with_results() {
	let root = session_root("replay");
	let provider = PairingProvider::new();
	let supervisor = one_turn(provider.clone(), 2);
	supervisor.set_child_session_root(Some(root.clone()));
	let id = reserve_in(&supervisor, "replay");
	let journaled = turn_journal_messages(&[TurnResult {
		message:       Message::assistant_with_tools(
			"working",
			vec![ToolCall {
				id:     "call-read".to_string(),
				name:   "read".to_string(),
				arguments: json!({ "path": "a" }),
			}],
		),
		tool_results:  vec![ToolResult {
			tool_call_id: "call-read".to_string(),
			content:      "file body".to_string(),
			is_error:     false,
			terminate:    false,
			details:      None,
		}],
		is_completed:  false,
	}]);
	let journaled_refs: Vec<&Message> = journaled.iter().collect();
	supervisor.append_child_transcript(&id, &journaled_refs);
	// Legacy/truncated tail: an assistant call with no result on disk.
	supervisor.append_child_transcript(
		&id,
		&[&Message::assistant_with_tools(
			"legacy",
			vec![ToolCall {
				id:     "stale-call".to_string(),
				name:   "read".to_string(),
				arguments: json!({ "path": "b" }),
			}],
		)],
	);

	let replayed = load_transcript(&transcript_path(&root, &id)).expect("transcript");
	assert!(
		replayed
			.iter()
			.filter(|message| message.role == Role::Assistant)
			.flat_map(|message| message.tool_calls.iter())
			.all(|call| call.id != "stale-call"),
		"an unanswered call must not be replayed"
	);
	assert!(
		replayed
			.iter()
			.any(|message| message.role == Role::Tool && message.content == "file body"),
		"the tool result is part of the journal"
	);

	let contract = contract_for(
		&id,
		&root,
		"prior work",
		vec!["read".to_string(), "yield".to_string()],
		"scout",
		1,
	);
	let mut revived = DefaultPersistedReviverFactory::new(provider.clone(), ToolRegistry::with_builtins())
		.revive(&contract, replayed, Some(&supervisor))
		.expect("revive");
	let turns = revived
		.agent_loop
		.run_dialog_message(Message::user("go"), &mut revived.state, None, None)
		.await
		.expect("a revived tool-using child must not be rejected");
	assert_eq!(turns.len(), 1);
	assert!(
		provider.saw("file body"),
		"the revived child keeps its tool context"
	);
}

/// Major 3: a restored parked `task-N` must never collide with a fresh spawn
/// after the supervisor's sequence restarts.
#[tokio::test]
async fn resumed_parked_child_does_not_collide_with_a_fresh_spawn() {
	let root = session_root("resume");
	let provider = gated_provider("unused");
	let supervisor = test_supervisor(&root, provider.clone());
	let (id, contract) = adoptable_child(&supervisor, &root);
	let manager = attach(
		&supervisor,
		DEFAULT_AGENT_IDLE_TTL_MS,
		reviver_factory(provider),
	);
	// `attach` scanned the spawn-time contract as a restored parked row.
	assert_eq!(manager.status(&id), Some(LifecycleStatus::Parked));

	let mut different = contract.clone();
	different.task = "a different spawn".to_string();
	different.display_name = Some("different".to_string());
	assert!(
		manager.adopt(&id, Some(different)).is_err(),
		"a different contract must not replace a restored parked row"
	);
	manager
		.adopt(&id, Some(contract))
		.expect("the same session may be re-adopted");
	assert_eq!(manager.status(&id), Some(LifecycleStatus::Idle));
	drop(manager);
	drop(supervisor);

	// A fresh supervisor on the same stable session directory restarts ids at 1.
	let restarted_provider = gated_provider("unused-restart");
	let restarted = test_supervisor(&root, restarted_provider.clone());
	let restarted_manager = attach(
		&restarted,
		DEFAULT_AGENT_IDLE_TTL_MS,
		reviver_factory(restarted_provider),
	);
	assert_eq!(
		restarted_manager.status(&id),
		Some(LifecycleStatus::Parked),
		"the parked row is restored from disk"
	);
	let fresh_id = reserve_in(&restarted, "fresh");
	assert_ne!(fresh_id, id, "a resumed parked id must never be reused");
	restarted.append_child_transcript(
		&fresh_id,
		&[&Message::user("fresh work"), &Message::assistant("fresh-answer")],
	);
	let fresh_contract = contract_for(
		&fresh_id,
		&root,
		"fresh work",
		vec!["read".to_string()],
		"scout",
		1,
	);
	fresh_contract.write(&root).expect("fresh contract");
	restarted.mark_completed(&fresh_id, 3, "fresh-answer");
	restarted_manager
		.adopt(&fresh_id, Some(fresh_contract))
		.expect("adopt the fresh child");

	let roster = restarted_manager.roster();
	assert!(roster
		.iter()
		.any(|row| row.id == id && row.status == LifecycleStatus::Parked));
	assert!(roster
		.iter()
		.any(|row| row.id == fresh_id && row.status == LifecycleStatus::Idle));
	let restored = PersistedChildSession::load(&root, &id).expect("restored contract");
	assert_eq!(
		restored.task, "investigate the repo",
		"the parked child's contract is untouched"
	);

	// Minor 4: a tombstone restored after a restart has no roster entry, yet
	// `history://` must still find its transcript on disk.
	assert!(restarted_manager.tombstone(&id, "hard kill"));
	drop(restarted_manager);
	drop(restarted);
	let after_provider = gated_provider("unused-after");
	let after = test_supervisor(&root, after_provider.clone());
	let after_manager = attach(
		&after,
		DEFAULT_AGENT_IDLE_TTL_MS,
		reviver_factory(after_provider),
	);
	assert_eq!(
		after_manager.status(&id),
		None,
		"a tombstoned id has no roster row"
	);
	assert!(
		after_manager.transcript_path(&id).is_some(),
		"history:// falls back to the transcript on disk"
	);
}
