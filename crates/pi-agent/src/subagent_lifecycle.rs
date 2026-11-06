//! Subagent park/revive lifecycle owner.
//!
//! Mirrors `packages/coding-agent/src/registry/agent-lifecycle.ts` for the
//! subset the Rust supervisor can support without touching its settle,
//! accounting, or retention paths:
//!
//! - `adopt` takes a finished keep-alive child (`idle`) and keeps its record,
//!   transcript, and persisted contract.
//! - `adopt_live` is the same handover for a child whose loop and state still
//!   exist: the row becomes `idle` with a live actor, exactly like TS keep-alive
//!   (`finalizeSubagentLifecycle` sets `idle` on the live session), so a send
//!   wakes the same in-memory conversation (`woken`) instead of rebuilding it.
//! - an idle TTL (default 420_000 ms, upstream `task/agentIdleTtlMs`) parks it:
//!   the live actor is cancelled, the transcript and contract stay on disk.
//! - `ensureLive`/`send` revives a parked child from the persisted contract and
//!   JSONL replay, coalescing concurrent revives into one live session.
//! - a hard kill tombstones the id: its transcript stays readable through
//!   `history://`, but it can never be messaged or revived again.
//! - every actor derives its token from the supervisor's session token and is
//!   registered there, so `hub cancel` reaches a follow-up turn and scope
//!   teardown cancels and joins the actor before session resources are released.
//! - `hub cancel` of a live session is terminal for that id (TS
//!   `cancelAgentRegistration` releases the registration): the actor stops under
//!   the same lock `send` uses, messages already accepted are journaled, and
//!   later sends are refused while `history://` keeps working. Only a marker
//!   whose file *and* directory entry are fsynced is reported as a durable
//!   cancel; a failed marker is reported and a repeated cancel retries it.
//! - a revival from persisted state restores the supervisor record from the
//!   contract verified inside this manager's root, so the journal path and
//!   owner-scoped cancellation work after a restart or a record eviction. The
//!   restore runs before the reviver builds the child registry (the child's
//!   `local://` root comes from the record) and fails closed when the contract,
//!   the transcript location, or the ownership check does not match.
//! - actor publication is atomic against cancellation: `cancel` bumps a
//!   generation before it decides, and an adoption/revive claims the actor under
//!   the same lock using a supervisor revive reservation taken before the record
//!   was read. The claim also consumes the reservation, which is what keeps
//!   retention from evicting the record while the reviver binds the registry.
//!   The claim runs before the live slot is published, so a failed claim leaves
//!   no slot a concurrent send could read as live.
//! - the journal is the conversation the loop actually accepted (assistant/tool
//!   pairs plus accepted steering/asides), written at every turn boundary
//!   before compaction can prune, so a revive replays the same context. When a
//!   flush is not fully durable the loop skips compaction instead of dropping
//!   an unwritten turn. The actor never rewrites that state after a failed turn
//!   — each completed step is journaled at its boundary, before the next request
//!   can fail, and shortening the conversation would put the cursor past it —
//!   so the append-only log stays in step with the cursor; a shrunk
//!   conversation refuses durability rather than claim a line is on disk.
//!
//! The manager holds only a [`Weak<Supervisor>`]; all state it needs comes from
//! the supervisor's existing read accessors plus its own files. The supervisor's
//! record is never mutated, so the five-minute retention path keeps working for
//! normal jobs.

use std::{
	collections::{HashMap, HashSet, VecDeque},
	fs,
	io::Write,
	path::{Path, PathBuf},
	sync::{
		Arc, LazyLock, Mutex, Weak,
		atomic::{AtomicU64, AtomicUsize, Ordering},
	},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::sync::mpsc;

use crate::{
	compaction::CompactionReport,
	state::{ContentPart, Message, Role},
	supervisor::Supervisor,
	task_persisted_revive::{
		PersistedChildReviverFactory, PersistedChildSession, RevivedChild, SESSION_INIT_SUFFIX,
		TOMBSTONE_SUFFIX, load_transcript, tombstone_path, transcript_path,
	},
};

/// Upstream `task/agentIdleTtlMs` default.
pub const DEFAULT_AGENT_IDLE_TTL_MS: u64 = 420_000;

/// How long teardown waits for a cancelled lifecycle actor before aborting it.
const ACTOR_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_millis() as u64)
		.unwrap_or_default()
}

/// Append one emitted producer event to a live history snapshot.
///
/// `MessageEnd` + `ToolEnd` are the authoritative per-message stream and keep
/// their emission order. `TurnEnd` repeats the same assistant/tool pair and is
/// ignored so nothing is rendered twice. `UserSteer` is emitted only after the
/// steering message entered history, and `BackgroundResult` only after the
/// result was injected, so both are real accepted messages.
fn record_live_event(messages: &mut Vec<Message>, event: &crate::loop_module::AgentEvent) {
	match event {
		crate::loop_module::AgentEvent::MessageEnd { message, .. } => messages.push(message.clone()),
		crate::loop_module::AgentEvent::ToolEnd { id, name, result, is_error, .. } => {
			messages.push(Message::tool_with_status(
				id.clone(),
				name.clone(),
				result.clone(),
				*is_error,
			));
		},
		crate::loop_module::AgentEvent::UserSteer { parts } => {
			messages.push(Message::user_with_parts(parts.clone()));
		},
		crate::loop_module::AgentEvent::BackgroundResult { text, .. } => {
			messages.push(Message::user(text.clone()));
		},
		_ => {},
	}
}

/// Lifecycle status exposed by the roster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleStatus {
	Idle,
	Parked,
	Running,
	Tombstoned,
}

impl LifecycleStatus {
	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Idle => "idle",
			Self::Parked => "parked",
			Self::Running => "running",
			Self::Tombstoned => "tombstoned",
		}
	}
}

/// How a message reached a lifecycle-managed agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryOutcome {
	/// The live session accepted the message as a non-interrupting aside in its
	/// running turn (or in the next step boundary of the turn about to start).
	Injected,
	/// The message woke an idle live actor: it started a real turn on the
	/// session's own in-memory [`crate::state::AgentState`], not a rebuilt one.
	Woken,
	/// The parked row was rebuilt from disk and the message started a turn.
	Revived,
}

impl DeliveryOutcome {
	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Injected => "injected",
			Self::Woken => "woken",
			Self::Revived => "revived",
		}
	}
}

/// One roster row for a lifecycle-managed agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleRow {
	pub id:            String,
	pub display_name:  String,
	pub parent_id:     Option<String>,
	pub status:        LifecycleStatus,
	pub unread:        usize,
	pub last_activity: u64,
	pub session_file:  PathBuf,
}

/// Read-only row for the `history://` index and lookup.
///
/// Unlike [`LifecycleRow`] this includes tombstoned ids: their transcript stays
/// readable even though `hub list` no longer shows them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRow {
	pub id:            String,
	pub status:        LifecycleStatus,
	pub parent_id:     Option<String>,
	/// Agent definition recorded in the contract, when one is known.
	pub agent:         Option<String>,
	pub last_activity: u64,
	pub session_file:  PathBuf,
}

struct Entry {
	display_name:  String,
	parent_id:     Option<String>,
	status:        LifecycleStatus,
	session_file:  PathBuf,
	contract:      Option<PersistedChildSession>,
	/// True when this row was restored from disk, not adopted in-process. A
	/// restored row is pinned to its transcript path: a later spawn that reuses
	/// the bare `task-N` id must not replace it.
	restored:      bool,
	/// Invalidates stale timer/actor callbacks after a park or revive.
	epoch:         u64,
	last_activity: u64,
}

struct LiveSession {
	/// Idle wake queue: each message starts its own turn on the owned state.
	inbound:  mpsc::Sender<String>,
	/// Busy aside queue: a mid-turn message joins the running dialog at its
	/// next step boundary (TS `deliverIrcMessage` non-interrupting aside).
	steering: mpsc::Sender<crate::loop_module::SteeringEvent>,
	cancel:   tokio_util::sync::CancellationToken,
	epoch:    u64,
}

struct LifecycleState {
	entries:    HashMap<String, Entry>,
	live:       HashMap<String, LiveSession>,
	tombstones: HashSet<String>,
	/// Producer-fed message snapshots for live/revived sessions. Owned by the
	/// manager so `history://` can serve an in-memory view before the JSONL
	/// flush; dropped when live ownership ends (park/tombstone/finish/adopt).
	history:    HashMap<String, Arc<Mutex<Vec<Message>>>>,
}

/// Park/revive owner for one supervisor.
pub struct SubagentLifecycleManager {
	supervisor:  Weak<Supervisor>,
	root:        Option<PathBuf>,
	idle_ttl_ms: AtomicU64,
	reviver:     Arc<dyn PersistedChildReviverFactory>,
	state:       Mutex<LifecycleState>,
	/// One gate per id: a second `send` that arrives while a revive is in flight
	/// waits here and then delivers into the session the winner published
	/// (TS coalesces revives instead of failing the loser). Entries are never
	/// removed — dropping one would let two revives use different gates.
	gates:       Mutex<HashMap<String, Arc<Mutex<()>>>>,
	/// Running actor tasks, so scope teardown can cancel and join them before
	/// the session's provider/registry/tools are released.
	actors:      Arc<ActorTracker>,
}

/// Counter plus abort handles for running lifecycle actors.
///
/// A lease is held by each spawned task; its `Drop` decrements the counter even
/// if the task panics, so [`SubagentLifecycleManager::shutdown_actors`] can
/// always return. Abort handles are pruned on insert and drained on shutdown.
#[derive(Default)]
struct ActorTracker {
	running: AtomicUsize,
	done:    tokio::sync::Notify,
	aborts:  Mutex<Vec<tokio::task::AbortHandle>>,
}

impl ActorTracker {
	fn enter(self: &Arc<Self>) -> ActorLease {
		self.running.fetch_add(1, Ordering::SeqCst);
		ActorLease { tracker: Arc::clone(self) }
	}

	fn track(&self, abort: tokio::task::AbortHandle) {
		let mut aborts = lock(&self.aborts);
		aborts.retain(|abort| !abort.is_finished());
		aborts.push(abort);
	}

	fn abort_all(&self) {
		let mut aborts = lock(&self.aborts);
		for abort in aborts.drain(..) {
			abort.abort();
		}
	}
}

struct ActorLease {
	tracker: Arc<ActorTracker>,
}

impl Drop for ActorLease {
	fn drop(&mut self) {
		self.tracker.running.fetch_sub(1, Ordering::SeqCst);
		// `notify_one` stores a permit when nobody waits, so a completion that
		// races the shutdown waiter cannot be lost.
		self.tracker.done.notify_one();
	}
}

/// Incremental transcript writer for one child session.
///
/// The owner calls [`Self::flush`] at every turn boundary the loop reports (the
/// call before compaction is the durable one) and once after the dialog, so a
/// prune can only drop messages already on disk. The cursor is an index into
/// the current conversation; after a rewrite it is remapped with the compactor's
/// [`CompactionReport`], never inferred from lengths (a later turn can restore
/// the old length and would then skip or repeat accepted messages).
pub struct TranscriptJournal {
	supervisor: Arc<Supervisor>,
	child_id:   String,
	/// Conversation index where the accepted tail starts on the first flush,
	/// before the loop may insert a system message at index 0.
	base:       usize,
	had_system: bool,
	written:    Mutex<Option<usize>>,
}

impl TranscriptJournal {
	/// Journal for a run whose accepted tail starts at `base`.
	pub fn new(
		supervisor: Arc<Supervisor>,
		child_id: String,
		base: usize,
		had_system: bool,
	) -> Self {
		Self { supervisor, child_id, base, had_system, written: Mutex::new(None) }
	}

	/// Append every accepted message not yet on disk, or remap the cursor after
	/// a compaction.
	///
	/// Returns whether the conversation is durable up to the last accepted
	/// message. `false` means the caller must not take a destructive step
	/// (compaction) that would drop an unwritten message; the next call retries
	/// from that index.
	pub fn flush(&self, messages: &[Message], prune: Option<CompactionReport>) -> bool {
		let mut cursor = lock(&self.written);
		if let Some(report) = prune {
			// The pre-compaction call journaled the old head; this view is only
			// a different index space for the same conversation.
			if let Some(written) = *cursor {
				*cursor = Some(report.remap(written).min(messages.len()));
			}
			return true;
		}
		let start = match *cursor {
			None => crate::tools::task::journal_start(messages, self.base, self.had_system),
			Some(written) if written <= messages.len() => written,
			// The conversation is shorter than this append-only log already wrote.
			// Everything below the old cursor is on disk, but the memory tail
			// cannot be placed after it without duplicating the superseded lines,
			// so the journal cannot claim the conversation is durable: report it
			// and keep the cursor so every later flush refuses a compaction that
			// would drop a message. The owner never rewrites the tail (`subagent_
			// lifecycle` journals the state exactly as the loop left it), so this
			// is an invariant violation, not a routine path.
			Some(written) => {
				eprintln!(
					"warning: child transcript for {} was rewritten shorter ({} < {written}); \
					 refusing compaction: the journal cannot represent the new tail",
					self.child_id,
					messages.len()
				);
				return false;
			},
		};
		if start < messages.len() {
			let appended: Vec<&Message> = messages[start..].iter().collect();
			let expected = appended.len();
			let written = self
				.supervisor
				.append_child_transcript(&self.child_id, &appended);
			// Advance only by what actually reached the file: a partial append or
			// a missing record is retried from that index, never silently skipped.
			*cursor = Some(start + written);
			return written == expected;
		}
		*cursor = Some(messages.len());
		true
	}
}

static MANAGERS: LazyLock<Mutex<HashMap<String, Weak<SubagentLifecycleManager>>>> =
	LazyLock::new(|| Mutex::new(HashMap::new()));

fn managers() -> &'static Mutex<HashMap<String, Weak<SubagentLifecycleManager>>> {
	&MANAGERS
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
	mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// Persist a tombstone sidecar so a restarted process cannot re-adopt `id`.
///
/// Fail-closed: the file content and the directory entry that names it are both
/// synced before the caller reports a cancel. An unsynced marker can vanish on
/// a crash and revive a cancelled child, so any create/write/sync error is
/// returned instead of being swallowed.
fn write_tombstone(root: &Path, id: &str, reason: &str) -> Result<(), String> {
	let path = tombstone_path(root, id);
	let mut file = fs::File::create(&path)
		.map_err(|error| format!("cannot create tombstone {}: {error}", path.display()))?;
	file
		.write_all(reason.as_bytes())
		.map_err(|error| format!("cannot write tombstone {}: {error}", path.display()))?;
	file
		.sync_all()
		.map_err(|error| format!("cannot sync tombstone {}: {error}", path.display()))?;
	// The file's *name* lives in the directory: without a directory fsync a crash
	// can drop the entry even though the inode was synced.
	fs::File::open(root)
		.and_then(|dir| dir.sync_all())
		.map_err(|error| {
			format!(
				"cannot sync session root {} for tombstone {}: {error}",
				root.display(),
				path.display()
			)
		})
}

/// Attach a manager to `supervisor` and register it for hub delegation.
///
/// Existing `*.init.json` contracts in the supervisor's child-session root are
/// scanned as `parked` rows, so a restarted process restores the same roster.
pub fn attach(
	supervisor: &Arc<Supervisor>,
	idle_ttl_ms: u64,
	reviver: Arc<dyn PersistedChildReviverFactory>,
) -> Arc<SubagentLifecycleManager> {
	let root = supervisor.child_session_root();
	let manager = Arc::new(SubagentLifecycleManager {
		supervisor: Arc::downgrade(supervisor),
		root,
		idle_ttl_ms: AtomicU64::new(idle_ttl_ms),
		reviver,
		actors: Arc::new(ActorTracker::default()),
		state: Mutex::new(LifecycleState {
			entries:    HashMap::new(),
			live:       HashMap::new(),
			tombstones: HashSet::new(),
			history:    HashMap::new(),
		}),
		gates: Mutex::new(HashMap::new()),
	});
	if let Some(root) = manager.root.clone() {
		manager.scan_persisted(&root);
	}
	let mut registry = lock(managers());
	registry.retain(|_, weak| weak.strong_count() > 0);
	registry.insert(supervisor.instance_key().to_string(), Arc::downgrade(&manager));
	manager
}

/// Manager attached to `supervisor`, if any.
#[must_use]
pub fn lookup(supervisor: &Supervisor) -> Option<Arc<SubagentLifecycleManager>> {
	let key = supervisor.instance_key();
	let mut registry = lock(managers());
	registry.retain(|_, weak| weak.strong_count() > 0);
	registry.get(key).and_then(|weak| weak.upgrade())
}

impl SubagentLifecycleManager {
	/// Directory child contracts/transcripts live in.
	#[must_use]
	pub fn root(&self) -> Option<&Path> {
		self.root.as_deref()
	}

	/// Current lifecycle status for `id`.
	#[must_use]
	pub fn status(&self, id: &str) -> Option<LifecycleStatus> {
		lock(&self.state).entries.get(id).map(|entry| entry.status)
	}

	/// Whether this manager owns a row (including a tombstone) for `id`.
	#[must_use]
	pub fn knows(&self, id: &str) -> bool {
		let state = lock(&self.state);
		state.entries.contains_key(id) || state.tombstones.contains(id)
	}

	/// Transcript path for `id`, including tombstoned ids (`history://`).
	///
	/// A row restored after a restart has no live entry when it was tombstoned,
	/// so fall back to the on-disk transcript the same way the very first
	/// `history://<id>` read does.
	#[must_use]
	pub fn transcript_path(&self, id: &str) -> Option<PathBuf> {
		let known = lock(&self.state)
			.entries
			.get(id)
			.map(|entry| entry.session_file.clone());
		if known.is_some() {
			return known;
		}
		let path = transcript_path(self.root.as_deref()?, id);
		path.is_file().then_some(path)
	}

	/// Rows the `history://` index and lookup may expose.
	///
	/// Includes rows `roster` filters out (tombstoned ids keep a readable
	/// transcript) and synthesizes a row for a restored tombstone marker whose
	/// entry is gone. Advisor ids are filtered by the caller, which owns the
	/// transcript namespace rules.
	#[must_use]
	pub fn history_rows(&self) -> Vec<HistoryRow> {
		let state = lock(&self.state);
		let mut rows: Vec<HistoryRow> = state
			.entries
			.iter()
			.map(|(id, entry)| HistoryRow {
				id:            id.clone(),
				status:        entry.status,
				parent_id:     entry.parent_id.clone(),
				agent:         entry
					.contract
					.as_ref()
					.and_then(|contract| contract.agent.clone()),
				last_activity: entry.last_activity,
				session_file:  entry.session_file.clone(),
			})
			.collect();
		if let Some(root) = self.root.as_deref() {
			for id in &state.tombstones {
				if state.entries.contains_key(id) {
					continue;
				}
				rows.push(HistoryRow {
					id:            id.clone(),
					status:        LifecycleStatus::Tombstoned,
					parent_id:     None,
					agent:         None,
					last_activity: 0,
					session_file:  transcript_path(root, id),
				});
			}
		}
		rows
	}

	/// Supervisor child records, for live rows not yet adopted by this manager.
	#[must_use]
	pub fn child_records(&self) -> Vec<crate::supervisor::ChildRecord> {
		self.supervisor
			.upgrade()
			.map_or_else(Vec::new, |supervisor| supervisor.children())
	}

	/// Seed the live snapshot with the exact producer-owned initial message.
	pub fn seed_live(&self, id: &str, message: Message) {
		let slot = {
			let mut state = lock(&self.state);
			Arc::clone(
				state
					.history
					.entry(id.to_string())
					.or_insert_with(|| Arc::new(Mutex::new(Vec::new()))),
			)
		};
		lock(&slot).push(message);
	}

	/// Append one already-accepted message to the live snapshot.
	pub fn append_live_message(&self, id: &str, message: Message) {
		let slot = {
			let state = lock(&self.state);
			state.history.get(id).cloned()
		};
		if let Some(slot) = slot {
			lock(&slot).push(message);
		}
	}

	/// Apply one emitted producer event to the live snapshot.
	///
	/// Callers apply the event BEFORE `Supervisor::report_event`, so accounting
	/// still observes it exactly once.
	pub fn apply_live_event(&self, id: &str, event: &crate::loop_module::AgentEvent) {
		let slot = {
			let state = lock(&self.state);
			state.history.get(id).cloned()
		};
		if let Some(slot) = slot {
			record_live_event(&mut lock(&slot), event);
		}
	}

	/// Replace the live snapshot with the final authoritative state.
	///
	/// A no-op when live ownership already ended, so a late reconcile can never
	/// resurrect a parked/tombstoned view.
	pub fn replace_live(&self, id: &str, messages: Vec<Message>) {
		let slot = {
			let state = lock(&self.state);
			state.history.get(id).cloned()
		};
		if let Some(slot) = slot {
			*lock(&slot) = messages;
		}
	}

	/// Drop the producer-owned snapshot when the run ends without a live actor.
	///
	/// A keep-alive child adopted as a live actor keeps its snapshot: the actor
	/// owns the in-memory view from then on, and only park/tombstone/finish may
	/// drop it. This guard keeps the run-scoped drop from wiping it.
	pub fn drop_live(&self, id: &str) {
		let mut state = lock(&self.state);
		if !state.live.contains_key(id) {
			state.history.remove(id);
		}
	}

	/// Whether a live actor currently owns `id`.
	#[must_use]
	pub fn is_live(&self, id: &str) -> bool {
		lock(&self.state).live.contains_key(id)
	}

	/// The single-flight gate for `id`'s revive/adopt.
	///
	/// Held across the whole revive (restore, reviver, claim, publish), so a
	/// concurrent send waits and then delivers into the published session instead
	/// of starting a second revive or failing.
	fn revive_gate(&self, id: &str) -> Arc<Mutex<()>> {
		let mut gates = lock(&self.gates);
		Arc::clone(gates.entry(id.to_string()).or_default())
	}

	/// Ids whose live session is executing a turn (TS `isRunning`: streaming).
	///
	/// Idle and parked sessions are excluded: they must not hold a `hub wait`
	/// open until its timeout.
	#[must_use]
	pub fn running_ids(&self) -> Vec<String> {
		let state = lock(&self.state);
		state
			.live
			.keys()
			.filter(|id| {
				state.entries.get(*id).map(|entry| entry.status)
					== Some(LifecycleStatus::Running)
			})
			.cloned()
			.collect()
	}

	/// Every live keep-alive session (`running` or `idle`, TS `listVisibleTo`).
	///
	/// Flat namespace: messaging is not owner-scoped, so a broadcast reaches any
	/// live peer, not just this hub's direct children.
	#[must_use]
	pub fn live_ids(&self) -> Vec<String> {
		let state = lock(&self.state);
		let mut ids: Vec<String> = state.live.keys().cloned().collect();
		ids.sort_unstable();
		ids
	}

	/// Read the live snapshot, cloned outside every manager lock.
	#[must_use]
	pub fn live_messages(&self, id: &str) -> Option<Vec<Message>> {
		let slot = {
			let state = lock(&self.state);
			state.history.get(id).cloned()
		};
		slot.map(|slot| lock(&slot).clone())
	}

	/// Epoch-scoped variants for the revived actor: a parked/re-revived session
	/// must never be clobbered by a late event from the previous actor.
	fn snapshot_for_epoch(&self, id: &str, epoch: u64) -> Option<Arc<Mutex<Vec<Message>>>> {
		let state = lock(&self.state);
		state
			.live
			.get(id)
			.filter(|live| live.epoch == epoch)?;
		state.history.get(id).cloned()
	}

	pub fn append_live_message_for_epoch(&self, id: &str, epoch: u64, message: Message) {
		if let Some(slot) = self.snapshot_for_epoch(id, epoch) {
			lock(&slot).push(message);
		}
	}

	pub fn apply_live_event_for_epoch(
		&self,
		id: &str,
		epoch: u64,
		event: &crate::loop_module::AgentEvent,
	) {
		if let Some(slot) = self.snapshot_for_epoch(id, epoch) {
			record_live_event(&mut lock(&slot), event);
		}
	}

	pub fn replace_live_for_epoch(&self, id: &str, epoch: u64, messages: Vec<Message>) {
		if let Some(slot) = self.snapshot_for_epoch(id, epoch) {
			*lock(&slot) = messages;
		}
	}

	/// Roster rows, newest activity first within each status.
	#[must_use]
	pub fn roster(&self) -> Vec<LifecycleRow> {
		let mailbox = self.supervisor.upgrade().map(|supervisor| supervisor.mailbox());
		let state = lock(&self.state);
		let mut rows: Vec<LifecycleRow> = state
			.entries
			.iter()
			.filter(|(_, entry)| entry.status != LifecycleStatus::Tombstoned)
			.map(|(id, entry)| LifecycleRow {
				id:            id.clone(),
				display_name:  entry.display_name.clone(),
				parent_id:     entry.parent_id.clone(),
				status:        entry.status,
				unread:        mailbox.as_ref().map_or(0, |mailbox| mailbox.unread_count(id)),
				last_activity: entry.last_activity,
				session_file:  entry.session_file.clone(),
			})
			.collect();
		rows.sort_by(|left, right| {
			right
				.last_activity
				.cmp(&left.last_activity)
				.then_with(|| left.id.cmp(&right.id))
		});
		rows
	}

	/// Adopt a finished keep-alive child as `idle`.
	///
	/// The record supplies the real transcript path; `contract` is the
	/// `session_init`-equivalent the executor captured at spawn. When `None`, the
	/// manager tries the persisted file, and otherwise keeps the row
	/// transcript-only (revive will refuse).
	pub fn adopt(
		self: &Arc<Self>,
		id: &str,
		contract: Option<PersistedChildSession>,
	) -> Result<(), String> {
		let supervisor = self
			.supervisor
			.upgrade()
			.ok_or_else(|| "supervisor is gone".to_string())?;
		let record = supervisor
			.children()
			.into_iter()
			.find(|record| record.id == id)
			.ok_or_else(|| format!("unknown child \"{id}\""))?;
		if record.status == crate::supervisor::ChildStatus::Aborted {
			return Err(format!(
				"Agent \"{id}\" was hard-aborted and cannot be adopted or revived."
			));
		}
		let session_file = record
			.session_file
			.clone()
			.ok_or_else(|| format!("child \"{id}\" has no transcript file"))?;
		let root = session_file
			.parent()
			.map(Path::to_path_buf)
			.ok_or_else(|| format!("transcript {} has no parent directory", session_file.display()))?;
		// A resumed session restores `task-N` rows from disk while the fresh
		// supervisor's `next_seq` restarts at 1. The executor skips ids whose
		// transcript/contract already exist, so a collision should be
		// impossible; this is the in-memory half of the same guard. A restored
		// row is pinned to its transcript and contract and is never silently
		// replaced by a different spawn.
		let existing = {
			let state = lock(&self.state);
			if state.tombstones.contains(id) {
				return Err(format!(
					"Agent \"{id}\" was hard-aborted and cannot be adopted or revived."
				));
			}
			state
				.entries
				.get(id)
				.map(|entry| (entry.session_file.clone(), entry.restored, entry.contract.clone()))
		};
		if let Some((existing_file, restored, existing_contract)) = existing {
			if existing_file != session_file {
				return Err(format!(
					"agent id \"{id}\" already names transcript {}; refusing to adopt {}",
					existing_file.display(),
					session_file.display()
				));
			}
			if restored {
				let incoming = contract
					.clone()
					.or_else(|| PersistedChildSession::load(&root, id));
				if incoming.as_ref() != existing_contract.as_ref() {
					return Err(format!(
						"agent \"{id}\" is a restored parked session; refusing to replace it \
						 with a different spawn"
					));
				}
			}
		}
		let contract = match contract {
			Some(contract) => {
				if contract.id != id {
					return Err(format!(
						"contract id {:?} does not match child \"{id}\"",
						contract.id
					));
				}
				contract.write(&root)?;
				Some(contract)
			},
			None => PersistedChildSession::load(&root, id),
		};
		let display_name = contract
			.as_ref()
			.and_then(|contract| contract.display_name.clone())
			.unwrap_or_else(|| record.name.clone());
		let parent_id = contract
			.as_ref()
			.and_then(|contract| contract.parent_id.clone())
			.or_else(|| record.parent_id.clone());
		let epoch = {
			let mut state = lock(&self.state);
			state.history.remove(id);
			let epoch = state.entries.get(id).map_or(0, |entry| entry.epoch.wrapping_add(1));
			state.entries.insert(
				id.to_string(),
				Entry {
					display_name,
					parent_id,
					status: LifecycleStatus::Idle,
					session_file,
					contract,
					restored: false,
					epoch,
					last_activity: now_ms(),
				},
			);
			epoch
		};
		self.arm_timer(id, epoch);
		Ok(())
	}

	/// Adopt a finished keep-alive child as a **live** `idle` actor.
	///
	/// TS `finalizeSubagentLifecycle` keeps the session itself and flips the ref
	/// to `idle`; a later send then wakes that same session (`woken`) until the
	/// idle TTL parks it. This is the Rust half: the child's own
	/// [`crate::loop_module::AgentLoop`] and [`crate::state::AgentState`] survive
	/// the run, so a follow-up turn continues the in-memory conversation instead
	/// of replaying the JSONL into a rebuilt session.
	///
	/// `contract` is not rewritten here: the settle path persists `output_id`
	/// after this call, and the actor reloads the contract before it rewrites a
	/// saved output.
	pub fn adopt_live(self: &Arc<Self>, id: &str, child: RevivedChild) -> Result<(), String> {
		if tokio::runtime::Handle::try_current().is_err() {
			return Err("adopting a live agent requires a tokio runtime".to_string());
		}
		let RevivedChild { agent_loop, state: child_state, agent, contract } = child;
		if contract.id != id {
			return Err(format!(
				"contract id {:?} does not match child \"{id}\"",
				contract.id
			));
		}
		let supervisor = self
			.supervisor
			.upgrade()
			.ok_or_else(|| "supervisor is gone".to_string())?;
		// A closing session must not publish an actor: the derived token is
		// already cancelled, and a live row would advertise a session that can
		// never run. Refuse so the child stays on the cold replay path.
		if supervisor.lifecycle_token().is_cancelled() {
			return Err("session is shutting down".to_string());
		}
		// Same single-flight gate `send` takes: a hub revive and the runner's
		// adopt must not both publish an actor for this id.
		let gate = self.revive_gate(id);
		let _gate = gate.lock().unwrap_or_else(|poison| poison.into_inner());
		// Pin the record until the claim: retention must not evict it while the
		// actor is being published; the guard also carries the cancel generation.
		let reservation = supervisor.reserve_revive(id);
		let record = supervisor
			.children()
			.into_iter()
			.find(|record| record.id == id)
			.ok_or_else(|| format!("unknown child \"{id}\""))?;
		if record.status == crate::supervisor::ChildStatus::Aborted {
			return Err(format!(
				"Agent \"{id}\" was hard-aborted and cannot be adopted or revived."
			));
		}
		let session_file = record
			.session_file
			.clone()
			.ok_or_else(|| format!("child \"{id}\" has no transcript file"))?;
		let display_name = contract
			.display_name
			.clone()
			.unwrap_or_else(|| record.name.clone());
		let parent_id = contract.parent_id.clone().or_else(|| record.parent_id.clone());
		// Keep the contract as persisted on disk: the runner snapshotted it
		// before the settle path wrote `output_id`, and this copy is the fallback
		// every later wake reads when the file is unavailable.
		let persisted_contract = session_file
			.parent()
			.and_then(|root| PersistedChildSession::load(root, id))
			.unwrap_or_else(|| contract.clone());
		let (inbound, receiver) = mpsc::channel(64);
		let (steering, steering_rx) = mpsc::channel(16);
		// Derived from the supervisor's session token, so `shutdown` cancels the
		// actor even before it is registered below.
		let cancel = supervisor.lifecycle_token();
		let epoch = {
			let state = lock(&self.state);
			if state.tombstones.contains(id) {
				return Err(format!(
					"Agent \"{id}\" was hard-aborted and cannot be adopted or revived."
				));
			}
			let existing = state
				.entries
				.get(id)
				.map(|entry| (entry.session_file.clone(), entry.restored, entry.contract.clone()));
			if let Some((existing_file, restored, existing_contract)) = existing {
				if existing_file != session_file {
					return Err(format!(
						"agent id \"{id}\" already names transcript {}; refusing to adopt {}",
						existing_file.display(),
						session_file.display()
					));
				}
				if restored && existing_contract.as_ref() != Some(&contract) {
					return Err(format!(
						"agent \"{id}\" is a restored parked session; refusing to replace it \
						 with a different spawn"
					));
				}
			}
			if state.live.contains_key(id) {
				return Err(format!("Agent \"{id}\" already has a live session."));
			}
			state.entries.get(id).map_or(0, |entry| entry.epoch.wrapping_add(1))
		};
		// Claim before publishing: a failed claim must not leave a live row that a
		// concurrent send would read as `injected` with no actor behind it.
		if cancel.is_cancelled()
			|| !supervisor.claim_lifecycle_actor(&reservation, epoch, cancel.clone())
		{
			return Err(format!(
				"Agent \"{id}\" was cancelled before its session started."
			));
		}
		{
			let mut state = lock(&self.state);
			if state.tombstones.contains(id) {
				drop(state);
				supervisor.unregister_lifecycle_actor(id, epoch);
				return Err(format!(
					"Agent \"{id}\" was cancelled before its session started."
				));
			}
			if state.live.contains_key(id) {
				drop(state);
				supervisor.unregister_lifecycle_actor(id, epoch);
				return Err(format!("Agent \"{id}\" already has a live session."));
			}
			state.live.insert(
				id.to_string(),
				LiveSession { inbound, steering, cancel: cancel.clone(), epoch },
			);
			// The in-memory state IS the producer state: `history://` serves this
			// snapshot before the next turn is journaled.
			state.history.insert(
				id.to_string(),
				Arc::new(Mutex::new(child_state.messages.clone())),
			);
			state.entries.insert(
				id.to_string(),
				Entry {
					display_name,
					parent_id,
					status: LifecycleStatus::Idle,
					session_file,
					contract: Some(persisted_contract.clone()),
					restored: false,
					epoch,
					last_activity: now_ms(),
				},
			);
		}
		self.arm_timer(id, epoch);
		self.spawn_actor(
			id.to_string(),
			RevivedChild {
				agent_loop,
				state: child_state,
				agent,
				contract: persisted_contract,
			},
			receiver,
			steering_rx,
			cancel,
			epoch,
		);
		Ok(())
	}

	/// Park an agent now (TTL expiry, explicit park, or shutdown).
	pub fn park(&self, id: &str) -> bool {
		let mut state = lock(&self.state);
		let Some(status) = state.entries.get(id).map(|entry| entry.status) else {
			return false;
		};
		if status == LifecycleStatus::Tombstoned {
			return false;
		}
		if let Some(live) = state.live.remove(id) {
			live.cancel.cancel();
		}
		state.history.remove(id);
		if let Some(entry) = state.entries.get_mut(id) {
			entry.epoch = entry.epoch.wrapping_add(1);
			entry.status = LifecycleStatus::Parked;
			entry.last_activity = now_ms();
		}
		true
	}

	/// Explicit user cancel of a live keep-alive session (`hub cancel`).
	///
	/// Runs under the same lock as [`Self::send`], so once it returns no message
	/// can be accepted into the dying actor: a racing send already queued is
	/// drained and journaled by the actor before it exits, and every later send
	/// hits the tombstone instead of a `woken` receipt.
	///
	/// TS `cancelAgentRegistration` aborts the session and releases the
	/// registration with no revive path; the durable tombstone is the Rust
	/// equivalent — `history://` stays readable, messaging and revival are
	/// refused, and a restart cannot re-adopt the transcript. Fail-closed: the
	/// sidecar is written and synced before this reports success, and a write
	/// error is returned to the caller, never swallowed.
	pub fn cancel_live(&self, id: &str, reason: &str) -> Result<(), String> {
		{
			let mut state = lock(&self.state);
			if let Some(live) = state.live.remove(id) {
				live.cancel.cancel();
			}
			state.history.remove(id);
			state.tombstones.insert(id.to_string());
			if let Some(entry) = state.entries.get_mut(id) {
				// The finishing actor must not resurrect the row.
				entry.epoch = entry.epoch.wrapping_add(1);
				entry.status = LifecycleStatus::Tombstoned;
				entry.last_activity = now_ms();
			}
		}
		self.ensure_tombstone(id, reason)
	}

	/// Whether `id` can never be messaged or revived again in this process.
	#[must_use]
	pub fn is_tombstoned(&self, id: &str) -> bool {
		let state = lock(&self.state);
		state.tombstones.contains(id)
			|| state.entries.get(id).map(|entry| entry.status)
				== Some(LifecycleStatus::Tombstoned)
	}

	/// Persist the tombstone marker for `id`, idempotently.
	///
	/// This is the only durable (restart-surviving) part of a cancel, and it is
	/// retried by a repeated `hub cancel` after the session root becomes
	/// writable again. A create/write/sync failure is returned, never reported
	/// as a durable cancel; the in-memory tombstone still blocks every wake,
	/// revival and adoption in this process.
	pub fn ensure_tombstone(&self, id: &str, reason: &str) -> Result<(), String> {
		let session_file = {
			let state = lock(&self.state);
			state
				.entries
				.get(id)
				.map(|entry| entry.session_file.clone())
		};
		let root = session_file
			.as_deref()
			.and_then(Path::parent)
			.map(Path::to_path_buf)
			.or_else(|| self.root.clone());
		let Some(root) = root else {
			return Err(format!(
				"cancel of \"{id}\" cannot be made durable: the session has no root"
			));
		};
		write_tombstone(&root, id, reason)
	}

	/// Hard kill: keep the transcript readable, refuse every later revive.
	///
	/// Returns whether the marker is durable: `false` means the in-memory
	/// tombstone is applied but a restart could still revive the transcript.
	pub fn tombstone(self: &Arc<Self>, id: &str, reason: &str) -> bool {
		// An aborted child is never adopted, so the transcript path has to come
		// from the supervisor record; otherwise its spawn-time contract would be
		// rediscovered as parked after a restart.
		let record_file = self.supervisor.upgrade().and_then(|supervisor| {
			supervisor
				.children()
				.into_iter()
				.find(|record| record.id == id)
				.and_then(|record| record.session_file)
		});
		let mut state = lock(&self.state);
		let session_file = state
			.entries
			.get(id)
			.map(|entry| entry.session_file.clone())
			.or(record_file);
		if let Some(live) = state.live.remove(id) {
			live.cancel.cancel();
		}
		state.history.remove(id);
		if let Some(entry) = state.entries.get_mut(id) {
			entry.epoch = entry.epoch.wrapping_add(1);
			entry.status = LifecycleStatus::Tombstoned;
			entry.last_activity = now_ms();
		}
		state.tombstones.insert(id.to_string());
		drop(state);
		match self.ensure_tombstone(id, reason) {
			Ok(()) => true,
			Err(error) => {
				eprintln!("warning: {error}");
				false
			},
		}
	}

	/// Deliver `text` to a lifecycle-managed agent.
	///
	/// A live actor reports the state it was in *before* the send, mirroring TS
	/// `IrcBus.send` → `deliverIrcMessage`:
	///
	/// - `idle` (waiting on its queue, nothing pending): the message wakes the
	///   real session and starts a turn — `woken`.
	/// - `running` (a turn is in flight): the message is queued as a
	///   non-interrupting aside at the next step boundary — `injected`.
	/// - no live actor: the parked row is rebuilt from the persisted contract
	///   and JSONL replay, and the message starts the first turn — `revived`.
	///
	/// The live-slot insert and the cancellation re-check share the manager
	/// lock, and the actor claim runs under the supervisor's lock with the
	/// generation captured when the revive reserved the record. A per-id gate
	/// makes the revive single-flight (TS coalesces): a second send waits for the
	/// winner, then delivers into the published session as `woken`/`injected`
	/// instead of starting a second actor or dropping its own text. A cancel
	/// racing the revive wins — no actor is published after it.
	///
	/// Refused with an error, never receipted: a tombstoned id (hard kill, or an
	/// explicit [`Self::cancel_live`]), a session whose lifecycle token was
	/// cancelled (teardown), and a live slot whose actor token is already
	/// cancelled (its actor is unwinding).
	pub fn send(self: &Arc<Self>, id: &str, text: String) -> Result<DeliveryOutcome, String> {
		if tokio::runtime::Handle::try_current().is_err() {
			return Err("reviving an agent requires a tokio runtime".to_string());
		}
		let supervisor = self.supervisor.upgrade();
		// A closed session must accept no new work: an actor spawned now would
		// inherit a cancelled token and drop the message, so report the refusal
		// instead of a `woken`/`revived` receipt for work that never runs.
		if let Some(supervisor) = supervisor.as_ref()
			&& supervisor.lifecycle_token().is_cancelled()
		{
			return Err(format!(
				"Agent session is shutting down; \"{id}\" cannot accept new messages."
			));
		}
		// One revive in flight per id (TS coalesces): a second send waits here and
		// then finds the live slot the winner published, instead of starting a
		// concurrent revive or failing with a message the peer never received.
		let gate = self.revive_gate(id);
		let _gate = gate.lock().unwrap_or_else(|poison| poison.into_inner());
		let mut state = lock(&self.state);
		if state.tombstones.contains(id) {
			return Err(format!(
				"Agent \"{id}\" was hard-aborted and cannot be messaged or revived."
			));
		}
		// A cancelled actor is already unwinding (`hub cancel` removes the slot
		// under this lock; teardown only cancels the token). Accepting a message
		// into its queue would report success for work no one drains, and a
		// revive started now could race the actor's closing journal flush.
		if state.live.get(id).is_some_and(|live| live.cancel.is_cancelled()) {
			state.live.remove(id);
			return Err(format!(
				"Agent \"{id}\" is stopping; retry once its session is closed."
			));
		}
		if let Some(live) = state.live.get(id) {
			let idle = state.entries.get(id).map(|entry| entry.status)
				== Some(LifecycleStatus::Idle);
			let inbound = live.inbound.clone();
			let steering = live.steering.clone();
			// The status read and the queue write happen under this same lock: a
			// concurrent actor idle-transition cannot route the same message into
			// both queues, and a message accepted while `idle` is never dropped.
			let delivery = if idle {
				match inbound.try_send(text.clone()) {
					Ok(()) => Some(DeliveryOutcome::Woken),
					Err(mpsc::error::TrySendError::Full(_)) => {
						return Err(format!("Agent \"{id}\" is busy; its session queue is full."));
					},
					Err(mpsc::error::TrySendError::Closed(_)) => None,
				}
			} else {
				match steering.try_send(crate::loop_module::SteeringEvent::InjectMessage(vec![
					crate::state::ContentPart::text(text.clone()),
				])) {
					Ok(()) => Some(DeliveryOutcome::Injected),
					Err(mpsc::error::TrySendError::Full(_)) => {
						return Err(format!("Agent \"{id}\" is busy; its aside queue is full."));
					},
					Err(mpsc::error::TrySendError::Closed(_)) => None,
				}
			};
			if let Some(outcome) = delivery {
				// Close the TTL race: a message queued at the exact expiry instant
				// must not be parked away before the actor dequeues it.
				if let Some(entry) = state.entries.get_mut(id) {
					entry.status = LifecycleStatus::Running;
					entry.last_activity = now_ms();
				}
				return Ok(outcome);
			}
			// The actor ended without recording it; drop the dead slot and fall
			// through to the persisted revive path.
			state.live.remove(id);
		}
		let Some(entry) = state.entries.get(id) else {
			return Err(format!("Unknown agent \"{id}\" — check `hub` list for live peers."));
		};
		if entry.status == LifecycleStatus::Tombstoned {
			return Err(format!(
				"Agent \"{id}\" was hard-aborted and cannot be messaged or revived."
			));
		}
		let contract = entry.contract.clone().ok_or_else(|| {
			format!(
				"Agent \"{id}\" has no persisted session_init contract; only its transcript \
				 (history://{id}) is available."
			)
		})?;
		let session_file = entry.session_file.clone();
		let epoch = entry.epoch.wrapping_add(1);
		let Some(supervisor) = supervisor.as_ref() else {
			return Err(format!(
				"cannot revive agent \"{id}\": its session is closing."
			));
		};
		drop(state);
		// Pin the record for the whole revive: retention must not evict it between
		// the restore and the registry bind inside the reviver (the child's
		// `local://` root is read from it), and the same guard carries the cancel
		// generation the claim compares. Taken after the manager lock is released:
		// manager→supervisor is not a legal lock order.
		let reservation = supervisor.reserve_revive(id);

		// Restore the record BEFORE the reviver builds the registry: without it
		// `url_scope_for(Some(child))` falls back to the parent's URL scope, and
		// the child's `read`/`write` would hold the parent's `local://` root.
		// Fail closed: a record restored from disk must come from the contract
		// verified inside this root, never from a live claim. A record that already
		// owns the transcript needs no restore, and the reviver stays the authority
		// on whether the spawn is still usable (e.g. a gone workspace), so its
		// precise refusal is not masked here.
		let root = self.root.as_deref().ok_or_else(|| {
			format!("cannot revive agent \"{id}\": the session has no persisted root.")
		})?;
		if session_file.parent() != Some(root) {
			return Err(format!(
				"cannot revive agent \"{id}\": its transcript is outside the session root."
			));
		}
		if !supervisor.owns_session_file(id, &session_file) {
			let verified = PersistedChildSession::load(root, id).ok_or_else(|| {
				format!("cannot revive agent \"{id}\": its persisted contract is unavailable.")
			})?;
			supervisor.restore_child_record(
				id,
				verified.display_name.as_deref().unwrap_or(id),
				verified.agent.as_deref(),
				verified.parent_id.as_deref(),
				&session_file,
				verified.published_output_id(),
			);
		}
		if !supervisor.owns_session_file(id, &session_file) {
			return Err(format!(
				"cannot revive agent \"{id}\": its session record could not be restored."
			));
		}

		let replayed = load_transcript(&session_file).map_err(|error| {
			format!("cannot reopen agent \"{id}\" transcript: {error}")
		})?;
		let revived = self.reviver.revive(&contract, replayed, Some(supervisor))?;
		let (inbound, receiver) = mpsc::channel(64);
		inbound
			.try_send(text)
			.map_err(|_| format!("Agent \"{id}\" session queue is unavailable."))?;
		let (steering, steering_rx) = mpsc::channel(16);
		// Derived from the supervisor's session token: `shutdown` cancels every
		// actor, registered or not, before session resources are released.
		let cancel = supervisor.lifecycle_token();
		// Claim before publishing the live slot: a failed claim must not leave a
		// slot a concurrent send would read as live (`injected`) with no actor
		// behind it — rolling such a slot back would destroy that message queue.
		if cancel.is_cancelled()
			|| !supervisor.claim_lifecycle_actor(&reservation, epoch, cancel.clone())
		{
			return Err(format!(
				"Agent \"{id}\" was cancelled before its session started."
			));
		}
		let mut blocker = None;
		{
			let mut state = lock(&self.state);
			if state.tombstones.contains(id)
				|| state.entries.get(id).map(|entry| entry.status)
					== Some(LifecycleStatus::Tombstoned)
			{
				blocker = Some("cancelled");
			} else if state.live.contains_key(id) {
				blocker = Some("live");
			} else {
				state.live.insert(
					id.to_string(),
					LiveSession { inbound, steering, cancel: cancel.clone(), epoch },
				);
				// The revived state IS the real producer state: this snapshot is
				// what `history://` serves before the actor journals the turn, and
				// the actor reconciles it with the final state after every turn.
				state.history.insert(
					id.to_string(),
					Arc::new(Mutex::new(revived.state.messages.clone())),
				);
				if let Some(entry) = state.entries.get_mut(id) {
					entry.epoch = epoch;
					entry.status = LifecycleStatus::Running;
					entry.last_activity = now_ms();
				}
			}
		}
		if let Some(blocker) = blocker {
			// Nothing was published, so the actor must not run: withdraw it and
			// leave the text to the caller (the hub buffers it as a failed send).
			supervisor.unregister_lifecycle_actor(id, epoch);
			return Err(match blocker {
				"live" => format!("Agent \"{id}\" is already live."),
				_ => format!("Agent \"{id}\" was cancelled before its session started."),
			});
		}
		let id = id.to_string();
		// The actor receives the first message immediately; `Revived` is honest
		// because a brand-new live session was built for it.
		self.spawn_actor(id, revived, receiver, steering_rx, cancel, epoch);
		Ok(DeliveryOutcome::Revived)
	}

	fn arm_timer(self: &Arc<Self>, id: &str, epoch: u64) {
		let ttl = self.idle_ttl_ms.load(Ordering::SeqCst);
		if ttl == 0 {
			return;
		}
		let Ok(handle) = tokio::runtime::Handle::try_current() else {
			return;
		};
		let manager = Arc::clone(self);
		let id = id.to_string();
		handle.spawn(async move {
			tokio::time::sleep(Duration::from_millis(ttl)).await;
			manager.expire_if_idle(&id, epoch);
		});
	}

	/// Spawn the actor that owns one live session.
	///
	/// The lease is created before the task and dropped with it (even on panic),
	/// so [`Self::shutdown_actors`] can always observe completion.
	fn spawn_actor(
		self: &Arc<Self>,
		id: String,
		revived: RevivedChild,
		receiver: mpsc::Receiver<String>,
		steering_rx: mpsc::Receiver<crate::loop_module::SteeringEvent>,
		cancel: tokio_util::sync::CancellationToken,
		epoch: u64,
	) {
		let lease = self.actors.enter();
		// Weak: an idle actor must not keep the manager (and its inbound sender)
		// alive in a cycle after the owner drops it.
		let manager = Arc::downgrade(self);
		let handle = tokio::spawn(async move {
			let _lease = lease;
			run_actor(manager, id, revived, receiver, steering_rx, cancel, epoch).await;
		});
		self.actors.track(handle.abort_handle());
	}

	/// Cancel every live actor and wait for it to stop.
	///
	/// Session teardown calls this before the provider, registry and tools are
	/// released, so a follow-up turn can never run after its owning scope
	/// closed. Cancellation is cooperative first; tasks that ignore it are
	/// aborted after [`ACTOR_SHUTDOWN_GRACE`] and reaped.
	pub async fn shutdown_actors(&self) {
		{
			let state = lock(&self.state);
			for live in state.live.values() {
				live.cancel.cancel();
			}
		}
		if tokio::time::timeout(ACTOR_SHUTDOWN_GRACE, self.wait_for_actors())
			.await
			.is_err()
		{
			self.actors.abort_all();
			// Aborted tasks unwind promptly; wait so nothing runs after the caller
			// releases session resources.
			let _ = tokio::time::timeout(ACTOR_SHUTDOWN_GRACE, self.wait_for_actors()).await;
		}
	}

	async fn wait_for_actors(&self) {
		while self.actors.running.load(Ordering::Acquire) > 0 {
			self.actors.done.notified().await;
		}
	}

	fn expire_if_idle(&self, id: &str, epoch: u64) {
		let mut state = lock(&self.state);
		let eligible = state.entries.get(id).is_some_and(|entry| {
			entry.epoch == epoch && entry.status == LifecycleStatus::Idle
		});
		if !eligible {
			return;
		}
		if let Some(live) = state.live.remove(id) {
			live.cancel.cancel();
		}
		state.history.remove(id);
		if let Some(entry) = state.entries.get_mut(id) {
			entry.epoch = entry.epoch.wrapping_add(1);
			entry.status = LifecycleStatus::Parked;
			entry.last_activity = now_ms();
		}
	}

	fn note_running(&self, id: &str, epoch: u64) {
		let mut state = lock(&self.state);
		let Some(entry) = state.entries.get_mut(id) else {
			return;
		};
		if entry.epoch != epoch
			|| matches!(entry.status, LifecycleStatus::Tombstoned | LifecycleStatus::Parked)
		{
			return;
		}
		entry.status = LifecycleStatus::Running;
		entry.last_activity = now_ms();
	}

	/// Close one live turn: decide the next row status and collect asides that
	/// missed the last step boundary in one critical section.
	///
	/// A send that already observed `running` wrote its aside under this same
	/// lock, so it is drained here; a send after the transition observes `idle`
	/// and lands in `inbound`, which the actor's `recv` picks up. A queued wake
	/// message or a collected aside keeps the row `running` and is returned as a
	/// follow-up turn (TS `#resumeStrandedIrcAsides`), so a busy send is never
	/// left as context without a turn. Otherwise the row becomes `idle` and the
	/// idle-TTL timer is armed.
	fn settle_turn(
		self: &Arc<Self>,
		id: &str,
		epoch: u64,
		inbound: &mpsc::Receiver<String>,
		steering: &mut mpsc::Receiver<crate::loop_module::SteeringEvent>,
	) -> Vec<Vec<ContentPart>> {
		let mut asides = Vec::new();
		let mut valid = false;
		let mut idle = false;
		{
			let mut state = lock(&self.state);
			while let Ok(event) = steering.try_recv() {
				if let crate::loop_module::SteeringEvent::InjectMessage(parts) = event {
					asides.push(parts);
				}
			}
			// Queued under the same lock a sender uses, so `idle` really means
			// "waiting on `recv`, nothing pending".
			let busy = inbound.len() > 0 || !asides.is_empty();
			if let Some(entry) = state.entries.get_mut(id)
				&& entry.epoch == epoch
				&& !matches!(entry.status, LifecycleStatus::Tombstoned | LifecycleStatus::Parked)
			{
				valid = true;
				idle = !busy;
				entry.status = if busy {
					LifecycleStatus::Running
				} else {
					LifecycleStatus::Idle
				};
				entry.last_activity = now_ms();
			}
		}
		if !valid {
			return Vec::new();
		}
		if idle {
			// TS `agent_end`: the turn fully unwound with nothing queued. Awaited
			// sends stop here when no reply arrived, instead of at their timeout.
			if let Some(supervisor) = self.supervisor.upgrade() {
				supervisor.note_agent_end(id);
			}
			self.arm_timer(id, epoch);
		}
		asides
	}

	fn finish_actor(&self, id: &str, epoch: u64) {
		let mut owned = false;
		let mut state = lock(&self.state);
		if state.live.get(id).is_some_and(|live| live.epoch == epoch) {
			state.live.remove(id);
			state.history.remove(id);
			owned = true;
		}
		if let Some(entry) = state.entries.get_mut(id)
			&& entry.epoch == epoch
			&& !matches!(
				entry.status,
				LifecycleStatus::Tombstoned | LifecycleStatus::Parked
			)
		{
			entry.status = LifecycleStatus::Idle;
			entry.last_activity = now_ms();
		}
		drop(state);
		// An actor that exits without an idle settle (error, cancellation) has
		// still ended its turn as far as an awaited sender is concerned.
		if owned
			&& let Some(supervisor) = self.supervisor.upgrade()
		{
			supervisor.note_agent_end(id);
		}
	}

	/// Restore parked rows and tombstones from disk (process restart / resume).
	fn scan_persisted(&self, root: &Path) {
		let Ok(entries) = fs::read_dir(root) else {
			return;
		};
		let mut state = lock(&self.state);
		for entry in entries.flatten() {
			let path = entry.path();
			let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
				continue;
			};
			if let Some(id) = name.strip_suffix(TOMBSTONE_SUFFIX) {
				state.tombstones.insert(id.to_string());
				continue;
			}
			let Some(id) = name.strip_suffix(SESSION_INIT_SUFFIX) else {
				continue;
			};
			if state.entries.contains_key(id) {
				continue;
			}
			if tombstone_path(root, id).exists() {
				state.tombstones.insert(id.to_string());
				continue;
			}
			let Some(contract) = PersistedChildSession::load(root, id) else {
				continue;
			};
			let display_name = contract
				.display_name
				.clone()
				.or_else(|| contract.agent.clone())
				.unwrap_or_else(|| id.to_string());
			state.entries.insert(
				id.to_string(),
				Entry {
					display_name,
					parent_id: contract.parent_id.clone(),
					// A restarted process has no live session: every restored row
					// is parked until a send revives it.
					status: LifecycleStatus::Parked,
					session_file: transcript_path(root, id),
					contract: Some(contract),
					restored: true,
					epoch: 0,
					last_activity: now_ms(),
				},
			);
		}
	}
}

/// Serial worker behind one live session (revived or adopted).
///
/// Messages queue here; each one runs a fresh dialog turn against the same
/// [`crate::state::AgentState`]. A message that arrives while a turn is already
/// running goes through the steering channel instead and joins the running
/// dialog at its next step boundary (TS `deliverIrcMessage` aside), so it never
/// starts a second session and never waits for the whole turn to finish.
async fn run_actor(
	manager: Weak<SubagentLifecycleManager>,
	id: String,
	revived: RevivedChild,
	mut receiver: mpsc::Receiver<String>,
	mut steering_rx: mpsc::Receiver<crate::loop_module::SteeringEvent>,
	cancel: tokio_util::sync::CancellationToken,
	epoch: u64,
) {
	let RevivedChild {
		mut agent_loop,
		mut state,
		agent,
		contract,
	} = revived;
	// One journal for the whole actor: the loop reports every turn boundary, so
	// accepted messages reach the file before compaction can prune them, and the
	// cursor carries across wake turns.
	let journal = manager
		.upgrade()
		.and_then(|manager| manager.supervisor.upgrade())
		.map(|supervisor| {
			Arc::new(TranscriptJournal::new(
				supervisor,
				id.clone(),
				state.messages.len(),
				state.messages.first().is_some_and(|m| m.role == Role::System),
			))
		});
	if let Some(journal) = journal.clone() {
		agent_loop = agent_loop.with_turn_observer(Some({
			let journal = Arc::clone(&journal);
			Arc::new(
				move |messages: &[Message], prune: Option<CompactionReport>| {
					journal.flush(messages, prune)
				},
			) as crate::loop_module::TurnObserver
		}));
	}
	// Asides that missed their last step boundary, resolved as wake turns.
	let mut pending: VecDeque<Message> = VecDeque::new();
	// Buffered mail addressed to this agent is drained at the first turn
	// boundary by the loop's peer inbox; it stays queued here until then.
	loop {
		let request = match pending.pop_front() {
			Some(message) => message,
			None => tokio::select! {
				maybe = receiver.recv() => match maybe {
					Some(text) => Message::user(text),
					None => break,
				},
				// Teardown/hub-cancel must stop an *idle* actor too: without this
				// arm a cancelled actor would sit on `recv` until someone sent it
				// another message or the sender was dropped.
				() = cancel.cancelled() => break,
			},
		};
		if cancel.is_cancelled() {
			// The request was already taken from the queue, and the exit drain
			// below can no longer see it: its `woken` receipt is out, so persist
			// it here instead of dropping it on the way out.
			state.messages.push(request);
			if let Some(journal) = journal.as_ref() {
				journal.flush(&state.messages, None);
			}
			break;
		}
		let Some(manager) = manager.upgrade() else {
			break;
		};
		manager.note_running(&id, epoch);
		let before = state.messages.len();
		// The request is a real accepted input; show it before the provider
		// emits anything. The drain task then applies the loop's events in
		// emission order, and the reconcile below replaces both with the final
		// authoritative state.
		manager.append_live_message_for_epoch(&id, epoch, request.clone());
		let (event_tx, mut event_rx) = mpsc::channel(256);
		let event_manager = Arc::downgrade(&manager);
		let event_id = id.clone();
		let event_epoch = epoch;
		let drain = tokio::spawn(async move {
			while let Some(event) = event_rx.recv().await {
				let Some(manager) = event_manager.upgrade() else {
					break;
				};
				manager.apply_live_event_for_epoch(&event_id, event_epoch, &event);
			}
		});
		// Each wake/follow-up is a new run: TS starts it with a fresh turn budget
		// instead of inheriting the finished run's counter, which would make a
		// child that hit its cap permanently unanswerable.
		state.total_turns = 0;
		let turn_result = agent_loop
			.run_dialog_message_with_cancel(
				request.clone(),
				&mut state,
				Some(&mut steering_rx),
				Some(event_tx),
				cancel.child_token(),
			)
			.await;
		let _ = drain.await;
		// TS `runSubagentFollowUpTurn`: a follow-up turn rewrites the same
		// `<outputId>.md` only when it produced a real yield. A hub wake with no
		// yield leaves the last completion artifact untouched (issue #9518).
		// The contract is re-read from disk: a live-adopted child is registered
		// before the settle path persists `output_id`, so the in-memory copy can
		// predate it. Resolve the directory from the lifecycle entry, not the
		// supervisor record: settled records are evicted after the retention
		// window while this actor still runs, and a stale copy would skip the
		// `output_id` rewrite for every later wake.
		if let Ok(turns) = &turn_result
			&& let Some(agent) = agent.as_ref()
		{
			let published = manager
				.transcript_path(&id)
				.and_then(|path| path.parent().map(Path::to_path_buf))
				.and_then(|root| PersistedChildSession::load(&root, &id))
				.unwrap_or_else(|| contract.clone());
			if let Some(output_id) = published.published_output_id()
				&& let Some(root) = manager
					.supervisor
					.upgrade()
					.and_then(|supervisor| supervisor.agent_output_root())
				&& let Some(body) =
					crate::tools::task::follow_up_output(&published, agent, turns)
				&& let Err(error) = crate::tools::agent_output::publish_agent_output_replace(
					&root,
					output_id,
					&body,
				)
			{
				eprintln!(
					"warning: cannot rewrite follow-up agent output {output_id}: {error}"
				);
			}
		}
		if let Err(error) = turn_result {
			// The hub receipt already reported this session as `revived`. A
			// failed turn is real information, not a silent success: report it
			// instead of dropping the error.
			//
			// Nothing is rewritten here, and no partial turn was journaled: the
			// transcript is an append-only log addressed by the journal cursor,
			// and the loop only appends accepted messages — the request before
			// its first await (this actor's loop is never a continuation), the
			// asides it absorbed (`next_assistant` hands them back on its error
			// path), and complete tool steps. Splitting the state here would
			// shorten the conversation below the cursor, because the step
			// boundary already wrote the completed pair; a later flush would then
			// either skip an accepted aside or claim durability the file does not
			// have. Keeping the state monotonic makes the flush below append
			// exactly the unwritten accepted messages, once and in order.
			eprintln!("warning: revived agent \"{id}\" turn failed: {error}");
			debug_assert!(
				state.messages.len() > before,
				"the loop echoes the request before its first await"
			);
		}
		if let Some(journal) = journal.as_ref() {
			journal.flush(&state.messages, None);
		}
		// Authoritative reconcile: replaces the request echo and the drained
		// events with the state the loop actually accepted, including a failed
		// turn's absorbed asides.
		manager.replace_live_for_epoch(&id, epoch, state.messages.clone());
		if cancel.is_cancelled() {
			break;
		}
		// Close the turn and take any aside that missed its last step boundary in
		// one critical section (see `settle_turn`). Resolved asides wake a real
		// follow-up turn, matching TS `#resumeStrandedIrcAsides`; keeping them as
		// context-only would silently break a busy send's `injected` promise.
		for parts in manager.settle_turn(&id, epoch, &receiver, &mut steering_rx) {
			pending.push_back(Message::user_with_parts(parts));
		}
	}
	// A cancel can race the last accepted `send`, whose receipt already said
	// `woken`/`injected`. Those messages sit in this actor's queues; the live
	// slot is gone (so nothing new can be enqueued), and dropping them here
	// would make the receipt a lie. Persist them in order instead: a later
	// revive replays the instruction.
	while let Some(message) = pending.pop_front() {
		state.messages.push(message);
	}
	while let Ok(text) = receiver.try_recv() {
		state.messages.push(Message::user(text));
	}
	while let Ok(event) = steering_rx.try_recv() {
		if let crate::loop_module::SteeringEvent::InjectMessage(parts) = event {
			state.messages.push(Message::user_with_parts(parts));
		}
	}
	if let Some(journal) = journal.as_ref() {
		journal.flush(&state.messages, None);
	}
	if let Some(manager) = manager.upgrade() {
		if let Some(supervisor) = manager.supervisor.upgrade() {
			supervisor.unregister_lifecycle_actor(&id, epoch);
			supervisor.clear_revived_output_prefix(&id);
		}
		manager.finish_actor(&id, epoch);
	}
}

