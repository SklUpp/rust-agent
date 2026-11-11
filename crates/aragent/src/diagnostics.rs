//! Local diagnostics trace, separate from `sessions.db`.
//!
//! One append-only SQLite event log records lifecycle boundaries and token
//! counters for sessions, model requests, tool calls and subagents. It never
//! stores prompt text, tool arguments or results, file paths, URLs, raw session
//! or subagent identifiers, provider messages, or error strings: the typed
//! writer accepts only fixed enums, generated correlation ids and nullable
//! counters.
//!
//! Enabled by default (`diagnostics.enabled`). Disabling it only stops new
//! writes; an existing database is never deleted or truncated. Every failure is
//! contained: opening or writing emits one generic warning and execution
//! continues. The writer runs on its own thread behind a bounded channel, so a
//! slow or locked database never blocks the async runtime; full queues drop
//! records instead of waiting. The database lives in a dedicated diagnostics
//! directory that must be owned by the current user with mode `0700`; on Unix
//! the database and its `-wal`/`-shm`/`-journal` sidecars are forced to `0600`.
//! Symlinked, foreign-owned, shared, or multiply-linked paths are refused and
//! leave diagnostics off.

use std::{
	collections::HashMap,
	fs, io,
	path::{Path, PathBuf},
	sync::{
		Arc, Mutex, RwLock,
		atomic::{AtomicBool, AtomicU64, Ordering},
		mpsc::{self, Receiver, SyncSender, TrySendError},
	},
	thread::{self, JoinHandle},
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use pi_agent::{
	TurnStatus,
	supervisor::{SubagentEvent, SubagentLifecycleStatus, SubagentObserver},
};
use rusqlite::{Connection, OpenFlags, params};

use crate::{config::Config, dirs};

/// Dedicated diagnostics directory under the agent data root.
pub const DIAGNOSTICS_DIR: &str = "diagnostics";
/// File name inside the private diagnostics directory. Never shared with
/// `sessions.db`.
pub const DATABASE_FILE: &str = "diagnostics.db";

const SCHEMA_VERSION: i64 = 1;
const QUEUE_CAPACITY: usize = 1024;
const BUSY_TIMEOUT: Duration = Duration::from_secs(2);

const CREATE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS spans (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    kind TEXT NOT NULL
        CHECK (kind IN ('session','model','tool_builtin','tool_custom','subagent')),
    status TEXT NOT NULL
        CHECK (status IN ('started','succeeded','failed','aborted','closed','truncated')),
    at_ms INTEGER NOT NULL,
    duration_ms INTEGER,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_write_tokens INTEGER,
    reasoning_tokens INTEGER
);
CREATE INDEX IF NOT EXISTS spans_span_id ON spans(span_id);
CREATE INDEX IF NOT EXISTS spans_at_ms ON spans(at_ms);
";

const INSERT_SPAN: &str = "
INSERT INTO spans (
    span_id, parent_span_id, kind, status, at_ms, duration_ms,
    input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
    reasoning_tokens
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
";

/// Owner-only file mode for the database and its SQLite sidecars.
#[cfg(unix)]
const PRIVATE_FILE_MODE: u32 = 0o600;
/// Owner-only directory mode for a diagnostics directory this process creates.
#[cfg(unix)]
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
	let mut os = path.as_os_str().to_os_string();
	os.push(suffix);
	PathBuf::from(os)
}

/// An ancestor owner we trust not to substitute entries: root, the current
/// user, or (on Android) the platform `system` uid.
#[cfg(unix)]
fn trusted_owner(uid: u32, euid: u32) -> bool {
	#[cfg(target_os = "android")]
	{
		if uid == 1000 {
			return true;
		}
	}
	uid == 0 || uid == euid
}

/// Prove that a group-writable directory's writable group is exclusive to a
/// trusted principal. GID 0 is not exclusive (wheel/root groups may contain
/// other users), so only established Android identities qualify: the platform
/// `system` uid/gid or an app's own uid/gid pair.
#[cfg(unix)]
fn exclusive_group(_uid: u32, _euid: u32, _gid: u32) -> bool {
	#[cfg(target_os = "android")]
	{
		// Verified on device: `/data` and `/data/data` are `system:system`
		// (uid/gid 1000), mode 0771. Older platform layouts use `root:system`
		// (uid 0, gid 1000); the writable principal is still the platform-only
		// gid 1000 either way.
		if _gid == 1000 && (_uid == 1000 || _uid == 0) {
			return true;
		}
		// App ids start at 10000 and use the app uid as its own private
		// primary gid; require the directory to match this process exactly.
		if _euid >= 10_000 && _uid == _euid && _gid == _euid {
			return true;
		}
	}
	false
}

/// A directory ancestor is trusted when its owner is trusted and other users
/// cannot create/rename inside it, or when sticky semantics protect our owned
/// entries, or when a writable group is provably platform-exclusive.
fn trusted_ancestor(metadata: &fs::Metadata, euid: u32) -> bool {
	#[cfg(unix)]
	{
		use std::os::unix::fs::{MetadataExt, PermissionsExt};
		if metadata.file_type().is_symlink() || !metadata.is_dir() {
			return false;
		}
		let mode = metadata.permissions().mode();
		let uid = metadata.uid();
		let gid = metadata.gid();
		let owner_trusted = trusted_owner(uid, euid);
		if mode & 0o022 == 0 {
			return owner_trusted;
		}
		if mode & 0o1000 != 0 {
			// Sticky: rename of our entries is limited to root/current user.
			return owner_trusted;
		}
		if mode & 0o002 != 0 {
			// World-writable without sticky semantics.
			return false;
		}
		// Group-writable only: reject unless the writable group is exclusive.
		exclusive_group(uid, euid, gid)
	}
	#[cfg(not(unix))]
	{
		let _ = euid;
		!metadata.file_type().is_symlink() && metadata.is_dir()
	}
}

fn verify_ancestor_chain(
	dir: &Path,
	euid: u32,
	missing_ok: bool,
) -> Result<(), OpenError> {
	if !dir.is_absolute() {
		return Err(OpenError);
	}
	for ancestor in dir.ancestors() {
		if ancestor.as_os_str().is_empty() {
			continue;
		}
		match fs::symlink_metadata(ancestor) {
			Ok(metadata) => {
				if !trusted_ancestor(&metadata, euid) {
					return Err(OpenError);
				}
			},
			Err(error) if missing_ok && error.kind() == io::ErrorKind::NotFound => {},
			Err(_) => return Err(OpenError),
		}
	}
	Ok(())
}

/// Require a private diagnostics directory: regular directory, owned by the
/// current user, mode exactly `0700`, and not a symlink. Every existing
/// ancestor must be safe from rename by other users; a missing directory is
/// created with that mode. Unsafe ancestors are refused, never chmodded.
fn ensure_private_directory(dir: &Path) -> Result<(), OpenError> {
	#[cfg(unix)]
	// SAFETY: geteuid has no preconditions.
	let euid = unsafe { libc::geteuid() };
	#[cfg(not(unix))]
	let euid = 0;
	verify_ancestor_chain(dir, euid, true)?;
	if fs::symlink_metadata(dir).is_err() {
		#[cfg(unix)]
		{
			use std::os::unix::fs::DirBuilderExt;
			let mut builder = fs::DirBuilder::new();
			builder.recursive(true).mode(PRIVATE_DIRECTORY_MODE);
			builder.create(dir).map_err(|_| OpenError)?;
		}
		#[cfg(not(unix))]
		{
			fs::create_dir_all(dir).map_err(|_| OpenError)?;
		}
	}
	verify_ancestor_chain(dir, euid, false)?;
	verify_private_directory(dir)
}

fn verify_private_directory(dir: &Path) -> Result<(), OpenError> {
	let metadata = fs::symlink_metadata(dir).map_err(|_| OpenError)?;
	if metadata.file_type().is_symlink() || !metadata.is_dir() {
		return Err(OpenError);
	}
	#[cfg(unix)]
	{
		use std::os::unix::fs::{MetadataExt, PermissionsExt};
		// SAFETY: geteuid has no preconditions.
		let uid = unsafe { libc::geteuid() };
		if metadata.uid() != uid {
			return Err(OpenError);
		}
		if metadata.permissions().mode() & 0o777 != PRIVATE_DIRECTORY_MODE {
			return Err(OpenError);
		}
	}
	Ok(())
}

/// Reject a symlink, non-regular file, or multiply-linked inode and force the
/// file to owner-only mode. A missing file is allowed (SQLite creates it).
fn require_private_file(path: &Path) -> Result<(), OpenError> {
	match fs::symlink_metadata(path) {
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
		Err(_) => Err(OpenError),
		Ok(metadata) => {
			if metadata.file_type().is_symlink() || !metadata.is_file() {
				return Err(OpenError);
			}
			#[cfg(unix)]
			{
				use std::os::unix::fs::{MetadataExt, PermissionsExt};
				if metadata.nlink() > 1 {
					return Err(OpenError);
				}
				if metadata.permissions().mode() & 0o7777 != PRIVATE_FILE_MODE {
					fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_FILE_MODE))
						.map_err(|_| OpenError)?;
				}
			}
			Ok(())
		},
	}
}

/// Enforce owner-only mode and no symlink/hardlink aliases for the database and
/// every possible SQLite sidecar, including files that already existed.
fn restrict_database_files(path: &Path) -> Result<(), OpenError> {
	require_private_file(path)?;
	for suffix in ["-journal", "-wal", "-shm"] {
		require_private_file(&sidecar_path(path, suffix))?;
	}
	Ok(())
}

/// Create the database leaf with owner-only mode before SQLite opens it, so the
/// first bytes never land in a world-readable file. `SQLITE_OPEN_NOFOLLOW` on
/// the connection narrows the remaining rename race.
fn ensure_database_file(path: &Path) -> Result<(), OpenError> {
	if fs::symlink_metadata(path).is_ok() {
		return require_private_file(path);
	}
	let mut options = fs::OpenOptions::new();
	options.read(true).write(true).create(true);
	#[cfg(unix)]
	{
		use std::os::unix::fs::OpenOptionsExt;
		options.mode(PRIVATE_FILE_MODE).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
	}
	options.open(path).map_err(|_| OpenError)?;
	require_private_file(path)
}

/// `dirs.ts` `agentSubdir(undefined, name, "data")`, then the private
/// diagnostics subdirectory.
#[must_use]
pub fn database_path() -> PathBuf {
	dirs::agent_subdir(DIAGNOSTICS_DIR, dirs::AgentDirCategory::Data)
		.join(DATABASE_FILE)
}

/// Opened diagnostics sink. Drop or [`shutdown`](Self::shutdown) drains the
/// queue and joins the writer thread.
pub struct Diagnostics {
	sender:   SyncSender<Message>,
	writer:   Mutex<Option<JoinHandle<()>>>,
	warned:   Arc<AtomicBool>,
	prefix:   u64,
	next:     AtomicU64,
	sessions: Mutex<HashMap<String, (String, Instant)>>,
	closed:   AtomicBool,
}

impl std::fmt::Debug for Diagnostics {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.debug_struct("Diagnostics").field("closed", &self.closed.load(Ordering::Relaxed)).finish()
	}
}

#[derive(Debug)]
pub struct OpenError;

impl std::fmt::Display for OpenError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str("diagnostics database unavailable")
	}
}

impl std::error::Error for OpenError {}

/// Lifecycle boundary kind. Tool names never reach the database; the enum says
/// only whether the name is a builtin one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
	Session,
	Model,
	ToolBuiltin,
	ToolCustom,
	Subagent,
}

impl SpanKind {
	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Session => "session",
			Self::Model => "model",
			Self::ToolBuiltin => "tool_builtin",
			Self::ToolCustom => "tool_custom",
			Self::Subagent => "subagent",
		}
	}
}

/// Span outcome. A start row always carries [`SpanStatus::Started`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanStatus {
	Started,
	Succeeded,
	Failed,
	Aborted,
	Closed,
	Truncated,
}

impl SpanStatus {
	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Started => "started",
			Self::Succeeded => "succeeded",
			Self::Failed => "failed",
			Self::Aborted => "aborted",
			Self::Closed => "closed",
			Self::Truncated => "truncated",
		}
	}
}

/// Provider-reported token counters for one model request. `None` means the
/// provider reported nothing; it is stored as SQL `NULL`, never `0`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenRecord {
	pub input:        Option<u64>,
	pub output:       Option<u64>,
	pub cache_read:   Option<u64>,
	pub cache_write:  Option<u64>,
	pub reasoning:    Option<u64>,
}

impl TokenRecord {
	#[must_use]
	pub fn from_usage(usage: Option<&pi_agent::TokenUsage>) -> Self {
		usage.map_or_else(Self::default, |usage| Self {
			// Providers set per-counter presence at their parser boundary.
			// Absent -> NULL; explicit provider zero -> Some(0).
			input:       usage.presence.input.then_some(usage.input),
			output:      usage.presence.output.then_some(usage.output),
			cache_read:  usage.presence.cache_read.then_some(usage.cache_read),
			cache_write: usage.presence.cache_write.then_some(usage.cache_write),
			reasoning:   usage.presence.reasoning.then_some(usage.reasoning_tokens.unwrap_or(0)),
		})
	}
}

/// Builtin tool names. Any other name (MCP, extension, host tool) is recorded
/// as [`SpanKind::ToolCustom`], so arbitrary names never reach the log.
const BUILTIN_TOOL_NAMES: [&str; 25] = [
	"read",
	"glob",
	"grep",
	"ast_grep",
	"ast_edit",
	"write",
	"edit",
	"bash",
	"lsp",
	"debug",
	"task",
	"hub",
	"todo",
	"eval",
	"think",
	"yield",
	"web_search",
	"web_fetch",
	"inspect_image",
	"ask",
	"advise",
	"checkpoint",
	"rewind",
	"goal",
	"security_scan",
];

#[must_use]
pub fn tool_kind(name: &str) -> SpanKind {
	if BUILTIN_TOOL_NAMES.contains(&name) {
		SpanKind::ToolBuiltin
	} else {
		SpanKind::ToolCustom
	}
}

#[must_use]
pub fn model_status(status: &TurnStatus) -> SpanStatus {
	match status {
		TurnStatus::EndTurn | TurnStatus::ToolUse => SpanStatus::Succeeded,
		TurnStatus::Length => SpanStatus::Truncated,
		TurnStatus::Aborted => SpanStatus::Aborted,
		TurnStatus::Error(_) => SpanStatus::Failed,
	}
}

#[must_use]
pub fn subagent_status(status: SubagentLifecycleStatus) -> SpanStatus {
	match status {
		SubagentLifecycleStatus::Started => SpanStatus::Started,
		SubagentLifecycleStatus::Completed => SpanStatus::Succeeded,
		SubagentLifecycleStatus::Failed => SpanStatus::Failed,
		SubagentLifecycleStatus::Aborted => SpanStatus::Aborted,
	}
}

/// One row to append. Private fields keep callers from inventing free-form
/// content; only enums, generated ids and counters are accepted.
#[derive(Debug)]
struct SpanEvent {
	span_id:        String,
	parent_span_id: Option<String>,
	kind:           SpanKind,
	status:         SpanStatus,
	at_ms:          i64,
	duration_ms:    Option<u64>,
	tokens:         TokenRecord,
}

enum Message {
	Record(Box<SpanEvent>),
	Flush(SyncSender<()>),
	Stop(SyncSender<()>),
}

impl Diagnostics {
	/// Open (creating if needed) the event log and start its writer thread.
	pub fn open(path: impl Into<PathBuf>) -> Result<Arc<Self>, OpenError> {
		let path = path.into();
		let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) else {
			return Err(OpenError);
		};
		ensure_private_directory(parent)?;
		ensure_database_file(&path)?;
		restrict_database_files(&path)?;
		let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
			| OpenFlags::SQLITE_OPEN_CREATE
			| OpenFlags::SQLITE_OPEN_NOFOLLOW;
		let connection = Connection::open_with_flags(&path, flags).map_err(|_| OpenError)?;
		connection.busy_timeout(BUSY_TIMEOUT).map_err(|_| OpenError)?;
		let _: String = connection
			.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
			.map_err(|_| OpenError)?;
		connection
			.execute_batch("PRAGMA synchronous=NORMAL;")
			.map_err(|_| OpenError)?;
		let version: i64 = connection
			.query_row("PRAGMA user_version", [], |row| row.get(0))
			.map_err(|_| OpenError)?;
		if version > SCHEMA_VERSION {
			// A newer writer owns this file; never rewrite or truncate it.
			return Err(OpenError);
		}
		if version == 0 {
			connection.execute_batch(CREATE_SCHEMA).map_err(|_| OpenError)?;
			connection
				.execute_batch(&format!("PRAGMA user_version={SCHEMA_VERSION};"))
				.map_err(|_| OpenError)?;
		}
		// SQLite creates WAL/SHM lazily; enforce owner-only on any that exist
		// now and again after the first write in the writer thread.
		restrict_database_files(&path)?;
		let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
		let warned = Arc::new(AtomicBool::new(false));
		let writer_warned = Arc::clone(&warned);
		let writer_path = path.clone();
		let writer = thread::Builder::new()
			.name("diagnostics-writer".to_string())
			.spawn(move || writer_loop(connection, receiver, writer_warned, writer_path))
			.map_err(|_| OpenError)?;
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_or(0, |elapsed| elapsed.as_nanos() as u64);
		let prefix = nanos ^ (u64::from(std::process::id()) << 32);
		Ok(Arc::new(Self {
			sender,
			writer: Mutex::new(Some(writer)),
			warned,
			prefix,
			next: AtomicU64::new(0),
			sessions: Mutex::new(HashMap::new()),
			closed: AtomicBool::new(false),
		}))
	}

	fn new_span_id(&self) -> String {
		let sequence = self.next.fetch_add(1, Ordering::Relaxed);
		format!("{:016x}{:016x}", self.prefix, sequence)
	}

	fn send(&self, message: Message) {
		if self.closed.load(Ordering::Relaxed) {
			return;
		}
		match self.sender.try_send(message) {
			Ok(()) => {},
			Err(TrySendError::Full(_)) => {
				self.warn("diagnostics queue is full; dropping records")
			},
			Err(TrySendError::Disconnected(_)) => {},
		}
	}

	fn warn(&self, message: &str) {
		if !self.warned.swap(true, Ordering::Relaxed) {
			eprintln!("warning: {message}");
		}
	}

	/// Begin a boundary. The returned timer records its own end on explicit
	/// [`SpanTimer::finish`] or, if dropped first, as aborted.
	pub fn begin_span(self: &Arc<Self>, kind: SpanKind, parent_span_id: Option<&str>) -> SpanTimer {
		let span_id = self.new_span_id();
		self.send(Message::Record(Box::new(SpanEvent {
			span_id:        span_id.clone(),
			parent_span_id: parent_span_id.map(str::to_string),
			kind,
			status:         SpanStatus::Started,
			at_ms:          now_ms(),
			duration_ms:    None,
			tokens:         TokenRecord::default(),
		})));
		SpanTimer {
			inner: Some(SpanTimerInner {
				sink:    Arc::clone(self),
				span_id,
				parent:  parent_span_id.map(str::to_string),
				kind,
				started: Instant::now(),
			}),
		}
	}

	/// Open a session boundary. The returned trace registers its correlation id
	/// for [`session_span`](Self::session_span) and closes on drop.
	pub fn start_session(self: &Arc<Self>, key: &str) -> SessionTrace {
		let span_id = self.new_span_id();
		self.send(Message::Record(Box::new(SpanEvent {
			span_id:        span_id.clone(),
			parent_span_id: None,
			kind:           SpanKind::Session,
			status:         SpanStatus::Started,
			at_ms:          now_ms(),
			duration_ms:    None,
			tokens:         TokenRecord::default(),
		})));
		if let Ok(mut sessions) = self.sessions.lock() {
			sessions.insert(key.to_string(), (span_id.clone(), Instant::now()));
		}
		SessionTrace {
			inner: Some(SessionTraceInner {
				sink: Arc::clone(self),
				key: key.to_string(),
				span_id,
				started: Instant::now(),
			}),
		}
	}

	/// Correlation id for an open session, used as the parent of its model and
	/// tool boundaries.
	#[must_use]
	pub fn session_span(&self, key: &str) -> Option<String> {
		self
			.sessions
			.lock()
			.ok()
			.and_then(|sessions| sessions.get(key).map(|(span_id, _)| span_id.clone()))
	}

	/// Map an alternate correlation key (for example a provider-facing run id)
	/// onto the trace of an existing session. The session span itself is
	/// unchanged; the alias is dropped with the trace.
	pub fn alias_session(&self, alias: &str, key: &str) {
		if let Ok(mut sessions) = self.sessions.lock()
			&& let Some((span_id, started)) = sessions.get(key).cloned()
		{
			sessions.insert(alias.to_string(), (span_id, started));
		}
	}

	fn close_session(&self, key: &str, span_id: &str, duration: Duration) {
		let mut matched = false;
		if let Ok(mut sessions) = self.sessions.lock()
			&& sessions.get(key).is_some_and(|(current, _)| current.as_str() == span_id)
		{
			// Remove the canonical key and every alias of this span.
			sessions.retain(|_, (current, _)| current.as_str() != span_id);
			matched = true;
		}
		if matched {
			self.send_end(
				span_id,
				None,
				SpanKind::Session,
				SpanStatus::Closed,
				duration,
				TokenRecord::default(),
			);
		}
	}

	/// Close every still-open session trace. Used by abrupt-exit teardown so a
	/// Ctrl-C cannot leave session boundaries unterminated.
	fn close_all_sessions(&self) {
		let sessions = self
			.sessions
			.lock()
			.map(|mut sessions| std::mem::take(&mut *sessions))
			.unwrap_or_default();
		let mut seen = std::collections::HashSet::new();
		for (_, (span_id, started)) in sessions {
			if seen.insert(span_id.clone()) {
				self.send_end(
					&span_id,
					None,
					SpanKind::Session,
					SpanStatus::Closed,
					started.elapsed(),
					TokenRecord::default(),
				);
			}
		}
	}

	fn send_end(
		&self,
		span_id: &str,
		parent: Option<&str>,
		kind: SpanKind,
		status: SpanStatus,
		duration: Duration,
		tokens: TokenRecord,
	) {
		self.send(Message::Record(Box::new(SpanEvent {
			span_id:        span_id.to_string(),
			parent_span_id: parent.map(str::to_string),
			kind,
			status,
			at_ms:          now_ms(),
			duration_ms:    Some(duration.as_millis() as u64),
			tokens,
		})));
	}

	/// Wait until the writer has persisted everything queued so far. Intended for
	/// tests and shutdown, not hot paths.
	pub fn flush(&self) {
		if self.closed.load(Ordering::Relaxed) {
			return;
		}
		let (ack_tx, ack_rx) = mpsc::sync_channel(0);
		if self.sender.send(Message::Flush(ack_tx)).is_err() {
			return;
		}
		let _ = ack_rx.recv_timeout(Duration::from_secs(5));
	}

	/// Drain and join the writer. Safe to call more than once.
	pub fn shutdown(&self) {
		self.close_all_sessions();
		if self.closed.swap(true, Ordering::SeqCst) {
			return;
		}
		let (ack_tx, ack_rx) = mpsc::sync_channel(0);
		let _ = self.sender.send(Message::Stop(ack_tx));
		let _ = ack_rx.recv_timeout(Duration::from_secs(5));
		let writer = self
			.writer
			.lock()
			.ok()
			.and_then(|mut writer| writer.take());
		if let Some(writer) = writer {
			let _ = writer.join();
		}
	}
}

impl Drop for Diagnostics {
	fn drop(&mut self) {
		self.shutdown();
	}
}

fn now_ms() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
}

fn warn_writer(warned: &AtomicBool) {
	if !warned.swap(true, Ordering::Relaxed) {
		eprintln!("warning: diagnostics write failed; dropping records");
	}
}

fn writer_loop(
	connection: Connection,
	receiver: Receiver<Message>,
	warned: Arc<AtomicBool>,
	path: PathBuf,
) {
	let mut statement = connection.prepare_cached(INSERT_SPAN).ok();
	let mut secure = true;
	let mut restricted = false;
	while let Ok(message) = receiver.recv() {
		match message {
			Message::Record(event) => {
				if !secure {
					continue;
				}
				let Some(statement) = statement.as_mut() else {
					warn_writer(&warned);
					continue;
				};
				let result = statement.execute(params![
					event.span_id,
					event.parent_span_id,
					event.kind.as_str(),
					event.status.as_str(),
					event.at_ms,
					event.duration_ms,
					event.tokens.input,
					event.tokens.output,
					event.tokens.cache_read,
					event.tokens.cache_write,
					event.tokens.reasoning,
				]);
				if result.is_err() {
					warn_writer(&warned);
					continue;
				}
				// The first successful write materializes WAL/SHM; clamp both
				// before any further rows are appended.
				if !restricted {
					if restrict_database_files(&path).is_ok() {
						restricted = true;
					} else {
						secure = false;
						warn_writer(&warned);
					}
				}
			},
			Message::Flush(ack) => {
				if secure && restrict_database_files(&path).is_err() {
					secure = false;
					warn_writer(&warned);
				}
				let _ = ack.send(());
			},
			Message::Stop(ack) => {
				if secure {
					let _ = restrict_database_files(&path);
				}
				let _ = ack.send(());
				break;
			},
		}
	}
}

struct SpanTimerInner {
	sink:    Arc<Diagnostics>,
	span_id: String,
	parent:  Option<String>,
	kind:    SpanKind,
	started: Instant,
}

/// One open boundary. Dropping without finishing records it as aborted.
#[derive(Debug)]
pub struct SpanTimer {
	inner: Option<SpanTimerInner>,
}

impl std::fmt::Debug for SpanTimerInner {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("SpanTimerInner")
			.field("span_id", &self.span_id)
			.field("kind", &self.kind)
			.finish()
	}
}

impl SpanTimer {
	#[must_use]
	pub fn span_id(&self) -> &str {
		self.inner.as_ref().map_or("", |inner| inner.span_id.as_str())
	}

	pub fn finish(self, status: SpanStatus, tokens: TokenRecord) {
		self.finish_with_duration(status, tokens, None);
	}

	/// Explicit duration when an upstream lifecycle already measured it;
	/// otherwise the local elapsed time is used.
	pub fn finish_with_duration(
		mut self,
		status: SpanStatus,
		tokens: TokenRecord,
		duration: Option<Duration>,
	) {
		if let Some(inner) = self.inner.take() {
			let elapsed = duration.unwrap_or_else(|| inner.started.elapsed());
			inner
				.sink
				.send_end(&inner.span_id, inner.parent.as_deref(), inner.kind, status, elapsed, tokens);
		}
	}
}

impl Drop for SpanTimer {
	fn drop(&mut self) {
		if let Some(inner) = self.inner.take() {
			let elapsed = inner.started.elapsed();
			inner.sink.send_end(
				&inner.span_id,
				inner.parent.as_deref(),
				inner.kind,
				SpanStatus::Aborted,
				elapsed,
				TokenRecord::default(),
			);
		}
	}
}

struct SessionTraceInner {
	sink:    Arc<Diagnostics>,
	key:     String,
	span_id: String,
	started: Instant,
}

/// Session lifetime handle. Dropping closes the session boundary.
pub struct SessionTrace {
	inner: Option<SessionTraceInner>,
}

impl std::fmt::Debug for SessionTrace {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("SessionTrace")
			.field("span_id", &self.span_id())
			.finish()
	}
}

impl SessionTrace {
	#[must_use]
	pub fn span_id(&self) -> &str {
		self.inner.as_ref().map_or("", |inner| inner.span_id.as_str())
	}
}

impl Drop for SessionTrace {
	fn drop(&mut self) {
		if let Some(inner) = self.inner.take() {
			inner
				.sink
				.close_session(&inner.key, &inner.span_id, inner.started.elapsed());
		}
	}
}

struct ChildTrace {
	subagent: SpanTimer,
	model:    Option<SpanTimer>,
	tools:    HashMap<String, SpanTimer>,
}

/// Subagent observer that maps child lifecycle and child model/tool events onto
/// diagnostics rows. Raw child ids stay in this in-memory map only.
pub struct SubagentRecorder {
	sink:  Arc<Diagnostics>,
	state: Mutex<HashMap<String, ChildTrace>>,
}

impl SubagentRecorder {
	#[must_use]
	pub fn new(sink: Arc<Diagnostics>) -> Self {
		Self { sink, state: Mutex::new(HashMap::new()) }
	}

	pub fn observe(&self, event: &SubagentEvent) {
		match event {
			SubagentEvent::Lifecycle(lifecycle) => self.observe_lifecycle(lifecycle),
			SubagentEvent::Progress(_) => {},
			SubagentEvent::Event { id, event } => self.observe_child_event(id, event),
		}
	}

	fn observe_lifecycle(&self, lifecycle: &pi_agent::supervisor::SubagentLifecycle) {
		let Ok(mut state) = self.state.lock() else {
			return;
		};
		if lifecycle.status == SubagentLifecycleStatus::Started {
			state.entry(lifecycle.id.clone()).or_insert_with(|| ChildTrace {
				subagent: self.sink.begin_span(SpanKind::Subagent, None),
				model:    None,
				tools:    HashMap::new(),
			});
			return;
		}
		let Some(mut child) = state.remove(&lifecycle.id) else {
			return;
		};
		if let Some(model) = child.model.take() {
			model.finish(SpanStatus::Aborted, TokenRecord::default());
		}
		for (_, tool) in child.tools.drain() {
			tool.finish(SpanStatus::Aborted, TokenRecord::default());
		}
		child.subagent.finish_with_duration(
			subagent_status(lifecycle.status),
			TokenRecord::default(),
			lifecycle.duration_ms.map(Duration::from_millis),
		);
	}

	fn observe_child_event(&self, id: &str, event: &pi_agent::AgentEvent) {
		let Ok(mut state) = self.state.lock() else {
			return;
		};
		let Some(child) = state.get_mut(id) else {
			return;
		};
		match event {
			pi_agent::AgentEvent::MessageUpdate { .. }
			| pi_agent::AgentEvent::ThinkingUpdate { .. } => {
				if child.model.is_none() {
					let parent = child.subagent.span_id().to_string();
					child.model = Some(self.sink.begin_span(SpanKind::Model, Some(&parent)));
				}
			},
			pi_agent::AgentEvent::MessageEnd { message, status } => {
				if child.model.is_none() {
					let parent = child.subagent.span_id().to_string();
					child.model = Some(self.sink.begin_span(SpanKind::Model, Some(&parent)));
				}
				if let Some(model) = child.model.take() {
					model.finish(model_status(status), TokenRecord::from_usage(message.usage.as_ref()));
				}
			},
			pi_agent::AgentEvent::ToolStart { id: tool_id, name, .. } => {
				let parent = child.subagent.span_id().to_string();
				let span = self.sink.begin_span(tool_kind(name), Some(&parent));
				child.tools.insert(tool_id.clone(), span);
			},
			pi_agent::AgentEvent::ToolEnd { id: tool_id, is_error, .. } => {
				if let Some(tool) = child.tools.remove(tool_id) {
					let status = if *is_error { SpanStatus::Failed } else { SpanStatus::Succeeded };
					tool.finish(status, TokenRecord::default());
				}
			},
			pi_agent::AgentEvent::ToolUpdate { .. }
			| pi_agent::AgentEvent::TurnEnd { .. }
			| pi_agent::AgentEvent::UserSteer { .. }
			| pi_agent::AgentEvent::BackgroundResult { .. } => {},
		}
	}
}

impl SubagentObserver for SubagentRecorder {
	fn observe(&self, event: &SubagentEvent) {
		SubagentRecorder::observe(self, event);
	}
}

static GLOBAL: RwLock<Option<Arc<Diagnostics>>> = RwLock::new(None);
static INSTALL_WARNED: AtomicBool = AtomicBool::new(false);


/// Currently installed sink, if diagnostics are enabled and opened.
#[must_use]
pub fn current() -> Option<Arc<Diagnostics>> {
	GLOBAL.read().ok().and_then(|guard| guard.clone())
}

fn warn_install_failure() {
	if !INSTALL_WARNED.swap(true, Ordering::Relaxed) {
		eprintln!("warning: diagnostics disabled: cannot open database");
	}
}

/// Open the configured sink without installing it globally. `None` means the
/// setting is off (no file is created) or the database could not be opened.
#[must_use]
pub fn open_for_config(config: &Config, path: impl Into<PathBuf>) -> Option<Arc<Diagnostics>> {
	if !config.diagnostics.enabled() {
		return None;
	}
	match Diagnostics::open(path) {
		Ok(sink) => Some(sink),
		Err(_) => {
			warn_install_failure();
			None
		},
	}
}

/// Install the process-wide sink at startup. The returned guard drains and
/// joins the writer on drop, including every early-return path in `main`.
pub struct DiagnosticsGuard {
	installed: bool,
}

impl Drop for DiagnosticsGuard {
	fn drop(&mut self) {
		if self.installed {
			let sink = GLOBAL.write().ok().and_then(|mut guard| guard.take());
			if let Some(sink) = sink {
				sink.shutdown();
			}
		}
	}
}

pub fn install(config: &Config) -> DiagnosticsGuard {
	if !config.diagnostics.enabled() {
		return DiagnosticsGuard { installed: false };
	}
	let path = database_path();
	let Ok(sink) = Diagnostics::open(path) else {
		warn_install_failure();
		return DiagnosticsGuard { installed: false };
	};
	let Ok(mut guard) = GLOBAL.write() else {
		return DiagnosticsGuard { installed: false };
	};
	if guard.is_some() {
		return DiagnosticsGuard { installed: false };
	}
	*guard = Some(sink);
	DiagnosticsGuard { installed: true }
}

/// Correlation id of an open session, used as parent for its model/tool spans.
#[must_use]
pub fn session_span(key: Option<&str>) -> Option<String> {
	let key = key?;
	current()?.session_span(key)
}

/// Explicit trace alias for callers whose provider-facing run id differs from
/// the id used to open the session store.
pub fn alias_session(alias: &str, key: &str) {
	if let Some(sink) = current() {
		sink.alias_session(alias, key);
	}
}

/// Close the global sink early (tests and explicit teardown).
pub fn shutdown() {
	let sink = GLOBAL.write().ok().and_then(|mut guard| guard.take());
	if let Some(sink) = sink {
		sink.shutdown();
	}
}

/// Fresh observer for one supervisor scope. Callers must retain the returned
/// `Arc` across turns so `subscribe_once` stays idempotent; separate scopes
/// must not share it because child ids are only unique within a supervisor.
#[must_use]
pub fn subagent_observer() -> Option<Arc<dyn SubagentObserver>> {
	current().map(|sink| Arc::new(SubagentRecorder::new(sink)) as Arc<dyn SubagentObserver>)
}

/// Session trace for a live [`crate::session::SessionStore`]. `None` when
/// diagnostics are disabled or no global sink is installed.
#[must_use]
pub fn start_session(key: &str) -> Option<SessionTrace> {
	current().map(|sink| sink.start_session(key))
}

#[cfg(all(test, unix))]
mod security_tests {
	use super::{exclusive_group, trusted_owner};

	#[test]
	fn only_root_current_or_android_system_owners_are_trusted() {
		assert!(trusted_owner(0, 10_249));
		assert!(trusted_owner(10_249, 10_249));
		assert!(!trusted_owner(12_345, 10_249));
		#[cfg(target_os = "android")]
		assert!(trusted_owner(1000, 10_249));
		#[cfg(not(target_os = "android"))]
		assert!(!trusted_owner(1000, 12_345));
	}

	#[test]
	fn only_platform_exclusive_groups_are_accepted() {
		assert!(!exclusive_group(0, 10_249, 0), "gid 0 is not exclusive");
		assert!(!exclusive_group(12_345, 10_249, 12_345));
		#[cfg(target_os = "android")]
		{
			assert!(exclusive_group(1000, 10_249, 1000));
			assert!(exclusive_group(0, 10_249, 1000));
			assert!(exclusive_group(10_249, 10_249, 10_249));
			assert!(!exclusive_group(10_249, 10_249, 9997));
		}
	}
}
