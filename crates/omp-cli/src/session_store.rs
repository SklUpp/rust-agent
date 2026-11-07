//! Durable SQLite authority for sessions, branches, messages, tool execution
//! intents/outcomes, worker deliveries, and artifact metadata.
//!
//! The API is synchronous by design: the runtime owns one writer and calls it
//! off-thread. There is no actor/channel framework here, and no generic
//! persistence layer. Every write either commits as one SQLite transaction or
//! returns an error to the caller; failures are never downgraded to warnings.
//!
//! The database path is explicit. This module never opens or falls back to the
//! credential store (`agent.db`) or a default location.
//!
//! Contract anchors:
//! - [`SqliteStore::record_intent`] must commit before a tool effect starts.
//! - [`SqliteStore::record_outcome`] is terminal and immutable; an equal
//!   duplicate is idempotent, a conflicting duplicate is an error.
//! - [`SqliteStore::unresolved_intents`] reports intents with no outcome. The
//!   stored intent is not proof the effect started; callers must use the tool
//!   contract, never a blind retry.
//! - [`SqliteStore::complete_worker`] commits the worker completion and the
//!   parent delivery atomically. The delivery keeps the origin branch even
//!   after the session's active branch changes.
//! - `delivered_at_ms` and `processed_at_ms` are distinct states. Delivery is
//!   at-least-once; marking processed is not implied by handing it to a context.

use std::{
	collections::{HashMap, HashSet},
	path::{Path, PathBuf},
	sync::atomic::{AtomicU64, Ordering},
	time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{
	Connection, OptionalExtension as _, Transaction, TransactionBehavior, params,
};
use serde_json::{Value, json};

/// Current schema version. Bump only with a new branch in [`SqliteStore::initialize`].
pub const SCHEMA_VERSION: i64 = 1;

const SCHEMA_VERSION_DDL: &str = "
	CREATE TABLE IF NOT EXISTS schema_version (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		version INTEGER NOT NULL
	);
";

const SCHEMA_V1_DDL: &str = "
	CREATE TABLE sessions (
		id TEXT PRIMARY KEY,
		parent_session_id TEXT REFERENCES sessions(id),
		active_branch_id TEXT,
		payload_json TEXT NOT NULL,
		created_at_ms INTEGER NOT NULL,
		updated_at_ms INTEGER NOT NULL
	);

	CREATE TABLE branches (
		id TEXT PRIMARY KEY,
		session_id TEXT NOT NULL REFERENCES sessions(id),
		parent_branch_id TEXT REFERENCES branches(id),
		parent_message_id TEXT,
		leaf_message_id TEXT,
		payload_json TEXT NOT NULL,
		created_at_ms INTEGER NOT NULL
	);

	CREATE TABLE messages (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		id TEXT NOT NULL UNIQUE,
		session_id TEXT NOT NULL REFERENCES sessions(id),
		branch_id TEXT NOT NULL REFERENCES branches(id),
		parent_message_id TEXT REFERENCES messages(id),
		payload_json TEXT NOT NULL,
		created_at_ms INTEGER NOT NULL
	);

	CREATE TABLE executions (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		execution_id TEXT NOT NULL UNIQUE,
		session_id TEXT NOT NULL REFERENCES sessions(id),
		branch_id TEXT NOT NULL REFERENCES branches(id),
		provider_call_id TEXT NOT NULL,
		tool_name TEXT NOT NULL,
		intent_json TEXT NOT NULL,
		created_at_ms INTEGER NOT NULL,
		outcome_json TEXT,
		outcome_at_ms INTEGER
	);

	CREATE TABLE deliveries (
		delivery_id TEXT PRIMARY KEY,
		worker_id TEXT NOT NULL,
		session_id TEXT NOT NULL REFERENCES sessions(id),
		origin_branch_id TEXT NOT NULL REFERENCES branches(id),
		payload_json TEXT NOT NULL,
		created_at_ms INTEGER NOT NULL,
		delivered_at_ms INTEGER,
		processed_at_ms INTEGER
	);

	CREATE TABLE worker_completions (
		worker_id TEXT PRIMARY KEY,
		session_id TEXT NOT NULL REFERENCES sessions(id),
		origin_branch_id TEXT NOT NULL REFERENCES branches(id),
		delivery_id TEXT NOT NULL UNIQUE REFERENCES deliveries(delivery_id),
		completion_json TEXT NOT NULL,
		completed_at_ms INTEGER NOT NULL
	);

	CREATE TABLE artifacts (
		artifact_id TEXT PRIMARY KEY,
		session_id TEXT NOT NULL REFERENCES sessions(id),
		branch_id TEXT REFERENCES branches(id),
		execution_id TEXT REFERENCES executions(execution_id),
		location TEXT NOT NULL,
		metadata_json TEXT NOT NULL,
		created_at_ms INTEGER NOT NULL
	);

	CREATE INDEX sessions_parent_idx ON sessions(parent_session_id);
	CREATE INDEX branches_session_idx ON branches(session_id);
	CREATE INDEX messages_session_seq_idx ON messages(session_id, seq);
	CREATE INDEX messages_branch_seq_idx ON messages(branch_id, seq);
	CREATE INDEX messages_parent_idx ON messages(parent_message_id);
	CREATE INDEX executions_unresolved_idx ON executions(session_id) WHERE outcome_json IS NULL;
	CREATE INDEX deliveries_pending_idx ON deliveries(session_id, delivered_at_ms, processed_at_ms);
	CREATE INDEX artifacts_session_idx ON artifacts(session_id);

	CREATE TRIGGER executions_outcome_immutable
	BEFORE UPDATE OF outcome_json ON executions
	WHEN OLD.outcome_json IS NOT NULL
	BEGIN
		SELECT RAISE(ABORT, 'execution outcome is terminal');
	END;
";

/// Store failure. Every variant is returned to the caller; nothing is logged
/// and ignored.
#[derive(Debug)]
pub enum StoreError {
	/// SQLite rejected the operation.
	Sqlite(rusqlite::Error),
	/// A stored or supplied JSON payload could not be encoded/decoded.
	Json(serde_json::Error),
	/// Filesystem failure while creating the database parent directory.
	Io(std::io::Error),
	/// Caller supplied an incomplete or inconsistent record.
	Validation(String),
	/// Durable state conflicts with the requested write.
	Conflict(String),
	/// Referenced durable state is missing.
	NotFound(String),
	/// Stored state is internally inconsistent (for example a broken branch chain).
	Corrupt(String),
	/// The schema is newer than this binary or cannot be migrated.
	Schema(String),
}

impl std::fmt::Display for StoreError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Sqlite(err) => write!(f, "session store: {err}"),
			Self::Json(err) => write!(f, "session store: invalid JSON payload: {err}"),
			Self::Io(err) => write!(f, "session store: {err}"),
			Self::Validation(err) => write!(f, "session store: {err}"),
			Self::Conflict(err) => write!(f, "session store conflict: {err}"),
			Self::NotFound(err) => write!(f, "session store: {err}"),
			Self::Corrupt(err) => write!(f, "session store is inconsistent: {err}"),
			Self::Schema(err) => write!(f, "session store schema: {err}"),
		}
	}
}

impl std::error::Error for StoreError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Sqlite(err) => Some(err),
			Self::Json(err) => Some(err),
			Self::Io(err) => Some(err),
			Self::Validation(_)
			| Self::Conflict(_)
			| Self::NotFound(_)
			| Self::Corrupt(_)
			| Self::Schema(_) => None,
		}
	}
}

impl From<rusqlite::Error> for StoreError {
	fn from(err: rusqlite::Error) -> Self {
		Self::Sqlite(err)
	}
}

impl From<serde_json::Error> for StoreError {
	fn from(err: serde_json::Error) -> Self {
		Self::Json(err)
	}
}

impl From<std::io::Error> for StoreError {
	fn from(err: std::io::Error) -> Self {
		Self::Io(err)
	}
}

pub type StoreResult<T> = Result<T, StoreError>;

/// A durable session row. `payload` is the session header/state JSON and is
/// stored exactly once, in `sessions.payload_json`.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionRecord {
	pub id: String,
	pub parent_session_id: Option<String>,
	pub active_branch_id: Option<String>,
	pub payload: Value,
	pub created_at_ms: i64,
	pub updated_at_ms: i64,
}

/// A branch row. `parent_message_id` is the branch point; `leaf_message_id`
/// advances only when a message is appended to this branch, not when another
/// branch becomes active.
#[derive(Debug, Clone, PartialEq)]
pub struct BranchRecord {
	pub id: String,
	pub session_id: String,
	pub parent_branch_id: Option<String>,
	pub parent_message_id: Option<String>,
	pub leaf_message_id: Option<String>,
	pub payload: Value,
	pub created_at_ms: i64,
}

/// A message row. The full message entry is `payload`; the other columns are
/// linkage/ordering metadata only and never a second mutable message body.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageRecord {
	pub id: String,
	pub session_id: String,
	pub branch_id: String,
	pub parent_message_id: Option<String>,
	pub payload: Value,
	pub seq: i64,
	pub created_at_ms: i64,
}

/// New session plus its initial root branch, committed in one transaction.
#[derive(Debug, Clone)]
pub struct NewSession {
	pub id: String,
	pub parent_session_id: Option<String>,
	pub payload: Value,
	pub initial_branch_id: String,
	pub initial_branch_payload: Value,
}

/// New branch. Set both `parent_branch_id` and `parent_message_id` for a child
/// branch, or leave both `None` for an explicit empty root branch inside the
/// session. A parent branch without a point, or a point without its declared
/// parent branch, is rejected; the point must be in the parent's ancestry.
/// Creation does not change the active branch.
#[derive(Debug, Clone)]
pub struct NewBranch {
	pub id: String,
	pub session_id: String,
	pub parent_branch_id: Option<String>,
	pub parent_message_id: Option<String>,
	pub payload: Value,
}

/// New message. `parent_message_id` must equal the branch leaf for every message
/// after the first, or the branch point for the first message.
#[derive(Debug, Clone)]
pub struct NewMessage {
	pub id: String,
	pub session_id: String,
	pub branch_id: String,
	pub parent_message_id: Option<String>,
	pub payload: Value,
}

/// A tool execution intent. The execution key is generated by the store and is
/// deliberately separate from `provider_call_id`.
#[derive(Debug, Clone)]
pub struct NewIntent {
	pub session_id: String,
	pub branch_id: String,
	pub provider_call_id: String,
	pub tool_name: String,
	pub payload: Value,
}

/// Intent with no durable outcome yet. Its effect may or may not have started.
#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedIntent {
	pub execution_id: String,
	pub session_id: String,
	pub branch_id: String,
	pub provider_call_id: String,
	pub tool_name: String,
	pub payload: Value,
	pub created_at_ms: i64,
}

/// Execution row in the store, recording intent and optional terminal outcome.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionRecord {
	pub seq: i64,
	pub execution_id: String,
	pub session_id: String,
	pub branch_id: String,
	pub provider_call_id: String,
	pub tool_name: String,
	pub intent: Value,
	pub created_at_ms: i64,
	pub outcome: Option<Value>,
	pub outcome_at_ms: Option<i64>,
}

/// Worker completion and the parent notification that must commit with it.
/// `worker_id` is the durable worker identity, not a reusable display index.
/// `delivery_id` is caller-owned and must stay stable across retries and
/// process restarts (for example the spawn-time result key); it is the
/// deduplication identity. The two ids may be the same string: they name two
/// distinct rows (completion + delivery), not two effects. A side effect is
/// still never replayed just because a row is re-read.
#[derive(Debug, Clone)]
pub struct WorkerCompletion {
	pub worker_id: String,
	pub delivery_id: String,
	pub session_id: String,
	pub origin_branch_id: String,
	pub completion: Value,
	pub delivery: Value,
}

/// A standalone pending delivery (peer mail) with no worker completion. Stable
/// `delivery_id` is caller-owned and is the deduplication identity.
#[derive(Debug, Clone)]
pub struct NewDelivery {
	pub delivery_id: String,
	pub worker_id: String,
	pub session_id: String,
	pub origin_branch_id: String,
	pub payload: Value,
}

/// One durable delivery. `delivered_at_ms` means the message was durably offered
/// to the target context; `processed_at_ms` means the target consumed it. They
/// are independent and neither is inferred from the other.
#[derive(Debug, Clone, PartialEq)]
pub struct DeliveryRecord {
	pub delivery_id: String,
	pub worker_id: String,
	pub session_id: String,
	pub origin_branch_id: String,
	pub payload: Value,
	pub created_at_ms: i64,
	pub delivered_at_ms: Option<i64>,
	pub processed_at_ms: Option<i64>,
}

/// Metadata for an already-published external artifact. This module never
/// reads, writes, validates, or deletes the artifact file itself.
#[derive(Debug, Clone)]
pub struct NewArtifact {
	pub artifact_id: String,
	pub session_id: String,
	pub branch_id: Option<String>,
	pub execution_id: Option<String>,
	/// Opaque URI or filesystem path recorded as metadata.
	pub location: String,
	pub metadata: Value,
}

/// A stored artifact metadata row.
#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactRecord {
	pub artifact_id: String,
	pub session_id: String,
	pub branch_id: Option<String>,
	pub execution_id: Option<String>,
	pub location: String,
	pub metadata: Value,
	pub created_at_ms: i64,
}

/// Synchronous SQLite session store. The runtime owns one instance on its
/// writer thread; `Connection` is `Send` but not `Sync`.
pub struct SqliteStore {
	path: PathBuf,
	connection: Connection,
}

impl SqliteStore {
	/// Open (or create) a dedicated session database. Parent directories are
	/// created when missing. The path is explicit: this never reuses `agent.db`
	/// or any credential database.
	pub fn open(path: impl Into<PathBuf>) -> StoreResult<Self> {
		let path = path.into();
		if path.as_os_str().is_empty() {
			return Err(StoreError::Validation(
				"session store path must not be empty".to_string(),
			));
		}
		if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
			std::fs::create_dir_all(parent)?;
		}
		let connection = Connection::open(&path)?;
		Self::from_connection(path, connection)
	}

	/// In-memory store for tests and short-lived hosts. Same schema and
	/// transaction rules as [`Self::open`].
	pub fn open_in_memory() -> StoreResult<Self> {
		let connection = Connection::open_in_memory()?;
		Self::from_connection(PathBuf::from(":memory:"), connection)
	}

	fn from_connection(path: PathBuf, mut connection: Connection) -> StoreResult<Self> {
		connection.busy_timeout(std::time::Duration::from_secs(5))?;
		connection.pragma_update(None, "foreign_keys", "ON")?;
		let _: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
		// Requested policy: FULL synchronous commits at intent/outcome
		// boundaries. This is not a power-loss guarantee; filesystem, hardware,
		// and WAL checkpoint behavior remain platform concerns.
		connection.pragma_update(None, "synchronous", "FULL")?;
		let mut store = Self { path, connection };
		store.initialize()?;
		Ok(store)
	}

	#[must_use]
	pub fn path(&self) -> &Path {
		&self.path
	}

	/// Create the schema or verify the recorded version, in one transaction.
	fn initialize(&mut self) -> StoreResult<()> {
		let transaction = self.connection.transaction()?;
		transaction.execute_batch(SCHEMA_VERSION_DDL)?;
		let recorded: Option<i64> = transaction
			.query_row("SELECT version FROM schema_version WHERE id = 1", [], |row| {
				row.get(0)
			})
			.optional()?;
		match recorded {
			None => {
				let existing_table: Option<String> = transaction
					.query_row(
						"SELECT name FROM sqlite_master \
						 WHERE type = 'table' AND name IN ('sessions', 'branches', 'messages', \
						 'executions', 'deliveries', 'worker_completions', 'artifacts') \
						 ORDER BY name LIMIT 1",
						[],
						|row| row.get(0),
					)
					.optional()?;
				if let Some(table) = existing_table {
					return Err(StoreError::Schema(format!(
						"schema_version row is missing but table {table} exists; refusing to recreate the schema"
					)));
				}
				transaction.execute_batch(SCHEMA_V1_DDL)?;
				transaction.execute(
					"INSERT INTO schema_version (id, version) VALUES (1, ?1)",
					params![SCHEMA_VERSION],
				)?;
			},
			Some(version) if version == SCHEMA_VERSION => {},
			Some(version) if version < SCHEMA_VERSION => {
				return Err(StoreError::Schema(format!(
					"database is version {version}, this binary only supports {SCHEMA_VERSION}"
				)));
			},
			Some(version) => {
				return Err(StoreError::Schema(format!(
					"database version {version} is newer than supported {SCHEMA_VERSION}"
				)));
			},
		}
		transaction.commit()?;
		Ok(())
	}

	/// Insert a session and its initial branch atomically, then make that branch
	/// active. A crash can never leave a session with no active branch.
	pub fn create_session(&mut self, session: NewSession) -> StoreResult<()> {
		validate_id("session id", &session.id)?;
		validate_id("initial branch id", &session.initial_branch_id)?;
		if session.parent_session_id.as_deref() == Some(session.id.as_str()) {
			return Err(StoreError::Validation(
				"a session cannot be its own parent".to_string(),
			));
		}
		let now = now_ms();
		let transaction = self.connection.transaction()?;
		let exists: bool = transaction.query_row(
			"SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
			params![session.id],
			|row| row.get(0),
		)?;
		if exists {
			return Err(StoreError::Conflict(format!(
				"session {} already exists",
				session.id
			)));
		}
		let session_payload = encode_json(&session.payload)?;
		let branch_payload = encode_json(&session.initial_branch_payload)?;
		transaction.execute(
			"INSERT INTO sessions \
			 (id, parent_session_id, active_branch_id, payload_json, created_at_ms, updated_at_ms) \
			 VALUES (?1, ?2, NULL, ?3, ?4, ?4)",
			params![session.id, session.parent_session_id, session_payload, now],
		)?;
		transaction.execute(
			"INSERT INTO branches \
			 (id, session_id, parent_branch_id, parent_message_id, leaf_message_id, payload_json, created_at_ms) \
			 VALUES (?1, ?2, NULL, NULL, NULL, ?3, ?4)",
			params![
				session.initial_branch_id,
				session.id,
				branch_payload,
				now
			],
		)?;
		transaction.execute(
			"UPDATE sessions SET active_branch_id = ?1 WHERE id = ?2",
			params![session.initial_branch_id, session.id],
		)?;
		transaction.commit()?;
		Ok(())
	}

	/// Load a session by id.
	pub fn session(&self, id: &str) -> StoreResult<Option<SessionRecord>> {
		let row: Option<(String, Option<String>, Option<String>, String, i64, i64)> = self
			.connection
			.query_row(
				"SELECT id, parent_session_id, active_branch_id, payload_json, created_at_ms, updated_at_ms \
				 FROM sessions WHERE id = ?1",
				params![id],
				|row| {
					Ok((
						row.get(0)?,
						row.get(1)?,
						row.get(2)?,
						row.get(3)?,
						row.get(4)?,
						row.get(5)?,
					))
				},
			)
			.optional()?;
		row.map(
			|(id, parent_session_id, active_branch_id, payload_json, created_at_ms, updated_at_ms)| {
				Ok(SessionRecord {
					id,
					parent_session_id,
					active_branch_id,
					payload: decode_json(&payload_json)?,
					created_at_ms,
					updated_at_ms,
				})
			},
		)
		.transpose()
	}

	/// Create a branch. It is not activated automatically; late/off-branch writes
	/// must not move the session's active leaf.
	pub fn create_branch(&mut self, branch: NewBranch) -> StoreResult<()> {
		validate_id("branch id", &branch.id)?;
		validate_id("branch session id", &branch.session_id)?;
		let now = now_ms();
		let transaction = self.connection.transaction()?;
		require_session(&transaction, &branch.session_id)?;
		let exists: bool = transaction.query_row(
			"SELECT EXISTS(SELECT 1 FROM branches WHERE id = ?1)",
			params![branch.id],
			|row| row.get(0),
		)?;
		if exists {
			return Err(StoreError::Conflict(format!(
				"branch {} already exists",
				branch.id
			)));
		}
		match (&branch.parent_branch_id, &branch.parent_message_id) {
			(None, None) => {
				// Explicit empty root branch inside the session. It has no
				// ancestry until its first message, which must also be a root.
			},
			(Some(_), None) => {
				return Err(StoreError::Validation(
					"a child branch requires a branch point message".to_string(),
				));
			},
			(None, Some(_)) => {
				return Err(StoreError::Validation(
					"a branch point requires a declared parent branch".to_string(),
				));
			},
			(Some(parent_branch_id), Some(parent_message_id)) => {
				let point_session: Option<String> = transaction
					.query_row(
						"SELECT session_id FROM messages WHERE id = ?1",
						params![parent_message_id.as_str()],
						|row| row.get(0),
					)
					.optional()?;
				match point_session {
					None => {
						return Err(StoreError::NotFound(format!(
							"branch point message {parent_message_id} does not exist"
						)));
					},
					Some(point_session) if point_session != branch.session_id => {
						return Err(StoreError::Validation(
							"branch point belongs to a different session".to_string(),
						));
					},
					Some(_) => {},
				}
				if !branch_point_on_ancestry(
					&transaction,
					parent_branch_id,
					&branch.session_id,
					parent_message_id,
				)? {
					return Err(StoreError::Validation(
						"branch point message is not on the declared parent branch ancestry"
							.to_string(),
					));
				}
			},
		}
		transaction.execute(
			"INSERT INTO branches \
			 (id, session_id, parent_branch_id, parent_message_id, leaf_message_id, payload_json, created_at_ms) \
			 VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6)",
			params![
				branch.id,
				branch.session_id,
				branch.parent_branch_id,
				branch.parent_message_id,
				encode_json(&branch.payload)?,
				now
			],
		)?;
		transaction.execute(
			"UPDATE sessions SET updated_at_ms = ?1 WHERE id = ?2",
			params![now, branch.session_id],
		)?;
		transaction.commit()?;
		Ok(())
	}

	/// Load a branch by id.
	pub fn branch(&self, id: &str) -> StoreResult<Option<BranchRecord>> {
		let row: Option<(
			String,
			String,
			Option<String>,
			Option<String>,
			Option<String>,
			String,
			i64,
		)> = self
			.connection
			.query_row(
				"SELECT id, session_id, parent_branch_id, parent_message_id, leaf_message_id, payload_json, created_at_ms \
				 FROM branches WHERE id = ?1",
				params![id],
				|row| {
					Ok((
						row.get(0)?,
						row.get(1)?,
						row.get(2)?,
						row.get(3)?,
						row.get(4)?,
						row.get(5)?,
						row.get(6)?,
					))
				},
			)
			.optional()?;
		row.map(
			|(
				id,
				session_id,
				parent_branch_id,
				parent_message_id,
				leaf_message_id,
				payload_json,
				created_at_ms,
			)| {
				Ok(BranchRecord {
					id,
					session_id,
					parent_branch_id,
					parent_message_id,
					leaf_message_id,
					payload: decode_json(&payload_json)?,
					created_at_ms,
				})
			},
		)
		.transpose()
	}

	/// Move the session's active branch. Existing branches and their leaves are
	/// untouched; a late append to the old branch stays off the active branch.
	pub fn set_active_branch(&mut self, session_id: &str, branch_id: &str) -> StoreResult<()> {
		validate_id("session id", session_id)?;
		validate_id("branch id", branch_id)?;
		let transaction = self.connection.transaction()?;
		let branch_session: Option<String> = transaction
			.query_row(
				"SELECT session_id FROM branches WHERE id = ?1",
				params![branch_id],
				|row| row.get(0),
			)
			.optional()?;
		match branch_session {
			None => {
				return Err(StoreError::NotFound(format!(
					"branch {branch_id} does not exist"
				)));
			},
			Some(branch_session) if branch_session != session_id => {
				return Err(StoreError::Validation(
					"branch belongs to a different session".to_string(),
				));
			},
			Some(_) => {},
		}
		let changed = transaction.execute(
			"UPDATE sessions SET active_branch_id = ?1, updated_at_ms = ?2 WHERE id = ?3",
			params![branch_id, now_ms(), session_id],
		)?;
		if changed == 0 {
			return Err(StoreError::NotFound(format!(
				"session {session_id} does not exist"
			)));
		}
		transaction.commit()?;
		Ok(())
	}

	/// Atomically cut to a new branch: create the new branch, switch active branch,
	/// clear durableProjection from the session payload, and append the branch summary message.
	/// All committed in a single IMMEDIATE transaction.
	pub fn cut_branch(
		&mut self,
		branch: NewBranch,
		summary_message: NewMessage,
	) -> StoreResult<()> {
		validate_id("branch id", &branch.id)?;
		validate_id("branch session id", &branch.session_id)?;
		validate_id("summary message id", &summary_message.id)?;
		validate_id("summary session id", &summary_message.session_id)?;
		validate_id("summary branch id", &summary_message.branch_id)?;
		if branch.session_id != summary_message.session_id {
			return Err(StoreError::Validation(
				"branch and summary message session ids do not match".to_string(),
			));
		}
		if branch.id != summary_message.branch_id {
			return Err(StoreError::Validation(
				"summary message branch id does not match new branch id".to_string(),
			));
		}
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;

		// Validate session exists
		let session_row: Option<(String, Option<String>)> = transaction
			.query_row(
				"SELECT payload_json, active_branch_id FROM sessions WHERE id = ?1",
				params![branch.session_id],
				|row| Ok((row.get(0)?, row.get(1)?)),
			)
			.optional()?;
		let Some((session_payload_json, _active_branch)) = session_row else {
			return Err(StoreError::NotFound(format!(
				"session {} not found",
				branch.session_id
			)));
		};

		// Validate branch does not already exist
		let exists: bool = transaction.query_row(
			"SELECT COUNT(*) > 0 FROM branches WHERE id = ?1",
			params![branch.id],
			|row| row.get(0),
		)?;
		if exists {
			return Err(StoreError::Conflict(format!("branch {} already exists", branch.id)));
		}
		match (&branch.parent_branch_id, &branch.parent_message_id) {
			(None, None) => {},
			(None, Some(_)) => {
				return Err(StoreError::Validation(
					"branch point message requires a parent branch".to_string(),
				));
			},
			(Some(_), None) => {
				return Err(StoreError::Validation(
					"parent branch requires an explicit branch point message".to_string(),
				));
			},
			(Some(parent_branch_id), Some(parent_message_id)) => {
				validate_id("parent branch id", parent_branch_id)?;
				validate_id("parent message id", parent_message_id)?;
				if !branch_point_on_ancestry(
					&transaction,
					parent_branch_id,
					&branch.session_id,
					parent_message_id,
				)? {
					return Err(StoreError::Validation(
						"branch point message is not on the declared parent branch ancestry"
							.to_string(),
					));
				}
			},
		}

		transaction.execute(
			"INSERT INTO branches \
			 (id, session_id, parent_branch_id, parent_message_id, leaf_message_id, payload_json, created_at_ms) \
			 VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6)",
			params![
				branch.id,
				branch.session_id,
				branch.parent_branch_id,
				branch.parent_message_id,
				encode_json(&branch.payload)?,
				now
			],
		)?;

		// Update session: switch active_branch_id and clear durableProjection
		let mut session_payload: Value = decode_json(&session_payload_json)?;
		if let Some(map) = session_payload.as_object_mut() {
			map.remove("durableProjection");
		}
		let updated_payload_json = encode_json(&session_payload)?;
		transaction.execute(
			"UPDATE sessions SET active_branch_id = ?1, payload_json = ?2, updated_at_ms = ?3 WHERE id = ?4",
			params![branch.id, updated_payload_json, now, branch.session_id],
		)?;

		// Append the summary message on the new branch
		append_message_tx(&transaction, &summary_message, now)?;

		transaction.commit()?;
		Ok(())
	}

	/// Append one message to an explicit branch. The branch leaf advances; the
	/// session's active branch does not change.
	pub fn append_message(&mut self, message: NewMessage) -> StoreResult<()> {
		let now = now_ms();
		let transaction = self.connection.transaction()?;
		append_message_tx(&transaction, &message, now)?;
		transaction.commit()?;
		Ok(())
	}

	/// Append a batch in one transaction. Any failure rolls back every message
	/// and every leaf update in the batch.
	pub fn append_messages(&mut self, messages: &[NewMessage]) -> StoreResult<()> {
		let now = now_ms();
		let transaction = self.connection.transaction()?;
		for message in messages {
			append_message_tx(&transaction, message, now)?;
		}
		transaction.commit()?;
		Ok(())
	}

	/// Load one message by id.
	pub fn message(&self, id: &str) -> StoreResult<Option<MessageRecord>> {
		let row: Option<(i64, String, String, String, Option<String>, String, i64)> = self
			.connection
			.query_row(
				"SELECT seq, id, session_id, branch_id, parent_message_id, payload_json, created_at_ms \
				 FROM messages WHERE id = ?1",
				params![id],
				|row| {
					Ok((
						row.get(0)?,
						row.get(1)?,
						row.get(2)?,
						row.get(3)?,
						row.get(4)?,
						row.get(5)?,
						row.get(6)?,
					))
				},
			)
			.optional()?;
		row.map(
			|(seq, id, session_id, branch_id, parent_message_id, payload_json, created_at_ms)| {
				Ok(MessageRecord {
					id,
					session_id,
					branch_id,
					parent_message_id,
					payload: decode_json(&payload_json)?,
					seq,
					created_at_ms,
				})
			},
		)
		.transpose()
	}

	/// Rebuild a branch's history in root-to-leaf order by following
	/// `parent_message_id` from the durable leaf. The chain may pass through
	/// ancestor messages that were appended on a parent branch; siblings not on
	/// the chain are excluded. A broken or cyclic chain is an error, not a
	/// silent fallback to physical journal order.
	pub fn branch_messages(&self, branch_id: &str) -> StoreResult<Vec<MessageRecord>> {
		let branch = self
			.branch(branch_id)?
			.ok_or_else(|| StoreError::NotFound(format!("branch {branch_id} does not exist")))?;
		let mut cursor = branch
			.leaf_message_id
			.clone()
			.or_else(|| branch.parent_message_id.clone());
		let mut seen = HashSet::new();
		let mut messages = Vec::new();
		while let Some(id) = cursor {
			if !seen.insert(id.clone()) {
				return Err(StoreError::Corrupt(format!(
					"branch {branch_id} contains a message cycle at {id}"
				)));
			}
			let message = self.message(&id)?.ok_or_else(|| {
				StoreError::Corrupt(format!(
					"branch {branch_id} references missing message {id}"
				))
			})?;
			if message.session_id != branch.session_id {
				return Err(StoreError::Corrupt(format!(
					"branch {branch_id} chain crosses into session {} at {id}",
					message.session_id
				)));
			}
			cursor = message.parent_message_id.clone();
			messages.push(message);
		}
		messages.reverse();
		Ok(messages)
	}

	/// Record a tool intent and return its runtime-generated execution key. The
	/// key is intentionally not the provider's `tool_call_id`, so a provider
	/// retry and a durable execution attempt have different identities.
	pub fn record_intent(&mut self, intent: NewIntent) -> StoreResult<String> {
		validate_id("session id", &intent.session_id)?;
		validate_id("branch id", &intent.branch_id)?;
		validate_id("provider call id", &intent.provider_call_id)?;
		validate_id("tool name", &intent.tool_name)?;
		let execution_id = next_execution_id();
		if execution_id == intent.provider_call_id {
			return Err(StoreError::Corrupt(
				"generated execution key collided with the provider call id".to_string(),
			));
		}
		let now = now_ms();
		let transaction = self.connection.transaction()?;
		let branch_session: Option<String> = transaction
			.query_row(
				"SELECT session_id FROM branches WHERE id = ?1",
				params![intent.branch_id],
				|row| row.get(0),
			)
			.optional()?;
		match branch_session {
			None => {
				return Err(StoreError::NotFound(format!(
					"branch {} does not exist",
					intent.branch_id
				)));
			},
			Some(branch_session) if branch_session != intent.session_id => {
				return Err(StoreError::Validation(
					"intent branch belongs to a different session".to_string(),
				));
			},
			Some(_) => {},
		}
		transaction.execute(
			"INSERT INTO executions \
			 (execution_id, session_id, branch_id, provider_call_id, tool_name, intent_json, created_at_ms, outcome_json, outcome_at_ms) \
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL)",
			params![
				execution_id,
				intent.session_id,
				intent.branch_id,
				intent.provider_call_id,
				intent.tool_name,
				encode_json(&intent.payload)?,
				now
			],
		)?;
		transaction.commit()?;
		Ok(execution_id)
	}

	/// Persist a terminal outcome. The first outcome wins. Repeating the exact
	/// same value is idempotent; any different value is a conflict and is not
	/// written.
	///
	/// The write transaction is `IMMEDIATE`, so the outcome check and update
	/// share one write lock and do not upgrade from a read snapshot. SQLite busy
	/// errors still propagate to the caller; this is not a no-busy guarantee.
	pub fn record_outcome(&mut self, execution_id: &str, outcome: Value) -> StoreResult<()> {
		validate_id("execution id", execution_id)?;
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		record_outcome_tx(&transaction, execution_id, &outcome, now)?;
		transaction.commit()?;
		Ok(())
	}

	/// Atomically persist a terminal outcome and its corresponding tool-result
	/// message within a single `IMMEDIATE` transaction.
	///
	/// Preserves branch/leaf stale checks: if `message.parent_message_id` does
	/// not match the branch's current leaf, validation fails and neither row is
	/// committed.
	/// Repeating the exact same outcome and message is idempotent. Any conflicting
	/// outcome or lineage mismatch is rejected without updating either row.
	pub fn record_outcome_with_message(
		&mut self,
		execution_id: &str,
		outcome: Value,
		message: NewMessage,
	) -> StoreResult<bool> {
		validate_id("execution id", execution_id)?;
		validate_id("message id", &message.id)?;
		validate_id("message session id", &message.session_id)?;
		validate_id("message branch id", &message.branch_id)?;
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;

		let exec_meta: Option<(String, String, String, String)> = transaction
			.query_row(
				"SELECT session_id, branch_id, provider_call_id, tool_name FROM executions WHERE execution_id = ?1",
				params![execution_id],
				|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
			)
			.optional()?;
		let Some((exec_session, exec_branch, exec_call_id, exec_tool_name)) = exec_meta else {
			return Err(StoreError::NotFound(format!(
				"execution {execution_id} does not exist"
			)));
		};
		if exec_session != message.session_id {
			return Err(StoreError::Validation(format!(
				"execution {execution_id} belongs to session {exec_session}, not {}",
				message.session_id
			)));
		}
		if exec_branch != message.branch_id {
			return Err(StoreError::Validation(format!(
				"execution {execution_id} belongs to branch {exec_branch}, not {}",
				message.branch_id
			)));
		}

		let msg_call_id = message
			.payload
			.get("message")
			.and_then(|m| m.get("toolCallId"))
			.or_else(|| message.payload.get("toolCallId"))
			.or_else(|| message.payload.get("tool_call_id"))
			.and_then(Value::as_str);
		if let Some(call_id) = msg_call_id {
			if call_id != exec_call_id {
				return Err(StoreError::Validation(format!(
					"message tool call id {call_id} does not match execution provider call id {exec_call_id}"
				)));
			}
		}

		let msg_tool_name = message
			.payload
			.get("message")
			.and_then(|m| m.get("toolName"))
			.or_else(|| message.payload.get("toolName"))
			.or_else(|| message.payload.get("tool_name"))
			.and_then(Value::as_str);
		if let Some(tool_name) = msg_tool_name {
			if tool_name != exec_tool_name {
				return Err(StoreError::Validation(format!(
					"message tool name {tool_name} does not match execution tool name {exec_tool_name}"
				)));
			}
		}

		let msg_exec_id = message
			.payload
			.get("message")
			.and_then(|m| m.get("details"))
			.or_else(|| message.payload.get("details"))
			.and_then(|d| d.get("executionId"))
			.and_then(Value::as_str);
		if let Some(eid) = msg_exec_id {
			if eid != execution_id {
				return Err(StoreError::Validation(format!(
					"message execution id {eid} does not match execution id {execution_id}"
				)));
			}
		}

		let newly_recorded = record_outcome_tx(&transaction, execution_id, &outcome, now)?;
		if newly_recorded {
			append_message_tx(&transaction, &message, now)?;
			update_projection_kept_tx(&transaction, &message.session_id, &message.id, now)?;
			transaction.commit()?;
			Ok(true)
		} else {
			let existing_msg_id: Option<String> = transaction
				.query_row(
					"SELECT id FROM messages WHERE branch_id = ?1 AND ( \
						 json_extract(payload_json, '$.message.details.executionId') = ?2 OR \
						 json_extract(payload_json, '$.details.executionId') = ?2 \
					 ) LIMIT 1",
					params![message.branch_id, execution_id],
					|row| row.get(0),
				)
				.optional()?;
			if existing_msg_id.is_some() {
				transaction.commit()?;
				Ok(false)
			} else {
				append_message_tx(&transaction, &message, now)?;
				update_projection_kept_tx(&transaction, &message.session_id, &message.id, now)?;
				transaction.commit()?;
				Ok(true)
			}
		}
	}

	/// Read an execution record by id, returning `None` when the execution does not exist.
	pub fn execution(&self, execution_id: &str) -> StoreResult<Option<ExecutionRecord>> {
		let row = self
			.connection
			.query_row(
				"SELECT seq, execution_id, session_id, branch_id, provider_call_id, tool_name, \
				 intent_json, created_at_ms, outcome_json, outcome_at_ms \
				 FROM executions WHERE execution_id = ?1",
				params![execution_id],
				|row| {
					Ok((
						row.get::<_, i64>(0)?,
						row.get::<_, String>(1)?,
						row.get::<_, String>(2)?,
						row.get::<_, String>(3)?,
						row.get::<_, String>(4)?,
						row.get::<_, String>(5)?,
						row.get::<_, String>(6)?,
						row.get::<_, i64>(7)?,
						row.get::<_, Option<String>>(8)?,
						row.get::<_, Option<i64>>(9)?,
					))
				},
			)
			.optional()?;
		let Some((
			seq,
			execution_id,
			session_id,
			branch_id,
			provider_call_id,
			tool_name,
			intent_json,
			created_at_ms,
			outcome_json,
			outcome_at_ms,
		)) = row
		else {
			return Ok(None);
		};
		let intent = decode_json(&intent_json)?;
		let outcome = match outcome_json {
			Some(json) => Some(decode_json(&json)?),
			None => None,
		};
		Ok(Some(ExecutionRecord {
			seq,
			execution_id,
			session_id,
			branch_id,
			provider_call_id,
			tool_name,
			intent,
			created_at_ms,
			outcome,
			outcome_at_ms,
		}))
	}

	/// Read a terminal outcome, or `None` when the execution is unresolved or
	/// unknown.
	pub fn outcome(&self, execution_id: &str) -> StoreResult<Option<Value>> {
		self.execution(execution_id).map(|rec| rec.and_then(|r| r.outcome))
	}

	/// Executions that have a terminal outcome recorded in SQLite, but no corresponding
	/// tool-result message in `messages`.
	pub fn executions_needing_recovery(
		&self,
		session_id: &str,
	) -> StoreResult<Vec<ExecutionRecord>> {
		let mut stmt = self.connection.prepare(
			"SELECT seq, execution_id, session_id, branch_id, provider_call_id, tool_name, \
			 intent_json, created_at_ms, outcome_json, outcome_at_ms \
			 FROM executions \
			 WHERE session_id = ?1 AND outcome_json IS NOT NULL \
			 ORDER BY seq",
		)?;
		let exec_rows = stmt.query_map(params![session_id], |row| {
			Ok((
				row.get::<_, i64>(0)?,
				row.get::<_, String>(1)?,
				row.get::<_, String>(2)?,
				row.get::<_, String>(3)?,
				row.get::<_, String>(4)?,
				row.get::<_, String>(5)?,
				row.get::<_, String>(6)?,
				row.get::<_, i64>(7)?,
				row.get::<_, Option<String>>(8)?,
				row.get::<_, Option<i64>>(9)?,
			))
		})?;
		let mut executions = Vec::new();
		for row in exec_rows {
			let (
				seq,
				execution_id,
				session_id,
				branch_id,
				provider_call_id,
				tool_name,
				intent_json,
				created_at_ms,
				outcome_json,
				outcome_at_ms,
			) = row?;
			let intent = decode_json(&intent_json)?;
			let outcome = match outcome_json {
				Some(json) => Some(decode_json(&json)?),
				None => None,
			};
			executions.push(ExecutionRecord {
				seq,
				execution_id,
				session_id,
				branch_id,
				provider_call_id,
				tool_name,
				intent,
				created_at_ms,
				outcome,
				outcome_at_ms,
			});
		}

		if executions.is_empty() {
			return Ok(Vec::new());
		}

		let messages = self.session_messages(session_id)?;
		let mut messaged_execution_ids: HashSet<String> = HashSet::new();
		let mut legacy_messages_by_branch_call: HashMap<(String, String), usize> = HashMap::new();

		for msg in messages {
			let val = &msg.payload;
			let exec_id = val
				.get("message")
				.and_then(|m| m.get("details"))
				.or_else(|| val.get("details"))
				.and_then(|d| d.get("executionId"))
				.and_then(Value::as_str);
			if let Some(id) = exec_id {
				messaged_execution_ids.insert(id.to_string());
			} else {
				let role = val
					.get("message")
					.and_then(|m| m.get("role"))
					.or_else(|| val.get("role"))
					.and_then(Value::as_str);
				if role == Some("toolResult") {
					let call_id = val
						.get("message")
						.and_then(|m| m.get("toolCallId"))
						.or_else(|| val.get("toolCallId"))
						.or_else(|| val.get("tool_call_id"))
						.and_then(Value::as_str);
					if let Some(call_id) = call_id {
						*legacy_messages_by_branch_call
							.entry((msg.branch_id.clone(), call_id.to_string()))
							.or_insert(0) += 1;
					}
				}
			}
		}

		let mut needing_recovery = Vec::new();
		for exec in executions {
			if messaged_execution_ids.contains(&exec.execution_id) {
				continue;
			}
			let key = (exec.branch_id.clone(), exec.provider_call_id.clone());
			if let Some(count) = legacy_messages_by_branch_call.get_mut(&key) {
				if *count > 0 {
					*count -= 1;
					continue;
 				}
 			}
			needing_recovery.push(exec);
		}

		Ok(needing_recovery)
	}

	/// Recover executions that have an outcome recorded in SQLite but no message on the branch.
	/// Appends recovered toolResult messages only when safely resolvable (active branch leaf matches
	/// the assistant turn that made the call). Returns number of recovered messages.
	pub fn recover_unmessaged_outcomes(&mut self, session_id: &str) -> StoreResult<usize> {
		let needing = self.executions_needing_recovery(session_id)?;
		if needing.is_empty() {
			return Ok(0);
		}
		let Some(session) = self.session(session_id)? else {
			return Ok(0);
		};
		let Some(active_branch_id) = session.active_branch_id else {
			return Ok(0);
		};

		let mut recovered_count = 0;
		for exec in needing {
			if exec.branch_id != active_branch_id {
				continue;
			}
			let Some(branch) = self.branch(&active_branch_id)? else {
				continue;
			};
			let Some(leaf_id) = branch.leaf_message_id else {
				continue;
			};
			let Some(leaf_msg) = self.message(&leaf_id)? else {
				continue;
			};

			let is_matching_assistant = is_assistant_with_call(&leaf_msg.payload, &exec.provider_call_id);
			let is_matching_sibling_tool =
				is_sibling_tool_result(self, &leaf_msg, &exec.provider_call_id)?;

			if !is_matching_assistant && !is_matching_sibling_tool {
				continue;
			}

			let Some(outcome) = &exec.outcome else {
				continue;
			};
			let content = outcome
				.get("content")
				.and_then(Value::as_str)
				.unwrap_or("")
				.to_string();
			let is_error = outcome
				.get("isError")
				.and_then(Value::as_bool)
				.unwrap_or(false);
			let mut details = outcome.get("details").cloned().unwrap_or_else(|| json!({}));
			if let Some(map) = details.as_object_mut() {
				map.insert("executionId".to_string(), json!(exec.execution_id));
				map.insert("recovered".to_string(), json!(true));
			} else {
				let mut map = serde_json::Map::new();
				map.insert("value".to_string(), details);
				map.insert("executionId".to_string(), json!(exec.execution_id));
				map.insert("recovered".to_string(), json!(true));
				details = Value::Object(map);
			}

			let now_time = SystemTime::now();
			let iso_ts = crate::output::iso8601(now_time);
			let now = now_ms();
			let msg_id = format!("msg-rec-{}", exec.execution_id);
			let sess_msg = json!({
				"role": "toolResult",
				"toolCallId": exec.provider_call_id,
				"toolName": exec.tool_name,
				"content": content,
				"isError": is_error,
				"timestamp": now,
				"details": details,
			});
			let entry = json!({
				"type": "message",
				"id": msg_id,
				"parentId": leaf_id,
				"timestamp": iso_ts,
				"message": sess_msg,
			});

			let new_msg = NewMessage {
				id: msg_id.clone(),
				session_id: exec.session_id.clone(),
				branch_id: active_branch_id.clone(),
				parent_message_id: Some(leaf_id),
				payload: entry,
			};

			let transaction = self
				.connection
				.transaction_with_behavior(TransactionBehavior::Immediate)?;

			append_message_tx(&transaction, &new_msg, now)?;
			update_projection_kept_tx(&transaction, &exec.session_id, &msg_id, now)?;
			transaction.commit()?;
			recovered_count += 1;
		}

		Ok(recovered_count)
	}

	/// Reconcile a session's `durableProjection.kept` with the active branch suffix.
	///
	/// Reads, computes, and updates within a single IMMEDIATE transaction so
	/// concurrent updates cannot resurrect a stale or cleared projection.
	/// Validates the entire branch suffix lineage, but only backfills conversation
	/// messages (filtering out metadata rows like branch_summary or custom).
	/// If the projection contains messages not on the active branch, fails closed
	/// with `StoreError::Corrupt` without altering branch history.
	pub fn reconcile_durable_projection(&mut self, session_id: &str) -> StoreResult<usize> {
		validate_id("session id", session_id)?;
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		let count = reconcile_durable_projection_tx(&transaction, session_id, now)?;
		transaction.commit()?;
		Ok(count)
	}

	/// Intents with no terminal outcome. Presence here is not proof the effect
	/// started; callers must follow the tool-specific recovery contract.
	pub fn unresolved_intents(&self, session_id: &str) -> StoreResult<Vec<UnresolvedIntent>> {
		let mut statement = self.connection.prepare(
			"SELECT execution_id, session_id, branch_id, provider_call_id, tool_name, intent_json, created_at_ms \
			 FROM executions WHERE session_id = ?1 AND outcome_json IS NULL ORDER BY seq",
		)?;
		let rows = statement.query_map(params![session_id], |row| {
			Ok((
				row.get::<_, String>(0)?,
				row.get::<_, String>(1)?,
				row.get::<_, String>(2)?,
				row.get::<_, String>(3)?,
				row.get::<_, String>(4)?,
				row.get::<_, String>(5)?,
				row.get::<_, i64>(6)?,
			))
		})?;
		let mut intents = Vec::new();
		for row in rows {
			let (execution_id, session_id, branch_id, provider_call_id, tool_name, json, created_at_ms) =
				row?;
			intents.push(UnresolvedIntent {
				execution_id,
				session_id,
				branch_id,
				provider_call_id,
				tool_name,
				payload: decode_json(&json)?,
				created_at_ms,
			});
		}
		Ok(intents)
	}

	/// Atomically record a worker completion and its pending parent delivery.
	/// The delivery keeps `origin_branch_id` even if the session's active branch
	/// changed before or after this call.
	///
	/// Repeating the same `worker_id`/`delivery_id` with the same payloads is
	/// idempotent. Any conflicting reuse is rejected without inserting either row.
	pub fn complete_worker(&mut self, completion: WorkerCompletion) -> StoreResult<()> {
		validate_id("worker id", &completion.worker_id)?;
		validate_id("delivery id", &completion.delivery_id)?;
		validate_id("session id", &completion.session_id)?;
		validate_id("origin branch id", &completion.origin_branch_id)?;
		let completion_json = encode_json(&completion.completion)?;
		let delivery_json = encode_json(&completion.delivery)?;
		let now = now_ms();
		let transaction = self.connection.transaction()?;

		let origin_session: Option<String> = transaction
			.query_row(
				"SELECT session_id FROM branches WHERE id = ?1",
				params![completion.origin_branch_id],
				|row| row.get(0),
			)
			.optional()?;
		match origin_session {
			None => {
				return Err(StoreError::NotFound(format!(
					"origin branch {} does not exist",
					completion.origin_branch_id
				)));
			},
			Some(origin_session) if origin_session != completion.session_id => {
				return Err(StoreError::Validation(
					"origin branch belongs to a different session".to_string(),
				));
			},
			Some(_) => {},
		}

		let existing_completion: Option<(String, String, String, String)> = transaction
			.query_row(
				"SELECT session_id, origin_branch_id, delivery_id, completion_json \
				 FROM worker_completions WHERE worker_id = ?1",
				params![completion.worker_id],
				|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
			)
			.optional()?;
		if let Some((
			session_id,
			origin_branch_id,
			delivery_id,
			stored_completion,
		)) = existing_completion
		{
			if session_id != completion.session_id
				|| origin_branch_id != completion.origin_branch_id
				|| delivery_id != completion.delivery_id
				|| decode_json(&stored_completion)? != completion.completion
			{
				return Err(StoreError::Conflict(format!(
					"worker {} already completed with different data",
					completion.worker_id
				)));
			}
			let stored_delivery: Option<(String, String, String, String)> = transaction
				.query_row(
					"SELECT worker_id, session_id, origin_branch_id, payload_json \
					 FROM deliveries WHERE delivery_id = ?1",
					params![completion.delivery_id],
					|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
				)
				.optional()?;
			let Some((
				delivery_worker_id,
				delivery_session_id,
				delivery_origin_branch_id,
				stored_delivery_json,
			)) = stored_delivery
			else {
				return Err(StoreError::Corrupt(format!(
					"worker {} completion references missing delivery {}",
					completion.worker_id, completion.delivery_id
				)));
			};
			if delivery_worker_id != completion.worker_id
				|| delivery_session_id != completion.session_id
				|| delivery_origin_branch_id != completion.origin_branch_id
				|| decode_json(&stored_delivery_json)? != completion.delivery
			{
				return Err(StoreError::Conflict(format!(
					"delivery {} already exists with different data",
					completion.delivery_id
				)));
			}
			transaction.commit()?;
			return Ok(());
		}

		let existing_delivery: Option<(String, String, String, String)> = transaction
			.query_row(
				"SELECT worker_id, session_id, origin_branch_id, payload_json \
				 FROM deliveries WHERE delivery_id = ?1",
				params![completion.delivery_id],
				|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
			)
			.optional()?;
		if let Some((worker_id, session_id, origin_branch_id, stored_delivery)) = existing_delivery {
			if worker_id != completion.worker_id
				|| session_id != completion.session_id
				|| origin_branch_id != completion.origin_branch_id
				|| decode_json(&stored_delivery)? != completion.delivery
			{
				return Err(StoreError::Conflict(format!(
					"delivery {} already exists with different data",
					completion.delivery_id
				)));
			}
			return Err(StoreError::Corrupt(format!(
				"delivery {} exists without its worker completion",
				completion.delivery_id
			)));
		}

		// Delivery first so the completion's foreign key is satisfied inside the
		// same transaction. Both rows commit or neither does.
		transaction.execute(
			"INSERT INTO deliveries \
			 (delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms) \
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL)",
			params![
				completion.delivery_id,
				completion.worker_id,
				completion.session_id,
				completion.origin_branch_id,
				delivery_json,
				now
			],
		)?;
		transaction.execute(
			"INSERT INTO worker_completions \
			 (worker_id, session_id, origin_branch_id, delivery_id, completion_json, completed_at_ms) \
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
			params![
				completion.worker_id,
				completion.session_id,
				completion.origin_branch_id,
				completion.delivery_id,
				completion_json,
				now
			],
		)?;
		transaction.commit()?;
		Ok(())
	}

	/// Insert a pending delivery that has no worker completion (peer mail).
	/// Repeating the same id/payload is idempotent; conflicting reuse is
	/// rejected. Enqueue never marks the delivery delivered or processed.
	pub fn enqueue_delivery(&mut self, delivery: NewDelivery) -> StoreResult<()> {
		validate_id("delivery id", &delivery.delivery_id)?;
		validate_id("worker id", &delivery.worker_id)?;
		validate_id("delivery session id", &delivery.session_id)?;
		validate_id("delivery origin branch id", &delivery.origin_branch_id)?;
		let payload_json = encode_json(&delivery.payload)?;
		let transaction = self.connection.transaction()?;
		let origin_session: Option<String> = transaction
			.query_row(
				"SELECT session_id FROM branches WHERE id = ?1",
				params![delivery.origin_branch_id],
				|row| row.get(0),
			)
			.optional()?;
		match origin_session {
			None => {
				return Err(StoreError::NotFound(format!(
					"delivery branch {} does not exist",
					delivery.origin_branch_id
				)));
			},
			Some(origin_session) if origin_session != delivery.session_id => {
				return Err(StoreError::Validation(
					"delivery branch belongs to a different session".to_string(),
				));
			},
			Some(_) => {},
		}
		let existing: Option<(String, String, String, String)> = transaction
			.query_row(
				"SELECT worker_id, session_id, origin_branch_id, payload_json \
				 FROM deliveries WHERE delivery_id = ?1",
				params![delivery.delivery_id],
				|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
			)
			.optional()?;
		if let Some((worker_id, session_id, origin_branch_id, stored)) = existing {
			if worker_id != delivery.worker_id
				|| session_id != delivery.session_id
				|| origin_branch_id != delivery.origin_branch_id
				|| decode_json(&stored)? != delivery.payload
			{
				return Err(StoreError::Conflict(format!(
					"delivery {} already exists with different data",
					delivery.delivery_id
				)));
			}
			transaction.commit()?;
			return Ok(());
		}
		let now = now_ms();
		transaction.execute(
			"INSERT INTO deliveries \
			 (delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms) \
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL)",
			params![
				delivery.delivery_id,
				delivery.worker_id,
				delivery.session_id,
				delivery.origin_branch_id,
				payload_json,
				now
			],
		)?;
		transaction.commit()?;
		Ok(())
	}

	/// Append a delivery message to the target branch and mark the delivery
	/// delivered in one transaction. The message must belong to the delivery's
	/// session; its branch is the target context's current branch. A delivery can
	/// be consumed once; processing stays a separate explicit checkpoint.
	pub fn consume_delivery(
		&mut self,
		delivery_id: &str,
		message: NewMessage,
		at_ms: i64,
	) -> StoreResult<()> {
		validate_id("delivery id", delivery_id)?;
		let transaction = self.connection.transaction()?;
		let delivery: Option<(String, Option<i64>)> = transaction
			.query_row(
				"SELECT session_id, delivered_at_ms FROM deliveries WHERE delivery_id = ?1",
				params![delivery_id],
				|row| Ok((row.get(0)?, row.get(1)?)),
			)
			.optional()?;
		let Some((session_id, delivered_at)) = delivery else {
			return Err(StoreError::NotFound(format!(
				"delivery {delivery_id} does not exist"
			)));
		};
		if session_id != message.session_id {
			return Err(StoreError::Validation(
				"delivery message belongs to a different session".to_string(),
			));
		}
		if delivered_at.is_some() {
			return Err(StoreError::Conflict(format!(
				"delivery {delivery_id} was already appended to the target context"
			)));
		}
		let changes = transaction.execute(
			"UPDATE deliveries SET delivered_at_ms = ?1 WHERE delivery_id = ?2 AND delivered_at_ms IS NULL",
			params![at_ms, delivery_id],
		)?;
		if changes == 0 {
			return Err(StoreError::Conflict(format!(
				"delivery {delivery_id} was already appended to the target context"
			)));
		}
		append_message_tx(&transaction, &message, at_ms)?;
		transaction.commit()?;
		Ok(())
	}

	/// Deliveries not yet durably offered to the target context.
	pub fn pending_deliveries(&self, session_id: &str) -> StoreResult<Vec<DeliveryRecord>> {
		self.query_deliveries(
			"SELECT delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms \
			 FROM deliveries WHERE session_id = ?1 AND delivered_at_ms IS NULL ORDER BY created_at_ms, delivery_id",
			session_id,
		)
	}

	/// Deliveries already offered to the target context but not confirmed
	/// processed. These are crash-recovery candidates, not automatic retries.
	pub fn unprocessed_deliveries(&self, session_id: &str) -> StoreResult<Vec<DeliveryRecord>> {
		self.query_deliveries(
			"SELECT delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms \
			 FROM deliveries WHERE session_id = ?1 AND delivered_at_ms IS NOT NULL AND processed_at_ms IS NULL \
			 ORDER BY delivered_at_ms, delivery_id",
			session_id,
		)
	}

	fn query_deliveries(&self, sql: &str, session_id: &str) -> StoreResult<Vec<DeliveryRecord>> {
		let mut statement = self.connection.prepare(sql)?;
		let rows = statement.query_map(params![session_id], |row| {
			Ok((
				row.get::<_, String>(0)?,
				row.get::<_, String>(1)?,
				row.get::<_, String>(2)?,
				row.get::<_, String>(3)?,
				row.get::<_, String>(4)?,
				row.get::<_, i64>(5)?,
				row.get::<_, Option<i64>>(6)?,
				row.get::<_, Option<i64>>(7)?,
			))
		})?;
		let mut deliveries = Vec::new();
		for row in rows {
			let (
				delivery_id,
				worker_id,
				session_id,
				origin_branch_id,
				json,
				created_at_ms,
				delivered_at_ms,
				processed_at_ms,
			) = row?;
			deliveries.push(DeliveryRecord {
				delivery_id,
				worker_id,
				session_id,
				origin_branch_id,
				payload: decode_json(&json)?,
				created_at_ms,
				delivered_at_ms,
				processed_at_ms,
			});
		}
		Ok(deliveries)
	}

	/// Mark a delivery as offered to the target context. Repeating the same mark
	/// is idempotent; processed state is not implied.
	pub fn mark_delivery_delivered(&mut self, delivery_id: &str, at_ms: i64) -> StoreResult<()> {
		validate_id("delivery id", delivery_id)?;
		let transaction = self.connection.transaction()?;
		let state: Option<(Option<i64>, Option<i64>)> = transaction
			.query_row(
				"SELECT delivered_at_ms, processed_at_ms FROM deliveries WHERE delivery_id = ?1",
				params![delivery_id],
				|row| Ok((row.get(0)?, row.get(1)?)),
			)
			.optional()?;
		let Some((delivered_at, processed_at)) = state else {
			return Err(StoreError::NotFound(format!(
				"delivery {delivery_id} does not exist"
			)));
		};
		if processed_at.is_some() && delivered_at.is_none() {
			return Err(StoreError::Corrupt(format!(
				"delivery {delivery_id} is marked processed without being delivered"
			)));
		}
		if delivered_at.is_none() {
			transaction.execute(
				"UPDATE deliveries SET delivered_at_ms = ?1 WHERE delivery_id = ?2 AND delivered_at_ms IS NULL",
				params![at_ms, delivery_id],
			)?;
		}
		transaction.commit()?;
		Ok(())
	}

	/// Mark a delivery as processed by the target. Delivery must already be
	/// recorded. Repeating the same mark is idempotent.
	pub fn mark_delivery_processed(&mut self, delivery_id: &str, at_ms: i64) -> StoreResult<()> {
		validate_id("delivery id", delivery_id)?;
		let transaction = self.connection.transaction()?;
		let state: Option<(Option<i64>, Option<i64>)> = transaction
			.query_row(
				"SELECT delivered_at_ms, processed_at_ms FROM deliveries WHERE delivery_id = ?1",
				params![delivery_id],
				|row| Ok((row.get(0)?, row.get(1)?)),
			)
			.optional()?;
		let Some((delivered_at, processed_at)) = state else {
			return Err(StoreError::NotFound(format!(
				"delivery {delivery_id} does not exist"
			)));
		};
		if processed_at.is_none() {
			if delivered_at.is_none() {
				return Err(StoreError::Validation(format!(
					"delivery {delivery_id} cannot be processed before it is delivered"
				)));
			}
			transaction.execute(
				"UPDATE deliveries SET processed_at_ms = ?1 WHERE delivery_id = ?2 AND processed_at_ms IS NULL",
				params![at_ms, delivery_id],
			)?;
		} else if delivered_at.is_none() {
			return Err(StoreError::Corrupt(format!(
				"delivery {delivery_id} is marked processed without being delivered"
			)));
		}
		transaction.commit()?;
		Ok(())
	}

	/// Record metadata for an artifact whose bytes were published separately.
	/// This never touches the artifact location, so a missing file is not
	/// silently recreated and a DB row never claims the file is readable.
	pub fn record_artifact(&mut self, artifact: NewArtifact) -> StoreResult<()> {
		validate_id("artifact id", &artifact.artifact_id)?;
		validate_id("artifact session id", &artifact.session_id)?;
		validate_id("artifact location", &artifact.location)?;
		let metadata_json = encode_json(&artifact.metadata)?;
		let now = now_ms();
		let transaction = self.connection.transaction()?;
		require_session(&transaction, &artifact.session_id)?;
		if let Some(branch_id) = &artifact.branch_id {
			let branch_session: Option<String> = transaction
				.query_row(
					"SELECT session_id FROM branches WHERE id = ?1",
					params![branch_id],
					|row| row.get(0),
				)
				.optional()?;
			match branch_session {
				None => {
					return Err(StoreError::NotFound(format!(
						"artifact branch {branch_id} does not exist"
					)));
				},
				Some(branch_session) if branch_session != artifact.session_id => {
					return Err(StoreError::Validation(
						"artifact branch belongs to a different session".to_string(),
					));
				},
				Some(_) => {},
			}
		}
		if let Some(execution_id) = &artifact.execution_id {
			let execution: Option<(String, String)> = transaction
				.query_row(
					"SELECT session_id, branch_id FROM executions WHERE execution_id = ?1",
					params![execution_id],
					|row| Ok((row.get(0)?, row.get(1)?)),
				)
				.optional()?;
			match execution {
				None => {
					return Err(StoreError::NotFound(format!(
						"artifact execution {execution_id} does not exist"
					)));
				},
				Some((execution_session, execution_branch)) => {
					if execution_session != artifact.session_id {
						return Err(StoreError::Validation(
							"artifact execution belongs to a different session".to_string(),
						));
					}
					if let Some(branch_id) = &artifact.branch_id
						&& &execution_branch != branch_id
					{
						return Err(StoreError::Validation(
							"artifact branch does not match the execution branch".to_string(),
						));
					}
				},
			}
		}
		let existing: Option<(String, Option<String>, Option<String>, String, String)> = transaction
			.query_row(
				"SELECT session_id, branch_id, execution_id, location, metadata_json \
				 FROM artifacts WHERE artifact_id = ?1",
				params![artifact.artifact_id],
				|row| {
					Ok((
						row.get(0)?,
						row.get(1)?,
						row.get(2)?,
						row.get(3)?,
						row.get(4)?,
					))
				},
			)
			.optional()?;
		if let Some((session_id, branch_id, execution_id, location, stored_metadata)) = existing {
			if session_id != artifact.session_id
				|| branch_id != artifact.branch_id
				|| execution_id != artifact.execution_id
				|| location != artifact.location
				|| decode_json(&stored_metadata)? != artifact.metadata
			{
				return Err(StoreError::Conflict(format!(
					"artifact {} already exists with different metadata",
					artifact.artifact_id
				)));
			}
			transaction.commit()?;
			return Ok(());
		}
		transaction.execute(
			"INSERT INTO artifacts \
			 (artifact_id, session_id, branch_id, execution_id, location, metadata_json, created_at_ms) \
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
			params![
				artifact.artifact_id,
				artifact.session_id,
				artifact.branch_id,
				artifact.execution_id,
				artifact.location,
				metadata_json,
				now
			],
		)?;
		transaction.commit()?;
		Ok(())
	}

	/// Artifact metadata for one session, oldest first.
	pub fn artifacts(&self, session_id: &str) -> StoreResult<Vec<ArtifactRecord>> {
		let mut statement = self.connection.prepare(
			"SELECT artifact_id, session_id, branch_id, execution_id, location, metadata_json, created_at_ms \
			 FROM artifacts WHERE session_id = ?1 ORDER BY created_at_ms, artifact_id",
		)?;
		let rows = statement.query_map(params![session_id], |row| {
			Ok((
				row.get::<_, String>(0)?,
				row.get::<_, String>(1)?,
				row.get::<_, Option<String>>(2)?,
				row.get::<_, Option<String>>(3)?,
				row.get::<_, String>(4)?,
				row.get::<_, String>(5)?,
				row.get::<_, i64>(6)?,
			))
		})?;
		let mut artifacts = Vec::new();
		for row in rows {
			let (artifact_id, session_id, branch_id, execution_id, location, json, created_at_ms) =
				row?;
			artifacts.push(ArtifactRecord {
				artifact_id,
				session_id,
				branch_id,
				execution_id,
				location,
				metadata: decode_json(&json)?,
				created_at_ms,
			});
		}
		Ok(artifacts)
	}

	/// All sessions in this database, newest first. Read-only: used by session
	/// listing and `--continue`; no second index file exists.
	pub fn list_sessions(&self) -> StoreResult<Vec<SessionRecord>> {
		let mut statement = self.connection.prepare(
			"SELECT id, parent_session_id, active_branch_id, payload_json, created_at_ms, updated_at_ms \
			 FROM sessions ORDER BY updated_at_ms DESC, created_at_ms DESC, id",
		)?;
		let rows = statement.query_map([], |row| {
			Ok((
				row.get::<_, String>(0)?,
				row.get::<_, Option<String>>(1)?,
				row.get::<_, Option<String>>(2)?,
				row.get::<_, String>(3)?,
				row.get::<_, i64>(4)?,
				row.get::<_, i64>(5)?,
			))
		})?;
		let mut sessions = Vec::new();
		for row in rows {
			let (id, parent_session_id, active_branch_id, json, created_at_ms, updated_at_ms) =
				row?;
			sessions.push(SessionRecord {
				id,
				parent_session_id,
				active_branch_id,
				payload: decode_json(&json)?,
				created_at_ms,
				updated_at_ms,
			});
		}
		Ok(sessions)
	}

	/// Every branch of one session (including abandoned branches), oldest
	/// first. Used to enumerate branch context after reopen without guessing
	/// from the active branch alone.
	pub fn session_branches(&self, session_id: &str) -> StoreResult<Vec<BranchRecord>> {
		let mut statement = self.connection.prepare(
			"SELECT id, session_id, parent_branch_id, parent_message_id, leaf_message_id, \
			 payload_json, created_at_ms FROM branches WHERE session_id = ?1 \
			 ORDER BY created_at_ms, id",
		)?;
		let rows = statement.query_map(params![session_id], |row| {
			Ok((
				row.get::<_, String>(0)?,
				row.get::<_, String>(1)?,
				row.get::<_, Option<String>>(2)?,
				row.get::<_, Option<String>>(3)?,
				row.get::<_, Option<String>>(4)?,
				row.get::<_, String>(5)?,
				row.get::<_, i64>(6)?,
			))
		})?;
		let mut branches = Vec::new();
		for row in rows {
			let (id, session_id, parent_branch_id, parent_message_id, leaf_message_id, json, created_at_ms) =
				row?;
			branches.push(BranchRecord {
				id,
				session_id,
				parent_branch_id,
				parent_message_id,
				leaf_message_id,
				payload: decode_json(&json)?,
				created_at_ms,
			});
		}
		Ok(branches)
	}

	/// Every message of one session in insertion order, all branches included.
	/// Branch views are derived by following parent links; this is the complete
	/// durable entry set for enumeration and export.
	pub fn session_messages(&self, session_id: &str) -> StoreResult<Vec<MessageRecord>> {
		let mut statement = self.connection.prepare(
			"SELECT id, session_id, branch_id, parent_message_id, payload_json, seq, created_at_ms \
			 FROM messages WHERE session_id = ?1 ORDER BY seq",
		)?;
		let rows = statement.query_map(params![session_id], |row| {
			Ok((
				row.get::<_, String>(0)?,
				row.get::<_, String>(1)?,
				row.get::<_, String>(2)?,
				row.get::<_, Option<String>>(3)?,
				row.get::<_, String>(4)?,
				row.get::<_, i64>(5)?,
				row.get::<_, i64>(6)?,
			))
		})?;
		let mut messages = Vec::new();
		for row in rows {
			let (id, session_id, branch_id, parent_message_id, json, seq, created_at_ms) = row?;
			messages.push(MessageRecord {
				id,
				session_id,
				branch_id,
				parent_message_id,
				payload: decode_json(&json)?,
				seq,
				created_at_ms,
			});
		}
		Ok(messages)
	}

	/// Replace a session payload (header/title/state) without touching its id,
	/// lineage, or active branch.
	pub fn update_session_payload(
		&mut self,
		session_id: &str,
		payload: Value,
	) -> StoreResult<()> {
		validate_id("session id", session_id)?;
		let encoded = encode_json(&payload)?;
		let changed = self.connection.execute(
			"UPDATE sessions SET payload_json = ?1, updated_at_ms = ?2 WHERE id = ?3",
			params![encoded, now_ms(), session_id],
		)?;
		if changed == 0 {
			return Err(StoreError::NotFound(format!(
				"session {session_id} does not exist"
			)));
		}
		Ok(())
	}
}

fn append_message_tx(
	transaction: &Transaction<'_>,
	message: &NewMessage,
	now: i64,
) -> StoreResult<()> {
	validate_id("message id", &message.id)?;
	validate_id("message session id", &message.session_id)?;
	validate_id("message branch id", &message.branch_id)?;
	let branch: Option<(String, Option<String>, Option<String>)> = transaction
		.query_row(
			"SELECT session_id, parent_message_id, leaf_message_id FROM branches WHERE id = ?1",
			params![message.branch_id],
			|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
		)
		.optional()?;
	let Some((branch_session, branch_parent, branch_leaf)) = branch else {
		return Err(StoreError::NotFound(format!(
			"branch {} does not exist",
			message.branch_id
		)));
	};
	if branch_session != message.session_id {
		return Err(StoreError::Validation(
			"message branch belongs to a different session".to_string(),
		));
	}
	let expected_parent = branch_leaf.or(branch_parent);
	if message.parent_message_id != expected_parent {
		return Err(StoreError::Validation(format!(
			"message {} must chain to {:?}, not {:?}",
			message.id, expected_parent, message.parent_message_id
		)));
	}
	if let Some(parent_id) = &message.parent_message_id {
		let parent_session: Option<String> = transaction
			.query_row(
				"SELECT session_id FROM messages WHERE id = ?1",
				params![parent_id],
				|row| row.get(0),
			)
			.optional()?;
		match parent_session {
			None => {
				return Err(StoreError::NotFound(format!(
					"parent message {parent_id} does not exist"
				)));
			},
			Some(parent_session) if parent_session != message.session_id => {
				return Err(StoreError::Validation(
					"parent message belongs to a different session".to_string(),
				));
			},
			Some(_) => {},
		}
	}
	transaction.execute(
		"INSERT INTO messages (id, session_id, branch_id, parent_message_id, payload_json, created_at_ms) \
		 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
		params![
			message.id,
			message.session_id,
			message.branch_id,
			message.parent_message_id,
			encode_json(&message.payload)?,
			now
		],
	)?;
	transaction.execute(
		"UPDATE branches SET leaf_message_id = ?1 WHERE id = ?2",
		params![message.id, message.branch_id],
	)?;
	transaction.execute(
		"UPDATE sessions SET updated_at_ms = ?1 WHERE id = ?2",
		params![now, message.session_id],
	)?;
	Ok(())
}

fn record_outcome_tx(
	transaction: &Transaction<'_>,
	execution_id: &str,
	outcome: &Value,
	now: i64,
) -> StoreResult<bool> {
	validate_id("execution id", execution_id)?;
	let encoded = encode_json(outcome)?;
	let existing: Option<Option<String>> = transaction
		.query_row(
			"SELECT outcome_json FROM executions WHERE execution_id = ?1",
			params![execution_id],
			|row| row.get(0),
		)
		.optional()?;
	let Some(existing) = existing else {
		return Err(StoreError::NotFound(format!(
			"execution {execution_id} does not exist"
		)));
	};
	if let Some(existing) = existing {
		let existing = decode_json(&existing)?;
		if existing == *outcome {
			return Ok(false);
		}
		return Err(StoreError::Conflict(format!(
			"execution {execution_id} already has a different terminal outcome"
		)));
	}
	let changed = transaction.execute(
		"UPDATE executions SET outcome_json = ?1, outcome_at_ms = ?2 \
		 WHERE execution_id = ?3 AND outcome_json IS NULL",
		params![encoded, now, execution_id],
	)?;
	if changed != 1 {
		return Err(StoreError::Corrupt(format!(
			"execution {execution_id} changed while recording its outcome"
		)));
	}
	Ok(true)
}

fn update_projection_kept_tx(
	transaction: &Transaction<'_>,
	session_id: &str,
	message_id: &str,
	now: i64,
) -> StoreResult<()> {
	let session_payload_json: Option<String> = transaction
		.query_row(
			"SELECT payload_json FROM sessions WHERE id = ?1",
			params![session_id],
			|row| row.get(0),
		)
		.optional()?;
	let Some(session_payload_json) = session_payload_json else {
		return Ok(());
	};
	let mut session_payload: Value = decode_json(&session_payload_json)?;
	let mut updated = false;
	if let Some(map) = session_payload.as_object_mut() {
		if let Some(proj_val) = map.get_mut("durableProjection") {
			if let Some(proj_obj) = proj_val.as_object_mut() {
				if let Some(kept_val) = proj_obj.get_mut("kept") {
					if let Some(kept_arr) = kept_val.as_array_mut() {
						if !kept_arr.iter().any(|v| v.as_str() == Some(message_id)) {
							kept_arr.push(Value::String(message_id.to_string()));
							updated = true;
						}
					}
				}
			}
		}
	}
	if updated {
		let updated_json = encode_json(&session_payload)?;
		transaction.execute(
			"UPDATE sessions SET payload_json = ?1, updated_at_ms = ?2 WHERE id = ?3",
			params![updated_json, now, session_id],
		)?;
	}
	Ok(())
}

fn branch_messages_tx(
	transaction: &Transaction<'_>,
	branch_id: &str,
) -> StoreResult<Vec<MessageRecord>> {
	let branch_row: Option<(String, Option<String>, Option<String>)> = transaction
		.query_row(
			"SELECT session_id, parent_message_id, leaf_message_id FROM branches WHERE id = ?1",
			params![branch_id],
			|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
		)
		.optional()?;
	let Some((session_id, parent_message_id, leaf_message_id)) = branch_row else {
		return Err(StoreError::NotFound(format!("branch {branch_id} does not exist")));
	};
	let mut cursor = leaf_message_id.or(parent_message_id);
	let mut seen = HashSet::new();
	let mut messages = Vec::new();
	while let Some(id) = cursor {
		if !seen.insert(id.clone()) {
			return Err(StoreError::Corrupt(format!(
				"branch {branch_id} contains a message cycle at {id}"
			)));
		}
		let msg_row: Option<(i64, String, String, String, Option<String>, String, i64)> = transaction
			.query_row(
				"SELECT seq, id, session_id, branch_id, parent_message_id, payload_json, created_at_ms \
				 FROM messages WHERE id = ?1",
				params![id],
				|row| {
					Ok((
						row.get(0)?,
						row.get(1)?,
						row.get(2)?,
						row.get(3)?,
						row.get(4)?,
						row.get(5)?,
						row.get(6)?,
					))
				},
			)
			.optional()?;
		let Some((seq, id, msg_session, msg_branch, parent, payload_json, created_at_ms)) = msg_row else {
			return Err(StoreError::Corrupt(format!(
				"branch {branch_id} references missing message {id}"
			)));
		};
		if msg_session != session_id {
			return Err(StoreError::Corrupt(format!(
				"branch {branch_id} chain crosses into session {msg_session} at {id}"
			)));
		}
		cursor = parent.clone();
		messages.push(MessageRecord {
			id,
			session_id: msg_session,
			branch_id: msg_branch,
			parent_message_id: parent,
			payload: decode_json(&payload_json)?,
			seq,
			created_at_ms,
		});
	}
	messages.reverse();
	Ok(messages)
}

fn extract_assistant_tool_call_counts(payload: &Value) -> HashMap<String, usize> {
	let mut counts = HashMap::new();
	let parts = payload
		.get("message")
		.and_then(|m| m.get("content"))
		.or_else(|| payload.get("content"))
		.or_else(|| payload.get("parts"));
	if let Some(parts) = parts.and_then(Value::as_array) {
		for part in parts {
			let is_tool_call = part
				.get("type")
				.or_else(|| part.get("kind"))
				.and_then(Value::as_str)
				== Some("toolCall");
			if is_tool_call {
				if let Some(id) = part
					.get("id")
					.or_else(|| part.get("toolCallId"))
					.or_else(|| part.get("tool_call_id"))
					.and_then(Value::as_str)
				{
					*counts.entry(id.to_string()).or_insert(0) += 1;
				}
			}
		}
	}
	let tool_calls = payload
		.get("message")
		.and_then(|m| m.get("tool_calls"))
		.or_else(|| payload.get("tool_calls"))
		.or_else(|| payload.get("toolCalls"));
	if let Some(tool_calls) = tool_calls.and_then(Value::as_array) {
		for call in tool_calls {
			if let Some(id) = call
				.get("id")
				.or_else(|| call.get("toolCallId"))
				.and_then(Value::as_str)
			{
				*counts.entry(id.to_string()).or_insert(0) += 1;
			}
		}
	}
	counts
}

fn decrement_call_count(counts: &mut HashMap<String, usize>, call_id: &str) -> bool {
	if let Some(count) = counts.get_mut(call_id) {
		if *count > 0 {
			*count -= 1;
			if *count == 0 {
				counts.remove(call_id);
			}
			return true;
		}
	}
	false
}

fn extract_tool_result_call_id(payload: &Value) -> Option<String> {
	payload
		.get("message")
		.and_then(|m| m.get("toolCallId"))
		.or_else(|| payload.get("toolCallId"))
		.or_else(|| payload.get("tool_call_id"))
		.or_else(|| payload.get("message").and_then(|m| m.get("tool_call_id")))
		.and_then(Value::as_str)
		.map(str::to_string)
}

fn validate_kept_messages_tx(
	transaction: &Transaction<'_>,
	session_id: &str,
	kept_ids: &[String],
	branch_messages: &[MessageRecord],
) -> StoreResult<bool> {
	let branch_msg_map: HashMap<&str, &MessageRecord> =
		branch_messages.iter().map(|m| (m.id.as_str(), m)).collect();

	let mut pending_tool_calls: HashMap<String, usize> = HashMap::new();

	for id in kept_ids {
		if let Some(msg) = branch_msg_map.get(id.as_str()) {
			let role = msg
				.payload
				.get("message")
				.and_then(|m| m.get("role"))
				.or_else(|| msg.payload.get("role"))
				.and_then(Value::as_str);

			match role {
				Some("assistant") => {
					pending_tool_calls = extract_assistant_tool_call_counts(&msg.payload);
				},
				Some("toolResult") => {
					if let Some(call_id) = extract_tool_result_call_id(&msg.payload) {
						decrement_call_count(&mut pending_tool_calls, &call_id);
					}
				},
				_ => {
					pending_tool_calls.clear();
				},
			}
		} else {
			let row: Option<String> = transaction
				.query_row(
					"SELECT payload_json FROM messages WHERE id = ?1 AND session_id = ?2",
					params![id, session_id],
					|r| r.get(0),
				)
				.optional()?;

			let Some(payload_json) = row else {
				return Ok(false);
			};

			let Ok(payload) = decode_json(&payload_json) else {
				return Ok(false);
			};

			let role = payload
				.get("message")
				.and_then(|m| m.get("role"))
				.or_else(|| payload.get("role"))
				.and_then(Value::as_str);

			if role != Some("toolResult") {
				return Ok(false);
			}

			let Some(call_id) = extract_tool_result_call_id(&payload) else {
				return Ok(false);
			};

			if !decrement_call_count(&mut pending_tool_calls, &call_id) {
				return Ok(false);
			}
		}
	}

	Ok(true)
}

fn reconcile_durable_projection_tx(
	transaction: &Transaction<'_>,
	session_id: &str,
	now: i64,
) -> StoreResult<usize> {
	let session_row: Option<(Option<String>, String)> = transaction
		.query_row(
			"SELECT active_branch_id, payload_json FROM sessions WHERE id = ?1",
			params![session_id],
			|row| Ok((row.get(0)?, row.get(1)?)),
		)
		.optional()?;
	let Some((active_branch_id, payload_json)) = session_row else {
		return Ok(0);
	};
	let Some(active_branch_id) = active_branch_id else {
		return Ok(0);
	};
	let mut session_payload: Value = decode_json(&payload_json)?;
	let Some(proj_val) = session_payload.get_mut("durableProjection") else {
		return Ok(0);
	};
	let Some(proj_obj) = proj_val.as_object_mut() else {
		return Ok(0);
	};
	let Some(kept_val) = proj_obj.get_mut("kept") else {
		return Ok(0);
	};
	let Some(kept_arr) = kept_val.as_array_mut() else {
		return Ok(0);
	};

	let kept_ids: Vec<String> = kept_arr
		.iter()
		.filter_map(Value::as_str)
		.map(str::to_string)
		.collect();

	if kept_ids.is_empty() {
		return Ok(0);
	}

	let branch_messages = branch_messages_tx(transaction, &active_branch_id)?;
	if branch_messages.is_empty() {
		return Ok(0);
	}

	let branch_id_set: HashSet<&str> = branch_messages.iter().map(|m| m.id.as_str()).collect();

	if !validate_kept_messages_tx(transaction, session_id, &kept_ids, &branch_messages)? {
		session_payload.as_object_mut().unwrap().remove("durableProjection");
		let updated_json = encode_json(&session_payload)?;
		transaction.execute(
			"UPDATE sessions SET payload_json = ?1, updated_at_ms = ?2 WHERE id = ?3",
			params![updated_json, now, session_id],
		)?;
		return Ok(0);
	}
	let mut last_kept_idx: Option<usize> = None;
	let kept_set: HashSet<&str> = kept_ids.iter().map(String::as_str).collect();

	for (idx, msg) in branch_messages.iter().enumerate() {
		if kept_set.contains(msg.id.as_str()) {
			last_kept_idx = Some(idx);
		}
	}

	let Some(last_kept_idx) = last_kept_idx else {
		session_payload.as_object_mut().unwrap().remove("durableProjection");
		let updated_json = encode_json(&session_payload)?;
		transaction.execute(
			"UPDATE sessions SET payload_json = ?1, updated_at_ms = ?2 WHERE id = ?3",
			params![updated_json, now, session_id],
		)?;
		return Ok(0);
	};

	let suffix = &branch_messages[last_kept_idx + 1..];
	if suffix.is_empty() {
		return Ok(0);
	}

	// Validate the suffix: each message must be a valid confirmed branch message chaining from the previous
	let mut backfill_ids = Vec::new();
	let mut expected_parent = Some(branch_messages[last_kept_idx].id.clone());

	for msg in suffix {
		if msg.parent_message_id != expected_parent {
			return Err(StoreError::Corrupt(format!(
				"broken lineage in branch suffix at message {}: expected parent {:?}, found {:?}",
				msg.id, expected_parent, msg.parent_message_id
			)));
		}
		expected_parent = Some(msg.id.clone());

		// Defect 1: only conversation messages (not metadata rows like branch_summary, custom, etc.) go into kept!
		let is_message = msg.payload.get("type").and_then(Value::as_str) == Some("message");
		if is_message {
			backfill_ids.push(msg.id.clone());
		}
	}

	if backfill_ids.is_empty() {
		return Ok(0);
	}

	for id in &backfill_ids {
		if !kept_arr.iter().any(|v| v.as_str() == Some(id.as_str())) {
			kept_arr.push(Value::String(id.clone()));
		}
	}

	let updated_json = encode_json(&session_payload)?;
	transaction.execute(
		"UPDATE sessions SET payload_json = ?1, updated_at_ms = ?2 WHERE id = ?3",
		params![updated_json, now, session_id],
	)?;

	Ok(backfill_ids.len())
}

fn is_assistant_with_call(payload: &Value, provider_call_id: &str) -> bool {
	let role = payload
		.get("message")
		.and_then(|m| m.get("role"))
		.or_else(|| payload.get("role"))
		.and_then(Value::as_str);
	if role != Some("assistant") {
		return false;
	}
	let parts = payload
		.get("message")
		.and_then(|m| m.get("content"))
		.or_else(|| payload.get("content"))
		.or_else(|| payload.get("parts"));
	if let Some(parts) = parts.and_then(Value::as_array) {
		for part in parts {
			let is_tool_call = part
				.get("type")
				.or_else(|| part.get("kind"))
				.and_then(Value::as_str)
				== Some("toolCall");
			if is_tool_call {
				let id = part
					.get("id")
					.or_else(|| part.get("toolCallId"))
					.or_else(|| part.get("tool_call_id"))
					.and_then(Value::as_str);
				if id == Some(provider_call_id) {
					return true;
				}
			}
		}
	}
	let tool_calls = payload
		.get("message")
		.and_then(|m| m.get("tool_calls"))
		.or_else(|| payload.get("tool_calls"))
		.or_else(|| payload.get("toolCalls"));
	if let Some(tool_calls) = tool_calls.and_then(Value::as_array) {
		for call in tool_calls {
			let id = call
				.get("id")
				.or_else(|| call.get("toolCallId"))
				.and_then(Value::as_str);
			if id == Some(provider_call_id) {
				return true;
			}
		}
	}
	false
}

fn is_sibling_tool_result(
	store: &SqliteStore,
	leaf_msg: &MessageRecord,
	provider_call_id: &str,
) -> StoreResult<bool> {
	let mut cursor = Some(leaf_msg.clone());
	let mut seen = HashSet::new();
	let mut answered_calls = HashSet::new();

	while let Some(msg) = cursor {
		if !seen.insert(msg.id.clone()) {
			break;
		}
		let payload = &msg.payload;
		let role = payload
			.get("message")
			.and_then(|m| m.get("role"))
			.or_else(|| payload.get("role"))
			.and_then(Value::as_str);

		if role == Some("toolResult") {
			let call_id = payload
				.get("message")
				.and_then(|m| m.get("toolCallId"))
				.or_else(|| payload.get("toolCallId"))
				.or_else(|| payload.get("tool_call_id"))
				.and_then(Value::as_str);
			if let Some(call_id) = call_id {
				answered_calls.insert(call_id.to_string());
			}
			cursor = msg
				.parent_message_id
				.and_then(|id| store.message(&id).ok().flatten());
		} else if role == Some("assistant") {
			if is_assistant_with_call(payload, provider_call_id) {
				if !answered_calls.contains(provider_call_id) {
					return Ok(true);
				}
			}
			break;
		} else {
			break;
		}
	}
	Ok(false)
}

fn branch_point_on_ancestry(
	transaction: &Transaction<'_>,
	parent_branch_id: &str,
	session_id: &str,
	point_message_id: &str,
) -> StoreResult<bool> {
	let branch: Option<(String, Option<String>, Option<String>)> = transaction
		.query_row(
			"SELECT session_id, parent_message_id, leaf_message_id FROM branches WHERE id = ?1",
			params![parent_branch_id],
			|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
		)
		.optional()?;
	let Some((branch_session, branch_parent, branch_leaf)) = branch else {
		return Err(StoreError::NotFound(format!(
			"parent branch {parent_branch_id} does not exist"
		)));
	};
	if branch_session.as_str() != session_id {
		return Err(StoreError::Validation(
			"parent branch belongs to a different session".to_string(),
		));
	}
	let mut cursor = branch_leaf.or(branch_parent);
	let mut seen = HashSet::new();
	while let Some(id) = cursor {
		if id.as_str() == point_message_id {
			return Ok(true);
		}
		if !seen.insert(id.clone()) {
			return Err(StoreError::Corrupt(format!(
				"parent branch {parent_branch_id} contains a message cycle at {id}"
			)));
		}
		let message: Option<(String, Option<String>)> = transaction
			.query_row(
				"SELECT session_id, parent_message_id FROM messages WHERE id = ?1",
				params![id],
				|row| Ok((row.get(0)?, row.get(1)?)),
			)
			.optional()?;
		let Some((message_session, parent_id)) = message else {
			return Err(StoreError::Corrupt(format!(
				"parent branch {parent_branch_id} references missing message {id}"
			)));
		};
		if message_session.as_str() != session_id {
			return Err(StoreError::Corrupt(format!(
				"parent branch {parent_branch_id} chain crosses into session {message_session}"
			)));
		}
		cursor = parent_id;
	}
	Ok(false)
}

fn require_session(transaction: &Transaction<'_>, session_id: &str) -> StoreResult<()> {
	let exists: bool = transaction.query_row(
		"SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
		params![session_id],
		|row| row.get(0),
	)?;
	if !exists {
		return Err(StoreError::NotFound(format!(
			"session {session_id} does not exist"
		)));
	}
	Ok(())
}

fn validate_id(label: &str, value: &str) -> StoreResult<()> {
	if value.is_empty() {
		return Err(StoreError::Validation(format!("{label} must not be empty")));
	}
	Ok(())
}

fn encode_json(value: &Value) -> StoreResult<String> {
	serde_json::to_string(value).map_err(StoreError::Json)
}

fn decode_json(value: &str) -> StoreResult<Value> {
	serde_json::from_str(value).map_err(StoreError::Json)
}

fn now_ms() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
}

/// Runtime-generated execution identity. The prefix and the process-wide
/// counter keep it distinct from provider call ids even in the same millisecond.
fn next_execution_id() -> String {
	static COUNTER: AtomicU64 = AtomicU64::new(0);
	let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_nanos());
	format!("exec-{:x}-{nanos:x}-{counter:x}", std::process::id())
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use serde_json::json;

	use super::*;

	fn seeded_store() -> SqliteStore {
		let mut store = SqliteStore::open_in_memory().expect("open in-memory store");
		store
			.create_session(NewSession {
				id: "s1".to_string(),
				parent_session_id: None,
				payload: json!({ "cwd": "/tmp/project" }),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({ "kind": "root" }),
			})
			.expect("create session");
		store
	}

	fn message(id: &str, parent: Option<&str>) -> NewMessage {
		NewMessage {
			id: id.to_string(),
			session_id: "s1".to_string(),
			branch_id: "b1".to_string(),
			parent_message_id: parent.map(str::to_string),
			payload: json!({ "id": id, "role": "assistant" }),
		}
	}

	fn temp_db(tag: &str) -> (PathBuf, PathBuf) {
		let dir = std::env::temp_dir().join(format!(
			"omp-session-store-{}-{}-{tag}",
			std::process::id(),
			now_ms()
		));
		std::fs::create_dir_all(&dir).expect("create temp dir");
		let path = dir.join("sessions.db");
		(dir, path)
	}

	#[test]
	fn reopen_reports_unresolved_intent() {
		let (dir, path) = temp_db("reopen");
		let execution_id;
		{
			let mut store = SqliteStore::open(&path).expect("open file store");
			store
				.create_session(NewSession {
					id: "s1".to_string(),
					parent_session_id: None,
					payload: json!({ "cwd": "/tmp/project" }),
					initial_branch_id: "b1".to_string(),
					initial_branch_payload: json!({ "kind": "root" }),
				})
				.expect("create session");
			execution_id = store
				.record_intent(NewIntent {
					session_id: "s1".to_string(),
					branch_id: "b1".to_string(),
					provider_call_id: "call-1".to_string(),
					tool_name: "bash".to_string(),
					payload: json!({ "command": "true" }),
				})
				.expect("record intent");
			assert_ne!(execution_id, "call-1", "execution key must be distinct");
			assert_eq!(
				store
					.outcome(&execution_id)
					.expect("read outcome"),
				None
			);
		}
		{
			let store = SqliteStore::open(&path).expect("reopen file store");
			let unresolved = store.unresolved_intents("s1").expect("read unresolved");
			assert_eq!(unresolved.len(), 1);
			assert_eq!(unresolved[0].execution_id, execution_id);
			assert_eq!(unresolved[0].provider_call_id, "call-1");
		}
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn failed_batch_rolls_back_messages_and_leaf() {
		let mut store = seeded_store();
		store.append_message(message("m1", None)).expect("append m1");
		let failed = store.append_messages(&[
			message("m2", Some("m1")),
			// Duplicate primary key: the second insert fails after m2 was written
			// and the leaf advanced. The batch must roll both back.
			NewMessage {
				id: "m1".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m2".to_string()),
				payload: json!({ "id": "m1" }),
			},
		]);
		assert!(matches!(failed, Err(StoreError::Sqlite(_))));
		let messages = store.branch_messages("b1").expect("read branch");
		assert_eq!(messages.len(), 1);
		assert_eq!(messages[0].id, "m1");
		let branch = store.branch("b1").expect("read branch row").expect("branch");
		assert_eq!(branch.leaf_message_id.as_deref(), Some("m1"));
	}

	#[test]
	fn duplicate_outcome_is_idempotent_but_conflicting_outcome_is_rejected() {
		let mut store = seeded_store();
		let execution_id = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "provider-call-7".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({ "command": "true" }),
			})
			.expect("record intent");
		let outcome = json!({ "status": "completed", "result": "ok" });
		store
			.record_outcome(&execution_id, outcome.clone())
			.expect("record outcome");
		store
			.record_outcome(&execution_id, outcome.clone())
			.expect("duplicate outcome is idempotent");
		let conflict = store.record_outcome(
			&execution_id,
			json!({ "status": "completed", "result": "different" }),
		);
		assert!(matches!(conflict, Err(StoreError::Conflict(_))));
		assert_eq!(
			store.outcome(&execution_id).expect("read outcome"),
			Some(outcome)
		);
		assert!(
			store
				.unresolved_intents("s1")
				.expect("read unresolved")
				.is_empty()
		);
	}

	#[test]
	fn offbranch_completion_keeps_origin_and_delivery_states_are_distinct() {
		let mut store = seeded_store();
		store.append_message(message("m1", None)).expect("append m1");
		store
			.create_branch(NewBranch {
				id: "b2".to_string(),
				session_id: "s1".to_string(),
				parent_branch_id: Some("b1".to_string()),
				parent_message_id: Some("m1".to_string()),
				payload: json!({ "kind": "worker-result" }),
			})
			.expect("create branch b2");
		store
			.set_active_branch("s1", "b2")
			.expect("activate b2");
		store
			.append_message(NewMessage {
				id: "m2".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m1".to_string()),
				payload: json!({ "id": "m2", "role": "user" }),
			})
			.expect("append off-branch result");

		let completion = WorkerCompletion {
			worker_id: "task-1".to_string(),
			delivery_id: "task-1:result".to_string(),
			session_id: "s1".to_string(),
			origin_branch_id: "b1".to_string(),
			completion: json!({ "status": "completed" }),
			delivery: json!({ "text": "worker result" }),
		};
		store
			.complete_worker(completion.clone())
			.expect("complete worker");
		store
			.complete_worker(completion.clone())
			.expect("duplicate completion is idempotent");

		// Conflicting reuse must not insert a second completion or delivery.
		let conflicting = WorkerCompletion {
			delivery: json!({ "text": "different result" }),
			..completion.clone()
		};
		let conflict = store.complete_worker(conflicting);
		assert!(matches!(conflict, Err(StoreError::Conflict(_))));

		let session = store.session("s1").expect("read session").expect("session");
		assert_eq!(session.active_branch_id.as_deref(), Some("b2"));
		let origin = store.branch("b1").expect("read branch").expect("branch");
		assert_eq!(origin.leaf_message_id.as_deref(), Some("m2"));
		let pending = store.pending_deliveries("s1").expect("pending deliveries");
		assert_eq!(pending.len(), 1);
		assert_eq!(pending[0].origin_branch_id, "b1");

		let before_delivery = store.mark_delivery_processed("task-1:result", 10);
		assert!(matches!(before_delivery, Err(StoreError::Validation(_))));
		store
			.mark_delivery_delivered("task-1:result", 11)
			.expect("mark delivered");
		assert!(
			store
				.pending_deliveries("s1")
				.expect("pending deliveries")
				.is_empty()
		);
		let unprocessed = store
			.unprocessed_deliveries("s1")
			.expect("unprocessed deliveries");
		assert_eq!(unprocessed.len(), 1);
		assert_eq!(unprocessed[0].delivered_at_ms, Some(11));
		assert_eq!(unprocessed[0].processed_at_ms, None);

		store
			.mark_delivery_processed("task-1:result", 12)
			.expect("mark processed");
		assert!(
			store
				.unprocessed_deliveries("s1")
				.expect("unprocessed deliveries")
				.is_empty()
		);
	}

	#[test]
	fn pending_delivery_survives_reopen_and_keeps_offbranch_origin() {
		let (dir, path) = temp_db("pending-reopen");
		{
			let mut store = SqliteStore::open(&path).expect("open file store");
			store
				.create_session(NewSession {
					id: "s1".to_string(),
					parent_session_id: None,
					payload: json!({ "cwd": "/tmp/project" }),
					initial_branch_id: "b1".to_string(),
					initial_branch_payload: json!({ "kind": "root" }),
				})
				.expect("create session");
			store.append_message(message("m1", None)).expect("append m1");
			store
				.create_branch(NewBranch {
					id: "b2".to_string(),
					session_id: "s1".to_string(),
					parent_branch_id: Some("b1".to_string()),
					parent_message_id: Some("m1".to_string()),
					payload: json!({ "kind": "cut" }),
				})
				.expect("create branch");
			store.set_active_branch("s1", "b2").expect("cut to b2");
			store
				.complete_worker(WorkerCompletion {
					worker_id: "ns:task-1".to_string(),
					delivery_id: "ns:task-1".to_string(),
					session_id: "s1".to_string(),
					origin_branch_id: "b1".to_string(),
					completion: json!({ "status": "completed" }),
					delivery: json!({ "text": "result" }),
				})
				.expect("complete worker on b1");
		}
		let mut store = SqliteStore::open(&path).expect("reopen file store");
		let session = store.session("s1").expect("session").expect("row");
		assert_eq!(session.active_branch_id.as_deref(), Some("b2"));
		let pending = store.pending_deliveries("s1").expect("pending");
		assert_eq!(pending.len(), 1);
		assert_eq!(pending[0].delivery_id, "ns:task-1");
		assert_eq!(
			pending[0].origin_branch_id, "b1",
			"a later branch cut must not move the parent notification"
		);
		// Consume into the target context and keep processing a separate mark.
		store
			.consume_delivery(
				"ns:task-1",
				NewMessage {
					id: "m2".to_string(),
					session_id: "s1".to_string(),
					branch_id: "b2".to_string(),
					parent_message_id: Some("m1".to_string()),
					payload: json!({ "id": "m2", "role": "user", "text": "result" }),
				},
				10,
			)
			.expect("consume delivery into b2");
		assert!(store.pending_deliveries("s1").expect("pending").is_empty());
		let unprocessed = store.unprocessed_deliveries("s1").expect("unprocessed");
		assert_eq!(unprocessed.len(), 1);
		assert_eq!(unprocessed[0].delivered_at_ms, Some(10));
		assert_eq!(unprocessed[0].processed_at_ms, None);
		store
			.mark_delivery_processed("ns:task-1", 11)
			.expect("explicit processed checkpoint");
		assert!(store.unprocessed_deliveries("s1").expect("done").is_empty());
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn child_intent_is_durable_before_any_tool_effect() {
		let mut store = seeded_store();
		store
			.create_session(NewSession {
				id: "child-1".to_string(),
				parent_session_id: Some("s1".to_string()),
				payload: json!({ "cwd": "/tmp/project", "parentSession": "s1" }),
				initial_branch_id: "child-1.main".to_string(),
				initial_branch_payload: json!({ "kind": "root" }),
			})
			.expect("create child session");
		let execution_id = store
			.record_intent(NewIntent {
				session_id: "child-1".to_string(),
				branch_id: "child-1.main".to_string(),
				provider_call_id: "child-call-1".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({ "command": "rm -rf build" }),
			})
			.expect("child intent before effect");
		let unresolved = store
			.unresolved_intents("child-1")
			.expect("read child unresolved");
		assert_eq!(unresolved.len(), 1);
		assert_eq!(unresolved[0].execution_id, execution_id);
		assert_eq!(unresolved[0].tool_name, "bash");
		assert_eq!(
			store.outcome(&execution_id).expect("outcome"),
			None,
			"the intent must not claim the effect ran"
		);
	}

	#[test]
	fn branch_point_must_be_on_declared_parent_ancestry() {
		let mut store = seeded_store();
		store.append_message(message("m1", None)).expect("append m1");
		store
			.create_branch(NewBranch {
				id: "b2".to_string(),
				session_id: "s1".to_string(),
				parent_branch_id: Some("b1".to_string()),
				parent_message_id: Some("m1".to_string()),
				payload: json!({ "kind": "child" }),
			})
			.expect("create b2 at m1");
		store
			.append_message(NewMessage {
				id: "m2".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b2".to_string(),
				parent_message_id: Some("m1".to_string()),
				payload: json!({ "id": "m2" }),
			})
			.expect("append m2 to b2");

		// m1 is on b2's ancestry through its branch point, so it is a legal
		// point even though the message row itself belongs to b1.
		store
			.create_branch(NewBranch {
				id: "b3".to_string(),
				session_id: "s1".to_string(),
				parent_branch_id: Some("b2".to_string()),
				parent_message_id: Some("m1".to_string()),
				payload: json!({ "kind": "ancestor-point" }),
			})
			.expect("create b3 at ancestor point");
		let b3_context: Vec<String> = store
			.branch_messages("b3")
			.expect("b3 context")
			.into_iter()
			.map(|message| message.id)
			.collect();
		assert_eq!(b3_context, vec!["m1".to_string()]);

		// Both missing is the explicit empty root branch contract.
		store
			.create_branch(NewBranch {
				id: "root2".to_string(),
				session_id: "s1".to_string(),
				parent_branch_id: None,
				parent_message_id: None,
				payload: json!({ "kind": "root" }),
			})
			.expect("create empty root branch");
		assert!(
			store
				.branch_messages("root2")
				.expect("empty root context")
				.is_empty()
		);
		store
			.append_message(NewMessage {
				id: "r1".to_string(),
				session_id: "s1".to_string(),
				branch_id: "root2".to_string(),
				parent_message_id: None,
				payload: json!({ "id": "r1" }),
			})
			.expect("append r1 to empty root");

		let missing_point = store.create_branch(NewBranch {
			id: "bad-missing-point".to_string(),
			session_id: "s1".to_string(),
			parent_branch_id: Some("b1".to_string()),
			parent_message_id: None,
			payload: json!({}),
		});
		assert!(matches!(missing_point, Err(StoreError::Validation(_))));

		let missing_parent = store.create_branch(NewBranch {
			id: "bad-missing-parent".to_string(),
			session_id: "s1".to_string(),
			parent_branch_id: None,
			parent_message_id: Some("m1".to_string()),
			payload: json!({}),
		});
		assert!(matches!(missing_parent, Err(StoreError::Validation(_))));

		// r1 is in the same session but on an unrelated root, not in b2's
		// ancestry.
		let unrelated = store.create_branch(NewBranch {
			id: "bad-unrelated".to_string(),
			session_id: "s1".to_string(),
			parent_branch_id: Some("b2".to_string()),
			parent_message_id: Some("r1".to_string()),
			payload: json!({}),
		});
		assert!(matches!(unrelated, Err(StoreError::Validation(_))));
	}

	#[test]
	fn artifact_branch_must_match_execution_branch() {
		let mut store = seeded_store();
		store.append_message(message("m1", None)).expect("append m1");
		store
			.create_branch(NewBranch {
				id: "b2".to_string(),
				session_id: "s1".to_string(),
				parent_branch_id: Some("b1".to_string()),
				parent_message_id: Some("m1".to_string()),
				payload: json!({ "kind": "child" }),
			})
			.expect("create b2");
		let execution_id = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b2".to_string(),
				provider_call_id: "artifact-call".to_string(),
				tool_name: "write".to_string(),
				payload: json!({ "path": "out.txt" }),
			})
			.expect("record intent");

		let mismatch = store.record_artifact(NewArtifact {
			artifact_id: "a1".to_string(),
			session_id: "s1".to_string(),
			branch_id: Some("b1".to_string()),
			execution_id: Some(execution_id.clone()),
			location: "artifact://1".to_string(),
			metadata: json!({ "bytes": 1 }),
		});
		assert!(matches!(mismatch, Err(StoreError::Validation(_))));
		assert!(
			store
				.artifacts("s1")
				.expect("artifacts after mismatch")
				.is_empty()
		);

		store
			.record_artifact(NewArtifact {
				artifact_id: "a1".to_string(),
				session_id: "s1".to_string(),
				branch_id: Some("b2".to_string()),
				execution_id: Some(execution_id),
				location: "artifact://1".to_string(),
				metadata: json!({ "bytes": 1 }),
			})
			.expect("record matching artifact");
		let artifacts = store.artifacts("s1").expect("artifacts");
		assert_eq!(artifacts.len(), 1);
		assert_eq!(artifacts[0].branch_id.as_deref(), Some("b2"));
	}

	#[test]
	fn record_outcome_with_message_commits_atomically() {
		let mut store = seeded_store();
		store.append_message(message("m1", None)).expect("append m1");
		let execution_id = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-1".to_string(),
				tool_name: "test_tool".to_string(),
				payload: json!({ "arg": 1 }),
			})
			.expect("record intent");

		let outcome = json!({ "content": "success", "isError": false });
		let msg = NewMessage {
			id: "m2".to_string(),
			session_id: "s1".to_string(),
			branch_id: "b1".to_string(),
			parent_message_id: Some("m1".to_string()),
			payload: json!({
				"type": "message",
				"id": "m2",
				"parentId": "m1",
				"message": {
					"role": "toolResult",
					"toolCallId": "call-1",
					"toolName": "test_tool",
					"content": "success",
					"isError": false,
					"details": { "executionId": execution_id }
				}
			}),
		};

		let first = store
			.record_outcome_with_message(&execution_id, outcome.clone(), msg.clone())
			.expect("atomic record outcome with message");
		assert!(first, "first record must commit rows");

		// Both outcome and message are committed.
		assert_eq!(store.outcome(&execution_id).expect("read outcome"), Some(outcome.clone()));
		let branch = store.branch("b1").expect("read branch").expect("b1");
		assert_eq!(branch.leaf_message_id.as_deref(), Some("m2"));
		assert!(store.unresolved_intents("s1").expect("unresolved").is_empty());

		// Duplicate call with exact same message is idempotent no-op.
		let duplicate = store
			.record_outcome_with_message(&execution_id, outcome.clone(), msg)
			.expect("idempotent duplicate");
		assert!(!duplicate, "duplicate must be a no-op");

		// Duplicate call with DIFFERENT message id is also idempotent no-op (does not duplicate message row or change leaf).
		let diff_msg = NewMessage {
			id: "m2_retry_different_id".to_string(),
			session_id: "s1".to_string(),
			branch_id: "b1".to_string(),
			parent_message_id: Some("m1".to_string()),
			payload: json!({
				"type": "message",
				"id": "m2_retry_different_id",
				"parentId": "m1",
				"message": {
					"role": "toolResult",
					"toolCallId": "call-1",
					"toolName": "test_tool",
					"content": "success",
					"isError": false,
					"details": { "executionId": execution_id }
				}
			}),
		};
		let diff_dup = store
			.record_outcome_with_message(&execution_id, outcome, diff_msg)
			.expect("duplicate with different message id is idempotent");
		assert!(!diff_dup, "duplicate with different message id must not append row");
		let branch = store.branch("b1").expect("read branch").expect("b1");
		assert_eq!(branch.leaf_message_id.as_deref(), Some("m2"), "leaf must remain m2");
		assert!(store.message("m2_retry_different_id").expect("read retry").is_none());

		// Conflicting outcome is rejected.
		let conflict = store.record_outcome_with_message(
			&execution_id,
			json!({ "content": "different", "isError": true }),
			NewMessage {
				id: "m3".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m2".to_string()),
				payload: json!({ "id": "m3" }),
			},
		);
		assert!(matches!(conflict, Err(StoreError::Conflict(_))));
	}

	#[test]
	fn record_outcome_with_message_validates_call_id_linkage() {
		let mut store = seeded_store();
		store.append_message(message("m1", None)).expect("append m1");
		let execution_id = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-expected".to_string(),
				tool_name: "tool_expected".to_string(),
				payload: json!({}),
			})
			.expect("record intent");

		let outcome = json!({ "content": "ok", "isError": false });

		// Mismatched toolCallId
		let bad_call_msg = NewMessage {
			id: "m2".to_string(),
			session_id: "s1".to_string(),
			branch_id: "b1".to_string(),
			parent_message_id: Some("m1".to_string()),
			payload: json!({
				"message": {
					"toolCallId": "call-WRONG",
					"toolName": "tool_expected"
				}
			}),
		};
		let err = store.record_outcome_with_message(&execution_id, outcome.clone(), bad_call_msg);
		assert!(matches!(err, Err(StoreError::Validation(_))));

		// Mismatched toolName
		let bad_tool_msg = NewMessage {
			id: "m2".to_string(),
			session_id: "s1".to_string(),
			branch_id: "b1".to_string(),
			parent_message_id: Some("m1".to_string()),
			payload: json!({
				"message": {
					"toolCallId": "call-expected",
					"toolName": "tool_WRONG"
				}
			}),
		};
		let err = store.record_outcome_with_message(&execution_id, outcome.clone(), bad_tool_msg);
		assert!(matches!(err, Err(StoreError::Validation(_))));

		// Mismatched details.executionId
		let bad_exec_msg = NewMessage {
			id: "m2".to_string(),
			session_id: "s1".to_string(),
			branch_id: "b1".to_string(),
			parent_message_id: Some("m1".to_string()),
			payload: json!({
				"message": {
					"toolCallId": "call-expected",
					"toolName": "tool_expected",
					"details": { "executionId": "other_execution" }
				}
			}),
		};
		let err = store.record_outcome_with_message(&execution_id, outcome, bad_exec_msg);
		assert!(matches!(err, Err(StoreError::Validation(_))));
	}
	#[test]
	fn record_outcome_with_message_rolls_back_on_stale_leaf() {
		let mut store = seeded_store();
		store.append_message(message("m1", None)).expect("append m1");
		store.append_message(message("m2", Some("m1"))).expect("append m2");

		let execution_id = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-stale".to_string(),
				tool_name: "test_tool".to_string(),
				payload: json!({}),
			})
			.expect("record intent");

		// Message chains to stale parent "m1" instead of current leaf "m2".
		let stale_msg = NewMessage {
			id: "m3".to_string(),
			session_id: "s1".to_string(),
			branch_id: "b1".to_string(),
			parent_message_id: Some("m1".to_string()),
			payload: json!({ "id": "m3", "role": "toolResult" }),
		};

		let outcome = json!({ "content": "should not commit", "isError": false });
		let err = store.record_outcome_with_message(&execution_id, outcome, stale_msg);
		assert!(matches!(err, Err(StoreError::Validation(_))));

		// Neither row was committed.
		assert_eq!(store.outcome(&execution_id).expect("read outcome"), None);
		let unresolved = store.unresolved_intents("s1").expect("unresolved");
		assert_eq!(unresolved.len(), 1);
		assert_eq!(unresolved[0].execution_id, execution_id);

		let branch = store.branch("b1").expect("read branch").expect("b1");
		assert_eq!(branch.leaf_message_id.as_deref(), Some("m2"));
		assert!(store.message("m3").expect("read m3").is_none());
	}

	#[test]
	fn recover_unmessaged_outcomes_recovers_resolvable_legacy_row() {
		let mut store = seeded_store();
		// Assistant message calls tool "call-rec"
		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": [
							{
								"type": "toolCall",
								"id": "call-rec",
								"name": "bash"
							}
						]
					}
				}),
			})
			.expect("append assistant message");

		let execution_id = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-rec".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({ "cmd": "ls" }),
			})
			.expect("record intent");

		let outcome = json!({ "content": "file1.txt", "isError": false });
		// Simulate pre-atomic crash: outcome is committed, but message was not appended.
		store
			.record_outcome(&execution_id, outcome.clone())
			.expect("record outcome");

		let needing = store.executions_needing_recovery("s1").expect("needing recovery");
		assert_eq!(needing.len(), 1);
		assert_eq!(needing[0].execution_id, execution_id);

		let recovered_count = store.recover_unmessaged_outcomes("s1").expect("recover");
		assert_eq!(recovered_count, 1);

		// Now no executions need recovery.
		assert!(store.executions_needing_recovery("s1").expect("needing").is_empty());

		// Branch leaf has advanced to the recovered toolResult message.
		let branch = store.branch("b1").expect("read branch").expect("b1");
		let leaf_id = branch.leaf_message_id.expect("leaf id");
		let leaf = store.message(&leaf_id).expect("read leaf").expect("leaf message");
		let payload = &leaf.payload;
		assert_eq!(payload["message"]["role"], "toolResult");
		assert_eq!(payload["message"]["toolCallId"], "call-rec");
		assert_eq!(payload["message"]["details"]["executionId"], execution_id);
		assert_eq!(payload["message"]["details"]["recovered"], true);
		assert!(payload["timestamp"].is_string(), "entry timestamp must be ISO string");
		assert!(payload["message"]["timestamp"].is_number(), "message timestamp must be number");

		// Subsequent recovery call is a no-op.
		assert_eq!(store.recover_unmessaged_outcomes("s1").expect("recover 2"), 0);
	}

	#[test]
	fn recover_unmessaged_outcomes_refuses_when_branch_moved_past() {
		let mut store = seeded_store();
		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": [
							{
								"type": "toolCall",
								"id": "call-lost",
								"name": "bash"
							}
						]
					}
				}),
			})
			.expect("append assistant message");

		let execution_id = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-lost".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("record intent");

		store
			.record_outcome(&execution_id, json!({ "content": "ok", "isError": false }))
			.expect("record outcome");

		// Branch moves past to a subsequent user message.
		store
			.append_message(NewMessage {
				id: "m_user_next".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_user_next",
					"message": {
						"role": "user",
						"content": "next turn"
					}
				}),
			})
			.expect("append next user turn");

		// Recovery must refuse to attach the tool result after the subsequent user turn.
		let recovered = store.recover_unmessaged_outcomes("s1").expect("recover");
		assert_eq!(recovered, 0);

		// Branch leaf remains untouched at m_user_next.
		let branch = store.branch("b1").expect("branch").expect("b1");
		assert_eq!(branch.leaf_message_id.as_deref(), Some("m_user_next"));
	}

	#[test]
	fn recover_unmessaged_outcomes_skips_legacy_message_without_execution_id() {
		let mut store = seeded_store();
		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": [
							{
								"type": "toolCall",
								"id": "call-leg-present",
								"name": "bash"
							}
						]
					}
				}),
			})
			.expect("append assistant message");

		// Legacy tool result message on branch with matching toolCallId but NO details.executionId:
		store
			.append_message(NewMessage {
				id: "m_tool_legacy".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_tool_legacy",
					"message": {
						"role": "toolResult",
						"toolCallId": "call-leg-present",
						"toolName": "bash",
						"content": "already ran",
						"isError": false
					}
				}),
			})
			.expect("append legacy tool message");

		let execution_id = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-leg-present".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("record intent");

		store
			.record_outcome(&execution_id, json!({ "content": "already ran", "isError": false }))
			.expect("record outcome");

		// Because a legacy message for call-leg-present already exists, recovery must skip it.
		let needing = store.executions_needing_recovery("s1").expect("needing recovery");
		assert!(needing.is_empty(), "execution must not need recovery when legacy message is present");

		let recovered = store.recover_unmessaged_outcomes("s1").expect("recover");
		assert_eq!(recovered, 0);

		let branch = store.branch("b1").expect("branch").expect("b1");
		assert_eq!(branch.leaf_message_id.as_deref(), Some("m_tool_legacy"));
	}

	#[test]
	fn recover_unmessaged_outcomes_recovers_missing_sibling_when_first_already_persisted() {
		let mut store = seeded_store();
		// Assistant calls two tools: call-A and call-B in batch.
		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": [
							{
								"type": "toolCall",
								"id": "call-A",
								"name": "bash"
							},
							{
								"type": "toolCall",
								"id": "call-B",
								"name": "bash"
							}
						]
					}
				}),
			})
			.expect("append batch assistant message");

		let exec_a = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-A".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent A");
		let exec_b = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-B".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent B");

		// Tool A finished and its message was appended:
		store
			.record_outcome_with_message(
				&exec_a,
				json!({ "content": "output A", "isError": false }),
				NewMessage {
					id: "m_tool_A".to_string(),
					session_id: "s1".to_string(),
					branch_id: "b1".to_string(),
					parent_message_id: Some("m_asst".to_string()),
					payload: json!({
						"type": "message",
						"id": "m_tool_A",
						"parentId": "m_asst",
						"message": {
							"role": "toolResult",
							"toolCallId": "call-A",
							"toolName": "bash",
							"content": "output A",
							"isError": false,
							"details": { "executionId": exec_a }
						}
					}),
				},
			)
			.expect("record outcome A with message");

		// Tool B finished in SQLite, but crashed before message was appended:
		store
			.record_outcome(&exec_b, json!({ "content": "output B", "isError": false }))
			.expect("record outcome B");

		// Only B needs recovery:
		let needing = store.executions_needing_recovery("s1").expect("needing");
		assert_eq!(needing.len(), 1);
		assert_eq!(needing[0].execution_id, exec_b);

		// Recovery must see leaf m_tool_A as a valid sibling tool result and attach B to it:
		let recovered = store.recover_unmessaged_outcomes("s1").expect("recover");
		assert_eq!(recovered, 1);

		let branch = store.branch("b1").expect("branch").expect("b1");
		let leaf_id = branch.leaf_message_id.expect("leaf id");
		let leaf_b = store.message(&leaf_id).expect("message").expect("leaf message");
		assert_eq!(leaf_b.parent_message_id.as_deref(), Some("m_tool_A"));
		assert_eq!(leaf_b.payload["message"]["toolCallId"], "call-B");
		assert_eq!(leaf_b.payload["message"]["details"]["executionId"], exec_b);

		// No more unmessaged outcomes:
		assert!(store.executions_needing_recovery("s1").expect("needing").is_empty());
	}

	#[test]
	fn recover_unmessaged_outcomes_recovers_multiple_unmessaged_batch_results() {
		let mut store = seeded_store();
		// Assistant calls two tools: call-A and call-B in batch.
		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": [
							{
								"type": "toolCall",
								"id": "call-A",
								"name": "bash"
							},
							{
								"type": "toolCall",
								"id": "call-B",
								"name": "bash"
							}
						]
					}
				}),
			})
			.expect("append batch assistant message");

		let exec_a = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-A".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent A");
		let exec_b = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-B".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent B");

		// Both A and B executed in SQLite, but crash happened before messages were appended.
		store
			.record_outcome(&exec_a, json!({ "content": "output A", "isError": false }))
			.expect("record outcome A");
		store
			.record_outcome(&exec_b, json!({ "content": "output B", "isError": false }))
			.expect("record outcome B");

		let needing = store.executions_needing_recovery("s1").expect("needing");
		assert_eq!(needing.len(), 2);

		let recovered = store.recover_unmessaged_outcomes("s1").expect("recover");
		assert_eq!(recovered, 2);

		// Verify chain: m_asst -> msg-rec-A -> msg-rec-B
		let branch = store.branch("b1").expect("branch").expect("b1");
		let leaf_id = branch.leaf_message_id.expect("leaf id");
		let leaf_b = store.message(&leaf_id).expect("message").expect("leaf message");
		assert_eq!(leaf_b.payload["message"]["toolCallId"], "call-B");

		let leaf_a_id = leaf_b.parent_message_id.expect("parent A");
		let leaf_a = store.message(&leaf_a_id).expect("message").expect("message A");
		assert_eq!(leaf_a.parent_message_id.as_deref(), Some("m_asst"));
		assert_eq!(leaf_a.payload["message"]["toolCallId"], "call-A");

		assert!(store.executions_needing_recovery("s1").expect("needing").is_empty());
	}

	#[test]
	fn recover_unmessaged_outcomes_atomically_updates_durable_projection_kept() {
		let mut store = SqliteStore::open_in_memory().expect("open store");
		store
			.create_session(NewSession {
				id: "s_proj".to_string(),
				parent_session_id: None,
				payload: json!({
					"cwd": "/tmp",
					"durableProjection": {
						"kept": ["m_asst"]
					}
				}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({ "kind": "root" }),
			})
			.expect("create session with projection");

		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s_proj".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": [
							{
								"type": "toolCall",
								"id": "call-compact-test",
								"name": "bash"
							}
						]
					}
				}),
			})
			.expect("append assistant message");

		let execution_id = store
			.record_intent(NewIntent {
				session_id: "s_proj".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-compact-test".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("record intent");

		store
			.record_outcome(&execution_id, json!({ "content": "compact result", "isError": false }))
			.expect("record outcome");

		let recovered = store.recover_unmessaged_outcomes("s_proj").expect("recover");
		assert_eq!(recovered, 1);

		let expected_msg_id = format!("msg-rec-{}", execution_id);

		// Verify durableProjection.kept was updated atomically with the message:
		let session_rec = store.session("s_proj").expect("read session").expect("session");
		let kept = session_rec
			.payload
			.get("durableProjection")
			.and_then(|p| p.get("kept"))
			.and_then(Value::as_array)
			.expect("kept array");
		assert_eq!(kept.len(), 2);
		assert_eq!(kept[0].as_str(), Some("m_asst"));
		assert_eq!(kept[1].as_str(), Some(expected_msg_id.as_str()));
	}

	#[test]
	fn reconcile_durable_projection_filters_metadata_rows_while_verifying_lineage() {
		let mut store = SqliteStore::open_in_memory().expect("open store");
		store
			.create_session(NewSession {
				id: "s_meta".to_string(),
				parent_session_id: None,
				payload: json!({
					"durableProjection": {
						"kept": ["m_asst"]
					}
				}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		// Assistant message (kept)
		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s_meta".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": { "role": "assistant", "content": "calling" }
				}),
			})
			.expect("m_asst");

		// Suffix message 1: metadata row (e.g. custom/service_tier_change)
		store
			.append_message(NewMessage {
				id: "m_meta_1".to_string(),
				session_id: "s_meta".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({
					"type": "custom",
					"id": "m_meta_1",
					"data": { "key": "value" }
				}),
			})
			.expect("m_meta_1");

		// Suffix message 2: tool result message
		store
			.append_message(NewMessage {
				id: "m_tool_1".to_string(),
				session_id: "s_meta".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_meta_1".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_tool_1",
					"message": { "role": "toolResult", "content": "res 1" }
				}),
			})
			.expect("m_tool_1");

		// Suffix message 3: another metadata row (e.g. branch_summary or title_change)
		store
			.append_message(NewMessage {
				id: "m_meta_2".to_string(),
				session_id: "s_meta".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_tool_1".to_string()),
				payload: json!({
					"type": "branch_summary",
					"id": "m_meta_2",
					"summary": "checkpoint"
				}),
			})
			.expect("m_meta_2");

		// Suffix message 4: assistant message
		store
			.append_message(NewMessage {
				id: "m_asst_2".to_string(),
				session_id: "s_meta".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_meta_2".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_asst_2",
					"message": { "role": "assistant", "content": "done" }
				}),
			})
			.expect("m_asst_2");

		// Reconcile projection:
		let backfilled = store.reconcile_durable_projection("s_meta").expect("reconcile");
		// Exactly 2 messages backfilled (m_tool_1 and m_asst_2), metadata rows m_meta_1 and m_meta_2 skipped!
		assert_eq!(backfilled, 2);

		let session = store.session("s_meta").expect("session").expect("s_meta");
		let kept = session.payload["durableProjection"]["kept"].as_array().expect("kept array");
		let kept_ids: Vec<&str> = kept.iter().filter_map(Value::as_str).collect();
		assert_eq!(kept_ids, vec!["m_asst", "m_tool_1", "m_asst_2"]);
		assert!(!kept_ids.contains(&"m_meta_1"));
		assert!(!kept_ids.contains(&"m_meta_2"));
	}

	#[test]
	fn reconcile_durable_projection_discards_stale_diverged_projection_preserving_history() {
		let mut store = SqliteStore::open_in_memory().expect("open store");
		store
			.create_session(NewSession {
				id: "s_diverge".to_string(),
				parent_session_id: None,
				payload: json!({
					"durableProjection": {
						"kept": ["m_user", "m_stale_branch1_msg"]
					}
				}),
				initial_branch_id: "b_active".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		// On active branch, only m_user exists, m_stale_branch1_msg does NOT exist on b_active:
		store
			.append_message(NewMessage {
				id: "m_user".to_string(),
				session_id: "s_diverge".to_string(),
				branch_id: "b_active".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_user",
					"message": { "role": "user", "content": "hi" }
				}),
			})
			.expect("m_user");

		let backfilled = store
			.reconcile_durable_projection("s_diverge")
			.expect("must safely discard stale projection without failing session");
		assert_eq!(backfilled, 0);

		let session = store.session("s_diverge").expect("session").expect("s_diverge");
		assert!(session.payload.get("durableProjection").is_none());

		// History is preserved: reading branch messages remains valid
		let branch_msgs = store.branch_messages("b_active").expect("branch messages");
		assert_eq!(branch_msgs.len(), 1);
		assert_eq!(branch_msgs[0].id, "m_user");
	}

	#[test]
	fn reconcile_durable_projection_preserves_compaction_with_offbranch_sibling_tool_result() {
		let mut store = SqliteStore::open_in_memory().expect("open store");
		store
			.create_session(NewSession {
				id: "s_sibling".to_string(),
				parent_session_id: None,
				payload: json!({
					"durableProjection": {
						"kept": ["m_asst", "m_tool_offbranch"]
					}
				}),
				initial_branch_id: "b_active".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		// Active branch has assistant calling tool call-sibling
		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s_sibling".to_string(),
				branch_id: "b_active".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": [
							{
								"type": "toolCall",
								"id": "call-sibling",
								"name": "bash"
							}
						]
					}
				}),
			})
			.expect("append m_asst");

		// Off-branch message on another branch (e.g. b_other) answering call-sibling
		store
			.create_branch(NewBranch {
				id: "b_other".to_string(),
				session_id: "s_sibling".to_string(),
				parent_branch_id: Some("b_active".to_string()),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({}),
			})
			.expect("create branch b_other");

		store
			.append_message(NewMessage {
				id: "m_tool_offbranch".to_string(),
				session_id: "s_sibling".to_string(),
				branch_id: "b_other".to_string(),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_tool_offbranch",
					"message": {
						"role": "toolResult",
						"toolCallId": "call-sibling",
						"toolName": "bash",
						"content": "sibling result"
					}
				}),
			})
			.expect("append m_tool_offbranch");

		// Reconcile must recognize m_tool_offbranch as a valid sibling tool result answering call-sibling from m_asst!
		let backfilled = store
			.reconcile_durable_projection("s_sibling")
			.expect("reconcile must succeed without error");
		assert_eq!(backfilled, 0);

		// durableProjection must be KEPT, not discarded!
		let session = store.session("s_sibling").expect("get").expect("s");
		let projection = session.payload.get("durableProjection");
		assert!(projection.is_some(), "durableProjection must be preserved for valid sibling toolResult");
		let kept = projection.unwrap()["kept"].as_array().expect("kept array");
		assert_eq!(kept, json!(["m_asst", "m_tool_offbranch"]).as_array().unwrap());
	}

	#[test]
	fn reconcile_durable_projection_preserves_compaction_with_duplicate_call_ids() {
		let mut store = SqliteStore::open_in_memory().expect("open store");
		store
			.create_session(NewSession {
				id: "s_dup".to_string(),
				parent_session_id: None,
				payload: json!({
					"durableProjection": {
						"kept": ["m_asst", "m_tool_onbranch", "m_tool_offbranch"]
					}
				}),
				initial_branch_id: "b_active".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		// Assistant calls tool "dup-call" TWICE with same provider call id
		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s_dup".to_string(),
				branch_id: "b_active".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": [
							{ "type": "toolCall", "id": "dup-call", "name": "bash" },
							{ "type": "toolCall", "id": "dup-call", "name": "bash" }
						]
					}
				}),
			})
			.expect("append m_asst");

		// First tool result is on active branch
		store
			.append_message(NewMessage {
				id: "m_tool_onbranch".to_string(),
				session_id: "s_dup".to_string(),
				branch_id: "b_active".to_string(),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_tool_onbranch",
					"message": {
						"role": "toolResult",
						"toolCallId": "dup-call",
						"toolName": "bash",
						"content": "res 1"
					}
				}),
			})
			.expect("append m_tool_onbranch");

		// Second tool result is off-branch on b_other
		store
			.create_branch(NewBranch {
				id: "b_other".to_string(),
				session_id: "s_dup".to_string(),
				parent_branch_id: Some("b_active".to_string()),
				parent_message_id: Some("m_tool_onbranch".to_string()),
				payload: json!({}),
			})
			.expect("create branch b_other");

		store
			.append_message(NewMessage {
				id: "m_tool_offbranch".to_string(),
				session_id: "s_dup".to_string(),
				branch_id: "b_other".to_string(),
				parent_message_id: Some("m_tool_onbranch".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_tool_offbranch",
					"message": {
						"role": "toolResult",
						"toolCallId": "dup-call",
						"toolName": "bash",
						"content": "res 2"
					}
				}),
			})
			.expect("append m_tool_offbranch");

		let backfilled = store
			.reconcile_durable_projection("s_dup")
			.expect("reconcile must succeed with duplicate call ids");
		assert_eq!(backfilled, 0);

		let session = store.session("s_dup").expect("get").expect("s");
		let projection = session.payload.get("durableProjection");
		assert!(projection.is_some(), "projection must be preserved for duplicate call ids");
		let kept = projection.unwrap()["kept"].as_array().expect("kept array");
		assert_eq!(
			kept,
			json!(["m_asst", "m_tool_onbranch", "m_tool_offbranch"]).as_array().unwrap()
		);
	}

	#[test]
	fn reconcile_durable_projection_discards_offbranch_tool_result_with_unmatched_call_id() {
		let mut store = SqliteStore::open_in_memory().expect("open store");
		store
			.create_session(NewSession {
				id: "s_unmatched".to_string(),
				parent_session_id: None,
				payload: json!({
					"durableProjection": {
						"kept": ["m_asst", "m_tool_unmatched"]
					}
				}),
				initial_branch_id: "b_active".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s_unmatched".to_string(),
				branch_id: "b_active".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": [{ "type": "toolCall", "id": "call-A", "name": "bash" }]
					}
				}),
			})
			.expect("append m_asst");

		store
			.create_branch(NewBranch {
				id: "b_other".to_string(),
				session_id: "s_unmatched".to_string(),
				parent_branch_id: Some("b_active".to_string()),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({}),
			})
			.expect("create branch b_other");

		// Tool result answering call-WRONG
		store
			.append_message(NewMessage {
				id: "m_tool_unmatched".to_string(),
				session_id: "s_unmatched".to_string(),
				branch_id: "b_other".to_string(),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_tool_unmatched",
					"message": {
						"role": "toolResult",
						"toolCallId": "call-WRONG",
						"toolName": "bash",
						"content": "wrong call id"
					}
				}),
			})
			.expect("append m_tool_unmatched");

		// Must safely discard projection because call-WRONG was not called by m_asst:
		let backfilled = store.reconcile_durable_projection("s_unmatched").expect("reconcile");
		assert_eq!(backfilled, 0);

		let session = store.session("s_unmatched").expect("get").expect("s");
		assert!(session.payload.get("durableProjection").is_none(), "projection with unmatched tool call id must be discarded");
	}

	#[test]
	fn reconcile_durable_projection_fails_closed_on_broken_branch_suffix_lineage() {
		let mut store = SqliteStore::open_in_memory().expect("open store");
		store
			.create_session(NewSession {
				id: "s_broken".to_string(),
				parent_session_id: None,
				payload: json!({
					"durableProjection": {
						"kept": ["m1"]
					}
				}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		store
			.append_message(NewMessage {
				id: "m1".to_string(),
				session_id: "s_broken".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({ "type": "message", "id": "m1" }),
			})
			.expect("m1");

		store
			.connection
			.execute(
				"INSERT INTO messages (id, session_id, branch_id, parent_message_id, payload_json, created_at_ms) \
				 VALUES ('m2', 's_broken', 'b1', 'wrong_parent', '{\"type\":\"message\",\"id\":\"m2\"}', 1000)",
				[],
			)
			.expect("inject broken row");
		store
			.connection
			.execute("UPDATE branches SET leaf_message_id = 'm2' WHERE id = 'b1'", [])
			.expect("update leaf");

		let err = store
			.reconcile_durable_projection("s_broken")
			.expect_err("broken suffix lineage must fail closed");
		assert!(matches!(err, StoreError::Corrupt(_)));
		assert!(err.to_string().contains("wrong_parent"));
	}

	#[test]
	fn reconcile_durable_projection_concurrent_interleaving_race() {
		let (dir, path) = temp_db("concurrent-race");
		let mut store1 = SqliteStore::open(&path).expect("open store 1");
		let mut store2 = SqliteStore::open(&path).expect("open store 2");

		store1
			.create_session(NewSession {
				id: "s_race".to_string(),
				parent_session_id: None,
				payload: json!({
					"durableProjection": {
						"kept": ["m_asst"]
					}
				}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		store1
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s_race".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": { "role": "assistant", "content": "hello" }
				}),
			})
			.expect("append m_asst");

		store1
			.append_message(NewMessage {
				id: "m_tool_1".to_string(),
				session_id: "s_race".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_tool_1",
					"message": { "role": "toolResult", "content": "res" }
				}),
			})
			.expect("append m_tool_1");

		// Interleaving: connection 2 starts an IMMEDIATE transaction and clears the projection (e.g. branch cut).
		{
			let tx2 = store2
				.connection
				.transaction_with_behavior(TransactionBehavior::Immediate)
				.expect("tx2 immediate");
			let payload_json: String = tx2
				.query_row(
					"SELECT payload_json FROM sessions WHERE id = 's_race'",
					[],
					|row| row.get(0),
				)
				.expect("query payload");
			let mut payload: Value = decode_json(&payload_json).unwrap();
			payload.as_object_mut().unwrap().remove("durableProjection");
			let encoded = serde_json::to_string(&payload).unwrap();
			tx2.execute(
				"UPDATE sessions SET payload_json = ?1 WHERE id = 's_race'",
				params![encoded],
			)
			.expect("tx2 update");
			tx2.commit().expect("tx2 commit");
		}

		// Now connection 1 runs reconcile_durable_projection.
		// Because reconcile_durable_projection reads the session INSIDE its own IMMEDIATE transaction,
		// it sees that durableProjection was already cleared by connection 2.
		// It returns Ok(0) and DOES NOT RESURRECT the cleared projection!
		let backfilled = store1.reconcile_durable_projection("s_race").expect("reconcile");
		assert_eq!(backfilled, 0, "must not backfill into a concurrently cleared projection");

		let session = store1.session("s_race").expect("get").expect("s");
		assert!(
			session.payload.get("durableProjection").is_none(),
			"projection must remain None, not resurrected by stale snapshot"
		);

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn reconcile_durable_projection_locks_against_concurrent_immediate_write() {
		let (dir, path) = temp_db("concurrent-lock");
		let mut store1 = SqliteStore::open(&path).expect("open store 1");
		let mut store2 = SqliteStore::open(&path).expect("open store 2");

		store1
			.create_session(NewSession {
				id: "s_lock".to_string(),
				parent_session_id: None,
				payload: json!({
					"durableProjection": {
						"kept": ["m1"]
					}
				}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		// When store1 holds an open IMMEDIATE transaction, store2 attempting to begin an IMMEDIATE transaction
		// must fail immediately due to SQLite write lock contention!
		let tx1 = store1
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)
			.expect("tx1");

		let tx2_err = store2
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate);
		assert!(tx2_err.is_err(), "concurrent immediate transaction must be blocked by SQLite write lock");

		drop(tx1);
		let _ = std::fs::remove_dir_all(dir);
	}
}
