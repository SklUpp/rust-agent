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
	collections::HashSet,
	path::{Path, PathBuf},
	sync::atomic::{AtomicU64, Ordering},
	time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{
	Connection, OptionalExtension as _, Transaction, TransactionBehavior, params,
};
use serde_json::Value;

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

/// Worker completion and the parent notification that must commit with it.
/// `worker_id` is the durable worker identity, not a reusable display index.
/// `delivery_id` is caller-owned and must stay stable across retries and
/// process restarts (for example the spawn-time result key); it is the
/// deduplication identity.
#[derive(Debug, Clone)]
pub struct WorkerCompletion {
	pub worker_id: String,
	pub delivery_id: String,
	pub session_id: String,
	pub origin_branch_id: String,
	pub completion: Value,
	pub delivery: Value,
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
		let encoded = encode_json(&outcome)?;
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
			if existing == outcome {
				transaction.commit()?;
				return Ok(());
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
		transaction.commit()?;
		Ok(())
	}

	/// Read a terminal outcome, or `None` when the execution is unresolved or
	/// unknown.
	pub fn outcome(&self, execution_id: &str) -> StoreResult<Option<Value>> {
		let row: Option<Option<String>> = self
			.connection
			.query_row(
				"SELECT outcome_json FROM executions WHERE execution_id = ?1",
				params![execution_id],
				|row| row.get(0),
			)
			.optional()?;
		match row.flatten() {
			Some(json) => Ok(Some(decode_json(&json)?)),
			None => Ok(None),
		}
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
}
