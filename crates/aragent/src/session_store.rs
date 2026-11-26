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
//! - [`SqliteStore::recover_unresolved_intents`] may append an interrupted
//!   knowledge `toolResult` for trailing active-branch calls only; it never
//!   writes `outcome_json` and never retries the effect. Exclusive cooperative
//!   writer open invokes it once after the session lease is held; plain
//!   load/reload paths must not.
//! - [`SqliteStore::complete_worker`] commits the worker completion and the
//!   parent delivery atomically. Synchronous completions are marked delivered
//!   and processed immediately; background completions remain pending.
//! - [`SqliteStore::consume_delivery`] atomically appends the delivery message
//!   to the active branch and records `delivered_at_ms` and `message_id` under
//!   an immediate write lock. Replay is idempotent; legacy rows without message_id
//!   fail closed.
//! - `delivered_at_ms` and `processed_at_ms` are distinct states. Delivery is
//!   at-least-once; marking processed is not implied by handing it to a context.

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	path::{Path, PathBuf},
	sync::atomic::{AtomicU64, Ordering},
	time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{
	Connection, OpenFlags, OptionalExtension as _, Transaction, TransactionBehavior, params,
	params_from_iter,
};
use serde_json::{Value, json};

use crate::{
	auth::error::{
		redact_free_text, redact_json_strings, redact_message_payload, redact_session_payload,
	},
	durable_session::StoredProjection,
};

/// Current schema version. Bump only with a new branch in [`SqliteStore::initialize`].
pub const SCHEMA_VERSION: i64 = 7;
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
		processed_at_ms INTEGER,
		message_id TEXT REFERENCES messages(id)
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
	CREATE UNIQUE INDEX deliveries_message_id_idx ON deliveries(message_id) WHERE message_id IS NOT NULL;

	CREATE TRIGGER executions_outcome_immutable
	BEFORE UPDATE OF outcome_json ON executions
	WHEN OLD.outcome_json IS NOT NULL
	BEGIN
		SELECT RAISE(ABORT, 'execution outcome is terminal');
	END;
";

const SCHEMA_V2_MIGRATION_DDL: &str = "
	ALTER TABLE deliveries ADD COLUMN message_id TEXT REFERENCES messages(id);
	CREATE UNIQUE INDEX IF NOT EXISTS deliveries_message_id_idx ON deliveries(message_id) WHERE message_id IS NOT NULL;
";

/// Branch-scoped draft of an in-progress assistant stream. At most one row per
/// branch; never replayed as executable history. The completed message clears
/// it in the same transaction that appends the message.
const SCHEMA_V4_MIGRATION_DDL: &str = "
	CREATE TABLE IF NOT EXISTS stream_drafts (
		branch_id TEXT PRIMARY KEY REFERENCES branches(id) ON DELETE CASCADE,
		session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
		payload_json TEXT NOT NULL,
		updated_at_ms INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS stream_drafts_session_idx ON stream_drafts(session_id);
";

/// Admission rows for child tasks. The row is written before the process/task
/// side effect, so a crash can never leave work that has no durable record.
/// `state` is the last known phase: `admitted` means the task may not have
/// started, `started` means the side effect was requested, and the terminal
/// states are written when the task finishes.
const SCHEMA_V5_MIGRATION_DDL: &str = "
	CREATE TABLE IF NOT EXISTS task_admissions (
		task_id TEXT PRIMARY KEY,
		parent_session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
		parent_branch_id TEXT NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
		worker_id TEXT NOT NULL,
		producer_generation INTEGER NOT NULL CHECK (producer_generation >= 0),
		state TEXT NOT NULL CHECK (state IN ('admitted', 'started', 'completed', 'failed', 'abandoned')),
		spec_json TEXT NOT NULL,
		created_at_ms INTEGER NOT NULL,
		updated_at_ms INTEGER NOT NULL
	);
	CREATE UNIQUE INDEX IF NOT EXISTS task_admissions_worker_idx ON task_admissions(worker_id, producer_generation);
	CREATE INDEX IF NOT EXISTS task_admissions_open_idx ON task_admissions(parent_session_id, state);
";

/// v5 shipped a single-column `UNIQUE(worker_id)` index, which permanently
/// blocked reusing `task-N` in a later producer generation. Rebuild the index
/// under the same name; generation epochs keep live spawns single-flight.
const SCHEMA_V6_MIGRATION_DDL: &str = "
	DROP INDEX IF EXISTS task_admissions_worker_idx;
	CREATE UNIQUE INDEX task_admissions_worker_idx ON task_admissions(worker_id, producer_generation);
";

/// v7 records the producer epoch that last acquired writer ownership of a
/// session. A persisted child contract stores the epoch observed at spawn; the
/// revive path refuses a contract whose epoch the store has never recorded.
/// Nullable: sessions written before v7 and sessions with no admission keep
/// no epoch, and only durable children ever read it.
const SCHEMA_V7_MIGRATION_DDL: &str = "
	ALTER TABLE sessions ADD COLUMN producer_generation INTEGER \
	 CHECK (producer_generation IS NULL OR producer_generation >= 0);
";

/// SQLite has no `ADD COLUMN IF NOT EXISTS`, so the column is probed first.
/// The probe keeps a retried migration idempotent even though the surrounding
/// transaction already makes a failed attempt roll back.
fn migrate_producer_generation(transaction: &rusqlite::Transaction<'_>) -> StoreResult<()> {
	let has_column: bool = transaction.query_row(
		"SELECT EXISTS(SELECT 1 FROM pragma_table_info('sessions') \
		 WHERE name = 'producer_generation')",
		[],
		|row| row.get(0),
	)?;
	if !has_column {
		transaction.execute_batch(SCHEMA_V7_MIGRATION_DDL)?;
	}
	Ok(())
}

/// Immutable fork-only classification of a copied tool-result message. Not an
/// intent: rows here never appear in `executions` and can never be recovered as
/// executable work. `outcome_resolved` is fixed at fork time; UPDATE is blocked
/// by a trigger. Deletion follows the session/message lifecycle through
/// `ON DELETE CASCADE`: provenance is derived fork state, not a tombstone, so a
/// legitimately deleted session does not leave orphan rows and is not blocked.
const SCHEMA_V3_MIGRATION_DDL: &str = "
	CREATE TABLE IF NOT EXISTS fork_provenance (
		message_id TEXT PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
		session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
		source_execution_id TEXT NOT NULL,
		provider_call_id TEXT NOT NULL,
		outcome_resolved INTEGER NOT NULL CHECK (outcome_resolved IN (0, 1))
	);
	CREATE INDEX IF NOT EXISTS fork_provenance_session_idx ON fork_provenance(session_id);
	CREATE TRIGGER IF NOT EXISTS fork_provenance_immutable
	BEFORE UPDATE ON fork_provenance
	BEGIN
		SELECT RAISE(ABORT, 'fork provenance is immutable');
	END;
";

/// Store failure. Every variant is returned to the caller; nothing is logged
/// and ignored.
#[derive(Debug)]
pub enum StoreError {
	/// SQLite rejected the operation.
	Sqlite(rusqlite::Error),
	/// Another connection holds the SQLite write lock (`SQLITE_BUSY`,
	/// `SQLITE_BUSY_SNAPSHOT`) or a conflicting lock (`SQLITE_LOCKED`). The
	/// operation did not commit; a caller may retry the whole operation later.
	/// The store itself never retries.
	Busy(rusqlite::Error),
	/// A write was enqueued but its outcome was never observed: the awaiting
	/// caller was cancelled or timed out, or the writer stopped before
	/// reporting. It may have committed; callers must never retry it
	/// automatically.
	UnknownOutcome(String),
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
			Self::Busy(err) => write!(f, "session store busy: {err}"),
			Self::UnknownOutcome(err) => write!(f, "session store outcome unknown: {err}"),
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
			Self::Busy(err) => Some(err),
			Self::Json(err) => Some(err),
			Self::Io(err) => Some(err),
			Self::UnknownOutcome(_)
			| Self::Validation(_)
			| Self::Conflict(_)
			| Self::NotFound(_)
			| Self::Corrupt(_)
			| Self::Schema(_) => None,
		}
	}
}

impl StoreError {
	/// True when SQLite refused the write because another connection holds the
	/// lock. Contention is retryable by the caller; the store never retries.
	#[must_use]
	pub fn is_busy(&self) -> bool {
		matches!(self, Self::Busy(_))
	}

	/// True when a write was enqueued but its outcome was never observed. It
	/// may have committed; never retry it automatically.
	#[must_use]
	pub fn is_unknown_outcome(&self) -> bool {
		matches!(self, Self::UnknownOutcome(_))
	}
}

impl From<rusqlite::Error> for StoreError {
	fn from(err: rusqlite::Error) -> Self {
		// `SQLITE_BUSY_SNAPSHOT` shares the primary `SQLITE_BUSY` code, so both
		// are classified here. Contention is not corruption: the operation did
		// not commit and the caller may retry it later.
		let busy = match &err {
			rusqlite::Error::SqliteFailure(inner, _) => matches!(
				inner.code,
				rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
			),
			_ => false,
		};
		if busy {
			Self::Busy(err)
		} else {
			Self::Sqlite(err)
		}
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

/// Portable session export format marker.
pub const SESSION_EXPORT_FORMAT: &str = "aragent-session-export";
/// Portable session export layout version; independent of [`SCHEMA_VERSION`].
pub const SESSION_EXPORT_VERSION: u32 = 1;

/// Session-scoped tables carried by a portable export and the column that
/// scopes each row to the exported session. Order is insertion order at
/// import: parents before children.
///
/// `artifacts` is deliberately absent: D55 exports history and metadata only,
/// and artifact files/workspace state are not part of the portable document.
/// The credential store is a separate database and is never opened here.
const SESSION_EXPORT_TABLES: &[(&str, &str)] = &[
	("sessions", "id"),
	("branches", "session_id"),
	("messages", "session_id"),
	("executions", "session_id"),
	("deliveries", "session_id"),
	("worker_completions", "session_id"),
	("stream_drafts", "session_id"),
	("task_admissions", "parent_session_id"),
	("fork_provenance", "session_id"),
];

/// Versioned, portable snapshot of one session: exact SQLite column values for
/// every history/metadata table row owned by the session.
///
/// TEXT payload columns stay strings, so import never decodes or executes
/// saved tool calls. Credentials are not present because the session store is
/// a separate database from the credential store.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionExport {
	pub format: String,
	pub version: u32,
	pub schema_version: i64,
	pub exported_at_ms: i64,
	pub session_id: String,
	/// Table name to rows. Each row maps every column of the live schema to its
	/// exact value; BLOB values use a `{"$blob": [bytes]}` marker.
	pub tables: BTreeMap<String, Vec<Value>>,
}

/// Portable session bundle format marker (R6/D55). The bundle carries history
/// and metadata only; workspace files, checkpoints, artifact bodies, and
/// credentials are never packaged.
pub const SESSION_BUNDLE_FORMAT: &str = "aragent.session.v1";

/// One exported `sessions` row, including `producer_generation` which
/// [`SessionRecord`] deliberately does not expose to live callers.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionBundleSession {
	pub id: String,
	#[serde(default)]
	pub parent_session_id: Option<String>,
	#[serde(default)]
	pub active_branch_id: Option<String>,
	pub payload: Value,
	pub created_at_ms: i64,
	pub updated_at_ms: i64,
	#[serde(default)]
	pub producer_generation: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionBundleBranch {
	pub id: String,
	pub session_id: String,
	#[serde(default)]
	pub parent_branch_id: Option<String>,
	#[serde(default)]
	pub parent_message_id: Option<String>,
	#[serde(default)]
	pub leaf_message_id: Option<String>,
	pub payload: Value,
	pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionBundleMessage {
	pub seq: i64,
	pub id: String,
	pub session_id: String,
	pub branch_id: String,
	#[serde(default)]
	pub parent_message_id: Option<String>,
	pub payload: Value,
	pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionBundleExecution {
	pub seq: i64,
	pub execution_id: String,
	pub session_id: String,
	pub branch_id: String,
	pub provider_call_id: String,
	pub tool_name: String,
	pub intent: Value,
	pub created_at_ms: i64,
	#[serde(default)]
	pub outcome: Option<Value>,
	#[serde(default)]
	pub outcome_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionBundleDelivery {
	pub delivery_id: String,
	pub worker_id: String,
	pub session_id: String,
	pub origin_branch_id: String,
	pub payload: Value,
	pub created_at_ms: i64,
	#[serde(default)]
	pub delivered_at_ms: Option<i64>,
	#[serde(default)]
	pub processed_at_ms: Option<i64>,
	#[serde(default)]
	pub message_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionBundleWorkerCompletion {
	pub worker_id: String,
	pub session_id: String,
	pub origin_branch_id: String,
	pub delivery_id: String,
	pub completion: Value,
	pub completed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionBundleArtifact {
	pub artifact_id: String,
	pub session_id: String,
	#[serde(default)]
	pub branch_id: Option<String>,
	#[serde(default)]
	pub execution_id: Option<String>,
	pub location: String,
	pub metadata: Value,
	pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionBundleForkProvenance {
	pub message_id: String,
	pub session_id: String,
	pub source_execution_id: String,
	pub provider_call_id: String,
	pub outcome_resolved: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionBundleTaskAdmission {
	pub task_id: String,
	pub parent_session_id: String,
	pub parent_branch_id: String,
	pub worker_id: String,
	pub producer_generation: i64,
	pub state: String,
	pub spec: Value,
	pub created_at_ms: i64,
	pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionBundleStreamDraft {
	pub branch_id: String,
	pub session_id: String,
	pub payload: Value,
	pub updated_at_ms: i64,
}

/// Versioned, portable, redacted snapshot of one session across every
/// session-scoped v7 table. Payload values are opaque strings/JSON; import
/// never executes recorded calls.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionBundle {
	pub format: String,
	pub schema_version: i64,
	pub exported_at_ms: i64,
	pub session: SessionBundleSession,
	#[serde(default)]
	pub branches: Vec<SessionBundleBranch>,
	#[serde(default)]
	pub messages: Vec<SessionBundleMessage>,
	#[serde(default)]
	pub executions: Vec<SessionBundleExecution>,
	#[serde(default)]
	pub deliveries: Vec<SessionBundleDelivery>,
	#[serde(default)]
	pub worker_completions: Vec<SessionBundleWorkerCompletion>,
	#[serde(default)]
	pub artifacts: Vec<SessionBundleArtifact>,
	#[serde(default)]
	pub fork_provenance: Vec<SessionBundleForkProvenance>,
	#[serde(default)]
	pub task_admissions: Vec<SessionBundleTaskAdmission>,
	#[serde(default)]
	pub stream_drafts: Vec<SessionBundleStreamDraft>,
}

/// Counts written by one validated bundle import.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ImportBundleReport {
	pub session_id: String,
	pub detached: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub detached_parent_session_id: Option<String>,
	pub branches: usize,
	pub messages: usize,
	pub executions: usize,
	pub deliveries: usize,
	pub worker_completions: usize,
	pub artifacts: usize,
	pub fork_provenance: usize,
	pub task_admissions: usize,
	pub stream_drafts: usize,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactSummary {
	pub artifact_id: String,
	pub location: String,
	pub exists_on_disk: bool,
}

/// D74 token estimate. Deliberately separate from provider-reported usage:
/// these are local estimates, not measured token counts.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct ContextTokenEstimate {
	#[serde(default)]
	pub system_prompt: usize,
	#[serde(default)]
	pub user: usize,
	#[serde(default)]
	pub assistant: usize,
	#[serde(default)]
	pub tool_calls_and_results: usize,
	#[serde(default)]
	pub thinking: usize,
	#[serde(default)]
	pub total: usize,
}

/// Provider-reported token totals, summed from stored message usage. `None`
/// means no provider usage was recorded; never zero-filled.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct ProviderUsageTotals {
	#[serde(default)]
	pub input: u64,
	#[serde(default)]
	pub output: u64,
	#[serde(default)]
	pub cache_read: u64,
	#[serde(default)]
	pub cache_write: u64,
	#[serde(default)]
	pub total_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionInspectorReport {
	pub id: String,
	#[serde(default)]
	pub parent_session_id: Option<String>,
	#[serde(default)]
	pub active_branch_id: Option<String>,
	#[serde(default)]
	pub producer_generation: Option<i64>,
	pub created_at_ms: i64,
	pub updated_at_ms: i64,
	#[serde(default)]
	pub cwd: Option<String>,
	#[serde(default)]
	pub title: Option<String>,
	pub branches: usize,
	pub messages: usize,
	pub active_branch_messages: usize,
	pub executions: usize,
	pub unresolved_executions: usize,
	pub deliveries: usize,
	pub pending_deliveries: usize,
	pub worker_completions: usize,
	pub artifacts: usize,
	pub missing_artifacts_on_disk: usize,
	pub fork_provenance: usize,
	pub task_admissions: usize,
	pub stream_drafts: usize,
	pub artifact_summaries: Vec<ArtifactSummary>,
	pub context_estimate: ContextTokenEstimate,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub provider_usage: Option<ProviderUsageTotals>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseSessionSummary {
	pub id: String,
	#[serde(default)]
	pub parent_session_id: Option<String>,
	#[serde(default)]
	pub active_branch_id: Option<String>,
	#[serde(default)]
	pub title: Option<String>,
	pub created_at_ms: i64,
	pub updated_at_ms: i64,
	pub branches: i64,
	pub messages: i64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseInspectorReport {
	pub schema_version: i64,
	pub table_counts: BTreeMap<String, i64>,
	pub sessions: Vec<DatabaseSessionSummary>,
}

/// Convert one SQLite row into the export JSON object, preserving the exact
/// column value and type.
fn session_export_row_json(row: &rusqlite::Row<'_>, columns: &[String]) -> StoreResult<Value> {
	let mut object = serde_json::Map::with_capacity(columns.len());
	for (index, column) in columns.iter().enumerate() {
		let value = match row.get_ref(index)? {
			rusqlite::types::ValueRef::Null => Value::Null,
			rusqlite::types::ValueRef::Integer(value) => Value::from(value),
			rusqlite::types::ValueRef::Real(value) => Value::from(value),
			rusqlite::types::ValueRef::Text(bytes) => {
				Value::from(std::str::from_utf8(bytes).map_err(|error| {
					StoreError::Corrupt(format!("export column {column} is not UTF-8: {error}"))
				})?)
			},
			rusqlite::types::ValueRef::Blob(bytes) => json!({ "$blob": bytes }),
		};
		object.insert(column.clone(), value);
	}
	Ok(Value::Object(object))
}

/// Decode one export value back into a SQLite bind value.
fn session_export_value_from_json(
	table: &str,
	column: &str,
	value: &Value,
) -> StoreResult<rusqlite::types::Value> {
	use rusqlite::types::Value as SqlValue;
	fn blob(bytes: &[Value], table: &str, column: &str) -> StoreResult<SqlValue> {
		let mut decoded = Vec::with_capacity(bytes.len());
		for byte in bytes {
			let byte = byte
				.as_u64()
				.filter(|byte| *byte <= u64::from(u8::MAX))
				.ok_or_else(|| {
					StoreError::Validation(format!("{table}.{column} blob entry is not a byte"))
				})?;
			decoded.push(byte as u8);
		}
		Ok(SqlValue::Blob(decoded))
	}
	match value {
		Value::Null => Ok(SqlValue::Null),
		Value::Bool(flag) => Ok(SqlValue::Integer(i64::from(*flag))),
		Value::Number(number) => number
			.as_i64()
			.map(SqlValue::Integer)
			.or_else(|| number.as_f64().map(SqlValue::Real))
			.ok_or_else(|| StoreError::Validation(format!("{table}.{column} number is out of range"))),
		Value::String(text) => Ok(SqlValue::Text(text.clone())),
		Value::Array(bytes) => blob(bytes, table, column),
		Value::Object(map) => match map.get("$blob") {
			Some(Value::Array(bytes)) if map.len() == 1 => blob(bytes, table, column),
			_ => {
				Err(StoreError::Validation(format!("{table}.{column} is not a supported export value")))
			},
		},
	}
}

/// Live column order for one exported table. The table name is a compile-time
/// allowlist entry, never caller input.
fn session_export_table_columns(connection: &Connection, table: &str) -> StoreResult<Vec<String>> {
	let statement = connection.prepare(&format!("SELECT * FROM {table} WHERE 0"))?;
	Ok(statement
		.column_names()
		.iter()
		.map(|name| (*name).to_string())
		.collect())
}

fn redact_bundle_string(value: &mut String) {
	let redacted = redact_free_text(value);
	if redacted != *value {
		*value = redacted;
	}
}

fn redact_bundle_optional(value: &mut Option<String>) {
	if let Some(text) = value.as_mut() {
		redact_bundle_string(text);
	}
}

fn redact_bundle_payload(payload: &mut Value) {
	redact_json_strings(payload);
}

fn redact_bundle_session_payload(payload: &mut Value) {
	redact_session_payload(payload);
	// The schema-aware pass preserves structural ids by design; the generic
	// pass then masks any remaining string, including ids, so no opaque
	// credential can hide there.
	redact_json_strings(payload);
}

fn redact_bundle_message_payload(payload: &mut Value) {
	redact_message_payload(payload);
	// The schema-aware pass preserves structural ids by design; the generic
	// pass then masks any remaining string, including ids, so no opaque
	// credential can hide there.
	redact_json_strings(payload);
}

/// D55/D27: every string and JSON payload leaving the store through a bundle is
/// redacted. References are redacted with the same deterministic function, so
/// a valid bundle stays internally consistent.
fn redact_session_bundle(bundle: &mut SessionBundle) {
	redact_bundle_string(&mut bundle.session.id);
	redact_bundle_optional(&mut bundle.session.parent_session_id);
	redact_bundle_optional(&mut bundle.session.active_branch_id);
	redact_bundle_session_payload(&mut bundle.session.payload);
	for branch in &mut bundle.branches {
		redact_bundle_string(&mut branch.id);
		redact_bundle_string(&mut branch.session_id);
		redact_bundle_optional(&mut branch.parent_branch_id);
		redact_bundle_optional(&mut branch.parent_message_id);
		redact_bundle_optional(&mut branch.leaf_message_id);
		redact_bundle_payload(&mut branch.payload);
	}
	for message in &mut bundle.messages {
		redact_bundle_string(&mut message.id);
		redact_bundle_string(&mut message.session_id);
		redact_bundle_string(&mut message.branch_id);
		redact_bundle_optional(&mut message.parent_message_id);
		redact_bundle_message_payload(&mut message.payload);
	}
	for execution in &mut bundle.executions {
		redact_bundle_string(&mut execution.execution_id);
		redact_bundle_string(&mut execution.session_id);
		redact_bundle_string(&mut execution.branch_id);
		redact_bundle_string(&mut execution.provider_call_id);
		redact_bundle_string(&mut execution.tool_name);
		redact_bundle_payload(&mut execution.intent);
		if let Some(outcome) = execution.outcome.as_mut() {
			redact_bundle_payload(outcome);
		}
	}
	for delivery in &mut bundle.deliveries {
		redact_bundle_string(&mut delivery.delivery_id);
		redact_bundle_string(&mut delivery.worker_id);
		redact_bundle_string(&mut delivery.session_id);
		redact_bundle_string(&mut delivery.origin_branch_id);
		redact_bundle_optional(&mut delivery.message_id);
		redact_bundle_payload(&mut delivery.payload);
	}
	for completion in &mut bundle.worker_completions {
		redact_bundle_string(&mut completion.worker_id);
		redact_bundle_string(&mut completion.session_id);
		redact_bundle_string(&mut completion.origin_branch_id);
		redact_bundle_string(&mut completion.delivery_id);
		redact_bundle_payload(&mut completion.completion);
	}
	for artifact in &mut bundle.artifacts {
		redact_bundle_string(&mut artifact.artifact_id);
		redact_bundle_string(&mut artifact.session_id);
		redact_bundle_optional(&mut artifact.branch_id);
		redact_bundle_optional(&mut artifact.execution_id);
		redact_bundle_string(&mut artifact.location);
		redact_bundle_payload(&mut artifact.metadata);
	}
	for provenance in &mut bundle.fork_provenance {
		redact_bundle_string(&mut provenance.message_id);
		redact_bundle_string(&mut provenance.session_id);
		redact_bundle_string(&mut provenance.source_execution_id);
		redact_bundle_string(&mut provenance.provider_call_id);
	}
	for admission in &mut bundle.task_admissions {
		redact_bundle_string(&mut admission.task_id);
		redact_bundle_string(&mut admission.parent_session_id);
		redact_bundle_string(&mut admission.parent_branch_id);
		redact_bundle_string(&mut admission.worker_id);
		redact_bundle_string(&mut admission.state);
		redact_bundle_payload(&mut admission.spec);
	}
	for draft in &mut bundle.stream_drafts {
		redact_bundle_string(&mut draft.branch_id);
		redact_bundle_string(&mut draft.session_id);
		redact_bundle_payload(&mut draft.payload);
	}
}

fn insert_unique_bundle_id<'a>(
	set: &mut HashSet<&'a str>,
	value: &'a str,
	label: &str,
) -> StoreResult<()> {
	if value.is_empty() {
		return Err(StoreError::Validation(format!("{label} must not be empty")));
	}
	if !set.insert(value) {
		return Err(StoreError::Validation(format!("duplicate {label} {value}")));
	}
	Ok(())
}

fn require_bundle_session(
	row_session_id: &str,
	session_id: &str,
	label: &str,
) -> StoreResult<()> {
	if row_session_id != session_id {
		return Err(StoreError::Validation(format!(
			"{label} session_id {row_session_id} does not match session {session_id}"
		)));
	}
	Ok(())
}

/// Structural validation before any write. References are checked against the
/// bundle itself; journal execution history is never executed.
fn validate_session_bundle(bundle: &SessionBundle) -> StoreResult<()> {
	if bundle.format != SESSION_BUNDLE_FORMAT {
		return Err(StoreError::Validation(format!(
			"unsupported session bundle format {:?}",
			bundle.format
		)));
	}
	if bundle.schema_version < 1 || bundle.schema_version > SCHEMA_VERSION {
		return Err(StoreError::Validation(format!(
			"session bundle schema v{} is outside supported range 1..={SCHEMA_VERSION}",
			bundle.schema_version
		)));
	}
	let session = &bundle.session;
	if !crate::session::valid_session_id(&session.id) {
		return Err(StoreError::Validation(format!(
			"invalid session bundle session id `{}`: allowed characters are [A-Za-z0-9._-] and the id may not be `.`/`..`",
			session.id
		)));
	}
	if session.parent_session_id.as_deref() == Some(session.id.as_str()) {
		return Err(StoreError::Validation("a session cannot be its own parent".into()));
	}
	if let Some(generation) = session.producer_generation
		&& generation < 0
	{
		return Err(StoreError::Validation(
			"session producer_generation must be non-negative".into(),
		));
	}

	let mut branch_ids: HashSet<&str> = HashSet::new();
	for branch in &bundle.branches {
		insert_unique_bundle_id(&mut branch_ids, &branch.id, "branch id")?;
		require_bundle_session(&branch.session_id, &session.id, "branch")?;
	}
	let mut message_ids: HashSet<&str> = HashSet::new();
	for message in &bundle.messages {
		insert_unique_bundle_id(&mut message_ids, &message.id, "message id")?;
		require_bundle_session(&message.session_id, &session.id, "message")?;
	}
	let mut execution_ids: HashSet<&str> = HashSet::new();
	for execution in &bundle.executions {
		insert_unique_bundle_id(&mut execution_ids, &execution.execution_id, "execution id")?;
		require_bundle_session(&execution.session_id, &session.id, "execution")?;
	}
	let mut delivery_ids: HashSet<&str> = HashSet::new();
	for delivery in &bundle.deliveries {
		insert_unique_bundle_id(&mut delivery_ids, &delivery.delivery_id, "delivery id")?;
		require_bundle_session(&delivery.session_id, &session.id, "delivery")?;
	}
	let mut worker_ids: HashSet<&str> = HashSet::new();
	for completion in &bundle.worker_completions {
		insert_unique_bundle_id(&mut worker_ids, &completion.worker_id, "worker completion id")?;
		require_bundle_session(&completion.session_id, &session.id, "worker completion")?;
	}
	let mut artifact_ids: HashSet<&str> = HashSet::new();
	for artifact in &bundle.artifacts {
		insert_unique_bundle_id(&mut artifact_ids, &artifact.artifact_id, "artifact id")?;
		require_bundle_session(&artifact.session_id, &session.id, "artifact")?;
		if artifact.location.is_empty() {
			return Err(StoreError::Validation("artifact location must not be empty".into()));
		}
	}
	let mut provenance_ids: HashSet<&str> = HashSet::new();
	for provenance in &bundle.fork_provenance {
		insert_unique_bundle_id(&mut provenance_ids, &provenance.message_id, "fork provenance message id")?;
		require_bundle_session(&provenance.session_id, &session.id, "fork provenance")?;
	}
	let mut task_ids: HashSet<&str> = HashSet::new();
	for admission in &bundle.task_admissions {
		insert_unique_bundle_id(&mut task_ids, &admission.task_id, "task admission id")?;
		require_bundle_session(&admission.parent_session_id, &session.id, "task admission")?;
		if admission.producer_generation < 0 {
			return Err(StoreError::Validation(
				"task admission producer_generation must be non-negative".into(),
			));
		}
		if !matches!(
			admission.state.as_str(),
			"admitted" | "started" | "completed" | "failed" | "abandoned"
		) {
			return Err(StoreError::Validation(format!(
				"task admission {} has invalid state {:?}",
				admission.task_id, admission.state
			)));
		}
	}
	let mut draft_branch_ids: HashSet<&str> = HashSet::new();
	for draft in &bundle.stream_drafts {
		insert_unique_bundle_id(&mut draft_branch_ids, &draft.branch_id, "stream draft branch id")?;
		require_bundle_session(&draft.session_id, &session.id, "stream draft")?;
	}

	let active_branch_id = session.active_branch_id.as_deref().ok_or_else(|| {
		StoreError::Validation("session bundle active_branch_id is required".into())
	})?;
	if !branch_ids.contains(active_branch_id) {
		return Err(StoreError::Validation(format!(
			"active branch {active_branch_id} is not present in the bundle"
		)));
	}
	for branch in &bundle.branches {
		if let Some(parent) = branch.parent_branch_id.as_deref() {
			if parent == branch.id {
				return Err(StoreError::Validation(format!(
					"branch {} cannot parent itself",
					branch.id
				)));
			}
			if !branch_ids.contains(parent) {
				return Err(StoreError::Validation(format!(
					"branch {} references missing parent branch {parent}",
					branch.id
				)));
			}
		}
		if let Some(message) = branch.parent_message_id.as_deref()
			&& !message_ids.contains(message)
		{
			return Err(StoreError::Validation(format!(
				"branch {} references missing parent message {message}",
				branch.id
			)));
		}
		if let Some(message) = branch.leaf_message_id.as_deref()
			&& !message_ids.contains(message)
		{
			return Err(StoreError::Validation(format!(
				"branch {} references missing leaf message {message}",
				branch.id
			)));
		}
	}
	for message in &bundle.messages {
		if !branch_ids.contains(message.branch_id.as_str()) {
			return Err(StoreError::Validation(format!(
				"message {} references missing branch {}",
				message.id, message.branch_id
			)));
		}
		if let Some(parent) = message.parent_message_id.as_deref() {
			if parent == message.id {
				return Err(StoreError::Validation(format!(
					"message {} cannot parent itself",
					message.id
				)));
			}
			if !message_ids.contains(parent) {
				return Err(StoreError::Validation(format!(
					"message {} references missing parent message {parent}",
					message.id
				)));
			}
		}
	}
	for execution in &bundle.executions {
		if !branch_ids.contains(execution.branch_id.as_str()) {
			return Err(StoreError::Validation(format!(
				"execution {} references missing branch {}",
				execution.execution_id, execution.branch_id
			)));
		}
	}
	for delivery in &bundle.deliveries {
		if !branch_ids.contains(delivery.origin_branch_id.as_str()) {
			return Err(StoreError::Validation(format!(
				"delivery {} references missing branch {}",
				delivery.delivery_id, delivery.origin_branch_id
			)));
		}
		if let Some(message) = delivery.message_id.as_deref()
			&& !message_ids.contains(message)
		{
			return Err(StoreError::Validation(format!(
				"delivery {} references missing message {message}",
				delivery.delivery_id
			)));
		}
	}
	for completion in &bundle.worker_completions {
		if !branch_ids.contains(completion.origin_branch_id.as_str()) {
			return Err(StoreError::Validation(format!(
				"worker completion {} references missing branch {}",
				completion.worker_id, completion.origin_branch_id
			)));
		}
		if !delivery_ids.contains(completion.delivery_id.as_str()) {
			return Err(StoreError::Validation(format!(
				"worker completion {} references missing delivery {}",
				completion.worker_id, completion.delivery_id
			)));
		}
	}
	for artifact in &bundle.artifacts {
		if let Some(branch) = artifact.branch_id.as_deref()
			&& !branch_ids.contains(branch)
		{
			return Err(StoreError::Validation(format!(
				"artifact {} references missing branch {branch}",
				artifact.artifact_id
			)));
		}
		if let Some(execution) = artifact.execution_id.as_deref()
			&& !execution_ids.contains(execution)
		{
			return Err(StoreError::Validation(format!(
				"artifact {} references missing execution {execution}",
				artifact.artifact_id
			)));
		}
	}
	for provenance in &bundle.fork_provenance {
		if !message_ids.contains(provenance.message_id.as_str()) {
			return Err(StoreError::Validation(format!(
				"fork provenance references missing message {}",
				provenance.message_id
			)));
		}
	}
	for admission in &bundle.task_admissions {
		if !branch_ids.contains(admission.parent_branch_id.as_str()) {
			return Err(StoreError::Validation(format!(
				"task admission {} references missing branch {}",
				admission.task_id, admission.parent_branch_id
			)));
		}
	}
	for draft in &bundle.stream_drafts {
		if !branch_ids.contains(draft.branch_id.as_str()) {
			return Err(StoreError::Validation(format!(
				"stream draft references missing branch {}",
				draft.branch_id
			)));
		}
	}
	Ok(())
}

fn order_session_bundle_branches(
	branches: &[SessionBundleBranch],
) -> StoreResult<Vec<&SessionBundleBranch>> {
	let mut remaining: Vec<&SessionBundleBranch> = branches.iter().collect();
	let mut inserted: HashSet<&str> = HashSet::new();
	let mut ordered = Vec::with_capacity(branches.len());
	while !remaining.is_empty() {
		let before = remaining.len();
		remaining.retain(|branch| {
			let ready = branch
				.parent_branch_id
				.as_deref()
				.is_none_or(|parent| inserted.contains(parent));
			if ready {
				inserted.insert(branch.id.as_str());
				ordered.push(*branch);
			}
			ready
		});
		if remaining.len() == before {
			return Err(StoreError::Validation(
				"branch parent references form a cycle".to_string(),
			));
		}
	}
	Ok(ordered)
}

fn order_session_bundle_messages(
	messages: &[SessionBundleMessage],
) -> StoreResult<Vec<&SessionBundleMessage>> {
	let mut remaining: Vec<&SessionBundleMessage> = messages.iter().collect();
	remaining.sort_by_key(|message| message.seq);
	let mut inserted: HashSet<&str> = HashSet::new();
	let mut ordered = Vec::with_capacity(messages.len());
	while !remaining.is_empty() {
		let before = remaining.len();
		remaining.retain(|message| {
			let ready = message
				.parent_message_id
				.as_deref()
				.is_none_or(|parent| inserted.contains(parent));
			if ready {
				inserted.insert(message.id.as_str());
				ordered.push(*message);
			}
			ready
		});
		if remaining.len() == before {
			return Err(StoreError::Validation(
				"message parent references form a cycle".to_string(),
			));
		}
	}
	Ok(ordered)
}

fn record_imported_parent_session(payload: &mut Value, parent_session_id: &str) {
	if let Some(map) = payload.as_object_mut() {
		map.insert(
			"_imported_parent_session_id".to_string(),
			Value::String(parent_session_id.to_string()),
		);
		return;
	}
	let previous = std::mem::take(payload);
	*payload = json!({
		"_imported_parent_session_id": parent_session_id,
		"importedPayload": previous,
	});
}

/// Path-like artifact locations are probed without failing; URI-like locations
/// are treated as externally owned and therefore not "missing on disk".
fn artifact_local_path(location: &str) -> Option<&str> {
	if let Some(path) = location.strip_prefix("file://") {
		return Some(path);
	}
	if location.contains("://") {
		return None;
	}
	Some(location)
}

fn artifact_exists_on_disk(location: &str) -> bool {
	match artifact_local_path(location) {
		Some(path) => Path::new(path).exists(),
		None => true,
	}
}

fn count_tokens(text: &str) -> usize {
	pi_agent::Tokenizer::count_text(text)
}

/// D74 estimate over the active branch only. The result contains counts, never
/// message bodies; provider usage is accumulated separately and only when the
/// provider actually reported it.
fn estimate_branch_context(
	messages: &[MessageRecord],
	session_payload: &Value,
) -> (ContextTokenEstimate, Option<ProviderUsageTotals>) {
	let mut estimate = ContextTokenEstimate::default();
	let mut usage: Option<ProviderUsageTotals> = None;
	if let Some(text) = session_payload.get("systemPrompt").and_then(Value::as_str) {
		estimate.system_prompt = estimate.system_prompt.saturating_add(count_tokens(text));
	}
	for record in messages {
		let mut payload = record.payload.clone();
		redact_message_payload(&mut payload);
		let Ok(entry) = serde_json::from_value::<crate::session::MessageEntry>(payload.clone()) else {
			estimate.assistant =
				estimate.assistant.saturating_add(count_tokens(&payload.to_string()));
			continue;
		};
		if let Some(message_usage) = entry.message.usage.as_ref() {
			let totals = usage.get_or_insert_with(ProviderUsageTotals::default);
			totals.input = totals.input.saturating_add(message_usage.input);
			totals.output = totals.output.saturating_add(message_usage.output);
			totals.cache_read = totals.cache_read.saturating_add(message_usage.cache_read);
			totals.cache_write = totals.cache_write.saturating_add(message_usage.cache_write);
			totals.total_tokens = totals.total_tokens.saturating_add(message_usage.total_tokens);
		}
		let role = entry.message.role.as_str();
		let parts = entry.message.content.parts();
		if parts.is_empty() {
			let tokens = count_tokens(&entry.message.content.text());
			match role {
				"system" => estimate.system_prompt = estimate.system_prompt.saturating_add(tokens),
				"user" => estimate.user = estimate.user.saturating_add(tokens),
				"toolResult" => {
					estimate.tool_calls_and_results =
						estimate.tool_calls_and_results.saturating_add(tokens);
				},
				_ => estimate.assistant = estimate.assistant.saturating_add(tokens),
			}
			continue;
		}
		for part in parts {
			match part.kind.as_str() {
				"thinking" => {
					if let Some(text) = part.thinking_text() {
						estimate.thinking = estimate.thinking.saturating_add(count_tokens(text));
					}
				},
				"toolCall" => {
					let arguments = Value::Object(part.extra.clone()).to_string();
					estimate.tool_calls_and_results = estimate
						.tool_calls_and_results
						.saturating_add(count_tokens(&arguments));
				},
				"text" => {
					let tokens = count_tokens(part.text.as_deref().unwrap_or_default());
					match role {
						"system" => {
							estimate.system_prompt = estimate.system_prompt.saturating_add(tokens);
						},
						"user" => estimate.user = estimate.user.saturating_add(tokens),
						"toolResult" => {
							estimate.tool_calls_and_results =
								estimate.tool_calls_and_results.saturating_add(tokens);
						},
						_ => estimate.assistant = estimate.assistant.saturating_add(tokens),
					}
				},
				_ => {},
			}
		}
	}
	estimate.total = estimate
		.system_prompt
		.saturating_add(estimate.user)
		.saturating_add(estimate.assistant)
		.saturating_add(estimate.tool_calls_and_results)
		.saturating_add(estimate.thinking);
	(estimate, usage)
}

fn count_rows(connection: &Connection, sql: &str, session_id: &str) -> StoreResult<usize> {
	let count: i64 = connection.query_row(sql, params![session_id], |row| row.get(0))?;
	usize::try_from(count)
		.map_err(|_| StoreError::Corrupt(format!("negative inspector row count {count}")))
}

fn count_session_rows(
	connection: &Connection,
	table: &str,
	scope_column: &str,
	session_id: &str,
) -> StoreResult<usize> {
	count_rows(
		connection,
		&format!("SELECT COUNT(*) FROM {table} WHERE {scope_column} = ?1"),
		session_id,
	)
}

/// True when a column exists; used only to read databases older than the
/// current schema through `open_read_only` without migrating them.
fn session_table_has_column(
	connection: &Connection,
	table: &str,
	column: &str,
) -> StoreResult<bool> {
	let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
	let mut rows = statement.query([])?;
	while let Some(row) = rows.next()? {
		let name: String = row.get(1)?;
		if name == column {
			return Ok(true);
		}
	}
	Ok(false)
}

/// Read execution rows by id on the caller's connection so a snapshot can copy
/// provenance inside its read transaction.
fn executions_by_ids_conn(
	connection: &Connection,
	ids: &HashSet<String>,
) -> StoreResult<Vec<ExecutionRecord>> {
	let mut statement = connection.prepare(
		"SELECT seq, execution_id, session_id, branch_id, provider_call_id, tool_name, intent_json, created_at_ms, outcome_json, outcome_at_ms FROM executions WHERE execution_id = ?1",
	)?;
	let mut executions = Vec::new();
	for id in ids {
		let row = statement
			.query_row(params![id], |row| {
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
			})
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
			continue;
		};
		executions.push(ExecutionRecord {
			seq,
			execution_id,
			session_id,
			branch_id,
			provider_call_id,
			tool_name,
			intent: decode_json(&intent_json)?,
			created_at_ms,
			outcome: match outcome_json {
				Some(json) => Some(decode_json(&json)?),
				None => None,
			},
			outcome_at_ms,
		});
	}
	Ok(executions)
}

fn table_exists_conn(connection: &Connection, table: &str) -> rusqlite::Result<bool> {
	use rusqlite::OptionalExtension as _;
	connection
		.query_row(
			"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
			params![table],
			|_| Ok(()),
		)
		.optional()
		.map(|row| row.is_some())
}

fn fork_provenance_conn(
	connection: &Connection,
	session_id: &str,
) -> StoreResult<Vec<ForkProvenance>> {
	// A read-only snapshot of a v1/v2 database has no provenance table yet; the
	// fork path fails closed on missing rows instead of mutating a read-only DB.
	// A v3 database without the table is damaged and MUST NOT be treated as empty.
	if !table_exists_conn(connection, "fork_provenance")? {
		let version: Option<i64> = connection
			.query_row("SELECT version FROM schema_version WHERE id = 1", [], |row| row.get(0))
			.optional()?;
		return match version {
			// Provenance is mandatory since schema v3: an older read-only
			// snapshot legitimately has no table. Keep this pinned to 3, not
			// SCHEMA_VERSION, so future migrations cannot reclassify a
			// damaged v3 snapshot as empty provenance.
			Some(version) if version < 3 => Ok(Vec::new()),
			Some(version) => Err(StoreError::Corrupt(format!(
				"schema version {version} is missing the fork_provenance table"
			))),
			None => Err(StoreError::Corrupt(
				"schema_version row is missing but durable tables exist".to_string(),
			)),
		};
	}
	let mut statement = connection.prepare(
		"SELECT message_id, source_execution_id, provider_call_id, outcome_resolved FROM fork_provenance WHERE session_id = ?1 ORDER BY message_id",
	)?;
	let rows = statement.query_map(params![session_id], |row| {
		Ok(ForkProvenance::new(row.get(0)?, row.get(1)?, row.get(2)?, row.get::<_, i64>(3)? != 0))
	})?;
	rows
		.collect::<rusqlite::Result<Vec<_>>>()
		.map_err(StoreError::from)
}

/// SELECT-only view of one session. Built by [`SqliteStore::load_session_snapshot`]
/// without recovery or reconcile.
#[derive(Debug, Clone)]
pub struct SessionSnapshot {
	pub session_id: String,
	pub header_payload: Value,
	pub branch_id: String,
	pub leaf: Option<String>,
	pub session_messages: Vec<MessageRecord>,
	pub branch_messages: Vec<MessageRecord>,
	pub projection: Option<StoredProjection>,
	pub branches: Vec<BranchRecord>,
	/// Execution rows referenced by `session_messages`, read in the same
	/// snapshot transaction.
	pub executions: Vec<ExecutionRecord>,
	/// Immutable fork classifications keyed by fork message id, read in the same
	/// snapshot transaction.
	pub provenance: Vec<ForkProvenance>,
}

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
/// Messages MUST be supplied in global parent-before-child order, including branch-point ancestors.
/// Primitive rejects missing prior parents and does not reorder entries.
/// Complete fork payload committed as one target transaction.
#[derive(Debug, Clone)]
pub struct ForkWrite {
	pub session: NewSession,
	pub branches: Vec<NewBranch>,
	pub messages: Vec<NewMessage>,
	pub active_branch_id: String,
	pub active_leaf_message_id: Option<String>,
	pub projection: Option<StoredProjection>,
}

/// Immutable fork-only classification of a copied tool-result message. Stored
/// in `fork_provenance`, never in `executions`, so it can never be recovered as
/// executable work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkProvenance {
	pub message_id: String,
	pub source_execution_id: String,
	pub provider_call_id: String,
	pub outcome_resolved: bool,
	/// Seals struct-literal construction outside this module: classification is
	/// never caller-defined data.
	_sealed: (),
}

impl ForkProvenance {
	fn new(
		message_id: String,
		source_execution_id: String,
		provider_call_id: String,
		outcome_resolved: bool,
	) -> Self {
		Self { message_id, source_execution_id, provider_call_id, outcome_resolved, _sealed: () }
	}

	/// Capture the classification for one source message from the source snapshot,
	/// proving the association with the actual source payload: the execution row (or
	/// stored provenance) must name the same provider call id as the source tool
	/// result. Untrusted target input never supplies this association.
	pub(crate) fn capture(
		source_messages: &[MessageRecord],
		executions: &[ExecutionRecord],
		provenance: &[ForkProvenance],
		source_message_id: &str,
		target_message_id: &str,
	) -> StoreResult<Vec<Self>> {
		let source = source_messages
			.iter()
			.find(|message| message.id == source_message_id)
			.ok_or_else(|| {
				StoreError::Corrupt(format!(
					"fork provenance source message {source_message_id} is missing"
				))
			})?;
		let referenced = referenced_execution_ids(std::iter::once(&source.payload));
		if referenced.is_empty() {
			return Ok(Vec::new());
		}
		let call_id = extract_tool_result_call_id(&source.payload).ok_or_else(|| {
			StoreError::Corrupt(format!(
				"message {source_message_id} references an execution without a toolCallId"
			))
		})?;
		let mut captured = Vec::new();
		for execution_id in referenced {
			// Direct-source ownership only: an execution row owned by an older session
			// (grandparent after a shared-DB fork) is not this message's source and must
			// not override the immutable provenance captured by the parent fork.
			let owned = executions.iter().find(|execution| {
				execution.execution_id == execution_id && execution.session_id == source.session_id
			});
			let copied = provenance.iter().find(|entry| {
				entry.message_id == source_message_id && entry.source_execution_id == execution_id
			});
			let resolved = match (owned, copied) {
				(Some(owned), Some(copied)) if owned.outcome.is_some() != copied.outcome_resolved => {
					return Err(StoreError::Corrupt(format!(
						"fork provenance conflicts with local execution {execution_id} for message {source_message_id}"
					)));
				},
				(Some(owned), _) => owned.outcome.is_some(),
				(None, Some(copied)) => copied.outcome_resolved,
				(None, None) => {
					return Err(StoreError::Corrupt(format!(
						"messages reference unknown execution(s): {execution_id}"
					)));
				},
			};
			if let Some(owned) = owned {
				if owned.provider_call_id != call_id {
					return Err(StoreError::Corrupt(format!(
						"execution {execution_id} provider call id does not match tool result {call_id}"
					)));
				}
			} else if let Some(copied) = copied {
				if copied.provider_call_id != call_id {
					return Err(StoreError::Corrupt(format!(
						"fork provenance for message {source_message_id} names provider call id {} but the tool result is {call_id}",
						copied.provider_call_id
					)));
				}
			}
			captured.push(Self::new(
				target_message_id.to_string(),
				execution_id,
				call_id.clone(),
				resolved,
			));
		}
		Ok(captured)
	}
}

impl SqliteStore {
	/// Insert fork session, branch, mapped messages, and projection atomically.
	/// Classification is derived from local execution rows; a referenced
	/// execution with no local row is rejected because this public entry point
	/// cannot supply trusted transitive provenance.
	pub fn create_fork_atomic(&mut self, fork: ForkWrite) -> StoreResult<()> {
		self.create_fork_atomic_with_provenance(fork, &[])
	}

	/// Crate-internal fork write that may carry provenance captured atomically with
	/// the source snapshot. Supplied classification is authoritative and is never
	/// re-read or re-derived against mutable execution rows; every referenced
	/// execution must have an exact (message, execution) captured entry.
	pub(crate) fn create_fork_atomic_with_provenance(
		&mut self,
		fork: ForkWrite,
		provenance: &[ForkProvenance],
	) -> StoreResult<()> {
		validate_id("fork session id", &fork.session.id)?;
		validate_id("fork active branch id", &fork.active_branch_id)?;
		if fork.session.parent_session_id.as_deref() == Some(fork.session.id.as_str()) {
			return Err(StoreError::Validation("fork session cannot parent itself".into()));
		}
		if fork.session.initial_branch_id != fork.active_branch_id
			|| fork.branches.is_empty()
			|| !fork.branches.iter().any(|b| b.id == fork.active_branch_id)
		{
			return Err(StoreError::Validation("fork active branch is invalid".into()));
		}
		let now = now_ms();
		let tx = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		let mut payload = fork.session.payload.clone();
		crate::auth::error::redact_session_payload(&mut payload);
		let header: crate::session::SessionHeader = serde_json::from_value(payload.clone())
			.map_err(|e| StoreError::Corrupt(format!("invalid fork session header: {e}")))?;
		if header.id != fork.session.id || header.parent_session != fork.session.parent_session_id {
			return Err(StoreError::Validation("fork header identity or parent mismatch".into()));
		}
		if let Some(projection) = &fork.projection {
			let mut proj_val = serde_json::to_value(projection)?;
			crate::auth::error::redact_stored_projection_value(&mut proj_val);
			payload["durableProjection"] = proj_val;
		} else if let Some(object) = payload.as_object_mut() {
			object.remove("durableProjection");
		}
		let relational_parent: Option<String> = match fork.session.parent_session_id.as_deref() {
			Some(id) => tx
				.query_row("SELECT id FROM sessions WHERE id = ?1", params![id], |r| {
					r.get::<_, String>(0)
				})
				.optional()?,
			None => None,
		};
		tx.execute("INSERT INTO sessions (id,parent_session_id,active_branch_id,payload_json,created_at_ms,updated_at_ms) VALUES (?1,?2,NULL,?3,?4,?4)", params![fork.session.id, relational_parent, encode_json(&payload)?, now])?;
		let branch_ids: std::collections::HashSet<&str> =
			fork.branches.iter().map(|b| b.id.as_str()).collect();
		if branch_ids.len() != fork.branches.len() {
			return Err(StoreError::Validation("duplicate fork branch id".into()));
		}
		let mut pending = fork.branches.clone();
		let mut inserted = std::collections::HashSet::new();
		while !pending.is_empty() {
			let before = pending.len();
			let mut next = Vec::new();
			for branch in pending {
				if branch
					.parent_branch_id
					.as_ref()
					.is_some_and(|p| !inserted.contains(p.as_str()))
				{
					next.push(branch);
					continue;
				}
				if branch.session_id != fork.session.id
					|| branch
						.parent_branch_id
						.as_ref()
						.is_some_and(|p| p == &branch.id || !branch_ids.contains(p.as_str()))
				{
					return Err(StoreError::Validation(
						"fork branch parent ownership or cycle invalid".into(),
					));
				}
				if branch.parent_branch_id.is_some() != branch.parent_message_id.is_some() {
					return Err(StoreError::Validation(
						"fork branch parent fields must be supplied together".into(),
					));
				}
				let mut branch_payload = branch.payload.clone();
				crate::auth::error::redact_json_strings(&mut branch_payload);
				tx.execute("INSERT INTO branches (id,session_id,parent_branch_id,parent_message_id,leaf_message_id,payload_json,created_at_ms) VALUES (?1,?2,?3,?4,NULL,?5,?6)", params![branch.id, fork.session.id, branch.parent_branch_id, branch.parent_message_id, encode_json(&branch_payload)?, now])?;
				inserted.insert(branch.id);
			}
			if next.len() == before {
				return Err(StoreError::Validation("fork branch topology cannot be ordered".into()));
			}
			pending = next;
		}
		let mut branch_leaf: std::collections::HashMap<String, Option<String>> = fork
			.branches
			.iter()
			.map(|b| (b.id.clone(), b.parent_message_id.clone()))
			.collect();
		for message in &fork.messages {
			let branch = fork
				.branches
				.iter()
				.find(|b| b.id == message.branch_id)
				.ok_or_else(|| StoreError::Validation("fork message branch missing".into()))?;
			if message.session_id != fork.session.id {
				return Err(StoreError::Validation("fork message ownership mismatch".into()));
			}
			if message.parent_message_id != branch_leaf.get(&message.branch_id).cloned().flatten() {
				return Err(StoreError::Validation(
					"fork message does not chain from branch leaf".into(),
				));
			}
			if message.parent_message_id.as_deref() == Some(message.id.as_str()) {
				return Err(StoreError::Validation("fork message cannot parent itself".into()));
			}
			let parent_in_fork = message
				.parent_message_id
				.as_deref()
				.and_then(|id| fork.messages.iter().find(|m| m.id == id));
			if let Some(parent) = parent_in_fork {
				if parent.branch_id != message.branch_id
					&& Some(parent.id.as_str()) != branch.parent_message_id.as_deref()
				{
					return Err(StoreError::Validation(
						"fork message lineage crosses branch without branch point".into(),
					));
				}
			}
			if message.parent_message_id.is_some()
				&& parent_in_fork.is_none()
				&& message.parent_message_id != branch.parent_message_id
			{
				return Err(StoreError::Validation("fork message parent missing".into()));
			}
			*branch_leaf
				.get_mut(&message.branch_id)
				.expect("validated branch") = Some(message.id.clone());
			let parent_exists = message
				.parent_message_id
				.as_deref()
				.map(|id| parent_in_fork.is_some() || Some(id) == branch.parent_message_id.as_deref())
				.unwrap_or(true);
			if !parent_exists {
				return Err(StoreError::Validation("fork message parent missing".into()));
			}
			let mut msg_payload = message.payload.clone();
			crate::auth::error::redact_message_payload(&mut msg_payload);
			if msg_payload.get("type").and_then(Value::as_str) == Some("message") {
				let entry: crate::session::MessageEntry =
					serde_json::from_value(msg_payload.clone())
						.map_err(|e| StoreError::Corrupt(format!("invalid fork message payload: {e}")))?;
				entry.validate_compat().map_err(StoreError::Validation)?;
				if entry.id != message.id || entry.parent_id != message.parent_message_id {
					return Err(StoreError::Validation(
						"fork message identity does not match payload".into(),
					));
				}
			} else if let Some(payload_id) = msg_payload.get("id").and_then(Value::as_str) {
				if payload_id != message.id {
					return Err(StoreError::Validation(
						"fork entry identity does not match payload".into(),
					));
				}
				if let Some(payload_parent) = msg_payload.get("parentId") {
					if payload_parent.as_str() != message.parent_message_id.as_deref() {
						return Err(StoreError::Validation(
							"fork entry parent does not match payload".into(),
						));
					}
				}
			}
			tx.execute("INSERT INTO messages (id,session_id,branch_id,parent_message_id,payload_json,created_at_ms) VALUES (?1,?2,?3,?4,?5,?6)", params![message.id, message.session_id, message.branch_id, message.parent_message_id, encode_json(&msg_payload)?, now])?;
			tx.execute(
				"UPDATE branches SET leaf_message_id = ?1 WHERE id = ?2",
				params![message.id, message.branch_id],
			)?;
		}
		if let Some(leaf) = &fork.active_leaf_message_id {
			let owns_leaf = fork
				.messages
				.iter()
				.any(|m| m.id == *leaf && m.branch_id == fork.active_branch_id)
				|| fork
					.branches
					.iter()
					.find(|b| b.id == fork.active_branch_id)
					.and_then(|b| b.parent_message_id.as_ref())
					.is_some_and(|id| id == leaf);
			if !owns_leaf {
				return Err(StoreError::Validation(
					"active leaf does not belong to active branch".into(),
				));
			}
			branch_leaf.insert(fork.active_branch_id.clone(), Some(leaf.clone()));
			tx.execute(
				"UPDATE branches SET leaf_message_id = ?1 WHERE id = ?2",
				params![leaf, fork.active_branch_id],
			)?;
		}
		for branch in &fork.branches {
			if let (Some(parent), Some(point)) = (&branch.parent_branch_id, &branch.parent_message_id)
			{
				let effective_leaf = branch_leaf.get(parent).cloned().flatten();
				let mut cursor = effective_leaf.as_deref();
				let mut reachable = std::collections::HashSet::new();
				while let Some(id) = cursor {
					if !reachable.insert(id) {
						break;
					}
					cursor = fork
						.messages
						.iter()
						.find(|m| m.id == id)
						.and_then(|m| m.parent_message_id.as_deref());
				}
				if !reachable.contains(point.as_str()) {
					return Err(StoreError::Validation(
						"fork branch point is not on parent ancestry".into(),
					));
				}
			}
		}
		if let Some(projection) = &fork.projection {
			let mut seen = std::collections::HashSet::new();
			let branch_messages = branch_messages_tx(&tx, &fork.active_branch_id)?;
			let branch_positions: std::collections::HashMap<&str, usize> = branch_messages
				.iter()
				.enumerate()
				.map(|(i, m)| (m.id.as_str(), i))
				.collect();
			let mut kept_entries = Vec::with_capacity(projection.kept.len());
			for id in &projection.kept {
				if !seen.insert(id) {
					return Err(StoreError::Validation(format!(
						"projection references duplicate message {id}"
					)));
				}
				let row: String = tx
					.query_row("SELECT payload_json FROM messages WHERE id = ?1", params![id], |r| {
						r.get(0)
					})
					.optional()?
					.ok_or_else(|| {
						StoreError::Validation(format!(
							"projection references missing target message {id}"
						))
					})?;
				let entry: crate::session::MessageEntry = serde_json::from_str(&row).map_err(|e| {
					StoreError::Corrupt(format!(
						"projection kept entry {id} is not a message entry: {e}"
					))
				})?;
				if entry.kind != "message" {
					return Err(StoreError::Validation(format!(
						"projection kept entry {id} has invalid kind"
					)));
				}
				kept_entries.push(entry);
			}
			let kept_on_branch: Vec<usize> = projection
				.kept
				.iter()
				.filter_map(|id| branch_positions.get(id.as_str()).copied())
				.collect();
			if kept_on_branch.windows(2).any(|pair| pair[0] >= pair[1]) {
				return Err(StoreError::Validation("fork projection order is invalid".into()));
			}
			let mut synthetic_pending = pending_synthetic_call_counts(&projection.synthetic)?;
			if !projection.kept.is_empty()
				&& !validate_kept_messages_tx(
					&tx,
					&fork.session.id,
					&projection.kept,
					&branch_messages,
					&mut synthetic_pending,
				)? {
				return Err(StoreError::Validation("fork projection lineage is invalid".into()));
			}
			let mut runtime_replay =
				Vec::with_capacity(projection.synthetic.len() + kept_entries.len());
			for synthetic in &projection.synthetic {
				runtime_replay.push(
					serde_json::from_value::<pi_agent::state::Message>(synthetic.clone()).map_err(
						|e| StoreError::Corrupt(format!("invalid projection synthetic message: {e}")),
					)?,
				);
			}
			runtime_replay.extend(
				crate::runtime::history_messages_from_entries(&kept_entries)
					.map_err(|e| StoreError::Corrupt(format!("invalid projected replay: {e}")))?,
			);
			crate::durable_session::validate_runtime_replay(&runtime_replay)?;
			if let Some(anchor) = &projection.anchor {
				let owner: Option<String> = tx
					.query_row("SELECT session_id FROM messages WHERE id = ?1", params![anchor], |r| {
						r.get(0)
					})
					.optional()?;
				if owner.as_deref() != Some(fork.session.id.as_str())
					|| !branch_positions.contains_key(anchor.as_str())
				{
					return Err(StoreError::Validation(format!(
						"projection anchor is not on active ancestry: {anchor}"
					)));
				}
			}
		}
		let mut seen_provenance = HashSet::new();
		for entry in provenance {
			if !seen_provenance.insert(entry.message_id.clone()) {
				return Err(StoreError::Validation(format!(
					"duplicate fork provenance for {}",
					entry.message_id
				)));
			}
			let message = fork
				.messages
				.iter()
				.find(|m| m.id == entry.message_id)
				.ok_or_else(|| {
					StoreError::Validation("fork provenance references unknown message".into())
				})?;
			if !referenced_execution_ids(std::iter::once(&message.payload))
				.contains(&entry.source_execution_id)
			{
				return Err(StoreError::Validation(
					"fork provenance execution does not match the message".into(),
				));
			}
			if extract_tool_result_call_id(&message.payload).as_deref()
				!= Some(entry.provider_call_id.as_str())
			{
				return Err(StoreError::Validation(format!(
					"fork provenance provider call id does not match tool result for {}",
					entry.message_id
				)));
			}
		}
		let mut effective_provenance = Vec::new();
		for message in &fork.messages {
			for execution_id in referenced_execution_ids(std::iter::once(&message.payload)) {
				let call_id = extract_tool_result_call_id(&message.payload).ok_or_else(|| {
					StoreError::Validation(format!(
						"fork message {} references an execution without a toolCallId",
						message.id
					))
				})?;
				if provenance.is_empty() {
					// Public entry point: derive from the fork's direct source session.
					// Single consistent read at fork time; there is no supplied value to
					// contradict, so no target-time re-read race. The execution's provider
					// call id must match this tool result, otherwise the pairing is forged.
					let (resolved, provider_call_id): (bool, String) = match fork.session.parent_session_id.as_deref() {
						Some(parent) => tx
							.query_row("SELECT outcome_json IS NOT NULL, provider_call_id FROM executions WHERE execution_id = ?1 AND session_id = ?2", params![execution_id, parent], |r| Ok((r.get(0)?, r.get(1)?)))
							.optional()?
							.ok_or_else(|| StoreError::Validation(format!(
								"fork message {} references execution {execution_id} with no local row in the source session",
								message.id
							)))?,
						None => {
							return Err(StoreError::Validation(format!(
								"fork message {} references execution {execution_id} without a source session",
								message.id
							)))
						},
					};
					if provider_call_id != call_id {
						return Err(StoreError::Validation(format!(
							"fork message {} toolCallId {call_id} does not match execution {execution_id} provider call id {provider_call_id}",
							message.id
						)));
					}
					effective_provenance.push(ForkProvenance::new(
						message.id.clone(),
						execution_id,
						call_id,
						resolved,
					));
					continue;
				}
				// Crate-internal fork path: the captured source-snapshot classification is
				// authoritative. It was read atomically with the messages, so a later outcome
				// change in the source database MUST NOT be re-read here and MUST NOT turn the
				// captured value into a conflict.
				let entry = provenance
					.iter()
					.find(|entry| {
						entry.message_id == message.id && entry.source_execution_id == execution_id
					})
					.ok_or_else(|| {
						StoreError::Validation(format!(
							"fork message {} references execution {execution_id} without a captured provenance entry",
							message.id
						))
					})?;
				if entry.provider_call_id != call_id {
					return Err(StoreError::Validation(format!(
						"fork message {} toolCallId {call_id} does not match captured provider call id {}",
						message.id, entry.provider_call_id
					)));
				}
				if let Some(parent) = fork.session.parent_session_id.as_deref() {
					let owned_provider: Option<String> = tx
						.query_row(
							"SELECT provider_call_id FROM executions WHERE execution_id = ?1 AND session_id = ?2",
							params![execution_id, parent],
							|r| r.get(0),
						)
						.optional()?;
					if let Some(owned_provider) = owned_provider {
						if owned_provider != entry.provider_call_id {
							return Err(StoreError::Conflict(format!(
								"fork provenance provider call id {} conflicts with execution {execution_id} in the source session",
								entry.provider_call_id
							)));
						}
					}
				}
				effective_provenance.push(entry.clone());
			}
		}
		for entry in &effective_provenance {
			let existing: Option<(String, String, i64)> = tx.query_row("SELECT source_execution_id, provider_call_id, outcome_resolved FROM fork_provenance WHERE message_id = ?1", params![entry.message_id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).optional()?;
			if let Some((source, provider, resolved)) = existing {
				if source != entry.source_execution_id
					|| provider != entry.provider_call_id
					|| (resolved != 0) != entry.outcome_resolved
				{
					return Err(StoreError::Conflict(format!(
						"fork provenance conflict for {}",
						entry.message_id
					)));
				}
				continue;
			}
			tx.execute("INSERT INTO fork_provenance (message_id, session_id, source_execution_id, provider_call_id, outcome_resolved) VALUES (?1, ?2, ?3, ?4, ?5)", params![entry.message_id, fork.session.id, entry.source_execution_id, entry.provider_call_id, i64::from(entry.outcome_resolved)])?;
		}
		let stored_leaf: Option<String> = tx.query_row(
			"SELECT leaf_message_id FROM branches WHERE id = ?1",
			params![fork.active_branch_id],
			|r| r.get(0),
		)?;
		let inherited_leaf = fork
			.branches
			.iter()
			.find(|b| b.id == fork.active_branch_id)
			.and_then(|b| b.parent_message_id.clone());
		let effective_leaf = stored_leaf.clone().or_else(|| inherited_leaf.clone());
		if let Some(leaf) = &fork.active_leaf_message_id {
			let owns_leaf: bool = tx.query_row(
				"SELECT EXISTS(SELECT 1 FROM messages WHERE id = ?1 AND session_id = ?2 AND branch_id = ?3)",
				params![leaf, fork.session.id, fork.active_branch_id],
				|r| r.get(0),
			)?;
			if !owns_leaf && inherited_leaf.as_deref() != Some(leaf.as_str()) {
				return Err(StoreError::Validation(
					"active leaf does not belong to active branch".into(),
				));
			}
		} else if effective_leaf.is_some() {
			return Err(StoreError::Validation("active leaf cannot be empty".into()));
		}
		if let Some(leaf) = &fork.active_leaf_message_id {
			let stored = if inherited_leaf.as_deref() == Some(leaf.as_str()) {
				None
			} else {
				Some(leaf.clone())
			};
			tx.execute(
				"UPDATE branches SET leaf_message_id = ?1 WHERE id = ?2",
				params![stored, fork.active_branch_id],
			)?;
		}
		tx.execute(
			"UPDATE sessions SET active_branch_id = ?1, updated_at_ms = ?2 WHERE id = ?3",
			params![fork.active_branch_id, now, fork.session.id],
		)?;
		tx.commit()?;
		Ok(())
	}
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
	pub message_id: Option<String>,
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

/// A child task reservation. Written before any spawn/first-call side effect so
/// a crash cannot leave work with no durable record.
#[derive(Debug, Clone, PartialEq)]
pub struct NewTaskAdmission {
	pub task_id: String,
	pub parent_session_id: String,
	pub parent_branch_id: String,
	pub worker_id: String,
	/// Monotonic producer generation captured at admission; survives restarts.
	pub producer_generation: u64,
	pub spec: Value,
}

/// A stored task admission row.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskAdmission {
	pub task_id: String,
	pub parent_session_id: String,
	pub parent_branch_id: String,
	pub worker_id: String,
	pub producer_generation: u64,
	pub state: String,
	pub spec: Value,
	pub created_at_ms: i64,
	pub updated_at_ms: i64,
}

/// Synchronous SQLite session store. The runtime owns one instance on its
/// writer thread; `Connection` is `Send` but not `Sync`.
pub struct SqliteStore {
	path: PathBuf,
	connection: Connection,
}

impl SqliteStore {
	/// Maximum preserved prior stream fragments per branch. Bounds the
	/// draft row when interrupted streams chain without terminals.
	const MAX_STREAM_PRIORS: usize = 8;
	/// Open (or create) a dedicated session database. Parent directories are
	/// created when missing. The path is explicit: this never reuses `agent.db`
	/// or any credential database.
	pub fn open(path: impl Into<PathBuf>) -> StoreResult<Self> {
		let path = path.into();
		if path.as_os_str().is_empty() {
			return Err(StoreError::Validation("session store path must not be empty".to_string()));
		}
		if let Some(parent) = path
			.parent()
			.filter(|parent| !parent.as_os_str().is_empty())
		{
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

	/// Open an existing database for SELECT-only reads. Refuses missing or empty
	/// paths; never creates directories, never migrates, never switches WAL.
	pub fn open_read_only(path: impl Into<PathBuf>) -> StoreResult<Self> {
		let path = path.into();
		if path.as_os_str().is_empty() {
			return Err(StoreError::Validation("session store path must not be empty".to_string()));
		}
		if !path.exists() {
			return Err(StoreError::NotFound(format!(
				"session database {} does not exist",
				path.display()
			)));
		}
		let connection = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
		connection.busy_timeout(std::time::Duration::from_secs(5))?;
		connection.pragma_update(None, "foreign_keys", "ON")?;
		Ok(Self { path, connection })
	}

	/// Export one session's exact schema rows into a portable document. Read-only
	/// from the caller's perspective: no recovery, no reconcile, no timestamp
	/// refresh. Artifacts and workspace state are excluded (D55).
	pub fn export_session(&self, session_id: &str) -> StoreResult<SessionExport> {
		validate_id("session id", session_id)?;
		let transaction = self.connection.unchecked_transaction()?;
		let schema_version: Option<i64> = transaction
			.query_row("SELECT version FROM schema_version WHERE id = 1", [], |row| row.get(0))
			.optional()?;
		let schema_version = schema_version.ok_or_else(|| {
			StoreError::Schema("session database has no schema_version row".to_string())
		})?;
		if schema_version != SCHEMA_VERSION {
			return Err(StoreError::Schema(format!(
				"session database schema v{schema_version} cannot be exported by a v{SCHEMA_VERSION} binary; migrate it first"
			)));
		}
		let exists: bool = transaction.query_row(
			"SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
			params![session_id],
			|row| row.get(0),
		)?;
		if !exists {
			return Err(StoreError::NotFound(format!("session {session_id} not found")));
		}
		let mut tables = BTreeMap::new();
		for (table, scope) in SESSION_EXPORT_TABLES {
			let rows = {
				let mut statement = transaction
					.prepare(&format!("SELECT * FROM {table} WHERE {scope} = ?1 ORDER BY rowid"))?;
				let columns: Vec<String> = statement
					.column_names()
					.iter()
					.map(|name| (*name).to_string())
					.collect();
				let mut rows = statement.query(params![session_id])?;
				let mut exported = Vec::new();
				while let Some(row) = rows.next()? {
					exported.push(session_export_row_json(row, &columns)?);
				}
				exported
			};
			tables.insert((*table).to_string(), rows);
		}
		transaction.commit()?;
		Ok(SessionExport {
			format: SESSION_EXPORT_FORMAT.to_string(),
			version: SESSION_EXPORT_VERSION,
			schema_version,
			exported_at_ms: now_ms(),
			session_id: session_id.to_string(),
			tables,
		})
	}

	/// Serialize [`Self::export_session`] to a file, replacing the target only
	/// after the complete document is on disk.
	pub fn export_session_to_path(
		&self,
		session_id: &str,
		path: impl AsRef<Path>,
	) -> StoreResult<()> {
		let export = self.export_session(session_id)?;
		let path = path.as_ref();
		if let Some(parent) = path
			.parent()
			.filter(|parent| !parent.as_os_str().is_empty())
		{
			std::fs::create_dir_all(parent)?;
		}
		let bytes = serde_json::to_vec_pretty(&export)?;
		let file_name = path
			.file_name()
			.and_then(|name| name.to_str())
			.ok_or_else(|| StoreError::Validation("export path must name a file".to_string()))?;
		let temporary = path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()));
		std::fs::write(&temporary, bytes)?;
		if let Err(error) = std::fs::rename(&temporary, path) {
			let _ = std::fs::remove_file(&temporary);
			return Err(error.into());
		}
		Ok(())
	}

	/// Import one portable session document atomically. Every row is inserted
	/// with its exact exported columns; saved tool calls are never executed.
	/// An existing session id is refused rather than overwritten.
	pub fn import_session(&mut self, export: SessionExport) -> StoreResult<()> {
		if export.format != SESSION_EXPORT_FORMAT {
			return Err(StoreError::Validation(format!(
				"unsupported session export format {:?}",
				export.format
			)));
		}
		if export.version != SESSION_EXPORT_VERSION {
			return Err(StoreError::Schema(format!(
				"session export version {} is not supported; expected {}",
				export.version, SESSION_EXPORT_VERSION
			)));
		}
		if export.schema_version != SCHEMA_VERSION {
			return Err(StoreError::Schema(format!(
				"session export schema v{} does not match store schema v{SCHEMA_VERSION}",
				export.schema_version
			)));
		}
		validate_id("session id", &export.session_id)?;
		if self.session(&export.session_id)?.is_some() {
			return Err(StoreError::Conflict(format!("session {} already exists", export.session_id)));
		}
		for (table, _) in SESSION_EXPORT_TABLES {
			if !export.tables.contains_key(*table) {
				return Err(StoreError::Validation(format!("session export is missing table {table}")));
			}
		}
		if export.tables.len() != SESSION_EXPORT_TABLES.len() {
			return Err(StoreError::Validation("session export contains unknown tables".to_string()));
		}
		let session_rows = export.tables.get("sessions").expect("checked above");
		let [session_row] = session_rows.as_slice() else {
			return Err(StoreError::Validation(
				"session export must contain exactly one session row".to_string(),
			));
		};
		let session_object = session_row.as_object().ok_or_else(|| {
			StoreError::Validation("session export session row is not an object".to_string())
		})?;
		if session_object.get("id").and_then(Value::as_str) != Some(export.session_id.as_str()) {
			return Err(StoreError::Validation(
				"session export session row id does not match session_id".to_string(),
			));
		}
		let parent_session = session_object
			.get("parent_session_id")
			.and_then(Value::as_str)
			.map(str::to_string);
		if parent_session.as_deref() == Some(export.session_id.as_str()) {
			return Err(StoreError::Validation("session export cannot parent itself".to_string()));
		}
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		// Branch parent_message_id and message branch_id form a cycle, so checks
		// are deferred to COMMIT; the session parent is verified explicitly
		// because its row may legitimately live in another database.
		transaction.pragma_update(None, "defer_foreign_keys", "ON")?;
		if let Some(parent) = parent_session.as_deref() {
			let parent_exists: bool = transaction.query_row(
				"SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
				params![parent],
				|row| row.get(0),
			)?;
			if !parent_exists {
				return Err(StoreError::NotFound(format!(
					"parent session {parent} is not present; import it first"
				)));
			}
		}
		for (table, scope) in SESSION_EXPORT_TABLES {
			let rows = export.tables.get(*table).expect("checked above");
			let columns = session_export_table_columns(&transaction, table)?;
			let placeholders = (1..=columns.len())
				.map(|index| format!("?{index}"))
				.collect::<Vec<_>>()
				.join(", ");
			let sql = format!("INSERT INTO {table} ({}) VALUES ({placeholders})", columns.join(", "));
			for (index, row) in rows.iter().enumerate() {
				let object = row.as_object().ok_or_else(|| {
					StoreError::Validation(format!("{table} row {index} is not an object"))
				})?;
				if object.len() != columns.len()
					|| !columns.iter().all(|column| object.contains_key(column))
				{
					return Err(StoreError::Validation(format!(
						"{table} row {index} does not match the live schema columns"
					)));
				}
				if object.get(*scope).and_then(Value::as_str) != Some(export.session_id.as_str()) {
					return Err(StoreError::Validation(format!(
						"{table} row {index} is outside session {}",
						export.session_id
					)));
				}
				let values = columns
					.iter()
					.map(|column| session_export_value_from_json(table, column, &object[column]))
					.collect::<StoreResult<Vec<_>>>()?;
				transaction.execute(&sql, params_from_iter(values))?;
			}
		}
		transaction.commit()?;
		Ok(())
	}

	/// Read and validate a portable export file, then import it atomically.
	pub fn import_session_from_path(&mut self, path: impl AsRef<Path>) -> StoreResult<()> {
		let bytes = std::fs::read(path)?;
		let export: SessionExport = serde_json::from_slice(&bytes)?;
		self.import_session(export)
	}

	fn transaction_immediate(&self) -> StoreResult<Transaction<'_>> {
		Ok(Transaction::new_unchecked(
			&self.connection,
			TransactionBehavior::Immediate,
		)?)
	}

	/// Export one session across all ten session-scoped v7 tables into a
	/// validated, redacted bundle. Read-only: no migration, recovery, reconcile,
	/// or timestamp refresh. Artifact files and workspace state are never read.
	pub fn export_session_bundle(&self, session_id: &str) -> StoreResult<SessionBundle> {
		validate_id("session id", session_id)?;
		let transaction = self.connection.unchecked_transaction()?;
		let schema_version: Option<i64> = transaction
			.query_row("SELECT version FROM schema_version WHERE id = 1", [], |row| row.get(0))
			.optional()?;
		let schema_version = schema_version.ok_or_else(|| {
			StoreError::Schema("session database has no schema_version row".to_string())
		})?;
		let has_producer_generation =
			session_table_has_column(&transaction, "sessions", "producer_generation")?;
		let session_sql = if has_producer_generation {
			"SELECT id, parent_session_id, active_branch_id, payload_json, created_at_ms, updated_at_ms, producer_generation \
			 FROM sessions WHERE id = ?1"
		} else {
			"SELECT id, parent_session_id, active_branch_id, payload_json, created_at_ms, updated_at_ms, NULL \
			 FROM sessions WHERE id = ?1"
		};
		let session_row = transaction
			.query_row(session_sql, params![session_id], |row| {
				Ok((
					row.get::<_, String>(0)?,
					row.get::<_, Option<String>>(1)?,
					row.get::<_, Option<String>>(2)?,
					row.get::<_, String>(3)?,
					row.get::<_, i64>(4)?,
					row.get::<_, i64>(5)?,
					row.get::<_, Option<i64>>(6)?,
				))
			})
			.optional()?;
		let Some((
			id,
			parent_session_id,
			active_branch_id,
			payload_json,
			created_at_ms,
			updated_at_ms,
			producer_generation,
		)) = session_row
		else {
			return Err(StoreError::NotFound(format!("session {session_id} not found")));
		};

		let mut branches = Vec::new();
		{
			let mut statement = transaction.prepare(
				"SELECT id, session_id, parent_branch_id, parent_message_id, leaf_message_id, payload_json, created_at_ms \
				 FROM branches WHERE session_id = ?1 ORDER BY created_at_ms, id",
			)?;
			let mut rows = statement.query(params![session_id])?;
			while let Some(row) = rows.next()? {
				branches.push(SessionBundleBranch {
					id: row.get(0)?,
					session_id: row.get(1)?,
					parent_branch_id: row.get(2)?,
					parent_message_id: row.get(3)?,
					leaf_message_id: row.get(4)?,
					payload: decode_json(&row.get::<_, String>(5)?)?,
					created_at_ms: row.get(6)?,
				});
			}
		}
		let mut messages = Vec::new();
		{
			let mut statement = transaction.prepare(
				"SELECT seq, id, session_id, branch_id, parent_message_id, payload_json, created_at_ms \
				 FROM messages WHERE session_id = ?1 ORDER BY seq ASC",
			)?;
			let mut rows = statement.query(params![session_id])?;
			while let Some(row) = rows.next()? {
				messages.push(SessionBundleMessage {
					seq: row.get(0)?,
					id: row.get(1)?,
					session_id: row.get(2)?,
					branch_id: row.get(3)?,
					parent_message_id: row.get(4)?,
					payload: decode_json(&row.get::<_, String>(5)?)?,
					created_at_ms: row.get(6)?,
				});
			}
		}
		let mut executions = Vec::new();
		{
			let mut statement = transaction.prepare(
				"SELECT seq, execution_id, session_id, branch_id, provider_call_id, tool_name, intent_json, created_at_ms, outcome_json, outcome_at_ms \
				 FROM executions WHERE session_id = ?1 ORDER BY seq ASC",
			)?;
			let mut rows = statement.query(params![session_id])?;
			while let Some(row) = rows.next()? {
				let outcome_json: Option<String> = row.get(8)?;
				executions.push(SessionBundleExecution {
					seq: row.get(0)?,
					execution_id: row.get(1)?,
					session_id: row.get(2)?,
					branch_id: row.get(3)?,
					provider_call_id: row.get(4)?,
					tool_name: row.get(5)?,
					intent: decode_json(&row.get::<_, String>(6)?)?,
					created_at_ms: row.get(7)?,
					outcome: outcome_json.as_deref().map(decode_json).transpose()?,
					outcome_at_ms: row.get(9)?,
				});
			}
		}
		let mut deliveries = Vec::new();
		{
			let mut statement = transaction.prepare(
				"SELECT delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms, message_id \
				 FROM deliveries WHERE session_id = ?1 ORDER BY created_at_ms, delivery_id",
			)?;
			let mut rows = statement.query(params![session_id])?;
			while let Some(row) = rows.next()? {
				deliveries.push(SessionBundleDelivery {
					delivery_id: row.get(0)?,
					worker_id: row.get(1)?,
					session_id: row.get(2)?,
					origin_branch_id: row.get(3)?,
					payload: decode_json(&row.get::<_, String>(4)?)?,
					created_at_ms: row.get(5)?,
					delivered_at_ms: row.get(6)?,
					processed_at_ms: row.get(7)?,
					message_id: row.get(8)?,
				});
			}
		}
		let mut worker_completions = Vec::new();
		{
			let mut statement = transaction.prepare(
				"SELECT worker_id, session_id, origin_branch_id, delivery_id, completion_json, completed_at_ms \
				 FROM worker_completions WHERE session_id = ?1 ORDER BY completed_at_ms, worker_id",
			)?;
			let mut rows = statement.query(params![session_id])?;
			while let Some(row) = rows.next()? {
				worker_completions.push(SessionBundleWorkerCompletion {
					worker_id: row.get(0)?,
					session_id: row.get(1)?,
					origin_branch_id: row.get(2)?,
					delivery_id: row.get(3)?,
					completion: decode_json(&row.get::<_, String>(4)?)?,
					completed_at_ms: row.get(5)?,
				});
			}
		}
		let mut artifacts = Vec::new();
		{
			let mut statement = transaction.prepare(
				"SELECT artifact_id, session_id, branch_id, execution_id, location, metadata_json, created_at_ms \
				 FROM artifacts WHERE session_id = ?1 ORDER BY created_at_ms, artifact_id",
			)?;
			let mut rows = statement.query(params![session_id])?;
			while let Some(row) = rows.next()? {
				artifacts.push(SessionBundleArtifact {
					artifact_id: row.get(0)?,
					session_id: row.get(1)?,
					branch_id: row.get(2)?,
					execution_id: row.get(3)?,
					location: row.get(4)?,
					metadata: decode_json(&row.get::<_, String>(5)?)?,
					created_at_ms: row.get(6)?,
				});
			}
		}
		let mut fork_provenance = Vec::new();
		if table_exists_conn(&transaction, "fork_provenance")? {
			let mut statement = transaction.prepare(
				"SELECT message_id, session_id, source_execution_id, provider_call_id, outcome_resolved \
				 FROM fork_provenance WHERE session_id = ?1 ORDER BY message_id",
			)?;
			let mut rows = statement.query(params![session_id])?;
			while let Some(row) = rows.next()? {
				fork_provenance.push(SessionBundleForkProvenance {
					message_id: row.get(0)?,
					session_id: row.get(1)?,
					source_execution_id: row.get(2)?,
					provider_call_id: row.get(3)?,
					outcome_resolved: row.get::<_, i64>(4)? != 0,
				});
			}
		}
		let mut task_admissions = Vec::new();
		if table_exists_conn(&transaction, "task_admissions")? {
			let mut statement = transaction.prepare(
				"SELECT task_id, parent_session_id, parent_branch_id, worker_id, producer_generation, state, spec_json, created_at_ms, updated_at_ms \
				 FROM task_admissions WHERE parent_session_id = ?1 ORDER BY created_at_ms, task_id",
			)?;
			let mut rows = statement.query(params![session_id])?;
			while let Some(row) = rows.next()? {
				let spec_json: String = row.get(6)?;
				task_admissions.push(SessionBundleTaskAdmission {
					task_id: row.get(0)?,
					parent_session_id: row.get(1)?,
					parent_branch_id: row.get(2)?,
					worker_id: row.get(3)?,
					producer_generation: row.get(4)?,
					state: row.get(5)?,
					spec: decode_json(&spec_json)?,
					created_at_ms: row.get(7)?,
					updated_at_ms: row.get(8)?,
				});
			}
		}
		let mut stream_drafts = Vec::new();
		if table_exists_conn(&transaction, "stream_drafts")? {
			let mut statement = transaction.prepare(
				"SELECT branch_id, session_id, payload_json, updated_at_ms \
				 FROM stream_drafts WHERE session_id = ?1 ORDER BY branch_id",
			)?;
			let mut rows = statement.query(params![session_id])?;
			while let Some(row) = rows.next()? {
				stream_drafts.push(SessionBundleStreamDraft {
					branch_id: row.get(0)?,
					session_id: row.get(1)?,
					payload: decode_json(&row.get::<_, String>(2)?)?,
					updated_at_ms: row.get(3)?,
				});
			}
		}
		transaction.commit()?;
		let mut bundle = SessionBundle {
			format: SESSION_BUNDLE_FORMAT.to_string(),
			schema_version,
			exported_at_ms: now_ms(),
			session: SessionBundleSession {
				id,
				parent_session_id,
				active_branch_id,
				payload: decode_json(&payload_json)?,
				created_at_ms,
				updated_at_ms,
				producer_generation,
			},
			branches,
			messages,
			executions,
			deliveries,
			worker_completions,
			artifacts,
			fork_provenance,
			task_admissions,
			stream_drafts,
		};
		redact_session_bundle(&mut bundle);
		Ok(bundle)
	}

	/// Validate and import one redacted bundle atomically under `BEGIN
	/// IMMEDIATE`. Recorded tool calls, shell commands, and hooks are only ever
	/// stored as opaque payloads and are never executed (D55).
	pub fn import_session_bundle(
		&self,
		bundle: &SessionBundle,
		detach_parent: bool,
	) -> StoreResult<ImportBundleReport> {
		let mut bundle = bundle.clone();
		redact_session_bundle(&mut bundle);
		validate_session_bundle(&bundle)?;
		let transaction = self.transaction_immediate()?;
		// Branch parent_message_id can point at a message inserted later; checks
		// are deferred to COMMIT after the whole graph exists.
		transaction.pragma_update(None, "defer_foreign_keys", "ON")?;
		let exists: bool = transaction.query_row(
			"SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
			params![bundle.session.id],
			|row| row.get(0),
		)?;
		if exists {
			return Err(StoreError::Conflict(format!(
				"session {} already exists",
				bundle.session.id
			)));
		}
		let mut detached = false;
		let mut detached_parent_session_id = None;
		if let Some(parent) = bundle.session.parent_session_id.clone() {
			let parent_exists: bool = transaction.query_row(
				"SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
				params![parent],
				|row| row.get(0),
			)?;
			if !parent_exists {
				if !detach_parent {
					return Err(StoreError::Validation(format!(
						"parent session {parent} does not exist; import it first or detach the parent"
					)));
				}
				bundle.session.parent_session_id = None;
				record_imported_parent_session(&mut bundle.session.payload, &parent);
				detached = true;
				detached_parent_session_id = Some(parent);
			}
		}
		let session_payload = encode_json(&bundle.session.payload)?;
		transaction.execute(
			"INSERT INTO sessions \
			 (id, parent_session_id, active_branch_id, payload_json, created_at_ms, updated_at_ms, producer_generation) \
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
			params![
				bundle.session.id,
				bundle.session.parent_session_id,
				bundle.session.active_branch_id,
				session_payload,
				bundle.session.created_at_ms,
				bundle.session.updated_at_ms,
				bundle.session.producer_generation,
			],
		)?;
		for branch in order_session_bundle_branches(&bundle.branches)? {
			transaction.execute(
				"INSERT INTO branches \
				 (id, session_id, parent_branch_id, parent_message_id, leaf_message_id, payload_json, created_at_ms) \
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
				params![
					branch.id,
					branch.session_id,
					branch.parent_branch_id,
					branch.parent_message_id,
					branch.leaf_message_id,
					encode_json(&branch.payload)?,
					branch.created_at_ms,
				],
			)?;
		}
		// `seq` is database-wide, not session-local. Keep the bundle's relative
		// order but allocate fresh monotonic values so an import into a
		// non-empty database cannot collide with another session.
		let message_base: i64 = transaction.query_row(
			"SELECT COALESCE(MAX(seq), 0) FROM messages",
			[],
			|row| row.get(0),
		)?;
		let mut message_seqs = HashMap::with_capacity(bundle.messages.len());
		let mut source_messages: Vec<&SessionBundleMessage> = bundle.messages.iter().collect();
		source_messages.sort_by_key(|message| (message.seq, message.id.as_str()));
		for (offset, message) in source_messages.iter().enumerate() {
			let seq = message_base
				.checked_add(1 + offset as i64)
				.ok_or_else(|| StoreError::Validation("message seq overflow".into()))?;
			message_seqs.insert(message.id.as_str(), seq);
		}
		for message in order_session_bundle_messages(&bundle.messages)? {
			let seq = message_seqs
				.get(message.id.as_str())
				.copied()
				.ok_or_else(|| StoreError::Validation("message seq allocation missing".into()))?;
			transaction.execute(
				"INSERT INTO messages \
				 (seq, id, session_id, branch_id, parent_message_id, payload_json, created_at_ms) \
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
				params![
					seq,
					message.id,
					message.session_id,
					message.branch_id,
					message.parent_message_id,
					encode_json(&message.payload)?,
					message.created_at_ms,
				],
			)?;
		}
		let execution_base: i64 = transaction.query_row(
			"SELECT COALESCE(MAX(seq), 0) FROM executions",
			[],
			|row| row.get(0),
		)?;
		let mut source_executions: Vec<&SessionBundleExecution> = bundle.executions.iter().collect();
		source_executions.sort_by_key(|execution| (execution.seq, execution.execution_id.as_str()));
		for (offset, execution) in source_executions.iter().enumerate() {
			let seq = execution_base
				.checked_add(1 + offset as i64)
				.ok_or_else(|| StoreError::Validation("execution seq overflow".into()))?;
			let outcome_json = execution.outcome.as_ref().map(encode_json).transpose()?;
			transaction.execute(
				"INSERT INTO executions \
				 (seq, execution_id, session_id, branch_id, provider_call_id, tool_name, intent_json, created_at_ms, outcome_json, outcome_at_ms) \
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
				params![
					seq,
					execution.execution_id,
					execution.session_id,
					execution.branch_id,
					execution.provider_call_id,
					execution.tool_name,
					encode_json(&execution.intent)?,
					execution.created_at_ms,
					outcome_json,
					execution.outcome_at_ms,
				],
			)?;
		}
		for delivery in &bundle.deliveries {
			transaction.execute(
				"INSERT INTO deliveries \
				 (delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms, message_id) \
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
				params![
					delivery.delivery_id,
					delivery.worker_id,
					delivery.session_id,
					delivery.origin_branch_id,
					encode_json(&delivery.payload)?,
					delivery.created_at_ms,
					delivery.delivered_at_ms,
					delivery.processed_at_ms,
					delivery.message_id,
				],
			)?;
		}
		for completion in &bundle.worker_completions {
			transaction.execute(
				"INSERT INTO worker_completions \
				 (worker_id, session_id, origin_branch_id, delivery_id, completion_json, completed_at_ms) \
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
				params![
					completion.worker_id,
					completion.session_id,
					completion.origin_branch_id,
					completion.delivery_id,
					encode_json(&completion.completion)?,
					completion.completed_at_ms,
				],
			)?;
		}
		for artifact in &bundle.artifacts {
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
					encode_json(&artifact.metadata)?,
					artifact.created_at_ms,
				],
			)?;
		}
		for provenance in &bundle.fork_provenance {
			transaction.execute(
				"INSERT INTO fork_provenance \
				 (message_id, session_id, source_execution_id, provider_call_id, outcome_resolved) \
				 VALUES (?1, ?2, ?3, ?4, ?5)",
				params![
					provenance.message_id,
					provenance.session_id,
					provenance.source_execution_id,
					provenance.provider_call_id,
					i64::from(provenance.outcome_resolved),
				],
			)?;
		}
		for admission in &bundle.task_admissions {
			transaction.execute(
				"INSERT INTO task_admissions \
				 (task_id, parent_session_id, parent_branch_id, worker_id, producer_generation, state, spec_json, created_at_ms, updated_at_ms) \
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
				params![
					admission.task_id,
					admission.parent_session_id,
					admission.parent_branch_id,
					admission.worker_id,
					admission.producer_generation,
					admission.state,
					encode_json(&admission.spec)?,
					admission.created_at_ms,
					admission.updated_at_ms,
				],
			)?;
		}
		for draft in &bundle.stream_drafts {
			transaction.execute(
				"INSERT INTO stream_drafts (branch_id, session_id, payload_json, updated_at_ms) \
				 VALUES (?1, ?2, ?3, ?4)",
				params![
					draft.branch_id,
					draft.session_id,
					encode_json(&draft.payload)?,
					draft.updated_at_ms,
				],
			)?;
		}
		transaction.commit()?;
		Ok(ImportBundleReport {
			session_id: bundle.session.id,
			detached,
			detached_parent_session_id,
			branches: bundle.branches.len(),
			messages: bundle.messages.len(),
			executions: bundle.executions.len(),
			deliveries: bundle.deliveries.len(),
			worker_completions: bundle.worker_completions.len(),
			artifacts: bundle.artifacts.len(),
			fork_provenance: bundle.fork_provenance.len(),
			task_admissions: bundle.task_admissions.len(),
			stream_drafts: bundle.stream_drafts.len(),
		})
	}

	/// D74 read-only session inspector. Returns metadata, counts, artifact
	/// existence, and token estimates only; no message bodies are included.
	pub fn inspect_session(&self, session_id: &str) -> StoreResult<SessionInspectorReport> {
		validate_id("session id", session_id)?;
		let transaction = self.connection.unchecked_transaction()?;
		let has_producer_generation =
			session_table_has_column(&transaction, "sessions", "producer_generation")?;
		let session_sql = if has_producer_generation {
			"SELECT id, parent_session_id, active_branch_id, payload_json, created_at_ms, updated_at_ms, producer_generation \
			 FROM sessions WHERE id = ?1"
		} else {
			"SELECT id, parent_session_id, active_branch_id, payload_json, created_at_ms, updated_at_ms, NULL \
			 FROM sessions WHERE id = ?1"
		};
		let row = transaction
			.query_row(session_sql, params![session_id], |row| {
				Ok((
					row.get::<_, String>(0)?,
					row.get::<_, Option<String>>(1)?,
					row.get::<_, Option<String>>(2)?,
					row.get::<_, String>(3)?,
					row.get::<_, i64>(4)?,
					row.get::<_, i64>(5)?,
					row.get::<_, Option<i64>>(6)?,
				))
			})
			.optional()?;
		let Some((
			id,
			parent_session_id,
			active_branch_id,
			payload_json,
			created_at_ms,
			updated_at_ms,
			producer_generation,
		)) = row
		else {
			return Err(StoreError::NotFound(format!("session {session_id} not found")));
		};
		let payload: Value = decode_json(&payload_json)?;
		let branches = count_session_rows(&transaction, "branches", "session_id", session_id)?;
		let messages = count_session_rows(&transaction, "messages", "session_id", session_id)?;
		let executions = count_session_rows(&transaction, "executions", "session_id", session_id)?;
		let unresolved_executions = count_rows(
			&transaction,
			"SELECT COUNT(*) FROM executions WHERE session_id = ?1 AND outcome_json IS NULL",
			session_id,
		)?;
		let deliveries = count_session_rows(&transaction, "deliveries", "session_id", session_id)?;
		let pending_deliveries = count_rows(
			&transaction,
			"SELECT COUNT(*) FROM deliveries WHERE session_id = ?1 AND delivered_at_ms IS NULL",
			session_id,
		)?;
		let worker_completions =
			count_session_rows(&transaction, "worker_completions", "session_id", session_id)?;
		let fork_provenance = if table_exists_conn(&transaction, "fork_provenance")? {
			count_session_rows(&transaction, "fork_provenance", "session_id", session_id)?
		} else {
			0
		};
		let task_admissions = if table_exists_conn(&transaction, "task_admissions")? {
			count_session_rows(&transaction, "task_admissions", "parent_session_id", session_id)?
		} else {
			0
		};
		let stream_drafts = if table_exists_conn(&transaction, "stream_drafts")? {
			count_session_rows(&transaction, "stream_drafts", "session_id", session_id)?
		} else {
			0
		};
		let mut artifact_summaries = Vec::new();
		let mut missing_artifacts_on_disk = 0usize;
		{
			let mut statement = transaction.prepare(
				"SELECT artifact_id, location FROM artifacts WHERE session_id = ?1 ORDER BY created_at_ms, artifact_id",
			)?;
			let mut rows = statement.query(params![session_id])?;
			while let Some(row) = rows.next()? {
				let mut artifact_id: String = row.get(0)?;
				let mut location: String = row.get(1)?;
				let exists_on_disk = artifact_exists_on_disk(&location);
				if !exists_on_disk {
					missing_artifacts_on_disk += 1;
				}
				redact_bundle_string(&mut artifact_id);
				redact_bundle_string(&mut location);
				artifact_summaries.push(ArtifactSummary {
					artifact_id,
					location,
					exists_on_disk,
				});
			}
		}
		let artifacts = artifact_summaries.len();
		let branch_messages = match active_branch_id.as_deref() {
			Some(branch_id) => self.branch_messages(branch_id)?,
			None => Vec::new(),
		};
		let active_branch_messages = branch_messages.len();
		let (context_estimate, provider_usage) =
			estimate_branch_context(&branch_messages, &payload);
		transaction.commit()?;
		let cwd = payload
			.get("cwd")
			.and_then(Value::as_str)
			.map(redact_free_text);
		let title = payload
			.get("title")
			.and_then(Value::as_str)
			.map(redact_free_text);
		Ok(SessionInspectorReport {
			id: redact_free_text(&id),
			parent_session_id: parent_session_id.as_deref().map(redact_free_text),
			active_branch_id: active_branch_id.as_deref().map(redact_free_text),
			producer_generation,
			created_at_ms,
			updated_at_ms,
			cwd,
			title,
			branches,
			messages,
			active_branch_messages,
			executions,
			unresolved_executions,
			deliveries,
			pending_deliveries,
			worker_completions,
			artifacts,
			missing_artifacts_on_disk,
			fork_provenance,
			task_admissions,
			stream_drafts,
			artifact_summaries,
			context_estimate,
			provider_usage,
		})
	}

	/// Read-only database-wide inspector: schema version, per-table totals, and
	/// redacted session summaries. No message bodies are returned.
	pub fn inspect_database(&self) -> StoreResult<DatabaseInspectorReport> {
		let transaction = self.connection.unchecked_transaction()?;
		let schema_version: Option<i64> = transaction
			.query_row("SELECT version FROM schema_version WHERE id = 1", [], |row| row.get(0))
			.optional()?;
		let schema_version = schema_version.ok_or_else(|| {
			StoreError::Schema("session database has no schema_version row".to_string())
		})?;
		let mut table_counts = BTreeMap::new();
		for table in [
			"schema_version",
			"sessions",
			"branches",
			"messages",
			"executions",
			"deliveries",
			"worker_completions",
			"artifacts",
			"fork_provenance",
			"task_admissions",
			"stream_drafts",
		] {
			let count: i64 = transaction.query_row(
				&format!("SELECT COUNT(*) FROM {table}"),
				[],
				|row| row.get(0),
			)?;
			table_counts.insert(table.to_string(), count);
		}
		let mut sessions = Vec::new();
		{
			let mut statement = transaction.prepare(
				"SELECT s.id, s.parent_session_id, s.active_branch_id, s.payload_json, s.created_at_ms, s.updated_at_ms, \
				        (SELECT COUNT(*) FROM branches b WHERE b.session_id = s.id), \
				        (SELECT COUNT(*) FROM messages m WHERE m.session_id = s.id) \
				 FROM sessions s ORDER BY s.created_at_ms, s.id",
			)?;
			let mut rows = statement.query([])?;
			while let Some(row) = rows.next()? {
				let id: String = row.get(0)?;
				let parent_session_id: Option<String> = row.get(1)?;
				let active_branch_id: Option<String> = row.get(2)?;
				let payload: Value = decode_json(&row.get::<_, String>(3)?)?;
				let title = payload
					.get("title")
					.and_then(Value::as_str)
					.map(redact_free_text);
				sessions.push(DatabaseSessionSummary {
					id: redact_free_text(&id),
					parent_session_id: parent_session_id.as_deref().map(redact_free_text),
					active_branch_id: active_branch_id.as_deref().map(redact_free_text),
					title,
					created_at_ms: row.get(4)?,
					updated_at_ms: row.get(5)?,
					branches: row.get(6)?,
					messages: row.get(7)?,
				});
			}
		}
		transaction.commit()?;
		Ok(DatabaseInspectorReport {
			schema_version,
			table_counts,
			sessions,
		})
	}

	/// One deferred read transaction: session header, active branch leaf, every
	/// session message row, and the active branch chain. No recovery, no
	/// reconcile, no writes. Replay validation runs on decoded branch messages.
	pub fn load_session_snapshot(&self, session_id: &str) -> StoreResult<SessionSnapshot> {
		validate_id("session id", session_id)?;
		// Deferred read transaction: subsequent queries on this connection share it.
		let transaction = self.connection.unchecked_transaction()?;
		let record = self
			.session(session_id)?
			.ok_or_else(|| StoreError::NotFound(format!("session {session_id} not found")))?;
		let branch_id = record.active_branch_id.clone().ok_or_else(|| {
			StoreError::Corrupt(format!("session {session_id} has no active branch"))
		})?;
		let branch = self
			.branch(&branch_id)?
			.ok_or_else(|| StoreError::Corrupt(format!("branch {branch_id} not found")))?;
		if branch.session_id != session_id {
			return Err(StoreError::Corrupt(format!(
				"active branch {branch_id} belongs to session {}, not {session_id}",
				branch.session_id
			)));
		}
		let leaf = branch.leaf_message_id.clone();
		let session_messages = self.session_messages(session_id)?;
		let branch_messages = self.branch_messages(&branch_id)?;
		let branches = self.session_branches(session_id)?;
		let projection = match record.payload.get("durableProjection") {
			None => None,
			Some(value) => Some(parse_stored_projection(value)?),
		};
		let referenced = referenced_execution_ids(session_messages.iter().map(|m| &m.payload));
		let executions = executions_by_ids_conn(&self.connection, &referenced)?;
		let provenance = fork_provenance_conn(&self.connection, session_id)?;
		// Only the active branch is validated here. Sibling branches that the fork
		// will prune may legitimately reference external executions (for example a
		// branch dropped by an older fork); the fork preflight validates exactly the
		// messages it copies, including kept off-branch ancestry.
		for message in &branch_messages {
			for execution_id in referenced_execution_ids(std::iter::once(&message.payload)) {
				let owned = executions
					.iter()
					.find(|e| e.execution_id == execution_id && e.session_id == session_id);
				let copied = provenance
					.iter()
					.find(|p| p.message_id == message.id && p.source_execution_id == execution_id);
				if let Some(owned) = owned {
					if extract_tool_result_call_id(&message.payload).as_deref()
						!= Some(owned.provider_call_id.as_str())
					{
						return Err(StoreError::Corrupt(format!(
							"tool result for execution {execution_id} does not carry provider call id {}",
							owned.provider_call_id
						)));
					}
				}
				match (copied, owned) {
					(Some(copied), Some(owned))
						if copied.outcome_resolved != owned.outcome.is_some()
							|| copied.provider_call_id != owned.provider_call_id =>
					{
						return Err(StoreError::Corrupt(format!(
							"fork provenance conflicts with local execution {execution_id} for message {}",
							message.id
						)));
					},
					(Some(_), _) | (None, Some(_)) => {},
					(None, None) => {
						return Err(StoreError::Corrupt(format!(
							"messages reference unknown execution(s): {execution_id}"
						)));
					},
				}
			}
		}
		for entry in &provenance {
			let message = session_messages
				.iter()
				.find(|m| m.id == entry.message_id)
				.ok_or_else(|| {
					StoreError::Corrupt(format!(
						"fork provenance references unknown message {}",
						entry.message_id
					))
				})?;
			if !referenced_execution_ids(std::iter::once(&message.payload))
				.contains(&entry.source_execution_id)
			{
				return Err(StoreError::Corrupt(format!(
					"fork provenance for message {} names execution {} which the message does not reference",
					entry.message_id, entry.source_execution_id
				)));
			}
			if extract_tool_result_call_id(&message.payload).as_deref()
				!= Some(entry.provider_call_id.as_str())
			{
				return Err(StoreError::Corrupt(format!(
					"fork provenance for message {} names provider call id {} which the tool result does not carry",
					entry.message_id, entry.provider_call_id
				)));
			}
		}
		transaction.commit()?;
		let mut decoded_branch = Vec::with_capacity(branch_messages.len());
		for row in &branch_messages {
			let entry = serde_json::from_value::<crate::session::SessionEntry>(row.payload.clone())
				.map_err(|error| {
					StoreError::Corrupt(format!("invalid branch entry {}: {error}", row.id))
				})?;
			if entry.kind == "message" {
				let message =
					serde_json::from_value::<crate::session::MessageEntry>(row.payload.clone())
						.map_err(|error| {
							StoreError::Corrupt(format!("invalid branch message {}: {error}", row.id))
						})?;
				decoded_branch.push(message);
			}
		}
		crate::session::derive_turn_history(&decoded_branch).map_err(StoreError::Corrupt)?;
		Ok(SessionSnapshot {
			session_id: session_id.to_string(),
			header_payload: record.payload,
			branch_id,
			leaf,
			session_messages,
			branch_messages,
			projection,
			branches,
			executions,
			provenance,
		})
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
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		transaction.execute_batch(SCHEMA_VERSION_DDL)?;
		let recorded: Option<i64> = transaction
			.query_row("SELECT version FROM schema_version WHERE id = 1", [], |row| row.get(0))
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
				transaction.execute_batch(SCHEMA_V3_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V4_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V5_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V6_MIGRATION_DDL)?;
				migrate_producer_generation(&transaction)?;
				transaction.execute(
					"INSERT INTO schema_version (id, version) VALUES (1, ?1)",
					params![SCHEMA_VERSION],
				)?;
			},
			Some(1) => {
				transaction.execute_batch(SCHEMA_V2_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V3_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V4_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V5_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V6_MIGRATION_DDL)?;
				migrate_producer_generation(&transaction)?;
				transaction.execute(
					"UPDATE schema_version SET version = ?1 WHERE id = 1",
					params![SCHEMA_VERSION],
				)?;
			},
			Some(2) => {
				transaction.execute_batch(SCHEMA_V3_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V4_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V5_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V6_MIGRATION_DDL)?;
				migrate_producer_generation(&transaction)?;
				transaction.execute(
					"UPDATE schema_version SET version = ?1 WHERE id = 1",
					params![SCHEMA_VERSION],
				)?;
			},
			Some(3) => {
				transaction.execute_batch(SCHEMA_V4_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V5_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V6_MIGRATION_DDL)?;
				migrate_producer_generation(&transaction)?;
				transaction.execute(
					"UPDATE schema_version SET version = ?1 WHERE id = 1",
					params![SCHEMA_VERSION],
				)?;
			},
			Some(4) => {
				transaction.execute_batch(SCHEMA_V5_MIGRATION_DDL)?;
				transaction.execute_batch(SCHEMA_V6_MIGRATION_DDL)?;
				migrate_producer_generation(&transaction)?;
				transaction.execute(
					"UPDATE schema_version SET version = ?1 WHERE id = 1",
					params![SCHEMA_VERSION],
				)?;
			},
			Some(5) => {
				transaction.execute_batch(SCHEMA_V6_MIGRATION_DDL)?;
				migrate_producer_generation(&transaction)?;
				transaction.execute(
					"UPDATE schema_version SET version = ?1 WHERE id = 1",
					params![SCHEMA_VERSION],
				)?;
			},
			Some(6) => {
				migrate_producer_generation(&transaction)?;
				transaction.execute(
					"UPDATE schema_version SET version = ?1 WHERE id = 1",
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
			return Err(StoreError::Validation("a session cannot be its own parent".to_string()));
		}
		let mut session = session;
		crate::auth::error::redact_session_payload(&mut session.payload);
		crate::auth::error::redact_json_strings(&mut session.initial_branch_payload);
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		let exists: bool = transaction.query_row(
			"SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
			params![session.id],
			|row| row.get(0),
		)?;
		if exists {
			return Err(StoreError::Conflict(format!("session {} already exists", session.id)));
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
					Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?))
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
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		require_session(&transaction, &branch.session_id)?;
		let exists: bool = transaction.query_row(
			"SELECT EXISTS(SELECT 1 FROM branches WHERE id = ?1)",
			params![branch.id],
			|row| row.get(0),
		)?;
		if exists {
			return Err(StoreError::Conflict(format!("branch {} already exists", branch.id)));
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
						"branch point message is not on the declared parent branch ancestry".to_string(),
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
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		let branch_session: Option<String> = transaction
			.query_row("SELECT session_id FROM branches WHERE id = ?1", params![branch_id], |row| {
				row.get(0)
			})
			.optional()?;
		match branch_session {
			None => {
				return Err(StoreError::NotFound(format!("branch {branch_id} does not exist")));
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
			return Err(StoreError::NotFound(format!("session {session_id} does not exist")));
		}
		transaction.commit()?;
		Ok(())
	}

	/// Atomically cut to a new branch: create the new branch, switch active branch,
	/// clear durableProjection from the session payload, and append the branch summary message.
	/// All committed in a single IMMEDIATE transaction.
	pub fn cut_branch(&mut self, branch: NewBranch, summary_message: NewMessage) -> StoreResult<()> {
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
			return Err(StoreError::NotFound(format!("session {} not found", branch.session_id)));
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
						"branch point message is not on the declared parent branch ancestry".to_string(),
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

	/// Atomically append a compaction message and set durableProjection on the session payload.
	/// All committed in a single IMMEDIATE transaction.
	pub fn compact_session(
		&mut self,
		message: NewMessage,
		projection: StoredProjection,
	) -> StoreResult<()> {
		validate_id("compaction message id", &message.id)?;
		validate_id("compaction session id", &message.session_id)?;
		validate_id("compaction branch id", &message.branch_id)?;
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;

		let session_row: Option<String> = transaction
			.query_row(
				"SELECT payload_json FROM sessions WHERE id = ?1",
				params![message.session_id],
				|row| row.get(0),
			)
			.optional()?;
		let Some(session_payload_json) = session_row else {
			return Err(StoreError::NotFound(format!("session {} not found", message.session_id)));
		};

		append_message_tx(&transaction, &message, now)?;

		let mut session_payload: Value = decode_json(&session_payload_json)?;
		let Some(map) = session_payload.as_object_mut() else {
			return Err(StoreError::Corrupt("session payload is not an object".to_string()));
		};
		let mut proj_val = serde_json::to_value(&projection)?;
		crate::auth::error::redact_stored_projection_value(&mut proj_val);
		map.insert("durableProjection".to_string(), proj_val);
		let updated_payload_json = encode_json(&session_payload)?;
		transaction.execute(
			"UPDATE sessions SET payload_json = ?1, updated_at_ms = ?2 WHERE id = ?3",
			params![updated_payload_json, now, message.session_id],
		)?;

		transaction.commit()?;
		Ok(())
	}

	/// Append one message to an explicit branch. The branch leaf advances; the
	/// session's active branch does not change.
	pub fn append_message(&mut self, message: NewMessage) -> StoreResult<()> {
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		append_message_tx(&transaction, &message, now)?;
		transaction.commit()?;
		Ok(())
	}

	/// Append a batch in one transaction. Any failure rolls back every message
	/// and every leaf update in the batch.
	pub fn append_messages(&mut self, messages: &[NewMessage]) -> StoreResult<()> {
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
		row.map(|(seq, id, session_id, branch_id, parent_message_id, payload_json, created_at_ms)| {
			Ok(MessageRecord {
				id,
				session_id,
				branch_id,
				parent_message_id,
				payload: decode_json(&payload_json)?,
				seq,
				created_at_ms,
			})
		})
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
				StoreError::Corrupt(format!("branch {branch_id} references missing message {id}"))
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
		// The intent is replayed after a crash, so it passes the same exact-secret
		// gate as messages: masking happens on the write path only.
		let mut intent_payload = intent.payload;
		crate::auth::error::redact_json_strings(&mut intent_payload);
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
				encode_json(&intent_payload)?,
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
			return Err(StoreError::NotFound(format!("execution {execution_id} does not exist")));
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
		self
			.execution(execution_id)
			.map(|rec| rec.and_then(|r| r.outcome))
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
			let Some(leaf_id) = branch
				.leaf_message_id
				.clone()
				.or_else(|| branch.parent_message_id.clone())
			else {
				continue;
			};
			let Some(leaf_msg) = self.message(&leaf_id)? else {
				continue;
			};

			let is_matching_assistant =
				is_assistant_with_call(&leaf_msg.payload, &exec.provider_call_id);
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

	/// Interrupted-unknown text written for trailing unresolved intents on reopen.
	/// Knowledge message only — never a durable outcome.
	pub const INTERRUPTED_UNKNOWN_OUTCOME: &'static str =
		"Execution interrupted; outcome unknown; not retried.";

	/// Close trailing unanswered tool calls that still have a durable intent but
	/// no outcome. Writes an error `toolResult` on the active branch only; leaves
	/// `outcome_json` NULL so the intent stays unresolved. Missing intents and
	/// calls that already have a result (including filter denials) are unchanged.
	/// Duplicate call ids inside the trailing assistant batch, or a reused
	/// `provider_call_id` that an earlier assistant on this branch also declared,
	/// fail closed — historical unresolved intents must not close a newer
	/// missing/denied call.
	pub fn recover_unresolved_intents(&mut self, session_id: &str) -> StoreResult<usize> {
		validate_id("session id", session_id)?;
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;

		let active_branch_id: Option<String> = transaction
			.query_row(
				"SELECT active_branch_id FROM sessions WHERE id = ?1",
				params![session_id],
				|row| row.get(0),
			)
			.optional()?
			.flatten();
		let Some(active_branch_id) = active_branch_id else {
			transaction.commit()?;
			return Ok(0);
		};

		let branch_messages = branch_messages_tx(&transaction, &active_branch_id)?;
		// Durable open never takes a snapshot first, so recovery is the only gate
		// before the branch is framed. A stored tool result must never borrow an
		// execution — local to this session or copied through `fork_provenance` —
		// that names a different provider call id, and a tool result that
		// references an execution without naming its provider call id fails
		// closed. Metadata entries are not tool results and keep the pre-existing
		// rules; unresolvable references stay with those rules too.
		let mut tool_results: Vec<(&MessageRecord, String, HashSet<String>)> = Vec::new();
		for message in &branch_messages {
			if payload_role(&message.payload) != Some("toolResult") {
				continue;
			}
			let referenced = referenced_execution_ids(std::iter::once(&message.payload));
			if referenced.is_empty() {
				continue;
			}
			let Some(call_id) = extract_tool_result_call_id(&message.payload) else {
				return Err(StoreError::Corrupt(format!(
					"tool result {} references an execution without a toolCallId",
					message.id
				)));
			};
			tool_results.push((message, call_id, referenced));
		}
		if !tool_results.is_empty() {
			let referenced: HashSet<String> = tool_results
				.iter()
				.flat_map(|(_, _, refs)| refs.iter().cloned())
				.collect();
			let executions = executions_by_ids_conn(&transaction, &referenced)?;
			let provenance = fork_provenance_conn(&transaction, session_id)?;
			for (message, call_id, refs) in tool_results {
				for execution_id in refs {
					if let Some(owned) = executions.iter().find(|execution| {
						execution.execution_id == execution_id && execution.session_id == session_id
					}) {
						if owned.provider_call_id != call_id {
							return Err(StoreError::Corrupt(format!(
								"tool result for execution {execution_id} does not carry provider call id {}",
								owned.provider_call_id
							)));
						}
						continue;
					}
					if let Some(copied) = provenance.iter().find(|entry| {
						entry.message_id == message.id && entry.source_execution_id == execution_id
					}) {
						if copied.provider_call_id != call_id {
							return Err(StoreError::Corrupt(format!(
								"fork provenance for message {} names provider call id {} which the tool result does not carry",
								message.id, copied.provider_call_id
							)));
						}
					}
				}
			}
		}
		let Some((asst_idx, asst)) = trailing_assistant_batch(&branch_messages) else {
			transaction.commit()?;
			return Ok(0);
		};

		let call_counts = extract_assistant_tool_call_counts(&asst.payload);
		if call_counts.values().any(|count| *count > 1) {
			return Err(StoreError::Corrupt(format!(
				"ambiguous duplicate toolCallId in trailing assistant batch {}",
				asst.id
			)));
		}
		if call_counts.is_empty() {
			transaction.commit()?;
			return Ok(0);
		}

		let mut answered: HashSet<String> = HashSet::new();
		for msg in &branch_messages[asst_idx + 1..] {
			let Some(call_id) = extract_tool_result_call_id(&msg.payload) else {
				continue;
			};
			if !answered.insert(call_id.clone()) {
				return Err(StoreError::Corrupt(format!(
					"ambiguous duplicate toolResult for {call_id} after assistant {}",
					asst.id
				)));
			}
		}

		let mut unresolved_stmt = transaction.prepare(
			"SELECT execution_id, session_id, branch_id, provider_call_id, tool_name, intent_json, created_at_ms \
			 FROM executions \
			 WHERE session_id = ?1 AND branch_id = ?2 AND outcome_json IS NULL \
			 ORDER BY seq",
		)?;
		let unresolved_rows =
			unresolved_stmt.query_map(params![session_id, active_branch_id], |row| {
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
		let mut intents_by_call: HashMap<String, Vec<UnresolvedIntent>> = HashMap::new();
		for row in unresolved_rows {
			let (execution_id, sess, branch_id, provider_call_id, tool_name, json, created_at_ms) =
				row?;
			intents_by_call
				.entry(provider_call_id.clone())
				.or_default()
				.push(UnresolvedIntent {
					execution_id,
					session_id: sess,
					branch_id,
					provider_call_id,
					tool_name,
					payload: decode_json(&json)?,
					created_at_ms,
				});
		}
		drop(unresolved_stmt);

		let mut recovered_count = 0usize;
		let mut leaf_id = branch_messages
			.last()
			.map(|message| message.id.clone())
			.or_else(|| {
				transaction
					.query_row(
						"SELECT parent_message_id FROM branches WHERE id = ?1",
						params![active_branch_id],
						|row| row.get::<_, Option<String>>(0),
					)
					.optional()
					.ok()
					.flatten()
					.flatten()
			})
			.unwrap_or_else(|| asst.id.clone());

		// Stable order: assistant declaration order.
		let ordered_calls: Vec<String> = {
			let mut seen = HashSet::new();
			let mut ordered = Vec::new();
			for part_id in assistant_tool_call_ids_in_order(&asst.payload) {
				if seen.insert(part_id.clone()) {
					ordered.push(part_id);
				}
			}
			ordered
		};

		for call_id in ordered_calls {
			if answered.contains(&call_id) {
				continue;
			}
			let Some(matches) = intents_by_call.get(&call_id) else {
				// Missing intent: leave dangling for seed refuse (unchanged).
				continue;
			};
			if matches.len() != 1 {
				return Err(StoreError::Corrupt(format!(
					"ambiguous unresolved intents for toolCallId {call_id}"
				)));
			}
			// Reused provider ids across turns are not uniquely attributable to
			// this trailing batch; never synthesize a result from a historical
			// unresolved intent onto a newer missing/denied call.
			if earlier_unanswered_declaration(&transaction, &branch_messages, asst_idx, &call_id)? {
				return Err(StoreError::Corrupt(format!(
					"unresolved intent for reused toolCallId {call_id} is not uniquely attributable to trailing assistant {}; recovery refused",
					asst.id
				)));
			}
			let intent = &matches[0];

			// Recheck: still unresolved inside the write lock.
			let outcome_still_null: bool = transaction.query_row(
				"SELECT outcome_json IS NULL FROM executions WHERE execution_id = ?1",
				params![intent.execution_id],
				|row| row.get(0),
			)?;
			if !outcome_still_null {
				continue;
			}

			let msg_id = format!("msg-int-{}", intent.execution_id);
			let already: bool = transaction.query_row(
				"SELECT EXISTS(SELECT 1 FROM messages WHERE id = ?1)",
				params![msg_id],
				|row| row.get(0),
			)?;
			if already {
				answered.insert(call_id);
				continue;
			}

			let (current_leaf, parent_point): (Option<String>, Option<String>) = transaction
				.query_row(
					"SELECT leaf_message_id, parent_message_id FROM branches WHERE id = ?1",
					params![active_branch_id],
					|row| Ok((row.get(0)?, row.get(1)?)),
				)?;
			if current_leaf.or(parent_point).as_deref() != Some(leaf_id.as_str()) {
				return Err(StoreError::Corrupt(format!(
					"active branch effective leaf changed during unresolved intent recovery for session {session_id}"
				)));
			}

			let iso_ts = crate::output::iso8601(SystemTime::now());
			let sess_msg = json!({
				"role": "toolResult",
				"toolCallId": intent.provider_call_id,
				"toolName": intent.tool_name,
				"content": Self::INTERRUPTED_UNKNOWN_OUTCOME,
				"isError": true,
				"timestamp": now,
				"details": {
					"executionId": intent.execution_id,
					"interruptedUnknown": true,
				},
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
				session_id: session_id.to_string(),
				branch_id: active_branch_id.clone(),
				parent_message_id: Some(leaf_id.clone()),
				payload: entry,
			};
			append_message_tx(&transaction, &new_msg, now)?;
			// Leave durableProjection.kept untouched. A mixed batch may already
			// have sibling toolResults on the branch that are not yet in kept
			// (crash after finish_execution, before set_projection). Pushing only
			// the synthetic leaf into kept would hide those siblings from
			// reconcile's suffix backfill. `reconcile_durable_projection` runs
			// next on open and appends every suffix message in branch order.
			answered.insert(call_id);
			leaf_id = msg_id;
			recovered_count += 1;
		}

		transaction.commit()?;
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
			let (
				execution_id,
				session_id,
				branch_id,
				provider_call_id,
				tool_name,
				json,
				created_at_ms,
			) = row?;
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
		// Mask before encoding and before the idempotency comparisons below: a
		// repeat carries the raw payload while the stored row is masked. Keep
		// the raw form so a repeat of a pre-redaction row stays idempotent too.
		let raw_completion = completion.completion.clone();
		let raw_delivery = completion.delivery.clone();
		let mut completion = completion;
		crate::auth::error::redact_json_strings(&mut completion.completion);
		crate::auth::error::redact_json_strings(&mut completion.delivery);
		let completion_json = encode_json(&completion.completion)?;
		let delivery_json = encode_json(&completion.delivery)?;
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;

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
		if let Some((session_id, origin_branch_id, delivery_id, stored_completion)) =
			existing_completion
		{
			let stored_completion = decode_json(&stored_completion)?;
			if session_id != completion.session_id
				|| origin_branch_id != completion.origin_branch_id
				|| delivery_id != completion.delivery_id
				|| (stored_completion != completion.completion && stored_completion != raw_completion)
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
			let stored_delivery_json = decode_json(&stored_delivery_json)?;
			if delivery_worker_id != completion.worker_id
				|| delivery_session_id != completion.session_id
				|| delivery_origin_branch_id != completion.origin_branch_id
				|| (stored_delivery_json != completion.delivery && stored_delivery_json != raw_delivery)
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
			let stored_delivery = decode_json(&stored_delivery)?;
			if worker_id != completion.worker_id
				|| session_id != completion.session_id
				|| origin_branch_id != completion.origin_branch_id
				|| (stored_delivery != completion.delivery && stored_delivery != raw_delivery)
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

		let is_sync = completion
			.delivery
			.get("notify_parent")
			.and_then(Value::as_bool)
			.map(|b| !b)
			.unwrap_or_else(|| {
				completion.delivery.get("mode").and_then(Value::as_str) == Some("sync")
			});
		let (delivered_at, processed_at) = if is_sync {
			(Some(now), Some(now))
		} else {
			(None, None)
		};
		// Delivery first so the completion's foreign key is satisfied inside the
		// same transaction. Both rows commit or neither does.
		transaction.execute(
			"INSERT INTO deliveries \
			 (delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms, message_id) \
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)",
			params![
				completion.delivery_id,
				completion.worker_id,
				completion.session_id,
				completion.origin_branch_id,
				delivery_json,
				now,
				delivered_at,
				processed_at,
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

	/// Reserve a child task before its side effect. A caller must not spawn,
	/// start, or call a model before this returns `Ok`; a crash between this
	/// write and the effect leaves an `admitted` row, never unreported work.
	pub fn admit_task(&mut self, admission: NewTaskAdmission) -> StoreResult<()> {
		validate_id("task id", &admission.task_id)?;
		validate_id("parent session id", &admission.parent_session_id)?;
		validate_id("parent branch id", &admission.parent_branch_id)?;
		validate_id("worker id", &admission.worker_id)?;
		let generation = i64::try_from(admission.producer_generation)
			.map_err(|_| StoreError::Validation("producer generation exceeds i64::MAX".to_string()))?;
		let mut spec = admission.spec;
		crate::auth::error::redact_json_strings(&mut spec);
		let spec_json = encode_json(&spec)?;
		let now = now_ms();
		self.connection.execute(
			"INSERT INTO task_admissions \
			 (task_id, parent_session_id, parent_branch_id, worker_id, producer_generation, \
			  state, spec_json, created_at_ms, updated_at_ms) \
			 VALUES (?1, ?2, ?3, ?4, ?5, 'admitted', ?6, ?7, ?7)",
			params![
				admission.task_id,
				admission.parent_session_id,
				admission.parent_branch_id,
				admission.worker_id,
				generation,
				spec_json,
				now
			],
		)?;
		Ok(())
	}

	/// Record that the admitted task's side effect was requested.
	pub fn mark_task_admission_started(&mut self, task_id: &str) -> StoreResult<()> {
		validate_id("task id", task_id)?;
		let changed = self.connection.execute(
			"UPDATE task_admissions SET state = 'started', updated_at_ms = ?2 \
			 WHERE task_id = ?1 AND state = 'admitted'",
			params![task_id, now_ms()],
		)?;
		if changed == 1 {
			return Ok(());
		}
		match self.task_admission(task_id)? {
			Some(row) => {
				Err(StoreError::Conflict(format!("task admission is already in state '{}'", row.state)))
			},
			None => Err(StoreError::NotFound(format!("unknown task admission '{task_id}'"))),
		}
	}

	/// Write a terminal state. `completed`, `failed`, and `abandoned` are the
	/// only accepted values; an already terminal row is a conflict, not a
	/// silent overwrite.
	pub fn finish_task_admission(&mut self, task_id: &str, state: &str) -> StoreResult<()> {
		validate_id("task id", task_id)?;
		if !matches!(state, "completed" | "failed" | "abandoned") {
			return Err(StoreError::Validation(format!(
				"task admission terminal state must be completed, failed, or abandoned, got '{state}'"
			)));
		}
		let changed = self.connection.execute(
			"UPDATE task_admissions SET state = ?2, updated_at_ms = ?3 \
			 WHERE task_id = ?1 AND state IN ('admitted', 'started')",
			params![task_id, state, now_ms()],
		)?;
		if changed == 1 {
			return Ok(());
		}
		match self.task_admission(task_id)? {
			Some(row) => Err(StoreError::Conflict(format!(
				"task admission is already terminal in state '{}'",
				row.state
			))),
			None => Err(StoreError::NotFound(format!("unknown task admission '{task_id}'"))),
		}
	}

	/// One admission row by task id.
	pub fn task_admission(&self, task_id: &str) -> StoreResult<Option<TaskAdmission>> {
		let row: Option<(String, String, String, String, i64, String, String, i64, i64)> = self
			.connection
			.query_row(
				"SELECT task_id, parent_session_id, parent_branch_id, worker_id, \
				 producer_generation, state, spec_json, created_at_ms, updated_at_ms \
				 FROM task_admissions WHERE task_id = ?1",
				params![task_id],
				|row| {
					Ok((
						row.get(0)?,
						row.get(1)?,
						row.get(2)?,
						row.get(3)?,
						row.get(4)?,
						row.get(5)?,
						row.get(6)?,
						row.get(7)?,
						row.get(8)?,
					))
				},
			)
			.optional()?;
		row.map(
			|(
				task_id,
				parent_session_id,
				parent_branch_id,
				worker_id,
				producer_generation,
				state,
				spec_json,
				created_at_ms,
				updated_at_ms,
			)| {
				Ok(TaskAdmission {
					task_id,
					parent_session_id,
					parent_branch_id,
					worker_id,
					producer_generation: u64::try_from(producer_generation).unwrap_or(0),
					state,
					spec: decode_json(&spec_json)?,
					created_at_ms,
					updated_at_ms,
				})
			},
		)
		.transpose()
	}

	/// Admissions that were never closed, oldest first. A crash leaves these
	/// rows behind; recovery decides whether to resume or mark them abandoned.
	pub fn open_task_admissions(&self, session_id: &str) -> StoreResult<Vec<TaskAdmission>> {
		let mut statement = self.connection.prepare(
			"SELECT task_id, parent_session_id, parent_branch_id, worker_id, \
			 producer_generation, state, spec_json, created_at_ms, updated_at_ms \
			 FROM task_admissions \
			 WHERE parent_session_id = ?1 AND state IN ('admitted', 'started') \
			 ORDER BY created_at_ms, task_id",
		)?;
		let rows = statement.query_map(params![session_id], |row| {
			Ok((
				row.get::<_, String>(0)?,
				row.get::<_, String>(1)?,
				row.get::<_, String>(2)?,
				row.get::<_, String>(3)?,
				row.get::<_, i64>(4)?,
				row.get::<_, String>(5)?,
				row.get::<_, String>(6)?,
				row.get::<_, i64>(7)?,
				row.get::<_, i64>(8)?,
			))
		})?;
		let mut admissions = Vec::new();
		for row in rows {
			let (
				task_id,
				parent_session_id,
				parent_branch_id,
				worker_id,
				producer_generation,
				state,
				spec_json,
				created_at_ms,
				updated_at_ms,
			) = row?;
			admissions.push(TaskAdmission {
				task_id,
				parent_session_id,
				parent_branch_id,
				worker_id,
				producer_generation: u64::try_from(producer_generation).unwrap_or(0),
				state,
				spec: decode_json(&spec_json)?,
				created_at_ms,
				updated_at_ms,
			});
		}
		Ok(admissions)
	}

	/// Record the producer epoch that just acquired writer ownership of
	/// `session_id`. Kept beside task admissions so a persisted child contract
	/// can prove the session it replays belongs to this lineage.
	pub fn set_session_producer_generation(
		&mut self,
		session_id: &str,
		generation: u64,
	) -> StoreResult<()> {
		validate_id("session id", session_id)?;
		let generation = i64::try_from(generation).map_err(|_| {
			StoreError::Validation("producer generation is out of range".to_string())
		})?;
		let changed = self.connection.execute(
			"UPDATE sessions SET producer_generation = ?2 WHERE id = ?1",
			params![session_id, generation],
		)?;
		if changed == 0 {
			return Err(StoreError::NotFound(format!("session {session_id} does not exist")));
		}
		Ok(())
	}

	/// Producer epoch recorded for `session_id`. `None` means the session
	/// predates v7 or has not been opened since; a missing session is an error.
	pub fn session_producer_generation(&self, session_id: &str) -> StoreResult<Option<u64>> {
		validate_id("session id", session_id)?;
		let recorded: Option<Option<i64>> = self
			.connection
			.query_row(
				"SELECT producer_generation FROM sessions WHERE id = ?1",
				params![session_id],
				|row| row.get(0),
			)
			.optional()?;
		match recorded {
			None => Err(StoreError::NotFound(format!("session {session_id} does not exist"))),
			Some(None) => Ok(None),
			Some(Some(value)) => match u64::try_from(value) {
				Ok(value) => Ok(Some(value)),
				Err(_) => Err(StoreError::Corrupt(format!(
					"session {session_id} has bad producer generation"
				))),
			},
		}
	}

	/// Next producer generation for a session: one above every generation this
	/// table has ever stored. Reopening a session therefore never reuses an
	/// earlier epoch, and a session with no admissions starts at 1.
	pub fn next_producer_generation(&self, session_id: &str) -> StoreResult<u64> {
		validate_id("parent session id", session_id)?;
		let max: Option<i64> = self.connection.query_row(
			"SELECT MAX(producer_generation) FROM task_admissions WHERE parent_session_id = ?1",
			params![session_id],
			|row| row.get(0),
		)?;
		Ok(u64::try_from(max.unwrap_or(0))
			.unwrap_or(0)
			.saturating_add(1))
	}

	/// Recovery for a session whose writer ownership was just acquired: any
	/// admission still `admitted` or `started` belonged to a producer that no
	/// longer owns the session, so its outcome is unknown and it is closed as
	/// `abandoned` — never replayed. Returns how many rows changed.
	pub fn abandon_open_task_admissions(&mut self, session_id: &str) -> StoreResult<usize> {
		validate_id("parent session id", session_id)?;
		Ok(self.connection.execute(
			"UPDATE task_admissions SET state = 'abandoned', updated_at_ms = ?2 \
			 WHERE parent_session_id = ?1 AND state IN ('admitted', 'started')",
			params![session_id, now_ms()],
		)?)
	}

	/// Insert a pending delivery that has no worker completion (peer mail).
	/// Repeating the same id/payload is idempotent; conflicting reuse is
	/// rejected. Enqueue never marks the delivery delivered or processed.
	pub fn enqueue_delivery(&mut self, delivery: NewDelivery) -> StoreResult<()> {
		validate_id("delivery id", &delivery.delivery_id)?;
		validate_id("worker id", &delivery.worker_id)?;
		validate_id("delivery session id", &delivery.session_id)?;
		validate_id("delivery origin branch id", &delivery.origin_branch_id)?;
		let mut delivery = delivery;
		let raw_payload = delivery.payload.clone();
		crate::auth::error::redact_json_strings(&mut delivery.payload);
		let payload_json = encode_json(&delivery.payload)?;
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
			let stored = decode_json(&stored)?;
			if worker_id != delivery.worker_id
				|| session_id != delivery.session_id
				|| origin_branch_id != delivery.origin_branch_id
				|| (stored != delivery.payload && stored != raw_payload)
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
			 (delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms, message_id) \
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL)",
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
		mut message: NewMessage,
		at_ms: i64,
	) -> StoreResult<()> {
		validate_id("delivery id", delivery_id)?;
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		let delivery: Option<(
			String,
			String,
			Option<i64>,
			Option<i64>,
			Option<String>,
			String,
			String,
		)> = transaction
			.query_row(
				"SELECT session_id, origin_branch_id, delivered_at_ms, processed_at_ms, message_id, payload_json, worker_id \
				 FROM deliveries WHERE delivery_id = ?1",
				params![delivery_id],
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
		let Some((
			session_id,
			origin_branch_id,
			delivered_at,
			_processed_at,
			message_id,
			payload_json,
			delivery_worker_id,
		)) = delivery
		else {
			return Err(StoreError::NotFound(format!("delivery {delivery_id} does not exist")));
		};
		if session_id != message.session_id {
			return Err(StoreError::Validation(
				"delivery message belongs to a different session".to_string(),
			));
		}

		// Origin branch must exist and belong to session
		let origin_session: Option<String> = transaction
			.query_row(
				"SELECT session_id FROM branches WHERE id = ?1",
				params![origin_branch_id],
				|row| row.get(0),
			)
			.optional()?;
		match &origin_session {
			None => {
				return Err(StoreError::NotFound(format!(
					"origin branch {origin_branch_id} does not exist"
				)));
			},
			Some(s) if s != &session_id => {
				return Err(StoreError::Validation(
					"origin branch belongs to a different session".to_string(),
				));
			},
			Some(_) => {},
		}

		// Check worker completion if this is a worker delivery
		let wc: Option<(String, String)> = transaction
			.query_row(
				"SELECT worker_id, completion_json FROM worker_completions WHERE delivery_id = ?1",
				params![delivery_id],
				|row| Ok((row.get(0)?, row.get(1)?)),
			)
			.optional()?;
		if let Some((wc_worker_id, _wc_json)) = wc {
			if wc_worker_id != delivery_worker_id {
				return Err(StoreError::Corrupt(format!(
					"delivery {delivery_id} worker {delivery_worker_id} does not match completion worker {wc_worker_id}"
				)));
			}
			let parsed_payload: Value = decode_json(&payload_json)?;
			if parsed_payload.get("mode").and_then(Value::as_str) == Some("sync")
				|| parsed_payload.get("notify_parent").and_then(Value::as_bool) == Some(false)
			{
				return Err(StoreError::Validation(format!(
					"delivery {delivery_id} is a synchronous worker result; cannot be consumed as background delivery"
				)));
			}
			// Fail-closed on legacy ambiguous rows
			if parsed_payload
				.get("version")
				.and_then(Value::as_i64)
				.unwrap_or(0)
				< 2 && parsed_payload.get("notify_parent").is_none()
			{
				return Err(StoreError::Corrupt(format!(
					"delivery {delivery_id} has ambiguous legacy worker payload; failing closed"
				)));
			}
		}

		// Idempotency / conflict check on already-delivered delivery
		if delivered_at.is_some() {
			let Some(stored_msg_id) = message_id else {
				return Err(StoreError::Corrupt(format!(
					"delivery {delivery_id} marked delivered without recorded message id (ambiguous legacy state)"
				)));
			};
			if stored_msg_id == message.id {
				let stored_message: Option<(String, String, String)> = transaction
					.query_row(
						"SELECT session_id, branch_id, payload_json FROM messages WHERE id = ?1",
						params![stored_msg_id],
						|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
					)
					.optional()?;
				if let Some((stored_session, stored_branch, stored_payload)) = stored_message {
					if stored_session == message.session_id
						&& (message.branch_id.is_empty() || message.branch_id == stored_branch)
						&& decode_json(&stored_payload)? == message.payload
					{
						transaction.commit()?;
						return Ok(());
					}
				}
				return Err(StoreError::Conflict(format!(
					"delivery {delivery_id} already consumed with different message data"
				)));
			}
			return Err(StoreError::Conflict(format!(
				"delivery {delivery_id} was already appended to the target context with message {stored_msg_id}"
			)));
		}

		// Pending delivery checks
		if message_id.is_some() {
			return Err(StoreError::Corrupt(format!(
				"delivery {delivery_id} has message_id without delivered_at_ms"
			)));
		}
		let existing_message: Option<String> = transaction
			.query_row("SELECT id FROM messages WHERE id = ?1", params![message.id], |row| row.get(0))
			.optional()?;
		if existing_message.is_some() {
			return Err(StoreError::Conflict(format!(
				"message {} already exists for pending delivery {}",
				message.id, delivery_id
			)));
		}

		// Target active branch check
		let active_branch_id: String = transaction.query_row(
			"SELECT active_branch_id FROM sessions WHERE id = ?1",
			params![message.session_id],
			|row| row.get(0),
		)?;
		if message.branch_id.is_empty() {
			message.branch_id = active_branch_id.clone();
		} else if message.branch_id != active_branch_id {
			return Err(StoreError::Validation(format!(
				"delivery target branch {} is not active branch {}",
				message.branch_id, active_branch_id
			)));
		}

		// Determine leaf and chain inside transaction
		let (branch_parent, branch_leaf): (Option<String>, Option<String>) = transaction.query_row(
			"SELECT parent_message_id, leaf_message_id FROM branches WHERE id = ?1",
			params![active_branch_id],
			|row| Ok((row.get(0)?, row.get(1)?)),
		)?;
		let expected_parent = branch_leaf.or(branch_parent);
		if message.parent_message_id.is_none() {
			message.parent_message_id = expected_parent.clone();
		} else if message.parent_message_id != expected_parent {
			return Err(StoreError::Validation(format!(
				"message {} must chain to {:?}, not {:?}",
				message.id, expected_parent, message.parent_message_id
			)));
		}

		append_message_tx(&transaction, &message, at_ms)?;
		let changes = transaction.execute(
			"UPDATE deliveries SET delivered_at_ms = ?1, message_id = ?2 WHERE delivery_id = ?3 AND delivered_at_ms IS NULL",
			params![at_ms, message.id, delivery_id],
		)?;
		if changes == 0 {
			return Err(StoreError::Conflict(format!(
				"delivery {delivery_id} was already appended to the target context"
			)));
		}
		transaction.commit()?;
		Ok(())
	}

	/// One delivery row by id, including delivered/processed state.
	pub fn delivery(&self, delivery_id: &str) -> StoreResult<Option<DeliveryRecord>> {
		validate_id("delivery id", delivery_id)?;
		let mut records = self.query_deliveries(
			"SELECT delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms, message_id \
			 FROM deliveries WHERE delivery_id = ?1",
			delivery_id,
		)?;
		Ok(records.pop())
	}

	/// Deliveries not yet durably offered to the target context.
	pub fn pending_deliveries(&self, session_id: &str) -> StoreResult<Vec<DeliveryRecord>> {
		self.query_deliveries(
			"SELECT delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms, message_id \
			 FROM deliveries WHERE session_id = ?1 AND delivered_at_ms IS NULL ORDER BY created_at_ms, delivery_id",
			session_id,
		)
	}

	/// Worker completions with undelivered parent notifications in `session_id`.
	/// Excludes peer deliveries and non-worker deliveries by joining with
	/// `worker_completions`.
	pub fn pending_worker_deliveries(&self, session_id: &str) -> StoreResult<Vec<DeliveryRecord>> {
		self.query_deliveries(
			"SELECT d.delivery_id, d.worker_id, d.session_id, d.origin_branch_id, d.payload_json, \
			        d.created_at_ms, d.delivered_at_ms, d.processed_at_ms, d.message_id \
			 FROM deliveries d \
			 JOIN worker_completions wc ON wc.delivery_id = d.delivery_id AND wc.session_id = d.session_id \
			 WHERE d.session_id = ?1 AND d.delivered_at_ms IS NULL \
			 ORDER BY d.created_at_ms, d.delivery_id",
			session_id,
		)
	}

	/// Peer mail (`payload.kind == "peer-message"`) not yet durably offered to
	/// the target context. Worker completions are excluded: they have their own
	/// recovery path and payload contract.
	pub fn pending_peer_deliveries(&self, session_id: &str) -> StoreResult<Vec<DeliveryRecord>> {
		let records = self.query_deliveries(
			"SELECT delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms, message_id \
			 FROM deliveries WHERE session_id = ?1 AND delivered_at_ms IS NULL ORDER BY created_at_ms, delivery_id",
			session_id,
		)?;
		Ok(records
			.into_iter()
			.filter(|record| record.payload.get("kind").and_then(Value::as_str) == Some("peer-message"))
			.collect())
	}

	/// Remove a still-pending delivery. Used when the send that enqueued it
	/// failed before any recipient could accept it, so a later resume does not
	/// replay a message no one received. Delivered rows are never withdrawn.
	pub fn withdraw_pending_delivery(&mut self, delivery_id: &str) -> StoreResult<bool> {
		validate_id("delivery id", delivery_id)?;
		let changes = self.connection.execute(
			"DELETE FROM deliveries WHERE delivery_id = ?1 AND delivered_at_ms IS NULL",
			params![delivery_id],
		)?;
		Ok(changes > 0)
	}

	/// Deliveries already offered to the target context but not confirmed
	/// processed. These are crash-recovery candidates, not automatic retries.
	pub fn unprocessed_deliveries(&self, session_id: &str) -> StoreResult<Vec<DeliveryRecord>> {
		self.query_deliveries(
			"SELECT delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms, message_id \
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
				row.get::<_, Option<String>>(8)?,
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
				message_id,
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
				message_id,
			});
		}
		Ok(deliveries)
	}

	/// Mark a delivery as offered to the target context. Repeating the same mark
	/// is idempotent; processed state is not implied.
	pub fn mark_delivery_delivered(&mut self, delivery_id: &str, at_ms: i64) -> StoreResult<()> {
		validate_id("delivery id", delivery_id)?;
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		let state: Option<(Option<i64>, Option<i64>)> = transaction
			.query_row(
				"SELECT delivered_at_ms, processed_at_ms FROM deliveries WHERE delivery_id = ?1",
				params![delivery_id],
				|row| Ok((row.get(0)?, row.get(1)?)),
			)
			.optional()?;
		let Some((delivered_at, processed_at)) = state else {
			return Err(StoreError::NotFound(format!("delivery {delivery_id} does not exist")));
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
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		let state: Option<(Option<i64>, Option<i64>)> = transaction
			.query_row(
				"SELECT delivered_at_ms, processed_at_ms FROM deliveries WHERE delivery_id = ?1",
				params![delivery_id],
				|row| Ok((row.get(0)?, row.get(1)?)),
			)
			.optional()?;
		let Some((delivered_at, processed_at)) = state else {
			return Err(StoreError::NotFound(format!("delivery {delivery_id} does not exist")));
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
		let mut artifact = artifact;
		let raw_metadata = artifact.metadata.clone();
		crate::auth::error::redact_json_strings(&mut artifact.metadata);
		let metadata_json = encode_json(&artifact.metadata)?;
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;
		require_session(&transaction, &artifact.session_id)?;
		if let Some(branch_id) = &artifact.branch_id {
			let branch_session: Option<String> = transaction
				.query_row("SELECT session_id FROM branches WHERE id = ?1", params![branch_id], |row| {
					row.get(0)
				})
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
				|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
			)
			.optional()?;
		if let Some((session_id, branch_id, execution_id, location, stored_metadata)) = existing {
			let stored_metadata = decode_json(&stored_metadata)?;
			if session_id != artifact.session_id
				|| branch_id != artifact.branch_id
				|| execution_id != artifact.execution_id
				|| location != artifact.location
				|| (stored_metadata != artifact.metadata && stored_metadata != raw_metadata)
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
			let (id, parent_session_id, active_branch_id, json, created_at_ms, updated_at_ms) = row?;
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
			let (
				id,
				session_id,
				parent_branch_id,
				parent_message_id,
				leaf_message_id,
				json,
				created_at_ms,
			) = row?;
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
	pub fn update_session_payload(&mut self, session_id: &str, payload: Value) -> StoreResult<()> {
		validate_id("session id", session_id)?;
		let mut payload = payload;
		crate::auth::error::redact_session_payload(&mut payload);
		let encoded = encode_json(&payload)?;
		let changed = self.connection.execute(
			"UPDATE sessions SET payload_json = ?1, updated_at_ms = ?2 WHERE id = ?3",
			params![encoded, now_ms(), session_id],
		)?;
		if changed == 0 {
			return Err(StoreError::NotFound(format!("session {session_id} does not exist")));
		}
		Ok(())
	}

	/// Overwrite the branch-scoped stream draft for `attempt`.
	///
	/// Empty text clears the draft. When the stored current draft belongs to
	/// a *different* attempt, it is preserved as a prior fragment instead of
	/// being overwritten: a new stream starting on the same branch must never
	/// destroy the recovered draft of an older interrupted stream before its
	/// terminal transition. Priors are capped (`MAX_STREAM_PRIORS`); overflow
	/// merges the two oldest into one entry so no fragment text is silently
	/// discarded. Drafts carry assistant text only, never tool calls, and
	/// are invisible to every replay projection.
	pub fn save_stream_draft(
		&mut self,
		session_id: &str,
		branch_id: &str,
		text: &str,
		attempt: &str,
	) -> StoreResult<()> {
		validate_id("session id", session_id)?;
		validate_id("branch id", branch_id)?;
		require_branch_owner(&self.connection, session_id, branch_id)?;
		Self::require_stream_drafts_table(&self.connection)?;
		let redacted_text = crate::auth::error::redact_free_text(text);
		if redacted_text.is_empty() {
			return self.clear_stream_draft(session_id, branch_id);
		}
		let existing = Self::read_stream_draft_row(&self.connection, session_id, branch_id)?;
		let mut priors = existing
			.as_ref()
			.and_then(|row| row.get("priors").cloned())
			.filter(|value| value.is_array())
			.unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
		if let Some(list) = priors.as_array_mut() {
			for entry in list.iter_mut() {
				if let Some(prior_text) = entry
					.get("text")
					.and_then(Value::as_str)
					.map(crate::auth::error::redact_free_text)
				{
					entry["text"] = Value::String(prior_text);
				}
			}
		}
		// Legacy single-text rows (pre attempt-scoped payloads) carry no
		// attempt: preserve the text as a prior so the first attempt-scoped
		// save never destroys it.
		if priors.as_array().is_some_and(|list| list.is_empty())
			&& existing
				.as_ref()
				.and_then(|row| row.get("current"))
				.is_none_or(|current| {
					current.is_null()
						|| current
							.get("text")
							.and_then(Value::as_str)
							.is_none_or(|text| text.is_empty())
				}) && let Some(legacy) = existing
			.as_ref()
			.and_then(|row| row.get("text"))
			.and_then(Value::as_str)
			.filter(|text| !text.is_empty())
			&& let Some(list) = priors.as_array_mut()
		{
			list.push(serde_json::json!({
				"text": crate::auth::error::redact_free_text(legacy),
				"attempt": "legacy",
			}));
		}
		if let Some(current) = existing.as_ref().and_then(|row| row.get("current")) {
			let current_attempt = current.get("attempt").and_then(Value::as_str);
			if current_attempt != Some(attempt)
				&& let Some(current_text) = current
					.get("text")
					.and_then(Value::as_str)
					.filter(|text| !text.is_empty())
				&& let Some(list) = priors.as_array_mut()
			{
				// A new stream on this branch supersedes a stored draft from
				// an older interrupted stream: preserve it as a prior
				// fragment. It clears with the new attempt's terminal, after
				// being delivered as interrupted context.
				let mut prior = current.clone();
				prior["text"] = Value::String(crate::auth::error::redact_free_text(current_text));
				list.push(prior);
				// Bound the row without silent loss: merge oldest priors into
				// one entry so no fragment text is discarded.
				while list.len() > Self::MAX_STREAM_PRIORS {
					let first = list.remove(0);
					let second = list.remove(0);
					let first_text = first.get("text").and_then(Value::as_str).unwrap_or("");
					let second_text = second.get("text").and_then(Value::as_str).unwrap_or("");
					let merged =
						format!("{first_text}\n\n--- interrupted stream fragment ---\n\n{second_text}");
					list.insert(0, serde_json::json!({ "text": merged, "attempt": "merged" }));
				}
			}
		}
		let payload = serde_json::json!({
			"current": { "text": redacted_text, "attempt": attempt },
			"priors": priors,
			"stopReason": "aborted",
			"interrupted": true,
			"partial": true,
		});
		let now = now_ms();
		self.connection.execute(
			"INSERT INTO stream_drafts (branch_id, session_id, payload_json, updated_at_ms) \
			 VALUES (?1, ?2, ?3, ?4) \
			 ON CONFLICT(branch_id) DO UPDATE SET session_id = excluded.session_id, \
			 payload_json = excluded.payload_json, updated_at_ms = excluded.updated_at_ms",
			params![branch_id, session_id, encode_json(&payload)?, now],
		)?;
		Ok(())
	}

	/// Drop the branch-scoped stream draft, if any.
	pub fn clear_stream_draft(&mut self, session_id: &str, branch_id: &str) -> StoreResult<()> {
		validate_id("session id", session_id)?;
		validate_id("branch id", branch_id)?;
		require_branch_owner(&self.connection, session_id, branch_id)?;
		Self::require_stream_drafts_table(&self.connection)?;
		self.connection.execute(
			"DELETE FROM stream_drafts WHERE branch_id = ?1 AND session_id = ?2",
			params![branch_id, session_id],
		)?;
		Ok(())
	}

	/// Read the branch-scoped stream draft text, if any.
	pub fn stream_draft(&self, session_id: &str, branch_id: &str) -> StoreResult<Option<String>> {
		validate_id("session id", session_id)?;
		validate_id("branch id", branch_id)?;
		if !table_exists_conn(&self.connection, "stream_drafts")? {
			// Read-only snapshots of pre-v4 databases legitimately have no
			// draft table. A v4 database without it is damaged: fail closed
			// instead of pretending no fragment exists.
			let version: Option<i64> = self
				.connection
				.query_row("SELECT version FROM schema_version WHERE id = 1", [], |row| row.get(0))
				.optional()?;
			return match version {
				Some(version) if version >= 4 => Err(StoreError::Schema(
					"stream_drafts table is missing; database schema is damaged".to_string(),
				)),
				_ => Ok(None),
			};
		}
		let row = Self::read_stream_draft_row(&self.connection, session_id, branch_id)?;
		let Some(payload) = row else {
			return Ok(None);
		};
		Ok(Some(combined_stream_draft_text(&payload)).filter(|text| !text.is_empty()))
	}
	/// Fail closed when the draft table is missing on a writable handle.
	/// Writable opens always run migrations, so a missing table is a damaged
	/// schema, not an old database.
	fn require_stream_drafts_table(connection: &Connection) -> StoreResult<()> {
		if table_exists_conn(connection, "stream_drafts")? {
			return Ok(());
		}
		Err(StoreError::Schema(
			"stream_drafts table is missing; database schema is damaged".to_string(),
		))
	}

	/// Read the raw draft payload row, if any. Returns `Ok(None)` when the
	/// row is absent. On a read-only snapshot of a pre-v4 database the table
	/// itself may legitimately be absent (`Ok(None)`); a v4 database without
	/// it is damaged and fails closed.
	fn read_stream_draft_row(
		connection: &Connection,
		session_id: &str,
		branch_id: &str,
	) -> StoreResult<Option<Value>> {
		if !table_exists_conn(connection, "stream_drafts")? {
			let version: Option<i64> = connection
				.query_row("SELECT version FROM schema_version WHERE id = 1", [], |row| row.get(0))
				.optional()?;
			return match version {
				Some(version) if version >= 4 => Err(StoreError::Schema(
					"stream_drafts table is missing; database schema is damaged".to_string(),
				)),
				_ => Ok(None),
			};
		}
		let row: Option<String> = connection
			.query_row(
				"SELECT payload_json FROM stream_drafts WHERE branch_id = ?1 AND session_id = ?2",
				params![branch_id, session_id],
				|row| row.get(0),
			)
			.optional()?;
		row.map(|raw| decode_json(&raw)).transpose()
	}

	/// Amend the terminal stop reason and optional error message of an assistant message row.
	pub fn update_message_terminal(
		&mut self,
		session_id: &str,
		branch_id: &str,
		message_id: Option<&str>,
		stop_reason: &str,
		error_message: Option<&str>,
	) -> StoreResult<String> {
		validate_id("session id", session_id)?;
		validate_id("branch id", branch_id)?;
		if let Some(id) = message_id {
			validate_id("message id", id)?;
		}
		let now = now_ms();
		let transaction = self
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)?;

		let branch_row: Option<(String, Option<String>, Option<String>)> = transaction
			.query_row(
				"SELECT session_id, parent_message_id, leaf_message_id FROM branches WHERE id = ?1",
				params![branch_id],
				|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
			)
			.optional()?;
		let Some((b_session, parent_message_id, leaf_message_id)) = branch_row else {
			return Err(StoreError::NotFound(format!("branch {branch_id} does not exist")));
		};
		if b_session != session_id {
			return Err(StoreError::Validation(format!(
				"branch {branch_id} belongs to session {b_session}, not {session_id}"
			)));
		}

		let (target_id, raw_payload) = if let Some(id) = message_id {
			let row: Option<(String, String)> = transaction
				.query_row(
					"SELECT branch_id, payload_json FROM messages WHERE id = ?1 AND session_id = ?2",
					params![id, session_id],
					|row| Ok((row.get(0)?, row.get(1)?)),
				)
				.optional()?;
			let Some((msg_branch, payload)) = row else {
				return Err(StoreError::NotFound(format!(
					"message {id} not found in session {session_id}"
				)));
			};
			if msg_branch != branch_id {
				return Err(StoreError::Validation(format!(
					"message {id} belongs to branch {msg_branch}, not {branch_id}"
				)));
			}
			(id.to_string(), payload)
		} else {
			let mut cursor = leaf_message_id.or(parent_message_id);
			let mut seen = HashSet::new();
			let mut found: Option<(String, String)> = None;
			while let Some(id) = cursor {
				if !seen.insert(id.clone()) {
					return Err(StoreError::Corrupt(format!(
						"branch {branch_id} contains a message cycle at {id}"
					)));
				}
				let row: Option<(String, String, Option<String>, String)> = transaction
					.query_row(
						"SELECT session_id, branch_id, parent_message_id, payload_json FROM messages WHERE id = ?1",
						params![id],
						|r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
					)
					.optional()?;
				let Some((msg_session, msg_branch, parent, payload)) = row else {
					return Err(StoreError::Corrupt(format!(
						"branch {branch_id} refers to missing message {id}"
					)));
				};
				if msg_session != session_id {
					return Err(StoreError::Corrupt(format!(
						"branch {branch_id} refers to message {id} belonging to session {msg_session}, not {session_id}"
					)));
				}
				if msg_branch != branch_id {
					// Ancestry traversed beyond branch root; do not mutate parent branch messages
					break;
				}
				let parsed: Value = decode_json(&payload)?;
				let role = parsed
					.get("message")
					.and_then(|m| m.get("role"))
					.or_else(|| parsed.get("role"))
					.and_then(Value::as_str);
				if role == Some("assistant") {
					found = Some((id, payload));
					break;
				}
				cursor = parent;
			}
			let Some(target) = found else {
				return Err(StoreError::NotFound(format!(
					"no assistant message found on branch {branch_id} for session {session_id}"
				)));
			};
			target
		};

		let mut payload: Value = decode_json(&raw_payload)?;
		let canon_stop = crate::session::canonical_stop_reason(stop_reason);

		if let Some(msg_obj) = payload.get_mut("message").and_then(Value::as_object_mut) {
			msg_obj.insert("stopReason".to_string(), Value::String(canon_stop.to_string()));
			if let Some(err) = error_message {
				msg_obj.insert("errorMessage".to_string(), Value::String(err.to_string()));
			} else {
				msg_obj.remove("errorMessage");
			}
		} else if let Some(top_obj) = payload.as_object_mut() {
			top_obj.insert("stopReason".to_string(), Value::String(canon_stop.to_string()));
			if let Some(err) = error_message {
				top_obj.insert("errorMessage".to_string(), Value::String(err.to_string()));
			} else {
				top_obj.remove("errorMessage");
			}
		}

		crate::auth::error::redact_message_payload(&mut payload);
		let encoded = encode_json(&payload)?;
		transaction.execute(
			"UPDATE messages SET payload_json = ?1 WHERE id = ?2",
			params![encoded, target_id],
		)?;
		transaction.execute(
			"UPDATE sessions SET updated_at_ms = ?1 WHERE id = ?2",
			params![now, session_id],
		)?;
		transaction.commit()?;

		Ok(target_id)
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
		return Err(StoreError::NotFound(format!("branch {} does not exist", message.branch_id)));
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
			.query_row("SELECT session_id FROM messages WHERE id = ?1", params![parent_id], |row| {
				row.get(0)
			})
			.optional()?;
		match parent_session {
			None => {
				return Err(StoreError::NotFound(format!("parent message {parent_id} does not exist")));
			},
			Some(parent_session) if parent_session != message.session_id => {
				return Err(StoreError::Validation(
					"parent message belongs to a different session".to_string(),
				));
			},
			Some(_) => {},
		}
	}
	// Redact before the row is written: this payload is what the UI renders and
	// what the model replays, so a credential in tool output or a provider error
	// must not be persisted in the clear.
	let mut payload = message.payload.clone();
	crate::auth::error::redact_message_payload(&mut payload);
	transaction.execute(
		"INSERT INTO messages (id, session_id, branch_id, parent_message_id, payload_json, created_at_ms) \
		 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
		params![
			message.id,
			message.session_id,
			message.branch_id,
			message.parent_message_id,
			encode_json(&payload)?,
			now
		],
	)?;
	transaction.execute(
		"UPDATE branches SET leaf_message_id = ?1 WHERE id = ?2",
		params![message.id, message.branch_id],
	)?;
	// Attempt-scoped terminal clearing, in the same transaction as the
	// terminal append. The draft exists to preserve text that the journal will
	// not replay, so it clears exactly when the assistant turn is durable and
	// replayable: a `stop`/`length` final answer, or a `toolUse` assistant whose
	// text (and tool calls) are journaled. A mirror write (RPC tail, replay)
	// carries no marker and never clears. `error`, `aborted`, and unknown-stop
	// rows never clear: that assistant is excluded from replay, so the draft is
	// the only surviving copy of its text.
	if payload_role(&message.payload) == Some("assistant") {
		// `SessionMessage.extra` is `#[serde(flatten)]`: the marker lands at
		// `message.streamAttempt`, not under an `extra` object.
		let terminal_attempt = message
			.payload
			.get("message")
			.and_then(|message| message.get("streamAttempt"))
			.and_then(Value::as_str);
		let stop = message
			.payload
			.get("message")
			.and_then(|message| message.get("stopReason"))
			.and_then(Value::as_str);
		let is_final = matches!(stop, Some("stop") | Some("length") | Some("toolUse"));
		if let (Some(attempt), true) = (terminal_attempt, is_final) {
			clear_stream_draft_attempt_tx(
				transaction,
				&message.session_id,
				&message.branch_id,
				attempt,
			)?;
		}
	}
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
	let mut redacted_outcome = outcome.clone();
	crate::auth::error::redact_json_strings(&mut redacted_outcome);
	let encoded = encode_json(&redacted_outcome)?;
	let existing: Option<Option<String>> = transaction
		.query_row(
			"SELECT outcome_json FROM executions WHERE execution_id = ?1",
			params![execution_id],
			|row| row.get(0),
		)
		.optional()?;
	let Some(existing) = existing else {
		return Err(StoreError::NotFound(format!("execution {execution_id} does not exist")));
	};
	if let Some(existing) = existing {
		let existing = decode_json(&existing)?;
		// Rows written before the redaction layer hold the raw value; repeating
		// the original payload must stay idempotent even though new rows store
		// the masked form.
		if existing == redacted_outcome || existing == *outcome {
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
		.query_row("SELECT payload_json FROM sessions WHERE id = ?1", params![session_id], |row| {
			row.get(0)
		})
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
		let Some((seq, id, msg_session, msg_branch, parent, payload_json, created_at_ms)) = msg_row
		else {
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

/// Role of a stored message payload: the typed `message.role` first, then the
/// flattened legacy `role`. Non-message entries have no role.
fn payload_role(payload: &Value) -> Option<&str> {
	payload
		.get("message")
		.and_then(|m| m.get("role"))
		.or_else(|| payload.get("role"))
		.and_then(Value::as_str)
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

/// Validates the projection synthetic prefix with the same transition rules as
/// [`crate::durable_session::validate_runtime_replay`] and returns the calls
/// still pending, which kept rows may complete.
fn pending_synthetic_call_counts(messages: &[Value]) -> StoreResult<HashMap<String, usize>> {
	let mut pending = HashMap::new();
	for value in messages {
		let message: pi_agent::state::Message = serde_json::from_value(value.clone())
			.map_err(|e| StoreError::Corrupt(format!("invalid projection synthetic message: {e}")))?;
		match message.role {
			pi_agent::Role::Assistant => {
				if !pending.is_empty() {
					return Err(StoreError::Corrupt(
						"synthetic projection assistant interrupts unresolved tool calls".into(),
					));
				}
				if !message.tool_calls.is_empty() {
					for call in &message.tool_calls {
						*pending.entry(call.id.clone()).or_default() += 1;
					}
				} else {
					for part in &message.parts {
						if let pi_agent::ContentPart::ToolCall { id, .. } = part {
							*pending.entry(id.clone()).or_default() += 1;
						}
					}
				}
			},
			pi_agent::Role::Tool => {
				let id = message.tool_call_id.as_deref().ok_or_else(|| {
					StoreError::Corrupt("synthetic projection tool result missing toolCallId".into())
				})?;
				let Some(count) = pending.get_mut(id) else {
					return Err(StoreError::Corrupt(format!(
						"synthetic projection tool result has no pending call {id}"
					)));
				};
				*count -= 1;
				if *count == 0 {
					pending.remove(id);
				}
			},
			pi_agent::Role::User if !pending.is_empty() => {
				return Err(StoreError::Corrupt(
					"synthetic projection user message interrupts unresolved tool calls".into(),
				));
			},
			_ => {},
		}
	}
	Ok(pending)
}

/// Execution ids referenced by stored message payloads (`details.executionId`
/// or `details.execution_id`), used to snapshot fork provenance.
pub(crate) fn referenced_execution_ids<'a>(
	payloads: impl Iterator<Item = &'a Value>,
) -> HashSet<String> {
	let mut ids = HashSet::new();
	for payload in payloads {
		let details = payload
			.get("message")
			.and_then(|m| m.get("details"))
			.or_else(|| payload.get("details"));
		let Some(details) = details else {
			continue;
		};
		if let Some(id) = details
			.get("executionId")
			.or_else(|| details.get("execution_id"))
			.and_then(Value::as_str)
		{
			ids.insert(id.to_string());
		}
	}
	ids
}

/// Strict shape for a stored durable projection. Absent `kept`/`synthetic`
/// arrays are historically valid defaults (empty); present members of the wrong
/// type are corruption rather than an absent projection.
pub(crate) fn parse_stored_projection(value: &Value) -> StoreResult<StoredProjection> {
	let object = value
		.as_object()
		.ok_or_else(|| StoreError::Corrupt("durableProjection must be an object".to_string()))?;
	if let Some(kept) = object.get("kept") {
		for id in kept.as_array().ok_or_else(|| {
			StoreError::Corrupt("durableProjection.kept must be an array".to_string())
		})? {
			if id.as_str().is_none() {
				return Err(StoreError::Corrupt(
					"durableProjection.kept must contain only strings".to_string(),
				));
			}
		}
	}
	if let Some(synthetic) = object.get("synthetic") {
		synthetic.as_array().ok_or_else(|| {
			StoreError::Corrupt("durableProjection.synthetic must be an array".to_string())
		})?;
	}
	for key in ["anchor", "firstKeptEntryId"] {
		if let Some(found) = object.get(key) {
			if !found.is_null() && found.as_str().is_none() {
				return Err(StoreError::Corrupt(format!(
					"durableProjection.{key} must be a string or null"
				)));
			}
		}
	}
	// Canonical `anchor` wins, including an explicit null; the legacy
	// `firstKeptEntryId` only fills in when `anchor` is absent. Normalizing
	// before deserialization keeps both fields legal together.
	let mut normalized = object.clone();
	if normalized.contains_key("anchor") {
		normalized.remove("firstKeptEntryId");
	} else if let Some(legacy) = normalized.remove("firstKeptEntryId") {
		normalized.insert("anchor".to_string(), legacy);
	}
	serde_json::from_value(Value::Object(normalized))
		.map_err(|error| StoreError::Corrupt(format!("invalid durableProjection: {error}")))
}

fn validate_kept_messages_tx(
	transaction: &Transaction<'_>,
	session_id: &str,
	kept_ids: &[String],
	branch_messages: &[MessageRecord],
	synthetic_pending: &mut HashMap<String, usize>,
) -> StoreResult<bool> {
	let branch_msg_map: HashMap<&str, &MessageRecord> =
		branch_messages.iter().map(|m| (m.id.as_str(), m)).collect();

	let mut pending_tool_calls: HashMap<String, usize> = HashMap::new();
	let mut pending_assistant_id: Option<&str> = None;
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
					if !synthetic_pending.is_empty() {
						return Ok(false);
					}
					pending_assistant_id = Some(msg.id.as_str());
					pending_tool_calls = extract_assistant_tool_call_counts(&msg.payload);
				},
				Some("toolResult") => {
					if let Some(call_id) = extract_tool_result_call_id(&msg.payload) {
						if pending_assistant_id.is_none() {
							if let Some(count) = synthetic_pending.get_mut(&call_id) {
								*count -= 1;
								if *count == 0 {
									synthetic_pending.remove(&call_id);
								}
							}
						} else {
							decrement_call_count(&mut pending_tool_calls, &call_id);
						}
					}
				},
				Some("user") => {
					if !synthetic_pending.is_empty() {
						return Ok(false);
					}
					pending_tool_calls.clear();
					pending_assistant_id = None;
				},
				_ => {
					pending_tool_calls.clear();
					pending_assistant_id = None;
				},
			}
		} else {
			let row: Option<(String, Option<String>)> = transaction
				.query_row(
					"SELECT payload_json, parent_message_id FROM messages WHERE id = ?1 AND session_id = ?2",
					params![id, session_id],
					|r| Ok((r.get(0)?, r.get(1)?)),
				)
				.optional()?;
			let Some((payload_json, mut parent_id)) = row else {
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
			let mut reaches_assistant = false;
			let mut seen = HashSet::new();
			while let Some(parent) = parent_id.take() {
				if !seen.insert(parent.clone()) {
					return Ok(false);
				}
				let ancestor: Option<(String, Option<String>)> = transaction
					.query_row(
						"SELECT payload_json, parent_message_id FROM messages WHERE id = ?1 AND session_id = ?2",
						params![parent, session_id],
						|r| Ok((r.get(0)?, r.get(1)?)),
					)
					.optional()?;
				let Some((ancestor_payload, ancestor_parent)) = ancestor else {
					return Ok(false);
				};
				let ancestor_value = decode_json(&ancestor_payload)
					.map_err(|_| StoreError::Corrupt("invalid ancestor payload".to_string()))?;
				let ancestor_role = ancestor_value
					.get("message")
					.and_then(|m| m.get("role"))
					.or_else(|| ancestor_value.get("role"))
					.and_then(Value::as_str);
				if ancestor_role == Some("assistant") {
					let ancestry_calls = extract_assistant_tool_call_counts(&ancestor_value);
					reaches_assistant = if let Some(pending_id) = pending_assistant_id {
						pending_id == parent.as_str()
					} else {
						synthetic_pending.contains_key(&call_id) && ancestry_calls.contains_key(&call_id)
					};
					break;
				}
				if ancestor_role != Some("toolResult") {
					return Ok(false);
				}
				parent_id = ancestor_parent;
			}
			if !reaches_assistant {
				return Ok(false);
			}
			if pending_assistant_id.is_some() {
				if !decrement_call_count(&mut pending_tool_calls, &call_id) {
					return Ok(false);
				}
			} else if let Some(count) = synthetic_pending.get_mut(&call_id) {
				*count -= 1;
				if *count == 0 {
					synthetic_pending.remove(&call_id);
				}
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
	parse_stored_projection(proj_val)?;
	let Some(proj_obj) = proj_val.as_object_mut() else {
		return Err(StoreError::Corrupt("durableProjection must be an object".to_string()));
	};
	let anchor_id = proj_obj
		.get("anchor")
		.or_else(|| proj_obj.get("firstKeptEntryId"))
		.and_then(Value::as_str)
		.map(str::to_string);
	let synthetic_values = proj_obj
		.get("synthetic")
		.and_then(Value::as_array)
		.cloned()
		.unwrap_or_default();
	let kept_ids: Vec<String> = proj_obj
		.get("kept")
		.and_then(Value::as_array)
		.map(|kept| {
			kept
				.iter()
				.filter_map(Value::as_str)
				.map(str::to_string)
				.collect()
		})
		.unwrap_or_default();

	let branch_messages = branch_messages_tx(transaction, &active_branch_id)?;

	let last_kept_idx: Option<usize> = if kept_ids.is_empty() {
		// Synthetic-only projection: the prefix alone must be a valid replay with
		// no unresolved tool calls, since no kept row can complete them.
		let replay_messages = synthetic_values
			.iter()
			.map(|value| {
				serde_json::from_value::<pi_agent::state::Message>(value.clone()).map_err(|e| {
					StoreError::Corrupt(format!("invalid projection synthetic message: {e}"))
				})
			})
			.collect::<StoreResult<Vec<_>>>()?;
		crate::durable_session::validate_runtime_replay(&replay_messages)?;
		// Only recover from an explicit compaction anchor. Never do an un-anchored
		// fallback scan that could resurrect previously pruned history.
		anchor_id
			.as_deref()
			.and_then(|id| branch_messages.iter().position(|m| m.id == id))
	} else {
		let mut synthetic_pending = pending_synthetic_call_counts(&synthetic_values)?;
		if !validate_kept_messages_tx(
			transaction,
			session_id,
			&kept_ids,
			&branch_messages,
			&mut synthetic_pending,
		)? {
			return Err(StoreError::Corrupt(format!(
				"durable projection lineage is invalid for session {session_id}"
			)));
		}
		let mut last_idx = None;
		let kept_set: HashSet<&str> = kept_ids.iter().map(String::as_str).collect();

		for (idx, msg) in branch_messages.iter().enumerate() {
			if kept_set.contains(msg.id.as_str()) {
				last_idx = Some(idx);
			}
		}
		last_idx.or_else(|| {
			anchor_id
				.as_deref()
				.and_then(|id| branch_messages.iter().position(|m| m.id == id))
		})
	};

	let Some(last_kept_idx) = last_kept_idx else {
		if kept_ids.is_empty() && anchor_id.is_none() {
			return Ok(0);
		}
		return Err(StoreError::Corrupt(format!(
			"durable projection has no active branch anchor for session {session_id}"
		)));
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

	{
		let kept_val = proj_obj
			.entry("kept".to_string())
			.or_insert_with(|| Value::Array(Vec::new()));
		let kept_arr = kept_val.as_array_mut().ok_or_else(|| {
			StoreError::Corrupt("durableProjection.kept must be an array".to_string())
		})?;
		for id in &backfill_ids {
			if !kept_arr.iter().any(|v| v.as_str() == Some(id.as_str())) {
				kept_arr.push(Value::String(id.clone()));
			}
		}
	}

	let updated_json = encode_json(&session_payload)?;
	transaction.execute(
		"UPDATE sessions SET payload_json = ?1, updated_at_ms = ?2 WHERE id = ?3",
		params![updated_json, now, session_id],
	)?;

	Ok(backfill_ids.len())
}

fn trailing_assistant_batch(branch_messages: &[MessageRecord]) -> Option<(usize, &MessageRecord)> {
	let mut idx = branch_messages.len();
	while idx > 0 {
		idx -= 1;
		let record = &branch_messages[idx];
		if record.payload.get("type").and_then(Value::as_str) != Some("message") {
			continue;
		}
		let role = record
			.payload
			.get("message")
			.and_then(|m| m.get("role"))
			.or_else(|| record.payload.get("role"))
			.and_then(Value::as_str);
		match role {
			Some("toolResult") => continue,
			Some("assistant") => return Some((idx, record)),
			_ => return None,
		}
	}
	None
}

fn assistant_tool_call_ids_in_order(payload: &Value) -> Vec<String> {
	let mut ids = Vec::new();
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
					ids.push(id.to_string());
				}
			}
		}
	}
	if !ids.is_empty() {
		return ids;
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
				ids.push(id.to_string());
			}
		}
	}
	ids
}

/// True when earlier assistant declarations of `call_id` outnumber verified
/// answers before the trailing assistant. Counts every declaration occurrence
/// (duplicate ids inside one batch included) and every answer whose durable
/// execution is resolved; unresolved or unknown executions never answer and an
/// unknown execution id fails closed. Provider id reuse across answered turns
/// must not block recovery of a unique trailing unresolved intent.
fn earlier_unanswered_declaration(
	transaction: &Transaction<'_>,
	branch_messages: &[MessageRecord],
	trailing_asst_idx: usize,
	call_id: &str,
) -> StoreResult<bool> {
	let before = &branch_messages[..trailing_asst_idx];
	let mut declarations = 0usize;
	for msg in before {
		if is_assistant_with_call(&msg.payload, call_id) {
			declarations += extract_assistant_tool_call_counts(&msg.payload)
				.get(call_id)
				.copied()
				.unwrap_or(1)
				.max(1);
		}
	}
	if declarations == 0 {
		return Ok(false);
	}
	if !table_exists_conn(transaction, "fork_provenance")? {
		return Err(StoreError::Corrupt(
			"session store is missing the fork_provenance table".to_string(),
		));
	}
	let mut verified = 0usize;
	for msg in before {
		if extract_tool_result_call_id(&msg.payload).as_deref() != Some(call_id) {
			continue;
		}
		let details = msg
			.payload
			.get("message")
			.and_then(|m| m.get("details"))
			.or_else(|| msg.payload.get("details"));
		let execution_id = details
			.and_then(|details| details.get("executionId"))
			.and_then(Value::as_str);
		match execution_id {
			// Legacy/manual results carry no durable execution to verify.
			None => verified += 1,
			Some(execution_id) => {
				let owned = transaction
					.query_row(
						"SELECT outcome_json IS NOT NULL, provider_call_id FROM executions WHERE execution_id = ?1 AND session_id = ?2",
						params![execution_id, msg.session_id],
						|row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?)),
					)
					.optional()?
					.map(|(resolved, provider_call_id)| {
						if provider_call_id != call_id {
							return Err(StoreError::Corrupt(format!(
								"toolResult for {call_id} does not match execution {execution_id} provider call id {provider_call_id}"
							)));
						}
						Ok(resolved)
					})
					.transpose()?;
				let copied = transaction
					.query_row(
						"SELECT outcome_resolved, provider_call_id FROM fork_provenance WHERE message_id = ?1 AND source_execution_id = ?2",
						params![msg.id, execution_id],
						|row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
					)
					.optional()?
					.map(|(resolved, provider_call_id)| {
						if provider_call_id != call_id {
							return Err(StoreError::Corrupt(format!(
								"fork provenance for {call_id} names provider call id {provider_call_id}"
							)));
						}
						Ok(resolved)
					})
					.transpose()?;
				match (copied, owned) {
					(Some(copied), Some(owned)) if (copied != 0) != owned => {
						return Err(StoreError::Corrupt(format!(
							"toolResult for {call_id} has fork provenance conflicting with execution {execution_id}"
						)));
					},
					(Some(1), _) => verified += 1,
					(Some(0), _) => {},
					(Some(_), _) => {
						return Err(StoreError::Corrupt(format!(
							"toolResult for {call_id} has invalid fork provenance"
						)));
					},
					(None, Some(true)) => verified += 1,
					(None, Some(false)) => {},
					(None, None) => {
						return Err(StoreError::Corrupt(format!(
							"toolResult for {call_id} references unknown execution or missing fork provenance {execution_id}"
						)));
					},
				}
			},
		}
	}
	Ok(declarations > verified)
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
		return Err(StoreError::NotFound(format!("parent branch {parent_branch_id} does not exist")));
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
		return Err(StoreError::NotFound(format!("session {session_id} does not exist")));
	}
	Ok(())
}

fn validate_id(label: &str, value: &str) -> StoreResult<()> {
	if value.is_empty() {
		return Err(StoreError::Validation(format!("{label} must not be empty")));
	}
	Ok(())
}
/// Full uncapped draft text: priors oldest-first, then live current, then
/// legacy fallback. Shared by delivery (`combined_stream_draft_text`) and
/// clear, so both compute the identical newest-tail cut.
fn full_draft_text(payload: &Value) -> String {
	let mut parts: Vec<String> = Vec::new();
	if let Some(priors) = payload.get("priors").and_then(Value::as_array) {
		for prior in priors {
			if let Some(text) = prior
				.get("text")
				.and_then(Value::as_str)
				.filter(|text| !text.is_empty())
			{
				parts.push(crate::auth::error::redact_free_text(text));
			}
		}
	}
	if let Some(text) = payload
		.get("current")
		.and_then(|current| current.get("text"))
		.and_then(Value::as_str)
		.filter(|text| !text.is_empty())
	{
		parts.push(crate::auth::error::redact_free_text(text));
	}
	if parts.is_empty()
		&& let Some(text) = payload
			.get("text")
			.and_then(Value::as_str)
			.filter(|text| !text.is_empty())
	{
		parts.push(crate::auth::error::redact_free_text(text));
	}
	parts.join("\n\n--- interrupted stream fragment ---\n\n")
}

/// Delivery window for an interrupted draft, in chars.
///
/// Deliberately a compile-time constant rather than config: the clear path
/// must use exactly the window the delivery path used, otherwise a config
/// change between two turns shifts the boundary and either deletes
/// undelivered text or re-delivers already-sent text. The window bounds the
/// model context a recovery costs, not what the row stores.
const STREAM_DRAFT_DELIVERY_CHARS: usize = 8000;
const STREAM_DRAFT_TRUNCATION_MARKER: &str = "[truncated: oldest fragments omitted]\n\n";

/// Combine a draft row into display/model text: priors oldest-first,
/// then the live current draft. Joined with an explicit interrupted banner
/// per fragment so concatenated streams never read as one response.
/// Capped to `STREAM_DRAFT_DELIVERY_CHARS`; truncation is marked and the
/// cut is char-exact so the undelivered prefix survives for the next turn.
fn combined_stream_draft_text(payload: &Value) -> String {
	let full = full_draft_text(payload);
	if full.chars().count() <= STREAM_DRAFT_DELIVERY_CHARS {
		return full;
	}
	// Keep the newest tail: cut from the front at a char boundary, then
	// mark so the model never mistakes the remainder for a complete reply.
	let excess = full.chars().count() - STREAM_DRAFT_DELIVERY_CHARS;
	let tail: String = full.chars().skip(excess).collect();
	format!("{STREAM_DRAFT_TRUNCATION_MARKER}{tail}")
}

/// Clear the delivered tail of the branch draft on a loop-stamped terminal
/// whose text the journal replays (`stop`/`length`/`toolUse`), preserving any
/// undelivered prefix as a legacy row. Delivery caps the combined text at
/// 8000 chars (newest tail) computed from the row as it stood at request
/// time — i.e. excluding this terminal's own current attempt, whose flushes
/// landed after the request was built. Only the loop stamps `streamAttempt`,
/// so mirror writes (RPC tail, replay) carry no marker and never clear.
/// `error`/`aborted`/unknown stops are the partial itself and their assistant
/// is not replayed, so the draft stays the only copy of that text.
fn clear_stream_draft_attempt_tx(
	transaction: &Transaction<'_>,
	session_id: &str,
	branch_id: &str,
	attempt: &str,
) -> StoreResult<()> {
	if !table_exists_conn(transaction, "stream_drafts")? {
		return Err(StoreError::Schema(
			"stream_drafts table is missing; database schema is damaged".to_string(),
		));
	}
	if attempt.is_empty() {
		return Ok(());
	}
	let raw: Option<String> = transaction
		.query_row(
			"SELECT payload_json FROM stream_drafts WHERE branch_id = ?1 AND session_id = ?2",
			params![branch_id, session_id],
			|row| row.get(0),
		)
		.optional()?;
	let Some(raw) = raw else {
		return Ok(());
	};
	let payload: Value = decode_json(&raw)?;
	// Request-time parts: everything except this terminal's own current
	// attempt, whose text flushed after the request was built and was never
	// delivered as interrupted context. A normal first stream (>8000 chars,
	// no priors) therefore has an empty request-time set: nothing was
	// delivered, and success deletes the row instead of keeping a bogus
	// "interrupted" prefix of its own completed output.
	let own = payload
		.get("current")
		.and_then(|current| current.get("attempt"))
		.and_then(Value::as_str)
		== Some(attempt);
	let mut request_parts: Vec<String> = Vec::new();
	if let Some(priors) = payload.get("priors").and_then(Value::as_array) {
		for prior in priors {
			if let Some(text) = prior
				.get("text")
				.and_then(Value::as_str)
				.filter(|text| !text.is_empty())
			{
				request_parts.push(crate::auth::error::redact_free_text(text));
			}
		}
	}
	if !own {
		if let Some(text) = payload
			.get("current")
			.and_then(|current| current.get("text"))
			.and_then(Value::as_str)
			.filter(|text| !text.is_empty())
		{
			request_parts.push(crate::auth::error::redact_free_text(text));
		}
		// Legacy single-text rows (pre attempt-scoped payloads).
		if request_parts.is_empty()
			&& let Some(text) = payload
				.get("text")
				.and_then(Value::as_str)
				.filter(|text| !text.is_empty())
		{
			request_parts.push(crate::auth::error::redact_free_text(text));
		}
	}
	let request_full = request_parts.join("\n\n--- interrupted stream fragment ---\n\n");
	let request_len = request_full.chars().count();
	// Undelivered head: the prefix beyond the newest-tail delivery window
	// (`STREAM_DRAFT_DELIVERY_CHARS`, the same constant delivery uses).
	// Char-exact split, so kept + delivered == request-time text with
	// no duplication or omission; the next turn delivers `kept` once.
	let kept: String = if request_len <= STREAM_DRAFT_DELIVERY_CHARS {
		String::new()
	} else {
		request_full
			.chars()
			.take(request_len - STREAM_DRAFT_DELIVERY_CHARS)
			.collect()
	};
	if kept.trim().is_empty() {
		transaction.execute(
			"DELETE FROM stream_drafts WHERE branch_id = ?1 AND session_id = ?2",
			params![branch_id, session_id],
		)?;
		return Ok(());
	}
	let kept_payload = serde_json::json!({ "text": crate::auth::error::redact_free_text(&kept) });
	transaction.execute(
		"UPDATE stream_drafts SET payload_json = ?1, updated_at_ms = ?2 WHERE branch_id = ?3 AND session_id = ?4",
		params![encode_json(&kept_payload)?, now_ms(), branch_id, session_id],
	)?;
	Ok(())
}

/// Fail closed when a draft write names a branch that does not belong to the
/// session: without this, an upsert keyed only by `branch_id` could attach a
/// fragment to the wrong session's branch and silently steal its recovery.
fn require_branch_owner(
	connection: &Connection,
	session_id: &str,
	branch_id: &str,
) -> StoreResult<()> {
	let owner: Option<String> = connection
		.query_row("SELECT session_id FROM branches WHERE id = ?1", params![branch_id], |row| {
			row.get(0)
		})
		.optional()?;
	match owner.as_deref() {
		Some(owner) if owner == session_id => Ok(()),
		Some(_) => Err(StoreError::Corrupt(format!(
			"branch {branch_id} does not belong to session {session_id}; refusing draft write"
		))),
		None => Err(StoreError::Corrupt(format!(
			"branch {branch_id} does not exist; refusing draft write"
		))),
	}
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
	#[test]
	fn fork_atomic_preserves_ancestry_and_projection() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		store.create_fork_atomic(ForkWrite {
			session: NewSession { id: "fork".into(), parent_session_id: Some("source".into()), payload: json!({"type":"session","id":"fork","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp","parentSession":"source"}), initial_branch_id: "branch".into(), initial_branch_payload: json!({}) },
			branches: vec![NewBranch { id: "branch".into(), session_id: "fork".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }], active_branch_id: "branch".into(), active_leaf_message_id: Some("m2".into()),
			messages: vec![NewMessage { id: "m1".into(), session_id: "fork".into(), branch_id: "branch".into(), parent_message_id: None, payload: json!({"type":"message","id":"m1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"one","timestamp":1}}) }, NewMessage { id: "m2".into(), session_id: "fork".into(), branch_id: "branch".into(), parent_message_id: Some("m1".into()), payload: json!({"type":"message","id":"m2","parentId":"m1","timestamp":"2026-01-01T00:00:01Z","message":{"role":"assistant","content":"two","timestamp":2,"api":"openai","provider":"test","model":"model"}}) }], projection: Some(StoredProjection { kept: vec!["m1".into(), "m2".into()], synthetic: vec![json!({"role":"user","content":"summary","timestamp":3})], anchor: Some("m1".into()) }),
		}).expect("fork");
		let branch = store.branch("branch").expect("branch").expect("exists");
		assert_eq!(branch.leaf_message_id.as_deref(), Some("m2"));
		assert_eq!(
			store
				.session("fork")
				.expect("session")
				.expect("exists")
				.parent_session_id
				.as_deref(),
			None
		);
		let projection = &store
			.session("fork")
			.expect("session")
			.expect("exists")
			.payload["durableProjection"];
		assert_eq!(projection["kept"], json!(["m1", "m2"]));
		assert_eq!(projection["anchor"], json!("m1"));
		assert_eq!(projection["synthetic"][0]["role"], json!("user"));
	}

	#[test]
	fn fork_atomic_rolls_back_on_mid_insert_failure() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		let result = store.create_fork_atomic(ForkWrite {
			session: NewSession { id: "rollback".into(), parent_session_id: None, payload: json!({"type":"session","id":"rollback","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "branch".into(), initial_branch_payload: json!({}) },
			branches: vec![NewBranch { id: "branch".into(), session_id: "rollback".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }], active_branch_id: "branch".into(), active_leaf_message_id: None,
			messages: vec![NewMessage { id: "ok".into(), session_id: "rollback".into(), branch_id: "branch".into(), parent_message_id: None, payload: json!({"type":"metadata","id":"ok"}) }, NewMessage { id: "bad".into(), session_id: "rollback".into(), branch_id: "branch".into(), parent_message_id: Some("missing".into()), payload: json!({"type":"metadata","id":"bad","parentId":"missing"}) }], projection: None,
		});
		assert!(result.is_err());
		assert!(store.session("rollback").expect("query").is_none());
		assert!(
			store
				.session_messages("rollback")
				.expect("query")
				.is_empty()
		);
	}

	#[test]
	fn fork_atomic_clears_stale_projection_without_authority() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		store.create_fork_atomic(ForkWrite { session: NewSession { id: "none".into(), parent_session_id: None, payload: json!({"type":"session","id":"none","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp","durableProjection":{"kept":["stale"]}}), initial_branch_id: "b".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "b".into(), session_id: "none".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }], active_branch_id: "b".into(), active_leaf_message_id: None, messages: vec![], projection: None }).expect("fork");
		assert!(
			store
				.session("none")
				.expect("session")
				.expect("exists")
				.payload
				.get("durableProjection")
				.is_none()
		);
	}

	#[test]
	fn fork_atomic_rejects_self_parent() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		let result = store.create_fork_atomic(ForkWrite {
			session: NewSession {
				id: "self".into(),
				parent_session_id: Some("self".into()),
				payload: json!({}),
				initial_branch_id: "b".into(),
				initial_branch_payload: json!({}),
			},
			branches: vec![NewBranch {
				id: "b".into(),
				session_id: "self".into(),
				parent_branch_id: None,
				parent_message_id: None,
				payload: json!({}),
			}],
			active_branch_id: "b".into(),
			active_leaf_message_id: None,
			messages: vec![],
			projection: None,
		});
		assert!(result.is_err());
	}

	#[test]
	fn fork_atomic_accepts_two_branches_and_rejects_cycle() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		let ok = store.create_fork_atomic(ForkWrite {
			session: NewSession {
				id: "multi".into(),
				parent_session_id: None,
				payload: json!({"type":"session","id":"multi","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}),
				initial_branch_id: "sibling".into(),
				initial_branch_payload: json!({}),
			},
			branches: vec![
				NewBranch {
					id: "root".into(),
					session_id: "multi".into(),
					parent_branch_id: None,
					parent_message_id: None,
					payload: json!({}),
				},
				NewBranch {
					id: "sibling".into(),
					session_id: "multi".into(),
					parent_branch_id: Some("root".into()),
					parent_message_id: Some("m1".into()),
					payload: json!({}),
				},
			],
			active_branch_id: "sibling".into(),
			active_leaf_message_id: Some("m2".into()),
			messages: vec![
				NewMessage {
					id: "m1".into(),
					session_id: "multi".into(),
					branch_id: "root".into(),
					parent_message_id: None,
					payload: json!({"type":"metadata","id":"m1"}),
				},
				NewMessage {
					id: "m2".into(),
					session_id: "multi".into(),
					branch_id: "sibling".into(),
					parent_message_id: Some("m1".into()),
					payload: json!({"type":"metadata","id":"m2","parentId":"m1"}),
				},
			],
			projection: None,
		});
		ok.expect("fork with two branches");
		let cycle = store.create_fork_atomic(ForkWrite {
			session: NewSession {
				id: "cycle".into(),
				parent_session_id: None,
				payload: json!({"type":"session","id":"cycle","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}),
				initial_branch_id: "a".into(),
				initial_branch_payload: json!({}),
			},
			branches: vec![
				NewBranch {
					id: "a".into(),
					session_id: "cycle".into(),
					parent_branch_id: Some("b".into()),
					parent_message_id: Some("x".into()),
					payload: json!({}),
				},
				NewBranch {
					id: "b".into(),
					session_id: "cycle".into(),
					parent_branch_id: Some("a".into()),
					parent_message_id: Some("x".into()),
					payload: json!({}),
				},
			],
			active_branch_id: "a".into(),
			active_leaf_message_id: None,
			messages: vec![],
			projection: None,
		});
		assert!(cycle.is_err());
		assert!(store.session("cycle").expect("query").is_none());
	}

	#[test]
	fn fork_atomic_accepts_inherited_branch_point() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		let result = store.create_fork_atomic(ForkWrite { session: NewSession { id: "inherit".into(), parent_session_id: None, payload: json!({"type":"session","id":"inherit","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "child".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "root".into(), session_id: "inherit".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }, NewBranch { id: "child".into(), session_id: "inherit".into(), parent_branch_id: Some("root".into()), parent_message_id: Some("m1".into()), payload: json!({}) }], active_branch_id: "child".into(), active_leaf_message_id: Some("m2".into()), messages: vec![NewMessage { id: "m1".into(), session_id: "inherit".into(), branch_id: "root".into(), parent_message_id: None, payload: json!({"type":"metadata","id":"m1"}) }, NewMessage { id: "m2".into(), session_id: "inherit".into(), branch_id: "child".into(), parent_message_id: Some("m1".into()), payload: json!({"type":"metadata","id":"m2","parentId":"m1"}) }], projection: None });
		assert!(result.is_ok());
	}

	#[test]
	fn fork_atomic_rejects_reverse_projection_order() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		let result = store.create_fork_atomic(ForkWrite { session: NewSession { id: "order".into(), parent_session_id: None, payload: json!({"type":"session","id":"order","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "b".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "b".into(), session_id: "order".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }], active_branch_id: "b".into(), active_leaf_message_id: Some("m2".into()), messages: vec![NewMessage { id: "m1".into(), session_id: "order".into(), branch_id: "b".into(), parent_message_id: None, payload: json!({"type":"message","id":"m1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"one","timestamp":1}}) }, NewMessage { id: "m2".into(), session_id: "order".into(), branch_id: "b".into(), parent_message_id: Some("m1".into()), payload: json!({"type":"message","id":"m2","parentId":"m1","timestamp":"2026-01-01T00:00:01Z","message":{"role":"assistant","content":"two","timestamp":2,"api":"openai","provider":"test","model":"model"}}) }], projection: Some(StoredProjection { kept: vec!["m2".into(), "m1".into()], synthetic: vec![], anchor: None }) });
		assert!(result.is_err());
	}

	#[test]
	fn fork_atomic_accepts_shuffled_branches_with_empty_inherited_leaf() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		let result = store.create_fork_atomic(ForkWrite { session: NewSession { id: "shuffled".into(), parent_session_id: None, payload: json!({"type":"session","id":"shuffled","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "child".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "child".into(), session_id: "shuffled".into(), parent_branch_id: Some("root".into()), parent_message_id: Some("m1".into()), payload: json!({}) }, NewBranch { id: "root".into(), session_id: "shuffled".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }], active_branch_id: "child".into(), active_leaf_message_id: Some("m1".into()), messages: vec![NewMessage { id: "m1".into(), session_id: "shuffled".into(), branch_id: "root".into(), parent_message_id: None, payload: json!({"type":"message","id":"m1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"root","timestamp":1}}) }], projection: None });
		assert!(result.is_ok());
	}

	#[test]
	fn fork_atomic_projection_order_filters_offbranch_entries() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		let result = store.create_fork_atomic(ForkWrite { session: NewSession { id: "filtered".into(), parent_session_id: None, payload: json!({"type":"session","id":"filtered","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "active".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "active".into(), session_id: "filtered".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }, NewBranch { id: "other".into(), session_id: "filtered".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }], active_branch_id: "active".into(), active_leaf_message_id: Some("a2".into()), messages: vec![NewMessage { id: "a1".into(), session_id: "filtered".into(), branch_id: "active".into(), parent_message_id: None, payload: json!({"type":"message","id":"a1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"a"}}) }, NewMessage { id: "o1".into(), session_id: "filtered".into(), branch_id: "other".into(), parent_message_id: None, payload: json!({"type":"message","id":"o1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"o"}}) }, NewMessage { id: "a2".into(), session_id: "filtered".into(), branch_id: "active".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"a2","parentId":"a1","timestamp":"2026-01-01T00:00:01Z","message":{"role":"assistant","content":"a2"}}) }], projection: Some(StoredProjection { kept: vec!["a1".into(), "o1".into(), "a2".into()], synthetic: vec![], anchor: None }) });
		assert!(result.is_err());
	}

	#[test]
	fn fork_atomic_accepts_sibling_tool_result() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		let result = store.create_fork_atomic(ForkWrite { session: NewSession { id: "tool-sibling".into(), parent_session_id: None, payload: json!({"type":"session","id":"tool-sibling","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "active".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "sibling".into(), session_id: "tool-sibling".into(), parent_branch_id: Some("active".into()), parent_message_id: Some("a1".into()), payload: json!({}) }, NewBranch { id: "active".into(), session_id: "tool-sibling".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }], active_branch_id: "active".into(), active_leaf_message_id: Some("r1".into()), messages: vec![NewMessage { id: "a1".into(), session_id: "tool-sibling".into(), branch_id: "active".into(), parent_message_id: None, payload: json!({"type":"message","id":"a1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"call-1","name":"echo","arguments":{}},{"type":"toolCall","id":"call-2","name":"echo","arguments":{}}],"timestamp":1,"api":"openai","provider":"test","model":"model"}}) }, NewMessage { id: "r1".into(), session_id: "tool-sibling".into(), branch_id: "active".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"r1","parentId":"a1","timestamp":"2026-01-01T00:00:01Z","message":{"role":"toolResult","content":"one","toolCallId":"call-1","toolName":"echo","isError":false,"timestamp":2}}) }, NewMessage { id: "r2".into(), session_id: "tool-sibling".into(), branch_id: "sibling".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"r2","parentId":"a1","timestamp":"2026-01-01T00:00:02Z","message":{"role":"toolResult","content":"two","toolCallId":"call-2","toolName":"echo","isError":false,"timestamp":3}}) }], projection: Some(StoredProjection { kept: vec!["a1".into(), "r1".into(), "r2".into()], synthetic: vec![], anchor: Some("a1".into()) }) });
		assert!(result.is_ok());
		assert_eq!(
			store
				.branch("active")
				.expect("branch")
				.expect("active")
				.leaf_message_id
				.as_deref(),
			Some("r1")
		);
	}

	#[test]
	fn fork_atomic_rejects_anchor_on_sibling_branch() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		let result = store.create_fork_atomic(ForkWrite { session: NewSession { id: "anchor-sibling".into(), parent_session_id: None, payload: json!({"type":"session","id":"anchor-sibling","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "active".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "active".into(), session_id: "anchor-sibling".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }, NewBranch { id: "sibling".into(), session_id: "anchor-sibling".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }], active_branch_id: "active".into(), active_leaf_message_id: Some("a".into()), messages: vec![NewMessage { id: "a".into(), session_id: "anchor-sibling".into(), branch_id: "active".into(), parent_message_id: None, payload: json!({"type":"message","id":"a","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"a","timestamp":1}}) }, NewMessage { id: "s".into(), session_id: "anchor-sibling".into(), branch_id: "sibling".into(), parent_message_id: None, payload: json!({"type":"message","id":"s","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"s","timestamp":2}}) }], projection: Some(StoredProjection { kept: vec!["a".into()], synthetic: vec![], anchor: Some("s".into()) }) });
		let error = result.expect_err("sibling anchor must fail");
		assert!(error.to_string().contains("anchor"), "unexpected error: {error}");
	}

	#[test]
	fn fork_atomic_rolls_back_reversed_message_order() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		let result = store.create_fork_atomic(ForkWrite { session: NewSession { id: "reverse-msg".into(), parent_session_id: None, payload: json!({"type":"session","id":"reverse-msg","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "b".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "b".into(), session_id: "reverse-msg".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }], active_branch_id: "b".into(), active_leaf_message_id: Some("child".into()), messages: vec![NewMessage { id: "child".into(), session_id: "reverse-msg".into(), branch_id: "b".into(), parent_message_id: Some("parent".into()), payload: json!({"type":"message","id":"child","parentId":"parent","timestamp":"2026-01-01T00:00:01Z","message":{"role":"assistant","content":"child","timestamp":2,"api":"openai","provider":"test","model":"model"}}) }, NewMessage { id: "parent".into(), session_id: "reverse-msg".into(), branch_id: "b".into(), parent_message_id: None, payload: json!({"type":"message","id":"parent","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"parent","timestamp":1}}) }], projection: None });
		let error = result.expect_err("reversed parent order must fail");
		assert!(
			error.to_string().contains("branch leaf") || error.to_string().contains("parent"),
			"unexpected error: {error}"
		);
		assert!(store.session("reverse-msg").expect("query").is_none());
	}

	#[test]
	fn fork_atomic_rejects_reverse_active_order_with_valid_sibling() {
		let mut store = SqliteStore::open_in_memory().expect("store");
		let result = store.create_fork_atomic(ForkWrite { session: NewSession { id: "order-sibling".into(), parent_session_id: None, payload: json!({"type":"session","id":"order-sibling","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "active".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "active".into(), session_id: "order-sibling".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }, NewBranch { id: "sibling".into(), session_id: "order-sibling".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }], active_branch_id: "active".into(), active_leaf_message_id: Some("r1".into()), messages: vec![NewMessage { id: "a".into(), session_id: "order-sibling".into(), branch_id: "active".into(), parent_message_id: None, payload: json!({"type":"message","id":"a","timestamp":"2026-01-01T00:00:00Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"c1","name":"echo","arguments":{}}],"timestamp":1,"api":"openai","provider":"test","model":"model"}}) }, NewMessage { id: "r1".into(), session_id: "order-sibling".into(), branch_id: "active".into(), parent_message_id: Some("a".into()), payload: json!({"type":"message","id":"r1","parentId":"a","timestamp":"2026-01-01T00:00:01Z","message":{"role":"toolResult","content":"ok","toolCallId":"c1","toolName":"echo","isError":false,"timestamp":2}}) }, NewMessage { id: "s".into(), session_id: "order-sibling".into(), branch_id: "sibling".into(), parent_message_id: None, payload: json!({"type":"message","id":"s","timestamp":"2026-01-01T00:00:02Z","message":{"role":"user","content":"s","timestamp":3}}) }], projection: Some(StoredProjection { kept: vec!["r1".into(), "s".into(), "a".into()], synthetic: vec![], anchor: Some("a".into()) }) });
		let error = result.expect_err("reverse active order must fail");
		assert!(error.to_string().contains("order"), "unexpected error: {error}");
	}

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
	fn compact_session_atomic_rollback_on_failure() {
		let mut store = seeded_store();
		store
			.append_message(message("m1", None))
			.expect("append m1");

		// Trigger on UPDATE sessions aborts the transaction AFTER append_message_tx has executed
		store
			.connection
			.execute(
				"CREATE TRIGGER fail_session_update BEFORE UPDATE ON sessions \
				 BEGIN SELECT RAISE(ABORT, 'forced update failure'); END;",
				[],
			)
			.expect("create trigger");

		let compact_msg = NewMessage {
			id: "m_compact".to_string(),
			session_id: "s1".to_string(),
			branch_id: "b1".to_string(),
			parent_message_id: Some("m1".to_string()),
			payload: json!({ "type": "custom_message" }),
		};
		let proj = StoredProjection {
			kept: Vec::new(),
			synthetic: vec![json!({"role": "user", "content": "summary"})],
			anchor: Some("m_compact".to_string()),
		};

		let res = store.compact_session(compact_msg, proj);
		assert!(res.is_err(), "compact_session must fail when trigger aborts update");

		store
			.connection
			.execute("DROP TRIGGER fail_session_update", [])
			.expect("drop trigger");

		// Verify atomic rollback: neither message was added nor leaf updated nor durableProjection set
		let session = store
			.session("s1")
			.expect("load session")
			.expect("session exists");
		assert!(
			session.payload.get("durableProjection").is_none(),
			"durableProjection must not exist after rollback"
		);
		let branch_msgs = store.branch_messages("b1").expect("branch messages");
		assert_eq!(branch_msgs.len(), 1, "only m1 must exist; m_compact must be rolled back");
		assert_eq!(branch_msgs[0].id, "m1");

		let branch_leaf: Option<String> = store
			.connection
			.query_row("SELECT leaf_message_id FROM branches WHERE id = 'b1'", [], |r| r.get(0))
			.unwrap();
		assert_eq!(branch_leaf.as_deref(), Some("m1"));
	}

	#[test]
	fn reconcile_fails_closed_when_anchor_missing_from_active_branch() {
		let (dir, path) = temp_db("absent-anchor");
		let mut store = SqliteStore::open(&path).expect("open store");
		store
			.create_session(NewSession {
				id: "s1".to_string(),
				parent_session_id: None,
				payload: json!({
					"cwd": "/tmp/project",
					"durableProjection": {
						"kept": [],
						"synthetic": [{"role": "user", "content": "summary"}],
						"anchor": "missing_anchor_id"
					}
				}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({ "kind": "root" }),
			})
			.expect("create session");

		store
			.append_message(message("m1", None))
			.expect("append m1");

		let error = store
			.reconcile_durable_projection("s1")
			.expect_err("missing anchor must fail closed");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");

		let session = store
			.session("s1")
			.expect("load session")
			.expect("session exists");
		assert!(
			session.payload.get("durableProjection").is_some(),
			"durableProjection must be retained when the anchor is missing"
		);

		std::fs::remove_dir_all(dir).ok();
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
			assert_eq!(store.outcome(&execution_id).expect("read outcome"), None);
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
		store
			.append_message(message("m1", None))
			.expect("append m1");
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
		let branch = store
			.branch("b1")
			.expect("read branch row")
			.expect("branch");
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
		let conflict = store
			.record_outcome(&execution_id, json!({ "status": "completed", "result": "different" }));
		assert!(matches!(conflict, Err(StoreError::Conflict(_))));
		assert_eq!(store.outcome(&execution_id).expect("read outcome"), Some(outcome));
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
		store
			.append_message(message("m1", None))
			.expect("append m1");
		store
			.create_branch(NewBranch {
				id: "b2".to_string(),
				session_id: "s1".to_string(),
				parent_branch_id: Some("b1".to_string()),
				parent_message_id: Some("m1".to_string()),
				payload: json!({ "kind": "worker-result" }),
			})
			.expect("create branch b2");
		store.set_active_branch("s1", "b2").expect("activate b2");
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
		let conflicting =
			WorkerCompletion { delivery: json!({ "text": "different result" }), ..completion.clone() };
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
			store
				.append_message(message("m1", None))
				.expect("append m1");
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
					delivery: json!({
						"version": 2,
						"kind": "task-result",
						"mode": "background",
						"notify_parent": true,
						"text": "result",
					}),
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
	fn worker_delivery_consume_is_atomic_and_links_message_id() {
		let mut store = seeded_store();
		store
			.append_message(message("m1", None))
			.expect("append m1");
		let completion = WorkerCompletion {
			worker_id: "w1".to_string(),
			delivery_id: "w1".to_string(),
			session_id: "s1".to_string(),
			origin_branch_id: "b1".to_string(),
			completion: json!({ "status": "completed" }),
			delivery: json!({ "version": 2, "kind": "task-result", "mode": "background", "notify_parent": true, "text": "result w1" }),
		};
		store.complete_worker(completion).expect("complete worker");

		let pending = store
			.pending_worker_deliveries("s1")
			.expect("pending worker");
		assert_eq!(pending.len(), 1);
		assert_eq!(pending[0].delivery_id, "w1");
		assert_eq!(pending[0].message_id, None);

		let msg = NewMessage {
			id: "msg-w1".to_string(),
			session_id: "s1".to_string(),
			branch_id: "b1".to_string(),
			parent_message_id: Some("m1".to_string()),
			payload: json!({ "id": "msg-w1", "role": "user", "text": "result w1" }),
		};
		store
			.consume_delivery("w1", msg.clone(), 100)
			.expect("consume");

		let branch = store.branch("b1").expect("branch").expect("row");
		assert_eq!(branch.leaf_message_id.as_deref(), Some("msg-w1"));

		let unprocessed = store.unprocessed_deliveries("s1").expect("unprocessed");
		assert_eq!(unprocessed.len(), 1);
		assert_eq!(unprocessed[0].delivered_at_ms, Some(100));
		assert_eq!(unprocessed[0].message_id.as_deref(), Some("msg-w1"));

		// Idempotent duplicate consume
		store
			.consume_delivery("w1", msg, 100)
			.expect("idempotent consume");
		assert_eq!(store.branch_messages("b1").expect("msgs").len(), 2);

		// Conflicting reuse is rejected
		let conflict = store.consume_delivery(
			"w1",
			NewMessage {
				id: "msg-other".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("msg-w1".to_string()),
				payload: json!({ "id": "msg-other", "role": "user", "text": "diff" }),
			},
			101,
		);
		assert!(matches!(conflict, Err(StoreError::Conflict(_))));
	}

	#[test]
	fn synchronous_worker_completion_is_never_pending() {
		let mut store = seeded_store();
		let completion = WorkerCompletion {
			worker_id: "sync-1".to_string(),
			delivery_id: "sync-1".to_string(),
			session_id: "s1".to_string(),
			origin_branch_id: "b1".to_string(),
			completion: json!({ "status": "completed" }),
			delivery: json!({ "version": 2, "kind": "task-result", "mode": "sync", "notify_parent": false, "text": "" }),
		};
		store
			.complete_worker(completion)
			.expect("complete sync worker");

		assert!(
			store
				.pending_worker_deliveries("s1")
				.expect("pending worker")
				.is_empty()
		);
		assert!(store.pending_deliveries("s1").expect("pending").is_empty());

		// Cannot consume a synchronous completion as delivery
		let err = store.consume_delivery(
			"sync-1",
			NewMessage {
				id: "m-sync".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({ "id": "m-sync" }),
			},
			100,
		);
		assert!(matches!(err, Err(StoreError::Validation(_))));
	}

	#[test]
	fn worker_recovery_fails_closed_on_legacy_ambiguous_rows() {
		let mut store = seeded_store();
		store
			.append_message(message("m1", None))
			.expect("append m1");
		// Manually insert legacy ambiguous row (delivered without message_id)
		store.connection.execute(
			"INSERT INTO deliveries (delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms, message_id) \
			 VALUES ('legacy-del', 'legacy-w', 's1', 'b1', '{\"kind\":\"task-result\",\"text\":\"legacy\"}', 10, 20, NULL, NULL)",
			[],
		).expect("insert legacy delivery");

		let err = store.consume_delivery(
			"legacy-del",
			NewMessage {
				id: "m-legacy".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m1".to_string()),
				payload: json!({ "id": "m-legacy", "text": "legacy" }),
			},
			30,
		);
		assert!(matches!(err, Err(StoreError::Corrupt(_))));
	}

	#[test]
	fn legacy_worker_completion_fails_closed_on_consume_gate() {
		let mut store = seeded_store();
		store
			.append_message(message("m1", None))
			.expect("append m1");
		// Real worker_completions row with legacy ambiguous delivery payload
		store.connection.execute(
			"INSERT INTO deliveries (delivery_id, worker_id, session_id, origin_branch_id, payload_json, created_at_ms, delivered_at_ms, processed_at_ms, message_id) \
			 VALUES ('legacy-w1', 'w-leg', 's1', 'b1', '{\"kind\":\"task-result\",\"text\":\"legacy result\"}', 10, NULL, NULL, NULL)",
			[],
		).expect("insert legacy delivery");
		store.connection.execute(
			"INSERT INTO worker_completions (worker_id, session_id, origin_branch_id, delivery_id, completion_json, completed_at_ms) \
			 VALUES ('w-leg', 's1', 'b1', 'legacy-w1', '{\"status\":\"completed\"}', 10)",
			[],
		).expect("insert worker completion");

		let err = store.consume_delivery(
			"legacy-w1",
			NewMessage {
				id: "m-legacy".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m1".to_string()),
				payload: json!({ "id": "m-legacy", "text": "legacy result" }),
			},
			20,
		);
		assert!(
			matches!(err, Err(StoreError::Corrupt(msg)) if msg.contains("ambiguous legacy worker payload"))
		);
	}

	#[test]
	fn pending_worker_deliveries_excludes_peer_deliveries() {
		let mut store = seeded_store();
		store
			.enqueue_delivery(NewDelivery {
				delivery_id: "peer-1".to_string(),
				worker_id: "peer:sender".to_string(),
				session_id: "s1".to_string(),
				origin_branch_id: "b1".to_string(),
				payload: json!({ "kind": "peer-message", "from": "sender", "text": "hello" }),
			})
			.expect("enqueue peer delivery");

		store.complete_worker(WorkerCompletion {
			worker_id: "worker-1".to_string(),
			delivery_id: "worker-1".to_string(),
			session_id: "s1".to_string(),
			origin_branch_id: "b1".to_string(),
			completion: json!({ "status": "completed" }),
			delivery: json!({ "version": 2, "kind": "task-result", "mode": "background", "notify_parent": true, "text": "worker ok" }),
		}).expect("complete worker");

		let worker_pending = store
			.pending_worker_deliveries("s1")
			.expect("worker pending");
		assert_eq!(worker_pending.len(), 1);
		assert_eq!(worker_pending[0].delivery_id, "worker-1");

		let all_pending = store.pending_deliveries("s1").expect("all pending");
		assert_eq!(all_pending.len(), 2);
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
		store
			.append_message(message("m1", None))
			.expect("append m1");
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
		store
			.append_message(message("m1", None))
			.expect("append m1");
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
		store
			.append_message(message("m1", None))
			.expect("append m1");
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
		assert!(
			store
				.unresolved_intents("s1")
				.expect("unresolved")
				.is_empty()
		);

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
		assert!(
			store
				.message("m2_retry_different_id")
				.expect("read retry")
				.is_none()
		);

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
		store
			.append_message(message("m1", None))
			.expect("append m1");
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
		store
			.append_message(message("m1", None))
			.expect("append m1");
		store
			.append_message(message("m2", Some("m1")))
			.expect("append m2");

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

		let needing = store
			.executions_needing_recovery("s1")
			.expect("needing recovery");
		assert_eq!(needing.len(), 1);
		assert_eq!(needing[0].execution_id, execution_id);

		let recovered_count = store.recover_unmessaged_outcomes("s1").expect("recover");
		assert_eq!(recovered_count, 1);

		// Now no executions need recovery.
		assert!(
			store
				.executions_needing_recovery("s1")
				.expect("needing")
				.is_empty()
		);

		// Branch leaf has advanced to the recovered toolResult message.
		let branch = store.branch("b1").expect("read branch").expect("b1");
		let leaf_id = branch.leaf_message_id.expect("leaf id");
		let leaf = store
			.message(&leaf_id)
			.expect("read leaf")
			.expect("leaf message");
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
	fn recover_unresolved_intents_writes_interrupted_keeps_outcome_null() {
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
						"content": [{ "type": "toolCall", "id": "call-u", "name": "bash" }]
					}
				}),
			})
			.expect("assistant");
		let execution_id = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-u".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");

		assert_eq!(store.recover_unresolved_intents("s1").expect("recover"), 1);
		assert_eq!(store.recover_unresolved_intents("s1").expect("idempotent"), 0);

		let unresolved = store.unresolved_intents("s1").expect("unresolved");
		assert_eq!(unresolved.len(), 1);
		assert_eq!(unresolved[0].execution_id, execution_id);
		assert_eq!(store.outcome(&execution_id).expect("outcome"), None);

		let branch = store.branch("b1").expect("branch").expect("b1");
		let leaf = store
			.message(&branch.leaf_message_id.expect("leaf"))
			.expect("leaf")
			.expect("msg");
		assert_eq!(leaf.payload["message"]["role"], "toolResult");
		assert_eq!(leaf.payload["message"]["toolCallId"], "call-u");
		assert_eq!(leaf.payload["message"]["content"], SqliteStore::INTERRUPTED_UNKNOWN_OUTCOME);
		assert_eq!(leaf.payload["message"]["isError"], true);
		assert_eq!(leaf.payload["message"]["details"]["executionId"], execution_id);
		assert_eq!(leaf.payload["message"]["details"]["interruptedUnknown"], true);
	}

	#[test]
	fn recover_unresolved_intents_skips_answered_and_missing_intent() {
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
							{ "type": "toolCall", "id": "call-denied", "name": "bash" },
							{ "type": "toolCall", "id": "call-missing", "name": "bash" }
						]
					}
				}),
			})
			.expect("assistant");
		// Known denial / answered result already on branch — no intent row.
		store
			.append_message(NewMessage {
				id: "m_denied".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_denied",
					"message": {
						"role": "toolResult",
						"toolCallId": "call-denied",
						"toolName": "bash",
						"content": "Tool execution denied: approval denied for bash",
						"isError": true
					}
				}),
			})
			.expect("denied result");

		assert_eq!(store.recover_unresolved_intents("s1").expect("recover"), 0);
		assert!(
			store
				.unresolved_intents("s1")
				.expect("unresolved")
				.is_empty()
		);
		let branch = store.branch("b1").expect("branch").expect("b1");
		assert_eq!(branch.leaf_message_id.as_deref(), Some("m_denied"));
		assert_eq!(store.branch_messages("b1").expect("msgs").len(), 2);
	}

	#[test]
	fn recover_unresolved_intents_fails_closed_on_duplicate_call_ids() {
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
							{ "type": "toolCall", "id": "dup", "name": "bash" },
							{ "type": "toolCall", "id": "dup", "name": "bash" }
						]
					}
				}),
			})
			.expect("assistant");
		store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "dup".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");

		let err = store
			.recover_unresolved_intents("s1")
			.expect_err("duplicate ids fail closed");
		assert!(err.to_string().contains("ambiguous duplicate toolCallId"), "{err}");
		assert_eq!(store.branch_messages("b1").expect("msgs").len(), 1);
		assert_eq!(store.unresolved_intents("s1").expect("unresolved").len(), 1);
	}

	#[test]
	fn snapshot_read_only_tolerates_absent_provenance_table() {
		let (dir, path) = temp_db("v2-readonly");
		let mut store = SqliteStore::open(&path).expect("store");
		store
			.create_session(NewSession {
				id: "s1".to_string(),
				parent_session_id: None,
				payload: json!({ "cwd": "/tmp/project" }),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({ "kind": "root" }),
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "m1".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({"type":"message","id":"m1","message":{"role":"user","content":"hi"}}),
			})
			.expect("message");
		drop(store);
		let connection = rusqlite::Connection::open(&path).expect("raw");
		connection
			.execute_batch("DROP TABLE fork_provenance; UPDATE schema_version SET version = 2;")
			.expect("downgrade");
		drop(connection);

		let read_only = SqliteStore::open_read_only(&path).expect("read-only open");
		let snapshot = read_only
			.load_session_snapshot("s1")
			.expect("v2 snapshot without provenance table");
		assert!(snapshot.provenance.is_empty());
		assert_eq!(snapshot.session_id, "s1");
		std::fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn fork_uses_captured_snapshot_classification_not_a_target_time_reread() {
		let mut store = seeded_store();
		let exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "shared".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");
		let fork = |id: &str| {
			let message_id = format!("m-{id}");
			let branch_id = format!("b-{id}");
			ForkWrite {
				session: NewSession {
					id: id.to_string(),
					parent_session_id: Some("s1".to_string()),
					payload: json!({"type":"session","id":id,"timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp","parentSession":"s1"}),
					initial_branch_id: branch_id.clone(),
					initial_branch_payload: json!({}),
				},
				branches: vec![NewBranch {
					id: branch_id.clone(),
					session_id: id.to_string(),
					parent_branch_id: None,
					parent_message_id: None,
					payload: json!({}),
				}],
				active_branch_id: branch_id.clone(),
				active_leaf_message_id: Some(message_id.clone()),
				messages: vec![NewMessage {
					id: message_id.clone(),
					session_id: id.to_string(),
					branch_id,
					parent_message_id: None,
					payload: json!({"type":"message","id":message_id,"timestamp":"2026-01-01T00:00:00Z","message":{"role":"toolResult","toolCallId":"shared","toolName":"bash","content":"x","isError":false,"timestamp":2,"details":{"executionId":exec}}}),
				}],
				projection: None,
			}
		};

		// Captured snapshot said "resolved"; the execution row is still unresolved on
		// disk. The captured value wins and the write must not conflict.
		store
			.create_fork_atomic_with_provenance(
				fork("f-captured"),
				&[ForkProvenance::new(
					"m-f-captured".to_string(),
					exec.clone(),
					"shared".to_string(),
					true,
				)],
			)
			.expect("captured classification is authoritative");
		let snapshot = store.load_session_snapshot("f-captured").expect("snapshot");
		assert!(snapshot.provenance[0].outcome_resolved);

		// Public entry point carries no captured classification and derives a single
		// consistent value at fork time.
		store
			.create_fork_atomic(fork("f-public"))
			.expect("public fork derives");
		let snapshot = store
			.load_session_snapshot("f-public")
			.expect("public snapshot");
		assert!(!snapshot.provenance[0].outcome_resolved);

		// A source outcome that changes after the snapshot was read must not turn the
		// captured "unknown" into a conflict or a resolved row.
		store
			.record_outcome(&exec, json!({"content":"done","isError":false}))
			.expect("late outcome");
		store
			.create_fork_atomic_with_provenance(
				fork("f-late"),
				&[ForkProvenance::new(
					"m-f-late".to_string(),
					exec.clone(),
					"shared".to_string(),
					false,
				)],
			)
			.expect("late source change must not conflict");
		let snapshot = store
			.load_session_snapshot("f-late")
			.expect("late snapshot");
		assert!(!snapshot.provenance[0].outcome_resolved, "captured unknown must stay unknown");
	}

	#[test]
	fn snapshot_rejects_provenance_naming_a_different_execution_than_the_message() {
		let mut store = seeded_store();
		let exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "shared".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");
		store.append_message(NewMessage { id: "a1".to_string(), session_id: "s1".to_string(), branch_id: "b1".to_string(), parent_message_id: None, payload: json!({"type":"message","id":"a1","message":{"role":"assistant","content":[{"type":"toolCall","id":"shared","name":"bash"}]}}) }).expect("assistant");
		store.append_message(NewMessage { id: "m".to_string(), session_id: "s1".to_string(), branch_id: "b1".to_string(), parent_message_id: Some("a1".to_string()), payload: json!({"type":"message","id":"m","parentId":"a1","message":{"role":"toolResult","toolCallId":"shared","toolName":"bash","content":"x","details":{"executionId":exec}}}) }).expect("result");
		store.connection.execute("INSERT INTO fork_provenance (message_id, session_id, source_execution_id, provider_call_id, outcome_resolved) VALUES ('m', 's1', 'exec-other', 'shared', 0)", []).expect("stale provenance row");

		let err = store
			.load_session_snapshot("s1")
			.expect_err("stale tuple must not authorize");
		assert!(
			err.to_string()
				.contains("which the message does not reference"),
			"{err}"
		);
	}

	#[test]
	fn damaged_v3_database_missing_provenance_table_is_corrupt() {
		let (dir, path) = temp_db("v3-damaged");
		let mut store = SqliteStore::open(&path).expect("store");
		store
			.create_session(NewSession {
				id: "s1".to_string(),
				parent_session_id: None,
				payload: json!({ "cwd": "/tmp/project" }),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({ "kind": "root" }),
			})
			.expect("session");
		store.append_message(message("m1", None)).expect("message");
		drop(store);

		let connection = rusqlite::Connection::open(&path).expect("raw");
		connection
			.execute_batch("DROP TABLE fork_provenance;")
			.expect("damage");
		drop(connection);

		let read_only = SqliteStore::open_read_only(&path).expect("read-only open");
		let err = read_only
			.load_session_snapshot("s1")
			.expect_err("damaged v3 snapshot must fail closed");
		assert!(
			err.to_string()
				.contains("missing the fork_provenance table"),
			"{err}"
		);

		let writable = SqliteStore::open(&path).expect("writable open keeps version 3");
		let err = writable
			.load_session_snapshot("s1")
			.expect_err("damaged v3 writable snapshot must fail closed");
		assert!(
			err.to_string()
				.contains("missing the fork_provenance table"),
			"{err}"
		);
		std::fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn capture_uses_direct_parent_provenance_after_same_db_late_resolution() {
		let mut store = seeded_store();
		let exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "shared".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");
		store.create_fork_atomic_with_provenance(
			ForkWrite {
				session: NewSession { id: "f1".to_string(), parent_session_id: Some("s1".to_string()), payload: json!({"type":"session","id":"f1","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp","parentSession":"s1"}), initial_branch_id: "b".to_string(), initial_branch_payload: json!({}) },
				branches: vec![NewBranch { id: "b".to_string(), session_id: "f1".to_string(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }],
				active_branch_id: "b".to_string(),
				active_leaf_message_id: Some("m-f1".to_string()),
				messages: vec![NewMessage { id: "m-f1".to_string(), session_id: "f1".to_string(), branch_id: "b".to_string(), parent_message_id: None, payload: json!({"type":"message","id":"m-f1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"toolResult","toolCallId":"shared","toolName":"bash","content":"x","isError":false,"timestamp":2,"details":{"executionId":exec}}}) }],
				projection: None,
			},
			&[ForkProvenance::new("m-f1".to_string(), exec.clone(), "shared".to_string(), false)],
		).expect("first fork captures unknown provenance");

		// Grandparent outcome resolves later in the same database.
		store
			.record_outcome(&exec, json!({"content":"done","isError":false}))
			.expect("late outcome");
		let snapshot = store.load_session_snapshot("f1").expect("parent snapshot");
		let captured = ForkProvenance::capture(
			&snapshot.session_messages,
			&snapshot.executions,
			&snapshot.provenance,
			"m-f1",
			"m-f2",
		)
		.expect("capture must not reread the grandparent row");
		assert_eq!(captured.len(), 1);
		assert!(!captured[0].outcome_resolved, "direct-parent provenance stays immutable");
		assert_eq!(captured[0].provider_call_id, "shared");
	}

	#[test]
	fn fork_rejects_forged_provider_call_pairing_on_every_path() {
		let mut store = seeded_store();
		let exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-b".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");
		let fork = |id: &str, tool_call_id: &str| {
			let message_id = format!("m-{id}");
			ForkWrite {
				session: NewSession {
					id: id.to_string(),
					parent_session_id: Some("s1".to_string()),
					payload: json!({"type":"session","id":id,"timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp","parentSession":"s1"}),
					initial_branch_id: "b".to_string(),
					initial_branch_payload: json!({}),
				},
				branches: vec![NewBranch {
					id: "b".to_string(),
					session_id: id.to_string(),
					parent_branch_id: None,
					parent_message_id: None,
					payload: json!({}),
				}],
				active_branch_id: "b".to_string(),
				active_leaf_message_id: Some(message_id.clone()),
				messages: vec![NewMessage {
					id: message_id.clone(),
					session_id: id.to_string(),
					branch_id: "b".to_string(),
					parent_message_id: None,
					payload: json!({"type":"message","id":message_id,"timestamp":"2026-01-01T00:00:00Z","message":{"role":"toolResult","toolCallId":tool_call_id,"toolName":"bash","content":"x","isError":false,"timestamp":2,"details":{"executionId":exec}}}),
				}],
				projection: None,
			}
		};

		// Unknown call "call-a" borrowing execution B must not be classified by B's
		// captured/resolved status.
		let err = store
			.create_fork_atomic_with_provenance(
				fork("f-forged", "call-a"),
				&[ForkProvenance::new(
					"m-f-forged".to_string(),
					exec.clone(),
					"call-b".to_string(),
					true,
				)],
			)
			.expect_err("captured pairing must match the tool result");
		assert!(matches!(err, StoreError::Validation(_)), "{err:?}");

		// A supplied provider id that matches the message but not the execution is
		// also rejected by the source-session metadata check.
		let err = store
			.create_fork_atomic_with_provenance(
				fork("f-forged-meta", "call-a"),
				&[ForkProvenance::new(
					"m-f-forged-meta".to_string(),
					exec.clone(),
					"call-a".to_string(),
					true,
				)],
			)
			.expect_err("execution provider id must match captured id");
		assert!(matches!(err, StoreError::Conflict(_)), "{err:?}");

		// Public derivation path rejects the forged pairing too.
		let err = store
			.create_fork_atomic(fork("f-forged-public", "call-a"))
			.expect_err("public derivation must match provider id");
		assert!(matches!(err, StoreError::Validation(_)), "{err:?}");
		assert!(
			store.session("f-forged-public").expect("query").is_none(),
			"rejection must roll back"
		);

		// The same forgery in a live session is rejected before any fork or recovery
		// classification: a tool result naming call-a cannot borrow execution B.
		store.append_message(NewMessage { id: "a-src".to_string(), session_id: "s1".to_string(), branch_id: "b1".to_string(), parent_message_id: None, payload: json!({"type":"message","id":"a-src","message":{"role":"assistant","content":[{"type":"toolCall","id":"call-a","name":"bash"}]}}) }).expect("assistant");
		store.append_message(NewMessage { id: "r-src".to_string(), session_id: "s1".to_string(), branch_id: "b1".to_string(), parent_message_id: Some("a-src".to_string()), payload: json!({"type":"message","id":"r-src","parentId":"a-src","message":{"role":"toolResult","toolCallId":"call-a","toolName":"bash","content":"x","details":{"executionId":exec}}}) }).expect("result");
		let err = store
			.load_session_snapshot("s1")
			.expect_err("snapshot must reject the forged association");
		assert!(err.to_string().contains("does not carry provider call id"), "{err}");

		store.append_message(NewMessage { id: "a-reuse".to_string(), session_id: "s1".to_string(), branch_id: "b1".to_string(), parent_message_id: Some("r-src".to_string()), payload: json!({"type":"message","id":"a-reuse","message":{"role":"assistant","content":[{"type":"toolCall","id":"call-a","name":"bash"}]}}) }).expect("reuse assistant");
		let err = store
			.recover_unresolved_intents("s1")
			.expect_err("recovery must reject the forged association");
		assert!(err.to_string().contains("provider call id"), "{err}");
	}

	#[test]
	fn fork_provenance_cascades_on_message_deletion_without_trigger_block() {
		let (dir, path) = temp_db("provenance-cascade");
		let mut store = SqliteStore::open(&path).expect("store");
		store.create_fork_atomic_with_provenance(
			ForkWrite {
				session: NewSession { id: "f".to_string(), parent_session_id: None, payload: json!({"type":"session","id":"f","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "b".to_string(), initial_branch_payload: json!({}) },
				branches: vec![NewBranch { id: "b".to_string(), session_id: "f".to_string(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }],
				active_branch_id: "b".to_string(),
				active_leaf_message_id: Some("m".to_string()),
				messages: vec![NewMessage { id: "m".to_string(), session_id: "f".to_string(), branch_id: "b".to_string(), parent_message_id: None, payload: json!({"type":"message","id":"m","timestamp":"2026-01-01T00:00:00Z","message":{"role":"toolResult","toolCallId":"c","toolName":"bash","content":"x","isError":false,"timestamp":2,"details":{"executionId":"external"}}}) }],
				projection: None,
			},
			&[ForkProvenance::new("m".to_string(), "external".to_string(), "c".to_string(), false)],
		).expect("fork");
		drop(store);

		let connection = rusqlite::Connection::open(&path).expect("raw");
		connection
			.pragma_update(None, "foreign_keys", "ON")
			.expect("fk");
		connection
			.execute("DELETE FROM messages WHERE session_id = 'f'", [])
			.expect("session deletion contract deletes messages");
		let remaining: i64 = connection
			.query_row("SELECT COUNT(*) FROM fork_provenance WHERE session_id = 'f'", [], |row| {
				row.get(0)
			})
			.expect("count");
		assert_eq!(remaining, 0, "provenance is deleted with the session");
		connection
			.execute("DELETE FROM branches WHERE session_id = 'f'", [])
			.expect("branches");
		connection
			.execute("DELETE FROM sessions WHERE id = 'f'", [])
			.expect("session row");
		drop(connection);
		std::fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn fork_rejects_message_entry_without_timestamp_and_rolls_back() {
		let mut store = seeded_store();
		let err = store
			.create_fork_atomic(ForkWrite {
				session: NewSession { id: "f-ts".to_string(), parent_session_id: None, payload: json!({"type":"session","id":"f-ts","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "b".to_string(), initial_branch_payload: json!({}) },
				branches: vec![NewBranch { id: "b".to_string(), session_id: "f-ts".to_string(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }],
				active_branch_id: "b".to_string(),
				active_leaf_message_id: Some("m-ts".to_string()),
				messages: vec![NewMessage { id: "m-ts".to_string(), session_id: "f-ts".to_string(), branch_id: "b".to_string(), parent_message_id: None, payload: json!({"type":"message","id":"m-ts","message":{"role":"user","content":"hi","timestamp":1}}) }],
				projection: None,
			})
			.expect_err("fork must never invent a missing entry timestamp");
		assert!(matches!(err, StoreError::Validation(_)), "{err:?}");
		assert!(err.to_string().contains("entry timestamp unavailable"), "{err}");
		assert!(
			store.session("f-ts").expect("query").is_none(),
			"rejected fork must not publish a session"
		);
	}

	#[test]
	fn recover_unresolved_intents_rejects_forged_copied_provenance_pairing() {
		let mut store = seeded_store();
		store
			.create_fork_atomic_with_provenance(
				ForkWrite {
					session: NewSession { id: "f-copy".to_string(), parent_session_id: None, payload: json!({"type":"session","id":"f-copy","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "b-copy".to_string(), initial_branch_payload: json!({}) },
					branches: vec![NewBranch { id: "b-copy".to_string(), session_id: "f-copy".to_string(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }],
					active_branch_id: "b-copy".to_string(),
					active_leaf_message_id: Some("m-copy".to_string()),
					messages: vec![NewMessage { id: "m-copy".to_string(), session_id: "f-copy".to_string(), branch_id: "b-copy".to_string(), parent_message_id: None, payload: json!({"type":"message","id":"m-copy","timestamp":"2026-01-01T00:00:00Z","message":{"role":"toolResult","toolCallId":"shared","toolName":"bash","content":"x","isError":false,"timestamp":2,"details":{"executionId":"exec-copied"}}}) }],
					projection: None,
				},
				&[ForkProvenance::new("m-copy".to_string(), "exec-copied".to_string(), "shared".to_string(), false)],
			)
			.expect("fork");

		// Forge the immutable classification out of band: UPDATE is blocked by the
		// trigger, so replace the row with a provider call id the copied tool
		// result never named. No local execution row exists in the fork, so only
		// the copied provenance can answer this association.
		store
			.connection
			.execute("DELETE FROM fork_provenance WHERE message_id = 'm-copy'", [])
			.expect("delete provenance");
		store.connection.execute("INSERT INTO fork_provenance (message_id, session_id, source_execution_id, provider_call_id, outcome_resolved) VALUES ('m-copy', 'f-copy', 'exec-copied', 'other-call', 0)", []).expect("forged provenance");

		let err = store
			.recover_unresolved_intents("f-copy")
			.expect_err("copied pairing must fail closed");
		assert!(err.to_string().contains("provider call id"), "{err}");
		assert_eq!(
			store.branch_messages("b-copy").expect("msgs").len(),
			1,
			"no result may be written for a forged copy"
		);
	}

	#[test]
	fn recover_unresolved_intents_ignores_metadata_execution_references() {
		let mut store = seeded_store();
		let exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-x".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");
		store.append_message(NewMessage { id: "a1".to_string(), session_id: "s1".to_string(), branch_id: "b1".to_string(), parent_message_id: None, payload: json!({"type":"message","id":"a1","message":{"role":"assistant","content":[{"type":"toolCall","id":"call-x","name":"bash"}]}}) }).expect("assistant");
		// Metadata/custom entry that carries an execution reference but is not a
		// tool result: recovery must not classify it as one.
		store
			.append_message(NewMessage {
				id: "meta".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("a1".to_string()),
				payload: json!({"type":"custom","id":"meta","parentId":"a1","details":{"executionId":exec}}),
			})
			.expect("metadata");

		assert_eq!(
			store
				.recover_unresolved_intents("s1")
				.expect("metadata must not be classified as a tool result"),
			1
		);
		let recovered = store.branch_messages("b1").expect("msgs");
		assert_eq!(recovered.len(), 3);
		assert_eq!(recovered[2].payload["message"]["toolCallId"], "call-x");
	}

	#[test]
	fn recover_unresolved_intents_fails_closed_on_tool_result_without_call_id() {
		let mut store = seeded_store();
		let exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-y".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");
		store.append_message(NewMessage { id: "r-orphan".to_string(), session_id: "s1".to_string(), branch_id: "b1".to_string(), parent_message_id: None, payload: json!({"type":"message","id":"r-orphan","message":{"role":"toolResult","toolName":"bash","content":"x","details":{"executionId":exec}}}) }).expect("result");

		let err = store
			.recover_unresolved_intents("s1")
			.expect_err("tool result without a call id must fail closed");
		assert!(err.to_string().contains("without a toolCallId"), "{err}");
	}

	#[test]
	fn snapshot_skips_execution_references_on_pruned_branches() {
		let mut store = seeded_store();
		store
			.append_message(NewMessage {
				id: "a1".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({"type":"message","id":"a1","message":{"role":"user","content":"hi"}}),
			})
			.expect("active message");
		store
			.create_branch(NewBranch {
				id: "b2".to_string(),
				session_id: "s1".to_string(),
				parent_branch_id: None,
				parent_message_id: None,
				payload: json!({}),
			})
			.expect("sibling branch");
		store.append_message(NewMessage { id: "orphan".to_string(), session_id: "s1".to_string(), branch_id: "b2".to_string(), parent_message_id: None, payload: json!({"type":"message","id":"orphan","message":{"role":"toolResult","toolCallId":"x","toolName":"bash","content":"x","details":{"executionId":"missing-exec"}}}) }).expect("sibling result");

		let snapshot = store
			.load_session_snapshot("s1")
			.expect("pruned branch reference must not fail the snapshot");
		assert!(snapshot.provenance.is_empty());
		assert_eq!(snapshot.branch_messages.len(), 1);
	}

	#[test]
	fn recover_unresolved_intents_ignores_forged_payload_provenance() {
		let mut store = seeded_store();
		store
			.append_message(NewMessage {
				id: "m_forged_a1".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_forged_a1",
					"message": {
						"role": "assistant",
						"content": [{ "type": "toolCall", "id": "forged-call", "name": "bash" }]
					}
				}),
			})
			.expect("assistant 1");
		store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "forged-call".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");
		store
			.append_message(NewMessage {
				id: "m_forged_r1".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_forged_a1".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_forged_r1",
					"parentId": "m_forged_a1",
					"message": {
						"role": "toolResult",
						"toolCallId": "forged-call",
						"toolName": "bash",
						"content": "forged",
						"details": {
							"executionId": "exec-missing",
							"executionProvenance": { "outcome": "resolved" }
						}
					}
				}),
			})
			.expect("forged result");
		store
			.append_message(NewMessage {
				id: "m_forged_a2".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_forged_r1".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_forged_a2",
					"message": {
						"role": "assistant",
						"content": [{ "type": "toolCall", "id": "forged-call", "name": "bash" }]
					}
				}),
			})
			.expect("assistant 2");

		let err = store
			.recover_unresolved_intents("s1")
			.expect_err("payload-provided provenance must never be trusted");
		assert!(
			err.to_string()
				.contains("unknown execution or missing fork provenance"),
			"{err}"
		);
		assert_eq!(store.branch_messages("b1").expect("msgs").len(), 3);
		assert_eq!(store.unresolved_intents("s1").expect("unresolved").len(), 1);
	}

	#[test]
	fn recover_unresolved_intents_skips_trailing_metadata() {
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
					"message": { "role": "assistant", "content": [
						{ "type": "toolCall", "id": "call-meta", "name": "bash" }
					] }
				}),
			})
			.expect("assistant");
		store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-meta".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");
		store
			.append_message(NewMessage {
				id: "meta".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({ "type": "custom", "customType": "advisor" }),
			})
			.expect("metadata");

		assert_eq!(store.recover_unresolved_intents("s1").expect("recover"), 1);
		assert_eq!(store.recover_unresolved_intents("s1").expect("idempotent"), 0);
		let messages = store.branch_messages("b1").expect("branch");
		assert_eq!(messages.last().unwrap().payload["message"]["role"], "toolResult");
		assert_eq!(messages.last().unwrap().parent_message_id.as_deref(), Some("meta"));
	}

	#[test]
	fn recover_unresolved_intents_only_touches_active_trailing_batch() {
		let mut store = seeded_store();
		store
			.append_message(NewMessage {
				id: "m_old".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_old",
					"message": {
						"role": "assistant",
						"content": [{ "type": "toolCall", "id": "call-old", "name": "bash" }]
					}
				}),
			})
			.expect("old assistant");
		let old_exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-old".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("old intent");
		store
			.append_message(NewMessage {
				id: "m_user".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_old".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_user",
					"message": { "role": "user", "content": "next" }
				}),
			})
			.expect("user");
		store
			.append_message(NewMessage {
				id: "m_new".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_user".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_new",
					"message": {
						"role": "assistant",
						"content": [{ "type": "toolCall", "id": "call-new", "name": "bash" }]
					}
				}),
			})
			.expect("new assistant");
		let new_exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call-new".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("new intent");

		assert_eq!(store.recover_unresolved_intents("s1").expect("recover"), 1);

		let unresolved = store.unresolved_intents("s1").expect("unresolved");
		assert_eq!(unresolved.len(), 2, "both intents stay unresolved");
		assert!(unresolved.iter().any(|u| u.execution_id == old_exec));
		assert!(unresolved.iter().any(|u| u.execution_id == new_exec));

		let results: Vec<_> = store
			.branch_messages("b1")
			.expect("msgs")
			.into_iter()
			.filter(|m| m.payload["message"]["role"] == "toolResult")
			.collect();
		assert_eq!(results.len(), 1);
		assert_eq!(results[0].payload["message"]["toolCallId"], "call-new");
		assert_eq!(store.outcome(&old_exec).expect("old"), None);
		assert_eq!(store.outcome(&new_exec).expect("new"), None);
	}

	#[test]
	fn recover_unresolved_intents_leaves_kept_for_reconcile_mixed_batch() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		store
			.create_session(NewSession {
				id: "s_mix".to_string(),
				parent_session_id: None,
				payload: json!({
					"cwd": "/tmp",
					"durableProjection": {
						"kept": ["m_asst"],
						"synthetic": []
					}
				}),
				initial_branch_id: "b_mix".to_string(),
				initial_branch_payload: Value::Null,
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s_mix".to_string(),
				branch_id: "b_mix".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": [
							{ "type": "toolCall", "id": "call_ok", "name": "bash" },
							{ "type": "toolCall", "id": "call_fail", "name": "bash" }
						]
					}
				}),
			})
			.expect("assistant");
		// Sibling result already on the branch (finish_execution committed) but
		// not yet in durableProjection.kept (crash before set_projection).
		store
			.append_message(NewMessage {
				id: "m_ok".to_string(),
				session_id: "s_mix".to_string(),
				branch_id: "b_mix".to_string(),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_ok",
					"message": {
						"role": "toolResult",
						"toolCallId": "call_ok",
						"toolName": "bash",
						"content": "ok",
						"isError": false
					}
				}),
			})
			.expect("ok result");
		let fail_exec = store
			.record_intent(NewIntent {
				session_id: "s_mix".to_string(),
				branch_id: "b_mix".to_string(),
				provider_call_id: "call_fail".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("fail intent");

		assert_eq!(store.recover_unresolved_intents("s_mix").expect("recover"), 1);
		// kept must still end at the assistant so reconcile can see m_ok + synthetic.
		let kept_before =
			store.session("s_mix").expect("session").expect("s").payload["durableProjection"]["kept"]
				.clone();
		assert_eq!(kept_before, json!(["m_asst"]));

		let backfilled = store
			.reconcile_durable_projection("s_mix")
			.expect("reconcile");
		assert_eq!(backfilled, 2, "sibling result and synthetic interrupted must both backfill");

		let session = store.session("s_mix").expect("session").expect("s");
		let kept = session.payload["durableProjection"]["kept"]
			.as_array()
			.expect("kept")
			.iter()
			.filter_map(Value::as_str)
			.collect::<Vec<_>>();
		assert_eq!(kept[0], "m_asst");
		assert_eq!(kept[1], "m_ok");
		assert!(kept[2].starts_with("msg-int-"), "synthetic interrupted id: {}", kept[2]);
		assert_eq!(kept[2], format!("msg-int-{fail_exec}"));
		assert_eq!(store.outcome(&fail_exec).expect("outcome"), None);
	}

	#[test]
	fn recover_unresolved_intents_refuses_reused_call_id_from_earlier_unresolved() {
		let mut store = seeded_store();
		// Older trailing batch admitted an intent for "shared", then conversation continued.
		store
			.append_message(NewMessage {
				id: "m_old".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_old",
					"message": {
						"role": "assistant",
						"content": [{ "type": "toolCall", "id": "shared", "name": "bash" }]
					}
				}),
			})
			.expect("old assistant");
		let old_exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "shared".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("old intent");
		store
			.append_message(NewMessage {
				id: "m_user".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_old".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_user",
					"message": { "role": "user", "content": "next" }
				}),
			})
			.expect("user");
		// Newer trailing call reuses the same provider id but never admitted an intent
		// (crash before begin_execution, or filter denial without a persisted result).
		store
			.append_message(NewMessage {
				id: "m_new".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_user".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_new",
					"message": {
						"role": "assistant",
						"content": [{ "type": "toolCall", "id": "shared", "name": "bash" }]
					}
				}),
			})
			.expect("new assistant");

		let err = store
			.recover_unresolved_intents("s1")
			.expect_err("reused id must fail closed");
		assert!(err.to_string().contains("not uniquely attributable"), "{err}");

		// No synthetic toolResult; historical unresolved intent untouched.
		assert!(
			store
				.branch_messages("b1")
				.expect("msgs")
				.iter()
				.all(|m| m.payload["message"]["role"] != "toolResult"),
			"must not synthesize a result for the newer missing-intent call"
		);
		let unresolved = store.unresolved_intents("s1").expect("unresolved");
		assert_eq!(unresolved.len(), 1);
		assert_eq!(unresolved[0].execution_id, old_exec);
		assert_eq!(store.outcome(&old_exec).expect("outcome"), None);
		assert_eq!(
			store
				.branch("b1")
				.expect("branch")
				.expect("b1")
				.leaf_message_id
				.as_deref(),
			Some("m_new")
		);
	}

	#[test]
	fn recover_unresolved_intents_refuses_reused_call_id_closed_by_interrupted_result() {
		let mut store = seeded_store();
		store
			.append_message(NewMessage {
				id: "m_a1".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_a1",
					"message": {
						"role": "assistant",
						"content": [{ "type": "toolCall", "id": "shared", "name": "bash" }]
					}
				}),
			})
			.expect("assistant 1");
		let exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "shared".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");
		assert_eq!(
			store
				.recover_unresolved_intents("s1")
				.expect("first recovery"),
			1
		);
		assert_eq!(
			store.outcome(&exec).expect("outcome"),
			None,
			"interrupted recovery keeps the outcome unresolved"
		);
		let leaf_id = store
			.branch("b1")
			.expect("branch")
			.expect("b1")
			.leaf_message_id
			.expect("recovered leaf");

		// Newer call reuses the provider id but was never admitted (no intent row).
		store
			.append_message(NewMessage {
				id: "m_a2".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some(leaf_id),
				payload: json!({
					"type": "message",
					"id": "m_a2",
					"message": {
						"role": "assistant",
						"content": [{ "type": "toolCall", "id": "shared", "name": "bash" }]
					}
				}),
			})
			.expect("assistant 2");

		let err = store.recover_unresolved_intents("s1").expect_err(
			"recovery-synthesized result must not answer the historical unresolved intent",
		);
		assert!(err.to_string().contains("not uniquely attributable"), "{err}");

		let results = store
			.branch_messages("b1")
			.expect("msgs")
			.iter()
			.filter(|m| m.payload["message"]["role"] == json!("toolResult"))
			.count();
		assert_eq!(results, 1, "no second recovered result may be appended");
		let unresolved = store.unresolved_intents("s1").expect("unresolved");
		assert_eq!(unresolved.len(), 1);
		assert_eq!(unresolved[0].execution_id, exec);
		assert_eq!(store.outcome(&exec).expect("outcome"), None);
		assert_eq!(
			store
				.branch("b1")
				.expect("branch")
				.expect("b1")
				.leaf_message_id
				.as_deref(),
			Some("m_a2")
		);
	}

	#[test]
	fn recover_unresolved_intents_refuses_duplicate_declarations_with_partial_answers() {
		let mut store = seeded_store();
		store
			.append_message(NewMessage {
				id: "m_a1".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_a1",
					"message": {
						"role": "assistant",
						"content": [
							{ "type": "toolCall", "id": "dup", "name": "bash" },
							{ "type": "toolCall", "id": "dup", "name": "bash" }
						]
					}
				}),
			})
			.expect("assistant with duplicate declarations");
		let old_exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "dup".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("old intent");
		store
			.append_message(NewMessage {
				id: "m_res".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_a1".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_res",
					"message": { "role": "toolResult", "toolCallId": "dup", "toolName": "bash", "content": "done" }
				}),
			})
			.expect("one of two answers");
		store
			.append_message(NewMessage {
				id: "m_a2".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_res".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_a2",
					"message": { "role": "assistant", "content": [{ "type": "toolCall", "id": "dup", "name": "bash" }] }
				}),
			})
			.expect("trailing assistant");

		let err = store
			.recover_unresolved_intents("s1")
			.expect_err("one answer cannot cover two earlier declarations");
		assert!(err.to_string().contains("not uniquely attributable"), "{err}");
		assert_eq!(
			store
				.branch_messages("b1")
				.expect("msgs")
				.iter()
				.filter(|m| m.payload["message"]["role"] == json!("toolResult"))
				.count(),
			1,
			"no synthetic result may be appended"
		);
		assert_eq!(store.unresolved_intents("s1").expect("unresolved")[0].execution_id, old_exec);
		assert_eq!(store.outcome(&old_exec).expect("outcome"), None);
	}

	#[test]
	fn recover_unresolved_intents_refuses_when_older_declaration_remains_unanswered() {
		let mut store = seeded_store();
		store
			.append_message(NewMessage {
				id: "m_a1".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_a1",
					"message": { "role": "assistant", "content": [{ "type": "toolCall", "id": "shared", "name": "bash" }] }
				}),
			})
			.expect("oldest unresolved declaration");
		let old_exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "shared".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("old intent");
		store
			.append_message(NewMessage {
				id: "m_a2".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_a1".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_a2",
					"message": { "role": "assistant", "content": [{ "type": "toolCall", "id": "shared", "name": "bash" }] }
				}),
			})
			.expect("answered declaration");
		store
			.append_message(NewMessage {
				id: "m_res".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_a2".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_res",
					"message": { "role": "toolResult", "toolCallId": "shared", "toolName": "bash", "content": "done" }
				}),
			})
			.expect("answer for the newer declaration");
		store
			.append_message(NewMessage {
				id: "m_a3".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_res".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_a3",
					"message": { "role": "assistant", "content": [{ "type": "toolCall", "id": "shared", "name": "bash" }] }
				}),
			})
			.expect("trailing assistant");

		let err = store
			.recover_unresolved_intents("s1")
			.expect_err("the answer belongs to the newer declaration, not the oldest");
		assert!(err.to_string().contains("not uniquely attributable"), "{err}");
		assert_eq!(store.unresolved_intents("s1").expect("unresolved")[0].execution_id, old_exec);
		assert_eq!(store.outcome(&old_exec).expect("outcome"), None);
	}

	#[test]
	fn recover_unresolved_intents_allows_reuse_after_resolved_execution_result() {
		let mut store = seeded_store();
		store
			.append_message(NewMessage {
				id: "m_a1".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_a1",
					"message": { "role": "assistant", "content": [{ "type": "toolCall", "id": "shared", "name": "bash" }] }
				}),
			})
			.expect("old assistant");
		let old_exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "shared".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("old intent");
		store
			.record_outcome(&old_exec, json!({ "content": "done", "isError": false }))
			.expect("old outcome");
		store
			.append_message(NewMessage {
				id: "m_res".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_a1".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_res",
					"message": {
						"role": "toolResult",
						"toolCallId": "shared",
						"toolName": "bash",
						"content": "done",
						"details": { "executionId": old_exec }
					}
				}),
			})
			.expect("verified answer");
		store
			.append_message(NewMessage {
				id: "m_a2".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_res".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_a2",
					"message": { "role": "assistant", "content": [{ "type": "toolCall", "id": "shared", "name": "bash" }] }
				}),
			})
			.expect("trailing assistant");
		let new_exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "shared".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("new intent");

		assert_eq!(store.recover_unresolved_intents("s1").expect("recover"), 1);
		let leaf_id = store
			.branch("b1")
			.expect("branch")
			.expect("b1")
			.leaf_message_id
			.expect("recovered leaf");
		let leaf = store.message(&leaf_id).expect("message").expect("leaf");
		assert_eq!(leaf.payload["message"]["details"]["executionId"], new_exec);
		assert_eq!(store.outcome(&old_exec).expect("old outcome").is_some(), true);
		assert_eq!(store.outcome(&new_exec).expect("new outcome"), None);
	}

	#[test]
	fn recover_unresolved_intents_allows_answered_earlier_reuse() {
		let mut store = seeded_store();
		// Earlier turn declared "shared" and got a toolResult.
		store
			.append_message(NewMessage {
				id: "m_old".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"type": "message",
					"id": "m_old",
					"message": {
						"role": "assistant",
						"content": [{ "type": "toolCall", "id": "shared", "name": "bash" }]
					}
				}),
			})
			.expect("old assistant");
		let old_exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "shared".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("old intent");
		store
			.record_outcome(&old_exec, json!({ "content": "done", "isError": false }))
			.expect("old outcome");
		store
			.append_message(NewMessage {
				id: "m_old_res".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_old".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_old_res",
					"message": {
						"role": "toolResult",
						"toolCallId": "shared",
						"toolName": "bash",
						"content": "done"
					}
				}),
			})
			.expect("old result");
		store
			.append_message(NewMessage {
				id: "m_user".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_old_res".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_user",
					"message": { "role": "user", "content": "next" }
				}),
			})
			.expect("user");
		// Trailing assistant reuses provider id; unique unresolved intent belongs here.
		store
			.append_message(NewMessage {
				id: "m_new".to_string(),
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_user".to_string()),
				payload: json!({
					"type": "message",
					"id": "m_new",
					"message": {
						"role": "assistant",
						"content": [{ "type": "toolCall", "id": "shared", "name": "bash" }]
					}
				}),
			})
			.expect("new assistant");
		let new_exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "shared".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("new intent");

		assert_eq!(store.recover_unresolved_intents("s1").expect("recover"), 1);
		let msgs = store.branch_messages("b1").expect("msgs");
		let tool = msgs.last().expect("leaf");
		assert_eq!(tool.payload["message"]["role"], "toolResult");
		assert_eq!(tool.payload["message"]["toolCallId"], "shared");
		assert_eq!(tool.payload["message"]["content"], SqliteStore::INTERRUPTED_UNKNOWN_OUTCOME);
		assert_eq!(store.outcome(&new_exec).expect("outcome"), None);
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
	fn recover_unmessaged_outcomes_uses_inherited_parent_point_for_child_branch() {
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
						"content": [{ "type": "toolCall", "id": "call-A", "name": "bash" }]
					}
				}),
			})
			.expect("append assistant");
		store
			.create_branch(NewBranch {
				id: "b2".to_string(),
				session_id: "s1".to_string(),
				parent_branch_id: Some("b1".to_string()),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({}),
			})
			.expect("child branch inheriting parent point");
		store
			.set_active_branch("s1", "b2")
			.expect("activate child branch");

		let exec = store
			.record_intent(NewIntent {
				session_id: "s1".to_string(),
				branch_id: "b2".to_string(),
				provider_call_id: "call-A".to_string(),
				tool_name: "bash".to_string(),
				payload: json!({}),
			})
			.expect("intent");
		store
			.record_outcome(&exec, json!({ "content": "ok", "isError": false }))
			.expect("outcome");

		let recovered = store.recover_unmessaged_outcomes("s1").expect("recover");
		assert_eq!(recovered, 1, "child branch leaf is NULL but inherits m_asst as effective leaf");

		let child = store.branch("b2").expect("branch").expect("b2");
		let leaf_id = child
			.leaf_message_id
			.expect("recovered leaf recorded on child branch");
		let leaf = store.message(&leaf_id).expect("message").expect("leaf");
		assert_eq!(leaf.parent_message_id.as_deref(), Some("m_asst"));
		assert_eq!(leaf.payload["message"]["toolCallId"], "call-A");
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
		let needing = store
			.executions_needing_recovery("s1")
			.expect("needing recovery");
		assert!(
			needing.is_empty(),
			"execution must not need recovery when legacy message is present"
		);

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
		let leaf_b = store
			.message(&leaf_id)
			.expect("message")
			.expect("leaf message");
		assert_eq!(leaf_b.parent_message_id.as_deref(), Some("m_tool_A"));
		assert_eq!(leaf_b.payload["message"]["toolCallId"], "call-B");
		assert_eq!(leaf_b.payload["message"]["details"]["executionId"], exec_b);

		// No more unmessaged outcomes:
		assert!(
			store
				.executions_needing_recovery("s1")
				.expect("needing")
				.is_empty()
		);
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
		let leaf_b = store
			.message(&leaf_id)
			.expect("message")
			.expect("leaf message");
		assert_eq!(leaf_b.payload["message"]["toolCallId"], "call-B");

		let leaf_a_id = leaf_b.parent_message_id.expect("parent A");
		let leaf_a = store
			.message(&leaf_a_id)
			.expect("message")
			.expect("message A");
		assert_eq!(leaf_a.parent_message_id.as_deref(), Some("m_asst"));
		assert_eq!(leaf_a.payload["message"]["toolCallId"], "call-A");

		assert!(
			store
				.executions_needing_recovery("s1")
				.expect("needing")
				.is_empty()
		);
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
						"kept": ["m_asst"],
						"synthetic": []
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

		let recovered = store
			.recover_unmessaged_outcomes("s_proj")
			.expect("recover");
		assert_eq!(recovered, 1);

		let expected_msg_id = format!("msg-rec-{}", execution_id);

		// Verify durableProjection.kept was updated atomically with the message:
		let session_rec = store
			.session("s_proj")
			.expect("read session")
			.expect("session");
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
						"kept": ["m_asst"],
						"synthetic": []
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
		let backfilled = store
			.reconcile_durable_projection("s_meta")
			.expect("reconcile");
		// Exactly 2 messages backfilled (m_tool_1 and m_asst_2), metadata rows m_meta_1 and m_meta_2 skipped!
		assert_eq!(backfilled, 2);

		let session = store.session("s_meta").expect("session").expect("s_meta");
		let kept = session.payload["durableProjection"]["kept"]
			.as_array()
			.expect("kept array");
		let kept_ids: Vec<&str> = kept.iter().filter_map(Value::as_str).collect();
		assert_eq!(kept_ids, vec!["m_asst", "m_tool_1", "m_asst_2"]);
		assert!(!kept_ids.contains(&"m_meta_1"));
		assert!(!kept_ids.contains(&"m_meta_2"));
	}

	#[test]
	fn reconcile_durable_projection_fails_closed_on_stale_diverged_projection_preserving_history() {
		let mut store = SqliteStore::open_in_memory().expect("open store");
		store
			.create_session(NewSession {
				id: "s_diverge".to_string(),
				parent_session_id: None,
				payload: json!({
					"durableProjection": {
						"kept": ["m_user", "m_stale_branch1_msg"],
						"synthetic": []
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

		let error = store
			.reconcile_durable_projection("s_diverge")
			.expect_err("stale diverged projection must fail closed");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");

		let session = store
			.session("s_diverge")
			.expect("session")
			.expect("s_diverge");
		assert!(session.payload.get("durableProjection").is_some());

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
		assert!(
			projection.is_some(),
			"durableProjection must be preserved for valid sibling toolResult"
		);
		let kept = projection.unwrap()["kept"].as_array().expect("kept array");
		assert_eq!(kept, json!(["m_asst", "m_tool_offbranch"]).as_array().unwrap());
	}

	#[test]
	fn reconcile_durable_projection_rejects_reused_call_id_from_unrelated_branch() {
		let mut store = SqliteStore::open_in_memory().expect("open store");
		store
			.create_session(NewSession {
				id: "s_reuse".into(),
				parent_session_id: None,
				payload: json!({"durableProjection":{"kept":["a1","r1"]}}),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "a1".into(),
				session_id: "s_reuse".into(),
				branch_id: "b1".into(),
				parent_message_id: None,
				payload: json!({"message":{"role":"assistant","content":[{"type":"toolCall","id":"same"}]}}),
			})
			.expect("assistant");
		store
			.create_branch(NewBranch {
				id: "b2".into(),
				session_id: "s_reuse".into(),
				parent_branch_id: Some("b1".into()),
				parent_message_id: Some("a1".into()),
				payload: json!({}),
			})
			.expect("branch");
		store
			.append_message(NewMessage {
				id: "a2".into(),
				session_id: "s_reuse".into(),
				branch_id: "b2".into(),
				parent_message_id: Some("a1".into()),
				payload: json!({"message":{"role":"assistant","content":[{"type":"toolCall","id":"same"}]}}),
			})
			.expect("unrelated assistant");
		store
			.append_message(NewMessage {
				id: "r1".into(),
				session_id: "s_reuse".into(),
				branch_id: "b2".into(),
				parent_message_id: Some("a2".into()),
				payload: json!({"message":{"role":"toolResult","toolCallId":"same"}}),
			})
			.expect("result");
		let error = store
			.reconcile_durable_projection("s_reuse")
			.expect_err("reused call id must fail closed");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");
		assert!(
			store
				.session("s_reuse")
				.expect("get")
				.expect("session")
				.payload
				.get("durableProjection")
				.is_some()
		);
	}

	#[test]
	fn reconcile_durable_projection_requires_synthetic_pending_call_for_offbranch_result() {
		let seed_offbranch = |store: &mut SqliteStore, id: &str, synthetic: Value| {
			store
				.create_session(NewSession {
					id: id.into(),
					parent_session_id: None,
					payload: json!({"durableProjection":{"kept":["r1","u1"],"synthetic":synthetic}}),
					initial_branch_id: "b1".into(),
					initial_branch_payload: json!({}),
				})
				.expect("session");
			store
				.append_message(NewMessage {
					id: "a1".into(),
					session_id: id.into(),
					branch_id: "b1".into(),
					parent_message_id: None,
					payload: json!({"message":{"role":"assistant","content":[{"type":"toolCall","id":"same"}]}}),
				})
				.expect("assistant");
			store
				.append_message(NewMessage {
					id: "u1".into(),
					session_id: id.into(),
					branch_id: "b1".into(),
					parent_message_id: Some("a1".into()),
					payload: json!({"message":{"role":"user","content":"next"}}),
				})
				.expect("user");
			store
				.create_branch(NewBranch {
					id: "b2".into(),
					session_id: id.into(),
					parent_branch_id: Some("b1".into()),
					parent_message_id: Some("a1".into()),
					payload: json!({}),
				})
				.expect("branch");
			store
				.append_message(NewMessage {
					id: "r1".into(),
					session_id: id.into(),
					branch_id: "b2".into(),
					parent_message_id: Some("a1".into()),
					payload: json!({"message":{"role":"toolResult","toolCallId":"same"}}),
				})
				.expect("result");
		};
		let synthetic_call = serde_json::to_value(pi_agent::Message::assistant_with_tools(
			"prefix",
			vec![pi_agent::ToolCall { id: "same".into(), name: "bash".into(), arguments: json!({}) }],
		))
		.expect("synthetic");

		let mut absent = SqliteStore::open_in_memory().expect("open");
		seed_offbranch(&mut absent, "s_no_synthetic", json!([]));
		let error = absent
			.reconcile_durable_projection("s_no_synthetic")
			.expect_err("off-branch result without a pending synthetic call must fail closed");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");
		assert!(
			absent
				.session("s_no_synthetic")
				.expect("get")
				.expect("session")
				.payload
				.get("durableProjection")
				.is_some()
		);

		let mut present = SqliteStore::open_in_memory().expect("open");
		seed_offbranch(&mut present, "s_synthetic", json!([synthetic_call]));
		present
			.reconcile_durable_projection("s_synthetic")
			.expect("reconcile");
		let session = present
			.session("s_synthetic")
			.expect("get")
			.expect("session");
		assert_eq!(
			session.payload["durableProjection"]["kept"],
			json!(["r1", "u1"]),
			"pending synthetic call must authorize the off-branch result"
		);
	}

	#[test]
	fn reconcile_durable_projection_consumes_synthetic_call_by_onbranch_result() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		let synthetic_call = serde_json::to_value(pi_agent::Message::assistant_with_tools(
			"prefix",
			vec![pi_agent::ToolCall { id: "same".into(), name: "bash".into(), arguments: json!({}) }],
		))
		.expect("synthetic");
		store
			.create_session(NewSession {
				id: "s_dup_synth".into(),
				parent_session_id: None,
				payload: json!({"durableProjection":{"kept":["r1","r2"],"synthetic":[synthetic_call]}}),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "a1".into(),
				session_id: "s_dup_synth".into(),
				branch_id: "b1".into(),
				parent_message_id: None,
				payload: json!({"message":{"role":"assistant","content":[{"type":"toolCall","id":"same"}]}}),
			})
			.expect("assistant");
		store
			.append_message(NewMessage {
				id: "r1".into(),
				session_id: "s_dup_synth".into(),
				branch_id: "b1".into(),
				parent_message_id: Some("a1".into()),
				payload: json!({"message":{"role":"toolResult","toolCallId":"same"}}),
			})
			.expect("on-branch result");
		store
			.create_branch(NewBranch {
				id: "b2".into(),
				session_id: "s_dup_synth".into(),
				parent_branch_id: Some("b1".into()),
				parent_message_id: Some("a1".into()),
				payload: json!({}),
			})
			.expect("branch");
		store
			.append_message(NewMessage {
				id: "r2".into(),
				session_id: "s_dup_synth".into(),
				branch_id: "b2".into(),
				parent_message_id: Some("a1".into()),
				payload: json!({"message":{"role":"toolResult","toolCallId":"same"}}),
			})
			.expect("off-branch result");
		let error = store
			.reconcile_durable_projection("s_dup_synth")
			.expect_err("one pending synthetic call must not authorize two tool results");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");
		assert!(
			store
				.session("s_dup_synth")
				.expect("get")
				.expect("session")
				.payload
				.get("durableProjection")
				.is_some()
		);
	}

	#[test]
	fn reconcile_durable_projection_rejects_offbranch_result_after_synthetic_interruption() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		let synthetic_call = serde_json::to_value(pi_agent::Message::assistant_with_tools(
			"prefix",
			vec![pi_agent::ToolCall { id: "same".into(), name: "bash".into(), arguments: json!({}) }],
		))
		.expect("synthetic");
		store
			.create_session(NewSession {
				id: "s_interrupt".into(),
				parent_session_id: None,
				payload: json!({"durableProjection":{"kept":["u1","r2"],"synthetic":[synthetic_call]}}),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "a1".into(),
				session_id: "s_interrupt".into(),
				branch_id: "b1".into(),
				parent_message_id: None,
				payload: json!({"message":{"role":"assistant","content":[{"type":"toolCall","id":"same"}]}}),
			})
			.expect("assistant");
		store
			.append_message(NewMessage {
				id: "u1".into(),
				session_id: "s_interrupt".into(),
				branch_id: "b1".into(),
				parent_message_id: Some("a1".into()),
				payload: json!({"message":{"role":"user","content":"next"}}),
			})
			.expect("user interruption");
		store
			.create_branch(NewBranch {
				id: "b2".into(),
				session_id: "s_interrupt".into(),
				parent_branch_id: Some("b1".into()),
				parent_message_id: Some("a1".into()),
				payload: json!({}),
			})
			.expect("branch");
		store
			.append_message(NewMessage {
				id: "r2".into(),
				session_id: "s_interrupt".into(),
				branch_id: "b2".into(),
				parent_message_id: Some("a1".into()),
				payload: json!({"message":{"role":"toolResult","toolCallId":"same"}}),
			})
			.expect("off-branch result");
		let error = store
			.reconcile_durable_projection("s_interrupt")
			.expect_err("a user interruption invalidates pending synthetic calls");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");
		assert!(
			store
				.session("s_interrupt")
				.expect("get")
				.expect("session")
				.payload
				.get("durableProjection")
				.is_some()
		);
	}

	#[test]
	fn reconcile_durable_projection_rejects_synthetic_prefix_with_unmatched_tool_result() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		let call = serde_json::to_value(pi_agent::Message::assistant_with_tools(
			"prefix",
			vec![pi_agent::ToolCall { id: "same".into(), name: "bash".into(), arguments: json!({}) }],
		))
		.expect("synthetic call");
		let orphan_result =
			serde_json::to_value(pi_agent::Message::tool_with_status("other", "bash", "ok", false))
				.expect("synthetic result");
		let header =
			json!({"durableProjection":{"kept":["r1","u1"],"synthetic":[call, orphan_result]}});
		store
			.create_session(NewSession {
				id: "s_bad_prefix".into(),
				parent_session_id: None,
				payload: header.clone(),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "a1".into(),
				session_id: "s_bad_prefix".into(),
				branch_id: "b1".into(),
				parent_message_id: None,
				payload: json!({"message":{"role":"assistant","content":[{"type":"toolCall","id":"same"}]}}),
			})
			.expect("assistant");
		store
			.append_message(NewMessage {
				id: "u1".into(),
				session_id: "s_bad_prefix".into(),
				branch_id: "b1".into(),
				parent_message_id: Some("a1".into()),
				payload: json!({"message":{"role":"user","content":"next"}}),
			})
			.expect("user");
		store
			.create_branch(NewBranch {
				id: "b2".into(),
				session_id: "s_bad_prefix".into(),
				parent_branch_id: Some("b1".into()),
				parent_message_id: Some("a1".into()),
				payload: json!({}),
			})
			.expect("branch");
		store
			.append_message(NewMessage {
				id: "r1".into(),
				session_id: "s_bad_prefix".into(),
				branch_id: "b2".into(),
				parent_message_id: Some("a1".into()),
				payload: json!({"message":{"role":"toolResult","toolCallId":"same"}}),
			})
			.expect("result");
		let error = store
			.reconcile_durable_projection("s_bad_prefix")
			.expect_err("unmatched synthetic tool result must fail closed");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");
		let session = store
			.session("s_bad_prefix")
			.expect("get")
			.expect("session");
		assert_eq!(
			session.payload.get("durableProjection"),
			header.get("durableProjection"),
			"failed validation must not mutate the projection or header"
		);
	}

	#[test]
	fn reconcile_durable_projection_rejects_synthetic_only_prefix_with_dangling_call() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		let call = serde_json::to_value(pi_agent::Message::assistant_with_tools(
			"prefix",
			vec![pi_agent::ToolCall {
				id: "dangling".into(),
				name: "bash".into(),
				arguments: json!({}),
			}],
		))
		.expect("synthetic call");
		let header = json!({"durableProjection":{"kept":[],"synthetic":[call],"anchor":"m_user"}});
		store
			.create_session(NewSession {
				id: "s_empty_kept".into(),
				parent_session_id: None,
				payload: header.clone(),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "m_user".into(),
				session_id: "s_empty_kept".into(),
				branch_id: "b1".into(),
				parent_message_id: None,
				payload: json!({"message":{"role":"user","content":"hi"}}),
			})
			.expect("user");
		let error = store
			.reconcile_durable_projection("s_empty_kept")
			.expect_err("synthetic-only prefix with a dangling call must fail closed");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");
		let session = store
			.session("s_empty_kept")
			.expect("get")
			.expect("session");
		assert_eq!(
			session.payload.get("durableProjection"),
			header.get("durableProjection"),
			"failed validation must not mutate the projection or header"
		);
	}

	#[test]
	fn stored_projection_accepts_missing_arrays_and_legacy_anchor() {
		let empty = parse_stored_projection(&json!({})).expect("absent arrays use defaults");
		assert!(empty.kept.is_empty());
		assert!(empty.synthetic.is_empty());
		assert!(empty.anchor.is_none());

		let legacy =
			parse_stored_projection(&json!({"firstKeptEntryId": "m1"})).expect("legacy anchor alias");
		assert_eq!(legacy.anchor.as_deref(), Some("m1"));

		let explicit = parse_stored_projection(&json!({"anchor": "a", "firstKeptEntryId": "b"}))
			.expect("explicit anchor");
		assert_eq!(
			explicit.anchor.as_deref(),
			Some("a"),
			"explicit anchor must win over the legacy alias"
		);

		let reordered = parse_stored_projection(&json!({"firstKeptEntryId": "b", "anchor": "a"}))
			.expect("both fields regardless of order");
		assert_eq!(reordered.anchor.as_deref(), Some("a"));

		let null_wins = parse_stored_projection(&json!({"anchor": null, "firstKeptEntryId": "b"}))
			.expect("explicit null anchor");
		assert_eq!(
			null_wins.anchor, None,
			"an explicit null anchor must not be filled from the legacy field"
		);

		assert!(
			parse_stored_projection(&json!({"kept": "nope"})).is_err(),
			"wrong present type must fail"
		);
		assert!(
			parse_stored_projection(&json!({"synthetic": 5})).is_err(),
			"wrong present type must fail"
		);
		assert!(
			parse_stored_projection(&json!({"firstKeptEntryId": 5})).is_err(),
			"wrong legacy anchor type must fail"
		);
	}

	#[test]
	fn snapshot_reads_legacy_anchor_alias_without_rewriting_header() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		let header = json!({"durableProjection": {"firstKeptEntryId": "m1"}});
		store
			.create_session(NewSession {
				id: "s_legacy_anchor".into(),
				parent_session_id: None,
				payload: header.clone(),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "m1".into(),
				session_id: "s_legacy_anchor".into(),
				branch_id: "b1".into(),
				parent_message_id: None,
				payload: json!({"type":"message","id":"m1","message":{"role":"user","content":"hi"}}),
			})
			.expect("message");
		let snapshot = store
			.load_session_snapshot("s_legacy_anchor")
			.expect("snapshot");
		assert_eq!(snapshot.projection.expect("projection").anchor.as_deref(), Some("m1"));
		let session = store
			.session("s_legacy_anchor")
			.expect("get")
			.expect("session");
		assert_eq!(session.payload, header, "snapshot read must not rewrite the header");
	}

	#[test]
	fn reconcile_accepts_projection_with_missing_arrays() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		let header = json!({"durableProjection": {"anchor": "m1"}});
		store
			.create_session(NewSession {
				id: "s_missing_arrays".into(),
				parent_session_id: None,
				payload: header.clone(),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "m1".into(),
				session_id: "s_missing_arrays".into(),
				branch_id: "b1".into(),
				parent_message_id: None,
				payload: json!({"type":"message","id":"m1","message":{"role":"user","content":"one"}}),
			})
			.expect("m1");
		assert_eq!(
			store
				.reconcile_durable_projection("s_missing_arrays")
				.expect("reconcile"),
			0
		);
		assert_eq!(
			store
				.session("s_missing_arrays")
				.expect("get")
				.expect("session")
				.payload,
			header,
			"nothing to backfill must not rewrite the header"
		);

		store.append_message(NewMessage { id: "m2".into(), session_id: "s_missing_arrays".into(), branch_id: "b1".into(), parent_message_id: Some("m1".into()), payload: json!({"type":"message","id":"m2","parentId":"m1","message":{"role":"user","content":"two"}}) }).expect("m2");
		assert_eq!(
			store
				.reconcile_durable_projection("s_missing_arrays")
				.expect("reconcile"),
			1
		);
		let session = store
			.session("s_missing_arrays")
			.expect("get")
			.expect("session");
		assert_eq!(session.payload["durableProjection"]["kept"], json!(["m2"]));
		assert_eq!(session.payload["durableProjection"]["anchor"], json!("m1"));
	}

	#[test]
	fn malformed_durable_projection_shapes_fail_closed() {
		let cases = [
			json!({"durableProjection": "not-an-object"}),
			json!({"durableProjection": null}),
			json!({"durableProjection": {"kept": "nope", "synthetic": []}}),
			json!({"durableProjection": {"kept": [], "synthetic": "nope"}}),
			json!({"durableProjection": {"kept": [1], "synthetic": []}}),
			json!({"durableProjection": {"kept": [], "synthetic": [], "anchor": 5}}),
		];
		for (index, payload) in cases.iter().enumerate() {
			let mut store = SqliteStore::open_in_memory().expect("open");
			let id = format!("s_shape_{index}");
			store
				.create_session(NewSession {
					id: id.clone(),
					parent_session_id: None,
					payload: payload.clone(),
					initial_branch_id: "b1".into(),
					initial_branch_payload: json!({}),
				})
				.expect("session");
			store
				.append_message(NewMessage {
					id: "m1".into(),
					session_id: id.clone(),
					branch_id: "b1".into(),
					parent_message_id: None,
					payload: json!({"type":"message","id":"m1","message":{"role":"user","content":"hi"}}),
				})
				.expect("message");
			let error = store
				.reconcile_durable_projection(&id)
				.expect_err("malformed projection must fail closed");
			assert!(matches!(error, StoreError::Corrupt(_)), "case {index}: unexpected error {error}");
			let session = store.session(&id).expect("get").expect("session");
			assert_eq!(
				session.payload.get("durableProjection"),
				payload.get("durableProjection"),
				"case {index}: header must be unchanged"
			);
		}
	}

	#[test]
	fn snapshot_rejects_malformed_durable_projection() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		store
			.create_session(NewSession {
				id: "s_snap".into(),
				parent_session_id: None,
				payload: json!({"durableProjection": {"kept": "nope", "synthetic": []}}),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "m1".into(),
				session_id: "s_snap".into(),
				branch_id: "b1".into(),
				parent_message_id: None,
				payload: json!({"type":"message","id":"m1","message":{"role":"user","content":"hi"}}),
			})
			.expect("message");
		let error = store
			.load_session_snapshot("s_snap")
			.expect_err("snapshot must fail closed on malformed projection");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");
	}

	#[test]
	fn reconcile_durable_projection_completes_trailing_synthetic_call_from_kept() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		let calls = serde_json::to_value(pi_agent::Message::assistant_with_tools(
			"prefix",
			vec![
				pi_agent::ToolCall { id: "same1".into(), name: "bash".into(), arguments: json!({}) },
				pi_agent::ToolCall { id: "same2".into(), name: "bash".into(), arguments: json!({}) },
			],
		))
		.expect("synthetic calls");
		let answered =
			serde_json::to_value(pi_agent::Message::tool_with_status("same1", "bash", "ok", false))
				.expect("synthetic result");
		store
			.create_session(NewSession {
				id: "s_trailing".into(),
				parent_session_id: None,
				payload: json!({"durableProjection":{"kept":["r2","u1"],"synthetic":[calls, answered]}}),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "a1".into(),
				session_id: "s_trailing".into(),
				branch_id: "b1".into(),
				parent_message_id: None,
				payload: json!({"message":{"role":"assistant","content":[{"type":"toolCall","id":"same2"}]}}),
			})
			.expect("assistant");
		store
			.append_message(NewMessage {
				id: "u1".into(),
				session_id: "s_trailing".into(),
				branch_id: "b1".into(),
				parent_message_id: Some("a1".into()),
				payload: json!({"message":{"role":"user","content":"next"}}),
			})
			.expect("user");
		store
			.create_branch(NewBranch {
				id: "b2".into(),
				session_id: "s_trailing".into(),
				parent_branch_id: Some("b1".into()),
				parent_message_id: Some("a1".into()),
				payload: json!({}),
			})
			.expect("branch");
		store
			.append_message(NewMessage {
				id: "r2".into(),
				session_id: "s_trailing".into(),
				branch_id: "b2".into(),
				parent_message_id: Some("a1".into()),
				payload: json!({"message":{"role":"toolResult","toolCallId":"same2"}}),
			})
			.expect("result");
		store
			.reconcile_durable_projection("s_trailing")
			.expect("reconcile");
		let session = store.session("s_trailing").expect("get").expect("session");
		assert_eq!(
			session.payload["durableProjection"]["kept"],
			json!(["r2", "u1"]),
			"in-prefix result is matched and the trailing call is completed by the kept result"
		);
	}

	#[test]
	fn reconcile_durable_projection_uses_anchor_for_offbranch_only_kept_rows() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		let synthetic_call = serde_json::to_value(pi_agent::Message::assistant_with_tools(
			"prefix",
			vec![pi_agent::ToolCall { id: "same".into(), name: "bash".into(), arguments: json!({}) }],
		))
		.expect("synthetic");
		store.create_session(NewSession { id: "s_anchor_off".into(), parent_session_id: None, payload: json!({"durableProjection":{"kept":["r_off"],"synthetic":[synthetic_call],"anchor":"a1"}}), initial_branch_id: "b1".into(), initial_branch_payload: json!({}) }).expect("session");
		store.append_message(NewMessage { id: "a1".into(), session_id: "s_anchor_off".into(), branch_id: "b1".into(), parent_message_id: None, payload: json!({"type":"message","id":"a1","message":{"role":"assistant","content":[{"type":"toolCall","id":"same"}]}}) }).expect("assistant");
		store.append_message(NewMessage { id: "x1".into(), session_id: "s_anchor_off".into(), branch_id: "b1".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"x1","parentId":"a1","message":{"role":"user","content":"after anchor"}}) }).expect("suffix");
		store
			.create_branch(NewBranch {
				id: "b2".into(),
				session_id: "s_anchor_off".into(),
				parent_branch_id: Some("b1".into()),
				parent_message_id: Some("a1".into()),
				payload: json!({}),
			})
			.expect("branch");
		store.append_message(NewMessage { id: "r_off".into(), session_id: "s_anchor_off".into(), branch_id: "b2".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"r_off","parentId":"a1","message":{"role":"toolResult","toolCallId":"same"}}) }).expect("off-branch result");
		let backfilled = store
			.reconcile_durable_projection("s_anchor_off")
			.expect("reconcile");
		assert_eq!(backfilled, 1, "suffix after the validated anchor is backfilled");
		let session = store
			.session("s_anchor_off")
			.expect("get")
			.expect("session");
		assert_eq!(
			session.payload["durableProjection"]["kept"],
			json!(["r_off", "x1"]),
			"off-branch-only projection is preserved and extended"
		);
	}

	#[test]
	fn reconcile_durable_projection_fails_closed_without_anchor_for_offbranch_only_kept_rows() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		let synthetic_call = serde_json::to_value(pi_agent::Message::assistant_with_tools(
			"prefix",
			vec![pi_agent::ToolCall { id: "same".into(), name: "bash".into(), arguments: json!({}) }],
		))
		.expect("synthetic");
		let header = json!({"durableProjection":{"kept":["r_off"],"synthetic":[synthetic_call]}});
		store
			.create_session(NewSession {
				id: "s_no_anchor".into(),
				parent_session_id: None,
				payload: header.clone(),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		store.append_message(NewMessage { id: "a1".into(), session_id: "s_no_anchor".into(), branch_id: "b1".into(), parent_message_id: None, payload: json!({"type":"message","id":"a1","message":{"role":"assistant","content":[{"type":"toolCall","id":"same"}]}}) }).expect("assistant");
		store
			.create_branch(NewBranch {
				id: "b2".into(),
				session_id: "s_no_anchor".into(),
				parent_branch_id: Some("b1".into()),
				parent_message_id: Some("a1".into()),
				payload: json!({}),
			})
			.expect("branch");
		store.append_message(NewMessage { id: "r_off".into(), session_id: "s_no_anchor".into(), branch_id: "b2".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"r_off","parentId":"a1","message":{"role":"toolResult","toolCallId":"same"}}) }).expect("off-branch result");
		let error = store
			.reconcile_durable_projection("s_no_anchor")
			.expect_err("no active anchor must fail closed");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");
		let session = store.session("s_no_anchor").expect("get").expect("session");
		assert_eq!(
			session.payload.get("durableProjection"),
			header.get("durableProjection"),
			"failed validation must not delete the projection"
		);
	}

	#[test]
	fn reconcile_durable_projection_validates_synthetic_prefix_on_empty_branch() {
		let mut store = SqliteStore::open_in_memory().expect("open");
		let dangling = serde_json::to_value(pi_agent::Message::assistant_with_tools(
			"prefix",
			vec![pi_agent::ToolCall {
				id: "dangling".into(),
				name: "bash".into(),
				arguments: json!({}),
			}],
		))
		.expect("synthetic");
		let header = json!({"durableProjection":{"kept":[],"synthetic":[dangling]}});
		store
			.create_session(NewSession {
				id: "s_empty_branch".into(),
				parent_session_id: None,
				payload: header.clone(),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		let error = store
			.reconcile_durable_projection("s_empty_branch")
			.expect_err("empty branch must still validate the synthetic prefix");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");
		let session = store
			.session("s_empty_branch")
			.expect("get")
			.expect("session");
		assert_eq!(
			session.payload.get("durableProjection"),
			header.get("durableProjection"),
			"failed validation must not delete the projection"
		);
	}

	#[test]
	fn reconcile_durable_projection_rejects_result_after_user_ancestor() {
		let mut store = SqliteStore::open_in_memory().expect("open store");
		store
			.create_session(NewSession {
				id: "s_gap".into(),
				parent_session_id: None,
				payload: json!({"durableProjection":{"kept":["a","r"]}}),
				initial_branch_id: "b1".into(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		store
			.append_message(NewMessage {
				id: "a".into(),
				session_id: "s_gap".into(),
				branch_id: "b1".into(),
				parent_message_id: None,
				payload: json!({"message":{"role":"assistant","content":[{"type":"toolCall","id":"call"}]}}),
			})
			.expect("assistant");
		store
			.create_branch(NewBranch {
				id: "b2".into(),
				session_id: "s_gap".into(),
				parent_branch_id: Some("b1".into()),
				parent_message_id: Some("a".into()),
				payload: json!({}),
			})
			.expect("branch");
		store
			.append_message(NewMessage {
				id: "u".into(),
				session_id: "s_gap".into(),
				branch_id: "b2".into(),
				parent_message_id: Some("a".into()),
				payload: json!({"message":{"role":"user","content":"gap"}}),
			})
			.expect("user gap");
		store
			.append_message(NewMessage {
				id: "r".into(),
				session_id: "s_gap".into(),
				branch_id: "b2".into(),
				parent_message_id: Some("u".into()),
				payload: json!({"message":{"role":"toolResult","toolCallId":"call"}}),
			})
			.expect("result");
		let error = store
			.reconcile_durable_projection("s_gap")
			.expect_err("result after a user ancestor must fail closed");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");
		assert!(
			store
				.session("s_gap")
				.expect("get")
				.expect("session")
				.payload
				.get("durableProjection")
				.is_some()
		);
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
			json!(["m_asst", "m_tool_onbranch", "m_tool_offbranch"])
				.as_array()
				.unwrap()
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

		// Must fail closed because call-WRONG was not called by m_asst:
		let error = store
			.reconcile_durable_projection("s_unmatched")
			.expect_err("unmatched tool call id must fail closed");
		assert!(matches!(error, StoreError::Corrupt(_)), "unexpected error: {error}");

		let session = store.session("s_unmatched").expect("get").expect("s");
		assert!(
			session.payload.get("durableProjection").is_some(),
			"projection with unmatched tool call id must be retained"
		);
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
			.execute_batch("PRAGMA foreign_keys = OFF")
			.expect("disable foreign keys for corrupt fixture");
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
			.execute_batch("PRAGMA foreign_keys = ON")
			.expect("restore foreign keys");
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
						"kept": ["m_asst"],
						"synthetic": []
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
				.query_row("SELECT payload_json FROM sessions WHERE id = 's_race'", [], |row| {
					row.get(0)
				})
				.expect("query payload");
			let mut payload: Value = decode_json(&payload_json).unwrap();
			payload.as_object_mut().unwrap().remove("durableProjection");
			let encoded = serde_json::to_string(&payload).unwrap();
			tx2.execute("UPDATE sessions SET payload_json = ?1 WHERE id = 's_race'", params![encoded])
				.expect("tx2 update");
			tx2.commit().expect("tx2 commit");
		}

		// Now connection 1 runs reconcile_durable_projection.
		// Because reconcile_durable_projection reads the session INSIDE its own IMMEDIATE transaction,
		// it sees that durableProjection was already cleared by connection 2.
		// It returns Ok(0) and DOES NOT RESURRECT the cleared projection!
		let backfilled = store1
			.reconcile_durable_projection("s_race")
			.expect("reconcile");
		assert_eq!(backfilled, 0, "must not backfill into a concurrently cleared projection");

		let session = store1.session("s_race").expect("get").expect("s");
		assert!(
			session.payload.get("durableProjection").is_none(),
			"projection must remain None, not resurrected by stale snapshot"
		);

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn contended_immediate_write_reports_busy_before_touching_rows() {
		let (dir, path) = temp_db("busy-classification");
		let mut owner = SqliteStore::open(&path).expect("open owner");
		owner
			.create_session(NewSession {
				id: "s_busy".to_string(),
				parent_session_id: None,
				payload: json!({ "cwd": "/tmp/project" }),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({ "kind": "root" }),
			})
			.expect("create session");

		let mut contender = SqliteStore::open(&path).expect("open contender");
		contender
			.connection
			.busy_timeout(std::time::Duration::from_millis(10))
			.expect("short busy timeout");

		let held = owner
			.connection
			.transaction_with_behavior(TransactionBehavior::Immediate)
			.expect("owner holds the write lock");

		// `append_message` used to begin DEFERRED, discovering contention only on
		// the INSERT. BEGIN IMMEDIATE must report Busy before any read/write.
		let error = contender
			.append_message(NewMessage {
				id: "m_busy".to_string(),
				session_id: "s_busy".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({ "type": "message", "id": "m_busy" }),
			})
			.expect_err("contended append must fail, not hang or succeed");
		assert!(error.is_busy(), "unexpected error: {error}");
		assert!(matches!(error, StoreError::Busy(_)), "unexpected error: {error}");

		// BEGIN IMMEDIATE runs before any validation/read: a would-be NotFound is
		// masked by contention. A DEFERRED transaction would read first and
		// report NotFound instead of Busy.
		let masked = contender
			.create_branch(NewBranch {
				id: "b_masked".to_string(),
				session_id: "s_missing".to_string(),
				parent_branch_id: None,
				parent_message_id: None,
				payload: json!({}),
			})
			.expect_err("contended branch create must report Busy");
		assert!(masked.is_busy(), "unexpected error: {masked}");

		drop(held);
		contender
			.append_message(NewMessage {
				id: "m_after".to_string(),
				session_id: "s_busy".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({ "type": "message", "id": "m_after" }),
			})
			.expect("write after lock release must succeed");
		assert_eq!(
			contender.branch_messages("b1").expect("messages").len(),
			1,
			"the contended write must have written nothing"
		);

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn two_independent_writers_serialize_without_loss_or_duplicates() {
		let (dir, path) = temp_db("two-writer");
		let mut setup = SqliteStore::open(&path).expect("open setup");
		setup
			.create_session(NewSession {
				id: "s_two".to_string(),
				parent_session_id: None,
				payload: json!({ "cwd": "/tmp/project" }),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({ "kind": "root" }),
			})
			.expect("create session");
		drop(setup);

		const PER_WRITER: usize = 20;
		let store_a = SqliteStore::open(&path).expect("open writer a");
		let store_b = SqliteStore::open(&path).expect("open writer b");
		let gate = std::sync::Arc::new(std::sync::Barrier::new(2));
		let writers: Vec<_> = [("a", store_a), ("b", store_b)]
			.into_iter()
			.map(|(tag, mut store)| {
				let gate = std::sync::Arc::clone(&gate);
				std::thread::spawn(move || {
					gate.wait();
					for index in 0..PER_WRITER {
						store
							.enqueue_delivery(NewDelivery {
								delivery_id: format!("d-{tag}-{index}"),
								worker_id: format!("worker-{tag}"),
								session_id: "s_two".to_string(),
								origin_branch_id: "b1".to_string(),
								payload: json!({ "tag": tag, "index": index }),
							})
							.expect("contended enqueue must wait and commit, never lose the row");
					}
				})
			})
			.collect();
		for writer in writers {
			writer.join().expect("writer thread");
		}

		let store = SqliteStore::open(&path).expect("verify store");
		let pending = store.pending_deliveries("s_two").expect("pending deliveries");
		assert_eq!(pending.len(), PER_WRITER * 2, "no lost or duplicated deliveries");
		let unique: HashSet<&str> =
			pending.iter().map(|row| row.delivery_id.as_str()).collect();
		assert_eq!(unique.len(), pending.len(), "delivery ids must stay unique");
		assert!(unique.contains("d-a-0") && unique.contains("d-b-19"));

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
		assert!(
			tx2_err.is_err(),
			"concurrent immediate transaction must be blocked by SQLite write lock"
		);

		drop(tx1);
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn update_message_terminal_amends_assistant_stop_reason_and_error() {
		let (dir, path) = temp_db("update-terminal");
		let mut store = SqliteStore::open(&path).expect("open store");

		store
			.create_session(NewSession {
				id: "s_term".to_string(),
				parent_session_id: None,
				payload: json!({}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s_term".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"kind": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": "",
						"stopReason": "toolUse"
					}
				}),
			})
			.expect("append assistant message");

		let updated_id = store
			.update_message_terminal(
				"s_term",
				"b1",
				Some("m_asst"),
				"aborted",
				Some("Interrupted by user"),
			)
			.expect("update terminal");
		assert_eq!(updated_id, "m_asst");

		let messages = store.session_messages("s_term").expect("session messages");
		assert_eq!(messages.len(), 1);
		let payload = &messages[0].payload;
		assert_eq!(
			payload
				.get("message")
				.and_then(|m| m.get("stopReason"))
				.and_then(Value::as_str),
			Some("aborted")
		);
		assert_eq!(
			payload
				.get("message")
				.and_then(|m| m.get("errorMessage"))
				.and_then(Value::as_str),
			Some("Interrupted by user")
		);

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn update_message_terminal_finds_active_assistant_by_branch_ancestry() {
		let (dir, path) = temp_db("update-terminal-ancestry");
		let mut store = SqliteStore::open(&path).expect("open store");

		store
			.create_session(NewSession {
				id: "s_ancestry".to_string(),
				parent_session_id: None,
				payload: json!({}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		store
			.append_message(NewMessage {
				id: "m_asst".to_string(),
				session_id: "s_ancestry".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"kind": "message",
					"id": "m_asst",
					"message": {
						"role": "assistant",
						"content": "",
						"stopReason": "toolUse"
					}
				}),
			})
			.expect("append assistant message");

		// A tool result was completed and appended before the interrupt
		store
			.append_message(NewMessage {
				id: "m_tool".to_string(),
				session_id: "s_ancestry".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_asst".to_string()),
				payload: json!({
					"kind": "message",
					"id": "m_tool",
					"message": {
						"role": "toolResult",
						"content": "ok"
					}
				}),
			})
			.expect("append tool message");

		// Calling with message_id: None finds m_asst by walking back from leaf (m_tool)
		let updated_id = store
			.update_message_terminal("s_ancestry", "b1", None, "aborted", Some("Interrupted by user"))
			.expect("update terminal");
		assert_eq!(updated_id, "m_asst");

		let messages = store
			.session_messages("s_ancestry")
			.expect("session messages");
		assert_eq!(messages.len(), 2);
		let asst = messages.iter().find(|m| m.id == "m_asst").unwrap();
		assert_eq!(
			asst
				.payload
				.get("message")
				.and_then(|m| m.get("stopReason"))
				.and_then(Value::as_str),
			Some("aborted")
		);
		let tool = messages.iter().find(|m| m.id == "m_tool").unwrap();
		assert_eq!(
			tool
				.payload
				.get("message")
				.and_then(|m| m.get("role"))
				.and_then(Value::as_str),
			Some("toolResult")
		);

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn update_message_terminal_validates_branch_and_session() {
		let (dir, path) = temp_db("update-terminal-validation");
		let mut store = SqliteStore::open(&path).expect("open store");

		store
			.create_session(NewSession {
				id: "s_val".to_string(),
				parent_session_id: None,
				payload: json!({}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		let err = store
			.update_message_terminal("s_val", "b_nonexistent", None, "aborted", None)
			.expect_err("nonexistent branch must fail");
		assert!(matches!(err, StoreError::NotFound(_)));

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn stream_draft_redacts_secrets_on_save_and_legacy_read() {
		let (dir, path) = temp_db("stream-draft-redact");
		let mut store = SqliteStore::open(&path).expect("open store");
		store
			.create_session(NewSession {
				id: "s_red".to_string(),
				parent_session_id: None,
				payload: json!({}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		store
			.save_stream_draft(
				"s_red",
				"b1",
				"use serde(skip_serializing_if) with sk-proj-abcdefghij1234567890 and Bearer abcdefghij1234567890abcdef",
				"1-0",
			)
			.expect("save draft");
		let loaded = store
			.stream_draft("s_red", "b1")
			.expect("load")
			.expect("present");
		assert!(loaded.contains("skip_serializing_if"), "normal identifiers kept: {loaded}");
		assert!(!loaded.contains("sk-proj-abcdefghij1234567890"), "secret redacted: {loaded}");
		assert!(!loaded.contains("abcdefghij1234567890abcdef"), "bearer redacted: {loaded}");
		drop(store);

		let conn = Connection::open(&path).expect("raw sqlite");
		conn
			.execute(
				"UPDATE stream_drafts SET payload_json = ?1 WHERE session_id = 's_red'",
				params![r#"{"text":"legacy ghp_1234567890abcdefghij leaked"}"#],
			)
			.expect("seed legacy secret");
		drop(conn);

		let reopened = SqliteStore::open(&path).expect("reopen");
		let legacy = reopened
			.stream_draft("s_red", "b1")
			.expect("read legacy")
			.expect("present");
		assert!(!legacy.contains("ghp_1234567890abcdefghij"), "legacy scrubbed: {legacy}");
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn stream_draft_clears_on_journaled_tool_use_but_survives_abort() {
		let (dir, path) = temp_db("stream-draft-tooluse");
		let mut store = SqliteStore::open(&path).expect("open store");
		store
			.create_session(NewSession {
				id: "s_tool".to_string(),
				parent_session_id: None,
				payload: json!({}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		// An aborted assistant is excluded from replay: its text survives only
		// in the draft, so the terminal write must keep it.
		store
			.save_stream_draft("s_tool", "b1", "aborted partial text", "1-0")
			.expect("save draft");
		store
			.append_message(NewMessage {
				id: "m_abort".to_string(),
				session_id: "s_tool".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"kind": "message",
					"id": "m_abort",
					"message": {
						"role": "assistant",
						"content": "",
						"stopReason": "aborted",
						"streamAttempt": "1-0"
					}
				}),
			})
			.expect("append aborted");
		assert!(
			store.stream_draft("s_tool", "b1").expect("read").is_some(),
			"aborted terminal keeps the only copy of the text"
		);

		// A tool-use assistant is journaled and replayed, so keeping the draft
		// would resend its text as a spurious interrupted fragment.
		store
			.save_stream_draft("s_tool", "b1", "tool-use text", "1-1")
			.expect("save draft");
		store
			.append_message(NewMessage {
				id: "m_tool".to_string(),
				session_id: "s_tool".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: Some("m_abort".to_string()),
				payload: json!({
					"kind": "message",
					"id": "m_tool",
					"message": {
						"role": "assistant",
						"content": "tool-use text",
						"stopReason": "toolUse",
						"streamAttempt": "1-1",
						"tool_calls": [{"id": "call_1", "name": "read", "arguments": "{}"}]
					}
				}),
			})
			.expect("append tool use");
		assert!(
			store.stream_draft("s_tool", "b1").expect("read").is_none(),
			"journaled tool-use assistant clears the delivered draft"
		);
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn appended_message_payload_is_redacted_before_persist() {
		// A tool result or provider error echoing a resolved credential must not
		// reach the messages table: that row is what the UI renders and what the
		// model replays next turn.
		let secret = "msg-registry-secret-4b81cd";
		crate::auth::known_secrets::register(secret);
		let (dir, path) = temp_db("message-redaction");
		let mut store = SqliteStore::open(&path).expect("open store");
		store
			.create_session(NewSession {
				id: "s_msg".to_string(),
				parent_session_id: None,
				payload: json!({}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");
		store
			.append_message(NewMessage {
				id: "m_msg".to_string(),
				session_id: "s_msg".to_string(),
				branch_id: "b1".to_string(),
				parent_message_id: None,
				payload: json!({
					"kind": "message",
					"id": "m_msg",
					"message": {
						"role": "user",
						"content": format!("tool said key={secret} and failed"),
					},
				}),
			})
			.expect("append");
		let stored = store.message("m_msg").expect("read").expect("present");
		let text = stored.payload["message"]["content"]
			.as_str()
			.expect("content");
		assert!(!text.contains(secret), "stored payload must be masked: {text}");
		assert!(text.contains("[REDACTED]"));
		assert!(text.contains("and failed"));
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn execution_delivery_artifact_and_header_rows_are_redacted_before_persist() {
		let secret = "row-registry-secret-71e0a4";
		crate::auth::known_secrets::register(secret);
		let (dir, path) = temp_db("row-redaction");
		let mut store = SqliteStore::open(&path).expect("open store");
		store
			.create_session(NewSession {
				id: "s_rows".to_string(),
				parent_session_id: None,
				payload: json!({ "title": format!("first turn {secret}") }),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");
		let header = store.session("s_rows").expect("read").expect("present");
		assert!(
			!header.payload.to_string().contains(secret),
			"creation payload masked: {}",
			header.payload
		);

		let execution_id = store
			.record_intent(NewIntent {
				session_id: "s_rows".to_string(),
				branch_id: "b1".to_string(),
				provider_call_id: "call_rows".to_string(),
				tool_name: "shell".to_string(),
				payload: json!({ "command": format!("curl -H 'Authorization: Bearer {secret}'") }),
			})
			.expect("record intent");
		let intent = store
			.execution(&execution_id)
			.expect("read")
			.expect("present");
		assert!(!intent.intent.to_string().contains(secret), "intent masked: {}", intent.intent);
		store
			.record_outcome(&execution_id, json!({ "output": format!("token={secret}") }))
			.expect("record outcome");
		let outcome = store
			.outcome(&execution_id)
			.expect("read")
			.expect("present");
		assert!(!outcome.to_string().contains(secret), "outcome masked: {outcome}");
		// Repeating the raw payload must stay idempotent although the stored row
		// is masked.
		store
			.record_outcome(&execution_id, json!({ "output": format!("token={secret}") }))
			.expect("repeat outcome");

		store
			.enqueue_delivery(NewDelivery {
				delivery_id: "d_rows".to_string(),
				worker_id: "w_rows".to_string(),
				session_id: "s_rows".to_string(),
				origin_branch_id: "b1".to_string(),
				payload: json!({ "text": format!("mail {secret}") }),
			})
			.expect("enqueue delivery");
		let deliveries = store.pending_deliveries("s_rows").expect("read deliveries");
		assert!(
			!deliveries[0].payload.to_string().contains(secret),
			"delivery masked: {}",
			deliveries[0].payload
		);
		// The same raw payload must stay idempotent against the masked row.
		store
			.enqueue_delivery(NewDelivery {
				delivery_id: "d_rows".to_string(),
				worker_id: "w_rows".to_string(),
				session_id: "s_rows".to_string(),
				origin_branch_id: "b1".to_string(),
				payload: json!({ "text": format!("mail {secret}") }),
			})
			.expect("repeat delivery");
		assert_eq!(store.pending_deliveries("s_rows").expect("read").len(), 1);

		let completion = || WorkerCompletion {
			worker_id: "w_rows2".to_string(),
			delivery_id: "d_rows2".to_string(),
			session_id: "s_rows".to_string(),
			origin_branch_id: "b1".to_string(),
			completion: json!({ "summary": format!("done {secret}") }),
			delivery: json!({ "text": format!("worker said {secret}") }),
		};
		store
			.complete_worker(completion())
			.expect("complete worker");
		let completion_json: String = store
			.connection
			.query_row(
				"SELECT completion_json FROM worker_completions WHERE worker_id = 'w_rows2'",
				[],
				|row| row.get(0),
			)
			.expect("read completion");
		assert!(!completion_json.contains(secret), "completion masked: {completion_json}");
		store
			.complete_worker(completion())
			.expect("repeat completion");

		let artifact = || NewArtifact {
			artifact_id: "a_rows".to_string(),
			session_id: "s_rows".to_string(),
			branch_id: Some("b1".to_string()),
			execution_id: Some(execution_id.clone()),
			location: "/tmp/row-artifact".to_string(),
			metadata: json!({ "preview": format!("artifact {secret}") }),
		};
		store.record_artifact(artifact()).expect("record artifact");
		let artifacts = store.artifacts("s_rows").expect("read artifacts");
		assert!(
			!artifacts[0].metadata.to_string().contains(secret),
			"artifact metadata masked: {}",
			artifacts[0].metadata
		);
		store.record_artifact(artifact()).expect("repeat artifact");

		store
			.update_session_payload("s_rows", json!({ "title": format!("renamed {secret}") }))
			.expect("update header");
		let header = store.session("s_rows").expect("read").expect("present");
		assert!(
			!header.payload.to_string().contains(secret),
			"updated header masked: {}",
			header.payload
		);
		assert!(header.payload.to_string().contains("[REDACTED]"));
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn task_admission_is_durable_redacted_and_single_flight() {
		let secret = "admission-secret-3c9e";
		crate::auth::known_secrets::register(secret);
		let (dir, path) = temp_db("task-admission");
		let mut store = SqliteStore::open(&path).expect("open store");
		store
			.create_session(NewSession {
				id: "s_adm".to_string(),
				parent_session_id: None,
				payload: json!({}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");
		let admission = NewTaskAdmission {
			task_id: "t_one".to_string(),
			parent_session_id: "s_adm".to_string(),
			parent_branch_id: "b1".to_string(),
			worker_id: "w_adm".to_string(),
			producer_generation: 7,
			spec: json!({ "prompt": format!("use {secret}") }),
		};
		store.admit_task(admission.clone()).expect("admit");
		let stored = store
			.task_admission("t_one")
			.expect("read")
			.expect("present");
		assert_eq!(stored.state, "admitted");
		assert_eq!(stored.producer_generation, 7);
		assert!(!stored.spec.to_string().contains(secret), "admission spec masked: {}", stored.spec);
		assert_eq!(
			store
				.open_task_admissions("s_adm")
				.expect("open list")
				.len(),
			1
		);

		// The same task id and the same worker id at the same generation are
		// single-flight; a later producer generation may reuse the worker name.
		assert!(store.admit_task(admission).is_err());
		let duplicate_worker = NewTaskAdmission {
			task_id: "t_other".to_string(),
			parent_session_id: "s_adm".to_string(),
			parent_branch_id: "b1".to_string(),
			worker_id: "w_adm".to_string(),
			producer_generation: 7,
			spec: json!({}),
		};
		assert!(store.admit_task(duplicate_worker).is_err());

		store
			.mark_task_admission_started("t_one")
			.expect("mark started");
		assert!(store.mark_task_admission_started("t_one").is_err());
		assert_eq!(
			store
				.task_admission("t_one")
				.expect("read")
				.expect("present")
				.state,
			"started"
		);
		store
			.finish_task_admission("t_one", "completed")
			.expect("finish");
		assert!(
			store
				.open_task_admissions("s_adm")
				.expect("open list")
				.is_empty()
		);
		assert!(store.finish_task_admission("t_one", "failed").is_err());
		assert!(store.finish_task_admission("t_one", "bogus").is_err());

		// Recovery epochs never reuse a stored generation.
		let next = store.next_producer_generation("s_adm").expect("generation");
		assert_eq!(next, 8);
		store
			.admit_task(NewTaskAdmission {
				task_id: "t_lost".to_string(),
				parent_session_id: "s_adm".to_string(),
				parent_branch_id: "b1".to_string(),
				worker_id: "w_lost".to_string(),
				producer_generation: next,
				spec: json!({ "prompt": "started before the crash" }),
			})
			.expect("admit lost task");
		store
			.mark_task_admission_started("t_lost")
			.expect("start lost task");
		assert_eq!(
			store
				.abandon_open_task_admissions("s_adm")
				.expect("recovery"),
			1
		);
		assert_eq!(
			store
				.abandon_open_task_admissions("s_adm")
				.expect("idempotent recovery"),
			0
		);
		assert_eq!(
			store
				.task_admission("t_lost")
				.expect("read")
				.expect("present")
				.state,
			"abandoned"
		);
		assert_eq!(store.next_producer_generation("s_adm").expect("generation"), 9);

		// Durable across reopen: the admission is not in-memory state.
		drop(store);
		let store = SqliteStore::open(&path).expect("reopen");
		let stored = store
			.task_admission("t_one")
			.expect("read")
			.expect("present");
		assert_eq!(stored.state, "completed");
		assert_eq!(stored.producer_generation, 7);
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn schema_v4_upgrades_and_accepts_task_admissions() {
		let (dir, path) = temp_db("v4-to-v5");
		let mut store = SqliteStore::open(&path).expect("store");
		store
			.create_session(NewSession {
				id: "s_up".to_string(),
				parent_session_id: None,
				payload: json!({}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		drop(store);
		// Simulate a database written by the previous release.
		let connection = rusqlite::Connection::open(&path).expect("raw");
		connection
			.execute_batch("DROP TABLE task_admissions; UPDATE schema_version SET version = 4;")
			.expect("downgrade to v4");
		drop(connection);

		let mut store = SqliteStore::open(&path).expect("migrate to the current schema");
		let version: i64 = store
			.connection
			.query_row("SELECT version FROM schema_version WHERE id = 1", [], |row| row.get(0))
			.expect("version");
		assert_eq!(version, SCHEMA_VERSION);
		store
			.admit_task(NewTaskAdmission {
				task_id: "t_up".to_string(),
				parent_session_id: "s_up".to_string(),
				parent_branch_id: "b1".to_string(),
				worker_id: "w_up".to_string(),
				producer_generation: 1,
				spec: json!({}),
			})
			.expect("admission after migration");
		assert_eq!(
			store
				.task_admission("t_up")
				.expect("read")
				.expect("present")
				.state,
			"admitted"
		);
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn schema_v5_index_upgrade_allows_worker_reuse_in_a_new_generation() {
		let (dir, path) = temp_db("v5-admission-index");
		let mut store = SqliteStore::open(&path).expect("open store");
		store
			.create_session(NewSession {
				id: "s_idx".to_string(),
				parent_session_id: None,
				payload: json!({}),
				initial_branch_id: "b1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("session");
		drop(store);
		// Simulate the v5 index shipped before generation-aware identity.
		let connection = rusqlite::Connection::open(&path).expect("raw");
		connection
			.execute_batch(
				"DROP INDEX task_admissions_worker_idx; \
				 CREATE UNIQUE INDEX task_admissions_worker_idx ON task_admissions(worker_id); \
				 UPDATE schema_version SET version = 5;",
			)
			.expect("downgrade to v5");
		drop(connection);

		let mut store = SqliteStore::open(&path).expect("migrate to v6");
		for (task_id, generation) in [("t_gen1", 1), ("t_gen2", 2)] {
			store
				.admit_task(NewTaskAdmission {
					task_id: task_id.to_string(),
					parent_session_id: "s_idx".to_string(),
					parent_branch_id: "b1".to_string(),
					worker_id: "w_reuse".to_string(),
					producer_generation: generation,
					spec: json!({}),
				})
				.expect("same worker in a new generation");
		}
		let index_sql: String = store
			.connection
			.query_row(
				"SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'task_admissions_worker_idx'",
				[],
				|row| row.get(0),
			)
			.expect("index present");
		assert!(index_sql.contains("producer_generation"));
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn stream_draft_delivery_window_keeps_newest_tail_char_exact() {
		// Exactly at the window: delivered whole and unmarked.
		let at_window = json!({ "priors": [{ "text": "x".repeat(STREAM_DRAFT_DELIVERY_CHARS) }] });
		let delivered = combined_stream_draft_text(&at_window);
		assert_eq!(delivered.chars().count(), STREAM_DRAFT_DELIVERY_CHARS);
		assert!(!delivered.contains("truncated"), "no marker at the boundary");

		// One char over: the single oldest char is dropped, the newest window
		// is kept whole, and the marker is the only addition.
		let over_window = json!({
			"priors": [{ "text": format!("A{}", "y".repeat(STREAM_DRAFT_DELIVERY_CHARS)) }]
		});
		let delivered = combined_stream_draft_text(&over_window);
		let tail = delivered
			.strip_prefix(STREAM_DRAFT_TRUNCATION_MARKER)
			.expect("truncation is marked");
		assert_eq!(tail.chars().count(), STREAM_DRAFT_DELIVERY_CHARS);
		assert!(
			tail.chars().all(|c| c == 'y'),
			"undelivered head must be cut, delivered tail must not be re-sent"
		);
	}
	#[test]
	fn fork_and_compaction_redaction_preserves_relational_identity_and_masks_user_id_and_dynamic_keys() {
		let secret = "sk-fork-compact-secret-9876543210";
		crate::auth::known_secrets::register(secret);
		let (dir, path) = temp_db("fork-compact-redact");
		let mut store = SqliteStore::open(&path).expect("open store");

		// 1. Create source session with secret in title and user field named "id"
		store
			.create_session(NewSession {
				id: "s_src".to_string(),
				parent_session_id: None,
				payload: json!({
					"type": "session",
					"version": 3,
					"id": "s_src",
					"timestamp": "2026-01-01T00:00:00Z",
					"cwd": "/tmp",
					"title": format!("src title with {secret}")
				}),
				initial_branch_id: "b_src".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create session");

		// 2. Append assistant tool-call message and tool-result message with timestamps
		let src_msg1_id = "m_src_asst_1";
		let call_id = "call_test_1";
		let msg1_payload = json!({
			"type": "message",
			"id": src_msg1_id,
			"timestamp": "2026-01-01T00:00:01Z",
			"message": {
				"role": "assistant",
				"timestamp": 1000,
				"content": [
					{
						"type": "toolCall",
						"id": call_id,
						"name": "exec_cmd",
						"arguments": {
							"id": format!("user-field-id-{secret}"),
							format!("sk-dynamic-key-{secret}"): "dynamic_val"
						}
					}
				]
			}
		});
		store
			.append_message(NewMessage {
				id: src_msg1_id.to_string(),
				session_id: "s_src".to_string(),
				branch_id: "b_src".to_string(),
				parent_message_id: None,
				payload: msg1_payload,
			})
			.expect("append msg1");

		let src_msg2_id = "m_src_res_1";
		let msg2_payload = json!({
			"type": "message",
			"id": src_msg2_id,
			"parentId": src_msg1_id,
			"timestamp": "2026-01-01T00:00:02Z",
			"message": {
				"role": "toolResult",
				"toolCallId": call_id,
				"toolName": "exec_cmd",
				"isError": false,
				"timestamp": 2000,
				"content": [
					{
						"type": "text",
						"text": format!("result text containing {secret}")
					}
				],
				"details": {
					"id": format!("detail-id-{secret}")
				}
			}
		});
		store
			.append_message(NewMessage {
				id: src_msg2_id.to_string(),
				session_id: "s_src".to_string(),
				branch_id: "b_src".to_string(),
				parent_message_id: Some(src_msg1_id.to_string()),
				payload: msg2_payload,
			})
			.expect("append msg2");

		// 3. Compact session
		let src_compact_id = "m_src_comp_1";
		let compact_payload = json!({
			"type": "message",
			"id": src_compact_id,
			"parentId": src_msg2_id,
			"timestamp": "2026-01-01T00:00:03Z",
			"message": {
				"role": "system",
				"timestamp": 3000,
				"content": format!("compaction notice with {secret}")
			}
		});
		let projection = StoredProjection {
			kept: vec![src_msg1_id.to_string(), src_msg2_id.to_string()],
			synthetic: vec![
				json!({
					"role": "system",
					"content": format!("synthetic summary {secret}")
				})
			],
			anchor: Some(src_compact_id.to_string()),
		};
		store
			.compact_session(
				NewMessage {
					id: src_compact_id.to_string(),
					session_id: "s_src".to_string(),
					branch_id: "b_src".to_string(),
					parent_message_id: Some(src_msg2_id.to_string()),
					payload: compact_payload,
				},
				projection,
			)
			.expect("compact session");

		// Verify compacted session payload
		let src_session = store.session("s_src").expect("read").expect("exists");
		let proj_val = &src_session.payload["durableProjection"];
		assert_eq!(proj_val["kept"], json!([src_msg1_id, src_msg2_id]));
		assert_eq!(proj_val["anchor"], json!(src_compact_id));
		assert!(
			proj_val["synthetic"][0]["content"] == "synthetic summary [REDACTED]",
			"synthetic notice content must be masked"
		);

		// 4. Create fork atomic from the compacted source with distinct IDs
		let fork_msg1_id = "m_fork_asst_1";
		let fork_msg2_id = "m_fork_res_1";
		let fork_compact_id = "m_fork_comp_1";

		let fork_write = ForkWrite {
			session: NewSession {
				id: "s_fork".to_string(),
				parent_session_id: Some("s_src".to_string()),
				payload: json!({
					"type": "session",
					"version": 3,
					"id": "s_fork",
					"parentSession": "s_src",
					"timestamp": "2026-01-01T00:00:04Z",
					"cwd": "/tmp",
					"title": format!("fork title with {secret}")
				}),
				initial_branch_id: "b_fork".to_string(),
				initial_branch_payload: json!({}),
			},
			branches: vec![
				NewBranch {
					id: "b_fork".to_string(),
					session_id: "s_fork".to_string(),
					parent_branch_id: None,
					parent_message_id: None,
					payload: json!({}),
				}
			],
			active_branch_id: "b_fork".to_string(),
			active_leaf_message_id: Some(fork_compact_id.to_string()),
			messages: vec![
				NewMessage {
					id: fork_msg1_id.to_string(),
					session_id: "s_fork".to_string(),
					branch_id: "b_fork".to_string(),
					parent_message_id: None,
					payload: json!({
						"type": "message",
						"id": fork_msg1_id,
						"timestamp": "2026-01-01T00:00:01Z",
						"message": {
							"role": "assistant",
							"timestamp": 1000,
							"content": [
								{
									"type": "toolCall",
									"id": call_id,
									"name": "exec_cmd",
									"arguments": {
										"id": format!("user-field-id-{secret}"),
										format!("sk-dynamic-key-{secret}"): "dynamic_val"
									}
								}
							]
						}
					}),
				},
				NewMessage {
					id: fork_msg2_id.to_string(),
					session_id: "s_fork".to_string(),
					branch_id: "b_fork".to_string(),
					parent_message_id: Some(fork_msg1_id.to_string()),
					payload: json!({
						"type": "message",
						"id": fork_msg2_id,
						"parentId": fork_msg1_id,
						"timestamp": "2026-01-01T00:00:02Z",
						"message": {
							"role": "toolResult",
							"toolCallId": call_id,
							"toolName": "exec_cmd",
							"isError": false,
							"timestamp": 2000,
							"content": [
								{
									"type": "text",
									"text": format!("result text containing {secret}")
								}
							],
							"details": {
								"id": format!("detail-id-{secret}")
							}
						}
					}),
				},
				NewMessage {
					id: fork_compact_id.to_string(),
					session_id: "s_fork".to_string(),
					branch_id: "b_fork".to_string(),
					parent_message_id: Some(fork_msg2_id.to_string()),
					payload: json!({
						"type": "message",
						"id": fork_compact_id,
						"parentId": fork_msg2_id,
						"timestamp": "2026-01-01T00:00:03Z",
						"message": {
							"role": "system",
							"timestamp": 3000,
							"content": format!("compaction notice with {secret}")
						}
					}),
				}
			],
			projection: Some(StoredProjection {
				kept: vec![fork_msg1_id.to_string(), fork_msg2_id.to_string()],
				synthetic: vec![
					json!({
						"role": "system",
						"content": format!("fork synthetic summary {secret}")
					})
				],
				anchor: Some(fork_compact_id.to_string()),
			}),
		};

		store.create_fork_atomic(fork_write).expect("create fork atomic");

		// Verify fork session in store
		let fork_session = store.session("s_fork").expect("read fork").expect("fork exists");
		assert_eq!(fork_session.id, "s_fork");
		assert_eq!(fork_session.parent_session_id.as_deref(), Some("s_src"));
		assert_eq!(fork_session.payload["id"], "s_fork");
		assert_eq!(fork_session.payload["parentSession"], "s_src");
		assert!(
			fork_session.payload["title"] == "fork title with [REDACTED]",
			"fork title must be masked"
		);

		let fork_proj = &fork_session.payload["durableProjection"];
		assert_eq!(fork_proj["kept"], json!([fork_msg1_id, fork_msg2_id]));
		assert_eq!(fork_proj["anchor"], json!(fork_compact_id));
		assert!(
			fork_proj["synthetic"][0]["content"] == "fork synthetic summary [REDACTED]",
			"fork synthetic summary must be masked"
		);

		// Verify messages in fork session
		let fork_msgs = store.session_messages("s_fork").expect("read fork messages");
		assert_eq!(fork_msgs.len(), 3);
		let asst_msg = &fork_msgs[0];
		assert_eq!(asst_msg.id, fork_msg1_id);
		assert_eq!(asst_msg.payload["id"], fork_msg1_id);
		assert_eq!(asst_msg.payload["message"]["content"][0]["id"], call_id);
		assert!(
			asst_msg.payload["message"]["content"][0]["arguments"]["id"]
				== "user-field-id-[REDACTED]",
			"tool argument user id must be masked"
		);
		assert!(
			asst_msg.payload["message"]["content"][0]["arguments"]
				.as_object()
				.unwrap()
				.contains_key("sk-dynamic-key-[REDACTED]")
		);

		let res_msg = &fork_msgs[1];
		assert_eq!(res_msg.id, fork_msg2_id);
		assert_eq!(res_msg.parent_message_id.as_deref(), Some(fork_msg1_id));
		assert_eq!(res_msg.payload["id"], fork_msg2_id);
		assert_eq!(res_msg.payload["parentId"], fork_msg1_id);
		assert_eq!(res_msg.payload["message"]["toolCallId"], call_id);
		assert!(
			res_msg.payload["message"]["details"]["id"] == "detail-id-[REDACTED]",
			"detail id must be masked"
		);
		assert!(
			res_msg.payload["message"]["content"][0]["text"]
				== "result text containing [REDACTED]",
			"tool result text must be masked"
		);

		// Check full database serialization: secret must NEVER appear in cleartext
		let dump: String = store
			.connection
			.query_row(
				"SELECT group_concat(payload_json, ' ') FROM messages WHERE session_id = 's_fork'",
				[],
				|row| row.get(0),
			)
			.expect("query dump");
		assert!(!dump.contains(secret), "secret must not appear in cleartext in messages");

		let sess_dump: String = store
			.connection
			.query_row(
				"SELECT group_concat(payload_json, ' ') FROM sessions WHERE id IN ('s_src', 's_fork')",
				[],
				|row| row.get(0),
			)
			.expect("query sess dump");
		assert!(!sess_dump.contains(secret), "secret must not appear in cleartext in sessions");

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn bundle_branch_order_is_parent_first_and_rejects_cycles() {
		fn branch(id: &str, parent: Option<&str>) -> SessionBundleBranch {
			SessionBundleBranch {
				id: id.to_string(),
				session_id: "s_order".to_string(),
				parent_branch_id: parent.map(str::to_string),
				parent_message_id: None,
				leaf_message_id: None,
				payload: json!({}),
				created_at_ms: 0,
			}
		}
		let child = branch("child", Some("root"));
		let root = branch("root", None);
		let ordered = order_session_bundle_branches(&[child.clone(), root.clone()])
			.expect("parent-first order");
		assert_eq!(
			ordered.iter().map(|branch| branch.id.as_str()).collect::<Vec<_>>(),
			vec!["root", "child"]
		);
		let error = order_session_bundle_branches(&[
			branch("a", Some("b")),
			branch("b", Some("a")),
		])
		.expect_err("cycle must fail");
		assert!(matches!(error, StoreError::Validation(_)));
	}
}
