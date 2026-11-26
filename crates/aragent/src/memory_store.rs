//! Durable SQLite memory store behind the `local` memory backend.
//!
//! Authority: D32 (SQLite is the durable authority), D53 (owner preferences vs
//! project knowledge, explicit user assertion vs agent inference, user
//! view/edit/delete, memory is context — never permissions, deleting a memory
//! record never deletes conversation history).
//!
//! What this owns:
//! - one `memory.db` per agent directory, separate from `sessions.db`, so a
//!   memory delete can never touch a session message row;
//! - persistent row ids, `global`/`project`/`session` scope (`project` rows are
//!   filtered by `cwd`, `session` rows strictly by `session_id`), body,
//!   `asserted`/`inferred` origin, optional actor/source, `created_at`/
//!   `updated_at` unix seconds, and access/decay/consolidation metadata;
//! - `add`, `list`, `get`, `update`, `delete`, bounded FTS5/BM25 `recall`, and
//!   optional vector + MMR hybrid recall when stored embeddings exist;
//! - deterministic consolidation: TTL expiry plus near-duplicate clusters
//!   (`cosine_similarity_pairs`) marked through `consolidated_into`;
//! - a one-time migration of legacy `learned.md` lessons per project scope,
//!   guarded by a marker row committed atomically with the imported rows. The
//!   file is read, never deleted, and afterwards is only a bounded export
//!   artifact rebuilt from rows, so old readers (`memory://root`, prompt
//!   guidance) keep working and a deleted row can never be re-imported.
//!
//! Retrieval uses an FTS5 external-content index (`memories_fts`) as the
//! always-available lexical path, so recall works offline with no embedding
//! provider. Embeddings are optional accelerators: when the caller supplies a
//! query vector for a stored model, hybrid recall blends BM25 with cosine
//! similarity and MMR diversity; otherwise the same query stays lexical.
//!
//! Schema v2 is a transactional, idempotent v1 upgrade: rows keep their ids,
//! timestamps, dedupe keys and text, while new metadata columns get defaults
//! and the FTS index is rebuilt from the preserved rows. All calls block on one
//! mutex-guarded connection; move to a writer thread if a session starts
//! issuing memory operations in the middle of model streaming.
//! Writers of one `memory.db` serialize on a per-path lock that also covers
//! store opens (WAL switch + schema init), so concurrent `learn` calls queue on
//! a mutex instead of racing SQLite's `busy_timeout`, whose expiry surfaces as
//! `database is locked`; different agent directories never wait on each other.
//! The lock is process-local: two `arag` processes still race, and a lost race is
//! reported, never retried (a retried commit would have unknown effect).
//!
//! Security: content is neutralized and secret-redacted on write and again on
//! read, so hand-edited or corrupted rows cannot inject prompt delimiters or
//! leak credentials. Memory text is data for the model, not instructions and
//! not an authorization signal. Deleting a row never scrubs prior copies from
//! conversation history or provider logs — it removes one memory record only.

use std::{
	collections::{BTreeMap, BTreeSet},
	fmt, fs,
	path::{Path, PathBuf},
	sync::{Mutex, MutexGuard},
	time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{
	Connection, OptionalExtension as _, TransactionBehavior, params, params_from_iter, types::Value,
};
use sha2::{Digest as _, Sha256};

use crate::local_memory::{
	LEARNED_LESSONS_FILE, MAX_LEARNED_CONTENT_CHARS, MAX_LEARNED_CONTEXT_CHARS, MAX_LEARNED_LESSONS,
	normalize_learned_text,
};
use crate::memory_vectors::{
	cosine_similarity, cosine_similarity_pairs, mmr_rerank_indices, vector_index_top_k,
};

/// Database filename under the agent directory.
pub const MEMORY_DB_FILE: &str = "memory.db";
/// Schema version recorded in `PRAGMA user_version`.
pub const SCHEMA_VERSION: i64 = 2;
/// Default result bound for [`MemoryStore::recall`].
pub const DEFAULT_RECALL_LIMIT: usize = 20;
/// Hard upper bound for a recall result.
pub const MAX_RECALL_LIMIT: usize = 50;
/// Hard upper bound for [`MemoryStore::list`].
pub const MAX_LIST_LIMIT: usize = 200;
/// Weight of the lexical BM25 component in hybrid recall; the remainder is
/// cosine similarity. Fixed so ranking is reproducible across runs.
pub const HYBRID_LEXICAL_WEIGHT: f64 = 0.5;
/// MMR diversity strength used after hybrid ranking.
pub const HYBRID_MMR_LAMBDA: f64 = 0.7;
/// Cosine threshold for near-duplicate consolidation.
pub const CONSOLIDATION_SIMILARITY_THRESHOLD: f64 = 0.92;
/// Candidate rows a recall may score before truncating to its result bound.
const MAX_RECALL_CANDIDATES: usize = 200;
/// Longest record id: `lower(hex(randomblob(16)))`.
const RECORD_ID_LEN: usize = 32;

/// Columns selected by every record read, in [`row_to_record`] order.
pub const RECORD_COLUMNS: &str = "id, scope, cwd, session_id, body, context, origin, actor, source, created_at, updated_at, access_count, last_accessed_at, confidence, ttl_secs, consolidated_into";
/// [`RECORD_COLUMNS`] qualified for joins with `memory_embeddings`.
const RECORD_COLUMNS_QUALIFIED: &str = "memories.id, memories.scope, memories.cwd, memories.session_id, memories.body, memories.context, memories.origin, memories.actor, memories.source, memories.created_at, memories.updated_at, memories.access_count, memories.last_accessed_at, memories.confidence, memories.ttl_secs, memories.consolidated_into";

const MEMORIES_V2_TABLE_DDL: &str = "
	CREATE TABLE memories (
		id TEXT PRIMARY KEY,
		scope TEXT NOT NULL CHECK (scope IN ('global', 'project', 'session')),
		cwd TEXT,
		session_id TEXT,
		body TEXT NOT NULL,
		context TEXT,
		origin TEXT NOT NULL CHECK (origin IN ('asserted', 'inferred')),
		actor TEXT,
		source TEXT,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		dedupe_key TEXT,
		access_count INTEGER NOT NULL DEFAULT 0 CHECK (access_count >= 0),
		last_accessed_at INTEGER,
		confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
		ttl_secs INTEGER CHECK (ttl_secs IS NULL OR ttl_secs >= 0),
		consolidated_into TEXT REFERENCES memories (id) ON DELETE SET NULL,
		CHECK (
			(scope = 'global' AND cwd IS NULL AND session_id IS NULL)
			OR (scope = 'project' AND cwd IS NOT NULL AND length(cwd) > 0 AND session_id IS NULL)
			OR (scope = 'session' AND cwd IS NULL AND session_id IS NOT NULL AND length(session_id) > 0)
		),
		CHECK (consolidated_into IS NULL OR consolidated_into != id)
	);
";

const MEMORIES_V2_AUX_DDL: &str = "
	CREATE UNIQUE INDEX IF NOT EXISTS memories_dedupe
		ON memories (dedupe_key) WHERE dedupe_key IS NOT NULL;
	CREATE INDEX IF NOT EXISTS memories_scope_updated
		ON memories (scope, cwd, session_id, updated_at DESC);
	CREATE INDEX IF NOT EXISTS memories_consolidated
		ON memories (consolidated_into) WHERE consolidated_into IS NOT NULL;

	CREATE TABLE IF NOT EXISTS memory_meta (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);
	CREATE TABLE IF NOT EXISTS memory_export_dirty (
		cwd TEXT PRIMARY KEY,
		dirty INTEGER NOT NULL DEFAULT 1,
		dirty_gen INTEGER NOT NULL DEFAULT 1
	);
";

const MEMORIES_FTS_DDL: &str = "
	CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
		body,
		context,
		content = 'memories',
		content_rowid = 'rowid',
		tokenize = 'unicode61'
	);

	CREATE TRIGGER IF NOT EXISTS memories_fts_ai AFTER INSERT ON memories BEGIN
		INSERT INTO memories_fts (rowid, body, context)
		VALUES (new.rowid, new.body, new.context);
	END;
	CREATE TRIGGER IF NOT EXISTS memories_fts_ad AFTER DELETE ON memories BEGIN
		INSERT INTO memories_fts (memories_fts, rowid, body, context)
		VALUES ('delete', old.rowid, old.body, old.context);
	END;
	CREATE TRIGGER IF NOT EXISTS memories_fts_au AFTER UPDATE OF body, context ON memories BEGIN
		INSERT INTO memories_fts (memories_fts, rowid, body, context)
		VALUES ('delete', old.rowid, old.body, old.context);
		INSERT INTO memories_fts (rowid, body, context)
		VALUES (new.rowid, new.body, new.context);
	END;
";

const MEMORY_EMBEDDINGS_DDL: &str = "
	CREATE TABLE IF NOT EXISTS memory_embeddings (
		memory_id TEXT NOT NULL REFERENCES memories (id) ON DELETE CASCADE,
		model TEXT NOT NULL,
		dim INTEGER NOT NULL CHECK (dim > 0),
		embedding BLOB NOT NULL,
		created_at INTEGER NOT NULL,
		PRIMARY KEY (memory_id, model)
	);
	CREATE INDEX IF NOT EXISTS memory_embeddings_model
		ON memory_embeddings (model);
";

/// Failure of one memory-store operation. Never downgraded to a warning.
#[derive(Debug)]
pub enum MemoryStoreError {
	/// SQLite rejected the operation or returned unusable data.
	Sqlite(rusqlite::Error),
	/// Legacy import or file export failed.
	Io(std::io::Error),
	/// Caller supplied an incomplete or inconsistent record.
	Validation(String),
	/// The requested record does not exist or is not visible in this project.
	NotFound(String),
	/// An edit would collide with another record's identity in the same scope.
	Conflict(String),
	/// The stored schema cannot be used by this binary.
	Schema(String),
	/// The `learned.md` export artifact could not be rewritten.
	Export(String),
}

impl fmt::Display for MemoryStoreError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Sqlite(err) => write!(f, "memory store: {err}"),
			Self::Io(err) => write!(f, "memory store: {err}"),
			Self::Validation(err) => write!(f, "memory store: {err}"),
			Self::NotFound(err) => write!(f, "memory store: {err}"),
			Self::Conflict(err) => write!(f, "memory store conflict: {err}"),
			Self::Schema(err) => write!(f, "memory store schema: {err}"),
			Self::Export(err) => write!(f, "memory store export: {err}"),
		}
	}
}

impl std::error::Error for MemoryStoreError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Sqlite(err) => Some(err),
			Self::Io(err) => Some(err),
			Self::Validation(_)
			| Self::NotFound(_)
			| Self::Conflict(_)
			| Self::Schema(_)
			| Self::Export(_) => None,
		}
	}
}

impl From<rusqlite::Error> for MemoryStoreError {
	fn from(err: rusqlite::Error) -> Self {
		Self::Sqlite(err)
	}
}

impl From<std::io::Error> for MemoryStoreError {
	fn from(err: std::io::Error) -> Self {
		Self::Io(err)
	}
}

impl From<MemoryStoreError> for crate::Error {
	fn from(err: MemoryStoreError) -> Self {
		crate::Error::new(err.to_string())
	}
}

pub type MemoryResult<T> = Result<T, MemoryStoreError>;

/// Where a record belongs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
	/// Owner preferences and portable experience, visible from every project.
	Global,
	/// Knowledge about one project, visible only under its `cwd`.
	Project,
	/// Knowledge about one conversation, visible only under its `session_id`.
	Session,
}

impl Scope {
	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Global => "global",
			Self::Project => "project",
			Self::Session => "session",
		}
	}

	#[must_use]
	pub fn parse(raw: &str) -> Option<Self> {
		match raw {
			"global" => Some(Self::Global),
			"project" => Some(Self::Project),
			"session" => Some(Self::Session),
			_ => None,
		}
	}
}

/// How the record came to be asserted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
	/// The user explicitly stated it.
	Asserted,
	/// The agent inferred it (including `learn` captures and legacy imports).
	Inferred,
}

impl Origin {
	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Asserted => "asserted",
			Self::Inferred => "inferred",
		}
	}

	#[must_use]
	pub fn parse(raw: &str) -> Option<Self> {
		match raw {
			"asserted" => Some(Self::Asserted),
			"inferred" => Some(Self::Inferred),
			_ => None,
		}
	}
}

/// One durable memory row.
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRecord {
	pub id: String,
	pub scope: Scope,
	/// Project path for `project` rows; always `None` for `global`/`session`.
	pub cwd: Option<String>,
	/// Conversation id for `session` rows; `None` otherwise.
	pub session_id: Option<String>,
	pub body: String,
	/// Legacy `learn` context (`_(context: ...)_` in `learned.md`).
	pub context: Option<String>,
	pub origin: Origin,
	pub actor: Option<String>,
	pub source: Option<String>,
	pub created_at: i64,
	pub updated_at: i64,
	pub access_count: u64,
	pub last_accessed_at: Option<i64>,
	/// Caller confidence in `0.0..=1.0`; defaults to `1.0`.
	pub confidence: f64,
	/// Record expiry relative to `created_at`; `None` never expires.
	pub ttl_secs: Option<i64>,
	/// Canonical row this duplicate was consolidated into.
	pub consolidated_into: Option<String>,
}

/// One `add` request.
#[derive(Debug, Clone)]
pub struct AddInput<'a> {
	pub scope: Scope,
	pub cwd: Option<&'a str>,
	pub session_id: Option<&'a str>,
	pub body: &'a str,
	pub context: Option<&'a str>,
	pub origin: Origin,
	pub actor: Option<&'a str>,
	pub source: Option<&'a str>,
	/// When true, an identical `(scope, cwd, session_id, body, context)` record
	/// is touched instead of duplicated. Legacy import and `learn` use this.
	pub dedupe: bool,
}

impl<'a> AddInput<'a> {
	/// Agent-captured lesson in one project (the `learn` tool path).
	#[must_use]
	pub fn project_lesson(cwd: &'a str, body: &'a str, context: Option<&'a str>) -> Self {
		Self {
			scope: Scope::Project,
			cwd: Some(cwd),
			session_id: None,
			body,
			context,
			origin: Origin::Inferred,
			actor: Some("agent"),
			source: Some("learn"),
			dedupe: true,
		}
	}

	/// Agent-captured lesson in one conversation.
	#[must_use]
	pub fn session_lesson(
		session_id: &'a str,
		body: &'a str,
		context: Option<&'a str>,
	) -> Self {
		Self {
			scope: Scope::Session,
			cwd: None,
			session_id: Some(session_id),
			body,
			context,
			origin: Origin::Inferred,
			actor: Some("agent"),
			source: Some("learn"),
			dedupe: true,
		}
	}

	/// Explicit user assertion, global or project.
	#[must_use]
	pub fn asserted(scope: Scope, cwd: Option<&'a str>, body: &'a str) -> Self {
		Self {
			scope,
			cwd,
			session_id: None,
			body,
			context: None,
			origin: Origin::Asserted,
			actor: Some("user"),
			source: Some("user"),
			dedupe: false,
		}
	}
}

/// Scope selector for [`MemoryStore::list`] and [`MemoryStore::recall`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeFilter {
	/// Global rows plus the rows of `cwd` and/or `session_id` when set; all
	/// rows otherwise.
	All,
	Global,
	Project,
	Session,
}

/// `list`/`recall` filter. `Project` requires a non-empty `cwd`; `Session`
/// requires a non-empty `session_id`.
#[derive(Debug, Clone, Default)]
pub struct Filter {
	pub scope: ScopeFilter,
	pub cwd: Option<String>,
	pub session_id: Option<String>,
	pub limit: Option<usize>,
}

impl Filter {
	/// Global rows plus the given project (the normal session view).
	#[must_use]
	pub fn all(cwd: Option<&str>) -> Self {
		Self {
			scope: ScopeFilter::All,
			cwd: cwd.map(str::to_string),
			session_id: None,
			limit: None,
		}
	}

	/// One project only.
	#[must_use]
	pub fn project(cwd: &str) -> Self {
		Self {
			scope: ScopeFilter::Project,
			cwd: Some(cwd.to_string()),
			session_id: None,
			limit: None,
		}
	}

	/// One conversation only.
	#[must_use]
	pub fn session(session_id: &str) -> Self {
		Self {
			scope: ScopeFilter::Session,
			cwd: None,
			session_id: Some(session_id.to_string()),
			limit: None,
		}
	}

	/// Global rows only.
	#[must_use]
	pub fn global() -> Self {
		Self {
			scope: ScopeFilter::Global,
			cwd: None,
			session_id: None,
			limit: None,
		}
	}

	/// Include this conversation's rows in an `All` view.
	#[must_use]
	pub fn with_session(mut self, session_id: &str) -> Self {
		self.session_id = Some(session_id.to_string());
		self
	}

	#[must_use]
	pub fn with_limit(mut self, limit: usize) -> Self {
		self.limit = Some(limit);
		self
	}
}

impl Default for ScopeFilter {
	fn default() -> Self {
		Self::All
	}
}

/// Outcome of importing legacy `learned.md` bullets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportReport {
	pub path: PathBuf,
	pub found: usize,
	pub imported: usize,
	pub skipped: usize,
	/// True when this project scope was already migrated and the file was not
	/// read (a later export/learn/delete must never re-import it).
	pub already_imported: bool,
}

/// Outcome of one deterministic consolidation pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConsolidationReport {
	/// TTL-expired rows deleted.
	pub expired: usize,
	/// Duplicate rows marked through `consolidated_into`.
	pub consolidated: usize,
	/// Near-duplicate clusters found.
	pub clusters: usize,
	/// Always zero: consolidation is deterministic and never calls an LLM.
	pub llm_used: u8,
}

/// One connection per agent directory. `rusqlite::Connection` is `Send` and
/// not `Sync`; the mutex is what lets the protocol handler share the store.
pub struct MemoryStore {
	path: PathBuf,
	connection: Mutex<Connection>,
}

impl MemoryStore {
	/// `<agent dir>/memory.db`.
	#[must_use]
	pub fn database_path(agent_dir: &Path) -> PathBuf {
		agent_dir.join(MEMORY_DB_FILE)
	}

	/// Open (creating the schema if needed) the agent's memory store.
	pub fn open(agent_dir: &Path) -> MemoryResult<Self> {
		Self::open_at(Self::database_path(agent_dir))
	}

	/// Open an existing store; missing database is an explicit error, so a read
	/// never silently creates an empty store.
	pub fn open_existing(agent_dir: &Path) -> MemoryResult<Self> {
		let path = Self::database_path(agent_dir);
		if !path.is_file() {
			return Err(MemoryStoreError::NotFound(format!(
				"memory store not found at {}; run a session with memories enabled first",
				path.display()
			)));
		}
		Self::open_at(path)
	}

	/// Open a store at an explicit path (tests, alternate agent dirs).
	pub fn open_at(path: impl Into<PathBuf>) -> MemoryResult<Self> {
		let path = path.into();
		if path.as_os_str().is_empty() {
			return Err(MemoryStoreError::Validation(
				"memory store path must not be empty".to_string(),
			));
		}
		// One file must mean one lock. The directory is created first and then
		// resolved, so `..`, relative and symlinked-directory spellings of the
		// same store collapse onto one key and one connection target. Only the
		// directory is resolved — a symlinked `memory.db` file keeps its own key —
		// and hardlinks, bind mounts or a second process are NOT unified here:
		// those keep relying on SQLite's own file locking.
		let parent = path
			.parent()
			.filter(|parent| !parent.as_os_str().is_empty());
		let file_name = path
			.file_name()
			.ok_or_else(|| {
				MemoryStoreError::Validation(format!(
					"memory store path {} has no file name",
					path.display()
				))
			})?
			.to_os_string();
		let parent = match parent {
			Some(parent) => {
				fs::create_dir_all(parent)?;
				fs::canonicalize(parent)?
			},
			None => fs::canonicalize(".")?,
		};
		let path = parent.join(file_name);
		// WAL switch, pragmas and schema init all take filesystem locks on this
		// one file. Serialized per database path, so a second thread opening the
		// same fresh store cannot race the switch into `database is locked`,
		// while other agent dirs open in parallel. Cross-process writers are not
		// ordered by this lock: they still resolve through `busy_timeout`, and a
		// lost race is reported instead of retried.
		crate::local_memory::with_path_lock(&path, || {
			let mut connection = Connection::open(&path)?;
			connection.busy_timeout(std::time::Duration::from_secs(5))?;
			let journal_mode: String =
				connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
			if !journal_mode.eq_ignore_ascii_case("wal") {
				// Falling back to the rollback journal silently would make readers
				// block writers and turn recall into a `database is locked` source.
				return Err(MemoryStoreError::Schema(format!(
					"memory store journal mode is {journal_mode}, expected wal"
				)));
			}
			connection.pragma_update(None, "synchronous", "FULL")?;
			connection.pragma_update(None, "foreign_keys", "ON")?;
			initialize(&mut connection)?;
			Ok(Self { path: path.clone(), connection: Mutex::new(connection) })
		})
	}

	/// In-memory store with the same schema, for tests.
	pub fn open_in_memory() -> MemoryResult<Self> {
		let mut connection = Connection::open_in_memory()?;
		connection.busy_timeout(std::time::Duration::from_secs(5))?;
		connection.pragma_update(None, "foreign_keys", "ON")?;
		initialize(&mut connection)?;
		Ok(Self { path: PathBuf::from(":memory:"), connection: Mutex::new(connection) })
	}

	#[must_use]
	pub fn path(&self) -> &Path {
		&self.path
	}

	/// Insert a record, or touch the existing record with the same dedupe key.
	/// A body that sanitizes to nothing is a validation error, never a no-op.
	pub fn add(&self, input: &AddInput<'_>) -> MemoryResult<MemoryRecord> {
		let body = normalize_learned_text(input.body, MAX_LEARNED_CONTENT_CHARS);
		if body.is_empty() {
			return Err(MemoryStoreError::Validation(
				"memory body is empty after sanitization".to_string(),
			));
		}
		let context = input
			.context
			.map(|context| normalize_learned_text(context, MAX_LEARNED_CONTEXT_CHARS))
			.filter(|context| !context.is_empty());
		let cwd = normalize_scope(input.scope, input.cwd)?;
		let session_id = normalize_session(input.scope, input.session_id)?;
		let actor = clean_optional(input.actor);
		let source = clean_optional(input.source);
		let dedupe_key = input.dedupe.then(|| {
			record_dedupe_key(
				input.scope,
				cwd.as_deref(),
				session_id.as_deref(),
				&body,
				context.as_deref(),
			)
		});

		crate::local_memory::with_path_lock(&self.path, || {
			let mut guard = self.lock();
			let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
			if let Some(key) = dedupe_key.as_deref()
				&& let Some(id) = transaction
					.query_row("SELECT id FROM memories WHERE dedupe_key = ?1", params![key], |row| {
						row.get::<_, String>(0)
					})
					.optional()?
			{
				let updated_at = next_timestamp(&transaction)?;
				transaction.execute(
					"UPDATE memories SET updated_at = ?1, consolidated_into = NULL WHERE id = ?2",
					params![updated_at, id],
				)?;
				let record = load_record(&transaction, &id)?.ok_or_else(|| {
					MemoryStoreError::NotFound(format!("memory record not found: {id}"))
				})?;
				transaction.commit()?;
				return Ok(record);
			}

			let updated_at = next_timestamp(&transaction)?;
			let id: String = transaction.query_row(
				"INSERT INTO memories \
				 (id, scope, cwd, session_id, body, context, origin, actor, source, created_at, updated_at, dedupe_key) \
				 VALUES (lower(hex(randomblob(16))), ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
				 RETURNING id",
				params![
					input.scope.as_str(),
					cwd,
					session_id,
					body,
					context,
					input.origin.as_str(),
					actor,
					source,
					updated_at,
					updated_at,
					dedupe_key,
				],
				|row| row.get(0),
			)?;
			let record = load_record(&transaction, &id)?.ok_or_else(|| {
				MemoryStoreError::NotFound(format!("memory record not found after insert: {id}"))
			})?;
			transaction.commit()?;
			Ok(record)
		})
	}

	/// List visible records newest-first. `ScopeFilter::All` without a `cwd` is
	/// an explicit cross-project listing; every session view passes a `cwd`.
	pub fn list(&self, filter: &Filter) -> MemoryResult<Vec<MemoryRecord>> {
		let (clause, values) = filter_clause(filter)?;
		let limit = filter
			.limit
			.unwrap_or(MAX_LIST_LIMIT)
			.clamp(1, MAX_LIST_LIMIT);
		let sql = format!(
			"SELECT {} FROM memories WHERE {clause} \
			 ORDER BY updated_at DESC, rowid ASC LIMIT {limit}",
			RECORD_COLUMNS
		);
		let connection = self.lock();
		let mut statement = connection.prepare(&sql)?;
		let rows = statement.query_map(params_from_iter(values.iter()), row_to_record)?;
		let mut records = Vec::new();
		for row in rows {
			records.push(row?);
		}
		Ok(records)
	}

	/// Load one record. `cwd` is the caller's project: records from another
	/// project are reported as not found rather than disclosed.
	pub fn get(&self, id: &str, cwd: Option<&str>) -> MemoryResult<MemoryRecord> {
		self.get_with_session(id, cwd, None)
	}

	/// Like [`Self::get`], also admitting this conversation's `session` rows.
	pub fn get_with_session(
		&self,
		id: &str,
		cwd: Option<&str>,
		session_id: Option<&str>,
	) -> MemoryResult<MemoryRecord> {
		let id = normalize_record_id(id)?;
		let session_id = clean_session_id(session_id);
		let connection = self.lock();
		let record = load_record(&connection, &id)?
			.ok_or_else(|| MemoryStoreError::NotFound(format!("memory record not found: {id}")))?;
		ensure_visible(&record, cwd, session_id.as_deref())?;
		Ok(record)
	}

	/// Replace a record body. Identity (id, scope, cwd, session_id, origin,
	/// actor, source) is immutable; timestamps move `updated_at` forward.
	pub fn update(&self, id: &str, cwd: Option<&str>, body: &str) -> MemoryResult<MemoryRecord> {
		self.update_with_session(id, cwd, None, body)
	}

	/// Like [`Self::update`], also admitting this conversation's `session` rows.
	pub fn update_with_session(
		&self,
		id: &str,
		cwd: Option<&str>,
		session_id: Option<&str>,
		body: &str,
	) -> MemoryResult<MemoryRecord> {
		let id = normalize_record_id(id)?;
		let session_id = clean_session_id(session_id);
		let body = normalize_learned_text(body, MAX_LEARNED_CONTENT_CHARS);
		if body.is_empty() {
			return Err(MemoryStoreError::Validation(
				"memory body is empty after sanitization".to_string(),
			));
		}
		crate::local_memory::with_path_lock(&self.path, || {
			let mut guard = self.lock();
			let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
			let current = load_record(&transaction, &id)?
				.ok_or_else(|| MemoryStoreError::NotFound(format!("memory record not found: {id}")))?;
			ensure_visible(&current, cwd, session_id.as_deref())?;
			// Identity (dedupe key) follows the body, or a later legacy-free import
			// and a re-learned original would otherwise duplicate or silently vanish.
			let dedupe_key = record_dedupe_key(
				current.scope,
				current.cwd.as_deref(),
				current.session_id.as_deref(),
				&body,
				current.context.as_deref(),
			);
			let collision: Option<String> = transaction
				.query_row(
					"SELECT id FROM memories WHERE dedupe_key = ?1 AND id != ?2",
					params![dedupe_key, id],
					|row| row.get(0),
				)
				.optional()?;
			if let Some(other) = collision {
				return Err(MemoryStoreError::Conflict(format!(
					"record {other} already has this body in the same scope"
				)));
			}
			let updated_at = next_timestamp(&transaction)?;
			transaction.execute(
				"UPDATE memories SET body = ?1, dedupe_key = ?2, updated_at = ?3 WHERE id = ?4",
				params![body, dedupe_key, updated_at, id],
			)?;
			// The stored vector described the old text; never rank stale vectors.
			transaction.execute(
				"DELETE FROM memory_embeddings WHERE memory_id = ?1",
				params![id],
			)?;
			let record = load_record(&transaction, &id)?
				.ok_or_else(|| MemoryStoreError::NotFound(format!("memory record not found: {id}")))?;
			transaction.commit()?;
			Ok(record)
		})
	}

	/// Delete one record and nothing else. This never touches `sessions.db`
	/// (conversation history is a different file and a different authority).
	pub fn delete(&self, id: &str, cwd: Option<&str>) -> MemoryResult<()> {
		self.delete_with_session(id, cwd, None)
	}

	/// Like [`Self::delete`], also admitting this conversation's `session` rows.
	pub fn delete_with_session(
		&self,
		id: &str,
		cwd: Option<&str>,
		session_id: Option<&str>,
	) -> MemoryResult<()> {
		let id = normalize_record_id(id)?;
		let session_id = clean_session_id(session_id);
		crate::local_memory::with_path_lock(&self.path, || {
			let mut guard = self.lock();
			let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
			let current = load_record(&transaction, &id)?
				.ok_or_else(|| MemoryStoreError::NotFound(format!("memory record not found: {id}")))?;
			ensure_visible(&current, cwd, session_id.as_deref())?;
			transaction.execute(
				"DELETE FROM memory_embeddings WHERE memory_id = ?1",
				params![id],
			)?;
			transaction.execute("DELETE FROM memories WHERE id = ?1", params![id])?;
			transaction.commit()?;
			Ok(())
		})
	}

	/// Bounded lexical recall over the visible scope.
	///
	/// Tokens from `query` are prefix-matched through the FTS5 index and ranked
	/// by BM25, then recency, then id. The SQL scope predicate is applied
	/// before `LIMIT`, so other projects' or sessions' rows can never occupy
	/// the candidate window. Recall increments `access_count` and
	/// `last_accessed_at` for returned rows.
	pub fn recall(
		&self,
		query: &str,
		cwd: Option<&str>,
		limit: Option<usize>,
	) -> MemoryResult<Vec<MemoryRecord>> {
		self.recall_with_scope(query, cwd, None, limit, ScopeFilter::All, None)
	}

	/// Global-only recall.
	pub fn recall_global(
		&self,
		query: &str,
		limit: Option<usize>,
	) -> MemoryResult<Vec<MemoryRecord>> {
		self.recall_with_scope(query, None, None, limit, ScopeFilter::Global, None)
	}

	/// Hybrid recall: BM25 always runs, and a supplied query embedding (for
	/// `model`) blends in cosine similarity and MMR diversity when stored
	/// vectors exist. Missing/partial vectors leave the lexical ranking intact
	/// rather than failing the call, so offline recall never regresses.
	pub fn recall_hybrid(
		&self,
		query: &str,
		cwd: Option<&str>,
		session_id: Option<&str>,
		embedding: Option<(&str, &[f32])>,
		limit: Option<usize>,
	) -> MemoryResult<Vec<MemoryRecord>> {
		self.recall_with_scope(query, cwd, session_id, limit, ScopeFilter::All, embedding)
	}

	fn recall_with_scope(
		&self,
		query: &str,
		cwd: Option<&str>,
		session_id: Option<&str>,
		limit: Option<usize>,
		scope: ScopeFilter,
		embedding: Option<(&str, &[f32])>,
	) -> MemoryResult<Vec<MemoryRecord>> {
		let tokens = query_tokens(query);
		let limit = limit
			.unwrap_or(DEFAULT_RECALL_LIMIT)
			.clamp(1, MAX_RECALL_LIMIT);
		let mut filter = Filter {
			scope,
			cwd: cwd.map(str::to_string),
			session_id: clean_session_id(session_id),
			limit: None,
		};
		if filter
			.cwd
			.as_deref()
			.is_some_and(|value| value.trim().is_empty())
		{
			filter.cwd = None;
		}
		let (clause, clause_values) = filter_clause(&filter)?;
		let now = now_seconds();
		let candidate_limit = limit
			.saturating_mul(4)
			.min(MAX_RECALL_CANDIDATES)
			.max(limit);
		let connection = self.lock();

		let (query_model, query_vector, vectors) = match embedding {
			Some((model, vector)) => {
				let model = model.trim();
				if model.is_empty() {
					return Err(MemoryStoreError::Validation(
						"embedding model must not be empty".to_string(),
					));
				}
				if vector.is_empty() || vector.iter().any(|value| !value.is_finite()) {
					return Err(MemoryStoreError::Validation(
						"query embedding must be finite and non-empty".to_string(),
					));
				}
				let model = model.to_string();
				let vectors = load_embeddings_for_model(&connection, &model)?;
				(
					Some(model),
					Some(vector.iter().map(|&value| f64::from(value)).collect::<Vec<_>>()),
					vectors,
				)
			},
			None => (None, None, BTreeMap::new()),
		};

		let mut candidates: Vec<(MemoryRecord, f64)> = Vec::new();
		if !tokens.is_empty() {
			let mut values = clause_values.clone();
			values.insert(0, Value::Text(fts_query(&tokens)));
			values.push(Value::Integer(now));
			let sql = format!(
				"WITH hits AS ( \
					SELECT rowid, bm25(memories_fts) AS rank FROM memories_fts \
					WHERE memories_fts MATCH ? \
				 ) \
				 SELECT {RECORD_COLUMNS}, hits.rank FROM memories \
				 JOIN hits ON hits.rowid = memories.rowid \
				 WHERE {clause} AND consolidated_into IS NULL \
				   AND (ttl_secs IS NULL OR created_at + ttl_secs > ? OR origin = 'asserted') \
				 ORDER BY hits.rank ASC, updated_at DESC, memories.rowid ASC LIMIT {candidate_limit}"
			);
			let mut statement = connection.prepare(&sql)?;
			let rows = statement.query_map(params_from_iter(values.iter()), |row| {
				let record = row_to_record(row)?;
				let rank: f64 = row.get(16)?;
				Ok((record, rank))
			})?;
			for row in rows {
				candidates.push(row?);
			}
		}

		// A semantic hit can have no lexical overlap at all. Union the scoped
		// vector top-k into the FTS candidate set; absent vectors keep a neutral
		// lexical score of zero and still flow through the same MMR pass.
		let mut seen: BTreeSet<String> = candidates.iter().map(|(row, _)| row.id.clone()).collect();
		if let (Some(model), Some(query_vector)) = (query_model.as_deref(), query_vector.as_ref()) {
			let mut vector_values = clause_values;
			vector_values.insert(0, Value::Text(model.to_string()));
			vector_values.push(Value::Integer(now));
			let sql = format!(
				"SELECT {RECORD_COLUMNS_QUALIFIED}, e.dim, e.embedding FROM memories \
				 JOIN memory_embeddings e ON e.memory_id = memories.id \
				 WHERE e.model = ? AND {clause} AND memories.consolidated_into IS NULL \
				   AND (memories.ttl_secs IS NULL OR memories.created_at + memories.ttl_secs > ? \
				        OR memories.origin = 'asserted') \
				 ORDER BY memories.rowid"
			);
			let mut vector_rows: Vec<(MemoryRecord, Vec<f32>)> = Vec::new();
			{
				let mut statement = connection.prepare(&sql)?;
				let rows = statement.query_map(params_from_iter(vector_values.iter()), |row| {
					let record = row_to_record(row)?;
					let dim: i64 = row.get(16)?;
					let blob: Vec<u8> = row.get(17)?;
					Ok((record, dim, blob))
				})?;
				for row in rows {
					let (record, dim, blob) = row?;
					let Some(vector) = decode_embedding(&blob, dim) else {
						continue;
					};
					if vector.len() == query_vector.len() {
						vector_rows.push((record, vector));
					}
				}
			}
			let query_norm = query_vector
				.iter()
				.map(|value| value * value)
				.sum::<f64>()
				.sqrt();
			if !vector_rows.is_empty() && query_norm > 0.0 && query_norm.is_finite() {
				let dimensions = query_vector.len();
				let mut matrix = Vec::with_capacity(vector_rows.len() * dimensions);
				for (_, vector) in &vector_rows {
					let norm = vector
						.iter()
						.map(|value| {
							let value = f64::from(*value);
							value * value
						})
						.sum::<f64>()
						.sqrt();
					if norm > 0.0 && norm.is_finite() {
						matrix.extend(vector.iter().map(|value| (f64::from(*value) / norm) as f32));
					} else {
						matrix.extend(std::iter::repeat_n(0.0f32, dimensions));
					}
				}
				let order = vector_index_top_k(
					&matrix,
					dimensions,
					query_vector,
					candidate_limit,
				)
				.map_err(|message| MemoryStoreError::Validation(message.to_string()))?;
				for index in order.indices {
					let Some((record, _)) = vector_rows.get(index as usize) else {
						continue;
					};
					if seen.insert(record.id.clone()) {
						candidates.push((record.clone(), 0.0));
					}
				}
			}
		}

		let use_vectors = query_vector.as_ref().is_some_and(|query| {
			candidates.iter().any(|(record, _)| {
				vectors
					.get(&record.id)
					.is_some_and(|vector| vector.len() == query.len())
			})
		});
		let max_lexical = candidates
			.iter()
			.map(|(_, rank)| (-rank).max(0.0))
			.fold(0.0f64, f64::max);
		let mut scored: Vec<(MemoryRecord, f64)> = candidates
			.into_iter()
			.map(|(record, rank)| {
				let lexical = (-rank).max(0.0)
					/ if max_lexical > 0.0 { max_lexical } else { 1.0 };
				let score = if use_vectors {
					let vector_score = query_vector.as_ref().map_or(0.5, |query| {
						vectors
							.get(&record.id)
							.filter(|vector| vector.len() == query.len())
							.map_or(0.5, |vector| {
								let stored: Vec<f64> =
									vector.iter().map(|&value| f64::from(value)).collect();
								((cosine_similarity(query, &stored) + 1.0) / 2.0).clamp(0.0, 1.0)
							})
					});
					HYBRID_LEXICAL_WEIGHT * lexical
						+ (1.0 - HYBRID_LEXICAL_WEIGHT) * vector_score
				} else {
					lexical
				};
				(record, score)
			})
			.collect();
		scored.sort_by(|(left, left_score), (right, right_score)| {
			right_score
				.total_cmp(left_score)
				.then_with(|| right.updated_at.cmp(&left.updated_at))
				.then_with(|| left.id.cmp(&right.id))
		});

		let mut records: Vec<MemoryRecord> = if use_vectors {
			let bodies: Vec<&str> = scored
				.iter()
				.map(|(record, _)| record.body.as_str())
				.collect();
			let scores: Vec<f64> = scored.iter().map(|(_, score)| *score).collect();
			let order = mmr_rerank_indices(&bodies, &scores, HYBRID_MMR_LAMBDA, limit)
				.map_err(|message| MemoryStoreError::Validation(message.to_string()))?;
			order
				.into_iter()
				.map(|index| scored[index as usize].0.clone())
				.collect()
		} else {
			scored.truncate(limit);
			scored.into_iter().map(|(record, _)| record).collect()
		};

		if !records.is_empty() {
			let mut update = connection.prepare_cached(
				"UPDATE memories SET access_count = access_count + 1, last_accessed_at = ?1 				 WHERE id = ?2",
			)?;
			for record in &mut records {
				update.execute(params![now, record.id])?;
				record.access_count = record.access_count.saturating_add(1);
				record.last_accessed_at = Some(now);
			}
		}
		Ok(records)
	}

	/// Store or replace one model's vector for a record. Vectors are `f32`
	/// little-endian blobs; non-finite values are rejected so ranking never
	/// sees corrupt input.
	pub fn set_embedding(&self, id: &str, model: &str, vector: &[f32]) -> MemoryResult<()> {
		let id = normalize_record_id(id)?;
		let model = model.trim();
		if model.is_empty() {
			return Err(MemoryStoreError::Validation(
				"embedding model must not be empty".to_string(),
			));
		}
		if vector.is_empty() || vector.iter().any(|value| !value.is_finite()) {
			return Err(MemoryStoreError::Validation(
				"embedding must be finite and non-empty".to_string(),
			));
		}
		let dim = i64::try_from(vector.len()).map_err(|_| {
			MemoryStoreError::Validation("embedding dimensions do not fit in i64".to_string())
		})?;
		let encoded = encode_embedding(vector);
		crate::local_memory::with_path_lock(&self.path, || {
			let mut guard = self.lock();
			let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
			transaction.execute(
				"INSERT INTO memory_embeddings (memory_id, model, dim, embedding, created_at) \
				 VALUES (?1, ?2, ?3, ?4, ?5) \
				 ON CONFLICT (memory_id, model) DO UPDATE SET \
				   dim = excluded.dim, embedding = excluded.embedding, \
				   created_at = excluded.created_at",
				params![id, model, dim, encoded, now_seconds()],
			)?;
			transaction.commit()?;
			Ok(())
		})
	}

	/// Read one stored vector.
	pub fn embedding(&self, id: &str, model: &str) -> MemoryResult<Option<Vec<f32>>> {
		let id = normalize_record_id(id)?;
		let connection = self.lock();
		let row: Option<(i64, Vec<u8>)> = connection
			.query_row(
				"SELECT dim, embedding FROM memory_embeddings WHERE memory_id = ?1 AND model = ?2",
				params![id, model.trim()],
				|row| Ok((row.get(0)?, row.get(1)?)),
			)
			.optional()?;
		Ok(row.and_then(|(dim, blob)| decode_embedding(&blob, dim)))
	}

	/// Delete one stored vector.
	pub fn delete_embedding(&self, id: &str, model: &str) -> MemoryResult<()> {
		let id = normalize_record_id(id)?;
		crate::local_memory::with_path_lock(&self.path, || {
			let guard = self.lock();
			guard.execute(
				"DELETE FROM memory_embeddings WHERE memory_id = ?1 AND model = ?2",
				params![id, model.trim()],
			)?;
			Ok(())
		})
	}

	/// Set or clear a record's TTL in seconds.
	pub fn set_ttl(&self, id: &str, ttl_secs: Option<i64>) -> MemoryResult<()> {
		let id = normalize_record_id(id)?;
		if ttl_secs.is_some_and(|ttl| ttl < 0) {
			return Err(MemoryStoreError::Validation(
				"ttl_secs must be non-negative".to_string(),
			));
		}
		self.update_metadata(&id, "ttl_secs", ttl_secs.map(Value::Integer).unwrap_or(Value::Null))
	}

	/// Set a record's confidence in `0.0..=1.0`.
	pub fn set_confidence(&self, id: &str, confidence: f64) -> MemoryResult<()> {
		let id = normalize_record_id(id)?;
		if !confidence.is_finite() || !(0.0..=1.0).contains(&confidence) {
			return Err(MemoryStoreError::Validation(
				"confidence must be a finite value in 0.0..=1.0".to_string(),
			));
		}
		self.update_metadata(&id, "confidence", Value::Real(confidence))
	}

	/// Mark a duplicate as consolidated into another record, or clear it.
	///
	/// A canonical target must belong to the same `(scope, cwd, session_id)`;
	/// otherwise consolidation could hide a record behind another project's or
	/// conversation's canonical row.
	pub fn set_consolidated_into(&self, id: &str, canonical: Option<&str>) -> MemoryResult<()> {
		let id = normalize_record_id(id)?;
		let canonical = canonical.map(normalize_record_id).transpose()?;
		if canonical.as_deref() == Some(id.as_str()) {
			return Err(MemoryStoreError::Validation(
				"a record cannot be consolidated into itself".to_string(),
			));
		}
		crate::local_memory::with_path_lock(&self.path, || {
			let mut guard = self.lock();
			let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
			let source = transaction
				.query_row(
					"SELECT scope, cwd, session_id FROM memories WHERE id = ?1",
					params![id],
					|row| {
						Ok((
							row.get::<_, String>(0)?,
							row.get::<_, Option<String>>(1)?,
							row.get::<_, Option<String>>(2)?,
						))
					},
				)
				.optional()?
				.ok_or_else(|| {
					MemoryStoreError::NotFound(format!("memory record not found: {id}"))
				})?;
			if let Some(canonical_id) = canonical.as_deref() {
				let target = transaction
					.query_row(
						"SELECT scope, cwd, session_id FROM memories WHERE id = ?1",
						params![canonical_id],
						|row| {
							Ok((
								row.get::<_, String>(0)?,
								row.get::<_, Option<String>>(1)?,
								row.get::<_, Option<String>>(2)?,
							))
						},
					)
					.optional()?
					.ok_or_else(|| {
						MemoryStoreError::NotFound(format!(
							"consolidation target not found: {canonical_id}"
						))
					})?;
				if source != target {
					return Err(MemoryStoreError::Validation(
						"consolidation target must share scope, cwd, and session_id"
							.to_string(),
					));
				}
			}
			let changed = transaction.execute(
				"UPDATE memories SET consolidated_into = ?1 WHERE id = ?2",
				params![canonical, id],
			)?;
			if changed == 0 {
				return Err(MemoryStoreError::NotFound(format!(
					"memory record not found: {id}"
				)));
			}
			transaction.commit()?;
			Ok(())
		})
	}

	fn update_metadata(&self, id: &str, column: &str, value: Value) -> MemoryResult<()> {
		crate::local_memory::with_path_lock(&self.path, || {
			let guard = self.lock();
			let changed = guard.execute(
				&format!("UPDATE memories SET {column} = ?1 WHERE id = ?2"),
				params![value, id],
			)?;
			if changed == 0 {
				return Err(MemoryStoreError::NotFound(format!(
					"memory record not found: {id}"
				)));
			}
			Ok(())
		})
	}

	/// Deterministic `sleep`-style maintenance: delete TTL-expired rows, then
	/// mark near-duplicate clusters (cosine ≥
	/// [`CONSOLIDATION_SIMILARITY_THRESHOLD`], same scope/cwd/session/model/dim)
	/// through `consolidated_into`. The lowest id in a cluster is canonical.
	/// No model call is made; `llm_used` is always zero.
	pub fn consolidate(&self) -> MemoryResult<ConsolidationReport> {
		self.consolidate_at(now_seconds())
	}

	/// [`Self::consolidate`] with an injected clock, for deterministic tests.
	pub fn consolidate_at(&self, now: i64) -> MemoryResult<ConsolidationReport> {
		let report = crate::local_memory::with_path_lock(&self.path, || {
			let mut guard = self.lock();
			let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
			let mut report = ConsolidationReport::default();
			// Asserted rows are user commitments and never expire silently.
			let expired_project_cwds: Vec<String> = {
				let mut statement = transaction.prepare(
					"SELECT DISTINCT cwd FROM memories WHERE scope = 'project' \
					 AND cwd IS NOT NULL AND origin = 'inferred' AND ttl_secs IS NOT NULL \
					 AND created_at + ttl_secs <= ?1",
				)?;
				let rows = statement.query_map(params![now], |row| row.get::<_, String>(0))?;
				let mut cwds = Vec::new();
				for row in rows {
					cwds.push(row?);
				}
				cwds
			};
			// Persist the export obligation before deleting rows: a crash or a
			// failed `learned.md` write leaves a retry marker for the next pass.
			for cwd in &expired_project_cwds {
				transaction.execute(
					"INSERT INTO memory_export_dirty (cwd, dirty, dirty_gen) VALUES (?1, 1, 1) \
					 ON CONFLICT(cwd) DO UPDATE SET dirty = 1, dirty_gen = dirty_gen + 1",
					params![cwd.as_str()],
				)?;
			}
			report.expired = transaction.execute(
				"DELETE FROM memories WHERE ttl_secs IS NOT NULL \
				 AND created_at + ttl_secs <= ?1 AND origin = 'inferred'",
				params![now],
			)?;

			let models: Vec<String> = {
				let mut statement =
					transaction.prepare("SELECT DISTINCT model FROM memory_embeddings ORDER BY model")?;
				let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
				let mut models = Vec::new();
				for row in rows {
					models.push(row?);
				}
				models
			};
			for model in models {
				let rows: Vec<(String, String, Option<String>, Option<String>, i64, Vec<u8>)> = {
					let mut statement = transaction.prepare(
						"SELECT m.id, m.scope, m.cwd, m.session_id, e.dim, e.embedding \
						 FROM memories m JOIN memory_embeddings e ON e.memory_id = m.id \
						 WHERE e.model = ?1 AND m.consolidated_into IS NULL \
						 ORDER BY m.id",
					)?;
					let rows = statement.query_map(params![model], |row| {
						Ok((
							row.get::<_, String>(0)?,
							row.get::<_, String>(1)?,
							row.get::<_, Option<String>>(2)?,
							row.get::<_, Option<String>>(3)?,
							row.get::<_, i64>(4)?,
							row.get::<_, Vec<u8>>(5)?,
						))
					})?;
					let mut rows_out = Vec::new();
					for row in rows {
						rows_out.push(row?);
					}
					rows_out
				};
				let mut groups: BTreeMap<
					(String, Option<String>, Option<String>, usize),
					Vec<(String, Vec<f32>)>,
				> = BTreeMap::new();
				for (id, scope, cwd, session_id, dim, blob) in rows {
					let Ok(dim) = usize::try_from(dim) else {
						continue;
					};
					let Some(vector) = decode_embedding(&blob, dim as i64) else {
						continue;
					};
					groups
						.entry((scope, cwd, session_id, dim))
						.or_default()
						.push((id, vector));
				}
				for group in groups.values() {
					if group.len() < 2 {
						continue;
					}
					let dim = group[0].1.len();
					let mut flattened = Vec::with_capacity(group.len() * dim);
					for (_, vector) in group {
						flattened.extend(vector.iter().map(|&value| f64::from(value)));
					}
					let pairs = cosine_similarity_pairs(
						&flattened,
						group.len(),
						dim,
						CONSOLIDATION_SIMILARITY_THRESHOLD,
					)
					.map_err(|message| MemoryStoreError::Validation(message.to_string()))?;
					if pairs.is_empty() {
						continue;
					}
					let mut parent: Vec<usize> = (0..group.len()).collect();
					for pair in pairs.chunks_exact(2) {
						union_indices(&mut parent, pair[0] as usize, pair[1] as usize);
					}
					let mut components: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
					for index in 0..group.len() {
						let root = find_index(&mut parent, index);
						components.entry(root).or_default().push(index);
					}
					for mut members in components.into_values() {
						if members.len() < 2 {
							continue;
						}
						members.sort_unstable();
						report.clusters += 1;
						let canonical = &group[members[0]].0;
						for index in &members[1..] {
							let id = &group[*index].0;
							let changed = transaction.execute(
								"UPDATE memories SET consolidated_into = ?1 \
								 WHERE id = ?2 AND consolidated_into IS NULL AND id != ?1",
								params![canonical, id],
							)?;
							report.consolidated += changed;
						}
					}
				}
			}
			transaction.commit()?;
			Ok(report)
		})?;
		// The database is the authority, but the derived project artifact must
		// not keep showing lessons that TTL expired. Dirty markers survive a
		// transient write failure, so the next consolidation retries them.
		if let Some(agent_dir) = self.path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
			let dirty = {
				let connection = self.lock();
				let mut statement = connection.prepare(
					"SELECT cwd, dirty_gen FROM memory_export_dirty WHERE dirty != 0 ORDER BY cwd",
				)?;
				let rows = statement.query_map([], |row| {
					Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
				})?;
				let mut entries = Vec::new();
				for row in rows {
					entries.push(row?);
				}
				entries
			};
			let mut first_error = None;
			for (cwd, generation) in dirty {
				match self.export_learned_lessons(agent_dir, Path::new(cwd.as_str())) {
					Ok(()) => {
						// A newer consolidation may have incremented the generation while
						// this export ran; only retire the exact generation we exported.
						let connection = self.lock();
						connection.execute(
							"UPDATE memory_export_dirty SET dirty = 0 \
							 WHERE cwd = ?1 AND dirty_gen = ?2",
							params![cwd.as_str(), generation],
						)?;
					},
					Err(error) => {
						if first_error.is_none() {
							first_error = Some(error);
						}
					},
				}
			}
			if let Some(error) = first_error {
				return Err(error);
			}
		}
		Ok(report)
	}

	/// Migrate bullets from the project's legacy `learned.md`, exactly once.
	///
	/// The marker row is inserted in the same transaction as the imported rows
	/// (also when the file is missing or has no bullets), so a later export,
	/// list, learn, or delete can never re-import a lesson the user deleted.
	/// The file is only read; it is never rewritten here.
	///
	/// A read failure leaves the marker unset (retry is possible). A partial
	/// insert, or an import that reports lessons while the store stays empty,
	/// is an explicit error — never a silent success.
	pub fn import_learned_lessons(
		&self,
		agent_dir: &Path,
		cwd: &Path,
	) -> MemoryResult<ImportReport> {
		// Serialized per database path: in-process peers must not both read the
		// artifact and race the marker insert. A second process is still ordered
		// only by the marker re-check inside the write transaction.
		crate::local_memory::with_path_lock(&self.path, || {
			self.import_learned_lessons_locked(agent_dir, cwd)
		})
	}

	/// Caller holds the store's per-path lock (`import_learned_lessons`).
	fn import_learned_lessons_locked(
		&self,
		agent_dir: &Path,
		cwd: &Path,
	) -> MemoryResult<ImportReport> {
		let cwd_text = project_cwd(cwd)?;
		let path = crate::local_memory::memory_root(agent_dir, cwd).join(LEARNED_LESSONS_FILE);
		let marker = legacy_marker_key(&cwd_text);
		if self.legacy_migrated(&marker)? {
			return Ok(ImportReport {
				path,
				found: 0,
				imported: 0,
				skipped: 0,
				already_imported: true,
			});
		}
		let raw = match fs::read_to_string(&path) {
			Ok(raw) => raw,
			Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
			Err(err) => return Err(MemoryStoreError::Io(err)),
		};
		let lessons = parse_legacy_lessons(&raw);
		let found = lessons.len();
		let mut imported = 0;
		let mut skipped = 0;
		let mut guard = self.lock();
		let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
		// Another process may have finished between the probe and this write
		// lock; re-check before inserting anything.
		if legacy_migrated_in(&transaction, &marker)? {
			transaction.commit()?;
			return Ok(ImportReport {
				path,
				found: 0,
				imported: 0,
				skipped: 0,
				already_imported: true,
			});
		}
		let now = now_seconds();
		for lesson in &lessons {
			let key = record_dedupe_key(
				Scope::Project,
				Some(&cwd_text),
				None,
				&lesson.body,
				lesson.context.as_deref(),
			);
			let existing: Option<String> = transaction
				.query_row("SELECT id FROM memories WHERE dedupe_key = ?1", params![key], |row| {
					row.get(0)
				})
				.optional()?;
			if existing.is_some() {
				skipped += 1;
				continue;
			}
			transaction.execute(
				"INSERT INTO memories \
				 (id, scope, cwd, session_id, body, context, origin, actor, source, created_at, updated_at, dedupe_key) \
				 VALUES (lower(hex(randomblob(16))), 'project', ?1, NULL, ?2, ?3, 'inferred', NULL, ?4, ?5, ?5, ?6)",
				params![cwd_text, lesson.body, lesson.context, "learned.md", now, key],
			)?;
			imported += 1;
		}
		let stored: i64 = transaction.query_row(
			"SELECT COUNT(*) FROM memories WHERE scope = 'project' AND cwd = ?1",
			params![cwd_text],
			|row| row.get(0),
		)?;
		if found > 0 && stored == 0 {
			return Err(MemoryStoreError::Schema(
				"legacy import reported lessons but the memory store is empty".to_string(),
			));
		}
		if imported + skipped != found {
			return Err(MemoryStoreError::Schema(format!(
				"legacy import accounted for {} of {found} lessons",
				imported + skipped
			)));
		}
		transaction.execute(
			"INSERT INTO memory_meta (key, value) VALUES (?1, ?2)",
			params![marker, path.display().to_string()],
		)?;
		transaction.commit()?;
		Ok(ImportReport { path, found, imported, skipped, already_imported: false })
	}

	fn legacy_migrated(&self, marker: &str) -> MemoryResult<bool> {
		let connection = self.lock();
		legacy_migrated_in(&connection, marker)
	}

	/// Project rows newest-first, bounded to the legacy export cap.
	pub fn project_lessons(&self, cwd: &Path) -> MemoryResult<Vec<MemoryRecord>> {
		self.list(&Filter::project(&project_cwd(cwd)?).with_limit(MAX_LEARNED_LESSONS))
	}

	/// Rewrite the project's `learned.md` bullet block from the store.
	///
	/// Export never re-reads the file's bullets: rows are the authority, so a
	/// deleted record cannot come back. Non-bullet prose and headings survive;
	/// the file stays a bounded derived artifact and the database keeps the
	/// full history. The per-path lock covers the database read, so two
	/// concurrent exports cannot write stale row lists.
	pub fn export_learned_lessons(&self, agent_dir: &Path, cwd: &Path) -> MemoryResult<()> {
		let path = crate::local_memory::memory_root(agent_dir, cwd).join(LEARNED_LESSONS_FILE);
		crate::local_memory::with_lesson_lock(&path, || {
			let records = self.project_lessons(cwd)?;
			let lessons: Vec<String> = records.iter().map(render_learned_line).collect();
			crate::local_memory::write_learned_lessons_locked(&path, &lessons)
		})
		.map_err(|error| MemoryStoreError::Export(error.to_string()))
	}
}

/// Update one record and keep the legacy project export in sync. User-facing
/// edit path.
pub fn update_record_and_export(
	agent_dir: &Path,
	cwd: &Path,
	id: &str,
	body: &str,
) -> MemoryResult<MemoryRecord> {
	update_record_and_export_with_session(agent_dir, cwd, None, id, body)
}

/// Like [`update_record_and_export`], also admitting session-scoped rows.
/// Session/global rows never touch `learned.md`; only project rows export.
pub fn update_record_and_export_with_session(
	agent_dir: &Path,
	cwd: &Path,
	session_id: Option<&str>,
	id: &str,
	body: &str,
) -> MemoryResult<MemoryRecord> {
	let store = MemoryStore::open_existing(agent_dir)?;
	let cwd_text = project_cwd(cwd)?;
	// One-time legacy migration (marker-guarded), so ids from a pre-store file
	// stay addressable. It never runs again after the first migration.
	store.import_learned_lessons(agent_dir, cwd)?;
	let record = store.update_with_session(id, Some(&cwd_text), session_id, body)?;
	if record.scope == Scope::Project {
		store.export_learned_lessons(agent_dir, cwd)?;
	}
	Ok(record)
}

/// Delete one record and keep the legacy project export in sync. User-facing
/// delete path. Conversation history in `sessions.db` is untouched by design.
pub fn delete_record_and_export(agent_dir: &Path, cwd: &Path, id: &str) -> MemoryResult<()> {
	delete_record_and_export_with_session(agent_dir, cwd, None, id)
}

/// Like [`delete_record_and_export`], also admitting session-scoped rows.
pub fn delete_record_and_export_with_session(
	agent_dir: &Path,
	cwd: &Path,
	session_id: Option<&str>,
	id: &str,
) -> MemoryResult<()> {
	let store = MemoryStore::open_existing(agent_dir)?;
	let cwd_text = project_cwd(cwd)?;
	// Import before deleting so a legacy row gets a real id; the marker then
	// prevents the still-present bullet from being resurrected by any export.
	store.import_learned_lessons(agent_dir, cwd)?;
	let record = store.get_with_session(id, Some(&cwd_text), session_id)?;
	store.delete_with_session(id, Some(&cwd_text), session_id)?;
	if record.scope == Scope::Project {
		store.export_learned_lessons(agent_dir, cwd)?;
	}
	Ok(())
}

/// Create one explicit user assertion (`origin=asserted`, `actor=user`) and
/// refresh the project's derived `learned.md`. Global/session rows have no file
/// and are injected by [`crate::local_memory::build_memory_instructions`]
/// (global) or read through a session-aware filter instead.
pub fn create_asserted_and_export(
	agent_dir: &Path,
	cwd: &Path,
	scope: Scope,
	body: &str,
	context: Option<&str>,
) -> MemoryResult<MemoryRecord> {
	create_asserted_with_session_and_export(agent_dir, cwd, None, scope, body, context)
}

/// Like [`create_asserted_and_export`]; `session_id` is required for
/// [`Scope::Session`] and must be absent otherwise.
pub fn create_asserted_with_session_and_export(
	agent_dir: &Path,
	cwd: &Path,
	session_id: Option<&str>,
	scope: Scope,
	body: &str,
	context: Option<&str>,
) -> MemoryResult<MemoryRecord> {
	let store = MemoryStore::open(agent_dir)?;
	let cwd_text = match scope {
		Scope::Global | Scope::Session => None,
		Scope::Project => Some(project_cwd(cwd)?),
	};
	if scope == Scope::Project {
		// One-time legacy migration before the first user write, so ids from a
		// pre-store file stay addressable and the export below is complete.
		store.import_learned_lessons(agent_dir, cwd)?;
	}
	let record = store.add(&AddInput {
		scope,
		cwd: cwd_text.as_deref(),
		session_id,
		body,
		context,
		origin: Origin::Asserted,
		actor: Some("user"),
		source: Some("user"),
		dedupe: false,
	})?;
	if scope == Scope::Project {
		store.export_learned_lessons(agent_dir, cwd)?;
	}
	Ok(record)
}

/// Read one visible record through the same migration path as a view.
pub fn get_visible(agent_dir: &Path, cwd: &Path, id: &str) -> MemoryResult<MemoryRecord> {
	get_visible_with_session(agent_dir, cwd, None, id)
}

/// Like [`get_visible`], also admitting this conversation's `session` rows.
pub fn get_visible_with_session(
	agent_dir: &Path,
	cwd: &Path,
	session_id: Option<&str>,
	id: &str,
) -> MemoryResult<MemoryRecord> {
	let cwd_text = project_cwd(cwd)?;
	let store = open_for_view(agent_dir, cwd)?;
	store.get_with_session(id, Some(&cwd_text), session_id)
}

/// All records a session for `cwd` may see: global plus this project.
///
/// A project whose only memory is a legacy `learned.md` is migrated first, so
/// an empty database is never reported as an empty memory. When neither a
/// store nor a legacy artifact exists the error is explicit.
pub fn list_visible(agent_dir: &Path, cwd: &Path) -> MemoryResult<Vec<MemoryRecord>> {
	list_visible_with_session(agent_dir, cwd, None)
}

/// Like [`list_visible`], also including this conversation's `session` rows.
pub fn list_visible_with_session(
	agent_dir: &Path,
	cwd: &Path,
	session_id: Option<&str>,
) -> MemoryResult<Vec<MemoryRecord>> {
	let cwd_text = project_cwd(cwd)?;
	let store = open_for_view(agent_dir, cwd)?;
	let filter = match clean_session_id(session_id) {
		Some(session_id) => Filter::all(Some(&cwd_text)).with_session(&session_id),
		None => Filter::all(Some(&cwd_text)),
	};
	store.list(&filter)
}

/// Scope-selected list for the user command. `None` is the normal session view
/// (global plus this project), `Global` ignores `cwd`, and `Project` is this
/// project only. A global-only view of a missing store is empty, not an error.
pub fn list_scope(
	agent_dir: &Path,
	cwd: &Path,
	scope: Option<Scope>,
) -> MemoryResult<Vec<MemoryRecord>> {
	list_scope_with_session(agent_dir, cwd, scope, None)
}

/// Like [`list_scope`], with an optional conversation's rows included/selected.
pub fn list_scope_with_session(
	agent_dir: &Path,
	cwd: &Path,
	scope: Option<Scope>,
	session_id: Option<&str>,
) -> MemoryResult<Vec<MemoryRecord>> {
	match scope {
		None => list_visible_with_session(agent_dir, cwd, session_id),
		Some(Scope::Global) => list_global(agent_dir),
		Some(Scope::Project) => {
			let store = open_for_view(agent_dir, cwd)?;
			store.list(&Filter::project(&project_cwd(cwd)?))
		},
		Some(Scope::Session) => {
			let session_id = clean_session_id(session_id).ok_or_else(|| {
				MemoryStoreError::Validation(
					"session scope requires a non-empty session id".to_string(),
				)
			})?;
			let store = open_for_view(agent_dir, cwd)?;
			store.list(&Filter::session(&session_id))
		},
	}
}

/// Global-only rows; absence of a store is an empty list.
pub fn list_global(agent_dir: &Path) -> MemoryResult<Vec<MemoryRecord>> {
	if !MemoryStore::database_path(agent_dir).is_file() {
		return Ok(Vec::new());
	}
	MemoryStore::open_existing(agent_dir)?.list(&Filter::global())
}

/// Scope-selected recall, same rules as [`list_scope`].
pub fn recall_scope(
	agent_dir: &Path,
	cwd: &Path,
	query: &str,
	scope: Option<Scope>,
	limit: Option<usize>,
) -> MemoryResult<Vec<MemoryRecord>> {
	recall_scope_with_session(agent_dir, cwd, query, scope, None, limit)
}

/// Like [`recall_scope`], with an optional conversation's rows included.
pub fn recall_scope_with_session(
	agent_dir: &Path,
	cwd: &Path,
	query: &str,
	scope: Option<Scope>,
	session_id: Option<&str>,
	limit: Option<usize>,
) -> MemoryResult<Vec<MemoryRecord>> {
	recall_scope_with_session_and_embedding(
		agent_dir,
		cwd,
		query,
		scope,
		session_id,
		None,
		limit,
	)
}

/// [`recall_scope_with_session`] with an optional query embedding for hybrid
/// BM25 + cosine + MMR recall. Any absent/failed provider stays lexical.
pub fn recall_scope_with_session_and_embedding(
	agent_dir: &Path,
	cwd: &Path,
	query: &str,
	scope: Option<Scope>,
	session_id: Option<&str>,
	embedding: Option<(&str, &[f32])>,
	limit: Option<usize>,
) -> MemoryResult<Vec<MemoryRecord>> {
	match scope {
		None => recall_visible_with_session_and_embedding(
			agent_dir,
			cwd,
			query,
			session_id,
			embedding,
			limit,
		),
		Some(Scope::Global) => {
			if !MemoryStore::database_path(agent_dir).is_file() {
				return Ok(Vec::new());
			}
			let store = MemoryStore::open_existing(agent_dir)?;
			store.recall_with_scope(
				query,
				None,
				None,
				limit,
				ScopeFilter::Global,
				embedding,
			)
		},
		Some(Scope::Project) => {
			let store = open_for_view(agent_dir, cwd)?;
			let cwd_text = project_cwd(cwd)?;
			// Project recall is project-only: a default session id must not
			// widen it to global or conversation rows.
			store.recall_with_scope(
				query,
				Some(&cwd_text),
				None,
				limit,
				ScopeFilter::Project,
				embedding,
			)
		},
		Some(Scope::Session) => {
			let session_id = clean_session_id(session_id).ok_or_else(|| {
				MemoryStoreError::Validation(
					"session scope requires a non-empty session id".to_string(),
				)
			})?;
			let store = open_for_view(agent_dir, cwd)?;
			store.recall_with_scope(
				query,
				None,
				Some(&session_id),
				limit,
				ScopeFilter::Session,
				embedding,
			)
		},
	}
}

/// Bounded recall for a session at `cwd`, with the same legacy import rule as
/// [`list_visible`].
pub fn recall_visible(
	agent_dir: &Path,
	cwd: &Path,
	query: &str,
	limit: Option<usize>,
) -> MemoryResult<Vec<MemoryRecord>> {
	recall_visible_with_session(agent_dir, cwd, query, None, limit)
}

/// Like [`recall_visible`], also including this conversation's `session` rows.
pub fn recall_visible_with_session(
	agent_dir: &Path,
	cwd: &Path,
	query: &str,
	session_id: Option<&str>,
	limit: Option<usize>,
) -> MemoryResult<Vec<MemoryRecord>> {
	recall_visible_with_session_and_embedding(
		agent_dir,
		cwd,
		query,
		session_id,
		None,
		limit,
	)
}

/// [`recall_visible_with_session`] with an optional query embedding.
pub fn recall_visible_with_session_and_embedding(
	agent_dir: &Path,
	cwd: &Path,
	query: &str,
	session_id: Option<&str>,
	embedding: Option<(&str, &[f32])>,
	limit: Option<usize>,
) -> MemoryResult<Vec<MemoryRecord>> {
	let cwd_text = project_cwd(cwd)?;
	let store = open_for_view(agent_dir, cwd)?;
	store.recall_hybrid(query, Some(&cwd_text), session_id, embedding, limit)
}

/// Open the project's store for reading. A project whose only memory is a
/// legacy `learned.md` is migrated first; imports are idempotent, so a view
/// always reconciles the artifact. When neither a store nor a legacy artifact
/// exists, say so instead of returning a misleading empty result.
fn open_for_view(agent_dir: &Path, cwd: &Path) -> MemoryResult<MemoryStore> {
	let path = MemoryStore::database_path(agent_dir);
	let legacy = crate::local_memory::memory_root(agent_dir, cwd).join(LEARNED_LESSONS_FILE);
	if !path.is_file() && !legacy.is_file() {
		return Err(MemoryStoreError::NotFound(format!(
			"no memory records for this project yet ({} is absent)",
			path.display()
		)));
	}
	let store = MemoryStore::open_at(path)?;
	store.import_learned_lessons(agent_dir, cwd)?;
	Ok(store)
}

/// Render one store record as a `learned.md` bullet.
#[must_use]
pub fn render_learned_line(record: &MemoryRecord) -> String {
	match record
		.context
		.as_deref()
		.filter(|context| !context.is_empty())
	{
		Some(context) => format!("- {} _(context: {context})_", record.body),
		None => format!("- {}", record.body),
	}
}

impl MemoryStore {
	fn lock(&self) -> MutexGuard<'_, Connection> {
		self
			.connection
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}
}

fn initialize(connection: &mut Connection) -> MemoryResult<()> {
	let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
	let version: i64 = transaction.query_row("PRAGMA user_version", [], |row| row.get(0))?;
	match version {
		0 => {
			let existing: i64 = transaction.query_row(
				"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' \
				 AND name IN ('memories', 'memory_meta', 'memories_fts', 'memory_embeddings')",
				[],
				|row| row.get(0),
			)?;
			if existing != 0 {
				return Err(MemoryStoreError::Schema(
					"memory tables exist without a schema version; refusing to recreate them"
						.to_string(),
				));
			}
			transaction.execute_batch(MEMORIES_V2_TABLE_DDL)?;
			transaction.execute_batch(MEMORIES_V2_AUX_DDL)?;
			ensure_export_dirty_gen(&transaction)?;
			transaction.execute_batch(MEMORIES_FTS_DDL)?;
			transaction.execute_batch(MEMORY_EMBEDDINGS_DDL)?;
			transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
		},
		1 => migrate_v1_to_v2(&transaction)?,
		SCHEMA_VERSION => {
			let memories: i64 = transaction.query_row(
				"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'memories'",
				[],
				|row| row.get(0),
			)?;
			if memories == 0 {
				return Err(MemoryStoreError::Schema(
					"database records the memory schema version but has no memories table"
						.to_string(),
				));
			}
			repair_v2_aux(&transaction)?;
		},
		version if version > SCHEMA_VERSION => {
			return Err(MemoryStoreError::Schema(format!(
				"database version {version} is newer than supported {SCHEMA_VERSION}"
			)));
		},
		version => {
			return Err(MemoryStoreError::Schema(format!(
				"database version {version} is older than supported {SCHEMA_VERSION}"
			)));
		},
	}
	transaction.commit()?;
	Ok(())
}

/// Transactional v1 -> v2 upgrade. Rows keep id, scope, cwd, text, origin,
/// actor/source, timestamps and dedupe keys; new metadata columns take their
/// defaults. The FTS index is rebuilt after the copy so pre-migration rows are
/// searchable immediately. Runs inside the caller's `Immediate` transaction,
/// so a failure leaves the v1 database untouched.
fn migrate_v1_to_v2(transaction: &Connection) -> MemoryResult<()> {
	let memories: i64 = transaction.query_row(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'memories'",
		[],
		|row| row.get(0),
	)?;
	if memories == 0 {
		return Err(MemoryStoreError::Schema(
			"database records schema v1 but has no memories table".to_string(),
		));
	}
	transaction.execute_batch("ALTER TABLE memories RENAME TO memories_v1;")?;
	transaction.execute_batch(MEMORIES_V2_TABLE_DDL)?;
	transaction.execute(
		"INSERT INTO memories \
		 (id, scope, cwd, session_id, body, context, origin, actor, source, \
		  created_at, updated_at, dedupe_key) \
		 SELECT id, scope, cwd, NULL, body, context, origin, actor, source, \
		        created_at, updated_at, dedupe_key \
		 FROM memories_v1",
		[],
	)?;
	transaction.execute_batch("DROP TABLE memories_v1;")?;
	transaction.execute_batch(MEMORIES_V2_AUX_DDL)?;
	ensure_export_dirty_gen(transaction)?;
	transaction.execute_batch(MEMORIES_FTS_DDL)?;
	transaction.execute_batch(MEMORY_EMBEDDINGS_DDL)?;
	transaction.execute_batch("INSERT INTO memories_fts (memories_fts) VALUES ('rebuild');")?;
	transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
	Ok(())
}

/// Recreate additive v2 indexes/tables/triggers if a hand-edited database
/// missing them still claims v2. The FTS index is rebuilt only when the
/// virtual table was actually absent.
fn repair_v2_aux(transaction: &Connection) -> MemoryResult<()> {
	let fts_existed: i64 = transaction.query_row(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'memories_fts'",
		[],
		|row| row.get(0),
	)?;
	transaction.execute_batch(MEMORIES_V2_AUX_DDL)?;
	ensure_export_dirty_gen(transaction)?;
	transaction.execute_batch(MEMORIES_FTS_DDL)?;
	transaction.execute_batch(MEMORY_EMBEDDINGS_DDL)?;
	if fts_existed == 0 {
		transaction.execute_batch("INSERT INTO memories_fts (memories_fts) VALUES ('rebuild');")?;
	}
	Ok(())
}

/// Upgrade a pre-generation `memory_export_dirty` table in place. The table is
/// additive schema; `CREATE TABLE IF NOT EXISTS` cannot add columns to an
/// existing v2 database, so check `PRAGMA table_info` once per open.
fn ensure_export_dirty_gen(connection: &Connection) -> MemoryResult<()> {
	let columns = {
		let mut statement = connection.prepare("PRAGMA table_info(memory_export_dirty)")?;
		let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
		let mut columns = Vec::new();
		for row in rows {
			columns.push(row?);
		}
		columns
	};
	if !columns.iter().any(|name| name.as_str() == "dirty_gen") {
		connection.execute_batch(
			"ALTER TABLE memory_export_dirty ADD COLUMN dirty_gen INTEGER NOT NULL DEFAULT 1",
		)?;
	}
	if !columns.iter().any(|name| name.as_str() == "dirty") {
		connection.execute_batch(
			"ALTER TABLE memory_export_dirty ADD COLUMN dirty INTEGER NOT NULL DEFAULT 1",
		)?;
	}
	Ok(())
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<MemoryRecord> {
	let id: String = row.get(0)?;
	let scope_raw: String = row.get(1)?;
	let scope = Scope::parse(&scope_raw).ok_or_else(|| unusable_column(1, "scope"))?;
	let cwd: Option<String> = row.get(2)?;
	let session_id: Option<String> = row.get(3)?;
	// Write path sanitizes; read repeats it so a hand-edited or corrupted
	// database row still cannot inject prompt delimiters or leak a secret.
	let raw_body: String = row.get(4)?;
	let body = normalize_learned_text(&raw_body, MAX_LEARNED_CONTENT_CHARS);
	let raw_context: Option<String> = row.get(5)?;
	let context = raw_context
		.map(|context| normalize_learned_text(&context, MAX_LEARNED_CONTEXT_CHARS))
		.filter(|context| !context.is_empty());
	let origin_raw: String = row.get(6)?;
	let origin = Origin::parse(&origin_raw).ok_or_else(|| unusable_column(6, "origin"))?;
	let actor: Option<String> = row.get(7)?;
	let source: Option<String> = row.get(8)?;
	let created_at: i64 = row.get(9)?;
	let updated_at: i64 = row.get(10)?;
	let access_count: i64 = row.get(11)?;
	let last_accessed_at: Option<i64> = row.get(12)?;
	let confidence: f64 = row.get(13)?;
	let ttl_secs: Option<i64> = row.get(14)?;
	let consolidated_into: Option<String> = row.get(15)?;
	Ok(MemoryRecord {
		id,
		scope,
		cwd,
		session_id,
		body,
		context,
		origin,
		actor,
		source,
		created_at,
		updated_at,
		access_count: u64::try_from(access_count).unwrap_or(0),
		last_accessed_at,
		confidence,
		ttl_secs,
		consolidated_into,
	})
}

fn unusable_column(index: usize, name: &str) -> rusqlite::Error {
	rusqlite::Error::InvalidColumnType(index, name.to_string(), rusqlite::types::Type::Text)
}

fn load_record(connection: &Connection, id: &str) -> MemoryResult<Option<MemoryRecord>> {
	let sql = format!("SELECT {RECORD_COLUMNS} FROM memories WHERE id = ?1");
	Ok(connection
		.query_row(&sql, params![id], row_to_record)
		.optional()?)
}

fn normalize_scope(scope: Scope, cwd: Option<&str>) -> MemoryResult<Option<String>> {
	match scope {
		Scope::Global | Scope::Session => {
			if cwd.is_some_and(|value| !value.trim().is_empty()) {
				return Err(MemoryStoreError::Validation(format!(
					"{} memory must not carry a project cwd",
					scope.as_str()
				)));
			}
			Ok(None)
		},
		Scope::Project => {
			let cwd = cwd
				.map(str::trim)
				.filter(|value| !value.is_empty())
				.ok_or_else(|| {
					MemoryStoreError::Validation("project memory requires a non-empty cwd".to_string())
				})?;
			Ok(Some(cwd.to_string()))
		},
	}
}

fn normalize_session(scope: Scope, session_id: Option<&str>) -> MemoryResult<Option<String>> {
	match scope {
		Scope::Session => {
			let session_id = session_id
				.map(str::trim)
				.filter(|value| !value.is_empty())
				.ok_or_else(|| {
					MemoryStoreError::Validation(
						"session memory requires a non-empty session_id".to_string(),
					)
				})?;
			Ok(Some(session_id.to_string()))
		},
		Scope::Global | Scope::Project => {
			if session_id.is_some_and(|value| !value.trim().is_empty()) {
				return Err(MemoryStoreError::Validation(format!(
					"{} memory must not carry a session_id",
					scope.as_str()
				)));
			}
			Ok(None)
		},
	}
}

fn ensure_visible(
	record: &MemoryRecord,
	cwd: Option<&str>,
	session_id: Option<&str>,
) -> MemoryResult<()> {
	match record.scope {
		Scope::Global => Ok(()),
		Scope::Project => {
			let cwd = cwd.map(str::trim).filter(|value| !value.is_empty());
			if cwd.is_some() && cwd == record.cwd.as_deref() {
				Ok(())
			} else {
				Err(MemoryStoreError::NotFound(format!(
					"memory record not found: {}",
					record.id
				)))
			}
		},
		Scope::Session => {
			let session_id = session_id.map(str::trim).filter(|value| !value.is_empty());
			if session_id.is_some() && session_id == record.session_id.as_deref() {
				Ok(())
			} else {
				Err(MemoryStoreError::NotFound(format!(
					"memory record not found: {}",
					record.id
				)))
			}
		},
	}
}

/// Trimmed non-empty session id; embeddings/session filters ignore blanks.
fn clean_session_id(session_id: Option<&str>) -> Option<String> {
	session_id
		.map(str::trim)
		.filter(|value| !value.is_empty())
		.map(str::to_string)
}

/// Record ids are `lower(hex(randomblob(16)))`; case-insensitive on input.
#[must_use]
pub fn is_record_id(raw: &str) -> bool {
	let raw = raw.trim();
	raw.len() == RECORD_ID_LEN && raw.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn normalize_record_id(id: &str) -> MemoryResult<String> {
	let id = id.trim().to_ascii_lowercase();
	if !is_record_id(&id) {
		return Err(MemoryStoreError::NotFound(format!("memory record not found: {id}")));
	}
	Ok(id)
}

/// `ScopeFilter` -> SQL predicate plus positional parameters.
fn filter_clause(filter: &Filter) -> MemoryResult<(String, Vec<Value>)> {
	let cwd = filter
		.cwd
		.as_deref()
		.map(str::trim)
		.filter(|value| !value.is_empty());
	let session_id = filter
		.session_id
		.as_deref()
		.map(str::trim)
		.filter(|value| !value.is_empty());
	match filter.scope {
		ScopeFilter::All => {
			if cwd.is_none() && session_id.is_none() {
				// Unscoped reads are global/project only. Session rows require an
				// explicit conversation id; otherwise every session would leak.
				return Ok(("scope != 'session'".to_string(), Vec::new()));
			}
			let mut parts = vec!["scope = 'global'".to_string()];
			let mut values = Vec::new();
			if let Some(cwd) = cwd {
				parts.push("(scope = 'project' AND cwd = ?)".to_string());
				values.push(Value::Text(cwd.to_string()));
			}
			if let Some(session_id) = session_id {
				parts.push("(scope = 'session' AND session_id = ?)".to_string());
				values.push(Value::Text(session_id.to_string()));
			}
			Ok((format!("({})", parts.join(" OR ")), values))
		},
		ScopeFilter::Global => Ok(("scope = 'global'".to_string(), Vec::new())),
		ScopeFilter::Project => {
			let cwd = cwd.ok_or_else(|| {
				MemoryStoreError::Validation("project filter requires a non-empty cwd".to_string())
			})?;
			Ok(("scope = 'project' AND cwd = ?".to_string(), vec![Value::Text(cwd.to_string())]))
		},
		ScopeFilter::Session => {
			let session_id = session_id.ok_or_else(|| {
				MemoryStoreError::Validation(
					"session filter requires a non-empty session_id".to_string(),
				)
			})?;
			Ok((
				"scope = 'session' AND session_id = ?".to_string(),
				vec![Value::Text(session_id.to_string())],
			))
		},
	}
}

fn query_tokens(query: &str) -> Vec<String> {
	let mut tokens: Vec<String> = Vec::new();
	for token in query.split(|ch: char| !ch.is_alphanumeric()) {
		if token.is_empty() {
			continue;
		}
		let token = token.to_lowercase();
		if !tokens.contains(&token) {
			tokens.push(token);
		}
		if tokens.len() == 16 {
			break;
		}
	}
	tokens
}

/// All tokens are alphanumeric (see [`query_tokens`]), so quoting plus a
/// prefix wildcard is safe and gives FTS5 `unicode61` prefix matching.
fn fts_query(tokens: &[String]) -> String {
	tokens
		.iter()
		.map(|token| format!("\"{token}\"*"))
		.collect::<Vec<_>>()
		.join(" OR ")
}

fn record_dedupe_key(
	scope: Scope,
	cwd: Option<&str>,
	session_id: Option<&str>,
	body: &str,
	context: Option<&str>,
) -> String {
	let mut hasher = Sha256::new();
	hasher.update(scope.as_str().as_bytes());
	hasher.update([0x1f]);
	// The slot after the scope held the project cwd in v1; session rows use it
	// for the conversation id, so existing v1 dedupe keys stay valid.
	if scope == Scope::Session {
		hasher.update(session_id.unwrap_or_default().as_bytes());
	} else {
		hasher.update(cwd.unwrap_or_default().as_bytes());
	}
	hasher.update([0x1f]);
	hasher.update(body.as_bytes());
	hasher.update([0x1f]);
	hasher.update(context.unwrap_or_default().as_bytes());
	hex(&hasher.finalize())
}

fn encode_embedding(vector: &[f32]) -> Vec<u8> {
	let mut bytes = Vec::with_capacity(vector.len() * 4);
	for value in vector {
		bytes.extend_from_slice(&value.to_le_bytes());
	}
	bytes
}

fn decode_embedding(blob: &[u8], dim: i64) -> Option<Vec<f32>> {
	let dim = usize::try_from(dim).ok()?;
	if dim == 0 || blob.len() != dim.checked_mul(4)? {
		return None;
	}
	let mut vector = Vec::with_capacity(dim);
	for chunk in blob.chunks_exact(4) {
		let bytes: [u8; 4] = chunk.try_into().ok()?;
		vector.push(f32::from_le_bytes(bytes));
	}
	Some(vector)
}

fn load_embeddings_for_model(
	connection: &Connection,
	model: &str,
) -> MemoryResult<BTreeMap<String, Vec<f32>>> {
	let mut statement = connection.prepare(
		"SELECT memory_id, dim, embedding FROM memory_embeddings WHERE model = ?1",
	)?;
	let rows = statement.query_map(params![model], |row| {
		Ok((
			row.get::<_, String>(0)?,
			row.get::<_, i64>(1)?,
			row.get::<_, Vec<u8>>(2)?,
		))
	})?;
	let mut embeddings = BTreeMap::new();
	for row in rows {
		let (id, dim, blob) = row?;
		if let Some(vector) = decode_embedding(&blob, dim) {
			embeddings.insert(id, vector);
		}
	}
	Ok(embeddings)
}

fn find_index(parent: &mut [usize], mut index: usize) -> usize {
	while parent[index] != index {
		let grandparent = parent[parent[index]];
		parent[index] = grandparent;
		index = grandparent;
	}
	index
}

fn union_indices(parent: &mut [usize], left: usize, right: usize) {
	let left_root = find_index(parent, left);
	let right_root = find_index(parent, right);
	if left_root != right_root {
		parent[right_root] = left_root;
	}
}

fn hex(bytes: &[u8]) -> String {
	const DIGITS: &[u8; 16] = b"0123456789abcdef";
	let mut out = String::with_capacity(bytes.len() * 2);
	for &byte in bytes {
		out.push(DIGITS[usize::from(byte >> 4)] as char);
		out.push(DIGITS[usize::from(byte & 0x0f)] as char);
	}
	out
}

fn next_timestamp(connection: &Connection) -> rusqlite::Result<i64> {
	let max: i64 =
		connection
			.query_row("SELECT COALESCE(MAX(updated_at), 0) FROM memories", [], |row| row.get(0))?;
	Ok(now_seconds().max(max.saturating_add(1)))
}

fn now_seconds() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_secs() as i64)
}

fn clean_optional(value: Option<&str>) -> Option<String> {
	value
		.map(str::trim)
		.filter(|value| !value.is_empty())
		.map(str::to_string)
}

fn project_cwd(cwd: &Path) -> MemoryResult<String> {
	let text = cwd.to_string_lossy().trim().to_string();
	if text.is_empty() {
		return Err(MemoryStoreError::Validation(
			"project memory requires a non-empty cwd".to_string(),
		));
	}
	Ok(text)
}

/// Migration marker key. One legacy artifact per project scope.
fn legacy_marker_key(cwd: &str) -> String {
	format!("legacy_import:project:{cwd}")
}

fn legacy_migrated_in(connection: &Connection, marker: &str) -> MemoryResult<bool> {
	let value: Option<String> = connection
		.query_row("SELECT value FROM memory_meta WHERE key = ?1", params![marker], |row| {
			row.get(0)
		})
		.optional()?;
	Ok(value.is_some())
}

struct LegacyLesson {
	body: String,
	context: Option<String>,
}

fn parse_legacy_lessons(raw: &str) -> Vec<LegacyLesson> {
	let mut lessons = Vec::new();
	for line in raw.lines() {
		let Some(rest) = line.trim().strip_prefix("- ") else {
			continue;
		};
		let rest = rest.trim();
		if rest.is_empty() {
			continue;
		}
		let (content, context) = split_legacy_context(rest);
		let body = normalize_learned_text(content, MAX_LEARNED_CONTENT_CHARS);
		if body.is_empty() {
			continue;
		}
		let context = context
			.map(|context| normalize_learned_text(context, MAX_LEARNED_CONTEXT_CHARS))
			.filter(|context| !context.is_empty());
		lessons.push(LegacyLesson { body, context });
	}
	lessons
}

/// Split `content _(context: context)_` exactly as `learned.md` renders it.
fn split_legacy_context(line: &str) -> (&str, Option<&str>) {
	const MARKER: &str = " _(context: ";
	if let Some(open) = line.rfind(MARKER)
		&& line.ends_with(")_")
	{
		let start = open + MARKER.len();
		let end = line.len() - 2;
		if start <= end {
			return (line[..open].trim_end(), Some(line[start..end].trim()));
		}
	}
	(line, None)
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicU32, Ordering};

	use super::*;

	static COUNTER: AtomicU32 = AtomicU32::new(0);

	fn temp_dir() -> PathBuf {
		let unique = format!(
			"aragent-memory-store-{}-{}",
			std::process::id(),
			COUNTER.fetch_add(1, Ordering::Relaxed)
		);
		let dir = std::env::temp_dir().join(unique);
		fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	fn add_project(store: &MemoryStore, cwd: &str, body: &str) -> MemoryRecord {
		store
			.add(&AddInput::project_lesson(cwd, body, None))
			.expect("add project record")
	}

	#[test]
	fn ids_and_records_survive_reopen() {
		let dir = temp_dir();
		let first = {
			let store = MemoryStore::open(&dir).expect("open");
			let record = add_project(&store, "/work/one", "keep the flaky retry");
			assert_eq!(record.id.len(), RECORD_ID_LEN);
			assert!(record.id.bytes().all(|byte| byte.is_ascii_hexdigit()));
			record.id
		};
		let store = MemoryStore::open(&dir).expect("reopen");
		let record = store
			.get(&first, Some("/work/one"))
			.expect("get after reopen");
		assert_eq!(record.body, "keep the flaky retry");
		assert_eq!(record.scope, Scope::Project);
		assert_eq!(record.origin, Origin::Inferred);
		assert_eq!(record.source.as_deref(), Some("learn"));
		assert!(store.path().ends_with(MEMORY_DB_FILE));
	}

	#[test]
	fn newer_schema_is_refused_and_unversioned_tables_are_not_recreated() {
		let dir = temp_dir();
		let path = MemoryStore::database_path(&dir);
		let store = MemoryStore::open(&dir).expect("open");
		drop(store);
		let connection = Connection::open(&path).expect("raw open");
		connection
			.execute_batch(&format!("PRAGMA user_version = {}", SCHEMA_VERSION + 1))
			.expect("bump version");
		drop(connection);
		let error = MemoryStore::open(&dir).err().expect("newer schema");
		assert!(error.to_string().contains("newer than supported"), "{error}");

		let unversioned = temp_dir();
		let connection =
			Connection::open(MemoryStore::database_path(&unversioned)).expect("raw open");
		connection
			.execute_batch("CREATE TABLE memories (id TEXT PRIMARY KEY)")
			.expect("seed table");
		drop(connection);
		let error = MemoryStore::open(&unversioned).err().expect("unversioned table");
		assert!(error.to_string().contains("refusing to recreate"), "{error}");
	}

	#[test]
	fn scope_isolation_edit_and_delete() {
		let store = MemoryStore::open_in_memory().expect("store");
		let one = add_project(&store, "/work/one", "project one deploy notes");
		let two = add_project(&store, "/work/two", "project two deploy notes");
		let global = store
			.add(&AddInput::asserted(Scope::Global, None, "prefer small diffs"))
			.expect("global");

		let one_view = store
			.list(&Filter::all(Some("/work/one")))
			.expect("list one");
		let ids: Vec<&str> = one_view.iter().map(|record| record.id.as_str()).collect();
		assert!(ids.contains(&one.id.as_str()));
		assert!(ids.contains(&global.id.as_str()));
		assert!(!ids.contains(&two.id.as_str()), "another project must not leak");
		assert_eq!(one_view[0].id, global.id, "newest row first");
		assert!(ids.len() >= 2);

		let two_view = store
			.list(&Filter::all(Some("/work/two")))
			.expect("list two");
		let ids: Vec<&str> = two_view.iter().map(|record| record.id.as_str()).collect();
		assert!(ids.contains(&two.id.as_str()));
		assert!(!ids.contains(&one.id.as_str()));

		assert!(
			store
				.get(&two.id, Some("/work/one"))
				.expect_err("cross-project get")
				.to_string()
				.contains("not found")
		);
		assert!(
			store
				.update(&two.id, Some("/work/one"), "hijack")
				.expect_err("cross-project update")
				.to_string()
				.contains("not found")
		);
		assert!(
			store
				.delete(&two.id, Some("/work/one"))
				.expect_err("cross-project delete")
				.to_string()
				.contains("not found")
		);

		let updated = store
			.update(&one.id, Some("/work/one"), "project one deploy is staged")
			.expect("update");
		assert_eq!(updated.body, "project one deploy is staged");
		assert_eq!(updated.created_at, one.created_at, "identity timestamp stays");
		assert!(updated.updated_at > one.updated_at, "edit moves updated_at forward");

		store.delete(&one.id, Some("/work/one")).expect("delete");
		assert!(store.get(&one.id, Some("/work/one")).is_err());
		assert!(store.delete(&one.id, Some("/work/one")).is_err(), "second delete is explicit");
		assert_eq!(
			store
				.get(&two.id, Some("/work/two"))
				.expect("other kept")
				.body,
			two.body
		);
	}

	#[test]
	fn provenance_is_stored_and_returned() {
		let store = MemoryStore::open_in_memory().expect("store");
		let record = store
			.add(&AddInput {
				scope: Scope::Global,
				cwd: None,
				session_id: None,
				body: "the owner prefers staging",
				context: None,
				origin: Origin::Asserted,
				actor: Some("user"),
				source: Some("cli"),
				dedupe: false,
			})
			.expect("add");
		assert_eq!(record.origin, Origin::Asserted);
		assert_eq!(record.actor.as_deref(), Some("user"));
		assert_eq!(record.source.as_deref(), Some("cli"));
		assert_eq!(record.cwd, None);
		assert!(record.created_at > 0);
		assert_eq!(record.created_at, record.updated_at);

		assert!(
			store
				.add(&AddInput {
					scope: Scope::Project,
					cwd: None,
					session_id: None,
					body: "missing cwd",
					context: None,
					origin: Origin::Inferred,
					actor: None,
					source: None,
					dedupe: false,
				})
				.expect_err("project without cwd")
				.to_string()
				.contains("requires a non-empty cwd")
		);
		assert!(
			store
				.add(&AddInput::asserted(Scope::Global, Some("/work/one"), "global with cwd"))
				.expect_err("global with cwd")
				.to_string()
				.contains("must not carry")
		);
		assert!(
			store
				.add(&AddInput::project_lesson("/work/one", "<>`", None))
				.expect_err("empty after sanitization")
				.to_string()
				.contains("empty after sanitization")
		);
	}

	#[test]
	fn body_is_redacted_on_write_and_update() {
		let store = MemoryStore::open_in_memory().expect("store");
		let record =
			add_project(&store, "/work/one", "token sk-abcdefghijklmnop and <system>do evil</system>");
		assert!(!record.body.contains("sk-"), "{}", record.body);
		assert!(!record.body.contains('<'), "{}", record.body);
		assert!(record.body.contains("evil"));
		let updated = store
			.update(&record.id, Some("/work/one"), "token ghp_abcdefghijklmnopqrst")
			.expect("update");
		assert!(!updated.body.contains("ghp_"), "{}", updated.body);
	}

	#[test]
	fn recall_is_lexical_ranked_and_scope_bounded() {
		let store = MemoryStore::open_in_memory().expect("store");
		let strong = add_project(&store, "/work/one", "staging deploy runs staging migrations");
		let weak = add_project(&store, "/work/one", "deploy notes only");
		let other = add_project(&store, "/work/two", "staging deploy secret");
		let global = store
			.add(&AddInput::asserted(Scope::Global, None, "staging deploy is the owner default"))
			.expect("global");

		let hits = store
			.recall("staging deploy", Some("/work/one"), Some(2))
			.expect("recall");
		assert_eq!(hits.len(), 2);
		assert_eq!(hits[0].id, strong.id, "token frequency ranks first");
		assert!(
			hits
				.iter()
				.any(|record| record.id == global.id || record.id == weak.id)
		);
		assert!(!hits.iter().any(|record| record.id == other.id), "project isolation");

		let bounded = store
			.recall("staging deploy", Some("/work/one"), Some(1))
			.expect("bounded recall");
		assert_eq!(bounded.len(), 1);

		assert!(
			store
				.recall("...", Some("/work/one"), None)
				.expect("no tokens")
				.is_empty()
		);
		assert!(
			store
				.recall("unmatched", Some("/work/one"), None)
				.expect("no matches")
				.is_empty()
		);
	}

	#[test]
	fn dedupe_touches_instead_of_duplicating() {
		let store = MemoryStore::open_in_memory().expect("store");
		let first = store
			.add(&AddInput::project_lesson("/work/one", "same lesson", Some("ctx")))
			.expect("first");
		let second = store
			.add(&AddInput::project_lesson("/work/one", "same lesson", Some("ctx")))
			.expect("second");
		assert_eq!(first.id, second.id);
		assert!(second.updated_at > first.updated_at);
		assert_eq!(
			store
				.list(&Filter::project("/work/one"))
				.expect("list")
				.len(),
			1
		);
	}

	#[test]
	fn legacy_import_is_idempotent_and_leaves_the_file_alone() {
		let dir = temp_dir();
		let cwd = dir.join("project");
		fs::create_dir_all(&cwd).expect("cwd");
		let root = crate::local_memory::memory_root(&dir.join("agent"), &cwd);
		fs::create_dir_all(&root).expect("root");
		let path = root.join(LEARNED_LESSONS_FILE);
		let original =
			"# Notes\n\nkeep this prose\n- old lesson _(context: review turn)_\n- second lesson\n";
		fs::write(&path, original).expect("seed learned.md");

		let store = MemoryStore::open(&dir.join("agent")).expect("store");
		let report = store
			.import_learned_lessons(&dir.join("agent"), &cwd)
			.expect("first import");
		assert_eq!(report.found, 2);
		assert_eq!(report.imported, 2);
		assert_eq!(report.skipped, 0);
		assert_eq!(fs::read_to_string(&path).expect("unchanged file"), original);

		let records = store.project_lessons(&cwd).expect("lessons");
		assert_eq!(records.len(), 2);
		assert_eq!(records[0].body, "old lesson", "file order is newest-first");
		assert_eq!(records[0].context.as_deref(), Some("review turn"));
		assert!(
			records
				.iter()
				.all(|record| record.origin == Origin::Inferred)
		);
		assert!(
			records
				.iter()
				.all(|record| record.source.as_deref() == Some("learned.md"))
		);

		let replay = store
			.import_learned_lessons(&dir.join("agent"), &cwd)
			.expect("replay import");
		assert!(replay.already_imported);
		assert_eq!((replay.found, replay.imported, replay.skipped), (0, 0, 0));
		assert_eq!(store.project_lessons(&cwd).expect("lessons").len(), 2);
	}

	#[test]
	fn marker_migrates_once_and_leaves_unreadable_artifacts_retryable() {
		let dir = temp_dir();
		let cwd = dir.join("project");
		fs::create_dir_all(&cwd).expect("cwd");
		let store = MemoryStore::open(&dir.join("agent")).expect("store");
		let report = store
			.import_learned_lessons(&dir.join("agent"), &cwd)
			.expect("missing file");
		assert_eq!((report.found, report.imported, report.skipped), (0, 0, 0));
		assert!(!report.already_imported);

		// A missing artifact still marks the scope: bullets added later are not
		// imported behind the user's back.
		let root = crate::local_memory::memory_root(&dir.join("agent"), &cwd);
		fs::create_dir_all(&root).expect("root");
		fs::write(root.join(LEARNED_LESSONS_FILE), "- late lesson\n").expect("late");
		let replay = store
			.import_learned_lessons(&dir.join("agent"), &cwd)
			.expect("marked");
		assert!(replay.already_imported);
		assert!(store.project_lessons(&cwd).expect("lessons").is_empty());

		// A read failure before the marker is set stays retryable.
		let other = temp_dir();
		let other_cwd = other.join("project");
		fs::create_dir_all(&other_cwd).expect("cwd");
		let other_store = MemoryStore::open(&other.join("agent")).expect("store");
		let other_root = crate::local_memory::memory_root(&other.join("agent"), &other_cwd);
		fs::create_dir_all(other_root.join(LEARNED_LESSONS_FILE)).expect("directory not file");
		let error = other_store
			.import_learned_lessons(&other.join("agent"), &other_cwd)
			.expect_err("unreadable legacy file");
		assert!(matches!(error, MemoryStoreError::Io(_)), "{error}");
		fs::remove_dir(other_root.join(LEARNED_LESSONS_FILE)).expect("remove dir");
		fs::write(other_root.join(LEARNED_LESSONS_FILE), "- recovered\n").expect("file");
		let recovered = other_store
			.import_learned_lessons(&other.join("agent"), &other_cwd)
			.expect("retry after fixing artifact");
		assert_eq!(recovered.imported, 1);
	}

	#[test]
	fn delete_never_resurrects_from_learned_md() {
		let dir = temp_dir();
		let agent = dir.join("agent");
		let cwd = dir.join("project");
		fs::create_dir_all(&cwd).expect("cwd");
		let root = crate::local_memory::memory_root(&agent, &cwd);
		fs::create_dir_all(&root).expect("root");
		let path = root.join(LEARNED_LESSONS_FILE);
		fs::write(&path, "prose\n- keep me\n- delete me\n").expect("seed");

		let store = MemoryStore::open(&agent).expect("store");
		store
			.import_learned_lessons(&agent, &cwd)
			.expect("import");
		let doomed = store
			.project_lessons(&cwd)
			.expect("lessons")
			.into_iter()
			.find(|record| record.body == "delete me")
			.expect("doomed record");
		delete_record_and_export(&agent, &cwd, &doomed.id).expect("delete");
		drop(store);

		// Any later read reopens the store; the marker means the stale bullet in
		// the file (removed by the export) is never imported again.
		let visible = list_visible(&agent, &cwd).expect("list after delete");
		assert!(!visible.iter().any(|record| record.id == doomed.id));
		assert!(!visible.iter().any(|record| record.body == "delete me"));
		assert!(visible.iter().any(|record| record.body == "keep me"));
		let again = list_visible(&agent, &cwd).expect("second list");
		assert_eq!(again.len(), visible.len());
		let text = fs::read_to_string(&path).expect("export");
		assert!(!text.contains("delete me"), "{text}");
		assert!(text.contains("keep me"));
	}

	#[test]
	fn update_recomputes_dedupe_and_rejects_collisions() {
		let store = MemoryStore::open_in_memory().expect("store");
		let first = add_project(&store, "/work/one", "original wording");
		let second = add_project(&store, "/work/one", "other wording");

		let edited = store
			.update(&first.id, Some("/work/one"), "edited wording")
			.expect("edit");
		assert_eq!(edited.body, "edited wording");
		// Re-learning the original wording now creates a distinct record.
		let relearned = add_project(&store, "/work/one", "original wording");
		assert_ne!(relearned.id, first.id);
		// The edited wording dedupes to the edited row instead of duplicating.
		let touched = add_project(&store, "/work/one", "edited wording");
		assert_eq!(touched.id, first.id);

		let collision = store
			.update(&second.id, Some("/work/one"), "edited wording")
			.expect_err("collision");
		assert!(matches!(collision, MemoryStoreError::Conflict(_)), "{collision}");
		assert_eq!(
			store.get(&second.id, Some("/work/one")).expect("unchanged").body,
			"other wording"
		);
	}

	#[test]
	fn export_preserves_prose_and_tracks_edit_and_delete() {
		let dir = temp_dir();
		let agent = dir.join("agent");
		let cwd = dir.join("project");
		fs::create_dir_all(&cwd).expect("cwd");
		let root = crate::local_memory::memory_root(&agent, &cwd);
		fs::create_dir_all(&root).expect("root");
		let path = root.join(LEARNED_LESSONS_FILE);
		fs::write(&path, "# Notes\n\nkeep me\n- legacy lesson\n").expect("seed");

		let store = MemoryStore::open(&agent).expect("store");
		store.import_learned_lessons(&agent, &cwd).expect("import");
		let record = store.project_lessons(&cwd).expect("lessons")[0].clone();
		let cwd_text = cwd.to_string_lossy();
		store
			.update(&record.id, Some(cwd_text.as_ref()), "edited lesson")
			.expect("update");
		store.export_learned_lessons(&agent, &cwd).expect("export");

		let text = fs::read_to_string(&path).expect("read export");
		assert!(text.contains("# Notes"));
		assert!(text.contains("keep me"));
		assert!(text.contains("- edited lesson"));
		assert!(!text.contains("legacy lesson"), "{text}");

		store
			.delete(&record.id, Some(cwd_text.as_ref()))
			.expect("delete");
		store
			.export_learned_lessons(&agent, &cwd)
			.expect("export after delete");
		let text = fs::read_to_string(&path).expect("read export");
		assert!(text.contains("# Notes"));
		assert!(text.contains("keep me"));
		assert!(!text.contains("edited lesson"), "{text}");
	}

	#[test]
	fn helper_paths_keep_the_export_in_sync() {
		let dir = temp_dir();
		let agent = dir.join("agent");
		let cwd = dir.join("project");
		fs::create_dir_all(&cwd).expect("cwd");
		let store = MemoryStore::open(&agent).expect("store");
		let record = add_project(&store, &cwd.to_string_lossy(), "helper lesson");
		drop(store);

		let updated = update_record_and_export(&agent, &cwd, &record.id, "helper lesson edited")
			.expect("update helper");
		assert_eq!(updated.body, "helper lesson edited");
		let text = fs::read_to_string(
			crate::local_memory::memory_root(&agent, &cwd).join(LEARNED_LESSONS_FILE),
		)
		.expect("export");
		assert!(text.contains("- helper lesson edited"), "{text}");

		delete_record_and_export(&agent, &cwd, &record.id).expect("delete helper");
		let text = fs::read_to_string(
			crate::local_memory::memory_root(&agent, &cwd).join(LEARNED_LESSONS_FILE),
		)
		.expect("export");
		assert!(!text.contains("helper lesson"), "{text}");
		assert!(list_visible(&agent, &cwd).expect("list").is_empty());
	}

	#[test]
	fn render_matches_the_legacy_bullet_shape() {
		let store = MemoryStore::open_in_memory().expect("store");
		let plain = add_project(&store, "/work/one", "plain lesson");
		assert_eq!(render_learned_line(&plain), "- plain lesson");
		let with_context = store
			.add(&AddInput::project_lesson("/work/one", "context lesson", Some("review turn")))
			.expect("context");
		assert_eq!(render_learned_line(&with_context), "- context lesson _(context: review turn)_");
	}

	#[test]
	fn concurrent_legacy_imports_run_once_and_keep_every_bullet() {
		let dir = temp_dir();
		let agent = dir.join("agent");
		let cwd = dir.join("project");
		fs::create_dir_all(&cwd).expect("cwd");
		let file = crate::local_memory::memory_root(&agent, &cwd).join(LEARNED_LESSONS_FILE);
		fs::create_dir_all(file.parent().expect("root")).expect("root");
		fs::write(&file, "- legacy one\n- legacy two\n").expect("seed");

		let mut handles = Vec::new();
		for _ in 0..8 {
			let agent = agent.clone();
			let cwd = cwd.clone();
			handles.push(std::thread::spawn(move || {
				let store = MemoryStore::open(&agent).expect("open");
				store.import_learned_lessons(&agent, &cwd).expect("import");
			}));
		}
		for handle in handles {
			handle.join().expect("thread");
		}

		let store = MemoryStore::open(&agent).expect("reopen");
		let records = store
			.list(&Filter::project(&cwd.to_string_lossy()))
			.expect("list");
		assert_eq!(records.len(), 2, "legacy bullets are imported exactly once");
		assert!(
			records
				.iter()
				.all(|record| matches!(record.body.as_str(), "legacy one" | "legacy two")),
			"{records:?}"
		);
	}

	#[test]
	fn aliased_spellings_of_one_store_share_a_path_and_a_lock() {
		let dir = temp_dir();
		let agent = dir.join("agent");
		let store = MemoryStore::open(&agent).expect("open");
		let canonical = store.path().to_path_buf();
		drop(store);

		// `agent/../agent`: same directory, different spelling, no symlink needed.
		let dotted = dir
			.join("agent")
			.join("..")
			.join("agent")
			.join(MEMORY_DB_FILE);
		let through_dots = MemoryStore::open_at(dotted.clone()).expect("open dotted");
		assert_eq!(
			through_dots.path(),
			canonical.as_path(),
			"`..` spelling must resolve to the same store"
		);
		drop(through_dots);

		#[cfg(unix)]
		{
			let alias = dir.join("alias");
			std::os::unix::fs::symlink(&agent, &alias).expect("symlink");
			let through_link = MemoryStore::open_at(alias.join(MEMORY_DB_FILE)).expect("open link");
			assert_eq!(
				through_link.path(),
				canonical.as_path(),
				"symlink spelling must resolve to the same store"
			);
			drop(through_link);

			// Both spellings must write one file under one lock.
			let mut handles = Vec::new();
			for index in 0..8 {
				let spelled = if index % 2 == 0 {
					alias.join(MEMORY_DB_FILE)
				} else {
					dotted.clone()
				};
				handles.push(std::thread::spawn(move || {
					let store = MemoryStore::open_at(spelled).expect("open");
					store
						.add(&AddInput::project_lesson("/work/one", &format!("alias {index}"), None))
						.expect("add");
				}));
			}
			for handle in handles {
				handle.join().expect("thread");
			}
			let store = MemoryStore::open(&agent).expect("reopen");
			let records = store.list(&Filter::project("/work/one")).expect("list");
			assert_eq!(records.len(), 8, "both spellings write the same store");
		}
	}

	#[test]
	fn concurrent_opens_and_writes_keep_every_record() {
		let dir = temp_dir();
		let cwd = "/work/one".to_string();
		let mut handles = Vec::new();
		for index in 0..12 {
			let dir = dir.clone();
			let cwd = cwd.clone();
			handles.push(std::thread::spawn(move || {
				let store = MemoryStore::open(&dir).expect("open");
				store
					.add(&AddInput::project_lesson(&cwd, &format!("record {index}"), None))
					.expect("add");
			}));
		}
		for handle in handles {
			handle.join().expect("thread");
		}
		let store = MemoryStore::open(&dir).expect("reopen");
		let records = store.list(&Filter::project(&cwd)).expect("list");
		assert_eq!(records.len(), 12, "every concurrent writer keeps its record");
		for index in 0..12 {
			let expected = format!("record {index}");
			assert!(records.iter().any(|record| record.body == expected), "missing {expected}");
		}
	}

	#[test]
	fn schema_v1_migrates_to_v2_preserving_rows_and_dedupe_identity() {
		let dir = temp_dir();
		let path = MemoryStore::database_path(&dir);
		let id = "0123456789abcdef0123456789abcdef";
		let body = "preserved migration lesson";
		let dedupe = record_dedupe_key(Scope::Project, Some("/work/one"), None, body, None);
		{
			let connection = Connection::open(&path).expect("raw v1 open");
			connection
				.execute_batch(
					"CREATE TABLE memories (
						id TEXT PRIMARY KEY,
						scope TEXT NOT NULL CHECK (scope IN ('global', 'project')),
						cwd TEXT,
						body TEXT NOT NULL,
						context TEXT,
						origin TEXT NOT NULL CHECK (origin IN ('asserted', 'inferred')),
						actor TEXT,
						source TEXT,
						created_at INTEGER NOT NULL,
						updated_at INTEGER NOT NULL,
						dedupe_key TEXT
					);
					CREATE UNIQUE INDEX memories_dedupe
						ON memories (dedupe_key) WHERE dedupe_key IS NOT NULL;
					CREATE INDEX memories_scope_updated
						ON memories (scope, cwd, updated_at DESC);
					CREATE TABLE memory_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
					PRAGMA user_version = 1;",
				)
				.expect("v1 schema");
			connection
				.execute(
					"INSERT INTO memories \
					 (id, scope, cwd, body, context, origin, actor, source, created_at, updated_at, dedupe_key) \
					 VALUES (?1, 'project', '/work/one', ?2, 'old context', 'inferred', 'agent', 'learn', 100, 200, ?3)",
					params![id, body, dedupe],
				)
				.expect("v1 row");
		}
		{
			let store = MemoryStore::open(&dir).expect("migrate");
			let record = store.get(id, Some("/work/one")).expect("preserved row");
			assert_eq!(record.body, body);
			assert_eq!(record.context.as_deref(), Some("old context"));
			assert_eq!(record.created_at, 100);
			assert_eq!(record.updated_at, 200);
			assert_eq!(record.session_id, None);
			assert_eq!(record.access_count, 0);
			assert_eq!(record.last_accessed_at, None);
			assert_eq!(record.confidence, 1.0);
			assert_eq!(record.ttl_secs, None);
			assert_eq!(record.consolidated_into, None);
			let hits = store.recall("migration lesson", Some("/work/one"), None).expect("fts after migration");
			assert_eq!(hits.len(), 1);
			assert_eq!(hits[0].id, id);
			// The v1 dedupe key still matches a post-migration learn of the same text.
			let touched = store
				.add(&AddInput::project_lesson("/work/one", body, Some("old context")))
				.expect("dedupe after migration");
			assert_eq!(touched.id, id);
			assert_eq!(store.list(&Filter::project("/work/one")).expect("list").len(), 1);
		}
		let store = MemoryStore::open(&dir).expect("idempotent reopen");
		let version: i64 = store
			.lock()
			.query_row("PRAGMA user_version", [], |row| row.get(0))
			.expect("version");
		assert_eq!(version, SCHEMA_VERSION);
		assert_eq!(store.list(&Filter::project("/work/one")).expect("still one").len(), 1);
	}

	#[test]
	fn session_scope_is_strictly_isolated_and_checked() {
		let store = MemoryStore::open_in_memory().expect("store");
		let project = add_project(&store, "/work/one", "project deploy note");
		let other = store
			.add(&AddInput::session_lesson("session-a", "session deploy note", None))
			.expect("session add");
		let second = store
			.add(&AddInput::session_lesson("session-b", "other deploy note", None))
			.expect("second session add");

		let project_view = store.list(&Filter::all(Some("/work/one"))).expect("project view");
		assert!(!project_view.iter().any(|record| record.id == other.id));
		let session_view = store
			.list(&Filter::all(Some("/work/one")).with_session("session-a"))
			.expect("session view");
		assert!(session_view.iter().any(|record| record.id == other.id));
		assert!(!session_view.iter().any(|record| record.id == second.id));
		let only_session = store.list(&Filter::session("session-a")).expect("session filter");
		assert_eq!(only_session.len(), 1);
		assert_eq!(only_session[0].id, other.id);
		let unscoped = store.list(&Filter::default()).expect("unscoped list");
		assert!(unscoped.iter().all(|record| record.scope != Scope::Session));

		assert!(store.get(&other.id, Some("/work/one")).is_err(), "session rows need a session");
		assert!(store.get_with_session(&other.id, Some("/work/one"), Some("session-b")).is_err());
		assert_eq!(
			store
				.get_with_session(&other.id, Some("/work/one"), Some("session-a"))
				.expect("owner session"),
			other
		);
		assert!(
			store
				.update_with_session(&other.id, Some("/work/one"), Some("session-b"), "hijack")
				.is_err()
		);
		assert!(
			store
				.delete_with_session(&other.id, Some("/work/one"), Some("session-b"))
				.is_err()
		);
		assert!(
			store
				.add(&AddInput::asserted(Scope::Session, None, "missing session"))
				.expect_err("session requires id")
				.to_string()
				.contains("session_id")
		);

		// The table CHECK rejects a session row that also carries a cwd, and a
		// project row that carries a session id.
		let guard = store.lock();
		let insert = "INSERT INTO memories \
		 (id, scope, cwd, session_id, body, origin, created_at, updated_at) \
		 VALUES ('ffffffffffffffffffffffffffffffff', ?1, ?2, ?3, 'bad', 'inferred', 1, 1)";
		assert!(guard.execute(insert, params!["session", "/work/one", "session-a"]).is_err());
		assert!(guard.execute(insert, params!["project", "/work/one", "session-a"]).is_err());
		assert!(guard.execute(insert, params!["session", None, ""]).is_err());
		assert_eq!(project.id.len(), RECORD_ID_LEN);
	}

	#[test]
	fn embeddings_roundtrip_hybrid_recall_and_invalidate_on_update() {
		let store = MemoryStore::open_in_memory().expect("store");
		let alpha = add_project(&store, "/work/one", "deploy staging alpha");
		let beta = add_project(&store, "/work/one", "deploy staging beta");
		let query = [1.0f32, 0.0, 0.0, 0.0];
		store.set_embedding(&alpha.id, "test-model", &query).expect("alpha vector");
		store
			.set_embedding(&beta.id, "test-model", &[0.0, 1.0, 0.0, 0.0])
			.expect("beta vector");
		assert_eq!(
			store.embedding(&alpha.id, "test-model").expect("read vector"),
			Some(query.to_vec())
		);
		assert!(store.set_embedding(&alpha.id, "test-model", &[f32::NAN]).is_err());

		let semantic = add_project(&store, "/work/one", "unrelated garden note");
		store.set_embedding(&semantic.id, "test-model", &query).expect("semantic vector");
		let lexical = store.recall("deploy staging", Some("/work/one"), Some(2)).expect("lexical");
		assert_eq!(lexical.len(), 2);
		let hybrid = store
			.recall_hybrid(
				"deploy staging",
				Some("/work/one"),
				None,
				Some(("test-model", query.as_slice())),
				Some(2),
			)
			.expect("hybrid");
		assert_eq!(hybrid.len(), 2);
		assert_eq!(hybrid[0].id, alpha.id, "matching vector ranks first");
		assert!(hybrid[0].access_count >= 1);
		assert!(hybrid[0].last_accessed_at.is_some());
		let union = store
			.recall_hybrid(
				"deploy staging",
				Some("/work/one"),
				None,
				Some(("test-model", query.as_slice())),
				Some(10),
			)
			.expect("vector candidates union FTS");
		assert!(union.iter().any(|record| record.id == semantic.id), "semantic-only row");
		let punctuation = store
			.recall_hybrid(
				"?!?!",
				Some("/work/one"),
				None,
				Some(("test-model", query.as_slice())),
				Some(10),
			)
			.expect("punctuation falls through to vectors");
		assert!(punctuation.iter().any(|record| record.id == semantic.id));
		assert!(
			store
				.recall("?!?!", Some("/work/one"), Some(10))
				.expect("punctuation lexical is empty")
				.is_empty()
		);

		// A body edit invalidates the stale vector.
		store
			.update(&alpha.id, Some("/work/one"), "deploy staging rewritten")
			.expect("update");
		assert_eq!(store.embedding(&alpha.id, "test-model").expect("after update"), None);
		store
			.recall_hybrid("deploy staging", Some("/work/one"), None, Some(("test-model", query.as_slice())), Some(2))
			.expect("hybrid falls back when vectors are missing");
	}

	#[test]
	fn asserted_ttl_rows_survive_and_consolidation_targets_stay_in_scope() {
		let store = MemoryStore::open_in_memory().expect("store");
		let asserted = store
			.add(&AddInput::asserted(Scope::Project, Some("/work/one"), "asserted ttl"))
			.expect("asserted");
		store.set_ttl(&asserted.id, Some(1)).expect("ttl");
		let inferred = add_project(&store, "/work/one", "inferred ttl");
		store.set_ttl(&inferred.id, Some(1)).expect("ttl");
		let report = store
			.consolidate_at(now_seconds().saturating_add(60))
			.expect("consolidate");
		assert_eq!(report.expired, 1, "only inferred rows expire");
		assert!(store.get(&asserted.id, Some("/work/one")).is_ok());
		assert!(store.get(&inferred.id, Some("/work/one")).is_err());
		// Expired against the real wall clock, not just the injected clock.
		store.set_ttl(&asserted.id, Some(0)).expect("past asserted ttl");
		let hits = store
			.recall("asserted ttl", Some("/work/one"), Some(10))
			.expect("asserted recall");
		assert!(
			hits.iter().any(|record| record.id == asserted.id),
			"asserted rows stay recallable after TTL"
		);

		let same_scope = add_project(&store, "/work/one", "same scope");
		let other_scope = add_project(&store, "/work/two", "other scope");
		assert!(
			store
				.set_consolidated_into(&same_scope.id, Some(other_scope.id.as_str()))
				.is_err()
		);
		store
			.set_consolidated_into(&same_scope.id, Some(asserted.id.as_str()))
			.expect("same scope");
		assert_eq!(
			store
				.get(&same_scope.id, Some("/work/one"))
				.expect("record")
				.consolidated_into
				.as_deref(),
			Some(asserted.id.as_str())
		);
	}

	#[test]
	fn ttl_consolidation_refreshes_the_derived_learned_file() {
		let dir = temp_dir();
		let agent_dir = dir.join("agent");
		let cwd = dir.join("project");
		fs::create_dir_all(&agent_dir).expect("agent");
		fs::create_dir_all(&cwd).expect("project");
		let cwd_text = cwd.to_string_lossy().to_string();
		let store = MemoryStore::open(&agent_dir).expect("store");
		let expired = store
			.add(&AddInput::project_lesson(&cwd_text, "stale exported lesson", None))
			.expect("lesson");
		store.set_ttl(&expired.id, Some(1)).expect("ttl");
		store
			.export_learned_lessons(&agent_dir, &cwd)
			.expect("initial export");
		let learned = crate::local_memory::memory_root(&agent_dir, &cwd)
			.join(crate::local_memory::LEARNED_LESSONS_FILE);
		assert!(fs::read_to_string(&learned).expect("learned").contains("stale exported lesson"));
		store
			.consolidate_at(now_seconds().saturating_add(60))
			.expect("consolidate");
		assert!(
			!fs::read_to_string(&learned)
				.expect("learned after expiry")
				.contains("stale exported lesson")
		);
	}

	#[test]
	fn failed_ttl_export_retries_on_the_next_consolidation() {
		let dir = temp_dir();
		let agent_dir = dir.join("agent");
		let cwd = dir.join("project");
		fs::create_dir_all(&agent_dir).expect("agent");
		fs::create_dir_all(&cwd).expect("project");
		let cwd_text = cwd.to_string_lossy().to_string();
		let store = MemoryStore::open(&agent_dir).expect("store");
		let expired = store
			.add(&AddInput::project_lesson(&cwd_text, "retry export lesson", None))
			.expect("lesson");
		store.set_ttl(&expired.id, Some(1)).expect("ttl");
		store
			.export_learned_lessons(&agent_dir, &cwd)
			.expect("initial export");
		let learned = crate::local_memory::memory_root(&agent_dir, &cwd)
			.join(crate::local_memory::LEARNED_LESSONS_FILE);
		// Block the atomic rename by making the destination a directory.
		fs::remove_file(&learned).expect("remove learned");
		fs::create_dir_all(&learned).expect("block learned");

		assert!(
			store
				.consolidate_at(now_seconds().saturating_add(60))
				.is_err(),
			"blocked export must surface"
		);
		assert!(store.get(&expired.id, Some(cwd_text.as_str())).is_err());
		let dirty: i64 = store
			.lock()
			.query_row(
				"SELECT COUNT(*) FROM memory_export_dirty WHERE dirty != 0",
				[],
				|row| row.get(0),
			)
			.expect("dirty count");
		assert_eq!(dirty, 1, "failed export stays marked");

		fs::remove_dir_all(&learned).expect("unblock learned");
		store
			.consolidate_at(now_seconds().saturating_add(60))
			.expect("retry export");
		assert!(
			!fs::read_to_string(&learned)
				.expect("learned repaired")
				.contains("retry export lesson")
		);
		let dirty: i64 = store
			.lock()
			.query_row(
				"SELECT COUNT(*) FROM memory_export_dirty WHERE dirty != 0",
				[],
				|row| row.get(0),
			)
			.expect("clean count");
		assert_eq!(dirty, 0);
	}

	#[test]
	fn consolidation_expires_ttl_and_clusters_near_duplicates_per_scope() {
		let store = MemoryStore::open_in_memory().expect("store");
		let first = add_project(&store, "/work/one", "staging deploy alpha");
		let second = add_project(&store, "/work/one", "staging deploy beta");
		let other_scope = add_project(&store, "/work/two", "staging deploy alpha");
		let expired = add_project(&store, "/work/one", "staging deploy expired");
		store.set_ttl(&expired.id, Some(10)).expect("ttl");
		let vector = [1.0f32, 0.0, 0.0, 0.0];
		let near = [0.999f32, 0.01, 0.0, 0.0];
		store.set_embedding(&first.id, "test-model", &vector).expect("first");
		store.set_embedding(&second.id, "test-model", &near).expect("second");
		store.set_embedding(&other_scope.id, "test-model", &vector).expect("other");

		let report = store
			.consolidate_at(now_seconds().saturating_add(60))
			.expect("consolidate");
		assert_eq!(report.expired, 1);
		assert_eq!(report.consolidated, 1);
		assert_eq!(report.clusters, 1);
		assert_eq!(report.llm_used, 0);

		let canonical = if first.id < second.id { &first.id } else { &second.id };
		let duplicate = if first.id < second.id { &second.id } else { &first.id };
		let duplicate_record = store.get(duplicate, Some("/work/one")).expect("duplicate row");
		assert_eq!(duplicate_record.consolidated_into.as_deref(), Some(canonical.as_str()));
		let cross_scope = store.get(&other_scope.id, Some("/work/two")).expect("other scope");
		assert_eq!(cross_scope.consolidated_into, None);
		assert!(store.get(&expired.id, Some("/work/one")).is_err(), "ttl row expired");

		let hits = store.recall("staging deploy", Some("/work/one"), Some(10)).expect("recall");
		assert!(hits.iter().all(|record| record.id != *duplicate), "duplicates hidden");
		assert!(hits.iter().any(|record| record.id == *canonical));

		store.set_ttl(&first.id, Some(0)).expect("immediate ttl");
		let report = store.consolidate_at(now_seconds()).expect("second pass");
		assert_eq!(report.expired, 1);
	}
}
