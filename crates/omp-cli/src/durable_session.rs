//! Concrete durable session owner.
//!
//! One synchronous [`SqliteStore`] lives on a dedicated thread; every caller —
//! including the async agent loop — sends a job and awaits its result. SQLite
//! work therefore never runs on a Tokio worker, and every write either commits
//! or returns its error to the caller.
//!
//! [`DurableSession`] implements [`pi_agent::DurableBoundary`] for the live
//! loop and exposes the small synchronous surface [`crate::session::SessionStore`]
//! needs. It is the only live session authority: no JSONL journal is written
//! beside it.

use std::{
	collections::{HashMap, VecDeque},
	path::{Path, PathBuf},
	sync::{
		Arc, Mutex,
		mpsc::{self, Sender},
	},
	time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use pi_agent::CompactionReport;
use rusqlite::{OpenFlags, OptionalExtension as _};
use pi_agent::{
	DurableBoundary, DurableError,
	state::{Message, Role, StopReason, ToolCall, ToolResult},
};
use serde_json::Value;
use xxhash_rust::xxh64::xxh64;

use crate::{
	output,
	session::{
		ContentPart as SessionPart, MessageContent, MessageEntry, SessionEntry, SessionHeader,
		SessionMessage, Usage, derive_turn_history,
	},
	session_store::{
		NewBranch, NewIntent, NewMessage, NewSession, SessionRecord, SqliteStore, StoreError,
		StoreResult, UnresolvedIntent,
	},
};

/// One job on the writer thread. The reply channel is already captured by the
/// closure, so sending or awaiting the reply can never reorder a write.
type Job = Box<dyn FnOnce(&mut SqliteStore) + Send>;

/// Handle to the dedicated SQLite writer thread.
pub struct DurableWriter {
	/// `mpsc::Sender` is not guaranteed `Sync` on every toolchain; the mutex
	/// makes the handle explicitly shareable by concurrent tool tasks.
	sender: Mutex<Sender<Job>>,
}

impl std::fmt::Debug for DurableWriter {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.debug_struct("DurableWriter").finish_non_exhaustive()
	}
}

impl DurableWriter {
	/// Open the database and start its writer thread. The thread keeps the
	/// connection until every handle is dropped; queued jobs still run.
	pub fn open(path: impl Into<PathBuf>) -> StoreResult<Self> {
		let mut store = SqliteStore::open(path)?;
		let (sender, receiver) = mpsc::channel::<Job>();
		std::thread::Builder::new()
			.name("omp-session-store".to_string())
			.spawn(move || {
				while let Ok(job) = receiver.recv() {
					job(&mut store);
				}
			})
			.map_err(|error| {
				StoreError::Io(std::io::Error::new(
					error.kind(),
					format!("cannot spawn session store writer: {error}"),
				))
			})?;
		Ok(Self { sender: Mutex::new(sender) })
	}

	fn send(&self, job: Job) -> StoreResult<()> {
		lock(&self.sender).send(job).map_err(|_| {
			StoreError::Io(std::io::Error::new(
				std::io::ErrorKind::BrokenPipe,
				"session store writer stopped",
			))
		})
	}

	/// Blocking call for the synchronous session manager. The SQLite work still
	/// happens on the writer thread; this thread only waits for the reply.
	pub fn call<T: Send + 'static>(
		&self,
		operation: impl FnOnce(&mut SqliteStore) -> StoreResult<T> + Send + 'static,
	) -> StoreResult<T> {
		let (reply, receiver) = mpsc::channel();
		self.send(Box::new(move |store| {
			let _ = reply.send(operation(store));
		}))?;
		receiver.recv().map_err(|_| {
			StoreError::Io(std::io::Error::new(
				std::io::ErrorKind::BrokenPipe,
				"session store writer stopped before replying",
			))
		})?
	}

	/// Async call for the agent loop. No Tokio worker blocks on SQLite.
	pub async fn call_async<T: Send + 'static>(
		&self,
		operation: impl FnOnce(&mut SqliteStore) -> StoreResult<T> + Send + 'static,
	) -> StoreResult<T> {
		let (reply, receiver) = tokio::sync::oneshot::channel();
		self.send(Box::new(move |store| {
			let _ = reply.send(operation(store));
		}))?;
		receiver.await.map_err(|_| {
			StoreError::Io(std::io::Error::new(
				std::io::ErrorKind::BrokenPipe,
				"session store writer stopped before replying",
			))
		})?
	}
}

/// Mutable cursor shared by the loop boundary and the synchronous manager.
#[derive(Debug, Clone)]
struct Cursor {
	/// Active branch id (store branch row).
	branch: String,
	/// Last message id on the active branch, used as the next parent.
	leaf: Option<String>,
}

/// One session's durable handle. Cloning is cheap: all clones share the writer,
/// branch cursor, and message cursor.
#[derive(Debug)]
pub struct DurableSession {
	writer: Arc<DurableWriter>,
	session_id: String,
	cursor: Arc<Mutex<Cursor>>,
	/// Provider tool-call ids whose result row was written together with the
	/// durable outcome. Synthetic/unexecuted tool messages are not in this set.
	written_tools: Arc<Mutex<HashMap<String, usize>>>,
	/// Stable revision (digest) of every conversation message in the last
	/// persisted view. Compaction remaps this by matching the retained tail,
	/// never by lengths alone.
	tracked: Arc<Mutex<Vec<u64>>>,
	/// Durable message id parallel to `tracked`; `None` for runtime-only
	/// messages (system prompt/compaction notice).
	tracked_ids: Arc<Mutex<Vec<Option<String>>>>,
	/// On resume, digests and ids of the stored (or projected) messages. The
	/// first live write aligns the incoming history against them before
	/// appending new messages.
	resume_seed: Arc<Mutex<Option<Vec<(u64, Option<String>)>>>>,
	written_tool_ids: Arc<Mutex<HashMap<String, VecDeque<String>>>>,
	/// Selected compacted projection, mirrored in the session payload.
	projection: Arc<Mutex<Option<StoredProjection>>>,
}

/// The selected compressed conversation: archive rows referenced by id plus
/// runtime-only synthetic messages (the compaction notice). Full archive rows
/// remain in SQLite; no message body is duplicated except the synthetic
/// notice.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct StoredProjection {
	#[serde(default)]
	pub kept: Vec<String>,
	#[serde(default)]
	pub synthetic: Vec<Value>,
}

/// Session state loaded from the authoritative database.
#[derive(Debug)]
pub struct LoadedSession {
	pub header: SessionHeader,
	pub branch_id: String,
	pub leaf: Option<String>,
	/// Every entry in the session, all branches included.
	pub entries: Vec<SessionEntry>,
	pub messages: Vec<MessageEntry>,
	/// Entries on the active branch only, oldest first.
	pub branch_messages: Vec<MessageEntry>,
	/// Selected compacted projection, when one is persisted.
	pub projection: Option<StoredProjection>,
}

impl DurableSession {
	/// Open the writer and create a new session plus its root branch in one
	/// transaction.
	pub fn create(
		db_path: impl Into<PathBuf>,
		header: SessionHeader,
	) -> StoreResult<Self> {
		let writer = Arc::new(DurableWriter::open(db_path)?);
		let session_id = header.id.clone();
		let branch_id = root_branch_id(&session_id);
		let payload = serde_json::to_value(&header)?;
		let parent_session_id = header.parent_session.clone();
		let session = NewSession {
			id: session_id.clone(),
			parent_session_id: None,
			payload,
			initial_branch_id: branch_id.clone(),
			initial_branch_payload: Value::Null,
		};
		writer.call(move |store| {
			// Lineage may name a session in another project's database; the FK
			// only accepts an id that exists here. The header keeps the original.
			let parent = parent_session_id.filter(|parent| {
				store.session(parent).ok().flatten().is_some()
			});
			store.create_session(NewSession { parent_session_id: parent, ..session })
		})?;
		Ok(Self {
			writer,
			session_id,
			cursor: Arc::new(Mutex::new(Cursor { branch: branch_id, leaf: None })),
			written_tools: Arc::new(Mutex::new(HashMap::new())),
			written_tool_ids: Arc::new(Mutex::new(HashMap::new())),
			tracked: Arc::new(Mutex::new(Vec::new())),
			tracked_ids: Arc::new(Mutex::new(Vec::new())),
			projection: Arc::new(Mutex::new(None)),
			resume_seed: Arc::new(Mutex::new(None)),
		})
	}

	/// Open an existing session or a new one. `None` means the row was absent
	/// and the header was inserted.
	pub fn open(
		db_path: impl Into<PathBuf>,
		session_id: &str,
		header: SessionHeader,
	) -> StoreResult<(Self, Option<LoadedSession>)> {
		let path = db_path.into();
		let writer = Arc::new(DurableWriter::open(path)?);
		let id = session_id.to_string();
		let existing = {
			let id = id.clone();
			writer.call(move |store| store.session(&id))?
		};
		match existing {
			Some(_) => {
				let (
					branch_id,
					leaf,
					entries,
					messages,
					branch_messages,
					stored_header,
					projection,
				) = {
					let id = id.clone();
					writer.call(move |store| load_parts(store, &id))?
				};
				let mut header = header;
				if let Some(stored) = stored_header {
					header = stored;
				}
				Ok((
					Self {
						writer,
						session_id: id,
						cursor: Arc::new(Mutex::new(Cursor {
							branch: branch_id.clone(),
							leaf: leaf.clone(),
						})),
						written_tools: Arc::new(Mutex::new(HashMap::new())),
						written_tool_ids: Arc::new(Mutex::new(HashMap::new())),
						projection: Arc::new(Mutex::new(projection.clone())),
						resume_seed: Arc::new(Mutex::new(Some(seed_entries(
							&branch_messages,
							projection.as_ref(),
							&messages,
						)))),
						tracked: Arc::new(Mutex::new(Vec::new())),
						tracked_ids: Arc::new(Mutex::new(Vec::new())),
					},
					Some(LoadedSession {
						header,
						branch_id,
						leaf,
						entries,
						messages,
						branch_messages,
						projection,
					}),
				))
			},
			None => {
				let session_id = id.clone();
				let branch_id = root_branch_id(&session_id);
				let payload = serde_json::to_value(&header)?;
				let parent = header.parent_session.clone();
				let create_branch = branch_id.clone();
				writer.call(move |store| {
					let parent = parent
						.filter(|parent| store.session(parent).ok().flatten().is_some());
					store.create_session(NewSession {
						id: session_id,
						parent_session_id: parent,
						payload,
						initial_branch_id: create_branch,
						initial_branch_payload: Value::Null,
					})
				})?;
				Ok((
					Self {
						writer,
						session_id: id,
						cursor: Arc::new(Mutex::new(Cursor { branch: branch_id, leaf: None })),
						written_tools: Arc::new(Mutex::new(HashMap::new())),
						written_tool_ids: Arc::new(Mutex::new(HashMap::new())),
						tracked: Arc::new(Mutex::new(Vec::new())),
						tracked_ids: Arc::new(Mutex::new(Vec::new())),
						projection: Arc::new(Mutex::new(None)),
						resume_seed: Arc::new(Mutex::new(None)),
					},
					None,
				))
			},
		}
	}

	#[must_use]
	pub fn session_id(&self) -> &str {
		&self.session_id
	}

	#[must_use]
	pub fn active_branch(&self) -> String {
		lock(&self.cursor).branch.clone()
	}

	/// Load the active branch and its full entry set. Used on resume and after
	/// an external durable write (the loop's assistant/tool rows).
	pub fn load(&self) -> StoreResult<LoadedSession> {
		let session_id = self.session_id.clone();
		let writer = Arc::clone(&self.writer);
		let (branch_id, leaf, entries, messages, branch_messages, header, projection) = {
			let session_id = session_id.clone();
			writer.call(move |store| load_parts(store, &session_id))?
		};
		let header = header.ok_or_else(|| {
			StoreError::NotFound(format!("session {session_id} has no stored header"))
		})?;
		{
			let mut cursor = lock(&self.cursor);
			cursor.branch = branch_id.clone();
			cursor.leaf = leaf.clone();
		}
		// Existing tool rows are part of the seed prefix, not pending outcomes.
		lock(&self.written_tools).clear();
		lock(&self.written_tool_ids).clear();
		*lock(&self.projection) = projection.clone();
		if lock(&self.tracked).is_empty() {
			*lock(&self.resume_seed) = Some(seed_entries(
				&branch_messages,
				projection.as_ref(),
				&messages,
			));
		}
		Ok(LoadedSession {
			header,
			branch_id,
			leaf,
			entries,
			messages,
			branch_messages,
			projection,
		})
	}

	/// Append one raw session entry (message or metadata) to the authoritative
	/// branch. A `branch_summary` opens a new branch at its parent first.
	pub fn append_entry(&self, entry: SessionEntry) -> StoreResult<String> {
		let id = entry.id.clone();
		let cursor = Arc::clone(&self.cursor);
		let session_id = self.session_id.clone();
		self.writer.call(move |store| {
			let mut cursor = lock(&cursor);
			// A metadata entry chains to the current leaf; only a branch cut
			// explicitly names its branch point.
			let parent = if entry.kind == "branch_summary" {
				entry.parent_id.clone()
			} else {
				cursor.leaf.clone()
			};
			if entry.kind == "branch_summary" {
				let branch_id = format!("{}.branch-{}", session_id, output::session_id());
				let (parent_branch_id, parent_message_id) = match &parent {
					Some(point) => (Some(cursor.branch.clone()), Some(point.clone())),
					// An explicit root cut starts an empty branch, not a child of
					// the current branch with a missing point.
					None => (None, None),
				};
				store.create_branch(NewBranch {
					id: branch_id.clone(),
					session_id: session_id.clone(),
					parent_branch_id,
					parent_message_id,
					payload: Value::Null,
				})?;
				store.set_active_branch(&session_id, &branch_id)?;
				cursor.branch = branch_id;
				cursor.leaf = parent.clone();
			}
			let mut stored = entry;
			if stored.kind != "branch_summary" {
				stored.parent_id = parent.clone();
			}
			store.append_message(NewMessage {
				id: stored.id.clone(),
				session_id: session_id.clone(),
				branch_id: cursor.branch.clone(),
				parent_message_id: parent,
				payload: serde_json::to_value(&stored)?,
			})?;
			cursor.leaf = Some(entry.id.clone());
			Ok(())
		})?;
		Ok(id)
	}

	/// Persist a changed session header (auto title/rename). The id, lineage,
	/// and active branch are not touched.
	pub fn update_header(&self, header: &SessionHeader) -> StoreResult<()> {
		let session_id = self.session_id.clone();
		let payload = serde_json::to_value(header)?;
		self.writer.call(move |store| {
			// Preserve a selected compaction projection: it is session state,
			// not part of the typed header.
			let mut merged = store
				.session(&session_id)?
				.ok_or_else(|| StoreError::NotFound(format!("session {session_id} not found")))?
				.payload;
			let Some(source) = payload.as_object() else {
				return Err(StoreError::Corrupt("session header is not an object".to_string()));
			};
			let Some(target) = merged.as_object_mut() else {
				return Err(StoreError::Corrupt("session payload is not an object".to_string()));
			};
			for (key, value) in source {
				target.insert(key.clone(), value.clone());
			}
			store.update_session_payload(&session_id, merged)
		})
	}

	fn align_ids(&self, messages: &[Message], branch: &[MessageEntry]) -> Vec<Option<String>> {
		let persisted: Vec<&MessageEntry> = branch
			.iter()
			.filter(|entry| {
				matches!(entry.message.role.as_str(), "user" | "assistant" | "toolResult")
			})
			.collect();
		let mut ids = Vec::with_capacity(messages.len());
		let mut seen = 0usize;
		for message in messages {
			if self.to_session_message(message).is_none() {
				ids.push(None);
				continue;
			}
			let digest = self.incoming_digest(message);
			match persisted.get(seen) {
				Some(entry) if session_digest(&entry.message) == digest => {
					ids.push(Some(entry.id.clone()));
					seen += 1;
				},
				_ => ids.push(None),
			}
		}
		ids
	}

	async fn set_projection(&self, projection: Option<StoredProjection>) -> Result<(), DurableError> {
		let session_id = self.session_id.clone();
		self.writer
			.call_async(move |store| {
				let mut payload = store
					.session(&session_id)?
					.ok_or_else(|| {
						StoreError::NotFound(format!("session {session_id} not found"))
					})?
					.payload;
				let Some(map) = payload.as_object_mut() else {
					return Err(StoreError::Corrupt("session payload is not an object".to_string()));
				};
				match projection {
					Some(projection) => {
						map.insert(
							"durableProjection".to_string(),
							serde_json::to_value(projection)?,
						);
					},
					None => {
						map.remove("durableProjection");
					},
				}
				store.update_session_payload(&session_id, payload)
			})
			.await
			.map_err(|error| DurableError::new(error.to_string()))
	}

	/// Remove the selected projection synchronously (branch cut/rewind).
	pub fn clear_projection(&self) -> StoreResult<()> {
		let session_id = self.session_id.clone();
		self.writer.call(move |store| {
			let mut payload = store
				.session(&session_id)?
				.ok_or_else(|| {
					StoreError::NotFound(format!("session {session_id} not found"))
				})?
				.payload;
			let Some(map) = payload.as_object_mut() else {
				return Err(StoreError::Corrupt("session payload is not an object".to_string()));
			};
			map.remove("durableProjection");
			store.update_session_payload(&session_id, payload)
		})
	}

	/// Unresolved executions for startup surfacing. Presence is not proof the
	/// effect ran; callers must not replay automatically.
	pub fn unresolved_intents(&self) -> StoreResult<Vec<UnresolvedIntent>> {
		let session_id = self.session_id.clone();
		self.writer.call(move |store| store.unresolved_intents(&session_id))
	}

	/// Persist a whole `MessageEntry` batch atomically on the current branch.
	pub fn append_message_entries(&self, entries: &[MessageEntry]) -> StoreResult<()> {
		if entries.is_empty() {
			return Ok(());
		}
		let cursor = Arc::clone(&self.cursor);
		let session_id = self.session_id.clone();
		let entries = entries.to_vec();
		self.writer.call(move |store| {
			let mut cursor = lock(&cursor);
			let mut batch = Vec::with_capacity(entries.len());
			let mut parent = cursor.leaf.clone();
			for mut entry in entries {
				entry.parent_id = parent.clone();
				parent = Some(entry.id.clone());
				batch.push(NewMessage {
					id: entry.id.clone(),
					session_id: session_id.clone(),
					branch_id: cursor.branch.clone(),
					parent_message_id: entry.parent_id.clone(),
					payload: serde_json::to_value(&entry)?,
				});
			}
			store.append_messages(&batch)?;
			cursor.leaf = parent;
			Ok(())
		})
	}

	fn incoming_digest(&self, message: &Message) -> u64 {
		match self.to_session_message(message) {
			Some(session) => session_digest(&session),
			// System/developer context is runtime-only but still occupies an
			// index in the compaction report; give it a stable revision too.
			None => xxh64(
				format!("{:?}\0{}", message.role, message.content).as_bytes(),
				0,
			),
		}
	}

	fn to_session_message(&self, message: &Message) -> Option<SessionMessage> {
		// Host-injected hidden guidance (goal continuation) is runtime-only and
		// never a user turn in the durable journal.
		if message.custom_type.is_some() {
			return None;
		}
		let role = match message.role {
			Role::User => "user",
			Role::Assistant => "assistant",
			Role::Tool => "toolResult",
			// The provider contract has no session entry for system/developer
			// guidance; it is runtime context, not a persisted turn.
			Role::System | Role::Developer => return None,
		};
		let now = SystemTime::now();
		let timestamp_ms = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
		let mut parts: Vec<SessionPart> = message
			.parts
			.iter()
			.filter_map(SessionPart::from_agent_part)
			.collect();
		if parts.is_empty() && !message.content.is_empty() {
			parts.push(SessionPart::text(message.content.clone()));
		}
		if role == "assistant"
			&& !message.tool_calls.is_empty()
			&& !parts.iter().any(|part| part.kind == "toolCall")
		{
			for call in &message.tool_calls {
				parts.push(SessionPart::tool_call(
					call.id.clone(),
					call.name.clone(),
					call.arguments.clone(),
				));
			}
		}
		let content = if parts.is_empty() {
			MessageContent::Text(message.content.clone())
		} else {
			MessageContent::Parts(parts)
		};
		let session_message = match role {
			"user" => SessionMessage::user(content, timestamp_ms),
			"assistant" => {
				let mut entry = SessionMessage::assistant(
					content,
					message.api.clone().unwrap_or_default(),
					message.provider.clone().unwrap_or_default(),
					message.model.clone().unwrap_or_default(),
					message.usage.map(Usage::from),
					message.stop_reason.map(StopReason::as_str).unwrap_or(if message
						.tool_calls
						.is_empty()
					{
						"stop"
					} else {
						"toolUse"
					}),
					timestamp_ms,
				);
				entry.error_message = message.error_message.clone();
				entry.disabled_features = message.disabled_features.clone();
				entry
			},
			_ => {
				let mut entry = SessionMessage::tool_result(
					message.tool_call_id.clone().unwrap_or_default(),
					message.name.clone().unwrap_or_else(|| "tool".to_string()),
					content,
					message.is_error,
					timestamp_ms,
				);
				entry
			},
		};
		Some(session_message)
	}

	fn to_message_entry(&self, message: &Message, leaf: Option<String>) -> Option<MessageEntry> {
		let session_message = self.to_session_message(message)?;
		let now = SystemTime::now();
		Some(MessageEntry::new(
			format!("msg-{}", output::session_id()),
			leaf,
			output::iso8601(now),
			session_message,
		))
	}

	fn to_tool_entry(
		&self,
		call: &ToolCall,
		result: &ToolResult,
		leaf: Option<String>,
	) -> MessageEntry {
		let now = SystemTime::now();
		let timestamp_ms = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
		let mut message = SessionMessage::tool_result(
			call.id.clone(),
			call.name.clone(),
			MessageContent::Text(result.content.clone()),
			result.is_error,
			timestamp_ms,
		);
		if let Some(details) = &result.details {
			message.extra.insert("details".to_string(), details.clone());
		}
		MessageEntry::new(
			format!("msg-{}", output::session_id()),
			leaf,
			output::iso8601(now),
			message,
		)
	}
}

#[async_trait]
impl DurableBoundary for DurableSession {
	async fn persist_messages(&self, messages: &[Message]) -> Result<(), DurableError> {
		let digests: Vec<u64> = messages
			.iter()
			.map(|message| self.incoming_digest(message))
			.collect();
		let start = {
			let mut tracked = lock(&self.tracked);
			if tracked.is_empty()
				&& let Some(seed) = lock(&self.resume_seed).take()
			{
				// Resume: the stored projected view is a prefix of the incoming
				// persisted-role messages. Align it by stable revision, not by
				// assuming the caller's history length.
				let mut matched: Vec<Option<String>> = vec![None; messages.len()];
				let mut seen = 0usize;
				let mut boundary = seed.is_empty().then_some(0);
				for (index, message) in messages.iter().enumerate() {
					if self.to_session_message(message).is_none() {
						continue;
					}
					let Some((revision, id)) = seed.get(seen) else {
						break;
					};
					if digests[index] != *revision {
						break;
					}
					matched[index] = id.clone();
					seen += 1;
					if seen == seed.len() {
						boundary = Some(index + 1);
						break;
					}
				}
				let Some(boundary) = boundary else {
					return Err(DurableError::new(
						"resume history does not match the durable branch revision",
					));
				};
				*tracked = digests[..boundary].to_vec();
				*lock(&self.tracked_ids) = matched[..boundary].to_vec();
			}
			let start = tracked.len();
			if messages.len() < start {
				return Err(DurableError::new(
					"conversation shrank without a compaction mapping",
				));
			}
			if tracked.as_slice() != &digests[..start] {
				return Err(DurableError::new(
					"conversation diverged from the durable branch; resync required",
				));
			}
			start
		};
		if start >= messages.len() {
			return Ok(());
		}

		let cursor = Arc::clone(&self.cursor);
		let tracked = Arc::clone(&self.tracked);
		let tracked_ids = Arc::clone(&self.tracked_ids);
		let mut written_tools = lock(&self.written_tools);
		let mut written_tool_ids = lock(&self.written_tool_ids);
		let leaf = lock(&self.cursor).leaf.clone();
		let mut entries = Vec::new();
		let mut tail_ids: Vec<Option<String>> = Vec::new();
		let mut parent = leaf;
		for message in &messages[start..] {
			if message.role == Role::Tool
				&& message.tool_call_id.as_deref().is_some_and(|id| {
					written_tools.get(id).is_some_and(|count| *count > 0)
				})
			{
				// The outcome write already stored this tool result, in order.
				// Consume one occurrence: a provider may reuse a tool-call id.
				let id = message.tool_call_id.as_deref().unwrap_or_default();
				if let Some(count) = written_tools.get_mut(id) {
					*count = count.saturating_sub(1);
				}
				tail_ids.push(
					written_tool_ids
						.get_mut(id)
						.and_then(|ids| ids.pop_front()),
				);
				continue;
			}
			let Some(entry) = self.to_message_entry(message, parent.clone()) else {
				tail_ids.push(None);
				continue;
			};
			parent = Some(entry.id.clone());
			tail_ids.push(Some(entry.id.clone()));
			entries.push(entry);
		}
		drop(written_tools);
		drop(written_tool_ids);
		let tail = digests[start..].to_vec();
		let appended_kept: Vec<String> = tail_ids.iter().flatten().cloned().collect();
		let session_id = self.session_id.clone();
		let branch = lock(&self.cursor).branch.clone();
		self
			.writer
			.call_async(move |store| {
				let mut batch = Vec::with_capacity(entries.len());
				let mut previous = lock(&cursor).leaf.clone();
				for mut entry in entries {
					entry.parent_id = previous.clone();
					batch.push(NewMessage {
						id: entry.id.clone(),
						session_id: session_id.clone(),
						branch_id: branch.clone(),
						parent_message_id: previous.clone(),
						payload: serde_json::to_value(&entry)?,
					});
					previous = Some(entry.id.clone());
				}
				if !batch.is_empty() {
					store.append_messages(&batch)?;
				}
				let mut state = lock(&cursor);
				state.leaf = previous;
				lock(&tracked).extend(tail);
				lock(&tracked_ids).extend(tail_ids);
				Ok(())
			})
			.await
			.map_err(|error| DurableError::new(error.to_string()))?;
		if !appended_kept.is_empty() {
			let projection = {
				let mut current = lock(&self.projection);
				current.as_mut().map(|projection| {
					projection.kept.extend(appended_kept);
					projection.clone()
				})
			};
			if let Some(projection) = projection {
				self.set_projection(Some(projection)).await?;
			}
		}
		Ok(())
	}

	async fn compacted(
		&self,
		messages: &[Message],
		report: &CompactionReport,
	) -> Result<(), DurableError> {
		let digests: Vec<u64> = messages
			.iter()
			.map(|message| self.incoming_digest(message))
			.collect();
		let tracked = lock(&self.tracked).clone();
		if report.old_len != tracked.len() {
			return Err(DurableError::new(format!(
				"compaction report covers {} messages but {} are tracked",
				report.old_len,
				tracked.len()
			)));
		}
		let tail = report
			.old_len
			.checked_sub(report.kept_from)
			.ok_or_else(|| DurableError::new("compaction report has an inverted range"))?;
		if report.kept_at.saturating_add(tail) != messages.len() {
			return Err(DurableError::new(
				"compaction report does not describe the compacted conversation",
			));
		}
		let tracked_ids = lock(&self.tracked_ids).clone();
		let mut kept = Vec::with_capacity(tail);
		let mut retained_ids: Vec<Option<String>> = vec![None; tail];
		for offset in 0..tail {
			let old_index = report.kept_from + offset;
			if digests[report.kept_at + offset] != tracked[old_index] {
				return Err(DurableError::new(
					"compaction retained a message that was never persisted",
				));
			}
			if let Some(id) = tracked_ids.get(old_index).cloned().flatten() {
				kept.push(id.clone());
				retained_ids[offset] = Some(id);
			}
		}
		// Runtime-only synthetic prefix (the compaction notice). The original
		// system prompt is re-injected by the next run, so it is not stored.
		let mut synthetic = Vec::new();
		for message in &messages[..report.kept_at] {
			if message.role == Role::System
				&& !message
					.content
					.starts_with(pi_agent::compaction::COMPACTION_NOTICE_PREFIX)
			{
				continue;
			}
			synthetic.push(
				serde_json::to_value(message)
					.map_err(|error| DurableError::new(error.to_string()))?,
			);
		}
		let projection = StoredProjection { kept, synthetic };
		self.set_projection(Some(projection.clone())).await?;
		let mut next_ids = vec![None; report.kept_at];
		next_ids.extend(retained_ids);
		if next_ids.len() != messages.len() {
			return Err(DurableError::new(
				"compaction projection does not cover the compacted conversation",
			));
		}
		*lock(&self.tracked) = digests;
		*lock(&self.tracked_ids) = next_ids;
		*lock(&self.projection) = Some(projection);
		Ok(())
	}

	async fn resync_messages(&self, messages: &[Message]) -> Result<(), DurableError> {
		let session_id = self.session_id.clone();
		let (branch_id, leaf, _entries, _all, branch_messages, _, projection) = self
			.writer
			.call_async(move |store| load_parts(store, &session_id))
			.await
			.map_err(|error| DurableError::new(error.to_string()))?;
		{
			let mut cursor = lock(&self.cursor);
			cursor.branch = branch_id;
			cursor.leaf = leaf;
		}
		// Existing tool rows are part of the seed prefix, not pending outcomes.
		lock(&self.written_tools).clear();
		lock(&self.written_tool_ids).clear();
		let ids = self.align_ids(messages, &branch_messages);
		*lock(&self.tracked) = messages
			.iter()
			.map(|message| self.incoming_digest(message))
			.collect();
		*lock(&self.tracked_ids) = ids;
		*lock(&self.resume_seed) = None;
		*lock(&self.projection) = projection;
		Ok(())
	}

	async fn begin_execution(&self, call: &ToolCall) -> Result<String, DurableError> {
		let session_id = self.session_id.clone();
		let branch_id = lock(&self.cursor).branch.clone();
		let payload = serde_json::json!({
			"arguments": call.arguments.clone(),
			"providerCallId": call.id.clone(),
		});
		self
			.writer
			.call_async(move |store| {
				store.record_intent(NewIntent {
					session_id,
					branch_id,
					provider_call_id: call.id.clone(),
					tool_name: call.name.clone(),
					payload,
				})
			})
			.await
			.map_err(|error| DurableError::new(error.to_string()))
	}

	async fn finish_execution(
		&self,
		execution_id: &str,
		call: &ToolCall,
		result: &ToolResult,
	) -> Result<(), DurableError> {
		let execution_id = execution_id.to_string();
		let cursor = Arc::clone(&self.cursor);
		let written_tools = Arc::clone(&self.written_tools);
		let written_tool_ids = Arc::clone(&self.written_tool_ids);
		let session_id = self.session_id.clone();
		let outcome = serde_json::json!({
			"content": result.content,
			"isError": result.is_error,
			"terminate": result.terminate,
			"details": result.details,
		});
		let call = call.clone();
		let result = result.clone();
		let tool_entry = {
			let leaf = lock(&cursor).leaf.clone();
			self.to_tool_entry(&call, &result, leaf)
		};
		let branch = lock(&cursor).branch.clone();
		self
			.writer
			.call_async(move |store| {
				store.record_outcome(&execution_id, outcome)?;
				let mut state = lock(&cursor);
				let mut tool_entry = tool_entry;
				tool_entry.parent_id = state.leaf.clone();
				store.append_message(NewMessage {
					id: tool_entry.id.clone(),
					session_id,
					branch_id: branch,
					parent_message_id: state.leaf.clone(),
					payload: serde_json::to_value(&tool_entry)?,
				})?;
				state.leaf = Some(tool_entry.id.clone());
				*lock(&written_tools).entry(call.id.clone()).or_insert(0) += 1;
				lock(&written_tool_ids)
					.entry(call.id.clone())
					.or_default()
					.push_back(tool_entry.id);
				Ok(())
			})
			.await
			.map_err(|error| DurableError::new(error.to_string()))
	}
}

fn load_parts(
	store: &mut SqliteStore,
	session_id: &str,
) -> StoreResult<(
	String,
	Option<String>,
	Vec<SessionEntry>,
	Vec<MessageEntry>,
	Vec<MessageEntry>,
	Option<SessionHeader>,
	Option<StoredProjection>,
)> {
	let record: SessionRecord = store
		.session(session_id)?
		.ok_or_else(|| StoreError::NotFound(format!("session {session_id} not found")))?;
	let projection = record
		.payload
		.get("durableProjection")
		.cloned()
		.and_then(|value| serde_json::from_value::<StoredProjection>(value).ok());
	let header = serde_json::from_value::<SessionHeader>(record.payload.clone()).ok();
	let branch_id = record
		.active_branch_id
		.ok_or_else(|| StoreError::Corrupt(format!("session {session_id} has no active branch")))?;
	let branch = store
		.branch(&branch_id)?
		.ok_or_else(|| StoreError::Corrupt(format!("branch {branch_id} not found")))?;
	let leaf = branch.leaf_message_id;
	let rows = store.session_messages(session_id)?;
	let mut entries = Vec::with_capacity(rows.len());
	let mut messages = Vec::new();
	for row in rows {
		if let Ok(entry) = serde_json::from_value::<SessionEntry>(row.payload.clone()) {
			entries.push(entry);
		}
		if let Ok(message) = serde_json::from_value::<MessageEntry>(row.payload) {
			messages.push(message);
		}
	}
	let mut branch_messages = Vec::new();
	for row in store.branch_messages(&branch_id)? {
		if let Ok(message) = serde_json::from_value::<MessageEntry>(row.payload) {
			branch_messages.push(message);
		}
	}
	derive_turn_history(&branch_messages).map_err(StoreError::Corrupt)?;
	Ok((branch_id, leaf, entries, messages, branch_messages, header, projection))
}

fn session_digest(message: &SessionMessage) -> u64 {
	// Revision identity is the replay-visible content only. Provider metadata
	// (api/model/usage/attribution/stopReason/disabledFeatures) is not restored
	// by every resume path, so including it would make a valid replay diverge.
	let canonical = serde_json::json!({
		"role": message.role,
		"content": message.content,
		"toolCallId": message.tool_call_id,
		"toolName": message.tool_name,
		"isError": message.is_error,
	});
	xxh64(&serde_json::to_vec(&canonical).unwrap_or_default(), 0)
}

/// Seed the resume cursor. A persisted projection selects the retained archive
/// rows (and omits the synthetic notice, which is runtime-only and replayed by
/// `context_messages`); otherwise the whole active branch is the seed.
fn seed_entries(
	branch: &[MessageEntry],
	projection: Option<&StoredProjection>,
	all: &[MessageEntry],
) -> Vec<(u64, Option<String>)> {
	let source: Vec<MessageEntry> = match projection {
		Some(projection) => {
			let by_id: HashMap<&str, &MessageEntry> =
				all.iter().map(|entry| (entry.id.as_str(), entry)).collect();
			projection
				.kept
				.iter()
				.filter_map(|id| by_id.get(id.as_str()).map(|entry| (*entry).clone()))
				.collect()
		},
		None => branch.to_vec(),
	};
	// Use the exact provider-replay projection: terminal error/aborted turns
	// and their paired tool results stay archived but are not part of the seed.
	crate::session::replayable_message_entries(&source)
		.into_iter()
		.filter(|entry| {
			matches!(entry.message.role.as_str(), "user" | "assistant" | "toolResult")
		})
		.map(|entry| (session_digest(&entry.message), Some(entry.id.clone())))
		.collect()
}

fn root_branch_id(session_id: &str) -> String {
	format!("{session_id}.main")
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
	mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One stored session as ACP listing/export sees it.
#[derive(Debug, Clone)]
pub struct StoredSessionSummary {
	pub id: String,
	pub cwd: String,
	pub title: Option<String>,
	pub modified_ms: i64,
	pub message_count: usize,
}

/// List every session in one project's database, newest first. Returns an
/// empty list when the database does not exist; read-only.
pub fn list_session_summaries(dir: &Path) -> StoreResult<Vec<StoredSessionSummary>> {
	let db_path = database_path(dir);
	if !db_path.exists() {
		return Ok(Vec::new());
	}
	let store = SqliteStore::open(&db_path)?;
	let mut summaries = Vec::new();
	for record in store.list_sessions()? {
		let header = serde_json::from_value::<SessionHeader>(record.payload.clone()).ok();
		let message_count = record
			.active_branch_id
			.as_deref()
			.and_then(|branch| store.branch_messages(branch).ok())
			.map(|messages| messages.len())
			.unwrap_or_default();
		summaries.push(StoredSessionSummary {
			id: record.id,
			cwd: header
				.as_ref()
				.map(|header| header.cwd.clone())
				.unwrap_or_default(),
			title: header.and_then(|header| header.title),
			modified_ms: record.updated_at_ms,
			message_count,
		});
	}
	Ok(summaries)
}

/// Virtual per-session path for a durable id. The content lives in the shared
/// database; this path keeps artifact directories and CLI arguments stable.
#[must_use]
pub fn session_path(dir: &Path, id: &str) -> PathBuf {
	dir.join(format!("{id}.jsonl"))
}

/// Whether the database already contains this session. Opens (and if needed
/// creates) the schema only when the file exists; a missing file is answered
/// without creating one.
pub fn session_exists(db_path: &Path, session_id: &str) -> StoreResult<bool> {
	if !db_path.exists() {
		return Ok(false);
	}
	let store = SqliteStore::open(db_path)?;
	Ok(store.session(session_id)?.is_some())
}

/// Newest stored session id for `--continue`. Read-only query on the same
/// authority; the writer thread is not involved and nothing is created when
/// the database is absent.
pub fn latest_session_id(db_path: &Path) -> StoreResult<Option<String>> {
	if !db_path.exists() {
		return Ok(None);
	}
	let connection =
		rusqlite::Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
	let id = connection
		.query_row(
			"SELECT id FROM sessions ORDER BY updated_at_ms DESC, created_at_ms DESC LIMIT 1",
			[],
			|row| row.get::<_, String>(0),
		)
		.optional()?;
	Ok(id)
}

/// Database path for one project's sessions. A single file is the authority for
/// every session in that directory.
#[must_use]
pub fn database_path(session_dir: &Path) -> PathBuf {
	session_dir.join("sessions.db")
}
