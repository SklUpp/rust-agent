//! Hub park/revive delegation and the `history://` protocol handler.
//!
//! This module is the only bridge between the hub tool and the lifecycle
//! manager. It keeps `hub.rs` to two narrow calls:
//!
//! - `roster` merges persisted parked/idle rows into `hub list` (including rows
//!   restored after a process restart).
//! - `delegate_send` hands a message to a lifecycle-managed agent: an idle
//!   live actor is woken on its own state (`woken`), a running one gets a
//!   non-interrupting aside (`injected`), and a parked row is revived when
//!   needed (`revived`).
//!
//! `history://` is the read-only transcript surface (upstream
//! `internal-urls/history-protocol.ts`):
//!
//! - `history://` lists the caller session's agents (id/status/kind/parent/last
//!   activity) plus transcripts found under the caller's own scope.
//! - `history://<id>` renders a concise markdown transcript, never raw JSONL.
//!
//! Authorization is caller-first: every resolve pins the caller's existing
//! roots through [`PinnedDir`] (component-by-component `O_NOFOLLOW` from `/`),
//! and the manager is visible only when its root is one of those pinned roots.
//! Live sessions read a producer-fed, manager-owned message snapshot until the
//! producer journals it; parked/tombstoned/disk rows read the transcript leaf
//! descriptor-relative with `O_NOFOLLOW` and parse that same descriptor.

use std::{
	collections::HashSet,
	ffi::OsString,
	path::{Component, Path, PathBuf},
	sync::Arc,
	time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;

use super::{
	agent_output::ADVISOR_OUTPUT_STEM,
	history_files::{EntryKind, PinnedDir},
	history_format::format_history_markdown,
	internal_urls::{
		InternalResource, InternalUrl, InternalUrlRouter, ProtocolHandler, ResolveContext,
	},
};
use crate::{
	CancellationToken,
	subagent_lifecycle::{
		DeliveryOutcome, HistoryRow, LifecycleRow, LifecycleStatus, SubagentLifecycleManager, lookup,
	},
	supervisor::{ChildStatus, Supervisor},
	task_persisted_revive::{read_transcript_file, transcript_path as child_transcript_path},
};

/// Deepest transcript directory scan.
///
/// Exact TS parity: `packages/coding-agent/src/internal-urls/registry-helpers.ts`
/// `sessionFilesFromDisk` walks at most 8 directories below an artifacts dir.
const MAX_SCAN_DEPTH: usize = 8;

/// Parked/idle roster rows for this supervisor, or an empty list when no
/// lifecycle manager is attached.
#[must_use]
pub fn roster(supervisor: &Arc<Supervisor>) -> Vec<LifecycleRow> {
	lookup(supervisor).map_or_else(Vec::new, |manager| manager.roster())
}

/// Delegate a send to the lifecycle manager.
///
/// `None` means this target is not lifecycle-managed, so the caller keeps its
/// existing live-mailbox path. `Some` reports the real delivery outcome or the
/// exact refusal (tombstone, missing contract, gone workspace).
#[must_use]
pub fn delegate_send(
	supervisor: &Arc<Supervisor>,
	to: &str,
	text: String,
) -> Option<Result<DeliveryOutcome, String>> {
	let manager = lookup(supervisor)?;
	if !manager.knows(to) {
		return None;
	}
	Some(manager.send(to, text))
}

/// One visible history entry: a manager row, a supervisor child, or a
/// transcript file found under the caller's pinned scope.
#[derive(Debug, Clone)]
struct Candidate {
	id:            String,
	status:        String,
	kind:          String,
	parent:        String,
	last_activity: Option<u64>,
	display:       PathBuf,
	root:          usize,
	relative:      Vec<OsString>,
	/// This row is a real live session with a manager-owned, producer-fed
	/// snapshot; only then may the handler serve memory instead of the file.
	live:          bool,
}

fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_millis() as u64)
		.unwrap_or_default()
}

/// Humanize a last-activity timestamp as `Ns/Nm/Nh/Nd ago`.
fn format_ago(timestamp: u64) -> String {
	let diff_ms = now_ms().saturating_sub(timestamp);
	let secs = diff_ms / 1000;
	if secs < 60 {
		return format!("{secs}s ago");
	}
	let mins = secs / 60;
	if mins < 60 {
		return format!("{mins}m ago");
	}
	let hours = mins / 60;
	if hours < 24 {
		return format!("{hours}h ago");
	}
	format!("{}d ago", hours / 24)
}

fn child_status(status: ChildStatus) -> &'static str {
	match status {
		ChildStatus::Running => "running",
		ChildStatus::Completed => "completed",
		ChildStatus::Failed => "failed",
		ChildStatus::Aborted => "aborted",
	}
}

fn is_advisor(id: &str) -> bool {
	id.starts_with(ADVISOR_OUTPUT_STEM)
}

fn absolute_path(path: &Path, cwd: &Path) -> PathBuf {
	if path.is_absolute() {
		path.to_path_buf()
	} else {
		cwd.join(path)
	}
}

/// Child transcript roots the caller may read, caller-first and lexical.
///
/// Session-file roots are `<artifacts>/subagents`. Every explicit `local_roots`
/// entry is itself an authorized caller root (plus its `subagents` child), so a
/// test/embedder scope must name the root it intends to expose.
fn caller_root_paths(context: &ResolveContext) -> Vec<PathBuf> {
	fn push(roots: &mut Vec<PathBuf>, path: PathBuf) {
		if !path.as_os_str().is_empty() && !roots.contains(&path) {
			roots.push(path);
		}
	}
	let mut roots = Vec::new();
	if let Some(session_file) = &context.session_file {
		let session_file = absolute_path(session_file, &context.cwd);
		let parent_is_subagents = session_file
			.parent()
			.and_then(Path::file_name)
			.and_then(|name| name.to_str())
			== Some("subagents");
		if parent_is_subagents {
			if let Some(parent) = session_file.parent() {
				push(&mut roots, parent.to_path_buf());
			}
		} else {
			let artifacts = session_file.with_extension("");
			if artifacts.file_name().and_then(|name| name.to_str()) == Some("subagents") {
				push(&mut roots, artifacts);
			} else {
				push(&mut roots, artifacts.join("subagents"));
			}
		}
	}
	for root in context.local_roots.values() {
		let root = absolute_path(root, &context.cwd);
		push(&mut roots, root.clone());
		push(&mut roots, root.join("subagents"));
	}
	roots
}

/// Pin every authorized root, dropping roots that are missing or contain a
/// symlink at any component. No canonicalize fallback exists.
fn pin_scope_roots(context: &ResolveContext) -> Vec<PinnedDir> {
	let mut pinned: Vec<PinnedDir> = Vec::new();
	for path in caller_root_paths(context) {
		let Ok(root) = PinnedDir::open(&path) else {
			continue;
		};
		if !pinned.iter().any(|existing| existing.same_file(&root)) {
			pinned.push(root);
		}
	}
	pinned
}

/// Relative components when `file` is lexically under `root`, rejecting any
/// `..`/root/prefix component instead of normalizing it away.
fn contained_components(root: &Path, file: &Path) -> Option<Vec<OsString>> {
	let relative = file.strip_prefix(root).ok()?;
	let mut components = Vec::new();
	for component in relative.components() {
		match component {
			Component::Normal(name) => components.push(name.to_os_string()),
			Component::CurDir => {},
			Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
		}
	}
	(!components.is_empty()).then_some(components)
}

fn contained_candidate(roots: &[PinnedDir], candidate: CandidateSeed) -> Option<Candidate> {
	let (root, relative) = roots.iter().enumerate().find_map(|(index, root)| {
		contained_components(root.display(), &candidate.display).map(|relative| (index, relative))
	})?;
	Some(Candidate {
		id: candidate.id,
		status: candidate.status,
		kind: candidate.kind,
		parent: candidate.parent,
		last_activity: candidate.last_activity,
		display: candidate.display,
		root,
		relative,
		live: candidate.live,
	})
}

struct CandidateSeed {
	id:            String,
	status:        String,
	kind:          String,
	parent:        String,
	last_activity: Option<u64>,
	display:       PathBuf,
	live:          bool,
}

/// Read-only `history://` handler for one session's lifecycle manager.
pub struct HistoryProtocolHandler {
	manager: Arc<SubagentLifecycleManager>,
	#[cfg(test)]
	open_hook: Option<Arc<dyn Fn(&Path) + Send + Sync>>,
}

impl HistoryProtocolHandler {
	#[must_use]
	pub fn new(manager: Arc<SubagentLifecycleManager>) -> Self {
		Self {
			manager,
			#[cfg(test)]
			open_hook: None,
		}
	}

	#[cfg(test)]
	#[must_use]
	pub(crate) fn with_open_hook(
		mut self,
		hook: Arc<dyn Fn(&Path) + Send + Sync>,
	) -> Self {
		self.open_hook = Some(hook);
		self
	}

	/// The bound manager is visible only when its root is one of the caller's
	/// pinned roots. A caller-root symlink therefore denies manager/live rows
	/// before any snapshot is considered.
	fn manager_visible(&self, context: &ResolveContext, roots: &[PinnedDir]) -> bool {
		let Some(root) = self.manager.root() else {
			return false;
		};
		let path = absolute_path(root, &context.cwd);
		let Ok(pinned) = PinnedDir::open(&path) else {
			return false;
		};
		roots.iter().any(|candidate| candidate.same_file(&pinned))
	}

	fn show_manager_row(roots: &[PinnedDir], row: HistoryRow) -> Option<Candidate> {
		if is_advisor(&row.id) {
			return None;
		}
		contained_candidate(
			roots,
			CandidateSeed {
				display: row.session_file,
				id: row.id,
				status: row.status.as_str().to_string(),
				kind: row.agent.unwrap_or_else(|| "—".to_string()),
				parent: row.parent_id.unwrap_or_else(|| "—".to_string()),
				last_activity: (row.last_activity > 0).then_some(row.last_activity),
				live: matches!(
					row.status,
					LifecycleStatus::Running | LifecycleStatus::Idle
				),
			},
		)
	}

	fn show_child_row(
		&self,
		context: &ResolveContext,
		roots: &[PinnedDir],
		record: crate::supervisor::ChildRecord,
	) -> Option<Candidate> {
		if is_advisor(&record.id) {
			return None;
		}
		let display = record
			.session_file
			.clone()
			.or_else(|| {
				self.manager
					.root()
					.map(|root| child_transcript_path(root, &record.id))
			})
			.map(|path| absolute_path(&path, &context.cwd))?;
		contained_candidate(
			roots,
			CandidateSeed {
				id: record.id,
				status: child_status(record.status).to_string(),
				kind: record.agent,
				parent: record.parent_id.unwrap_or_else(|| "—".to_string()),
				last_activity: record.started_at,
				display,
				live: record.status == ChildStatus::Running,
			},
		)
	}

	/// Manager/child/disk rows for this call, caller-scoped and deduplicated.
	fn candidates(
		&self,
		context: &ResolveContext,
	) -> Result<(Vec<Candidate>, Vec<PinnedDir>), String> {
		let signal = context.signal.clone();
		if signal.is_cancelled() {
			return Err("aborted".to_string());
		}
		let roots = pin_scope_roots(context);
		let visible = self.manager_visible(context, &roots);
		let mut candidates: Vec<Candidate> = Vec::new();
		let mut seen: HashSet<String> = HashSet::new();
		if visible {
			for row in self.manager.history_rows() {
				let Some(candidate) = Self::show_manager_row(&roots, row) else {
					continue;
				};
				if seen.insert(candidate.id.clone()) {
					candidates.push(candidate);
				}
			}
			for record in self.manager.child_records() {
				let Some(candidate) = self.show_child_row(context, &roots, record) else {
					continue;
				};
				if seen.insert(candidate.id.clone()) {
					candidates.push(candidate);
				}
			}
		}
		for (index, root) in roots.iter().enumerate() {
			scan_transcripts(root, index, &signal, &mut candidates, &mut seen)?;
		}
		Ok((candidates, roots))
	}

	fn render_index(&self, url: &InternalUrl, candidates: &[Candidate]) -> InternalResource {
		let mut lines: Vec<String> = vec!["# Agents".to_string(), String::new()];
		if candidates.is_empty() {
			lines.push("No agents registered.".to_string());
		} else {
			lines.push("| id | status | kind | parent | last activity |".to_string());
			lines.push("|---|---|---|---|---|".to_string());
			for candidate in candidates {
				let last_activity = candidate
					.last_activity
					.map(format_ago)
					.unwrap_or_else(|| "—".to_string());
				lines.push(format!(
					"| {} | {} | {} | {} | {} |",
					candidate.id,
					candidate.status,
					candidate.kind,
					candidate.parent,
					last_activity
				));
			}
			lines.push(String::new());
			lines.push("Read a transcript with `read history://<id>`.".to_string());
		}
		let content = format!("{}\n", lines.join("\n"));
		let mut resource = InternalResource::text(&url.href, content);
		resource.content_type = "text/markdown".to_string();
		resource
	}

	fn source_note(candidate: &Candidate) -> String {
		match candidate.status.as_str() {
			"on disk" => "Source: session file (read-only, on disk)".to_string(),
			"tombstoned" => "Source: session file (read-only, tombstoned)".to_string(),
			status => format!("Source: transcript file (read-only, {status})"),
		}
	}
}

/// Scan one pinned root. Names come from the pinned descriptor; directories are
/// opened relative with `O_NOFOLLOW`; symlinked dirs/leaves are never followed.
fn scan_transcripts(
	root: &PinnedDir,
	root_index: usize,
	signal: &CancellationToken,
	candidates: &mut Vec<Candidate>,
	seen: &mut HashSet<String>,
) -> Result<(), String> {
	fn walk(
		root: &PinnedDir,
		root_index: usize,
		depth: usize,
		relative: &[OsString],
		signal: &CancellationToken,
		candidates: &mut Vec<Candidate>,
		seen: &mut HashSet<String>,
	) -> Result<(), String> {
		if signal.is_cancelled() {
			return Err("aborted".to_string());
		}
		let names = root.list().map_err(|error| {
			format!("history:// cannot scan {}: {error}", root.display().display())
		})?;
		for name in names {
			if signal.is_cancelled() {
				return Err("aborted".to_string());
			}
			let Ok(kind) = root.kind(&name) else {
				continue;
			};
			let mut entry_relative = relative.to_vec();
			entry_relative.push(name.clone());
			match kind {
				EntryKind::Dir => {
					if depth >= MAX_SCAN_DEPTH {
						continue;
					}
					let Ok(child) = root.open_dir(&name) else {
						continue;
					};
					walk(
						&child,
						root_index,
						depth + 1,
						&entry_relative,
						signal,
						candidates,
						seen,
					)?;
				},
				EntryKind::File => {
					let Some(name_str) = name.to_str() else {
						continue;
					};
					let Some(id) = name_str.strip_suffix(".jsonl") else {
						continue;
					};
					if id.is_empty() || is_advisor(id) || !seen.insert(id.to_string()) {
						continue;
					}
					candidates.push(Candidate {
						id: id.to_string(),
						status: "on disk".to_string(),
						kind: "—".to_string(),
						parent: "—".to_string(),
						last_activity: None,
						display: root.display().join(&name),
						root: root_index,
						relative: entry_relative,
						live: false,
					});
				},
				EntryKind::Other => {},
			}
		}
		Ok(())
	}

	walk(root, root_index, 0, &[], signal, candidates, seen)
}

#[async_trait]
impl ProtocolHandler for HistoryProtocolHandler {
	fn scheme(&self) -> &str {
		"history"
	}

	/// Transcripts are append-only data: a snapshot may change between reads
	/// even though this scheme never accepts a write. The resource flag stays
	/// `false` (upstream parity), while `read` suppresses hashline anchors for
	/// `history://` because there is no edit path back through the URI.
	fn immutable(&self) -> bool {
		false
	}

	async fn resolve(
		&self,
		url: &InternalUrl,
		context: &ResolveContext,
	) -> Result<InternalResource, String> {
		if context.signal.is_cancelled() {
			return Err("aborted".to_string());
		}
		let raw = if url.host.is_empty() {
			url.path.trim_start_matches('/')
		} else {
			url.host.as_str()
		};
		let id = raw
			.split(['?', '#'])
			.next()
			.unwrap_or_default()
			.trim()
			.to_string();
		let (candidates, roots) = self.candidates(context)?;
		if id.is_empty() {
			return Ok(self.render_index(url, &candidates));
		}

		let lower = id.to_lowercase();
		let ordered: Vec<&Candidate> = candidates
			.iter()
			.filter(|candidate| candidate.id == id)
			.chain(
				candidates
					.iter()
					.filter(|candidate| candidate.id != id && candidate.id.to_lowercase() == lower),
			)
			.collect();
		let Some(candidate) = ordered.first().copied() else {
			let known: Vec<&str> = candidates.iter().map(|candidate| candidate.id.as_str()).collect();
			let known = if known.is_empty() {
				"none".to_string()
			} else {
				known.join(", ")
			};
			return Err(format!(
				"Unknown agent: {id}\nKnown agents: {known}\nList all with history://"
			));
		};

		if candidate.live
			&& let Some(messages) = self.manager.live_messages(&candidate.id)
		{
			if context.signal.is_cancelled() {
				return Err("aborted".to_string());
			}
			let title = format!("{} ({})", candidate.id, candidate.status);
			let content = format_history_markdown(&title, &messages);
			let mut resource = InternalResource::text(&url.href, content);
			resource.content_type = "text/markdown".to_string();
			resource.source_path = Some(candidate.display.clone());
			resource.notes = Some(vec![format!(
				"Source: live session (read-only, {})",
				candidate.status
			)]);
			return Ok(resource);
		}
		if context.signal.is_cancelled() {
			return Err("aborted".to_string());
		}
		#[cfg(test)]
		if let Some(hook) = &self.open_hook {
			hook(&candidate.display);
		}
		let display = candidate.display.clone();
		let file = roots[candidate.root]
			.open_relative(&candidate.relative)
			.map_err(|error| {
				if error.kind() == std::io::ErrorKind::NotFound {
					format!(
						"Agent {} has no transcript: session is gone and no session file was retained",
						candidate.id
					)
				} else {
					format!("history://{}: cannot read transcript: {error}", candidate.id)
				}
			})?;
		let messages = tokio::task::spawn_blocking(move || read_transcript_file(file))
			.await
			.map_err(|error| format!("history://{}: transcript read task failed: {error}", candidate.id))?
			.map_err(|error| format!("history://{}: {error}", candidate.id))?;

		let title = format!("{} ({})", candidate.id, candidate.status);
		let content = format_history_markdown(&title, &messages);
		let mut resource = InternalResource::text(&url.href, content);
		resource.content_type = "text/markdown".to_string();
		resource.source_path = Some(display);
		resource.notes = Some(vec![Self::source_note(candidate)]);
		Ok(resource)
	}
}

/// Mount `history://` on a session router.
///
/// The scheme is reserved: a host URI bridge can never register its own
/// `history://` and shadow the built-in transcripts.
pub fn register_history(
	router: &InternalUrlRouter,
	manager: Arc<SubagentLifecycleManager>,
) -> Result<(), String> {
	router
		.register_core(Arc::new(HistoryProtocolHandler::new(manager)))
		.map(|_| ())
}

/// Transcript path for an id, used by tests/embedders that read the raw JSONL.
#[must_use]
pub fn transcript_path(supervisor: &Arc<Supervisor>, id: &str) -> Option<PathBuf> {
	lookup(supervisor)?.transcript_path(id)
}

#[cfg(all(test, unix))]
mod tests {
	use std::{
		os::unix::fs::symlink,
		path::{Path, PathBuf},
		sync::Arc,
		time::{SystemTime, UNIX_EPOCH},
	};

	use async_trait::async_trait;

	use super::*;
	use crate::{
		provider::{ChunkStream, ModelProvider, ProviderError, ToolChoice},
		state::Message,
		subagent_lifecycle::attach,
		supervisor::{DelegationLimits, Supervisor},
		task_persisted_revive::{append_messages, DefaultPersistedReviverFactory},
		tools::{
			internal_urls::{InternalUrlRouter, UrlScope},
			registry::ToolRegistry,
		},
	};

	struct NoopProvider;

	#[async_trait]
	impl ModelProvider for NoopProvider {
		async fn complete(
			&self,
			_messages: &[Message],
			_tools: &[serde_json::Value],
			_tool_choice: &ToolChoice,
		) -> Result<Message, ProviderError> {
			Err(ProviderError::Protocol("unused".to_string()))
		}

		async fn stream(
			&self,
			_messages: &[Message],
			_tools: &[serde_json::Value],
			_tool_choice: &ToolChoice,
		) -> Result<ChunkStream, ProviderError> {
			Err(ProviderError::Protocol("unused".to_string()))
		}
	}

	fn temp_root(label: &str) -> PathBuf {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map(|duration| duration.as_nanos())
			.unwrap_or_default();
		let home = std::env::var_os("HOME").expect("HOME");
		let path = PathBuf::from(home).join(format!(
			".omp-history-swap-{label}-{}-{nanos}",
			std::process::id()
		));
		std::fs::create_dir_all(path.join("session").join("subagents")).expect("fixture");
		path
	}

	/// The actual handler read path must refuse a leaf swapped for a symlink
	/// after the scan, not just a helper called in isolation.
	#[tokio::test]
	async fn swapped_leaf_is_denied_through_handler() {
		let base = temp_root("leaf");
		let subagents = base.join("session").join("subagents");
		let outside = base.join("outside.jsonl");
		append_messages(&outside, &[&Message::user("outside-secret")]);
		let leaf = subagents.join("task-1.jsonl");
		append_messages(&leaf, &[&Message::user("inside-body")]);

		let provider: Arc<dyn ModelProvider> = Arc::new(NoopProvider);
		let supervisor = Supervisor::new(
			ToolRegistry::with_builtins(),
			provider.clone(),
			None,
			DelegationLimits::default(),
		);
		supervisor.set_child_session_root(Some(subagents.clone()));
		let manager = attach(
			&supervisor,
			0,
			Arc::new(DefaultPersistedReviverFactory::new(
				provider,
				ToolRegistry::with_builtins(),
			)),
		);
		let swap = Arc::new(move |path: &Path| {
			let _ = std::fs::remove_file(path);
			let _ = symlink(&outside, path);
		});
		let handler = HistoryProtocolHandler::new(manager).with_open_hook(swap);
		let router = InternalUrlRouter::new();
		router
			.register_core(Arc::new(handler))
			.expect("history handler");
		let mut scope = UrlScope::default();
		scope.session_file = Some(base.join("session.jsonl"));
		let context = scope.resolve_context(&base, &CancellationToken::new());
		let error = router
			.resolve("history://task-1", &context)
			.await
			.expect_err("swapped leaf");
		assert!(!error.contains("outside-secret"), "{error}");
		assert!(
			error.contains("cannot read transcript"),
			"handler must fail at the leaf open: {error}"
		);
		let _ = std::fs::remove_dir_all(&base);
	}
}
