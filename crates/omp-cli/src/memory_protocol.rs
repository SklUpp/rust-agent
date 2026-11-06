//! `memory://` protocol handler for the local backend.
//!
//! Anchors: `packages/coding-agent/src/internal-urls/memory-protocol.ts`
//! (`MemoryProtocolHandler`, `resolveMemoryUrlToPath`, `tryResolveInRoot`).
//!
//! Namespaces:
//! - `root` — the calling session's memory directory
//!   (`local_memory::memory_root`): legacy/derived artifacts such as
//!   `memory_summary.md` and `learned.md`. Two sessions with different cwds
//!   never read each other's artifacts. A bare `memory://root` means
//!   `memory_summary.md`, exactly like TS. Every path is percent-decoded,
//!   traversal-checked and canonicalised under the root.
//! - `list` — records visible to this project (global + this cwd), newest
//!   first, with durable ids.
//! - `search/<query>` — bounded lexical recall (global + this cwd).
//! - `<id>` — one SQLite memory record (`crate::memory_store`), rendered with
//!   provenance. This namespace accepts writes: writing the body replaces the
//!   stored text and refreshes the derived `learned.md` export.
//!
//! `memory://root` stays read-only: deletion is an explicit store/CLI
//! operation, never a side effect of writing a file. Mnemopi ids and Hindsight
//! pointers are not implemented here.

use std::{
	fs,
	io::Read as _,
	path::{Path, PathBuf},
};

use async_trait::async_trait;
use pi_agent::tools::internal_urls::{
	InternalResource, InternalUrl, ProtocolHandler, ResolveContext, WriteContext,
};

use crate::{
	autolearn::MemoryBackend,
	local_memory,
	memory_store::{self, MemoryRecord},
	skill_protocol::{content_type_for, decode_percent, validate_relative_path},
};

/// Scheme served by [`MemoryUrlHandler`].
pub const MEMORY_SCHEME: &str = "memory";

const MEMORY_NAMESPACE: &str = "root";
const LIST_NAMESPACE: &str = "list";
const SEARCH_NAMESPACE: &str = "search";
const DEFAULT_MEMORY_FILE: &str = "memory_summary.md";

/// `memory://` handler bound to one session's agent dir.
///
/// `root` reads legacy file artifacts; record ids read and write the SQLite
/// memory store. The file namespace remains the export/import surface.
pub struct MemoryUrlHandler {
	agent_dir: PathBuf,
	backend:   MemoryBackend,
}

impl MemoryUrlHandler {
	#[must_use]
	pub fn new(agent_dir: PathBuf, backend: MemoryBackend) -> Self {
		Self { agent_dir, backend }
	}

	/// `list`, `search/<query>`, and record-id namespaces backed by the SQLite
	/// store. `root` is handled by the file path in [`ProtocolHandler::resolve`].
	fn resolve_record_namespace(
		&self,
		url: &InternalUrl,
		namespace: &str,
		context: &ResolveContext,
	) -> Result<InternalResource, String> {
		let relative = url.path.trim_start_matches('/');
		let mut resource = match namespace {
			LIST_NAMESPACE if relative.is_empty() => {
				let records = memory_store::list_visible(&self.agent_dir, &context.cwd)
					.map_err(|error| error.to_string())?;
				InternalResource::text(&url.href, render_record_list(&records))
			},
			SEARCH_NAMESPACE if !relative.is_empty() => {
				let query = decode_percent(relative);
				let records = memory_store::recall_visible(&self.agent_dir, &context.cwd, &query, None)
					.map_err(|error| error.to_string())?;
				InternalResource::text(&url.href, render_record_list(&records))
			},
			record if relative.is_empty() && memory_store::is_record_id(record) => {
				let store = memory_store::MemoryStore::open_existing(&self.agent_dir)
					.map_err(|error| error.to_string())?;
				let cwd = context.cwd.to_string_lossy();
				let record = store
					.get(namespace, Some(cwd.as_ref()))
					.map_err(|error| error.to_string())?;
				InternalResource::text(&url.href, render_record(&record))
			},
			_ => {
				return Err(format!(
					"Unknown memory namespace: {namespace}. Supported: root (file-backed artifacts), \
					 list, search/<query>, or a memory record id (memory://<id>)."
				));
			},
		};
		resource.content_type = "text/markdown".to_string();
		resource.size = resource.content.len();
		Ok(resource)
	}
}

#[async_trait]
impl ProtocolHandler for MemoryUrlHandler {
	fn scheme(&self) -> &str {
		MEMORY_SCHEME
	}

	fn immutable(&self) -> bool {
		// Records are store rows, not files: the `read` tool renders them without
		// line anchors. Write capability is reported separately by `writable`.
		true
	}

	fn writable(&self) -> bool {
		self.backend == MemoryBackend::Local
	}

	async fn write(
		&self,
		url: &InternalUrl,
		content: &str,
		context: &WriteContext,
	) -> Result<(), String> {
		if context.signal.is_cancelled() {
			return Err("aborted".to_string());
		}
		if self.backend != MemoryBackend::Local {
			return Err("Unknown protocol: memory://".to_string());
		}
		let namespace = url.host.as_str();
		if namespace.is_empty() {
			return Err("memory:// write requires a record id: memory://<id>".to_string());
		}
		let relative = url.path.trim_start_matches('/');
		if namespace == MEMORY_NAMESPACE || !relative.is_empty() {
			return Err(format!(
				"{} is not a writable memory record. memory://root is a read-only export; edit memory://<id> instead.",
				url.href
			));
		}
		if !memory_store::is_record_id(namespace) {
			return Err(format!(
				"Unknown memory namespace: {namespace}. Only memory://<id> accepts a body."
			));
		}
		// Edit only: identity (scope, cwd, origin, actor, source) is immutable.
		// The derived `learned.md` export is refreshed from the store.
		memory_store::update_record_and_export(&self.agent_dir, &context.cwd, namespace, content)
			.map_err(|error| error.to_string())?;
		Ok(())
	}

	async fn resolve(
		&self,
		url: &InternalUrl,
		context: &ResolveContext,
	) -> Result<InternalResource, String> {
		if context.signal.is_cancelled() {
			return Err("aborted".to_string());
		}
		// TS `memoryBackendFromContext`: with the backend off the scheme is not
		// mounted at all, so the registry reports it as unknown.
		if self.backend != MemoryBackend::Local {
			return Err("Unknown protocol: memory://".to_string());
		}
		let namespace = url.host.as_str();
		if namespace.is_empty() {
			return Err(
				"memory:// URL requires a namespace: memory://root, memory://list, memory://search/<query>, or memory://<id>"
					.to_string(),
			);
		}
		if namespace != MEMORY_NAMESPACE {
			return self.resolve_record_namespace(url, namespace, context);
		}

		// Root is per calling session cwd (`memoryRootsForContext`), not global.
		let lexical_root = local_memory::memory_root(&self.agent_dir, &context.cwd);
		let root = match fs::canonicalize(&lexical_root) {
			Ok(root) => root,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Err(
					"Memory artifacts are not available for this project yet. Run a session with memories enabled first."
						.to_string(),
				);
			},
			Err(error) => return Err(format!("{}: {error}", lexical_root.display())),
		};

		let relative = url.path.trim_start_matches('/');
		let target = if relative.is_empty() {
			root.join(DEFAULT_MEMORY_FILE)
		} else {
			let relative = decode_percent(relative);
			validate_relative_path_memory(&relative)?;
			root.join(relative)
		};
		let canonical = match fs::canonicalize(&target) {
			Ok(path) => path,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Err(format!("Memory file not found: {}", url.href));
			},
			Err(error) => return Err(format!("{}: {error}", target.display())),
		};
		if !canonical.starts_with(&root) {
			return Err("memory:// URL escapes memory root".to_string());
		}

		let metadata = fs::metadata(&canonical).map_err(|error| format!("{}: {error}", canonical.display()))?;
		if metadata.is_dir() {
			let mut entries = fs::read_dir(&canonical)
				.map_err(|error| format!("{}: {error}", canonical.display()))?
				.filter_map(Result::ok)
				.map(|entry| entry.file_name().to_string_lossy().into_owned())
				.collect::<Vec<_>>();
			entries.sort();
			let mut resource = InternalResource::text(&url.href, entries.join("\n"));
			resource.is_directory = true;
			resource.source_path = Some(canonical);
			return Ok(resource);
		}
		if !metadata.is_file() {
			return Err(format!(
				"memory:// URL must resolve to a file or directory: {}",
				url.href
			));
		}
		let mut bytes = Vec::new();
		fs::File::open(&canonical)
			.map_err(|error| format!("{}: {error}", canonical.display()))?
			.read_to_end(&mut bytes)
			.map_err(|error| format!("{}: {error}", canonical.display()))?;
		let content = String::from_utf8(bytes)
			.map_err(|_| format!("{} is not UTF-8 text; binary resources are not supported", url.href))?;
		let mut resource = InternalResource::text(&url.href, content);
		resource.content_type = content_type_for(&canonical).to_string();
		resource.size = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
		resource.source_path = Some(canonical);
		Ok(resource)
	}
}

/// `validateRelativePath` with the `memory://` wording TS rewrites.
fn validate_relative_path_memory(relative: &str) -> Result<(), String> {
	validate_relative_path(relative).map_err(|error| error.replace("skill://", "memory://"))
}

/// Absolute path helper shared with tests.
#[must_use]
pub fn memory_path(agent_dir: &Path, cwd: &Path, relative: &str) -> PathBuf {
	local_memory::memory_root(agent_dir, cwd).join(relative)
}

/// One line per record, with its durable id so it can be opened directly.
fn render_record_list(records: &[MemoryRecord]) -> String {
	if records.is_empty() {
		return "No memory records.".to_string();
	}
	let mut out = String::new();
	for record in records {
		let scope = match &record.cwd {
			Some(cwd) => format!("project:{cwd}"),
			None => "global".to_string(),
		};
		out.push_str(&format!(
			"- {} [{scope}] origin={} actor={} source={} updated={}\n  {}\n",
			record.id,
			record.origin.as_str(),
			record.actor.as_deref().unwrap_or("-"),
			record.source.as_deref().unwrap_or("-"),
			record.updated_at,
			record.body.replace('\n', " "),
		));
	}
	out
}

/// One record with the provenance D53 requires for viewing/editing.
fn render_record(record: &MemoryRecord) -> String {
	let mut out = format!("# Memory {}\n\n- scope: {}\n", record.id, record.scope.as_str());
	if let Some(cwd) = &record.cwd {
		out.push_str(&format!("- cwd: {cwd}\n"));
	}
	out.push_str(&format!("- origin: {}\n", record.origin.as_str()));
	if let Some(actor) = &record.actor {
		out.push_str(&format!("- actor: {actor}\n"));
	}
	if let Some(source) = &record.source {
		out.push_str(&format!("- source: {source}\n"));
	}
	out.push_str(&format!(
		"- created_at: {}\n- updated_at: {}\n\n{}\n",
		record.created_at, record.updated_at, record.body
	));
	if let Some(context) = &record.context {
		out.push_str(&format!("\nContext: {context}\n"));
	}
	out
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicU32, Ordering};

	use pi_agent::tools::internal_urls::{ProtocolHandler, ResolveContext};

	use super::*;

	static COUNTER: AtomicU32 = AtomicU32::new(0);

	fn fixture() -> (PathBuf, PathBuf, MemoryUrlHandler) {
		let dir = std::env::temp_dir().join(format!(
			"omp-memory-protocol-{}-{}",
			std::process::id(),
			COUNTER.fetch_add(1, Ordering::Relaxed)
		));
		let agent_dir = dir.join("agent");
		let cwd = dir.join("project");
		std::fs::create_dir_all(&cwd).expect("cwd");
		let root = local_memory::memory_root(&agent_dir, &cwd);
		std::fs::create_dir_all(&root).expect("memory root");
		let handler = MemoryUrlHandler::new(agent_dir.clone(), MemoryBackend::Local);
		(cwd, agent_dir, handler)
	}

	async fn resolve(
		handler: &MemoryUrlHandler,
		cwd: &Path,
		href: &str,
	) -> Result<InternalResource, String> {
		let url = InternalUrl::parse(href).expect("url");
		let context = ResolveContext { cwd: cwd.to_path_buf(), ..ResolveContext::default() };
		handler.resolve(&url, &context).await
	}

	#[tokio::test]
	async fn bare_root_reads_summary_and_paths_read_learned() {
		let (cwd, agent_dir, handler) = fixture();
		let root = local_memory::memory_root(&agent_dir, &cwd);
		std::fs::write(root.join("memory_summary.md"), "summary text").expect("summary");
		std::fs::write(root.join("learned.md"), "lesson text").expect("learned");

		let bare = resolve(&handler, &cwd, "memory://root").await.expect("bare root");
		assert_eq!(bare.content, "summary text");
		assert_eq!(bare.content_type, "text/markdown");

		let learned = resolve(&handler, &cwd, "memory://root/learned.md")
			.await
			.expect("learned");
		assert_eq!(learned.content, "lesson text");

		std::fs::create_dir_all(root.join("skills")).expect("skills dir");
		std::fs::write(root.join("skills/readme.md"), "x").expect("skill file");
		let listing = resolve(&handler, &cwd, "memory://root/skills")
			.await
			.expect("listing");
		assert!(listing.is_directory);
		assert!(listing.content.contains("readme.md"));
	}

	#[tokio::test]
	async fn learned_is_available_without_a_summary() {
		let (cwd, agent_dir, handler) = fixture();
		std::fs::write(
			local_memory::memory_root(&agent_dir, &cwd).join("learned.md"),
			"lesson only",
		)
		.expect("learned");
		let learned = resolve(&handler, &cwd, "memory://root/learned.md")
			.await
			.expect("learned");
		assert_eq!(learned.content, "lesson only");
		let bare = resolve(&handler, &cwd, "memory://root").await;
		assert_eq!(bare.unwrap_err(), "Memory file not found: memory://root");
	}

	#[tokio::test]
	async fn traversal_and_unknown_namespaces_are_rejected() {
		let (cwd, agent_dir, handler) = fixture();
		std::fs::write(local_memory::memory_root(&agent_dir, &cwd).join("learned.md"), "x")
			.expect("learned");
		assert!(resolve(&handler, &cwd, "memory://root/../learned.md").await.is_err());
		assert!(
			resolve(&handler, &cwd, "memory://elsewhere/learned.md")
				.await
				.expect_err("namespace")
				.starts_with("Unknown memory namespace: elsewhere")
		);
	}

	async fn write_record(
		handler: &MemoryUrlHandler,
		cwd: &Path,
		href: &str,
		body: &str,
	) -> Result<(), String> {
		let url = InternalUrl::parse(href).expect("url");
		let context = WriteContext { cwd: cwd.to_path_buf(), ..WriteContext::default() };
		handler.write(&url, body, &context).await
	}

	#[tokio::test]
	async fn store_records_list_read_search_and_edit() {
		let (cwd, agent_dir, handler) = fixture();
		let cwd_text = cwd.to_string_lossy().to_string();
		let other_cwd = cwd.parent().expect("parent").join("other");
		std::fs::create_dir_all(&other_cwd).expect("other cwd");
		let other_text = other_cwd.to_string_lossy().to_string();

		let store = memory_store::MemoryStore::open(&agent_dir).expect("store");
		let record = store
			.add(&memory_store::AddInput::project_lesson(
				&cwd_text,
				"deploy staging before production",
				Some("review turn"),
			))
			.expect("record");
		store
			.add(&memory_store::AddInput::project_lesson(&other_text, "other project secret", None))
			.expect("other record");
		drop(store);

		let listing = resolve(&handler, &cwd, "memory://list").await.expect("list");
		assert!(listing.content.contains(record.id.as_str()), "{}", listing.content);
		assert!(listing.content.contains("deploy staging before production"));
		assert!(!listing.content.contains("other project secret"), "{}", listing.content);

		let search = resolve(&handler, &cwd, "memory://search/deploy").await.expect("search");
		assert!(search.content.contains(record.id.as_str()), "{}", search.content);
		assert!(!search.content.contains("other project secret"), "{}", search.content);

		let href = format!("memory://{}", record.id);
		let viewed = resolve(&handler, &cwd, &href).await.expect("record");
		assert!(viewed.content.contains("deploy staging before production"));
		assert!(viewed.content.contains("origin: inferred"));
		assert!(viewed.content.contains("actor: agent"));
		assert!(viewed.content.contains("source: learn"));
		assert!(viewed.content.contains("Context: review turn"));

		assert!(handler.writable());
		write_record(&handler, &cwd, &href, "deploy staging only after review")
			.await
			.expect("edit");
		let viewed = resolve(&handler, &cwd, &href).await.expect("edited record");
		assert!(viewed.content.contains("deploy staging only after review"));
		let exported = std::fs::read_to_string(
			local_memory::memory_root(&agent_dir, &cwd).join(local_memory::LEARNED_LESSONS_FILE),
		)
		.expect("export");
		assert!(exported.contains("deploy staging only after review"), "{exported}");

		// Another project cannot read the record through the protocol.
		assert!(
			resolve(&handler, &other_cwd, &href)
				.await
				.expect_err("cross-project read")
				.contains("not found")
		);
	}

	#[tokio::test]
	async fn root_stays_read_only_and_missing_stores_are_explicit() {
		let (cwd, agent_dir, handler) = fixture();
		let root_href = "memory://root/learned.md";
		let error = write_record(&handler, &cwd, root_href, "x").await.expect_err("root write");
		assert!(error.contains("read-only"), "{error}");

		let error = resolve(&handler, &cwd, "memory://list").await.expect_err("no store");
		assert!(error.contains("no memory records"), "{error}");

		let legacy = local_memory::memory_root(&agent_dir, &cwd).join("learned.md");
		std::fs::write(&legacy, "- legacy deploy lesson\n").expect("learned");
		let listing = resolve(&handler, &cwd, "memory://list").await.expect("imported list");
		assert!(listing.content.contains("legacy deploy lesson"), "{}", listing.content);
		assert!(memory_store::MemoryStore::database_path(&agent_dir).is_file());
	}

	#[tokio::test]
	async fn backend_off_reports_the_scheme_as_unknown() {
		let (cwd, agent_dir, _) = fixture();
		let handler = MemoryUrlHandler::new(agent_dir, MemoryBackend::Off);
		assert_eq!(
			resolve(&handler, &cwd, "memory://root").await.expect_err("off"),
			"Unknown protocol: memory://"
		);
	}
}
