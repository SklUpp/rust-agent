//! `memory://` protocol handler for the local, file-backed backend.
//!
//! Anchors: `packages/coding-agent/src/internal-urls/memory-protocol.ts`
//! (`MemoryProtocolHandler`, `resolveMemoryUrlToPath`, `tryResolveInRoot`).
//!
//! Only the `root` namespace exists in this port: it maps to the calling
//! session's memory directory (`local_memory::memory_root`), so two sessions
//! with different cwds never read each other's artifacts. A bare
//! `memory://root` means `memory_summary.md`, exactly like TS. Every path is
//! percent-decoded, traversal-checked and canonicalised under the root.
//! Mnemopi ids and Hindsight pointers are not implemented here.

use std::{
	fs,
	io::Read as _,
	path::{Path, PathBuf},
};

use async_trait::async_trait;
use pi_agent::tools::internal_urls::{
	InternalResource, InternalUrl, ProtocolHandler, ResolveContext,
};

use crate::{
	autolearn::MemoryBackend,
	local_memory,
	skill_protocol::{content_type_for, decode_percent, validate_relative_path},
};

/// Scheme served by [`MemoryUrlHandler`].
pub const MEMORY_SCHEME: &str = "memory";

const MEMORY_NAMESPACE: &str = "root";
const DEFAULT_MEMORY_FILE: &str = "memory_summary.md";

/// Read-only `memory://` handler bound to one session's agent dir.
pub struct MemoryUrlHandler {
	agent_dir: PathBuf,
	backend:   MemoryBackend,
}

impl MemoryUrlHandler {
	#[must_use]
	pub fn new(agent_dir: PathBuf, backend: MemoryBackend) -> Self {
		Self { agent_dir, backend }
	}
}

#[async_trait]
impl ProtocolHandler for MemoryUrlHandler {
	fn scheme(&self) -> &str {
		MEMORY_SCHEME
	}

	fn immutable(&self) -> bool {
		true
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
			return Err("memory:// URL requires a namespace: memory://root".to_string());
		}
		if namespace != MEMORY_NAMESPACE {
			return Err(format!(
				"Unknown memory namespace: {namespace}. Supported: root (file-backed memory summary), or a mnemopi memory id when memory.backend=mnemopi is active."
			));
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
