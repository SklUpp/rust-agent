//! Built-in `artifact://` read handler.
//!
//! Mirrors `packages/coding-agent/src/internal-urls/artifact-protocol.ts`:
//! numeric per-session artifact ids, caller-first root search, the exact
//! no-session / missing-directory / not-found-with-available-ids errors,
//! immutable text resources, and an 8 MiB full-content cap. The cap gates
//! materialization only: `read` resolves the backing file and streams ranges,
//! so larger artifacts stay addressable without loading them.
//!
//! Roots come from the immutable per-call [`ResolveContext`], never a
//! process-global session registry: a session binds its artifacts directory as
//! the `local://` root (`RpcSession::url_scope`), and a subagent's scope adds
//! the owning root's artifact directory under `artifact://` because every
//! child publishes into that same directory (`crates/omp-cli/src/eval_host.rs`
//! hands children one shared `artifacts_dir`). Scanning every session on the
//! box would leak artifacts across unrelated sessions, so only those
//! caller-owned roots are searched. This is a deliberate security boundary:
//! upstream `artifact-protocol.ts` also walks the process-global agent registry
//! after its pinned dir, and this port does not claim that behaviour.
//!
//! [`resolve_artifact`] is the path-only seam: it returns identity, path and
//! size without reading bytes, so consumers can address an artifact of any
//! size. Rust `read` is its only caller; the minimizer layers
//! (`pi-shell`, `pi-natives`) only splice `artifact://<id>` references into
//! text and never resolve them.

use std::{
	fs,
	io::Read,
	path::{Path, PathBuf},
};

use async_trait::async_trait;

use super::internal_urls::{
	InternalResource, InternalUrl, LOCAL_SCHEME, ProtocolHandler, ResolveContext, decode_utf8,
};

/// Scheme for spilled tool output.
pub const ARTIFACT_SCHEME: &str = "artifact";

/// Full-content cap: an artifact at or below this size resolves to text;
/// larger ones must be read through selectors (which stream from the file).
pub const MAX_INLINE_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024;

/// One resolved artifact: identity, verified backing path, size and the open
/// descriptor that was verified (never re-opened by path for content).
#[derive(Debug)]
pub struct ResolvedArtifact {
	pub id:   String,
	pub path: PathBuf,
	pub size: u64,
	file:     fs::File,
}

impl ResolvedArtifact {
	/// The verified leaf descriptor. Ranged readers consume this handle
	/// instead of reopening [`Self::path`].
	pub fn file(&self) -> &fs::File {
		&self.file
	}

	/// Read the whole artifact from the verified descriptor in bounded chunks.
	///
	/// The metadata size checked before this call is stale: a same-UID writer
	/// can grow the file after resolution, so the actual byte count is enforced
	/// here. Cancellation is checked between chunks.
	pub fn read_text(self, href: &str, signal: &crate::CancellationToken) -> Result<String, String> {
		let Self { id, path, mut file, .. } = self;
		let cap = MAX_INLINE_ARTIFACT_BYTES as usize;
		let mut bytes: Vec<u8> = Vec::new();
		let mut chunk = [0u8; 64 * 1024];
		loop {
			if signal.is_cancelled() {
				return Err("aborted".to_string());
			}
			let read = file
				.read(&mut chunk)
				.map_err(|error| format!("{href}: {error}"))?;
			if read == 0 {
				break;
			}
			if bytes.len() + read > cap {
				return Err(format!(
					"Artifact {id} grew past the {cap} byte full-content cap while reading; full \
					 internal resolution is blocked. Use read selectors such as artifact://{id}:1-3000 \
					 or artifact://{id}:raw:1-3000, and use the artifact file path for search/copy \
					 workflows: {}",
					path.display()
				));
			}
			bytes.extend_from_slice(&chunk[..read]);
		}
		decode_utf8(&bytes, false, href)
	}
}

/// Artifact directories this call may search, deduped, caller-first: an
/// explicit per-scope `artifact://` root (children read the owning root's
/// producer directory) wins over the session's `local://` root.
#[must_use]
pub fn artifact_roots(context: &ResolveContext) -> Option<Vec<PathBuf>> {
	let mut roots: Vec<PathBuf> = Vec::new();
	for scheme in [ARTIFACT_SCHEME, LOCAL_SCHEME] {
		if let Some(root) = context.local_roots.get(scheme)
			&& !roots.iter().any(|known| known == root)
		{
			roots.push(root.clone());
		}
	}
	(!roots.is_empty()).then_some(roots)
}

fn parse_artifact_id(url: &InternalUrl) -> Result<&str, String> {
	if url.host.is_empty() {
		return Err("artifact:// URL requires a numeric ID: artifact://0".to_string());
	}
	if !url.host.bytes().all(|byte| byte.is_ascii_digit()) {
		return Err(format!("artifact:// ID must be numeric, got: {}", url.host));
	}
	Ok(&url.host)
}

/// Resolve an `artifact://<id>` URL to its backing file without reading the
/// artifact bytes. Ids are per-session counters, so the caller's own roots are
/// the only ones consulted and the first root that owns the id wins.
pub async fn resolve_artifact(
	url: &InternalUrl,
	context: &ResolveContext,
) -> Result<ResolvedArtifact, String> {
	let id = parse_artifact_id(url)?.to_string();
	if context.signal.is_cancelled() {
		return Err("aborted".to_string());
	}
	let Some(roots) = artifact_roots(context) else {
		return Err(
			"No session - artifacts unavailable: artifact:// needs the session artifacts \
			 directory bound in UrlScope::local_roots"
				.to_string(),
		);
	};
	let href = url.href.clone();
	tokio::task::spawn_blocking(move || resolve_in_roots(&id, &roots))
		.await
		.map_err(|error| format!("{href}: artifact lookup task failed: {error}"))?
}

fn resolve_in_roots(id: &str, roots: &[PathBuf]) -> Result<ResolvedArtifact, String> {
	let mut available: Vec<String> = Vec::new();
	let mut any_dir_exists = false;
	let mut found: Option<(PathBuf, String)> = None;

	for root in roots {
		let entries = match fs::read_dir(root) {
			Ok(entries) => entries,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
			Err(error) => {
				return Err(format!("artifacts directory {}: {error}", root.display()));
			},
		};
		any_dir_exists = true;
		let mut names: Vec<String> = Vec::new();
		for entry in entries {
			let entry =
				entry.map_err(|error| format!("artifacts directory {}: {error}", root.display()))?;
			let name = entry.file_name().to_string_lossy().into_owned();
			if let Some(digits) = name.split_once('.').map(|(head, _)| head)
				&& !digits.is_empty()
				&& digits.bytes().all(|byte| byte.is_ascii_digit())
				&& !available.iter().any(|known| known == digits)
			{
				available.push(digits.to_string());
			}
			names.push(name);
		}
		names.sort();
		let prefix = format!("{id}.");
		if let Some(name) = names.into_iter().find(|name| name.starts_with(&prefix)) {
			found = Some((root.clone(), name));
			break;
		}
	}

	if !any_dir_exists {
		return Err("No artifacts directory found".to_string());
	}

	let Some((root, name)) = found else {
		available.sort_by_key(|id| id.parse::<u64>().unwrap_or(u64::MAX));
		available.dedup();
		let list = if available.is_empty() {
			"none".to_string()
		} else {
			available.join(", ")
		};
		return Err(format!("Artifact {id} not found. Available: {list}"));
	};

	let path = root.join(&name);
	let file = open_artifact_file(&root, &name)
		.map_err(|error| format!("Artifact {id} ({name}): {error}"))?;
	let metadata = file
		.metadata()
		.map_err(|error| format!("Artifact {id} ({name}): {error}"))?;
	if metadata.is_dir() {
		return Err(format!("Artifact {id} resolved to a directory, not a file"));
	}
	if !metadata.is_file() {
		return Err(format!("Artifact {id} is not a regular file"));
	}
	Ok(ResolvedArtifact {
		id: id.to_string(),
		path,
		size: metadata.len(),
		file,
	})
}

/// Open one artifact entry below `root` without following a symlink at either
/// the root or the leaf, so a swapped link cannot redirect the read outside the
/// session's artifact store.
#[cfg(unix)]
pub(super) fn open_artifact_file(root: &Path, name: &str) -> Result<fs::File, String> {
	use std::{
		ffi::CString,
		os::{
			fd::{AsRawFd, FromRawFd, OwnedFd},
			unix::ffi::OsStrExt,
		},
	};

	fn last_error() -> std::io::Error {
		std::io::Error::last_os_error()
	}

	let root_c = CString::new(root.as_os_str().as_bytes())
		.map_err(|_| format!("artifacts root contains a NUL: {}", root.display()))?;
	let name_c = CString::new(name)
		.map_err(|_| format!("artifact file name contains a NUL: {name:?}"))?;
	// SAFETY: `root_c` is NUL-terminated; the fd returned is owned by this call.
	let root_fd = unsafe {
		libc::open(
			root_c.as_ptr(),
			libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
		)
	};
	if root_fd < 0 {
		return Err(last_error().to_string());
	}
	// SAFETY: `root_fd` is a fresh descriptor this call owns.
	let directory = unsafe { OwnedFd::from_raw_fd(root_fd) };
	// SAFETY: `directory` is open and `name_c` is NUL-terminated. `O_NONBLOCK`
	// keeps a FIFO from parking this worker in `open`; the regular-file check
	// in the caller still rejects it.
	let fd = unsafe {
		libc::openat(
			directory.as_raw_fd(),
			name_c.as_ptr(),
			libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
		)
	};
	if fd < 0 {
		return Err(last_error().to_string());
	}
	// SAFETY: fresh descriptor, ownership moves into the `File`.
	Ok(fs::File::from(unsafe { OwnedFd::from_raw_fd(fd) }))
}

#[cfg(not(unix))]
pub(super) fn open_artifact_file(_root: &Path, _name: &str) -> Result<fs::File, String> {
	Err("artifact:// needs descriptor-relative traversal; unsupported on this platform".to_string())
}

/// Built-in read-only `artifact://` handler.
pub struct ArtifactUrlHandler;

#[async_trait]
impl ProtocolHandler for ArtifactUrlHandler {
	fn scheme(&self) -> &str {
		ARTIFACT_SCHEME
	}

	fn immutable(&self) -> bool {
		true
	}

	async fn resolve(
		&self,
		url: &InternalUrl,
		context: &ResolveContext,
	) -> Result<InternalResource, String> {
		let artifact = resolve_artifact(url, context).await?;
		let pointer = format!("artifact://{}", artifact.id);
		let path = artifact.path.clone();
		let size = artifact.size;
		if size > MAX_INLINE_ARTIFACT_BYTES {
			return Err(format!(
				"Artifact {id} is {size} bytes; full internal resolution is blocked. Use read \
				 selectors such as {pointer}:1-3000 or {pointer}:raw:1-3000, and use the artifact \
				 file path for search/copy workflows: {path}",
				id = artifact.id,
				path = path.display(),
			));
		}
		let href = url.href.clone();
		let signal = context.signal.clone();
		let content = tokio::task::spawn_blocking(move || artifact.read_text(&href, &signal))
			.await
			.map_err(|error| format!("{}: artifact read task failed: {error}", url.href))??;
		let mut resource = InternalResource::text(&url.href, content);
		resource.size = usize::try_from(size).unwrap_or(usize::MAX);
		resource.source_path = Some(path);
		resource.immutable = true;
		Ok(resource)
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::BTreeMap,
		time::{SystemTime, UNIX_EPOCH},
	};

	use super::*;
	use crate::CancellationToken;

	fn temp_dir(label: &str) -> PathBuf {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map(|duration| duration.as_nanos())
			.unwrap_or_default();
		let path = std::env::temp_dir().join(format!(
			"omp-artifact-uri-{label}-{}-{nanos}",
			std::process::id()
		));
		fs::create_dir_all(&path).expect("create temp dir");
		path
	}

	fn scope(root: &Path) -> ResolveContext {
		ResolveContext {
			local_roots: BTreeMap::from([(LOCAL_SCHEME.to_string(), root.to_path_buf())]),
			..ResolveContext::default()
		}
	}

	fn runtime() -> tokio::runtime::Runtime {
		tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.expect("runtime")
	}

	fn parse(url: &str) -> InternalUrl {
		InternalUrl::parse(url).expect("parse")
	}

	#[test]
	fn resolves_the_producer_name_and_reports_available_ids() {
		// `CliEvalHost::write_artifact_file` publishes `<id>.<kind>.log`.
		let root = temp_dir("producer");
		fs::write(root.join("0.eval.log"), "zero\n").expect("artifact 0");
		fs::write(root.join("2.eval.log"), "two\n").expect("artifact 2");
		let context = scope(&root);

		let artifact = runtime()
			.block_on(resolve_artifact(&parse("artifact://2"), &context))
			.expect("resolve");
		assert_eq!(artifact.id, "2");
		assert_eq!(artifact.path, root.join("2.eval.log"));
		assert_eq!(artifact.size, 4);
		assert_eq!(
			artifact
				.read_text("artifact://2", &CancellationToken::new())
				.expect("text"),
			"two\n"
		);

		let error = runtime()
			.block_on(resolve_artifact(&parse("artifact://1"), &context))
			.expect_err("missing");
		assert_eq!(error, "Artifact 1 not found. Available: 0, 2");
		fs::remove_dir_all(&root).ok();
	}

	#[test]
	fn explicit_artifact_root_wins_over_local_root_for_same_id() {
		let local = temp_dir("local");
		let producer = temp_dir("producer");
		fs::write(local.join("0.eval.log"), "local\n").expect("local artifact");
		fs::write(producer.join("0.eval.log"), "producer\n").expect("producer artifact");
		let context = ResolveContext {
			local_roots: BTreeMap::from([
				(LOCAL_SCHEME.to_string(), local.clone()),
				(ARTIFACT_SCHEME.to_string(), producer.clone()),
			]),
			..ResolveContext::default()
		};

		let artifact = runtime()
			.block_on(resolve_artifact(&parse("artifact://0"), &context))
			.expect("resolve");
		assert_eq!(artifact.path, producer.join("0.eval.log"));
		assert!(
			artifact
				.read_text("artifact://0", &CancellationToken::new())
				.expect("text")
				.contains("producer")
		);
		fs::remove_dir_all(&local).ok();
		fs::remove_dir_all(&producer).ok();
	}

	#[test]
	fn malformed_ids_and_missing_scope_are_exact_errors() {
		let root = temp_dir("errors");
		let context = scope(&root);
		let resolve = |url: &str| runtime().block_on(resolve_artifact(&parse(url), &context));
		assert_eq!(
			resolve("artifact://").expect_err("empty"),
			"artifact:// URL requires a numeric ID: artifact://0"
		);
		assert_eq!(
			resolve("artifact://abc").expect_err("non numeric"),
			"artifact:// ID must be numeric, got: abc"
		);
		assert_eq!(
			resolve("artifact://0").expect_err("empty dir"),
			"Artifact 0 not found. Available: none"
		);

		let unbound = ResolveContext::default();
		assert_eq!(
			runtime()
				.block_on(resolve_artifact(&parse("artifact://0"), &unbound))
				.expect_err("no session"),
			"No session - artifacts unavailable: artifact:// needs the session artifacts \
			 directory bound in UrlScope::local_roots"
		);

		let missing = scope(&root.join("absent"));
		assert_eq!(
			runtime()
				.block_on(resolve_artifact(&parse("artifact://0"), &missing))
				.expect_err("missing dir"),
			"No artifacts directory found"
		);
		fs::remove_dir_all(&root).ok();
	}

	#[test]
	fn cancellation_and_non_regular_entries_are_rejected() {
		let root = temp_dir("nonregular");
		fs::create_dir_all(root.join("3.eval.log")).expect("directory artifact");
		let context = scope(&root);
		assert_eq!(
			runtime()
				.block_on(resolve_artifact(&parse("artifact://3"), &context))
				.expect_err("directory"),
			"Artifact 3 resolved to a directory, not a file"
		);

		let signal = CancellationToken::new();
		signal.cancel();
		let cancelled = ResolveContext {
			local_roots: context.local_roots.clone(),
			signal,
			..ResolveContext::default()
		};
		assert_eq!(
			runtime()
				.block_on(resolve_artifact(&parse("artifact://3"), &cancelled))
				.expect_err("cancelled"),
			"aborted"
		);
		fs::remove_dir_all(&root).ok();
	}

	#[test]
	fn full_content_cap_blocks_materialization_but_not_lookup() {
		let root = temp_dir("cap");
		let path = root.join("0.eval.log");
		let mut big = vec![b'a'; MAX_INLINE_ARTIFACT_BYTES as usize + 16];
		big[MAX_INLINE_ARTIFACT_BYTES as usize + 15] = b'\n';
		fs::write(&path, &big).expect("big artifact");
		let context = scope(&root);
		let handler = ArtifactUrlHandler;
		let error = runtime()
			.block_on(handler.resolve(&parse("artifact://0"), &context))
			.expect_err("cap");
		assert!(error.contains("full internal resolution is blocked"), "{error}");
		assert!(error.contains(&path.display().to_string()), "{error}");
		let artifact = runtime()
			.block_on(resolve_artifact(&parse("artifact://0"), &context))
			.expect("lookup still works");
		assert_eq!(artifact.size, big.len() as u64);
		fs::remove_dir_all(&root).ok();
	}

	#[test]
	fn full_read_enforces_actual_bytes_after_growth_and_cancellation() {
		let root = temp_dir("growth");
		let path = root.join("0.eval.log");
		fs::write(&path, "small\n").expect("small artifact");
		let context = scope(&root);
		let artifact = runtime()
			.block_on(resolve_artifact(&parse("artifact://0"), &context))
			.expect("resolve");
		assert_eq!(artifact.size, 6);

		// Same-UID growth after the metadata check: the descriptor sees the new
		// bytes, so the bounded read must reject instead of allocating to EOF.
		fs::write(&path, vec![b'x'; MAX_INLINE_ARTIFACT_BYTES as usize + 1])
			.expect("grow artifact");
		let error = artifact
			.read_text("artifact://0", &CancellationToken::new())
			.expect_err("growth cap");
		assert!(error.contains("grew past"), "{error}");

		let artifact = runtime()
			.block_on(resolve_artifact(&parse("artifact://0"), &context))
			.expect("re-resolve");
		let signal = CancellationToken::new();
		signal.cancel();
		assert_eq!(
			artifact.read_text("artifact://0", &signal).expect_err("cancelled"),
			"aborted"
		);
		fs::remove_dir_all(&root).ok();
	}
}
