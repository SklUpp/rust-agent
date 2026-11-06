//! `skill://` protocol handler.
//!
//! Anchors: `packages/coding-agent/src/internal-urls/skill-protocol.ts:1-131`.
//!
//! URL forms:
//! - `skill://<name>` → the skill's `SKILL.md`;
//! - `skill://<name>/<path>` → a relative path inside the skill directory.
//!
//! Read-only. Every resolution is confined to the skill's base directory:
//! absolute paths and `..` components are rejected up front, the joined path
//! must stay lexically inside the base, and the canonical path must too (a
//! symlink cannot redirect a read outside the skill). Plugin-provided skills
//! carry a [`SkillEntry::contain_root`]; their canonical path must additionally
//! stay inside that plugin root, and a dangling link there fails closed.
//!
//! Skills come from the *caller's* [`SkillSnapshot`], never a process-global
//! registry, so concurrent sessions cannot read each other's skill set.

use std::{
	fs,
	io::Read as _,
	path::{Path, PathBuf},
};

use async_trait::async_trait;
use pi_agent::tools::internal_urls::{
	InternalResource, InternalUrl, ProtocolHandler, ResolveContext,
};

use crate::skills::{SkillEntry, SkillSnapshot};

/// Scheme served by [`SkillUrlHandler`].
pub const SKILL_SCHEME: &str = "skill";

/// Read-only `skill://` handler bound to one session's snapshot.
pub struct SkillUrlHandler {
	snapshot: SkillSnapshot,
}

impl SkillUrlHandler {
	#[must_use]
	pub fn new(snapshot: SkillSnapshot) -> Self {
		Self { snapshot }
	}

	/// The snapshot this handler resolves against.
	#[must_use]
	pub fn snapshot(&self) -> &SkillSnapshot {
		&self.snapshot
	}
}

#[async_trait]
impl ProtocolHandler for SkillUrlHandler {
	fn scheme(&self) -> &str {
		SKILL_SCHEME
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
		let name = url.host.as_str();
		if name.is_empty() {
			return Err("skill:// URL requires a skill name: skill://<name>".to_string());
		}
		let entries = self.snapshot.entries();
		let Some(entry) = entries.iter().find(|entry| entry.skill.name == name) else {
			let available = entries
				.iter()
				.map(|entry| entry.skill.name.as_str())
				.collect::<Vec<_>>();
			let available = if available.is_empty() { "none".to_string() } else { available.join(", ") };
			return Err(format!("Unknown skill: {name}\nAvailable: {available}"));
		};
		resolve_entry(entry, url, context)
	}
}

/// Resolve one entry (shared by the handler and tests).
fn resolve_entry(
	entry: &SkillEntry,
	url: &InternalUrl,
	context: &ResolveContext,
) -> Result<InternalResource, String> {
	let base_dir = entry.base_dir();
	let relative = url.path.trim_start_matches('/');
	let target = if relative.is_empty() {
		entry.skill.path.clone()
	} else {
		let relative = decode_percent(relative);
		validate_relative_path(&relative)?;
		let joined = base_dir.join(&relative);
		let resolved = normalize_lexically(&joined);
		let base = normalize_lexically(&base_dir);
		if !resolved.starts_with(&base) {
			return Err("Path traversal is not allowed".to_string());
		}
		let canonical_base = fs::canonicalize(&base_dir).map_err(|error| {
			format!("File not found: {} ({error})", base_dir.display())
		})?;
		let canonical = match fs::canonicalize(&resolved) {
			Ok(canonical) => canonical,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Err(format!("File not found: {}", resolved.display()));
			},
			Err(error) => return Err(format!("{}: {error}", resolved.display())),
		};
		if let Some(root) = &entry.contain_root {
			// Agent Plugin skills (§4.1): the resource must canonically resolve
			// within the plugin root; a dangling or unresolvable path fails
			// closed. Symlinks may target other files inside the same package,
			// so the base-directory check relaxes to the plugin root.
			let canonical_root = fs::canonicalize(root)
				.map_err(|error| format!("skill:// plugin root is unavailable: {} ({error})", root.display()))?;
			if !canonical.starts_with(&canonical_root) {
				return Err(format!("skill:// path resolves outside the plugin root: {}", url.href));
			}
		} else if !canonical.starts_with(&canonical_base) {
			return Err("Path traversal is not allowed".to_string());
		}
		canonical
	};

	let metadata = fs::metadata(&target).map_err(|error| {
		if error.kind() == std::io::ErrorKind::NotFound {
			format!("File not found: {}", target.display())
		} else {
			format!("{}: {error}", target.display())
		}
	})?;
	if context.signal.is_cancelled() {
		return Err("aborted".to_string());
	}
	if metadata.is_dir() {
		let mut entries = fs::read_dir(&target)
			.map_err(|error| format!("{}: {error}", target.display()))?
			.filter_map(Result::ok)
			.map(|entry| entry.file_name().to_string_lossy().into_owned())
			.collect::<Vec<_>>();
		entries.sort();
		let mut resource = InternalResource::text(&url.href, entries.join("\n"));
		resource.is_directory = true;
		resource.source_path = Some(target);
		return Ok(resource);
	}
	if !metadata.is_file() {
		return Err(format!("skill:// URL must resolve to a file or directory: {}", url.href));
	}
	let (bytes, size) = read_file(&target)?;
	let content = String::from_utf8(bytes).map_err(|_| {
		format!("{} is not UTF-8 text; binary resources are not supported", url.href)
	})?;
	let mut resource = InternalResource::text(&url.href, content);
	resource.content_type = content_type_for(&target).to_string();
	resource.source_path = Some(target);
	resource.size = usize::try_from(size).unwrap_or(usize::MAX);
	Ok(resource)
}

fn read_file(path: &Path) -> Result<(Vec<u8>, u64), String> {
	let mut file =
		fs::File::open(path).map_err(|error| format!("File not found: {} ({error})", path.display()))?;
	let mut bytes = Vec::new();
	file.read_to_end(&mut bytes)
		.map_err(|error| format!("{}: {error}", path.display()))?;
	let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
	Ok((bytes, size))
}

/// Reject absolute paths and `..` components (TS `validateRelativePath`).
pub fn validate_relative_path(relative_path: &str) -> Result<(), String> {
	if Path::new(relative_path).is_absolute() {
		return Err("Absolute paths are not allowed in skill:// URLs".to_string());
	}
	if relative_path
		.split(['/', '\\'])
		.any(|component| component == "..")
	{
		return Err("Path traversal (..) is not allowed in skill:// URLs".to_string());
	}
	Ok(())
}

/// Percent-decode a URL path segment; invalid escapes are kept verbatim.
pub(crate) fn decode_percent(input: &str) -> String {
	let bytes = input.as_bytes();
	let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
	let mut index = 0;
	while index < bytes.len() {
		if bytes[index] == b'%' && index + 2 < bytes.len() {
			let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).ok();
			if let Some(value) = hex.and_then(|hex| u8::from_str_radix(hex, 16).ok()) {
				out.push(value);
				index += 3;
				continue;
			}
		}
		out.push(bytes[index]);
		index += 1;
	}
	String::from_utf8_lossy(&out).into_owned()
}

/// Lexical normalization: resolve `.`/`..` without touching the filesystem.
fn normalize_lexically(path: &Path) -> PathBuf {
	let mut out = PathBuf::new();
	for component in path.components() {
		match component {
			std::path::Component::ParentDir => {
				out.pop();
			},
			std::path::Component::CurDir => {},
			other => out.push(other.as_os_str()),
		}
	}
	out
}

pub(crate) fn content_type_for(path: &Path) -> &'static str {
	let markdown = path
		.extension()
		.and_then(std::ffi::OsStr::to_str)
		.is_some_and(|ext| matches!(ext.to_ascii_lowercase().as_str(), "md" | "mdx" | "markdown"));
	if markdown { "text/markdown" } else { "text/plain" }
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::skills::Skill;
	use std::io::Write as _;
	use std::sync::atomic::{AtomicU32, Ordering};

	static COUNTER: AtomicU32 = AtomicU32::new(0);

	fn temp_dir() -> PathBuf {
		let unique = format!(
			"omp-cli-skill-protocol-{}-{}",
			std::process::id(),
			COUNTER.fetch_add(1, Ordering::Relaxed)
		);
		let dir = std::env::temp_dir().join(unique);
		fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	fn write(path: &Path, text: &str) {
		fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
		let mut file = fs::File::create(path).expect("create");
		file.write_all(text.as_bytes()).expect("write");
	}

	fn entry(root: &Path, name: &str, body: &str) -> SkillEntry {
		let skill_dir = root.join(name);
		let path = skill_dir.join("SKILL.md");
		write(&path, &format!("---\nname: {name}\ndescription: test\n---\n{body}\n"));
		SkillEntry {
			skill: Skill {
				name: name.to_string(),
				description: "test".to_string(),
				path,
				body: body.to_string(),
			},
			source: "native:user".to_string(),
			hide: false,
			contain_root: None,
		}
	}

	fn url(href: &str) -> InternalUrl {
		InternalUrl::parse(href).expect("parse url")
	}

	fn context() -> ResolveContext {
		ResolveContext::default()
	}

	#[test]
	fn reads_skill_md_and_relative_files() {
		let root = temp_dir();
		let entry = entry(&root, "deploy", "# Deploy\nbody");
		write(&root.join("deploy/scripts/run.sh"), "#!/bin/sh\necho hi\n");

		let resource = resolve_entry(&entry, &url("skill://deploy"), &context()).expect("resolve");
		assert!(resource.content.contains("# Deploy"));
		assert_eq!(resource.content_type, "text/markdown");
		assert_eq!(resource.source_path, Some(entry.skill.path.clone()));
		assert!(!resource.is_directory);
		assert_eq!(resource.size, fs::metadata(&entry.skill.path).expect("stat").len() as usize);

		let script = resolve_entry(&entry, &url("skill://deploy/scripts/run.sh"), &context()).expect("resolve");
		assert_eq!(script.content, "#!/bin/sh\necho hi\n");
		assert_eq!(script.content_type, "text/plain");
		assert_eq!(script.source_path, Some(root.join("deploy/scripts/run.sh")));

		let dir = resolve_entry(&entry, &url("skill://deploy/scripts"), &context()).expect("resolve");
		assert!(dir.is_directory);
		assert_eq!(dir.content, "run.sh");
	}

	#[test]
	fn rejects_absolute_and_traversal_paths() {
		let root = temp_dir();
		let entry = entry(&root, "deploy", "body");
		write(&root.join("outside.txt"), "secret");
		for href in [
			"skill://deploy/../outside.txt",
			"skill://deploy/..%2F..%2Fetc%2Fpasswd",
			"skill://deploy/%2Fetc%2Fpasswd",
		] {
			let error = resolve_entry(&entry, &url(href), &context()).expect_err("must refuse");
			assert!(
				error.contains("traversal") || error.contains("Absolute"),
				"{href} -> {error}"
			);
		}
		assert!(validate_relative_path("/etc/passwd").is_err());
		assert!(validate_relative_path("a/../b").is_err());
		assert!(validate_relative_path("a/b").is_ok());
	}

	#[test]
	fn symlink_escape_is_refused() {
		let root = temp_dir();
		let entry = entry(&root, "deploy", "body");
		let secret = root.join("secret.txt");
		write(&secret, "secret");
		#[cfg(unix)]
		std::os::unix::fs::symlink(&secret, root.join("deploy/link.txt")).expect("symlink");
		let error = resolve_entry(&entry, &url("skill://deploy/link.txt"), &context()).expect_err("refuse");
		assert_eq!(error, "Path traversal is not allowed");
	}

	#[test]
	fn plugin_root_containment_allows_inside_and_rejects_outside() {
		let root = temp_dir();
		let mut entry = entry(&root.join("plugin/skills"), "deploy", "body");
		entry.contain_root = Some(root.join("plugin"));
		write(&root.join("plugin/templates/x.yaml"), "a: 1\n");
		write(&root.join("outside.txt"), "outside");
		#[cfg(unix)]
		{
			std::os::unix::fs::symlink(
				root.join("plugin/templates/x.yaml"),
				root.join("plugin/skills/deploy/ok.yaml"),
			)
			.expect("symlink inside");
			std::os::unix::fs::symlink(root.join("outside.txt"), root.join("plugin/skills/deploy/bad.txt"))
				.expect("symlink outside");
		}
		let ok = resolve_entry(&entry, &url("skill://deploy/ok.yaml"), &context()).expect("inside root");
		assert_eq!(ok.content, "a: 1\n");
		let error = resolve_entry(&entry, &url("skill://deploy/bad.txt"), &context()).expect_err("outside root");
		assert!(error.contains("outside the plugin root"), "{error}");
	}

	#[tokio::test]
	async fn unknown_skill_lists_available_names() {
		let root = temp_dir();
		let snapshot = SkillSnapshot::new();
		snapshot.set(vec![entry(&root, "deploy", "body"), entry(&root, "review", "body")]);
		let handler = SkillUrlHandler::new(snapshot);
		let error = handler
			.resolve(&url("skill://missing"), &context())
			.await
			.err()
			.expect("unknown skill");
		assert!(error.starts_with("Unknown skill: missing\nAvailable: "));
		assert!(error.contains("deploy") && error.contains("review"));
		let missing_name = handler
			.resolve(&url("skill://"), &context())
			.await
			.err()
			.expect("name");
		assert!(missing_name.contains("requires a skill name"));
	}

	#[tokio::test]
	async fn handler_reads_from_the_callers_snapshot() {
		let root = temp_dir();
		let first = SkillSnapshot::new();
		first.set(vec![entry(&root, "deploy", "first session")]);
		let second = SkillSnapshot::new();
		second.set(vec![entry(&root.join("second"), "deploy", "second session")]);

		let first_handler = SkillUrlHandler::new(first);
		let second_handler = SkillUrlHandler::new(second);
		let a = first_handler.resolve(&url("skill://deploy"), &context()).await.expect("first");
		let b = second_handler.resolve(&url("skill://deploy"), &context()).await.expect("second");
		assert!(a.content.contains("first session"));
		assert!(b.content.contains("second session"));
	}
}
