//! Built-in `agent://` read handler: saved subagent output only.
//!
//! Mirrors `packages/coding-agent/src/internal-urls/agent-protocol.ts` and its
//! `json-query.ts` subset. This scheme never exposes a live transcript or a
//! session-history view; `history://` owns that. The backing files are the
//! `<id>.md` artifacts written by the task executor into the caller's session
//! artifacts directory, so an id remains readable after the producing process
//! exits.
//!
//! URL forms:
//! - `agent://<id>` — full saved output
//! - `agent://<id>/<child>` — nested output (`<id>.<child>.md`) when the
//!   slash path names a real nested id; otherwise JSON extraction on `<id>.md`
//! - `agent://<id>/<path>` — JSON extraction (jq-like path form)
//! - `agent://<id>?q=<query>` — JSON extraction (explicit query form)
//!
//! Roots come only from the immutable per-call [`ResolveContext`]: the owning
//! producer artifacts root, bound as `artifact://` for a child scope and as
//! `local://` for a root scope. `session_file` is deliberately not used: child
//! transcripts live under `<producer>/subagents`, while every saved output is
//! in the shared `<producer>/<outputId>.md` namespace. There is no
//! process-global session scan, so one session cannot read another session's
//! outputs by guessing an id.

use std::{
	fs,
	io::Read,
	path::PathBuf,
};

use async_trait::async_trait;
use serde_json::Value;

use super::{
	internal_urls::{
		InternalResource, InternalUrl, LOCAL_SCHEME, ProtocolHandler, ResolveContext, decode_utf8,
	},
	internal_urls_artifact::{ARTIFACT_SCHEME, open_artifact_file},
	internal_urls_github::{percent_decode, query_param},
};

/// Scheme for saved subagent outputs.
pub const AGENT_SCHEME: &str = "agent";

/// The single owning producer artifacts root for this call.
///
/// A root scope binds it as `local`; a child scope pins the same producer root
/// as `artifact` while its own `local` is the transcript-only
/// `<producer>/subagents` directory. Never fall back to `session_file`: that
/// would resolve against the child transcript directory.
#[must_use]
pub fn agent_roots(context: &ResolveContext) -> Vec<PathBuf> {
	context
		.local_roots
		.get(ARTIFACT_SCHEME)
		.or_else(|| context.local_roots.get(LOCAL_SCHEME))
		.cloned()
		.into_iter()
		.collect()
}

/// Parsed `agent://` target, split the same way `parseInternalUrl` plus
/// `URLSearchParams` split a normal URL (authority ends at `/`, `?`, `#`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentTarget {
	pub output_id: String,
	/// URL path form, still percent-encoded.
	pub path:      String,
	/// First `q` parameter, percent-decoded; `Some("")` for `?q=`.
	pub query:     Option<String>,
}

impl AgentTarget {
	fn has_path(&self) -> bool {
		!self.path.is_empty() && self.path != "/"
	}

	fn has_query(&self) -> bool {
		self.query.as_deref().is_some_and(|query| !query.is_empty())
	}

	/// Read-tool extraction shape: a non-root path or a non-empty `?q=`.
	pub(crate) fn has_extraction(&self) -> bool {
		self.has_path() || self.has_query()
	}

	/// `Parent/Child` resolves `Parent.Child`; a segment containing `.` marks
	/// JSON-path syntax instead of a hierarchy hop.
	fn nested_id(&self) -> Option<String> {
		if !self.has_path() {
			return None;
		}
		let segments: Vec<String> = self
			.path
			.split('/')
			.filter(|part| !part.is_empty())
			.map(|segment| percent_decode(segment).unwrap_or_else(|| segment.to_string()))
			.collect();
		if segments.is_empty() || segments.iter().any(|segment| segment.contains('.')) {
			return None;
		}
		Some(format!("{}.{}", self.output_id, segments.join(".")))
	}
}

fn parse_target(url: &InternalUrl) -> Result<AgentTarget, String> {
	let mut host = url.host.as_str();
	let mut path = url.path.as_str();
	let mut raw_query: Option<String> = None;

	// The authority ends at the first `/`, `?`, or `#`. `InternalUrl::parse`
	// only splits on `/`, so a query in the authority may already contain the
	// later path text; put it back where URLSearchParams would see it.
	let question = host.find('?');
	let hash = host.find('#');
	if question.is_some() || hash.is_some() {
		let query_first = match (question, hash) {
			(Some(question), Some(hash)) => question < hash,
			(Some(_), None) => true,
			_ => false,
		};
		if query_first {
			let at = question.expect("checked");
			let after_question = &host[at + 1..];
			// If the fragment starts in the authority, the slash-split `path`
			// is fragment text too; appending it would extend the query past
			// `#` (URLSearchParams sees only the query before the fragment).
			let tail = if after_question.contains('#') {
				after_question.to_string()
			} else {
				format!("{after_question}{path}")
			};
			raw_query = Some(
				tail
					.split('#')
					.next()
					.unwrap_or_default()
					.to_string(),
			);
			host = &host[..at];
			path = "";
		} else {
			let at = hash.expect("checked");
			host = &host[..at];
			path = "";
		}
	} else {
		// URL fragment precedence: the first `#` starts the fragment, so a
		// later `?` inside it is not a query.
		let question = path.find('?');
		let hash = path.find('#');
		match (question, hash) {
			(Some(question), Some(hash)) if question < hash => {
				raw_query = Some(path[question + 1..hash].to_string());
				path = &path[..question];
			},
			(Some(question), None) => {
				raw_query = Some(path[question + 1..].to_string());
				path = &path[..question];
			},
			(_, Some(hash)) => {
				path = &path[..hash];
			},
			(None, None) => {},
		}
	}

	let output_id = percent_decode(host).unwrap_or_else(|| host.to_string());
	let query = raw_query.as_deref().and_then(|query| query_param(query, "q"));
	Ok(AgentTarget {
		output_id,
		path: path.to_string(),
		query,
	})
}

/// Whether a read-tool URL has the agent extraction shape. Used before routing
/// so line selectors are refused with the exact upstream message and so the
/// extracted body bypasses pagination.
#[must_use]
pub(crate) fn has_extraction(raw: &str) -> bool {
	InternalUrl::parse(raw)
		.and_then(|url| parse_target(&url).ok())
		.map_or(false, |target| target.has_extraction())
}

/// Built-in read-only `agent://` handler.
pub struct AgentUrlHandler;

#[async_trait]
impl ProtocolHandler for AgentUrlHandler {
	fn scheme(&self) -> &str {
		AGENT_SCHEME
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
		let target = parse_target(url)?;
		if target.output_id.is_empty() {
			return Err("agent:// URL requires an output ID: agent://<id>".to_string());
		}
		if target.has_path() && target.has_query() {
			return Err("agent:// URL cannot combine path extraction with ?q=".to_string());
		}
		let roots = agent_roots(context);
		if roots.is_empty() {
			return Err("No session - agent outputs unavailable".to_string());
		}

		let href = url.href.clone();
		let signal = context.signal.clone();
		let target_clone = target.clone();
		let scan = tokio::task::spawn_blocking(move || resolve_in_roots(&target_clone, &roots, &signal))
			.await
			.map_err(|error| format!("{href}: agent output lookup task failed: {error}"))??;

		let nested_id = target.nested_id();
		// A slash that named a real nested id is a hierarchy hop, not a jq path.
		let extract = target.has_query()
			|| (target.has_path() && nested_id.as_deref() != Some(scan.matched_id.as_str()));
		let (content, content_type, notes) = if extract {
			let query = if target.has_query() {
				target.query.as_deref().unwrap_or_default().to_string()
			} else {
				path_to_query(&target.path)
			};
			let (content, note) = extract_json(&scan.content, &scan.matched_id, &query)?;
			(content, "application/json".to_string(), note)
		} else {
			(scan.content, "text/markdown".to_string(), None)
		};
		let mut resource = InternalResource::text(&url.href, content);
		resource.content_type = content_type;
		resource.source_path = Some(scan.path);
		resource.immutable = true;
		resource.notes = notes;
		Ok(resource)
	}
}

struct ScanResult {
	matched_id: String,
	path:       PathBuf,
	content:    String,
}

fn resolve_in_roots(
	target: &AgentTarget,
	roots: &[PathBuf],
	signal: &crate::CancellationToken,
) -> Result<ScanResult, String> {
	let nested_id = target.nested_id();
	let candidate_ids: Vec<&str> = match nested_id.as_deref() {
		Some(nested) => vec![nested, target.output_id.as_str()],
		None => vec![target.output_id.as_str()],
	};

	let mut by_id: Vec<(String, PathBuf)> = Vec::new();
	let mut available: Vec<String> = Vec::new();
	let mut any_dir_exists = false;
	for root in roots {
		if signal.is_cancelled() {
			return Err("aborted".to_string());
		}
		let entries = match fs::read_dir(root) {
			Ok(entries) => entries,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
			Err(error) => {
				return Err(format!("artifacts directory {}: {error}", root.display()));
			},
		};
		any_dir_exists = true;
		for entry in entries {
			let entry =
				entry.map_err(|error| format!("artifacts directory {}: {error}", root.display()))?;
			let name = entry.file_name().to_string_lossy().into_owned();
			let Some(id) = name.strip_suffix(".md") else {
				continue;
			};
			if !available.iter().any(|known| known == id) {
				available.push(id.to_string());
			}
			if !by_id.iter().any(|(known, _)| known == id) {
				by_id.push((id.to_string(), root.join(&name)));
			}
		}
	}

	if !any_dir_exists {
		return Err("No artifacts directory found".to_string());
	}
	// Test- and human-stable listing; TS leaves this in readdir order, which is
	// filesystem-defined, so no consumer can depend on a different ordering.
	available.sort();

	let found = candidate_ids
		.iter()
		.find_map(|candidate| by_id.iter().find(|(id, _)| id.as_str() == *candidate));
	let Some((matched_id, path)) = found else {
		let target_id = nested_id.as_deref().unwrap_or(target.output_id.as_str());
		let list = if available.is_empty() {
			"none".to_string()
		} else {
			available.join(", ")
		};
		return Err(format!("Not found: {target_id}\nAvailable: {list}"));
	};

	let name = path
		.file_name()
		.map(|name| name.to_string_lossy().into_owned())
		.ok_or_else(|| format!("agent output {} has no file name", path.display()))?;
	let root = path
		.parent()
		.ok_or_else(|| format!("agent output {} has no parent directory", path.display()))?;
	let mut file = open_artifact_file(root, &name)
		.map_err(|error| format!("agent output {matched_id}: {error}"))?;
	let mut bytes: Vec<u8> = Vec::new();
	let mut chunk = [0u8; 64 * 1024];
	loop {
		if signal.is_cancelled() {
			return Err("aborted".to_string());
		}
		let read = file
			.read(&mut chunk)
			.map_err(|error| format!("agent output {matched_id}: {error}"))?;
		if read == 0 {
			break;
		}
		bytes.extend_from_slice(&chunk[..read]);
	}
	let content = decode_utf8(&bytes, false, &matched_id)?;
	Ok(ScanResult {
		matched_id: matched_id.clone(),
		path: path.clone(),
		content,
	})
}

/// TS `applyQuery`/`pathToQuery` result rendering. Missing paths render the
/// literal `null` JSON scalar, exactly like `JSON.stringify(undefined) ?? "null"`.
fn extract_json(
	raw: &str,
	matched_id: &str,
	query: &str,
) -> Result<(String, Option<Vec<String>>), String> {
	let value: Value = serde_json::from_str(raw)
		.map_err(|error| format!("Output {matched_id} is not valid JSON: {error}"))?;
	if query.is_empty() {
		let content = serde_json::to_string_pretty(&value)
			.unwrap_or_else(|_| "null".to_string());
		return Ok((content, None));
	}
	let extracted = apply_query(&value, query)?;
	let content = serde_json::to_string_pretty(extracted.unwrap_or(&Value::Null))
		.unwrap_or_else(|_| "null".to_string());
	Ok((content, Some(vec![format!("Extracted: {query}")])))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueryToken {
	Key(String),
	/// `None` is a syntactically numeric bracket that exceeds `usize`; TS
	/// `Number` indexes no real array, so the extraction is missing.
	Index(Option<usize>),
}

/// Parse the documented jq-like subset: `.foo`, `[0]`, `["special-key"]`,
/// `.foo.bar[0].baz`, `['single']`. Dots are optional separators. Error text
/// matches `json-query.ts`.
fn parse_query(query: &str) -> Result<Vec<QueryToken>, String> {
	let mut input = query.trim();
	if let Some(rest) = input.strip_prefix('.') {
		input = rest;
	}
	if input.is_empty() {
		return Ok(Vec::new());
	}
	let bytes = input.as_bytes();
	let mut tokens = Vec::new();
	let mut index = 0usize;
	while index < bytes.len() {
		match bytes[index] {
			b'.' => index += 1,
			b'[' => {
				let Some(offset) = input[index + 1..].find(']') else {
					return Err(format!("Invalid query: missing ] in {query}"));
				};
				let close = index + 1 + offset;
				let raw = input[index + 1..close].trim();
				if raw.is_empty() {
					return Err(format!("Invalid query: empty [] in {query}"));
				}
				let first = raw.chars().next().unwrap_or_default();
				if (first == '"' || first == '\'') && raw.ends_with(first) {
					let inner = raw.get(1..raw.len().saturating_sub(1)).unwrap_or("");
					tokens.push(QueryToken::Key(unescape_quoted(inner)));
				} else if raw.bytes().all(|byte| byte.is_ascii_digit()) {
					tokens.push(QueryToken::Index(raw.parse::<usize>().ok()));
				} else {
					tokens.push(QueryToken::Key(raw.to_string()));
				}
				index = close + 1;
			},
			byte if is_identifier_byte(byte) => {
				let start = index;
				while index < bytes.len() && is_identifier_byte(bytes[index]) {
					index += 1;
				}
				tokens.push(QueryToken::Key(input[start..index].to_string()));
			},
			_ => {
				let token = input[index..].chars().next().unwrap_or_default();
				return Err(format!(
					"Invalid query: unexpected token '{token}' in {query}"
				));
			},
		}
	}
	Ok(tokens)
}

fn is_identifier_byte(byte: u8) -> bool {
	byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

fn unescape_quoted(raw: &str) -> String {
	let mut out = String::with_capacity(raw.len());
	let mut chars = raw.chars();
	while let Some(ch) = chars.next() {
		if ch == '\\'
			&& let Some(next) = chars.next()
			&& matches!(next, '"' | '\'' | '\\')
		{
			out.push(next);
			continue;
		}
		out.push(ch);
	}
	out
}

/// Apply a parsed query. `None` is TS `undefined`, rendered as `null` by the
/// caller. Returned by reference so a large extraction is not copied before
/// serialization.
fn apply_query<'a>(data: &'a Value, query: &str) -> Result<Option<&'a Value>, String> {
	let tokens = parse_query(query)?;
	let mut current: Option<&Value> = Some(data);
	for token in tokens {
		let Some(value) = current else {
			return Ok(None);
		};
		if value.is_null() {
			return Ok(None);
		}
		current = match token {
			QueryToken::Index(index) => {
				index.and_then(|index| value.as_array().and_then(|array| array.get(index)))
			},
			QueryToken::Key(key) => match value {
				Value::Object(object) => object.get(&key),
				Value::Array(array) => key
					.parse::<usize>()
					.ok()
					.and_then(|index| array.get(index)),
				_ => None,
			},
		};
	}
	Ok(current)
}

/// TS `pathToQuery`: `/foo/bar/0` -> `.foo.bar[0]`.
fn path_to_query(url_path: &str) -> String {
	let mut parts = Vec::new();
	for segment in url_path.split('/').filter(|part| !part.is_empty()) {
		let decoded = percent_decode(segment).unwrap_or_else(|| segment.to_string());
		if !decoded.is_empty() && decoded.bytes().all(|byte| byte.is_ascii_digit()) {
			parts.push(format!("[{decoded}]"));
		} else if !decoded.is_empty()
			&& decoded
				.bytes()
				.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
		{
			parts.push(format!(".{decoded}"));
		} else {
			let escaped = decoded.replace('\\', "\\\\").replace('\'', "\\'");
			parts.push(format!("['{escaped}']"));
		}
	}
	parts.join("")
}

#[cfg(test)]
mod tests {
	use std::{
		collections::BTreeMap,
		fs,
		path::{Path, PathBuf},
		time::{SystemTime, UNIX_EPOCH},
	};

	use super::*;
	use crate::CancellationToken;

	struct TempRoot(PathBuf);

	impl TempRoot {
		fn new(label: &str) -> Self {
			let nanos = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.map(|duration| duration.as_nanos())
				.unwrap_or_default();
			let home = std::env::var_os("HOME")
				.map(PathBuf::from)
				.expect("HOME must be set for agent output tests");
			let path = home.join(format!(
				".omp-agent-uri-{label}-{}-{nanos}",
				std::process::id()
			));
			fs::create_dir_all(&path).expect("create root");
			Self(path)
		}
	}

	impl Drop for TempRoot {
		fn drop(&mut self) {
			let _ = fs::remove_dir_all(&self.0);
		}
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

	fn resolve(url: &str, context: &ResolveContext) -> Result<InternalResource, String> {
		runtime().block_on(AgentUrlHandler.resolve(&parse(url), context))
	}

	#[test]
	fn full_output_resolves_saved_markdown_immutably() {
		let root = TempRoot::new("full");
		fs::write(root.0.join("Saved.md"), "hello output\n").expect("output");
		let resource = resolve("agent://Saved", &scope(&root.0)).expect("resolve");
		assert_eq!(resource.content, "hello output\n");
		assert_eq!(resource.content_type, "text/markdown");
		assert_eq!(resource.source_path, Some(root.0.join("Saved.md")));
		assert!(resource.immutable);
		assert!(!resource.writable);
	}

	#[test]
	fn empty_aborted_output_still_resolves() {
		let root = TempRoot::new("empty-abort");
		fs::write(root.0.join("Aborted.md"), "").expect("empty output");
		let resource = resolve("agent://Aborted", &scope(&root.0)).expect("resolve");
		assert_eq!(resource.content, "");
	}

	#[test]
	fn exact_errors_for_missing_id_session_directory_and_available_ids() {
		let empty_scope = ResolveContext::default();
		assert_eq!(
			resolve("agent://", &empty_scope).expect_err("id"),
			"agent:// URL requires an output ID: agent://<id>"
		);
		assert_eq!(
			resolve("agent://Saved", &empty_scope).expect_err("session"),
			"No session - agent outputs unavailable"
		);

		let missing_root = TempRoot::new("missing-root");
		let absent = missing_root.0.join("does-not-exist");
		assert_eq!(
			resolve("agent://Saved", &scope(&absent)).expect_err("dir"),
			"No artifacts directory found"
		);

		let root = TempRoot::new("available");
		fs::write(root.0.join("Alpha.md"), "a").expect("a");
		fs::write(root.0.join("Beta.md"), "b").expect("b");
		assert_eq!(
			resolve("agent://Saved", &scope(&root.0)).expect_err("not found"),
			"Not found: Saved\nAvailable: Alpha, Beta"
		);
	}

	#[test]
	fn nested_slash_wins_over_json_extraction() {
		let root = TempRoot::new("nested");
		fs::write(root.0.join("Parent.md"), r#"{"Child":"json-value"}"#).expect("parent");
		fs::write(root.0.join("Parent.Child.md"), "nested output").expect("nested");
		let resource = resolve("agent://Parent/Child", &scope(&root.0)).expect("resolve");
		assert_eq!(resource.content, "nested output");
		assert_eq!(resource.content_type, "text/markdown");
	}

	#[test]
	fn dotted_path_falls_back_to_json_extraction() {
		let root = TempRoot::new("dotted");
		fs::write(root.0.join("Saved.md"), r#"{"Child":{"leaf":7}}"#).expect("json");
		let resource = resolve("agent://Saved/Child/leaf", &scope(&root.0)).expect("resolve");
		assert_eq!(resource.content, "7");
		assert_eq!(resource.content_type, "application/json");
		assert_eq!(
			resource.notes,
			Some(vec!["Extracted: .Child.leaf".to_string()])
		);
	}

	#[test]
	fn query_extraction_matches_the_documented_subset() {
		let root = TempRoot::new("query");
		fs::write(
			root.0.join("Saved.md"),
			r#"{"foo":{"bar":[1,2,3]},"special-key":"v"}"#,
		)
		.expect("json");
		assert_eq!(
			resolve("agent://Saved?q=.foo.bar[1]", &scope(&root.0))
				.expect("index")
				.content,
			"2"
		);
		assert_eq!(
			resolve("agent://Saved?q=%5B'special-key'%5D", &scope(&root.0))
				.expect("quoted")
				.content,
			"\"v\""
		);
		assert_eq!(
			resolve("agent://Saved?q=.missing", &scope(&root.0))
				.expect("missing")
				.content,
			"null"
		);
	}

	#[test]
	fn numeric_bracket_uses_ts_number_semantics_on_objects_and_overflow() {
		let root = TempRoot::new("numeric-brackets");
		fs::write(
			root.0.join("Saved.md"),
			r#"{"0":"zero","foo bar":"x","arr":[1]}"#,
		)
		.expect("json");
		assert_eq!(
			resolve("agent://Saved?q=%5B%220%22%5D", &scope(&root.0))
				.expect("quoted numeric key")
				.content,
			"\"zero\"",
			"a quoted numeric bracket is an object key"
		);
		assert_eq!(
			resolve("agent://Saved?q=%5Bfoo%20bar%5D", &scope(&root.0))
				.expect("bare key")
				.content,
			"\"x\""
		);
		assert_eq!(
			resolve("agent://Saved?q=%5B0%5D", &scope(&root.0))
				.expect("number token on object")
				.content,
			"null",
			"TS number tokens index arrays only, never object properties"
		);
		assert_eq!(
			resolve("agent://Saved?q=%5B999999999999999999999%5D", &scope(&root.0))
				.expect("overflow number token")
				.content,
			"null"
		);
	}

	#[test]
	fn fragment_precedes_a_later_question_mark() {
		let root = TempRoot::new("fragment");
		fs::write(root.0.join("Saved.md"), r#"{"foo":1,"Child":2}"#).expect("json");
		let full = resolve("agent://Saved#frag?q=.foo", &scope(&root.0)).expect("fragment");
		assert_eq!(full.content, r#"{"foo":1,"Child":2}"#);
		assert_eq!(full.content_type, "text/markdown");
		assert_eq!(
			resolve("agent://Saved?q=.foo#frag", &scope(&root.0))
				.expect("query before fragment")
				.content,
			"1"
		);
		// The `/path` after `#` is fragment text, not query text: do not append
		// it to `q` just because `InternalUrl` split at the slash.
		assert_eq!(
			resolve("agent://Saved?q=.foo#frag/path", &scope(&root.0))
				.expect("query before fragment with slash")
				.content,
			"1"
		);
	}

	#[test]
	fn path_and_query_extraction_are_mutually_exclusive() {
		let root = TempRoot::new("conflict");
		fs::write(root.0.join("Saved.md"), "{}").expect("json");
		assert_eq!(
			resolve("agent://Saved/foo?q=.bar", &scope(&root.0)).expect_err("conflict"),
			"agent:// URL cannot combine path extraction with ?q="
		);
	}

	#[test]
	fn extraction_requires_valid_json() {
		let root = TempRoot::new("invalid-json");
		fs::write(root.0.join("Saved.md"), "not json").expect("text");
		let error = resolve("agent://Saved?q=.foo", &scope(&root.0)).expect_err("invalid");
		assert!(
			error.starts_with("Output Saved is not valid JSON: "),
			"unexpected: {error}"
		);
	}

	#[test]
	fn producer_artifact_root_wins_over_the_child_session_directory() {
		let child = TempRoot::new("child-local");
		let producer = TempRoot::new("producer");
		let subagents = child.0.join("subagents");
		fs::create_dir_all(&subagents).expect("subagents dir");
		fs::write(subagents.join("Main.md"), "wrong child output").expect("child copy");
		fs::write(producer.0.join("Main.md"), "producer output").expect("producer");
		let context = ResolveContext {
			session_file: Some(subagents.join("task-1.jsonl")),
			local_roots: BTreeMap::from([
				(ARTIFACT_SCHEME.to_string(), producer.0.clone()),
				(LOCAL_SCHEME.to_string(), subagents.clone()),
			]),
			..ResolveContext::default()
		};
		let resource = resolve("agent://Main", &context).expect("resolve");
		assert_eq!(resource.content, "producer output");
	}

	#[test]
	fn extraction_shape_is_detected_for_read() {
		assert!(!has_extraction("agent://Saved"));
		assert!(has_extraction("agent://Saved/Child"));
		assert!(has_extraction("agent://Saved?q=.foo"));
		assert!(!has_extraction("agent://Saved?q="));
	}

	#[test]
	fn query_parser_rejects_malformed_syntax_exactly() {
		assert_eq!(
			parse_query(".foo[").expect_err("missing"),
			"Invalid query: missing ] in .foo["
		);
		assert_eq!(
			parse_query(".foo[]").expect_err("empty"),
			"Invalid query: empty [] in .foo[]"
		);
		assert_eq!(
			parse_query(".foo bar").expect_err("token"),
			"Invalid query: unexpected token ' ' in .foo bar"
		);
	}

	#[test]
	fn path_to_query_matches_ts_forms() {
		assert_eq!(path_to_query("/foo/bar/0"), ".foo.bar[0]");
		assert_eq!(path_to_query("/special key"), "['special key']");
		assert_eq!(path_to_query("/"), "");
	}

	#[test]
	fn cancellation_is_reported() {
		let root = TempRoot::new("cancel");
		fs::write(root.0.join("Saved.md"), "x").expect("output");
		let mut context = scope(&root.0);
		context.signal = CancellationToken::new();
		context.signal.cancel();
		assert_eq!(resolve("agent://Saved", &context).expect_err("abort"), "aborted");
	}
}
