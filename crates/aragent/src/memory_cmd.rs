//! One user-facing memory command shared by `arag memory`, REPL `/memory`, and
//! headless callers.
//!
//! D53: the user owns global (owner preferences) and project (project
//! knowledge) records, with `asserted` provenance on explicit creation. SQLite
//! stays the only live authority: `learned.md` is rewritten from rows after a
//! project create/update/delete and is never re-imported after the one-time
//! migration. Deleting a record never touches conversation history.
//!
//! Every action prints the scope (`global`, `project:<cwd>`, or
//! `session:<id>`) and origin, so a user can tell an asserted preference from
//! an inferred agent lesson.

use std::{io::Write as _, path::Path};

use crate::{
	autolearn::MemoryBackend,
	memory_embeddings::{EmbeddingProvider, embed_best_effort_blocking},
	memory_protocol::{render_record, render_record_list, scope_label},
	memory_store::{self, Scope},
};

/// Actions accepted by the shared command.
const USAGE: &str = "Usage: arag memory <action> [--global|--project|--session ID] [--context TEXT]
Actions:
  list                         list visible records (global + this project)
  search <query>               lexical search (bounded)
  get <id>                     show one record with scope/origin
  create <text>                create a user-asserted record (default: project)
  update <id> <text>           replace the body; identity stays immutable
  delete <id>                  delete one record; conversation history is untouched

Scope flags: --global for owner preferences, --project (default for create)
for this project's knowledge, --session ID for one conversation. Mutually
exclusive.
Options are read only before the first body word; use `--` to start literal
text that begins with `--`.";

/// Run one memory command.
///
/// `argv` is the action plus its operands and flags. Callers that already have
/// a session (`/memory`) and the standalone CLI pass the same arguments.
pub fn execute(
	argv: &[String],
	agent_dir: &Path,
	cwd: &Path,
	backend: MemoryBackend,
	out: &mut dyn std::io::Write,
) -> Result<(), String> {
	execute_with_session(argv, agent_dir, cwd, None, backend, out)
}

/// [`execute`] with the calling conversation's id, so `list`, `search`, `get`,
/// and edits also see that session's own rows. An explicit `--session` flag
/// selects one conversation and overrides `default_session`.
pub fn execute_with_session(
	argv: &[String],
	agent_dir: &Path,
	cwd: &Path,
	default_session: Option<&str>,
	backend: MemoryBackend,
	out: &mut dyn std::io::Write,
) -> Result<(), String> {
	execute_with_session_and_embeddings(
		argv,
		agent_dir,
		cwd,
		default_session,
		None,
		backend,
		out,
	)
}

/// [`execute_with_session`] with an optional embedding provider. Search uses it
/// for hybrid recall; create stores the embedding best-effort. Provider errors
/// fall back to lexical FTS5 without failing the command.
pub fn execute_with_session_and_embeddings(
	argv: &[String],
	agent_dir: &Path,
	cwd: &Path,
	default_session: Option<&str>,
	provider: Option<&dyn EmbeddingProvider>,
	backend: MemoryBackend,
	out: &mut dyn std::io::Write,
) -> Result<(), String> {
	let parsed = parse(argv)?;
	let effective_session = parsed
		.session
		.as_deref()
		.or(default_session)
		.map(str::trim)
		.filter(|value| !value.is_empty());
	if parsed.scope == Some(Scope::Session) && effective_session.is_none() {
		return Err("--session requires a non-empty session id".to_string());
	}
	let Some(action) = parsed.action.as_deref() else {
		writeln!(out, "{USAGE}").map_err(|error| error.to_string())?;
		return Ok(());
	};
	if matches!(action, "help" | "--help" | "-h") {
		writeln!(out, "{USAGE}").map_err(|error| error.to_string())?;
		return Ok(());
	}
	if backend != MemoryBackend::Local {
		return Err(
			"memory backend is not local; set `memory.backend: local` to manage the SQLite memory store"
				.to_string(),
		);
	}
	match action {
		"list" | "ls" => {
			let records = match memory_store::list_scope_with_session(
				agent_dir,
				cwd,
				parsed.scope,
				effective_session,
			) {
				Ok(records) => records,
				Err(memory_store::MemoryStoreError::NotFound(_)) => Vec::new(),
				Err(error) => return Err(error.to_string()),
			};
			let rendered = render_record_list(&records);
			write!(out, "{rendered}").map_err(|error| error.to_string())?;
			if !records.is_empty() {
				writeln!(out).map_err(|error| error.to_string())?;
			}
		},
		"search" | "find" => {
			let query = rest_text(&parsed.rest);
			if query.is_empty() {
				return Err("search requires a query: arag memory search <query>".to_string());
			}
			// Redact/neutralize before a remote embedding call; lexical recall
			// still uses the query exactly as the user typed it.
			let embedding_query = crate::local_memory::normalize_lesson_for_embedding(&query);
			let embedding = if embedding_query.is_empty() {
				None
			} else {
				query_embedding(provider, &embedding_query)
			};
			let embedding = embedding
				.as_ref()
				.map(|(model, vector)| (model.as_str(), vector.as_slice()));
			let records = match memory_store::recall_scope_with_session_and_embedding(
				agent_dir,
				cwd,
				&query,
				parsed.scope,
				effective_session,
				embedding,
				None,
			) {
				Ok(records) => records,
				Err(memory_store::MemoryStoreError::NotFound(_)) => Vec::new(),
				Err(error) => return Err(error.to_string()),
			};
			write!(out, "{}", render_record_list(&records)).map_err(|error| error.to_string())?;
			if !records.is_empty() {
				writeln!(out).map_err(|error| error.to_string())?;
			}
		},
		"get" | "show" => {
			let id = parsed
				.rest
				.first()
				.ok_or_else(|| "get requires an id: arag memory get <id>".to_string())?;
			let record = memory_store::get_visible_with_session(
				agent_dir,
				cwd,
				effective_session,
				id,
			)
			.map_err(|error| error.to_string())?;
			write!(out, "{}", render_record(&record)).map_err(|error| error.to_string())?;
		},
		"create" | "add" | "new" => {
			let body = rest_text(&parsed.rest);
			if body.is_empty() {
				return Err("create requires text: arag memory create [--global] <text>".to_string());
			}
			let scope = parsed.scope.unwrap_or(Scope::Project);
			let create_session = if scope == Scope::Session {
				effective_session
			} else {
				None
			};
			let record = memory_store::create_asserted_with_session_and_export(
				agent_dir,
				cwd,
				create_session,
				scope,
				&body,
				parsed.context.as_deref(),
			)
			.map_err(|error| error.to_string())?;
			let embedding_text = crate::local_memory::normalize_lesson_for_embedding(&body);
			if let Some((model, vector)) = query_embedding(provider, &embedding_text)
				&& let Ok(store) = memory_store::MemoryStore::open_existing(agent_dir)
			{
				// Best effort: the lexical row is already committed.
				let _ = store.set_embedding(&record.id, &model, &vector);
			}
			writeln!(
				out,
				"Created {} [{}] origin={} actor={}",
				record.id,
				scope_label(&record),
				record.origin.as_str(),
				record.actor.as_deref().unwrap_or("-")
			)
			.map_err(|error| error.to_string())?;
		},
		"update" | "edit" => {
			let id = parsed
				.rest
				.first()
				.ok_or_else(|| "update requires an id: arag memory update <id> <text>".to_string())?;
			let body = rest_text(parsed.rest.get(1..).unwrap_or_default());
			if body.is_empty() {
				return Err(
					"update requires replacement text: arag memory update <id> <text>".to_string(),
				);
			}
			let record = memory_store::update_record_and_export_with_session(
				agent_dir,
				cwd,
				effective_session,
				id,
				&body,
			)
			.map_err(|error| error.to_string())?;
			writeln!(
				out,
				"Updated {} [{}] origin={}",
				record.id,
				crate::memory_protocol::scope_label(&record),
				record.origin.as_str()
			)
			.map_err(|error| error.to_string())?;
		},
		"delete" | "rm" => {
			let id = parsed
				.rest
				.first()
				.ok_or_else(|| "delete requires an id: arag memory delete <id>".to_string())?;
			let record = memory_store::get_visible_with_session(
				agent_dir,
				cwd,
				effective_session,
				id,
			)
			.map_err(|error| error.to_string())?;
			memory_store::delete_record_and_export_with_session(
				agent_dir,
				cwd,
				effective_session,
				id,
			)
			.map_err(|error| error.to_string())?;
			writeln!(
				out,
				"Deleted {} [{}] origin={}. Conversation history is untouched.",
				record.id,
				crate::memory_protocol::scope_label(&record),
				record.origin.as_str()
			)
			.map_err(|error| error.to_string())?;
		},
		other => {
			return Err(format!("unknown memory action '{other}'.\n{USAGE}"));
		},
	}
	Ok(())
}

fn query_embedding(provider: Option<&dyn EmbeddingProvider>, text: &str) -> Option<(String, Vec<f32>)> {
	let provider = provider?;
	let texts = vec![text.to_string()];
	let vector = embed_best_effort_blocking(Some(provider), &texts)?.into_iter().next()?;
	Some((provider.model().to_string(), vector))
}

#[derive(Debug, Default)]
struct Parsed {
	action:  Option<String>,
	rest:    Vec<String>,
	scope:   Option<Scope>,
	session: Option<String>,
	context: Option<String>,
}

fn parse(argv: &[String]) -> Result<Parsed, String> {
	let mut action = None;
	let mut rest = Vec::new();
	let mut scope = None;
	let mut session = None;
	let mut context = None;
	let mut end_of_options = false;
	let mut index = 0;
	while index < argv.len() {
		let value = argv[index].as_str();
		// Options are recognized only before the first body token, so text
		// containing `--global`, `--project`, or `--context` stays literal.
		let options_open = !end_of_options && rest.is_empty();
		if options_open {
			match value {
				"--" => {
					end_of_options = true;
					index += 1;
					continue;
				},
				"--global" => {
					if matches!(scope, Some(Scope::Project | Scope::Session)) {
						return Err("--global conflicts with --project/--session".to_string());
					}
					scope = Some(Scope::Global);
					index += 1;
					continue;
				},
				"--project" => {
					if matches!(scope, Some(Scope::Global | Scope::Session)) {
						return Err("--project conflicts with --global/--session".to_string());
					}
					scope = Some(Scope::Project);
					index += 1;
					continue;
				},
				"--session" => {
					if matches!(scope, Some(Scope::Global | Scope::Project)) {
						return Err("--session conflicts with --global/--project".to_string());
					}
					if session.is_some() {
						return Err("--session was supplied more than once".to_string());
					}
					index += 1;
					let value = argv
						.get(index)
						.ok_or_else(|| "--session requires a value".to_string())?;
					let value = value.trim();
					if value.starts_with("--") {
						return Err("--session requires a session id, not another option".to_string());
					}
					if value.is_empty() {
						return Err("--session requires a non-empty session id".to_string());
					}
					session = Some(value.to_string());
					scope = Some(Scope::Session);
					index += 1;
					continue;
				},
				"--context" => {
					index += 1;
					let value = argv
						.get(index)
						.ok_or_else(|| "--context requires a value".to_string())?;
					context = Some(value.clone());
					index += 1;
					continue;
				},
				_ => {},
			}
		}
		if action.is_none() {
			action = Some(value.to_string());
		} else {
			rest.push(value.to_string());
		}
		index += 1;
	}
	Ok(Parsed { action, rest, scope, session, context })
}

/// Join operands with single spaces, preserving embedded text as typed.
fn rest_text(rest: &[String]) -> String {
	let mut out = String::new();
	for (index, part) in rest.iter().enumerate() {
		if index > 0 {
			out.push(' ');
		}
		out.push_str(part);
	}
	out
}

#[cfg(test)]
mod tests {
	use std::{
		fs,
		path::PathBuf,
		sync::atomic::{AtomicU32, Ordering},
	};

	use super::*;

	static COUNTER: AtomicU32 = AtomicU32::new(0);

	fn temp_dir() -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"omp-memory-cmd-{}-{}",
			std::process::id(),
			COUNTER.fetch_add(1, Ordering::Relaxed)
		));
		fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	fn run(argv: &[&str], agent_dir: &Path, cwd: &Path) -> Result<String, String> {
		let argv = argv
			.iter()
			.map(|value| (*value).to_string())
			.collect::<Vec<_>>();
		let mut out = Vec::new();
		execute(&argv, agent_dir, cwd, MemoryBackend::Local, &mut out)?;
		String::from_utf8(out).map_err(|error| error.to_string())
	}

	#[test]
	fn create_list_isolate_update_delete_and_reopen() {
		let dir = temp_dir();
		let agent_dir = dir.join("agent");
		let project = dir.join("project");
		let other = dir.join("other");
		fs::create_dir_all(&project).expect("project");
		fs::create_dir_all(&other).expect("other");

		let global = run(&["create", "--global", "prefer terse answers"], &agent_dir, &project)
			.expect("create global");
		assert!(global.contains("[global] origin=asserted actor=user"), "{global}");
		let project_record = run(&["create", "run formatter before commit"], &agent_dir, &project)
			.expect("create project");
		assert!(
			project_record.contains(&format!("[project:{}] origin=asserted", project.display())),
			"{project_record}"
		);

		// Same project sees both; a different project sees only the global row.
		let listed = run(&["list"], &agent_dir, &project).expect("list project");
		assert!(listed.contains("prefer terse answers"), "{listed}");
		assert!(listed.contains("run formatter before commit"), "{listed}");
		let other_list = run(&["list"], &agent_dir, &other).expect("list other");
		assert!(other_list.contains("prefer terse answers"), "{other_list}");
		assert!(!other_list.contains("run formatter before commit"), "{other_list}");

		let searched = run(&["search", "formatter"], &agent_dir, &project).expect("search");
		assert!(searched.contains("run formatter before commit"), "{searched}");
		let other_search = run(&["search", "formatter"], &agent_dir, &other).expect("search other");
		assert!(other_search.contains("No memory records"), "{other_search}");

		let id = listed
			.lines()
			.find(|line| line.starts_with("- ") && line.contains("[project:"))
			.and_then(|line| line.strip_prefix("- "))
			.and_then(|line| line.split_whitespace().next())
			.expect("project id")
			.to_string();
		let updated = run(
			&["update", id.as_str(), "run tests before commit"],
			&agent_dir,
			&project,
		)
		.expect("update");
		assert!(updated.contains(&format!("Updated {id} [project:")), "{updated}");
		let got = run(&["get", id.as_str()], &agent_dir, &project).expect("get");
		assert!(got.contains("origin: asserted"), "{got}");
		assert!(got.contains("run tests before commit"), "{got}");
		assert!(!got.contains("run formatter before commit"), "{got}");

		// Reopen (fresh command run) keeps the edit.
		let reopened = run(&["get", id.as_str()], &agent_dir, &project).expect("reopen");
		assert!(reopened.contains("run tests before commit"), "{reopened}");

		run(&["delete", id.as_str()], &agent_dir, &project).expect("delete");
		assert!(run(&["get", id.as_str()], &agent_dir, &project).is_err());
		let after = run(&["list"], &agent_dir, &project).expect("list after delete");
		assert!(!after.contains("run tests before commit"), "{after}");
		// The derived export must not resurrect the deleted row.
		let learned = fs::read_to_string(
			crate::local_memory::memory_root(&agent_dir, &project).join("learned.md"),
		)
		.unwrap_or_default();
		assert!(!learned.contains("run tests before commit"), "{learned}");
	}

	#[test]
	fn scope_flag_conflicts_and_backend_gate_are_explicit() {
		let dir = temp_dir();
		let argv = vec!["list".to_string(), "--global".to_string(), "--project".to_string()];
		let mut out = Vec::new();
		let error = execute(&argv, &dir, &dir, MemoryBackend::Local, &mut out).expect_err("conflict");
		assert!(error.contains("conflicts"), "{error}");

		let argv = vec!["list".to_string()];
		let error =
			execute(&argv, &dir, &dir, MemoryBackend::Off, &mut out).expect_err("backend gate");
		assert!(error.contains("memory.backend: local"), "{error}");

		// A missing session value must not swallow the next option as an id.
		let argv = vec![
			"list".to_string(),
			"--session".to_string(),
			"--global".to_string(),
		];
		let error = parse(&argv).expect_err("option is not a session id");
		assert!(error.contains("session id"), "{error}");
	}

	#[test]
	fn options_stop_at_the_first_body_token() {
		let parsed = parse(&["create".to_string(), "use".to_string(), "--global".to_string()])
			.expect("literal flag");
		assert_eq!(parsed.action.as_deref(), Some("create"));
		assert_eq!(parsed.scope, None, "flag after body is literal text");
		assert_eq!(parsed.rest, ["use", "--global"]);

		let parsed = parse(&[
			"create".to_string(),
			"--".to_string(),
			"--global".to_string(),
		])
		.expect("end of options");
		assert_eq!(parsed.action.as_deref(), Some("create"));
		assert_eq!(parsed.scope, None);
		assert_eq!(parsed.rest, ["--global"]);

		// Documented before-action / before-body forms still work.
		let parsed = parse(&[
			"--global".to_string(),
			"create".to_string(),
			"keep".to_string(),
		])
		.expect("before action");
		assert_eq!(parsed.action.as_deref(), Some("create"));
		assert_eq!(parsed.scope, Some(Scope::Global));
		assert_eq!(parsed.rest, ["keep"]);

		let parsed = parse(&[
			"create".to_string(),
			"--context".to_string(),
			"ctx".to_string(),
			"body".to_string(),
			"--project".to_string(),
		])
		.expect("context before body");
		assert_eq!(parsed.scope, None);
		assert_eq!(parsed.context.as_deref(), Some("ctx"));
		assert_eq!(parsed.rest, ["body", "--project"]);
	}

	#[test]
	fn session_scope_is_isolated_from_project_and_default_views() {
		let dir = temp_dir();
		let agent_dir = dir.join("agent");
		let project = dir.join("project");
		fs::create_dir_all(&project).expect("project");
		run(
			&["create", "--session", "session-a", "session only marker"],
			&agent_dir,
			&project,
		)
		.expect("session create");
		run(
			&["create", "--session", "session-b", "other session marker"],
			&agent_dir,
			&project,
		)
		.expect("second session create");

		let default_view = run(&["list"], &agent_dir, &project).expect("default list");
		assert!(!default_view.contains("session only marker"), "{default_view}");
		let session_view = run(&["list", "--session", "session-a"], &agent_dir, &project)
			.expect("session list");
		assert!(session_view.contains("session only marker"), "{session_view}");
		assert!(!session_view.contains("other session marker"), "{session_view}");
		assert!(session_view.contains("[session:session-a]"), "{session_view}");

		let argv = vec!["search".to_string(), "marker".to_string()];
		let mut out = Vec::new();
		execute_with_session(
			&argv,
			&agent_dir,
			&project,
			Some("session-a"),
			MemoryBackend::Local,
			&mut out,
		)
		.expect("session-aware search");
		let searched = String::from_utf8(out).expect("utf8");
		assert!(searched.contains("session only marker"), "{searched}");
		assert!(!searched.contains("other session marker"), "{searched}");
	}

	#[test]
	fn global_recall_never_returns_project_rows() {
		let dir = temp_dir();
		let agent_dir = dir.join("agent");
		let project = dir.join("project");
		let other = dir.join("other");
		fs::create_dir_all(&project).expect("project");
		fs::create_dir_all(&other).expect("other");

		run(
			&["create", "project-only secret marker"],
			&agent_dir,
			&project,
		)
		.expect("project create");
		run(&["create", "--global", "global shared marker"], &agent_dir, &project)
			.expect("global create");
		// A flag-looking token after the body starts is literal text, not scope.
		run(
			&["create", "use", "--global", "before", "editing"],
			&agent_dir,
			&project,
		)
		.expect("literal body create");
		let project_only = run(&["list", "--project"], &agent_dir, &project)
			.expect("project list");
		assert!(project_only.contains("use --global before editing"), "{project_only}");
		assert!(!project_only.contains("global shared marker"), "{project_only}");

		let leaked = run(&["search", "--global", "secret"], &agent_dir, &other)
			.expect("global search");
		assert!(leaked.contains("No memory records"), "{leaked}");
		let found = run(&["search", "--global", "shared"], &agent_dir, &other)
			.expect("global search hit");
		assert!(found.contains("global shared marker"), "{found}");
		assert!(!found.contains("project-only secret marker"), "{found}");
	}

	struct RecordingProvider {
		seen: std::sync::Mutex<Vec<String>>,
	}

	#[async_trait::async_trait]
	impl EmbeddingProvider for RecordingProvider {
		fn model(&self) -> &str {
			"recording"
		}

		async fn embed(
			&self,
			texts: &[String],
		) -> Result<Vec<Vec<f32>>, crate::memory_embeddings::EmbeddingError> {
			if let Ok(mut seen) = self.seen.lock() {
				seen.extend(texts.iter().cloned());
			}
			Ok(texts.iter().map(|_| vec![1.0f32]).collect())
		}
	}

	#[test]
	fn search_redacts_the_remote_embedding_query() {
		let dir = temp_dir();
		let agent_dir = dir.join("agent");
		let project = dir.join("project");
		fs::create_dir_all(&project).expect("project");
		run(&["create", "searchable lesson"], &agent_dir, &project).expect("create");

		let provider = std::sync::Arc::new(RecordingProvider {
			seen: std::sync::Mutex::new(Vec::new()),
		});
		let provider_ref: &dyn EmbeddingProvider = &*provider;
		let argv = vec![
			"search".to_string(),
			"token sk-abcdefghijklmnop".to_string(),
		];
		let mut out = Vec::new();
		execute_with_session_and_embeddings(
			&argv,
			&agent_dir,
			&project,
			None,
			Some(provider_ref),
			MemoryBackend::Local,
			&mut out,
		)
		.expect("search");
		let seen = provider.seen.lock().expect("seen");
		assert_eq!(seen.len(), 1);
		assert!(seen[0].contains("[REDACTED]"), "{}", seen[0]);
		assert!(!seen[0].contains("sk-"), "{}", seen[0]);
	}
}
