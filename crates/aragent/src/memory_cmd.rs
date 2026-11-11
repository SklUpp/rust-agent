//! One user-facing memory command shared by `arag memory`, REPL `/memory`, and
//! headless callers.
//!
//! D53: the user owns global (owner preferences) and project (project
//! knowledge) records, with `asserted` provenance on explicit creation. SQLite
//! stays the only live authority: `learned.md` is rewritten from rows after a
//! project create/update/delete and is never re-imported after the one-time
//! migration. Deleting a record never touches conversation history.
//!
//! Every action prints the scope (`global` or `project:<cwd>`) and origin, so a
//! user can tell an asserted preference from an inferred agent lesson.

use std::{io::Write as _, path::Path};

use crate::{
	autolearn::MemoryBackend,
	memory_protocol::{render_record, render_record_list},
	memory_store::{self, Scope},
};

/// Actions accepted by the shared command.
const USAGE: &str = "Usage: arag memory <action> [--global|--project] [--context TEXT]
Actions:
  list                         list visible records (global + this project)
  search <query>               lexical search (bounded)
  get <id>                     show one record with scope/origin
  create <text>                create a user-asserted record (default: project)
  update <id> <text>           replace the body; identity stays immutable
  delete <id>                  delete one record; conversation history is untouched

Scope flags: --global for owner preferences, --project (default for create)
for this project's knowledge. Both is an error.
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
	let parsed = parse(argv)?;
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
			let records = match memory_store::list_scope(agent_dir, cwd, parsed.scope) {
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
			let records = match memory_store::recall_scope(agent_dir, cwd, &query, parsed.scope, None)
			{
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
			let record =
				memory_store::get_visible(agent_dir, cwd, id).map_err(|error| error.to_string())?;
			write!(out, "{}", render_record(&record)).map_err(|error| error.to_string())?;
		},
		"create" | "add" | "new" => {
			let body = rest_text(&parsed.rest);
			if body.is_empty() {
				return Err("create requires text: arag memory create [--global] <text>".to_string());
			}
			let scope = parsed.scope.unwrap_or(Scope::Project);
			let record = memory_store::create_asserted_and_export(
				agent_dir,
				cwd,
				scope,
				&body,
				parsed.context.as_deref(),
			)
			.map_err(|error| error.to_string())?;
			let scope_label = match record.cwd.as_deref() {
				Some(cwd) => format!("project:{cwd}"),
				None => "global".to_string(),
			};
			writeln!(
				out,
				"Created {} [{scope_label}] origin={} actor={}",
				record.id,
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
			let record = memory_store::update_record_and_export(agent_dir, cwd, id, &body)
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
			let record =
				memory_store::get_visible(agent_dir, cwd, id).map_err(|error| error.to_string())?;
			memory_store::delete_record_and_export(agent_dir, cwd, id)
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

struct Parsed {
	action:  Option<String>,
	rest:    Vec<String>,
	scope:   Option<Scope>,
	context: Option<String>,
}

fn parse(argv: &[String]) -> Result<Parsed, String> {
	let mut action = None;
	let mut rest = Vec::new();
	let mut scope = None;
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
					if scope == Some(Scope::Project) {
						return Err("--global conflicts with --project".to_string());
					}
					scope = Some(Scope::Global);
					index += 1;
					continue;
				},
				"--project" => {
					if scope == Some(Scope::Global) {
						return Err("--project conflicts with --global".to_string());
					}
					scope = Some(Scope::Project);
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
	Ok(Parsed { action, rest, scope, context })
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
}
