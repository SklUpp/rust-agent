use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use serde_json::json;

use super::*;

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
	fn new(label: &str) -> Self {
		let nonce = COUNTER.fetch_add(1, Ordering::Relaxed);
		let path = std::env::temp_dir().join(format!("omp-c5-{label}-{}-{nonce}", std::process::id()));
		fs::create_dir_all(&path).expect("create temp dir");
		Self(path)
	}

	fn path(&self) -> &Path {
		&self.0
	}

	fn write(&self, relative: &str, content: &str) -> PathBuf {
		let path = self.0.join(relative);
		if let Some(parent) = path.parent() {
			fs::create_dir_all(parent).expect("create parent");
		}
		fs::write(&path, content).expect("write fixture");
		path
	}
}

impl Drop for TempDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}

fn set_old_mtime(path: &Path, age: Duration) {
	let file = fs::OpenOptions::new().write(true).open(path).expect("open for mtime");
	file.set_modified(SystemTime::now() - age).expect("set modified");
}

fn run_ttsr_capture(opts: &TtsrOptions, cwd: &Path) -> (u8, String, String) {
	let mut out = Vec::new();
	let mut err = Vec::new();
	let code = run_ttsr(opts, cwd, &mut out, &mut err).expect("ttsr handler");
	(
		code,
		String::from_utf8_lossy(&out).into_owned(),
		String::from_utf8_lossy(&err).into_owned(),
	)
}

fn sample_rule(dir: &TempDir) -> PathBuf {
	dir.write(
		"no-any.md",
		"---\ncondition: '\\bany\\b'\nscope: 'tool:edit(*.ts)'\ndescription: Avoid any\n---\nUse unknown, not any.\n",
	)
}

#[test]
fn ttsr_isolated_test_reports_triggered_rule_json() {
	let dir = TempDir::new("ttsr-json");
	let rule = sample_rule(&dir);
	let opts = TtsrOptions {
		action:       Some("test".to_string()),
		snippet:      Some("let value: any = 1;".to_string()),
		file:         None,
		rule:         Some(rule),
		source:       Some("tool".to_string()),
		tool:         Some("edit".to_string()),
		path:         Some("src/a.ts".to_string()),
		verbose:      false,
		json:         true,
		no_gitignore: false,
		max_bytes:    None,
	};
	let (code, stdout, _) = run_ttsr_capture(&opts, dir.path());
	assert_eq!(code, 0);
	let report: Value = serde_json::from_str(stdout.trim()).expect("json report");
	assert_eq!(report["source"], "tool");
	assert_eq!(report["tool"], "edit");
	assert_eq!(report["filePath"], "src/a.ts");
	assert_eq!(report["evaluated"], 1);
	assert_eq!(report["triggered"][0]["name"], "no-any");
	assert_eq!(report["triggered"][0]["matched"]["regex"][0], "\\bany\\b");
	assert_eq!(report["triggered"][0]["sourceProvider"], "ttsr-cli");
	assert_eq!(report["notTriggered"], json!([]));
	assert!(report.get("inferenceNote").is_none());
}

#[test]
fn ttsr_isolated_test_renders_triggered_and_defined_sections() {
	let dir = TempDir::new("ttsr-text");
	let rule = sample_rule(&dir);
	let opts = TtsrOptions {
		action:       Some("test".to_string()),
		snippet:      Some("aTS".to_string()),
		file:         None,
		rule:         Some(rule),
		source:       Some("text".to_string()),
		tool:         None,
		path:         None,
		verbose:      true,
		json:         false,
		no_gitignore: false,
		max_bytes:    None,
	};
	let (code, stdout, _) = run_ttsr_capture(&opts, dir.path());
	assert_eq!(code, 1, "isolated rule with no trigger exits 1");
	assert!(stdout.contains("TTSR test — source=text"), "{stdout}");
	assert!(stdout.contains("No rules triggered. (evaluated 1)"), "{stdout}");
	assert!(stdout.contains("Not triggered (1)"), "{stdout}");
	assert!(stdout.contains("condition: /\\bany\\b/"), "{stdout}");
}

#[test]
fn ttsr_invalid_source_and_rule_gaps_are_exact() {
	let dir = TempDir::new("ttsr-errors");
	let opts = TtsrOptions {
		action:       Some("test".to_string()),
		snippet:      Some("x".to_string()),
		file:         None,
		rule:         None,
		source:       Some("bogus".to_string()),
		tool:         None,
		path:         None,
		verbose:      false,
		json:         false,
		no_gitignore: false,
		max_bytes:    None,
	};
	let error = run_ttsr(&opts, dir.path(), &mut Vec::new(), &mut Vec::new()).expect_err("invalid source");
	assert_eq!(
		error.to_string(),
		"Invalid --source: bogus. Expected one of: text, thinking, tool"
	);

	let rule = dir.write("plain.md", "No frontmatter here.\n");
	let opts = TtsrOptions {
		rule: Some(rule),
		source: Some("text".to_string()),
		..opts
	};
	let error = run_ttsr(&opts, dir.path(), &mut Vec::new(), &mut Vec::new()).expect_err("no condition");
	assert_eq!(
		error.to_string(),
		"Rule \"plain\" has no usable TTSR condition. Add a `condition` (regex) or `astCondition` (ast-grep pattern) to its frontmatter."
	);
}

#[test]
fn ttsr_scan_counts_rule_matches() {
	let dir = TempDir::new("ttsr-scan");
	let rule = sample_rule(&dir);
	dir.write("src/a.ts", "let value: any = 1;\n");
	dir.write("src/b.ts", "let value: unknown = 1;\n");
	dir.write("notes.txt", "any\n");
	let scan_dir = dir.path().join("src");
	let opts = TtsrOptions {
		action:       Some("scan".to_string()),
		snippet:      Some(scan_dir.display().to_string()),
		file:         None,
		rule:         Some(rule),
		source:       None,
		tool:         None,
		path:         None,
		verbose:      true,
		json:         false,
		no_gitignore: true,
		max_bytes:    None,
	};
	let (code, stdout, stderr) = run_ttsr_capture(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert!(stdout.contains("TTSR scan — directory="), "{stdout}");
	assert!(stdout.contains("Found violations/matches: (1 matches across 1 files)"), "{stdout}");
	assert!(stdout.contains("a.ts"), "{stdout}");
	assert!(!stdout.contains("b.ts"), "{stdout}");
}

#[test]
fn gallery_state_aliases_and_errors_are_exact() {
	assert_eq!(
		parse_gallery_states(&["streaming args".into(), "in progress".into(), "done".into(), "failed".into()])
			.expect("known states"),
		vec![
			GalleryState::Streaming,
			GalleryState::Progress,
			GalleryState::Success,
			GalleryState::Error
		]
	);
	let error = parse_gallery_states(&["nope".into()]).expect_err("invalid state");
	assert_eq!(
		error.to_string(),
		"Invalid --state 'nope'. Valid values: streaming, streaming args, progress, in progress, success, done, error, failed"
	);
}

#[test]
fn gallery_covers_every_runtime_tool() {
	let mut expected: Vec<String> = crate::runtime::BUILTIN_TOOLS
		.iter()
		.map(|(name, _)| (*name).to_string())
		.chain(crate::runtime::DELEGATION_TOOLS.iter().map(|name| (*name).to_string()))
		.collect();
	expected.extend(["checkpoint", "rewind", "goal", "think"].map(str::to_string));
	expected.sort();
	expected.dedup();
	assert_eq!(gallery_known_tools(), expected, "gallery table must cover the Rust tool roster");
}

#[test]
fn gallery_fixture_arguments_validate_against_tool_schemas() {
	use pi_agent::tools::Tool;

	let args_for = |name: &str| -> Value {
		let fixture = GALLERY_TOOLS
			.iter()
			.find(|tool| tool.name == name)
			.expect("gallery fixture");
		serde_json::from_str(fixture.args).expect("fixture args JSON")
	};
	let controller = std::sync::Arc::new(pi_agent::tools::checkpoint::CheckpointController::new());
	let cases = [
		("think", pi_agent::tools::think::ThinkTool::new().schema()),
		(
			"checkpoint",
			pi_agent::tools::checkpoint::CheckpointTool::new(controller.clone()).schema(),
		),
		(
			"rewind",
			pi_agent::tools::checkpoint::RewindTool::new(controller).schema(),
		),
	];
	for (name, schema) in cases {
		let result = pi_agent::tools::task_catalog::validate_output(&schema, &args_for(name));
		assert!(
			result.is_ok(),
			"{name}: {}",
			result.err().map(|error| error.message).unwrap_or_default()
		);
	}
}

#[test]
fn gallery_unknown_tool_and_plain_output() {
	let opts = GalleryOptions {
		tool:     Some("nope".to_string()),
		states:   vec!["done".to_string()],
		width:    Some(80),
		expanded: false,
		plain:    true,
	};
	let mut out = Vec::new();
	assert_eq!(run_gallery(&opts, &mut out).expect("gallery"), 0);
	let stdout = String::from_utf8(out).expect("utf8");
	assert!(stdout.starts_with("Unknown tool 'nope'. Known tools: "), "{stdout}");
	assert!(stdout.contains("read"), "{stdout}");

	let opts = GalleryOptions {
		tool:     Some("read".to_string()),
		states:   vec!["done".to_string(), "failed".to_string()],
		width:    Some(10),
		expanded: true,
		plain:    true,
	};
	let mut out = Vec::new();
	assert_eq!(run_gallery(&opts, &mut out).expect("gallery"), 0);
	let stdout = String::from_utf8(out).expect("utf8");
	assert!(!stdout.contains('\x1b'), "plain output must carry no ANSI codes");
	assert!(stdout.contains("── read "), "{stdout}");
	assert!(stdout.contains("· done"), "{stdout}");
	assert!(stdout.contains("✗ read: Error: operation failed"), "{stdout}");

	let opts = GalleryOptions { plain: false, ..opts };
	let mut out = Vec::new();
	run_gallery(&opts, &mut out).expect("gallery");
	assert!(String::from_utf8(out).expect("utf8").contains("\x1b["));
}

#[test]
fn grievances_missing_empty_and_rows() {
	let dir = TempDir::new("grievances");
	let opts = GrievancesOptions { limit: 20, tool: None, json: false };

	let mut out = Vec::new();
	list_grievances_in(&dir.path().join("missing.db"), &opts, &mut out).expect("missing db");
	assert_eq!(
		String::from_utf8(out).expect("utf8"),
		"No grievances database found. Auto-QA has not recorded any reports yet (or was disabled).\n"
	);

	let db_path = dir.path().join("autoqa.db");
	let connection = rusqlite::Connection::open(&db_path).expect("open db");
	connection
		.execute_batch(
			"CREATE TABLE grievances (id INTEGER PRIMARY KEY, model TEXT, version TEXT, tool TEXT, report TEXT);",
		)
		.expect("schema");

	let mut out = Vec::new();
	list_grievances_in(&db_path, &opts, &mut out).expect("empty db");
	assert_eq!(String::from_utf8(out).expect("utf8"), "No grievances recorded yet.\n");

	let json_opts = GrievancesOptions { limit: 20, tool: None, json: true };
	let mut out = Vec::new();
	list_grievances_in(&db_path, &json_opts, &mut out).expect("empty json");
	assert_eq!(String::from_utf8(out).expect("utf8"), "[]\n");

	connection
		.execute(
			"INSERT INTO grievances (id, model, version, tool, report) VALUES (1, 'model-x', '1.2.3', 'bash', 'boom')",
			[],
		)
		.expect("insert");
	let mut out = Vec::new();
	list_grievances_in(&db_path, &opts, &mut out).expect("rows");
	assert_eq!(
		String::from_utf8(out).expect("utf8"),
		"#1 bash (model-x v1.2.3)\n  boom\n\nShowing 1 most recent\n"
	);

	let mut out = Vec::new();
	list_grievances_in(&db_path, &json_opts, &mut out).expect("rows json");
	let rows: Value = serde_json::from_slice(&out).expect("json rows");
	assert_eq!(rows[0]["id"], 1);
	assert_eq!(rows[0]["tool"], "bash");
	assert_eq!(rows[0]["report"], "boom");
}

fn gc_json(opts: &GcOptions, cwd: &Path) -> (u8, Value, String) {
	let mut out = Vec::new();
	let mut err = Vec::new();
	let code = run_gc(opts, cwd, &mut out, &mut err).expect("gc handler");
	let value = serde_json::from_slice(&out).expect("gc json");
	(code, value, String::from_utf8_lossy(&err).into_owned())
}

#[test]
fn gc_blob_dry_run_counts_referenced_and_unreferenced() {
	let dir = TempDir::new("gc-blobs");
	let agent = dir.path().join("agent");
	let referenced = "a".repeat(64);
	let orphan = "b".repeat(64);
	let referenced_file = dir.write(&format!("agent/blobs/{referenced}"), "ref!!");
	let orphan_file = dir.write(&format!("agent/blobs/{orphan}"), "orphan!");
	dir.write(
		"agent/sessions/project/s1.jsonl",
		&format!("{{\"type\":\"message\",\"message\":{{\"content\":\"blob:sha256:{referenced}\"}}}}\n"),
	);
	set_old_mtime(&referenced_file, Duration::from_secs(3600));
	set_old_mtime(&orphan_file, Duration::from_secs(3600));

	let opts = GcOptions {
		apply:                  false,
		json:                    true,
		agent_dir:               Some(agent),
		blobs:                   true,
		archive:                 false,
		wal:                     false,
		cold_archive_after_days: None,
		retain_newest_global:    None,
		retain_newest_per_cwd:   None,
	};
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert_eq!(value["apply"], false);
	assert_eq!(value["blobs"]["referenced"], 1);
	assert_eq!(value["blobs"]["candidates"], 2);
	assert_eq!(value["blobs"]["wouldDelete"], 1);
	assert_eq!(value["blobs"]["deleted"], 0);
	assert_eq!(value["blobs"]["bytes"], "orphan!".len() as u64);
	assert!(orphan_file.exists(), "dry-run must not delete blobs");
	assert!(value["lockPath"].as_str().is_some_and(|path| path.ends_with("gc.lock")));
}

#[test]
fn gc_archive_dry_run_counts_cold_complete_sessions() {
	let dir = TempDir::new("gc-archive");
	let agent = dir.path().join("agent");
	let session = dir.write(
		"agent/sessions/project/s1.jsonl",
		"{\"type\":\"session\",\"id\":\"s1\",\"cwd\":\"/tmp/project\"}\n{\"type\":\"message\",\"message\":{\"role\":\"assistant\",\"stopReason\":\"stop\",\"content\":[]}}\n",
	);
	set_old_mtime(&session, Duration::from_secs(60 * 86_400));

	let opts = GcOptions {
		apply:                  false,
		json:                    true,
		agent_dir:               Some(agent),
		blobs:                   false,
		archive:                 true,
		wal:                     false,
		cold_archive_after_days: Some(30),
		retain_newest_global:    Some(0),
		retain_newest_per_cwd:   Some(0),
	};
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert_eq!(value["archive"]["scanned"], 1);
	assert_eq!(value["archive"]["skippedActive"], 0);
	assert_eq!(value["archive"]["wouldArchive"], 1);
	assert_eq!(value["archive"]["archived"], 0);
	assert!(session.exists(), "dry-run must keep session logs");
}

#[test]
fn gc_wal_dry_run_reports_wal_bytes() {
	let dir = TempDir::new("gc-wal");
	let agent = dir.path().join("agent");
	let db = dir.write("agent/history.db", "");
	dir.write("agent/history.db-wal", "wal-bytes");
	let opts = GcOptions {
		apply:                  false,
		json:                    true,
		agent_dir:               Some(agent),
		blobs:                   false,
		archive:                 false,
		wal:                     true,
		cold_archive_after_days: None,
		retain_newest_global:    None,
		retain_newest_per_cwd:   None,
	};
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert_eq!(value["wal"]["walBytes"], "wal-bytes".len() as u64);
	assert_eq!(value["wal"]["wouldCheckpoint"], true);
	assert_eq!(value["wal"]["checkpointed"], false);
	assert_eq!(value["wal"]["databases"].as_array().map(Vec::len), Some(2));
	assert!(db.exists());
}

fn gc_apply_options(agent: PathBuf, blobs: bool, archive: bool, wal: bool) -> GcOptions {
	GcOptions {
		apply:                   true,
		json:                    true,
		agent_dir:               Some(agent),
		blobs,
		archive,
		wal,
		cold_archive_after_days: Some(30),
		retain_newest_global:    Some(0),
		retain_newest_per_cwd:   Some(0),
	}
}

fn read_gzip_text(path: &Path) -> String {
	use std::io::Read as _;
	let file = fs::File::open(path).expect("open gzip");
	let mut decoder = flate2::read::GzDecoder::new(file);
	let mut text = String::new();
	decoder.read_to_string(&mut text).expect("gunzip fixture");
	text
}

#[test]
fn gc_blob_apply_deletes_orphans_and_keeps_referenced_fresh_files() {
	let dir = TempDir::new("gc-blob-apply");
	let agent = dir.path().join("agent");
	let referenced = "a".repeat(64);
	let orphan = "b".repeat(64);
	let fresh = "c".repeat(64);
	let referenced_file = dir.write(&format!("agent/blobs/{referenced}"), "ref");
	let orphan_file = dir.write(&format!("agent/blobs/{orphan}"), "orphan");
	let fresh_file = dir.write(&format!("agent/blobs/{fresh}"), "fresh");
	dir.write(
		"agent/sessions/project/s1.jsonl",
		&format!("{{\"type\":\"message\",\"message\":{{\"content\":\"blob:sha256:{referenced}\"}}}}\n"),
	);
	set_old_mtime(&referenced_file, Duration::from_secs(3600));
	set_old_mtime(&orphan_file, Duration::from_secs(3600));

	let opts = gc_apply_options(agent.clone(), true, false, false);
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert_eq!(value["apply"], true);
	assert_eq!(value["blobs"]["deleted"], 1);
	assert_eq!(value["blobs"]["wouldDelete"], 1);
	assert!(!orphan_file.exists(), "unreferenced old blob must be swept");
	assert!(referenced_file.exists(), "referenced blob must survive");
	assert!(fresh_file.exists(), "fresh blob must survive the write grace");
	assert!(!agent.join("gc.lock").exists(), "lock must be released");
}

#[test]
fn gc_archive_apply_moves_gzip_and_artifacts() {
	let dir = TempDir::new("gc-archive-apply");
	let agent = dir.path().join("agent");
	let content = "{\"type\":\"session\",\"id\":\"s1\",\"cwd\":\"/tmp/project\"}\n{\"type\":\"message\",\"message\":{\"role\":\"assistant\",\"stopReason\":\"stop\",\"content\":[]}}\n";
	let session = dir.write("agent/sessions/project/s1.jsonl", content);
	dir.write("agent/sessions/project/s1/artifacts/a.txt", "artifact");
	set_old_mtime(&session, Duration::from_secs(60 * 86_400));
	let pending = dir.write(
		"agent/sessions/project/pending.jsonl",
		"{\"type\":\"session\",\"id\":\"pending\"}\n{\"type\":\"message\",\"message\":{\"role\":\"user\",\"content\":\"waiting\"}}\n",
	);
	set_old_mtime(&pending, Duration::from_secs(60 * 86_400));
	let fresh = dir.write(
		"agent/sessions/project/fresh.jsonl",
		"{\"type\":\"session\",\"id\":\"fresh\"}\n{\"type\":\"message\",\"message\":{\"role\":\"assistant\",\"stopReason\":\"stop\",\"content\":[]}}\n",
	);

	let opts = gc_apply_options(agent.clone(), false, true, false);
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert_eq!(value["archive"]["archived"], 1);
	assert_eq!(value["archive"]["wouldArchive"], 1);
	assert_eq!(value["archive"]["skippedActive"], 2);
	assert!(pending.exists(), "pending session must not be archived");
	assert!(fresh.exists(), "fresh session must not be archived");
	assert!(!session.exists(), "archived source must be removed");
	let archived = agent.join("archive/sessions/project/s1.jsonl.gz");
	assert!(archived.exists(), "session must be gzipped into the archive");
	assert_eq!(read_gzip_text(&archived), content);
	assert_eq!(
		fs::read_to_string(agent.join("archive/sessions/project/s1/artifacts/a.txt")).expect("artifact"),
		"artifact"
	);
}

#[test]
fn gc_archive_apply_preserves_foreign_destination_and_source() {
	let dir = TempDir::new("gc-archive-collision");
	let agent = dir.path().join("agent");
	let session = dir.write(
		"agent/sessions/project/s1.jsonl",
		"{\"type\":\"session\",\"id\":\"s1\"}\n{\"type\":\"message\",\"message\":{\"role\":\"assistant\",\"stopReason\":\"stop\",\"content\":[]}}\n",
	);
	set_old_mtime(&session, Duration::from_secs(60 * 86_400));
	let foreign = dir.write("agent/archive/sessions/project/s1.jsonl.gz", "foreign");

	let opts = gc_apply_options(agent.clone(), false, true, false);
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 1);
	assert!(stderr.contains("GC completed with 1 error"), "{stderr}");
	assert_eq!(value["archive"]["archived"], 0);
	assert!(session.exists(), "collision must preserve the source");
	assert_eq!(fs::read_to_string(&foreign).expect("foreign file"), "foreign");
}

#[test]
fn gc_archive_apply_retries_history_and_stats_for_archived_sessions() {
	let dir = TempDir::new("gc-archive-retry");
	let agent = dir.path().join("agent");
	let archived = agent.join("archive/sessions/project/old.jsonl.gz");
	fs::create_dir_all(archived.parent().expect("archive parent")).expect("archive dir");
	let header = "{\"type\":\"session\",\"id\":\"old\",\"cwd\":\"/tmp\"}\n";
	write_gzip(
		&archived,
		&format!(
			"{header}{{\"type\":\"message\",\"id\":\"entry\",\"timestamp\":\"2026-06-26T12:00:00.000Z\",\"message\":{{\"role\":\"assistant\",\"content\":[]}}}}\n"
		),
	);
	let history_db = dir.write(
		"agent/history.db",
		"",
	);
	let history = rusqlite::Connection::open(&history_db).expect("history db");
	history
		.execute_batch(
			"CREATE TABLE history (id INTEGER PRIMARY KEY, prompt TEXT NOT NULL, session_id TEXT);\
			 INSERT INTO history (prompt, session_id) VALUES ('keep', 'old');",
		)
		.expect("history fixture");
	drop(history);
	let stats_db = dir.write("agent/stats.db", "");
	let stats = rusqlite::Connection::open(&stats_db).expect("stats db");
	stats
		.execute_batch(
			"CREATE TABLE messages (session_file TEXT NOT NULL, entry_id TEXT NOT NULL, timestamp INTEGER NOT NULL);",
		)
		.expect("stats schema");
	let source_path = agent.join("sessions/project/old.jsonl");
	stats
		.execute(
			"INSERT INTO messages (session_file, entry_id, timestamp) VALUES (?, ?, ?)",
			rusqlite::params![source_path.to_string_lossy().into_owned(), "entry", 1i64],
		)
		.expect("stats row");
	drop(stats);

	let opts = gc_apply_options(agent.clone(), false, true, false);
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert_eq!(value["archive"]["archived"], 0);
	assert_eq!(value["archive"]["historyRowsDeleted"], 1);
	assert_eq!(value["archive"]["statsRowsDeleted"], 1);
	assert!(value["archive"]["errors"].as_array().is_some_and(Vec::is_empty));
	let history = rusqlite::Connection::open(&history_db).expect("history db");
	let history_rows: i64 = history
		.query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
		.expect("history count");
	assert_eq!(history_rows, 0);
	let stats = rusqlite::Connection::open(&stats_db).expect("stats db");
	let stats_rows: i64 = stats
		.query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
		.expect("stats count");
	assert_eq!(stats_rows, 0);
}

#[test]
fn gc_archive_apply_retargets_shared_stats_identities_to_retained_peer() {
	let dir = TempDir::new("gc-archive-transfer");
	let agent = dir.path().join("agent");
	let parent_path = agent.join("sessions/project/parent.jsonl");
	let archived = agent.join("archive/sessions/project/parent.jsonl.gz");
	fs::create_dir_all(archived.parent().expect("archive parent")).expect("archive dir");
	let header = |id: &str, parent: Option<&Path>| {
		let parent_field = parent
			.map(|path| format!(",\"parentSession\":\"{}\"", path.display()))
			.unwrap_or_default();
		format!("{{\"type\":\"session\",\"id\":\"{id}\",\"cwd\":\"/tmp\"{parent_field}}}\n")
	};
	let entry = "{\"type\":\"message\",\"id\":\"shared\",\"timestamp\":\"2026-06-26T12:00:00.000Z\",\"message\":{\"role\":\"assistant\",\"content\":[]}}\n";
	write_gzip(&archived, &format!("{}{entry}", header("parent", None)));
	let child = dir.write(
		"agent/sessions/project/child.jsonl",
		&format!("{}{entry}", header("child", Some(&parent_path))),
	);
	let stats_db = dir.write("agent/stats.db", "");
	let stats = rusqlite::Connection::open(&stats_db).expect("stats db");
	stats
		.execute_batch(
			"CREATE TABLE messages (session_file TEXT NOT NULL, entry_id TEXT NOT NULL, timestamp INTEGER NOT NULL, UNIQUE(session_file, entry_id));",
		)
		.expect("stats schema");
	let timestamp = crate::foreign_session::parse_iso_timestamp_ms("2026-06-26T12:00:00.000Z")
		.expect("timestamp") as i64;
	stats
		.execute(
			"INSERT INTO messages (session_file, entry_id, timestamp) VALUES (?, ?, ?)",
			rusqlite::params![parent_path.to_string_lossy().into_owned(), "shared", timestamp],
		)
		.expect("stats row");
	drop(stats);

	let opts = gc_apply_options(agent.clone(), false, true, false);
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert_eq!(value["archive"]["archived"], 0);
	assert_eq!(value["archive"]["statsRowsDeleted"], 0);
	assert!(value["archive"]["errors"].as_array().is_some_and(Vec::is_empty));
	let stats = rusqlite::Connection::open(&stats_db).expect("stats db");
	let mut statement = stats
		.prepare("SELECT session_file, entry_id FROM messages")
		.expect("query");
	let rows: Vec<(String, String)> = statement
		.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
		.expect("rows")
		.collect::<std::result::Result<_, _>>()
		.expect("collect");
	assert_eq!(rows, vec![(child.to_string_lossy().into_owned(), "shared".to_string())]);
}

#[test]
fn gc_live_and_breaker_locks_block_and_stale_lock_recovers() {
	let dir = TempDir::new("gc-locks");
	let agent = dir.path().join("agent");
	let lock = agent.join("gc.lock");
	dir.write("agent/blobs/.keep", "");
	let live = format!("{}\n2026-01-01T00:00:00.000Z\n", std::process::id());
	dir.write("agent/gc.lock", &live);
	let opts = gc_apply_options(agent.clone(), true, false, false);
	let mut out = Vec::new();
	let mut err = Vec::new();
	let error = run_gc(&opts, dir.path(), &mut out, &mut err).expect_err("live lock");
	assert!(error.to_string().contains("GC already running"), "{error}");
	assert!(lock.exists(), "live lock must not be broken");

	dir.write("agent/gc.lock.break", &live);
	dir.write("agent/gc.lock", "2147483647\n2026-01-01T00:00:00.000Z\n");
	let error = run_gc(&opts, dir.path(), &mut out, &mut err).expect_err("breaker lock");
	assert!(error.to_string().contains("GC already running"), "{error}");
	assert!(lock.exists());
	assert!(agent.join("gc.lock.break").exists());

	fs::remove_file(agent.join("gc.lock.break")).expect("remove breaker");
	let opts = gc_apply_options(agent.clone(), true, false, false);
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert_eq!(value["blobs"]["wouldDelete"], 0);
	assert!(!lock.exists(), "stale lock must be replaced and released");
}

#[test]
fn gc_live_breaker_without_main_lock_blocks_acquisition() {
	let dir = TempDir::new("gc-breaker-only");
	let agent = dir.path().join("agent");
	fs::create_dir_all(&agent).expect("agent dir");
	let breaker = agent.join("gc.lock.break");
	fs::write(
		&breaker,
		format!("{}\n2026-01-01T00:00:00.000Z\n", std::process::id()),
	)
	.expect("live breaker");
	let error = match acquire_gc_lock(&agent) {
		Err(error) => error,
		Ok(_) => panic!("live breaker without main lock must block acquisition"),
	};
	assert!(error.contains("GC already running"), "{error}");
	assert!(
		!agent.join("gc.lock").exists(),
		"must not create the main lock under a live breaker"
	);
	assert!(breaker.exists(), "live breaker must be preserved");
	fs::remove_file(&breaker).expect("remove breaker");
	let held = acquire_gc_lock(&agent).expect("breaker cleared");
	drop(held);
	assert!(!agent.join("gc.lock").exists(), "lock must be released");
}

#[test]
fn gc_wal_apply_failure_is_nonzero_and_releases_lock() {
	let dir = TempDir::new("gc-wal-apply");
	let agent = dir.path().join("agent");
	dir.write("agent/history.db", "not sqlite");
	dir.write("agent/history.db-wal", "wal");
	let opts = gc_apply_options(agent.clone(), false, false, true);
	let mut out = Vec::new();
	let mut err = Vec::new();
	let error = run_gc(&opts, dir.path(), &mut out, &mut err).expect_err("invalid wal database");
	assert!(error.to_string().contains("history.db"), "{error}");
	assert!(out.is_empty(), "fatal WAL failure must not print a result");
	assert!(!agent.join("gc.lock").exists(), "lock must be released");
}

#[test]
fn gc_wal_apply_busy_checkpoint_is_nonzero_and_releases_lock() {
	let dir = TempDir::new("gc-wal-busy");
	let agent = dir.path().join("agent");
	let db_path = agent.join("history.db");
	fs::create_dir_all(&agent).expect("agent dir");
	let holder = rusqlite::Connection::open(&db_path).expect("holder db");
	holder
		.execute_batch(
			"PRAGMA journal_mode=WAL;\
			 PRAGMA wal_autocheckpoint=0;\
			 CREATE TABLE t (x INTEGER);\
			 INSERT INTO t VALUES (1);\
			 BEGIN IMMEDIATE;\
			 INSERT INTO t VALUES (2);",
		)
		.expect("writer transaction");

	let opts = gc_apply_options(agent.clone(), false, false, true);
	let mut out = Vec::new();
	let mut err = Vec::new();
	let error = run_gc(&opts, dir.path(), &mut out, &mut err).expect_err("busy checkpoint");
	assert!(error.to_string().contains("history.db"), "{error}");
	assert!(out.is_empty());
	assert!(!agent.join("gc.lock").exists(), "lock must be released");
	drop(holder);
}

#[test]
fn gc_archive_rollback_restores_gzip_source_after_move_failure() {
	let dir = TempDir::new("gc-rollback");
	let source = dir.path().join("sessions/project/s1.jsonl");
	let destination = dir.path().join("archive/sessions/project/s1.jsonl.gz");
	fs::create_dir_all(destination.parent().expect("archive parent")).expect("archive dir");
	let content = "{\"type\":\"session\",\"id\":\"s1\"}\n";
	write_gzip(&destination, content);
	rollback_moves(&[(source.clone(), destination.clone(), true)]);
	assert_eq!(fs::read_to_string(&source).expect("restored source"), content);
	assert!(!destination.exists(), "rollback must remove the published archive");

	// An artifact rollback must never overwrite a concurrent source entry.
	let artifacts_source = dir.path().join("sessions/project/s1");
	let artifacts_destination = dir.path().join("archive/sessions/project/s1");
	fs::create_dir_all(&artifacts_source).expect("source artifacts");
	fs::create_dir_all(&artifacts_destination).expect("archive artifacts");
	fs::write(artifacts_source.join("live.txt"), "live").expect("concurrent artifact");
	fs::write(artifacts_destination.join("old.txt"), "old").expect("archived artifact");
	rollback_moves(&[(artifacts_source.clone(), artifacts_destination.clone(), false)]);
	assert_eq!(
		fs::read_to_string(artifacts_source.join("live.txt")).expect("live artifact"),
		"live"
	);
	assert!(
		artifacts_destination.join("old.txt").exists(),
		"refused rollback must leave the archive intact"
	);
}

#[test]
fn gc_archive_apply_rejects_escaping_descendant_before_creating_dirs() {
	use std::os::unix::fs::symlink;

	let dir = TempDir::new("gc-escape");
	let agent = dir.path().join("agent");
	let session = dir.write(
		"agent/sessions/project/s1.jsonl",
		"{\"type\":\"session\",\"id\":\"s1\"}\n{\"type\":\"message\",\"message\":{\"role\":\"assistant\",\"stopReason\":\"stop\",\"content\":[]}}\n",
	);
	set_old_mtime(&session, Duration::from_secs(60 * 86_400));
	let foreign = dir.path().join("foreign");
	fs::create_dir_all(&foreign).expect("foreign dir");
	fs::create_dir_all(agent.join("archive/sessions")).expect("archive dir");
	symlink(&foreign, agent.join("archive/sessions/project")).expect("escaping symlink");
	let opts = gc_apply_options(agent.clone(), false, true, false);
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 1, "{stderr}");
	assert!(stderr.contains("escapes root"), "{stderr}");
	assert_eq!(value["archive"]["archived"], 0);
	assert!(session.exists(), "source must survive containment refusal");
	assert!(
		!foreign.join("sub").exists(),
		"must not create a directory outside the archive root"
	);
	// A missing descendant below the escaping symlink must be rejected before
	// mkdir, not only after the archive parent already exists.
	let escape_destination = agent.join("archive/sessions/project/sub/s1.jsonl.gz");
	let error = ensure_destination_parent(&agent.join("archive/sessions"), &escape_destination)
		.expect_err("descendant symlink escape");
	assert!(error.contains("escapes root"), "{error}");
	assert!(
		!foreign.join("sub").exists(),
		"must not create a directory outside the archive root"
	);
}

#[test]
fn gc_native_no_replace_rename_publishes_completed_file_and_refuses_collision() {
	let dir = TempDir::new("gc-rename");
	let source = dir.write("source.bin", "payload");
	let destination = dir.path().join("published.bin");
	assert!(rename_noreplace_native(&source, &destination).expect("native rename"));
	assert!(!source.exists(), "rename must move, not copy");
	assert_eq!(fs::read_to_string(&destination).expect("published"), "payload");

	let again = dir.write("source.bin", "other");
	assert!(!rename_noreplace_native(&again, &destination).expect("collision"));
	assert_eq!(fs::read_to_string(&destination).expect("published"), "payload");
	assert_eq!(fs::read_to_string(&again).expect("source"), "other");
}

#[test]
fn gc_gzip_publication_is_complete_before_rename_and_cleans_staging() {
	let dir = TempDir::new("gc-gzip-publish");
	let archive_root = dir.path().join("archive");
	let content = "{\"type\":\"session\",\"id\":\"s1\"}\n";
	let source = dir.write("sessions/project/s1.jsonl", content);
	let destination = archive_root.join("project/s1.jsonl.gz");
	gzip_session_file(&source, &destination, &archive_root).expect("gzip publish");
	assert!(!source.exists(), "published session source must be removed");
	assert_eq!(read_gzip_text(&destination), content);
	let leftovers: Vec<PathBuf> = fs::read_dir(destination.parent().expect("archive parent"))
		.expect("archive dir")
		.filter_map(|entry| entry.ok())
		.map(|entry| entry.path())
		.filter(|path| path.to_string_lossy().contains(".tmp"))
		.collect();
	assert!(leftovers.is_empty(), "staging files must not survive publication");
}

#[test]
fn gc_gzip_publish_failure_removes_staged_temp_and_preserves_source() {
	let dir = TempDir::new("gc-publish-fail");
	let archive_root = dir.path().join("archive");
	let content = "{\"type\":\"session\",\"id\":\"s1\"}\n";
	let source = dir.write("sessions/project/s1.jsonl", content);
	let destination = archive_root.join("project/s1.jsonl.gz");
	super::FAIL_GC_PUBLISH.with(|flag| flag.set(true));
	let error = gzip_session_file(&source, &destination, &archive_root)
		.expect_err("injected publish failure");
	assert!(error.contains("injected publish failure"), "{error}");
	assert!(source.exists(), "source must survive a publication failure");
	assert!(!destination.exists(), "failed publication must not create the archive");
	let leftovers: Vec<PathBuf> = fs::read_dir(destination.parent().expect("archive parent"))
		.expect("archive dir")
		.filter_map(|entry| entry.ok())
		.map(|entry| entry.path())
		.filter(|path| path.to_string_lossy().contains(".tmp"))
		.collect();
	assert!(
		leftovers.is_empty(),
		"failed publication must not leak the staged session"
	);
}

#[test]
fn gc_torn_empty_stale_lock_uses_mtime_milliseconds() {
	let dir = TempDir::new("gc-torn-lock");
	let agent = dir.path().join("agent");
	dir.write("agent/blobs/.keep", "");
	let lock = dir.write("agent/gc.lock", "");
	set_old_mtime(&lock, Duration::from_secs(3600));
	let opts = gc_apply_options(agent.clone(), true, false, false);
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert_eq!(value["blobs"]["wouldDelete"], 0);
	assert!(!lock.exists(), "torn stale lock must be recovered");
}

#[test]
fn gc_archive_refuses_cross_filesystem_artifact_move_before_mutation() {
	use std::os::unix::fs::MetadataExt as _;

	let dir = TempDir::new("gc-exdev-refuse");
	let agent = dir.path().join("agent");
	let sessions_root = agent.join("sessions");
	let session = dir.write(
		"agent/sessions/project/s1.jsonl",
		"{\"type\":\"session\",\"id\":\"s1\"}\n",
	);
	dir.write("agent/sessions/project/s1/artifacts/a.txt", "artifact");
	set_old_mtime(&session, Duration::from_secs(60 * 86_400));
	let foreign_root = Path::new("/dev");
	assert_ne!(
		fs::metadata(&sessions_root).expect("sessions device").dev(),
		fs::metadata(foreign_root).expect("/dev device").dev(),
		"test environment needs /dev on a different filesystem"
	);
	let candidate = ArchiveCandidate {
		session: SessionMeta {
			path: session.clone(),
			modified: 0,
			status: "idle",
			cwd: None,
			id: "s1".to_string(),
			parent_session: None,
		},
		destination: foreign_root.join("omp-gc-exdev-project/s1.jsonl.gz"),
	};
	let error = move_session_with_artifacts(&candidate, &sessions_root, foreign_root)
		.expect_err("cross-device refusal");
	assert!(error.contains("across filesystems"), "{error}");
	assert!(session.exists(), "session source must survive the refusal");
	assert_eq!(
		fs::read_to_string(agent.join("sessions/project/s1/artifacts/a.txt")).expect("artifact"),
		"artifact"
	);
	assert!(
		!foreign_root.join("omp-gc-exdev-project").exists(),
		"refusal must not create archive directories"
	);
}

#[test]
fn gc_guard_lock_contends_and_persists_without_blocking_after_release() {
	let dir = TempDir::new("gc-guard");
	let agent = dir.path().join("agent");
	fs::create_dir_all(&agent).expect("agent dir");
	let lock_path = agent.join("gc.lock");
	let guard = open_gc_guard(&lock_path).expect("first guard");
	let error = match acquire_gc_lock(&agent) {
		Err(error) => error,
		Ok(_) => panic!("contended guard must block acquisition"),
	};
	assert!(error.contains("GC already running"), "{error}");
	assert!(!lock_path.exists(), "contention must not create gc.lock");
	drop(guard);

	let held = acquire_gc_lock(&agent).expect("guard released");
	assert!(lock_path.exists(), "acquired lock must be visible to TS");
	drop(held);
	assert!(!lock_path.exists(), "lock must be released");
	assert!(
		agent.join("gc.lock.guard").exists(),
		"guard inode must persist across runs"
	);
}

#[test]
fn gc_guard_serializes_concurrent_acquire_and_stale_lock_recovery() {
	use std::sync::{Arc, Barrier};
	use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

	let dir = TempDir::new("gc-breaker-race");
	let agent = dir.path().join("agent");
	fs::create_dir_all(&agent).expect("agent dir");
	fs::write(agent.join("gc.lock"), "2147483647\n2026-01-01T00:00:00.000Z\n").expect("stale lock");
	fs::write(
		agent.join("gc.lock.break"),
		"2147483647\n2026-01-01T00:00:00.000Z\n",
	)
	.expect("stale breaker");
	let barrier = Arc::new(Barrier::new(8));
	let holders = Arc::new(AtomicUsize::new(0));
	let peak = Arc::new(AtomicUsize::new(0));
	let acquisitions = Arc::new(AtomicUsize::new(0));
	let mut handles = Vec::new();
	for _ in 0..8 {
		let agent = agent.clone();
		let barrier = Arc::clone(&barrier);
		let holders = Arc::clone(&holders);
		let peak = Arc::clone(&peak);
		let acquisitions = Arc::clone(&acquisitions);
		handles.push(std::thread::spawn(move || {
			barrier.wait();
			for _ in 0..4 {
				if let Ok(lock) = acquire_gc_lock(&agent) {
					acquisitions.fetch_add(1, AtomicOrdering::SeqCst);
					let active = holders.fetch_add(1, AtomicOrdering::SeqCst) + 1;
					peak.fetch_max(active, AtomicOrdering::SeqCst);
					std::thread::sleep(Duration::from_millis(10));
					holders.fetch_sub(1, AtomicOrdering::SeqCst);
					drop(lock);
				}
			}
		}));
	}
	for handle in handles {
		handle.join().expect("lock contender");
	}
	assert!(
		acquisitions.load(AtomicOrdering::SeqCst) > 0,
		"at least one contender must recover the stale lock"
	);
	assert_eq!(
		peak.load(AtomicOrdering::SeqCst),
		1,
		"stale-breaker recovery must stay exclusive"
	);
	assert!(!agent.join("gc.lock").exists(), "lock must be released");
}

#[test]
fn gc_wal_attempts_models_when_history_checkpoint_is_busy() {
	let dir = TempDir::new("gc-wal-both");
	let agent = dir.path().join("agent");
	fs::create_dir_all(&agent).expect("agent dir");
	let history = rusqlite::Connection::open(agent.join("history.db")).expect("history db");
	history
		.execute_batch(
			"PRAGMA journal_mode=WAL;\
			 PRAGMA wal_autocheckpoint=0;\
			 CREATE TABLE t (x INTEGER);\
			 INSERT INTO t VALUES (1);\
			 BEGIN IMMEDIATE;\
			 INSERT INTO t VALUES (2);",
		)
		.expect("history writer");
	let models = rusqlite::Connection::open(agent.join("models.db")).expect("models db");
	models
		.execute_batch(
			"PRAGMA journal_mode=WAL;\
			 PRAGMA wal_autocheckpoint=0;\
			 CREATE TABLE t (x INTEGER);\
			 INSERT INTO t VALUES (1);",
		)
		.expect("models wal");
	let models_wal = PathBuf::from(format!("{}-wal", agent.join("models.db").display()));
	assert!(
		fs::metadata(&models_wal).expect("models wal file").len() > 0,
		"fixture needs uncheckpointed models frames"
	);

	let opts = gc_apply_options(agent.clone(), false, false, true);
	let mut out = Vec::new();
	let mut err = Vec::new();
	let error = run_gc(&opts, dir.path(), &mut out, &mut err).expect_err("busy history checkpoint");
	assert!(error.to_string().contains("history.db"), "{error}");
	assert_eq!(
		fs::metadata(&models_wal).map(|meta| meta.len()).unwrap_or(0),
		0,
		"models WAL must be attempted despite the history failure"
	);
	assert!(!agent.join("gc.lock").exists(), "lock must be released");
	drop(history);
	drop(models);
}

#[test]
fn setup_component_errors_are_exact() {
	let dir = TempDir::new("setup-errors");
	let mut out = Vec::new();
	let mut err = Vec::new();
	let opts = SetupOptions { component: None, check: true, json: false };
	let error = run_setup(&opts, dir.path(), &mut out, &mut err).expect_err("missing component");
	assert_eq!(
		error.to_string(),
		"setup --check/--json requires a COMPONENT (python|speech)"
	);

	let opts = SetupOptions { component: None, check: false, json: false };
	let error = run_setup(&opts, dir.path(), &mut out, &mut err).expect_err("headless wizard");
	assert_eq!(error.to_string(), "omp setup requires an interactive TTY.");

	let opts = SetupOptions { component: Some("bogus".to_string()), check: false, json: false };
	let error = run_setup(&opts, dir.path(), &mut out, &mut err).expect_err("unknown component");
	assert_eq!(error.to_string(), "Unknown component: bogus\nValid components: python, speech");
}

#[test]
fn setup_python_probe_rejects_missing_interpreter() {
	let dir = TempDir::new("setup-python");
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.expect("runtime");
	let (available, path) = runtime.block_on(probe_python(
		dir.path(),
		Some("/nonexistent/python-c5"),
		&dir.path().join("python-env"),
	));
	assert!(!available);
	assert!(path.is_none());
}

#[test]
fn setup_speech_probes_model_and_runtime_cache() {
	let dir = TempDir::new("setup-speech");
	let tiny = dir.path().join("tiny-models");
	let stt_dir = "tiny-models/csukuangfj/sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8";
	// Empty files and directories must not count as cached assets.
	dir.write(&format!("{stt_dir}/encoder.int8.onnx"), "");
	for file in ["decoder.int8.onnx", "joiner.int8.onnx", "tokens.txt"] {
		dir.write(&format!("{stt_dir}/{file}"), "x");
	}
	fs::create_dir_all(dir.path().join(stt_dir).join("dummy.int8.onnx")).expect("dummy dir");
	assert!(!stt_model_cached(&tiny, "parakeet"));
	dir.write(&format!("{stt_dir}/encoder.int8.onnx"), "x");
	assert!(stt_model_cached(&tiny, "parakeet"));
	assert!(!stt_model_cached(&tiny, "fast"));
	assert!(!tts_model_cached(&tiny, "kokoro"));

	let kokoro = "tiny-models/onnx-community/Kokoro-82M-v1.0-ONNX/kokoro.onnx";
	dir.write(kokoro, "");
	assert!(!tts_model_cached(&tiny, "kokoro"));
	dir.write(kokoro, "x");
	dir.write(
		"tts-runtime/kokoro-1.2.1/node_modules/kokoro-js/package.json",
		"{\"version\":\"1.2.1\"}",
	);
	let cache_root = tiny.parent().expect("cache root");
	assert!(tts_model_cached(&tiny, "kokoro"));
	assert!(tts_runtime_cached(&tiny));
	assert!(cache_root.exists());
}

#[test]
fn preview_and_scope_parsing_match_cli_contract() {
	assert_eq!(preview_snippet("  a\n\n b  c "), "a b c");
	let long = "x".repeat(100);
	assert_eq!(preview_snippet(&long).chars().count(), 78);
	assert!(preview_snippet(&long).ends_with('…'));

	let scope = parse_tool_scope_token("tool:edit(*.ts)").expect("tool scope");
	assert_eq!(scope.tool_name.as_deref(), Some("edit"));
	assert_eq!(scope.path_glob.as_deref(), Some("*.ts"));
	assert!(parse_tool_scope_token("tool:").is_none());
	assert!(parse_tool_scope_token("bad token").is_none());

	let plan = build_scope(&Rule {
		name:          "r".to_string(),
		path:          PathBuf::from("r.md"),
		content:       String::new(),
		globs:         None,
		always_apply:  false,
		description:   None,
		condition:     None,
		ast_condition: None,
		scope:         None,
		provider:      "native".to_string(),
	});
	assert!(plan.allow_text && plan.allow_any_tool && !plan.allow_thinking);
}

#[test]
fn setup_speech_json_and_check_reports_match_contract() {
	let dir = TempDir::new("setup-speech-cli");
	let tiny = dir.path().join("tiny-models");
	for file in ["encoder.int8.onnx", "decoder.int8.onnx", "joiner.int8.onnx", "tokens.txt"] {
		dir.write(
			&format!("tiny-models/csukuangfj/sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8/{file}"),
			"x",
		);
	}

	let json_opts = SetupOptions { component: Some("speech".to_string()), check: false, json: true };
	let mut out = Vec::new();
	let code = run_setup_speech_with_cache(&json_opts, dir.path(), &tiny, &mut out).expect("speech json");
	assert_eq!(code, 1, "cache presence is not execution readiness");
	let report: Value = serde_json::from_slice(&out).expect("speech json report");
	assert_eq!(report["Speech-to-Text model"]["cached"], true);
	assert_eq!(report["Speech-to-Text model"]["ready"], false);
	assert_eq!(
		report["Speech-to-Text model"]["status"],
		"parakeet — cached; execution runtime not verified"
	);
	assert_eq!(report["Text-to-Speech model"]["cached"], false);
	assert_eq!(report["Text-to-Speech model"]["ready"], false);
	assert_eq!(
		report["Text-to-Speech model"]["status"],
		"kokoro — model/runtime not installed"
	);

	dir.write("tiny-models/onnx-community/Kokoro-82M-v1.0-ONNX/kokoro.onnx", "x");
	dir.write(
		"tts-runtime/kokoro-1.2.1/node_modules/kokoro-js/package.json",
		"{\"version\":\"1.2.1\"}",
	);
	let check_opts = SetupOptions { component: Some("speech".to_string()), check: true, json: false };
	let mut out = Vec::new();
	let code = run_setup_speech_with_cache(&check_opts, dir.path(), &tiny, &mut out).expect("speech check");
	assert_eq!(code, 1, "native build has no verified execution backend");
	let text = String::from_utf8(out).expect("utf8");
	assert!(text.starts_with("Speech dependencies:\n"), "{text}");
	assert!(text.contains("[missing] Speech-to-Text model: parakeet — cached; execution runtime not verified"), "{text}");
	assert!(text.contains("[missing] Text-to-Speech model: kokoro — cached; execution runtime not verified"), "{text}");
	assert!(!text.contains("[ok]"), "{text}");
}

#[test]
fn gc_text_renders_dry_run_lines() {
	let result = json!({
		"agentDir": "/tmp/agent",
		"apply": false,
		"lockPath": "/tmp/agent/gc.lock",
		"blobs": { "referenced": 2, "candidates": 3, "wouldDelete": 1, "deleted": 0, "bytes": 7, "errors": [] },
		"archive": {
			"scanned": 4,
			"skippedActive": 2,
			"keptNewestGlobal": 0,
			"keptNewestPerCwd": 0,
			"wouldArchive": 1,
			"archived": 0,
			"historyRowsDeleted": 0,
			"statsRowsDeleted": 0,
			"ftsRebuilt": false,
			"errors": [],
		},
		"wal": { "databases": [], "walBytes": 0, "wouldCheckpoint": false, "checkpointed": false },
	});
	assert_eq!(
		gc_text(&result),
		concat!(
			"GC dry-run (/tmp/agent)\n",
			"blobs: 0/1 files, 7 B, 2 refs\n",
			"sessions: 0/1 archived, 0 history rows and 0 stats rows removed\n",
			"sessions skipped active: 2\n",
			"wal: checkpoint dry-run, 0 B across 0 dbs\n",
		)
	);
}

fn write_gzip(path: &Path, content: &str) {
	use std::io::Write as _;
	use flate2::{Compression, write::GzEncoder};
	let file = fs::File::create(path).expect("create gzip fixture");
	let mut encoder = GzEncoder::new(file, Compression::best());
	encoder.write_all(content.as_bytes()).expect("gzip fixture");
	encoder.finish().expect("finish gzip fixture");
}

#[test]
fn gc_blob_refs_scan_compressed_and_backup_jsonl() {
	let dir = TempDir::new("gc-blob-refs");
	let agent = dir.path().join("agent");
	let a = "a".repeat(64);
	let b = "b".repeat(64);
	dir.write(
		"agent/sessions/project/s1.jsonl",
		"{\"type\":\"session\",\"id\":\"s1\",\"cwd\":\"/tmp/project\"}\n",
	);
	dir.write(
		"agent/sessions/project/s1.jsonl.1700000000.bak",
		&format!("{{\"blob\":\"BLOB:SHA256:{}\"}}\n", b.to_uppercase()),
	);
	let gz_path = dir.path().join("agent/archive/sessions/a.jsonl.gz");
	fs::create_dir_all(gz_path.parent().expect("gz parent")).expect("gz dir");
	write_gzip(&gz_path, &format!("{{\"blob\":\"blob:sha256:{a}\"}}\n"));

	let a_file = dir.write(&format!("agent/blobs/{a}"), "aaaa");
	let b_file = dir.write(&format!("agent/blobs/{b}.json"), "bb");
	set_old_mtime(&a_file, Duration::from_secs(3600));
	set_old_mtime(&b_file, Duration::from_secs(3600));

	let opts = GcOptions {
		apply:                  false,
		json:                    true,
		agent_dir:               Some(agent),
		blobs:                   true,
		archive:                 false,
		wal:                     false,
		cold_archive_after_days: None,
		retain_newest_global:    None,
		retain_newest_per_cwd:   None,
	};
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert_eq!(value["blobs"]["referenced"], 2);
	assert_eq!(value["blobs"]["candidates"], 2);
	assert_eq!(value["blobs"]["wouldDelete"], 0);
}

#[test]
fn gc_archive_uses_tail_status_and_nested_liveness() {
	let dir = TempDir::new("gc-session-status");
	let agent = dir.path().join("agent");
	let header = "{\"type\":\"session\",\"id\":\"s\",\"cwd\":\"/tmp/project\"}\n";
	let interrupted = dir.write(
		"agent/sessions/project/interrupted.jsonl",
		&format!(
			"{header}{{\"type\":\"message\",\"message\":{{\"role\":\"assistant\",\"stopReason\":\"length\",\"content\":[]}}}}\n"
		),
	);
	let aborted = dir.write(
		"agent/sessions/project/aborted.jsonl",
		&format!(
			"{header}{{\"type\":\"message\",\"message\":{{\"role\":\"assistant\",\"stopReason\":\"aborted\",\"content\":[]}}}}\n"
		),
	);
	let nested_parent = dir.write(
		"agent/sessions/project/complete-nested.jsonl",
		&format!(
			"{header}{{\"type\":\"message\",\"message\":{{\"role\":\"assistant\",\"stopReason\":\"stop\",\"content\":[]}}}}\n"
		),
	);
	dir.write(
		"agent/sessions/project/complete-nested/nested.jsonl",
		&format!(
			"{header}{{\"type\":\"message\",\"message\":{{\"role\":\"user\",\"content\":\"hi\"}}}}\n"
		),
	);
	// Invalid header is skipped by listSessionsReadOnly; root-level logs are not
	// part of a project directory either.
	dir.write("agent/sessions/project/invalid.jsonl", "{\"type\":\"message\"}\n");
	dir.write("agent/sessions/root.jsonl", header);
	for path in [&interrupted, &aborted, &nested_parent] {
		set_old_mtime(path, Duration::from_secs(60 * 86_400));
	}

	let opts = GcOptions {
		apply:                  false,
		json:                    true,
		agent_dir:               Some(agent),
		blobs:                   false,
		archive:                 true,
		wal:                     false,
		cold_archive_after_days: Some(30),
		retain_newest_global:    Some(0),
		retain_newest_per_cwd:   Some(0),
	};
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert_eq!(value["archive"]["scanned"], 3);
	assert_eq!(value["archive"]["skippedActive"], 2);
	assert_eq!(value["archive"]["wouldArchive"], 1);
}

#[test]
fn ttsr_builtin_defaults_are_embedded_from_ts_assets() {
	let rules = builtin_default_rules();
	assert_eq!(rules.len(), 27);
	let no_any = rules.iter().find(|rule| rule.name == "ts-no-any").expect("ts-no-any");
	assert_eq!(no_any.provider, "builtin-defaults");
	assert_eq!(no_any.path.to_string_lossy(), "builtin-defaults:ts-no-any.md");
	assert!(no_any.condition.as_ref().is_some_and(|items| !items.is_empty()));
	assert!(no_any.scope.as_ref().is_some_and(|items| items.iter().any(|scope| scope.contains("tool:edit"))));
}

#[test]
fn terminal_escaping_neutralizes_control_sequences() {
	let raw = "safe\n\u{1b}]52;c;base64\u{7}\u{9b}31m";
	let escaped = escape_terminal_text(raw, false);
	assert!(!escaped.contains('\u{1b}'), "{escaped}");
	assert!(!escaped.contains('\u{7}'), "{escaped}");
	assert!(!escaped.contains('\u{9b}'), "{escaped}");
	assert!(!escaped.contains('\n'), "{escaped}");
	assert!(!escaped.chars().any(char::is_control), "{escaped}");

	let kept = escape_terminal_text("a\nb\tc\u{1b}", true);
	assert!(kept.contains('\n'), "newlines preserved: {kept}");
	assert!(kept.contains('\t'), "tabs preserved: {kept}");
	assert!(!kept.contains('\u{1b}'), "other controls still escaped: {kept}");

	let detail = json!({
		"name": "rule\u{1b}[31m",
		"sourceProvider": "native\u{1b}]0;t\u{7}",
		"defined": { "regex": ["a\u{1b}[32m"], "ast": [] },
		"skippedAst": null,
	});
	let mut out = Vec::new();
	render_rule_detail(&mut out, &detail, false).expect("detail");
	let text = String::from_utf8(out).expect("utf8");
	assert!(!text.contains('\u{1b}'), "{text}");
	assert!(!text.contains('\u{7}'), "{text}");
	assert!(text.contains("rule"), "{text}");
}

#[test]
fn grievance_human_output_escapes_terminal_controls() {
	let dir = TempDir::new("grievance-escape");
	let db = dir.path().join("autoqa.db");
	let connection = rusqlite::Connection::open(&db).expect("db");
	connection
		.execute_batch(
			"CREATE TABLE grievances (id INTEGER PRIMARY KEY, model TEXT, version TEXT, tool TEXT, report TEXT);",
		)
		.expect("schema");
	connection
		.execute(
			"INSERT INTO grievances (id, model, version, tool, report) VALUES (1, 'm', '1', 'bash', ?1)",
			rusqlite::params!["line\u{1b}]52;c;x\nsecond\u{7}"],
		)
		.expect("row");
	drop(connection);
	let opts = GrievancesOptions { limit: 20, tool: None, json: false };
	let mut out = Vec::new();
	list_grievances_in(&db, &opts, &mut out).expect("list");
	let text = String::from_utf8(out).expect("utf8");
	assert!(!text.contains('\u{1b}'), "{text}");
	assert!(!text.contains('\u{7}'), "{text}");
	assert!(text.contains("\nsecond"), "newline layout preserved: {text}");
}

#[cfg(unix)]
#[test]
fn walkers_do_not_follow_symlink_cycles() {
	let dir = TempDir::new("gc-walk-bounds");
	let loop_dir = dir.path().join("sessions/project/loop");
	fs::create_dir_all(&loop_dir).expect("loop dir");
	std::os::unix::fs::symlink(&loop_dir, loop_dir.join("cycle")).expect("cycle symlink");
	dir.write("sessions/project/s.jsonl", "{}");
	assert_eq!(walk_visible_files(&dir.path().join("sessions"), is_jsonl_name).len(), 1);
	assert_eq!(collect_jsonl_files(&loop_dir, ".jsonl").len(), 0);
}

struct ChunkReader {
	data: Vec<u8>,
	pos:  usize,
	step: usize,
}

impl std::io::Read for ChunkReader {
	fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
		let remaining = self.data.len() - self.pos;
		let count = remaining.min(buffer.len()).min(self.step);
		buffer[..count].copy_from_slice(&self.data[self.pos..self.pos + count]);
		self.pos += count;
		Ok(count)
	}
}

#[test]
fn gzip_ref_scan_streams_large_archives_and_chunk_boundaries() {
	let a = "a".repeat(64);
	let b = "b".repeat(64);
	let ref_a = format!("blob:sha256:{a}");
	let ref_b = format!("blob:sha256:{b}");

	let mut payload = vec![b'.'; BLOB_SCAN_CHUNK - 10];
	payload.extend_from_slice(ref_a.as_bytes());
	payload.extend_from_slice(b" tail ");
	payload.extend_from_slice(ref_b.as_bytes());
	let mut hashes = HashSet::new();
	let mut reader = ChunkReader { data: payload, pos: 0, step: 7 };
	scan_blob_refs_reader(&mut reader, Path::new("chunked.jsonl"), &mut hashes).expect("chunked scan");
	assert!(hashes.contains(&a), "reference spanning a chunk boundary");
	assert!(hashes.contains(&b), "reference after the boundary");

	let dir = TempDir::new("gc-gzip-large");
	let gz_path = dir.path().join("large.jsonl.gz");
	let mut encoder = flate2::write::GzEncoder::new(
		fs::File::create(&gz_path).expect("gz"),
		flate2::Compression::fast(),
	);
	use std::io::Write as _;
	let padding = vec![b'x'; DEFAULT_MAX_SCAN_BYTES as usize + 1024 * 1024];
	encoder.write_all(&padding).expect("padding");
	// `/\bblob:/` requires a non-word byte before the reference; padding ends
	// in a word byte, so this newline is part of the fixture's lexical context.
	encoder.write_all(b"\n").expect("separator");
	encoder.write_all(ref_a.as_bytes()).expect("ref");
	encoder.finish().expect("finish");
	let mut hashes = HashSet::new();
	scan_blob_ref_file(&gz_path, true, &mut hashes).expect("large gzip scan");
	assert!(hashes.contains(&a), "large archive refs are not rejected");
}

#[test]
fn gc_agent_dir_uses_its_own_settings() {
	let dir = TempDir::new("gc-agent-settings");
	let agent = dir.path().join("alt-agent");
	dir.write(
		"alt-agent/config.yml",
		"gc:\n  blobs: false\n  archive: false\n  wal: false\n",
	);
	let opts = GcOptions {
		apply:                  false,
		json:                    true,
		agent_dir:               Some(agent),
		blobs:                   false,
		archive:                 false,
		wal:                     false,
		cold_archive_after_days: None,
		retain_newest_global:    None,
		retain_newest_per_cwd:   None,
	};
	let (code, value, stderr) = gc_json(&opts, dir.path());
	assert_eq!(code, 0, "{stderr}");
	assert!(value.get("blobs").is_none(), "{value}");
	assert!(value.get("archive").is_none(), "{value}");
	assert!(value.get("wal").is_none(), "{value}");
}

#[test]
fn gc_agent_dir_text_escapes_controls() {
	let result = json!({ "agentDir": "/tmp/\u{1b}]0;x", "apply": false, "lockPath": "/tmp/x" });
	let text = gc_text(&result);
	assert!(!text.contains('\u{1b}'), "{text}");
	assert!(text.contains("GC dry-run"), "{text}");
}

#[test]
fn ttsr_preview_escapes_terminal_controls() {
	let dir = TempDir::new("ttsr-preview-escape");
	let rule = dir.write(
		"plain.md",
		"---\ncondition: 'trigger'\n---\nBody\n",
	);
	let opts = TtsrOptions {
		action:  Some("test".to_string()),
		snippet: Some("trigger\u{1b}]52;c;x".to_string()),
		file:    None,
		rule:    Some(rule),
		source:  Some("text".to_string()),
		tool:    None,
		path:    None,
		verbose: false,
		json:    false,
		no_gitignore: false,
		max_bytes: None,
	};
	let mut out = Vec::new();
	let mut err = Vec::new();
	run_ttsr(&opts, dir.path(), &mut out, &mut err).expect("ttsr test");
	let text = String::from_utf8(out).expect("utf8");
	assert!(!text.contains('\u{1b}'), "{text}");
	assert!(text.contains("trigger"), "{text}");
}

#[test]
fn python_candidates_use_reported_managed_env_order() {
	let dir = TempDir::new("python-candidates");
	let venv = dir.path().join("project/.venv");
	let venv_python = if cfg!(windows) {
		venv.join("Scripts/python.exe")
	} else {
		venv.join("bin/python")
	};
	dir.write(
		&venv_python.strip_prefix(dir.path()).expect("relative").to_string_lossy(),
		"x",
	);
	let managed = dir.path().join("custom/python-env");
	let managed_python = if cfg!(windows) {
		managed.join("Scripts/python.exe")
	} else {
		managed.join("bin/python")
	};
	dir.write(
		&managed_python.strip_prefix(dir.path()).expect("relative").to_string_lossy(),
		"x",
	);
	let mut base = HashMap::new();
	base.insert("PATH".to_string(), String::new());
	let cwd = dir.path().join("project");
	let candidates = python_candidates(&cwd, &base, None, &managed);
	assert_eq!(
		candidates.first().map(|runtime| runtime.program.as_path()),
		Some(venv_python.as_path())
	);
	assert_eq!(
		candidates.get(1).map(|runtime| runtime.program.as_path()),
		Some(managed_python.as_path())
	);

	let no_venv = dir.path().join("no-venv");
	fs::create_dir_all(&no_venv).expect("cwd");
	let candidates = python_candidates(&no_venv, &base, None, &managed);
	assert_eq!(
		candidates.first().map(|runtime| runtime.program.as_path()),
		Some(managed_python.as_path())
	);
}

#[test]
fn grievances_missing_table_is_empty_but_corrupt_or_busy_fails() {
	let dir = TempDir::new("grievance-errors");
	let opts = GrievancesOptions { limit: 20, tool: None, json: false };

	let missing = dir.path().join("missing-table.db");
	let connection = rusqlite::Connection::open(&missing).expect("db");
	connection
		.execute_batch("CREATE TABLE unrelated (id INTEGER);")
		.expect("schema");
	drop(connection);
	let mut out = Vec::new();
	list_grievances_in(&missing, &opts, &mut out).expect("missing table is empty");
	assert!(String::from_utf8(out).expect("utf8").contains("No grievances recorded yet."));

	let corrupt = dir.path().join("corrupt.db");
	fs::write(&corrupt, b"not a sqlite database").expect("corrupt db");
	let mut out = Vec::new();
	assert!(list_grievances_in(&corrupt, &opts, &mut out).is_err());

	let busy = dir.path().join("busy.db");
	let writer = rusqlite::Connection::open(&busy).expect("busy db");
	writer
		.execute_batch(
			"CREATE TABLE grievances (id INTEGER PRIMARY KEY, model TEXT, version TEXT, tool TEXT, report TEXT);\nPRAGMA locking_mode = EXCLUSIVE;\nBEGIN EXCLUSIVE;",
		)
		.expect("lock");
	let mut out = Vec::new();
	assert!(list_grievances_in(&busy, &opts, &mut out).is_err(), "busy must fail closed");
}

#[test]
fn ttsr_discovers_configured_extension_rules() {
	let dir = TempDir::new("ttsr-extension-rules");
	dir.write(".omp/config.yml", "extensions:\n  - ext\n");
	dir.write(
		"ext/rules/extension-rule.md",
		"---\ncondition: 'extension trigger'\n---\nExtension body\n",
	);
	let rules = discover_rules(dir.path(), &dir.path().join("agent"));
	let rule = rules
		.iter()
		.find(|rule| rule.name == "extension-rule")
		.expect("extension rule");
	assert_eq!(rule.provider, "omp-plugins");
	assert_eq!(rule.condition, Some(vec!["extension trigger".to_string()]));
}
