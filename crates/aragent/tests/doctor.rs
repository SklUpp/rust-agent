//! `arag doctor` end-to-end: JSON schema, findings, and repair/redaction flows.
//!
//! Every test spawns the real binary with a private `HOME`, `PI_CONFIG_DIR`,
//! `TMPDIR`, and broker runtime so doctor cannot touch the developer's state.

use std::{
	fs,
	path::{Path, PathBuf},
	process::{Command, Output, Stdio},
	sync::atomic::{AtomicU64, Ordering},
	time::{Duration, SystemTime},
};

use aragent::session_store::{NewSession, SqliteStore};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

const SECRET: &str = "sk-proj-abcdefghij1234567890";

struct Fixture {
	root:     PathBuf,
	home:     PathBuf,
	base:     PathBuf,
	agent:    PathBuf,
	sessions: PathBuf,
	work:     PathBuf,
	project:  PathBuf,
}

impl Fixture {
	fn new(label: &str) -> Self {
		static NEXT: AtomicU64 = AtomicU64::new(0);
		let root = std::env::temp_dir().join(format!(
			"aragent-doctor-{}-{}-{label}",
			std::process::id(),
			NEXT.fetch_add(1, Ordering::Relaxed)
		));
		let _ = fs::remove_dir_all(&root);
		let home = root.join("home");
		let base = home.join(".omp");
		let agent = base.join("agent");
		let sessions = agent.join("sessions");
		let work = root.join("work");
		let project = sessions.join("project");
		let tmp = root.join("tmp");
		let runtime = root.join("run");
		for dir in [&home, &base, &agent, &sessions, &project, &work, &tmp, &runtime] {
			fs::create_dir_all(dir).expect("create fixture directory");
		}
		for dir in [&home, &base, &agent, &sessions, &tmp, &runtime] {
			set_mode(dir, 0o700);
		}
		Self { root, home, base, agent, sessions, work, project }
	}

	fn db(&self) -> PathBuf {
		self.project.join("sessions.db")
	}

	fn command(&self) -> Command {
		let mut command = Command::new(env!("CARGO_BIN_EXE_arag"));
		command
			.current_dir(&self.work)
			.env("HOME", &self.home)
			.env("PI_CONFIG_DIR", &self.base)
			.env("TMPDIR", self.root.join("tmp"))
			.env("XDG_RUNTIME_DIR", self.root.join("run"))
			.stdin(Stdio::null());
		for key in [
			"PI_CODING_AGENT_DIR",
			"PI_CONFIG_FILES",
			"PI_PROFILE",
			"OMP_PROFILE",
			"ARAGENT_PROFILE",
			"XDG_DATA_HOME",
			"XDG_STATE_HOME",
			"XDG_CACHE_HOME",
			"OMP_DAEMON_RUNTIME_ROOT",
			"OMP_WORKSPACE_WRITER_LOCK_DIR",
			"ARAGENT_WORKSPACE_WRITER_LOCK_DIR",
		] {
			command.env_remove(key);
		}
		command
	}

	fn run(&self, args: &[&str]) -> Output {
		self.command().args(args).output().expect("run arag")
	}

	/// Like [`Fixture::command`] but without an explicit storage override, so
	/// `~/.omp` is the legacy root and `~/.aragent` the default destination.
	fn legacy_command(&self) -> Command {
		let mut command = self.command();
		command
			.env_remove("PI_CONFIG_DIR")
			.env_remove("PI_CODING_AGENT_DIR");
		command
	}

	fn seed_session(&self) -> PathBuf {
		let db = self.db();
		let mut store = SqliteStore::open(&db).expect("open fixture database");
		store
			.create_session(NewSession {
				id:                    "session-1".to_string(),
				parent_session_id:     None,
				payload:               json!({
					"type": "session",
					"id": "session-1",
					"timestamp": "2026-01-01T00:00:00Z",
					"cwd": self.work.to_string_lossy(),
				}),
				initial_branch_id:     "branch-1".to_string(),
				initial_branch_payload: json!({}),
			})
			.expect("create fixture session");
		drop(store);
		set_mode(&db, 0o600);
		db
	}
}

impl Drop for Fixture {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.root);
	}
}

fn code(output: &Output) -> i32 {
	output.status.code().expect("exit code")
}

fn stdout(output: &Output) -> String {
	String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
	String::from_utf8_lossy(&output.stderr).into_owned()
}

fn report(output: &Output) -> Value {
	serde_json::from_slice(&output.stdout).expect("doctor JSON report")
}

fn section_codes<'a>(report: &'a Value, name: &str) -> Vec<&'a str> {
	report["sections"]
		.as_array()
		.expect("sections array")
		.iter()
		.find(|section| section["name"] == name)
		.expect("section present")["findings"]
		.as_array()
		.expect("findings array")
		.iter()
		.map(|finding| finding["code"].as_str().expect("finding code"))
		.collect()
}

fn assert_json_schema(report: &Value) {
	assert_eq!(report["version"], 1);
	assert!(report["clean"].is_boolean());
	let sections = report["sections"].as_array().expect("sections array");
	assert_eq!(sections.len(), 9);
	for section in sections {
		assert!(section["name"].is_string());
		assert!(section["status"].is_string());
		assert!(section["findings"].is_array());
	}
	assert!(report["summary"]["errors"].is_u64());
	assert!(report["summary"]["warnings"].is_u64());
	assert!(report["summary"]["info"].is_u64());
}

fn age_file(path: &Path, age: Duration) {
	let time = SystemTime::now() - age;
	fs::File::create(path)
		.expect("create aged file")
		.set_times(fs::FileTimes::new().set_modified(time))
		.expect("set old mtime");
}

fn files_under(root: &Path) -> Vec<String> {
	fn walk(root: &Path, base: &Path, files: &mut Vec<String>) {
		let Ok(entries) = fs::read_dir(root) else {
			return;
		};
		for entry in entries.flatten() {
			let path = entry.path();
			let Ok(kind) = entry.file_type() else {
				continue;
			};
			if kind.is_dir() {
				walk(&path, base, files);
			} else if kind.is_file() {
				let relative = path.strip_prefix(base).unwrap_or(path.as_path());
				files.push(relative.to_string_lossy().into_owned());
			}
		}
	}
	let mut files = Vec::new();
	walk(root, root, &mut files);
	files.sort();
	files
}

#[test]
fn corrupted_database_reports_json_findings() {
	let fixture = Fixture::new("corrupt");
	let broken = fixture.sessions.join("broken");
	fs::create_dir_all(&broken).expect("create broken project dir");
	fs::write(broken.join("sessions.db"), b"this is not a sqlite database").expect("write garbage");

	let output = fixture.run(&["doctor", "--json"]);
	assert_eq!(code(&output), 1, "{}", stdout(&output));
	let initial_report = report(&output);
	assert_json_schema(&initial_report);
	assert_eq!(initial_report["clean"], false);
	let db = initial_report["sections"]
		.as_array()
		.expect("sections")
		.iter()
		.find(|section| section["name"] == "db")
		.expect("db section");
	assert_ne!(db["status"], "ok");
	let codes = section_codes(&initial_report, "db");
	assert!(
		codes.contains(&"db-open") || codes.contains(&"integrity"),
		"expected an open/integrity finding, got {codes:?}"
	);

	let fixed = fixture.run(&["doctor", "--fix", "--json"]);
	assert_eq!(code(&fixed), 1, "--fix must not hide a corrupt database");
	assert!(
		broken.join("sessions.db").is_file(),
		"--fix must never delete or replace an unreadable database"
	);

	let blobs = fixture.agent.join("blobs");
	fs::create_dir_all(&blobs).expect("create blobs dir");
	set_mode(&blobs, 0o700);
	let orphan = blobs.join("looks-unreferenced.bin");
	fs::write(&orphan, b"keep me").expect("write blob");
	age_file(&orphan, Duration::from_secs(40 * 24 * 60 * 60));

	let prune = fixture.run(&["doctor", "--prune-artifacts", "--yes", "--json"]);
	assert_eq!(code(&prune), 1, "corrupt database remains a finding");
	let prune_report = report(&prune);
	assert!(
		section_codes(&prune_report, "artifacts").contains(&"prune-skipped"),
		"{}",
		stdout(&prune)
	);
	assert!(orphan.is_file(), "incomplete scan must never prune blobs");
}

#[test]
fn orphan_rows_fail_and_fix_only_checkpoints_the_wal() {
	let fixture = Fixture::new("orphan");
	let db = fixture.seed_session();
	let connection = Connection::open(&db).expect("open raw fixture database");
	connection
		.pragma_update(None, "journal_mode", "WAL")
		.expect("enable WAL");
	connection
		.pragma_update(None, "foreign_keys", "OFF")
		.expect("disable FK for fixture");
	connection
		.execute(
			"INSERT INTO messages (id, session_id, branch_id, parent_message_id, payload_json, \
			 created_at_ms) VALUES ('ghost-message', 'ghost-session', 'ghost-branch', NULL, '{}', \
			 1)",
			[],
		)
		.expect("insert orphan message");
	let wal = PathBuf::from(format!("{}-wal", db.display()));
	assert!(wal.exists(), "fixture must leave a WAL behind");

	let scan = fixture.run(&["doctor", "--json"]);
	assert_eq!(code(&scan), 1, "{}", stdout(&scan));
	let scan_report = report(&scan);
	assert_json_schema(&scan_report);
	assert!(section_codes(&scan_report, "db").contains(&"foreign-key"));

	let fixed = fixture.run(&["doctor", "--fix", "--json"]);
	assert_eq!(code(&fixed), 1, "orphans must remain without explicit repair");
	let fixed_report = report(&fixed);
	assert!(
		section_codes(&fixed_report, "db").contains(&"wal-checkpointed"),
		"{}",
		stdout(&fixed)
	);
	let orphans: i64 = connection
		.query_row(
			"SELECT COUNT(*) FROM messages WHERE session_id = 'ghost-session'",
			[],
			|row| row.get(0),
		)
		.expect("count orphan rows");
	assert_eq!(orphans, 1, "--fix must not delete rows");
	assert!(
		!wal.exists() || fs::metadata(&wal).expect("wal metadata").len() == 0,
		"--fix must truncate the WAL"
	);
}

#[test]
fn legacy_secret_is_reported_never_printed_and_redacted_after_backup() {
	let fixture = Fixture::new("redact");
	let db = fixture.seed_session();
	{
		let connection = Connection::open(&db).expect("open raw fixture database");
		connection
			.execute(
				"INSERT INTO messages (id, session_id, branch_id, parent_message_id, payload_json, \
				 created_at_ms) VALUES ('secret-message', 'session-1', 'branch-1', NULL, ?1, 1)",
				params![serde_json::json!({"text": SECRET}).to_string()],
			)
			.expect("insert secret payload");
		connection
			.execute(
				"INSERT INTO executions (execution_id, session_id, branch_id, provider_call_id, \
				 tool_name, intent_json, created_at_ms, outcome_json, outcome_at_ms) VALUES \
				 ('execution-1', 'session-1', 'branch-1', 'call-1', 'bash', '{}', 1, ?1, 2)",
				params![serde_json::json!({"output": SECRET}).to_string()],
			)
			.expect("insert completed execution with secret outcome");
	}

	let scan = fixture.run(&["doctor", "--json"]);
	assert_eq!(code(&scan), 1, "{}", stdout(&scan));
	let text = stdout(&scan);
	assert!(text.contains("messages: 1"), "{text}");
	assert!(text.contains("executions: 1"), "{text}");
	assert!(!text.contains(SECRET), "doctor must never print the secret");

	let missing_backup = fixture.run(&["doctor", "--redact-legacy", "--json"]);
	assert_eq!(code(&missing_backup), 2, "{}", stderr(&missing_backup));
	assert!(stderr(&missing_backup).contains("--backup"));

	let backup = fixture.root.join("backup.db");
	let no_confirm = fixture.run(&[
		"doctor",
		"--redact-legacy",
		"--backup",
		backup.to_str().expect("utf-8 backup path"),
		"--json",
	]);
	assert_eq!(code(&no_confirm), 2, "{}", stderr(&no_confirm));
	assert!(!backup.exists(), "confirmation must happen before the backup is written");
	let still_secret: String = Connection::open(&db)
		.expect("reopen fixture database")
		.query_row("SELECT payload_json FROM messages WHERE id = 'secret-message'", [], |row| {
			row.get(0)
		})
		.expect("read untouched payload");
	assert!(still_secret.contains(SECRET));

	let redacted = fixture.run(&[
		"doctor",
		"--redact-legacy",
		"--backup",
		backup.to_str().expect("utf-8 backup path"),
		"--yes",
		"--json",
	]);
	assert_eq!(code(&redacted), 0, "{}", stdout(&redacted));
	let report = report(&redacted);
	assert_json_schema(&report);
	assert_eq!(report["clean"], true);
	assert!(backup.is_file(), "backup must exist");
	assert_eq!(mode(&backup), 0o600, "backup must not be readable by other users");
	let staging_leftovers = fs::read_dir(&fixture.root)
		.expect("list fixture root")
		.flatten()
		.filter(|entry| {
			entry.file_name().to_string_lossy().starts_with(".arag-doctor-backup-")
		})
		.count();
	assert_eq!(staging_leftovers, 0, "backup staging directory must be removed");
	let backup_db = Connection::open(&backup).expect("open backup");
	let integrity: String =
		backup_db.query_row("PRAGMA integrity_check", [], |row| row.get(0)).expect("verify backup");
	assert_eq!(integrity, "ok");
	// The snapshot must describe the committed pre-redaction state, so the
	// backup still contains the legacy value the rewrite removes from the source.
	let backed: String = backup_db
		.query_row("SELECT payload_json FROM messages WHERE id = 'secret-message'", [], |row| {
			row.get(0)
		})
		.expect("read backup payload");
	assert!(backed.contains(SECRET), "backup must preserve the pre-redaction value");
	let rewritten = Connection::open(&db).expect("reopen fixture database");
	let payload: String = rewritten
		.query_row("SELECT payload_json FROM messages WHERE id = 'secret-message'", [], |row| {
			row.get(0)
		})
		.expect("read redacted payload");
	assert!(!payload.contains(SECRET), "payload must be rewritten: {payload}");
	let outcome: String = rewritten
		.query_row(
			"SELECT outcome_json FROM executions WHERE execution_id = 'execution-1'",
			[],
			|row| row.get(0),
		)
		.expect("read redacted outcome");
	assert!(!outcome.contains(SECRET), "outcome must be rewritten: {outcome}");
	let trigger: Option<String> = rewritten
		.query_row(
			"SELECT sql FROM sqlite_master WHERE type = 'trigger' AND \
			 name = 'executions_outcome_immutable'",
			[],
			|row| row.get(0),
		)
		.optional()
		.expect("read trigger");
	assert!(trigger.is_some(), "immutability trigger must be recreated");
	let update = rewritten.execute(
		"UPDATE executions SET outcome_json = '{}' WHERE execution_id = 'execution-1'",
		[],
	);
	assert!(update.is_err(), "immutability trigger must remain active");
	assert!(stdout(&redacted).contains("redacted"));
}

#[test]
fn fix_preserves_artifacts_and_prune_deletes_only_old_unreferenced_files() {
	let fixture = Fixture::new("artifacts");
	let db = fixture.seed_session();
	let session_dir = fixture.sessions.join("project").join("session-1");
	let blobs = fixture.agent.join("blobs");
	let referenced = session_dir.join("referenced.bin");
	let old_orphan = blobs.join("old-orphan.bin");
	let recent_orphan = blobs.join("recent-orphan.bin");
	fs::create_dir_all(&session_dir).expect("create session artifact dir");
	fs::create_dir_all(&blobs).expect("create blobs dir");
	set_mode(&blobs, 0o700);
	fs::write(&referenced, b"referenced").expect("write referenced artifact");
	fs::write(&old_orphan, b"old orphan").expect("write old orphan");
	fs::write(&recent_orphan, b"recent orphan").expect("write recent orphan");
	let old_age = Duration::from_secs(40 * 24 * 60 * 60);
	age_file(&referenced, old_age);
	age_file(&old_orphan, old_age);
	// Stored with a `.`/`..` alias: canonicalization must still match the file
	// the disk walk sees, otherwise this old referenced file would be pruned.
	let dotted = format!("{}/../session-1/referenced.bin", session_dir.display());
	{
		let connection = Connection::open(&db).expect("open raw fixture database");
		connection
			.execute(
				"INSERT INTO artifacts (artifact_id, session_id, branch_id, execution_id, location, \
				 metadata_json, created_at_ms) VALUES ('artifact-1', 'session-1', 'branch-1', NULL, \
				 ?1, '{}', 1)",
				params![dotted],
			)
			.expect("insert artifact metadata");
	}

	let fixed = fixture.run(&["doctor", "--fix", "--json"]);
	assert_eq!(code(&fixed), 1, "unreferenced files remain a finding");
	for path in [&referenced, &old_orphan, &recent_orphan] {
		assert!(path.is_file(), "--fix must preserve {}", path.display());
	}

	let pruned = fixture.run(&["doctor", "--prune-artifacts", "--yes", "--json"]);
	assert_eq!(code(&pruned), 1, "the recent orphan must remain a finding");
	let report = report(&pruned);
	assert_json_schema(&report);
	assert!(section_codes(&report, "artifacts").contains(&"pruned"), "{}", stdout(&pruned));
	assert!(!old_orphan.exists(), "old unreferenced artifact must be pruned");
	assert!(referenced.is_file(), "referenced artifact must survive");
	assert!(recent_orphan.is_file(), "young unreferenced artifact must survive");

	let without_yes = fixture.run(&["doctor", "--prune-artifacts", "--json"]);
	assert_eq!(code(&without_yes), 2, "{}", stderr(&without_yes));

	let shared = blobs.join("shared-old.bin");
	fs::write(&shared, b"shared").expect("write shared blob");
	age_file(&shared, old_age);
	let narrowed = fixture.run(&[
		"doctor",
		"--session-dir",
		fixture.sessions.to_str().expect("utf-8 sessions path"),
		"--prune-artifacts",
		"--yes",
		"--json",
	]);
	assert_eq!(code(&narrowed), 2, "{}", stderr(&narrowed));
	assert!(stderr(&narrowed).contains("--session-dir"));
	assert!(shared.is_file(), "shared blobs must survive a narrowed prune request");
}

#[test]
fn missing_artifact_is_not_relinked_by_basename() {
	let fixture = Fixture::new("artifact-missing");
	let db = fixture.seed_session();
	let session_dir = fixture.sessions.join("project").join("session-1");
	let candidate = session_dir.join("lost.bin");
	let missing = fixture.work.join("lost.bin");
	fs::create_dir_all(&session_dir).expect("create session artifact dir");
	fs::write(&candidate, b"same basename, unknown contents").expect("write candidate");
	{
		let connection = Connection::open(&db).expect("open raw fixture database");
		connection
			.execute(
				"INSERT INTO artifacts (artifact_id, session_id, branch_id, execution_id, location, \
				 metadata_json, created_at_ms) VALUES ('artifact-missing', 'session-1', 'branch-1', \
				 NULL, ?1, '{}', 1)",
				params![missing.to_string_lossy().into_owned()],
			)
			.expect("insert missing artifact metadata");
	}

	let fixed = fixture.run(&["doctor", "--fix", "--json"]);
	assert_eq!(code(&fixed), 1, "{}", stdout(&fixed));
	let report = report(&fixed);
	let codes = section_codes(&report, "artifacts");
	assert!(codes.contains(&"artifact-missing"), "{codes:?}");
	assert!(!codes.contains(&"relinked"), "{codes:?}");
	let location: String = Connection::open(&db)
		.expect("reopen fixture database")
		.query_row(
			"SELECT location FROM artifacts WHERE artifact_id = 'artifact-missing'",
			[],
			|row| row.get(0),
		)
		.expect("read artifact location");
	assert_eq!(
		location,
		missing.to_string_lossy().into_owned(),
		"doctor must not guess a replacement"
	);
	assert!(candidate.is_file(), "candidate must remain untouched");
}

#[cfg(unix)]
#[test]
fn symlink_alias_artifact_is_not_pruned() {
	let fixture = Fixture::new("symlink-artifact");
	let db = fixture.seed_session();
	let session_dir = fixture.sessions.join("project").join("session-1");
	let blobs = fixture.agent.join("blobs");
	let referenced = session_dir.join("aliased.bin");
	let alias = blobs.join("alias.bin");
	fs::create_dir_all(&session_dir).expect("create session artifact dir");
	fs::create_dir_all(&blobs).expect("create blobs dir");
	set_mode(&blobs, 0o700);
	fs::write(&referenced, b"aliased").expect("write referenced artifact");
	age_file(&referenced, Duration::from_secs(40 * 24 * 60 * 60));
	std::os::unix::fs::symlink(&referenced, &alias).expect("create symlink alias");
	{
		let connection = Connection::open(&db).expect("open raw fixture database");
		connection
			.execute(
				"INSERT INTO artifacts (artifact_id, session_id, branch_id, execution_id, location, \
				 metadata_json, created_at_ms) VALUES ('artifact-alias', 'session-1', 'branch-1', \
				 NULL, ?1, '{}', 1)",
				params![alias.to_string_lossy().into_owned()],
			)
			.expect("insert symlink-aliased artifact metadata");
	}

	let pruned = fixture.run(&["doctor", "--prune-artifacts", "--yes", "--json"]);
	assert_eq!(code(&pruned), 0, "{}", stdout(&pruned));
	assert!(referenced.is_file(), "symlink-aliased referenced artifact must survive");
}

#[test]
fn dead_gc_lock_is_removed_and_permissions_are_repaired() {
	let fixture = Fixture::new("repair");
	let db = fixture.seed_session();
	for dir in [&fixture.base, &fixture.agent, &fixture.sessions] {
		set_mode(dir, 0o755);
	}
	set_mode(&db, 0o644);
	let gc_lock = fixture.agent.join("gc.lock");
	fs::write(&gc_lock, "999999999\n").expect("write stale gc lock");
	age_file(&gc_lock, Duration::from_secs(25 * 60 * 60));
	let run_dir = fixture.agent.join("run");
	fs::create_dir_all(&run_dir).expect("create run dir");
	let stable_lock = run_dir.join("stable-test.lock");
	age_file(&stable_lock, Duration::from_secs(25 * 60 * 60));
	let held_lock = run_dir.join("held-test.lock");
	let held = fs::File::create(&held_lock).expect("create held lock");
	held.set_times(fs::FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(25 * 60 * 60)))
		.expect("age held lock");
	#[cfg(unix)]
	{
		use std::os::fd::AsRawFd as _;
		// SAFETY: `flock` operates on a valid open descriptor owned by `held`.
		let locked = unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
		assert_eq!(locked, 0, "test must hold the exclusive lock");
	}

	let fixed = fixture.run(&["doctor", "--fix", "--json"]);
	assert_eq!(code(&fixed), 0, "{}", stdout(&fixed));
	assert!(!gc_lock.exists(), "dead, old gc.lock must be removed");
	assert!(stable_lock.exists(), "advisory lock files are never unlinked");
	assert!(held_lock.exists(), "a held lock must never be unlinked");
	assert_eq!(mode(&stable_lock), 0o600);
	assert_eq!(mode(&held_lock), 0o600);
	assert_eq!(mode(&fixture.agent), 0o700);
	assert_eq!(mode(&fixture.sessions), 0o700);
	assert_eq!(mode(&db), 0o600);

	let again = fixture.run(&["doctor", "--json"]);
	assert_eq!(code(&again), 0, "{}", stdout(&again));
	assert_eq!(report(&again)["clean"], true);
}

#[test]
fn doctor_reports_aragent_session_writer_locks() {
	let fixture = Fixture::new("aragent-locks");
	let session_lock_dir = fixture.home.join(".aragent/run/session-writer");
	fs::create_dir_all(&session_lock_dir).expect("session lock dir");
	set_mode(&session_lock_dir, 0o700);
	let lock = session_lock_dir.join("idle-test.lock");
	fs::write(&lock, "idle\n").expect("idle lock");
	set_mode(&lock, 0o600);

	let report = fixture.run(&["doctor", "--json"]);
	assert_eq!(code(&report), 0, "{}", stdout(&report));
	let output = stdout(&report);
	assert!(
		output.contains("session-writer") && output.contains("idle-test.lock"),
		"doctor must enumerate ~/.aragent lock roots: {output}"
	);
	assert!(lock.exists(), "doctor never removes advisory locks");
}

#[cfg(unix)]
#[test]
fn gc_lock_guard_blocks_doctor_removal() {
	let fixture = Fixture::new("gc-guard");
	fixture.seed_session();
	let gc_lock = fixture.agent.join("gc.lock");
	fs::write(&gc_lock, "999999999\n").expect("write stale gc lock");
	age_file(&gc_lock, Duration::from_secs(25 * 60 * 60));
	let guard_path = fixture.agent.join("gc.lock.guard");
	let guard = fs::File::create(&guard_path).expect("create gc guard");
	{
		use std::os::fd::AsRawFd as _;
		// SAFETY: `flock` operates on a valid open descriptor owned by `guard`.
		let locked = unsafe { libc::flock(guard.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
		assert_eq!(locked, 0, "test must hold the GC guard");
	}

	let fixed = fixture.run(&["doctor", "--fix", "--json"]);
	assert_eq!(code(&fixed), 0, "{}", stdout(&fixed));
	assert!(gc_lock.exists(), "gc.lock must survive while GC holds gc.lock.guard");
	assert!(stdout(&fixed).contains("gc-lock-guard-held"), "{}", stdout(&fixed));
}

#[cfg(unix)]
#[test]
fn broker_lease_lock_blocks_doctor_cleanup() {
	let fixture = Fixture::new("broker-lease");
	fixture.seed_session();
	let dir = fixture.agent.join("run").join("daemons").join("scope-test");
	fs::create_dir_all(&dir).expect("create broker runtime dir");
	let pid_path = dir.join("broker.pid");
	let token_path = dir.join("broker.token");
	let socket_path = dir.join("broker.sock");
	fs::write(
		&pid_path,
		serde_json::json!({"pid": 999_999u32, "startTime": null, "instanceId": "dead"})
			.to_string(),
	)
	.expect("write dead broker pid");
	set_mode(&pid_path, 0o600);
	fs::write(&token_path, b"dead token").expect("write broker token");
	fs::write(&socket_path, b"stale socket placeholder").expect("write broker socket");
	let lease_lock = dir.join("broker.lease.lock");
	let guard = fs::File::create(&lease_lock).expect("create broker lease lock");
	{
		use std::os::fd::AsRawFd as _;
		// SAFETY: `flock` operates on a valid open descriptor owned by `guard`.
		let locked = unsafe { libc::flock(guard.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
		assert_eq!(locked, 0, "test must hold the broker lease lock");
	}

	let held = fixture.run(&["doctor", "--fix", "--json"]);
	assert_eq!(code(&held), 0, "{}", stdout(&held));
	assert!(pid_path.is_file(), "dead-looking broker.pid must survive while the lock is held");
	assert!(token_path.is_file(), "broker.token must survive while the lock is held");
	assert!(socket_path.is_file(), "broker.sock must survive while the lock is held");
	assert!(stdout(&held).contains("broker-lease-lock-held"), "{}", stdout(&held));

	drop(guard);
	let cleaned = fixture.run(&["doctor", "--fix", "--json"]);
	assert_eq!(code(&cleaned), 0, "{}", stdout(&cleaned));
	assert!(!pid_path.exists(), "dead broker.pid must be removed once the lock is free");
	assert!(!token_path.exists(), "broker.token must be removed once the lock is free");
	assert!(!socket_path.exists(), "broker.sock must be removed once the lock is free");
}

#[test]
fn config_invalid_keys_are_detected_and_fix_never_rewrites_config() {
	let fixture = Fixture::new("config");
	let config = fixture.agent.join("config.yml");
	let original = "model: stale-model\nunknownDoctorKey: true\ntask:\n  maxConcurrency: 0\n";
	fs::write(&config, original).expect("write config");
	set_mode(&config, 0o600);

	let scan = fixture.run(&["doctor", "--json"]);
	assert_eq!(code(&scan), 1, "{}", stdout(&scan));
	let text = stdout(&scan);
	assert!(text.contains("stale-key"), "{text}");
	assert!(text.contains("unknown-key"), "{text}");
	assert!(text.contains("invalid-limit"), "{text}");
	let report = report(&scan);
	assert_json_schema(&report);
	assert!(section_codes(&report, "config").contains(&"stale-key"));

	let fixed = fixture.run(&["doctor", "--fix", "--json"]);
	assert_eq!(code(&fixed), 1, "config findings still require a manual edit");
	assert!(stdout(&fixed).contains("suggestion"), "{}", stdout(&fixed));
	assert_eq!(fs::read_to_string(&config).expect("read config"), original);
}

#[test]
fn default_doctor_is_strictly_read_only() {
	let fixture = Fixture::new("readonly");
	let before_base = files_under(&fixture.base);
	let before_tmp = files_under(&fixture.root.join("tmp"));

	let output = fixture.run(&["doctor", "--json"]);
	assert_eq!(code(&output), 0, "{}", stdout(&output));
	assert_eq!(report(&output)["clean"], true);

	assert_eq!(files_under(&fixture.base), before_base, "default doctor wrote into the config root");
	assert_eq!(files_under(&fixture.root.join("tmp")), before_tmp, "default doctor wrote into TMPDIR");
	assert!(!fixture.agent.join("android-probes").exists(), "probe cache must not be created");
	assert!(!fixture.base.join("security").exists(), "state dir must not be created");
	assert!(!fixture.agent.join("config.yml").exists());
	assert!(!fixture.base.join("config.yml").exists());
}

#[test]
fn legacy_home_doctor_is_read_only_and_reports_migration_status() {
	let fixture = Fixture::new("legacy-root");
	let legacy_config = fixture.agent.join("config.yml");
	fs::write(&legacy_config, "{}\n").expect("write legacy config");
	set_mode(&legacy_config, 0o600);
	let current = fixture.home.join(".aragent");
	assert!(!current.exists(), "fixture must start without ~/.aragent");

	// Root help/version must never perform startup side effects either.
	for flag in ["--help", "-V"] {
		let help = fixture
			.legacy_command()
			.arg(flag)
			.output()
			.expect("run arag");
		assert_eq!(code(&help), 0, "{flag}: {}", stderr(&help));
		assert!(!current.exists(), "{flag} must not create ~/.aragent");
	}

	let pending = fixture
		.legacy_command()
		.args(["doctor", "--json"])
		.output()
		.expect("run arag");
	assert!(!stderr(&pending).contains("panicked"), "{}", stderr(&pending));
	assert!(code(&pending) < 2, "doctor usage failure: {}", stderr(&pending));
	let parsed = report(&pending);
	assert_json_schema(&parsed);
	assert_eq!(parsed["summary"]["errors"], 0, "{}", stdout(&pending));
	assert!(section_codes(&parsed, "config").contains(&"legacy-pending"), "{}", stdout(&pending));
	assert!(!current.exists(), "read-only doctor must not create ~/.aragent");

	let marker = current.join(aragent::dirs::MIGRATION_MARKER);
	fs::create_dir_all(&current).expect("create ~/.aragent");
	set_mode(&current, 0o700);
	fs::write(&marker, b"copied\n").expect("write migration marker");

	let migrated = fixture
		.legacy_command()
		.args(["doctor", "--json"])
		.output()
		.expect("run arag");
	assert!(!stderr(&migrated).contains("panicked"), "{}", stderr(&migrated));
	assert!(code(&migrated) < 2, "doctor usage failure: {}", stderr(&migrated));
	let parsed = report(&migrated);
	assert_json_schema(&parsed);
	assert_eq!(parsed["summary"]["errors"], 0, "{}", stdout(&migrated));
	assert!(section_codes(&parsed, "config").contains(&"legacy-root"), "{}", stdout(&migrated));
	assert!(!section_codes(&parsed, "config").contains(&"legacy-pending"));
	assert!(!current.join("agent").exists(), "doctor must not create ~/.aragent/agent");
	assert_eq!(
		fs::read_to_string(&legacy_config).ok().as_deref(),
		Some("{}\n"),
		"doctor must never rewrite the legacy config"
	);
}

#[test]
fn doctor_returns_exit_code_2_when_destructive_flags_lack_guards() {
	let fixture = Fixture::new("exit-2");
	let missing_yes = fixture.run(&["doctor", "--prune-artifacts"]);
	assert_eq!(code(&missing_yes), 2, "{}", stderr(&missing_yes));
	let missing_backup = fixture.run(&["doctor", "--redact-legacy", "--yes"]);
	assert_eq!(code(&missing_backup), 2, "{}", stderr(&missing_backup));
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) {
	use std::os::unix::fs::PermissionsExt;
	fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("chmod fixture");
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) {}

#[cfg(unix)]
fn mode(path: &Path) -> u32 {
	use std::os::unix::fs::PermissionsExt;
	fs::metadata(path).expect("metadata").permissions().mode() & 0o777
}

#[cfg(not(unix))]
fn mode(_path: &Path) -> u32 {
	0o700
}
