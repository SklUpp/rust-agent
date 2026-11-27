//! `arag doctor`: diagnostics for the local runtime state.
//!
//! Read-only by default. `--fix` performs only non-destructive repairs (WAL
//! checkpoint, dead gc-lock/broker-lease cleanup, `chmod`, probe-cache reset)
//! and never relinks artifacts by filename. Destructive work is opt-in:
//! `--prune-artifacts --yes` deletes orphaned artifacts, and
//! `--redact-legacy --backup <PATH> --yes`
//! rewrites legacy history after a verified atomic backup.
//!
//! Never prints secret values, credential URLs, tokens, or response bodies:
//! findings contain categories, counts, codes, and scrubbed paths only.
//!
//! Exit contract: `0` clean, `1` findings remain, `2` I/O or argument error
//! (the CLI maps a returned error to [`crate::EXIT_USAGE`]).

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fs,
	io::{BufRead as _, IsTerminal as _, Read as _, Write},
	path::{Path, PathBuf},
	process::{Command, Stdio},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OpenFlags, OptionalExtension as _, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt as _;

use crate::{
	Result,
	auth::error::{redact_free_text, redact_json_strings},
	config::Config,
	dirs,
	session_store::{SCHEMA_VERSION, SqliteStore},
	yaml,
};

const REPORT_VERSION: u32 = 1;
const SECTIONS: [&str; 9] = [
	"db",
	"sessions",
	"redaction",
	"artifacts",
	"config",
	"locks",
	"permissions",
	"env",
	"platform",
];
const WAL_WARN_BYTES: u64 = 32 * 1024 * 1024;
const ARTIFACT_TTL_DAYS: u64 = 30;
const ARTIFACT_TTL: Duration = Duration::from_secs(ARTIFACT_TTL_DAYS * 24 * 60 * 60);
const LOCK_STALE_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const PROBE_NAMES: [&str; 4] = ["pidfd_open", "seccomp", "landlock_create_ruleset", "memfd_create"];

/// Columns scanned for legacy secrets and rewritten by `--redact-legacy`.
/// `bool` marks a JSON column (parsed and traversed) versus free text.
const REDACTION_COLUMNS: &[(&str, &[(&str, bool)])] = &[
	("sessions", &[("payload_json", true)]),
	("branches", &[("payload_json", true)]),
	("messages", &[("payload_json", true)]),
	("executions", &[("intent_json", true), ("outcome_json", true)]),
	("deliveries", &[("payload_json", true)]),
	("worker_completions", &[("completion_json", true)]),
	("artifacts", &[("location", false), ("metadata_json", true)]),
	("task_admissions", &[("spec_json", true)]),
	("stream_drafts", &[("payload_json", true)]),
];

/// Root settings keys this binary parses or intentionally shares with the
/// TypeScript app. Unknown keys are informational, not failures.
const CONFIG_ROOT_KEYS: &[&str] = &[
	"defaultModel",
	"systemPrompt",
	"appendSystemPrompt",
	"cwd",
	"approvalMode",
	"allow",
	"tools",
	"providers",
	"hooks",
	"tier",
	"disabledProviders",
	"disabledExtensions",
	"modelRoles",
	"task",
	"security",
	"extensions",
	"edit",
	"readLineNumbers",
	"ask",
	"searxng",
	"bash",
	"compaction",
	"retry",
	"images",
	"shell",
	"diagnostics",
	"memory",
	"lsp",
	"skills",
	"autolearn",
	"commits",
	"debug",
	"advisor",
	"sandbox",
	"goal",
	"plan",
	"python",
	"ruby",
	"julia",
	"eval",
	"worktree",
	"notifications",
	"theme",
	"keybindings",
	"setupVersion",
	"lastChangelogVersion",
	"checkpoint",
	"prewalk",
	"notes",
	"agents",
	"logging",
	"telemetry",
	"cache",
	"models",
];

/// Keys this runtime no longer reads. They are reported as stale.
const STALE_ROOT_KEYS: &[&str] = &[
	"model",
	"provider",
	"apiKey",
	"openaiApiKey",
	"anthropicApiKey",
	"googleApiKey",
	"temperature",
	"maxTokens",
	"max_tokens",
	"maxConcurrency",
	"maxRecursionDepth",
	"thinking",
	"topP",
];

/// Options for one doctor pass.
#[derive(Debug, Clone, Default)]
pub struct DoctorOptions {
	/// Apply safe non-destructive repairs before reporting.
	pub fix:             bool,
	/// Emit one JSON document instead of the human section report.
	pub json:            bool,
	/// Session storage root to scan instead of the default agent sessions dir.
	pub session_dir:     Option<PathBuf>,
	/// Delete orphaned artifact files eligible for pruning; requires `yes`.
	pub prune_artifacts: bool,
	/// Rewrite stored history that still contains redactable secrets.
	pub redact_legacy:   bool,
	/// Database backup path written before `--redact-legacy` modifies anything.
	pub backup:          Option<PathBuf>,
	/// Confirm a data-modifying operation without an interactive prompt.
	pub yes:             bool,
	/// Discard the cached Android runtime capability probe profile.
	pub reset_probes:    bool,
	/// Extra `--config` overlays, resolved against the effective cwd.
	pub overlays:        Vec<PathBuf>,
}

/// Validate `arag doctor` data-modification flags before any scan or write.
///
/// Redaction confirmation is separate: `--yes` or an interactive TTY prompt.
/// A non-TTY run without `--yes` fails closed with exit `2`.
pub fn validate_options(options: &DoctorOptions) -> std::result::Result<(), String> {
	if options.prune_artifacts && !options.yes {
		return Err(
			"arag doctor --prune-artifacts requires --yes; no artifact was deleted".to_string(),
		);
	}
	if options.prune_artifacts && options.session_dir.is_some() {
		return Err(
			"arag doctor --prune-artifacts cannot be combined with --session-dir: shared blob \
			 storage may be referenced by session databases outside the subset; no artifact was \
			 deleted"
				.to_string(),
		);
	}
	if options.redact_legacy && options.backup.is_none() {
		return Err(
			"arag doctor --redact-legacy requires --backup <PATH>; no history was rewritten"
				.to_string(),
		);
	}
	if options.backup.is_some() && !options.redact_legacy {
		return Err(
			"arag doctor --backup is only valid together with --redact-legacy".to_string(),
		);
	}
	Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
enum Level {
	Info,
	Warn,
	Error,
}

impl Level {
	fn tag(self) -> &'static str {
		match self {
			Self::Info => "info",
			Self::Warn => "warn",
			Self::Error => "error",
		}
	}

	fn is_finding(self) -> bool {
		self >= Self::Warn
	}
}

#[derive(Debug, Serialize)]
struct Finding {
	level:   Level,
	code:    &'static str,
	message: String,
}

#[derive(Debug)]
struct Section {
	name:     &'static str,
	findings: Vec<Finding>,
}

impl Section {
	fn status(&self) -> &'static str {
		self.findings.iter().map(|finding| finding.level).max().map_or("ok", Level::tag)
	}
}

#[derive(Debug, Default)]
struct Report {
	sections: Vec<Section>,
}

#[derive(Debug, Serialize)]
struct Summary {
	errors:   usize,
	warnings: usize,
	info:     usize,
}

#[derive(Debug, Serialize)]
struct SectionReport<'a> {
	name:     &'static str,
	status:   &'static str,
	findings: &'a [Finding],
}

#[derive(Debug, Serialize)]
struct JsonReport<'a> {
	version:  u32,
	clean:    bool,
	summary:  Summary,
	sections: Vec<SectionReport<'a>>,
}

impl Report {
	fn new() -> Self {
		Self {
			sections: SECTIONS
				.iter()
				.map(|name| Section { name, findings: Vec::new() })
				.collect(),
		}
	}

	fn push(&mut self, section: &'static str, level: Level, code: &'static str, message: String) {
		let message = redact_free_text(&message);
		let section = self
			.sections
			.iter_mut()
			.find(|candidate| candidate.name == section)
			.unwrap_or_else(|| panic!("unknown doctor section {section}"));
		section.findings.push(Finding { level, code, message });
	}

	fn info(&mut self, section: &'static str, code: &'static str, message: String) {
		self.push(section, Level::Info, code, message);
	}

	fn warn(&mut self, section: &'static str, code: &'static str, message: String) {
		self.push(section, Level::Warn, code, message);
	}

	fn error(&mut self, section: &'static str, code: &'static str, message: String) {
		self.push(section, Level::Error, code, message);
	}

	fn section_len(&self, section: &'static str) -> usize {
		self
			.sections
			.iter()
			.find(|candidate| candidate.name == section)
			.map_or(0, |candidate| candidate.findings.len())
	}

	fn summary(&self) -> Summary {
		let mut summary = Summary { errors: 0, warnings: 0, info: 0 };
		for finding in self.sections.iter().flat_map(|section| &section.findings) {
			match finding.level {
				Level::Error => summary.errors += 1,
				Level::Warn => summary.warnings += 1,
				Level::Info => summary.info += 1,
			}
		}
		summary
	}

	fn has_findings(&self) -> bool {
		self
			.sections
			.iter()
			.flat_map(|section| &section.findings)
			.any(|finding| finding.level.is_finding())
	}
}

/// Run every check and write the report to `out`.
///
/// Returns the process exit code: `0` clean, `1` findings remain. I/O and
/// mutation failures propagate; the CLI maps them to exit `2`.
pub fn run_doctor(options: &DoctorOptions, cwd: &Path, out: &mut dyn Write) -> Result<u8> {
	let agent_dir = dirs::agent_dir();
	let config_root = dirs::config_root();
	let sessions_root = options
		.session_dir
		.clone()
		.unwrap_or_else(|| dirs::agent_subdir("sessions", dirs::AgentDirCategory::Data));
	let blobs_root = dirs::agent_subdir("blobs", dirs::AgentDirCategory::Data);
	let session_dbs = discover_session_dbs(&sessions_root);
	let auxiliary_dbs = auxiliary_databases(&agent_dir);

	let mut report = Report::new();
	let config = check_config(&mut report, cwd, &options.overlays, options.fix);
	check_databases(&mut report, &session_dbs, &auxiliary_dbs, options.fix);
	check_sessions(&mut report, &session_dbs);
	check_redaction(&mut report, &session_dbs, options)?;
	check_artifacts(&mut report, &sessions_root, &blobs_root, &session_dbs, options);
	check_locks(&mut report, &agent_dir, options.fix);
	check_migration_status(&mut report);
	check_permissions(
		&mut report,
		&config_root,
		&agent_dir,
		&sessions_root,
		&blobs_root,
		&session_dbs,
		options.fix,
	);
	check_env(&mut report, config.as_ref(), &agent_dir, &sessions_root, &blobs_root);
	check_platform(&mut report, options.reset_probes || options.fix);

	if options.json {
		let json = JsonReport {
			version:  REPORT_VERSION,
			clean:    !report.has_findings(),
			summary:  report.summary(),
			sections: report
				.sections
				.iter()
				.map(|section| SectionReport {
					name:     section.name,
					status:   section.status(),
					findings: &section.findings,
				})
				.collect(),
		};
		writeln!(out, "{}", serde_json::to_string_pretty(&json)?)?;
	} else {
		write_human(&report, out)?;
	}
	Ok(if report.has_findings() { crate::EXIT_FAILURE } else { crate::EXIT_SUCCESS })
}

fn write_human(report: &Report, out: &mut dyn Write) -> std::io::Result<()> {
	writeln!(out, "arag doctor")?;
	for section in &report.sections {
		writeln!(out, "\n{} [{}]", section.name, section.status())?;
		if section.findings.is_empty() {
			writeln!(out, "  ok")?;
			continue;
		}
		for finding in &section.findings {
			writeln!(out, "  {} {}: {}", finding.level.tag(), finding.code, finding.message)?;
		}
	}
	let summary = report.summary();
	writeln!(
		out,
		"\nsummary: {} error(s), {} warning(s), {} info",
		summary.errors, summary.warnings, summary.info
	)?;
	writeln!(
		out,
		"{}",
		if report.has_findings() { "findings present" } else { "clean" }
	)?;
	Ok(())
}

// ── shared helpers ──────────────────────────────────────────────────────────

fn safe_path(path: &Path) -> String {
	redact_free_text(&path.display().to_string())
}

fn first_line(text: &str) -> &str {
	text.lines().next().unwrap_or_default()
}

fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

fn digest_hex(bytes: &[u8]) -> String {
	let digest = Sha256::digest(bytes);
	let mut out = String::with_capacity(digest.len() * 2);
	for byte in digest {
		use std::fmt::Write as _;
		let _ = write!(out, "{byte:02x}");
	}
	out
}

fn crate_error(error: impl std::fmt::Display) -> crate::Error {
	crate::Error::new(error.to_string())
}

fn is_old(metadata: &fs::Metadata) -> bool {
	metadata
		.modified()
		.ok()
		.and_then(|modified| SystemTime::now().duration_since(modified).ok())
		.is_some_and(|age| age >= LOCK_STALE_AGE)
}

fn discover_session_dbs(sessions_root: &Path) -> Vec<PathBuf> {
	let mut dbs = Vec::new();
	let Ok(entries) = fs::read_dir(sessions_root) else {
		return dbs;
	};
	for entry in entries.flatten() {
		let db = entry.path().join("sessions.db");
		if db.is_file() {
			dbs.push(db);
		}
	}
	dbs.sort();
	dbs
}

fn auxiliary_databases(agent_dir: &Path) -> Vec<(PathBuf, &'static str)> {
	let candidates = [
		(agent_dir.join("agent.db"), "agent"),
		(agent_dir.join("memory.db"), "memory"),
		(dirs::agent_subdir("history.db", dirs::AgentDirCategory::Data), "history"),
		(dirs::agent_subdir("models.db", dirs::AgentDirCategory::Data), "models"),
		(
			dirs::agent_subdir("diagnostics", dirs::AgentDirCategory::Data).join("diagnostics.db"),
			"diagnostics",
		),
	];
	let mut found = Vec::new();
	for (path, label) in candidates {
		if path.is_file() {
			found.push((path, label));
		}
	}
	found
}

// ── config ──────────────────────────────────────────────────────────────────

fn check_config(
	report: &mut Report,
	cwd: &Path,
	overlays: &[PathBuf],
	fix: bool,
) -> Option<Config> {
	let before = report.section_len("config");
	for path in config_layers(cwd, overlays) {
		if !path.is_file() {
			continue;
		}
		match fs::read_to_string(&path).map_err(|error| error.to_string()).and_then(|text| {
			yaml::parse(&text).map_err(|error| error.to_string())
		}) {
			Ok(yaml::Value::Map(pairs)) => {
				inspect_config_keys(report, &path, &pairs, fix);
			},
			Ok(_) => report.error(
				"config",
				"config-root",
				format!("{}: expected a mapping at the document root", safe_path(&path)),
			),
			Err(error) => report.error(
				"config",
				"config-parse",
				format!("{}: {error}", safe_path(&path)),
			),
		}
	}

	let config = match Config::load_run(overlays, Some(cwd)) {
		Ok(config) => Some(config),
		Err(error) => {
			report.error("config", "config-load", format!("cannot load effective config: {error}"));
			None
		},
	};
	if let Some(config) = config.as_ref() {
		check_config_limits(report, config);
	}
	if fix && report.section_len("config") > before {
		report.info(
			"config",
			"suggestion",
			"suggested edit only: fix the keys named above in the reported config file; doctor never \
			 rewrites configuration"
				.to_string(),
		);
	}
	config
}

fn config_layers(cwd: &Path, overlays: &[PathBuf]) -> Vec<PathBuf> {
	let mut layers = Vec::new();
	if let Some(base) = crate::config::base_path() {
		layers.push(base);
	}
	for name in dirs::MAIN_CONFIG_FILENAMES {
		let path = cwd.join(dirs::PROJECT_CONFIG_DIR_NAME).join(name);
		if path.is_file() && !layers.contains(&path) {
			layers.push(path);
		}
	}
	if let Some(raw) = std::env::var_os("PI_CONFIG_FILES") {
		for path in std::env::split_paths(&raw).filter(|path| !path.as_os_str().is_empty()) {
			if !layers.contains(&path) {
				layers.push(path);
			}
		}
	}
	for overlay in overlays {
		let path = dirs::absolute(cwd, overlay);
		if !layers.contains(&path) {
			layers.push(path);
		}
	}
	layers
}

fn inspect_config_keys(
	report: &mut Report,
	path: &Path,
	pairs: &[(String, yaml::Value)],
	fix: bool,
) {
	for (key, _) in pairs {
		if STALE_ROOT_KEYS.contains(&key.as_str()) {
			report.warn(
				"config",
				"stale-key",
				format!("{}: stale root key `{key}` is no longer read", safe_path(path)),
			);
			if fix {
				report.info(
					"config",
					"suggestion",
					format!("suggested edit: remove stale key `{key}` from {}", safe_path(path)),
				);
			}
		} else if !CONFIG_ROOT_KEYS.contains(&key.as_str()) {
			report.info(
				"config",
				"unknown-key",
				format!("{}: unknown root key `{key}` (ignored)", safe_path(path)),
			);
		}
	}
}

fn check_config_limits(report: &mut Report, config: &Config) {
	if config.max_concurrency == Some(0) {
		report.warn(
			"config",
			"invalid-limit",
			"task.maxConcurrency is 0; background task admission will never run".to_string(),
		);
	}
	if let Some(depth) = config.max_recursion_depth
		&& depth <= 0
	{
		report.warn(
			"config",
			"invalid-limit",
			format!("task.maxRecursionDepth is {depth}; it must be positive"),
		);
	}
	if let Some(timeout) = config.ask.timeout_ms
		&& timeout > 24 * 60 * 60 * 1000
	{
		report.warn(
			"config",
			"invalid-timeout",
			format!("ask.timeout is {timeout} ms; longer than 24 hours"),
		);
	}
}

// ── databases ───────────────────────────────────────────────────────────────

fn check_databases(
	report: &mut Report,
	session_dbs: &[PathBuf],
	auxiliary_dbs: &[(PathBuf, &'static str)],
	fix: bool,
) {
	let mut checkpoints = 0usize;
	if fix {
		for db in session_dbs.iter().map(PathBuf::as_path).chain(
			auxiliary_dbs.iter().map(|(path, _)| path.as_path()),
		) {
			match SqliteStore::open_read_write_existing(db) {
				Ok(store) => match store.checkpoint_wal_truncate() {
					Ok(()) => checkpoints += 1,
					Err(error) => report.warn(
						"db",
						"wal-checkpoint",
						format!("{}: {error}", safe_path(db)),
					),
				},
				Err(error) => report.warn(
					"db",
					"wal-checkpoint",
					format!("{}: {error}", safe_path(db)),
				),
			}
		}
		if checkpoints > 0 {
			report.info(
				"db",
				"wal-checkpointed",
				format!("checkpointed the write-ahead log of {checkpoints} database(s)"),
			);
		}
	}

	for db in session_dbs {
		check_database(report, db, true);
	}
	for (db, label) in auxiliary_dbs {
		check_database(report, db, false);
		let _ = label;
	}
	report.info(
		"db",
		"database-count",
		format!(
			"checked {} session database(s) and {} auxiliary database(s)",
			session_dbs.len(),
			auxiliary_dbs.len()
		),
	);
}

fn check_database(report: &mut Report, path: &Path, session_schema: bool) {
	let label = safe_path(path);
	let store = match SqliteStore::open_read_only(path) {
		Ok(store) => store,
		Err(error) => {
			report.error("db", "db-open", format!("{label}: {error}"));
			return;
		},
	};
	let connection = store.connection();

	match connection.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0)) {
		Ok(result) if result == "ok" => {},
		Ok(result) => report.error(
			"db",
			"integrity",
			format!("{label}: integrity_check reported {}", first_line(&result)),
		),
		Err(error) => {
			report.error("db", "integrity", format!("{label}: integrity_check failed: {error}"));
		},
	}

	match count_statement_rows(connection, "PRAGMA foreign_key_check") {
		Ok(0) => {},
		Ok(count) => report.error(
			"db",
			"foreign-key",
			format!("{label}: foreign_key_check found {count} broken reference(s)"),
		),
		Err(error) => {
			report.error("db", "foreign-key", format!("{label}: foreign_key_check failed: {error}"));
		},
	}

	if !session_schema {
		return;
	}
	check_schema_version(report, connection, &label);
	check_orphans(report, connection, &label);
	check_wal(report, path, &label);
}

fn check_schema_version(report: &mut Report, connection: &Connection, label: &str) {
	let recorded = connection
		.query_row("SELECT version FROM schema_version WHERE id = 1", [], |row| row.get::<_, i64>(0))
		.optional();
	match recorded {
		Ok(Some(version)) if version == SCHEMA_VERSION => {},
		Ok(Some(version)) => report.warn(
			"db",
			"schema-version",
			format!("{label}: schema v{version}, binary expects v{SCHEMA_VERSION}"),
		),
		Ok(None) => report.error("db", "schema-version", format!("{label}: no schema_version row")),
		Err(error) => report.error(
			"db",
			"schema-version",
			format!("{label}: cannot read schema_version: {error}"),
		),
	}
}

fn check_orphans(report: &mut Report, connection: &Connection, label: &str) {
	orphan_check(
		report,
		connection,
		label,
		"active-branch-missing",
		Level::Warn,
		"SELECT COUNT(*) FROM sessions s WHERE s.active_branch_id IS NOT NULL AND NOT EXISTS \
		 (SELECT 1 FROM branches b WHERE b.id = s.active_branch_id)",
		"session(s) point at an active branch that does not exist",
	);
	orphan_check(
		report,
		connection,
		label,
		"branch-parent-missing",
		Level::Error,
		"SELECT COUNT(*) FROM branches b WHERE b.parent_branch_id IS NOT NULL AND NOT EXISTS \
		 (SELECT 1 FROM branches p WHERE p.id = b.parent_branch_id)",
		"branch(es) point at a missing parent branch",
	);
	orphan_check(
		report,
		connection,
		label,
		"message-parent-missing",
		Level::Error,
		"SELECT COUNT(*) FROM messages m WHERE m.parent_message_id IS NOT NULL AND NOT EXISTS \
		 (SELECT 1 FROM messages p WHERE p.id = m.parent_message_id)",
		"message(s) point at a missing parent message",
	);
	orphan_check(
		report,
		connection,
		label,
		"message-branch-missing",
		Level::Error,
		"SELECT COUNT(*) FROM messages m WHERE NOT EXISTS \
		 (SELECT 1 FROM branches b WHERE b.id = m.branch_id)",
		"message(s) belong to a missing branch",
	);
	orphan_check(
		report,
		connection,
		label,
		"message-session-missing",
		Level::Error,
		"SELECT COUNT(*) FROM messages m WHERE NOT EXISTS \
		 (SELECT 1 FROM sessions s WHERE s.id = m.session_id)",
		"message(s) belong to a missing session",
	);
	if table_exists(connection, "executions") {
		orphan_check(
			report,
			connection,
			label,
			"execution-session-missing",
			Level::Error,
			"SELECT COUNT(*) FROM executions e WHERE NOT EXISTS \
			 (SELECT 1 FROM sessions s WHERE s.id = e.session_id)",
			"execution(s) belong to a missing session",
		);
		orphan_check(
			report,
			connection,
			label,
			"execution-branch-missing",
			Level::Error,
			"SELECT COUNT(*) FROM executions e WHERE NOT EXISTS \
			 (SELECT 1 FROM branches b WHERE b.id = e.branch_id)",
			"execution(s) belong to a missing branch",
		);
	}
	if table_exists(connection, "deliveries") && column_exists(connection, "deliveries", "message_id")
	{
		orphan_check(
			report,
			connection,
			label,
			"delivery-message-missing",
			Level::Error,
			"SELECT COUNT(*) FROM deliveries d WHERE d.message_id IS NOT NULL AND NOT EXISTS \
			 (SELECT 1 FROM messages m WHERE m.id = d.message_id)",
			"delivery row(s) point at a missing message",
		);
	}
	if table_exists(connection, "fork_provenance") {
		orphan_check(
			report,
			connection,
			label,
			"fork-source-pruned",
			Level::Info,
			"SELECT COUNT(*) FROM fork_provenance f WHERE NOT EXISTS \
			 (SELECT 1 FROM executions e WHERE e.execution_id = f.source_execution_id)",
			"fork provenance row(s) reference a pruned source execution (informational)",
		);
		orphan_check(
			report,
			connection,
			label,
			"fork-malformed",
			Level::Error,
			"SELECT COUNT(*) FROM fork_provenance f WHERE trim(f.message_id) = '' OR \
			 trim(f.session_id) = '' OR trim(f.source_execution_id) = '' OR \
			 trim(f.provider_call_id) = ''",
			"fork provenance row(s) have empty identity fields",
		);
	}
}

fn check_wal(report: &mut Report, path: &Path, label: &str) {
	let mut wal = path.as_os_str().to_os_string();
	wal.push("-wal");
	let wal = PathBuf::from(wal);
	let Ok(metadata) = fs::metadata(&wal) else {
		return;
	};
	if metadata.len() > WAL_WARN_BYTES {
		report.warn(
			"db",
			"wal-large",
			format!(
				"{label}: write-ahead log is {} bytes; `arag doctor --fix` would checkpoint it",
				metadata.len()
			),
		);
	}
}

// ── sessions ────────────────────────────────────────────────────────────────

fn check_sessions(report: &mut Report, session_dbs: &[PathBuf]) {
	for db in session_dbs {
		let label = safe_path(db);
		let Ok(store) = SqliteStore::open_read_only(db) else {
			continue;
		};
		let connection = store.connection();

		if let Some(count) =
			count_optional(connection, "SELECT COUNT(*) FROM executions WHERE outcome_json IS NULL")
			&& count > 0
		{
			report.info(
				"sessions",
				"unresolved-intents",
				format!("{label}: {count} execution(s) without an outcome (never auto-replayed)"),
			);
		}
		if table_exists(connection, "stream_drafts")
			&& let Some(count) = count_optional(connection, "SELECT COUNT(*) FROM stream_drafts")
			&& count > 0
		{
			report.warn(
				"sessions",
				"stream-drafts",
				format!("{label}: {count} residual stream draft(s)"),
			);
		}
		if table_exists(connection, "deliveries")
			&& let Some(count) = count_optional(
				connection,
				"SELECT COUNT(*) FROM deliveries WHERE delivered_at_ms IS NULL OR processed_at_ms IS NULL",
			)
			&& count > 0
		{
			report.info(
				"sessions",
				"pending-deliveries",
				format!("{label}: {count} delivery row(s) not fully processed"),
			);
		}
		if table_exists(connection, "task_admissions")
			&& let Some(count) = count_optional(
				connection,
				"SELECT COUNT(*) FROM task_admissions WHERE state IN ('admitted', 'started')",
			)
			&& count > 0
		{
			report.info(
				"sessions",
				"open-admissions",
				format!("{label}: {count} task admission(s) still open"),
			);
		}
		if let Some(count) = count_optional(
			connection,
			"SELECT COUNT(*) FROM branches b WHERE NOT EXISTS \
			 (SELECT 1 FROM messages m WHERE m.branch_id = b.id)",
		) && count > 0
		{
			report.info(
				"sessions",
				"empty-branches",
				format!("{label}: {count} branch(es) without messages"),
			);
		}
	}
}

// ── redaction ───────────────────────────────────────────────────────────────

fn check_redaction(
	report: &mut Report,
	session_dbs: &[PathBuf],
	options: &DoctorOptions,
) -> Result<()> {
	let mut scanned: BTreeMap<PathBuf, Vec<(&'static str, usize)>> = BTreeMap::new();
	for db in session_dbs {
		let Ok(store) = SqliteStore::open_read_only(db) else {
			continue;
		};
		match scan_redaction_tables(store.connection()) {
			Ok(counts) => {
				scanned.insert(db.clone(), counts);
			},
			Err(error) => report.warn(
				"redaction",
				"scan-failed",
				format!("{}: cannot scan session text: {error}", safe_path(db)),
			),
		}
	}

	let targets: Vec<&PathBuf> = scanned
		.iter()
		.filter(|(_, counts)| counts.iter().any(|(_, count)| *count > 0))
		.map(|(db, _)| db)
		.collect();

	if options.redact_legacy && !targets.is_empty() {
		if confirm_redaction(targets.len(), options.yes)? {
			let backup = options.backup.as_deref().expect("validated redaction backup path");
			let many = targets.len() > 1;
			if many {
				fs::create_dir_all(backup)?;
				if !backup.is_dir() {
					return Err(crate::Error::new(format!(
						"{}: multiple session databases need a backup directory",
						safe_path(backup)
					)));
				}
			}
			let mut backups = Vec::new();
			let mut total_rows = 0usize;
			for db in &targets {
				let destination = backup_destination(db, backup, many);
				let mut store =
					SqliteStore::open_read_write_existing(db.as_path()).map_err(crate_error)?;
				let source = db.as_path().to_path_buf();
				// The store method acquires BEGIN IMMEDIATE before calling the
				// snapshot, so no other connection can commit between the
				// verified backup and the rewrite below.
				let changed = store
					.redact_legacy_history_with_backup(&destination, |target| {
						backup_snapshot(&source, target).map_err(|error| error.to_string())
					})
					.map_err(crate_error)?;
				backups.push(destination);
				let rows: usize = changed.iter().map(|(_, count)| count).sum();
				total_rows += rows;
				let tables = changed
					.iter()
					.map(|(table, count)| format!("{table}:{count}"))
					.collect::<Vec<_>>()
					.join(", ");
				report.info(
					"redaction",
					"redacted",
					format!(
						"{}: rewrote {} row(s) in one transaction ({tables})",
						safe_path(db),
						rows
					),
				);
			}
			report.info(
				"redaction",
				"backup",
				format!(
					"verified backup of {} database(s) written to {}",
					backups.len(),
					safe_path(backup)
				),
			);
			report.info(
				"redaction",
				"redacted-total",
				format!("redacted {total_rows} legacy row(s); no secret value was printed"),
			);
			// Re-scan so the report reflects the post-redaction state.
			scanned.clear();
			for db in session_dbs {
				if let Ok(store) = SqliteStore::open_read_only(db) {
					if let Ok(counts) = scan_redaction_tables(store.connection()) {
						scanned.insert(db.clone(), counts);
					}
				}
			}
		} else {
			report.info(
				"redaction",
				"cancelled",
				"legacy redaction cancelled; no history was rewritten".to_string(),
			);
		}
	}

	for (db, counts) in &scanned {
		for (table, count) in counts {
			if *count > 0 {
				report.warn(
					"redaction",
					"unredacted-secrets",
					format!(
						"{}: {table}: {count} row(s); use --redact-legacy --backup <PATH> --yes to \
						 rewrite",
						safe_path(db)
					),
				);
			}
		}
	}
	Ok(())
}

fn scan_redaction_tables(
	connection: &Connection,
) -> rusqlite::Result<Vec<(&'static str, usize)>> {
	let mut counts = Vec::new();
	for (table, columns) in REDACTION_COLUMNS {
		if !table_exists(connection, table) {
			continue;
		}
		let mut present: Vec<(&'static str, bool)> = Vec::new();
		for (column, is_json) in *columns {
			if column_exists(connection, table, column) {
				present.push((*column, *is_json));
			}
		}
		if present.is_empty() {
			continue;
		}
		let projection = present.iter().map(|(column, _)| *column).collect::<Vec<_>>().join(", ");
		let sql = format!("SELECT {projection} FROM {table}");
		let mut statement = connection.prepare(&sql)?;
		let mut rows = statement.query([])?;
		let mut changed = 0usize;
		while let Some(row) = rows.next()? {
			let mut row_changed = false;
			for (offset, (_, is_json)) in present.iter().enumerate() {
				let value: Option<String> = row.get(offset)?;
				let Some(value) = value else {
					continue;
				};
				if *is_json {
					if let Ok(mut json) = serde_json::from_str::<serde_json::Value>(&value) {
						let before = json.clone();
						redact_json_strings(&mut json);
						if json != before {
							row_changed = true;
						}
					}
				} else if redact_free_text(&value) != value {
					row_changed = true;
				}
			}
			if row_changed {
				changed += 1;
			}
		}
		counts.push((*table, changed));
	}
	Ok(counts)
}

fn confirm_redaction(targets: usize, assume_yes: bool) -> Result<bool> {
	if assume_yes {
		return Ok(true);
	}
	let stdin = std::io::stdin();
	if !stdin.is_terminal() {
		return Err(crate::Error::new(
			"arag doctor --redact-legacy needs --yes when stdin is not a terminal; no history was \
			 rewritten",
		));
	}
	let mut stderr = std::io::stderr();
	write!(
		stderr,
		"Rewrite legacy history in {targets} database(s) after writing verified backups? [y/N] "
	)?;
	stderr.flush()?;
	let mut answer = String::new();
	stdin.lock().read_line(&mut answer)?;
	Ok(matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes"))
}

fn backup_destination(db: &Path, backup: &Path, many: bool) -> PathBuf {
	if !many {
		return backup.to_path_buf();
	}
	let project = db
		.parent()
		.and_then(Path::file_name)
		.map_or_else(|| "sessions".to_string(), |name| name.to_string_lossy().into_owned());
	backup.join(format!("{project}.sessions.db"))
}

/// Write a consistent snapshot of `database` to `destination` without writing
/// the source. The caller holds the database writer transaction open, so the
/// snapshot and the subsequent redaction cover the same committed state.
fn backup_snapshot(database: &Path, destination: &Path) -> Result<()> {
	if destination.exists() {
		return Err(crate::Error::new(format!(
			"backup {} already exists; refusing to overwrite",
			safe_path(destination)
		)));
	}
	let parent = destination
		.parent()
		.filter(|parent| !parent.as_os_str().is_empty())
		.ok_or_else(|| crate::Error::new("backup path has no parent directory"))?;
	fs::create_dir_all(parent)?;
	let file_name = destination
		.file_name()
		.ok_or_else(|| crate::Error::new("backup path has no file name"))?;
	let staging =
		parent.join(format!(".arag-doctor-backup-{}-{}", std::process::id(), now_ms()));
	#[cfg(unix)]
	let builder = {
		use std::os::unix::fs::DirBuilderExt as _;
		let mut builder = fs::DirBuilder::new();
		builder.mode(0o700);
		builder
	};
	#[cfg(not(unix))]
	let builder = fs::DirBuilder::new();
	builder.create(&staging)?;
	let temp = staging.join(file_name);
	let result = (|| -> Result<()> {
		let store = SqliteStore::open_read_only(database).map_err(crate_error)?;
		// `VACUUM INTO` reads one committed snapshot from a separate read-only
		// connection; the source writer transaction is never modified. The
		// staging directory is 0700, so the file created under the process umask
		// is never reachable by another local user.
		store.vacuum_into(&temp).map_err(crate_error)?;
		drop(store);
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt as _;
			fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))?;
		}
		fs::File::open(&temp)?.sync_all()?;
		let check = Connection::open_with_flags(&temp, OpenFlags::SQLITE_OPEN_READ_ONLY)
			.map_err(crate_error)?;
		let integrity: String = check
			.query_row("PRAGMA integrity_check", [], |row| row.get(0))
			.map_err(crate_error)?;
		if integrity != "ok" {
			return Err(crate::Error::new(format!(
				"backup verification failed for {}",
				safe_path(&temp)
			)));
		}
		let schema: Option<i64> = check
			.query_row("SELECT version FROM schema_version WHERE id = 1", [], |row| row.get(0))
			.optional()
			.map_err(crate_error)?;
		if schema.is_none() {
			return Err(crate::Error::new("backup verification failed: no schema_version row"));
		}
		drop(check);
		// The file is already 0600; rename preserves that mode at the destination.
		fs::rename(&temp, destination)?;
		fs::remove_dir(&staging)?;
		#[cfg(unix)]
		if let Some(parent) = destination.parent() {
			let _ = fs::File::open(parent).and_then(|dir| dir.sync_all());
		}
		Ok(())
	})();
	if result.is_err() {
		let _ = fs::remove_file(&temp);
		let _ = fs::remove_dir(&staging);
	}
	result
}

// ── artifacts ───────────────────────────────────────────────────────────────

#[derive(Debug)]
struct UnreferencedArtifact {
	path:  PathBuf,
	bytes: u64,
	old:   bool,
}

#[derive(Debug, Default)]
struct ArtifactState {
	scan_incomplete: bool,
	referenced:   BTreeSet<PathBuf>,
	missing:      Vec<String>,
	unreferenced: Vec<UnreferencedArtifact>,
	total_bytes:  u64,
	total_files:  u64,
	old_files:    u64,
}

fn check_artifacts(
	report: &mut Report,
	sessions_root: &Path,
	blobs_root: &Path,
	session_dbs: &[PathBuf],
	options: &DoctorOptions,
) {
	let mut state = inspect_artifacts(sessions_root, blobs_root, session_dbs);

	if options.prune_artifacts {
		if state.scan_incomplete {
			report.warn(
				"artifacts",
				"prune-skipped",
				"artifact scan is incomplete (a session database or session directory could not be \
				 read); no artifact was deleted"
					.to_string(),
			);
		} else {
			let mut removed = 0usize;
			let mut removed_bytes = 0u64;
			for artifact in &state.unreferenced {
				if !artifact.old {
					continue;
				}
				match fs::remove_file(&artifact.path) {
					Ok(()) => {
						removed += 1;
						removed_bytes += artifact.bytes;
					},
					Err(error) => report.warn(
						"artifacts",
						"prune-failed",
						format!("{}: {error}", safe_path(&artifact.path)),
					),
				}
			}
			if removed > 0 {
				report.info(
					"artifacts",
					"pruned",
					format!("pruned {removed} orphaned artifact file(s), {removed_bytes} byte(s)"),
				);
				state = inspect_artifacts(sessions_root, blobs_root, session_dbs);
			}
		}
	}

	if state.total_files > 0 {
		report.info(
			"artifacts",
			"size",
			format!(
				"{} artifact file(s) use {} byte(s); {} older than {ARTIFACT_TTL_DAYS} days",
				state.total_files, state.total_bytes, state.old_files
			),
		);
	}
	if !state.missing.is_empty() {
		report.warn(
			"artifacts",
			"artifact-missing",
			format!(
				"{} artifact metadata row(s) point at missing files; doctor never relinks by \
				 filename alone, and the artifacts schema records no content digest",
				state.missing.len()
			),
		);
	}
	if !state.unreferenced.is_empty() {
		let old = state.unreferenced.iter().filter(|artifact| artifact.old).count();
		let message = if state.scan_incomplete {
			format!(
				"{} artifact file(s) have no metadata row in the databases that could be scanned; \
				 the scan is incomplete, so `--prune-artifacts` is disabled",
				state.unreferenced.len()
			)
		} else {
			format!(
				"{} artifact file(s) have no metadata row ({old} orphan(s) older than \
				 {ARTIFACT_TTL_DAYS} days are `--prune-artifacts` candidates)",
				state.unreferenced.len()
			)
		};
		report.warn("artifacts", "artifact-unreferenced", message);
	}
}

fn inspect_artifacts(
	sessions_root: &Path,
	blobs_root: &Path,
	session_dbs: &[PathBuf],
) -> ArtifactState {
	let mut state = ArtifactState::default();
	let mut session_ids = BTreeSet::new();

	for db in session_dbs {
		let Ok(store) = SqliteStore::open_read_only(db) else {
			state.scan_incomplete = true;
			continue;
		};
		let connection = store.connection();
		let ids = match session_ids_from_db(connection) {
			Ok(ids) => ids,
			Err(_) => {
				state.scan_incomplete = true;
				continue;
			},
		};
		session_ids.extend(ids);
		if !table_exists(connection, "artifacts") {
			state.scan_incomplete = true;
			continue;
		}
		let cwds = match session_cwds(connection) {
			Ok(cwds) => cwds,
			Err(_) => {
				state.scan_incomplete = true;
				continue;
			},
		};
		let Ok(mut statement) =
			connection.prepare("SELECT session_id, location FROM artifacts")
		else {
			state.scan_incomplete = true;
			continue;
		};
		let Ok(mut rows) = statement.query([]) else {
			state.scan_incomplete = true;
			continue;
		};
		loop {
			let row = match rows.next() {
				Ok(Some(row)) => row,
				Ok(None) => break,
				Err(_) => {
					state.scan_incomplete = true;
					break;
				},
			};
			let (Ok(session_id), Ok(location)) =
				(row.get::<_, String>(0), row.get::<_, String>(1))
			else {
				state.scan_incomplete = true;
				continue;
			};
			let Some(resolved) = resolve_artifact_path(&location, &session_id, &cwds) else {
				if !location.contains("://") {
					// A local relative path we cannot anchor to a session cwd may
					// point anywhere, including at a blob we must not prune.
					state.scan_incomplete = true;
				}
				continue;
			};
			let Some(resolved) = normalize_artifact_path(&resolved, &mut state) else {
				continue;
			};
			state.referenced.insert(resolved.clone());
			if !resolved.exists() {
				state.missing.push(location);
			}
		}
	}

	if sessions_root.is_dir() {
		match fs::read_dir(sessions_root) {
			Ok(projects) => {
				for project in projects.flatten() {
					let project_path = project.path();
					if !project_path.is_dir() {
						continue;
					}
					let Ok(entries) = fs::read_dir(&project_path) else {
						state.scan_incomplete = true;
						continue;
					};
					for entry in entries.flatten() {
						let path = entry.path();
						if !path.is_dir() {
							continue;
						}
						let Some(name) =
							path.file_name().map(|name| name.to_string_lossy().into_owned())
						else {
							continue;
						};
						let legacy = project_path.join(format!("{name}.jsonl")).is_file();
						let tracked = session_ids.contains(&name);
						if !legacy && !tracked {
							continue;
						}
						walk_artifact_dir(&path, 0, legacy, &mut state);
					}
				}
			},
			Err(_) => state.scan_incomplete = true,
		}
	}

	if blobs_root.is_dir() {
		walk_artifact_dir(blobs_root, 0, false, &mut state);
	}
	state
}

fn walk_artifact_dir(root: &Path, depth: usize, legacy: bool, state: &mut ArtifactState) {
	if depth > 16 {
		return;
	}
	let Ok(entries) = fs::read_dir(root) else {
		return;
	};
	for entry in entries.flatten() {
		let Ok(file_type) = entry.file_type() else {
			continue;
		};
		if file_type.is_symlink() {
			continue;
		}
		let path = entry.path();
		if file_type.is_dir() {
			walk_artifact_dir(&path, depth + 1, legacy, state);
			continue;
		}
		if !file_type.is_file() {
			continue;
		}
		let Ok(metadata) = entry.metadata() else {
			continue;
		};
		let Some(path) = normalize_artifact_path(&path, state) else {
			continue;
		};
		let bytes = metadata.len();
		let old = metadata
			.modified()
			.ok()
			.and_then(|modified| SystemTime::now().duration_since(modified).ok())
			.is_some_and(|age| age >= ARTIFACT_TTL);
		state.total_files += 1;
		state.total_bytes += bytes;
		if old {
			state.old_files += 1;
		}
		if !legacy && !state.referenced.contains(&path) {
			state.unreferenced.push(UnreferencedArtifact { path, bytes, old });
		}
	}
}

fn session_ids_from_db(connection: &Connection) -> rusqlite::Result<Vec<String>> {
	let mut statement = connection.prepare("SELECT id FROM sessions")?;
	let mut rows = statement.query([])?;
	let mut ids = Vec::new();
	while let Some(row) = rows.next()? {
		ids.push(row.get(0)?);
	}
	Ok(ids)
}

fn session_cwds(connection: &Connection) -> rusqlite::Result<HashMap<String, PathBuf>> {
	let mut cwds = HashMap::new();
	let mut statement = connection.prepare("SELECT id, payload_json FROM sessions")?;
	let mut rows = statement.query([])?;
	while let Some(row) = rows.next()? {
		let (Ok(id), Ok(payload)) = (row.get::<_, String>(0), row.get::<_, String>(1)) else {
			continue;
		};
		let Ok(value) = serde_json::from_str::<serde_json::Value>(&payload) else {
			continue;
		};
		if let Some(cwd) = value.get("cwd").and_then(serde_json::Value::as_str) {
			cwds.insert(id, PathBuf::from(cwd));
		}
	}
	Ok(cwds)
}

/// Canonicalize existing artifact paths (resolving `.`/`..` and symlink
/// aliases) so metadata and disk listings compare by target. Missing paths keep
/// a lexical normalization for missing-file reporting. A path that exists but
/// cannot be canonicalized makes the scan incomplete, which disables pruning.
fn normalize_artifact_path(path: &Path, state: &mut ArtifactState) -> Option<PathBuf> {
	if path.exists() {
		match fs::canonicalize(path) {
			Ok(canonical) => Some(canonical),
			Err(_) => {
				state.scan_incomplete = true;
				None
			},
		}
	} else {
		Some(lexical_normalize(path))
	}
}

fn lexical_normalize(path: &Path) -> PathBuf {
	use std::path::Component;
	let absolute = path.is_absolute();
	let mut out = PathBuf::new();
	for component in path.components() {
		match component {
			Component::CurDir => {},
			Component::ParentDir => {
				if !out.pop() && !absolute {
					out.push("..");
				}
			},
			Component::Prefix(prefix) => out.push(prefix.as_os_str()),
			Component::RootDir => out.push(component.as_os_str()),
			Component::Normal(part) => out.push(part),
		}
	}
	if absolute && out.as_os_str().is_empty() {
		out.push(Path::new("/"));
	}
	out
}

fn resolve_artifact_path(
	location: &str,
	session_id: &str,
	cwds: &HashMap<String, PathBuf>,
) -> Option<PathBuf> {
	let local = if let Some(path) = location.strip_prefix("file://") {
		path
	} else if location.contains("://") {
		return None;
	} else {
		location
	};
	let path = Path::new(local);
	if path.is_absolute() {
		Some(path.to_path_buf())
	} else {
		cwds.get(session_id).map(|cwd| cwd.join(path))
	}
}

// ── locks ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct LockFile {
	path: PathBuf,
	held: bool,
}

/// Match `local_cmd::open_gc_guard`: the GC protocol serializes stale recovery
/// on `gc.lock.guard` with a non-blocking exclusive `flock`. Doctor must hold
/// that same guard before touching `gc.lock`, or it could unlink a lock a live
/// GC just acquired. `None` means the guard is held by GC; `Err` means it
/// could not be opened safely (for example a symlink).
#[cfg(unix)]
fn acquire_gc_guard(gc_lock: &Path) -> std::io::Result<Option<fs::File>> {
	use std::os::unix::{fs::OpenOptionsExt as _, io::AsRawFd as _};

	let guard_path = PathBuf::from(format!("{}.guard", gc_lock.display()));
	let file = fs::OpenOptions::new()
		.read(true)
		.write(true)
		.create(true)
		.mode(0o600)
		.custom_flags(libc::O_NOFOLLOW)
		.open(&guard_path)?;
	// SAFETY: `flock` operates on an owned, open descriptor.
	if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
		return Ok(Some(file));
	}
	let error = std::io::Error::last_os_error();
	match error.raw_os_error() {
		Some(libc::EWOULDBLOCK) => Ok(None),
		_ => Err(error),
	}
}

#[cfg(not(unix))]
fn acquire_gc_guard(_gc_lock: &Path) -> std::io::Result<Option<fs::File>> {
	Ok(None)
}

fn check_gc_lock(report: &mut Report, agent_dir: &Path, fix: bool) {
	let gc_lock = agent_dir.join("gc.lock");
	if !gc_lock.exists() {
		return;
	}
	// Only a fix can unlink, so only a fix needs the guard. A reporting-only
	// pass must not create lock artifacts.
	let guard = if fix {
		match acquire_gc_guard(&gc_lock) {
			Ok(Some(guard)) => Some(guard),
			Ok(None) => {
				report.info(
					"locks",
					"gc-lock-guard-held",
					"gc.lock.guard is held by GC; gc.lock was not inspected or removed".to_string(),
				);
				return;
			},
			Err(error) => {
				report.warn(
					"locks",
					"gc-lock-guard-error",
					format!("{}: {error}", safe_path(&gc_lock)),
				);
				return;
			},
		}
	} else {
		None
	};
	// Re-read content and metadata after acquiring the guard. GC cannot replace
	// gc.lock until this guard is released.
	let parsed = match fs::read_to_string(&gc_lock) {
		Ok(text) => Ok(text.lines().next().and_then(|line| line.trim().parse::<i64>().ok())),
		Err(error) => Err(error),
	};
	if let Err(error) = &parsed {
		report.warn("locks", "gc-lock-unreadable", format!("{}: {error}", safe_path(&gc_lock)));
	}
	let old = fs::metadata(&gc_lock).ok().is_some_and(|metadata| is_old(&metadata));
	let dead = match &parsed {
		Ok(Some(pid)) => !process_exists(*pid),
		Ok(None) => true,
		Err(_) => false,
	};
	if dead && !old {
		report.info(
			"locks",
			"gc-lock-recent",
			format!("{} looks orphaned but is not old enough to remove", safe_path(&gc_lock)),
		);
	}
	let stale = dead && old;
	if stale {
		if let Some(_guard) = guard {
			match fs::remove_file(&gc_lock) {
				Ok(()) => report.info(
					"locks",
					"gc-lock-removed",
					format!("removed stale gc lock {}", safe_path(&gc_lock)),
				),
				Err(error) => report.warn(
					"locks",
					"gc-lock-remove-failed",
					format!("{}: {error}", safe_path(&gc_lock)),
				),
			}
		} else {
			report.warn(
				"locks",
				"gc-lock-stale",
				format!("stale gc lock {} (no live owner)", safe_path(&gc_lock)),
			);
		}
	} else if parsed.is_ok() {
		report.info("locks", "gc-lock-active", "gc.lock has a live owner".to_string());
	}
}

/// Legacy `~/.omp` migration status. Read-only: doctor never runs the copy.
fn check_migration_status(report: &mut Report) {
	let Some(home) = dirs::home_dir() else {
		return;
	};
	let legacy = home.join(dirs::LEGACY_CONFIG_DIR_NAME);
	if !legacy.exists() || dirs::explicit_storage_override() {
		return;
	}
	let marker = dirs::base_config_root().join(dirs::MIGRATION_MARKER);
	if marker.exists() {
		report.info(
			"migration",
			"legacy-root",
			format!(
				"legacy {} is retained read-only; {} records the completed copy",
				legacy.display(),
				marker.display()
			),
		);
	} else {
		report.info(
			"migration",
			"legacy-pending",
			format!(
				"legacy {} will be copied to {} on the next start unless PI_CONFIG_DIR or \
				 PI_CODING_AGENT_DIR is set",
				legacy.display(),
				dirs::base_config_root().display()
			),
		);
	}
}

fn check_locks(report: &mut Report, agent_dir: &Path, fix: bool) {
	check_gc_lock(report, agent_dir, fix);

	let locks = enumerate_lock_files(agent_dir);
	let held = locks.iter().filter(|lock| lock.held).count();
	let idle = locks.len().saturating_sub(held);
	for lock in &locks {
		report.info(
			"locks",
			"lock-file",
			format!(
				"{}: {}",
				safe_path(&lock.path),
				if lock.held { "held" } else { "idle" }
			),
		);
	}
	if !locks.is_empty() {
		report.info(
			"locks",
			"lock-files",
			format!(
				"{} advisory lock file(s): {held} held, {idle} idle; doctor never removes them",
				locks.len()
			),
		);
	}

	check_broker_leases(report, fix);
}

fn enumerate_lock_files(agent_dir: &Path) -> Vec<LockFile> {
	let mut roots = Vec::new();
	if let Ok(dir) = pi_agent::workspace_writer::default_lock_dir() {
		roots.push(dir);
	}
	if let Some(home) = dirs::home_dir() {
		roots.push(home.join(dirs::CONFIG_DIR_NAME).join("run").join("session-writer"));
	}
	let security = dirs::root_subdir("security", dirs::AgentDirCategory::State);
	roots.push(security);
	roots.extend(broker_runtime_roots());
	roots.push(agent_dir.join("run"));

	let mut seen = BTreeSet::new();
	let mut locks = Vec::new();
	for root in roots {
		walk_lock_files(&root, 0, &mut seen, &mut locks);
	}
	locks.sort_by(|a, b| a.path.cmp(&b.path));
	locks
}

fn walk_lock_files(
	root: &Path,
	depth: usize,
	seen: &mut BTreeSet<PathBuf>,
	locks: &mut Vec<LockFile>,
) {
	if depth > 4 {
		return;
	}
	let Ok(entries) = fs::read_dir(root) else {
		return;
	};
	for entry in entries.flatten() {
		let Ok(file_type) = entry.file_type() else {
			continue;
		};
		if file_type.is_symlink() {
			continue;
		}
		let path = entry.path();
		if file_type.is_dir() {
			walk_lock_files(&path, depth + 1, seen, locks);
			continue;
		}
		if !file_type.is_file() {
			continue;
		}
		let Some(name) = path.file_name().and_then(std::ffi::OsStr::to_str) else {
			continue;
		};
		let is_lock = name.ends_with(".lock") && name != "gc.lock";
		if !is_lock || !seen.insert(path.clone()) {
			continue;
		}
		let Some(held) = flock_probe(&path) else {
			continue;
		};
		locks.push(LockFile { path, held });
	}
}

#[cfg(unix)]
fn flock_probe(path: &Path) -> Option<bool> {
	use std::os::unix::io::AsRawFd as _;
	let file = fs::File::open(path).ok()?;
	// SAFETY: `flock` only inspects/takes a lock on a valid open descriptor.
	let status = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) };
	if status == 0 {
		// SAFETY: release the probe lock on the same valid descriptor.
		unsafe {
			libc::flock(file.as_raw_fd(), libc::LOCK_UN);
		}
		Some(false)
	} else {
		match std::io::Error::last_os_error().raw_os_error() {
			Some(libc::EWOULDBLOCK) => Some(true),
			_ => None,
		}
	}
}

#[cfg(not(unix))]
fn flock_probe(_path: &Path) -> Option<bool> {
	None
}

fn broker_runtime_roots() -> Vec<PathBuf> {
	let mut roots = Vec::new();
	roots.push(pi_agent::process_broker::protocol::runtime_root());
	if let Some(short) = pi_agent::process_broker::protocol::short_runtime_base() {
		roots.push(short);
	}
	roots
}

fn broker_scope_dirs() -> Vec<PathBuf> {
	let mut dirs = Vec::new();
	for root in broker_runtime_roots() {
		let Ok(entries) = fs::read_dir(&root) else {
			continue;
		};
		for entry in entries.flatten() {
			if entry.path().is_dir() {
				dirs.push(entry.path());
			}
		}
	}
	dirs.sort();
	dirs
}

/// Match `process_broker::broker::acquire_lease`: the daemon serializes
/// read/check/replace/remove of `broker.pid` on `broker.lease.lock` with an
/// exclusive `flock`. `None` means a live broker owns the guard; `Err` means
/// the guard could not be opened safely.
#[cfg(unix)]
fn acquire_broker_lease_lock(dir: &Path) -> std::io::Result<Option<fs::File>> {
	use std::os::unix::{fs::OpenOptionsExt as _, io::AsRawFd as _};

	let path = dir.join("broker.lease.lock");
	let file = fs::OpenOptions::new()
		.read(true)
		.write(true)
		.create(true)
		.mode(0o600)
		.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
		.open(&path)?;
	pi_agent::process_broker::protocol::verify_owner(&path)?;
	// SAFETY: `flock` operates on an owned, open descriptor.
	if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
		return Ok(Some(file));
	}
	let error = std::io::Error::last_os_error();
	match error.raw_os_error() {
		Some(libc::EWOULDBLOCK) => Ok(None),
		_ => Err(error),
	}
}

#[cfg(not(unix))]
fn acquire_broker_lease_lock(_dir: &Path) -> std::io::Result<Option<fs::File>> {
	Ok(None)
}

fn check_broker_leases(report: &mut Report, fix: bool) {
	let pid_file = pi_agent::process_broker::protocol::PID_FILE;
	let token_file = pi_agent::process_broker::protocol::TOKEN_FILE;
	let socket_file = pi_agent::process_broker::protocol::SOCKET_FILE;
	for dir in broker_scope_dirs() {
		let pid_path = dir.join(pid_file);
		if !pid_path.is_file() {
			continue;
		}
		// Hold the lease guard across the re-read and every unlink. A broker
		// that acquires/replaces the lease while doctor looks at a dead record
		// must never lose its fresh pid/token/socket files.
		let guard = if fix {
			match acquire_broker_lease_lock(&dir) {
				Ok(Some(guard)) => Some(guard),
				Ok(None) => {
					report.info(
						"locks",
						"broker-lease-lock-held",
						format!(
							"broker lease lock is held for {}; no lease file was removed",
							safe_path(&dir)
						),
					);
					continue;
				},
				Err(error) => {
					report.warn(
						"locks",
						"broker-lease-lock-error",
						format!("{}: {error}", safe_path(&dir)),
					);
					continue;
				},
			}
		} else {
			None
		};
		// Re-read only after acquiring the lease guard.
		let Ok(text) = pi_agent::process_broker::protocol::read_private_file(&pid_path) else {
			continue;
		};
		let Ok(lease) = serde_json::from_str::<pi_agent::process_broker::protocol::LeaseRecord>(
			&text,
		) else {
			report.warn(
				"locks",
				"broker-lease-malformed",
				format!("{}: unreadable broker lease", safe_path(&pid_path)),
			);
			continue;
		};
		if pi_agent::process_broker::protocol::process_alive(lease.pid, lease.start_time) {
			report.info(
				"locks",
				"broker-lease-active",
				format!("broker lease {} is live", safe_path(&dir)),
			);
			continue;
		}
		if let Some(_guard) = guard {
			let mut removed = 0usize;
			for path in [pid_path.clone(), dir.join(token_file), dir.join(socket_file)] {
				if fs::remove_file(&path).is_ok() {
					removed += 1;
				}
			}
			report.info(
				"locks",
				"broker-lease-removed",
				format!("removed {removed} stale broker lease file(s) from {}", safe_path(&dir)),
			);
		} else {
			report.warn(
				"locks",
				"broker-lease-stale",
				format!("stale broker lease {} (pid is dead)", safe_path(&dir)),
			);
		}
	}
}

#[cfg(unix)]
fn process_exists(pid: i64) -> bool {
	if pid <= 0 || pid > i64::from(i32::MAX) {
		return false;
	}
	// SAFETY: `kill(pid, 0)` performs only an existence/permission check.
	let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
	if result == 0 {
		return true;
	}
	let error = std::io::Error::last_os_error();
	!matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::EINVAL))
}

/// Without `kill(pid, 0)` the liveness answer is unknown; treat the lock as
/// live so doctor never recommends breaking a lock it cannot inspect.
#[cfg(not(unix))]
fn process_exists(_pid: i64) -> bool {
	true
}

// ── permissions ─────────────────────────────────────────────────────────────

fn check_permissions(
	report: &mut Report,
	config_root: &Path,
	agent_dir: &Path,
	sessions_root: &Path,
	blobs_root: &Path,
	session_dbs: &[PathBuf],
	fix: bool,
) {
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt as _;

		let cache_root = dirs::agent_subdir("android-probes", dirs::AgentDirCategory::Cache);
		let security_root = dirs::root_subdir("security", dirs::AgentDirCategory::State);
		let mut dirs_to_check: Vec<PathBuf> = vec![
			config_root.to_path_buf(),
			agent_dir.to_path_buf(),
			sessions_root.to_path_buf(),
			blobs_root.to_path_buf(),
			cache_root,
			security_root,
		];
		if let Ok(dir) = pi_agent::workspace_writer::default_lock_dir() {
			dirs_to_check.push(dir);
		}
		if let Some(home) = dirs::home_dir() {
			dirs_to_check
				.push(home.join(dirs::CONFIG_DIR_NAME).join("run").join("session-writer"));
		}
		for dir in broker_runtime_roots() {
			dirs_to_check.push(dir);
		}

		let mut files_to_check: Vec<PathBuf> = vec![
			agent_dir.join("agent.db"),
			config_root.join("config.yml"),
			agent_dir.join("config.yml"),
			agent_dir.join("trust.json"),
			agent_dir.join("memory.db"),
			agent_dir.join("gc.lock"),
			dirs::agent_subdir("history.db", dirs::AgentDirCategory::Data),
			dirs::agent_subdir("models.db", dirs::AgentDirCategory::Data),
			dirs::agent_subdir("diagnostics", dirs::AgentDirCategory::Data)
				.join("diagnostics.db"),
		];
		files_to_check.extend(session_dbs.iter().cloned());
		for lock in enumerate_lock_files(agent_dir) {
			files_to_check.push(lock.path);
		}
		for dir in broker_scope_dirs() {
			files_to_check.push(dir.join(pi_agent::process_broker::protocol::PID_FILE));
			files_to_check.push(dir.join(pi_agent::process_broker::protocol::TOKEN_FILE));
		}

		let mut fixed = 0usize;
		for (path, expected) in dirs_to_check
			.iter()
			.map(|path| (path.as_path(), 0o700u32))
			.chain(files_to_check.iter().map(|path| (path.as_path(), 0o600u32)))
		{
			let Ok(metadata) = fs::metadata(path) else {
				continue;
			};
			let mode = metadata.permissions().mode() & 0o777;
			if mode == expected {
				continue;
			}
			if fix {
				if fs::set_permissions(path, fs::Permissions::from_mode(expected)).is_ok() {
					fixed += 1;
					continue;
				}
			}
			report.warn(
				"permissions",
				"mode",
				format!("{} has mode {mode:04o}; expected {expected:04o}", safe_path(path)),
			);
		}
		if fixed > 0 {
			report.info(
				"permissions",
				"fixed",
				format!("set private permissions on {fixed} path(s)"),
			);
		}
	}
	#[cfg(not(unix))]
	{
		let _ = (report, config_root, agent_dir, sessions_root, blobs_root, session_dbs, fix);
	}
}

// ── environment ─────────────────────────────────────────────────────────────

fn check_env(
	report: &mut Report,
	config: Option<&Config>,
	agent_dir: &Path,
	sessions_root: &Path,
	blobs_root: &Path,
) {
	report.info("env", "agent-dir", safe_path(agent_dir));
	report.info("env", "sessions-dir", safe_path(sessions_root));
	report.info("env", "blobs-dir", safe_path(blobs_root));
	let Some(config) = config else {
		return;
	};
	match config.default_model.as_deref() {
		Some(model) => report.info("env", "default-model", format!("default model: {model}")),
		None => report.info(
			"env",
			"default-model",
			"no default model configured; runs need --model or modelRoles.default".to_string(),
		),
	}
	for (id, provider) in &config.providers {
		if let Some(base_url) = provider.base_url.as_deref() {
			if !(base_url.starts_with("http://") || base_url.starts_with("https://")) {
				report.warn(
					"env",
					"provider-endpoint",
					format!("provider {id} has a base URL that is not http(s)"),
				);
			}
			if endpoint_has_credentials(base_url) {
				report.warn(
					"env",
					"provider-endpoint-credentials",
					format!("provider {id} base URL embeds credentials (value hidden)"),
				);
			}
		}
		match provider.api_key.as_deref() {
			Some(key) if is_env_var_name(key) && std::env::var_os(key).is_none() => {
				report.warn(
					"env",
					"provider-env-missing",
					format!("provider {id} names API key env var {key}, which is not set"),
				);
			},
			Some(key) if !is_env_var_name(key) => report.info(
				"env",
				"provider-key",
				format!("provider {id} carries a literal API key (value hidden)"),
			),
			Some(_) => report.info(
				"env",
				"provider-key",
				format!("provider {id} resolves its API key from the environment"),
			),
			None => report.info(
				"env",
				"provider-key",
				format!("provider {id} has no apiKey configured"),
			),
		}
	}
	if config.searxng.endpoint.is_some() {
		report.info(
			"env",
			"searxng",
			format!(
				"SearXNG endpoint configured; token {}",
				if config.searxng.token.is_some() { "present" } else { "absent" }
			),
		);
	}
}

fn is_env_var_name(value: &str) -> bool {
	value.len() <= 64
		&& value
			.bytes()
			.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn endpoint_has_credentials(url: &str) -> bool {
	if url.contains("access_token=") || url.contains("api_key=") || url.contains("token=") {
		return true;
	}
	let Some((_, rest)) = url.split_once("://") else {
		return false;
	};
	rest.split('/').next().is_some_and(|authority| authority.contains('@'))
}

// ── platform probes ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ProbeKey {
	kernel:  String,
	sdk:     String,
	selinux: String,
	version: String,
}

impl ProbeKey {
	fn current() -> Self {
		Self {
			kernel:  kernel_release(),
			sdk:     android_sdk(),
			selinux: selinux_context_hash(),
			version: env!("CARGO_PKG_VERSION").to_string(),
		}
	}

	fn hash(&self) -> String {
		digest_hex(
			format!("{}\n{}\n{}\n{}", self.kernel, self.sdk, self.selinux, self.version).as_bytes(),
		)
	}
}

#[derive(Debug, Serialize, Deserialize)]
struct ProbeCache {
	key:        ProbeKey,
	probes:     BTreeMap<String, String>,
	checked_at: u64,
}

fn check_platform(report: &mut Report, refresh: bool) {
	if !cfg!(any(target_os = "linux", target_os = "android")) {
		report.info(
			"platform",
			"unsupported",
			"syscall probes are only run on Linux/Android".to_string(),
		);
		return;
	}
	let key = ProbeKey::current();
	let cache_root = dirs::agent_subdir("android-probes", dirs::AgentDirCategory::Cache);
	if refresh {
		match fs::remove_dir_all(&cache_root) {
			Ok(()) => report.info(
				"platform",
				"cache-reset",
				format!("reset Android probe cache {}", safe_path(&cache_root)),
			),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
			Err(error) => report.info(
				"platform",
				"cache-reset-failed",
				format!("{}: {error}", safe_path(&cache_root)),
			),
		}
	}
	let cache_path = cache_root.join(format!("{}.json", key.hash()));
	if !refresh
		&& let Some(cache) = read_probe_cache(&cache_path)
		&& cache.key == key
	{
		report.info(
			"platform",
			"cache",
			format!("using cached probes from {}", safe_path(&cache_path)),
		);
		for (name, status) in &cache.probes {
			report.info("platform", "probe", format!("{name}: {status}"));
		}
		return;
	}

	let mut probes = BTreeMap::new();
	for name in PROBE_NAMES {
		let status = run_probe_child(name);
		report.info("platform", "probe", format!("{name}: {status}"));
		probes.insert(name.to_string(), status.to_string());
	}
	if refresh {
		// Only explicit maintenance (`--fix` / `--reset-probes`) may create or
		// rewrite the probe cache; a default doctor run is strictly read-only.
		let cache = ProbeCache { key, probes, checked_at: now_ms() };
		if let Err(error) = write_probe_cache(&cache_root, &cache_path, &cache) {
			report.info(
				"platform",
				"cache-write-failed",
				format!("{}: {error}", safe_path(&cache_path)),
			);
		}
	}
}

fn read_probe_cache(path: &Path) -> Option<ProbeCache> {
	let text = fs::read_to_string(path).ok()?;
	serde_json::from_str(&text).ok()
}

fn write_probe_cache(root: &Path, path: &Path, cache: &ProbeCache) -> std::io::Result<()> {
	fs::create_dir_all(root)?;
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt as _;
		let _ = fs::set_permissions(root, fs::Permissions::from_mode(0o700));
	}
	let text = serde_json::to_vec_pretty(cache)
		.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
	let temp = path.with_extension(format!("tmp-{}", std::process::id()));
	{
		let mut options = fs::OpenOptions::new();
		options.write(true).create(true).truncate(true);
		#[cfg(unix)]
		{
			use std::os::unix::fs::OpenOptionsExt as _;
			options.mode(0o600);
		}
		let mut file = options.open(&temp)?;
		file.write_all(&text)?;
		file.sync_all()?;
	}
	fs::rename(&temp, path)?;
	Ok(())
}

fn run_probe_child(name: &str) -> ProbeStatus {
	let Ok(exe) = std::env::current_exe() else {
		return ProbeStatus::Error;
	};
	let mut child = match Command::new(exe)
		.arg("__probe")
		.arg(name)
		.stdin(Stdio::null())
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.spawn()
	{
		Ok(child) => child,
		Err(_) => return ProbeStatus::Error,
	};
	let deadline = std::time::Instant::now() + PROBE_TIMEOUT;
	loop {
		match child.try_wait() {
			Ok(Some(status)) => {
				#[cfg(unix)]
				{
					if let Some(signal) = status.signal()
						&& signal == libc::SIGSYS
					{
						return ProbeStatus::Sigsys;
					}
				}
				return match status.code() {
					Some(0) => ProbeStatus::Ok,
					Some(10) => ProbeStatus::Enosys,
					Some(11) => ProbeStatus::Eperm,
					_ => ProbeStatus::Error,
				};
			},
			Ok(None) if std::time::Instant::now() < deadline => {
				std::thread::sleep(Duration::from_millis(10));
			},
			Ok(None) => {
				let _ = child.kill();
				let _ = child.wait();
				return ProbeStatus::Error;
			},
			Err(_) => return ProbeStatus::Error,
		}
	}
}

#[derive(Debug, Clone, Copy)]
enum ProbeStatus {
	Ok,
	Enosys,
	Eperm,
	Sigsys,
	Error,
}

impl ProbeStatus {
	const fn as_str(self) -> &'static str {
		match self {
			Self::Ok => "ok",
			Self::Enosys => "enosys",
			Self::Eperm => "eperm",
			Self::Sigsys => "sigsys",
			Self::Error => "error",
		}
	}
}

impl std::fmt::Display for ProbeStatus {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str(self.as_str())
	}
}

fn kernel_release() -> String {
	fs::read_to_string("/proc/sys/kernel/osrelease")
		.map(|text| text.trim().to_string())
		.unwrap_or_default()
}

fn android_sdk() -> String {
	#[cfg(target_os = "android")]
	{
		Command::new("getprop")
			.arg("ro.build.version.sdk")
			.output()
			.ok()
			.and_then(|output| String::from_utf8(output.stdout).ok())
			.map(|text| text.trim().to_string())
			.unwrap_or_default()
	}
	#[cfg(not(target_os = "android"))]
	{
		String::new()
	}
}

fn selinux_context_hash() -> String {
	fs::read_to_string("/proc/self/attr/current")
		.map(|context| digest_hex(context.trim().as_bytes()))
		.unwrap_or_default()
}

// ── hidden syscall probe child ──────────────────────────────────────────────

/// Successful probe: the syscall returned normally.
pub const PROBE_OK: u8 = 0;
/// Kernel reports the syscall unavailable (`ENOSYS` / `EOPNOTSUPP`).
pub const PROBE_ENOSYS: u8 = 10;
/// A sandbox or permission layer refused the syscall (`EPERM` / `EACCES`).
pub const PROBE_EPERM: u8 = 11;
/// Any other errno; the syscall exists but the probe did not complete.
pub const PROBE_OTHER: u8 = 12;
/// Unknown probe name; nothing was attempted.
pub const PROBE_UNKNOWN: u8 = 13;

/// Run one hidden `__probe <syscall>` capability attempt in this process.
///
/// The caller isolates each probe in its own short-lived child so one `SIGSYS`
/// cannot abort the rest. Nothing is printed; the exit code carries the result.
#[must_use]
pub fn run_internal_probe(syscall: &str) -> u8 {
	#[cfg(any(target_os = "linux", target_os = "android"))]
	{
		probe_syscall(syscall)
	}
	#[cfg(not(any(target_os = "linux", target_os = "android")))]
	{
		let _ = syscall;
		PROBE_ENOSYS
	}
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn probe_syscall(name: &str) -> u8 {
	use std::os::fd::FromRawFd as _;

	if matches!(name, "seccomp" | "seccomp_unotify" | "landlock_create_ruleset") {
		// Unprivileged seccomp/landlock setup needs no_new_privs; set it only in
		// this short-lived probe child before installing the filter/ruleset.
		// SAFETY: prctl(PR_SET_NO_NEW_PRIVS) affects only this probe process.
		unsafe {
			libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
		}
	}

	let (result, returns_fd) = match name {
		"pidfd_open" => (
			// SAFETY: pidfd_open only opens a descriptor for this process's pid.
			unsafe {
				libc::syscall(
					libc::SYS_pidfd_open,
					libc::getpid() as libc::c_long,
					0 as libc::c_long,
				)
			},
			true,
		),
		"landlock_create_ruleset" => (
			// SAFETY: the VERSION query with a null attribute never creates a ruleset.
			unsafe {
				libc::syscall(
					libc::SYS_landlock_create_ruleset,
					std::ptr::null::<libc::c_void>(),
					0 as libc::c_long,
					1 as libc::c_long,
				)
			},
			true,
		),
		"memfd_create" | "memfd_create_execveat" => (
			// SAFETY: memfd_create only allocates an anonymous in-memory file.
			unsafe {
				libc::syscall(
					libc::SYS_memfd_create,
					c"arag-probe".as_ptr(),
					0 as libc::c_long,
				)
			},
			true,
		),
		"unshare_user" => (
			// SAFETY: unshare runs only in this probe child, which exits right after.
			unsafe { libc::syscall(libc::SYS_unshare, libc::CLONE_NEWUSER as libc::c_long) },
			false,
		),
		"seccomp" | "seccomp_unotify" => (probe_seccomp_unotify(), true),
		_ => return PROBE_UNKNOWN,
	};

	if result >= 0 {
		if returns_fd {
			// SAFETY: the kernel returned a fresh descriptor owned by this process.
			unsafe {
				drop(std::fs::File::from_raw_fd(result as i32));
			}
		}
		return PROBE_OK;
	}
	match std::io::Error::last_os_error().raw_os_error() {
		Some(code) if code == libc::ENOSYS || code == libc::EOPNOTSUPP => PROBE_ENOSYS,
		Some(code) if code == libc::EPERM || code == libc::EACCES => PROBE_EPERM,
		_ => PROBE_OTHER,
	}
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn probe_seccomp_unotify() -> libc::c_long {
	const BPF_RET: u16 = 0x06;
	const BPF_K: u16 = 0x00;
	let filter = libc::sock_filter {
		code: BPF_RET | BPF_K,
		jt:   0,
		jf:   0,
		k:    libc::SECCOMP_RET_ALLOW,
	};
	let mut program =
		libc::sock_fprog { len: 1, filter: std::ptr::addr_of!(filter).cast_mut() };
	// SAFETY: a one-instruction ALLOW filter is installed only in this probe
	// child, and the returned listener descriptor is closed by the caller.
	unsafe {
		libc::syscall(
			libc::SYS_seccomp,
			libc::SECCOMP_SET_MODE_FILTER as libc::c_long,
			libc::SECCOMP_FILTER_FLAG_NEW_LISTENER as libc::c_long,
			std::ptr::addr_of_mut!(program),
		)
	}
}

// ── shared SQL helpers ──────────────────────────────────────────────────────

fn count_statement_rows(connection: &Connection, sql: &str) -> rusqlite::Result<usize> {
	let mut statement = connection.prepare(sql)?;
	let mut rows = statement.query([])?;
	let mut count = 0usize;
	while rows.next()?.is_some() {
		count += 1;
	}
	Ok(count)
}

fn count_optional(connection: &Connection, sql: &str) -> Option<i64> {
	connection.query_row(sql, [], |row| row.get(0)).ok()
}

fn table_exists(connection: &Connection, table: &str) -> bool {
	connection
		.query_row(
			"SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
			params![table],
			|row| row.get::<_, bool>(0),
		)
		.unwrap_or(false)
}

fn column_exists(connection: &Connection, table: &str, column: &str) -> bool {
	connection
		.query_row(
			"SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name = ?2)",
			params![table, column],
			|row| row.get::<_, bool>(0),
		)
		.unwrap_or(false)
}

fn orphan_check(
	report: &mut Report,
	connection: &Connection,
	label: &str,
	code: &'static str,
	level: Level,
	sql: &str,
	what: &str,
) {
	match connection.query_row(sql, [], |row| row.get::<_, i64>(0)) {
		Ok(0) => {},
		Ok(count) => report.push("db", level, code, format!("{label}: {count} {what}")),
		Err(error) => report.warn(
			"db",
			"db-query",
			format!("{label}: {code} check failed: {error}"),
		),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn level_ordering_treats_warn_and_error_as_findings() {
		assert!(!Level::Info.is_finding());
		assert!(Level::Warn.is_finding());
		assert!(Level::Error.is_finding());
		assert_eq!(Level::Info.tag(), "info");
	}

	#[test]
	fn endpoint_credentials_are_detected_without_echoing_values() {
		assert!(endpoint_has_credentials("https://user:pass@example.test/v1"));
		assert!(endpoint_has_credentials("https://example.test/v1?token=abc"));
		assert!(!endpoint_has_credentials("https://example.test/v1"));
	}

	#[test]
	fn data_modification_requires_confirmation() {
		let prune = DoctorOptions { prune_artifacts: true, ..DoctorOptions::default() };
		assert!(validate_options(&prune).is_err());
		assert!(validate_options(&DoctorOptions { yes: true, ..prune.clone() }).is_ok());
		let narrowed = DoctorOptions {
			prune_artifacts: true,
			session_dir:     Some(std::path::PathBuf::from("/tmp/sessions-subset")),
			yes:             true,
			..DoctorOptions::default()
		};
		assert!(validate_options(&narrowed).is_err());

		let redact = DoctorOptions { redact_legacy: true, ..DoctorOptions::default() };
		assert!(validate_options(&redact).is_err());
		let with_backup = DoctorOptions {
			backup: Some(std::path::PathBuf::from("/tmp/backup.db")),
			..redact.clone()
		};
		// Interactive confirmation is resolved at run time; the backup is the
		// only flag-level requirement.
		assert!(validate_options(&with_backup).is_ok());

		let backup_only = DoctorOptions {
			backup: Some(std::path::PathBuf::from("/tmp/backup.db")),
			..DoctorOptions::default()
		};
		assert!(validate_options(&backup_only).is_err());
	}

	#[test]
	fn lexical_normalization_removes_dot_segments() {
		assert_eq!(
			lexical_normalize(Path::new("/work/blobs/../blobs/file.bin")),
			PathBuf::from("/work/blobs/file.bin")
		);
		assert_eq!(lexical_normalize(Path::new("a/./b/../c")), PathBuf::from("a/c"));
		assert_eq!(lexical_normalize(Path::new("../a")), PathBuf::from("../a"));
	}

	#[test]
	fn artifact_paths_resolve_only_local_locations() {
		let mut cwds = HashMap::new();
		cwds.insert("session-1".to_string(), PathBuf::from("/work"));
		assert_eq!(
			resolve_artifact_path("file:///work/a.bin", "session-1", &cwds),
			Some(PathBuf::from("/work/a.bin"))
		);
		assert_eq!(
			resolve_artifact_path("sub/a.bin", "session-1", &cwds),
			Some(PathBuf::from("/work/sub/a.bin"))
		);
		assert_eq!(resolve_artifact_path("https://example.test/a.bin", "session-1", &cwds), None);
		assert_eq!(resolve_artifact_path("sub/a.bin", "unknown", &cwds), None);
	}

	#[test]
	fn redaction_scan_reports_table_counts_only() {
		use rusqlite::params;
		let connection = Connection::open_in_memory().expect("in-memory database");
		connection
			.execute_batch(
				"CREATE TABLE messages (seq INTEGER PRIMARY KEY, payload_json TEXT);\
				 CREATE TABLE sessions (id TEXT PRIMARY KEY, payload_json TEXT);",
			)
			.expect("create redaction fixture tables");
		connection
			.execute(
				"INSERT INTO messages (payload_json) VALUES (?1)",
				params![serde_json::json!({"text": "sk-proj-abcdefghij1234567890"}).to_string()],
			)
			.expect("insert secret payload");
		let counts = scan_redaction_tables(&connection).expect("scan tables");
		assert_eq!(
			counts.iter().find(|(table, _)| *table == "messages").map(|(_, count)| *count),
			Some(1)
		);
		assert!(counts.iter().all(|(table, _)| *table == "messages" || *table == "sessions"));
	}

	#[test]
	fn probe_key_stable_for_identical_inputs() {
		let key = ProbeKey {
			kernel:  "6.1".to_string(),
			sdk:     "34".to_string(),
			selinux: "abc".to_string(),
			version: "0.1.0".to_string(),
		};
		assert_eq!(key.hash(), key.clone().hash());
	}
}
