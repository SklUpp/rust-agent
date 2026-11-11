//! Behavioral checks for the local diagnostics SQLite trace: enabled rows,
//! disabled no-write behavior, nullable unknown usage, contained failures, no
//! raw identifiers/content, and concurrent writers.

use std::{
	fs,
	path::{Path, PathBuf},
	sync::Arc,
	thread,
	time::{SystemTime, UNIX_EPOCH},
};

use aragent::{
	config::Config,
	diagnostics::{
		self, Diagnostics, SpanKind, SpanStatus, SubagentRecorder, TokenRecord, tool_kind,
	},
};
use pi_agent::{
	AgentEvent, Message, TokenUsage, TurnStatus, UsagePresence,
	supervisor::{SubagentEvent, SubagentLifecycle, SubagentLifecycleStatus},
};
use rusqlite::Connection;

struct TestDir(PathBuf);

impl TestDir {
	fn new(tag: &str) -> Self {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("clock")
			.as_nanos();
		let dir = std::env::temp_dir().join(format!(
			"arag-diagnostics-{}-{nanos}-{tag}",
			std::process::id()
		));
		fs::create_dir_all(&dir).expect("temp dir");
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt;
			fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).expect("private dir");
		}
		Self(dir)
	}

	fn db(&self) -> PathBuf {
		self.0.join("diagnostics.db")
	}
}

impl Drop for TestDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}

#[derive(Debug)]
struct Row {
	span_id:  String,
	parent:   Option<String>,
	kind:     String,
	status:   String,
	duration: Option<i64>,
	input:    Option<i64>,
	output:   Option<i64>,
	read:     Option<i64>,
	write:    Option<i64>,
	reason:   Option<i64>,
}

fn fetch_rows(path: &Path) -> Vec<Row> {
	let connection = Connection::open(path).expect("open diagnostics");
	let mut statement = connection
		.prepare(
			"SELECT span_id, parent_span_id, kind, status, duration_ms, input_tokens, \
			 output_tokens, cache_read_tokens, cache_write_tokens, reasoning_tokens \
			 FROM spans ORDER BY seq",
		)
		.expect("prepare");
	statement
		.query_map([], |row| {
			Ok(Row {
				span_id: row.get(0)?,
				parent: row.get(1)?,
				kind: row.get(2)?,
				status: row.get(3)?,
				duration: row.get(4)?,
				input: row.get(5)?,
				output: row.get(6)?,
				read: row.get(7)?,
				write: row.get(8)?,
				reason: row.get(9)?,
			})
		})
		.expect("query")
		.collect::<Result<Vec<_>, _>>()
		.expect("rows")
}

fn lifecycle(
	id: &str,
	status: SubagentLifecycleStatus,
	duration_ms: Option<u64>,
) -> SubagentEvent {
	SubagentEvent::Lifecycle(SubagentLifecycle {
		id: id.to_string(),
		index: 0,
		agent: "raw-agent-name".to_string(),
		agent_source: "raw-agent-source".to_string(),
		task: "raw task text".to_string(),
		status,
		session_file: Some("/tmp/raw-session-file".to_string()),
		parent_tool_call_id: Some("raw-parent-call".to_string()),
		detached: false,
		error: Some("raw provider error".to_string()),
		abort_reason: None,
		duration_ms,
	})
}

fn finished_message(usage: Option<TokenUsage>) -> Message {
	let mut message = Message::assistant("raw assistant content");
	message.usage = usage;
	message
}

#[test]
fn enabled_trace_records_boundaries_and_nullable_tokens() {
	let dir = TestDir::new("enabled");
	let sink = Diagnostics::open(dir.db()).expect("open");

	let trace = sink.start_session("raw-session-identifier");
	let session_span = trace.span_id().to_string();
	let mut model = sink.begin_span(SpanKind::Model, Some(&session_span));
	model.finish(
		SpanStatus::Succeeded,
		TokenRecord {
			input:       Some(10),
			output:      Some(4),
			cache_read:  Some(2),
			cache_write: Some(1),
			reasoning:   None,
		},
	);
	let tool = sink.begin_span(tool_kind("bash"), Some(&session_span));
	tool.finish(SpanStatus::Failed, TokenRecord::default());
	let unknown = sink.begin_span(tool_kind("mcp_secret_server_tool"), Some(&session_span));
	unknown.finish(SpanStatus::Succeeded, TokenRecord::default());

	let recorder = SubagentRecorder::new(Arc::clone(&sink));
	recorder.observe(&lifecycle(
		"raw-child-id",
		SubagentLifecycleStatus::Started,
		None,
	));
	recorder.observe(&SubagentEvent::Event {
		id: "raw-child-id".to_string(),
		event: AgentEvent::MessageEnd {
			message: finished_message(Some(TokenUsage {
				input: 3,
				output: 2,
				cache_read: 0,
				cache_write: 0,
				total_tokens: 5,
				reasoning_tokens: Some(1),
				presence: UsagePresence {
					input: true,
					output: true,
					cache_read: true,
					cache_write: true,
					reasoning: true,
				},
				..TokenUsage::default()
			})),
			status: TurnStatus::EndTurn,
		},
	});
	recorder.observe(&SubagentEvent::Event {
		id: "raw-child-id".to_string(),
		event: AgentEvent::ToolStart {
			id: "raw-call-id".to_string(),
			name: "mcp_secret_server_tool".to_string(),
			arguments: serde_json::json!({ "secret": "hunter2" }),
		},
	});
	recorder.observe(&SubagentEvent::Event {
		id: "raw-child-id".to_string(),
		event: AgentEvent::ToolEnd {
			id: "raw-call-id".to_string(),
			name: "mcp_secret_server_tool".to_string(),
			result: "raw tool output".to_string(),
			is_error: false,
			images: Vec::new(),
			details: None,
		},
	});
	recorder.observe(&lifecycle(
		"raw-child-id",
		SubagentLifecycleStatus::Completed,
		Some(7),
	));

	drop(trace);
	sink.flush();

	let rows = fetch_rows(&dir.db());
	assert!(
		rows
			.iter()
			.any(|row| row.kind == "session" && row.status == "closed" && row.duration.is_some()),
		"session start/end: {rows:?}"
	);
	assert!(
		rows
			.iter()
			.any(|row| row.kind == "model"
				&& row.status == "succeeded"
				&& row.input == Some(10)
				&& row.output == Some(4)
				&& row.read == Some(2)
				&& row.write == Some(1)
				&& row.reason.is_none()),
		"root model tokens with unknown reasoning NULL: {rows:?}"
	);
	assert!(
		rows
			.iter()
			.any(|row| row.kind == "tool_builtin" && row.status == "failed"),
		"builtin tool boundary: {rows:?}"
	);
	assert!(
		rows
			.iter()
			.any(|row| row.kind == "tool_custom" && row.status == "succeeded"),
		"custom tool name collapsed to enum: {rows:?}"
	);
	assert!(
		rows
			.iter()
			.any(|row| row.kind == "subagent" && row.status == "succeeded" && row.duration == Some(7)),
		"subagent end with duration: {rows:?}"
	);
	assert!(
		rows
			.iter()
			.any(|row| row.kind == "model"
				&& row.input == Some(3)
				&& row.reason == Some(1)
				&& row.read == Some(0)
				&& row.write == Some(0)
				&& rows.iter().any(|parent| parent.kind == "subagent"
					&& parent.span_id == row.parent.clone().unwrap_or_default())),
		"child model tokens parented to subagent span: {rows:?}"
	);

	let serialized = rows
		.iter()
		.map(|row| format!("{row:?}"))
		.collect::<Vec<_>>()
		.join("\n");
	for forbidden in [
		"raw-session-identifier",
		"raw-child-id",
		"raw-call-id",
		"mcp_secret_server_tool",
		"hunter2",
		"raw assistant content",
		"raw tool output",
		"raw task text",
		"raw-agent-name",
		"raw-agent-source",
		"raw-parent-call",
		"/tmp/raw-session-file",
		"raw provider error",
	] {
		assert!(
			!serialized.contains(forbidden),
			"raw value `{forbidden}` leaked into diagnostics: {serialized}"
		);
	}
}

#[test]
fn disabled_setting_writes_nothing_and_keeps_existing_rows() {
	let dir = TestDir::new("disabled");
	let mut config = Config::default();
	assert!(config.diagnostics.enabled(), "diagnostics default on");

	// Seed one enabled row.
	let sink = diagnostics::open_for_config(&config, dir.db()).expect("enabled sink");
	let mut span = sink.begin_span(SpanKind::Model, None);
	span.finish(SpanStatus::Succeeded, TokenRecord::default());
	sink.flush();
	drop(sink);
	let before = fetch_rows(&dir.db()).len();
	assert_eq!(before, 2);

	config.diagnostics.enabled = Some(false);
	assert!(diagnostics::open_for_config(&config, dir.db()).is_none());
	assert!(dir.db().exists(), "disabled mode must not delete the database");
	let after = fetch_rows(&dir.db()).len();
	assert_eq!(before, after, "disabled mode must not append rows");
}

#[test]
fn absent_cache_counters_stay_null() {
	let usage = TokenUsage {
		input: 7,
		output: 3,
		cache_read: 0,
		cache_write: 0,
		total_tokens: 10,
		reasoning_tokens: None,
		presence: UsagePresence { input: true, output: true, ..UsagePresence::default() },
		..TokenUsage::default()
	};
	let record = TokenRecord::from_usage(Some(&usage));
	assert_eq!(record.input, Some(7));
	assert_eq!(record.output, Some(3));
	assert_eq!(record.cache_read, None, "absent cache read must not become 0");
	assert_eq!(record.cache_write, None, "absent cache write must not become 0");
	assert_eq!(record.reasoning, None);

	let empty = TokenRecord::from_usage(Some(&TokenUsage::default()));
	assert_eq!(empty.input, None);
	assert_eq!(empty.output, None);
	assert_eq!(empty.cache_read, None);
	assert_eq!(empty.cache_write, None);
	assert_eq!(empty.reasoning, None);

	let explicit_zero = TokenUsage {
		cache_read: 0,
		presence: UsagePresence { cache_read: true, ..UsagePresence::default() },
		..TokenUsage::default()
	};
	assert_eq!(
		TokenRecord::from_usage(Some(&explicit_zero)).cache_read,
		Some(0),
		"explicit provider zero must stay 0, not NULL"
	);

	let zero_reasoning = TokenUsage {
		presence: UsagePresence { reasoning: true, ..UsagePresence::default() },
		..TokenUsage::default()
	};
	assert_eq!(
		TokenRecord::from_usage(Some(&zero_reasoning)).reasoning,
		Some(0),
		"explicit zero reasoning is known even when domain stores None"
	);
}

#[test]
fn unknown_provider_usage_is_null_not_zero() {
	let dir = TestDir::new("unknown");
	let sink = Diagnostics::open(dir.db()).expect("open");
	let mut span = sink.begin_span(SpanKind::Model, None);
	span.finish(SpanStatus::Succeeded, TokenRecord::default());
	sink.flush();
	let rows = fetch_rows(&dir.db());
	let end = rows
		.iter()
		.find(|row| row.status == "succeeded")
		.expect("end row");
	assert!(end.input.is_none());
	assert!(end.output.is_none());
	assert!(end.read.is_none());
	assert!(end.write.is_none());
	assert!(end.reason.is_none());
}

#[test]
fn open_failure_is_contained() {
	let dir = TestDir::new("failure");
	let occupied = dir.0.join("not-a-directory");
	fs::write(&occupied, b"x").expect("fixture");
	let impossible = occupied.join("diagnostics.db");
	assert!(Diagnostics::open(&impossible).is_err());
	assert!(diagnostics::open_for_config(&Config::default(), impossible).is_none());
}

#[cfg(unix)]
fn mode(path: &Path) -> u32 {
	use std::os::unix::fs::PermissionsExt;
	fs::metadata(path).expect("metadata").permissions().mode() & 0o777
}

#[cfg(unix)]
#[test]
fn database_and_sidecars_are_owner_only() {
	let dir = TestDir::new("permissions");
	let sink = Diagnostics::open(dir.db()).expect("open");
	let mut span = sink.begin_span(SpanKind::Model, None);
	span.finish(SpanStatus::Succeeded, TokenRecord::default());
	sink.flush();
	assert_eq!(mode(&dir.db()), 0o600, "database must be owner-only");
	for suffix in ["-wal", "-shm", "-journal"] {
		let mut path = dir.db().into_os_string();
		path.push(suffix);
		let path = PathBuf::from(path);
		if path.exists() {
			assert_eq!(mode(&path), 0o600, "sidecar must be owner-only: {suffix}");
		}
	}
	drop(sink);

	// An existing world-readable database is clamped on the next open.
	use std::os::unix::fs::PermissionsExt;
	fs::set_permissions(&dir.db(), fs::Permissions::from_mode(0o644)).expect("chmod");
	let sink = Diagnostics::open(dir.db()).expect("reopen");
	assert_eq!(mode(&dir.db()), 0o600, "existing file must be clamped");
	drop(sink);

	// A diagnostics directory created by this open is owner-only.
	let nested_dir = dir.0.join("nested");
	let nested_db = nested_dir.join("diagnostics.db");
	let sink = Diagnostics::open(&nested_db).expect("nested open");
	assert_eq!(mode(&nested_dir), 0o700, "created directory must be owner-only");
	assert_eq!(mode(&nested_db), 0o600);
	drop(sink);
}

#[cfg(unix)]
#[test]
fn nonsticky_world_writable_ancestor_is_rejected_without_chmod() {
	use std::os::unix::fs::PermissionsExt;

	let dir = TestDir::new("shared-dir");
	let shared = dir.0.join("shared");
	fs::create_dir_all(&shared).expect("shared dir");
	fs::set_permissions(&shared, fs::Permissions::from_mode(0o777)).expect("shared mode");
	let database = shared.join("diagnostics.db");
	assert!(
		Diagnostics::open(&database).is_err(),
		"a nonsticky world-writable ancestor must disable diagnostics"
	);
	assert_eq!(mode(&shared), 0o777, "shared directory must not be chmodded");
	assert!(!database.exists(), "no database may be created in a shared directory");
}

#[cfg(all(unix, target_os = "android"))]
#[test]
fn android_private_group_ancestor_is_allowed() {
	use std::os::unix::fs::PermissionsExt;

	let dir = TestDir::new("android-group");
	let group_dir = dir.0.join("groupdir");
	fs::create_dir_all(&group_dir).expect("group dir");
	fs::set_permissions(&group_dir, fs::Permissions::from_mode(0o770)).expect("group mode");
	// The ancestor may be group-writable; the diagnostics directory itself
	// must still be a private 0700 child.
	let trace_dir = group_dir.join("diagnostics");
	fs::create_dir_all(&trace_dir).expect("trace dir");
	fs::set_permissions(&trace_dir, fs::Permissions::from_mode(0o700)).expect("trace mode");
	let sink = Diagnostics::open(trace_dir.join("diagnostics.db"))
		.expect("Android per-app group-writable ancestor is trusted");
	assert_eq!(mode(&group_dir), 0o770, "user directory must not be chmodded");
	assert_eq!(mode(&trace_dir), 0o700);
	drop(sink);
}

#[cfg(all(unix, not(target_os = "android")))]
#[test]
fn nonsticky_group_writable_ancestor_is_rejected_without_chmod() {
	use std::os::unix::fs::PermissionsExt;

	// GID 0 is not treated as exclusive; this applies to ordinary users and
	// root alike.
	let dir = TestDir::new("group-dir");
	let group_dir = dir.0.join("groupdir");
	fs::create_dir_all(&group_dir).expect("group dir");
	fs::set_permissions(&group_dir, fs::Permissions::from_mode(0o770)).expect("group mode");
	let database = group_dir.join("diagnostics.db");
	assert!(
		Diagnostics::open(&database).is_err(),
		"unproven group-writable ancestor must disable diagnostics"
	);
	assert_eq!(mode(&group_dir), 0o770, "ancestor must not be chmodded");
	assert!(!database.exists());
}

#[cfg(unix)]
#[test]
fn symlinked_database_and_parent_are_rejected() {
	use std::os::unix::fs::symlink;

	let dir = TestDir::new("symlink-db");
	let target = dir.0.join("target.db");
	fs::write(&target, b"not a database").expect("target");
	let link = dir.0.join("diagnostics.db");
	symlink(&target, &link).expect("symlink");
	assert!(Diagnostics::open(&link).is_err(), "symlink leaf must be rejected");
	assert_eq!(fs::read(&target).expect("target intact"), b"not a database");

	let parent_link = dir.0.join("linked-parent");
	symlink(&dir.0, &parent_link).expect("parent symlink");
	assert!(
		Diagnostics::open(parent_link.join("diagnostics.db")).is_err(),
		"symlink parent must be rejected"
	);
}

#[cfg(unix)]
#[test]
fn symlinked_sidecar_is_rejected() {
	use std::os::unix::fs::symlink;

	let dir = TestDir::new("symlink-wal");
	let target = dir.0.join("sidecar-target");
	fs::write(&target, b"do not touch").expect("target");
	let wal = dir.0.join("diagnostics.db-wal");
	symlink(&target, &wal).expect("wal symlink");
	assert!(
		Diagnostics::open(dir.db()).is_err(),
		"a pre-existing symlink sidecar must disable diagnostics"
	);
	assert_eq!(fs::read(&target).expect("target intact"), b"do not touch");
}

#[test]
fn subagent_recorders_are_isolated_per_scope() {
	let dir = TestDir::new("observer-isolation");
	let sink = Diagnostics::open(dir.db()).expect("open");
	let first = SubagentRecorder::new(Arc::clone(&sink));
	let second = SubagentRecorder::new(Arc::clone(&sink));
	let id = "colliding-child-id";
	for recorder in [&first, &second] {
		recorder.observe(&lifecycle(id, SubagentLifecycleStatus::Started, None));
		recorder.observe(&lifecycle(id, SubagentLifecycleStatus::Completed, Some(3)));
	}
	sink.flush();
	let rows = fetch_rows(&dir.db());
	let subagents = rows
		.iter()
		.filter(|row| row.kind == "subagent" && row.status == "succeeded")
		.collect::<Vec<_>>();
	assert_eq!(subagents.len(), 2, "each supervisor gets its own child map");
	assert_ne!(subagents[0].span_id, subagents[1].span_id);
}

#[test]
fn abrupt_shutdown_closes_open_sessions_once() {
	let dir = TestDir::new("abrupt");
	let sink = Diagnostics::open(dir.db()).expect("open");
	let trace = sink.start_session("raw-abrupt-session");
	assert!(sink.session_span("raw-abrupt-session").is_some());
	sink.shutdown();
	let initial = fetch_rows(&dir.db());
	let closed = initial
		.iter()
		.filter(|row| row.kind == "session" && row.status == "closed")
		.count();
	assert_eq!(closed, 1, "abrupt teardown closes the open session once");
	drop(trace);
	let after = fetch_rows(&dir.db());
	assert_eq!(
		after.iter().filter(|row| row.kind == "session" && row.status == "closed").count(),
		1,
		"trace drop after shutdown must not duplicate the close row"
	);
}

#[test]
fn session_alias_parents_security_runs() {
	let dir = TestDir::new("alias");
	let sink = Diagnostics::open(dir.db()).expect("open");
	let trace = sink.start_session("scan-42");
	sink.alias_session("security:scan-42", "scan-42");
	let canonical = sink.session_span("scan-42").expect("canonical");
	assert_eq!(sink.session_span("security:scan-42"), Some(canonical));
	drop(trace);
	sink.flush();
	let rows = fetch_rows(&dir.db());
	assert_eq!(
		rows.iter().filter(|row| row.kind == "session").count(),
		2,
		"alias must not create a second session boundary"
	);
}

#[test]
fn concurrent_writers_all_flush() {
	let dir = TestDir::new("concurrent");
	let sink = Diagnostics::open(dir.db()).expect("open");
	let mut handles = Vec::new();
	for _ in 0..4 {
		let sink = Arc::clone(&sink);
		handles.push(thread::spawn(move || {
			for _ in 0..50 {
				let mut span = sink.begin_span(SpanKind::ToolCustom, None);
				span.finish(SpanStatus::Succeeded, TokenRecord::default());
			}
		}));
	}
	for handle in handles {
		handle.join().expect("writer");
	}
	sink.flush();
	let rows = fetch_rows(&dir.db());
	assert_eq!(rows.len(), 400, "every start/end pair persisted");
}
