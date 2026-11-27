//! Round-trip coverage for the portable session export document and the R6
//! bundle/inspector APIs: exported schema rows survive export -> import at the
//! column-value level, redaction stays defensive, and imports fail closed.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use aragent::session_store::{
	NewArtifact, NewBranch, NewDelivery, NewIntent, NewMessage, NewSession, NewTaskAdmission,
	SCHEMA_VERSION, SESSION_BUNDLE_FORMAT, SESSION_EXPORT_FORMAT, SESSION_EXPORT_VERSION,
	SqliteStore, StoreError, WorkerCompletion,
};
use serde_json::json;

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_dir(prefix: &str) -> PathBuf {
	let unique = format!(
		"aragent-session-export-{prefix}-{}-{}",
		std::process::id(),
		COUNTER.fetch_add(1, Ordering::Relaxed)
	);
	let dir = std::env::temp_dir().join(unique);
	fs::create_dir_all(&dir).expect("create temp dir");
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).expect("chmod temp dir");
	}
	dir
}

const SESSION: &str = "session-export-1";
const ROOT_BRANCH: &str = "branch-root";
const CHILD_BRANCH: &str = "branch-child";
const ARTIFACT_LOCATION: &str = "/tmp/do-not-export-artifact.txt";

fn create_session(store: &mut SqliteStore, id: &str, parent: Option<&str>, branch: &str) {
	store
		.create_session(NewSession {
			id: id.to_string(),
			parent_session_id: parent.map(str::to_string),
			payload: json!({"kind": "session", "title": id}),
			initial_branch_id: branch.to_string(),
			initial_branch_payload: json!({"kind": "branch"}),
		})
		.expect("create session");
}

fn message(id: &str, branch: &str, parent: Option<&str>) -> NewMessage {
	NewMessage {
		id: id.to_string(),
		session_id: SESSION.to_string(),
		branch_id: branch.to_string(),
		parent_message_id: parent.map(str::to_string),
		payload: json!({
			"type": "message",
			"id": id,
			"parentId": parent,
			"timestamp": "2026-01-01T00:00:00Z",
			"message": {"role": "user", "content": id},
		}),
	}
}

fn populated_source(dir: &std::path::Path) -> SqliteStore {
	let mut source = SqliteStore::open(dir.join("source.db")).expect("open source store");
	create_session(&mut source, SESSION, None, ROOT_BRANCH);
	source
		.append_message(message("message-1", ROOT_BRANCH, None))
		.expect("append root message");
	source
		.create_branch(NewBranch {
			id: CHILD_BRANCH.to_string(),
			session_id: SESSION.to_string(),
			parent_branch_id: Some(ROOT_BRANCH.to_string()),
			parent_message_id: Some("message-1".to_string()),
			payload: json!({"kind": "branch", "summary": "child"}),
		})
		.expect("create child branch");
	source
		.append_message(message("message-2", CHILD_BRANCH, Some("message-1")))
		.expect("append child message");

	let execution_id = source
		.record_intent(NewIntent {
			session_id: SESSION.to_string(),
			branch_id: CHILD_BRANCH.to_string(),
			provider_call_id: "call-1".to_string(),
			tool_name: "read".to_string(),
			payload: json!({"path": "README.md"}),
		})
		.expect("record intent");
	source
		.record_outcome(&execution_id, json!({"status": "ok", "bytes": 42}))
		.expect("record outcome");

	source
		.enqueue_delivery(NewDelivery {
			delivery_id: "delivery-peer-1".to_string(),
			worker_id: "worker-peer-1".to_string(),
			session_id: SESSION.to_string(),
			origin_branch_id: ROOT_BRANCH.to_string(),
			payload: json!({"kind": "peer", "text": "hello"}),
		})
		.expect("enqueue peer delivery");
	source
		.complete_worker(WorkerCompletion {
			worker_id: "worker-complete-1".to_string(),
			delivery_id: "delivery-complete-1".to_string(),
			session_id: SESSION.to_string(),
			origin_branch_id: ROOT_BRANCH.to_string(),
			completion: json!({"ok": true}),
			delivery: json!({"kind": "worker", "mode": "sync"}),
		})
		.expect("complete worker");
	source
		.admit_task(NewTaskAdmission {
			task_id: "task-1".to_string(),
			parent_session_id: SESSION.to_string(),
			parent_branch_id: ROOT_BRANCH.to_string(),
			worker_id: "task-1".to_string(),
			producer_generation: 0,
			spec: json!({"prompt": "child work"}),
		})
		.expect("admit task");
	source
		.save_stream_draft(SESSION, ROOT_BRANCH, "partial output", "attempt-1")
		.expect("save stream draft");
	// Artifact metadata references workspace state; D55 keeps it out of the
	// portable document even though the row exists in the source database.
	source
		.record_artifact(NewArtifact {
			artifact_id: "artifact-1".to_string(),
			session_id: SESSION.to_string(),
			branch_id: Some(ROOT_BRANCH.to_string()),
			execution_id: None,
			location: ARTIFACT_LOCATION.to_string(),
			metadata: json!({"kind": "file"}),
		})
		.expect("record artifact");
	source
}

#[test]
fn session_export_import_round_trip_is_lossless() {
	let dir = temp_dir("round-trip");
	let source = populated_source(&dir);
	let export_path = dir.join("session-export.json");

	source
		.export_session_to_path(SESSION, &export_path)
		.expect("export session to file");
	let document = fs::read_to_string(&export_path).expect("read export file");
	assert!(
		!document.contains(ARTIFACT_LOCATION),
		"artifact metadata must not leak into the portable export"
	);

	let exported = source.export_session(SESSION).expect("export session");
	assert_eq!(exported.format, SESSION_EXPORT_FORMAT);
	assert_eq!(exported.version, SESSION_EXPORT_VERSION);
	assert_eq!(exported.schema_version, SCHEMA_VERSION);
	assert_eq!(exported.session_id, SESSION);
	assert!(
		!exported.tables.contains_key("artifacts"),
		"artifact rows are excluded from the portable document"
	);
	for table in [
		"sessions",
		"branches",
		"messages",
		"executions",
		"deliveries",
		"worker_completions",
		"stream_drafts",
		"task_admissions",
		"fork_provenance",
	] {
		assert!(exported.tables.contains_key(table), "missing exported table {table}");
	}
	assert!(!exported.tables["executions"].is_empty());
	assert!(!exported.tables["deliveries"].is_empty());
	assert!(!exported.tables["worker_completions"].is_empty());
	assert!(!exported.tables["task_admissions"].is_empty());
	assert!(!exported.tables["stream_drafts"].is_empty());

	let mut target = SqliteStore::open(dir.join("target.db")).expect("open target store");
	target
		.import_session_from_path(&export_path)
		.expect("import session");
	assert!(
		target
			.artifacts(SESSION)
			.expect("target artifacts")
			.is_empty(),
		"import must not synthesize artifact metadata"
	);

	let round_trip = target
		.export_session(SESSION)
		.expect("re-export imported session");
	assert_eq!(round_trip.session_id, exported.session_id);
	assert_eq!(round_trip.schema_version, exported.schema_version);
	assert_eq!(
		round_trip.tables, exported.tables,
		"every exported column value must survive export -> import"
	);

	let error = target
		.import_session_from_path(&export_path)
		.expect_err("existing session must be refused");
	assert!(
		matches!(&error, StoreError::Conflict(message) if message.contains("already exists")),
		"unexpected duplicate import error: {error}"
	);
}

#[test]
fn import_requires_the_parent_session_and_rejects_bad_versions() {
	let dir = temp_dir("guards");
	let mut source = SqliteStore::open(dir.join("source.db")).expect("open source store");
	create_session(&mut source, "parent-session", None, "parent-branch");
	create_session(&mut source, "child-session", Some("parent-session"), "child-branch");

	let parent = source
		.export_session("parent-session")
		.expect("export parent");
	let child = source
		.export_session("child-session")
		.expect("export child");

	let mut target = SqliteStore::open(dir.join("target.db")).expect("open target store");
	let error = target
		.import_session(child.clone())
		.expect_err("importing a child without its parent must fail");
	assert!(
		matches!(&error, StoreError::NotFound(message) if message.contains("parent session")),
		"unexpected missing-parent error: {error}"
	);

	target.import_session(parent).expect("import parent first");
	target
		.import_session(child)
		.expect("import child after parent");

	let mut bad_version = target
		.export_session("parent-session")
		.expect("export parent again");
	bad_version.version = SESSION_EXPORT_VERSION + 1;
	let error = target
		.import_session(bad_version)
		.expect_err("unsupported export version must fail");
	assert!(matches!(&error, StoreError::Schema(_)), "unexpected version error: {error}");

	let mut bad_schema = target
		.export_session("parent-session")
		.expect("export parent again");
	bad_schema.schema_version = SCHEMA_VERSION - 1;
	let error = target
		.import_session(bad_schema)
		.expect_err("mismatched schema version must fail");
	assert!(matches!(&error, StoreError::Schema(_)), "unexpected schema error: {error}");

	let error = source
		.export_session("missing-session")
		.expect_err("missing session must fail");
	assert!(matches!(&error, StoreError::NotFound(_)), "unexpected missing-session error: {error}");
}

#[test]
fn session_bundle_round_trip_is_lossless_and_redacted() {
	let dir = temp_dir("bundle-round-trip");
	let secret = "sk-bundle-secret-4f5d6a7b8c9d";
	aragent::auth::known_secrets::register(secret);
	let mut source = SqliteStore::open(dir.join("source.db")).expect("open source store");
	source
		.create_session(NewSession {
			id: SESSION.to_string(),
			parent_session_id: None,
			payload: json!({
				"kind": "session",
				"title": format!("title {secret}"),
				"cwd": "/tmp/bundle",
			}),
			initial_branch_id: ROOT_BRANCH.to_string(),
			initial_branch_payload: json!({"kind": "branch"}),
		})
		.expect("create bundle session");
	source
		.append_message(NewMessage {
			id: "bundle-message-1".to_string(),
			session_id: SESSION.to_string(),
			branch_id: ROOT_BRANCH.to_string(),
			parent_message_id: None,
			payload: json!({
				"type": "message",
				"id": "bundle-message-1",
				"parentId": null,
				"timestamp": "2026-01-01T00:00:00Z",
				"message": {
					"role": "user",
					"content": format!("secret {secret}"),
				},
			}),
		})
		.expect("append bundle message");
	source
		.record_artifact(NewArtifact {
			artifact_id: "bundle-artifact-1".to_string(),
			session_id: SESSION.to_string(),
			branch_id: Some(ROOT_BRANCH.to_string()),
			execution_id: None,
			location: format!("/tmp/{secret}-artifact.txt"),
			metadata: json!({"kind": "file"}),
		})
		.expect("record bundle artifact");

	let bundle = source.export_session_bundle(SESSION).expect("export bundle");
	assert_eq!(bundle.format, SESSION_BUNDLE_FORMAT);
	assert_eq!(bundle.schema_version, SCHEMA_VERSION);
	assert_eq!(bundle.session.id, SESSION);
	assert_eq!(bundle.branches.len(), 1);
	assert_eq!(bundle.messages.len(), 1);
	assert_eq!(bundle.artifacts.len(), 1);
	let document = serde_json::to_string(&bundle).expect("serialize bundle");
	assert!(
		!document.contains(secret),
		"bundle must not contain registered credential material"
	);

	let mut target = SqliteStore::open(dir.join("target.db")).expect("open target store");
	let report = target
		.import_session_bundle(&bundle, false)
		.expect("import bundle");
	assert_eq!(report.session_id, SESSION);
	assert!(!report.detached);
	assert_eq!(report.branches, 1);
	assert_eq!(report.messages, 1);
	assert_eq!(report.artifacts, 1);

	let round_trip = target.export_session_bundle(SESSION).expect("re-export bundle");
	let mut expected = bundle.clone();
	expected.exported_at_ms = round_trip.exported_at_ms;
	assert_eq!(round_trip, expected, "bundle must survive export -> import -> export");

	let error = target
		.import_session_bundle(&bundle, false)
		.expect_err("existing session must be refused");
	assert!(matches!(&error, StoreError::Conflict(_)), "unexpected conflict error: {error}");
}

#[test]
fn session_bundle_import_can_detach_a_missing_parent() {
	let dir = temp_dir("bundle-detach");
	let mut source = SqliteStore::open(dir.join("source.db")).expect("open source store");
	create_session(&mut source, "bundle-parent", None, "bundle-parent-branch");
	create_session(
		&mut source,
		"bundle-child",
		Some("bundle-parent"),
		"bundle-child-branch",
	);
	let child = source
		.export_session_bundle("bundle-child")
		.expect("export child bundle");

	let mut target = SqliteStore::open(dir.join("target.db")).expect("open target store");
	let error = target
		.import_session_bundle(&child, false)
		.expect_err("missing parent without detach must fail");
	assert!(matches!(&error, StoreError::Validation(_)), "unexpected parent error: {error}");

	let report = target
		.import_session_bundle(&child, true)
		.expect("detach import child");
	assert!(report.detached);
	assert_eq!(report.detached_parent_session_id.as_deref(), Some("bundle-parent"));

	let imported = target
		.export_session_bundle("bundle-child")
		.expect("re-export detached child");
	assert_eq!(imported.session.parent_session_id, None);
	assert_eq!(
		imported.session.payload.get("_imported_parent_session_id"),
		Some(&json!("bundle-parent"))
	);
}

#[test]
fn inspectors_report_counts_and_never_message_bodies() {
	let dir = temp_dir("inspect");
	let source = populated_source(&dir);
	let report = source.inspect_session(SESSION).expect("inspect session");
	assert_eq!(report.id, SESSION);
	assert_eq!(report.branches, 2);
	assert_eq!(report.messages, 2);
	assert_eq!(report.active_branch_messages, 1);
	assert_eq!(report.executions, 1);
	assert_eq!(report.unresolved_executions, 0);
	assert_eq!(report.deliveries, 2);
	assert_eq!(report.worker_completions, 1);
	assert_eq!(report.artifacts, 1);
	assert_eq!(report.task_admissions, 1);
	assert_eq!(report.stream_drafts, 1);
	assert_eq!(
		report.missing_artifacts_on_disk,
		if std::path::Path::new(ARTIFACT_LOCATION).exists() {
			0
		} else {
			1
		}
	);
	assert!(report.context_estimate.total > 0);
	assert!(report.provider_usage.is_none());
	let document = serde_json::to_string(&report).expect("serialize inspector");
	assert!(
		!document.contains("\"content\"") && !document.contains("message-1"),
		"inspector output must not contain raw message bodies: {document}"
	);
	assert_eq!(report.artifact_summaries.len(), 1);
	assert_eq!(report.artifact_summaries[0].artifact_id, "artifact-1");

	let database = source.inspect_database().expect("inspect database");
	assert_eq!(database.schema_version, SCHEMA_VERSION);
	assert_eq!(database.table_counts.get("sessions"), Some(&1));
	assert_eq!(database.sessions.len(), 1);
	assert_eq!(database.sessions[0].messages, 2);
}

#[test]
fn full_v7_bundle_round_trip_preserves_every_table_and_redacts() {
	let dir = temp_dir("full-v7");
	let secret = "sk-full-bundle-secret-19a7b3c5";
	aragent::auth::known_secrets::register(secret);
	let missing_artifact = dir.join("missing-artifact.bin");
	let _ = fs::remove_file(&missing_artifact);

	let mut source = SqliteStore::open(dir.join("source.db")).expect("open source store");
	source
		.create_session(NewSession {
			id: SESSION.to_string(),
			parent_session_id: None,
			payload: json!({
				"kind": "session",
				"title": format!("full {secret}"),
				"cwd": format!("/tmp/{secret}"),
			}),
			initial_branch_id: ROOT_BRANCH.to_string(),
			initial_branch_payload: json!({"kind": "branch", "note": secret}),
		})
		.expect("create full session");
	source
		.set_session_producer_generation(SESSION, 7)
		.expect("set producer generation");
	source
		.append_message(NewMessage {
			id: "full-message-1".to_string(),
			session_id: SESSION.to_string(),
			branch_id: ROOT_BRANCH.to_string(),
			parent_message_id: None,
			payload: json!({
				"type": "message",
				"id": "full-message-1",
				"parentId": null,
				"timestamp": "2026-01-01T00:00:00Z",
				"message": {
					"role": "assistant",
					"content": [{"type": "text", "text": format!("root {secret}")}],
					"usage": {"input": 11, "output": 7, "totalTokens": 18},
				},
			}),
		})
		.expect("append root message");
	source
		.create_branch(NewBranch {
			id: CHILD_BRANCH.to_string(),
			session_id: SESSION.to_string(),
			parent_branch_id: Some(ROOT_BRANCH.to_string()),
			parent_message_id: Some("full-message-1".to_string()),
			payload: json!({"kind": "branch", "secret": secret}),
		})
		.expect("create child branch");
	source
		.append_message(NewMessage {
			id: "full-message-2".to_string(),
			session_id: SESSION.to_string(),
			branch_id: CHILD_BRANCH.to_string(),
			parent_message_id: Some("full-message-1".to_string()),
			payload: json!({
				"type": "message",
				"id": "full-message-2",
				"parentId": "full-message-1",
				"timestamp": "2026-01-01T00:00:00Z",
				"message": {
					"role": "toolResult",
					"content": [{"type": "text", "text": format!("child {secret}")}],
				},
			}),
		})
		.expect("append child message");

	let resolved = source
		.record_intent(NewIntent {
			session_id: SESSION.to_string(),
			branch_id: CHILD_BRANCH.to_string(),
			provider_call_id: "full-call-resolved".to_string(),
			tool_name: "read".to_string(),
			payload: json!({"path": format!("README-{secret}.md")}),
		})
		.expect("record resolved intent");
	source
		.record_outcome(&resolved, json!({"status": "ok", "text": secret}))
		.expect("record resolved outcome");
	source
		.record_intent(NewIntent {
			session_id: SESSION.to_string(),
			branch_id: ROOT_BRANCH.to_string(),
			provider_call_id: "full-call-unresolved".to_string(),
			tool_name: "shell".to_string(),
			payload: json!({"command": format!("echo {secret}")}),
		})
		.expect("record unresolved intent");

	source
		.enqueue_delivery(NewDelivery {
			delivery_id: "full-delivery-peer".to_string(),
			worker_id: "full-worker-peer".to_string(),
			session_id: SESSION.to_string(),
			origin_branch_id: ROOT_BRANCH.to_string(),
			payload: json!({"kind": "peer", "text": secret}),
		})
		.expect("enqueue full peer delivery");
	source
		.complete_worker(WorkerCompletion {
			worker_id: "full-worker-complete".to_string(),
			delivery_id: "full-delivery-complete".to_string(),
			session_id: SESSION.to_string(),
			origin_branch_id: CHILD_BRANCH.to_string(),
			completion: json!({"ok": true, "secret": secret}),
			delivery: json!({"kind": "worker", "text": secret}),
		})
		.expect("complete full worker");
	source
		.record_artifact(NewArtifact {
			artifact_id: "full-artifact".to_string(),
			session_id: SESSION.to_string(),
			branch_id: Some(CHILD_BRANCH.to_string()),
			execution_id: Some(resolved.clone()),
			location: missing_artifact.display().to_string(),
			metadata: json!({"kind": "file", "secret": secret}),
		})
		.expect("record full artifact");
	source
		.admit_task(NewTaskAdmission {
			task_id: "full-task".to_string(),
			parent_session_id: SESSION.to_string(),
			parent_branch_id: ROOT_BRANCH.to_string(),
			worker_id: "full-task-worker".to_string(),
			producer_generation: 7,
			spec: json!({"prompt": format!("task {secret}")}),
		})
		.expect("admit full task");
	source
		.save_stream_draft(
			SESSION,
			CHILD_BRANCH,
			&format!("draft {secret}"),
			"full-attempt",
		)
		.expect("save full stream draft");
	{
		let connection = rusqlite::Connection::open(dir.join("source.db")).expect("raw source");
		connection
			.execute(
				"INSERT INTO fork_provenance (message_id, session_id, source_execution_id, provider_call_id, outcome_resolved) \
				 VALUES (?1, ?2, ?3, ?4, ?5)",
				rusqlite::params!["full-message-2", SESSION, resolved, "full-call-resolved", 1i64],
			)
			.expect("insert fork provenance");
	}

	let bundle = source.export_session_bundle(SESSION).expect("export full bundle");
	assert_eq!(bundle.format, SESSION_BUNDLE_FORMAT);
	assert_eq!(bundle.schema_version, SCHEMA_VERSION);
	assert_eq!(bundle.session.producer_generation, Some(7));
	assert_eq!(bundle.branches.len(), 2);
	assert_eq!(bundle.messages.len(), 2);
	assert_eq!(bundle.executions.len(), 2);
	assert_eq!(bundle.deliveries.len(), 2);
	assert_eq!(bundle.worker_completions.len(), 1);
	assert_eq!(bundle.artifacts.len(), 1);
	assert_eq!(bundle.fork_provenance.len(), 1);
	assert_eq!(bundle.task_admissions.len(), 1);
	assert_eq!(bundle.stream_drafts.len(), 1);
	assert_eq!(bundle.artifacts[0].location, missing_artifact.display().to_string());
	let document = serde_json::to_string(&bundle).expect("serialize full bundle");
	assert!(!document.contains(secret), "exported bundle leaked a registered secret");

	let before = source.inspect_session(SESSION).expect("inspect source");
	assert_eq!(before.unresolved_executions, 1);
	assert_eq!(before.missing_artifacts_on_disk, 1);
	assert_eq!(before.provider_usage.as_ref().map(|usage| usage.total_tokens), Some(18));

	let mut target = SqliteStore::open(dir.join("target.db")).expect("open target store");
	let report = target
		.import_session_bundle(&bundle, false)
		.expect("import full bundle");
	assert_eq!(report.session_id, SESSION);
	assert_eq!(report.branches, 2);
	assert_eq!(report.messages, 2);
	assert_eq!(report.executions, 2);
	assert_eq!(report.deliveries, 2);
	assert_eq!(report.worker_completions, 1);
	assert_eq!(report.artifacts, 1);
	assert_eq!(report.fork_provenance, 1);
	assert_eq!(report.task_admissions, 1);
	assert_eq!(report.stream_drafts, 1);

	let round_trip = target
		.export_session_bundle(SESSION)
		.expect("re-export full bundle");
	let mut expected = bundle.clone();
	expected.exported_at_ms = round_trip.exported_at_ms;
	assert_eq!(round_trip, expected, "every v7 column must survive export -> import");
	assert_eq!(
		target.session_producer_generation(SESSION).expect("target generation"),
		Some(7)
	);
	let after = target.inspect_session(SESSION).expect("inspect target");
	assert_eq!(after, before, "inspector output must survive bundle round trip");

	let conflict = target
		.import_session_bundle(&bundle, false)
		.expect_err("existing id must conflict");
	assert!(matches!(conflict, StoreError::Conflict(_)));
	let unchanged = target
		.export_session_bundle(SESSION)
		.expect("re-export after conflict");
	let mut expected_unchanged = round_trip.clone();
	expected_unchanged.exported_at_ms = unchanged.exported_at_ms;
	assert_eq!(unchanged, expected_unchanged, "conflict must not mutate the target");
}

#[test]
fn invalid_bundles_are_rejected_without_mutating_the_target() {
	let dir = temp_dir("invalid-bundles");
	let mut source = SqliteStore::open(dir.join("source.db")).expect("open source store");
	create_session(&mut source, SESSION, None, ROOT_BRANCH);
	source
		.append_message(message("invalid-message-1", ROOT_BRANCH, None))
		.expect("append root message");
	source
		.create_branch(NewBranch {
			id: CHILD_BRANCH.to_string(),
			session_id: SESSION.to_string(),
			parent_branch_id: Some(ROOT_BRANCH.to_string()),
			parent_message_id: Some("invalid-message-1".to_string()),
			payload: json!({"kind": "branch"}),
		})
		.expect("create invalid-test branch");
	let bundle = source.export_session_bundle(SESSION).expect("export bundle");

	let mut target = SqliteStore::open(dir.join("target.db")).expect("open target store");
	let mut bad_format = bundle.clone();
	bad_format.format = "not-an-aragent-bundle".to_string();
	let error = target
		.import_session_bundle(&bad_format, false)
		.expect_err("bad bundle format must fail");
	assert!(matches!(error, StoreError::Validation(_)), "unexpected format error: {error}");

	let mut bad_schema = bundle.clone();
	bad_schema.schema_version = SCHEMA_VERSION + 1;
	let error = target
		.import_session_bundle(&bad_schema, false)
		.expect_err("unsupported bundle schema must fail");
	assert!(matches!(error, StoreError::Validation(_)), "unexpected schema error: {error}");

	let mut broken_parent = bundle.clone();
	broken_parent
		.branches
		.iter_mut()
		.find(|branch| branch.id == CHILD_BRANCH)
		.expect("child branch in bundle")
		.parent_branch_id = Some("missing-branch".to_string());
	let error = target
		.import_session_bundle(&broken_parent, false)
		.expect_err("broken branch parent must fail");
	assert!(matches!(error, StoreError::Validation(_)), "unexpected parent error: {error}");

	let database = target.inspect_database().expect("inspect empty target");
	assert_eq!(database.sessions.len(), 0, "validation failures must not create a session");
	assert_eq!(database.table_counts.get("messages").copied(), Some(0));
	assert_eq!(database.table_counts.get("branches").copied(), Some(0));
}

#[test]
fn bundle_import_allocates_database_wide_sequence_numbers() {
	let dir = temp_dir("bundle-seq");
	let mut source = SqliteStore::open(dir.join("source.db")).expect("open source store");
	create_session(&mut source, SESSION, None, ROOT_BRANCH);
	source
		.append_message(message("seq-message-1", ROOT_BRANCH, None))
		.expect("append source message 1");
	source
		.append_message(message("seq-message-2", ROOT_BRANCH, Some("seq-message-1")))
		.expect("append source message 2");
	source
		.record_intent(NewIntent {
			session_id: SESSION.to_string(),
			branch_id: ROOT_BRANCH.to_string(),
			provider_call_id: "seq-call-1".to_string(),
			tool_name: "read".to_string(),
			payload: json!({"path": "one"}),
		})
		.expect("record source execution 1");
	source
		.record_intent(NewIntent {
			session_id: SESSION.to_string(),
			branch_id: ROOT_BRANCH.to_string(),
			provider_call_id: "seq-call-2".to_string(),
			tool_name: "read".to_string(),
			payload: json!({"path": "two"}),
		})
		.expect("record source execution 2");
	let bundle = source.export_session_bundle(SESSION).expect("export seq bundle");

	let mut target = SqliteStore::open(dir.join("target.db")).expect("open target store");
	target
		.create_session(NewSession {
			id: "existing-session".to_string(),
			parent_session_id: None,
			payload: json!({
				"type": "session",
				"version": 3,
				"id": "existing-session",
				"timestamp": "2026-01-01T00:00:00Z",
				"cwd": "/tmp/existing",
			}),
			initial_branch_id: "existing-branch".to_string(),
			initial_branch_payload: json!({"kind": "branch"}),
		})
		.expect("create existing session");
	for (id, parent) in [
		("existing-message-1", None),
		("existing-message-2", Some("existing-message-1")),
	] {
		target
			.append_message(NewMessage {
				id: id.to_string(),
				session_id: "existing-session".to_string(),
				branch_id: "existing-branch".to_string(),
				parent_message_id: parent.map(str::to_string),
				payload: json!({
					"type": "message",
					"id": id,
					"parentId": parent,
					"timestamp": "2026-01-01T00:00:00Z",
					"message": {"role": "user", "content": id},
				}),
			})
			.expect("append existing message");
	}
	for call in ["existing-call-1", "existing-call-2"] {
		target
			.record_intent(NewIntent {
				session_id: "existing-session".to_string(),
				branch_id: "existing-branch".to_string(),
				provider_call_id: call.to_string(),
				tool_name: "read".to_string(),
				payload: json!({"path": call}),
			})
			.expect("record existing execution");
	}

	target
		.import_session_bundle(&bundle, false)
		.expect("import bundle over non-empty target");
	let imported = target.export_session_bundle(SESSION).expect("export imported bundle");
	let message_seqs: Vec<i64> = {
		let mut ids = imported.messages.iter().map(|m| (m.seq, m.id.as_str())).collect::<Vec<_>>();
		ids.sort_by_key(|(seq, _)| *seq);
		ids.iter().map(|(seq, _)| *seq).collect()
	};
	assert_eq!(message_seqs, vec![3, 4], "messages must append after existing seq 1..2");
	let mut message_ids = imported
		.messages
		.iter()
		.map(|message| (message.seq, message.id.as_str()))
		.collect::<Vec<_>>();
	message_ids.sort_by_key(|(seq, _)| *seq);
	assert_eq!(
		message_ids.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
		vec!["seq-message-1", "seq-message-2"],
		"relative message order must be preserved"
	);
	let mut execution_seqs = imported.executions.iter().map(|e| e.seq).collect::<Vec<_>>();
	execution_seqs.sort_unstable();
	assert_eq!(execution_seqs, vec![3, 4], "executions must append after existing seq 1..2");
	let existing = target
		.export_session_bundle("existing-session")
		.expect("existing bundle unchanged");
	let mut existing_seqs = existing.messages.iter().map(|m| m.seq).collect::<Vec<_>>();
	existing_seqs.sort_unstable();
	assert_eq!(existing_seqs, vec![1, 2]);
}

#[test]
fn bundle_import_rejects_path_like_session_ids_without_mutation() {
	let dir = temp_dir("bundle-bad-id");
	let mut source = SqliteStore::open(dir.join("source.db")).expect("open source store");
	create_session(&mut source, SESSION, None, ROOT_BRANCH);
	let bundle = source.export_session_bundle(SESSION).expect("export bundle");

	let mut target = SqliteStore::open(dir.join("target.db")).expect("open target store");
	for invalid in ["../escape", "foo/bar"] {
		let mut bad = bundle.clone();
		bad.session.id = invalid.to_string();
		let error = target
			.import_session_bundle(&bad, false)
			.expect_err("path-like id must be refused");
		assert!(
			matches!(error, StoreError::Validation(_)),
			"unexpected id error for {invalid}: {error}"
		);
	}
	let database = target.inspect_database().expect("inspect target");
	assert_eq!(database.sessions.len(), 0);
}
