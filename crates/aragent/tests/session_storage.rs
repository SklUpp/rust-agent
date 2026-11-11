use std::fs;
use std::path::{Path, PathBuf};

use aragent::dirs;
use aragent::durable_session::StoredProjection;
use aragent::session::{
	ContentPart, MessageContent, MessageEntry, SessionMessage, SessionOptions, SessionStore,
	ToolCallRecord, TurnRecord, compute_default_session_dir, continue_recent_with_breadcrumb,
	fork_session, import_legacy_v1, import_v3_session, overlay_title_slot, overwrite_title_slot_rolled_back,
	parse_title_slot, read_terminal_breadcrumb, resolve_resumable_session, serialize_title_slot,
	session_matches_resume_arg, write_terminal_breadcrumb,
};
use aragent::session_store::{ForkWrite, NewBranch, NewIntent, NewMessage, NewSession, SqliteStore};
use serde_json::json;

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn temp_dir(prefix: &str) -> PathBuf {
	let unique = format!(
		"omp-test-{prefix}-{}-{}",
		std::process::id(),
		COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
	);
	let dir = std::env::temp_dir().join(unique);
	fs::create_dir_all(&dir).expect("create temp dir");
	dir
}

#[test]
fn read_ts_v3_session_fixture() {
	let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures/v3_session.jsonl");
	assert!(fixture_path.is_file(), "fixture must exist");

	let temp = temp_dir("v3-read");
	let test_file = temp.join("session.jsonl");
	fs::copy(&fixture_path, &test_file).expect("copy fixture");

	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/test-project"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("session".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});

	assert!(store.enabled(), "store must be enabled: {:?}", store.take_warnings());
	let header = store.header().expect("header must be present");
	assert_eq!(header.id, "01a178ce-3463-7504-8047-ca4fab78a16b");
	assert_eq!(header.cwd, "/path/to/test-project");
	assert_eq!(header.title.as_deref(), Some("Test Session Title"));

	let history = store.history();
	assert_eq!(history.len(), 1, "history must reconstruct 1 turn");
	assert_eq!(history[0].prompt, "hello from test user");
	assert_eq!(history[0].assistant, "calling tool");
	assert_eq!(history[0].tool_calls.len(), 1);
	assert_eq!(history[0].tool_calls[0].id, "call_abc123");
	assert_eq!(history[0].tool_calls[0].name, "read");
	assert_eq!(history[0].tool_calls[0].result, "file content");
	assert!(!history[0].tool_calls[0].is_error);

	assert!(store.has_image_content(), "must preserve image block");
	assert!(!store.entries().is_empty(), "must retain entries");

	// Typed image parts must survive resume for provider replay.
	let messages = store.messages();
	assert_eq!(messages.len(), 3, "fixture has 3 message entries (user, assistant, toolResult)");
	let user_parts = messages[0].message.content.parts();
	assert!(
		user_parts
			.iter()
			.any(|part| part.is_image()
				&& part.data.as_deref() == Some("aGVsbG8=")
				&& part.mime_type.as_deref() == Some("image/png")),
		"user image part must survive replay: {user_parts:?}"
	);
	let tool_parts = messages[2].message.content.parts();
	assert!(
		tool_parts
			.iter()
			.any(|part| part.is_image() && part.data.as_deref() == Some("aW1hZ2VkYXRh")),
		"toolResult image part must survive replay: {tool_parts:?}"
	);

	// Provider metadata and TS catalog usage keys (including required cost) round-trip truthfully.
	let asst = &messages[1].message;
	assert_eq!(asst.api.as_deref(), Some("openai-completions"));
	assert_eq!(asst.provider.as_deref(), Some("openai"));
	assert_eq!(asst.model.as_deref(), Some("gpt-4o"));
	let usage = asst.usage.as_ref().expect("usage must round-trip");
	assert_eq!((usage.input, usage.output, usage.total_tokens), (10, 20, 30));
	let cost = usage.cost.as_ref().expect("cost must round-trip");
	assert!((cost.total - 0.0003).abs() < 1e-6, "cost.total must round-trip truthfully: {:?}", usage.cost);
	assert!((cost.input - 0.0001).abs() < 1e-6);
	assert!((cost.output - 0.0002).abs() < 1e-6);
	assert_eq!(asst.stop_reason.as_deref(), Some("toolUse"));
	fs::remove_dir_all(temp).ok();
}

#[test]
fn v1_legacy_log_is_read_only_and_never_overwritten() {
	let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures/v1_session.jsonl");
	let temp = temp_dir("v1-readonly");
	let v1_file = temp.join("v1-legacy-id.jsonl");
	fs::copy(&fixture_path, &v1_file).expect("copy fixture");
	let original_content = fs::read_to_string(&v1_file).expect("read original");

	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/legacy"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("v1-legacy-id".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});

	assert!(store.enabled(), "legacy store can be opened for reading");
	assert_eq!(store.history().len(), 1);
	assert_eq!(store.history()[0].prompt, "fix the bug");

	// Appending to v1 must fail / be rejected and NEVER mutate the v1 file on disk
	let dummy_turn = TurnRecord {
		prompt: "next".to_string(),
		assistant: "done".to_string(),
		reasoning: String::new(),
		reasoning_after_text: false,
		stop_reason: "end_turn".to_string(),
		tool_calls: Vec::new(),
		parts: Vec::new(),
	};
	let append_result = store.append_turn(&dummy_turn);
	assert!(
		append_result.is_err(),
		"appending to legacy v1 log must be rejected"
	);

	let current_content = fs::read_to_string(&v1_file).expect("read v1");
	assert_eq!(
		original_content, current_content,
		"legacy v1 file on disk must not be modified"
	);

	fs::remove_dir_all(temp).ok();
}

#[test]
fn explicit_import_legacy_v1_creates_valid_v3_copy_without_mutating_v1() {
	let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures/v1_session.jsonl");
	let temp = temp_dir("v1-import");
	let v1_file = temp.join("v1-source.jsonl");
	fs::copy(&fixture_path, &v1_file).expect("copy fixture");
	let original_content = fs::read_to_string(&v1_file).expect("read v1");

	let target_dir = temp.join("v3-target");
	fs::create_dir_all(&target_dir).expect("create target dir");

	let imported_path = import_legacy_v1(&v1_file, &target_dir, None)
		.expect("import legacy v1");
	let imported_id = imported_path
		.file_stem()
		.and_then(|stem| stem.to_str())
		.expect("imported session id")
		.to_string();
	assert!(target_dir.join("sessions.db").is_file());
	assert!(!imported_path.exists(), "import must not write a JSONL journal");

	// v1 original must be untouched
	assert_eq!(
		fs::read_to_string(&v1_file).expect("read v1"),
		original_content,
		"original v1 must remain untouched"
	);

	// Imported session must be reopenable from SQLite and have the same turns.
	let store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/legacy"),
		model: None,
		provider: None,
		dir: Some(target_dir.clone()),
		id: Some(imported_id),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});
	assert!(store.enabled(), "reopened imported store must be enabled");
	assert_eq!(store.history().len(), 1);
	assert_eq!(store.history()[0].prompt, "fix the bug");

	fs::remove_dir_all(temp).ok();
}

#[test]
fn malformed_header_fails_without_append() {
	let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures/malformed_header.jsonl");
	let temp = temp_dir("bad-header");
	let bad_file = temp.join("bad.jsonl");
	fs::copy(&fixture_path, &bad_file).expect("copy bad");
	let original = fs::read_to_string(&bad_file).expect("read");

	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("bad".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});

	assert!(!store.enabled(), "store must be disabled on malformed header");
	let warnings = store.take_warnings();
	assert!(
		warnings.iter().any(|w| w.contains("session header") || w.contains("invalid")),
		"must report warning about bad header: {warnings:?}"
	);

	let turn = TurnRecord {
		prompt: "hi".into(),
		assistant: "hello".into(),
		reasoning: String::new(),
		reasoning_after_text: false,
		stop_reason: "end_turn".into(),
		tool_calls: vec![],
		parts: vec![],
	};
	let _ = store.append_turn(&turn);
	assert_eq!(
		fs::read_to_string(&bad_file).expect("read"),
		original,
		"file must never be modified when header is invalid"
	);

	fs::remove_dir_all(temp).ok();
}

#[test]
fn malformed_entry_disables_persistence_and_refuses_append() {
	let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures/malformed_entry.jsonl");
	let temp = temp_dir("bad-entry");
	let file = temp.join("malformed.jsonl");
	fs::copy(&fixture_path, &file).expect("copy");
	let original = fs::read_to_string(&file).expect("read");

	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/test"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("malformed".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});

	assert!(!store.enabled(), "store must disable persistence when log contains corrupted records");
	let warnings = store.take_warnings();
	assert!(
		warnings.iter().any(|w| w.contains("malformed") || w.contains("corrupting")),
		"warnings must report malformed record: {warnings:?}"
	);

	let turn = TurnRecord {
		prompt: "hi".into(),
		assistant: "hello".into(),
		reasoning: String::new(),
		reasoning_after_text: false,
		stop_reason: "end_turn".into(),
		tool_calls: vec![],
		parts: vec![],
	};
	let _ = store.append_turn(&turn);
	assert_eq!(
		fs::read_to_string(&file).expect("read after append attempt"),
		original,
		"corrupted file must never be appended to"
	);

	fs::remove_dir_all(temp).ok();
}

#[test]
fn title_slot_exact_256_bytes_and_overlay() {
	let slot = serialize_title_slot(
		"Test Title with Unicode 🦀 emoji and long description",
		Some("auto"),
		"2026-10-26T10:00:00.000Z",
	);
	assert_eq!(slot.as_bytes().len(), 256, "slot must be exactly 256 bytes");
	assert!(slot.ends_with('\n'), "slot must end with newline");

	let parsed = parse_title_slot(&slot).expect("parse slot");
	assert_eq!(parsed.title, "Test Title with Unicode 🦀 emoji and long description");
	assert_eq!(parsed.source.as_deref(), Some("auto"));
	assert_eq!(parsed.updated_at, "2026-10-26T10:00:00.000Z");

	// Massive title exceeding 256 bytes truncated cleanly by codepoints
	let massive_title = "🦀".repeat(200);
	let truncated_slot = serialize_title_slot(&massive_title, Some("user"), "2026-10-26T10:00:00.000Z");
	assert_eq!(truncated_slot.as_bytes().len(), 256, "truncated slot must be exactly 256 bytes");
	let parsed_trunc = parse_title_slot(&truncated_slot).expect("parse truncated slot");
	assert!(!parsed_trunc.title.is_empty());
	assert!(massive_title.starts_with(&parsed_trunc.title));

	// Overlay test
	let content = format!("{slot}line 2\nline 3\n");
	let new_slot = serialize_title_slot("Updated Title", Some("user"), "2026-10-26T11:00:00.000Z");
	let overlaid = overlay_title_slot(&content, &new_slot);
	assert!(overlaid.ends_with("line 2\nline 3\n"));
	assert_eq!(overlaid.as_bytes().len(), content.as_bytes().len());
	let parsed_overlaid = parse_title_slot(&overlaid).expect("parse overlaid");
	assert_eq!(parsed_overlaid.title, "Updated Title");
}

#[test]
fn ts_cwd_encoded_directory_names() {
	let home = dirs::home_dir().expect("home dir");
	let sessions_root = home.join(".omp/agent/sessions");

	// Home subpath
	let home_project = home.join("projects/my-app");
	let dir = compute_default_session_dir(&home_project, Some(&sessions_root));
	assert_eq!(dir, sessions_root.join("-projects-my-app"));

	// Home root
	let dir_home = compute_default_session_dir(&home, Some(&sessions_root));
	assert_eq!(dir_home, sessions_root.join("-"));

	// Tmp subpath
	let tmp = std::env::temp_dir();
	let tmp_project = tmp.join("sandbox/app");
	let dir_tmp = compute_default_session_dir(&tmp_project, Some(&sessions_root));
	assert!(
		dir_tmp.file_name().unwrap().to_str().unwrap().starts_with("-tmp"),
		"tmp dir must start with -tmp: {:?}", dir_tmp
	);
}

#[test]
fn resume_matching_and_breadcrumb_continue() {
	let temp = temp_dir("resume-test");

	// Filename matching
	let session_file = temp.join("2026-10-26T10-00-00-000Z_0192a5b0-7e44-7123-8abc-0123456789ab.jsonl");
	fs::write(&session_file, "{\"type\":\"session\",\"version\":3,\"id\":\"0192a5b0-7e44-7123-8abc-0123456789ab\",\"timestamp\":\"2026-10-26T10:00:00.000Z\",\"cwd\":\"/work\"}\n").expect("write");

	assert!(session_matches_resume_arg("0192a5b0-7e44-7123-8abc-0123456789ab", &session_file, "0192a5b0"));
	assert!(session_matches_resume_arg("0192a5b0-7e44-7123-8abc-0123456789ab", &session_file, "2026-10-26"));
	assert!(session_matches_resume_arg("0192a5b0-7e44-7123-8abc-0123456789ab", &session_file, "0192a"));
	assert!(!session_matches_resume_arg("0192a5b0-7e44-7123-8abc-0123456789ab", &session_file, ""));
	assert!(!session_matches_resume_arg("0192a5b0-7e44-7123-8abc-0123456789ab", &session_file, "nonexistent"));

	// Breadcrumb write / read
	let crumb_file = temp.join("test-term-id");
	write_terminal_breadcrumb(&PathBuf::from("/work"), &session_file, false, Some(&crumb_file)).expect("write crumb");
	let crumb = read_terminal_breadcrumb(Some(&crumb_file)).expect("read crumb");
	assert_eq!(crumb.cwd, PathBuf::from("/work"));
	assert_eq!(crumb.session_file, session_file);
	assert!(crumb.exists);
	assert!(!crumb.fresh);

	// Fresh breadcrumb with missing target file
	let missing_target = temp.join("never_created.jsonl");
	write_terminal_breadcrumb(&PathBuf::from("/work"), &missing_target, true, Some(&crumb_file)).expect("write fresh crumb");
	let fresh_crumb = read_terminal_breadcrumb(Some(&crumb_file)).expect("read fresh crumb");
	assert_eq!(fresh_crumb.session_file, missing_target);
	assert!(!fresh_crumb.exists);
	assert!(fresh_crumb.fresh);

	fs::remove_dir_all(temp).ok();
}

#[test]
fn fork_session_creates_new_session_with_parent_reference() {
	let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures/v3_session.jsonl");
	let temp = temp_dir("fork-test");
	let original = temp.join("original.jsonl");
	fs::copy(&fixture_path, &original).expect("copy");

	let target_dir = temp.join("forks");
	fs::create_dir_all(&target_dir).expect("create forks dir");

	let forked_path = fork_session(&original, None, Some(&target_dir))
		.expect("fork session");
	assert!(!forked_path.exists(), "fork writes SQLite, not a JSONL journal");
	assert!(target_dir.join("sessions.db").is_file());

	let forked_store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/test-project"),
		model: None,
		provider: None,
		dir: Some(target_dir.clone()),
		id: None,
		continue_latest: true,
		parent_session: None,
		no_title: false,
	});
	assert!(forked_store.enabled());
	let forked_header = forked_store.header().expect("forked header");
	assert_ne!(forked_header.id, "01a178ce-3463-7504-8047-ca4fab78a16b");
	assert_eq!(
		forked_header.parent_session.as_deref(),
		Some("01a178ce-3463-7504-8047-ca4fab78a16b")
	);
	assert_eq!(forked_store.history().len(), 1);

	fs::remove_dir_all(temp).ok();
}

#[test]
fn durable_fork_preserves_offbranch_tool_result_projection() {
	let temp = temp_dir("durable-sibling");
	let source_dir = temp.join("source"); let target_dir = temp.join("target"); fs::create_dir_all(&source_dir).expect("source");
	let mut store = SqliteStore::open(source_dir.join("sessions.db")).expect("store");
	let message = |id: &str, branch: &str, parent: Option<&str>, role: &str, content: &str| NewMessage { id: id.into(), session_id: "source".into(), branch_id: branch.into(), parent_message_id: parent.map(str::to_string), payload: json!({"type":"message","id":id,"parentId":parent,"timestamp":"2026-01-01T00:00:00Z","message":{"role":role,"content":content,"timestamp":1}}) };
	let user = message("m1", "active", None, "user", "request");
	let assistant = NewMessage { id: "a1".into(), session_id: "source".into(), branch_id: "active".into(), parent_message_id: Some("m1".into()), payload: json!({"type":"message","id":"a1","parentId":"m1","timestamp":"2026-01-01T00:00:01Z","message":{"role":"assistant","timestamp":2,"content":[{"type":"text","text":"call"},{"type":"toolCall","id":"c1","name":"one","arguments":{}},{"type":"toolCall","id":"c2","name":"two","arguments":{}}],"api":"openai-completions","provider":"test","model":"model","usage":{"input":1,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":2,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"toolUse"}}) };
	let result = |id: &str, branch: &str, content: &str, call: &str, parent: &str| NewMessage { id: id.into(), session_id: "source".into(), branch_id: branch.into(), parent_message_id: Some(parent.into()), payload: json!({"type":"message","id":id,"parentId":parent,"timestamp":"2026-01-01T00:00:02Z","message":{"role":"toolResult","toolCallId":call,"toolName":call,"content":[{"type":"text","text":content}],"isError":false,"timestamp":3}}) };
	store.create_fork_atomic(ForkWrite { session: NewSession { id: "source".into(), parent_session_id: None, payload: json!({"type":"session","version":3,"id":"source","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "active".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "active".into(), session_id: "source".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }, NewBranch { id: "sibling".into(), session_id: "source".into(), parent_branch_id: Some("active".into()), parent_message_id: Some("a1".into()), payload: json!({}) }, NewBranch { id: "unrelated".into(), session_id: "source".into(), parent_branch_id: None, parent_message_id: None, payload: json!({"name":"unrelated"}) }], messages: vec![user, assistant, result("r1", "active", "one", "c1", "a1"), result("r2", "sibling", "two", "c2", "a1"), message("u1", "unrelated", None, "user", "unrelated")], active_branch_id: "active".into(), active_leaf_message_id: Some("r1".into()), projection: Some(StoredProjection { kept: vec!["a1".into(), "r1".into(), "r2".into()], synthetic: vec![], anchor: Some("r1".into()) }) }).expect("seed"); drop(store);
	let forked = fork_session(&source_dir.join("source.jsonl"), None, Some(&target_dir)).expect("fork"); let id = forked.file_stem().unwrap().to_str().unwrap(); let target_db = target_dir.join("sessions.db"); let target = SqliteStore::open(&target_db).expect("target");
	let messages = target.session_messages(id).expect("messages"); assert_eq!(messages.len(), 4); assert!(messages.iter().any(|m| m.payload["message"]["content"].as_array().is_some_and(|parts| parts.iter().any(|part| part["text"] == json!("two"))))); assert!(messages.iter().any(|m| m.payload["message"]["role"] == json!("assistant") && m.payload["message"]["content"].is_array())); assert!(!messages.iter().any(|m| m.payload["message"]["content"] == json!("unrelated")));
	let projection = target.session(id).expect("session").expect("target").payload["durableProjection"].clone(); let anchor = messages.iter().find(|m| m.payload["message"]["role"] == json!("toolResult") && m.payload["message"]["toolCallId"] == json!("c1")).expect("active result"); assert_eq!(projection["anchor"], json!(anchor.id)); assert_eq!(projection["kept"].as_array().expect("kept").len(), 3);
	let source_store = SqliteStore::open(source_dir.join("sessions.db")).expect("source reopen"); let source_rows = source_store.session_messages("source").expect("source rows"); let source_projection = source_store.session("source").expect("source session").expect("source").payload["durableProjection"].clone(); let source_kept = source_projection["kept"].as_array().expect("source kept").iter().map(|id| source_rows.iter().find(|row| row.id == id.as_str().unwrap()).unwrap().payload.clone()).collect::<Vec<_>>(); let source_entries = source_kept.iter().map(|payload| serde_json::from_value(payload.clone()).expect("source entry")).collect::<Vec<aragent::session::MessageEntry>>(); let source_replay = aragent::runtime::history_messages_from_entries(&source_entries).expect("source projected replay"); let target_kept = projection["kept"].as_array().expect("target kept").iter().map(|id| messages.iter().find(|row| row.id == id.as_str().unwrap()).unwrap().payload.clone()).collect::<Vec<_>>(); let target_entries = target_kept.iter().map(|payload| serde_json::from_value(payload.clone()).expect("target entry")).collect::<Vec<aragent::session::MessageEntry>>(); let target_replay = aragent::runtime::history_messages_from_entries(&target_entries).expect("target projected replay"); assert_eq!(source_replay, target_replay); drop(target);
	let mut next = SqliteStore::open(&target_db).expect("reopen"); let leaf = anchor; next.append_message(NewMessage { id: "next".into(), session_id: id.into(), branch_id: leaf.branch_id.clone(), parent_message_id: Some(leaf.id.clone()), payload: json!({"type":"message","id":"next","parentId":leaf.id,"timestamp":"2026-01-01T00:00:03Z","message":{"role":"user","content":"next","timestamp":4}}) }).expect("next write"); assert!(next.session_messages(id).unwrap().iter().any(|m| m.id == "next"));
	fs::remove_dir_all(temp).ok();
}

#[test]
fn durable_fork_validates_synthetic_call_with_offbranch_result() {
	let temp = temp_dir("synthetic-call"); let source_dir = temp.join("source"); let target_dir = temp.join("target"); fs::create_dir_all(&source_dir).expect("source");
	let mut store = SqliteStore::open(source_dir.join("sessions.db")).expect("store");
	let assistant = NewMessage { id: "a1".into(), session_id: "source".into(), branch_id: "root".into(), parent_message_id: None, payload: json!({"type":"message","id":"a1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"c1","name":"tool","arguments":{}}],"timestamp":1,"api":"openai-completions","provider":"test","model":"model","stopReason":"toolUse"}}) };
	let result = NewMessage { id: "r1".into(), session_id: "source".into(), branch_id: "sibling".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"r1","parentId":"a1","timestamp":"2026-01-01T00:00:01Z","message":{"role":"toolResult","toolCallId":"c1","toolName":"tool","content":[{"type":"text","text":"ok"}],"isError":false,"timestamp":2}}) };
	let synthetic_call = serde_json::to_value(pi_agent::Message::assistant_with_tools("prefix", vec![pi_agent::ToolCall { id: "c1".into(), name: "tool".into(), arguments: json!({}) }])).expect("runtime synthetic");
	store.create_fork_atomic(ForkWrite { session: NewSession { id: "source".into(), parent_session_id: None, payload: json!({"type":"session","version":3,"id":"source","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "active".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "root".into(), session_id: "source".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }, NewBranch { id: "active".into(), session_id: "source".into(), parent_branch_id: Some("root".into()), parent_message_id: Some("a1".into()), payload: json!({}) }, NewBranch { id: "sibling".into(), session_id: "source".into(), parent_branch_id: Some("root".into()), parent_message_id: Some("a1".into()), payload: json!({}) }], messages: vec![assistant, result], active_branch_id: "active".into(), active_leaf_message_id: Some("a1".into()), projection: Some(StoredProjection { kept: vec!["r1".into()], synthetic: vec![synthetic_call], anchor: None }) }).expect("seed"); drop(store);
	let forked = fork_session(&source_dir.join("source.jsonl"), None, Some(&target_dir)).expect("fork"); let id = forked.file_stem().unwrap().to_str().unwrap(); let target = SqliteStore::open(target_dir.join("sessions.db")).expect("target"); let session = target.session(id).expect("session").expect("fork"); let projection = serde_json::from_value::<StoredProjection>(session.payload["durableProjection"].clone()).expect("projection"); let rows = target.session_messages(id).expect("rows"); let kept = projection.kept.iter().map(|id| serde_json::from_value::<MessageEntry>(rows.iter().find(|row| row.id == *id).expect("kept row").payload.clone()).expect("entry")).collect::<Vec<_>>(); let mut replay = projection.synthetic.iter().map(|value| serde_json::from_value::<pi_agent::Message>(value.clone()).expect("synthetic")).collect::<Vec<_>>(); replay.extend(aragent::runtime::history_messages_from_entries(&kept).expect("kept replay")); assert!(aragent::durable_session::validate_runtime_replay(&replay).is_ok());
	fs::remove_dir_all(temp).ok();
}

#[test]
fn durable_fork_rejects_projected_tool_result_without_matching_ancestor() {
	let temp = temp_dir("synthetic-unmatched"); let source_dir = temp.join("source"); let target_dir = temp.join("target"); fs::create_dir_all(&source_dir).expect("source");
	let mut store = SqliteStore::open(source_dir.join("sessions.db")).expect("store");
	let result = NewMessage { id: "r1".into(), session_id: "source".into(), branch_id: "sibling".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"r1","parentId":"a1","timestamp":"2026-01-01T00:00:01Z","message":{"role":"toolResult","toolCallId":"real","toolName":"tool","content":"ok","isError":false,"timestamp":2}}) };
	let assistant = NewMessage { id: "a1".into(), session_id: "source".into(), branch_id: "root".into(), parent_message_id: None, payload: json!({"type":"message","id":"a1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"real","name":"tool","arguments":{}}],"timestamp":1,"api":"openai-completions","provider":"test","model":"model","stopReason":"toolUse"}}) };
	store.create_fork_atomic(ForkWrite { session: NewSession { id: "source".into(), parent_session_id: None, payload: json!({"type":"session","version":3,"id":"source","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "active".into(), initial_branch_payload: json!({}) }, branches: vec![NewBranch { id: "root".into(), session_id: "source".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }, NewBranch { id: "active".into(), session_id: "source".into(), parent_branch_id: Some("root".into()), parent_message_id: Some("a1".into()), payload: json!({}) }, NewBranch { id: "sibling".into(), session_id: "source".into(), parent_branch_id: Some("root".into()), parent_message_id: Some("a1".into()), payload: json!({}) }], messages: vec![assistant, result], active_branch_id: "active".into(), active_leaf_message_id: Some("a1".into()), projection: None }).expect("valid source seed");
	store.update_session_payload("source", json!({"type":"session","version":3,"id":"source","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp","durableProjection":{"kept":["r1"],"synthetic":[{"role":"assistant","content":"prefix","tool_calls":[{"id":"wrong","name":"tool","arguments":{}}]}],"anchor":null}})).expect("corrupt projection only"); drop(store);
	assert!(fork_session(&source_dir.join("source.jsonl"), None, Some(&target_dir)).is_err());
	let target_db = target_dir.join("sessions.db"); if target_db.exists() { let target = SqliteStore::open(target_db).expect("target store"); assert!(target.list_sessions().expect("sessions").iter().all(|session| session.id != "source")); }
	fs::remove_dir_all(temp).ok();
}

#[test]
fn fork_of_fork_retains_immutable_unknown_provenance_after_source_outcome_change() {
	let temp = temp_dir("provenance-transitive");
	let source_dir = temp.join("source");
	let first_dir = temp.join("first");
	let second_dir = temp.join("second");
	fs::create_dir_all(&source_dir).expect("source");
	let mut store = SqliteStore::open(source_dir.join("sessions.db")).expect("store");
	store.create_session(NewSession { id: "source".into(), parent_session_id: None, payload: json!({"type":"session","version":3,"id":"source","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "source.main".into(), initial_branch_payload: json!({}) }).expect("session");
	store.append_message(NewMessage { id: "a1".into(), session_id: "source".into(), branch_id: "source.main".into(), parent_message_id: None, payload: json!({"type":"message","id":"a1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"shared","name":"bash"}],"timestamp":1}}) }).expect("assistant");
	let exec = store.record_intent(NewIntent { session_id: "source".into(), branch_id: "source.main".into(), provider_call_id: "shared".into(), tool_name: "bash".into(), payload: json!({}) }).expect("intent");
	store.append_message(NewMessage { id: "r1".into(), session_id: "source".into(), branch_id: "source.main".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"r1","parentId":"a1","timestamp":"2026-01-01T00:00:01Z","message":{"role":"toolResult","toolCallId":"shared","toolName":"bash","content":"interrupted","isError":true,"timestamp":2,"details":{"executionId":exec}}}) }).expect("result");
	drop(store);

	let first = fork_session(&source_dir.join("source.jsonl"), None, Some(&first_dir)).expect("first fork");
	let first_id = first.file_stem().unwrap().to_str().unwrap().to_string();
	let mut first_store = SqliteStore::open(first_dir.join("sessions.db")).expect("first store");
	assert!(!first_store.load_session_snapshot(&first_id).expect("first snapshot").provenance[0].outcome_resolved);
	drop(first_store);

	let mut store = SqliteStore::open(source_dir.join("sessions.db")).expect("source reopen");
	store.record_outcome(&exec, json!({"content":"done","isError":false})).expect("late source outcome");
	drop(store);

	let second = fork_session(&first_dir.join(format!("{first_id}.jsonl")), None, Some(&second_dir)).expect("fork of fork");
	let second_id = second.file_stem().unwrap().to_str().unwrap().to_string();
	let mut second_store = SqliteStore::open(second_dir.join("sessions.db")).expect("second store");
	let provenance = second_store.load_session_snapshot(&second_id).expect("second snapshot").provenance;
	assert_eq!(provenance.len(), 1, "transitive fork keeps exactly one classification");
	assert!(!provenance[0].outcome_resolved, "late source resolution must not mutate the fork classification");
	assert!(second_store.unresolved_intents(&second_id).expect("unresolved").is_empty());

	let branch = second_store.session(&second_id).expect("session").expect("fork").active_branch_id.expect("branch");
	let result_row = second_store.session_messages(&second_id).expect("rows").into_iter().find(|row| row.payload["message"]["role"] == json!("toolResult")).expect("result");
	second_store.append_message(NewMessage { id: "a2".into(), session_id: second_id.clone(), branch_id: branch.clone(), parent_message_id: Some(result_row.id), payload: json!({"type":"message","id":"a2","message":{"role":"assistant","content":[{"type":"toolCall","id":"shared","name":"bash"}]}}) }).expect("reuse");
	let new_exec = second_store.record_intent(NewIntent { session_id: second_id.clone(), branch_id: branch.clone(), provider_call_id: "shared".into(), tool_name: "bash".into(), payload: json!({}) }).expect("new intent");
	let error = second_store.recover_unresolved_intents(&second_id).expect_err("transitive unknown provenance stays non-operational");
	assert!(error.to_string().contains("not uniquely attributable"), "{error}");
	assert_eq!(second_store.outcome(&new_exec).expect("outcome"), None);

	let same = fork_session(&first_dir.join(format!("{first_id}.jsonl")), None, Some(&first_dir)).expect("same-db fork of fork");
	let same_id = same.file_stem().unwrap().to_str().unwrap().to_string();
	let mut first_store = SqliteStore::open(first_dir.join("sessions.db")).expect("first store");
	let same_provenance = first_store.load_session_snapshot(&same_id).expect("same snapshot").provenance;
	assert_eq!(same_provenance.len(), 1);
	assert!(!same_provenance[0].outcome_resolved, "same-db fork of fork keeps the immutable classification");
	drop(first_store);

	// Fork of fork into the original source database: the late-resolved source
	// execution row is owned by the grandparent, so the immutable unknown
	// classification still wins.
	let into_source = fork_session(&first_dir.join(format!("{first_id}.jsonl")), None, Some(&source_dir)).expect("fork of fork into source db");
	let into_source_id = into_source.file_stem().unwrap().to_str().unwrap().to_string();
	let source_store = SqliteStore::open(source_dir.join("sessions.db")).expect("source store");
	let into_source_provenance = source_store.load_session_snapshot(&into_source_id).expect("source-db snapshot").provenance;
	assert_eq!(into_source_provenance.len(), 1);
	assert!(!into_source_provenance[0].outcome_resolved, "late source resolution must not mutate a transitive fork");
	fs::remove_dir_all(temp).ok();
}

#[test]
fn fork_rejects_message_referencing_missing_execution_before_publication() {
	let temp = temp_dir("provenance-missing");
	let source_dir = temp.join("source");
	let target_dir = temp.join("target");
	fs::create_dir_all(&source_dir).expect("source");
	let mut store = SqliteStore::open(source_dir.join("sessions.db")).expect("store");
	store.create_session(NewSession { id: "source".into(), parent_session_id: None, payload: json!({"type":"session","version":3,"id":"source","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "source.main".into(), initial_branch_payload: json!({}) }).expect("session");
	store.append_message(NewMessage { id: "a1".into(), session_id: "source".into(), branch_id: "source.main".into(), parent_message_id: None, payload: json!({"type":"message","id":"a1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"shared","name":"bash"}],"timestamp":1}}) }).expect("assistant");
	store.append_message(NewMessage { id: "r1".into(), session_id: "source".into(), branch_id: "source.main".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"r1","parentId":"a1","timestamp":"2026-01-01T00:00:01Z","message":{"role":"toolResult","toolCallId":"shared","toolName":"bash","content":"x","isError":false,"timestamp":2,"details":{"executionId":"exec-missing"}}}) }).expect("dangling result");
	drop(store);

	let error = fork_session(&source_dir.join("source.jsonl"), None, Some(&target_dir)).expect_err("missing provenance must fail closed");
	assert!(error.to_string().contains("unknown execution"), "{error}");
	assert!(!target_dir.exists() || fs::read_dir(&target_dir).expect("target dir").next().is_none(), "failed preflight must not publish artifacts");
	fs::remove_dir_all(temp).ok();
}

#[test]
fn fork_prunes_sibling_branch_with_dangling_execution_reference() {
	let temp = temp_dir("provenance-pruned");
	let source_dir = temp.join("source");
	let target_dir = temp.join("target");
	fs::create_dir_all(&source_dir).expect("source");
	let mut store = SqliteStore::open(source_dir.join("sessions.db")).expect("store");
	store.create_session(NewSession { id: "source".into(), parent_session_id: None, payload: json!({"type":"session","version":3,"id":"source","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "source.main".into(), initial_branch_payload: json!({}) }).expect("session");
	store.append_message(NewMessage { id: "a1".into(), session_id: "source".into(), branch_id: "source.main".into(), parent_message_id: None, payload: json!({"type":"message","id":"a1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"one","timestamp":1}}) }).expect("active message");
	store.create_branch(NewBranch { id: "source.sibling".into(), session_id: "source".into(), parent_branch_id: None, parent_message_id: None, payload: json!({}) }).expect("sibling branch");
	store.append_message(NewMessage { id: "pruned".into(), session_id: "source".into(), branch_id: "source.sibling".into(), parent_message_id: None, payload: json!({"type":"message","id":"pruned","timestamp":"2026-01-01T00:00:00Z","message":{"role":"toolResult","toolCallId":"kept","toolName":"bash","content":"x","isError":false,"timestamp":2,"details":{"executionId":"exec-missing"}}}) }).expect("sibling result");
	drop(store);

	let forked = fork_session(&source_dir.join("source.jsonl"), None, Some(&target_dir)).expect("fork prunes the unrelated branch");
	let fork_id = forked.file_stem().unwrap().to_str().unwrap().to_string();
	let mut target = SqliteStore::open(target_dir.join("sessions.db")).expect("target");
	let snapshot = target.load_session_snapshot(&fork_id).expect("fork snapshot");
	assert!(snapshot.provenance.is_empty());
	let rows = target.session_messages(&fork_id).expect("rows");
	assert_eq!(rows.len(), 1, "pruned sibling message must not be copied");
	assert_eq!(rows[0].payload["id"], json!(rows[0].id));
	fs::remove_dir_all(temp).ok();
}

#[test]
fn fork_remaps_legacy_anchor_alias() {
	let temp = temp_dir("legacy-anchor"); let source_dir = temp.join("source"); let target_dir = temp.join("target"); fs::create_dir_all(&source_dir).expect("source");
	let mut store = SqliteStore::open(source_dir.join("sessions.db")).expect("store");
	store.create_session(NewSession { id: "source".into(), parent_session_id: None, payload: json!({"type":"session","version":3,"id":"source","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "active".into(), initial_branch_payload: json!({}) }).expect("session");
	store.append_message(NewMessage { id: "m1".into(), session_id: "source".into(), branch_id: "active".into(), parent_message_id: None, payload: json!({"type":"message","id":"m1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"one","timestamp":1}}) }).expect("message");
	store.update_session_payload("source", json!({"type":"session","version":3,"id":"source","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp","durableProjection":{"firstKeptEntryId":"m1"}})).expect("legacy anchor"); drop(store);
	let forked = fork_session(&source_dir.join("source.jsonl"), None, Some(&target_dir)).expect("fork");
	let id = forked.file_stem().unwrap().to_str().unwrap();
	let target = SqliteStore::open(target_dir.join("sessions.db")).expect("target");
	let projection = &target.session(id).expect("session").expect("fork").payload["durableProjection"];
	let anchor = projection["anchor"].as_str().expect("legacy alias must surface as anchor");
	assert_ne!(anchor, "m1", "anchor must be remapped to the fork id space");
	let rows = target.session_messages(id).expect("rows");
	assert!(rows.iter().any(|row| row.id == anchor && row.payload["message"]["content"] == json!("one")), "anchor must point at the fork's copied message");
	assert_eq!(projection["kept"], json!([]));
	fs::remove_dir_all(temp).ok();
}

#[test]
fn moved_worktree_continue_reroots_and_resume_prefix_matches() {
	let temp = temp_dir("moved-wt");
	let sessions_dir = temp.join("sessions");
	let old_cwd = temp.join("old-project");
	let new_cwd = temp.join("new-project");
	fs::create_dir_all(&sessions_dir).expect("create sessions dir");
	fs::create_dir_all(&old_cwd).expect("create old cwd");
	fs::create_dir_all(&new_cwd).expect("create new cwd");

	let id = "0192a5b0-7e44-7123-8abc-0123456789ab";
	let mut seed = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: old_cwd.clone(),
		model: None,
		provider: None,
		dir: Some(sessions_dir.clone()),
		id: Some(id.to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});
	assert!(seed.enabled(), "seed durable session");
	seed.set_title("Moved", Some("auto")).expect("title");
	let session_file = sessions_dir.join(format!("{id}.jsonl"));
	drop(seed);

	let crumb = temp.join("term-crumb");
	write_terminal_breadcrumb(&old_cwd, &session_file, false, Some(&crumb)).expect("write crumb");

	// The worktree moved away: the recorded cwd is gone.
	fs::remove_dir_all(&old_cwd).expect("remove old cwd");

	let resumed = continue_recent_with_breadcrumb(&new_cwd, Some(&sessions_dir), Some(&crumb))
		.expect("continue")
		.expect("session re-rooted");
	assert_eq!(resumed, session_file, "same-dir move keeps the identity path");

	// Header cwd is re-rooted to the new worktree (TS `moveTo` semantics).
	let rerooted = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: new_cwd.clone(),
		model: None,
		provider: None,
		dir: Some(sessions_dir.clone()),
		id: Some(id.to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: true,
	});
	let rerooted = PathBuf::from(rerooted.header().expect("header").cwd.clone());
	assert_eq!(
		fs::canonicalize(&rerooted).unwrap_or_else(|_| rerooted.clone()),
		fs::canonicalize(&new_cwd).expect("canonical new cwd"),
		"header cwd must be re-rooted to the moved worktree"
	);

	// Resume selector matches the moved session by id prefix.
	let resolved = resolve_resumable_session("0192a5b0", &new_cwd, Some(&sessions_dir), false)
		.expect("resolve")
		.expect("prefix match");
	assert_eq!(resolved, session_file);

	fs::remove_dir_all(temp).ok();
}

#[test]
fn breadcrumb_resumes_durable_session_after_legacy_file_deletion() {
	let temp = temp_dir("breadcrumb-delete-legacy");
	let sessions_dir = temp.join("sessions");
	let cwd = temp.join("project");
	fs::create_dir_all(&sessions_dir).expect("sessions dir");
	fs::create_dir_all(&cwd).expect("cwd");

	let session_id = "0192a5b0-7e44-7123-8abc-0123456789ab";
	let legacy_filename = format!("2026-03-31T08-00-00-000Z_{session_id}.jsonl");
	let legacy_path = sessions_dir.join(&legacy_filename);
	let content = format!(
		r#"{{"type":"session","version":3,"id":"{session_id}","timestamp":"2026-03-31T08:00:00.000Z","cwd":"{}"}}"#,
		cwd.display()
	);
	fs::write(&legacy_path, format!("{content}\n")).expect("write legacy file");

	// 1. Explicit import into SQLite store
	import_v3_session(&legacy_path, &sessions_dir).expect("import into SQLite");

	// 2. Terminal breadcrumb points to the timestamped legacy path
	let crumb = temp.join("term-crumb");
	write_terminal_breadcrumb(&cwd, &legacy_path, false, Some(&crumb)).expect("write crumb");

	// 3. Legacy file is deleted from disk
	fs::remove_file(&legacy_path).expect("delete legacy file");
	assert!(!legacy_path.exists());

	// 4. read_terminal_breadcrumb recognizes session in SQLite and normalizes to identity path
	let breadcrumb = read_terminal_breadcrumb(Some(&crumb)).expect("read breadcrumb");
	assert!(breadcrumb.exists);
	let expected_identity = sessions_dir.join(format!("{session_id}.jsonl"));
	assert_eq!(
		breadcrumb.session_file, expected_identity,
		"breadcrumb must be normalized to durable identity path"
	);

	// 5. continue_recent_with_breadcrumb resumes the imported session
	let continued = continue_recent_with_breadcrumb(&cwd, Some(&sessions_dir), Some(&crumb))
		.expect("continue")
		.expect("session found");
	assert_eq!(continued, expected_identity);

	// 6. SessionStore opens the resumed session cleanly
	let store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: cwd.clone(),
		model: None,
		provider: None,
		dir: Some(sessions_dir.clone()),
		id: Some(session_id.to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: true,
	});
	assert!(store.enabled());
	assert_eq!(store.id(), session_id);

	fs::remove_dir_all(temp).ok();
}

#[test]
fn breadcrumb_exact_stem_takes_priority_over_timestamp_stripped_id() {
	let temp = temp_dir("breadcrumb-exact-stem");
	let sessions_dir = temp.join("sessions");
	let cwd = temp.join("project");
	fs::create_dir_all(&sessions_dir).expect("sessions dir");
	fs::create_dir_all(&cwd).expect("cwd");

	let stripped_id = "0192a5b0-test-stripped";
	let exact_stem_id = format!("2026-03-31T08-00-00-000Z_{stripped_id}");

	// Create both sessions in the durable SQLite database
	let store_stripped = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: cwd.clone(),
		model: None,
		provider: None,
		dir: Some(sessions_dir.clone()),
		id: Some(stripped_id.to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: true,
	});
	assert!(store_stripped.enabled());
	drop(store_stripped);

	let store_exact = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: cwd.clone(),
		model: None,
		provider: None,
		dir: Some(sessions_dir.clone()),
		id: Some(exact_stem_id.clone()),
		continue_latest: false,
		parent_session: None,
		no_title: true,
	});
	assert!(store_exact.enabled());
	drop(store_exact);

	// Write breadcrumb pointing to the exact stem file name (virtual file, does not exist on disk)
	let breadcrumb_file_path = sessions_dir.join(format!("{exact_stem_id}.jsonl"));
	let crumb = temp.join("term-crumb");
	write_terminal_breadcrumb(&cwd, &breadcrumb_file_path, false, Some(&crumb)).expect("write crumb");

	// read_terminal_breadcrumb must match the exact stem session ID, not strip it
	let breadcrumb = read_terminal_breadcrumb(Some(&crumb)).expect("read breadcrumb");
	assert!(breadcrumb.exists);
	assert_eq!(
		breadcrumb.session_file, breadcrumb_file_path,
		"exact stem ID must take priority when both exact and stripped IDs exist in SQLite"
	);

	fs::remove_dir_all(temp).ok();
}

#[test]
fn append_turn_titles_and_appends_entries_and_reopens_cleanly() {
	let temp = temp_dir("append-test");
	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/append-test"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("01a178ce-append-test".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});
	assert!(store.enabled());
	let file = store.file().expect("file identity exists").to_path_buf();
	let messages_before = store.messages().len();
	assert!(!file.exists(), "durable sessions do not create a JSONL file");

	// First turn auto-titles the session slot and appends entries.
	store
		.append_turn(&TurnRecord {
			prompt: "initial prompt".to_string(),
			assistant: "initial reply".to_string(),
			reasoning: String::new(),
			reasoning_after_text: false,
			stop_reason: "end_turn".to_string(),
			tool_calls: Vec::new(),
			parts: Vec::new(),
		})
		.expect("initial append succeeds");

	assert_eq!(store.title_slot().expect("title slot").title, "initial prompt");
	assert_eq!(store.header().expect("header").title.as_deref(), Some("initial prompt"));
	assert!(store.messages().len() > messages_before);
	assert!(!file.exists(), "durable sessions never create a JSONL journal");
	drop(store);

	// Reopening loads the title slot and turn history cleanly.
	let reopened = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/append-test"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("01a178ce-append-test".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});
	assert!(reopened.enabled());
	assert_eq!(reopened.title_slot().expect("title slot").title, "initial prompt");
	assert_eq!(reopened.history().len(), 1);
	assert_eq!(reopened.history()[0].prompt, "initial prompt");
	assert_eq!(reopened.history()[0].assistant, "initial reply");

	fs::remove_dir_all(temp).ok();
}

#[test]
fn slotless_v3_log_imports_read_only_and_reopens_untitled() {
	let temp = temp_dir("slotless-append");
	let file = temp.join("2026-10-26T12-00-00-000Z_01a178ce-slotless.jsonl");
	let header_line = r#"{"type":"session","version":3,"id":"01a178ce-slotless","timestamp":"2026-10-26T12:00:00.000Z","cwd":"/work/slotless"}"#;
	fs::write(&file, format!("{header_line}\n")).expect("write slotless v3 log");
	let original = fs::read(&file).expect("read original bytes");

	let options = SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/slotless"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("01a178ce-slotless".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	};
	let mut legacy = SessionStore::open(&options);
	assert!(legacy.enabled());
	assert!(legacy.legacy_read_only(), "an old log is a read-only view");
	assert!(legacy.title_slot().is_none(), "slotless v3 log has no leading slot");
	assert!(
		legacy
			.append_turn(&TurnRecord {
				prompt: "must not overwrite the header".to_string(),
				assistant: "reply".to_string(),
				reasoning: String::new(),
				reasoning_after_text: false,
				stop_reason: "stop".to_string(),
				tool_calls: Vec::new(),
				parts: Vec::new(),
			})
			.is_err(),
		"legacy JSONL is read-only until explicitly imported"
	);

	// The old file is byte-identical: migration never mutates it.
	assert_eq!(fs::read(&file).expect("read legacy"), original);
	drop(legacy);

	// Explicit import creates the SQLite authority; no slot is invented.
	import_v3_session(&file, &temp).expect("import slotless v3");
	let mut store = SessionStore::open(&SessionOptions { no_title: true, ..options.clone() });
	assert!(store.enabled());
	assert!(store.title_slot().is_none(), "import must not create a slot");
	store
		.append_turn(&TurnRecord {
			prompt: "must not overwrite the header".to_string(),
			assistant: "reply".to_string(),
			reasoning: String::new(),
			reasoning_after_text: false,
			stop_reason: "stop".to_string(),
			tool_calls: Vec::new(),
			parts: Vec::new(),
		})
		.expect("append to imported slotless session");
	drop(store);

	let reopened = SessionStore::open(&SessionOptions { no_title: true, ..options });
	assert!(reopened.enabled(), "imported session must reopen");
	assert_eq!(reopened.history().len(), 1, "appended turn must remain readable");
	assert_eq!(reopened.history()[0].prompt, "must not overwrite the header");
	assert_eq!(reopened.history()[0].assistant, "reply");
	assert!(reopened.header().expect("header").title.is_none());
	assert_eq!(fs::read(&file).expect("read legacy"), original);

	fs::remove_dir_all(temp).ok();
}

#[test]
fn overwrite_title_slot_rolled_back_updates_slot_and_preserves_length() {
	let temp = temp_dir("title-update-test");
	let file = temp.join("01a178ce-test.jsonl");

	let initial_slot = serialize_title_slot("Initial Title", Some("auto"), "2026-10-26T12:00:00.000Z");
	let header_line = r#"{"type":"session","version":3,"id":"01a178ce-test","timestamp":"2026-10-26T12:00:00.000Z","cwd":"/work/test"}"#;
	let mut init = Vec::new();
	init.extend_from_slice(initial_slot.as_bytes());
	init.extend_from_slice(format!("{header_line}\n").as_bytes());
	fs::write(&file, &init).expect("write initial");

	assert_eq!(initial_slot.as_bytes().len(), 256);
	let total_len = init.len();

	let store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/test"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("01a178ce-test".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});
	assert!(store.enabled());
	assert_eq!(store.title_slot().expect("slot").title, "Initial Title");

	// Update title using overwrite_title_slot_rolled_back
	let updated_slot = serialize_title_slot("Updated Title", Some("user"), "2026-10-26T13:00:00.000Z");
	overwrite_title_slot_rolled_back(&file, &updated_slot).expect("update slot");

	// Verify total file length is strictly preserved and content parses
	assert_eq!(fs::metadata(&file).expect("stat").len() as usize, total_len);
	let content = fs::read_to_string(&file).expect("read");
	let parsed = parse_title_slot(content.lines().next().unwrap()).expect("parse");
	assert_eq!(parsed.title, "Updated Title");

	// Reopen with SessionStore verifies new title is loaded
	let reopened = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/test"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("01a178ce-test".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});
	assert!(reopened.enabled());
	assert_eq!(reopened.title_slot().expect("slot").title, "Updated Title");

	fs::remove_dir_all(temp).ok();
}

#[test]
fn unsupported_session_version_rejected_without_append() {
	let temp = temp_dir("bad-version");
	let file = temp.join("unsupported-v4.jsonl");

	let initial_slot = serialize_title_slot("", Some("auto"), "2026-10-26T12:00:00.000Z");
	let header_line = r#"{"type":"session","version":4,"id":"unsupported-v4","timestamp":"2026-10-26T12:00:00.000Z","cwd":"/work/test"}"#;
	let mut init = Vec::new();
	init.extend_from_slice(initial_slot.as_bytes());
	init.extend_from_slice(format!("{header_line}\n").as_bytes());
	fs::write(&file, &init).expect("write bad version file");

	let original = fs::read_to_string(&file).expect("read");

	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/test"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("unsupported-v4".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});

	assert!(!store.enabled(), "store must be disabled for unsupported session version");
	let warnings = store.take_warnings();
	assert!(
		warnings.iter().any(|w| w.contains("unsupported session version `4`")),
		"must report warning about unsupported session version 4: {warnings:?}"
	);

	let turn = TurnRecord {
		prompt: "hi".into(),
		assistant: "hello".into(),
		reasoning: String::new(),
		reasoning_after_text: false,
		stop_reason: "stop".into(),
		tool_calls: vec![],
		parts: vec![],
	};
	let _ = store.append_turn(&turn);
	assert_eq!(
		fs::read_to_string(&file).expect("read"),
		original,
		"file must never be modified when session version is unsupported"
	);

	fs::remove_dir_all(temp).ok();
}

#[test]
fn missing_session_version_rejected_without_append() {
	let temp = temp_dir("missing-version");
	let file = temp.join("missing-ver.jsonl");

	let initial_slot = serialize_title_slot("", Some("auto"), "2026-10-26T12:00:00.000Z");
	let header_line = r#"{"type":"session","id":"missing-ver","timestamp":"2026-10-26T12:00:00.000Z","cwd":"/work/test"}"#;
	let mut init = Vec::new();
	init.extend_from_slice(initial_slot.as_bytes());
	init.extend_from_slice(format!("{header_line}\n").as_bytes());
	fs::write(&file, &init).expect("write missing version file");

	let original = fs::read_to_string(&file).expect("read");

	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/test"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("missing-ver".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});

	assert!(!store.enabled(), "store must be disabled when session version is missing");
	let warnings = store.take_warnings();
	assert!(
		warnings.iter().any(|w| w.contains("unsupported session version `missing`")),
		"must report warning about missing session version: {warnings:?}"
	);

	let turn = TurnRecord {
		prompt: "hi".into(),
		assistant: "hello".into(),
		reasoning: String::new(),
		reasoning_after_text: false,
		stop_reason: "stop".into(),
		tool_calls: vec![],
		parts: vec![],
	};
	let _ = store.append_turn(&turn);
	assert_eq!(
		fs::read_to_string(&file).expect("read"),
		original,
		"file must never be modified when session version is missing"
	);

	fs::remove_dir_all(temp).ok();
}

#[test]
fn stop_reason_canonicalized_in_persisted_v3_log() {
	let temp = temp_dir("canon-stop-reason");
	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/canon"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("canon-stop-test".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});
	assert!(store.enabled());
	let file = store.file().expect("file exists").to_path_buf();

	// Turn 1 with legacy/variant stop reason "end_turn"
	store
		.append_turn(&TurnRecord {
			prompt: "first prompt".to_string(),
			assistant: "first reply".to_string(),
			reasoning: String::new(),
			reasoning_after_text: false,
			stop_reason: "end_turn".to_string(),
			tool_calls: Vec::new(),
			parts: Vec::new(),
		})
		.expect("append turn 1");

	// Turn 2 with legacy/variant stop reason "tool_use"
	store
		.append_turn(&TurnRecord {
			prompt: "second prompt".to_string(),
			assistant: "second reply".to_string(),
			reasoning: String::new(),
			reasoning_after_text: false,
			stop_reason: "tool_use".to_string(),
			tool_calls: vec![ToolCallRecord {
				id: "call-1".to_string(),
				name: "read".to_string(),
				arguments: serde_json::json!({"path": "foo"}),
				result: "contents".to_string(),
				is_error: false,
				result_parts: Vec::new(),
				details: None,
			}],
			parts: Vec::new(),
		})
		.expect("append turn 2");

	assert!(!file.exists(), "durable sessions do not write a JSONL log");
	let stored: Vec<&str> = store
		.messages()
		.iter()
		.filter(|entry| entry.message.role == "assistant")
		.filter_map(|entry| entry.message.stop_reason.as_deref())
		.collect();
	assert_eq!(stored, vec!["stop", "toolUse"]);
	drop(store);

	// Reopen and verify resumed history records canonical stop reasons
	let reopened = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/canon"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("canon-stop-test".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});
	assert!(reopened.enabled());
	let hist = reopened.history();
	assert_eq!(hist.len(), 2);
	assert_eq!(hist[0].stop_reason, "stop");
	assert_eq!(hist[1].stop_reason, "toolUse");

	fs::remove_dir_all(temp).ok();
}

#[test]
fn session_storage_preserves_and_replays_tool_result_images() {
	let temp = temp_dir("tool-img");

	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/img"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("tool-img-test".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});
	assert!(store.enabled());

	store
		.append_turn(&TurnRecord {
			prompt: "capture screen".to_string(),
			assistant: "calling screenshot".to_string(),
			reasoning: String::new(),
			reasoning_after_text: false,
			stop_reason: "stop".to_string(),
			tool_calls: vec![ToolCallRecord {
				id: "call-sc-1".to_string(),
				name: "screenshot".to_string(),
				arguments: serde_json::json!({}),
				result: "Captured screen".to_string(),
				is_error: false,
				result_parts: vec![
					aragent::session::ContentPart::text("Captured screen"),
					aragent::session::ContentPart::image("aW1nMQ==", "image/png"),
				],
				details: None,
			}],
			parts: Vec::new(),
		})
		.expect("append turn with tool image");

	let tool = store
		.messages()
		.iter()
		.find(|entry| entry.message.role == "toolResult")
		.expect("persisted tool result");
	let parts = tool.message.content.parts();
	assert!(parts.iter().any(|part| part.is_image()
		&& part.data.as_deref() == Some("aW1nMQ==")
		&& part.mime_type.as_deref() == Some("image/png")));
	drop(store);

	// Reopen session and check history
	let reopened = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/img"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some("tool-img-test".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: false,
	});
	assert!(reopened.enabled());
	let hist = reopened.history();
	assert_eq!(hist.len(), 1);
	assert_eq!(hist[0].tool_calls.len(), 1);
	assert_eq!(hist[0].tool_calls[0].result_parts.len(), 2);
	assert_eq!(hist[0].tool_calls[0].result, "Captured screen");
	assert_eq!(hist[0].tool_calls[0].result_parts[1].data.as_deref(), Some("aW1nMQ=="));

	// Replay through runtime history messages and wire conversion
	let msgs = aragent::runtime::history_messages(&hist).expect("history messages");
	assert_eq!(msgs.len(), 3);
	assert_eq!(msgs[2].role, pi_agent::Role::Tool);
	assert_eq!(msgs[2].parts.len(), 2);

	let wire = pi_agent::to_openai_messages(&msgs).expect("to wire");
	assert_eq!(wire.len(), 4);
	assert_eq!(wire[2].role, "tool");
	assert_eq!(wire[2].content, Some(pi_agent::OpenAiContent::Text("Captured screen".to_string())));
	assert_eq!(wire[3].role, "user");
	if let Some(pi_agent::OpenAiContent::Parts(parts)) = &wire[3].content {
		assert_eq!(parts.len(), 2);
		assert_eq!(parts[0], pi_agent::OpenAiContentPart::Text {
			text: "Attached image(s) from tool result:".to_string(),
		});
		assert_eq!(parts[1], pi_agent::OpenAiContentPart::ImageUrl {
			image_url: pi_agent::OpenAiImageUrl {
				url: "data:image/png;base64,aW1nMQ==".to_string(),
			},
		});
	} else {
		panic!("expected OpenAiContent::Parts");
	}

	fs::remove_dir_all(temp).ok();
}

#[test]
fn branch_to_and_parent_lineage_survive_reload() {
	let dir = temp_dir("branch-to");
	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work"),
		model: None,
		provider: None,
		dir: Some(dir.clone()),
		id: Some("branch-test".to_string()),
		continue_latest: false,
		parent_session: Some("parent-session".to_string()),
		no_title: true,
	});
	assert!(store.enabled(), "store must be enabled: {:?}", store.take_warnings());
	assert_eq!(
		store.header().expect("header").parent_session.as_deref(),
		Some("parent-session"),
		"parent lineage must be persisted at creation"
	);

	store
		.append_message(&MessageEntry::new(
			"msg-1",
			None,
			"2026-10-29T10:00:00.000Z",
			SessionMessage::user(MessageContent::Text("first".to_string()), 1),
		))
		.expect("append first");
	store
		.append_message(&MessageEntry::new(
			"msg-2",
			Some("msg-1".to_string()),
			"2026-10-29T10:00:01.000Z",
			SessionMessage::user(MessageContent::Text("second".to_string()), 2),
		))
		.expect("append second");

	let marker = store.branch_to("msg-1").expect("branch to an existing entry");
	assert!(store.has_entry(&marker), "branch marker must be persisted");
	assert_eq!(store.leaf_id(), Some(marker.as_str()), "marker is the new leaf");
	assert!(
		store.has_branch_context(),
		"a branched journal needs the exact branch context"
	);
	let branch: Vec<&str> = store.branch_messages().iter().map(|entry| entry.id.as_str()).collect();
	assert_eq!(branch, ["msg-1"], "abandoned entries must leave the active branch");

	// The loader derives the active branch from the last physical entry, so a
	// reload must land on the same leaf and the same branch.
	drop(store);
	let mut reloaded = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work"),
		model: None,
		provider: None,
		dir: Some(dir.clone()),
		id: Some("branch-test".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: true,
	});
	assert!(reloaded.enabled(), "reload must open the log: {:?}", reloaded.take_warnings());
	assert_eq!(
		reloaded.header().expect("header").parent_session.as_deref(),
		Some("parent-session"),
		"parent lineage must survive a reload"
	);
	assert_eq!(reloaded.leaf_id(), Some(marker.as_str()), "leaf must survive a reload");
	let branch: Vec<&str> = reloaded
		.branch_messages()
		.iter()
		.map(|entry| entry.id.as_str())
		.collect();
	assert_eq!(branch, ["msg-1"], "active branch must survive a reload");

	let unknown = reloaded.branch_to("msg-missing");
	assert!(
		unknown.as_ref().is_err_and(|error| error.contains("msg-missing")),
		"branching to an entry outside the store must be rejected: {unknown:?}"
	);

	fs::remove_dir_all(dir).ok();
}

/// C1f acceptance: a real TypeScript-written v3 JSONL session must survive
/// `read -> append -> reread`, keep interleaved text/image part order and
/// canonical stop reasons, and keep pre-existing logs usable through resume
/// and fork (TS fixture and legacy Rust v1 log).
#[test]
fn ts_v3_session_round_trip_preserves_ordered_parts_stop_reasons_and_resume_fork() {
	const ORIGINAL_ID: &str = "01a178ce-3463-7504-8047-ca4fab78a16b";
	let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures/v3_session.jsonl");
	assert!(fixture_path.is_file(), "real TS v3 fixture must exist");

	let temp = temp_dir("ts-roundtrip");
	// TS-style file name (`file_safe_timestamp` + `_` + session id) so resume
	// selection and id-prefix resolution exercise the real naming contract.
	let session_file = temp
		.join(format!("2026-10-26T15-22-22-179Z_{ORIGINAL_ID}.jsonl"));
	fs::copy(&fixture_path, &session_file).expect("copy TS fixture");
	let imported_path = import_v3_session(&session_file, &temp)
		.expect("import TS fixture into the SQLite authority");

	let part_signature = |content: &MessageContent| -> Vec<String> {
		content
			.parts()
			.into_iter()
			.map(|part| {
				format!(
					"{}|{}|{}|{}",
					part.kind,
					part.text.as_deref().unwrap_or_default(),
					part.data.as_deref().unwrap_or_default(),
					part.mime_type.as_deref().unwrap_or_default()
				)
			})
			.collect()
	};
	let expected = |lines: &[&str]| lines.iter().map(|line| line.to_string()).collect::<Vec<_>>();

	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/ts-roundtrip"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: Some(ORIGINAL_ID.to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: true,
	});
	assert!(
		store.enabled(),
		"store must open the real TS fixture: {:?}",
		store.take_warnings()
	);
	assert_eq!(store.header().expect("header").id, ORIGINAL_ID);

	// READ: the TS-written text/image order is surfaced exactly.
	let read_messages = store.messages();
	assert_eq!(read_messages.len(), 3, "fixture has user/assistant/toolResult");
	assert_eq!(
		part_signature(&read_messages[0].message.content),
		expected(&["text|hello from test user||", "image||aGVsbG8=|image/png"]),
		"user text+image order must be preserved"
	);
	assert_eq!(
		part_signature(&read_messages[2].message.content),
		expected(&["text|file content||", "image||aW1hZ2VkYXRh|image/png"]),
		"toolResult text+image order must be preserved"
	);

	// WRITE: append a message with interleaved text/image parts and an assistant
	// carrying a canonical stop reason, then reopen from disk.
	store
		.append_message(&MessageEntry::new(
			"roundtrip-user",
			Some("59e1c6b4".to_string()),
			"2026-10-26T15:22:23.000Z",
			SessionMessage::user(
				MessageContent::Parts(vec![
					ContentPart::text("first text"),
					ContentPart::image("aW1nLW9uZQ==", "image/png"),
					ContentPart::text("second text"),
					ContentPart::image("aW1nLXR3bw==", "image/jpeg"),
				]),
				1_793_028_143_000,
			),
		))
		.expect("append interleaved user message");
	store
		.append_message(&MessageEntry::new(
			"roundtrip-assistant",
			Some("roundtrip-user".to_string()),
			"2026-10-26T15:22:23.001Z",
			SessionMessage::assistant(
				MessageContent::Text("round-trip answer".to_string()),
				"openai-completions",
				"openai",
				"gpt-4o",
				None,
				"length",
				1_793_028_143_001,
			),
		))
		.expect("append assistant message");
	drop(store);

	// READ AGAIN (resume): every message survives, in order, with canonical
	// stop reasons untouched.
	let mut resumed = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/ts-roundtrip"),
		model: None,
		provider: None,
		dir: Some(temp.clone()),
		id: None,
		continue_latest: true,
		parent_session: None,
		no_title: true,
	});
	assert!(
		resumed.enabled(),
		"resumed store must reopen the round-tripped log: {:?}",
		resumed.take_warnings()
	);
	assert_eq!(resumed.header().expect("resumed header").id, ORIGINAL_ID);
	let resumed_messages = resumed.messages();
	assert_eq!(resumed_messages.len(), 5, "both appended messages must persist");
	assert_eq!(
		part_signature(&resumed_messages[3].message.content),
		expected(&[
			"text|first text||",
			"image||aW1nLW9uZQ==|image/png",
			"text|second text||",
			"image||aW1nLXR3bw==|image/jpeg",
		]),
		"interleaved text/image order must survive write -> reread"
	);
	assert_eq!(
		resumed_messages[1].message.stop_reason.as_deref(),
		Some("toolUse"),
		"TS canonical stop reason must survive unchanged"
	);
	assert_eq!(
		resumed_messages[4].message.stop_reason.as_deref(),
		Some("length"),
		"appended canonical stop reason must survive unchanged"
	);

	// Resume replay surface: the runtime history conversion keeps the same
	// interleaved order when rehydrating a resumed session.
	let history = resumed.history();
	assert_eq!(history.len(), 2, "fixture turn plus appended turn");
	let replay = aragent::runtime::history_messages(history).expect("resume replay");
	let replay_user = replay
		.iter()
		.find(|message| message.role == pi_agent::Role::User && message.parts.len() == 4)
		.expect("appended user message must replay with all four parts");
	let replay_signature: Vec<String> = replay_user
		.parts
		.iter()
		.map(|part| match part {
			pi_agent::ContentPart::Text { text } => format!("text|{text}"),
			pi_agent::ContentPart::Image { data, mime_type } => {
				format!("image|{data}|{mime_type}")
			},
			pi_agent::ContentPart::Thinking { thinking, .. } => format!("thinking|{thinking}"),
			pi_agent::ContentPart::RedactedThinking { data } => format!("redacted|{data}"),
			pi_agent::ContentPart::ToolCall { id, .. } => format!("toolCall|{id}"),
		})
		.collect();
	assert_eq!(
		replay_signature,
		expected(&[
			"text|first text",
			"image|aW1nLW9uZQ==|image/png",
			"text|second text",
			"image|aW1nLXR3bw==|image/jpeg",
		]),
		"resume replay must keep interleaved text/image order"
	);

	// Resume selector resolves the pre-existing TS session by id prefix.
	let resolved = resolve_resumable_session(
		"01a178ce",
		Path::new("/work/ts-roundtrip"),
		Some(&temp),
		false,
	)
	.expect("resolve resume selector")
	.expect("id-prefix must resolve the TS session");
	assert_eq!(resolved, imported_path);

	// FORK: the round-tripped TS session still forks with lineage and parts.
	let ts_fork_dir = temp.join("ts-forks");
	fs::create_dir_all(&ts_fork_dir).expect("create TS fork dir");
	let forked_path =
		fork_session(&imported_path, None, Some(&ts_fork_dir)).expect("fork TS session");
	assert!(!forked_path.exists(), "fork writes SQLite, not JSONL");
	assert!(ts_fork_dir.join("sessions.db").is_file());
	let mut forked = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/ts-roundtrip"),
		model: None,
		provider: None,
		dir: Some(ts_fork_dir.clone()),
		id: None,
		// ponytail: one fork per dir keeps `continue_latest` unambiguous; parse
		// the fork header id if several forks ever share a directory.
		continue_latest: true,
		parent_session: None,
		no_title: true,
	});
	assert!(forked.enabled(), "fork must reopen: {:?}", forked.take_warnings());
	let forked_header = forked.header().expect("forked header");
	assert_ne!(forked_header.id, ORIGINAL_ID, "fork gets a new session id");
	assert_eq!(
		forked_header.parent_session.as_deref(),
		Some(ORIGINAL_ID),
		"fork must record the TS parent session"
	);
	let forked_messages = forked.messages();
	assert_eq!(forked_messages.len(), 5, "fork must carry every message");
	assert_eq!(
		part_signature(&forked_messages[3].message.content),
		expected(&[
			"text|first text||",
			"image||aW1nLW9uZQ==|image/png",
			"text|second text||",
			"image||aW1nLXR3bw==|image/jpeg",
		]),
		"fork must preserve interleaved part order"
	);
	assert_eq!(forked_messages[1].message.stop_reason.as_deref(), Some("toolUse"));
	assert_eq!(forked_messages[4].message.stop_reason.as_deref(), Some("length"));

	// Old Rust session no-regression: the legacy v1 log still imports,
	// resumes, and forks after the TS round-trip surface is exercised.
	let legacy_fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures/v1_session.jsonl");
	assert!(legacy_fixture.is_file(), "legacy v1 fixture must exist");
	let legacy_source = temp.join("v1-source.jsonl");
	fs::copy(&legacy_fixture, &legacy_source).expect("copy v1 fixture");
	let imported_dir = temp.join("v3-from-v1");
	fs::create_dir_all(&imported_dir).expect("create import dir");
	let imported_path = import_legacy_v1(&legacy_source, &imported_dir, None).expect("import v1");

	let mut imported = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/legacy"),
		model: None,
		provider: None,
		dir: Some(imported_dir.clone()),
		id: None,
		continue_latest: true,
		parent_session: None,
		no_title: true,
	});
	assert!(
		imported.enabled(),
		"imported legacy session must resume: {:?}",
		imported.take_warnings()
	);
	assert_eq!(imported.history().len(), 1);
	assert_eq!(imported.history()[0].prompt, "fix the bug");
	assert_eq!(
		imported.history()[0].stop_reason, "stop",
		"legacy `end_turn` must migrate to the canonical `stop`"
	);
	let imported_id = imported.header().expect("imported header").id.clone();

	let legacy_fork_dir = temp.join("legacy-forks");
	fs::create_dir_all(&legacy_fork_dir).expect("create legacy fork dir");
	let legacy_fork_path =
		fork_session(&imported_path, None, Some(&legacy_fork_dir)).expect("fork imported v1");
	assert!(!legacy_fork_path.exists(), "fork writes SQLite, not JSONL");
	assert!(legacy_fork_dir.join("sessions.db").is_file());
	let legacy_fork = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/legacy"),
		model: None,
		provider: None,
		dir: Some(legacy_fork_dir),
		id: None,
		continue_latest: true,
		parent_session: None,
		no_title: true,
	});
	assert!(legacy_fork.enabled(), "legacy fork must reopen");
	assert_eq!(
		legacy_fork.header().expect("legacy fork header").parent_session.as_deref(),
		Some(imported_id.as_str()),
		"legacy fork must record the imported parent"
	);
	assert_eq!(legacy_fork.history().len(), 1, "legacy fork keeps its turn");
	assert_eq!(legacy_fork.history()[0].prompt, "fix the bug");
	assert_eq!(legacy_fork.history()[0].assistant, "done");

	fs::remove_dir_all(temp).ok();
}

#[test]
fn signed_reasoning_and_length_terminal_survive_session_resume_and_replay() {
	let dir = temp_dir("signed-resume");
	let mut store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/signed"),
		model: Some("anthropic/claude".to_string()),
		provider: Some("anthropic".to_string()),
		dir: Some(dir.clone()),
		id: Some("signed-resume".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: true,
	});
	assert!(store.enabled(), "store must be enabled: {:?}", store.take_warnings());

	store
		.append_message(&MessageEntry::new(
			"msg-u",
			None,
			"2026-11-02T10:00:00.000Z",
			SessionMessage::user(MessageContent::Text("inspect".to_string()), 1),
		))
		.expect("append user");
	let mut assistant = SessionMessage::assistant(
		MessageContent::Parts(vec![
			ContentPart::thinking_with_signature("plan", "sig-resume"),
			ContentPart::redacted_thinking("opaque"),
			ContentPart::text("answer"),
			ContentPart::tool_call("call-1", "read", serde_json::json!({ "path": "a" })),
		]),
		"anthropic-messages",
		"anthropic",
		"claude",
		None,
		"length",
		2,
	);
	assistant.error_message = Some("max tokens".to_string());
	assistant.attribution = Some("agent".to_string());
	store
		.append_message(&MessageEntry::new(
			"msg-a",
			Some("msg-u".to_string()),
			"2026-11-02T10:00:01.000Z",
			assistant,
		))
		.expect("append assistant");
	store
		.append_message(&MessageEntry::new(
			"msg-t",
			Some("msg-a".to_string()),
			"2026-11-02T10:00:02.000Z",
			SessionMessage::tool_result(
				"call-1",
				"read",
				MessageContent::Text("file".to_string()),
				false,
				3,
			),
		))
		.expect("append tool result");

	// The durable row keeps the typed fields, not just a flatten passthrough.
	let persisted = store
		.messages()
		.iter()
		.find(|entry| entry.message.role == "assistant")
		.expect("assistant row");
	assert!(persisted.message.content.parts().iter().any(|part| {
		part.kind == "thinking"
			&& part
				.extra
				.get("thinkingSignature")
				.and_then(serde_json::Value::as_str)
				== Some("sig-resume")
	}));
	assert!(persisted
		.message
		.content
		.parts()
		.iter()
		.any(|part| part.kind == "redactedThinking"));
	assert_eq!(persisted.message.stop_reason.as_deref(), Some("length"));
	assert_eq!(persisted.message.error_message.as_deref(), Some("max tokens"));
	assert_eq!(persisted.message.attribution.as_deref(), Some("agent"));

	drop(store);
	let reloaded = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: PathBuf::from("/work/signed"),
		model: None,
		provider: None,
		dir: Some(dir.clone()),
		id: Some("signed-resume".to_string()),
		continue_latest: false,
		parent_session: None,
		no_title: true,
	});
	assert!(reloaded.enabled(), "reload must succeed");
	let history = aragent::runtime::history_messages_from_entries(reloaded.messages())
		.expect("lossless resume history");
	let assistant = history
		.iter()
		.find(|message| message.role == pi_agent::Role::Assistant)
		.expect("assistant message");
	assert_eq!(assistant.stop_reason, Some(pi_agent::StopReason::Length));
	assert_eq!(assistant.error_message.as_deref(), Some("max tokens"));
	assert_eq!(assistant.provider.as_deref(), Some("anthropic"));
	assert_eq!(assistant.model.as_deref(), Some("claude"));
	assert_eq!(assistant.attribution, Some(pi_agent::Attribution::Agent));
	assert_eq!(assistant.parts[0].thinking_signature(), Some("sig-resume"));
	assert_eq!(assistant.parts[1].as_redacted_thinking(), Some("opaque"));
	assert!(matches!(assistant.parts[2], pi_agent::ContentPart::Text { .. }));
	assert_eq!(assistant.tool_calls.len(), 1);
	assert_eq!(assistant.tool_calls[0].id, "call-1");
	assert!(matches!(assistant.parts[3], pi_agent::ContentPart::ToolCall { .. }));

	// Cross-provider replay strips signed/redacted reasoning but keeps the call.
	let wire = pi_agent::to_openai_messages(&history).expect("wire");
	let wire = serde_json::to_string(&wire).expect("serialize wire");
	assert!(!wire.contains("sig-resume"));
	assert!(!wire.contains("opaque"));
	assert!(wire.contains("\"tool_calls\""));

	fs::remove_dir_all(dir).ok();
}

#[test]
fn fork_copies_resolved_provenance_and_allows_new_reuse_cross_db() {
	let temp = temp_dir("provenance-cross");
	let source_dir = temp.join("source");
	let target_dir = temp.join("target");
	fs::create_dir_all(&source_dir).expect("source");
	let mut store = SqliteStore::open(source_dir.join("sessions.db")).expect("store");
	store.create_session(NewSession { id: "source".into(), parent_session_id: None, payload: json!({"type":"session","version":3,"id":"source","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "source.main".into(), initial_branch_payload: json!({}) }).expect("session");
	store.append_message(NewMessage { id: "a1".into(), session_id: "source".into(), branch_id: "source.main".into(), parent_message_id: None, payload: json!({"type":"message","id":"a1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"shared","name":"bash"}],"timestamp":1}}) }).expect("assistant");
	let exec = store.record_intent(NewIntent { session_id: "source".into(), branch_id: "source.main".into(), provider_call_id: "shared".into(), tool_name: "bash".into(), payload: json!({}) }).expect("intent");
	store.record_outcome(&exec, json!({"content":"done","isError":false})).expect("outcome");
	store.append_message(NewMessage { id: "r1".into(), session_id: "source".into(), branch_id: "source.main".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"r1","parentId":"a1","timestamp":"2026-01-01T00:00:01Z","message":{"role":"toolResult","toolCallId":"shared","toolName":"bash","content":"done","isError":false,"timestamp":2,"details":{"executionId":exec}}}) }).expect("result");
	drop(store);

	let forked = fork_session(&source_dir.join("source.jsonl"), None, Some(&target_dir)).expect("fork");
	let fork_id = forked.file_stem().unwrap().to_str().unwrap().to_string();
	let mut target = SqliteStore::open(target_dir.join("sessions.db")).expect("target");
	let rows = target.session_messages(&fork_id).expect("rows");
	let result_row = rows.iter().find(|row| row.payload["message"]["role"] == json!("toolResult")).expect("result row");
	let provenance = target.load_session_snapshot(&fork_id).expect("snapshot").provenance;
	assert_eq!(provenance.len(), 1, "fork provenance row must be stored out of band");
	assert_eq!(provenance[0].message_id, result_row.id);
	assert!(provenance[0].outcome_resolved);
	assert!(result_row.payload["message"]["details"].get("executionProvenance").is_none(), "no trust in payload markers");
	assert!(target.unresolved_intents(&fork_id).expect("unresolved").is_empty(), "fork provenance must not become an operational intent");

	let branch = target.session(&fork_id).expect("session").expect("fork").active_branch_id.expect("branch");
	target.append_message(NewMessage { id: "a2".into(), session_id: fork_id.clone(), branch_id: branch.clone(), parent_message_id: Some(result_row.id.clone()), payload: json!({"type":"message","id":"a2","message":{"role":"assistant","content":[{"type":"toolCall","id":"shared","name":"bash"}]}}) }).expect("reuse assistant");
	let new_exec = target.record_intent(NewIntent { session_id: fork_id.clone(), branch_id: branch.clone(), provider_call_id: "shared".into(), tool_name: "bash".into(), payload: json!({}) }).expect("new intent");
	assert_eq!(target.recover_unresolved_intents(&fork_id).expect("recovery"), 1);
	let leaf_id = target.branch(&branch).expect("branch").expect("branch").leaf_message_id.expect("leaf");
	let leaf = target.message(&leaf_id).expect("message").expect("leaf");
	assert_eq!(leaf.payload["message"]["details"]["executionId"], new_exec);
	assert_eq!(target.outcome(&new_exec).expect("new outcome"), None);
	fs::remove_dir_all(temp).ok();
}

#[test]
fn fork_same_db_unknown_provenance_is_immutable_and_not_operational() {
	let temp = temp_dir("provenance-same");
	let dir = temp.join("db");
	fs::create_dir_all(&dir).expect("dir");
	let db = dir.join("sessions.db");
	let mut store = SqliteStore::open(&db).expect("store");
	store.create_session(NewSession { id: "source".into(), parent_session_id: None, payload: json!({"type":"session","version":3,"id":"source","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp"}), initial_branch_id: "source.main".into(), initial_branch_payload: json!({}) }).expect("session");
	store.append_message(NewMessage { id: "a1".into(), session_id: "source".into(), branch_id: "source.main".into(), parent_message_id: None, payload: json!({"type":"message","id":"a1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"shared","name":"bash"}],"timestamp":1}}) }).expect("assistant");
	let exec = store.record_intent(NewIntent { session_id: "source".into(), branch_id: "source.main".into(), provider_call_id: "shared".into(), tool_name: "bash".into(), payload: json!({}) }).expect("intent");
	store.append_message(NewMessage { id: "r1".into(), session_id: "source".into(), branch_id: "source.main".into(), parent_message_id: Some("a1".into()), payload: json!({"type":"message","id":"r1","parentId":"a1","timestamp":"2026-01-01T00:00:01Z","message":{"role":"toolResult","toolCallId":"shared","toolName":"bash","content":"interrupted","isError":true,"timestamp":2,"details":{"executionId":exec,"interruptedUnknown":true}}}) }).expect("interrupted result");
	drop(store);

	let forked = fork_session(&dir.join("source.jsonl"), None, Some(&dir)).expect("same-db fork");
	let fork_id = forked.file_stem().unwrap().to_str().unwrap().to_string();
	let mut target = SqliteStore::open(&db).expect("target");
	let rows = target.session_messages(&fork_id).expect("rows");
	let result_row = rows.iter().find(|row| row.payload["message"]["role"] == json!("toolResult")).expect("fork result");
	let provenance = target.load_session_snapshot(&fork_id).expect("snapshot").provenance;
	assert_eq!(provenance.len(), 1);
	assert!(!provenance[0].outcome_resolved, "unknown status must stay unknown");
	assert!(result_row.payload["message"]["details"].get("executionProvenance").is_none(), "payload carries no trusted marker");
	assert!(target.load_session_snapshot("source").expect("source snapshot").provenance.is_empty(), "source has no fork provenance");
	assert!(target.unresolved_intents(&fork_id).expect("unresolved").is_empty(), "unknown provenance must not become an operational intent");
	assert_eq!(target.unresolved_intents("source").expect("source unresolved").len(), 1, "source intent stays unresolved");

	let branch = target.session(&fork_id).expect("session").expect("fork").active_branch_id.expect("branch");
	target.append_message(NewMessage { id: "a2".into(), session_id: fork_id.clone(), branch_id: branch.clone(), parent_message_id: Some(result_row.id.clone()), payload: json!({"type":"message","id":"a2","message":{"role":"assistant","content":[{"type":"toolCall","id":"shared","name":"bash"}]}}) }).expect("reuse assistant");
	assert_eq!(target.recover_unresolved_intents(&fork_id).expect("no intent to recover"), 0, "historical source intent must never be attributed to the fork");
	let new_exec = target.record_intent(NewIntent { session_id: fork_id.clone(), branch_id: branch.clone(), provider_call_id: "shared".into(), tool_name: "bash".into(), payload: json!({}) }).expect("new intent");
	let error = target.recover_unresolved_intents(&fork_id).expect_err("unknown copied provenance must fail closed on reuse");
	assert!(error.to_string().contains("not uniquely attributable"), "{error}");
	assert!(!error.to_string().contains("unknown execution"), "provenance marker must answer classification without a copied execution row: {error}");
	assert_eq!(target.branch(&branch).expect("branch").expect("branch").leaf_message_id.as_deref(), Some("a2"), "no recovered result may be appended");
	assert_eq!(target.outcome(&new_exec).expect("new outcome"), None);
	assert_eq!(target.outcome(&exec).expect("source outcome"), None, "fork must never resolve the source execution");
	fs::remove_dir_all(temp).ok();
}
