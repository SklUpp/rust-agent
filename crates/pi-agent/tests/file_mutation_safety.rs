//! Safety boundaries for the file-mutating tools that are reachable today:
//! the non-hashline `edit` engines (internal-URL refusal, UTF-8 refusal,
//! symlink/mode preservation, BOM+CRLF round-trip, hashline dispatch refusal)
//! and the staged AST preview gate (stale requeue, partial-apply retry).
//!
//! Every case drives a real tool entry point against a task-owned temp
//! directory and asserts the on-disk bytes afterwards.

use std::{
	fs,
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
};

use pi_agent::{
	AgentContext, Tool, ToolError,
	tools::{
		AstEditTool, EditTool, PendingStore, WriteTool,
		edit_variants::EditMode,
		hashline::SnapshotStore,
	},
};
use serde_json::json;

async fn execute(
	mode: EditMode,
	args: &serde_json::Value,
	ctx: &AgentContext,
) -> Result<pi_agent::state::ToolResult, ToolError> {
	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), mode)
		.with_pending(Arc::clone(&pending));
	let result = tool.execute(args, ctx).await?;
	if result.is_error {
		return Err(ToolError::ExecutionFailed(result.content));
	}
	let applied = pending.resolve(ctx, "apply").await?;
	if applied.is_error {
		return Err(ToolError::ExecutionFailed(applied.content));
	}
	Ok(applied)
}
struct TempDir {
	path: PathBuf,
}

impl TempDir {
	fn new(label: &str) -> Self {
		static NEXT: AtomicUsize = AtomicUsize::new(0);
		let unique = format!(
			"pi-mutation-{label}-{}-{}",
			std::process::id(),
			NEXT.fetch_add(1, Ordering::SeqCst)
		);
		let path = std::env::temp_dir().join(unique);
		fs::create_dir_all(&path).expect("create temp dir");
		Self { path }
	}

	fn write(&self, name: &str, content: &str) -> PathBuf {
		let path = self.path.join(name);
		if let Some(parent) = path.parent() {
			fs::create_dir_all(parent).expect("create parent");
		}
		fs::write(&path, content).expect("write fixture");
		path
	}

	fn read(&self, name: &str) -> String {
		String::from_utf8(fs::read(self.path.join(name)).expect("read fixture")).expect("utf-8")
	}
}

impl Drop for TempDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.path);
	}
}

fn ctx(dir: &TempDir) -> AgentContext {
	AgentContext::new(dir.path.clone())
}

fn ast_args(paths: &[&str]) -> serde_json::Value {
	json!({
		"ops": [{ "pat": "foo($A)", "out": "bar($A)" }],
		"paths": paths,
	})
}

#[tokio::test]
async fn internal_url_paths_are_refused_without_a_filesystem_fallback() {
	let dir = TempDir::new("internal-urls");
	let ctx = ctx(&dir);

	let cases = [
		(
			EditMode::Replace,
			json!({ "path": "artifact://note", "old_string": "a", "new_string": "b" }),
		),
		(
			EditMode::Patch,
			json!({ "path": "agent://child", "edits": [{ "op": "create", "diff": "x\n" }] }),
		),
		(
			EditMode::ApplyPatch,
			json!({ "input": "*** Begin Patch\n*** Add File: skill://rule\n+x\n*** End Patch" }),
		),
		(
			EditMode::Sloppy,
			json!({ "input": "§local://notes\n«a\n»\nb" }),
		),
	];
	for (mode, args) in cases {
		let error = execute(mode, &args, &ctx)
			.await
			.expect_err(&format!("{mode:?} must refuse an internal URL"));
		let message = error.to_string();
		assert!(
			message.contains("must be resolved through the proper protocol handler"),
			"{mode:?}: {message}"
		);
	}
	assert_eq!(
		fs::read_dir(&dir.path).expect("dir").count(),
		0,
		"no authored URL may degrade into a local file"
	);
}

#[tokio::test]
async fn non_utf8_targets_are_refused_without_a_lossy_rewrite() {
	let dir = TempDir::new("non-utf8");
	let file = dir.write("bad.txt", "");
	fs::write(&file, [b'f', b'o', b'o', 0xff, b'\n']).expect("write bytes");
	let ctx = ctx(&dir);

	let error = execute(
		EditMode::Replace,
		&json!({ "path": "bad.txt", "old_string": "foo", "new_string": "bar" }),
		&ctx,
	)
	.await
	.expect_err("non-UTF-8 must be refused");
	assert!(error.to_string().contains("not valid UTF-8"), "{error}");
	assert_eq!(fs::read(&file).expect("read"), vec![b'f', b'o', b'o', 0xff, b'\n']);
}

#[cfg(unix)]
#[tokio::test]
async fn replace_through_a_symlink_updates_the_target_and_keeps_the_link() {
	use std::os::unix::fs::PermissionsExt;

	let dir = TempDir::new("symlink");
	let target = dir.write("real.txt", "alpha\n");
	fs::set_permissions(&target, fs::Permissions::from_mode(0o640)).expect("chmod");
	let link = dir.path.join("link.txt");
	std::os::unix::fs::symlink("real.txt", &link).expect("symlink");
	let ctx = ctx(&dir);

	execute(
		EditMode::Replace,
		&json!({ "path": "link.txt", "old_string": "alpha", "new_string": "beta" }),
		&ctx,
	)
	.await
	.expect("replace through symlink");

	assert!(
		fs::symlink_metadata(&link).expect("link meta").file_type().is_symlink(),
		"the authored symlink must survive the write"
	);
	assert_eq!(fs::read_link(&link).expect("link target"), Path::new("real.txt"));
	assert_eq!(dir.read("real.txt"), "beta\n");
	assert_eq!(
		fs::metadata(&target).expect("meta").permissions().mode() & 0o777,
		0o640,
		"the destination mode must be preserved"
	);
}

#[tokio::test]
async fn apply_patch_update_round_trips_bom_and_crlf() {
	let dir = TempDir::new("apply-patch-bom");
	dir.write("win.txt", "\u{feff}alpha\r\nbeta\r\n");
	let ctx = ctx(&dir);

	execute(
		EditMode::ApplyPatch,
		&json!({ "input": "*** Begin Patch\n*** Update File: win.txt\n@@\n-alpha\n+ALPHA\n*** End Patch" }),
		&ctx,
	)
	.await
	.expect("apply patch update");
	assert_eq!(dir.read("win.txt"), "\u{feff}ALPHA\r\nbeta\r\n");
}

#[tokio::test]
async fn hashline_mode_is_not_served_by_the_plain_engines() {
	let dir = TempDir::new("hashline-dispatch");
	let file = dir.write("a.txt", "keep\n");
	let ctx = ctx(&dir);

	let error = pi_agent::tools::edit_variants::execute(
		EditMode::Hashline,
		&json!({ "input": "[a.txt#0000]\nPUT 1.=1:\n+changed\n" }),
		&ctx,
		None,
		Arc::new(SnapshotStore::new()),
	)
	.await
	.expect_err("hashline must be dispatched to the native engine");
	assert!(
		error.to_string().contains("native hashline engine"),
		"{error}"
	);
	assert_eq!(fs::read_to_string(&file).expect("read"), "keep\n");
}

#[tokio::test]
async fn stale_ast_edit_preview_writes_nothing_and_a_fresh_preview_applies() {
	let dir = TempDir::new("ast-stale");
	let first = dir.write("a.ts", "const a = foo(1);\n");
	let second = dir.write("b.ts", "const b = foo(1);\n");
	let pending = Arc::new(PendingStore::new());
	let tool = AstEditTool::new(Arc::new(SnapshotStore::new()), Arc::clone(&pending));
	let ctx = ctx(&dir);

	tool.execute(&ast_args(&["a.ts", "b.ts"]), &ctx)
		.await
		.expect("preview");
	assert_eq!(pending.len(), 1);

	// Same replacement count, different bytes: a counts-only gate would apply.
	dir.write("b.ts", "const b = foo(2);\n");
	let stale = pending.resolve(&ctx, "apply").await.expect("stale resolve");
	assert!(stale.is_error, "{}", stale.content);
	assert!(stale.content.contains("stale / no longer matches"), "{}", stale.content);
	assert_eq!(fs::read_to_string(&first).expect("a"), "const a = foo(1);\n");
	assert_eq!(fs::read_to_string(&second).expect("b"), "const b = foo(2);\n");
	let details = stale.details.as_ref().expect("details");
	// `PendingStore::resolve` decorates the tool result and moves its details
	// under `sourceResultDetails`.
	let source = &details["sourceResultDetails"];
	assert_eq!(source["stale"], json!(true));
	assert_eq!(source["partial"], json!(false));
	assert_eq!(source["writtenFiles"], json!([]));
	assert_eq!(source["attemptWrittenFiles"], json!([]));
	assert_eq!(pending.len(), 1, "a failed apply keeps the preview for retry");

	// A fresh preview reads the new bytes; resolving it applies the current state.
	tool.execute(&ast_args(&["a.ts", "b.ts"]), &ctx)
		.await
		.expect("fresh preview");
	assert_eq!(pending.len(), 2);
	let applied = pending.resolve(&ctx, "apply current").await.expect("fresh resolve");
	assert!(!applied.is_error, "{}", applied.content);
	assert_eq!(fs::read_to_string(&first).expect("a"), "const a = bar(1);\n");
	assert_eq!(fs::read_to_string(&second).expect("b"), "const b = bar(2);\n");
	assert_eq!(pending.len(), 1, "the superseded stale preview stays staged");
	assert!(!pending.reject(&ctx, "discard stale").await.expect("reject").is_error);
	assert!(pending.is_empty());
}

/// Deterministic partial apply without permission tricks: the second staged
/// file's name is readable but long enough that the engine's
/// `.<name>.tmp-<16 hex>` sibling exceeds `NAME_MAX`, so that one write fails
/// after the first file already landed.
#[cfg(unix)]
#[tokio::test]
async fn partial_ast_apply_retry_reports_prior_writes_and_rewrites_nothing() {
	let dir = TempDir::new("ast-partial");
	let first = dir.write("a.ts", "const a = foo(1);\n");
	let blocked_name = format!("{}.ts", "x".repeat(237));
	assert_eq!(blocked_name.len(), 240);
	assert!(
		blocked_name.len() + "..tmp-0123456789abcdef".len() > 255,
		"the temp sibling must exceed NAME_MAX"
	);
	let blocked = dir.write(&format!("blocked/{blocked_name}"), "const b = foo(1);\n");
	let blocked_display = format!("blocked/{blocked_name}");
	let pending = Arc::new(PendingStore::new());
	let tool = AstEditTool::new(Arc::new(SnapshotStore::new()), Arc::clone(&pending));
	let ctx = ctx(&dir);

	tool.execute(&ast_args(&["a.ts", &blocked_display]), &ctx)
		.await
		.expect("preview");

	let partial = pending.resolve(&ctx, "apply").await.expect("partial resolve");
	assert!(partial.is_error, "{}", partial.content);
	let details = partial.details.as_ref().expect("details");
	let source = &details["sourceResultDetails"];
	assert_eq!(source["partial"], json!(true), "{}", partial.content);
	assert_eq!(source["writtenFiles"], json!(["a.ts"]));
	assert_eq!(source["snapshotInvalidations"], json!(["a.ts"]));
	assert_eq!(source["failedFile"], json!(blocked_display));
	assert_eq!(fs::read_to_string(&first).expect("a"), "const a = bar(1);\n");
	assert_eq!(fs::read_to_string(&blocked).expect("b"), "const b = foo(1);\n");
	assert_eq!(pending.len(), 1, "a partial apply keeps the preview for retry");

	let retry = pending.resolve(&ctx, "retry").await.expect("retry");
	assert!(retry.is_error, "{}", retry.content);
	assert!(retry.content.contains("this attempt applied nothing"), "{}", retry.content);
	assert!(
		retry.content.contains("Previously written by this staged action: a.ts"),
		"{}",
		retry.content
	);
	let details = retry.details.as_ref().expect("details");
	let source = &details["sourceResultDetails"];
	assert_eq!(source["stale"], json!(true));
	assert_eq!(source["partial"], json!(true));
	assert_eq!(source["writtenFiles"], json!(["a.ts"]));
	assert_eq!(source["attemptWrittenFiles"], json!([]));
	assert_eq!(fs::read_to_string(&first).expect("a"), "const a = bar(1);\n");
	assert_eq!(fs::read_to_string(&blocked).expect("b"), "const b = foo(1);\n");
}

// ── Sloppy recovery regressions (EditEngineReview 1, 3, 5, 6) ─────────────
//
// Consumer-level acceptance through the public dispatch. They encode the
// intended post-fix behavior, so on a snapshot without the engine fixes they
// fail: finding 1 panics at the UTF-8 boundary, finding 3 writes the inline
// value instead of the authored REWRITE, and findings 5/6 do not refuse or
// delete as asserted. They are kept red on purpose.

fn refusal_message(error: &ToolError) -> &str {
	match error {
		ToolError::ExecutionFailed(message) | ToolError::InvalidArguments(message) => message,
		other => panic!("expected an input or execution refusal, got {other:?}"),
	}
}

#[tokio::test]
async fn sloppy_deletes_a_whole_first_line_that_starts_with_multibyte_text() {
	let dir = TempDir::new("sloppy-cjk-delete");
	let file = dir.write("cjk.txt", "日本語\nsecond\n");
	let ctx = ctx(&dir);

	execute(EditMode::Sloppy, &json!({ "input": "§cjk.txt\n«日本語\n»" }), &ctx)
		.await
		.expect("whole-line deletion at offset 0");
	assert_eq!(fs::read_to_string(&file).expect("read"), "second\n");
}

#[tokio::test]
async fn sloppy_mixed_inline_selection_and_rewrite_keeps_the_authored_rewrite() {
	let dir = TempDir::new("sloppy-mixed-rewrite");
	let file = dir.write("mixed.txt", "let a = 1;\n");
	let ctx = ctx(&dir);

	execute(
		EditMode::Sloppy,
		&json!({ "input": "§mixed.txt\n«let a = ⟪1│2⟫;\n»\nlet a = 3;" }),
		&ctx,
	)
	.await
	.expect("mixed inline + rewrite");
	assert_eq!(
		fs::read_to_string(&file).expect("read"),
		"let a = 3;\n",
		"a non-redundant REWRITE is the final text, not the inline selection"
	);
}

#[tokio::test]
async fn sloppy_rejects_a_rewrite_carrying_selection_markers_and_keeps_the_file() {
	let dir = TempDir::new("sloppy-marker-guard");
	let file = dir.write("markers.txt", "x\n");
	let ctx = ctx(&dir);

	let error = execute(
		EditMode::Sloppy,
		&json!({ "input": "§markers.txt\n«x\n»\n⟪y⟫" }),
		&ctx,
	)
	.await
	.expect_err("REWRITE markers must be refused");
	let message = refusal_message(&error).to_lowercase();
	assert!(
		message.contains("marker") || message.contains('⟪'),
		"the refusal must name the rejected marker: {message}"
	);
	assert_eq!(fs::read_to_string(&file).expect("read"), "x\n", "a refused payload writes nothing");
}

#[tokio::test]
async fn sloppy_rejects_an_unclaimed_ellipsis_in_the_rewrite_and_keeps_the_file() {
	let dir = TempDir::new("sloppy-ellipsis-guard");
	let file = dir.write("dots.txt", "a\nb\n");
	let ctx = ctx(&dir);

	let error = execute(
		EditMode::Sloppy,
		&json!({ "input": "§dots.txt\n«a\n»\n…" }),
		&ctx,
	)
	.await
	.expect_err("an unclaimed whole-line … must be refused");
	let message = refusal_message(&error).to_lowercase();
	assert!(
		message.contains('…') || message.contains("ellipsis") || message.contains("gap"),
		"the refusal must name the unclaimed ellipsis: {message}"
	);
	assert_eq!(
		fs::read_to_string(&file).expect("read"),
		"a\nb\n",
		"an ellipsis must not silently delete a line"
	);
}

#[tokio::test]
async fn sloppy_whole_match_deletion_form_removes_the_line() {
	let dir = TempDir::new("sloppy-whole-match-delete");
	let file = dir.write("delete.txt", "keep\nremove me\nkeep2\n");
	let ctx = ctx(&dir);

	execute(
		EditMode::Sloppy,
		&json!({ "input": "§delete.txt\n«remove me\n⟪│⟫" }),
		&ctx,
	)
	.await
	.expect("sole ⟪│⟫ at the match end deletes the match");
	assert_eq!(fs::read_to_string(&file).expect("read"), "keep\nkeep2\n");
}

#[tokio::test]
async fn ambiguous_replace_refuses_and_replace_all_is_explicit() {
	let dir = TempDir::new("replace-ambiguous");
	dir.write("demo.txt", "let x = 1;\nlet x = 1;\n");
	let ctx = ctx(&dir);

	let error = execute(
		EditMode::Replace,
		&json!({ "path": "demo.txt", "old_string": "let x = 1;", "new_string": "let x = 2;" }),
		&ctx,
	)
	.await
	.expect_err("ambiguous replace must fail");
	assert!(error.to_string().contains("Found 2 occurrences"), "{error}");
	assert_eq!(
		dir.read("demo.txt"),
		"let x = 1;\nlet x = 1;\n",
		"a refusal must not write"
	);

	execute(
		EditMode::Replace,
		&json!({
			"path": "demo.txt",
			"old_string": "let x = 1;",
			"new_string": "let x = 2;",
			"replace_all": true,
		}),
		&ctx,
	)
	.await
	.expect("explicit replace_all");
	assert_eq!(dir.read("demo.txt"), "let x = 2;\nlet x = 2;\n");
}

#[tokio::test]
async fn sloppy_ambiguity_leaves_every_section_untouched() {
	let dir = TempDir::new("sloppy-ambiguous");
	dir.write("a.txt", "let x = 1;\nlet x = 1;\n");
	dir.write("b.txt", "b\n");
	let ctx = ctx(&dir);

	let error = execute(
		EditMode::Sloppy,
		&json!({ "input": "§a.txt\n«let x = 1;\n»\nlet x = 2;\n§b.txt\n«b\n»\nB" }),
		&ctx,
	)
	.await
	.expect_err("non-unique match must fail");
	let message = error.to_string();
	assert!(message.contains("No files were modified"), "{message}");
	assert_eq!(dir.read("a.txt"), "let x = 1;\nlet x = 1;\n");
	assert_eq!(dir.read("b.txt"), "b\n", "later sections must not land");
}

#[tokio::test]
async fn patch_move_and_delete_refuse_non_file_targets() {
	let dir = TempDir::new("patch-refusals");
	dir.write("folder/inner.txt", "inner\n");
	dir.write("note.txt", "note\n");
	dir.write("existing.txt", "existing\n");
	let ctx = ctx(&dir);

	let error = execute(
		EditMode::Patch,
		&json!({ "path": "folder", "edits": [{ "op": "delete" }] }),
		&ctx,
	)
	.await
	.expect_err("deleting a directory must fail");
	assert!(error.to_string().contains("is not a regular file"), "{error}");
	assert_eq!(dir.read("folder/inner.txt"), "inner\n", "directory target stays intact");

	let error = execute(
		EditMode::Patch,
		&json!({ "path": "note.txt", "edits": [{ "op": "update", "rename": "note.txt", "diff": "@@\n-note\n+changed" }] }),
		&ctx,
	)
	.await
	.expect_err("rename onto itself must fail");
	assert!(error.to_string().contains("same as source path"), "{error}");
	assert_eq!(dir.read("note.txt"), "note\n");

	let error = execute(
		EditMode::Patch,
		&json!({ "path": "note.txt", "edits": [{ "op": "update", "rename": "existing.txt", "diff": "@@\n-note\n+changed" }] }),
		&ctx,
	)
	.await
	.expect_err("rename onto an existing file must fail");
	assert!(error.to_string().contains("destination already exists"), "{error}");
	assert_eq!(dir.read("note.txt"), "note\n");
	assert_eq!(dir.read("existing.txt"), "existing\n");
}

#[tokio::test]
async fn apply_patch_never_overwrites_an_existing_destination() {
	let dir = TempDir::new("apply-patch-trust");
	dir.write("existing.txt", "existing\n");
	dir.write("source.txt", "source\n");
	let ctx = ctx(&dir);

	let error = execute(
		EditMode::ApplyPatch,
		&json!({ "input": "*** Begin Patch\n*** Add File: existing.txt\n+overwrite\n*** End Patch" }),
		&ctx,
	)
	.await
	.expect_err("Add File must not overwrite");
	assert!(error.to_string().contains("already exists"), "{error}");
	assert_eq!(dir.read("existing.txt"), "existing\n");

	let error = execute(
		EditMode::ApplyPatch,
		&json!({
			"input": "*** Begin Patch\n*** Update File: source.txt\n*** Move to: existing.txt\n@@\n-source\n+moved\n*** End Patch",
		}),
		&ctx,
	)
	.await
	.expect_err("Move to an existing path must fail");
	assert!(error.to_string().contains("destination already exists"), "{error}");
	assert_eq!(dir.read("source.txt"), "source\n");
	assert_eq!(dir.read("existing.txt"), "existing\n");
}

#[tokio::test]
async fn replace_preserves_bom_crlf_and_multibyte_text() {
	let dir = TempDir::new("replace-encoding");
	let file = dir.write("win.txt", "\u{feff}α\r\nβ\r\nγ\r\n");
	let ctx = ctx(&dir);

	execute(
		EditMode::Replace,
		&json!({ "path": "win.txt", "old_string": "β", "new_string": "δ" }),
		&ctx,
	)
	.await
	.expect("replace multibyte");
	let bytes = fs::read(&file).expect("read");
	assert_eq!(
		String::from_utf8(bytes).expect("utf8"),
		"\u{feff}α\r\nδ\r\nγ\r\n",
		"BOM and CRLF endings survive the edit"
	);
}

#[tokio::test]
async fn write_refuses_to_overwrite_a_file_without_a_retained_read() {
	let dir = TempDir::new("write-no-read");
	dir.write("notes.txt", "original\n");
	let snapshots = Arc::new(SnapshotStore::new());
	let result = WriteTool::new()
		.with_snapshots(Some(snapshots))
		.execute(
			&json!({ "path": "notes.txt", "content": "replacement\n" }),
			&ctx(&dir),
		)
		.await
		.expect("write executes");
	assert!(result.is_error, "{}", result.content);
	assert!(
		result.content.contains("without a retained read snapshot"),
		"{}",
		result.content
	);
	assert_eq!(dir.read("notes.txt"), "original\n");
	let leftovers: Vec<String> = fs::read_dir(&dir.path)
		.expect("read dir")
		.map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
		.filter(|name| name.contains(".tmp-"))
		.collect();
	assert!(leftovers.is_empty(), "no staged write may remain: {leftovers:?}");
}

#[tokio::test]
async fn write_refuses_a_stale_snapshot_and_accepts_the_current_raw_version() {
	let dir = TempDir::new("write-stale");
	let file = dir.write("notes.txt", "v1\n");
	let store = Arc::new(SnapshotStore::new());
	let key = pi_agent::tools::hashline::canonical_key(&file);
	store
		.record_with_raw(&key, b"v1\n", "v1\n", None)
		.expect("record raw");
	let tool = WriteTool::new().with_snapshots(Some(Arc::clone(&store)));

	fs::write(&file, "v2\n").expect("external edit");
	let stale = tool
		.execute(
			&json!({ "path": "notes.txt", "content": "v3\n" }),
			&ctx(&dir),
		)
		.await
		.expect("write executes");
	assert!(stale.is_error, "{}", stale.content);
	assert!(
		stale.content.contains("changed since the retained read"),
		"{}",
		stale.content
	);
	assert_eq!(dir.read("notes.txt"), "v2\n");

	store
		.record_with_raw(&key, b"v2\n", "v2\n", None)
		.expect("record current raw");
	let fresh = tool
		.execute(
			&json!({ "path": "notes.txt", "content": "v3\n" }),
			&ctx(&dir),
		)
		.await
		.expect("write executes");
	assert!(!fresh.is_error, "{}", fresh.content);
	assert_eq!(dir.read("notes.txt"), "v2\n", "workspace must not be modified before apply");
	let applied = tool.pending().unwrap().resolve(&ctx(&dir), "apply").await.expect("apply");
	assert!(!applied.is_error, "{}", applied.content);
	assert_eq!(dir.read("notes.txt"), "v3\n");
}

#[tokio::test]
async fn concurrent_new_file_writes_publish_exactly_one_winner() {
	let dir = TempDir::new("write-new-race");
	let left_pending = Arc::new(PendingStore::new());
	let right_pending = Arc::new(PendingStore::new());
	let left_tool = WriteTool::with_pending(Arc::clone(&left_pending));
	let right_tool = WriteTool::with_pending(Arc::clone(&right_pending));
	let left_ctx = ctx(&dir);
	let right_ctx = ctx(&dir);
	let left_args = json!({ "path": "race.txt", "content": "left\n" });
	let right_args = json!({ "path": "race.txt", "content": "right\n" });
	left_tool.execute(&left_args, &left_ctx).await.expect("left stage");
	right_tool.execute(&right_args, &right_ctx).await.expect("right stage");
	let left_apply = left_pending.resolve(&left_ctx, "apply");
	let right_apply = right_pending.resolve(&right_ctx, "apply");
	let (left, right) = tokio::join!(left_apply, right_apply);
	let left = left.expect("left resolve");
	let right = right.expect("right resolve");
	let winners = [&left, &right].iter().filter(|result| !result.is_error).count();
	assert_eq!(
		winners, 1,
		"exactly one creator may publish: left={} right={}",
		left.content, right.content
	);
	let content = dir.read("race.txt");
	assert!(content == "left\n" || content == "right\n", "{content:?}");
}

#[tokio::test]
async fn write_does_not_mutate_workspace_before_explicit_apply() {
	let dir = TempDir::new("write-stage-only");
	let pending = Arc::new(PendingStore::new());
	let tool = WriteTool::with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	let res = tool
		.execute(&json!({ "path": "hello.txt", "content": "world\n" }), &ctx)
		.await
		.expect("stage");
	assert!(!res.is_error, "{}", res.content);
	assert_eq!(pending.len(), 1);
	assert!(!dir.path.join("hello.txt").exists(), "file must not exist on disk before apply");

	let applied = pending.resolve(&ctx, "apply").await.expect("apply");
	assert!(!applied.is_error, "{}", applied.content);
	assert_eq!(dir.read("hello.txt"), "world\n");
	assert!(pending.is_empty());
}

#[tokio::test]
async fn edit_does_not_mutate_workspace_before_explicit_apply() {
	let dir = TempDir::new("edit-stage-only");
	dir.write("target.txt", "initial text\n");
	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Replace)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	let res = tool
		.execute(&json!({ "path": "target.txt", "old_string": "initial", "new_string": "updated" }), &ctx)
		.await
		.expect("stage edit");
	assert!(!res.is_error, "{}", res.content);
	assert_eq!(pending.len(), 1);
	assert_eq!(dir.read("target.txt"), "initial text\n", "file must remain unchanged before apply");

	let applied = pending.resolve(&ctx, "apply").await.expect("apply");
	assert!(!applied.is_error, "{}", applied.content);
	assert_eq!(dir.read("target.txt"), "updated text\n");
}

#[tokio::test]
async fn reject_discards_staged_operation_without_modifying_workspace() {
	let dir = TempDir::new("stage-reject");
	dir.write("foo.txt", "alpha\n");
	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Replace)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	tool.execute(&json!({ "path": "foo.txt", "old_string": "alpha", "new_string": "beta" }), &ctx)
		.await
		.expect("stage");
	assert_eq!(pending.len(), 1);

	let discarded = pending.reject(&ctx, "no longer needed").await.expect("reject");
	assert!(!discarded.is_error);
	assert_eq!(dir.read("foo.txt"), "alpha\n");
	assert!(pending.is_empty());
}

#[tokio::test]
async fn gate_1_stale_aborts_without_modifying_any_file() {
	let dir = TempDir::new("gate1-stale");
	let f1 = dir.write("file1.txt", "one\n");
	let f2 = dir.write("file2.txt", "two\n");
	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::ApplyPatch)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	let patch_input = json!({
		"input": "*** Begin Patch\n*** Update File: file1.txt\n@@\n-one\n+ONE\n*** Update File: file2.txt\n@@\n-two\n+TWO\n*** End Patch"
	});

	tool.execute(&patch_input, &ctx).await.expect("stage patch");
	assert_eq!(pending.len(), 1);

	// Modify file2 externally before apply
	fs::write(&f2, "concurrent external change\n").expect("external write");

	let stale = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(stale.is_error, "{}", stale.content);
	assert_eq!(fs::read_to_string(&f1).expect("read"), "one\n", "zero files written on gate 1 failure");
	assert_eq!(fs::read_to_string(&f2).expect("read"), "concurrent external change\n");
}

#[cfg(unix)]
#[tokio::test]
async fn parent_symlink_retarget_is_rejected_before_apply() {
	let dir = TempDir::new("parent-retarget");
	let real_dir = dir.path.join("real");
	fs::create_dir_all(&real_dir).expect("real dir");
	fs::write(real_dir.join("data.txt"), "original\n").expect("write data");

	// Create symlink dir -> real
	let link_dir = dir.path.join("link");
	std::os::unix::fs::symlink(&real_dir, &link_dir).expect("symlink dir");

	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Replace)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	tool.execute(&json!({ "path": "link/data.txt", "old_string": "original", "new_string": "replaced" }), &ctx)
		.await
		.expect("stage");
	assert_eq!(pending.len(), 1);

	// Retarget link to another directory (e.g. trap dir)
	let trap_dir = dir.path.join("trap");
	fs::create_dir_all(&trap_dir).expect("trap dir");
	fs::write(trap_dir.join("data.txt"), "original\n").expect("write trap data");

	fs::remove_file(&link_dir).expect("remove link");
	std::os::unix::fs::symlink(&trap_dir, &link_dir).expect("retarget link");

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(result.is_error, "retargeted parent symlink must be rejected");
	assert!(
		result.content.contains("directory identity changed")
			|| result.content.contains("parent directory")
			|| result.content.contains("symlink retarget")
			|| result.content.contains("dev/ino")
			|| result.content.contains("identity"),
		"{}",
		result.content
	);
	assert_eq!(fs::read_to_string(real_dir.join("data.txt")).expect("read"), "original\n");
	assert_eq!(fs::read_to_string(trap_dir.join("data.txt")).expect("read"), "original\n");
}

#[cfg(unix)]
#[tokio::test]
async fn inode_change_with_equal_bytes_is_rejected() {
	let dir = TempDir::new("inode-change");
	let file_path = dir.write("file.txt", "same content\n");
	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Replace)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	tool.execute(&json!({ "path": "file.txt", "old_string": "same", "new_string": "changed" }), &ctx)
		.await
		.expect("stage");
	assert_eq!(pending.len(), 1);

	// Replace file with a new file (different inode) having the exact same bytes!
	let replacement = dir.write("replacement.txt", "same content\n");
	fs::rename(&replacement, &file_path).expect("replace with distinct inode");

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(result.is_error, "dev/ino change must be rejected even when bytes match");
	assert!(result.content.contains("dev/ino"), "{}", result.content);
}

#[cfg(unix)]
#[tokio::test]
async fn move_preserves_restrictive_mode_0600() {
	use std::os::unix::fs::PermissionsExt;
	let dir = TempDir::new("move-mode");
	let src = dir.write("src.txt", "secret content\n");
	fs::set_permissions(&src, fs::Permissions::from_mode(0o600)).expect("chmod");

	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Patch)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	let patch_input = json!({
		"path": "src.txt",
		"edits": [
			{
				"path": "src.txt",
				"rename": "dest.txt",
				"diff": "@@ -1,1 +1,1 @@\n-secret content\n+updated secret\n"
			}
		]
	});

	tool.execute(&patch_input, &ctx).await.expect("stage move");
	assert_eq!(pending.len(), 1);

	let result = pending.resolve(&ctx, "apply").await.expect("apply move");
	assert!(!result.is_error, "{}", result.content);

	let dest = dir.path.join("dest.txt");
	assert!(dest.exists());
	let meta = fs::metadata(&dest).expect("metadata");
	assert_eq!(meta.permissions().mode() & 0o777, 0o600, "destination must inherit mode 0600 from source");
	assert_eq!(fs::read_to_string(&dest).expect("read"), "updated secret\n");
	assert!(!src.exists(), "source must be unlinked after successful move");
}

#[tokio::test]
async fn modified_source_during_move_preserves_source_and_reports_partial() {
	let dir = TempDir::new("move-partial");
	let src = dir.write("src.txt", "version 1\n");
	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Patch)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	let patch_input = json!({
		"path": "src.txt",
		"edits": [
			{
				"path": "src.txt",
				"rename": "dest.txt",
				"diff": "@@ -1,1 +1,1 @@\n-version 1\n+version 2\n"
			}
		]
	});

	tool.execute(&patch_input, &ctx).await.expect("stage move");
	assert_eq!(pending.len(), 1);

	// Modify source right before apply
	fs::write(&src, "version 1 externally modified\n").expect("modify source");

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	// Gate 1 detects modified source and aborts with zero files modified
	assert!(result.is_error, "stale source must be rejected");
	assert!(!dir.path.join("dest.txt").exists(), "dest must not be created on Gate 1 abort");
	assert_eq!(fs::read_to_string(&src).expect("read"), "version 1 externally modified\n");
}

#[tokio::test]
async fn modified_source_during_delete_is_not_unlinked() {
	let dir = TempDir::new("delete-stale");
	let src = dir.write("del.txt", "to be deleted\n");
	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Patch)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	let patch_input = json!({
		"path": "del.txt",
		"edits": [
			{
				"path": "del.txt",
				"op": "delete"
			}
		]
	});

	tool.execute(&patch_input, &ctx).await.expect("stage delete");
	assert_eq!(pending.len(), 1);

	// Concurrently modify del.txt
	fs::write(&src, "important data modified concurrently\n").expect("modify del.txt");

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(result.is_error, "stale delete must be rejected");
	assert!(src.exists(), "source file must not be unlinked when stale");
	assert_eq!(fs::read_to_string(&src).expect("read"), "important data modified concurrently\n");
}

#[tokio::test]
async fn hardlink_temp_is_cleaned_up_after_successful_publication() {
	let dir = TempDir::new("temp-cleanup");
	let pending = Arc::new(PendingStore::new());
	let tool = WriteTool::with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	tool.execute(&json!({ "path": "published.txt", "content": "hello world\n" }), &ctx)
		.await
		.expect("stage");
	let result = pending.resolve(&ctx, "apply").await.expect("apply");
	assert!(!result.is_error, "{}", result.content);
	assert_eq!(dir.read("published.txt"), "hello world\n");

	// Verify no temporary files remain in dir
	let entries: Vec<String> = fs::read_dir(&dir.path)
		.expect("read dir")
		.map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
		.filter(|name| name.contains(".tmp-"))
		.collect();
	assert!(entries.is_empty(), "temporary hardlink files must not remain after publish: {entries:?}");
}

#[cfg(unix)]
#[tokio::test]
async fn move_destination_dir_pin_mismatch_fails_closed() {
	let dir = TempDir::new("move-dest-retarget");
	let src = dir.write("src.txt", "data to move\n");

	let real_dest_dir = dir.path.join("real_dest");
	fs::create_dir_all(&real_dest_dir).expect("real dest dir");

	// Link dest_link -> real_dest
	let link_dest_dir = dir.path.join("dest_link");
	std::os::unix::fs::symlink(&real_dest_dir, &link_dest_dir).expect("symlink dest dir");

	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Patch)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	let patch_input = json!({
		"path": "src.txt",
		"edits": [
			{
				"path": "src.txt",
				"rename": "dest_link/moved.txt",
				"diff": "@@ -1,1 +1,1 @@\n-data to move\n+data moved\n"
			}
		]
	});

	tool.execute(&patch_input, &ctx).await.expect("stage move");
	assert_eq!(pending.len(), 1);

	// Retarget dest_link to trap dir before apply
	let trap_dir = dir.path.join("trap_dest");
	fs::create_dir_all(&trap_dir).expect("trap dir");
	fs::remove_file(&link_dest_dir).expect("remove dest_link");
	std::os::unix::fs::symlink(&trap_dir, &link_dest_dir).expect("retarget dest link");

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(result.is_error, "move to retargeted destination directory must fail closed");
	assert!(result.content.contains("destination") || result.content.contains("parent directory"), "{}", result.content);
	assert!(src.exists(), "source file must not be modified or unlinked");
	assert!(!trap_dir.join("moved.txt").exists(), "trap dir must not receive the moved file");
}

#[cfg(unix)]
#[tokio::test]
async fn update_rejects_concurrent_leaf_replacement_immediately_before_rename() {
	let dir = TempDir::new("update-leaf-replace");
	let path = dir.write("target.txt", "v1 content\n");

	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Replace)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	tool.execute(&json!({ "path": "target.txt", "old_string": "v1 content", "new_string": "v2 content" }), &ctx)
		.await
		.expect("stage");

	// Replace the leaf file with a new file (different inode) before apply
	let replacement = dir.write("replacement.txt", "v1 content\n");
	fs::rename(&replacement, &path).expect("replace leaf file with different inode");

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(result.is_error, "replacing leaf file must be detected and rejected");
	assert!(result.content.contains("dev/ino") || result.content.contains("stale"), "{}", result.content);
}

#[tokio::test]
async fn update_rejects_in_place_byte_modification_before_rename() {
	let dir = TempDir::new("update-inplace-bytes");
	let path = dir.write("target.txt", "original v1 bytes\n");

	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Replace)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	tool.execute(&json!({ "path": "target.txt", "old_string": "original v1 bytes", "new_string": "v2 content" }), &ctx)
		.await
		.expect("stage");

	// Modify the file IN-PLACE without changing inode!
	{
		use std::io::Write;
		let mut f = fs::OpenOptions::new().write(true).open(&path).expect("open target.txt");
		f.write_all(b"tampered v1 bytes\n").expect("in-place overwrite");
		f.flush().expect("flush");
	}

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(result.is_error, "in-place byte modification before rename must be detected and rejected");
	assert!(result.content.contains("in-place") || result.content.contains("changed") || result.content.contains("stale"), "{}", result.content);
	assert_eq!(fs::read_to_string(&path).expect("read"), "tampered v1 bytes\n");
}

#[tokio::test]
async fn delete_rejects_in_place_byte_modification_before_unlink() {
	let dir = TempDir::new("delete-inplace-bytes");
	let path = dir.write("del.txt", "original delete content\n");

	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Patch)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	let patch_input = json!({
		"path": "del.txt",
		"edits": [
			{
				"path": "del.txt",
				"diff": "@@ -1,1 +0,0 @@\n-original delete content\n"
			}
		]
	});

	tool.execute(&patch_input, &ctx).await.expect("stage delete");
	assert_eq!(pending.len(), 1);

	// In-place byte overwrite
	{
		use std::io::Write;
		let mut f = fs::OpenOptions::new().write(true).open(&path).expect("open del.txt");
		f.write_all(b"in-place changed content\n").expect("in-place overwrite");
		f.flush().expect("flush");
	}

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(result.is_error, "in-place byte modification before delete must be detected and rejected");
	assert!(path.exists(), "source file must not be deleted when bytes changed in-place");
	assert_eq!(fs::read_to_string(&path).expect("read"), "in-place changed content\n");
}

#[tokio::test]
async fn move_source_in_place_byte_modification_yields_partial_outcome() {
	let dir = TempDir::new("move-inplace-bytes");
	let src = dir.write("src.txt", "data to move\n");
	let dest = dir.path.join("dest.txt");

	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Patch)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	let patch_input = json!({
		"path": "src.txt",
		"edits": [
			{
				"path": "src.txt",
				"rename": "dest.txt",
				"diff": "@@ -1,1 +1,1 @@\n-data to move\n+data moved\n"
			}
		]
	});

	tool.execute(&patch_input, &ctx).await.expect("stage move");
	assert_eq!(pending.len(), 1);

	// In-place byte overwrite of source before apply
	{
		use std::io::Write;
		let mut f = fs::OpenOptions::new().write(true).open(&src).expect("open src.txt");
		f.write_all(b"tampered data\n").expect("in-place overwrite");
		f.flush().expect("flush");
	}

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(result.is_error, "move with tampered source must fail or yield partial outcome");
	assert!(src.exists(), "source file must not be unlinked when bytes changed in-place");
	assert_eq!(fs::read_to_string(&src).expect("read"), "tampered data\n");
}

#[cfg(unix)]
#[tokio::test]
async fn staged_mode_stale_after_chmod_fails_closed() {
	use std::os::unix::fs::PermissionsExt;

	let dir = TempDir::new("stale-mode");
	let path = dir.write("sensitive.txt", "v1 content\n");

	// Set mode 0o644 initially
	fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("set initial mode 0644");

	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Replace)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	tool.execute(&json!({ "path": "sensitive.txt", "old_string": "v1 content", "new_string": "v2 content" }), &ctx)
		.await
		.expect("stage");

	// User restricts access to 0o600 before apply
	fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(result.is_error, "stale mode after chmod must fail closed");
	assert!(result.content.contains("mode changed") || result.content.contains("stale"), "{}", result.content);

	// Verify original file and permissions were strictly preserved
	assert_eq!(fs::read_to_string(&path).expect("read"), "v1 content\n");
	let current_mode = fs::metadata(&path).expect("meta").permissions().mode() & 0o777;
	assert_eq!(current_mode, 0o600, "0600 mode must be preserved, not overwritten with stale 0644");
}

#[cfg(unix)]
#[tokio::test]
async fn staged_move_source_mode_stale_after_chmod_fails_closed() {
	use std::os::unix::fs::PermissionsExt;

	let dir = TempDir::new("move-stale-mode");
	let src = dir.write("src.txt", "data to move\n");
	let dest = dir.path.join("dest.txt");
	fs::set_permissions(&src, fs::Permissions::from_mode(0o644)).expect("set 0644");

	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Patch)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	let patch_input = json!({
		"path": "src.txt",
		"edits": [
			{
				"path": "src.txt",
				"rename": "dest.txt",
				"diff": "@@ -1,1 +1,1 @@\n-data to move\n+data moved\n"
			}
		]
	});

	tool.execute(&patch_input, &ctx).await.expect("stage move");
	assert_eq!(pending.len(), 1);

	// Modify mode to 0600 before apply
	fs::set_permissions(&src, fs::Permissions::from_mode(0o600)).expect("chmod 0600");

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(result.is_error, "move with stale mode must fail closed");
	assert!(result.content.contains("mode changed") || result.content.contains("stale"), "{}", result.content);
	assert!(src.exists(), "source file must not be deleted");
	assert!(!dest.exists(), "dest file must not be published when source mode is stale");
	assert_eq!(fs::metadata(&src).expect("meta").permissions().mode() & 0o777, 0o600);
}

#[tokio::test]
async fn gate_one_is_strictly_read_only_and_does_not_create_directories() {
	let dir = TempDir::new("gate1-readonly");
	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = WriteTool::with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	// Stage a write into a nonexistent directory
	let nonexistent_file = "nonexistent_dir/sub/new_file.txt";
	tool.execute(&json!({ "path": nonexistent_file, "content": "hello\n" }), &ctx)
		.await
		.expect("stage write");

	// Also stage an invalid update that is guaranteed to fail Gate 2
	let op_fail = pi_agent::tools::staged_ops::PreparedOp::new_update(
		dir.path.join("nonexistent_base.txt"),
		"nonexistent_base.txt".to_string(),
		b"expected".to_vec(),
		None,
		b"new".to_vec(),
		None,
	);
	let action = pi_agent::tools::staged_ops::FileOpsAction::new(
		"write",
		"failing batch",
		vec![
			pi_agent::tools::staged_ops::PreparedOp::new_create(
				dir.path.join(nonexistent_file),
				nonexistent_file.to_string(),
				b"hello\n".to_vec(),
				None,
			),
			op_fail,
		],
		Arc::clone(&snapshots),
	);
	pending.register(Arc::new(action));

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(result.is_error, "batch must fail");
	assert!(result.content.contains("Preview is stale / no longer matches"));

	// Gate 1 must NOT have created the directory!
	assert!(!dir.path.join("nonexistent_dir").exists(), "Gate 1 must be strictly read-only and not create directories on failure");
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_delete_unlinks_link_and_preserves_target() {
	let dir = TempDir::new("symlink-delete-preserves-target");
	let real_file = dir.write("real_target.txt", "critical target data\n");
	let link_file = dir.path.join("link_to_target.txt");
	std::os::unix::fs::symlink(&real_file, &link_file).expect("symlink");

	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let tool = EditTool::with_mode(snapshots, pi_agent::tools::hashline::edit::EditToolOptions::default(), EditMode::Patch)
		.with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	let patch_input = json!({
		"path": "link_to_target.txt",
		"edits": [
			{
				"op": "delete",
				"path": "link_to_target.txt",
				"diff": ""
			}
		]
	});

	tool.execute(&patch_input, &ctx).await.expect("stage delete");
	assert_eq!(pending.len(), 1);

	let result = pending.resolve(&ctx, "apply").await.expect("resolve");
	assert!(!result.is_error, "{}", result.content);

	// The symlink must be unlinked
	assert!(std::fs::symlink_metadata(&link_file).is_err(), "symlink itself must be unlinked");
	// The real target file MUST be preserved!
	assert!(real_file.exists(), "real target file must not be deleted when unlinking symlink");
	assert_eq!(fs::read_to_string(&real_file).expect("read"), "critical target data\n");
}

#[tokio::test]
async fn existing_directory_not_counted_in_created_directories() {
	let dir = TempDir::new("existing-dir-accounting");
	let existing = dir.path.join("already_exists");
	fs::create_dir_all(&existing).expect("create existing dir");

	let pending = Arc::new(PendingStore::new());
	let tool = WriteTool::with_pending(Arc::clone(&pending));
	let ctx = ctx(&dir);

	tool.execute(&json!({ "path": "already_exists/new_sub/file.txt", "content": "test\n" }), &ctx)
		.await
		.expect("stage write");

	let result = pending.resolve(&ctx, "apply").await.expect("apply");
	assert!(!result.is_error, "{}", result.content);

	let details = result.details.expect("details");
	let source = &details["sourceResultDetails"];
	let created_dirs = source["createdDirectories"].as_array().expect("created dirs array");
	// already_exists was pre-existing, so it must not be in createdDirectories
	assert!(created_dirs.iter().all(|d| !d.as_str().unwrap().ends_with("already_exists")));
	assert!(created_dirs.iter().any(|d| d.as_str().unwrap().contains("new_sub")));
}

#[tokio::test]
async fn retry_directory_accounting_tracks_attempt_delta() {
	let dir = TempDir::new("retry-dir-accounting");
	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let ctx = ctx(&dir);

	let op1 = pi_agent::tools::staged_ops::PreparedOp::new_create(
		dir.path.join("brand_new_dir/file1.txt"),
		"brand_new_dir/file1.txt".to_string(),
		b"content 1\n".to_vec(),
		None,
	);
	// Op 2 has a filename whose temp sibling exceeds NAME_MAX, so Gate 1 passes
	// but create_temp_file fails at commit time after op1 created brand_new_dir
	let blocked_name = format!("{}.txt", "x".repeat(237));
	let op2_path = dir.path.join(format!("brand_new_dir/{blocked_name}"));
	let op2 = pi_agent::tools::staged_ops::PreparedOp::new_create(
		op2_path.clone(),
		format!("brand_new_dir/{blocked_name}"),
		b"content 2\n".to_vec(),
		None,
	);

	let action = Arc::new(pi_agent::tools::staged_ops::FileOpsAction::new(
		"write",
		"two files",
		vec![op1, op2],
		Arc::clone(&snapshots),
	));
	pending.register(action);

	// First apply attempt: op1 creates brand_new_dir and file1.txt, op2 fails commit
	let res1 = pending.resolve(&ctx, "apply").await.expect("first apply");
	assert!(res1.is_error);
	let details1 = res1.details.expect("details 1");
	let source1 = &details1["sourceResultDetails"];
	assert_eq!(source1["partial"], json!(true));
	let created1 = source1["createdDirectories"].as_array().expect("created dirs");
	assert!(created1.iter().any(|d| d.as_str().unwrap().contains("brand_new_dir")));
	let attempt_created1 = source1["attemptCreatedDirectories"].as_array().expect("attempt created");
	assert!(attempt_created1.iter().any(|d| d.as_str().unwrap().contains("brand_new_dir")));

	// Second apply attempt (retry)
	let res2 = pending.resolve(&ctx, "apply").await.expect("second apply");
	assert!(res2.is_error);
	let details2 = res2.details.expect("details 2");
	let source2 = &details2["sourceResultDetails"];
	// On retry, brand_new_dir already existed, so attemptCreatedDirectories must be empty
	assert_eq!(source2["attemptCreatedDirectories"], json!([]));
	let created2 = source2["createdDirectories"].as_array().expect("cumulative created dirs");
	assert!(created2.iter().any(|d| d.as_str().unwrap().contains("brand_new_dir")));
}

#[tokio::test]
async fn reject_reports_cumulative_created_directories_without_rollback() {
	let dir = TempDir::new("reject-dir-reporting");
	let pending = Arc::new(PendingStore::new());
	let snapshots = Arc::new(SnapshotStore::new());
	let ctx = ctx(&dir);

	let op1_path = dir.path.join("partial_created_dir/file1.txt");
	let op1 = pi_agent::tools::staged_ops::PreparedOp::new_create(
		op1_path.clone(),
		"partial_created_dir/file1.txt".to_string(),
		b"hello\n".to_vec(),
		None,
	);
	// Op 2 fails at commit time after op1 creates partial_created_dir
	let blocked_name = format!("{}.txt", "x".repeat(237));
	let op2_path = dir.path.join(format!("partial_created_dir/{blocked_name}"));
	let op2 = pi_agent::tools::staged_ops::PreparedOp::new_create(
		op2_path,
		format!("partial_created_dir/{blocked_name}"),
		b"content 2\n".to_vec(),
		None,
	);

	let action = Arc::new(pi_agent::tools::staged_ops::FileOpsAction::new(
		"write",
		"failing batch with dir",
		vec![op1, op2],
		Arc::clone(&snapshots),
	));
	pending.register(action);

	// First apply: op1 creates partial_created_dir and publishes file1, then op2 fails commit
	let res_apply = pending.resolve(&ctx, "apply").await.expect("apply");
	assert!(res_apply.is_error);

	// Now user rejects the remaining pending batch
	let res_reject = pending.reject(&ctx, "reject").await.expect("reject");
	assert!(!res_reject.is_error);
	assert!(res_reject.content.contains("previously created by an earlier attempt remain in workspace"));
	assert!(res_reject.content.contains("discard does not roll back directory creation"));

	let details = res_reject.details.expect("details");
	let source = &details["sourceResultDetails"];
	assert_eq!(source["action"], json!("discard"));
	let created_dirs = source["createdDirectories"].as_array().expect("created directories array");
	assert!(created_dirs.iter().any(|d| d.as_str().unwrap().contains("partial_created_dir")));
	assert_eq!(source["directoriesRolledBack"], json!(false));
}
