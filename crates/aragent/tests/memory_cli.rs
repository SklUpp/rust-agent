//! Consumer regression for D53 memory management through the real `arag`
//! binary: create/list/search/get/update/delete for global and project scope,
//! user-asserted provenance, scope isolation between projects, and a fresh
//! process reopening the same SQLite store.

use std::{
	fs,
	path::{Path, PathBuf},
	process::Command,
	sync::atomic::{AtomicU32, Ordering},
};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_dir(prefix: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"omp-memory-cli-{prefix}-{}-{}",
		std::process::id(),
		COUNTER.fetch_add(1, Ordering::Relaxed)
	));
	fs::create_dir_all(&dir).expect("create temp dir");
	dir
}

struct Fixture {
	agent:   PathBuf,
	project: PathBuf,
	other:   PathBuf,
}

impl Fixture {
	fn new() -> Self {
		let root = temp_dir("fixture");
		let agent = root.join("agent");
		let project = root.join("project");
		let other = root.join("other");
		for dir in [&agent, &project, &other] {
			fs::create_dir_all(dir).expect("fixture dir");
		}
		for dir in [agent.join("config"), agent.join("share")] {
			fs::create_dir_all(dir).expect("env dir");
		}
		fs::write(agent.join("config.yml"), "memory:\n  backend: local\n").expect("config");
		Self { agent, project, other }
	}

	fn run(&self, cwd: &Path, args: &[&str]) -> String {
		let output = Command::new(env!("CARGO_BIN_EXE_arag"))
			.env("PI_CODING_AGENT_DIR", &self.agent)
			.env("PI_CONFIG_DIR", self.agent.join("config"))
			.env("OMP_SHARE_DIR", self.agent.join("share"))
			.stdin(std::process::Stdio::null())
			.args(["--cwd", cwd.to_str().expect("cwd")])
			.args(args)
			.output()
			.expect("run arag");
		assert!(
			output.status.success(),
			"arag {args:?} failed: {}",
			String::from_utf8_lossy(&output.stderr)
		);
		String::from_utf8_lossy(&output.stdout).into_owned()
	}

	fn run_err(&self, cwd: &Path, args: &[&str]) -> String {
		let output = Command::new(env!("CARGO_BIN_EXE_arag"))
			.env("PI_CODING_AGENT_DIR", &self.agent)
			.env("PI_CONFIG_DIR", self.agent.join("config"))
			.env("OMP_SHARE_DIR", self.agent.join("share"))
			.stdin(std::process::Stdio::null())
			.args(["--cwd", cwd.to_str().expect("cwd")])
			.args(args)
			.output()
			.expect("run arag");
		assert!(!output.status.success(), "arag {args:?} unexpectedly succeeded");
		String::from_utf8_lossy(&output.stderr).into_owned()
	}
}

fn id_from_list(list: &str, need: &str) -> String {
	list
		.lines()
		.find(|line| line.starts_with("- ") && line.contains(need))
		.and_then(|line| line.strip_prefix("- "))
		.and_then(|line| line.split_whitespace().next())
		.expect("record id")
		.to_string()
}

#[test]
fn user_crud_is_scope_isolated_and_survives_reopen() {
	let fixture = Fixture::new();

	let global = fixture.run(&fixture.project, &[
		"memory",
		"create",
		"--global",
		"owner prefers terse answers",
	]);
	assert!(global.contains("[global] origin=asserted actor=user"), "{global}");
	let project = fixture.run(&fixture.project, &["memory", "create", "run formatter"]);
	assert!(project.contains("[project:"), "{project}");
	assert!(project.contains("origin=asserted"), "{project}");

	let list = fixture.run(&fixture.project, &["memory", "list"]);
	assert!(list.contains("[global] origin=asserted"), "{list}");
	assert!(list.contains("owner prefers terse answers"), "{list}");
	assert!(list.contains("run formatter"), "{list}");

	// Another project sees the global assertion but never this project's row.
	let other = fixture.run(&fixture.other, &["memory", "list"]);
	assert!(other.contains("owner prefers terse answers"), "{other}");
	assert!(!other.contains("run formatter"), "{other}");
	let other_search = fixture.run(&fixture.other, &["memory", "search", "formatter"]);
	assert!(other_search.contains("No memory records"), "{other_search}");
	let other_global_search =
		fixture.run(&fixture.other, &["memory", "search", "--global", "formatter"]);
	assert!(
		other_global_search.contains("No memory records"),
		"global search must not return project rows: {other_global_search}"
	);

	// `arag read memory://list` uses the same handler and backend gate as a
	// headless session.
	let read = fixture.run(&fixture.project, &["read", "memory://list"]);
	assert!(read.contains("[global] origin=asserted"), "{read}");
	assert!(read.contains("[project:"), "{read}");
	assert!(read.contains("run formatter"), "{read}");

	let id = id_from_list(&list, "[project:");
	let updated = fixture.run(&fixture.project, &[
		"memory",
		"update",
		id.as_str(),
		"run tests before commit",
	]);
	assert!(updated.contains(&format!("Updated {id}")), "{updated}");
	let got = fixture.run(&fixture.project, &["memory", "get", id.as_str()]);
	assert!(got.contains("origin: asserted"), "{got}");
	assert!(got.contains("run tests before commit"), "{got}");

	// A fresh process (reopen) reads the persisted edit.
	let reopened = fixture.run(&fixture.project, &["memory", "get", id.as_str()]);
	assert!(reopened.contains("run tests before commit"), "{reopened}");

	let deleted = fixture.run(&fixture.project, &["memory", "delete", id.as_str()]);
	assert!(deleted.contains("[project:"), "{deleted}");
	assert!(deleted.contains("origin=asserted"), "{deleted}");
	assert!(deleted.contains("Conversation history is untouched"), "{deleted}");
	let missing = fixture.run_err(&fixture.project, &["memory", "get", id.as_str()]);
	assert!(missing.contains("not found"), "{missing}");
	let learned = fs::read_to_string(
		aragent::local_memory::memory_root(&fixture.agent, &fixture.project).join("learned.md"),
	)
	.unwrap_or_default();
	assert!(!learned.contains("run tests before commit"), "{learned}");
	assert!(!learned.contains("run formatter"), "{learned}");
}

#[test]
fn backend_off_is_an_explicit_error() {
	let root = temp_dir("off");
	let agent = root.join("agent");
	let cwd = root.join("project");
	fs::create_dir_all(&agent).expect("agent");
	fs::create_dir_all(&cwd).expect("cwd");
	fs::create_dir_all(agent.join("config")).expect("config dir");
	fs::create_dir_all(agent.join("share")).expect("share dir");
	fs::write(agent.join("config.yml"), "memory:\n  backend: off\n").expect("config");
	let output = Command::new(env!("CARGO_BIN_EXE_arag"))
		.env("PI_CODING_AGENT_DIR", &agent)
		.env("PI_CONFIG_DIR", agent.join("config"))
		.env("OMP_SHARE_DIR", agent.join("share"))
		.stdin(std::process::Stdio::null())
		.args(["--cwd", cwd.to_str().expect("cwd")])
		.args(["memory", "list"])
		.output()
		.expect("run arag");
	assert!(!output.status.success());
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(stderr.contains("memory.backend: local"), "{stderr}");
}
