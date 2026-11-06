//! Deterministic loopback test for the `issue://` / `pr://` consumers.
//!
//! A fake `gh` executable placed first on `PATH` records its argv and replays
//! canned stdout/stderr/exit codes, so the subprocess contract is asserted
//! without a real `gh`, network, or credentials: exact argv for each readonly
//! subcommand, stdout surfaced as resource content, `gh` failures mapped to
//! their CLI-contract error strings, and no credential text leaking out of the
//! auth failure mapping.
//!
//! One test function per binary on purpose: `PATH` is process-global, so a
//! single sequential test keeps the injection invisible to anything else.
//!
//! `ponytail:` the fake models only the readonly subcommands this path invokes
//! (`issue view`, `pr view`, `repo view`); anything else exits 127, and
//! mutations never reach `gh` because the router refuses them first.

#![cfg(unix)]

use std::{
	collections::BTreeMap,
	ffi::OsString,
	fs,
	path::{Path, PathBuf},
	sync::atomic::{AtomicUsize, Ordering},
};

use pi_agent::{
	CancellationToken,
	tools::internal_urls::{InternalResource, InternalUrlRouter, ResolveContext, WriteContext},
};

/// `gh issue view --json` field list for a view with comments disabled.
const ISSUE_FIELDS_NO_COMMENTS: &str =
	"author,body,createdAt,labels,number,state,stateReason,title,updatedAt,url";
/// `gh pr view --json` field list for a view with comments disabled.
const PR_FIELDS_NO_COMMENTS: &str = "author,baseRefName,body,createdAt,files,headRefName,\
                                     isDraft,labels,mergeStateStatus,number,reviews,reviewDecision,\
                                     state,title,updatedAt,url";

const ISSUE_JSON: &str = r#"{
  "author": {"login": "octocat"},
  "body": "Body text from fake gh.",
  "createdAt": "2026-01-01T00:00:00Z",
  "labels": [],
  "number": 7,
  "state": "OPEN",
  "stateReason": null,
  "title": "Fake issue title",
  "updatedAt": "2026-01-02T00:00:00Z",
  "url": "https://github.com/o/r/issues/7"
}
"#;

const PR_JSON: &str = r#"{
  "author": {"login": "octocat"},
  "baseRefName": "main",
  "body": "PR body from fake gh.",
  "createdAt": "2026-01-01T00:00:00Z",
  "files": [],
  "headRefName": "feat",
  "isDraft": false,
  "labels": [],
  "mergeStateStatus": "CLEAN",
  "number": 5,
  "reviews": [],
  "reviewDecision": null,
  "state": "OPEN",
  "title": "Fake PR title",
  "updatedAt": "2026-01-02T00:00:00Z",
  "url": "https://github.com/o/r/pull/5"
}
"#;

fn argv(parts: &[&str]) -> Vec<String> {
	parts.iter().map(|part| (*part).to_string()).collect()
}

/// One recorded `gh` invocation: the argv after `argv[0]` plus the cwd the
/// child observed.
#[derive(Debug, Default)]
struct Call {
	args: Vec<String>,
	cwd:  String,
}

/// Fake `gh` harness: owns the temp tree, the `PATH`/`GH_HOST` overrides, and
/// the fixture files the script dispatches on.
struct FakeGh {
	root:     PathBuf,
	bin:      PathBuf,
	scratch:  PathBuf,
	old_path: Option<OsString>,
	old_host: Option<OsString>,
}

impl FakeGh {
	fn new() -> Self {
		static NEXT: AtomicUsize = AtomicUsize::new(0);
		let root = std::env::temp_dir().join(format!(
			"pi-agent-gh-loopback-{}-{}",
			std::process::id(),
			NEXT.fetch_add(1, Ordering::SeqCst)
		));
		let bin = root.join("bin");
		let scratch = root.join("scratch");
		fs::create_dir_all(&bin).expect("create fake gh dir");
		fs::create_dir_all(&scratch).expect("create scratch cwd");

		let old_path = std::env::var_os("PATH");
		let old_host = std::env::var_os("GH_HOST");
		let path = match &old_path {
			Some(existing) => format!("{}:{}", bin.display(), existing.to_string_lossy()),
			// The script is extern-free, so the fake dir alone is a valid PATH.
			None => bin.display().to_string(),
		};
		// SAFETY: this test binary contains exactly one test, and the
		// current-thread runtime is built only after these mutations, so no
		// other thread can read the environment concurrently.
		unsafe {
			std::env::set_var("PATH", &path);
			// Pin the host so `repo view` URLs map to bare `owner/repo`.
			std::env::set_var("GH_HOST", "github.com");
		}

		let fake = Self { root, bin, scratch, old_path, old_host };
		fake.write_script();
		fake
	}

	/// POSIX `sh` script: log argv, then replay `err.<sub>.<verb>` (nonzero
	/// exit) or `out.<sub>.<verb>` (success), both keyed off the first two
	/// arguments. Uses shell builtins only, so no inherited `PATH` is needed.
	fn write_script(&self) {
		let root = self.root.display().to_string();
		assert!(!root.contains('\''), "temp path must not contain a quote: {root}");
		let script = format!(
			r#"#!/bin/sh
set -u
root='{root}'
{{
	printf '%s\n' '---'
	printf '%s\n' "$@"
	printf '%s\n' "---cwd=$(pwd -P)"
}} >> "$root/calls.log"
key="${{1:-}}.${{2:-}}"
if [ -f "$root/err.$key" ]; then
	while IFS= read -r line; do
		printf '%s\n' "$line"
	done < "$root/err.$key" >&2
	exit 4
fi
if [ -f "$root/out.$key" ]; then
	while IFS= read -r line; do
		printf '%s\n' "$line"
	done < "$root/out.$key"
	exit 0
fi
printf 'fake gh: unmodelled invocation: %s\n' "$key" >&2
exit 127
"#,
			root = root
		);
		let executable = self.bin.join("gh");
		fs::write(&executable, script).expect("write fake gh");
		use std::os::unix::fs::PermissionsExt;
		fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).expect("chmod fake gh");
	}

	fn set_stdout(&self, key: &str, content: &str) {
		fs::write(self.root.join(format!("out.{key}")), content).expect("write fake stdout");
	}

	fn set_stderr(&self, key: &str, content: &str) {
		fs::write(self.root.join(format!("err.{key}")), content).expect("write fake stderr");
	}

	/// Drop every fixture and the call log, keeping the fake executable.
	fn reset(&self) {
		for entry in fs::read_dir(&self.root).expect("read fake gh dir") {
			let path = entry.expect("fake gh dir entry").path();
			let name = path
				.file_name()
				.map_or_else(String::new, |name| name.to_string_lossy().into_owned());
			if name == "calls.log" || name.starts_with("out.") || name.starts_with("err.") {
				fs::remove_file(&path).expect("remove fixture");
			}
		}
	}

	fn calls(&self) -> Vec<Call> {
		let Ok(raw) = fs::read_to_string(self.root.join("calls.log")) else {
			return Vec::new();
		};
		let mut calls: Vec<Call> = Vec::new();
		let mut current: Option<Call> = None;
		for line in raw.lines() {
			if line == "---" {
				calls.extend(current.take());
				current = Some(Call::default());
			} else if let Some(cwd) = line.strip_prefix("---cwd=") {
				if let Some(call) = current.as_mut() {
					call.cwd = cwd.to_string();
				}
			} else if let Some(call) = current.as_mut() {
				call.args.push(line.to_string());
			}
		}
		calls.extend(current.take());
		calls
	}

	/// Assert exactly one invocation with this argv, run in the requested cwd.
	fn assert_single_call(&self, expected: &[&str]) {
		let calls = self.calls();
		assert_eq!(calls.len(), 1, "expected exactly one gh invocation, got {calls:?}");
		assert_eq!(calls[0].args, argv(expected), "gh argv mismatch");
		assert_eq!(
			fs::canonicalize(Path::new(&calls[0].cwd)).expect("recorded cwd exists"),
			fs::canonicalize(&self.scratch).expect("scratch cwd exists"),
			"gh must run in the requested cwd"
		);
	}

	fn resolve_context(&self) -> ResolveContext {
		ResolveContext {
			cwd: self.scratch.clone(),
			session_file: None,
			local_roots: BTreeMap::new(),
			signal: CancellationToken::new(),
		}
	}

	async fn resolve(
		&self,
		router: &InternalUrlRouter,
		href: &str,
	) -> Result<InternalResource, String> {
		router.resolve(href, &self.resolve_context()).await
	}
}

impl Drop for FakeGh {
	fn drop(&mut self) {
		// SAFETY: the only runtime in this binary is already dropped when the
		// harness drops, so no other thread reads the environment here.
		unsafe {
			match &self.old_path {
				Some(value) => std::env::set_var("PATH", value),
				None => std::env::remove_var("PATH"),
			}
			match &self.old_host {
				Some(value) => std::env::set_var("GH_HOST", value),
				None => std::env::remove_var("GH_HOST"),
			}
		}
		let _ = fs::remove_dir_all(&self.root);
	}
}

#[test]
fn gh_loopback_contract() {
	let fake = FakeGh::new();
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.expect("current-thread runtime");
	runtime.block_on(async {
		let router = InternalUrlRouter::default();

		// 1. `issue://` view: argv contract, stdout surfaced, resource shape.
		fake.reset();
		fake.set_stdout("issue.view", ISSUE_JSON);
		let resource = fake
			.resolve(&router, "issue://o/r/7?comments=0")
			.await
			.expect("issue view should succeed");
		assert_eq!(resource.url, "issue://o/r/7?comments=0");
		assert_eq!(resource.content_type, "text/markdown");
		assert!(
			resource.content.starts_with("# Issue #7: Fake issue title"),
			"stdout must surface as content: {}",
			resource.content
		);
		assert!(resource.content.contains("Body text from fake gh."));
		assert_eq!(
			resource.notes,
			Some(vec!["Fetched live".to_string(), "Comments disabled".to_string()])
		);
		assert_eq!(resource.size, resource.content.len());
		assert!(resource.immutable);
		fake.assert_single_call(&[
			"issue", "view", "7", "--repo", "o/r", "--json", ISSUE_FIELDS_NO_COMMENTS,
		]);

		// 2. `pr://` view: same contract for the PR subcommand.
		fake.reset();
		fake.set_stdout("pr.view", PR_JSON);
		let resource = fake
			.resolve(&router, "pr://o/r/5?comments=0")
			.await
			.expect("pr view should succeed");
		assert_eq!(resource.content_type, "text/markdown");
		assert!(
			resource.content.starts_with("# Pull Request #5: Fake PR title"),
			"stdout must surface as content: {}",
			resource.content
		);
		assert!(resource.content.contains("PR body from fake gh."));
		assert_eq!(
			resource.notes,
			Some(vec![
				"Fetched live".to_string(),
				"Comments disabled".to_string(),
				"Diff: pr://o/r/5/diff".to_string(),
			])
		);
		fake.assert_single_call(&[
			"pr", "view", "5", "--repo", "o/r", "--json", PR_FIELDS_NO_COMMENTS,
		]);

		// 3. Bare `issue://7` resolves the repo through `gh repo view` first,
		//    then passes the resolved slug to the view call.
		fake.reset();
		fake.set_stdout("repo.view", "https://github.com/acme/widgets\n");
		fake.set_stdout("issue.view", ISSUE_JSON);
		fake
			.resolve(&router, "issue://7?comments=0")
			.await
			.expect("issue view via default repo should succeed");
		let calls = fake.calls();
		assert_eq!(calls.len(), 2, "expected repo view then issue view: {calls:?}");
		assert_eq!(calls[0].args, argv(&["repo", "view", "--json", "url", "-q", ".url"]));
		assert_eq!(
			calls[1].args,
			argv(&[
				"issue",
				"view",
				"7",
				"--repo",
				"acme/widgets",
				"--json",
				ISSUE_FIELDS_NO_COMMENTS,
			])
		);

		// 4. Auth failure: mapped to the fixed prerequisite string, and the
		//    credential-looking stderr payload never reaches the caller.
		fake.reset();
		fake.set_stderr(
			"issue.view",
			"error: not logged into any GitHub hosts. Run gh auth login to authenticate.\n\
			 token=ghp_FAKEloopbackSECRET123\n",
		);
		let error = fake
			.resolve(&router, "issue://o/r/7?comments=0")
			.await
			.expect_err("auth failure must be an error");
		assert_eq!(
			error,
			"issue:// resolution failed: GitHub CLI is not authenticated. Run `gh auth login`."
		);
		assert!(
			!error.contains("ghp_") && !error.contains("FAKEloopback"),
			"auth mapping must not leak gh stderr: {error}"
		);

		// 5. Non-auth failure: `gh`'s own message surfaces verbatim, not a
		//    generic wrapper and not an empty result.
		fake.reset();
		fake.set_stderr(
			"pr.view",
			"error: could not resolve to a PullRequest with the number of 5.\n",
		);
		let error = fake
			.resolve(&router, "pr://o/r/5?comments=0")
			.await
			.expect_err("gh failure must be an error");
		assert_eq!(
			error,
			"pr:// resolution failed: error: could not resolve to a PullRequest with the number \
			 of 5."
		);

		// 6. Missing repository context for a bare `pr://5`: the prerequisite
		//    error is the CLI-contract one, and no view call follows it.
		fake.reset();
		fake.set_stderr(
			"repo.view",
			"fatal: not a git repository (or any of the parent directories): .git\n",
		);
		let error = fake
			.resolve(&router, "pr://5?comments=0")
			.await
			.expect_err("missing repo context must be an error");
		assert_eq!(
			error,
			"pr://5 could not resolve a default repo from the current session: GitHub repository \
			 context is unavailable. Pass `repo` explicitly or run the tool inside a GitHub \
			 checkout.\nUse pr://<owner>/<repo>/5."
		);
		assert_eq!(
			fake.calls().len(),
			1,
			"a failed repo resolution must stop before the view call"
		);

		// 7. Malformed stdout is rejected, never surfaced as content.
		fake.reset();
		fake.set_stdout("issue.view", "not json\n");
		let error = fake
			.resolve(&router, "issue://o/r/7?comments=0")
			.await
			.expect_err("invalid JSON must be an error");
		assert_eq!(
			error,
			"issue:// resolution failed: GitHub CLI returned invalid JSON output."
		);

		// 8. Mutations are refused by the router before any subprocess runs.
		fake.reset();
		let write_context = WriteContext {
			cwd: fake.scratch.clone(),
			local_roots: BTreeMap::new(),
			signal: CancellationToken::new(),
		};
		for (href, scheme) in [("issue://o/r/7", "issue"), ("pr://o/r/5", "pr")] {
			let error = router
				.write(href, "mutate", &write_context)
				.await
				.expect_err("GitHub URLs are read-only");
			assert_eq!(
				error,
				format!(
					"{scheme}:// URLs are read-only for write; use the protocol-specific tool for \
					 mutations."
				)
			);
		}
		assert!(fake.calls().is_empty(), "a refused write must not spawn gh");
	});
	// `runtime` drops before `fake`, so no thread observes the PATH restore.
}
