//! Real-CLI security-scan fixture.
//!
//! Runs `scripts/security-scan-home-harness.sh` against the just-built `omp`
//! binary with an isolated `$HOME`, a real git repository, a real SQLite auth
//! store, and a localhost OpenAI-compatible provider. It exercises the actual
//! `RuntimeSecurityHost`/`PiAgentRunner` path (not a test host): default
//! `security_scan` selection, preflight, start, restricted worker, publication,
//! `security://` reads, persistent worker journal, and parent `lsp.enabled:
//! false` with default `always-ask` approvals.

use std::path::PathBuf;
use std::process::Command;

#[test]
fn real_cli_security_scan_harness_passes() {
	// The harness writes its whole fixture under a disposable root. Use `$HOME`
	// (never a shared `/tmp`) and remove only that root on exit.
	let base = std::env::var_os("HOME")
		.map(PathBuf::from)
		.unwrap_or_else(std::env::temp_dir);
	let root = base.join(format!(
		".omp-security-runtime-harness-{}",
		std::process::id()
	));
	let _ = std::fs::remove_dir_all(&root);
	std::fs::create_dir_all(&root).expect("create harness home");
	let script: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("../..")
		.join("scripts/security-scan-home-harness.sh");
	let output = Command::new("bash")
		.arg(&script)
		.env("OMP_BIN", env!("CARGO_BIN_EXE_omp"))
		.env("HOME", &root)
		.env("OMP_SECURITY_HARNESS_ROOT", root.join("run"))
		.output()
		.expect("run security harness");
	let stdout = String::from_utf8_lossy(&output.stdout);
	let stderr = String::from_utf8_lossy(&output.stderr);
	if !output.status.success() || !stdout.contains("security harness passed") {
		panic!(
			"harness failed (root kept at {})\nstdout:\n{stdout}\nstderr:\n{stderr}",
			root.display()
		);
	}
	let _ = std::fs::remove_dir_all(&root);
}
