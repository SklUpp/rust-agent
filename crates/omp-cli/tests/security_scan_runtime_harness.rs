//! Real-CLI security-scan fixture.
//!
//! Runs `scripts/security-scan-home-harness.sh` against the just-built `omp`
//! binary with an isolated `$HOME`, a real git repository, a real SQLite auth
//! store, and a localhost OpenAI-compatible provider. It exercises the actual
//! `RuntimeSecurityHost`/`PiAgentRunner` path (not a test host): default
//! `security_scan` selection, preflight, start, restricted worker, publication,
//! `security://` reads, persistent worker journal, and parent `lsp.enabled:
//! false` with default `always-ask` approvals.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn create_unique_harness_root() -> PathBuf {
	let base = std::env::var_os("HOME")
		.map(PathBuf::from)
		.unwrap_or_else(std::env::temp_dir);
	let pid = std::process::id();
	let now = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|d| d.as_nanos())
		.unwrap_or(0);
	for _ in 0..100 {
		let count = COUNTER.fetch_add(1, Ordering::Relaxed);
		let candidate = base.join(format!(".omp-security-runtime-harness-{pid}-{now}-{count}"));
		match fs::create_dir(&candidate) {
			Ok(()) => return candidate,
			Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
			Err(err) => panic!("cannot create unique harness root {}: {err}", candidate.display()),
		}
	}
	panic!("exhausted unique harness root attempts under {}", base.display());
}

#[test]
fn real_cli_security_scan_harness_passes() {
	let root = create_unique_harness_root();
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
