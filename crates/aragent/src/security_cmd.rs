//! `arag security` — import external security results and inspect the project
//! security store.
//!
//! Anchors: `packages/coding-agent/src/security/importers/*` and `store.ts`.
//! This is a thin translator: it opens the same [`SecurityStore`] the scan
//! coordinator uses and hands each importer the store's resolved repository
//! root, so an imported scan (SARIF or Codex Security bundle) lands under the
//! same project key as a native scan. Remediation, publication, and isolation
//! are not part of this surface.

use std::{
	io::Write,
	path::{Path, PathBuf},
};

use crate::{
	Error, dirs,
	local_cmd::escape_terminal_text,
	security::{
		SecurityFinding, SecurityScan, SecurityStore,
		importers::{
			codex_security::{CodexSecurityImportOptions, import_codex_security_bundle_into_store},
			sarif::{SarifImportOptions, import_sarif_file},
		},
	},
};

/// One parsed `arag security` invocation (clap owns the grammar).
pub struct Request {
	/// `import-sarif`, `import-codex`, `list`, or `show`.
	pub action: String,
	/// SARIF file, Codex bundle directory, or scan id (`show`).
	pub target: Option<PathBuf>,
	/// Repository root; defaults to the current directory's worktree.
	pub repo: Option<PathBuf>,
	/// Fixed scan id for imports.
	pub scan_id: Option<String>,
	/// Print machine-readable JSON.
	pub json: bool,
}

pub fn run(request: &Request, cwd: &Path, out: &mut impl Write) -> crate::Result<()> {
	run_with_state_root(request, cwd, None, out)
}

/// [`run`] with an explicit security state root; `None` uses the active
/// profile's state directory. The CLI passes `None`; tests and embedders use
/// this to keep the project store out of the real profile.
pub fn run_with_state_root(
	request: &Request,
	cwd: &Path,
	state_root: Option<&Path>,
	out: &mut impl Write,
) -> crate::Result<()> {
	let repository = match &request.repo {
		Some(repo) => dirs::absolute(cwd, repo),
		None => cwd.to_path_buf(),
	};
	let store = SecurityStore::open(&repository, state_root)?;
	match request.action.as_str() {
		"import-sarif" => {
			let target = import_target(request, cwd)?;
			let options = SarifImportOptions {
				repository_root: PathBuf::from(store.repository_root()),
				source_path: None,
				created_at: None,
				scan_id: request.scan_id.clone(),
			};
			let bundle = import_sarif_file(&target, &options)?;
			store.put_bundle(&bundle)?;
			write_import(&bundle.scan, &store, request.json, out)
		},
		"import-codex" => {
			let target = import_target(request, cwd)?;
			let options = CodexSecurityImportOptions {
				repository_root: PathBuf::from(store.repository_root()),
				created_at: None,
				scan_id: request.scan_id.clone(),
			};
			let bundle = import_codex_security_bundle_into_store(&target, &store, &options)?;
			write_import(&bundle.scan, &store, request.json, out)
		},
		"list" => {
			if request.target.is_some() {
				return Err(Error::new("security list takes no operand"));
			}
			let summaries = store.list_scans()?;
			if request.json {
				writeln!(out, "{}", json_text(&summaries)?)?;
			} else {
				for summary in &summaries {
					writeln!(
						out,
						"{}  {}  {} findings  {}  {}",
						summary.id,
						escape_terminal_text(&summary.status, false),
						summary.finding_count,
						escape_terminal_text(&summary.created_at, false),
						escape_terminal_text(&summary.producer.name, false),
					)?;
				}
			}
			Ok(())
		},
		"show" => {
			let id = require_target(request)?.to_string_lossy().into_owned();
			let bundle = store.get_bundle(&id)?.ok_or_else(|| {
				Error::new(format!("Unknown security scan: {id}"))
			})?;
			if request.json {
				writeln!(out, "{}", json_text(&bundle)?)?;
			} else {
				write_show(&bundle.scan, &bundle.findings, out)?;
			}
			Ok(())
		},
		other => Err(usage(other)),
	}
}

fn usage(action: &str) -> Error {
	Error::new(format!(
		"Unknown security action '{action}'. Expected: import-sarif | import-codex | list | show"
	))
}

fn require_target(request: &Request) -> crate::Result<&Path> {
	request.target.as_deref().ok_or_else(|| {
		Error::new(format!("security {} requires a PATH operand", request.action))
	})
}

/// Import operands are user paths relative to the effective `--cwd`, not to
/// the process working directory.
fn import_target(request: &Request, cwd: &Path) -> crate::Result<PathBuf> {
	let target = require_target(request)?;
	Ok(if target.is_relative() {
		cwd.join(target)
	} else {
		target.to_path_buf()
	})
}

fn write_import(
	scan: &SecurityScan,
	store: &SecurityStore,
	json: bool,
	out: &mut impl Write,
) -> crate::Result<()> {
	if json {
		writeln!(out, "{}", json_text(scan)?)?;
	} else {
		writeln!(
			out,
			"Imported {} into {}: {} findings",
			scan.id,
			escape_terminal_text(&store.project_directory().display().to_string(), false),
			scan.finding_ids.len(),
		)?;
	}
	Ok(())
}

fn write_show(
	scan: &SecurityScan,
	findings: &[SecurityFinding],
	out: &mut impl Write,
) -> crate::Result<()> {
	writeln!(
		out,
		"{} [{}] {} findings",
		scan.id,
		escape_terminal_text(&scan.status, false),
		findings.len(),
	)?;
	writeln!(out, "repository: {}", escape_terminal_text(&scan.target.repository_root, false))?;
	writeln!(
		out,
		"producer: {} {}",
		escape_terminal_text(&scan.producer.name, false),
		escape_terminal_text(scan.producer.version.as_deref().unwrap_or(""), false),
	)?;
	for finding in findings {
		let place = finding
			.occurrences
			.first()
			.and_then(|occurrence| occurrence.locations.first())
			.map_or_else(|| "unknown".to_string(), |location| {
				format!("{}:{}", escape_terminal_text(&location.path, false), location.start_line)
			});
		writeln!(
			out,
			"- {} {} {} [{}]",
			escape_terminal_text(&finding.severity.level, false),
			escape_terminal_text(&finding.rule_id, false),
			escape_terminal_text(&finding.title, false),
			place,
		)?;
	}
	Ok(())
}

fn json_text(value: &impl serde::Serialize) -> crate::Result<String> {
	serde_json::to_string_pretty(value)
		.map_err(|error| Error::new(format!("cannot encode security output: {error}")))
}

#[cfg(test)]
mod tests {
	use std::fs;

	use super::*;

	fn temp_dir(label: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"{label}-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	const SARIF: &str = r#"{
		"version": "2.1.0",
		"runs": [{
			"tool": {"driver": {"name": "demo", "version": "1.0"}},
			"results": [{
				"ruleId": "rust.unsafe",
				"message": {"text": "unsafe block"},
				"locations": [{"physicalLocation": {
					"artifactLocation": {"uri": "src/a.rs"},
					"region": {"startLine": 3}
				}}]
			}]
		}]
	}"#;

	fn request(action: &str, target: Option<&str>, json: bool) -> Request {
		Request {
			action:   action.to_string(),
			target:   target.map(PathBuf::from),
			repo:     None,
			scan_id:  Some("secscan_test01".to_string()),
			json,
		}
	}

	#[test]
	fn imports_lists_and_shows_a_sarif_scan() {
		let root = temp_dir("omp-security-cmd");
		let repo = root.join("repo");
		let state = root.join("state");
		fs::create_dir_all(repo.join("src")).expect("repo");
		let sarif = repo.join("in.sarif");
		fs::write(&sarif, SARIF).expect("sarif");

		let mut out = Vec::new();
		// Relative operands resolve against the effective cwd, not the process
		// working directory.
		run_with_state_root(
			&request("import-sarif", Some("in.sarif"), true),
			&repo,
			Some(&state),
			&mut out,
		)
		.expect("import-sarif");
		let imported = String::from_utf8(out).expect("utf8");
		assert!(imported.contains("\"id\": \"secscan_test01\""), "{imported}");
		assert!(imported.contains("\"findingIds\""), "{imported}");

		let mut out = Vec::new();
		run_with_state_root(&request("list", None, true), &repo, Some(&state), &mut out)
			.expect("list");
		let listed = String::from_utf8(out).expect("utf8");
		assert!(listed.contains("secscan_test01"), "{listed}");
		assert!(listed.contains("\"findingCount\": 1"), "{listed}");

		let mut out = Vec::new();
		run_with_state_root(
			&request("show", Some("secscan_test01"), true),
			&repo,
			Some(&state),
			&mut out,
		)
		.expect("show");
		let shown = String::from_utf8(out).expect("utf8");
		assert!(shown.contains("rust.unsafe"), "{shown}");
		assert!(shown.contains("\"findings\": ["), "{shown}");

		let mut out = Vec::new();
		run_with_state_root(
			&request("show", Some("secscan_missing"), false),
			&repo,
			Some(&state),
			&mut out,
		)
		.expect_err("missing scan");
		let mut out = Vec::new();
		run_with_state_root(&request("list", Some("extra"), false), &repo, Some(&state), &mut out)
			.expect_err("list rejects an operand");

		fs::remove_dir_all(&root).ok();
	}

	#[test]
	fn show_text_escapes_producer_controlled_rule_ids() {
		const HOSTILE: &str = r#"{
			"version": "2.1.0",
			"runs": [{
				"tool": {"driver": {"name": "demo"}},
				"results": [{
					"ruleId": "evil\u001b[2Jrule",
					"message": {"text": "clears the screen"},
					"locations": [{"physicalLocation": {
						"artifactLocation": {"uri": "src/a.rs"},
						"region": {"startLine": 1}
					}}]
				}]
			}]
		}"#;
		let root = temp_dir("omp-security-cmd-escape");
		let repo = root.join("repo");
		let state = root.join("state");
		fs::create_dir_all(repo.join("src")).expect("repo");
		fs::write(repo.join("in.sarif"), HOSTILE).expect("sarif");
		let mut out = Vec::new();
		run_with_state_root(
			&Request {
				action:  "import-sarif".to_string(),
				target:  Some(PathBuf::from("in.sarif")),
				repo:    None,
				scan_id: Some("secscan_esc01".to_string()),
				json:    false,
			},
			&repo,
			Some(&state),
			&mut out,
		)
		.expect("import");
		let mut out = Vec::new();
		run_with_state_root(
			&request("show", Some("secscan_esc01"), false),
			&repo,
			Some(&state),
			&mut out,
		)
		.expect("show");
		let text = String::from_utf8(out).expect("utf8");
		assert!(!text.contains('\u{1b}'), "raw ESC leaked: {text:?}");
		assert!(text.contains("\\u{1b}"), "{text:?}");
		assert!(text.contains("evil"), "{text}");
		fs::remove_dir_all(&root).ok();
	}
}
