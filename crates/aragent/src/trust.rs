//! Persistent workspace trust (D20).
//!
//! A workspace is an arbitrary checkout; its `.claude`/`.codex` hook files and
//! `.omp` MCP files can register subprocesses or endpoints. The runtime only
//! consumes those project surfaces after the operator runs `arag trust grant`
//! in the directory, and only while the recorded hook/MCP fingerprint still
//! matches. Trust never weakens policy: approval mode, allowlists, `bash`
//! allow rules and `searxng.*` stay stripped from project layers regardless of
//! trust (see [`crate::config::Config::apply_project_layers`]).
//!
//! State lives in `<agent_dir>/trust.json` (schema v1): canonical workspace
//! root -> `{ granted_at_ms, config_hashes }`. Writes are atomic and private
//! (directory `0700`, file `0600` on Unix).

use std::{
	collections::BTreeMap,
	fmt::Write as _,
	fs::{self, OpenOptions},
	io::Write,
	path::{Path, PathBuf},
	time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::{Error, Result, config::absolute_canonical, yaml};

/// Trust store schema version. Unknown versions fail closed (empty store).
const SCHEMA_VERSION: u32 = 1;

/// File name inside the agent directory.
const TRUST_FILE_NAME: &str = "trust.json";

/// Project files that can register subprocesses, launch commands, or redirect
/// a transport. Changes to any of these invalidate a grant.
///
/// `ssh.json` entries choose an SSH destination/identity for `ssh://` reads and
/// LSP files launch arbitrary server commands, so both are workspace-controlled
/// execution surfaces just like the hook/MCP files above.
const TRACKED_SURFACES: [&str; 40] = [
	".claude/settings.json",
	".claude/settings.local.json",
	".codex/hooks.json",
	".omp/mcp.json",
	".omp/.mcp.json",
	"mcp.json",
	".mcp.json",
	".omp/ssh.json",
	"ssh.json",
	".ssh.json",
	"lsp.json",
	".lsp.json",
	"lsp.yaml",
	".lsp.yaml",
	"lsp.yml",
	".lsp.yml",
	".omp/lsp.json",
	".omp/.lsp.json",
	".omp/lsp.yaml",
	".omp/.lsp.yaml",
	".omp/lsp.yml",
	".omp/.lsp.yml",
	".claude/lsp.json",
	".claude/.lsp.json",
	".claude/lsp.yaml",
	".claude/.lsp.yaml",
	".claude/lsp.yml",
	".claude/.lsp.yml",
	".codex/lsp.json",
	".codex/.lsp.json",
	".codex/lsp.yaml",
	".codex/.lsp.yaml",
	".codex/lsp.yml",
	".codex/.lsp.yml",
	".gemini/lsp.json",
	".gemini/.lsp.json",
	".gemini/lsp.yaml",
	".gemini/.lsp.yaml",
	".gemini/lsp.yml",
	".gemini/.lsp.yml",
];

/// Project files whose `hooks` block is fingerprinted on its own; unrelated
/// keys (for example `defaultModel`) must not invalidate a grant.
const HOOK_BLOCK_FILES: [&str; 2] = [".omp/settings.json", ".omp/config.yml"];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrustRecord {
	granted_at_ms:  u64,
	config_hashes:  BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrustFile {
	version:    u32,
	#[serde(default)]
	workspaces: BTreeMap<String, TrustRecord>,
}

impl Default for TrustFile {
	fn default() -> Self {
		Self { version: SCHEMA_VERSION, workspaces: BTreeMap::new() }
	}
}

/// Result of [`TrustStore::grant`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustGrantReport {
	pub path:           PathBuf,
	pub granted_at_ms:  u64,
	pub config_hashes:  BTreeMap<String, String>,
}

/// One entry for [`TrustStore::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustEntryView {
	pub path:          String,
	pub granted_at_ms: u64,
	pub config_files:  usize,
}

/// Verdict for one workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustStatus {
	/// No grant recorded for the canonical path.
	Untrusted,
	/// Grant recorded and the hook/MCP fingerprint still matches.
	Trusted { granted_at_ms: u64 },
	/// A grant exists but project hook/MCP configuration drifted; fail closed.
	Invalidated { reason: String },
}

impl TrustStatus {
	#[must_use]
	pub fn is_trusted(&self) -> bool {
		matches!(self, Self::Trusted { .. })
	}
}

/// File-backed trust store.
#[derive(Debug, Clone)]
pub struct TrustStore {
	path: PathBuf,
	file: TrustFile,
}

impl TrustStore {
	/// Load `<agent_dir>/trust.json` (missing file means no grants).
	///
	/// For the active default agent dir, a missing current file falls back to
	/// the legacy `~/.omp/agent/trust.json` read-only (no explicit override).
	/// The in-memory grants then persist to the current path on [`Self::save`].
	#[must_use]
	pub fn load_from_agent_dir(agent_dir: &Path) -> Self {
		let path = agent_dir.join(TRUST_FILE_NAME);
		if !path.exists() && agent_dir == crate::dirs::agent_dir().as_path()
			&& let Some(legacy) = crate::dirs::legacy_agent_dir().map(|dir| dir.join(TRUST_FILE_NAME))
			&& legacy.is_file()
		{
			return Self::load_at(&path, &legacy);
		}
		Self::load(&path)
	}

	/// Load the active profile's trust store.
	#[must_use]
	pub fn load_default() -> Self {
		Self::load_from_agent_dir(&crate::dirs::agent_dir())
	}

	/// Load an explicit path. Unreadable or unsupported files fail closed by
	/// starting from an empty store (a corrupt store never grants trust).
	#[must_use]
	pub fn load(path: &Path) -> Self {
		Self::load_at(path, path)
	}

	/// Read `read_path` but bind writes to `write_path` (legacy read fallback).
	fn load_at(write_path: &Path, read_path: &Path) -> Self {
		let file = match fs::read_to_string(read_path) {
			Ok(text) => match serde_json::from_str::<TrustFile>(&text) {
				Ok(file) if file.version == SCHEMA_VERSION => file,
				Ok(file) => {
					eprintln!(
						"warning: ignoring trust store {}: unsupported schema version {}",
						read_path.display(),
						file.version
					);
					TrustFile::default()
				},
				Err(error) => {
					eprintln!(
						"warning: ignoring unreadable trust store {}: {error}",
						read_path.display()
					);
					TrustFile::default()
				},
			},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => TrustFile::default(),
			Err(error) => {
				eprintln!("warning: cannot read trust store {}: {error}", read_path.display());
				TrustFile::default()
			},
		};
		Self { path: write_path.to_path_buf(), file }
	}

	#[must_use]
	pub fn path(&self) -> &Path {
		&self.path
	}

	/// Trust the canonical `dir` and fingerprint its project hook/MCP surface.
	///
	/// # Errors
	///
	/// Fails when the directory does not exist, cannot be canonicalized, a
	/// tracked file cannot be read, or the store cannot be written.
	pub fn grant(&mut self, dir: &Path) -> Result<TrustGrantReport> {
		let canonical = fs::canonicalize(dir).map_err(|error| {
			Error::with_source(
				format!("cannot trust {}: directory does not exist", dir.display()),
				error,
			)
		})?;
		if !canonical.is_dir() {
			return Err(Error::new(format!("cannot trust {}: not a directory", dir.display())));
		}
		let config_hashes = project_config_fingerprint(&canonical)?;
		let granted_at_ms = now_ms();
		self.file.workspaces.insert(
			path_key(&canonical),
			TrustRecord { granted_at_ms, config_hashes: config_hashes.clone() },
		);
		self.save()?;
		Ok(TrustGrantReport { path: canonical, granted_at_ms, config_hashes })
	}

	/// Remove the grant for `dir`. Removed directories can still be revoked:
	/// the key falls back to the lexically absolute path.
	///
	/// # Errors
	///
	/// Fails when the updated store cannot be written.
	pub fn revoke(&mut self, dir: &Path) -> Result<bool> {
		let key = path_key(&absolute_canonical(dir));
		if self.file.workspaces.remove(&key).is_none() {
			return Ok(false);
		}
		self.save()?;
		Ok(true)
	}

	/// Verify a workspace, returning the invalidation reason when the project
	/// hook/MCP configuration changed since the grant.
	#[must_use]
	pub fn verify_workspace(&self, dir: &Path) -> TrustStatus {
		let canonical = absolute_canonical(dir);
		let Some(record) = self.file.workspaces.get(&path_key(&canonical)) else {
			return TrustStatus::Untrusted;
		};
		match project_config_fingerprint(&canonical) {
			Ok(current) if current == record.config_hashes => {
				TrustStatus::Trusted { granted_at_ms: record.granted_at_ms }
			},
			Ok(current) => TrustStatus::Invalidated {
				reason: describe_drift(&record.config_hashes, &current),
			},
			Err(error) => TrustStatus::Invalidated {
				reason: format!("cannot verify project hook/MCP configuration: {error}"),
			},
		}
	}

	/// Same verdict as [`Self::verify_workspace`], named for callers that only
	/// need the store view.
	#[must_use]
	pub fn status(&self, dir: &Path) -> TrustStatus {
		self.verify_workspace(dir)
	}

	/// Boolean trust decision. An invalidated grant warns once per call and
	/// fails closed.
	#[must_use]
	pub fn is_trusted(&self, dir: &Path) -> bool {
		match self.verify_workspace(dir) {
			TrustStatus::Trusted { .. } => true,
			TrustStatus::Untrusted => false,
			TrustStatus::Invalidated { reason } => {
				eprintln!(
					"warning: workspace trust invalidated: {reason}; run `arag trust grant` to review and \
					 re-trust"
				);
				false
			},
		}
	}

	#[must_use]
	pub fn list(&self) -> Vec<TrustEntryView> {
		self
			.file
			.workspaces
			.iter()
			.map(|(path, record)| TrustEntryView {
				path:          path.clone(),
				granted_at_ms: record.granted_at_ms,
				config_files:  record.config_hashes.len(),
			})
			.collect()
	}

	/// Atomically replace `trust.json` (`temp` + `rename`, `0600` on Unix).
	///
	/// # Errors
	///
	/// Fails when the parent directory or the file cannot be written.
	pub fn save(&self) -> Result<()> {
		let parent = match self.path.parent() {
			Some(parent) if !parent.as_os_str().is_empty() => parent,
			_ => Path::new("."),
		};
		ensure_private_dir(parent)?;
		let payload = serde_json::to_vec_pretty(&self.file)?;
		let temp = parent.join(format!(
			".{TRUST_FILE_NAME}.{}-{}.tmp",
			std::process::id(),
			now_ms()
		));
		let written = (|| -> Result<()> {
			let mut options = OpenOptions::new();
			options.write(true).create_new(true);
			#[cfg(unix)]
			{
				use std::os::unix::fs::OpenOptionsExt;
				options.mode(0o600);
			}
			let mut file = options
				.open(&temp)
				.map_err(|error| Error::with_source(format!("cannot create {}", temp.display()), error))?;
			file.write_all(&payload)?;
			file.sync_all()?;
			Ok(())
		})();
		if let Err(error) = written {
			let _ = fs::remove_file(&temp);
			return Err(error);
		}
		fs::rename(&temp, &self.path).map_err(|error| {
			let _ = fs::remove_file(&temp);
			Error::with_source(format!("cannot replace {}", self.path.display()), error)
		})?;
		Ok(())
	}
}

/// One-call CLI handler for `arag trust <action> [DIR]`; `main.rs` only needs
/// to forward the parsed flags.
///
/// # Errors
///
/// Fails on an unknown action or any store operation error.
pub fn run_trust_command(
	action: Option<&str>,
	target: Option<&Path>,
	json: bool,
	cwd: &Path,
	agent_dir: &Path,
	out: &mut dyn Write,
) -> Result<()> {
	let action = action.unwrap_or("status");
	let target = target.unwrap_or(cwd);
	let mut store = TrustStore::load_from_agent_dir(agent_dir);
	match action {
		"grant" => {
			let report = store.grant(target)?;
			if json {
				writeln!(
					out,
					"{}",
					serde_json::to_string_pretty(&serde_json::json!({
						"path": report.path.display().to_string(),
						"grantedAtMs": report.granted_at_ms,
						"configFiles": report.config_hashes.len(),
					}))?
				)?;
			} else {
				writeln!(
					out,
					"trusted {} ({} hook/MCP surface(s) fingerprinted)",
					report.path.display(),
					report.config_hashes.len()
				)?;
			}
		},
		"revoke" => {
			let revoked = store.revoke(target)?;
			if json {
				writeln!(
					out,
					"{}",
					serde_json::to_string_pretty(&serde_json::json!({
						"path": absolute_canonical(target).display().to_string(),
						"revoked": revoked,
					}))?
				)?;
			} else if revoked {
				writeln!(out, "revoked {}", absolute_canonical(target).display())?;
			} else {
				writeln!(out, "not trusted: {}", absolute_canonical(target).display())?;
			}
		},
		"list" => {
			let entries = store.list();
			if json {
				let workspaces: Vec<JsonValue> = entries
					.iter()
					.map(|entry| {
						serde_json::json!({
							"path": entry.path,
							"grantedAtMs": entry.granted_at_ms,
							"configFiles": entry.config_files,
						})
					})
					.collect();
				writeln!(
					out,
					"{}",
					serde_json::to_string_pretty(&serde_json::json!({ "workspaces": workspaces }))?
				)?;
			} else if entries.is_empty() {
				writeln!(out, "no trusted workspaces")?;
			} else {
				for entry in entries {
					writeln!(
						out,
						"{} (granted at {} ms, {} hook/MCP surface(s))",
						entry.path, entry.granted_at_ms, entry.config_files
					)?;
				}
			}
		},
		"status" => {
			let status = store.status(target);
			if json {
				let (state, reason, granted_at_ms) = match &status {
					TrustStatus::Untrusted => ("untrusted", None, None),
					TrustStatus::Trusted { granted_at_ms } => {
						("trusted", None, Some(*granted_at_ms))
					},
					TrustStatus::Invalidated { reason } => {
						("invalidated", Some(reason.clone()), None)
					},
				};
				writeln!(
					out,
					"{}",
					serde_json::to_string_pretty(&serde_json::json!({
						"path": absolute_canonical(target).display().to_string(),
						"trusted": status.is_trusted(),
						"status": state,
						"reason": reason,
						"grantedAtMs": granted_at_ms,
					}))?
				)?;
			} else {
				match status {
					TrustStatus::Trusted { granted_at_ms } => {
						writeln!(out, "trusted (granted at {granted_at_ms} ms)")?;
					},
					TrustStatus::Untrusted => writeln!(out, "untrusted")?,
					TrustStatus::Invalidated { reason } => writeln!(
						out,
						"invalidated: {reason}; run `arag trust grant` to review and re-trust"
					)?,
				}
			}
		},
		other => {
			return Err(Error::new(format!(
				"unknown trust action `{other}`; expected grant, revoke, list, or status"
			)));
		},
	}
	Ok(())
}

fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_millis() as u64)
}

fn path_key(path: &Path) -> String {
	path.to_string_lossy().into_owned()
}

fn ensure_private_dir(dir: &Path) -> Result<()> {
	#[cfg(unix)]
	{
		use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
		fs::DirBuilder::new()
			.recursive(true)
			.mode(0o700)
			.create(dir)
			.map_err(|error| Error::with_source(format!("cannot create {}", dir.display()), error))?;
		// Tighten an existing directory (for example one created with 0755).
		fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(|error| {
			Error::with_source(format!("cannot restrict permissions on {}", dir.display()), error)
		})
	}
	#[cfg(not(unix))]
	{
		fs::create_dir_all(dir)
			.map_err(|error| Error::with_source(format!("cannot create {}", dir.display()), error))
	}
}

/// Fingerprint every project hook/MCP surface that exists now. Verification
/// compares the whole map, so edits, additions and removals all invalidate.
fn project_config_fingerprint(root: &Path) -> Result<BTreeMap<String, String>> {
	let mut hashes = BTreeMap::new();
	for relative in TRACKED_SURFACES {
		let path = root.join(relative);
		if !path.is_file() {
			continue;
		}
		let bytes = fs::read(&path).map_err(|error| {
			Error::with_source(format!("cannot fingerprint {}", path.display()), error)
		})?;
		hashes.insert((*relative).to_string(), sha256_hex(&bytes));
	}
	for relative in HOOK_BLOCK_FILES {
		let path = root.join(relative);
		if !path.is_file() {
			continue;
		}
		let text = fs::read_to_string(&path).map_err(|error| {
			Error::with_source(format!("cannot fingerprint {}", path.display()), error)
		})?;
		match extract_hooks(relative, &text) {
			HooksBlock::Absent => {},
			HooksBlock::Parsed(hooks) => {
				let canonical = serde_json::to_string(&hooks)?;
				hashes.insert(format!("{relative}#hooks"), sha256_hex(canonical.as_bytes()));
			},
			HooksBlock::Unparseable => {
				hashes.insert(format!("{relative}#unparseable"), sha256_hex(text.as_bytes()));
			},
		}
	}
	Ok(hashes)
}

enum HooksBlock {
	Absent,
	Parsed(JsonValue),
	Unparseable,
}

fn extract_hooks(relative: &str, text: &str) -> HooksBlock {
	if relative.ends_with(".json") {
		match serde_json::from_str::<JsonValue>(text) {
			Ok(value) => value
				.get("hooks")
				.cloned()
				.map_or(HooksBlock::Absent, HooksBlock::Parsed),
			Err(_) => HooksBlock::Unparseable,
		}
	} else {
		match yaml::parse(text) {
			Ok(value) => value.get("hooks").map_or(HooksBlock::Absent, |hooks| {
				HooksBlock::Parsed(yaml_to_json(hooks))
			}),
			Err(_) => HooksBlock::Unparseable,
		}
	}
}

fn yaml_to_json(value: &yaml::Value) -> JsonValue {
	match value {
		yaml::Value::Null => JsonValue::Null,
		yaml::Value::Bool(value) => JsonValue::Bool(*value),
		yaml::Value::Int(value) => JsonValue::from(*value),
		yaml::Value::Float(value) => serde_json::Number::from_f64(*value)
			.map_or(JsonValue::Null, JsonValue::Number),
		yaml::Value::Str(value) => JsonValue::String(value.clone()),
		yaml::Value::List(items) => {
			JsonValue::Array(items.iter().map(yaml_to_json).collect())
		},
		yaml::Value::Map(pairs) => JsonValue::Object(
			pairs
				.iter()
				.map(|(key, value)| (key.clone(), yaml_to_json(value)))
				.collect(),
		),
	}
}

fn sha256_hex(bytes: &[u8]) -> String {
	let digest = Sha256::digest(bytes);
	let mut hex = String::with_capacity(digest.len() * 2);
	for byte in digest {
		// `write!` to a `String` cannot fail.
		let _ = write!(hex, "{byte:02x}");
	}
	hex
}

fn describe_drift(recorded: &BTreeMap<String, String>, current: &BTreeMap<String, String>) -> String {
	let mut added = Vec::new();
	let mut removed = Vec::new();
	let mut modified = Vec::new();
	for (key, hash) in current {
		match recorded.get(key) {
			None => added.push(key.clone()),
			Some(previous) if previous != hash => modified.push(key.clone()),
			Some(_) => {},
		}
	}
	for key in recorded.keys() {
		if !current.contains_key(key) {
			removed.push(key.clone());
		}
	}
	let mut parts = Vec::new();
	if !modified.is_empty() {
		parts.push(format!("modified {}", modified.join(", ")));
	}
	if !added.is_empty() {
		parts.push(format!("added {}", added.join(", ")));
	}
	if !removed.is_empty() {
		parts.push(format!("removed {}", removed.join(", ")));
	}
	if parts.is_empty() {
		parts.push("fingerprint mismatch".to_string());
	}
	format!("project hook/MCP/ssh/LSP configuration changed ({})", parts.join("; "))
}

/// Live trust verdict for `cwd` under the default agent directory.
///
/// Hosts pass this as the SSH trust policy so every resolution observes a
/// revoked/regranted grant, surfaces tampered after the grant, and delegated
/// child cwds that were never granted.
#[must_use]
pub fn workspace_is_trusted(cwd: &Path) -> bool {
	TrustStore::load_from_agent_dir(&crate::dirs::agent_dir())
		.status(cwd)
		.is_trusted()
}
