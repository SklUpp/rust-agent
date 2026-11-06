//! Skill discovery from `<dir>/skills/*/SKILL.md`.
//!
//! Anchors: `packages/coding-agent/src/extensibility/skills.ts:91-323`
//! (`loadSkills`, `isNameClaimedByAuthoredSkill`, `getActiveSkills`),
//! `packages/coding-agent/src/discovery/helpers.ts:369-441`
//! (`scanSkillsFromDir`, `compareSkillOrder`) and
//! `packages/coding-agent/src/discovery/builtin.ts:280-340` (native + managed
//! providers).
//!
//! Roots and priority (highest first, first-wins on a name collision):
//! 1. native 100 — every ancestor of `cwd` up to `$HOME`/repo root contributing
//!    `<ancestor>/.omp/skills`, then `<agent dir>/skills`;
//! 2. claude 80 — `<ancestor>/.claude/skills` (home skipped), then
//!    `$HOME/.claude/skills`;
//! 3. agents 70 — `<ancestor>/.agent[s]/skills` (home skipped), then
//!    `$HOME/.agent[s]/skills`;
//! 4. codex 70 — `<cwd>/.codex/skills`, then `$HOME/.codex/skills`;
//! 5. custom — `skills.customDirectories` (explicit user intent: they override
//!    a default-provider skill of the same name);
//! 6. managed (auto-learn) 5 — `<agent dir>/managed-skills`, discovered
//!    unconditionally but never allowed to shadow any authored skill.
//!
//! `skill.hide` / `disableModelInvocation` frontmatter keeps a skill out of the
//! system-prompt listing (`LoadResult::prompt_skills`) while leaving it
//! reachable through `skill://`, `/skill:<name>` and autoload.
//!
//! No snapshot is process-global: callers hold their own [`SkillSnapshot`]
//! (`Arc<RwLock<...>>`), so concurrent RPC/ACP sessions and child agents cannot
//! observe each other's loaded skills.

use std::{
	collections::{BTreeMap, HashSet},
	fs,
	path::{Path, PathBuf},
	sync::{Arc, RwLock},
};

use globset::{Glob, GlobMatcher, GlobSet, GlobSetBuilder};

use crate::{
	Error, Result, dirs,
	managed_skills::{
		MANAGED_SKILLS_PROVIDER_ID, is_valid_managed_skill_name, managed_skills_dir,
		sanitize_managed_description,
	},
	yaml::{self, Value},
};

/// One discovered skill.
///
/// Kept to the fields the runtime prompt/`RunRequest` already carries; richer
/// discovery metadata lives on [`SkillEntry`] so existing callers and their
/// literal constructions stay unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skill {
	pub name: String,
	pub description: String,
	pub path: PathBuf,
	pub body: String,
}

impl Skill {
	/// Directory containing `SKILL.md` (TS `baseDir`).
	#[must_use]
	pub fn base_dir(&self) -> PathBuf {
		self.path.parent().map_or_else(|| self.path.clone(), Path::to_path_buf)
	}
}

/// A discovered skill plus its provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillEntry {
	pub skill: Skill,
	/// `<provider>:<level>` (TS `Skill.source`).
	pub source: String,
	/// `hide` / `disableModelInvocation` frontmatter: prompt-invisible but
	/// reachable.
	pub hide: bool,
	/// Filesystem-resolved plugin root for Agent Plugin skills; a `skill://`
	/// read must canonically resolve inside it.
	pub contain_root: Option<PathBuf>,
}

impl SkillEntry {
	#[must_use]
	pub fn base_dir(&self) -> PathBuf {
		self.skill.base_dir()
	}

	/// Managed (auto-learn) skills always resolve below authored ones.
	#[must_use]
	pub fn is_managed(&self) -> bool {
		self.source.starts_with(&format!("{MANAGED_SKILLS_PROVIDER_ID}:"))
	}
}

/// Validation warning with the file it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillWarning {
	pub skill_path: PathBuf,
	pub message: String,
}

/// Discovery output: every reachable skill plus warnings.
#[derive(Debug, Clone, Default)]
pub struct LoadResult {
	pub entries: Vec<SkillEntry>,
	pub warnings: Vec<SkillWarning>,
}

impl LoadResult {
	/// Skills eligible for the system-prompt listing (hidden ones excluded).
	#[must_use]
	pub fn prompt_skills(&self) -> Vec<Skill> {
		self
			.entries
			.iter()
			.filter(|entry| !entry.hide)
			.map(|entry| entry.skill.clone())
			.collect()
	}

	/// Every loaded skill, including prompt-hidden ones.
	#[must_use]
	pub fn all_skills(&self) -> Vec<Skill> {
		self.entries.iter().map(|entry| entry.skill.clone()).collect()
	}

	#[must_use]
	pub fn find(&self, name: &str) -> Option<&SkillEntry> {
		self.entries.iter().find(|entry| entry.skill.name == name)
	}
}

/// Discovery settings, mapped from `skills.*` by the config layer.
#[derive(Debug, Clone)]
pub struct LoadOptions {
	pub cwd: PathBuf,
	/// Stop ancestor walks here (`skills.repoRoot` / git root). Defaults to the
	/// home directory, matching the TS `repoRoot ?? home` clamp.
	pub repo_root: Option<PathBuf>,
	/// Override for `$HOME` (tests, embedders). Defaults to `dirs::home_dir()`.
	pub home: Option<PathBuf>,
	/// Override for the agent dir. Defaults to `dirs::agent_dir()`.
	pub agent_dir: Option<PathBuf>,
	pub no_skills: bool,
	/// `--skills` glob patterns matched against name or path.
	pub patterns: Vec<String>,
	pub enabled: bool,
	pub enable_codex_user: bool,
	pub enable_claude_user: bool,
	pub enable_claude_project: bool,
	pub enable_pi_user: bool,
	pub enable_pi_project: bool,
	pub enable_agents_user: bool,
	pub enable_agents_project: bool,
	pub custom_directories: Vec<PathBuf>,
	pub ignored_skills: Vec<String>,
	pub include_skills: Vec<String>,
	/// `skills.enableSkillCommands`: `/skill:<name>` plus available-command
	/// listings; discovery itself is unaffected.
	pub enable_skill_commands: bool,
	/// `skill:<name>` entries disable that skill (TS `disabledExtensions`).
	pub disabled_extensions: Vec<String>,
}

impl LoadOptions {
	#[must_use]
	pub fn new(cwd: impl AsRef<Path>) -> Self {
		Self {
			cwd: cwd.as_ref().to_path_buf(),
			repo_root: None,
			home: None,
			agent_dir: None,
			no_skills: false,
			patterns: Vec::new(),
			enabled: true,
			enable_codex_user: true,
			enable_claude_user: true,
			enable_claude_project: true,
			enable_pi_user: true,
			enable_pi_project: true,
			enable_agents_user: true,
			enable_agents_project: true,
			custom_directories: Vec::new(),
			ignored_skills: Vec::new(),
			include_skills: Vec::new(),
			enable_skill_commands: true,
			disabled_extensions: Vec::new(),
		}
	}

	/// Map the `skills.*` group plus the CLI `--no-skills` / `--skills` inputs
	/// onto discovery settings. Tilde expansion and the home/agent overrides
	/// stay with [`load_with`], which every session owner calls.
	#[must_use]
	pub fn from_config(
		config: &crate::config::Config,
		cwd: impl AsRef<Path>,
		no_skills: bool,
		patterns: &[String],
	) -> Self {
		let skills = &config.skills;
		Self {
			cwd: cwd.as_ref().to_path_buf(),
			repo_root: None,
			home: None,
			agent_dir: None,
			no_skills,
			patterns: patterns.to_vec(),
			enabled: skills.enabled(),
			enable_codex_user: skills.enable_codex_user(),
			enable_claude_user: skills.enable_claude_user(),
			enable_claude_project: skills.enable_claude_project(),
			enable_pi_user: skills.enable_pi_user(),
			enable_pi_project: skills.enable_pi_project(),
			enable_agents_user: skills.enable_agents_user(),
			enable_agents_project: skills.enable_agents_project(),
			custom_directories: skills.custom_directories().to_vec(),
			ignored_skills: skills.ignored_skills().to_vec(),
			include_skills: skills.include_skills().to_vec(),
			enable_skill_commands: skills.enable_skill_commands(),
			disabled_extensions: config.disabled_extensions().to_vec(),
		}
	}
}

impl Default for LoadOptions {
	fn default() -> Self {
		Self::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
	}
}

/// Per-top-level-session skill snapshot.
///
/// Shared with the tools that mint managed skills and with the `skill://`
/// handler, so a mutation refreshes exactly the caller's view and never leaks
/// into another session.
#[derive(Debug, Clone, Default)]
pub struct SkillSnapshot {
	entries: Arc<RwLock<Vec<SkillEntry>>>,
}

impl SkillSnapshot {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	/// Replace the snapshot (called once per top-level session load).
	pub fn set(&self, entries: Vec<SkillEntry>) {
		let mut guard = self.entries.write().unwrap_or_else(std::sync::PoisonError::into_inner);
		*guard = entries;
	}

	#[must_use]
	pub fn entries(&self) -> Vec<SkillEntry> {
		self.entries.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
	}

	/// Skills eligible for the prompt listing.
	#[must_use]
	pub fn prompt_skills(&self) -> Vec<Skill> {
		self
			.entries
			.read()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.iter()
			.filter(|entry| !entry.hide)
			.map(|entry| entry.skill.clone())
			.collect()
	}

	#[must_use]
	pub fn find(&self, name: &str) -> Option<SkillEntry> {
		self
			.entries
			.read()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.iter()
			.find(|entry| entry.skill.name == name)
			.cloned()
	}

	/// Whether an authored (non-managed) skill already claims `name`.
	///
	/// Managed skills resolve dead-last, so minting one under a claimed name
	/// writes a file that never surfaces; `manage_skill`/`learn` refuse up
	/// front instead of reporting a false "Created".
	#[must_use]
	pub fn is_name_claimed_by_authored_skill(&self, name: &str) -> bool {
		self
			.entries
			.read()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.iter()
			.any(|entry| entry.skill.name == name && !entry.is_managed())
	}

	/// Reload the caller's view in place (after a managed mutation).
	pub fn refresh(&self, options: &LoadOptions) -> Result<LoadResult> {
		let result = load_with(options)?;
		self.set(result.entries.clone());
		Ok(result)
	}
}

/// Load skills for one session.
pub fn load_with(options: &LoadOptions) -> Result<LoadResult> {
	if options.no_skills || !options.enabled {
		return Ok(LoadResult::default());
	}
	let path_matcher = build_matcher(&options.patterns)?;
	let include = build_name_matchers(&options.include_skills)?;
	let ignore = build_name_matchers(&options.ignored_skills)?;
	let disabled: HashSet<String> = options
		.disabled_extensions
		.iter()
		.filter_map(|id| id.strip_prefix("skill:"))
		.map(str::to_string)
		.collect();
	let home = options.home.clone().or_else(dirs::home_dir);
	let agent_dir = options.agent_dir.clone().unwrap_or_else(dirs::agent_dir);

	let mut warnings: Vec<SkillWarning> = Vec::new();
	let mut authored: Vec<SkillEntry> = Vec::new();
	let mut enabled_authored_names: HashSet<String> = HashSet::new();

	for scan in provider_scans(options, home.as_deref(), &agent_dir) {
		let (items, scan_warnings) = scan_dir(&scan.dir, scan.require_description)?;
		warnings.extend(scan_warnings.into_iter().map(|message| SkillWarning {
			skill_path: scan.dir.clone(),
			message,
		}));
		for scanned in items {
			let name = scanned.skill.name.clone();
			if disabled.contains(&name) {
				continue;
			}
			if matches_name(&ignore, &name) || !matches_name_opt(&include, &name) {
				continue;
			}
			enabled_authored_names.insert(name);
			authored.push(SkillEntry {
				skill: scanned.skill,
				source: format!("{}:{}", scan.provider, scan.level),
				hide: scanned.hide,
				contain_root: None,
			});
		}
	}

	let mut map: BTreeMap<String, SkillEntry> = BTreeMap::new();
	let mut real_paths: HashSet<PathBuf> = HashSet::new();
	for entry in authored {
		let real = real_path_or(&entry.skill.path);
		if real_paths.contains(&real) {
			continue;
		}
		if let Some(existing) = map.get(&entry.skill.name) {
			warnings.push(collision_warning(existing, &entry));
			continue;
		}
		real_paths.insert(real);
		map.insert(entry.skill.name.clone(), entry);
	}

	for directory in &options.custom_directories {
		let expanded = expand_tilde(directory, home.as_deref());
		let (items, scan_warnings) = scan_dir(&expanded, true)?;
		warnings.extend(scan_warnings.into_iter().map(|message| SkillWarning {
			skill_path: expanded.clone(),
			message,
		}));
		for scanned in items {
			let name = scanned.skill.name.clone();
			if disabled.contains(&name) {
				continue;
			}
			if matches_name(&ignore, &name) || !matches_name_opt(&include, &name) {
				continue;
			}
			let entry = SkillEntry {
				hide: scanned.hide,
				skill: scanned.skill,
				source: "custom:user".to_string(),
				contain_root: None,
			};
			let real = real_path_or(&entry.skill.path);
			if real_paths.contains(&real) {
				continue;
			}
			if let Some(existing) = map.get(&entry.skill.name) {
				if existing.source.starts_with("custom:") {
					warnings.push(collision_warning(existing, &entry));
					continue;
				}
				// A default-path provider yields to the explicitly configured
				// custom dir (user intent wins), per `skills.ts:270-286`.
				real_paths.insert(real);
				map.insert(entry.skill.name.clone(), entry);
				continue;
			}
			real_paths.insert(real);
			map.insert(entry.skill.name.clone(), entry);
		}
	}

	let (managed_items, managed_warnings) = scan_dir(&managed_skills_dir(&agent_dir), true)?;
	warnings.extend(managed_warnings.into_iter().map(|message| SkillWarning {
		skill_path: managed_skills_dir(&agent_dir),
		message,
	}));
	for scanned in managed_items {
		let name = scanned.skill.name.clone();
		// A hand-placed managed file could carry an unsafe frontmatter name; it
		// must never render into the prompt.
		if !is_valid_managed_skill_name(&name) {
			continue;
		}
		if disabled.contains(&name)
			|| matches_name(&ignore, &name)
			|| !matches_name_opt(&include, &name)
		{
			continue;
		}
		// Enabled authored skills always win, even when capability-level dedup
		// would have hidden them behind a disabled higher-priority provider.
		if enabled_authored_names.contains(&name) || map.contains_key(&name) {
			continue;
		}
		let real = real_path_or(&scanned.skill.path);
		if real_paths.contains(&real) {
			continue;
		}
		let description = sanitize_managed_description(&scanned.skill.description);
		let entry = SkillEntry {
			hide: scanned.hide,
			skill: Skill { description, ..scanned.skill },
			source: format!("{MANAGED_SKILLS_PROVIDER_ID}:user"),
			contain_root: None,
		};
		real_paths.insert(real);
		map.insert(entry.skill.name.clone(), entry);
	}

	let mut entries: Vec<SkillEntry> = map
		.into_values()
		.filter(|entry| {
			path_matcher.as_ref().is_none_or(|set| {
				set.is_match(&entry.skill.name) || set.is_match(&entry.skill.path)
			})
		})
		.collect();
	entries.sort_by(|left, right| {
		compare_skill_order(
			&left.skill.name,
			&left.skill.path,
			&right.skill.name,
			&right.skill.path,
		)
	});
	Ok(LoadResult { entries, warnings })
}

/// Session-scoped `autoloadSkills` resolver for subagent agents.
///
/// Uses the same snapshot as `skill://` and `/skill`, so hidden skills and
/// freshly refreshed entries resolve identically. Unknown names are `Ok(None)`
/// (TS filters them out); a read failure is returned as `Err` instead of being
/// mistaken for a missing skill.
pub struct SnapshotAutoloadResolver {
	snapshot: SkillSnapshot,
}

impl SnapshotAutoloadResolver {
	#[must_use]
	pub fn new(snapshot: SkillSnapshot) -> Self {
		Self { snapshot }
	}
}

impl pi_agent::tools::task::AutoloadSkillResolver for SnapshotAutoloadResolver {
	fn autoload_message(&self, name: &str) -> Result<Option<String>, String> {
		let Some(entry) = self.snapshot.find(name) else {
			return Ok(None);
		};
		let built = build_skill_prompt_message(&entry.skill, "", SkillInvocationKind::Autoload)
			.map_err(|error| format!("cannot autoload skill \"{name}\": {error}"))?;
		Ok(Some(built.message))
	}
}

/// Resolve a `/skill:<name>` invocation against one session's snapshot.
///
/// TS `tryRunRpcSkillCommand` / interactive input controller: parse the
/// invocation (leading or mid-prompt), look it up in the reachable set (hidden
/// skills included), re-read the file from disk and render the user-invocation
/// template. `Ok(None)` means the text is not a known skill command and must be
/// sent unchanged; the caller gates this on `skills.enableSkillCommands`.
pub fn expand_skill_invocation(snapshot: &SkillSnapshot, text: &str) -> Result<Option<String>> {
	let Some(invocation) = parse_skill_invocation(text) else {
		return Ok(None);
	};
	let Some(entry) = snapshot.find(&invocation.name) else {
		return Ok(None);
	};
	let built = build_skill_prompt_message(
		&entry.skill,
		&invocation.args,
		SkillInvocationKind::User,
	)?;
	Ok(Some(built.message))
}

struct ProviderScan {
	dir: PathBuf,
	provider: &'static str,
	level: &'static str,
	require_description: bool,
}

/// Provider roots in capability-priority order (highest first).
///
/// Within one provider the item order decides ties, mirroring the TS loaders:
/// native scans projects then user; claude and codex scan user then projects;
/// agents scans projects then user.
fn provider_scans(
	options: &LoadOptions,
	home: Option<&Path>,
	agent_dir: &Path,
) -> Vec<ProviderScan> {
	let mut scans = Vec::new();
	let stop = options.repo_root.as_deref().or(home);
	let project_ancestors = ancestors(&options.cwd, stop);

	if options.enable_pi_project {
		for dir in &project_ancestors {
			scans.push(scan(dirs::project_config_dir(dir).join("skills"), "native", "project", true));
		}
	}
	if options.enable_pi_user {
		scans.push(scan(agent_dir.join("skills"), "native", "user", true));
	}

	if let Some(home) = home {
		if options.enable_claude_user {
			scans.push(scan(home.join(".claude").join("skills"), "claude", "user", false));
		}
		if options.enable_claude_project {
			for dir in project_ancestors.iter().filter(|dir| dir.as_path() != home) {
				scans.push(scan(dir.join(".claude").join("skills"), "claude", "project", false));
			}
		}
		if options.enable_agents_project {
			for dir in project_ancestors.iter().filter(|dir| dir.as_path() != home) {
				for base in [".agent", ".agents"] {
					scans.push(scan(dir.join(base).join("skills"), "agents", "project", false));
				}
			}
		}
		if options.enable_agents_user {
			for base in [".agent", ".agents"] {
				scans.push(scan(home.join(base).join("skills"), "agents", "user", false));
			}
		}
		if options.enable_codex_user {
			scans.push(scan(home.join(".codex").join("skills"), "codex", "user", false));
		}
	}
	// Codex has no project toggle: its project dir follows the third-party
	// fallback gate (`skills.ts:157-183`), so it disappears only when every
	// third-party source toggle is off.
	let any_third_party = options.enable_codex_user
		|| options.enable_claude_user
		|| options.enable_claude_project
		|| options.enable_pi_user
		|| options.enable_pi_project;
	if any_third_party {
		scans.push(scan(options.cwd.join(".codex").join("skills"), "codex", "project", false));
	}
	scans
}

fn scan(
	dir: PathBuf,
	provider: &'static str,
	level: &'static str,
	require_description: bool,
) -> ProviderScan {
	ProviderScan { dir, provider, level, require_description }
}

/// `cwd` plus every ancestor, closest first, stopping at `stop` (inclusive).
fn ancestors(cwd: &Path, stop: Option<&Path>) -> Vec<PathBuf> {
	let mut out = Vec::new();
	let mut current = Some(cwd.to_path_buf());
	while let Some(dir) = current {
		out.push(dir.clone());
		if stop.is_some_and(|stop| dir.as_path() == stop) {
			break;
		}
		current = dir.parent().map(Path::to_path_buf);
	}
	out
}

fn real_path_or(path: &Path) -> PathBuf {
	fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn collision_warning(existing: &SkillEntry, skipped: &SkillEntry) -> SkillWarning {
	SkillWarning {
		skill_path: skipped.skill.path.clone(),
		message: format!(
			"name collision: \"{}\" already loaded from {}, skipping this one",
			skipped.skill.name,
			existing.skill.path.display()
		),
	}
}

fn expand_tilde(path: &Path, home: Option<&Path>) -> PathBuf {
	let Some(home) = home else {
		return path.to_path_buf();
	};
	let Ok(raw) = path.strip_prefix("~") else {
		return path.to_path_buf();
	};
	home.join(raw)
}

fn build_matcher(patterns: &[String]) -> Result<Option<GlobSet>> {
	let patterns: Vec<&String> = patterns
		.iter()
		.filter(|pattern| !pattern.trim().is_empty())
		.collect();
	if patterns.is_empty() {
		return Ok(None);
	}
	let mut builder = GlobSetBuilder::new();
	for pattern in patterns {
		let glob = Glob::new(pattern.trim())
			.map_err(|err| Error::new(format!("invalid --skills pattern `{pattern}`: {err}")))?;
		builder.add(glob);
	}
	builder
		.build()
		.map(Some)
		.map_err(|err| Error::new(format!("invalid --skills patterns: {err}")))
}

fn build_name_matchers(patterns: &[String]) -> Result<Vec<GlobMatcher>> {
	let mut matchers = Vec::new();
	for pattern in patterns {
		let pattern = pattern.trim();
		if pattern.is_empty() {
			continue;
		}
		let glob = Glob::new(pattern)
			.map_err(|err| Error::new(format!("invalid skill pattern `{pattern}`: {err}")))?;
		matchers.push(glob.compile_matcher());
	}
	Ok(matchers)
}

fn matches_name(matchers: &[GlobMatcher], name: &str) -> bool {
	matchers.iter().any(|matcher| matcher.is_match(name))
}

/// Empty include list means "everything".
fn matches_name_opt(matchers: &[GlobMatcher], name: &str) -> bool {
	matchers.is_empty() || matches_name(matchers, name)
}

/// Stable ordering used for skill lists in prompts: name (case-insensitive),
/// then exact name, then path.
#[must_use]
pub fn compare_skill_order(
	left_name: &str,
	left_path: &Path,
	right_name: &str,
	right_path: &Path,
) -> std::cmp::Ordering {
	left_name
		.to_lowercase()
		.cmp(&right_name.to_lowercase())
		.then_with(|| left_name.cmp(right_name))
		.then_with(|| left_path.cmp(right_path))
}

/// One scanned `SKILL.md` plus the prompt-visibility flag from its frontmatter.
struct ScannedSkill {
	skill: Skill,
	hide:  bool,
}

fn scan_dir(dir: &Path, require_description: bool) -> Result<(Vec<ScannedSkill>, Vec<String>)> {
	let mut warnings = Vec::new();
	let entries = match fs::read_dir(dir) {
		Ok(entries) => entries,
		Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), warnings)),
		Err(err) => {
			warnings.push(format!("Failed to read skills directory: {} ({err})", dir.display()));
			return Ok((Vec::new(), warnings));
		},
	};
	let mut skills = Vec::new();
	for entry in entries {
		let entry = entry?;
		let name = entry.file_name();
		if name.to_string_lossy().starts_with('.') {
			continue;
		}
		let path = entry.path();
		if !path.is_dir() {
			continue;
		}
		let skill_path = path.join("SKILL.md");
		if !skill_path.is_file() {
			continue;
		}
		let content = match fs::read_to_string(&skill_path) {
			Ok(content) => content,
			Err(_) => {
				warnings.push(format!("Failed to read skill file: {}", skill_path.display()));
				continue;
			},
		};
		let (skill, warning) = parse_skill(&skill_path, &content, require_description);
		if let Some(warning) = warning {
			warnings.push(warning);
		}
		if let Some(skill) = skill {
			skills.push(skill);
		}
	}
	skills.sort_by(|left, right| {
		compare_skill_order(
			&left.skill.name,
			&left.skill.path,
			&right.skill.name,
			&right.skill.path,
		)
	});
	Ok((skills, warnings))
}

fn parse_skill(
	path: &Path,
	content: &str,
	require_description: bool,
) -> (Option<ScannedSkill>, Option<String>) {
	// TS `parseFrontmatter` never rejects a missing or unclosed `---` block: the
	// metadata is empty and the whole file is the body, so a frontmatter-less
	// third-party `SKILL.md` still loads under its directory name.
	let (frontmatter, body) = split_frontmatter(content).unwrap_or(("", content));
	let mut warning = None;
	let meta = if frontmatter.trim().is_empty() {
		Value::Null
	} else {
		match yaml::parse(frontmatter) {
			Ok(meta) => meta,
			Err(err) => {
				// TS warns and keeps going with a `key: value` recovery pass.
				warning = Some(format!(
					"Failed to parse YAML frontmatter in {} ({err}); using the key: value fallback",
					path.display()
				));
				parse_metadata_fallback(frontmatter)
			},
		}
	};
	if meta.get("enabled").and_then(Value::as_bool) == Some(false) {
		return (None, warning);
	}
	let directory_name = path
		.parent()
		.and_then(Path::file_name)
		.map_or_else(String::new, |name| name.to_string_lossy().into_owned());
	let name = meta
		.get("name")
		.and_then(Value::to_text)
		.map(|name| name.trim().to_string())
		.filter(|name| !name.is_empty())
		.unwrap_or(directory_name);
	let description = meta
		.get("description")
		.and_then(Value::to_text)
		.unwrap_or_default();
	if require_description && description.trim().is_empty() {
		return (None, warning);
	}
	let hide = meta.get("hide").and_then(Value::as_bool) == Some(true)
		|| meta.get("disableModelInvocation").and_then(Value::as_bool) == Some(true);
	let scanned = ScannedSkill {
		skill: Skill { name, description, path: path.to_path_buf(), body: body.to_string() },
		hide,
	};
	(Some(scanned), warning)
}

/// Recovery pass for metadata the YAML parser rejects, mirroring
/// `parseFrontmatter`'s simple `key: value` fallback.
///
/// Each matched line is reparsed on its own so one malformed sibling cannot
/// swallow the rest. A value that is not a scalar (null, mapping) or does not
/// parse at all stays the raw trimmed text, exactly like the TS fallback.
fn parse_metadata_fallback(metadata: &str) -> Value {
	let mut pairs: Vec<(String, Value)> = Vec::new();
	for line in metadata.lines() {
		let Some((key, rest)) = line.split_once(':') else {
			continue;
		};
		let valid_key = !key.is_empty()
			&& key.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
		if !valid_key {
			continue;
		}
		let raw = rest.trim();
		let value = if raw.is_empty() {
			Value::Str(String::new())
		} else {
			match yaml::parse(raw) {
				Ok(Value::Null | Value::Map(_)) | Err(_) => Value::Str(raw.to_string()),
				Ok(value) => value,
			}
		};
		pairs.push((key.to_string(), value));
	}
	Value::Map(pairs)
}

/// Split a `---` frontmatter block from the markdown body.
fn split_frontmatter(content: &str) -> Option<(&str, &str)> {
	let rest = content
		.strip_prefix("---\n")
		.or_else(|| content.strip_prefix("---\r\n"))?;
	let mut offset = 0;
	for line in rest.split_inclusive('\n') {
		if line.trim_end() == "---" {
			return Some((&rest[..offset], &rest[offset + line.len()..]));
		}
		offset += line.len();
	}
	None
}

/// `skill:<name>` slash-command name.
#[must_use]
pub fn slash_command_name(name: &str) -> String {
	format!("skill:{name}")
}

/// Parsed `/skill:<name>` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedInvocation {
	pub name: String,
	/// Everything outside the `/skill:<name>` token.
	pub args: String,
}

/// Detect a `/skill:<name>` invocation in a user draft.
///
/// Leading form (`/skill:foo bar`) yields `name=foo`, `args=bar`; mid-prompt
/// form (`fix it /skill:foo focus`) collapses the surrounding prose into args.
/// A draft that starts with another slash command or a local-execution sigil
/// (`!`/`!!`, `$`/`$$`) is never treated as a mid-prompt invocation.
#[must_use]
pub fn parse_skill_invocation(text: &str) -> Option<ParsedInvocation> {
	let trimmed_start = text.trim_start();
	if let Some(rest) = trimmed_start.strip_prefix("/skill:") {
		let (name, args) = match rest.find(' ') {
			Some(index) => (&rest[..index], rest[index + 1..].trim()),
			None => (rest, ""),
		};
		if name.is_empty() {
			return None;
		}
		return Some(ParsedInvocation { name: name.to_string(), args: args.to_string() });
	}
	if trimmed_start.starts_with('/') || starts_with_local_execution_prefix(trimmed_start) {
		return None;
	}
	mid_prompt_invocation(text)
}

fn starts_with_local_execution_prefix(trimmed_start: &str) -> bool {
	if trimmed_start.starts_with('!') {
		return true;
	}
	if !trimmed_start.starts_with('$') {
		return false;
	}
	if trimmed_start.starts_with("${") {
		return false;
	}
	let sigil_length = if trimmed_start.starts_with("$$") { 2 } else { 1 };
	match trimmed_start[sigil_length..].chars().next() {
		None => true,
		Some(next) => next.is_whitespace(),
	}
}

fn mid_prompt_invocation(text: &str) -> Option<ParsedInvocation> {
	const MARKER: &str = "/skill:";
	let mut search_from = 0;
	while let Some(relative) = text[search_from..].find(MARKER) {
		let token_start = search_from + relative;
		let preceded = token_start == 0
			|| text[..token_start]
				.chars()
				.next_back()
				.is_some_and(char::is_whitespace);
		let name_start = token_start + MARKER.len();
		let mut name_end = name_start;
		while let Some(ch) = text[name_end..].chars().next() {
			if ch.is_whitespace() || ch == '/' {
				break;
			}
			name_end += ch.len_utf8();
		}
		let name = &text[name_start..name_end];
		let followed = text[name_end..].chars().next().is_none_or(char::is_whitespace);
		if preceded && !name.is_empty() && followed {
			let before = text[..token_start].trim_end();
			let after = text[name_end..].trim_start();
			let args = [before, after]
				.into_iter()
				.filter(|part| !part.is_empty())
				.collect::<Vec<_>>()
				.join(" ");
			return Some(ParsedInvocation { name: name.to_string(), args: args.trim().to_string() });
		}
		search_from = name_end.max(token_start + MARKER.len());
	}
	None
}

/// How a skill body is injected into the conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillInvocationKind {
	/// User invoked `/skill:<name>`: announce it and expose the skill dir.
	User,
	/// Hidden autoload from task/agent metadata: provenance only.
	Autoload,
}

/// Rendered `/skill` prompt message plus the details the session persists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillPromptMessage {
	pub message: String,
	pub name: String,
	pub path: PathBuf,
	pub args: Option<String>,
	pub line_count: usize,
}

/// Render the exact TS templates
/// (`packages/coding-agent/src/prompts/skills/user-invocation.md` and
/// `autoload.md`).
///
/// The body is re-read from disk so a skill edited after discovery is injected
/// in its current form; frontmatter is stripped.
pub fn build_skill_prompt_message(
	skill: &Skill,
	args: &str,
	kind: SkillInvocationKind,
) -> Result<SkillPromptMessage> {
	let content = fs::read_to_string(&skill.path)
		.map_err(|err| Error::with_source(format!("cannot read {}", skill.path.display()), err))?;
	let body = split_frontmatter(&content).map_or(content.as_str(), |(_, body)| body).trim();
	let trimmed_args = args.trim();
	let message = match kind {
		SkillInvocationKind::User => {
			let mut message = format!(
				"[IMPORTANT: User invoked the \"{}\" skill; follow its instructions. Full skill below.]\n\n\
				 {body}\n\n---\n\n[Skill directory: {}]\n\
				 Resolve relative paths in this skill (e.g. `scripts/foo.js`, `templates/config.yaml`) \
				 against this absolute directory; read referenced assets and templates; run scripts \
				 with the terminal tool when skill instructions call for it.",
				skill.name,
				skill.base_dir().display()
			);
			if !trimmed_args.is_empty() {
				message.push_str(&format!("\nUser: {trimmed_args}"));
			}
			message.trim().to_string()
		},
		SkillInvocationKind::Autoload => {
			let mut message = format!("{body}\n\n---\n\nSkill: {}", skill.path.display());
			if !trimmed_args.is_empty() {
				message.push_str(&format!("\nUser: {trimmed_args}"));
			}
			message.trim().to_string()
		},
	};
	let line_count = if body.is_empty() { 0 } else { body.lines().count() };
	Ok(SkillPromptMessage {
		message,
		name: skill.name.clone(),
		path: skill.path.clone(),
		args: (!trimmed_args.is_empty()).then(|| trimmed_args.to_string()),
		line_count,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::io::Write as _;
	use std::sync::atomic::{AtomicU32, Ordering};

	static COUNTER: AtomicU32 = AtomicU32::new(0);

	fn temp_dir() -> PathBuf {
		let unique = format!(
			"omp-cli-skills-{}-{}",
			std::process::id(),
			COUNTER.fetch_add(1, Ordering::Relaxed)
		);
		let dir = std::env::temp_dir().join(unique);
		fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	fn write(path: &Path, text: &str) {
		fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
		let mut file = fs::File::create(path).expect("create");
		file.write_all(text.as_bytes()).expect("write");
	}

	struct EnvGuard {
		agent: Option<std::ffi::OsString>,
		config: Option<std::ffi::OsString>,
		home: Option<std::ffi::OsString>,
	}

	impl EnvGuard {
		fn isolate(root: &Path) -> Self {
			let guard = Self {
				agent: std::env::var_os("PI_CODING_AGENT_DIR"),
				config: std::env::var_os("PI_CONFIG_DIR"),
				home: std::env::var_os("HOME"),
			};
			// SAFETY: callers hold the crate-wide env lock.
			unsafe {
				std::env::set_var("PI_CONFIG_DIR", root);
				std::env::set_var("PI_CODING_AGENT_DIR", root.join("agent"));
				std::env::set_var("HOME", root);
			}
			guard
		}
	}

	impl Drop for EnvGuard {
		fn drop(&mut self) {
			// SAFETY: restoring what this guard replaced.
			unsafe {
				for (key, value) in [
					("PI_CODING_AGENT_DIR", &self.agent),
					("PI_CONFIG_DIR", &self.config),
					("HOME", &self.home),
				] {
					match value {
						Some(value) => std::env::set_var(key, value),
						None => std::env::remove_var(key),
					}
				}
			}
		}
	}

	fn skill(name: &str, description: &str, body: &str) -> String {
		format!("---\nname: {name}\ndescription: {description}\n---\n{body}\n")
	}

	fn isolated_options(dir: &Path) -> LoadOptions {
		let mut options = LoadOptions::new(dir);
		options.home = Some(dir.to_path_buf());
		options.agent_dir = Some(dir.join("agent"));
		options
	}

	#[test]
	fn discovers_project_and_user_skills_with_filters() {
		let _env = crate::TEST_ENV_LOCK.lock().expect("env lock");
		let dir = temp_dir();
		let _guard = EnvGuard::isolate(&dir);
		write(&dir.join(".omp/skills/review/SKILL.md"), &skill("review", "Code review", "# Review\nbody"));
		write(&dir.join("agent/skills/test/SKILL.md"), &skill("test", "Run tests", "# Test"));
		write(&dir.join("agent/skills/undescribed/SKILL.md"), "---\nname: undescribed\n---\nno description\n");
		write(&dir.join("agent/skills/disabled/SKILL.md"), "---\ndescription: off\nenabled: false\n---\nx\n");

		let all = load_with(&isolated_options(&dir)).expect("skills").prompt_skills();
		let names: Vec<&str> = all.iter().map(|skill| skill.name.as_str()).collect();
		assert_eq!(names, ["review", "test"]);
		assert!(all[0].body.contains("body"));

		let mut filtered_options = isolated_options(&dir);
		filtered_options.patterns = vec!["test".to_string()];
		let filtered = load_with(&filtered_options).expect("skills").prompt_skills();
		assert_eq!(filtered.len(), 1);
		assert_eq!(filtered[0].name, "test");
		let mut none_options = isolated_options(&dir);
		none_options.no_skills = true;
		assert!(load_with(&none_options).expect("skills").prompt_skills().is_empty());
	}

	#[test]
	fn managed_dir_is_discovered_last_and_never_shadows_authored() {
		let dir = temp_dir();
		write(&dir.join(".omp/skills/deploy/SKILL.md"), &skill("deploy", "Authored deploy", "authored"));
		write(
			&dir.join("agent/managed-skills/deploy/SKILL.md"),
			&skill("deploy", "Managed deploy", "managed"),
		);
		write(
			&dir.join("agent/managed-skills/notes/SKILL.md"),
			&skill("notes", "Take notes", "managed notes"),
		);
		let result = load_with(&isolated_options(&dir)).expect("skills");
		let deploy = result.find("deploy").expect("deploy");
		assert_eq!(deploy.skill.body.trim(), "authored");
		assert!(!deploy.is_managed());
		let notes = result.find("notes").expect("notes");
		assert!(notes.is_managed());
		assert_eq!(notes.skill.description, "Take notes");
	}

	#[test]
	fn managed_dir_stays_visible_when_thirds_party_providers_are_off() {
		let dir = temp_dir();
		write(
			&dir.join("agent/managed-skills/notes/SKILL.md"),
			&skill("notes", "Take notes", "managed notes"),
		);
		let mut options = isolated_options(&dir);
		options.enable_codex_user = false;
		options.enable_claude_user = false;
		options.enable_claude_project = false;
		options.enable_pi_user = false;
		options.enable_pi_project = false;
		options.enable_agents_user = false;
		options.enable_agents_project = false;
		let result = load_with(&options).expect("skills");
		assert!(result.find("notes").is_some());
	}

	#[test]
	fn hide_excludes_from_prompt_but_stays_reachable() {
		let dir = temp_dir();
		write(
			&dir.join("agent/skills/quiet/SKILL.md"),
			"---\nname: quiet\ndescription: hidden\nhide: true\n---\nhidden body\n",
		);
		write(
			&dir.join("agent/skills/loud/SKILL.md"),
			&skill("loud", "visible", "loud body"),
		);
		let result = load_with(&isolated_options(&dir)).expect("skills");
		let skills = result.prompt_skills();
		let visible: Vec<&str> = skills.iter().map(|skill| skill.name.as_str()).collect();
		assert_eq!(visible, ["loud"]);
		assert!(result.find("quiet").is_some());
		assert!(result.find("quiet").expect("quiet").hide);
	}

	#[test]
	fn custom_directory_overrides_default_provider_and_dedupes_realpath() {
		let dir = temp_dir();
		write(
			&dir.join("agent/skills/review/SKILL.md"),
			&skill("review", "default", "default body"),
		);
		write(
			&dir.join("shared/review/SKILL.md"),
			&skill("review", "custom", "custom body"),
		);
		let mut options = isolated_options(&dir);
		options.custom_directories = vec![dir.join("shared")];
		let result = load_with(&options).expect("skills");
		let review = result.find("review").expect("review");
		assert_eq!(review.source, "custom:user");
		assert_eq!(review.skill.body.trim(), "custom body");

		// The same real path is loaded only once even when two roots reach it.
		let mut options = isolated_options(&dir);
		options.custom_directories = vec![dir.join("shared"), dir.join("shared")];
		let result = load_with(&options).expect("skills");
		assert_eq!(result.entries.iter().filter(|entry| entry.skill.name == "review").count(), 1);
	}

	#[test]
	fn ignored_include_and_disabled_extensions_filter_by_name() {
		let dir = temp_dir();
		for name in ["alpha", "beta", "gamma"] {
			write(
				&dir.join(format!("agent/skills/{name}/SKILL.md")),
				&skill(name, name, "body"),
			);
		}
		let mut options = isolated_options(&dir);
		options.ignored_skills = vec!["beta".to_string()];
		let names: Vec<String> = load_with(&options)
			.expect("skills")
			.entries
			.into_iter()
			.map(|entry| entry.skill.name)
			.collect();
		assert_eq!(names, ["alpha", "gamma"]);

		let mut options = isolated_options(&dir);
		options.include_skills = vec!["a*".to_string()];
		let names: Vec<String> = load_with(&options)
			.expect("skills")
			.entries
			.into_iter()
			.map(|entry| entry.skill.name)
			.collect();
		assert_eq!(names, ["alpha"]);

		let mut options = isolated_options(&dir);
		options.disabled_extensions = vec!["skill:gamma".to_string()];
		let result = load_with(&options).expect("skills");
		let names: Vec<String> =
			result.entries.iter().map(|entry| entry.skill.name.clone()).collect();
		assert_eq!(names, ["alpha", "beta"]);
		let snapshot = SkillSnapshot::new();
		snapshot.set(result.entries);
		assert!(snapshot.find("gamma").is_none());
		assert!(!snapshot.prompt_skills().iter().any(|skill| skill.name == "gamma"));
		assert!(
			expand_skill_invocation(&snapshot, "/skill:gamma").expect("expand").is_none(),
			"a disabled skill must not expand through the same snapshot"
		);
	}

	#[test]
	fn config_disabled_extensions_reach_load_options() {
		let config = crate::config::Config {
			disabled_extensions: Some(vec!["skill:quiet".to_string()]),
			..crate::config::Config::default()
		};
		let options = LoadOptions::from_config(&config, ".", false, &[]);
		assert_eq!(options.disabled_extensions, ["skill:quiet"]);
	}

	#[test]
	fn provider_toggles_gate_third_party_dirs() {
		let dir = temp_dir();
		let project = dir.join("project");
		write(&project.join(".claude/skills/c/SKILL.md"), &skill("c", "claude project", "body"));
		write(&project.join(".agent/skills/a/SKILL.md"), &skill("a", "agent project", "body"));
		write(&project.join(".codex/skills/x/SKILL.md"), &skill("x", "codex project", "body"));

		let mut options = isolated_options(&dir);
		options.cwd = project.clone();
		options.enable_claude_project = false;
		options.enable_agents_project = false;
		let names: Vec<String> = load_with(&options)
			.expect("skills")
			.entries
			.into_iter()
			.map(|entry| entry.skill.name)
			.collect();
		assert_eq!(names, ["x"]);

		let mut options = isolated_options(&dir);
		options.cwd = project;
		options.enable_agents_project = false;
		let names: Vec<String> = load_with(&options)
			.expect("skills")
			.entries
			.into_iter()
			.map(|entry| entry.skill.name)
			.collect();
		assert_eq!(names, ["c", "x"]);
	}

	#[tokio::test]
	async fn frontmatter_less_third_party_skill_loads_body_prompt_and_uri() {
		use pi_agent::tools::internal_urls::{InternalUrl, ProtocolHandler, ResolveContext};

		let dir = temp_dir();
		let body = "# Bare\nno frontmatter at all\n";
		write(&dir.join(".claude/skills/bare/SKILL.md"), body);

		let result = load_with(&isolated_options(&dir)).expect("skills");
		let bare = result.find("bare").expect("frontmatter-less claude skill loads");
		assert_eq!(bare.skill.name, "bare", "the directory name is the name fallback");
		assert_eq!(bare.skill.body, body, "the whole file becomes the body");
		assert_eq!(bare.source, "claude:user");
		assert_eq!(
			result.prompt_skills().iter().map(|skill| skill.name.as_str()).collect::<Vec<_>>(),
			["bare"]
		);

		let snapshot = SkillSnapshot::new();
		snapshot.set(result.entries.clone());
		let handler = crate::skill_protocol::SkillUrlHandler::new(snapshot);
		let resource = handler
			.resolve(
				&InternalUrl::parse("skill://bare").expect("url"),
				&ResolveContext::default(),
			)
			.await
			.expect("skill:// resolves a frontmatter-less skill");
		assert!(resource.content.contains("no frontmatter at all"));
		assert_eq!(resource.content_type, "text/markdown");
	}

	#[test]
	fn invalid_native_frontmatter_warns_and_skips_the_skill() {
		let dir = temp_dir();
		write(&dir.join("agent/skills/broken/SKILL.md"), "---\nname: broken\n- stray\n---\nbody\n");

		let result = load_with(&isolated_options(&dir)).expect("skills");
		assert!(result.find("broken").is_none(), "a native skill still needs a description");
		assert!(
			result
				.warnings
				.iter()
				.any(|warning| warning.message.contains("Failed to parse YAML frontmatter")),
			"the YAML failure is reported, not swallowed: {:?}",
			result.warnings
		);
	}

	#[test]
	fn load_options_follow_the_skills_group() {
		let mut config = crate::config::Config::default();
		config.skills.enabled = Some(false);
		config.skills.enable_skill_commands = Some(false);
		config.skills.enable_pi_project = Some(false);
		config.skills.custom_directories = Some(vec![PathBuf::from("~/skills")]);
		config.skills.ignored_skills = Some(vec!["draft".to_string()]);
		let options = LoadOptions::from_config(&config, "/work", true, &["a*".to_string()]);
		assert!(!options.enabled);
		assert!(!options.enable_skill_commands);
		assert!(!options.enable_pi_project);
		assert!(options.enable_claude_user, "unset toggles keep the TS default");
		assert_eq!(options.custom_directories, [PathBuf::from("~/skills")]);
		assert_eq!(options.ignored_skills, ["draft"]);
		assert!(options.no_skills);
		assert_eq!(options.patterns, ["a*"]);
	}

	#[test]
	fn skill_invocation_renders_the_user_template_and_reaches_hidden_skills() {
		let dir = temp_dir();
		write(
			&dir.join("agent/skills/quiet/SKILL.md"),
			"---\nname: quiet\ndescription: hidden\nhide: true\n---\n# Quiet\nbody\n",
		);
		let result = load_with(&isolated_options(&dir)).expect("skills");
		let snapshot = SkillSnapshot::new();
		snapshot.set(result.entries);

		let expanded = expand_skill_invocation(&snapshot, "/skill:quiet focus")
			.expect("expand")
			.expect("known skill");
		assert!(expanded.contains("[IMPORTANT: User invoked the \"quiet\" skill"));
		assert!(expanded.contains("# Quiet\nbody"));
		assert!(expanded.ends_with("User: focus"));
		assert!(expand_skill_invocation(&snapshot, "/skill:missing").expect("expand").is_none());
		assert!(expand_skill_invocation(&snapshot, "plain prompt").expect("expand").is_none());
	}

	#[test]
	fn snapshot_refresh_is_per_session() {
		let dir = temp_dir();
		write(&dir.join(".omp/skills/deploy/SKILL.md"), &skill("deploy", "Authored", "body"));
		let first = SkillSnapshot::new();
		let second = SkillSnapshot::new();
		let options = isolated_options(&dir);
		first.refresh(&options).expect("refresh");
		assert!(first.find("deploy").is_some());
		assert!(second.find("deploy").is_none());
		assert!(first.is_name_claimed_by_authored_skill("deploy"));

		write(
			&dir.join("agent/managed-skills/notes/SKILL.md"),
			&skill("notes", "Take notes", "body"),
		);
		first.refresh(&options).expect("refresh");
		assert!(first.find("notes").is_some());
		// A managed name is not "claimed by authored": minting a skill must be
		// refused only when an authored skill owns the name.
		assert!(!first.is_name_claimed_by_authored_skill("notes"));
		assert!(second.find("notes").is_none());
	}

	#[test]
	fn parses_leading_and_mid_prompt_invocations() {
		assert_eq!(
			parse_skill_invocation("/skill:review check auth"),
			Some(ParsedInvocation { name: "review".to_string(), args: "check auth".to_string() })
		);
		assert_eq!(
			parse_skill_invocation("  /skill:review"),
			Some(ParsedInvocation { name: "review".to_string(), args: String::new() })
		);
		assert_eq!(
			parse_skill_invocation("fix the bug /skill:review focus on auth"),
			Some(ParsedInvocation {
				name:  "review".to_string(),
				args:  "fix the bug focus on auth".to_string(),
			})
		);
		assert!(parse_skill_invocation("/compact /skill:review").is_none());
		assert!(parse_skill_invocation("!echo /skill:review").is_none());
		assert!(parse_skill_invocation("$ cmd /skill:review").is_none());
		assert!(parse_skill_invocation("$$ cmd /skill:review").is_none());
		// `$cmd` without a following space is not a python sigil, so the
		// mid-prompt form still applies (mirrors `pythonCommandPrefixLength`).
		assert!(parse_skill_invocation("$cmd /skill:review").is_some());
		assert!(parse_skill_invocation("mention /skill: only").is_none());
	}

	#[test]
	fn renders_ts_skill_prompt_templates() {
		let dir = temp_dir();
		let path = dir.join(".omp/skills/review/SKILL.md");
		write(&path, &skill("review", "Code review", "# Review\nbody"));
		let result = load_with(&isolated_options(&dir)).expect("skills");
		let skill = result.find("review").expect("review").skill.clone();
		assert_eq!(slash_command_name(&skill.name), "skill:review");

		let user = build_skill_prompt_message(&skill, " check auth ", SkillInvocationKind::User)
			.expect("message");
		assert!(user.message.starts_with(
			"[IMPORTANT: User invoked the \"review\" skill; follow its instructions. Full skill below.]"
		));
		assert!(user.message.contains("# Review\nbody"));
		assert!(user.message.contains(&format!("[Skill directory: {}]", dir.join(".omp/skills/review").display())));
		assert!(user.message.ends_with("User: check auth"));
		assert_eq!(user.args.as_deref(), Some("check auth"));
		assert_eq!(user.line_count, 2);

		let bare = build_skill_prompt_message(&skill, "", SkillInvocationKind::User).expect("message");
		assert!(!bare.message.contains("\nUser:"));
		assert!(bare.args.is_none());

		let autoload = build_skill_prompt_message(&skill, "go", SkillInvocationKind::Autoload)
			.expect("message");
		assert!(autoload
			.message
			.starts_with(&format!("# Review\nbody\n\n---\n\nSkill: {}", path.display())));
		assert!(autoload.message.ends_with("User: go"));
		assert!(!autoload.message.contains("IMPORTANT"));
	}
}
