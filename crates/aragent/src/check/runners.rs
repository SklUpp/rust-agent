//! Project checker discovery and bounded execution.
//!
//! Port of `cleanse/checkers.ts`: discover configured language checkers from a
//! project snapshot without installing anything, then run each selected checker
//! and normalize its diagnostics. Missing executables become
//! [`SkippedCleanseCheck`] entries; a checker that fails without producing a
//! parsable diagnostic contributes one failure diagnostic capped at
//! [`MAX_FAILURE_OUTPUT`] bytes.

use std::{
	collections::{HashMap, HashSet},
	fs,
	io::Read,
	path::{Path, PathBuf},
	process::{Command, Stdio},
	sync::{Arc, mpsc},
	thread,
	time::{Duration, Instant},
};

use serde_json::Value;
use tokio::{
	io::{AsyncRead, AsyncReadExt},
	process::Command as ProcessCommand,
	sync::Semaphore,
	time::timeout,
};

use super::{
	CleanseCheckResult, CleanseCheckerDescriptor, CleanseDiagnostic, CleanseDiagnosticReport,
	CleanseDiagnosticSuiteOptions, CleanseParserInput, CleanseParserKind, CleanseSeverity,
	CustomCleanseCheckerSpec, SkippedCleanseCheck, diagnostic_key, parse_cleanse_diagnostics,
	parsers::{normalize_lexical, path_relative, sanitize_text},
};

/// Directories never traversed or reported when snapshotting a project.
const IGNORED_DIRECTORIES: [&str; 11] = [
	".git",
	".hg",
	".svn",
	".terraform",
	".venv",
	"build",
	"coverage",
	"dist",
	"node_modules",
	"target",
	"vendor",
];
/// Cap on checker failure output embedded in one diagnostic.
pub const MAX_FAILURE_OUTPUT: usize = 12_000;
/// Captured stdout/stderr per checker; the rest is drained but discarded.
const MAX_CAPTURE_BYTES: usize = 256 * 1024;
/// Maximum checker processes allowed to run at once.
const MAX_CONCURRENT_CHECKERS: usize = 8;
/// Wall-clock limit for one checker invocation.
const CHECKER_TIMEOUT: Duration = Duration::from_secs(300);
/// Wall-clock limit for Cargo workspace metadata discovery.
const CARGO_METADATA_TIMEOUT: Duration = Duration::from_secs(60);
/// Metadata capture cap. Exceeding it fails discovery instead of truncating JSON.
const CARGO_METADATA_CAPTURE_BYTES: usize = 4 * 1024 * 1024;
/// Shell files per ShellCheck invocation.
const SHELLCHECK_BATCH_SIZE: usize = 200;

#[derive(Clone, Debug)]
struct CheckerPlan {
	id:         String,
	label:      String,
	language:   String,
	cwd:        PathBuf,
	executable: PathBuf,
	args:       Vec<String>,
	parser:     CleanseParserKind,
	command:    String,
	mutates:    bool,
}

struct PlanRequest<'a> {
	id:         &'a str,
	label:      &'a str,
	language:   &'a str,
	root:       &'a str,
	binaries:   &'a [&'a str],
	args:       Vec<String>,
	parser:     CleanseParserKind,
	executable: Option<PathBuf>,
	mutates:    bool,
	optional:   bool,
}

struct DiscoveryState {
	project_cwd:   PathBuf,
	files:         Vec<String>,
	plans:         Vec<CheckerPlan>,
	skipped:       Vec<SkippedCleanseCheck>,
	include_tests: bool,
}

/// Discovery failure tied to the checker id it prevented from running.
#[derive(Clone, Debug)]
struct DiscoveryFailure {
	checker_id: String,
	diagnostic: CleanseDiagnostic,
}

/// Re-runnable checker set discovered from one project snapshot.
#[derive(Clone, Debug)]
pub struct CleanseDiagnosticSuite {
	project_cwd:   PathBuf,
	plans:         Vec<CheckerPlan>,
	checkers:      Vec<CleanseCheckerDescriptor>,
	active:             Vec<usize>,
	skipped:            Vec<SkippedCleanseCheck>,
	allowed_files:      HashSet<String>,
	discovery_failures: Vec<DiscoveryFailure>,
	active_failures:    Vec<usize>,
}

impl CleanseDiagnosticSuite {
	fn new(
		project_cwd: PathBuf,
		plans: Vec<CheckerPlan>,
		skipped: Vec<SkippedCleanseCheck>,
		files: Vec<String>,
	) -> Self {
		let checkers = plans.iter().map(|plan| describe(&project_cwd, plan)).collect();
		let active = (0..plans.len()).collect();
		Self {
			project_cwd,
			plans,
			checkers,
			active,
			skipped,
			allowed_files: files.into_iter().collect(),
			discovery_failures: Vec::new(),
			active_failures: Vec::new(),
		}
	}

	fn with_discovery_failures(mut self, failures: Vec<DiscoveryFailure>) -> Self {
		self.discovery_failures = failures;
		self.active_failures = (0..self.discovery_failures.len()).collect();
		self
	}

	fn failed(project_cwd: PathBuf, reason: String) -> Self {
		let skipped = vec![SkippedCleanseCheck {
			label:    "checker discovery".to_owned(),
			language: "unknown".to_owned(),
			reason,
		}];
		Self::new(project_cwd, Vec::new(), skipped, Vec::new())
	}

	/// Every discovered checker; unaffected by [`Self::select`].
	pub fn checkers(&self) -> &[CleanseCheckerDescriptor] {
		&self.checkers
	}

	/// Checkers the next [`Self::run`] will execute.
	pub fn selected(&self) -> Vec<CleanseCheckerDescriptor> {
		self.active
			.iter()
			.filter_map(|&index| self.plans.get(index))
			.map(|plan| describe(&self.project_cwd, plan))
			.collect()
	}

	/// Checkers omitted because a required executable was unavailable.
	pub fn skipped(&self) -> &[SkippedCleanseCheck] {
		&self.skipped
	}

	/// Narrow subsequent runs to exact checker ids or checker-kind prefixes
	/// (`clippy` matches `clippy:.`). Discovery failures are selected by the same
	/// ids, so a failed `clippy:.` discovery can be inspected without a plan.
	///
	/// # Errors
	///
	/// Returns an error naming every pattern that matched no checker; no
	/// checker remains selected after an error.
	pub fn select(&mut self, ids: &[&str]) -> Result<(), String> {
		if ids.is_empty() {
			self.active = (0..self.plans.len()).collect();
			self.active_failures = (0..self.discovery_failures.len()).collect();
			return Ok(());
		}
		let mut matched = vec![false; ids.len()];
		let mut active = Vec::new();
		for (index, plan) in self.plans.iter().enumerate() {
			let mut keep = false;
			for (pattern_index, pattern) in ids.iter().copied().enumerate() {
				if checker_id_matches(&plan.id, pattern) {
					matched[pattern_index] = true;
					keep = true;
				}
			}
			if keep {
				active.push(index);
			}
		}
		let mut active_failures = Vec::new();
		for (index, failure) in self.discovery_failures.iter().enumerate() {
			let mut keep = false;
			for (pattern_index, pattern) in ids.iter().copied().enumerate() {
				if checker_id_matches(&failure.checker_id, pattern) {
					matched[pattern_index] = true;
					keep = true;
				}
			}
			if keep {
				active_failures.push(index);
			}
		}
		let unmatched: Vec<&str> = ids
			.iter()
			.zip(matched)
			.filter_map(|(id, hit)| (!hit).then_some(*id))
			.collect();
		if !unmatched.is_empty() {
			let available = self
				.checkers
				.iter()
				.map(|checker| checker.id.as_str())
				.chain(
					self.discovery_failures
						.iter()
						.map(|failure| failure.checker_id.as_str()),
				)
				.collect::<Vec<_>>()
				.join(", ");
			self.active.clear();
			self.active_failures.clear();
			return Err(format!(
				"unknown checker id(s): {}; available: {available}",
				unmatched.join(", ")
			));
		}
		self.active = active;
		self.active_failures = active_failures;
		Ok(())
	}

	/// Run every selected checker and aggregate normalized diagnostics.
	pub async fn run(&self) -> CleanseDiagnosticReport {
		let active: Vec<&CheckerPlan> =
			self.active.iter().filter_map(|&index| self.plans.get(index)).collect();
		let allowed = Arc::new(self.allowed_files.clone());
		let mut checks: Vec<(usize, CleanseCheckResult)> = Vec::with_capacity(active.len());

		// Mutating checkers rewrite files, so they run alone and in order.
		for (position, plan) in active.iter().enumerate() {
			if plan.mutates {
				let check = run_checker((**plan).clone(), self.project_cwd.clone(), Arc::clone(&allowed))
					.await;
				checks.push((position, check));
			}
		}

		let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CHECKERS));
		let mut handles = Vec::new();
		for (position, plan) in active.iter().enumerate() {
			if plan.mutates {
				continue;
			}
			let plan = (**plan).clone();
			let descriptor = describe(&self.project_cwd, &plan);
			let project_cwd = self.project_cwd.clone();
			let allowed = Arc::clone(&allowed);
			let permit = semaphore
				.clone()
				.acquire_owned()
				.await
				.expect("checker semaphore is never closed");
			let handle = tokio::spawn(async move {
				let _permit = permit;
				run_checker(plan, project_cwd, allowed).await
			});
			handles.push((position, descriptor, handle));
		}
		for (position, descriptor, handle) in handles {
			let check = match handle.await {
				Ok(check) => check,
				Err(error) => {
					internal_failure(descriptor, format!("checker task failed: {error}"))
				},
			};
			checks.push((position, check));
		}

		checks.sort_by_key(|(position, _)| *position);
		let checks: Vec<CleanseCheckResult> =
			checks.into_iter().map(|(_, check)| check).collect();
		let mut discovered = self
			.active_failures
			.iter()
			.filter_map(|&index| self.discovery_failures.get(index))
			.map(|failure| failure.diagnostic.clone())
			.collect::<Vec<_>>();
		discovered.extend(checks.iter().flat_map(|check| check.diagnostics.iter().cloned()));
		let diagnostics = deduplicate_project_diagnostics(discovered);
		CleanseDiagnosticReport { checks, diagnostics, skipped: self.skipped.clone() }
	}
}

/// Discover configured language checkers without installing missing tools.
pub async fn discover_cleanse_diagnostic_suite(
	project_cwd: &Path,
	options: CleanseDiagnosticSuiteOptions,
) -> CleanseDiagnosticSuite {
	let cwd = project_cwd.to_path_buf();
	let fallback = cwd.clone();
	match tokio::task::spawn_blocking(move || discover_blocking(&cwd, &options)).await {
		Ok(suite) => suite,
		Err(error) => {
			CleanseDiagnosticSuite::failed(fallback, format!("checker discovery failed: {error}"))
		},
	}
}

/// Build a runnable suite from discovery-agent checker specs, dropping
/// unrunnable entries into `skipped`.
pub async fn build_custom_cleanse_suite(
	project_cwd: &Path,
	specs: Vec<CustomCleanseCheckerSpec>,
) -> CleanseDiagnosticSuite {
	let cwd = project_cwd.to_path_buf();
	let fallback = cwd.clone();
	match tokio::task::spawn_blocking(move || build_custom_blocking(&cwd, &specs)).await {
		Ok(suite) => suite,
		Err(error) => {
			CleanseDiagnosticSuite::failed(fallback, format!("custom checker setup failed: {error}"))
		},
	}
}

fn discover_blocking(
	project_cwd: &Path,
	options: &CleanseDiagnosticSuiteOptions,
) -> CleanseDiagnosticSuite {
	let project_cwd = resolve_project_cwd(project_cwd);
	let files = list_project_files(&project_cwd);
	let mut state = DiscoveryState {
		project_cwd,
		files,
		plans: Vec::new(),
		skipped: Vec::new(),
		include_tests: options.include_tests,
	};
	let mut failures = Vec::new();
	discover_rust(&mut state, &mut failures);
	discover_go(&mut state);
	discover_python(&mut state);
	discover_javascript(&mut state);
	discover_ruby(&mut state);
	discover_php(&mut state);
	discover_swift(&mut state);
	discover_dart(&mut state);
	discover_elixir(&mut state);
	discover_shell(&mut state);
	discover_haskell(&mut state);
	discover_terraform(&mut state);
	discover_lua(&mut state);
	discover_cpp(&mut state);
	discover_dotnet(&mut state);
	discover_zig(&mut state);
	discover_jvm(&mut state);
	discover_github_actions(&mut state);
	let DiscoveryState { project_cwd, files, plans, skipped, .. } = state;
	CleanseDiagnosticSuite::new(project_cwd, plans, skipped, files)
		.with_discovery_failures(failures)
}

fn build_custom_blocking(
	project_cwd: &Path,
	specs: &[CustomCleanseCheckerSpec],
) -> CleanseDiagnosticSuite {
	let project_cwd = resolve_project_cwd(project_cwd);
	let files = list_project_files(&project_cwd);
	let mut plans = Vec::new();
	let mut skipped = Vec::new();
	for (index, spec) in specs.iter().enumerate() {
		let label = {
			let trimmed = spec.label.trim();
			if trimmed.is_empty() {
				format!("custom checker {}", index + 1)
			} else {
				trimmed.to_owned()
			}
		};
		let language = spec
			.language
			.as_deref()
			.map(str::trim)
			.filter(|value| !value.is_empty())
			.unwrap_or("Custom")
			.to_owned();
		let Some(binary) = spec.command.first() else {
			skipped.push(SkippedCleanseCheck {
				label,
				language,
				reason: "empty command".to_owned(),
			});
			continue;
		};
		let args = spec.command[1..].to_vec();
		let Some(root) = normalize_custom_root(&project_cwd, spec.cwd.as_deref()) else {
			skipped.push(SkippedCleanseCheck {
				label,
				language,
				reason: format!(
					"working directory escapes the project: {}",
					spec.cwd.as_deref().unwrap_or("")
				),
			});
			continue;
		};
		let executable = if binary.contains('/') || binary.contains('\\') {
			let candidate = project_cwd.join(&root).join(binary);
			is_executable_file(&candidate).then_some(candidate)
		} else {
			resolve_binary(&project_cwd, &root, &[binary.as_str()])
		};
		let Some(executable) = executable else {
			skipped.push(SkippedCleanseCheck {
				label,
				language,
				reason: format!("executable not found: {binary}"),
			});
			continue;
		};
		let parser = spec
			.parser
			.as_deref()
			.and_then(CleanseParserKind::from_name)
			.unwrap_or(CleanseParserKind::Generic);
		let display = executable
			.file_name()
			.map_or_else(|| executable.to_string_lossy().into_owned(), |name| name.to_string_lossy().into_owned());
		let mut argv = Vec::with_capacity(args.len() + 1);
		argv.push(display);
		argv.extend(args.iter().cloned());
		plans.push(CheckerPlan {
			id: format!("custom-{}", index + 1),
			label,
			language,
			cwd: project_cwd.join(&root),
			executable,
			args,
			parser,
			command: format_command(&argv),
			mutates: false,
		});
	}
	CleanseDiagnosticSuite::new(project_cwd, plans, skipped, files)
}

fn normalize_custom_root(project_cwd: &Path, cwd: Option<&str>) -> Option<String> {
	let trimmed = cwd.map(str::trim).filter(|value| !value.is_empty());
	let Some(trimmed) = trimmed else {
		return Some(".".to_owned());
	};
	if trimmed == "." {
		return Some(".".to_owned());
	}
	let resolved = fs::canonicalize(project_cwd.join(trimmed)).ok()?;
	if !resolved.starts_with(project_cwd) {
		return None;
	}
	let relative = path_relative(&resolved, project_cwd)?;
	if relative.as_os_str().is_empty() {
		return Some(".".to_owned());
	}
	let rendered = relative.to_string_lossy().replace('\\', "/");
	if rendered.starts_with("..") || relative.is_absolute() {
		None
	} else {
		Some(rendered)
	}
}

fn resolve_project_cwd(project_cwd: &Path) -> PathBuf {
	fs::canonicalize(project_cwd).unwrap_or_else(|_| {
		if project_cwd.is_absolute() {
			normalize_lexical(project_cwd)
		} else {
			std::env::current_dir()
				.map(|current| normalize_lexical(&current.join(project_cwd)))
				.unwrap_or_else(|_| project_cwd.to_path_buf())
		}
	})
}

fn list_project_files(cwd: &Path) -> Vec<String> {
	let mut files = Vec::new();
	walk_project(cwd, cwd, &mut files);
	normalize_files(files)
}

fn walk_project(root: &Path, directory: &Path, files: &mut Vec<String>) {
	let Ok(entries) = fs::read_dir(directory) else {
		return;
	};
	let mut entries: Vec<fs::DirEntry> = entries.filter_map(Result::ok).collect();
	entries.sort_by_key(fs::DirEntry::file_name);
	for entry in entries {
		let name = entry.file_name();
		let name = name.to_string_lossy();
		let Ok(file_type) = entry.file_type() else {
			continue;
		};
		if file_type.is_dir() {
			if is_ignored_directory(&name) {
				continue;
			}
			walk_project(root, &entry.path(), files);
		} else if file_type.is_file() {
			let path = entry.path();
			let Ok(relative) = path.strip_prefix(root) else {
				continue;
			};
			let normalized = relative.to_string_lossy().replace('\\', "/");
			if !has_ignored_directory(&normalized) {
				files.push(normalized);
			}
		}
	}
}

fn normalize_files(files: Vec<String>) -> Vec<String> {
	let mut seen = HashSet::new();
	let mut normalized = Vec::new();
	for file in files {
		let file = file.replace('\\', "/");
		let value = file.strip_prefix("./").unwrap_or(file.as_str());
		if value.is_empty() || has_ignored_directory(value) || !seen.insert(value.to_owned()) {
			continue;
		}
		normalized.push(value.to_owned());
	}
	normalized.sort();
	normalized
}

fn is_ignored_directory(name: &str) -> bool {
	IGNORED_DIRECTORIES.contains(&name) || name.starts_with("bazel-")
}

fn has_ignored_directory(file: &str) -> bool {
	file.split('/').any(is_ignored_directory)
}

fn basename(file: &str) -> &str {
	file.rsplit('/').next().unwrap_or(file)
}

fn posix_dirname(file: &str) -> &str {
	file.rfind('/').map_or(".", |index| &file[..index])
}

fn posix_join(root: &str, file: &str) -> String {
	if root == "." { file.to_owned() } else { format!("{root}/{file}") }
}

fn contains_file(files: &[String], file: &str) -> bool {
	files.iter().any(|candidate| candidate.as_str() == file)
}

fn marker_roots(
	files: &[String],
	matches: impl Fn(&str, &str) -> bool,
) -> Vec<String> {
	let roots: Vec<String> = files
		.iter()
		.filter(|file| matches(file.as_str(), basename(file.as_str())))
		.map(|file| posix_dirname(file.as_str()).to_owned())
		.collect();
	topmost_roots(&roots)
}

fn roots_for_basenames(files: &[String], basenames: &[&str]) -> Vec<String> {
	marker_roots(files, |_file, name| basenames.contains(&name))
}

fn roots_for_pattern(files: &[String], matches: impl Fn(&str) -> bool) -> Vec<String> {
	marker_roots(files, |file, _name| matches(file))
}

fn topmost_roots(roots: &[String]) -> Vec<String> {
	let mut unique: Vec<String> = roots.to_vec();
	unique.sort();
	unique.dedup();
	unique.sort_by(|left, right| {
		root_depth(left.as_str())
			.cmp(&root_depth(right.as_str()))
			.then_with(|| left.cmp(right))
	});
	let mut selected: Vec<String> = Vec::new();
	for root in unique {
		let root = root.as_str();
		if selected.iter().any(|parent| {
			let parent = parent.as_str();
			root == parent
				|| parent == "."
				|| root
					.strip_prefix(parent)
					.is_some_and(|remainder| remainder.starts_with('/'))
		}) {
			continue;
		}
		selected.push(root.to_owned());
	}
	selected
}

fn root_depth(root: &str) -> usize {
	if root == "." { 0 } else { root.split('/').count() }
}

fn contains_extension(files: &[String], extensions: &[&str]) -> bool {
	files.iter().any(|file| {
		let lower = file.to_ascii_lowercase();
		extensions.iter().any(|extension| lower.ends_with(*extension))
	})
}

fn lower_ends_with(file: &str, suffix: &str) -> bool {
	file.to_ascii_lowercase().ends_with(suffix)
}

fn resolve_binary(project_cwd: &Path, root: &str, names: &[&str]) -> Option<PathBuf> {
	let cwd = project_cwd.join(root);
	let mut search_directories = Vec::new();
	let mut current: Option<&Path> = Some(&cwd);
	while let Some(directory) = current {
		for relative in [
			"node_modules/.bin",
			if cfg!(windows) { ".venv/Scripts" } else { ".venv/bin" },
			if cfg!(windows) { "venv/Scripts" } else { "venv/bin" },
			"vendor/bin",
		] {
			search_directories.push(directory.join(relative));
		}
		if directory == project_cwd {
			break;
		}
		let Some(parent) = directory.parent() else {
			break;
		};
		if parent == directory
			|| !(parent == project_cwd || parent.starts_with(project_cwd))
		{
			break;
		}
		current = Some(parent);
	}
	if let Some(path) = std::env::var_os("PATH") {
		search_directories.extend(
			std::env::split_paths(&path).filter(|path| !path.as_os_str().is_empty()),
		);
	}
	let mut seen = HashSet::new();
	let search_directories: Vec<PathBuf> = search_directories
		.into_iter()
		.filter(|directory| seen.insert(directory.clone()))
		.collect();
	for name in names {
		for directory in &search_directories {
			let candidate = directory.join(*name);
			if is_executable_file(&candidate) {
				return Some(candidate);
			}
		}
	}
	None
}

fn is_executable_file(path: &Path) -> bool {
	let Ok(metadata) = fs::metadata(path) else {
		return false;
	};
	if !metadata.is_file() {
		return false;
	}
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		metadata.permissions().mode() & 0o111 != 0
	}
	#[cfg(not(unix))]
	{
		true
	}
}

fn add_plan(state: &mut DiscoveryState, request: PlanRequest<'_>) {
	let cwd = state.project_cwd.join(request.root);
	let executable = request
		.executable
		.or_else(|| resolve_binary(&state.project_cwd, request.root, request.binaries));
	let root_label = request.root;
	let Some(executable) = executable else {
		if request.optional {
			return;
		}
		state.skipped.push(SkippedCleanseCheck {
			label:    format!("{} ({root_label})", request.label),
			language: request.language.to_owned(),
			reason:   format!("executable not found: {}", request.binaries.join(" or ")),
		});
		return;
	};
	let display = executable.file_name().map_or_else(
		|| executable.to_string_lossy().into_owned(),
		|name| name.to_string_lossy().into_owned(),
	);
	let mut argv = Vec::with_capacity(request.args.len() + 1);
	argv.push(display);
	argv.extend(request.args.iter().cloned());
	state.plans.push(CheckerPlan {
		id: format!("{}:{}", request.id, request.root),
		label: format!("{} ({root_label})", request.label),
		language: request.language.to_owned(),
		cwd,
		executable,
		args: request.args,
		parser: request.parser,
		command: format_command(&argv),
		mutates: request.mutates,
	});
}

fn format_command(argv: &[String]) -> String {
	argv
		.iter()
		.map(|argument| {
			if command_argument_is_safe(argument) {
				argument.clone()
			} else {
				serde_json::to_string(argument)
					.expect("serializing a string argument cannot fail")
			}
		})
		.collect::<Vec<_>>()
		.join(" ")
}

fn command_argument_is_safe(argument: &str) -> bool {
	!argument.is_empty()
		&& argument.chars().all(|character| {
			character.is_ascii_alphanumeric()
				|| matches!(character, '_' | '.' | '/' | ':' | '=' | '@' | '+' | '-')
		})
}

fn arg_list(values: &[&str]) -> Vec<String> {
	values.iter().map(|value| (*value).to_owned()).collect()
}

fn checker_id_matches(id: &str, pattern: &str) -> bool {
	id == pattern || id.strip_prefix(pattern).is_some_and(|rest| rest.starts_with(':'))
}

fn discover_rust(state: &mut DiscoveryState, failures: &mut Vec<DiscoveryFailure>) {
	for root in roots_for_basenames(&state.files, &["Cargo.toml"]) {
		let Some(cargo) = resolve_binary(&state.project_cwd, &root, &["cargo"]) else {
			add_plan(state, PlanRequest {
				id:         "clippy",
				label:      "cargo clippy",
				language:   "Rust",
				root:       &root,
				binaries:   &["cargo"],
				args:       Vec::new(),
				parser:     CleanseParserKind::Rust,
				executable: None,
				mutates:    false,
				optional:   false,
			});
			continue;
		};
		let packages = match cargo_workspace_packages(state, &root, &cargo) {
			Ok(packages) => packages,
			Err(reason) => {
				let label = format!("cargo clippy ({root})");
				failures.push(DiscoveryFailure {
					checker_id: format!("clippy:{root}"),
					diagnostic: discovery_failure_diagnostic(label.clone(), reason.clone()),
				});
				state.skipped.push(SkippedCleanseCheck {
					label,
					language: "Rust".to_owned(),
					reason,
				});
				continue;
			},
		};
		if packages.is_empty() {
			state.skipped.push(SkippedCleanseCheck {
				label:    format!("cargo clippy ({root})"),
				language: "Rust".to_owned(),
				reason:   "no first-party workspace packages found".to_owned(),
			});
			continue;
		}
		let mut package_args = Vec::new();
		for package in &packages {
			package_args.push("--package".to_owned());
			package_args.push(package.clone());
		}
		let mut args = arg_list(&["clippy", "--no-deps", "--broken-code", "--allow-no-vcs"]);
		args.extend(if state.include_tests {
			arg_list(&["--all-targets"])
		} else {
			arg_list(&["--lib", "--bins"])
		});
		args.extend(package_args.iter().cloned());
		args.extend(arg_list(&["--message-format=json", "--", "-D", "warnings"]));
		add_plan(state, PlanRequest {
			id:         "clippy",
			label:      "cargo clippy",
			language:   "Rust",
			root:       &root,
			binaries:   &["cargo"],
			args,
			parser:     CleanseParserKind::Rust,
			executable: Some(cargo.clone()),
			mutates:    false,
			optional:   false,
		});
		if state.include_tests {
			let mut test_args = arg_list(&["test", "--no-fail-fast", "--all-targets"]);
			test_args.extend(package_args);
			test_args.push("--message-format=json".to_owned());
			add_plan(state, PlanRequest {
				id:         "cargo-test",
				label:      "cargo test",
				language:   "Rust",
				root:       &root,
				binaries:   &["cargo"],
				args:       test_args,
				parser:     CleanseParserKind::RustTest,
				executable: Some(cargo),
				mutates:    false,
				optional:   false,
			});
		}
	}
}

struct BoundedCommandOutput {
	status:           std::process::ExitStatus,
	stdout:           Vec<u8>,
	stderr:           Vec<u8>,
	stdout_truncated: bool,
}

/// Run a synchronous helper with bounded capture. Timeout and wait errors kill
/// and reap the live child, the synchronous equivalent of `kill_on_drop`.
fn run_bounded_command(
	command: &mut Command,
	timeout: Duration,
	limit: usize,
) -> Result<BoundedCommandOutput, String> {
	command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
	let mut child = command.spawn().map_err(|error| format!("spawn failed: {error}"))?;
	let stdout = child.stdout.take().expect("stdout is piped");
	let stderr = child.stderr.take().expect("stderr is piped");
	let (sender, receiver) = mpsc::channel();
	let stdout_sender = sender.clone();
	thread::spawn(move || {
		let (data, truncated) = read_capped_blocking(stdout, limit);
		let _ = stdout_sender.send((true, data, truncated));
	});
	thread::spawn(move || {
		let (data, truncated) = read_capped_blocking(stderr, limit);
		let _ = sender.send((false, data, truncated));
	});
	let deadline = Instant::now() + timeout;
	let status = loop {
		match child.try_wait() {
			Ok(Some(status)) => break status,
			Ok(None) => {},
			Err(error) => {
				let _ = child.kill();
				let _ = child.wait();
				return Err(format!("wait failed: {error}"));
			},
		}
		if Instant::now() >= deadline {
			let _ = child.kill();
			let _ = child.wait();
			return Err(format!("timed out after {}s", timeout.as_secs()));
		}
		thread::sleep(Duration::from_millis(20));
	};
	let mut stdout = None;
	let mut stderr = None;
	let mut stdout_truncated = false;
	while stdout.is_none() || stderr.is_none() {
		let remaining = deadline.saturating_duration_since(Instant::now());
		if remaining.is_zero() {
			return Err("output capture timed out".to_owned());
		}
		match receiver.recv_timeout(remaining) {
			Ok((true, data, truncated)) => {
				stdout = Some(data);
				stdout_truncated = truncated;
			},
			Ok((false, data, _)) => stderr = Some(data),
			Err(_) => return Err("output capture timed out".to_owned()),
		}
	}
	Ok(BoundedCommandOutput {
		status,
		stdout: stdout.unwrap_or_default(),
		stderr: stderr.unwrap_or_default(),
		stdout_truncated,
	})
}

fn read_capped_blocking<R: Read>(mut reader: R, limit: usize) -> (Vec<u8>, bool) {
	let mut output = Vec::with_capacity(limit.min(8 * 1024));
	let mut buffer = [0_u8; 8 * 1024];
	let mut truncated = false;
	loop {
		match reader.read(&mut buffer) {
			Ok(0) | Err(_) => break,
			Ok(read) => {
				let remaining = limit.saturating_sub(output.len());
				if read > remaining {
					truncated = true;
				}
				if remaining > 0 {
					output.extend_from_slice(&buffer[..read.min(remaining)]);
				}
			},
		}
	}
	(output, truncated)
}

fn cargo_workspace_packages(
	state: &DiscoveryState,
	root: &str,
	cargo: &Path,
) -> Result<Vec<String>, String> {
	let cwd = state.project_cwd.join(root);
	let manifest = cwd.join("Cargo.toml");
	let mut command = Command::new(cargo);
	// `--no-deps` keeps metadata limited to workspace members, not the full graph.
	command
		.args(["metadata", "--no-deps", "--format-version=1", "--manifest-path"])
		.arg(&manifest)
		.current_dir(&cwd);
	let BoundedCommandOutput { status, stdout, stderr, stdout_truncated } =
		run_bounded_command(
			&mut command,
			CARGO_METADATA_TIMEOUT,
			CARGO_METADATA_CAPTURE_BYTES,
		)
		.map_err(|error| format!("cargo metadata failed: {error}"))?;
	if !status.success() {
		let stderr = String::from_utf8_lossy(&stderr);
		let stdout = String::from_utf8_lossy(&stdout);
		let text = if stderr.trim().is_empty() { stdout } else { stderr };
		let text = collapse_whitespace(&sanitize_text(&text));
		return Err(format!("cargo metadata failed: {text}"));
	}
	if stdout_truncated {
		return Err(format!(
			"cargo metadata failed: stdout exceeded {CARGO_METADATA_CAPTURE_BYTES} bytes"
		));
	}
	let parsed: Value = serde_json::from_slice(&stdout)
		.map_err(|error| format!("cargo metadata failed: invalid JSON: {error}"))?;
	let Some(parsed) = parsed.as_object() else {
		return Err("cargo metadata failed: metadata root is not an object".to_owned());
	};
	let workspace_members: HashSet<&str> = parsed
		.get("workspace_members")
		.and_then(Value::as_array)
		.map(|members| members.iter().filter_map(Value::as_str).collect())
		.unwrap_or_default();
	let allowed_files: HashSet<&str> =
		state.files.iter().map(String::as_str).collect();
	let mut packages = Vec::new();
	if let Some(entries) = parsed.get("packages").and_then(Value::as_array) {
		for entry in entries {
			let Some(entry) = entry.as_object() else {
				continue;
			};
			let (Some(id), Some(name), Some(manifest_path)) = (
				entry.get("id").and_then(Value::as_str),
				entry.get("name").and_then(Value::as_str),
				entry.get("manifest_path").and_then(Value::as_str),
			) else {
				continue;
			};
			if !workspace_members.contains(id) {
				continue;
			}
			let Ok(relative) = Path::new(manifest_path).strip_prefix(&state.project_cwd) else {
				continue;
			};
			let relative = relative.to_string_lossy().replace('\\', "/");
			if allowed_files.contains(relative.as_str()) {
				packages.push(name.to_owned());
			}
		}
	}
	packages.sort();
	packages.dedup();
	Ok(packages)
}

fn discover_go(state: &mut DiscoveryState) {
	let work_roots = roots_for_basenames(&state.files, &["go.work"]);
	let roots = if work_roots.is_empty() {
		roots_for_basenames(&state.files, &["go.mod"])
	} else {
		work_roots
	};
	for root in roots {
		let prefix = posix_join(&root, "staticcheck.conf");
		let staticcheck_optional = !contains_file(&state.files, &prefix);
		add_plan(state, PlanRequest {
			id:         "go-vet",
			label:      "go vet",
			language:   "Go",
			root:       &root,
			binaries:   &["go"],
			args:       arg_list(&["vet", "-json", "./..."]),
			parser:     CleanseParserKind::Go,
			executable: None,
			mutates:    false,
			optional:   false,
		});
		add_plan(state, PlanRequest {
			id:         "staticcheck",
			label:      "staticcheck",
			language:   "Go",
			root:       &root,
			binaries:   &["staticcheck"],
			args:       arg_list(&["-f", "json", "./..."]),
			parser:     CleanseParserKind::Staticcheck,
			executable: None,
			mutates:    false,
			optional:   staticcheck_optional,
		});
		if state.include_tests {
			add_plan(state, PlanRequest {
				id:         "go-test",
				label:      "go test",
				language:   "Go",
				root:       &root,
				binaries:   &["go"],
				args:       arg_list(&["test", "-json", "./..."]),
				parser:     CleanseParserKind::GoTest,
				executable: None,
				mutates:    false,
				optional:   false,
			});
		}
	}
	for root in roots_for_pattern(&state.files, is_golangci_config) {
		add_plan(state, PlanRequest {
			id:         "golangci",
			label:      "golangci-lint",
			language:   "Go",
			root:       &root,
			binaries:   &["golangci-lint"],
			args:       arg_list(&["run"]),
			parser:     CleanseParserKind::Golangci,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
}

fn is_golangci_config(file: &str) -> bool {
	matches!(
		basename(file),
		".golangci.yml" | ".golangci.yaml" | ".golangci.toml" | ".golangci.json"
	)
}

fn discover_python(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".py", ".pyi"]) {
		return;
	}
	let mut roots = roots_for_basenames(&state.files, &[
		".ruff.toml",
		"pyproject.toml",
		"ruff.toml",
		"setup.cfg",
		"tox.ini",
	]);
	if roots.is_empty() {
		roots.push(".".to_owned());
	}
	for root in roots {
		add_plan(state, PlanRequest {
			id:         "ruff",
			label:      "ruff",
			language:   "Python",
			root:       &root,
			binaries:   &["ruff"],
			args:       arg_list(&["check", "--output-format=json", "."]),
			parser:     CleanseParserKind::Ruff,
			executable: None,
			mutates:    false,
			optional:   false,
		});
		if state.include_tests {
			add_plan(state, PlanRequest {
				id:         "pytest",
				label:      "pytest",
				language:   "Python",
				root:       &root,
				binaries:   &["pytest"],
				args:       arg_list(&["-q"]),
				parser:     CleanseParserKind::Generic,
				executable: None,
				mutates:    false,
				optional:   false,
			});
		}
	}
	for root in roots_for_basenames(&state.files, &["pyrightconfig.json"]) {
		add_plan(state, PlanRequest {
			id:         "pyright",
			label:      "pyright",
			language:   "Python",
			root:       &root,
			binaries:   &["pyright", "basedpyright"],
			args:       arg_list(&["--outputjson"]),
			parser:     CleanseParserKind::Pyright,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	let pyproject_roots = roots_for_basenames(&state.files, &["pyproject.toml"]);
	let mut pyproject_content = HashMap::new();
	for root in &pyproject_roots {
		let file = posix_join(root, "pyproject.toml");
		let content = fs::read_to_string(state.project_cwd.join(file)).unwrap_or_default();
		pyproject_content.insert(root.clone(), content);
	}
	for root in tool_roots(
		&state.files,
		&pyproject_roots,
		&pyproject_content,
		&["mypy.ini", ".mypy.ini"],
		"[tool.mypy",
	) {
		add_plan(state, PlanRequest {
			id:         "mypy",
			label:      "mypy",
			language:   "Python",
			root:       &root,
			binaries:   &["mypy"],
			args:       arg_list(&["--no-error-summary", "--no-pretty", "."]),
			parser:     CleanseParserKind::Mypy,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	for root in tool_roots(
		&state.files,
		&pyproject_roots,
		&pyproject_content,
		&[".pylintrc", "pylintrc"],
		"[tool.pylint",
	) {
		add_plan(state, PlanRequest {
			id:         "pylint",
			label:      "pylint",
			language:   "Python",
			root:       &root,
			binaries:   &["pylint"],
			args:       arg_list(&["--output-format=json", "--recursive=y", "."]),
			parser:     CleanseParserKind::Pylint,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	for root in roots_for_basenames(&state.files, &[".flake8"]) {
		add_plan(state, PlanRequest {
			id:         "flake8",
			label:      "flake8",
			language:   "Python",
			root:       &root,
			binaries:   &["flake8"],
			args:       arg_list(&["."]),
			parser:     CleanseParserKind::Flake8,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	for root in tool_roots(
		&state.files,
		&pyproject_roots,
		&pyproject_content,
		&["ty.toml"],
		"[tool.ty",
	) {
		add_plan(state, PlanRequest {
			id:         "ty",
			label:      "ty check",
			language:   "Python",
			root:       &root,
			binaries:   &["ty"],
			args:       arg_list(&["check", "--output-format", "concise"]),
			parser:     CleanseParserKind::Ty,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
}

fn tool_roots(
	files: &[String],
	pyproject_roots: &[String],
	pyproject_content: &HashMap<String, String>,
	basenames: &[&str],
	section: &str,
) -> Vec<String> {
	let mut roots = roots_for_basenames(files, basenames);
	for root in pyproject_roots {
		if pyproject_content.get(root).is_some_and(|content| content.contains(section)) {
			roots.push(root.clone());
		}
	}
	topmost_roots(&roots)
}

fn discover_javascript(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[
		".js", ".jsx", ".mjs", ".cjs", ".ts", ".tsx", ".mts", ".cts",
	]) {
		return;
	}
	for root in roots_for_basenames(&state.files, &["biome.json", "biome.jsonc"]) {
		add_plan(state, PlanRequest {
			id:         "biome",
			label:      "biome check",
			language:   "JavaScript/TypeScript",
			root:       &root,
			binaries:   &["biome"],
			args:       arg_list(&["check", ".", "--reporter=json"]),
			parser:     CleanseParserKind::Biome,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	for root in roots_for_pattern(&state.files, is_eslint_config) {
		add_plan(state, PlanRequest {
			id:         "eslint",
			label:      "eslint",
			language:   "JavaScript/TypeScript",
			root:       &root,
			binaries:   &["eslint"],
			args:       arg_list(&[".", "--format=json"]),
			parser:     CleanseParserKind::Eslint,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	for root in roots_for_basenames(&state.files, &["tsconfig.json"]) {
		let prefix = if root == "." { String::new() } else { format!("{root}/") };
		let has_vue = state
			.files
			.iter()
			.any(|file| file.starts_with(prefix.as_str()) && file.ends_with(".vue"));
		let binaries: &[&str] = if has_vue {
			&["vue-tsc", "tsgo", "tsc"]
		} else {
			&["tsgo", "tsc"]
		};
		add_plan(state, PlanRequest {
			id:         "typescript",
			label:      if has_vue { "Vue TypeScript" } else { "TypeScript" },
			language:   "TypeScript",
			root:       &root,
			binaries,
			args:       arg_list(&["--noEmit", "--pretty", "false"]),
			parser:     CleanseParserKind::Generic,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	for root in roots_for_basenames(&state.files, &[".oxlintrc.json"]) {
		add_plan(state, PlanRequest {
			id:         "oxlint",
			label:      "oxlint",
			language:   "JavaScript/TypeScript",
			root:       &root,
			binaries:   &["oxlint"],
			args:       arg_list(&["--format=unix"]),
			parser:     CleanseParserKind::Oxlint,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	for root in roots_for_basenames(&state.files, &["deno.json", "deno.jsonc"]) {
		add_plan(state, PlanRequest {
			id:         "deno-lint",
			label:      "deno lint",
			language:   "JavaScript/TypeScript",
			root:       &root,
			binaries:   &["deno"],
			args:       arg_list(&["lint", "--json"]),
			parser:     CleanseParserKind::DenoLint,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	for root in roots_for_pattern(&state.files, is_stylelint_config) {
		add_plan(state, PlanRequest {
			id:         "stylelint",
			label:      "stylelint",
			language:   "CSS",
			root:       &root,
			binaries:   &["stylelint"],
			args:       arg_list(&[
				"**/*.{css,scss,sass,less}",
				"--formatter",
				"json",
				"--allow-empty-input",
			]),
			parser:     CleanseParserKind::Stylelint,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	if state.include_tests {
		discover_package_tests(state);
	}
}

fn is_eslint_config(file: &str) -> bool {
	matches!(
		basename(file),
		"eslint.config.js"
			| "eslint.config.mjs"
			| "eslint.config.cjs"
			| "eslint.config.ts"
			| "eslint.config.mts"
			| "eslint.config.cts"
			| ".eslintrc"
			| ".eslintrc.js"
			| ".eslintrc.cjs"
			| ".eslintrc.json"
			| ".eslintrc.yaml"
			| ".eslintrc.yml"
	)
}

fn is_stylelint_config(file: &str) -> bool {
	matches!(
		basename(file),
		".stylelintrc"
			| ".stylelintrc.json"
			| ".stylelintrc.yaml"
			| ".stylelintrc.yml"
			| ".stylelintrc.js"
			| ".stylelintrc.cjs"
			| ".stylelintrc.mjs"
			| "stylelint.config.js"
			| "stylelint.config.cjs"
			| "stylelint.config.mjs"
			| "stylelint.config.ts"
	)
}

fn discover_package_tests(state: &mut DiscoveryState) {
	let mut candidates = Vec::new();
	for file in &state.files {
		if basename(file) != "package.json" {
			continue;
		}
		let Ok(text) = fs::read_to_string(state.project_cwd.join(file)) else {
			continue;
		};
		let Ok(Value::Object(manifest)) = serde_json::from_str::<Value>(&text) else {
			continue;
		};
		let Some(Value::Object(scripts)) = manifest.get("scripts") else {
			continue;
		};
		let Some(Value::String(test_script)) = scripts.get("test") else {
			continue;
		};
		if test_script.trim().is_empty()
			|| test_script.to_lowercase().contains("no test specified")
		{
			continue;
		}
		let root = posix_dirname(file).to_owned();
		let prefix = if root == "." { String::new() } else { format!("{root}/") };
		let mut manager = manifest
			.get("packageManager")
			.and_then(Value::as_str)
			.map(|value| {
				value.split_once('@').map_or(value, |(name, _version)| name).to_lowercase()
			})
			.unwrap_or_default();
		if !matches!(manager.as_str(), "bun" | "npm" | "pnpm" | "yarn") {
			manager = if contains_file(&state.files, &format!("{prefix}bun.lock"))
				|| contains_file(&state.files, &format!("{prefix}bun.lockb"))
			{
				"bun".to_owned()
			} else if contains_file(&state.files, &format!("{prefix}pnpm-lock.yaml")) {
				"pnpm".to_owned()
			} else if contains_file(&state.files, &format!("{prefix}yarn.lock")) {
				"yarn".to_owned()
			} else {
				"npm".to_owned()
			};
		}
		candidates.push((root, manager));
	}
	let selected =
		topmost_roots(&candidates.iter().map(|(root, _)| root.clone()).collect::<Vec<_>>());
	let selected: HashSet<&str> = selected.iter().map(String::as_str).collect();
	for (root, manager) in &candidates {
		if !selected.contains(root.as_str()) {
			continue;
		}
		let id = format!("{manager}-test");
		let label = format!("{manager} test");
		add_plan(state, PlanRequest {
			id:         &id,
			label:      &label,
			language:   "JavaScript/TypeScript",
			root,
			binaries:   &[manager.as_str()],
			args:       arg_list(&["run", "test"]),
			parser:     CleanseParserKind::Generic,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
}

fn discover_ruby(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".rb", ".rake"]) {
		return;
	}
	for root in roots_for_basenames(&state.files, &[".rubocop.yml", ".rubocop.yaml"]) {
		add_plan(state, PlanRequest {
			id:         "rubocop",
			label:      "rubocop",
			language:   "Ruby",
			root:       &root,
			binaries:   &["rubocop"],
			args:       arg_list(&["--format", "json", "--force-exclusion"]),
			parser:     CleanseParserKind::Rubocop,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	if state.include_tests {
		for root in roots_for_basenames(&state.files, &[".rspec", "Gemfile"]) {
			add_plan(state, PlanRequest {
				id:         "rspec",
				label:      "RSpec",
				language:   "Ruby",
				root:       &root,
				binaries:   &["rspec"],
				args:       arg_list(&["--format", "progress"]),
				parser:     CleanseParserKind::Generic,
				executable: None,
				mutates:    false,
				optional:   false,
			});
		}
	}
}

fn discover_php(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".php"]) {
		return;
	}
	for root in roots_for_pattern(&state.files, is_phpstan_config) {
		add_plan(state, PlanRequest {
			id:         "phpstan",
			label:      "PHPStan",
			language:   "PHP",
			root:       &root,
			binaries:   &["phpstan"],
			args:       arg_list(&["analyse", "--error-format=json", "--no-progress"]),
			parser:     CleanseParserKind::Phpstan,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	for root in roots_for_pattern(&state.files, is_psalm_config) {
		add_plan(state, PlanRequest {
			id:         "psalm",
			label:      "Psalm",
			language:   "PHP",
			root:       &root,
			binaries:   &["psalm"],
			args:       arg_list(&["--output-format=json", "--no-progress"]),
			parser:     CleanseParserKind::Psalm,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	if state.include_tests {
		for root in roots_for_pattern(&state.files, is_phpunit_config) {
			add_plan(state, PlanRequest {
				id:         "phpunit",
				label:      "PHPUnit",
				language:   "PHP",
				root:       &root,
				binaries:   &["phpunit"],
				args:       Vec::new(),
				parser:     CleanseParserKind::Generic,
				executable: None,
				mutates:    false,
				optional:   false,
			});
		}
	}
}

fn is_phpstan_config(file: &str) -> bool {
	matches!(basename(file), "phpstan.neon" | "phpstan.neon.dist")
}

fn is_psalm_config(file: &str) -> bool {
	matches!(basename(file), "psalm.xml" | "psalm.xml.dist")
}

fn is_phpunit_config(file: &str) -> bool {
	matches!(basename(file), "phpunit.xml" | "phpunit.xml.dist")
}

fn discover_swift(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".swift"]) {
		return;
	}
	let configured = roots_for_basenames(&state.files, &[".swiftlint.yml"]);
	let roots = if configured.is_empty() {
		roots_for_basenames(&state.files, &["Package.swift"])
	} else {
		configured
	};
	for root in roots {
		add_plan(state, PlanRequest {
			id:         "swiftlint",
			label:      "SwiftLint",
			language:   "Swift",
			root:       &root,
			binaries:   &["swiftlint"],
			args:       arg_list(&["lint", "--reporter", "json", "--quiet"]),
			parser:     CleanseParserKind::Swiftlint,
			executable: None,
			mutates:    false,
			optional:   false,
		});
		if state.include_tests {
			add_plan(state, PlanRequest {
				id:         "swift-test",
				label:      "swift test",
				language:   "Swift",
				root:       &root,
				binaries:   &["swift"],
				args:       arg_list(&["test"]),
				parser:     CleanseParserKind::Generic,
				executable: None,
				mutates:    false,
				optional:   false,
			});
		}
	}
}

fn discover_dart(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".dart"]) {
		return;
	}
	for root in roots_for_basenames(&state.files, &["pubspec.yaml"]) {
		add_plan(state, PlanRequest {
			id:         "dart-analyze",
			label:      "dart analyze",
			language:   "Dart",
			root:       &root,
			binaries:   &["dart"],
			args:       arg_list(&["analyze", "--format", "machine"]),
			parser:     CleanseParserKind::Dart,
			executable: None,
			mutates:    false,
			optional:   false,
		});
		if state.include_tests {
			add_plan(state, PlanRequest {
				id:         "dart-test",
				label:      "dart test",
				language:   "Dart",
				root:       &root,
				binaries:   &["dart"],
				args:       arg_list(&["test", "--reporter", "json"]),
				parser:     CleanseParserKind::Generic,
				executable: None,
				mutates:    false,
				optional:   false,
			});
		}
	}
}

fn discover_elixir(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".ex", ".exs"]) {
		return;
	}
	for root in roots_for_basenames(&state.files, &[".credo.exs"]) {
		add_plan(state, PlanRequest {
			id:         "credo",
			label:      "Credo",
			language:   "Elixir",
			root:       &root,
			binaries:   &["mix"],
			args:       arg_list(&["credo", "--format=json"]),
			parser:     CleanseParserKind::Credo,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	if state.include_tests {
		for root in roots_for_basenames(&state.files, &["mix.exs"]) {
			add_plan(state, PlanRequest {
				id:         "mix-test",
				label:      "mix test",
				language:   "Elixir",
				root:       &root,
				binaries:   &["mix"],
				args:       arg_list(&["test"]),
				parser:     CleanseParserKind::Generic,
				executable: None,
				mutates:    false,
				optional:   false,
			});
		}
	}
}

fn discover_shell(state: &mut DiscoveryState) {
	let shell_files: Vec<String> = state
		.files
		.iter()
		.filter(|file| {
			let lower = file.to_ascii_lowercase();
			[".sh", ".bash", ".ksh", ".zsh"]
				.iter()
				.any(|extension| lower.ends_with(*extension))
		})
		.cloned()
		.collect();
	if shell_files.is_empty() {
		return;
	}
	for (batch, files) in shell_files.chunks(SHELLCHECK_BATCH_SIZE).enumerate() {
		let id = format!("shellcheck-{}", batch + 1);
		let mut args = vec!["--format=json1".to_owned()];
		args.extend(files.iter().cloned());
		add_plan(state, PlanRequest {
			id:         &id,
			label:      "ShellCheck",
			language:   "Shell",
			root:       ".",
			binaries:   &["shellcheck"],
			args,
			parser:     CleanseParserKind::Shellcheck,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
}

fn discover_haskell(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".hs", ".lhs"]) {
		return;
	}
	let mut roots = roots_for_pattern(&state.files, is_haskell_marker);
	if roots.is_empty() {
		roots.push(".".to_owned());
	}
	for root in roots {
		add_plan(state, PlanRequest {
			id:         "hlint",
			label:      "HLint",
			language:   "Haskell",
			root:       &root,
			binaries:   &["hlint"],
			args:       arg_list(&["--json", "."]),
			parser:     CleanseParserKind::Hlint,
			executable: None,
			mutates:    false,
			optional:   false,
		});
		if state.include_tests {
			let use_stack = contains_file(&state.files, &posix_join(&root, "stack.yaml"));
			let binaries: &[&str] = if use_stack { &["stack"] } else { &["cabal"] };
			add_plan(state, PlanRequest {
				id:         if use_stack { "stack-test" } else { "cabal-test" },
				label:      if use_stack { "stack test" } else { "cabal test" },
				language:   "Haskell",
				root:       &root,
				binaries,
				args:       if use_stack {
					arg_list(&["test"])
				} else {
					arg_list(&["test", "all"])
				},
				parser:     CleanseParserKind::Generic,
				executable: None,
				mutates:    false,
				optional:   false,
			});
		}
	}
}

fn is_haskell_marker(file: &str) -> bool {
	let name = basename(file);
	matches!(name, ".hlint.yaml" | "stack.yaml" | "cabal.project")
		|| (name.ends_with(".cabal") && name.len() > ".cabal".len())
}

fn discover_terraform(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".tf"]) {
		return;
	}
	let configured = roots_for_basenames(&state.files, &[".tflint.hcl"]);
	let roots = if configured.is_empty() {
		roots_for_pattern(&state.files, |file| file.ends_with(".tf"))
	} else {
		configured
	};
	for root in roots {
		add_plan(state, PlanRequest {
			id:         "tflint",
			label:      "TFLint",
			language:   "Terraform",
			root:       &root,
			binaries:   &["tflint"],
			args:       arg_list(&["--format=json", "--recursive"]),
			parser:     CleanseParserKind::Tflint,
			executable: None,
			mutates:    false,
			optional:   false,
		});
		add_plan(state, PlanRequest {
			id:         "terraform-validate",
			label:      "terraform validate",
			language:   "Terraform",
			root:       &root,
			binaries:   &["terraform"],
			args:       arg_list(&["validate", "-json"]),
			parser:     CleanseParserKind::Terraform,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
}

fn discover_lua(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".lua"]) {
		return;
	}
	let configured = roots_for_basenames(&state.files, &[".luacheckrc"]);
	for root in if configured.is_empty() { vec![".".to_owned()] } else { configured } {
		add_plan(state, PlanRequest {
			id:         "luacheck",
			label:      "Luacheck",
			language:   "Lua",
			root:       &root,
			binaries:   &["luacheck"],
			args:       arg_list(&["."]),
			parser:     CleanseParserKind::Generic,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
	if state.include_tests && state.files.iter().any(|file| is_lua_spec(file)) {
		add_plan(state, PlanRequest {
			id:         "busted",
			label:      "Busted",
			language:   "Lua",
			root:       ".",
			binaries:   &["busted"],
			args:       Vec::new(),
			parser:     CleanseParserKind::Generic,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
}

fn is_lua_spec(file: &str) -> bool {
	file.ends_with("_spec.lua")
		&& (file.starts_with("spec/")
			|| file.starts_with("test/")
			|| file.contains("/spec/")
			|| file.contains("/test/"))
}

fn discover_cpp(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[
		".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx",
	]) {
		return;
	}
	for root in roots_for_basenames(&state.files, &["compile_commands.json"]) {
		let compile_database = state.project_cwd.join(&root).to_string_lossy().into_owned();
		add_plan(state, PlanRequest {
			id:         "clang-tidy",
			label:      "clang-tidy",
			language:   "C/C++",
			root:       ".",
			binaries:   &["run-clang-tidy", "run-clang-tidy.py"],
			args:       vec!["-quiet".to_owned(), "-p".to_owned(), compile_database],
			parser:     CleanseParserKind::Generic,
			executable: None,
			mutates:    false,
			optional:   false,
		});
	}
}

fn discover_dotnet(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".cs", ".fs", ".vb"]) {
		return;
	}
	let solutions = roots_for_pattern(&state.files, |file| {
		lower_ends_with(file, ".sln") || lower_ends_with(file, ".slnx")
	});
	let roots = if solutions.is_empty() {
		roots_for_pattern(&state.files, |file| {
			lower_ends_with(file, ".csproj")
				|| lower_ends_with(file, ".fsproj")
				|| lower_ends_with(file, ".vbproj")
		})
	} else {
		solutions
	};
	for root in roots {
		add_plan(state, PlanRequest {
			id:         "dotnet-build",
			label:      "dotnet build",
			language:   ".NET",
			root:       &root,
			binaries:   &["dotnet"],
			args:       arg_list(&[
				"build",
				"--no-restore",
				"--nologo",
				"--verbosity",
				"quiet",
			]),
			parser:     CleanseParserKind::Generic,
			executable: None,
			mutates:    false,
			optional:   false,
		});
		if state.include_tests {
			add_plan(state, PlanRequest {
				id:         "dotnet-test",
				label:      "dotnet test",
				language:   ".NET",
				root:       &root,
				binaries:   &["dotnet"],
				args:       arg_list(&[
					"test",
					"--no-restore",
					"--nologo",
					"--verbosity",
					"quiet",
				]),
				parser:     CleanseParserKind::Generic,
				executable: None,
				mutates:    false,
				optional:   false,
			});
		}
	}
}

fn discover_zig(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".zig"]) {
		return;
	}
	for root in roots_for_basenames(&state.files, &["build.zig"]) {
		add_plan(state, PlanRequest {
			id:         "zig-build",
			label:      "zig build",
			language:   "Zig",
			root:       &root,
			binaries:   &["zig"],
			args:       arg_list(&["build"]),
			parser:     CleanseParserKind::Generic,
			executable: None,
			mutates:    false,
			optional:   false,
		});
		if state.include_tests {
			add_plan(state, PlanRequest {
				id:         "zig-test",
				label:      "zig build test",
				language:   "Zig",
				root:       &root,
				binaries:   &["zig"],
				args:       arg_list(&["build", "test"]),
				parser:     CleanseParserKind::Generic,
				executable: None,
				mutates:    false,
				optional:   false,
			});
		}
	}
}

fn discover_jvm(state: &mut DiscoveryState) {
	if !contains_extension(&state.files, &[".java", ".kt", ".kts", ".scala"]) {
		return;
	}
	for root in roots_for_basenames(&state.files, &[
		"build.gradle",
		"build.gradle.kts",
		"settings.gradle",
		"settings.gradle.kts",
	]) {
		let wrapper = if contains_file(&state.files, &posix_join(&root, "gradlew")) {
			Some(state.project_cwd.join(&root).join("gradlew"))
		} else if contains_file(&state.files, &posix_join(&root, "gradlew.bat")) {
			Some(state.project_cwd.join(&root).join("gradlew.bat"))
		} else {
			None
		}
		.filter(|wrapper| is_executable_file(wrapper));
		add_plan(state, PlanRequest {
			id:         "gradle-check",
			label:      "Gradle check",
			language:   "JVM",
			root:       &root,
			binaries:   &["gradle"],
			args:       arg_list(&[
				"check",
				"-x",
				"test",
				"--console=plain",
				"--no-daemon",
			]),
			parser:     CleanseParserKind::Generic,
			executable: wrapper.clone(),
			mutates:    false,
			optional:   false,
		});
		if state.include_tests {
			add_plan(state, PlanRequest {
				id:         "gradle-test",
				label:      "Gradle test",
				language:   "JVM",
				root:       &root,
				binaries:   &["gradle"],
				args:       arg_list(&["test", "--console=plain", "--no-daemon"]),
				parser:     CleanseParserKind::Generic,
				executable: wrapper,
				mutates:    false,
				optional:   false,
			});
		}
	}
	for root in roots_for_basenames(&state.files, &["pom.xml"]) {
		let wrapper = if contains_file(&state.files, &posix_join(&root, "mvnw")) {
			Some(state.project_cwd.join(&root).join("mvnw"))
		} else if contains_file(&state.files, &posix_join(&root, "mvnw.cmd")) {
			Some(state.project_cwd.join(&root).join("mvnw.cmd"))
		} else {
			None
		}
		.filter(|wrapper| is_executable_file(wrapper));
		add_plan(state, PlanRequest {
			id:         "maven-verify",
			label:      "Maven verify",
			language:   "JVM",
			root:       &root,
			binaries:   &["mvn"],
			args:       arg_list(&["-q", "-DskipTests", "-DskipITs", "verify"]),
			parser:     CleanseParserKind::Generic,
			executable: wrapper.clone(),
			mutates:    false,
			optional:   false,
		});
		if state.include_tests {
			add_plan(state, PlanRequest {
				id:         "maven-test",
				label:      "Maven test",
				language:   "JVM",
				root:       &root,
				binaries:   &["mvn"],
				args:       arg_list(&["-q", "test"]),
				parser:     CleanseParserKind::Generic,
				executable: wrapper,
				mutates:    false,
				optional:   false,
			});
		}
	}
}

fn discover_github_actions(state: &mut DiscoveryState) {
	let mut roots = Vec::new();
	for file in &state.files {
		if let Some(root) = workflow_root(file) {
			roots.push(root.to_owned());
		}
	}
	for root in topmost_roots(&roots) {
		add_plan(state, PlanRequest {
			id:         "actionlint",
			label:      "actionlint",
			language:   "GitHub Actions",
			root:       &root,
			binaries:   &["actionlint"],
			args:       arg_list(&["-format", "{{json .}}"]),
			parser:     CleanseParserKind::Actionlint,
			executable: None,
			mutates:    false,
			optional:   true,
		});
	}
}

fn workflow_root(file: &str) -> Option<&str> {
	if let Some(rest) = file.strip_prefix(".github/workflows/") {
		return is_workflow_file(rest).then_some(".");
	}
	let marker = "/.github/workflows/";
	let index = file.find(marker)?;
	let rest = &file[index + marker.len()..];
	is_workflow_file(rest).then_some(&file[..index])
}

fn is_workflow_file(name: &str) -> bool {
	!name.is_empty()
		&& !name.contains('/')
		&& (name.ends_with(".yml") || name.ends_with(".yaml"))
}

async fn read_capped<R>(reader: &mut R, limit: usize) -> std::io::Result<Vec<u8>>
where
	R: AsyncRead + Unpin,
{
	let mut output = Vec::with_capacity(limit.min(8 * 1024));
	let mut buffer = [0_u8; 8 * 1024];
	loop {
		let read = reader.read(&mut buffer).await?;
		if read == 0 {
			break;
		}
		let remaining = limit.saturating_sub(output.len());
		if remaining > 0 {
			output.extend_from_slice(&buffer[..read.min(remaining)]);
		}
	}
	Ok(output)
}

async fn run_checker(
	plan: CheckerPlan,
	project_cwd: PathBuf,
	allowed_files: Arc<HashSet<String>>,
) -> CleanseCheckResult {
	let mut command = ProcessCommand::new(&plan.executable);
	command
		.args(&plan.args)
		.current_dir(&plan.cwd)
		.stdin(Stdio::null())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.kill_on_drop(true);
	let (exit_code, stdout, stderr, spawn_error) = match command.spawn() {
		Err(error) => (None, String::new(), String::new(), Some(error.to_string())),
		Ok(mut child) => {
			let mut child_stdout = child.stdout.take().expect("checker stdout is piped");
			let mut child_stderr = child.stderr.take().expect("checker stderr is piped");
			let execution = async {
				let (stdout, stderr, status) = tokio::join!(
					read_capped(&mut child_stdout, MAX_CAPTURE_BYTES),
					read_capped(&mut child_stderr, MAX_CAPTURE_BYTES),
					child.wait(),
				);
				let stdout = stdout?;
				let stderr = stderr?;
				let status = status?;
				Ok::<_, std::io::Error>((status.code(), stdout, stderr))
			};
			let result = timeout(CHECKER_TIMEOUT, execution).await;
			match result {
				Ok(Ok((exit_code, stdout, stderr))) => (
					exit_code,
					String::from_utf8_lossy(&stdout).into_owned(),
					String::from_utf8_lossy(&stderr).into_owned(),
					None,
				),
				Ok(Err(error)) => (
					None,
					String::new(),
					String::new(),
					Some(format!("checker I/O failed: {error}")),
				),
				Err(_) => {
					let _ = child.kill().await;
					let _ = child.wait().await;
					(
						None,
						String::new(),
						String::new(),
						Some(format!(
							"checker timed out after {}s",
							CHECKER_TIMEOUT.as_secs()
						)),
					)
				},
			}
		},
	};
	let mut diagnostics = Vec::new();
	if let Some(error) = spawn_error {
		diagnostics.push(checker_failure_diagnostic(&plan, None, "", &error));
	} else {
		let parsed = parse_cleanse_diagnostics(plan.parser, &CleanseParserInput {
			checker:     &plan.label,
			project_cwd: &project_cwd,
			checker_cwd: &plan.cwd,
			stdout:      &stdout,
			stderr:      &stderr,
		});
		diagnostics = parsed
			.into_iter()
			.filter(|diagnostic| {
				diagnostic
					.file
					.as_ref()
					.is_none_or(|file| allowed_files.contains(file))
			})
			.collect();
		if exit_code != Some(0) && diagnostics.is_empty() {
			diagnostics.push(checker_failure_diagnostic(
				&plan,
				exit_code,
				&stdout,
				&stderr,
			));
		}
	}
	CleanseCheckResult {
		id: plan.id,
		label: plan.label,
		language: plan.language,
		cwd: project_relative(&project_cwd, &plan.cwd),
		command: plan.command,
		exit_code,
		diagnostics,
	}
}

fn discovery_failure_diagnostic(checker: String, message: String) -> CleanseDiagnostic {
	CleanseDiagnostic {
		checker,
		file: None,
		line: None,
		column: None,
		end_line: None,
		end_column: None,
		code: None,
		severity: CleanseSeverity::Error,
		message,
		suggestion: None,
	}
}

fn checker_failure_diagnostic(
	plan: &CheckerPlan,
	exit_code: Option<i32>,
	stdout: &str,
	stderr: &str,
) -> CleanseDiagnostic {
	let combined = match (stderr.is_empty(), stdout.is_empty()) {
		(false, false) => format!("{stderr}\n{stdout}"),
		(false, true) => stderr.to_owned(),
		(true, false) => stdout.to_owned(),
		(true, true) => String::new(),
	};
	let output = sanitize_text(&combined).trim().to_owned();
	let suffix = if output.is_empty() {
		String::new()
	} else {
		format!(": {}", truncate_bytes(&output, MAX_FAILURE_OUTPUT))
	};
	CleanseDiagnostic {
		checker:    plan.label.clone(),
		file:       None,
		line:       None,
		column:     None,
		end_line:   None,
		end_column: None,
		code:       None,
		severity:   CleanseSeverity::Error,
		message:    format!(
			"{} failed{}{suffix}",
			plan.label,
			exit_code.map_or_else(String::new, |code| format!(" with exit code {code}"))
		),
		suggestion: None,
	}
}

fn truncate_bytes(text: &str, max: usize) -> &str {
	if text.len() <= max {
		return text;
	}
	let mut end = max;
	while !text.is_char_boundary(end) {
		end -= 1;
	}
	&text[..end]
}

fn internal_failure(
	descriptor: CleanseCheckerDescriptor,
	message: String,
) -> CleanseCheckResult {
	let diagnostic = CleanseDiagnostic {
		checker:    descriptor.label.clone(),
		file:       None,
		line:       None,
		column:     None,
		end_line:   None,
		end_column: None,
		code:       None,
		severity:   CleanseSeverity::Error,
		message,
		suggestion: None,
	};
	CleanseCheckResult {
		id: descriptor.id,
		label: descriptor.label,
		language: descriptor.language,
		cwd: descriptor.cwd,
		command: descriptor.command,
		exit_code: None,
		diagnostics: vec![diagnostic],
	}
}

fn describe(project_cwd: &Path, plan: &CheckerPlan) -> CleanseCheckerDescriptor {
	CleanseCheckerDescriptor {
		id:       plan.id.clone(),
		label:    plan.label.clone(),
		language: plan.language.clone(),
		cwd:      project_relative(project_cwd, &plan.cwd),
		command:  plan.command.clone(),
	}
}

fn project_relative(project_cwd: &Path, path: &Path) -> String {
	let relative = path.strip_prefix(project_cwd).unwrap_or(Path::new(""));
	let relative = relative.to_string_lossy().replace('\\', "/");
	if relative.is_empty() { ".".to_owned() } else { relative }
}

fn deduplicate_project_diagnostics(
	diagnostics: Vec<CleanseDiagnostic>,
) -> Vec<CleanseDiagnostic> {
	let mut seen = HashSet::new();
	let mut unique = Vec::new();
	for diagnostic in diagnostics {
		if seen.insert(diagnostic_key(&diagnostic)) {
			unique.push(diagnostic);
		}
	}
	unique.sort_by(|left, right| {
		left.file
			.as_deref()
			.unwrap_or("")
			.cmp(right.file.as_deref().unwrap_or(""))
			.then_with(|| left.line.unwrap_or(0).cmp(&right.line.unwrap_or(0)))
			.then_with(|| left.message.cmp(&right.message))
	});
	unique
}

fn collapse_whitespace(text: &str) -> String {
	text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
	use std::{fs, path::PathBuf, sync::Arc};

	use super::{
		CARGO_METADATA_CAPTURE_BYTES, CheckerPlan, CleanseDiagnostic, CleanseDiagnosticSuite,
		CleanseDiagnosticSuiteOptions, CleanseParserKind, CleanseSeverity,
		CustomCleanseCheckerSpec, DiscoveryFailure, DiscoveryState, MAX_FAILURE_OUTPUT,
		PlanRequest, add_plan, build_custom_cleanse_suite, checker_failure_diagnostic,
		discover_blocking, discovery_failure_diagnostic, deduplicate_project_diagnostics,
		list_project_files, run_checker, topmost_roots,
	};

	fn temp_project(name: &str) -> PathBuf {
		let path = std::env::temp_dir().join(format!(
			"arag-check-{name}-{}",
			std::process::id()
		));
		let _ = fs::remove_dir_all(&path);
		fs::create_dir_all(&path).expect("temporary project");
		path
	}

	#[test]
	fn walks_project_files_and_ignores_dependency_directories() {
		let root = temp_project("walk");
		fs::create_dir_all(root.join("src")).expect("create src");
		fs::create_dir_all(root.join("node_modules/pkg")).expect("create node_modules");
		fs::create_dir_all(root.join("target/debug")).expect("create target");
		fs::create_dir_all(root.join(".github/workflows")).expect("create workflows");
		fs::write(root.join("src/main.rs"), "").expect("write source");
		fs::write(root.join("Cargo.toml"), "").expect("write manifest");
		fs::write(root.join("node_modules/pkg/index.js"), "").expect("write dependency");
		fs::write(root.join("target/debug/out"), "").expect("write target");
		fs::write(root.join(".github/workflows/ci.yml"), "").expect("write workflow");
		let files = list_project_files(&root);
		assert_eq!(
			files,
			vec![
				".github/workflows/ci.yml".to_owned(),
				"Cargo.toml".to_owned(),
				"src/main.rs".to_owned(),
			]
		);
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn keeps_only_topmost_marker_roots() {
		let roots = topmost_roots(&[
			"a/b".to_owned(),
			"a".to_owned(),
			"c/d".to_owned(),
			"c".to_owned(),
		]);
		assert_eq!(roots, vec!["a".to_owned(), "c".to_owned()]);
		let roots = topmost_roots(&["a/b".to_owned(), ".".to_owned()]);
		assert_eq!(roots, vec![".".to_owned()]);
	}

	#[test]
	fn caps_failure_output() {
		let plan = CheckerPlan {
			id:         "checker:.".to_owned(),
			label:      "checker".to_owned(),
			language:   "C".to_owned(),
			cwd:        PathBuf::from("."),
			executable: PathBuf::from("true"),
			args:       Vec::new(),
			parser:     CleanseParserKind::Generic,
			command:    "true".to_owned(),
			mutates:    false,
		};
		let stderr = "x".repeat(MAX_FAILURE_OUTPUT * 2);
		let diagnostic = checker_failure_diagnostic(&plan, Some(2), "", &stderr);
		assert_eq!(diagnostic.severity, CleanseSeverity::Error);
		assert!(diagnostic.message.starts_with("checker failed with exit code 2: xxx"));
		assert!(diagnostic.message.len() <= "checker failed with exit code 2: ".len() + MAX_FAILURE_OUTPUT);
	}

	#[test]
	fn deduplicates_and_sorts_project_diagnostics() {
		let diagnostic = |file: &str, line: u64, message: &str| CleanseDiagnostic {
			checker:    "checker".to_owned(),
			file:       Some(file.to_owned()),
			line:       Some(line),
			column:     None,
			end_line:   None,
			end_column: None,
			code:       None,
			severity:   CleanseSeverity::Warning,
			message:    message.to_owned(),
			suggestion: None,
		};
		let diagnostics = deduplicate_project_diagnostics(vec![
			diagnostic("b.rs", 2, "z"),
			diagnostic("a.rs", 3, "y"),
			diagnostic("b.rs", 2, "z"),
		]);
		assert_eq!(diagnostics.len(), 2);
		assert_eq!(diagnostics[0].file.as_deref(), Some("a.rs"));
		assert_eq!(diagnostics[1].file.as_deref(), Some("b.rs"));
	}

	#[test]
	fn deduplicates_the_same_diagnostic_across_checkers() {
		let make = |checker: &str| CleanseDiagnostic {
			checker:    checker.to_owned(),
			file:       Some("src/a.rs".to_owned()),
			line:       Some(4),
			column:     Some(2),
			end_line:   Some(4),
			end_column: Some(6),
			code:       Some("E001".to_owned()),
			severity:   CleanseSeverity::Warning,
			message:    "same diagnostic".to_owned(),
			suggestion: None,
		};
		let diagnostics = deduplicate_project_diagnostics(vec![make("first"), make("second")]);
		assert_eq!(diagnostics.len(), 1);
		assert_eq!(diagnostics[0].checker, "first");
	}

	#[test]
	fn selects_checkers_by_kind_prefix_or_exact_id() {
		let root = temp_project("select");
		let executable = std::env::current_exe().expect("current executable");
		let mut state = DiscoveryState {
			project_cwd:   root.clone(),
			files:         Vec::new(),
			plans:         Vec::new(),
			skipped:       Vec::new(),
			include_tests: false,
		};
		for (id, label) in [("clippy", "cargo clippy"), ("go-vet", "go vet")] {
			add_plan(&mut state, PlanRequest {
				id,
				label,
				language:   "Test",
				root:       ".",
				binaries:   &[],
				args:       Vec::new(),
				parser:     CleanseParserKind::Generic,
				executable: Some(executable.clone()),
				mutates:    false,
				optional:   false,
			});
		}
		let mut suite = CleanseDiagnosticSuite::new(
			state.project_cwd,
			state.plans,
			state.skipped,
			Vec::new(),
		);
		assert!(suite.select(&["clippy"]).is_ok());
		assert_eq!(suite.selected().len(), 1);
		assert_eq!(suite.selected()[0].id, "clippy:.");
		assert!(suite.select(&["clippy:."]).is_ok());
		assert_eq!(suite.selected()[0].id, "clippy:.");
		assert!(suite.select(&["clippy", "clippy"]).is_ok());
		assert_eq!(suite.selected().len(), 1);
		assert!(suite.select(&["clippy", "clippy:."]).is_ok());
		assert_eq!(suite.selected().len(), 1);
		let error = suite.select(&["no-such-checker"]).expect_err("unknown checker");
		assert!(error.contains("unknown checker id"));
		assert!(suite.selected().is_empty());
		assert!(suite.select(&["clippy", "no-such-checker"]).is_err());
		let _ = fs::remove_dir_all(root);
	}

	#[tokio::test]
	async fn selects_discovery_failure_by_checker_id() {
		let root = temp_project("select-failure");
		let diagnostic = discovery_failure_diagnostic(
			"cargo clippy (.)".to_owned(),
			"cargo metadata failed: boom".to_owned(),
		);
		let mut suite = CleanseDiagnosticSuite::new(
			root.clone(),
			Vec::new(),
			Vec::new(),
			Vec::new(),
		)
		.with_discovery_failures(vec![DiscoveryFailure {
			checker_id: "clippy:.".to_owned(),
			diagnostic,
		}]);
		assert!(suite.select(&["clippy"]).is_ok());
		let report = suite.run().await;
		assert!(report.checks.is_empty());
		assert_eq!(report.diagnostics.len(), 1);
		assert!(report.diagnostics[0].message.contains("cargo metadata failed"));
		assert!(suite.select(&["clippy:."]).is_ok());
		let error = suite.select(&["no-such-checker"]).expect_err("unknown checker");
		assert!(error.contains("clippy:."), "{error}");
		assert!(suite.active_failures.is_empty());
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn select_omits_unselected_discovery_failures() {
		let root = temp_project("select-mixed-failure");
		let executable = std::env::current_exe().expect("current executable");
		let mut state = DiscoveryState {
			project_cwd:   root.clone(),
			files:         Vec::new(),
			plans:         Vec::new(),
			skipped:       Vec::new(),
			include_tests: false,
		};
		add_plan(&mut state, PlanRequest {
			id:         "eslint",
			label:      "eslint",
			language:   "JavaScript",
			root:       ".",
			binaries:   &[],
			args:       Vec::new(),
			parser:     CleanseParserKind::Generic,
			executable: Some(executable),
			mutates:    false,
			optional:   false,
		});
		let diagnostic = discovery_failure_diagnostic(
			"cargo clippy (.)".to_owned(),
			"cargo metadata failed: boom".to_owned(),
		);
		let mut suite = CleanseDiagnosticSuite::new(
			state.project_cwd,
			state.plans,
			state.skipped,
			Vec::new(),
		)
		.with_discovery_failures(vec![DiscoveryFailure {
			checker_id: "clippy:.".to_owned(),
			diagnostic,
		}]);
		assert!(suite.select(&["eslint"]).is_ok());
		assert_eq!(suite.selected().len(), 1);
		assert_eq!(suite.selected()[0].id, "eslint:.");
		assert!(suite.active_failures.is_empty());
		assert!(suite.select(&["clippy"]).is_ok());
		assert!(suite.selected().is_empty());
		assert_eq!(suite.active_failures.len(), 1);
		let _ = fs::remove_dir_all(root);
	}

	#[test]
	fn skips_missing_checker_binaries() {
		let root = temp_project("missing-binary");
		let mut state = DiscoveryState {
			project_cwd:   root.clone(),
			files:         Vec::new(),
			plans:         Vec::new(),
			skipped:       Vec::new(),
			include_tests: false,
		};
		add_plan(&mut state, PlanRequest {
			id:         "missing",
			label:      "Missing",
			language:   "Test",
			root:       ".",
			binaries:   &["./arag-check-missing-binary"],
			args:       Vec::new(),
			parser:     CleanseParserKind::Generic,
			executable: None,
			mutates:    false,
			optional:   false,
		});
		assert!(state.plans.is_empty());
		assert_eq!(state.skipped.len(), 1);
		assert_eq!(state.skipped[0].label, "Missing (.)");
		assert_eq!(
			state.skipped[0].reason,
			"executable not found: ./arag-check-missing-binary"
		);
		let _ = fs::remove_dir_all(root);
	}

	#[tokio::test]
	async fn skips_unrunnable_custom_checker_specs() {
		let root = temp_project("custom-skips");
		let executable = std::env::current_exe()
			.expect("current executable")
			.to_string_lossy()
			.into_owned();
		let suite = build_custom_cleanse_suite(&root, vec![
			CustomCleanseCheckerSpec {
				label:    "available".to_owned(),
				language: Some("Test".to_owned()),
				cwd:      None,
				command:  vec![executable.clone()],
				parser:   None,
			},
			CustomCleanseCheckerSpec {
				label:    "missing binary".to_owned(),
				language: None,
				cwd:      None,
				command:  vec!["./definitely-missing-checker".to_owned()],
				parser:   None,
			},
			CustomCleanseCheckerSpec {
				label:    "escaping cwd".to_owned(),
				language: None,
				cwd:      Some("../outside".to_owned()),
				command:  vec![executable],
				parser:   None,
			},
			CustomCleanseCheckerSpec {
				label:    "empty command".to_owned(),
				language: None,
				cwd:      None,
				command:  Vec::new(),
				parser:   None,
			},
		])
		.await;
		assert_eq!(suite.checkers().len(), 1);
		assert_eq!(suite.checkers()[0].label, "available");
		assert_eq!(suite.skipped().len(), 3);
		assert!(
			suite.skipped()[0].reason.contains("executable not found"),
			"{:?}",
			suite.skipped()[0]
		);
		assert!(
			suite.skipped()[1].reason.contains("working directory escapes the project"),
			"{:?}",
			suite.skipped()[1]
		);
		assert_eq!(suite.skipped()[2].reason, "empty command");
		let _ = fs::remove_dir_all(root);
	}

	#[cfg(unix)]
	#[tokio::test]
	async fn runs_checker_and_filters_to_project_files() {
		let root = temp_project("run");
		let shell = if cfg!(target_os = "android") { "/system/bin/sh" } else { "/bin/sh" };
		let plan = CheckerPlan {
			id:         "generic:.".to_owned(),
			label:      "generic".to_owned(),
			language:   "C".to_owned(),
			cwd:        root.clone(),
			executable: PathBuf::from(shell),
			args:       vec!["-c".to_owned(), "echo 'src/a.c:1:2: warning: bad'; exit 0".to_owned()],
			parser:     CleanseParserKind::Generic,
			command:    "sh -c ...".to_owned(),
			mutates:    false,
		};
		let allowed = Arc::new(["src/a.c".to_owned()].into_iter().collect());
		let check = run_checker(plan, root.clone(), allowed).await;
		assert_eq!(check.exit_code, Some(0));
		assert_eq!(check.diagnostics.len(), 1);
		assert_eq!(check.diagnostics[0].file.as_deref(), Some("src/a.c"));
		assert_eq!(check.diagnostics[0].message, "bad");
		let _ = fs::remove_dir_all(root);
	}

	#[cfg(unix)]
	#[tokio::test]
	async fn truncated_cargo_metadata_is_not_a_clean_report() {
		use std::os::unix::fs::PermissionsExt;

		let root = temp_project("metadata-truncated");
		fs::write(root.join("Cargo.toml"), "[workspace]\nmembers = []\n").expect("write manifest");
		let bin = root.join("node_modules/.bin");
		fs::create_dir_all(&bin).expect("create bin");
		let cargo = bin.join("cargo");
		let shell = if cfg!(target_os = "android") { "/system/bin/sh" } else { "/bin/sh" };
		let chunks = CARGO_METADATA_CAPTURE_BYTES / 1024 + 1;
		let script = format!(
			"#!{shell}\ni=0\nwhile [ \"$i\" -lt {chunks} ]; do printf '%01024d' 0; i=$((i+1)); done\n"
		);
		fs::write(&cargo, script).expect("write fake cargo");
		fs::set_permissions(&cargo, fs::Permissions::from_mode(0o755)).expect("make executable");
		let suite = discover_blocking(&root, &CleanseDiagnosticSuiteOptions {
			include_tests: false,
		});
		let report = suite.run().await;
		assert!(report.checks.is_empty());
		assert!(
			report.diagnostics.iter().any(|diagnostic| {
				diagnostic.message.contains("cargo metadata failed")
					&& diagnostic.message.contains("stdout exceeded")
			}),
			"{:?}",
			report.diagnostics
		);
		let _ = fs::remove_dir_all(root);
	}

	#[cfg(unix)]
	#[tokio::test]
	async fn reports_failure_when_all_parsed_diagnostics_are_filtered_out() {
		let root = temp_project("filtered-failure");
		let shell = if cfg!(target_os = "android") { "/system/bin/sh" } else { "/bin/sh" };
		let plan = CheckerPlan {
			id:         "generic:.".to_owned(),
			label:      "generic".to_owned(),
			language:   "C".to_owned(),
			cwd:        root.clone(),
			executable: PathBuf::from(shell),
			args:       vec!["-c".to_owned(), "echo 'src/b.c:1:2: warning: bad'; exit 1".to_owned()],
			parser:     CleanseParserKind::Generic,
			command:    "sh -c ...".to_owned(),
			mutates:    false,
		};
		let allowed = Arc::new(["src/a.c".to_owned()].into_iter().collect());
		let check = run_checker(plan, root.clone(), allowed).await;
		assert_eq!(check.exit_code, Some(1));
		assert_eq!(check.diagnostics.len(), 1);
		assert!(check.diagnostics[0].file.is_none());
		assert!(
			check.diagnostics[0].message.contains("failed with exit code 1"),
			"{}",
			check.diagnostics[0].message
		);
		let _ = fs::remove_dir_all(root);
	}

}
