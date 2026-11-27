//! Debug adapter definitions, project config discovery, and auto-selection.
//!
//! Mirrors `packages/coding-agent/src/dap/config.ts` + `defaults.json`: the 14
//! built-in adapters stay embedded, project/user `dap.{json,yaml,yml}` overlays
//! merge lowest-to-highest priority, and adapter commands are resolved on
//! `PATH` without installing anything. A configured-but-missing adapter is
//! reported with the command the user must provide.

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Embedded copy of the upstream `defaults.json`.
const DEFAULTS_JSON: &str = r#"
{
	"gdb": {
		"command": "gdb", "args": ["-i", "dap"],
		"languages": ["c", "cpp", "rust"],
		"fileTypes": [".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx", ".rs"],
		"rootMarkers": ["Makefile", "CMakeLists.txt", "Cargo.toml", "compile_commands.json"],
		"launchDefaults": { "request": "launch", "stopOnEntry": true, "stopAtBeginningOfMainSubprogram": true },
		"attachDefaults": { "request": "attach" }
	},
	"lldb-dap": {
		"command": "lldb-dap", "args": [],
		"languages": ["c", "cpp", "objc", "swift", "rust", "zig"],
		"fileTypes": [".c", ".cc", ".cpp", ".cxx", ".m", ".mm", ".swift", ".rs", ".zig"],
		"rootMarkers": ["Package.swift", "Cargo.toml", "Makefile", "CMakeLists.txt", "build.zig"],
		"launchDefaults": { "request": "launch", "stopOnEntry": true },
		"attachDefaults": { "request": "attach" }
	},
	"codelldb": {
		"command": "codelldb", "args": ["--port", "0"],
		"languages": ["c", "cpp", "rust", "zig"],
		"fileTypes": [".c", ".cc", ".cpp", ".cxx", ".rs", ".zig"],
		"rootMarkers": ["Cargo.toml", "CMakeLists.txt", "Makefile", "compile_commands.json", "build.zig"],
		"launchDefaults": { "request": "launch", "stopOnEntry": true },
		"attachDefaults": { "request": "attach" }
	},
	"debugpy": {
		"command": "python", "args": ["-m", "debugpy.adapter"],
		"languages": ["python"],
		"fileTypes": [".py"],
		"rootMarkers": ["pyproject.toml", "setup.py", "requirements.txt", "Pipfile"],
		"launchDefaults": { "request": "launch", "justMyCode": false, "stopOnEntry": true },
		"attachDefaults": { "request": "attach", "justMyCode": false }
	},
	"dlv": {
		"command": "dlv", "args": ["dap"], "connectMode": "socket",
		"languages": ["go"],
		"fileTypes": [".go"],
		"rootMarkers": ["go.mod", "go.sum", "go.work"],
		"acceptsDirectoryProgram": true,
		"launchDefaults": { "request": "launch", "mode": "debug", "stopOnEntry": true },
		"attachDefaults": { "request": "attach", "mode": "local" }
	},
	"js-debug-adapter": {
		"command": "js-debug-adapter", "args": [],
		"languages": ["javascript", "typescript"],
		"fileTypes": [".js", ".jsx", ".ts", ".tsx", ".mjs", ".cjs"],
		"rootMarkers": ["package.json", "tsconfig.json", "jsconfig.json"],
		"launchDefaults": { "request": "launch", "type": "pwa-node", "stopOnEntry": true },
		"attachDefaults": { "request": "attach", "type": "pwa-node" }
	},
	"netcoredbg": {
		"command": "netcoredbg", "args": ["--interpreter=vscode"],
		"languages": ["csharp", "fsharp"],
		"fileTypes": [".cs", ".csx", ".fs", ".fsx"],
		"rootMarkers": ["*.sln", "*.csproj", "*.fsproj", "global.json"],
		"launchDefaults": { "request": "launch", "stopAtEntry": true },
		"attachDefaults": { "request": "attach" }
	},
	"kotlin-debug-adapter": {
		"command": "kotlin-debug-adapter", "args": [],
		"languages": ["kotlin"],
		"fileTypes": [".kt", ".kts"],
		"rootMarkers": ["build.gradle", "build.gradle.kts", "pom.xml", "settings.gradle", "settings.gradle.kts"],
		"launchDefaults": { "request": "launch", "mainClass": "", "projectRoot": "" },
		"attachDefaults": { "request": "attach" }
	},
	"rdbg": {
		"command": "rdbg", "args": ["--open", "--command", "--"],
		"languages": ["ruby"],
		"fileTypes": [".rb", ".rake", ".gemspec"],
		"rootMarkers": ["Gemfile", "Rakefile", ".ruby-version"],
		"launchDefaults": { "request": "launch", "type": "rdbg" },
		"attachDefaults": { "request": "attach", "type": "rdbg" }
	},
	"php-debug-adapter": {
		"command": "php-debug-adapter", "args": [],
		"languages": ["php"],
		"fileTypes": [".php", ".phtml"],
		"rootMarkers": ["composer.json", "composer.lock"],
		"launchDefaults": { "request": "launch", "stopOnEntry": true },
		"attachDefaults": { "request": "attach" }
	},
	"bash-debug-adapter": {
		"command": "bash-debug-adapter", "args": [],
		"languages": ["bash", "shell"],
		"fileTypes": [".sh", ".bash"],
		"rootMarkers": [".git"],
		"launchDefaults": { "request": "launch", "type": "bashdb", "pathBashdb": "bashdb", "pathBash": "bash" },
		"attachDefaults": { "request": "attach" }
	},
	"dart-debug-adapter": {
		"command": "dart", "args": ["debug_adapter"],
		"languages": ["dart"],
		"fileTypes": [".dart"],
		"rootMarkers": ["pubspec.yaml", "pubspec.lock"],
		"launchDefaults": { "request": "launch", "stopOnEntry": true },
		"attachDefaults": { "request": "attach" }
	},
	"flutter-debug-adapter": {
		"command": "dart", "args": ["debug_adapter", "--flutter-sdk-path", ""],
		"languages": ["dart"],
		"fileTypes": [".dart"],
		"rootMarkers": ["pubspec.yaml", "android", "ios", "lib/main.dart"],
		"launchDefaults": { "request": "launch" },
		"attachDefaults": { "request": "attach" }
	},
	"elixir-ls-debugger": {
		"command": "elixir-ls-debugger", "args": [],
		"languages": ["elixir"],
		"fileTypes": [".ex", ".exs", ".heex", ".eex"],
		"rootMarkers": ["mix.exs", "mix.lock"],
		"launchDefaults": { "request": "launch", "type": "mix_task", "task": "run", "stopOnEntry": true },
		"attachDefaults": { "request": "attach" }
	}
}
"#;

const DAP_PORT_ARGUMENT: &str = "${port}";
const JS_DEBUG_SERVER_ENV: &str = "JS_DEBUG_DAP_SERVER";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectMode {
	Stdio,
	Socket,
	Tcp,
}

impl ConnectMode {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Stdio => "stdio",
			Self::Socket => "socket",
			Self::Tcp => "tcp",
		}
	}
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdapterConfig {
	pub command: String,
	#[serde(default)]
	pub args: Vec<String>,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub languages: Vec<String>,
	#[serde(default)]
	pub file_types: Vec<String>,
	#[serde(default)]
	pub root_markers: Vec<String>,
	#[serde(default)]
	pub launch_defaults: Value,
	#[serde(default)]
	pub attach_defaults: Value,
	#[serde(default)]
	pub connect_mode: Option<ConnectMode>,
	#[serde(default)]
	pub accepts_directory_program: bool,
}

impl AdapterConfig {
	fn normalize(mut self) -> Self {
		self.file_types = self.file_types.into_iter().map(|entry| entry.to_lowercase()).collect();
		self.languages.retain(|entry| !entry.is_empty());
		self.args.retain(|entry| !entry.is_empty());
		self.root_markers.retain(|entry| !entry.is_empty());
		if !self.launch_defaults.is_object() {
			self.launch_defaults = Value::Object(Map::new());
		}
		if !self.attach_defaults.is_object() {
			self.attach_defaults = Value::Object(Map::new());
		}
		self
	}
}

#[derive(Debug, Clone)]
pub struct ResolvedAdapter {
	pub name: String,
	pub command: String,
	pub args: Vec<String>,
	pub resolved_command: String,
	pub languages: Vec<String>,
	pub file_types: Vec<String>,
	pub root_markers: Vec<String>,
	pub launch_defaults: Value,
	pub attach_defaults: Value,
	pub connect_mode: ConnectMode,
	pub accepts_directory_program: bool,
}

impl ResolvedAdapter {
	pub fn skip_attach_request(&self) -> bool {
		self.attach_defaults.get("skipAttachRequest").and_then(Value::as_bool) == Some(true)
	}
}

/// Adapter configs in discovery order (defaults first, overlays appended).
#[derive(Debug, Clone, Default)]
pub struct AdapterConfigs {
	entries: Vec<(String, AdapterConfig)>,
}

impl AdapterConfigs {
	pub fn get(&self, name: &str) -> Option<&AdapterConfig> {
		self.entries.iter().find(|(key, _)| key == name).map(|(_, config)| config)
	}

	pub fn names(&self) -> Vec<&str> {
		self.entries.iter().map(|(name, _)| name.as_str()).collect()
	}

	pub fn iter(&self) -> impl Iterator<Item = (&str, &AdapterConfig)> {
		self.entries.iter().map(|(name, config)| (name.as_str(), config))
	}

	fn set(&mut self, name: String, config: AdapterConfig) {
		if let Some(existing) = self.entries.iter_mut().find(|(key, _)| *key == name) {
			existing.1 = config;
		} else {
			self.entries.push((name, config));
		}
	}
}

fn defaults() -> AdapterConfigs {
	let mut configs = AdapterConfigs::default();
	let parsed: Value = serde_json::from_str(DEFAULTS_JSON).expect("embedded DAP defaults are valid JSON");
	let Some(object) = parsed.as_object() else {
		return configs;
	};
	for (name, raw) in object {
		if let Ok(config) = serde_json::from_value::<AdapterConfig>(raw.clone()) {
			configs.set(name.clone(), config.normalize());
		}
	}
	configs
}

fn merge_adapter(base: Option<&AdapterConfig>, raw: &Value) -> Option<AdapterConfig> {
	let Some(raw_object) = raw.as_object() else {
		return None;
	};
	let mut merged = base.map(|config| serde_json::to_value(AdapterConfigWire::from(config)).unwrap_or(Value::Null));
	let mut target = match merged.take() {
		Some(Value::Object(object)) => object,
		_ => Map::new(),
	};
	for (key, value) in raw_object {
		if key == "launchDefaults" || key == "attachDefaults" {
			let nested_key = if key == "launchDefaults" { "launchDefaults" } else { "attachDefaults" };
			let mut nested = target
				.get(nested_key)
				.and_then(Value::as_object)
				.cloned()
				.unwrap_or_default();
			if let Some(overrides) = value.as_object() {
				for (nested_name, nested_value) in overrides {
					nested.insert(nested_name.clone(), nested_value.clone());
				}
			}
			target.insert(nested_key.to_string(), Value::Object(nested));
			continue;
		}
		target.insert(key.clone(), value.clone());
	}
	serde_json::from_value::<AdapterConfig>(Value::Object(target)).ok().map(AdapterConfig::normalize)
}

/// Serialization bridge so merge keeps canonical camelCase field names.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AdapterConfigWire<'a> {
	command: &'a str,
	args: &'a Vec<String>,
	languages: &'a Vec<String>,
	file_types: &'a Vec<String>,
	root_markers: &'a Vec<String>,
	launch_defaults: &'a Value,
	attach_defaults: &'a Value,
	connect_mode: Option<ConnectMode>,
	accepts_directory_program: bool,
}

impl<'a> From<&'a AdapterConfig> for AdapterConfigWire<'a> {
	fn from(config: &'a AdapterConfig) -> Self {
		Self {
			command: &config.command,
			args: &config.args,
			languages: &config.languages,
			file_types: &config.file_types,
			root_markers: &config.root_markers,
			launch_defaults: &config.launch_defaults,
			attach_defaults: &config.attach_defaults,
			connect_mode: config.connect_mode,
			accepts_directory_program: config.accepts_directory_program,
		}
	}
}

fn env_value<'a>(env: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
	env.get(key).map(String::as_str)
}

fn home_dir(env: &HashMap<String, String>) -> Option<PathBuf> {
	env_value(env, "HOME").filter(|home| !home.is_empty()).map(PathBuf::from)
}

fn config_file_names() -> [&'static str; 6] {
	["dap.json", ".dap.json", "dap.yaml", ".dap.yaml", "dap.yml", ".dap.yml"]
}

fn push_config_files(sources: &mut Vec<PathBuf>, dir: &Path) {
	for name in config_file_names() {
		sources.push(dir.join(name));
	}
}

/// Nearest first: with the later reversal in `load_adapter_configs`, closer
/// directories override ancestors and the launch cwd overrides everything.
fn config_sources(cwd: &Path, env: &HashMap<String, String>, trust_project: bool) -> Vec<PathBuf> {
	let mut sources = Vec::new();
	// Project surfaces can register adapters that launch arbitrary commands, so
	// they are repository content: untrusted workspaces get home sources only.
	// A grant fingerprints paths under `cwd` only, so ancestors above it are
	// never project content here either.
	if trust_project {
		// Plain files directly in the launch cwd win over every directory overlay.
		push_config_files(&mut sources, cwd);
		for dir in [".omp", ".claude", ".codex", ".gemini"] {
			push_config_files(&mut sources, &cwd.join(dir));
		}
	}
	if let Some(home) = home_dir(env) {
		let explicit = env.get("PI_CONFIG_DIR").is_some_and(|value| !value.is_empty())
			|| env.get("PI_CODING_AGENT_DIR").is_some_and(|value| !value.is_empty());
		let mut dirs = vec![".aragent/agent"];
		if !explicit {
			dirs.push(".omp/agent");
		}
		dirs.extend([".claude", ".codex", ".gemini"]);
		for dir in dirs {
			push_config_files(&mut sources, &home.join(dir));
		}
		// Plugin roots are excluded by the Android boundary; the home fallback stays.
		push_config_files(&mut sources, &home);
	}
	sources
}

fn parse_yaml_scalar(text: &str) -> Value {
	let text = text.trim();
	if text.is_empty() {
		return Value::Null;
	}
	if (text.starts_with('"') && text.ends_with('"') && text.len() >= 2)
		|| (text.starts_with('\'') && text.ends_with('\'') && text.len() >= 2)
	{
		let inner = &text[1..text.len() - 1];
		return Value::String(if text.starts_with('"') {
			inner.replace("\\\"", "\"").replace("\\n", "\n").replace("\\\\", "\\")
		} else {
			inner.replace("''", "'")
		});
	}
	match text {
		"true" => return Value::Bool(true),
		"false" => return Value::Bool(false),
		"null" | "~" => return Value::Null,
		_ => {},
	}
	if let Ok(number) = text.parse::<i64>() {
		return Value::Number(number.into());
	}
	if let Ok(number) = text.parse::<f64>() {
		if let Some(number) = serde_json::Number::from_f64(number) {
			return Value::Number(number);
		}
	}
	if text.starts_with('[') && text.ends_with(']') {
		let inner = &text[1..text.len() - 1];
		return Value::Array(split_inline(inner).into_iter().map(|entry| parse_yaml_scalar(&entry)).collect());
	}
	if text.starts_with('{') && text.ends_with('}') {
		let mut object = Map::new();
		for entry in split_inline(&text[1..text.len() - 1]) {
			if let Some((key, value)) = entry.split_once(':') {
				object.insert(key.trim().trim_matches('"').to_string(), parse_yaml_scalar(value));
			}
		}
		return Value::Object(object);
	}
	Value::String(text.to_string())
}

fn split_inline(text: &str) -> Vec<String> {
	let mut parts = Vec::new();
	let mut current = String::new();
	let mut quote: Option<char> = None;
	for ch in text.chars() {
		match quote {
			Some(active) => {
				current.push(ch);
				if ch == active {
					quote = None;
				}
			},
			None if ch == '"' || ch == '\'' => {
				quote = Some(ch);
				current.push(ch);
			},
			None if ch == ',' => {
				parts.push(current.trim().to_string());
				current.clear();
			},
			None => current.push(ch),
		}
	}
	if !current.trim().is_empty() {
		parts.push(current.trim().to_string());
	}
	parts
}

fn strip_yaml_comment(line: &str) -> &str {
	let mut quote: Option<char> = None;
	for (index, ch) in line.char_indices() {
		match quote {
			Some(active) if ch == active => quote = None,
			Some(_) => {},
			None if ch == '"' || ch == '\'' => quote = Some(ch),
			None if ch == '#' => return &line[..index],
			None => {},
		}
	}
	line
}

/// Minimal YAML reader for adapter overlays: nested maps, scalar sequences,
/// inline arrays/objects, quoted/plain scalars. Anchors, multi-line scalars and
/// merge keys are intentionally unsupported.
fn parse_yaml_subset(text: &str) -> Result<Value, String> {
	let lines: Vec<(usize, String)> = text
		.lines()
		.filter_map(|line| {
			let trimmed = strip_yaml_comment(line).trim_end();
			if trimmed.trim().is_empty() {
				return None;
			}
			let indent = trimmed.len() - trimmed.trim_start().len();
			Some((indent, trimmed.trim_start().to_string()))
		})
		.collect();
	let mut index = 0usize;
	let value = parse_yaml_block(&lines, &mut index, lines.first().map(|(indent, _)| *indent).unwrap_or(0))?;
	Ok(value)
}

fn parse_yaml_block(lines: &[(usize, String)], index: &mut usize, indent: usize) -> Result<Value, String> {
	if *index >= lines.len() {
		return Ok(Value::Null);
	}
	if lines[*index].1.starts_with("- ") || lines[*index].1 == "-" {
		let mut array = Vec::new();
		while *index < lines.len() && lines[*index].0 == indent && (lines[*index].1.starts_with("- ") || lines[*index].1 == "-") {
			let content = lines[*index].1[1..].trim().to_string();
			*index += 1;
			if content.is_empty() {
				let next_indent = lines.get(*index).map(|(indent, _)| *indent).unwrap_or(indent + 2);
				array.push(parse_yaml_block(lines, index, next_indent)?);
			} else {
				array.push(parse_yaml_scalar(&content));
			}
		}
		return Ok(Value::Array(array));
	}
	let mut object = Map::new();
	while *index < lines.len() && lines[*index].0 == indent {
		let content = lines[*index].1.clone();
		let Some(split) = find_yaml_colon(&content) else {
			return Err(format!("unsupported YAML line: {content}"));
		};
		let key = content[..split].trim().trim_matches('"').trim_matches('\'').to_string();
		let rest = content[split + 1..].trim().to_string();
		*index += 1;
		let value = if rest.is_empty() {
			let next_indent = lines.get(*index).map(|(indent, _)| *indent).unwrap_or(indent);
			if next_indent > indent {
				parse_yaml_block(lines, index, next_indent)?
			} else {
				Value::Null
			}
		} else {
			parse_yaml_scalar(&rest)
		};
		object.insert(key, value);
	}
	Ok(Value::Object(object))
}

fn find_yaml_colon(text: &str) -> Option<usize> {
	let mut quote: Option<char> = None;
	for (index, ch) in text.char_indices() {
		match quote {
			Some(active) if ch == active => quote = None,
			Some(_) => {},
			None if ch == '"' || ch == '\'' => quote = Some(ch),
			None if ch == ':' => return Some(index),
			None => {},
		}
	}
	None
}

fn read_config_file(path: &Path) -> Option<Value> {
	let content = std::fs::read_to_string(path).ok()?;
	let extension = path.extension().and_then(|value| value.to_str()).unwrap_or("");
	let parsed = if extension.eq_ignore_ascii_case("yaml") || extension.eq_ignore_ascii_case("yml") {
		serde_json::from_str(&content).or_else(|_| parse_yaml_subset(&content)).ok()?
	} else {
		serde_json::from_str(&content).ok()?
	};
	let object = parsed.as_object()?;
	Some(match object.get("adapters") {
		Some(Value::Object(adapters)) => Value::Object(adapters.clone()),
		Some(_) => return None,
		None => parsed,
	})
}

fn load_adapter_configs(cwd: &Path, env: &HashMap<String, String>, trust_project: bool) -> AdapterConfigs {
	let mut configs = defaults();
	let mut sources = config_sources(cwd, env, trust_project);
	sources.reverse();
	for source in sources {
		let Some(Value::Object(overrides)) = read_config_file(&source) else {
			continue;
		};
		for (name, raw) in overrides {
			let base = configs.get(&name);
			match merge_adapter(base, &raw) {
				Some(config) => configs.set(name, config),
				None => {
					eprintln!("Ignoring invalid DAP adapter config: {name} ({})", source.display());
				},
			}
		}
	}
	configs
}

pub fn get_adapter_configs(cwd: &Path, env: &HashMap<String, String>) -> AdapterConfigs {
	get_adapter_configs_with_trust(cwd, env, false)
}

/// Trusted-caller variant: `trust_project` loads cwd and ancestor DAP overlays.
pub fn get_adapter_configs_with_trust(
	cwd: &Path,
	env: &HashMap<String, String>,
	trust_project: bool,
) -> AdapterConfigs {
	load_adapter_configs(cwd, env, trust_project)
}

fn is_executable_file(path: &Path) -> bool {
	let Ok(metadata) = std::fs::metadata(path) else {
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

fn normalize_command_for_cwd(command: &str, cwd: &Path) -> PathBuf {
	let path = PathBuf::from(command);
	if path.is_absolute() {
		return path;
	}
	if command.starts_with("./") || command.starts_with("../") || command.starts_with(".\\") || command.starts_with("..\\") {
		return cwd.join(command);
	}
	path
}

/// Canonical project roots an untrusted adapter lookup must never execute
/// from: the launch cwd, the session workspace, an explicit root-marker dir,
/// and any enclosing git checkout. `/` and `$HOME` never become checkout roots.
fn untrusted_checkout_roots(
	cwd: &Path,
	session_cwd: Option<&Path>,
	root_dir: Option<&Path>,
) -> Vec<PathBuf> {
	let home = std::env::var_os("HOME")
		.map(PathBuf::from)
		.and_then(|home| std::fs::canonicalize(home).ok());
	let mut roots = Vec::new();
	for path in [Some(cwd), session_cwd, root_dir].into_iter().flatten() {
		if let Ok(canonical) = std::fs::canonicalize(path) {
			roots.push(canonical);
		}
	}
	for path in [Some(cwd), session_cwd].into_iter().flatten() {
		collect_checkout_roots(path, home.as_deref(), &mut roots);
	}
	roots.sort();
	roots.dedup();
	roots
}

/// Push every ancestor (inclusive) of canonical `start` containing `.git`.
///
/// Nested markers (submodules, attacker-placed `.git` directories) must not
/// hide an outer checkout, so the walk continues past a match. It stops before
/// `/` and `$HOME`.
fn collect_checkout_roots(start: &Path, home: Option<&Path>, out: &mut Vec<PathBuf>) {
	// Walk the canonical path: a symlinked workspace alias must still find the
	// real checkout's `.git` instead of a lexical ancestor of the alias.
	let canonical_start = std::fs::canonicalize(start).unwrap_or_else(|_| start.to_path_buf());
	let mut current = canonical_start.as_path();
	loop {
		// `Path::parent` is `None` for `/`, so the filesystem root is never
		// inspected as a checkout.
		if current.parent().is_none() || home.is_some_and(|home| current == home) {
			break;
		}
		if current.join(".git").exists() {
			out.push(current.to_path_buf());
		}
		let Some(parent) = current.parent() else {
			break;
		};
		current = parent;
	}
}

/// Resolve an executable name to an absolute path, without instantiating it.
pub fn resolve_command(command: &str, cwd: &Path, env: &HashMap<String, String>) -> Option<String> {
	resolve_command_with_base(command, Some(cwd), &[], env)
}

/// Whether `path` canonicalizes inside one of the untrusted checkout roots.
fn path_inside_untrusted_roots(path: &Path, untrusted_roots: &[PathBuf]) -> bool {
	!untrusted_roots.is_empty()
		&& std::fs::canonicalize(path).is_ok_and(|candidate| {
			untrusted_roots
				.iter()
				.any(|root| candidate.starts_with(root))
		})
}

/// `base` is what an empty `PATH` entry means (`cwd`). A non-empty
/// `untrusted_roots` turns on fail-closed `PATH` filtering: `base` is `None`,
/// relative entries are skipped, and any entry or binary that canonicalizes
/// inside ANY untrusted root is refused, so a checkout (or a sibling directory
/// in it) cannot shadow a system adapter.
fn resolve_command_with_base(
	command: &str,
	base: Option<&Path>,
	untrusted_roots: &[PathBuf],
	env: &HashMap<String, String>,
) -> Option<String> {
	let looks_like_path = command.contains('/') || command.contains('\\') || Path::new(command).is_absolute();
	if looks_like_path {
		let path = if Path::new(command).is_absolute() {
			PathBuf::from(command)
		} else {
			normalize_command_for_cwd(command, base?)
		};
		if path_inside_untrusted_roots(&path, untrusted_roots) {
			return None;
		}
		return is_executable_file(&path).then(|| path.to_string_lossy().into_owned());
	}
	let path_var = env_value(env, "PATH")
		.map(str::to_string)
		.or_else(|| std::env::var("PATH").ok())
		.unwrap_or_default();
	let separator = if cfg!(windows) { ';' } else { ':' };
	for dir in path_var.split(separator) {
		let entry = Path::new(dir);
		let candidate_base = if dir.is_empty() {
			match base {
				Some(base) => base.to_path_buf(),
				None => continue,
			}
		} else {
			if !untrusted_roots.is_empty() && !entry.is_absolute() {
				continue;
			}
			entry.to_path_buf()
		};
		if path_inside_untrusted_roots(&candidate_base, untrusted_roots) {
			continue;
		}
		let candidate = candidate_base.join(command);
		if is_executable_file(&candidate) && !path_inside_untrusted_roots(&candidate, untrusted_roots) {
			return Some(candidate.to_string_lossy().into_owned());
		}
		if cfg!(windows) {
			for suffix in [".exe", ".cmd", ".bat"] {
				let candidate = candidate_base.join(format!("{command}{suffix}"));
				if is_executable_file(&candidate) && !path_inside_untrusted_roots(&candidate, untrusted_roots) {
					return Some(candidate.to_string_lossy().into_owned());
				}
			}
		}
	}
	None
}

fn resolve_js_debug_server(
	cwd: &Path,
	env: &HashMap<String, String>,
	trust_project: bool,
	untrusted_roots: &[PathBuf],
) -> Option<PathBuf> {
	let mut candidates = Vec::new();
	if let Some(configured) = env_value(env, JS_DEBUG_SERVER_ENV).filter(|value| !value.is_empty()) {
		let configured = PathBuf::from(configured);
		if configured.is_absolute() {
			candidates.push(configured);
		} else if trust_project {
			candidates.push(cwd.join(configured));
		}
	}
	let data_home = env_value(env, "XDG_DATA_HOME")
		.filter(|value| !value.is_empty())
		.map(PathBuf::from)
		.or_else(|| home_dir(env).map(|home| home.join(".local/share")));
	if let Some(data_home) = data_home {
		candidates.push(data_home.join("nvim/mason/packages/js-debug-adapter/js-debug/src/dapDebugServer.js"));
	}
	if let Some(home) = home_dir(env) {
		candidates.push(home.join(".local/opt/js-debug/src/dapDebugServer.js"));
	}
	// A server inside an untrusted checkout is checkout-controlled code that
	// Node would execute, even when the path comes from an absolute setting.
	candidates.into_iter().find(|candidate| {
		candidate.is_file()
			&& (trust_project || !path_inside_untrusted_roots(candidate, untrusted_roots))
	})
}

fn resolve_adapter_from_config(
	name: &str,
	configs: &AdapterConfigs,
	cwd: &Path,
	env: &HashMap<String, String>,
	local_roots: Option<&[PathBuf]>,
	untrusted_roots: &[PathBuf],
) -> Option<ResolvedAdapter> {
	let trusted = untrusted_roots.is_empty();
	let config = configs.get(name)?;
	if name == "js-debug-adapter" && config.command == "js-debug-adapter" {
		let server = resolve_js_debug_server(cwd, env, trusted, untrusted_roots)?;
		let node = if trusted {
			resolve_command("node", cwd, env)?
		} else {
			resolve_command_with_base("node", None, untrusted_roots, env)?
		};
		return Some(ResolvedAdapter {
			name: name.to_string(),
			command: "node".to_string(),
			args: vec![server.to_string_lossy().into_owned(), DAP_PORT_ARGUMENT.to_string(), "127.0.0.1".to_string()],
			resolved_command: node,
			languages: config.languages.clone(),
			file_types: config.file_types.clone(),
			root_markers: config.root_markers.clone(),
			launch_defaults: config.launch_defaults.clone(),
			attach_defaults: config.attach_defaults.clone(),
			connect_mode: ConnectMode::Tcp,
			accepts_directory_program: config.accepts_directory_program,
		});
	}
	let normalized = normalize_command_for_cwd(&config.command, cwd);
	let command_is_bare = !Path::new(&config.command).is_absolute()
		&& !config.command.contains('/')
		&& !config.command.contains('\\');
	let search_cwd = if command_is_bare {
		local_roots
			.and_then(|roots| roots.last().cloned())
			.unwrap_or_else(|| cwd.to_path_buf())
	} else {
		cwd.to_path_buf()
	};
	let resolved_command = if command_is_bare {
		let mut resolved = None;
		if let Some(roots) = local_roots {
			for root in roots.iter() {
				if let Some(found) = resolve_command(&config.command, root, env) {
					resolved = Some(found);
					break;
				}
			}
		}
		resolved.or_else(|| {
			if trusted {
				resolve_command(&config.command, &search_cwd, env)
			} else {
				// Empty or relative entries would resolve inside the checkout.
				resolve_command_with_base(&config.command, None, untrusted_roots, env)
			}
		})
	} else if trusted || Path::new(&config.command).is_absolute() {
		resolve_command_with_base(&normalized.to_string_lossy(), Some(cwd), untrusted_roots, env)
	} else {
		// A relative command path would resolve inside the untrusted checkout.
		None
	}?;
	Some(ResolvedAdapter {
		name: name.to_string(),
		command: config.command.clone(),
		args: config.args.clone(),
		resolved_command,
		languages: config.languages.clone(),
		file_types: config.file_types.clone(),
		root_markers: config.root_markers.clone(),
		launch_defaults: config.launch_defaults.clone(),
		attach_defaults: config.attach_defaults.clone(),
		connect_mode: config.connect_mode.unwrap_or(ConnectMode::Stdio),
		accepts_directory_program: config.accepts_directory_program,
	})
}

pub fn resolve_adapter(name: &str, cwd: &Path, env: &HashMap<String, String>) -> Option<ResolvedAdapter> {
	resolve_adapter_with_trust(name, cwd, env, false)
}

pub fn resolve_adapter_with_trust(
	name: &str,
	cwd: &Path,
	env: &HashMap<String, String>,
	trust_project: bool,
) -> Option<ResolvedAdapter> {
	resolve_adapter_with_untrusted_roots(
		name,
		cwd,
		env,
		&untrusted_roots_for(cwd, None, None, trust_project),
	)
}

fn resolve_adapter_with_untrusted_roots(
	name: &str,
	cwd: &Path,
	env: &HashMap<String, String>,
	untrusted_roots: &[PathBuf],
) -> Option<ResolvedAdapter> {
	let configs = get_adapter_configs_with_trust(cwd, env, untrusted_roots.is_empty());
	resolve_adapter_from_config(name, &configs, cwd, env, None, untrusted_roots)
}

pub fn get_available_adapters(cwd: &Path, env: &HashMap<String, String>) -> Vec<ResolvedAdapter> {
	get_available_adapters_with_trust(cwd, env, false)
}

pub fn get_available_adapters_with_trust(
	cwd: &Path,
	env: &HashMap<String, String>,
	trust_project: bool,
) -> Vec<ResolvedAdapter> {
	get_available_adapters_with_untrusted_roots(
		cwd,
		env,
		&untrusted_roots_for(cwd, None, None, trust_project),
	)
}

fn get_available_adapters_with_untrusted_roots(
	cwd: &Path,
	env: &HashMap<String, String>,
	untrusted_roots: &[PathBuf],
) -> Vec<ResolvedAdapter> {
	let configs = get_adapter_configs_with_trust(cwd, env, untrusted_roots.is_empty());
	configs
		.iter()
		.filter_map(|(name, _)| {
			resolve_adapter_from_config(name, &configs, cwd, env, None, untrusted_roots)
		})
		.collect()
}

/// `untrusted_checkout_roots` for a lookup, empty when the workspace is trusted.
fn untrusted_roots_for(
	cwd: &Path,
	session_cwd: Option<&Path>,
	root_dir: Option<&Path>,
	trust_project: bool,
) -> Vec<PathBuf> {
	if trust_project {
		Vec::new()
	} else {
		untrusted_checkout_roots(cwd, session_cwd, root_dir)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchProgramKind {
	File,
	Directory,
	Missing,
}

#[derive(Debug, Clone)]
pub enum LaunchAdapterSelection {
	Adapter(ResolvedAdapter),
	Unavailable { adapter_name: String, command: String },
	None,
}

fn marker_matches(dir: &Path, marker: &str) -> bool {
	if let Some(suffix) = marker.strip_prefix('*') {
		let Ok(entries) = std::fs::read_dir(dir) else {
			return false;
		};
		return entries.filter_map(Result::ok).any(|entry| {
			entry
				.file_name()
				.to_string_lossy()
				.to_lowercase()
				.ends_with(&suffix.to_lowercase())
		});
	}
	dir.join(marker).exists()
}

fn has_root_markers(dir: &Path, markers: &[String]) -> bool {
	markers.iter().any(|marker| marker_matches(dir, marker))
}

fn find_root_marker_in_launch_ancestry(
	program: &Path,
	cwd: &Path,
	markers: &[String],
	program_kind: LaunchProgramKind,
) -> Option<PathBuf> {
	if markers.is_empty() {
		return None;
	}
	let resolved = if program.is_absolute() { program.to_path_buf() } else { cwd.join(program) };
	let mut dir = match program_kind {
		LaunchProgramKind::Directory => resolved,
		_ => resolved.parent().map(Path::to_path_buf).unwrap_or(resolved),
	};
	loop {
		if has_root_markers(&dir, markers) {
			return Some(dir);
		}
		let Some(parent) = dir.parent() else {
			return None;
		};
		if parent == dir {
			return None;
		}
		dir = parent.to_path_buf();
	}
}

fn resolve_adapter_for_launch(
	adapter_name: &str,
	configs: &AdapterConfigs,
	cwd: &Path,
	env: &HashMap<String, String>,
	root_dir: Option<&PathBuf>,
	untrusted_roots: &[PathBuf],
) -> Option<ResolvedAdapter> {
	// Root-marker and cwd directories hold repository files; searching them for
	// a bare adapter executable is a project-controlled command, so only a
	// trusted workspace may prefer them over `PATH`.
	let roots = if untrusted_roots.is_empty() {
		root_dir.filter(|root| root.as_path() != cwd).map(|root| vec![root.clone(), cwd.to_path_buf()])
	} else {
		None
	};
	resolve_adapter_from_config(adapter_name, configs, cwd, env, roots.as_deref(), untrusted_roots)
}

fn extensionless_rank(name: &str) -> Option<usize> {
	["gdb", "lldb-dap"].iter().position(|candidate| *candidate == name)
}

fn sort_adapters_for_launch(
	program: &Path,
	cwd: &Path,
	program_kind: LaunchProgramKind,
	mut adapters: Vec<ResolvedAdapter>,
) -> Vec<ResolvedAdapter> {
	let extension = program
		.extension()
		.map(|value| format!(".{}", value.to_string_lossy().to_lowercase()))
		.unwrap_or_default();
	adapters.sort_by(|left, right| {
		let left_extension = !extension.is_empty() && left.file_types.contains(&extension);
		let right_extension = !extension.is_empty() && right.file_types.contains(&extension);
		if left_extension != right_extension {
			return right_extension.cmp(&left_extension);
		}
		let left_root =
			find_root_marker_in_launch_ancestry(program, cwd, &left.root_markers, program_kind).is_some();
		let right_root =
			find_root_marker_in_launch_ancestry(program, cwd, &right.root_markers, program_kind).is_some();
		if left_root != right_root {
			return right_root.cmp(&left_root);
		}
		let left_rank = extensionless_rank(&left.name).unwrap_or(usize::MAX);
		let right_rank = extensionless_rank(&right.name).unwrap_or(usize::MAX);
		left_rank.cmp(&right_rank).then_with(|| left.name.cmp(&right.name))
	});
	adapters
}

fn select_automatic_launch_adapter(
	program: &Path,
	cwd: &Path,
	session_cwd: Option<&Path>,
	program_kind: LaunchProgramKind,
	configs: &AdapterConfigs,
	env: &HashMap<String, String>,
	trust_project: bool,
) -> LaunchAdapterSelection {
	let extension = program
		.extension()
		.map(|value| format!(".{}", value.to_string_lossy().to_lowercase()))
		.unwrap_or_default();
	if !extension.is_empty() {
		let mut configured: Vec<(String, Option<PathBuf>)> = Vec::new();
		let mut available = Vec::new();
		for (name, config) in configs.iter() {
			if !config.file_types.contains(&extension) {
				continue;
			}
			let root = find_root_marker_in_launch_ancestry(program, cwd, &config.root_markers, program_kind);
			configured.push((name.to_string(), root.clone()));
			let untrusted_roots = untrusted_roots_for(cwd, session_cwd, root.as_deref(), trust_project);
			if let Some(adapter) =
				resolve_adapter_for_launch(name, configs, cwd, env, root.as_ref(), &untrusted_roots)
			{
				available.push(adapter);
			}
		}
		if let Some(selected) = sort_adapters_for_launch(program, cwd, program_kind, available).into_iter().next() {
			return LaunchAdapterSelection::Adapter(selected);
		}
		let root_match = configured.iter().find(|(_, root)| root.is_some());
		if let Some((name, _)) = root_match.or_else(|| configured.first()) {
			return unavailable_adapter(name, configs);
		}
	}

	let mut available = Vec::new();
	let mut root_matches = Vec::new();
	let mut directory_matches = Vec::new();
	for (name, config) in configs.iter() {
		let root = find_root_marker_in_launch_ancestry(program, cwd, &config.root_markers, program_kind);
		if root.is_some() {
			root_matches.push(name.to_string());
			if config.accepts_directory_program {
				directory_matches.push(name.to_string());
			}
		}
		if extensionless_rank(name).is_none() && root.is_none() {
			continue;
		}
		let untrusted_roots = untrusted_roots_for(cwd, session_cwd, root.as_deref(), trust_project);
		if let Some(adapter) =
			resolve_adapter_for_launch(name, configs, cwd, env, root.as_ref(), &untrusted_roots)
		{
			available.push(adapter);
		}
	}

	if program_kind == LaunchProgramKind::Directory && !directory_matches.is_empty() {
		let directory_adapters: Vec<ResolvedAdapter> = available
			.iter()
			.filter(|adapter| adapter.accepts_directory_program && directory_matches.contains(&adapter.name))
			.cloned()
			.collect();
		if let Some(selected) = sort_adapters_for_launch(program, cwd, program_kind, directory_adapters)
			.into_iter()
			.next()
		{
			return LaunchAdapterSelection::Adapter(selected);
		}
		return unavailable_adapter(&directory_matches[0], configs);
	}

	let candidates: Vec<ResolvedAdapter> = if program_kind == LaunchProgramKind::Directory {
		let directory_adapters: Vec<ResolvedAdapter> =
			available.iter().filter(|adapter| adapter.accepts_directory_program).cloned().collect();
		if directory_adapters.is_empty() {
			available
		} else {
			directory_adapters
		}
	} else {
		available
	};
	if let Some(selected) = sort_adapters_for_launch(program, cwd, program_kind, candidates).into_iter().next() {
		return LaunchAdapterSelection::Adapter(selected);
	}
	match root_matches.first() {
		Some(name) => unavailable_adapter(name, configs),
		None => LaunchAdapterSelection::None,
	}
}

fn unavailable_adapter(name: &str, configs: &AdapterConfigs) -> LaunchAdapterSelection {
	match configs.get(name) {
		Some(config) => LaunchAdapterSelection::Unavailable {
			adapter_name: name.to_string(),
			command: config.command.clone(),
		},
		None => LaunchAdapterSelection::None,
	}
}

/// Pick a launch adapter. Explicit names win; otherwise ranking is extension,
/// root-marker, native debugger preference (`gdb`, `lldb-dap`), then name.
///
/// Untrusted by default: project DAP overlays and cwd executables are ignored.
pub fn select_launch_adapter(
	program: &Path,
	cwd: &Path,
	env: &HashMap<String, String>,
	adapter_name: Option<&str>,
	program_kind: LaunchProgramKind,
) -> LaunchAdapterSelection {
	select_launch_adapter_with_trust(program, cwd, env, adapter_name, program_kind, false)
}

pub fn select_launch_adapter_with_trust(
	program: &Path,
	cwd: &Path,
	env: &HashMap<String, String>,
	adapter_name: Option<&str>,
	program_kind: LaunchProgramKind,
	trust_project: bool,
) -> LaunchAdapterSelection {
	select_launch_adapter_core(program, None, cwd, env, adapter_name, program_kind, trust_project)
}

/// Session-aware variant: `session_cwd` is the workspace's own root, so an
/// untrusted command cwd nested under it blocks sibling checkout binaries even
/// when the checkout has no `.git` marker.
pub fn select_launch_adapter_in_session_with_trust(
	program: &Path,
	session_cwd: &Path,
	cwd: &Path,
	env: &HashMap<String, String>,
	adapter_name: Option<&str>,
	program_kind: LaunchProgramKind,
	trust_project: bool,
) -> LaunchAdapterSelection {
	select_launch_adapter_core(
		program,
		Some(session_cwd),
		cwd,
		env,
		adapter_name,
		program_kind,
		trust_project,
	)
}

fn select_launch_adapter_core(
	program: &Path,
	session_cwd: Option<&Path>,
	cwd: &Path,
	env: &HashMap<String, String>,
	adapter_name: Option<&str>,
	program_kind: LaunchProgramKind,
	trust_project: bool,
) -> LaunchAdapterSelection {
	let configs = get_adapter_configs_with_trust(cwd, env, trust_project);
	if let Some(name) = adapter_name {
		let Some(config) = configs.get(name) else {
			return LaunchAdapterSelection::None;
		};
		let root =
			find_root_marker_in_launch_ancestry(program, cwd, &config.root_markers, program_kind);
		let untrusted_roots = untrusted_roots_for(cwd, session_cwd, root.as_deref(), trust_project);
		return match resolve_adapter_for_launch(
			name,
			&configs,
			cwd,
			env,
			root.as_ref(),
			&untrusted_roots,
		) {
			Some(adapter) => LaunchAdapterSelection::Adapter(adapter),
			None => LaunchAdapterSelection::Unavailable {
				adapter_name: name.to_string(),
				command: config.command.clone(),
			},
		};
	}
	select_automatic_launch_adapter(
		program,
		cwd,
		session_cwd,
		program_kind,
		&configs,
		env,
		trust_project,
	)
}

/// Attach selection: explicit name, `debugpy` when a remote port is given,
/// native debuggers in order, then the first resolvable adapter.
pub fn select_attach_adapter(
	cwd: &Path,
	env: &HashMap<String, String>,
	adapter_name: Option<&str>,
	port: Option<u16>,
) -> Option<ResolvedAdapter> {
	select_attach_adapter_with_trust(cwd, env, adapter_name, port, false)
}

pub fn select_attach_adapter_with_trust(
	cwd: &Path,
	env: &HashMap<String, String>,
	adapter_name: Option<&str>,
	port: Option<u16>,
	trust_project: bool,
) -> Option<ResolvedAdapter> {
	select_attach_adapter_core(cwd, None, env, adapter_name, port, trust_project)
}

/// Session-aware variant: untrusted roots include `session_cwd`, so sibling
/// checkout binaries are refused even without a `.git` marker.
pub fn select_attach_adapter_in_session_with_trust(
	cwd: &Path,
	session_cwd: &Path,
	env: &HashMap<String, String>,
	adapter_name: Option<&str>,
	port: Option<u16>,
	trust_project: bool,
) -> Option<ResolvedAdapter> {
	select_attach_adapter_core(cwd, Some(session_cwd), env, adapter_name, port, trust_project)
}

fn select_attach_adapter_core(
	cwd: &Path,
	session_cwd: Option<&Path>,
	env: &HashMap<String, String>,
	adapter_name: Option<&str>,
	port: Option<u16>,
	trust_project: bool,
) -> Option<ResolvedAdapter> {
	let untrusted_roots = untrusted_roots_for(cwd, session_cwd, None, trust_project);
	if let Some(name) = adapter_name {
		return resolve_adapter_with_untrusted_roots(name, cwd, env, &untrusted_roots);
	}
	let available = get_available_adapters_with_untrusted_roots(cwd, env, &untrusted_roots);
	if port.is_some() {
		if let Some(debugpy) = available.iter().find(|adapter| adapter.name == "debugpy") {
			return Some(debugpy.clone());
		}
	}
	for preferred in ["gdb", "lldb-dap"] {
		if let Some(found) = available.iter().find(|adapter| adapter.name == preferred) {
			return Some(found.clone());
		}
	}
	available.into_iter().next()
}

pub fn classify_program(program: &Path) -> LaunchProgramKind {
	match std::fs::metadata(program) {
		Ok(metadata) if metadata.is_dir() => LaunchProgramKind::Directory,
		Ok(_) => LaunchProgramKind::File,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => LaunchProgramKind::Missing,
		Err(_) => LaunchProgramKind::File,
	}
}

/// dlv interprets directories and `.go` files as packages, binaries as `exec`.
pub fn resolve_launch_overrides(
	adapter: &ResolvedAdapter,
	program: &Path,
	program_kind: LaunchProgramKind,
) -> Value {
	let mut overrides = Map::new();
	if adapter.name == "dlv" {
		let extension = program
			.extension()
			.map(|value| value.to_string_lossy().to_lowercase())
			.unwrap_or_default();
		if program_kind == LaunchProgramKind::Directory || extension == ".go" {
			overrides.insert("mode".to_string(), Value::String("debug".to_string()));
		} else if program_kind == LaunchProgramKind::File {
			overrides.insert("mode".to_string(), Value::String("exec".to_string()));
		}
	}
	Value::Object(overrides)
}

/// Human-readable `Installed adapters: ...` fallback list.
pub fn configured_adapter_names(cwd: &Path, env: &HashMap<String, String>) -> Vec<String> {
	configured_adapter_names_with_trust(cwd, env, false)
}

pub fn configured_adapter_names_with_trust(
	cwd: &Path,
	env: &HashMap<String, String>,
	trust_project: bool,
) -> Vec<String> {
	let mut names: Vec<String> = get_available_adapters_with_trust(cwd, env, trust_project)
		.into_iter()
		.map(|adapter| adapter.name)
		.collect();
	names.sort();
	names.dedup();
	names
}

/// Accurate missing-prerequisite message for a configured adapter.
pub fn adapter_unavailable_message(adapter_name: &str, command: &str, cwd: &Path, env: &HashMap<String, String>) -> String {
	adapter_unavailable_message_with_trust(adapter_name, command, cwd, env, false)
}

pub fn adapter_unavailable_message_with_trust(
	adapter_name: &str,
	command: &str,
	cwd: &Path,
	env: &HashMap<String, String>,
	trust_project: bool,
) -> String {
	let canonical_command = match adapter_name {
		"debugpy" => "python",
		"dlv" => "dlv",
		"rdbg" => "rdbg",
		"js-debug-adapter" => "node",
		_ => adapter_name,
	};
	if command != canonical_command {
		return format!(
			"adapter '{adapter_name}' is not available: configured command '{command}' did not resolve. Check the DAP adapter config for this workspace."
		);
	}
	if adapter_name == "debugpy" {
		return "adapter 'debugpy' is not available: python not found in PATH".to_string();
	}
	if adapter_name == "js-debug-adapter" {
		return "adapter 'js-debug-adapter' is not available: node and a local dapDebugServer.js are required (set JS_DEBUG_DAP_SERVER)".to_string();
	}
	let installed = configured_adapter_names_with_trust(cwd, env, trust_project);
	let names = if installed.is_empty() {
		"none".to_string()
	} else {
		installed.join(", ")
	};
	format!(
		"adapter '{adapter_name}' is not available: '{command}' did not resolve. Verify it with 'command -v {command}' or configure a DAP adapter path. Installed adapters: {names}"
	)
}

/// Resolve `${port}` placeholders for TCP adapters.
pub fn substitute_port(args: &[String], port: u16) -> Vec<String> {
	args.iter().map(|arg| arg.replace(DAP_PORT_ARGUMENT, &port.to_string())).collect()
}

/// Root-marker scan used by launch ranking; exposed for tests.
pub fn root_marker_dirs(program: &Path, cwd: &Path, markers: &[String], kind: LaunchProgramKind) -> Option<PathBuf> {
	find_root_marker_in_launch_ancestry(program, cwd, markers, kind)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn env_with(path: &str) -> HashMap<String, String> {
		let mut env = HashMap::new();
		env.insert("PATH".to_string(), path.to_string());
		env
	}

	fn unique_dir(label: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!("omp-dap-trust-{}-{label}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		std::fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	/// Home-only environment: no inherited config, no real `PATH` lookup.
	fn isolated_env(home: &Path) -> HashMap<String, String> {
		let mut env = HashMap::new();
		env.insert("HOME".to_string(), home.to_string_lossy().into_owned());
		env.insert("PATH".to_string(), home.join("bin").to_string_lossy().into_owned());
		env
	}

	fn write_json(path: &Path, json: &str) {
		if let Some(parent) = path.parent() {
			std::fs::create_dir_all(parent).expect("config parent");
		}
		std::fs::write(path, json).expect("write config");
	}

	#[test]
	fn untrusted_workspace_ignores_project_dap_configs() {
		let root = unique_dir("project-config");
		let project = root.join("project");
		let home = root.join("home");
		std::fs::create_dir_all(&project).expect("project dir");
		write_json(&project.join("dap.json"), r#"{"adapters":{"project-local":{"command":"sh"}}}"#);
		write_json(&project.join(".omp/dap.json"), r#"{"adapters":{"project-dir-local":{"command":"sh"}}}"#);
		write_json(&root.join(".omp/dap.json"), r#"{"adapters":{"ancestor-local":{"command":"sh"}}}"#);
		write_json(
			&home.join(".aragent/agent/dap.json"),
			r#"{"adapters":{"home-local":{"command":"sh"}}}"#,
		);
		let env = isolated_env(&home);

		let untrusted = get_adapter_configs_with_trust(&project, &env, false);
		assert!(untrusted.get("project-local").is_none(), "cwd overlay must be ignored");
		assert!(untrusted.get("project-dir-local").is_none(), "cwd tool-dir overlay must be ignored");
		assert!(untrusted.get("ancestor-local").is_none(), "ancestor overlay must be ignored");
		assert!(untrusted.get("home-local").is_some(), "home overlay stays operator-owned");
		assert!(
			get_adapter_configs(&project, &env).get("project-local").is_none(),
			"default getter is fail-closed"
		);

		let trusted = get_adapter_configs_with_trust(&project, &env, true);
		assert!(trusted.get("project-local").is_some(), "trusted cwd overlay loads");
		assert!(trusted.get("project-dir-local").is_some(), "trusted cwd tool-dir overlay loads");
		assert!(
			trusted.get("ancestor-local").is_none(),
			"the grant covers only cwd; ancestor overlays must not load"
		);
		assert!(trusted.get("home-local").is_some());
	}

	#[cfg(unix)]
	#[test]
	fn untrusted_workspace_never_runs_bare_local_adapters() {
		use std::os::unix::fs::PermissionsExt;

		let root = unique_dir("bare-binary");
		let project = root.join("project");
		let home = root.join("home");
		let program_dir = project.join("src");
		std::fs::create_dir_all(&program_dir).expect("program dir");
		std::fs::write(program_dir.join("marker.file"), "").expect("marker");
		let command = format!("omp-fake-dap-{}", std::process::id());
		let binary = program_dir.join(&command);
		std::fs::write(&binary, "#!/bin/sh\n").expect("fake adapter");
		std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).expect("chmod");
		let cwd_binary = project.join(&command);
		std::fs::copy(&binary, &cwd_binary).expect("copy fake adapter");
		write_json(
			&home.join(".aragent/agent/dap.json"),
			&format!(
				r#"{{"adapters":{{"local-dap":{{"command":"{command}","fileTypes":[".c"],"rootMarkers":["marker.file"]}}}}}}"#
			),
		);
		let mut env = isolated_env(&home);
		// A trailing empty PATH entry means "current directory"; untrusted
		// resolution must refuse it.
		env.insert("PATH".to_string(), String::new());
		let program = program_dir.join("main.c");

		let untrusted = select_launch_adapter_with_trust(
			&program,
			&project,
			&env,
			Some("local-dap"),
			LaunchProgramKind::File,
			false,
		);
		assert!(
			matches!(untrusted, LaunchAdapterSelection::Unavailable { .. }),
			"untrusted launch must not resolve repository executables"
		);

		let trusted = select_launch_adapter_with_trust(
			&program,
			&project,
			&env,
			Some("local-dap"),
			LaunchProgramKind::File,
			true,
		);
		match trusted {
			LaunchAdapterSelection::Adapter(adapter) => {
				assert_eq!(adapter.resolved_command, binary.to_string_lossy().into_owned());
			},
			other => panic!("trusted launch should resolve the root-marker binary: {other:?}"),
		}
	}

	#[cfg(unix)]
	#[test]
	fn untrusted_path_search_rejects_relative_and_checkout_entries() {
		use std::os::unix::fs::PermissionsExt;

		let root = unique_dir("untrusted-path");
		let project = root.join("project");
		let project_bin = project.join("bin");
		let external_bin = root.join("external-bin");
		std::fs::create_dir_all(&project_bin).expect("project bin");
		std::fs::create_dir_all(&external_bin).expect("external bin");
		let command = format!("omp-path-dap-{}", std::process::id());
		for dir in [&project_bin, &external_bin] {
			let path = dir.join(&command);
			std::fs::write(&path, "#!/bin/sh\n").expect("fake adapter");
			std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
		}

		let mut env = HashMap::new();
		let untrusted_roots = vec![std::fs::canonicalize(&project).expect("canonical project")];
		env.insert("PATH".to_string(), ".".to_string());
		assert!(
			resolve_command_with_base(&command, None, &untrusted_roots, &env).is_none(),
			"relative PATH entries must not resolve"
		);
		env.insert("PATH".to_string(), project_bin.to_string_lossy().into_owned());
		assert!(
			resolve_command_with_base(&command, None, &untrusted_roots, &env).is_none(),
			"PATH entries inside the untrusted cwd must not resolve"
		);
		env.insert("PATH".to_string(), external_bin.to_string_lossy().into_owned());
		let resolved = resolve_command_with_base(&command, None, &untrusted_roots, &env)
			.expect("an external absolute PATH binary still resolves");
		assert_eq!(resolved, external_bin.join(&command).to_string_lossy().into_owned());
	}

	#[cfg(unix)]
	#[test]
	fn untrusted_git_checkout_blocks_sibling_binaries() {
		use std::os::unix::fs::PermissionsExt;

		let root = unique_dir("checkout-git");
		let repo = root.join("repo");
		let src = repo.join("src");
		let bin = repo.join("bin");
		std::fs::create_dir_all(&src).expect("src dir");
		std::fs::create_dir_all(repo.join(".git")).expect(".git dir");
		std::fs::create_dir_all(&bin).expect("bin dir");
		let gdb = bin.join("gdb");
		std::fs::write(&gdb, "#!/bin/sh\n").expect("fake gdb");
		std::fs::set_permissions(&gdb, std::fs::Permissions::from_mode(0o755)).expect("chmod");

		let mut env = HashMap::new();
		env.insert("PATH".to_string(), bin.to_string_lossy().into_owned());
		assert!(
			resolve_adapter_with_trust("gdb", &src, &env, false).is_none(),
			"the enclosing git checkout must block a sibling PATH binary"
		);
		assert!(
			resolve_adapter_with_trust("gdb", &src, &env, true).is_some(),
			"trusted launches may still use PATH"
		);
	}

	#[cfg(unix)]
	#[test]
	fn symlinked_workspace_alias_still_finds_the_checkout_root() {
		use std::os::unix::fs::PermissionsExt;

		let root = unique_dir("checkout-symlink");
		let repo = root.join("repo");
		let src = repo.join("src");
		let bin = repo.join("bin");
		std::fs::create_dir_all(&src).expect("src dir");
		std::fs::create_dir_all(repo.join(".git")).expect(".git dir");
		std::fs::create_dir_all(&bin).expect("bin dir");
		let gdb = bin.join("gdb");
		std::fs::write(&gdb, "#!/bin/sh\n").expect("fake gdb");
		std::fs::set_permissions(&gdb, std::fs::Permissions::from_mode(0o755)).expect("chmod");
		let alias = root.join("alias");
		std::os::unix::fs::symlink(&src, &alias).expect("symlink alias");

		let mut env = HashMap::new();
		env.insert("PATH".to_string(), bin.to_string_lossy().into_owned());
		assert!(
			resolve_adapter_with_trust("gdb", &alias, &env, false).is_none(),
			"a symlinked cwd must still recognize the real checkout root"
		);
	}

	#[cfg(unix)]
	#[test]
	fn nested_git_marker_does_not_hide_the_outer_checkout() {
		use std::os::unix::fs::PermissionsExt;

		let root = unique_dir("checkout-nested");
		let repo = root.join("repo");
		let src = repo.join("src");
		let bin = repo.join("bin");
		std::fs::create_dir_all(repo.join(".git")).expect(".git dir");
		std::fs::create_dir_all(src.join(".git")).expect("nested .git dir");
		std::fs::create_dir_all(&bin).expect("bin dir");
		let gdb = bin.join("gdb");
		std::fs::write(&gdb, "#!/bin/sh\n").expect("fake gdb");
		std::fs::set_permissions(&gdb, std::fs::Permissions::from_mode(0o755)).expect("chmod");

		let mut env = HashMap::new();
		env.insert("PATH".to_string(), bin.to_string_lossy().into_owned());
		assert!(
			resolve_adapter_with_trust("gdb", &src, &env, false).is_none(),
			"a nested .git marker must not hide the outer checkout from the root set"
		);
	}

	#[test]
	fn untrusted_checkout_cannot_supply_the_js_debug_server() {
		let root = unique_dir("js-debug-server");
		let checkout = root.join("checkout");
		std::fs::create_dir_all(&checkout).expect("checkout dir");
		let server = checkout.join("dapDebugServer.js");
		std::fs::write(&server, "// checkout-controlled\n").expect("server file");

		let mut env = HashMap::new();
		env.insert("JS_DEBUG_DAP_SERVER".to_string(), server.to_string_lossy().into_owned());
		env.insert("PATH".to_string(), String::new());
		let untrusted_roots = vec![std::fs::canonicalize(&checkout).expect("canonical checkout")];

		assert!(
			resolve_js_debug_server(&checkout, &env, false, &untrusted_roots).is_none(),
			"an untrusted checkout must not provide the js-debug server"
		);
		assert_eq!(
			resolve_js_debug_server(&checkout, &env, true, &untrusted_roots),
			Some(server),
			"trusted callers may still use the configured js-debug server"
		);
	}

	#[cfg(unix)]
	#[test]
	fn untrusted_session_root_blocks_sibling_binaries_without_git() {
		use std::os::unix::fs::PermissionsExt;

		let root = unique_dir("checkout-session");
		let repo = root.join("repo");
		let src = repo.join("src");
		let bin = repo.join("bin");
		let external_bin = root.join("external-bin");
		std::fs::create_dir_all(&src).expect("src dir");
		std::fs::create_dir_all(&bin).expect("bin dir");
		std::fs::create_dir_all(&external_bin).expect("external bin");
		for dir in [&bin, &external_bin] {
			let gdb = dir.join("gdb");
			std::fs::write(&gdb, "#!/bin/sh\n").expect("fake gdb");
			std::fs::set_permissions(&gdb, std::fs::Permissions::from_mode(0o755)).expect("chmod");
		}

		let mut env = HashMap::new();
		env.insert("PATH".to_string(), bin.to_string_lossy().into_owned());
		assert!(
			select_attach_adapter_in_session_with_trust(&src, &repo, &env, Some("gdb"), None, false)
				.is_none(),
			"session_cwd must block sibling binaries without a .git marker"
		);
		let program = src.join("main.c");
		assert!(
			matches!(
				select_launch_adapter_in_session_with_trust(
					&program,
					&repo,
					&src,
					&env,
					Some("gdb"),
					LaunchProgramKind::File,
					false,
				),
				LaunchAdapterSelection::Unavailable { .. }
			),
			"launch must refuse the sibling checkout binary"
		);

		env.insert("PATH".to_string(), external_bin.to_string_lossy().into_owned());
		let adapter =
			select_attach_adapter_in_session_with_trust(&src, &repo, &env, Some("gdb"), None, false)
				.expect("an external absolute PATH binary still resolves");
		assert_eq!(adapter.resolved_command, external_bin.join("gdb").to_string_lossy().into_owned());
	}

	#[test]
	fn embedded_defaults_cover_all_fourteen_adapters() {
		let configs = defaults();
		let names: Vec<&str> = configs.names();
		assert_eq!(names.len(), 14);
		for name in [
			"gdb",
			"lldb-dap",
			"codelldb",
			"debugpy",
			"dlv",
			"js-debug-adapter",
			"netcoredbg",
			"kotlin-debug-adapter",
			"rdbg",
			"php-debug-adapter",
			"bash-debug-adapter",
			"dart-debug-adapter",
			"flutter-debug-adapter",
			"elixir-ls-debugger",
		] {
			assert!(names.contains(&name), "missing adapter {name}");
		}
	}

	#[test]
	fn yaml_subset_parses_adapter_overlay() {
		let parsed = parse_yaml_subset(
			r#"
adapters:
  custom:
    command: sh
    args:
      - -c
      - echo
    fileTypes: [".txt"]
    launchDefaults:
      request: launch
      stopOnEntry: true
"#,
		)
		.expect("yaml parses");
		let custom = &parsed["adapters"]["custom"];
		assert_eq!(custom["command"], "sh");
		assert_eq!(custom["fileTypes"][0], ".txt");
		assert_eq!(custom["launchDefaults"]["stopOnEntry"], true);
	}

	#[test]
	fn adapter_unavailable_names_the_missing_command() {
		let cwd = std::env::temp_dir();
		let message = adapter_unavailable_message("gdb", "gdb", &cwd, &env_with(""));
		assert!(message.contains("command -v gdb"), "{message}");
		assert!(message.contains("not available"), "{message}");
	}

	#[test]
	fn port_placeholder_is_substituted() {
		let args = vec!["--port".to_string(), "${port}".to_string()];
		assert_eq!(substitute_port(&args, 4242), vec!["--port", "4242"]);
	}
}
