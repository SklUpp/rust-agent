//! Named profile selection and `--alias` shell shortcuts.
//!
//! Mirrors `packages/utils/src/dirs.ts` (`normalizeProfileName`,
//! `resolveProfileEnv`, `setProfile`) and `packages/coding-agent/src/cli/profile-alias.ts`.
//! Profile resolution runs before config, sessions, completions, or the agent
//! runtime, so every `dirs::agent_dir()` consumer sees the selected profile.
//!
//! XDG pin rules for the consumed categories live in `dirs::agent_subdir`
//! (sessions = data, terminal-session state = state). Config/auth roots remain
//! under the agent dir because `DirResolver.configRoot`/`agentDir` do too.

use std::env;
use std::fs;
use std::path::PathBuf;

use crate::{Error, Result, dirs};

/// Profile selectors, canonical first.
pub const PROFILE_ENV_KEYS: [&str; 2] = ["OMP_PROFILE", "PI_PROFILE"];

/// Flags whose following token is always their value, even when it starts with
/// `-`. Mirrors `STRING_SETTERS` in `cli/flag-tables.ts`, reduced to the flags
/// this binary defines (`crates/aragent/src/args.rs`).
const STRING_VALUE_FLAGS: [&str; 17] = [
	"--cwd",
	"--model",
	"--provider",
	"--api-key",
	"--system-prompt",
	"--append-system-prompt",
	"--max-time",
	"--tools",
	"--approval-mode",
	"--allow",
	"--config",
	"--fork",
	"--provider-session-id",
	"--prompt-cache-key",
	"--session-dir",
	"--session-id",
	"--skills",
];

/// Optional-value flags: consume the next token only when it is value-like.
/// Mirrors `OPTIONAL_FLAGS` (`--resume`, `-r`, `--session`).
const OPTIONAL_VALUE_FLAGS: [&str; 3] = ["--resume", "-r", "--session"];

/// First residual token naming a subcommand stops global extraction, matching
/// `isSubcommand` in `profile-bootstrap.ts`. Must list every `args::Command`
/// variant; `subcommand_gate_stops_extraction_for_every_clap_subcommand`
/// enforces that through the extraction boundary, and `args` checks its upstream
/// verb refusal tables against this list.
pub(crate) const SUBCOMMANDS: [&str; 25] = [
	"acp",
	"repl",
	"completions",
	"__complete",
	"agents",
	"models",
	"memory",
	"read",
	"grep",
	"config",
	"ps",
	"worktree",
	"wt",
	"search",
	"q",
	"export",
	"stats",
	"render",
	"cleanse",
	"commit",
	"ttsr",
	"gallery",
	"grievances",
	"gc",
	"setup",
];

/// Internal marker inserted when stripping `--profile`/`--alias` would let the
/// following value-like token become an optional flag's value. `args.rs`
/// declares it as a hidden boolean, so clap leaves it unset and keeps the
/// following token positional (`--resume --profile work` → `--resume work`
/// would otherwise resume session "work").
const PROFILE_BOOTSTRAP_BOUNDARY_ARG: &str = "--arag-profile-boundary";

/// Residual argv plus the global flags cracked out before clap runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bootstrap {
	pub argv: Vec<std::ffi::OsString>,
	pub profile: Option<String>,
	pub alias: Option<String>,
}

fn os_text(arg: &std::ffi::OsStr) -> Option<&str> {
	arg.to_str()
}

/// Strip global `--profile` / `--alias` before clap parses argv, preserving the
/// value-consumption contract of `packages/coding-agent/src/cli/profile-bootstrap.ts`:
/// known string flags consume their successor even when it looks like a flag
/// (`--system-prompt --profile` keeps the literal), optional flags consume only
/// value-like successors, `--` forwards everything verbatim, and the first
/// non-launch subcommand token ends extraction.
pub fn extract_cli_flags(argv: Vec<std::ffi::OsString>) -> Result<Bootstrap> {
	let mut stripped: Vec<std::ffi::OsString> = Vec::with_capacity(argv.len());
	let mut profile = None;
	let mut alias = None;
	let mut pass_through = false;
	let mut saw_subcommand = false;
	let mut can_dispatch_subcommand = true;
	let mut boundary_before_next_value = false;
	let mut index = 0;

	while index < argv.len() {
		let arg = &argv[index];
		if pass_through || saw_subcommand {
			stripped.push(arg.clone());
			index += 1;
			continue;
		}
		if index == 0 {
			stripped.push(arg.clone());
			index += 1;
			continue;
		}
		let Some(text) = os_text(arg) else {
			can_dispatch_subcommand = false;
			stripped.push(arg.clone());
			index += 1;
			continue;
		};
		if boundary_before_next_value {
			if !text.starts_with('-') {
				stripped.push(PROFILE_BOOTSTRAP_BOUNDARY_ARG.into());
			}
			boundary_before_next_value = false;
		}
		if text == "--" {
			pass_through = true;
			stripped.push(arg.clone());
			index += 1;
			continue;
		}
		if text == "--profile" || text.starts_with("--profile=") {
			let value = if let Some(value) = text.strip_prefix("--profile=") {
				if value.is_empty() {
					return Err(Error::new("--profile requires a profile name"));
				}
				value.to_string()
			} else {
				match argv.get(index + 1).and_then(|next| os_text(next)) {
					Some(value) if !value.is_empty() && !value.starts_with('-') => value.to_string(),
					_ => return Err(Error::new("--profile requires a profile name")),
				}
			};
			profile = Some(value);
			boundary_before_next_value = needs_boundary(&stripped);
			index += if text.starts_with("--profile=") { 1 } else { 2 };
			continue;
		}
		if text == "--alias" || text.starts_with("--alias=") {
			let value = if let Some(value) = text.strip_prefix("--alias=") {
				if value.is_empty() {
					return Err(Error::new("--alias requires a command name"));
				}
				value.to_string()
			} else {
				match argv.get(index + 1).and_then(|next| os_text(next)) {
					Some(value) if !value.is_empty() && !value.starts_with('-') => value.to_string(),
					_ => return Err(Error::new("--alias requires a command name")),
				}
			};
			alias = Some(value);
			boundary_before_next_value = needs_boundary(&stripped);
			index += if text.starts_with("--alias=") { 1 } else { 2 };
			continue;
		}
		if OPTIONAL_VALUE_FLAGS.contains(&text) {
			can_dispatch_subcommand = false;
			stripped.push(arg.clone());
			if let Some(next) = argv.get(index + 1).and_then(|next| os_text(next))
				&& !next.starts_with('-')
			{
				stripped.push(argv[index + 1].clone());
				index += 2;
			} else {
				index += 1;
			}
			continue;
		}
		if STRING_VALUE_FLAGS.contains(&text) {
			can_dispatch_subcommand = false;
			if let Some(next) = argv.get(index + 1) {
				// clap would reject a flag-looking value for a separate token;
				// `--flag=value` preserves the TS "string flags always consume"
				// contract (`--system-prompt --profile` is a literal).
				stripped.push(if os_text(next).is_some_and(|next| next.starts_with('-')) {
					let mut combined = std::ffi::OsString::from(text);
					combined.push("=");
					combined.push(next);
					combined
				} else {
					arg.clone()
				});
				if !os_text(next).is_some_and(|next| next.starts_with('-')) {
					stripped.push(next.clone());
				}
				index += 2;
			} else {
				stripped.push(arg.clone());
				index += 1;
			}
			continue;
		}
		// Unknown bare long option: mirror extension-flag consumption, i.e.
		// protect a value-like successor but leave flag-looking tokens for the
		// extraction loop.
		if text.starts_with("--") {
			can_dispatch_subcommand = false;
			stripped.push(arg.clone());
			if let Some(next) = argv.get(index + 1).and_then(|next| os_text(next))
				&& !next.starts_with('-')
			{
				stripped.push(argv[index + 1].clone());
				index += 2;
			} else {
				index += 1;
			}
			continue;
		}
		if can_dispatch_subcommand && SUBCOMMANDS.contains(&text) {
			saw_subcommand = true;
		}
		can_dispatch_subcommand = false;
		stripped.push(arg.clone());
		index += 1;
	}

	Ok(Bootstrap { argv: stripped, profile, alias })
}

fn needs_boundary(stripped: &[std::ffi::OsString]) -> bool {
	stripped
		.last()
		.and_then(|last| os_text(last))
		.is_some_and(|last| OPTIONAL_VALUE_FLAGS.contains(&last))
}

/// Named aliases a shell refuses to treat as commands.
const POSIX_RESERVED: [&str; 17] = [
	"case", "coproc", "do", "done", "elif", "else", "esac", "fi", "for", "function", "if", "in",
	"select", "then", "time", "until", "while",
];
const FISH_RESERVED: [&str; 18] = [
	"and", "begin", "break", "builtin", "case", "command", "continue", "else", "end", "exec",
	"for", "function", "if", "not", "or", "return", "switch", "while",
];
const POWERSHELL_RESERVED: [&str; 33] = [
	"begin", "break", "catch", "class", "continue", "data", "do", "dynamicparam", "else",
	"elseif", "end", "enum", "exit", "filter", "finally", "for", "foreach", "from", "function",
	"if", "in", "param", "process", "return", "switch", "throw", "trap", "try", "until", "using",
	"var", "while", "workflow",
];

/// Normalize and validate a profile name. `None` means the implicit default
/// profile: empty/whitespace and the explicit `default` sentinel.
pub fn normalize(raw: Option<&str>) -> Result<Option<String>> {
	let Some(value) = raw else {
		return Ok(None);
	};
	let normalized = value.trim();
	if normalized.is_empty() || normalized == "default" {
		return Ok(None);
	}
	let first = normalized.as_bytes()[0];
	let valid_first = first.is_ascii_lowercase() || first.is_ascii_digit();
	let valid_body = normalized.len() <= 64
		&& valid_first
		&& normalized
			.bytes()
			.skip(1)
			.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
	if !valid_body
		|| normalized == "."
		|| normalized == ".."
		|| normalized.ends_with('.')
		|| is_windows_reserved(normalized)
	{
		return Err(Error::new(format!(
			"Invalid Aragent profile \"{value}\". Profile names must match \
			 ^[a-z0-9][a-z0-9._-]{{0,63}}$, cannot be \".\" or \"..\", cannot end with \".\", and \
			 cannot be a Windows reserved device name (CON, PRN, AUX, NUL, COM0-9, LPT0-9, or any \
			 of those with an extension)."
		)));
	}
	Ok(Some(normalized.to_string()))
}

fn is_windows_reserved(value: &str) -> bool {
	let base = value.split('.').next().unwrap_or(value).to_ascii_uppercase();
	matches!(base.as_str(), "CON" | "PRN" | "AUX" | "NUL")
		|| (base.len() == 4
			&& (base.starts_with("COM") || base.starts_with("LPT"))
			&& base.as_bytes()[3].is_ascii_digit())
}

/// `OMP_PROFILE` (primary) wins over `PI_PROFILE` (fallback), including when
/// explicitly empty. The variable names stay: they are existing user state.
pub fn resolve_env(primary: Option<&str>, fallback: Option<&str>) -> Result<Option<String>> {
	normalize(if primary.is_some() { primary } else { fallback })
}

fn env_value(key: &str) -> Option<String> {
	env::var(key).ok()
}

/// Safe module-load equivalent: invalid env falls back to the default profile
/// so a bare library call never panics before CLI validation.
#[must_use]
pub fn active_from_env() -> Option<String> {
	let primary = env_value(PROFILE_ENV_KEYS[0]);
	let fallback = env_value(PROFILE_ENV_KEYS[1]);
	resolve_env(primary.as_deref(), fallback.as_deref()).ok().flatten()
}

/// Resolve the selected profile: an explicit CLI `--profile` (including
/// `default`, which overrides env) else the environment chain.
pub fn resolve_cli(raw: Option<&str>) -> Result<Option<String>> {
	match raw {
		Some(value) => normalize(Some(value)),
		None => resolve_env(
			env_value(PROFILE_ENV_KEYS[0]).as_deref(),
			env_value(PROFILE_ENV_KEYS[1]).as_deref(),
		),
	}
}

/// Apply a resolved profile to the process environment. `None` clears both
/// selectors; an explicit non-profile `PI_CODING_AGENT_DIR` baseline is left
/// untouched, while a profile-derived value is dropped so the default profile
/// cannot inherit a named profile's agent dir (`setProfile(undefined)`).
pub fn activate(profile: Option<&str>) {
	match profile {
		Some(name) => {
			// SAFETY: startup is single-threaded before the agent runtime exists.
			unsafe {
				env::set_var(PROFILE_ENV_KEYS[0], name);
				env::set_var(PROFILE_ENV_KEYS[1], name);
				env::set_var("PI_CODING_AGENT_DIR", dirs::profile_agent_dir(name));
			}
		},
		None => {
			let derived_override = env_value("PI_CODING_AGENT_DIR").is_some_and(|dir| {
				env_value("PI_PROFILE").is_some_and(|legacy| {
					normalize(Some(legacy.as_str()))
						.ok()
						.flatten()
						.is_some_and(|name| dirs::profile_agent_dir(&name) == PathBuf::from(dir.as_str()))
				})
			});
			// SAFETY: startup is single-threaded before the agent runtime exists.
			unsafe {
				for key in PROFILE_ENV_KEYS {
					env::remove_var(key);
				}
				if derived_override {
					env::remove_var("PI_CODING_AGENT_DIR");
				}
			}
		},
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasInstall {
	pub alias_name: String,
	pub profile: String,
	pub config_path: PathBuf,
	pub reloaded_with: String,
}

/// Create or replace a managed shell function that runs `arag --profile=<name>`.
pub fn install_alias(alias_name: &str, profile: Option<&str>) -> Result<AliasInstall> {
	let Some(profile) = profile else {
		return Err(Error::new("--alias requires --profile <name> or OMP_PROFILE"));
	};
	let shell = detect_shell()?;
	let alias = validate_alias_name(alias_name, shell)?;
	let home = dirs::home_dir()
		.ok_or_else(|| Error::new("--alias requires HOME to resolve the shell config file"))?;
	let config_path = shell_config_path(shell, &home)?;
	let legacy_fish_path = (shell == Shell::Fish).then(|| fish_conf_dir(&home).join("omp-profiles.fish"));
	let block = render_alias_block(shell, &alias, profile);
	let current = match fs::read_to_string(&config_path) {
		Ok(text) => text,
		Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
		Err(err) => {
			return Err(Error::with_source(
				format!("cannot read {}", config_path.display()),
				err,
			));
		},
	};
	let mut updated = migrate_legacy_blocks(&current, shell)?;
	let mut legacy_retirement: Option<(String, Vec<LegacyAliasBlock>)> = None;
	if let Some(legacy_path) = &legacy_fish_path {
		// The old `omp-profiles.fish` drop-in is loaded after `arag-profiles.fish`
		// (alphabetical `*.fish` order), so its validated aliases move here.
		match fs::read_to_string(legacy_path) {
			Ok(legacy) => {
				let blocks = parse_legacy_blocks(&legacy, Shell::Fish)?;
				for legacy_block in &blocks {
					let migrated =
						render_alias_block(Shell::Fish, &legacy_block.alias, &legacy_block.profile);
					updated = upsert_block(&updated, &legacy_block.alias, &migrated)?;
				}
				legacy_retirement = Some((legacy, blocks));
			},
			Err(err) if err.kind() == std::io::ErrorKind::NotFound => {},
			Err(err) => {
				return Err(Error::with_source(
					format!("cannot read {}", legacy_path.display()),
					err,
				));
			},
		}
	}
	// Every legacy file was read and validated above; writes start only now.
	let updated = upsert_block(&updated, &alias, &block)?;
	if let Some(parent) = config_path.parent() {
		fs::create_dir_all(parent).map_err(|err| {
			Error::with_source(format!("cannot create {}", parent.display()), err)
		})?;
	}
	fs::write(&config_path, updated)
		.map_err(|err| Error::with_source(format!("cannot write {}", config_path.display()), err))?;
	if let (Some(legacy_path), Some((validated, blocks))) = (&legacy_fish_path, &legacy_retirement) {
		retire_legacy_alias_file(legacy_path, validated, blocks)?;
	}
	let quoted = format!("'{}'", config_path.display().to_string().replace('\'', "'\"'\"'"));
	let reloaded_with = match shell {
		Shell::PowerShell | Shell::Pwsh => {
			format!(". '{}'", config_path.display().to_string().replace('\'', "''"))
		},
		Shell::Fish => format!("source {quoted}"),
		_ => format!(". {quoted}"),
	};
	Ok(AliasInstall {
		alias_name: alias,
		profile: profile.to_string(),
		config_path,
		reloaded_with,
	})
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shell {
	Bash,
	Zsh,
	Fish,
	Pwsh,
	PowerShell,
}

fn detect_shell() -> Result<Shell> {
	let shell_path = env_value("SHELL").unwrap_or_default();
	let name = PathBuf::from(&shell_path)
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("")
		.trim_end_matches(".exe")
		.to_ascii_lowercase();
	match name.as_str() {
		"bash" => Ok(Shell::Bash),
		"zsh" => Ok(Shell::Zsh),
		"fish" => Ok(Shell::Fish),
		"pwsh" => Ok(Shell::Pwsh),
		"powershell" => Ok(Shell::PowerShell),
		_ if cfg!(windows) => {
			// `detectWindowsPowerShell`: SHELL is normally unset on Windows.
			let module_path = env_value("PSModulePath")
				.or_else(|| env_value("PSMODULEPATH"))
				.or_else(|| env_value("psmodulepath"))
				.unwrap_or_default()
				.to_ascii_lowercase();
			let pwsh = module_path.contains("\\powershell\\")
				|| module_path.contains("/powershell/")
				|| env_value("POWERSHELL_DISTRIBUTION_CHANNEL").is_some_and(|value| !value.is_empty());
			Ok(if pwsh { Shell::Pwsh } else { Shell::PowerShell })
		},
		_ => Err(Error::new(if name.is_empty() {
			"Unsupported shell. Supported shells: bash, zsh, fish, PowerShell.".to_string()
		} else {
			format!(
				"Unsupported shell \"{name}\". Supported shells: bash, zsh, fish, PowerShell."
			)
		})),
	}
}

fn validate_alias_name(raw: &str, shell: Shell) -> Result<String> {
	let normalized = raw.trim();
	let valid = normalized.len() <= 64
		&& normalized
			.bytes()
			.next()
			.is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
		&& normalized
			.bytes()
			.skip(1)
			.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
	if !valid {
		return Err(Error::new(format!(
			"Invalid alias \"{raw}\". Alias names must match ^[A-Za-z_][A-Za-z0-9_-]{{0,63}}$."
		)));
	}
	let lower = normalized.to_ascii_lowercase();
	if lower == "arag" {
		return Err(Error::new("Invalid alias \"arag\". Refusing to shadow the base arag command."));
	}
	let reserved: &[&str] = match shell {
		Shell::Bash | Shell::Zsh => &POSIX_RESERVED,
		Shell::Fish => &FISH_RESERVED,
		Shell::Pwsh | Shell::PowerShell => &POWERSHELL_RESERVED,
	};
	if reserved.contains(&lower.as_str()) {
		let shell_name = match shell {
			Shell::Bash => "bash",
			Shell::Zsh => "zsh",
			Shell::Fish => "fish",
			Shell::Pwsh => "pwsh",
			Shell::PowerShell => "powershell",
		};
		return Err(Error::new(format!(
			"Invalid alias \"{raw}\". Refusing to create a {shell_name} reserved word."
		)));
	}
	Ok(normalized.to_string())
}

/// `$XDG_CONFIG_HOME/fish/conf.d` (or `~/.config/fish/conf.d`), where fish
/// auto-loads every `*.fish` in filename order.
fn fish_conf_dir(home: &std::path::Path) -> PathBuf {
	env_value("XDG_CONFIG_HOME")
		.filter(|dir| !dir.is_empty())
		.map_or_else(|| home.join(".config"), PathBuf::from)
		.join("fish/conf.d")
}

fn shell_config_path(shell: Shell, home: &std::path::Path) -> Result<PathBuf> {
	let path = match shell {
		Shell::Zsh => env_value("ZDOTDIR")
			.filter(|dir| !dir.is_empty())
			.map_or_else(|| home.join(".zshrc"), |dir| PathBuf::from(dir).join(".zshrc")),
		Shell::Bash if cfg!(target_os = "macos") => home.join(".bash_profile"),
		Shell::Bash => home.join(".bashrc"),
		Shell::Fish => fish_conf_dir(home).join("arag-profiles.fish"),
		Shell::Pwsh if cfg!(windows) => {
			home.join("Documents/PowerShell/Microsoft.PowerShell_profile.ps1")
		},
		Shell::Pwsh => home.join(".config/powershell/Microsoft.PowerShell_profile.ps1"),
		Shell::PowerShell => {
			home.join("Documents/WindowsPowerShell/Microsoft.PowerShell_profile.ps1")
		},
	};
	if path.as_os_str().is_empty() {
		return Err(Error::new("cannot resolve the shell config file"));
	}
	Ok(path)
}

/// Marker word current builds write. Earlier builds wrote `omp`; those blocks
/// invoke the removed binary, so they are migrated rather than duplicated.
const CURRENT_ALIAS_MARKER: &str = "arag";
/// Marker word written before the rename.
const LEGACY_ALIAS_MARKER: &str = "omp";

fn alias_block_start(marker: &str) -> String {
	format!("# >>> {marker} profile alias: ")
}

fn alias_block_markers(marker: &str, alias: &str) -> (String, String) {
	(
		format!("# >>> {marker} profile alias: {alias} >>>"),
		format!("# <<< {marker} profile alias: {alias} <<<"),
	)
}

fn render_alias_block(shell: Shell, alias: &str, profile: &str) -> String {
	let (start, end) = alias_block_markers(CURRENT_ALIAS_MARKER, alias);
	let body = match shell {
		Shell::Fish => format!(
			"function {alias} --wraps arag --description 'Aragent profile {profile}'\n\
			     command arag --profile={profile} $argv\n\
			 end"
		),
		Shell::Pwsh | Shell::PowerShell => {
			format!("function {alias} {{\n    & arag --profile={profile} @args\n}}")
		},
		Shell::Bash | Shell::Zsh => {
			format!("{alias}() {{\n    command arag --profile={profile} \"$@\"\n}}")
		},
	};
	format!("{start}\n{body}\n{end}")
}

/// Exact block the pre-rename binary wrote. Migration compares against this,
/// so any user edit (extra command, flag, comment, changed profile) is refused
/// instead of being rewritten.
fn render_legacy_alias_block(shell: Shell, alias: &str, profile: &str) -> String {
	let (start, end) = alias_block_markers(LEGACY_ALIAS_MARKER, alias);
	let body = match shell {
		Shell::Fish => format!(
			"function {alias} --wraps omp --description 'OMP profile {profile}'\n\
			     command omp --profile={profile} $argv\n\
			 end"
		),
		Shell::Pwsh | Shell::PowerShell => {
			format!("function {alias} {{\n    & omp --profile={profile} @args\n}}")
		},
		Shell::Bash | Shell::Zsh => {
			format!("{alias}() {{\n    command omp --profile={profile} \"$@\"\n}}")
		},
	};
	format!("{start}\n{body}\n{end}")
}

/// One validated pre-rename managed block, as byte offsets into its source.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LegacyAliasBlock {
	alias: String,
	profile: String,
	start: usize,
	end: usize,
}

fn legacy_block_profile(block: &str) -> Option<&str> {
	let marker = "--profile=";
	let start = block.find(marker)? + marker.len();
	let tail = &block[start..];
	let end = tail
		.find(|ch: char| ch.is_whitespace() || matches!(ch, '"' | '\'' | '$' | '@'))
		.unwrap_or(tail.len());
	Some(&tail[..end])
}

fn edited_legacy_block_error(alias: Option<&str>) -> Error {
	let subject = alias.map_or_else(
		|| "a pre-rename profile alias block".to_string(),
		|alias| format!("the pre-rename profile alias block for \"{alias}\""),
	);
	Error::new(format!(
		"Found {subject} that no longer matches what the old binary generated, so nothing was \
		 written: rewriting it would drop user edits (commands, flags, comments). Restore the \
		 generated block, or delete its `# >>> omp profile alias:` and `# <<< omp profile alias:` \
		 marker lines, then rerun --alias."
	))
}

/// Parse every pre-rename managed block, refusing anything that is not the
/// exact generated form. Ambiguity is an error: a migration that guesses would
/// risk a user's commands, profile selector or surrounding content.
fn parse_legacy_blocks(content: &str, shell: Shell) -> Result<Vec<LegacyAliasBlock>> {
	let start_prefix = alias_block_start(LEGACY_ALIAS_MARKER);
	let mut blocks = Vec::new();
	let mut cursor = 0usize;
	while let Some(rel) = content[cursor..].find(&start_prefix) {
		let start = cursor + rel;
		let alias_start = start + start_prefix.len();
		let Some(alias_rel) = content[alias_start..].find(" >>>") else {
			return Err(edited_legacy_block_error(None));
		};
		let alias = &content[alias_start..alias_start + alias_rel];
		let (_, end_marker) = alias_block_markers(LEGACY_ALIAS_MARKER, alias);
		let Some(end_rel) = content[alias_start..].find(&end_marker) else {
			return Err(edited_legacy_block_error(Some(alias)));
		};
		let end = alias_start + end_rel + end_marker.len();
		let block = &content[start..end];
		let Some(profile) = legacy_block_profile(block) else {
			return Err(edited_legacy_block_error(Some(alias)));
		};
		if normalize(Some(profile)).ok().flatten().as_deref() != Some(profile)
			|| render_legacy_alias_block(shell, alias, profile) != block
		{
			return Err(edited_legacy_block_error(Some(alias)));
		}
		blocks.push(LegacyAliasBlock {
			alias: alias.to_string(),
			profile: profile.to_string(),
			start,
			end,
		});
		cursor = end;
	}
	Ok(blocks)
}

/// Rewrite validated pre-rename blocks in place. Unrelated shell content
/// between them is copied byte-for-byte; a single non-canonical block aborts
/// the whole migration before the caller writes anything.
fn migrate_legacy_blocks(content: &str, shell: Shell) -> Result<String> {
	let blocks = parse_legacy_blocks(content, shell)?;
	if blocks.is_empty() {
		return Ok(content.to_string());
	}
	let mut out = String::with_capacity(content.len());
	let mut cursor = 0usize;
	for block in &blocks {
		out.push_str(&content[cursor..block.start]);
		out.push_str(&render_alias_block(shell, &block.alias, &block.profile));
		cursor = block.end;
	}
	out.push_str(&content[cursor..]);
	Ok(out)
}

/// Remove exactly the blocks returned by [`parse_legacy_blocks`]. The caller
/// only ever passes blocks it already copied to the current file.
fn strip_legacy_blocks(content: &str, blocks: &[LegacyAliasBlock]) -> String {
	let mut out = String::with_capacity(content.len());
	let mut cursor = 0usize;
	for block in blocks {
		out.push_str(&content[cursor..block.start]);
		cursor = block.end;
	}
	out.push_str(&content[cursor..]);
	out
}

/// Retire the pre-rename fish drop-in after its validated aliases moved to the
/// current file: strip those blocks, keep the file and every unrelated line.
/// The caller writes the current file first. If the drop-in changed between
/// validation and this call it is left untouched: duplicates are safe, a
/// guessed rewrite is not.
fn retire_legacy_alias_file(
	path: &std::path::Path,
	validated_text: &str,
	blocks: &[LegacyAliasBlock],
) -> Result<()> {
	match fs::read_to_string(path) {
		Ok(current) if current == validated_text => {
			let remaining = strip_legacy_blocks(&current, blocks);
			if remaining == current {
				Ok(())
			} else {
				fs::write(path, remaining)
					.map_err(|err| Error::with_source(format!("cannot rewrite {}", path.display()), err))
			}
		},
		Ok(_) => Ok(()),
		Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(err) => Err(Error::with_source(format!("cannot read {}", path.display()), err)),
	}
}

fn upsert_block(content: &str, alias: &str, block: &str) -> Result<String> {
	let (start, end) = alias_block_markers(CURRENT_ALIAS_MARKER, alias);
	if let Some(start_index) = content.find(&start) {
		let Some(end_index) = content[start_index + start.len()..].find(&end) else {
			return Err(Error::new(format!(
				"Found \"{start}\" without a matching \"{end}\" in the shell config. The managed \
				 alias block is malformed; remove the stale marker line and rerun --alias."
			)));
		};
		let end_index = start_index + start.len() + end_index;
		let after_end = end_index + end.len();
		let prefix = content[..start_index]
			.trim_end_matches(|c| matches!(c, ' ' | '\t' | '\n'));
		let suffix = content[after_end..].trim_start_matches('\n');
		let parts: Vec<&str> = [prefix, block, suffix]
			.into_iter()
			.filter(|part| !part.is_empty())
			.collect();
		let joined = parts.join("\n\n");
		return Ok(format!("{}\n", joined.trim_end_matches('\n')));
	}
	let trimmed = content.trim_end_matches(char::is_whitespace);
	Ok(if trimmed.is_empty() {
		format!("{block}\n")
	} else {
		format!("{trimmed}\n\n{block}\n")
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn normalization_matches_typescript_rules() {
		assert_eq!(normalize(None).unwrap(), None);
		assert_eq!(normalize(Some("")).unwrap(), None);
		assert_eq!(normalize(Some("  ")).unwrap(), None);
		assert_eq!(normalize(Some(" default ")).unwrap(), None);
		assert_eq!(normalize(Some("work-1._x")).unwrap().as_deref(), Some("work-1._x"));
		for invalid in [".", "..", "Work", "-work", "work.", "con", "NUL.txt", "com7", "lpt0.x", "a/b", "../x"] {
			assert!(normalize(Some(invalid)).is_err(), "{invalid} must fail");
		}
		assert_eq!(normalize(Some("a".repeat(64).as_str())).unwrap().as_deref(), Some("a".repeat(64).as_str()));
		assert!(normalize(Some("a".repeat(65).as_str())).is_err());
	}

	#[test]
	fn preparser_matches_the_typescript_value_consumption_contract() {
		fn extract(args: &[&str]) -> Result<Bootstrap> {
			extract_cli_flags(args.iter().map(std::ffi::OsString::from).collect())
		}
		fn argv(result: &Bootstrap) -> Vec<String> {
			result.argv.iter().map(|arg| arg.to_string_lossy().into_owned()).collect()
		}

		let flags = extract(&["arag", "--profile", "work", "--alias", "wk", "hello"]).expect("flags");
		assert_eq!(flags.profile.as_deref(), Some("work"));
		assert_eq!(flags.alias.as_deref(), Some("wk"));
		assert_eq!(argv(&flags), ["arag", "hello"]);

		let equals = extract(&["arag", "--profile=work", "hi"]).expect("equals");
		assert_eq!(equals.profile.as_deref(), Some("work"));
		assert_eq!(argv(&equals), ["arag", "hi"]);

		for args in [
			&["arag", "--profile"][..],
			&["arag", "--profile="],
			&["arag", "--profile", "--print"],
			&["arag", "--alias"],
			&["arag", "--alias="],
			&["arag", "--alias", "--print"],
		] {
			assert!(extract(args).is_err(), "{args:?} must fail");
		}

		// A known string flag consumes a flag-looking successor; the value is
		// rewritten as `--flag=value` so clap cannot reinterpret it.
		let literal = extract(&["arag", "--system-prompt", "--profile", "work"]).expect("literal");
		assert_eq!(literal.profile, None);
		assert_eq!(argv(&literal), ["arag", "--system-prompt=--profile", "work"]);

		// Value-less flags leave a following profile active.
		let trailing = extract(&["arag", "--print", "--profile", "work"]).expect("trailing");
		assert_eq!(trailing.profile.as_deref(), Some("work"));
		assert_eq!(argv(&trailing), ["arag", "--print"]);

		// Optional flags get a boundary marker so the next token stays positional;
		// a trailing token is required for the marker iteration to run.
		let optional =
			extract(&["arag", "--resume", "--profile", "work", "hello"]).expect("optional");
		assert_eq!(optional.profile.as_deref(), Some("work"));
		assert_eq!(
			argv(&optional),
			["arag", "--resume", "--arag-profile-boundary", "hello"]
		);

		// A dispatched subcommand and `--` both stop global extraction.
		for args in [
			&["arag", "repl", "--profile", "work"][..],
			&["arag", "completions", "--profile", "work"],
			&["arag", "__complete", "sessions", "--profile", "work"],
			&["arag", "--", "--profile", "work"],
		] {
			let result = extract(args).expect("passthrough");
			assert_eq!(result.profile, None, "{args:?}");
			assert_eq!(argv(&result), args);
		}

		// Unknown long options protect only a value-like successor.
		let unknown = extract(&["arag", "--some-ext", "val", "--profile", "work"]).expect("unknown");
		assert_eq!(unknown.profile.as_deref(), Some("work"));
		assert_eq!(argv(&unknown), ["arag", "--some-ext", "val"]);
		let unknown_flag = extract(&["arag", "--some-ext", "--profile", "work"]).expect("unknown flag");
		assert_eq!(unknown_flag.profile.as_deref(), Some("work"));
	}

	#[test]
	fn subcommand_gate_stops_extraction_for_every_clap_subcommand() {
		use clap::CommandFactory as _;
		let subcommands: Vec<String> = crate::args::Cli::command()
			.get_subcommands()
			.map(|command| command.get_name().to_string())
			.collect();
		assert!(!subcommands.is_empty());
		for name in subcommands {
			for flag in [&["--alias", "wk"][..], &["--profile", "work"]] {
				let args = ["arag", name.as_str(), flag[0], flag[1]];
				let result =
					extract_cli_flags(args.iter().map(std::ffi::OsString::from).collect())
						.expect("passthrough");
				assert_eq!(result.profile, None, "{args:?}");
				assert_eq!(result.alias, None, "{args:?}");
				let argv: Vec<String> =
					result.argv.iter().map(|arg| arg.to_string_lossy().into_owned()).collect();
				assert_eq!(argv, args, "{args:?}");
			}
		}
	}

	#[test]
	fn profile_env_precedence_holds_even_when_empty() {
		assert_eq!(resolve_env(Some("work"), Some("other")).unwrap().as_deref(), Some("work"));
		assert_eq!(resolve_env(Some(""), Some("other")).unwrap(), None);
		assert_eq!(resolve_env(None, Some("other")).unwrap().as_deref(), Some("other"));
		assert!(resolve_env(Some("BAD"), None).is_err());
	}

	#[test]
	fn managed_block_is_idempotent() {
		let block = render_alias_block(Shell::Bash, "wk", "work");
		let once = upsert_block("# existing\n", "wk", &block).expect("insert");
		let twice = upsert_block(&once, "wk", &block).expect("replace");
		assert_eq!(once, twice);
		assert_eq!(once.matches("arag profile alias: wk").count(), 2);
		assert!(once.contains("--profile=work"));
	}

	#[test]
	fn canonical_legacy_blocks_migrate_per_shell() {
		for shell in [Shell::Bash, Shell::Zsh, Shell::Fish, Shell::Pwsh, Shell::PowerShell] {
			let legacy = render_legacy_alias_block(shell, "wk", "work");
			let content = format!("# keep me\nexport KEEP=1\n{legacy}\n# tail\n");
			let migrated = migrate_legacy_blocks(&content, shell).expect("canonical block");
			assert!(!migrated.contains("omp profile alias"), "{shell:?}: {migrated}");
			assert!(migrated.contains("arag profile alias: wk"), "{shell:?}: {migrated}");
			assert!(migrated.contains("--profile=work"), "{shell:?}: {migrated}");
			assert!(migrated.contains("# keep me"), "{shell:?}: {migrated}");
			assert!(migrated.contains("export KEEP=1"), "{shell:?}: {migrated}");
			assert!(migrated.contains("# tail"), "{shell:?}: {migrated}");

			// The requested alias is then replaced, not appended a second time.
			let block = render_alias_block(shell, "wk", "other");
			let updated = upsert_block(&migrated, "wk", &block).expect("replace");
			assert!(updated.contains("--profile=other"), "{shell:?}: {updated}");
			assert_eq!(updated.matches("arag profile alias: wk").count(), 2);
			assert!(updated.contains("# keep me"));
		}
	}

	#[test]
	fn legacy_fish_blocks_carry_over_and_legacy_file_is_kept() {
		let legacy = render_legacy_alias_block(Shell::Fish, "wk", "work");
		let blocks = parse_legacy_blocks(&legacy, Shell::Fish).expect("canonical block");
		assert_eq!(blocks.len(), 1);
		let current = render_alias_block(Shell::Fish, &blocks[0].alias, &blocks[0].profile);
		assert!(current.contains("command arag --profile=work"), "{current}");

		// Exactly the validated blocks are removed; the caller keeps the file.
		let stripped = strip_legacy_blocks(&legacy, &blocks);
		assert!(stripped.trim().is_empty(), "{stripped}");

		// Unrelated content survives byte-for-byte around the removed block.
		let mixed = format!("set -gx KEEP 1\n{legacy}");
		let mixed_blocks = parse_legacy_blocks(&mixed, Shell::Fish).expect("canonical block");
		let remaining = strip_legacy_blocks(&mixed, &mixed_blocks);
		assert!(remaining.contains("set -gx KEEP 1"), "{remaining}");
		assert!(!remaining.contains("profile alias"), "{remaining}");
	}

	#[test]
	fn edited_legacy_blocks_are_rejected_without_rewrite() {
		let canonical = render_legacy_alias_block(Shell::Bash, "wk", "work");

		// A user command added inside the block is not guessed away.
		let with_extra = canonical.replace(
			"    command omp --profile=work",
			"    export OMP_HOME=~/.omp\n    command omp --profile=work",
		);
		let error = migrate_legacy_blocks(&with_extra, Shell::Bash).expect_err("hand-edited");
		let message = error.to_string();
		assert!(message.contains("wk"), "{message}");
		assert!(message.contains("no longer matches"), "{message}");
		assert!(message.contains("nothing was written"), "{message}");

		// Added flag, changed command, or missing end marker: refuse too.
		let extra_flag = canonical.replace("--profile=work", "--profile=work --yolo");
		assert!(parse_legacy_blocks(&extra_flag, Shell::Bash).is_err());
		let changed_command = canonical.replace("command omp", "command something-else");
		assert!(parse_legacy_blocks(&changed_command, Shell::Bash).is_err());
		let missing_end = canonical.replace("# <<< omp profile alias: wk <<<", "");
		assert!(parse_legacy_blocks(&missing_end, Shell::Bash).is_err());

		// A canonical block from another shell is not accepted as this shell's.
		let fish = render_legacy_alias_block(Shell::Fish, "wk", "work");
		assert!(parse_legacy_blocks(&fish, Shell::Bash).is_err());
	}
}
