//! `omp` binary entry point: resolve files, then hand turns to the runner.

use std::io::{self, BufRead, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use omp_cli::{
	Error, Result,
	acp,
	agent::{self, AgentRunner, ExtraTool, RunRequest},
	agents,
	approve::{ApprovalMode, Approvals},
	args::{Cli, Command, normalize_continue_session_args, parse_cli, refused_verb_message},
	autolearn::{self, LocalLearnTool, ManageSkillTool, MemoryBackend},
	catalog::Catalog,
	completions,
	config::Config,
	config_cmd,
	daemon_broker,
	deadline::{self, Deadline},
	dirs,
	file_mentions,
	foreign_session::{self, ForeignSessionSource, ForeignSessionStore},
	goal::{self, GoalMode, GoalModeState, GoalPersist, GoalRuntime, RestorePlan, TokenCounters},
	goal_tool::{GOAL_TOOL_NAME, GoalTool},
	local_cmd,
	local_memory,
	local_models,
	memory_protocol::MemoryUrlHandler,
	output::{self, OutputMode, Reporter, session_id},
	plan_yolo,
	prewalk,
	process_cli,
	profile,
	prompts::{self, Prompts},
	repl,
	rpc::{RpcMode, session::{RpcLaunch, SteerFn, SteeringMessage}},
	runtime::{self, PiAgentRunner},
	session::{
		self, ContentPart, MessageContent, MessageEntry, SessionHeader, SessionMessage,
		SessionOptions, SessionStore, TurnBuilder, TurnRecord,
	},
	session_checkpoint::CheckpointHost,
	skill_protocol::SkillUrlHandler,
	skills::{self, LoadOptions, SkillSnapshot},
	tool_commands,
	worktree_cmd,
};

struct CliError {
	message: String,
	code: u8,
}

impl CliError {
	fn fail(msg: impl Into<String>) -> Self {
		Self {
			message: msg.into(),
			code: omp_cli::EXIT_FAILURE,
		}
	}

	fn usage(msg: impl Into<String>) -> Self {
		Self {
			message: msg.into(),
			code: omp_cli::EXIT_USAGE,
		}
	}

	/// Non-zero exit whose explanation was already printed on stdout.
	fn silent(code: u8) -> Self {
		Self { message: String::new(), code }
	}
}

/// Prewalk and plan-yolo both own the one-way model switch. Reject an
/// effective pair from flags or settings before any session/provider starts.
fn reject_dual_startup_arms(
	prewalk_armed: bool,
	plan_yolo_armed: bool,
) -> std::result::Result<(), CliError> {
	if prewalk_armed && plan_yolo_armed {
		return Err(CliError::usage(
			"--plan-yolo cannot be combined with an effective prewalk arm \
			 (--prewalk, --prewalk-into, or prewalk.enabled); choose one startup switch",
		));
	}
	Ok(())
}

fn base_dir(cli: &Cli) -> Result<PathBuf> {
	match &cli.cwd {
		Some(cwd) => Ok(dirs::absolute(&std::env::current_dir()?, cwd)),
		None => Ok(std::env::current_dir()?),
	}
}

/// Map a handler's explicit exit code to the CLI result: zero succeeds, any
/// other code is silent because the handler already rendered its output.
fn local_exit(code: u8) -> std::result::Result<(), CliError> {
	if code == omp_cli::EXIT_SUCCESS {
		Ok(())
	} else {
		Err(CliError::silent(code))
	}
}

impl From<Error> for CliError {
	fn from(err: Error) -> Self {
		let msg = err.to_string();
		if msg.contains("legacy v1 session log: read-only")
			|| msg.contains("cannot append to legacy v1 session log")
		{
			Self::usage(msg)
		} else {
			Self::fail(msg)
		}
	}
}

/// Releases a loop waiting on a turn-end ack when the consumer returns early,
/// errors, or drops the stream.
struct TurnAckGuard(Arc<pi_agent::TurnAck>);

impl Drop for TurnAckGuard {
	fn drop(&mut self) {
		self.0.cancel();
	}
}

impl From<std::io::Error> for CliError {
	fn from(err: std::io::Error) -> Self {
		Self::fail(err.to_string())
	}
}

fn main() -> std::process::ExitCode {
	match run() {
		Ok(()) => std::process::ExitCode::from(omp_cli::EXIT_SUCCESS),
		Err(error) => {
			if !error.message.is_empty() {
				eprintln!("{}", error.message);
			}
			std::process::ExitCode::from(error.code)
		},
	}
}

fn run() -> std::result::Result<(), CliError> {
	// Bootstrap before clap (as `runCli` does before `parseArgs`): profile flags
	// are cracked out of raw argv with the shared value-consumption contract so
	// `--system-prompt --profile` stays literal and invalid OMP_PROFILE errors
	// even for `--help`.
	let bootstrap = profile::extract_cli_flags(std::env::args_os().collect())?;
	// A known upstream verb this binary does not run must fail here, before the
	// profile, config, session, provider, or JS runtime starts. Without this
	// guard clap accepts `omp acp` as the positional prompt "acp" and the agent
	// runtime treats a command as text, which looks like a successful launch.
	if let Some(message) = refused_verb_message(&bootstrap.argv) {
		return Err(CliError::usage(message));
	}
	let profile = profile::resolve_cli(bootstrap.profile.as_deref())?;
	profile::activate(profile.as_deref());
	// Hidden worker dispatch before clap/config/session boot: the broker worker
	// is a separate process carrying its own scope in the environment, not a
	// user CLI invocation.
	if daemon_broker::is_worker_invocation() {
		daemon_broker::run_worker().map_err(CliError::fail)?;
		return Ok(());
	}
	// The shared LSP mux is the same kind of hidden worker: broker-spawned with
	// its endpoint and project scope in the environment. Dispatch before clap,
	// config, and session boot so a worker never looks like a user command.
	if pi_agent::lsp::mux::worker::is_worker_invocation() {
		let args: Vec<std::ffi::OsString> = std::env::args_os().skip(2).collect();
		pi_agent::lsp::mux::worker::run_mux_worker_blocking(&args).map_err(CliError::fail)?;
		return Ok(());
	}
	// Share this profile's exact runtime scope with `pi-agent` broker clients.
	daemon_broker::share_runtime_root(&dirs::agent_dir());
	let mut cli = parse_cli(bootstrap.argv.clone());
	// `--continue <uuid>` names a session (TS parity), not prompt text.
	normalize_continue_session_args(&mut cli, &bootstrap.argv);
	// TS `main.ts` sets `PI_NO_PTY=1` for the process; tool children inherit it
	// through `AgentContext.env`, so their bash never selects an interactive
	// user-shell PTY. The Rust foreground bash tool has no PTY path, making the
	// environment the portable half of the contract.
	if cli.no_pty {
		// SAFETY: startup is single-threaded here; the runner and every worker
		// thread are created later.
		unsafe { std::env::set_var("PI_NO_PTY", "1") };
	}
	if let Some(alias_name) = &bootstrap.alias {
		let result = profile::install_alias(alias_name, profile.as_deref())?;
		print!(
			"Created {} for profile {} in {}\nRestart your shell or run: {}\nThen use: {} update, {} --version, or {}\n",
			result.alias_name,
			result.profile,
			result.config_path.display(),
			result.reloaded_with,
			result.alias_name,
			result.alias_name,
			result.alias_name,
		);
		return Ok(());
	}
	// TS `--export` startup fast-path: export the named session log and exit
	// before config, auth, provider, or live session startup. The first
	// positional operand is the output path (`parsedArgs.messages[0]` in TS). A
	// parsed subcommand owns its own surface (`omp export ...`), so it wins;
	// `parse_cli` has already dispatched any registered verb found behind the
	// flag (`omp --export log.jsonl launch`), leaving the log's own `.html`
	// sibling as the output.
	if let (Some(session), None) = (cli.export.as_deref(), &cli.command) {
		maybe_auto_chdir(&cli);
		let base = match &cli.cwd {
			Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
			None => std::env::current_dir()?,
		};
		let output = cli.prompt.first().map(PathBuf::from);
		let path = run_export_command(
			Some(session),
			output.as_deref(),
			&base,
			cli.session_dir.as_deref(),
		)?;
		println!("Exported to: {path}");
		return Ok(());
	}
	// `agents`, `models`, read/grep, and completion commands run before the agent
	// runtime, config, and sessions are loaded: unpacking definitions, listing
	// the catalog, or generating a script must never boot a session, read config,
	// or touch the network.
	match &cli.command {
		Some(Command::Agents { action, dir, user, project, force, json }) => {
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			let Some(action) = action.as_deref() else {
				agents::print_help()?;
				return Ok(());
			};
			if action != "unpack" {
				return Err(CliError::usage(format!(
					"Unknown agents action '{action}'. Expected: unpack"
				)));
			}
			let summary = agents::unpack(
				&base,
				&agents::UnpackOptions {
					force: *force,
					dir: dir.clone(),
					user: *user,
					project: *project,
				},
			)?;
			agents::print_unpack(&summary, *json)?;
			return Ok(());
		},
		Some(Command::Models { action, pattern, json, extension, no_extensions, config }) => {
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			// Root `--config` (before the subcommand) plus command-level overlays,
			// later files winning, like the run path.
			let request = local_models::Request {
				action: action.clone(),
				pattern: pattern.clone(),
				json: *json,
				extensions: extension.clone(),
				no_extensions: *no_extensions,
				config: cli.config.iter().chain(config).cloned().collect(),
			};
			let stdout = io::stdout();
			let mut out = io::BufWriter::new(stdout.lock());
			local_models::run(&request, &base, &mut out)
				.map_err(|err| CliError::fail(err.to_string()))?;
			return Ok(());
		},
		Some(Command::Read { path }) => {
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			let stdout = io::stdout();
			let mut out = io::BufWriter::new(stdout.lock());
			tool_commands::run_read(path.as_deref().unwrap_or(""), &base, &mut out)
				.map_err(CliError::fail)?;
			return Ok(());
		},
		Some(Command::Grep {
			pattern,
			path,
			glob,
			limit,
			context,
			files,
			count,
			no_gitignore,
		}) => {
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			let mode = tool_commands::cli_mode(*count, *files);
			let request = tool_commands::GrepRequest {
				pattern: pattern.clone().unwrap_or_default(),
				path: path.clone().unwrap_or_else(|| ".".to_string()),
				glob: glob.clone(),
				limit: *limit,
				context: *context,
				mode,
				gitignore: !*no_gitignore,
			};
			let stdout = io::stdout();
			let mut out = io::BufWriter::new(stdout.lock());
			tool_commands::run_grep(&request, &base, &mut out).map_err(CliError::fail)?;
			return Ok(());
		},
		Some(Command::Config { action, key, value, json }) => {
			let action = match action.as_deref() {
				None => config_cmd::Action::List,
				Some(raw) => config_cmd::Action::parse(raw)
					.ok_or_else(|| CliError::fail(config_cmd::unknown_action(raw).to_string()))?,
			};
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			let request = config_cmd::Request {
				action,
				key: key.clone(),
				value: if value.is_empty() { None } else { Some(value.join(" ")) },
				json: *json,
				cwd: base,
				overlays: cli.config.clone(),
			};
			let stdout = io::stdout();
			let mut out = io::BufWriter::new(stdout.lock());
			config_cmd::run(&request, &mut out).map_err(|err| CliError::fail(err.to_string()))?;
			out.flush().map_err(|err| CliError::fail(err.to_string()))?;
			return Ok(());
		},
		Some(Command::Worktree { action, all, dry_run, json }) => {
			let cwd = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			// `worktree.base` is the raw effective setting value (same layers as
			// `config get`); `resolve_base` owns getWorktreesDir semantics:
			// `OMP_WORKTREE_DIR` first, then `~` expansion + absolute
			// normalisation, then the XDG data-root default.
			let configured =
				match config_cmd::effective_setting(&cwd, &cli.config, "worktree.base")? {
					Some(serde_json::Value::String(path)) => Some(PathBuf::from(path)),
					_ => None,
				};
			let base = worktree_cmd::resolve_base(configured.as_deref()).map_err(CliError::usage)?;
			let stdout = io::stdout();
			let mut out = io::BufWriter::new(stdout.lock());
			match action.as_deref().unwrap_or("list") {
				"list" => {
					worktree_cmd::list_worktrees(&base, *json, &mut out)
						.map_err(|err| CliError::fail(err.to_string()))?;
				},
				"clear" => {
					let options =
						worktree_cmd::ClearOptions { all: *all, dry_run: *dry_run, json: *json };
					let report = worktree_cmd::clear_worktrees(&base, &options, &mut out)
						.map_err(|err| CliError::fail(err.to_string()))?;
					out.flush().map_err(|err| CliError::fail(err.to_string()))?;
					if report.exit_failure() {
						return Err(CliError::silent(omp_cli::EXIT_FAILURE));
					}
					return Ok(());
				},
				other => {
					return Err(CliError::usage(format!(
						"Unknown worktree action '{other}'. Expected: list | clear"
					)));
				},
			}
			out.flush().map_err(|err| CliError::fail(err.to_string()))?;
			return Ok(());
		},
		Some(Command::Ps {
			action, name, all, json, plain, dir, global, follow, head, lines, grep, timeout,
		}) => {
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			let request = process_cli::PsRequest {
				action: action.clone().unwrap_or_else(|| "list".to_string()),
				name: name.clone(),
				all: *all,
				json: *json,
				plain: *plain,
				dir: dir.as_ref().map(|dir| dirs::absolute(&base, dir)),
				global: global.clone(),
				follow: *follow,
				head: *head,
				lines: *lines,
				grep: grep.clone(),
				timeout: *timeout,
			};
			let stdout = io::stdout();
			let mut out = io::BufWriter::new(stdout.lock());
			return process_cli::run(&request, &mut out).map_err(CliError::fail);
		},
		Some(Command::Search { query, recency, limit }) => {
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			let config = Config::load_run(&cli.config, Some(base.as_path()))
				.map_err(|err| CliError::fail(err.to_string()))?;
			let stdout = io::stdout();
			let mut out = io::BufWriter::new(stdout.lock());
			run_search_command(
				&query.join(" "),
				recency.as_deref(),
				*limit,
				&config,
				&base,
				&mut out,
			)?;
			return Ok(());
		},
		Some(Command::Export { session, output }) => {
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			let path = run_export_command(
				session.as_deref(),
				output.as_deref(),
				&base,
				cli.session_dir.as_deref(),
			)?;
			println!("Exported to: {path}");
			return Ok(());
		},
		Some(Command::Stats { session }) => {
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			let stdout = io::stdout();
			let mut out = io::BufWriter::new(stdout.lock());
			run_stats_command(
				session.as_deref(),
				&base,
				cli.session_dir.as_deref(),
				&mut out,
			)?;
			return Ok(());
		},
		Some(Command::Render { session, output }) => {
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			run_render_command(
				session.as_deref(),
				output.as_deref(),
				&base,
				cli.session_dir.as_deref(),
			)?;
			return Ok(());
		},
		Some(Command::Cleanse { session, confirm }) => {
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			run_cleanse_command(
				session.as_deref(),
				*confirm,
				&base,
				cli.session_dir.as_deref(),
			)?;
			return Ok(());
		},
		Some(Command::Commit { message }) => {
			let base = match &cli.cwd {
				Some(cwd) => dirs::absolute(&std::env::current_dir()?, cwd),
				None => std::env::current_dir()?,
			};
			run_commit_command(message.as_deref(), &base)?;
			return Ok(());
		},
		Some(Command::Ttsr {
			action,
			snippet,
			file,
			rule,
			source,
			tool,
			path,
			verbose,
			json,
			no_gitignore,
			max_bytes,
		}) => {
			let base = base_dir(&cli)?;
			let opts = local_cmd::TtsrOptions {
				action: action.clone(),
				snippet: snippet.clone(),
				file: file.clone(),
				rule: rule.clone(),
				source: source.clone(),
				tool: tool.clone(),
				path: path.clone(),
				verbose: *verbose,
				json: *json,
				no_gitignore: *no_gitignore,
				max_bytes: *max_bytes,
			};
			let stdout = io::stdout();
			let stderr = io::stderr();
			let mut out = io::BufWriter::new(stdout.lock());
			let mut err = io::BufWriter::new(stderr.lock());
			let code = local_cmd::run_ttsr(&opts, &base, &mut out, &mut err)?;
			out.flush()?;
			err.flush()?;
			return local_exit(code);
		},
		Some(Command::Gallery { tool, state, width, expanded, plain }) => {
			let opts = local_cmd::GalleryOptions {
				tool: tool.clone(),
				states: state.clone(),
				width: *width,
				expanded: *expanded,
				plain: *plain,
			};
			let stdout = io::stdout();
			let mut out = io::BufWriter::new(stdout.lock());
			let code = local_cmd::run_gallery(&opts, &mut out)?;
			out.flush()?;
			return local_exit(code);
		},
		Some(Command::Grievances { action: _, limit, tool, json }) => {
			let opts = local_cmd::GrievancesOptions { limit: *limit, tool: tool.clone(), json: *json };
			let stdout = io::stdout();
			let mut out = io::BufWriter::new(stdout.lock());
			let code = local_cmd::run_grievances(&opts, &mut out)?;
			out.flush()?;
			return local_exit(code);
		},
		Some(Command::Gc {
			apply,
			json,
			agent_dir,
			blobs,
			archive,
			wal,
			cold_archive_after_days,
			retain_newest_global,
			retain_newest_per_cwd,
		}) => {
			let base = base_dir(&cli)?;
			let opts = local_cmd::GcOptions {
				apply: *apply,
				json: *json,
				agent_dir: agent_dir.clone(),
				blobs: *blobs,
				archive: *archive,
				wal: *wal,
				cold_archive_after_days: *cold_archive_after_days,
				retain_newest_global: *retain_newest_global,
				retain_newest_per_cwd: *retain_newest_per_cwd,
			};
			let stdout = io::stdout();
			let stderr = io::stderr();
			let mut out = io::BufWriter::new(stdout.lock());
			let mut err = io::BufWriter::new(stderr.lock());
			let code = local_cmd::run_gc(&opts, &base, &mut out, &mut err)?;
			out.flush()?;
			err.flush()?;
			return local_exit(code);
		},
		Some(Command::Setup { component, check, json }) => {
			let base = base_dir(&cli)?;
			let opts = local_cmd::SetupOptions { component: component.clone(), check: *check, json: *json };
			let stdout = io::stdout();
			let stderr = io::stderr();
			let mut out = io::BufWriter::new(stdout.lock());
			let mut err = io::BufWriter::new(stderr.lock());
			let code = local_cmd::run_setup(&opts, &base, &mut out, &mut err)?;
			out.flush()?;
			err.flush()?;
			return local_exit(code);
		},
		Some(Command::Completions { shell }) => {
			return completions::run(shell.as_deref()).map_err(|message| CliError::fail(message));
		},
		Some(Command::Complete { args }) => {
			completions::complete(args);
			return Ok(());
		},
		_ => {},
	}
	// One shared runner instance: it is the installed `AgentRunner` and the
	// concrete type behind the live steering channel (`RpcLaunch.steer`), so a
	// turn and a steering hook never drive two runtimes.
	let native_runner = Arc::new(PiAgentRunner::new()?);
	// The installed `AgentRunner` holds an `Arc` in a static, so the runner's
	// own `Drop` never runs when this process exits; this guard runs the
	// explicit teardown (supervisor, eval bridge, LSP clients, debug adapters)
	// on every return path below, including the `?` errors. RPC returns only
	// after its input is drained, so the guard never cuts a live turn short.
	let _runner_shutdown = runtime::RunnerShutdownGuard::new(Arc::clone(&native_runner));
	if agent::install(Box::new(Arc::clone(&native_runner))).is_err() {
		return Err(CliError::fail("agent runtime already registered"));
	}
	let Some(runner) = agent::runner() else {
		return Err(CliError::fail(agent::MISSING_RUNTIME));
	};

	// TS `applyStartupCwd`: an explicit `--cwd` owns the launch directory;
	// otherwise a launch from `$HOME` moves to a temp dir unless `--allow-home`.
	maybe_auto_chdir(&cli);
	// Project discovery (and relative overlays) use the effective run directory:
	// an explicit `--cwd` first, else the base config `cwd`, else the process cwd.
	let mut config = Config::load_run(&cli.config, cli.cwd.as_deref())?;
	// Dedicated flags are the runtime override layer: they win over every file.
	config.apply_runtime(&cli);
	let cwd = resolve_cwd(cli.cwd.clone(), config.cwd.clone())?;
	config.normalize_workspace_roots(&cwd);
	let processed_cli_files = if !cli.file_args.is_empty() {
		process_cli_file_args(&cli.file_args, &cwd)?
	} else {
		ProcessedCliFiles::default()
	};
	if cli.resume.as_deref() == Some("")
		&& (!io::stdin().is_terminal() || !io::stdout().is_terminal())
	{
		return Err(CliError::usage(
			"Non-interactive environment cannot display session picker. Specify a session ID or file path with --resume <id>.",
		));
	}

	// Foreign and session flags validation
	if cli.from_claude && cli.from_codex {
		return Err(CliError::usage(
			"--from-claude and --from-codex cannot be used together",
		));
	}
	let foreign_source = if cli.from_claude {
		Some(foreign_session::ForeignSessionSource::Claude)
	} else if cli.from_codex {
		Some(foreign_session::ForeignSessionSource::Codex)
	} else {
		None
	};
	if let Some(src) = foreign_source {
		let src_name = src.to_string().to_ascii_lowercase();
		if cli.no_session {
			return Err(CliError::usage(format!(
				"--from-{src_name} requires session persistence"
			)));
		}
		if cli.continue_session || cli.resume.is_some() || cli.fork.is_some() {
			return Err(CliError::usage(format!(
				"--from-{src_name} cannot be combined with --continue, --resume, or --fork"
			)));
		}
		if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
			return Err(CliError::usage(format!(
				"Non-interactive environment cannot display session picker. --from-{src_name} requires an interactive session picker."
			)));
		}
	}
	if cli.fork.is_some() && cli.no_session {
		return Err(CliError::usage("--fork requires session persistence"));
	}

	let prompt_inputs = (config.system_prompt.clone(), config.append_system_prompt.clone());
	let mut loaded_prompts =
		prompts::resolve(prompt_inputs.0.as_deref(), prompt_inputs.1.as_deref(), &cwd)?;
	// TS `system-prompt.ts` discovers project context files for every additional
	// workspace root beyond `cwd`; root order follows `--add-dir`, duplicates by
	// path collapse onto the first copy.
	for root in &config.add_dirs {
		for file in prompts::resolve(None, None, root)?.files {
			if !loaded_prompts.files.iter().any(|existing| existing.path == file.path) {
				loaded_prompts.files.push(file);
			}
		}
	}
	// TS `--no-rules` passes `options.rules = []`, disabling rules discovery;
	// this build's rule surface is the `RULES.md` context file.
	if config.no_rules {
		loaded_prompts
			.files
			.retain(|file| file.path.file_name().and_then(|name| name.to_str()) != Some("RULES.md"));
	}
	let skill_options = LoadOptions::from_config(&config, &cwd, cli.no_skills, &cli.skills);
	let skill_snapshot = SkillSnapshot::new();
	let loaded_skills = skills::load_with(&skill_options)?;
	for warning in &loaded_skills.warnings {
		eprintln!("warning: {}", warning.message);
	}
	skill_snapshot.set(loaded_skills.entries);
	// One router per top-level session: the `skill://` handler resolves against
	// this session's snapshot only, never a process-global skill registry.
	let internal_urls = Arc::new(pi_agent::tools::internal_urls::InternalUrlRouter::new());
	let _ = internal_urls
		.register(Arc::new(SkillUrlHandler::new(skill_snapshot.clone())))
		.map_err(Error::new)?;
	let memory_agent_dir = skill_options
		.agent_dir
		.clone()
		.unwrap_or_else(dirs::agent_dir);
	let _ = internal_urls
		.register(Arc::new(MemoryUrlHandler::new(
			memory_agent_dir,
			config.memory_backend(),
		)))
		.map_err(Error::new)?;
	let catalog = Arc::new(Catalog::load(&cwd)?);
	let approvals = Approvals::new(
		config.approval_mode.unwrap_or_default(),
		config.allow.iter().cloned(),
	);
	let default_model = config.default_model.clone();
	// TS findInitialModel: an explicit `--model` wins, then the first model in
	// the `--models` scope for a fresh session. A resumed session keeps its own
	// model, so the scope does not override it here. An explicitly supplied
	// scope that matches nothing is a hard error instead of a fallback to the
	// configured default (a CLI `--models` scope never silently widens).
	let restoring_session =
		cli.continue_session || cli.resume.is_some() || cli.fork.is_some() || foreign_source.is_some();
	let (requested_model, scoped_thinking) = match &cli.model {
		Some(model) => (Some(model.clone()), None),
		None if !restoring_session => match &config.model_scope {
			// An all-empty `--models ""` has no patterns to resolve, so it
			// imposes no scope (TS `resolveModelScope` skips empty entries).
			Some(patterns) if !patterns.is_empty() => match first_scoped_model(patterns, &catalog) {
				Some((model, thinking)) => (Some(model), thinking),
				None => {
					return Err(CliError::usage(format!(
						"no models match --models {}",
						patterns.join(",")
					)));
				},
			},
			_ => (None, None),
		},
		None => (None, None),
	};
	let requested_provider = cli.provider.clone();
	let requested_api_key = cli.api_key.clone();
	let thinking = cli
		.thinking
		.as_deref()
		.and_then(|raw| {
			let parsed = runtime::parse_cli_thinking(raw);
			if parsed.is_none() {
				eprintln!("{}", runtime::cli_thinking_warning(raw));
			}
			parsed
		})
		.or_else(|| scoped_thinking.as_deref().and_then(runtime::parse_cli_thinking));
	let session_dir = cli
		.session_dir
		.as_ref()
		.map(|dir| dirs::absolute(&cwd, dir));

	// TS `main.ts:1234-1264`: prewalk conflicts fail startup; an optional target
	// that cannot be resolved or has no configured auth only warns and leaves
	// prewalk unarmed. Config enablement never applies to a restored session.
	// The policy travels into every session owner (CLI, RPC, ACP).
	let prewalk_policy = prewalk::resolve_policy(
		&cli,
		&config,
		&catalog,
		restoring_session,
		requested_api_key.as_deref(),
	)
	.map_err(CliError::fail)?;
	for warning in &prewalk_policy.warnings {
		eprintln!("{warning}");
	}
	// Plan-yolo is explicit and fails hard: an unresolved target, missing
	// credential, or unapprovable tool mode must not silently run without it.
	let plan_yolo_arm = plan_yolo::resolve_arm(
		&cli,
		&config,
		&catalog,
		requested_api_key.as_deref(),
	)
	.map_err(CliError::fail)?;
	reject_dual_startup_arms(prewalk_policy.arm.is_some(), plan_yolo_arm.is_some())?;

	// Pre-resolve session target
	let mut chosen_file: Option<PathBuf> = None;

	if let Some(src) = foreign_source {
		let store = foreign_session::create_foreign_session_store(src, None);
		let sessions = store
			.list()
			.map_err(|e| CliError::fail(format!("Failed to list {} sessions: {e}", src.name())))?;
		if sessions.is_empty() {
			eprintln!("No {} sessions found", src.name());
			return Ok(());
		}
		if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
			eprintln!("No {} session selected", src.name());
			return Ok(());
		}
		eprintln!("Import {} Session", src.name());
		for (i, info) in sessions.iter().enumerate() {
			let label = info
				.title
				.as_deref()
				.or(info.first_message.as_deref())
				.unwrap_or(&info.id);
			eprintln!("{}. {} ({})", i + 1, label, info.created_iso());
		}
		eprint!("Select session (1-{}): ", sessions.len());
		io::stderr().flush()?;
		let mut line = String::new();
		io::stdin().read_line(&mut line)?;
		let idx: usize = line
			.trim()
			.parse()
			.map_err(|_| CliError::fail(format!("No {} session selected", src.name())))?;
		if idx < 1 || idx > sessions.len() {
			eprintln!("No {} session selected", src.name());
			return Ok(());
		}
		let selected_info = &sessions[idx - 1];
		let loaded = store
			.load(selected_info)
			.map_err(|e| CliError::fail(format!("Failed to import {} session: {e}", src.name())))?;

		let target_cwd = if loaded.cwd.as_os_str().is_empty() || !loaded.cwd.exists() {
			cwd.clone()
		} else {
			loaded.cwd.clone()
		};
		let mut imported_store = SessionStore::open(&SessionOptions {
			enabled: true,
			cwd: target_cwd,
			model: requested_model.clone().or_else(|| default_model.clone()),
			provider: requested_provider.clone(),
			dir: session_dir.clone(),
			id: None,
			continue_latest: false,
			parent_session: None,
			no_title: false,
		});
		let prov = loaded.provenance_entry(selected_info);
		let prov_entry: session::SessionEntry = serde_json::from_value(prov)
			.map_err(|e| CliError::fail(format!("cannot parse provenance entry: {e}")))?;
		imported_store
			.append_entry(prov_entry)
			.map_err(|e| CliError::fail(format!("cannot append provenance entry: {e}")))?;
		for entry_val in &loaded.entries {
			let entry: session::SessionEntry = serde_json::from_value(entry_val.clone())
				.map_err(|e| CliError::fail(format!("cannot parse imported entry: {e}")))?;
			imported_store
				.append_entry(entry)
				.map_err(|e| CliError::fail(format!("cannot append imported entry: {e}")))?;
		}
		chosen_file = imported_store.file().map(Path::to_path_buf);
	} else if let Some(fork_arg) = &cli.fork {
		let source_path = if fork_arg.contains('/') || fork_arg.contains('\\') || fork_arg.ends_with(".jsonl") {
			dirs::absolute(&cwd, Path::new(fork_arg))
		} else {
			match session::resolve_resumable_session(fork_arg, &cwd, session_dir.as_deref(), true) {
				Ok(Some(path)) => path,
				_ => {
					return Err(CliError::usage(format!(
						"Session \"{fork_arg}\" not found.\nRun `omp --resume` without an argument to pick from recent sessions, or `omp` to start a new one."
					)));
				}
			}
		};
		let new_file = session::fork_session(&source_path, Some(&cwd), session_dir.as_deref())
			.map_err(CliError::usage)?;
		// The fork's new file has no runner scope yet; reset the real source
		// scope, and let the new scope seed from its first transcript.
		native_runner.advisor_reset_boundary(None, Some(&source_path), &[]);
		chosen_file = Some(new_file);
	} else if let Some(resume_arg) = &cli.resume {
		if resume_arg.trim().is_empty() {
			if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
				return Err(CliError::usage(
					"Non-interactive environment cannot display session picker. Specify a session ID or file path with --resume <id>.",
				));
			}
			let target_dir = session_dir
				.clone()
				.unwrap_or_else(|| session::compute_default_session_dir(&cwd, None));
			let mut sessions = list_sessions_for_picker(&target_dir);
			if sessions.is_empty() {
				let global_root = SessionOptions::default_sessions_root();
				if global_root != target_dir && global_root.is_dir() {
					sessions = list_sessions_for_picker(&global_root);
				}
			}
			if sessions.is_empty() {
				eprintln!("No sessions found");
				return Ok(());
			}
			for (i, (path, title, id, _)) in sessions.iter().enumerate() {
				let name = title.as_deref().unwrap_or(id);
				eprintln!("{}. {} ({})", i + 1, name, path.display());
			}
			eprint!("Select session (1-{}): ", sessions.len());
			io::stderr().flush()?;
			let mut line = String::new();
			io::stdin().read_line(&mut line)?;
			let idx: usize = line
				.trim()
				.parse()
				.map_err(|_| CliError::fail("No session selected"))?;
			if idx < 1 || idx > sessions.len() {
				eprintln!("No session selected");
				return Ok(());
			}
			chosen_file = Some(sessions[idx - 1].0.clone());
		} else if resume_arg.contains('/') || resume_arg.contains('\\') || resume_arg.ends_with(".jsonl") {
			let path = dirs::absolute(&cwd, Path::new(resume_arg));
			if !path.exists() {
				return Err(CliError::usage(format!(
					"Session \"{resume_arg}\" not found.\nRun `omp --resume` without an argument to pick from recent sessions, or `omp` to start a new one."
				)));
			}
			chosen_file = Some(path);
		} else {
			match session::resolve_resumable_session(resume_arg, &cwd, session_dir.as_deref(), true) {
				Ok(Some(path)) => {
					chosen_file = Some(path);
				},
				_ => {
					return Err(CliError::usage(format!(
						"Session \"{resume_arg}\" not found.\nRun `omp --resume` without an argument to pick from recent sessions, or `omp` to start a new one."
					)));
				}
			}
		}
	}

	let (advisor_cards_tx, advisor_cards_rx) = std::sync::mpsc::channel();
	let mut session = Session {
		runner,
		config,
		cwd: cwd.clone(),
		prompts: loaded_prompts,
		skill_snapshot,
		skill_options,
		internal_urls,
		catalog,
		approvals,
		model: None,
		provider: None,
		api_key: None,
		root_base_url: None,
		thinking,
		provider_session_id: None,
		prompt_cache_key: None,
		prompt_cache_key_source: None,
		max_time: cli.max_time,
		deadline: Deadline::new(cli.max_time),
		max_turns: runtime::DEFAULT_MAX_TURNS,
		tools_enabled: !cli.no_tools,
		prompt_inputs,
		store: SessionStore::default(),
		// Resolved once below, after the session store is opened.
		service_tier_by_family: std::collections::BTreeMap::new(),
		service_tier_explicit: cli.service_tier.is_some(),
		service_tier_recorded: false,
		history: Vec::new(),
		checkpoint_host: CheckpointHost::new(),
		turn_tool_calls: 0,
		turn_started_in_goal: false,
		in_memory_messages: None,
		session_dir: session_dir.clone(),
		goal: Arc::new(GoalRuntime::new()),
		prewalk: prewalk::PrewalkState::new(prewalk_policy.arm.clone()),
		plan_yolo: None,
		plan_yolo_tools: None,
		advisor_cards_tx: Arc::new(std::sync::Mutex::new(advisor_cards_tx)),
		advisor_cards_rx,
	};
	if let Some(model) = requested_model.or(default_model) {
		session.select_model(model);
	}
	if let Some(provider) = requested_provider {
		session.api_key = session.config.resolve_api_key(&provider);
		session.provider = Some(provider);
	}
	if requested_api_key.is_some() {
		session.api_key = requested_api_key;
	}
	session.refresh_root_endpoint();

	let mut explicitly_existing = chosen_file.is_some();
	// TS `main.ts` truthiness: any non-empty `PI_NO_TITLE` disables titles,
	// including `0`. Resolved here (CLI boundary) so the library never reads
	// process-wide env and tests stay race-free.
	let no_title = cli.no_title
		|| std::env::var_os("PI_NO_TITLE").is_some_and(|value| !value.is_empty());
	let open_opts = if let Some(target) = &chosen_file {
		let dir = target.parent().map(PathBuf::from);
		let id = target.file_stem().and_then(|s| s.to_str()).map(str::to_string);
		SessionOptions {
			enabled: !cli.no_session,
			cwd: cwd.clone(),
			model: session.model.clone(),
			provider: session.provider.clone(),
			dir,
			id,
			continue_latest: false,
			parent_session: None,
			no_title,
		}
	} else {
		if let Some(req_id) = &cli.session_id {
			let check_dir = session_dir
				.clone()
				.unwrap_or_else(|| session::compute_default_session_dir(&cwd, None));
			let direct_file = check_dir.join(format!("{req_id}.jsonl"));
			if direct_file.is_file() {
				explicitly_existing = true;
			}
		}
		SessionOptions {
			enabled: !cli.no_session,
			cwd: cwd.clone(),
			model: session.model.clone(),
			provider: session.provider.clone(),
			dir: session_dir,
			id: cli.session_id.clone(),
			continue_latest: cli.continue_session,
			parent_session: None,
			no_title,
		}
	};

	// `omp acp`: JSON-RPC 2.0 ACP over stdio. One `AcpRuntime` owns the
	// transport writer; sessions are opened by the client through
	// `session/new|load|resume|fork`, never from a positional prompt.
	if let Some(Command::Acp { .. }) = &cli.command {
		let auth = acp::AcpAuth::open(dirs::agent_dir().join("agent.db"))
			.map_err(|err| CliError::fail(format!("cannot open agent.db for ACP: {err}")))?;
		let runner: Arc<dyn AgentRunner> = native_runner.clone();
		// ACP opens sessions later, so resolve the fresh-session settings arm
		// independently of this process's restored-session state.
		let fresh_prewalk = prewalk::resolve_policy(
			&cli,
			&session.config,
			&session.catalog,
			false,
			session.api_key.as_deref(),
		)
		.map_err(CliError::fail)?;
		reject_dual_startup_arms(fresh_prewalk.arm.is_some(), plan_yolo_arm.is_some())?;
		let mut recipe = acp::AcpRecipe::new(
			runner,
			session.config.clone(),
			session.prompts.clone(),
			Arc::clone(&session.catalog),
			auth.resolver(),
		);
		// Default `AlwaysAsk` lets `AcpPermissionBroker` gate `bash/edit/delete/move`
		// via `session/request_permission` unless `--approval-mode yolo` is set.
		recipe.approval_mode = session.config.approval_mode.unwrap_or_default();
		recipe.allow = session.config.allow.clone();
		recipe.model = session.model.clone();
		recipe.provider = session.provider.clone();
		recipe.api_key = session.api_key.clone();
		recipe.thinking = session.thinking;
		recipe.model_explicit = cli.model.is_some();
		recipe.prewalk = prewalk_policy.arm.clone();
		recipe.prewalk_fresh = fresh_prewalk.arm;
		recipe.prewalk_explicit = cli.prewalk || cli.prewalk_into.is_some();
		recipe.plan_yolo = plan_yolo_arm.clone();
		recipe.validate_startup_arms().map_err(CliError::fail)?;
		recipe.max_time = session.max_time;
		recipe.max_turns = session.max_turns;
		recipe.tools_enabled = session.tools_enabled;
		recipe.sessions_root = session
			.session_dir
			.clone()
			.unwrap_or_else(SessionOptions::default_sessions_root);
		recipe.plan_enabled = session.config.plan.enabled();
		return acp::run(recipe, io::stdin().lock(), io::stdout(), Duration::from_secs(2))
			.map(|_| ())
			.map_err(|err| CliError::fail(err.to_string()));
	}

	// `--mode rpc`: the process speaks NDJSON command/event frames on stdout.
	// `RpcMode` owns that single writer and its `ready` frame, so this path
	// never builds a `Reporter`, never reads the terminal (approval and `ask`
	// go over `extension_ui_*` frames) and never drains piped stdin as prompt
	// text: a positional prompt is not required and is not consumed here.
	if cli.mode == OutputMode::Rpc {
		let steer_runner = Arc::clone(&native_runner);
		let steer: Option<SteerFn> = Some(Arc::new(move |message: &SteeringMessage| {
			// Exact user parts travel with the steering message; an image is
			// never dropped or flattened to text.
			let mut parts = Vec::with_capacity(message.images.len() + 1);
			if !message.text.is_empty() {
				parts.push(pi_agent::ContentPart::text(message.text.clone()));
			}
			for image in &message.images {
				match pi_agent::ContentPart::image(image.data.clone(), image.mime_type.clone()) {
					Ok(part) => parts.push(part),
					Err(error) => {
						eprintln!("warning: cannot steer with image: {error}");
						return false;
					},
				}
			}
			steer_runner.steer(parts)
		}));
		// Preflight the log the RPC session will own: a resume whose log cannot
		// be persisted must fail here (nothing has been written to stdout yet)
		// instead of running in memory, and an inherited cache key has to come
		// from the header this process resumes. `RpcSession::open` opens the
		// store again from the same options; ponytail: double read at startup,
		// pass the store once `RpcLaunch` accepts a pre-opened one.
		let mut rpc_store = SessionStore::open(&open_opts);
		for warning in rpc_store.take_warnings() {
			eprintln!("warning: {warning}");
		}
		if !cli.no_session && explicitly_existing && !rpc_store.enabled() {
			return Err(CliError::usage(
				"cannot open session log: persistence disabled due to format error",
			));
		}
		let rpc_cache_key = cli
			.prompt_cache_key
			.clone()
			.filter(|key| !key.is_empty())
			.or_else(|| {
				let shape_changed = cli.model.is_some()
					|| session.thinking.is_some()
					|| session.config.system_prompt.is_some()
					|| session.config.append_system_prompt.is_some()
					|| cli.tools.is_some()
					|| cli.no_tools;
				if shape_changed {
					return None;
				}
				rpc_store
					.header()
					.and_then(|header| header.provider_prompt_cache_key.clone())
			});
		// Typed local so the `Arc<PiAgentRunner>` unsizes once here; method
		// receiver keeps `T = PiAgentRunner` concrete before the coercion.
		let rpc_runner: Arc<dyn AgentRunner> = native_runner.clone();
		let launch = RpcLaunch {
			runner: rpc_runner,
			config: session.config.clone(),
			cwd: session.cwd.clone(),
			prompts: session.prompts.clone(),
			skill_snapshot: session.skill_snapshot.clone(),
			skill_options: session.skill_options.clone(),
			catalog: Arc::clone(&session.catalog),
			// One store for the whole process; the auth bridge opens it before the
			// session exists and fails startup when it cannot.
			agent_db: dirs::agent_dir().join("agent.db"),
			approval_mode: session.config.approval_mode.unwrap_or_default(),
			allow: session.config.allow.clone(),
			model: session.model.clone(),
			provider: session.provider.clone(),
			api_key: session.api_key.clone(),
			thinking: session.thinking,
			restore_model: cli.model.is_none()
				&& (restoring_session || explicitly_existing),
			prewalk: prewalk_policy.arm.clone(),
			plan_yolo: plan_yolo_arm.clone(),
			max_time: session.max_time,
			max_turns: session.max_turns,
			tools_enabled: session.tools_enabled,
			// Same rule as the `--print` path below; resolved above.
			prompt_cache_key_inherited: cli
				.prompt_cache_key
				.as_deref()
				.filter(|key| !key.is_empty())
				.is_none()
				&& rpc_cache_key.is_some(),
			prompt_cache_key: rpc_cache_key,
			service_tier_explicit: cli.service_tier.is_some(),
			provider_session_id: cli
				.provider_session_id
				.clone()
				.filter(|id| !id.is_empty()),
			store_options: open_opts,
			steer,
		};
		return RpcMode::run(launch, io::stdin().lock(), io::stdout()).map_err(CliError::from);
	}

	let mut store = SessionStore::open(&open_opts);
	for warning in store.take_warnings() {
		eprintln!("warning: {warning}");
	}
	// TS `main.ts:buildSessionOptions`: an explicit `--prompt-cache-key` always
	// wins; otherwise a resumed/forked header key is inherited only when the
	// provider route and prompt/tool shape are unchanged.
	let explicit_cache_key = cli.prompt_cache_key.clone().filter(|key| !key.is_empty());
	let cache_shape_changed = cli.model.is_some()
		|| session.thinking.is_some()
		|| session.config.system_prompt.is_some()
		|| session.config.append_system_prompt.is_some()
		|| cli.tools.is_some()
		|| cli.no_tools;
	if let Some(key) = explicit_cache_key {
		session.prompt_cache_key = Some(key);
		session.prompt_cache_key_source = Some(PromptCacheKeySource::Explicit);
	} else if !cache_shape_changed {
		if let Some(key) = store
			.header()
			.and_then(|header| header.provider_prompt_cache_key.clone())
		{
			session.prompt_cache_key = Some(key);
			session.prompt_cache_key_source = Some(PromptCacheKeySource::Inherited);
		}
	}
	session.provider_session_id = cli.provider_session_id.clone().filter(|id| !id.is_empty());
	if !cli.no_session && explicitly_existing && !store.enabled() {
		return Err(CliError::usage(
			"cannot open session log: persistence disabled due to format error",
		));
	}
	session.history = if store.has_branch_context() {
		store.branch_history().map_err(CliError::fail)?
	} else {
		store.history().to_vec()
	};
	session.checkpoint_host.rehydrate(&store);
	session.store = store;
	// One authoritative live selection, resolved once at launch with the TS
	// precedence (explicit flag, then the recorded entry including a reset,
	// then configured family defaults, then the legacy single value).
	let base_service_tiers = if session.store.has_service_tier_entry() {
		session.store.service_tier_by_family()
	} else {
		session.config.configured_service_tiers()
	};
	// `--service-tier` is the TS OpenAI-only override (`cli/args.ts:48` ->
	// `sdk.ts:3408`): it layers onto the recorded/configured map and never
	// touches the anthropic or google families.
	let live_service_tiers = omp_cli::service_tier::overlay_openai(
		if session.service_tier_explicit {
			session.config.service_tier.as_deref()
		} else {
			None
		},
		&base_service_tiers,
	);
	session.service_tier_by_family = live_service_tiers.clone();
	session.service_tier_recorded = true;
	if session.service_tier_explicit {
		// TS `setServiceTierFamily`: an explicit launch flag is recorded, so a
		// later resume restores the re-armed selection.
		if session.store.enabled()
			&& let Err(err) = session.store.append_service_tier_change(&live_service_tiers)
		{
			eprintln!("warning: cannot record service tier change: {err}");
		}
	}
	// TS `sdk.ts:1505-1545`: an explicit `--model` wins; otherwise a restored
	// session's recorded model/role decides the route, with the configured
	// default as fallback.
	if cli.model.is_none() && (restoring_session || explicitly_existing) {
		if let Some(restored) = session.restore_persisted_model(cli.thinking.is_none()) {
			eprintln!("Restored session model: {restored}");
		}
	}
	// Restore the persisted goal mode from the active branch before any turn:
	// a malformed/disabled record is cleared, an active replay is paused.
	session.reconcile_goal_mode();
	if let Some(arm) = plan_yolo_arm {
		let gate = Arc::new(std::sync::Mutex::new(acp::plan::PlanGate::new(true)));
		session.plan_yolo = Some(Arc::new(plan_yolo::PlanYoloShared::new(gate, arm)));
	}

	let mode = cli.mode;
	let reporter_id = if session.store.enabled() {
		session.store.id().to_string()
	} else {
		session_id()
	};
	let mut reporter = Reporter::new(mode, io::stdout(), io::stderr());
	reporter.session_header(&reporter_id, &session.cwd)?;

	if matches!(cli.command.as_ref(), Some(Command::Repl)) {
		let result = run_repl(&mut session, mode, &mut reporter).map_err(CliError::from);
		// Post-shutdown child accounting reaches the log before it closes.
		session.finish(&native_runner);
		return result;
	}

	let piped_stdin = read_piped_stdin()?;

	let repl_mode = cli.command.is_none()
		&& cli.prompt.is_empty()
		&& cli.file_args.is_empty()
		&& piped_stdin.is_none()
		&& !cli.print;
	if repl_mode {
		let result = run_repl(&mut session, mode, &mut reporter).map_err(CliError::from);
		// Post-shutdown child accounting reaches the log before it closes.
		session.finish(&native_runner);
		return result;
	}

	// TS `packages/coding-agent/src/main.ts:1551` applies `--print-thoughts`
	// only when `!isInteractive`: the REPL renders no reasoning through it, and
	// it flushes `finish()` per turn, so the gate belongs here, after the REPL
	// branches and not at reporter construction.
	let mut reporter = reporter.with_print_thoughts(cli.print_thoughts);

	let mut remaining_prompts = cli.prompt.clone();
	let initial_prompt = if !remaining_prompts.is_empty() {
		remaining_prompts.remove(0)
	} else {
		String::new()
	};

	let mut context_prefix = String::new();
	if let Some(stdin) = piped_stdin {
		context_prefix.push_str(&stdin);
		context_prefix.push('\n');
	}
	if !processed_cli_files.text.is_empty() {
		context_prefix.push_str(&processed_cli_files.text);
	}

	let has_initial_content = !context_prefix.is_empty()
		|| !initial_prompt.trim().is_empty()
		|| !processed_cli_files.images.is_empty();

	if !has_initial_content && remaining_prompts.is_empty() {
		// TS print mode with zero messages writes nothing and exits 0.
		return reporter.finish().map_err(CliError::from);
	}

	let mut prompts_to_run = Vec::new();
	if has_initial_content {
		prompts_to_run.push((
			if context_prefix.is_empty() { None } else { Some(context_prefix) },
			initial_prompt,
			processed_cli_files.images,
		));
	}
	for prompt in remaining_prompts {
		prompts_to_run.push((None, prompt, Vec::new()));
	}

	for (ctx, prompt, parts) in prompts_to_run {
		let prompt = match session.expand_skill_command(&prompt) {
			Ok(prompt) => prompt,
			Err(error) => {
				eprintln!("warning: {error}");
				prompt
			},
		};
		if let Err(error) = session.turn(ctx, prompt, parts, mode, &mut reporter) {
			// A failed turn still settles the roster, but bounded short: never
			// wait ten minutes on a session that already failed.
			let _ = session.advisor_headless_drain(&mut reporter, Duration::from_secs(30));
			reporter.finish().ok();
			session.finish(&native_runner);
			return Err(CliError::from(error));
		}
		// Opt-in headless continuation (`goal.continuationModes`); the default
		// `["interactive"]` leaves this a no-op.
		if let Err(error) = session.drive_goal_continuation(mode, &mut reporter) {
			let _ = session.advisor_headless_drain(&mut reporter, Duration::from_secs(30));
			reporter.finish().ok();
			session.finish(&native_runner);
			return Err(CliError::from(error));
		}
		session.drive_autolearn_capture(mode);
	}
	// Every primary prompt has settled: preserve late advice, wait bounded for
	// the roster, persist/render it, then stop advisor work (TS
	// `prepareForHeadlessAdvisorDrain` -> `waitForAdvisorCatchup(10min)`).
	session.advisor_headless_drain(&mut reporter, Duration::from_secs(600))?;
	// Last chance for a child that settled after the final turn boundary.
	session.finish(&native_runner);
	reporter.finish().map_err(CliError::from)
}

fn list_sessions_for_picker(dir: &Path) -> Vec<(PathBuf, Option<String>, String, String)> {
	let mut result = Vec::new();
	let Ok(entries) = std::fs::read_dir(dir) else {
		return result;
	};
	for entry in entries.flatten() {
		let path = entry.path();
		if path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
			continue;
		}
		let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
			continue;
		};
		let id = stem.rfind('_').map_or(stem, |pos| &stem[pos + 1..]).to_string();
		let mtime = entry
			.metadata()
			.and_then(|m| m.modified())
			.unwrap_or(std::time::SystemTime::UNIX_EPOCH);
		let iso = output::iso8601(mtime);

		let title = std::fs::read_to_string(&path).ok().and_then(|c| {
			c.lines()
				.next()
				.and_then(session::parse_title_slot)
				.map(|s| s.title)
		});
		result.push((path, title, id, iso));
	}
	result.sort_by(|a, b| b.3.cmp(&a.3));
	result
}

fn run_repl<W: Write, E: Write>(
	session: &mut Session<'_>,
	mode: OutputMode,
	reporter: &mut Reporter<W, E>,
) -> Result<()> {
	// The turn needs stdin for approvals and `ask`; the REPL locks only while
	// reading its own prompt line.
	if io::stdin().is_terminal() && io::stderr().is_terminal() {
		session
			.approvals
			.set_ask(Some(omp_cli::ask::terminal_channel(session.config.ask.timeout())));
	} else {
		session.approvals.set_ask(None);
	}
	let mut line = String::new();
	loop {
		eprint!("> ");
		io::stderr().flush()?;
		line.clear();
		{
			let stdin = io::stdin();
			let mut input = stdin.lock();
			if input.read_line(&mut line)? == 0 {
				break;
			}
		}
		let Some(command) = repl::parse(&line) else {
			continue;
		};
		match command {
			repl::ReplCommand::Exit => break,
			repl::ReplCommand::Model(Some(id)) => {
				session.select_model(id.clone());
				eprintln!("model: {id}");
			},
			repl::ReplCommand::Model(None) => {
				eprintln!("model: {}", session.model.as_deref().unwrap_or("(default)"));
			},
			repl::ReplCommand::Cwd(Some(path)) => {
				session.change_cwd(PathBuf::from(path))?;
				eprintln!("cwd: {}", session.cwd.display());
			},
			repl::ReplCommand::Cwd(None) => eprintln!("cwd: {}", session.cwd.display()),
			repl::ReplCommand::Prompt(text) => {
				let text = match session.expand_skill_command(&text) {
					Ok(text) => text,
					Err(error) => {
						eprintln!("warning: {error}");
						text
					},
				};
				if let Err(error) = session.turn(None, text, Vec::new(), mode, reporter) {
					reporter.finish().ok();
					eprintln!("{error}");
					continue;
				}
				// Opt-in headless continuation (`goal.continuationModes`).
				if let Err(error) = session.drive_goal_continuation(mode, reporter) {
					reporter.finish().ok();
					eprintln!("{error}");
					continue;
				}
				session.drive_autolearn_capture(mode);
				// Text mode buffers the final assistant message until `finish`:
				// flush it per turn so REPL output is visible immediately instead
				// of only when `/exit` ends the session.
				reporter.finish()?;
			},
		}
	}
	reporter.finish()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromptCacheKeySource {
	Explicit,
	Inherited,
}

/// TS `#clearInheritedProviderPromptCacheKey`: a model switch clears only the
/// inherited header key, never an explicit `--prompt-cache-key`.
fn invalidate_inherited_cache_key(
	key: &mut Option<String>,
	source: &mut Option<PromptCacheKeySource>,
) {
	if *source == Some(PromptCacheKeySource::Inherited) {
		*key = None;
		*source = None;
	}
}

/// Drain queued goal writes into the session log, in order.
///
/// The goal tool runs inside the agent loop, which must never open a second
/// writer on the same log, so transitions are queued and applied here by the
/// one owner. Headless has no live `goal_updated` subscriber: the typed events
/// are consumed so the queue cannot grow across turns.
///
/// After a successful `background-usage` append the matching keys move from
/// the in-flight set into the turn-scoped durable seed; other in-flight keys
/// stay until they append or the next turn seed.
fn apply_goal_intents(store: &mut SessionStore, goal_runtime: &GoalRuntime) {
	let _ = goal_runtime.take_events();
	let mut retired = Vec::new();
	for intent in goal_runtime.take_pending() {
		let result = match intent {
			GoalPersist::ModeChange { mode, data } => store.append_mode_change(mode, data),
			GoalPersist::Completed(data) => store.append_goal_completed(data),
			GoalPersist::BackgroundUsage { id, result_key, usage } => {
				if store.enabled() {
					let mut data = serde_json::json!({ "id": id, "usage": usage });
					let retire_key = result_key
						.as_deref()
						.filter(|key| !key.is_empty())
						.map(str::to_string)
						.or_else(|| (!id.is_empty()).then_some(id.clone()));
					if let Some(result_key) = result_key {
						data["resultKey"] = serde_json::Value::String(result_key);
					}
					match store.append_custom(goal::BACKGROUND_USAGE_CUSTOM_TYPE, Some(data)) {
						Ok(entry_id) => {
							if let Some(key) = retire_key {
								retired.push(key);
							}
							Ok(entry_id)
						},
						Err(err) => Err(err),
					}
				} else {
					Ok(String::new())
				}
			},
		};
		if let Err(err) = result {
			eprintln!("warning: {err}");
		}
	}
	if !retired.is_empty() {
		goal_runtime.retire_durable_background_keys(retired);
	}
}

/// Persist one advisor card as a `custom_message` (so resume replays it) and
/// render it on the active output surface. Exactly one call per card: the
/// runner's sink never also writes an event-wire card.
fn persist_advisor_card<W: Write, E: Write>(
	store: &mut SessionStore,
	reporter: &mut Reporter<W, E>,
	content: &str,
	details: &serde_json::Value,
) -> Result<()> {
	if store.enabled()
		&& let Err(err) = store.append_custom_message(
			omp_cli::advisor_runtime::CARD_CUSTOM_TYPE,
			content,
			true,
			details.clone(),
		)
	{
		eprintln!("warning: {err}");
	}
	reporter.advisor_card(content, details)
}

struct Session<'a> {
	runner: &'a dyn AgentRunner,
	config: Config,
	cwd: PathBuf,
	prompts: Prompts,
	/// One snapshot per top-level session; tools and `skill://` share it.
	skill_snapshot: SkillSnapshot,
	skill_options: LoadOptions,
	/// Session internal URL router with `skill://` already mounted.
	internal_urls: Arc<pi_agent::tools::internal_urls::InternalUrlRouter>,
	catalog: Arc<Catalog>,
	approvals: Approvals,
	model: Option<String>,
	provider: Option<String>,
	api_key: Option<String>,
	/// Effective endpoint the current root credential belongs to, resolved
	/// exactly like request planning. Mid-session switches reuse the root key
	/// only for the same provider **and** this endpoint.
	root_base_url: Option<String>,
	/// Validated `--thinking` selector; wins over a model `:level` suffix.
	thinking: Option<runtime::CliThinking>,
	/// Provider-side sticky session id from `--provider-session-id`.
	provider_session_id: Option<String>,
	/// Explicit prompt-cache identity: flag value, else resumed/forked header.
	prompt_cache_key: Option<String>,
	/// Where `prompt_cache_key` came from: inherited keys die on a model switch,
	/// explicit `--prompt-cache-key` values survive (TS `providerPromptCacheKeySource`).
	prompt_cache_key_source: Option<PromptCacheKeySource>,
	max_time: Option<Duration>,
	deadline: Option<Deadline>,
	max_turns: usize,
	tools_enabled: bool,
	/// Raw `--system-prompt` / `--append-system-prompt` inputs, kept so a REPL
	/// `/cwd` re-resolves the project files for the new directory.
	prompt_inputs: (Option<String>, Option<String>),
	store: SessionStore,
	/// TS `ServiceTierByFamily`: restored live selection, kept in memory even
	/// when `--no-session` disables persistence so a downgrade sticks across
	/// prompts in this process.
	service_tier_by_family: std::collections::BTreeMap<String, String>,
	/// `--service-tier` was supplied at launch: it rearms the selection ahead
	/// of any recorded entry, per TS precedence.
	service_tier_explicit: bool,
	/// A downgrade or rearm changed the in-memory selection (kept under
	/// `--no-session`, where there is no journal to restore from).
	service_tier_recorded: bool,
	history: Vec<TurnRecord>,
	checkpoint_host: CheckpointHost,
	/// Tool results in the just-finished turn (TS `tool_execution_end` count),
	/// consumed by the `autolearn.minToolCalls` gate.
	turn_tool_calls: usize,
	/// Goal mode when the just-finished turn started; capture skips the turn
	/// even if a goal tool completed it.
	turn_started_in_goal: bool,
	/// Live branch conversation for `--no-session` runs (no leading system
	/// message). Rewinds rewrite it; each later request replays it verbatim.
	in_memory_messages: Option<Vec<pi_agent::Message>>,
	session_dir: Option<PathBuf>,
	/// Session goal/budget controller (Goal Mode accounting and `+Nk` window).
	goal: Arc<GoalRuntime>,
	/// One-way prewalk state machine; disarmed after a switch or no-op target.
	prewalk: prewalk::PrewalkState,
	/// Explicit plan-yolo gate/pending; armed before the first prompt.
	plan_yolo: Option<Arc<plan_yolo::PlanYoloShared>>,
	/// Planning tool selection to restore after approval or a failed arm.
	plan_yolo_tools: Option<plan_yolo::PlanYoloToolSelection>,
	/// Advisor card sink (session-scoped): the runner calls it once per card and
	/// the host persists/forwards from the receiver, so a card emitted after a
	/// turn settles (or during the headless drain) is never lost.
	advisor_cards_tx: Arc<std::sync::Mutex<std::sync::mpsc::Sender<(String, serde_json::Value)>>>,
	advisor_cards_rx: std::sync::mpsc::Receiver<(String, serde_json::Value)>,
}

/// Scope selector matching a run request, derived from the live store rather
/// than from `self`: rewind/fork handlers need the current journal identity.
fn advisor_scope_for(store: &SessionStore) -> (Option<String>, Option<PathBuf>) {
	(
		store.enabled().then(|| store.id().to_string()),
		store.file().map(Path::to_path_buf),
	)
}

impl Session<'_> {
	/// TS `sdk.ts:1505-1545`: with no explicit `--model`, a restored session
	/// selects the first recorded model on its branch that exists in the
	/// catalog and has usable auth. The ephemeral prewalk `fallback` role falls
	/// back to the configured default, so the fast model does not survive
	/// resume. Returns the selected `provider/model[:level]` string.
	fn restore_persisted_model(&mut self, restore_thinking: bool) -> Option<String> {
		let candidates = self
			.store
			.restorable_models(self.config.default_model.as_deref());
		for candidate in candidates {
			let Some((provider, rest)) = candidate.split_once('/') else {
				continue;
			};
			if provider.is_empty() {
				continue;
			}
			let (id, _) = runtime::split_effort(rest);
			if id.is_empty()
				|| !self
					.catalog
					.providers
					.get(provider)
					.is_some_and(|models| models.contains_key(id))
			{
				continue;
			}
			let candidate_endpoint = runtime::effective_endpoint(
				&self.catalog,
				&self.config,
				provider,
				id,
			)
			.ok();
			let same_route = self.provider.as_deref() == Some(provider)
				&& candidate_endpoint.is_some()
				&& candidate_endpoint.as_deref() == self.root_base_url.as_deref()
				&& self.api_key.as_deref().is_some_and(|key| !key.trim().is_empty());
			if !same_route
				&& !runtime::provider_has_configured_auth(
					&self.catalog,
					&self.config,
					provider,
					None,
				)
			{
				continue;
			}
			let api_key = if same_route {
				self.api_key.clone()
			} else {
				self.config.resolve_api_key(provider)
			};
			self.provider = Some(provider.to_string());
			self.model = Some(id.to_string());
			self.api_key = api_key;
			if restore_thinking
				&& let Some(level) = runtime::thinking_suffix(rest)
			{
				self.thinking = Some(level);
			}
			// Restoration reconstructs the session's own route; it is not an
			// explicit model switch. The header cache key was inherited for this
			// session before this call and must survive unchanged.

			self.root_base_url = runtime::effective_endpoint(
				&self.catalog,
				&self.config,
				provider,
				id,
			)
			.ok();
			return Some(candidate);
		}
		None
	}

	fn refresh_root_endpoint(&mut self) {
		self.root_base_url = self
			.provider
			.as_deref()
			.zip(self.model.as_deref())
			.and_then(|(provider, model)| {
				runtime::effective_endpoint(&self.catalog, &self.config, provider, model).ok()
			});
	}

	fn select_model(&mut self, id: String) {
		let provider = self.catalog.provider_of(&id).map(str::to_string);
		let changed = self.model.as_deref() != Some(id.as_str()) || self.provider != provider;
		if changed {
			invalidate_inherited_cache_key(
				&mut self.prompt_cache_key,
				&mut self.prompt_cache_key_source,
			);
		}
		self.provider = provider;
		self.api_key = self
			.provider
			.as_deref()
			.and_then(|provider| self.config.resolve_api_key(provider));
		self.model = Some(id);
		self.root_base_url = self
			.provider
			.as_deref()
			.zip(self.model.as_deref())
			.and_then(|(provider, model)| {
				runtime::effective_endpoint(&self.catalog, &self.config, provider, model).ok()
			});
	}

	fn change_cwd(&mut self, path: PathBuf) -> Result<()> {
		let candidate = dirs::absolute(&self.cwd, &path);
		self.cwd = std::fs::canonicalize(&candidate).map_err(|err| {
			Error::with_source(format!("cwd {} is not accessible", candidate.display()), err)
		})?;
		self.prompts = prompts::resolve(
			self.prompt_inputs.0.as_deref(),
			self.prompt_inputs.1.as_deref(),
			&self.cwd,
		)?;
		self.skill_options.cwd = self.cwd.clone();
		let reloaded = skills::load_with(&self.skill_options)?;
		for warning in &reloaded.warnings {
			eprintln!("warning: {}", warning.message);
		}
		self.skill_snapshot.set(reloaded.entries);
		Ok(())
	}

	/// Whether the current tool selection includes `name` (no restriction = all).
	fn tool_selected(&self, name: &str) -> bool {
		match &self.config.tools {
			None => true,
			Some(allowed) => allowed.iter().any(|tool| tool == name),
		}
	}

	/// Headless registration rule for the hidden goal tool: eager when
	/// `goal.enabled` does not exclude it, and always for a live or paused goal
	/// so a restored goal stays manageable (TS issue #9444).
	fn goal_tool_mounted(&self) -> bool {
		self.tools_enabled
			&& self.config.goal.enabled()
			&& (self.goal.has_goal() || self.tool_selected(GOAL_TOOL_NAME))
	}

	fn goal_extra_tools(&self) -> Vec<ExtraTool> {
		if !self.goal_tool_mounted() {
			return Vec::new();
		}
		let tool: Arc<dyn pi_agent::tools::Tool> =
			Arc::new(GoalTool::new(Arc::clone(&self.goal)));
		vec![ExtraTool::from(tool)]
	}

	fn agent_dir(&self) -> PathBuf {
		self.skill_options.agent_dir.clone().unwrap_or_else(dirs::agent_dir)
	}

	/// Whether this run mounts each auto-learn tool, mirroring TS `isToolAllowed`
	/// plus the top-level registry gate: `manage_skill` follows
	/// `autolearn.enabled`, `learn` additionally needs a backend. Only the local
	/// backend exists in this port, so Mnemopi/Hindsight mount nothing.
	fn autolearn_tool_presence(&self) -> (bool, bool) {
		if !self.tools_enabled || !self.config.autolearn.enabled() {
			return (false, false);
		}
		let manage = self.tool_selected(autolearn::MANAGE_SKILL_TOOL_NAME);
		let learn = matches!(self.config.memory_backend(), MemoryBackend::Local)
			&& self.tool_selected(autolearn::LEARN_TOOL_NAME);
		(manage, learn)
	}

	fn autolearn_extra_tools(&self) -> Vec<ExtraTool> {
		let (manage, learn) = self.autolearn_tool_presence();
		let mut tools = Vec::new();
		if manage && let Some(tool) =
			ManageSkillTool::create_if(true, self.skill_options.clone(), self.skill_snapshot.clone())
		{
			let tool: Arc<dyn pi_agent::tools::Tool> = Arc::new(tool);
			tools.push(ExtraTool::from(tool));
		}
		if learn && let Some(tool) = LocalLearnTool::create_if(
			true,
			MemoryBackend::Local,
			self.skill_options.clone(),
			self.skill_snapshot.clone(),
		) {
			let tool: Arc<dyn pi_agent::tools::Tool> = Arc::new(tool);
			tools.push(ExtraTool::from(tool));
		}
		tools
	}

	/// TS `appendPrompt` suffixes for a top-level run: the configured append,
	/// memory guidance, then the auto-learn guidance driven by mounted tools.
	fn runtime_append_prompt(&self) -> Option<String> {
		let memory = matches!(self.config.memory_backend(), MemoryBackend::Local)
			.then(|| {
				local_memory::build_memory_instructions(
					&self.agent_dir(),
					&self.cwd,
					self.config.summary_injection_token_limit(),
				)
			})
			.flatten();
		let (manage, learn) = self.autolearn_tool_presence();
		let autolearn = autolearn::build_auto_learn_instructions(manage, learn);
		let mut parts: Vec<String> = self.prompts.append.iter().cloned().collect();
		parts.extend(memory);
		parts.extend(autolearn);
		(!parts.is_empty()).then(|| parts.join("\n\n"))
	}

	/// Session usage at turn start: the goal accounting baseline and budget seed.
	fn goal_seed(&self) -> TokenCounters {
		let usage = self.store.session_usage();
		TokenCounters {
			input:       usage.input,
			output:      usage.output,
			cache_read:  usage.cache_read,
			cache_write: usage.cache_write,
		}
	}

	/// Hidden per-turn goal context (TS `goal-mode-context`): the active prompt
	/// plus live persisted todos when the `todo` tool is available.
	fn goal_mode_context(
		&self,
		todo_phases: &[pi_agent::tools::todo::TodoPhase],
	) -> Option<String> {
		let active = self.goal.build_active_prompt()?;
		let todo = goal::build_goal_todo_context(todo_phases, self.tool_selected("todo"));
		Some(goal::render_goal_mode_context(&active, todo.as_deref()))
	}

	/// Apply queued goal session writes through this session's single writer, in
	/// order. A tool transition is queued inside the agent loop and drained here
	/// so no second writer ever touches the log.
	fn apply_goal_pending(&mut self) {
		apply_goal_intents(&mut self.store, &self.goal);
	}

	/// Final durable drain before this session's log closes.
	///
	/// Stops every runner-owned supervisor first, so a background child that
	/// outlived the last turn has already settled its real usage into the
	/// accounting ledger, then writes that queue into the session log.
	fn finish(&mut self, runner: &runtime::PiAgentRunner) {
		runner.shutdown();
		self.apply_goal_pending();
	}

	/// Persist and render every advisor card queued since the last boundary.
	fn drain_advisor_cards<W: Write, E: Write>(
		&mut self,
		reporter: &mut Reporter<W, E>,
	) -> Result<()> {
		while let Ok((content, details)) = self.advisor_cards_rx.try_recv() {
			persist_advisor_card(&mut self.store, reporter, &content, &details)?;
		}
		Ok(())
	}

	/// Headless settle (TS `prepareForHeadlessAdvisorDrain`): from here every card
	/// is preserved, the primary waits once for the roster, and every late card
	/// is persisted/rendered before advisor work stops. `timeout` is 10min on
	/// the success path and 30s when a turn already failed.
	fn advisor_headless_drain<W: Write, E: Write>(
		&mut self,
		reporter: &mut Reporter<W, E>,
		timeout: Duration,
	) -> Result<()> {
		let (id, file) = advisor_scope_for(&self.store);
		self
			.runner
			.advisor_prepare_headless_drain(id.as_deref(), file.as_deref());
		self
			.runner
			.advisor_wait_catchup(id.as_deref(), file.as_deref(), timeout);
		self.drain_advisor_cards(reporter)?;
		self.runner.advisor_dispose(id.as_deref(), file.as_deref());
		Ok(())
	}

	/// Complete-exit (TS `#exitGoalMode(reason: "completed")`): the goal tool
	/// already set `exiting`; after this turn drains, clear the mode and write the
	/// terminal metric exactly once.
	fn finish_goal_turn(&mut self) {
		self.apply_goal_pending();
		let Some(completed) = self.goal.take_exiting() else {
			return;
		};
		if let Err(err) = self.store.append_mode_change(goal::MODE_NONE, None) {
			eprintln!("warning: {err}");
		}
		if let Err(err) =
			self.store.append_goal_completed(goal::encode_completed_data(&completed))
		{
			eprintln!("warning: {err}");
		}
		self.apply_goal_pending();
	}

	/// TS `#reconcileModeFromSession` for the headless paths.
	fn reconcile_goal_mode(&mut self) {
		let goal_enabled = self.config.goal.enabled();
		match goal::plan_restore(&self.store, goal_enabled) {
			// No goal on the active branch: drop any restored live goal so a
			// resumed log without one cannot inherit a previous goal. The
			// headless session is single-store, so this only guards reloads.
			RestorePlan::Idle => {
				self.goal.set_restored_state(None);
				self.goal.clear_accounting();
			},
			RestorePlan::ClearGoalMode => {
				self.goal.set_restored_state(None);
				self.goal.clear_accounting();
				if let Err(err) = self.store.append_mode_change(goal::MODE_NONE, None) {
					eprintln!("warning: {err}");
				}
			},
			RestorePlan::Restore { enabled, goal } => {
				self.goal.set_restored_state(Some(GoalModeState {
					enabled,
					mode: GoalMode::Active,
					goal,
				}));
				// Replayed without an explicit preserve: an active goal pauses.
				let _ = self.goal.on_thread_resumed(false);
				self.apply_goal_pending();
			},
		}
	}

	/// Between-turn headless continuation, gated by `goal.continuationModes`
	/// (`"interactive"` by default, so nothing loops unless the user opted in).
	fn drive_goal_continuation<W: Write, E: Write>(
		&mut self,
		mode: OutputMode,
		reporter: &mut Reporter<W, E>,
	) -> Result<()> {
		if !self.config.goal.enabled()
			|| !goal::headless_continuation_allowed(&self.config.goal.continuation_modes())
		{
			return Ok(());
		}
		loop {
			let Some(prompt) = self.goal.build_continuation_prompt() else {
				break;
			};
			self.turn_inner(
				None,
				prompt,
				Vec::new(),
				Some(goal::GOAL_CONTINUATION_CUSTOM_TYPE),
				mode,
				reporter,
			)?;
		}
		Ok(())
	}

	/// `/skill:<name>` expansion for the CLI prompt entry points. Unknown names
	/// and disabled skill commands leave the text untouched.
	fn expand_skill_command(&self, text: &str) -> Result<String> {
		if !self.skill_options.enable_skill_commands {
			return Ok(text.to_string());
		}
		Ok(skills::expand_skill_invocation(&self.skill_snapshot, text)?
			.unwrap_or_else(|| text.to_string()))
	}

	/// TS `AutoLearnController` auto-continue arm: run one isolated capture pass
	/// after a substantive turn. The capture reuses this conversation prefix and
	/// model but mounts only `manage_skill`/`learn`, drops goal/plan context and
	/// never writes to this session's journal.
	fn drive_autolearn_capture(&mut self, mode: OutputMode) {
		let (manage, learn) = self.autolearn_tool_presence();
		let goal_now = self.goal.has_goal();
		let plan_active = self
			.plan_yolo
			.as_ref()
			.is_some_and(|shared| shared.is_armed());
		if !autolearn::should_capture_turn(
			&self.config,
			self.turn_tool_calls,
			false,
			plan_active,
			self.turn_started_in_goal,
			goal_now,
			manage || learn,
		) {
			return;
		}
		let result = (|| -> Result<()> {
			let mut request =
				self.request(autolearn::auto_learn_nudge().to_string(), Vec::new(), mode)?;
			request.session_persisted = false;
			request.session_file = None;
			request.session_id = None;
			request.child_session_root = None;
			request.checkpoint_controller = None;
			request.turn_ack = None;
			request.goal = None;
			request.goal_context = None;
			request.plan_context = None;
			request.todo_phases = Vec::new();
			request.acp_bridge = None;
			request.extra_mcp_servers = Vec::new();
			request.config.tools = Some(Vec::new());
			request.extra_tools = self.autolearn_extra_tools();
			// Plan hooks stay off: plan mode is gated out above and a capture
			// must never stage or approve plan writes.
			request.plan_mode = agent::PlanModeHooks::default();
			// No advisor card may reach the primary session's sink from a
			// capture; the runtime gate also refuses an advisor manager.
			request.advisor_card_sink = None;
			// Capability ceiling: no MCP, no extension/eval bridge for capture.
			request.tool_ceiling = Some(agent::ToolCeiling {
				exclude_mcp:             true,
				exclude_extension_tools: true,
			});
			let stream = self.runner.run(&request)?;
			for event in stream {
				event?;
			}
			Ok(())
		})();
		if let Err(error) = result {
			eprintln!("warning: auto-learn capture failed: {error}");
		}
	}

	fn plan_artifacts(&self) -> Option<PathBuf> {
		self.store
			.file()
			.filter(|_| self.store.enabled())
			.and_then(session::artifacts_directory_for)
	}

	fn arm_plan_yolo(&mut self) -> Result<()> {
		let Some(shared) = self.plan_yolo.as_ref().cloned() else {
			return Ok(());
		};
		let Some(arming) = shared.arm() else {
			return Ok(());
		};
		let Some(_artifacts) = self.plan_artifacts() else {
			shared.rollback_arm(arming);
			return Err(Error::new(
				"plan-yolo requires a persisted session for plan artifacts",
			));
		};
		let tools = match plan_yolo::plan_tools(
			self.tools_enabled,
			self.config.tools.as_deref(),
		) {
			Ok(tools) => tools,
			Err(error) => {
				shared.rollback_arm(arming);
				return Err(Error::new(error));
			},
		};
		self.config.tools = tools.selected().map(|selected| selected.to_vec());
		self.plan_yolo_tools = Some(tools);
		Ok(())
	}

	fn plan_runtime(&self) -> (agent::PlanModeHooks, Option<String>) {
		let Some(shared) = self.plan_yolo.as_ref().filter(|shared| shared.is_armed()) else {
			return (agent::PlanModeHooks::default(), None);
		};
		let hooks = plan_yolo::plan_hooks(shared, self.plan_artifacts());
		let gate = shared.gate();
		let gate = gate.lock().unwrap_or_else(|poison| poison.into_inner());
		let context = gate.prompt_prefix(
			self.tools_enabled && self.tool_selected("ask"),
			self.tools_enabled && self.tool_selected("scout"),
			self.tools_enabled && self.tool_selected("task"),
		);
		(hooks, context)
	}

	fn request(
		&mut self,
		prompt: String,
		parts: Vec<ContentPart>,
		output_mode: OutputMode,
	) -> Result<RunRequest> {
		self.arm_plan_yolo()?;
		let (plan_mode, plan_context) = self.plan_runtime();
		let model = match (self.model.as_deref(), self.thinking) {
			(Some(model), Some(level)) => Some(runtime::apply_thinking(model, level)),
			(model, _) => model.map(str::to_string),
		};
		let history_messages = if !self.store.enabled() {
			// No journal: replay the exact branch conversation the loop last
			// accepted, so an in-memory rewind survives the next prompt.
			Some(self.in_memory_messages.clone().unwrap_or_default())
		} else if self.store.is_v1() {
			None
		} else {
			if let Some(warning) = self.store.branch_chain_warning() {
				eprintln!("warning: {warning}");
			}
			// Fail closed: a damaged journal must surface before the provider
			// call instead of silently replaying a degraded conversation.
			Some(CheckpointHost::context_messages(&self.store).map_err(Error::new)?)
		};
		let checkpoint_controller = self
			.config
			.checkpoint
			.enabled()
			.then(|| Arc::clone(self.checkpoint_host.controller()));
		// Journaled branches rehydrate from the log (it owns `user_todo_edit`
		// ordering); `--no-session` derives from the in-memory branch the runner
		// replayed, so a rewind/fork follows `self.history` with no reset hook.
		let session_persisted = self.store.enabled() && !self.store.is_v1();
		let todo_phases = if session_persisted {
			self.store.branch_todo_phases()
		} else {
			runtime::latest_todo_phases_from_turns(&self.history)
		};
		let goal_context = self.goal_mode_context(&todo_phases);
		// The launch resolved the live selection once; later prompts read that
		// single owner, so a downgrade or reset is never re-armed by the flag.
		let service_tier_by_family = self.service_tier_by_family.clone();
		let session_file = session_persisted
			.then(|| self.store.file().map(std::path::Path::to_path_buf))
			.flatten();
		// Session URLs resolve from this scope only: `local://` and `artifact://`
		// both address the session's own artifacts directory, exactly as the RPC
		// session scope does. An unpersisted run keeps the empty scope rather
		// than inventing a directory.
		let local_root = session_file
			.as_deref()
			.and_then(crate::session::artifacts_directory_for);
		// Same persisted-session gate as `RpcSession::child_session_root`: child
		// transcripts live beside the root's artifact store so `hub jobs` and
		// the task tool can inspect them. In-memory and v1 runs keep `None`
		// rather than inventing a path.
		let child_session_root = local_root.as_ref().map(|dir| dir.join("subagents"));
		let url_scope = pi_agent::tools::internal_urls::UrlScope {
			session_file: session_file.clone(),
			local_roots:  local_root
				.map(|dir| std::collections::BTreeMap::from([("local".to_string(), dir)]))
				.unwrap_or_default(),
		};
		// One card per advisor note batch reaches this sink exactly once; the
		// host persists and renders it from the receiver at event boundaries and
		// after the headless drain, so late advice is never dropped.
		let advisor_cards = Arc::clone(&self.advisor_cards_tx);
		let advisor_card_sink: omp_cli::advisor_runtime::CardSink =
			Arc::new(move |content: &str, details: &serde_json::Value| {
				if let Ok(tx) = advisor_cards.lock() {
					let _ = tx.send((content.to_string(), details.clone()));
				}
			});
		Ok(RunRequest {
			prompt,
			parts,
			cwd: self.cwd.clone(),
			output_mode,
			model,
			provider: self.provider.clone(),
			api_key: self.api_key.clone(),
			provider_session_id: self.provider_session_id.clone(),
			prompt_cache_key: self.prompt_cache_key.clone(),
			system_prompt: self.prompts.system.clone(),
			append_system_prompt: self.runtime_append_prompt(),
			context_files: self.prompts.files.clone(),
			skills: self.skill_snapshot.prompt_skills(),
			history: self.history.clone(),
			history_messages,
			checkpoint_controller,
			turn_ack: None,
			todo_phases,
			session_persisted,
			session_file,
			session_id: session_persisted.then(|| self.store.id().to_string()),
			config: self.config.clone(),
			catalog: Arc::clone(&self.catalog),
			approvals: self.approvals.clone(),
			max_time: self.max_time,
			max_turns: self.max_turns,
			tools_enabled: self.tools_enabled,
			cancellation_token: None,
			extra_tools: {
				let mut tools = self.goal_extra_tools();
				tools.extend(self.autolearn_extra_tools());
				tools
			},
			acp_bridge: None,
			extra_mcp_servers: Vec::new(),
			internal_urls: Some(Arc::clone(&self.internal_urls)),
			url_scope: Some(Arc::new(url_scope)),
			subagent_observer: None,
			child_session_root,
			goal: Some(Arc::clone(&self.goal)),
			goal_context,
			credential_resolver: None,
			plan_mode,
			tool_ceiling: None,
			skill_autoload: agent::AutoloadHandle(Some(Arc::new(
				skills::SnapshotAutoloadResolver::new(self.skill_snapshot.clone()),
			))),
			continuation: false,
			plan_context,
			advisor_card_sink: Some(agent::AdvisorCardHandle(advisor_card_sink)),
			service_tier_by_family,
			security_host: None,
			security_worker: false,
})
	}

	fn turn<W: Write, E: Write>(
		&mut self,
		context_prefix: Option<String>,
		raw_prompt: String,
		initial_parts: Vec<ContentPart>,
		mode: OutputMode,
		reporter: &mut Reporter<W, E>,
	) -> Result<()> {
		self.turn_inner(context_prefix, raw_prompt, initial_parts, None, mode, reporter)
	}

	/// One turn. `hidden_custom_type` marks an agent-initiated hidden prompt
	/// (goal continuation): it is journaled as a `custom_message` instead of a
	/// user message and never resets the `+Nk` budget window.
	fn turn_inner<W: Write, E: Write>(
		&mut self,
		context_prefix: Option<String>,
		raw_prompt: String,
		initial_parts: Vec<ContentPart>,
		hidden_custom_type: Option<&str>,
		mode: OutputMode,
		reporter: &mut Reporter<W, E>,
	) -> Result<()> {
		if self.store.is_v1() {
			return Err(Error::new(
				"cannot append to legacy v1 session log: read-only; use import_legacy_v1",
			));
		}
		if hidden_custom_type.is_none()
			&& let Some(shared) = self.plan_yolo.as_ref()
		{
			// A new user decision supersedes any approval left by an aborted
			// turn; the gate still requires a fresh proposal.
			shared.cancel_pending();
		}
		reporter.begin_turn()?;

		let now_start = std::time::SystemTime::now();
		let now_iso = output::iso8601(now_start);
		let now_ms = now_start
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap_or_default()
			.as_millis() as u64;

		let mentions = file_mentions::extract_file_mentions(&raw_prompt);
		let mention_msg = if !mentions.is_empty() {
			file_mentions::process_file_mentions(&mentions, &self.cwd)
		} else {
			None
		};

		let mut full_prompt = String::new();
		if let Some(ctx) = context_prefix {
			full_prompt.push_str(&ctx);
		}

		let mut parts = initial_parts;

		if let Some(msg) = &mention_msg {
			let text_block = msg.format_text_prompt_block();
			let image_block = msg.format_image_prompt_block();
			if !text_block.is_empty() {
				full_prompt.push_str(&text_block);
				full_prompt.push('\n');
			}
			if !image_block.is_empty() {
				full_prompt.push_str(&image_block);
				full_prompt.push('\n');
			}
			for file in msg.image_files() {
				if let Some(img) = &file.image {
					parts.push(ContentPart::image(&img.data_base64, &img.mime_type));
				}
			}
		}

		if !raw_prompt.is_empty() {
			full_prompt.push_str(&raw_prompt);
		}

		let has_images = parts.iter().any(ContentPart::is_image);
		if has_images {
			if let Some(requested) = self.model.as_deref() {
				let (provider, model_id) = match self.provider.as_deref() {
					Some(p) => {
						let prefix = format!("{p}/");
						let raw = requested.strip_prefix(&prefix).unwrap_or(requested);
						let (id, _) = runtime::split_effort(raw);
						(p.to_string(), id.to_string())
					},
					None => match requested.split_once('/') {
						Some((p, id)) if !p.is_empty() && !id.is_empty() => {
							let (id, _) = runtime::split_effort(id);
							(p.to_string(), id.to_string())
						},
						_ => {
							let (id, _) = runtime::split_effort(requested);
							let p = self
								.catalog
								.provider_of(id)
								.map(str::to_string)
								.unwrap_or_default();
							(p, id.to_string())
						},
					},
				};
				let supports_images = self.catalog.supports_images(&provider, &model_id).unwrap_or(true);
				if !supports_images {
					return Err(Error::new(format!(
						"model '{provider}/{model_id}' does not support image input"
					)));
				}
			}
		}

		// TS `#createMagicKeywordNotices`: a user prompt opens the `+Nk` budget
		// window and the goal accounting baseline before any request. A hidden
		// synthetic turn keeps the window the user set.
		let seed = self.goal_seed();
		self
			.goal
			.begin_turn(hidden_custom_type.is_none().then_some(full_prompt.as_str()), seed);
		self.goal
			.seed_background_dedup(session::durable_background_keys(&self.store));

		let (turn_parts, user_content) = if has_images {
			let mut assembled = Vec::new();
			if !full_prompt.is_empty() {
				assembled.push(ContentPart::text(&full_prompt));
			}
			for p in parts {
				if p.is_image() {
					assembled.push(p);
				}
			}
			(assembled.clone(), MessageContent::Parts(assembled))
		} else {
			(Vec::new(), MessageContent::Text(full_prompt.clone()))
		};

		let mut user_id = format!("msg-{}", output::session_id());
		let parent_id = self.store.leaf_id().map(str::to_string);

		let user_entry = MessageEntry::new(
			user_id.clone(),
			parent_id,
			&now_iso,
			SessionMessage::user(user_content, now_ms),
		);
		// Build the provider request before the journal gains this turn's user
		// entry: the typed rebuild must describe the conversation *before* the
		// prompt the loop is about to append, or every journaled turn replays it
		// twice. The entry still lands before the provider call for resume.
		let mut request = self.request(full_prompt.clone(), turn_parts.clone(), mode)?;
		if self.store.enabled() {
			if let Some(custom_type) = hidden_custom_type {
				// Hidden continuation: never replayed as a user prompt, but it is
				// part of the branch context a resumed session rebuilds.
				// The continuation entries chain to the persisted `custom_message`
				// entry, never to the unpersisted `msg-...` id above.
				match self.store.append_custom_message(
					custom_type,
					&full_prompt,
					false,
					serde_json::Value::Null,
				) {
					Ok(id) => user_id = id,
					Err(err) => eprintln!("warning: {err}"),
				}
			} else if let Err(err) = user_entry.validate() {
				eprintln!("warning: {err}");
			} else if let Err(err) = self.store.append_message(&user_entry) {
				eprintln!("warning: {err}");
			}
		}

		let in_memory_before = self.in_memory_messages.as_ref().map_or(0, Vec::len);

		let mut current_parent_id = Some(user_id);
		let mut tool_name_by_id: std::collections::HashMap<String, String> =
			std::collections::HashMap::new();
		let mut turn_tool_calls = 0usize;
		let turn_started_in_goal = self.goal.has_goal();

		let cancel_token = pi_agent::CancellationToken::new();
		if !self.store.enabled() {
			let user_message = runtime::user_message_from_parts(&full_prompt, &turn_parts)?;
			self
				.in_memory_messages
				.get_or_insert_with(Vec::new)
				.push(user_message);
		}
		request.cancellation_token = Some(cancel_token.clone());
		let turn_ack = pi_agent::TurnAck::new();
		request.turn_ack = Some(Arc::clone(&turn_ack));
		let _turn_ack_guard = TurnAckGuard(Arc::clone(&turn_ack));
		let started = std::time::Instant::now();
		let mut builder = if !turn_parts.is_empty() {
			TurnBuilder::new_with_parts(full_prompt, turn_parts)
		} else {
			TurnBuilder::new(full_prompt)
		};
		let mut stream = match self.runner.run(&request) {
			Ok(stream) => stream,
			Err(err) => {
				if let Some(messages) = self.in_memory_messages.as_mut() {
					messages.truncate(in_memory_before);
				}
				return Err(err);
			},
		};
		let mut reporter_failed = false;

		let session_config = &self.config;
		// Prewalk state is taken out of the session for the duration of the run
		// (the turn-end handler owns it), then restored below.
		let mut prewalk_state = std::mem::take(&mut self.prewalk);
		let mut prewalk_switch: Option<prewalk::AppliedSwitch> = None;
		let prewalk_catalog = Arc::clone(&self.catalog);
		let prewalk_api_key = self.api_key.clone();
		let prewalk_root_base_url = self.root_base_url.clone();
		let prewalk_model = self.model.clone();
		let prewalk_provider = self.provider.clone();
		let prewalk_thinking = self.thinking;
		let prewalk_cache_key = self.prompt_cache_key.clone();
		let prewalk_cache_inherited =
			self.prompt_cache_key_source == Some(PromptCacheKeySource::Inherited);
		let prewalk_todo_available =
			prewalk::todo_available(self.tools_enabled, self.config.tools.as_deref());
		// The downgrade effects touch `self`; the event closure only records it.
		let mut priority_downgraded = false;
		let plan_yolo_shared = self.plan_yolo.clone();
		let plan_yolo_tools_restore = self.plan_yolo_tools.clone();
		let plan_yolo_artifacts = self.plan_artifacts();
		let mut plan_yolo_switch: Option<(
			runtime::ModelSwitch,
			Option<Option<runtime::CliThinking>>,
			Option<String>,
		)> = None;
		let mut handle_event = |event: &agent::Event,
		                        store: &mut SessionStore,
		                        host: &CheckpointHost,
		                        ack: &Arc<pi_agent::TurnAck>,
		                        in_memory: &mut Option<Vec<pi_agent::Message>>,
		                        rewind_history: &mut Option<Vec<TurnRecord>>,
		                        parent: &mut Option<String>,
		                        tools: &mut std::collections::HashMap<String, String>,
		                        model_fallback: Option<&str>,
		                        provider_fallback: Option<&str>,
		                        prewalk_state: &mut prewalk::PrewalkState,
		                        prewalk_switch: &mut Option<prewalk::AppliedSwitch>,
		                        priority_downgraded: &mut bool| -> Result<()> {
			match event {
				agent::Event::ToolStart { id, tool, .. } => {
					tools.insert(id.clone(), tool.clone());
				},
				agent::Event::TurnEnd {
					message,
					tool_results,
					stop_reason,
					api,
					provider,
					model,
				} => {
					let now_evt = std::time::SystemTime::now();
					let evt_iso = output::iso8601(now_evt);
					let evt_ms = now_evt
						.duration_since(std::time::UNIX_EPOCH)
						.unwrap_or_default()
						.as_millis() as u64;

					let mut parts = Vec::new();
					// Keep the streamed block order end to end: the persisted
					// content array is the same order the deltas indexed, and
					// signatures/redactions survive so a resume replays them.
					let mut has_tool_part = false;
					for part in &message.parts {
						// Assistant content never persisted images before; keep
						// that shape.
						let Some(part) = ContentPart::from_agent_part(part) else {
							continue;
						};
						has_tool_part |= part.kind == "toolCall";
						parts.push(part);
					}
					if parts.is_empty() && !message.content.is_empty() {
						parts.push(ContentPart::text(message.content.clone()));
					}
					// Only legacy/completion messages lack ToolCall parts; append
					// their execution projection at the end, never alongside
					// ordered blocks.
					if !has_tool_part {
						for tc in &message.tool_calls {
							parts.push(ContentPart::tool_call(
								tc.id.clone(),
								tc.name.clone(),
								tc.arguments.clone(),
							));
						}
					}
					let content = if parts.is_empty() {
						MessageContent::Text(String::new())
					} else {
						MessageContent::Parts(parts)
					};

					// The assembled message knows the route that actually served
					// it; the event/config values only cover adapters that omit it.
					let asst_api = message
						.api
						.clone()
						.or_else(|| api.clone())
						.or_else(|| {
							provider
								.as_deref()
								.or(provider_fallback)
								.and_then(|p| session_config.providers.get(p).and_then(|c| c.api.clone()))
						})
						.unwrap_or_default();
					let asst_provider = message
						.provider
						.clone()
						.or_else(|| provider.clone())
						.or_else(|| provider_fallback.map(str::to_string))
						.unwrap_or_default();
					let asst_model = message
						.model
						.clone()
						.or_else(|| model.clone())
						.or_else(|| model_fallback.map(str::to_string))
						.unwrap_or_default();

					let asst_id = format!("msg-{}", output::session_id());
					// The message's own terminal reason is authoritative; the
					// event string only covers adapters that do not report one.
					let canon_stop = message
						.stop_reason
						.map(pi_agent::StopReason::as_str)
						.unwrap_or_else(|| session::canonical_stop_reason(&stop_reason));
					let mut asst_msg = SessionMessage::assistant(
						content,
						asst_api,
						asst_provider,
						asst_model,
						session::reported_usage(message),
						canon_stop,
						evt_ms,
					);
					asst_msg.error_message = message.error_message.clone().or_else(|| {
						(canon_stop == "aborted").then(|| "Interrupted by user".to_string())
					});
					asst_msg.attribution =
						message.attribution.map(|value| value.as_str().to_string());
					asst_msg.disabled_features = message.disabled_features.clone();
					if message
						.disabled_features
						.as_deref()
						.is_some_and(|features| features.iter().any(|feature| feature == "priority"))
					{
						*priority_downgraded = true;
					}
					let asst_entry = MessageEntry::new(
						asst_id.clone(),
						parent.take(),
						&evt_iso,
						asst_msg,
					);

					let mut turn_entries = vec![asst_entry];
					let mut prev_id = asst_id;

					for tr in tool_results {
						let res_id = format!("msg-{}", output::session_id());
						let tool_name = tools
							.get(&tr.tool_call_id)
							.cloned()
							.unwrap_or_default();
						let mut tool_msg = SessionMessage::tool_result(
							tr.tool_call_id.clone(),
							tool_name,
							MessageContent::Text(tr.content.clone()),
							tr.is_error,
							evt_ms,
						);
						if let Some(details) = &tr.details {
							tool_msg.extra.insert("details".to_string(), details.clone());
						}
						let tool_entry = MessageEntry::new(
							res_id.clone(),
							Some(prev_id.clone()),
							&evt_iso,
							tool_msg,
						);
						turn_entries.push(tool_entry);
						prev_id = res_id;
					}

					let mut valid_turn_entries = Vec::new();
					for entry in turn_entries {
						if let Err(err) = entry.validate() {
							eprintln!("warning: {err}");
						} else {
							valid_turn_entries.push(entry);
						}
					}
					if !valid_turn_entries.is_empty() {
						if let Err(err) = store.append_messages(&valid_turn_entries) {
							eprintln!("warning: {err}");
						}

					}
					// The journal leaf (a downgrade metadata entry included) is the
					// ancestry for the next assistant turn, so a tool continuation
					// cannot orphan the recorded reset; the non-persistent path keeps
					// the in-memory id chain.
					*parent = if store.enabled() {
						store.leaf_id().map(str::to_string).or(Some(prev_id))
					} else {
						Some(prev_id)
					};

					if !store.enabled()
						&& let Some(messages) = in_memory.as_mut()
					{
						messages.push(message.clone());
						for tr in tool_results {
							let name = tools
								.get(&tr.tool_call_id)
								.map(String::as_str)
								.unwrap_or("tool");
							messages.push(pi_agent::Message::tool_with_status(
								&tr.tool_call_id,
								name,
								&tr.content,
								tr.is_error,
							));
						}
					}

					// The loop waits for this reply before deciding to continue.
					let pending = ack.take();
					let turn_start = pending
						.as_ref()
						.map_or(0, |(request, _)| request.turn_start);
					let mut decision = pi_agent::TurnAckDecision::default();
					if store.enabled() {
						let captured = host.note_persisted_tool_results(store, &valid_turn_entries);
						if let Err(err) = host.persist_active_reminder(store) {
							eprintln!("warning: {err}");
						}
						if captured {
							decision.warning = host.active_notice().map(str::to_string);
						}
						match host.apply_pending_rewind(store) {
							Ok(Some(applied)) => {
								// Same-batch checkpoint+rewind: the checkpoint is no
								// longer active, so its notice must not contradict the
								// retained report.
								decision.warning = None;
								let rebuilt = {
									// Debug-only fault injection: a production rebuild
									// cannot fail this way, because the typed request gate
									// rejects an unreplayable journal before the turn.
									#[cfg(debug_assertions)]
									if std::env::var_os("OMP_TEST_FAIL_REWIND_REBUILD").is_some() {
										Err("injected rewind rebuild failure".to_string())
									} else {
										CheckpointHost::context_messages(store)
									}
									#[cfg(not(debug_assertions))]
									CheckpointHost::context_messages(store)
								};
								match rebuilt {
									Ok(messages) => {
										if let Some(runner) = agent::runner() {
											let (id, file) = advisor_scope_for(store);
											runner.advisor_reset_boundary(
												id.as_deref(),
												file.as_deref(),
												&messages,
											);
										}
										decision.rewrite = Some(pi_agent::TurnRewrite::Replace(messages));
									},
									// The journal already moved to the rewound branch.
									// Cancel the provider loop *before* releasing the parked
									// ack, or it can observe the default decision and start
									// the next request; then fail the turn.
									Err(err) => {
										cancel_token.cancel();
										if let Some((_, reply)) = pending {
											let _ = reply.send(pi_agent::TurnAckDecision::default());
										}
										return Err(Error::new(format!(
											"cannot rebuild branch context after rewind: {err}"
										)));
									},
								}
								*rewind_history = Some(applied.history);
								*parent = store.leaf_id().map(str::to_string);
							},
							Ok(None) => {},
							Err(err) => eprintln!("warning: cannot apply rewind: {err}"),
						}
					} else {
						let captured =
							host.note_unpersisted_tool_results(tool_results, tools, turn_start);
						if let Some(rewind) = host.apply_pending_rewind_in_memory() {
							decision.rewrite = match in_memory.as_mut() {
								Some(messages) => {
									// The mirror is the uncompacted branch; replay it
									// wholesale so the current run does not depend on
									// the checkpoint result surviving compaction.
									omp_cli::session_checkpoint::apply_in_memory_rewind(
										messages, &rewind,
									);
									if let Some(runner) = agent::runner() {
										let (id, file) = advisor_scope_for(store);
										runner.advisor_reset_boundary(
											id.as_deref(),
											file.as_deref(),
											messages,
										);
									}
									Some(pi_agent::TurnRewrite::Replace(messages.clone()))
								},
								None => {
									if let Some(runner) = agent::runner() {
										let (id, file) = advisor_scope_for(store);
										runner.advisor_reset_boundary(
											id.as_deref(),
											file.as_deref(),
											&[],
										);
									}
									Some(pi_agent::TurnRewrite::InMemory(rewind))
								},
							};
						} else if captured {
							decision.warning = host.active_notice().map(str::to_string);
						}
					}
					if host.needs_rewind_before_yield() && message.tool_calls.is_empty() {
						if let Some(warning) = host.controller().yield_warning() {
							decision.continue_turn = true;
							decision.warning = Some(warning.to_string());
						}
					}
					if !store.enabled()
						&& let Some(warning) = decision.warning.as_deref().filter(|w| !w.is_empty())
						&& let Some(messages) = in_memory.as_mut()
					{
						messages.push(pi_agent::Message::system(warning));
					}

					// Prewalk: advance the one-way switch at this settled boundary.
					// A terminal provider turn (error/aborted) never starts another
					// request, so it must not swap the provider or inject guidance.
					let terminal = matches!(
						message.stop_reason,
						Some(pi_agent::StopReason::Error | pi_agent::StopReason::Aborted)
					);
					if !terminal
						&& let Some(arm) = prewalk_state.target().cloned()
					{
						let noop = prewalk::would_be_noop(
							&prewalk_catalog,
							&arm.target,
							arm.thinking,
							prewalk_provider.as_deref(),
							prewalk_model.as_deref(),
							prewalk_thinking,
						);
						// TS `isPrewalkImplementationAction` keys on the
						// tool/typed tier only; a failed edit/write still counts.
						let action_tool = tool_results.iter().find_map(|result| {
							let name =
								tools.get(&result.tool_call_id).map(String::as_str)?;
							prewalk::is_implementation_action(
								name,
								result.details.as_ref(),
							)
							.then_some(name)
						});
						let todo_succeeded = tool_results.iter().any(|result| {
							!result.is_error
								&& tools.get(&result.tool_call_id).map(String::as_str)
									== Some("todo")
						});
						let boundary = prewalk::TurnBoundary {
							has_tool_results: !tool_results.is_empty(),
							todo_succeeded,
							action: action_tool,
							todo_available: prewalk_todo_available,
							noop,
						};
						let action = prewalk_state.advance(&boundary);
						for notice in &action.notices {
							eprintln!("{notice}");
						}
						if action.scrub_plan {
							decision.scrub_custom_type =
								Some(prewalk::PREWALK_PLAN_TYPE.to_string());
						}
						let mut guidance = action.guidance;
						if let Some(target) = action.switch {
							// A root credential follows a switch only to the same
							// provider **and** the same effective endpoint. Target
							// models may override `baseUrl`, so a provider id match
							// alone would leak the root secret to another origin.
							let target_endpoint = runtime::effective_endpoint(
								&prewalk_catalog,
								session_config,
								&target.provider,
								&target.model,
							)
							.ok();
							let reuses_root = prewalk_provider.as_deref()
								== Some(target.provider.as_str())
								&& target_endpoint.is_some()
								&& target_endpoint.as_deref() == prewalk_root_base_url.as_deref();
							let api_key = if reuses_root {
								prewalk_api_key.clone()
							} else {
								session_config.resolve_api_key(&target.provider)
							};
							let target_auth = reuses_root
								|| api_key.as_deref().is_some_and(|key| !key.trim().is_empty())
								|| runtime::provider_has_configured_auth(
									&prewalk_catalog,
									session_config,
									&target.provider,
									None,
								);
							if !target_auth {
								// The root key was not copied across origins and no
								// independent target credential exists: disarm
								// loudly instead of calling the endpoint unauthenticated.
								eprintln!(
									"warning: prewalk switch skipped — no independent credential for {}/{}",
									target.provider, target.model
								);
								guidance.clear();
							} else {
							// Explicit suffix wins; otherwise apply the target's
							// declared default clamped through the same resolver.
							let switch_thinking = action.switch_thinking.map(Some).or_else(|| {
								match runtime::model_default_thinking(
									&prewalk_catalog,
									&target.provider,
									&target.model,
								) {
									runtime::DefaultThinking::Unspecified => None,
									runtime::DefaultThinking::Resolved(level) => Some(level),
								}
							});
							let mut switch_target = target.clone();
							if switch_target.effort.is_none()
								&& let Some(Some(runtime::CliThinking::Effort(effort))) =
									switch_thinking
							{
								switch_target.effort = Some(effort.to_string());
							}
							let cache_key = if prewalk_cache_inherited {
								None
							} else {
								prewalk_cache_key.clone()
							};
							match runtime::build_switch_provider(
								&prewalk_catalog,
								session_config,
								&switch_target,
								api_key.clone(),
								// Provider session belongs to the previous route.
								None,
								cache_key,
								&store.service_tier_by_family(),
							) {
								Ok(provider) => {
									// Ephemeral role: resume/fork fall back to the
									// configured default instead of the fast model.
									if store.enabled() {
										match store.append_model_change(
											&format!("{}/{}", target.provider, target.model),
											"fallback",
										) {
											Ok(id) => *parent = Some(id),
											Err(err) => eprintln!(
												"warning: cannot record prewalk model change: {err}"
											),
										}
									}
									for (custom_type, content) in &action.journal {
										if store.enabled() {
											match store.append_custom_message(
												custom_type,
												content,
												false,
												serde_json::Value::Null,
											) {
												Ok(id) => *parent = Some(id),
												Err(err) => eprintln!("warning: {err}"),
											}
										} else if let Some(messages) = in_memory.as_mut() {
											messages.push(pi_agent::Message::guidance(
												*custom_type,
												*content,
											));
										}
									}
									decision.provider = Some(provider);
									decision.context_limit = Some(
										prewalk_catalog
											.context_window(&target.provider, &target.model)
											.unwrap_or(runtime::DEFAULT_CONTEXT_LIMIT),
									);
									*prewalk_switch = Some(prewalk::AppliedSwitch {
										target: switch_target,
										thinking: switch_thinking,
										api_key,
									});
									decision.continue_turn |= action.continue_turn;
									decision.guidance = guidance;
								},
								Err(err) => {
									// Startup already resolved this target; a setup
									// failure here must not silently claim a switch.
									eprintln!("warning: prewalk switch failed — {err}");
									guidance.clear();
								},
							}
							}
						} else {
							for (custom_type, content) in &action.journal {
								if store.enabled() {
									match store.append_custom_message(
										custom_type,
										content,
										false,
										serde_json::Value::Null,
									) {
										Ok(id) => *parent = Some(id),
										Err(err) => eprintln!("warning: {err}"),
									}
								} else if let Some(messages) = in_memory.as_mut() {
									messages.push(pi_agent::Message::guidance(
										*custom_type,
										*content,
									));
								}
							}
							decision.continue_turn |= action.continue_turn;
							decision.guidance = guidance;
						}
					}

					if terminal {
						if let Some(shared) = plan_yolo_shared.as_ref() {
							shared.cancel_pending();
						}
					} else if let Some(shared) = plan_yolo_shared.clone()
						&& shared.has_pending()
					{
						let committed = shared.commit_pending(
							plan_yolo_artifacts.as_deref(),
							|pending| {
								let target = pending.target().clone();
								let switch_thinking =
									pending.thinking().map(Some).or_else(|| {
										match runtime::model_default_thinking(
											&prewalk_catalog,
											&target.provider,
											&target.model,
										) {
											runtime::DefaultThinking::Unspecified => None,
											runtime::DefaultThinking::Resolved(level) => {
												Some(level)
											},
										}
									});
								let mut switch_target = target.clone();
								if switch_target.effort.is_none()
									&& let Some(Some(runtime::CliThinking::Effort(
										effort,
									))) = switch_thinking
								{
									switch_target.effort = Some(effort.to_string());
								}
								let target_endpoint = runtime::effective_endpoint(
									&prewalk_catalog,
									session_config,
									&target.provider,
									&target.model,
								)
								.ok();
								let reuses_root = prewalk_provider.as_deref()
									== Some(target.provider.as_str())
									&& target_endpoint.is_some()
									&& target_endpoint.as_deref()
										== prewalk_root_base_url.as_deref();
								let api_key = if reuses_root {
									prewalk_api_key.clone()
								} else {
									session_config.resolve_api_key(&target.provider)
								};
								let stored_auth = reuses_root
									|| api_key
										.as_deref()
										.is_some_and(|key| !key.trim().is_empty())
									|| runtime::provider_has_configured_auth(
										&prewalk_catalog,
										session_config,
										&target.provider,
										None,
									);
								if !stored_auth {
									return Err(format!(
										"no independent credential for {}/{}",
										target.provider, target.model
									));
								}
								let cache_key = if prewalk_cache_inherited {
									None
								} else {
									prewalk_cache_key.clone()
								};
								let provider = runtime::build_switch_provider(
									&prewalk_catalog,
									session_config,
									&switch_target,
									api_key.clone(),
									None,
									cache_key,
									&store.service_tier_by_family(),
								)?;
								Ok((provider, switch_target, switch_thinking, api_key))
							},
						);
						match committed {
							Ok((
								commit,
								(provider, target, switch_thinking, api_key),
							)) => {
								if store.enabled() {
									match store.append_model_change(
										&format!(
											"{}/{}",
											target.provider, target.model
										),
										"fallback",
									) {
										Ok(id) => *parent = Some(id),
										Err(err) => eprintln!(
											"warning: cannot record plan-yolo model change: {err}"
										),
									}
								}
								decision.provider = Some(provider);
								decision.context_limit = Some(
									prewalk_catalog
										.context_window(
											&target.provider,
											&target.model,
										)
										.unwrap_or(runtime::DEFAULT_CONTEXT_LIMIT),
								);
								decision.guidance.push(
									pi_agent::Message::guidance(
										plan_yolo::PLAN_YOLO_HANDOFF_TYPE,
										commit.handoff,
									),
								);
								decision.continue_turn = true;
								decision.tool_selection = plan_yolo_tools_restore
									.as_ref()
									.map(|tools| pi_agent::TurnToolSelection::Hide(
										tools.hidden_by_plan(),
									));
								eprintln!("{}", commit.notice);
								plan_yolo_switch =
									Some((target, switch_thinking, api_key));
							},
							Err(err) => {
								let message = format!("plan-yolo switch failed — {err}");
								if store.enabled()
									&& let Err(append) =
										store.append_plan_yolo_switch_failure(&message)
								{
									eprintln!(
										"warning: cannot record plan-yolo switch failure: {append}"
									);
								}
								return Err(Error::new(message));
							},
						}
					}

					if let Some((_, reply)) = pending {
						let _ = reply.send(decision);
					}
				},
				_ => {},
			}
			Ok(())
		};

		let model_opt = self.model.clone();
		let provider_opt = self.provider.clone();
		let mut rewind_history: Option<Vec<TurnRecord>> = None;
		let drive_result = deadline::drive(
			&mut stream,
			self.deadline,
			|| started.elapsed(),
			|event| {
				builder.observe(event);
				if matches!(event, agent::Event::ToolEnd { .. }) {
					turn_tool_calls += 1;
				}
				handle_event(
					event,
					&mut self.store,
					&self.checkpoint_host,
					&turn_ack,
					&mut self.in_memory_messages,
					&mut rewind_history,
					&mut current_parent_id,
					&mut tool_name_by_id,
					model_opt.as_deref(),
					provider_opt.as_deref(),
					&mut prewalk_state,
					&mut prewalk_switch,
					&mut priority_downgraded,
				)?;
				// A goal tool transition queued during this event lands before the
				// turn-end entries, like TS `appendModeChange` during execution.
				apply_goal_intents(&mut self.store, &self.goal);
				if let Err(err) = reporter.event(event) {
					reporter_failed = true;
					return Err(err);
				}
				// Advice cards reach the host through the session-scoped sink;
				// persist/render them at this boundary so they cannot be lost
				// between turns or after the stream drops.
				while let Ok((content, details)) = self.advisor_cards_rx.try_recv() {
					if let Err(err) =
						persist_advisor_card(&mut self.store, reporter, &content, &details)
					{
						reporter_failed = true;
						return Err(err);
					}
				}
				Ok(())
			},
		);


		if drive_result.is_err() {
			cancel_token.cancel();
			if let Some(shared) = self.plan_yolo.as_ref() {
				shared.cancel_pending();
			}
			for event in stream {
				if let Ok(event) = event {
					builder.observe(&event);
					let _ = handle_event(
						&event,
						&mut self.store,
						&self.checkpoint_host,
						&turn_ack,
						&mut self.in_memory_messages,
						&mut rewind_history,
						&mut current_parent_id,
						&mut tool_name_by_id,
						model_opt.as_deref(),
						provider_opt.as_deref(),
						&mut prewalk_state,
						&mut prewalk_switch,
					&mut priority_downgraded,
					);
					if !reporter_failed {
						let _ = reporter.event(&event);
					}
					while let Ok((content, details)) = self.advisor_cards_rx.try_recv() {
						if persist_advisor_card(
							&mut self.store,
							reporter,
							&content,
							&details,
						)
						.is_err()
						{
							reporter_failed = true;
						}
					}
				}
			}
		}
		drop(handle_event);
		self.turn_tool_calls = turn_tool_calls;
		self.turn_started_in_goal = turn_started_in_goal;
		if priority_downgraded
			&& self.service_tier_by_family.get("anthropic").map(String::as_str)
				== Some("priority")
		{
			// One authoritative update after the turn: the journal entry appended
			// here becomes the branch leaf, so the next assistant (tool
			// continuation or resume) stays downgraded.
			self.service_tier_by_family.remove("anthropic");
			self.service_tier_recorded = true;
			if self.store.enabled()
				&& let Err(err) =
					self.store.append_service_tier_change(&self.service_tier_by_family)
			{
				eprintln!("warning: cannot record service tier change: {err}");
			}
			eprintln!(
				"warning: Priority/fast mode rejected for this model; retried without it. Fast mode is now off."
			);
		}
		self.prewalk = prewalk_state;
		// A switch applied mid-turn reaches later prompts through the session
		// fields; the journal already carries the ephemeral `fallback` role.
		if let Some(applied) = prewalk_switch.take() {
			self.provider = Some(applied.target.provider);
			self.model = Some(applied.target.model);
			if let Some(level) = applied.thinking {
				self.thinking = level;
			}
			self.api_key = applied.api_key;
			self.root_base_url = self
				.provider
				.as_deref()
				.zip(self.model.as_deref())
				.and_then(|(provider, model)| {
					runtime::effective_endpoint(&self.catalog, &self.config, provider, model).ok()
				});
			// The sticky provider session and an inherited cache key belonged to
			// the previous route (TS `setModelWithProviderSessionReset`).
			self.provider_session_id = None;
			invalidate_inherited_cache_key(
				&mut self.prompt_cache_key,
				&mut self.prompt_cache_key_source,
			);
		}
		if let Some((target, thinking, api_key)) = plan_yolo_switch.take() {
			self.provider = Some(target.provider);
			self.model = Some(target.model);
			self.api_key = api_key;
			if let Some(level) = thinking {
				self.thinking = level;
			}
			self.provider_session_id = None;
			invalidate_inherited_cache_key(
				&mut self.prompt_cache_key,
				&mut self.prompt_cache_key_source,
			);
			self.root_base_url = self
				.provider
				.as_deref()
				.zip(self.model.as_deref())
				.and_then(|(provider, model)| {
					runtime::effective_endpoint(&self.catalog, &self.config, provider, model)
						.ok()
				});
			self.config.tools = plan_yolo_tools_restore
				.and_then(|tools| tools.restore());
			self.plan_yolo_tools = None;
		}
		// Drain the final goal writes (agent-end flush, interrupt pause) and, if
		// the goal tool completed this goal, write `none` + the terminal metric.
		self.finish_goal_turn();

		let mut record = builder.finish();
		if let Err(err) = drive_result {
			let canon = session::canonical_stop_reason(&record.stop_reason);
			if record.stop_reason.is_empty() || canon == "toolUse" {
				record.stop_reason = "aborted".to_string();
			}
			self.checkpoint_host.abandon_in_memory_rewind();
			if let Some(messages) = self.in_memory_messages.as_mut() {
				messages.truncate(in_memory_before);
			}
			if let Some(history) = rewind_history.take() {
				self.history = history;
			}
			self.history.push(record);
			return Err(err);
		}
		let rewound = self.checkpoint_host.finish_in_memory_rewind();
		if let Some(history) = rewind_history.take() {
			self.history = history;
		} else if !rewound {
			self.history.push(record);
		}
		Ok(())
	}
}

/// TS `maybeAutoChdir`: a launch from `$HOME` moves to the first existing
/// candidate (`$HOME/tmp`, `/tmp`, `/var/tmp`, else the OS temp dir). An
/// explicit `--cwd` or `--allow-home` opts out. This changes the process cwd
/// only; it grants no permission and does not confine any path.
fn maybe_auto_chdir(cli: &Cli) {
	if cli.allow_home || cli.cwd.is_some() {
		return;
	}
	let Some(home) = dirs::home_dir() else {
		return;
	};
	let Ok(cwd) = std::env::current_dir() else {
		return;
	};
	let comparable = |path: &Path| std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
	if comparable(&cwd) != comparable(&home) {
		return;
	}
	for candidate in [home.join("tmp"), PathBuf::from("/tmp"), PathBuf::from("/var/tmp")] {
		if candidate.is_dir() && std::env::set_current_dir(&candidate).is_ok() {
			return;
		}
	}
	let fallback = std::env::temp_dir();
	if comparable(&fallback) != comparable(&cwd) && fallback.is_dir() {
		let _ = std::env::set_current_dir(fallback);
	}
}

/// Thinking levels accepted as a `--models` suffix (TS `parseThinkingSuffix`
/// plus the `auto` sentinel coerced by the scope resolver).
const MODEL_THINKING_SUFFIXES: [&str; 8] =
	["off", "minimal", "low", "medium", "high", "xhigh", "max", "auto"];

fn split_model_thinking_suffix(pattern: &str) -> (&str, Option<&str>) {
	match pattern.rsplit_once(':') {
		Some((base, level)) if MODEL_THINKING_SUFFIXES.contains(&level) => (base, Some(level)),
		_ => (pattern, None),
	}
}

/// First concrete model matched by an explicit `--models` scope, in pattern
/// order and then catalog order (TS `resolveModelScope`). Glob patterns match
/// `provider/id` or the bare id case-insensitively; non-glob patterns match
/// exactly, then by case-insensitive substring (TS fuzzy selector). The
/// returned thinking level is the `:level` suffix carried by the winning
/// pattern (TS scoped-model thinking, `main.ts` initialThinking).
fn first_scoped_model(
	patterns: &[String],
	catalog: &Catalog,
) -> Option<(String, Option<String>)> {
	for pattern in patterns {
		let raw = pattern.trim();
		if raw.is_empty() {
			continue;
		}
		let (stripped, suffix) = split_model_thinking_suffix(raw);
		let suffix = suffix.map(str::to_string);
		if raw.contains('*') || raw.contains('?') || raw.contains('[') {
			let Ok(glob) = globset::GlobBuilder::new(stripped)
				.case_insensitive(true)
				.literal_separator(true)
				.build()
			else {
				continue;
			};
			let matcher = glob.compile_matcher();
			if let Some(model) = find_in_catalog(catalog, |full, id| {
				matcher.is_match(full) || matcher.is_match(id)
			}) {
				return Some((model, suffix));
			}
			continue;
		}
		// A literal id ending in `:level` keeps priority over suffix parsing.
		if let Some(model) = find_in_catalog(catalog, |full, id| {
			full.eq_ignore_ascii_case(raw) || id.eq_ignore_ascii_case(raw)
		}) {
			return Some((model, None));
		}
		if stripped == raw {
			let needle = raw.to_lowercase();
			if let Some(model) =
				find_in_catalog(catalog, |_, id| id.to_lowercase().contains(&needle))
			{
				return Some((model, None));
			}
			continue;
		}
		if let Some(model) = find_in_catalog(catalog, |full, id| {
			full.eq_ignore_ascii_case(stripped) || id.eq_ignore_ascii_case(stripped)
		}) {
			return Some((model, suffix));
		}
		let needle = stripped.to_lowercase();
		if let Some(model) =
			find_in_catalog(catalog, |_, id| id.to_lowercase().contains(&needle))
		{
			return Some((model, suffix));
		}
	}
	None
}

fn find_in_catalog(
	catalog: &Catalog,
	mut matches: impl FnMut(&str, &str) -> bool,
) -> Option<String> {
	for (provider, models) in &catalog.providers {
		for id in models.keys() {
			let full = format!("{provider}/{id}");
			if matches(&full, id) {
				return Some(full);
			}
		}
	}
	None
}

fn resolve_cwd(cli_cwd: Option<PathBuf>, config_cwd: Option<PathBuf>) -> Result<PathBuf> {
	let base = std::env::current_dir()?;
	let Some(path) = cli_cwd.or(config_cwd) else {
		return Ok(base);
	};
	let path = dirs::absolute(&base, &path);
	std::fs::canonicalize(&path)
		.map_err(|err| Error::with_source(format!("cwd {} is not accessible", path.display()), err))
}


#[cfg(unix)]
fn open_file_safe(path: &Path) -> std::io::Result<std::fs::File> {
	use std::os::unix::fs::OpenOptionsExt;
	#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd", target_os = "dragonfly"))))]
	const O_NONBLOCK: i32 = 0o4000;
	#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd", target_os = "dragonfly"))]
	const O_NONBLOCK: i32 = 0x0004;

	let mut options = std::fs::OpenOptions::new();
	options.read(true);
	options.custom_flags(O_NONBLOCK);
	options.open(path)
}

#[cfg(not(unix))]
fn open_file_safe(path: &Path) -> std::io::Result<std::fs::File> {
	std::fs::File::open(path)
}

// Mirrors TS `readPipedInput` (packages/coding-agent/src/main.ts:199-217): the read is
// unbounded — a timer that stops early would silently drop input — but the wait must be
// audible, so say what we are blocked on once per silent interval.
const STDIN_WAIT_NOTICE_MS: std::os::raw::c_int = 1_000;
const STDIN_WAIT_NOTICE_REPEAT_MS: i32 = 10_000;

#[cfg(unix)]
fn poll_stdin(timeout_ms: std::os::raw::c_int) -> std::io::Result<bool> {
	#[repr(C)]
	struct PollFd {
		fd: std::os::raw::c_int,
		events: std::os::raw::c_short,
		revents: std::os::raw::c_short,
	}
	// `read` can proceed on any of these: a hangup means the writer closed (read yields 0/EOF),
	// an error surfaces through `read` itself. Treating only POLLIN as ready would spin forever
	// on an EOF'd pipe, where Linux reports POLLHUP and no POLLIN.
	const READY: std::os::raw::c_short = 0x0001 | 0x0008 | 0x0010 | 0x0020; // POLLIN|POLLERR|POLLHUP|POLLNVAL
	const EINTR: i32 = 4;
	unsafe extern "C" {
		fn poll(fds: *mut PollFd, nfds: usize, timeout_ms: std::os::raw::c_int) -> std::os::raw::c_int;
	}
	let mut pfd = PollFd {
		fd: 0,
		events: 0x0001,
		revents: 0,
	};
	loop {
		let ret = unsafe { poll(&mut pfd, 1, timeout_ms) };
		if ret == 0 {
			return Ok(false);
		}
		if ret > 0 {
			return Ok((pfd.revents & READY) != 0);
		}
		// Interrupted by a signal: retry. Anything else must surface, not masquerade as a timeout.
		let err = std::io::Error::last_os_error();
		if err.raw_os_error() != Some(EINTR) {
			return Err(err);
		}
		pfd.revents = 0;
	}
}

/// Reads stdin to EOF, printing the wait notice while a pipe stays silent.
#[cfg(unix)]
fn read_stdin_to_eof() -> std::io::Result<Vec<u8>> {
	let mut bytes = Vec::new();
	let mut buf = [0u8; 8192];
	let mut silent_ms: i32 = 0;
	loop {
		if !poll_stdin(STDIN_WAIT_NOTICE_MS)? {
			silent_ms = silent_ms.saturating_add(STDIN_WAIT_NOTICE_MS);
			if silent_ms == STDIN_WAIT_NOTICE_MS || silent_ms % STDIN_WAIT_NOTICE_REPEAT_MS == 0 {
				eprintln!("Reading prompt from piped stdin (waiting for EOF; ctrl+c to abort)…");
			}
			continue;
		}
		silent_ms = 0;
		match io::stdin().read(&mut buf) {
			Ok(0) => break,
			Ok(n) => bytes.extend_from_slice(&buf[..n]),
			Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
			Err(err) => return Err(err),
		}
	}
	Ok(bytes)
}

#[cfg(not(unix))]
fn read_stdin_to_eof() -> std::io::Result<Vec<u8>> {
	let mut bytes = Vec::new();
	io::stdin().read_to_end(&mut bytes)?;
	Ok(bytes)
}

fn read_piped_stdin() -> std::io::Result<Option<String>> {
	if io::stdin().is_terminal() {
		return Ok(None);
	}
	let bytes = read_stdin_to_eof()?;
	let text = String::from_utf8(bytes).map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
	if text.trim().is_empty() {
		Ok(None)
	} else {
		Ok(Some(text))
	}
}

#[derive(Debug, Default, Clone)]
struct ProcessedCliFiles {
	text: String,
	images: Vec<ContentPart>,
}

fn process_cli_file_args(file_args: &[String], cwd: &Path) -> std::result::Result<ProcessedCliFiles, CliError> {
	let mut text = String::new();
	let mut images = Vec::new();
	for file_arg in file_args {
		let resolved = file_mentions::resolve_path(file_arg, cwd);
		let abs_path = dirs::absolute(cwd, &resolved);

		if abs_path.is_dir() {
			let listing = file_mentions::build_directory_listing(&abs_path).map_err(|err| {
				CliError::fail(format!(
					"Error: Could not read directory {}: {err}",
					abs_path.display()
				))
			})?;
			text.push_str(&format!(
				"<file name=\"{}\">\n{}\n</file>\n",
				file_mentions::escape_xml_attr(&abs_path.display().to_string()),
				listing.output
			));
			continue;
		}

		let mut file = match open_file_safe(&abs_path) {
			Ok(f) => f,
			Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
				return Err(CliError::fail(format!(
					"Error: File not found: {}",
					abs_path.display()
				)));
			},
			Err(err) => {
				return Err(CliError::fail(format!(
					"Error: Could not read file {}: {err}",
					abs_path.display()
				)));
			},
		};

		let opened_meta = match file.metadata() {
			Ok(m) => m,
			Err(err) => {
				return Err(CliError::fail(format!(
					"Error: Could not read file {}: {err}",
					abs_path.display()
				)));
			},
		};

		if !opened_meta.is_file() {
			return Err(CliError::fail(format!(
				"Error: Could not read file {}: not a regular file",
				abs_path.display()
			)));
		}

		if opened_meta.len() == 0 {
			continue;
		}

		let mut header = [0u8; 512];
		let mut n = 0;
		while n < header.len() {
			match file.read(&mut header[n..]) {
				Ok(0) => break,
				Ok(bytes_read) => n += bytes_read,
				Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
				Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
				Err(err) => {
					return Err(CliError::fail(format!(
						"Error: Could not read file {}: {err}",
						abs_path.display()
					)));
				},
			}
		}

		let detected_image_mime = file_mentions::detect_image_mime(&header[..n]);
		let initial_buf = header[..n].to_vec();

		if let Some(mime) = detected_image_mime {
			let max_bytes = file_mentions::MAX_AUTO_READ_IMAGE_BYTES;
			let bytes = match file_mentions::read_bounded(
				&mut file,
				initial_buf,
				max_bytes,
			) {
				Ok(b) => b,
				Err(file_mentions::BoundedReadError::TooLarge { actual_read }) => {
					let disk_size = file.metadata().map(|m| m.len()).unwrap_or(0);
					let reported_size = if disk_size > max_bytes {
						disk_size
					} else {
						actual_read
					};
					eprintln!(
						"Warning: Skipping file contents (too large: {}): {}",
						file_mentions::format_bytes(reported_size),
						abs_path.display()
					);
					text.push_str(&format!(
						"<file name=\"{}\">(skipped: too large, {})</file>\n",
						file_mentions::escape_xml_attr(&abs_path.display().to_string()),
						file_mentions::format_bytes(reported_size)
					));
					continue;
				},
				Err(file_mentions::BoundedReadError::Io(err)) => {
					return Err(CliError::fail(format!(
						"Error: Could not read file {}: {err:?}",
						abs_path.display()
					)));
				},
			};

			let b64 = file_mentions::base64_encode(&bytes);
			text.push_str(&format!(
				"<file name=\"{}\"></file>\n",
				file_mentions::escape_xml_attr(&abs_path.display().to_string())
			));
			images.push(ContentPart::image(b64, mime));
		} else {
			let max_bytes = file_mentions::MAX_AUTO_READ_TEXT_BYTES;
			let bytes = match file_mentions::read_bounded(
				&mut file,
				initial_buf,
				max_bytes,
			) {
				Ok(b) => b,
				Err(file_mentions::BoundedReadError::TooLarge { actual_read }) => {
					let disk_size = file.metadata().map(|m| m.len()).unwrap_or(0);
					let reported_size = if disk_size > max_bytes {
						disk_size
					} else {
						actual_read
					};
					eprintln!(
						"Warning: Skipping file contents (too large: {}): {}",
						file_mentions::format_bytes(reported_size),
						abs_path.display()
					);
					text.push_str(&format!(
						"<file name=\"{}\">(skipped: too large, {})</file>\n",
						file_mentions::escape_xml_attr(&abs_path.display().to_string()),
						file_mentions::format_bytes(reported_size)
					));
					continue;
				},
				Err(file_mentions::BoundedReadError::Io(err)) => {
					return Err(CliError::fail(format!(
						"Error: Could not read file {}: {err:?}",
						abs_path.display()
					)));
				},
			};

			let content = String::from_utf8_lossy(&bytes);
			text.push_str(&format!(
				"<file name=\"{}\">\n{content}\n</file>\n",
				file_mentions::escape_xml_attr(&abs_path.display().to_string())
			));
		}
	}
	Ok(ProcessedCliFiles { text, images })
}

/// Headless `omp search`/`omp q`: invoke `WebSearchTool` with host searxng config.
fn run_search_command(
	query: &str,
	recency: Option<&str>,
	limit: Option<u32>,
	config: &Config,
	cwd: &Path,
	out: &mut dyn Write,
) -> std::result::Result<(), CliError> {
	let query = query.trim();
	if query.is_empty() {
		return Err(CliError::fail("Error: Query is required"));
	}
	use pi_agent::Tool as _;
	use serde_json::json;
	let env: std::collections::HashMap<String, String> = std::env::vars().collect();
	let tool = pi_agent::tools::WebSearchTool::new(config.searxng.clone().with_env_overrides(&env));
	let mut args = json!({ "query": query });
	if let Some(recency) = recency {
		args["recency"] = json!(recency);
	}
	if let Some(limit) = limit {
		args["limit"] = json!(limit);
	}
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|err| CliError::fail(format!("Failed to start runtime: {err}")))?;
	let ctx = pi_agent::AgentContext::new(cwd.to_path_buf());
	let result = runtime
		.block_on(tool.execute(&args, &ctx))
		.map_err(|err| CliError::fail(format!("Error: {err}")))?;
	if result.is_error {
		return Err(CliError::fail(format!("Error: {}", result.content)));
	}
	out.write_all(result.content.as_bytes())
		.map_err(|err| CliError::fail(err.to_string()))?;
	if !result.content.is_empty() && !result.content.ends_with('\n') {
		out.write_all(b"\n")
			.map_err(|err| CliError::fail(err.to_string()))?;
	}
	out.flush().map_err(|err| CliError::fail(err.to_string()))?;
	Ok(())
}

/// Headless `omp export`: open session via `SessionStore`, render HTML, write file.
fn run_export_command(
	session: Option<&str>,
	output: Option<&Path>,
	cwd: &Path,
	session_dir: Option<&Path>,
) -> std::result::Result<String, CliError> {
	let Some(session) = session.map(str::trim).filter(|s| !s.is_empty()) else {
		return Err(CliError::usage(
			"Usage: omp export <session.jsonl|id> [--output <path>]\n\
			 Exports a session log to standalone HTML without starting a live session.",
		));
	};
	let session_path = if session.contains('/') || session.contains('\\') || session.ends_with(".jsonl")
	{
		let path = dirs::absolute(cwd, Path::new(session));
		if !path.is_file() {
			return Err(CliError::fail(format!("File not found: {session}")));
		}
		path
	} else {
		match session::resolve_resumable_session(session, cwd, session_dir, true) {
			Ok(Some(path)) => path,
			_ => {
				return Err(CliError::fail(format!(
					"Session \"{session}\" not found.\nPass a session JSONL path or id."
				)));
			},
		}
	};
	let dir = session_path.parent().map(PathBuf::from);
	let id = session_path
		.file_stem()
		.and_then(|s| s.to_str())
		.map(str::to_string);
	let store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: cwd.to_path_buf(),
		model: None,
		provider: None,
		dir,
		id,
		continue_latest: false,
		parent_session: None,
		no_title: true,
	});
	if !store.enabled() {
		let reason = store.corrupt_reason().unwrap_or("persistence disabled");
		return Err(CliError::fail(format!(
			"cannot open session log {}: {reason}",
			session_path.display()
		)));
	}
	let out_path = match output {
		Some(path) => {
			if path.is_absolute() {
				path.to_path_buf()
			} else {
				dirs::absolute(cwd, path)
			}
		},
		None => session_path.with_extension("html"),
	};
	let html = render_session_html(&store);
	if let Some(parent) = out_path.parent() {
		if !parent.as_os_str().is_empty() {
			std::fs::create_dir_all(parent).map_err(|err| {
				CliError::fail(format!("cannot create {}: {err}", parent.display()))
			})?;
		}
	}
	std::fs::write(&out_path, html).map_err(|err| {
		CliError::fail(format!("cannot write {}: {err}", out_path.display()))
	})?;
	Ok(out_path.display().to_string())
}

fn html_escape_text(text: &str) -> String {
	text.replace('&', "&amp;")
		.replace('<', "&lt;")
		.replace('>', "&gt;")
		.replace('"', "&quot;")
}

/// Minimal HTML transcript mirroring `rpc/session.rs` `render_html`, over `SessionStore`.
fn render_session_html(store: &SessionStore) -> String {
	let entries: Vec<&MessageEntry> = if store.has_branch_context() {
		store.branch_messages()
	} else {
		store.messages().iter().collect()
	};
	let mut body = String::new();
	for entry in entries {
		let role = entry.message.role.as_str();
		let text = entry.message.content.text();
		body.push_str(&format!(
			"<article class=\"{role}\"><h2>{role}</h2><pre>{}</pre></article>\n",
			html_escape_text(&text)
		));
	}
	let title = store
		.header()
		.and_then(|header| header.title.as_deref())
		.unwrap_or("OMP session");
	let title = html_escape_text(title);
	format!(
		"<!doctype html>\n<html><head><meta charset=\"utf-8\"><title>{title}</title>\
		 <style>body{{font-family:system-ui,sans-serif;max-width:52rem;margin:2rem auto;padding:0 1rem;}}\
		 article{{border-top:1px solid #ddd;padding:1rem 0}}pre{{white-space:pre-wrap}}\
		 .user h2{{color:#0b6}}.assistant h2{{color:#06c}}</style></head><body>\
		 <h1>{title}</h1>\n{body}</body></html>\n"
	)
}

/// Resolve a session path/id (or most recent) into an open `SessionStore`.
fn open_headless_session(
	session: Option<&str>,
	cwd: &Path,
	session_dir: Option<&Path>,
	required: bool,
) -> std::result::Result<(PathBuf, SessionStore), CliError> {
	let session_path = match session.map(str::trim).filter(|s| !s.is_empty()) {
		Some(session) => {
			if session.contains('/') || session.contains('\\') || session.ends_with(".jsonl") {
				let path = dirs::absolute(cwd, Path::new(session));
				if !path.is_file() {
					return Err(CliError::fail(format!("File not found: {session}")));
				}
				path
			} else {
				match session::resolve_resumable_session(session, cwd, session_dir, true) {
					Ok(Some(path)) => path,
					_ => {
						return Err(CliError::fail(format!(
							"Session \"{session}\" not found.\nPass a session JSONL path or id."
						)));
					},
				}
			}
		},
		None => {
			if required {
				return Err(CliError::usage(
					"session argument is required (path or id)",
				));
			}
			match session::continue_recent(cwd, session_dir) {
				Ok(Some(path)) => path,
				Ok(None) => {
					return Err(CliError::fail(
						"No sessions found.\nPass a session JSONL path or id.",
					));
				},
				Err(err) => return Err(CliError::fail(err)),
			}
		},
	};
	let dir = session_path.parent().map(PathBuf::from);
	let id = session_path
		.file_stem()
		.and_then(|s| s.to_str())
		.map(str::to_string);
	let store = SessionStore::open(&SessionOptions {
		enabled: true,
		cwd: cwd.to_path_buf(),
		model: None,
		provider: None,
		dir,
		id,
		continue_latest: false,
		parent_session: None,
		no_title: true,
	});
	if !store.enabled() {
		let reason = store.corrupt_reason().unwrap_or("persistence disabled");
		return Err(CliError::fail(format!(
			"cannot open session log {}: {reason}",
			session_path.display()
		)));
	}
	Ok((session_path, store))
}

/// Headless `omp stats`: print token / tool / duration table for one session.
fn run_stats_command(
	session: Option<&str>,
	cwd: &Path,
	session_dir: Option<&Path>,
	out: &mut impl Write,
) -> std::result::Result<(), CliError> {
	let (session_path, store) = open_headless_session(session, cwd, session_dir, false)?;
	let messages = store.messages();
	let usage = store.session_usage();
	let mut reasoning = 0u64;
	let mut cost = 0.0f64;
	let mut tool_calls = 0usize;
	let mut user_messages = 0usize;
	let mut assistant_messages = 0usize;
	let mut tool_results = 0usize;
	let mut first_ts: Option<&str> = None;
	let mut last_ts: Option<&str> = None;
	for entry in messages {
		if first_ts.is_none() && !entry.timestamp.is_empty() {
			first_ts = Some(entry.timestamp.as_str());
		}
		if !entry.timestamp.is_empty() {
			last_ts = Some(entry.timestamp.as_str());
		}
		match entry.message.role.as_str() {
			"user" => user_messages += 1,
			"assistant" => {
				assistant_messages += 1;
				if let Some(u) = &entry.message.usage {
					reasoning = reasoning.saturating_add(u.reasoning_tokens.unwrap_or(0));
					if let Some(c) = &u.cost {
						cost += c.total;
					}
				}
				for part in entry.message.content.parts() {
					if part.kind == "toolCall" {
						tool_calls += 1;
					}
				}
			},
			"toolResult" => tool_results += 1,
			_ => {},
		}
	}
	let duration = match (first_ts, last_ts) {
		(Some(a), Some(b)) => format_iso_span(a, b),
		_ => "unknown".to_string(),
	};
	let session_id = store.id();
	writeln!(out, "Session:  {session_id}").map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "File:     {}", session_path.display())
		.map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "Duration: {duration}").map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out).map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "Messages").map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "  user         {user_messages}")
		.map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "  assistant    {assistant_messages}")
		.map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "  tool calls   {tool_calls}")
		.map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "  tool results {tool_results}")
		.map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "  total        {}", messages.len())
		.map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out).map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "Tokens").map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "  input        {}", usage.input)
		.map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "  output       {}", usage.output)
		.map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "  reasoning    {reasoning}")
		.map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "  cache read   {}", usage.cache_read)
		.map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "  cache write  {}", usage.cache_write)
		.map_err(|err| CliError::fail(err.to_string()))?;
	writeln!(out, "  total        {}", usage.total_tokens)
		.map_err(|err| CliError::fail(err.to_string()))?;
	if cost > 0.0 {
		writeln!(out, "  cost         ${cost:.4}")
			.map_err(|err| CliError::fail(err.to_string()))?;
	}
	out.flush().map_err(|err| CliError::fail(err.to_string()))?;
	Ok(())
}

fn format_iso_span(start: &str, end: &str) -> String {
	fn rough_secs(iso: &str) -> Option<i64> {
		let (date, time) = iso.split_once('T')?;
		let mut d = date.split('-');
		let y: i64 = d.next()?.parse().ok()?;
		let mo: i64 = d.next()?.parse().ok()?;
		let da: i64 = d.next()?.parse().ok()?;
		let time = time.trim_end_matches('Z');
		let (hms, frac) = time
			.split_once('.')
			.map_or((time, 0i64), |(h, f)| (h, f.chars().take(3).collect::<String>().parse().unwrap_or(0)));
		let mut t = hms.split(':');
		let h: i64 = t.next()?.parse().ok()?;
		let mi: i64 = t.next()?.parse().ok()?;
		let s: i64 = t.next()?.parse().ok()?;
		// Approximate days since epoch (enough for duration deltas).
		let days = y * 365 + (y / 4) + mo * 30 + da;
		Some(days * 86_400 + h * 3600 + mi * 60 + s + frac / 1000)
	}
	match (rough_secs(start), rough_secs(end)) {
		(Some(a), Some(b)) if b >= a => {
			let secs = (b - a) as u64;
			let h = secs / 3600;
			let m = (secs % 3600) / 60;
			let s = secs % 60;
			if h > 0 {
				format!("{h}h {m}m {s}s")
			} else if m > 0 {
				format!("{m}m {s}s")
			} else {
				format!("{s}s")
			}
		},
		_ => "unknown".to_string(),
	}
}

/// Headless `omp render`: plain-text transcript (stdout or `--output`).
fn run_render_command(
	session: Option<&str>,
	output: Option<&Path>,
	cwd: &Path,
	session_dir: Option<&Path>,
) -> std::result::Result<(), CliError> {
	let Some(session) = session.map(str::trim).filter(|s| !s.is_empty()) else {
		return Err(CliError::usage(
			"Usage: omp render <session.jsonl|id> [--output <path>]\n\
			 Renders a session log to plain text without starting a live session.",
		));
	};
	let (_path, store) = open_headless_session(Some(session), cwd, session_dir, true)?;
	let text = render_session_text(&store);
	match output {
		Some(path) => {
			let out_path = if path.is_absolute() {
				path.to_path_buf()
			} else {
				dirs::absolute(cwd, path)
			};
			if let Some(parent) = out_path.parent() {
				if !parent.as_os_str().is_empty() {
					std::fs::create_dir_all(parent).map_err(|err| {
						CliError::fail(format!("cannot create {}: {err}", parent.display()))
					})?;
				}
			}
			std::fs::write(&out_path, text).map_err(|err| {
				CliError::fail(format!("cannot write {}: {err}", out_path.display()))
			})?;
			println!("Rendered to: {}", out_path.display());
		},
		None => {
			print!("{text}");
			if !text.ends_with('\n') {
				println!();
			}
		},
	}
	Ok(())
}

fn render_session_text(store: &SessionStore) -> String {
	let entries: Vec<&MessageEntry> = if store.has_branch_context() {
		store.branch_messages()
	} else {
		store.messages().iter().collect()
	};
	let title = store
		.header()
		.and_then(|header| header.title.as_deref())
		.unwrap_or("OMP session");
	let mut out = String::new();
	out.push_str("# ");
	out.push_str(title);
	out.push_str("\n\n");
	for entry in entries {
		let role = entry.message.role.as_str();
		let text = entry.message.content.text();
		out.push_str("## ");
		out.push_str(role);
		out.push('\n');
		if !text.is_empty() {
			out.push_str(&text);
			if !text.ends_with('\n') {
				out.push('\n');
			}
		}
		out.push('\n');
	}
	out
}

/// Headless `omp cleanse`: redact secrets in the JSONL and rewrite in place.
fn run_cleanse_command(
	session: Option<&str>,
	confirm: bool,
	cwd: &Path,
	session_dir: Option<&Path>,
) -> std::result::Result<(), CliError> {
	if !confirm {
		return Err(CliError::usage(
			"Usage: omp cleanse <session.jsonl|id> --confirm\n\
			 Refusing to rewrite without --confirm.",
		));
	}
	let Some(session) = session.map(str::trim).filter(|s| !s.is_empty()) else {
		return Err(CliError::usage(
			"Usage: omp cleanse <session.jsonl|id> --confirm\n\
			 Removes API keys and tokens from a session log.",
		));
	};
	let (session_path, _store) = open_headless_session(Some(session), cwd, session_dir, true)?;
	let raw = std::fs::read_to_string(&session_path).map_err(|err| {
		CliError::fail(format!("cannot read {}: {err}", session_path.display()))
	})?;
	let mut changed = 0usize;
	let mut out = String::with_capacity(raw.len());
	for line in raw.lines() {
		if line.is_empty() {
			out.push('\n');
			continue;
		}
		let redacted = omp_cli::auth::error::redact(line);
		if redacted != line {
			changed += 1;
		}
		out.push_str(&redacted);
		out.push('\n');
	}
	std::fs::write(&session_path, out).map_err(|err| {
		CliError::fail(format!("cannot write {}: {err}", session_path.display()))
	})?;
	println!(
		"Cleansed {} ({} line{} rewritten)",
		session_path.display(),
		changed,
		if changed == 1 { "" } else { "s" }
	);
	Ok(())
}

/// Headless `omp commit`: `git add -A` then `git commit` in the cwd.
fn run_commit_command(
	message: Option<&str>,
	cwd: &Path,
) -> std::result::Result<(), CliError> {
	let msg = message
		.map(str::trim)
		.filter(|s| !s.is_empty())
		.unwrap_or("omp commit");
	let status = std::process::Command::new("git")
		.args(["rev-parse", "--is-inside-work-tree"])
		.current_dir(cwd)
		.stdout(std::process::Stdio::null())
		.stderr(std::process::Stdio::null())
		.status()
		.map_err(|err| CliError::fail(format!("git not available: {err}")))?;
	if !status.success() {
		return Err(CliError::fail(format!(
			"not a git repository: {}",
			cwd.display()
		)));
	}
	let unmerged = std::process::Command::new("git")
		.args(["ls-files", "-u"])
		.current_dir(cwd)
		.output()
		.map_err(|err| CliError::fail(format!("git ls-files failed: {err}")))?;
	if !unmerged.status.success() {
		// `output()` succeeds for a non-zero exit; empty stdout would otherwise
		// read as "no conflicts".
		return Err(CliError::fail(format!(
			"git ls-files -u exited {}: {}",
			unmerged.status.code().unwrap_or(-1),
			String::from_utf8_lossy(&unmerged.stderr).trim()
		)));
	}
	if !unmerged.stdout.is_empty() {
		return Err(CliError::fail(
			"cannot commit: unresolved merge conflicts exist. Resolve conflicts before committing."
				.to_string(),
		));
	}
	let diff = std::process::Command::new("git")
		.args(["status", "--porcelain"])
		.current_dir(cwd)
		.output()
		.map_err(|err| CliError::fail(format!("git status failed: {err}")))?;
	if !diff.status.success() {
		return Err(CliError::fail(format!(
			"git status --porcelain exited {}: {}",
			diff.status.code().unwrap_or(-1),
			String::from_utf8_lossy(&diff.stderr).trim()
		)));
	}
	if diff.stdout.is_empty() {
		println!("Nothing to commit, working tree clean.");
		return Ok(());
	}
	let add = std::process::Command::new("git")
		.args(["add", "-A"])
		.current_dir(cwd)
		.status()
		.map_err(|err| CliError::fail(format!("git add failed: {err}")))?;
	if !add.success() {
		return Err(CliError::fail(format!(
			"git add -A exited {}",
			add.code().unwrap_or(-1)
		)));
	}
	let commit = std::process::Command::new("git")
		.args(["commit", "-m", msg])
		.current_dir(cwd)
		.status()
		.map_err(|err| CliError::fail(format!("git commit failed: {err}")))?;
	if !commit.success() {
		return Err(CliError::fail(format!(
			"git commit exited {}",
			commit.code().unwrap_or(-1)
		)));
	}
	println!("Committed: {msg}");
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn model_switch_clears_inherited_cache_key_but_keeps_explicit() {
		let mut key = Some("header-cache".to_string());
		let mut source = Some(PromptCacheKeySource::Inherited);
		invalidate_inherited_cache_key(&mut key, &mut source);
		assert_eq!(key, None);
		assert_eq!(source, None);

		let mut key = Some("explicit-cache".to_string());
		let mut source = Some(PromptCacheKeySource::Explicit);
		invalidate_inherited_cache_key(&mut key, &mut source);
		assert_eq!(key.as_deref(), Some("explicit-cache"));
		assert_eq!(source, Some(PromptCacheKeySource::Explicit));
	}

	/// Fresh repo in a temp dir, or `None` when `git` is unavailable / cannot init.
	fn temp_git_dir(name: &str) -> Option<std::path::PathBuf> {
		let dir = std::env::temp_dir().join(format!("omp-cli-{name}-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		std::fs::create_dir_all(&dir).ok()?;
		let init = std::process::Command::new("git")
			.args(["init", "-q", "."])
			.current_dir(&dir)
			.status()
			.ok()?;
		if init.success() {
			Some(dir)
		} else {
			let _ = std::fs::remove_dir_all(&dir);
			None
		}
	}

	/// `output()` succeeds for a non-zero git exit, so a broken index used to be
	/// reported as "Nothing to commit, working tree clean." with exit 0.
	#[test]
	fn commit_command_surfaces_a_failing_git_probe() {
		let Some(dir) = temp_git_dir("commit-corrupt-index") else { return };
		std::fs::write(dir.join("tracked.txt"), "x").expect("write tracked");
		let added = std::process::Command::new("git")
			.args(["add", "-A"])
			.current_dir(&dir)
			.status()
			.expect("git add");
		assert!(added.success(), "git add must succeed in the fixture repo");
		std::fs::write(dir.join(".git/index"), b"garbage").expect("corrupt index");

		let error = run_commit_command(Some("unused"), &dir).expect_err("a broken index must fail");
		assert!(
			error.message.starts_with("git ls-files -u exited"),
			"unexpected message: {}",
			error.message
		);
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn commit_command_reports_a_clean_tree_without_committing() {
		let Some(dir) = temp_git_dir("commit-clean-tree") else { return };
		if let Err(error) = run_commit_command(Some("unused"), &dir) {
			panic!("a clean tree is not an error: {}", error.message);
		}
		let head = std::process::Command::new("git")
			.args(["rev-parse", "--verify", "-q", "HEAD"])
			.current_dir(&dir)
			.output()
			.expect("git rev-parse");
		assert!(!head.status.success(), "nothing may be committed on a clean tree");
		let _ = std::fs::remove_dir_all(&dir);
	}
}
