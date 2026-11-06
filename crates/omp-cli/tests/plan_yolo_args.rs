//! Plan-yolo CLI surface: the flags are parsed as options, never as prompt
//! text, and `--plan-yolo-into` is retained for the startup hard-error check.

use std::{ffi::OsString, process::Command};

use clap::Parser;
use omp_cli::args::{Cli, parse_cli};

fn parse(args: &[&str]) -> Cli {
	parse_cli(args.iter().map(OsString::from).collect())
}

#[test]
fn plan_yolo_flags_parse_before_the_prompt() {
	let cli = parse(&[
		"omp",
		"--plan-yolo",
		"--plan-yolo-into",
		"@slow",
		"implement the plan",
	]);
	assert!(cli.plan_yolo);
	assert_eq!(cli.plan_yolo_into.as_deref(), Some("@slow"));
	assert_eq!(cli.prompt, vec!["implement the plan".to_string()]);
}

#[test]
fn plan_yolo_defaults_to_no_target_override() {
	let cli = parse(&["omp", "--plan-yolo", "hello"]);
	assert!(cli.plan_yolo);
	assert_eq!(cli.plan_yolo_into, None);
	assert_eq!(cli.prompt, vec!["hello".to_string()]);
}

#[test]
fn explicit_prewalk_and_plan_yolo_conflict_before_startup() {
	let error = Cli::try_parse_from([
		"omp",
		"--prewalk",
		"--plan-yolo",
		"prompt",
		"must not run",
	])
	.expect_err("both one-way startup switches must be rejected");
	assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
}

#[test]
fn settings_prewalk_and_plan_yolo_conflict_before_contact() {
	let root = std::env::temp_dir().join(format!(
		"omp-plan-yolo-settings-conflict-{}",
		std::process::id()
	));
	let agent = root.join("agent");
	std::fs::create_dir_all(&agent).expect("agent dir");
	std::fs::create_dir_all(root.join("config")).expect("config dir");
	std::fs::create_dir_all(root.join("share")).expect("share dir");
	std::fs::write(
		agent.join("config.yml"),
		"defaultModel: mock/mock\nmodelRoles:\n  smol: mock/mock\nprewalk:\n  enabled: true\nproviders:\n  mock:\n    baseUrl: \"http://127.0.0.1:9/v1\"\n    api: openai-completions\n    apiKey: test-key\n",
	)
	.expect("config");
	std::fs::write(
		agent.join("models.yml"),
		"providers:\n  mock:\n    baseUrl: \"http://127.0.0.1:9/v1\"\n    api: openai-completions\n    apiKey: test-key\n    models:\n      - id: mock\n        contextWindow: 128000\n",
	)
	.expect("models");
	let output = Command::new(env!("CARGO_BIN_EXE_omp"))
		.args(["--yolo", "--plan-yolo", "must not run"])
		.env("PI_CONFIG_DIR", root.join("config"))
		.env("PI_CODING_AGENT_DIR", &agent)
		.env("OMP_SHARE_DIR", root.join("share"))
		.env("PI_NO_TITLE", "1")
		.output()
		.expect("run binary");
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert_ne!(output.status.code(), Some(0), "settings conflict must fail");
	assert!(
		stderr.contains("--plan-yolo cannot be combined with an effective prewalk arm"),
		"stderr={stderr}"
	);
	assert!(output.stdout.is_empty(), "nothing may start before the conflict");
	std::fs::remove_dir_all(root).ok();
}

#[test]
fn explicit_prewalk_and_plan_yolo_exit_before_contact() {
	let output = Command::new(env!("CARGO_BIN_EXE_omp"))
		.args([
			"--prewalk-into",
			"@smol",
			"--plan-yolo-into",
			"@slow",
			"must not run",
		])
		.output()
		.expect("run binary");
	assert_eq!(output.status.code(), Some(2), "argument conflict is a usage exit");
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(
		stderr.contains("cannot be used with") || stderr.contains("conflict"),
		"stderr={stderr}"
	);
	assert!(output.stdout.is_empty(), "nothing may start before the conflict");
}
