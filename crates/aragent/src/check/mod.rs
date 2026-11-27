//! Project checker discovery and output normalization ported from `cleanse/`.
//!
//! [`parsers`] turns raw checker output into deduplicated, project-relative
//! diagnostics. [`runners`] discovers configured checkers from a project
//! snapshot and executes them without installing anything.

use serde::{Deserialize, Serialize};

pub mod parsers;
pub mod runners;

pub use parsers::{
	CLEANSE_PARSER_KINDS, CleanseDiagnostic, CleanseParserInput, CleanseParserKind, CleanseSeverity,
	diagnostic_key, parse_cleanse_diagnostics,
};
pub use runners::{
	CleanseDiagnosticSuite, build_custom_cleanse_suite, discover_cleanse_diagnostic_suite,
};

/// Result metadata for one checker invocation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanseCheckResult {
	pub id:          String,
	pub label:       String,
	pub language:    String,
	pub cwd:         String,
	pub command:     String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub exit_code:   Option<i32>,
	pub diagnostics: Vec<CleanseDiagnostic>,
}

/// Checker discovery result omitted because its required executable was
/// unavailable.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkippedCleanseCheck {
	pub label:    String,
	pub language: String,
	pub reason:   String,
}

/// Aggregate diagnostics from every checker selected for a project.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanseDiagnosticReport {
	pub checks:      Vec<CleanseCheckResult>,
	pub diagnostics: Vec<CleanseDiagnostic>,
	pub skipped:     Vec<SkippedCleanseCheck>,
}

/// Identity and display metadata for one runnable checker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanseCheckerDescriptor {
	pub id:       String,
	pub label:    String,
	pub language: String,
	/// Project-relative working directory the checker command runs in.
	pub cwd:      String,
	pub command:  String,
}

/// Optional checker families enabled for a cleanse run.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanseDiagnosticSuiteOptions {
	pub include_tests: bool,
}

/// One checker proposed by the prompted discovery agent.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomCleanseCheckerSpec {
	pub label:    String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub language: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub cwd:      Option<String>,
	pub command:  Vec<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub parser:   Option<String>,
}
