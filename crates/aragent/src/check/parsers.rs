//! Machine- and fallback-format parsers for checker output.
//!
//! Port of `cleanse/parsers.ts`. Every parser returns diagnostics whose file
//! paths are project-relative, whose messages are sanitized (ANSI and control
//! characters removed, whitespace collapsed), and whose duplicates are removed
//! by `(file, line, column, code, message)`.

use std::{
	collections::HashSet,
	path::{Component, Path, PathBuf},
	sync::LazyLock,
};

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Normalized severity shared across checker-specific output formats.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CleanseSeverity {
	Error,
	Warning,
	Info,
}

/// One actionable problem reported by a project checker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanseDiagnostic {
	pub checker:      String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub file:         Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub line:         Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub column:       Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub end_line:     Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub end_column:   Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub code:         Option<String>,
	pub severity:     CleanseSeverity,
	pub message:      String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub suggestion:   Option<String>,
}

/// Machine and fallback output formats understood by the checker pipeline.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CleanseParserKind {
	Rust,
	RustTest,
	Go,
	GoTest,
	Staticcheck,
	Golangci,
	Ruff,
	Pyright,
	Mypy,
	Pylint,
	Flake8,
	Ty,
	Eslint,
	Biome,
	Oxlint,
	DenoLint,
	Stylelint,
	Rubocop,
	Phpstan,
	Psalm,
	Swiftlint,
	Dart,
	Credo,
	Shellcheck,
	Hlint,
	Terraform,
	Tflint,
	Actionlint,
	Generic,
}

/// Every parser kind, in donor order.
pub const CLEANSE_PARSER_KINDS: [CleanseParserKind; 29] = [
	CleanseParserKind::Rust,
	CleanseParserKind::RustTest,
	CleanseParserKind::Go,
	CleanseParserKind::GoTest,
	CleanseParserKind::Staticcheck,
	CleanseParserKind::Golangci,
	CleanseParserKind::Ruff,
	CleanseParserKind::Pyright,
	CleanseParserKind::Mypy,
	CleanseParserKind::Pylint,
	CleanseParserKind::Flake8,
	CleanseParserKind::Ty,
	CleanseParserKind::Eslint,
	CleanseParserKind::Biome,
	CleanseParserKind::Oxlint,
	CleanseParserKind::DenoLint,
	CleanseParserKind::Stylelint,
	CleanseParserKind::Rubocop,
	CleanseParserKind::Phpstan,
	CleanseParserKind::Psalm,
	CleanseParserKind::Swiftlint,
	CleanseParserKind::Dart,
	CleanseParserKind::Credo,
	CleanseParserKind::Shellcheck,
	CleanseParserKind::Hlint,
	CleanseParserKind::Terraform,
	CleanseParserKind::Tflint,
	CleanseParserKind::Actionlint,
	CleanseParserKind::Generic,
];

impl CleanseParserKind {
	/// Stable kebab-case name used by checker specs and configuration.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Rust => "rust",
			Self::RustTest => "rust-test",
			Self::Go => "go",
			Self::GoTest => "go-test",
			Self::Staticcheck => "staticcheck",
			Self::Golangci => "golangci",
			Self::Ruff => "ruff",
			Self::Pyright => "pyright",
			Self::Mypy => "mypy",
			Self::Pylint => "pylint",
			Self::Flake8 => "flake8",
			Self::Ty => "ty",
			Self::Eslint => "eslint",
			Self::Biome => "biome",
			Self::Oxlint => "oxlint",
			Self::DenoLint => "deno-lint",
			Self::Stylelint => "stylelint",
			Self::Rubocop => "rubocop",
			Self::Phpstan => "phpstan",
			Self::Psalm => "psalm",
			Self::Swiftlint => "swiftlint",
			Self::Dart => "dart",
			Self::Credo => "credo",
			Self::Shellcheck => "shellcheck",
			Self::Hlint => "hlint",
			Self::Terraform => "terraform",
			Self::Tflint => "tflint",
			Self::Actionlint => "actionlint",
			Self::Generic => "generic",
		}
	}

	/// Look up a parser kind by its [`Self::as_str`] name.
	pub fn from_name(name: &str) -> Option<Self> {
		CLEANSE_PARSER_KINDS.into_iter().find(|kind| kind.as_str() == name)
	}
}

/// Captured checker process output passed to a format parser.
#[derive(Clone, Copy, Debug)]
pub struct CleanseParserInput<'a> {
	pub checker:     &'a str,
	pub project_cwd: &'a Path,
	pub checker_cwd: &'a Path,
	pub stdout:      &'a str,
	pub stderr:      &'a str,
}

type JsonMap = Map<String, Value>;

#[derive(Default)]
struct DiagnosticFields {
	file:       Option<String>,
	line:       Option<f64>,
	column:     Option<f64>,
	end_line:   Option<f64>,
	end_column: Option<f64>,
	code:       Option<String>,
	severity:   Option<SeverityField>,
	message:    Option<String>,
	suggestion: Option<String>,
}

enum SeverityField {
	Text(String),
	Number(f64),
}

struct ParsedLocation {
	file:   String,
	line:   f64,
	column: Option<f64>,
}

/// Parse one checker invocation into normalized, project-relative diagnostics.
pub fn parse_cleanse_diagnostics(
	kind: CleanseParserKind,
	input: &CleanseParserInput<'_>,
) -> Vec<CleanseDiagnostic> {
	let diagnostics = match kind {
		CleanseParserKind::Rust => parse_rust(input),
		CleanseParserKind::RustTest => parse_rust_test(input),
		CleanseParserKind::Go => parse_go(input),
		CleanseParserKind::GoTest => parse_go_test(input),
		CleanseParserKind::Staticcheck => parse_staticcheck(input),
		CleanseParserKind::Golangci => parse_golangci(input),
		CleanseParserKind::Ruff => parse_ruff(input),
		CleanseParserKind::Pyright => parse_pyright(input),
		CleanseParserKind::Mypy => parse_generic(input),
		CleanseParserKind::Pylint => parse_pylint(input),
		CleanseParserKind::Flake8 => parse_flake8(input),
		CleanseParserKind::Ty => parse_ty(input),
		CleanseParserKind::Eslint => parse_eslint(input),
		CleanseParserKind::Biome => parse_biome(input),
		CleanseParserKind::Oxlint => parse_unix_format(input),
		CleanseParserKind::DenoLint => parse_deno_lint(input),
		CleanseParserKind::Stylelint => parse_stylelint(input),
		CleanseParserKind::Rubocop => parse_rubocop(input),
		CleanseParserKind::Phpstan => parse_phpstan(input),
		CleanseParserKind::Psalm => parse_psalm(input),
		CleanseParserKind::Swiftlint => parse_swiftlint(input),
		CleanseParserKind::Dart => parse_dart(input),
		CleanseParserKind::Credo => parse_credo(input),
		CleanseParserKind::Shellcheck => parse_shellcheck(input),
		CleanseParserKind::Hlint => parse_hlint(input),
		CleanseParserKind::Terraform => parse_terraform(input),
		CleanseParserKind::Tflint => parse_tflint(input),
		CleanseParserKind::Actionlint => parse_actionlint(input),
		CleanseParserKind::Generic => parse_generic(input),
	};
	deduplicate_diagnostics(diagnostics)
}

fn to_record(value: &Value) -> Option<&JsonMap> {
	value.as_object()
}

fn to_array(value: &Value) -> &[Value] {
	value.as_array().map_or(&[], Vec::as_slice)
}

fn array_field<'a>(record: Option<&'a JsonMap>, key: &str) -> &'a [Value] {
	record.and_then(|map| map.get(key)).and_then(Value::as_array).map_or(&[], Vec::as_slice)
}

fn string_field(record: Option<&JsonMap>, key: &str) -> Option<String> {
	let value = record?.get(key)?.as_str()?;
	if value.trim().is_empty() { None } else { Some(value.to_owned()) }
}

fn number_field(record: Option<&JsonMap>, key: &str) -> Option<f64> {
	let value = record?.get(key)?.as_f64()?;
	if value.is_finite() { Some(value) } else { None }
}

fn nested_record<'a>(record: Option<&'a JsonMap>, key: &str) -> Option<&'a JsonMap> {
	to_record(record?.get(key)?)
}

fn normalize_severity(value: Option<SeverityField>) -> CleanseSeverity {
	match value {
		Some(SeverityField::Number(number)) => {
			if number >= 2.0 {
				CleanseSeverity::Error
			} else if number == 1.0 {
				CleanseSeverity::Warning
			} else {
				CleanseSeverity::Info
			}
		},
		Some(SeverityField::Text(text)) => {
			let normalized = text.to_lowercase();
			if normalized.contains("error") || normalized == "fatal" {
				CleanseSeverity::Error
			} else if normalized.contains("warn")
				|| normalized == "convention"
				|| normalized == "refactor"
			{
				CleanseSeverity::Warning
			} else {
				CleanseSeverity::Info
			}
		},
		None => CleanseSeverity::Warning,
	}
}

/// Resolve one checker-reported path against the project root.
///
/// Absolute paths that escape the project, `<internal>` pseudo-paths, and
/// malformed `file://` URLs yield `None`, matching the donor parser.
fn normalize_file(raw_file: &str, input: &CleanseParserInput<'_>) -> Option<String> {
	let file = raw_file.trim();
	let file = file.strip_prefix('\'').or_else(|| file.strip_prefix('"')).unwrap_or(file);
	let mut file = file.strip_suffix('\'').or_else(|| file.strip_suffix('"')).unwrap_or(file).to_owned();
	if file.is_empty() || file.starts_with('<') {
		return None;
	}
	let file_uri_path = file
		.strip_prefix("file://")
		.map(|rest| rest.find('/').map_or("", |index| &rest[index..]).to_owned());
	if let Some(pathname) = file_uri_path {
		file = percent_decode(&pathname)?;
	}
	if file.contains('\0') {
		return None;
	}
	let path = Path::new(&file);
	let absolute = if path.is_absolute() {
		normalize_lexical(path)
	} else {
		normalize_lexical(&input.checker_cwd.join(path))
	};
	let project_cwd = normalize_lexical(input.project_cwd);
	let relative = path_relative(&absolute, &project_cwd)?;
	if relative.as_os_str().is_empty() {
		return None;
	}
	if matches!(
		relative.components().next(),
		Some(Component::ParentDir | Component::RootDir | Component::Prefix(_))
	) {
		return None;
	}
	Some(
		relative
			.components()
			.filter_map(|component| match component {
				Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
				Component::ParentDir => Some("..".to_owned()),
				Component::CurDir | Component::RootDir | Component::Prefix(_) => None,
			})
			.collect::<Vec<_>>()
			.join("/"),
	)
}

fn make_diagnostic(
	input: &CleanseParserInput<'_>,
	fields: DiagnosticFields,
) -> Option<CleanseDiagnostic> {
	let message = collapse_whitespace(&sanitize_text(fields.message.as_deref().unwrap_or("")));
	if message.is_empty() {
		return None;
	}
	let file = match fields.file {
		Some(raw_file) if raw_file.is_empty() => None,
		Some(raw_file) => Some(normalize_file(&raw_file, input)?),
		None => None,
	};
	Some(CleanseDiagnostic {
		checker: input.checker.to_owned(),
		file,
		line: positive_integer(fields.line),
		column: positive_integer(fields.column),
		end_line: positive_integer(fields.end_line),
		end_column: positive_integer(fields.end_column),
		code: fields.code.map(|code| code.trim().to_owned()).filter(|code| !code.is_empty()),
		severity: normalize_severity(fields.severity),
		message,
		suggestion: fields
			.suggestion
			.map(|suggestion| suggestion.trim().to_owned())
			.filter(|suggestion| !suggestion.is_empty()),
	})
}

#[allow(
	clippy::cast_possible_truncation,
	clippy::cast_precision_loss,
	clippy::cast_sign_loss,
	reason = "finite, integral, positive and in-range values are checked before the cast"
)]
fn positive_integer(value: Option<f64>) -> Option<u64> {
	let value = value?;
	if value.is_finite() && value.fract() == 0.0 && value > 0.0 && value < u64::MAX as f64 {
		Some(value as u64)
	} else {
		None
	}
}

fn add_diagnostic(
	target: &mut Vec<CleanseDiagnostic>,
	input: &CleanseParserInput<'_>,
	fields: DiagnosticFields,
) {
	if let Some(diagnostic) = make_diagnostic(input, fields) {
		target.push(diagnostic);
	}
}

fn parse_json_values(text: &str) -> Vec<Value> {
	let sanitized = sanitize_text(text);
	let trimmed = sanitized.trim();
	if trimmed.is_empty() {
		return Vec::new();
	}
	if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
		return vec![value];
	}
	let bytes = trimmed.as_bytes();
	let mut values = Vec::new();
	let mut start: Option<usize> = None;
	let mut depth = 0i32;
	let mut in_string = false;
	let mut escaped = false;
	for (index, byte) in bytes.iter().copied().enumerate() {
		if start.is_none() {
			if byte != b'{' && byte != b'[' {
				continue;
			}
			start = Some(index);
			depth = 1;
			in_string = false;
			escaped = false;
			continue;
		}
		if in_string {
			if escaped {
				escaped = false;
			} else if byte == b'\\' {
				escaped = true;
			} else if byte == b'"' {
				in_string = false;
			}
			continue;
		}
		if byte == b'"' {
			in_string = true;
		} else if byte == b'{' || byte == b'[' {
			depth += 1;
		} else if byte == b'}' || byte == b']' {
			depth -= 1;
		}
		if depth != 0 {
			continue;
		}
		let start_index = start.take().unwrap_or(index);
		if let Ok(value) = serde_json::from_str::<Value>(&trimmed[start_index..=index]) {
			values.push(value);
		}
	}
	values
}

fn all_json_values(input: &CleanseParserInput<'_>) -> Vec<Value> {
	let mut values = parse_json_values(input.stdout);
	values.extend(parse_json_values(input.stderr));
	values
}

fn parse_rust(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for value in parse_json_values(input.stdout) {
		let envelope = to_record(&value);
		if string_field(envelope, "reason").as_deref() != Some("compiler-message") {
			continue;
		}
		let message = nested_record(envelope, "message");
		let Some(level) = string_field(message, "level") else {
			continue;
		};
		if level != "error" && level != "warning" {
			continue;
		}
		let spans: Vec<&JsonMap> =
			array_field(message, "spans").iter().filter_map(to_record).collect();
		let primary = spans
			.iter()
			.copied()
			.find(|span| span.get("is_primary").and_then(Value::as_bool) == Some(true))
			.or_else(|| spans.first().copied());
		let code = nested_record(message, "code");
		add_diagnostic(&mut diagnostics, input, DiagnosticFields {
			file:       string_field(primary, "file_name"),
			line:       number_field(primary, "line_start"),
			column:     number_field(primary, "column_start"),
			end_line:   number_field(primary, "line_end"),
			end_column: number_field(primary, "column_end"),
			code:       string_field(code, "code"),
			severity:   Some(SeverityField::Text(level)),
			message:    string_field(message, "message"),
			suggestion: string_field(primary, "suggested_replacement"),
			..Default::default()
		});
	}
	diagnostics
}

fn parse_rust_test(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = parse_rust(input);
	let text = format!("{}\n{}", input.stdout, input.stderr);
	for line in text.split('\n') {
		let Some(captures) = PANIC_RE.captures(line) else {
			continue;
		};
		let (Some(name), Some(file), Some(line_number), Some(column)) =
			(captures.get(1), captures.get(2), captures.get(3), captures.get(4))
		else {
			continue;
		};
		add_diagnostic(&mut diagnostics, input, DiagnosticFields {
			file:     Some(file.as_str().to_owned()),
			line:     line_number.as_str().parse::<f64>().ok(),
			column:   column.as_str().parse::<f64>().ok(),
			code:     Some("test-failure".to_owned()),
			severity: Some(SeverityField::Text("error".to_owned())),
			message:  Some(format!("test {} panicked", name.as_str())),
			..Default::default()
		});
	}
	diagnostics
}

fn parse_go(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for value in all_json_values(input) {
		visit_go(&value, None, input, &mut diagnostics);
	}
	if diagnostics.is_empty() { parse_generic(input) } else { diagnostics }
}

fn visit_go(
	value: &Value,
	analyzer: Option<&str>,
	input: &CleanseParserInput<'_>,
	diagnostics: &mut Vec<CleanseDiagnostic>,
) {
	if let Some(children) = value.as_array() {
		for child in children {
			visit_go(child, analyzer, input, diagnostics);
		}
		return;
	}
	let Some(record) = to_record(value) else {
		return;
	};
	let position =
		string_field(Some(record), "posn").or_else(|| string_field(Some(record), "position"));
	let message = string_field(Some(record), "message");
	if let (Some(position), Some(message)) = (position, message) {
		if let Some(location) = parse_location(&position) {
			add_diagnostic(diagnostics, input, DiagnosticFields {
				file: Some(location.file),
				line: Some(location.line),
				column: location.column,
				code: string_field(Some(record), "category")
					.or_else(|| analyzer.map(str::to_owned)),
				severity: Some(SeverityField::Text("warning".to_owned())),
				message: Some(message),
				..Default::default()
			});
		}
		return;
	}
	for (key, child) in record {
		visit_go(child, Some(key), input, diagnostics);
	}
}

fn parse_go_test(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for value in parse_json_values(input.stdout) {
		let event = to_record(&value);
		if let Some(output) = string_field(event, "Output") {
			for line in output.split('\n') {
				let Some(captures) = GO_TEST_RE.captures(line) else {
					continue;
				};
				let (Some(file), Some(line_number), Some(message)) =
					(captures.get(1), captures.get(2), captures.get(4))
				else {
					continue;
				};
				add_diagnostic(&mut diagnostics, input, DiagnosticFields {
					file:     Some(file.as_str().to_owned()),
					line:     line_number.as_str().parse::<f64>().ok(),
					column:   captures.get(3).and_then(|value| value.as_str().parse::<f64>().ok()),
					code:     Some("test-failure".to_owned()),
					severity: Some(SeverityField::Text("error".to_owned())),
					message:  Some(message.as_str().to_owned()),
					..Default::default()
				});
			}
		}
		let test_name = string_field(event, "Test");
		if string_field(event, "Action").as_deref() == Some("fail") {
			if let Some(test_name) = test_name {
				add_diagnostic(&mut diagnostics, input, DiagnosticFields {
					code:     Some("test-failure".to_owned()),
					severity: Some(SeverityField::Text("error".to_owned())),
					message:  Some(format!("test {test_name} failed")),
					..Default::default()
				});
			}
		}
	}
	if diagnostics.is_empty() { parse_generic(input) } else { diagnostics }
}

fn parse_staticcheck(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for value in all_json_values(input) {
		let record = to_record(&value);
		let location = nested_record(record, "location");
		let end = nested_record(record, "end");
		add_diagnostic(&mut diagnostics, input, DiagnosticFields {
			file:       string_field(location, "file"),
			line:       number_field(location, "line"),
			column:     number_field(location, "column"),
			end_line:   number_field(end, "line"),
			end_column: number_field(end, "column"),
			code:       string_field(record, "code"),
			severity:   Some(SeverityField::Text(
				string_field(record, "severity").unwrap_or_else(|| "warning".to_owned()),
			)),
			message:    string_field(record, "message"),
			..Default::default()
		});
	}
	diagnostics
}

fn parse_golangci(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	let text = sanitize_text(&format!("{}\n{}", input.stdout, input.stderr));
	for line in text.split('\n') {
		let Some(captures) = GOLANGCI_RE.captures(line.trim()) else {
			continue;
		};
		let (Some(file), Some(line_number), Some(column), Some(message), Some(code)) = (
			captures.get(1),
			captures.get(2),
			captures.get(3),
			captures.get(4),
			captures.get(5),
		) else {
			continue;
		};
		add_diagnostic(&mut diagnostics, input, DiagnosticFields {
			file:     Some(file.as_str().to_owned()),
			line:     line_number.as_str().parse::<f64>().ok(),
			column:   column.as_str().parse::<f64>().ok(),
			code:     Some(code.as_str().to_owned()),
			severity: Some(SeverityField::Text("warning".to_owned())),
			message:  Some(message.as_str().to_owned()),
			..Default::default()
		});
	}
	diagnostics
}

fn parse_ruff(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root in all_json_values(input) {
		for value in to_array(&root) {
			let record = to_record(value);
			let location = nested_record(record, "location");
			let end_location = nested_record(record, "end_location");
			let fix = nested_record(record, "fix");
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:       string_field(record, "filename"),
				line:       number_field(location, "row"),
				column:     number_field(location, "column"),
				end_line:   number_field(end_location, "row"),
				end_column: number_field(end_location, "column"),
				code:       string_field(record, "code"),
				severity:   Some(SeverityField::Text("warning".to_owned())),
				message:    string_field(record, "message"),
				suggestion: string_field(fix, "message"),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_pyright(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root_value in all_json_values(input) {
		let root = to_record(&root_value);
		for value in array_field(root, "generalDiagnostics") {
			let record = to_record(value);
			let range = nested_record(record, "range");
			let start = nested_record(range, "start");
			let end = nested_record(range, "end");
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:       string_field(record, "file"),
				line:       zero_based(number_field(start, "line")),
				column:     zero_based(number_field(start, "character")),
				end_line:   zero_based(number_field(end, "line")),
				end_column: zero_based(number_field(end, "character")),
				code:       string_field(record, "rule"),
				severity:   string_field(record, "severity").map(SeverityField::Text),
				message:    string_field(record, "message"),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_pylint(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root in all_json_values(input) {
		for value in to_array(&root) {
			let record = to_record(value);
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:       string_field(record, "path"),
				line:       number_field(record, "line"),
				column:     zero_based(number_field(record, "column")),
				end_line:   number_field(record, "endLine"),
				end_column: zero_based(number_field(record, "endColumn")),
				code:       string_field(record, "symbol")
					.or_else(|| string_field(record, "message-id")),
				severity:   string_field(record, "type").map(SeverityField::Text),
				message:    string_field(record, "message"),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_flake8(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	let text = sanitize_text(&format!("{}\n{}", input.stdout, input.stderr));
	for line in text.split('\n') {
		let Some(captures) = FLAKE8_RE.captures(line.trim()) else {
			continue;
		};
		let (Some(file), Some(line_number), Some(column), Some(code), Some(message)) = (
			captures.get(1),
			captures.get(2),
			captures.get(3),
			captures.get(4),
			captures.get(5),
		) else {
			continue;
		};
		let code = code.as_str();
		let severity = if code.starts_with('F') || code.starts_with("E9") {
			"error"
		} else {
			"warning"
		};
		add_diagnostic(&mut diagnostics, input, DiagnosticFields {
			file:     Some(file.as_str().to_owned()),
			line:     line_number.as_str().parse::<f64>().ok(),
			column:   column.as_str().parse::<f64>().ok(),
			code:     Some(code.to_owned()),
			severity: Some(SeverityField::Text(severity.to_owned())),
			message:  Some(message.as_str().to_owned()),
			..Default::default()
		});
	}
	diagnostics
}

fn parse_ty(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	let text = sanitize_text(&format!("{}\n{}", input.stdout, input.stderr));
	for line in text.split('\n') {
		let Some(captures) = TY_RE.captures(line.trim()) else {
			continue;
		};
		let (Some(file), Some(line_number), Some(column), Some(severity), Some(code), Some(message)) =
			(
				captures.get(1),
				captures.get(2),
				captures.get(3),
				captures.get(4),
				captures.get(5),
				captures.get(6),
			)
		else {
			continue;
		};
		add_diagnostic(&mut diagnostics, input, DiagnosticFields {
			file:     Some(file.as_str().to_owned()),
			line:     line_number.as_str().parse::<f64>().ok(),
			column:   column.as_str().parse::<f64>().ok(),
			code:     Some(code.as_str().to_owned()),
			severity: Some(SeverityField::Text(severity.as_str().to_owned())),
			message:  Some(message.as_str().to_owned()),
			..Default::default()
		});
	}
	diagnostics
}

fn zero_based(value: Option<f64>) -> Option<f64> {
	value.map(|value| value + 1.0)
}

fn parse_eslint(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root in all_json_values(input) {
		for file_value in to_array(&root) {
			let file = to_record(file_value);
			for message_value in array_field(file, "messages") {
				let message = to_record(message_value);
				let fix = nested_record(message, "fix");
				add_diagnostic(&mut diagnostics, input, DiagnosticFields {
					file:       string_field(file, "filePath"),
					line:       number_field(message, "line"),
					column:     number_field(message, "column"),
					end_line:   number_field(message, "endLine"),
					end_column: number_field(message, "endColumn"),
					code:       string_field(message, "ruleId"),
					severity:   number_field(message, "severity").map(SeverityField::Number),
					message:    string_field(message, "message"),
					suggestion: fix.map(|_| "automatic fix available".to_owned()),
					..Default::default()
				});
			}
		}
	}
	diagnostics
}

fn parse_biome(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root_value in all_json_values(input) {
		let root = to_record(&root_value);
		for value in array_field(root, "diagnostics") {
			let record = to_record(value);
			let location = nested_record(record, "location");
			let path = nested_record(location, "path");
			let message = string_field(record, "description")
				.or_else(|| string_field(record, "message"))
				.or_else(|| string_field(record, "title"));
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:     string_field(path, "file").or_else(|| string_field(location, "path")),
				code:     string_field(record, "category"),
				severity: string_field(record, "severity").map(SeverityField::Text),
				message,
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_unix_format(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	let text = sanitize_text(&format!("{}\n{}", input.stdout, input.stderr));
	for line in text.split('\n') {
		let Some(captures) = UNIX_RE.captures(line.trim()) else {
			continue;
		};
		let (Some(file), Some(line_number), Some(column), Some(message), Some(severity), Some(code)) =
			(
				captures.get(1),
				captures.get(2),
				captures.get(3),
				captures.get(4),
				captures.get(5),
				captures.get(6),
			)
		else {
			continue;
		};
		add_diagnostic(&mut diagnostics, input, DiagnosticFields {
			file:     Some(file.as_str().to_owned()),
			line:     line_number.as_str().parse::<f64>().ok(),
			column:   column.as_str().parse::<f64>().ok(),
			code:     Some(code.as_str().to_owned()),
			severity: Some(SeverityField::Text(severity.as_str().to_owned())),
			message:  Some(message.as_str().to_owned()),
			..Default::default()
		});
	}
	diagnostics
}

fn parse_deno_lint(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root in all_json_values(input) {
		let record = to_record(&root);
		for value in array_field(record, "diagnostics") {
			let entry = to_record(value);
			let range = nested_record(entry, "range");
			let start = nested_record(range, "start");
			let end = nested_record(range, "end");
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:       string_field(entry, "filename"),
				line:       number_field(start, "line"),
				column:     zero_based(number_field(start, "col")),
				end_line:   number_field(end, "line"),
				end_column: zero_based(number_field(end, "col")),
				code:       string_field(entry, "code"),
				severity:   Some(SeverityField::Text("warning".to_owned())),
				message:    string_field(entry, "message"),
				suggestion: string_field(entry, "hint"),
				..Default::default()
			});
		}
		for value in array_field(record, "errors") {
			let entry = to_record(value);
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:     string_field(entry, "file_path"),
				severity: Some(SeverityField::Text("error".to_owned())),
				message:  string_field(entry, "message"),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_stylelint(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root in all_json_values(input) {
		for value in to_array(&root) {
			let record = to_record(value);
			let Some(source) = string_field(record, "source") else {
				continue;
			};
			for warning in array_field(record, "warnings") {
				let entry = to_record(warning);
				add_diagnostic(&mut diagnostics, input, DiagnosticFields {
					file:       Some(source.clone()),
					line:       number_field(entry, "line"),
					column:     number_field(entry, "column"),
					end_line:   number_field(entry, "endLine"),
					end_column: number_field(entry, "endColumn"),
					code:       string_field(entry, "rule"),
					severity:   string_field(entry, "severity").map(SeverityField::Text),
					message:    string_field(entry, "text"),
					..Default::default()
				});
			}
		}
	}
	diagnostics
}

fn parse_rubocop(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root_value in all_json_values(input) {
		let root = to_record(&root_value);
		for file_value in array_field(root, "files") {
			let file = to_record(file_value);
			for offense_value in array_field(file, "offenses") {
				let offense = to_record(offense_value);
				let location = nested_record(offense, "location");
				add_diagnostic(&mut diagnostics, input, DiagnosticFields {
					file:       string_field(file, "path"),
					line:       number_field(location, "start_line"),
					column:     number_field(location, "start_column"),
					end_line:   number_field(location, "last_line"),
					end_column: number_field(location, "last_column"),
					code:       string_field(offense, "cop_name"),
					severity:   string_field(offense, "severity").map(SeverityField::Text),
					message:    string_field(offense, "message"),
					suggestion: (offense
						.and_then(|record| record.get("corrected"))
						.and_then(Value::as_bool)
						== Some(true))
					.then(|| "automatic correction available".to_owned()),
					..Default::default()
				});
			}
		}
	}
	diagnostics
}

fn parse_phpstan(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root_value in all_json_values(input) {
		let root = to_record(&root_value);
		if let Some(files) = nested_record(root, "files") {
			for (file_name, file_value) in files {
				let file = to_record(file_value);
				for message_value in array_field(file, "messages") {
					let message = to_record(message_value);
					add_diagnostic(&mut diagnostics, input, DiagnosticFields {
						file:     Some(file_name.clone()),
						line:     number_field(message, "line"),
						code:     string_field(message, "identifier"),
						severity: Some(SeverityField::Text("error".to_owned())),
						message:  string_field(message, "message"),
						..Default::default()
					});
				}
			}
		}
		for error in array_field(root, "errors") {
			if let Some(message) = error.as_str() {
				add_diagnostic(&mut diagnostics, input, DiagnosticFields {
					severity: Some(SeverityField::Text("error".to_owned())),
					message:  Some(message.to_owned()),
					..Default::default()
				});
			}
		}
	}
	diagnostics
}

fn parse_psalm(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root in all_json_values(input) {
		for value in to_array(&root) {
			let record = to_record(value);
			let shortcode = number_field(record, "shortcode");
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:       string_field(record, "file_name")
					.or_else(|| string_field(record, "file_path")),
				line:       number_field(record, "line_from"),
				column:     number_field(record, "column_from"),
				end_line:   number_field(record, "line_to"),
				end_column: number_field(record, "column_to"),
				code:       string_field(record, "type")
					.or_else(|| shortcode.map(js_number_string)),
				severity:   string_field(record, "severity").map(SeverityField::Text),
				message:    string_field(record, "message"),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_swiftlint(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root in all_json_values(input) {
		for value in to_array(&root) {
			let record = to_record(value);
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:     string_field(record, "file"),
				line:     number_field(record, "line"),
				column:   number_field(record, "character"),
				code:     string_field(record, "rule_id"),
				severity: string_field(record, "severity").map(SeverityField::Text),
				message:  string_field(record, "reason"),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_dart(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	let text = sanitize_text(&format!("{}\n{}", input.stdout, input.stderr));
	for line in text.split('\n') {
		let fields: Vec<&str> = line.split('|').collect();
		if fields.len() < 8 {
			continue;
		}
		add_diagnostic(&mut diagnostics, input, DiagnosticFields {
			file:     Some(fields[3].to_owned()),
			line:     parse_js_int(fields[4]),
			column:   parse_js_int(fields[5]),
			code:     Some(fields[2].to_owned()),
			severity: Some(SeverityField::Text(fields[0].to_owned())),
			message:  Some(fields[7..].join("|")),
			..Default::default()
		});
	}
	diagnostics
}

fn parse_credo(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root_value in all_json_values(input) {
		let root = to_record(&root_value);
		for value in array_field(root, "issues") {
			let record = to_record(value);
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:     string_field(record, "filename"),
				line:     number_field(record, "line_no")
					.or_else(|| number_field(record, "line")),
				column:   number_field(record, "column"),
				code:     string_field(record, "check"),
				severity: string_field(record, "priority")
					.or_else(|| string_field(record, "category"))
					.map(SeverityField::Text),
				message:  string_field(record, "message"),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_shellcheck(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root_value in all_json_values(input) {
		let values: &[Value] = if root_value.is_array() {
			to_array(&root_value)
		} else {
			array_field(to_record(&root_value), "comments")
		};
		for value in values {
			let record = to_record(value);
			let code = number_field(record, "code").map(|code| format!("SC{}", js_number_string(code)));
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:       string_field(record, "file"),
				line:       number_field(record, "line"),
				column:     number_field(record, "column"),
				end_line:   number_field(record, "endLine"),
				end_column: number_field(record, "endColumn"),
				code,
				severity:   string_field(record, "level").map(SeverityField::Text),
				message:    string_field(record, "message"),
				suggestion: truthy(record.and_then(|record| record.get("fix")))
					.then(|| "automatic fix available".to_owned()),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_hlint(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root in all_json_values(input) {
		for value in to_array(&root) {
			let record = to_record(value);
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:       string_field(record, "file"),
				line:       number_field(record, "startLine"),
				column:     number_field(record, "startColumn"),
				end_line:   number_field(record, "endLine"),
				end_column: number_field(record, "endColumn"),
				code:       string_field(record, "hint"),
				severity:   string_field(record, "severity").map(SeverityField::Text),
				message:    string_field(record, "hint")
					.or_else(|| string_field(record, "from")),
				suggestion: string_field(record, "to"),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_terraform(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root_value in all_json_values(input) {
		let root = to_record(&root_value);
		for value in array_field(root, "diagnostics") {
			let record = to_record(value);
			let range = nested_record(record, "range");
			let start = nested_record(range, "start");
			let end = nested_record(range, "end");
			let summary = string_field(record, "summary");
			let detail = string_field(record, "detail");
			let message = match (summary, detail) {
				(Some(summary), Some(detail)) => format!("{summary}: {detail}"),
				(Some(summary), None) => summary,
				(None, Some(detail)) => detail,
				(None, None) => String::new(),
			};
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:       string_field(range, "filename"),
				line:       number_field(start, "line"),
				column:     number_field(start, "column"),
				end_line:   number_field(end, "line"),
				end_column: number_field(end, "column"),
				severity:   string_field(record, "severity").map(SeverityField::Text),
				message:    Some(message),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_tflint(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root_value in all_json_values(input) {
		let root = to_record(&root_value);
		for value in array_field(root, "issues") {
			let record = to_record(value);
			let rule = nested_record(record, "rule");
			let range = nested_record(record, "range");
			let start = nested_record(range, "start");
			let end = nested_record(range, "end");
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:       string_field(range, "filename"),
				line:       number_field(start, "line"),
				column:     number_field(start, "column"),
				end_line:   number_field(end, "line"),
				end_column: number_field(end, "column"),
				code:       string_field(rule, "name"),
				severity:   string_field(rule, "severity").map(SeverityField::Text),
				message:    string_field(record, "message"),
				..Default::default()
			});
		}
		for error_value in array_field(root, "errors") {
			let error = to_record(error_value);
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				severity: Some(SeverityField::Text("error".to_owned())),
				message:  string_field(error, "message"),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_actionlint(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	for root in all_json_values(input) {
		for value in to_array(&root) {
			let record = to_record(value);
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:     string_field(record, "filepath"),
				line:     number_field(record, "line"),
				column:   number_field(record, "column"),
				code:     string_field(record, "kind"),
				severity: Some(SeverityField::Text("error".to_owned())),
				message:  string_field(record, "message"),
				..Default::default()
			});
		}
	}
	diagnostics
}

fn parse_generic(input: &CleanseParserInput<'_>) -> Vec<CleanseDiagnostic> {
	let mut diagnostics = Vec::new();
	let text = sanitize_text(&format!("{}\n{}", input.stdout, input.stderr));
	for line in text.split('\n') {
		let trimmed = line.trim();
		if trimmed.is_empty() {
			continue;
		}
		if let Some(captures) = GENERIC_MSVC_RE.captures(trimmed) {
			let (Some(file), Some(line_number), Some(column), Some(severity), Some(message)) = (
				captures.get(1),
				captures.get(2),
				captures.get(3),
				captures.get(4),
				captures.get(6),
			) else {
				continue;
			};
			add_diagnostic(&mut diagnostics, input, DiagnosticFields {
				file:     Some(file.as_str().to_owned()),
				line:     line_number.as_str().parse::<f64>().ok(),
				column:   column.as_str().parse::<f64>().ok(),
				code:     captures.get(5).map(|code| code.as_str().to_owned()),
				severity: Some(SeverityField::Text(severity.as_str().to_owned())),
				message:  Some(message.as_str().to_owned()),
				..Default::default()
			});
			continue;
		}
		let Some(captures) = GENERIC_GCC_RE.captures(trimmed) else {
			continue;
		};
		let (Some(file), Some(line_number), Some(severity), Some(message)) =
			(captures.get(1), captures.get(2), captures.get(4), captures.get(6))
		else {
			continue;
		};
		add_diagnostic(&mut diagnostics, input, DiagnosticFields {
			file:     Some(file.as_str().to_owned()),
			line:     line_number.as_str().parse::<f64>().ok(),
			column:   captures.get(3).and_then(|column| column.as_str().parse::<f64>().ok()),
			code:     captures.get(5).map(|code| code.as_str().to_owned()),
			severity: Some(SeverityField::Text(severity.as_str().to_owned())),
			message:  Some(message.as_str().to_owned()),
			..Default::default()
		});
	}
	diagnostics
}

fn parse_location(value: &str) -> Option<ParsedLocation> {
	let trimmed = value.trim();
	if let Some(captures) = LOCATION_PAREN_RE.captures(trimmed) {
		return Some(ParsedLocation {
			file:   captures.get(1)?.as_str().to_owned(),
			line:   captures.get(2)?.as_str().parse::<f64>().ok()?,
			column: Some(captures.get(3)?.as_str().parse::<f64>().ok()?),
		});
	}
	let captures = LOCATION_COLON_RE.captures(trimmed)?;
	Some(ParsedLocation {
		file:   captures.get(1)?.as_str().to_owned(),
		line:   captures.get(2)?.as_str().parse::<f64>().ok()?,
		column: captures.get(3).and_then(|column| column.as_str().parse::<f64>().ok()),
	})
}

fn deduplicate_diagnostics(diagnostics: Vec<CleanseDiagnostic>) -> Vec<CleanseDiagnostic> {
	let mut seen = HashSet::new();
	diagnostics
		.into_iter()
		.filter(|diagnostic| {
			seen.insert((
				diagnostic.file.clone().unwrap_or_default(),
				diagnostic.line,
				diagnostic.column,
				diagnostic.code.clone().unwrap_or_default(),
				diagnostic.message.clone(),
			))
		})
		.collect()
}

/// Identity key shared by parser deduplication and cross-checker reporting.
pub fn diagnostic_key(diagnostic: &CleanseDiagnostic) -> String {
	[
		diagnostic.file.as_deref().unwrap_or("").to_owned(),
		diagnostic.line.map_or_else(String::new, |line| line.to_string()),
		diagnostic.column.map_or_else(String::new, |column| column.to_string()),
		diagnostic.code.as_deref().unwrap_or("").to_owned(),
		diagnostic.message.clone(),
	]
	.join("\u{0}")
}

fn truthy(value: Option<&Value>) -> bool {
	match value {
		None | Some(Value::Null) => false,
		Some(Value::Bool(value)) => *value,
		Some(Value::Number(value)) => value.as_f64().is_some_and(|value| value != 0.0),
		Some(Value::String(value)) => !value.is_empty(),
		Some(Value::Array(_) | Value::Object(_)) => true,
	}
}

fn js_number_string(value: f64) -> String {
	if value.fract() == 0.0 { format!("{value:.0}") } else { value.to_string() }
}

/// `Number.parseInt(value, 10)`-style leading-integer parse.
fn parse_js_int(value: &str) -> Option<f64> {
	let trimmed = value.trim_start();
	let (sign, rest) = if let Some(rest) = trimmed.strip_prefix('-') {
		(-1.0, rest)
	} else if let Some(rest) = trimmed.strip_prefix('+') {
		(1.0, rest)
	} else {
		(1.0, trimmed)
	};
	let end = rest.find(|character: char| !character.is_ascii_digit()).unwrap_or(rest.len());
	if end == 0 {
		return None;
	}
	rest[..end].parse::<f64>().ok().map(|value| value * sign)
}

/// Strip ANSI escapes and the control characters the donor sanitizer removes
/// (C0 except tab/newline, DEL, C1), preserving all other text.
pub(super) fn sanitize_text(text: &str) -> String {
	let mut output = String::with_capacity(text.len());
	let mut characters = text.chars().peekable();
	while let Some(character) = characters.next() {
		if character == '\u{1b}' {
			skip_escape(&mut characters);
			continue;
		}
		if !is_control(character) {
			output.push(character);
		}
	}
	output
}

fn skip_escape(characters: &mut std::iter::Peekable<std::str::Chars<'_>>) {
	match characters.next() {
		Some('[') => {
			for character in characters.by_ref() {
				if matches!(character, '\u{40}'..='\u{7e}') {
					break;
				}
			}
		},
		Some(']') => {
			while let Some(character) = characters.next() {
				if character == '\u{7}' {
					break;
				}
				if character == '\u{1b}' {
					if characters.peek() == Some(&'\\') {
						characters.next();
					}
					break;
				}
			}
		},
		Some('P' | '^' | '_') => {
			while let Some(character) = characters.next() {
				if character == '\u{1b}' && characters.peek() == Some(&'\\') {
					characters.next();
					break;
				}
			}
		},
		Some(_) | None => {},
	}
}

const fn is_control(character: char) -> bool {
	matches!(character, '\u{0}'..='\u{8}' | '\u{b}'..='\u{1f}' | '\u{7f}'..='\u{9f}')
}

fn collapse_whitespace(text: &str) -> String {
	let mut output = String::with_capacity(text.len());
	let mut pending_space = false;
	for character in text.chars() {
		if character.is_whitespace() || character == '\u{feff}' {
			pending_space = !output.is_empty();
		} else {
			if pending_space {
				output.push(' ');
				pending_space = false;
			}
			output.push(character);
		}
	}
	output
}

pub(super) fn normalize_lexical(path: &Path) -> PathBuf {
	let absolute = path.is_absolute();
	let mut output = PathBuf::new();
	for component in path.components() {
		match component {
			Component::Prefix(prefix) => output.push(prefix.as_os_str()),
			Component::RootDir => output.push(Component::RootDir.as_os_str()),
			Component::CurDir => {},
			Component::ParentDir => {
				if output.file_name().is_some() {
					output.pop();
				} else if !absolute {
					output.push("..");
				}
			},
			Component::Normal(part) => output.push(part),
		}
	}
	output
}

pub(super) fn path_relative(target: &Path, base: &Path) -> Option<PathBuf> {
	let target_parts: Vec<Component<'_>> = target.components().collect();
	let base_parts: Vec<Component<'_>> = base.components().collect();
	if (target.is_absolute() || base.is_absolute()) && target_parts.first() != base_parts.first() {
		return None;
	}
	let common = target_parts
		.iter()
		.zip(&base_parts)
		.take_while(|(target, base)| target == base)
		.count();
	let mut relative = PathBuf::new();
	for _ in common..base_parts.len() {
		relative.push("..");
	}
	for component in &target_parts[common..] {
		relative.push(component.as_os_str());
	}
	Some(relative)
}

fn percent_decode(input: &str) -> Option<String> {
	let bytes = input.as_bytes();
	let mut decoded = Vec::with_capacity(bytes.len());
	let mut index = 0;
	while index < bytes.len() {
		if bytes[index] == b'%' {
			let high = hex_value(*bytes.get(index + 1)?)?;
			let low = hex_value(*bytes.get(index + 2)?)?;
			decoded.push(high * 16 + low);
			index += 3;
		} else {
			decoded.push(bytes[index]);
			index += 1;
		}
	}
	String::from_utf8(decoded).ok()
}

const fn hex_value(byte: u8) -> Option<u8> {
	match byte {
		b'0'..=b'9' => Some(byte - b'0'),
		b'a'..=b'f' => Some(byte - b'a' + 10),
		b'A'..=b'F' => Some(byte - b'A' + 10),
		_ => None,
	}
}

static PANIC_RE: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r"thread '([^']+)' panicked at (.*?):(\d+):(\d+):")
		.expect("static checker regex must compile")
});
static GO_TEST_RE: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r"^\s*(?:\./)?(.+?\.go):(\d+)(?::(\d+))?:\s*(.+?)\s*$")
		.expect("static checker regex must compile")
});
static GOLANGCI_RE: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r"^(.+?):(\d+):(\d+):\s+(.*?)\s+\(([A-Za-z0-9_-]+)\)$")
		.expect("static checker regex must compile")
});
static FLAKE8_RE: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r"^(.+?):(\d+):(\d+):\s+([A-Z]+\d+)\s+(.*)$")
		.expect("static checker regex must compile")
});
static TY_RE: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r"^(.+?):(\d+):(\d+):\s+(error|warning|info)\[([^\]]+)\]\s+(.*)$")
		.expect("static checker regex must compile")
});
static UNIX_RE: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r"(?i)^(.+?):(\d+):(\d+):\s+(.*?)\s+\[(Error|Warning)/([^\]]+)\]$")
		.expect("static checker regex must compile")
});
static GENERIC_MSVC_RE: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(
		r"(?i)^(.*?)\((\d+),(\d+)\):\s*(error|warning|info)(?:\s+([A-Za-z]+\d+))?:\s*(.*?)(?:\s+\[[^\]]+\])?$",
	)
	.expect("static checker regex must compile")
});
static GENERIC_GCC_RE: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(
		r"(?i)^(.*?):(\d+)(?::(\d+))?:\s*(error|warning|info|note)(?:\s*\[([^\]]+)\])?:\s*(.*)$",
	)
	.expect("static checker regex must compile")
});
static LOCATION_PAREN_RE: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r"^(.*?)\((\d+),(\d+)\)$").expect("static checker regex must compile")
});
static LOCATION_COLON_RE: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r"^(.*?):(\d+)(?::(\d+))?$").expect("static checker regex must compile")
});

#[cfg(test)]
mod tests {
	use std::path::Path;

	use super::{
		CleanseDiagnostic, CleanseParserInput, CleanseParserKind, CleanseSeverity, normalize_file,
		parse_cleanse_diagnostics,
	};

	fn input_with_stderr<'a>(
		project_cwd: &'a Path,
		checker_cwd: &'a Path,
		stdout: &'a str,
		stderr: &'a str,
	) -> CleanseParserInput<'a> {
		CleanseParserInput { checker: "test", project_cwd, checker_cwd, stdout, stderr }
	}

	fn input<'a>(
		project_cwd: &'a Path,
		checker_cwd: &'a Path,
		stdout: &'a str,
	) -> CleanseParserInput<'a> {
		input_with_stderr(project_cwd, checker_cwd, stdout, "")
	}

	fn single(
		kind: CleanseParserKind,
		project_cwd: &Path,
		checker_cwd: &Path,
		stdout: &str,
		stderr: &str,
	) -> CleanseDiagnostic {
		let diagnostics =
			parse_cleanse_diagnostics(kind, &input_with_stderr(project_cwd, checker_cwd, stdout, stderr));
		assert_eq!(diagnostics.len(), 1, "{kind:?} parsed {diagnostics:?}");
		diagnostics.into_iter().next().expect("one diagnostic")
	}

	#[test]
	fn normalizes_paths_against_the_checker_cwd() {
		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::Generic,
			&input(
				Path::new("project"),
				Path::new("project/sub"),
				"src/main.c:12:3: warning: unused variable [foo]\n",
			),
		);
		assert_eq!(diagnostics.len(), 1);
		assert_eq!(diagnostics[0].file.as_deref(), Some("sub/src/main.c"));
		assert_eq!(diagnostics[0].line, Some(12));
		assert_eq!(diagnostics[0].column, Some(3));
		assert_eq!(diagnostics[0].severity, CleanseSeverity::Warning);
		assert_eq!(diagnostics[0].message, "unused variable [foo]");
	}

	#[test]
	fn drops_paths_outside_the_project() {
		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::Generic,
			&input(Path::new("project"), Path::new("other"), "src/main.c:1:1: error: bad\n"),
		);
		assert!(diagnostics.is_empty());
	}

	#[cfg(unix)]
	#[test]
	fn decodes_file_urls() {
		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::Generic,
			&input(
				Path::new("/project"),
				Path::new("/project"),
				"file:///project/src/a%20b.c:1:1: error: bad\n",
			),
		);
		assert_eq!(diagnostics.len(), 1);
		assert_eq!(diagnostics[0].file.as_deref(), Some("src/a b.c"));
	}

	#[test]
	fn parses_rust_compiler_messages() {
		let stdout = concat!(
			r#"{"reason":"compiler-message","message":{"level":"warning","message":"unused"#,
			r#" variable","code":{"code":"unused_variables"},"spans":[{"file_name":"src/lib.rs","#,
			r#""line_start":3,"column_start":5,"is_primary":true}]}}"#,
		);
		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::Rust,
			&input(Path::new("project"), Path::new("project"), stdout),
		);
		assert_eq!(diagnostics.len(), 1);
		assert_eq!(diagnostics[0].file.as_deref(), Some("src/lib.rs"));
		assert_eq!(diagnostics[0].code.as_deref(), Some("unused_variables"));
		assert_eq!(diagnostics[0].severity, CleanseSeverity::Warning);
	}

	#[test]
	fn parses_eslint_severities() {
		let stdout = concat!(
			r#"[{"filePath":"src/app.ts","messages":["#,
			r#"{"ruleId":"no-x","severity":2,"message":"boom","line":1,"column":2},"#,
			r#"{"ruleId":"no-y","severity":1,"message":"meh","line":3}]}]"#,
		);
		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::Eslint,
			&input(Path::new("project"), Path::new("project"), stdout),
		);
		assert_eq!(diagnostics.len(), 2);
		assert_eq!(diagnostics[0].severity, CleanseSeverity::Error);
		assert_eq!(diagnostics[1].severity, CleanseSeverity::Warning);
		assert_eq!(diagnostics[0].file.as_deref(), Some("src/app.ts"));
	}

	#[test]
	fn deduplicates_identical_diagnostics() {
		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::Generic,
			&input(
				Path::new("project"),
				Path::new("project"),
				"src/a.c:1:1: error: bad\nsrc/a.c:1:1: error: bad\n",
			),
		);
		assert_eq!(diagnostics.len(), 1);
	}

	#[test]
	fn parses_representative_output_from_every_structured_and_line_parser() {
		let cases: &[(
			CleanseParserKind,
			&str,
			&str,
			Option<u64>,
			Option<u64>,
			Option<&str>,
			CleanseSeverity,
			&str,
		)] = &[
			(
				CleanseParserKind::Rust,
				r#"{"reason":"compiler-message","message":{"level":"warning","message":"useless use of vec!","code":{"code":"clippy::useless_vec"},"spans":[{"file_name":"src/main.rs","line_start":2,"column_start":18,"line_end":2,"column_end":31,"is_primary":true,"suggested_replacement":"[1, 2, 3]"}]}}"#,
				"src/main.rs",
				Some(2),
				Some(18),
				Some("clippy::useless_vec"),
				CleanseSeverity::Warning,
				"useless use of vec!",
			),
			(
				CleanseParserKind::Staticcheck,
				r#"{"code":"S1002","severity":"error","location":{"file":"/repo/main.go","line":5,"column":7},"end":{"line":5,"column":20},"message":"should omit comparison"}"#,
				"main.go",
				Some(5),
				Some(7),
				Some("S1002"),
				CleanseSeverity::Error,
				"should omit comparison",
			),
			(
				CleanseParserKind::Go,
				r#"{"unused":[{"posn":"/repo/main.go:7:2","message":"bad go"}]}"#,
				"main.go",
				Some(7),
				Some(2),
				Some("unused"),
				CleanseSeverity::Warning,
				"bad go",
			),
			(
				CleanseParserKind::Golangci,
				"main.go:10:2: ineffectual assignment to err (ineffassign)\n",
				"main.go",
				Some(10),
				Some(2),
				Some("ineffassign"),
				CleanseSeverity::Warning,
				"ineffectual assignment to err",
			),
			(
				CleanseParserKind::Ruff,
				r#"[{"filename":"/repo/src/app.py","location":{"row":2,"column":5},"end_location":{"row":2,"column":8},"code":"F401","message":"unused import","fix":{"message":"remove"}}]"#,
				"src/app.py",
				Some(2),
				Some(5),
				Some("F401"),
				CleanseSeverity::Warning,
				"unused import",
			),
			(
				CleanseParserKind::Pyright,
				r#"{"generalDiagnostics":[{"file":"/repo/src/app.py","severity":"error","rule":"reportMissingImports","message":"Could not resolve import","range":{"start":{"line":1,"character":2},"end":{"line":1,"character":4}}}]}"#,
				"src/app.py",
				Some(2),
				Some(3),
				Some("reportMissingImports"),
				CleanseSeverity::Error,
				"Could not resolve import",
			),
			(
				CleanseParserKind::Mypy,
				"src/app.py:4:1: error: Incompatible types [assignment]\n",
				"src/app.py",
				Some(4),
				Some(1),
				None,
				CleanseSeverity::Error,
				"Incompatible types [assignment]",
			),
			(
				CleanseParserKind::Pylint,
				r#"[{"type":"error","path":"src/app.py","line":3,"column":0,"endLine":3,"endColumn":10,"symbol":"undefined-variable","message":"Undefined variable 'x'"}]"#,
				"src/app.py",
				Some(3),
				Some(1),
				Some("undefined-variable"),
				CleanseSeverity::Error,
				"Undefined variable 'x'",
			),
			(
				CleanseParserKind::Flake8,
				"src/app.py:1:1: F401 'os' imported but unused\n",
				"src/app.py",
				Some(1),
				Some(1),
				Some("F401"),
				CleanseSeverity::Error,
				"'os' imported but unused",
			),
			(
				CleanseParserKind::Ty,
				"src/main.py:1:8: error[unresolved-import] Cannot resolve imported module 'foo'\n",
				"src/main.py",
				Some(1),
				Some(8),
				Some("unresolved-import"),
				CleanseSeverity::Error,
				"Cannot resolve imported module 'foo'",
			),
			(
				CleanseParserKind::Eslint,
				r#"[{"filePath":"/repo/src/app.ts","messages":[{"ruleId":"no-x","severity":2,"message":"boom","line":1,"column":2}]}]"#,
				"src/app.ts",
				Some(1),
				Some(2),
				Some("no-x"),
				CleanseSeverity::Error,
				"boom",
			),
			(
				CleanseParserKind::Biome,
				r#"{"diagnostics":[{"category":"lint/suspicious/noDebugger","severity":"warning","description":"Unexpected debugger","location":{"path":{"file":"/repo/src/a.ts"}}}]}"#,
				"src/a.ts",
				None,
				None,
				Some("lint/suspicious/noDebugger"),
				CleanseSeverity::Warning,
				"Unexpected debugger",
			),
			(
				CleanseParserKind::Oxlint,
				"src/index.ts:4:10: Variable 'x' is declared but never used. [Warning/no-unused-vars]\n",
				"src/index.ts",
				Some(4),
				Some(10),
				Some("no-unused-vars"),
				CleanseSeverity::Warning,
				"Variable 'x' is declared but never used.",
			),
			(
				CleanseParserKind::Stylelint,
				r#"[{"source":"/repo/styles/site.css","warnings":[{"line":7,"column":3,"rule":"color-no-invalid-hex","severity":"error","text":"Unexpected invalid hex color"}]}]"#,
				"styles/site.css",
				Some(7),
				Some(3),
				Some("color-no-invalid-hex"),
				CleanseSeverity::Error,
				"Unexpected invalid hex color",
			),
			(
				CleanseParserKind::Rubocop,
				r#"{"files":[{"path":"/repo/src/app.rb","offenses":[{"cop_name":"Style/FrozenStringLiteralComment","severity":"convention","message":"Missing frozen string literal comment","corrected":true,"location":{"start_line":1,"start_column":1,"last_line":1,"last_column":2}}]}]}"#,
				"src/app.rb",
				Some(1),
				Some(1),
				Some("Style/FrozenStringLiteralComment"),
				CleanseSeverity::Warning,
				"Missing frozen string literal comment",
			),
			(
				CleanseParserKind::Phpstan,
				r#"{"files":{"src/app.php":{"messages":[{"line":5,"identifier":"argument.type","message":"expects int"}]}}}"#,
				"src/app.php",
				Some(5),
				None,
				Some("argument.type"),
				CleanseSeverity::Error,
				"expects int",
			),
			(
				CleanseParserKind::Psalm,
				r#"[{"file_name":"/repo/src/app.php","line_from":4,"column_from":3,"line_to":4,"column_to":8,"type":"InvalidArgument","severity":"error","message":"bad psalm"}]"#,
				"src/app.php",
				Some(4),
				Some(3),
				Some("InvalidArgument"),
				CleanseSeverity::Error,
				"bad psalm",
			),
			(
				CleanseParserKind::Swiftlint,
				r#"[{"file":"/repo/Sources/App.swift","line":8,"character":2,"rule_id":"force_cast","severity":"error","reason":"Force casts should be avoided"}]"#,
				"Sources/App.swift",
				Some(8),
				Some(2),
				Some("force_cast"),
				CleanseSeverity::Error,
				"Force casts should be avoided",
			),
			(
				CleanseParserKind::Dart,
				"ERROR|COMPILE_TIME_ERROR|undefined_identifier|lib/src/a.dart|3|7|4|Undefined name 'x'\n",
				"lib/src/a.dart",
				Some(3),
				Some(7),
				Some("undefined_identifier"),
				CleanseSeverity::Error,
				"Undefined name 'x'",
			),
			(
				CleanseParserKind::Credo,
				r#"{"issues":[{"filename":"lib/app.ex","line_no":4,"column":2,"check":"Credo.Check.X","category":"warning","message":"bad credo"}]}"#,
				"lib/app.ex",
				Some(4),
				Some(2),
				Some("Credo.Check.X"),
				CleanseSeverity::Warning,
				"bad credo",
			),
			(
				CleanseParserKind::Shellcheck,
				r#"{"comments":[{"file":"/repo/scripts/a.sh","line":2,"column":1,"endLine":2,"endColumn":4,"code":2086,"level":"warning","message":"Quote this","fix":{}}]}"#,
				"scripts/a.sh",
				Some(2),
				Some(1),
				Some("SC2086"),
				CleanseSeverity::Warning,
				"Quote this",
			),
			(
				CleanseParserKind::Hlint,
				r#"[{"file":"/repo/src/App.hs","startLine":3,"startColumn":4,"endLine":3,"endColumn":8,"hint":"Redundant bracket","severity":"warning","from":"(x)","to":"x"}]"#,
				"src/App.hs",
				Some(3),
				Some(4),
				Some("Redundant bracket"),
				CleanseSeverity::Warning,
				"Redundant bracket",
			),
			(
				CleanseParserKind::Terraform,
				r#"{"diagnostics":[{"severity":"error","summary":"Invalid reference","detail":"Unknown value","range":{"filename":"/repo/main.tf","start":{"line":2,"column":3},"end":{"line":2,"column":8}}}]}"#,
				"main.tf",
				Some(2),
				Some(3),
				None,
				CleanseSeverity::Error,
				"Invalid reference: Unknown value",
			),
			(
				CleanseParserKind::Tflint,
				r#"{"issues":[{"rule":{"name":"terraform_unused_declarations","severity":"warning"},"range":{"filename":"/repo/main.tf","start":{"line":3,"column":1},"end":{"line":3,"column":5}},"message":"unused"}]}"#,
				"main.tf",
				Some(3),
				Some(1),
				Some("terraform_unused_declarations"),
				CleanseSeverity::Warning,
				"unused",
			),
			(
				CleanseParserKind::Actionlint,
				r#"[{"message":"bad workflow","filepath":".github/workflows/ci.yml","line":12,"column":9,"kind":"syntax-check"}]"#,
				".github/workflows/ci.yml",
				Some(12),
				Some(9),
				Some("syntax-check"),
				CleanseSeverity::Error,
				"bad workflow",
			),
			(
				CleanseParserKind::Generic,
				"src/main.c:2:3: error [E123]: bad thing\n",
				"src/main.c",
				Some(2),
				Some(3),
				Some("E123"),
				CleanseSeverity::Error,
				"bad thing",
			),
		];
		for &(kind, stdout, file, line, column, code, severity, message) in cases {
			let diagnostic = single(kind, Path::new("/repo"), Path::new("/repo"), stdout, "");
			assert_eq!(diagnostic.file.as_deref(), Some(file), "{kind:?}");
			assert_eq!(diagnostic.line, line, "{kind:?}");
			assert_eq!(diagnostic.column, column, "{kind:?}");
			assert_eq!(diagnostic.code.as_deref(), code, "{kind:?}");
			assert_eq!(diagnostic.severity, severity, "{kind:?}");
			assert_eq!(diagnostic.message, message, "{kind:?}");
		}
	}

	#[test]
	fn parses_rust_test_panic_from_stderr() {
		let diagnostic = single(
			CleanseParserKind::RustTest,
			Path::new("/repo"),
			Path::new("/repo"),
			"",
			"thread 'tests::broken' panicked at tests/broken.rs:9:3:\nassertion failed\n",
		);
		assert_eq!(diagnostic.file.as_deref(), Some("tests/broken.rs"));
		assert_eq!(diagnostic.line, Some(9));
		assert_eq!(diagnostic.column, Some(3));
		assert_eq!(diagnostic.code.as_deref(), Some("test-failure"));
		assert_eq!(diagnostic.severity, CleanseSeverity::Error);
		assert_eq!(diagnostic.message, "test tests::broken panicked");
	}

	#[test]
	fn parses_go_test_output_and_failure_events() {
		let stdout = concat!(
			r#"{"Action":"output","Output":"    main_test.go:12: expected 1\n"}"#,
			"\n",
			r#"{"Action":"fail","Test":"TestMain","Output":"--- FAIL: TestMain (0.00s)\n"}"#,
		);
		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::GoTest,
			&input(Path::new("/repo"), Path::new("/repo"), stdout),
		);
		assert_eq!(diagnostics.len(), 2);
		assert_eq!(diagnostics[0].file.as_deref(), Some("main_test.go"));
		assert_eq!(diagnostics[0].line, Some(12));
		assert_eq!(diagnostics[0].column, None);
		assert_eq!(diagnostics[0].code.as_deref(), Some("test-failure"));
		assert_eq!(diagnostics[0].message, "expected 1");
		assert_eq!(diagnostics[1].file, None);
		assert_eq!(diagnostics[1].message, "test TestMain failed");
	}

	#[test]
	fn parses_deno_lint_diagnostics_and_errors() {
		let stdout = r#"{"diagnostics":[{"filename":"file:///repo/mod.ts","range":{"start":{"line":2,"col":4},"end":{"line":2,"col":9}},"code":"no-var","message":"`var` keyword is not allowed.","hint":"Use `let` or `const` instead."}],"errors":[{"file_path":"/repo/main.ts","message":"bad config"}]}"#;
		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::DenoLint,
			&input(Path::new("/repo"), Path::new("/repo"), stdout),
		);
		assert_eq!(diagnostics.len(), 2);
		assert_eq!(diagnostics[0].file.as_deref(), Some("mod.ts"));
		assert_eq!(diagnostics[0].line, Some(2));
		assert_eq!(diagnostics[0].column, Some(5));
		assert_eq!(diagnostics[0].code.as_deref(), Some("no-var"));
		assert_eq!(diagnostics[0].severity, CleanseSeverity::Warning);
		assert_eq!(
			diagnostics[0].suggestion.as_deref(),
			Some("Use `let` or `const` instead.")
		);
		assert_eq!(diagnostics[1].file.as_deref(), Some("main.ts"));
		assert_eq!(diagnostics[1].severity, CleanseSeverity::Error);
		assert_eq!(diagnostics[1].message, "bad config");
	}

	#[test]
	fn normalizes_absolute_quoted_and_escaping_paths() {
		let absolute = single(
			CleanseParserKind::Generic,
			Path::new("/repo"),
			Path::new("/repo/sub"),
			"/repo/src/../src/./main.c:1:1: warning: absolute\n",
			"",
		);
		assert_eq!(absolute.file.as_deref(), Some("src/main.c"));

		let quoted = single(
			CleanseParserKind::Generic,
			Path::new("/repo"),
			Path::new("/repo/sub"),
			"'../src/main.c':1:1: warning: quoted\n",
			"",
		);
		assert_eq!(quoted.file.as_deref(), Some("src/main.c"));

		for line in [
			"../../outside.c:1:1: warning: outside\n",
			"<internal>:1:1: warning: internal\n",
			"file:///repo/src/a%ZZ.c:1:1: warning: malformed\n",
		] {
			let diagnostics = parse_cleanse_diagnostics(
				CleanseParserKind::Generic,
				&input(Path::new("/repo"), Path::new("/repo/sub"), line),
			);
			assert!(diagnostics.is_empty(), "{line:?} must be dropped");
		}
	}

	#[test]
	fn rejects_nul_bytes_in_paths() {
		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::Generic,
			&input(
				Path::new("/repo"),
				Path::new("/repo"),
				"file:///repo/src/a%00.c:1:1: warning: nul\n",
			),
		);
		assert!(diagnostics.is_empty());
		let stdout = r#"[{"filePath":"src/a\u0000.c","messages":[{"ruleId":"no-x","severity":2,"message":"boom","line":1,"column":1}]}]"#;
		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::Eslint,
			&input(Path::new("/repo"), Path::new("/repo"), stdout),
		);
		assert!(diagnostics.is_empty());
		let raw = CleanseParserInput {
			checker: "test",
			project_cwd: Path::new("/repo"),
			checker_cwd: Path::new("/repo"),
			stdout: "",
			stderr: "",
		};
		assert!(normalize_file("src/a\0.c", &raw).is_none());
	}

	#[test]
	fn deduplicates_across_stdout_and_stderr_but_keeps_distinct_locations() {
		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::Generic,
			&input_with_stderr(
				Path::new("/repo"),
				Path::new("/repo"),
				"src/a.c:1:2: warning: same\n",
				"src/a.c:1:2: error: same\n",
			),
		);
		assert_eq!(diagnostics.len(), 1);
		assert_eq!(diagnostics[0].severity, CleanseSeverity::Warning);

		let diagnostics = parse_cleanse_diagnostics(
			CleanseParserKind::Generic,
			&input(
				Path::new("/repo"),
				Path::new("/repo"),
				"src/a.c:1:2: warning: same\nsrc/a.c:1:3: warning: same\n",
			),
		);
		assert_eq!(diagnostics.len(), 2);
	}
}
