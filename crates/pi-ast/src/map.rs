//! Syntax-only declaration map for repository directories.
//!
//! This is deliberately *not* a semantic index: it reports the declarations
//! tree-sitter can see (kind, name, 1-based start line) and never resolves
//! references, types, or overloads. Bodies are omitted; a declaration whose
//! body contains another declaration is still traversed only when it is a
//! container (class/struct/impl/module/...), not when it is a function body.
//!
//! Only languages with an explicit declaration table below are mapped. A file
//! in any other supported language is left to the caller to count as skipped,
//! so the map cannot invent symbols from guesses about unknown grammars.
//!
//! Mapped languages: Rust, TypeScript/TSX/JavaScript, Python, Go, Java, C/C++,
//! C#, Ruby, PHP, Swift, Kotlin, Scala, Lua, Bash, Zig, Dart, and Solidity.

use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tree_sitter::Node;

use crate::{language::SupportLang, parse_cache::parse_cached, summary::node_start_line};

/// Per-file symbol ceiling. Callers that aggregate many files impose a second,
/// global ceiling on top of this one.
pub const MAP_MAX_SYMBOLS_PER_FILE: usize = 200;
/// Longest rendered name. Tree-sitter names can include qualified paths; the
/// cap keeps one hostile name from dominating the output.
const MAX_NAME_CHARS: usize = 120;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapOptions {
	/// Source code to map.
	pub code:        String,
	/// Language alias (e.g. "rust", "typescript"); path inference is used when
	/// omitted.
	pub lang:        Option<String>,
	/// File path used to infer language by extension when `lang` is omitted.
	pub path:        Option<String>,
	/// Per-file symbol ceiling. Defaults to [`MAP_MAX_SYMBOLS_PER_FILE`].
	pub max_symbols: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MapSymbol {
	/// Short syntax label (`fn`, `struct`, `class`, `method`, ...).
	pub kind: String,
	/// Declared name as written, with whitespace collapsed.
	pub name: String,
	/// 1-based line of the declaration node.
	pub line: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MapResult {
	/// Canonical language name when a map rule exists.
	pub language:  Option<String>,
	/// False for unparseable sources or tree-sitter syntax errors. Symbols
	/// collected before the error are still returned, so the caller can mark
	/// the file as incomplete rather than silently dropping it.
	pub parsed:    bool,
	/// Declarations in source order. Empty when the language has no map rule.
	pub symbols:   Vec<MapSymbol>,
	/// True when a declaration was dropped because of `max_symbols`.
	pub truncated: bool,
}

/// Return the language for `path` only when this module has declaration rules
/// for it. Unsupported languages are intentionally `None`.
#[must_use]
pub fn map_supported_path(path: &Path) -> Option<SupportLang> {
	SupportLang::from_path(path).filter(|lang| has_map_rules(*lang))
}

/// Map declarations in `options.code` without emitting any body text.
pub fn map_code(options: MapOptions) -> Result<MapResult> {
	let source = options.code;
	let language = options
		.lang
		.as_deref()
		.and_then(SupportLang::from_alias)
		.or_else(|| options.path.as_deref().and_then(|path| SupportLang::from_path(Path::new(path))))
		.filter(|language| has_map_rules(*language));
	let Some(language) = language else {
		return Ok(MapResult {
			language:  None,
			parsed:    false,
			symbols:   Vec::new(),
			truncated: false,
		});
	};
	if source.is_empty() {
		return Ok(MapResult {
			language:  Some(language.canonical_name().to_string()),
			parsed:    true,
			symbols:   Vec::new(),
			truncated: false,
		});
	}

	let Some(tree) = parse_cached(&source, language)? else {
		return Ok(MapResult {
			language:  Some(language.canonical_name().to_string()),
			parsed:    false,
			symbols:   Vec::new(),
			truncated: false,
		});
	};

	let max_symbols = options.max_symbols.unwrap_or(MAP_MAX_SYMBOLS_PER_FILE).max(1);
	let root = tree.root_node();
	let mut symbols = std::collections::BTreeSet::new();
	let mut truncated = false;
	collect_declarations(root, language, &source, max_symbols, &mut symbols, &mut truncated);

	Ok(MapResult {
		language: Some(language.canonical_name().to_string()),
		parsed: !root.has_error(),
		symbols: sorted_symbols(symbols),
		truncated,
	})
}

fn sorted_symbols(symbols: std::collections::BTreeSet<MapSymbol>) -> Vec<MapSymbol> {
	let mut symbols: Vec<MapSymbol> = symbols.into_iter().collect();
	symbols.sort_by(|left, right| {
		left
			.line
			.cmp(&right.line)
			.then_with(|| left.kind.cmp(&right.kind))
			.then_with(|| left.name.cmp(&right.name))
	});
	symbols
}

fn collect_declarations(
	root: Node<'_>,
	language: SupportLang,
	source: &str,
	max_symbols: usize,
	symbols: &mut std::collections::BTreeSet<MapSymbol>,
	truncated: &mut bool,
) {
	let mut stack = vec![root];
	while let Some(node) = stack.pop() {
		// Function expressions, object literals, anonymous classes, and Rust
		// async/const/unsafe or JS static blocks are not declarations; their
		// bodies must not leak local declarations into the map.
		if is_body_only_kind(language, node.kind()) {
			continue;
		}
		let label = declaration_label(language, node, source);
		if let Some(label) = label {
			let mut budget_exhausted = false;
			for name in declaration_names(node, language, source) {
				let symbol = MapSymbol { kind: label.to_string(), name, line: node_start_line(node) };
				if symbols.len() >= max_symbols && !symbols.contains(&symbol) {
					*truncated = true;
					budget_exhausted = true;
					break;
				}
				symbols.insert(symbol);
			}
			if budget_exhausted {
				// No later declaration can be emitted; stop instead of walking
				// the remaining tree.
				break;
			}
			// Function-like declarations own their body: do not walk into it.
			// Container declarations expose their member declarations.
			if !is_container_label(label) {
				continue;
			}
		}
		let mut cursor = node.walk();
		let mut children: Vec<Node<'_>> = node.named_children(&mut cursor).collect();
		while let Some(child) = children.pop() {
			stack.push(child);
		}
	}
}

/// Short labels for the declaration node kinds we actually emit.
fn declaration_label(language: SupportLang, node: Node<'_>, source: &str) -> Option<&'static str> {
	let kind = node.kind();
	use SupportLang::*;
	match language {
		Rust => match kind {
			"function_item" | "function_signature_item" => Some("fn"),
			"struct_item" => Some("struct"),
			"enum_item" => Some("enum"),
			"union_item" => Some("union"),
			"trait_item" => Some("trait"),
			"impl_item" => Some("impl"),
			"mod_item" => Some("mod"),
			"const_item" => Some("const"),
			"static_item" => Some("static"),
			"type_item" | "associated_type" => Some("type"),
			"macro_definition" => Some("macro"),
			_ => None,
		},
		TypeScript | Tsx => match kind {
			"function_declaration" | "generator_function_declaration" | "function_signature" => {
				Some("fn")
			},
			"class_declaration" | "abstract_class_declaration" => Some("class"),
			"method_definition" | "method_signature" | "abstract_method_signature" => {
				Some("method")
			},
			"interface_declaration" => Some("interface"),
			"type_alias_declaration" => Some("type"),
			"enum_declaration" => Some("enum"),
			"module" | "internal_module" | "namespace_declaration" => Some("module"),
			_ => None,
		},
		JavaScript => match kind {
			"function_declaration" | "generator_function_declaration" => Some("fn"),
			"class_declaration" => Some("class"),
			"method_definition" => Some("method"),
			_ => None,
		},
		Python => match kind {
			"function_definition" => Some("fn"),
			"class_definition" => Some("class"),
			_ => None,
		},
		Go => match kind {
			"function_declaration" => Some("fn"),
			"method_declaration" => Some("method"),
			"type_spec" => Some("type"),
			"const_spec" => Some("const"),
			"var_spec" => Some("var"),
			_ => None,
		},
		Java => match kind {
			"class_declaration" => Some("class"),
			"interface_declaration" | "annotation_type_declaration" => Some("interface"),
			"enum_declaration" => Some("enum"),
			"record_declaration" => Some("record"),
			"method_declaration" => Some("method"),
			"constructor_declaration" | "compact_constructor_declaration" => Some("constructor"),
			_ => None,
		},
		C => match kind {
			"function_definition" => Some("fn"),
			"struct_specifier" => Some("struct"),
			"union_specifier" => Some("union"),
			"enum_specifier" => Some("enum"),
			_ => None,
		},
		Cpp => match kind {
			"function_definition" => Some("fn"),
			"class_specifier" => Some("class"),
			"struct_specifier" => Some("struct"),
			"union_specifier" => Some("union"),
			"enum_specifier" => Some("enum"),
			"namespace_definition" => Some("namespace"),
			"alias_declaration" => Some("alias"),
			"concept_definition" => Some("concept"),
			_ => None,
		},
		CSharp => match kind {
			"class_declaration" => Some("class"),
			"interface_declaration" => Some("interface"),
			"struct_declaration" => Some("struct"),
			"enum_declaration" => Some("enum"),
			"record_declaration" => Some("record"),
			"namespace_declaration" | "file_scoped_namespace_declaration" => Some("namespace"),
			"method_declaration" => Some("method"),
			"constructor_declaration" => Some("constructor"),
			"property_declaration" => Some("property"),
			"delegate_declaration" => Some("delegate"),
			"event_declaration" => Some("event"),
			_ => None,
		},
		Ruby => match kind {
			"class" => Some("class"),
			"module" => Some("module"),
			"method" | "singleton_method" => Some("method"),
			_ => None,
		},
		Php => match kind {
			"class_declaration" => Some("class"),
			"interface_declaration" => Some("interface"),
			"trait_declaration" => Some("trait"),
			"enum_declaration" => Some("enum"),
			"function_definition" => Some("fn"),
			"method_declaration" => Some("method"),
			"namespace_definition" => Some("namespace"),
			_ => None,
		},
		Swift => match kind {
			"class_declaration" => Some(swift_type_label(node, source)),
			"function_declaration" => Some("fn"),
			"init_declaration" => Some("init"),
			"protocol_declaration" => Some("protocol"),
			"protocol_function_declaration" => Some("fn"),
			"property_declaration" => Some("property"),
			"typealias_declaration" => Some("typealias"),
			"associatedtype_declaration" => Some("associatedtype"),
			_ => None,
		},
		Kotlin => match kind {
			"class_declaration" => Some("class"),
			"object_declaration" => Some("object"),
			"function_declaration" => Some("fn"),
			"property_declaration" => Some("property"),
			"type_alias" => Some("type"),
			_ => None,
		},
		Scala => match kind {
			"class_definition" => Some("class"),
			"object_definition" => Some("object"),
			"trait_definition" => Some("trait"),
			"enum_definition" => Some("enum"),
			"function_definition" => Some("fn"),
			"type_definition" => Some("type"),
			_ => None,
		},
		Lua => match kind {
			"function_declaration" => Some("fn"),
			_ => None,
		},
		Bash => match kind {
			"function_definition" => Some("fn"),
			_ => None,
		},
		Zig => match kind {
			"function_declaration" => Some("fn"),
			_ => None,
		},
		Dart => match kind {
			"class_declaration" => Some("class"),
			"mixin_declaration" => Some("mixin"),
			"extension_declaration" | "extension_type_declaration" => Some("extension"),
			"enum_declaration" => Some("enum"),
			"function_declaration" => Some("fn"),
			"method_declaration" => Some("method"),
			"constructor_signature" => Some("constructor"),
			_ => None,
		},
		Solidity => match kind {
			"contract_declaration" => Some("contract"),
			"library_declaration" => Some("library"),
			"interface_declaration" => Some("interface"),
			"struct_declaration" => Some("struct"),
			"enum_declaration" => Some("enum"),
			"function_definition" => Some("fn"),
			"modifier_definition" => Some("modifier"),
			"event_definition" => Some("event"),
			"error_declaration" => Some("error"),
			"user_defined_type_definition" => Some("type"),
			_ => None,
		},
		_ => None,
	}
}

fn has_map_rules(language: SupportLang) -> bool {
	use SupportLang::*;
	matches!(
		language,
		Rust | TypeScript
			| Tsx | JavaScript
			| Python | Go
			| Java | C
			| Cpp | CSharp
			| Ruby | Php
			| Swift | Kotlin
			| Scala | Lua
			| Bash | Zig
			| Dart | Solidity
	)
}

/// Container declarations expose nested declarations; function bodies do not.
fn is_container_label(label: &str) -> bool {
	matches!(
		label,
		"class"
			| "struct"
			| "union"
			| "enum"
			| "trait"
			| "impl"
			| "mod"
			| "module"
			| "namespace"
			| "interface"
			| "object"
			| "protocol"
			| "extension"
			| "contract"
			| "library"
			| "record"
			| "mixin"
			| "actor"
	)
}

/// Swift models class/struct/enum/extension/actor via a `declaration_kind`
/// token on `class_declaration`; report the token instead of guessing "class".
fn swift_type_label(node: Node<'_>, source: &str) -> &'static str {
	let kind = node
		.child_by_field_name("declaration_kind")
		.and_then(|kind| kind.utf8_text(source.as_bytes()).ok())
		.map(str::trim);
	match kind {
		Some("class") => "class",
		Some("struct") => "struct",
		Some("enum") => "enum",
		Some("extension") => "extension",
		Some("actor") => "actor",
		_ => "type",
	}
}

/// Bodies that never define declarations visible in the map. Kind names are
/// grammar-specific (`class` is an anonymous class expression in JS/TS but a
/// real declaration in Ruby), so the check is scoped by language.
fn is_body_only_kind(language: SupportLang, kind: &str) -> bool {
	use SupportLang::*;
	match language {
		JavaScript | TypeScript | Tsx => matches!(
			kind,
			"function_expression"
				| "generator_function"
				| "arrow_function"
				| "object"
				| "class"
				| "class_static_block"
		),
		Rust => matches!(
			kind,
			"closure_expression" | "async_block" | "const_block" | "unsafe_block"
		),
		Python => kind == "lambda",
		Go => kind == "func_literal",
		Java | Cpp | Scala => kind == "lambda_expression",
		CSharp => matches!(kind, "lambda_expression" | "anonymous_method_expression"),
		Kotlin => matches!(kind, "anonymous_function" | "lambda_literal"),
		Swift => kind == "lambda_literal",
		Dart => kind == "function_expression",
		Php => matches!(kind, "anonymous_function" | "arrow_function"),
		_ => false,
	}
}

/// One declaration node can declare several names (`var a, b int` in Go).
fn declaration_names(node: Node<'_>, language: SupportLang, source: &str) -> Vec<String> {
	if language == SupportLang::Go && matches!(node.kind(), "const_spec" | "var_spec") {
		let mut cursor = node.walk();
		let names: Vec<String> = node
			.children_by_field_name("name", &mut cursor)
			.filter_map(|name| text_name(name, source))
			.collect();
		if !names.is_empty() {
			return names;
		}
	}
	declaration_name(node, language, source).into_iter().collect()
}

fn declaration_name(node: Node<'_>, language: SupportLang, source: &str) -> Option<String> {
	if let Some(name) = node.child_by_field_name("name") {
		if let Some(name) = text_name(name, source) {
			return Some(name);
		}
	}
	match language {
		SupportLang::Rust if node.kind() == "impl_item" => {
			node.child_by_field_name("type").and_then(|name| text_name(name, source))
		},
		SupportLang::C | SupportLang::Cpp if node.kind() == "function_definition" => {
			node.child_by_field_name("declarator")
				.and_then(|declarator| declarator_name(declarator, source))
		},
		_ => signature_name(node, source, 3),
	}
}

/// Dart wraps method names in `method_signature` -> `function_signature`, and
/// Kotlin has no `name` fields at all. Both resolve through this bounded
/// direct-child search, which only follows wrapper nodes and identifier nodes.
fn signature_name(node: Node<'_>, source: &str, depth: u8) -> Option<String> {
	if depth == 0 {
		return None;
	}
	if let Some(name) = node.child_by_field_name("name") {
		if let Some(name) = text_name(name, source) {
			return Some(name);
		}
	}
	let mut cursor = node.walk();
	for child in node.named_children(&mut cursor) {
		if let Some(name) = normalize_name(child, source) {
			return Some(name);
		}
		if is_signature_wrapper(child.kind()) {
			if let Some(name) = signature_name(child, source, depth - 1) {
				return Some(name);
			}
		}
	}
	None
}

fn declarator_name(node: Node<'_>, source: &str) -> Option<String> {
	if is_identifier_kind(node.kind()) {
		return normalize_name(node, source);
	}
	if matches!(node.kind(), "operator_name" | "destructor_name" | "qualified_identifier") {
		return text_name(node, source);
	}
	for field in ["declarator", "name"] {
		if let Some(child) = node.child_by_field_name(field) {
			if let Some(name) = declarator_name(child, source) {
				return Some(name);
			}
		}
	}
	let mut cursor = node.walk();
	for child in node.named_children(&mut cursor) {
		if is_declarator_wrapper(child.kind()) {
			if let Some(name) = declarator_name(child, source) {
				return Some(name);
			}
		}
	}
	None
}

fn normalize_name(node: Node<'_>, source: &str) -> Option<String> {
	if !is_identifier_kind(node.kind()) {
		return None;
	}
	text_name(node, source)
}

/// Read a node's text as a display name. Used only where the caller already
/// selected a name-bearing node (a `name` field, a container's type field, or
/// a declarator wrapper), so non-identifier syntax like `Foo<T>` is accepted.
fn text_name(node: Node<'_>, source: &str) -> Option<String> {
	let text = node.utf8_text(source.as_bytes()).ok()?;
	let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
	if collapsed.is_empty() || !collapsed.chars().any(|ch| ch.is_alphanumeric() || ch == '_' || ch == '$')
	{
		return None;
	}
	Some(collapsed.chars().take(MAX_NAME_CHARS).collect())
}

fn is_identifier_kind(kind: &str) -> bool {
	matches!(
		kind,
		"identifier"
			| "type_identifier"
			| "field_identifier"
			| "property_identifier"
			| "private_property_identifier"
			| "simple_identifier"
			| "namespace_identifier"
			| "operator_identifier"
			| "word"
			| "variable_name"
			| "constant"
			| "alias"
	)
}

fn is_signature_wrapper(kind: &str) -> bool {
	kind.ends_with("signature")
		|| matches!(
			kind,
			"method_signature"
				| "function_signature"
				| "constructor_signature"
				| "getter_signature"
				| "setter_signature"
				| "operator_signature"
				| "factory_constructor_signature"
				| "variable_declaration"
		)
}

fn is_declarator_wrapper(kind: &str) -> bool {
	matches!(
		kind,
		"pointer_declarator"
			| "function_declarator"
			| "reference_declarator"
			| "array_declarator"
			| "parenthesized_declarator"
			| "attributed_declarator"
			| "init_declarator"
			| "abstract_function_declarator"
			| "template_function"
			| "template_method"
			| "template_type"
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn map(code: &str, path: &str) -> MapResult {
		map_code(MapOptions {
			code:        code.to_string(),
			lang:        None,
			path:        Some(path.to_string()),
			max_symbols: None,
		})
		.expect("map parses")
	}

	#[test]
	fn rust_declarations_include_container_members_and_no_bodies() {
		let result = map(
			"pub struct Alpha;\nimpl Alpha {\n    pub fn beta(&self) -> u32 {\n        let hidden_body_marker = 1;\n        hidden_body_marker\n    }\n}\nfn gamma() {}\n",
			"src/lib.rs",
		);
		assert!(result.parsed);
		let found: Vec<(&str, &str, u32)> = result
			.symbols
			.iter()
			.map(|symbol| (symbol.kind.as_str(), symbol.name.as_str(), symbol.line))
			.collect();
		assert!(found.contains(&("struct", "Alpha", 1)), "{found:?}");
		assert!(found.contains(&("impl", "Alpha", 2)), "{found:?}");
		assert!(found.contains(&("fn", "beta", 3)), "{found:?}");
		assert!(found.contains(&("fn", "gamma", 8)), "{found:?}");
		assert!(!result.symbols.iter().any(|symbol| symbol.name.contains("hidden_body_marker")));
	}

	#[test]
	fn c_function_name_comes_from_the_declarator() {
		let result = map("int compute(int x) {\n  return x + 1;\n}\nstruct Point { int x; };\n", "a.c");
		let found: Vec<(&str, &str, u32)> = result
			.symbols
			.iter()
			.map(|symbol| (symbol.kind.as_str(), symbol.name.as_str(), symbol.line))
			.collect();
		assert!(found.contains(&("fn", "compute", 1)), "{found:?}");
		assert!(found.contains(&("struct", "Point", 4)), "{found:?}");
	}

	#[test]
	fn python_and_typescript_declarations_are_mapped() {
		let python = map("class Widget:\n    def render(self):\n        pass\n", "w.py");
		assert!(python.symbols.iter().any(|symbol| symbol.kind == "class" && symbol.name == "Widget"));
		assert!(python.symbols.iter().any(|symbol| symbol.kind == "fn" && symbol.name == "render"));

		let typescript = map(
			"interface Shape { area(): number; }\nclass Square implements Shape { area() { return 1; } }\nfunction make(): Square { return new Square(); }\n",
			"a.ts",
		);
		let names: Vec<&str> = typescript.symbols.iter().map(|symbol| symbol.name.as_str()).collect();
		assert!(names.contains(&"Shape"), "{names:?}");
		assert!(names.contains(&"Square"), "{names:?}");
		assert!(names.contains(&"make"), "{names:?}");
	}

	#[test]
	fn unlabeled_function_bodies_do_not_leak_declarations() {
		let typescript = map(
			"const f = () => { function leaked() {} };\nfunction top() {}\n",
			"a.ts",
		);
		let ts_names: Vec<&str> =
			typescript.symbols.iter().map(|symbol| symbol.name.as_str()).collect();
		assert!(ts_names.contains(&"top"), "{ts_names:?}");
		assert!(!ts_names.contains(&"leaked"), "{ts_names:?}");

		let cpp = map(
			"auto f = [](int) { struct Leaked {}; };\nstruct Top {};\n",
			"a.cpp",
		);
		let cpp_names: Vec<&str> = cpp.symbols.iter().map(|symbol| symbol.name.as_str()).collect();
		assert!(cpp_names.contains(&"Top"), "{cpp_names:?}");
		assert!(!cpp_names.contains(&"Leaked"), "{cpp_names:?}");
	}

	#[test]
	fn object_and_anonymous_class_members_do_not_leak() {
		let result = map(
			"const obj = { objMethod() { function leak1() {} } };\nconst C = class { classMethod() { function leak2() {} } };\nclass D { static { function leak3() {} } dMethod() {} }\n",
			"a.js",
		);
		let names: Vec<&str> = result.symbols.iter().map(|symbol| symbol.name.as_str()).collect();
		assert!(names.contains(&"D"), "{names:?}");
		assert!(names.contains(&"dMethod"), "{names:?}");
		for absent in ["objMethod", "classMethod", "leak1", "leak2", "leak3"] {
			assert!(!names.contains(&absent), "{absent} leaked: {names:?}");
		}
	}

	#[test]
	fn rust_async_and_const_blocks_do_not_leak() {
		let result = map(
			"enum E {\n    A = const { fn leaked_const() {} 1 },\n    B = { let _ = async { fn leaked_async() {} }; 1 },\n}\nfn top() {}\n",
			"a.rs",
		);
		let names: Vec<&str> = result.symbols.iter().map(|symbol| symbol.name.as_str()).collect();
		assert!(names.contains(&"E"), "{names:?}");
		assert!(names.contains(&"top"), "{names:?}");
		assert!(!names.contains(&"leaked_const"), "{names:?}");
		assert!(!names.contains(&"leaked_async"), "{names:?}");
	}

	#[test]
	fn ruby_classes_are_not_skipped_as_anonymous_js_classes() {
		let result = map("class Widget\n  def render\n  end\nend\n", "a.rb");
		let names: Vec<&str> = result.symbols.iter().map(|symbol| symbol.name.as_str()).collect();
		assert!(names.contains(&"Widget"), "{names:?}");
		assert!(names.contains(&"render"), "{names:?}");
	}

	#[test]
	fn go_multi_name_var_and_const_specs_keep_every_name() {
		let result = map("package p\nvar a, b int\nconst c, d = 1, 2\n", "a.go");
		let found: Vec<(&str, &str)> = result
			.symbols
			.iter()
			.map(|symbol| (symbol.kind.as_str(), symbol.name.as_str()))
			.collect();
		assert!(found.contains(&("var", "a")), "{found:?}");
		assert!(found.contains(&("var", "b")), "{found:?}");
		assert!(found.contains(&("const", "c")), "{found:?}");
		assert!(found.contains(&("const", "d")), "{found:?}");
	}

	#[test]
	fn swift_type_declarations_use_the_real_keyword() {
		let result = map("struct S {}\nenum E {}\n", "a.swift");
		let found: Vec<(&str, &str)> = result
			.symbols
			.iter()
			.map(|symbol| (symbol.kind.as_str(), symbol.name.as_str()))
			.collect();
		assert!(found.contains(&("struct", "S")), "{found:?}");
		assert!(found.contains(&("enum", "E")), "{found:?}");
		assert!(!found.iter().any(|(kind, _)| *kind == "class"), "{found:?}");
	}

	#[test]
	fn per_file_symbol_ceiling_marks_truncation() {
		let result = map_code(MapOptions {
			code:        "fn a() {}\nfn b() {}\nfn c() {}\n".to_string(),
			lang:        None,
			path:        Some("a.rs".to_string()),
			max_symbols: Some(1),
		})
		.expect("map parses");
		assert_eq!(result.symbols.len(), 1);
		assert!(result.truncated);
	}

	#[test]
	fn unsupported_language_returns_no_language_and_no_symbols() {
		let result = map("# Title\n", "notes.md");
		assert_eq!(result.language, None);
		assert!(result.symbols.is_empty());
		assert!(!result.parsed);
	}
}
