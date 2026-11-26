//! Declaration map and Aider-style ranked repo map.
//!
//! The syntax-only half reports the declarations tree-sitter can see (kind,
//! name, 1-based start line) without bodies. The ranked half adds
//! `tree-sitter-tags` definition/reference facts, name-based reference counts,
//! and a small personalized PageRank over the file graph; it still performs no
//! type or call resolution.
//!
//! Mapped languages: Rust, TypeScript/TSX/JavaScript, Python, Go, Java, C/C++,
//! C#, Ruby, PHP, Swift, Kotlin, Scala, Lua, Bash, Zig, Dart, and Solidity.
//! Tag queries additionally cover Elixir, Fortran, Nix, OCaml, R, and Emacs
//! Lisp; languages without either source fall back to the walker's skip count.

use std::{
	collections::{BTreeMap, BTreeSet},
	path::Path,
	sync::LazyLock,
};

use anyhow::Result;
use ast_grep_core::tree_sitter::LanguageExt;
use serde::{Deserialize, Serialize};
use tree_sitter::Node;
use tree_sitter_tags::{TagsConfiguration, TagsContext};

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
	pub code: String,
	/// Language alias (e.g. "rust", "typescript"); path inference is used when
	/// omitted.
	pub lang: Option<String>,
	/// File path used to infer language by extension when `lang` is omitted.
	pub path: Option<String>,
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
	pub language: Option<String>,
	/// False for unparseable sources or tree-sitter syntax errors. Symbols
	/// collected before the error are still returned, so the caller can mark
	/// the file as incomplete rather than silently dropping it.
	pub parsed: bool,
	/// Declarations in source order. Empty when the language has no map rule.
	pub symbols: Vec<MapSymbol>,
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
		.or_else(|| {
			options
				.path
				.as_deref()
				.and_then(|path| SupportLang::from_path(Path::new(path)))
		})
		.filter(|language| has_map_rules(*language));
	let Some(language) = language else {
		return Ok(MapResult {
			language: None,
			parsed: false,
			symbols: Vec::new(),
			truncated: false,
		});
	};
	if source.is_empty() {
		return Ok(MapResult {
			language: Some(language.canonical_name().to_string()),
			parsed: true,
			symbols: Vec::new(),
			truncated: false,
		});
	}

	let Some(tree) = parse_cached(&source, language)? else {
		return Ok(MapResult {
			language: Some(language.canonical_name().to_string()),
			parsed: false,
			symbols: Vec::new(),
			truncated: false,
		});
	};

	let max_symbols = options
		.max_symbols
		.unwrap_or(MAP_MAX_SYMBOLS_PER_FILE)
		.max(1);
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
			"method_definition" | "method_signature" | "abstract_method_signature" => Some("method"),
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
		Rust
			| TypeScript
			| Tsx | JavaScript
			| Python
			| Go | Java
			| C | Cpp
			| CSharp
			| Ruby | Php
			| Swift
			| Kotlin
			| Scala
			| Lua | Bash
			| Zig | Dart
			| Solidity
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
		Rust => matches!(kind, "closure_expression" | "async_block" | "const_block" | "unsafe_block"),
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
	declaration_name(node, language, source)
		.into_iter()
		.collect()
}

fn declaration_name(node: Node<'_>, language: SupportLang, source: &str) -> Option<String> {
	if let Some(name) = node.child_by_field_name("name") {
		if let Some(name) = text_name(name, source) {
			return Some(name);
		}
	}
	match language {
		SupportLang::Rust if node.kind() == "impl_item" => node
			.child_by_field_name("type")
			.and_then(|name| text_name(name, source)),
		SupportLang::C | SupportLang::Cpp if node.kind() == "function_definition" => node
			.child_by_field_name("declarator")
			.and_then(|declarator| declarator_name(declarator, source)),
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
	if collapsed.is_empty()
		|| !collapsed
			.chars()
			.any(|ch| ch.is_alphanumeric() || ch == '_' || ch == '$')
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

// ── Aider-style repo map ────────────────────────────────────────────────
//
// Port of the ranking half of Aider's `repomap.py` (Apache-2.0): definition
// and reference tags come from `tree-sitter-tags`, weights follow Aider's
// identifier heuristics, and files are ranked by a small personalized
// power-iteration PageRank. Names only — no type or call resolution.

/// One symbol in a ranked repo map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedSymbol {
	pub kind: String,
	pub name: String,
	pub line: u32,
	/// Name-based references across the mapped files.
	pub refs: u32,
}

/// One file in a ranked repo map.
#[derive(Debug, Clone, PartialEq)]
pub struct RankedFile {
	pub path: String,
	pub parsed: bool,
	/// Personalized PageRank score; files are sorted by it (ties by path).
	pub score: f64,
	pub symbols: Vec<RankedSymbol>,
	/// True when tree-sitter tags produced the symbols; false means the
	/// declaration-map fallback did.
	pub used_tags: bool,
}

/// Ranked files plus deterministic truncation flags.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RepoMapRanking {
	pub files: Vec<RankedFile>,
	pub symbol_truncated: bool,
	pub per_file_truncated: bool,
}

/// Options for [`RepoMapBuilder`].
#[derive(Debug, Clone)]
pub struct RepoMapOptions {
	/// Global symbol ceiling.
	pub max_symbols: usize,
	/// Per-file symbol ceiling.
	pub per_file_symbols: usize,
	/// Files in the conversation; boosts their incoming rank.
	pub chat_files: Vec<String>,
	/// Identifiers mentioned in the conversation; boosts their edges.
	pub mentioned_idents: Vec<String>,
}

impl Default for RepoMapOptions {
	fn default() -> Self {
		Self {
			max_symbols: 2000,
			per_file_symbols: MAP_MAX_SYMBOLS_PER_FILE,
			chat_files: Vec::new(),
			mentioned_idents: Vec::new(),
		}
	}
}

struct FileFacts {
	path: String,
	parsed: bool,
	used_tags: bool,
	defs: Vec<DefFacts>,
}

struct DefFacts {
	kind: String,
	name: String,
	line: u32,
}

/// Incremental repo-map index. Feed files one at a time so source text can be
/// dropped immediately; [`RepoMapBuilder::finish`] runs PageRank.
pub struct RepoMapBuilder {
	options: RepoMapOptions,
	files: Vec<FileFacts>,
	defines: BTreeMap<String, Vec<usize>>,
	references: BTreeMap<String, Vec<(usize, u32)>>,
	per_file_truncated: bool,
}

impl RepoMapBuilder {
	pub fn new(options: RepoMapOptions) -> Self {
		Self {
			options,
			files: Vec::new(),
			defines: BTreeMap::new(),
			references: BTreeMap::new(),
			per_file_truncated: false,
		}
	}

	/// Index one file. Tag extraction failures fall back to the syntax-only
	/// declaration map; languages without either produce a parsed-but-empty
	/// entry.
	pub fn add(&mut self, path: &str, code: &str, language: SupportLang) {
		let per_file = self.options.per_file_symbols.max(1);
		let (mut defs, refs, parsed, used_tags) = match tag_facts(code, language) {
			Some((defs, refs, parsed)) => (defs, refs, parsed, true),
			None => {
				let result = map_code(MapOptions {
					code: code.to_string(),
					lang: Some(language.canonical_name().to_string()),
					path: None,
					max_symbols: Some(per_file),
				})
				.ok();
				let Some(result) = result else {
					return;
				};
				if result.truncated {
					self.per_file_truncated = true;
				}
				(
					result
						.symbols
						.into_iter()
						.map(|symbol| DefFacts {
							kind: symbol.kind,
							name: symbol.name,
							line: symbol.line,
						})
						.collect(),
					BTreeMap::new(),
					result.parsed,
					false,
				)
			},
		};

		defs.sort_by(|a, b| a.line.cmp(&b.line).then_with(|| a.name.cmp(&b.name)));
		let mut seen = BTreeSet::new();
		defs.retain(|def| seen.insert(def.name.clone()));
		if defs.len() > per_file {
			defs.truncate(per_file);
			self.per_file_truncated = true;
		}

		let index = self.files.len();
		for def in &defs {
			self
				.defines
				.entry(def.name.clone())
				.or_default()
				.push(index);
		}
		for (name, count) in refs {
			self
				.references
				.entry(name)
				.or_default()
				.push((index, count));
		}
		self
			.files
			.push(FileFacts { path: path.to_string(), parsed, used_tags, defs });
	}

	/// Rank files with personalized PageRank and pick symbols by reference
	/// count. Deterministic: score desc, path asc; refs desc, line asc, name asc.
	#[must_use]
	pub fn finish(self) -> RepoMapRanking {
		let count = self.files.len();
		if count == 0 {
			return RepoMapRanking::default();
		}
		let mut edges: BTreeMap<(usize, usize), f64> = BTreeMap::new();
		let chat: BTreeSet<&str> = self.options.chat_files.iter().map(String::as_str).collect();
		let mentioned: BTreeSet<&str> = self
			.options
			.mentioned_idents
			.iter()
			.map(String::as_str)
			.collect();
		for (ident, def_files) in &self.defines {
			if ident.chars().count() < 3 {
				continue;
			}
			let mut multiplier = 1.0f64;
			if mentioned.contains(ident.as_str()) {
				multiplier *= 10.0;
			}
			let long_name = ident.chars().count() >= 8
				&& (ident.contains('_')
					|| ident.contains('-')
					|| ident.chars().any(|ch| ch.is_ascii_uppercase()));
			if long_name {
				multiplier *= 10.0;
			}
			if ident.starts_with('_') {
				multiplier *= 0.1;
			}
			if def_files.len() > 5 {
				multiplier *= 0.1;
			}
			let refs = self.references.get(ident);
			for &def_file in def_files {
				let mut had_reference = false;
				if let Some(refs) = refs {
					for &(ref_file, num_refs) in refs {
						let mut weight = multiplier * f64::from(num_refs).sqrt();
						if chat.contains(self.files[ref_file].path.as_str()) {
							weight *= 50.0;
						}
						*edges.entry((ref_file, def_file)).or_insert(0.0) += weight;
						had_reference = true;
					}
				}
				if !had_reference {
					*edges.entry((def_file, def_file)).or_insert(0.0) += 0.1;
				}
			}
		}
		let personalization = {
			let base = 100.0 / count as f64;
			let mut values = vec![base; count];
			for (index, file) in self.files.iter().enumerate() {
				if chat.contains(file.path.as_str()) {
					values[index] += 100.0;
				}
			}
			values
		};
		let edge_list: Vec<(usize, usize, f64)> = edges
			.into_iter()
			.map(|((from, to), weight)| (from, to, weight))
			.collect();
		let scores = pagerank(count, &edge_list, &personalization, 0.85, 40);

		let mut order: Vec<usize> = (0..count).collect();
		order.sort_by(|&a, &b| {
			scores[b]
				.partial_cmp(&scores[a])
				.unwrap_or(std::cmp::Ordering::Equal)
				.then_with(|| self.files[a].path.cmp(&self.files[b].path))
		});

		let max_symbols = self.options.max_symbols.max(1);
		let mut emitted = 0usize;
		let mut symbol_truncated = false;
		let mut files = Vec::with_capacity(count);
		for index in order {
			let facts = &self.files[index];
			let mut symbols: Vec<RankedSymbol> = facts
				.defs
				.iter()
				.map(|def| RankedSymbol {
					kind: def.kind.clone(),
					name: def.name.clone(),
					line: def.line,
					refs: self
						.references
						.get(&def.name)
						.map(|refs| refs.iter().map(|(_, count)| *count).sum())
						.unwrap_or(0),
				})
				.collect();
			symbols.sort_by(|a, b| {
				b.refs
					.cmp(&a.refs)
					.then_with(|| a.line.cmp(&b.line))
					.then_with(|| a.name.cmp(&b.name))
			});
			if emitted >= max_symbols && !symbols.is_empty() {
				symbol_truncated = true;
				break;
			}
			let remaining = max_symbols - emitted;
			if symbols.len() > remaining {
				symbols.truncate(remaining);
				symbol_truncated = true;
			}
			emitted += symbols.len();
			files.push(RankedFile {
				path: facts.path.clone(),
				parsed: facts.parsed,
				score: scores[index],
				symbols,
				used_tags: facts.used_tags,
			});
		}
		RepoMapRanking { files, symbol_truncated, per_file_truncated: self.per_file_truncated }
	}
}

/// ~20-line power iteration for personalized PageRank (damping 0.85).
fn pagerank(
	count: usize,
	edges: &[(usize, usize, f64)],
	personalization: &[f64],
	damping: f64,
	iterations: usize,
) -> Vec<f64> {
	if count == 0 {
		return Vec::new();
	}
	let sum: f64 = personalization.iter().sum();
	let preference: Vec<f64> = if sum > 0.0 {
		personalization.iter().map(|value| value / sum).collect()
	} else {
		vec![1.0 / count as f64; count]
	};
	let mut out_weight = vec![0.0f64; count];
	for &(from, _, weight) in edges {
		out_weight[from] += weight;
	}
	let mut rank = preference.clone();
	for _ in 0..iterations {
		let mut next = vec![0.0f64; count];
		let mut dangling = 0.0f64;
		for (index, weight) in out_weight.iter().enumerate() {
			if *weight == 0.0 {
				dangling += rank[index];
			}
		}
		for &(from, to, weight) in edges {
			if out_weight[from] > 0.0 {
				next[to] += rank[from] * weight / out_weight[from];
			}
		}
		for index in 0..count {
			next[index] = (1.0 - damping) * preference[index]
				+ damping * (next[index] + dangling * preference[index]);
		}
		let delta: f64 = next
			.iter()
			.zip(&rank)
			.map(|(new, old)| (new - old).abs())
			.sum();
		rank = next;
		if delta < 1.0e-10 {
			break;
		}
	}
	rank
}

/// Tag-supported languages. Every other language falls back to the
/// declaration map, and files with neither are skipped by the walker.
#[must_use]
pub fn has_tag_query(language: SupportLang) -> bool {
	use SupportLang::*;
	matches!(
		language,
		Rust
			| Python
			| TypeScript
			| Tsx | JavaScript
			| Go | C
			| Cpp | Java
			| CSharp
			| Ruby | Php
			| Swift
			| Kotlin
			| Scala
			| Lua | Dart
			| Solidity
			| Elixir
			| Fortran
			| Nix | Ocaml
			| R | EmacsLisp
	)
}

/// Language support for the directory repo map: tags first, declaration-map
/// fallback second.
#[must_use]
pub fn repo_map_supported_path(path: &Path) -> Option<SupportLang> {
	SupportLang::from_path(path).filter(|language| {
		language.is_available() && (has_tag_query(*language) || has_map_rules(*language))
	})
}

macro_rules! tag_config {
	($language:ident, $query:literal) => {{
		static CONFIG: LazyLock<Option<TagsConfiguration>> = LazyLock::new(|| {
			TagsConfiguration::new(SupportLang::$language.get_ts_language(), include_str!($query), "")
				.ok()
		});
		CONFIG.as_ref()
	}};
}

fn tags_config(language: SupportLang) -> Option<&'static TagsConfiguration> {
	use SupportLang::*;
	if !language.is_available() {
		return None;
	}
	match language {
		Rust => tag_config!(Rust, "../queries/tags/rust.scm"),
		Python => tag_config!(Python, "../queries/tags/python.scm"),
		TypeScript => tag_config!(TypeScript, "../queries/tags/typescript.scm"),
		Tsx => tag_config!(Tsx, "../queries/tags/tsx.scm"),
		JavaScript => tag_config!(JavaScript, "../queries/tags/javascript.scm"),
		Go => tag_config!(Go, "../queries/tags/go.scm"),
		C => tag_config!(C, "../queries/tags/c.scm"),
		Cpp => tag_config!(Cpp, "../queries/tags/cpp.scm"),
		Java => tag_config!(Java, "../queries/tags/java.scm"),
		CSharp => tag_config!(CSharp, "../queries/tags/c_sharp.scm"),
		Ruby => tag_config!(Ruby, "../queries/tags/ruby.scm"),
		Php => tag_config!(Php, "../queries/tags/php.scm"),
		Swift => tag_config!(Swift, "../queries/tags/swift.scm"),
		Kotlin => tag_config!(Kotlin, "../queries/tags/kotlin.scm"),
		Scala => tag_config!(Scala, "../queries/tags/scala.scm"),
		Lua => tag_config!(Lua, "../queries/tags/lua.scm"),
		Dart => tag_config!(Dart, "../queries/tags/dart.scm"),
		Solidity => tag_config!(Solidity, "../queries/tags/solidity.scm"),
		Elixir => tag_config!(Elixir, "../queries/tags/elixir.scm"),
		Fortran => tag_config!(Fortran, "../queries/tags/fortran.scm"),
		Nix => tag_config!(Nix, "../queries/tags/nix.scm"),
		Ocaml => tag_config!(Ocaml, "../queries/tags/ocaml.scm"),
		R => tag_config!(R, "../queries/tags/r.scm"),
		EmacsLisp => tag_config!(EmacsLisp, "../queries/tags/elisp.scm"),
		_ => None,
	}
}

fn tag_facts(
	code: &str,
	language: SupportLang,
) -> Option<(Vec<DefFacts>, BTreeMap<String, u32>, bool)> {
	let config = tags_config(language)?;
	let mut context = TagsContext::new();
	let (tags, has_error) = context.generate_tags(config, code.as_bytes(), None).ok()?;
	let mut defs = Vec::new();
	let mut refs: BTreeMap<String, u32> = BTreeMap::new();
	for tag in tags {
		let tag = tag.ok()?;
		let raw = code.get(tag.name_range.clone())?;
		let Some(name) = clean_tag_name(raw) else {
			continue;
		};
		if tag.is_definition {
			defs.push(DefFacts {
				kind: config.syntax_type_name(tag.syntax_type_id).to_string(),
				name,
				line: (tag.span.start.row as u32).saturating_add(1),
			});
		} else {
			*refs.entry(name).or_insert(0) += 1;
		}
	}
	Some((defs, refs, !has_error))
}

fn clean_tag_name(raw: &str) -> Option<String> {
	let joined = raw.split_whitespace().collect::<Vec<_>>().join(" ");
	if joined.is_empty() {
		None
	} else {
		Some(joined.chars().take(MAX_NAME_CHARS).collect())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn map(code: &str, path: &str) -> MapResult {
		map_code(MapOptions {
			code: code.to_string(),
			lang: None,
			path: Some(path.to_string()),
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
		assert!(
			!result
				.symbols
				.iter()
				.any(|symbol| symbol.name.contains("hidden_body_marker"))
		);
	}

	#[test]
	fn c_function_name_comes_from_the_declarator() {
		let result =
			map("int compute(int x) {\n  return x + 1;\n}\nstruct Point { int x; };\n", "a.c");
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
		assert!(
			python
				.symbols
				.iter()
				.any(|symbol| symbol.kind == "class" && symbol.name == "Widget")
		);
		assert!(
			python
				.symbols
				.iter()
				.any(|symbol| symbol.kind == "fn" && symbol.name == "render")
		);

		let typescript = map(
			"interface Shape { area(): number; }\nclass Square implements Shape { area() { return 1; } }\nfunction make(): Square { return new Square(); }\n",
			"a.ts",
		);
		let names: Vec<&str> = typescript
			.symbols
			.iter()
			.map(|symbol| symbol.name.as_str())
			.collect();
		assert!(names.contains(&"Shape"), "{names:?}");
		assert!(names.contains(&"Square"), "{names:?}");
		assert!(names.contains(&"make"), "{names:?}");
	}

	#[test]
	fn unlabeled_function_bodies_do_not_leak_declarations() {
		let typescript =
			map("const f = () => { function leaked() {} };\nfunction top() {}\n", "a.ts");
		let ts_names: Vec<&str> = typescript
			.symbols
			.iter()
			.map(|symbol| symbol.name.as_str())
			.collect();
		assert!(ts_names.contains(&"top"), "{ts_names:?}");
		assert!(!ts_names.contains(&"leaked"), "{ts_names:?}");

		let cpp = map("auto f = [](int) { struct Leaked {}; };\nstruct Top {};\n", "a.cpp");
		let cpp_names: Vec<&str> = cpp
			.symbols
			.iter()
			.map(|symbol| symbol.name.as_str())
			.collect();
		assert!(cpp_names.contains(&"Top"), "{cpp_names:?}");
		assert!(!cpp_names.contains(&"Leaked"), "{cpp_names:?}");
	}

	#[test]
	fn object_and_anonymous_class_members_do_not_leak() {
		let result = map(
			"const obj = { objMethod() { function leak1() {} } };\nconst C = class { classMethod() { function leak2() {} } };\nclass D { static { function leak3() {} } dMethod() {} }\n",
			"a.js",
		);
		let names: Vec<&str> = result
			.symbols
			.iter()
			.map(|symbol| symbol.name.as_str())
			.collect();
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
		let names: Vec<&str> = result
			.symbols
			.iter()
			.map(|symbol| symbol.name.as_str())
			.collect();
		assert!(names.contains(&"E"), "{names:?}");
		assert!(names.contains(&"top"), "{names:?}");
		assert!(!names.contains(&"leaked_const"), "{names:?}");
		assert!(!names.contains(&"leaked_async"), "{names:?}");
	}

	#[cfg(feature = "full-grammars")]
	#[test]
	fn ruby_classes_are_not_skipped_as_anonymous_js_classes() {
		let result = map("class Widget\n  def render\n  end\nend\n", "a.rb");
		let names: Vec<&str> = result
			.symbols
			.iter()
			.map(|symbol| symbol.name.as_str())
			.collect();
		assert!(names.contains(&"Widget"), "{names:?}");
		assert!(names.contains(&"render"), "{names:?}");
	}

	#[test]
	fn repo_map_tags_capture_go_var_and_const() {
		let mut builder = RepoMapBuilder::new(RepoMapOptions::default());
		builder.add("a.go", "package p\n\nconst Answer = 42\nvar counter int\n", SupportLang::Go);
		let ranking = builder.finish();
		let file = &ranking.files[0];
		assert!(file.used_tags, "Go must use tag facts");
		assert!(file.parsed, "valid Go must parse without errors");
		let found: Vec<(&str, &str)> = file
			.symbols
			.iter()
			.map(|symbol| (symbol.kind.as_str(), symbol.name.as_str()))
			.collect();
		assert!(found.contains(&("constant", "Answer")), "{found:?}");
		assert!(found.contains(&("var", "counter")), "{found:?}");
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

	#[cfg(feature = "full-grammars")]
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
			code: "fn a() {}\nfn b() {}\nfn c() {}\n".to_string(),
			lang: None,
			path: Some("a.rs".to_string()),
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
