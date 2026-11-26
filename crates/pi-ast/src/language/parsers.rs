//! Tree-sitter parser functions for all supported languages.
//!
//! The twelve core grammars are always linked (`core-grammars`); every other
//! grammar is behind `full-grammars`. A compiled-out parser keeps its entry
//! point so `SupportLang`'s exhaustive dispatch still builds, and panics if
//! called — the language is never handed out by
//! [`SupportLang::from_path`](super::SupportLang::from_path) /
//! [`from_alias`](super::SupportLang::from_alias) in that build.

use ast_grep_core::tree_sitter::TSLanguage;

macro_rules! optional_grammar {
	($func:ident, $grammar:expr) => {
		#[cfg(feature = "full-grammars")]
		pub fn $func() -> TSLanguage {
			$grammar.into()
		}
		#[cfg(not(feature = "full-grammars"))]
		pub fn $func() -> TSLanguage {
			panic!(concat!(stringify!($func), " requires the `full-grammars` feature"));
		}
	};
}

// ── Core grammars ───────────────────────────────────────────────────────

pub fn language_bash() -> TSLanguage {
	tree_sitter_bash::LANGUAGE.into()
}
pub fn language_c() -> TSLanguage {
	tree_sitter_c::LANGUAGE.into()
}
pub fn language_cpp() -> TSLanguage {
	tree_sitter_cpp::LANGUAGE.into()
}
pub fn language_go() -> TSLanguage {
	tree_sitter_go::LANGUAGE.into()
}
pub fn language_javascript() -> TSLanguage {
	tree_sitter_javascript::LANGUAGE.into()
}
pub fn language_json() -> TSLanguage {
	tree_sitter_json::LANGUAGE.into()
}
pub fn language_markdown() -> TSLanguage {
	tree_sitter_md::LANGUAGE.into()
}
pub fn language_python() -> TSLanguage {
	tree_sitter_python::LANGUAGE.into()
}
pub fn language_rust() -> TSLanguage {
	tree_sitter_rust::LANGUAGE.into()
}
pub fn language_toml() -> TSLanguage {
	tree_sitter_toml_ng::LANGUAGE.into()
}
pub fn language_tsx() -> TSLanguage {
	tree_sitter_typescript::LANGUAGE_TSX.into()
}
pub fn language_typescript() -> TSLanguage {
	tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into()
}
pub fn language_yaml() -> TSLanguage {
	tree_sitter_yaml::LANGUAGE.into()
}

// ── Full-grammar set ────────────────────────────────────────────────────

optional_grammar!(language_astro, tree_sitter_astro::LANGUAGE);
optional_grammar!(language_clojure, tree_sitter_clojure::LANGUAGE);
optional_grammar!(language_cmake, tree_sitter_cmake::LANGUAGE);
optional_grammar!(language_c_sharp, tree_sitter_c_sharp::LANGUAGE);
optional_grammar!(language_css, tree_sitter_css::LANGUAGE);
optional_grammar!(language_dart, tree_sitter_dart::LANGUAGE);
optional_grammar!(language_diff, tree_sitter_diff::LANGUAGE);
optional_grammar!(language_dockerfile, tree_sitter_dockerfile::language());
optional_grammar!(language_elixir, tree_sitter_elixir::LANGUAGE);
optional_grammar!(language_elisp, tree_sitter_elisp::LANGUAGE);
optional_grammar!(language_erlang, tree_sitter_erlang::LANGUAGE);
optional_grammar!(language_fortran, tree_sitter_fortran::LANGUAGE);
optional_grammar!(language_graphql, tree_sitter_graphql::LANGUAGE);
optional_grammar!(language_haskell, tree_sitter_haskell::LANGUAGE);
optional_grammar!(language_hcl, tree_sitter_hcl::LANGUAGE);
optional_grammar!(language_html, tree_sitter_html::LANGUAGE);
optional_grammar!(language_ini, tree_sitter_ini::LANGUAGE);
optional_grammar!(language_java, tree_sitter_java::LANGUAGE);
optional_grammar!(language_julia, tree_sitter_julia::LANGUAGE);
optional_grammar!(language_just, tree_sitter_just::LANGUAGE);
optional_grammar!(language_kotlin, tree_sitter_kotlin::LANGUAGE);
optional_grammar!(language_lua, tree_sitter_lua::LANGUAGE);
optional_grammar!(language_make, tree_sitter_make::LANGUAGE);
optional_grammar!(language_nix, tree_sitter_nix::LANGUAGE);
optional_grammar!(language_objc, tree_sitter_objc::LANGUAGE);
optional_grammar!(language_ocaml, tree_sitter_ocaml::LANGUAGE_OCAML);
optional_grammar!(language_odin, tree_sitter_odin::LANGUAGE);
optional_grammar!(language_php, tree_sitter_php::LANGUAGE_PHP_ONLY);
optional_grammar!(language_powershell, tree_sitter_powershell::LANGUAGE);
optional_grammar!(language_proto, tree_sitter_proto::LANGUAGE);
optional_grammar!(language_r, tree_sitter_r::LANGUAGE);
optional_grammar!(language_regex, tree_sitter_regex::LANGUAGE);
optional_grammar!(language_ruby, tree_sitter_ruby::LANGUAGE);
optional_grammar!(language_scala, tree_sitter_scala::LANGUAGE);
optional_grammar!(language_solidity, tree_sitter_solidity::LANGUAGE);
optional_grammar!(language_sql, tree_sitter_sql::LANGUAGE);
optional_grammar!(language_starlark, tree_sitter_starlark::LANGUAGE);
optional_grammar!(language_svelte, tree_sitter_svelte::LANGUAGE);
optional_grammar!(language_swift, tree_sitter_swift::LANGUAGE);
optional_grammar!(language_tlaplus, tree_sitter_tlaplus::LANGUAGE);
optional_grammar!(language_verilog, tree_sitter_verilog::LANGUAGE);
optional_grammar!(language_vue, tree_sitter_vue::LANGUAGE);
optional_grammar!(language_xml, tree_sitter_xml::LANGUAGE_XML);
optional_grammar!(language_zig, tree_sitter_zig::LANGUAGE);
