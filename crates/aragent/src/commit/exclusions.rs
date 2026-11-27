//! Lockfile exclusions for `arag commit` auto-staging.
//!
//! Ported from upstream `packages/coding-agent/src/commit/utils/exclusions.ts`.
//! Auto-staging must never sweep a generated lockfile into a commit: it churns
//! on every dependency resolution and buries the actual change. An explicit
//! `--files <path>` still wins, because that names the file on purpose.

/// Lockfile basenames that auto-staging always skips (case-insensitive).
pub const EXCLUDED_FILES: &[&str] = &[
	"Cargo.lock",
	"package-lock.json",
	"npm-shrinkwrap.json",
	"yarn.lock",
	"pnpm-lock.yaml",
	"shrinkwrap.yaml",
	"bun.lock",
	"bun.lockb",
	"deno.lock",
	"composer.lock",
	"Gemfile.lock",
	"poetry.lock",
	"Pipfile.lock",
	"pdm.lock",
	"uv.lock",
	"go.sum",
	"flake.lock",
	"pubspec.lock",
	"Podfile.lock",
	"Packages.resolved",
	"mix.lock",
	"packages.lock.json",
	"config.yml.lock",
	"config.yaml.lock",
	"settings.yml.lock",
	"settings.yaml.lock",
];

/// Generated lock suffixes that are excluded wherever they appear.
pub const EXCLUDED_SUFFIXES: &[&str] = &[".lock.yml", ".lock.yaml", "-lock.yml", "-lock.yaml"];

/// Whether `path` names a generated lockfile auto-staging must skip.
///
/// Matching is ASCII-case-insensitive: [`EXCLUDED_FILES`] compares against the
/// platform basename (so `crates/foo/Cargo.lock` is excluded but
/// `foo_Cargo.lock` is not, and a backslash is a separator only on Windows),
/// while [`EXCLUDED_SUFFIXES`] match anywhere.
pub fn is_excluded_file(path: &str) -> bool {
	let name = std::path::Path::new(path)
		.file_name()
		.and_then(|name| name.to_str());
	name.is_some_and(|name| {
		EXCLUDED_FILES
			.iter()
			.any(|candidate| name.eq_ignore_ascii_case(candidate))
	}) || EXCLUDED_SUFFIXES
		.iter()
		.any(|suffix| ends_with_ignore_ascii_case(path, suffix))
}

/// Drop excluded lockfiles from an owned file list, preserving order.
pub fn filter_excluded_files<I>(files: I) -> Vec<String>
where
	I: IntoIterator<Item = String>,
{
	files
		.into_iter()
		.filter(|file| !is_excluded_file(file))
		.collect()
}

/// Split `files` into `(stageable, excluded)` without cloning the survivors.
pub fn partition_excluded_files(files: Vec<String>) -> (Vec<String>, Vec<String>) {
	let mut stageable = Vec::new();
	let mut excluded = Vec::new();
	for file in files {
		if is_excluded_file(&file) {
			excluded.push(file);
		} else {
			stageable.push(file);
		}
	}
	(stageable, excluded)
}

fn ends_with_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
	haystack.len() >= needle.len()
		&& haystack.as_bytes()[haystack.len() - needle.len()..]
			.eq_ignore_ascii_case(needle.as_bytes())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn excluded_lockfile_names_match_case_insensitively() {
		assert!(is_excluded_file("Cargo.lock"));
		assert!(is_excluded_file("cargo.LOCK"));
		assert!(is_excluded_file("crates/aragent/Cargo.lock"));
		assert!(is_excluded_file("frontend/package-lock.json"));
		assert!(is_excluded_file("pnpm-lock.yaml"));
		assert!(is_excluded_file("bun.lock"));
		assert!(is_excluded_file("bun.lockb"));
		assert!(is_excluded_file("uv.lock"));
		assert!(is_excluded_file("go.sum"));
		assert!(is_excluded_file("settings.yaml.lock"));
		// Basename must match exactly, not merely end with a lockfile name.
		assert!(!is_excluded_file("foo_Cargo.lock"));
		assert!(!is_excluded_file("dir/foo_Cargo.lock"));
		assert!(!is_excluded_file("backup-Cargo.lock.bak"));
		assert!(!is_excluded_file("my-package-lock.json"));
		// On Unix a backslash is an ordinary filename byte, not a separator.
		#[cfg(unix)]
		assert!(!is_excluded_file("foo\\Cargo.lock"));
	}

	#[test]
	fn excluded_suffixes_cover_generated_lock_variants() {
		assert!(is_excluded_file("deploy.lock.yml"));
		assert!(is_excluded_file("deploy.LOCK.YAML"));
		assert!(is_excluded_file("services/api-lock.yml"));
		assert!(is_excluded_file("nested/dir/base-lock.yaml"));
		assert!(!is_excluded_file("deploy.lock.json"));
	}

	#[test]
	fn ordinary_sources_stay_stageable() {
		assert!(!is_excluded_file("src/main.rs"));
		assert!(!is_excluded_file("crates/aragent/src/commit/mod.rs"));
		assert!(!is_excluded_file("docs/commit.md"));
		assert!(!is_excluded_file("Cargo.toml"));
		// The needle must end at the path boundary, not mid-stem.
		assert!(!is_excluded_file("src/Cargo.lock.bak"));
	}

	#[test]
	fn filter_and_partition_agree() {
		let files = vec![
			"src/lib.rs".to_string(),
			"Cargo.lock".to_string(),
			"docs/commit.md".to_string(),
			"yarn.lock".to_string(),
		];
		let filtered = filter_excluded_files(files.clone());
		assert_eq!(filtered, vec!["src/lib.rs", "docs/commit.md"]);
		let (stageable, excluded) = partition_excluded_files(files);
		assert_eq!(stageable, filtered);
		assert_eq!(excluded, vec!["Cargo.lock", "yarn.lock"]);
	}
}
