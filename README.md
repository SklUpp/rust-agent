# rust-agent (standalone baseline)

Standalone Rust workspace for the coding-agent runtime: one Linux/Termux
(Bionic, arm64) binary (`omp`) plus embedded text assets. This tree is an
extraction baseline, not a finished product; it has not been built on this
device yet.

## Provenance

- Donor working tree: `/data/data/com.termux/files/home/omp-work`
  (`feat/rust-agent-termux-bionic`), HEAD
  `3008cfd0ca88bb0e28323561a165f39efa7159e1` (SklUpp/omp-rust-termux fork).
- Upstream original: can1357/oh-my-pi
  `035e4f95c314f9aff2f5a779fa335422680f8e36` (see `docs/`).
- Extracted from the donor working tree at that commit, not from a Git
  worktree. The donor is read-only reference material and is not linked to this
  repository.
- Upstream copyright and license are unchanged (`LICENSE`, MIT). This project
  does not relicense the extracted code or claim authorship of it.
  Third-party attributions are in `THIRD-PARTY-NOTICES.txt` and, only where
  the donor has them, crate-local files: `crates/pi-ast/LICENSE`,
  `crates/pi-builtins/LICENSE` + `LICENSE-MIT`, `crates/pi-iso/LICENSE`,
  `crates/pi-shell/LICENSE` + `NOTICE`, `crates/pi-walker/LICENSE`,
  `crates/vendor/brush-core/LICENSE`. `packages/coding-agent/LICENSE` covers
  the prompt assets. `omp-cli`, `pi-agent`, and `pi-grep-pattern` have no
  crate-local license file and rely on the root `LICENSE`.

## Extracted closure

Workspace members: `crates/omp-cli`, `crates/pi-agent`, `crates/pi-ast`,
`crates/pi-builtins`, `crates/pi-grep-pattern`, `crates/pi-iso`,
`crates/pi-shell`, `crates/pi-walker`, `crates/vendor/brush-core`.

Only the compile-time closure of `omp-cli` and its local path dependencies is
present: Rust sources, tests, build script, embedded assets, licenses, root
`Cargo.lock`, `rustfmt.toml`, `rust-toolchain.toml`. `pi-vcs`, `pi-voice`, and
`pi-natives` (NAPI) are not part of this closure. TypeScript sources,
`node_modules`, `target/`, Bazel files, Android assets, and the donor Git
metadata were not copied.

Assets referenced by `include_str!` live at the same workspace-relative paths as
in the donor: `packages/coding-agent/src/{prompts,discovery/builtin-rules}` and
`packages/coding-agent/src/priority.json`.

## Commands

```sh
# Manifest-only check; no network, no compilation.
cargo metadata --offline --no-deps --format-version 1

# Build (not run in this baseline). Requires the crates in Cargo.lock to be
# available locally; `rust-toolchain.toml` pins nightly-2026-08-08 for the
# donor toolchain.
cargo build --release --offline
```

No claim is made that the build or tests pass until they are actually run.
