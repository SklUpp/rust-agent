# aragent

Standalone Rust workspace for the coding-agent runtime: one Linux/Termux
(Bionic, arm64) binary (`arag`) plus embedded text assets. The project was
previously carried under the name `omp`; this tree renames the first-party
crates and CLI surface but keeps the existing data locations (`~/.omp`, XDG
`omp` roots, `OMP_*` variables, stored sessions and credentials) so upgrades do
not migrate or orphan user state. This tree is an extraction baseline, not a
finished product. Android release artifact at `d9c6078` was built in CI and its
CLI tool roundtrip passed on Termux.

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
  the prompt assets. `aragent`, `pi-agent`, and `pi-grep-pattern` have no
  crate-local license file and rely on the root `LICENSE`.

## Extracted closure

Workspace members: `crates/aragent`, `crates/pi-agent`, `crates/pi-ast`,
`crates/pi-builtins`, `crates/pi-grep-pattern`, `crates/pi-iso`,
`crates/pi-shell`, `crates/pi-walker`, `crates/vendor/brush-core`.

Only the compile-time closure of `aragent` and its local path dependencies is
present: Rust sources, tests, build script, embedded assets, licenses, root
`Cargo.lock`, `rustfmt.toml`, `rust-toolchain.toml`. `pi-vcs`, `pi-voice`, and
`pi-natives` (NAPI) are not part of this closure. TypeScript sources,
`node_modules`, `target/`, Bazel files, Android assets, and the donor Git
metadata were not copied.

Assets referenced by `include_str!` live at the same workspace-relative paths as
in the donor: `packages/coding-agent/src/{prompts,discovery/builtin-rules}` and
`packages/coding-agent/src/priority.json`.

## Diagnostics

`arag` writes a local, append-only SQLite trace to
`<agent dir>/diagnostics/diagnostics.db` (or the XDG data root variant after
`config init-xdg`), separate from `sessions.db`. It records session,
model-request, tool-call and subagent lifecycle boundaries with start/end status,
duration and provider-reported token counters.

- Enabled by default. Set `diagnostics.enabled: false` in `config.yml` or
  `.omp/settings.json` to stop new writes. A disabled run neither creates nor
  deletes the database; previously written rows remain. An implicit project
  setting cannot re-enable diagnostics that the host explicitly disabled; an
  operator `--config`/`PI_CONFIG_FILES` layer may.
- The database lives in a dedicated `diagnostics/` directory that must be owned
  by the current user with mode `0700`. Every ancestor owner must be root, the
  current user, or (on Android) the platform `system` uid `1000`.
- Non-sticky group/world-writable ancestors are rejected unless the writable
  principal is a proven Android platform identity (Android `system:system` or
  an Android app's own uid/gid pair) or the ancestor is sticky and owned by a
  trusted owner. GID `0` is not treated as exclusive. Symlinks and
  foreign-owned ancestors are rejected; nothing outside the diagnostics
  directory is chmodded.
- On non-Android systems a shared group-writable ancestor is treated as
  untrusted, so diagnostics stay off and the limitation is only the generic
  warning. On Android, paths owned by other non-system principals (for example
  some shared-storage mounts) are likewise unsupported by design.
- The database and its SQLite sidecars (`-wal`, `-shm`, `-journal`) are forced
  to owner-only mode (`0600`), including files that already existed. A symlink,
  non-regular file, or multiply-linked inode is rejected (one generic warning,
  no path or raw error).
- Rows live in the `spans` table: `span_id`, `parent_span_id`, `kind`,
  `status`, `at_ms`, `duration_ms`, `input_tokens`, `output_tokens`,
  `cache_read_tokens`, `cache_write_tokens`, `reasoning_tokens`.
- Token counters are nullable and presence-aware per provider parser: an
  omitted counter stores `NULL`, an explicit provider `0` stores `0`. The
  persisted/wire reasoning field keeps its prior positive-only filter; live
  diagnostics still record an explicit reasoning zero using parser presence.
- The trace stores only fixed enums, generated correlation ids and counters. It
  never stores prompt or tool content, arguments, file paths, URLs, raw session
  or subagent identifiers, or provider error strings.
- Writing runs on a bounded background queue; a failed or locked database emits
  one generic warning and never changes the agent's answer or exit code.

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

## Observed verification

- CI run `42240498060`: Linux `cargo check` and Android release build passed;
  host test compilation failed and was fixed separately. Full suite not yet green.
- Android artifact SHA-256:
  `423aff2912245478d94ba44bd0a93ae0d093562f1ed144bd4c0630e56ca01e79`.
- On Termux, the pre-rename `omp --version` returned `omp 18.0.9` (exit 0);
  the renamed `arag` binary reports `arag 18.0.9`.
- `python scripts/standalone-smoke.py --binary <downloaded-pre-rename-omp>
  --timeout 120` passed (exit 0): two local fixture API requests, streamed
  `read` call, real tool result, final response. No external provider
  credentials used.
- Newer shell/writer changes are not covered by that artifact; they require
  a new CI build and device smoke.
