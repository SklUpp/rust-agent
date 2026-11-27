# Унифицированный аудит — Домен 6: Platform HAL (`pi-sys`), Android/Termux SDK/Kernel, Multiplatform Offload, Ecosystem Crates, Packaging/Licenses/Notices, CI & R12 (`ScoutUnifiedPlatformPkg`)

Дата: 2026-11-27  
Источник: сырой отчёт `ScoutUnifiedPlatformPkg` (только чтение документации и исходного кода).

---

## Whole-assignment result — Domain 6 cross-check

Read-only sweep. No files changed, no `cargo`/`git` commands run.

**Authority order applied:** live code/tests → `Cargo.toml`/`Cargo.lock` → decisions → plan → handoff → research. 2026-11-05/06 docs are donor-era evidence; 2026-11-27 tracks are non-normative research.

**Correction to step 1:** `arag doctor` **is implemented** (`crates/aragent/src/doctor.rs:48-433`; CLI in `args.rs:1483,1518`; dispatch `main.rs:787`). It includes R11 sections, `--json`, 0/1/2 exits, `--fix`, pruning/redaction, and fork-isolated syscall probes. Step 1’s “doctor absent” was wrong. `pi-sys`/`Caps`/`Tool::requirements()` remain absent.

---

## A. Done / verified against live artifacts

### Toolchain, manifest, lock
- Workspace is edition 2024, version `0.1.0`, 10 members including `crates/pi-vcs` and `crates/vendor/brush-core` (`Cargo.toml:1-17`).
- Patches: `brush-core` and `rmcp` path overrides (`Cargo.toml:24-26`).
- Profiles: release LTO/strip/unwind; `ci`, `local`, `profiling`, `dev`, `test` (`Cargo.toml:28-94`). `profile.ci` is **not used by CI**.
- Toolchain pin: `nightly-2026-08-08`; targets `x86_64-unknown-linux-gnu`, `x86_64-pc-windows-msvc` (`rust-toolchain.toml:2-4`). CI env matches the nightly.
- `Cargo.lock` contains `gix 0.85.0`, `fastembed 7.8.0` + `ort`, `ruzstd`, `tree-sitter-tags 0.25.10`, `xutf 1.4.0`, `xxhash-rust 0.8.18`; **no** `aws-lc-sys`, `aws-lc-rs`, `cmake`, `openssl-sys`, `libz-sys` (`Cargo.lock:1664,1346,5140,6469,7539`).
- TLS: `webpki-roots`, rustls ring provider installed once (`aragent/src/main.rs:221`).

### CI
- Two jobs: host check/tests/smoke, Android arm64 build (`standalone-runtime.yml:24-145`).
- Host: `cargo check/test --locked`, opt-in local-embeddings check, `pi-vcs` tests, selected shell tests, debug build, smoke, SHA upload (`:46-79`).
- Android: NDK `r27c`, API **24**, target added manually, `cargo check --features local-embeddings`, `cargo build --release`, `llvm-strip`, ELF magic check, SHA (`:82-145`).
- `R0.1–R0.3`, `R1`, `R2`, `R3`, `R4`, `R5`, `R6`, `R7`, `R8` are recorded closed in plan/handoff; CI runs cited there. Not independently rerun.

### Android/runtime adaptations already landed
- Android avoids pidfd: start-time identity, `kill(-pgid)` (`pi-shell/src/process.rs:20-55,176-225,312-317`).
- Process broker length-checks `sun_path` (104) and uses short `$TMPDIR` runtime (`pi-agent/src/process_broker/protocol.rs:51-52,160-164,375-385`).
- Doctor implements per-syscall isolated probes with SIGSYS detection, cache key `(kernel,sdk,selinux,version)`, `getprop ro.build.version.sdk`, SELinux hash, `pidfd_open`, `seccomp`, `landlock`, `memfd_create` (`doctor.rs:58,2186-2385`). Probe list is 4; `unshare_user`/`memfd_create_execveat` exist in the dispatcher but are not in `PROBE_NAMES`. Cache writes only under `--fix`/`--reset-probes`.
- Diagnostics: enabled by default, `0700/0600`, typed spans only, no paths/content (`diagnostics.rs:1-18,43-56`; README §Diagnostics matches).
- Trust/env: SSH and LSP clear env; project surfaces gated by trust list; ACP read path mapped (`ssh.rs:172-215,408`; `lsp/client.rs:934`; `lsp/config.rs:290-320`; `trust.rs:36-82`; `read.rs:680-710`).
- 3-way/merge: `pi-vcs` gix backend, `pi-iso` xxh3/rcopy cap, fallback lists (`pi-agent/Cargo.toml:29`; `pi-iso/src/lib.rs:141,358-375`).

### Licensing
- Root MIT, Stencil attribution (`LICENSE:1-10`); prompt assets have `packages/coding-agent/LICENSE`; vendored `rmcp` and `brush-core` have LICENSE; crate-local licenses for `pi-ast`, `pi-builtins`, `pi-iso`, `pi-shell`, `pi-walker`.
- Notices contain `rmcp 3.9.0`, `brush-core`, JS/Python license sections, and toolchain attribution (`THIRD-PARTY-NOTICES.txt:22-35,1039,10852,12542+`).

---

## B. Doc/code drift, stale or missing artifacts

### 1. README ↔ manifest/lock/CI
- **`pi-vcs` contradiction.** README §Extracted closure says `pi-vcs`, `pi-voice`, `pi-natives` are **not** present and lists 9 members. Live: `Cargo.toml:3-14` includes `crates/pi-vcs`; `pi-agent/Cargo.toml:29` depends on it; CI tests it; `Cargo.lock:4454` contains it.
- **License list drift.** README names `aragent`, `pi-agent`, `pi-grep-pattern` as the only root-license crates. `pi-vcs` has no crate-local LICENSE and is omitted; `crates/vendor/rmcp/LICENSE` also exists but is absent from README’s local-file list.
- **Commit-SHA claim false.** README §Versioning says every CI artifact ships the commit SHA. Workflow uploads only `arag` + `arag.sha256` (`standalone-runtime.yml:70-79,137-145`); no commit file or version file.
- **Observed-verification section stale.** It says host test compilation failed and full suite is not green; handoff records green CI through `14ad1a5` and later runs. Historical text not updated.
- **“No claim tests pass” vs CI.** README §Commands says no claim; CI/handoff do claim selected green runs. Not contradictory logically, but stale.
- **Manifest metadata points upstream.** `repository = https://github.com/can1357/oh-my-pi`, `homepage = omp.sh` (`Cargo.toml:20-22`), not `SklUpp/rust-agent`. Packaging/release metadata misattributes the product.
- **Dangling comments.** `Cargo.toml:33-44` references `crates/pi-natives/...`, which is absent (`crates/pi-natives` path not found). Comment-only, but misleading.

### 2. CI/R12
- **No release path.** No tag trigger, no release job, no signed/attested artifact, no `GITHUB_SHA` file (`standalone-runtime.yml:6-12`). R12’s “CI release job” is missing.
- **Host artifact is a debug binary.** `cargo build` uses `profile.dev`; artifact is named `arag-x86_64-unknown-linux-gnu` and has a SHA, but is not a release build (`:66-79`). `profile.ci` is unused.
- **Android artifact scope.** Release build excludes `local-embeddings`; only `cargo check` covers that feature (`:132-134`). No runtime/device validation, no ONNX presence check.
- **Packaging matrix missing.** SCOPE R8 requires “artifacts of four platforms … CI platform matrix”; release-plan e09 is marked done with “macOS/Windows CI matrix deferred” (`release-plan.yaml:87-90`). Only Linux x86_64 + Android arm64 exist.
- **Linkage claim unverified.** Workflow comment calls Android “statically linked”; only `file` checks ELF magic, no `readelf -d`/`llvm-readelf` closure check (`:138-139`).
- **No license/notices gate.** No `cargo about`, `cargo deny`, notice diff, or notice upload. Notices are not in artifacts.
- **Selected tests only.** No `--workspace` check/test; `pi-builtins`, `pi-ast`, `pi-walker`, doctor tests are not CI gates.

### 3. THIRD-PARTY-NOTICES
Generated claim: “Generated from Cargo.lock by cargo-about 0.8.2 … all-feature, all-target … cargo-deny independently evaluates” (`THIRD-PARTY-NOTICES.txt:10852`).

Live lock disproves completeness:
- No entry for `gix 0.85.0` (`Cargo.lock:1664`).
- No entry for `ruzstd` (`Cargo.lock:5140`).
- No entry for `tree-sitter-tags 0.25.10` (`Cargo.lock:6469`).
- No entry for `xutf 1.4.0` (`Cargo.lock:7539`).
- `fastembed` appears only as JS `fastembed 2.1.0` (`:1039`), not Rust `fastembed 7.8.0` (`Cargo.lock:1346`) or its `ort` stack.
- `xxhash-rust 0.8.18` is present; `xutf` is not.
- No `about.toml`, `deny.toml`, `about.hbs`, or `licenses/` exists; the cargo-deny claim has no policy artifact.

This is both a stale-artifact defect and a legal/release risk.

### 4. Specs are not executable state
- `specs/epics/` contains **only `e01-r0-mcp/`**. `release-plan.yaml` declares capsule dirs for `e02`–`e14` (`:31-128`), but all are absent; only `e01` has `file:`. `execute-plan` cannot resume e02–e14.
- `specs/execution-status.yaml` is stale:
  - `e01s02: in_progress`, but `specs/epics/e01-r0-mcp/epic.yaml` and `e01s02-tasks.yaml` say **done**.
  - `e01s04: in_progress`, but epic/tasks say **failing**.
  - It tracks only e01 although e02–e09 are marked done.
- `specs/state.yaml` sets `active_epic_id: e10`, `active_story_id: null`, while e01 is still failing and e10 is `planned` with a live-device gate.
- `release-plan.yaml` version `0.2.0-dev`; package/README `0.1.0`. Expected for a prerelease, but there is no single source of truth for the next bump.

### 5. Docs/decisions drift
- `docs/README.md:9` says decisions **D01–D83**; `SCOPE_LATEST.yaml:4` says **D01–D96**. Decisions file actually ends at **D83** and contains a **duplicate `### D80`** (`agent-architecture-decisions.md:421,550`). No D84–D96 entries exist.
- Plan preamble explicitly says D84–D96 were **not adopted** (`agent-preliminary-implementation-plan.md:5`), directly contradicting SCOPE’s D01–D96 claim. `research-2026-11-27-09` references `D86`, which has no decision entry.
- `agent-preliminary-implementation-plan.md:60` still says local Termux build pulls `aws-lc-sys`/`cmake`; `:85` records that the reqwest migration removed them, and `Cargo.lock` confirms removal. Stale paragraph.
- SCOPE R1 says SQLite **v6**; live `session_store.rs:55` is **v7**, plan and release-plan e02 say v7. SCOPE stale.
- SCOPE R5/out-of-scope forbids “Local ONNX / fastembed Neural Inference on Device”; live `aragent/Cargo.toml:31,54`, `pi-agent`/plan R5, release-plan e05/e06, and CI all implement/check opt-in `local-embeddings` with `fastembed 7.8`. Normative conflict, not just stale text.
- R12 gate conflict:
  - SCOPE R12: final 0.2.0 requires closing **all R0–R12**.
  - release-plan ordering: stable 0.2.0 is “not declared before R7”.
  - plan R12: tag matches README §Versioning, SHA artifacts, reproducible CI.
  - Three different thresholds; only the last maps to CI work.
- Plan R8 says “part of feature profiles deferred”; SCOPE R8 requires Cargo feature profiles. Only `core-grammars`/`full-grammars`, `utok-all-families`, `local-embeddings`, `test-support` exist. `direct-providers`, `dap`, `security-scan`, `termux-slim` are absent from all manifests.
- `docs/agent-work-handoff.md` says `THIRD-PARTY-NOTICES.txt` was updated at `08107cd`; the missing gix/ruzstd/tags/xutf/fastembed-7.8 entries prove it was not fully regenerated.
- Handoff says R11 (`arag doctor`) is next; doctor is already implemented. Code is ahead of plan.
- README says `pi-vcs` absent; handoff says `pi-vcs` ported. Handoff correct, README stale.

### 6. Platform/feature gaps that contradict “done” claims
- `Tool::requirements()` and `Caps` do not exist; `Tool` exposes only `is_exclusive` (`tool.rs:27-58`). Doctor reports probes but no registry filters on them.
- `pi-sys` crate absent. No `TargetId`/`TargetCaps`/`TargetRouter`.
- `pi-iso` Android auto-order still hardcodes `[Rcopy]`; FUSE is only a fallback for non-linux/macos/windows (`pi-iso/src/lib.rs:358-375`).
- No `__omp_worker_remote`, no remote `Exclusivity`/`UnknownOutcome` offload.
- Default `pi-ast` embeds all grammars (`Cargo.toml:80`); `pi-agent` uses default features (`:32`), so every build is full. No slim profile.
- `pi-builtins` dependencies are non-optional; `onig`/`jaq`/`grep-pcre2` cannot be dropped by features (`pi-builtins/Cargo.toml:55-184`).
- Windows: `rust-toolchain.toml:4` lists `x86_64-pc-windows-msvc`, but `config_cmd.rs:24`, `local_cmd.rs:21`, `rpc/mode.rs:11` import `std::os::unix` unconditionally; env allowlist lacks `SystemRoot`/`APPDATA`/`USERPROFILE` (`state.rs:1615-1628`), and no Windows CI exists.
- Android `find` birthtime remains inverted: `cfg(target_os="linux")` is false on Android, so the unsupported branch runs (`find.rs:4455-4480`).
- `sort` uses Linux-only available memory; Android falls back to physical RAM/4 (`sort.rs:113-120`).
- `mktemp` falls back to `env::temp_dir()`/`/tmp` if `$TMPDIR` is absent (`mktemp.rs:544-550`).
- `read` treats raw errno 36 as ENAMETOOLONG on all platforms (`read.rs:1379`); wrong on macOS/BSD.
- XDG split remains: `dirs.rs:151` treats Android as Linux; `config init-xdg` includes Android (`config_cmd.rs:2221`).
- Duplicate lock/rename paths remain (`config_cmd.rs:2495-2620` vs `local_cmd`/`atomic_publish`), including abstract-socket lock on Linux but flock fallback on Android.
- PCRE2 JIT is disabled only on macOS (`grep.rs:25-31`); Android W^X implications unverified.

---

## C. Pending / deferred — exact conditions

1. **R0.4 live smoke**
   - Must run `ARAGENT_LIVE_FILESYSTEM_SMOKE=1 cargo test --locked -p aragent --test mcp_filesystem_smoke` on Termux with an external filesystem MCP server, including `kill -9` reconnect and zero zombies.
   - Current artifact: `e01s04` = `failing`; task 3 = `failing`; CI skips the gated path.

2. **Live device proof R1–R8**
   - Hard gate for R9/R10: `state.yaml` open decision, `release-plan` e10/e11 `gates.live_device_proof`, SCOPE ordering.
   - Requires running the Android arm64 artifact on Termux/Android, not just CI compile.

3. **R9**
   - `~/.omp` → `~/.aragent` copy, no source deletion, dual-read, writes only to new path.
   - Also requires “full license and NOTICE audit” per SCOPE R9. Current notices fail that precheck.

4. **R10**
   - `/retry` and `Alt+R`, interrupted-turn warnings, explicit confirmation for unknown tool outcome, warning retained in next context.
   - No implementation found (only generic `UnknownOutcome` handling). Must not auto-retry mutating effects.

5. **R11**
   - Doctor is implemented, but release-plan e12 is still `planned`; CI does not run doctor tests; R11 “done” must be reconciled in plan/spec/state after verification.

6. **R12**
   - Needs tag==README version, pre-release chain, reproducible CI artifact, `arag.sha256` + commit SHA, final full-scope gate.
   - Missing: tag/release job, commit-SHA artifact, notices/license artifact, host release build, macOS/Windows artifacts, reproducibility proof.
   - Condition conflict: R7-only vs R0–R12 final gate.

7. **Deferred by decision**
   - macOS/Windows CI matrix (`release-plan` e09 note; handoff).
   - `fastproot`/Linux env, R3 kernel sandbox (`seccompiler`/`landlock`/`bwrap`), `process-wrap` (plan R3/R8 notes).
   - `termux-slim` and optional builtin deps (blueprint; not implemented).
   - `direct-providers`, `dap`, `security-scan` features (decision 2026-11-20; absent).
   - Local ONNX scope exception: code exists opt-in; SCOPE still forbids it.

8. **Android SDK/kernel pending proof**
   - CI floor is API **24**; no `targetSdk`/manifest exists. Docs call for SDK **28** target; no artifact config.
   - No exec-from-writable-dir probe, no `execve`/`execveat` probe, no post-`targetSdk` behavior check.
   - Doctor probes SDK/SELinux/seccomp/memfd/landlock, but no `CAP` gating and no cache refresh by default. Probe list omits `unshare_user` and exec probes.

9. **Notices/license**
   - Regenerate from current `Cargo.lock` with `cargo-about` all-feature/all-target and run a `cargo-deny` policy; add `about.toml`/`deny.toml` if that workflow is claimed.
   - Add notices to release artifacts and gate on diff.
   - Resolve `pi-vcs` crate-local license/README text.

10. **Windows**
    - Either drop the Windows target/claim or make `arag` compile: remove unconditional `std::os::unix` imports, add Windows env allowlist, add a CI build. `rust-toolchain.toml` currently advertises a target no job builds.

---

## D. Open decisions & risks

1. **Scope conflict: local ONNX/fastembed.** SCOPE out-of-scope vs implemented opt-in feature + CI check + plan R5. Decide: amend SCOPE to allow opt-in `local-embeddings`, or remove the feature from code/CI. Leaving both makes R12 qualification ambiguous.
2. **R12 definition conflict.** Three gates disagree. Pick one authoritative rule; if SCOPE’s “all R0–R12” stands, e12 cannot close with e10/e11/e13 planned.
3. **Packaging contract vs actual CI.** SCOPE R8 says four platforms; e09 says done with macOS/Windows deferred. Decide whether R8 is “partially done” or SCOPE is amended. Same for feature profiles (`direct-providers`/`dap`/`security-scan`).
4. **Legal risk: notices stale.** Missing `gix`, `ruzstd`, `tree-sitter-tags`, `xutf`, `fastembed 7.8`, `ort` entries; no policy files; no artifact inclusion. Do not ship 0.2.0 until regenerated and audited.
5. **Release-readiness risk.** Host artifact is debug; no release job; no commit SHA; no reproducibility check. Current CI cannot support R12’s “reproducible CI artifact” claim.
6. **Android W^X/SELinux.** `targetSdk 28` strategy is documented but not configured; doctor probes host behavior only. `memfd_create` is probed but exec-from-memfd is not; no guarantee for API 29+ or vendor kernels.
7. **Tool availability remains ungated.** Doctor is diagnostic, not `Caps`; no `Tool::requirements()`, no `pi-sys`, no `TargetRouter`. Tools can still disappear/fail by platform rather than being filtered once.
8. **Windows claim contradiction.** Target pinned but code unix-only; remove or implement.
9. **Notices provenance statement is false.** `THIRD-PARTY-NOTICES.txt:10852` claims current `Cargo.lock` all-feature generation; the lock proves otherwise.
10. **Crash-symbol risk.** `strip=true` + Android `strip=symbols`; no separate symbol artifact; crash backtraces lose file/line detail. README no longer claims symbols unreadable, but R12 diagnostics may need a symbol bundle.
11. **Specs are not executable.** Missing e02–e14 capsules, stale `execution-status.yaml`, `state.active_epic_id: e10` while e01 fails. `execute-plan` will operate on incomplete state.
12. **Doc authority drift.** `docs/README.md` D01–D83 vs SCOPE D01–D96; duplicate D80; plan says D84–D96 not adopted; research references D86. Decide the decision log boundary before using D84–D96 as requirements.

---

### Bottom line

Domain 6’s core runtime pieces are real: Android-safe process handling, fork-isolated doctor probes, diagnostics, trust/env isolation, gix/pi-vcs, and the API-24 CI build. The **release contract is not met**: notices are stale/incomplete, CI has no release path, host artifact is debug, packaging matrix is 2/4, README/specs disagree about `pi-vcs`, D-number ranges, R12 gates, and local ONNX scope. Live Termux proof remains the hard gate for R9/R10 and any honest R12 claim.
