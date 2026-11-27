# Целевое исследование Stage 2: DIR-01, DIR-04, DIR-05 — канонический статус/HEAD, дрейф `SCOPE_LATEST.yaml` и журнал решений (`TargetDir01StatusTruth`)

Дата: 2026-11-27  
Источник: сырой отчёт `TargetDir01StatusTruth` (только чтение кода, спеков, документации и GitHub CI).

---

# DIR-04 + DIR-05 — combined final result

Read-only. No files changed; no builds/tests/cargo commands run. Evidence from source, Cargo manifests/lock, CI workflow/API, specs, and decision journal.

## Part A — DIR-01 (Step 1) canonical state, retained

|Item|Truth|Evidence|
|---|---|---|
|Canonical HEAD|`58129519` on `feat/standalone-runtime`|local ref + remote branch API|
|Stale pins|`54f0fb1` (15 behind), `3b021a58` (12 behind)|compare API|
|Current CI|run `45927444304`, in_progress on `58129519`|Actions API|
|Last full green code rev|`a40c4ef`, run `45926181147`, both jobs success|Actions API|
|Prior green chain|`0ec6d99`/`45827228390`, `6828ab0`/`45839280446`, `c309722`/`45856046782`, `d73b753`/`45873202054`, `14ad1a5`/`45893863926`, `3b021a5`/`45906993418`|Actions API|
|Resolved failures|`f7e8007` host test failure; `6df00fd` host check + Android build failure|Actions API|
|Active epic|Actually e01 only; `state.yaml:2` claims e10, which is planned and uncapsuled|`specs/state.yaml:2`; only `specs/epics/e01-r0-mcp/` exists|
|Open story|e01s04/R0.4 live smoke, never run; test gated by `ARAGENT_LIVE_FILESYSTEM_SMOKE`|`e01s04-tasks.yaml:18-32`; `mcp_filesystem_smoke.rs:94-107`|
|R1–R8|code + CI only; zero live-device proof at current HEAD|`state.yaml:11`; `handoff:37,41`; no run/log artifact|
|Ledger drift|e01s01/s03 task “failing” stale; e01s02 “in_progress” stale; handoff package version `0.1.0` vs release plan `0.2.0-dev`|`e01s01-tasks.yaml:4-20`; `e01s03-tasks.yaml:5-19`; `execution-status.yaml:2`; `handoff:7`; `release-plan.yaml:2`|

---

## Part B — DIR-04: SCOPE R0–R12 disposition

|Ref|SCOPE claim|Actual evidence|Disposition|
|---|---|---|---|
|R0|redaction + SSE guard + rmcp text strip + live MCP smoke|R0.1–R0.3 implemented/tests present; R0.4 test exists but never run|**partial: live proof missing**|
|R1|durable admission/crash recovery|code + SIGKILL tests; CI green; live proof pending|**code/CI done; live unproven**|
|R2|IMMEDIATE txns, Busy/UnknownOutcome|code + tests; CI green|**code/CI done; live unproven**|
|R3|process/lease/policy + seccomp-BPF + landlock/bwrap|process/lease/policy exist; no production seccomp/landlock/bwrap; `process-wrap` only vendored under rmcp|**scope drift: sandbox absent, deferral recorded**|
|R4|full reopen/refactor/resolve semantics; LSP disabled by default|RepoMap/utok/matrix exist; semantic matrix exposes no refactor/resolve op; LSP default is `true`|**partial + scope drift**|
|R5|full memory + skills lifecycle|memory FTS/vectors/consolidation present; skills create/update/delete + load; no validation pipeline, usage accounting, or retirement/review|**partial**|
|R6|export/import/inspector|implemented `ebce4aa`; covered by later green descendant|**done**|
|R7|`diffy` 3-way merge|locked `similar 3.2.0`; no `diffy` in lock; code uses `similar::TextMerge`|**scope drift: SCOPE stale**|
|R8|hooks + four-platform packaging matrix|hooks/trust store done; CI only Linux x86_64 + Android arm64; notices incomplete; artifacts lack commit SHA/notices|**partial: packaging contract unmet**|
|R9|`~/.omp` → `~/.aragent` dual-read|`CONFIG_DIR_NAME = ".omp"` remains canonical; no `.aragent` migration|**missing**|
|R10|`/retry`, `Alt+R`, manual unknown-outcome confirmation|no retry control found in CLI/TUI|**missing**|
|R11|`arag doctor` full diagnostic surface|implemented: 8 sections, `--json`, `--fix`, `--prune-artifacts`, `--redact-legacy`, exit codes|**implemented; ledger says planned**|
|R12|0.2.0 release qualification|`Cargo.toml:17` version `0.1.0`; release-plan `0.2.0-dev`; no release/tag job|**missing + version drift**|
|SEC-TUI|inline-viewport resize/reflow|custom ANSI TUI, alternate screen, resize repaint; no inline reflow/keyboard integration|**partial/not done**|

### DIR-04 focused answers

1. **R3 sandbox** — `seccompiler`, `landlock`, `bwrap` are absent from workspace dependencies and production code; only `doctor.rs:58,2450-2475` probes them. `process-wrap` exists only in `crates/vendor/rmcp/Cargo.toml:848`, not the R3 process engine. Deferral is recorded in the 2026-11-26 journal (`agent-architecture-decisions.md:522-525`) and plan R3 row, but **SCOPE still asserts seccomp-BPF/landlock/bwrap** (`SCOPE_LATEST.yaml:61-62,232`).

2. **R4 semantic operations** — `SemanticOp::ALL` contains only `readSymbols`, `astQuery`, `astReplace`, `repoMapTags`, `lspDefinition`, `lspReferences`, `lspHover`, `lspRename`, `lspDiagnostics`, `lspFormat` (`pi-ast/src/language/mod.rs:389-420`). LSP tool adds `code_actions`/`rename_file`/`type_definition` (`lsp/tool.rs:264-268`), but **no declared refactor or resolve operation**; refactor is only reachable through generic `code_actions`. LSP default is **enabled** (`config.rs:355-362`), contradicting SCOPE’s “disabled by default”.

3. **R5 skills lifecycle** — create/update/delete and authored-vs-managed precedence exist (`managed_skills.rs:195-286`; `autolearn.rs:392-420`). Grep finds no skill retirement/review, skill usage ledger, or candidate validation/evaluation model; memory consolidation is separate. **Only partial lifecycle.**

4. **R7 merge engine** — `Cargo.lock:5426-5427` locks `similar 3.2.0`; grep finds no `diffy` dependency. 2026-11-26 journal explicitly replaces `diffy` with `similar` (`agent-architecture-decisions.md:525`), while SCOPE `:107` and blueprint `:52` still say `diffy`. **Code/decisions agree on `similar`; SCOPE is stale.**

5. **R8 CI matrix** — workflow has two jobs only: host `ubuntu-latest` x86_64 and Android `aarch64-linux-android` (`.github/workflows/standalone-runtime.yml:29-30,82`). No macOS, Windows, musl, or Linux arm64 artifact. macOS/Windows deferral is recorded in journal/plan/release-plan e09 note, **not in SCOPE R8**. Notices: `xutf 1.4.0` present (`THIRD-PARTY-NOTICES.txt:22378`), but `gix`, `ruzstd`, `tree-sitter-tags`, Rust `fastembed 7.8`, and `ort` absent; only TS `fastembed 2.1.0` appears. Uploads include sha256 only, no commit SHA/notices artifact.

6. **local-embeddings conflict** — feature is optional and default-off: `crates/aragent/Cargo.toml:31,54`; CI checks it on Linux and Android (`.github/workflows/standalone-runtime.yml:55-56` host, `:131` Android). But SCOPE declares “Local ONNX / fastembed Neural Inference on Device” **out of scope** (`SCOPE_LATEST.yaml:206`) while R5/plan and the 2026-11-26 journal accept it as opt-in. **Direct SCOPE contradiction; implementation matches the later decision, not SCOPE.**

7. **Deferral provenance** — R3 kernel sandbox, `process-wrap`, R6/4-platform CI deferrals are recorded in dated journal entries and plan notes; R6 was subsequently implemented. R4 LSP default-on and R7 `diffy`→`similar` changes are recorded only in later journal/plan text; SCOPE was not amended for any of R3, R7, R8, or local-embeddings.

---

## Part C — DIR-05: decision-journal integrity

|Item|Evidence|Verdict|
|---|---|---|
|Duplicate D80|`agent-architecture-decisions.md:421` = SQLite session foundation; `:550` = fork atomicity limit|Duplicate number, no direct contradiction; second is later appended decision|
|Which D80 is authoritative|Architecture references (`agent-architecture.md:189`), proposals, and research all point to SQLite basis|`:421` is canonical D80; `:550` needs renumbering|
|Fork atomicity boundary|`create_fork_atomic_with_provenance` opens `TransactionBehavior::Immediate` (`session_store.rs:1692`); filesystem/SQLite cross-store atomicity explicitly not promised|Accepted; implementation matches|
|`fork_provenance` requirement|Controlling sentence `agent-architecture-decisions.md:550-554`: copied tool-result messages classified **only** by `fork_provenance`, written in same SQLite transaction; payload markers untrusted|Required and implemented|
|Schema v7 and provenance|`SCHEMA_VERSION = 7` (`session_store.rs:55`); v7 DDL adds only `sessions.producer_generation` (`:202-210`); `fork_provenance` created by **v3** migration (`:235-249`), inserted inside the fork tx (`:2108`), UPDATE-blocking trigger + cascade delete|**v7 includes the table, but provenance predates v7; it is not a v7-era addition**|
|D84–D93|Plan explicitly says triage only, accepted journal unchanged (`agent-preliminary-implementation-plan.md:286-298`)|**Not accepted decisions**|
|D94–D96|No text anywhere in journal/specs; only mentioned as absent (`SCOPE_LATEST.yaml:5`; domain reports)|**Nonexistent**|
|SCOPE citation|`SCOPE_LATEST.yaml:5` claims “D01–D96”; `docs/README.md:9` says D01–D83|**SCOPE cites decisions that do not exist**|
|Later acceptance of triage items|No D-numbered acceptance exists; 2026-11-26 dated “Уточнения” decide R4/R5/R7/R8 mechanics without D-numbers|**D84–D93 remain unimplemented as decisions**|
|D86 “operative” claim|Errata confirms D86 is not accepted (`research-2026-11-27-18-unified-reviews-errata.md:16`)|**Research wording wrong; VFS split has no accepted D86 basis**|

### Journal repair proposal (no edits performed)

1. Keep `D80` at `:421` as the canonical SQLite-foundation decision.
2. Renumber `:550` to the next free ID (`D97`), or add an explicit `D80-fork` alias; do not leave two `D80` headings.
3. Change `SCOPE_LATEST.yaml:5` from “D01–D96” to the actual accepted set (`D01–D83`, plus any renumbered fork decision), or formally accept D84–D93 into the journal first.
4. Never cite `D94–D96`: no text exists anywhere.
5. Treat `fork_provenance` as governed by the later D80; it is implemented in schema v3 and carried through v7, independent of the D84–D96 numbering problem.
