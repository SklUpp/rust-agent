# Унифицированный аудит — Домен 1: Core Loop, State, SQLite Durability, Context, Compaction, VFS (`ScoutUnifiedCoreState`)

Дата: 2026-11-27  
Источник: сырой отчёт `ScoutUnifiedCoreState` (только чтение документации и исходного кода).

---

# Domain 1 final cross-check — docs vs live code

Read-only. Every claim below checked against the named source; no builds/tests run.

## (A) Done and verified

### A1. Core loop ordering / durability contract
- `crates/pi-agent/src/durable.rs:1-24,98-190` states and enforces: `persist_messages` before request and after assistant turn; `begin_execution` after selection/approval/cancel recheck and before effect; `finish_execution` after effect; execution id distinct from provider call id; no second journal; no silent downgrade.
- `loop.rs:1498-1535`: journal accepted messages (`turn_observer`) **before** destructive compaction; skip prune when that write failed. Compaction remaps via `CompactionReport`, never length inference (`compaction.rs:34-50`).
- `loop.rs:1645-1680`: provider response accepted first, then `mark_deliveries_processed`; assistant turn persisted before tool effects.
- `loop.rs:2099-2185`: durability failure stops before next request; unknown outcome stops **without** synthetic tool result and is raised as `AgentError::DurableUnknownOutcome`; no automatic retry.
- `loop.rs:598-635`: `repair_tool_batch_boundary` finishes partial tool-result runs and drops a dangling trailing assistant tool call; matches the documented cut rule.
- `TurnAck` (`loop.rs:132-290,1201-1245`) is optional; default decision continues, cancellation resolves default; not a journal.
- `state.rs:1630-1660`: env is an allowlist (`PATH/HOME/PREFIX/TMP*/LANG/TERM/SHELL/TZ/USER/LOGNAME`, toolchain roots, named `XDG_*`, `PI_MAX_AST_FILES`, `LD_PRELOAD/LD_LIBRARY_PATH/NODE_OPTIONS`, `LC_*` prefix), non-UTF-8 dropped; `AgentContext::Debug` prints env keys only (`state.rs:1732-1750`); all three constructors use `platform_env` (`state.rs:1754,1768,1787`). D27 leakage fix verified.

### A2. SQLite v7 durability
- Schema v7, 10 app tables, idempotent probes: `session_store.rs:55-248`. v7 adds nullable `sessions.producer_generation` with `CHECK`; v6 rebuilds the admissions unique index.
- Immutability triggers on `executions` outcome and `fork_provenance`; cascade deletes; FKs on.
- Writable connection: `busy_timeout=5s`, `foreign_keys=ON`, `journal_mode=WAL`, `synchronous=FULL` (`:3393-3402`). Read-only opens never migrate/WAL-switch (`:2339-2340`).
- One writer thread, flock lease, symlink/hardlink refusal, `0700` lock root, owner UID check (`durable_session.rs:64-143`).
- Every write is `BEGIN IMMEDIATE`; `Busy`/`UnknownOutcome` error classes (`session_store.rs:250-350`). `WriteTracker/CancelGuard/call_async_tracked` mark cancelled writes Unknown; `barrier()` returns `UnknownOutcome` on failure (`durable_session.rs:314-543`).
- Recovery: unresolved intents are reported without mutation; recovery appends an interrupted tool result on the active branch only, leaves `outcome_json NULL`, validates provider call-id (`session_store.rs:4519-4780`). `INTERRUPTED_UNKNOWN_OUTCOME` text as documented.

### A3. stream_drafts
- One row per branch, text-only, cascade; `save_stream_draft` redacts, preserves foreign attempt as prior, caps priors at 8 by merging oldest with explicit separator, preserves legacy `{"text":…}`, always `aborted/interrupted/partial` (`session_store.rs:6049-6260`).
- Cleared atomically only with a terminal assistant message carrying `streamAttempt` and stop ∈ `stop|length|toolUse` (`:6445-6475`); `error/aborted/unknown` retain.
- Debug defaults exactly `memory_snapshot_chunks=5`, `persist_every_chunks=20`, `flush_interval_ms=2000` (`pi-agent/src/state.rs:1032-1048`); config keys wired (`aragent/src/config.rs:2519-2548`).
- Resume uses `peek_recovered_partial`; draft persists until next terminal; text shown as interrupted fragment and passed once as interrupted model context, never replayable (`durable_session.rs:848-874`, `main.rs:1496-1512`, `runtime.rs:1938-1942`).

### A4. executions / deliveries / admissions
- `record_intent` uses a store-owned execution key plus separate provider call id; redacted; IMMEDIATE insert (`session_store.rs:4027-4090`).
- `record_outcome` first-terminal-wins, exact idempotent, conflicting value `Conflict`; outcome + tool-result message in one txn (`:4092-4120,6497-6535`).
- Deliveries: stable ID, unique message link, `delivered_at` vs `processed_at` separate; `complete_worker` writes completion + delivery atomically; sync completions pre-delivered/pre-processed; legacy ambiguous payloads `Corrupt`; `consume_delivery` atomically appends + marks delivered, rejects sync rows; repeat exact = OK, different = `Conflict` (`:4872-5035,5380-5588,5735-5765`).
- Admissions: `task_id = worker@generation`, unique `(worker,generation)`, state CHECK, admit before spawn, start after effect request, terminal finish idempotent, takeover abandons open rows (`session_store.rs:179-191,5176-5300`; `durable_session.rs:2286-2330`).

### A5. fork_provenance
All documented rules verified:
- Table-only classification; payload markers untrusted; no operational intent install; UPDATE trigger; cascade; sealed ctor (`session_store.rs:234-248,1560-1580`).
- `capture` direct-source ownership + provider-call-id match + owned/copied consistency (`:1582-1663`).
- `create_fork_atomic_with_provenance` does row-insert, duplicate checks, and provenance insert inside the fork transaction (`:1690-2150`).
- Read-only v1/v2 → empty, v3 missing table → `Corrupt` (`:1421-1465`).
- Fork caller captures from source snapshot before writing (`session.rs:4934-5030`), including projection remap, synthetic messages, replay validation.

### A6. R6 export / import / inspect
- Read-only bundle read; redaction before export; legacy 9-table export and v7 bundle both present (`session_store.rs:2431-2800`).
- `validate_session_bundle` is non-mutating and runs before the import transaction (`:2804-2843,894-1130`).
- Import is atomic, reallocates DB-wide `messages.seq` and `executions.seq`, detaches missing parent only on request with payload marker, rejects existing session (`:2804-3050`).
- Bundle covers the 10 app tables including `artifacts` **metadata** (files never copied); legacy path omits artifacts.
- `inspect_session` and `inspect_database` are read-only, redacted, counts/metadata only (`:3057-3323`); CLI `--context` prints category counts (`main.rs:4728-4739`).

### A7. utok
- Families/mechanism table matches docs; exact embedded o200k + Claude; `utok-all-families` gates others, fallback o200k (`utok/mod.rs:1-20,113-143`).
- ruzstd `StreamingDecoder`, 16 MiB decoded cap, trailing-byte and checksum rejection at build time (`mod.rs:40-90`); Claude encode returns `None` (tests `tests/claude.rs:65`).
- BPE uses rank tables + `BinaryHeap` merge engine (`bpe.rs:1-30,95+`); `ignore_merges` whole-piece semantics tested (`tests/glm.rs`).

### A8. Compaction (core) and context breakdown
- Mechanical prune: leading system messages preserved, backward token budget, minimum recent, orphan tool-result trimming, explicit `COMPACTION_NOTICE_PREFIX` system notice (`compaction.rs:15-183`); pairing repair also in loop (`loop.rs:598`).
- `CompactionReport` + `remap`, and durable verification by digest plus report-shape checks; refuses a retained message that was never persisted (`durable_session.rs:1695-1755`).
- Projection persisted in `sessions.payload_json.durableProjection`, redacted, with `anchor` set by `compact_session`; full original messages remain in `messages` (`session_store.rs:4011-4050`; `durable_session.rs:1200-1245`).
- Context estimate is `system_prompt/user/assistant/tool_calls_and_results/thinking/total`; payload redacted before counting; provider usage accumulated only from provider-reported `message.usage` (`session_store.rs:1220-1310`).
- model-aware tokenizer used in loop (`compact_report_for_model`, `loop.rs:1510-1520`); limit from catalog `contextWindow`, fallback `DEFAULT_CONTEXT_LIMIT=32768` (`runtime.rs:63,4354-4357`).

### A9. VFS state
- No `PathSpace`/`RootMap`; only `PathMap` read mapping with identity-or-strict-containment and symlink-escape fail-closed (`state.rs:1367-1423`). D86 remains operative, as documented.

## (B) Drifted, stale, or partial

1. **Step 1 correction — provider usage.** I wrote “lifetime aggregate”. Live code sums `message.usage` over the **active branch’s messages only** (`session_store.rs:1242-1250`), not a session lifetime counter. It survives restart because it is embedded in messages, but it resets when the branch changes. D74 accepted text only requires “usage provider separate”, so this is not a contract breach — but the stronger lifetime claim from my Step 1 is wrong.
2. **Plan’s “12 BEGIN IMMEDIATE”** is a historical R2 snapshot. Live `session_store.rs` has many more write sites (schema v3–v7, forks, admissions, drafts, import). Do not quote 12 as current.
3. **Architecture journal has two `### D80` headings**: SQLite basis (line 421) and fork atomicity (line 550). Fork rules are the second. `D84–D96` are triage items in plan §6, explicitly not accepted journal entries.
4. **`agent-core-extraction-design.md` env statement is stale.** It says `AgentContext::{new,with_cancellation,with_approver}` do `std::env::vars().collect()`. Live code uses `platform_env` in all three (`state.rs:1754-1787`). The plan R1 “mandatory D27 fix” is done.
5. **D18 is partial.** Accepted intent: automatic near-limit management, full archive, manual run, configurable strategies/thresholds. Live:
   - Loop auto-prunes mechanically before a request; full archive retained.
   - RPC exposes `compact` (model-written summary) and `set_auto_compaction` (`rpc/types.rs:289-298`, `rpc/session.rs:3595-3660`, `rpc/mode.rs:1683-1730`).
   - RPC auto-compaction threshold is hardcoded **90%** with a `ponytail:` seam comment (`mode.rs:1684-1713`), default off (`rpc/session.rs:561`).
   - TUI/CLI in-process command list has **no `/compact`** (`tui.rs:1262-1284`).
   - No configurable strategy/threshold keys exist in `config.rs`.
6. **Compaction wording drift.** Docs describe “summary” / synthetic projection; the core loop produces a **mechanical system notice**, not a model summary. The only model-written summary is the RPC `compact` path. Both are real, but they are different mechanisms with different semantics (RPC replaces live history with a custom `compaction` message; loop keeps a kept-tail projection).
7. **Compaction budget has no reserve.** `loop.rs:1510-1515` passes the full catalog context window as `token_budget`; it does not subtract response reserve. `min_recent` is kept even when over budget, so a post-compaction request can still exceed the window. D18 defaults were left open, but this is a live risk, not a satisfied “approaching limit” rule.
8. **RPC vs loop anchor semantics differ.** `DurableSession::compacted` writes projection with `anchor = None` (`durable_session.rs:1751`); `Session::compact_session`/RPC summary writes an anchor entry (`session.rs:1905-1928`, `rpc/session.rs:3642-3650`). Resume seeding handles both, but docs should not describe one uniform “compaction anchor”.
9. **Projection is session-global, not branch-keyed.** `durableProjection` lives on the session payload and has no branch id. Branch cut clears it in the session layer (`session.rs:2865-2870`), and fork rejects off-branch/ anchor-sibling projections (`session_store.rs:8095,8138`), but correctness relies on the branch-cut clear. This is weaker than “projection is attached to a branch”.
10. **Fork artifact publication is not strictly before DB commit.** Docs say fork is rejected before artifact directory publication. Live validation happens before staging, but artifacts are staged/copied **before** the target DB transaction; DB failure triggers cleanup (`session.rs:4990-5030`). A crash between copy and commit leaves an orphan artifact directory; a later fork fails with “fork artifacts already exist”.
11. **Export table count wording.** Bundle = 9 row tables + `artifacts` metadata = all 10 app tables; legacy `SessionExport` = 9 and omits artifacts. Artifact bytes are never exported or imported.
12. **`take_recovered_partial` vs `peek_recovered_partial`.** Both exist; `take` consumes, but runtime/resume uses `peek` and relies on terminal append to clear. Docs should say “peeked once for display, cleared by the next terminal”.
13. **D47 headless auto-resume is not implemented.** Live session open, when it proves exclusivity, records a new `producer_generation`, recovers unresolved intents, and **abandons open child task admissions** (`durable_session.rs:744-760`). This matches the local-CLI “no automatic continuation” rule and is fail-closed, but the accepted headless default (“automatically continue unfinished tasks after crash/restart”) has no implementation. Child contracts require a matching producer epoch and are refused after restart (`task_persisted_revive.rs:80-115`).
14. **R10 entirely absent.** No `/retry`, no Alt+R, no key-modifier handling in the TUI parser (`tui.rs:690-780`); only RPC `set_auto_retry`/`abort_retry` exist. Unknown-outcome text is surfaced and the recovery tool result carries the warning.
15. **`session_path()` remains a virtual `.jsonl` name** although SQLite is the only authority. Naming compatibility, not a second journal.

## (C) Pending / deferred with exact conditions

1. **R10 `/retry` + Alt+R.** Must be manual only. Warning must state turn/command interruption and, for unknown outcome, that the action may have run. Warning must stay in next-turn context. Unknown-outcome retry requires confirmation, not one keypress. No auto-retry. Blocked on live device proof of R1–R3 per `specs/state.yaml`.
2. **D18 strategy/threshold config and TUI manual compact.** Conditions: selectable strategies, configurable triggers/percent/token threshold, reserve handling, explicit UI before/after report; preserve full archive; invalid config keeps previous effective settings. Current RPC summary path and 90% seam are the only pieces.
3. **D47 headless resume.** Conditions: only non-cancelled/unfinished work; explicit cancel/stop not resumed; runtime code decides from durable state; child epoch must match; current behavior abandons old admissions and refuses old-epoch child revive.
4. **D84–D96.** Triage-only; require per-decision acceptance before implementation. Do not treat plan §6 wording as normative.
5. **PathSpace/RootMap.** Research proposal only (M0–M3), non-normative. Preconditions if adopted: physical path authority, URL alias only when writable, `RootId` for worktrees, outbound processes get physical paths, descriptor-relative symlink checks, literal `://` filename and `ssh://host:22` tests, LSP `WorkspaceEdit` containment, Termux `$PREFIX`.
6. **Fork artifact atomicity.** Needs a defined crash-recovery rule (orphan detection or publish-on-commit) before the “reject before publication” wording can be restored.
7. **Overflow behavior.** Docs state overflow errors are not auto-compacted+retried. Live code has no special overflow handler and no reserve; provider errors pass through. That matches “no automatic recovery”, but the compaction threshold still needs a reserve policy.

## (D) Open decisions and risks

1. **Two compaction paths** (mechanical pre-request prune vs RPC model summary post-turn) can produce different context shapes for the same session depending on frontend. Decide whether RPC summary should also drive TUI/headless or be explicitly a separate feature.
2. **Compaction can leave context over budget** because `min_recent` overrides the budget and there is no reserve. A hard limit path or reserve subtraction is undecided.
3. **Projection is not branch-scoped in storage.** Branch switching clears it, but any future writer that changes branches without the session-layer clear can seed a cross-branch history.
4. **Unknown outcome is surfaced only as an error tool result.** The model may still choose to retry; the decision not to auto-retry is enforced in runtime, not in prompt/policy. A stronger “do not retry” prompt contract is not accepted.
5. **Fork database/artifact two-store atomicity** remains impossible; the crash window is currently handled by cleanup only after an error, not after a process kill.
6. **Artifact metadata import can point at files absent on the target machine.** Inspector reports `missing_artifacts_on_disk`; this is honest but means “loss-free import” is metadata-loss-free, not byte-loss-free.
7. **Provider usage is branch-local.** Cross-branch/lifetime accounting is not implemented; D79’s “all available metrics” is satisfied only per branch/message.
8. **VFS duplicate resolvers** (`internal_urls`, read selectors, edit scheme rejection, AST URL check, ACP `PathMap`) remain; D86 permits the split, but inconsistent normalization is a cross-domain risk.
9. **Headless restart abandons child admissions** and cannot revive old-epoch children. This is safe but conflicts with the accepted D47 default until a resume design lands.
10. **Device proof still gates** R9/R10 and any claim that SQLite durability survives real Termux crashes.

## Corrections to Step 1

- Provider usage: active-branch message usage sum, not a lifetime aggregate.
- Compaction: core loop is mechanical prune + notice; RPC has a real model-written summary and manual `compact` command.
- Recovered draft: runtime uses `peek`, not `take`; the draft clears on the next terminal.
- Fork artifacts: copied before DB commit, then cleaned on error; not universally “rejected before publication”.
- `AgentContext` env is already allowlisted; extraction-design text is stale.
- D84–D96 are not journal-accepted; only plan triage.

## Checks a later verifier should run (not run here)

`crates/aragent/tests/{durable_session,task_process_kill,session_export_import,session_storage,session_cli_lifecycle}.rs`, fork atomicity/provenance unit tests in `session_store.rs`, stream-draft tests, `pi-agent` compaction/tokenizer tests, and a live device crash test covering SIGKILL after intent, after effect, during stream, and during compaction.
