# Унифицированный индекс корпуса документации и целевые направления Stage 2 (`ScoutDocsUnifiedIndex`)

Дата: 2026-11-27  
Источник: сырой отчёт `ScoutDocsUnifiedIndex` (только чтение документации и спецификаций, шаги 1–3 + матрица шага 2).

---

# Final Assignment Result — Steps 1–3

## 1. Step 1 result — corpus read and unified

**Read as one corpus:**

- `docs/`: **36 substantive documents + `README.md`** = 37 entries.
- `specs/`: **13 files** — `release-plan.yaml`, `state.yaml`, `execution-status.yaml`, `product/SCOPE_LATEST.yaml`, `epics/e01-r0-mcp/epic.yaml`, four task files, four story files.
- Precedence used: **code/tests → accepted decisions → implementation plan → handoff → proposals → research**.
- Research marked “принято” treated as non-normative unless reflected in the accepted journal.

**Unified synthesis:** the project is a coding-first Rust runtime with one loop for TUI/CLI/headless/RPC/ACP, SQLite authority, durable task admission, isolated parallel writers, provider/MCP abstraction, memory/skills, and Termux/Bionic packaging. R0.1–R0.3 closed; R0.4 written but not live-run; R1–R8 implemented with CI evidence; R9–R12 pending. Android app is future scope; Android kernel sandbox and `fastproot` are deferred.

**Core corpus-level finding:** documentation is not a single consistent truth. The accepted journal ends at **D83 + P01**, with **duplicate D80**; the plan discusses **D84–D93 as triage only**; `SCOPE_LATEST.yaml:5` claims **D01–D96**, but **D94–D96 have no text**. Several “done” claims are code/CI done, not live-device proof done.

---

## 2. Step 2 result — unified matrix

Built a **25-row matrix** covering every agent subsystem:

| Matrix plane | Rows | Subsystems |
|---|---|---|
| Core runtime/model | M01–M07 | product scope, modes/lifecycle, providers, permissions, tool loop, persistence, recovery |
| State/effects | M08–M12 | sessions/workspace ownership, VFS/artifacts, execution/shell, isolation/merge, code intelligence |
| Knowledge/context | M13–M17 | memory/skills, instructions/compaction, config/profiles/trust/hooks, MCP, network/web/attachments |
| Surfaces/release | M18–M25 | frontends, diagnostics/metrics, delegation, budgets, Android HAL, packaging/licensing, security/redaction, release/CI |

Each row recorded: governing D-numbers, mandatory invariants, Go/No-Go review gate, stale/contradicting statements, and open questions.

**22 cross-document conflicts** were registered. The highest-impact ones:

- **Status truth:** `state.yaml` says active epic `e10`; `execution-status.yaml` says `e01s02` and `e01s04` in progress; `epic.yaml` says `e01s04`; `release-plan.yaml` says e01 in progress. Task statuses also disagree with story statuses.
- **HEAD truth:** `54f0fb1` in handoff/state versus `3b021a58` in 11-27 research.
- **Scope truth:** SCOPE includes kernel sandbox, four-platform CI, strict semantic coverage, full skills lifecycle; release plan defers or narrows them. SCOPE declares local ONNX/fastembed out of scope while e06 implements opt-in `fastembed`.
- **Decision truth:** duplicate D80; D84–D96 mismatch; D50 architecture stale; D57 hooks deferred but implemented; D20 “no separate trust registry” versus `trust.json`.
- **Behavior truth:** docs say user context files last, source yields user-first; provider contract says no silent drop, code drops reasoning/thinking; MCP contract requires full matrix, fork lacks resources/prompts/structured content.

**Cross-cutting invariants carried into Step 3:**

- Single writer per state; SQLite authority.
- Intent before effect; save failure blocks new model/tool calls.
- Unknown outcome is not “no effect”; no automatic blind retry.
- `deny > ask > allow`; unattended ask denies with no timer.
- Redaction before persistence and before next model turn.
- One runtime for all surfaces; detach does not stop runtime.
- No silent model fallback; no silent capability drop.
- Snapshot base is actual files, not HEAD/index; conflict writes no file.
- Compaction changes projection, not archive.
- Memory is context, not permissions.
- Trust does not expand policy; VFS/path filter is not an OS sandbox.
- Android `targetSdk 28` does not guarantee universal `execve`.

---

## 3. Step 3 — prioritized Stage-2 investigation directions

These are **investigation directions**, not implementation tasks. Each is read-only evidence collection with a required verdict. Order is dependency-driven.

### P0 — blocking truth and safety

#### DIR-01 — Canonical status and HEAD reconciliation

**Derived from:** status-ledger conflict, task/story mismatch, HEAD ambiguity, live-proof gap.

**Scope:** all four status ledgers, handoff, 11-27 sync research, git history, CI run references.

**Anchors:**
- `specs/state.yaml:2` (`active_epic_id: e10`), `:10-11` (live proof pending).
- `specs/execution-status.yaml:2,4` (`e01s02`, `e01s04` in progress).
- `specs/epics/e01-r0-mcp/epic.yaml:6-7` (`in_progress`, active story `e01s04`).
- `specs/release-plan.yaml:10,19-26` (R0.4 pending, e01 in progress, e02–e09 done).
- `agent-work-handoff.md` (`54f0fb1`).
- `docs/research-2026-11-27-01-omp-sync.md` (`3b021a58`).

**Acceptance questions:**
1. Which commit is the authoritative HEAD of `feat/standalone-runtime`, and do both `54f0fb1` and `3b021a58` exist in its history?
2. Which epic/story is actually active, and why does `state.yaml` say `e10` when only `e01-r0-mcp` exists under `specs/epics/`?
3. Are `e01s01`/`e01s03` tasks genuinely failing, or are the task ledgers stale against completed stories?
4. Is `e01s02` really `in_progress` despite all tasks and the story being done?
5. Has R0.4 live smoke ever run on Termux, and what exact command/result proves it?
6. Does “e02–e09 done” mean code + CI only, or live-device proof too?

**Exit artifact:** one canonical table: commit, branch, CI run, story status, live proof, unresolved row. No fixes.

---

#### DIR-02 — D48 context-file order: docs versus source

**Derived from:** user-first vs user-last contradiction.

**Scope:** context discovery, dedupe, prompt rendering, docs, tests/fixtures.

**Anchors:**
- `docs/context-files.md:93-95` claims user files last.
- `src/system-prompt.ts:425-435` sorts by `depth ?? -1`.
- `src/system-prompt.ts:476-484` dedupes with `depth ?? Number.POSITIVE_INFINITY`.
- `docs/project-prompt.md:8-15` renders without resorting.

**Acceptance questions:**
1. What is the actual order for global, project-root, nested project, user, role, and per-task context files?
2. Does `dedupeContainedContextFiles` with `Infinity` invert the sort and produce user-first?
3. Does `project-prompt.md` preserve that order without a second sort?
4. Which order does D48 require, and is “user last” actually a decision or only a doc claim?
5. What minimal fixture proves the order for a project root plus nested directory plus user-level file?

**Exit artifact:** order matrix + failing fixture + one-line recommendation: change doc or change sort/dedupe.

---

#### DIR-03 — D50 cancellation and unattended approval safety

**Derived from:** architecture still describes auto-answer timer; D50 revision forbids it.

**Scope:** turn cancellation, approval state machine, headless prompt handling, security gate, architecture text.

**Anchors:**
- `agent-architecture-decisions.md:277+` — timer/auto-answerer cancelled; no manager → denial; justified assumption may be recorded.
- `agent-architecture.md` D50 — old auto-answer timer description.
- Approval/policy paths around `DeniedUnattended` and `force_ask`.

**Acceptance questions:**
1. Trace a cancelled turn that reaches a security/policy gate: does it return denial, ask, or auto-answer?
2. Is any timer or auto-answerer still reachable in code, config, or defaults?
3. Does no available managing client produce `DeniedUnattended` without waiting?
4. Where is a “justified assumption” recorded, and is it clearly not user consent?
5. Can a cancelled tool still produce an effect, and how is that reported?
6. Which architecture passages still contradict the revision?

**Exit artifact:** cancel → approval → tool state machine + exact stale passages.

---

#### DIR-04 — Scope drift and release-truth audit

**Derived from:** R3/R4/R5/R7/R8, local embeddings, and “done” claims diverging from SCOPE.

**Scope:** `SCOPE_LATEST.yaml`, `release-plan.yaml`, plan R3–R8, actual code/deps/CI.

**Anchors:**
- R3 sandbox: `SCOPE_LATEST.yaml:61-62,232`; `release-plan.yaml:50`; plan `:27,90,522`.
- R4 semantic: SCOPE R4; plan `:48`; decisions `:512`; D91 triage plan `:301`.
- R5 skills: SCOPE R5; plan `:49`; S9 `:243`; `release-plan.yaml:66`.
- R7 merge: `SCOPE_LATEST.yaml:107` (`diffy`); plan `:31,93`; decisions `:471,525` (`similar`).
- R8 CI: SCOPE R8; `release-plan.yaml:90`; plan `:94`.
- Embeddings: `SCOPE_LATEST.yaml:206`; `release-plan.yaml:66`; plan `:49,92`.

**Acceptance questions:**
1. Is `seccompiler`/`landlock`/`bwrap`/`process-wrap` present, disabled, or absent?
2. Which rename/refactor/resolve operations are actually supported, and by which backend?
3. Does a complete skills lifecycle exist — create, validate, use, retire — or only memory consolidation?
4. Is the merge dependency `similar` or `diffy`; is `diffy` still referenced anywhere normative?
5. Which CI platforms actually build, and is macOS/Windows matrix explicitly deferred?
6. Is `local-embeddings` default-off, and does SCOPE’s out-of-scope wording need amendment?
7. For every deferred item: who approved the deferral, and is it recorded in SCOPE/release plan?

**Exit artifact:** disposition matrix: in scope / implemented / deferred / missing / approved by whom.

---

#### DIR-05 — Decision-journal integrity: D80, D84–D96, fork provenance

**Derived from:** duplicate D80, missing decisions, provenance contradiction.

**Scope:** decisions journal, SCOPE references, plan triage, doctor/schema expectations.

**Anchors:**
- `agent-architecture-decisions.md:421` — D80 SQLite session base.
- `agent-architecture-decisions.md:550` — D80 fork atomicity limit.
- `agent-preliminary-implementation-plan.md:299-303` — D84–D93 triage only.
- `SCOPE_LATEST.yaml:5` — D01–D96.
- Plan doctor `:101` — broken `fork_provenance` check.

**Acceptance questions:**
1. Which D80 is authoritative, and should one be renumbered?
2. Is fork atomicity actually limited; what transaction boundary exists?
3. Is `fork_provenance` required or explicitly not required? Find the controlling sentence.
4. Do D84–D96 exist anywhere outside SCOPE? If not, which numbers should SCOPE cite?
5. Which D84–D93 triage items have since become accepted, and where is that recorded?
6. Does schema v7 include `fork_provenance`, or only v6-era task admission?

**Exit artifact:** journal repair proposal: duplicate, missing, renumber, provenance verdict. No edits.

---

#### DIR-06 — Hooks and trust: D57 deferral versus R8 implementation

**Derived from:** hooks accepted then deferred, then implemented; trust registry contradiction.

**Scope:** `hooks.rs`, `trust.rs`, config reload, approvals, project hooks/MCP gating.

**Anchors:**
- `agent-architecture-decisions.md:313` — D57 hooks deferred 2026-11-09.
- Plan `:32,47,94` — R8 subprocess JSON hooks implemented.
- Plan `:299` — hook runtime/language/format/sync still listed open.
- D20 implementation note and `trust.json`; plan `:47`; `release-plan.yaml:90`.
- D20 2026-11-09 — no separate trust registry.

**Acceptance questions:**
1. Which hook events are implemented: `PreToolUse`, `PostToolUse`, `PermissionRequest`, `Stop`, `SessionStart`?
2. What is the exact input/output format, ordering, sync/async behavior, timeout, and error contract?
3. Can a hook bypass `BashPolicy::Prompt` or `force_ask`? Is failure fail-closed?
4. Does config-hash invalidation cover added and changed project hook/MCP files?
5. Does `trust.json` contradict “do not introduce a separate trust registry,” or is it the approved implementation?
6. What happens on revoke, config reload, and project-file change while hooks are active?

**Exit artifact:** implemented hook contract + trust-model gap list + exact stale journal text.

---

### P1 — contract, durability, platform

#### DIR-07 — Provider and MCP wire-level contract audit

**Derived from:** silent reasoning drop, Anthropic-only replay, MCP matrix gaps, transport limits.

**Scope:** provider adapter, replay filter, MCP client, transport layer, protocol negotiation.

**Anchors:**
- `provider.rs:1380-1400` — OpenAI reasoning/thinking fields dropped.
- Provider/MCP contract proposal — no silent drop.
- D05/D30 — current stage OpenAI-compatible only; Anthropic adapter deferred.
- D54 — full MCP client.
- Research `2026-11-06-providers-mcp.md` — resources subscribe/unsubscribe, prompts list/get, `structuredContent`/`_meta`, protocol `2025-11-25`, `rmcp 3.9` `2026-07-28`, 16 MiB stdio, 64 KiB HTTP body, redirects forbidden.

**Acceptance questions:**
1. Which request/response fields are preserved for OpenAI, Anthropic, and replay?
2. Is reasoning/thinking drop deliberate or accidental; is it documented?
3. Is signed replay Anthropic-only, and what happens on OpenAI replay?
4. Is `filterProviderReplayMessages` absent everywhere, or only in one path?
5. Which MCP methods exist: resources subscribe/unsubscribe, prompts list/get, sampling, elicitation, roots?
6. Are `structuredContent` and `_meta` preserved end to end?
7. What are the actual stdio size limit, HTTP error-body limit, and redirect behavior?

**Exit artifact:** wire-level gap table: must-fix / documented limitation / out-of-scope.

---

#### DIR-08 — Durable authority, redaction, recovery

**Derived from:** task admission, intent/outcome, unknown outcome, R0.1 coverage, schema drift.

**Scope:** schema/migrations, `task_admissions`, `executions`, `deliveries`, `worker_completions`, redaction paths, doctor.

**Anchors:**
- D49 — `task_admissions`, states, identity, unique index.
- D33/D34 — intent before tool, result after, save failure stops new calls.
- D47 — missing result means unknown effect.
- R0.1 redaction: `executions.intent_json`/`outcome_json`, `deliveries.payload_json`, `worker_completions`, session title, artifact metadata.
- D80 duplicate; schema v6 vs handoff schema v7.

**Acceptance questions:**
1. Is the admission row written before spawn/effect, and does replay of the same `<worker_id>@<producer_generation>` fail?
2. Are all tool paths covered by intent-before-effect, including MCP, shell, and file writes?
3. What happens when an execution has intent but no outcome after crash — how is “unknown” surfaced?
4. Exactly which tables/columns are redacted before write and before next model send?
5. Does schema v7 still contain v6 admission semantics, and is migration lossless?
6. Does `arag doctor` report counts without values, and can `--redact-legacy` be run without backup/confirmation?

**Exit artifact:** invariant checklist + exact table/column coverage + schema migration verdict.

---

#### DIR-09 — VFS, artifact, URI, external-path model

**Derived from:** `PathMap` vs `RootMap`, read-only resource rule, artifact publication, URI spelling open.

**Scope:** `state.rs`, VFS resolution, artifact store, external path attachments, URI tool.

**Anchors:**
- `state.rs:1367` — `PathMap`.
- D21/D79/D81/D82 — real workspace paths, separate artifact store, one managed root, publication order.
- D86 triage only — real workspace/snapshot vs synthetic URI router.
- Read-only mutation rule: mutate URL only if `InternalResource.source_path` set and writable; otherwise `"read-only resource"`.

**Acceptance questions:**
1. Is the implementation `PathMap` or `RootMap`/`PathSpace`, and what does `Resolved` actually contain?
2. Can a synthetic resource URL be mutated when `source_path` is absent or read-only?
3. Does artifact publication follow ID → temp → publish → SQLite, and can it overwrite an existing artifact?
4. How are external paths attached, and do they carry write permission separately?
5. Is URI spelling decided anywhere, or still open under D21/D79/D81?
6. Does any code treat artifact references as ordinary OS paths?

**Exit artifact:** implemented model vs recommended Variant 3; mutation/publication gaps.

---

#### DIR-10 — Android HAL, packaging, fastproot/LKL

**Derived from:** false platform assumptions and deferred sandbox/env.

**Scope:** Android build/runtime, process control, temp paths, `sort`, inotify, packaging, fastproot/LKL.

**Anchors:**
- `crates/pi-builtins/src/sort.rs:113-120` — `available_memory` Linux-only, otherwise total RAM.
- `dirs.rs` — `${TMPDIR:-$PREFIX/tmp}`.
- Android research 03/04/10 — `targetSdk 28`, `execve`, `AT_SECURE`, `memfd_create`, `execveat`, `inotify_rm_watch` cast.
- Plan `:90` — fastproot/kernel sandbox deferred.

**Acceptance questions:**
1. On the actual device, does `execve` work for all shipped artifacts, or only some paths?
2. Does `LD_LIBRARY_PATH` work only with `AT_SECURE == 0`; what breaks otherwise?
3. Is `memfd_create`/`execveat` used, and is W^X bypass actually proven?
4. How is process-tree kill implemented without `pidfd`, and is TERM→KILL tested?
5. Does `sort` use total RAM on Android, and does that cause OOM?
6. Is the Bionic `inotify_rm_watch` cast fixed everywhere?
7. Are `fastproot`/LKL absent, deferred, or partially present?

**Exit artifact:** device-proof matrix: proven / unproven / false assumption.

---

#### DIR-11 — Memory, skills, compaction, context integration

**Derived from:** full skills lifecycle gap, local embeddings scope, compaction epoch, context order.

**Scope:** `memory_store`, embeddings/vectors, consolidation, skills catalog/lifecycle, compaction, context assembly.

**Anchors:**
- D14/D52/D53/D58 — memory, skills, provenance, custom commands.
- D18/D74 — compaction archive/projection, inspector.
- D89 triage only — FTS baseline, vectors optional.
- D90 triage only — compaction epoch/revision.
- `local-embeddings` feature and SCOPE out-of-scope.
- Context order anchors from DIR-02.

**Acceptance questions:**
1. Are scopes `global`/`project`/`session` real, and is provenance user vs inferred persisted?
2. Does deleting memory leave history untouched, and is the derived index rebuildable?
3. Is `local-embeddings` default-off, and is its SCOPE conflict resolved?
4. Which skills lifecycle stages exist: create, validate, use, retire, confirm?
5. Does skill loading ever execute scripts without separate permission?
6. Does compaction keep full archive, retained tail, `parentId`, branch summary, and visible epoch?
7. Does context assembly use the source order or the documented order?

**Exit artifact:** capability matrix: memory / skills / embeddings / compaction / context order.

---

### P2 — surface parity and release gate

#### DIR-12 — Frontends, notifications, diagnostics, packaging release gate

**Derived from:** partial S10, Ratatui candidate-vs-pin, unverified export/inspector, notifications incomplete.

**Scope:** TUI, CLI, headless, RPC/ACP, notification hooks, doctor, export/import, packaging.

**Anchors:**
- Frontends research: `ratatui 0.30.2`, `init()`, `restore()`, `init_with_options`, `Terminal` API.
- Plan S10 — partial; export/import and inspector unverified.
- D65/D72/D77 — notifications, diagnostics, metrics.
- D71/D83 — packaging/licensing.

**Acceptance questions:**
1. Does TUI use `ratatui 0.30.2` init/restore and a panic hook correctly?
2. Do TUI, CLI, headless, RPC/ACP all use the same runtime loop?
3. Is NDJSON stdout clean, with service text only on stderr?
4. Does detach leave runtime alive, and is one-active-client enforced?
5. Does the inspector show categories/estimates only, with no message content or secrets?
6. Does export/import exclude credentials and avoid executing imported calls?
7. Are notifications local-only by default, and is diagnostic report sending manual with confirmation?

**Exit artifact:** surface-parity checklist + packaging/doctor release gate.

---

## 4. Recommended Stage-2 execution order

1. **DIR-01** — establish canonical commit/status truth. Everything else depends on it.
2. **DIR-02 and DIR-03** — resolve the two highest-risk behavioral contradictions: context order and unattended approval.
3. **DIR-04 and DIR-05** — reconcile scope and decision journal; stop relying on D84–D96 until repaired.
4. **DIR-06** — verify hooks/trust cannot bypass policy.
5. **DIR-07 and DIR-08** — audit provider/MCP wire contract and durable authority/redaction.
6. **DIR-09, DIR-10, DIR-11** — VFS/artifacts, Android HAL, memory/skills/compaction.
7. **DIR-12** — surface parity and release gate last, because it depends on all prior verdicts.

**Global acceptance rule:** a direction closes only with an evidence table containing commit, file anchors, test/CI/live result, verdict, and residual risk. No direction may close on prose alone.

**Priority changes if new evidence appears:**

- If `3b021a58` is not on the current branch, treat all 11-27 research findings as **unmerged external evidence**, not current-state proof.
- If live-device proof exists but is unrecorded, DIR-01 and DIR-10 collapse into evidence-location tasks.
- If D84–D96 are found in an unmerged journal, DIR-05 becomes a merge/reconciliation task, not a missing-decision task.
- If `trust.json` is formally accepted as D20 implementation, DIR-06 changes from contradiction to documentation repair.

**Assignment complete. Step 3 directions derived; no Step-3 investigation was executed.**

---

# Step 2 — Unified Subsystem / Decision / Invariant / Gate Matrix (полная таблица)

## 0. Matrix basis

**Decision status is not uniform:**

- **Accepted:** D01–D83.
- **Preliminary:** P01 (extensions).
- **Triage only, not accepted into journal:** D84–D93 (`agent-preliminary-implementation-plan.md:299-303`).
- **No text found:** D94–D96, although `SCOPE_LATEST.yaml:5` claims D01–D96.
- **Duplicate:** D80 appears at `agent-architecture-decisions.md:421` (SQLite session base) and `:550` (fork atomicity limit).

**Review precedence:** code/tests → accepted decisions → implementation plan → handoff → proposals → research. **No GO may be based on docs alone.** Research marked “принято” is still non-normative unless it matches the journal.

## 1. Core runtime and model plane

|ID|Subsystem|Governing decisions|Mandatory invariants, conditions, nuances|Go / No-Go review gate|Stale / contradicting docs|Unresolved questions|
|---|---|---|---|---|---|---|
|**M01**|Product scope, independence, delivery targets|**D01, D02, D03, D37, D71, D83**|Coding-first Rust runtime; Termux/Linux CLI/TUI + personal headless; Android app future, not current; one owner, multi-session, not multi-tenant; OMP/9router not mandatory; one command/event contract; one binary per platform; Termux/Bionic separate artifact; GPL only after concrete license audit.|**GO** if scope, release plan, and actual artifacts agree on targets and non-goals. **NO-GO** if SCOPE says in-scope while release plan silently defers it, or if a Linux ARM64 artifact is presented as Termux.|**M4** local ONNX/fastembed; **M5** kernel sandbox; **M6** 4-platform CI; **M18** D84–D96.|Exact delivery matrix; Android timeline; whether 9router gateway functionality is embedded or optional; license policy.|
|**M02**|Modes, lifecycle, detach, one runtime|**D03, D08, D31, D37, D56, D62, D68**|TUI, CLI, headless, RPC/ACP use one loop; CLI is not a reduced engine; local CLI stays alive while descendants work; detach does not stop runtime; explicit exit stops best-effort; no hidden mandatory daemon; one active managing client; CLI data on stdout, service text on stderr; stopping an answer does not cancel descendants.|**GO** if all surfaces exercise the same runtime and CLI emits clean NDJSON. **NO-GO** if any surface has a separate engine, detach kills work, or service text contaminates stdout.|Plan S10 is partial; no direct contradiction found for the one-loop rule.|Attach/detach transport; wire format and versioning; exact exit keys/signals; future multi-client arbitration.|
|**M03**|Providers, models, streaming, replay, attachments|**D05, D27, D30, D36, D41, D77**|Current stage only OpenAI-compatible API; configurable `base_url`; key from runtime env; Anthropic adapter, OAuth, credential store not in starter; model override by role/task; parent may choose within owner limits; no silent fallback; attachments passed by default, vision errors explicit, no silent text substitution; per-request metrics; secrets never in history/logs/child env.|**GO** if the provider wire preserves all declared fields and replay semantics, and model fallback is explicit. **NO-GO** if reasoning/thinking fields are silently dropped, or Anthropic/OAuth behavior is claimed as starter support.|**M15** reasoning/thinking drop; **M23** Anthropic replay path vs D05/D30.|API dialect, base URL, env var name; retry/timeout/effort limits; signed replay; `filterProviderReplayMessages`; Anthropic adapter status.|
|**M04**|Permissions, policy, approvals, unattended operation|**D04, D15, D20, D29, D50**|Any matching `deny` from global/project/session wins immediately; `allow`/`ask` cannot override deny; `ask` with no manager returns `DeniedUnattended` with **no timer**; model/auto-agent cannot grant permission; project trust does not expand policy; tool/web/file content grants no authority; new deny/redaction applies before next action; running process not silently killed.|**GO** if deny-over-ask-over-allow is proven at every tool seam and unattended ask is tested. **NO-GO** if an auto-answer timer can approve, or project layer can set `allow`/`approvalMode`.|**M11** architecture still describes auto-answer timer; D50 old text remains above the revision.|Exact rule hierarchy; default allow scope; approval persistence; `force_ask` interaction; how “justified assumption” is recorded.|
|**M05**|Tool loop, dispatch, results, edits|**D33, D34, D39, D42, D43, D44, D45, D46**|Persist intent before tool; result after; save failure blocks new model/tool calls; batch support does not imply atomicity or parallel execution; call identity/result mapping preserved; toolset configurable, excluded tools not callable by name; full result artifact plus bounded fragment with truncation marker; stale-read edit returns conflict; formatter is a writer and must not overwrite newer version; build/test/lint are explicit operations, not merge gates.|**GO** if intent/result durability and call identity survive partial batch failure. **NO-GO** if a hidden tool is still callable, or stale edit writes blindly.|D44 hashline is only 16-bit xxh32; not CAS; `write` has no equivalent guard; host write may leave a prefix on error.|Batch schemas and partial errors; artifact thresholds; tool interception rules; formatter defaults.|
|**M06**|Persistence, schema, history, task list|**D31, D32, D33, D34, D47, D61, D80**|SQLite is authority for session/agents/messages/branching/intents/outcomes/deliveries/artifact metadata; large artifacts and snapshots on disk; save is ordinary durable write, not permission/ack; `TurnAck` not mandatory; compaction changes projection only; calls remain part of history; task list in DB separate from model text.|**GO** if schema/migrations preserve intent/outcome and compaction cannot mutate archive. **NO-GO** if save failure continues execution, or compaction replaces the archive.|**M10** duplicate D80 and fork provenance; schema v6 `task_admissions` vs handoff schema v7.|Exact schema/migrations/fsync/WAL; crash consistency; fork atomicity/provenance requirement.|
|**M07**|Recovery, cancellation, resume, unknown outcome|**D08, D16, D47, D49, D50, D68**|Local CLI/TUI does not auto-continue after restart; headless auto-resumes unfinished work by default; explicit stop/cancel is not resumed; missing result means “effect unknown”, not “tool did not run”; tool contract decides safe retry/verification; no invented success; cancel only the specified branch; runtime exit cancels work, detach does not.|**GO** if unknown-outcome recovery cannot auto-retry blindly and requires confirmation when required. **NO-GO** if recovery assumes a missing result means no effect.|**M20** live-device proof and R10 still pending.|Error classes; retry limits/backoff; confirmation UX; post-crash permission/credential recheck; `/retry` semantics.|

## 2. State, workspace, effects

|ID|Subsystem|Governing decisions|Mandatory invariants, conditions, nuances|Go / No-Go review gate|Stale / contradicting docs|Unresolved questions|
|---|---|---|---|---|---|---|
|**M08**|Sessions, projects, workspace ownership|**D21, D22, D23, D24, D56, D64, D70**|Session identity/history/lifecycle independent of project; project association ≠ permissions; one writer per shared workspace; other sessions read or use isolated copy; lock is cooperative, external editor changes must be detected and not overwritten; dialog branch does not revert files; one main task directory; retention never silently deletes unfinished/conflict data.|**GO** if one-writer and external-change detection are proven. **NO-GO** if a second session can silently overwrite shared files.|**M22** D23 initially requires cross-program coordination; 2026-11-09 says it is not mandatory, while exclusive durable-session ownership remains.|Workspace identity, lease granularity/recovery; project/session/workspace cardinality; multi-root support; external-editor race handling.|
|**M09**|VFS, resources, artifacts, URI|**D21, D27, D43, D66, D70, D79, D81, D82; D86 triage only**|Workspace paths are real OS paths; artifact store is separate and addressed by ID/reference; one managed root; credentials outside VFS and not connectable; path filter is not a shell/native sandbox; artifact order: allocate ID → temp file → publish → SQLite metadata; never overwrite existing artifact; incomplete collection marked incomplete; subagent VFS reads saved history.|**GO** if artifact publication cannot expose credentials and read-only resources reject mutation. **NO-GO** if artifact references are treated as OS paths, or URL mutation is allowed without `source_path` and writable.|`PathMap` vs recommended `RootMap`; URI spelling open; docs may imply generic VFS proxy.|URI spelling (D21/D79/D81); collision/streaming output; external path attachment permissions; multiple roots.|
|**M10**|Execution, shell, PTY, process control|**D38, D59, D76; D87 triage only**|In-process builtins where safe; incompatible builtins via same binary child; external toolchains/PTY subprocess; per-agent shell state; cwd/env/functions/options persist between that agent’s calls, not other agents; stop is TERM→KILL best-effort; no transparent cwd/env-only hibernation; saved history does not restore a live shell.|**GO** if process-tree kill, cwd reset, per-agent env isolation, and policy are proven. **NO-GO** if shell state leaks globally or stop only sends TERM.|S3 partial; TERM→KILL and interprocess lease unproven; Android lacks `pidfd`; lease/coordinator evidence incomplete.|Live vs serialized shell (G-shell); exact builtin command list/CLI compatibility; fds/jobs/external processes; crash reset semantics.|
|**M11**|Isolation, parallel writers, merge, checkpoints|**D07, D10, D11, D12, D17, D23, D28, D45, D60**|Shared workspace one writer; parallel writers only in isolated workspaces; three-way base/current/result over actual files, not HEAD/index; no forced ours/theirs/union; conflict writes nothing and preserves variants; mechanism error ≠ conflict; stale target rejected; bounded recompute; checks are separate from merge; no automatic user commits; snapshot does not mutate HEAD/index; formatter/merge writers coordinate with version checks.|**GO** if conflict path writes no file, stale target is rejected, and no automatic commit occurs. **NO-GO** if merge silently chooses ours/theirs, or snapshot mutates HEAD/index.|**M9** SCOPE says `diffy`, decisions/plan retain `similar`; D11 backend/crash recovery still open.|jj vs own store (G-jj); merge engine (G-merge); checkpoint composition; crash recovery; large/symlink/submodule handling.|
|**M12**|Code intelligence, RepoMap, semantic operations|**D13, D35, D75; D91 triage only**|Embedded analyzers preferred; LSP optional, lazy, default-off, explicit owner enable; map enriches navigation but never replaces actual read before edit; index is budget-limited, not dumped wholesale; every backend declares supported operations; unsupported operation must not crash; rename/refactor/resolve requirement remains.|**GO** if capability matrix honestly declares supported language × operation and unsupported paths degrade safely. **NO-GO** if tags/syntax are presented as semantic rename/refactor coverage.|**M13** architecture says LSP not planned/not implemented; decisions allow optional LSP; code defaults `lsp.enabled` true; **M7** SCOPE demands strict semantics but implementation is tags + RepoMap + optional LSP.|Which semantic backend is mandatory, if any; exact language/operation matrix; index freshness; external LSP trust and resource limits.|

## 3. Knowledge, context, configuration

|ID|Subsystem|Governing decisions|Mandatory invariants, conditions, nuances|Go / No-Go review gate|Stale / contradicting docs|Unresolved questions|
|---|---|---|---|---|---|---|
|**M13**|Memory, skills, learned experience|**D14, D52, D53, D58; D89 triage only**|Memory is context, not instructions/permissions; scopes global/project/session; provenance user-asserted vs agent-inferred; user can inspect/correct/delete; deleting memory does not delete history; skills: names+descriptions first, full text lazy, manual invocation, loading does not execute scripts; custom commands are skills; candidates proposed for confirmation by default; automatic mode is configurable and not consent.|**GO** if provenance/deletion are durable and skill loading cannot execute code. **NO-GO** if memory changes policy or skill load runs scripts.|**M8** SCOPE/plan require full experience→skills lifecycle; implementation evidence is memory consolidation/wiring; **M4** local embeddings scope conflict.|Extraction/consolidation algorithms; skill validation/retirement/confirmation; vector index optionality; `local-embeddings` default and policy.|
|**M14**|Instructions, project context, compaction, inspector|**D15, D18, D48, D74; D90 triage only**|Settings layers: built-in → user → project → launch → session; project LLM instructions separate from machine config; text recommendation is not enforcement; compaction keeps full archive and changes only model projection; must not lose current task/constraints/unfinished actions; epoch/revision visible; inspector shows categories/estimates, not message contents; token estimate ≠ provider usage.|**GO** if compaction preserves archive/task and epoch is visible. **NO-GO** if summary replaces archive or current task can be lost.|**M14** docs say user context files last; source yields user-first; D18 defaults and exact strategies still open.|Exact context-file order/scope/AGENTS.md exclusion; compaction triggers/defaults/retained tail; cache-prefix invalidation; inspector export.|
|**M15**|Configuration, role profiles, trust, hooks, extensions|**D15, D20, D29, D51, D57, D66, D69; D93 triage only; P01 preliminary**|Effective config is versioned; display changes immediately, model/tools/budgets at safe boundary; new deny/redaction before next action; full parse+semantic validation before publish; bad reload keeps previous config; trust is explicit and project-scoped; project hooks/MCP/commands only after trust; hooks fail-closed; role profile = Markdown + YAML; project profile does not activate hooks/extensions itself; config UI and CLI use same validation/layers.|**GO** if reload is atomic and no untrusted project code runs. **NO-GO** if project layer can set `allow`/`approvalMode`, or hooks run without trust.|**M12** D57 hooks deferred 2026-11-09, R8 implements them; **M21** D20 says no separate trust registry, implementation adds `trust.json`; plan says hook runtime not chosen.|Trust revoke/reload reaction; hook events/format/order/sync/error actions; role profile fields/paths/reload/precedence; config format; extension trust model.|
|**M16**|MCP client|**D20, D54**|Full MCP client, not tools-only; launch of local stdio server is execution and must pass policy/trust before launch; external MCP optional; rmcp is candidate, not a substitute for compatibility/trust checks; selected capability matrix must work on Linux and Termux/Bionic; server-initiated requests and reconnect are in scope.|**GO** if the declared matrix works and every response preserves structured fields. **NO-GO** if tools-only, or stdio launch is ungated.|**M16** fork lacks resources subscribe/unsubscribe, prompts list/get, `structuredContent`/`_meta`; original protocol `2025-11-25` vs rmcp `2026-07-28`; **M17** transport limits absent from contract.|Required capability matrix; auth; lifecycle/reconnect; resources/prompts; structured output; protocol version policy.|
|**M17**|Network tools, web, attachments|**D40, D41, D04, D27**|Built-in HTTP/web without curl/browser; configurable search backend; internet/localhost/private networks configured separately; redirects/DNS/limits explicit; built-in HTTP is not a sandbox for shell network access; attachments passed by default; vision absence is an explicit provider error, not silent text substitution or model switch; secret protection applies to attachments.|**GO** if destination policy distinguishes internet/localhost/private and redirect behavior is explicit. **NO-GO** if web content grants authority or attachments are silently dropped.|Transport research says redirects forbidden without user approval; D40 leaves redirects/DNS open. Not necessarily contradiction, but contract gap.|HTTP/extraction/search components; redirects/DNS; defaults; vision role routing; attachment size/type limits.|

## 4. Surfaces, platform, security, release

|ID|Subsystem|Governing decisions|Mandatory invariants, conditions, nuances|Go / No-Go review gate|Stale / contradicting docs|Unresolved questions|
|---|---|---|---|---|---|---|
|**M18**|Frontends: TUI, CLI, headless, RPC/ACP|**D03, D25, D37, D56, D60, D62, D63, D65, D73, D74**|One runtime; display mode does not change execution/permissions/redaction; CLI data stdout/service stderr; one active client; worker viewer shows status/history without redirecting input; session search local and not injected into context; inspector redacted; detach does not stop runtime; export/import history excludes credentials but cannot guarantee no pasted secret.|**GO** if TUI/CLI/headless parity and clean NDJSON are proven. **NO-GO** if TUI has separate engine or inspector leaks content/secrets.|S10 partial; Ratatui called candidate while research pins `ratatui 0.30.2`; export/import and inspector unverified; notifications incomplete.|Exact UX/keys; NDJSON schema; attach/detach semantics; export format; configurator scope; multi-client future.|
|**M19**|Notifications, diagnostics, metrics|**D65, D72, D77**|Notification is a signal; task state persists regardless of delivery; local notifications configured; external send off by default; diagnostic logs local/automatic, report manual with explicit confirmation; no usage telemetry; report excludes credentials, history, file content, full tool payloads; metrics per request, estimates labeled, unavailable data not invented.|**GO** if diagnostics cannot auto-send and redaction covers logs/reports. **NO-GO** if history/body/secrets enter report payload.|D72 report composition open; plan says safe payload composition open; notifications hooks incomplete.|Channels/payload/defaults; retention; exact report schema; metric persistence and cost data.|
|**M20**|Delegation, messaging, subagent completion|**D06, D09, D10, D36, D42, D49, D68, D79**|Flexible multi-agent topology; selective task context; async descendants by default; parent may choose model/toolset within owner limits; messages durable in DB, delivered between steps without interrupting tool; admission row before spawn/effect; replay of same `<worker_id>@<producer_generation>` rejected; worker final response template; runtime stores result/context and notifies parent with agent ID and VFS address; wake/receipt ≠ model processed.|**GO** if admission-before-spawn and durable delivery are proven, and replay is rejected. **NO-GO** if spawn precedes admission, or a message is treated as consent.|**M10** `task_admissions` schema v6 vs handoff schema v7; D79 template/VFS address still open; original OMP mailbox is in-memory while fork adds durable admission.|Peer messaging defaults; wake/park semantics; receipt meaning; agent context address; max hops not assumed; delivery confirmation.|
|**M21**|Agent limits and budgets|**D26, D67**|One configurable budget mechanism for steps/retries/time/tokens/cost/parallelism/depth/process/output; descendants count against parent budget; local limits per subagent; on limit, save state, report reason, allow continuation with changed budget; no infinite auto-resume; limit queue counts actively working agents, not stored agents; provider concurrency is separate.|**GO** if exhausted budget saves state and stops cleanly, and every exposed setting is enforced. **NO-GO** if a budget setting is displayed but not enforced.|Exact values, inheritance, enforcement and overrun semantics deferred; active/waiting definition unresolved.|Active/waiting definition; queue order/timeout; budget inheritance; hard-stop limits; cost accuracy.|
|**M22**|Android / Termux platform HAL|**D38, D59, D71, D76; research pins**|Termux/Bionic separate artifact; no universal `execve` guarantee at `targetSdk 28`; `LD_LIBRARY_PATH` only for non-secure Bionic (`AT_SECURE == 0`); `memfd_create`/`execveat` not proven W^X bypass; Landlock probe `SIGSYS` on Android, guarded out; process control without `pidfd`; use `${TMPDIR:-$PREFIX/tmp}`; `sort` Android memory probe fallback; `inotify_rm_watch` cast; kernel sandbox/fastproot deferred.|**GO** if live-device smoke proves process control, temp paths, DB, and artifact execution. **NO-GO** if Android is treated as Linux ARM64 or Landlock is called on Android.|**M19** Android assumptions; **M5** kernel sandbox deferral.|API/NDK floor; W^X strategy; process-group reliability; LKL/fastproot; credential and env isolation.|
|**M23**|Packaging, installation, licensing|**D70, D71, D83**|One binary per platform plus working dirs created at install/init; managed data under D21; secrets under D27; install/update paths explicit; no mandatory external toolchain; GPL components allowed only after concrete license/usage audit; full transitive LICENSE/NOTICE audit still pending.|**GO** if artifact installs and runs on Termux without external toolchain and license obligations are documented. **NO-GO** if Termux artifact is Linux ARM64 or GPL inclusion is unexamined.|**M6** 4-platform CI deferred; **M19** Android artifact; plan says NDK/API floor is compatibility proof, not fixed.|Exact paths/update/assets; NDK/API floor; bundled C policy; license audit completion.|
|**M24**|Security, secrets, redaction|**D04, D20, D27, D32, D34, D50, D55, D72**|Credentials in separate physical directory outside VFS, not connectable as workspace; aliases/symlinks/parent paths considered; credentials separate from project/history/memory; no mass env inheritance to children; redaction before persistence and before next model turn; export excludes credential store but not previously pasted secrets; VFS/trust/path filter are not OS sandbox; same-UID shell/native code limitation acknowledged; diagnostics counts only.|**GO** if secret paths cannot enter history/logs/child env and redaction is tested at all write/send boundaries. **NO-GO** if credentials enter VFS/history or child env mass-inherits secrets.|R0.1 covers only selected tables; doctor/report payload composition open; legacy redaction flag.|Encryption/master key/unlock; credential broker; subprocess isolation; redaction coverage; legacy redaction procedure.|
|**M25**|Release, CI, verification, status truth|**D12, D16, D33, D47 + specs**|CI uses local fixtures; live external smoke is optional/gated; R1–R3 live-device proof required before R9/R10; “done” needs commit + CI or live proof as applicable; release plan cannot override task/story evidence; checks are not correctness proof.|**GO** only after status ledger is reconciled and required live proof exists. **NO-GO** if release plan marks done while tasks are failing or live proof is pending.|**M1–M3** status/HEAD conflicts; **M20** live proof pending; **M4–M9** scope drift.|Authoritative HEAD and status source; R9/R10 execution; live R0.4 smoke; CI matrix scope.|

## 5. Conflict register — exact anchors

|Code|Conflict / stale statement|Exact evidence|Verdict|
|---|---|---|---|
|**M1**|Four ledgers disagree on active work: `state.yaml` says e10; `execution-status` says e01s02 and e01s04; `epic.yaml` says e01s04; `release-plan` says e01 in_progress.|`specs/state.yaml:2`; `specs/execution-status.yaml:2,4`; `specs/epics/e01-r0-mcp/epic.yaml:6-7`; `specs/release-plan.yaml:19-26`|Unresolved; status truth missing.|
|**M2**|`e01s01`/`e01s03` tasks marked failing while stories/history say done; `e01s02` tasks done but execution-status in_progress.|`e01s01-tasks.yaml`; `e01s03-tasks.yaml`; `e01s02-tasks.yaml`; `execution-status.yaml:2`|Unresolved; story/task status models not reconciled.|
|**M3**|HEAD ambiguity: handoff/state/release line uses `54f0fb1`; 11-27 research anchors `3b021a58`.|`agent-work-handoff.md`; `docs/research-2026-11-27-01-omp-sync.md`|Must resolve before trusting 11-27 findings.|
|**M4**|SCOPE says local ONNX/fastembed out of scope; release plan e06 implements opt-in `fastembed`; CI checks `--features local-embeddings`.|`SCOPE_LATEST.yaml:206`; `release-plan.yaml:66`; `agent-preliminary-implementation-plan.md:49,92`|Real scope contradiction.|
|**M5**|SCOPE R3 includes seccomp-BPF + Linux landlock/bwrap; implementation defers kernel sandbox and `process-wrap`.|`SCOPE_LATEST.yaml:61-62,232`; `release-plan.yaml:50`; plan `:27,90,522`|Deferred without SCOPE amendment.|
|**M6**|SCOPE R8 includes four-platform CI; implementation defers macOS/Windows; actual Linux + Android only.|SCOPE R8; `release-plan.yaml:90`; plan `:94`|Deferred without SCOPE amendment.|
|**M7**|SCOPE R4 requires strict semantic preservation incl. rename/refactor; actual path is tags + RepoMap + optional LSP; no rust-analyzer/Oxc/SCIP evidence.|SCOPE R4; plan `:48`; decisions `:512`; D91 triage plan `:301`|Semantic requirement not demonstrated.|
|**M8**|SCOPE R5 requires full experience→skills pipeline; implementation evidence is memory store/FTS/embeddings + `consolidate()`, not full skill validation/retirement.|SCOPE R5; plan `:49`; S9 `:243`; `release-plan.yaml:66`|Requirement not met as written.|
|**M9**|SCOPE R7 names `diffy` 3-way merge; decisions and plan keep `similar` and reject duplicate `diffy`.|`SCOPE_LATEST.yaml:107`; plan `:31,93`; decisions `:471,525`|SCOPE stale; `similar` is current decision.|
|**M10**|D80 appears twice: session base vs fork atomicity; later text says provenance not required while doctor/plan require `fork_provenance`; task_admissions schema v6 vs handoff schema v7.|decisions `:421`, `:550`; plan doctor `:101`; D49 schema v6; handoff schema v7|Duplicate decision; provenance requirement unresolved.|
|**M11**|D50 revision removes timer/auto-answer and returns denial without user; architecture still describes auto-answer timer.|decisions `:277+`; `agent-architecture.md` D50|Architecture stale.|
|**M12**|D57 says hooks deferred 2026-11-09; R8 implements subprocess JSON hooks; plan §6 still says hook runtime/format/sync not chosen.|decisions `:313`; plan `:32,47,94,299`; `release-plan.yaml:90`|Journal/plan stale relative to implementation.|
|**M13**|Architecture says “LSP не планируется” and “LSP пока не реализуется”; decisions later allow optional lazy LSP; code defaults `lsp.enabled` true; SCOPE says optional default-off.|decisions `:75,77,512`; `agent-architecture.md:472`; SCOPE R4; plan R4|Three-way contradiction: docs vs decision vs code.|
|**M14**|Docs say user context files last; source yields user-first because dedupe uses `depth ?? Number.POSITIVE_INFINITY`.|`docs/context-files.md:93-95`; `src/system-prompt.ts:425-435`, `:476-484`; `docs/project-prompt.md:8-15`|Source wins; docs wrong.|
|**M15**|Provider contract says no silent drop; OpenAI reasoning/thinking wire fields dropped.|provider/MCP contract; `provider.rs:1380-1400`|Contract violation unless explicitly documented.|
|**M16**|MCP contract requires full matrix; fork lacks resources subscribe/unsubscribe, prompts list/get, `structuredContent`/`_meta`; protocol `2025-11-25` vs rmcp `2026-07-28`.|D54; provider/MCP proposal; fork MCP client; MCP research|Capability/protocol gap.|
|**M17**|Transport limits: stdio >16 MiB dropped without close; HTTP error body 64 KiB; redirects forbidden without approval. Not reflected in normative contract.|`research-2026-11-06-providers-mcp.md`; contract proposal|Contract omission.|
|**M18**|SCOPE claims D01–D96; journal ends D83 + P01; D84–D93 are triage only; no D94–D96 text.|`SCOPE_LATEST.yaml:5`; decisions journal; plan `:299-303`|SCOPE references nonexistent/accepted decisions.|
|**M19**|Android assumptions: `targetSdk 28` + `execve` universal; `LD_LIBRARY_PATH` general; `memfd_create`/`execveat` W^X bypass; `sort` uses `available_memory` only Linux; Bionic inotify cast; temp path.|Android research 03/04/10; `crates/pi-builtins/src/sort.rs:113-120`; `dirs.rs`|Several assumptions are false or unproven.|
|**M20**|Release plan marks R1–R8 done, but live-device proof for R1–R3 and R0.4 live smoke are pending.|`state.yaml:10-11`; `release-plan.yaml:10`; plan `:38`; `e01s04-tasks.yaml`|“Done” is code/CI done, not live-proof done.|
|**M21**|D20 2026-11-09 says do not introduce separate trust registry; R8 implements `<agent_dir>/trust.json`.|decisions D20, implementation note; plan `:47`; `release-plan.yaml:90`|Decision text stale relative to implementation.|
|**M22**|D23 initially requires cross-program workspace coordination; 2026-11-09 says cross-program coordination is not mandatory, while exclusive durable-session ownership remains.|decisions D23 original + 2026-11-09 update|Nuance unresolved: mandatory single-session ownership vs optional cross-program coordination.|

## 6. Global Go/No-Go rules

1. **No GO from prose.** Every row requires code/test or live evidence.
2. **No GO on D84–D96 as governing decisions** until the journal is updated. Use D84–D93 only as triage direction.
3. **No GO if a fallback deletes the requirement.** Deferred scope must be explicit in SCOPE/release plan, not hidden in implementation notes.
4. **No GO if a security invariant is only enforced in the UI/model.** Deny, redaction, trust, and secret boundaries must be host-enforced.
5. **No GO on “done” without the required proof type:** CI for build/tests, live-device smoke for Android/process/temp/MCP, and explicit unknown-outcome handling for recovery.
6. **No GO if status sources disagree.** Reconcile `state.yaml`, `execution-status.yaml`, `epic.yaml`, and `release-plan.yaml` first.
