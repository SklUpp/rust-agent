# Целевое исследование Stage 2: DIR-03, DIR-06 и пробелы политики/траста (`hub start`, DAP) (`TargetDir03ApprovalSafety`)

Дата: 2026-11-27  
Источник: сырой отчёт `TargetDir03ApprovalSafety` (только чтение кода и документации).

---

# DIR-06 + errata — hooks/trust/hub/DAP (with DIR-03 carried forward)

No files modified; no cargo commands run. All anchors are current working tree.

---

# Part A — DIR-03 recap (required final coverage)

## A1. Cancellation reaches approval gates

`pi-agent/src/tools/scheduler.rs:196-199` races approval against `ctx.cancellation_token.cancelled()`; approval denial becomes `approval cancelled`, no `execute`. Post-approval and pre-admission checks at `:210-230`; durable admission rechecks at `:268-281`.

Per surface:
- **CLI/headless**: blocking reader polls a cancellation flag every 100 ms; non-terminal stdin is immediate `Err("approval required but stdin closed")` (`approve.rs:563-566,606-634`).
- **TUI**: Ctrl-C `fail_dialogs` replies `false` (`tui.rs:1630-1660,2875-2880`).
- **RPC**: `abort`/`abort_and_prompt`/shutdown call `ui.cancel_all()` (`rpc/mode.rs:894,999-1013`) → pending `confirm` resolves cancelled.
- **ACP**: same turn token stored at `acp/session.rs:1202-1203`; checked at `:2217`; `request_cancellable` at `:2230`; broker spawns in `spawn_blocking`.
- **Children**: `Approvals::without_ask()` (`runtime.rs:2030`); child token aborts.
- **Bare `pi-agent`**: `ToolScheduler::new` has `approver=None` (`loop.rs:890-920`, `scheduler.rs:29-35`) → Ask-tier tools execute without approval.

Tests: `scheduler.rs:554,580`; `pi-agent/tests/hooks_runner.rs:180`.

## A2. Timers

No auto-answer timer/agent in code or schema. Approval waits unbounded (CLI poll, ACP `request_cancellable(..., None)`, RPC `RpcApprovalInput::new(ui, None)`, TUI `rx.await`). `ask.timeout` auto-fills only the `ask` tool. ACP 5 s cancel barrier is cleanup, not answering. `security_runtime` yolo is explicit and restricted to read-only tools.

## A3. `DeniedUnattended`

No literal code identifier. Equivalent is closed-stdin immediate denial. An attached idle TTY waits forever; bare `pi-agent` has no gate at all. `tools.approval` is advertised in `settings-schema.snapshot.json:3638-3647` but only `tools.approvalMode` is parsed (`config.rs:2364-2368`) — per-tool allow/prompt/deny is dead config.

## A4. `force_ask` / `yolo` / `--allow`

`approve.rs:482-536`: policy `Forbidden` hard-denies first; `forced_prompt = verdict==Prompt || hint.force_ask` ignores matrix/allow/yolo; hook allow only if not forced; ACP honors hints, cached `AllowAlways` does not satisfy forced prompt (`acp/session.rs:2210`). TUI `TuiApprover` lacks `supports_approval_hints` (`tui.rs:90-125`) → policy-forced prompts and `PreToolUse:ask` are denied in TUI, not surfaced.

## A5. Stale D50 passages

- `docs/agent-architecture.md:91` — stale auto-answer timer text: «настраиваемый таймер до 10 минут может передать его отдельному агенту для автоответа… Точный backend, поведение при отказе и разрешение гонок ответов ещё открыты.» Contradicts the D50 revision.
- `docs/agent-architecture-decisions.md:279` — superseded pre-revision direction, not marked historical.
- `docs/agent-architecture-decisions.md:283` — still frames «поведение при отказе/недоступности автоагента, гонка ответа пользователя с таймером» as open. Revision at `:281` is authoritative: timer and auto-answerer cancelled, no-manager gate returns denial.

---

# Part B — DIR-06: hooks runtime

## B1. Implemented contract

`pi-agent/src/hooks.rs`:

- Events: `PreToolUse`, `PostToolUse`, `PermissionRequest`, `Stop`, `SessionStart` (`:38-78`).
- Input: one JSON object on stdin. Emitted fields:
  - lifecycle: `hook_event_name`, `session_id`, `cwd`;
  - tool events: `tool_name`, `tool_input`;
  - Post: `tool_output`, `tool_is_error`;
  - Stop: `stop_hook_active` (always `false`);
  - SessionStart: `source:"startup"`.
  No `transcript_path`, `permission_mode`, `tool_use_id`.
- Output:
  - exit `0`: optional JSON (`continue`, `decision`, `reason`, `systemMessage`, `hookSpecificOutput.permissionDecision|permissionDecisionReason|updatedInput|additionalContext`);
  - exit `2`: blocking, stderr preferred then stdout (`blocking_reason`, `:751-796`);
  - other exit/timeout/spawn error: failure (`:798-816`).
- `updatedInput` shallow-merges object input and feeds the next hook (`:438-460,884-895`).
- `PreToolUse allow` is parsed but **not used** by the scheduler to bypass approval; only `deny`, `ask`, `updatedInput`, context are consumed (`hooks.rs:430-460`, `scheduler.rs:155-173`). Policy remains authoritative.
- `PermissionRequest` runs only when the approval matrix reaches `Ask`; `allow` overrides matrix but not `Forbidden`/forced prompt; `allow`+`ask` resolves to `Ask`; no decision from a matched hook also resolves to `Ask` (`hooks.rs:520-575`, `approve.rs:500-518`).
- `PostToolUse` runs after execution; `blocked` only appends `"Hook feedback"` to the result, does not undo the effect or set `is_error` (`scheduler.rs:297-312`).
- `Stop` runs once per turn before accepting model stop; block injects reason and continues; `stop_hook_active` is initialized false (`loop.rs:1382-1383`) and never passed as true (`loop.rs:1967-1973`).
- `SessionStart` runs once per `HookEngine` via `OnceCell`, cached across turns (`hooks.rs:397-411`); invoked at `runtime.rs:2434`. Block/deny/failure has no effect; only messages/context are summarized — `failClosed` is not consulted for this event.

Execution:
- Serial, awaited in rule order (`run_event`, `hooks.rs:591-601`); no parallel hooks.
- `sh -c` for string `command`, direct argv for `argv` (`:644-678`).
- Per-rule timeout default `10_000` ms; keys `timeoutMs`/`timeout_ms` are ms, `timeout` is seconds (`:276-288`); no upper cap, minimum 1 ms.
- Timeout and cancellation kill the whole process group (`:660-700,917-945`); stdout/stderr capped at 1 MiB each.
- `failClosed` defaults **false** (`:289-292`): spawn error/timeout/non-0/non-2 is advisory + warning, unless configured closed. Exit 2 always blocks regardless.
- Hook processes inherit the full parent environment and project cwd (no `env_clear`, no env allowlist) — trusted hooks see all process secrets.

## B2. Registration and fail-closed behavior

- Loaded from trusted sources only (`config.rs:1236-1270`):
  - home `.claude/settings.json`, `.claude/settings.local.json`, `.codex/hooks.json` (operator-owned; files resolving inside the project root are rejected);
  - project identical set only when `trust_project` is true.
- Project `.omp/settings.json` / `.omp/config.yml` hooks are merged by `parse_hooks` only after trust filtering (`config.rs:2100-2120,1645-1655`).
- Malformed hook files are skipped with a warning; a malformed trusted project file silently drops that file’s hooks (availability, not escalation).
- Order: base config hooks → project `.omp` hooks → discovered home `.claude`/`.codex` → discovered project `.claude`/`.codex`; `HookConfig::merge` appends, so later layers run later (`hooks.rs:155-160`).
- `PreToolUse deny` cannot be bypassed by approval; `PermissionRequest` cannot bypass bash policy or `Forbidden`; forced prompts still reach the alternate approver only if it supports hints.
- `SessionStart` failure/denial is not fail-closed; `PostToolUse failClosed` cannot prevent the already-executed effect.

## B3. Trust store and drift (`trust.rs`)

- Store: `~/.aragent/trust.json` (configurable in tests), versioned; directory `0700`, file `0600`; atomic temp+rename (`trust.rs:210-260`).
- Fingerprints: `TRACKED_SURFACES` (40 project surfaces) plus selective hook blocks in `.omp/settings.json` and `.omp/config.yml`; each entry is blake3 hash of file bytes, missing files omitted (`trust.rs:52-104,138-180`).
- Additions/removals are detected because the whole map is recomputed on every verify; unrelated edits to `.omp` files do not invalidate; unknown/unparseable hook blocks invalidate via full-text hash.
- Grant computes and stores fingerprint; verify reloads store and recomputes; revoke removes by canonical path with lexical fallback; corrupt/invalid/wrong-version store yields empty (fail closed) (`trust.rs:200-390`).
- `workspace_is_trusted` short-circuits only for the current PID, then always re-verifies fingerprint; a moved/renamed directory loses the canonical record.
- Revocation/drift propagation:
  - SSH: rechecked per resolution (`main.rs:969`);
  - LSP: rechecked per request (`runtime.rs:1817-1821`);
  - hooks: **not live** — loaded into `Approvals` at startup (`main.rs:1510-1513`), RPC new-session only rebinds the session id around the already-loaded config (`rpc/session.rs:1427`);
  - MCP: discovery/launch at session setup; a launched server is not stopped by revoke.
  So `trust revoke` affects future processes/new loads, not hooks already running in the current session; fingerprint drift likewise does not disable loaded hooks until reload/restart.
- MCP launch gate (`runtime.rs:6050-6090`): project-discovered servers are marked untrusted; stdio spawn requires workspace trust plus the qualifying yolo/explicit-allow posture, otherwise the server is reported unavailable and not spawned. D20 acceptance “spawn without trust — refusal before launch” is met.
- Revocation caveat: a grant whose recorded fingerprint is empty (project had zero tracked surfaces) can still verify as trusted for a now-missing path, because missing files and an empty manifest compare equal.

## B4. Hub `start` BashPolicy bypass (confirmed)

- Tool name `hub` (`pi-agent/src/tools/hub.rs:522`); `start` builds `ProcessSpec { application, args, env, cwd, … }` and consults no policy (`hub.rs:955-987`).
- `classify_tool` falls through to `_ => Exec` (`approve.rs:193`), so hub requires matrix approval by default.
- `bash_verdict` returns early unless `name == "bash"` (`approve.rs:447-449`), so `bash.patterns` deny/prompt and built-in destructive rules never inspect hub `application`/`args`.
- Consequence: `--allow hub` or `yolo` auto-approves arbitrary hub `start` commands that equivalent bash policy would deny/prompt. `PreToolUse` hooks matching `hub` can still deny, but are not a substitute for policy. Process backend does use `env_clear` + allowlisted env (`tools/process.rs:2684-2687,2734-2738`), so the gap is command policy, not env leakage.

## B5. DAP project trust/env gaps (confirmed)

- Project discovery: `dap/config.rs:343-370` walks `cwd` plus ancestors and home for `.omp/.claude/.codex/.gemini` DAP config, merged cwd-wins; `get_adapter_configs(cwd, env)` has **no trust parameter** and no `workspace_is_trusted` check (`:542-565`).
- Project config controls adapter `command` and `args` (`AdapterConfig`, `:150-170`). Launch resolution searches project root/cwd for bare commands (`resolve_adapter_for_launch`, `:790-800`), so a project-local executable named like an adapter can be selected.
- Adapter spawn (`dap/client.rs:761-791`) does **not** call `env_clear`: full parent environment is inherited, then non-interactive vars and `ctx.env` overlaid. Debuggee/`runInTerminal` (`dap/session.rs:2515-2545`) also inherits parent env and lets the adapter append arbitrary env vars and launch an arbitrary program/args after the session is open.
- `setsid()` return value is ignored in `pre_exec` (`client.rs:781-790`).
- Debug approval detail shows only `action` + `program` (`approve.rs:224-236`), not the resolved adapter command. An approved `launch` can therefore execute a project-substituted adapter; once a session exists, further read-only debug actions need no prompt and the adapter can issue `runInTerminal`.
- DAP config files are **absent from `TRACKED_SURFACES` and hook blocks**, so changing a project DAP config neither invalidates trust nor triggers any fingerprint warning.

## B6. Stale D57/D20 journal text

- `docs/agent-architecture-decisions.md:317` (D57) — still normative: «Hooks (D57): пользовательские hooks … отложены … «механизм и hooks runtime» — отдельные решения.» No revision/closed note, contradicted by `:526` («R8 Hooks реализуется по единому JSON stdin/stdout контракту…») and `specs/release-plan.yaml` R8 close.
- `docs/agent-preliminary-implementation-plan.md:226-230` (S6) — «**Статус 2026-11-18: partial.** … hooks-seams и исполняемый hooks runtime не сделаны» and «исполняемый hooks runtime (D57 целиком) — отдельный этап…». Stale relative to plan R8 row and state block (`R4–R8` complete).
- `docs/agent-preliminary-implementation-plan.md:286` (§6 «Отложено») — still lists hooks runtime plus «trust-хранилище/revoke/config-reload reaction (D20/D29)» as deferred. Store/revoke/reload exist in code, so the list is stale.
- `docs/agent-architecture-decisions.md:119` (D20, 2026-11-09) — «отдельного реестра/поле в конфиге не заводить». Superseded by the explicit implementation note at `:121` (`.omp/trust.json`, project MCP as untrusted, hooks gated), but `:119` remains unmarked normative text.
- `docs/agent-architecture.md:185` — still says hook mechanisms/format/runtime are decided later; superseded by R8 subprocess-JSON choice.

Conclusion for D57 feasibility: implementation is technically feasible and already present; blockers are docs status, not the code contract (five events, serial subprocess JSON, failClosed, trust gating all implemented). Remaining contract defects: `stop_hook_active` never true, `SessionStart` ignores failClosed, `PreToolUse allow` is a no-op, hooks inherit full env.
