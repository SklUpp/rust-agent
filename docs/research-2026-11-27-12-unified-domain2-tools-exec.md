# Унифицированный аудит — Домен 2: Execution, Tools, Shell/Process Isolation, Task/Hub, Workspace Writer, `pi-iso`/`pi-vcs`, `fastproot`/LKL (`ScoutUnifiedToolsExec`)

Дата: 2026-11-27  
Источник: сырой отчёт `ScoutUnifiedToolsExec` (только чтение документации и исходного кода).

---

## A. Done / verified in live code

**Execution shell**
- `BashTool` now defaults to a persistent `pi_shell::Shell` from `AgentContext::shell`; one-shot is explicit `BashEngine::LegacyOneShot` (`pi-agent/src/tools/mod.rs:660–695`). Background `bash` inherits shell cwd/env via `snapshot_state()` (`:372–400`).
- `AgentShell` is one per agent, created on first use from explicit `AgentContext::env` (`pi-agent/src/state.rs:1676–1712`); child agents construct their own.
- `pi_shell::Shell`: dirty `in_flight` generation discard, cancellation discards session, `exit`/`break`/`continue`/`return` reset session, `session_reset` reported; calls serialized per session (`pi-shell/src/shell.rs:221–235,397+`).
- Env hardening implemented: `do_not_inherit_env(true)`, profile/rc skipped, allowlist `PATH/HOME/TMP*/LANG/TERM/PREFIX` etc. (`:834–845,991–1015`); `SSH_AUTH_SOCK` excluded in agent env allowlist (`state.rs:1642–1661`).
- `BashPolicy` is live: prefix/host-executable model over `brush-parser`, segments/subshells/command-substitution walked, destructive MIT rules, user rules, `explicit_allow` (`pi-shell/src/policy.rs:240–430`). Wired before approval matrix; `Forbidden` cannot be overridden by yolo/`--allow`/ACP (`aragent/src/approve.rs:482–500`).
- `process.rs` matches docs: Android pidfd gated off, `/proc` start-time identity, descendants post-order, TERM→KILL with re-walk, protected harness PIDs, group kill (`pi-shell/src/process.rs:1380–1570`).
- `process_broker`: token file 0600 `O_EXCL`, constant-time compare, socket owner/mode check, lease/flock, long-path fallback; live auth rejects wrong token (`process_broker/protocol.rs:529–600`, `broker.rs:824–840`).

**Tools**
- `builtin_tools()` = bash/read/write/edit/glob/grep/ast_grep/ast_edit/todo (`tools/mod.rs:4011–4035`). Registry no longer imports it in production; `with_builtins` is `#[cfg(test)]` (`registry.rs:10,48–57`) — extraction design done.
- `EXCLUDED_TOOLS` enforcement: `computer` gets boundary diagnostic, not “not found” (`registry.rs:12–27,109`).
- `todo` restores from newest `user_todo_edit` or `todo` `details.phases` (`todo.rs:1–9`; `aragent/src/runtime.rs:5236,5336,7088`).
- `ask` exclusive, no channel → cancels token + `Aborted`, never reads stdin (`ask.rs:404–425`). Availability gate via `ask_available` (`runtime.rs:5143,5183–5189`).
- `hub` implements jobs/cancel/wait/send/inbox/list/start/ps/logs/stop/restart/describe; owner-scoped jobs, flat peer namespace, durable delivery receipts (`hub.rs:1–18,119+`).
- `task` has durable child admission, producer generation, background spawn/settle paths (`task.rs:510,1533,3338`), but see B/C.
- `workspace_writer`: per-file `flock`, full `replace_if_unchanged`, documented no cross-file atomicity (`workspace_writer.rs:1–23,125–153`).
- `write` now stages via `PendingStore` and applies through `xd://resolve`; `staged_ops` verifies base bytes/dev/ino and has explicit partial-failure model (`staged_ops.rs:985–1115`).

**Isolation / VCS**
- `task_isolation`: baseline = HEAD+staged+unstaged+untracked with size cap; non-git real 3-way merge using `similar::TextMerge`; git path uses `git apply --binary` preflight; `[AUTO-APPLIED]`/`[PENDING_APPLY]`; `conflict://` registration; process-wide `MERGE_LOCK` only (`task_isolation.rs:1–50,77–86,1791–1799`).
- `pi-vcs`: `gix` used, `ApplyOptions.three_way = false` by default (`pi-vcs/src/types.rs:224–250`); branch mode uses real `git cherry-pick` (`task_isolation.rs:3600–3703`).
- `pi-iso`: Linux backends cfg `target_os="linux"` only; on Android `auto_order()` = `[Rcopy]`, explicit reflink/overlayfs/btrfs/zfs unavailable (`pi-iso/src/lib.rs`). `Rcopy` 1 GiB cap, skips escaping symlinks/FIFOs.
- `worktree_cmd` matches its safety contract: never deletes; quarantines to `.trash-<date>`, refuses live isolation owner, rewrites git pointers atomically (`worktree_cmd.rs:1–63`).

**Eval**
- Persistent kernel: separate process, `env_clear`, filtered env, `setsid`, protocol channel dup’d, TERM/KILL group, bounded frames (`eval/kernel.rs:1–90,160–180,519–544`). Explicitly not a sandbox.
- Probe model fixed at doctor level: hidden `__probe <syscall>` in its own child; SIGSYS/E* mapped separately; probe cache read-only unless refresh (`doctor.rs:58,2429–2478,2312–2363`). Eval probe also own session + `killpg` (`eval/probe.rs:93–150`).

**Deferred**
- No `fastproot`/LKL/landlock/bwrap code in repo; only docs + doctor probes. Correctly deferred.

---

## B. Doc/code drift and partial gaps

1. **Bash persistent shell docs stale.** Prior research described pinned fork as one-shot with `session_env: None` and full ambient env. Live code: default persistent shell, one-shot only explicit legacy; env allowlisted. Update R3/shell docs.
2. **`snapshot_path` supports sourcing but no generator.** `pi_shell` sources a snapshot if supplied (`shell.rs:1023–1055`), but no production snapshot writer found; persistent state is in-memory only. D87-compatible, but any doc claiming snapshot restore is unimplemented.
3. **`todo` is not a dedicated DB table.** State is tool-instance `RwLock`, restored from session log/custom entry (`todo.rs`, `runtime.rs:7088`). D61 says “saved task list in DB”; clarify “session store entries” unless a table is added.
4. **`task` durability contradiction.** `task_admissions`/`admit_child_durable` exist (`task.rs:510,1533`), but scheduler still rejects every `task` call when durability active (`scheduler.rs:233–249`). Admission path is unreachable through the scheduler. Docs record this; it remains a real gap.
5. **Bash policy breadth vs upstream.** Live `BashPolicy` covers destructive rules and prefix semantics, but docs/TS list extra lineage (`CRITICAL_BASH_PATTERNS`-style tier escalation, git guard). Live has no git guard and no per-command “prompt override” equivalent beyond `force_ask`. Treat policy port as partial, not full upstream parity.
6. **`write` bulk no longer bypasses version gate.** Earlier audit said bulk write lacked version gate; live `WriteTool` stages every write and applies full base-byte + dev/ino check. Update findings.
7. **Branch/patch merge docs outdated for non-git.** Live non-git path performs real 3-way `similar` merge (docs older B6 said patch mode only). Git patch mode still `ApplyOptions.three_way=false`; Gix may return conflict and pending artifact.
8. **Hub scope larger than docs.** Durable peer deliveries, revoke/withdraw, idle wake, revived receipts now exist (`hub.rs`, `durable.rs:171–234,419–424`). Docs describe only the basic mailbox/durable delivery model.
9. **`local_cmd.rs` is rule-evaluation CLI, not shell execution.** It evaluates TTSR/AST rules and “local” command classification; no `BashPolicy`, PTY, or sandbox wiring there. Any doc implying it owns local shell execution is wrong.
10. **Eval env differs from D27 shell policy.** Eval allowlist includes `SSH_AUTH_SOCK`, `SSH_AGENT_PID`, `SSH_CONNECTION`, `SSH_CLIENT`, `SSH_TTY` (`eval/env.rs:44–56`), whereas agent env deliberately excludes `SSH_AUTH_SOCK`. Eval cells get an SSH credential capability; this is an unresolved policy inconsistency.
11. **Android Rcopy-only is hard fallback, not probe-ordered.** `pi_iso` still cannot attempt fuse-overlayfs/reflink on Android; docs’ “probe-order” recommendation is not implemented.
12. **No cross-process merge serialization.** `MERGE_LOCK` is `static StdMutex` within one process (`task_isolation.rs:86`); two runtimes can mutate the same parent repo concurrently. Docs note this; still open.
13. **No bounded recompute evidence.** Target-drift detection exists via preflight/base checks, but no iterative “recompute merge on target change” loop found. Integration conflicts/pends instead.
14. **`persist`/`detached` only via broker.** Local `ProcessManager` rejects them (`tools/process.rs:857–863`); broker-owned manager keeps detached processes alive. Docs’ D59 nuance matches, but any claim local background survives runtime exit is false.
15. **Doc claim about eval combined probe** is now partly obsolete: doctor’s `__probe` isolates each syscall and distinguishes SIGSYS (`doctor.rs:2312–2350`). Keep the warning for raw in-process probes only.

---

## C. Pending / deferred, exact conditions

1. **Live Termux/Android smoke** — required before declaring R1–R8 production-ready. No run recorded; docs explicitly pending.
2. **`task` under durable sessions** — blocked by scheduler gate. To enable: remove gate only after worker turns are inside the durable boundary; `admit_child_durable` must cover child loop/turn persistence, not just spawn admission.
3. **Kernel cross-process serialization for merge/apply** — add file lock or broker lease around parent repo mutation; current `MERGE_LOCK` is process-local.
4. **Shell snapshot generator** — needed only if live-shell state must survive restart. Must not claim cwd/env-only reconstruction restores functions/options/jobs (D87).
5. **Git patch 3-way** — current `ApplyOptions.three_way=false`; conflict returns pending. Enable only with explicit `--3way`-equivalent and tests for binary/baseline behavior.
6. **Android isolation probing** — replace forced `Rcopy` with runtime probe order (`fuse-overlayfs`, reflink) when available; keep `Rcopy` fallback.
7. **Bash policy parity** — port/decide git guard and upstream tier-escalation rules; keep deny > ask > allow/default and `DeniedUnattended`-without-user semantics.
8. **Eval credential scope** — decide whether eval cells retain `SSH_AUTH_SOCK`/SSH vars. If D27 applies, move them behind explicit opt-in.
9. **Hub/task UI** — durable delivery, parked revive, and child completion are implemented; docs/UX need alignment with saved-history/inbox semantics.
10. **fastproot/LKL** — remain deferred after R8. Preconditions unchanged:
    - fastproot: fix `g_child_pid`/PDEATHSIG, TTY/setsid tree kill, `.fp-*` cleanup, RUNPATH.
    - LKL: full syscall mediation or explicit compat-only status, `SO_PEERCRED`+token, path allowlist, FD < `VFD_BASE`.
    - kernel sandbox: Linux-only landlock/bwrap after capability probes; never claim Android sandbox from LKL/PRoot.
11. **Doctor probe expansion** — `PROBE_NAMES` currently 4 (`pidfd_open`, `seccomp`, `landlock_create_ruleset`, `memfd_create`); `unshare_user`/`seccomp_unotify` helpers exist but are not all in the cached set. Add only if backend selection needs them.
12. **macOS/Windows execution** — tools matrix still blocked by build/tool availability; `pi-shell` Windows branches exist but `BashTool`/policy/proc semantics unproven. Docs defer.

---

## D. Open decisions and risks

1. **Task durability ownership**: enable `task` under durable sessions vs keep hard block. Current code is contradictory; choose one and remove dead path.
2. **Process-wide vs broker-wide locks**: decide whether merge/apply serialization belongs in `pi-agent` or `process_broker`. Current process-local lock is insufficient for multi-runtime.
3. **Persistent shell lifetime**: in-memory session only vs snapshot persistence. D87 forbids pretending a live shell is fully restorable; choosing snapshots requires explicit degraded-state contract.
4. **Eval as capability boundary**: eval is not a sandbox and currently exposes SSH agent vars; decide whether it is “trusted local tool” or must obey D27. Risk: credential capability leak.
5. **Android isolation**: `Rcopy` is correctness-safe but expensive; accepting hard fallback vs probe-order is a performance/capability tradeoff, not security.
6. **BashPolicy completeness**: destructive-command prefix matching is not a full shell classifier. Risk: compound commands, wrappers, encoded payloads. Keep human approval for high-risk cases; do not advertise sandbox.
7. **`hub`/`task` semantics**: live hub supports restart/revive/stop/wake beyond docs. Risk: stale docs cause wrong assumptions about “completion ≠ acceptance” and detached background behavior.
8. **LKL temptation**: current PoC is not a security boundary. Any “kernel sandbox” claim before syscall mediation/auth/FD safety is a security regression.
9. **Doc drift control**: research docs describe prior fork states; they must be marked historical or updated, otherwise agents will implement against obsolete assumptions (one-shot bash, all-env inheritance, no policy, no staged write).
10. **Verification debt**: no live Android build/smoke for persistent shell, PTY, `git apply/cherry-pick`, Rcopy, process tree kill, or eval kernels. Treat CI-green as compile/test evidence, not device proof.
