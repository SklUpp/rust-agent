# Блок B: execution substrate — evidence

Дата: 2026-11-06. Read-only source research. Нет кода/сборки/тестов/лицензионного заключения.
Оригинал и форк разведены строго: **O** = `can1357/oh-my-pi@035e4f95c314f9aff2f5a779fa335422680f8e36` (MIT), **F** = `SklUpp/omp-rust-termux@3008cfd0ca88bb0e28323561a165f39efa7159e1` (MIT).

**Легенда якорей.** `[P]` — проверено по pinned source (URL/файл на 3008cfd). `[WT]` — прочитано только в рабочем дереве `omp-work` HEAD `32796e32` (потомок 3008cfd, ветка `feat/rust-agent-termux-bionic`); такой факт не доказывает состояние 3008cfd и не должен цитироваться как pinned. Номера строк `[P]` — из pinned файла. Наличие/отсутствие F-крейтов и модулей сверено по pinned tree: в 3008cfd нет `pi-edit`, `pi-vfs`, `pi-diff`, `pi-predict`, `pi-iso/src/cow.rs`.

**D59 (scope note).** Поддержка detached/nohup-процессов в продукте **не принята**, отложена. Ниже — только source-facts доноров о поведении, без вывода «требование покрыто».

---

## B1. Persistent shell per agent (D76)

### 1) Требования, для которых искались доноры
D76 (своя сохраняемая shell на агента; cwd/vars/functions/options переносятся; не глобальный env процесса). D59 — только source-facts, требование отложено.

### 2) Donors
| Источник | Pin | License | Symbols / anchors |
|---|---|---|---|
| O, Rust engine | 035e4f95 | MIT (root `LICENSE`; GNU-bash/GPL notice не найден) | `crates/pi-shell/src/session.rs`: `Session`, `SessionConfig{env,cwd,fs,vfs,runtime,output_decoding}`, `RunOptions`, `RunStdio`, `RunOutcome{status,cancelled,exited,cwd}`, `SessionError{Closed,Unresponsive,RelativeCwd}`, `Session::new` :312, `run` :374, `run_with` :396; module docs :1-46 (state in `MemStore`; `exit` keeps state; cancel TERM→KILL; nohup detached) [P, raw URL] |
| O, TS host wiring | 035e4f95 | MIT | `packages/coding-agent/src/exec/bash-executor.ts`: `sessionKey` :57, `shellSessions`/`brokenShellSessions`/`shellSessionsInUse` :257-261, quarantine :321-341, `releaseShellSessions` :352-360, reuse/busy :730-752, teardown/reset :953-962, `buildSessionKey` :976-985 [P, raw URL] |
| O, TS snapshot | 035e4f95 | MIT | `packages/coding-agent/src/utils/shell-snapshot.ts`: `scrubSnapshotInPlace` :66, `generateSnapshotScript` :104, `getOrCreateSnapshot` :246-310, mode 0600, postmortem unlink :326-331 [P, raw URL] |
| F, brush engine (библиотека) | 3008cfd | MIT (crate header; vendor brush-core MIT) | `crates/pi-shell/src/shell.rs`: `ShellSessionCore` :37-39, `Shell{session,abort_state,config}` :184-187, `run_shell_session` :326-329+, `create_session_for_run` :701-704+, `copy_env_into_shell` :652-655+, `apply_command_env` :1528-1531, `should_skip_env_var` :1580-1583, `session_keepalive` :1648-1651, `terminate_run` :1465-1468, `source_snapshot`, `RunCancelGuard` [P] |
| F, N-API options | 3008cfd | MIT | `crates/pi-natives/src/shell.rs`: `ShellOptions{session_env,snapshot_path,minimizer}`, class `Shell` [WT; pinned tree `pi-natives/src` содержит shell.rs — файл в дереве, строки не сверялись] |
| F, agent tool | 3008cfd | MIT | `crates/pi-agent/src/tools/mod.rs`: `Tool::is_exclusive` :125-127, `BashTool` `is_exclusive` :607-609, foreground `ShellExecuteOptions{... session_env: None, snapshot_path: None}` :654-662 [P] |

### 3) Фактические границы
- [P] F-библиотека даёт сохраняемую shell: `BrushShell` живёт в `ShellSessionCore`; следующий вызов идёт в ту же сессию, cwd/vars/functions/options остаются; env хоста копируется один раз (`copy_env_into_shell`), shell-внутренние имена вычищаются (`should_skip_env_var`), per-command env — `EnvironmentScope::Command` (`apply_command_env`). Builder: `do_not_inherit_env(true)`, `profile(Skip)`, `rc(Skip)`.
- [P] **F agent-путь persistent shell не использует**: foreground `BashTool` строит one-shot `pi_shell::ShellExecuteOptions` с `session_env: None`, `snapshot_path: None` (:654-662). [WT] `pi_shell::Shell` не инстанцируется в `crates/omp-cli/src` (grep по рабочему дереву; pinned omp-cli построчно не проверялся) — это отдельный, WT-only факт о wiring.
- [P] **F: `exit`/`set -e` рвёт сессию**: `session_keepalive` false для `ExitShell|BreakLoop|ContinueLoop|ReturnFromFunction` (:1648-1651), затем сессия обнуляется — следующий вызов создаёт новый shell. **Source-fact O**: в O Rust `Session` `exit N` завершает run, shell остаётся с состоянием (session.rs :24-31). Это сравнение двух реализаций, не утверждение о требованиях продукта.
- [P] **F: нет генератора snapshot**. `snapshot_path` только потребляется (`source_snapshot`); писателя в `crates/` нет (pinned tree `pi-shell/src`, `pi-builtins/src`, `pi-agent` — модуля-генератора нет; в TS-слое O генератор есть: `getOrCreateSnapshot` :246-310 [P]). Без генератора F-путь snapshot мёртв; persistent in-memory shell им не пользуется.
- [P] **F: нет per-agent ключа**. Изоляция — один объект `Shell` на владельца; O-TS ключует `agentSessionKey + shell + prefix + snapshot + env + minimizer`, имеет `shellSessionsInUse`, quarantine, `releaseShellSessions`, удержание shell при живых background jobs, `isSessionCurrent` (:257-261, :352-360, :953-962, :976-985).
- [P] Env inheritance: O `SessionConfig.env` — явный список, «no inheritance from the pi process beyond what is listed» (session.rs :97-101). F копирует весь `std::env::vars_os()` и фильтрует только shell-специфику (:652-655+, :1580+). Разные гарантии: F не чистит пользовательские секреты из env.

### 4) Внешние зависимости (не смешивать)
- Snapshot-формат/создание — TS-слой O; F Rust его не портировал.
- F `Shell` — публичный API библиотеки; product-side wiring отсутствует, это не свойство библиотеки.
- Открыто для parent: per-agent shell = один `Shell` на агента (F) либо O Rust `Session` как основа; поведение `exit` — выбор продукта.

### 5) Не доказано
Сборка/запуск F `Shell` на Termux; поведение `BrushShell` на `exit`/traps в неинтерактивном режиме; перенос state через рестарт (сериализации ни `MemStore`, ни snapshot для F нет); D27-фильтрация env не реализована ни в F-shell, ни в F-tool.

---

## B2. Builtin command breadth (D38)

### 1) Требования, для которых искались доноры
D38 (максимально широкий in-process набор; несовместимые — тем же бинарником в дочернем процессе).

### 2) Donors
| Источник | Pin | License | Breadth |
|---|---|---|---|
| O `pi-builtins` | 035e4f95 | MIT; uutils coreutils/findutils/sed MIT (THIRD-PARTY) | 95 модулей: `cp.rs` (124 KB), `dd.rs` (74 KB), `chmod.rs`, `df.rs`, `du.rs`, `env.rs`, `id.rs`, `nl.rs`, `od.rs`, `rmdir.rs`, `uptime.rs`, `fold.rs`, `rsync/`, `ast_grep.rs`, плюс cat/grep/sed/ls/jq/sort/find/xargs/stat/diff. Features `util.cp|chmod|dd|df|du|env|id|nl|od|rmdir|rsync|uptime|fold|ast-grep` [P, tree + Cargo.toml] |
| O shell-side tools | 035e4f95 | MIT | `crates/pi-shell/src/tools/{extra,git,nohup,timeout}.rs`; `timeout`/`nohup`/`git` — host callouts, не builtins [P, tree] |
| F `pi-builtins` | 3008cfd | MIT; brush-builtins fork MIT; uutils MIT | 59 зарегистрированных utilities (`factory.rs` `utility_builtins`) + 8 process builtins (`pgrep,pkill,pidwait,ps,top,sleep,timeout,nohup`); модулей `cp/dd/chmod/df/du/env/id/nl/od/rmdir/uptime/fold/rsync/ast-grep` нет [P, tree + factory list] |
| F shell builtins | 3008cfd | MIT (brush) | `default_builtins(BashMode)`: POSIX special + bash builtins; `disown`/`logout` → `UnimplementedCommand` [WT — factory.rs прочитан локально; pinned файл не сверялся построчно] |

### 3) Фактические границы
- F has: cat, grep, rg, sed, find, xargs, ls, jq, sort, stat, diff, date, tail/head, tee, tr, cut, uniq, wc, sha*/md5/b2sum, base32/64, mktemp/mkdir/rm/mv/ln/touch/truncate/readlink/realpath, ps/top/kill/pgrep/pkill/pidwait/nohup/sleep/timeout, `combine`, `ifne`, `isutf8`, `sponge`, `ts`, `errno` [P, module list + factory].
- **F gaps vs O**: `cp`, `dd`, `chmod`, `df`, `du`, `env`, `id`, `nl`, `od`, `rmdir`, `uptime`, `fold`, `rsync`, `ast-grep` (в F ast_grep — отдельный tool, не builtin). `chown` нет ни у O, ни у F [P, trees].
- F dispatch missing names в system `PATH` через brush (внешний процесс) — соответствует D38 для несовместимых, но не даёт in-process набор O.
- [P] F builtins отключаемы env-переключателями: `PI_DISABLE_UUTILS_BUILTINS`, `PI_DISABLE_UUTILS_DESTRUCTIVE`, `PI_DISABLE_RM_BUILTIN`, `PI_DISABLE_MV_BUILTIN`, `PI_DISABLE_NOHUP_BUILTIN` (shell.rs :714-743, :1982-1985); `exec`/`suspend` builtins отключены всегда (:715-720).
- O и F — разные движки: O собственный bash-5.3 VM (`bash::Host` seam), F brush-core. Перенос builtins O→F требует адаптера к `pi_builtins::host::Host` (F host.rs 43 KB vs O host.rs 135 KB [P, tree sizes]), не копирования модулей.
- O `timeout`/`nohup`/`git` — shell tools; F `timeout`/`nohup` — builtins, `git` builtin/guard в F нет [P, trees].

### 4) Внешние зависимости
- Лицензии ported-кода: uutils MIT, brush MIT, onig (BSD, C), O `pcre2-sys` (C), `jaq` (MIT). Транзитивный license closure не проверялся.
- O pi-builtins `rust-version` 1.99 при workspace 1.101, edition 2024; F workspace edition 2024, nightly pin `nightly-2026-08-08` [P, Cargo.toml обоих].

### 5) Не доказано
Паритет F-builtins с GNU/O (флаги, locale, binary safety); какие имена F-shell резолвит в builtin vs external на Termux; производительность in-process на Android arm64.

---

## B3. Batch scheduler (D39)

### 1) Требования, для которых искались доноры
D39 (широкое исполнение batches, порядок call/result, не один вызов за ответ).

### 2) Donors
| Источник | Pin | License | Symbols |
|---|---|---|---|
| O agent loop | 035e4f95 | MIT | `packages/agent/src/agent-loop.ts`: `executeToolCalls` :3299+, per-record `tool.concurrency` (`"shared"|"exclusive"|fn(args)`) :3907-3937, `Promise.allSettled(tasks)` :3940, mid-batch steering/`interruptSkipped` :3336-3345, :3559-3570, auto-background bash/`handlesToolInterrupt` :3449-3456 [P] |
| O tool concurrency contract | 035e4f95 | MIT | `packages/coding-agent/src/tools/bash.ts`: `readonly concurrency = (args) => args.pty === true ? "exclusive" : "shared"` :677-679, `strict = true` :679 [P] |
| F scheduler | 3008cfd | MIT | `crates/pi-agent/src/tools/scheduler.rs`: `use futures::future::join_all` :3, `execute_batch` :59-83, `run_concurrent` :160-162, `run_single` (hidden deny, registry lookup, approver `tokio::select!` с cancel, `excluded_tool_reason`) [P] |
| F tool trait | 3008cfd | MIT | `crates/pi-agent/src/tools/mod.rs`: `Tool::is_exclusive` default `false` :125-127; `BashTool` static `true` :607 [P] |

### 3) Фактические границы
- [P] F: бинарное `is_exclusive` на инструмент; непрерывный shared-сегмент — `join_all` без лимита параллелизма; exclusive — строго по одному; порядок результатов = порядок вызовов; один result на call гарантирован.
- [P] F: approver вызывается внутри `run_single`, т.е. для shared-группы несколько approval-запросов могут идти параллельно (порядок prompt не арбитрируется).
- [P] F: cancellation turn-token отменяет approver-ожидание; exclusive-инструменты не отменяются посреди batch.
- [P] **F теряет per-call concurrency**: O разрешает `concurrency(args)` (bash: PTY → exclusive, иначе shared) и `strict`. F bash всегда exclusive, включая read-only команды.
- [P] **F не имеет mid-batch interrupt**: O при steering/IRC/aside в течение batch посылает cooperative `steeringSignal`, разрешает auto-background bash/eval, помечает пропущенные вызовы `interrupt_skipped`. F ждёт весь batch; loop-level steering здесь не проверялся.
- F не имеет счётчика параллельных tool-вызовов; лимита конкурентности нет [P — в scheduler.rs нет счётчика].

### 4) Внешние зависимости
Codex `ToolCallRuntime` admission уже зафиксирован в старой карте; здесь не переисследовался. Approver/policy (D04) — отдельная полоса.

### 5) Не доказано
Реальный параллелизм shared-инструментов на Android (tokio/rayon pool); отсутствие гонок на общих файловых ресурсах при unbounded `join_all`; поведение двух одновременных approval prompt.

---

## B4. Process / PTY

### 1) Требования, для которых искались доноры
PTY для интерактивных процессов; teardown принадлежащих процессов. D59 detached/nohup — **не принято**, отложено (source-facts ниже только как поведение доноров).

### 2) Donors
| Источник | Pin | License | Symbols |
|---|---|---|---|
| F `pi-shell` process tree | 3008cfd | MIT | `crates/pi-shell/src/process.rs`: `#[cfg(any(target_os="linux",target_os="android"))]` :20, `Process{pid,start_time}` + `pidfd` только `cfg(not(android))` :32-55, Android kill без pidfd (seccomp SIGSYS) :175-183, `status()` Android start_time :216-221, `/proc/{pid}/task/*/children` + fallback скан `/proc` :71-116, `SpawnRegistry`/`PRUNE_THRESHOLD` :1765-1825 [P] |
| F shell teardown | 3008cfd | MIT | `crates/pi-shell/src/shell.rs`: `terminate_run` 3 волны TERM/KILL :1465-1492, `RunCancelGuard` (cancel → abort task → registry.cancel_all), `session_keepalive` :1648 [P] |
| F agent process manager | 3008cfd | MIT | `crates/pi-agent/src/tools/process.rs`: `ProcessManager`, `ProcessBackend`, `ProcessSpec{pty,ready,restart,persist,detached}`, `spawn_pty` через `portable-pty` :44-46, :2674-2680 [WT only] |
| F process broker | 3008cfd | MIT | `crates/pi-agent/src/process_broker/{mod,broker,client,protocol}.rs`: Unix-socket broker, `LeaseLock` flock, stale PID [WT only] |
| F N-API PTY | 3008cfd | MIT | `crates/pi-natives/src/pty.rs`: `portable_pty::{native_pty_system,PtySize}`, `PtyStartOptions`/`PtyArgvStartOptions`, control input/resize/kill, `process_group_leader` unix [WT only; файл в pinned tree есть] |
| F bash tool | 3008cfd | MIT | async → `ProcessSpec{pty: args.pty}`; foreground PTY отклоняется: "Foreground 'pty' needs an interactive terminal UI…" :321-323 [P] |
| O TS bash | 035e4f95 | MIT | `bash.ts` pty + concurrency :677-679; `bash-executor.ts` foreground PTY :544-548, :708-712, user-shell PTY env :512-516 [P] |

### 3) Фактические границы
- [P] F: PTY есть только (a) в async/hub процессе через `portable-pty` [WT для деталей], (b) в N-API `pipe`-обёртке [WT]; foreground `bash` PTY отклоняется (:321-323).
- [P] F foreground bash → brush; cancel: TERM → (2 s grace) → abort task → registry `cancel_all`; `terminate_run` — 3 волны TERM, затем KILL (75/150 ms). On Android — `libc::kill` после проверки start_time, pidfd не используется (seccomp).
- [P] F: `CONFIG_PROC_CHILDREN` может отсутствовать → fallback со сканом `/proc` (комментарий упоминает Kata guest). `nohup` — transparent background wrapper; `exec`/`suspend` отключены [P] / [WT для wrapper-детали].
- [P] O: `nohup` не завершается cancel/закрытием session (session.rs :30-39); `KILL_WAIT` 5 s, затем `Unresponsive`; `timeout` — host callout.

### 4) Внешние зависимости
`portable-pty` 0.9 (wezterm, MIT) в workspace; O дополнительно патчит `cfg_aliases` (vendor) из-за nix 0.28 — в F patch только `brush-core` [P, Cargo.toml]. PTY на Android не проверялся ни в O, ни в F.

### 5) Не доказано
`openpty`/`posix_openpt` и job control под Termux/Bionic; доступность `/proc/*/task/*/children`; поведение `portable-pty` на Android arm64; сборка `pi-natives` (N-API cdylib) на Termux.

---

## B5. Version-guard edit (D44)

### 1) Требования, для которых искались доноры
D44 (правка привязана к прочитанной версии; изменившийся файл → конфликт, не слепая запись; не затирать чужие изменения).

### 2) Donors
| Источник | Pin | License | Symbols |
|---|---|---|---|
| O `pi-edit` | 035e4f95 | MIT | `crates/pi-edit/src/lib.rs`: `ModeEngine`, `StagedFile`, `PreviewBatch`, `Session`, `EditWriter`, `WriteRequest`, `WriteResponse`, `ApplyOutcome`, `EditStore`, `PathPolicy`, `ArgStream`; modes `replace|patch|apply_patch|hashline|sloppy`; отдельные `store.rs` (snapshots, clipboard, no-op guard), `notebook.rs`, `fuzzy.rs`, `session.rs` (streaming preview → staged files → host-owned writer), `stream_json.rs` [P, lib.rs + tree] |
| O hashline tag | 035e4f95 | MIT | уже зафиксировано D44: `[PATH#TAG]`, tag = low 16 bit xxh32 нормализованного текста |
| F hashline | 3008cfd | MIT | `hash.rs`: `compute_file_hash` = `xxh32(normalize,0)&0xffff`, `HASH_LENGTH=4` :8, :24-28; `snapshot.rs`: caps `SNAPSHOT_MAX_BYTES=4 MiB`, `MAX_PATHS=256`, `MAX_VERSIONS_PER_PATH=4`, `MAX_TOTAL_BYTES=64 MiB` :19-25, `SnapshotStore` :78-80, `record` :93-126, `record_seen_lines` :130-137; `edit.rs`: `enforce_seen_lines` default **false** :49-57, `assert_seen_lines` :615-618+ [P] |
| F ast_edit gate | 3008cfd | MIT | `ast_edit.rs`: «re-reads every affected file and compares bytes before the first write; any change is stale, so zero files are written» :6-10; **«There is no filesystem lock… a writer racing inside that window can still be overwritten»** :12-15; stale details `{stale,partial,gateFailures,writtenFiles,snapshotInvalidations}` :928-952; `AttemptWrites` :863-866; partial-write reporting :986-990 [P] |
| F modes | 3008cfd | MIT | `edit_variants/mod.rs`: `Hashline` dispatch refuses non-native; `replace|patch|apply_patch|sloppy`; `mode.rs`: model exclusion list (kimi, mimo, deepseek-v4-flash, step-3.7-flash) → `sloppy` [WT only] |

### 3) Фактические границы
- [P] F: основной guard — content tag. `edit` принимает `[path#TAG]`; tag не совпал с live-хэшем → mismatch/recovery, запись не выполняется. Tag 16-bit → коллизии возможны; store различает коллизии по полному тексту (`record`: hash+text).
- [P] F: `enforce_seen_lines` — opt-in, default false (совпадает с upstream `edit.enforceSeenLines`). Включённый guard отклоняет анкоры на строках, которые read не показывал; отказ не делает запись, а обновляет provenance.
- [P] F: `ast_edit` — staging + byte gate: preview готовит выходы, `write xd://resolve` перечитывает все затронутые файлы и сравнивает байты; любое изменение до первой записи = ноль записей. **Staging, не CAS и не multi-file transaction**: файловой блокировки нет, гонка внутри окна (в т.ч. после частичной записи) может перезаписать; частичная запись репортится (`partial:true`, `writtenFiles`), но не откатывается.
- [P] F: `write` (bulk) не пишет provenance и не проходит hashline-гейт; `write xd://resolve` финализирует уже одобренный staged preview.
- **F не имеет notebook-режима** (O `notebook.rs`), O `pi-edit` также содержит `fuzzy.rs`, `path_policy.rs`, `stream_json.rs` streaming-preview; F переносил только hashline/edit variants внутрь `pi-agent` [P, trees].
- O `EditStore` — «snapshots, clipboard, no-op guard»; F аналог только `SnapshotStore`; no-op guard/clipboard не подтверждены [P для O lib.rs, WT для F полноты].
- F `PathPolicy` отсутствует как crate-модуль (в 3008cfd нет `pi-edit`); path-политика живёт в read/write tools и `pi-agent` [P, tree].

### 4) Внешние зависимости
`xxhash-rust` (MIT/Apache) в pi-shell и pi-agent; `similar` в pi-iso/pi-builtins. Fuzzy matching — только O.

### 5) Не доказано
Что все edit-пути (не только `edit`/`ast_edit`) гейтятся версией; поведение при tag-коллизии в одной сессии; эквивалентность нормализации (BOM/CRLF/trailing ws) O↔F во всех режимах; отсутствие blind write в `sloppy`/`patch` modes [WT-режимы не сверялись с pin].

---

## B6. Isolation snapshot / merge (D07, D11, D28)

### 1) Требования, для которых искались доноры
D07 (один писатель; изолированные workspace; не security sandbox), D11 (three-way, учёт незакоммиченных, сериализация, конфликт без принудительных ours/theirs), D28 (без автокоммитов пользователя).

### 2) Donors
| Источник | Pin | License | Symbols |
|---|---|---|---|
| F `pi-iso` | 3008cfd | MIT | `crates/pi-iso/src/lib.rs`: `IsolationBackend{probe,start,stop,diff}`, `BackendKind{...}`, `native()` :106-123, `auto_order` :303-319, `LINUX_AUTO_ORDER` :128-135, `FALLBACK_AUTO_ORDER=[Rcopy]` :139-140; backends `apfs,btrfs,zfs,linux_reflink,overlayfs,projfs,windows_block_clone,rcopy,diff` [P] |
| F task isolation | 3008cfd | MIT | `crates/pi-agent/src/tools/task_isolation.rs`: `ISOLATION_BASELINE_MAX_CONTENT_BYTES = 1 GiB` :52-53, `MERGE_LOCK` :57-59, `prepare_context` :726-731, budget-ошибка :820-828, `ensure` :1032-1033, `ensure_blocking` :1050-1053, `apply_patch` (`["apply","--binary","--whitespace=nowarn"]`, **без `--3way`**) :592-601, `patch_applies` :606-617, `merge_patch` :1708-1709, `merge_branch` :1782-1783+, cherry-pick :1875-1878, stash `--include-untracked` :1852-1864; branch capture docs про «git's 3-way merge machinery» для ребейза агентских коммитов поверх dirty baseline :1523-1530 [P] |
| O `pi-iso` | 035e4f95 | MIT | тот же набор + `cow.rs` (15 KB, CoW tree clone), `tree.rs`; O `pi-builtins/src/cp.rs` использует `pi_iso::cow` [P, tree] |

### 3) Фактические границы
- [P] **Termux cfg gap (главный)**: все Linux-бэкенды гейтятся `#[cfg(target_os = "linux")]`; Android не `linux` в Rust cfg. На Termux `auto_order()` = `[Rcopy]`, `native()` = `Rcopy`; явные `btrfs|zfs|reflink|overlayfs` → `ProbeResult::unavailable` / `IsoError::Unavailable`. Итог: только `Rcopy` — `git worktree` если lower git-repo, иначе полный рекурсивный copy.
- `zfs.rs` гейт `cfg(unix)` — теоретически доступен на Android, но требует ZFS pool и `zfs` CLI (не проверялось) [WT для гейта].
- [P] F merge: `patch`-режим — `git apply --check` (forward/reverse) + `git apply --binary`; three-way **не запрошен**. Если patch не применяется прямо или обратно → `applied:false`, patch-артефакт сохраняется, «must be handled manually». `branch`-режим — `git cherry-pick base..branch` (внутри git делает 3-way), при конфликте `--abort`, ветка сохраняется, stash pop. Для dirty baseline с агентскими коммитами `prepare_branch_capture` использует git-ребейз (3-way machinery) поверх синтетического baseline-коммита; финальный parent-side merge всё равно cherry-pick.
- [P] F baseline учитывает незакоммиченное: staged/unstaged/untracked, budget 1 GiB; capture диффит против baseline-дерева, не HEAD.
- [P] F сериализация: один process-wide `MERGE_LOCK` на мутации parent-репо (patch apply, cherry-pick, nested stash/apply/commit/pop). Кросс-процессной сериализации нет.
- [P] F «изменение цели требует пересчёта с лимитом» (D11) — **не найден**: нет retry/пересчёта относительно изменившегося target; один снимок на finalize.
- F изоляция не security sandbox (D07); ownership marker + orphan scan — против повторного использования чужого sandbox, не против вредоносного кода [P, module docs].
- [P] F `Rcopy` diff для не-git деревьев — `(size,mtime)` short-circuit + content diff; git-деревья — `git diff`, byte-identical для downstream `git apply`.

### 4) Внешние зависимости
`git` CLI обязателен (worktree/apply/cherry-pick/stash/rev-parse). O имеет `cow.rs` (и `cp` использует его); в F нет ни `cow.rs`, ни `cp`.

### 5) Не доказано
Работа `git worktree`, `git apply --binary`, `cherry-pick`, `stash` под Termux; поведение на Android FUSE/sdcard (symlink, mode, xattr); скорость полного `Rcopy` на большом repo; отсутствие потери незакоммиченных изменений при конфликте (тесты F есть, не запускались).

---

## B7. Env и command guard (D04, D27, D29)

### 1) Требования, для которых искались доноры
D04 (allow/deny/ask, статические правила), D27 (credentials вне shell/VFS, защита от symlink/alias), D29 (новые запреты проверяются перед каждым действием).

### 2) Donors
| Источник | Pin | License | Symbols |
|---|---|---|---|
| O command guard | 035e4f95 | MIT | `packages/coding-agent/src/tools/bash.ts`: `CRITICAL_BASH_PATTERNS` :191-260 (`rm -rf /`, `--no-preserve-root`, `sudo rm`, `chmod -R … /`, fork bomb, `>/dev/sd*`, `mkfs`, `dd of=/dev/*` с исключением null/zero/stdout/stderr/tty/fd, `tee /etc/{passwd,shadow,sudoers}`, `curl|bash`, process-sub/eval variants, `kill -9 1`, shutdown/reboot/halt, `init 0`, `nc -e`), `bashApprovalRuleMatches` :309-322, segment matching через `tokenizeShellSegments`, critical → tier `exec` + `override` (prompt, не deny) :586-618 [P] |
| O git guard | 035e4f95 | MIT | `crates/pi-shell/src/git/guard.rs`: opt-in `PI_GIT_GUARD`, `Review::of`, `Decision{Run,RunNoted,Refuse}`, `REFUSED_EXIT=1`, `DISCARDED_REF="refs/omp/discarded"`, правила shared-checkout (stash/reset --hard/switch/checkout path/restore) vs private worktree; `git/mod.rs` [P] |
| F approval matrix | 3008cfd | MIT | `crates/omp-cli/src/approve.rs`: `ApprovalMode{AlwaysAsk,Write,Yolo}` :21-49, `ToolTier{Read,Write,Exec}` :63, `classify_tool` :96-178 (per SSH target, LSP/DAP per action, MCP→Exec), `decide_tier` :323-332 (allow или Yolo → Auto; Read → Auto; иначе Ask); `acp/session.rs` `approve` [P] |
| F shell env | 3008cfd | MIT | `crates/pi-shell/src/shell.rs`: `copy_env_into_shell` :652+, `normalize_env_key`, `should_skip_env_var` :1580+ (BASH_FUNC_*, BASH_ENV, ENV, PS*, HIST*, BASH_*, SHELLOPTS, IFS, PWD…), `apply_env_fallback`, `apply_command_env` :1528+ [P] |
| F output redaction | 3008cfd | MIT | `crates/pi-shell/src/minimizer/filters/system.rs`: маскирование `token|secret|password|credential|api_key|private_key|jwt` в выводе `env` [WT only] |

### 3) Фактические границы
- **F не имеет command-pattern guard**: ни `CRITICAL_BASH_PATTERNS`, ни bash approval rules, ни segment tokenizer; grep по `crates/` не нашёл [P — в pinned pi-agent/omp-cli таких символов нет; WT grep совпадает]. Есть только tier-матрица по инструменту (`bash` → Exec :146) и optional interactive prompt. `--allow bash` разрешает любую команду.
- **F не имеет git guard**: `PI_GIT_GUARD`/`refs/omp/discarded` отсутствуют; F shell не перехватывает `git` [P, tree].
- [P] F env guard — только «не заносить shell-внутренние переменные»; пользовательские секреты (`*_API_KEY`, токены) копируются в shell полностью. O `SessionConfig.env` — явный allowlist, наследования нет.
- [P] F per-command env валидирует имя (`is_valid_env_name`) и тип значения; значения не фильтруются [WT для `is_valid_env_name`, pinned `bash_env` :260-263].
- [WT] F output redaction — отображение (`env`-фильтр minimizer), не enforcement; полный текст уже мог уйти в модель/лог.
- [P] F approval: `yolo` авто-одобряет всё; `allow` — точное имя инструмента (`mcp:server:tool` origin отдельно); read-tier авто. Deny-правил по аргументам нет.
- [P] O critical-паттерны повышают tier до `exec`/override (prompt), а не deny; `bash` default `strict = true`.

### 4) Внешние зависимости
D27 storage/VFS-запрет credentials — другая полоса, не проверялась. `tokenizeShellSegments` (shell-tokenize.ts) — parser-донор сегментации; в F его нет (в minimizer — только split по whitespace/`;|&`, не полноценный токенайзер) [WT].

### 5) Не доказано
False-positive/false-negative исходных regex на модельных командах; эквивалентность сегментации O-tokenizer и compound shells; F prompt без tty в headless (deny реализован, UX не испытан); фильтрация env в product-side runtime `omp-cli` не найдена [WT grep].

---

## Сводка неподтверждённых гарантий (кандидаты на проверку/фикс)
1. F: persistent shell для агента не подключён. Pinned: foreground `BashTool` всегда строит one-shot `ShellExecuteOptions` с `session_env: None`, `snapshot_path: None` (`tools/mod.rs:654-662`). WT-only: `pi_shell::Shell` не инстанцируется в `crates/omp-cli/src` (grep рабочего дерева; pinned omp-cli не проверялся построчно). Это два разных факта: pinned options — про отсутствие per-call session env/snapshot; WT — про отсутствие wiring.
2. F: shell рвётся на `exit`/`set -e`; O Rust `Session` сохраняет состояние [P].
3. F: нет per-agent session key, quarantine, `releaseShellSessions`, удержания shell при background jobs [P].
4. F: нет генератора snapshot; `snapshot_path` — мёртвый вход [P].
5. F: env наследуется целиком; D27-очистки секретов нет [P].
6. F: builtin breadth уже O (нет cp/dd/chmod/df/du/env/id/nl/od/rmdir/uptime/fold/rsync) [P].
7. F: batch — статический `is_exclusive`, без per-call concurrency и mid-batch interrupt [P].
8. F: foreground PTY отсутствует; PTY только async/hub [P для отказа, WT для реализации].
9. F: isolation на Android = `Rcopy` только; reflink/overlay/btrfs недоступны по cfg [P].
10. F merge — не three-way (`git apply` без `--3way`); нет пересчёта при изменении target [P].
11. F: нет command guard и git guard; approval только по tier/инструменту [P].
12. F: `write` bulk не проходит version-gate; notebook edit отсутствует; `ast_edit` — staging без fs-lock, гонка в окне возможна [P].
13. D59 detached/nohup — не требование; source-facts не считать согласованной поддержкой.

## Termux / native gaps
- `cfg(target_os="linux")` в `pi-iso` исключает Android → `FALLBACK_AUTO_ORDER=[Rcopy]`; `LinuxReflink/Overlayfs/Btrfs` probe → unavailable [P].
- `pi-shell/src/process.rs`: pidfd отключён на Android (seccomp SIGSYS), identity = start_time, kill через `libc::kill`; `/proc/*/task/*/children` может отсутствовать, fallback — скан `/proc` [P].
- `rust-toolchain.toml` targets: `x86_64-unknown-linux-gnu`, `x86_64-pc-windows-msvc` — `aarch64-linux-android` нужно добавлять [P, локальный pin workspace].
- `pi-natives` — N-API cdylib; PTY/shell доступны агенту только через N-API (fork) или через `pi_shell` (без PTY).
- C-зависимости: O `pcre2-sys`, F `onig` (grep-pcre2), `portable-pty`/nix — требуют NDK/clang; сборка на Termux не проверялась.
- `brush-parser` версии: workspace 0.3 vs требование `pi-builtins` ^0.4 — возможны два экземпляра в графе [WT для workspace Cargo.toml].

## Лицензии (что проверено)
- O root и F root: MIT. F `pi-shell`, `pi-builtins`, `pi-iso`: `license = "MIT"` [P].
- F vendored `brush-core`: MIT (`crates/vendor/brush-core/LICENSE`, THIRD-PARTY :22-30); brush-builtins fork MIT (:61-65); uutils coreutils/findutils/sed MIT (:78-133) [P, локальный THIRD-PARTY].
- `portable-pty` 0.9 / `filedescriptor` — wezterm (THIRD-PARTY :16884-5); текст лицензии в файле; юридически не проверялось.
- O `pi-shell` не содержит GNU-bash GPL notice; движок позиционируется как собственный Rust. Это source-fact о notices, не юридическое заключение.
- Транзитивный license closure, `cargo deny`, Android-артефакты не проверялись.
