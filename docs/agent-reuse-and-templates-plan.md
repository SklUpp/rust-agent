# План оптимизации переиспользования кода и шаблонов модулей (`rust-agent`)

Дата: 2026-11-27 (аудит `ArchReuseToolsExec`, `ArchReuseIpcStorage`, `ArchReuseAgentLifecycle` + синтез `PlannerReuseArchitecture`, база `HEAD b755a8b`).
Статус: **Предложение** (не нормативно до фиксации решений в [agent-architecture-decisions.md](agent-architecture-decisions.md)).
Дополняет [modular-reuse-blueprint.md](modular-reuse-blueprint.md): там зафиксированы внешние зависимости/лицензии/Cargo features, здесь — внутренняя унификация кода, интерфейсы и шаблоны написания модулей.

---

## 1. Диагноз

Ядро уже построено вокруг удачных абстракций: `Tool` (`crates/pi-agent/src/tool.rs:24`), `ToolScheduler` (`crates/pi-agent/src/tools/scheduler.rs:13-339`), `ModelProvider` + `TimeoutProvider` (`crates/pi-agent/src/provider.rs:198`, `crates/pi-agent/src/provider/timeout.rs:69`), `IsolationBackend` (`crates/pi-iso/src/lib.rs:295`), `ProtocolHandler` (`crates/pi-agent/src/tools/internal_urls.rs:214`), `EditContract` (`crates/pi-agent/src/tools/edit_variants/mod.rs:28-92`), `ScriptAssets` (`crates/pi-agent/src/eval/languages.rs:34-184`).

Дублирование накопилось не в самих контрактах, а в обвязке, написанной вокруг них по 2–19 раз:
- **Subprocess / signals / caps:** 19 реализаций запуска `Command`, 5–10 локальных копий `kill(-pid, SIGKILL)` при наличии `pi_shell::process::kill_process_group`, 4 таблицы env allowlist, 4 цикла `read_capped`, 6 clamp-политик таймаутов, дубль `SNAPSHOT_MAX_BYTES`.
- **IPC / FS / Storage:** 3 реализации `Content-Length` кодировщика (одна с багом тихой подмены ошибки на `"null"`), 3 `constant_time_eq`, 3 `random_hex` (со слабым time/pid fallback в `eval/util.rs`), 5 расходящихся `lexical_normalize`, 2 копии no-clobber публикации, 3 неполные копии защиты каталогов/БД рядом с эталонным `aragent/src/diagnostics.rs`.
- **Agent / Provider / `pi-iso`:** 4 места сборки дочернего `AgentLoop`, 2 несвязанные машины состояний `ChildStatus`/`LifecycleStatus` + ручное слияние в `tools/hub.rs`, 4 хост-проекции событий поверх дубля `aragent::Event`, дубль HTTP `BoundedBody`/retry в провайдерах (OpenAI-compatible не ретраит 429/5xx), 6 копий FS-хелперов в `pi-iso` и неиспользуемая зависимость `pi-vcs` в `crates/pi-iso/Cargo.toml:14`.

---

## 2. Канонический конвейер и 7 шаблонов модулей

Любая новая вариация модуля проектируется по единой 4-фазной последовательности:

`validate -> authorize / containment -> execute / bound -> record / cleanup`

| Область | Эталонный шаблон | Контракт новой вариации | Граница (чего не делать в модуле) |
|---|---|---|---|
| **Tools (режимы одного инструмента)** | `crates/pi-agent/src/tools/edit_variants/mod.rs:28-92` + `tools/scheduler.rs:13-339` | `enum Mode` + `const DESCRIPTION` + `schema()` (`EditContract`) + единый `execute(mode, ...)`; парсинг аргументов через общие `args_object`/`require_str`/`reject_unknown` | Не вшивать hooks, approvals, cancel-precheck, durability — их гарантирует `ToolScheduler::run_single` |
| **Protocol / URL handlers** | `crates/pi-agent/src/tools/internal_urls.rs:42-254` | Реализовать `ProtocolHandler` (`resolve`, опционально `write` через `writable()`) поверх immutable `UrlScope`; регистрировать в `InternalUrlRouter::register` | Не переписывать `O_NOFOLLOW` обход путей локально; встроенные схемы закрыты в `RESERVED_SCHEMES` |
| **Language runners (`eval`)** | `crates/pi-agent/src/eval/languages.rs:34-184` | Добавить запись `ScriptAssets { runner, prelude, extension }` + `argv` / `init_script` / `exit_payload` / `ProbeFn` в таблицу | Handshake, framing, watchdog, `setsid` и kill-цикл держит `eval/kernel.rs` |
| **Isolation backends** | `crates/pi-iso/src/lib.rs` | Реализовать `IsolationBackend` (`kind`, `probe`, `start`, `stop`) + включить в OS `auto_order` | Переиспользовать `pi_iso::fsutil` (`canonical_existing_dir`, `absolutize`, `prepare_destination`) и `pi_iso::diff` |
| **Providers & decorators** | `crates/pi-agent/src/provider/timeout.rs:69-105` | Адаптер диалекта реализует `ModelProvider` поверх общих `BoundedBody` + `RetryPolicy`; сквозные политики (таймауты, fallback, quarantine) — декораторы над `Arc<dyn ModelProvider>` | Не дублировать `read_body_bounded`, redaction и retry-ветки внутри каждого HTTP-провайдера |
| **Private Local IPC & Stores** | `process_broker/protocol.rs:430-600` (сокеты/токены) + `aragent/src/diagnostics.rs:101-278` (БД) | `PrivateFs` / `PrivateEndpoint` / `PrivateDb`: проверка предков, `O_NOFOLLOW`, `uid == geteuid()`, `0700`/`0600` (`fchmod`), pre-open/post-open sidecars (`-wal`/`-shm`/`-journal`), CSPRNG `random_hex` + `constant_time_eq` | Не вызывать `std::fs::set_permissions(path)` (следует по симлинкам) и `create_dir_all` вместо `ensure_private_directory` |
| **Subprocess & Subagent runners** | Планируемые `ExecPlan` и `ChildLoopSpec` | Подпроцесс: `ExecPlan` с явным `SpawnIsolation::{ProcessGroup, NewSession}`, `read_capped`, deadline/cancel и `kill_process_group`; субагент: `ChildLoopSpec` builder | Не собирать вручную `AgentLoop` и не копировать `libc::kill(-pid, SIGKILL)` |

---

## 3. Дорожная карта волн (по отдаче и риску)

### Wave 1 — Zero-risk дедупликация (без изменения контрактов и поведения)

1. **Единый `kill_process_group`**
   - Реэкспортировать `pub use pi_shell::process::{kill_process_group, KILL_SIGNAL, TERM_SIGNAL};` из `crates/pi-agent/src/lib.rs` (у `aragent` нет прямой зависимости от `pi-shell`).
   - Удалить локальные копии `kill(-pid, SIGKILL)` и заменить на `kill_process_group` (сохраняющую защиту `is_self_process_group` из `crates/pi-shell/src/process.rs:1658-1677`): `crates/pi-agent/src/hooks.rs:953`, `mcp/transports/stdio.rs:455-464`, `tools/ssh.rs:537-548`, `tools/read_archive.rs:755`, `tools/internal_urls_github.rs:1506`, `eval/kernel.rs:650`, `eval/probe.rs:148`, `lsp/client.rs:276-277`, `lsp/mux/server.rs:975-976`, `tools/process.rs:2776-2789`, `crates/aragent/src/rpc/mode.rs:403`.
   - Примечание: в `lsp/client.rs` и `mux/server.rs` сохранить fallback на `kill(pid, SIGKILL)` только для `setsid`-detached лидеров.
2. **Дедупликация `SNAPSHOT_MAX_BYTES`**
   - `crates/pi-agent/src/tools/read.rs:75` перевести на `super::hashline::snapshot::SNAPSHOT_MAX_BYTES` (`crates/pi-agent/src/tools/hashline/snapshot.rs:19`).
3. **Единый `ContentLengthCodec` (устранение бага `"null"`)**
   - Вынести `encode_frame(&Value) -> Result<Vec<u8>, serde_json::Error>` рядом с единым декодером `MessageFramer` (`crates/pi-agent/src/lsp/framing.rs:45`).
   - Перевести `lsp/client.rs:304-307`, `dap/client.rs:956-960`, `lsp/mux/mod.rs:48-52` (убрать `.unwrap_or_else(|_| "null")`).
4. **Единый `publish_staged_file` (no-clobber)**
   - Добавить `publish_staged_file_io(...) -> io::Result<bool>` в `crates/pi-agent/src/tools/atomic_publish.rs:26-78` и перевести `crates/aragent/src/migration.rs:454-492`.
5. **Единый контракт `IsolationOwnerMarker`**
   - Экспортировать `OwnerMarker` / `ISOLATION_OWNER_FILE` / `owner_is_live` из `crates/pi-agent/src/tools/task_isolation.rs:65,1024-1045` и переиспользовать в `crates/aragent/src/worktree_cmd.rs:67,569-610`.

### Wave 2 — Унификация безопасности, путей и границ доверия

1. **CSPRNG `random_hex` + `constant_time_eq`**
   - Свести `process_broker/protocol.rs:403,589`, `eval/bridge.rs:466`, `auth/pkce.rs:69` в `pi_agent::security_primitives` (fail-closed OS CSPRNG).
   - Убрать слабый fallback `SystemTime ^ pid ^ counter` в `crates/pi-agent/src/eval/util.rs:50-67`.
2. **Единый `lexical_normalize`**
   - Слить 5 расходящихся реализаций (`state.rs:1466`, `edit_variants/paths.rs:108`, `tools/mod.rs:3252`, `task_isolation.rs:3116`, `aragent/doctor.rs:1554`, `aragent/migration.rs:317`) в root/prefix-aware функцию: не сжимать `Component::RootDir`/`Prefix` (`/../a -> /a`), сохранять ведущие `..` только для относительных путей. Покрыть табличным тестом.
3. **`PrivateFs` / `PrivateDb` / `PrivateEndpoint`**
   - Вынести `crates/pi-agent/src/private_fs.rs` (цепочка предков из `aragent/src/diagnostics.rs:101-278` с сохранением Android `uid 1000` / `>= 10_000` + сокеты/токены/lease из `process_broker/protocol.rs:430-585`) и `private_db.rs`.
   - Закрывает пробелы в `aragent/src/auth/store.rs:176-188` (нет tightening существующего каталога и `-wal`/`-shm`), `aragent/src/local_memory.rs:624` (нет `0600` и `sync_all`), `tools/process.rs:549` (не затягивает широкий каталог логов), а также унифицирует `dap/client.rs` (`SocketPath`), `lsp/mux/{server,client}.rs`, `durable_session.rs`, `session_store.rs`.
4. **`ExecPlan` + `io_cap::read_capped`**
   - Унифицировать `read_capped` (`tools/mod.rs:151`, `ssh.rs:517`, `internal_urls_github.rs:1476`, `aragent/rpc/mode.rs:320`, `tools/read.rs`) и `clamp_secs`.
   - Завернуть шаблонные `Command` + timeout + capped stdio + `kill_process_group` (`hooks.rs`, `ssh.rs`, `read_archive.rs`, `lsp/linter.rs`, `lsp/workspace_diagnostics.rs`, `rpc/mode.rs:371`) в `ExecPlan` (отдельно проверить/закрыть `crates/pi-vcs/src/git/mutate.rs:631`, где git commit hooks сейчас запускаются без таймаута, лимита вывода и `process_group`).
5. **Унификация мелкого boilerplate в `tools`**
   - В `tool.rs`: общие `args_object`, `require_str`, `reject_unknown`, `aborted_result`; удалить 5 избыточных `is_exclusive() -> false`; сборку schema envelope перенести в `ToolRegistry`.

### Wave 3 — Жизненный цикл агента, провайдеры и `pi-iso`

1. **`ChildLoopSpec` вместо 4 ручных сборок `AgentLoop`**
   - Унифицировать `tools/task.rs:921-965` (background), `tools/task.rs:2226-2275` (sync), `task_persisted_revive.rs:711-753` (revive) и при необходимости advisor (`runtime.rs:4234-4275`) через `ChildLoopSpec` builder, чтобы исключить рассинхрон `TranscriptJournal` / `DurableBoundary` / `peer_inbox`.
2. **Единая read-only проекция `Supervisor::liveness(id)`**
   - Убрать 3-источниковое слияние и 4-веточный разбор в `tools/hub.rs:118-201` над `ChildStatus` (`supervisor.rs:444`) и `LifecycleStatus` (`subagent_lifecycle.rs:202`).
3. **Разделение `PiAgentRunner::run` + унификация `AgentEvent`**
   - Разбить `aragent/src/runtime.rs:1595` (~1260 строк) на фазы (`prepare_engine`, `spawn_dialog`, `pump_events`, `settle`).
   - Устранить 4-кратный синтез `MessageStart`/`AgentEnd` (`runtime.rs:2485`, `rpc/mode.rs:157`, `rpc/subagents.rs:160`, `acp/session.rs:2679`) и дубль `aragent::Event` (`aragent/src/agent.rs:31-100`).
4. **Provider `BoundedBody` + `RetryPolicy`**
   - Вынести `BoundedBody` / `read_body_bounded` / `Retry-After` (`crates/pi-agent/src/provider/retry.rs`) из `provider_anthropic.rs` и `provider_ollama.rs` (под feature `direct-providers`), удалить дубль `redact_with` (`provider_ollama.rs:218`) и подключить `RetryPolicy` к `provider/http.rs:334-472` (сейчас OpenAI-compatible адаптер не ретраит 429/5xx).
5. **`pi-iso::fsutil` + подключение `pi-vcs`**
   - Вынести общие `canonical_existing_dir`, `absolutize`, `prepare_destination`, `copy_symlink_confined`, `preserve_meta` в `crates/pi-iso/src/fsutil.rs` (из 6 бэкендов).
   - Заменить ручные вызовы `git worktree` / `git diff` в `crates/pi-iso/src/rcopy.rs:128` и `diff.rs` на уже объявленный в `crates/pi-iso/Cargo.toml:14` крейт `pi-vcs`.

---

## 4. Явные non-goals (границы, которые объединять НЕЛЬЗЯ)

1. **Четыре контракта atomic-write:** no-clobber (`atomic_publish`), overwrite-in-place (`write_atomic_with_mode`), per-path lock+merge (`local_memory.rs:602-652`) и SQLite WAL-транзакции (`session_store.rs`) не сводить в один хелпер — ломает `RENAME_NOREPLACE` или inode-проверки.
2. **Три политики env:** `eval/env.rs:18-50` (строгий secret denylist для недоверенного кода), `state::platform_env` (наследуемый env инструментов) и `pi-shell/shell.rs:834-878` должны оставаться отдельными таблицами политики.
3. **`process_group(0)` vs `setsid()`:** PTY/detached процессы и eval/DAP kernels (`tools/process.rs:2755`, `eval/kernel.rs:172`, `dap/client.rs:855`) требуют отрыва от управляющего TTY через `setsid()`, а pipe-утилиты — `process_group(0)`.
4. **`mailbox` vs `lifecycle` queues:** `subagent_lifecycle.rs:226-235` (`inbound` vs `steering`) и `supervisor` mailbox имеют разную семантику долговечности; общей делаем только read-only проекцию `Liveness`.
5. **`pi-iso::diff` plain walker vs `git diff`:** non-git `Rcopy` использует `(size, mtime)`-обход (`diff.rs:73-79`), а git-бэкенды — `git diff` для точной совместимости с `git apply`.
6. **Не вводить `ToolSpec` DSL и глобальный circuit breaker:** инструменты различаются структурой `details` JSON, а `TimeoutProvider` + ретраи адаптеров уже несут строгий инвариант «не ретраить после первого полученного чанка» (`provider/timeout.rs:2-20`).

---

## 5. Ключевые риски перед стартом волн

- Перевод `hooks.rs` на `EnvPolicy::Isolated` меняет поведение для пользовательских hook-скриптов, полагающихся на ambient env — требует явного решения и `HookEngine.env` overlay.
- `platform_env` не содержит `SSL_CERT_FILE` / `NODE_EXTRA_CA_CERTS`, нужных MCP (`stdio.rs:60-75`) — расширять через `platform_env_with(extras)`, не глобально.
- Все изменения в `provider_anthropic.rs` / `provider_ollama.rs` обязательно прогонять в CI с `--features direct-providers`.
- `PrivateFs` должен сохранять Android-исключения из `diagnostics.rs:97-130` (`uid/gid 1000`, app `uid >= 10_000`) и macOS symlink-предки (`/var -> private/var`).
