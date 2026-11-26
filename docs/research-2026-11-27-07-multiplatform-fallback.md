# Track 7: Мультиплатформенная одновременная работа и деградирующий fallback (2026-11-27)

> **Источники:** `ResearchMultiPlatformFallback` + аудит `ResearchReviewPlatform`.
> **Якоря:** `~/rust-agent/crates/pi-agent/src/{tools/ssh.rs,tools/process.rs,tools/mod.rs,tools/read.rs,tools/scheduler.rs,tools/task_isolation.rs,lsp/,state.rs,durable.rs,workspace_writer.rs}`, `crates/pi-iso`, `crates/pi-vcs`, `crates/aragent/src/{acp/,rpc/,approve.rs,trust.rs,config.rs}`.
> **Границы проверки:** статический анализ исходников; реальные SSH/LSP-интеграционные прогоны и бенчмарки синхронизации не выполнялись.

---

## 1. Аудит текущих швов и обнаруженные уязвимости/пробелы

| Компонент | Текущее состояние | Пробелы (подтверждено ревью) |
|---|---|---|
| **`ssh://` (`tools/ssh.rs`)** | Только чтение (1 файл ≤ 1 MiB, листинг, POSIX-проба, 30s timeout). Нет write/edit/ControlMaster (`ssh.rs:26–27`) | 1) **D20 gap:** `.omp/ssh.json` в проекте загружается **без trust-gate** (`ssh.rs:864–882`, а `trust.rs:34–49` покрывает только hooks/MCP); 2) нет `env_clear()` — процесс наследует `SSH_AUTH_SOCK` и весь env (`ssh.rs:356–367`), что даёт локальную auth; отсутствие `-A` само по себе не доказывает запрет forwarding при внешнем ssh config; 3) `approve.rs:86–160` допускает маршрутизацию `ssh://` как Exec для write/edit, хотя инструменты это ещё не умеют |
| **Remote exec (`tools/mod.rs`, `acp/`, `read.rs`)** | `AcpClientBridge` делегирует в `terminal/create` только для `LegacyOneShot`; persistent shell всегда локальный (`tools/mod.rs:610–645`). `read.rs:688–702` передаёт локальный абсолютный путь в ACP | При обрыве связи `ProcessState`/`ProcessError` (`process.rs:95–114, 312–325`) не отличают `UnknownOutcome` от провала запуска; отсутствует `PathMap` для ACP |
| **LSP (`lsp/config.rs`, `lsp/client.rs`, `lsp/mux/`)** | Есть готовый шаблон mux worker (`lsp/mux/`, `main.rs:255–265`). `lsp.enabled` в `config.rs:346–371` по умолчанию `true` (расхождение со `SCOPE_LATEST.yaml` R4) | 1) **D20 gap:** project LSP config (`lsp/config.rs:290–320`) задаёт произвольный `command` без `trust.rs`; 2) `LspClient` спавнит без `env_clear()` (`lsp/client.rs:860–873`), отдавая процессу API-ключи из env |
| **Планировщик и durability** | `bash` жёстко `is_exclusive() == true` (`tools/mod.rs:600–602`), блокирует параллельный local edit + remote `cargo test` | `tools/scheduler.rs:235–249` отклоняет `task` при активной durability; `DurableError::unknown_outcome` (`durable.rs:43–78`) и `McpError::UnknownOutcome` не покрывают remote process dispatch |
| **Изоляция и запись** | `pi-iso` (`xxh3_64`, `Rcopy` 1 GiB), `task_isolation.rs` (3-way merge, `[AUTO-APPLIED]`/`[PENDING_APPLY]`), `WorkspaceWriter` | `MERGE_LOCK` (`task_isolation.rs:86`) только process-local; `WorkspaceWriter` не даёт мультифайловых транзакций и не защищает от произвольного shell / конкурентного `.git/index.lock` |

---

## 2. Целевая архитектура одновременной работы и синхронизации

### 2.1 `TargetRegistry`, `TargetRouter` и `Exclusivity::Key`
- Ввести `TargetId` (`local`, `ssh:<host>`, `acp`, `fastproot`) и кэш `TargetCaps` (OS/arch, toolchains, warm/cold LSP, remote roots) с инвалидацией при ошибке транспорта.
- Заменить `Tool::is_exclusive() -> bool` на `Exclusivity { Global, Key(String) }`:
  - `bash` получает `Key(target.fs_epoch)` — локальный `edit` / локальный `bash` и удалённый `cargo test` работают **параллельно**, а команды на одном workspace-срезе по-прежнему идут последовательно.
  - Per-target семафор + общий `max_concurrency` (`supervisor.rs:183–184`), сохранение порядка по `tool_call_id`.

### 2.2 Узкий протокол `arag __omp_worker_remote` vs `--mode rpc`
- **Для offload инструментов, файлов и LSP:** скрытая подкоманда `arag __omp_worker_remote` через `ssh host arag __omp_worker_remote` (NDJSON + chunking из `rpc/frame.rs`, token auth по образцу `process_broker/protocol.rs:20–40`, операции `probe`, `read/stat/list`, `exec` с `-pgid`, `ProcessBackend` jobs, `sync_put`/`sync_get`, `diff`, `lsp_attach`).
- **Для делегирования автономного агента на удалённую ОС:** существующий `arag --mode rpc` (`main.rs:1320–1434`), строго по явному opt-in (`target.agent: true`), т.к. требует ключи провайдера на целевой машине.

### 2.3 Слепок, CAS и возврат изменений (с поправками ревью)
1. **Git baseline:** `git bundle` + `diff --binary` (`task_isolation.rs:1034–1195`) в изолированный remote workspace (никогда не делить общий `.git/index.lock`), с исключением `target/`, `node_modules/`, escaping symlinks/FIFO (`pi-iso/src/rcopy.rs:70–80`).
2. **Не-git / ленивые ссылки `remote://<target>/<path>@<rev>`:** `xxh3_64` допустим только для быстрого обнаружения изменений; **для проверки целостности CAS-блобов от недоверенного remote обязателен SHA-256** (поправка ревью).
3. **Cross-OS Git нюансы:** на границе Android/Linux/Windows/macOS явно фиксировать правила для `core.autocrlf`/`.gitattributes`, executable bits, `core.symlinks`, case collisions и переименований.
4. **Apply-back:** возврат диффов через 3-way merge (`[AUTO-APPLIED]`/`[PENDING_APPLY]`), захват всех `WorkspaceWriter` lease до начала записи и побайтовая проверка (`replace_if_unchanged`), без ложных обещаний атомарного отката при сбое посреди multi-file записи.

### 2.4 `EnvPlan` и `PathMap` (с поправками ревью)
- **`EnvPlan`:** env удалённого login-shell считать **недоверенным входом** (может исполнять startup-скрипты и содержать секреты). Явный allowlist; `PREFIX`, `LD_PRELOAD`, `LD_LIBRARY_PATH`, `NODE_OPTIONS`, `PYTHONPATH`, `CARGO_HOME` и `SSH_AUTH_SOCK` исключены по умолчанию (`-A` только явный opt-in с блокировкой config-override).
- **`PathMap` и LSP URI remap:** обрабатывать не только `rootUri` и `Location.uri`, но также:
  - вложенные `LocationLink`, `WorkspaceEdit`, `workspace_diagnostics`, версии документов и server-initiated запросы;
  - несуществующие пути (создание файлов), symlink TOCTOU, `%`-экранированные разделители, не-UTF-8 пути, case-insensitive ФС, пути внешних зависимостей (напр. удалённый `~/.cargo/registry/...` маппится в read-only `remote://`, а не в отсутствующий локальный путь).

---

## 3. Лестница деградации и инварианты безопасности

1. **Exec-лестница (строго ДО отправки кадра):** `RemoteWorker` → `AcpTerminal` → `LocalIso`/`LocalNative` → `DegradedStatic` (`ast_grep`/`grep` + явная ошибка).
   - **Запрещено:** автоматически повторять мутирующую команду при потере связи. Разделять: ошибка/таймаут до отправки (`NotStarted`) и обрыв после `exec`/`start`/`write` (**`UnknownOutcome`** в durable store, с явным предупреждением в `/retry`).
2. **LSP-лестница:** `Warm remote mux` → `Cold remote LSP (bounded init)` → `Local LSP` → `pi-ast + RepoMap + grep` с меткой `degraded`.
   - **Запрещено:** молча мигрировать мутирующие LSP-операции (`rename`, `format`, `code_action`) между разными backend'ами. Деградируют только read-only запросы. Таймаут холодного старта не кэшировать (`lsp/client.rs:799–830`).
3. **`BashPolicy` & D20 parity:** `BashPolicy` (`approve.rs:447–485`) проверяет **сырую** команду на любой target (`Forbidden` непобедим); закрыть D20 и `env_clear()` в `ssh.rs` и `lsp/{config,client}.rs`.

---

## 4. Поэтапная дорожная карта (M0–M3)

| Этап | Задачи | Критерий приёмки |
|---|---|---|
| **M0** | D20-гейт для `.omp/ssh.json` и project LSP `command`; `env_clear()` в `ssh.rs:356` и `lsp/client.rs:860`; фикс `read.rs:688` для ACP `PathMap` | Untrusted проект не запускает LSP-команды; SSH/LSP не утекают секреты и `SSH_AUTH_SOCK` |
| **M1** | `Exclusivity::Key`, `arag __omp_worker_remote`, `RemoteProcessBackend`, durable `UnknownOutcome` | Одновременный local edit + remote `cargo test`; обрыв даёт `UnknownOutcome` без повтора |
| **M2** | Git bundle + SHA-256 CAS, cross-OS Git правила, 3-way merge `[AUTO-APPLIED]`/`[PENDING_APPLY]` | Локальные правки не затираются; `target/`/`node_modules/` не синхронизируются |
| **M3** | Streamlocal LSP mux + полный remap (`LocationLink`/`WorkspaceEdit`) + read-only degrade в `pi-ast` | Падение remote LSP не роняет ход; `rename` не уходит молча на локальный LSP |
