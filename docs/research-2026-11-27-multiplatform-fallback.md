# Исследование: мультиплатформенная одновременная работа и деградирующий fallback (2026-11-27)

> **Статус:** ненормативное исследование + независимое ревью (`ResearchMultiPlatformFallback`, `ResearchReviewPlatform`).
> **Базовые якоря:** `~/rust-agent` (`crates/pi-agent/src/{tools/ssh.rs,tools/process.rs,tools/mod.rs,tools/scheduler.rs,tools/task_isolation.rs,lsp/,state.rs,durable.rs,workspace_writer.rs}`, `crates/pi-iso`, `crates/pi-vcs`, `crates/aragent/src/{acp/,rpc/,approve.rs,trust.rs,config.rs}`).
> **Границы проверки:** только анализ кода; реальные SSH/LSP-интеграционные прогоны и замеры пропускной способности не выполнялись.

---

## 1. Аудит текущих возможностей и вскрытые пробелы безопасности

| Поверхность | Текущее состояние | Критические пробелы (подтверждено ревью) |
|---|---|---|
| **`ssh://` (`tools/ssh.rs`)** | Только чтение (1 файл ≤ 1 MiB, листинг, POSIX-проба, 30s timeout). Нет write/edit/ControlMaster (`ssh.rs:26-27`) | 1) **D20 gap:** проектный `.omp/ssh.json` загружается без trust-gate (`ssh.rs:864-882`); 2) нет `env_clear()` — процесс наследует весь env, включая `SSH_AUTH_SOCK` (`ssh.rs:356-367`, локальная auth; отсутствия `-A` недостаточно при глобальном ssh config); 3) approval в `approve.rs:86-160` считает `ssh://` в `write/edit` Exec-тиром, хотя tools это ещё не поддерживают |
| **Remote command (`tools/mod.rs`, `acp/`)** | `AcpClientBridge` делегирует в `terminal/create` только для `legacy_one_shot`; persistent shell всегда локальный (`tools/mod.rs:610-645`). `read.rs:688-702` шлёт локальный абсолютный путь в ACP | Отсутствует трансляция путей (`PathMap`); при обрыве связи `ProcessState`/`ProcessError` (`process.rs:95-114,312-325`) не отличают `UnknownOutcome` от провала запуска |
| **LSP (`lsp/config.rs`, `lsp/client.rs`)** | Есть готовый шаблон mux worker (`lsp/mux/`, `main.rs:255-265`). `lsp.enabled` в `config.rs:346-371` по умолчанию `true` (расходится со `SCOPE_LATEST.yaml` R4) | 1) **D20 gap:** project LSP config (`lsp/config.rs:290-320`) выбирает произвольный `command` без trust-store (`trust.rs:34-49` покрывает только hooks/MCP); 2) `LspClient` спавнит без `env_clear()` (`lsp/client.rs:860-873`), отдавая серверу весь env процесса (включая API-ключи) |
| **Планировщик и durability** | `bash` жёстко `is_exclusive() == true` (`tools/mod.rs:600-602`), блокирует параллельный local edit + remote test | `tools/scheduler.rs:235-249` **отклоняет `task` при активной durability**; `DurableError::unknown_outcome` (`durable.rs:43-78`) и `McpError::UnknownOutcome` не покрывают remote exec |
| **Изоляция и запись** | `pi-iso` (`xxh3_64`, `Rcopy` 1 GiB), `task_isolation.rs` (3-way merge, `[AUTO-APPLIED]`/`[PENDING_APPLY]`), `WorkspaceWriter` (per-file `flock` + full-byte check) | `MERGE_LOCK` (`task_isolation.rs:86`) **только process-local**; `WorkspaceWriter` не даёт мультифайловых транзакций и не защищает от произвольного shell / конкурентного Git в shared metadata |

---

## 2. Целевая архитектура одновременного исполнения и проброса

### 2.1 Модель целей (`TargetRegistry`, `TargetRouter`) и параллельность
- Ввести `crates/pi-agent/src/target/` (`TargetId`: `local`, `ssh:<host>`, `acp`, `fastproot`; `TargetCaps`: OS/arch, toolchains, warm/cold LSP, FUSE, remote roots; кэш пробы по `(dest, key, port)` с инвалидацией по ошибке).
- Заменить `fn is_exclusive(&self) -> bool` на `fn exclusivity(&self) -> Exclusivity { Global | Key(String) }`:
  - `bash` получает `Key(target.fs_epoch)` — локальный `edit`/`bash` и удалённый `cargo test` идут **одновременно**, но два `bash` в одном shell по-прежнему сериализуются.
  - Per-target семафор + общий `max_concurrency` (`supervisor.rs:183-184`), сохранение порядка по `tool_call_id`.

### 2.2 Узкий протокол `arag __omp_worker_remote` vs `--mode rpc`
- **Для offload инструментов и LSP:** новый скрытый subcommand `arag __omp_worker_remote` поверх `ssh host arag __omp_worker_remote` (NDJSON с chunking из `rpc/frame.rs`, token-auth из `process_broker/protocol.rs:20-40`, операции `probe`, `read/stat/list`, `exec` с pgid kill, `ProcessBackend` async jobs, `sync_put`/`sync_get`, `diff`, `lsp_attach`).
- **Для автономного агентского offload:** существующий `arag --mode rpc` (`main.rs:1320-1434`), но только явным выбором (`target.agent: true`), так как требует LLM credentials на удалённом хосте.

### 2.3 Snapshot, CAS и bidirectional sync (с поправками ревью)
1. **Git-native (primary):** `git bundle` + `diff --binary` baseline (`task_isolation.rs:1034-1195`), исключая `target/`, `node_modules/`, escaping symlinks/FIFO (`pi-iso/src/rcopy.rs:70-80`).
2. **Не-git / ленивые ссылки `remote://<target>/<path>@<rev>`:** манифест с hash + **SHA-256 для проверки целостности CAS-блобов** (поправка ревью: `xxh3_64` годен лишь как быстрый detector изменений, но **недостаточен против подделки/коллизий** от недоверенного remote).
3. **Cross-OS Git нюансы (поправка ревью):** на границе Android/Linux/Windows/macOS явно задавать поведение для `core.autocrlf`/`.gitattributes`, executable bits, `core.symlinks`, case collisions и переименований, иначе успешный round-trip молча изменит файлы.
4. **Apply-back:** только в отдельные remote workspace-копии (без гонки с локальным `.git/index.lock`), возврат через 3-way merge (`[AUTO-APPLIED]`/`[PENDING_APPLY]`), захват всех `WorkspaceWriter` lease до записи и `replace_if_unchanged` (но помнить: после I/O-сбоя посреди multi-file записи откат не транзакционен).

### 2.4 `EnvPlan` и `PathMap` (с поправками ревью)
- **`EnvPlan`:** удалённый login-shell env считать **недоверенным входом** (может исполнять startup-скрипты и содержать секреты). Явная фильтрация: `PREFIX`, `LD_PRELOAD`, `LD_LIBRARY_PATH`, `NODE_OPTIONS`, `PYTHONPATH`, `CARGO_HOME` не форвардятся через ОС автоматически; `SSH_AUTH_SOCK` исключён (а `-A` только явный opt-in с предупреждением и блокировкой config-override).
- **`PathMap` и LSP URI remap:** обязан обрабатывать не только `rootUri` и `Location.uri`, но и:
  - вложенные **`LocationLink`**, **`WorkspaceEdit`**, версии документов, `workspace_diagnostics` (ремап путей `cargo`/`go`) и server-initiated запросы;
  - несуществующие пути (для новых файлов), symlink-изменения между проверкой и использованием (TOCTOU), `%`-экранированные разделители, не-UTF-8 пути, case-insensitive ФС, пути внешних зависимостей (напр. удалённый `~/.cargo/registry/...` не существует локально и должен маппиться в read-only `remote://`, а не в несуществующий локальный путь!).

---

## 3. Деградирующий fallback и жёсткие инварианты безопасности

1. **Exec-лестница (только ДО отправки команды):**
   `RemoteWorker` → `AcpTerminal` → `LocalIso`/`LocalNative` → `DegradedStatic` (`ast_grep`/`grep` + честный отказ).
   - **Запрещено:** автоматически повторять мутирующую команду локально или повторно после отправки кадра на remote (`connect/timeout` до кадра = `NotStarted`; обрыв после `exec`/`start`/`write` = **`UnknownOutcome`**, сохраняемый в durable storage и явно предупреждающий в `/retry`).
2. **LSP-лестница:**
   `Warm remote mux (streamlocal)` → `Cold remote LSP (bounded init)` → `Local LSP` → `pi-ast + RepoMap + grep` с меткой `degraded`.
   - **Запрещено:** молча мигрировать мутирующие LSP-операции (`rename`, `format`, `code_action`) между backend'ами (разные offsets/версии дерева). Деградируют только read-only операции (`definition`, `references`, `hover`, `diagnostics`). Cold-start timeout не кэшировать (`lsp/client.rs:799-830`).
3. **`BashPolicy` + D20 Trust Parity:**
   - `BashPolicy` (`approve.rs:447-485`) проверяет **сырую** команду до отправки на любой target; `Forbidden` непобедим. `yolo` не переносится на удалённые хосты автоматически.
   - Обязательно расширить `crates/aragent/src/trust.rs` на проектные `.omp/ssh.json` и проектные LSP-команды (`lsp/config.rs:290-320`), плюс перевести `ssh.rs:356` и `lsp/client.rs:860` на `env_clear()` + `EnvPlan`.

---

## 4. Поэтапный план (если брать в работу)

| Фаза | Содержание | Главный критерий приёмки |
|---|---|---|
| **M0 (Trust & Read Parity)** | D20-гейт на `.omp/ssh.json` и project LSP `command`; `env_clear()` в `ssh.rs` и `lsp/client.rs`; фикс `read.rs:688` для ACP `PathMap` | Untrusted repo не может запустить LSP-команду или прочитать/утечь env; SSH не наследует `SSH_AUTH_SOCK` |
| **M1 (Worker & Concurrent Exec)** | `Exclusivity::Key`, `arag __omp_worker_remote`, `RemoteProcessBackend`, durable `ProcessError::UnknownOutcome` | Параллельный local edit + remote `cargo test`; обрыв связи даёт `UnknownOutcome` без повтора |
| **M2 (Snapshot & Merge-back)** | Git bundle + SHA-256 CAS, cross-OS Git rules, 3-way merge с `[AUTO-APPLIED]`/`[PENDING_APPLY]` | Локальные правки не затираются; `target/`/`node_modules/` исключены |
| **M3 (Remote LSP & Degrade)** | Streamlocal mux / stdio remote LSP, полный remap (`LocationLink`/`WorkspaceEdit`), read-only degrade в `pi-ast` | Убийство remote LSP не роняет ход; `rename` не мигрирует на локальный LSP молча |
