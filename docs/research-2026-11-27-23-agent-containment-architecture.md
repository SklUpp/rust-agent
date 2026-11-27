# Архитектура сдерживания и контейнеризации внутри `arag` (2026-11-27)

> **Источники:** `ResearchAgentContainmentArchitecture` (аудит всех точек спавна, мутации ФС, брокеров, БД и кредов в `~/rust-agent`, `~/tmp/fastproot`, `~/tmp/lkl-probe`).
> **Границы проверки:** статический аудит исходного кода без запуска сборок и тестов.

---

## 1. Инвентаризация точек спавна и хранения

- **Защищённые швы:** `process_broker` (`0700` dir, `0600` O_EXCL/O_NOFOLLOW token, `SO_PEERCRED`, constant-time compare, `0600` socket/meta — `protocol.rs:444-599`, `broker.rs:735-755,973-981`), `ssh://` (`env_clear`, `ForwardAgent=no`, без `SSH_AUTH_SOCK` — `ssh.rs:392-490`), `auth/store.rs:176-187` (`0700`/`0600`), `trust.rs:346-390`, `approve.rs:382-610`.
- **Выявленные пробелы сдерживания в текущем коде:**
  1. **P1 (`lsp/mux/server.rs:270-287`, `client.rs:120-158`):** Unix-сокет LSP mux создаётся без `0700`/`0600`, без `SO_PEERCRED` и без проверки владельца/симлинка клиентом.
  2. **P1 (`eval/bridge.rs:89,289`):** один общий bearer-токен на процесс и выбор `(session, run)` из JSON-тела запроса (код из `eval` может вызывать инструменты другой активной сессии).
  3. **P1 (`tools/mod.rs:2643,871-891`, `approve.rs:129-142`):** обычный `write` разрешает пути за пределами `cwd` и имеет TOCTOU-окно между `resolve_symlink_target` и `fs::rename`, при этом `xd://resolve` в `approve.rs` классифицируется как `Read`.
  4. **P1 (`hooks.rs:628-655`):** дочерние процессы хуков стартуют без `env_clear()` и наследуют ключи провайдеров и `SSH_AUTH_SOCK`.
  5. **P2 (`task_isolation.rs:644-654`):** `run_git` не сбрасывает переменные `GIT_DIR`/`GIT_WORK_TREE`/`GIT_CONFIG_*`/`GIT_ASKPASS`/`GIT_SSH_COMMAND`.
  6. **P2 (`session_store.rs:2303-2318`):** `sessions.db` открывается без явного `0700`/`0600` (в отличие от `auth/store.rs`).
  7. **P2 (`mcp/transports/stdio.rs:393-399`, `dap/client.rs:471-497,769-781`):** MCP stdio убивает только прямой PID (без `-pgid`); DAP TCP имеет окно гонки на loopback-порту без аутентификации и позволяет model `env` перезаписать `PATH`/`LD_PRELOAD`; `state::platform_env` (`state.rs:1612-1629`) сохраняет `LD_PRELOAD`/`NODE_OPTIONS` для SSH/LSP/DAP/MCP.

---

## 2. Целевая архитектура: Host Control Plane + Contained Execution Plane

### 2.1 Почему нельзя делить SQLite и креды через границу контейнера
1. `DurableWriter` использует single-writer модель с `BEGIN IMMEDIATE` (`durable_session.rs:1-6,363-503`): второй писатель из контейнера разрушит модель admission.
2. SQLite WAL (`-shm` mmap + POSIX locks) ломается при remapping UID, в user namespaces или через границу контейнера/VM.
3. По D27 изолированная среда выполнения не должна иметь доступа ни к `agent.db`, ни к `sessions.db` (в истории есть выводы инструментов), ни к `trust.json` / `known_secrets`.

### 2.2 Разделение плоскостей

| Плоскость | Владение | Контракт на границе |
|---|---|---|
| **Host Control Plane (доверенная)** | `sessions.db` (`DurableWriter`), `auth/agent.db`, `trust.json`, `known_secrets`, `BashPolicy`/`Approve`, `task_admissions`, `MERGE_LOCK`, публикация артефактов D81 (`atomic_publish.rs`), транспорты **LSP / DAP / MCP** | Фиксирует `intent` до отправки запроса; применяет политику до маршрутизации; инжектит токены только на host-границе. |
| **Contained Execution Plane (недоверенная)** | `bash`, `hub`, `eval` kernels, дочерние `task` в RW workspace + RO rootfs без `~/.aragent` и без секретов | Возвращает `StartAck(pid/scope)` или `NotStarted`; при потере связи host фиксирует `UnknownOutcome` без автоповтора; idempotent `status(execution_id)` при восстановлении. |

### 2.3 Фасад `ExecutionSandbox` и `PathSpace`
- **Не включать LSP/DAP/MCP в `ExecutionSandbox` целиком:** их жизненный цикл, framing, epoch и reconnect уже реализованы поверх `TransportPolicy`; в песочницу можно делегировать только сам `spawn` при отсутствии хост-секретов.
- **Единый `PathSpace` (`RootId` + `physical` authority):** устраняет рассинхронизацию путей между host-инструментами и гостевыми процессами `bash`/`task`, но не заменяет дескрипторно-безопасные операции (`O_NOFOLLOW` / `openat` / `dev+ino`) против symlink TOCTOU.
- **Артефакты D81 и merge:** EP пишет только в отдельный staging-каталог (без права на `hard_link` внутрь host artifact root); Host CP проверяет размер, переносит/публикует без перезаписи и пишет ссылку в БД последней. Merge (`MERGE_LOCK` + `WorkspaceWriter::acquire_workspace` flock) остаётся на Host CP.
- **`eval` bridge (`eval/bridge.rs`):** заменить единый токен процесса на per-`(session, run)` / `execution_id` токен.

---

## 3. Порядок внедрения без ломки инвариантов
1. **Немедленно (гигиена границ на текущем runtime):** `env_clear()` в `hooks.rs`; `0700`/`0600` + `SO_PEERCRED` для `lsp/mux`; сброс `GIT_*` в `task_isolation::run_git`; `-pgid` kill в `mcp/transports/stdio.rs`; `0600` на `sessions.db`.
2. **Этап 1 (Model 1 + `PathSpace`):** per-`execution_id` токен `eval/bridge.rs`, containment + descriptor-safe `write` (`O_NOFOLLOW`), `PathSpace` для `write`/`edit`/`glob`/`grep`/`task_isolation`.
3. **Этап 2 (Model 2 / `ExecutionSandbox` в `pi-sys`):** разделение Host CP / EP, EP effect journal с `UnknownOutcome` и `status(execution_id)`, подключение Linux userns+overlay+Landlock+seccomp и опционального `fastproot` compat на Android.
