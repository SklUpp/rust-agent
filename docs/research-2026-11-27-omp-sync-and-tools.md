# Исследование: выборочный перенос из OMP и оптимизация/унификация инструментов `arag` (2026-11-27)

> **Статус:** ненормативное исследование + независимое ревью (`ResearchOmpSync2`, `ResearchToolsUnify2`, `ResearchReviewCoreEcosystem`).
> **Базовые якоря:** `~/rust-agent` @ `feat/standalone-runtime` (`3b021a58`), `~/omp-work` (`3008cfd0`, upstream pin `0b5c27ab`), `~/omp-termux` (`61325cdd`, upstream pin `e1a9f4b4`).
> **Границы проверки:** только чтение исходников и release notes; `cargo`/`rustc`, тесты на устройстве и замеры аллокаций/латентности не запускались.

---

## Часть 1. Выборочная синхронизация `OMP` / `omp-termux` → `arag` (Track 1)

### 1.1 Точки расхождения и актуальные пины

| Репозиторий | Пин / HEAD | Статус |
|---|---|---|
| `rust-agent` (`arag`) | `3b021a58` (`feat/standalone-runtime`) | Самостоятельный Rust runtime; `pi-builtins` / `pi-agent` / `aragent` |
| `omp-work` | `3008cfd0` (upstream `0b5c27ab`) | TS + `crates/omp-cli` донор (v19.x база) |
| `omp-termux` | `61325cdd` (upstream `e1a9f4b4`) | Termux/Bionic патчи и Android-фиксы |

### 1.2 Кандидаты на перенос и вердикты ревью

| Область | Якорь источника → Цель в `arag` | Классификация | Поправки и гейты ревью (`ResearchReviewCoreEcosystem`) |
|---|---|---|---|
| **Android `sort` OOM / memory cap** | `omp-termux` → `crates/pi-builtins/src/sort.rs:113–120` | **Port (адаптировать)** | Подтверждено: `sort.rs` на Android падает к физическому RAM (`available_memory` только на Linux). Порт memory-aware лимита оправдан, но тяжесть OOM/LMK требует замера на устройстве |
| **Bionic `inotify_rm_watch`** | `omp-termux` `61325cdd` (`inotify_rm_watch` c_int/u32 конверсия) | **Port при использовании `inotify`** | Подтверждено по коммиту `61325cdd`. Учесть, что Bionic-сигнатура отличается от glibc |
| **MCP retry / reconnect** | Upstream OMP 19.1/19.2 release notes → `crates/pi-agent/src/mcp/client.rs:565–635, 915–940` | **No-Go на слепой порт** | **Поправка ревью:** утверждение «у `arag` нет retry в MCP» неверно — `tools/list` уже переподключается и ретраит, а `tools/call` **намеренно** не повторяет `UnknownOutcome`. Автоповтор мутаций запрещён (R1/R2); допустимо ретраить только доказанную недоставку до отправки |
| **Background process cleanup** | Upstream OMP 19.1/19.2 → `pi-agent/src/tools/process.rs`, `pi-shell` | **Adapt (высокий риск)** | Не мелкий прямой порт: обязательно сохранить ownership supervisor/detached задач, защиту от PID reuse, RPC-семантику, session ownership и проверить на Android (Phantom Process Killer) |
| **`sed` edge cases / cache warming / retained workspaces / `pidfd`** | Release notes OMP 19.1/19.2 | **Verify first (не drop-in)** | Release notes не доказывают наличие бага в Rust-порте. `sed` и cache warming сначала воспроизвести тестом; `pidfd` на Android пре-API-31 даёт `SIGSYS` |

---

## Часть 2. Инструменты `arag`: эффективность, унификация, упрощение (Track 2)

### 2.1 Ключевые архитектурные границы (подтверждено ревью)

1. **Approval tier ≠ Scheduler exclusivity:**
   - `crates/aragent/src/approve.rs:113–200` классифицирует вызов в `Read` / `Write` / `Exec` по имени и аргументам.
   - `crates/pi-agent/src/tools/scheduler.rs:80–105` планирует параллельность по `Tool::is_exclusive()`, а не по approval tier. `run_concurrent` использует неограниченный `join_all`.
   - `checkpoint` и `rewind` (`crates/aragent/src/checkpoint.rs:494–595`) **строго эксклюзивны** — делать их конкурентными запрещено.
2. **Гарантии `WorkspaceWriter` (`crates/pi-agent/src/workspace_writer.rs:1–23, 125–153`):**
   - Использует канонизированные per-file `flock` lease и полную побайтовую/version проверку (`replace_if_unchanged`).
   - **Официально не даёт** cross-file атомарных транзакций или CAS между несколькими файлами.
3. **AST staging (`ast_edit`):**
   - `ast_edit` складывает изменения в общий `PendingStore` / `SnapshotStore`; фактическая валидация и запись происходят позже через `write xd://resolve`.

### 2.2 Что брать и что запрещено при оптимизации/унификации

| Предложение | Вердикт | Обязательные инварианты / поправки ревью |
|---|---|---|
| **Ограничить параллелизм `run_concurrent`** (`scheduler.rs`) | **Go** | Вместо unbounded `join_all` ввести bounded semaphore (особенно важно на Android по RAM/FD). Сохранить порядок результатов по `tool_call_id` и тесты отмены/durability |
| **Вынести блокирующий I/O / холодные обходы** | **Go (после тестов)** | Проверить cancellation и порядок доставки событий; числа экономии аллокаций/латентности в исходном отчёте — расчётные `[INFERENCE]`, не бенчмарк |
| **Кэш схем инструментов (`tools`)** | **Go с инвалидацией** | Обязательна инвалидация при смене сессии, allowlist инструментов, MCP-подключений и lookup по wire-именам |
| **Общий `WalkSpec` и бюджеты вывода** | **Go с оговоркой** | Сохранить индивидуальные лимиты инструментов; усечение (truncation) **не должно** молча обрывать машиночитаемые JSON-структуры или continuation-метаданные |
| **Упростить `goal` tool до `Read`** (`goal_tool.rs:116–159`) | **No-Go (частично)** | Нельзя классифицировать весь `goal` как `Read`: `create`, `resume`, `drop` и completion мутируют состояние. Кандидат на `Read` — только строго read-only `get` |
| **Слить `ast_grep` + `ast_edit`** | **Только с жёстким разделением** | Не просто слияние схем: действие `rewrite` обязано оставаться `Write`-тиром и exclusive, сохранять `PendingStore`, проверку устаревания файла, `WorkspaceWriter` lease и не обходить `xd://resolve` |
| **Batch `multi_edit` / `multi_read`** | **Adapt (с контрактом ошибок)** | `multi_edit` требует явной семантики partial failure (атомарности на уровне ФС нет), per-file full-version validation и захвата всех lease до записи. `multi_read` обязан классифицировать **каждый** путь (включая смешанные local + `ssh://`) до запуска. Примечание: scheduler уже запускает параллельно несколько `read` в одном batch — ограничение concurrency экономит пиковые ресурсы, но не убирает раунды модели при последовательных запросах |

---

## Часть 3. Итоговые приоритеты по этому пакету

1. **Низкий риск, быстрая отдача:** ограничение `run_concurrent` семафором; порт memory-aware `sort.rs` на Android (после замера); инвалидируемый кэш схем инструментов.
2. **Требует контрактов и тестов:** любые изменения `ast_edit`/`multi_edit`, `WalkSpec`/truncation и background-process cleanup.
3. **Запрещено без смены модели безопасности:** слепой retry `tools/call` в MCP, снятие эксклюзивности с `checkpoint`/`rewind`, перевод мутирующих `goal`-операций в `Read`.
