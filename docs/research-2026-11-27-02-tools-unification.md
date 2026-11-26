# Track 2: Оптимизация, унификация и упрощение инструментов `arag` (2026-11-27)

> **Источники:** `ResearchToolsUnify2` + аудит `ResearchReviewCoreEcosystem`.
> **Якоря:** `~/rust-agent/crates/pi-agent/src/{tools/,lsp/,workspace_writer.rs}`, `crates/pi-builtins/`, `crates/pi-ast/`, `crates/aragent/src/{approve.rs,checkpoint.rs,goal_tool.rs,runtime.rs,autolearn.rs,advisor_tool.rs}`.
> **Границы проверки:** только инспекция кода; цифры экономии аллокаций/латентности в скауте являются расчётными `[INFERENCE]`, реальный профайлинг не запускался.

---

## 1. Подтверждённые инварианты архитектуры инструментов

1. **Approval tier и Scheduler exclusivity — разные контракты (`ResearchReviewCoreEcosystem`):**
   - `crates/aragent/src/approve.rs:113–200` классифицирует вызов в `Read` / `Write` / `Exec` по имени и аргументам.
   - `crates/pi-agent/src/tools/scheduler.rs:80–105` планирует параллельность по `Tool::is_exclusive()`, а не по approval tier.
   - `checkpoint` и `rewind` (`crates/aragent/src/checkpoint.rs:494–595`) явно эксклюзивны — параллелить их нельзя.
   - `run_concurrent` сейчас использует неограниченный `join_all`.
2. **Гарантии `WorkspaceWriter` (`crates/pi-agent/src/workspace_writer.rs:1–23, 125–153`):**
   - Канонизированный per-file `flock` lease + полная побайтовая/version сверка (`replace_if_unchanged`).
   - Документация модуля **явно отрицает** наличие cross-file атомарных транзакций или CAS между несколькими файлами.
3. **AST staging (`ast_edit`):**
   - `ast_edit` записывает отложенный результат в общий `PendingStore` / `SnapshotStore`; фактическая валидация и запись происходят только на этапе `write xd://resolve`.
4. **Инструмент `goal` (`crates/aragent/src/goal_tool.rs:116–159`):**
   - Поддерживает как `get`, так и мутирующие действия `create`, `resume`, `drop` и завершение цели.

---

## 2. Оценка предложений по оптимизации, унификации и расширению

| Направление | Суть предложения | Вердикт ревью | Обязательные ограничения |
|---|---|---|---|
| **Bounded concurrency в `run_concurrent`** (`scheduler.rs`) | Заменить неограниченный `join_all` на семафор (особенно важно на Termux/Android по RAM и FD) | **Go** | Сохранить порядок результатов по `tool_call_id`, тесты отмены и durability |
| **Снятие блокирующего I/O / холодных обходов с async-потока** | Изолировать тяжёлые fs-обходы и I/O | **Go (после тестов)** | Проверить cancellation, backpressure и порядок событий |
| **Кэширование схем и lookup по wire-именам** | Избежать пересборки JSON-схем на каждом шаге цикла | **Go с инвалидацией** | Инвалидация обязательна при смене сессии, allowlist, подключении/отключении MCP-инструментов и алиасах |
| **Общий `WalkSpec` и бюджеты вывода** | Унифицировать фильтрацию `.gitignore`/исключений и лимиты вывода у `find`/`grep`/`repo_map` | **Adapt** | Сохранить специфические лимиты инструментов; усечение **не должно** молча обрезать JSON-структуры или continuation-токены |
| **Оценка «30 `read` = 30 раундов»** | Утверждение о сокращении раундов за счёт батчинга | **Поправка ревью** | Scheduler уже запускает параллельно все non-exclusive вызовы в одном ответе модели. Bounded concurrency бережёт ресурсы, но не устраняет последовательные раунды модели, если она сама вызывает по одному файлу |
| **Слияние `ast_grep` + `ast_edit`** | Объединить структурный поиск и перезапись под одним API | **Adapt (строго)** | Это не просто слить схему: `rewrite` обязан оставаться `Write`-тиром и exclusive, сохранять `PendingStore`/`SnapshotStore`, stale-check, `WorkspaceWriter` lease и не обходить `xd://resolve` |
| **Перевод `goal` в `Read`-тир** | Убрать лишние подтверждения у `goal` | **No-Go целиком** | Только строго read-only операция (`get`) может рассматриваться как `Read`; `create`/`resume`/`drop`/complete мутируют и остаются под текущим гейтом |
| **Пакетные `multi_edit` / `multi_read`** | Добавить батчевые файловые операции | **Adapt (с контрактом)** | `multi_edit` не может обещать FS-атомарность: нужна явная модель partial failure, заблаговременный захват всех lease и per-file проверка полной версии. `multi_read` обязан классифицировать **каждый** путь (включая смешанные local + `ssh://`) до выполнения |
| **Отмена эксклюзивности `checkpoint`/`rewind`** | Ускорить за счёт параллелизма | **No-Go** | Разрушает целостность снапшотов состояния |

---

## 3. Приоритет работ без регрессий

1. **Первая очередь (безопасно, высокая отдача):** bounded `run_concurrent` (`scheduler.rs`), кэш схем с явной инвалидацией, вынос тяжёлого синхронного обхода в `spawn_blocking` с cancellation-тестами.
2. **Вторая очередь (после фиксации контрактов):** единый `WalkSpec` с сохранением structured truncation, `multi_read` с per-path проверкой политики/протокола, выделение read-only `goal get`.
3. **Третья очередь / осторожно:** объединение `ast_grep`/`ast_edit` и `multi_edit` только при 100% сохранении `PendingStore`, `xd://resolve`, `WorkspaceWriter` и явного `PartialFailure` в отчёте модели.
