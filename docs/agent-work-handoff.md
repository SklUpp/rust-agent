# Состояние проекта и задачи

Обновлено 2026-11-19. Состояние после MCP-миграции, точного redaction, закрытия версии/таймаутов и R0.1–R0.3. Дальнейший порядок работ — `docs/agent-preliminary-implementation-plan.md`, раздел «Актуальный план» (R0–R12) с примечаниями.

## Проект

`aragent` — самостоятельный coding-first AI runtime на Rust, бинарник `arag`, целевые платформы Linux и Termux/Bionic. Репозиторий `/data/data/com.termux/files/home/rust-agent`, ветка `feat/standalone-runtime`, HEAD `7901f49` (схема БД v6, `task_admissions`). Версия пакета `0.1.0` со схемой в `README.md` (`## Versioning`); версия схемы БД версионируется отдельно. Донор `/data/data/com.termux/files/home/omp-work` — только чтение. План: `docs/agent-preliminary-implementation-plan.md`; решения: `docs/agent-architecture-decisions.md`; MCP: `docs/agent-provider-mcp-contract-proposal.md`.

## Проверено

- CI зелёный на `2b8d1bb` (запуск `44553928264`, R0.1/R0.3 + срез R1 со схемой v5): host-набор, persistent shell, CLI против локального фикстура, `aarch64-linux-android`. Гоняются `cargo check`, полный host-набор `aragent` и `pi-agent`, persistent shell и env allowlist, реальный CLI против локального OpenAI-compatible фикстура, сборка `aarch64-linux-android`.
- Живой прогон на Termux (сборка из CI до MCP-изменений), 9Router `http://127.0.0.1:20128/v1`, модель `9router/ocg/deepseek-v4.1-flash`: `read` вернул точное содержимое; SQLite `sessions` 1 / `branches` 1 / `messages` 5 / `executions` 1 / `stream_drafts` 0; `--resume` без повторного инструмента; SIGINT оставил черновик 2628 символов, следующий ход показал его модели один раз и очистил строку.
- MCP Streamable HTTP через `rmcp` покрыт тестами против локальных фикстур: `mcp.rs` (включая category-only транспортную ошибку без URL и без тела), `mcp_boundaries.rs` (definite-ошибки, epoch-изоляция, один reconnect на несколько вызовов, close против reconnect, редирект с HTTP-статусом), `mcp_http_cancel.rs` (границы по времени), `mcp_redirect_contract.rs`. Живой внешний сервер: `context7-mcp`, прогон `arag`: `initialize` → `tools/list` → `tools/call` (ответ `/tokio-rs/tokio`), после выхода процесс сервера не остаётся; reconnect после `kill -9` вживую не проверялся.
- Redaction известных значений: `auth/known_secrets.rs` (aho-corasick, leftmost-longest, raw + percent в двух регистрах + base64/URL-safe с паддингом и без), регистрация в `api_key_from`, `api_key_for_core`, `peek_api_key`, `resolve_api_key` конфига и в OAuth-резолвере security-сканов; применяется в черновиках, ошибках, payload сообщений, `executions.intent_json/outcome_json`, `deliveries`, `worker_completions`, `sessions.payload_json` (заголовок) и метаданных артефактов при записи; повторный запрос с raw-значением остаётся идемпотентным. Тесты: `redact_free_text_masks_registered_credentials_exactly`, `config_api_key_is_registered_for_exact_redaction`, `appended_message_payload_is_redacted_before_persist`, `execution_delivery_artifact_and_header_rows_are_redacted_before_persist`.
- Durable admission дочерних задач: схема БД v6, `task_admissions` (`admitted` → `started` → `completed`/`failed`/`abandoned`, identity `<worker>@<generation>`), `producer_generation` из БД (`MAX+1`), redacted `spec_json`; `TaskTool` пишет admission до открытия scope/спавна, открытие scope → `started`, завершение/ошибка/отмена → терминал, переоткрытие сессии → `abandoned`. Тесты: `task_admission_is_durable_redacted_and_single_flight`, `schema_v4_upgrades_and_accepts_task_admissions`, `schema_v5_index_upgrade_allows_worker_reuse_in_a_new_generation`, `child_admission_is_durable_and_abandoned_on_reopen`, `admission_state_matches_the_child_outcome`. Не сделано: generation в spool/JSONL, FIFO barrier.
- Окно доставки черновика — одна константа `STREAM_DRAFT_DELIVERY_CHARS = 8000`, общая для доставки и очистки; граница проверена тестом `stream_draft_delivery_window_keeps_newest_tail_char_exact`.
- Независимые ревью: streaming/timeout (2 прохода), MCP lifecycle (проход + повторный по исправлениям), redaction (найдено 5 дефектов покрытия — исправлены в `2cd96f3`), durable admission (2 прохода; исправлены terminal mapping, v6-миграция и `cancelled` → `abandoned`; итоговый verdict — APPROVE на `7901f49`).

## Ближайшие работы

Полный порядок с критериями и проверками — в `docs/agent-preliminary-implementation-plan.md` («Актуальный план», R0–R12). Кратко:

1. **R0** — R0.1 закрыт (redaction `executions.intent_json/outcome_json`, `deliveries`, `worker_completions`, `sessions.payload_json` включая заголовок, метаданные артефактов; идемпотентность повторов сохраняется для строк до redaction) и R0.3 закрыт (`map_initialize_error` оставляет категорию и шаг rmcp, но не URL/тело; статус извлекается из `HttpStatus` и reqwest-ошибки). R0.2 не закрыт — ограничение P3: отмена `initialize` не закрывает сокет в пределах 5 с даже после SSE-заголовков; транспорт rmcp — `WorkerTransport`, закрытие зависит от hyper/reqwest, общий request timeout недопустим (сломает долгие SSE-вызовы). R0.4 выполнен частично: живой `initialize` → `tools/list` → `tools/call` через внешний `context7-mcp` (ответ `/tokio-rs/tokio`), процесс сервера после выхода не остаётся; reconnect после `kill -9` вживую не проверялся (покрыт `mcp_boundaries`).
2. **R1** — S4 durable `task`: слой хранения готов (схема БД v6; `task_admissions` с `admitted`/`started`/`completed`/`failed`/`abandoned`, identity `<worker>@<generation>`, `producer_generation` в БД, redaction `spec_json`, API `admit_task`/`mark_task_admission_started`/`finish_task_admission`/`task_admission`/`open_task_admissions`/`next_producer_generation`/`abandon_open_task_admissions`, тесты на переоткрытие, single-flight, миграции v4→v6 и v5→v6, abandoned-epoch). Wiring готов: admission до scope/спавна, `started` при открытии scope, терминал на complete/failed/abort, recovery при переоткрытии. Осталось: generation в spool/JSONL, FIFO barrier для child-сообщений.
3. **R2** — S5 `SQLITE_BUSY`: граница до statements, cancellation-aware контракт, без повтора вокруг неоднозначного commit.
4. **R3** — S3 процессы: TERM→KILL группе, lease, восстановление `cwd`.
5. **R4–R8** — S8 semantic matrix, S9 память, S10 export/inspector, S11 параллельные писатели с merge, S12 hooks/packaging.
6. **R9** — миграция `~/.omp` → `~/.aragent` (копия, dual-read) и license/NOTICE audit.
7. **R10** — `/retry` и `Alt+R`: ручной повтор по решению пользователя, предупреждение о прерванном ходе и об неизвестном исходе (автоповтора нет).
8. **R11** — `arag doctor` (+ `--fix` для безопасных действий): проверки `db`, `sessions`, `redaction`, `artifacts`, `config`, `locks`, `permissions`, `env`; отчёт `--json`, exit codes 0/1/2.
9. **R12** — релиз `0.2.0` с артефактами SHA+`arag.sha256`; `MAJOR` остаётся 0 минимум до R7.

Решено 2026-11-19: `MAJOR = 0` до R7; `/retry` — ручное действие с предупреждением и подтверждением для неизвестного исхода; `arag doctor` — штатная команда. Открыт только backfill старых строк под redaction (по умолчанию не делается, только opt-in с резервной копией).

## Ограничения окружения

- Локальная сборка на Termux по-прежнему нежелательна: `Cargo.lock` тянет `aws-lc-sys`/`cmake` через HTTP-зависимости MCP. Проверки — в CI; локально допустим только короткий smoke готового артефакта.
- `security-reviewer` не имеет выбранной модели в этом окружении; security-разборы выполняются `reviewer` с явной формулировкой threat model.
- Один писатель в основном дереве; параллельные писатели — в отдельных worktree.
- Три файла `packages/coding-agent/src/prompts/security/scan-coordinator.md`, `packages/coding-agent/src/prompts/tools/security-publish.md`, `packages/coding-agent/src/prompts/tools/security-scan.md` изменены пользователем — не трогать и не включать в коммиты.
- Индекс `.graphtrail/retry.db` устарел после переименования `omp-cli` → `aragent`; выводы по нему проверять по исходникам.

## Рабочее дерево

Закоммичено и отправлено: streaming durability, model timeout/retry, версия `0.1.0`, MCP на `rmcp` целиком (stdio, Streamable HTTP, legacy SSE, тесты), точный redaction известных секретов, единое окно доставки черновика, документация. Незакоммичено: `.trash-2026-11-12/` (старый `transports/http.rs`), `crates/aragent/src/tui.rs` и три prompt-файла — изменения пользователя, не трогать. Перед продолжением сверять `git status` и не откатывать чужие изменения.
