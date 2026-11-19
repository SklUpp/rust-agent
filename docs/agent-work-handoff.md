# Состояние проекта и задачи

Обновлено 2026-11-19. Фактическое состояние после MCP-миграции, точного redaction и закрытия версии/таймаутов.

## Проект

`aragent` — самостоятельный coding-first AI runtime на Rust, бинарник `arag`, целевые платформы Linux и Termux/Bionic. Репозиторий `/data/data/com.termux/files/home/rust-agent`, ветка `feat/standalone-runtime`, HEAD `2cd96f3`. Версия пакета `0.1.0` со схемой в `README.md` (`## Versioning`); версия схемы БД версионируется отдельно. Донор `/data/data/com.termux/files/home/omp-work` — только чтение. План: `docs/agent-preliminary-implementation-plan.md`; решения: `docs/agent-architecture-decisions.md`; MCP: `docs/agent-provider-mcp-contract-proposal.md`.

## Проверено

- CI зелёный на `3c5d948` (дефолты таймаута), `7f90830` (MCP-исправления) и `44388709069` (HTTP-статус + ошибочные пути). Гоняются `cargo check`, полный host-набор `aragent` и `pi-agent`, persistent shell и env allowlist, реальный CLI против локального OpenAI-compatible фикстура, сборка `aarch64-linux-android`.
- Живой прогон на Termux (сборка из CI до MCP-изменений), 9Router `http://127.0.0.1:20128/v1`, модель `9router/ocg/deepseek-v4.1-flash`: `read` вернул точное содержимое; SQLite `sessions` 1 / `branches` 1 / `messages` 5 / `executions` 1 / `stream_drafts` 0; `--resume` без повторного инструмента; SIGINT оставил черновик 2628 символов, следующий ход показал его модели один раз и очистил строку.
- MCP Streamable HTTP через `rmcp` покрыт тестами против локальных фикстур: `mcp.rs` (включая category-only транспортную ошибку без URL и без тела), `mcp_boundaries.rs` (definite-ошибки, epoch-изоляция, один reconnect на несколько вызовов, close против reconnect), `mcp_http_cancel.rs` (границы по времени), `mcp_redirect_contract.rs`. Внешний MCP-сервер вживую не проверялся.
- Redaction известных значений: `auth/known_secrets.rs` (aho-corasick, leftmost-longest, raw + percent в двух регистрах + base64/URL-safe с паддингом и без), регистрация в `api_key_from`, `api_key_for_core`, `peek_api_key`, `resolve_api_key` конфига и в OAuth-резолвере security-сканов; применяется в черновиках, ошибках и в payload сообщений при записи. Тесты: `redact_free_text_masks_registered_credentials_exactly`, `config_api_key_is_registered_for_exact_redaction`, `appended_message_payload_is_redacted_before_persist`.
- Окно доставки черновика — одна константа `STREAM_DRAFT_DELIVERY_CHARS = 8000`, общая для доставки и очистки; граница проверена тестом `stream_draft_delivery_window_keeps_newest_tail_char_exact`.
- Независимые ревью: streaming/timeout (2 прохода), MCP lifecycle (проход + повторный по исправлениям), redaction (найдено 5 дефектов покрытия — исправлены в `2cd96f3`).

## Незавершённая работа

1. **S4 durable `task`** — fail-closed до доказанных admission и crash recovery; cached durable revive — до producer generation, сохранения принятых сообщений и FIFO writer barrier.
2. **S5 SQLITE_BUSY** — безопасная граница до statements и cancellation-aware контракт; повтор вокруг неоднозначного commit запрещён.
3. **S3 процессы** — доказать TERM→KILL группе; потомки, вышедшие из группы, не гарантируются; shell под тем же UID — не sandbox.
4. **S8–S12** — semantic matrix, extraction pipeline, export/inspector, параллельные пишущие workers с merge engine, hooks runtime и packaging-контракт.
5. **Redaction, остаток** — base64 токена внутри крупного блока (`Basic base64(user:token)`, payload JWT) требует декодирования кандидатов; сообщения, записанные до `2cd96f3`, и таблицы executions/inbox не проходят через новый слой; `local_memory.rs` имеет собственный эвристический scrub.
6. **Кросс-срезно** — миграция `~/.omp` → `~/.aragent` без удаления источника, `/retry`/`Alt+R`, license/NOTICE audit, живой smoke HTTP MCP против внешнего сервера.

## Ограничения окружения

- Локальная сборка на Termux по-прежнему нежелательна: `Cargo.lock` тянет `aws-lc-sys`/`cmake` через HTTP-зависимости MCP. Проверки — в CI; локально допустим только короткий smoke готового артефакта.
- `security-reviewer` не имеет выбранной модели в этом окружении; security-разборы выполняются `reviewer` с явной формулировкой threat model.
- Один писатель в основном дереве; параллельные писатели — в отдельных worktree.
- Три файла `packages/coding-agent/src/prompts/security/scan-coordinator.md`, `packages/coding-agent/src/prompts/tools/security-publish.md`, `packages/coding-agent/src/prompts/tools/security-scan.md` изменены пользователем — не трогать и не включать в коммиты.
- Индекс `.graphtrail/retry.db` устарел после переименования `omp-cli` → `aragent`; выводы по нему проверять по исходникам.

## Рабочее дерево

Закоммичено и отправлено: streaming durability, model timeout/retry, версия `0.1.0`, MCP на `rmcp` целиком (stdio, Streamable HTTP, legacy SSE, тесты), точный redaction известных секретов, единое окно доставки черновика, документация. Незакоммичено: `.trash-2026-11-12/` (старый `transports/http.rs`), три пользовательских prompt-файла, правки этого документа. Перед продолжением сверять `git status` и не откатывать чужие изменения.
