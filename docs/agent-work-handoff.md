# Состояние проекта и задачи

Обновлено 2026-11-19. Состояние после MCP-миграции, точного redaction и закрытия версии/таймаутов. Дальнейший порядок работ — `docs/agent-preliminary-implementation-plan.md`, раздел «Актуальный план» (R0–R10) с примечаниями.

## Проект

`aragent` — самостоятельный coding-first AI runtime на Rust, бинарник `arag`, целевые платформы Linux и Termux/Bionic. Репозиторий `/data/data/com.termux/files/home/rust-agent`, ветка `feat/standalone-runtime`, HEAD `e272f2c`. Версия пакета `0.1.0` со схемой в `README.md` (`## Versioning`); версия схемы БД версионируется отдельно. Донор `/data/data/com.termux/files/home/omp-work` — только чтение. План: `docs/agent-preliminary-implementation-plan.md`; решения: `docs/agent-architecture-decisions.md`; MCP: `docs/agent-provider-mcp-contract-proposal.md`.

## Проверено

- CI зелёный на `e272f2c` (финальная линия MCP + redaction): host-набор, persistent shell, CLI против локального фикстура, `aarch64-linux-android`. Гоняются `cargo check`, полный host-набор `aragent` и `pi-agent`, persistent shell и env allowlist, реальный CLI против локального OpenAI-compatible фикстура, сборка `aarch64-linux-android`.
- Живой прогон на Termux (сборка из CI до MCP-изменений), 9Router `http://127.0.0.1:20128/v1`, модель `9router/ocg/deepseek-v4.1-flash`: `read` вернул точное содержимое; SQLite `sessions` 1 / `branches` 1 / `messages` 5 / `executions` 1 / `stream_drafts` 0; `--resume` без повторного инструмента; SIGINT оставил черновик 2628 символов, следующий ход показал его модели один раз и очистил строку.
- MCP Streamable HTTP через `rmcp` покрыт тестами против локальных фикстур: `mcp.rs` (включая category-only транспортную ошибку без URL и без тела), `mcp_boundaries.rs` (definite-ошибки, epoch-изоляция, один reconnect на несколько вызовов, close против reconnect), `mcp_http_cancel.rs` (границы по времени), `mcp_redirect_contract.rs`. Внешний MCP-сервер вживую не проверялся.
- Redaction известных значений: `auth/known_secrets.rs` (aho-corasick, leftmost-longest, raw + percent в двух регистрах + base64/URL-safe с паддингом и без), регистрация в `api_key_from`, `api_key_for_core`, `peek_api_key`, `resolve_api_key` конфига и в OAuth-резолвере security-сканов; применяется в черновиках, ошибках и в payload сообщений при записи. Тесты: `redact_free_text_masks_registered_credentials_exactly`, `config_api_key_is_registered_for_exact_redaction`, `appended_message_payload_is_redacted_before_persist`.
- Окно доставки черновика — одна константа `STREAM_DRAFT_DELIVERY_CHARS = 8000`, общая для доставки и очистки; граница проверена тестом `stream_draft_delivery_window_keeps_newest_tail_char_exact`.
- Независимые ревью: streaming/timeout (2 прохода), MCP lifecycle (проход + повторный по исправлениям), redaction (найдено 5 дефектов покрытия — исправлены в `2cd96f3`).

## Ближайшие работы

Полный порядок с критериями и проверками — в `docs/agent-preliminary-implementation-plan.md` («Актуальный план», R0–R10). Кратко:

1. **R0** — хвосты: redaction остальных writer'ов (`executions`, `deliveries`, заголовок сессии, артефакты), close-on-drop при отмене SSE-хендшейка, санитайзинг `map_initialize_error`, живой smoke внешнего MCP-сервера.
2. **R1** — S4 durable `task`: admission до эффекта, crash recovery, cached revive (producer generation, FIFO barrier).
3. **R2** — S5 `SQLITE_BUSY`: граница до statements, cancellation-aware контракт, без повтора вокруг неоднозначного commit.
4. **R3** — S3 процессы: TERM→KILL группе, lease, восстановление `cwd`.
5. **R4–R8** — S8 semantic matrix, S9 память, S10 export/inspector, S11 параллельные писатели с merge, S12 hooks/packaging.
6. **R9–R10** — миграция `~/.omp` → `~/.aragent` (копия, dual-read), `/retry`, license/NOTICE, релиз `0.2.0` с артефактами SHA+`arag.sha256`.

Открытые решения, которые не выбираются молча, перечислены в примечаниях к плану: backfill истории под redaction, момент выхода из `0.x`, `/retry` для неизвестного исхода, нужность `arag doctor`.

## Ограничения окружения

- Локальная сборка на Termux по-прежнему нежелательна: `Cargo.lock` тянет `aws-lc-sys`/`cmake` через HTTP-зависимости MCP. Проверки — в CI; локально допустим только короткий smoke готового артефакта.
- `security-reviewer` не имеет выбранной модели в этом окружении; security-разборы выполняются `reviewer` с явной формулировкой threat model.
- Один писатель в основном дереве; параллельные писатели — в отдельных worktree.
- Три файла `packages/coding-agent/src/prompts/security/scan-coordinator.md`, `packages/coding-agent/src/prompts/tools/security-publish.md`, `packages/coding-agent/src/prompts/tools/security-scan.md` изменены пользователем — не трогать и не включать в коммиты.
- Индекс `.graphtrail/retry.db` устарел после переименования `omp-cli` → `aragent`; выводы по нему проверять по исходникам.

## Рабочее дерево

Закоммичено и отправлено: streaming durability, model timeout/retry, версия `0.1.0`, MCP на `rmcp` целиком (stdio, Streamable HTTP, legacy SSE, тесты), точный redaction известных секретов, единое окно доставки черновика, документация. Незакоммичено: `.trash-2026-11-12/` (старый `transports/http.rs`), `crates/aragent/src/tui.rs` и три prompt-файла — изменения пользователя, не трогать. Перед продолжением сверять `git status` и не откатывать чужие изменения.
