# Состояние проекта и задачи

Обновлено 2026-11-18. Прежнее содержимое описывало MCP-блокеры на `9f21f0c`; оно заменено фактическим состоянием.

## Проект

`aragent` — самостоятельный coding-first AI runtime на Rust, бинарник `arag`, целевые платформы Linux и Termux/Bionic. Репозиторий `/data/data/com.termux/files/home/rust-agent`, ветка `feat/standalone-runtime`, HEAD `19f72d5` (`test(security): assert the worker keeps the service tier`). Донор `/data/data/com.termux/files/home/omp-work` — только чтение. План: `docs/agent-preliminary-implementation-plan.md`; решения: `docs/agent-architecture-decisions.md`; MCP: `docs/agent-provider-mcp-contract-proposal.md`.

## Проверено

- CI `44317748386` — success: `cargo check`, полный host-набор тестов `aragent` и `pi-agent`, persistent shell и env allowlist, реальный CLI против локального OpenAI-compatible фикстура, сборка `aarch64-linux-android`.
- Живой прогон на Termux бинарником из этого CI, живой 9Router `http://127.0.0.1:20128/v1`, модель `9router/ocg/deepseek-v4.1-flash`: `read` вернул точное содержимое файла; SQLite получил `sessions` 1 / `branches` 1 / `messages` 5 / `executions` 1 / `stream_drafts` 0; `--resume` загрузил историю без повторного вызова инструмента (5→7 сообщений, `executions` осталось 1).
- Прерывание стрима SIGINT сохранило черновик 2628 символов; следующий ход показал баннер `recovered an interrupted assistant fragment (not journaled; sent to the model once as interrupted context, no tool calls)` и очистил `stream_drafts` после успешного терминала.
- `arag config get` видит `model.requestTimeout.{timeoutMs,maxRetries}` и `model.streamPartial.{memorySnapshotChunks,persistEveryChunks,flushIntervalMs}`.
- Независимые ревью: два прохода по streaming/timeout; три P2 (service tier, redaction прозы, дублирование при `toolUse`) исправлены, повторный проход — без блокеров.

## Незавершённая работа

1. **MCP Streamable HTTP через `rmcp`** — изменения в рабочем дереве, вне коммитов. Блокеры: compare-and-clear для `pending_generation`/`handled_generation`; общая синхронизация `reconnect_locked` и `close`; `close()` без deadline и с возможным незавершённым cleanup; политика pending при отмене non-tool запросов; детерминированные проверки конкурентных вызовов, terminal close и отсутствия replay. stdout/stdin-результаты HTTP не подтверждают. Старый транспорт лежит в `.trash-2026-11-12/`, не удалять.
2. **S4 durable `task`** — fail-closed до доказанных admission и crash recovery; cached durable revive — до producer generation, сохранения принятых сообщений и FIFO writer barrier.
3. **S5 SQLITE_BUSY** — нужна безопасная граница до statements и cancellation-aware контракт; повтор вокруг неоднозначного commit запрещён.
4. **S3 процессы** — доказать TERM→KILL группе; потомки, вышедшие из группы, не гарантируются; shell под тем же UID — не sandbox.
5. **S8/S9/S10/S11/S12** — semantic matrix, extraction pipeline, export/inspector, параллельные пишущие workers с merge engine, hooks runtime и packaging-контракт.
6. **Кросс-срезно** — единый D67 redactor известных секретов (`aho-corasick`, границы чанков), миграция `~/.omp` → `~/.aragent` без удаления источника, `/retry`/`Alt+R`, license/NOTICE audit.

## Ограничения окружения

- Локальная сборка на Termux недоступна: `Cargo.lock` тянет `aws-lc-sys`/`cmake` из незакоммиченных MCP HTTP-зависимостей; сборка и полные тесты — в CI.
- `security-reviewer` запускать только при реальном изменении границ доверия или обработки секретов.
- Один писатель в основном дереве; параллельные писатели — в отдельных worktree.
- Три файла `packages/coding-agent/src/prompts/security/scan-coordinator.md`, `packages/coding-agent/src/prompts/tools/security-publish.md`, `packages/coding-agent/src/prompts/tools/security-scan.md` изменены пользователем — не трогать и не включать в коммиты.
- Индекс `.graphtrail/retry.db` устарел после переименования `omp-cli` → `aragent`; выводы по нему проверять по исходникам.

## Рабочее дерево

Закоммичено и отправлено: streaming durability, model timeout/retry, redaction черновиков, service tier сканера, разделение доверия для восстановленного фрагмента. Незакоммичено: MCP HTTP (код, тесты, `Cargo.lock`, `crates/pi-agent/Cargo.toml`, `runtime.rs` — только MCP-ханк), `docs/agent-provider-mcp-contract-proposal.md`, `.trash-2026-11-12/`, этот файл. Перед продолжением сверять `git status` и не откатывать чужие изменения.
