# Точный разрез Rust-ядра OMP

Дата: 2026-11-06. Статус: S1 и часть S2 (tool/delivery/env) применены в standalone-репозитории (`tool.rs`, `delivery.rs`, `tools/naming.rs`, env allowlist); SQLite/session owner не начаты, compilation proof не выполнялся. Остальные пункты — предложение. Продолжает [карту доноров](agent-loop-tool-implementation-map.md), подчиняется [D01–D79](agent-architecture-decisions.md). Пути и номера строк ниже относятся к исходному Rust-форку `3008cfd0ca88bb0e28323561a165f39efa7159e1`, а не к будущей раскладке продукта.

## 1. Вывод

Loop уже отделён от CLI на уровне зависимости: `omp-cli → pi-agent`, обратной зависимости нет. Основные проблемы — компиляционная связанность `tools/mod.rs`, импорт целого supervisor ради delivery trait и дублированное владение сессией в локальном/RPC host.

Предлагается оставить один crate исполнения с внутренними модулями loop/types/dispatch и один существующий host-слой сборки продукта. Не выделять crate для каждой сущности. Конкретные имена будущих crates не выбираются здесь. Ресурсные engines остаются отдельными библиотеками там, где уже существует рабочая граница.

## 2. Минимальные разрезы исходников

| Исходник / якорь | Конкретное изменение | Что не меняется |
|---|---|---|
| `tools/mod.rs:101–215` | Вынести `Tool`, `ToolError` и используемые result helpers из файла с реализациями встроенных tools в самостоятельный модуль контракта | Сигнатуру `execute(args, ctx)` и `execute_with_call_id` сохранить на первом шаге |
| `tools/mod.rs:1–33` и inline реализации | Core-дерево `tools` переобъявить только для contract/registry/scheduler и необходимых лёгких ask/conflict/pending; конкретные tools, их дочерние модули и `builtin_tools` перенести в product toolset | Без этого удаления связи родительского модуля dependency closure не достигается |
| `tools/registry.rs:5,47–52` | Убрать импорт/конструктор `builtin_tools` из registry; сборку набора оставить product host | `BTreeMap`, стабильный порядок schemas, internal/wire lookup и ambiguity guard |
| `tools/registry.rs:112–122` | Чистую функцию подсказки MCP-имени перенести рядом с name lookup; не подключать MCP client ради подсказки | Диагностику неизвестного имени; не выкидывать её ради лёгкого графа |
| `supervisor.rs:123–138,167–174` | Вынести `BackgroundResultSink` и `BackgroundDelivery` в delivery-contract module | `loop.rs` принимает существующий trait, не весь supervisor |
| `state.rs:1315–1455` | Оставить `AgentContext` рядом с loop; перенести интерфейс `AskChannel` из implementation-модуля при необходимости | Не создавать отдельный types crate: `progress: Sender<AgentEvent>` сейчас образует допустимую внутрикрейтовую связь |
| `state.rs:1019–1063,1140–1200` | Конкретные `StagedPreviewPolicy`/`CompositeToolPolicy` держать рядом с product assembly; loop требует только `ToolPolicy` | План/preview как возможности не удалять из продукта |
| `provider.rs:191–253,632–1016` | Сохранить `ModelProvider`, chunks/assembler и общие ошибки; HTTP реализацию `OpenAiCompatibleProvider` отделить внутри crate | Первый продуктовый адаптер D30 остаётся включённым; не строить plugin framework для одного адаптера |
| `lib.rs:1–52` | Публично экспортировать только используемый контракт и явные продуктовые модули; убрать обязательное объявление невыбранных реализаций из loop-среза | Нет compatibility aliases/shims ради старой раскладки; перенести всех фактических consumers |

После разреза loop не обязан компилировать `pi-shell`, `pi-ast`, `pi-iso`, LSP/DAP/eval и MCP transports. Но конечный бинарник по-прежнему подключает выбранные engines и MCP: минимальный loop не означает урезанный продукт.

Не переносить `tools/mod.rs` целиком с новым именем: он одновременно определяет контракт и конкретные shell/read/write/grep/AST tools, а также безусловно объявляет task/hub/SSH/web/SQLite модули. Именно эта связь заставляет тащить реализации через registry.

### 2.1 Применённые разрезы (S1/S2)

- `Tool`, `ToolError`, `tool_result` живут в `crates/pi-agent/src/tool.rs`; `tools/mod.rs` только импортирует контракт, все callers (loop, scheduler, builtin/dap/lsp/eval tools, `omp-cli`) переведены на новый путь без aliases.
- `BackgroundResultSink`/`BackgroundDelivery` — в `crates/pi-agent/src/delivery.rs`; `loop.rs` импортирует delivery, не `supervisor`.
- `mcp/naming.rs` → `tools/naming.rs`: чистая std-логика имён больше не тянет MCP implementation; registry использует её рядом с lookup.
- `ToolRegistry::with_builtins` — только `#[cfg(test)]`; production-сборка набора остаётся у product host (`omp-cli/src/runtime.rs`), integration-тесты строят registry явно.
- `state::platform_env` — явный allowlist (`PATH`, `HOME`, `PREFIX`, `TMP*`, `LANG`, `LC_*`, `TERM`, `SHELL`, `TZ`); `AgentContext`-конструкторы и overlay-ветка process broker больше не копируют ambient env. `ctx.env` остаётся явным caller override.
- HTTP-адаптер `OpenAiCompatibleProvider` пока не выделен: provider связан с `reqwest` и общими stream-типами; follow-up — вынести `provider.rs:257–630` в внутренний `provider_http`-модуль и пересчитать фактические зависимости после compilation proof.

## 3. Точный состав сохраняемого loop-среза

- `loop.rs`: `AgentLoop`, `AgentEvent`, `SteeringEvent`, `TurnAck*`, `TurnObserver`, terminal/pairing helpers.
- `state.rs`: messages/parts/calls/results/usage, `AgentConfig`, `AgentState`, `AgentContext`, необходимые policy interfaces и их связанные типы.
- `provider.rs`: общий контракт и assembler плюс выбранный первый HTTP adapter отдельным модулем.
- `compaction.rs`, `tokenizer.rs`: сохранить действующие типы/алгоритмы как исходную реализацию; не выдавать их за всю целевую compaction D18.
- `mailbox.rs`: сохранить типы и live-waiting механику, заменить pending ownership по разделу 7.
- registry/scheduler, лёгкие ask/conflict/pending contracts и выделенные delivery types.

В `AgentContext` сегодня остаются ссылки на ask/conflict/plan и `AgentEvent`. Их можно оставить внутри одного crate, не вводя новые traits только ради ацикличной картинки. Конкретные CLI/ACP bridge types размещать с соответствующим adapter, если они не используются ядром.

## 4. Host: не переписывать все инструменты под универсальный интерфейс

Готовые точки внедрения уже есть:

| Шов | Исходник | Использование |
|---|---|---|
| `ProcessBackend` | `tools/process.rs:403–459` | Переиспользовать для jobs/PTY/process ownership; не создавать второй общий ProcessHost |
| `ReadSnapshotSink` / snapshot store | `tools/read.rs:98`, `tools/mod.rs:3631–3643`; подключение `3650–3653` | Сохранить связь read/edit; расширить strict version guard по D44 отдельно |
| `InternalUrlRouter` / `ProtocolHandler` | `tools/internal_urls.rs` | Адаптировать для локальных context/artifact ресурсов D79; не утверждать все исходные URI schemes |
| `ToolApprover`, `AskChannel` | `state.rs:1302–1311`, `tools/ask.rs` | Разделить допуск исполнения и вопрос пользователю, сохраняя существующие точки вызова |
| `ChildModelFactory` | `tools/task_catalog.rs` | Подключить выбор модели worker в пределах разрешённого набора |

Прямые filesystem операции в read/write/edit и foreground `pi_shell::execute_shell` сейчас НЕ закрыты общим host trait. Предлагается не оборачивать каждый stdlib вызов: общий execution admission охватывает регистрацию операции/политику/ресурсы, а реальные файловые писатели получают один координатор/version-checked writer. Конкретный writer требует отдельного проектирования D44, не считается готовым от добавления scheduler lock.

У `AgentContext::{new,with_cancellation,with_approver}` сейчас `std::env::vars().collect()`; BashTool передаёт этот env shell. Поэтому ключ провайдера из окружения сейчас может наследоваться shell. Заменить сбор всего env явным разрешённым набором. Цель после исправления: provider key остаётся у trusted adapter, не наследуется shell автоматически. Это обязательная правка D27, не optional cleanup.

Для D76 существующий foreground BashTool не подходит без изменения: `tools/mod.rs:654–685` вызывает shell на каждый вызов. Product owner должен владеть shell handle каждого агента и передавать его BashTool; в проверенном Rust-форке переиспользовать `pi_shell::Shell::new` и `Shell::run`/`abort`, не несуществующий там экспорт `Session`. Не использовать общий process-global cwd/env. Правила конкурентных вызовов этой shell остаются открытыми.

## 5. Scheduler: проверки исполнения и запись состояния

Исходный `scheduler.rs:85–158` уже делает lookup, hidden deny, approval, cancel check и `execute_with_call_id`; после исполнения назначает правильный `tool_call_id`. Это сохраняется, но расширяется в одном месте:

1. Разрешить имя только из актуального набора D42 и проверить args. Если будущий контракт D57 разрешит preexec изменение аргументов, повторять validation/policy для изменённых args; это условное расширение, не уже утверждённый hook pipeline.
2. Дождаться approval/ресурса при необходимости. После ожидания перепроверить текущие запреты, отмену и ресурсную версию перед эффектом.
3. Передать владельцу сохранения эффективные call args и execution intent; дождаться успешного commit. Не начинать эффект, если storage недоступен.
4. После ожидания commit снова проверить текущие policy/cancel и ресурсную версию непосредственно перед запуском. При изменении не начинать эффект; если достоверно не запускался, сохранить этот исход. Финальная проверка не заменяет атомарный version-checked writer для гонок внешних файловых изменений. Затем исполнить инструмент, сохранить outcome с подтверждением commit до следующего model request.

Сохранение — обычная операция записи с ожиданием результата (`await`), без запроса разрешения пользователю/модели и без отдельного протокола разрешения записи. Session owner вызывает storage до/после эффекта; SQL не встраивается в каждый Tool. Если storage реализован через отдельный writer, канал — внутренняя деталь, не обязательная архитектура.

Завершённое сообщение модели сохраняется сразу по D33, до допуска инструментов. `TurnAck` — существующая синхронизация донора на границе хода, не обязательный механизм продукта и не основание вводить новые ack-сущности. Обычного ожидания успешной записи достаточно; при ошибке применяется D34.

`is_exclusive()` оставить только как исходную подсказку безопасного планирования, не доказательство независимости. Workspace ownership, shell identity и dependency constraints обслуживаются продуктовым координатором. Точный набор resource claims открыт; active agent slots и provider request limiter не объединять.

## 6. Session owner: главный разрез host

Исходный `PiAgentRunner::run` собирает registry/provider/loop заново на turn; session-scoped tools хранятся в `RunnerSession` (`omp-cli/src/runtime.rs:227–267`). История и её сохранение принадлежат локальному/RPC host, loop получает snapshot.

Не переносить целиком синхронную оболочку `PiAgentRunner`: `PiAgentRunner::new` создаёт собственный Tokio runtime (`runtime.rs:522–530`), а `agent.rs:484–498` содержит process-wide `OnceLock`/`install`. Для нашего продукта frontend не должен создавать ещё один nested runtime при запуске каждой сессии.

Предлагается:

- оставить loop исполняющим механизмом с переданными provider/registry;
- вынести из локальной/RPC оболочек одного session owner: история/ветка, текущий turn, context projection, IDs, cancel scopes и session-scoped tool handles;
- сборку registry/provider из `runtime.rs` перенести в product composition рядом с owner, без терминального renderer;
- CLI/TUI/headless направляют команды этому owner и получают события одного контракта D37;
- SQLite writer принимает durable операции owner; один общий механизм, а не отдельное SQLite сохранение в каждом frontend.

Дублирование сейчас: `main.rs:2534–3200` и `rpc/session.rs:2740–3277` независимо пишут root history и отвечают на turn ack. Ошибки записи в прослеженных путях подавляются, ack всё равно разрешает продолжить. При переносе заменить эти ветки единым fail-closed механизмом D34, не сохранить подавление ошибок как совместимость.

Это выделение одного существующего владельца, не введение отдельного LLM-manager. Точная структура live session task и commands ещё проектируется; гарантии БД описаны в карте реализации, а не подтверждены исходным JSONL.

## 7. Supervisor и delivery после разреза

`Supervisor`, `TaskTool`, model factory и worker registry assembly остаются product-side, используют тот же loop, что родитель. Убрать из обязательной сборки LSP/DAP и невыбранные providers; остальные нужные tools подключать явно. Исходный `child_registry` не должен снова навязать весь builtin set в обход D42.

Final worker response, agent ID, VFS context reference и pending parent notification сохраняются по D79 одним потоком. `BackgroundResultSink` остаётся потребительским швом, но источником становится durable pending queue плюс live signal, а не только in-memory `take`.

In-memory drain не равен durable обработке: сообщение не помечается обработанным только потому, что `take_*` очистил вектор. Стабильный delivery ID связывает сохранение, включение в контекст и дедупликацию при resume; «модель увидела» и «модель выполнила» не смешиваются. Exact ack semantics ещё открыты D49.

Исходные JSONL `task_persisted_revive` и transcript readers нужны как донор реконструкции/импорта, но не становятся вторым authoritative store рядом с SQLite. Экспорт D55 — производный файл, не самостоятельный live журнал, расходящийся с БД.

## 8. Зависимости и удаления из обязательного пути

| Текущая зависимость | Решение для разреза |
|---|---|
| Tokio/tokio-util, serde/json, futures, async-trait, thiserror | Сохранить по реальному использованию; урезание features — после compilation proof, не угадывать |
| reqwest | Нужен выбранному provider adapter; не входит в абстрактные `ModelProvider` signatures |
| pi-shell/pi-walker/pi-ast/pi-iso/portable-pty | Product tools/host, не обязательная зависимость loop-only модуля; конечный продукт может использовать их все |
| rusqlite `0.33.0`, bundled/hooks | В текущем pi-agent прежде всего read-SQLite tool, не session store. Binding/features для новой БД выбираются отдельно |
| Anthropic/Ollama adapters | Не включать в первый product build D30, сохранить donor provenance при выборочном переносе |
| LSP | Не переносить в продукт по принятому отказу от LSP |
| ACP/RPC старых CLI | Не тащить ради loop; новый общий interface D37 без compatibility shims, старые consumers мигрировать либо не переносить |

Разрез не требует новых packages. После модульного разделения пересчитать actual dependencies и только тогда удалить ненужные записи нового manifest. Не менять исходный пользовательский checkout в рамках этого исследования.

## 9. Порядок будущего extraction diff

1. Перенести tool contract/delivery types, отвязать registry от builtin constructor и MCP implementation; обновить всех фактических consumers без aliases.
2. Отделить общие provider types от выбранного adapter; собрать loop с явно зарегистрированным реальным tool. Не поставлять пустой registry как готовую функциональность.
3. Выделить общий session owner из frontend assembly; передать env явно, сохранить scopes cancellation.
4. Встроить awaited message/call persistence и fail-closed storage path; затем заменить authoritative JSONL на SQLite.
5. Подключить per-agent shell и выбранные engines через существующие process/snapshot/router швы; интегрировать durable worker completion.

Это порядок безопасных изменений, не этапы выпуска и не разрешение начать код. Полный продукт не объявляется готовым после шагов 1–2.

## 10. Проверка и границы доказательств

Source audit scouts подтвердил imports, signatures, конструкторы и ownership paths. Не выполнены extraction diff, compilation, smoke, dependency/license closure audit. ACP persistence path не прослежен целиком; перенос его конкретной совместимости не является целью продукта.

Будущие обязательные проверки: core compilation без concrete engine imports; настоящий model→tool→answer; message commit до approvals; intent commit до эффекта; ошибка SQLite блокирует следующий вызов; stop ответа не отменяет worker; свой shell env/cwd не течёт другому агенту; direct name не обходит toolset; local и headless имеют один persistence path.

verify: `test -s docs/agent-core-extraction-design.md`. Проверка UTF-8/ссылок/merge markers относится только к артефакту документа. Runtime готовность этим не доказана.

## Источники

Все source anchors разрешаются относительно [дерева Rust-форка](https://github.com/SklUpp/omp-rust-termux/tree/3008cfd0ca88bb0e28323561a165f39efa7159e1). Ключевые файлы:

- [loop.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/loop.rs), [state.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/state.rs), [provider.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/provider.rs).
- [tools/mod.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/tools/mod.rs), [registry.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/tools/registry.rs), [scheduler.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/tools/scheduler.rs).
- [runtime.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/omp-cli/src/runtime.rs), [agent.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/omp-cli/src/agent.rs), [main.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/omp-cli/src/main.rs), [RPC session](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/omp-cli/src/rpc/session.rs).

Дополнительные доноры и лицензии зафиксированы в [первой карте](agent-loop-tool-implementation-map.md); новый поиск здесь не требовался: уточнялся конкретный source cut выбранной основы.
