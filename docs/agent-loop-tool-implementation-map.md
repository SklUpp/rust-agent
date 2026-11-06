# Карта реализации: agent loop и исполнение инструментов

Дата: 2026-11-06. Статус: инженерное предложение, не утверждённый dependency lock и не реализация. Основание: [решения D01–D79](agent-architecture-decisions.md), [архитектура](agent-architecture.md). Частичные переписывания и переносы разрешены пользователем; конкретный выбор ниже предлагается основным агентом на основе source research.

Назначение: определить, какие исходники становятся основой продукта, где их менять и чем проверить результат. Не выбирать заново отложенные UI/provider настройки и не расширять список функций.

## 1. Выбор основы

**Предложение: адаптировать существующий Rust loop форка, сверяя поведение с оригинальным OMP; original Rust engines подключать напрямую без N-API.** Не ставить Rig/Goose/Codex вторым loop поверх него. От них брать только доказанно полезные срезы. Не переносить весь TS runtime, чтобы получить уже существующий Rust цикл.

Самостоятельность продукта обеспечивается своим владением состоянием, политиками, lifecycle и интерфейсами, а не обязательным переписыванием каждого алгоритма. Начать с извлечения связного работающего среза; не создавать универсальный framework и отдельный crate на каждый тип.

| Источник | Фиксация | Роль | Лицензия верхнего уровня |
|---|---|---|---|
| Original OMP | `035e4f95c314f9aff2f5a779fa335422680f8e36` | Эталон поведения loop, донор Rust engines и TS delivery | [MIT](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/LICENSE) |
| Rust-форк OMP | `3008cfd0ca88bb0e28323561a165f39efa7159e1` | Исходная Rust реализация loop/registry/scheduler/supervisor | [MIT](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/LICENSE) |
| OpenAI Codex | `e934edd68c73da93486320c77af672a015b4514a` | Отдельный срез parallel/exclusive admission и cancellation | [Apache-2.0](https://github.com/openai/codex/blob/e934edd68c73da93486320c77af672a015b4514a/LICENSE) |
| OVH shai | `d4c315b983422eaca7640a677f518a9a79b9dbfa` | Дополнительный донор контроллера команд и stream-open error handling | [Apache-2.0](https://github.com/ovh/shai/blob/d4c315b983422eaca7640a677f518a9a79b9dbfa/LICENSE) |
| Goose | `d66323246cb21f159994415c1f589368b85b0bb1` | Multiplex tool progress/result и разделение run cancellation | [Apache-2.0](https://github.com/aaif-goose/goose/blob/d66323246cb21f159994415c1f589368b85b0bb1/LICENSE) |

Это лицензии репозиториев, не аудит всех файлов/транзитивных зависимостей. При заимствовании сохранять применимые LICENSE/NOTICE, attribution и сведения об изменениях; перенос на Rust не отменяет обязательств. Новые доноры не обязательные зависимости: если срез форка уже удовлетворяет контракту, дополнительный код не добавлять.

## 2. Конкретные швы заимствования

| Узел | Проверенный исходник / символ | Что делать в продукте |
|---|---|---|
| Цикл | Форк [loop.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/loop.rs): `AgentLoop`, `assistant_status`, `repair_tool_batch_boundary` | Адаптировать цикл provider → готовые calls → результаты → следующий запрос. Сохранить различие stop/length/error/abort и pairing |
| Сверка оригинала | Original [agent-loop.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/agent-loop.ts), [replay-policy.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/replay-policy.ts) | Переносить недостающую семантику адресно, не объявлять полный parity и не копировать retry-константы как defaults |
| Provider seam | Форк [provider.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/provider.rs): `ModelProvider`, `StreamAssembler`, `OpenAiCompatibleProvider` | Первый адаптер по D30; сохранить tool IDs, usage и provider payload. Конкретный endpoint/dialect ещё открыт |
| Tools | Форк [tools/mod.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/tools/mod.rs): `Tool`; [registry.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/tools/registry.rs): `ToolRegistry` | Взять основу регистрации/schema lookup, убрать невыбранные зависимости из обязательного пути. D42: опубликованный набор совпадает с разрешённым, проверка по имени не обходит его |
| Batch | Форк [scheduler.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/tools/scheduler.rs): `execute_batch`, `run_concurrent` | Сохранить связь call/result и исходный порядок представления. Заменить недостаточный один `is_exclusive` на координацию реальных ресурсов |
| Durable boundary | Форк `TurnAck`, `TurnObserver` в `loop.rs` | Полезный шов ожидания владельца сессии, но конец turn недостаточен для D33: добавить фиксацию намерения до каждого эффекта |
| Workers | Форк [supervisor.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/supervisor.rs): `Supervisor`, `ChildRecord`, `BackgroundResultSink`; [task.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/tools/task.rs): `ChildContext` | Адаптировать IDs, parent linkage, роли и ceilings; живые записи отделить от durable состояния |
| Завершение worker | Original [executor.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/task/executor.ts): `finalizeSubprocessOutput`; [task-summary.md](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/prompts/tools/task-summary.md) | Перенести единый поток D79 в Rust: ответ по шаблону, сохранение, ID/context URI и доставка родителю. TS renderer не нужен |
| Mailbox | Форк [mailbox.rs](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/src/mailbox.rs): `IrcMessage`, `take_or_register`; original [bus.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/irc/bus.ts) | Типы/ожидание можно адаптировать; in-memory очередь заменить SQLite pending delivery. Не переносить автоматический revive отменённого |
| Shell | Original [session.rs](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-shell/src/session.rs): `Session`, `RunOptions`, `RunOutcome` | Извлечь/адаптировать Rust engine и Host seam; одна сохраняемая shell на агента по D76. Не брать N-API слой |

`pi-agent` форка сейчас связан с tools, shell, AST, isolation, provider clients и другими подсистемами через [Cargo.toml](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/Cargo.toml). Простое подключение всего крейта не равно минимальному ядру. Удаление LSP из обязательного пути соответствует продукту; глубину остальных разрезов определять dependency graph, не эстетикой.

## 3. Дополнительные доноры: что действительно пригодится

### Codex: допуск и прерывание tools

[parallel.rs](https://github.com/openai/codex/blob/e934edd68c73da93486320c77af672a015b4514a/codex-rs/core/src/tools/parallel.rs), `ToolCallRuntime::handle_tool_call`: общий `RwLock` допускает параллельные tools через shared access, exclusive — через write access. Полезен как компактный образец admission; не готовая библиотека, связан с `Session`, router и turn context.

**Применение:** если форковый scheduler недостаточен, адаптировать wrapper для cancellation/admission. Одного lock на все tools недостаточно для нескольких workspace и persistent shell; ресурсный координатор продукта остаётся владельцем.

[stream_events_utils.rs](https://github.com/openai/codex/blob/e934edd68c73da93486320c77af672a015b4514a/codex-rs/core/src/stream_events_utils.rs), `record_completed_response_item`: полезная точка сравнения фиксации завершённых items при последующей отмене turn. Не переносить Responses transport в первый адаптер до выбора API dialect.

### shai: управление без привязки к UI

[protocol.rs](https://github.com/ovh/shai/blob/d4c315b983422eaca7640a677f518a9a79b9dbfa/shai-core/src/agent/protocol.rs), `AgentRequest::{Terminate,StopCurrentTask,SendUserInput,WaitTurn}`; [chat.rs](https://github.com/ovh/shai/blob/d4c315b983422eaca7640a677f518a9a79b9dbfa/shai-llm/src/chat.rs), `chat_completion_stream` проверяет открытие SSE и HTTP error до выдачи стрима.

**Применение:** сравнить с форком при формировании общего command/event контракта и ранних ошибок подключения. Не тащить `openai_dive`, claims/cache и второй loop только ради этих приёмов. Политика retry и форматы ошибок отложены пользователем.

### Goose: progress/result stream

[tool_execution.rs](https://github.com/aaif-goose/goose/blob/d66323246cb21f159994415c1f589368b85b0bb1/crates/goose/src/agents/tool_execution.rs), `tool_stream` совмещает progress, action-required и result. `emit_best_effort` не блокирует tool уведомлением. [active_run.rs](https://github.com/aaif-goose/goose/blob/d66323246cb21f159994415c1f589368b85b0bb1/crates/goose/src/agents/active_run.rs) даёт отдельный реестр live runs.

**Применение:** срез multiplexing можно адаптировать, если он сокращает наш execution adapter. Best-effort допустим для промежуточного progress, не для durable результата/вопроса. Goose `AbortOnDrop` для foreground child не переносить как жизненный цикл наших асинхронных workers: противоречит D68.

### Библиотеки вместо нового собственного кода

| Потребность | Кандидат | Что остаётся нашим |
|---|---|---|
| Async, cancellation, channels | Уже используемые Tokio / tokio-util | Раздельные scopes отмены, ограничения и ownership |
| PTY | [portable-pty 0.9.0](https://docs.rs/portable-pty/0.9.0/portable_pty/), MIT | Process registry, env/cwd, TERM→KILL, вывод/артефакты, Termux проверка |
| SQLite | [rusqlite](https://github.com/rusqlite/rusqlite), MIT; версия перед реализацией | Схема, один владелец записи, crash protocol и артефакты; не новый SQL abstraction framework |
| MCP | [официальный rmcp](https://github.com/modelcontextprotocol/rust-sdk) | Полная выбранная protocol/capability матрица D54, trust до spawn, policy на действия; не самостоятельный loop |

Точные patch-версии и feature sets зависимостей закрепить после проверки manifest/lock и лицензий. Наличие библиотек не доказывает их комбинацию на Bionic. Дополнительные shell engines не нужны, пока `pi-shell` покрывает контракт.

## 4. Предлагаемая внутренняя сборка

Один владелец изменяемого состояния агента обрабатывает команды через очередь; model/tool futures возвращают ему события. Это async task, не обязательный OS thread и не новый отдельный daemon. Родитель и worker используют один механизм. UI/API только отправляет команды/читает события.

Минимальные данные исполнения: stable `session_id`, `agent_id`, `task_id`, `turn_id`, `call_id`, версия контекста и исход операции. Имена полей предварительные; ID нужны для pairing, pending delivery и восстановления, не для лишних публичных сущностей.

1. Собрать контекст из выбранной ветки и применимых инструкций/skills; не путать durable историю с model projection.
2. Получить streaming ответ. Частичные args копить в assembler, не передавать на execution. Завершённое сообщение и calls сохранить сразу в SQLite; terminal reason и целостность определяют пригодность calls для исполнения, не право скрыть уже полученное сообщение. Сбой записи закрывает допуск новых calls по D34.
3. Для call проверить текущие rights/budgets/trust, hooks и ресурсы. Изменённые hook аргументы заново валидируются и проверяются до эффекта.
4. Зафиксировать call_id, эффективные аргументы после hooks и намерение исполнения в SQLite; подтверждение записи предшествует запуску инструмента. Сбой storage закрывает допуск новых model/tool calls по D34.
5. Выполнить call через встроенный engine, subprocess/PTY либо MCP. Сопоставление ответа с `call_id` назначает runtime, не tool.
6. Сохранить outcome и доступные artifacts. Доставить результат loop; следующий model request не обгоняет обязательную запись. Полный вывод и model excerpt разделены по D43.
7. Финальный ответ сохранить сразу. Для worker выполнить поток D79, для родителя — завершить ответ без отмены workers. Настройка wake управляет следующим turn, не сохранностью результата.

Это рабочий дизайн, не требование архитектуры event-sourcing для каждой внутренней операции. Typed structs, каналы и SQL transactions достаточны; дополнительный broker/framework не нужен.

## 5. Параллелизм и отмена

- Лимит активных агентов и provider request concurrency — разные счётчики по уточнению D67. Не выводить активность агента из наличия HTTP stream.
- Все существующие/сохранённые агенты не занимают active slots. Точное правило ожидания потомка/инструмента ещё открыто; здесь не утверждается прежнее release/requeue предложение.
- Независимые reads/search допускаются параллельно. Calls с зависимостями не получают разрешение только потому, что пришли одним batch.
- Общий workspace имеет одного писателя между runtime-сессиями по D23; formatter и merge пользуются тем же координатором, а не обходят его.
- Сохраняемую shell одного агента предлагается исполнять последовательно. Это конкретное решение для D76 на согласование: параллельные команды — отдельные явно запрошенные jobs/sessions, а не случайный one-shot fallback, теряющий состояние.
- Реализация отмены предлагает отдельные scopes: ответ/turn, task branch, runtime. Stop ответа родителя не отменяет workers. До запуска отмена предотвращает запуск: queued-задача снимается с очереди, намерение получает сохранённый исход «не запускался», если отсутствие запуска достоверно известно runtime. После запуска реальный исход может быть неподтверждённым; после crash одного намерения недостаточно, чтобы утверждать отсутствие запуска.
- `CancellationToken` сигнализирует, а не гарантирует остановку arbitrary in-process кода. Started blocking closure не останавливается одним drop/abort. Неадаптируемые команды идут в дочерний процесс по D38.
- Начальное управление процессами — D59, без нового detach-daemon механизма. Crash может оставить внешний процесс живым; прочитанная из БД строка не даёт права считать PID тем же процессом.

## 6. Что придётся переписать для продукта

| Часть | Причина | Граница |
|---|---|---|
| Persistence orchestration | JSONL/turn-end donor не выполняет D32–D34 | SQLite intent/result/progress, artifact publication, storage-failure admission gate |
| Durable mailbox/completion delivery | In-memory mailbox не переживает restart | Очередь в БД; одна запись финала и pending notification в транзакции; replay не запускает tools |
| Host/resource coordinator | `is_exclusive` недостаточно, process-global state опасен | Workspace ownership, per-agent shell, actual subprocess cancellation, same-binary child route |
| Scope-specific cancellation | Donor guards могут отменять детей вместе с parent turn | Стоп генерации отдельно от отмены task/runtime; D68 |
| Provider/context seam | Готовый fork содержит своё provider поведение | Один адаптер D30; не скрывать provider errors и не фильтровать image по capability до запроса |
| Public command/event adapter | Fork RPC/ACP и оригинальные UI не наш продуктовый API | Одна реализация runtime для CLI/TUI/headless без сохранения ненужной совместимости |

Оригинальный `HookToolWrapper` полезен как референс preexec/revalidation, но полная hook система отложена до детализации D57. Собственный интерфейс проверки нужен, точный формат расширений сейчас не выбирать.

## 7. Persistence и доставка без ложных гарантий

Предлагается один SQLite write owner; bounded канал команд записи, async подтверждение только после commit. Синхронный SQLite доступ вынести из executor poll-path на выделенное выполнение; не держать SQL transaction во время model stream или tool.

Большие artifacts публикуются отдельно: записать временный файл → завершить запись → опубликовать устойчивый объект → зафиксировать ссылку в БД. Crash до DB commit оставляет потенциальный orphan для последующей очистки; не публиковать DB ссылку на ещё неготовый файл. Требуемые fsync/durability settings, протокол partial outputs и GC уточняются, а не считаются готовыми от `rename`.

Final worker response, context reference и pending parent notification записываются совместно в SQLite. Доставку помечать по стабильному ID; восстановление может повторно предложить notification, потребитель дедуплицирует. Не обещать exactly-once обработку моделью или внешний effect. Различать runtime-enqueued и реально включённое в следующий model context.

Намерение без сохранённого исхода не доказывает запуск или отсутствие эффекта. Продолжение определяется кодом по контракту инструмента D47, не новым LLM-следователем. История для model pairing может содержать синтетический interruption result, но durable outcome при этом не превращается в success.

## 8. Последовательность реализации и доказательства

Это рекомендуемый порядок, не разрешение начать кодирование. Срезы не заменяют полный согласованный продукт.

| Шаг | Реальный результат | Проверка перед следующим шагом |
|---|---|---|
| 1. Extract Rust loop и registry | Один настоящий OpenAI-compatible запрос, tool call, результат, финальный ответ без OMP/Bun/Node | Короткий CLI smoke Linux и Termux; fragmentation/tool IDs/length boundaries через небольшой replay corpus |
| 2. Durable execution | Intent до эффекта, outcome после; сохранение stream и storage-failure gate | Прервать процесс между каждой парой границ; уже начатое не повторяется вслепую, проверка/повтор — только по детерминированному контракту D47 |
| 3. Engines и host | Persistent shell каждого агента, builtin calls, subprocess/PTY | Состояние shell сохраняется у владельца, не течёт другому; общий writer и TERM→KILL реально проверены |
| 4. Async workers | Final response + VFS context + durable parent delivery | Parent final/stop не отменяет worker; wake off сохраняет результат; restart не теряет pending message |
| 5. Batch/resource integration | Параллельные независимые операции и сериализованные конфликтующие | Call/result pairing при mixed failure/cancel; два writers не обходят общий координатор |

Обязательные сценарии: truncated tool args не исполняются; partial HTTP stream не превращается в окончательный ответ; display toolset нельзя обойти по имени; policy reload проверяется перед эффектом; storage failure не допускает новые calls; format/merge stale result не затирает target; user stop branch отличается от stop ответа.

Тяжёлые сборки/полные suites — CI, не телефон. На Termux — короткий smoke готового артефакта: shell env/cwd, сигнал отмены, PTY resize/вывод, artifact persistence, restart. Это план проверок, ни один smoke продукта в данном исследовании не выполнялся.

## 9. Осталось выбрать до соответствующего кода

1. API dialect первого OpenAI-compatible endpoint — при реализации provider, как отложено пользователем.
2. Формальные состояния active/waiting и область лимита агентов; не подменять provider limiter.
3. Сериализация persistent shell одного агента: предложение раздела 5 пока не принято.
4. SQLite Rust binding/features, schema и durability/artifact crash protocol.
5. Минимальный Host API и selected engine feature graph с лицензиями/native dependencies.

Точные UI, список команд, языки/глубина анализа, hook runtime, автоматические обновления и прочие отложенные вопросы не решаются молча в этой карте.

## 10. Проверка исследования и следующий шаг

Проведено: чтение исходников donor pins и локальных research, проверка верхнеуровневых лицензий scouts; parent synthesis. Не проведено: сборка выбранной композиции, transitive license audit, benchmark, portability smoke. Source presence не доказывает runtime готовность.

verify: `test -s docs/agent-loop-tool-implementation-map.md` из checkout. Дополнительно проверить UTF-8, локальные ссылки, pinned URL и отсутствие merge markers; эти проверки доказывают целостность документа, не работоспособность агента.

Следующий шаг после согласования подхода: спроектировать точный extraction diff loop/registry/state и host boundary по выбранным исходникам, затем перейти к реализации только по отдельному разрешению. Не искать ещё один полный framework без обнаруженного пробела в этом срезе.
