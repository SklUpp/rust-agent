# Исследование реализации: блоки на проверку

Дата: 2026-11-06. Статус: source research и предложения реализации; не изменение принятых D01–D81, не готовый продукт. Восемь областей исследовались раздельными scout-контекстами; дизайн и проверка границ выполнены основным агентом. Пользователь не должен подтверждать каждый найденный символ: на проверку предлагаются связные блоки.

Общая основа: [архитектура](agent-architecture.md), [решения](agent-architecture-decisions.md), [доноры loop](agent-loop-tool-implementation-map.md), [разрез ядра](agent-core-extraction-design.md). Original OMP `035e4f95c314f9aff2f5a779fa335422680f8e36` и Rust-форк `3008cfd0ca88bb0e28323561a165f39efa7159e1` — разные реализации, не взаимозаменяемые доказательства. WT означает чтение локального рабочего дерева без независимой сверки удалённых байтов; это не доказательство нового коммита или содержимого опубликованного pin. Номера строк и утверждения о локальном HEAD в отдельных evidence требуют этой оговорки.

## 1. Состояние, история и контекст

**Назначение.** Хранить диалог, ветки, агентов и незавершённую работу; собирать модельный контекст без разрушения архива.

**Доноры.** Goose SQLite session/message store (Apache-2.0); original OMP entries/context builder/artifacts (MIT); Rust-форк output allocator, loop pairing helpers и transcript readers (MIT). [Доказательства](research-2026-11-06-state-context.md).

**Предлагаемая реализация.** Одна SQLite authority, сообщения с JSON payload и parent linkage; отдельное текущее состояние, pending deliveries и call outcomes. Завершение worker и pending parent notification — совместная транзакция. Артефакт публикуется до DB-ссылки. Контекст строится по выбранной ветке; compaction — сохранённая проекция summary+tail, не DELETE архива. VFS-контекст worker — представление истории, не обязательный дубль файла.

**Переписывание.** В Goose нет готовой нашей branch/call/delivery схемы; у OMP mailbox in-memory; упрощённый compactor форка не заменяет original compaction. Перенести buildSessionContext и связать с SQLite, а не копировать JSONL writer.

**Границы и проверка.** Durable active leaf должен быть независим от поздних off-branch результатов. Call IDs, подписанные/opaque payload и маршрут провайдера сохраняются без потерь. SQLite/file backup и GC требуют своего протокола. Обычный await записи, не разрешение пользователя и не новый TurnAck-протокол. Будущий proof: crash между intent/effect/result, late worker result при смене ветки, восстановление pending delivery, compaction с исходным архивом.

**На проверку.** Состав accepted D80/D81 сохраняется; конкретные SQL columns, migrations и fsync настройки пока proposal/implementation detail.

## 2. Исполнение, shell, процессы и workspace

**Назначение.** Выполнять tools/batches, сохранять собственную shell каждого агента, не смешивать права, данные и отмену.

**Доноры.** Rust loop/registry/scheduler; original Rust shell/edit/isolation engines и TS host wiring; fork persistent Shell, portable-pty/process backend. Все OMP root MIT; транзитивные лицензии проверяются отдельно. [Доказательства](research-2026-11-06-execution.md).

**Предлагаемая реализация.** Один shell object на агента; runtime хранит handles и допустимое окружение. Широкие in-process utilities из engines, несовместимые — child того же бинарника. Tool admission проверяет опубликованный набор, текущую policy, ресурсы и intent write до эффекта. Writer ownership общий для edit/formatter/merge; отмена ответа не отменяет workers. Provider request limit отдельно от active-agent limit.

**Переписывание.** Foreground BashTool форка использует one-shot execution, несмотря на наличие persistent library. Scheduler имеет static exclusivity, не полноценные ресурсные зависимости. Foreground PTY и command guards не покрыты одним готовым путём. Конечный parent-side merge форка не выполняет весь принятый three-way контракт; одного process-local MERGE_LOCK недостаточно для нескольких runtime.

**Границы и проверка.** Короткий hashline tag не строгая версия raw bytes; ast-edit preflight не CAS, внешняя гонка остаётся. Shell/session isolation не filesystem sandbox. Android использует fallback процессов и copy-isolation; native Linux backend нельзя объявить Android-supported по имени ARM64. Будущий proof: чужой env/cwd не течёт, writer coordination, stale write race, dirty/non-Git merge, PTY и TERM→KILL на устройстве.

**На проверку.** Поведение shell при exit, сериализация вызовов одного агента и strict writer ещё требуют проектирования, не меняют выбранный per-agent persistent подход. Detached/nohup independent режим остаётся отложенным.

## 3. Провайдерный адаптер и MCP

**Назначение.** Первый OpenAI-compatible маршрут и полный выбранный MCP-клиент без зависимости от внешнего gateway.

**Доноры.** Fork OpenAiCompatibleProvider/StreamAssembler и original provider compatibility/replay utilities; официальный rmcp SDK (Apache-2.0). Rig не нужен вторым loop ради MCP. [Доказательства](research-2026-11-06-providers-mcp.md).

**Предлагаемая реализация.** Использовать Rust stream/call основу; адресно портировать отсутствующие original semantics. Ключ резолвить явно из выбранной env-переменной: donor fallback «env name либо literal» не считать готовым нашим контрактом. MCP изолировать adapter-модулем поверх SDK с проверяемой capability matrix; не переносить ограниченный tools-only client форка как «полный MCP».

**Переписывание.** Native signed/reasoning replay, content blocks и tool IDs нельзя терять при wire conversion. MCP images/resources не сплющивать безусловно в base64-текст. Original/fork не покрывают все prompts/roots/sampling/elicitation одного SDK одинаково. SDK protocol version и HTTP dependency major требуют явной совместимости, не выбора «самое новое» по умолчанию.

**Границы и проверка.** Roots — advisory scope, не ACL (spec-факт). D54 требует проверки безопасности до исполнения, включая сам stdio spawn; конкретный механизм (approval UI или иной) не выбран. Provider ошибки допустимо передавать без обязательной нормализации, с защитой секретов. Будущий proof: fragmented args, early HTTP error, partial stream, call/result pairing, binary content, MCP interop/version negotiation и cancellation.

**На проверку.** Endpoint/dialect, auth/reasoning/retry/timeout policies и MCP protocol matrix оставлены до соответствующей реализации. Полнота SDK не доказывает полноту нашего host integration.

## 4. Встроенная работа с кодом

**Назначение.** Максимум полезных возможностей чтения, поиска, навигации и безопасных изменений, не только диагностика или дерево файлов. Без LSP.

**Доноры.** pi-ast/pi-walker/pi-grep-pattern, original pi-edit; tree-sitter/ast-grep/tags queries; Aider repo-map алгоритм; языковые semantic engines и codemod инструменты — по доказанным API. [Доказательства](research-2026-11-06-code-analysis.md).

**Предлагаемая реализация.** Общий контур чтения/поиска/индекса/правок с едиными path filters и file versions. Структурное чтение показывает symbols/blocks и позволяет дочитывание. AST matching/rewrite обслуживает codemods; индекс definitions/references и графы дополняются semantic backends там, где они доступны. Repo map выбирает полезный обзор из индекса, не грузит его целиком. Форматтер применяет новую revision через того же writer.

**Переписывание.** Готовые parse/search/summary не равны persistent cross-file semantic index. Tags дают синтаксические определения/упоминания; resolve/import/type/rename требуют языкового анализа либо честной пометки heuristic. Aider ranking — порт алгоритма, не готовый Rust crate. Прямой перенос всего rust-analyzer runtime не подразумевается; библиотечные срезы проверяются отдельно.

**Границы и проверка.** Популярные языки не имеют одинаковых tags queries и semantic coverage. Native grammars, PCRE2 и тяжёлые backends требуют Bionic проверки. GPL-компоненты требуют осознанного лицензионного решения, не являются автоматически запрещёнными или свободно встраиваемыми. Будущий proof: multi-file rename со shadowing/imports, macros/generated code, AST transform boundaries, stale index и preview/write coherence.

**На проверку.** Направление — максимальные возможности, не syntax-only первая поставка. Матрица язык×операция×точность и отдельный выбор тяжёлых backends не утверждаются молча; расширенное source evidence уточняет доступных доноров.

## 5. Память и skills

**Назначение.** Общий/проектный опыт, управляемые записи, фоновое извлечение и повторное использование процедур.

**Доноры.** Original MemoryBackend/Mnemopi/local pipeline; fork local_memory/managed_skills/skill protocol; Mem0 как донор extraction/provenance/delete алгоритмов, не обязательный Python runtime. [Доказательства](research-2026-11-06-memory-skills.md).

**Предлагаемая реализация.** Явные записи и производные сведения хранят scope/source, distinction user assertion vs inferred. Skills — Markdown metadata сначала, полный текст по требованию/preload, commands как их вызовы. Сохранение/удаление памяти и её производных — отдельные операции от session history. Использовать уже имеющийся SQL механизм, не отдельный обязательный vector server.

**Переписывание.** Rust enum Mnemopi не означает реализованный backend. Полный original pipeline требует переноса либо упрощения только с согласованием требований. Mem0 Python dependencies не включаются в минимальную поставку ради нескольких алгоритмов.

**Границы и проверка.** Embeddings/ANN не обязательная предпосылка явной памяти; FTS и точный поиск допустимы по нужному retrieval контракту. fastembed/ONNX и ANN добавляют native вес, Bionic support не доказан. Не называть soft delete полной очисткой derived data. Будущий proof: scoped retrieval, provenance, edit/delete propagation, skill collision/traversal, extraction качество на примерах.

**На проверку.** Exact extraction/consolidation и retrieval backend открыты; память не новые полномочия и не автоматически высшие инструкции.

## 6. Инструкции, роли, конфигурация и hooks

**Назначение.** Управлять поведением и настройками без наследования непонятных precedence правил всех доноров.

**Доноры.** Original discovery/settings/role loader/extensions semantics; fork Markdown/YAML profiles, config_cmd и schema metadata. [Доказательства](research-2026-11-06-profiles-hooks.md).

**Предлагаемая реализация.** Одна typed/schema основа для CLI/TUI/headless и текстового конфига. Отдельно instruction scopes D48, role launch overrides и runtime policy. Профиль не активирует project executable код. Hooks используют общие lifecycle seams, конкретный runtime пока не выбирать.

**Переписывание.** OMP basename-фильтр AGENTS.md у workers не переносить. Native discovery precedence разных подсистем не превращать в наш общий закон. Original hooks Bun/JS, в Rust-форке полноценный пользовательский hook runtime не найден. Generated settings snapshot — донор структуры, но требует сверки происхождения и drift-check.

**Границы и проверка.** Найдено расхождение docs/source порядка context files: source/test user-first, часть docs user-last. Это не основание выбирать наш precedence случайно. Проектные hooks/MCP/команды не активируются без явного доверия либо правила владельца (D20/D57); режим без подтверждений инструментов это не обходит. Безусловная загрузка project hooks донора не переносится. Будущий proof: слои config, invalid reload, scope selection, launch override без редактирования профиля, safe secret view.

**На проверку.** Runtime/язык hooks, форматы и конфликтные приоритеты остаются открытыми как ранее согласовано.

## 7. TUI, CLI и headless

**Назначение.** Одна runtime логика, разные интерфейсы, один активный управляющий клиент сессии.

**Доноры.** Fork CLI/RPC event model и worker transcript readers; original terminal/history-viewport semantics; Ratatui/Crossterm и совместимые Rust input widgets. [Доказательства](research-2026-11-06-frontends.md).

**Предлагаемая реализация.** Typed command/event контракт runtime, локальный in-process consumer и headless adapter; TUI — новый Rust renderer, не порт всего ANSI renderer original. Worker viewer читает те же snapshots/history. Configurator использует schema, а не отдельные списки defaults.

**Переписывание.** Fork repl — plain stdin, не готовый TUI. Reedline сам владеет терминалом и не является drop-in widget Ratatui. Версии textarea/widgets должны совпадать по Ratatui major/minor; не подключать все кандидаты разом. Native scrollback/ANSI original и cell-buffer TUI имеют разные модели.

**Границы и проверка.** Выбор Ratatui backend ещё предложение, не approved layout. Детали @files/keys отложены. Headless protocol не следует сохранять совместимым с fork RPC ради случайных legacy consumers. Будущий proof: long output, worker switching, resize, Unicode/IME, clean stdout NDJSON, detach/reconnect и config edits без перерисовочных потерь на Termux.

**На проверку.** Renderer/input stack выбирается после минимального реального terminal smoke; никакие UX defaults не утверждены этим source report.

## 8. Linux/Termux-поставка и диагностика

**Назначение.** Один бинарник на платформу плюс рабочие каталоги; диагностика и error reports без usage/analytics telemetry.

**Доноры.** Fork build/config dirs/native feature graph; original notices/legal payload; Rust logging/error-report primitives. [Доказательства](research-2026-11-06-packaging.md).

**Предлагаемая реализация.** Платформенная сборка с явно выбранными native features и встраиваемыми/распаковываемыми ресурсами, собственные пути D21/D27. Один logging facade, безопасные поля по умолчанию, error report с ограниченным payload. Не добавлять несколько panic/log frameworks. Выбор error-only SDK не равен telemetry разрешению и требует отдельной настройки.

**Переписывание.** Fork include_str paths связаны с TS workspace при сборке; пакет ресурсов продукта нужно сделать самодостаточным. Есть platform cfg gaps, долгие socket paths, неполная Linux release contract/notice automation. Полноценная Rust диагностика не получается переносом napi-only crash hook.

**Границы и проверка.** Nightly pinned не доказывает невозможность stable build. strip убирает symbols/debug info, но не означает невозможность capture; symbolization — вопрос release artifacts. Native dependency closure и фактические лицензии не выводятся только из общего workspace lock. Будущий proof: ELF/DT_NEEDED, Linux ABI floor, Bionic device run, permissions, no-secrets report, complete LICENSE/NOTICE.

**На проверку.** Обновления отложены; никакой автоматической установки/отправки в research нет. Логи/отчёты не повод включить usage telemetry.

## Проверка и порядок выдачи

Все блоки остаются отдельными evidence-документами; исследователи не обменивались рабочими контекстами после запрета пользователя. Parent сверял только пересечения контрактов. Независимый reviewer проверил storage/execution/providers и затем синтез; исправлены ложные формулировки coverage, roots как ACL, обязательного approval и происхождения WT. Не приняты именно прежние ложные гарантии, а не корректирующие ограничения раздела 3.

Проверки данного этапа: существование файлов, UTF-8, локальные ссылки, merge markers и наличие восьми блоков. Не выполнялись: сборка/исполнение, benchmark, полного dependency/license closure, runtime security audit. Source-проверка не доказывает работоспособность выбранной комбинации.

Для пользователя: рассматривать блоки по порядку, начиная с 1. Правка одного блока не означает автоматическое принятие остальных. Детали, ранее отложенные до реализации, не возвращаются отдельными мелкими вопросами.

verify: `test -s docs/agent-research-blocks-2026-11-06.md`.
