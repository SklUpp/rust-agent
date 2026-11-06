# Preliminary implementation plan — Rust runtime (предложение planner)

Статус: **предварительный план для parent-acceptance**. Не принятие D84–D96, не разрешение кодировать, не blanket acceptance. Код/сборки/чеки не выполнялись (read-only ограничение). Provenance анкоров: `crates/pi-agent/...`, `crates/omp-cli/...` — рабочее дерево форка SklUpp/omp-rust-termux@`3008cfd0ca88bb0e28323561a165f39efa7159e1` (WT); original OMP (`crates/pi-shell/src/session.rs`, TS-файлы) — pin can1357/oh-my-pi@`035e4f95c314f9aff2f5a779fa335422680f8e36`; версии библиотек — research-кандидаты на дату, не accepted-версии. Имена новых модулей — proposal, parent владеет именованием. Существующие имена crate-ов (`pi-agent`, `omp-cli`) сохраняются: crate boundaries — по фактическому dependency cut (fixed assumption 6), не crate-per-type, не migration ради naming.

Обновление исполнения 2026-11-06: пользователь разрешил реализацию по плану с инженерным выбором конкретных механизмов и потребовал отдельный репозиторий `/data/data/com.termux/files/home/rust-agent`. Все 30 документов этого проектирования перенесены в его `docs/` с проверкой неизменности SHA-256 перед этой записью. `/data/data/com.termux/files/home/omp-work` теперь только донор, не целевой репозиторий. Исходный статус выше описывает момент подготовки плана; разрешение кодировать получено позже. Принятые требования не сокращаются; новые продуктовые развилки не считаются автоматически принятыми. Source/build/runtime результаты отмечаются по мере исполнения, а не задним числом.

Состояние исполнения 2026-11-06: отдельная основа зафиксирована `3d4adf1`; интегрированы разрез Tool/delivery и env allowlist после review, отдельный HTTP provider module, SQLite store-компонент и исправления persistent shell cancellation. S2 host cutover остаётся в изолированной доработке, SQLite store сам по себе не считается готовым сквозным срезом. Публичный репозиторий: https://github.com/SklUpp/rust-agent. CI `42227956525` остановился до компиляции из-за донорского lockfile; `6fea271` удалил 459 неиспользуемых package identities без добавления версий, полный offline locked metadata прошёл. CI `42229953427` дошёл до компиляции и выявил duplicate `tool_result` и незавершённые provider imports; helper исправлен `2c9268e`, imports завершаются в host cutover. Успешной сборки, Rust-тестов и CLI/device smoke пока нет. Тяжёлые локальные сборки не выполняются.

## 0. Цель и не-цели

Цель: конкретный порядок реализации самостоятельного coding-first Rust runtime (Linux+Termux/Bionic, один binary+assets, TUI/CLI/headless один loop) поверх выбранной donor-основы форка — с указанием, какая логика где живёт, кто единолично пишет каждое состояние, сквозной путь данных и какие gates блокируют какие решения.

Не-цели: не новая конкурентная архитектура; не пересборка research; не выбор отложенных product-решений (API dialect, hooks runtime, форматы конфига, TUI-клавиши и т.д.) — они перечислены в §6 и не выбираются молча; не обещания ready/build-verified; не сокращение agreed scope (свободная топология D06/D49, полный MCP D54, hooks D57, memory D14/D53, код D35, TUI D25/D63 — все остаются в плане по фазам).

## 1. Владельцы состояния (single writer)

| Состояние | Единственный писатель | Хранилище | Донор/API |
|---|---|---|---|
| Сообщения, ветки, агенты (ID/parent linkage/состояние), call intent/outcome, pending deliveries, artifact links, current plan, effective settings versions | SQLite store owner (один writer-task на runtime-процесс, bounded канал команд; подтверждение записи — обычный awaited store result, не отдельный ack-протокол) | SQLite (D32/D80) | rusqlite; Goose session/message паттерн (`session_manager.rs` донора `d663232`); донорский `TurnAck` (`loop.rs:206-252`) — опциональная деталь шва, не обязательный протокол |
| Файлы workspace (runtime-контролируемые записи: write/edit/formatter/merge) | Writer coordinator (межпроцессный lease, D23) | реальные файлы managed root | `tools/read.rs:98` ReadSnapshotSink + snapshot store `tools/mod.rs:3631-3643`; strong full raw-file version token (proposal, не CAS) |
| Изолированные workspace/базы merge | Isolation/Integration модуль owner | sibling-каталоги managed root (не внутри repo) | D28 snapshot-база; движок — gate §5 |
| Опубликованные артефакты (worker output, большие tool results) | Artifact publisher (файл → потом DB-ссылка, D81) | disk + SQLite link | донор `tools/agent_output.rs:42-53` `OutputIdAllocator`, `:168` `publish_agent_output` (no-clobber) — адаптация, не слепой перенос |
| Shell-interpreter state каждого агента | Per-agent shell module (owner: agent scope) | in-memory + сериализуемый supported-set | два разных донора с не взаимозаменяемыми API: original `pi-shell::session::Session` (pin `035e4f9`) и fork `pi_shell::Shell` (`shell.rs:134,779-811`, pin `3008cfd`) |
| Живые process handles / jobs | ProcessBackend registry | in-memory; в БД — наблюдённое состояние, PID не идентичность | `tools/process.rs:403-459` `ProcessBackend`; portable-pty |
| UI-события | Session owner публикует command/event поток (D37); опционально atomic state+outbox в той же SQLite-транзакции (proposal, не event sourcing) | SQLite outbox / live channel | `AgentEvent` `loop.rs:266-330`; durable UI cursor — optional read-position, не ack |
| Память (scope/source записи) | Memory store owner | SQLite (+ derived FTS — rebuildable, отдельная операция) | D53; FTS5 — gate §5 |
| Credentials | Credential store (каталог вне VFS, D27) | отдельное хранилище | `omp-cli/src/auth/store.rs:18`; secrecy — wrapper-кандидат |

Правила: нет второго live JSONL (донорские JSONL/transcript readers — только import/reconstruction, `task_persisted_revive.rs:40-66`); **row-state — parent-proposal, не мандат D80** (D80 предписывает сущности — messages/branches/agents/intent-outcome/deliveries/artifacts — а не форму хранения; event-подход допустим, если даёт те же сущности и переживает crash); commands ≠ replayable events: intent-записи можно фиксировать как события/факты — запрещён именно **replay-эффект** (повторное исполнение по записи), не сама форма записи (journal-revision §2.3 направление); «модель увидела» ≠ «модель обработала» (D49).

## 2. Donor cuts: файлы/символы и переносимые API

### 2.1 Loop cut (`pi-agent`) — готовые швы, фактический разрез

- Сохранить как исполнительное ядро: `loop.rs` (`AgentLoop`, `AgentEvent`, `SteeringEvent`, `TurnAck*`, `repair_tool_batch_boundary`), `state.rs` (messages/parts/calls/results/usage, `AgentConfig/AgentState/AgentContext`, `ToolApprover`, `ToolPolicy`), `provider.rs` ядро (`ModelProvider` `:191-253`, `StreamAssembler` `:632-1016`, `Stream*`/`ProviderError`/`ToolChoice`), `compaction.rs`+`tokenizer.rs`, `mailbox.rs` (типы/live-waiting), `tools/registry.rs`, `tools/scheduler.rs`.
- Разрезы по loop-type-cut S1–S6: `Tool`/`ToolError`/`tool_result` из `tools/mod.rs:101-215` → отдельный contract-модуль; `registry.rs:5,47-52` — убрать `with_builtins`/`builtin_tools` из registry (сборка набора — product host; prod-вызов `pi_agent::tools::builtin_tools()` уже в `omp-cli/src/runtime.rs:1038`); `registry.rs:112-122` — `mcp/naming.rs` (чистый std) к registry, без MCP client; `supervisor.rs:123-138,167-174` — `BackgroundResultSink`/`BackgroundDelivery` → delivery-модуль (loop больше не импортирует весь supervisor); HTTP-адаптер `OpenAiCompatibleProvider` (`provider.rs:257-630`) — внутренний модуль (reqwest не в сигнатурах core).
- **Обязательная правка D27** (не optional): `state.rs:1352-1390` — конструкторы `AgentContext` делают `std::env::vars().collect()`; BashTool передаёт env shell → ключ провайдера утекает. Замена: явный allowlist env. Приоритет: до первого реального хода (S1).
- Не тащить в loop-cut: `tools/mod.rs` реализации, `tools/read.rs`, `tools/task.rs`, `task_isolation.rs` (pi-iso), `lsp/*`, `dap/*`, `eval/*`, `mcp/client+transports`, `provider_anthropic/ollama`, остаток `supervisor.rs`. LSP не переносится вообще (D13).
- `AgentContext` (state.rs:1315-1455) остаётся рядом с loop в одном crate — не выдумывать traits ради ацикличности.

### 2.2 Session owner cut (`omp-cli`) — главный разрез host

- Сегодня два независимых автора persistence: `main.rs:2534-3200` (headless `handle_event`) и `rpc/session.rs:2740-3277` (`persist_turn_end`) — дублирование с подавлением ошибок записи. Перенести в **одного session owner**: история/ветка, текущий turn, context projection, IDs, cancel scopes, session-scoped tool handles (`RunnerSession` `runtime.rs:227-267`), единый fail-closed persistence path D34 (не сохранять подавление ошибок как совместимость). Сохранение — обычный awaited store result; отдельный ack-протокол не вводится, донорский `TurnAck` — опциональная деталь шва.
- Не переносить `PiAgentRunner` целиком: `runtime.rs:522-530` создаёт собственный Tokio runtime, `agent.rs:484-498` — process-wide `OnceLock`; pump policy (`runtime.rs:2149-2555`: dialog task, steering dispatcher, deadline timer, bounded 64-slot channel, abort-on-drop) переносится в owner как async task.
- CLI/TUI/headless → команды owner, события одного контракта D37 (`AgentEvent` нейтрален host; `omp_cli::agent::Event` `agent.rs:32-99` — adapter-концерн).
- Registry-сборка (`build_registry_with_display` `runtime.rs:4960-5062`, `BUILTIN_TOOLS` `runtime.rs:69-84`) — product toolset module рядом с owner.
- Реальные швы reuse: `ToolRegistry`/`ToolScheduler` (per-turn), `RunRequest.history_messages` (история инжектится снапшотом), `BackgroundResultSink` (host-installable), `ChildModelFactory` (`tools/task_catalog.rs:21`).

### 2.3 Tools/host швы (tool-host-cut §7)

Уже trait-injected и reuse: `ProcessBackend` (`tools/process.rs:403-459`), `ReadSnapshotSink`/`SnapshotStore`, `InternalUrlRouter`/`ProtocolHandler` (`tools/internal_urls.rs`) — под synthetic resource URI router (история/артефакты/планы, fixed assumption 1; не generic VFS proxy для обычных файлов), `ToolApprover`/`AskChannel` (`state.rs:1302-1311`, `tools/ask.rs`), `PlanModeGuard`, `ToolPolicy`/`CompositeToolPolicy`, `PendingStore`, `ChildModelFactory`. НЕ за трейтами: прямые fs-записи write/edit, `pi_walker` сканы, `pi_ast` анализ, foreground `pi_shell::execute_shell` (`tools/mod.rs:654-685`) — их закрывает общий execution admission + один version-checked writer, не обёртка каждого stdlib вызова.

### 2.4 Исполнение/процессы (execution-donors)

- Shell engine: **два разных донора, API не взаимозаменяемы**: original `pi-shell::session::Session` (`Session/RunOptions/RunOutcome`, TERM→KILL, pin `035e4f9`) и fork persistent `pi_shell::Shell::{new,run,abort}` (`shell.rs:134,779-811`, pin `3008cfd`); выбор engine — за реализацией (gate G-shell), не drop-in «либо/либо». N-API `pi-natives` не переносится как lib. Bashkit — кандидат interpreter-state (gate §5).
- PTY: portable-pty (кандидат, версия фиксируется при реализации) + OMP-контракт `pi-natives/src/pty.rs` (dims clamp, heartbeat, post-cancel drain); cancel: `pi-shell/src/cancel.rs` `CancelToken/AbortToken` контракт.
- SQLite: rusqlite (bundled) — adoption-кандидат, схема/protocol наш (D80/D33/D34); точная версия/фичи фиксируются при реализации.
- MCP: rmcp (Apache-2.0) — adapter поверх SDK; поведенческий контракт OMP `mcp-runtime-lifecycle.md` (timeout precedence, reconnect backoff, circuit breaker).
- Артефакты: **адаптация** `tools/agent_output.rs` под D81 (allocate ID → temp → publish → DB-link); брать только нужные функции (`OutputIdAllocator`, no-clobber publish, symlink refusal), `publish_agent_output_replace` (follow-up replace) — не переносить слепо как API: семантика замены уточняется при адаптации; донорскую dependency closure не тащить.

### 2.5 Код-анализ (crate-revision-evidence, research-code-analysis)

Доноры-кандидаты (точные версии **не accepted** — фиксируются при реализации с лицензионной проверкой): ast-grep-core (MIT, syntax pattern, 28 языков), tree-sitter (грамматики), ignore + globset (Unlicense/MIT; **не ACL**; `*` матчт `/` — нужен `literal_separator(true)`), pi-ast/pi-walker/pi-grep-pattern форка, pi-edit/hashline. Semantic backends — кандидаты (Oxc, библиотеки rust-analyzer, SCIP и др.), покрытие — по матрице язык×операция (D35/D75: rename/refactor/resolve не сужаются до tags). imara-diff — только diff computation/отображение (не fuzzy, не merge). stack-graphs/tree-sitter-stack-graphs — archived (MIT OR Apache-2.0, не MPL), только как language-rule кандидаты с явным статусом. Веб-чтение: **dom_smoothie 0.19+ (обновлённый parent-кандидат) + htmd**; readability 0.3 — старый кандидат (2023, без size-limits, устарел) к замене; внешний лимит размера HTML обязателен в обоих случаях.

## 3. Сквозной путь input → persist → model → tool intent → effect/result → worker delivery → UI events → crash reconcile

Один путь для всех frontends (D37). Стадии = границы владельцев из §1; каждая durable-граница помечена.

1. **Input**: TUI/CLI/stdin/headless-adapter → команда контракта → session owner. Steering/очередь/side question/cancel — по D19. Гонка «вопрос: пользователь vs auto-resolve» (D50) решается **одной atomic conditional transition в store** (победитель фиксируется одним commit), не гонкой таймеров в UI.
2. **Projection**: owner собирает model projection выбранной ветки + инструкции (D48) + skills metadata (D52). Persisted message representation ≠ wire DTO (provider-MCP contract). Compaction/summary — **revision/epoch проекции** (направление D90), не «prefix навсегда».
3. **Persist input**: user message — durable commit до model request (обычный awaited store result).
4. **Model**: `ModelProvider::stream` → `StreamAssembler` (сборка по provider IDs/index, не конкатенация); partial content/events → UI outbox; токен ключа — только у trusted adapter (D30).
5. **Persist message**: завершённое сообщение + calls — durable сразу (D33), до допуска инструментов; неполный stream периодически; truncated args никогда не исполняются.
6. **Admission**: scheduler (`tools/scheduler.rs:59-163`) + toolset D42 (published == allowed, hidden deny `:86-101`) + policy/trust D20/D29 + budgets D26 + ожидания ресурсов (writer lease D23, agent-slot queue D67). **После каждого ожидания — recheck policy/cancel/resource** (execution contract §Вызов п.4).
7. **Intent**: owner сохраняет call args + intent → обычный awaited store result → только потом effect. Ошибка записи intent → действие не стартует (D34 gate).
8. **Effect**: in-process engine / same-binary child / PTY / MCP. Tool-specific writer проверяет актуальность файла при записи (strong full raw-file version token, stale → reject D44; не CAS). `tool_call_id` назначает runtime (`scheduler.rs:157`).
9. **Result**: outcome + artifact publish (файл → DB-ссылка, D81) → durable до следующего model request. Ошибка сохранения после эффекта ≠ повтор действия.
10. **Worker delivery (D79)**: worker = тот же loop; финальный ответ по шаблону → `OutputIdAllocator` + publish → одна SQLite-транзакция: completion + pending parent notification (стабильный delivery ID). Parent-граница ответа — безопасная доставка (D68); wake — настройка, не условие сохранности.
11. **UI events**: owner → adapters (TUI/NDJSON/headless); отображение изменений по операциям (D60), display modes (D25). Durable cursor — optional read-position; gap → full resync.
12. **Crash reconcile (D16/D47, per-tool recovery D88)**: старт читает durable state; «intent без outcome» → **контракт конкретного инструмента**: idempotent replay по стабильному key/revision (rebuildable index) либо явный исход «прервано, результат не подтверждён»; background handle без exit → interrupted/unconfirmed, PID не identity; pending deliveries пере-предлагаются, consumer дедуплицирует по delivery ID; явно отменённое не возобновляется; permissions/credentials/workspace перепроверяются. Отображение сохранённого ≠ повторное исполнение.

## 4. Вертикальные срезы (dependency order)

Каждый срез — рабочий вертикальный проход, не scaffolding. Порядок: **S1 (compilation/provider, без внешних эффектов) → S2 (первый реальный tool vertical после intent/outcome) → S3 → S4 → S5**; далее S6–S10 частично параллельно; S11 после S3+S2; S12 — насквозь. **Правило S4/S11:** до S11-iso workers — без записи в общий workspace (read-only) либо последовательные исполнители под единственным workspace-owner; параллельные пишущие workers — acceptance S11.

### S1. Ядро: loop-разрез + provider/compilation smoke (без внешних эффектов)
- Файлы: `pi-agent` — contract-модуль из `tools/mod.rs:101-215`; `tools/registry.rs` (обрыв `:5,47-52`, `mcp/naming` к registry `:112-122`); delivery-модуль из `supervisor.rs:123-174`; `provider.rs` — HTTP adapter внутренним модулем; `lib.rs:1-52` — экспорт только контракта; **`state.rs:1352-1390` env allowlist (D27)**. `omp-cli` — product toolset module (`builtin_tools` сюда).
- Donor API: `AgentLoop`, `ToolRegistry`, `ToolScheduler`, `ModelProvider`/`StreamAssembler`/`OpenAiCompatibleProvider`, `Tool`/`ToolError`.
- Acceptance: compile-proof (loop-closure не тянет pi-shell/pi-ast/pi-iso/MCP transports); provider smoke через **throwaway/fake provider server** (без секретных API-ключей): streaming, fragmented tool args/IDs, length-границы, truncated args не превращаются в исполняемые calls; **никаких tool-эффектов не выполняется** — D33/D34 требуют intent/outcome persistence до эффекта, реальный tool vertical — S2; bash не наследует env провайдера.
- Почему первым: loop должен компилироваться и стримить до всего; эффекты — только после durable-границ S2.

### S2. SQLite authority + единый session owner + первый реальный tool vertical
- Файлы: `omp-cli` — новый `store/` (rusqlite writer-task; подтверждение — awaited store result) и session owner (слияние `main.rs:2534-3200` + `rpc/session.rs:2740-3277`, fail-closed D34); intent-точка в admission-пути (расширение `scheduler.rs:85-158` через owner, не SQL внутри Tool).
- Donor API: Goose session-manager паттерн; JSONL readers — import only; донорский `TurnAck` — опциональная деталь, не требование.
- Acceptance: **первый настоящий tool call с внешним эффектом выполняется только после durable intent и получает durable outcome** (D33/D34) — вертикаль model→tool→effect→result→answer; crash-инъекция между (message, intent, effect, outcome): intent без outcome → «не подтверждено», никакого blind retry; ошибка записи блокирует следующий вызов/запрос с показом причины (D34); local и headless — один persistence path; нет второго live JSONL; durable active leaf независим от поздних off-branch результатов.
- Почему вторым: без intent/outcome-границ эффекты недопустимы (D33/D34); всё дальнейшее пишет через owner.

### S3. Исполнение: per-agent shell, процессы, FS-policy, writer coordinator
- Файлы: `tools/mod.rs:654-685` BashTool → per-agent shell handle (donor — выбор между original `Session` (pin `035e4f9`) и fork `Shell` (pin `3008cfd`), не взаимозаменяемые); `tools/process.rs:403-459` ProcessBackend reuse; `omp-cli` — shell_state модуль (D76), fs_policy (D82 visibility всех проектов; vault exclusion D27; aliases/symlink/rename проверка), writer coordinator (межпроцессный lease D23), version token (strong full raw-file hash, proposal; не CAS) в write/edit/formatter путях.
- Donor API: `Session/RunOptions/RunOutcome` (original) и/или `Shell::{new,run,abort}` (fork); portable-pty + cancel-контракт; Bashkit Fs-adapter — implementation detail поверх реальных shared files (не вторая namespace).
- Acceptance: две shell делят файлы, но не cwd/env (state не течёт между агентами); stale edit → reject/перечитывание (D44), никакого silent fuzzy rebase; TERM→grace→KILL; второй writer блокируется coordinator-ом; shell env — allowlist; exit in-process не валит runtime.
- Shell default (live vs serialized supported-state) — **decision parent** (gate G-shell §5); обе ветки удовлетворяют D76 в границах runtime-жизни; transparent hibernation не заявляется ни в одной (D87).

### S4. Workers + durable delivery + артефакты
- Файлы: product-side supervisor (сборка из `supervisor.rs` донора без LSP/DAP/eval), delivery-consumers (durable pending queue вместо in-memory `take_*`), адаптация `tools/agent_output.rs` под D81 (см. §2.4).
- **Ограничение до S11:** параллельных пишущих workers нет — workers read-only либо последовательные под workspace-owner; полная acceptance параллельной записи/iso-writer — в S11.
- Donor API: `ChildRecord`/`BackgroundResultSink`/`BackgroundDelivery`, `finalizeSubprocessOutput` (TS executor) как шаблон D79, `Mailbox` типы.
- Acceptance: конец/stop ответа parent не останавливает worker (D68); wake-off сохраняет результат; restart пере-предлагает pending с дедупликацией; артефакт опубликован до DB-ссылки (D81), no-clobber; контекст worker — представление истории (VFS URI router), не дубль файла.

### S5. Batch/бюджеты/координация ресурсов
- Файлы: расширение admission (перепроверка после ожиданий, dependency-группировка batch), бюджеты D26 (steps/time/tokens/parallel/depth/output), agent-slot limiter + очередь D67 (лишние слоты не занимают сохранённые агенты), provider request limiter — отдельный счётчик; явный SQLITE_BUSY retry/backoff протокол store (busy-free не гарантирован ни topology, ни одним writer — см. §7.4).
- Acceptance: mixed failure/cancel сохраняет call/result pairing и порядок; queued-cancel до старта → «не запускался»; зависимые вызовы не параллелятся; лимиты/очередь видны parent и пользователю; overrun → сохранить состояние + причина + продолжение с новым бюджетом (D26), не вечный auto-retry; busy-события видимы и обработаны, не «timeout скрывает».

### S6. Конфиг/инструкции/роли/trust + hooks-seams
- Файлы: config layering (D15: встроенные → user → project → запуск → сессия; LLM-инструкции отдельно от машинного конфига), instruction scopes D48 (общий/project/частный; nested-каталоги), role profiles D51 (Markdown+YAML), trust store D20 (project hooks/MCP/commands — только после явного trust; сохраняемое решение; revoke), policy recheck D29 (новые deny — перед каждым действием, включая субагентов; effective versions в store), hooks **lifecycle seams** (точки расширения pre/post tool, turn, delivery).
- **Hooks staging:** S6 = только seams; **исполняемый hooks runtime (D57 целиком) — отдельный этап после выбора пользователем runtime/формата** (gate, §6). Seams ≠ «hooks done»: D57 не объявляется выполненным по одним seams.
- Donor API: original discovery/settings/role loader semantics; fork Markdown/YAML profiles + config_cmd + schema metadata; `HookToolWrapper` — reference preexec/revalidation. Не переносить: basename-фильтр AGENTS.md, unconditional project hooks.
- Acceptance: слои конфига с проверяемым precedence; named command из конфига не исполняется сам (D15); project-данные не расширяют permissions; invalid reload не портит state; секреты не в обычном просмотре (D69); seams покрыты тестами расширения, но статус D57 — «runtime не выбран», не done.
- Gate: Cedar (G-auth) — optional engine; fallback hand-coded rules сохраняет ту же поверхность allow/deny + host-owned ask/trust/ceilings. Разделение полномочий: tool description hash / env-токены — не sandbox и не authority (D92: concerns разделены, env token не идентичность).

### S7. MCP (параллелимо после S2)
- Файлы: `mcp/` adapter модуль поверх rmcp; registry naming (`mcp/naming.rs`); trust-check до stdio spawn.
- Acceptance: spawn без trust — отказ до запуска; имена привязаны к server identity (collision не вызывает чужой сервер); бинарные resources с типами/MIME (не «всё в base64-text»); sampling/elicitation — через policy/budgets; cancel ≠ отмена remote effect (unknown outcome записывается); capability matrix выбранной версии задокументирована (D54 «полный» = выбранный matrix, не «всё сразу»).

### S8. Код-анализ + карта проекта (параллелимо)
- Файлы: read/search/index/ast-инструменты (вырезка из `tools/mod.rs`/`read.rs`/`ast_*`/glob/grep), path filters D66 (ignore/globset; `literal_separator(true)`), repo map D75 (из индекса, не весь индекс в контекст), formatter через writer coordinator D45, **semantic backends** (Oxc, библиотеки rust-analyzer, SCIP и др. — по доказанным API) с матрицей покрытия язык×операция.
- Acceptance: чтение показывает symbols/blocks + дочитывание; stale index помечен; правки всегда version-checked; **rename/refactor/resolve реализуются по покрытию языка через semantic backends — требование сохраняется, не сужается до tags/syntax**; heuristic-пометка только там, где backend действительно отсутствует (D35/D91); форматтер — писатель через тот же coordinator.

### S9. Память + skills (параллелимо)
- Файлы: memory tables (scope: общий/проектный; source: user-asserted vs agent-inferred), CRUD + просмотр/удаление (D53), skills catalog (D52: names+descriptions → lazy full text; ручной вызов; загрузка не исполняет), custom commands = skills (D58), **полный extraction/learning pipeline** (D14: фоновое извлечение опыта, consolidation, генерация переиспользуемых skills из опыта — автоматизация настраивается; Mem0 — reference алгоритмов, не обязательный runtime).
- Acceptance: **pipeline целиком, не только CRUD**: extraction quality на примерах; provenance user vs inferred; consolidation/derived propagation; жизненный цикл сгенерированного skill (создание → проверка → использование → удаление/пересмотр); удаление memory не трогает историю; skill collision/traversal отклоняются; retrieval baseline — lexical (FTS5 gate G-fts; fallback exact/substring search; lexical baseline ≠ запрет vectors D89), vectors (lancedb embedded) — optional supplement; derived-индекс rebuildable и синхронизирован с deletion/retention.

### S10. TUI/CLI/headless + UI-поверхность
- Файлы: adapters к command/event контракту D37; TUI renderer (Ratatui-кандидат, gate G-tui; не port ANSI renderer original); CLI D62 (text/JSON/NDJSON stream; stderr служебное, stdout данные); worker viewer D63; display modes D25; configurator D69 (TUI + текстовый конфиг + read-only CLI view); session list/search D73 (FTS по истории — derived); context inspector D74 (оценка токенов ≠ usage); export/import D55 (версия формата; credentials не экспортируются).
- Acceptance: один runtime loop во всех режимах; detach не останавливает runtime (D08); чистый NDJSON stdout; resize/Unicode/IME/long output на Termux smoke; секреты скрыты в inspector; один активный клиент (D56).

### S11. Изоляция/интеграция/merge/checkpoints
- Файлы: isolation snapshot (D28: база — внутренний snapshot фактических файлов, не HEAD/index), integration (base/current/result, проверка версии target, bounded recompute D11), ветвление диалога + restore D17/D64, конфликт-flow к parent (D28).
- **Precondition:** iso-часть этого среза открывает параллельных пишущих workers (снятие ограничения S4).
- Donor API: `OutputIdAllocator`-паттерн no-clobber для snapshot-объектов; движок — **gate G-jj / G-merge** (§5, D85: jj vs own store остаётся gate); imara-diff только для отображения diff.
- Acceptance: параллельные пишущие workers с iso-workspaces и merge-интеграцией; dirty/non-Git snapshot integration; конфликт сохраняет варианты + владельца ожидания, без auto ours/theirs; ошибка механизма ≠ merge conflict; повторное применение только при свежей цели; не перезаписывает чужие внешние изменения молча.

### S12. Compaction/контекст, уведомления, диагностика, поставка
- Файлы: `compaction.rs` (донор-семантика как исходная реализация; D18: архив полный, compaction меняет projection; пороги настраиваемые, defaults — отложены; **revision/epoch маркировка проекции**, cache-prefix инвалидация по версии конфига/toolset — направление D90), notifications D65 (TUI индикатор/звук; hooks/notifier наружу), diagnostics D72 (redaction обязательна; **состав безопасного payload отчёта открыт** — не blanket-запрет истории/вывода, конкретный состав — отложенное решение), packaging D71 (один binary + рабочие каталоги; Termux/Bionic отдельный артефакт; CI: cross-Android toolchain matrix, bundled C — cc; **конкретные NDK/API floor — compatibility proof при реализации**, текущие значения CI донора — факт, не требование), budgets UI (D26), обновления — отложены пользователем.
- Acceptance: compaction не удаляет архив и не теряет текущую задачу/незавершённое (D18); смена epoch видима и не выдаётся за бесплатный cache hit; отчёты/логи без credentials (redaction); init каталогов D71 создатся при установке; CI собирает Linux + aarch64-linux-android с выбранным совместимым toolchain.

## 5. Library candidate gates (fallback сохраняет требования)

Proof-политика: тяжёлые сборки/полные suites — **CI** (Linux + cross-Android NDK); **device smoke допустим после разрешения (approval)**, короткий; blanket «CI only» не требуется; CI не использует секретные API-ключи — provider/MCP proof через throwaway/fake server (provider-MCP contract); тяжёлые сборки не на телефоне (fixed assumption 8). Не выбранный optional candidate не блокирует независимые срезы (§7). Точные версии кандидатов фиксируются при реализации — здесь только research-пины.

| Кандидат | Gate (проверяемый до выбора) | Fallback, сохраняющий требования |
|---|---|---|
| **jj-lib 0.44/0.47** (Apache-2.0) — private internal backend для snapshot/merge (S11) | G-jj: CI build Linux+Termux; `Workspace::init_internal_git` на plain dir; snapshot **без** HEAD/index mutation (lib-вызов `reset_head` не делать; colocated пишет objects+`refs/jj/keep` — избегать); commit caller-owned; **замер** стоимости storage/первичного snapshot (jj копирует blobs в `.jj/repo/store/git/objects`) vs бюджет — до решения, не предрешённый kill; checkout per-file без COW допустим только для bounded iso-workspaces; пин MSRV/toolchain (0.44: rust 1.89; 0.47: 1.97 + git-backend split) | Свой snapshot store (file copy + hash manifest, no-clobber паттерн) + textual 3-way + явные конфликты — D11/D28 требования сохраняются (base/current/result, stale reject, bounded recompute); fallback тоже стоит storage (копии) — сравнение по замеру. D85: jj vs own store — решение только после gate |
| **Merge engine**: diffy (3-way candidate), mergiraf (semantic; нужен license/coverage proof), jj-lib MergedTree | G-merge: 3-way корректность на dirty/non-Git фикстурах; stale target reject; bounded recompute; конфликт ≠ ошибка механизма | Текстовый 3-way + сохранение вариантов конфликта. **imara-diff — только diff algo, никогда merge/fuzzy** (crate-revision §3) |
| **Cedar 4.x** (Apache-2.0) — auth (S6) | G-auth: Allow/Deny only, default deny, forbid > permit; skip-on-error → conservative-обращение с diagnostics; host валидирует policy/schema и владеет ask/trust/child ceilings/path resolution | Hand-coded rules с той же поверхностью (allow/deny; ask — host, не engine). Engine не mandatory до gate; не OS sandbox |
| **rmcp** (Apache-2.0) — MCP (S7) | G-mcp: выбранный protocol matrix (tools/resources/prompts/sampling/elicitation/roots), stdio+HTTP, cancellation unknown-outcome, Bionic build, MSRV | Свой client за тем же adapter-швом; требование D54 (полный выбранный matrix) сохраняется, scope не режется |
| **Bashkit** interpreter-state vs live shell session (S3) | G-shell: round-trip serialized supported-set (env/vars/arrays/cwd/functions/aliases/traps/options по shell-revision §2); RealFs с общими реальными файлами (не вторая namespace); лимиты задокументированы: fds/jobs/внешние процессы НЕ сериализуются (ProcessBackend владеет, D59) | Live per-agent shell в памяти процесса + startup-script export (donor `snapshot_path` паттерн) — D76 в границах runtime-жизни; реставрируется только supported-set, остальное явно «не восстановлено». **Никакого transparent hibernation: cwd/env-only усыпление не сохраняет functions/options/aliases/fds/jobs; sentinel не authority** (D87, shell-revision §3-§6). Донор live-session: original `Session` (pin `035e4f9`) либо fork `Shell` (pin `3008cfd`) — разные API |
| **rusqlite bundled** (+FTS5) (S2/S9) | G-sql: bundled build на aarch64-linux-android и Termux-native clang (toolchain floor — compatibility proof, не фиксированная NDK-версия); WAL + явный retry/backoff протокол на SQLITE_BUSY (**busy возможен и при одном writer** — readers/checkpoints/прочие соединения; busy-free не гарантирован) | Другой SQLite binding — authority остаётся SQLite (D32); схема наша в любом случае. Архитектурная справка: SQLite blobs допускают incremental I/O; внешние файлы (D32) — архитектурный выбор, не следствие неизбежного BUSY |
| **FTS5 BM25/trigram** (S9/S10) | G-fts: build+query smoke, размер индекса на реалистичном корпусе | Exact/substring search по SQLite-строкам (bounded scans) — retrieval требование сохраняется, perf — позже. Lexical baseline ≠ запрет vectors (D89); vectors (lancedb 0.41 embedded, Apache-2.0) — optional supplement; «zero RAM» не заявлять |
| **portable-pty** (S3) | G-pty: openpty/resize/TERM→KILL — короткий device smoke (после approval) | Fork `pi-natives` pty-путь (donor) за тем же `ProcessBackend` |
| **reqwest/TLS** (S1/S7) | G-http: Bionic TLS build (aws-lc-rs/ring требуют NDK cc — shell-revision §5.3; «pure Rust без setup» — ложь); smoke через fake server, не live credentials | Альтернативный HTTP client за `ModelProvider` — D30 требование то же |
| **TUI stack** (Ratatui/crossterm) (S10) | G-tui: terminal smoke (long output, resize, Unicode/IME, Termux) — device smoke после approval | Другой renderer за тем же event contract |
| **Semantic backends** (S8): Oxc, библиотеки rust-analyzer, SCIP и др. | G-sem: build на целевых платформах; покрытие операций (rename/refactor/resolve) по матрице язык×операция; honesty-marking для непокрытого | Другой backend за тем же index-контрактом; требование rename/refactor по покрытию сохраняется (D35/D75). **stack-graphs — archived; tags/syntax не заменяют semantic требование** (D91) |
| **secrecy + aho-corasick** (кросс-срезно) | Кандидаты-wrapper'ы; scrub = streaming известных секретов (literal Aho + отдельные regex-сигнатуры, chunk-boundary обязателен), scan только чтение | Regex-сканер свой; не Gitleaks-engine; **не 100% leak-proof, не переписывание исходников** |

## 6. Отложенные решения (не выбираются молча)

Не решать в реализации без parent/user: API dialect/base_url/имя env-переменной (D30); retry/backoff/timeouts/effort/token limits (provider); **hooks runtime/язык/формат/синхронность (D57 — исполняемый runtime только после выбора; seams недостаточно)**; формат конфига и schema подсказок (D69); формат/пути role profiles и precedence контекст-файлов (D48; в донорах docs/source расходятся); определения active/waiting для лимита агентов (D67); default shell: live vs serialized (G-shell); SQLite schema/migrations/fsync/WAL tuning; URI spelling resource router (D21/D79/D81); trust-хранилище/revoke/config-reload reaction (D20/D29); формат export-файла (D55); каналы/defaults уведомлений (D65); retention defaults (D24); memory extraction/consolidation финальные алгоритмы и retrieval backend (D14); dangerous-command pre-intercept критерии (D39 — предложение не принято); шифрование/master key/unlock (D27 — отложено пользователем); multi-client (D56); обновления агента (отложены); @files/клавиши UX; OS-изоляция внешних программ (явно отложена); значения budgets; default D50-таймера auto-resolve; состав безопасного payload отчёта (D72 — redaction обязательна, состав открыт); toolchain/NDK/API floor (compatibility proof при реализации); точные версии всех библиотек-кандидатов.

**D84–D93: parent-triage получен, accepted journal НЕ изменён; фиксация — за parent/user.** План закладывает только triage-направления, не package defaults:
- **D86**: conceptual split **реального disk workspace/snapshot/integration vs synthetic resource URI router** — совпадает с fixed assumption 1; generic VFS proxy для обычных файлов не требуется.
- **D84**: raw content version + stale protection, не CAS rename; hash+rename не atomic CAS против external writer; mtime — только оптимизация.
- **D88**: recovery — per-tool контракт (§3.12); гонка вопроса — одна atomic conditional transition (§3.1); wchan/kill-on-silence не используются — stdin режимы явные (EOF/PTY, fixed assumption 13).
- **D89**: FTS lexical baseline, не запрет vectors; single writer не гарантирует отсутствия busy (retry-протокол в S5/§7.4).
- **D90**: compaction/prefix — revision/epoch, не «навсегда fixed» (§3.2, S12).
- **D85**: jj vs own store — остаётся gate (G-jj), решение после замера.
- **D87**: нет transparent cwd/env-only hibernation; sentinel не authority (G-shell).
- **D91**: tags ≠ полное semantic покрытие; semantic backends и rename/refactor требования сохраняются (S8).
- **D92**: concerns разделены; env token / tool description hash не authority и не sandbox (S6).
- **D93**: форматные выборы (TOML/JSON-RPC и пр.) — deferred, не принимать автоматически.
- Не закладываются: `max_peer_hops` default/blocking-запреты, `_AGENT_TASK_TOKEN` как identity, blake3/zstd CAS, delta-scan как isolation.

## 7. Реальные конфликты и независимые части

Реальные конфликты/напряжения:
1. **jj-lib snapshot vs мобильное хранилище**: jj копирует blobs в store (evidence §4) — существенный риск для D24-limits, **но не предрешённый kill**: собственная fallback-копия тоже стоит storage; решение — по замеру G-jj, обе ветки сравниваются одинаково.
2. **Serialized shell default vs D59 long servers**: сериализация не переносит живые jobs/procs/fds (shell-revision §4) — ProcessBackend обязателен в обоих вариантах; «default shell» — parent-decision, а не implementation detail.
3. **D50 auto-resolve default** (таймер/включённость) не выбран — headless-вопросы без ответа зависят от значения; сама гонка ответа решается atomic conditional transition (D88), но default политики не выбирать молча.
4. **BUSY не убирается полностью**: один writer на процесс — topology-выбор, но SQLITE_BUSY возможен и внутри процесса (readers, checkpoint, параллельные соединения) и между runtime-процессами (D23 lease не выбран) — явный busy retry/backoff протокол обязателен везде (риск S5, не повод блокировать остальное).
5. D82 visibility vs scope инструкций — не конфликт решений, но правило реализации: видеть проект ≠ загружать его инструкции/индексы (shared-fs §5); фиксирую, чтобы не задвоить контекст.

Независимые (не ждут optional candidates): S2 store; S7 MCP; S8 анализ (read-only + semantic backends); S9 memory/skills pipeline; provider+replay corpus (fake server); secrets scrubber; S10 adapters после контракта S2. G-jj/G-merge блокируют только механику S11; ограничение S4 (read-only/sequential workers) держится до S11-iso независимо от исхода gates.

## 8. Evidence (anchors)

Provenance: `crates/...` — WT форка `3008cfd`; original OMP — pin `035e4f9`; версии крейтов — research-кандидаты, не accepted.

- Loop/тип-резы (форк `3008cfd`): `crates/pi-agent/src/loop.rs:679-707,206-252,266-330`; `state.rs:1315-1455,1352-1390`; `tools/registry.rs:5,47-52,112-122`; `tools/scheduler.rs:59-163`; `supervisor.rs:123-138,167-174`; `provider.rs:191-253,257-630,632-1016`; `lib.rs:1-52`; `local://loop-type-cut.md`, `local://tool-host-cut.md`.
- Session owner (форк `3008cfd`): `crates/omp-cli/src/runtime.rs:227-267,2149-2555,4960-5062,522-530`; `main.rs:2534-3200`; `rpc/session.rs:2740-3277`; `agent.rs:32-99,484-498`; `session.rs:1396-1420`; `local://session-ownership-cut.md`.
- Исполнение/shell/процессы: форк `tools/mod.rs:654-685`, `tools/process.rs:403-459`, `tools/read.rs:98`, `tools/agent_output.rs:42-53,168,236`, `crates/pi-shell/src/shell.rs:134,779-811`; original (pin `035e4f9`) `crates/pi-shell/src/session.rs`, `crates/pi-shell/src/cancel.rs`; `local://execution-donors.md`, `local://shell-revision-evidence.md` (§2 Bashkit ShellState, §3 brush serde skip jobs, §6 выводы).
- Состояние/журнал: `docs/agent-architecture-decisions.md` D32-D34/D47/D80-D81; `local://journal-revision-evidence.md` (§2 event-sourcing ≠ D80, §2.3 commands≠replay, §3 cursor, §10 таблица required/optional); `local://jj-revision-evidence.md` (§1-§5, §9 verdicts).
- Код-анализ: `local://crate-revision-evidence.md` (§3 imara-diff, §4 globset `literal_separator`, §6 stack-graphs archived); `docs/research-2026-11-06-code-analysis.md` (semantic backends, §4.3/§8).
- Политика/секреты: `local://policy-redaction-revision.md` (Cedar PARC, secrecy, aho-corasick); D27/D30.
- Контракты: `docs/agent-execution-contract-proposal.md` (вызов/восстановление/proof), `docs/agent-provider-mcp-contract-proposal.md`, `docs/agent-core-extraction-design.md` (§2 разрезы, §9 порядок extraction diff), `docs/agent-loop-tool-implementation-map.md` (§4 flow, §8 порядок шагов).

## 9. Critical gaps

1. Всё source-only: extraction diff/compilation/smoke — не выполнялись (ограничение read-only); «runtime готовность» ничем не доказана.
2. Bionic/NDK evidence нет ни для одной C-зависимости (rusqlite bundled, TLS, грамматики); compatibility proof toolchain — обязательный первый шаг любого gate (конкретная NDK/API floor не предрешена).
3. ACP persistence path не прослежен целиком (session-ownership-cut R7) — не переносить его совместимость молча.
4. D84–D93: triage parent есть, фиксации user нет; до неё внутренние решения S3 (shell default), S5 (busy/deadlock протокол), S11 (merge engine) остаются двухветочными — но не блокируют S1–S10.
5. Мобильная стоимость jj-snapshot и FTS-индекса не измерена (обе ветки storage стоят); gates ставят замер до выбора. Межпроцессный writer lease (D23) не выбран.
6. Донорские API original `Session` vs fork `Shell` не сверены по совместимости — выбор engine и мост состояния остаётся за S3-реализацией (G-shell).
