# Документация aragent

Единый комплект: нормативные документы, предложения и архив исследований.
Нормативный статус имеет только первый раздел; остальное — основания и история.

## Порядок чтения

1. [План работ](agent-preliminary-implementation-plan.md) — что делаем сейчас и что осталось.
2. [Решения](agent-architecture-decisions.md) — принятые границы, D01–D83, D97 и уточнения.
3. [Handoff](agent-work-handoff.md) — снимок HEAD, проверки и ближайший шаг.
4. [Архитектура](agent-architecture.md) — общая картина и границы продукта.

При расхождении: код и тесты → решения → план → handoff → предложения → исследования.
Исследовательский документ не является требованием, даже если содержит «принято».

## Нормативные

| Файл | Роль | Актуальность |
|---|---|---|
| `agent-preliminary-implementation-plan.md` | порядок R0–R12, критерии и состояние срезов | обновляется при закрытии среза |
| `agent-architecture-decisions.md` | принятые решения и их границы | обновляется при новом решении |
| `agent-work-handoff.md` | снимок состояния и следующий шаг | обновляется после каждого CI-прогона |

## Предложения и проекты

Не нормативны, пока решение не внесено в `agent-architecture-decisions.md`.

| Файл | Тема |
|---|---|
| `agent-architecture.md` | архитектурный проект (2026-11-05); решения D01–D81, не спецификация |
| `agent-provider-mcp-contract-proposal.md` | provider и MCP: контракт интеграции |
| `agent-execution-contract-proposal.md` | исполнение инструментов, admission и границы эффекта |
| `agent-core-extraction-design.md` | извлечение Rust-ядра из донора |
| `agent-loop-tool-implementation-map.md` | карта реализации agent loop и инструментов |
| `modular-reuse-blueprint.md` | согласованная матрица модульного переиспользования, Cargo features и лицензий (2026-11-20) |
| `agent-reuse-and-templates-plan.md` | план унификации обвязки, шаблоны вариаций модулей и дорожная карта волн W1–W3 (2026-11-27) |
| `agent-cross-os-exec-pipeline.md` | кросс-платформенный пайплайн изоляции/запуска команд (`pi-sys::exec`, Linux/Android/macOS/Windows) (2026-11-27) |

## Архив исследований

Датированные исследования 2026-11-05/06. Сохраняются как evidence, не обновляются.

- `omp-original-foundation-research-2026-11-05.md`, `rust-agent-research-2026-11-05.md`, `rust-port-reuse-research-2026-11-05.md`
- `independent-agent-research-2026-11-05.md`, `agent-roles-topologies-research-2026-11-05.md`, `rust-agent-component-verdicts-2026-11-05.md`
- `9router-integration-research-2026-11-05.md`, `omp-compaction-research-2026-11-05.md`
- `agent-shared-fs-research-2026-11-06.md`
- `research-2026-11-06-state-context.md`, `research-2026-11-06-execution.md`, `research-2026-11-06-providers-mcp.md`
- `research-2026-11-06-code-analysis.md`, `research-2026-11-06-memory-skills.md`, `research-2026-11-06-frontends.md`
- `research-2026-11-06-packaging.md`, `research-2026-11-06-profiles-hooks.md`

### Исследования 2026-11-27 (7 треков + независимый аудит ревью)

Не являются нормативными требованиями; каждый документ включает поправки и ограничения из `ResearchReviewPlatform` и `ResearchReviewCoreEcosystem`.

- `research-2026-11-27-01-omp-sync.md` — Track 1: выборочный перенос из `OMP` / `omp-termux` → `arag`
- `research-2026-11-27-02-tools-unification.md` — Track 2: оптимизация, унификация и упрощение инструментов `arag`
- `research-2026-11-27-03-fastproot-lkl.md` — Track 3: интеграция `fastproot` + `lkl` и единая модульная кросс-OS система
- `research-2026-11-27-04-android-sdk-kernel.md` — Track 4: адаптация к Android (SDK 28, ядра, Bionic, SELinux, fork-пробы)
- `research-2026-11-27-05-ecosystem-crates-profiles.md` — Track 5: экосистема крейтов/проектов и адаптивные Android-профили
- `research-2026-11-27-06-9router-fork-sync.md` — Track 6: интеграция `9router` / `9routerx` / `CLIProxyAPI` (fork/upstream sync)
- `research-2026-11-27-07-multiplatform-fallback.md` — Track 7: мультиплатформенная одновременная работа, remote offload и fallback
- `research-2026-11-27-08-reviews-audit.md` — Сводный аудит независимых ревью (`ResearchReviewPlatform` и `ResearchReviewCoreEcosystem`) + матрица Go/No-Go
- `research-2026-11-27-09-vfs-path-mirroring.md` — Расширение VFS до единого формата зеркалирования путей (`PathSpace` / `RootMap`, пересмотр границ D86)
- `research-2026-11-27-10-platform-hal-tool-availability.md` — Платформенное разделение кода (`pi-sys` HAL), runtime-пробы `Caps` и максимальная доступность инструментов по OS
- `research-2026-11-27-11-unified-domain1-core-state.md` — Унифицированный аудит Домена 1 (`ScoutUnifiedCoreState`): core loop, SQLite v7, compaction, VFS
- `research-2026-11-27-12-unified-domain2-tools-exec.md` — Унифицированный аудит Домена 2 (`ScoutUnifiedToolsExec`): инструменты, shell, изоляция, `pi-iso`/`pi-vcs`, `fastproot`/LKL
- `research-2026-11-27-13-unified-domain3-providers-sec.md` — Унифицированный аудит Домена 3 (`ScoutUnifiedProvidersSec`): провайдеры, `9Router`, MCP, auth, trust, redaction
- `research-2026-11-27-14-unified-domain4-analysis-memory.md` — Унифицированный аудит Домена 4 (`ScoutUnifiedAnalysisMemory`): `pi-ast`, RepoMap, LSP/DAP, memory/embeddings, skills/rules
- `research-2026-11-27-15-unified-domain5-frontends-ops.md` — Унифицированный аудит Домена 5 (`ScoutUnifiedFrontendsOps`): TUI/CLI/REPL/RPC/ACP, hooks, profiles, R9–R11
- `research-2026-11-27-16-unified-domain6-platform-pkg.md` — Унифицированный аудит Домена 6 (`ScoutUnifiedPlatformPkg`): платформа/Android HAL, packaging/notices, CI и R12
- `research-2026-11-27-17-unified-corpus-index.md` — Унифицированный индекс корпуса (`ScoutDocsUnifiedIndex`): 25-строчная матрица, 22 конфликта и 12 целевых направлений Stage 2 (`DIR-01..DIR-12`)
- `research-2026-11-27-18-unified-reviews-errata.md` — Независимый аудит ревью унифицированных исследований (`ReviewUnifiedBatch1`, `ReviewUnifiedBatch2`, `ReviewUnifiedBatch3`)
- `research-2026-11-27-19-target-dir01-04-05-status-scope-journal.md` — Целевое исследование Stage 2 (`TargetDir01StatusTruth`): канонический статус/HEAD/CI (`DIR-01`), дрейф `SCOPE_LATEST.yaml` (`DIR-04`) и журнал решений D80/D84–D96 (`DIR-05`)
- `research-2026-11-27-20-target-dir03-06-approval-hooks-safety.md` — Целевое исследование Stage 2 (`TargetDir03ApprovalSafety`): отмена/approvals D50 (`DIR-03`), hooks/trust (`DIR-06`), обход `hub start` и пробелы DAP
- `research-2026-11-27-21-target-dir07-provider-mcp-wire.md` — Целевое исследование Stage 2 (`TargetDir07ProviderMcpWire`): wire-контракт провайдеров и MCP (`DIR-07`)
- `research-2026-11-27-22-multi-os-containment.md` — Мультиплатформенная процессная и файловая контейнеризация (`ResearchMultiOSContainment`): матрица Linux/Android/macOS/Windows, проверка лицензий и контракты `UnknownOutcome`/kill/PTY/net
- `research-2026-11-27-23-agent-containment-architecture.md` — Архитектура сдерживания внутри `arag` (`ResearchAgentContainmentArchitecture`): инвентаризация швов спавна/БД/ФС, Host Control Plane vs Contained EP, P1/P2 дыры границ
- `research-2026-11-27-24-omp-ts-uncovered-gaps.md` — Карта и аудит непокрытого/частичного TS/JS кода `~/omp-work/packages/` vs `rust-agent` (`ScoutOmpTsMap` + `ScoutOmpGapD1..D6`)

## Правила обновления

- Статус среза меняется только в плане; решение — только в решениях.
- Handoff не дублирует план: в нём только HEAD, проверки и ближайший шаг.
- Старые исследования не переписываются; новое состояние оформляется решением или срезом плана.
- Удалённые документы не восстанавливаются из истории молча: замена указывается в решении.
