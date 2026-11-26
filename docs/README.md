# Документация aragent

Единый комплект: нормативные документы, предложения и архив исследований.
Нормативный статус имеет только первый раздел; остальное — основания и история.

## Порядок чтения

1. [План работ](agent-preliminary-implementation-plan.md) — что делаем сейчас и что осталось.
2. [Решения](agent-architecture-decisions.md) — принятые границы, D01–D83 и уточнения.
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

## Архив исследований

Датированные исследования 2026-11-05/06. Сохраняются как evidence, не обновляются.

- `omp-original-foundation-research-2026-11-05.md`, `rust-agent-research-2026-11-05.md`, `rust-port-reuse-research-2026-11-05.md`
- `independent-agent-research-2026-11-05.md`, `agent-roles-topologies-research-2026-11-05.md`, `rust-agent-component-verdicts-2026-11-05.md`
- `9router-integration-research-2026-11-05.md`, `omp-compaction-research-2026-11-05.md`
- `agent-shared-fs-research-2026-11-06.md`
- `research-2026-11-06-state-context.md`, `research-2026-11-06-execution.md`, `research-2026-11-06-providers-mcp.md`
- `research-2026-11-06-code-analysis.md`, `research-2026-11-06-memory-skills.md`, `research-2026-11-06-frontends.md`
- `research-2026-11-06-packaging.md`, `research-2026-11-06-profiles-hooks.md`

## Правила обновления

- Статус среза меняется только в плане; решение — только в решениях.
- Handoff не дублирует план: в нём только HEAD, проверки и ближайший шаг.
- Старые исследования не переписываются; новое состояние оформляется решением или срезом плана.
- Удалённые документы не восстанавливаются из истории молча: замена указывается в решении.
