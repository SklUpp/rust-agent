# Типы, роли и организация агентов

Дата: 2026-11-05. Для самостоятельного Rust coding-first агента: Termux/Linux TUI/CLI и личный headless. Исследование документации и публикаций; реализации и benchmark runs не выполнялись.

## Вывод

Предлагаемая архитектура: универсальный agent loop, runtime executor для lifecycle и конфигурируемые профили. При включённом делегировании executor управляет дочерними задачами. Базовый путь — один агент; мультиагентная организация не обязательный этап каждой задачи. Это рекомендация для продукта, не требование источников.

## Разделить пять понятий

| Понятие | Пример | Что определяет |
|---|---|---|
| Роль/профиль | researcher, implementer, reviewer | Инструкция, формат результата, предпочтения инструментов и default model binding; не выдаёт полномочия |
| Полномочия | read-only, write workspace, network | Реально разрешённые эффекты; enforce runtime, не prompt |
| Топология | parent-workers, handoff, peers | Владение целью, координацией и финальным ответом |
| Lifecycle | one-shot, resumable, parked, background | Продолжительность, состояние и управление |
| Место исполнения | local, isolated workspace, remote host | Ресурсы и доступ к workspace, не модель LLM |

Профиль не выдаёт полномочия. Одна модель может выполнять разные роли; разные модели могут исполнять одинаковый профиль. Фоновый процесс shell не становится агентом, а один LLM-вызов compaction не требует самостоятельного agent loop.

## Что подтверждают первоисточники

- [Anthropic: building effective agents](https://www.anthropic.com/engineering/building-effective-agents): workflow имеет предопределённые code paths, agent динамически выбирает действия. Chaining, routing, parallelization, orchestrator-workers и evaluator-optimizer — разные паттерны, не обязательные роли.
- [OpenAI Agents orchestration](https://openai.github.io/openai-agents-python/multi_agent/): agents-as-tools оставляет владельцем разговора manager; handoff передаёт активную работу специалисту. Это разные контракты, а не два имени одной делегации. Дополнительно проверено через Context7 официальной документации.
- [OpenAI handoff filtering](https://openai.github.io/openai-agents-python/handoffs/): передаваемый контекст настраивается; удаление tool records не очищает копии их содержимого в обычных сообщениях. Изоляция контекстов не равна гарантированной секретности.
- [Claude Code subagents](https://code.claude.com/docs/en/sub-agents): профили с отдельным context window, instructions, tools и permissions; built-ins Explore/Plan/general-purpose — пример конфигурации, не нормативный набор нового продукта.
- [LangChain subagents](https://docs.langchain.com/oss/python/langchain/multi-agent/subagents): supervisor вызывает workers через tools; workers могут быть stateless, контекст менеджера и подзадачи различается. Не переносить Python framework ради самого паттерна.
- [Microsoft Agent Framework orchestration](https://learn.microsoft.com/en-us/agent-framework/workflows/orchestrations/): sequential, concurrent, handoff, group chat и manager-driven orchestration представлены отдельно. Наличие паттерна в SDK не доказывает его нужность coding-agent.
- [Anthropic Research](https://www.anthropic.com/research/multi-agent-research-system): lead-worker подход полезен для независимого breadth-first поиска; контексты workers сжимаются в результаты. Авторы отдельно предупреждают о token cost и меньшем количестве параллелизуемых частей в coding. Артефакты со ссылками уменьшают потери пересказа.

## Сравнение топологий

| Топология | Кому принадлежит результат | Когда полезна | Решение для продукта |
|---|---|---|---|
| Один агент | Основному агенту | Малые и связанные задачи, общий изменяемый контекст | Стандартный путь |
| Parent + bounded workers | Родителю | Исследование, изолированные изменения, независимое ревью | Основной механизм делегирования |
| Иерархия supervisors | Верхнему владельцу цели | Большие декомпозируемые задачи с самостоятельными подсистемами | Возможность с лимитом глубины, не default |
| Handoff | Новому активному агенту | Передача владения целью и пользовательским диалогом | Опционально; смена профиля и compaction сами по себе handoff не являются |
| Peer/group chat | Зависит от отдельно заданного протокола | Специализированная кооперация и экспериментальные сценарии | Не основа coding runtime |
| Fixed workflow/DAG | Управляющему коду | Известный pipeline с проверяемыми стадиями | Пригоден как опциональная автоматизация, не замена свободному loop |
| Evaluator-optimizer | Контроллеру цикла | Есть проверяемый критерий улучшения и конечный бюджет | Опционально; мнение evaluator не заменяет тест |

Runtime executor — детерминированное управление lifecycle/бюджетами; LLM-supervisor — профиль агента, выбирающего делегирование. Это разные понятия. Основной агент может координировать сам, без дополнительного manager-agent. Effective model выводится из default binding профиля и разрешённых overrides и фиксируется в запуске; tools профиля — заявка, effective доступ вычисляет policy.

Compaction заменяет представление контекста, сохраняя владельца, effective profile/version, permissions и оставшиеся бюджеты. Profile switch — отдельная явная операция с новой версией и актуальной policy; владелец не меняется автоматически. Handoff меняет владельца продолжения и финального ответа по явному протоколу передачи.

## Какие роли действительно нужны

| Профиль | Статус | Ограничение |
|---|---|---|
| Основной агент | Базовый запуск | Владеет пользовательской задачей и итоговым утверждением; не обязательный отдельный specialist type |
| Универсальный worker | Базовый механизм, запуск опционален | Цель, scope, бюджет, исходная revision, критерий возврата |
| Research/explore | Удобный встроенный профиль | Read-only enforce инструментами; чтение тоже может раскрывать секреты |
| Implementer | Удобный встроенный профиль | Один writer общего workspace, параллельные writers в изоляции |
| Reviewer | Полезный опциональный профиль | Отдельный контекст/исполнение, findings с evidence; независимость ошибок не гарантируется |
| Security reviewer | Опциональный профиль по риску | Не security boundary и не сертификация отсутствия уязвимостей |
| Planner/designer | По неоднозначности задачи | Предлагает решения, не получает автоматически запись/исполнение |
| Advisor | По потребности | Второе мнение, не второй владелец задачи |
| Memory/compaction/title | Обычно bounded service call | Без tools и автономного loop, пока они не нужны |
| Test runner/indexer | Обычно deterministic job | Не нужен LLM для выполнения заданной команды |

Результат ревью должен ссылаться на проверенную revision и артефакт; findings могут устареть после правок. Другой prompt той же модели не гарантирует независимость ошибок, хотя отдельный контекст полезен. Несколько согласных агентов не доказательство корректности.

## Что показывают исследования

[CooperBench](https://arxiv.org/abs/2601.13295), опубликован 2026-01-19: более 600 collaborative coding задач, 12 библиотек, четыре языка, expert tests. Двум агентам поручались разные фичи, способные конфликтовать. Авторы сообщают в среднем меньший success совместной работы, чем одиночной. Это не доказательство бесполезности делегирования: benchmark проверяет именно координацию пересекающихся изменений.

[Towards a Science of Scaling Agent Systems](https://arxiv.org/abs/2512.08296), опубликован 2025-12-09: 260 конфигураций, шесть benchmarks, пять архитектур, три семейства моделей. Контролировались tools/prompts/compute. В конфигурациях статьи результаты связаны со структурой задач: tool-heavy и sequential задачи могут проигрывать от coordination overhead. Это направление наблюдения, не универсальный порог или прогноз для нашего runtime.

[Anthropic Research engineering](https://www.anthropic.com/research/multi-agent-research-system) показывает практическую пользу parallel research, но внутренние результаты и token ratios относятся к их системе и сравнению; переносить проценты на телефон, coding и новые модели нельзя. Вывод для нашего продукта — измерять собственные задачи и стоимость, не закреплять «больше агентов лучше».

Другие найденные работы: [When Agents Coordinate](https://arxiv.org/html/2608.16801v1), [When Parallelism Pays Off](https://arxiv.org/html/2606.00953), [ParallACT](https://arxiv.org/abs/2611.00314). Они обнаружены отдельным исследовательским агентом; в этом отчёте не используются как единственное основание решений и их численные результаты не переносятся на продукт.

## Предлагаемый контракт worker

Это design proposal, не готовая реализация:

- Task identity, parent identity и профиль с версией.
- Objective, acceptance, allowed resources/tools, base workspace revision.
- Context pack: выделенные материалы/ссылки; не вся история и credentials по умолчанию.
- Model override и budgets: concurrency, turns, tokens/cost, deadline, descendant limits.
- Явные terminal outcomes: completed, failed, cancelled; ожидание/парковка — не успех. При неизвестном эффекте сохраняется отдельный outcome операции.
- Результат: summary, findings/changes, ссылки на артефакты, выполненные проверки и ограничения.
- Resume/steer не меняют permissions автоматически; новая инструкция повторно проходит актуальную policy.
- Результаты доступны без живого TUI. Отмена родителя задаёт политику потомков, best-effort остановку и durable учёт известных процессов; сбой/daemonization могут потребовать reconciliation, полного контроля любого процесса не обещаем.
- Интеграция результата сериализована владельцем записи. Проверяется текущая версия относительно base revision и локальных изменений; устаревший patch требует rebase/merge и повторной проверки. Конфликт останавливает применение, не перезаписывает чужие изменения. Автоматическое объединение допустимо только по отдельно заданной политике.
- Failed/cancelled/timeout reviewer означает отсутствие завершённого ревью, не pass; findings перепроверяются для актуальной revision.

Runtime states и журнал не должны хранить provider chain-of-thought как необходимый контракт. Достаточны действия, публичные сообщения, результаты и служебные metadata, нужные для replay и диагностики.

## Правила принятия делегирования

Single-agent baseline обязателен для оценки. Сравнивать один и тот же corpus задач: small fix, coupled refactor, independent modules, research, review. Фиксировать модели/версии и бюджеты; учитывать success, regression, merge conflicts, wall time, tokens/cost, число сообщений, вмешательства человека. Несколько повторов для распределения, а не вывод из одного успешного запуска.

Параллельный research и независимое read-only review — ранние кандидаты. Параллельные writers требуют не только разных путей, но согласованного интерфейса и интеграции; worktree устраняет файловую гонку, не семантический конфликт. Не заставлять основной агент собирать команду для тривиальной правки.

## Итоговое предложение

Выбрано пользователем: базовый самостоятельный агент и нефиксированная многоступенчатая организация нескольких агентов. Один механизм исполнения обслуживает одиночную задачу, parent-workers, последовательные стадии, параллельные ветви и вложенную декомпозицию. Роли — профили, не отдельные типы runtime. Возможность использовать модель организации не означает обязательность делегирования для каждой задачи.

Предложение для дальнейшего согласования: динамический выбор организации внутри лимитов плюс явный workflow по запросу пользователя. Runtime executor управляет lifecycle, агент выбирает декомпозицию; отдельный LLM-координатор не обязателен. Handoff владельца диалога и peer teams — отдельные опции. Сохраняются scope/permissions, владелец результата, лимиты потомков и сериализованная интеграция в общий workspace. Background/resumable/isolation — параметры исполнения, не роли.

Проверка артефакта: `test -s docs/agent-roles-topologies-research-2026-11-05.md` из checkout. Runtime и benchmark проверки в исследовании не выполнялись.

## Конкретный пример original OMP

На [035e4f95, task-agent-discovery.md](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/task-agent-discovery.md) профиль содержит обязательные name/description/systemPrompt и опциональные tools/spawns/model/output/structured. Это подтверждает отделение специализации от механизма исполнения. `modelRoles` отдельно связывает назначение с моделью; роль reviewer и alias модели review — не одна сущность.

Discovery допускает project/user/extension/bundled профили, exact-name precedence и runtime rediscovery. Для нашего продукта это пример, не готовая политика доверия: файл репозитория не должен расширять operator permissions, а смена профиля посреди задачи требует сохранения effective profile/version. Профили из workspace считать недоверенными до применения operator policy.

Важная ловушка OMP: отсутствие tools и явный пустой tools list различаются; дополнительные extension/MCP инструменты учитываются отдельно. При собственном schema не копировать это неявно: определить, означает ли список полную allowlist либо только видимость built-ins. Если нужен read-only профиль, проверять реальные эффекты всех доступных путей, включая bash, eval, MCP и вложенные agents, а не только отсутствие edit/write.

Дополнительные источники профилей: [original scout](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/prompts/agents/scout.md), [original reviewer](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/prompts/agents/reviewer.md), [Codex subagents](https://developers.openai.com/codex/subagents). Конкретные лимиты конкуренции/depth/tokens донора не становятся default нашего агента.

