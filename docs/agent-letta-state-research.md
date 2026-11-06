# Letta отдельно: контекст, память и восстановление

Дата 2026-11-06. Source research, не внедрение framework. Pin letta-ai/letta@1131535716e8a31c9a437f8695e25ac98f203a24, tag 0.16.8, Apache-2.0. Python service stack не подходит обязательной зависимостью standalone Rust runtime; перенос допустимых механизмов рассматривается отдельно. Ничего не запускалось/устанавливалось/собиралось.

## Назначение и доноры

Letta полезна как донор разделения сохранённых сообщений и текущего model context, блочной памяти и её представления. Не замена SQLite authority или нашему loop. В pin есть agent.message_ids и conversation_messages(conversation_id,message_id,position,in_context). Сами сообщения хранятся отдельно. Compaction меняет membership, сохраняет summary message со статистикой, не удаляет сообщения в рамках этого прохода. Это не обещание append-only всей БД: другие API обновляют/удаляют сообщения и системный prompt.

Конкретные source: letta/orm/{conversation_messages,message,agent,block,block_history}.py; services/summarizer/{compact,summarizer_sliding_window}.py; schemas/memory.py; agents/letta_agent_v3.py; groups/sleeptime_multi_agent_v4.py; data_sources/redis_client.py. Подробный source index: local://letta-state-research.md.

## Полезные механизмы

- Membership/order проекции отдельно от message payload: ветки/проекции могут ссылаться на одни сообщения без копирования текста.
- Summary — сохраняемый объект с mode/input range/statistics, а не молчаливое изменение архива. Donor token threshold/window defaults не переносятся как наши defaults.
- Блок памяти имеет label/description/value и версию; renderer включает выбранные блоки и metadata. Формат XML не выбран автоматически.
- BlockHistory различает actor user/agent. Это не то же, что content origin stated/inferred: агент может записать прямое указание пользователя. Оба измерения нужны для D53.
- Skills metadata/name/location перед полным текстом: пригодная lazy presentation идея, но формат/пути остаются нашим контрактом D52.
- Tool return truncation обозначена явно; у нас большие результаты также artifacts по D43.
- Проверка уже сохранённого tool result по call ID полезна для дубликатов доставки, но не предотвращает неизвестный эффект до сохранения результата.

## Что не переносить как готовую гарантию

В inspected path step messages сохраняются на checkpoint; intent до tool effect не найден. Значит это не донор нашего D47 recovery. Sleeptime watermark обновляется до safe_create_task обработки: in-memory task не является durable queue. Сообщения agent-agent в изученном tools path идут HTTP SDK, не подтверждают durable delivery. При отсутствии Redis часть lock/dedup methods — no-op; runtime не должен молча выключать своё ownership/delivery consistency.

messages FK к agent имеет CASCADE; удаление runtime handle/остановка worker не должно автоматически удалять history по нашему контракту retention. Саму Letta lifecycle schema не переносить без изменения ownership.

Git-memory объявляет Git source of truth, БД — кэш; это не наша authority session architecture. Per-edit git commit/reset path не нужен для D53, новый Git mirror не добавляем. PostgreSQL/Redis/Temporal/Turbopuffer/Python libraries не переносим как обязательный runtime ради алгоритмов.

Provenance derived memory в inspected archival_passages не имеет готового source-message/derivation graph. Metadata actor недостаточно для edit/delete propagation. SQLite search paths в pin используют LIKE и Python cosine UDF; это не evidence native ANN/FTS5 или качества поиска.

## Предлагаемая интеграция parent

Пользоваться идеей membership projection и сохраняемого summary совместно с уже выбранными OMP branch/context builder и original compaction. Не вводить параллельный Letta-owned store. Source messages и effective instruction/config versions остаются восстанавливаемыми; один hash не заменяет нужные для replay данные.

Compaction output связывать с выбранной веткой, исходной revision/input range и методом. Если активная проекция изменилась за время LLM summary, не устанавливать результат поверх новой ветки; сохранить результат и повторно проверить применимость. Внешняя модель делает summary вероятностным; provenance не превращает его в детерминированную реконструкцию. Summary-from-summary может накапливать потери, но запрет этого метода не принят: выбор алгоритма остаётся по D18, архив сохраняется.

Memory records имеют scope, source references/revisions, actor и origin. Delete/edit меняют текущую запись и инвалидируют соответствующие retrieval projections; старые history copies остаются по D53. Если автоматическое извлечение повторно видит старый source, правила suppression/re-extraction требуют явного дизайна: нельзя обещать вечное удаление только удалением индексной строки. Это implementation question, не утверждённая новая privacy guarantee.

Background extraction получает durable work item и фиксирует processing progress после коммита результата, не до dispatch. Новая pending запись не создаёт права на выполнение; extraction policies/budgets и model selection сохраняются.

## Проверки и решения

Нужны разрешённые runnable checks: branch change во время compaction; архив после нескольких summaries; actor vs inferred origin; edit/delete затем delayed extraction; crash до/после background result; duplicate delivery; остановка агента без потери session history. Не выполнены.

Нового обязательного сервиса/формата/алгоритма не принимаем. Letta дополняет уже изученный OMP механизмами, не заменяет его. Противоречия доноров решаются адаптацией, не вопросами о SQL columns пользователю. Лицензии копируемых файлов/NOTICE/transitive closure проверяются по фактической поставке, не общему Apache label.

## Первичные источники

- https://github.com/letta-ai/letta/tree/1131535716e8a31c9a437f8695e25ac98f203a24
- https://raw.githubusercontent.com/letta-ai/letta/1131535716e8a31c9a437f8695e25ac98f203a24/letta/orm/conversation_messages.py
- https://raw.githubusercontent.com/letta-ai/letta/1131535716e8a31c9a437f8695e25ac98f203a24/letta/services/summarizer/compact.py
- https://raw.githubusercontent.com/letta-ai/letta/1131535716e8a31c9a437f8695e25ac98f203a24/letta/orm/block_history.py
- https://raw.githubusercontent.com/letta-ai/letta/1131535716e8a31c9a437f8695e25ac98f203a24/letta/groups/sleeptime_multi_agent_v4.py
- https://raw.githubusercontent.com/letta-ai/letta/1131535716e8a31c9a437f8695e25ac98f203a24/letta/data_sources/redis_client.py
