# Сжатие контекста оригинального OMP

Исследование 2026-11-05. Только оригинальный can1357/oh-my-pi на 035e4f95c314f9aff2f5a779fa335422680f8e36; не Rust-форк. Документация сопоставлена с исходниками двумя scout-агентами. Запуск и измерения не проводились.

## Общая модель

OMP различает журнал/транскрипт, активный контекст модели и provider-native replay. Compaction добавляет запись с summary, boundary и preserveData, затем перестраивает активный контекст; это не новая сессия. Видимый transcript может сохраняться, хотя вход модели уже сокращён. Некоторые pruning/rescue операции переписывают или отбрасывают данные: нельзя обещать безусловно полный неизменяемый архив любого исходного блока.

Источники: [compaction documentation](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/compaction.md), [session-context.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/session/session-context.ts), [session-maintenance.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/session/session-maintenance.ts).

## Методы и порядок

Default preference list: remote → snapcompact → handoff → shake → soft. Это конфигурируемый dispatcher, не обязательная последовательность выполнения всех методов. Пропускаются недоступные методы; результат должен дать сокращение и пригодный бюджет, иначе fallback/no-progress handling. Pruning до maintenance — отдельный механизм.

| Метод | Реализация | Зависимость и потеря информации |
|---|---|---|
| remote | Provider-native compaction, либо настроенный endpoint | Возможности provider/API; opaque replacement history не универсальный summary |
| snapcompact | Локальная сериализация истории в текстовые края и PNG-середину | Рендер локален, чтение требует vision-модели; truncation/foveation и восприятие модели не lossless |
| handoff | Структурированный рабочий документ через side LLM request | Текстовое резюме с потерями; сохраняет свежий хвост, не передаёт владельца пользовательской задачи |
| shake | Механическое вынесение крупных подходящих блоков в recoverable artifacts | Сокращает активный контекст; агенту может потребоваться перечитать источник |
| soft | LLM-summary старой истории с сохранением свежего хвоста | Модельный вызов и семантические потери |

Первичный dispatcher: [compaction-methods.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/session/compaction-methods.ts). Движок: [compaction.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/compaction/compaction.ts), [shake.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/compaction/shake.ts).

## Когда запускается

Документация и maintenance описывают ручной запуск, threshold после turn, safe mid-turn boundary, overflow recovery, incomplete-output recovery и optional idle maintenance. Эти причины имеют разные правила: handoff generation пропускается на input overflow; snapcompact требует image input и не подходит для media/byte-shaped rejection. HTTP 413 не следует автоматически считать token overflow. Output-token limit тоже не всегда лечится сжатием входа.

Threshold учитывает окно модели, reserve, fixed/percent overrides и оценку реально сохранённой истории; provider usage не единственный источник. Safe cut не начинается на toolResult; preparation сохраняет call/result структуру и учитывает reset boundaries. Размер свежего хвоста адаптируется под бюджет. Ни настройки, ни оценки токенов не доказывают provider-billed размер.

## Pruning отдельно от summary

[pruning.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/compaction/pruning.ts): старые крупные tool outputs, superseded reads и flagged useless results сокращаются по разным правилам. Есть protected tools/skill reads, minimum savings и cache-aware timing. Замена result placeholder сохраняет pairing, но содержание исходного блока в текущей истории может исчезнуть. Для своего продукта полный raw artifact следует сохранять отдельно до destructive projection, если обещается полный архив.

## Спекулятивное сжатие

`maybeStartSpeculativeCompaction`, `#runSpeculation`, `#armedSpeculationValid`, `#claimArmedSpeculation` готовят remote/handoff/soft по snapshot в предпороговой зоне. Lead в этом снимке — clamp(12.5% threshold, 8192, 32000). Это implementation constants, не утверждённые defaults нового агента.

Готовый результат применяется только при пригодности snapshot и provider replay; последующие сообщения учитываются отдельно. Branch navigation, reset/new compaction или несовместимый replay делают результат stale. Есть cancellation и blocking fallback. Shake/snapcompact локальны и не выбираются для фонового LLM generation. Быстрый commit возможен, когда результат уже готов; обещание нулевой задержки неверно.

Источники: [maintenance](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/session/session-maintenance.ts), [speculation-lead.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/session/speculation-lead.ts).

## Handoff

[SessionHandoff](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/session/session-handoff.ts) использует live side-request transforms/cache routing. `generateHandoffFromContext` начинает с toolChoice:none, допускает compatibility retry auto; returned tool calls не исполняются, извлекается только текст. Commit остаётся на текущей сессии и хранит boundary/details/method. Это compaction handoff, не multiagent handoff управления.

Abort и transient retry есть, но общий hard deadline нельзя считать доказанным для всех генераций. Текстовый результат не проходит полноценную проверку сохранности всех обязательств: качественный summary остаётся модельной задачей. Источник: [handoff pipeline](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/handoff-generation-pipeline.md).

## Snapcompact

[snapcompact.ts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/snapcompact/src/snapcompact.ts): сериализатор ограничивает tool result и аргументы, затем renderer формирует model-aware frames. Archive хранит bounded source text и frames; повторное сжатие рендерит source text заново, а не бесконечно переносит старые PNG. Frame/image/request budgets могут удалять старые frames. Поэтому lossless PNG-кодек не означает lossless context compression.

Рендер не требует сети/API key; польза требует пригодного vision backend и оценки качества/стоимости. Конкретные shape/billing правила быстро зависят от моделей и endpoint, не должны попадать в универсальное ядро как вечные константы.

## Ещё один режим: notes-backed windows

В документации этого снимка есть opt-in experimentalContextManagement: model-managed notebook, локальный rollover без LLM-summary и поиск полного текущего branch history. Требует context_notes/new_context/read/grep, сохраняет recent call/result units. Это экспериментальный отдельный путь; детали runtime в данном проходе полностью не трассированы. Нельзя объявлять его гарантированно лучшим или восстанавливающим ранее удалённые данные.

## Перенос в собственный Rust-агент

Предложение, ещё не принято как точная спецификация:

- Отдельный ContextManager со стратегиями и причиной запуска, не один summarizer.
- Raw history/artifacts отдельно от model projection; восстановимость и retention формулировать явно.
- Локальные pruning/shake, текстовые summary/handoff, native provider compaction и vision archive — независимые configurable backends.
- Предпороговая подготовка с snapshot identity, stale invalidation, bounded retry/deadline и проверкой реального прогресса.
- Смена модели проверяет пригодность replay/media и перестраивает projection; opaque provider state не считать универсальным.
- Notes-backed rollover рассматривать отдельной опцией после source/eval проверки.
- Не наследовать upstream preference order/threshold constants без проверки своего corpus и providers.

## Проверка

Исследованы исходники и docs; никакие методы не запускались, recall/стоимость/latency не измерялись. Проверка артефакта: `test -s docs/omp-compaction-research-2026-11-05.md` из checkout.
