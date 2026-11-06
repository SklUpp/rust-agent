# Provider/MCP: контракт интеграции

Статус: предложение реализации, не новые accepted defaults. Основа D20/D27/D30/D39/D41/D42/D54/D80. Source evidence: [provider/MCP research](research-2026-11-06-providers-mcp.md), [core extraction](agent-core-extraction-design.md). Runtime/build проверки не выполнялись.

## Назначение и разрез

Сохраняем Rust ModelProvider/StreamAssembler и первый OpenAI-compatible adapter внутри существующего core boundary; конкретный HTTP adapter отдельным внутренним модулем, без второго loop. MCP adapter использует официальный rmcp и связывает его lifecycle с product runtime. Полнота SDK не означает полноты host implementation.

## Provider flow

Runtime формирует model projection выбранной ветки. Persisted message representation и wire DTO различаются: конверсия не изменяет archive. Tool IDs, signed/opaque/reasoning blocks и provider/model routing metadata сохраняются, даже если текущий маршрут не может их отправить. Правило переноса между несовместимыми маршрутами требует отдельного решения, не silent drop.

Ключ разрешается trusted adapter из явно настроенной env-переменной. Строка имени отсутствующей переменной не используется как literal API key по донорскому fallback. Base URL и выбранный dialect определяют endpoint; /chat/completions форка — donor fact, не универсальный контракт OpenAI-compatible.

HTTP status/body error обрабатывается до SSE parse; diagnostic payload ограничивается и защищается от секретов. Error normalization не навязывается: разрешено сохранить provider text с безопасной обработкой. Transport error, invalid stream и корректный model finish не объединяются в один success.

Assembler собирает delta по provider IDs/index, а не arrival text concat: fragmented tool names/args, несколько content blocks, usage snapshots и finish states различаются. Незавершённый stream сохраняет доступное partial content, но не разрешает запуск неполного tool call. Как показать partial content в UI — frontend detail.

Retry не выбирается здесь. Если повтор разрешён, попытка имеет собственное состояние; нельзя молча склеить начало первой генерации с концом другой или исполнить tool дважды. После опубликованного tool effect восстановление следует call-outcome contract, не provider retry. Backoff/Retry-After/timeouts/effort/token limits остаются до реализации адаптера.

Явные вложения передаются выбранному маршруту по D41; отсутствие capability flag не основание тихо отбросить данные, заменить текстом или сменить модель. Неподдержанный provider wire format возвращает явную ошибку; конкретные binary representations/vision routing открыты.

## MCP lifecycle и границы

Server config discovery не запускает процесс. До stdio spawn runtime проверяет trust/policy, разрешённые executable/args/env/cwd и budgets. Не копировать весь runtime env, не расширять права через project config. Tool-call policy проверяется также после negotiation/ожидания, не только один раз при connect.

SDK session/transport handles принадлежат runtime; адаптер публикует поддержанные capabilities и преобразует события в общий command/event contract. Конкретные protocol version/transports/auth/lifecycle defaults не объявляются согласованными этим документом.

Registry имена связываются с конкретным server identity и remote tool name, чтобы совпадения не приводили к вызову другого сервера. Public toolset отражает allowed selection; скрытый tool не вызывается по догаданному имени. Изменение advertised capabilities не переписывает прошлые call records.

Resources/prompts/images не превращаются безусловно в base64 strings. Adapter сохраняет типы, MIME и ссылки/embedded content по фактическому protocol contract, затем product решает включение в model context. Полученный prompt/resource остаётся недоверенным содержимым, не новым runtime authority.

Roots — описательное объявление контекста, не ACL. Видимость всех проектов D82 не означает автоматически объявить все проекты каждому MCP-серверу. Конкретный roots contract открыт до реализации; SDK не изолирует filesystem stdio child.

Server-initiated sampling/elicitation и другие запросы проходят соответствующие product policy/budgets; наличие обработчика в SDK не разрешает модельный запрос или действие. Не объявляем «полный MCP» до проверки выбранной matrix; fork tools-only implementation не годится как доказательство полноты.

Cancel запроса и фактическое завершение remote effect различаются. Обрыв соединения не доказывает отмену действия; unknown outcome сохраняется. Перезапуск server после ошибки не означает безопасного повтора tools/call. Reconnection/auth details остаются открытыми.

## Состав переносов

- Rust provider.rs: ModelProvider, chunks, StreamAssembler, partial finish/pairing; concrete HTTP в своём модуле.
- Original provider utilities: адресно port replay validation/content semantics; donor retry defaults не переносить как product defaults.
- rmcp: официальный protocol/client/transport слой; host callbacks/admission/history/resource storage писать в runtime, без отдельного framework loop.
- Fork registry naming helpers можно reuse без импорта всего MCP client в core registry.

## Проверки перед принятой реализацией

Throwaway provider server: early HTTP error, fragmented UTF-8/SSE/tool args, многоблочный ответ, EOF без завершения, partial content после ошибки, cancel, usage snapshot, повтор после частичного потока без смешивания попыток. MCP fixture: stdio spawn denied до запуска, names collision, negotiation, бинарный resource, error result, sampling/elicitation denied без разрешения, cancel/connection-loss unknown outcome. Реальный Linux/Termux transport smoke — после разрешения, тяжёлые сборки CI. Пока не выполнялись.
