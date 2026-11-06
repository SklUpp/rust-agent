# Сетевые доноры и перенос компонентов на Rust

Дата: 2026-11-05. Новое условие: разрешено переносить части исходных проектов на Rust внутри нового агента. Исследование охватывает релевантные классы компонентов в сети, не только OMP. Не означает просмотр всего интернета. Реализация и установки не выполнялись.

## Пересмотр основного вывода

**Отсутствие готовой Rust-библиотеки больше не причина отклонить функцию.** Различать четыре решения: adopt готовую библиотеку; extract модуль; port конкретный алгоритм/контракт; оставить внешним сервисом. Отказ от полного продукта не равен отказу от полезных частей.

Рекомендация: собственный переносимый harness с заимствованными Rust engines, выбранными gateway dialects и адаптированными контекстными механизмами. Первым сравнивается [оригинал OMP на 035e4f95](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36), затем [Rust-форк SklUpp на 3008cfd](https://github.com/SklUpp/omp-rust-termux/tree/3008cfd0ca88bb0e28323561a165f39efa7159e1). Переносить код заново только при несовпадении существующей реализации с нужным контрактом.

## Граница подтверждения

«Да к переносу» ниже — технически обоснованный выбор алгоритма или API, не готовый порт и не гарантия Android. Новые кандидаты с `main/master/latest` не закреплены навсегда; перед заимствованием фиксировать revision и лицензии выбранных файлов. Старые отчёты отвечали на вопрос о готовых интеграциях; этот документ расширяет их, не превращая прежнее «нет готовой библиотеке» в «невозможно».

Сетевые агенты отдельно исследовали OMP, 9router, контекст/память, другие harness, инфраструктуру и риски перевода языков. При синтезе исключены неподтверждённые утверждения о платформенной невозможности, размере/скорости, архивировании проектов и обязательности Docker/Java. Прочитанный manifest подтверждает зависимости/API, не runtime-совместимость.

## 1. Оригинальный OMP: перенос поведения, не монорепозитория

| Подсистема | Решение | Контракт, который сохранять |
|---|---|---|
| Agent loop/recovery | Взять Rust-форк, сравнить с original; портировать отсутствующие нужные ветки | Stop/error/abort/refusal/length различаются; частичный JSON не запускается; устойчивые tool IDs и call/result pairing |
| Session tree | Портировать форматы или сделать явный import/export adapter | JSONL v3, id/parentId/leaf, compaction/reset/branch; transcript не равен provider context |
| Approvals | Перенести policy evaluator, но дать собственный host UI/lifecycle | Tool tier → user policy → mode → restrictions; caller ceiling независимо от режима |
| Hooks/extensions | Перенести смысл вызовов, не произвольный Bun loader | Block до эффекта, error остаётся error; мутация args требует повторной проверки approval; prompt и automation имеют разные deadline |
| Subagents | Переиспользовать Rust supervisor и сверить с upstream lifecycle | Владение процессами, mailbox/park/revive/cancel, cost bounds, stale result revision |
| Context/cache | Портировать выбранные стратегии | Сохранять signed/native blocks, cache/account identity, replay boundary и полный архив |
| Native tools | Брать оригинальные Rust crates напрямую | pi-edit host writer, pi-ast без ненужного WASM, выбранные VFS/shell/VCS/builtins features |

Первичные контракты: [agent loop](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/agent-loop.ts), [replay policy](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/replay-policy.ts), [approval](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/tools/approval.ts), [session entries](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/session/session-entries.ts), [hook wrapper](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/extensibility/hooks/tool-wrapper.ts). Подробные Rust seams: [original-only исследование](omp-original-foundation-research-2026-11-05.md).

Не требуется сохранять все upstream retry constants, роли и таймеры как продуктовые значения. Если цель wire parity — значения входят в отдельный compatibility profile; для собственного поведения они выбираются по требованиям и измерениям.

## 2. 9router: встроенный Rust gateway теперь допустим

Снимок [1fe586bb](https://github.com/decolua/9router/tree/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d). Root MIT; проверить зависимости и provider conditions. Ранее предпочтительный внешний сервис остаётся рабочим вариантом, но больше не единственным.

| Форма | Что переносить | Что исключить или оставить снаружи |
|---|---|---|
| Dialect core | OpenAI/Anthropic/Gemini request/response transforms, model mappings, incremental stream assembly, ошибки | Dashboard, аккаунт-пулы, web-provider reverse integrations, все combos |
| Выбранные провайдеры | Dialect core плюс exact provider headers/quirks, account locks, ограниченный fallback и refresh | Неподдержанные аккаунты/схемы не имитировать; один владелец refresh |
| Gateway parity | Account selection, combos/cooldown, affinity, usage/storage, management/auth | Next UI и launcher не обязательно портировать; новый control plane отдельный контракт |

Конкретные исходники для порта:
- [translator/index.js](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/translator/index.js): register/translateRequest/translateResponse/initState, direct pair или pivot через OpenAI; thinking/tool-name/tool-ID обработка. Pivot может терять provider-specific поля: сохранять native path там, где он нужен, а не нормализовать всё безусловно.
- [providerModels.js](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/config/providerModels.js): supported formats, upstream IDs, strip rules. Это конфигурационные данные с версией, не вечная истина о провайдере.
- [accountFallback.js](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/services/accountFallback.js): error classification, quota cooldown, account/model locks. Request-scoped ошибки не должны безусловно выводить аккаунт из обращения.
- [combo.js](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/services/combo.js): rotation, capability reordering, empty-stream failover. Состояние переносится из global Map в владельца gateway.
- [base executor](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/executors/base.js), [streamHandler](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/utils/streamHandler.js): HTTP/timeouts/disconnect/backpressure, не Node streams буквально.
- [app auth](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/src/sse/services/auth.js), [refresh](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/services/tokenRefresh.js): selection mutex, account filters и обновление секретов. Перенести policy, а не импорты Next storage.

**Рекомендация:** встроенный dialect/provider срез — да; полный gateway переносить только если его функции обязательны продукту. Не копировать все таймауты/лимиты без оценки. Web/CLI reverse executors не запрещены языком Rust, но их изменчивость и условия доступа сохраняются после переписывания.

## 3. Другие gateway и provider доноры

| Кандидат | Полезное переиспользование | Граница решения |
|---|---|---|
| [litellm-rs](https://github.com/majiayu000/litellm-rs) | Рассмотреть готовый Rust kernel вместо собственного порта всего 9router | README различает runtime-backed API и legacy adapters; sticky sessions пока reserved, не принимать как реализованную affinity |
| [LiteLLM](https://github.com/BerriAI/litellm) | Provider quirks, catalog и fixtures для сравнения с собственным gateway | Проверять именно открытый core и условия enterprise файлов; Python runtime можно не включать |
| [TensorZero](https://github.com/tensorzero/tensorzero) | Исследовать routing/eval/feedback model и нужные Rust модули | Не объявлять весь gateway обязательным; runtime/source API выбранного модуля требует отдельного принятия |
| [Rig](https://github.com/0xPlaygrounds/rig) | Выбранные provider adapters и типы | Не надстраивать второй retry/loop поверх уже выбранного владельца |
| [Goose providers](https://github.com/aaif-goose/goose/blob/main/crates/goose-providers/src/lib.rs) | Уже разделённые Rust modules и canonical/cache/retry/provider types | lib.rs подтверждает seams, не все реализации и не Android readiness |

Новый значимый факт: [goose-sdk 0.1.0-alpha.13](https://docs.rs/goose-sdk/0.1.0-alpha.13/goose_sdk/) с feature `uniffi` предоставляет Python/Kotlin in-process API для declarative providers и streaming completions. Это **не** доказательство экспорта всего agent lifecycle через FFI. [Feature graph](https://docs.rs/crate/goose-sdk/0.1.0-alpha.13/features) позволяет оценить зависимости до выбора.

## 4. Repo map и альтернативные harness

**Aider — конкретный кандидат port, а не просто идея.** [repomap.py](https://github.com/Aider-AI/aider/blob/main/aider/repomap.py), [Apache-2.0](https://github.com/Aider-AI/aider/blob/main/LICENSE.txt): tree-sitter tags, mtime cache/fallback, token-budgeted карта; `get_ranked_tags` строит weighted MultiDiGraph с def/ref и персонализацией по chat/mentioned files/identifiers. Это синтаксическая эвристика, не семантический call graph. В Rust использовать уже выбранные parser/cache/graph средства; не тащить Python/diskcache/networkx ради одной функции. Не повторять автоматически удаление cache directory из recovery пути донора.

Проверять ranking на фиксированном corpus и token budget, стабилизировать tie-breaks, отделить улучшение алгоритма от exact parity. Mtime-only freshness можно заменить content hash, но это объявленное изменение, а не побайтово эквивалентный порт.

**mini-swe-agent:** [default.py](https://github.com/SWE-agent/mini-swe-agent/blob/main/src/minisweagent/agents/default.py), [MIT](https://github.com/SWE-agent/mini-swe-agent/blob/main/LICENSE.md). `DefaultAgent.run/step/query/execute_actions/serialize` — простой контрольный loop; trajectory `mini-swe-agent-1.1` сохраняет messages, model stats, config и exit status. Полезен как baseline и формат evaluation adapter, а не обязательный production loop. model/environment API отдельно адаптировать; наличие внешней env не доказывает sandbox.

**Codex:** уже Rust, [Apache-2.0](https://github.com/openai/codex/blob/main/LICENSE). [linux-sandbox/lib.rs](https://github.com/openai/codex/blob/main/codex-rs/linux-sandbox/src/lib.rs) экспортирует Linux-only no_new_privs/seccomp/bwrap paths и на другой ОС не является готовым backend. Рассматривать его policy/exec boundaries и fixtures; **не объявлять Linux sandbox готовой Android-защитой** и не переносить весь модуль «как есть» по одному lib.rs.

**OpenCode, Cline, pi-mono и OpenHands condenser** получили узкую source-проверку в заключительных разделах ниже. **SWE-agent** остаётся discovery candidate; mini-swe-agent проверен отдельно. Source-проверка конкретного модуля не означает runtime readiness или включение всего проекта в обязательный набор. OpenCode ниже цитируется под каноническим `anomalyco/opencode` (прежнее имя `sst/opencode`); pi-mono перенаправляется в `earendil-works/pi`.

## 5. Контекст, память и инструменты

| Донор | Переносимый срез | Условие |
|---|---|---|
| OMP local/autolearn | Извлечение lessons, consolidation, scoped summaries и managed skills | LLM-качество проверять отдельно от файловой корректности; provenance/delete/lineage |
| OMP Mnemopi | SQLite записи, retrieval API, scope, FTS/embedding strategy | Не утверждать, что Rust-форк уже имеет полный backend |
| Sharpshooter | Decision files и scheduler/extraction | Не переносить все таймеры и token limits без mobile budget |
| Headroom | Выбранный compression/retrieval pipeline и archive references | Проверить конкретный проект, лицензию, изменение payload и сохранение originals |
| Snapcompact | Text archive + model-aware image rendering strategy | Vision gate, качество и стоимость модели; не универсальное lossless-сжатие |
| RTK OSS | Выделить нужные Rust filters в свой модуль | Это extract уже Rust-кода, а не переписывание; лицензия конкретной редакции и notices сохраняются |
| Dynamic skills/tools | Schema visibility и lazy loading, host registry/policy | Видимость инструмента не выдаёт полномочия; отсутствие JS loader — новая plugin модель |

Источники: [OMP memory docs](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/memory.md), [Mnemopi source](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/mnemopi), [autolearn](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/autolearn), [Headroom](https://github.com/headroomlabs-ai/headroom), [RTK editions](https://www.rtk-ai.app/docs/resources/editions/), [snapcompact](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/snapcompact).

**Не путать Headroom проекты:** headroomlabs-ai/headroom содержит несколько языковых частей, root Apache-2.0; DarkPhilosophy/omp-headroom — TS plugin под [GPL-3.0](https://github.com/DarkPhilosophy/omp-headroom/blob/main/LICENSE). Порт GPL-кода на Rust не превращает его в MIT. Новое ограничение требует либо соблюдения лицензии, либо другого источника/самостоятельной реализации, не косметического изменения языка.

Fork gap: Rust local_memory/managed_skills покрывает ограниченные capture/read/guards; из этого нельзя вывести наличие полного Mnemopi, Hindsight, Sharpshooter или двухфазной LLM-консолидации. Эти блоки остаются кандидатами отдельного переноса.

## 6. Инфраструктура из сети: библиотека против алгоритма

- [Tantivy schema](https://github.com/quickwit-oss/tantivy/blob/main/src/schema/mod.rs), MIT: реальная embedded Rust API schema/indexing; кандидат lexical/BM25 поиска при доказанной потребности сверх SQLite FTS. Android readiness не выводится из pure Rust.
- [tree-sitter binding](https://github.com/tree-sitter/tree-sitter/blob/master/lib/binding_rust/lib.rs), MIT: Language/Tree/Range/Point и version compatibility; брать готовым через OMP или прямо, не портировать повторно.
- [ast-grep-core](https://github.com/ast-grep/ast-grep), MIT: кандидат структурного matching; сравнить с уже используемым pi-ast, не включать дублирующий слой без причины.
- [Temporal](https://github.com/temporalio/temporal), MIT: reference event history/retry/replay; сервер не нужен локальному ядру по умолчанию. Это направление заимствования алгоритмов, не подтверждённый перенос его durable semantics простым enum+SQLite.
- [DBOS Rust](https://github.com/dbos-inc/dbos-transact-rust): workspace содержит library/macro crates; candidate при внешнем durable workflow сценарии. Прочтение Cargo.toml недостаточно для принятия runtime/storage API.
- [Restate](https://github.com/restatedev/restate): серверная лицензия BSL требует проверки конкретного текста/версии; Rust SDK и server — разные компоненты. Не выбирать embed без лицензионного и source-аудита. Прежнее предположение о Java runtime исключено как неподтверждённое.
- [Qdrant API](https://github.com/qdrant/qdrant/blob/master/lib/api/src/grpc/mod.rs), Apache-2.0: подтверждён API слой; клиент/удалённый service опционален. Ни Docker, ни собственный vector server не обязательны локальному агенту.
- [Mem0](https://github.com/mem0ai/mem0), Apache-2.0, и [cognee-rs](https://github.com/topoteretes/cognee-rs): discovery retrieval/memory candidates; не включать без аудита update/delete/provenance и фактических feature dependencies.

Это приоритизация, не запрет остальных проектов. Отсутствие read-only source validation не замещается положительным verdict.

## 7. Android/UI доноры, найденные дополнительно

[goose-phone-app](https://github.com/PhillipChaffee/goose-phone-app) — Rust/Dioxus remote client, не автономное локальное ядро. [goose-acp-client manifest](https://github.com/PhillipChaffee/goose-phone-app/blob/main/crates/goose-acp-client/Cargo.toml) указывает MIT и UI-независимые Tokio/WebSocket/rustls зависимости; [lib.rs](https://github.com/PhillipChaffee/goose-phone-app/blob/main/crates/goose-acp-client/src/lib.rs) экспортирует AcpClient/ConnectConfig/types и отделяет Goose-specific namespace. Это конкретный donor transport/reconnect/UI model, не доказательство локального exec или готовности своего APK.

Kotlin/Compose остаётся допустимым host; Dioxus — альтернатива UI, не обязательная замена. Goose SDK UniFFI seam выше — дополнительный пример FFI, а не reason тащить весь Goose.

## 8. Как доказывать перенос, не копировать скрытые ошибки

Определить contract profile: собственная функциональная совместимость либо точный upstream wire parity. Для первого допускаются объявленные улучшения; для второго нельзя незаметно менять error classification, fields, token limits и ordering.

Differential corpus сравнивает **наблюдаемое поведение**, а не весь nondeterministic transcript: request bodies/headers без секретов, canonical blocks, stop/error, tool IDs, approvals, state transitions и stored lineage. Случайные ID/время нормализуются только там, где не влияют на полномочия. Для numeric/token ranking определить tolerance или deterministic tie-break, не требовать произвольной byte identity.

Обязательные группы: fragmented UTF-8/SSE/JSON; tool args после length; multi-call ordering; cancellation на каждом await; disconnect после начала ответа; 401/429/refresh race; request-scoped 400; redirect/endpoint change без утечки credentials; crash между эффектом и durable result; unknown outcome без слепого retry; path/symlink/CRLF/BOM; stale index; memory invalidation и hooks changing approved args. Исходный проект — reference, не oracle безопасности: известный дефект оформляется как намеренное divergence и собственный regression case.

## Итоговый вердикт

**Да:** расширить архитектуру от подключения готовых сервисов к выборочным Rust-портам. Предметные кандидаты — OMP harness gaps, 9router dialect/provider core, Aider repo-map, scoped memory/learning, compression, OpenHands condenser и выбранные OpenCode/Cline/pi-mono контракты. Уже Rust-компоненты — выделять, а не переписывать.

**Нет:** переписывать все найденные продукты целиком ради единого языка; считать такой rewrite автоматически быстрее, безопаснее, совместимее или пригодным для Android; обходить лицензии сменой языка.

Проверка документа из checkout: `test -s docs/rust-port-reuse-research-2026-11-05.md`. Исследование не создавало порт или prototype; преимущества latency/RAM/размера и APK-ready статус не измерены.

## Дополнительные ловушки перевода языков

- JS UTF-16 indexing, Python code points и Rust UTF-8 byte offsets — разные величины. Задать единицы каждой длины/позиции; не нормализовать Unicode или обрезать по grapheme там, где wire требует байтов. Для UI и wire нужны разные проверки. [Rust String](https://doc.rust-lang.org/std/string/struct.String.html).
- JS Number, Python arbitrary-precision int и Rust typed integers требуют явной политики IDs/overflow/float. `serde_json::Number` различает представления; не считать ошибкой любое отличие от исходной потери точности и не канонизировать provider payload самовольно. [serde_json Number](https://docs.rs/serde_json/latest/serde_json/struct.Number.html).
- `regex` не поддерживает произвольные backreferences/look-around; Unicode classes и флаги отличаются. Инвентаризировать паттерны redaction/policy/compression; неподдерживаемый шаблон не должен молча отключать защиту. [regex syntax](https://docs.rs/regex/latest/regex/#syntax).
- Отсутствующее поле, null, пустое значение и unknown field не взаимозаменяемы. Порядок сериализации важен только там, где он действительно часть протокола/подписи; provider-specific signing следует его спецификации, не универсальной схеме sorted-keys+NFC.
- SSE parser должен переживать произвольную фрагментацию UTF-8, строки CRLF и multiline data; LLM POST/SSE не автоматически использует browser EventSource reconnect. [SSE specification](https://html.spec.whatwg.org/multipage/server-sent-events.html).
- HTTP redirects/retries требуют отдельного теста headers/body/method; не наследовать неявно Node fetch defaults. [Fetch standard](https://fetch.spec.whatwg.org/). Refresh single-owner и idempotency — инварианты независимо от языка.
- Rust rewrite может убрать Node/Bun/Python runtime **из выбранного среза**, но не отменяет Android W^X/background/SAF и не устраняет C/FFI или model resources. JS plugins сохраняют потребность в JS host, если их совместимость требуется; порт gateway позволяет убрать 9router sidecar, а не навсегда делает его обязательным.
- Перевод кода не снимает авторские/лицензионные условия. [FSF о переводе языка](https://www.gnu.org/licenses/gpl-faq.html#TranslateCode) объясняет это для GPL; применимый текст каждой другой лицензии проверяется отдельно. Это не юридическое заключение.

Не приняты как универсальные правила из исследовательских черновиков: сравнивать весь JSON побайтово независимо от контракта; обязательная NFC-нормализация подписанного payload; любой mismatch считать багом вместо объявленного исправления; трактовать `spawn_blocking.abort` как остановку closure; переносить локальные PROroot/targetSdk решения на все Android-продукты.


## Углубление дополнительных source-кандидатов

**OpenHands software-agent-sdk:** [condenser/base.py](https://github.com/OpenHands/software-agent-sdk/blob/main/openhands-sdk/openhands/sdk/context/condenser/base.py), [LICENSE](https://github.com/OpenHands/software-agent-sdk/blob/main/LICENSE). `CondenserBase` возвращает View/Condensation; `RollingCondenser` различает SOFT/HARD потребность, отсутствие результата при SOFT сохраняет view, при HARD требует hard reset или явную ошибку. **Port:** собственный strategy trait и типизированный результат, не инфраструктуру приложения. Summary вызывается через выбранный LLM backend, локальный или удалённый; отдельный сервер логически не обязателен. Сохранить original events и связи забытых IDs, а не только текст summary.

**Mem0:** [memory/main.py](https://github.com/mem0ai/mem0/blob/main/mem0/memory/main.py), [Apache-2.0](https://github.com/mem0ai/mem0/blob/main/LICENSE). `_search_vector_store` объединяет semantic/keyword/entity scoring; `_update_memory` не разрешает незаметно заменить identity fields, пишет history и обновляет entity links; `_delete_memory/delete_all` обслуживают связанные данные и ограничивают повторяющиеся batch loops. **Port:** scope invariants, version/history и явный fusion ranker; не копировать коэффициенты и MD5 как универсальные решения безопасности. Vector/embedding/entity backends могут быть локальными или удалёнными: наличие интерфейса не требует Qdrant-сервера, но отсутствие entity backend означает иной контракт retrieval. Удаление raw record не доказывает удаления влияния из summaries/skills.

**Restate journal:** [entries.rs](https://github.com/restatedev/restate/blob/main/crates/types/src/journal/entries.rs) содержит Input/Output/state/Call/Run/Sleep/Cancel и completion types. Полезен как reference vocabulary для собственных pending/completed operations. Но enum не реализует crash-safe replay или exactly-once effect: это требует runtime/protocol и downstream semantics. Код server под [BSL](https://github.com/restatedev/restate/blob/main/LICENSE), [Rust SDK](https://github.com/restatedev/sdk-rust) лицензируется отдельно. **Не переносить server source под видом свободной MIT-идеи:** либо соблюдать его условия, либо проектировать собственный алгоритм по требованиям, либо использовать подходящий SDK/сервис. Даже сервис не делает произвольный external side effect exactly-once без его контракта.


## Проверенные дополнительные harness-модули

Ранее discovery-only кандидаты получили узкую source-проверку; это уточняет раздел 4, не означает аудит всего проекта.

| Проект | Конкретный источник | Полезный перенос и ограничение |
|---|---|---|
| OpenCode | [compaction.ts на b2baddcd](https://github.com/anomalyco/opencode/blob/b2baddcd/packages/opencode/src/session/compaction.ts), [MIT LICENSE](https://github.com/anomalyco/opencode/blob/b2baddcd/LICENSE) | `select/splitTurn/buildPrompt/prune`: сохранять последние turns и бюджетированный хвост, объединять предыдущий summary и актуальные constraints. Перенести алгоритм выбора, не Effect/Bus/Plugin runtime и не все token constants |
| Cline | [AutoApprove](https://github.com/cline/cline/blob/9dea336c/src/core/task/tools/autoApprove.ts), [CheckpointTracker](https://github.com/cline/cline/blob/9dea336c/src/integrations/checkpoints/CheckpointTracker.ts), [Apache-2.0](https://github.com/cline/cline/blob/9dea336c/LICENSE) | Категории tool policy и обновление разрешений в живой задаче; shadow Git lifecycle — reference для сравнения, не готовый transplant. Свои безопасные defaults, не заимствование `useMcp:true` как blanket approval |
| pi-mono / earendil-works/pi | [agent-loop.ts на 4bb837c9](https://github.com/earendil-works/pi/blob/4bb837c960295478224e9879fcd94ac0a09db5ab/packages/agent/src/agent-loop.ts), [types.ts](https://github.com/earendil-works/pi/blob/4bb837c960295478224e9879fcd94ac0a09db5ab/packages/agent/src/types.ts), [MIT](https://github.com/earendil-works/pi/blob/4bb837c960295478224e9879fcd94ac0a09db5ab/LICENSE) | Инъекции transformContext/convertToLlm, steering/follow-up, before/after tool; различать порядок completion events и порядок результатов в model history. Хороший компактный reference, не обязательный второй loop рядом с OMP |

Вариант собственного loop: сохранить лишь необходимые точки расширения в Rust contract, а не копировать всю динамическую систему. Блокировка, отмена, ошибки и порядок результатов должны быть явными; слово «non-throw» из JS-контракта не значит игнорировать ошибку Rust hook. CheckpointTracker не отклоняется потому, что он TypeScript: исключается **полный transplant**, а полезные операции и тестовые случаи остаются кандидатами.

