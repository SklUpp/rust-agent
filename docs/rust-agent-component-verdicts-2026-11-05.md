# Составное Rust-ядро: решения по компонентам

Связанные исследования: [архитектура](rust-agent-research-2026-11-05.md), [отдельный 9router](9router-integration-research-2026-11-05.md). Дата: 2026-11-05.
**Расширенное условие:** разрешены частичные переносы сетевых проектов на Rust. [Новое исследование доноров и Rust-портов](rust-port-reuse-research-2026-11-05.md) дополняет таблицы ниже: «нет готовой библиотеке» не означает «нет переносу алгоритма».


## Что означает да/нет

**Да** — включить указанный способ интеграции в рассматриваемую архитектуру: первоисточник подтверждает нужный механизм. **Нет** — не принимать конкретное обещание или готовность указанной интеграции. Это не утверждение, что адаптацию невозможно разработать. Source audit не доказывает сборку конкретного APK. Вердикты принятия ниже — инженерные решения для нового ядра, не рейтинг проектов.

## Ядро и инструменты

| Кандидат и точный вопрос | Вердикт | Причина и граница |
|---|---|---|
| Свой session actor/state/policy/effect journal как связующее ядро | Да | Требуемое поведение продукта; не отдавать нескольким библиотекам одновременно ownership retries/approvals/persistence |
| Rust-порт OMP (SklUpp, 3008cfd) как отдельный донор | Да | Реальный цикл, типы, tools, providers и supervisor; это evidence форка, не оригинала |
| Rust-порт SklUpp как полностью готовый минимальный mobile kernel | Нет | Scope/parity остаются открытыми; зависимости tools входят в pi-agent |
| Rig как кандидат provider SDK | Да к сравнению dialects; нет к замене без тестов | Библиотечный API существует, но выбор вместо оригинальных OMP adapters не утверждён |
| Goose/Codex целиком как drop-in ядро собственного продукта | Нет | Самостоятельные приложения и контракты; заимствование конкретных модулей допустимо после аудита |
| Eval Rust-порта SklUpp как persistent interpreter supervisor в Termux/server | Да | Есть донорский eval и bridge; внешние интерпретаторы остаются prerequisites |
| Eval Rust-порта SklUpp как готовый автономный APK runtime | Нет | Не решены exec/упаковка интерпретаторов/Android lifecycle |
| Monty как ограниченный Python capability backend | Да | Явные host functions и ограниченный язык; не эквивалент CPython |
| rquickjs как ограниченный JS backend | Да | Interrupt и resource APIs; native callbacks отдельно ограничиваются, APK готовность не доказана |
| RTK internal filters как готовая публичная Rust library | Нет | Нельзя считать внутренний модуль стабильным библиотечным API; лицензии редакций различаются |
| Открытый RTK CLI как опциональный output adapter | Да | Чёткая process boundary; полный вывод сохранять отдельно; конкретную редакцию и LICENSE закрепить |
| Headroom как встроенный Rust-крейт | Нет | Рассмотренные integrations используют другие runtime/proxy; одноимённые проекты не взаимозаменяемы |
| Headroom как опциональный внешний compressor | Да | Отдельный proxy path; не включать без privacy/latency/retention контракта |
| tree-sitter native грамматики выбранных языков | Да | Синтаксический API, не гарантия семантики компилятора |
| Oxc как Rust semantic analyzer | Нет | JS/TS инструментарий, не анализатор языка Rust |
| SCIP как обязательная всегда актуальная навигация | Нет | Нужен отдельный indexer и freshness по revision/build/dependencies |
| SCIP как опциональный индекс со stale-check | Да | Явно понижать точность при недоступности, не выдавать fallback за ту же семантику |
| Wasmi вместо штатного tree-sitter Wasmtime без изменений | Нет | Binding напрямую использует Wasmtime Engine |
| gix для ограниченного Git plumbing | Да | Ограничить нужными API и сценариями |
| gix как доказанная полная замена Git porcelain | Нет | Не принимать общее обещание без набора необходимых контрактов |
| Полный внешний tool по одному mutates_workspace bool под общей security защитой | Нет | Side effects и полномочия шире файлов; декларация tool не enforcement |
| Семь URI schemes как фиксированное ядро | Нет | Нужен расширяемый resolver contract, а не магическое число; адрес не разрешение |
| Единый EventEnvelope/CapabilityCall как внутренний контракт | Да | С версией, scope, порядком, bounded delivery и resync; не устраняет adapters |
| Полная совместимость Web UI от одного --mode rpc-ui | Нет | Клиенты зависят от команд, session files, config, auth и lifecycle |

Первичные исходники: [Rust-порт SklUpp snapshot](https://github.com/SklUpp/omp-rust-termux/tree/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates), [оригинальный OMP snapshot](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36), [Rig](https://github.com/0xPlaygrounds/rig), [Goose](https://github.com/block/goose), [Codex](https://github.com/openai/codex), [Monty](https://github.com/pydantic/monty), [rquickjs API](https://docs.rs/rquickjs/latest/rquickjs/struct.Runtime.html), [RTK editions](https://www.rtk-ai.app/docs/resources/editions/), [Headroom](https://github.com/headroomlabs-ai/headroom), [Oxc](https://github.com/oxc-project/oxc), [SCIP](https://github.com/sourcegraph/scip), [tree-sitter WASM binding](https://github.com/tree-sitter/tree-sitter/blob/master/lib/binding_rust/wasm_language.rs), [gix](https://github.com/GitoxideLabs/gitoxide), [Po1nt9 UI](https://github.com/Po1nt9/omp-web).

## Протоколы: библиотеки не равны готовому продукту

| Компонент | Решение | Ограничение |
|---|---|---|
| [Официальный ACP Rust SDK](https://github.com/agentclientprotocol/rust-sdk) | Да отдельному versioned editor adapter | Выбрать stable wire и exact crate version; unstable режимы не обязательны для ядра |
| [Официальный MCP Rust SDK rmcp](https://github.com/modelcontextprotocol/rust-sdk) | Да клиентскому adapter с минимальными features | Spawn/HTTP, auth, origin, cancellation и caller ceiling задаёт host; elicitation не равно разрешению выполнить tool |
| [A2A Rust SDK](https://github.com/a2aproject/a2a-rs) | Нет обязательной зависимости ядра; да исследованию отдельного federation adapter при потребности | A2A между агентами не нужен для обычного локального tool loop; статус конкретного API требует pinned версии |
| [lsp-types](https://github.com/gluon-lang/lsp-types) | Да типам и отдельному LSP client | Типы не выполняют initialize/framing/timeout/cleanup |
| [async-lsp](https://github.com/oxalica/async-lsp), [tower-lsp](https://github.com/ebkalderon/tower-lsp) | Нет автоматическому выбору как обязательного ядра | Сначала выбрать client/server роль; отказ не означает технической непригодности duplex framework |
| DAP | Да отдельному adapter при debug-сценарии, нет обязательному запуску debugger | Можно взять существующий OMP client; серверный crate не заменяет debugger и Android permissions |

Для каждой интеграции проверять реальную роль (host/client/server), handshake версии, ошибки, отмену, reconnect и ownership дочерних процессов. Не включать TLS/OpenSSL/native runtime features без необходимости. Не считать рекомендацию scout о версии SDK проверкой Cargo.lock будущего продукта.

## Локальная модель, embeddings и мультимодальность

| Кандидат | Вердикт по конкретной роли | Условия |
|---|---|---|
| [llama.cpp](https://github.com/ggml-org/llama.cpp), [Android guide](https://github.com/ggml-org/llama.cpp/blob/master/docs/android.md), [C ABI](https://github.com/ggml-org/llama.cpp/blob/master/include/llama.h) | Да кандидату NDK `.so` backend для APK; нет готовности конкретного артефакта | Android guide подтверждает путь, не испытание. Termux source build и сервер — отдельные варианты с отдельными acceptance; tool loop остаётся в ядре |
| [ExecuTorch Android](https://docs.pytorch.org/executorch/stable/using-executorch-android.html) | Да опциональному Kotlin/native ML backend | Экспортированные модели и поддержанные operators/backend; не GGUF drop-in и не готовый агент |
| [ONNX Runtime Android](https://onnxruntime.ai/docs/build/android.html) | Да кандидату embedding/reranking/vision backend | Отдельные tokenizer/model/EP; GPU/NPU acceleration конкретной модели не гарантируется |
| [Candle](https://github.com/huggingface/candle) | Да кандидату pure-Rust CPU inference/embedding; нет обещанию готового Android GPU ускорения | Не принимать наличие Rust за доказательство скорости, отмены или поддержки нужной модели |
| [Burn](https://github.com/tracel-ai/burn) | Нет обязательному LLM backend в выбранной архитектуре | Общий ML framework, без явной потребности в собственной сети добавляет интеграцию; возможен отдельный эксперимент |
| Любой из перечисленных runtime как универсальная модель для любого телефона | Нет | RAM, context/KV, ABI, operators, quantization и thermal различаются |

Выбор по умолчанию для обсуждения: cloud gateway + опциональный llama.cpp. Embedding backend добавлять только если retrieval-корпус показывает преимущество над FTS/lexical search. Сравнивать качество recall, задержку, память и батарею; не выбирать vector DB только из архитектурной моды.

Изображения/аудио/документы/browser — отдельные capabilities. SAF URI не POSIX path; импорт с лимитами размера и конфликтов. Для аудио нужны permission и lifecycle; удалённая обработка требует явной privacy политики. Browser automation — отдельный renderer/process и поверхность недоверенного web-контента, не свойство ядра или embedding library. Выполнение JavaScript страницы не должно получать tool.* полномочия агента.

## Что не принимается как гарантия

- Готовность всех выбранных компонентов в одном бинарнике/APK: **нет**, такого проверенного артефакта исследование не создавало.
- Наличие конкретного механизма в опубликованном исходнике: **да**, в пределах ссылок и ролей выше.
- «Без непроверенного» как запрет выдавать догадки за факт: **да**. Как требование доказать отсутствие всех неизвестных дефектов без реализации: **нет**, технически недостижимо.
- Бесконечный поиск «всех проектов»: **нет** конечного множества; покрытие фиксируется по перечисленным subsystem/рискам и решениям, а не по всему GitHub.

Проверка артефакта: `test -s docs/rust-agent-component-verdicts-2026-11-05.md` из корня checkout. Runtime-испытания не выполнялись.
## Эксплуатация и проверяемость: ещё один обязательный слой

| Аспект | Решение | Что проверять |
|---|---|---|
| Версии состояния | Да явному schema version; SQLite — вариант, не утверждённый обязательный store | При выборе SQLite нужны транзакционные миграции и импорт/экспорт OMP JSONL с сохранением IDs/ветвей; нет молчаливому downgrade или обещанию совместимости без проверки |
| Артефакты | Да same-filesystem temp/write/sync/atomic publish и hash manifest | Конфликт назначения, ENOSPC, crash и symlink; rename сам по себе не доказывает power-loss durability |
| Backup | Да SQLite online backup и проверяемому restore; нет простой копии живого main DB без WAL-aware процедуры | Backup integrity и фактический restore на отдельном каталоге; не считать checksum доказательством семантической корректности |
| GC | Да mark roots/retention/dry-run, согласование с живыми readers/writers; нет удалению неизвестных пользовательских файлов | Crash/retry, pinned artifacts, незавершённые публикации; GC не гоняется с текущим turn |
| Sync устройств | Нет обязательному CRDT в ядре; да optional outbox/dedup для данных | History replay не вызывает tools; дедупликация сообщения не обещает exactly-once внешнего эффекта |
| Mergeable заметки | Да [Automerge](https://github.com/automerge/automerge) как опции при реальном offline-editing | CRDT convergence не разрешает конфликт полномочий или команд; ограничивать размер/историю |
| Observability | Да structured allowlisted fields и redact до записи/экспорта; нет raw bodies по умолчанию | Ошибки, retries, spans и crash logs не обходят политику; regex не находит все виды секретов |
| Supply chain | Да locked dependencies, SBOM, license audit, provenance и контролируемым обновлениям | Native/vendor/model/tokenizer/JS зависимости входят в аудит, не только Cargo.lock |
| Воспроизводимость | Да двум независимым controlled builds и сравнению; нет обещанию bit-for-bit от одного SOURCE_DATE_EPOCH | NDK/JDK/Gradle/Rust/JS assets и signing фиксируются; отличия объясняются, не скрываются |

Первичные механизмы: [SQLite user_version](https://sqlite.org/pragma.html#pragma_user_version), [online backup](https://www.sqlite.org/backup.html), [WAL](https://sqlite.org/wal.html), [Automerge sync source](https://github.com/automerge/automerge/blob/main/rust/automerge/src/sync.rs), [OpenTelemetry sensitive data](https://opentelemetry.io/docs/security/handling-sensitive-data/), [cargo-deny](https://github.com/EmbarkStudios/cargo-deny), [CycloneDX Rust](https://github.com/CycloneDX/cyclonedx-rust-cargo), [SLSA provenance](https://slsa.dev/spec/v1.2/build-provenance), [Android SBOM](https://source.android.com/docs/setup/create/create-sbom).

Не принимать предложенные исследовательским агентом фиксированные busy timeout, depth cap или crypto/container зависимости как обязательные продуктовые значения: они требуют заданных ограничений и замеров. Не выбирать wipe как автоматический способ downgrade. Не обещать exactly-once внешних эффектов по одному `op_id`: нужен idempotent downstream либо reconciliation.

## Окончательный выбор для обсуждения

**Да:** собственное переносимое ядро + выборочные компоненты [оригинального OMP на 035e4f95](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36); Rust-порт SklUpp рассматривать отдельно, не смешивая evidence; отдельные provider/protocol/execution adapters; Kotlin/UniFFI host для Android; внешний 9router как самостоятельный gateway; native tools и разрешённые workers по возможностям host. Store и provider SDK ещё не выбраны как обязательные зависимости.

**Нет:** один универсальный middleware как достаточная безопасность; готовая 100% совместимость клиентов; автоматическая замена LSP синтаксисом; весь JS gateway внутри 15-МБ Rust-бинаря; обязательное копирование всех memory backends и 15 ролей; гарантия APK без его сборки и испытаний.

Это конечный вердикт по изученным способам композиции. Решение о реализации нового продукта остаётся за пользователем; исследование не создавало прототип, план работ или production artifact.

## Оригинал OMP отдельно от Rust-форка

[Углублённое исследование оригинала](omp-original-foundation-research-2026-11-05.md) — первичный источник выбора основы: original native engines пригодны как доноры; готового upstream Rust AgentLoop на проверенном снимке нет. Первые строки таблицы про SklUpp не относятся к устройству оригинала.

