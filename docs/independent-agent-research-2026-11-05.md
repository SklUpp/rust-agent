# Функционально независимый Rust-агент

Дата: 2026-11-05. Исследовательское заключение, не утверждённая спецификация и не план реализации. Уточнение цели: предпочтительно самостоятельный продукт, а не совместимый OMP clone. Три поставки: Android app, Termux/Linux CLI/TUI agent, headless server.

## Вывод

Рекомендуется собственный продуктовый контракт с выборочным заимствованием библиотек и алгоритмов. Оригинальный OMP — первичная reference implementation, Rust-форк — разрешённый донор после сравнения. Ни OMP, ни 9router, ни чужой auth/session/plugin runtime не должны быть обязательным установленным приложением. Совместимость — опциональный адаптер вне ядра.

Это предлагаемый критерий независимости, не утверждение, что такой агент уже реализован.

## Независимость имеет несколько измерений

| Измерение | Предлагаемое требование | Чего не обещает |
|---|---|---|
| Runtime | Свой бинарь/библиотека выполняет выбранные функции без запуска донорских приложений | Отсутствия всех сторонних библиотек или C/FFI |
| Данные | Собственная versioned session/config schema и миграции | Автоматической совместимости с OMP JSONL |
| Продукт | Свои команды, permissions, docs, release и update policy | Отказа от стандартных протоколов |
| Инференс | Прямой provider backend и опциональный локальный inference, без обязательного 9router | Работы cloud-модели без сети/аккаунта или offline LLM без весов |
| Платформа | Явные capability profiles для APK/Termux/Linux/server | Полного shell/toolchain и одинаковых функций везде |
| Разработка | Закреплённые доноры, воспроизводимая сборка, свой regression corpus | Отсутствия upstream сопровождения, license obligations или уязвимостей |

«Без аккаунта» означает без обязательного аккаунта самого продукта/донора. Учётная запись выбранного облачного провайдера может быть необходима. «Локальное ядро» не равно «локальная модель». Без модели доступны управление данными и разрешённые инструменты; автономный LLM-turn требует настроенного inference backend.

## Что должно принадлежать продукту

1. Канонические messages/content blocks и provider-native данные там, где потеря ломает replay; provider-specific поля не выдавать за универсальные.
2. Сессия/turn/task/call IDs, явные состояния и владелец исполнения. Возобновление и replay не исполняют исторические вызовы повторно.
3. Полномочия, approval и текущая авторизация независимо от frontend; scope потомка не расширяется делегированием.
4. Контекстный сборщик, бюджет, архив и provenance; summaries и память не получают уровень системных правил.
5. Registry инструментов с контрактами эффектов/ошибок/отмены. Реальные ограничения реализует host, декларации не sandbox.
6. Durable lifecycle и artifacts, собственные migration/import/export правила. Выбор SQLite/JSONL остаётся отдельным решением, не требованием OMP.
7. Собственные UI-facing commands/events/snapshots с версиями; bounded queues, reconnect и resync.

Не обязательно сразу реализовывать A2A, все memory backends, отдельный gateway proxy, полный набор конфигурационных ролей моделей, произвольные TS plugins или каждый UI. Это опции, не условия существования независимого агента.

## Библиотека не равна зависимости от приложения

[Оригинальный pi-edit manifest](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-edit/Cargo.toml) наследует workspace metadata/dependencies и зависит от соседних Rust crates. Для отдельного использования нужно контролировать этот build graph; установленный OMP как процесс из этого не требуется. [Engine seam](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-edit/src/lib.rs) использует FileSource/EditStore и host-owned EditWriter. Policy и фактическая запись остаются обязанностью нового host.

[Root MIT](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/LICENSE) допускает copy/modify/merge/distribute при сохранении notices. Это не юридическое заключение по всему транзитивному графу. Отдельный Cargo dependency либо vendored модуль не лишает продукт функциональной самостоятельности; сопровождение такого кода становится ответственностью продукта.

[pi-natives](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-natives/Cargo.toml) — N-API addon, не готовая Kotlin/Rust agent library. [Upstream loop](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/agent-loop.ts) — TypeScript. [Rust-форк](https://github.com/SklUpp/omp-rust-termux/tree/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent) предоставляет отдельный кандидат harness; соответствие выбранному контракту проверяется отдельно от наличия типов.

## Прямой inference вместо обязательного gateway

Собственный provider boundary может использовать выбранные OMP/fork adapters, Rig/Goose либо перенос 9router dialects. [Rig completion API](https://docs.rs/rig-core/0.46.0/rig/completion/index.html) разделяет normalized messages, model handles, replay target и provider options. Это пример library seam, не утверждение выбора Rig или совместимости всех dialects.

9router допускается opt-in сервисом или источником выбранного Rust-порта. Нельзя требовать его dashboard/SQLite/OAuth broker только для обычного provider request. Если подключён внешний gateway, он явно указан в конфигурации и не скрывает владельца refresh/retry. См. [Rust-переносы](rust-port-reuse-research-2026-11-05.md) и [9router audit](9router-integration-research-2026-11-05.md).

## Три поставки и два расположения исполнения

| Поставка | Локальная работа | Подключение к удалённому исполнению |
|---|---|---|
| Android app | Compose + Rust library, Android filesystem/credentials/lifecycle adapters, выбранные инструменты в составе поставки APK | Тот же UI управляет серверной сессией; это явно remote mode |
| Termux/Linux CLI/TUI | Свой бинарь, workspace/process/PTY/toolchain adapters; различия Bionic/Linux явны | TUI может attach к серверу; пользователь видит местоположение workspace |
| Headless server | То же ядро, процессы и persistence без терминального UI | Аутентифицированные clients, session ownership, разграничение workspace и credentials |

Android app может содержать оба режима, а не быть двумя несвязанными продуктами. Локальные и удалённые workspace/resources имеют разные IDs/scopes; путь `/src/a.rs` на сервере не считается локальным файлом телефона. Один UI может подключать несколько исполнителей, но явный владелец каждой изменяемой сессии сохраняется.

[Android W^X](https://developer.android.com/about/versions/10/behavior-changes-10), [SAF](https://developer.android.com/training/data-storage/shared/documents-files), [FGS](https://developer.android.com/develop/background-work/services/fgs/service-types) продолжают действовать после Rust rewrite. [llama.cpp Android](https://github.com/ggml-org/llama.cpp/blob/master/docs/android.md) — кандидат локального inference, не гарантия модели/устройства. Отсутствие Node не доказывает автономный toolchain или бесконечный фон.

## Критерии, которыми можно доказать независимость

Это будущие acceptance scenarios, не проведённые испытания.

- Чистый профиль, без установленного OMP/Bun/Node/9router: запуск, настройка, выбранные native tools и сохранение/повторное открытие сессии.
- Прямой контролируемый provider HTTP/SSE endpoint: полный tool loop без gateway; partial JSON не исполняется, отмена и ошибки отражаются корректно.
- Политика исходящих соединений блокирует всё, кроме явно разрешённых endpoints: нет скрытых catalog/model/telemetry downloads. Для предлагаемого APK загружаются данные/веса, а исполняемый native-код входит в поставку; другие способы обновления требуют отдельной проверки Android/канала распространения. Загрузка — только явно, с источником/размером/integrity checks.
- Отключены compatibility adapters: базовые команды, UI, storage и tools продолжают работать. Импорт OMP проверяется отдельно и не требует его живого процесса.
- Сервер рестартует, клиент переподключается; события восстанавливают state, но не повторяют неизвестные side effects. Текущая авторизация проверяется снова.
- Android: чистая установка, отсутствие Termux, revocation SAF grant, activity recreation, process death, смена сети; unavailable capability не заменяется скрытым запуском внешнего runtime.
- Офлайн inference проверяется только с явно установленными моделью/tokenizer/backend; без них честное MissingBackend, не успешная фиктивная генерация.
- Release artifact включает нужные ресурсы и notices; lockfile, ABI/NDK, schema version и scope возможностей документированы. Android native libraries проверяются также на ELF/упаковочное выравнивание и запуск на 16-КБ page-size системе: [официальные требования](https://developer.android.com/guide/practices/page-sizes). Дедлайны store policy проверяются на момент выпуска, не фиксируются предположением.

## Выбор направления

Рекомендация — **самостоятельное ядро с разрешённым заимствованием и опциональной совместимостью**. Не требовать OMP wire/session/plugin parity для внутреннего дизайна, но сохранять импорт/адаптер там, где он приносит реальную пользовательскую пользу. Это исследовательский выбор, требующий подтверждения перед реализацией, не скрыто утверждённая спецификация.

Проверка артефакта из checkout: `test -s docs/independent-agent-research-2026-11-05.md`. В рамках исследования нет сборки/прототипа/device validation.

## Проверенные seams и устранение скрытых зависимостей

| Донор | Что можно взять без установленного приложения | Что требуется заменить или предоставить |
|---|---|---|
| Original pi-edit/AST/VFS | Rust engines и host APIs | Workspace Cargo metadata, sibling dependencies, свой writer/filesystem/runtime; native grammar dependencies остаются |
| Original pi-shell | Rust engine с `bash::Host` и session APIs | Host processes/signals и нужные builtins; shell не становится security boundary |
| Fork pi-agent | Rust loop/providers/supervisor/mailbox | Свои config/credential/tool/store границы; не подключать весь omp-cli ради одного loop |
| 9router translator/fallback | Данные форматов, transform functions, error classification | Перенос JS логики; injected model tables, state owner, clock и credential store вместо Next globals |
| Rig providers | Library API вместо отдельного приложения | Выбранные TLS/features/provider options; свой loop owner |
| Goose declarative providers | Rust API и definitions | `api_key_env/auth.command` заменить контролируемым credential resolver/runner; команда auth не разрешена автоматически |

Это альтернативные или адаптируемые seams, не обещание совместной сборки двух donor workspaces. При соединении original/fork выбрать согласованный dependency graph, проверить версии и API pi-ast/pi-diff/pi-walker и workspace inheritance; не смешивать дублирующиеся типы разных версий. Source audit не доказывает их совместимость.

Дополнительные первоисточники: [pi-shell Host](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-shell/src/lib.rs), [fork manifest](https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-agent/Cargo.toml), [9router combo state](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/services/combo.js), [9router app auth](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/src/sse/services/auth.js), [Goose declarative source](https://github.com/aaif-goose/goose/blob/main/crates/goose-providers/src/declarative.rs).

Общие globals, прямое чтение env и CLI prompts не обязательно запрещают reuse: их можно заменить параметрами/host callbacks. Но изменение границы проверяется своим integration corpus, а не предполагается безопасным по факту dependency injection. VFS BlockingFs требует совместимого Tokio runtime; current-thread `block_in_place` может паниковать, не объявлять это автоматически deadlock.

## Стандарты и внешняя совместимость

ACP — adapter для редактора, MCP — adapter инструментов/контекста, LSP/DAP — code/debug backends. Их словарь не должен определять внутреннюю schema продукта. Пользовательские scopes и authorization остаются у ядра/host; `kind` tool и permission option из UI не заменяют проверку эффекта. [ACP](https://agentclientprotocol.com/), [MCP](https://modelcontextprotocol.io/specification/2025-11-25).

Полезные независимые reference seams: [pi runLoop и transformContext/convertToLlm](https://github.com/earendil-works/pi/blob/4bb837c960295478224e9879fcd94ac0a09db5ab/packages/agent/src/agent-loop.ts), [Goose conversation](https://github.com/aaif-goose/goose/blob/main/crates/goose-provider-types/src/conversation.rs), [provider errors](https://github.com/aaif-goose/goose/blob/main/crates/goose-provider-types/src/errors.rs), [Codex thread ownership](https://github.com/openai/codex/blob/main/codex-rs/core/src/thread_manager.rs). Они подтверждают полезность разделения loop, canonical history, provider errors и session ownership; не являются обязательными зависимостями нового продукта.

Импорт внутренних библиотечных типов донора допустим внутри адаптера. Независимость означает, что публичные команды/данные продукта не обязаны наследовать чужую schema и upgrade cycle; она не требует бессмысленно дублировать каждый third-party type. Не скрывать errors через safe fallback только ради «non-throw» API: ошибки hook/provider должны сохранять корректный terminal outcome.


### Ограничение ссылок и распространения

Goose/Codex ссылки на `main` и llama.cpp на `master` в этом исследовании — информационные reference-кандидаты, **не принятые закреплённые зависимости**. До фактического заимствования требуются commit/lockfile и API-проверка. Проверенный original/fork material выше закреплён отдельно.

Профиль Android-поставки должен учитывать [Play Device and Network Abuse policy](https://support.google.com/googleplay/android-developer/answer/9888379) наряду с ограничениями ОС. Не сводить W^X к универсальному запрету любого dlopen для всех target SDK и типов файлов: способ упаковки/загрузки и канал распространения проверяются отдельно. Предложенный профиль не использует скачивание native code как скрытый обход.

