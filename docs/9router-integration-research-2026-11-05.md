# 9router: интеграция с новым Rust-ядром

Проверка исходников 2026-11-05. Ревизия `1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d`, версия 0.5.112: [commit](https://github.com/decolua/9router/commit/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d). Результат — source audit, не запуск на Android. Ниже ссылки закреплены на эту ревизию.

## Вердикт

**Да: использовать 9router отдельным сервисом по HTTP. Нет: считать его готовой встраиваемой Rust-библиотекой или готовым Android runtime.** Собственное ядро сохраняет loop, tools, approvals, память и состояние; шлюзу принадлежат только явно выбранные маршрутизация и provider credentials. Лицензия корня [MIT](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/LICENSE) не освобождает от проверки зависимостей и условий провайдеров.

| Точное утверждение | Да/нет | Обоснование |
|---|---|---|
| Есть поддержанный серверный HTTP-путь интеграции | Да | Next API, rewrites, custom-server, standalone packaging |
| Есть готовый Rust library/FFI интерфейс | Нет | JS/Next приложение; open-sse — ESM JavaScript |
| open-sse можно взять как независимый готовый HTTP server без app-слоя | Нет | Нет такого entrypoint; auth/settings/combo/storage связаны с app |
| Выделение open-sse в свой JS sidecar технически предусмотрено структурой исходников | Да | Есть facade/executors/translators; необходим собственный host и разрыв связей, это разработка |
| Исходники доказывают готовность конкретного Termux/Android APK | Нет | Наличие SQLite fallback не доказывает весь runtime, ABI, lifecycle и providers |
| Допустимо выбрать HTTP sidecar как архитектурный путь для Termux | Да | Разделение по HTTP не требует портирования gateway в Rust; acceptance устройства остаётся отдельной проверкой |
| Порт всех возможностей 9router на Rust уже готов | Нет | Это самостоятельная переработка, не доступный готовый компонент |

«Нет» не означает невозможность написать адаптацию. Оно отвергает конкретное утверждение о готовности. Утверждения о производительности и проценте экономии не приняты без замеров.

## Устройство по исходникам

- [package.json](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/package.json): `9router-app`, private Next/React app, JS runtime dependencies и optional SQLite adapter.
- [cli/package.json](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/cli/package.json): отдельный публикуемый launcher, не ядро маршрутизации.
- [custom-server.js](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/custom-server.js): wrapper сервера, обработка происхождения запросов и startup/background сервисов. Не обходить его запуском произвольного собранного server.js без эквивалентных проверок.
- [next.config.mjs](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/next.config.mjs): rewrites `/v1/*` и других совместимых путей, external dependencies.
- [Dockerfile](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/Dockerfile): пример серверной упаковки standalone, static resources, runtime JS и lazy dependencies. Это не Android-артефакт и не инструкция запускать Docker на телефоне.

## Реальная граница open-sse

[Facade](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/index.js) экспортирует конфигурации, сервисы моделей/refresh/fallback, core handler и executors. [chatCore.js](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/handlers/chatCore.js) связывает преобразование запроса, исполнителя и streaming response; содержит app-зависимость `@/lib/usageDb.js`.

[App chat handler](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/src/sse/handlers/chat.js) делает API key ACL, выбор credentials, combos, refresh и retry. Нельзя вырезать его и считать, что open-sse сохранил все права и маршрутизацию. [Model service](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/services/model.js) разбирает provider/model и aliases; [accountFallback](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/services/accountFallback.js) определяет error/cooldown политику; [combo](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/services/combo.js) — дополнительные стратегии. Это не простой URL forwarder.

Вариант выделения: собственный JS host передаёт credentials/config/context, usage sink заменяет app import, сохраняются translator registration и executor dependencies. Глобальные изменения fetch, lazy installs и побочные эффекты импортов становятся частью аудита. Оценка «несколько shims» не доказывает малый объём или полноту порта.

## Хранение и credentials

[DB driver](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/src/lib/db/driver.js) пробует Bun SQLite либо `better-sqlite3`, затем `node:sqlite`, затем `sql.js`; если ни один не доступен — бросает ошибку. Следовательно, **fallback предусмотрен, но не гарантирован**. `sql.js` требует доступных WASM/JS ресурсов и своих условий persistence. [paths](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/src/lib/db/paths.js), [dataDir](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/src/lib/dataDir.js) задают пути данных; переносить нужно весь необходимый набор, не предполагать, что все логи следуют DATA_DIR.

[Credential selection](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/src/sse/services/auth.js), [token refresh](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/open-sse/services/tokenRefresh.js) и [background refresh](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/src/sse/services/backgroundTokenRefresh.js) — собственная stateful система. Rust-клиент не должен параллельно обновлять те же refresh tokens: некоторые провайдеры используют одноразовые значения. Gateway API key и upstream credential — разные секреты и разные области действия.

## Как встраивать в продукт

### Предпочтительный вариант: отдельный управляемый сервис

Rust gateway adapter обращается к закреплённому loopback/HTTPS endpoint с явным model route. Сервис имеет отдельный lifecycle, каталог данных, ограниченный API key и конфигурацию. Клиент поддерживает SSE/JSON, cancellation и честное отображение фактически выбранной модели и usage. Сессия/роль/operation ID передаются только в разрешённых metadata/headers; выбранное поле идентичности сверяется с конкретной версией gateway, не выдумывается универсальным `X-Session-Id`.

Запретить вложенную независимую retry/fallback политику без одного владельца: иначе клиент и шлюз могут умножать попытки, менять аккаунт и скрывать реальную причину. Правило безопасности: смена endpoint не наследует credentials, cross-origin redirect не пересылает Authorization автоматически, логирование не раскрывает prompt и ключи. UI/dashboard management не должен быть доступен недоверенной сети лишь потому, что inference endpoint ограничен API key.

### Termux sidecar

Допустимый вариант архитектуры — production JS server с явным DATA_DIR и контролируемыми зависимостями. Не использовать tray/desktop/browser/MITM/tunnel возможности без отдельного решения. [CLI runtime hook](https://github.com/decolua/9router/blob/1fe586bb1cf35e1ed0cb2150a8ef7d1db040b21d/cli/hooks/sqliteRuntime.js) может ставить зависимости в writable runtime; такая автоустановка не разрешена по умолчанию в новом продукте. Сборки вне телефона, установленные артефакты проверяются на устройстве. Из исходников нельзя вывести отсутствие SIGSEGV или работоспособность всех native bindings на Bionic.

### Нативный APK

Не брать готовый launcher как embedded runtime: собственные UID/пути, native library packaging, exec restrictions, service lifecycle, Node bootstrap и управление памятью требуют Android-host проекта. Full Next dashboard внутри APK — не обязательное условие; можно оставить gateway снаружи и сделать native settings UI, но management API и его auth нужно реализовать явно. Не считать WebView UI заменой серверу Node.

## Acceptance выбранного варианта

1. Зафиксировать commit, lockfile, JS runtime, backend DB и цель поставки; выключить невыбранные providers и lazy installation.
2. В изолированном DATA_DIR проверить startup, auth отказ, model listing, streaming tool call и cancellation; runtime не трогает существующую пользовательскую БД.
3. Контролируемый upstream доказывает retry limit, partial stream failure, 429, refresh и отсутствие cross-origin secret forwarding; не считать fixture проверкой реального OAuth провайдера.
4. Проверить совместимость reasoning/tool replay и usage с конкретным Rust adapter, в том числе смену фактической модели.
5. Для Termux/APK отдельно проверить Bionic, рестарт, background/pause, утечки процессов и секретов. Только после этого отвечать «да» на готовность конкретного артефакта.

Сборка/запуск этих сценариев в исследовании не выполнены. Это конечный критерий включения, не скрытый положительный verdict.

Проверка документа: `test -s docs/9router-integration-research-2026-11-05.md` из корня checkout.
