# Track 6: Интеграция `9router` / `9routerx` / `CLIProxyAPI` с поддержкой форка и апстрима (2026-11-27)

> **Источники:** `Research9RouterForkSync`, секция `9routerx` из `ResearchEcosystemProviders2`, аудит `ResearchReviewCoreEcosystem`.
> **Якоря:** `~/rust-agent/crates/aragent/src/{config.rs:36–43, runtime.rs:1964–1982,3009–3465,4369–4396,7635}`, `crates/pi-agent/src/provider/timeout.rs`, `~/9router-termux/{patch-9router.mjs,gw2a.sh}`, установленный `9router@0.5.123` (`cli.js:120`, `src/shared/utils/apiKey.js:3`, `settings.js:16`, `.next-cli-build/server/middleware.js:2`), `~/OmniRoute` (`v3.8.55`), `9routerx` (`v1.5.6`), `CLIProxyAPI`, `CLIProxyAPI-Rust` (`v0.3.2`).
> **Границы проверки:** статический анализ кода и конфигов; живых атак, замеров на телефоне и тестирования обновлений схемы БД между версиями не проводилось.

---

## 1. Текущая точка опоры в `arag` и обзор шлюзов

`arag` **уже поддерживает** OpenAI-compatible HTTP endpoints: `ProviderConfig` (`config.rs:36–43`), `wire_base_url` / `wire_provider` (`runtime.rs:3009–3465`) и тестовый fixture для `9router` (`http://127.0.0.1:20128/v1`, `runtime.rs:7635`). Сейчас отсутствует только слой **управления жизненным циклом (supervisor), изоляции ключей и проверки владения процессом**.

| Проект | Стек / лицензия | Дефолты и особенности | Вердикт |
|---|---|---|---|
| **`9router`** (`0.5.123`) | Node / Next.js · MIT | Bind `0.0.0.0` (`cli.js:120`), пароль `123456` (`settings.js:16`), fallback `API_KEY_SECRET` (`apiKey.js:3`), health `/api/health` | **Go через Pattern A** (только с явным `127.0.0.1`, случайным паролем/секретом и без `patch-9router.mjs`) |
| **`9router-termux`** | Скрипты + JS-патчер (без `.git`) | `patch-9router.mjs` правит минифицированные чанки (14.4 KB, ~20 замен). `gw2a.sh` даёт рабочий шаблон `proot` (`resolv.conf` + `SSL_CERT_FILE`) для статического Go | **No-Go на `patch-9router.mjs`:** ломается на минорных апдейтах и в `:66–75` **молча отбрасывает неподдержанные Gemini tools и non-text `content-parts`** |
| **`9routerx`** (`v1.5.6`) | Go 1.27 (`CGO_ENABLED=0`, ~48 MB amd64) · MIT | Отстаёт от `9router` на 38 версий (`0.5.85` vs `0.5.123`); нет Android release assets; DNS/TLS на Bionic требует проверки или `proot` | **Отложить/проверить Android runtime**; не делить БД `data.sqlite` с `9router` |
| **`CLIProxyAPI`** | Go · MIT | Дефолтный bind на все интерфейсы; свой `request-retry: 3` | **Опционально через внешний endpoint** с `request-retry: 0` и `127.0.0.1` |
| **`CLIProxyAPI-Rust`** (`v0.3.2`) | Rust (`axum` + `rustls`) · Unlicense | Чистый Rust без Go DNS/TLS-проблем, но ранний статус и нет Android CI | **Наблюдать** |
| **`OmniRoute`** (`v3.8.55`) | Node · MIT | Хороший донор паттерна `ServiceSupervisor` / `adopt` / `DISABLE_MITM=true` / `127.0.0.1`; дефолт `--max-old-space-size=6144` недопустим на телефоне | **Идея supervisor (снизить heap cap)** |

---

## 2. Стратегия форка и синхронизации с апстримом

| Паттерн | Устройство | Плюсы / минусы |
|---|---|---|
| **Pattern A: Zero-patch upstream + supervisor в `arag`** *(рекомендуется)* | Пин релиза по `npm` integrity или `SHA256SUMS`; запуск с явным `127.0.0.1`, сгенерированными секретами, лимитом памяти и pre-upgrade snapshot БД | 0 merge-конфликтов при обновлении апстрима; чистая граница лицензий |
| **Pattern B: Тонкая патч-очередь (<100 LoC в исходниках)** | Форк с минимальными патчами **только в исходном коде** (никогда в minified bundle) + CI auto-rebase с fail-fast тестом | Приемлемо только для блокирующих фиксов, которых нет в апстриме |
| **Pattern C: `GatewayBackend` trait в `arag`** | Тонкий адаптер (`probe`, `spawn_spec`, `stop`) поверх существующего `OpenAiCompatibleProvider` без переписывания wire-протокола | Добавлять только после фиксации контрактов владения и секретов |

---

## 3. Критические нюансы безопасности и надёжности (после ревью)

1. **Уточнение по `API_KEY_SECRET` (`ResearchReviewCoreEcosystem`):** `validateApiKey` в `.next-cli-build/server/middleware.js:2` дополнительно сверяет токен с таблицей активных ключей `apiKeys`, поэтому знание fallback-секрета **не даёт мгновенного обхода API** подбором HMAC. При этом дефолтные `0.0.0.0` и пароль дашборда `123456` всё равно требуют принудительной блокировки на старте.
2. **Identity-checked adoption:** усыновлять уже запущенный процесс роутера только по `200 OK` на `/api/health` **запрещено** (на порту может висеть сторонний или чужой сервис). Проверять ownership/идентичность процесса и никогда не убивать чужой PID.
3. **Один владелец БД и бэкапы:** никогда не открывать `data.sqlite` параллельно двумя версиями/реализациями (`9router` + `9routerx`) и не совмещать её с `agent.db`. Перед обновлением версии — остановка шлюза и копия `data.sqlite` + `WAL`/`SHM`.
4. **Гонка OAuth refresh (`runtime.rs:1964–1982, 4369–4396`):** при отсутствии явного root-ключа `CredentialCallback` обращается в локальный store (а на промахе возвращается к resolved key). Для gateway-провайдеров хранить единый явный ключ/policy так, чтобы локальный `AuthBackend` не обновлял и не перетирал токен параллельно со шлюзом.
5. **Единый владелец retry (`provider/timeout.rs`):** вложенные retry на уровне шлюза и `arag` умножают запросы и биллинг (отсутствие SSE-чанка не гарантирует, что upstream не выполнил запрос). Отключать повторы на одном из слоёв (`maxRetries = 0` в `arag` или `request-retry: 0` в шлюзе).
6. **D27 redaction и denied paths:** регистрировать gateway/management/master ключи в `known_secrets`; включить в D27 denylist все настраиваемые `DATA_DIR` роутера (включая `WAL`/`SHM`, backups, `.cli-proxy-api`, `.9routerx`, `.omniroute`). Помнить: D27 не защищает от прямого чтения SQLite из `bash`.
7. **Go DNS/TLS на Android:** `gw2a.sh` показывает рабочий обход через `proot` (`resolv.conf` + `SSL_CERT_FILE`), но поведение конкретной CGO/static сборки нужно тестировать на устройстве, а не экстраполировать на все Go-бинарники.
