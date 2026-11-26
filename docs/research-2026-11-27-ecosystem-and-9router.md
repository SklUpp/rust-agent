# Исследование: экосистема крейтов, Android-профили и интеграция `9router` / `9routerx` / `CLIProxyAPI` (2026-11-27)

> **Статус:** ненормативное исследование + независимое ревью (`ResearchEcosystemProviders2`, `Research9RouterForkSync`, `ResearchReviewCoreEcosystem`).
> **Источники:** `~/rust-agent` (`3b021a58`), `~/9router-termux`, `~/OmniRoute`, установленный `9router@0.5.123`, `9routerx` (`v1.5.6`), `CLIProxyAPI`, `CLIProxyAPI-Rust` (`v0.3.2`).
> **Границы проверки:** только инспекция исходников, лицензий и конфигов; живых атак, замеров RAM/батареи и сборок новых крейтов на Android не проводилось.

---

## Часть 1. Крейты и проекты для интеграции в `arag` (Track 5)

### 1.1 Отбор зрелых проектов (с поправками лицензий и Android-рисков)

| Кандидат | Лицензия / статус | Потенциал для `arag` | Ограничения и вердикт ревью |
|---|---|---|---|
| **`wasmi`** | MIT / Apache-2.0 | Интерпретатор WASM без Cranelift JIT (лучше `wasmtime` при W^X-ограничениях на Android `[INFERENCE]`) | **Не является авто-песочницей:** обязательны жёсткие host capabilities, лимиты fuel, памяти и прерывания |
| **`wreq` / `rquest`** | Apache-2.0 (`wreq`) | TLS-имперсонация браузера для OAuth-логина прямо из Rust без Go sidecar | Зависит от BoringSSL и нативной сборки; **совместимость с Android Bionic не доказана**, нужно проверять pin и lineage `rquest` |
| **`regorus` / `cedar-policy`** | MIT / Apache-2.0 | Декларативные политики approval вместо роста regex-правил | Подходит для проверки на устройстве (без сети); нужна аккуратная UX-диагностика отказов |
| **`frizbee`** | MIT | Быстрый fuzzy-поиск (SIMD/NEON) | Хороший кандидат для локальной навигации без тяжёлого индекса |
| **`wezterm-term`** | MIT | Эмуляция терминала (`vim`/`top`/`ssh`) поверх PTY | Opt-in; ограничивать scrollback (напр. 2–3k строк) для контроля RAM на телефоне |
| **`ra_ap_*` / `stack-graphs` / `oxc_*`** | MIT / Apache-2.0 | Глубокая семантика (Rust, multi-lang def/ref, JS/TS codemods) | **Только desktop / on-demand (не default Android):** `ra_ap_*` ломает API каждые ~6 недель; `stack-graphs` ~3× к индексу |
| **`sqlite-vec`** | Pre-1.0 (проверять pin) | Векторный поиск в SQLite без отдельного процесса | **Отложить (поправка ревью):** задокументированный KNN — **brute-force**, он не гарантирует низкий RAM (page cache, temp allocs, I/O). Не брать как «бесплатное решение» до замеров на Android |
| **`tensorzero`** | Apache-2.0 | AI gateway crate | **No-Go:** репозиторий **archived** (2026-06) |
| **`nucleo`** | **MPL-2.0** | Fuzzy matcher | **No-Go** при текущей политике без copyleft-согласования |
| **`new-api`** | **AGPL-3.0** | Go API gateway | **No-Go** (лицензия) |
| **`landlock` / `hickory-resolver`** | — | — | **Поправка ревью:** не отбрасывать догматически формулировкой «всегда падает»; фиксировать как platform-specific compatibility risk и проверять пробой на устройстве |

### 1.2 Адаптивные Android-профили (`Pocket` / `Docked` / `Server`)

- Идея вынесена в отдельный `device_profile.rs` по образцу fail-open `wake_lock.rs:1–90` (`termux-battery-status`, `/sys/class/thermal`, `MemAvailable`) для гейтинга тяжёлых задач (эмбеддинги, индексация, sidecar, параллелизм).
- **Ограничения из ревью:**
  - Конкретные пороги (RSS 400/800 MB, `GOMEMLIMIT=192MiB`, 2/4 потока) являются **неизмеренными оценками**, а `GOMEMLIMIT` — мягкий ориентир Go GC, не жёсткий RSS cap.
  - **Lossy token compression (даже для read-only `grep`/`diff`/логов) опасен:** сжатый/потерянный вывод вводит модель в заблуждение при последующих правках. Допустим только как **явный opt-in с маркировкой** и сохранением доступа к оригиналу.
  - Системный браузер + loopback OAuth зависит от разрешения провайдером `127.0.0.1` redirect URI; state/PKCE и loopback-привязка обязательны.
  - Волна внедрения «Волна 1 как дешёвая без риска» признана **No-Go**: любой новый крейт/sidecar проходит license + Android build + memory/battery/security gate.

---

## Часть 2. Интеграция `9router` / `9routerx` / `CLIProxyAPI` и синхронизация с апстримом (Track 6)

### 2.1 Что есть в `arag` и что представляют собой кандидаты

- **`arag` уже поддерживает OpenAI-compatible HTTP endpoints:** `ProviderConfig` (`crates/aragent/src/config.rs:36-43`), `wire_base_url` / `wire_provider` (`runtime.rs:3009-3465`), fixture-тест `9router` (`http://127.0.0.1:20128/v1`, `runtime.rs:7635`). Не хватает только **управления lifecycle, изоляции секретов, ownership процессов и health**.
- **`9router` (Node/Next.js `0.5.123`, MIT):**
  - Default bind **`0.0.0.0`** (`cli.js:120`), default password **`123456`** (`settings.js:16`), fallback **`API_KEY_SECRET = 'endpoint-proxy-api-key-secret'`** (`apiKey.js:3`).
  - **Поправка ревью (`ResearchReviewCoreEcosystem`):** `validateApiKey` в `.next-cli-build/server/middleware.js:2` дополнительно проверяет ключ по таблице активных ключей `apiKeys`, поэтому подпись HMAC сама по себе **не даёт мгновенного обхода авторизации**; однако дефолтный `0.0.0.0` + `123456` остаются недопустимыми на мобильном устройстве и требуют явного переопределения `127.0.0.1` и секретов.
  - Health endpoint в bundle — `/api/health` (вместо `/health` в доках).
- **`9router-termux` (`~/9router-termux`):**
  - Не имеет `.git`; `patch-9router.mjs` правит **минифицированные чанки** Next.js (14.4 KB, ~20 замен), из-за чего ломается молча при апдейте.
  - **Критический баг патча (`patch-9router.mjs:66-75`, вскрыто ревью):** превращает отказ по неподдержанным Gemini-инструментам в warning и **молча отбрасывает `content-parts`/неподдержанные tool calls** → потеря действий модели. **Вердикт: No-Go** для coding-агента; неподдержанные входы должны падать fail-closed.
  - `gw2a.sh` даёт рабочий прецедент для статических Go-роутеров в Termux: `proot -b $PREFIX/etc/resolv.conf:/etc/resolv.conf` + `SSL_CERT_FILE=$PREFIX/etc/tls/cert.pem`.
- **`9routerx` (Go `v1.5.6`, MIT-текст) и `CLIProxyAPI` (Go, MIT) / `CLIProxyAPI-Rust` (`v0.3.2`, Unlicense):**
  - `9routerx` отстал от `decolua/9router` на **38 версий** (`0.5.85` vs `0.5.123`) и не имеет Android release assets; Go static `CGO_ENABLED=0` требует проверки DNS/TLS на Bionic (через `proot` или CGO-сборку).
  - `CLIProxyAPI` биндится на все интерфейсы по умолчанию и включает свой `request-retry: 3`.
  - `CLIProxyAPI-Rust` — перспективен (чистый `rustls`/`axum`), но молод и без Android CI.
- **`OmniRoute` (Node, MIT, `v3.8.55`):** донор **архитектурного паттерна** (`ServiceSupervisor`, `adopt`, `DISABLE_MITM=true`, `127.0.0.1`), но `--max-old-space-size=6144` на телефоне недопустим (снижать до 512–1024 MB).

### 2.2 Стратегия форка и синхронизации с апстримом

| Паттерн | Суть | Вердикт |
|---|---|---|
| **Pattern A: Zero-patch upstream + supervisor в `arag`** | Пин версии/SHA (`npm` integrity или `SHA256SUMS`), запуск как child/on-demand с явными `127.0.0.1`, случайным `API_KEY_SECRET`/паролем, `GOMEMLIMIT`/`max-old-space-size`, pre-upgrade snapshot БД | **Рекомендуется по умолчанию.** Нулевые merge-конфликты, чистые лицензионные границы |
| **Pattern B: Тонкий форк (<100 LoC в исходниках)** | Патч-очередь только в **исходниках** (никогда в minified bundle!), CI rebase + fail-fast тест на применение патча | **Только при блокирующих багах upstream** |
| **Pattern C: Протокольный trait `GatewayBackend`** | Тонкий lifecycle-контракт поверх существующего `OpenAiCompatibleProvider` (`probe`, `spawn_spec`, `stop`) без создания нового wire-слоя | **Go после Фазы 0** (сейчас уже работает явный `baseUrl` в `models.yml`) |

### 2.3 Инварианты безопасности и отказов при интеграции роутера (с поправками ревью)

1. **Identity-checked adoption:** запрещено усыновлять уже работающий процесс только по `200 OK` на `/health` — проверять идентичность процесса/владельца/порта; никогда не останавливать чужой процесс без подтверждения ownership.
2. **Один владелец БД и токенов:** никогда не запускать два роутера (напр. `9router` + `9routerx`) на одной `data.sqlite` и не смешивать её с `agent.db`. Перед любым обновлением версии — бэкап `data.sqlite` + `WAL`/`SHM` при остановленном процессе.
3. **OAuth refresh race (`runtime.rs:1964–1982, 4369–4396`):** если используется gateway-ключ и нет явного root-ключа, store-callback может подтягивать хранимый токен и конкурировать со шлюзом. Для gateway-провайдеров явно отключать `CredentialCallback` и тестировать обход stored credentials.
4. **Единый владелец retry:** отключать повторы на одном уровне (`model.requestTimeout.maxRetries = 0` в `arag` **либо** `request-retry: 0` в CLIProxy). После первого SSE-чанка повторов нет (`provider/timeout.rs`).
5. **D27 redaction и denied paths:** регистрировать gateway/management/master ключи в `known_secrets`; внести в D27 denylist реальные конфигурируемые `DATA_DIR` (включая `WAL`/`SHM`, backups, `.cli-proxy-api`, `.9routerx`, `.omniroute`). Помнить: D27 не защищает от чтения сырой SQLite через `bash`.
