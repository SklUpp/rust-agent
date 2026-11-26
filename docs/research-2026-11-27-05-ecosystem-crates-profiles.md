# Track 5: Экосистема крейтов, проектов и адаптивные Android-профили (2026-11-27)

> **Источники:** `ResearchEcosystemProviders2` (обе части: снятие ограничений по сборке + строгий отбор под Android) + аудит `ResearchReviewCoreEcosystem`.
> **Базовые якоря:** `~/rust-agent/Cargo.toml`, `~/rust-agent/docs/modular-reuse-blueprint.md`, `crates/aragent/src/wake_lock.rs:1–90`, `crates/pi-agent/src/{tls.rs:1–56, tools/process.rs:44, tools/task_isolation.rs:50}`, `crates/pi-ast/src/language/mod.rs:10`.
> **Примечание:** уже интегрированные крейты (`gix`, `fastembed`, `tree-sitter-tags`, `ruzstd`, `xxhash-rust`, `rmcp`, `brush`, `ratatui`, `rusqlite`) исключены.
> **Границы проверки:** только инспекция лицензий, исходников и `Cargo.toml`; сборки новых крейтов под NDK и замеры RAM/батареи на устройстве не запускались.

---

## 1. Кандидаты на интеграцию в `arag` (с поправками лицензий и ревью)

| Домен | Кандидат | Лицензия / статус | Что даёт и точка интеграции | Вердикт и поправки ревью |
|---|---|---|---|---|
| **Плагины / WASM** | `wasmi` | MIT / Apache-2.0 | Интерпретатор WASM без Cranelift JIT (`hooks` / `tools` registry); избегает зависимости от executable JIT-страниц под W^X на Android `[INFERENCE]` | **Go с жёсткими лимитами:** сам по себе не является песочницей — обязательны host capabilities, лимиты fuel, памяти и прерывания |
| **Policy / Approvals** | `regorus` / `cedar-policy` | MIT / Apache-2.0 | Декларативные правила вместо разрастающихся regex-эвристик в `pi-shell` / `approve.rs` | **Go (on-demand):** без сети и C++; нужна чёткая диагностика причин отказа для UX |
| **Fuzzy-поиск** | `frizbee` | MIT | Быстрый SIMD/NEON fuzzy matching (замена/ускорение локального поиска) | **Go:** малый вес, пермиссивная лицензия |
| **Эмулятор терминала** | `wezterm-term` | MIT | Экранная эмуляция поверх PTY (`vim`/`top`/`ssh` в `tools/process.rs:44`) | **Opt-in:** ограничивать scrollback (напр. 2–3k строк) для удержания RAM на телефоне |
| **OAuth / TLS-фингерпринт** | `wreq` / `rquest` | Apache-2.0 (`wreq`) | Chrome/JA3 impersonation в `pi-agent/src/tls.rs:1–56` (OAuth-логин без Go sidecar) | **Verify Android first:** тянет BoringSSL и нативные build-скрипты; совместимость с Bionic не доказана, у `rquest` нужна проверка pin/lineage |
| **Глубокая семантика кода** | `ra_ap_*` / `stack-graphs` / `oxc_*` | MIT / Apache-2.0 | Реальный resolve/rename (Rust), multi-lang def/ref (`pi-ast`), JS/TS type-aware codemods | **Только desktop / opt-in:** `ra_ap_*` (30+ крейтов) ломает API каждые ~6 недель; `stack-graphs` даёт ~×3 к индексу |
| **Merge / VCS** | `jj-lib` / `gix-merge` | Apache-2.0 | Conflict-aware tree-level 3-way merge (`task_isolation.rs:50`, `pi-vcs`) | **Defer:** `jj-lib` приносит свой формат репозитория; `similar` + `pi-vcs` уже закрыли R7 |
| **Браузер / E2E** | `chromiumoxide` | MIT | Web-login провайдеры, визуальная проверка UI | **Desktop only:** на Android требует Termux-X11 / proot и тяжёлый runtime |
| **Локальный ML** | `candle` | MIT / Apache-2.0 | Локальный rerank/классификация без C++/NDK (чище для Bionic, чем ONNX Runtime) | **Opt-in:** только при реальной задаче |
| **Подсветка / Поиск** | `syntect` / `tantivy` | MIT | Подсветка TUI / BM25-фасеты | **Defer:** FTS5 уже закрывает R5; второй индекс создаст рассинхрон |
| **Аналитика / Vector** | `datafusion` / `usearch` | Apache-2.0 | SQL-аналитика по `agent.db` / HNSW | **Defer:** `usearch` = C++/cmake риск на NDK; `datafusion` тяжёл без запроса от продукта |
| **Vector в SQLite** | `sqlite-vec` | Pre-1.0 (проверять pin) | Векторный поиск без отдельного процесса | **Defer (поправка ревью):** документированный KNN — **brute-force**, он не гарантирует низкий RAM (SQLite page cache, temp allocs, IO/нагрев). Не внедрять до замеров на Android |
| **AI Gateway (Rust)** | `tensorzero` | Apache-2.0 | Embedded Rust gateway | **No-Go:** репозиторий **archived** (2026-06) |
| **Fuzzy matcher** | `nucleo` | **MPL-2.0** | Fuzzy поиск | **No-Go** при политике без copyleft-обязательств на модификации |
| **Go API Gateway** | `new-api` | **AGPL-3.0** | Multi-provider proxy | **No-Go** (лицензия) |
| **Android compat** | `landlock` / `hickory-resolver` | — | — | **Поправка ревью:** не отвергать категоричным ярлыком «всегда падает/ломает»; фиксировать как platform-specific compatibility risk и проверять пробой на устройстве |

---

## 2. Адаптивные профили на Android (`Pocket` / `Docked` / `Server`)

- **Паттерн реализации:** `device_profile.rs` по fail-open шаблону `crates/aragent/src/wake_lock.rs:1–90` (чтение `/proc/meminfo` `MemAvailable`, `/sys/class/thermal`, `termux-battery-status`) для гейтинга тяжёлых операций (`fastembed`, индексации, параллельности, sidecar).
- **Критическая поправка ревью:**
  - Числа вроде «Pocket < 400 MB RSS / 2 потока, Docked < 800 MB / 4 потока, `GOMEMLIMIT=192MiB`» являются **неизмеренными оценками**, а `GOMEMLIMIT` — мягкий ориентир сборщика Go, не жёсткий предел RSS.
  - **Сжатие вывода (token compression) небезопасно «автоматически даже для read-only»:** потеря строк в `grep`, diff или логах сборки вводит модель в заблуждение при правке кода. Разрешено только как **явный opt-in с маркировкой и доступом к оригиналу**.
  - **System-browser + loopback OAuth:** работает только если провайдер разрешает `127.0.0.1` redirect URI и сеть/браузер допускают callback; `state`/PKCE и loopback-bind обязательны.
  - **Отмена «Волны 1 как дешёвой и безрисковой»:** любая новая зависимость обязана проходить проверку лицензии, сборки под Android arm64, влияния на память/батарею и модели угроз.
