# Research: packaging (single binary, init dirs, native closure, Linux vs Android, notices, diagnostics)

Read-only investigation of the fork tree `/data/data/com.termux/files/home/omp-work`. Pins: original OMP `035e4f95c314f9aff2f5a779fa335422680f8e36`, fork `3008cfd0ca88bb0e28323561a165f39efa7159e1`. Nothing built, installed, or run. No product architecture chosen. Direct pinned URLs: §9.

## 0. Corrections vs the first revision of this report

1. **`strip = true`** не означает «backtraces нечитаемы безусловно». Исправлено в §3.2/§6: strip снимает таблицу символов/DWARF, backtrace всё равно захватывается; читаемость зависит от наличия отдельного symbol-артефакта и от того, какие кадры резолвятся через динамические символы. Утверждение «⇒ нечитаемые backtrace» снято.
2. **`sentry`** не противоречит D72 самим фактом наличия SDK. Исправлено в §4.4: D72 запрещает usage/analytics-телеметрию, а не error-reporting; возможна error-only конфигурация. Выбор (SDK / локальный отчёт / ручная отправка) не сделан.
3. **Nightly** — это выбор пина в исходниках, а не доказанная обязательность. Исправлено в §2.1/§8: `#![feature]` в замыкании `omp-cli` нет, MSRV-поля не выставлены глобально; сборка на stable не выполнялась, поэтому «stable не соберётся» — непроверенное.
4. Добавлены прямые pinned URL (§9).

## 1. Требования (из документов)

- **D71** (`docs/agent-architecture-decisions.md:325`): один основной бинарник на платформу; при установке/инициализации создаются рабочие каталоги, включая конфиги и инструменты; цели — Linux и Termux; Termux/Bionic требует отдельного артефакта, Linux ARM64 его не заменяет; «не требование хранить всё внутри исполняемого файла»; состав каталогов, пути, установка/обновление и матрица архитектур — открыты.
- **D72** (`:327-329`): только диагностические логи и возможность отправки отчётов об ошибках; **без телеметрии usage/analytics и общего экспорта активности**; защита секретов/чувствительных данных; не включать автоматически credentials, историю диалогов, содержимое файлов, полные tool payloads; состав безопасного отчёта, адресат, ручная/автоматическая отправка, настройки и retention — открыты; согласование функции не разрешает отправку сейчас. D72 описывает, **что** можно отправлять, и не запрещает конкретный клиент/транспорт.
- `docs/agent-architecture.md:15` повторяет D71.
- `docs/rust-cli-project-context.md:11`: «Installed `$PREFIX/bin/omp`, installer and existing Android release packaging remain unchanged» — существующая Bun-упаковка не является Rust-поставкой.
- `AGENTS.md:199` (TS-контракт логов): `~/.omp/logs/omp.YYYY-MM-DD.log` c автоматической ротацией; structured stdout держать чистым.
- `docs/environment-variables.md:594-605`: OTLP-экспорт только при явном endpoint, `OTEL_SDK_DISABLED=true` отключает; в Rust-бинаре отсутствует.
- Auto-update отложен; updater-зависимостей в крейтах нет (grep по корневому `Cargo.toml`: только `tree-sitter-dockerfile-updated`).

## 2. Факты исходников: single binary

### 2.1 Бинарник, состав, toolchain

- Один bin: `crates/omp-cli/Cargo.toml:16-18` — `[[bin]] name = "omp"`; lib `omp_cli`; workspace version `18.0.9` (`Cargo.toml:20`), license MIT, edition 2024, `resolver = "3"`.
- Прямые зависимости `omp-cli`: `async-trait, base64, clap, flate2 1.1, globset, grep-searcher, libc, pi-agent, pi-ast, pi-grep-pattern, pi-walker, reqwest, serde, serde_json, sha2 0.10.9, rusqlite 0.33.0 (bundled), tokio, tokio-util, xxhash-rust`. `pi-natives`, `pi-voice`, `pi-shell`, `pi-builtins` не подключены напрямую; `pi-shell`/`pi-builtins`/`brush-core` приходят транзитивно через `pi-agent` (`crates/pi-agent/Cargo.toml:24-30`).
- Следствие: N-API, arboard, pi-voice/opus/webrtc, pdf-inspector, zstd, `napi*`, `pi-natives/build.rs` в бинарь не входят. Browser/Chromium — внешняя зависимость.
- Вкомпилированные ассеты (`include_str!` из дерева TS-пакетов, а не из publishable crate-каталога): агенты `crates/omp-cli/src/agents.rs:22-31`; tool-описания `autolearn.rs:47-56`, `goal_tool.rs:22-23`, `acp/plan.rs:568-570`; goals `goal.rs:366-377`; 27 builtin-правил `local_cmd.rs:377-407`; snapshot настроек `config_cmd.rs:114-117` → `crates/omp-cli/settings-schema.snapshot.json` (генерируется `scripts/dump-settings-schema.ts`).
- **Следствие для публикации crate**: `cargo package/publish` для `omp-cli` в текущем виде невозможен — файлы по путям `../../../packages/...` лежат вне каталога пакета и в `.crate` не попадают (механика cargo; `[INFERENCE]`, архив не собирался). Поставка — только из полного дерева.
- **Toolchain, что доказуемо по source**: `rust-toolchain.toml` (`channel = "nightly-2026-08-08"`, `components = [rustfmt, clippy, rust-analyzer]`, `targets = ["x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc"]`) — пин выбирает nightly; `.cargo/config.toml:37-40` содержит `[unstable] embed-metadata = false` с комментарием о привязке к этому же nightly; MSRV объявлен только у `crates/pi-builtins/Cargo.toml:20` и `crates/vendor/brush-core/Cargo.toml:14` (`rust-version = "1.88.0"`), у workspace/`omp-cli`/`pi-agent` поля нет; `#![feature(...)]` внутри замыкания `omp-cli` не найдено (единственное nightly-атрибутное место — `pi-natives`, вне замыкания). **Обязательность nightly не доказана**: сборка на stable не выполнялась; `[unstable]`-таблица на стабильном cargo в рамках этого исследования не проверялась.
- Ресурсы, читаемые с диска в рантайме (точка расширения установщика): `dirs::share_dirs()` (`dirs.rs:191-220`) = `$OMP_SHARE_DIR`, `$PREFIX/share/omp`, `<exe>/../share/omp`, `~/.local/share/omp`, `/usr/local/share/omp`, `/usr/share/omp`; `catalog.rs:726-730` — `models.json`; `prompts.rs:50-53` — `system-prompt.md`. Порядок промптов (`prompts.rs:1-60`): `--system-prompt` > `<cwd>/.omp/SYSTEM.md` > `~/.omp/agent/SYSTEM.md` > `share/omp/system-prompt.md`; плюс `APPEND_SYSTEM.md`, `PERSONALITY.md`, `RULES.md`, `AGENTS.md`.
- `--version` = `CARGO_PKG_VERSION` (clap `version`, `args.rs:249-256`; ACP `acp/protocol.rs:398`; UA `auth/http.rs:184`). Git-хеша/`VERGEN` нет. Глобального `--verbose`/`--debug-log` нет (`-v/--verbose` — только подкоманда `ttsr`, `args.rs:1285`). Флага `--smoke-test` (есть у TS-бинаря, `docs/macos-signing-notarization.md:24-27`) нет.

### 2.2 Инициализация каталогов (состояние исходников)

Каталоги вычисляются в `crates/omp-cli/src/dirs.rs` (anchors — `packages/utils/src/dirs.ts`):

| Символ | Значение |
|---|---|
| `CONFIG_DIR_NAME` | `.omp` |
| `MAIN_CONFIG_FILENAMES` | `["config.yml", "config.yaml"]` |
| `base_config_root()` | `$PI_CONFIG_DIR` либо `~/.omp` |
| `config_root()` | профиль → `<base>/profiles/<name>`, иначе `<base>` |
| `agent_dir()` | профиль → `<base>/profiles/<n>/agent`; иначе `$PI_CODING_AGENT_DIR` либо `<base>/agent` |
| `root_subdir` / `agent_subdir` | XDG-перенаправление только при `cfg!(linux|macos)` и уже существующем `<XDG>/omp` (`dirs.rs:150-185`) |
| `project_config_dir(cwd)` | `<cwd>/.omp` |
| `worktrees_root()` | `<config root>/wt` или XDG data |
| `share_dirs()` | см. §2.1 |

Что создаётся и чем:

- `omp agents unpack` (`agents.rs:444-473`): 7 bundled-агентов в `<agent>/agents` по умолчанию; `--user`/`--project`/`--dir`, `--force`; `create_dir_all`, отказ на небезопасное имя, повторный запуск пропускает существующие.
- `omp config init-xdg` (`config_cmd.rs:1951-1982`): создаёт `XDG_DATA_HOME/omp`, `XDG_STATE_HOME/omp`, `XDG_CACHE_HOME/omp`; гейт `cfg!(linux|android|macos)`.
- `omp config set/reset`: создают родителя, берут `ConfigLock` (§3.3).
- Сессии/артефакты/blob/архив: по требованию при записи (`session.rs`, `output.rs`, `eval_host.rs:257`), под `<agent>/sessions/omp-cli` (дефолт `--session-dir`, `args.rs:219-221`).
- Процессы: `<agent>/processes/<runner-pid>/<name>/{output.log, output.previous.log}` (`pi-agent/src/tools/process.rs:72-73, 901-923, 1309`), права 0600, приватные каталоги (`:615-637`).
- Демон-брокер: `<agent>/run/daemons/<scope-hash>/` с `broker.sock`, `broker.token`, `broker.pid`, `scope.json`, `meta.json`, `clients/` (`pi-agent/src/process_broker/protocol.rs:28-52, 66-115`); при длинном пути — `$TMPDIR/omp-broker-<uid>/<hash>` (`:155-171`), лимит `MAX_SOCKET_PATH_BYTES = 104`.
- Секреты: `<config-dir>/auth-broker.token`, `<config-dir>/auth-gateway.token` 0600 (`docs/environment-variables.md:114,119`); credential-store каталог 0700, файл 0600 (`auth/store.rs:178-185`).
- Данные: `AgentDirCategory::{Data,State,Cache}` (`dirs.rs:94-126`).
- **Нет единой `omp init`**: D71-инициализация складывается из `agents unpack` + `config init-xdg` + ленивого создания при записи; `omp setup` требует TTY и опрашивает только `python|speech` (`local_cmd.rs:5510-5513`). Это факт, не предложение.

## 3. Native dependency closure (Linux и Android/Bionic)

### 3.1 C/asm в замыкании `omp`

| Компонент (lock-версия) | Откуда | Сборка |
|---|---|---|
| `libsqlite3-sys 0.31.0` (rusqlite `bundled`) | `omp-cli`, `pi-agent` | amalgamation SQLite через `cc` (+ опционально `pkg-config`/`vcpkg`) |
| `ring 0.17.14` + `rustls 0.23.43` | `reqwest` (`rustls-tls`; OpenSSL исключён намеренно, `Cargo.toml:463-471`) | C + asm через `cc`; на Android нужен NDK clang |
| `onig 6.5.3` / `onig_sys 69.9.3` | `pi-builtins` | vendored Oniguruma через `cc` |
| `pcre2 0.2.11` / `pcre2-sys 0.2.10` | `pi-builtins` (`grep-pcre2`) | vendored PCRE2 через `cc` |
| `tree-sitter 0.25.10` + 56 grammar-crates (`crates/pi-ast/Cargo.toml:19-75`) | `pi-ast` | C-исходники грамматик через `cc` |
| `portable-pty 0.9.0` | `pi-agent` | только `libc` (bionic `forkpty/openpty` с API 23) |
| `sha2 0.10.9` (+ `sha3` через uucore, feature `asm` на aarch64) | `pi-builtins` | asm-диспетчеризация |

Чисто Rust: `flate2 1.1 → miniz_oxide 0.8.9`, `brush-core` (vendored), `brush-parser`, `clap`, `ignore/globset`, `nix 0.31.2`, `procfs 0.18`, `notify 8.2.0`, `rustix`, `uucore 0.8`, `jiff` (tzdb bundled), `serde/serde_json`, `tokio`.

**Вне замыкания**: `bindgen`/`libspa-sys`/`system-deps` (pipewire, только pi-natives), `zstd-sys`, `cmake`/`audiopus_sys`/`opus`/`webrtc`, `arboard`, `resvg`, `pdf-inspector`, `napi*`. Проверено по манифестам и lock-зависимостям. Т.е. `cmake` и `libclang` для сборки `omp` не требуются.

### 3.2 Linux

- Release-артефакта нет: `rust-agent.yml` для x86_64 делает `cargo check/test -p pi-agent -p omp-cli` (+ support-крейты) и не собирает/не пакует release-Linux. glibc-floor не определён; musl-цели в CI нет.
- Профили (`Cargo.toml:32-49`): `[profile.release]` `opt-level=3`, `lto="fat"`, `codegen-units=1`, `strip=true`, `panic="unwind"` (обоснование unwind — восстановимые паники на boundary); `debug` в профиле не задан (=0 по умолчанию). `[profile.ci]` (`:51-58`): `lto="thin"`, `strip="symbols"`, `split-debuginfo="off"`, `debug=false`.
- **Символы в поставке (исправленная формулировка)**: `strip=true` убирает symbol table/DWARF собственных крейтов; `Backtrace::force_capture` при этом всё равно работает (донорский комментарий `crash_handler.rs:20-22`), но собственные кадры будут адресами/резолвиться лишь частично. Читаемость отчёта зависит от (а) наличия отдельного unstripped/`.sym`/split-debuginfo артефакта той же сборки, (б) линковки динамических символов. Android-сборка добавляет `-C debuginfo=0 -C strip=symbols` (`rust-agent.yml:116`), т.е. там символы тоже сняты до загрузки.

### 3.3 Android/Bionic

- `pi-shell/src/process.rs`: `#[cfg(any(linux, android))] mod platform` (`:20`), pidfd-поле и `open_pidfd` только `cfg(not(android))` (`:36-60, 312-325`); на Android — `start_time`-идентичность + `libc::kill` (`:53-59, 175-184`), `status()` по `/proc/<pid>/stat` (`:216-224`), `exited()` — 50 мс поллинг.
- `pi-builtins`: `proc_snapshot` на android (`proc_snapshot.rs:44`), `sigqueue`/`--queue` отключены (`:242-261`), `tail` = inotify на android (`tail.rs:364, 2795`), `kill` по `libc::SIGRTMAX()` (`kill.rs:643-648`), android-ветви в `ln.rs:661-683`, `ls.rs:567, 2340-2347, 3206`, `touch.rs:850-858`, `wc.rs:23-29`.
- `pi-natives`: arboard/pi-voice за `cfg(not(android))`, alloc-error-hook отключён (`android/docs/port-changes.md`, transforms 1-3). На `omp` не влияет.
- `std::os::linux::net::SocketAddrExt` — только под `#[cfg(target_os = "linux")]` (`config_cmd.rs:25-29`, `local_cmd.rs:18-22`); на Android компилируется fallback.
  - **Доказанный gap:** на Linux `ConfigLock`/`StatsSyncLock` = abstract Unix datagram `omp-file-lock-<xxh64>`; на Android = `${path}.lock` + `flock`. Комментарий `config_cmd.rs:2225-2231` фиксирует, что TS-писатель ведёт себя так же (abstract socket на Linux, flock «elsewhere, including Android»), т.е. паритет сохранён, но семантика разная (появляется файловый артефакт; поведение `flock` на FUSE/SAF не проверено).
- XDG: `init_xdg` включает Android (`config_cmd.rs:1952`), а `xdg_agent_root` — нет (`dirs.rs:150-152`, `cfg!(linux|macos)`). Каталоги создаются, но пин их не использует.
- Сокет брокера: лимит 104 байта (`protocol.rs:51-52`); Termux-путь длинный → shortened `$TMPDIR/omp-broker-<uid>/<hash>`, ошибка с подсказкой «Set a shorter PI_CONFIG_DIR or use a named profile» (`:376-387`). Упаковка обязана обеспечить writable `$TMPDIR` (Termux: `$PREFIX/tmp`).
- Android-сборка CLI (`rust-agent.yml:86-130`): NDK r27c, `API=${ANDROID_API:-24}` (из `android/versions.env`, `ANDROID_API=24`), env `CC_/AR_/CARGO_TARGET_*_LINKER/TARGET_CC/TARGET_AR`, `RUSTFLAGS="-C opt-level=3 -C debuginfo=0 -C strip=symbols -C link-arg=-fuse-ld=lld"`, `cargo build --release --locked -p omp-cli --target aarch64-linux-android`, затем `llvm-strip`, `file`-проверка `ELF 64-bit ... aarch64`, `sha256sum`.
- Расхождение пинов: `nightly-2026-08-08` (`rust-toolchain.toml`, `rust-agent.yml`) vs `RUST_TOOLCHAIN=nightly-2026-07-28` (`android/versions.env:5`, Bun-релиз). `rust-toolchain.toml` не содержит `aarch64-linux-android` (CI делает `rustup target add`).
- Известные ограничения устройства: API-24 floor; `pidfd_*` (API 34) и `sigqueue` не используются; `O_PATH` для execute-only предков (`docs/rust-cli-project-context.md:63-65`); нет X11/Wayland/AppKit/Win32-поверхностей.

## 4. Диагностика и отчёты об ошибках

### 4.1 Чего нет

- Логгера нет: ни `log`, ни `tracing` в `[workspace.dependencies]` (grep `Cargo.toml:300-475`), нет записи `~/.omp/logs/omp.*.log`, ротации, глобального `--verbose`. Диагностика идёт в stderr и в session JSONL.
- `pi-natives/src/crash_handler.rs` не линкуется в `omp` (pi-natives не зависимость, crate-type `cdylib`) ⇒ panic-hook для CLI отсутствует.
- OTel/экспортёры (`docs/environment-variables.md:594-605`) — только TS; в Rust нет `opentelemetry`, `sentry`, `env_logger` в зависимостях.

### 4.2 Что есть (донор)

- Process-log (`pi-agent/src/tools/process.rs`): `output.log` + `output.previous.log`, `MAX_LOG_BYTES = 25 MiB` (`:60`), чтение 2 MiB/вызов, модель видит ≤256 KiB, readiness-буфер 64 KiB; ротация `:616-619, 676-690`; 0600/0700.
- Session JSONL/artifacts — персистентная диагностика (`session.rs`, `eval_host.rs`), но транскрипт не является лог-файлом и не должен уходить в отчёт (D72).
- Grievances локально: `omp grievances list` читает `<data>/autoqa.db` таблицу `grievances` (`local_cmd.rs:2141-2259`); сетевой отправки нет (endpoint/consent только в схеме настроек: `dev.autoqa`, `dev.autoqaPush.endpoint = https://qa.omp.sh/v1/grievances`, `dev.autoqaConsent` — `settings-schema.snapshot.json:5538-5580`). Вывод экранирует terminal-control (`local_cmd/tests.rs:1389-1410`).

### 4.3 Redaction-примитивы в fork

| Символ | Файл | Покрытие |
|---|---|---|
| `omp_cli::auth::error::redact(&str) -> String` (pub) | `crates/omp-cli/src/auth/error.rs:180-320` | JSON-поля `access_token/refresh_token/id_token/token/secret/password/code/code_verifier/key`, URL-query секреты, JWT `eyJ…`, `Bearer`/`sk-`/`sk_`/`xai-`/`nvapi-`/`gho_`/`csk-`/`fw_`/`bt_`/`fpk_` |
| `auth::error::summarize_error_body` | `auth/error.rs` | тело token-endpoint → только `error/message` |
| `local_memory::redact_secrets(&str) -> String` (pub) | `local_memory.rs:136-155` | `sk/pk/rk/tok/key/secret/token/password`, JWT, `AKIA/ASIA`, `ghp_`, `github_pat_`, `npm_`, `xox[baprs]-`, `AIza` |
| `config_cmd::REDACTED = "********"` + флаг `credential` | `config_cmd.rs:43-44, 1825-1860` | JSON `{"redacted": true}`, human `********` |
| `sanitize_yaml_error` | `config_cmd.rs:2210-2214` | убирает backtick-фрагменты |
| `omp cleanse --confirm` | `main.rs:4228-4265` | перезапись JSONL через `auth::error::redact` |
| TS-аналоги (паритет) | `docs/secrets.md`, `docs/install-id.md`, `AGENTS.md:199` | secrets-obfuscator (`secrets.enabled`/`secrets.yml`), `share.redactSecrets`, `logger` с ротацией. **В Rust отсутствует** (grep `secrets.yml|secrets.enabled` — 0 в `crates/`) |

- Донор `crash_handler.rs` **не редактирует** payload паники: `format_panic_report` пишет `message: {panic_payload}` и полный backtrace в `native-<kind>-<pid>-<ts>.log` (`:159-197, 240-300`). Для D72 требует фильтра через `redact`.

### 4.4 Готовые Rust-примитивы (metadata проверена)

| Crate | Версия | License | MSRV | В `Cargo.lock` | Примечание |
|---|---|---|---|---|---|
| `tracing` | 0.1.44 | MIT | 1.65 | **да** (транзитивно; workspace-зависимости нет) | facade |
| `tracing-core` | 0.1.36 | MIT | — | да | |
| `tracing-subscriber` | 0.3.23 | MIT | 1.65 | нет | features `fmt`, `env-filter` (→`matchers`,`regex-automata`), `json` (→`tracing-serde`), `ansi` (→`nu-ansi-term`, уже в lock), `registry` (→`sharded-slab`,`thread_local` — **в lock отсутствуют**) |
| `tracing-appender` | 0.2.5 | MIT | 1.63 | нет | daily rolling + non-blocking writer; `crossbeam-channel`, `time 0.3.55` уже в lock |
| `log` | 0.4.34 | MIT/Apache-2.0 | — | да | `env_logger 0.11.11` тоже в lock (через `pdf-inspector`) |
| `color-eyre` | 0.6.5 | MIT OR Apache-2.0 | 1.65 | нет | panic/eyre report; **0.6.4 yanked**; default `capture-spantrace` тянет `tracing-error`+`color-spantrace` |
| `human-panic` | 2.0.8 | MIT OR Apache-2.0 | **1.88** | нет | минимальный panic-report; default feature `color` → `anstyle`/`anstream` |
| `redact` | 0.1.11 | MIT/Apache-2.0 | 1.65 | нет | `Redacted<T>`/`RedactedString` («не печатать секрет»), не regex-скраббер логов |
| `redactable` | 0.15.2 | не проверено | — | нет | derive-редакция полей |
| `zeroize` | 1.9.0 | MIT/Apache-2.0 | — | **да** | затирание в памяти (feature `redact/zeroize`) |
| `sentry` | 0.49.5 | MIT | — | нет | SDK = клиент событий; D72 не запрещает error-reporting. Default features `backtrace, contexts, debug-images, logs, metrics, panic, transport, release-health`; `transport` → `reqwest`+`native-tls`, есть `rustls`/`rustls-no-provider`. Error-only сборка возможна только с урезанным feature-set (без `metrics`, `logs`, `release-health`); вопрос DSN/consent/redaction не решён. Выбор SDK vs локальный отчёт vs ручная отправка не сделан |

По объёму: `tracing + tracing-subscriber(fmt,env-filter) + tracing-appender` добавит lock-записи `sharded-slab`, `thread_local`, `tracing-log`, `matchers` (+`tracing-serde` при `json`); остальное уже присутствует. Альтернатива — собственный append-only writer + существующий `redact`, без новых зависимостей, но с ручной ротацией/уровнями. Выбор не в этом блоке.

## 5. Лицензии и notices

- Первичная: `LICENSE` MIT, workspace `license = "MIT"`; `pi-natives`/`pi-shell`/`pi-ast`/`pi-builtins` тоже MIT.
- `THIRD-PARTY-NOTICES.txt` (22 909 строк) — «generated, self-contained aggregate accompanies OMP release artifacts; does not replace package-level license files»; содержит `crates/vendor/brush-core/LICENSE` (brush, MIT, перечень локальных модификаций) и `crates/pi-builtins/LICENSE` (brush-builtins + uutils-порты + jaq). Оба крейта в замыкании `omp` ⇒ notice обязателен.
- `about.toml` — cargo-about **0.8.2**; accepted: `MIT, Apache-2.0, Apache-2.0 WITH LLVM-exception, BSD-2-Clause, BSD-3-Clause, ISC, Unicode-3.0, Zlib, 0BSD, BSL-1.0, CC0-1.0, MIT-0`; package-scoped: `inferno → CDDL-1.0`, `terminfo → WTFPL`, clarify `tree-sitter-graphql 0.1.0 → MIT` (`license-files` + checksum).
- `deny.toml` — cargo-deny **0.20.2**: `targets = []`, `all-features = true`, `yanked = "deny"`, `unknown-registry/unknown-git = "deny"`, `unused-allowed-license = "deny"`; exceptions `inferno@0.12.8 → CDDL-1.0`, `terminfo@0.9.0 → WTFPL`, `uluru@3.1.0 → MPL-2.0`.
- **Пробел:** `cargo about`/`cargo deny` не вызываются ни в одном workflow (grep `.github/`, `package.json`, `scripts/` — 0). Нет gate, проверяющего наличие `LICENSE`+`THIRD-PARTY-NOTICES.txt` в артефакте. Для Bun-пакетов контракт есть только в TS (`scripts/ci-release-publish.ts:84-88`).
- `[INFERENCE]`: `uluru` (MPL-2.0) идёт через `pi-vcs`→`gix`, `inferno`/`terminfo` — через pi-natives/uutils; в замыкании `omp-cli` их, вероятно, нет. Точный набор требует `cargo tree -p omp-cli`, не выполнялось.

## 6. Доказанные platform/evidence gaps

1. **Логов/отчётов в Rust нет**: нет логгера, ротации, panic-hook, объекта отчёта (D72 не реализован на стороне бинаря).
2. **Crash-донор без redaction** и не линкуется (napi-only crate).
3. **Символы сняты в релизе** (`strip=true`, Android `-C strip=symbols`): отчёт будет в основном адресным, если не поставлен отдельный symbol-артефакт; это вопрос поставки символов, не невозможности отчёта.
4. **Android lock divergence**: abstract-socket vs `${path}.lock`+flock.
5. **XDG pin выключен на Android** при включённом `config init-xdg`.
6. **Длинные пути Termux** → broker.sock >104 байт, нужна shortened `$TMPDIR`-база.
7. **Два Rust-пина**; `rust-toolchain.toml` без Android-трипла.
8. **Нет Linux-релиза и glibc-floor**.
9. **Нет license/notices gate**; `deny.toml`/`about.toml` не исполняются.
10. **secrets-obfuscator отсутствует** в Rust ⇒ «защита секретов» опирается только на `auth::error::redact`/`local_memory::redact_secrets`/`config_cmd` redaction.
11. **Нет `--smoke-test` и build-stamp**: пакетный smoke требует prompt-прогона (credentials/сеть) либо read-only подкоманд.
12. **crate-публикация невозможна** из-за `include_str!` вне каталога крейта (`[INFERENCE]`).

## 7. Предлагаемая CI/smoke матрица (proposal, не решение)

Факт: `rust-agent.yml` — 3 джобы (`test-host` x86_64 check/test `pi-agent`+`omp-cli` + support-крейты; `test-native-grep`; `build-android`: NDK r27c, API 24, release-сборка, `file`, `sha256sum`, upload). Нет Linux-артефакта, проверки на устройстве, license/notices, ELF-зависимостей.

1. **Artifact contract (Linux+Android, общие шаги):** `cargo build --release --locked -p omp-cli --target <triple>`; `file`; `readelf -d` c whitelist `DT_NEEDED` (`libc`, `libdl`, `libm`, `liblog` на Android; минимальный glibc-набор на Linux); отсутствие `libssl/libcrypto`; `--version` == workspace version; `LICENSE`+`THIRD-PARTY-NOTICES.txt` рядом; tarball + `sha256sum`; отдельный `.sym`/unstripped-артефакт, если отчёты должны быть символическими.
2. **Offline smoke (no credentials, no network):** `omp --version`; `omp completions bash`; `omp config path`; `omp agents unpack --dir <tmp>` (7 файлов); `omp read <fixture>`; `omp config set/get` (lock-путь); `omp gc` dry-run; `omp grievances` (пустая БД).
3. **Matrix:** Android `aarch64-linux-android` API 24 + Linux `x86_64-unknown-linux-gnu`; опционально Linux aarch64 (требует решения о glibc-floor/musl/zig — не принимается здесь).
4. **License gate:** `cargo-deny check licenses` + `cargo-about` для `cargo tree -p omp-cli --edges normal,build` и дифф с закоммиченным notices-файлом.
5. **Устройство (self-hosted/manual):** скачать артефакт, `sha256sum -c`, `--version`, offline-smoke п.2 на реальном Termux (Bionic, `$TMPDIR`, длинные пути, `flock`, `forkpty`), затем один промпт с изолированным `HOME`.
6. **Диагностика (после решения D72):** panic-инъекция по env-флагу и проверка, что отчёт лежит в `<agent>/logs/` с 0600 и **не** содержит fixture-секретов (`sk-test…`, JWT) — тест redaction, не тест отправки.

Ограничение: п.4-6 зависят от открытых D71/D72 и не являются утверждёнными.

## 8. Непроверенное / ограничения

- Ничего не собрано: link-зависимости, размер, glibc-floor, поведение Bionic — из манифестов/lock/CI-конфигов, не из `cargo tree`/`readelf`/запуска.
- **Nightly vs stable**: пин выбирает nightly; `#![feature]` в замыкании `omp-cli` нет; сборка на stable не выполнялась; поведение `[unstable] embed-metadata` на стабильном cargo не проверялось ⇒ «требуется nightly» — предположение, не доказательство.
- `[INFERENCE]` про `cargo publish` и про фактический набор лицензий в бинаре.
- Не проверено на устройстве: `flock` на shared storage, `$TMPDIR`-shortening на реальном Termux, `portable-pty` под CLI-сессией Android.
- Матрица архитектур не определена; Windows/macOS Rust-артефактов в CI нет.
- Auto-update не исследовался (по заданию); updater-зависимостей нет.
- Отчёт не выбирает: crate для логов, формат/адресата отчёта, каталог логов, отправку, состав notices-пакета, поддержку XDG на Android.

## 9. Прямые pinned URL

Репозитории: **U** = `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36`; **F** = `https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1` (slug форка взят из CI-ссылок `docs/rust-cli-parity-plan.md`; в этом проходе URL не открывались — собраны из пинов).

Воркспейс/политика:
- `U/Cargo.toml`, `U/rust-toolchain.toml`, `U/.cargo/config.toml`, `U/deny.toml`, `U/about.toml`, `U/LICENSE`, `U/THIRD-PARTY-NOTICES.txt`, `U/AGENTS.md`
- `U/docs/agent-architecture-decisions.md`, `U/docs/agent-architecture.md`, `U/docs/environment-variables.md`, `U/docs/secrets.md`, `U/docs/install-id.md`, `U/docs/macos-signing-notarization.md`, `U/android/versions.env`, `U/android/docs/port-changes.md`, `U/.github/workflows/rust-agent.yml`
- `F/docs/rust-cli-project-context.md`, `F/docs/rust-cli-parity-plan.md` (в `omp-work` присутствуют; если в U их нет — использовать F)
- `U/crates/pi-natives/src/crash_handler.rs`

Fork-крейты (Rust CLI):
- `F/crates/omp-cli/Cargo.toml`, `F/crates/omp-cli/src/{dirs.rs,config_cmd.rs,local_cmd.rs,agents.rs,prompts.rs,args.rs,auth/error.rs,local_memory.rs}`, `F/crates/omp-cli/settings-schema.snapshot.json`
- `F/crates/pi-agent/Cargo.toml`, `F/crates/pi-agent/src/tools/process.rs`, `F/crates/pi-agent/src/process_broker/protocol.rs`
- `F/crates/pi-shell/src/process.rs`, `F/crates/pi-shell/build.rs`, `F/crates/pi-builtins/Cargo.toml`, `F/crates/pi-ast/Cargo.toml`, `F/crates/vendor/brush-core/Cargo.toml`

Библиотеки (metadata, использованная в §4.4):
- `https://crates.io/api/v1/crates/tracing` → `https://crates.io/crates/tracing/0.1.44`, `https://docs.rs/tracing/0.1.44`
- `https://crates.io/crates/tracing-subscriber/0.3.23` (features сняты из `https://crates.io/api/v1/crates/tracing-subscriber`)
- `https://crates.io/crates/tracing-appender/0.2.5`
- `https://crates.io/crates/color-eyre/0.6.5` (0.6.4 yanked — `https://crates.io/api/v1/crates/color-eyre`)
- `https://crates.io/crates/human-panic/2.0.8`
- `https://crates.io/crates/redact/0.1.11`, `https://crates.io/crates/redactable/0.15.2`
- `https://crates.io/crates/zeroize/1.9.0`
- `https://crates.io/crates/sentry/0.49.5`, features: `https://crates.io/api/v1/crates/sentry`
- `https://doc.rust-lang.org/cargo/reference/config.html` (проверялось для `[unstable]`; отдельного утверждения о поведении stable cargo в отчёт не вносится)
