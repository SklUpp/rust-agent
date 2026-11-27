# Исследование: разделение кода по платформам (`pi-sys` HAL) и максимальная доступность инструментов по OS

Дата: 2026-11-27  
Статус: исследование (`ScoutOsPlatformSplit`, только чтение кода) — не нормативное требование.

---

## 1. Аудит текущих `#[cfg]` и платформенных предположений

### Фактическая модель `cfg`
- В Rust `target_os = "android"` **не** включает `target_os = "linux"` (`unix = true`). Любой голый `#[cfg(target_os = "linux")]` отрезает Android/Termux.
- Сейчас `#[cfg(target_os = "android")]` встречается в ~30 файлах, а `cfg(any(target_os = "linux", target_os = "android"))` продублирован ≥15 раз (`config_cmd.rs:2221`, `eval_host.rs:1152`, `tui.rs:229`, `process_broker/broker.rs:736`, `protocol.rs:604`, `atomic_publish.rs:50`, `history_files.rs:46`, `read.rs:97,1386`, `pi-builtins/Cargo.toml:150`, `kill:644`, `ps:1247`, `sort:148`, `tail:364`, `umask:64`, `wc:23`, `brush-core`).
- `pi-natives` и `pi-voice` не входят в `[workspace].members` (`Cargo.toml:2-13`).
- `rust-toolchain.toml:4` перечисляет `x86_64-pc-windows-msvc`, но `.github/workflows/standalone-runtime.yml` собирает только `x86_64-unknown-linux-gnu` и `aarch64-linux-android` (macOS/Windows CI-джоб нет).

### Удачные шаблоны (сохранить как основу HAL)
- `crates/pi-iso`: каждый бэкенд разделён на реализацию + `cfg(not(...))` stub с `ProbeResult::unavailable(reason)` (`apfs.rs:14-48`, `overlayfs.rs:21-65`, `linux_reflink.rs:17-63`).
- `crates/pi-shell/src/process.rs`: обход `pidfd` на Android (`:36-53`), kill по pgid со сверкой `start_time` (`:176-225`), выделенные модули `platform` (`:20,366,740`).
- `crates/aragent/src/tui.rs:206-244`: корректный `errno` (`__errno` на Android Bionic vs `__errno_location` на glibc) + save/restore в обработчике `SIGWINCH`.
- `config_cmd.rs:2506-2595`, `local_cmd.rs:4429-4495`: abstract Unix socket на Linux и `flock(0600)` на прочих Unix.
- `process_broker/protocol.rs:52,160-166,376-390`: учёт лимита `sun_path` 104 байта и укороченной базы на Android.
- `wake_lock.rs:61-90,206-245`: мягкая деградация при отсутствии `termux-wake-lock`.

### Обнаруженные пробелы и конфликты

| Приоритет | Проблема | Анкоры и следствие |
|---|---|---|
| **P0-1** | `aragent` не компилируется на Windows | Безусловные `std::os::unix::*` на уровне модулей в `local_cmd.rs:20,22`, `config_cmd.rs:27`, `rpc/mode.rs:10`; при этом в `tui.rs:26-34`, `diagnostics.rs:1143-1153`, `config_cmd.rs:2221` лежат мёртвые `#[cfg(not(unix))]`-стабы |
| **P0-2** | Закрытый набор ОС в `pi-shell` и `pi-builtins` | `pi-shell/src/process.rs` и `pi-builtins/src/proc_snapshot.rs:44,377,679` (`ps.rs:1247`, `kill.rs:644`) покрывают только 4 ОС; BSD/illumos не компилируются (`E0433`) |
| **P1-3** | Два расходящихся `env`-allowlist | `pi-agent/src/state.rs:1615-1631` vs `mcp/transports/stdio.rs:60-79`: оба без Windows-переменных (`SystemRoot`, `ComSpec`, `PATHEXT`, `APPDATA`, `USERPROFILE`) и без `ANDROID_ROOT`/`ANDROID_DATA`/`TERMUX_*`; MCP-транспорт и `bash` получают разное окружение |
| **P1-4** | Сломанный fallback `temp_dir()` на Android | `std::env::temp_dir()` на Android возвращает `/data/local/tmp` (недоступен app-UID); `mktemp.rs:50,547` падает в `"/tmp"`, `process_broker/protocol.rs:160-166` и `main.rs:3809-3816` опираются на `std::env::temp_dir()` / `/tmp` вместо `$TMPDIR → $PREFIX/tmp` |
| **P1-5** | Раздвоенный XDG на Android | `dirs.rs:151` применяет XDG только на `linux \|\| macos`, а `config_cmd.rs:2221` включает `android` |
| **P1-6** | `pi-iso` на Android зашит в `rcopy` | `pi-iso/src/lib.rs:21-25,118-125,140,378-382` (`FALLBACK_AUTO_ORDER = [Rcopy]`), хотя Termux имеет `fuse-overlayfs`, а ядра Android с f2fs могут поддерживать `FICLONE` (`linux_reflink.rs:17`) |
| **P1-7** | Нет пробы shell в `bash`/`eval` | `tools/mod.rs:399-402,475-478` жёстко берут `SHELL` / `"sh"` без предварительной пробы доступности (в отличие от `ssh.rs:415-432`) |
| **P1-8** | Хардкод `ENAMETOOLONG == 36` | `tools/read.rs:1378-1389` проверяет `raw_os_error() == Some(36)` (на macOS/BSD это `EINPROGRESS`, `ENAMETOOLONG` там 63) |
| **P1-9** | Конфляция `linux` vs `android` в `find` и PCRE2 JIT | `find.rs:4470-4475` ограничивает проверку birthtime одним `linux`; `pi-builtins/src/grep.rs:25-31` и `pi-grep-pattern/src/lib.rs:21-26` выключают PCRE2 JIT только на macOS, хотя на Android также действует `W^X` |
| **P2-10** | Дубли `renameat2` и `FileLock` | Две копии `renameat2(RENAME_NOREPLACE)` (`local_cmd.rs:3309-3349` vs `atomic_publish.rs:50-85`) и две копии abstract-socket/flock (`config_cmd.rs:2506` vs `local_cmd.rs:4429`) |
| **P2-11** | Env-less shell теряет `$PREFIX/bin` | `pi-shell/src/shell.rs:943` (`do_not_inherit_env(true)`) + `ANDROID_DEFPATH` в `brush-core` без `$PREFIX/bin` |
| **P3** | Мелкие утечки | `tail.rs:1560` (`linux` guard); `vendor/rmcp/build.rs:10-20` (модификация `.git/config`) |

---

## 2. Матрица доступности инструментов по OS

`pi_agent::tools::builtin_tools()` (`tools/mod.rs:4010-4033`) не содержит платформенных `#[cfg]` — базовые инструменты (`bash`, `read`, `write`, `edit`, `glob`, `grep`, `ast_grep`, `ast_edit`, `todo`) регистрируются безусловно. Дополнительные (`ask`, `lsp`, `web_search`, `eval`, `checkpoint`/`rewind`, `think`/`yield`, `task`, `hub`, `ssh`, `fetch`, `memory`) подключаются в `aragent` по конфигу/сессии. Единственное явное исключение — `EXCLUDED_TOOLS` для `computer` (`registry.rs:12-19`).

Обозначения: `✓` работает; `✓c` условно по конфигу/сессии; `✗` заблокировано сборкой/платформой; `?` требует проверки в рантайме.

| Инструмент | Гейт сегодня | Linux (glibc/musl) | Android / Termux | macOS (без CI) | Windows (нет CI) | Главный блокер максимума |
|---|---|---|---|---|---|---|
| `read`, `write`, `edit`, `glob`, `grep`, `ast_grep`, `ast_edit`, `todo` | builtin, без cfg | ✓ | ✓ | ✓ | ✗ | Сборка `aragent` на Windows (P0-1); `ENAMETOOLONG=36`; PCRE2 JIT на Android; `renameat2` дубль |
| `bash` | builtin (`SHELL`/`sh`) | ✓ | ✓ (`$SHELL`) | ✓ | ✗ | Нет ветки `cmd /C` / PowerShell и runtime-пробы shell |
| `process` | условно (PTY + pid tree) | ✓ | ✓ (без `pidfd`) | ✓? | ✗ | P1-4 `temp_dir`, сборка Windows |
| `eval` | `eval_host.rs:163` | ✓ | ✓ (`eval/kernel.rs:42`) | ✓ | ✗ | `setsid`/сигналы, env-allowlist |
| `task` | изоляция worktree | ✓ | ✓ (`rcopy`) | ✓ (`apfs`/`rcopy`) | ✗ | На Android `pi-iso` зажат в `rcopy` вместо `fuse-overlayfs`/`FICLONE` |
| `hub` | broker / unix-socket | ✓ | ✓ (`$TMPDIR`, ≤104B) | ✓ | ✗ | `sun_path` и Windows IPC |
| `ask` | `ask_available` (`runtime.rs:5143`) | ✓ | ✓ | ✓ | ✗ | Unix TTY-логика (`approve.rs:900-1010`) |
| `lsp` | `runtime.rs:1851` | ✓ | ✓ | ✓ | ✗ | Unix `process_group` + `lspmux` |
| `dap` | `dap/tool.rs:134` | ✓ | ✓ | ✓ | ✗ (есть TCP fallback) | Сборка `aragent` |
| `ssh` | `ssh.rs:415` (проба PATH) | ✓ | ✓ | ✓ | ✗ (`kill_group` no-op) | `kill_group` на Windows |
| `web_search`, `fetch`, `mcp`, `memory` | конфиг/сессия | ✓ | ✓ | ✓ | ✗ | Сборка + единый `INHERITED_ENV` для MCP stdio |
| `voice` | крейт отсутствует | — | — | — | — | `pi-voice` вне workspace |
| `vcs` (`pi-vcs`) | библиотека, не инструмент | ✓ (lib) | ✓ (lib) | ✓ (lib) | ✗ | `git` spawn/kill unix-only |
| `computer` | `EXCLUDED_TOOLS` (`registry.rs:12`) | ✗ | ✗ | ✗ | ✗ | Намеренная политика границ |

**Ключевой вывод:** у инструментов нет OS-гейтов — доступность блокируется ошибками сборки под ОС, расходящимся env-allowlist и статическими `linux`-конфлациями вместо runtime-проб.

---

## 3. Целевая архитектура: `crates/pi-sys` + `Caps` + `Tool::requirements()`

### 3.1 Принципы
1. **Единственный HAL-крейт `crates/pi-sys`:** вся платформенная условная компиляция (`#[cfg(target_os/unix/windows)]`) локализуется в `pi-sys` (и syscall-зависимостях `pi-iso`). Во всех остальных крейтах `crates/*/src` платформенные `cfg` запрещаются CI-линтом:
   ```sh
   rg -n 'cfg\((target_os|unix|windows|not\()' crates \
     --glob '!crates/pi-sys/**' --glob '!crates/vendor/**'
   ```
2. **Способность как runtime-проба, а не `cfg`:** `cfg` остаётся только там, где различается наличие API/ABI (`libc`-символы, `windows-sys`). Всё прочее (`/proc`, `pidfd`, `FICLONE`, `fuse-overlayfs`, `PROT_EXEC`, `PATH`, `temp_dir`, Termux) проверяется пробой.
3. **Максимум доступности через `Requirements`:** инструмент не вырезается молча; при неудовлетворённых требованиях он регистрирует диагностическую причину через существующий `missing_tool_diagnostic` / `register_server_error`.
4. **Без новых OS feature-флагов:** только `[target.'cfg(...)'.dependencies]` в `Cargo.toml`, без комбинаторного взрыва фич.

### 3.2 Структура `crates/pi-sys`

```text
crates/pi-sys/src/
  lib.rs         # pub mod os, cap, env, temp, shell, spawn, proc, term, fsx, lock
  os.rs          # Os { Linux, Android, MacOs, Windows, OtherUnix } + const fn current()
  cap.rs         # Caps (OnceLock), Caps::for_fs(path), чистая Caps::from_probe(ProbeInput)
  env.rs         # EnvProfile { Strict, Explicit }, inherited(), default_path()
  temp.rs        # dir(): TMPDIR -> $PREFIX/tmp -> writable std::env::temp_dir() -> cache_dir
  shell.rs       # ShellKind { Posix, Cmd, PowerShell }, detect() -> Option<Shell>
  spawn.rs       # CommandSpec (.group(), .env_profile(), .stdin_null())
  proc.rs        # ProcStat, start_time, cmdline, children(), tree_kill(), pidfd_available()
  term.rs        # raw_mode(), winsize(), winch_pipe(), errno_ptr()
  fsx.rs         # rename_noreplace(), symlink(), set_mode_private(), is_name_too_long()
  lock.rs        # FileLock::acquire(path) (abstract socket | flock | windows lock)
  linuxish.rs    # общий слой Linux + Android
  android.rs     # __errno, ANDROID_DEFPATH + $PREFIX/bin, seccomp-safe pidfd, Termux detect
  macos.rs       # sysctl, attrlist/renamex_np
  windows.rs     # windows-sys реализация
  other_unix.rs  # generic kill(2) + flock fallback для BSD/illumos
```

**Чистые unit-тесты без кросс-железа:** `Caps::from_probe(ProbeInput)` принимает сырые результаты проб, что позволяет тестировать матрицу доступности для всех 4+ ОС прямо в `cargo test` на Linux-хосте.

### 3.3 `pi-iso` и декларативные требования инструментов
- `pi-iso::auto_order()` формирует список бэкендов из `Caps::for_fs(cwd)`: `[Btrfs?, Zfs?, LinuxReflink?(Linux+Android при успехе FICLONE), Overlayfs?(kernel или fuse-overlayfs), WindowsBlockClone/ProjFS?, Apfs?, Rcopy]`. Атрибуты `#[allow(dead_code)]` и жёсткий `FALLBACK_AUTO_ORDER` удаляются.
- В `Tool` добавляется `fn requirements(&self) -> Requirements { Requirements::none() }` (`os`, `caps`, `binaries`). `aragent` проверяет `requirements.satisfied(Caps::global(), cwd)` при сборке `ToolRegistry`.

---

## 4. Поэтапный план внедрения

| Шаг | Изменение | Критерий приёмки |
|---|---|---|
| **S1** | Каркас `pi-sys` (`os`, `cap` + `from_probe`) + CI `cfg`-линт | Линт ловит текущие утечки; в `pi-sys` работает матрица `from_probe` |
| **S2** | `env`, `temp`, `shell`, `fsx`, `lock` и удаление дублей | Удалены дубли `lock`/`renameat2`/`allowlist`; `bash`/`mktemp`/broker переведены на `pi-sys` |
| **S3** | `proc`, `spawn`, `term` + `other_unix` fallback | Убраны `platform`-модули из `pi-shell`/`pi-builtins`; BSD-ветка компилируется |
| **S4** | `pi-iso` runtime probe-order | На Termux при наличии `fuse-overlayfs` или `FICLONE` не падает в `rcopy` вслепую |
| **S5** | `Tool::requirements()` + диагностика в `ToolRegistry` | `bash` без shell и `ssh` без бинаря дают ясную причину вместо ошибки спавна |
| **S6** | Фикс P0-1 под Windows (`local_cmd.rs`, `config_cmd.rs`, `rpc/mode.rs`) | Сборка `arag.exe` и smoke на Windows-раннере (если Windows остаётся в поддержке) |
| **S7** | Удаление мёртвых `not(unix)`-стабов и `#[allow(dead_code)]` | Нулевой результат `cfg`-линта вне `pi-sys` и `vendor` |
| **S8** | Обновление `docs/platform-hal.md` из артефакта `caps-table` | Таблица в доках 1-в-1 совпадает с выводом теста |

---

## 5. Открытые решения (влияют на объём S3/S6)

1. **Windows:** чинить сборку + включать CI-раннер или официально исключать (с удалением из `rust-toolchain.toml` и удалением мёртвых `not(unix)`-стабов)?
2. **BSD / illumos:** оставлять `other_unix` fallback (`kill(2)`/`flock`) или явный `compile_error!`?
3. **Linux `musl`:** нужна ли статическая сборка (проверить `__errno_location` линковку в `tui.rs`)?
4. **Termux дефолты:** считать ли `$PREFIX/tmp` и `$PREFIX/bin` дефолтными даже при пустом `PATH`/`TMPDIR`?
5. **macOS CI:** добавлять ли `macos-14` раннер для проверки `apfs`/`renamex_np`/walker?
