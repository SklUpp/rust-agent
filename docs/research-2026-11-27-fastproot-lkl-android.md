# Исследование: интеграция `fastproot`/`lkl`, кросс-OS бэкенды и адаптация под Android (2026-11-27)

> **Статус:** ненормативное исследование + независимое ревью (`ResearchFastprootLkl2`, `ResearchAndroidSdkKernel2`, `ResearchReviewPlatform`).
> **Источники:** `~/tmp/fastproot`, `~/tmp/lkl-probe`, `~/tmp/hybrid-fastproot-lkl-spec.md`, `~/tmp/lkl-research-and-plan.md`, `~/rust-agent` (`pi-shell`, `pi-agent`, `aragent`), `~/omp-termux` (`61325cdd`).
> **Границы проверки:** только инспекция исходников и pinned-коммитов; сборки NDK, syscall-пробы на железе и замеры размера установленного rootfs не проводились.

---

## Часть 1. `fastproot` vs `lkl` vs `hybrid` и модульная кросс-OS система (Track 3)

### 1.1 Сравнение механизмов (и критические поправки ревью)

| Критерий | `fastproot` (`~/tmp/fastproot`) | `lkl` (`~/tmp/lkl-probe`) | `hybrid` (проект) |
|---|---|---|---|
| **Механизм** | `LD_PRELOAD` (перехват libc) + `seccomp-unotify` на `execve`/пути (при поддержке ядра) | Userspace Linux kernel (`liblkl.a`) + `lkl_daemon` по Unix socket + `lkl-wrap.so` | Быстрые host-утилиты через `fastproot`, несовместимые/glibc/stat/ext4 — через LKL |
| **Статические / Go бинарники** | **Не перехватываются `LD_PRELOAD`**; без working `seccomp-unotify` уходят мимо прослойки | Работают через ptrace/seccomp или переписанный libc wrap (в зависимости от режима) | Маршрутизация по ELF/бинарнику |
| **Известные баги PoC** | 1) `g_child_pid` пишется после `fork`, handler бьёт только по `g_child_pid`, нет `PR_SET_PDEATHSIG` check (`fastproot.c:25-28,699,791`); 2) cleanup после `waitpid` оставляет `.fp-*` при `SIGKILL` (`:791-812`); 3) `patchelf --remove-rpath` опционален (`Makefile:20,28`) | 1) **`lkl_daemon.c:178-200` перехватывает лишь список syscall и пропускает остальные**; 2) zygote форкает обычные host-процессы (`:425-479`); 3) socket `0700` без `SO_PEERCRED`, `EXEC` берёт произвольный путь (`:510-542`); 4) `open`-флаги транслируются списком — неподдержанные молча отбрасываются (`:73-91`); 5) `VFD_BASE=100000` сломается при host FD ≥ 100000 | Наследует ограничения обоих |
| **Размер/нагрузка** | Малый (`.so` + launcher) | `[НЕ ПРОВЕРЕНО]` Черновик оценивает ~147 MB, но static archive ≠ установленный размер rootfs/пакета | Зависит от упаковки |

> **Критический вердикт `ResearchReviewPlatform` (No-Go как sandbox):**
> Текущий `lkl_daemon.c` PoC **НЕ является границей безопасности для недоверенного кода**. Из-за выборочного перехвата syscall, отсутствия `SO_PEERCRED` и запуска host-процессов из zygote, заявления о невозможности доступа к данным приложения или гарантированном убийстве всех guest-задач через host process-group **не доказаны**. Полная медиация syscall, аутентификация сокета, отмена per-scope и изоляция процессов — это требования к будущей реализации, а не свойства текущего PoC.

### 1.2 Точки исправлений в `fastproot` / `lkl` до интеграции

1. **`fastproot.c`:** переход на процессную группу с осторожным учётом интерактивного TTY/job-control и потомков с `setsid()` (`kill(-pid, sig)` не универсален для дерева процессов); сверка `PR_SET_PDEATHSIG` после `fork`; очистка `.fp-*` на старте только для мёртвых PID, без сноса кэшей и живых сессий; проверка реального `RUNPATH` артефакта `libfastproot_loader.so`.
2. **`lkl-probe`:** отказ от пропуска неперехваченных syscall; `SO_PEERCRED` + token auth на сокете; строгая проверка `FD < VFD_BASE` (или динамическое выделение); верификация `net_shim.c` (`ifreq` layout и DNS) под реальные ABI.

### 1.3 Модульная кросс-OS архитектура в `arag`

Единый абстрактный слой (`ExecutionBackend` / `OsEnvAdapter` в `crates/pi-shell` + `pi-agent`), единый для Linux, Android/Termux и macOS:
- **Контракт `LaunchPlan` недостаточен как `(program, argv)`** (поправка ревью): для `lkl`/remote daemon нужен полноценный **daemon/RPC execution contract** — admission (семафор/квота), streaming stdio, cancellation (`TERM→KILL` локально или explicit `DaemonCancel` по `scope_id`), и явное состояние `UnknownOutcome` при потере связи.
- **Матрица бэкендов по ОС (согласованная с R1–R3):**
  - **Linux:** `native` (текущий `pi-shell` + `process_tree_kill.rs`) | `sandbox` (отложенные `bwrap` / `landlock` / `seccompiler` при поддержке ядра) | `fastproot`/`lkl` (опционально для compat).
  - **Android/Termux:** `native` | `fastproot` (только compatibility layer, не sandbox!) | `lkl`/`hybrid` (compat/glibc rootfs).
  - **macOS/прочие:** `native` (sandbox-профиль — отдельно).
- **Неизменные инварианты:** `BashPolicy` (`crates/pi-shell/src/policy.rs`, `approve.rs:447-485`) всегда оценивает **сырую команду до запуска**; `Forbidden`/`ask` не отменяются выбором бэкенда. Исключение `tools/scheduler.rs:235-249` (отказ `task` при активной durability) нужно учитывать при проектировании durable execution.

---

## Часть 2. Адаптация к Android: SDK 28, ядра, Bionic, SELinux (Track 4)

### 2.1 SDK 28 vs API 29+ (`W^X`) и пути исполнения

| Фактор | Реальность (после ревью `ResearchReviewPlatform`) | Что делать в `arag` |
|---|---|---|
| **`targetSdk 28` vs runtime API 29+** | Стратегия `targetSdk <= 28` для `execve` из app-private writable dir — известная схема совместимости (как в Termux), но **не универсальная гарантия** на всех прошивках: решают `targetSdk`, runtime API, SELinux-домен приложения и вендорная политика. `extractNativeLibs` зависит от способа упаковки | Различать compile-time/packaging схему и runtime-проверку; не полагаться на единое правило для всех APK/Termux окружений |
| **`LD_LIBRARY_PATH` / `DT_RPATH` / `RUNPATH`** | Bionic **не** игнорирует `LD_LIBRARY_PATH` для обычных non-secure процессов (в исходном отчёте формулировка была слишком широкой); влияют linker namespaces и `AT_SECURE` | Управлять env через явный `EnvPlan`, проверять `patchelf` / `RUNPATH` артефактов на целевом устройстве |
| **`memfd_create` / `execveat`** | **Не является доказанным обходом W^X** на Android; `MFD_EXEC` на 6.3+ зависит от политики ядра и флагов, а SELinux/seccomp могут резать `memfd` или `execveat` | Не закладывать `memfd_create` как базовый путь исполнения без device-верификации |
| **`android_get_device_api_level`** | Отсутствие символа само по себе не доказывает API < 29 без проверки целевого NDK/Bionic ABI | Читать безопасно с fallback на `ro.build.version.sdk` |
| **Phantom Process Killer** | `termux-wake-lock` и foreground service **не гарантируют выживание** дочерних процессов при лимите Android (обычно 32 дочерних процесса / давление памяти, зависит от версии/вендора) | Ограничивать число долгоживущих процессов (max 1–2 sidecar, bounded concurrency, on-demand lifecycle) |

### 2.2 Матрица ядер и почему `uname` недостаточно

- **Ядра 4.14 / 4.19 / 5.4 / 5.10 / 5.15 / 6.1+:** таблицы апстримного Linux (когда появился `pidfd_open`, `landlock` 5.13, `seccomp_unotify` `ADDFD`/`CONTINUE`, `clone3`, `io_uring`) **не доказывают** их работу на Android из-за vendor-бэкпортов, выключенных Kconfig (например, отключённый `CONFIG_SECURITY_LANDLOCK` или `USER_NS`) и **seccomp-фильтров Android** (например, `pidfd` → `SIGSYS` до API 31 в `omp-termux`, `61325cdd`).
- **Другие Bionic-нюансы:**
  - `inotify_rm_watch` на Bionic (`omp-termux 61325cdd`).
  - DNS без `/etc/resolv.conf` (на Termux файл в `$PREFIX/etc/resolv.conf`; Bionic `getaddrinfo` ходит в `netd`, статический Go/musl — ищет `/etc/resolv.conf`).
  - TLS: `/system/etc/security/cacerts` / `$PREFIX/etc/tls/cert.pem` (`SSL_CERT_FILE`) vs `webpki-roots` / `rustls-native-certs`.
  - `alloc_error_hook` — осторожность на `target_os = "android"` / `scudo`.

### 2.3 Дизайн безопасных capability-проб (после поправки ревью)

- **Не использовать `eval/probe.rs:91-100` в текущем виде:** он отбрасывает `stdout` и `stderr`.
- **Не объединять `SIGSYS`-опасные syscall в один probe-процесс:** первый `SIGSYS` убьёт процесс до проверки остальных возможностей.
- **Правильная схема (`arag doctor` / startup probe):**
  1. Каждая фатальная проба (`pidfd_open`, `landlock_create_ruleset`, `seccomp_unotify`, `unshare(CLONE_NEWUSER)`, `memfd_create+execveat`) выполняется в **отдельном короткоживущем fork/subprocess** с таймаутом и возвратом результата через pipe/exit-status (разделяя `ENOSYS`, `EPERM`, `SIGSYS`).
  2. Кэшировать профиль с привязкой к `uname -r`, API level и версии `arag`.
  3. Строить явную лестницу fallback (например, `pidfd` → kill по `-pgid`; `landlock` → отсутствует без имитации песочницы).
