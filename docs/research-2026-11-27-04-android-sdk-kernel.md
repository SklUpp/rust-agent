# Track 4: Адаптация и интеграция к Android (SDK 28, ядра, Bionic, SELinux) (2026-11-27)

> **Источники:** `ResearchAndroidSdkKernel2` + аудит `ResearchReviewPlatform`.
> **Якоря:** `~/rust-agent` (`crates/aragent/src/{wake_lock.rs,crash_handler.rs,dirs.rs,lib.rs}`, `crates/pi-agent/src/{eval/probe.rs,tls.rs}`, `crates/pi-shell/src/`, `.github/workflows/standalone-runtime.yml`), `~/omp-termux` (`61325cdd`, `android/patches/`), `~/tmp/lkl-probe/{bionic_compat.h,net_shim.c}`, `~/tmp/android-agent-runtime` (read-only).
> **Границы проверки:** инспекция кода, патчей и NDK/kernel документации; живые пробы на реальных устройствах Android с разными SDK/вендорами не запускались.

---

## 1. SDK 28 vs API 29+ (`W^X`) и среда выполнения

| Аспект | Фактическое поведение (после поправок ревью) | Рекомендация для `arag` |
|---|---|---|
| **`targetSdk 28` и `execve` из writable app-dir** | `targetSdk <= 28` — документированная стратегия совместимости (используется Termux), но **не универсальная гарантия** на всех устройствах: на практике влияют `targetSdk`, runtime API, SELinux-домен процесса и вендорная политика. Требование `extractNativeLibs` зависит от конкретного способа упаковки APK | Разделять compile-time/packaging стратегию и runtime-возможности; проверять реальный `execve` на старте, а не полагаться на версию SDK по предположению |
| **`LD_LIBRARY_PATH`, `DT_RPATH`, `RUNPATH`** | **Поправка ревью:** утверждение, что Bionic категорически игнорирует `LD_LIBRARY_PATH`, слишком широко — для обычных non-secure процессов переменная работает в рамках linker namespaces и при `AT_SECURE == 0` | Явно формировать `EnvPlan` для подпроцессов; проверять `RUNPATH`/`patchelf` на целевых бинарниках |
| **`memfd_create` / `execveat`** | **Не является доказанным обходом W^X** на Android. На ядрах 6.3+ флаг `MFD_EXEC` зависит от политики ядра; SELinux/seccomp могут блокировать `memfd` или `execveat` | Не проектировать критические пути `arag` через `memfd_create` без device-верификации |
| **Определение API level** | Отсутствие символа `android_get_device_api_level` само по себе не доказывает API < 29 без учёта NDK/Bionic ABI сборки | Использовать безопасный lookup символа (или системное свойство `ro.build.version.sdk`) как информационный тег диагностики |
| **Хранилище и пути** | Предпочитать `$HOME`, `$PREFIX`, `$TMPDIR` внутри приватного пространства Termux/app; избегать жёсткого `/tmp` | Сохранять правило работы с `${TMPDIR:-$PREFIX/tmp}` (`dirs.rs`) |
| **Phantom Process Killer / Doze** | `termux-wake-lock` (`wake_lock.rs`) и даже foreground service **не гарантируют** выживание дочерних процессов при лимите дочерних процессов (обычно ~32) и давлении памяти / настройках вендора | Минимизировать число постоянных фоновых процессов (держать max 1–2 sidecar, on-demand запуск, bounded concurrency) |

---

## 2. Матрица ядер (4.14 — 6.1+) и опасность опоры на `uname`

> **Ключевое правило из ревью:** таблицы версий апстримного Linux (когда появились `pidfd`, `landlock` 5.13, `seccomp_unotify` `ADDFD`/`CONTINUE`, `clone3`, `io_uring`) **не доказывают**, что функция доступна на конкретном телефоне. Android-вендоры делают бэкпорты, отключают `CONFIG_*` (часто отключены `LANDLOCK` и `USER_NS`) и блокируют сисколы **seccomp-фильтром Zygote/App**.

| Механизм | Риск на Android | Что подтверждено в источниках |
|---|---|---|
| **`pidfd_open` / `pidfd_send_signal`** | **`SIGSYS`** на старых Android / строгих seccomp-профилях | `omp-termux` (`61325cdd`) патчит `pidfd` из-за `SIGSYS` до API 31; основной kill в `arag` через `-pgid` безопаснее как baseline |
| **`landlock` / `user_namespaces` / `unshare`** | `ENOSYS`, `EPERM` или `SIGSYS` (отсутствие поддержки ядром / SELinux / seccomp) | Не отбрасывать догматически «всегда падает» — проверять изолированной пробой на устройстве |
| **`seccomp-unotify` (`ADDFD`/`CONTINUE`) / `clone3` / `io_uring`** | Могут отсутствовать даже при совпадающем major/minor или резаться seccomp | Только runtime probe per-syscall; иметь fallback без `io_uring`/`seccomp-unotify` |
| **`inotify` (`inotify_rm_watch`)** | Несовпадение signed/unsigned аргумента на Bionic | Подтверждено коммитом `omp-termux` `61325cdd` |
| **`flock`, `PR_SET_PDEATHSIG`, `/proc` `hidepid`** | `hidepid` скрывает чужие процессы (но свой UID виден); `PR_SET_PDEATHSIG` сбрасывается при смене UID/capability (`AT_SECURE`) | Проверять возврат `prctl(PR_SET_PDEATHSIG)` и аккуратно обрабатывать `EACCES`/`ESRCH` при чтении `/proc` |

---

## 3. Bionic и Rust-среда выполнения

- **DNS:** Bionic `getaddrinfo` обращается к Android `netd` (работает Private DNS и смена сетей без `/etc/resolv.conf`). Статические сторонние бинарники (Go `CGO_ENABLED=0`, musl) ищут `/etc/resolv.conf` — в Termux его нет на стандартном пути, лежит в `$PREFIX/etc/resolv.conf`.
- **TLS:** системные корневые сертификаты находятся в `/system/etc/security/cacerts` (а также `$PREFIX/etc/tls/cert.pem`, `SSL_CERT_FILE`). Встроенные `webpki-roots` не зависят от пути на ФС, но не видят пользовательских корпоративных CA.
- **Память/аллокатор:** `alloc_error_hook` требует осторожности (`#[cfg(not(target_os = "android"))]`); `scudo` (Android 11+) усиливает проверки кучи и болезненно реагирует на любые UAF/double-free в C-обёртках (`fastproot`/`lkl`/`tree-sitter`).
- **PTY/TTY/IME:** ресайз окна и IME-буферизация в Termux требуют корректного `TIOCSWINSZ` и отсутствия жёстких допущений о `/dev/tty`.

---

## 4. Безопасный дизайн runtime capability-проб (`arag doctor` / селектор)

Поправка ревью к идее использовать `eval/probe.rs:91–100`:
- **Нельзя использовать текущий `run_bounded_probe` в `eval/probe.rs:91–100`:** он отбрасывает `stdout` и `stderr`.
- **Нельзя проверять несколько `SIGSYS`-опасных syscall в одном дочернем процессе:** первый `SIGSYS` убьёт процесс-пробник и не даст проверить остальные.

**Целевой дизайн пробы:**
1. **Изоляция per-syscall:** каждый опасный syscall (`pidfd_open`, `landlock_create_ruleset`, `seccomp_unotify`, `unshare(CLONE_NEWUSER)`, `memfd_create+execveat`) проверяется в отдельном коротком fork/subprocess с коротким таймаутом и возвратом `(enosys | eperm | sigsys | ok)` через exit code или pipe.
2. **Кэширование профиля:** кэшировать результат по ключу `(uname -r, sdk_level, selinux_context_hash, arag_version)` с возможностью сброса в `arag doctor`.
3. **Честная лестница деградации:** отсутствие `landlock`/`bwrap` переводит бэкенд в `Native` / compat без ложного заявления о sandbox-изоляции.
