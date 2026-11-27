# Мультиплатформенная процессная и файловая контейнеризация (2026-11-27)

> **Источники:** `ResearchMultiOSContainment`, первичные спецификации Linux kernel (`landlock`, `overlayfs`, `user_namespaces(7)`), AOSP Bionic (`SYSCALLS.TXT`, `SECCOMP_BLOCKLIST_APP.TXT`, `seccomp_policy.cpp`), Apple (`sandbox-exec`, `apple/container`, Virtualization.framework), Microsoft Win32 (`Job Objects`, `AppContainer`, `Block Cloning`, `WSL security model`), локальные исходники `crates/pi-shell`, `crates/pi-iso`, `crates/aragent/src/doctor.rs`, `~/tmp/fastproot`, `~/tmp/lkl-probe`.
> **Границы проверки:** статический анализ кода, лицензий и официальных спецификаций без запуска сборок/тестов на целевых ОС.

---

## 1. Сравнение архитектурных моделей

| Измерение | **A. Раздельные `pi-iso` + `pi-shell`** | **B. Внешний / общеагентный контейнер** | **C. Единый фасад `ContainerBackend`** |
|---|---|---|---|
| **Что изолирует** | ФС: CoW/копия дерева на host-пути (`pi-iso`). Процесс: супервизия дерева без изоляции ФС (`pi-shell`). | Вся среда целиком: rootfs, PID/mount/net namespaces, cgroups или VM. | Контракт поверх реальных возможностей ОС с явным уровнем гарантий (`CapabilitySet`). |
| **Граница безопасности** | Нет на Android/macOS/Windows; на Linux появляется только при добавлении namespace/Landlock. | Настоящая на Linux userns/OCI и VM на macOS/Windows; **отсутствует** на compat-слоях Android. | Ровно настолько сильна, насколько силён выбранный бэкенд; дефолт обязан явно объявлять отсутствие sandbox. |
| **Завершение дерева процессов** | `SpawnRegistry` + `(pid, start_time)` + TERM→KILL + защита harness (`process.rs:1470,1804,1882`). | Требует `Cancel(scope_id)`: host PID бессмыслен в PID namespace. | `KillPlan::{HostTree, ContainerScope, VmAgent}` + `UnknownOutcome` при потере связи. |
| **Файловая изоляция** | APFS `clonefile`, btrfs/ZFS, reflink, overlayfs, `Rcopy` (`pi-iso/src/lib.rs:130,294,358`). | Образ/rootfs, overlay, явная политика bind-mounts. | `RootfsPlan` + bind mounts + cwd; одного пути `merged` недостаточно без mount confinement. |
| **TTY/PTY и сетевая политика** | Pipe/stream (`shell.rs:366,604`); нет netns/egress-политики. | Управляется рантаймом/VM (`--network none/host/slirp`, NAT). | Явные `pty/term/winsize/resize` и `NetworkPolicy::{None, LoopbackOnly, Allowlist, Host}`. |
| **Android (без root)** | Работает сегодня: нативный shell + `Rcopy`, без sandbox-гарантий. | Compat-only (`fastproot`/`proot`/`LKL`); настоящая граница — только внешний VM/SSH. | Деградирует до `Native`/`Compat` с `sandbox: false`. |

---

## 2. Матрица по ОС и проверка лицензий

### 2.1 Лицензионный фильтр (политика MIT/Apache-2.0/BSD/ISC)

| Компонент / крейт | Лицензия | Решение | Причина |
|---|---|---|---|
| `rustix`, `nix`, `landlock` (0.4.x), `cgroups-rs`, `windows`/`windows-sys`, `clonetree`, `projfs` | MIT / Apache-2.0 | **Разрешён** | Совместимые пермиссивные лицензии (`landlock` ABI V1–V10). |
| `seccompiler` (0.5.0) | Apache-2.0 OR BSD-3-Clause | **Разрешён** (с риском поддержки) | Таблицы до kernel 6.12, `TSYNC`; оригинальный репо архивирован и перенесён в `rust-vmm`. |
| `libcontainer`, `oci-spec` | Apache-2.0 | **Разрешён без `libseccomp`** | Не включать feature `libseccomp` (тянет LGPL C-библиотеку). |
| `apple/container`, `Lima`, `Colima`, `podman` + `runc` | Apache-2.0 / MIT | **Разрешён как внешний** | Только опциональный внешний рантайм, не встраивать в бинарник. |
| `fastproot` (`~/tmp/fastproot`) | Нет `LICENSE` | **Требует фиксации авторства** | При наличии полных прав выпустить под MIT/Apache-2.0; использовать строго как compat layer. |
| `bwrap`, `libseccomp` (C) | LGPL-2.1(+) | **Исключён** | Нарушает политику зависимостей для встраивания/распространения. |
| `fuse-overlayfs`, `crun`, `proot`, `proot-distro`, `liblkl`/`kbox`, `windows-projfs` | GPL-2.0(+) / GPL-3.0 | **Исключён** | Копилефт GPL (`liblkl.a` также весит 328 MB, `kbox` ~140.6 MB). |

### 2.2 Возможности платформ

1. **Linux (`x86_64`/`arm64`):** лучший встраиваемый путь — прямые syscalls через `rustix`: `clone(CLONE_NEWUSER|CLONE_NEWNS|CLONE_NEWPID|CLONE_NEWNET)` → `pivot_root` → rootless kernel `overlayfs` с `userxattr` (с Linux 5.11) / `FICLONE` → `landlock` (`current_kernel_abi()`, V1–V10) → `seccompiler` (`TSYNC`). Всегда проверять `unshare(CLONE_NEWUSER)` пробой в runtime (на Ubuntu 24.04 AppArmor `kernel.apparmor_restrict_unprivileged_userns=1` может блокировать). `cgroups v2` дают только ресурсные квоты и в rootless требуют systemd delegation.
2. **Android / Termux (non-root Bionic arm64, SDK 28–35):**
   - `mount`/`umount2`/`chroot` запрещены `SECCOMP_BLOCKLIST_APP.TXT`.
   - `landlock_create_ruleset` отсутствует в `SYSCALLS.TXT` Bionic и бьёт `SIGSYS` (например, Android 14 / 5.10).
   - `pidfd_open` на старых устройствах ловит `SIGSYS`; текущий fallback в `pi-shell/src/process.rs:34,42,178-183` (`/proc` + `start_time`) корректен.
   - `seccomp` отсутствует в `SYSCALLS.TXT` (`SECCOMP_FILTER_FLAG_NEW_LISTENER` не гарантирован; `fastproot` опирается на `prctl(PR_SET_SECCOMP)`).
   - `unshare(CLONE_NEWUSER)` часто даёт `EPERM`/`EINVAL` по SELinux/OEM.
   - **Итог:** без root на Android нет лицензионно-чистой локальной FS-песочницы; `fastproot` допустим только как compat-слой (`sandbox: false`), настоящая граница — внешний VM/SSH.
3. **macOS:** APFS `clonefile(2)` (`pi-iso/src/apfs.rs:98`) даёт быстрый CoW без изоляции процесса; `sandbox-exec`/Seatbelt помечен deprecated; настоящая граница безопасности — только VM (`Virtualization.framework` с entitlement, `apple/container` на macOS 26+, `Lima`/`Colima`).
4. **Windows:** `Job Object` (`KILL_ON_JOB_CLOSE`, без `SILENT_BREAKAWAY_OK`) + **AppContainer / LPAC** (`CreateAppContainerProfile`) дают настоящую границу; Low IL / Restricted Token сами по себе не закрывают чтение. Для ФС `FSCTL_DUPLICATE_EXTENTS_TO_FILE` работает **только на ReFS**, а не на NTFS (`pi-iso/src/windows_block_clone.rs:3,32` содержит неверное упоминание NTFS и безусловный `probe` — требуется проверка ReFS). WSL2 по модели Microsoft не является границей безопасности от того же Windows-пользователя.

---

## 3. Обязательные контракты абстракции

1. **`ExecOutcome::{Exited(i32), Signaled(i32), UnknownOutcome}`:** потеря связи с демоном/контейнером/VM не должна маппиться ни в успех, ни в ошибку «не стартовал» и никогда не должна запускать автоповтор.
2. **Честная деградация по пробам (`doctor.rs:2255`):** выбор бэкенда управляется runtime-пробами (`Caps`), а не только `#[cfg]`; `pi-iso` и `pi-shell` остаются раздельными базовыми модулями с тонким фасадом `ExecutionSandbox` / `EnvironmentPlan`.

---

## 4. Веб-верификация по официальным источникам (2026-11-27)

Проверено агентом `VerifyContainmentWeb` по `docs.kernel.org`, AOSP Bionic (`SECCOMP_BLOCKLIST_APP.TXT`, `pidfd.h`, CTS `gen_blocklist.py`), `docs.docker.com`, `developer.android.com`, `source.android.com`, `developer.apple.com`, `learn.microsoft.com`.

| Механизм | ОС / условие | Вердикт | Официальный источник | Критичная ловушка |
|---|---|---|---|---|
| `mount`, `umount2`, `chroot` | Android (zygote-spawned, все API) | **NO** (`SIGSYS`) | [SECCOMP_BLOCKLIST_APP.TXT](https://android.googlesource.com/platform/bionic/+/refs/heads/main/libc/SECCOMP_BLOCKLIST_APP.TXT), [CTS gen_blocklist.py](https://android.googlesource.com/platform/cts/+/refs/heads/main/hostsidetests/seccomp/app/gen_blocklist.py) | `setns`/`unshare` в этом файле **отсутствуют**; они режутся отдельно ядром (`CONFIG_USER_NS`) и SELinux |
| `PR_SET_NO_NEW_PRIVS` + stacked `seccomp-BPF` | Linux ≥3.5/3.17, Termux (unprivileged) | **YES** | [no_new_privs](https://docs.kernel.org/userspace-api/no_new_privs.html), [seccomp_filter](https://docs.kernel.org/userspace-api/seccomp_filter.html), [seccomp(2)](https://man7.org/linux/man-pages/man2/seccomp.2.html) | Ставить `seccomp` строго последним; sandbox обязан запрещать `ptrace` (`SECCOMP_RET_TRACE` + ptracer = escape); NNP сам по себе `ptrace`/`proot` не блокирует |
| `Landlock` | Linux host ≥5.13 (`NO` на Termux `5.4.147`) | **YES** (probe-gated) / **NO** (`<5.13`) | [kernel.org Landlock](https://docs.kernel.org/userspace-api/landlock.html), [landlock.io/news/5](https://landlock.io/news/5/) | LTS `v1..v7` (`5.13..6.15`), свежие доки до `v12`; ABI 6 (`SCOPES`) рвёт abstract sockets/signals — маскировать `handled & allowed`, пустые правила скипать |
| `close_range` | Linux / Android | **PARTIAL** (`≥5.9`, glibc `2.34`) | [close_range(2)](https://www.man7.org/linux/man-pages/man2/close_range.2.html) | На ядре `5.4` Termux отсутствует — обязателен fallback на цикл по `/proc/self/fd` |
| `pidfd_open` / `pidfd_send_signal` | Android | **NO** (`<31`) / **YES** (`≥31`) | [Bionic pidfd.h](https://android.googlesource.com/platform/bionic/+/master/libc/include/sys/pidfd.h), [golang#69065](https://github.com/golang/go/issues/69065) | До API 31 даёт `SIGSYS`-kill (не `ENOSYS`); шим в `pi-shell/src/process.rs:42-61` обязателен |
| `proot`/`fastproot` (`ptrace`) | Android 12+ Termux | **PARTIAL** (`sandbox: false`) | [b/205156966](https://issuetracker.google.com/issues/205156966), [termux/proot#260](https://github.com/termux/proot/issues/260), [termux-packages#11798](https://github.com/termux/termux-packages/issues/11798) | Глобальный Phantom Process Killer (32 процесса system-wide) + W^X `execve` |
| `android:isolatedProcess` + `useAppZygote` | Android APK (API `29+`) | **YES** | [service-element](https://developer.android.com/guide/topics/manifest/service-element), [ServiceInfo](https://developer.android.com/reference/android/content/pm/ServiceInfo), [Process](https://developer.android.com/reference/android/os/Process) | UID `90000..99999`, SELinux `isolated_app`, нет сети/диска, только Binder+FD; в репо нет JNI/`cdylib`/манифеста |
| `AVF` / `pKVM` | Android APK (обычное app) | **NO** | [AVF security](https://source.android.com/docs/core/virtualization/security), [AVF architecture](https://source.android.com/docs/core/virtualization/architecture), [pkvm.html](https://docs.kernel.org/virt/kvm/arm/pkvm.html) | ARM64-only, только подписанные образы; `MANAGE_VIRTUAL_MACHINE` вне публичного `Manifest.permission` (`@SystemApi`, пруф partial) |
| Docker default seccomp + Ubuntu `24.04` userns | Контейнеры / Ubuntu `23.10+` | **PARTIAL** / **NO** | [Docker seccomp](https://docs.docker.com/engine/security/seccomp/), [default.json](https://github.com/moby/moby/blob/master/vendor/github.com/moby/profiles/seccomp/default.json), [Docker rootless troubleshoot](https://docs.docker.com/engine/security/rootless/troubleshoot/) | Default seccomp режет `mount`/`pivot_root`/`unshare`, `setns` gated `CAP_SYS_ADMIN`; `apparmor_restrict_unprivileged_userns=1` блокирует и host `bwrap` |
| `sandbox-exec` vs `Virtualization.framework` | macOS | **NO** (deprecated) / **YES** (VM) | [Apple forums](https://developer.apple.com/forums/tags/app-sandbox), [KEXT deprecation](https://developer.apple.com/support/kernel-extensions/), [Virtualization entitlement](https://developer.apple.com/documentation/bundleresources/entitlements/com.apple.security.virtualization) | `sandbox-exec` пруф средней силы (форум+KEXT); `Virtualization.framework` требует entitlement и поднимает целую ВМ; `setpgid`+`kill(-pgid)` — только kill-plane |
| `CreateJobObject` (`KILL_ON_JOB_CLOSE`, nested) + `AppContainer`/LPAC | Windows (`8`/`10`/`11`) | **YES** | [Job Objects](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects), [Nested Jobs](https://learn.microsoft.com/en-us/windows/win32/procthread/nested-jobs), [AppContainer](https://learn.microsoft.com/en-us/windows/win32/secauthz/implementing-an-appcontainer), [CreateAppContainerProfile](https://learn.microsoft.com/en-us/windows/win32/api/userenv/nf-userenv-createappcontainerprofile) | Не терять job handle и не разрешать breakaway; для LPAC нужны явные caps + redirect `%TEMP%` |
| Restricted Token / Low IL как read-барьер | Windows | **NO** | [MIC Vista](https://learn.microsoft.com/en-us/archive/blogs/steriley/mandatory-integrity-control-in-windows-vista), [MIC docs](https://learn.microsoft.com/en-us/windows/win32/secauthz/mandatory-integrity-control) | MIC = Biba (no-write-up): Low IL читает higher-IL по умолчанию (`NO_READ_UP` opt-in); для secrecy нужны `AppContainer`/DACL |
