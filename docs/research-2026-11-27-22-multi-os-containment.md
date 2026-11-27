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
