# Кросс-платформенный пайплайн изоляции и запуска команд (`pi-sys`)

Дата: 2026-11-27 (опора: разведка `ScoutCrossOsExecPipeline` по состоянию `HEAD 932c434`, а также [research-2026-11-27-10](research-2026-11-27-10-platform-hal-tool-availability.md), [research-2026-11-27-22](research-2026-11-27-22-multi-os-containment.md), [research-2026-11-27-23](research-2026-11-27-23-agent-containment-architecture.md)).
Статус: **Предложение** (не нормативно до фиксации в [agent-architecture-decisions.md](agent-architecture-decisions.md)). Развивает `ExecPlan` из [agent-reuse-and-templates-plan.md](agent-reuse-and-templates-plan.md).

---

## 1. Фактическая база в коде

- Единственная точка допуска инструментов: `ToolScheduler::run_single` (`crates/pi-agent/src/tools/scheduler.rs:112-350`) и `Approvals::bash_verdict` (`crates/aragent/src/approve.rs:448-474,545-575`).
- Единый бэкенд-шов процессов: `ProcessBackend` (`crates/pi-agent/src/tools/process.rs:403-458`) + `ProcessManager::start` (`:1217`) + `LazyProcessBackend` (`crates/aragent/src/runtime.rs:661,7211-7268`).
- Каноническая остановка дерева процессов: `Process::terminate_tree` и `kill_process_group` (`crates/pi-shell/src/process.rs:1421-1432,1659-1678`).
- **Пробелы текущего кода:**
  - `crates/pi-sys` (`Caps` / `ExecPlan`) в дереве пока отсутствует; пробы `unshare`/`PR_SET_NO_NEW_PRIVS`/Landlock/seccomp живут только в `crates/aragent/src/doctor.rs:2488-2570`.
  - `sandbox` ключ конфигурации схемой/doctor принимается, но `config.rs` парсит только `task.isolation.*` (`1096-1103,2687-2705`); `PathMap` явно не является границей сдерживания (`config.rs:2101`).
  - `LazyProcessBackend` при недоступном брокере тихо переключается на in-process `ProcessManager` (`runtime.rs:7253,7264`) вместо возврата `UnknownOutcome`.
  - Дублируются `kill(-pgid)` (`tools/process.rs:2776`, `aragent/src/rpc/mode.rs:401-403` и др.), `Command::new` (~19 мест), а на Windows `group_id()` = `None` (`pi-shell/src/process.rs:1024,1274`).

---

## 2. Единый 7-стадийный пайплайн (`S0 -> S6`)

```text
S0 admission      ToolScheduler: hidden -> PreToolUse rewrite -> Approvals(BashPolicy, forced ask)
                  -> cancel recheck -> durable begin. Политика проверяет точный argv/команду.
S1 scrub          pi_sys::exec::plan(): EnvProfile (allowlist, без ambient inherit), argv как Vec<OsString>
                  (без повторного split), close_range(3.., UNSHARE|CLOEXEC), ровно 3 stdio FD, rlimits.
S2 select         Caps::from_probe(ProbeInput) -> route(trust, caps) -> Containment + KillPlan.
S3 contain/spawn  Единый spawn: pgid/setsid -> NNP -> rlimits -> capset -> Landlock restrict_self
                  -> seccomp строго последним -> exec. Внешние обёртки (bwrap/fastproot/lkl) меняют только argv.
S4 bound          Ограниченный дренаж stdout/stderr (read_capped) + deadline/cancel + PTY winsize.
S5 settle         ExecOutcome::{Exited(i32), Signaled(i32), TimedOut(i32), UnknownOutcome(String)}.
S6 kill/cleanup   KillPlan::{HostTree{pgid,identity} | ContainerScope{scope_id} | VmAgent{..}} + уборка tmp/сокетов.
```

Инварианты:
1. **Политика до переписывания:** `Approvals::bash_verdict` (`approve.rs:448`) оценивает исходную команду **до** `S2`; бэкенд никогда не перепарсивает shell-строку.
2. **Порядок привилегий в `S3`:** `PR_SET_NO_NEW_PRIVS` (или `LANDLOCK_RESTRICT_SELF_NO_NEW_PRIVS`) -> rlimits/capset -> `landlock_restrict_self` -> `seccomp` последним (иначе seccomp заблокирует собственную настройку).
3. **Раздельность групп и сессий:** detached/PTY используют `setsid()`, piped-утилиты — `process_group(0)`, но оба режима идут через один `pi_sys::exec::run`.
4. **Честный исход в `S5`:** таймаут даёт `TimedOut(signal)` (не успех и не `UnknownOutcome`). `UnknownOutcome` выдаётся только при потере плоскости владения (брокер/демон/VM отвалились после принятия сайд-эффекта в `runtime.rs:7253`).

---

## 3. Матрица платформ, fallback и лицензионные границы

| Платформа | Основной встроенный контур | Fallback | OS API и критичные ограничения | Лицензионная граница |
|---|---|---|---|---|
| **Linux host ≥5.13** | `Landlock` (`v1..v6` probe) + `seccompiler` + `NNP` + `rlimits` | `Native` (`pgid` + `(pid, start_time)`) | Landlock — deny-policy, не rootfs. ABI v6 (`6.12` `SCOPE_SIGNAL`/`ABSTRACT_UNIX`) может ломать IPC/аддоны | Разрешено встраивать: `rustix`, `landlock`, `seccompiler` (MIT/Apache/BSD) |
| **Linux в Docker / Ubuntu 24.04** | In-process `Landlock` + `seccomp` без mount-ns | `Native` | Без `CAP_SYS_ADMIN` и при `kernel.apparmor_restrict_unprivileged_userns=1` вложенные `unshare`/`bwrap` получают `EPERM`; rootless cgroups v2 требует systemd delegation | `bwrap` / `libseccomp` (LGPL-2.1) — **не встраивать**, только внешний бинарь |
| **Android Termux (текущий хост `5.4.147`)** | `Native` + `PR_SET_NO_NEW_PRIVS` + `rlimits` + env/FD scrub | `fastproot` (только compat, `sandbox: false`); `LKL` отдельным процессом (не граница) | `mount`/`umount2`/`chroot` режутся `SECCOMP_BLOCKLIST_APP.TXT`; Landlock отсутствует в Bionic и ядре `5.4`; `pidfd` обходится (`pi-shell/src/process.rs:42-61`) | `fastproot` без LICENSE до фикса не отгружать; `liblkl`/`kbox`/`proot` (GPL-2.0) — **исключены из бинаря** |
| **Android APK** | `android:isolatedProcess="true"` (+ `useAppZygote` API 29+) | `fastproot` через `jniLibs`, `LKL` отдельным `.so`; иначе только approval gate | UID `90000..99999`, только Binder+FD, без сети/каталога данных; в репо нет `cdylib`/JNI/манифеста; AVF/pKVM требует `@SystemApi` | Apache-2.0 для AOSP API; исключение GPL для LKL сохраняется |
| **macOS** | `Virtualization.framework` (`com.apple.security.virtualization`) или внешний `apple/container` | `Native` (`setpgid`/`kill(-pgid)`, `proc_bsdinfo` `pi-shell/src/process.rs:520-537`) | `sandbox-exec`/Seatbelt deprecated; `EndpointSecurity` требует Apple entitlement + root; `clonefile(2)` — только FS CoW | Apple APIs совместимы; без GPL-рантаймов |
| **Windows** | `CreateJobObject` (`KILL_ON_JOB_CLOSE`) + `AppContainer`/LPAC (`STARTUPINFOEX`) | `Native` (`TerminateProcess` + Toolhelp snapshot) | Обычный Restricted Token / Low IL не блокирует чтение; нет `pgid` (`group_id()` = `None`, `pi-shell/src/process.rs:1024,1274`) — Job Object нужен и для `KillPlan::HostTree` | `windows-sys` (MIT/Apache) |

---

## 4. Минимальный стык в Rust (`crates/pi-sys`)

Отдельный крейт `pi-exec` не нужен: вся низкоуровневая обвязка ложится в один leaf-крейт `crates/pi-sys` (`cap.rs` + `exec.rs`), не создавая циклов с `pi-vcs`/`pi-iso`/`pi-agent`/`aragent`.

```rust
// crates/pi-sys/src/cap.rs — только пробы возможностей OS, без спавна
pub struct Caps {
    pub landlock_abi: Option<u8>,
    pub seccomp_unotify: bool,
    pub userns: bool,
    pub pidfd: bool,
    pub job_objects: bool,
    pub appcontainer: bool,
    pub fastproot: bool,
    pub lkl: bool,
}
impl Caps {
    pub fn from_probe(p: ProbeInput) -> Self;
    pub fn global() -> &'static Self;
}

// crates/pi-sys/src/exec.rs — единственное место с Command::spawn / pre_exec / portable-pty
pub struct ExecPlan {
    pub program: OsString,
    pub argv: Vec<OsString>,
    pub cwd: PathBuf,
    pub env: EnvProfile,          // Strict(allowlist) | Explicit(map)
    pub io: IoPlan,               // Null | Piped { cap } | Pty { rows, cols } | Append { path }
    pub isolation: Isolation,     // Inherit | NewGroup | NewSession
    pub containment: Containment, // None | Linux { .. } | Windows { .. } | Wrapped { program, argv }
    pub kill: KillPlan,           // HostTree { pgid, identity } | ContainerScope { scope_id } | VmAgent { .. }
    pub deadline: Option<Duration>,
}
pub enum ExecOutcome { Exited(i32), Signaled(i32), TimedOut(i32), UnknownOutcome(String) }
pub fn run(plan: ExecPlan, ct: CancelToken) -> Result<Spawned, ExecError>;
pub fn run_bounded(plan: ExecPlan, ct: CancelToken) -> ExecOutcome;
```

Правила интеграции:
1. **Один план, не два спавнера:** `ExecutionBackend` (`fn wrap(&self, ExecPlan) -> Result<ExecPlan, ExecError>`) — чистый трансформер плана (меняет `program`/`argv`/`containment`/`kill`), а не альтернативный спавнер.
2. **Единый `run` / `run_bounded`:** и `ProcessManager` (`tools/process.rs:1217`), и `pi-shell` (`shell.rs:1389,1521`), и все one-shot утилиты (hooks, ssh, `read_archive`, LSP linter, `rpc/mode.rs:362`, `pi-vcs/src/git/mutate.rs:631`) переходят на `pi_sys::exec::{run, run_bounded}`. `ProcessManager` остаётся только таблицей состояния и non-blocking reader.
3. **Единый `KillPlan`:** `HostTree` делегирует в `pi_shell::process::kill_process_group` (и `CreateJobObject` на Windows), удаляя дубли (`tools/process.rs:2776`, `rpc/mode.rs:401-403` и др.); `ContainerScope` отменяет `scope_id` на стороне брокера/демона.
4. **Единый PTY-шов:** `IoPlan::Pty` переносит `portable-pty` в `pi-sys`, сохраняя инвариант «отмена запуска не оставляет живого дочернего процесса» (`tools/process.rs:2668-2671`).
5. **Проверка в CI:** `rg 'Command::new|pre_exec\(' crates --glob '!crates/pi-sys/**'` (с поэтапным дожигом оставшихся вызовов до нуля).
