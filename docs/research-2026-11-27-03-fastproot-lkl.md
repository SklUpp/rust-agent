# Track 3: Интеграция `fastproot` + `lkl` и единая модульная кросс-OS система (2026-11-27)

> **Источники:** `ResearchFastprootLkl2` + аудит `ResearchReviewPlatform`.
> **Якоря:** `~/tmp/hybrid-fastproot-lkl-spec.md`, `~/tmp/lkl-research-and-plan.md`, `~/tmp/fastproot/{Makefile,src/fastproot.c}`, `~/tmp/lkl-probe/{lkl_daemon.c,lkl_runner.c,lkl-wrap.c,net_shim.c,bionic_compat.h,kbox/}`, `~/rust-agent/crates/{pi-shell/src/,pi-iso/src/,pi-agent/src/tools/,aragent/src/approve.rs}`.
> **Границы проверки:** только статический разбор исходников и черновиков. Число ~147 MB из черновика не перепроверялось на реальном установленном пакете; сборки NDK и тесты на Android не запускались.

---

## 1. Сравнение `fastproot` vs `lkl` vs `hybrid` и подтверждённые баги PoC

| Свойство | `fastproot` (`~/tmp/fastproot`) | `lkl` (`~/tmp/lkl-probe`) | `hybrid` (проект) |
|---|---|---|---|
| **Принцип работы** | `LD_PRELOAD` (перехват libc) + `seccomp-unotify` на `execve`/пути (если ядро разрешает) | Userspace Linux kernel (`liblkl.a`) + `lkl_daemon` на Unix socket + `lkl-wrap.so` | Быстрые host-утилиты через `fastproot`; glibc/stat/ext4/несовместимые бинарники через LKL |
| **Статические и Go бинарники** | **Не перехватываются `LD_PRELOAD`**; при отсутствии `seccomp-unotify` уходят мимо прослойки | Зависят от режима перехвата (ptrace/seccomp/wrap) | Выбор маршрута по ELF/типу бинарника |
| **Подтверждённые дефекты PoC** | 1) `g_child_pid` пишется после `fork`, обработчик бьёт только по `g_child_pid`, нет сверки `PR_SET_PDEATHSIG` (`fastproot.c:25–28,699,791`); 2) cleanup идёт после `waitpid` → `SIGKILL` оставляет `.fp-*` (`:791–812`); 3) `patchelf --remove-rpath` в `Makefile:20,28` опционален (реальный `RUNPATH` артефакта не проверялся) | 1) **`lkl_daemon.c:178–200` перехватывает лишь выбранный список syscall и пропускает остальные**; 2) zygote форкает обычные host-процессы (`:425–479`), а не потоки внутри демона; 3) socket `0700` без `SO_PEERCRED`, `EXEC` берёт произвольный путь (`:510–542`); 4) `open`-флаги транслируются списком (неперечисленные молча теряются, `:73–91`); 5) `VFD_BASE=100000` сломается при host FD ≥ 100k; 6) `net_shim.c` задаёт локальный `ifreq` (корректность для всех ABI не доказана) | Наследует ограничения обоих |

> **Критический вердикт `ResearchReviewPlatform`:**
> Текущий `lkl_daemon.c` PoC **НЕ является границей безопасности для недоверенного кода**. Из-за неполного перехвата syscall, отсутствия аутентификации сокета (`SO_PEERCRED`) и host-процессов из zygote, тезисы о том, что недоверенный код не доберётся до данных приложения или что host killpg гарантированно убьёт все задачи в guest scope, **не доказаны**. Полная медиация syscall, аутентификация сокета, per-scope отмена и конкурентность — это обязательные требования к проектированию, а не готовые свойства PoC.

---

## 2. Необходимые исправления до интеграции

1. **`fastproot`:**
   - Сигналы группе процессов (`-pid`): при доработке обязательно сохранить интерактивное TTY/job-control поведение и учитывать потомков, вызывающих `setsid()` (`kill(-pid, sig)` не является универсальным убийством всего дерева).
   - Проверять возврат `PR_SET_PDEATHSIG` в дочернем процессе.
   - Уборка `.fp-*` на старте не должна сносить все подходящие по маске файлы слепо — нужно отличать живые сессии и общие кэши от осиротевших.
   - Проверить реальный `RUNPATH`/`DT_RPATH` собранного `libfastproot_loader.so` (`patchelf --remove-rpath`).
2. **`lkl-probe`:**
   - Запретить пропуск неперехваченных syscall в `lkl_daemon.c`; перейти к модели с полной медиацией или честному статусу «compatibility layer без sandbox-гарантий».
   - Добавить `SO_PEERCRED` + per-session auth token на Unix-сокете; проверять разрешённые пути и инвариант `FD < VFD_BASE` (или динамическое выделение).
   - Разделять стоимость статического архива (упомянутые ~147 MB не верифицированы) и реальный размер установленного/сжатого rootfs.

---

## 3. Модульная кросс-OS архитектура в `arag`

### 3.1 Швы интеграции (`crates/pi-shell` + `pi-agent`)
- Точки запуска: `pi-shell/src/{process.rs,shell.rs}`, `pi-agent/src/tools/{mod.rs,process.rs,task_isolation.rs}`, `eval/kernel.rs`, `lsp/client.rs`, `process_broker/client.rs`.
- **Поправка ревью к `LaunchPlan`:** структуры только с `(program, argv, env)` недостаточно. Для `lkl` / удалённых демонов нужен **контракт daemon/RPC исполнения**:
  - admission control (квота/семафор);
  - streaming stdout/stderr;
  - lifecycle и отмена (`KillPlan::ProcessGroup` локально vs explicit `DaemonCancel(scope_id)` в демоне);
  - явное состояние `UnknownOutcome` при потере связи.
- **Учёт планировщика (`tools/scheduler.rs:235–249`):** сейчас `scheduler.rs` явно отклоняет `task` при включённой durability-персистентности — этот разрыв надо учитывать при проектировании durable execution поверх изолированных бэкендов.

### 3.2 Кросс-OS матрица бэкендов (под одним интерфейсом `ExecutionBackend`)

| ОС | Доступные бэкенды | Назначение | Граница безопасности |
|---|---|---|---|
| **Linux (desktop/server)** | `Native` (`pi-shell` + pgid) · `KernelSandbox` (отложенные `bwrap`/`landlock`/`seccomp` после capability probe) | Изоляция под тем же UID при поддержке ядра | `KernelSandbox` при полной настройке; `Native` — без изоляции ФС |
| **Android / Termux** | `Native` · `Fastproot` · `Lkl` / `Hybrid` | Совместимость rootfs / FHS / glibc (`Fastproot`/`Lkl` как **compat layer**) | Под одним UID и без полной syscall-медиации — **не sandbox** |
| **macOS / прочие** | `Native` | Базовое исполнение | Без изоляции ФС (sandbox-профиль отдельно) |

- **Неизменный верхний гейт:** `BashPolicy` (`crates/pi-shell/src/policy.rs`, `aragent/src/approve.rs:447–485`) всегда проверяет **сырую команду до маршрутизации в бэкенд**. Выбор `fastproot`/`lkl`/`native` никогда не отменяет `Forbidden` или обязательный prompt.
