# Track 1: Выборочная синхронизация `OMP` / `omp-termux` → `arag` (2026-11-27)

> **Источники:** `ResearchOmpSync2` + аудит `ResearchReviewCoreEcosystem`.
> **Якоря:** `~/rust-agent` (`feat/standalone-runtime` @ `3b021a58`), `~/omp-work` (`3008cfd0`, upstream pin `0b5c27ab`), `~/omp-termux` (`61325cdd`, tracked upstream `e1a9f4b4`), upstream `can1357/oh-my-pi` (`v19.0.0`, `v19.1.0`, `v19.2.1`).
> **Границы проверки:** статическая инспекция репозиториев и release notes upstream. Локальный git-объект `e1a9f4b4` отсутствует в shallow-истории `omp-termux` (`tracked-pin` файл подтверждает пин, не содержимое коммита); `cargo`/тесты на устройстве не запускались.

---

## 1. Базовые пины

| Репозиторий | Текущий HEAD / пин | Примечание |
|---|---|---|
| `~/rust-agent` (`arag`) | `3b021a58` (`feat/standalone-runtime`) | Полностью Rust workspace (`aragent`, `pi-agent`, `pi-builtins`, `pi-shell`, `pi-ast`, `pi-iso`, `pi-vcs`, `pi-tui`, `pi-walker`) |
| `~/omp-work` | `3008cfd0` (upstream pin `0b5c27ab`) | Смешанный TS + `crates/omp-cli` донор |
| `~/omp-termux` | `61325cdd` (tracked pin `e1a9f4b4`) | Termux/Bionic патчи и Android-специфичные фиксы |
| Upstream `oh-my-pi` | `v19.1.0` / `v19.2.1` | Содержит более свежие фиксы MCP, background jobs, `sed`, cache warming |

---

## 2. Таблица кандидатов и вердикты ревью

| Область | Источник → Цель в `arag` | Статус в `arag` (проверено ревью) | Вердикт | Риски и условия |
|---|---|---|---|---|
| **Android `sort` OOM / memory cap** | `omp-termux` → `crates/pi-builtins/src/sort.rs:113–120` | `available_memory` реализован только для Linux; на Android падает к общему физическому RAM | **Port (адаптировать)** | Сбор реального `MemAvailable` на Android оправдан, но влияние OOM/LMK требует замера на устройстве |
| **Bionic `inotify_rm_watch`** | `omp-termux` `61325cdd` (конверсия `c_int`/`u32` аргумента) | Нужен там, где задействован сырой `inotify` на Bionic | **Port при использовании `inotify`** | Проверить exact ABI `libc::inotify_rm_watch` для `aarch64-linux-android` |
| **Android `pidfd` пре-API-31 `SIGSYS`** | `omp-termux` `61325cdd` | В `arag` kill дерева процессов идёт через `process_tree_kill.rs` (`TERM→KILL` по pgid) | **Keep pgid / Probe** | Не полагаться на `pidfd_open`/`pidfd_send_signal` на Android без fork-изолированной пробы (на API < 31 ловит `SIGSYS`) |
| **MCP reconnect / retry** | Upstream `oh-my-pi` 19.1/19.2 → `crates/pi-agent/src/mcp/client.rs:565–635, 915–940`, `mcp/timeout.rs` | **Поправка ревью:** утверждение «у `arag` нет retry в MCP» неверно. `tools/list` уже переподключается и ретраит; `tools/call` **намеренно** не повторяет `UnknownOutcome`. Дефолтный таймаут 30s (`timeout.rs`), настраивается/отключается | **No-Go на слепой порт** | Повтор `tools/call` нарушает R1/R2 инвариант (никакого автоповтора мутаций с неизвестным исходом). Ретраить допустимо только доказанную недоставку до отправки кадра |
| **Background process cleanup** | Upstream `oh-my-pi` 19.1/19.2 → `crates/pi-agent/src/tools/process.rs`, `pi-shell` | Supervisor/detached процессы, RPC lifecycle | **Adapt (высокий риск)** | Не мелкий порт: нельзя сносить намеренно detached/supervisor процессы; учитывать PID reuse, RPC-семантику, session ownership и Android Phantom Process Killer |
| **`sed` edge cases** | Upstream release notes 19.1/19.2 → `crates/pi-builtins/src/sed.rs` | Не доказано, что баг есть в Rust-порте | **Verify first** | Release notes TS/fork-версии не доказывают дефект в `pi-builtins`. Сначала воспроизвести конкретным unit-тестом |
| **Cache warming / retained workspaces** | Upstream release notes 19.1/19.2 | Контракты кэша и worktree | **Verify first** | Проверить на совместимость с R7 (`pi-vcs` + `task_isolation.rs`) и prompt/KV cache; не переносить как drop-in |

---

## 3. Выводы по Track 1

1. Единственный прямо подтверждённый перенос с низкой ценой и ясной пользой — memory-aware лимит для Android в `crates/pi-builtins/src/sort.rs:113–120` (и учёт `inotify_rm_watch` по `61325cdd`).
2. Всё связанное с retry (`mcp/client.rs`) и жизненным циклом фоновых процессов несовместимо с прямым копированием из upstream без сохранения инвариантов R1/R2/R3 `arag`.
