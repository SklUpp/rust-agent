# Сводный аудит независимых ревью по исследованиям 2026-11-27

> **Ревьюеры:** `ResearchReviewPlatform` (Tracks 3, 4, 7) и `ResearchReviewCoreEcosystem` (Tracks 1, 2, 5, 6).
> **Вердикт обоих ревью:** **`GAPS FOUND`**.
> **Границы проверки:** инспекция полных отчётов 7 скаутов, выборочная сверка с исходниками `~/rust-agent`, `~/omp-work`, `~/omp-termux`, `~/tmp/fastproot`, `~/tmp/lkl-probe`, `~/9router-termux`, `~/OmniRoute`, проверка лицензий/ссылок. Код не менялся; сборки, тесты на устройстве, syscall-пробы и живые замеры не выполнялись.

---

## 1. Главные сквозные замечания (Critical / P1)

1. **LKL daemon PoC (`~/tmp/lkl-probe/lkl_daemon.c`) НЕ является границей безопасности (`ResearchReviewPlatform`):**
   - `:178–200` перехватывает только выбранный список syscall и **пропускает все остальные**.
   - `:425–479` zygote запускает обычные host-процессы вместо потоков внутри демона.
   - `:510–542` сокет без `SO_PEERCRED`, `EXEC` принимает произвольный путь.
   - `:73–91` `VFD_BASE=100000` конфликтует при host FD ≥ 100k; `open`-флаги копируются списком с тихой потерей остальных.
   - **Решение:** классифицировать текущий PoC строго как *compatibility layer*, а не sandbox.
2. **D20 trust & утечка окружения в `ssh.rs` и `lsp/` (`ResearchReviewPlatform`):**
   - `crates/aragent/src/trust.rs:34–49` защищает hooks/MCP, но **не покрывает** `.omp/ssh.json` (`ssh.rs:864–882`) и project LSP `command` (`lsp/config.rs:290–320`).
   - `ssh.rs:356–367` (наследует `SSH_AUTH_SOCK`, отсутствие `-A` не гарантирует запрет forwarding при внешнем ssh config) и `lsp/client.rs:860–873` (передаёт весь `env`, включая API-ключи) работают без `env_clear()`.
3. **`UnknownOutcome` не доведён до remote execution (`ResearchReviewPlatform`):**
   - `DurableError::unknown_outcome` относится к записям durability, а `McpError::UnknownOutcome` — к MCP; для remote exec такого durable-статуса нет.
   - `tools/scheduler.rs:235–249` сейчас отклоняет `task` при активной durability.
4. **Границы гарантий `WorkspaceWriter` и `MERGE_LOCK` (`ResearchReviewPlatform` & `ResearchReviewCoreEcosystem`):**
   - `workspace_writer.rs:1–23, 125–153` — только per-file `flock` lease + full-byte check, **без** cross-file атомарности при I/O-сбое/краше и без защиты от произвольного `bash` или гонок `.git/index.lock`. `MERGE_LOCK` (`task_isolation.rs:86`) — только process-local.
5. **Approval tier ≠ Scheduler exclusivity (`ResearchReviewCoreEcosystem`):**
   - `approve.rs:113–200` классифицирует по имени/аргументам, а `scheduler.rs:80–105` планирует по `Tool::is_exclusive()`. Смешивать их нельзя; `checkpoint`/`rewind` (`checkpoint.rs:494–595`) строго эксклюзивны; `goal` (`goal_tool.rs:116–159`) содержит мутирующие действия и не может целиком стать `Read`.
6. **Тихое отбрасывание в `patch-9router.mjs` (`ResearchReviewCoreEcosystem`):**
   - `~/9router-termux/patch-9router.mjs` правит минифицированные чанки и в `:66–75` молча теряет неподдержанные Gemini tools и non-text `content-parts` (**No-Go** для coding-агента).
7. **Неверные допущения по Android capability probing и Ядрам (`ResearchReviewPlatform`):**
   - `uname` и таблица версий ядра не доказывают поддержку `pidfd`/`landlock`/`seccomp_unotify`/`user_ns` на Android; `eval/probe.rs:91–100` отбрасывает stdout/stderr; комбинированная проба умрёт на первом `SIGSYS`. Каждая `SIGSYS`-опасная проба должна быть отдельно fork-изолирована.
   - `targetSdk 28`, `LD_LIBRARY_PATH`, `memfd_create`/`MFD_EXEC` и защита `wake_lock` от Phantom Process Killer не имеют универсальных гарантий без проверки политики конкретного устройства.

---

## 2. Мастер-таблица решений по всем 7 трекам

| Трек | Документ | Go (брать / делать) | Adapt / Verify first | No-Go / Отложить |
|---|---|---|---|---|
| **1. OMP sync** | `research-2026-11-27-01-omp-sync.md` | Android `available_memory` в `sort.rs:113–120`; `inotify_rm_watch` (`61325cdd`) при использовании `inotify` | Background cleanup (ownership/PID-reuse/Android), `sed`/cache warming (сначала воспроизвести) | Слепой retry `tools/call` в MCP (`mcp/client.rs:565–635`) |
| **2. Инструменты** | `research-2026-11-27-02-tools-unification.md` | Bounded `run_concurrent` (`scheduler.rs`), инвалидируемый кэш схем, вынос blocking I/O | `WalkSpec` + structured truncation, `ast_grep`+`ast_edit` (сохраняя `PendingStore` и `xd://resolve`), `multi_edit`/`multi_read` с explicit partial failure | `goal` целиком в `Read`, параллельный `checkpoint`/`rewind` |
| **3. `fastproot`/`lkl`** | `research-2026-11-27-03-fastproot-lkl.md` | `ExecutionBackend` + daemon/RPC `LaunchPlan` (admission, streaming, cancel, `UnknownOutcome`), `BashPolicy` на входе | Фикс `fastproot.c` (`-g_child_pid` + TTY/setsid, безопасная уборка `.fp-*`, `RUNPATH`), фикс `lkl_daemon` (`SO_PEERCRED`, FD < `VFD_BASE`) | Использование текущего LKL PoC как sandbox для untrusted кода |
| **4. Android SDK/Ядра** | `research-2026-11-27-04-android-sdk-kernel.md` | Отдельные fork-изолированные syscall-пробы + кэшируемый runtime-профиль в `arag doctor` | Дифференциация `targetSdk`/SELinux/seccomp, DNS/TLS проверки на реальных устройствах | Опора на `uname`, общий probe-процесс, `eval/probe.rs:91–100`, `memfd_create` как W^X-обход |
| **5. Экосистема / Профили** | `research-2026-11-27-05-ecosystem-crates-profiles.md` | `wasmi` (с fuel/memory/host caps), `regorus`/`cedar-policy`, `frizbee`, `wezterm-term` (cap scrollback), `device_profile` (fail-open) | `wreq` (проверить Bionic/BoringSSL), `ra_ap_*`/`stack-graphs`/`oxc_*` (только desktop/opt-in), opt-in reversible compression | `tensorzero` (archived), `nucleo` (MPL-2.0), `new-api` (AGPL-3.0), `sqlite-vec` (defer), «Волна 1 без риска» |
| **6. `9router` / Форк** | `research-2026-11-27-06-9router-fork-sync.md` | Pattern A (pinned upstream + `127.0.0.1` + случайные секреты), D27 `DATA_DIR` denylist, единый владелец retry & OAuth | Pattern B (<100 LoC в исходниках), `GatewayBackend` + identity-checked adoption, `9routerx` после проверки Android Bionic | `patch-9router.mjs`, усыновление по одному `/health`, общая `data.sqlite` между версиями, `max-old-space-size=6144` |
| **7. Мультиплатформа** | `research-2026-11-27-07-multiplatform-fallback.md` | M0: D20 на `.omp/ssh.json` & project LSP + `env_clear()` + `read.rs:688` ACP fix; `Exclusivity::Key`; `arag __omp_worker_remote`; SHA-256 CAS | Полный `PathMap` (`LocationLink`/`WorkspaceEdit`/внешние deps/TOCTOU), cross-OS Git rules, read-only LSP degrade в `pi-ast` | Автоповтор мутаций при обрыве, `xxh3_64` для untrusted CAS, тихая миграция `rename`/`format` |
