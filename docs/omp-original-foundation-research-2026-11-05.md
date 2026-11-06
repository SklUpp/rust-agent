# Оригинальный OMP как основа нового Rust-ядра

Источник только **can1357/oh-my-pi**, не SklUpp. Снимок [035e4f95c314f9aff2f5a779fa335422680f8e36](https://github.com/can1357/oh-my-pi/commit/035e4f95c314f9aff2f5a779fa335422680f8e36), 2026-11-04, workspace 18.16.0. Три read-only агента проверяли harness, native crates и границы runtime. Сборка/запуск не проводились. Это source audit, не доказательство Android-совместимости.

## Главный результат

**Да: оригинал — основа поведения и донор полноценных Rust-подсистем. Нет: на этом снимке из него нельзя импортировать готовое Rust-ядро агента.** Agent loop, провайдеры, policy, sessions и orchestration остаются TypeScript/Bun; Rust-перенос инструментальных подсистем уже существенный. Не считать весь Rust-код лишь мелкими ускорителями: `pi-edit` содержит edit engine и streaming session, `pi-shell` — shell engine с host APIs. Но это ещё не Rust agent lifecycle.

Доказательства: [workspace members](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/Cargo.toml) перечисляют native crates, без `pi-agent/omp-cli`; [root package scripts](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/package.json) запускают Bun CLI. [Agent exports](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/index.ts), [loop](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/agent-loop.ts), [agent](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/agent.ts), [session owner](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/session/agent-session.ts).

Следовательно, пользовательская идея собственной композиции актуальна: взять Rust engines напрямую, а свой session/effect/policy kernel написать или выбрать отдельно. Альтернативный путь — сохранить TS harness и общаться RPC; тогда это не полностью Rust execution core. Полный порт TS semantics — третий путь, но не обязательный: можно определить меньший собственный контракт.

## Конкретные границы заимствования

Все пути ниже относительно [закреплённого upstream tree](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36). Для native строк проверены Cargo.toml и src/lib.rs; «да» относится к наличию шва, не к успешной Android-сборке.

| Компонент | Проверенный шов | Решение |
|---|---|---|
| `crates/pi-edit` | `FileSource`, `EditStore`, `ModeEngine::preview/stage`, `StagedFile`, host-owned `EditWriter`; ArgStream и Session для progressive preview/final apply | Да первоочередному донору. Свой writer обеспечивает полномочия, конфликты и запись. Не переписывать hashline engine без причины |
| `crates/pi-ast` | parse/search/summary, SupportLang; `default-features=false` выключает Wasm/Wasmtime path, linked grammars остаются | Да выбранным нативным языкам; transitive VFS/walker и C grammars остаются частью сборки |
| `crates/pi-vfs` | FileSystem/Fs/BlockingFs, path helpers и host operations | Да native host adapter; BlockingFs использует block_in_place/block_on, нельзя вызывать на current-thread Tokio runtime как будто без ограничений |
| `crates/pi-iso` | IsolationBackend, probe/start/stop/clone_tree/diff, BackendKind | Да Rcopy/workspace isolation. На Android `native()` по cfg выбирает Rcopy, не Linux reflink. Это управление workspace, не security sandbox |
| `crates/pi-vcs` | Repo Git/Jj, status/diff/log/commit/ls_files, Feature | Да выбранным VCS операциям. Git CLI нужен некоторым paths, включая signing/network/reftable; `jj-lib` увеличивает graph зависимостей |
| `crates/pi-builtins` | Per-utility features, `default-features=false`, Host и extras | Да выбранным builtins, не всему набору по умолчанию. Есть onig/uucore/libc и Unix deps; `proc_sys` экспортируется linux/macos/windows, не Android |
| `crates/pi-shell` | Bash engine, persistent session и OS/VFS hosts | Да отдельному execution backend после проверки host contract; не leaf и не permission boundary |
| `crates/pi-natives` | N-API cdylib aggregate, module_init, version stamp, custom runtime/crash handling | Нет как Rust/Kotlin kernel library. Использовать crates под ним; новый C/UniFFI boundary вместо переноса addon loader |

Точный шов edit: [lib.rs](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-edit/src/lib.rs). Остальные: [VFS](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-vfs), [AST](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-ast), [ISO](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-iso), [VCS](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-vcs), [builtins](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-builtins), [shell](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-shell), [N-API manifest](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-natives/Cargo.toml).

## Что переносить как контракт, а не Rust-библиотеку

1. **Agent semantics:** канонические blocks, terminal reasons, replay policy, tool scheduling, speculative execution, pause и output budgets. Наличие speculative module не означает безопасное speculative выполнение side effects; policy остаётся обязательной. Источник — agent exports и loop выше.
2. **Session tree:** JSONL v3, parent links, leaf, compaction/branch/reset entries; transcript и provider context различаются. Нельзя заменить всё списком TextDelta. [Session manager](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/session/session-manager.ts), [context reconstruction](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/session/session-context.ts), [session docs](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/session.md).
3. **Различные reset операции:** new, fresh, clear, fork, resume и switch имеют разную идентичность, provider state и durable boundary. Для нового продукта определить свой минимальный набор, а compatibility adapter явно переводит каждую операцию. Нельзя всем отвечать «очистить историю». [Session operations](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/session-operations-export-share-fork-resume.md).
4. **Provider affinity:** providerPromptCacheKey, task cache inheritance, credential_pin, model/thinking/service tier и usage purpose должны жить вне обычного текста чата. Не путать cache identity с authorization. [Entry taxonomy](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/session/session-entries.ts).
5. **Context pipeline:** upstream сочетает prune/compression, LLM summary/handoff, snapcompact и provider-native replay. Новому ядру нужна стратегия по модели и бюджету, не один универсальный regex-компрессор. Safe boundaries сохраняют call/result pairing; summary не заменяет архив. [Compaction](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/compaction.md), [handoff](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/handoff-generation-pipeline.md).
6. **Subagent lifecycle:** isolation, mailbox/steering, typed yield, park/revive, cost limits и cancellation должны иметь владельца. Подсистема шире `tokio::spawn`. [Lifecycle manager](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/registry/agent-lifecycle.ts), [agent hub](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/agent-hub.md).
7. **Learning и skills:** сами схемы памяти, консолидация и policy загрузки не возникают из SQLite; воспроизводить provenance, scope и пользовательский контроль. [Memory](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/memory.md), [autolearn source](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/autolearn).

## Ранее упущенные подсистемы оригинала

- Разделение provider/account/session identity, model usage по purpose/role, service tier, configured vs resolved thinking и fallback model state. Эти metadata влияют на повторное воспроизведение и стоимость, не только UI.
- Idle maintenance и prompt-cache warming могут противоречить энергобюджету телефона; scheduler должен различать user work, maintenance и speculative work.
- Контекст как изображения `snapcompact` требует vision-модели и подходящего quality/cost режима; это не бесплатная универсальная lossless-компрессия. Форматы, retry и budget описаны в compaction, переносить числа между моделями нельзя.
- Export/share включает не только чат, но system/tool definitions, subSessions и связанные ресурсы. Redaction и attachment policy проверяются отдельно. Viewer может зависеть от сети; native offline UI не должен полагаться на него. [Share implementation](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/export/share.ts).
- Blobs и artifacts — часть persistence/GC, не случайные временные файлы. [Blob artifact architecture](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/blob-artifact-architecture.md).
- Новые команды/режимы, browser relay, collab/web и observability существуют рядом с harness; включение всего монорепозитория переносит их runtime/обновления. Listing пакета не считается проверкой всех его функций.

## Toolchain и Android: точные границы утверждений

Workspace задаёт edition 2024/resolver 3/Rust 1.101; `pi-natives` дополнительно содержит nightly feature и post-link stamp. При заимствовании pure engines не нужно автоматически переносить весь addon build. Сохранить необходимые vendor patches: grep-searcher push API, napi lifetime fix, tree-sitter Go grammar и cfg_aliases. Патч napi нужен только при сохранении соответствующего пути, не обязателен новому Kotlin host.

`cfg(unix)` включает Android, `cfg(target_os="linux")` — нет. Это подтверждает необходимость анализа каждой ветки, но не доказывает падение или успешную сборку. Проверенные manifest включают C/native зависимости; NDK, ELF page alignment, thread sizing и cancellation должны проверяться выбранным артефактом. Ни агентское чтение, ни успешный desktop CI не дают вердикта готового APK.

Root MIT не значит, что все transitive dependencies MIT. Зафиксировать Cargo graph, C grammars, native engines и notices. Не переносить panics через FFI; callback ownership, cancellation и Kotlin lifecycle — новая host-ответственность, которой N-API loader не решает.

## Окончательный выбор основы

| Вопрос | Ответ |
|---|---|
| Оригинальный OMP полезен как главная архитектурная reference implementation? | Да |
| Можно напрямую взять готовый upstream Rust AgentLoop/AgentSession? | Нет на указанном снимке: их implementation TS |
| Стоит использовать оригинальные pi-edit/pi-ast и выбранные VFS/shell/VCS механизмы? | Да, через проверенные host seams и минимальные features |
| Нужно обязательно переносить весь TS harness в Rust? | Нет: своё ядро может реализовать другой, явно ограниченный контракт |
| Можно пока оставить оригинальный OMP отдельным runtime для совместимости? | Да, через RPC и проверку конкретных версий; тогда Bun остаётся частью поставки |
| Доказана готовность этих crates как единого native Android APK? | Нет; source audit не является NDK/device validation |

Связанные результаты: [архитектурные варианты](rust-agent-research-2026-11-05.md), [компоненты](rust-agent-component-verdicts-2026-11-05.md), [9router](9router-integration-research-2026-11-05.md). Проверка документа: `test -s docs/omp-original-foundation-research-2026-11-05.md`.

## Проверенные tool/policy/hooks исходники оригинала

- [agent-loop.ts:636–900](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/agent/src/agent-loop.ts#L636-L900): `agentLoop`, `agentLoopContinue`, `unpairedToolCallTail`, `createAgentStream`. Resume допускает определённый незавершённый tool tail, но не length-трункейшн; `agent_end` завершает EventStream. Это source-факт о прочитанном срезе, не анализ всего большого runLoop.
- [HookToolWrapper.execute](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/extensibility/hooks/tool-wrapper.ts): `tool_call` hooks до вызова, block/ошибка блокируют выполнение; возможна замена input, кроме computer calls; затем `tool.execute`; `tool_result` hooks могут менять content/details, но не снимать `isError:true`. Ошибка инструмента остаётся ошибкой и после hook-обработки.
- [approval.ts:301–600](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/tools/approval.ts#L301-L600): `resolveApproval`, `resolvePolicy`, `resolveCachedModeApproval`, `resolveToolTier`. Порядок учитывает tool tier, user policy, mode/cache и restricted-folder reason; `yolo` не означает игнорирование authoritative user policy. В новом ядре отделить policy decision от UI approval, а не копировать название режима как полномочие.
- [HookRunner](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/extensibility/hooks/runner.ts#L301-L446): регистрационный порядок, ранний return при tool_call block, последовательное объединение patches/context. Dispatch не имеет deadline намеренно: пользовательские prompts могут требовать неограниченного ожидания. [Hook types](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/extensibility/hooks/types.ts#L301-L608) различают `sendMessage` (model context) и `appendEntry` (состояние без LLM).

Предложение для нового дизайна: deadline для command/automation hooks, отдельный user-prompt lifecycle с отменой/expiry/reconnect; явные trust level и устойчивость результата, ограничение изменения args после approval. Это не заявление об эксплуатируемой уязвимости оригинала и не требование обрывать живой диалог фиксированным таймером. Утверждения о TTSR, park/revive и compaction method order выше опираются на docs, не на полное чтение runtime paths.

## Порядок выбора: оригинал, затем разрешённый Rust-форк

Уточнение пользователя: Rust-форк **разрешён для переиспользования**, но после изучения оригинала. Исследование оригинального harness и Rust engines выполнено выше; теперь для каждого выбранного компонента сопоставлять upstream контракт с реализацией [SklUpp на 3008cfd](https://github.com/SklUpp/omp-rust-termux/tree/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates). Переиспользовать fork loop/provider/supervisor допустимо, если они покрывают нужный собственному ядру контракт; не требуется писать их заново только ради независимого проекта.

Практический выбор: оригинальные engines как исходная точка; fork — кандидат готового Rust harness и адаптеров; собственный код — для действительно новых state/effect/context/Android host контрактов. Это не запрет форка и не обязательство портировать весь TypeScript. Свойства и паритет каждой реализации остаются отдельными, проверяются по соответствующему источнику и сценарию.

