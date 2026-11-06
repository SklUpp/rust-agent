# Frontend research: TUI, CLI events, headless single client

Дата: 2026-11-06. Метод: чтение локального форка, docs, pinned upstream-файлов и crates.io/docs.rs. Код не собирался, тесты не запускались. Продуктовая архитектура сознательно не выбирается.

Теги: **[Ф]** — проверенный исходник; **[О]** — оценка/следствие; **[?]** — не проверено.

## 0. Коротко

- **[Ф]** В Rust-форке TUI отсутствует: `crates/omp-cli/src/repl.rs` — построчное чтение без raw mode, истории и виджетов; TUI-зависимостей нет в `Cargo.lock`.
- **[Ф]** Оригинальный OMP TUI — не ratatui-подобный cell-buffer renderer. Контракт: `Component::render(width) -> readonly string[]`, отдельные history/viewport планы, ANSI-строки, нативный scrollback, OSC/kitty-возможности, TSP `describe()`.
- **[Ф]** `ratatui` 0.30.2 не содержит input handling, работает с `Buffer`/`Text`/`Span` и не принимает ANSI-строки как контент; полноэкранный режим и inline viewport — разные модели, native scrollback напрямую не сохраняется.
- **[Ф]** `reedline` 0.52.1 — самостоятельный line editor, владеет терминалом; внутри ratatui-экрана не работает. Для TUI-форм нужны `tui-input`, `edtui` или собственные виджеты.
- **[Ф]** `tui-textarea` 0.7.0 требует `ratatui ^0.29`, поэтому несовместим с `ratatui 0.30.2` без дублирования типов.
- **[Ф]** CLI JSON-контракт уже реализован в Rust: NDJSON в stdout, диагностика/прогресс в stderr, RPC только в stdout; ACP — отдельная stdio-подкоманда, один активный peer.
- **[Ф]** Worker viewer в Rust уже имеет данные: `SubagentSnapshot`, `subagent_lifecycle`, `subagent_progress`, `subagent_event`, `TranscriptSlice`, `read_transcript`. Оригинальный Agent Hub завязан на TS `AgentRecordLike`/`ObservableSession` и напрямую не переносится.
- **[?]** Поведение ratatui/crossterm/reedline на Termux (aarch64-linux-android, Bionic) не проверялось.

## 1. Требования (из принятых/предварительных решений)

| ID | Статус | Что следует для frontend |
|---|---|---|
| D03 | принято | TUI — основной интерфейс интерактивного режима и только при TTY. Не-TTY/`--print` → headless. `--mode json` — NDJSON. RPC — stdio. ACP — `omp acp`, отдельный stdio-клиент. `launch` не форсирует TUI. |
| D37 | принято | Frontend работает через единый command/event contract. JSON-режим — те же события, что TUI. Frontend не владеет turn loop, tool execution, session persistence. |
| D56 | принято | Один активный controlling client на сессию; один writer. Второй клиент не подключается параллельно. Доставка событий после реконнекта — задача runtime, не UI. |
| D62 | принято, CLI verb set не заморожен | Обязательны text/JSON/RPC/ACP. Text: финальный ответ stdout, прогресс/диагностика stderr. JSON: только NDJSON stdout. RPC: только protocol frames stdout. Новые CLI-verb’ы не добавлять. |
| D63 | принято | Worker viewer: roster + detail/inspector, statuses, usage/metrics, transcript, revive/kill, persisted-agent scan, advisor read-only. |
| D69 | принято | Settings configurator: schema-driven paths, value controls (boolean/enum/int/string), submenu, preview, atomic apply, redaction секретов. Секреты не читаются через `config get`; UI не должен их показывать. |
| — | отложено | Конкретные key bindings, layout, темы, `@file` UX, editor UX, provider/storage internals, multi-client/attach, auth UI. |

**[О]** Для frontend это означает: слой представления может быть сменным, но контракт событий и владение сессией — общие. TUI не должен быть единственным местом, где существует настройка/форма/ваншот статуса воркера.

## 2. Оригинальный OMP: контракты (pin `035e4f95c314f9aff2f5a779fa335422680f8e36`)

**Важно:** локальное дерево `packages/*` — 18.0.9, а pin — 18.16.0. Пути и API заметно переехали: settings/ask/hub overlays на pin лежат в `packages/tui/src/overlays/`, а не в `packages/coding-agent/src/modes/components/`. Ниже — проверка по pinned-файлам; локальные docs использовать только как вторичное описание.

### 2.1 Renderer / terminal

Pinned `packages/tui/src/tui.ts`:

- `interface Component`
  - `render(width: number): readonly string[]` — физические строки, ANSI допустим; компонент владеет результатом и может возвращать тот же массив.
  - `handleInput?(data: string)`, `invalidate?()`, `setIgnoreTight?()`, focus/cursor hooks.
  - `describe?(cx: DescribeContext): NativeNode | null` — семантический рендер для Tern Surface Protocol; при наличии native backend вызывается вместо `render()`.
  - `nativeOverlay`, `nativeSheet`, fullscreen page metadata.
  - `debugId`, `debugKind`, `debugState()`, `debugChildren` для debug tree.
- `CURSOR_MARKER = "\x1b_pi:c\x07"` — маркер позиции hardware cursor внутри ANSI-строки; renderer находит его и скрывает из вывода.
- `TerminalFrameProvider`, `TerminalFramePlan { history?, viewport }`, `HistoryBatch { id, rows, kind }` — product-owned bounded frame provider: история и viewport разделены, history append-only/replay.
- `OverlayHandle` — `hide()`, `setHidden()`, `isHidden()`, dispose/focus-restore семантика; overlay stack.
- `TUI`: `setFrameProvider()`, `showOverlay()`, `setFocus()`, `start()/stop()`, `requestRender(force, options)`, `renderNow()`, `resetDisplay()`, `invalidate()`; resize handling с replay/settle, alternate-screen state для fullscreen overlays, postmortem restore hook.
- `ProcessTerminal`, `StdinBuffer` — raw input, escape-sequence parsing, resize/OSC/kitty probes; `TerminalInfo`/`TerminalCapabilities` (truecolor, images, kitty, sync output, DECCARA и т.п.).
- `matchesKey(data, keyId)`; `KeybindingsManager.matches(data, action)` — actions вроде `app.agents.hub`, `app.tools.expand`, `app.interrupt`.

**[О]** Это не «виджеты», а протокол кадров: renderer получает готовые ANSI-строки и сам решает, что дописать в native scrollback, что перерисовать, как обработать resize/overlay. Ratatui заменяет этот слой, а не переиспользует его.

### 2.2 Settings configurator

Pinned `packages/tui/src/overlays/settings-defs.ts`:

- `SettingTab`: `appearance | model | interaction | context | memory | files | shell | tools | tasks | providers`; `SETTING_TABS`, `TAB_METADATA`, `TAB_LEADS`, `TAB_GROUPS`.
- `SettingsDisplayEntry`: `path`, `type`, `defaultValue`, `ui?: AnyUiMetadata`, `enumValues?`, `credential?`, `condition?`.
- `AnyUiMetadata`: базовые label/description + `options?: ReadonlyArray<SubmenuOption> | "runtime"`, `secret?`, `ordered?`, `editor?: "json"`.
- `SettingsHost`: `entries`, `get(path)`, `set(path, value)`, `unset(path)`, `normalizeProviderLimits(value)`, `validateProviderLimits(value)`.
- Setting variants (`BaseSettingDef` +): `boolean`; `enum { values }`; `submenu { options, onPreview?, onPreviewCancel? }`; `text { secret }`; плюс `warning`, `group`, `condition`, `tab`, `label`, `description`.
- `SettingsList` (`components/settings-list.ts`): `SettingItem { id, label, description?, warning?, currentValue, values?, submenu?, menu?, changed?, defaultLabel? }`; `SettingEditor`; `SettingsListTheme`, `SettingsListOptions`; клавиатура и fuzzy-фильтр.
- `SettingsSelectorComponent` (`overlays/settings-selector.ts`) — вкладки/группы, список, редактор значения, подтверждение/сброс.

**[Ф]** Apply-модель: host `set()`/`unset()` меняет слой конфигурации; `unset` оставляет project/env/default слой. **[О]** Для Rust-порта нужен не только snapshot метаданных, но и семантика «preview → apply → atomic save → redaction».

### 2.3 Forms / ask dialog

Pinned `packages/tui/src/components/form.ts`:

- `Form`, `FormField`, `TextFormField`, `SelectFormField`; `FormFieldTheme`, `FormFieldOptions`, `TextFormFieldOptions`, `SelectFormFieldOptions`, `FormOptions`.
- `FormControl = Component & Partial<Focusable> & { handleInput(data); pasteText?(text) }`.
- `EmptyInputBehavior = "submit" | "cancel" | "reject"`; `TextFormFieldOptions.secret`; `SelectFormFieldOptions.items/currentValue/maxVisible`; live preview hook.
- `Form` dispatches focus/input across fields, cancel predicate, mouse routing.

Pinned `packages/tui/src/overlays/ask-dialog.ts`:

- `AskDialogComponent`, `AskDialogOptions`.
- `ExtensionAskDialogQuestion`: `question`, `header?`, `options[]`, `multi?`, `recommended?`; timeout/отмена — в options.
- Ответ: выбор option, custom text, note; multi-select; таймаут; отмена.
- В headless/print UI форма не показывается — операция должна fail/abort, а не висеть.

**[О]** Это форма с валидацией и отменой, а не просто `List`. В ratatui её надо собирать из `Block`/`List`/`Paragraph` + input widget + own state machine; `Form`/`FormField` — хороший источник контракта, но не переносимый код.

### 2.4 Worker viewer (Agent Hub)

Pinned `packages/tui/src/overlays/agent-hub.ts`, `agents-hub.ts`, `agent-transcript-viewer.ts`:

- `AgentHubOverlayComponent` / `AgentsHubComponent`.
- Вход: `AgentRecordLike` (`id`, `status`, `lastActivity`, `parentId`, `kind`, `sessionFile`, `history`, `createdAt`), `ObservableSession` (`description`, `progress`, `progress.modelRole`, `progress.task`), `AgentMetrics` (`cost`, `durationMs`, `requests`, `tools`, `tokens`, `contextTokens`, `contextWindow`).
- Roster/tree: parent-child, `MAIN_AGENT_ID`, rank freeze, status order; detail inspector: usage, context gauge, lineage, model, last activity, transcript.
- Actions: open transcript, revive/kill (где разрешено), persisted-agent scan; advisor — read-only.
- Remote/collab: отдельный `AgentHubRemote`/IRC unread counts; на узком терминале Tab переключает roster/inspector.

**[О]** Контракт viewer’а — не только список: статус + task + usage + lineage + transcript paging + действия. Rust-данные для этого уже есть, но UI-модель надо строить заново.

### 2.5 CLI stdout/stderr и события

Pinned `packages/coding-agent/src/modes/print-mode.ts`:

- `runPrintMode(session, options)`, `printableEvent(event)`.
- JSON: сначала `session` header, затем каждый `AgentSessionEvent` как NDJSON. `message_update` несёт только delta и метаданные; `providerPayload` и `partial` snapshot вырезаются; авторитетное сообщение — в `message_end`; финал — `agent_end` со всеми messages.
- Text: финальный текст stdout; `Working...`, retries, MCP warnings, persistence notices — stderr. Ошибки не должны попадать в stdout.
- stdout/stderr пишутся последовательно с учётом backpressure; финальный `agent_end` может быть multi-MB, поэтому нельзя `process.exit` до drain.
- Exit codes: 0 успех; 1 terminal failure/durability/MCP/strict/builtin/async timeout; 3 cost cap; 130/143/129 — SIGINT/SIGTERM/SIGHUP через postmortem.
- В print mode отключён UI и plan-review; plan.defaultOnStartup игнорируется с предупреждением в stderr.

Pinned `packages/coding-agent/src/modes/rpc/rpc-types.ts`:

- `ready`: `protocolVersion: 1`, `supportedProtocolVersions: [1, 2]`, `maxFrameBytes`, `maxReassembledFrameBytes`.
- `rpc_chunk` (`chunkId`, `index`, `count`) для больших frames; `negotiate_protocol`; `prompt` с `streamingBehavior: "steer" | "followUp"`.
- RPC stdout — только протокол; stderr — диагностика. Один stdio-клиент.

### 2.6 Rust-форк: что уже есть

Pin `3008cfd0ca88bb0e28323561a165f39efa7159e1`:

- `crates/omp-cli/src/output.rs`:
  - `OutputMode { Text, Json, Rpc }` (clap value enum).
  - `Reporter<W: Write, E: Write>`: `session_header()`, `begin_turn()`, `event()`.
  - NDJSON events: `session`, `message_start`, `message_update` (`text_delta`, `thinking_delta`, `done`/`error`), `message_end`, `tool_execution_start`, `tool_execution_update`, `tool_execution_end`, `turn_end`, `agent_end`; advisor cards — `message_start`/`message_end` с `customType: "advisor"`.
  - Text: `Working...` stderr, финальный текст stdout; thinking при `--print-thoughts`.
- `crates/omp-cli/src/repl.rs` — plain stdin loop, без raw mode/history/line editing.
- `crates/omp-cli/src/rpc/`:
  - `RpcMode`, `RpcSession`; `ready_frame(max_frame_bytes, max_reassembled_bytes)` → `protocolVersion: 1`, `supportedProtocolVersions: [1,2]`, `maxFrameBytes`, `maxReassembledFrameBytes`; `negotiate_protocol`.
  - `SubagentRegistry`, `SubagentSnapshot`, `TranscriptSlice`, `read_transcript(session_file, from_byte)`; события `subagent_lifecycle`, `subagent_progress`, `subagent_event`; RPC-команды `get_subagents`, `get_subagent_messages`.
  - `SubagentSnapshot` поля: `id`, `index`, `agent`, `agent_source`, `description`, `status`, `task`, `assignment`, `session_file`, `parent_tool_call_id`, `last_update`, `progress`.
  - `SubagentSubscriptionLevel { Off, Progress, Events }` — уровень подписки/backpressure.
- `crates/pi-agent/src/supervisor.rs`:
  - `ChildStatus { Running, Completed, Failed, Aborted }`.
  - `ChildRecord`: `id`, `result_key`, `name`, `index`, `agent`, `task`, `status`, `parent_id`, `started_at`, `duration_ms`, `output`, `error`, `aborted`, `abort_reason`, `output_id`, `output_published`.
  - `SubagentLifecycle`, `SubagentProgress`, `SubagentEvent`, `subscribe()`/`subscribe_once()`; observer bridge.
- `crates/omp-cli/src/rpc/subagents.rs`: observer bridge + transcript paging (`next_byte`, `reset`, `entries`, `messages`); `MAX_RETAINED_TRANSCRIPT_REFERENCES = 256`.
- `crates/omp-cli/settings-schema.snapshot.json` — снимок схемы настроек/UI-метаданных из TS; `config_cmd` умеет list/get/set с redaction.
- ACP — clap-подкоманда `Acp`, отдельный stdio-сервер; `--mode` принимает только `text|json|rpc`. `cli-reference.md` упоминает `--mode acp`/`rpc-ui` — **[О]** это расхождение docs с текущим enum.

**[Ф]** Ни одного ratatui/crossterm/reedline в `Cargo.lock` нет. Добавление = новые зависимости + network/offline provisioning.

## 3. Rust TUI доноры: версии, лицензии, API, границы

Все версии — crates.io на 2026-11-06. Лицензии верхнего уровня MIT (manifest package `license`), транзитивные — не аудированы.

| Crate | Версия / pin | License | Rust | Ключевой API | Роль / граница |
|---|---|---|---|---|---|
| `ratatui` | 0.30.2 (source commit `e665c36cb14752a61cd777fbd06dbef8474f2add`) | MIT | 1.88, ed. 2024 | `Terminal`, `Frame`, `Buffer`, `Rect`, `Layout`, `Constraint`, `Widget`, `StatefulWidget`, `Text`, `Line`, `Span`, `Style`, `Paragraph`, `List`, `Table`, `Tabs`, `Scrollbar`, `Clear` | layout/diff/render; нет input, нет ANSI-контента, нет scrollback |
| `ratatui-core` | 0.1.2 | MIT | 1.88 | `Buffer`, `Cell`, `Rect`, `Style`, `Text` | core types; re-export через `ratatui` |
| `ratatui-widgets` | 0.3.2 | MIT | 1.88 | widgets | только готовые widgets, форм нет |
| `ratatui-crossterm` | 0.1.2 | MIT | 1.88 | `CrosstermBackend` | backend; crossterm 0.29 |
| `crossterm` | 0.29.0 | MIT | 1.63 | `terminal::{enable_raw_mode, disable_raw_mode, size, window_size, EnterAlternateScreen, LeaveAlternateScreen, Clear, ClearType}`, `event::{poll, read, Event, KeyEvent, KeyCode, KeyModifiers, KeyEventKind, EventStream}`, `execute!`, `queue!`, bracketed paste | terminal I/O; `/dev/tty` fallback, TIOCGWINSZ, `tput` fallback |
| `reedline` | 0.52.1 | MIT | 1.95 | `Reedline::create()`, `read_line() -> Result<Signal>`, `Signal::{Success, CtrlC, CtrlD}`, `with_history`, `FileBackedHistory`, `DefaultPrompt`, `with_completer`, `with_hinter`, `with_validator`, `with_menu`, `with_edit_mode`, `EditMode::{Emacs, Vi}`, `ExternalPrinter` (experimental), `ReedlineEvent`, `Keybindings` | plain REPL only; владеет терминалом, не совместим с ratatui fullscreen |
| `tui-input` | 0.15.5 | MIT | n/a | `Input::{new, with_value, with_cursor, value, cursor, visual_cursor, visual_scroll, reset, handle}`, `InputRequest::{Submit, Cancel}`, `backend::crossterm::EventHandler` | single-line field; ratatui ^0.30 |
| `tui-textarea` | 0.7.0 | MIT | 1.56.1 | `TextArea::default/new`, `input()`, `lines()`, `Widget for &TextArea` | multiline; ratatui ^0.29 → конфликт с 0.30 |
| `edtui` | 0.11.7 | MIT | n/a | `EditorState`, `EditorView`, `EditorEventHandler`, `EditorTheme`, `Lines`, `EditorMode`, `KeyEventHandler`; `.single_line(true)`, `.wrap`, `.line_numbers`, `.theme`; Vim/Emacs | ratatui ^0.30; форма/редактор; clipboard/system-editor features требуют проверки на Termux |

### 3.1 ratatui: точные свойства, важные для порта

Pinned docs 0.30.2:

- `ratatui::init()` — convenience: raw mode + alternate screen + panic hook; `ratatui::restore()` — восстановление. Есть `init_with_options(TerminalOptions)`.
- `Terminal<B: Backend>`: `draw(|frame| ...)`, `insert_before(height, height, |buf| ...)`, `clear()`, `size()`, `viewport_area()`.
- `TerminalOptions::default().viewport(Viewport::Inline(h))` — inline viewport фиксированной высоты; `Viewport::{Fullscreen, Inline(u16), Fixed(Rect)}`.
- `Viewport::Inline` документирует ограничение: inserted lines идут в scrollback, но это не произвольный append/reflow; `insert_before` требует closure и используется для печати истории. Для полноценного OMP-подобного scrollback нужен собственный frame writer/backend.
- `Widget::render(self, area, buf)`, `StatefulWidget::render(self, area, buf, state)`; `Frame::render_widget`, `render_stateful_widget`.
- `Buffer` хранит `Cell { symbol, fg, bg, underline_color, modifiers, skip }`; ANSI escape sequences не являются контентом. Стилизация — `Style`/`Span`/`Line`, не raw ANSI.
- Ratatui docs прямо: «Ratatui does not include any input handling»; input — crossterm (или другой backend).
- Default features включают `crossterm`; `scrolling-regions` — отдельная feature, важна для `insert_before` без очистки (не включена по умолчанию).

### 3.2 crossterm: Termux-релевантные факты

Pinned 0.29.0 `src/terminal/sys/unix.rs` и `file_descriptor.rs`:

- `window_size()`: открывает `/dev/tty`, иначе fallback на stdout; `ioctl(TIOCGWINSZ)` / `rustix::termios::tcgetwinsize`.
- `size()`: `window_size()`, при ошибке — `tput cols/lines`; `TERM`/`COLUMNS`/`LINES` учитываются в tput-path.
- `tty_fd()`: stdin, если `isatty(stdin)`, иначе открывает `/dev/tty` (read+write); ошибка открытия пробрасывается.
- raw mode — termios (`get_terminal_attr`/`raw_terminal_attr`/`set_terminal_attr`), alternate screen/clear — ANSI sequences.
- `event` — `poll`/`read`; `EventStream` (feature `event-stream`); bracketed paste `Event::Paste`; `EnableBracketedPaste`/`DisableBracketedPaste`; keyboard enhancement flags.
- feature `use-dev-tty` заставляет читать `/dev/tty` вместо stdin.
- `libc` vs `rustix`: default без `libc`; Android — unix-ветка, но сборка `aarch64-linux-android` не проверялась.

### 3.3 reedline: границы

- `read_line(&mut prompt)` блокирует поток и рисует prompt в текущей строке; `ExternalPrinter` помечен experimental и не даёт полноценный full-duplex.
- Reedline сам включает/выключает raw mode и bracketed paste; вложить его в ratatui-цикл нельзя без остановки одного из них.
- `FileBackedHistory` использует файл; на Android в зависимостях reedline 0.52.1 есть `fd-lock` (target `cfg(target_os = "android")`) — вероятно, для file locking; runtime не проверен.
- `system_clipboard` feature тянет платформенный clipboard; на Termux/Android не проверять как обязательный путь.
- Подходит для `omp` plain REPL без TUI; не подходит для prompt внутри настроек/форм/worker viewer.

### 3.4 Формы и editors

- `tui-input` — single-line, cursor/visual scroll, `InputRequest`; ratatui 0.30-совместим. Хорош для пути/числа/однострочного секрета (с маскированием).
- `edtui` — ratatui 0.30, `EditorView::single_line(true)` для форм, Emacs/Vim; умеет mouse, bracketed paste, clipboard (clipboard на Termux — риск), syntax highlighting (лишний вес).
- `tui-textarea` — не брать вместе с ratatui 0.30: `ratatui ^0.29`; либо весь стек на 0.29, либо duplicate types.
- Готового schema-driven form widget нет ни в ratatui, ни в перечисленных crate’ах. Форма = `Block` + `List`/`Table` + input + state machine (валидация, cancel, timeout, multi-select).
- Для destructive/secret операций UI обязан иметь подтверждение и не логировать значение; это требование D69, не дизайн-выбор.

## 4. Termux / native ограничения

- **[Ф]** Android — отдельный target: `aarch64-linux-android`, Bionic; `cfg(target_os = "linux")` не покрывает Android. В форке это уже учитывается в native-крейтах (`android/docs/port-architecture.md`, `crates/pi-natives`).
- **[Ф]** Нет glibc/systemd/Docker; запуск — Termux app process. TUI не может рассчитывать на systemd notify/socket activation.
- **[Ф]** `crossterm` на unix использует `/dev/tty` fallback и `TIOCGWINSZ`; Termux обычно даёт tty, но перенаправленный stdin (pipe/CI) требует `use-dev-tty` или явного headless-режима.
- **[Ф]** `tput` fallback зависит от ncurses/terminfo; в Termux `tput` может отсутствовать. Основной путь — ioctl, но fallback нельзя считать гарантией.
- **[Ф]** В docs/natives отмечено отсутствие нативного clipboard-обёртки/OSC52-пути для Termux; копирование — через terminal bracketed paste/selection, не через `arboard`. **[?]** Поддержка OSC 52 самим Termux не проверена.
- **[?]** Alternate screen, mouse, kitty keyboard protocol, synchronized output (DEC 2026), DECCARA, inline images, sixel, OSC 66 — возможности Termux не проверялись. Любой capability probe должен иметь fallback.
- **[Ф]** Offline-сборка: новые crates отсутствуют в `Cargo.lock`; для сборки нужен cargo fetch/registry cache. Добавление зависимостей требует явного approval (политика репозитория); `deny.toml` разрешает MIT.
- **[О]** `ratatui 0.30.2` требует rust 1.88, `reedline 0.52.1` — 1.95. Репозиторий пинится на nightly-2026-08-08; совместимость с 1.95 нужно проверить `rustc --version` перед выбором.
- **[?]** Android/Bionic: `termios`, `ioctl(TIOCGWINSZ)`, `poll`, signal handling — ожидаемо есть, но сборка и runtime не проверялись. `fd-lock` на Android в reedline — только факт зависимости.
- **[О]** Нет SIGWINCH-зависимости в ratatui: resize приходит как `Event::Resize` от crossterm; в Termux при сворачивании/разворачивании приложения события могут теряться — нужен redraw по следующему событию/таймеру.

## 5. Границы frontend ↔ runtime

1. **Владение состоянием.** Frontend не создаёт turn, не выполняет tools, не пишет session storage. Он отправляет команды и отображает события. Это следует из D37/D56.
2. **Один writer.** TUI, JSON и RPC не должны одновременно писать в один stdout/сессию. D56: один active controlling client; второй — отклоняется runtime, не UI.
3. **stdout/stderr.** stdout — только данные (финальный текст или NDJSON/RPC frames); stderr — прогресс, warnings, diagnostics, stack traces. Смешение ломает pipe-контракт.
4. **NDJSON.** Один JSON object на строку; flush после записи; учитывать backpressure/EPIPE; не обрезать финальный `agent_end`; не вставлять ANSI в JSON.
5. **Секреты.** Settings UI не читает secret values; `config get` маскирует; формы не логируют введённое; ошибки не содержат токенов. D69.
6. **Untrusted content.** tool output/agent text/transcript — данные, не команды UI. Перед вставкой в TUI: sanitize control chars/ANSI, ограничить длину, не интерпретировать escape sequences; worker IDs/description — single-line display.
7. **Отмена/таймауты.** Формы и настройки должны уметь cancel и timeout; headless-режим не должен ждать UI-подтверждения. Print mode уже так делает (plan review отключён).
8. **Восстановление терминала.** raw mode/alternate screen/bracketed paste/mouse/cursor hidden должны сниматься на normal exit, panic, SIGINT/SIGTERM/SIGHUP. Ratatui `init/restore` даёт hook, но собственные режимы (paste/mouse/kitty) нужно регистрировать отдельно.
9. **Settings apply.** Preview ≠ commit; запись атомарная; ошибка apply не должна оставлять UI в «сохранено». Секреты — write-only.
10. **Worker viewer.** Источник данных — snapshots/events/transcript API runtime; viewer не сканирует session files сам и не убивает процессы в обход supervisor.
11. **CLI parity.** Rust `OutputMode` уже фиксирует text/json/rpc; ACP — подкоманда. `--mode acp`/`rpc-ui` из docs не поддержаны enum’ом — либо docs, либо CLI нужно привести к одному контракту (не решать здесь).
12. **Не входит:** provider requests/router, storage format, session compaction, tool implementations, auth flows, multi-client attach. Это соседние блоки.

## 6. Сценарии для последующей проверки (не выполнялись)

- TUI: запуск в Termux, resize, Ctrl+C/Ctrl+D, SIGTERM, сворачивание приложения, восстановление после panic; отсутствие мусора в scrollback.
- TUI: узкий терминал (40/80/120 колонок), emoji/CJK/combining, длинные tool output, вставка bracketed paste с newline/escape.
- Settings: boolean/enum/int/string, submenu, secret (не отображается), cancel, apply error, atomic save, повторное открытие.
- Forms: single/multi choice, custom text, note, timeout, cancel, очень длинные options, ввод с IME/emoji.
- Worker viewer: 0/1/100+ агентов, parent-child, running/completed/failed/aborted, advisor read-only, transcript paging по `next_byte`, reset при truncate, detached session.
- CLI JSON: pipe stdout в parser, stderr отдельно, большой tool result, `agent_end` multi-MB, broken pipe, SIGINT во время turn, exit codes 0/1/3/130/143/129.
- RPC: frame > `maxFrameBytes`, `rpc_chunk` reassembly, protocol v1/v2 negotiate, один клиент, второй writer отклонён.
- Headless: stdin не TTY → print mode; TUI не стартует; `--mode json` не содержит ANSI; `--print` без TUI.
- Termux: `cargo build --target aarch64-linux-android`; отсутствие `tput`; `TERM=xterm-256color`/`screen`/`tmux`; OSC/kitty probes с fallback.

## 7. Непроверенное / открытые вопросы

- **[?]** Сборка `ratatui 0.30.2` + `crossterm 0.29.0` + `reedline 0.52.1` под `aarch64-linux-android` в этом workspace; наличие registry cache/offline.
- **[?]** Совместимость `reedline 0.52.1` (rust 1.95) с nightly-2026-08-08.
- **[?]** Поведение `ratatui` inline viewport на Termux: scrollback, resize, `insert_before`, `scrolling-regions`.
- **[?]** Termux capabilities: alternate screen, mouse, bracketed paste, kitty keyboard, synchronized output, DECCARA, images/OSC 66, OSC 52 clipboard.
- **[?]** `crossterm` `use-dev-tty` и `/dev/tty` в фоновом/разделённом процессе Termux; поведение при закрытии app.
- **[?]** `edtui`/`tui-input` на Android: clipboard feature, IME, wide chars, performance.
- **[?]** Полный набор pinned `AgentHubOverlayComponent` deps/actions (remote/collab, revive/kill permissions).
- **[?]** Транзитивные лицензии и размер dependency tree (chrono, nu-ansi-term, syntect, arboard и т.д.) не аудированы.
- **[?]** Производительность: ratatui full-buffer diff на 100+ агентов/длинных transcript vs OMP ANSI frame renderer; память и частота redraw.
- **[?]** Точное соответствие локальных docs (18.0.9) pinned 18.16.0; локальные docs использовать только как подсказки.
- **[?]** Совместимость `--mode` enum с docs (`acp`, `rpc-ui`): расхождение зафиксировано, решение не принято.

## 8. Источники

### Original OMP (pin `035e4f95c314f9aff2f5a779fa335422680f8e36`)

- `packages/tui/src/tui.ts` — `Component`, `TUI`, `TerminalFrameProvider`, `TerminalFramePlan`, `HistoryBatch`, `OverlayHandle`, `CURSOR_MARKER`.
- `packages/tui/src/components/settings-list.ts` — `SettingItem`, `SettingEditor`, `SettingsListTheme`, `SettingsListOptions`, `SettingsList`.
- `packages/tui/src/components/form.ts` — `Form`, `FormField`, `TextFormField`, `SelectFormField`, `FormFieldOptions`, `FormOptions`, `EmptyInputBehavior`.
- `packages/tui/src/overlays/settings-defs.ts` — `SettingTab`, `SETTING_TABS`, `TAB_METADATA`, `TAB_LEADS`, `TAB_GROUPS`, `SettingsDisplayEntry`, `SettingsHost`, setting variants.
- `packages/tui/src/overlays/settings-selector.ts` — `SettingsSelectorComponent`.
- `packages/tui/src/overlays/ask-dialog.ts` — `AskDialogComponent`, `AskDialogOptions`, `ExtensionAskDialogQuestion`.
- `packages/tui/src/overlays/agent-hub.ts` — `AgentHubOverlayComponent`, `AgentRecordLike`, `AgentMetrics`, rank/tree/detail.
- `packages/tui/src/overlays/agents-hub.ts` — `AgentsHubComponent`.
- `packages/tui/src/overlays/agent-transcript-viewer.ts` — transcript viewer.
- `packages/coding-agent/src/config/settings.ts`, `config/settings-ui.ts` — схема/UI-метаданные.
- `packages/coding-agent/src/modes/print-mode.ts` — `runPrintMode`, `printableEvent`, stdout/stderr, exit codes.
- `packages/coding-agent/src/modes/rpc/rpc-types.ts` — `ready`, `rpc_chunk`, `negotiate_protocol`, `prompt`.
- License: MIT (repo `LICENSE`).

URL-префикс: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/`

### Rust fork (pin `3008cfd0ca88bb0e28323561a165f39efa7159e1`)

- `crates/omp-cli/src/output.rs` — `OutputMode`, `Reporter`, NDJSON event names.
- `crates/omp-cli/src/repl.rs` — plain REPL.
- `crates/omp-cli/src/rpc/` (`mod.rs`, `mode.rs`, `session.rs`, `subagents.rs`, `types.rs`) — `RpcMode`, `RpcSession`, `ready_frame`, `SubagentRegistry`, `SubagentSnapshot`, `TranscriptSlice`, `read_transcript`.
- `crates/pi-agent/src/supervisor.rs` — `ChildStatus`, `ChildRecord`, `SubagentLifecycle`, `SubagentProgress`, `SubagentEvent`, `subscribe`.
- `crates/omp-cli/settings-schema.snapshot.json` — settings metadata.
- `Cargo.lock` — TUI crates отсутствуют.
- License: MIT.

URL-префикс: `https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/`

### Donors (crates.io / docs.rs)

- ratatui 0.30.2 — https://crates.io/crates/ratatui/0.30.2 ; https://docs.rs/ratatui/0.30.2/ratatui/ ; source commit `e665c36cb14752a61cd777fbd06dbef8474f2add`; MIT.
- ratatui-core 0.1.2 — https://crates.io/crates/ratatui-core/0.1.2 ; MIT.
- ratatui-widgets 0.3.2 — https://crates.io/crates/ratatui-widgets/0.3.2 ; MIT.
- ratatui-crossterm 0.1.2 — https://crates.io/crates/ratatui-crossterm/0.1.2 ; MIT.
- crossterm 0.29.0 — https://crates.io/crates/crossterm/0.29.0 ; https://docs.rs/crossterm/0.29.0/crossterm/ ; MIT.
- reedline 0.52.1 — https://crates.io/crates/reedline/0.52.1 ; https://docs.rs/reedline/0.52.1/reedline/ ; MIT.
- tui-input 0.15.5 — https://crates.io/crates/tui-input/0.15.5 ; https://docs.rs/tui-input/0.15.5/tui_input/ ; MIT.
- tui-textarea 0.7.0 — https://crates.io/crates/tui-textarea/0.7.0 ; MIT.
- edtui 0.11.7 — https://crates.io/crates/edtui/0.11.7 ; https://docs.rs/edtui/0.11.7/edtui/ ; MIT.

### Локальные docs (вторично, версия 18.0.9)

- `docs/tui-core-renderer.md`, `docs/tui-runtime-internals.md`, `docs/tui.md`, `docs/keybindings.md`, `docs/settings.md`, `docs/agent-hub.md`, `docs/rpc.md`, `docs/cli-reference.md`, `docs/agent-architecture-decisions.md`, `docs/agent-implementation-research-plan.md`, `docs/planning-research.md`, `android/docs/port-architecture.md`.
