# Непокрытый и частичный TS/JS-код оригинального OMP (`~/omp-work`) vs `rust-agent` (2026-11-27)

> **Источники:** Step 1 (`ScoutOmpTsMap`) + Step 2 (`ScoutOmpGapD1Providers`, `ScoutOmpGapD2Frontend`, `ScoutOmpGapD3WebConvert`, `ScoutOmpGapD4MemoryCompact`, `ScoutOmpGapD5VoiceCollab`, `ScoutOmpGapD6AutoGovernance`).
> **База сверки:** `~/omp-work/packages/` (17 пакетов, ~2125 `src` TS/TSX-файлов, ~760k est. LOC) + донор `~/omp-work/crates/pi-natives/src/` vs `~/rust-agent/crates/` (`HEAD 0d2608b`). `~/omp-termux/` содержит только Android/Bionic патчи и сборочный клей (`packages/` и `crates/` там отсутствуют).

---

## 1. Карта пакетов `~/omp-work/packages` и донорский pure-Rust резерв

| Пакет TS | Статус в `rust-agent` | Основание / примечание |
|---|---|---|
| `agent/src` (27) | **COVERED** (`compaction` PARTIAL) | `pi-agent/{loop,state,compaction,tokenizer,utok}` |
| `hashline` (22) | **COVERED** | `pi-agent/src/tools/hashline/` + `edit_variants/` |
| `ai/src` (289) | **PARTIAL** | есть `provider.rs`, `provider_anthropic.rs`, `provider_ollama.rs`, `provider/http.rs`; нет `dialect/`, `usage/`, `error/flags`, `auth-broker/gateway`, `oneshot-retry` |
| `catalog/src` (56) | **PARTIAL** | `aragent/src/catalog.rs` читает `2` compat-флага из ~50+; нет `identity/`, `discovery/` |
| `utils/src` (119) | **PARTIAL** | отсутствуют `dom/`, `turndown/`, `marked/`, `docx/`, `vterm/`, `ar/` кодеки (`read_archive.rs` пока на `python3`) |
| `mnemopi` (65) | **PARTIAL** | `memory_store.rs` — плоская таблица + FTS5 + vector/MMR; нет `aaak`/`sleep` деградации и графа |
| `tui/src` (45) | **PARTIAL** | `aragent/src/tui.rs` (alt-screen, 12 клавиш, без width-engine/editor/markdown/pickers) |
| `browser-relay` (5), `wire` (1) | **UNCOVERED** | `Drop` (`D02` Termux / `D56` collab deferred) |
| `collab-web` (69), `snapcompact` (2) | **DEFERRED** | `D56` (`decisions:313`) и `decisions:502` |
| `stats`, `metaharness`, `omptype`, `natives`, `typescript-edit-benchmark` | **OUT_OF_SCOPE / NATIVE** | внешний бенчмарк, web UI, TS-типы, N-API |

**Скрытый pure-Rust резерв в доноре `~/omp-work/crates/pi-natives/src/` (MIT):** `html.rs` (`html_to_markdown_rs`), `pdf.rs` (`pdf_inspector`), `text.rs` (ANSI/grapheme width-движок поверх уже подключённого `xutf`), `keys.rs` (kitty CSI-u / `modifyOtherKeys`), `diff.rs` (word-diff), `highlight.rs` (`syntect`). N-API-клей срезается без переписывания алгоритмов с нуля.

---

## 2. Непокрытые и частичные модули `coding-agent/src` (сводка D1–D6)

### D1 — Провайдеры, модели и файлы
- **P0 (`Port`):** `ai/src/error/{flags,rate-limit,auth-classify,finalize}.ts` (21 семантический флаг ошибок взамен 14 строковых шаблонов в `rpc/mode.rs:1743-1790`), `catalog/src/compat/openai.ts` (полный набор OpenAI-compat флагов для 9Router/локальных бэкендов), `ai/src/utils/openai-http.ts` (6 попыток, concurrency passthrough, body caps).
- **P1 (`Port`/`Adapt`):** `transform-messages.ts` (дедуп tool-result, 64-char id, demotion), `oneshot-retry.ts`, `empty-completion-retry.ts`, `thinking-loop.ts`, `catalog/src/identity/{classify,family,markers,dialect}` + `hosts.ts`, `utils/schema/` (`moonshot-mfjs`/`grammar`), онлайн-заголовки из `tiny/` (TINY-роль, локальный ONNX `Drop`).
- **P2 / `Defer`:** `discovery/openai-compatible.ts` (`/v1/models`), `auto-thinking/classifier.ts` (онлайн), `openai-reasoning-fallback.ts`, `dialect/` (11 in-band text-tool парсеров), `usage/` (18 OAuth-парсеров), `auth-broker/` + `auth-gateway/` (`D02`/`D27`), `blob-broker/` (`D03`/отложенный `snapcompact`).

### D2 — TUI, режимы, slash-команды и промпты
- **Риск корректности (`Port` P0):** в `aragent/src/prompts.rs:43-95` и `runtime.rs:6720-6749` отсутствует базовый `system-prompt.md` (`coding-agent/src/prompts/system-prompt.md`, 251 L).
- **Двойной режим TUI (согласовано):** единое ядро (`pi-natives/{text,keys}`, multiline editor с undo/kill-ring/history, слэш-реестр ~22 команд с готовыми бэкендами, markdown/OSC 8/palette) + два выбираемых интерфейса: **Lite/Native** (`inline-viewport` на нативном скроллбеке, по умолчанию) и **Full TUI** (`alt-screen`, слоистые панели, word-diff `diff.rs`, HUD, полный набор `D19`/`D25`).
- **`Defer` / `Drop`:** kitty graphics/`ImageBudget`/Sixel/DECCARA/tmux-passthru/100 тем/splash.

### D3 — Веб, браузер, конвертеры и архивы
- **Crate-first `Port`:** `tools/fetch.ts` (HTTP+content-type+`llms.txt`/RSS/JSON; согласовать капы с `read.rs` и `artifact://`), `web/scrapers/types.ts` + топ ~15 скраперов через `html5ever`/`scraper` + `html_to_markdown_rs`, `markit/` PDF через `pdf_inspector` (DOCX/PPTX/XLSX/EPUB адаптировать поверх `zip`+XML), перевод `read_archive.rs` с `python3` на MIT/Apache Rust-крейты (`zip`/`tar`/`flate2`/`bzip2`/`xz2`/`ruzstd`/`sevenz-rust`, без `unrar`).
- **`Adapt`:** `inspect_image.rs` (добавить resize через `image`, без `resvg`).
- **`Drop`:** `tools/browser*`, `browser-relay/`, скриншоты `read-pdf.ts`, `tools/computer*`, `packages/wire`. `image-gen*` — `Defer` (потом OpenAI-compatible по `D30`).

### D4 — Память, контекст и компакция
- **P0 (`Port`, закрывает `D502`):** `agent/src/compaction/` — `tool-protection.ts`, `pruning.ts` (supersede/age pruning), `shake.ts`, `compaction.ts:499,1315` (cut-point + сохранение хвоста по токен-бюджету) + чтение `methodOrder` (`config_cmd.rs:1230`) и настраиваемого порога (`rpc/mode.rs:1716`).
- **P1 (`Port`, закрывает `D515`):** `mnemopi/src/core/aaak.ts` + `sleep` деградация (`consolidate.ts:835,972`), инструменты `recall`/`retain`/`memory_edit` поверх `memory_store.rs`.
- **P2–P3 / `Defer` / `Drop`:** `branch-summarization.ts` (P2), `entities.ts` + `episodic-graph.ts` (P3), `hindsight/`, `memories/` LLM-консолидация, `compress/`, `snapcompact/` (`D502`), `shmr`/`triples`/`e6-*`.

### D5 — Голос, Live и Collab
- **Включено в backlog (`Adapt`):** `coding-agent/src/vibe/` (~1600 L) поверх `Supervisor` + `tools/hub.rs` + `task_persisted_revive.rs`.
- **Малые чистые куски:** `tts/speakable.ts` + `wav.ts`, удалённый HTTP `tools/tts.ts`.
- **`Drop` / `Defer`:** `stt/**` и локальный `tts/**` (`onnxruntime-node`/`sherpa-onnx-node`/`AudioCapture` не работают на Android/Termux, `pi-voice` в `unsupported.rs`), `live/**` (Codex Desktop WebRTC + `DeviceCheck` macOS-only), `collab/**` и `collab-web` (`D56` + внешний relay `wss://my.omp.sh`).

### D6 — Автоматизация, диагностика, MCP и секреты
- **Покрыто (не портировать):** `irc/` (`tools/hub.rs`), `launch/` брокер (`process_broker/`), `async/job-manager.ts` (`supervisor.rs`).
- **P0–P1 (`Port` / `Adapt`):**
  1. `commit/` (conventional + exclusions + changelog + затем agentic) вместо `git add -A` (`main.rs:5463`, конфликт с `D28`).
  2. Полный MCP по стандарту внутри `arag` (`rmcp` + OAuth RFC 9728/`WWW-Authenticate`/PKCE поверх `aragent/src/auth/` + `mcp add/remove/list/disable` CLI + `resources/prompts`/серверные запросы).
  3. `discovery/` MCP-импортёры (`.claude.json`, `.cursor/mcp.json`, `.vscode/mcp.json`, `opencode.json`, `~/.codex/config.toml` с trust-gate; без абстракции `capability/`).
  4. `cleanse/` 28 парсеров + checker runner под именем `arag check`/`repair` (не путать с существующим `arag cleanse`).
  5. `debug/` (`report-bundle`, `system-info`, `--debug-sse` в `provider/http.rs`; V8-профилировщик `Drop`).
  6. `security/importers/{sarif,codex-security}.ts` + `remediation.ts`.
  7. `export/html` (карточки tool-вызовов, палитра, дерево подсессий; `share.ts` `Defer`).
- **Согласованный гибрид секретов (`B1`):** `KnownSecrets` + эвристики при `read` тегируются в RAM эфемерным `session_hmac_key` (`[REDACTED:sec_XXXXXX]`), в SQLite/логи/экспорт пишется только необратимый `[REDACTED]`, обратная подстановка разрешена только в `edit` для тегов того же `canonical_path`.

---

## 3. Сводка конфликтов с кодом/настройками (`D20`/`D28`/`D29`/`D502`)

1. `main.rs:5463` (`git add -A`) нарушает `D28`.
2. `settings-schema.snapshot.json` содержит мёртвые ключи без consumer'а (`browser.*`, `xdev.*`, `secrets.enabled`, `bash.autoBackground.*`, `tui.resizeScrollback`/`maxInlineImage*`, `tools.format`, `auth.broker.*`) и `acp/capabilities` speech catalog с `"ready": false` — привести в соответствие с `D29`.
3. `read.rs:100-101` перечисляет `.doc/.ppt/.xls/.rtf` без конвертера; `registry.rs:8-14` не декларирует исключёнными `browser` и `generate_image`.
4. `D20` («личные расширения загружаются автоматически») расходится с кодом (`args.rs:636`, `acp/capabilities.rs:160`) — уточнено поправкой о переносе плагинов за R12.
5. `compaction` в коде (`loop.rs:1512`, `rpc/mode.rs:1716`, `config_cmd.rs:1230`) отстаёт от `D502`, а `memory_store.rs:1317` — от `D515` (`AAAK`).
