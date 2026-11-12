# Code intelligence и работа с кодом без LSP

Статус: read-only исследование source, donors и границ. Не architecture decision, не план реализации, не сборка. Код не писался, пакеты не устанавливались, build/test не запускались.
Дата: 2026-11-06.

Пины:
- original OMP: `can1357/oh-my-pi@035e4f95c314f9aff2f5a779fa335422680f8e36`;
- fork/termux: `SklUpp/omp-rust-termux@3008cfd0ca88bb0e28323561a165f39efa7159e1`;
- локальный снимок: `/data/data/com.termux/files/home/omp-work` (workspace `18.0.9`).

Границы: syntax/search/symbols/index/repo map + чтение, навигация, structural edits/codemods/rename/refactor, version guard/preview/format. LSP не планируется по D13. Память, providers, UI/TUI, MCP, debugger, model gateway не входят.

Обозначения:
- **Факт** — прочитано в source/manifest/лицензии.
- **Оценка** — вывод из API/поведения, не испытание.
- **Не проверено** — нет сборки, запуска или device-теста.

## 0. Краткий вывод

1. **Syntax-слой широкий.** Fork `pi-ast` парсит 56 языков 56 нативными tree-sitter грамматиками; original — 17 linked + WASM fallback. Structural search/rewrite, block/node-chain и structural summary уже есть.
2. **Полноценного persistent cross-file symbol/reference index нет.** Но summary/block/ast-grep дают syntax-level symbol-like данные (декларации, узлы, диапазоны, имена), достаточные для outline и эвристической навигации. Это не то же самое, что разрешённые definition/reference.
3. **Пользовательский контракт не должен быть syntax-only.** Возможны три явно маркируемых уровня: `syntax`, `heuristic`, `semantic`. Уровень выбирается по языку и доступному toolchain, а не одним глобальным флагом.
4. **Definitions/references/call/import graph реальны в трёх формах:**
   - tree-sitter tags — per-file defs/refs для языков с `TAGS_QUERY`;
   - Oxc — scope-resolved symbols/references для JS/TS в пределах программы;
   - rust-analyzer/SCIP — семантический Rust-индекс или внешний SCIP-индекс.
   Для остальных языков — AST/name-heuristic, без обещания compiler parity.
5. **AST edits/codemods уже есть**, но rename/refactor с semantic correctness ограничен: structural substitution не знает shadowing, imports, macros, dynamic references. Semantic rename есть у rust-analyzer; для JS/TS — Oxc scope-level, не type-aware; для остальных — только эвристика.
6. **Version guard — сильная часть доноров.** Original `pi-edit`: content tags, snapshot store, stale recovery, seen-lines, staged apply. Fork: hashline `SnapshotStore` + exact-byte stale gate в `ast_edit`. Multi-file writes не транзакционны: частичная запись возможна и явно репортится.
7. **Preview/format/parse guard:** preview встроен; format — host/writer или отдельный LSP/CLI formatter; в fork Rust edit paths автоматический format-on-write не найден. Parse regression фиксируется в original `pi-edit`; fork hashline имеет `syntax::parses_cleanly`.
8. **Termux/Android не доказаны.** 56 C-грамматик + PCRE2 требуют C toolchain/NDK; Oxc/ra_ap требуют новых Rust editions; tree-sitter-graph 0.12 зависит от tree-sitter 0.24 при текущем 0.25.10. Всё это — проверки, не выводы.

## 1. Требования

| # | Требование | Источник |
|---|---|---|
| R1 | LSP не планируется; встроенные анализаторы/готовые компоненты | D13, `docs/agent-architecture-decisions.md:69`; D35, `:171` |
| R2 | Уровни: syntax, index/symbols, language semantics; глубина отложена | `docs/agent-architecture.md:150-151` |
| R3 | Максимум популярных языков; глубокая семантика не для всех | D35; D13 |
| R4 | Repo map: структура + символы/сигнатуры, бюджет, on-demand | D75, `docs/agent-architecture-decisions.md:341`; `docs/agent-architecture.md:150` |
| R5 | Freshness после изменений, фильтрация путей, настраиваемость | D75; D66 |
| R6 | Termux/Bionic — отдельный артефакт | D71 |
| R7 | Fallback не выдавать за точную семантику; stale index не считать актуальным | `docs/rust-agent-component-verdicts-2026-11-05.md:28-31` |
| R8 | Контракт работы с кодом: чтение, навигация, правки; не ограничивать syntax-only и не ограничивать несколькими языками | Новое требование пользователя, 2026-11-06 |
| R9 | Правки: version guard, preview, format integration, явные semantic limits | Новое требование пользователя, 2026-11-06 |
| R10 | Capability (`syntax`/`heuristic`/`semantic`) маркируется на результате/языке | Следствие R7/R8 |

## 2. Original OMP: доноры

### 2.1 `pi-ast` original pin

Manifest: [`crates/pi-ast/Cargo.toml`](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-ast/Cargo.toml), license `MIT` (workspace); [`src/lib.rs`](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-ast/src/lib.rs).

Модули: `block`, `language`, `ops`, `parse_cache`, `search`, `summary`.

- **17 linked grammars**: bash, c, cpp, css, diff, go, html, java, javascript, json, markdown, python, regex, rust, toml-ng, typescript, yaml.
- **`wasm` feature default**: missing grammars грузятся как WebAssembly; тянет wasmtime + cranelift + `cmake`. С `default-features=false` missing grammar → `GrammarError::Invalid`.
- **`search`**: async structural search/rewrite over `pi_vfs::Fs` + original `pi_walker`; `futures::Stream`, `tokio_util::CancellationToken`, `exec::ordered`.

Original `search` API (exact symbols):

- `Discover { include: Option<Selector>, exclude: Option<Selector>, hidden, gitignore, node_modules, follow_links, workers, any_file }`
- `Candidate { path, display }`
- `Query<'a> { patterns, lang, selector, strictness: MatchStrictness, captures }`
- `Match { span: Span, text, captures: Option<Captures> }`; `Capture`, `Captures`, `Span`, `Pos`
- `Finder: Stream<Item = Result<FileMatches, Error>>`; `FileMatches { path, display, language, source, matches }`
- `find(fs, candidates, query) -> Finder`
- `find_page(fs, candidates, query, Page) -> Result<FindPage, Error>`; `Page`, `Hit`, `FindPage`
- `find_source(source, language, query, page, cancel) -> Result<SourceMatches, Error>`
- `rewrite(fs, candidates, &Rewrite) -> Result<Edited, Error>`; `Mode { Preview, Render, Write }`, `Rewrite`, `Change`, `FileEdit`, `Edited`
- `rewrite_source(...) -> Result<SourceEdit, Error>`
- `discover(fs, root, &Discover) -> Result<Vec<Candidate>, Error>`

Факты поведения:

- discovery через original `pi_walker::{glob::Selector, walk, exec, Action, WalkError}`;
- explicit `lang` переопределяет расширение; иначе язык по файлу;
- паттерны компилируются per-language;
- ошибки языка/файла/паттерна — предупреждения, не падение поиска;
- missing wasm grammars → `Report::missing_grammars`;
- `Mode::Write` сначала рендерит/валидирует весь проход, затем пишет; **не транзакция**: ошибка записи может оставить уже записанные файлы;
- cancellation/drop останавливает `Finder`/`rewrite`; порядок — `exec::ordered`.

### 2.2 `pi-walker` original pin

[`crates/pi-walker/Cargo.toml`](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-walker/Cargo.toml), MIT.

- зависимости `pi-vfs`, `tokio`, `futures`, `flume`, `grep-*`, `regex`, `parking_lot`;
- API, видимый из `search`: `pi_walker::glob::Selector`, `walk::{collect, Options, Action, WalkError}`, `exec::ordered`;
- async traversal over `pi_vfs::Fs`, не только local `std::fs`;
- scan cache из `docs/fs-scan-cache-architecture.md` относится к **fork** `pi-walker`.

### 2.3 `pi-edit` original pin

Manifest: [`crates/pi-edit/Cargo.toml`](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-edit/Cargo.toml), MIT (workspace). Зависимости: `pi-ast`, `pi-diff`, `pi-walker`, `async-trait`, `parking_lot`, `regex`, `serde_json`, `thiserror`, `xutf`, `xxhash-rust`.

Модули: `diff_string`, `engine`, `error`, `files`, `fuzzy`, `modes`, `notebook`, `path_policy`, `session`, `store`, `stream_json`, `text`.

Exact public API ([`src/lib.rs`](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-edit/src/lib.rs)):

- `EditMode { Replace, Patch, ApplyPatch, Hashline, Sloppy }`, `EditMode::{as_str, parse}`;
- `FileOp { Create, Update, Delete, Noop }`;
- `Resolved { absolute, display }`, `HeaderKind { Path, HashlineTag }`;
- `StagedFile { display, absolute, op, move_to, existed, before_raw, before, after, persisted, diff, preview_diff, first_changed_line, header, before_preview, after_preview, warnings, text_override, record_snapshot, clipboard_after }`;
- `PreviewFile`, `Inspection { paths, entries, file_ops }`, `FileOpIntent { Delete, Move }`;
- `trait ModeEngine { mode; preview(args, streaming, files, store) -> Vec<PreviewFile>; stage(...) -> Result<Vec<StagedFile>, EditError>; inspect(...) -> Inspection }`;
- `EditStore`; `Session`; `SessionConfig { mode, policy, allow_fuzzy, fuzzy_threshold, enforce_seen_lines, max_columns, raw_input }`;
- `PreviewBatch { generation, streaming, files }`;
- `WriteRequest { absolute, display, op, move_to, content, flush_lsp, lsp_batch_id }`;
- `WriteResponse { written, diagnostics_json }`;
- `trait EditWriter { async fn write(WriteRequest) -> EditResult<WriteResponse> }`;
- `ApplyRequest { lsp_batch_id, lsp_flush }`;
- `FileOutcome { absolute, display, op, move_to, diff, first_changed_line, old_text, new_text, snapshots_pruned, diagnostics_json, warnings, text, parse_regressed }`;
- `ApplyOutcome { text, files }`;
- `Session::{new, engine, push, set_args_json, take_unresolved, provide, take_recovery_misses, provide_recovery, apply_recovery_misses, begin_apply_url_targets, preview, apply}`;
- `source_parses(code, path) -> bool`; `write_drift_warning(path) -> String`.

Факты поведения:

- `ModeEngine` — pure in-memory transformation; writes только через host `EditWriter`; Rust сам диск не пишет.
- `Session::preview` строит streaming diff по мере прихода аргументов; `PreviewBatch.generation`; финальный pass.
- `Session::apply`: `begin_apply`, re-read всех целей через `FileCache`, `stage` всё в памяти (любая ошибка — до первой записи), plan-mode guard, затем запись в payload order. Writer failure останавливает цикл; уже записанные файлы остаются; ошибка возвращается.
- `WriteResponse.written` — текст, реально записанный; bridge может переформатировать. При drift `write_drift_warning`; `FileOutcome.parse_regressed` = before парсился, after — нет.
- `EditStore` (`store.rs`): `Snapshot { path, text, hash, seen_lines, ... }`, `Clipboard`, `file_hash` (xxh32-based), `payload_hash` (xxh64), `seen_lines_from_body`, `EditStore::{new, with_limits, fork, record, record_file, record_seen_lines, head, head_hash, versions, by_hash, has_hash, by_content, paths_with_hash, invalidate, relocate, clear, start_clipboard_batch, commit_clipboard, record_noop, reset_noop}`.
- `source_parses` использует `pi_ast::summary::parse_code_status`; unknown language never parses, поэтому не даёт ложный regression.
- `notebook` поддержка: JSON notebooks нормализуются/сериализуются; diff по cell text.

### 2.4 Original workspace tree

[`workspace-tree.ts`](https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/workspace-tree.ts):

- `buildWorkspaceTree(cwd)`: `maxDepth: 3`, `perDirLimit: 12`, `lineCap: 120`;
- файлы/каталоги + size/mtime + `[unreadable: ...]`;
- `agentsMdFiles`; **символов/сигнатур нет**;
- `buildDirectoryTree` — listing read tool, без gitignore, глубина 1.

D75 symbols/signatures исходным workspace-tree не реализованы.

## 3. Fork `3008cfd`: что есть

### 3.1 `pi-ast` fork

Локальный `crates/pi-ast/Cargo.toml`, версия `18.0.9`, MIT, LICENSE есть.

| Свойство | Original | Fork |
|---|---|---|
| `search` | async VFS | удалён |
| wasm | default + wasmtime/cranelift/cmake | нет |
| grammars | 17 | 56 native |
| VFS/tokio | есть | нет |
| scan в ops | original walker | `ignore::WalkBuilder` |
| API | `Finder`/rewrite | sync `ops` + N-API wrappers |

Модули: `block`, `language`, `ops`, `parse_cache`, `summary`.

Публичные символы:

- `SupportLang::{all_langs, canonical_name, from_alias, from_path, sorted_aliases}`; 56 вариантов, отдельные `Tsx`/`TypeScript`.
- `ops::{AstMatchStrictness, AstMatch, MatchedFile, CompiledRewrite, resolve_strictness, supported_lang_list, resolve_supported_lang, resolve_language, is_supported_file, compile_pattern, compile_search_patterns, compile_rewrite_rules, parse_source, tree_has_parse_error, collect_matches, collect_matches_in, rewrite_source, apply_edits, collect_matched_files, has_glob_syntax}`.
- `parse_cache::{MAX_ENTRY_SOURCE_BYTES=4 MiB, MAX_TOTAL_SOURCE_BYTES=4 MiB, MAX_ENTRIES=12, parse_cached, ParseCacheStats, parse_cache_stats, clear_parse_cache}`; ключ xxh64 + длина + язык, hit проверяется байтами.
- `summary::{SummaryOptions, SummarySegment, SummaryResult, summarize_code}`; defaults body 4/comment 6; read tool — 100-line floor, 50/100 unfold, 2 MiB/20k lines (`docs/tools/read.md:106-110,386-393`).
- `block::{BlockRangeOptions, BlockRange, LineRange, EnclosingBoundaryOptions, NodeSpan, block_range_at, enclosing_block_boundaries, node_chain_at}`; ERROR-узлы → `None`.

**Важно:** summary/block **дают** syntax-level structural/symbol-like данные: сохраняют декларации/сигнатуры, возвращают диапазоны узлов, node chain, block boundaries. Чего нет — dedicated типа `Symbol`/`Definition`/`Reference`, ссылочного разрешения и persistent cross-file index. Нельзя говорить «нет никакого symbol extraction»: ast-grep по `kind`/declaration patterns и summary уже извлекают declarations.

### 3.2 `pi-walker` fork

`crates/pi-walker/Cargo.toml`, MIT. Зависимости: `dashmap`, `globset`, `ignore`, `parking_lot`, `rayon`, `libc`, `windows-sys`.

Ключевые API:

- `FileType`, `WalkDetail::{Minimal, Full}`, `WalkOrder`, `DirectoryErrorMode`, `FollowLinks`, `CachePolicy`, `EmptyRecheck`, `SizeHintPolicy`, `VisitOrder`;
- `CompiledWalkGlob`, `WalkFilter { files_only/dirs_only, max_file_size, node_modules_unless_mentioned, glob }`;
- `WalkPredicate`, `EntryMeta`, `FileCandidate`, `WalkBackend`, `WalkRank`, `WalkStats`, `WalkOutcome`, `WalkOptions`;
- `WalkRequest::{new, from_options, root, filter, collect, collect_with_heartbeat, collect_ranked, collect_files, collect_dirs, collect_file_candidates, stream, for_each_entry, for_each_file, for_each_file_candidate_parallel}`;
- `CollectedEntry`, `WalkError`, `collect_entries`, `walk_entries`, `execute_candidates`, `parallel_for_each`.

Scan cache: 1 s TTL, empty recheck 200 ms, max 16 entries, process-local `DashMap`; `invalidate_path`, `invalidate_all`, helpers after write/delete/rename. `pi_ast::ops::collect_matched_files` использует другой scan (`ignore::WalkBuilder`, hidden false), поэтому нельзя смешивать выдачи без явного выбора.

### 3.3 `pi-grep-pattern` fork

`crates/pi-grep-pattern/Cargo.toml`, `18.0.9`, MIT manifest (LICENSE-файла в локальном listing нет — проверить).

- `CompiledMatcher { Rust(RegexMatcher), Pcre(PcreMatcher) }`, `CompiledMatcherError`, `build_matcher`, `compile_strict`, `pattern_matches`.
- Порядок: sanitize braces → Rust regex → PCRE2 (lookaround/backrefs) → retry escaped parentheses → literal fallback.
- Lexical only; не AST/symbols. `OMP_PCRE2_JIT`; macOS default off.

### 3.4 Fork edit stack

Отдельного `crates/pi-edit` в fork нет. Реализация:

- `crates/pi-agent/src/tools/hashline/` — native hashline:
  - `parser::{Edit, Cursor, BlockMode, PasteAt, Range, FileOp, PatchSection, ParsedPatch, split_patch_sections, parse_section_body, collect_anchor_lines}`;
  - `apply::{apply_edits, apply_edits_checked, ApplyResult}`;
  - `block::{BlockSpan, resolve_block_edits}`;
  - `budget::Budget` (batch edits/bytes caps);
  - `clipboard::{Clipboard, start_batch, commit_batch, validate_sequence, resolve}`;
  - `hash::{normalize_hash_text, compute_file_hash, detect_line_ending, normalize_to_lf, restore_line_endings, strip_bom}`; tag = `xxh32(normalized,0) & 0xffff`, 4 uppercase hex;
  - `snapshot::{Snapshot, SnapshotStore, canonical_key, format_header}`; `record`, `record_seen_lines`, `head`, `by_hash`, `by_content`, `invalidate`, `relocate`, `clear`; max snapshot 4 MiB;
  - `recovery::try_recover`; `mismatch::mismatch_message`; `syntax::parses_cleanly`;
  - `edit::{EditToolOptions { enforce_seen_lines }, NoopGuard, execute}`; default `enforce_seen_lines=false` (source comment says upstream schema default false; original docs say true — см. divergence).
- `crates/pi-agent/src/tools/edit_variants/` — `replace`, `patch`, `apply_patch`, `sloppy`, `mode`:
  - `EditMode { Replace, Patch, Hashline, ApplyPatch, Sloppy }`, `EDIT_MODES`, `DEFAULT_EDIT_MODE=Hashline`;
  - `resolve_edit_mode(settings, active_model, env_variant, strict)`; fallback to `Sloppy` for known-bad models unless `PI_STRICT_EDIT_MODE`;
  - `contract(mode)`, `wire_name(mode)`, `execute(mode, args, ctx)`;
  - `apply_patch::{parse_apply_patch, parse_apply_patch_streaming}`; `patch` engine.
- `crates/pi-agent/src/tools/ast_edit.rs` — preview/apply structural rewrites:
  - `AstEditTool`, `AstEditPending`, `PendingAction`;
  - `parse_ops`, `parse_paths`, `compute_edits`, `render_preview`;
  - preview always `dryRun: true`, `failOnParseError: false`;
  - exact original bytes per affected file stored; apply via `write xd://resolve` re-reads and compares bytes; any change => stale, **zero files written**; partial write/cancel => error + requeue, retry fails byte gate;
  - parse-error files skipped for rewriting; no partial edits;
  - 1:1 substitution; `astEdit` cap from `PI_MAX_AST_FILES` (default 1000).
- `crates/pi-agent/src/tools/ast_grep.rs` — search; hashline sink `RecordSnapshot`; `AstGrepTool::{new, with_snapshot_sink, with_display_inputs}`; output `*LINE:TEXT` / `*LINE|TEXT`.
- `crates/pi-agent/src/tools/pending.rs` — staged action queue; failed apply requeues.

### 3.5 `pi-natives` fork

`crates/pi-natives/src/ast.rs`:

- `AstGrepOptions`, `AstFindOptions`, `AstFindMatch`, `AstFindResult`, `AstMatchOptions`, `AstMatchResult`, `AstReplaceOptions`, `AstReplaceChange`, `AstReplaceFileChange`, `AstReplaceResult`;
- N-API `astGrep`, `astMatch`, `astEdit`;
- `DEFAULT_FIND_LIMIT=50`; hidden true, gitignore true, `.git` skipped, node_modules unless glob mentions, `FollowLinks::Never`, `WalkDetail::Minimal`, path order, cache true;
- `astEdit` dryRun default true; parse-error files skipped unless `failOnParseError`;
- result includes `applied`, `limitReached`, `parseErrors`.

`block.rs`: `blockRangeAt`, `nodeChainAt`, `enclosingBlockBoundaries`.
`summary.rs`: `summarizeCode`.
`lib.rs` N-API addon; для Rust-ядра не нужен, тянет `napi`, `napi-derive`, tokio, прочие bindings.

### 3.6 Что fork потерял

- async `find`/`find_page`/`rewrite` + ordered cancellation;
- `pi_vfs::Fs` (non-local FS, schemes);
- wasm fallback / `Report::missing_grammars`;
- standalone `pi-edit` `EditStore`/`Session`/`ModeEngine`/host writer abstraction (заменён fork-модулями);
- original whole-pass `Mode::Write` semantics;
- `parse_regressed`/`write_drift_warning` в fork hashline paths (есть `syntax::parses_cleanly`, но не тот же outcome type).

## 4. Языки: capabilities, не один флаг

### 4.1 Parse

Fork `SupportLang` — 56 языков, все native. Точные pinned версии: Rust 0.24.2, Python 0.25.0, Go 0.25.0, Java 0.23.5, C 0.24.2, C++ 0.23.4, C# 0.23.5, JS 0.25.0, TS 0.23.2, Ruby 0.23.1, PHP 0.24.2, Kotlin 0.4.1, Scala 0.26.2, Swift 0.7.3, Dart 0.2.0, Lua 0.5.0, Bash 0.25.1, SQL 0.3.11, Markdown 0.5.3, YAML 0.7.2, TOML 0.7.0. Core `tree-sitter 0.25.10`; `tree-sitter-language 0.1.7`.

### 4.2 Symbol/reference support

`tree-sitter-tags` uses `TAGS_QUERY`/`LOCALS_QUERY`; `Tag` has `is_definition`, `docs`, `name_range`, `line_range`, `utf16_column_range`. Spot-check:

| Язык | tags | local scopes | defs/refs | call/import graph |
|---|---|---|---|---|
| Rust | да | `LOCALS_QUERY`? нужно проверить | per-file | heuristic; rust-analyzer for semantic |
| Python | да | — | per-file | heuristic |
| Go | да | — | per-file | heuristic |
| Java | да | — | per-file | heuristic |
| C/C++ | да | — | per-file | heuristic |
| C# | да | — | per-file | heuristic |
| JS/JSX | да | `LOCALS_QUERY` | per-file; Oxc scope-resolved | Oxc/parser + heuristic |
| TS/TSX | да | `LOCALS_QUERY` | per-file; Oxc scope-resolved | Oxc/parser + heuristic |
| Ruby | да | — | per-file | heuristic |
| PHP | да | — | per-file | heuristic |
| Swift | да | — | per-file | heuristic |
| Dart | да | — | per-file | heuristic |
| Lua | да | — | per-file | heuristic |
| Kotlin | **нет** | — | heuristic/ast-grep | heuristic |
| Scala | **нет** | `LOCALS_QUERY` | heuristic/ast-grep | heuristic |
| Bash | **нет** | — | heuristic | heuristic |
| SQL | **нет** | — | heuristic | heuristic |
| Vue/Svelte/Astro | не проверено | — | grammar AST; no TS injection | heuristic |
| HTML/CSS/JSON/YAML/TOML/Markdown | не проверено | — | structural only | n/a |

Остальные 38 языков требуют audit `TAGS_QUERY`; parser crate не доказывает tags query.

### 4.3 Semantic donors

| Язык | Донор | Что реально даёт | Граница |
|---|---|---|---|
| JS/TS/JSX/TSX | Oxc `oxc_parser 0.157.0`, `oxc_semantic 0.157.0` | AST, scopes, symbols, references, CFG, `symbol_references`, `symbol_declaration`, `reference_span`; `SemanticBuilder::new().build(&program)` | Не type checker; module resolution/tsconfig paths не входят; TS/JS only |
| Rust | `ra_ap_ide 0.0.358` | `Analysis::{file_structure, symbol_search, goto_definition, goto_declaration, goto_implementation, find_all_refs, hover, call_hierarchy, incoming_calls, outgoing_calls, rename, prepare_rename, structural_search_replace}` | Нужен workspace load/cargo metadata/sysroot; тяжёлый; Termux/Android не проверены |
| Любой с SCIP indexer | `scip 0.10.0` | `Index { metadata, documents, external_symbols }`; occurrences/symbols | Нужен внешний indexer; freshness по revision/build/deps; stale index нельзя выдавать за live |
| Несколько языков | `stack-graphs 0.14.1` (archived) | name binding graph из tree-sitter | archived, правила не все языки, сложность/поддержка; не проверено |
| Несколько языков | `tree-sitter-graph 0.12.0` | graph construction DSL из tree-sitter | нужны TSG-правила; зависит от tree-sitter 0.24, конфликт с 0.25.10 |

## 5. Чтение: structural/symbol-aware

### 5.1 Что уже есть

- `read` tool: structural summary через `pi_ast::summary::summarize_code`; caps 2 MiB/20k lines, `minTotalLines=100`, body/comment 4/6, unfold 50/100; output `…` + footer re-read ranges; `read_summary_result` details.
- `block`/`node_chain_at`/`enclosing_block_boundaries`: точный syntax block вокруг позиции, не догадка.
- `ast_grep`: structural search по 56 языкам, AST patterns, captures; parse-error files still searched.
- `ast_edit`: preview structural rewrites.
- `summary` — structural elision, не symbol index; сохраняет declarations/сигнатуры, но не создаёт `Symbol` objects.

### 5.2 Symbol-aware outline

Optimal slice: `tree-sitter-tags` per file:

- `TagsContext::new()`, `TagsContext::parser()`;
- `TagsContext::generate_tags(config, source, cancel) -> Result<(impl Iterator<Item=Result<Tag,Error>> + 'a, bool), Error>`;
- `TagsConfiguration::new(language, tags_query, locals_query)`;
- `Tag { range, name_range, line_range, span, utf16_column_range, docs, is_definition, syntax_type_id }`.
- `docs` обычно preceding comment; ranges byte/line/UTF-16.

Это даёт file outline с kind/name/docs/ranges; не даёт project-wide references. Для языков без `TAGS_QUERY` — ast-grep declaration patterns или summary; это `heuristic`, не `semantic`.

### 5.3 Repo map

Aider `repomap.py` (Apache-2.0) — конкретный алгоритм:

- `get_tags(fname)` cache by mtime (diskcache SQLite, fallback dict);
- `get_ranked_tags`: `defines: name -> set(file)`, `references: name -> list(file)`;
- personalization: base `100/len(fnames)`; chat +personalize; mentioned file `max(current, personalize)`; path-component match +personalize once;
- graph `networkx.MultiDiGraph`; per ident `definers x referencers`; `mul`:
  mentioned ident ×10; snake/kebab/camel len≥8 ×10; `ident.startswith("_")` ×0.1; `len(defines)>5` ×0.1; referencer in chat ×50; edge weight `mul * sqrt(num_refs)`;
- self-edge 0.1 for def without refs;
- `nx.pagerank(weight="weight", personalization=..., dangling=...)`;
- token-budget bisection over `to_tree`; chat files skipped from map.

Границы: name heuristic, not semantic; mtime freshness; tie-breaks/token budget must be stabilized; Python code cannot be linked into Rust — port algorithm.

### 5.4 Freshness

- file map: `WalkDetail::Full` mtime/size; scan cache 1 s TTL; content hash better.
- outline/tags: per-file content hash + language + parser version; invalidate on write/rename/delete; `SnapshotStore`/scan cache helpers.
- graph: any changed file can invalidate incoming edges from other files; byte hash of one file does not validate whole graph. Recompute or mark stale.
- semantic index: revision/build/dependencies, not just file hash; SCIP stale-check.

## 6. Навигация: definitions/references/call/import

### 6.1 Уровни

| Уровень | Что можно | Что нельзя |
|---|---|---|
| `syntax` | AST node at position, block range, node chain, declaration outline, textual rename, structural search | resolved definition, reference set, type, import binding |
| `heuristic` | name-based defs/refs, call edges by callee text, import graph by path strings, repo-map ranking | shadowing, overloads, macros, dynamic dispatch, package resolution |
| `semantic` | compiler/IDE index: resolved defs/refs/calls, rename, type info | still limited by build context, generated code, dynamic features |

### 6.2 Definitions/references

- **tree-sitter-tags**: per-file definitions/refs with `is_definition`; no cross-file resolution; language coverage gaps.
- **Oxc**: `SemanticBuilder` builds `Semantic { AstNodes, Scoping, CFG }`; `Scoping::{symbol_ids, symbol_name, symbol_span, get_resolved_reference_ids, get_resolved_references, find_binding, get_binding, root_unresolved_references}`; `Semantic::{symbol_references, symbol_declaration, reference_name, reference_span, is_unresolved_reference}`. Scope-resolved inside program; imports/modules not fully resolved; no type checker.
- **rust-analyzer**: `Analysis::goto_definition/find_all_refs`; semantic, requires workspace; best correctness for Rust.
- **SCIP**: `Index.documents[].occurrences[]` with symbol/role; external indexer; freshness.
- **Others**: tags + name/visibility heuristics; mark `heuristic`.

### 6.3 Call graph

Built-in call graph нет. Feasible slices:

- AST call nodes: ast-grep pattern per language (e.g. call expression + callee field), capture callee text; `[Оценка]` requires per-grammar patterns, not tested here.
- tags `reference` captures where grammar provides call-specific captures; availability varies; audit needed.
- Oxc: scoping/reference graph gives identifier references, not call-specific edges; CFG exists but call graph requires AST traversal.
- rust-analyzer: `call_hierarchy`, `incoming_calls`, `outgoing_calls`; semantic, toolchain-bound.
- Heuristic call graph: caller = enclosing function node; callee = callee text/name; unresolved dynamic/method calls; no overload resolution; must label `heuristic`.

### 6.4 Import graph

Built-in import graph нет. Feasible:

- parse import/use/include/require nodes and path strings; resolve relative paths lexically; workspace roots known; package/external imports unresolved.
- JS/TS: Oxc module records/AST; tsconfig paths/baseUrl not automatically; Oxc not a resolver.
- Rust: `use` path is not a file path; module resolution needs rust-analyzer.
- C/C++: includes need include paths/preprocessor; not safe without compiler.
- Go: import path -> module/package; needs go.mod/module cache.
- Python: absolute/relative imports + sys.path; ambiguous.
- Mark graph edges as `syntax` or `heuristic`, never `semantic` unless compiler/indexer.

### 6.5 Language capability matrix

| Language | parse | tags defs/refs | local semantic | call graph | import graph |
|---|---|---|---|---|---|
| TS/TSX/JS/JSX | yes | yes | Oxc scope-level (no type check) | Oxc AST/CFG + heuristic; no compiler graph | AST + path heuristic; tsconfig not resolved |
| Rust | yes | yes | rust-analyzer/SCIP | rust-analyzer semantic; heuristic fallback | rust-analyzer; heuristic `mod`/`use` |
| Python | yes | yes | none embedded | heuristic | heuristic (sys.path ambiguity) |
| Go | yes | yes | none embedded | heuristic | heuristic (go.mod/module cache needed) |
| Java | yes | yes | none embedded | heuristic | heuristic (classpath needed) |
| C | yes | yes | none | heuristic | heuristic (include paths needed) |
| C++ | yes | yes | none | heuristic | heuristic (includes/templates) |
| C# | yes | yes | none | heuristic | heuristic (project/classpath) |
| Ruby | yes | yes | none | heuristic | heuristic (load path) |
| PHP | yes | yes | none | heuristic | heuristic (autoload) |
| Kotlin | yes | **no** | none | heuristic/ast-grep | heuristic |
| Scala | yes | **no** | none | heuristic/ast-grep | heuristic |
| Swift | yes | yes | none | heuristic | heuristic |
| Dart | yes | yes | none | heuristic | heuristic |
| Lua | yes | yes | none | heuristic | heuristic |
| Bash | yes | **no** | none | heuristic | heuristic |
| SQL | yes | **no** | none | n/a | n/a |
| Vue/Svelte/Astro | yes | unverified | none | grammar AST only | grammar AST only |

### 6.6 Semantic correctness limits

- Definitions/references by name collide on shadowing/overloads/methods/dynamic dispatch.
- Macro expansion (Rust), decorators/metaprogramming (Python), include/preprocessor (C/C++), conditional compilation, generated code, reflection/dynamic imports break static graphs.
- Workspace boundaries, classpath, sysroot, module cache, tsconfig paths affect resolution.
- A hash of one file cannot prove references from other files/dependencies are fresh.
- Semantic results must be invalidated on build/dependency changes, not only edits.

## 7. Правки: AST edits/codemods/rename/refactor

### 7.1 Structural edits/codemods

- `ast_edit` (`ops: [{pat, out}]`) supports `$NAME` and `$$$NAME` captures in replacement templates. Anonymous `$_` and `$$$` match without reusable bindings; name a capture to substitute it. No multi-sibling expansion unless grammar permits.
- Preview first; actual write via `write xd://resolve`; exact-byte stale gate; error files skipped; max files cap; parse issues reported; files not modified on stale.
- `ast_grep` structural search; `pi_ast::ops::{compile_rewrite_rules, rewrite_source, apply_edits}` is the lower-level engine.
- Codemod can rename identifiers textually, change calls, rewrite patterns; cannot guarantee semantic equivalence.
- For language-specific rewrite, one pattern per language; mixed-language trees compile per discovered language.

### 7.2 Rename/refactor limits

| Operation | Feasible today | Correctness limit |
|---|---|---|
| Textual rename via ast-grep | yes | matches identifier text/token; shadowing/member vs local/aliases/strings/comments not handled; may miss refs |
| File-local rename via tags/Oxc | partial | tags gives name ranges; Oxc scopes resolve local symbols; cross-file imports not complete |
| Cross-file rename Rust | `ra_ap_ide::Analysis::rename/prepare_rename` | Requires workspace load/cargo/sysroot; macros/generated code still bounded; Termux unverified |
| Cross-file rename JS/TS | no full semantic rename | Oxc scope-level only; no type checker/module resolver; imports/reexports/aliases need module graph |
| Refactor structural pattern | yes (`ast_edit`) | 1:1 substitution; no scope/type validation; new parse errors possible |
| Move/rename file | hashline `MV`; original `Move to` | bytes/permissions; references in code not updated |
| Semantic codemod by compiler | not embedded | rust-analyzer SSR (`structural_search_replace`) only Rust; external |

### 7.3 Version guard / preview

**Original `pi-edit`:**

- content tag = `file_hash` (xxh32-based, 4 hex) per normalized text;
- `EditStore` retains snapshots (limits), seen lines, clipboard, no-op guard;
- `Session::preview` streaming, `PreviewBatch.generation`;
- `Session::apply` re-reads every target, stages all, any failure before first write; then writes in order; writer failure leaves earlier writes; `FileOutcome` reports per-file;
- stale recovery in hashline engine: only unique safe result; otherwise mismatch/context; seen-line enforcement;
- `WriteResponse.written` may differ due to host/LSP formatting; `write_drift_warning`; `parse_regressed`.

**Fork hashline/ast_edit:**

- `SnapshotStore` with `record`, `record_seen_lines`, `by_hash`, `by_content`, `invalidate`, `relocate`;
- `compute_file_hash` 4-hex; `try_recover`; `mismatch_message`;
- hashline re-checks canonical identity `(dev, ino)` and exact validated source bytes before commit; preserves BOM/line ending/mode/symlink; multi-file partial possible;
- `ast_edit`: stores exact original bytes + prepared bytes; apply re-reads all, byte compare, zero writes on stale; partial write/cancel is error + requeue; retry fails byte gate; `details.writtenFiles`, `partial`, `stale`;
- no filesystem lock; byte gates mitigate but do not make multi-file atomic.

### 7.4 Format integration

- Original `pi-edit`: host `EditWriter` writes; `WriteResponse.written` is actual persisted text; bridge may reformat; drift warning; `FileOutcome.parse_regressed`; no rollback on parse regression.
- Fork: `write_atomic`/`write_atomic_with_mode` preserve mode/symlink; **automatic formatter call in hashline/ast_edit paths not found**. Available separate modules:
  - `pi_agent::lsp::diagnostics::{format_content, format_file}`;
  - `format_content` tries CLI custom linter (Biome/SwiftLint) then LSP formatter;
  - `lsp` tool has `format` action;
  - LSP integration is optional/external; not a semantic guarantee.
- Therefore format-on-write must be an explicit integration point, and post-format content must be re-tagged/re-read; otherwise next edit may see drift.
- Formatting can introduce parse regression; original reports it; fork should combine `syntax::parses_cleanly`/`parse_regressed` with formatter result.

### 7.5 Atomicity / move / multi-file

- Original `ModeEngine::stage` all-or-nothing; writes are sequential, not transactional; partial write possible.
- Fork hashline same: all sections validated before writes, then per-file commits; partial possible; canonical identity re-check.
- `ast_edit`: all-files byte gate before first write; per-file writes; partial possible; explicit `partial`/`writtenFiles`.
- Move/rename: original `MV`/`Move to`; fork `MV`; bytes and permissions; destination conflicts; no code-reference updates.
- No file lock; concurrent external edits detected by content tag/byte gate, not prevented.

### 7.6 Donor slices (transfer allowed, no product decision)

- `pi-edit` `EditStore`/`Snapshot`/`ModeEngine`/`Session`/`EditWriter` — strongest version guard/preview contract; host writer must be implemented for target.
- `pi-edit` `hashline` mode + `text` normalization + `notebook` — line-anchored edits; no native deps beyond pi-ast.
- `pi-ast::ops` — structural rewrite engine.
- `ast_edit` exact-byte stale gate — simplest transferable guard for AST rewrites.
- `Oxc` `SemanticBuilder`/`Scoping` — JS/TS local symbol/reference graph; add only if JS/TS depth is required.
- `ra_ap_ide` — Rust semantic defs/refs/rename/calls; add only if Rust semantic depth is required and toolchain size accepted.
- `SCIP` — optional external index; add only with explicit stale/fallback semantics.

## 8. License / native / Termux

### 8.1 Licenses

| Component | License | Fact |
|---|---|---|
| `pi-ast`, `pi-walker`, `pi-edit`, `pi-grep-pattern` | MIT | manifests; `pi-grep-pattern` LICENSE file absent locally |
| `tree-sitter` | MIT | upstream LICENSE |
| `ast-grep-core` | MIT | ast-grep LICENSE |
| `grep-pcre2` | Unlicense OR MIT | upstream Cargo.toml |
| PCRE2 | BSD-3-Clause WITH PCRE2-exception | PCRE2 LICENCE.md |
| `tree-sitter-tags` | MIT | crates.io 0.27.1 |
| `ignore`, `globset` | MIT/Unlicense | ripgrep crates; check version NOTICE |
| Aider | Apache-2.0 | LICENSE.txt |
| Oxc | MIT | crates 0.157.0 |
| `ra_ap_ide` | MIT OR Apache-2.0 | crates.io 0.0.358 |
| `scip` | Apache-2.0 | crates.io 0.10.0 |
| `scip-rust` | Apache-2.0 | repo LICENSE |
| `tree-sitter-graph` | MIT OR Apache-2.0 | crates.io 0.12.0 |
| `stack-graphs` | MIT OR Apache-2.0 | crates.io 0.14.1; archived |
| `petgraph` | MIT/Apache-2.0 | LICENSE-MIT/LICENSE-APACHE |
| `tantivy` | MIT | LICENSE |
| universal-ctags | GPL-2.0 | COPYING |

**GPL obligations, not a categorical legal conclusion:** universal-ctags is GPL-2.0. If it is embedded/linked and the combined work is distributed, GPL-2.0 obligations (source availability, copyleft, notice) can attach; whether a separate-process invocation creates a derivative/combined work is a licensing question that depends on the chosen product license, distribution model, and legal review. It is not automatically “forbidden”; it is a strong-copyleft boundary to resolve, not a per-file technical choice.

Grammar licenses: spot-checked `tree-sitter-sequel` MIT, `tree-sitter-objc` MIT, `tree-sitter-kotlin-sg` MIT, `tree-sitter-astro-next` MIT OR Apache-2.0, `tree-sitter-svelte-next` MIT OR Apache-2.0, `tree-sitter-vue-next` MIT. All 56 need generated NOTICE/audit; `pi-ast` MIT does not cover transitive grammar licenses.

### 8.2 Native build

- `pi-ast` fork: 56 grammar crates + `tree-sitter` core; `cc` builds `parser.c`/`scanner.c`; some C++ scanner possible (not audited).
- `ast-grep-core` pure Rust.
- `pi-grep-pattern`: `pcre2 0.2.11` bundled C + `grep-pcre2 0.1.10`; C compiler; PCRE2 JIT optional; Android/Termux unverified.
- `pi-walker`: pure Rust + `libc`/`windows-sys`; native directory fast path; Android matches `cfg(unix)`, not `cfg(target_os="linux")`; inspect per branch.
- `pi-edit`: no extra native beyond `pi-ast`; pure Rust engine + host writer.
- Oxc: pure Rust, edition 2024, rust-version 1.97.0 for 0.157.0; heavy dependency graph/size; no C.
- `ra_ap_ide 0.0.358`: edition 2024, rust-version 1.98; workspace loading/cargo/sysroot; heavy; not a drop-in.
- `tree-sitter-graph 0.12.0`: pure Rust but depends `tree-sitter 0.24`; current workspace core 0.25.10 — potential duplicate/version bridge; not tested.
- `stack-graphs 0.14.1`: pure Rust; archived; rule/language coverage limited.
- `scip 0.10.0`: pure Rust protobuf; index generation is external.
- `pi-natives`: N-API addon not needed for Rust core; brings napi/tokio/other bindings.

### 8.3 Termux/Android

- Termux aarch64: clang from `$PREFIX` may build tree-sitter/PCRE2; **not checked**.
- Android app: NDK clang/sysroot/Bionic; libc++ if C++ scanner; **not checked**.
- Original `pi-ast` wasm default adds wasmtime/cranelift/cmake; fork removes it (portability win, fewer languages without native grammar).
- `tree-sitter` ABI: core 0.25.10 vs grammar versions; runtime `set_language` compatibility must be tested.
- Oxc/ra_ap require newer Rust than some Termux toolchains; check `rustc --version`, target support, memory/CPU.
- Size/RSS/CPU for 56 grammars, Oxc, ra_ap not measured.
- Cancellation: fork walker heartbeat; pi-natives `CancelToken`; original search drop. Tree-sitter parse itself has no preemption; cancellation around parse.
- No Android/Termux build, test, or benchmark evidence.

## 9. Пробелы, подтверждённые source

1. **Нет dedicated persistent cross-file symbol/reference index.** `pi-ast`/`pi-walker` не экспортируют полноценный `Index` с разрешёнными ссылками. Summary/block/ast-grep дают syntax-level symbol-like extraction.
2. **Нет repo map.** Original workspace-tree — file tree; D75 symbols/signatures не реализованы.
3. **Нет cross-file resolution.** tags per-file; Aider name heuristic; Oxc program-local; rust-analyzer only Rust.
4. **Нет встроенного semantic layer** для большинства языков. Oxc/ra_ap/SCIP — отдельные donors, не единый анализатор.
5. **Нет полного tags coverage.** Kotlin/Scala/Bash/SQL без `TAGS_QUERY`; 38 языков не аудированы.
6. **Две scan-policy.** `pi_ast::ops` (`ignore`) vs `pi-natives` (`pi-walker` cache).
7. **Original search несовместим с fork walker** (`Selector`/`exec`/`pi-vfs` vs fork API); прямой copy не соберётся.
8. **Wasm fallback потерян в fork**; язык вне 56 native не парсится.
9. **Инъекции только Html**; Vue/Svelte/Astro не получают TS/JS injection.
10. **Fork edit stack не содержит standalone `pi-edit`**; `Session`/`EditStore`/`ModeEngine` нужно переносить или адаптировать.
11. **Fork hashline default `enforce_seen_lines=false`** (source comment), тогда как original docs описывают `edit.enforceSeenLines=true` default. Это divergence, не parity.
12. **Automatic format-on-write не найден** в fork hashline/ast_edit; format modules есть отдельно.
13. **Multi-file edits не транзакционны** ни в original Session, ни в fork hashline/ast_edit; partial writes явно возможны.
14. **Нет Android/Termux evidence**; Oxc/ra_ap/tree-sitter-graph version/ABI risks.

## 10. Что проверить до принятия любого варианта

- Собрать `pi-ast` fork под Termux и `aarch64-linux-android`; C/C++ scanner audit по 56 грамматикам.
- Проверить `tree-sitter 0.25.10` ABI со всеми грамматиками; `set_language` failures.
- Аудит `TAGS_QUERY`/`LOCALS_QUERY` по 56 языкам; матрица parse/tags/docs/refs.
- Проверить tags quality на реальном corpus: duplicate defs, false refs, docs boundaries, UTF-16 columns, CRLF/BOM.
- Для definitions/references/call/import: сравнить syntax/heuristic/semantic на фиксированном corpus; зафиксировать false positives/negatives.
- Для Oxc: проверить API 0.157.0 на JS/TS/JSX/TSX, scopes/refs, unresolved imports, memory/CPU; подтвердить отсутствие type-checker parity.
- Для rust-analyzer: проверить `ra_ap_ide` 0.0.358 загрузку workspace на Termux/Android, `goto_definition`, `find_all_refs`, `rename`, `call_hierarchy`; размер/время.
- Для SCIP: проверить indexer, freshness (revision/build/deps), stale fallback; не выдавать stale за live.
- Для `tree-sitter-graph`: проверить tree-sitter 0.24 vs 0.25.10, API `File::from_str`/graph, правила; иначе duplicate runtime.
- Для edits: прогнать hashline/ast_edit на concurrent edits, renames, symlinks, CRLF/BOM, large files, partial write, stale retry; проверить `writtenFiles`/`partial`/`stale`.
- Для rename/refactor: corpus с shadowing, overloads, imports/reexports, macros, generated code; измерить, где syntax rename меняет смысл.
- Для format: проверить Biome/SwiftLint/LSP formatter on Termux; format-after-edit + re-tag; parse regression.
- Лицензии: NOTICE для 56 грамматик; `pi-grep-pattern` LICENSE; GPL universal-ctags obligations под выбранную лицензию продукта (юридическая проверка, не технический вывод).

## 11. Источники и точные пины

### Local docs/source

- `docs/agent-architecture-decisions.md` — D13, D35, D66, D71, D75.
- `docs/agent-architecture.md` — code intelligence, workspace, D75.
- `docs/omp-original-foundation-research-2026-11-05.md:19-20,28` — `pi-ast`/`pi-edit`/`pi-walker` donors.
- `docs/rust-agent-component-verdicts-2026-11-05.md:28-31` — tree-sitter/SCIP/Oxc boundaries.
- `docs/rust-port-reuse-research-2026-11-05.md:65-69` — Aider repo map.
- `docs/natives-text-search-pipeline.md`; `docs/fs-scan-cache-architecture.md`.
- `docs/tools/ast-grep.md`; `docs/tools/ast-edit.md`; `docs/tools/read.md`.
- `crates/pi-ast/src/{lib,ops,parse_cache,summary,block,language/mod,language/parsers}.rs`.
- `crates/pi-walker/src/{lib,cache}.rs`.
- `crates/pi-grep-pattern/src/lib.rs`.
- `crates/pi-agent/src/tools/{ast_edit,ast_grep}.rs`; `crates/pi-agent/src/tools/hashline/{edit,parser,snapshot,recovery,hash,apply,block,syntax}.rs`; `crates/pi-agent/src/tools/edit_variants/{mod,mode,replace,patch,apply_patch,sloppy}.rs`.
- `crates/pi-natives/src/{ast,block,summary}.rs`.

### Original pin

- https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-ast/Cargo.toml
- https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-ast/src/search/mod.rs
- https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-walker/Cargo.toml
- https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-edit/Cargo.toml
- https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-edit/src/{lib,engine,session,store}.rs
- https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/tools/edit.md
- https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/workspace-tree.ts

### Fork pin

- https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-ast/Cargo.toml
- https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-walker/Cargo.toml
- https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-grep-pattern/Cargo.toml

### External pins

- tree-sitter core: repo `tree-sitter/tree-sitter@62816e1c0d0a4ca9553174f3cbe5b9631047f7f4`; tags API source `crates/tags/src/tags.rs`; crate `tree-sitter-tags 0.27.1` (MIT).
- Aider: `Aider-AI/aider@5dc9490bb35f9729ef2c95d00a19ccd30c26339c`; `aider/repomap.py` (Apache-2.0).
- Oxc: `0.157.0`; release commit `f799f174b5db10f1fa9e136594e53de82d4f4f83`; repo main `af1a41b8379229735a50eb60beb2470ca6d6ac67`; crates `oxc_parser 0.157.0`, `oxc_semantic 0.157.0` (MIT); docs.rs API for `SemanticBuilder`, `Semantic`, `Scoping`.
- rust-analyzer: `ra_ap_ide 0.0.358` (MIT OR Apache-2.0), release commit `2ad81e72b7e978cb800dffe1bcce5c9f98032acf`; docs.rs `Analysis` API.
- SCIP: `scip 0.10.0` (Apache-2.0), `types::Index { metadata, documents, external_symbols }`; `scip-rust` repo Apache-2.0.
- tree-sitter-graph: `0.12.0` (MIT OR Apache-2.0), `tree-sitter/tree-sitter-graph@b930fb59c2177a90b3a6a68e1feeca6918ceb58b`.
- stack-graphs: `0.14.1` (MIT OR Apache-2.0), repo archived.
- petgraph: `petgraph/petgraph@6b4ee17598daf3237ffe570d526b86f992ef4748` (MIT/Apache-2.0).
- tantivy: `quickwit-oss/tantivy@d66f88d8f88df5b5ab62129f4a367eebdc70dd32` (MIT).
- PCRE2: BSD-3-Clause WITH PCRE2-exception.
- universal-ctags: GPL-2.0; strong-copyleft obligations depend on product license/distribution; legal review required.
