# Унифицированный аудит — Домен 4: Code Analysis (`pi-ast`, RepoMap, LSP, DAP), Memory (`mnemopi`, embeddings), Skills, Agents, Rules & Prompts (`ScoutUnifiedAnalysisMemory`)

Дата: 2026-11-27  
Источник: сырой отчёт `ScoutUnifiedAnalysisMemory` (только чтение документации и исходного кода).

---

# Step 2 result — full cross-check report (steps 1 + 2)

Read-only. No files modified, no cargo commands run. Every claim below is anchored to live code unless marked `[INFERENCE]` or “doc-only”.

---

## A. Done — verified against live code

### A1. pi-ast grammar gating
Verified:
- `default = ["core-grammars", "full-grammars"]`; workspace consumers do not disable defaults (`crates/pi-ast/Cargo.toml:12-80`, `Cargo.toml:256`, `crates/aragent/Cargo.toml:37`).
- 56 declared variants, 13 core grammars (`language/mod.rs:340-365`).
- Compiled-out parser entry panics; lookup APIs filter it out; `from_path_unconditional` is reporting-only (`language/parsers.rs:1-23`, `language/mod.rs:526-538`).
- Capability matrix: `ast_supported`, `ast_full_grammar_only`, `repo_map_tags`, `default_lsp_server`, `supported_ops`; 10 `SemanticOp`s, 6 LSP (`language/mod.rs:368-438,552-580,632-635`).

### A2. ast_grep / ast_edit
Verified:
- `ast_grep` read-only, 50-result cap, 20 parse-error cap, `pi-walker` discovery, deterministic merge/paging (`tools/ast_grep.rs:1-41,80-99`).
- `ast_edit` preview-only staging; exact pre-edit bytes; `PendingStore`/`SnapshotStore`; apply through `write xd://resolve`; all-file byte gate; any stale change ⇒ zero writes; failed apply requeues; partial apply explicit (`tools/ast_edit.rs:1-25,53-54,96-103,1136-1160`).
- No FS lock; per-file recheck narrows but does not eliminate race; no automatic format-on-write (`tools/ast_edit.rs:8-14`; `lsp/diagnostics.rs` formatter is separate).

### A3. RepoMap
Verified implemented, not absent:
- `pi-ast/src/map.rs`: declaration map (`kind`, `name`, line), 200 symbols/file, 20 map-rule languages, 24 tag-query languages (`:24-60,369-390,962-986`).
- `RepoMapBuilder`: 2000 global symbols, 200/file, Aider weights, PageRank damping 0.85/40 iterations/tol 1e-10, deterministic ordering, truncation flags (`:658-917,928-953`).
- `pi-agent/src/tools/repo_map.rs`: `read <dir>:map`, 500 files, 512 KiB/file, 2000 symbols, 1024 tokens, hidden/gitignore/skip-git/follow-never, symlinked root rejected, structured truncation (`:30-37,149-276,285-330`).
- Output explicitly labels syntax-only + heuristic PageRank + bodies omitted (`:285-290`).

### A4. LSP
Verified:
- 16 actions, 10 read-only; mutating actions include `rename`, `rename_file`, `code_actions`, `format`, `reload`, `request` (`lsp/types.rs:462-493`).
- `matrix` static, no server spawn; reports AST ops, repoMap tags, LSP ops only when server `ready`; unknown extension unsupported unless LSP applies (`lsp/tool.rs:1405-1550`).
- Config precedence and `idleTimeoutMs` `Option<Option<u64>>`; default 300000; explicit 0/null disables per-workspace (`lsp/config.rs:48-56,83-142,390-402`, `lsp/defaults.json:2`).
- Idle checker 60 s tick, pending requests skipped, per-workspace timeout, shared mux server never killed (`lsp/client.rs:2191-2240`).
- Trust gating: project LSP files skipped when `allow_project_config=false`; runtime reads `TrustStore::status(cwd).is_trusted()`; cache key includes trust bit (`lsp/config.rs:68-80,305-330`; `runtime.rs:1812-1830`; `lsp/client.rs:2230-2252`).
- LSP spawn now `env_clear()` + platform allowlist + explicit wrapper env, own process group (`lsp/client.rs:920-940`).

### A5. DAP
Verified:
- 14 adapters embedded (`dap/config.rs:20-130`).
- 28 actions; 10 read-only; unknown = exec; timeout 30 s, range 5–300 s (`dap/tool.rs:38-93`).
- Discovery: launch-cwd files, ancestors’ `.omp/.claude/.codex/.gemini`, user/home dirs; merge low→high (`dap/config.rs:339-360,544-566`).
- Commands resolved on PATH, never installed (`dap/config.rs:1-4,568+`).
- Spawn uses `setsid`, `non_interactive_env` then supplied env; **no `env_clear()`** (`dap/client.rs:725-788`).
- **No project trust gate** in DAP config loading (`dap/config.rs:339-360`).

### A6. memory_store v2
Verified:
- One `memory.db` under agent dir; separate from `sessions.db`; WAL required, `synchronous=FULL`, FK on (`memory_store.rs:1-35,600-620`).
- Schema v2: scope/origin CHECK constraints, dedupe key, TTL, confidence, access metadata, `consolidated_into` self-FK (`:72,98-180`).
- FTS5 external content `memories_fts(body, context)`, `unicode61`, triggers ai/ad/au (`:144-167`).
- `memory_embeddings(memory_id, model, dim, BLOB, PK(memory_id,model))`, FK cascade (`:169-180`).
- Schema lifecycle: v0 refusal if tables exist without version, transactional v1→v2 rebuild, v2 repair, newer/older version errors (`:1997-2090`).
- Add dedupe = SHA-256(scope + cwd-or-session + body + context); touch clears `consolidated_into` (`:630-700,2371-2390`).
- Recall: BM25 + prefix tokens, scope predicate before limit, excludes consolidated/expired inferred, asserted rows exempt from TTL, increments access (`:860-1000`).
- Hybrid: lexical 0.5, MMR 0.7, candidate cap 200, vector union, missing vectors stay lexical (`:79-90,960-1030`).
- Consolidation: deterministic, `llm_used=0`; TTL deletes only inferred; near-duplicate cosine ≥0.92; canonical = lowest id in sorted cluster; marks `consolidated_into`; export-dirty marker written before delete (`:1288-1430`; test `:3516-3549`).
- Vector kernels pure Rust: TS-bit-exact cosine, pair scan, top-k, MMR (`memory_vectors.rs:1-95`).
- Legacy `learned.md` migration marker-guarded, read-only source, never re-imported (`memory_store.rs:19-24,1511-1543`).
- Export rewrites only derived `learned.md` bullet block from store rows; prose/headings preserved; store is authority (`memory_store.rs:1590-1625`).

### A7. Embeddings
Verified:
- Async provider contract + best-effort fallback + blocking scoped-thread variant (`memory_embeddings.rs:60-123`).
- HTTP OpenAI-compatible `POST <base>/v1/embeddings`; full `/embeddings` accepted; whole batch per request; optional `dimensions`; 30 s default; URL/body/key redaction (`:130-273`).
- Selection only for backend `Local`: local flag > `memory.embeddingsUrl` > 9router/ninerouter base URL; default model `text-embedding-3-small` (`config.rs:1318-1366`).
- Settings: `memory.embeddingsUrl|Model|ApiKey|Local` (`config.rs:1063-1070`).
- FastEmbed behind `local-embeddings`; workspace `fastembed 7.8` with `onnx`, `ort-load-dynamic`, `hf-hub-rustls-tls`, `fancy-regex`; default `BGESmallENV15`; dynamic ORT load; spawn_blocking inference (`Cargo.toml:220-226`, `aragent/Cargo.toml:30-54`, `memory_embeddings.rs:440-563`).

### A8. Mnemopi
Verified absent as a working backend:
- Enum values and `memory.backend` parse only; legacy `mnemosyne`→`mnemopi` accepted (`autolearn.rs:59-80`, `config.rs:2189-2200`, `config_cmd.rs:1355-1365`).
- `learn` only mounts for `MemoryBackend::Local`; `off/hindsight/mnemopi` get no tool (`autolearn.rs:214-250`).
- `memory_protocol.rs:25-26` explicitly says mnemopi ids/Hindsight pointers not implemented.
- `mnemopi.*` keys exist in `settings-schema.snapshot.json`, not parsed into `Config`.

### A9. autolearn
Verified:
- `learn` requires `autolearn.enabled` + `Local`; `manage_skill` requires only `autolearn.enabled` (`autolearn.rs:9-14,223-250,402-410`).
- Auto-capture gate: not aborted, capture tools present, enabled, autoContinue, `tool_calls >= minToolCalls` (default 5), not plan, not goal (started or active) (`autolearn.rs:109-154`; `config.rs:666-682`).
- `manage_skill` create/update/delete; requires description+body; refuses shadowing authored skill; managed root only (`autolearn.rs:435-504`).

### A10. Skills
Verified:
- Roots/priority: native 100, claude 80, agents 70, codex 70, custom overrides defaults, managed 5 dead-last; no omp-plugins/opencode/github/claude-plugins providers (`skills.rs:10-24,606-672`).
- Frontmatter: `enabled=false` skip; name default dir; description required only for native; `hide` or camelCase `disableModelInvocation`; **no globs/alwaysApply** (`:895-925`).
- Dedup name-first-wins, realpath, custom override, managed never shadows authored (`:460-535`).
- Metadata-only discovery; lazy read with canonical containment; concurrent swap window not prevented (`:1-34,114-140`).
- Per-session snapshot; hidden omitted from prompt listing but callable; refresh after managed mutation (`:320-392`).
- Prompt shape live: `# Skill: <name>\n\n<description-or-body>` (`runtime.rs:6645-6655`).
- `/skill:` leading/mid-prompt rules, unknown unchanged, exact user template, `enableSkillCommands` gate (`skills.rs:994-1118`).
- `skill://` read-only containment; hidden reachable (`skill_protocol.rs:1-63`).
- `autoloadSkills` declaration-order, unknown skipped, read failure errors spawn (`skills.rs:555-580`; `task.rs:3208-3240`).

### A11. Agents
Verified:
- Discovery nearest `.omp/agents`, user `<agent-dir>/agents`, extension roots, then 7 bundled (scout, designer, reviewer, security-reviewer, librarian, task, sonic); first wins (`agents.rs:40-102`).
- Frontmatter: name/description required; tools normalization + mandatory `yield` for non-empty list; spawns inference; model list; thinking; blocking; readSummarize; prewalk/advisor; autoloadSkills; output; body system prompt (`:157-213`).
- Model resolution precedence: overrides > patterns; `:effort` unique-prefix; caller effort > pattern suffix > thinkingLevel; maxEffort cap (`task_catalog.rs:319-433`).

### A12. Rules & prompts
Verified:
- Prompt order: CLI > `.omp/SYSTEM.md` > agent `SYSTEM.md` > share; append additive; context files `PERSONALITY.md`, `RULES.md`, `AGENTS.md` project then agent (`prompts.rs:1-7,46-75`).
- Rule discovery providers/priorities: native 100, extensions 90, agents 70, cursor 60, windsurf 50, cline 40, github 30, builtin-defaults 1; 27 built-ins; duplicate name highest priority first (`local_cmd.rs:507-662`).
- Rule frontmatter: `globs`, `alwaysApply`, `description`, condition/astCondition/scope, `ttsr_trigger`; glob-looking conditions infer edit/write scope (`local_cmd.rs:299-370`).
- **Live runtime consumes rules only through `arag ttsr` CLI**, not prompt/runtime interception: `discover_rules` call sites are `local_cmd.rs:935,1607`; `--no-rules` only removes `RULES.md` (`main.rs:905-910`). `[INFERENCE: no other call site found]`

---

## B. Doc/code drift and partial gaps

| # | Documented claim | Live reality | Verdict |
|---|---|---|---|
| B1 | Repo map / persistent ranked map absent (`research-2026-11-06-code-analysis.md` §9.2) | Fully implemented `pi-ast/src/map.rs` + `tools/repo_map.rs` | **Stale doc** |
| B2 | Rust local memory is file-only, no SQLite/mnemopi (`research-2026-11-06-memory-skills.md` §2.2, §6) | SQLite `memory_store` v2 with FTS5, embeddings, consolidation, migration; mnemopi still absent | **Partial stale**: store implemented; mnemopi gap true |
| B3 | LSP passes full env incl. API keys (`research-2026-11-27-08-reviews-audit.md` §1.2) | LSP now `env_clear()` + allowlist + wrapper env | **Fixed** |
| B4 | D13: LSP optional, disabled by default (`agent-architecture-decisions.md:76-78`) | `LspConfig::DEFAULT_ENABLED = true` (`config.rs:355-360`) | **Drift / unresolved decision** |
| B5 | DAP project config is trusted/loaded safely | DAP loads project `.omp/.claude/...` without trust verdict; spawn inherits ambient env | **Gap** |
| B6 | Skills missing omp-plugins/opencode/github/claude-plugins, globs/alwaysApply, kebab-case disable (`research-2026-11-06-memory-skills.md` §4.4) | Still missing in live `skills.rs` | **Still true** |
| B7 | Prompt skill listing is `name: description` line list (TS behavior) | Live emits `# Skill: name` block; still metadata-only | **Drift in rendering** |
| B8 | Rules are only `RULES.md` context (`main.rs:905-910`) | Full TTSR rule discovery/frontmatter exists, but only `arag ttsr` CLI consumes it; no live stream interception found | **Partial**: discovery done, enforcement not wired |
| B9 | `ast_edit` rewrite is exclusive and uses `WorkspaceWriter` (`research-2026-11-27-02-tools-unification.md` §2) | `ast_edit` has no `is_exclusive()` override; direct `write_atomic` in apply path, no per-file `WorkspaceWriter` lease; application goes through `write xd://resolve` (which is exclusive) | **Partial drift / risk** |
| B10 | Memory `learn` writes `learned.md` directly | `learn` writes SQLite then exports derived `learned.md`; store is authority | **Stale doc** |
| B11 | Mnemopi settings parsed (or expected) | Only `memory.backend`; `mnemopi.*` schema keys not parsed | **Gap remains** |
| B12 | `autolearn.minToolCalls` default implied 3 (doc does not pin) | Live `DEFAULT_MIN_TOOL_CALLS = 5` (`config.rs:666-667`) | **Pin to live value** |
| B13 | Memory consolidation may call LLM / `llm_used` nonzero | Deterministic union-find over cosine pairs; `llm_used=0` | **Live stricter** |
| B14 | `ast_edit`/hashline preserve `WorkspaceWriter` lease | `ast_edit` uses `write_atomic`; hashline/edit tool uses exclusive behavior but no cross-file lease | **Partial** |
| B15 | `lsp`/`debug` mutating actions cannot race writes | Mutating LSP (`rename`, `format`, `request`) and mutating DAP are Write/Exec tier but tools are not `is_exclusive()`; scheduler exclusivity is only `write`/`bash`/hashline/checkpoint/ask/think/todo/task/hub/yield | **Risk: approval tier ≠ scheduler exclusivity** |
| B16 | `memory://root` read-only; record writes only existing ids | Verified: root/list/search read-only; `memory://<id>` only updates existing record, cannot create asserted or delete | **Matches** |
| B17 | `read` tool auto-summary caps 2 MiB/20k lines, 100-line floor, 50/100 unfold | Summary engine present; read integration caps documented in `docs/tools/read.md`; live summary tests cover BFS unfold | **Matches within inspected scope** |

---

## C. Pending / deferred items, with exact conditions and nuances

### C1. LSP policy
- D13 clarification says LSP is optional and disabled by default; live default is enabled. Condition to close: either flip `DEFAULT_ENABLED` to false or explicitly amend D13 and document owner opt-in.
- External language servers are never installed by `lsp/config.rs`; unresolvable binary simply removes the server.
- `idleTimeoutMs=0`/`null` disables idle-kill only for that workspace; config cache key includes trust bit.
- `matrix`/`capabilities` must not spawn servers; LSP operations require `ready`.
- Shared mux server is never killed by idle checker; broker owns shutdown.

### C2. DAP trust and environment
- Project `dap.*` overlays are loaded without `allow_project_config` or `TrustStore` check. Condition: add trust verdict gating before launching project-provided adapter commands.
- DAP spawn inherits ambient env (`non_interactive_env` then `env`; no `env_clear`). Condition: allowlist env like LSP, especially provider keys/SSH agent.
- Project adapter command + args are executable; untrusted repo can supply a command path. Treat as code execution until gated.

### C3. Mnemopi
Deferred entirely:
- No SQLite bank manager, no `working_memory`/`episodic_memory`/`memoria_*`, no `recall/retain/reflect`, no `memory_edit`, no `memory://<id>` mnemopi ids.
- No `mnemopi.*` config parsing; only `memory.backend` and enum.
- `learn` intentionally not mounted for `mnemopi`/`hindsight`; `manage_skill` backend-independent.
- Silo/scoping features from TS (`global`, `per-project`, `per-project-tagged`) are not implemented in live memory_store; live supports `global`, `project`, `session` only.
- Condition to implement: separate store/backend, scope/bank model, provenance/trust tiers, consolidation/degradation, delete cascade semantics.

### C4. Skills
- Missing providers: omp-plugins 90, opencode 55, github 30, claude-plugins.
- Missing skill frontmatter: `globs`, `alwaysApply`, kebab-case `disable-model-invocation`.
- Missing TS behavior: no `read` tool ⇒ skill list not rendered; Rust always composes skill descriptors.
- Prompt listing shape differs from TS; if exact TS parity required, change `compose_system_prompt`.
- Managed skills: cannot shadow authored; 64 KB cap; create O_EXCL, update O_NOFOLLOW; symlink root/dir/file refusal; per-name serialization; hidden still callable.

### C5. Rules / TTSR
- Full discovery implemented, 27 built-in rules, frontmatter conditions/scopes/globs.
- No runtime streaming interception call site; only `arag ttsr` inspect/test/scan. Condition: wire rule evaluation into stream or explicitly document CLI-only.
- TTSR settings (`repeat`, gap, disabledRules, builtinRules) partially parsed: live only `ttsr.builtinRules` observed in discovery; other keys remain schema-only.
- Project rules are context unless runtime enforces them; cannot widen permissions.

### C6. ast_edit / formatting / atomicity
- No automatic formatter after `ast_edit`/hashline write. Condition: explicit integration point, then re-tag/re-read and parse-regression check.
- Multi-file AST edits remain non-transactional; stale gate prevents stale write, but partial apply is explicit; no FS lock.
- `ast_edit` preview is non-exclusive (read-only); application occurs through exclusive `write xd://resolve`. If contract requires tool-level exclusivity, add `is_exclusive()` or document the split.
- `write_atomic` preserves mode/symlink but is not a workspace lease; `WorkspaceWriter` per-file lease is not used by `ast_edit`.

### C7. RepoMap
- Tag coverage: 24 tag-query languages; 20 declaration-map languages; remaining 38 languages unsupported; audit still pending.
- No definitions/references resolution, call graph, or import graph; names only.
- Tag extraction failure falls back to declaration map; parse errors leave partial symbols and `parsed=false`.
- Truncation is structured and reported (`symbol_truncated`, `per_file_truncated`, token/byte truncation), must not silently cut JSON.

### C8. Memory store
- Tokenizer `unicode61`: no stemming; Russian porter not applicable.
- Consolidation marks duplicates but does not merge bodies/delete; canonical = lowest id; scope/model/dim must match.
- Asserted rows never TTL-expire; inferred rows do.
- Delete removes embeddings + row; it does not touch session history or other derived artifacts beyond the store.
- Process-local per-path lock; cross-process race reported, not retried.
- Embedding rows keyed by model; changing model leaves old vectors unused for that query but not deleted.
- `memory_embeddings` provider failure collapses to lexical recall silently; no user-visible status unless caller inspects errors.

### C9. Embeddings
- HTTP path assumes OpenAI-compatible `/v1/embeddings`; no chunking; whole batch per request.
- `dimensions` omitted unless explicitly set.
- Local fastembed requires `local-embeddings` feature + runtime ONNX library; `ort-load-dynamic` avoids link-time hard dependency but still needs a loadable runtime at use time.
- Android/Termux build of fastembed/ORT not verified; no benchmarks.
- Local model download on first use; offline ⇒ `Unavailable` ⇒ lexical fallback.

### C10. LSP write actions
- `lsp rename`/`rename_file`/`code_actions apply`/`format` mutate via LSP and write tier; no tool-level exclusivity; approval is per-action (`approve.rs:167-176`).
- LSP workspace edits are applied through `apply_workspace_edit_with_lsp`; formatting action separate from write path.
- Project LSP config is fail-closed without trust; user/home config still applies.

---

## D. Open decisions and risks

1. **LSP default state (D13).** Must decide: keep live `true` and amend D13, or default `false` and require explicit owner enablement. Risk: external server auto-start and project-supplied commands.
2. **Trust boundary completeness (D20).** LSP project config now gated; DAP project config and `.omp/ssh.json` remain outside trust checks. Risk: untrusted repo executes adapter/SSH command or reads ambient credentials.
3. **DAP environment leakage.** No `env_clear()`; provider API keys/SSH agent can reach adapter and debuggee. Risk: secret exfiltration by project-controlled adapter.
4. **Approval tier vs scheduler exclusivity.** `lsp` mutating actions and `debug` mutating actions are Write/Exec tier but not `is_exclusive()`; scheduler parallelizes by exclusivity only. Risk: concurrent LSP rename/format or debug session mutation interleaving with file writes.
5. **AST edit atomicity.** No FS lock; per-file byte gate only; multi-file partial writes possible. Risk: external writer race; must not present as transaction.
6. **Format-on-write absent.** Formatter can change bytes after edit; next hashline/edit may see stale content unless re-read/re-tag. Parse regression detection must be explicit.
7. **RepoMap semantic labeling.** Aider-style PageRank is name heuristic; no type/call resolution. Risk: presenting it as semantic navigation. Must keep `syntax`/`heuristic` labels.
8. **Mnemopi absence.** No compatible backend; `memory.backend=mnemopi` is accepted but yields no memory tools. Risk: confusing configuration that appears enabled but stores nothing. Condition: either implement or reject/clearly report unavailable.
9. **Memory provenance/delete.** Asserted vs inferred implemented; TTL never expires asserted; delete does not touch chat history. Risk: users expect delete to erase all derived copies; documentation must state store-only deletion.
10. **Consolidation semantics.** Marking `consolidated_into` hides duplicates from recall but does not merge text; canonical lowest id is deterministic but arbitrary. Risk: losing the richer duplicate body; future merge policy needed.
11. **Embedding backend choice.** Remote HTTP vs local fastembed; remote may leak memory text to endpoint; local is heavy/Android-unverified. Silent lexical fallback can mask a broken provider. Decision needed on status reporting/telemetry.
12. **Rules enforcement.** TTSR discovery exists but live stream interception is absent; `RULES.md` is context. Risk: users believe project rules are enforced when they are only prompt context.
13. **Skills parity.** Missing providers and activation metadata; prompt rendering differs. Risk: users expect TS skill ecosystem behavior (globs/alwaysApply) that live code ignores.
14. **Managed skill race window.** Read-time containment recheck does not prevent concurrent replacement between check and read. Mitigated by symlink refusal and O_NOFOLLOW on writes, not a full TOCTOU fix.
15. **LSP workspace corruption risk.** LSP rename/format edits are applied as received; no transactional cross-file guarantee. Must rely on LSP correctness plus per-file writes; concurrent edits can conflict.

---

## Final state

- Step 1 completed: priority docs fully read and extracted.
- Step 2 completed: every major documented claim cross-checked against the listed live files.
- No code changed.
- The largest live-code deltas from the older docs are: RepoMap now exists, memory is SQLite v2, LSP env leak fixed, DAP trust/env gap remains, LSP default enabled contradicts D13, and TTSR rules are discovered but not live-enforced.
