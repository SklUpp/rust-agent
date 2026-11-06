# Память и skills: доноры, API, границы (D-блок)

Дата: 2026-11-06. Read-only исследование. Код не писался, сборки/тесты не запускались.
Пины: original OMP `035e4f95c314f9aff2f5a779fa335422680f8e36`; Rust-форк `3008cfd0ca88bb0e28323561a165f39efa7159e1` (parent-supplied; читался локальный checkout в `/data/data/com.termux/files/home/omp-work`). Mem0: `main` = commit `ed1f10fc87080f0b67f99be370817504888462ab` (2026-11-04), package version `2.2.1`.
Режим: source facts отделены от предложений; непроверенное помечено `[UNVERIFIED]`/`[INFERENCE]`. Runtime permissions, code intelligence, GUI и provider-адаптеры в этот блок не входят.

## 1. Требования из журнала

| ID | Что принято | Следствие для этого блока |
|---|---|---|
| D14 | Явная память, фоновое извлечение опыта, генерация переиспользуемых skills; автоматизация настраивается; механизмы/scope/provenance/delete/consolidation/безопасность проработать при реализации; веса модели не меняются | Нужны контракты памяти и learning, а не готовый framework |
| D52 | Общий настраиваемый каталог skills; модели сначала name+description, полный текст по требованию или явно заранее для роли/задачи; ручной вызов; загрузка не запускает вложенные скрипты | Discovery, prompt-метаданные, `skill://`-подобное чтение, manual invocation |
| D53 | Global — предпочтения владельца и переносимый опыт; project — знания проекта. Хранить источник, различать явное указание пользователя и вывод агента. Просмотр/исправление/удаление доступны пользователю. Память — контекст, не инструкции/permissions. Удаление записи не удаляет копии из истории диалогов | Обязательные: scope, provenance, edit/delete, отдельная граница derived data vs session history |
| D58 | Пользовательские команды — через skills, без отдельной системы prompt templates; синтаксис/аргументы/конфликты имён отложены | Ручной вызов skill — входная точка для команд |
| D15 | Проектные данные не расширяют operator permissions; trust/policy отдельно | Запись памяти/skills — trust boundary, не «permission system» |

Пробелы, которые журнал оставляет открытыми: точный store (SQLite/файлы), набор backends, формат provenance, политика consolidation, набор skill-провайдеров, семантика удаления производных записей.

## 2. Доступная основа

### 2.1 Original OMP (TS), MIT

Корневой `LICENSE` — MIT (`Copyright (c) 2025 Mario Zechner; 2025-2026 Can Bölük; 2026 Stencil Labs, Inc.`). Пакет `packages/mnemopi` — MIT, `@oh-my-pi/pi-mnemopi@18.0.9`.

Ключевые узлы:

- `packages/coding-agent/src/memory-backend/types.ts:1-160` — интерфейс `MemoryBackend`: `start`, `buildDeveloperInstructions`, `clear`, `enqueue`, `status?`, `search?`, `save?`, `stats?`, `diagnose?`, `beforeAgentStartPrompt?`, `preCompactionContext?`; backend id `off|local|hindsight|mnemopi`; статус содержит `writable/searchable/scope/retainBank/recallBanks/workingCount/episodicCount/tripleCount`.
- `memory-backend/local-backend.ts` — файловый backend: `save` + `status` (search не поддерживается).
- `memories/index.ts` — локальный pipeline: Phase 1 per-session extraction, Phase 2 cross-session consolidation; артефакты `MEMORY.md`, `memory_summary.md`, `skills/`, `rollout_summaries/`, `learned.md`; `getMemoryRoot`, `encodeProjectPath`, `redactSecrets`, `neutralizeInjection`, `saveLearnedLesson`, `readLearnedLessons`; `/memory` entry points `clearMemoryData`, `enqueueMemoryConsolidation`.
- `memories/storage.ts:24-70` — SQLite job queue: таблицы `threads`, `stage1_outputs`, `jobs` (`kind`, `job_key`, `status`, `ownership_token`, `lease_until`, `retry_at`, `input_watermark`, `last_success_watermark`), WAL + busy_timeout.
- `packages/coding-agent/src/prompts/memories/*` — extraction/consolidation/read-path prompts.
- `packages/coding-agent/src/mnemopi/*` — backend wrapper: `state.ts`, `config.ts`, `backend.ts`, отдельный embed subprocess (`embed-client.ts`, `embed-worker.ts`).
- Tools: `tools/memory-recall.ts`, `memory-retain.ts`, `memory-reflect.ts`, `memory-edit.ts`, `learn.ts`, `manage-skill.ts`. `recall`/`retain`/`reflect` доступны для `hindsight|mnemopi`; `memory_edit` — только `mnemopi`; `learn` — `local|hindsight|mnemopi` при `autolearn.enabled`.
- `autolearn/managed-skills.ts`, `autolearn/controller.ts` — генерация managed skills и post-stop capture.

### 2.2 Rust-форк: что уже есть

| Файл | Содержимое | Факт |
|---|---|---|
| `crates/omp-cli/src/local_memory.rs` | `memory_root`, `encode_project_path`, `save_learned_lesson`, `read_learned_lessons`, `read_memory_summary`, `build_memory_instructions`, `neutralize_injection`, `redact_secrets` | Только файловый local backend; summary не производит |
| `crates/omp-cli/src/memory_protocol.rs` | `memory://root` + относительные пути под project memory root | «Mnemopi ids and Hindsight pointers are not implemented here» (:11) |
| `crates/omp-cli/src/managed_skills.rs` | `managed_skills_dir`, `sanitize_skill_name`, `sanitize_managed_description`, `to_skill_frontmatter`, `write_managed_skill`, `delete_managed_skill` | Изолированный каталог, symlink/O_EXCL/O_NOFOLLOW guard, 64 KB cap |
| `crates/omp-cli/src/skills.rs` | discovery, `SkillSnapshot`, `load_with`, `expand_skill_invocation`, `parse_skill_invocation`, `build_skill_prompt_message`, `SnapshotAutoloadResolver` | Провайдеры native/claude/agents/codex/custom/managed |
| `crates/omp-cli/src/skill_protocol.rs` | `skill://<name>` и относительные ресурсы | traversal/symlink containment |
| `crates/omp-cli/src/autolearn.rs` | `learn`, `manage_skill`, gating, prompts | Только `local` backend монтирует `learn`; Mnemopi/Hindsight — нет (`:222-226,1838-1840`) |
| `crates/omp-cli/src/config.rs` | `memory.backend` (`off/local/hindsight/mnemopi`), `memories.*`, `autolearn.*`, `skills.*` | `mnemopi.*` поля не парсятся |
| `crates/pi-natives/src/vectors.rs` | `vector_index_top_k(matrix: Float32Array, dimensions, query: Float64Array, limit)`, `cosine_similarity_pairs` | Точный cosine/top-k, MIT |

Вывод по форку: полноценного Mnemopi backend в Rust нет; `MemoryBackend::Mnemopi` — только enum-значение и migration `mnemosyne→mnemopi` (`config.rs:1508-1515`).

### 2.3 Mem0 (Python), Apache-2.0

Лицензия: `LICENSE` — Apache-2.0 (`Copyright [2023] [Taranjeet Singh]` в appendix); `pyproject.toml` — `name = "mem0ai"`, `version = "2.2.1"`, `license = "Apache-2.0"`, Python `>=3.10,<4.0`, mandatory `qdrant-client`, `pydantic`, `openai`, `httpx`, `posthog`, `sqlalchemy`, `protobuf`; vector stores — extras.

Публичный API (`mem0/memory/main.py`):

| Symbol | Signature / поведение |
|---|---|
| `Memory.add` | `add(messages, *, user_id, agent_id, run_id, metadata, timestamp, expiration_date, infer=True, memory_type, prompt)`; scope обязателен; `infer=True` → LLM extraction |
| `Memory.search` | `search(query, *, filters, ...)`; entity ids только через `filters` |
| `Memory.get` / `get_all` | по id / по `filters` |
| `Memory.update` | `update(memory_id, text=None, metadata=None, expiration_date=UNSET, data=None)`; `user_id/agent_id/run_id/actor_id` неизменяемы |
| `Memory.delete` | удаляет vector row, пишет history `DELETE`/`is_deleted=1`, чистит entity links |
| `Memory.delete_all` | требует хотя бы один filter; иначе `ValueError`; `reset()` — всё |
| `Memory.history` | записи из SQLite `history` |
| `Memory.reset` / `close` | drop tables/collection / закрытие SQLite |

Данные:
- `memory/configs/base.py:15-55`: `MemoryItem{id,memory,hash,metadata,score,created_at,updated_at}`; `MemoryConfig{vector_store,llm,embedder,history_db_path,reranker,version="v1.1",custom_instructions}`.
- `memory/storage.py`: `history(id, memory_id, old_memory, new_memory, event, created_at, updated_at, is_deleted, actor_id, role)`; `messages(id, session_scope, role, ...)`; `add_history`, `batch_add_history`, `get_history`, `save_messages`, `reset`.
- Provenance в payload: `data`, `hash` (md5), `text_lemmatized`, `created_at`, `updated_at`, `user_id/agent_id/run_id/actor_id/role`, `expiration_date`. `_strip_identity_keys` (`main.py:140-159`) отбрасывает identity-поля из `metadata`.
- `add` phases (`main.py:900-1010`): 0 context (last 10 messages), 1 vector search existing top_k=10, 2 один LLM-вызов `ADDITIVE_EXTRACTION_PROMPT`, 3 batch embed, 4 CPU processing (hash/lemmatize), 5 hash dedup.
- Prompts (`mem0/configs/prompts.py`): `FACT_RETRIEVAL_PROMPT`, `USER_MEMORY_EXTRACTION_PROMPT`, `AGENT_MEMORY_EXTRACTION_PROMPT`, legacy `DEFAULT_UPDATE_MEMORY_PROMPT` (ADD/UPDATE/DELETE/NONE), `ADDITIVE_EXTRACTION_PROMPT` (V3, только ADD + `linked_memory_ids`), `PROCEDURAL_MEMORY_SYSTEM_PROMPT`.
- Search: `_search_vector_store` смешивает semantic/keyword/entity; `_compute_entity_boosts`; `lemmatize_for_bm25` — optional `spacy` extra.

Ограничение: это Python-фреймворк с обязательным vector store (default Qdrant) и LLM/embedder провайдерами; официального Rust-порта нет. Использовать можно отдельные механики (immutable identity, history, delete gates, extraction contract), не весь framework.

### 2.4 Готовые Rust-срезы (по реальным пробелам)

| Кандидат | Версия/license | API / факт | Применимость |
|---|---|---|---|
| `rusqlite` + `libsqlite3-sys` | 0.33.0 / 0.31.0; MIT; в lock | `bundled`; build.rs включает `SQLITE_ENABLE_FTS5`, `FTS3`, `JSON1`, `LOAD_EXTENSION`, `RTREE`; на Android `SQLITE_TEMP_STORE=3` | База для memory store; FTS5 доступен без внешних зависимостей |
| `pi-natives::vector_index_top_k` | локальный крейт, MIT | точный cosine top-k по нормализованной `f32`-матрице; ranking = TS stable sort | Уже покрывает то, что mnemopi TS делает через N-API; ANN пока не нужен |
| `fastembed` | 7.6.0, Apache-2.0 | default features `onnx`, `ort-download-binaries-native-tls`, `hf-hub-native-tls`, `image-models`; `TextEmbedding` (docs.rs) | Локальные embeddings — только если измерена необходимость; тянет ONNX Runtime |
| `usearch` | 2.26.6, Apache-2.0 | C++ ANN (HNSW), cxx-биндинги | Только при доказанном масштабе; для локальной памяти избыточен |
| Mem0 Rust | — | не найдено официального крейта | Не включать целиком |

`[UNVERIFIED]`: сборка `fastembed`/`ort` под Android arm64/Termux; поведение `load_extension` в `rusqlite` без feature `load_extension` (в `omp-cli/Cargo.toml` включён только `bundled`).

## 3. Контракт памяти

### 3.1 Режимы и scope

Original OMP: `memory.backend = off | local | hindsight | mnemopi`. Local — файлы, Mnemopi — SQLite, Hindsight — remote. `per-project` банк выводится только из `cwd`: `basename + Bun.hash(absPath).toString(36)`, лимит 64 символа (`packages/coding-agent/src/mnemopi/config.ts:175-195`). Git-root намеренно не используется (иначе `.git` меняет банк, #2412); legacy-банки подхватываются только если все их rows имеют тот же `metadata_json.$.cwd` (`:199-238`).

Scope-модель:
- `global` — один shared bank;
- `per-project` — запись и recall только project bank;
- `per-project-tagged` — запись в project bank, recall из project + shared, результаты объединяются и де-дуплицируются (`state.ts:744-780`);
- `scope` колонка в строках (`global` по умолчанию), `metadata_json.cwd` — фактический project-фильтр.

### 3.2 Mnemopi data model (факты)

`packages/mnemopi/src/core/beam/schema.ts` создаёт:
- `working_memory`: `id, content, embed_text, source, timestamp, session_id, importance, metadata_json, veracity, memory_type, consolidated_at, recall_count, last_recalled, valid_until, superseded_by, scope, author_id, author_type, channel_id, trust_tier, validator, validated_at, validation_count, event_date, event_date_precision, temporal_tags, corrected_by, created_at`.
- `episodic_memory`: те же поля + `rowid`, `summary_of`, `tier`, `degraded_at`, `binary_vector`.
- `scratchpad`.
- `fts_working` (fts5, `id UNINDEXED, content`) и `fts_episodes` (external content + triggers `em_ai/em_ad/em_au`); `fts_working` синхронизируется триггерами `wm_ai/wm_ad/wm_au` по `COALESCE(embed_text, content)`.
- `memoria_facts` (versioned: `version_id`, `previous_value`, `valid_from_msg_idx/valid_to_msg_idx`, `source_memory_id`), `memoria_timelines`, `memoria_instructions`, `memoria_preferences`, `memoria_kg`, `facts` (+`fts_facts`), `annotations`, `triples`, `consolidation_log`, `memory_embeddings(memory_id, embedding_json, model)`, `memory_validations` (trigger держит 3 последние).

Индексы: `idx_wm_unconsolidated`, `idx_em_scope_imp WHERE superseded_by IS NULL`, `idx_wm_session_recall WHERE valid_until IS NULL` и др.

`BankManager` (`core/banks.ts`): `default` → `<dataDir>/mnemopi.db`; прочие → `<dataDir>/banks/<name>/mnemopi.db`; `createBank/deleteBank/listBanks/renameBank/getBankDbPath/getBankStats`; имя ≤64, `[A-Za-z0-9_-]`; `default` нельзя удалить/переименовать без `force`.

### 3.3 Provenance

Доступные поля на записи: `source`, `timestamp`, `session_id`, `metadata_json` (в coding-agent: `cwd`, `session_id`, `source_id`, `message_count`, `retained_through_user_turn`, `context`, `tool`), `veracity` (`stated|inferred|tool|imported|unknown` + строка), `memory_type`, `trust_tier` (`STATED` default, `IMPORTED` защищён от trim), `author_id/author_type/channel_id`, `recall_count/last_recalled`, `valid_until/superseded_by/corrected_by`, `validator/validated_at/validation_count`.

`retain` tool пишет `source:"coding-agent-retain"`, `veracity:"tool"`, `memoryType:"fact"`, `importance:0.75`, `scope:"bank"` (`tools/memory-retain.ts:39-70`). `learn` — `source:"coding-agent-learn"`, `importance:0.8`, `veracity:"tool"`, `memoryType:"fact"` (`tools/learn.ts:60-85`). Transcript retention — `source:"coding-agent-transcript"`, `importance:0.65` (`state.ts:523-546`).

### 3.4 Edit / delete (Mnemopi TS)

`memory_edit` schema: `{op: update|forget|invalidate, id, content?, importance?, replacement_id?}` (`tools/memory-edit.ts:6-13`).

Матрица (`state.ts:283-368`):
- `update` — только `working`; меняет `content`/`importance`, ставит `embed_text=NULL`, перепланирует embedding (`store.ts:704-730`).
- `forget` — только `working`; hard delete строки **в пределах session_id** + каскад артефактов.
- `invalidate` — `working` или `episodic`; soft delete: `valid_until=now`, `superseded_by=replacement_id`; разрешён и для `scope='global'`.
- `fact` — read-only: `not_editable`, id резолвится для чтения (`state.ts:341-350`).
- `episodic` для `update/forget` → `not_found` (не ошибка «нет id»).

Каскад удаления (`store.ts:180-223`, `purgeWorkingMemoryArtifacts`): `facts WHERE source_msg_id IN (...)`, `annotations`, `memory_embeddings`, `memoria_facts/instructions/kg/preferences/timelines WHERE source_memory_id`, `gists WHERE memory_id`, `graph_edges WHERE source/target IN (memory ids, gist_<id>, fact ids)`; FTS обновляется триггерами. Всё в одной транзакции; `working_memory` удаляется отдельно тем же caller.

Что каскад **не** делает: не удаляет `episodic_memory` (для этого отдельный `invalidate`/свой путь), не трогает session transcripts/history диалогов, не чистит `consolidation_log`, не удаляет bank-файл. `/memory clear` удаляет scoped SQLite DB + WAL/SHM (docs/mnemosyne-memory-backend.md), т.е. это отдельная операция от `forget`.

TTL trim (`store.ts:225-262`): удаляет только `consolidated_at IS NULL`, `trust_tier IS NOT 'IMPORTED'`, старше `workingMemoryTtlHours` или сверх `workingMemoryLimit`; каскад тот же.

### 3.5 Извлечение и консолидация

Mnemopi (`core/extraction.ts:42-63`): `EXTRACTION_PROMPT_TEMPLATE` — JSON-категории `facts/instructions/preferences/timelines/kg`, язык-агностично, «persistent, non-transient». `core/extraction/client.ts` — `ExtractionClient.chat` (default model `google/gemini-2.5-flash`, OpenRouter base URL, fallback `google/gemini-flash-latest`, rate-limit backoff). LLM-режимы coding-agent: `smol|remote|none` (`docs/mnemosyne-memory-backend.md`).

Консолидация (`core/beam/consolidate.ts`):
- `sleep`/`sleepAllSessions` берут rows старше `floor(workingMemoryTtlHours/2)` (12ч при 24ч TTL), `consolidated_at IS NULL`; claim через `UPDATE ... SET consolidated_at = claimTs`; батчи.
- `consolidateToEpisodic` вставляет summary в `episodic_memory` (`:389-434`), `summary_of`, `scope`, `veracity`, author/channel.
- `degradeEpisodic` — tier 2/3 по 30/180 дням; `health`, `getContamination`, `memoriaRetrieve`.
- `weibull` decay, `recall_count/last_recalled`, `tier` — retrieval-time сигналы, не удаление.

OMP local pipeline (`memories/index.ts`): Phase 1 — по изменённым persisted sessions, модель роли `default`, вход `memories.phase1InputTokenLimit` (4000); Phase 2 — модель `smol`→`default`→session→registry, выход `MEMORY.md` + `memory_summary.md` + `skills/`; lease/heartbeat; stale skills prune; redaction до записи. `learned.md` Phase 2 не перезаписывает.

`learned.md` инварианты (`memories/index.ts:1286-1418`, Rust `local_memory.rs`): newest-first, dedupe by exact line, cap 100, content 2000 / context 400 chars, write path `neutralizeInjection → redactSecrets → bound`, read path neutralize+redact снова; managed только строки `- `; atomic rename; per-path mutex (cross-process lock не обещан). Строка: `- <content> _(context: <ctx>)_`.

Артефакты и владельцы:
- `<agentDir>/memories/<encoded-cwd>/` — `MEMORY.md`, `memory_summary.md`, `learned.md`, `skills/<name>/SKILL.md`(+`scripts/templates/examples`), `rollout_summaries/`.
- `<agentDir>/managed-skills/<name>/SKILL.md` — auto-learn managed skills, отдельно от consolidation `skills/`.

### 3.6 Mem0 как референс (не как runtime)

Что стоит перенять точечно:
- неизменяемые identity-поля при update (`_IDENTITY_KEYS`, `_strip_identity_keys`);
- `history` с `old_memory/new_memory/event/is_deleted/actor_id/role` как audit trail поверх destructive delete;
- `delete_all` требует scope-фильтр; `reset` — отдельная явная операция;
- extraction contract: существующие memories + last_k + observation date + dedup reference в одном prompt;
- hash-dedup и `text_lemmatized` как отдельное поле, не замена `content`.

Что не переносить: Qdrant/vector-store обязательность, PostHog telemetry, provider factories, весь `Memory`/`AsyncMemory` класс, Python deps.

## 4. Skills

### 4.1 Metadata

TS `SkillFrontmatter` (`capability/skill.ts:11-32`): `name?`, `description?`, `globs?: string[]`, `alwaysApply?: boolean`, `hide?: boolean`, `disableModelInvocation?: boolean` (нормализуется из kebab-case `disable-model-invocation`; явная нормализация видна в `discovery/agent-plugins.ts:179-181`), unknown keys сохраняются.

Runtime-факты:
- `name` по умолчанию = имя каталога; `description` обязателен для native/omp-plugins/github/custom-dirs, не обязателен для claude/codex/agents/opencode/claude-plugins.
- `hide`/`disableModelInvocation` убирают skill из prompt-листинга, но не отключают: `skill://` и `/skill:<name>` работают.
- `globs`/`alwaysApply` у skills в найденных путях не создают eager-activation; `alwaysApply` реально применяется к rules (`capability/rule-buckets.ts`). Для skills это metadata/UI-отображение `[INFERENCE: иного activation path не найдено]`.

### 4.2 Loading (TS)

Один уровень: `<skills-root>/<name>/SKILL.md`; вложенность не сканируется. `loadSkills()` — три прохода: capability providers → `skills.customDirectories` (`scanSkillsFromDir`, requireDescription) → managed (`omp-managed`, dead-last).

Провайдеры/приоритеты (TS): native 100, omp-plugins 90, claude 80, claude-plugins/agents/codex 70, opencode 55, github 30 (`.github/skills`, project-only), omp-managed 5. Dedup key = name, first wins (кроме custom, который переопределяет provider-skill с тем же именем; между custom — first wins). Symlink-safe dedup по `realpath`; collision warnings.

Фильтры: `disabledExtensions` `skill:<name>` → source toggles (`enableCodexUser`, `enableClaudeUser/Project`, `enablePiUser/Project`, `enableAgentsUser/Project`) → `ignoredSkills` glob → `includeSkills` glob (пусто = все). `agents` (`.agent[s]/skills`) имеет собственные toggles и не отключается Claude/Codex toggles. Провайдеры без toggle (`claude-plugins`, `opencode`, `github`) включены, если включён любой named third-party toggle.

### 4.3 Prompt и manual calls

- System prompt: список `name: description`; скрытые исключены; при отсутствии `read` tool список в TS не рендерится (`system-prompt.ts:956-990`). Полный текст не инжектится, модель обязана прочитать `skill://<name>`.
- `skill://<name>` → `SKILL.md`; `skill://<name>/<rel>` → файл внутри каталога. Guards: absolute paths, `..`, escape после canonicalize, symlink containment; `.md` → `text/markdown`, прочее → `text/plain`; нет fallback-поиска (TS `internal-urls/skill-protocol.ts`; Rust `skill_protocol.rs`).
- `/skill:<name> [args]`: leading или mid-prompt token; mid-prompt не срабатывает, если draft начинается с другого `/` или `!`/`!!`/`$`/`$$`; frontmatter срезается; сообщение содержит `[IMPORTANT: User invoked ...]`, body и `[Skill directory: ...]`; unknown name → текст без изменений; `skills.enableSkillCommands=false` отключает surface, не discovery. TS delivery mode = submission key (Enter → steer, Ctrl+Enter → followUp).
- `autoloadSkills` у агента: резолвится через session snapshot; unknown name пропускается; ошибка чтения/containment валит spawn; порядок declaration-order (`pi-agent/src/tools/task.rs:2721-2745`).

### 4.4 Rust-форк: покрытие и пробелы

Есть: native/claude/agents/codex/custom/managed; приоритеты 100/80/70/70/custom/5; name/description/hide/enabled; `no_skills`, `--skills` globs, include/ignore, `disabledExtensions`; custom overrides default provider (тест `skills.rs:1170-1192`); managed не может затенять authored (`:428-442`); session-scoped snapshot; `skill://`; `/skill:`; autoload resolver; RPC список `skill:<name>`; prompt получает `prompt_skills()` (`main.rs:2248-2249`, `rpc/session.rs:2257-2258`).

Не найдено в Rust (по сравнению с TS):
- провайдеры `omp-plugins` (90), `opencode` (55), `github` (30), `claude-plugins`;
- `globs`/`alwaysApply` в `parse_skill` (`skills.rs:760-800` парсит только `enabled`, `name`, `description`, `hide`, `disableModelInvocation`);
- kebab-case `disable-model-invocation` (Rust проверяет только camelCase) — `[INFERENCE]` gap для не-plugin провайдеров;
- TS delivery-mode routing (steer/followUp) — CLI расширяет prompt строкой и не имеет очередей;
- `hide` + контекстный фильтр «нет `read` tool» — Rust передаёт `prompt_skills()` без проверки наличия read.

## 5. Границы удаления derived data (свод)

| Операция | Удаляет | Не удаляет |
|---|---|---|
| Mnemopi `forget` | working row (session-scoped) + annotations, embeddings, extracted facts, memoria projections, gists, graph edges (транзакция) | episodic rows, session transcripts, consolidation_log, bank |
| Mnemopi `invalidate` | ничего физически; ставит `valid_until`, `superseded_by` | строки/артефакты; recall должен фильтровать `superseded_by IS NULL` / `valid_until` |
| Mnemopi TTL/overflow trim | только `consolidated_at IS NULL`, не `IMPORTED`, старые/сверх лимита + каскад | consolidated/episodic/imported rows |
| Mnemopi `/memory clear` | scoped SQLite DB + `-wal`/`-shm` | session history; соседние banks вне scope |
| OMP local `/memory clear` | `stage1_outputs`, `threads`, jobs kinds + (в source) `MEMORY.md`/`memory_summary.md`/`skills/` | `learned.md` (отдельный файл), session JSONL |
| OMP `learned.md` cap | старейшие bullet-строки сверх 100 | не-bullet строки/заголовки, summary |
| Managed skill delete | каталог `<agentDir>/managed-skills/<name>` | authored skills, consolidation `skills/` |
| Mem0 `delete` | vector row, entity links, history record `is_deleted=1` | другие memories; Qdrant collection |
| Mem0 `reset` | history+messages tables, vector collection | внешние exports/логи |
| Session transcript | — | память не удаляет историю диалогов автоматически (D53) |

Trust boundaries (не permissions): sanitize names (`^[a-z0-9][a-z0-9-]{0,63}$`), symlink refusal для root/dir/file, `O_CREAT|O_EXCL` / `O_NOFOLLOW`, path traversal + canonical containment, prompt-injection neutralization и secret redaction на write и read, read-only facts, `IMPORTED` защита от trim.

## 6. Реальные пробелы (факты, без выбора архитектуры)

1. Rust не имеет Mnemopi backend: ни store, ни recall/retain/reflect, ни `memory://<id>`, ни `memory_edit`; `learn` намеренно не монтируется для mnemopi/hindsight (`autolearn.rs:222-226,1838-1840`).
2. Rust local memory не производит summary/консолидацию: `read_memory_summary` только читает файл; SQLite job queue/lease/phase1/phase2 отсутствуют.
3. Rust не парсит `mnemopi.*` настройки (только `memory.backend`), значит backend нельзя сконфигурировать без новых полей.
4. Skills: отсутствуют 4 TS-провайдера, `globs`/`alwaysApply`, kebab-case hide, delivery queue routing.
5. Derived-state deletion в Rust отсутствует вместе с backend; текущий local backend удаляет только файлы через `/memory clear` и не имеет per-record delete API.
6. Embeddings: TS использует npm fastembed + subprocess и remote OpenAI-compatible API; Rust-путь не выбран. Точный vector kernel уже есть; локальная модель — отдельная тяжёлая зависимость.
7. FTS: bundled SQLite даёт FTS5; выбор tokenizer/stemming (unicode61 vs porter vs ICU) не сделан; для русского porter неприменим.
8. sqlite-vec: TS код проверяет наличие `vec_episodes`, но создание таблицы/загрузка extension в найденных путях mnemopi отсутствуют; в Rust `load_extension` feature не включён — `[UNVERIFIED]`/`[INFERENCE]`.

## 7. Непроверенное / ограничения evidence

- Всё выше — source/doc чтение; runtime-поведение (SQLite locks, Android builds, fastembed/ort, реальные LLM extraction quality) не проверялось.
- Line-числа относятся к локальному checkout; pins parent-supplied, дрейф не сверялся.
- `sqlite-vec` путь: только `vecAvailable`/SQL-запросы; нет доказательства, кто создаёт `vec_episodes`.
- Mem0 `delete_all` pagination/`DELETE_ALL_BATCH_SIZE` и expiration behavior прочитаны частично; полный async-путь не аудирован.
- `prompt`/`llm` extraction в mnemopi зависит от host credentials; fallback поведение без LLM описано в docs, не воспроизводилось.
- `alwaysApply`/`globs` для skills: отсутствие activation-path — вывод из поиска, не заявление о баге.

## 8. Открытые развилки для parent

1. Владелец memory store: новый модуль в `omp-cli` (рядом с session store) или отдельный crate; где живёт `memory://<id>`.
2. Формат provenance: колонки как в mnemopi (veracity/trust_tier/source) vs JSON metadata; нужен ли `history` audit trail (Mem0-подобный) поверх destructive delete.
3. Граница delete: hard delete + cascade vs soft invalidate по умолчанию; что считается derived data, подлежащей каскаду (gists/graph/facts).
4. Extraction: один LLM-вызов на turn vs batched phase1; роль модели; поведение без LLM (heuristic-only).
5. Embeddings: remote-only через уже принятый OpenAI-compatible слой vs локальный fastembed; FTS-only fallback.
6. Skills: набор провайдеров для первого релиза; поддержка `globs`/`alwaysApply`; маршрутизация ручного вызова.
