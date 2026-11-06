# Блок A — состояние, история/ветки, вызовы, durable delivery, артефакты/VFS, контекст/compaction

Дата: 2026-11-06. Read-only source pass; код/сборка/тесты не запускались.
Pins:
- original `can1357/oh-my-pi@035e4f95c314f9aff2f5a779fa335422680f8e36` — raw sources fetched in this pass;
- Goose `aaif-goose/goose@d66323246cb21f159994415c1f589368b85b0bb1` — raw sources fetched in this pass;
- fork `SklUpp/omp-rust-termux@3008cfd0ca88bb0e28323561a165f39efa7159e1` — **WT**: assigned local worktree anchors; remote pin independently not verified.

Legend: `[WT]` = local worktree anchor only, not remote-verified. `[INFERENCE]` = derived behavior, not executed. Coverage ≠ readiness: this document lists donor evidence, not a statement that a component is ready to ship.

## 1. Требования исследования / донорское покрытие

| Решение | Донорское покрытие (не readiness) | Статус |
|---|---|---|
| D17 ветвление | per-entry `parentId`, leaf, branch summary, durable branch marker, sibling tool results | original remote + fork WT |
| D18 длинный контекст | compaction entry, summary-first projection, retained tail, full archive separate, provider-native payload | original remote; fork WT только prune |
| D21/D22 VFS/сессии | `pi-vfs` provider/facade, `local://`/`artifact://` roots, session identity отдельно от проекта | original VFS plumbing remote; fork URL scopes WT; **изоляция не покрыта** |
| D32/D80 SQLite | Goose sessions/messages, OMP parentId, branches, calls, pending delivery, artifact metadata | Goose remote + OMP shape remote; calls/delivery/artifact metadata — пробел |
| D33/D34/D47 | intent before tool, outcome after, unknown outcome, fail-closed persistence gate | seams/контракты, не готовая реализация |
| D43 большие результаты | artifact spill, head/tail, ranges, 8 MiB inline cap, explicit truncation | original remote + fork WT |
| D49 сообщения | durable pending queue, between-step delivery, receipt ≠ processed | пробел: оба донора in-memory |
| D81 публикация | ID alloc, stage/verify/publish; **no-clobber только fork**; original — atomic replace; DB link after file | fork WT + original remote |

D21/D22: `pi-vfs` — это filesystem plumbing/injection, не OS sandbox и не граница изоляции. D21/D27 (VFS не sandbox, secrets вне VFS) остаются в силе; donor evidence не закрывает isolation.

## 2. Доноры: pin, license, symbols

### 2.1 Original OMP — MIT — remote-verified

- LICENSE: `MIT`, © 2025 Mario Zechner; 2025–2026 Can Bölük; 2026 Stencil Labs, Inc.
- Tree: `packages/coding-agent/src/session/`, `packages/coding-agent/src/irc/`, `packages/ai/src/providers/`, `crates/pi-vfs/`.

| Область | Файл / symbols |
|---|---|
| История/ветки | `session-entries.ts`: `SessionHeader`, `SessionEntryBase`, `SessionMessageEntry`, `BranchSummaryEntry`, `CompactionEntry`, `ResetBoundaryEntry`, `CURRENT_SESSION_VERSION=3`; `session-manager.ts`: `appendMessage`, `appendMessageToBranch`, `branch`, `branchWithSummary`, `createBranchedSession`, `fork`, `forkFrom`, `snapshotForReplication` |
| Контекст | `session-context.ts`: `buildSessionContext`, `resolveSessionLeafId`, `getOpenAiRemoteCompactionPayload`, `getAnthropicCompactionPayload`, `createCompactionSummaryMessage`; dangling-strip в том же файле |
| Provider replay | `packages/agent/src/replay-policy.ts`: `isProviderRefusalMessage`, `filterProviderReplayMessages`; `packages/ai/src/providers/transform-messages.ts`: `transformMessages`, `normalizeToolCallId`, `MAX_TOOL_CALL_ID_LENGTH=64` |
| Artifacts | `session/artifacts.ts`: `ArtifactManager`, `allocateId`, `allocatePath`, `save`, `writeArtifact`; `task/output-manager.ts`: `AgentOutputManager`; `task/executor.ts`: output artifact write |
| Artifact URI | `internal-urls/artifact-protocol.ts`: `ArtifactProtocolHandler`, `MAX_INLINE_ARTIFACT_BYTES=8*1024*1024` |
| Payload | `session/session-persistence.ts`: `truncateForPersistence`, `MAX_PERSIST_CHARS=500_000`, `BLOB_EXTERNALIZE_THRESHOLD=1024`; `session/blob-store.ts`: `BlobStore`, `BLOB_PREFIX="blob:sha256:"` |
| Mailbox | `irc/bus.ts`: `IrcBus`, `send`, `wait`, `take`, `unreadCount`, `unansweredSends`, `recordReply`, `MAILBOX_CAP=100`; receipts `injected/woken/revived/failed` |
| VFS | `crates/pi-vfs/src/lib.rs`: `Fs`, `BlockingFs`, `FileSystem`, path helpers; `provider.rs`: `FileSystem::{open,metadata,read_dir,is_native_local,backing_path}` |

### 2.2 Fork Rust — MIT — WT only

- LICENSE: MIT text in the worktree; same copyright holders. Remote LICENSE independently not re-fetched in this pass.
- All anchors below are `[WT]`: `omp-work/crates/...`.

| Область | Symbols |
|---|---|
| Session store JSONL | `[WT]` `omp-cli/src/session.rs`: `SessionHeader`, `SessionEntry`, `MessageEntry`, `SessionMessage`, `SessionStore::{branch_entries,branch_messages,branch_history,branch_to,branch_with_summary,append_entry,append_messages,append_lines_rolled_back}` |
| Context | `[WT]` `session_checkpoint.rs`: `CheckpointHost::{context_messages,apply_pending_rewind}`; `runtime.rs`: `replayable_entries`, `history_messages_from_entries` |
| Pairing | `[WT]` `pi-agent/src/loop.rs`: `repair_tool_batch_boundary`, `tool_batch_end` |
| Compaction | `[WT]` `pi-agent/src/compaction.rs`: `Compactor`, `CompactionReport`, `COMPACTION_NOTICE_PREFIX` |
| Mailbox | `[WT]` `pi-agent/src/mailbox.rs`: `Mailbox`, `IrcMessage`, `take_or_register`, `MAILBOX_CAP=100` |
| Delivery | `[WT]` `pi-agent/src/supervisor.rs`: `BackgroundResultSink`, `PendingDelivery`, `BackgroundDelivery`, `enqueue_delivery`, `take_deliveries_for`; caps `MAX_OUTSTANDING_RESULTS=64`, `MAX_RESULT_BYTES=64*1024`, `MAX_PENDING_RESULT_BYTES=8*1024*1024` |
| Child persist | `[WT]` `task_persisted_revive.rs`: `PersistedChildSession`, `output_id`, `output_published`; `subagent_lifecycle.rs`: `TranscriptJournal` |
| Artifacts | `[WT]` `pi-agent/src/tools/agent_output.rs`: `OutputIdAllocator`, `publish_agent_output`, `publish_agent_output_replace`, `stage_body`; `atomic_publish.rs`: `publish_staged_file` |
| Artifact URI | `[WT]` `pi-agent/src/tools/internal_urls_artifact.rs`: `MAX_INLINE_ARTIFACT_BYTES=8 MiB`, `resolve_artifact`, `open_artifact_file`; `read.rs`: `DEFAULT_MAX_BYTES=50*1024`, `DEFAULT_MAX_LINES=3000`, `Sel::Raw` guard |
| Agent/history URI | `[WT]` `internal_urls_agent.rs`: saved output only; `hub_park.rs`: `HistoryProtocolHandler`; `history_files.rs`: `O_NOFOLLOW` traversal |

### 2.3 Goose — Apache-2.0 — remote-verified

- LICENSE: Apache License 2.0.
- `crates/goose/src/session/session_manager.rs`: `CURRENT_SCHEMA_VERSION=18`, `SESSIONS_FOLDER="sessions"`, `DB_NAME="sessions.db"`, `create_schema`, `run_migrations`, `apply_migration`, `add_message`, `get_conversation`, `replace_conversation_inner`; pool: WAL, foreign keys, busy timeout 30 s.
- `crates/goose-provider-types/src/conversation/message.rs`: `Message {id, role, created, content: Vec<MessageContentBlock>, metadata}`, `MessageMetadata`, `ToolRequest {id, tool_call, metadata, tool_meta}`, `ToolResponse {id, tool_result, metadata}`.
- Exact DDL: `sessions` (Goose columns), `messages (id INTEGER PK AUTOINCREMENT, message_id TEXT, session_id TEXT FK, role TEXT, content_json TEXT NOT NULL, created_timestamp INTEGER NOT NULL, timestamp, tokens, metadata_json)`, `usage_ledger`; indexes `idx_messages_session_created(session_id, created_timestamp, id)` etc. **No `calls` table; no delivery/ack table. No message `parent_id`.**
- `add_message` uses `BEGIN IMMEDIATE`, serializes `content`/`metadata` with `serde_json`, clamps `created = max(message.created, MAX(created_timestamp))`, inserts `msg_<session>_<uuid>` when no id. `get_conversation` reads order `(created_timestamp, id)` and skips roles other than `user`/`assistant`.

## 3. Фактические границы

### 3.1 История и ветки

- Original remote: append-only JSONL; line 1 header; tree via `(id,parentId)`; active leaf mutable. `appendMessage` = child of current leaf, then leaf advances. `branch()` only moves leaf in memory; `branchWithSummary` sets leaf to `branchFromId` and appends a `branch_summary` whose `parentId` is that branch point, not the abandoned leaf.
- Original remote: off-branch append `appendMessageToBranch` records `activeLeafId` (the leaf at append time) so a reload replays that leaf instead of adopting the off-branch tail. `model_usage` uses the same pattern.
- Original remote: branched session copies root→leaf path only; `parentSession` is a **file path**, not a session id. Do not copy that semantics into SQL.
- `[WT]` Fork: `SessionEntry` has `id`, `parentId`, `timestamp`, flattened extras; `MessageEntry` same + typed `SessionMessage`. `branch_entries` walks from last physical entry via `parent_id`, stops on missing/cyclic parent; if `branch_chain_warning` (dangling/unexpected root), falls back to journal order. `branch_messages` additionally includes sibling `toolResult` rows matched by tool-call id, because production appends later results under the previous result, so parentage cannot identify a batch.
- `[WT]` Fork `branch_to` appends a metadata-only `branch_summary` marker with empty summary because its loader derives the leaf from the last physical entry; a bare in-memory leaf change would be lost on reload. This is a fork repair of an original in-memory-only leaf move.
- Ordering: Goose remote `(created_timestamp,id)` with monotonic clamp. Original remote/fork WT use physical append order; OMP ISO timestamps are not the authority for branch reconstruction. `[INFERENCE]` A SQL history table should treat the parent chain/sequence as authority; timestamps only for display.
- **Risk:** a single `sessions.active_leaf_id` is insufficient for late/background appends. Original stores `activeLeafId` per off-branch entry. In SQL, an off-branch row must carry the leaf snapshot (or append must not update `active_leaf_id`); otherwise a late worker result silently moves the user’s branch.
- **Risk:** recursive CTE rebuild from a corrupted parent graph can cycle; both donors use application-level cycle guards and fallback. A recursive CTE needs a path/cycle check or a depth bound.

### 3.2 Контекст и compaction

- Original remote `buildSessionContext(entries, leafId)`:
  - resolve leaf (explicit leaf, else replay `activeLeafId`, skip late usage markers); missing explicit leaf falls back to replayed leaf; `null` = empty;
  - walk leaf→root, reverse, cycle guard; metadata from path only (latest `compaction`, model roles, thinking level, service tier, mode, TTSR);
  - only the latest compaction; a later `reset_boundary` wins and suppresses earlier compaction + tail;
  - model projection: summary first, retained `firstKeptEntryId..compaction`, post-compaction entries; display transcript is separate and keeps all entries/compactions inline;
  - `branch_summary` becomes a user-visible message; rewind report duplicate suppression;
  - provider payload attached, not reinterpreted: OpenAI `preserveData.openaiRemoteCompaction` → `items`; Anthropic `preserveData.anthropicCompaction` + retained tail; `providerReplayThroughEntryId` covers snapshot-to-commit; `historyRewriteAt` predates first retained entry for Anthropic so signed thinking is not invalidated.
- `[WT]` Fork `CheckpointHost::context_messages`: branch entries; developer messages verbatim; assistant run + sibling tool results by call id, dedup by entry id; `branch_summary` as user; custom messages as system/guidance; ignores compaction rows. No summary-first projection.
- `[WT]` Fork `Compactor`: destructive in-memory prune to token budget; preserves leading system messages; drops leading orphan tool results; inserts a system prune notice; `CompactionReport` remaps cursor. It does **not** persist a summary or provider-native payload. It is a utility donor, not D18 projection.
- `[WT]` Fork `TranscriptJournal`: flush at every turn boundary before compaction; if flush failed or conversation shrank, refuses compaction; cursor advanced only by bytes/messages actually written. This is the correct “never prune unwritten” invariant.
- **Risk:** compaction summary and retained tail must be projected, not applied destructively to stored rows. D80: compaction changes model projection, not the source archive. A schema that `DELETE`s pruned rows loses D18 archive/replay and cannot reconstruct display transcript.
- **Risk:** provider-native compaction is route/model-bound opaque state. Original stores provider identity; foreign payload must be rejected. On a route change, plain summary + retained tail is safe; opaque replacement history is not.
- **Risk:** signed `thinking` / `redacted_thinking` / OpenAI reasoning `encrypted_content` / `anthropicServerTool` blocks must persist byte-for-byte. Any truncation, re-serialization, or image externalization that changes them causes provider 400.
- **Risk:** compaction cut inside a tool batch. Original strips dangling calls and neutralizes signatures; fork `repair_tool_batch_boundary` finishes a partial result run, then drops a trailing assistant tool call whose results were cut. Without this, provider synthesizes phantom “aborted” results.

### 3.3 Replay pairing

Fork-части таблицы — `[WT]`; original/Goose — remote.

| Risk | Evidence |
|---|---|
| Cut lands inside tool-result run | `[WT]` `repair_tool_batch_boundary`: if `messages[keep].role == Tool`, finish whole run; if kept tail ends with assistant tool call and next is not Tool, decrement keep. Original remote strips any assistant `toolCall` without matching result on selected path. |
| Tool result on sibling branch | `[WT]` `branch_messages`/`context_messages` pulls sibling `toolResult` by call id even when not ancestors; dedup by entry id. Original remote strips calls whose results are off the selected path. |
| Duplicate call ids across turns | `[WT]` `replayable_entries` removes only the result block immediately following the dropped assistant; a later turn reusing an id keeps its own result. `[INFERENCE]` Sibling pull must be scoped to the assistant entry/turn, otherwise duplicate ids attach the wrong result. |
| Modified signed thinking | Original remote: when a turn is rewritten, drop `redactedThinking`, clear `thinkingSignature`, drop empty turn; otherwise Anthropic rejects modified/latest signed thinking. |
| Cross-model / cross-provider replay | Original remote `transformMessages` normalizes tool ids, sanitizes malformed calls, demotes/rebinds thinking. Provider refusal turns are filtered by `filterProviderReplayMessages`. |
| Fork Anthropic route check | `[WT]` `assistant_blocks` replays signed thinking but does not compare persisted `message.api/provider` with the active route; `with_source` stamps route but conversion ignores it (`provider_anthropic.rs:1455-1500`). |
| Missing result for in-flight call | Original remote `inFlightToolCallIds` keeps the call visible during a live rebuild; completed-history rebuild strips it. |
| Orphan result | Original remote/provider `transformMessages` can convert to a stale user block; original projection avoids emitting it. A storage/rebuild path should not rely on provider repair for a normal resume. |
| Image payload | `[WT]` `normalize_image_payload` validates MIME (`png/jpeg/webp/gif`), base64 alphabet/padding, rejects external URLs and MIME mismatch; session path validates before write. Original remote externalizes large images instead of truncating. |
| Tool id wire limits | Original remote `MAX_TOOL_CALL_ID_LENGTH=64`, `[A-Za-z0-9_-]`; Responses composite `call_id|item_id` must pair by the `call_id` half only for Responses-origin ids. |

**Pairing rule derived from donors:** pair by stable call id within the selected branch/turn, not by parent chain alone; include sibling results only when the assistant call is on the branch; strip unpaired calls for model replay but preserve them for display (`history://`) with pending/orphan markers. Fork WT explicitly separates `read_transcript_file` (display, no sanitize) from `sanitize_replay` (model replay).

### 3.4 Вызовы: intent/outcome

- Donors do not implement it:
  - Goose remote: call id only inside `content_json` (`ToolRequest.id`/`ToolResponse.id`); no status/ack columns.
  - Original remote: tool call inline in assistant content; results separate messages; no call table.
  - `[WT]` Fork: `ToolStart/ToolEnd` events and `TurnAck`, but no intent-before-exec durable row; `TurnAckRequest` has only `turn_start` and `message_count`, no call/message ids.
- D80 permits a separate execution table **or** a history row type. The earlier SqliteSchemaDonor recommendation “do not add calls now” predates D80’s explicit intent/outcome requirement; treat it as “no donor schema,” not “not required.”
- Minimum constraints implied by D33/D34/D47:
  - intent committed before dispatch; outcome committed after return; no automatic retry when intent exists without outcome;
  - one intent per call, not per batch: D39 says a batch is not one atomic transaction and partial errors exist; per-call rows preserve partial success;
  - state must distinguish `unknown outcome` from `not started` and from `failed`; absence of result is not proof the tool did not run;
  - persistence failure pauses new tool/model calls (D34), while already-started actions are not repeated;
  - partial assistant stream persisted periodically with a stable message/turn id; final flush at exit;
  - tool contract decides whether a retry/recheck is safe; arbitrary shell/network effects remain non-exactly-once.
- `[INFERENCE]` A separate calls table is justified only if status queries/concurrency need it; otherwise a history row type (`call_intent`/`call_outcome`) keeps call content in one place. Either way, do not duplicate full args/results in two authoritative places.

### 3.5 Durable mailbox / pending delivery

- Original remote `IrcBus`: in-memory `Map<agent, IrcMessage[]>`; `send` fire-and-forget; receipt `injected/woken/revived/failed` describes transport, not model processing; a successful live hand-off is never buffered (avoids double delivery); only failed hand-off is buffered; cap 100 drops oldest with a debug log.
- `[WT]` Fork `Mailbox`: in-memory `HashMap<String, VecDeque<QueuedMessage>>`; `take_or_register` atomically takes buffered mail or registers a waiter; waiter drop recovers an unpolled hand-off into the queue to avoid loss; `MAILBOX_CAP=100`; no park/revive, receipts never `woken/revived`.
- `[WT]` Fork supervisor delivery: `PendingDelivery` in memory; `enqueue_delivery` under lock; `take_deliveries_for` removes and marks `consumed` under the same lock (exactly-once in-process); caps truncate bodies and reject when aggregate full. Crash after `take` but before the model request loses the body. `output_published` / contract writes are separate from delivery.
- D49 requires DB persistence and between-step delivery; neither donor provides it. No pending table, no delivery id, no ack state in Goose or OMP.
- Minimum consistency constraints:
  - persist pending message before live hand-off; otherwise crash between hand-off and DB write loses it;
  - stable delivery id; dedup on resume/context projection; at-least-once, never exactly-once model processing;
  - mark “injected” only when the message is durably appended to the target agent’s history in the same transaction; do not mark “model processed”;
  - if live hand-off succeeds but marking fails, resume may redeliver; consumer dedups by id;
  - if marking succeeds before the model request, crash loses the message; therefore do not mark before history commit;
  - order by monotonic sequence, not wall clock; cap/overflow must be explicit, not silent oldest-drop (donor cap is a UI/memory bound, not a durability policy);
  - wake is setting-driven; cancelled agent needs explicit resume; acceptance ≠ processing.
- `[INFERENCE]` The safe default is a write-ahead pending row + live signal after commit, with the delivery id also recorded in the appended history entry so projection can dedup without a second authority.

### 3.6 Artifacts и VFS

- Original remote `ArtifactManager`: per-session numeric id space; `#nextId` seeded by directory scan of `^(\d+)\..*\.log$`; `allocateId` sync counter; `allocatePath` sanitizes tool type to `[A-Za-z0-9_-]`, 64 chars; subagents adopt the parent manager so one id space/dir; cross-process allocation absent.
- Original remote `writeArtifact`: stage `${path}.tmp-<uuid>`, write, verify bytes/size/1-byte read, then `replaceFileAtomically`; this is atomic **replace**, not first-publish no-clobber. `AgentOutputManager` uniqueness relies on scan, not `O_EXCL`.
- **D81 correction:** no-clobber first publication is proven only by the fork WT path. Original `writeArtifact`/`executor` use atomic replace; do not attribute fork’s no-clobber guarantee to original.
- `[WT]` Fork `OutputIdAllocator`: seeds from `<root>/*.md|*.jsonl`; requested name first, then `-2`, `-3`; nested `Parent.Child`; reserves `__advisor`; `reserve(id)` for DB/contract ids; valid id rejects empty/leading/trailing dot/`..` and only allows ASCII alnum plus `._-`.
- `[WT]` Fork `publish_agent_output`: stage `.agent-output.md.stage-<pid>-<nanos>-<seq>` with `create_new`, `write_all`, `flush`, `sync_all`, size check; `publish_staged_file` primary `hard_link` (atomic no-clobber), fallback `renameat2(RENAME_NOREPLACE)`; collision → `Ok(None)`, existing file untouched. Follow-up replace only after real yield, canonicalizes root, rejects symlink/dir target, stages then `rename`.
- `[WT]` Fork `Supervisor::save_agent_output`: retries up to 64 candidate ids; on success `mark_output_published`; on collision reserves colliding id and allocates next; error returns `None`. `PersistedChildSession.output_id` is written only with `output_published=true`; consumers must use `child_published_output_id`, never a pending allocation.
- `[WT]` Fork ordering: `save_agent_output` → `persist_child_output_id`; if contract write fails, child is tombstoned and reported `failed (durable metadata)`, delivery still enqueued. So the artifact may exist as an orphan, but a consumer never gets a fake link.
- `[WT]` Fork `artifact://`: numeric per-session id; caller-owned roots only (session `local://` root / explicit artifact root), first root wins; no global session scan. These caller-owned-root/no-global-scan claims are **WT evidence**, not independently remote-verified. Full-content cap 8 MiB; over cap error suggests `artifact://N:1-3000` / `:raw:1-3000`; `read` resolves backing file and streams ranges; unbounded `:raw` >50 KiB blocked; default read 3000 lines/50 KiB; descriptor opened with `O_NOFOLLOW` component traversal; directory/non-regular file rejected.
- `[WT]` Fork `agent://`: saved `<id>.md` output only, never live transcript; nested `Parent.Child`; optional JSON extraction; no line selectors on extraction; id separate from supervisor `task-N`.
- `[WT]` Fork `history://`: read-only markdown transcript; live/parked/tombstoned; caller-first pinned roots; `O_NOFOLLOW` every path component; display path keeps unanswered/orphaned calls visible, while model replay sanitizes them. Fork `history://` and `agent://` deliberately separate. Caller-first root behavior is WT.
- Original remote `pi-vfs`: `Fs` async facade + `BlockingFs` sync twin; `FileSystem` provider sees every path verbatim including `scheme://authority/...`; native host paths fast-path; a URL without provider is `NotFound`, never resolved against cwd; provider can expose `backing_path` for realpath; `is_native_local` is a strong capability (true means no virtual descendants/interposition); `BlockingFs` uses `block_in_place`+`block_on` and panics on a current-thread runtime’s own thread.
- **VFS scope correction:** `pi-vfs` is plumbing, not isolation. It routes/injects filesystem providers; it does not create an OS security boundary, does not protect secrets from same-UID code, and does not replace D21/D27. VFS URL spelling helpers preserve `://` and percent-encode segments; `file_name` decodes once.
- **Gap:** integration of `pi-vfs` provider with `artifact://`/`agent://`/`history://` is not proven. Fork WT artifact readers are separate protocol handlers using `ResolveContext.local_roots`, not a mounted `FileSystem`. A future VFS mount must not lose the caller-first root and no-global-scan boundary.
- **Gap:** no artifact metadata table in either donor. D81 requires a DB link. No retention/GC coordination: original `omp gc` has blob write grace; fork WT has no session artifact GC.

### 3.7 DB/file ordering

Facts:
- Original remote artifact bytes are written before the task result is returned; the session JSONL result line is appended separately. There is no transaction spanning artifact file and JSONL entry. `artifactId` lives in result metadata; allocation itself is reconstructed by directory scan.
- `[WT]` Fork publishes file first, then marks record/contract; contract write failure downgrades the result and tombstones. This is a deliberate “file first, link second” order.
- Original remote blob externalization: blob bytes are written synchronously to the content-addressed store before the JSONL line referencing `blob:sha256:<hash>`; on failed session write it forgets cached refs so GC can collect unreferenced blobs.
- `[WT]` Fork JSONL append writes lines and `flush`es; on write/flush error it truncates back to the original size via `set_len`. It has no `sync_all`/`sync_data`; software-crash safety after a completed batch, not power-loss durability.
- SQLite commit and filesystem rename cannot be one atomic operation. D32/D81 explicitly do not promise power-loss durability without chosen sync settings.

Crash matrix (derived, `[INFERENCE]` unless source states):

| Crash point | State | Required behavior |
|---|---|---|
| After call intent commit, before dispatch | intent, no outcome | `not started` or `unknown` depending on dispatch marker; do not auto-retry unknown-outcome tools |
| During tool effect, before outcome commit | intent, no outcome | unknown outcome; explicit “interrupted, unconfirmed” per D47; no fabricated success |
| After outcome commit, before artifact metadata commit | history result durable, file may exist | orphan file eligible for maintenance GC; history must not claim a missing artifact |
| After artifact publish, before artifact metadata commit | file durable, no link | orphan file; GC after grace; retry publish must not clobber |
| Artifact metadata commit, then file lost | DB link, missing file | explicit missing-artifact error; do not silently regenerate/replace |
| History result transaction fails after tool success | effect happened, no result row | fail-closed D34; report unknown; do not repeat |
| DB commit success, artifact rename not durable / dir entry lost | DB link, file missing after crash | backup/repair check; SQLite durability settings do not cover FS dir fsync |
| WAL commit lost on power loss | DB older than file | orphan file; acceptable per D81 only if explicitly stated |

Constraints:
- Write artifact file completely; for a **new identity** use no-clobber publication where available (fork WT `hard_link`/`RENAME_NOREPLACE`). Original’s atomic replace is not a no-clobber primitive.
- Then commit its metadata/link. Never commit a link before the file is readable.
- Prefer one transaction for history outcome + artifact metadata + worker completion notification. The file is already published; if the transaction fails, the file is an orphan.
- If DB id allocation is authoritative, `reserve` the id in the file allocator before publishing, or allocator may hand out an id named only in DB.
- Backup/restore must include SQLite WAL and artifact/blob directories; no donor proves a consistent snapshot order. `[INFERENCE]` quiesce writers or copy DB after checkpoint plus artifacts; a live copy is not automatically consistent.

### 3.8 JSON payload / native format

- Goose remote: `content_json TEXT` = serialized `Vec<MessageContentBlock>`; `metadata_json TEXT` = serialized `MessageMetadata`; both via `serde_json`. Tool call/result are blocks with `id`; no normalized call columns. Role is `TEXT`; reader currently accepts only `user`/`assistant`, so a system/developer/tool-result role schema from OMP is not directly representable by Goose’s reader.
- Original remote + fork WT: message content is either plain text or ordered parts; fork preserves unknown/forward fields in `SessionMessage.extra` and `SessionEntry.extra` (`details`, `providerPayload`, `usage` extras, `responseId`, etc.). This matters for provider replay and forward compatibility. `usage: None` means provider reported none, never zero-filled.
- Original remote persistence transforms before JSONL line:
  - strings >500,000 chars truncated with `[Session persistence truncated large content]`, except signature keys;
  - image payloads (`content` image block, `images[]`, snapcompact frames) at >=1024 base64 externalized to `blob:sha256:<64hex>`, bytes written synchronously before the line;
  - signed/encrypted/opaque nodes returned verbatim without descending: signed `thinking`/`text`/`toolCall`, `redactedThinking`, OpenAI `reasoning.encrypted_content`, `anthropicCompaction`, `cursorHistory`, `geminiNativeContent`, valid `anthropicServerTool` block;
  - `jsonlEvents` stripped (already in artifacts); `lineCount` recomputed.
- Native payloads (original remote):
  - OpenAI Responses: `preserveData.openaiRemoteCompaction = {provider, replacementHistory}`; `getOpenAiRemoteCompactionPayload` validates non-empty provider and array items.
  - Anthropic: `preserveData.anthropicCompaction` with `content`, optional `signature`, `toolChanges`, `encryptedContent`, `filesText`, `retainedFiles`, `exactTail`; `exactTail` chooses message timestamp vs entry timestamp for `historyRewriteAt`.
  - `providerReplayThroughEntryId` marks the last branch entry represented by native history; later entries replay normally.
  - Route/model binding is mandatory. `[WT]` Fork has no native compaction write path; only imports foreign OpenAI compaction into JSONL (`foreign_session.rs`). `[WT]` Fork OpenAI-compatible never replays thinking/redacted; fork Anthropic replays signed thinking without active-route comparison.
- `[INFERENCE]` SQLite TEXT JSON is sufficient for fidelity, but must preserve exact serialized opaque subobjects; do not round-trip through a lossy typed model. JSON1 indexing can index ids, but call/result columns must not become a second mutable authority.
- `[INFERENCE]` Large inline base64 in SQLite rows bloats WAL/backups; original’s blob-ref/externalization policy is the donor-proven answer. If images stay inline, set explicit row/blob limits and do not truncate signed/opaque data.

## 4. Межблочные зависимости (внешние, не пересылать)

- Execution/safety (B): intent commit must be a pre-dispatch hook; persistence failure must gate new tool/model calls; artifact/workspace ownership must not be inferred from session id; per-agent shell state is separate from history.
- Providers (C): native payloads and tool-call ids are provider-specific; route/model metadata must be persisted per assistant turn; cross-route replay must reject/neutralize opaque payloads; image capability must not filter before request.
- Knowledge/extensions (D): compaction summary generation and memory references consume history/artifact metadata; summary is data, not a destructive rewrite.
- Product shell (E): `history://`/`agent://`/`artifact://` surfaces are read-only views over DB/file state; headless resume needs snapshots/cursors; no second store.

## 5. Что ещё не доказано

- No build/test/run; all behavior claims are static source reading. Crash matrix is derived, not fault-injected.
- Fork anchors are WT only. Fork pin `3008cfd…` is assigned worktree HEAD, not independently remote-verified; fork LICENSE was read only in the worktree.
- Original and Goose anchors were raw-fetched at their pins in this pass; line numbers may still shift, symbol names are the stable anchors.
- No SQLite schema/migration chosen; no WAL/busy-timeout/backup behavior tested on Android/Termux/Bionic.
- No measured performance for recursive branch rebuild, JSON scanning, large rows, or artifact range reads.
- Fork WT `SessionStore` has no fsync; exact power-loss behavior beyond `flush` is unproven.
- Fork WT SQLite present only for credentials/settings/stats/read-only tool; no session DB. All session schema work is new.
- Artifact GC/retention and DB↔file backup ordering are unspecified; D24/D32 remain open.
- `pi-vfs` + internal URL handlers integration is unproven; `BlockingFs` current-thread panic and Android URL behavior need tests.
- `pi-vfs` is not an isolation boundary; D21/D27 security claims remain unproven.
- Caller-owned artifact/history roots are WT claims; cross-session isolation is not independently verified.
- Native provider payload route compatibility (OpenAI Responses replacement history, Anthropic encrypted compaction, signatures) is not tested.
- ACP persistence path not traced in this pass.
- D80 “exact schema/migrations” remains a parent decision; this block supplies donor facts and risks, not an adopted schema or readiness.

## 6. Evidence-derived minimum (не принятая схема)

Только для сверки parent; не решение.

- `sessions`: Goose subset + `parent_session_id` + durable `active_leaf_id`; no recipe/schedule/container/threads/tags columns.
- `history`: one append-only table, `id` stable, `session_id`, nullable `parent_id`, `kind` (`message`/`branch_summary`/`compaction`/`reset_boundary`/custom), `role`, `content_json`, `metadata_json`, `created_at`, monotonic `seq`; branch rebuild by parent chain, display order by seq.
- `calls` or call row types: required by D80; per-call intent/outcome, `call_id`, tool, args, state, `entry_id`; not per batch.
- `deliveries`: pending message id, from/to/agent/session, body, created_seq, state (`pending`/`injected`), `history_entry_id`; dedup by id; no “processed” claim.
- `artifacts`: owner session/agent, URI/id, relative path, size, complete/truncated, content hash, created_at, retention; link committed after publish.
- Indexes: `(session_id, seq)`, `(session_id, parent_id)`, `(session_id, call_id)`, `(to_agent, state, created_seq)`, artifact id unique.

## 7. Anchor index

- Original remote entries/branch: `packages/coding-agent/src/session/session-entries.ts` L37-180, L160-260; `session-manager.ts` L3576-3630 (`appendMessage`, `appendMessageToBranch`), L4044-4054 (`branch`), L4092-4115 (`branchWithSummary`), L4118+ (`createBranchedSession`).
- Original remote context/compaction: `session-context.ts` L240-400 (leaf/path/metadata), L400-560 (reset/rewind/compaction payload), L560-760 (summary/kept tail/dangling strip/error turns).
- Original remote artifacts/VFS: `session/artifacts.ts` L20-200; `internal-urls/artifact-protocol.ts` L31-150; `task/executor.ts` L2900-2945; `crates/pi-vfs/src/lib.rs` L1-60; `provider.rs` L20-90; `fs.rs` L1-70.
- Original remote payload: `session/session-persistence.ts` L1-60, L240-330; `session/blob-store.ts` L1-60.
- Original remote mailbox: `irc/bus.ts` L60-160, L220-360, L392-405.
- Original remote replay: `packages/ai/src/providers/transform-messages.ts` L30-60; `packages/agent/src/replay-policy.ts` L1-20.
- Fork WT session: `crates/omp-cli/src/session.rs` L707-775, L963-1000, L1326-1345, L1842-1960, L2033-2076, L2085-2140, L2347-2580, L2912-2930.
- Fork WT context: `session_checkpoint.rs` L447-640; `runtime.rs` L6425-6560.
- Fork WT pairing/compaction: `pi-agent/src/loop.rs` L479-530; `compaction.rs` L1-180.
- Fork WT mailbox/delivery: `mailbox.rs` L1-260, L400-430; `supervisor.rs` L64-80, L1350-1465, L2280-2390; `subagent_lifecycle.rs` L290-380.
- Fork WT artifacts: `agent_output.rs` L1-300; `internal_urls_artifact.rs` L1-235; `read.rs` L230-310, L900-970; `internal_urls_agent.rs` L1-40; `hub_park.rs` L244-300; `history_files.rs` L1-160.
- Goose remote: `session_manager.rs` L41, L1040-1120, L1145-1245, L1390-1480, L2180-2260; `message.rs` L120-200, L820-900, L960-1000.
