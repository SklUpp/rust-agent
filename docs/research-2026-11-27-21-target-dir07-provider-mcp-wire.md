# Целевое исследование Stage 2: DIR-07 — wire-level контракт провайдеров и MCP (`TargetDir07ProviderMcpWire`)

Дата: 2026-11-27  
Источник: сырой отчёт `TargetDir07ProviderMcpWire` (только чтение кода и документации).

---

# DIR-07 final report — Provider + MCP wire contract

Read-only trace. No builds/tests/cargo run. Errata checked and used.

---

## Part A — Step 1 recap (provider wire)

| Finding | Verdict | Anchors |
|---|---|---|
| OpenAI request field preservation | Weak: fixed subset, no `max_tokens`/`temperature`/`top_p`/`stop`/`user`/`metadata` | `provider.rs:659-681`, `provider/http.rs:243-282` |
| OpenAI response preservation | Lossy: no `refusal`, `annotations`, `function_call`, `audio` | `provider.rs:785-796`, `:824-836`, `:1270-1345` |
| Anthropic response preservation | Unknown blocks dropped (`#[serde(other)]`), no `id/model/type/role` | `provider_anthropic.rs:3107-3115`, `:3204`, `:1662` |
| Ollama preservation | No `top_k/num_ctx/keep_alive`; response drops durations; config `Authorization` header ignored; cloud strips assistant thinking; synthetic tool ids | `provider_ollama.rs:348-360`, `:362-376`, `:976-981`, `:1387-1393`, `:4013-4016` |
| OpenAI reasoning replay drop | Confirmed: `Thinking`/`RedactedThinking` never serialized; only `reasoning_effort` goes out; no Responses API | `provider.rs:1004-1012`, `:1098-1143`, `:1188-1213` |
| Anthropic replay route gate | Missing: signed thinking replayed without checking `api/provider/model`; only Ollama gates (`is_same_model`) | `provider_anthropic.rs:1312`, `:1455-1510`; `provider_ollama.rs:1104-1108` |
| OpenAI SSE line cap | Unbounded: raw `Vec<u8>` grown until newline | `provider.rs:1369`, `:1444` |
| Non-stream body caps | OpenAI unbounded `response.text()`; Anthropic 8 MiB success/64 KiB error; Ollama 8 MiB NDJSON record | `provider/http.rs:339,295-306`; `provider_anthropic.rs:55-62`, `:1730-1755`; `provider_ollama.rs:145`, `:3754-3775` |
| Unredacted HTTP snippets | Confirmed OpenAI: `snippet` truncates only; callback-refreshed key absent from stream secrets | `provider/http.rs:295-306`, `:374-381`; `provider.rs:13,1492-1494` |
| Literal `apiKey` fallback | Confirmed: unset env name becomes literal credential, env-shaped value not registered for redaction | `config.rs:44-83` |
| Timeout wrapper | No field loss; retry only on timeout, stream retry only pre-first-chunk; retry ownership stacks (Ollama 5 attempts + empty retry + TimeoutProvider) | `timeout.rs:150-300`; `provider_ollama.rs:1409-1520` |

Errata corrections: Anthropic/Ollama caps are 8 MiB success / 64 KiB error, not "64 KiB all"; OpenAI reasoning-drop anchor is `provider.rs:1098-1143`, not `:1112-1170`.

---

## Part B — Step 2 (MCP)

### B1. Method matrix

Client API (`client.rs`) vs rmcp dispatch (`rmcp_adapter.rs:310-339`) vs raw transports:

| MCP method | Client | rmcp HTTP/stdio | Legacy SSE / stdio raw |
|---|---|---|---|
| `initialize`, `notifications/initialized` | yes | rmcp handshake / explicit | explicit (`client.rs:139-205`) |
| `tools/list` | yes, paginated | yes | passthrough |
| `tools/call` | yes | yes, mutating classification | passthrough |
| `resources/list` | yes (returns `[]` if cap absent) | yes | passthrough |
| `resources/templates/list` | yes; `-32601` → `[]` | yes | passthrough |
| `resources/read` | yes | yes | passthrough |
| `prompts/list` | yes; requires `prompts` cap | yes | passthrough |
| `prompts/get` | yes; single-shot | custom request | passthrough |
| `ping` (server→client) | handler replies `{}` | rmcp default | stdio/sse realize (`stdio.rs:210-217`, `sse.rs:400-410`) |
| `roots/list` (server→client) | advertised, served | `Handler::list_roots` | `roots_list_reply` |
| `resources/subscribe` / `unsubscribe` | **absent** | **unsupported** (`:338`) | raw passthrough only |
| `completion/complete` | absent; server capability erased | `map.remove("completions")` | n/a |
| `sampling/createMessage` | **not advertised**; rmcp default returns method-not-found | n/a | n/a |
| `elicitation/create` | **not advertised**; rmcp default declines | n/a | n/a |
| `logging/setLevel` | absent | unsupported (`:338`) | passthrough |

Anchors: client capabilities only `roots` (`client.rs:171-178`, `types.rs:228-233`; same literal in `rmcp_adapter.rs:160-167,218-225`). rmcp dispatch whitelist `rmcp_adapter.rs:310-339`. compeletions capability erased `rmcp_adapter.rs:255-258`. rmcp `create_message` default method-not-found and elicitation default decline: `vendor/rmcp/src/handler/client.rs:94-138`. No `resources/subscribe`/`notifications/resources/list_changed` anywhere in `crates/pi-agent/src/mcp` (grep empty). Errata M16 confirmed: prompts list/get exist (`client.rs:819,886`), unlike donor.

Consequence: resource update notifications are never requested or handled; prompt completions, sampling, elicitation are unavailable. Roots is the only server→client feature.

### B2. `structuredContent` / `_meta` flattening

- `MCPToolDefinition` models only `name/description/inputSchema` (`types.rs:285-298`) — `title`, `outputSchema`, `annotations`, `_meta` discarded at discovery.
- `MCPToolCallResult` has no `structuredContent` field (`types.rs:327-334`); serde silently ignores it. `_meta` is parsed but never read.
- Runtime converts every result to one string: text joined; image/audio/resource blob turned into `[Image: mime; base64=…]` / `[Audio: …]` / `[Resource: uri; base64=…]`; `details: None` (`runtime.rs:5723-5790`). So structured tool output and result `_meta` are lost before the model sees them.
- `resources/read` result drops `_meta` (`types.rs:414-419`); `prompts/get` drops `_meta`/message annotations (`types.rs:485-498`); prompt message `role` kept verbatim.
- `tools/call` error results are mutated: first text prefixed `Error: ` or a text block inserted (`client.rs:939-957`), so server payload is not byte-preserved.
- Errata item 6 ("MCP tool results flattened to strings") confirmed, with the added loss of `structuredContent`/`outputSchema` not mentioned there.

### B3. `tools/call` vs `list_tools` retry semantics

- `tools/call`: no automatic retry. Transport failure records `failed_epoch` and returns unchanged (`client.rs:910-960`). rmcp classification: mutating timeout/TransportSend/TransportClosed/502/503 → `UnknownOutcome`; non-mutating timeout → `Timeout`; definite status → `Http` (`rmcp_adapter.rs:662-720`). Runtime then emits "execution uncertain; check remote state" only for `UnknownOutcome`/`Timeout`/`AcceptedStreamError`/`Closed` (`runtime.rs:5386-5420`).
- `list_tools`: the only method with automatic replay. On `is_retriable_connection_error || is_unverified_delivery`, it forces `reconnect_until` when the epoch still looks healthy, then reissues the page once (`client.rs:585-612`). Bounded by 64 pages × 10k entries and a single deadline (`client.rs:26-29,566-650`).
- Other list/read/prompt methods: single-shot, no retry, no implicit reconnect (`list_resources`, `list_resource_templates`, `read_resource`, `list_prompts`, `get_prompt`). Retry only happens later via `acquire_transport` if a prior failure marked the epoch.
- **Gap:** stdio and legacy SSE `tools/call` losses that are accepted-but-response-lost surface as `McpError::Transport` (stdio pending drain `stdio.rs:257-266`; SSE reader death `sse.rs:578-590`), not `UnknownOutcome`. Runtime maps `Transport` to generic `"execution failed"` (`runtime.rs:5386-5420` fallback), so the model is not told the mutation may have run. Only the rmcp adapter performs that reclassification.
- `is_retriable_connection_error` treats `Closed`, specific `Http` 404/502/503, and substring-matched transport/timeout/tool errors as retryable (`client.rs:27-64`); `is_unverified_delivery` excludes `Closed` by design (`:67-75`).

### B4. `close()` vs transport disconnect

- Explicit `close()`: unregisters `mcp://` session, bounded wait on `close_barrier`, sets `McpConnection.closed=true` **before** transport teardown, then bounded transport close (`client.rs:963-1005`). Every request path checks `closed` before dispatch (`:382-383,396-398,472-474,510-513`), so post-close calls return `Closed` and are definite "not executed". **Terminal:** no reconnect after explicit close — `acquire_transport`/`reconnect_locked`/`reconnect_until` all return `Closed`. Errata Batch1 item 4 confirmed and sharpened.
- Idempotence: `close_completed` short-circuits a repeat (`:978-981`). A failed close leaves `closed=true`, `close_completed=false`; retry is possible but the rmcp service may already be taken, so the retry can return `Ok` after a failed shutdown (`rmcp_adapter.rs:376-396`). [INFERENCE from code paths]
- Transport-originated disconnect: connection stays open; failure is recorded per transport epoch (`failed_epoch`, `client.rs:104-121,376-405`), and the next request reconnects exactly once under the state barrier (`:471-544`). Stdio reader exit marks only the transport's own `closed` and drains pending with `Transport` (`stdio.rs:257-266`); the client still reconnects because `Closed`/`Transport` is a transport failure (`client.rs:60-86`). Legacy SSE clears `endpoint_url` and drains pending with `Transport`; `request`/`notify` reconnect inline (`sse.rs:355-363,630-665`).
- rmcp `close()` sets its own `closed` flag, takes the service, and uses `close_with_timeout(8s)`; timeout maps to `Transport` (`rmcp_adapter.rs:376-396`). In-flight `tools/call` that then sees a Transport error is reclassified `UnknownOutcome`; other in-flight calls keep `Transport` (`:340-360`).

### B5. Transport limits / redirects

- **stdio:** isolated env (`env_clear` + allowlist + explicit config, `stdio.rs:60-88`); inbound NDJSON line cap 16 MiB — on overflow the reader exits (`break 'outer`), marks closed, drains pending as Transport (`:150-183,257-266`). Outbound requests have no size cap; stdin queue bound 128 (`:130`). Errata M17 confirmed: oversized frame closes/drops the connection.
- **legacy SSE:** `MAX_MCP_FRAME_SIZE = 16 MiB` for connect buffer, event data, request buffer, and error bodies (`sse.rs:18,399-402,430-433,490-492,566-568,658,726`); endpoint origin must equal configured origin (`:460-470`); manual redirects max 5 because reqwest policy is `none` (`:189,297`); optional `origin-locked` policy forbids cross-origin redirects (`:209-212,313-317`); cross-origin hops strip `authorization`/`mcp-session-id` and all configured headers (`:126-133,296-303`); POST rejects 301/302/303 only under origin-locked, otherwise correctly re-POSTs (`:310-318`).
- **rmcp HTTP:** redirects refused (`reqwest::redirect::Policy::none()`, `rmcp_adapter.rs:193`) — any 3xx becomes an HTTP-status transport error, no following; SSE event cap 16 MiB (`:205`, `:455-470`); `reinit_on_expired_session(false)` (`:206`); `pool_max_idle_per_host(0)` (`:192`); transport errors are converted to category-only messages/status so URLs and bodies containing credentials do not leak (`:690-760`).
- **SSE connect race:** `connect()` checks `endpoint_url` and then awaits without holding a lock (`sse.rs:355-363`); concurrent first requests can each open an SSE stream, and the later `sse_handle` overwrite does not abort the earlier task (`:589-592`). [INFERENCE: duplicate live stream until inner drop]

### B6. Other MCP observations

- Server capability struct models only `tools/resources/prompts/logging` (`types.rs:262-273`); `completions` is deliberately erased (`rmcp_adapter.rs:257`), while sampling/elicitation capability objects are ignored.
- `McpConnection` close state is checked before dispatch but not held across the request, so an explicit close racing an in-flight call is handled per-transport (rmcp reclassifies; stdio drains pending as Transport; SSE aborts reader and drains as Transport).
- `tools/list` pagination caps shared across list methods (`MAX_LIST_PAGES=64`, `MAX_LIST_ENTRIES=10_000`, `client.rs:26-29`); resource/template/prompt lists have the same caps but no replay.
- `notify` is generic; the client only sends `notifications/initialized`. Inbound notifications (list-changed, resource-updated, logging) are ignored/no-op at the client layer.
- Discovery/list failure handling: `tools/list` failure closes the connection and records a safe server error; resource-only servers with no `tools` capability are still registered for `mcp://` (`runtime.rs:6400-6450`).

---

## Consolidated verdicts

| Claim | Status |
|---|---|
`resources/subscribe` unsupported | Confirmed — absent in client and rmcp dispatch; only raw stdio/SSE passthrough
`prompts/list` + `prompts/get` | Implemented (corrects M16)
Sampling / elicitation | Not implemented, not advertised; rmcp defaults method-not-found/decline
Roots | Implemented and advertised by all transports
`structuredContent` | Dropped entirely (field absent from result type); `outputSchema` dropped from tool defs
`_meta` | Parsed only on tool results, then discarded by runtime; absent elsewhere
`tools/call` retry | Never retried; mutating rmcp failures → `UnknownOutcome`; stdio/SSE Transport loss not reclassified in runtime → replay-risk wording gap
`list_tools` retry | One replay per page after reconnect; other list methods no retry
`close()` vs disconnect | Explicit close terminal and pre-dispatch `Closed`; transport disconnect reconnects at next request; errata item 4 correct
Transport limits | stdio 16 MiB inbound line + drop; SSE 16 MiB frame/event; rmcp 16 MiB SSE event
Redirects | rmcp refuses all; legacy SSE manual max 5, strips auth cross-origin, optional origin lock, endpoint origin check
Additional | SSE connect race; failed rmcp close can later report success; stdio/SSE `Transport` error wording loses "may have executed" guarantee
