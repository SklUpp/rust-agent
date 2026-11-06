# Блок C. OpenAI-compatible provider + MCP: evidence

Статус: read-only source research. Код, конфиги, зависимости не менялись. Pins: `original 035e4f95`, `fork 3008cfd`. Line anchors — локальный checkout; при drift номера строк могут смещаться.

## 1. Покрытые требования

| Требование | Статус | Факт |
|---|---|---|
| D30: первый провайдер — OpenAI-compatible; `base_url` настраивается; ключ из env; Anthropic/OAuth не в первом адаптере | принято | Оба донора содержат рабочий OpenAI-compatible слой. Имя env-переменной, endpoint и reasoning defaults не выбирались — отложены. |
| Streaming + tool calls + images + errors + replay | принято как поведение | Fork реализует streaming/tools/images/errors частично; original TS содержит более полный parity-слой. Разрывы — §3. |
| D54: полный MCP-клиент; проверки безопасности до исполнения; stdio spawn сам является исполнением и проверяется до запуска | принято направление, matrix открыта | D54 требует **security check before execution**, не обязательно интерактивный approval. Конкретный механизм (policy, approval UI, allowlist) не зафиксирован. Версия протокола и capabilities отложены. |
| D27: ключ не наследуется shell-средой | принято | Env credentials не должны массово наследоваться дочерними процессами; ключ резолвится в trusted composition. |
| D34: ошибки storage не маскируются | вне блока | Provider/MCP ошибки возвращаются наверх; слепой retry может повторить side effect. |
| `mcp://` форма URI | открыто; D79 **не** утверждает схему | D79 — про завершение субагента, не про MCP. Original auto-target по resource URI; fork требует server prefix. Это несовместимые product contracts, выбор не сделан. |

## 2. Donors, pins, licenses

| Donor | Pin / version | License | Ключевые source symbols |
|---|---|---|---|
| Original OMP | `035e4f95` | MIT | `packages/ai/src/providers/openai-completions.ts`; `packages/ai/src/utils/openai-http.ts`; `packages/ai/src/providers/openai-shared.ts`; `packages/coding-agent/src/mcp/*`; `packages/agent/src/replay-policy.ts` |
| Fork OMP | `3008cfd` | MIT | `crates/pi-agent/src/provider.rs`; `crates/pi-agent/src/mcp/*`; `crates/omp-cli/src/runtime.rs` (`McpTool`); `crates/omp-cli/src/config.rs`; `crates/pi-agent/src/tools/internal_urls.rs` |
| Official Rust SDK | `rmcp 3.9.0`, tag `rmcp-v3.9.0` | Apache-2.0 | `ClientHandler`, `ClientServiceExt`, `ClientLifecycleMode`, `Peer`, `ClientConfig`, `ClientCapabilities`, `TokioChildProcess`, `StreamableHttpClientTransport` |
| Rig | `rig-core 0.46.0`, `rig-rmcp 0.46.0` | MIT | `rig::agent::Agent`; `rig_rmcp::McpClientHandler`; MCP tools as dynamic tools, resources as context |
| Project runtime | local Cargo workspace | MIT | `reqwest 0.12` only `rustls-tls` (`Cargo.toml:465-471`) |

Licenses/versions pinned at:
`https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/rmcp-v3.9.0/Cargo.toml` (workspace `license = "Apache-2.0"`, `rust-version = "1.88"`, `edition = "2024"`), `https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/rmcp-v3.9.0/crates/rmcp/Cargo.toml`, `https://docs.rs/crate/rig-rmcp/0.46.0/source/Cargo.toml.orig`, `https://crates.io/api/v1/crates/rig-core/0.46.0`.

## 3. Provider adapter

### 3.1 Env-key / base URL boundary

Original:
- `getEnvApiKey(provider)` — catalog `envVars` + computed resolver; checks `Bun.env`, cwd `.env`, `~/.env` (`packages/ai/src/stream.ts:837-860`).
- `resolveOpenAIRequestSetup` throws `MissingApiKeyError` if no key; fallback `OPENAI_API_KEY` (`openai-shared.ts:220-227`).
- `baseUrl` from model/provider; TS donor default is `https://api.openai.com/v1` (`openai-completions.ts:1464-1493`). Это donor fact, не выбранный default.

Fork:
- `ProviderConfig::resolve_api_key` (`crates/omp-cli/src/config.rs:46-52`): `apiKey` is an env variable name if set, otherwise a literal. This is the env-key adapter.
- `wire_base_url` (`runtime.rs:2736-2760`): `openai-completions` requires configured `baseUrl`; no endpoint fallback.
- `wire_provider` (`runtime.rs:3050-3100`): `OpenAiCompatibleProvider::new(base_url, api_key, model, credential_callback, ...)`.
- `CredentialCallback`/OAuth-refresh exists but D30 does not select it; first adapter can pass `None`.
- Constructor takes `api_key: String` and does not validate emptiness; original throws. This difference is an open decision.

Not selected: env variable name, endpoint, `reasoning_effort`, auth header/refresh policy.

### 3.2 Contract и streaming

Fork (`crates/pi-agent/src/provider.rs`):
- `trait ModelProvider` (`:232`): `complete`, `stream`, `api`, `provider`, `model`, `supports_images`, `check_support`.
- `StreamChunk`, `ToolCallDelta`, `StreamAssembler`; `ChunkStream = Pin<Box<dyn Stream<Item = Result<StreamChunk, ProviderError>> + Send>>`.
- `OpenAiChatRequest` (`:37-61`): `model`, `messages`, `tools`, `tool_choice`, `stream`, `stream_options.include_usage`, `reasoning_effort`, `prompt_cache_key`, `service_tier`.
- `OpenAiCompatibleProvider::stream` (`:327`): URL `base_url.trim_end_matches('/') + "/chat/completions"`, `Authorization: Bearer`, JSON body, connect timeout 30 s.
- `sse_stream` (`:1618-1860`): line-oriented SSE, `data:` payloads only, `[DONE]`, in-band `{"error":...}` -> `ProviderError::Protocol`; redacts API key from protocol error text. EOF without terminal emits `done` only if a stop reason was already seen.
- `StreamAssembler` (`:1085-1360`): `content_index` authoritative; text/reasoning/redacted/signature deltas; tool-call fragments by index; usage last-snapshot; `finish` rejects a tool call without name; `finish_partial` preserves text on error/abort.
- Loop turns "stream ended before completion" into a run error (`loop.rs:1110-1135`), so truncation is not success.

Original:
- `streamOpenAICompletions` (`openai-completions.ts:1457-1461`) wraps `streamOpenAICompletionsOnce` in `withReplaySafeStreamRetry`.
- `postOpenAIStream` checks non-2xx, bounded body, code/status; `DEFAULT_MAX_ATTEMPTS = 6`, retries 408/429/5xx/network, respects `Retry-After` (`openai-http.ts:33-108`).
- Terminal validation: in-band error, missing `[DONE]`, incomplete blocks -> typed stream error (`openai-completions.ts:592-650`, `:1330-1395`).
- First-event timeout, idle timeout, empty-completion retry, strict-tools fallback, reasoning fallback.

Gap fork: no 429/5xx retry, no `Retry-After`, no first-event/idle watchdog, no strict-tools/reasoning fallback. Only one auth-refresh retry on `401/403 + invalid_key`.

### 3.3 Tools и images

Fork:
- `tools` pass through; no strict-schema normalization, no 40-char tool-id limit (`to_openai_messages`, `provider.rs:1366-1458`).
- Assistant content is always a string; `tool_calls` from `m.tool_calls`; `reasoning_content`/signature are **not** sent.
- User image: `image_url` data URI; MIME/base64 validation; `supports_images == false` -> `UnsupportedImage` (`provider.rs:292-320`).
- Tool-result images: separate user message after tool messages (`provider.rs:1420-1458`).
- `NormalizedUsage`: prompt/completion/cache read/write/reasoning (`provider.rs:1485-1510`).

Original:
- `convertMessages` (`openai-completions.ts:1886`): strict tool schemas, tool-id normalization (Mistral 9, OpenAI 40, duplicate suffix), placeholder for unsupported images, compat-driven `reasoning_content` replay, ordering guards.
- Images: user and tool-result (`:1999-2045`, `:2339-2360`).
- Tool choice: forced tool validation; compat fallback `auto`/`required` (`:1717-1756`).

Gap fork: hard error instead of placeholder; no tool-id normalization; no strict schema; no reasoning replay.

### 3.4 Errors / cancel / timeout

Fork `ProviderError` (`provider.rs:63-120`):
`Network`, `HttpStatus(u16, String)` (body snippet up to 2048), `AuthRefresh`, `Serialization`, `Protocol`, `InvalidBaseUrl`, `UnsupportedImage`, `InvalidToolChoice`.

- HTTP non-2xx: body snippet in error; headers/`Retry-After` not retained.
- Auth: only `invalid_key` + refresh callback; otherwise immediate error.
- Cancellation: `tokio::select!`/drop; no idle timeout; connect timeout 30 s.
- Secret: stream protocol errors redacted; HTTP body snippet not guaranteed if server echoes headers.

Original:
- `OpenAIHttpError` stores status, headers, code, bounded body; retry classifies 408/429/5xx/network (`openai-http.ts`).
- Distinguishes in-band error, incomplete stream, empty completion; `withReplaySafeStreamRetry` does not repeat confirmed tool calls.
- Tool loops get terminal error, not empty success.

### 3.5 Replay

Fork:
- State keeps `ContentPart::Thinking { signature }` and `RedactedThinking`; assembler preserves them.
- `provider_signed_replay.rs` checks thinking/signature/tool-boundary survive two turns in state.
- `to_openai_messages` **drops** reasoning/thinking for OpenAI wire (`provider.rs:1380-1400`). Signed replay exists only in the Anthropic adapter.
- `filterProviderReplayMessages` from original is absent: terminal refusal/error messages are not filtered before replay.

Original:
- `convertMessages` conditionally returns `reasoning_content`/signature per compat (`openai-completions.ts:2075-2137`).
- `replay-policy.ts:4-12` removes provider-refusal messages from replayed context.

Consequence: reasoning models lose multi-turn reasoning continuity/cache locality in fork. Concrete gap.

## 4. MCP

### 4.1 Capability matrix

| Capability | Original TS | Fork Rust | `rmcp 3.9` |
|---|---|---|---|
| Protocol | `2025-11-25` (`types.ts:166`) | `2025-11-25` (`types.rs:6`) | `2026-07-28` modern (no initialize) + legacy `2025-11-25` via `Initialize` |
| Lifecycle | `initialize` | `initialize` | `Initialize` / `Discover` / `Auto`; `ServiceExt::serve` remains legacy init |
| tools/list + tools/call | да | да | да |
| tool content text/image/resource | да (audio нет) | да, image/resource flattened to base64 text | да, full union |
| `structuredContent` / `_meta` | `_meta` kept, no `structuredContent` | `_meta` dropped (`details: None`) | да |
| resources list/templates/read | да | да | да |
| resources subscribe/unsubscribe | да | нет | legacy `subscribe`; 2026 `listen(SubscriptionFilter)` / `subscriptions/listen` |
| prompts list/get | да | **нет** | да |
| roots | declared + `roots/list` | answers `roots/list` cwd, capability not declared | support; deprecated in 2026; 2026 via MRTR `roots/list` |
| sampling | нет (type stub only) | нет | support; deprecated in 2026; 2026 via MRTR `sampling/createMessage` |
| elicitation | нет | нет | support (form/url); 2026 via MRTR `elicitation/create` |
| logging | capability, no handler | capability, no handler | `set_level` legacy; 2026 removes logging/setLevel |
| completion/complete | нет | нет | да |
| tasks/skills extensions | нет | нет | да |
| OAuth | credential storage/refresh + discovery | config reject (`mcp/config.rs:16-30`) | feature `auth`; DCR deprecated in 2026 but available |
| stdio | да | да | `TokioChildProcess` |
| Streamable HTTP | да | да | `StreamableHttpClientTransport` |
| legacy HTTP+SSE | да (`transports/sse.ts`) | да | no standard legacy transport; spec deprecated 2025-03-26 |
| notifications/list_changed | да | no handlers | да (`ClientHandler::on_*_list_changed`) |
| reconnect | crash-burst limits | list/read reconnect; tool call never replayed when unverified | `RunningService` lifetime |
| tool name cap | 64 | 64 | product concern |

### 4.2 Original TS: «full» scope

- `MCP_PROTOCOL_VERSION = "2025-11-25"`; initialize declares **only roots** (`client.ts:100-103`). `MCPClientCapabilities.sampling` exists in the type (`types.ts:187-190`), but is not declared or served.
- `defaultRequestHandler` answers `ping` and `roots/list`; no sampling/elicitation methods (`client.ts:55-65`).
- Tools: `listTools` (`:209`), `callTool` (`:246`); text/image/resource (`tool-bridge.ts:193-236`); `isError`; `_meta` in `details`; donor approval tier `write` (`:490`); name cap 64 (`:362-381`). Donor behavior, not D54 mandate.
- Resources: list/templates/read/subscribe/unsubscribe (`client.ts:281-420`); `resources/updated` / `list_changed` (`manager.ts:780-797`); `mcp://<resource-uri>` auto-target (`internal-urls/mcp-protocol.ts:112-140`).
- Prompts: list/get (`client.ts:458-510`); exposed as session commands (`session/index.ts:4130-4131`).
- OAuth credential storage/refresh and discovery: `mcp/oauth-credentials.ts`, `mcp/oauth-flow.ts`, `mcp/oauth-discovery.ts`; legacy SSE: `transports/sse.ts`.
- Retry hazard: `MCPTool.execute` reconnects and retries `callTool` on classified connection error (`tool-bridge.ts:563-570`, `:691-698`). For a mutating tool this can replay. HTTP transport has a no-replay guard only for accepted SSE-resume (`transports/http.ts:30-40`), not all errors.

So original «full» = tools + resources + prompts + roots + notifications + OAuth. Sampling/elicitation absent.

### 4.3 Fork Rust: present / absent

- `connect_to_server` (`mcp/client.rs:143`), initialize with `capabilities: Default::default()` (`:114-115`) — roots not declared.
- `list_tools` (`:194`), `list_resources` (`:255`), `list_resource_templates` (`:329`), `read_resource` (`:412`), `call_tool` (`:428`).
- `McpTransport` request/notify/close; stdio, streamable HTTP, legacy SSE (`mcp/transports/mod.rs`).
- stdio answers `roots/list` with cwd (`transports/stdio.rs:179-190`), but server is not told roots are supported.
- `tools/call` timeout 30 s / `OMP_MCP_TIMEOUT_MS`; timeout/abort sends `notifications/cancelled` (`mcp/timeout.rs`, transports).
- `McpTool` (`runtime.rs:5310-5400`): name `mcp__<server>_<tool>`, cap 64; `execute` -> `call_tool`; text/image/resource flatten:
  - image -> `"[Image: <mime>; base64=<data>]"`;
  - resource blob -> base64 text;
  - `_meta`/structured content dropped; `details: None`.
- `mcp://` server-scoped: `mcp://`, `mcp://<server>`, `mcp://<server>/<uri>` (`internal_urls.rs:460-466`); immutable read.
- OAuth config rejected (`mcp/config.rs:16-30`); auth headers static.
- Notifications: stdio ignores methods without id; list_changed/resources_updated not handled. No subscriptions.
- No prompts, completion, sampling, elicitation, logging handler.

Fork is a minimal, working tools/resources client. It is not D54 «full»; extending it is cheaper in dependencies but more protocol work.

### 4.4 `rmcp 3.9` — pinned API

```toml
rmcp = { version = "=3.9.0", default-features = false, features = [
  "client",
  "transport-child-process",
  # + "transport-streamable-http-client-reqwest", "reqwest" (rustls)
  # + "auth", "elicitation" only if product enables them
] }
```
- `rmcp 3.9.0`: `rust-version = "1.88"`, edition 2024, Apache-2.0. Default features (6): `base64`, `macros`, `server`, `schemars`, `transport-async-rw`, `uuid`; client-only build should disable them.
- Deps: `reqwest 0.13.2` (only via HTTP feature; `reqwest` feature selects rustls), `process-wrap ^10.0` (child process), `tokio`, `futures`, `serde`, `thiserror`, `schemars` optional. OpenSSL not required with `reqwest`; native-tls is a separate feature.
- Feature `client-side-sse` exists, but the standard transport list exposes only stdio and Streamable HTTP; rmcp 3.9 has no legacy HTTP+SSE client transport. The spec marks legacy HTTP+SSE deprecated. Sources: `https://docs.rs/rmcp/3.9.0/rmcp/transport/index.html`, `https://modelcontextprotocol.io/specification/2026-07-28/deprecated.md`.

API (verified):
- `ClientCapabilities::builder()`; `enable_elicitation()`, `enable_elicitation_with()`, `enable_tasks()`; `enable_roots()` / `enable_sampling()` exist but are deprecated since 1.8.0 by SEP-2577.
- `ClientConfig` is an alias for `InitializeRequestParams`:
  ```rust
  let config = ClientConfig::new(
      ClientCapabilities::builder().enable_elicitation().build(),
      Implementation::new("client", "1.0.0"),
  ).with_protocol_version(/* chosen ProtocolVersion */);
  ```
  `ProtocolVersion::has_initialize()` distinguishes legacy revisions; `InitializeRequestParams::with_protocol_version` sets it.
- `ClientHandler`: `ping`, `create_message`, `list_roots`, `create_elicitation`, `on_custom_request`, cancellation/progress/logging/resource/tool/prompt notifications, `get_info() -> ClientConfig`.
- `ServiceExt::serve` remains legacy initialization; `ClientServiceExt::serve_with_lifecycle(transport, ClientLifecycleMode)` selects `Initialize`, `Discover`, or `Auto`.
- `RunningService::peer()` -> `Peer`; `Peer::list_tools`, `call_tool`, `call_tool_once`, `list_resources`, `read_resource`, `list_prompts`, `get_prompt`, `complete`, `subscribe`, `unsubscribe`, `listen`, `set_level`, `create_message`, `list_roots`, `create_elicitation`.
- MRTR: `Peer::call_tool_once` returns `CallToolResponse::{Complete, InputRequired}`; `InputRequiredResult` carries `inputRequests` / `inputResponses`; typed elicitation helpers exist.
- 2026: `Peer::create_message`, `list_roots`, `create_elicitation` return an error on protocol `2026-07-28+` because server-initiated requests are forbidden; use MRTR. `listen(SubscriptionFilter)` replaces `subscribe`; `server/discover` replaces initialize; logging removed. Source: `https://docs.rs/rmcp/3.9.0/rmcp/struct.Peer.html`, `https://modelcontextprotocol.io/specification/2026-07-28/changelog.md`.
- `Auto` probes with `server/discover` and falls back to legacy `initialize`; for OMP-style `2025-11-25` servers use `Initialize` and the legacy protocol revision.

Strengths: prompts, completions, structured content, MRTR, OAuth, tasks, full protocol. Costs: second `reqwest` major, protocol-era branching, no legacy HTTP+SSE transport, Bionic build untested.

### 4.5 Границы roots/resources/prompts/elicitation/sampling

**Facts**

- Roots are **advertised scope**, not an enforced filesystem allowlist or sandbox. Spec: “informational guidance rather than an access-control mechanism. The protocol does not enforce that servers stay within roots.” Client-side URI/access validation is required, server cooperation is expected, but the protocol does not sandbox. Source: `https://modelcontextprotocol.io/specification/2026-07-28/client/roots.md`.
- Original and fork expose cwd as a root; fork does not declare roots support. Neither is a security boundary for a hostile server.
- Resources are untrusted read-only content: arbitrary URI/MIME/size/blob. Original converts unsupported binary to placeholders; fork flattens image/blob to base64 text. `mcp://` URI shape is a product contract, not a D-number.
- Prompts are user-invoked commands, not system instructions. Original maps them to commands; fork lacks them.
- Sampling/elicitation are absent in original and fork. `rmcp` supports both; in `2026-07-28` they are reachable only through MRTR, and roots/sampling/logging are deprecated. Sampling spec recommends human-in-the-loop review (SHOULD); elicitation spec requires clear server identity, decline/cancel UI, and URL-mode consent (MUST).
- `rmcp` 2026 `Peer::create_message`/`list_roots`/`create_elicitation` error; this is an API/spec fact, not a product decision.

**Proposals — not accepted requirements**

- Roots: expose only project root/explicit mounts, never whole home. This is a design proposal; D54 does not specify it, and roots do not enforce it.
- Sampling: default off, allowlist models, cap budget/context. Proposal; spec only recommends human-in-the-loop.
- Elicitation: explicit user confirm, schema validation, URL host display. Proposal; spec mandates review/decline UI, not a particular confirm flow.
- MCP tools: interactive approval by default. D54 requires security checks before execution; approval UI is one implementation, not mandated. Donor OMP uses `approval = "write"`.
- `mcp://` URL shape: choose original auto-target vs fork server-scoped. Open.
- Retry: never replay a mutating MCP tool after unverified delivery. Fork behavior; proposal for product, not a D54 requirement.
- OAuth/sampling/elicitation enablement: deferred.

**Security facts**

- stdio MCP launches an arbitrary process; D54 says spawn itself is execution and must be checked before launch. D20 trust/policy still applies.
- Tool results/resources/prompts are untrusted data, never instructions.
- D27: env credentials must not be inherited en masse by child processes; do not pass the provider key into MCP child env.
- Credential store is outside history and not exported (D55); OAuth tokens follow that boundary. D30 does not include OAuth.

## 5. Межблочные зависимости

- Provider contract (`ModelProvider`, `StreamChunk`, usage) feeds loop/compaction/persistence. Block C does not change its shape; parity stays inside the adapter.
- MCP tools enter registry/scheduler; security classification and approval mechanics are external to this block, but `McpTool` must honor them.
- MCP resources feed the internal URL router. URI shape is an open contract; D79 does not define it.
- MCP prompts feed the command surface; fork's absence is both MCP and UX gap.
- Session owner owns connections/teardown; loop must not own MCP lifecycle.
- Env-key resolution is trusted composition, not shell or child env.

## 6. Что ещё не доказано

1. No build/test of `rmcp 3.9` on Termux/Bionic; duplicate `reqwest 0.13`, `process-wrap`/`libc` behavior unmeasured.
2. No runtime interop with real MCP servers: OMP-style `2025-11-25`, legacy SSE, prompts, roots, notifications.
3. Not selected: `2025-11-25` vs `2026-07-28`; legacy SSE; OAuth; sampling/elicitation; roots policy.
4. Not selected: provider retry/idle/first-event/timeout policy; missing-key behavior; env name; endpoint; reasoning defaults — deferred.
5. No parity check against target gateways: tool-id limits, object-shaped arguments, strict schemas, reasoning replay, image placeholders.
6. Rig: no runtime test. `rig-rmcp 0.46.0` requires `rmcp ^3.5` (not pinned to 3.9), and its advertised scope is tools + resources as context + tool-list sync. Evaluation — no unique gap found; not a capability proof.
7. Original `callTool` replay hazard is inferred from code, not reproduced by a test.

## 7. Anchors

Provider fork:
- `crates/pi-agent/src/provider.rs:37-61` request struct
- `:63-120` error enum
- `:232` `ModelProvider`
- `:327` `OpenAiCompatibleProvider::stream`
- `:1085-1360` assembler
- `:1366-1458` message conversion / tool images
- `:1485-1510` usage
- `:1618-1860` SSE
- `crates/pi-agent/src/loop.rs:1110-1135` incomplete stream -> error
- `crates/pi-agent/tests/provider_signed_replay.rs` signed state replay

Provider original:
- `packages/ai/src/stream.ts:837-860` env key
- `packages/ai/src/providers/openai-shared.ts:220-227` `OPENAI_API_KEY`
- `packages/ai/src/providers/openai-completions.ts:592-650` stream errors
- `:1330-1395` terminal validation
- `:1457-1461` retry wrapper
- `:1464-1493` request setup
- `:1717-1756` tool choice compat
- `:1886-2360` convert messages / images / reasoning replay
- `packages/ai/src/utils/openai-http.ts:33-108` HTTP error + 6 attempts
- `packages/agent/src/replay-policy.ts:4-12` refusal filter

MCP original:
- `packages/coding-agent/src/mcp/types.ts:166` protocol version
- `:187-198` client capabilities type
- `packages/coding-agent/src/mcp/client.ts:55-65` ping/roots handler
- `:100-103` initialize capabilities
- `:209`, `:246`, `:281-420`, `:458-510` methods
- `packages/coding-agent/src/mcp/tool-bridge.ts:50-110` retriable errors
- `:193-236` content conversion
- `:362-381` name cap
- `:490` donor approval `write`
- `:563-570`, `:691-698` tool retry
- `packages/coding-agent/src/mcp/manager.ts:420-424`, `:780-797` subscriptions/notifications
- `packages/coding-agent/src/internal-urls/mcp-protocol.ts:112-140` auto-target `mcp://`
- `packages/coding-agent/src/session/index.ts:4130-4131` prompts as commands
- `packages/coding-agent/src/mcp/oauth-*.ts`, `transports/sse.ts` OAuth/legacy SSE

MCP fork:
- `crates/pi-agent/src/mcp/types.rs:6` protocol version
- `:228-240` client capabilities
- `crates/pi-agent/src/mcp/client.rs:114-115` initialize capabilities default
- `:143` connect
- `:194`, `:255`, `:329`, `:412`, `:428` methods
- `crates/pi-agent/src/mcp/transports/stdio.rs:179-190` roots/list cwd
- `crates/pi-agent/src/mcp/config.rs:16-30` OAuth reject
- `crates/pi-agent/src/mcp/timeout.rs` 30 s / `OMP_MCP_TIMEOUT_MS`
- `crates/omp-cli/src/runtime.rs:5310-5455` `McpTool` + content flatten
- `crates/pi-agent/src/tools/internal_urls.rs:460-570` server-scoped `mcp://`
- `crates/omp-cli/src/config.rs:46-52` env-or-literal key
- `crates/omp-cli/src/runtime.rs:2736-2760` required base URL
- `crates/omp-cli/src/runtime.rs:3050-3100` provider construction

Official SDK / spec:
- `https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/rmcp-v3.9.0/Cargo.toml`
- `https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/rmcp-v3.9.0/crates/rmcp/Cargo.toml`
- `https://docs.rs/crate/rmcp/3.9.0/features`
- `https://docs.rs/rmcp/3.9.0/rmcp/trait.ClientHandler.html`
- `https://docs.rs/rmcp/3.9.0/rmcp/model/struct.ClientCapabilitiesBuilder.html`
- `https://docs.rs/rmcp/3.9.0/rmcp/model/struct.ClientConfig.html`
- `https://docs.rs/rmcp/3.9.0/rmcp/model/struct.InitializeRequestParams.html`
- `https://docs.rs/rmcp/3.9.0/rmcp/enum.ClientLifecycleMode.html`
- `https://docs.rs/rmcp/3.9.0/rmcp/struct.Peer.html`
- `https://docs.rs/rmcp/3.9.0/rmcp/transport/index.html`
- `https://modelcontextprotocol.io/specification/2026-07-28/changelog.md`
- `https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning.md`
- `https://modelcontextprotocol.io/specification/2026-07-28/basic/transports.md`
- `https://modelcontextprotocol.io/specification/2026-07-28/client/roots.md`
- `https://modelcontextprotocol.io/specification/2026-07-28/client/sampling.md`
- `https://modelcontextprotocol.io/specification/2026-07-28/client/elicitation.md`
- `https://modelcontextprotocol.io/specification/2026-07-28/deprecated.md`

Rig:
- `https://docs.rs/crate/rig-rmcp/0.46.0/source/Cargo.toml.orig`
- `https://crates.io/api/v1/crates/rig-core/0.46.0`
- `https://crates.io/api/v1/crates/rig-rmcp/0.46.0/dependencies`
