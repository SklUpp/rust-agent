# Унифицированный аудит — Домен 3: Providers, `9Router`, MCP, Auth, Redaction, Trust & Security (`ScoutUnifiedProvidersSec`)

Дата: 2026-11-27  
Источник: сырой отчёт `ScoutUnifiedProvidersSec` (только чтение документации и исходного кода).

---

## Final report — Domain 3 documentation + live-code cross-check

No files changed; no cargo/build/test commands run.

### A. Documented behavior confirmed in live code

**Provider**
- OpenAI-compatible path only for default flow; dialect dispatch and required base URL live (`runtime.rs:3022-3044`, `3356+`).
- Timeout defaults exact: 300000 ms, 2 retries, `0` disables (`config.rs:946-978`). Stream retry only before first chunk, never merges streams (`provider/timeout.rs:133-225`).
- HTTP status checked before body/SSE parse; auth refresh only `401/403 + invalid_key`, once (`provider/http.rs:280-310`).
- Explicit `UnsupportedImage` instead of silent attachment drop (`provider.rs:237-252`).
- Assembler by index, rejects empty tool name, partial turn omits tool calls (`provider.rs:258-613`).
- Malformed tool arguments preserved as string (`provider.rs:1185-1197`).
- Workspace `reqwest 0.13`, vendored `rmcp 3.9.0` confirmed.

**MCP**
- rmcp HTTP refuses all redirects; 16 MiB SSE cap; session auto-reinit disabled (`rmcp_adapter.rs:193-207`).
- 64 KiB error body / 16 MiB JSON cap in vendored rmcp.
- stdio env-isolated; 16 MiB line cap closes connection (`stdio.rs:78,150-185`).
- String `requestIdFormat` rejected before connect (`mcp/config.rs:8,16`; `client.rs:266`).
- Legacy SSE separate; Drop guard drains pending `Closed`.
- `tools/call` no retry; timeout/post-close transport failure → `UnknownOutcome`; `close()` terminal; next explicit call reconnects without replay (`client.rs:376-560,917-935`).
- Prompts implemented; prompt get single-shot; roots-only client capability.
- MCP error bodies/transport text not echoed (`rmcp_adapter.rs:660-711`; tests present).
- MCP launch gated before spawn; project transport needs approval (`runtime.rs:6059-6140`).

**Trust / secrets / security**
- `trust.json` 0700/0600, canonical path + SHA-256, fail-closed on drift.
- `TRACKED_SURFACES` now covers hooks, MCP, SSH and LSP files (`trust.rs:42-80`) — **M0 D20 gaps closed**, earlier docs stale.
- Project LSP ignored unless trusted; LSP/SSH `env_clear`; SSH drops `SSH_AUTH_SOCK`; ACP read uses `map_read_path`.
- Known-secret matcher: leftmost-longest, raw/percent/base64 variants, monotonic, min 8.
- Redaction applied to intent, outcome, delivery, session, worker completion, artifact metadata.
- `doctor --redact-legacy` needs backup + confirmation; not automatic.
- Security scan runtime opt-in default false; five local actions; cloud actions explicit paused error.

### B. Drift / partial gaps

1. **`direct-providers` feature absent** — Anthropic/Ollama modules unconditional, contrary to D30/build plan.
2. **Literal API-key fallback live** — missing env-shaped `apiKey` becomes the literal key (`config.rs:66-80`), conflicting with contract prohibition.
3. **D27 separate credential dir absent** — credentials in shared `<agent_dir>/agent.db` (0600); SEC-TUI host-side injection missing.
4. **`known_secrets.rs` header stale** — says executions/deliveries/artifacts excluded; code redacts them.
5. **MCP launch gate lacks executable/args/cwd allowlists and budgets** — only server allow/yolo/prompt + env isolation.
6. **MCP tool results flattened to strings** — images/audio/resources become base64 text (`runtime.rs:5715-5755`), contrary to preservation contract.
7. **Redirect ban transport-specific** — rmcp refuses all; legacy SSE follows up to 5, stripping auth only cross-origin.
8. **OpenAI HTTP bodies unbounded** — `response.text()` reads whole body before snippet (`provider/http.rs:296-310,339,371`); Anthropic/Ollama cap at 64 KiB.
9. **OpenAI reasoning replay still dropped** (`provider.rs:1112-1170`); signed replay Anthropic-only.
10. **LSP default mismatch** — live default `true` (`config.rs:358-362`); docs say disabled by default.
11. **No configurable `security.redact_heuristics`** — heuristics always on; docs describe a toggle/exceptions.
12. **`security-scan` is runtime opt-in, not a Cargo feature.**
13. **`list_tools` retries one unverified delivery** — safe/idempotent, but an exception to blanket “no retry after `UnknownOutcome`”; `tools/call` correctly does not retry.
14. **`AcceptedStreamError` remains but unused.**
15. **9Router lifecycle absent** — only manual OpenAI-compatible config/tests; no `GatewayBackend`, supervisor, ownership check, secret startup enforcement, DB snapshot.

### C. Pending / deferred with conditions

- **9Router Pattern A** recommended, not implemented: pin upstream, bind `127.0.0.1`, random secrets, memory cap, DB snapshot, no `patch-9router.mjs`, one retry owner, one OAuth refresh owner. Pattern B only if blocking fix absent upstream, <100 LoC source-only. Pattern C only after ownership/secret contracts.
- **9routerx:** defer until Bionic DNS/TLS verified; never share `data.sqlite`; identity-checked adoption only.
- **CLIProxyAPI:** optional external, retry disabled, loopback; Rust port observe only.
- **MCP OAuth:** explicitly rejected pending credential storage unless explicit `Authorization` header. Sampling/elicitation/subscriptions unimplemented.
- **MCP rich content:** needs typed result channel before preserving images/audio/resources.
- **SEC-TUI e13:** host-only credential injection + endpoint allowlist + Termux TUI reflow still pending.
- **M0:** trust/env gates done; remaining work is verification, not code.
- **Backfill:** exists with backup/confirmation and no automatic run.

### D. Open decisions / risks

- Final choice **Pattern A vs B vs C** unrecorded; gateway retry/OAuth ownership unenforced until chosen.
- **Literal key fallback**: compatibility vs fail-fast. Current behavior can turn a missing variable into a bogus bearer token.
- **LSP default**: change code or docs.
- **MCP redirect policy**: unify legacy SSE with rmcp or document exception.
- **MCP tool content model**: string flattening loses multimodal/resource fidelity.
- **Redaction configurability**: docs claim a toggle; code always-on. Safer to change docs.
- **OpenAI body cap** needed for memory/secret safety.
- **Verification risk:** no live Bionic/rmcp build, no post-MCP Termux smoke, no real 9Router supervisor/adoption or MCP OAuth run. All conclusions source-level.
