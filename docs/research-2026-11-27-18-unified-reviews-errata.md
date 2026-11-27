# Независимый аудит ревью унифицированных исследований (`ReviewUnifiedBatch1`, `ReviewUnifiedBatch2`, `ReviewUnifiedBatch3`)

Дата: 2026-11-27  
Источники: сырые вердикты независимых ревьюеров по батчам `-11-..-17-*.md` (только чтение кода и документации).

---

## 1. `ReviewUnifiedBatch1` — ревью Доменов 1–3 (`-11-`, `-12-`, `-13-`)

### Result

Read all three reports, then spot-checked critical claims against source and normative docs. Findings below concern report accuracy or concrete risks missed by the reports.

### Inaccurate or overstated claims

1. **D86 is not an accepted decision.** Domain 1 calls it operative and says it permits the VFS split (`docs/research-2026-11-27-11-unified-domain1-core-state.md:71,115`). The plan explicitly calls D84–D93 triage directions, not accepted journal decisions (`docs/agent-preliminary-implementation-plan.md:286-289`). The report itself acknowledges this distinction at line 101.

2. **Not every `write` uses staged version checks.** Domain 2 says every write goes through `PendingStore` (`docs/research-2026-11-27-12-unified-domain2-tools-exec.md:50`). Plan artifacts, internal URLs, and ACP writes take separate paths (`crates/pi-agent/src/tools/mod.rs:2601-2604,2620-2628,2647-2650`); ordinary workspace writes reach staging at `:2735-2778`.

3. **Anthropic and Ollama do not cap all response bodies at 64 KiB.** Domain 3 makes that unqualified claim (`docs/research-2026-11-27-13-unified-domain3-providers-sec.md:52`). Their error-body caps are 64 KiB; success bodies or records have 8 MiB caps (`crates/pi-agent/src/provider_anthropic.rs:55-58`, `crates/pi-agent/src/provider_ollama.rs:142-145`).

4. **MCP reconnect wording conflates transport closure with explicit `close()`.** Domain 3 says `close()` is terminal, then says the next explicit call reconnects (`docs/research-2026-11-27-13-unified-domain3-providers-sec.md:29`). Explicit `close()` sets the terminal flag (`crates/pi-agent/src/mcp/client.rs:963-975`); later acquisition returns `Closed` (`:383,397`). Reconnection applies to a transport-originated closure while the client itself remains open.

5. **Automatic retry needs narrower wording.** Domain 1 says no auto-retry and mentions RPC controls without describing their implementation (`docs/research-2026-11-27-11-unified-domain1-core-state.md:93,110`). RPC can automatically retry classified transient provider-stream failures, with replay-safety checks and backoff (`crates/aragent/src/rpc/mode.rs:1844-1849,1950-2020`). This is distinct from retrying an unknown-outcome tool action; it does not establish that R10 is implemented.

6. **Several source anchors drifted.** Domain 1’s `session_store.rs` ranges for draft clearing, intents, recovery and imports point partly to unrelated code. Current entry points include `crates/aragent/src/session_store.rs:4155,4658,6177,6565,2927`. Domain 2’s eval SSH-variable anchor should be `crates/pi-agent/src/eval/env.rs:60-64`, not `:44-56`.

### Missed concrete risks

1. **`hub start` bypasses `BashPolicy`.** The policy applies only to tool name `bash` (`crates/aragent/src/approve.rs:448-455`), while `hub start` accepts an executable and arguments and starts it directly (`crates/pi-agent/src/tools/hub.rs:955-987`). With `--yolo` or explicit `--allow hub`, approval is automatic (`crates/aragent/src/approve.rs:390-396`). A command forbidden through `bash` can therefore run through `hub`; domain 2’s generic policy-breadth warning does not identify this boundary.

2. **OpenAI-compatible SSE buffering is unbounded.** Domain 3 identifies unbounded non-stream response reads, but not streaming framing. `sse_stream` buffers until newline and appends without a size limit (`crates/pi-agent/src/provider.rs:1369,1427,1444`). An endpoint sending an indefinitely long line can exhaust memory.

3. **OpenAI HTTP error snippets can expose echoed credentials.** Error responses are passed to `snippet` without credential redaction (`crates/pi-agent/src/provider/http.rs:295-306`); `snippet` only truncates (`crates/pi-agent/src/provider.rs:1492-1494`), and `ProviderError` displays the resulting text (`:48-50`). An endpoint echoing the bearer token in its error body can put it in the returned diagnostic.

### Confirmed sample

Source checks support the reports’ claims about the durable task scheduler gate, eval SSH-agent environment exposure, missing `direct-providers` feature, literal `apiKey` fallback, default-enabled LSP, dropped OpenAI reasoning replay, and unbounded OpenAI non-stream body reads. D18 and D47 wording was checked against `docs/agent-architecture-decisions.md:105-110,261-263`.

**Limits:** source-level spot-check only. No files changed; no builds, tests, cargo commands, or live device checks run.

---

## 2. `ReviewUnifiedBatch2` — ревью Доменов 4–6 (`-14-`, `-15-`, `-16-`)

### Step 1

Read all three saved reports: domains 4, 5 and 6.

### Step 2 — corrections and missed risks

1. **Domain 4 incorrectly says SSH project config bypasses trust.** `docs/research-2026-11-27-14-unified-domain4-analysis-memory.md:214` groups `.omp/ssh.json` with ungated DAP config. SSH defaults to deny and checks trust on each resolution (`crates/pi-agent/src/tools/ssh.rs:169-204,288-289`); production installs `workspace_is_trusted` (`crates/aragent/src/main.rs:968-975`). DAP remains different: project and ancestor configs load without that gate (`crates/pi-agent/src/dap/config.rs:339-358,542-565`).

2. **Domain 4 miscounts RepoMap coverage.** `docs/research-2026-11-27-14-unified-domain4-analysis-memory.md:183` says 38 languages are unsupported. There are **57** `SupportLang` variants (`crates/pi-ast/src/language/mod.rs:270-327`), not 56 as claimed at report line 19. The 24 tag languages and 20 declaration-map languages overlap; their union is 26, leaving **31** without either RepoMap route (`crates/pi-ast/src/map.rs:369-386,962-992`).

3. **Domain 6 falsely reports `xutf` missing from notices.** `docs/research-2026-11-27-16-unified-domain6-platform-pkg.md:76-78` is contradicted by `THIRD-PARTY-NOTICES.txt:22378`, which lists `xutf 1.4.0`. Other cited omissions remain: `gix`, `ruzstd`, `tree-sitter-tags`, Rust `fastembed 7.8.0` and `ort`. The notices also list crates absent from the current lockfile, including `uiautomation` and `gl_generator` (`THIRD-PARTY-NOTICES.txt:10862-10868`), further weakening its claimed current-lock provenance.

4. **Domain 6 incorrectly says CI skips doctor tests.** `docs/research-2026-11-27-16-unified-domain6-platform-pkg.md:66,144` conflicts with `.github/workflows/standalone-runtime.yml:55-56`, which runs `cargo test --locked -p pi-agent -p aragent`; doctor unit and integration tests are included (`crates/aragent/src/doctor.rs:2599`; `crates/aragent/tests/doctor.rs:196`).

5. **Domain 6 reverses Android XDG behavior.** `docs/research-2026-11-27-16-unified-domain6-platform-pkg.md:119` says the resolver treats Android as Linux. It does not: `xdg_agent_root` accepts only Linux or macOS (`crates/aragent/src/dirs.rs:150-154`). Domain 5 correctly identifies the split: `init-xdg` accepts Android and creates directories that resolver ignores (`crates/aragent/src/config_cmd.rs:2220-2255`).

6. **Domain 6 overstates an R12 gate conflict.** `docs/research-2026-11-27-16-unified-domain6-platform-pkg.md:96-100` treats “not before R7” as an alternative to closing all R0–R12. The former is a necessary lower bound (`specs/release-plan.yaml:16`), compatible with the full-scope requirement (`specs/product/SCOPE_LATEST.yaml:182`).

7. **Domain 5 overstates doctor probe reporting.** `docs/research-2026-11-27-15-unified-domain5-frontends-ops.md:28` lists `0/10/11/12/13` without noting that the parent maps unrecognized exit codes to generic `Error` (`crates/aragent/src/doctor.rs:2340-2345`). Default runs read existing cache but write it only on explicit refresh (`crates/aragent/src/doctor.rs:2248-2279`). The `memfd_create_execveat` alias probes only `memfd_create`, not execution (`crates/aragent/src/doctor.rs:2483-2492`).

8. **Missed memory scaling risk.** The reported 200-candidate cap does not bound vector loading. Recall loads model embeddings, then scans and materializes all eligible scoped vectors before top-k selection (`crates/aragent/src/memory_store.rs:930-936,974-1020`). Large stores can consume memory and CPU proportional to stored vectors. Local memory also defaults off (`crates/aragent/src/config.rs:1307`).

9. **Missed RPC contract inconsistency.** Ready advertises protocol version 1 and support for `[1, 2]` (`crates/aragent/src/rpc/types.rs:13-15,585-592`), but `negotiateProtocol` rejects version 1 (`crates/aragent/src/rpc/mode.rs:970-974`). A client explicitly negotiating an advertised version receives an error.

10. **DAP session isolation is best-effort.** The report says adapters run in their own session, but `setsid()` errors are ignored (`crates/pi-agent/src/dap/client.rs:779-787`). Environment exposure comes from inherited parent environment, not from `ctx.env` being unfiltered: spawn omits `env_clear()` (`crates/pi-agent/src/dap/client.rs:764-775`).

### Confirmed spot checks

- **LSP:** enabled by default (`crates/aragent/src/config.rs:355-362`), contrary to D13’s default-off decision.
- **TUI:** uses alternate screen and clears/redraws on resize; it does not preserve transcript rows in native scrollback (`crates/aragent/src/tui.rs:416,462-471,489`).
- **ACP:** read mapping exists, but write and terminal paths are passed without equivalent mapping (`crates/aragent/src/acp/bridge.rs:71-78,101-130`; `crates/pi-agent/src/tools/mod.rs:2656`).
- **Specs:** execution status disagrees with the e01 capsule (`specs/execution-status.yaml:2,4`; `specs/epics/e01-r0-mcp/epic.yaml:18-32`); release plan references later capsule directories absent from `specs/epics/`.
- **Packaging:** CI uploads a debug host binary and Android release binary, with checksums but no commit-SHA or notices artifact (`.github/workflows/standalone-runtime.yml:66-79,130-145`).

Spot checks, not a full audit. No files modified; no cargo commands, tests or builds run.

---

## 3. `ReviewUnifiedBatch3` — ревью единого индекса и Stage-2 направлений (`-17-`)

### Verdict: REQUEST_CHANGES

**Step 1:** Read unified index in full.
**Step 2:** Spot-checked conflict register M1–M22, matrix M01–M25, and DIR-01–DIR-12 against cited documents, domain reports 11–16, and selected live source. Findings below. No files changed; no builds or tests run.

### Findings

1. **[P1] Separate donor context-order evidence from live runtime** — `docs/research-2026-11-27-17-unified-corpus-index.md:102-105,473`
   DIR-02 and M14 present donor TypeScript paths and user-first ordering as this repository’s behavior. Those paths are absent here. Donor evidence is pinned in `docs/research-2026-11-06-profiles-hooks.md:51-66`; live Rust loads each project context file before its agent-directory counterpart in `crates/aragent/src/prompts.rs:65-70`. The index also reverses donor sort and dedupe line anchors. Label donor evidence, correct anchors, and investigate live ordering separately.

2. **[P1] Include documented security and durability gaps in directions** — `docs/research-2026-11-27-17-unified-corpus-index.md:246-268,439,449,453`
   Matrix and DIR-08 omit several material findings from domain reports: durable scheduler rejects `task` calls (`crates/pi-agent/src/tools/scheduler.rs:233-247`); headless auto-resume remains unimplemented (`docs/research-2026-11-27-11-unified-domain1-core-state.md:91-93`); credentials reside in shared `agent.db`, not D27’s separate directory (`docs/research-2026-11-27-13-unified-domain3-providers-sec.md:47`); and project DAP configuration and adapter environment lack the trust/env protections described in `docs/research-2026-11-27-14-unified-domain4-analysis-memory.md:125-126,145-150`. Add explicit investigation and gates; otherwise the matrix can appear complete while missing known security and execution constraints.

3. **[P1] Replace Ratatui premise with live TUI evidence** — `docs/research-2026-11-27-17-unified-corpus-index.md:351-362,447`
   DIR-12 asks whether TUI uses `ratatui 0.30.2`, although live TUI is a custom ANSI renderer: `docs/research-2026-11-27-15-unified-domain5-frontends-ops.md:16`, `docs/agent-architecture-decisions.md:518`. That domain report identifies alternate-screen scrollback loss and an inert `tui.resizeScrollback` setting at `:35`; DIR-12 omits both. Redirect the investigation to the implemented renderer and its actual release risks.

4. **[P1] Correct MCP capability and transport claims** — `docs/research-2026-11-27-17-unified-corpus-index.md:440,475-476`
   M16 says fork lacks `prompts/list` and `prompts/get`; both exist in `crates/pi-agent/src/mcp/client.rs:819,886`. M17 says oversized stdio frames are dropped without closing, but `crates/pi-agent/src/mcp/transports/stdio.rs:178-181` exits the reader to drop the connection. Redirect behavior also differs by transport, per `docs/research-2026-11-27-13-unified-domain3-providers-sec.md:52`. Distinguish donor limitations from current implementation and retain genuine rich-content and subscription gaps.

5. **[P2] Correct fork-provenance chronology and trust-store status** — `docs/research-2026-11-27-17-unified-corpus-index.md:469,480`
   M10 says *later* decision text exempts `fork_provenance`; the later D80 explicitly requires it in the fork transaction (`docs/agent-architecture-decisions.md:550-554`). M21 calls `trust.json` unrecorded or stale, but D20 already documents its R8 implementation at `docs/agent-architecture-decisions.md:121`. Preserve the duplicate-D80 and journal-wording conflicts without reversing their chronology or treating the recorded implementation as unknown.

6. **[P2] Correct stale write and merge evidence** — `docs/research-2026-11-27-17-unified-corpus-index.md:419,430`
   M05 says `write` lacks an equivalent guard, but live writes stage captured bytes and file identity (`crates/pi-agent/src/tools/mod.rs:2690-2770`); report 12 explicitly marks the earlier bypass claim stale at `docs/research-2026-11-27-12-unified-domain2-tools-exec.md:51`. The same report records process-local merge locking and no bounded-recompute evidence at `:57-58`; M11 does not surface those gaps. Update both rows and their investigation coverage.

7. **[P2] Fix Android temp-path evidence and add actual fallback investigation** — `docs/research-2026-11-27-17-unified-corpus-index.md:303,451`
   DIR-10 attributes `${TMPDIR:-$PREFIX/tmp}` to `dirs.rs`, but it is a proposed platform design, not that file’s implementation. Domain report 16 identifies live temporary-path defects at `docs/research-2026-11-27-16-unified-domain6-platform-pkg.md:117`. DIR-10 names temp paths in scope but has no acceptance question covering those fallback paths.

8. **[P2] Repair evidence anchors and distinguish historical status** — `docs/research-2026-11-27-17-unified-corpus-index.md:177,202,227,357,405,472`
   D84–D93 triage is at `docs/agent-preliminary-implementation-plan.md:288-298`, not `:299-303`; hooks-open text is at `:286`; reasoning replay drop is at `crates/pi-agent/src/provider.rs:1112-1170`, not `:1380-1400`; and `agent-architecture.md:472` is past EOF. DIR-12 also calls export/import and inspector unverified despite newer evidence in `docs/research-2026-11-27-11-unified-domain1-core-state.md:51-58`. Correct anchors and date historical status claims.

9. **[P2] Cover release-artifact evidence omitted from DIR-12** — `docs/research-2026-11-27-17-unified-corpus-index.md:349-370`
   Packaging release gate omits documented incomplete third-party notices and absent release/notice CI checks. Domain report 16 gives concrete missing lockfile entries and CI evidence at `docs/research-2026-11-27-16-unified-domain6-platform-pkg.md:59-80`. Add notice completeness and release-artifact provenance to acceptance questions.

10. **[P3] Disambiguate matrix and conflict references** — `docs/research-2026-11-27-17-unified-corpus-index.md:415-417,455-480`
    Matrix uses M01–M25 while conflict register uses M1–M22, creating ambiguous references. Matrix M03 also cites conflict `M23`, which does not exist. Use distinct prefixes and repair cross-references.
