# e01s04: Live External MCP Server Smoke on Termux (R0.4)

## 1. Story Title & Identity
- Story ID: `e01s04`
- Title: Live External MCP Server Smoke on Termux (R0.4)
- Epic: `e01-r0-mcp`
- Status: failing (test written in `cf1e673` as `crates/aragent/tests/mcp_filesystem_smoke.rs`, gated behind `ARAGENT_LIVE_FILESYSTEM_SMOKE`; the live Termux run has not been executed yet. Per `docs/agent-preliminary-implementation-plan.md:38`: context7 smoke executed on Termux; live filesystem MCP server smoke and live reconnect after `kill -9` remain pending)
- BCPs: 3
- Risk: P1
- Context: domain / integration / mcp-external

## 2. Problem Statement
External MCP server integration previously passed partial live smoke on Termux using `context7-mcp` (`initialize` -> `tools/list` -> `tools/call`), leaving no lingering processes on exit. However, live reconnect after `kill -9` was not exercised on device (covered only by unit tests in `mcp_boundaries`), and the external filesystem server was not used in place of context7.

## 3. User Value & Business Context
Guarantees reliable external MCP server operation (specifically the filesystem server) on Termux, verifying that unexpected server crashes (`kill -9`) trigger automatic reconnection on the next tool call and that shutdown leaves zero zombie processes in the OS process table.

## 4. Target Personas / Agents
Runtime execution supervisor, MCP manager, external tool caller subagents.

## 5. Scope & Boundary Conditions
- In scope: An executable device smoke test in `crates/aragent/tests/mcp_filesystem_smoke.rs` (written in `cf1e673`, gated behind `ARAGENT_LIVE_FILESYSTEM_SMOKE`) exercising an external filesystem MCP server on Termux/Linux with `initialize`, `tools/list`, `tools/call`, automatic reconnect after an abrupt `kill -9` of the server process, and clean `close()` ensuring zero zombie processes. The gated live run on device has not been executed yet. Automated transport reconnect logic is covered by existing `crates/pi-agent/tests/mcp_boundaries.rs`.
- Out of scope: Sequential dependency on R0.1–R0.3 (R0.1–R0.4 execute in parallel per `docs/agent-preliminary-implementation-plan.md:52`). Arbitrary shutdown duration limits (clean exit without zombie processes is required, with no arbitrary time ceiling).

## 6. Technical Architecture & Component Impact
- `crates/aragent/tests/mcp_filesystem_smoke.rs`: New executable integration test target for live external filesystem MCP server lifecycle on Termux/Linux (written in `cf1e673`, gated behind `ARAGENT_LIVE_FILESYSTEM_SMOKE`; live run pending).
- `crates/pi-agent/tests/mcp_boundaries.rs`: Existing automated reconnect verification suite.
- `crates/pi-agent/src/mcp/client.rs`: Connection lifecycle, broken transport detection, and reconnect backoff.

## 7. Prerequisites & Dependencies
- Parallel ordering: R0.1–R0.4 can execute in parallel as tails of the 0.1.x line (`docs/agent-preliminary-implementation-plan.md:52`); does not depend sequentially on R0.1–R0.3.
- Execution environment: Android Termux arm64 or Linux environment with external filesystem MCP server.

## 8. Assumptions
- External filesystem MCP server communicates via stdio and handles standard POSIX signals.

## 9. Non-Functional Requirements & Performance
- Process cleanup: Clean exit leaving zero zombie (`<defunct>`) processes after `close()`.

## 10. Security & Privacy Considerations (Threat Model)
- Process Isolation: External server termination does not corrupt host state or crash the runtime. Stdio communication channels are isolated.

## 11. Error Handling & Edge Cases
- Sudden `SIGKILL` (`kill -9`) of server: Runtime detects broken transport on EOF and cleanly reconnects on the subsequent tool call.
- Process termination: Child process is cleanly reaped, preventing process table bloat.

## 12. Data Schema / State Mutations
None. Runtime process handles and MCP tool registry only.

## 13. Observability, Logging & Diagnostics
Process termination and reconnect events logged with server identifier.

## 14. Interfaces & Contract Definitions
`MCPServerConfig::Stdio` and MCP client connection recovery contracts.

## 15. Backward Compatibility & Migrations
Compatible with standard Model Context Protocol stdio transport specifications.

## 16. Requirements
#### ADDED: Executable live filesystem MCP server smoke test
**Before:** No dedicated integration test existed for live external filesystem MCP server execution on Termux.
**After:** Creation of `crates/aragent/tests/mcp_filesystem_smoke.rs` provides an executable integration test verifying the full lifecycle on device: `initialize` -> `tools/list` -> `tools/call` -> `kill -9` -> automatic reconnect -> clean `close()` without zombie processes.

#### ADDED: Reconnection verification
**Before:** Live reconnection after process kill was not verified on hardware.
**After:** Automated coverage in `mcp_boundaries.rs` and the new `mcp_filesystem_smoke.rs` test verify seamless reconnection after unexpected server termination.

## 17. Acceptance Criteria
### Scenario: Live filesystem MCP server smoke on Termux
  Given an external filesystem MCP server configured in aragent on Termux
  When aragent executes `initialize`, queries `tools/list`, and calls a filesystem tool via `tools/call`
  Then the tool call completes and returns valid filesystem output
  And killing the server process with `kill -9` triggers automatic reconnect on the subsequent tool call
  And invoking `close()` cleanly terminates the server without zombie processes

### Scenario: Automated reconnect after transport break
  Given an active MCP connection in `mcp_boundaries`
  When the server connection is broken
  Then subsequent requests automatically trigger reconnection and succeed

## 18. Implementation Steps
1. Verify automated reconnect logic after server disconnect in existing tests → verify: cargo test --locked -p pi-agent --test mcp_boundaries
2. Created real executable device smoke test in `crates/aragent/tests/mcp_filesystem_smoke.rs` (`cf1e673`) covering external filesystem MCP server initialize, tools/list, tools/call, kill -9 reconnect, and close without zombies; live Termux run not yet executed → verify: cargo test --locked -p aragent --test mcp_filesystem_smoke

## 19. Verification Script (Step-by-Step)
1. Run existing transport reconnect regression test: `cargo test --locked -p pi-agent --test mcp_boundaries`
2. Run newly created executable device smoke test against external filesystem MCP server: `cargo test --locked -p aragent --test mcp_filesystem_smoke`
3. Confirm server recovers after `kill -9` and `close()` leaves zero zombie processes.

## 20. Out of Scope & Risks
- Test exists but is not exercised by default: `crates/aragent/tests/mcp_filesystem_smoke.rs` (cf1e673) is gated behind `ARAGENT_LIVE_FILESYSTEM_SMOKE`, so CI skips it; the live Termux run remains pending and its verify command reflects that.
- Network dependency: Live external MCP smoke is executed on target device and is not an obligatory CI gate; CI gate relies on local fixtures (`docs/agent-preliminary-implementation-plan.md:62`).
