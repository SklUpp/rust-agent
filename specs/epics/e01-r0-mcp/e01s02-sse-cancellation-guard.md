# e01s02: SSE Handshake Cancellation Socket Guard (R0.2)

## 1. Story Title & Identity
- Story ID: `e01s02`
- Title: SSE Handshake Cancellation Socket Guard (R0.2)
- Epic: `e01-r0-mcp`
- Status: failing
- BCPs: 5
- Risk: P0
- Context: domain / networking / mcp-transport

## 2. Problem Statement
When `connect_to_server` is cancelled or times out during the HTTP/SSE `initialize` handshake, the uninitialized transport future is dropped without completing graceful shutdown. Dropping the transport leaves background worker tasks and underlying TCP loopback sockets open until the remote server times out, violating the 5-second cleanup requirement. Adding a blanket client request timeout is prohibited because it prematurely breaks legitimate long-lived SSE event streams.

## 3. User Value & Business Context
Prevents file descriptor exhaustion and zombie connection buildup when MCP server connections stall or are cancelled by user interruption on Linux and Termux environments.

## 4. Target Personas / Agents
Runtime core, MCP client supervisor, tool execution subagents, long-running headless agent workers.

## 5. Scope & Boundary Conditions
- In scope: Implement an uninitialized transport cancellation guard that intercepts future drop during `create_and_init_transport`, ensuring that within 5.0 seconds (`CLOSE_TIMEOUT`), the underlying TCP connection is aborted and background worker tasks are stopped. Ensure subsequent/repeated `initialize` succeeds without error. Add explicit tests in `crates/pi-agent/tests/mcp_http_cancel.rs` for socket/task termination, repeated `initialize`, and long SSE stream preservation.
- Out of scope: Modifying the established `McpConnection::close()` path for fully initialized connections; altering SSE wire framing or message protocols; adding blanket HTTP client request timeouts that break legitimate long SSE streams.

## 6. Technical Architecture & Component Impact
- `crates/pi-agent/src/mcp/client.rs`: Guarding `create_and_init_transport` and `published_rmcp` across future drop boundaries.
- `crates/pi-agent/tests/mcp_http_cancel.rs`: Test harness asserting socket closure within 5s, background task termination, repeated `initialize` success, and long SSE stream survival.

## 7. Prerequisites & Dependencies
- R0.1 transport redaction baseline established.
- rmcp transport service abstraction.

## 8. Assumptions
- Tokio runtime aborts background hyper I/O tasks when child task cancellation tokens trigger.
- Execution environment provides loopback TCP networking for integration tests.

## 9. Non-Functional Requirements & Performance
- Bounded Cleanup: Socket closure and background task termination strictly bounded within 5.0 seconds of cancellation.

## 10. Security & Privacy Considerations (Threat Model)
- Denial of Service (CWE-400 / Resource Exhaustion): Prevents socket descriptor exhaustion caused by repeated aborted MCP server connection attempts.
- Error Sanitization: Handshake timeout errors must not leak authentication tokens in display output.

## 11. Error Handling & Edge Cases
- Client abort during initialize delay: TCP socket closed and background task aborted within 5s.
- Repeated initialize after abort: Next `initialize` request completes successfully without state or port collision.
- Extended SSE streaming: Idle intervals during valid tool executions do not trigger transport cleanup guards.

## 12. Data Schema / State Mutations
None. In-memory connection handles and transport task lifecycle only.

## 13. Observability, Logging & Diagnostics
Teardown events logged at debug level; connection status inspectable via `arag doctor`.

## 14. Interfaces & Contract Definitions
- `connect_to_server(name: &str, config: MCPServerConfig) -> Result<McpConnection, McpError>` signature remains unchanged.
- Cancellation yields `McpError::Timeout` within the configured initialization deadline.

## 15. Backward Compatibility & Migrations
Fully transparent to existing MCP configuration blocks (`MCPServerConfig::Http`, `MCPServerConfig::Sse`).

## 16. Requirements
#### MODIFIED: Transport lifecycle teardown on handshake abort
**Before:** An aborted or timed-out `connect_to_server` call dropped the handshake future without executing cleanup, leaving the background SSE listener and TCP socket open until remote disconnect.
**After:** A drop-guard task explicitly triggers transport shutdown, aborts the underlying hyper/reqwest connection, and terminates the background worker task within 5 seconds without placing a global client timeout on subsequent long SSE streams.

#### ADDED: Bounded socket closure and task termination regression test
**Before:** `mcp_http_cancel.rs` only validated tool-call deadline bounds after successful initialization.
**After:** `mcp_http_cancel.rs` asserts that cancelling during an in-flight `initialize` response releases the loopback socket and terminates the background worker task within 5 seconds.

#### ADDED: Repeated initialize success verification
**Before:** No test verified that a subsequent `initialize` call succeeds after an earlier cancelled handshake.
**After:** `mcp_http_cancel.rs` asserts that a repeated `initialize` call cleanly establishes a connection after a cancelled attempt.

#### ADDED: Long SSE stream preservation test
**Before:** `mcp_http_cancel.rs` had no explicit test verifying long-lived SSE stream preservation under idle periods.
**After:** `mcp_http_cancel.rs` tests that long-lived SSE connections remain intact across idle intervals without premature cancellation.

## 17. Acceptance Criteria
### Scenario: Bounded socket cleanup and task termination on initialize timeout
  Given an MCP HTTP server that delays its initialize response beyond client timeout
  When client invokes `connect_to_server` with a short timeout
  Then `connect_to_server` returns `McpError::Timeout`
  And the underlying TCP socket is closed within 5 seconds
  And the background transport worker task is stopped

### Scenario: Repeated initialize succeeds after cancelled handshake
  Given a previous `connect_to_server` call was aborted during initialize
  When client initiates a new `connect_to_server` call to a responsive MCP server
  Then `connect_to_server` succeeds
  And the server capabilities and tools can be queried without connection error

### Scenario: Long SSE stream preservation without premature timeout
  Given an established MCP connection to an SSE server
  When a streaming tool call remains active across multiple event chunks with idle intervals
  Then the connection remains open and is not interrupted by transport cancellation guards

## 18. Implementation Steps
1. Add regression test in `crates/pi-agent/tests/mcp_http_cancel.rs` asserting socket closure and background task termination within 5s on initialize delay → verify: cargo test --locked -p pi-agent --test mcp_http_cancel
2. Add regression test in `crates/pi-agent/tests/mcp_http_cancel.rs` asserting repeated initialize succeeds after a cancelled handshake → verify: cargo test --locked -p pi-agent --test mcp_http_cancel
3. Add test in `crates/pi-agent/tests/mcp_http_cancel.rs` verifying long SSE streams survive idle intervals without premature timeout → verify: cargo test --locked -p pi-agent --test mcp_http_cancel
4. Implement transport cancellation guard in `crates/pi-agent/src/mcp/client.rs` aborting uninitialized SSE transport on future drop/timeout within 5s → verify: cargo test --locked -p pi-agent --test mcp_http_cancel

## 19. Verification Script (Step-by-Step)
1. Run MCP cancellation and long-stream test suite: `cargo test --locked -p pi-agent --test mcp_http_cancel`
2. Run boundary test suite: `cargo test --locked -p pi-agent --test mcp_boundaries`
3. Observe all tests pass and socket handles release cleanly.

## 20. Out of Scope & Risks
- Known P3 constraint: Underlying rmcp/hyper/reqwest internals may retain socket state if remote server keeps TCP window open; guard must enforce forceful abort rather than cooperative drain.
- Environment constraint: Tests require loopback TCP socket binding; can execute in CI or on Termux/Linux where Rust toolchain is configured.
