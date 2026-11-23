# e01s03: Strip Sensitive rmcp Text from McpError Display (R0.3)

## 1. Story Title & Identity
- Story ID: `e01s03`
- Title: Strip Sensitive rmcp Text from McpError Display (R0.3)
- Epic: `e01-r0-mcp`
- Status: done (closed per `docs/agent-preliminary-implementation-plan.md:38`: `map_initialize_error` preserves rmcp step and category, omitting URL and body preview; status sourced from HttpStatus and reqwest redirect error)
- BCPs: 2
- Risk: P1
- Context: domain / security / mcp-errors

## 2. Problem Statement
`McpError::Protocol` formatting previously risked exposing the request URL (including query parameters such as API keys or OAuth tokens) and response body previews (<= 256 bytes) in display strings. Existing tests verified URL query scrubbing and status-only reporting with `None` body, but lacked an explicit regression test asserting that an actual secret-bearing response body is excluded from the formatted display output.

## 3. User Value & Business Context
Prevents credential exposure in user-facing error messages, audit logs, and agent contexts without retaining raw secrets in memory.

## 4. Target Personas / Agents
Runtime operators, audit logging subsystems, model context builder.

## 5. Scope & Boundary Conditions
- In scope: Sanitization via `map_initialize_error` preserving the rmcp step and error category while omitting URL query parameters and response body previews. Status codes are sourced from `HttpStatus` and reqwest redirect error types. Concrete regression test `initialize_http_status_excludes_secret_response_body` asserting that a non-empty secret-bearing response body is completely omitted from the formatted error string.
- Out of scope: Retaining raw unredacted secret credentials in memory. Modifying JSON-RPC wire protocol errors.

## 6. Technical Architecture & Component Impact
- `crates/pi-agent/src/mcp/transports/rmcp_adapter.rs:585`: `map_initialize_error` implementation and unit tests: `initialize_transport_error_does_not_echo_the_transport_text` (URL/query scrubbing) and regression test `initialize_http_status_excludes_secret_response_body` (secret response body exclusion).

## 7. Prerequisites & Dependencies
- Core MCP error structures.
- Parallel execution: R0.1–R0.4 execute in parallel (`docs/agent-preliminary-implementation-plan.md:52`).

## 8. Assumptions
- Callers and diagnostic logs rely on standard `Display` output for error inspection.

## 9. Non-Functional Requirements & Performance
- Zero memory retention of raw credentials; zero heap allocation when formatting sanitized error categories.

## 10. Security & Privacy Considerations (Threat Model)
- CWE-532 (Information Exposure Through Log Files): Ensures sensitive query parameters and body snippets never appear in formatted error strings.

## 11. Error Handling & Edge Cases
- Redirect errors: Sourced cleanly from reqwest without leaking redirect target query tokens.
- HTTP status errors with response bodies: Formatted using standard HTTP status codes; response body text is strictly excluded from `Display`.

## 12. Data Schema / State Mutations
None. In-memory error formatting only.

## 13. Observability, Logging & Diagnostics
Logs show sanitized error summary with rmcp step and category.

## 14. Interfaces & Contract Definitions
`Display` trait implementation for `McpError` prints sanitized error category and step.

## 15. Backward Compatibility & Migrations
Existing match blocks on `McpError` variants continue to compile and function.

## 16. Requirements
#### MODIFIED: McpError::Protocol Display sanitization
**Before:** Formatting `McpError::Protocol` could echo raw URL queries and response bodies up to 256 bytes.
**After:** `map_initialize_error` preserves the rmcp step and error category while omitting URL query parameters and response body previews; status is derived from `HttpStatus` and reqwest errors with zero raw-secret retention.

#### ADDED: Concrete secret-bearing response body regression test
**Before:** Existing unit test `initialize_http_status_is_reported_without_the_body` used `HttpStatusError::new(503, None)` and only asserted status code presence without testing body leakage.
**After:** Regression test `initialize_http_status_excludes_secret_response_body` explicitly supplies a secret-bearing response body (`HttpStatusError` with non-empty secret body string) and asserts that `map_initialize_error` excludes the body payload from the formatted string.

## 17. Acceptance Criteria
### Scenario: Secret query parameter is scrubbed from McpError Display
  Given an MCP initialization error with a URL containing query credentials
  When the error is converted via `to_string()` or `Display`
  Then the error displays the step and category
  And the query parameters and body preview are not present

### Scenario: Secret-bearing response body is excluded from Display
  Given an MCP server returns an HTTP status error with a secret-bearing response body (e.g. `HttpStatusError` with `Some("secret-token-body-leak-xyz")`)
  When `map_initialize_error` formats the error for display
  Then the error output contains the status `HTTP 503`
  And the secret response body `secret-token-body-leak-xyz` does not appear in the formatted string

## 18. Implementation Steps
1. Verify `map_initialize_error` preserves rmcp step and category without URL query or body preview in existing test → verify: cargo test --locked -p pi-agent --lib initialize_transport_error_does_not_echo_the_transport_text
2. Add concrete regression test `initialize_http_status_excludes_secret_response_body` with secret-bearing response body (`HttpStatusError` with non-empty secret body) asserting `map_initialize_error` does not leak response body text (task specifies adding missing test; existing test only checked None body) → verify: cargo test --locked -p pi-agent --lib initialize_http_status_excludes_secret_response_body

## 19. Verification Script (Step-by-Step)
1. Run URL scrubbing test: `cargo test --locked -p pi-agent --lib initialize_transport_error_does_not_echo_the_transport_text`
2. Run response body regression test: `cargo test --locked -p pi-agent --lib initialize_http_status_excludes_secret_response_body`
3. Confirm error display tests pass with zero secrets disclosed.

## 20. Out of Scope & Risks
- Security invariant: Raw secrets are never retained in error diagnostics.
