# e01s01: Transport Redaction Completion (R0.1)

## 1. Story Title & Identity
- Story ID: `e01s01`
- Title: Transport Redaction Completion (R0.1)
- Epic: `e01-r0-mcp`
- Status: failing (ledger unverified in active capsule; historical implementation completed in commit `3d4adf1` per `docs/agent-preliminary-implementation-plan.md:38`)
- BCPs: 3
- Risk: P1
- Context: domain / storage

## 2. Problem Statement
Persisted SQLite state previously recorded unredacted environment credentials and secrets when serializing certain execution records, delivery payloads, worker completion blocks, session auto-titles, and published artifact metadata.

## 3. User Value & Business Context
Prevents unintentional credential exposure and secret leaks in local SQLite databases, audit transcripts, and resume payloads across all execution contexts.

## 4. Target Personas / Agents
Runtime execution engine, subagent delivery workers, session store writer, human operator reviewing persistent sessions.

## 5. Scope & Boundary Conditions
- In scope: Applying `redact_json_strings` to `executions.intent_json/outcome_json`, `deliveries.payload_json`, `worker_completions`, `sessions.payload_json` (including session auto-title), and artifact metadata.
- Out of scope: Automated retrospective rewriting of previously committed historical database entries (reserved for explicit `--redact-legacy` subcommand).

## 6. Technical Architecture & Component Impact
Impacts SQLite persistence writer paths in `crates/aragent/src/store/` and session manager serialization.

## 7. Prerequisites & Dependencies
Core secret registry in `crates/aragent/src/auth/` and `pi-agent` string redaction primitives.

## 8. Assumptions
Secret patterns are registered in the global secret catalog prior to tool execution or session emission.

## 9. Non-Functional Requirements & Performance
Zero measurable latency overhead on SQLite write operations; no heap allocation when no registered secrets match.

## 10. Security & Privacy Considerations (Threat Model)
Mitigates credential leakage (CWE-200 / CWE-532) in local session storage and worker completion summaries.

## 11. Error Handling & Edge Cases
- Malformed JSON payloads fall back to direct substring scrubbing before persistence failure.
- Empty or whitespace-only secret keys are ignored and do not trigger blanket redaction.

## 12. Data Schema / State Mutations
No schema alterations; ensures text stored in JSON columns contains masked placeholders (`[REDACTED]`) instead of secret values.

## 13. Observability, Logging & Diagnostics
Sanitized records can be inspected via `arag doctor` without dumping raw secrets to standard output.

## 14. Interfaces & Contract Definitions
Store writer methods accept raw data structures and ensure redaction at the persistence boundary.

## 15. Backward Compatibility & Migrations
Existing unredacted historical records remain unchanged; new writes are masked transparently without schema migration.

## 16. Requirements
#### ADDED: Redaction on SQLite execution and delivery writers
**Before:** Store writer recorded raw tool intent/outcome and worker delivery payloads directly to SQLite tables.
**After:** All writer entry points pass JSON text through `redact_json_strings` before disk commit.

## 17. Acceptance Criteria
### Scenario: Secret masked in execution outcome
  Given a registered API key token in the credential store
  When a tool output contains the registered token
  Then the value persisted in `executions.outcome_json` is masked
  And resuming the session does not return the secret to the agent context

### Scenario: Secret masked in session auto-title
  Given a session title generation response containing a registered token
  When the session row is updated in `sessions.payload_json`
  Then the title string in the database contains `[REDACTED]`

## 18. Implementation Steps
1. Apply `redact_json_strings` in execution store writes → verify: cargo test --locked -p aragent
2. Apply `redact_json_strings` in delivery and worker completion store writes → verify: cargo test --locked -p aragent
3. Verify session resume does not echo redacted values → verify: cargo test --locked -p aragent

## 19. Verification Script (Step-by-Step)
1. Run crate tests for CLI and store: `cargo test --locked -p aragent`
2. Run core agent tests: `cargo test --locked -p pi-agent`
3. Inspect verified outcomes in test output.

## 20. Out of Scope & Risks
- Retroactive database rewrite is omitted by design to prevent data corruption without backup.
