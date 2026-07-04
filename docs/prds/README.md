# PRD series — TTA integration (MCP side)

One session, one commit each (CLAUDE.md discipline). Numbering continues the
existing PRD sequence referenced in code comments (PRD 004–015 were session
prompts; from 016 on, PRDs live here as files).

| PRD | Title | Depends on |
|---|---|---|
| [016](prd-016-green-integration-suite.md) | Green the integration suite | — (do first) |
| [017](prd-017-export-result-rematerialize.md) | `export_result` + `rematerialize` egress contract | 016 |
| [018](prd-018-http-transport-token-compose.md) | HTTP transport, bearer token, compose topology | 016 |
| [019](prd-019-live-matrix-consumer-hygiene.md) | Live TTA-collection matrix + consumer hygiene | 017, 018 |

The consumer-side series (T01–T10) lives in the Talking-To-Air repo under
`docs/prds/`. T01 requires 018; T02 requires 017+018; 019 runs concurrently
with TTA Phase 1 (overlap decision, 2026-07-04).

**Tracker note:** these were intended as GitHub issues labeled
`ready-for-agent`, but the current fine-grained PAT lacks Issues write
permission. Once the token has Issues: Read & Write, each file body can be
published as an issue verbatim.

Source plans: [tta_integration_fix_plan.md](../tta_integration_fix_plan.md),
[tta_research_platform_plan.md](../tta_research_platform_plan.md) (decision
record in §8).
