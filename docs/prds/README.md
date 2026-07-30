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

*(Table not backfilled for 020–022; those PRDs are in this directory and
self-describe their dependencies in their own headers.)*

## Caching series — 023–025 (design session 2026-07-28)

Requested by the consumer during its own cache design. Build **in order**: 025
must not ship before 024, or speculative data competes with user-requested data
on equal terms in the eviction ranking and the first thing prefetching does is
evict something a researcher asked for.

| PRD | Title | Depends on |
|---|---|---|
| [023](prd-023-materialization-identity.md) | Record + expose what was *actually* retrieved (`content_digest`) | 017, 020, 021 |
| [024](prd-024-retention-pinning-cost-aware-eviction.md) | Pinning, cost-aware eviction, per-workspace fairness | 015, 017, 020 |
| [025](prd-025-adjacent-granule-prefetch.md) | Adjacent-granule prefetch (ships **disabled**) | 023, 024 |

**Why 023 exists.** Investigating whether an `obs_` handle denotes stable *bytes*
— which a consumer-side derived-artifact cache needs — established that it does
not. `rematerialize` replays the persisted spec unchanged, but Harmony resolves
granules server-side at submit time, is frequently submitted unpinned (a hard
rule in CLAUDE.md), and OPeNDAP is a runtime fallback rather than a planning-time
choice. So an identical spec can legitimately yield different data. This server
records intent and never records outcome; 023 closes that gap.

The consumer-side series (T01–T10) lives in the Talking-To-Air repo under
`docs/prds/`. T01 requires 018; T02 requires 017+018; 019 runs concurrently
with TTA Phase 1 (overlap decision, 2026-07-04). That repo's caching series is
T50–T54: its T52 (Zarr cube cache) is the direct counterpart to 024, and its T54
is blocked on 023.

**Tracker note:** these were intended as GitHub issues labeled
`ready-for-agent`, but the current fine-grained PAT lacks Issues write
permission. Once the token has Issues: Read & Write, each file body can be
published as an issue verbatim.

Source plans: [tta_integration_fix_plan.md](../tta_integration_fix_plan.md),
[tta_research_platform_plan.md](../tta_research_platform_plan.md) (decision
record in §8).
