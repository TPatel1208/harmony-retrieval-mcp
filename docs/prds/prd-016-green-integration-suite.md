# PRD 016 — Green the integration suite

**Repo:** harmony-retrieval-mcp · **Session scope:** one session, one commit · **Label:** ready-for-agent
**Depends on:** nothing — do this first; everything after lands on green.

## Problem Statement

As the maintainer preparing this server for consumption by an external application (Talking-to-Air), I cannot trust the integration gate: the durable-pipeline integration suite reports 4 failures / 1 pass, so any regression introduced by the upcoming consumer-contract work (PRD 017/018) would be invisible.

## Solution

Make the integration suite fully green and hermetic. Three failures are stale expectations (the suite predates the `routed` plan-time and `job-failed` terminal provenance events, which are intended behavior); one failure is a live network call to the AppEEARS product/layer lookup that escaped the provider fake seam.

## User Stories

1. As the maintainer, I want the integration suite green, so that PRD 017/018 changes are gated by a trustworthy signal.
2. As the maintainer, I want the provenance-event expectations to match the intended event vocabulary (routed, submitted, provider-fallback, job-failed, materialized), so that the tests document the real lifecycle.
3. As a contributor without Earthdata credentials, I want the integration suite to make zero network calls, so that I can run the gate offline.
4. As the maintainer, I want the AppEEARS layer lookup behind the same fake seam as the rest of the provider, so that future AppEEARS tests stay hermetic by default.
5. As a future agent session, I want the gate command (`docker compose exec mcp python -m pytest tests/integration -v`) to pass exactly as written in CLAUDE.md, so that the one-session-one-commit discipline holds.

## Implementation Decisions

- Update the three stale test expectations to assert the current, intended event orderings — the code behavior is correct per CONTEXT.md (branch explanations are *in addition to* JOB_FAILED); the tests are what changed underneath.
- The AppEEARS point-path test reaches the real product/layer lookup endpoint; extend that test's provider fake so the layer lookup is faked alongside job submission. No production code changes expected — this is a test-seam completion, not a behavior change.
- If greening reveals a genuine behavior bug rather than a stale expectation, fix the behavior; never weaken an assertion to pass.

## Testing Decisions

- The deliverable IS tests. Good tests here assert externally observable pipeline behavior (event sequences read back from Postgres, materialized artifacts on the storage backend), never internal call orders.
- Seam: the existing durable-pipeline integration harness (fake provider + real Postgres/Redis in the compose stack). No new seams.
- Gate: integration suite fully green; unit suite stays at 668 passing.

## Out of Scope

- Any new tool or transport work (PRD 017/018).
- The live TTA-collection matrix (PRD 019) — deliberately non-hermetic and opt-in.

## Further Notes

Half-session by estimate; if it finishes early do NOT pull PRD 017 work into the same commit.
