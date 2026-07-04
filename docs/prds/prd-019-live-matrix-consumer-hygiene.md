# PRD 019 — Live validation matrix for TTA collections + consumer hygiene

**Repo:** harmony-retrieval-mcp · **Session scope:** one session (plus expect 1–2 follow-up quirk-fix sessions), one commit · **Label:** ready-for-agent
**Depends on:** PRD 017 + 018 (the matrix exercises export over the real pipeline). Runs concurrently with Talking-to-Air Phase 1 per the overlap decision.

## Problem Statement

As the integrator, I have no evidence this server works against the eight real collections Talking-to-Air actually uses — the live suite has only ever touched test environments and MOD13Q1. TEMPO's group-prefixed variables and hourly volume, OMI's HDF-EOS grids and multi-group files, TROPOMI's monthly temporal semantics, and MODIS AOD's −999 fills are each a plausible silent breaker. Separately, three small consumer-facing gaps remain: a reconnecting consumer cannot re-enumerate its handles, per-variable masking metadata may not surface, and the repo ships no LICENSE.

## Solution

An opt-in live test matrix that walks search → describe → retrieve (small AOI, minimal window) → poll → export → open-with-xarray for each of the eight collections, filing every quirk into the quirk ledger; plus three hygiene items: a `list_workspace` tool, verified fill/valid-range surfacing in `describe_dataset`, and a LICENSE.

## User Stories

1. As the integrator, I want each of the eight TTA collections (TEMPO NO2/O3TOT/HCHO L3 V04, OMI_MINDS_NO2d, OMDOAO3e, OMHCHOd, TROPOMI NO2 monthly, MODIS Terra/Aqua AOD) validated live end-to-end, so that Phase 1 demos are built on collections known to work.
2. As a test runner without credentials, I want the matrix opt-in behind the live marker, so that default runs stay hermetic.
3. As a future debugger, I want every quirk discovered (group paths, fill handling, grid oddities, temporal semantics) recorded in the quirk ledger with the collection and symptom, so that fixes are traceable and prompt guidance can cite them.
4. As a consumer that reconnects after a restart or page reload, I want `list_workspace(workspace_id, handle_type=None)` returning `{handles: [{handle, type, created_at, summary}]}`, so that I can re-enumerate my datasets, AOIs, jobs, and results without replaying a conversation.
5. As a consumer applying masks, I want `describe_dataset`'s per-variable enrichment to surface UMM-Var fill values and valid ranges (or an explicit advisory note when CMR lacks them), so that I can drop hand-pinned fill/valid-range tables.
6. As an external project consuming this repo, I want a LICENSE file, so that reuse terms are explicit.

## Implementation Decisions

- The live matrix is a parameterized test over the eight collections: search by short name/version → describe → define a tight AOI → retrieve a 1-day (hourly products) or 1-month (monthly products) window with the primary variable → poll to terminal → export → open with xarray/pyarrow → assert the primary variable exists and CF time decodes.
- Failures are data, not blockers: a failing collection files a ledger entry and (if the fix is small) a fix in the same session; structural fixes spawn follow-up sessions in this repo.
- `list_workspace` is a thin read over the existing workspace-store listing seam; no new state, PRD 014 answer shapes, workspace-scoped.
- Masking metadata: verify the UMM-Var enrichment path; where fill/valid-range exist in CMR they must appear per-variable; where absent, the response carries an advisory note naming the gap (the consumer keeps a thin override table only for wrong records).
- LICENSE: maintainer's choice (MIT unless told otherwise) — a file, not a discussion.

## Testing Decisions

- The live matrix itself is the test artifact; marked live, needs real Earthdata Login credentials, never in the default gate.
- `list_workspace` and the masking-metadata check get hermetic unit tests at the standard tool seam with store/CMR fakes (prior art: discovery tool tests).
- Good live tests assert consumer-observable outcomes (variable present, time decodes, values unmasked-but-flagged) rather than provider internals — the same assertion style regardless of which provider the router chose.

## Out of Scope

- Fixing every quirk found — quirks beyond quick fixes become their own sessions.
- Prompt guidance for the TTA agent (Talking-to-Air PRD T04 consumes the ledger).
- Per-workspace quotas, S3, auth (pre-hosting phase).

## Further Notes

This is the highest-uncertainty item in the whole program (decision record §8.7): run it early and in parallel with TTA Phase 1, and let ledger entries flow into T04's prompt guidance.
