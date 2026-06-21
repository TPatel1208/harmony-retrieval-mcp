# Session 3 — Phase 2.1: CMR patterns reference (research, no code)

**Read first:** `PLAN.md` Phase 2 task 2.1 and §0 (dependency reality check);
`CLAUDE.md`.

## Goal
A short reference doc capturing how to query CMR correctly, so the provider code
in the next session is right the first time.

## Tasks
Read NASA's `get_collections`, `get_granules`, `get_variables`, and
`get_services` tool implementations in `github.com/nasa/earthdata-mcp` **at a
specific pinned commit**. Summarize, for each, the exact CMR endpoint,
parameters, pagination approach, and UMM-JSON parsing.

Write `docs/cmr_patterns.md` that:
- Cites the **pinned commit hash** you read.
- States clearly that **canon is CMR's public API docs + UMM schemas**, and
  NASA's repo is a worked example to re-verify (it is young and refactoring).
- Notes which UMM-C fields the capability merge will need later:
  `ProcessingLevel`, `ArchiveAndDistributionInformation`,
  `DirectDistributionInformation`, `CollectionProgress`, `StandardProduct`,
  `Purpose`.

## Constraints
- **No provider code this session.** Documentation only.
- If you can't access the repo, build the doc from the CMR API docs and say so;
  do not invent parameter names.

## Gate
- `docs/cmr_patterns.md` exists, cites a pinned commit, and covers all four
  tools + the UMM-C fields above.

## Commit
`docs: CMR query patterns reference (Phase 2.1)`
