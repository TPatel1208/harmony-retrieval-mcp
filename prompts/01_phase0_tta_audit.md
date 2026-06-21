# Session 1 — Phase 0: TTA reuse audit

**Read first:** `PLAN.md` §5 (Phase 0) and §8 (reuse map); `CLAUDE.md`.

## Goal
Decide, per component, whether we reuse TTA code or rewrite it using TTA as a
reference. The schedule depends on this; do not write feature code yet.

## Tasks
For each TTA component the plan intends to reuse — `async_harmony_service`,
`opendap_fetch_service`, `cache_manager`, `dataset_parser`, `earthaccess_client`,
`utils/db`, `docker-compose` — assess:
- **Test coverage** (is there any?).
- **License compatibility** with our intended OSS license.
- **Coupling** (can it be lifted without dragging LangGraph/SSE/app state?).

Write `docs/tta_audit.md` with an explicit **reuse / rewrite** decision and a
one-line justification per component.

Note up front: `async_harmony_service` is slated for **replacement by
`harmony-py`** (PLAN.md §2, §8) — record that decision; the audit mainly informs
OPeNDAP, cache, parser, auth, and db.

## Constraints
- No provider or tool code this session. This is analysis + one doc.
- If TTA source isn't available to you, say so in the doc and mark each
  component "rewrite (reference unavailable)" rather than guessing.

## Gate (definition of done)
- `docs/tta_audit.md` exists with a decision + justification for every component
  listed above.

## Commit
`docs: TTA reuse audit (Phase 0)`
