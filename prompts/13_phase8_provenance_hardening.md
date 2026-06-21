# Session 13 — Phase 8: Provenance tools, citations, hardening

**Read first:** `PLAN.md` Phase 8, §4.5; `CLAUDE.md`. Final phase.

## Goal
Ship the provenance/citation tools, per-provider rate limiting, and integration
tests — with the **real Harmony path included in "done," not skipped.**

## Tasks
1. **`tools/provenance.py`** — `get_provenance` (lineage via the recursive CTE),
   `cite_dataset` (reuse CMR's citation records — NASA's `get_citations` pattern —
   for official DOIs and formal strings).
2. **Rate limiting** per provider.
3. **Register all tools** in `server.py`; confirm the v1 surface from PLAN.md §7.
4. **Integration tests** end-to-end. The full-retrieval flow against real Harmony
   is a `@live` test that **runs** (nightly/release) — do not exclude Harmony from
   the meaning of "done."

## Constraints
- Citations come from CMR's records, not hand-rolled strings.
- Do not weaken the integration gate to make it pass; the live Harmony flow is
  required.

## Gate
```bash
docker compose exec mcp pytest tests/unit/ -v --tb=short
docker compose exec mcp pytest tests/integration/ -v
# Required for release: the real Harmony flow (needs EDL creds):
EARTHDATA_TOKEN=... docker compose exec mcp pytest -m live tests/live/test_full_retrieval.py -v
docker compose exec mcp python -c "
from earthdata_mcp.server import mcp; print('tools:', len(mcp.list_tools()))"
```
Confirm the tool count matches PLAN.md §7.

## Commit
`feat: provenance + citations + hardening + integration (Phase 8)`
