# Session 11 — Phase 7.1–7.2: Preview & transform

**Read first:** `PLAN.md` Phase 7; `CLAUDE.md`.

## Goal
Preview/inspection tools and the transform pipeline, with provenance edges
recorded on every transform.

## Tasks
1. **`tools/preview.py`** — `preview_dataset` (via GIBS), `summarize_dataset`,
   `inspect_statistics`.
2. **`tools/transform.py`** — `subset`, `reproject`, `resample`,
   `convert_format`, `align` → produce `cube_` handles + an alignment report.
   Each transform records a **provenance edge** (Phase 3), keyed to the spec.

## Constraints
- Every transform appends to the provenance DAG; no silent transforms.
- Reuse the materialization/storage path from Phase 6 (StorageBackend, format by
  shape) — don't introduce a second storage route.

## Gate
```bash
docker compose exec mcp pytest tests/unit/test_tools/test_preview.py \
  tests/unit/test_tools/test_transform.py -v
```
Tests assert transform outputs are `cube_` handles and that a provenance edge is
written per transform.

## Commit
`feat: preview + transform tools with provenance edges (Phase 7.1-7.2)`
