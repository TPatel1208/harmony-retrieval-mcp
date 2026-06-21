# Session 12 — Phase 7.3–7.4: OPeNDAP + AppEEARS providers

**Read first:** `PLAN.md` Phase 7, §4.2 (output shape), §4.4 (Parquet for
tabular); `CLAUDE.md`.

## Goal
Two more `RetrievalProvider`s, wired into the router — with AppEEARS point data
going to **Parquet**, not a Zarr cube.

## Tasks
1. **`providers/opendap.py`** — Hyrax/DAP4 subset for gridded collections (e.g.
   GES_DISC). Implements `RetrievalProvider`; integrates with the router's
   decision tree.
2. **`providers/appeears.py`** — point/area sample tasks. **Point time-series
   output flows to Parquet → a `series_`/`obs_` handle**, never to `cube_`/Zarr.
3. Confirm the router (Phase 4) now selects OPeNDAP / AppEEARS where appropriate
   via capabilities — still one whole service, still no Harmony fallback.

## Constraints (do not violate)
- AppEEARS point results are tabular → Parquet. Do not funnel them through the
  gridded cube path.
- These providers honor the same durable job model and StorageBackend as Harmony.

## Gate
```bash
docker compose exec mcp pytest tests/unit/test_providers/test_opendap.py \
  tests/unit/test_providers/test_appeears.py -v
# Live, nightly (needs EDL creds):
EARTHDATA_TOKEN=... docker compose exec mcp pytest -m live \
  tests/live/test_appeears_point.py tests/live/test_opendap_subset.py -v
```

## Commit
`feat: OPeNDAP + AppEEARS providers (Parquet point path) (Phase 7.3-7.4)`
