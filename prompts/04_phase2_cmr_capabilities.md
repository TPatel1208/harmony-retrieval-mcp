# Session 4 — Phase 2.2–2.5: CMR provider, CollectionCapabilities, KMS, enrichment

**Read first:** `PLAN.md` Phase 2 (tasks 2.2–2.5), **§4.2 in full** (the
capability model and the union trap), and §4.4; `docs/cmr_patterns.md`;
`CLAUDE.md`.

## Goal
`providers/cmr.py` (metadata only) plus the merged `CollectionCapabilities`
view, KMS normalization, and thin enrichment — all tested, including the
union-trap fixture.

## Tasks
1. **`providers/cmr.py`** as a `MetadataProvider`: `search_collections`,
   `search_granules`, `get_variables`, `get_services`, `check_availability`.
   httpx + tenacity (retry 5xx/timeout, never 4xx). Metadata only — no retrieve.
   The collection fetch must surface the UMM-C fields named in
   `docs/cmr_patterns.md`.
2. **`providers/_capabilities.py`** — build `CollectionCapabilities` exactly as
   in §4.2: UMM-C (Layer 1) merged with **per-service** `ServiceCapability`
   parsed from `get_services` (Layer 2). **Parse each service's own
   `capabilities` block; ignore the rolled-up top-level booleans.** Implement
   `find_service(plan)` (matches one whole service or returns `None`) and expose
   `direct_s3`, `output_shape`, `advisory`.
3. **`catalog/kms.py`** — `normalize_keyword(term) -> list[str]` mirroring NASA's
   `get_keywords`; cache the KMS dump, refresh on schedule.
4. **`catalog/enrichment.py`** — pull scale/offset/fill/QA from **UMM-Var first**;
   curated YAML adds only genuinely-additive notes for **≤10** products, each with
   `owner` + `last_reviewed`. Pass through cleanly when uncurated; mark notes
   **advisory/non-authoritative**.

## Constraints (do not violate)
- Routing/gating logic must use `find_service`, never the union flags.
- `providers/cmr.py` must not gain a `retrieve` — it's metadata only.
- Enrichment is advisory; don't let curated notes override UMM facts.

## Gate
```bash
docker compose exec mcp pytest tests/unit/test_cmr.py tests/unit/test_kms.py \
  tests/unit/test_capabilities.py tests/unit/test_enrichment.py -v
```
`test_capabilities.py` **must** use the saved fixtures and assert:
- `find_service(bbox + png)` → `None`
- `find_service(bbox + netcdf)` → `l2-subsetter-batchee-stitchee-concise`
- `find_service(variable + png)` → `asdc/imagenator_l2`
- L3 fixture: `output_shape == "grid"`, `direct_s3 is not None`, advisory mentions PROVISIONAL

Then the real-CMR smoke from PLAN.md (no auth) must print a shape, a direct_s3
bool, and a service count.

## Commit
`feat: CMR provider, CollectionCapabilities, KMS, enrichment (Phase 2)`
