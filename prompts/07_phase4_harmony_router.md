# Session 7 — Phase 4.3–4.5: Harmony (harmony-py) + capability-gated router

**Read first:** `PLAN.md` Phase 4 (tasks 4.3–4.5), **§4.2 routing decision tree**;
`CLAUDE.md`. This is the heart of the build.

## Goal
A `RetrievalProvider` for Harmony that wraps `harmony-py`, and a router that
selects exactly one service via `find_service` — or fails fast.

## Tasks
1. **`providers/harmony.py`** — **wrap `harmony-py`.** Our code is only the
   `TransformSpec → harmony.Request` mapping and the optional `on_progress` glue.
   `harmony-py` owns request construction, the EDL session, polling, and Zarr.
   Submit **only** the service returned by `find_service(plan)` — pass its
   `service_name` explicitly so Harmony uses the matched service.
2. **`providers/router.py`** — implement the §4.2 decision tree:
   `find_service(plan)` → Harmony with that service; else `direct_s3`/native →
   direct fetch (**skip Harmony**); else OPeNDAP if present; else
   `NotRetrievable(reason, available=[...])` **at planning time**.
   **No Harmony fallback. No unioning across services.**
3. **Tests (4.5)** — router decision tree using the **Phase 2 union-trap
   fixture**:
   - `bbox + png` → `NotRetrievable`, with `available` listing both services' real
     capabilities;
   - `bbox + netcdf` → subsetter;
   - L3 direct-S3 case → direct fetch, **no Harmony submit**.
   Plus: Harmony service-name mapping, mocked poll/materialize, `on_progress`.
   Plus a **live** submit test marked `@pytest.mark.live`, against a small
   known-serviceable collection.

## Constraints (do not violate)
- Do not hand-roll Harmony request/polling logic — that's harmony-py's job.
- The router must never submit a job no single service can satisfy.

## Gate
```bash
docker compose exec mcp pytest tests/unit/test_router.py tests/unit/test_providers/ -v
# Nightly / on-demand (needs EDL creds) — this path is part of "done":
EARTHDATA_TOKEN=... docker compose exec mcp pytest -m live tests/live/test_harmony_submit.py -v
```

## Commit
`feat: Harmony provider (harmony-py) + capability-gated router (Phase 4)`
