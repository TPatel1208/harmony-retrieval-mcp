# Session 10 — Phase 6.3: Durable async retrieval

**Read first:** `PLAN.md` Phase 6 task 6.3, **§4.3 (durable job model) in full**,
§4.4 (storage / format by shape); `CLAUDE.md`. This is the operational core.

## Goal
Retrieval tools backed by a durable, restart-safe job model. A long Harmony job
must survive a process restart.

## Tasks
1. **Finish `jobs/`** (skeleton from Phase 1): the Postgres `jobs` table
   (`job_id`, `job_handle`, `obs_handle`, `provider`, `request_spec` JSONB,
   `state`, `provider_job_url`, `progress`, `output_expires_at`, `error`,
   timestamps), the explicit **state machine** (`state.py`), and the **stateless
   worker** (`worker.py`) that drives submit → poll → materialize and **resumes
   non-terminal jobs on startup** from `provider_job_url`.
2. **`tools/retrieval.py`** — `retrieve_data`, `retrieve_subset`,
   `retrieve_timeseries`, `get_retrieval_status`, `cancel_retrieval`. Each mints
   a `job_` handle + a pending `obs_`; `get_retrieval_status` reads job state from
   Postgres (not memory). Materialize **by result shape**: gridded → Zarr,
   tabular → Parquet. Record the **request spec** as provenance. Cache-key per
   §4.4 (include `service_version`).
3. Handle the **sync path**: a single-product Harmony response that comes back
   inline is recorded `ready` immediately (or set `forceAsync=true` for
   uniformity).

## Constraints (do not violate)
- No in-memory background tasks as the source of truth — Postgres is.
- Provenance stores the spec, never the staged-output URL.
- Format follows shape; do not force tabular results into a Zarr cube.

## Gate
```bash
docker compose exec mcp pytest tests/unit/test_tools/test_retrieval.py -v
# Restart-resume: submit a (mocked) job, kill the worker, restart, assert resume.
docker compose exec mcp pytest tests/unit/test_jobs/test_resume.py -v
```

## Commit
`feat: durable async retrieval + worker resume (Phase 6.3)`
