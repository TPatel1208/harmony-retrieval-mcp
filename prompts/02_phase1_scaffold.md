# Session 2 — Phase 1: Scaffold & infrastructure

**Read first:** `PLAN.md` Phase 1 (§6) and §4.4 (storage); `CLAUDE.md`.

## Goal
A clean, importable skeleton with Docker, Postgres/PostGIS, the **local-default
storage backend**, and a **worker runtime** wired — nothing feature-specific yet.

## Tasks
1. Initialize the project with `uv` (Python 3.13+). Add deps the later phases
   need but don't use yet: FastMCP, httpx, tenacity, pydantic, SQLAlchemy,
   asyncpg, harmony-py, earthaccess, xarray, zarr, pyarrow, plus the worker lib.
2. Create the package layout from PLAN.md Phase 1, including:
   - `storage/` → `backend.py` (the `StorageBackend` interface), `local.py`
     (`LocalFilesystemBackend`, the default, under `EARTHDATA_DATA_DIR`), `s3.py`
     (`ObjectStoreBackend`, present but off by default).
   - `jobs/` → `models.py`, `state.py`, `worker.py` (stubs; the durable model is
     built in Phase 6, but the table/skeleton lands here).
   - `providers/`, `workspace/`, `catalog/` (+ `catalog/data/*.yaml` stubs),
     `tools/` files as listed.
   - `tests/fixtures/` already contains the two TEMPO JSON files — leave them.
3. Config: backend selected by `EARTHDATA_STORAGE` (`local` | `s3://...`),
   default `local`. DB + worker connection settings via env.
4. `docker-compose.yml`: Postgres+PostGIS, Redis (only if using the Arq worker),
   and the MCP service. Storage is a local volume by default; an optional
   MinIO/S3 profile may exist but stays off.
5. Wire FastMCP server entry (`server.py`) that imports cleanly with zero tools
   registered.

## Constraints
- No CMR/Harmony logic, no real tools. Skeleton + infra only.
- Storage code must go through the interface; never hard-code a cloud path.
- Worker is stateless; the (soon-to-exist) Postgres `jobs` table is the source
  of truth.

## Gate
```bash
docker compose up -d
docker compose exec mcp python -c "import earthdata_mcp.server"   # imports clean
docker compose exec mcp pytest tests/unit/test_config.py tests/unit/test_storage_local.py -v
```
`test_storage_local.py` round-trips put/get/delete on the **local** backend (no
cloud account). A parametrized S3 variant runs only when `EARTHDATA_STORAGE`
points at MinIO/S3.

## Commit
`feat: scaffold, Docker, local storage backend, worker runtime (Phase 1)`
