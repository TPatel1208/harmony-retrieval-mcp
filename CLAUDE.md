# Earthdata MCP Server — project rules

## Spec
PLAN.md is the spec and the single source of truth. Before any task, read the
PLAN.md section the current prompt names, plus this file. If a prompt and PLAN.md
disagree, PLAN.md wins — flag the conflict, don't silently pick one.

## Hard rules (never violate)
- Use the official harmony-py client; do NOT hand-roll a Harmony client.
- Check service capability via get_services and CollectionCapabilities.find_service
  before any Harmony submit. Match ONE whole service. Never trust the rolled-up
  top-level capability booleans (they are an unsatisfiable union). Never "fall
  back" to Harmony for a collection no service can handle.
- All retrieval is a durable job: state persisted in Postgres, resumable on
  restart. No in-memory background tasks for anything that matters.
- Provenance records the request SPEC (re-materializable), never an ephemeral
  staged-output URL.
- Storage goes through the StorageBackend interface. Local filesystem is the
  default; never hard-code S3 or a cloud path.
- Canon for CMR is CMR's public API + UMM schemas, NOT NASA's MCP repo (it is
  young and refactoring). Cite a pinned commit when borrowing a pattern.
- No analysis tools (correlation, trend, anomaly, hotspot, risk, narrative).

## Working discipline
- One prompt = one session = one commit. Do the tasks, run the gate commands
  exactly as written, and only when they pass, commit with the message the prompt
  gives. If a gate fails, fix it or stop and report — do not commit red, and do
  not weaken or skip a gate to make it pass.
- Keep changes scoped to the current phase. Do not build ahead.

## Stack
Python 3.13+, uv, FastMCP, httpx + tenacity, pydantic, SQLAlchemy + asyncpg,
Postgres + PostGIS, harmony-py, earthaccess, xarray + zarr, pyarrow (Parquet).
Worker: Arq (Redis) by default; APScheduler in-process is acceptable for a
single-node research box — either way the Postgres `jobs` table is the source of
truth and the worker is stateless.

## TTA reuse decisions (from docs/tta_audit.md)
- async_harmony_service → replaced by harmony-py, do not port
- opendap_fetch_service → reference only, no tests, add them when porting
- cache_manager → do NOT port; rewrite from scratch as StorageBackend (§4.4)
- dataset_parser → reference only, zero coupling, add tests when lifting
- earthaccess_client → reuse with light adaptation (68 lines, tested) — read it in Phase 4.2
- utils/db → do NOT port; rewrite as SQLAlchemy + asyncpg, no LangGraph checkpointer
- docker-compose → adapt: keep PostGIS+volumes, add Arq+Redis, drop frontend/LLM/JWT