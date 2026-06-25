# Earthdata MCP Server — project rules

## Spec
This file is the source of truth for project rules. Routing canon lives in the
hard rules below and in `src/earthdata_mcp/providers/router.py`. (PLAN.md, the old
phased spec, has been removed; ignore any lingering `PLAN.md §…` citations in
docstrings — they are stale references, not authority.)

## Hard rules (never violate)
- Use the official harmony-py client; do NOT hand-roll a Harmony client.
- Harmony is always tried first for any transform plan. Check service capability via
  get_services and CollectionCapabilities.find_service first: when ONE whole service
  satisfies the plan, pin it; when none does — including the union-trap case (the
  rolled-up top-level capability booleans are an unsatisfiable union; never trust
  them) and collections with no registered services — submit Harmony UNPINNED and let
  the server pick the chain. Never union across services to build a service_id.
  OPeNDAP is the worker's runtime fallback when a real Harmony submit fails; it is not
  a planning-time choice. The one non-Harmony shortcut is direct-S3 for a "data as-is"
  plan, and only when actually connected to the DAAC's S3 (in-region + enabled).
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