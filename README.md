# Earthdata Retrieval MCP Server

An [MCP](https://modelcontextprotocol.io) server for **retrieving, transforming, and
tracking the provenance of NASA Earthdata** — the half of the Earthdata workflow
that NASA's own [`nasa/earthdata-mcp`](https://github.com/nasa/earthdata-mcp)
deliberately stops short of.

NASA's server does CMR **discovery** and hands the agent an `earthaccess` snippet
("Access"); it never returns data. This server picks up there: an agent reasons in
**datasets, areas, jobs, and handles**, and the server drives CMR + Harmony +
OPeNDAP + AppEEARS internally to actually materialize analysis-ready output, record
where it came from, and let it be cited and re-built. CMR and Harmony are internal
dependencies, never exposed.

> Canon for our CMR code is CMR's public API + the UMM schemas — see
> [`docs/cmr_patterns.md`](docs/cmr_patterns.md). The full design rationale lives in
> [`Plan.md`](Plan.md); contributor rules in [`CLAUDE.md`](CLAUDE.md).

---

## What it does

- **Discovery → handles.** `search_datasets` / `describe_dataset` KMS-normalize a
  query, hit CMR, and mint opaque `dataset_` handles the agent reasons with.
- **Areas & coverage.** `define_area_of_interest` (place name, bbox, GeoJSON, HUC,
  FIPS) plus fast metadata-only coverage/availability/size checks.
- **Durable retrieval.** `retrieve_data` / `retrieve_subset` / `retrieve_timeseries`
  plan a job, persist it to Postgres, and return a pollable `job_` handle + a
  pending `obs_` result handle. An out-of-process **Arq worker** drives
  submit → poll → materialize and **resumes in-flight jobs across restarts** — no
  in-memory background tasks. Routing is capability-gated: a request goes to the
  single Harmony service that satisfies *the whole plan* (never a union of
  services), or to OPeNDAP / AppEEARS, or fails fast — never a blind "fall back to
  Harmony."
- **Format by shape.** Gridded output → **Zarr**; point/area samples (AppEEARS) →
  **Parquet**. Storage is pluggable behind a `StorageBackend` (local filesystem by
  default, S3 optional — config only).
- **Preview & transform.** GIBS previews, structural summaries, statistics; subset /
  reproject / resample / convert / align, each recording provenance edges.
- **Provenance & citations.** `get_provenance` returns a result's lineage (a
  recursive-CTE ancestry graph + first-class events) keyed to the **durable request
  spec**, never an ephemeral staged-output URL. `cite_dataset` returns the official
  DOI + formal citation strings straight from CMR's records.

The full v1 surface is **22 tools** (see [`Plan.md`](Plan.md) §7).

---

## Architecture at a glance

```
agent ──MCP──> server.py (FastMCP, 22 tools)
                  │
   ┌──────────────┼─────────────────────────────────────────┐
   │              │                                          │
 providers/    workspace/                                  jobs/
  cmr          store (handles, workspace isolation)         models + crud (durable `jobs` table)
  harmony      provenance (spec-keyed DAG, recursive CTE)   worker (Arq: submit→poll→materialize,
  opendap                                                          restart-resume)
  appeears   storage/ (StorageBackend: local default | s3)
  router     catalog/ (KMS keyword normalization, thin enrichment)
  ratelimit  (per-provider token buckets)
```

- **Postgres + PostGIS** is the source of truth for handles, jobs, and provenance.
- **Redis** backs the Arq worker queue.
- **Local filesystem** is the default materialization store (`./data`); S3 is one
  config flag away.

---

## Prerequisites

- **Docker + Docker Compose** (the supported path — brings up Postgres/PostGIS,
  Redis, the server, and the worker).
- A **NASA Earthdata Login (EDL)** account for any real retrieval — register at
  <https://urs.earthdata.nasa.gov>. Discovery/coverage (CMR-only) needs no auth.

For local (non-Docker) development you additionally need Python **3.13+**,
[`uv`](https://docs.astral.sh/uv/), and reachable Postgres/PostGIS + Redis.

---

## Quick start (Docker Compose)

```bash
# 1. Clone
git clone https://github.com/TPatel1208/harmony-retrieval-mcp.git
cd harmony-retrieval-mcp

# 2. Configure environment
cp .env.example .env
#    Edit .env and set your Earthdata Login credentials (see "Configuration" below).
#    Discovery works without them; retrieval (Harmony/OPeNDAP/AppEEARS) does not.

# 3. Build and start the stack (db + redis + mcp + worker)
docker compose up -d --build

# 4. Create the database schema (idempotent; safe to re-run)
docker compose exec mcp python -c "
import asyncio
from earthdata_mcp.db import create_engine
from earthdata_mcp.workspace import create_schema
from earthdata_mcp.jobs.models import create_jobs_schema
async def go():
    engine = create_engine()
    await create_schema(engine)        # handles + provenance tables
    await create_jobs_schema(engine)   # durable jobs table
    await engine.dispose()
asyncio.run(go())
print('schema ready')
"

# 5. Smoke-test: the server imports cleanly and exposes 22 tools
docker compose exec mcp python -c "
import asyncio; from earthdata_mcp.server import mcp
print('tools:', len(asyncio.run(mcp.list_tools())))"
```

The `worker` service starts automatically and runs
`arq earthdata_mcp.jobs.worker.WorkerSettings`; it reclaims any non-terminal job on
boot, so retrievals survive a restart.

### Running the MCP server

**stdio (default).** The container keeps itself alive (`sleep infinity`) so you can
`exec` into it:

```bash
docker compose exec mcp python -m earthdata_mcp.server
```

Point your MCP client at that process (stdio transport). The server import has **no
DB/network/credential side effects** — every tool builds its dependencies lazily on
first call.

**HTTP (cross-container consumers).** Set `EARTHDATA_MCP_TRANSPORT=http` in `.env`
and the `mcp` service *becomes* the server process — `docker compose up` yields a
working streamable-HTTP endpoint at `http://mcp:${EARTHDATA_MCP_PORT:-8765}/mcp`
instead of the idle placeholder. Set `EARTHDATA_MCP_TOKEN` and every request needs
`Authorization: Bearer <token>` (a missing/wrong token gets a bare 401); leave it
unset to serve unauthenticated (fine for a single private-network lab box, not
beyond it).

#### Attaching a second stack (e.g. Talking-to-Air)

This stack owns the shared network (`earthdata_net`) and materialization volume
(`earthdata_data`) — both get fixed names and are created automatically the first
time you run `docker compose up` here, no extra step needed on this side.

A consumer running as its own, independent compose stack declares both as
**external** (they already exist — this stack created them) and joins them on the
service that talks to this server:

```yaml
services:
  tta-backend:
    # ...
    environment:
      MCP_URL: http://mcp:8765/mcp
      MCP_TOKEN: ${EARTHDATA_MCP_TOKEN}
    networks:
      - earthdata_net
    volumes:
      - earthdata_data:/data   # same absolute path as this server's EARTHDATA_DATA_DIR

networks:
  earthdata_net:
    external: true

volumes:
  earthdata_data:
    external: true
```

Because both stacks mount the same volume at `/data`, a `file://` URI returned by
`export_result` resolves identically inside the consumer container — opening it is
a plain filesystem read, no download step.

**Security posture.** This is a lab-box design: the bearer token is a tripwire (it
stops an open port from being a free-for-all, and gets consumers sending
credentials from day one) — it is **not** real authentication. Real authn/OAuth,
an S3 backend with presigned URLs, and per-workspace quotas are deferred to a
pre-hosting hardening phase; do not expose this beyond a private network as-is.

---

## Configuration

All settings come from environment variables (loaded from `.env` by Compose). See
[`.env.example`](.env.example) for the full list; the essentials:

| Variable | Default | Purpose |
|---|---|---|
| `EARTHDATA_TOKEN` | — | EDL bearer token (preferred). Required for Harmony/OPeNDAP/AppEEARS retrieval and downloads. |
| `EDL_USERNAME` / `EDL_PASSWORD` | — | EDL credentials (alternative to a token; `earthaccess`/`harmony-py` can also use them). |
| `DATABASE_URL` | `postgresql+asyncpg://…@db:5432/earthdata_mcp` | Postgres + PostGIS DSN (set by Compose). |
| `REDIS_URL` | `redis://redis:6379/0` | Arq worker broker (set by Compose). |
| `EARTHDATA_STORAGE` | `local` | `local` or `s3://bucket/prefix`. |
| `EARTHDATA_DATA_DIR` | `/data` (Compose) / `./data` | Local materialization root (Zarr/Parquet land here). |
| `EARTHDATA_CACHE_MAX_BYTES` | `5368709120` (~5 GiB) | Materialization cache eviction cap. |
| `LOG_LEVEL` | `INFO` | Logging verbosity. |
| `EARTHDATA_MCP_TRANSPORT` | `stdio` | `stdio` or `http` (streamable HTTP). See "Running the MCP server" above. |
| `EARTHDATA_MCP_HOST` / `EARTHDATA_MCP_PORT` | `0.0.0.0` / `8765` | Bind address for `http` transport. |
| `EARTHDATA_MCP_TOKEN` | — | Static bearer token required on every HTTP request when set. |

Per-provider rate limits (token buckets, generous defaults) are also configurable:
`CMR_RATE_PER_SEC` (20), `HARMONY_RATE_PER_SEC` / `APPEEARS_RATE_PER_SEC` /
`OPENDAP_RATE_PER_SEC` (10).

### Getting an Earthdata Login token

1. Create an account at <https://urs.earthdata.nasa.gov>.
2. Generate a token under **Profile → Generate Token**.
3. Put it in `.env` as `EARTHDATA_TOKEN=…`, then recreate the app containers so they
   pick it up: `docker compose up -d --force-recreate mcp worker`.

> EDL **production** and **UAT** are separate systems with separate tokens. A
> production token will not authenticate against `*.uat.earthdata.nasa.gov`.

### Optional: S3-compatible storage (MinIO)

The default local-filesystem backend needs no cloud account. To exercise the
object-store backend, start the bundled MinIO and point storage at it:

```bash
docker compose --profile s3 up -d minio
# then set EARTHDATA_STORAGE=s3://<bucket>/<prefix> in .env and recreate mcp/worker
```

---

## Running the tests

Tests run inside the `mcp` container against the live Postgres/Redis in the stack.

```bash
# Unit tests (no network)
docker compose exec mcp pytest tests/unit/ -v --tb=short

# Integration tests (real Postgres job table + worker lifecycle; provider faked)
docker compose exec mcp pytest tests/integration/ -v
```

**Live tests** (`@pytest.mark.live`) hit real NASA services and need EDL
credentials; they are skipped otherwise. Pass the token into the container env:

```bash
# Full durable retrieval against real Harmony (uses a UAT EEDTEST collection)
docker compose exec -e EARTHDATA_TOKEN mcp \
  pytest -m live tests/live/test_full_retrieval.py -v
```

Other live suites exercise the Harmony submit, OPeNDAP subset, and AppEEARS point
paths (`tests/live/`).

---

## Tool surface (v1)

```
Discovery     search_datasets · describe_dataset
Area          define_area_of_interest
Coverage      check_coverage · check_availability · inspect_granules · estimate_retrieval_size
Retrieval     retrieve_data · retrieve_subset · retrieve_timeseries ·
              get_retrieval_status · cancel_retrieval
Preview       preview_dataset · summarize_dataset · inspect_statistics
Transform     subset · reproject · resample · convert_format · align
Provenance    get_provenance · cite_dataset
```

No analysis tools (correlation, trend, anomaly, hotspot, risk, narrative) — by
design.

---

## Local development (without Docker)

```bash
uv sync --extra dev          # install deps incl. test tooling
# Ensure Postgres+PostGIS and Redis are reachable and DATABASE_URL/REDIS_URL point at them.
uv run python -c "
import asyncio
from earthdata_mcp.db import create_engine
from earthdata_mcp.workspace import create_schema
from earthdata_mcp.jobs.models import create_jobs_schema
async def go():
    e = create_engine(); await create_schema(e); await create_jobs_schema(e); await e.dispose()
asyncio.run(go())"
uv run pytest tests/unit -q          # run unit tests
uv run ruff check src tests          # lint
uv run python -m earthdata_mcp.server  # run the MCP server (stdio)
uv run arq earthdata_mcp.jobs.worker.WorkerSettings  # run the worker
```

---

## Project layout

```
src/earthdata_mcp/
  server.py            FastMCP app; registers the 22 tools
  config.py            env-driven settings
  db.py                async engine / session factory
  providers/           cmr, harmony, opendap, appeears, router, auth, _capabilities, ratelimit
  workspace/           handles, store (isolation), provenance (spec-keyed DAG)
  jobs/                durable jobs table, state machine, Arq worker (restart-resume)
  storage/             StorageBackend + local / s3 implementations
  catalog/             KMS keyword normalization, thin advisory enrichment
  tools/               discovery, understanding, area, coverage, retrieval,
                       preview, transform, provenance
tests/                 unit/ · integration/ · live/
docs/                  cmr_patterns.md · tta_audit.md
Plan.md                design spec (single source of truth)
CLAUDE.md              contributor hard rules
```

---

## License

See repository for licensing. This is a research-project effort intended to be
maintainable by engineers who did not write it; please keep the hard rules in
[`CLAUDE.md`](CLAUDE.md) intact.
