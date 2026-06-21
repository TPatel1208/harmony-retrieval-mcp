# TTA reuse audit (Phase 0)

**Status:** complete · **Gate:** explicit reuse/rewrite decision per component.
**Scope:** analysis only — no provider or tool code this session (per
`prompts/01_phase0_tta_audit.md` and CLAUDE.md working discipline).

PLAN.md §5 makes the whole schedule contingent on this audit: v2 leaned on "port
TTA's X" without examining TTA's quality. This decides, per component, whether we
**reuse** TTA code or **rewrite** it using TTA as a reference, on three axes:
test coverage, license compatibility, and coupling.

## TTA source availability

**TTA source IS available.** It is the "Talking to Air" project; the canonical,
richest copy is at `C:\Rutgers\Semester 4\tta-test\`, which contains all seven
audited components, a real pytest suite, and a docker-compose stack. Partial
copies also exist under `C:\Rutgers\Semester 3\Talking to Air*` and
`C:\Rutgers\Semester 4\Talking-to-Air`; `tta-test` is treated as canonical and is
the reference for every component below. Because the source was available, no
component is marked "rewrite (reference unavailable)".

## License

TTA carries **no `LICENSE` file** and `pyproject.toml` declares no license field.
Per the author, **TTA is wholly first-party (user-authored)**: there is no
upstream license constraint and it is freely relicensable under whatever the new
repo adopts. **License compatibility is not a blocker for any component** and
needs no citation. The only residual (non-blocking) action item is to add a
`LICENSE` file to this repo when it is published.

## Findings

Evidence gathered by reading each component's imports, the test suite,
`docker-compose.yml`, and `pyproject.toml`.

| Component | Path in `tta-test` | LOC | Test coverage | Coupling |
|---|---|---|---|---|
| async_harmony_service | `Backend/services/async_harmony_service.py` | 735 | Yes — `tests/test_async_harmony_service.py` (186L) + `tests/test_data_routing.py` | `config.settings`, `utils.streaming` (SSE: `emit_status`, `current_thread_id`), `utils.metrics`, `utils.earthaccess_client`. Already wraps the official harmony-py `Client`, buried in streaming/metrics glue. |
| opendap_fetch_service | `Backend/services/opendap_fetch_service.py` | 377 | **None** (no test references it) | Only `config.settings` (+ numpy/xarray/requests/tenacity). No LangGraph/SSE/app-state. Cleanly liftable. |
| cache_manager | `Backend/preprocessing/cache_manager.py` | 333 | Yes — `tests/test_cache_manager.py` (180L, fakes repositories) | `utils.geo_utils`, `utils.metrics`, xarray. Zarr/bbox-specific cache with a repository pattern — **not** an opaque-key backend. |
| dataset_parser | `Backend/preprocessing/dataset_parser.py` | 412 | **None** | netCDF4/numpy/pandas/xarray only — **zero app coupling**, fully liftable. |
| earthaccess_client | `Backend/utils/earthaccess_client.py` | 68 | Yes — `tests/test_earthaccess_client.py` (86L) | Only `config.settings`. Thin, thread-safe EDL bootstrap. |
| utils/db | `Backend/utils/db.py` | 184 | Yes — `tests/test_db_pool.py` (85L) | **`langgraph.checkpoint.postgres.aio.AsyncPostgresSaver`** + **psycopg / psycopg_pool**. Our stack is SQLAlchemy + asyncpg. |
| docker-compose | `tta-test/docker-compose.yml` | — | N/A (infra) | Bundles React/Vite/nginx frontend, frontend-test, LangChain backend, GROQ/JWT secrets; PostGIS 16-3.4 + named volumes. **No worker service.** |

## Decisions

| Component | Decision | Justification |
|---|---|---|
| **async_harmony_service** | **Replace with `harmony-py`** | Already decided in PLAN.md §2/§8; recorded here. It is 735 lines of SSE/metrics glue around harmony-py — a hand-maintained client is a 5-year liability. Keep only the `TransformSpec → harmony.Request` mapping idea as reference. |
| **opendap_fetch_service** | **Reuse as reference (rewrite-light, add tests)** | Low coupling (only `config.settings`) and cleanly liftable, but **no test coverage** — port the fetch logic, swap config access, add unit tests. |
| **cache_manager** | **Rewrite behind `StorageBackend` (cache_manager as reference)** | Abstraction mismatch: it is a Zarr/bbox-specific cache, while PLAN.md §4.4 requires an opaque-key `put/get/delete/list/stat` backend (local FS default). Mine its eviction/index logic and its test fakes. |
| **dataset_parser** | **Reuse as reference (clean lift, add tests)** | Zero app coupling (fully liftable), but 412 LOC with **no tests** and our parsing needs differ — lift selectively and add coverage. |
| **earthaccess_client** | **Reuse (light adaptation)** | 68 lines, tested, only `config.settings` coupling; Phase 4 EDL auth builds directly on it. Adapt config access, keep the thread-safe EDL bootstrap. |
| **utils/db** | **Rewrite (reference only)** | Two blockers: the LangGraph checkpointer must be stripped (CLAUDE.md / PLAN.md §8), and it is psycopg-based while our stack is SQLAlchemy + asyncpg. The pool-lifecycle pattern informs but does not port. |
| **docker-compose** | **Adapt / rewrite** | Keep PostGIS 16-3.4 + named-volume pattern; drop frontend/nginx/LangChain/GROQ/JWT; add a worker service (Arq + Redis per stack) and a local storage volume (optional MinIO/S3 profile). |

**Net:** the audit confirms PLAN.md's instinct. The two components with **no
tests** (opendap, dataset_parser) and the two with a **stack/abstraction
mismatch** (utils/db, cache_manager) are reference-grade, not drop-in. Only
`earthaccess_client` is a near-clean reuse. This supports the "+30–50% if
research-grade" caveat in PLAN.md §9 landing on the OPeNDAP / cache / parser / db
lines of the estimate.

## Action items for Phase 1

- Add a `LICENSE` file to this repo (non-blocking; first-party code).
- Storage: implement the `StorageBackend` interface (PLAN.md §4.4); cache_manager
  is reference, not a port.
- DB: build the SQLAlchemy + asyncpg layer fresh; do **not** carry the psycopg
  pool or any LangGraph checkpointer.
- docker-compose: keep PostGIS + volumes; add the worker (Arq + Redis); drop the
  frontend and all LLM/JWT/GROQ config.
- Tests: when porting opendap_fetch and dataset_parser logic, add the unit tests
  they currently lack.
