# Earthdata MCP Server — Implementation Plan v3

Revised after a principal-engineer design review of v2. v2's strategic bet was
right: NASA's `nasa/earthdata-mcp` already does CMR discovery and **deliberately
stops at "Access"** (it returns `earthaccess` snippets, never data). The white
space is **retrieval, handles, materialization, transformation, and
provenance**. v3 keeps that thesis intact and fixes the three structural defects
the review found:

1. The highest-value capability (Harmony retrieval) was the least-tested and
   latest-integrated thing in the schedule.
2. Auth was sequenced *after* the retrieval engine that cannot function without it.
3. The async job model — the real operational core — was hand-waved, and the
   effort estimate (~7 days) was off by a large multiple.

This plan is written for a **research-project server that must survive 5+ years
in the hands of engineers who did not write it.** Every "clever" shortcut that
trades long-term maintainability for short-term speed has been removed.

---

## 0. Reality check on our dependencies (read first)

Before designing around NASA's ecosystem, we verified it. The facts below are
load-bearing and dated; **re-verify at implementation time.**

**NASA's MCP server (`nasa/earthdata-mcp`)**
- Deployed at `https://cmr.earthdata.nasa.gov/mcp/v1` over **Streamable HTTP**.
- Tools: `get_keywords`, `get_collections`, `get_granules`, `get_services`,
  `get_tools`, `get_citations`, `get_variables`.
- Enforces a **Discover → Verify → Access** workflow and stops at handing the
  agent an `earthaccess` snippet.
- **It is young and mid-refactor.** The repo is small (~10 stars, a handful of
  contributors) and is *actively deprecating* its old embedding/ingest pipeline
  in favor of direct real-time CMR calls. **Consequence:** their source is a
  *worked example*, not a stable contract. We do not pin our design to their
  code; we pin it to CMR's public API and the UMM schemas, and cite a specific
  commit of their repo where we borrow a pattern.

**Harmony** (`https://harmony.earthdata.nasa.gov`)
- OGC-Coverages / WMS APIs; requests run as **jobs**; results stage to NASA S3
  with **signed URLs or temporary credentials and an expiration** (~30 days in
  NASA's own examples).
- Service support is **per-collection and heterogeneous** — many collections
  support no Harmony service at all. Capability must be checked, never assumed.
- Single-product requests run **synchronously by default**; `forceAsync=true`
  forces the job path.
- An official client, **`harmony-py`**, already handles request construction,
  the EDL OAuth session, polling, and Zarr output. We use it.

**Auth:** Earthdata Login (EDL) is required for any Harmony transform or data
download. `earthaccess` manages EDL credentials, tokens, and in-region S3
access.

---

## 1. The composition decision (unchanged thesis, sharper edges)

We own the critical path; we delegate nothing at runtime.

- **Internal CMR access → call CMR directly.** Our retrieval pipeline needs
  granule metadata and service capabilities to drive Harmony, so
  `providers/cmr.py` hits `cmr.earthdata.nasa.gov` directly. Canon = CMR's
  public API docs + UMM schemas; NASA's MCP repo is a pinned reference only.
- **KMS vocabulary → mirror NASA's approach.** Keyword normalization
  ("rain" → "PRECIPITATION AMOUNT") is genuinely hard; we mirror their KMS
  lookup rather than inventing our own.
- **Consumer-facing discovery → a minimal handle-minting layer.** We expose
  just enough discovery to give the agent one clean, handle-based interface
  (see §6, Phase 5 — trimmed hard from v2). We do **not** try to out-search CMR,
  and we do **not** re-expose NASA's full tool surface.

The review strengthened, not weakened, this choice: because NASA's server is
young and refactoring, runtime delegation (the rejected Option A) would have
been even riskier than v2 assumed. NASA's deployed server remains available to
consumers who want pure discovery; the two coexist.

**The agent connects to *our* server and reasons in datasets, jobs, and
handles. CMR and Harmony are internal dependencies, never exposed.**

---

## 2. What changed from v2

| Area | v2 | v3 | Why |
|---|---|---|---|
| Harmony client | Port TTA's bespoke `async_harmony_service.py` | **Wrap official `harmony-py`** | A hand-rolled client is a 5-year liability the moment Harmony's API moves |
| EDL auth | Phase 8 (last) | **Phase 4 (before first real submit)** | Retrieval cannot work without it; late auth means the core path is never exercised until the end |
| Async job model | "background task materializes → ready" (hand-waved) | **Durable job table + worker + restart-resume + state machine** | This is the real operational core, not an afterthought |
| Provider Protocol | One `DataProvider`; CMR's `retrieve` raises `NotImplementedError` | **Split `MetadataProvider` / `RetrievalProvider`** | A core method that throws is a leaky abstraction |
| Routing | "Harmony-capable → Harmony; else Harmony fallback" | **`ServiceCapability`-gated; fail fast, no Harmony fallback** | Falling back into Harmony for an unserviceable collection fails opaquely at submit time |
| Service detection | `'harmony' in str(svcs).lower()` | **Merged `CollectionCapabilities` (UMM-C + per-service UMM-S); `find_service` matches one whole service, never the rolled-up union** | Top-level capability flags are an unsatisfiable union — a real TEMPO L2 record advertises bbox+png that no single service can do |
| Output format | "materialize to Zarr" (always) | **Zarr for gridded, Parquet/CSV for tabular (AppEEARS point)** | Forcing point time-series into a cube is an impedance mismatch |
| Storage backend | Unspecified ("port cache_manager") | **Pluggable `StorageBackend`: local filesystem default, object store optional; eviction policy + size metric either way** | Research deployment is single-node; local disk is the right default, with cloud one config flag away |
| Provenance source | Output URL | **Durable request spec (re-materializable); URLs expire** | Harmony staged outputs expire (~30 days); a lineage graph of dead links is worse than none |
| Discovery tools | 7 tools | **2 handle-minting tools in v1; rest deferred** | The agent + NASA's tools already compose the rest |
| Enrichment catalog | ~30 products, curated QA notes | **≤10 products, advisory/non-authoritative, owner+freshness fields; pull QA from UMM-Var** | Hand-curated science notes are a maintenance time-bomb |
| Integration test | Final gate skips Harmony (`-k "not harmony"`) | **Credentialed live Harmony test, nightly CI** | The highest-value path must have a real end-to-end test |
| TTA reuse | Assumed clean | **Gated on a reuse audit (coverage/license/coupling)** | The estimate's credibility depends on TTA's actual quality |
| Effort | ~7 days | **~18–25 engineer-days (~3–4 weeks)** | Honest accounting of the retrieval/provenance core |

What did **not** change: the handle abstraction, provenance as a first-class
concern, owning the critical path, reusing (not reimplementing) CMR discovery
and KMS, and the phase-gated commit-on-green discipline. Those were the strong
decisions and they stay.

---

## 3. CLAUDE.md — add this section

```markdown
## Relationship to nasa/earthdata-mcp

NASA maintains a CMR discovery MCP server at https://cmr.earthdata.nasa.gov/mcp/v1
(Streamable HTTP). Source: github.com/nasa/earthdata-mcp. It does discovery and
stops at "Access" (returns earthaccess snippets, never data). We build the
retrieval/transform/provenance half.

Canon for our CMR code is CMR's own public API + UMM schemas — NOT NASA's MCP
repo, which is young and actively refactoring. When we borrow a query/pagination
pattern from their tools/, cite the exact commit and re-verify it; do not treat
their current HEAD as a contract.

DO reuse: KMS keyword normalization (mirror get_keywords); citation records
(get_citations pattern).

DO NOT: reimplement CMR discovery, add analysis tools (correlation, trend,
anomaly, hotspot, risk, narrative), or try to out-search CMR. Our discovery is a
thin handle-minting layer.

Hard rules for the retrieval core:
- Use the official harmony-py client; do not hand-roll a Harmony client.
- Check service capability via get_services before any Harmony submit. Never
  "fall back" to Harmony for a collection that has no Harmony service.
- All retrieval is a durable job: persisted state, resumable on restart. No
  in-memory background tasks for anything that matters.
- Provenance records the request SPEC (re-materializable), never an ephemeral
  staged-output URL.
- The critical path (retrieval → Harmony) calls CMR/Harmony directly and must
  not depend on NASA's deployed MCP server being up.
```

---

## 4. Cross-cutting design (the parts v2 left implicit)

### 4.1 Provider abstraction

Two Protocols, not one:

```python
class MetadataProvider(Protocol):
    async def search(self, query: SearchSpec) -> list[CollectionMeta]: ...
    async def check_availability(self, spec: AvailabilitySpec) -> Availability: ...
    def capabilities(self) -> ProviderCapabilities: ...

class RetrievalProvider(Protocol):
    def can_handle(self, plan: RetrievalPlan) -> bool: ...   # capability gate
    async def submit(self, plan: RetrievalPlan) -> JobRef: ...
    async def poll(self, job: JobRef) -> JobStatus: ...
    async def materialize(self, job: JobRef) -> MaterializedResult: ...
```

- `CMRProvider` implements **`MetadataProvider` only** — no throwing stubs.
- `HarmonyProvider`, `OPeNDAPProvider`, `AppEEARSProvider` implement
  **`RetrievalProvider`**.
- The router composes a metadata provider + the first retrieval provider whose
  `can_handle(plan)` is true.

### 4.2 Capability model (drives routing AND the gate)

Collection metadata is **two layers from two fetches**, merged into one view.
This is not optional structure — it's what real records force (see the worked
TEMPO example below).

**Layer 1 — UMM-C (`get_collections` / collection fetch).** What the data *is*
and how to get it *raw*: processing level (→ output shape), native formats,
spatial/temporal extent, DOI/citation, science keywords, maturity, and
`DirectDistributionInformation` (in-region S3 → you can fetch granules directly,
no service needed). Says nothing reliable about subsetting.

**Layer 2 — `get_services` (UMM-S / Harmony capabilities).** What
transformations are possible and *through which service*. Per-service, typed —
never the rolled-up booleans (see the trap below).

```python
@dataclass(frozen=True)
class ServiceCapability:
    service_name: str
    concept_id: str
    subset_bbox: bool
    subset_variable: bool
    subset_temporal: bool
    subset_shape: bool
    subset_dimension: bool
    concatenate: bool
    reproject: bool
    output_formats: frozenset[str]        # {"application/netcdf", "image/png", ...}

@dataclass
class CollectionCapabilities:
    concept_id: str
    short_name: str
    processing_level: str                 # "2" -> swath, "3"/"4" -> grid (heuristic)
    output_shape: Literal["grid", "swath", "point"]
    native_formats: frozenset[str]        # UMM-C ArchiveAndDistributionInformation
    direct_s3: S3DirectAccess | None      # UMM-C DirectDistributionInformation
    services: list[ServiceCapability]     # get_services, PER-service
    capabilities_version: str             # e.g. "2" — part of the cache key
    advisory: list[str]                   # pulled from UMM-C, not hand-curated

    def find_service(self, plan: RetrievalPlan) -> ServiceCapability | None:
        """A SINGLE service must satisfy the ENTIRE plan. Never union across services."""
        for s in self.services:
            if plan.needs_bbox      and not s.subset_bbox:      continue
            if plan.needs_variable  and not s.subset_variable:  continue
            if plan.needs_temporal  and not s.subset_temporal:  continue
            if plan.needs_shape     and not s.subset_shape:     continue
            if plan.needs_reproject and not s.reproject:        continue
            if plan.output_format not in s.output_formats:      continue
            return s
        return None
```

**The trap (why per-service is mandatory): the rolled-up flags are a union, and
the union lies.** A real TEMPO_NO2_L2 capabilities record advertises
`bboxSubset: true` and `outputFormats: [netcdf, csv, png]` at the top level, but
its two services are disjoint: `l2-subsetter-batchee-stitchee-concise` does
bbox/variable/temporal/shape/concatenate and outputs **netcdf, csv** (no png);
`asdc/imagenator_l2` does variable subsetting only and outputs **png** (no
bbox). So "bbox subset → png" is satisfiable by *neither* service, yet the
top-level booleans imply it's fine. A router that trusts the union builds a
request no service can fulfill and fails opaquely at submit time. `find_service`
matches one whole service or returns `None`.

**Routing decision tree:**
1. Transforms needed and `find_service(plan)` returns a service → Harmony with
   *that named service*.
2. "Data as-is" and `direct_s3` present (in-region) or native download is
   acceptable → direct-fetch granules, **skip Harmony**. (A COMPLETE gridded
   netCDF-4 L3 collection with an S3 prefix is this case.)
3. Gridded variable/bbox subset with an OPeNDAP service → OPeNDAP path.
4. Nothing satisfies → `NotRetrievable(reason=..., available=[...])` at
   **planning time**, returning what each service *can* do so the agent can
   relax the request ("png only without a bbox; want full-scene png or a bbox
   subset as netCDF?"). Never "fall back" into Harmony.

**Output shape** comes from processing level (heuristic, refined by service
dimension hints): L3/L4 → grid → Zarr; L2 → swath → the concatenate/`stitchee`/
extend machinery matters on long time-series so you don't get thousands of tiny
files. **Maturity/advisory** notes are read from UMM-C (`Purpose`,
`CollectionProgress`, `StandardProduct`) — e.g. "PROVISIONAL; see known issues" —
not hand-curated, reinforcing §4.4's pull-from-UMM-first rule.

`CollectionCapabilities` is cached per `concept_id`; `capabilities_version` and
the chosen service's version feed the materialization cache key (§4.4), since
service outputs change across versions.

### 4.3 Durable async job model (the operational core)

This is the thing v2 hand-waved. It is specified here because everything
downstream depends on it.

- **`jobs` table (Postgres):** `job_id`, `job_handle` (`job_`), `obs_handle`
  (the eventual result), `provider`, `request_spec` (JSONB — the durable,
  re-materializable spec), `state` (`pending|submitted|running|materializing|
  ready|failed|expired|cancelled`), `provider_job_url`, `progress`,
  `output_expires_at`, `error`, timestamps.
- **State machine** is explicit and the only legal transitions are encoded in
  one place.
- **Worker:** an out-of-process worker (Arq/Celery/RQ — pick one in Phase 1)
  drives submit → poll → materialize. The MCP server process never owns a
  long-lived background task.
- **Restart-resume:** on startup the worker reclaims any job not in a terminal
  state and resumes polling from `provider_job_url`. No job is lost to a restart.
- **Sync path:** if Harmony returns a single product synchronously, the worker
  records `ready` immediately; we set `forceAsync=true` for uniformity unless a
  caller opts out.
- **Handles:** a retrieval mints a `job_` handle immediately (pollable,
  cancellable) and an `obs_` handle that resolves once `ready`. The agent can
  reference, poll, and cancel the job distinctly from its result.

### 4.4 Storage and materialization

- **Format by result shape:** gridded → **Zarr**; point/tabular (AppEEARS) →
  **Parquet** (+ CSV on request). Never force tabular through a cube.
- **Pluggable backend (`StorageBackend`).** One small interface
  (`put` / `get` / `delete` / `list` / `stat` over an opaque key), two
  implementations:
  - **`LocalFilesystemBackend` — the default.** Materializes under a configured
    data root (e.g. `EARTHDATA_DATA_DIR=./data`). This is the right choice for a
    single-node research deployment: no cloud account, no credentials, no S3
    emulator in dev. Zarr-on-local-FS and Parquet-on-local-FS are first-class.
  - **`ObjectStoreBackend` — optional, opt-in.** S3 or S3-compatible (MinIO),
    selected by config (`EARTHDATA_STORAGE=s3://bucket/prefix`) when you later
    need horizontal scaling or shared multi-node access. No code change — just
    config — because everything above the backend speaks the same interface.
  - Selection is config-only (`EARTHDATA_STORAGE=local|s3://...`); the rest of
    the system never imports a concrete backend. Keeping this seam from day one
    is the whole point — local now, cloud later, zero rework.
- **We re-host the analysis-ready output** we materialize (durable, under
  whichever backend is configured), and we **store the request spec** so any
  expired or evicted result can be rebuilt.
- **Eviction (applies to both backends):** size/TTL policy on the
  materialization cache and a cache-size metric. On local FS this matters even
  more than on S3 — an unbounded cache fills the dev machine's disk and takes
  the server down — so a sane default cap (e.g. a few GB) ships enabled.
- **Cache key:** `(short_name, version, aoi, time_range, variables, transforms,
  service_version)`. `service_version` is in the key because Harmony service
  outputs change across versions.

### 4.5 Provenance

- Lineage edges are keyed to **durable request specs and granule IDs**, never to
  ephemeral staged-output URLs.
- Stored in Postgres; ancestry queries use a **recursive CTE**, written
  deliberately and **tested on deep (≥20-hop) graphs**, not discovered later.
- `expired`/`re-materialized` are first-class provenance events.

### 4.6 Workspace scoping

`workspace_id` carries **ownership and isolation** semantics from day one:
every handle belongs to a workspace, every tool call is scoped to one, and
cross-workspace reads are denied. Auth identity (Phase 4) maps to workspace
ownership. This is defined before any multi-user deployment, not after.

---

## 5. Phase 0 — TTA reuse audit (gate before scheduling)

**The schedule below is contingent on this.** v2 leaned heavily on "port TTA's
X" without examining TTA. Before committing dates, audit each TTA component we
intend to reuse:

```
For each of: async_harmony_service, opendap_fetch_service, cache_manager,
dataset_parser, earthaccess_client, utils/db, docker-compose —
  - test coverage (is there any?)
  - license compatibility with our OSS license
  - coupling (can it be lifted without dragging LangGraph/SSE/app state?)
Produce docs/tta_audit.md with a reuse/rewrite decision per component.
```

If a component is research-grade, its "port" line in the estimate is 2–3× and
should become a "rewrite using TTA as reference." Note: `async_harmony_service`
is already slated for **replacement by `harmony-py`**, so the audit mainly
matters for OPeNDAP, cache, parser, auth, and db.

**Gate:** `docs/tta_audit.md` exists with an explicit decision per component.

---

## 6. Phases

Phase-gated, one phase per checkpoint, commit only on green. Gates that touch the
retrieval core now require **real** (credentialed, opt-in) execution, not just
mocked CMR.

### Phase 1 — Scaffold and infrastructure (1 d)

Server skeleton, config, Postgres + PostGIS, **pluggable storage backend wired
(local filesystem default; object store behind the same interface, off by
default)**, **worker runtime chosen and wired** (Arq/Celery/RQ), Docker stack,
CLAUDE.md section. Drop v2's heavy catalog-search modules; keep
`catalog/enrichment.py` and `catalog/kms.py`.

```
providers/        cmr.py base.py harmony.py opendap.py appeears.py router.py auth.py
workspace/        models.py store.py provenance.py
jobs/             models.py worker.py state.py        # durable job model
storage/          backend.py local.py s3.py           # StorageBackend + 2 impls
catalog/
  ├── enrichment.py     # thin, advisory notes
  ├── kms.py            # KMS normalization (mirrors NASA)
  └── data/
      ├── concepts.yaml
      ├── variables.yaml   # owner + last_reviewed per entry
      └── products.yaml    # ≤10 products, advisory only
tools/            discovery.py understanding.py area.py coverage.py
                  retrieval.py preview.py transform.py provenance.py
tests/fixtures/   tempo_no2_l2_capabilities.json   # union-trap: 2 disjoint services
                  tempo_no2_l3_umm_c.json          # gridded, direct S3, no service
```

**Gate:** server imports clean; config + DB + worker smoke tests green; the
**`StorageBackend` round-trips (put/get/delete) against the local filesystem
backend** (the default — no cloud account needed to develop or test); Docker
stack up. A parametrized version of the same backend test runs against MinIO/S3
only when `EARTHDATA_STORAGE` points at one, so the cloud path is covered without
being required.

### Phase 2 — CMR access, CollectionCapabilities, KMS, thin enrichment (2 d)

**2.1** Read NASA's `get_collections`/`get_granules`/`get_variables`/
`get_services` at a **pinned commit**; write `docs/cmr_patterns.md` citing that
commit. Canon remains CMR API docs + UMM. (Research task, no provider code.)

**2.2** `providers/cmr.py` (`MetadataProvider`): `search_collections`,
`search_granules`, `get_variables`, `get_services`, `check_availability`. httpx +
tenacity (retry 5xx/timeout, never 4xx). Metadata only. The collection fetch must
surface UMM-C fields the capability merge needs: `ProcessingLevel`,
`ArchiveAndDistributionInformation` (native formats), `DirectDistributionInformation`
(in-region S3), `CollectionProgress`/`StandardProduct`/`Purpose` (maturity → advisory).

**2.3** `providers/_capabilities.py`: build the merged `CollectionCapabilities`
(§4.2) — UMM-C (Layer 1) merged with **per-service** `ServiceCapability` parsed
from `get_services` (Layer 2). Parse each service's own `capabilities` block;
**ignore the rolled-up top-level booleans** (they are an unsatisfiable union).
Expose `find_service(plan)` (one whole service or `None`) and `direct_s3`. This
is the gate's source of truth.

**2.4** `catalog/kms.py`: `normalize_keyword(term) -> list[str]`, mirroring
NASA's `get_keywords`; cache the KMS dump, refresh on schedule.

**2.5** `catalog/enrichment.py`: pull scale/offset/fill/QA from **UMM-Var
first**; the curated YAML adds only genuinely-additive notes for **≤10**
products, each with `owner` and `last_reviewed`. `enrich_collection` passes
through cleanly when uncurated and marks notes **advisory/non-authoritative** in
output.

**Gate:**
```bash
docker compose exec mcp pytest tests/unit/test_cmr.py tests/unit/test_kms.py \
  tests/unit/test_capabilities.py tests/unit/test_enrichment.py -v
# test_capabilities.py MUST include the union-trap fixture (saved real records):
#   - tests/fixtures/tempo_no2_l2_capabilities.json   (two disjoint services)
#   - tests/fixtures/tempo_no2_l3_umm_c.json           (gridded, direct S3, no service)
# Required assertions:
#   * find_service(bbox + png)        is None    # neither service does both
#   * find_service(bbox + netcdf)     == "l2-subsetter-batchee-stitchee-concise"
#   * find_service(variable + png)    == "asdc/imagenator_l2"
#   * L3 caps: output_shape == "grid", direct_s3 is not None, advisory mentions PROVISIONAL
#
# Real CMR + merged capability view (no auth needed):
docker compose exec mcp python -c "
import asyncio; from earthdata_mcp.providers.cmr import CMRProvider
p=CMRProvider()
caps=asyncio.run(p.collection_capabilities('C2565788901-LPCLOUD'))  # MOD13Q1
print(caps.output_shape, bool(caps.direct_s3), len(caps.services))
"
```

### Phase 3 — Workspace, handles, provenance (2 d)

`workspace/models.py` (handle types incl. **`job_`**), `workspace/store.py`
(Postgres, **workspace ownership/isolation**), `workspace/provenance.py` (durable
spec-keyed lineage, **recursive-CTE ancestry tested to ≥20 hops**, `expired`/
`re-materialized` events). The NASA server has no equivalent; this is our core.

**Gate:** handle round-trip, cross-workspace denial, deep-graph provenance, and
spec-based re-materialization stub all green.

### Phase 4 — Retrieval engine: auth + Harmony + capability-gated router (3 d)

**Auth moves here.** This phase is where the core first does something real.

**4.1** `providers/base.py`: the two Protocols (§4.1) + shared types.

**4.2** `providers/auth.py`: EDL via `earthaccess` — token/session lifecycle,
in-region S3 creds. Identity maps to workspace ownership.

**4.3** `providers/harmony.py`: **wrap `harmony-py`.** Our code is only
`TransformSpec → harmony.Request` mapping and the `on_progress` glue; `harmony-py`
owns request construction, EDL session, polling, Zarr. Submit only the service
returned by `CollectionCapabilities.find_service(plan)` — pass its `service_name`
explicitly so Harmony uses the matched service, never the wrong one.

**4.4** `providers/router.py`: implements the §4.2 decision tree.
`find_service(plan)` → Harmony with that named service; else `direct_s3`/native
→ direct fetch (skip Harmony); else OPeNDAP if present; else
`NotRetrievable(reason, available=[...])` at planning time. **No Harmony
fallback, no unioning across services.**

**4.5** Tests: router decision tree incl. the **union-trap fixture from Phase 2**
(assert `bbox + png` → `NotRetrievable` with `available` listing both services'
real capabilities; `bbox + netcdf` → subsetter; the L3 direct-S3 case → direct
fetch, no Harmony submit); Harmony service-name mapping; mocked poll/materialize;
on_progress. **Plus a live, credentialed Harmony submit** test, marked
`@pytest.mark.live`, run in **nightly CI** (not on every commit), against a small
known-serviceable collection.

**Gate:**
```bash
docker compose exec mcp pytest tests/unit/test_router.py tests/unit/test_providers/ -v
# Nightly / on-demand, requires EDL creds:
EDL_TOKEN=... docker compose exec mcp pytest -m live tests/live/test_harmony_submit.py -v
```

### Phase 5 — Discovery and understanding tools (trimmed) (1 d)

**v1 ships only the two handle-minting tools.** The rest are deferred — a capable
agent composes them from these primitives plus NASA's server.

**5.1** `tools/discovery.py`: `search_datasets(query, filters, workspace_id)` —
KMS-normalize → `cmr.search_collections` → enrich → mint `dataset_` handles.

**5.2** `tools/understanding.py`: `describe_dataset(dataset_)` — resolve handle →
collection metadata + `get_variables` + advisory enrichment.

**Deferred to post-v1:** `discover_datasets`, `recommend_datasets`,
`list_variables`, `explain_variable`, `compare_datasets`.

**Gate:** mocked-CMR tests assert `dataset_` prefixes, advisory-note flagging,
and workspace persistence.

### Phase 6 — Area, coverage, durable retrieval (4 d)

The heaviest phase, because durable async retrieval is the operational core.

**6.1** `tools/area.py`: `define_area_of_interest` (place name via Nominatim,
bbox, GeoJSON, HUC watershed, FIPS admin) → `aoi_`.

**6.2** `tools/coverage.py`: `check_coverage`, `check_availability`,
`inspect_granules`, `estimate_retrieval_size` (from CMR granule sizes) → delegate
to `cmr.search_granules`. Metadata-only, fast.

**6.3** `tools/retrieval.py` on the durable job model (§4.3): `retrieve_data`,
`retrieve_subset`, `retrieve_timeseries`, `get_retrieval_status`, `cancel_retrieval`.
Each mints `job_` + pending `obs_`; the worker drives submit→poll→materialize;
`get_retrieval_status` reads job state from Postgres; result format by shape
(Zarr/Parquet); provenance records the spec. Cache-keyed per §4.4.

**Gate:**
```bash
docker compose exec mcp pytest tests/unit/test_tools/test_area.py \
  tests/unit/test_tools/test_coverage.py tests/unit/test_tools/test_retrieval.py -v
# Restart-resume: submit a (mocked) job, kill the worker, restart, assert it resumes.
docker compose exec mcp pytest tests/unit/test_jobs/test_resume.py -v
# Real availability through the full stack (no auth):
docker compose exec mcp python -c "
import asyncio
from earthdata_mcp.tools.area import define_area_of_interest
from earthdata_mcp.tools.coverage import check_availability
from earthdata_mcp.tools.discovery import search_datasets
async def go():
    aoi=await define_area_of_interest('-105,37,-104,38')
    ds=(await search_datasets('vegetation'))['datasets'][0]['handle']
    print(await check_availability(ds, aoi['handle'], '2024-01-01/2024-03-31'))
asyncio.run(go())"
```

### Phase 7 — Preview, inspection, transform; OPeNDAP + AppEEARS (4 d)

`tools/preview.py` (GIBS preview, summarize, inspect_statistics);
`tools/transform.py` (subset, reproject, resample, convert_format, align →
`cube_` + alignment_report; records provenance edges). Implement
`OPeNDAPProvider` (Hyrax/DAP4) and `AppEEARSProvider` (point/area tasks) as
`RetrievalProvider`s. **AppEEARS point output flows to Parquet/`series_`**, not
to a Zarr cube.

**Gate:** transform unit tests + provenance-edge assertions; one live AppEEARS
point task (`@live`, nightly); OPeNDAP subset against a known GES_DISC collection
(`@live`, nightly).

### Phase 8 — Provenance tools, citations, hardening (2 d)

`tools/provenance.py` (`get_provenance`, `cite_dataset`). `cite_dataset` reuses
CMR's citation records (NASA's `get_citations` pattern) for official DOIs and
formal strings. Per-provider rate limiting. Integration tests — **Harmony is no
longer skipped.**

**Gate:**
```bash
docker compose exec mcp pytest tests/unit/ -v --tb=short
docker compose exec mcp pytest tests/integration/ -v
# The core path is part of "done": runs the real Harmony flow (nightly/release).
EDL_TOKEN=... docker compose exec mcp pytest -m live tests/live/test_full_retrieval.py -v
docker compose exec mcp python -c "
from earthdata_mcp.server import mcp; print('tools:', len(mcp.list_tools()))"
```

---

## 7. Tool surface (v1)

Smaller than v2 by design. Discovery is two tools, not seven; retrieval gains a
cancel and a status that read durable state.

`search_datasets`, `describe_dataset`,
`define_area_of_interest`,
`check_coverage`, `check_availability`, `inspect_granules`, `estimate_retrieval_size`,
`retrieve_data`, `retrieve_subset`, `retrieve_timeseries`, `get_retrieval_status`, `cancel_retrieval`,
`preview_dataset`, `summarize_dataset`, `inspect_statistics`,
`subset`, `reproject`, `resample`, `convert_format`, `align`,
`get_provenance`, `cite_dataset`.

Deferred: `discover_datasets`, `recommend_datasets`, `list_variables`,
`explain_variable`, `compare_datasets`. No analysis tools, ever.

---

## 8. Reuse map (v3)

| Component | Source | Decision |
|---|---|---|
| CMR query patterns | CMR API docs + UMM (NASA MCP repo as pinned example) | Reference only |
| KMS normalization | NASA `get_keywords` approach | Mirror in `catalog/kms.py` |
| Citation records | NASA `get_citations` pattern | Reuse for `cite_dataset` |
| **Harmony client** | **`harmony-py` (official)** | **Wrap — do NOT hand-port** |
| OPeNDAP fetch | TTA `opendap_fetch_service.py` | Reuse/rewrite per Phase 0 audit |
| Cache / storage backend | TTA `cache_manager.py` | Reuse/rewrite per audit behind `StorageBackend`; **local FS default, object store optional** |
| DatasetParser | TTA `dataset_parser.py` | Reuse/rewrite per audit |
| earthaccess auth | TTA `earthaccess_client.py` | Reuse/rewrite per audit |
| DB pool | TTA `utils/db.py` | Reuse per audit; strip checkpointer |
| Docker/PostGIS | TTA `docker-compose.yml` | Adapt; add worker; storage is local volume by default (optional MinIO/S3 profile); no frontend/LLM |
| Handle system | **New** | Core |
| Workspace + provenance | **New** | Core |
| Durable job model | **New** | Core |
| Storage backend (`StorageBackend` + local/S3) | **New** | Core; local default for research |
| Retrieval / transform tools | **New** | Core |
| ServiceCapability model | **New** | Core (drives routing + gate) |
| Catalog enrichment | **New** | Thin, advisory |
| AppEEARS + GIBS providers | **New** | Not in TTA or NASA server |

Worth a read before Phase 6/7: `datalayer/earthdata-mcp-server` solved an
earthaccess search→download→Jupyter flow that may inform the download
integration.

---

## 9. Effort (v3)

| Phase | Deliverable | Effort |
|---|---|---|
| 0 | TTA reuse audit | 0.5 d |
| 1 | Scaffold, Docker, DB, storage backend (local), worker, config | 1 d |
| 2 | CMR access + ServiceCapability + KMS + thin enrichment | 2 d |
| 3 | Workspace, handles, provenance | 2 d |
| 4 | Auth + Harmony (harmony-py) + capability-gated router + live test | 3 d |
| 5 | Discovery + understanding (2 tools) | 1 d |
| 6 | Area, coverage, durable async retrieval | 4 d |
| 7 | Preview, transform, OPeNDAP, AppEEARS | 4 d |
| 8 | Provenance, citations, rate limiting, hardening, integration | 2 d |
| **Total** | | **~19.5 d (~4 weeks)** |

This assumes the Phase 0 audit finds TTA reusable. If TTA is research-grade, add
**30–50%**. v2's "~7 days" was an estimate for wiring existing code on a happy
path; this is an estimate for a production server that survives its authors. The
discovery savings v2 celebrated are real but small (~2 d); the cost and risk live
in retrieval and provenance — exactly where v2 was thinnest.

---

## 10. Session sequencing (v3)

```
Session 1:   Phase 0 — TTA audit → tta_audit.md
Session 2:   Phase 1 — scaffold + Docker + storage backend (local) + worker + gate
Session 3:   Phase 2, task 2.1 — pinned NASA read → cmr_patterns.md
Session 4:   Phase 2, tasks 2.2–2.5 + gate — CMR provider, capabilities, KMS, enrichment
Session 5:   Phase 3 — workspace + handles + provenance + gate
Session 6:   Phase 4, tasks 4.1–4.2 — base Protocols + EDL auth
Session 7:   Phase 4, tasks 4.3–4.5 + gate — Harmony (harmony-py) + router + live test
Session 8:   Phase 5 + gate — search_datasets + describe_dataset
Session 9:   Phase 6, tasks 6.1–6.2 — area + coverage
Session 10:  Phase 6, task 6.3 (durable retrieval) + worker resume test
Session 11:  Phase 6 gate + buffer for the job model
Session 12:  Phase 7, tasks 7.1–7.2 — preview + transform
Session 13:  Phase 7, OPeNDAP + AppEEARS (Parquet path) + gate
Session 14:  Phase 8 + gate — provenance + citations + hardening + integration (Harmony NOT skipped)
```

Same discipline as v1/v2: one phase per checkpoint, run gates yourself, commit on
green — but the gates that touch the retrieval core now require real execution.
```