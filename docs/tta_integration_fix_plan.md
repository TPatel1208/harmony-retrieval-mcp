# Retrieval MCP — readiness audit & fix plan for Talking-to-Air integration

**Date:** 2026-07-04
**Verdict:** the server core is ready — 668/668 unit tests pass, the durable-job
pipeline, provenance DAG, and the 22-tool surface are complete per Plan.md. But it
is **not yet consumable by an external application**: there is no way for a
consumer to get the materialized data out, no network transport, and the
integration suite is red. Three P0 items block integration; everything else is
sequenced behind them.

---

## Audit summary

| Area | Status | Evidence |
|---|---|---|
| Tool surface (22 tools) | ✅ complete | `server.py` registers discovery, area, coverage, retrieval, preview, transform, provenance |
| Unit tests | ✅ 668 passed | `docker compose exec mcp python -m pytest tests/unit` (2026-07-04) |
| Integration tests | ❌ 4 failed / 1 passed | stale provenance-event expectations ×3; live network call ×1 |
| Durable jobs / restart-resume | ✅ | Arq worker, Postgres source of truth, startup reclaim |
| Data egress to a consumer | ❌ **missing** | no tool returns bytes, a path, or a URL; `StorageBackend.path()` exists but is internal-only |
| Network transport | ❌ stdio only | `mcp.run()` default; compose runs `sleep infinity`, no port exposed |
| Live validation vs TTA's collections | ⚠️ untested | live suite uses EEDTEST/UAT + MOD13Q1, not TEMPO/OMI/TROPOMI L3 |
| Workspace listing / session resume | ⚠️ internal only | `WorkspaceStore.list_handles` exists; not exposed as a tool |
| License file | ⚠️ absent | residual from `docs/tta_audit.md` |

---

## P0 — blockers (do these before any TTA work)

### 1. Add a data-egress tool: `export_result`

**Problem.** TTA's plotting/statistics layer must open the materialized
Zarr/Parquet with xarray/pyarrow. Today the only readers of `storage_key` are the
server's own preview/transform tools; the key lives in the handle payload and is
never surfaced. An external consumer literally cannot reach the data.

**Fix.** New tool in `tools/` (registered in `server.py`):

```
export_result(handle, workspace_id) ->
  {handle, storage_uri, media_type, size_bytes, rematerialize_hint}
```

- Resolve `obs_`/`cube_` via the existing `resolve_materialized` seam.
- `storage_uri`: `LocalFilesystemBackend` → `file://` path from
  `storage.path(key)` (already implemented for exactly this use);
  `S3Backend` → presigned GET URL. Never hard-code a backend (CLAUDE.md).
- This is a **read**, not a transform: no new handle, no provenance edge. Record
  nothing, or at most an `exported` event — keep it out of the ancestry graph.
- `rematerialize_hint`: the message that the URI is cache-managed and may be
  evicted; re-running the originating `retrieve_*`/transform re-materializes via
  `cache_key()` (fast on a cache hit). This keeps the hard rule intact —
  provenance still records the spec, and the URI is explicitly ephemeral.
- **Contract must be scheme-agnostic** (grilled 2026-07-04): the response shape
  is identical whether `storage_uri` is `file://` or a presigned `https://` URL.
  Consumers branch on URI scheme, never assume local paths — hosting later is a
  config change, not an egress refactor.
- Not-found / not-ready answers follow the PRD 014 pattern (structured answer,
  not an exception).

### 1b. Add `rematerialize(handle)` (decided 2026-07-04, ships with 1)

`get_provenance` returns ancestry + events but **not** the request spec, and no
tool does — so a consumer whose exported URI was evicted has no durable way to
re-derive it once the originating conversation is gone (TTA restart, resumed
session). The spec IS durably stored server-side; only the MCP can guarantee
replay fidelity. New tool:

```
rematerialize(handle, workspace_id) -> {job_handle, obs_handle, status}
```

Resolve the `obs_`/`cube_` handle, re-enqueue the durable job from the persisted
spec (a cache hit makes it near-instant), return the job handle to poll. The
consumer loop becomes: `export → evicted? → rematerialize → await → export`.
Adds ~half a session to P0-1; this is what makes the "provenance records the
spec" rule cash out for consumers.

**Tests.** Unit: local path round-trip, pending-handle answer, cross-workspace
collapse to not-found. Integration: materialize a fake job → export → open with
xarray from the returned path.

### 2. Env-selectable HTTP transport

**Problem.** TTA's backend is a separate container (separate compose stack).
stdio requires being the child process of the consumer — a non-starter across
containers.

**Fix.**
- `config.py`: `mcp_transport: str = "stdio"`, `mcp_host: str = "0.0.0.0"`,
  `mcp_port: int = 8765` (env: `EARTHDATA_MCP_TRANSPORT` etc.).
- `server.py:main()`: `mcp.run(transport="http", host=…, port=…)` when
  configured (FastMCP ≥2 supports streamable HTTP natively).
- `docker-compose.yml`: replace `sleep infinity` with the server command when
  `EARTHDATA_MCP_TRANSPORT=http`, expose the port, and join an **external Docker
  network** (e.g. `earthdata_net`) so the TTA stack can attach to it.
- Security posture (grilled 2026-07-04 — end goal is hosted/public, current
  scope is a lab-group research box): ship the **static bearer token in P0-2**,
  not later — it's ~10 lines here and one header in TTA, and it keeps Phase 1–3
  from baking in trust-the-network assumptions. Real authn, S3 backend +
  presigned URLs, and per-user quotas stay deferred to a pre-hosting phase.

### 3. Green the integration suite

Four failures in `tests/integration/test_durable_pipeline.py`:

- **3 stale expectations.** Recent commits added `routed` (plan-time) and
  `job-failed` (terminal) provenance events; the tests still assert the old
  orderings (`["submitted","materialized"]` etc.). Update the expected event
  lists — the code behavior is the intended one (CONTEXT.md: branch explanations
  are *in addition to* `JOB_FAILED`).
- **1 live network call.** `test_appeears_point_path_materialises_parquet`
  reaches `https://appeears.earthdatacloud.nasa.gov/api/product/MOD13Q1` — the
  AppEEARS product/layer lookup isn't behind the fake seam. Patch the layer
  lookup in the test's provider fake so the suite is hermetic.

Gate: `docker compose exec mcp python -m pytest tests/integration -v` fully green.

---

## P1 — needed early in the integration itself

### 4. Live validation matrix against TTA's real collections

TTA's registry pins these; the live suite has never touched them:

| TTA key | short_name / version | Known risk |
|---|---|---|
| TEMPO_NO2 | TEMPO_NO2_L3 V04 | group-prefixed vars (`product/…`), hourly granule volume |
| TEMPO_O3TOT | TEMPO_O3TOT_L3 V04 | same |
| TEMPO_HCHO | TEMPO_HCHO_L3 V04 | same |
| OMI_NO2 | OMI_MINDS_NO2d 1.1 | no variable subsetting service |
| OMI_O3 | OMDOAO3e 003 | HDF-EOS grid via OPeNDAP |
| OMI_HCHO | OMHCHOd 003 | multi-group (`key_science_data`, `qa_statistics`) |
| TROPOMI_NO2 | HAQ_TROPOMI_NO2_GLOBAL_M_L3 2.4 | monthly temporal semantics |
| MODIS AOD | AER_DBDT_D10KM_L3_MODIS_{TERRA,AQUA} 001 | fill −999, no subsetting |

Add `tests/live/test_tta_collections.py` (opt-in, `@pytest.mark.live`):
parameterized `search → describe → retrieve_subset (small AOI, 1 day/1 month) →
poll → export → xr.open_*` per collection, asserting the primary variable is
present and CF time decodes. Run it once with real EDL credentials and file
quirks into `docs/opendap_quirk_ledger.md` as found.

### 5. Expose `list_workspace`

`WorkspaceStore.list_handles` (store.py:135) exists but has no tool. A TTA
session that reconnects (page reload, backend restart) needs to re-enumerate its
handles without replaying the conversation. Thin tool:
`list_workspace(workspace_id, handle_type=None) -> {handles: [{handle, type,
created_at, summary}]}`.

### 6. Poll ergonomics (decide, likely no code)

An LLM agent polling `get_retrieval_status` burns model turns. Preferred:
TTA-side awaiter (its backend polls in asyncio and streams progress to the UI —
see the TTA plan, Phase 1). Optional later: `wait_seconds` long-poll parameter on
`get_retrieval_status`. Do **not** add blocking retrieval tools — durable jobs
are the design.

---

## P2 — hardening (not blocking)

7. **Eviction vs. open consumers.** TTA may hold a `file://` URI whose key gets
   evicted under `EARTHDATA_CACHE_MAX_BYTES`. Consumers must treat exported URIs
   as re-derivable (the `rematerialize_hint` contract). Consider LRU-touching a
   key on `export_result` so actively-used results aren't first out.
8. **`describe_dataset` masking metadata.** Verify UMM-Var fill values/valid
   ranges surface in the per-variable enrichment so TTA can drop its hand-pinned
   `fill_value`/`valid_min`/`valid_max` registry entries. Add to the advisory
   notes if missing.
9. **LICENSE file** (tta_audit residual) before the repo is consumed by another
   project.

---

## Sequencing

1. P0-3 (green tests) — half a session; do first so everything after lands on green.
2. P0-1 + P0-2 (`export_result` + HTTP transport) — one session together; they
   are the consumer contract and should ship with docs + compose changes.
3. P1-5 (`list_workspace`) — small, same session as #2 if room.
4. P1-4 (live matrix) — one session with credentials; expect follow-up quirk
   fixes (this is the highest-uncertainty item).
5. P2 items — opportunistic.

Per CLAUDE.md: one prompt = one session = one commit; run the test gates exactly
as written before each commit.
