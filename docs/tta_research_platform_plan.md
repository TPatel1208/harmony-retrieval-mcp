# Talking-to-Air → environmental research workbench: technical plan

**Date:** 2026-07-04
**Goal:** evolve TTA from a chatbot that plots a few hard-coded satellite
products into an AI-guided research tool: any-Earthdata discovery, durable
retrievals, comparisons (satellite↔ground, region↔region, period↔period),
visualizations as first-class artifacts, and provenance-backed, citable outputs.
The earthdata-retrieval MCP becomes the entire data-loading layer; TTA keeps and
grows the layers the MCP deliberately excludes: analysis, visualization, and the
researcher-facing UX.

**Prerequisite:** the P0 items in `docs/tta_integration_fix_plan.md`
(`export_result`, HTTP transport, green integration suite).

---

## 0. Which TTA is "the" TTA

Two divergent local working copies of `github.com/TPatel1208/Talking-To-Air`:

- `Semester 4/Talking-to-Air` — branch `main`, last commit 2026-05-22. Simple:
  supervisor + ground + satellite agents, regex-scraped PNG paths, hand-rolled
  Harmony `DataLoader` + Zarr/PostGIS cache.
- `Semester 4/tta-test` — branch `talking-to-air-v2`, last commit 2026-06-17.
  Much richer: JWT auth, artifact model (tables/charts), chart persistence +
  CSV/PNG export, intent router, SSE parser, real pytest suite,
  services/repositories layering.

**Decision: build on `talking-to-air-v2`.** It already has the three primitives
the research platform needs (auth/users, artifacts, exports). Fold anything
still wanted from `main` (the newer prompt wording, the multi-agent supervisor
fixes) into v2, then retire the older copy. First task of Phase 0.

---

## 1. Target architecture

```
React frontend ─ chat pane + workbench panes (jobs, artifacts, datasets, provenance)
      │ SSE
FastAPI backend (v2 api.py)
      │
Supervisor agent (LangGraph, Postgres checkpointer)
  ├─ earthdata agent      → MCP tools via langchain-mcp-adapters
  │                          (streamable HTTP → earthdata-mcp server)
  ├─ ground-sensor agent  → EPA AQS tools (unchanged — outside MCP scope)
  └─ analysis & viz tools → open materialized Zarr/Parquet directly
                             (path from export_result, shared volume)

earthdata-mcp stack: mcp server (HTTP) + arq worker + postgres + redis
shared: external docker network `earthdata_net` + `earthdata_data` volume
        (mounted read-only into the TTA backend)
```

Division of labor (this is the complementary design, not an accident):

| Concern | Owner |
|---|---|
| discovery, AOI, coverage, retrieval, caching, transforms, provenance, citations | **MCP** |
| EPA AQS ground data | TTA ground agent |
| statistics, comparisons, anomalies, plots, narratives | TTA analysis/viz tools (MCP has none, by hard rule) |
| sessions, users, artifacts, exports, UI | TTA backend/frontend |

Workspace mapping: `workspace_id = f"user-{user_id}"` (per-user isolation;
handles shared across that user's threads — a researcher's datasets/AOIs are
project assets, not conversation-locals). Revisit to per-project workspaces in
Phase 4.

---

## 2. Phase 0 — consolidation & deployment topology (~1 session)

1. Merge `main`'s post-May fixes into `talking-to-air-v2`; make v2 the default
   branch; archive the stale working copy.
2. Compose topology: keep the two stacks separate but joined —
   - `earthdata_net` external network; both stacks join it.
   - `earthdata_data` named volume: written by the MCP worker, mounted **ro**
     into `tta-backend` at the same absolute path (`/data`) so `file://` URIs
     from `export_result` resolve identically in both containers.
   - TTA drops its own `zarr_cache` volume (the MCP owns caching now).
3. Config: `EARTHDATA_MCP_URL=http://earthdata-mcp:8765/mcp` in TTA's `.env`.

## 3. Phase 1 — data-layer swap (~2–3 sessions)

**3.1 MCP client layer.** Add `langchain-mcp-adapters`; build the earthdata
agent's toolset from `MultiServerMCPClient` at startup (async load, fail loud if
the MCP is unreachable). Bind `workspace_id` into every call via a thin wrapper
so the model never sees or invents it. Send the MCP bearer token on every call.

**Curated tool surface (decided 2026-07-04).** The agent sees ~12 tools, not
all 22: discovery (`search_datasets`, `describe_dataset`, `preview_dataset`,
`summarize_dataset`), `define_area_of_interest`, coverage
(`check_availability`, `check_coverage`), retrieval status + `cite_dataset` +
`get_provenance` + `retrieve_timeseries` (point questions), plus two **backend
composites**: `await_retrieval` (3.3) and `safe_retrieve` — which runs
`estimate_retrieval_size` → gate → `retrieve_subset` deterministically. Gate
numbers live in TTA config, not prompts: ≤2 GB proceeds, 2–10 GB interrupts and
asks the researcher in-chat, >10 GB refuses with tighten-your-AOI guidance.
Plumbing tools (`convert_format`, `reproject`, `resample`, `inspect_*`,
`cancel_retrieval`, raw `retrieve_*`) stay hidden until a workflow needs them —
`align` is exposed when Phase 2's comparison artifact lands.

**3.2 Replace the satellite data tools.**

| Today (satellite agent) | Becomes |
|---|---|
| `geocode_location` (Nominatim) | `define_area_of_interest` (also does bbox/GeoJSON/HUC/FIPS) |
| `COLLECTIONS` dict (10 pinned datasets) | `search_datasets` + `describe_dataset` + a small **preset list** of curated short_names as prompt guidance — presets are suggestions, not a ceiling |
| `check_data_availability` (raw CMR requests) | `check_availability` / `check_coverage` / `estimate_retrieval_size` |
| `fetch_environmental_data` → `DataLoader.download_dataset_harmony` | `retrieve_subset` / `retrieve_data` / `retrieve_timeseries` + job polling + `export_result` |

**3.3 Retrieval awaiter (backend, not model).** The MCP returns
`{job_handle, obs_handle}` immediately. Add a backend `await_retrieval(job_handle)`
LangChain tool that polls `get_retrieval_status` in asyncio (backoff 2→15 s,
timeout configurable), pushes `job_progress` SSE events to the frontend as it
polls, and returns the terminal status. The **model** makes one tool call; the
**backend** absorbs the latency. Failures return the MCP's stage/provider-
prefixed error string verbatim — it is designed to be legible.

**3.4 Rework plot/stat tools to handles.** Replace the `data_dict` /
`_fetch_params` / `_load_data` reload pattern with:

```python
def open_handle(handle: str) -> xr.Dataset | pa.Table:
    uri = mcp.export_result(handle)          # file:// on the shared volume
    # zarr → xr.open_zarr, parquet → pa, netcdf → xr.open_dataset
```

- `plot_singular` / `plot_multiple` / `compute_statistic_tool` /
  `conduct_temporal_statistic` / `find_daily_peak` take `obs_`/`cube_` handles.
- On `FileNotFoundError` (evicted), call the MCP's `rematerialize(handle)`
  (fix-plan P0-1b) → `await_retrieval` → re-export — wrapped once in
  `open_handle`. Never assume the original tool args are still in context; the
  MCP replays from its persisted spec. `open_handle` branches on URI **scheme**
  (`file://` now, presigned `https://` at hosting time), never assumes local.
- Masking: prefer fill/valid-range metadata from `describe_dataset`; keep a thin
  TTA-side override table only for products whose UMM-Var records are wrong.

**3.5 Deletions.** `preprocessing/data_loader.py` (~660 LOC),
`preprocessing/cache_index.py`, `sql/init_cache_index.sql`, the Harmony/
earthaccess/netCDF4 download deps from requirements, most of `COLLECTIONS`.
Ground-sensor agent untouched.

**3.6 Prompts.** Rewrite the satellite→**earthdata** agent prompt around the
handle workflow: *search → describe → AOI → coverage/size check → retrieve →
await → open/plot*, with the rule "always check coverage + size before
retrieving; keep AOIs tight and time windows minimal (TEMPO is hourly)."

**3.7 Structured sub-agent returns.** Sub-agents currently return prose that the
API regex-scrapes for `.png` paths. Change sub-agent → supervisor returns to a
JSON envelope `{summary, artifact_ids, handles}`; the API layer stops regexing.
(v2's artifact model already supports this — plots register as artifacts.)

**Exit criteria:** the demo conversations of today ("plot TEMPO NO2 over NJ on
date X", "compare NYC vs LA") work end-to-end through the MCP, plus one
previously-impossible one ("find a soil-moisture dataset and map it over the
Raritan basin") that proves discovery is open-ended.

## 4. Phase 2 — research workbench UX (~3–4 sessions)

**4.1 Jobs panel.** Durable-retrieval UI fed by the `job_progress` SSE events +
a `GET /jobs` endpoint that lists the workspace's jobs via `list_workspace` +
`get_retrieval_status`. Retrievals survive page reloads and backend restarts —
this is the payoff of the MCP's durability, surface it.

**4.2 Artifact system generalization.** v2 has `table` and `chart`. Add:
- `map` — PNG + bbox + colorbar/units + source handles;
- `comparison` — n-panel maps or a difference map (inputs aligned via the MCP's
  `align`, differenced TTA-side);
- `timeseries` — satellite (retrieve_timeseries / AppEEARS Parquet) overlaid
  with EPA AQS ground series; the satellite↔ground comparison is TTA's
  signature move, make it one artifact type.
Every artifact stores `source_handles` in metadata — that's the provenance hook.

**4.3 GIBS quick-look.** Before any retrieval, the agent calls
`preview_dataset`; the frontend renders `gibs_url` directly as an inline image.
Researchers confirm "is this the right product/region?" in seconds instead of
waiting minutes for a Harmony job.

**4.4 Dataset discovery pane.** Search box → `search_datasets`; card per result
(`describe_dataset` summary, variables, temporal extent); buttons: quick-look,
check coverage, retrieve. The chat can drive it, but researchers can also browse.

## 5. Phase 3 — research-grade outputs (~2–3 sessions)

**5.1 Provenance panel.** Per-artifact "how was this made": walk
`source_handles` → `get_provenance` → render ancestry + events (routed →
submitted → materialized, provider, granule counts). Every figure traces to a
re-materializable spec.

**5.2 Citations & methods export.** `cite_dataset` per source dataset →
references list. "Export methods" button: generate a markdown/DOCX blurb (AOI
definition, time range, datasets + DOIs, processing chain from provenance
events, retrieval dates) — the paragraph a paper's methods section needs.

**5.3 Data export.** `convert_format` (→ CSV/NetCDF) + `export_result` behind
TTA download endpoints (extends v2's `export_service`/artifact CSV routes).

**5.4 Analysis toolset (TTA-side, by design).** Exceedance-day overlays
(ground exceedances × satellite maps), ground↔satellite correlation at monitor
locations, period-over-period anomaly (two retrievals, MCP `align`, TTA diff +
stats). Each records input handles in artifact metadata so 5.1 covers them.

## 6. Phase 4 — projects & multi-user (optional, later)

Project = named MCP workspace + saved artifacts + notes. v2 auth already
provides users; add a `projects` table, switch `workspace_id` to
`user-{id}-proj-{id}`, and a project switcher in the sidebar.

---

## 7. Risks & open decisions

1. **Model strength — decided 2026-07-04.** Per-agent model config; the
   earthdata agent gets a **frontier tool-use model** from day one (e.g. Gemini
   2.5 Pro or Claude Sonnet via API); supervisor/ground agents can stay cheap.
   Still build the 10-task scripted eval — as a regression gate, not a
   spend-avoidance experiment.
2. **Latency honesty.** Harmony/AppEEARS jobs take minutes. Phase 2's jobs panel
   is not polish — without it, Phase 1 feels like a regression next to the old
   synchronous (but brittle) fetch. Ship 3.3's SSE progress in Phase 1.
3. **TEMPO volume — decided 2026-07-04.** Enforced in code by the
   `safe_retrieve` composite (§3.1), not in prompts: ≤2 GB silent, 2–10 GB
   in-chat researcher confirmation, >10 GB refusal. The agent has no raw
   retrieve tool to bypass it with.
4. **Eviction races.** Exported `file://` URIs can be evicted; `open_handle`'s
   re-materialize wrapper (3.4) is the safety net — test it deliberately.
5. **Live-collection quirks.** The fix plan's live matrix (P1-4) will surface
   OPeNDAP/HDF-EOS surprises for OMI/TROPOMI. Decided 2026-07-04: **overlap**,
   don't serialize — Phase 0/1 plumbing (MCP client, awaiter, open_handle)
   doesn't depend on collection quirks, so run the live matrix in the same
   window and land quirk fixes as parallel MCP sessions. Assumes EDL
   credentials are available now. Expect 1–2 quirk-fix sessions in the MCP repo.
6. **Ground-data scope.** EPA AQS stays TTA-owned. If international ground data
   (OpenAQ) is ever wanted, that's a new TTA toolset, not an MCP change.
   (Audited 2026-07-04: v2's AQS toolset is substantial and tested —
   monitors-in-bbox, daily/quarterly/annual summaries, exceedance days, sample
   data — workflow-ready as-is.)

---

## 8. Decision record (grilling session, 2026-07-04)

1. **Deployment scope.** End goal is hosted/public; the version being built now
   targets a lab group on a single research box. Posture: *contract-proof now,
   harden later* — bearer token + scheme-agnostic egress in P0, S3/presigned +
   real authn + quotas deferred to a pre-hosting phase.
2. **Signature workflows** (everything else builds after these are excellent):
   (a) satellite↔ground validation (TEMPO/OMI × EPA AQS), (b) region/period
   comparison maps, (c) open-ended dataset discovery. Reproducible methods
   export stays Phase 3.
3. **Sat↔ground mechanics.** One `safe_retrieve` cube over the AOI → TTA-side
   `xr.sel(..., method="nearest")` at every monitor location at once.
   `retrieve_timeseries(point_sample=True)` (AppEEARS-only) remains an agent
   tool for single-point questions, not the comparison path.
4. **LLM.** Frontier tool-use model on the earthdata agent; cheap models
   elsewhere; scripted 10-task eval as regression gate.
5. **Tool surface.** Curated ~12 MCP tools + `await_retrieval` and
   `safe_retrieve` composites (2 GB ask / 10 GB refuse, in config not prompts).
6. **Eviction recovery.** MCP `rematerialize(handle)` (fix-plan P0-1b);
   `open_handle` never depends on conversation memory or local-path schemes.
7. **Sequencing.** Overlap: P0 first, then Phase 0/1 concurrent with the live
   collection matrix; quirk fixes land as parallel MCP sessions.
8. **Frontend.** Full four-pane workbench, landing in order:
   Jobs + Artifacts (Phase 2a) → Dataset discovery (Phase 2b) → Provenance
   (ships with Phase 3 citations/methods so lineage UI and lineage outputs
   arrive as one story).
9. **Cut line.** No hard external deadline; quality-driven. If anything slips:
   Phase 4 (projects/multi-user) slides first, then the provenance pane. The
   three signature workflows never slide.
