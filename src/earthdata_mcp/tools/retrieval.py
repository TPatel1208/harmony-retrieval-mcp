"""Durable retrieval tools (PLAN.md §6 Phase 6.3, §4.3).

Five tools on the durable job model. A retrieval call does **not** block on
Harmony — it plans, persists a durable job, and hands the agent two handles:

* a ``job_`` handle — pollable (:func:`get_retrieval_status`) and cancellable
  (:func:`cancel_retrieval`); its state lives in Postgres, never in memory, so a
  worker restart never loses it.
* an ``obs_`` handle — the eventual result, resolved once the job is ``ready``.

The out-of-process worker (``jobs/worker.py``) drives submit → poll → materialize.
The tool's job is the **planning** half: resolve handles, fetch the merged
:class:`CollectionCapabilities`, route the plan (Harmony-first — pin a matched
service, else submit unpinned and let the server pick the chain), persist the
**durable request spec** (re-materializable, never a staged URL), and enqueue.
OPeNDAP is the worker's runtime fallback if a real Harmony submit fails.

Format follows shape (§4.4): a gridded collection defaults to Zarr, a point
collection to Parquet, everything else to netCDF-4 — we never force tabular data
through a cube. The chosen format is recorded in the spec and must match the
routed service's ``output_formats`` or the route fails at planning time.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from uuid import uuid4

from earthdata_mcp.config import get_settings
from earthdata_mcp.db import create_engine, create_session_factory
from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.state import TERMINAL_STATES, JobState
from earthdata_mcp.providers.appeears import AppEEARSProvider
from earthdata_mcp.providers.base import AOI, RetrievalPlan, TimeRange, TransformSpec
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.providers.harmony import HarmonyProvider
from earthdata_mcp.providers.opendap import AxisGeometry, OPeNDAPProvider, VarDimPlan, plan_subset
from earthdata_mcp.providers.request_spec import RequestSpec
from earthdata_mcp.providers.router import Router
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.workspace.handles import resolve_aoi, resolve_dataset
from earthdata_mcp.workspace.models import HandleType, ProvenanceEventType, handle_type_of
from earthdata_mcp.workspace.provenance import ProvenanceStore
from earthdata_mcp.workspace.store import WorkspaceStore

#: An ``enqueue_fn`` enqueues ``(task_name, *args)`` onto the worker queue. The
#: default wraps an Arq pool; tests inject an ``AsyncMock`` so no Redis is needed.
EnqueueFn = Callable[..., Awaitable[object]]

#: Default output media type per result shape (§4.4 format-by-shape).
#:
#: Grid defaults to netCDF-4, **not** Zarr: few Harmony services advertise Zarr in
#: their ``output_formats``, so a Zarr default made ``find_service`` reject most
#: subset-able L3 collections (TEMPO HCHO L3, most LARC/GES_DISC L3). netCDF-4 is
#: near-universally supported by Harmony subsetters and is exactly what OPeNDAP
#: returns, so it routes broadly. ``_dataio`` reads the netCDF back (flattening
#: groups); Zarr is produced only by the transform tools when a cube is derived.
_FORMAT_BY_SHAPE = {
    "grid": "application/netcdf4",
    "point": "application/x-parquet",
    "swath": "application/netcdf4",
}
_DEFAULT_FORMAT = "application/netcdf4"

# Lazily-built process defaults. Stay ``None`` until the first un-injected call so
# importing this module (and server.py) builds no engine and opens no connection.
_session_factory = None
_provenance: ProvenanceStore | None = None
_pool = None


def _default_session_factory():
    """Process-wide jobs session factory, built on first use."""
    global _session_factory
    if _session_factory is None:
        _session_factory = create_session_factory(create_engine())
    return _session_factory


def _default_provenance() -> ProvenanceStore:
    """Process-wide provenance store sharing the jobs session factory."""
    global _provenance
    if _provenance is None:
        _provenance = ProvenanceStore(_default_session_factory())
    return _provenance


async def _default_enqueue(task_name: str, *args: object) -> object:
    """Enqueue onto the Arq pool (built on first use). Tests inject a mock instead."""
    global _pool
    if _pool is None:
        from arq import create_pool
        from arq.connections import RedisSettings

        _pool = await create_pool(RedisSettings.from_dsn(get_settings().redis_url))
    return await _pool.enqueue_job(task_name, *args)


# -- the five tools --------------------------------------------------------


async def retrieve_data(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    output_format: str | None = None,
    *,
    cmr: CMRProvider | None = None,
    store: WorkspaceStore | None = None,
    provenance: ProvenanceStore | None = None,
    session_factory=None,
    enqueue_fn: EnqueueFn | None = None,
) -> dict:
    """Retrieve a dataset over an AOI + time window (bbox-subset to that AOI).

    Mints a durable job and returns ``{job_handle, obs_handle, status, provider}``.
    Format defaults by the collection's shape (grid → Zarr) unless ``output_format``
    is given.
    """
    return await _submit_retrieval(
        dataset_handle=dataset_handle,
        aoi_handle=aoi_handle,
        time_range=time_range,
        variables=(),
        workspace_id=workspace_id,
        output_format=output_format,
        needs_bbox=True,
        needs_variable=False,
        needs_temporal=bool(time_range),
        needs_point_sample=False,
        cmr=cmr,
        store=store,
        provenance=provenance,
        session_factory=session_factory,
        enqueue_fn=enqueue_fn,
    )


async def retrieve_subset(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    variables: list[str],
    workspace_id: str = DEFAULT_WORKSPACE,
    output_format: str | None = None,
    *,
    cmr: CMRProvider | None = None,
    store: WorkspaceStore | None = None,
    provenance: ProvenanceStore | None = None,
    session_factory=None,
    enqueue_fn: EnqueueFn | None = None,
) -> dict:
    """Retrieve a variable + bbox + temporal subset.

    Harmony is tried first: a single service that does all three is pinned; if none
    does (the union-trap case), Harmony is submitted unpinned and the server picks
    the chain. OPeNDAP is the worker's runtime fallback if that submit fails."""
    return await _submit_retrieval(
        dataset_handle=dataset_handle,
        aoi_handle=aoi_handle,
        time_range=time_range,
        variables=tuple(variables or ()),
        workspace_id=workspace_id,
        output_format=output_format,
        needs_bbox=True,
        needs_variable=True,
        needs_temporal=bool(time_range),
        needs_point_sample=False,
        cmr=cmr,
        store=store,
        provenance=provenance,
        session_factory=session_factory,
        enqueue_fn=enqueue_fn,
    )


async def retrieve_timeseries(
    dataset_handle: str,
    time_range: str,
    variables: list[str],
    workspace_id: str = DEFAULT_WORKSPACE,
    output_format: str | None = None,
    aoi_handle: str | None = None,
    point_sample: bool = False,
    *,
    cmr: CMRProvider | None = None,
    store: WorkspaceStore | None = None,
    provenance: ProvenanceStore | None = None,
    session_factory=None,
    enqueue_fn: EnqueueFn | None = None,
) -> dict:
    """Retrieve a variable time-series; the AOI is optional (a global series omits it).

    A bbox subset is requested only when ``aoi_handle`` is given, so the route is
    gated on ``needs_bbox`` accordingly.

    Set ``point_sample=True`` to sample the variable at a point/area: that is an
    *intent* the router honours by routing to AppEEARS (§4.4), which returns a
    tabular series — so the result defaults to Parquet, never a Zarr cube. The
    ``aoi_handle`` then carries the sample point (a degenerate point bbox is the
    common case).
    """
    return await _submit_retrieval(
        dataset_handle=dataset_handle,
        aoi_handle=aoi_handle,
        time_range=time_range,
        variables=tuple(variables or ()),
        workspace_id=workspace_id,
        output_format=output_format,
        needs_bbox=aoi_handle is not None and not point_sample,
        needs_variable=True,
        needs_temporal=bool(time_range),
        needs_point_sample=point_sample,
        cmr=cmr,
        store=store,
        provenance=provenance,
        session_factory=session_factory,
        enqueue_fn=enqueue_fn,
    )


async def get_retrieval_status(
    job_handle: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    session_factory=None,
) -> dict:
    """Read a job's current state **from Postgres** (never from memory, §4.3).

    Resolves the ``job_`` handle within the workspace first (cross-workspace
    access is denied), then reads the durable row. Returns
    ``{job_handle, obs_handle, status, progress, error, provider_job_url}``.
    """
    store = store or _default_store()
    session_factory = session_factory or _default_session_factory()

    if handle_type_of(job_handle) is not HandleType.JOB:
        raise ValueError(f"expected a job_ handle, got {job_handle!r}")
    # Isolation gate: raises CrossWorkspaceError / HandleNotFoundError.
    await store.get_handle(workspace_id, job_handle)

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, job_handle)
        if job is None:
            raise crud.JobNotFoundError(job_handle)
        return {
            "job_handle": job.job_handle,
            "obs_handle": job.obs_handle,
            "status": job.state,
            "progress": job.progress,
            "error": job.error,
            "provider_job_url": job.provider_job_url,
        }


async def cancel_retrieval(
    job_handle: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    session_factory=None,
) -> dict:
    """Cancel a non-terminal job; a no-op on one that is already terminal.

    A job's state can change between when a caller decides to cancel and when
    the call lands (e.g. it finishes or fails first), so cancelling an
    already-terminal job (ready/failed/expired/cancelled) returns that state
    as-is rather than raising — the caller's intent ("stop this job") is
    already satisfied, and this is not an illegal request.
    """
    store = store or _default_store()
    session_factory = session_factory or _default_session_factory()

    if handle_type_of(job_handle) is not HandleType.JOB:
        raise ValueError(f"expected a job_ handle, got {job_handle!r}")
    await store.get_handle(workspace_id, job_handle)  # isolation gate

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, job_handle)
        if job is None:
            raise crud.JobNotFoundError(job_handle)
        if JobState(job.state) in TERMINAL_STATES:
            return {"job_handle": job_handle, "status": job.state}
        crud.set_state(job, JobState.CANCELLED)
        await session.commit()

    return {"job_handle": job_handle, "status": JobState.CANCELLED.value}


# -- shared planning + persistence ----------------------------------------


async def _submit_retrieval(
    *,
    dataset_handle: str,
    aoi_handle: str | None,
    time_range: str,
    variables: tuple[str, ...],
    workspace_id: str,
    output_format: str | None,
    needs_bbox: bool,
    needs_variable: bool,
    needs_temporal: bool,
    needs_point_sample: bool,
    cmr: CMRProvider | None,
    store: WorkspaceStore | None,
    provenance: ProvenanceStore | None,
    session_factory,
    enqueue_fn: EnqueueFn | None,
) -> dict:
    """Plan → persist a durable job → enqueue. The shared core of the three
    ``retrieve_*`` tools.

    Order matters: routing happens **before** any handle or row is created, so an
    unserviceable request (``NotRetrievable``) leaves no orphan job behind.
    """
    cmr = cmr or CMRProvider()
    store = store or _default_store()
    provenance = provenance or _default_provenance()
    session_factory = session_factory or _default_session_factory()
    enqueue_fn = enqueue_fn or _default_enqueue

    concept_id, bbox = await _resolve_handles(
        dataset_handle, aoi_handle, workspace_id, store
    )

    caps = await cmr.collection_capabilities(concept_id)

    # Discover OPeNDAP URLs for any gridded/swath collection with a bbox — not just
    # those with no Harmony services. A collection may have service associations that
    # Harmony returns but that cannot satisfy a bbox+temporal plan (e.g. TEMPO HCHO L3
    # with a LARC_CLOUD service that lacks bbox subsetting). The router already
    # prefers Harmony (step 1) over OPeNDAP (step 3), so discovering the URLs here
    # costs one extra CMR granule-search but never displaces a working Harmony service.
    # All granules in the window are collected so a multi-day request covers the whole
    # span, not just the first scan cycle (Part 3).
    opendap_plan = None
    if (
        not needs_point_sample
        and caps.output_shape in ("grid", "swath")
        and bbox is not None
    ):
        opendap_plan = await plan_subset(cmr, caps, concept_id, bbox, time_range, variables)
    opendap_urls = opendap_plan.opendap_urls if opendap_plan else []

    # Format by shape (grid/swath → netCDF-4). An explicit caller format still wins.
    if needs_point_sample:
        fmt = output_format or _FORMAT_BY_SHAPE["point"]
    elif output_format is not None:
        fmt = output_format
    else:
        fmt = _FORMAT_BY_SHAPE.get(caps.output_shape, _DEFAULT_FORMAT)

    plan = RetrievalPlan(
        output_format=fmt,
        needs_bbox=needs_bbox,
        needs_variable=needs_variable,
        needs_temporal=needs_temporal,
        needs_point_sample=needs_point_sample,
        concept_id=concept_id,
        short_name=caps.short_name,
        aoi=AOI(bbox=bbox) if bbox is not None else None,
        time_range=TimeRange.from_cmr(time_range) if time_range else None,
        transform=TransformSpec(output_format=fmt, variables=variables)
        if variables
        else None,
    )

    # Plan-time gate: raises NotRetrievable if no single service fits. No fallback.
    # OPeNDAP is wired when a granule URL was discovered (router step 3, PLAN.md §4.2).
    # AppEEARS handles point-sample plans (step 0); OPeNDAP handles gridded/swath
    # bbox+variable+temporal subsets when Harmony has no capable service (step 3).
    #
    # plan_subset resolves variable names to their full UMM-V paths in one
    # get_variables call whenever a granule URL was found: bare leaf names become
    # /<group>/<leaf> for grouped collections (TEMPO), coordinate names are
    # discovered in the same pass. Resolved names flow into both the Harmony
    # primary submit and the OPeNDAP fallback via the durable spec, so neither
    # path re-derives them.
    coord_lat, coord_lon = "lat", "lon"
    coord_time: str | None = None
    lat_axis: AxisGeometry | None = None
    lon_axis: AxisGeometry | None = None
    var_dims: dict[str, VarDimPlan] = {}
    if opendap_plan is not None:
        coord_lat, coord_lon = opendap_plan.coord_lat, opendap_plan.coord_lon
        coord_time = opendap_plan.coord_time
        lat_axis, lon_axis, var_dims = (
            opendap_plan.lat_axis, opendap_plan.lon_axis, opendap_plan.var_dims
        )
        variables = opendap_plan.variables
    harmony_provider = HarmonyProvider(caps)
    opendap_provider = (
        OPeNDAPProvider(
            caps,
            opendap_urls=opendap_urls,
            coord_lat=coord_lat,
            coord_lon=coord_lon,
            coord_time=coord_time,
            lat_axis=lat_axis,
            lon_axis=lon_axis,
            var_dims=var_dims,
        )
        if opendap_urls
        else None
    )
    decision = Router(
        caps,
        harmony=harmony_provider,
        appeears=AppEEARSProvider(caps),
        opendap=opendap_provider,
    ).route(plan)
    # Mint the two handles, then persist the durable spec that ties them together.
    obs_handle = await store.put_handle(
        workspace_id, HandleType.OBS, payload={"status": "pending"}
    )
    # Durable, re-materializable record of *why* this path was chosen — computed
    # by `route` from data already in hand, never re-derived here (CLAUDE.md: the
    # union-trap booleans are never trusted; provenance never stores an ephemeral
    # staged-output URL, and this trace carries none).
    await provenance.record_event(
        workspace_id, obs_handle, ProvenanceEventType.ROUTED, detail=decision.trace
    )
    job_handle = await store.put_handle(
        workspace_id,
        HandleType.JOB,
        payload={"obs_handle": obs_handle, "dataset_handle": dataset_handle},
    )
    spec = RequestSpec.from_plan(
        plan,
        decision=decision,
        caps=caps,
        workspace_id=workspace_id,
        job_handle=job_handle,
        obs_handle=obs_handle,
        time_range=time_range,
        variables=variables,
        opendap_urls=opendap_urls,
        coord_lat=coord_lat if opendap_urls else None,
        coord_lon=coord_lon if opendap_urls else None,
        coord_time=coord_time if opendap_urls else None,
        lat_axis=lat_axis,
        lon_axis=lon_axis,
        var_dims=var_dims,
    ).to_jsonb()

    job_id = uuid4().hex
    async with session_factory() as session:
        await crud.create_job(
            session,
            job_id=job_id,
            job_handle=job_handle,
            obs_handle=obs_handle,
            provider=decision.path,
            request_spec=spec,
        )

    # Provenance keyed to the durable spec, never a staged URL (CLAUDE.md hard rule).
    await provenance.record_edge(
        workspace_id,
        target_handle=obs_handle,
        source_handle=dataset_handle,
        request_spec=spec,
    )

    await enqueue_fn("submit_job", job_id)

    return {
        "job_handle": job_handle,
        "obs_handle": obs_handle,
        "status": JobState.PENDING.value,
        "provider": decision.path,
    }


async def _resolve_handles(
    dataset_handle: str,
    aoi_handle: str | None,
    workspace_id: str,
    store: WorkspaceStore,
) -> tuple[str, tuple[float, float, float, float] | None]:
    """Resolve a ``dataset_`` (and optional ``aoi_``) handle → ``(concept_id, bbox)``.

    A thin composition of the two typed resolvers. ``aoi_handle`` may be
    ``None`` (a global time-series), in which case ``bbox`` is ``None`` — a
    caller decision made here, before ``resolve_aoi`` is ever called.
    """
    concept_id = await resolve_dataset(store, workspace_id, dataset_handle)
    if aoi_handle is None:
        return concept_id, None
    bbox = await resolve_aoi(store, workspace_id, aoi_handle)
    return concept_id, bbox
