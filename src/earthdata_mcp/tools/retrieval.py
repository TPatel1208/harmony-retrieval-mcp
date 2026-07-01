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

import hashlib
from collections.abc import Awaitable, Callable
from uuid import uuid4

from earthdata_mcp.config import get_settings
from earthdata_mcp.db import create_engine, create_session_factory
from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.appeears import AppEEARSProvider
from earthdata_mcp.providers.base import AOI, RetrievalPlan, TimeRange, TransformSpec
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.providers.harmony import HarmonyProvider
from earthdata_mcp.providers.opendap import (
    AxisGeometry,
    OPeNDAPProvider,
    VarDimPlan,
    _discover_grid_geometry,
    _resolve_from_cmr,
)
from earthdata_mcp.providers.router import Router, RoutingDecision
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.workspace.models import HandleType, handle_type_of
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
    """Cancel a non-terminal job. Legal only from pending/submitted/running.

    Cancelling an already-terminal job (ready/failed/expired/cancelled) raises
    :class:`~earthdata_mcp.jobs.state.IllegalTransition` — the state machine is
    the authority, not this tool.
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
    opendap_urls: list[str] = []
    if (
        not needs_point_sample
        and caps.output_shape in ("grid", "swath")
        and bbox is not None
    ):
        opendap_urls = await _discover_opendap_urls(cmr, concept_id, bbox, time_range)

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
    # When OPeNDAP URLs are present we resolve variable names to their full UMM-V
    # paths in one get_variables call: bare leaf names become /<group>/<leaf> for
    # grouped collections (TEMPO), coordinate names are discovered in the same pass.
    # Resolved names flow into both the Harmony primary submit and the OPeNDAP
    # fallback via the durable spec, so neither path re-derives them.
    coord_lat, coord_lon = "lat", "lon"
    lat_axis: AxisGeometry | None = None
    lon_axis: AxisGeometry | None = None
    var_dims: dict[str, VarDimPlan] = {}
    if opendap_urls:
        coord_lat, coord_lon, variables = await _resolve_from_cmr(cmr, concept_id, variables)
        # Grid geometry discovery mirrors coordinate-name discovery — same CMR
        # pass, same fail-soft posture. Only a "grid" collection ever gets a
        # hyperslab; a swath's curvilinear geolocation cannot use a 1D index
        # range, so we do not bother discovering geometry for it.
        if caps.output_shape == "grid":
            lat_axis, lon_axis, var_dims = await _discover_grid_geometry(
                cmr, concept_id, caps.spatial_extent, variables, coord_lat, coord_lon
            )
    harmony_provider = HarmonyProvider(caps)
    opendap_provider = (
        OPeNDAPProvider(
            caps,
            opendap_urls=opendap_urls,
            coord_lat=coord_lat,
            coord_lon=coord_lon,
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
    service_name = decision.service.service_name if decision.service else None

    # Mint the two handles, then persist the durable spec that ties them together.
    obs_handle = await store.put_handle(
        workspace_id, HandleType.OBS, payload={"status": "pending"}
    )
    job_handle = await store.put_handle(
        workspace_id,
        HandleType.JOB,
        payload={"obs_handle": obs_handle, "dataset_handle": dataset_handle},
    )
    spec = _build_spec(
        concept_id=concept_id,
        caps=caps,
        decision=decision,
        service_name=service_name,
        output_format=fmt,
        bbox=bbox,
        time_range=time_range,
        variables=variables,
        needs_bbox=needs_bbox,
        needs_variable=needs_variable,
        needs_temporal=needs_temporal,
        needs_point_sample=needs_point_sample,
        workspace_id=workspace_id,
        job_handle=job_handle,
        obs_handle=obs_handle,
        opendap_urls=opendap_urls,
        coord_lat=coord_lat if opendap_urls else None,
        coord_lon=coord_lon if opendap_urls else None,
        lat_axis=lat_axis,
        lon_axis=lon_axis,
        var_dims=var_dims,
    )

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


def _build_spec(
    *,
    concept_id: str,
    caps: CollectionCapabilities,
    decision: RoutingDecision,
    service_name: str | None,
    output_format: str,
    bbox: tuple[float, float, float, float] | None,
    time_range: str,
    variables: tuple[str, ...],
    needs_bbox: bool,
    needs_variable: bool,
    needs_temporal: bool,
    needs_point_sample: bool,
    workspace_id: str,
    job_handle: str,
    obs_handle: str,
    opendap_urls: list[str] | None = None,
    coord_lat: str | None = None,
    coord_lon: str | None = None,
    lat_axis: AxisGeometry | None = None,
    lon_axis: AxisGeometry | None = None,
    var_dims: dict[str, VarDimPlan] | None = None,
) -> dict:
    """Assemble the durable, re-materializable request spec stored in JSONB.

    Everything the worker needs to rebuild the plan and everything provenance
    needs to rebuild the result — and nothing ephemeral. No staged-output URL ever
    enters this dict (the provenance store rejects one if it slips in).
    ``opendap_urls`` are durable granule endpoints (re-materializable on re-run),
    not staged output URLs, so they are safe to persist here (PLAN.md §4.5).
    ``opendap_url`` (the first) is kept for specs/readers that expect the singular key.
    ``lat_axis``/``lon_axis``/``var_dims`` are the discovered grid geometry,
    recorded so a re-planned submit reproduces the identical DAP4 hyperslab.
    """
    return {
        "concept_id": concept_id,
        "short_name": caps.short_name,
        "output_format": output_format,
        "output_shape": caps.output_shape,
        "needs_bbox": needs_bbox,
        "needs_variable": needs_variable,
        "needs_temporal": needs_temporal,
        "needs_point_sample": needs_point_sample,
        "aoi_bbox": list(bbox) if bbox is not None else None,
        "time_range": time_range or None,
        "variables": list(variables),
        "provider": decision.path,
        "service_name": service_name,
        "opendap_urls": list(opendap_urls) if opendap_urls else None,
        "opendap_url": opendap_urls[0] if opendap_urls else None,
        "coord_lat": coord_lat,
        "coord_lon": coord_lon,
        "lat_axis": _serialize_axis(lat_axis),
        "lon_axis": _serialize_axis(lon_axis),
        "var_dims": _serialize_var_dims(var_dims),
        "workspace_id": workspace_id,
        "job_handle": job_handle,
        "obs_handle": obs_handle,
        "cache_key": _cache_key(
            short_name=caps.short_name,
            output_format=output_format,
            bbox=bbox,
            time_range=time_range,
            variables=variables,
            service_name=service_name,
            service_version=caps.capabilities_version,
        ),
    }


def _serialize_axis(axis: AxisGeometry | None) -> dict | None:
    """``AxisGeometry`` -> a JSONB-safe dict, or ``None``. See ``_axis_from_spec``."""
    if axis is None:
        return None
    return {"name": axis.name, "origin": axis.origin, "step": axis.step, "length": axis.length}


def _serialize_var_dims(var_dims: dict[str, VarDimPlan] | None) -> dict:
    """Per-variable ``VarDimPlan`` map -> a JSONB-safe dict (tuples -> lists).

    The worker mirror is ``jobs.worker._var_dims_from_spec``.
    """
    if not var_dims:
        return {}
    return {var: [list(pair) for pair in dims] for var, dims in var_dims.items()}


def _cache_key(
    *,
    short_name: str,
    output_format: str,
    bbox: tuple[float, float, float, float] | None,
    time_range: str,
    variables: tuple[str, ...],
    service_name: str | None,
    service_version: str,
) -> str:
    """Materialization cache key (§4.4).

    Keyed on ``(short_name, format, aoi, time_range, variables, service,
    service_version)`` — ``service_version`` is in the key because a service's
    output changes across versions, so a cached result from an old version must
    not be reused.
    """
    aoi_str = ",".join(str(c) for c in bbox) if bbox is not None else ""
    raw = ":".join(
        [
            short_name or "",
            output_format,
            aoi_str,
            time_range or "",
            ",".join(sorted(variables)),
            service_name or "",
            service_version or "",
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


#: Cap on granules pulled into a single OPeNDAP bundle (one DAP4 bbox subset each).
#: Matches CMR's per-request granule limit; a wider window is truncated to this.
_OPENDAP_GRANULE_LIMIT = 50


async def _discover_opendap_urls(
    cmr: CMRProvider,
    concept_id: str,
    bbox: tuple[float, float, float, float] | None,
    time_range: str,
) -> list[str]:
    """Search the window's granules and collect each one's OPeNDAP access URL.

    Mirrors the discovery pattern in ``tests/live/test_opendap_subset.py``, but over
    every granule in the AOI+time window (up to :data:`_OPENDAP_GRANULE_LIMIT`) so a
    multi-day request covers the whole span. Returns ``[]`` if no granules are found
    or none advertise an OPeNDAP URL.
    """
    bbox_str = ",".join(str(c) for c in bbox) if bbox is not None else None
    granules = await cmr.search_granules(
        concept_id,
        bounding_box=bbox_str,
        temporal=time_range or None,
        limit=_OPENDAP_GRANULE_LIMIT,
    )
    urls: list[str] = []
    for granule in granules:
        url = _opendap_url_of(granule)
        if url:
            urls.append(url)
    return urls


def _opendap_url_of(granule: dict) -> str | None:
    """Extract a granule's OPeNDAP access URL (sans trailing ``.html``) or ``None``."""
    for entry in granule.get("related_urls", []):
        url = str(entry.get("URL", ""))
        subtype = str(entry.get("Subtype", "")).upper()
        if "OPENDAP" in subtype or "opendap" in url.lower():
            return url[: -len(".html")] if url.endswith(".html") else url
    return None


async def _resolve_handles(
    dataset_handle: str,
    aoi_handle: str | None,
    workspace_id: str,
    store: WorkspaceStore,
) -> tuple[str, tuple[float, float, float, float] | None]:
    """Resolve a ``dataset_`` (and optional ``aoi_``) handle → ``(concept_id, bbox)``.

    Type-checks each prefix before any DB hit, then resolves within
    ``workspace_id`` (cross-workspace access raises). ``aoi_handle`` may be
    ``None`` (a global time-series), in which case ``bbox`` is ``None``.
    """
    if handle_type_of(dataset_handle) is not HandleType.DATASET:
        raise ValueError(f"expected a dataset_ handle, got {dataset_handle!r}")

    dataset_record = await store.get_handle(workspace_id, dataset_handle)
    concept_id = dataset_record.payload.get("concept_id")
    if not concept_id:
        raise ValueError(
            f"dataset handle {dataset_handle!r} payload missing 'concept_id'"
        )

    if aoi_handle is None:
        return concept_id, None

    if handle_type_of(aoi_handle) is not HandleType.AOI:
        raise ValueError(f"expected an aoi_ handle, got {aoi_handle!r}")
    aoi_record = await store.get_handle(workspace_id, aoi_handle)
    bbox = aoi_record.payload.get("bbox")
    if not bbox or len(bbox) != 4:
        raise ValueError(
            f"aoi handle {aoi_handle!r} payload missing or malformed 'bbox'"
        )
    return concept_id, tuple(float(c) for c in bbox)
