"""Durable retrieval tools (PLAN.md §6 Phase 6.3, §4.3).

Five tools on the durable job model. A retrieval call does **not** block on
Harmony — it plans, persists a durable job, and hands the agent two handles:

* a ``job_`` handle — pollable (:func:`get_retrieval_status`) and cancellable
  (:func:`cancel_retrieval`); its state lives in Postgres, never in memory, so a
  worker restart never loses it.
* an ``obs_`` handle — the eventual result, resolved once the job is ``ready``.

The out-of-process worker (``jobs/worker.py``) drives submit → poll → materialize.
The tool's job is the **planning** half: resolve handles, fetch the merged
:class:`CollectionCapabilities`, route the plan to one service (fail fast with
:class:`NotRetrievable` if none fits — never a Harmony fallback), persist the
**durable request spec** (re-materializable, never a staged URL), and enqueue.

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
from earthdata_mcp.providers.opendap import OPeNDAPProvider
from earthdata_mcp.providers.router import Router, RoutingDecision
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.workspace.models import HandleType, handle_type_of
from earthdata_mcp.workspace.provenance import ProvenanceStore
from earthdata_mcp.workspace.store import WorkspaceStore

#: An ``enqueue_fn`` enqueues ``(task_name, *args)`` onto the worker queue. The
#: default wraps an Arq pool; tests inject an ``AsyncMock`` so no Redis is needed.
EnqueueFn = Callable[..., Awaitable[object]]

#: Default output media type per result shape (§4.4 format-by-shape).
_FORMAT_BY_SHAPE = {
    "grid": "application/zarr",
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
    """Retrieve a variable + bbox + temporal subset; routes only to a service that
    does all three (PLAN.md §4.2 — one whole service, never the union)."""
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

    # Discover an OPeNDAP URL only when Harmony has no capable services — avoids
    # an extra CMR granule search on the Harmony-serviced path (the common case).
    opendap_url: str | None = None
    if (
        not needs_point_sample
        and not caps.services
        and caps.output_shape in ("grid", "swath")
        and bbox is not None
    ):
        opendap_url = await _discover_opendap_url(cmr, concept_id, bbox, time_range)

    # Format by shape; fall back to netCDF when no Harmony services are available
    # and OPeNDAP is the only transform path (OPeNDAP returns netCDF, not Zarr).
    if needs_point_sample:
        fmt = output_format or _FORMAT_BY_SHAPE["point"]
    elif output_format is not None:
        fmt = output_format
    elif caps.output_shape == "grid" and not caps.services and opendap_url:
        fmt = _DEFAULT_FORMAT  # "application/netcdf4" — OPeNDAP produces netCDF, not Zarr
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
    coord_lat, coord_lon = "lat", "lon"
    if opendap_url:
        coord_lat, coord_lon = await _discover_coordinate_names(cmr, concept_id)
    opendap_provider = (
        OPeNDAPProvider(caps, opendap_url=opendap_url, coord_lat=coord_lat, coord_lon=coord_lon)
        if opendap_url
        else None
    )
    decision = Router(caps, appeears=AppEEARSProvider(caps), opendap=opendap_provider).route(plan)
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
        opendap_url=opendap_url,
        coord_lat=coord_lat if opendap_url else None,
        coord_lon=coord_lon if opendap_url else None,
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
    opendap_url: str | None = None,
    coord_lat: str | None = None,
    coord_lon: str | None = None,
) -> dict:
    """Assemble the durable, re-materializable request spec stored in JSONB.

    Everything the worker needs to rebuild the plan and everything provenance
    needs to rebuild the result — and nothing ephemeral. No staged-output URL ever
    enters this dict (the provenance store rejects one if it slips in).
    ``opendap_url`` is a durable granule endpoint (re-materializable on re-run),
    not a staged output URL, so it is safe to persist here (PLAN.md §4.5).
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
        "opendap_url": opendap_url,
        "coord_lat": coord_lat,
        "coord_lon": coord_lon,
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


_LAT_CANONICAL = frozenset({"lat", "latitude"})
_LON_CANONICAL = frozenset({"lon", "longitude"})


async def _discover_coordinate_names(
    cmr: CMRProvider,
    concept_id: str,
) -> tuple[str, str]:
    """Return ``(lat_name, lon_name)`` from CMR UMM-V for this collection.

    Priority:
    1. Variable whose ``standard_name`` is ``"latitude"`` / ``"longitude"``.
    2. Variable whose ``name`` itself is a canonical coordinate name (covers
       collections where UMM-V has no StandardName but the variable is named
       ``"latitude"``/``"longitude"``).
    3. Default ``("lat", "lon")`` when UMM-V has no coordinate metadata or
       the lookup fails.
    """
    try:
        variables = await cmr.get_variables(concept_id)
    except Exception:
        return "lat", "lon"
    lat_name, lon_name = "lat", "lon"
    for var in variables:
        sn = (var.get("standard_name") or "").lower()
        name = var.get("name") or ""
        if not name:
            continue
        name_lower = name.lower()
        if sn == "latitude" or (sn == "" and name_lower in _LAT_CANONICAL):
            lat_name = name
        elif sn == "longitude" or (sn == "" and name_lower in _LON_CANONICAL):
            lon_name = name
    return lat_name, lon_name


async def _discover_opendap_url(
    cmr: CMRProvider,
    concept_id: str,
    bbox: tuple[float, float, float, float] | None,
    time_range: str,
) -> str | None:
    """Search one granule and extract its OPeNDAP access URL from RelatedUrls.

    Mirrors the discovery pattern in ``tests/live/test_opendap_subset.py``.
    Returns ``None`` if no granules are found or none advertise an OPeNDAP URL.
    """
    bbox_str = ",".join(str(c) for c in bbox) if bbox is not None else None
    granules = await cmr.search_granules(
        concept_id,
        bounding_box=bbox_str,
        temporal=time_range or None,
        limit=1,
    )
    if not granules:
        return None
    for entry in granules[0].get("related_urls", []):
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
