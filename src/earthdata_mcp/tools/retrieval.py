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

``retrieve_subset`` is additionally idempotent per workspace (PRD 020): before
planning, it computes a canonical signature of the request and looks it up
among the workspace's existing ``obs_`` handles (see :func:`_check_reuse`),
reusing an exact match instead of enqueueing a duplicate job.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit
from uuid import uuid4

from earthdata_mcp.config import get_settings
from earthdata_mcp.db import create_engine, create_session_factory
from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.state import LEGAL_TRANSITIONS, TERMINAL_STATES, JobState
from earthdata_mcp.providers import build as _build_provider
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.appeears import AppEEARSProvider
from earthdata_mcp.providers.base import (
    AOI,
    JobRef,
    RetrievalPlan,
    RetrievalProvider,
    TimeRange,
    TransformSpec,
    normalize_time_range,
)
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.providers.harmony import HarmonyProvider
from earthdata_mcp.providers.identity import (
    EDLIdentity,
    decrypt_credential,
    encrypt_credential,
    ensure_credential_key,
    parse_edl_token,
    service_identity_scope,
)
from earthdata_mcp.providers.opendap import (
    AxisGeometry,
    OPeNDAPProvider,
    VarDimPlan,
    plan_subset,
    validate_variables,
)
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.providers.request_spec import (
    InvalidSpecError,
    RequestSpec,
    compute_request_signature,
)
from earthdata_mcp.providers.router import Router
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.workspace.handles import resolve_aoi, resolve_dataset
from earthdata_mcp.workspace.models import HandleType, ProvenanceEventType, handle_type_of
from earthdata_mcp.workspace.provenance import ProvenanceStore
from earthdata_mcp.workspace.store import (
    CrossWorkspaceError,
    HandleNotFoundError,
    HandleRecord,
    WorkspaceStore,
)

logger = logging.getLogger(__name__)

#: An ``enqueue_fn`` enqueues ``(task_name, *args)`` onto the worker queue. The
#: default wraps an Arq pool; tests inject an ``AsyncMock`` so no Redis is needed.
EnqueueFn = Callable[..., Awaitable[object]]

#: A ``provider_factory`` builds the ``RetrievalProvider`` ``cancel_retrieval`` calls
#: ``cancel`` on, from the job's own durable spec. The default (``_default_cancel_provider``)
#: never re-fetches CMR capabilities (PRD 021: cancel must stay fast); tests inject a
#: fake provider instead of a real Harmony/AppEEARS client.
CancelProviderFactory = Callable[[RequestSpec], RetrievalProvider]

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

#: A qualitative, provider-agnostic label per durable state (PRD 007) — the
#: signal a caller falls back to when no numeric ``progress`` exists, so a
#: long-queued job never looks indistinguishable from a stalled one.
_PHASE_BY_STATE: dict[str, str] = {
    JobState.PENDING.value: "queued",
    JobState.SUBMITTED.value: "queued at provider",
    JobState.RUNNING.value: "processing",
    JobState.MATERIALIZING.value: "materializing",
    JobState.READY.value: "ready",
    JobState.FAILED.value: "failed",
    JobState.EXPIRED.value: "expired",
    JobState.CANCELLED.value: "cancelled",
}

#: PRD 014 — a well-formed ``job_`` handle that resolves to nothing is a
#: structured answer, not a raised error. Unknown-to-the-store and
#: cross-workspace collapse to this same message so handle existence never
#: leaks across a workspace boundary; only the orphan case (handle visible in
#: the caller's own workspace, no jobs row behind it) gets a distinct message.
_NOT_FOUND_MESSAGE = "no job found for handle {handle!r}"
_ORPHANED_HANDLE_MESSAGE = (
    "handle {handle!r} exists in this workspace but has no job record behind it"
)

# Lazily-built process defaults. Stay ``None`` until the first un-injected call so
# importing this module (and server.py) builds no engine and opens no connection.
_session_factory = None
_provenance: ProvenanceStore | None = None
_pool = None

#: AppEEARS product ids confirmed in-catalog, for the process lifetime — so the
#: planning-time fail-fast (WS4) costs nothing on repeat plans against the same
#: collection. Only successes are cached: a catalog miss is never assumed durable.
_appeears_catalog_cache: set[str] = set()


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
    edl_token: str | None = None,
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

    ``time_range`` accepts ``start/end`` or ``start,end`` with ISO-8601
    endpoints (date-only or full timestamp) — the same grammar the coverage
    tools accept, so a string copied from ``check_coverage`` always parses
    here too. Both bounds are required (no open ranges) and empty input is
    rejected before any handle, CMR, or DB call.
    """
    return await _submit_retrieval(
        dataset_handle=dataset_handle,
        aoi_handle=aoi_handle,
        time_range=time_range,
        variables=(),
        workspace_id=workspace_id,
        output_format=output_format,
        edl_token=edl_token,
        needs_bbox=True,
        needs_variable=False,
        needs_temporal=True,
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
    edl_token: str | None = None,
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
    the chain. OPeNDAP is the worker's runtime fallback if that submit fails.

    ``time_range`` accepts ``start/end`` or ``start,end`` with ISO-8601
    endpoints (date-only or full timestamp) — the same grammar the coverage
    tools accept. Both bounds are required (no open ranges) and empty input is
    rejected before any handle, CMR, or DB call.

    Idempotent per workspace (PRD 020): a request whose canonical signature
    (resolved dataset, resolved AOI, normalized time bounds, variables,
    output format) exactly matches an earlier one in this workspace reuses
    that handle instead of enqueueing a new job — ``reused: true`` is added to
    the response. A materialized match returns immediately; an in-flight match
    collapses onto it; an evicted match re-enqueues via the same durable-replay
    path ``rematerialize`` uses, keeping the same ``obs_`` handle identity.
    Exact match only — a strict subset (fewer variables, a narrower time
    range) is a miss, and reuse never crosses a workspace boundary.
    """
    return await _submit_retrieval(
        dataset_handle=dataset_handle,
        aoi_handle=aoi_handle,
        time_range=time_range,
        variables=tuple(variables or ()),
        workspace_id=workspace_id,
        output_format=output_format,
        edl_token=edl_token,
        needs_bbox=True,
        needs_variable=True,
        needs_temporal=True,
        needs_point_sample=False,
        cmr=cmr,
        store=store,
        provenance=provenance,
        session_factory=session_factory,
        enqueue_fn=enqueue_fn,
        dedupe=True,
    )


async def retrieve_timeseries(
    dataset_handle: str,
    time_range: str,
    variables: list[str],
    workspace_id: str = DEFAULT_WORKSPACE,
    output_format: str | None = None,
    aoi_handle: str | None = None,
    point_sample: bool = False,
    edl_token: str | None = None,
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

    ``time_range`` accepts ``start/end`` or ``start,end`` with ISO-8601
    endpoints (date-only or full timestamp) — the same grammar the coverage
    tools accept. Both bounds are required (no open ranges) and empty input is
    rejected before any handle, CMR, or DB call.
    """
    return await _submit_retrieval(
        dataset_handle=dataset_handle,
        aoi_handle=aoi_handle,
        time_range=time_range,
        variables=tuple(variables or ()),
        workspace_id=workspace_id,
        output_format=output_format,
        edl_token=edl_token,
        needs_bbox=aoi_handle is not None and not point_sample,
        needs_variable=True,
        needs_temporal=True,
        needs_point_sample=point_sample,
        cmr=cmr,
        store=store,
        provenance=provenance,
        session_factory=session_factory,
        enqueue_fn=enqueue_fn,
    )


#: The curated slice of ``request_spec`` :func:`get_retrieval_status` projects
#: (PRD 021). Never the raw spec: ``opendap_urls`` alone can carry dozens of
#: URLs and must not cross the wire.
_ENRICHMENT_KEYS = (
    "short_name",
    "variables",
    "aoi_bbox",
    "time_range",
    "provider",
    "output_format",
)


def _status_enrichment(spec: dict) -> dict:
    """Curate ``job.request_spec`` into what a job *pertains to* (PRD 021).

    A field absent from the spec (``None`` — the dataclass's own default for
    an optional field like ``aoi_bbox``/``time_range``) is omitted entirely
    rather than emitted as ``null``, so a consumer never has to distinguish
    "not applicable to this job" from "not yet known." ``granule_count`` is
    derived from ``opendap_urls`` (discovered at planning time for any
    gridded/swath bbox request, regardless of which provider ultimately runs
    it) — the only granule tally the durable spec carries; omitted when no
    OPeNDAP URLs were discovered (e.g. an AppEEARS point-sample job).
    """
    enrichment = {key: spec[key] for key in _ENRICHMENT_KEYS if spec.get(key) is not None}
    opendap_urls = spec.get("opendap_urls")
    if opendap_urls:
        enrichment["granule_count"] = len(opendap_urls)
    return enrichment


async def get_retrieval_status(
    job_handle: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    edl_token: str | None = None,
    *,
    store: WorkspaceStore | None = None,
    session_factory=None,
) -> dict:
    """Read a job's current state **from Postgres** (never from memory, §4.3).

    Resolves the ``job_`` handle within the workspace first, then reads the
    durable row. Returns
    ``{job_handle, obs_handle, status, progress, phase, message, error,
    submitted_at, last_polled_at, provider_job_url}`` plus a curated slice of
    the already-loaded ``request_spec`` — ``short_name``, ``variables``,
    ``aoi_bbox``, ``time_range``, ``provider``, ``output_format``, and
    ``granule_count`` (PRD 021, see :func:`_status_enrichment`) — so a caller
    can tell what a job pertains to without resolving other handles. ``phase``
    is a qualitative, provider-agnostic label derived from the durable state
    (PRD 007) — the signal a caller falls back to when ``progress`` is 0 but
    the job is not actually stalled (e.g. an AppEEARS job still queued at the
    provider). ``message`` is whatever status text the provider reported on
    the last poll, when any.

    A missing job is a valid answer, not an exception (PRD 014): a handle
    unknown to the workspace store, one owned by another workspace, and one
    that exists but has no jobs row (an orphan) all return
    ``{job_handle, status: "not_found", message}`` instead of raising — the
    first two are byte-identical so handle existence never leaks across a
    workspace boundary. A malformed handle (anything not prefixed ``job_``)
    still raises ``ValueError``: that is a caller bug, not a missing job.

    ``edl_token`` (PRD 022) is accepted for surface uniformity and validated
    at the boundary; this read touches only Postgres, so the identity is not
    otherwise consulted.
    """
    parse_edl_token(edl_token)
    store = store or _default_store()
    session_factory = session_factory or _default_session_factory()

    if handle_type_of(job_handle) is not HandleType.JOB:
        raise ValueError(f"expected a job_ handle, got {job_handle!r}")
    # Isolation gate, collapsed to a not_found answer (PRD 014): a handle
    # unknown to the store and one owned by another workspace are
    # indistinguishable to the caller, so existence never leaks.
    try:
        await store.get_handle(workspace_id, job_handle)
    except (HandleNotFoundError, CrossWorkspaceError):
        return {
            "job_handle": job_handle,
            "status": "not_found",
            "message": _NOT_FOUND_MESSAGE.format(handle=job_handle),
        }

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, job_handle)
        if job is None:
            return {
                "job_handle": job_handle,
                "status": "not_found",
                "message": _ORPHANED_HANDLE_MESSAGE.format(handle=job_handle),
            }
        return {
            "job_handle": job.job_handle,
            "obs_handle": job.obs_handle,
            "status": job.state,
            "progress": job.progress,
            "phase": _PHASE_BY_STATE.get(job.state, job.state),
            "message": job.message,
            "error": job.error,
            "submitted_at": job.created_at.isoformat(),
            "last_polled_at": (job.last_polled_at or job.updated_at).isoformat(),
            "provider_job_url": job.provider_job_url,
            **_status_enrichment(job.request_spec),
        }


#: A job whose current state has no legal edge to CANCELLED but also isn't
#: terminal is a friendly refusal, not an illegal-transition error. Derived
#: from the state machine's own transition map (never hardcoded), so a future
#: change to ``LEGAL_TRANSITIONS`` can't silently drift out of sync with this
#: message. Today ``materializing`` is the only such state: the result is
#: already downloaded and being persisted, so it can't be cancelled and will
#: complete shortly.
_NON_CANCELLABLE_NOTE = (
    "job is {state}: the result is already downloaded and being persisted, "
    "so it cannot be cancelled — it will complete shortly"
)


def _cancel_capabilities(spec: RequestSpec) -> CollectionCapabilities:
    """A capabilities stub good enough to construct a cancel-only provider.

    ``cancel`` touches no capability-gated logic (``find_service`` and
    friends, used only by ``can_handle``/``submit``) — it only needs the
    provider constructor's required identity fields, so this never re-fetches
    from CMR. A provider-side cancel must stay fast and must not block the
    tool on a network round-trip nothing here actually needs.
    """
    return CollectionCapabilities(
        concept_id=spec.concept_id or "",
        short_name=spec.short_name or "",
        processing_level="",
        output_shape=spec.output_shape or "grid",
        native_formats=frozenset(),
        direct_s3=None,
        services=[],
    )


def _default_cancel_provider(
    spec: RequestSpec, *, edl_token: str | None = None
) -> RetrievalProvider:
    """Build the provider ``cancel_retrieval`` calls ``cancel`` on, from the
    job's own durable spec — the same ``spec -> provider`` seam the worker
    uses (:func:`earthdata_mcp.providers.build`), bound to a no-network
    capabilities stub (see :func:`_cancel_capabilities`). ``edl_token``
    (PRD 022) makes the upstream cancel authenticate as the job's identity."""
    return _build_provider(spec, _cancel_capabilities(spec), edl_token=edl_token)


def _provider_job_id_from_url(url: str | None) -> str | None:
    """Recover a provider's job/task id from its stored ``provider_job_url``.

    Harmony and AppEEARS both encode the id as the URL's last *path* segment
    (``…/jobs/<uuid>?linktype=…`` / ``…/task/<task_id>``) — mirrors the
    worker's own ``_job_id_from_url`` (``jobs/worker.py``), parsed explicitly
    so a trailing query string never leaks into the id. OPeNDAP's
    ``provider_job_url`` (newline-joined constraint URLs) is never passed
    here — its ``cancel`` is a documented no-op the caller short-circuits to.
    """
    if not url:
        return None
    path = urlsplit(url).path
    return path.rstrip("/").rsplit("/", 1)[-1] or None


async def _attempt_provider_cancel(
    *,
    provider_name: str,
    provider_job_url: str | None,
    request_spec: dict,
    job_handle: str,
    provider_factory: CancelProviderFactory,
) -> str:
    """Best-effort, non-fatal provider-side cancel; returns the ``upstream`` outcome.

    ``"unsupported"`` covers both OPeNDAP (a synchronous provider with no
    durable upstream job) and a job that was cancelled before it was ever
    submitted (no ``provider_job_url`` yet, e.g. a still-``pending`` job) — in
    both cases there is genuinely nothing upstream to cancel, so the provider
    is never even constructed. Any other failure (a malformed spec, or the
    provider's own ``cancel`` raising) is logged and reported as
    ``"error"`` — the local cancel has already committed and is never undone.
    """
    if provider_name == "opendap" or not provider_job_url:
        return "unsupported"
    try:
        spec = RequestSpec.from_jsonb(request_spec)
    except InvalidSpecError:
        logger.exception(
            "job %s: cannot parse request_spec for provider-side cancel", job_handle
        )
        return "error"
    ref = JobRef(
        provider=provider_name,
        provider_job_id=_provider_job_id_from_url(provider_job_url),
        provider_job_url=provider_job_url,
        job_handle=job_handle,
    )
    try:
        provider = provider_factory(spec)
        await provider.cancel(ref)
    except Exception:
        logger.exception(
            "job %s: provider-side cancel failed (provider=%s)", job_handle, provider_name
        )
        return "error"
    return "requested"


async def cancel_retrieval(
    job_handle: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    edl_token: str | None = None,
    *,
    store: WorkspaceStore | None = None,
    session_factory=None,
    provider_factory: CancelProviderFactory | None = None,
) -> dict:
    """Cancel a cancellable job; a friendly non-cancelling response otherwise.

    Three outcomes, all returning ``{job_handle, status, cancelled, ...}``:

    * A cancellable state (pending/submitted/running) is flipped to
      ``cancelled`` locally (``cancelled`` is ``True``) — the worker stops
      polling/materializing, a real local resource release. A best-effort
      provider-side cancel is then attempted (PRD 021) and never fails the
      tool; the response carries ``upstream``: ``"requested"`` (issued to the
      provider), ``"unsupported"`` (OPeNDAP, or a job never submitted
      upstream — nothing to cancel there), or ``"error"`` (the provider
      cancel raised — logged, non-fatal).
    * A non-terminal state with no legal edge to ``cancelled`` in the state
      machine — today only ``materializing`` (the result is already
      downloaded and being persisted) — is left untouched, and the response
      carries ``cancelled: False`` plus a plain-language ``note`` instead of
      the raw ``illegal job state transition`` error the state machine would
      otherwise raise.
    * A terminal state (ready/failed/expired/cancelled) echoes as-is with
      ``cancelled: False`` and ``upstream: "already_terminal"``: a job's
      state can change between when a caller decides to cancel and when the
      call lands (e.g. it finishes or fails first), so this is not an
      illegal request — the caller's intent ("stop this job") is already
      satisfied, upstream included.

    A fourth outcome, a missing job, is likewise not an exception (PRD 014): a
    handle unknown to the workspace store, one owned by another workspace, or
    one that exists but has no jobs row (an orphan) all return
    ``{job_handle, status: "not_found", cancelled: False, message}`` — "stop
    this job" on a gone job is already satisfied. The first two are
    byte-identical so handle existence never leaks across a workspace
    boundary. A malformed handle (anything not prefixed ``job_``) still
    raises ``ValueError``.

    ``edl_token`` (PRD 022): the provider-side cancel authenticates as the
    per-call identity when one is supplied, else as the identity persisted
    (encrypted) on the job row — the identity the job has been running as —
    and only then as the env credential.
    """
    identity = parse_edl_token(edl_token)
    store = store or _default_store()
    session_factory = session_factory or _default_session_factory()

    if handle_type_of(job_handle) is not HandleType.JOB:
        raise ValueError(f"expected a job_ handle, got {job_handle!r}")
    # Isolation gate, collapsed to a not_found answer (PRD 014): see
    # get_retrieval_status for why unknown and cross-workspace must match.
    try:
        await store.get_handle(workspace_id, job_handle)
    except (HandleNotFoundError, CrossWorkspaceError):
        return {
            "job_handle": job_handle,
            "status": "not_found",
            "cancelled": False,
            "message": _NOT_FOUND_MESSAGE.format(handle=job_handle),
        }

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, job_handle)
        if job is None:
            return {
                "job_handle": job_handle,
                "status": "not_found",
                "cancelled": False,
                "message": _ORPHANED_HANDLE_MESSAGE.format(handle=job_handle),
            }
        current = JobState(job.state)
        if current in TERMINAL_STATES:
            return {
                "job_handle": job_handle,
                "status": job.state,
                "cancelled": False,
                "upstream": "already_terminal",
            }
        if JobState.CANCELLED not in LEGAL_TRANSITIONS[current]:
            return {
                "job_handle": job_handle,
                "status": job.state,
                "cancelled": False,
                "note": _NON_CANCELLABLE_NOTE.format(state=current.value),
            }
        # Read the persisted per-job credential BEFORE the transition below:
        # set_state erases it the moment the job goes terminal (PRD 022), and
        # the provider-side cancel still needs to authenticate as that identity.
        credential_encrypted = job.credential_encrypted
        crud.set_state(job, JobState.CANCELLED)
        await session.commit()
        provider_name = job.provider
        provider_job_url = job.provider_job_url
        request_spec = job.request_spec

    cancel_token = identity.token if identity is not None else None
    if cancel_token is None and credential_encrypted:
        try:
            cancel_token = decrypt_credential(credential_encrypted)
        except Exception:
            # The local cancel has already committed; an undecryptable stored
            # credential must not fail the tool — the provider cancel simply
            # proceeds without a per-call identity (and may report "error").
            logger.exception(
                "job %s: cannot decrypt stored credential for provider cancel",
                job_handle,
            )
    if provider_factory is None:
        provider_factory = lambda spec: _default_cancel_provider(  # noqa: E731
            spec, edl_token=cancel_token
        )

    upstream = await _attempt_provider_cancel(
        provider_name=provider_name,
        provider_job_url=provider_job_url,
        request_spec=request_spec,
        job_handle=job_handle,
        provider_factory=provider_factory,
    )
    return {
        "job_handle": job_handle,
        "status": JobState.CANCELLED.value,
        "cancelled": True,
        "upstream": upstream,
    }


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
    dedupe: bool = False,
    edl_token: str | None = None,
) -> dict:
    """Plan → persist a durable job → enqueue. The shared core of the three
    ``retrieve_*`` tools.

    Order matters: routing happens **before** any handle or row is created, so an
    unserviceable request (``NotRetrievable``) leaves no orphan job behind.

    ``dedupe=True`` (``retrieve_subset`` only, PRD 020) additionally computes
    the request's canonical signature right after the output format is known
    and, on an exact match against an existing handle in this workspace,
    returns the reused handle instead of proceeding — short-circuiting before
    the OPeNDAP granule search, routing, and any handle/job creation.
    ``retrieve_data``/``retrieve_timeseries`` leave this ``False`` and are
    unaffected.

    ``edl_token`` (PRD 022) is the caller's per-user EDL credential. Parsed
    once here — before any handle, row, or network call — into an opaque
    :class:`EDLIdentity` (a malformed/expired paste fails fast with its
    classification), threaded through every NASA-facing planning call, and
    persisted Fernet-encrypted on the job row so the durable worker keeps
    acting as this identity for the job's whole life. Absent means the
    env-credential path, byte-for-byte today's behavior.
    """
    # Parse the per-call identity first (PRD 022): a bad token must fail before
    # any handle or job row exists, and a supplied token needs the repo-private
    # encryption key or the durable job could never authenticate later.
    identity = parse_edl_token(edl_token)
    if identity is not None:
        ensure_credential_key()

    # allow_open=False: TimeRange.from_cmr below needs both bounds to build a
    # closed range, unlike the coverage tools' open-range-friendly grammar.
    time_range = normalize_time_range(time_range, allow_open=False)

    cmr = cmr or CMRProvider(edl_token=identity.token if identity else None)
    store = store or _default_store()
    provenance = provenance or _default_provenance()
    session_factory = session_factory or _default_session_factory()
    enqueue_fn = enqueue_fn or _default_enqueue

    concept_id, bbox = await _resolve_handles(
        dataset_handle, aoi_handle, workspace_id, store
    )

    caps = await cmr.collection_capabilities(concept_id)

    # Format by shape (grid/swath → netCDF-4). An explicit caller format still wins.
    # Computed here — before OPeNDAP discovery/routing below — because a dedupe
    # signature needs the final format and a hit should short-circuit both.
    if needs_point_sample:
        fmt = output_format or _FORMAT_BY_SHAPE["point"]
    elif output_format is not None:
        fmt = output_format
    else:
        fmt = _FORMAT_BY_SHAPE.get(caps.output_shape, _DEFAULT_FORMAT)

    signature = None
    if dedupe:
        identity_scope = await _reuse_identity_scope(
            caps, concept_id, identity, session_factory
        )
        signature = compute_request_signature(
            concept_id=concept_id,
            bbox=bbox,
            time_range=time_range,
            variables=variables,
            output_format=fmt,
            identity_scope=identity_scope,
        )
        reused = await _check_reuse(
            store=store,
            session_factory=session_factory,
            provenance=provenance,
            enqueue_fn=enqueue_fn,
            workspace_id=workspace_id,
            signature=signature,
            edl_token=identity.token if identity else None,
        )
        if reused is not None:
            return reused

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

    # Plan-time gate (PRD 002): reject an unknown/typo'd variable before any job
    # row or handle exists, reusing plan_subset's UMM-V fetch above rather than a
    # second CMR round-trip. A no-op when opendap_plan wasn't computed (no bbox,
    # point-sample) or its collection has no registered UMM-V catalog.
    if needs_variable and variables and opendap_plan is not None:
        validate_variables(opendap_plan.cmr_vars, variables)

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
    var_coords: dict[str, tuple[str, str]] = {}
    variable_expansions: dict[str, tuple[str, ...]] = {}
    if opendap_plan is not None:
        coord_lat, coord_lon = opendap_plan.coord_lat, opendap_plan.coord_lon
        coord_time = opendap_plan.coord_time
        lat_axis, lon_axis, var_dims = (
            opendap_plan.lat_axis, opendap_plan.lon_axis, opendap_plan.var_dims
        )
        var_coords = opendap_plan.var_coords
        variable_expansions = opendap_plan.variable_expansions
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
            var_coords=var_coords,
        )
        if opendap_urls
        else None
    )
    decision = Router(
        caps,
        harmony=harmony_provider,
        # The planning-time catalog fail-fast below does a real AppEEARS lookup,
        # so the per-call identity rides it too (PRD 022).
        appeears=AppEEARSProvider(caps, edl_token=identity.token if identity else None),
        opendap=opendap_provider,
    ).route(plan)

    # Planning-time AppEEARS catalog fail-fast (WS4): a point-sample plan routed
    # here would otherwise mint a job/handle for a product AppEEARS submit later
    # rejects. Runs only on this path, before any handle exists, and reuses the
    # provider's existing product lookup — the provider's own submit-time check
    # stays, so a resumed job remains safe even past this cache's lifetime.
    if decision.path == "appeears":
        await _check_appeears_catalog(decision.provider)

    # Mint the two handles, then persist the durable spec that ties them together.
    # ``signature`` (PRD 020, ``dedupe=True`` only) rides in the obs_ handle's own
    # payload so a later identical request finds this one via
    # ``find_handle_by_payload_value`` — no parallel index.
    obs_payload = {"status": "pending"}
    if signature is not None:
        obs_payload["signature"] = signature
        # Atomic insert against the partial unique index on (workspace_id,
        # signature) — the safety net for PRD 020 story 6: two concurrent
        # identical retrieve_subset calls can both miss the earlier
        # SELECT-based ``_check_reuse`` above (a classic check-then-act race),
        # but only one of them wins this insert. The loser collapses onto the
        # winner's handle exactly like a sequential reuse hit, so at most one
        # job is ever enqueued per signature.
        obs_record, created = await store.put_handle_unique_signature(
            workspace_id, HandleType.OBS, obs_payload
        )
        if created:
            obs_handle = obs_record.handle
        else:
            reused = await _await_reuse_after_conflict(
                obs_record,
                store=store,
                session_factory=session_factory,
                provenance=provenance,
                enqueue_fn=enqueue_fn,
                workspace_id=workspace_id,
                signature=signature,
                edl_token=identity.token if identity else None,
            )
            if reused is not None:
                return reused
            # The winner's own job died (FAILED/CANCELLED) or genuinely never
            # landed even after the retry above — the same "miss" verdict
            # _check_reuse's early SELECT path would reach, but the signature
            # slot is already claimed, so recovery means submitting a fresh
            # job under its existing handle rather than minting a new one.
            # Reset the payload to "pending" (a no-op for the FAILED/CANCELLED
            # case, which never left "pending" to begin with; load-bearing
            # only for the defensive expired-with-no-job case, so a live job
            # underneath is never masked by a stale "expired" status).
            obs_handle = obs_record.handle
            await store.update_handle(workspace_id, obs_handle, {"status": "pending"})
    else:
        obs_handle = await store.put_handle(workspace_id, HandleType.OBS, payload=obs_payload)
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
        var_coords=var_coords,
        variable_expansions=variable_expansions,
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
            # PRD 022: the per-call credential rides the job row encrypted (the
            # worker decrypts it per attempt); the queue payload below stays
            # job_id-only — no credential ever enters Redis.
            credential_encrypted=(
                encrypt_credential(identity.token) if identity is not None else None
            ),
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


# -- idempotent dedupe (PRD 020) -------------------------------------------


async def _reuse_identity_scope(
    caps: CollectionCapabilities,
    concept_id: str,
    identity: EDLIdentity | None,
    session_factory,
) -> str | None:
    """The credential-identity dimension for this request's dedupe signature
    (PRD 022): ``None`` — the globally shared cache — only for a collection CMR
    affirmatively reports unrestricted *and* that no EULA-classified failure
    has ever durably flagged; restricted or unknown scopes to the fetching
    identity (the EDL username behind the token, or the service identity for
    env-credential fetches). Conservative default: unknown → per-identity.
    """
    restricted = caps.access_restricted
    if restricted is not True:
        async with session_factory() as session:
            flagged = await crud.collection_restriction(session, concept_id)
        if flagged:
            restricted = True
    if restricted is False:
        return None
    return identity.scope_key if identity is not None else service_identity_scope()


async def _check_reuse(
    *,
    store: WorkspaceStore,
    session_factory,
    provenance: ProvenanceStore,
    enqueue_fn: EnqueueFn,
    workspace_id: str,
    signature: str,
    edl_token: str | None = None,
) -> dict | None:
    """Exact-match reuse lookup for ``retrieve_subset`` (PRD 020).

    Looks up ``signature`` among the workspace's existing ``obs_`` handles
    (never across a workspace boundary) and, on a hit, delegates to
    :func:`_reuse_response_for_match` to build the response. ``None`` means a
    miss (no match, or a match that turned out unusable) — the caller proceeds
    with a normal submission.

    This is a read-only pre-check, cheap insurance against the wasted
    OPeNDAP/routing work below when a duplicate is likely; it is not itself
    the concurrency guarantee (two callers can both pass it before either has
    written a handle). See :func:`_submit_retrieval`'s atomic
    ``put_handle_unique_signature`` insert for that.
    """
    match = await store.find_handle_by_payload_value(
        workspace_id, HandleType.OBS, "signature", signature
    )
    if match is None:
        return None
    return await _reuse_response_for_match(
        match,
        store=store,
        session_factory=session_factory,
        provenance=provenance,
        enqueue_fn=enqueue_fn,
        workspace_id=workspace_id,
        signature=signature,
        edl_token=edl_token,
    )


async def _reuse_response_for_match(
    match: HandleRecord,
    *,
    store: WorkspaceStore,
    session_factory,
    provenance: ProvenanceStore,
    enqueue_fn: EnqueueFn,
    workspace_id: str,
    signature: str,
    edl_token: str | None = None,
) -> dict | None:
    """Build the reuse response for an ``obs_`` handle already known to match
    ``signature`` — shared by :func:`_check_reuse`'s pre-submit SELECT and
    :func:`_submit_retrieval`'s atomic-insert conflict fallback (PRD 020 story
    6): a duplicate discovered by the database's unique constraint is handled
    identically to one discovered by the earlier lookup.

    Returns one of three shapes, matching a normal ``retrieve_subset``
    response plus ``reused: true``:

    * materialized (handle payload ``status == "ready"``) — the existing
      handles, no enqueue.
    * still in flight (a non-terminal job actually behind the ``pending``
      payload) — the existing ``job_`` handle, no enqueue (collapses a
      duplicate double-fire onto the job already running).
    * evicted (``status == "expired"``) — re-enqueued via
      :func:`_reenqueue_replay`, the same durable-replay path
      ``rematerialize`` uses, keeping the same ``obs_`` handle identity.

    ``None`` means the match is unusable and the caller must treat it as a
    miss:

    * an evicted handle with no job row to replay from, or
    * a payload frozen at ``pending`` behind a job that is actually FAILED or
      CANCELLED (or has no job row at all). The worker only ever writes
      ``"ready"`` on success and ``"expired"`` on eviction — a failed or
      cancelled job never touches the handle payload, so without this check a
      dead job would look identical to one still running and the matching
      signature would be permanently un-retryable.

    Records a ``reused`` provenance event on the existing handle for every hit
    (never a new lineage node — reuse is an event, not a derivation).
    """
    async with session_factory() as session:
        job = await crud.get_job_by_obs_handle(session, match.handle)

    status = match.payload.get("status")
    if status == "expired":
        if job is None:
            # No job to replay from — not a usable match; fall through to a
            # normal submission rather than handing back a dead end.
            return None
        result = await _reenqueue_replay(
            handle=match.handle,
            workspace_id=workspace_id,
            job=job,
            store=store,
            provenance=provenance,
            session_factory=session_factory,
            enqueue_fn=enqueue_fn,
            edl_token=edl_token,
        )
    elif status == "ready":
        result = {
            "job_handle": job.job_handle if job is not None else None,
            "obs_handle": match.handle,
            "status": status,
            "provider": job.provider if job is not None else None,
        }
    else:
        # status is "pending" (or unset). That alone can't distinguish a job
        # still legitimately in flight from one that died without ever
        # touching the handle — treat a dead or missing job the same as the
        # expired-with-no-job case above: not a usable match.
        if job is None or JobState(job.state) in (JobState.FAILED, JobState.CANCELLED):
            return None
        result = {
            "job_handle": job.job_handle,
            "obs_handle": match.handle,
            "status": status or "unknown",
            "provider": job.provider,
        }

    await provenance.record_event(
        workspace_id,
        match.handle,
        ProvenanceEventType.REUSED,
        detail={"signature": signature, "status": status},
    )
    result["reused"] = True
    return result


#: Bounds for :func:`_await_reuse_after_conflict`'s retry of a just-lost
#: ``put_handle_unique_signature`` race. The winner's obs_ handle insert and
#: its job-row insert are two separate commits a few awaits apart (routing
#: provenance, a job_ handle mint, spec serialization all sit in between) — a
#: loser that looks up the winner's job immediately can catch that window and
#: find none yet. In practice this resolves in low single-digit milliseconds;
#: the bound exists only to cap how long a genuinely stuck/dead winner (a
#: crash between its two commits) makes this call wait before giving up.
_RACE_RETRY_ATTEMPTS = 20
_RACE_RETRY_DELAY_SECONDS = 0.05


async def _await_reuse_after_conflict(
    match: HandleRecord,
    *,
    store: WorkspaceStore,
    session_factory,
    provenance: ProvenanceStore,
    enqueue_fn: EnqueueFn,
    workspace_id: str,
    signature: str,
    edl_token: str | None = None,
) -> dict | None:
    """Resolve the loser side of a ``put_handle_unique_signature`` race (PRD
    020 story 6): the winner's obs_ handle is already committed (that's *why*
    this call lost), but its job row may not be yet.

    :func:`_reuse_response_for_match` treats "handle exists, no job behind
    it" as a dead match — correct for its other caller (:func:`_check_reuse`'s
    plain SELECT, where no concurrent insert is in flight and a jobless
    handle really is orphaned). Applied naively here it would misread the
    winner's brief window mid-commit as a dead job and mint a second job for
    the same signature — exactly the bug this whole mechanism exists to
    prevent. Retrying briefly closes that window; only a winner that never
    finishes (crashes between its two commits) exhausts the retries and falls
    through to the dead-match ``None`` a caller must recover from by
    submitting fresh.
    """
    for attempt in range(_RACE_RETRY_ATTEMPTS):
        reused = await _reuse_response_for_match(
            match,
            store=store,
            session_factory=session_factory,
            provenance=provenance,
            enqueue_fn=enqueue_fn,
            workspace_id=workspace_id,
            signature=signature,
            edl_token=edl_token,
        )
        if reused is not None:
            return reused
        if attempt < _RACE_RETRY_ATTEMPTS - 1:
            await asyncio.sleep(_RACE_RETRY_DELAY_SECONDS)
    return None


async def _reenqueue_replay(
    *,
    handle: str,
    workspace_id: str,
    job: Job,
    store: WorkspaceStore,
    provenance: ProvenanceStore,
    session_factory,
    enqueue_fn: EnqueueFn,
    edl_token: str | None = None,
) -> dict:
    """Re-enqueue the durable job behind ``handle`` from ``job``'s already-
    persisted request spec — never inventing a new spec, never accepting
    overrides. Mints a fresh ``job_`` handle but keeps ``handle`` itself
    (the ``obs_``/``cube_`` identity never changes).

    Shared by :func:`~earthdata_mcp.tools.export.rematerialize` (PRD 017,
    caller-initiated recovery) and :func:`_check_reuse`'s evicted branch
    (PRD 020, submit-time reuse) — one durable-replay path, two callers.

    ``edl_token`` (PRD 022) is the *current call's* identity, encrypted onto
    the replay job row. The original job's own credential was erased when it
    reached its terminal state, so a replay never resurrects a stale token —
    the caller either supplies its identity afresh or the replay runs as the
    service account, exactly like a fresh submission.
    """
    new_job_id = uuid4().hex
    new_job_handle = await store.put_handle(
        workspace_id, HandleType.JOB, payload={"obs_handle": handle}
    )
    await store.update_handle(workspace_id, handle, {"status": "pending"})
    async with session_factory() as session:
        await crud.create_job(
            session,
            job_id=new_job_id,
            job_handle=new_job_handle,
            obs_handle=handle,
            provider=job.provider,
            request_spec=job.request_spec,
            credential_encrypted=(
                encrypt_credential(edl_token) if edl_token else None
            ),
        )
    await provenance.record_event(
        workspace_id,
        handle,
        ProvenanceEventType.RE_MATERIALIZED,
        detail={"job_handle": new_job_handle, "previous_job_handle": job.job_handle},
    )
    await enqueue_fn("submit_job", new_job_id)

    return {
        "job_handle": new_job_handle,
        "obs_handle": handle,
        "status": JobState.PENDING.value,
        "provider": job.provider,
    }


async def _check_appeears_catalog(appeears: AppEEARSProvider) -> None:
    """Fail fast if this collection's product isn't in the AppEEARS catalog.

    Reuses :meth:`AppEEARSProvider.check_product_in_catalog` (the same lookup
    ``submit`` performs) and re-raises its ``RuntimeError`` as a ``ValueError`` —
    consistent with this module's other planning-time rejections (e.g. the
    ambiguous-variable raise). Cached per product id for the process lifetime;
    only successes are cached, since a miss may reflect a catalog that changes.
    """
    product = appeears.product_id()
    if product in _appeears_catalog_cache:
        return
    try:
        await appeears.check_product_in_catalog()
    except RuntimeError as exc:
        raise ValueError(str(exc)) from exc
    _appeears_catalog_cache.add(product)


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
