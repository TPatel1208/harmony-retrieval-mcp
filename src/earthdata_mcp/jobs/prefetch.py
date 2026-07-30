"""Adjacent-granule prefetch — warm the next window before it is asked for (PRD 025).

Research access is sequential: a researcher who retrieves 1–7 April very often
asks for 8–14 April next, and every one of those is a full cold path. PRD 020's
dedupe helps only on an *exact* repeat, so the near-miss case — the common one in
exploratory work — always pays full price. This module spends a little provider
quota to make that case warm.

Two halves, deliberately separated:

* :func:`predict_next` — the prediction. Pure: a spec in, a spec or ``None``
  out, no I/O and no provider knowledge. There is exactly **one** prediction in
  v1 (the next contiguous window, same everything else) because it has the
  strongest signal and the least ambiguity; adjacent-*variable* and
  adjacent-*collection* prediction are far weaker and belong behind evidence
  from this one.
* :class:`PrefetchService` — the gates and the enqueue. Prefetching spends
  quota, worker capacity and cache bytes on data nobody asked for, so it is
  refused unless **every** gate passes, and each gate answers with its own
  named reason so a refusal is legible in a log line rather than a mystery.

Three properties are load-bearing and none of them are optimizations:

* **It never chains.** A speculative job issues no prefetch of its own, so one
  retrieval can trigger at most one speculative step, never a cascade.
* **It never delays real work.** A pending prefetch is *dropped*, not deferred,
  whenever a user-requested job is in flight — a researcher's job must never
  wait behind a guess.
* **It never costs a researcher their own results.** Prefetched results are
  marked ``speculative`` and the eviction sweep (PRD 024) discards every
  speculative candidate before any user-requested one, regardless of size, cost
  or recency. A prefetch that could evict real data would be strictly worse
  than not prefetching at all.

The whole feature ships **off** (``earthdata_prefetch_enabled``). It earns its
default by measurement — ``prefetch-hit`` over ``prefetch-issued`` in the logs —
not by argument.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from earthdata_mcp.config import Settings, get_settings
from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.eviction import under_fair_share
from earthdata_mcp.providers.base import TimeRange
from earthdata_mcp.providers.identity import (
    decrypt_credential,
    parse_edl_token,
    service_identity_scope,
)
from earthdata_mcp.providers.request_spec import (
    RequestSpec,
    compute_cache_key,
    compute_request_signature,
)
from earthdata_mcp.workspace.models import HandleType, ProvenanceEventType
from earthdata_mcp.workspace.provenance import ProvenanceStore
from earthdata_mcp.workspace.store import (
    CrossWorkspaceError,
    HandleNotFoundError,
    WorkspaceStore,
)

logger = logging.getLogger(__name__)

#: The named log events an operator greps for. ``prefetch-hit`` over
#: ``prefetch-issued`` is the ratio that decides whether this feature stays on;
#: ``prefetch-wasted`` is what it cost when it was wrong.
PREFETCH_ISSUED = "prefetch-issued"
PREFETCH_HIT = "prefetch-hit"
PREFETCH_WASTED = "prefetch-wasted"

#: Every verdict :meth:`PrefetchService.maybe_prefetch` can reach. Named rather
#: than a bare bool because "we did not prefetch" is not one fact — an operator
#: deciding whether this feature is worth its quota needs to know *which* gate
#: keeps refusing, and each of these is asserted in isolation by the tests.
ISSUED = "issued"
DISABLED = "disabled"
CHAINED = "chained"
NO_PREDICTION = "no_prediction"
REAL_WORK_QUEUED = "real_work_queued"
OVER_BUDGET = "over_budget"
OVER_FAIR_SHARE = "over_fair_share"
NRT_WINDOW = "nrt_window"
TOO_LARGE = "too_large"
UNKNOWN_SIZE = "unknown_size"
ALREADY_HELD = "already_held"
RESTRICTED_NO_IDENTITY = "restricted_no_identity"
NO_DATA_FOR_WINDOW = "no_data_for_window"


@dataclass(frozen=True)
class WindowPlan:
    """What a fresh planning pass says about the predicted window.

    ``spec`` is the predicted spec with this window's own discovery outputs
    filled in, or ``None`` when the archive has nothing there — a prediction is
    a guess about what will be *asked for*, not about what exists.
    ``access_restricted`` is the collection's own restriction verdict from the
    same fetch (CMR's ``bool | None``), so the identity gate needs no second
    round-trip.
    """

    spec: RequestSpec | None
    access_restricted: bool | None = None
    capabilities_version: str = ""


#: Re-plans the predicted window. Injected so the gates can be tested without
#: CMR, and so the whole CMR/OPeNDAP dependency sits behind one seam:
#: ``(predicted_spec, edl_token) -> WindowPlan``.
WindowPlanFn = Callable[[RequestSpec, str | None], Awaitable[WindowPlan]]

#: Enqueues ``(task_name, *args)`` onto the worker queue — the same shape
#: ``tools/retrieval``'s ``EnqueueFn`` has, so the prefetch path uses the
#: ordinary durable job queue and not a private one.
EnqueueFn = Callable[..., Awaitable[object]]


@dataclass(frozen=True)
class PrefetchDecision:
    """What :meth:`PrefetchService.maybe_prefetch` did, and why."""

    reason: str
    job_id: str | None = None
    job_handle: str | None = None
    obs_handle: str | None = None
    time_range: str | None = None

    @property
    def issued(self) -> bool:
        return self.reason == ISSUED


def predict_next(spec: RequestSpec) -> RequestSpec | None:
    """The next request this spec predicts, or ``None`` for no prediction.

    ``[T0, T1)`` predicts ``[T1, T1 + (T1 - T0))``: the same length,
    immediately adjacent, with nothing else varied (see
    :meth:`RequestSpec.for_time_range` for exactly what "nothing else" excludes).

    ``None`` — no prediction at all — for the three cases where stepping
    forward is not the pattern this feature observed:

    * no time range: there is no next window without a window;
    * a point sample: a series at a location, not a step through windows;
    * a zero-length or unparseable window: nothing to advance by, and
      predicting the same instant again is a guaranteed duplicate.
    """
    if spec.needs_point_sample or not spec.time_range:
        return None
    try:
        window = TimeRange.from_cmr(spec.time_range)
    except ValueError:
        return None
    span = window.end - window.start
    if span <= timedelta(0):
        return None
    return spec.for_time_range(
        TimeRange(start=window.end, end=window.end + span).to_cmr()
    )


class PrefetchService:
    """Decides whether to warm the next window, and enqueues it when it should.

    Constructed once per worker process (ctx-cached, like
    :class:`~earthdata_mcp.jobs.eviction.EvictionService`) and asked once per
    retrieval that reaches ``READY``. Every dependency that touches the network
    or the queue is injected, so the gates are testable without either.
    """

    def __init__(
        self,
        session_factory,
        *,
        enqueue_fn: EnqueueFn,
        store: WorkspaceStore | None = None,
        provenance: ProvenanceStore | None = None,
        window_plan_fn: WindowPlanFn | None = None,
        settings: Settings | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._store = store or WorkspaceStore(session_factory)
        self._provenance = provenance or ProvenanceStore(session_factory)
        self._enqueue = enqueue_fn
        self._window_plan = window_plan_fn or _plan_window
        self._settings = settings or get_settings()

    async def maybe_prefetch(
        self,
        spec: RequestSpec,
        *,
        speculative_origin: bool,
        size_bytes: int | None,
        credential_encrypted: str | None = None,
        now: datetime | None = None,
    ) -> PrefetchDecision:
        """Issue at most one speculative job for the window ``spec`` predicts.

        ``speculative_origin`` is whether the retrieval that just finished was
        itself a prefetch — the no-chaining gate, and the one this feature can
        least afford to get wrong. ``size_bytes`` is what that retrieval actually
        delivered: for a same-length adjacent window over the same AOI and
        variables it is the best size estimate obtainable, and it costs no CMR
        round-trip to obtain. ``credential_encrypted`` is the originating job's
        own credential, read *before* it reached its terminal state (which erases
        it), so the prefetch runs as the identity that asked for the window
        before this one.

        Gates run cheapest-first, so the two that need I/O (the durable
        restriction flag and the fresh planning pass) are only reached by a
        prediction everything local already agreed to.
        """
        settings = self._settings
        if not settings.earthdata_prefetch_enabled:
            return PrefetchDecision(DISABLED)
        # No chaining, checked before anything else: one speculative step per
        # user request, never a cascade (PRD 025 story 7).
        if speculative_origin:
            return PrefetchDecision(CHAINED)

        predicted = predict_next(spec)
        if predicted is None:
            return PrefetchDecision(NO_PREDICTION)

        workspace_id = spec.workspace_id
        if not workspace_id or not spec.concept_id:
            # A legacy spec with no workspace to charge or collection to fetch
            # cannot be prefetched for; nothing to explain beyond that.
            return PrefetchDecision(NO_PREDICTION)

        async with self._session_factory() as session:
            if await crud.user_work_in_flight(session):
                return PrefetchDecision(REAL_WORK_QUEUED)

        if size_bytes is None:
            return PrefetchDecision(UNKNOWN_SIZE)
        if size_bytes > settings.earthdata_prefetch_max_bytes:
            return PrefetchDecision(TOO_LARGE)

        since = (now or datetime.now(timezone.utc)) - timedelta(days=1)
        async with self._session_factory() as session:
            issued_today = await crud.speculative_jobs_since(session, workspace_id, since)
        if issued_today >= settings.earthdata_prefetch_budget_per_workspace_per_day:
            return PrefetchDecision(OVER_BUDGET)

        if not await under_fair_share(
            self._session_factory,
            workspace_id,
            max_bytes=settings.earthdata_cache_max_bytes,
            extra_bytes=size_bytes,
        ):
            return PrefetchDecision(OVER_FAIR_SHARE)

        if not self._window_is_settled(predicted, now=now):
            return PrefetchDecision(NRT_WINDOW)

        edl_token = _inherited_token(credential_encrypted)
        plan = await self._window_plan(predicted, edl_token)
        if plan.spec is None:
            return PrefetchDecision(NO_DATA_FOR_WINDOW)

        restricted = plan.access_restricted
        if restricted is not True:
            async with self._session_factory() as session:
                if await crud.collection_restriction(session, spec.concept_id):
                    restricted = True
        # Story 8: never fetch what the caller could not have fetched. A
        # restricted collection's entitlements are per-EDL-account, so with no
        # identity to run as there is no defensible identity to speculate under —
        # and bytes fetched as the service account would be scoped to it by
        # PRD 022's cache keying anyway, i.e. unreusable by the researcher.
        if restricted is True and edl_token is None:
            return PrefetchDecision(RESTRICTED_NO_IDENTITY)

        # The dedupe machinery decides sameness, not a second notion of it
        # (PRD 020). Scoping is conservative in the other direction from the
        # skip above: restricted *or unknown* scopes to the fetching identity, so
        # a metadata gap can never leak one user's bytes to another.
        identity_scope = None
        if restricted is not False:
            identity = parse_edl_token(edl_token) if edl_token else None
            identity_scope = (
                identity.scope_key if identity is not None else service_identity_scope()
            )
        signature = compute_request_signature(
            concept_id=spec.concept_id,
            bbox=predicted.aoi_bbox,
            time_range=predicted.time_range,
            variables=plan.spec.variables,
            output_format=predicted.output_format,
            identity_scope=identity_scope,
        )
        existing = await self._store.find_handle_by_payload_value(
            workspace_id, HandleType.OBS, "signature", signature
        )
        if existing is not None:
            return PrefetchDecision(ALREADY_HELD)

        return await self._enqueue_prefetch(
            plan.spec,
            signature=signature,
            capabilities_version=plan.capabilities_version,
            origin_job_handle=spec.job_handle,
            workspace_id=workspace_id,
            credential_encrypted=credential_encrypted,
            size_bytes=size_bytes,
        )

    # -- gates that need no I/O ------------------------------------------------

    def _window_is_settled(self, predicted: RequestSpec, *, now: datetime | None) -> bool:
        """Has the predicted window been complete upstream long enough to fetch?

        Stands in for the collection's latency. Prefetching a window upstream is
        still filling buys a partial result that gets re-fetched the moment the
        rest lands — the quota is spent twice for one window, which is worse than
        not prefetching it at all.
        """
        if not predicted.time_range:
            return False
        try:
            window = TimeRange.from_cmr(predicted.time_range)
        except ValueError:
            return False
        horizon = (now or datetime.now(timezone.utc)) - timedelta(
            hours=self._settings.earthdata_prefetch_min_window_age_hours
        )
        end = window.end
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        return end <= horizon

    # -- the enqueue ----------------------------------------------------------

    async def _enqueue_prefetch(
        self,
        predicted: RequestSpec,
        *,
        signature: str,
        capabilities_version: str,
        origin_job_handle: str | None,
        workspace_id: str,
        credential_encrypted: str | None,
        size_bytes: int,
    ) -> PrefetchDecision:
        """Mint the speculative handles and job, then enqueue it like any other.

        Uses the same atomic ``put_handle_unique_signature`` insert
        ``retrieve_subset`` does, so the race a read-only signature check cannot
        close (a user request for this very window landing between the check
        above and this insert) resolves in the *user's* favour: the prefetch
        loses the insert and is abandoned, never duplicated.
        """
        obs_record, created = await self._store.put_handle_unique_signature(
            workspace_id,
            HandleType.OBS,
            {"status": "pending", "signature": signature, "speculative": True},
        )
        if not created:
            return PrefetchDecision(ALREADY_HELD)
        obs_handle = obs_record.handle

        dataset_handle = await self._origin_dataset_handle(workspace_id, origin_job_handle)
        job_handle = await self._store.put_handle(
            workspace_id,
            HandleType.JOB,
            payload={"obs_handle": obs_handle, "dataset_handle": dataset_handle},
        )
        spec_jsonb = _finalize(
            predicted,
            workspace_id=workspace_id,
            job_handle=job_handle,
            obs_handle=obs_handle,
            capabilities_version=capabilities_version,
        ).to_jsonb()

        job_id = uuid4().hex
        async with self._session_factory() as session:
            await crud.create_job(
                session,
                job_id=job_id,
                job_handle=job_handle,
                obs_handle=obs_handle,
                provider=predicted.provider,
                request_spec=spec_jsonb,
                credential_encrypted=credential_encrypted,
                speculative=True,
            )

        # Provenance keyed to the durable spec, never a staged URL — a prefetched
        # result is a materialization like any other and must be describable.
        if dataset_handle:
            await self._provenance.record_edge(
                workspace_id,
                target_handle=obs_handle,
                source_handle=dataset_handle,
                request_spec=spec_jsonb,
            )
        await self._provenance.record_event(
            workspace_id,
            obs_handle,
            ProvenanceEventType.PREFETCHED,
            detail={
                "predicted_from": origin_job_handle,
                "time_range": predicted.time_range,
                "estimated_bytes": size_bytes,
            },
        )

        await self._enqueue("submit_job", job_id)

        logger.info(
            "%s: %s for %s",
            PREFETCH_ISSUED,
            obs_handle,
            predicted.time_range,
            extra={
                "event": PREFETCH_ISSUED,
                "workspace_id": workspace_id,
                "obs_handle": obs_handle,
                "job_handle": job_handle,
                "time_range": predicted.time_range,
                "estimated_bytes": size_bytes,
            },
        )
        return PrefetchDecision(
            ISSUED,
            job_id=job_id,
            job_handle=job_handle,
            obs_handle=obs_handle,
            time_range=predicted.time_range,
        )

    async def _origin_dataset_handle(
        self, workspace_id: str, origin_job_handle: str | None
    ) -> str | None:
        """The ``dataset_`` handle the originating retrieval descended from.

        Absent for a job minted by the durable-replay path (which carries only
        ``obs_handle``), in which case the prefetched result simply gets no
        lineage *edge* — its ``prefetched`` event still records where it came
        from, and a missing edge must never be the reason a prefetch fails.
        """
        if not origin_job_handle:
            return None
        try:
            record = await self._store.get_handle(workspace_id, origin_job_handle)
        except (HandleNotFoundError, CrossWorkspaceError):
            return None
        return record.payload.get("dataset_handle")


def _finalize(
    predicted: RequestSpec,
    *,
    workspace_id: str,
    job_handle: str,
    obs_handle: str,
    capabilities_version: str,
) -> RequestSpec:
    """Bind a predicted spec to the handles it was just minted for, and give it
    back the cache key :meth:`RequestSpec.for_time_range` could not compute
    without fresh capabilities in hand."""
    return replace(
        predicted,
        workspace_id=workspace_id,
        job_handle=job_handle,
        obs_handle=obs_handle,
        cache_key_value=compute_cache_key(
            short_name=predicted.short_name,
            output_format=predicted.output_format,
            bbox=predicted.aoi_bbox,
            time_range=predicted.time_range,
            variables=predicted.variables,
            service_name=predicted.service_name,
            service_version=capabilities_version,
        ),
    )


def _inherited_token(credential_encrypted: str | None) -> str | None:
    """The originating job's credential in plaintext, or ``None``.

    ``None`` covers both "the original ran on the environment credential" and
    "the stored blob can no longer be decrypted" (a rotated key). Neither is an
    error worth failing a guess over — but neither is it license to speculate on
    a restricted collection, which is why the caller treats ``None`` as "no
    identity available" rather than "run as the service account".
    """
    if not credential_encrypted:
        return None
    try:
        return decrypt_credential(credential_encrypted)
    except Exception:
        logger.warning("prefetch: originating job's credential could not be decrypted")
        return None


async def _plan_window(predicted: RequestSpec, edl_token: str | None) -> WindowPlan:
    """The real planning pass for a predicted window (the default
    :data:`WindowPlanFn`).

    Re-runs exactly the discovery ``tools/retrieval`` does at planning time,
    against the *new* window: fresh capabilities (which also answer the
    restriction question) and, for a gridded/swath bbox request, fresh OPeNDAP
    granule discovery. Re-running it is not optional — the old window's granule
    URLs describe the old window, and reusing them would file April 1–8's data
    under April 8–15's signature.

    A window with no granules at all yields ``spec=None``: there is nothing to
    warm. Imported lazily so importing this module costs no provider imports.
    """
    from earthdata_mcp.providers.cmr import CMRProvider
    from earthdata_mcp.providers.opendap import plan_subset

    cmr = CMRProvider(edl_token=edl_token)
    caps = await cmr.collection_capabilities(predicted.concept_id)

    if predicted.aoi_bbox is None or caps.output_shape not in ("grid", "swath"):
        # Nothing window-specific to rediscover (an AppEEARS point sample never
        # reaches here, and a Harmony-only plan carries no granule URLs).
        return WindowPlan(
            spec=predicted,
            access_restricted=caps.access_restricted,
            capabilities_version=caps.capabilities_version,
        )

    plan = await plan_subset(
        cmr,
        caps,
        predicted.concept_id,
        predicted.aoi_bbox,
        predicted.time_range,
        predicted.variables,
    )
    if not plan.opendap_urls:
        # No granule in the window advertises an OPeNDAP URL. For a job routed to
        # OPeNDAP that is fatal; for a Harmony-routed one it is only the loss of
        # the runtime fallback, and Harmony may still have the window. Either way
        # an empty window is the common cause, so treat it as nothing to warm
        # rather than submit a job that probably has no data behind it.
        return WindowPlan(
            spec=None,
            access_restricted=caps.access_restricted,
            capabilities_version=caps.capabilities_version,
        )

    return WindowPlan(
        spec=replace(
            predicted,
            opendap_urls=tuple(plan.opendap_urls),
            coord_lat=plan.coord_lat,
            coord_lon=plan.coord_lon,
            coord_time=plan.coord_time,
            lat_axis=plan.lat_axis,
            lon_axis=plan.lon_axis,
            var_dims=plan.var_dims,
            var_coords=plan.var_coords,
            variable_expansions=plan.variable_expansions,
            variables=plan.variables,
        ),
        access_restricted=caps.access_restricted,
        capabilities_version=caps.capabilities_version,
    )
