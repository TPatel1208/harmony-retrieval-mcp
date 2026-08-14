"""Stateless Arq worker that drives the durable job lifecycle (PLAN.md §4.3).

The worker owns no state: the Postgres ``jobs`` table is the source of truth. It
exposes three tasks — ``submit_job`` → ``poll_job`` → ``materialize_job`` — that
walk a job through the state machine, each re-enqueueing the next, and two
reclaim passes that put a stranded job back on the queue: a ``startup`` hook
that **reclaims every non-terminal job on boot**, so a process restart never
strands an in-flight job, and a ``sweep_stalled_jobs`` cron that **reclaims
jobs showing no sign of life while the worker keeps running**, so a stall does
not wait on a human noticing and restarting it. The second selects on
staleness, never on non-terminality — see its docstring for why that
distinction is load-bearing.

Run with ``arq earthdata_mcp.jobs.worker.WorkerSettings``.

The harmony-py client, the EDL session, polling, and download all live in
:class:`~earthdata_mcp.providers.harmony.HarmonyProvider`; the worker only
sequences the durable transitions around it (we never hand-roll a Harmony client
— CLAUDE.md hard rule). The provider + plan are rebuilt from the durable
``request_spec`` on every task, so resumption after a restart needs nothing but
the row.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

import httpx
from sqlalchemy import exc as sa_exc

from arq import cron
from arq.connections import RedisSettings
from arq.worker import func

from earthdata_mcp import providers
from earthdata_mcp.config import get_settings
from earthdata_mcp.db import create_engine, create_session_factory
from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.eviction import EvictionService
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import TERMINAL_STATES, JobState
from earthdata_mcp.providers.base import JobRef, RetrievalProvider
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.providers.identity import (
    EULA_NOT_ACCEPTED,
    CredentialError,
    EDLIdentity,
    TokenExpiredError,
    classify_credential_failure,
    decrypt_credential,
    parse_edl_token,
)
from earthdata_mcp.jobs.prefetch import PrefetchService
from earthdata_mcp.providers.materialization import MaterializationRecord
from earthdata_mcp.providers.request_spec import InvalidSpecError, RequestSpec
from earthdata_mcp.redaction import redact
from earthdata_mcp.storage import get_storage_backend
from earthdata_mcp.workspace.models import ProvenanceEventType
from earthdata_mcp.workspace.provenance import ProvenanceStore

logger = logging.getLogger(__name__)


def _configure_harmony_download_concurrency() -> None:
    """Raise harmony-py's hardcoded 3-worker download thread pool.

    harmony.config.Config.NUM_REQUESTS_WORKERS reads the env var live
    (os.getenv) at every Client() construction — HarmonyProvider builds a
    fresh Client per job (providers/harmony.py) — so setting this once here
    raises download concurrency for every job in this process. setdefault so
    an operator's own explicit NUM_REQUESTS_WORKERS (e.g. in .env/docker-
    compose) still wins. Downloads already stream file-to-file (commit
    9802d84), so raising this trades network parallelism for disk I/O and
    open file descriptors, not RAM.

    Called from :func:`startup` — not module scope — because get_settings()
    is @lru_cache'd: calling it at import time would permanently freeze the
    process-wide Settings singleton the instant anything merely imports this
    module (a test importing it for an unrelated helper, e.g.), instead of
    only when the worker actually starts.
    """
    os.environ.setdefault(
        "NUM_REQUESTS_WORKERS", str(get_settings().retrieval_download_concurrency)
    )


# Which task resumes a job found in each non-terminal state on startup (§4.3).
_RESUME_TASK: dict[JobState, str] = {
    JobState.PENDING: "submit_job",
    JobState.SUBMITTED: "poll_job",
    JobState.RUNNING: "poll_job",
    JobState.MATERIALIZING: "materialize_job",
}

# Delay (seconds) before the next poll while a job is still running.
_POLL_INTERVAL = 5

# Longer delay before re-polling after a transient fault, so an outage lasting
# minutes costs a couple of probes a minute rather than one every five seconds.
_POLL_TRANSIENT_RETRY_INTERVAL = 30

#: Faults that say nothing about the job -- only that we could not reach one of
#: its dependencies this time. DNS, refused connections, resets and timeouts all
#: land here, and so do connection-level Postgres failures: a restarted database
#: or an exhausted pool is no more a verdict on the retrieval than a DNS blip
#: is, and it is strictly worse to treat as one, because the path that records
#: a verdict needs that same database.
#:
#: Deliberately narrower than ``httpx.TransportError`` and
#: ``sqlalchemy.exc.DBAPIError``: those would also swallow
#: ``UnsupportedProtocol`` and ``ProgrammingError`` -- a malformed URL and a
#: broken query, both real bugs that would then retry forever instead of
#: failing loudly.
_TRANSIENT_FAULTS = (
    httpx.TimeoutException,
    httpx.NetworkError,
    ConnectionError,
    TimeoutError,
    sa_exc.OperationalError,
    sa_exc.InterfaceError,
    sa_exc.DisconnectionError,
)


def _is_transient_fault(exc: BaseException) -> bool:
    """Whether ``exc`` is a failure to *reach* a dependency rather than a
    verdict on the job.

    The distinction is the whole point: a transient fault is not evidence the
    retrieval failed, so it must not fail the job, and it is not permission to
    stop watching, so the caller must reschedule instead of returning silently.
    """
    return isinstance(exc, _TRANSIENT_FAULTS)


def lifecycle_job_id(task: str, job_id: str) -> str:
    """The arq job id used to resume ``job_id`` via ``task``.

    Deterministic on purpose. ``enqueue_job`` with an explicit ``_job_id`` is a
    no-op when a job with that id is already queued or running, so reclaiming the
    same row twice cannot stack two copies of the same work.

    This is what bounds :func:`startup`. Before it, every resume drew a fresh
    random arq id, so a worker in a crash-loop left one more copy of each
    non-terminal job behind per restart — observed live as 4 stuck jobs holding
    20 in-flight arq entries, which saturated arq's concurrency slots and starved
    every newly submitted retrieval. Two resumed copies of one job also raced each
    other through the state guard, double-submitting it to the provider
    (``IllegalTransition: submitted -> submitted``).

    Only the reclaim path uses this. The lifecycle's own hand-offs
    (``submit``→``poll``→``materialize``, and the self-chaining poll) keep fresh
    ids: each is a distinct next step enqueued by the step before it, and pinning
    the chained poll to a fixed id would make arq reject it as a duplicate of the
    poll still running — stopping the chain dead.
    """
    return f"resume:{task}:{job_id}"


# -- session factory --------------------------------------------------------


def _session_factory(ctx: dict[str, Any]):
    """Return the ctx-provided session factory, or build the process default.

    Tests inject ``ctx["session_factory"]`` (a real test engine); the running
    worker builds one once and caches it on ``ctx``.
    """
    factory = ctx.get("session_factory")
    if factory is None:
        factory = create_session_factory(create_engine())
        ctx["session_factory"] = factory
    return factory


def _provenance(ctx: dict[str, Any]) -> ProvenanceStore:
    """Return the ctx-cached :class:`ProvenanceStore`, building it once.

    Mirrors :func:`_session_factory`'s ctx-caching convention rather than reusing
    ``tools/retrieval.py``'s module-global singleton, so the worker doesn't mix two
    different lifecycle-caching strategies.
    """
    store = ctx.get("provenance")
    if store is None:
        store = ProvenanceStore(_session_factory(ctx))
        ctx["provenance"] = store
    return store


def _eviction(ctx: dict[str, Any]) -> EvictionService:
    """Return the ctx-cached :class:`EvictionService`, building it once.

    Mirrors :func:`_provenance`'s ctx-caching convention (PRD 015).
    """
    service = ctx.get("eviction")
    if service is None:
        settings = get_settings()
        service = EvictionService(
            _session_factory(ctx),
            get_storage_backend(),
            settings.earthdata_cache_max_bytes,
            default_rebuild_cost_seconds=settings.earthdata_rebuild_cost_default_seconds,
            orphan_sweep_interval_seconds=settings.earthdata_orphan_sweep_interval_seconds,
        )
        ctx["eviction"] = service
    return service


def _prefetch(ctx: dict[str, Any]) -> PrefetchService:
    """Return the ctx-cached :class:`PrefetchService`, building it once.

    Mirrors :func:`_eviction`'s ctx-caching convention (PRD 025). Enqueues
    through this worker's own Redis connection, so a speculative job rides the
    ordinary durable job queue and is never an in-memory background task.
    """
    service = ctx.get("prefetch")
    if service is None:
        service = PrefetchService(
            _session_factory(ctx),
            provenance=_provenance(ctx),
            enqueue_fn=ctx["redis"].enqueue_job,
        )
        ctx["prefetch"] = service
    return service


async def _maybe_prefetch_safe(
    ctx: dict[str, Any],
    spec: RequestSpec,
    *,
    speculative_origin: bool,
    size_bytes: int | None,
    credential_encrypted: str | None,
) -> None:
    """Ask for a prefetch without ever letting it block the caller (PRD 025):
    same log-and-swallow convention as :func:`_sweep_eviction_safe`. Speculation
    is optional; the retrieval it followed is not, and a guess must never be the
    reason a real materialization fails.
    """
    try:
        decision = await _prefetch(ctx).maybe_prefetch(
            spec,
            speculative_origin=speculative_origin,
            size_bytes=size_bytes,
            credential_encrypted=credential_encrypted,
        )
        logger.debug("prefetch decision for %s: %s", spec.obs_handle, decision.reason)
    except Exception:
        logger.exception("prefetch evaluation failed")


async def _sweep_eviction_safe(ctx: dict[str, Any], *, protected_handle: str | None) -> None:
    """Run the eviction sweep without ever letting it block the caller (PRD 015):
    mirrors :func:`_record_event_safe`'s log-and-swallow convention — cache
    maintenance must never be the reason a materialization or startup fails.
    """
    try:
        await _eviction(ctx).sweep(protected_handle=protected_handle)
    except Exception:
        logger.exception("eviction sweep failed")


async def _record_event_safe(
    ctx: dict[str, Any],
    job_id: str,
    workspace_id: str | None,
    handle: str | None,
    event_type: ProvenanceEventType,
    detail: dict[str, Any],
) -> None:
    """Record a provenance event without ever letting it block the lifecycle
    (PRD 004): a legacy/malformed spec with no ``workspace_id``/``handle`` skips
    the write (logged) rather than violating the tables' NOT NULL columns, and
    any other write failure (DB hiccup, etc.) is logged and swallowed instead of
    propagating — a routing/failure-legibility event explains a decision, it
    must never be the reason a job gets stuck.
    """
    if not workspace_id or not handle:
        logger.warning(
            "job %s: skipping %s provenance event — spec has no workspace_id/obs_handle",
            job_id,
            event_type,
        )
        return
    try:
        await _provenance(ctx).record_event(workspace_id, handle, event_type, detail=detail)
    except Exception:
        logger.exception(
            "job %s: failed to record %s provenance event", job_id, event_type
        )


async def _fail_job(
    ctx: dict[str, Any],
    job_id: str,
    spec: RequestSpec,
    stage: str,
    exc: BaseException,
    *,
    error_prefix: str | None = None,
    classification: str | None = None,
    detail_extra: dict[str, Any] | None = None,
) -> None:
    """The one choke point every lifecycle task's exception handler routes
    through on its way to ``FAILED``: persists the stage- and provider-prefixed
    error string, then records a ``JOB_FAILED`` provenance event.

    The state transition happens **first** and needs nothing but the job id
    (PRD 004): a job that is failing must reach ``FAILED`` even if the
    provenance write that explains *why* can't itself succeed (missing
    workspace/obs coordinates on a legacy spec, a DB hiccup, ...) — see
    :func:`_record_event_safe`. Built only from ``spec``, ``stage``, and the
    caught ``exc`` — never a fresh CMR/Harmony call — per the failure-legibility
    convention in CONTEXT.md. ``error_prefix`` overrides the default
    ``"<stage>/<provider> failed"`` prefix for a branch (like the
    no-OPeNDAP-fallback case) that already has its own explanatory prefix to
    preserve verbatim. ``classification`` (PRD 002) tags the submit-failure
    branch that led here (``invalid_request`` vs ``infrastructure``; since
    PRD 022 also ``TOKEN_EXPIRED``/``TOKEN_INVALID``/``EULA_NOT_ACCEPTED``) so
    the provenance trail explains *why* fallback was or wasn't attempted.
    ``detail_extra`` merges extra keys (e.g. a EULA ``resolution_url``) into
    the provenance detail.
    """
    message = _exc_message(exc)
    prefix = error_prefix if error_prefix is not None else f"{stage}/{spec.provider} failed"

    async with _session_factory(ctx)() as session:
        await crud.transition_state(
            session, job_id, JobState.FAILED, error=f"{prefix}: {message}"
        )

    detail: dict[str, Any] = {
        "stage": stage,
        "provider": spec.provider,
        "error_type": type(exc).__name__,
        "message": message,
    }
    if classification is not None:
        detail["classification"] = classification
    if detail_extra:
        detail.update(detail_extra)
    await _record_event_safe(
        ctx, job_id, spec.workspace_id, spec.obs_handle, ProvenanceEventType.JOB_FAILED, detail
    )


async def _load_spec_or_fail(
    session: Any, job_id: str, raw_spec: dict
) -> RequestSpec | None:
    """Deserialize the durable spec, failing the job cleanly on a malformed
    payload instead of letting the task crash uncaught (PRD 004).

    ``RequestSpec.from_jsonb`` raises :class:`InvalidSpecError` for a
    field-stripped nested payload (bad axis geometry, var-dims, ...); the
    caller must never let that reach the provider. Returns ``None`` after
    already transitioning the job to ``FAILED`` — the caller should bail out.
    """
    try:
        return RequestSpec.from_jsonb(raw_spec)
    except InvalidSpecError as exc:
        logger.warning("job %s has an invalid durable spec, failing: %s", job_id, exc)
        await crud.transition_state(
            session, job_id, JobState.FAILED, error=f"invalid job spec: {exc}"
        )
        return None


def _exc_message(exc: BaseException) -> str:
    """A clean, human-readable message for an exception — never a raw args tuple.

    harmony-py raises plain ``Exception(response.reason, message)`` on a failed
    submit/poll (two positional args). Left to Python's default
    ``BaseException.__str__``, a multi-arg exception renders as ``repr(args)`` —
    e.g. ``('Unprocessable Entity', 'Error: the requested combination of
    operations...')`` — a raw Python tuple leaking straight into a job's stored
    ``error`` and from there to the MCP client. Joining the args reads as one
    sentence instead. Redacted (PRD 022): a provider error echoing request
    headers or arguments must never carry a registered credential into the
    durable ``error`` column or the MCP client.
    """
    if len(exc.args) > 1:
        return redact(": ".join(str(a) for a in exc.args))
    return redact(str(exc) or type(exc).__name__)


#: HTTP reason phrases (lowercased) harmony-py's ``_handle_error_response``
#: passes as ``exc.args[0]`` for a client-error (4xx) JSON response.
_INVALID_REQUEST_REASONS = frozenset({"bad request", "unprocessable entity"})

#: Known Harmony rejection phrasings that show up in the message body even when
#: the reason string alone isn't diagnostic.
_INVALID_REQUEST_PHRASES = (
    "coverages were not found",
    "the requested combination of operations",
)


def _classify_submit_failure(exc: BaseException) -> str:
    """Classify a caught Harmony submit exception as ``"invalid_request"`` (the
    request itself is bad — a different provider can't fix it, don't fall back)
    or ``"infrastructure"`` (timeout/5xx/connection error — OPeNDAP fallback is
    the safer next step).

    harmony-py raises a plain two-arg ``Exception(response.reason, message)``
    for a JSON-bodied HTTP error response (see ``_exc_message``); ``reason`` is
    the HTTP reason phrase (e.g. ``"Bad Request"``, ``"Unprocessable Entity"``).
    A non-JSON error body instead raises ``requests.exceptions.HTTPError``, and
    a client-side validation failure in harmony-py's own ``submit()`` raises a
    single-arg ``Exception`` — neither carries a reason in ``args[0]``, so both
    fall through to the phrase check and then the ``"infrastructure"`` default.
    Defaulting to ``"infrastructure"`` when unsure is the safer wrong answer: it
    only costs a wasted OPeNDAP round-trip, never a swallowed real rejection.
    """
    if len(exc.args) > 1 and str(exc.args[0]).strip().lower() in _INVALID_REQUEST_REASONS:
        return "invalid_request"
    message = _exc_message(exc).lower()
    if any(phrase in message for phrase in _INVALID_REQUEST_PHRASES):
        return "invalid_request"
    return "infrastructure"


# -- per-job credential (PRD 022) --------------------------------------------


async def _load_job_identity(
    ctx: dict[str, Any],
    job_id: str,
    spec: RequestSpec,
    stage: str,
    credential_encrypted: str | None,
) -> EDLIdentity | None:
    """Decrypt this attempt's per-job credential into an identity handle.

    ``None`` means the env-credential fallback (no credential persisted). Any
    failure — an undecryptable blob (key rotated away), a malformed token, or
    a token that expired mid-job (EDL tokens live 60 days; a late retry is a
    real path) — fails the job *here*, before any provider is built, with its
    credential classification when it has one, and re-raises. The caller never
    proceeds: a job started as one identity must terminate rather than
    silently degrade to the service account (PRD 022 Further Notes).
    """
    if not credential_encrypted:
        return None
    try:
        token = decrypt_credential(credential_encrypted)
        identity = parse_edl_token(token)
        if identity is not None and identity.is_expired():
            raise TokenExpiredError(
                "the job's stored credential has expired mid-job — renew the "
                "token at Earthdata Login and retry the retrieval"
            )
        return identity
    except BaseException as exc:
        await _fail_job(
            ctx,
            job_id,
            spec,
            stage,
            exc,
            error_prefix="job credential unusable",
            classification=getattr(exc, "classification", None),
        )
        raise


async def _fail_classified_credential(
    ctx: dict[str, Any],
    job_id: str,
    spec: RequestSpec,
    stage: str,
    credential_exc: CredentialError,
) -> None:
    """Terminate a job on a classified credential failure (PRD 022).

    A EULA-classified failure additionally marks the collection restricted for
    future cache/dedupe scoping decisions. Never triggers the OPeNDAP
    fallback — the request isn't the problem, the identity's entitlements are,
    and a different provider would just substitute the wrong identity.
    """
    detail_extra: dict[str, Any] | None = None
    if credential_exc.classification == EULA_NOT_ACCEPTED:
        await _mark_restricted_safe(ctx, spec.concept_id)
        if credential_exc.resolution_url:
            detail_extra = {"resolution_url": credential_exc.resolution_url}
    await _fail_job(
        ctx,
        job_id,
        spec,
        stage,
        credential_exc,
        classification=credential_exc.classification,
        detail_extra=detail_extra,
    )


#: The named event an operator greps for when a replay stopped reproducing its
#: original (PRD 023). Not a provenance edge: drift is an observation about an
#: existing node, and this PRD only makes it observable — deciding what to do
#: about a changed replay belongs to the consumer.
_DRIFT_EVENT = "materialization-drift"


def _delivered_projection(record: MaterializationRecord) -> dict[str, Any]:
    """The delivered-content fields written onto the ``obs_`` handle payload
    (PRD 023), read back by ``export_result._delivered_content``.

    ``jobs.materialization`` stays the record's authoritative owner — drift
    detection and status enrichment read it there. This is the read model:
    the handle payload is what ``export_result`` resolves, and a consumer must
    not need a second lookup to learn what its handle contains.
    ``granule_count`` is omitted for a partial record so it never reads as
    zero, matching the record's own absent-not-empty posture.
    """
    projection: dict[str, Any] = {
        "content_digest": record.content_digest,
        "partial": record.partial,
    }
    if record.granule_ids is not None:
        projection["granule_count"] = len(set(record.granule_ids))
    return projection


async def _log_materialization_drift(
    ctx: dict[str, Any],
    job_id: str,
    spec: RequestSpec,
    record: MaterializationRecord,
) -> None:
    """Log :data:`_DRIFT_EVENT` when this materialization's digest differs from
    the previous one for the same ``obs_`` handle (PRD 023).

    This is the first time this server can observe upstream drift at all — an
    NRT late-arrival, an unpinned Harmony submit that took a different chain, a
    job that fell back to OPeNDAP — so it says so, with both digests and the
    handle. Best-effort in the same log-and-swallow spirit as
    :func:`_record_event_safe`: an observation must never be the reason a
    materialization fails.
    """
    if not spec.obs_handle:
        return
    try:
        async with _session_factory(ctx)() as session:
            previous = await crud.previous_materialization(
                session, spec.obs_handle, before_job_id=job_id
            )
    except Exception:
        logger.exception("job %s: failed to read the previous materialization", job_id)
        return

    prior = MaterializationRecord.from_jsonb(previous)
    if prior is None or prior.content_digest == record.content_digest:
        return
    logger.warning(
        "%s: handle %s rematerialized to different content (was %s, now %s)",
        _DRIFT_EVENT,
        spec.obs_handle,
        prior.content_digest,
        record.content_digest,
    )


async def _mark_restricted_safe(ctx: dict[str, Any], concept_id: str | None) -> None:
    """Durably flag a collection restricted, without ever blocking the caller —
    same log-and-swallow posture as :func:`_record_event_safe`."""
    if not concept_id:
        return
    try:
        async with _session_factory(ctx)() as session:
            await crud.mark_collection_restricted(session, concept_id)
    except Exception:
        logger.exception("failed to mark collection %s restricted", concept_id)


# -- lifecycle tasks --------------------------------------------------------


async def submit_job(ctx: dict[str, Any], job_id: str) -> None:
    """Submit a ``PENDING`` job to its provider and advance to ``SUBMITTED``.

    If the provider is Harmony and the submit fails, and OPeNDAP URLs are present
    in the spec, the job is transparently re-routed to OPeNDAP: the spec's
    ``provider`` field is updated to ``"opendap"`` in Postgres and ``submit_job``
    is re-enqueued so the worker retries via OPeNDAP on the next pass.

    If the provider is Harmony and the submit fails but no OPeNDAP URLs were
    discovered at plan time, the fallback cannot be attempted; an
    ``OPENDAP_NOT_APPLICABLE`` provenance event is recorded and the job's stored
    error is prefixed to say so explicitly before it transitions to ``FAILED``.
    """
    session_factory = _session_factory(ctx)
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
        if job is None or JobState(job.state) is not JobState.PENDING:
            return
        spec = await _load_spec_or_fail(session, job_id, job.request_spec)
        if spec is None:
            return
        credential_encrypted = job.credential_encrypted

    identity = await _load_job_identity(ctx, job_id, spec, "submit", credential_encrypted)
    provider = await _load_provider(spec, edl_token=identity.token if identity else None)
    plan = spec.to_plan()
    try:
        ref = await provider.submit(plan)
    except BaseException as exc:
        # BaseException, like materialize_job: arq's job_timeout cancels the
        # task, and CancelledError is not an Exception. A cancelled submit is
        # failed rather than retried — it may well have reached the provider,
        # but without the returned job URL there is nothing on our side left to
        # poll, so a terminal row a user can retry from beats a PENDING one
        # nobody will ever look at again.
        #
        # PRD 022: a credential failure is first-class and terminates the job —
        # checked before the PRD 002 invalid/infrastructure split so an auth
        # rejection can never be misread as infrastructure and trigger the
        # OPeNDAP fallback under the wrong identity.
        credential_exc = (
            classify_credential_failure(exc, identity)
            if isinstance(exc, Exception)
            else None
        )
        if credential_exc is not None:
            await _fail_classified_credential(ctx, job_id, spec, "submit", credential_exc)
            raise
        classification = _classify_submit_failure(exc) if spec.provider == "harmony" else None
        if spec.provider == "harmony" and classification == "infrastructure" and spec.opendap_urls:
            logger.warning(
                "Harmony submit failed for job %s (%s); falling back to OPeNDAP",
                job_id,
                exc,
            )
            await _record_event_safe(
                ctx,
                job_id,
                spec.workspace_id,
                spec.obs_handle,
                ProvenanceEventType.PROVIDER_FALLBACK,
                detail={
                    "from_provider": "harmony",
                    "to_provider": "opendap",
                    "reason": {"error_type": type(exc).__name__, "message": _exc_message(exc)},
                },
            )
            async with session_factory() as session:
                job = await crud.get_job(session, job_id)
                if job is None:
                    return
                new_spec = {**dict(job.request_spec), "provider": "opendap"}
                job.provider = "opendap"
                job.request_spec = new_spec
                await session.commit()
            await ctx["redis"].enqueue_job("submit_job", job_id)
            return
        if spec.provider == "harmony" and classification == "invalid_request":
            # PRD 002: Harmony rejected the request itself (bad variable, unsupported
            # operation combo) — a different provider can't fix that, so no OPeNDAP
            # fallback is attempted regardless of whether opendap_urls were discovered.
            await _fail_job(
                ctx,
                job_id,
                spec,
                "submit",
                exc,
                error_prefix="Harmony rejected the request",
                classification=classification,
            )
        elif spec.provider == "harmony":
            await _record_event_safe(
                ctx,
                job_id,
                spec.workspace_id,
                spec.obs_handle,
                ProvenanceEventType.OPENDAP_NOT_APPLICABLE,
                detail={
                    "harmony_error": {
                        "error_type": type(exc).__name__,
                        "message": _exc_message(exc),
                    },
                    "reason": "no_opendap_endpoint_discovered",
                    "output_shape": spec.output_shape,
                    "had_bbox": spec.aoi_bbox is not None,
                },
            )
            await _fail_job(
                ctx,
                job_id,
                spec,
                "submit",
                exc,
                error_prefix=(
                    "Harmony failed and no OPeNDAP fallback is available for "
                    "this collection"
                ),
                classification=classification,
            )
        else:
            await _fail_job(ctx, job_id, spec, "submit", exc)
        raise

    async with session_factory() as session:
        await crud.transition_state(
            session,
            job_id,
            JobState.SUBMITTED,
            provider_job_url=ref.provider_job_url,
        )
    await _record_event_safe(
        ctx,
        job_id,
        spec.workspace_id,
        spec.obs_handle,
        ProvenanceEventType.SUBMITTED,
        detail={"provider": spec.provider},
    )
    await ctx["redis"].enqueue_job("poll_job", job_id)


async def poll_job(ctx: dict[str, Any], job_id: str) -> None:
    """Poll a ``SUBMITTED``/``RUNNING`` job once and advance per provider status.

    Every transient fault in the whole cycle -- reaching the provider *or*
    reaching Postgres, at any of the three seams that touch one of them --
    funnels through the one reschedule below. That is deliberate: a job stops
    being watched the moment a single path out of this function forgets to
    leave a poll queued, and the two live incidents behind this design were both
    exactly that (a DNS error raised before the guarded call; a DB read left
    outside it). One choke point is the only version of this that stays correct
    as the body grows -- the same reason ``_fail_job`` is one.
    """
    try:
        await _poll_once(ctx, job_id)
    except BaseException as exc:
        # A cancelled poll (arq's job_timeout firing) is the same kind of event
        # as an unreachable dependency: no answer came back, so nothing is known
        # about the job. It reschedules for that reason -- and then re-raises,
        # because swallowing CancelledError breaks asyncio's cancellation
        # contract and tells arq a timed-out task finished cleanly.
        cancelled = isinstance(exc, asyncio.CancelledError)
        if not (cancelled or _is_transient_fault(exc)):
            raise
        # Not a verdict on the job -- just an unreachable dependency. Leaving
        # the state alone is the honest record (the job really is still running
        # out there), and re-queueing is what keeps anything looking at it.
        # Bounded in the end by the reclaim sweep's staleness horizon, which
        # fails a job that never resolves.
        logger.warning(
            "job %s: transient fault during poll, re-polling in %ss: %s",
            job_id,
            _POLL_TRANSIENT_RETRY_INTERVAL,
            redact(str(exc)),
        )
        # Best-effort when ``cancelled``: this await can itself be cancelled by
        # the same timeout, in which case no poll is queued and the log line
        # above overstates what happened. Not worth defending against -- the
        # stalled sweep picks the row up within ``job_stalled_after_seconds``,
        # which is exactly the class of hole it exists to cover.
        await ctx["redis"].enqueue_job(
            "poll_job", job_id, _defer_by=_POLL_TRANSIENT_RETRY_INTERVAL
        )
        if cancelled:
            raise


async def _poll_once(ctx: dict[str, Any], job_id: str) -> None:
    """One poll cycle. Raises transient faults for :func:`poll_job` to reschedule."""
    session_factory = _session_factory(ctx)
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
        if job is None or JobState(job.state) not in (
            JobState.SUBMITTED,
            JobState.RUNNING,
        ):
            return
        spec = await _load_spec_or_fail(session, job_id, job.request_spec)
        if spec is None:
            return
        url = job.provider_job_url
        credential_encrypted = job.credential_encrypted

    # Left outside the guard below on purpose: it fails the job itself and
    # re-raises (PRD 022), so catching it here would fail the same job twice.
    identity = await _load_job_identity(ctx, job_id, spec, "poll", credential_encrypted)

    # _load_provider re-fetches collection capabilities from CMR on every poll,
    # so the network is touched *before* provider.poll -- and until 2026-08-02
    # that call sat outside this guard. A DNS blip there escaped poll_job
    # entirely, arq burned its retries, and because nothing reached _fail_job
    # the row was never written: the job stalled in RUNNING with an empty error
    # and no poll queued, invisible until someone restarted the worker.
    try:
        provider = await _load_provider(
            spec, edl_token=identity.token if identity else None
        )
        ref = JobRef(
            provider=spec.provider,
            provider_job_id=_job_id_from_url(url),
            provider_job_url=url,
        )
        status = await provider.poll(ref)
    except Exception as exc:
        if _is_transient_fault(exc):
            raise  # poll_job reschedules; this path must not decide anything
        credential_exc = classify_credential_failure(exc, identity)
        if credential_exc is not None:
            await _fail_classified_credential(ctx, job_id, spec, "poll", credential_exc)
        else:
            await _fail_job(ctx, job_id, spec, "poll", exc)
        raise

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
        if job is None or JobState(job.state) in TERMINAL_STATES:
            return  # cancelled out from under us, or already done
        # Stamped unconditionally — the poll succeeded and this durable proof-of-life
        # must move even when the status is byte-identical to the last one and no
        # other column changes (PRD 012: this is the fix for the frozen-timestamp bug).
        job.last_polled_at = datetime.now(timezone.utc)
        next_task = _apply_poll(job, status)
        await session.commit()

    if next_task == "materialize_job":
        await ctx["redis"].enqueue_job("materialize_job", job_id)
    elif next_task == "poll_job":
        await ctx["redis"].enqueue_job("poll_job", job_id, _defer_by=_POLL_INTERVAL)


async def materialize_job(ctx: dict[str, Any], job_id: str) -> None:
    """Persist a ``MATERIALIZING`` job's result and advance to ``READY``.

    Once ``READY``, runs the cache eviction sweep (PRD 015) with this job's own
    result protected — a retrieval can never evict its own output before it's
    read — and then asks whether to warm the next contiguous window (PRD 025).
    Both are best-effort: neither ever fails the materialization.
    """
    session_factory = _session_factory(ctx)
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
        if job is None or JobState(job.state) is not JobState.MATERIALIZING:
            return
        spec = await _load_spec_or_fail(session, job_id, job.request_spec)
        if spec is None:
            return
        url = job.provider_job_url
        credential_encrypted = job.credential_encrypted
        # Read here, before the READY transition below erases the credential
        # (crud.set_state) — a prefetch has to inherit the identity this job ran
        # as, and this is the last moment it is knowable (PRD 025).
        speculative_origin = bool(job.speculative)

    identity = await _load_job_identity(
        ctx, job_id, spec, "materialize", credential_encrypted
    )
    provider = await _load_provider(spec, edl_token=identity.token if identity else None)
    ref = JobRef(
        provider=spec.provider,
        provider_job_id=_job_id_from_url(url),
        provider_job_url=url,
        job_handle=spec.job_handle,
    )
    try:
        result = await provider.materialize(ref)
    except BaseException as exc:
        # BaseException catches CancelledError (raised when Arq's job_timeout fires
        # inside asyncio.to_thread) which Exception alone does not.
        credential_exc = (
            classify_credential_failure(exc, identity)
            if isinstance(exc, Exception)
            else None
        )
        if credential_exc is not None:
            await _fail_classified_credential(
                ctx, job_id, spec, "materialize", credential_exc
            )
        else:
            await _fail_job(ctx, job_id, spec, "materialize", exc)
        raise

    # What this run actually delivered (PRD 023) — built from the provider's own
    # report, so it describes the outcome rather than replaying the intent.
    record = MaterializationRecord.from_result(result, spec)
    await _log_materialization_drift(ctx, job_id, spec, record)

    # Resolve the pending obs_ handle to the durable storage key (not a URL).
    if spec.obs_handle:
        from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store

        store = _default_store()
        await store.update_handle(
            spec.workspace_id or DEFAULT_WORKSPACE,
            spec.obs_handle,
            {
                "status": "ready",
                "storage_key": result.storage_key,
                "media_type": result.media_type,
                "size_bytes": result.size_bytes,
                # PRD 023: the delivered-content projection `export_result`
                # serves. Written in the same call as the storage key it
                # describes, so the two can never disagree about which
                # materialization a consumer is looking at.
                **_delivered_projection(record),
            },
        )

    await _record_event_safe(
        ctx,
        job_id,
        spec.workspace_id,
        spec.obs_handle,
        ProvenanceEventType.MATERIALIZED,
        detail={
            "provider": spec.provider,
            "storage_key": result.storage_key,
            "media_type": result.media_type,
            "size_bytes": result.size_bytes,
        },
    )
    async with session_factory() as session:
        await crud.transition_state(
            session,
            job_id,
            JobState.READY,
            progress=100,
            materialization=record.to_jsonb(),
        )

    await _sweep_eviction_safe(ctx, protected_handle=spec.obs_handle)

    # PRD 025: sequential exploration is predictable, so warm the next window —
    # after the sweep, so a speculative job is never admitted into a cache this
    # very materialization has just put over its cap. Off by default; refused
    # unless every gate in ``jobs/prefetch.py`` passes; never chains.
    await _maybe_prefetch_safe(
        ctx,
        spec,
        speculative_origin=speculative_origin,
        size_bytes=result.size_bytes,
        credential_encrypted=credential_encrypted,
    )


async def healthcheck(ctx: dict[str, Any]) -> str:
    """Trivial liveness task."""
    return "ok"


# -- restart-resume ---------------------------------------------------------


async def startup(ctx: dict[str, Any]) -> None:
    """Reclaim every non-terminal job and re-enqueue it from its current state —
    unless it can never make progress, in which case it is failed instead
    (PRD 004).

    This is what makes a long Harmony job survive a worker restart (§4.3): the
    work list comes straight from Postgres, and each job is dispatched to the task
    that resumes its state — ``PENDING`` re-submits, ``SUBMITTED``/``RUNNING``
    re-poll, ``MATERIALIZING`` re-materializes. Terminal jobs are left alone.

    Two classes of non-terminal job are dead ends rather than resumable, and
    blindly re-enqueueing them forever is exactly how debris jobs accumulate
    (live evidence: 106 stuck rows): a job whose spec can't be reconstructed is
    failed immediately, and a job older than the configured staleness horizon
    (generous — real Harmony/AppEEARS jobs run long) is failed as stale. Both
    checks happen once, in this same reclaim pass, so one worker restart
    converges existing debris to an honest terminal state.
    """
    _configure_harmony_download_concurrency()
    async with _session_factory(ctx)() as session:
        jobs = await crud.reclaim_non_terminal(session)
        resumable = _triage_reclaimed(jobs)
        await session.commit()
    await _resume_all(ctx, resumable)

    # PRD 015: converge a box restarted after heavy use below the cap without
    # waiting for the next retrieval.
    await _sweep_eviction_safe(ctx, protected_handle=None)


async def sweep_stalled_jobs(ctx: dict[str, Any]) -> None:
    """Re-enqueue every job that has fallen out of the lifecycle chain, on a
    timer, without waiting for a worker restart.

    :func:`startup` recovers stranded jobs only at boot, which made every stall
    a silent hang of unbounded duration — the row non-terminal, the ``error``
    column empty, nothing queued, and no way back short of restarting the
    worker. Three stalls are unreachable by any in-task error handling, because
    in each the process that would have retried no longer knows the job exists:
    a state committed to Postgres whose follow-on ``enqueue_job`` never landed
    (the arq retry re-enters the task, hits the state guard, and returns early);
    a Redis that lost its queue, which strands every in-flight job at once while
    the worker lives on; and ``max_tries`` exhaustion, which by design leaves the
    row non-terminal. This is also what actually bounds ``poll_job``'s
    transient-fault re-poll loop, whose own docstring promises a staleness
    horizon that until now only ever ran at boot.

    It selects on **staleness**, never on non-terminality alone — see
    :func:`crud.reclaim_stalled`. Running startup's blanket pass on a timer
    would enqueue a resume beside the chained poll a healthy job already has
    deferred, reviving the double-handling that :func:`lifecycle_job_id`
    exists to prevent.
    """
    settings = get_settings()
    now = datetime.now(timezone.utc)
    async with _session_factory(ctx)() as session:
        jobs = await crud.reclaim_stalled(
            session,
            stalled_before=now - timedelta(seconds=settings.job_stalled_after_seconds),
            materializing_stalled_before=now
            - timedelta(seconds=settings.job_materializing_stalled_after_seconds),
        )
        if jobs:
            logger.warning(
                "reclaim sweep found %d stalled job(s): %s",
                len(jobs),
                ", ".join(f"{job.job_id}({job.state})" for job in jobs),
            )
        resumable = _triage_reclaimed(jobs)
        await session.commit()
    await _resume_all(ctx, resumable)


def _triage_reclaimed(jobs: list[Job]) -> list[tuple[str, JobState]]:
    """Split reclaimed jobs into "resume this" and "this can never make
    progress, fail it now" (PRD 004).

    Mutates the passed rows in the caller's open session — the caller commits.
    Shared by both reclaim paths so the boot sweep and the periodic one can
    never drift into two different policies about the same row.
    """
    stale_before = datetime.now(timezone.utc) - timedelta(
        days=get_settings().job_stale_after_days
    )
    resumable: list[tuple[str, JobState]] = []
    for job in jobs:
        try:
            RequestSpec.from_jsonb(job.request_spec)
        except InvalidSpecError as exc:
            logger.warning(
                "reclaimed job %s has an invalid durable spec, failing: %s",
                job.job_id,
                exc,
            )
            crud.set_state(job, JobState.FAILED, error=f"invalid job spec: {exc}")
            continue
        if job.created_at < stale_before:
            logger.warning(
                "reclaimed job %s is stale (non-terminal since %s), failing",
                job.job_id,
                job.created_at,
            )
            crud.set_state(
                job,
                JobState.FAILED,
                error=(
                    f"reclaimed as stale: non-terminal since "
                    f"{job.created_at.isoformat()}"
                ),
            )
            continue
        resumable.append((job.job_id, JobState(job.state)))
    return resumable


async def _resume_all(ctx: dict[str, Any], resumable: list[tuple[str, JobState]]) -> None:
    """Dispatch each reclaimed job to the task that resumes its state, under the
    deterministic arq id that makes a repeated reclaim a no-op rather than a
    second copy of the same work (:func:`lifecycle_job_id`)."""
    redis = ctx["redis"]
    for job_id, state in resumable:
        task = _RESUME_TASK.get(state)
        if task is not None:
            await redis.enqueue_job(task, job_id, _job_id=lifecycle_job_id(task, job_id))
            logger.info("resuming job %s (%s) via %s", job_id, state.value, task)


# -- helpers ----------------------------------------------------------------


def _apply_poll(job: Job, status: object) -> str | None:
    """Advance ``job`` per a poll ``status``; return the next task to enqueue.

    Harmony reports success in one hop, but the durable machine routes through
    ``RUNNING`` → ``MATERIALIZING``; we walk the legal intermediate edges here so
    materialization is always a distinct, restart-safe state.
    """
    target = status.state  # type: ignore[attr-defined]
    current = JobState(job.state)

    if target is JobState.FAILED:
        crud.set_state(
            job, JobState.FAILED, error=status.error, progress=job.progress, message=status.message  # type: ignore[attr-defined]
        )
        return None
    if target is JobState.CANCELLED:
        crud.set_state(job, JobState.CANCELLED)
        return None

    if target is JobState.READY:
        if current is JobState.SUBMITTED:
            crud.set_state(job, JobState.RUNNING, message=status.message)  # type: ignore[attr-defined]
        crud.set_state(job, JobState.MATERIALIZING, progress=status.progress, message=status.message)  # type: ignore[attr-defined]
        return "materialize_job"

    if target is JobState.SUBMITTED:
        # Still queued at the provider (AppEEARS pending/queued) — the state
        # machine has no finer edge than SUBMITTED, and a poll happening is not
        # itself grounds to promote to RUNNING; only a running-class status is
        # (PRD 012). Progress/message can still move even though the state doesn't.
        job.progress = status.progress  # type: ignore[attr-defined]
        job.message = status.message  # type: ignore[attr-defined]
        return "poll_job"

    # Still running: move SUBMITTED -> RUNNING only once the provider itself
    # reports a running-class status, update progress.
    if current is JobState.SUBMITTED:
        crud.set_state(job, JobState.RUNNING, progress=status.progress, message=status.message)  # type: ignore[attr-defined]
    else:
        job.progress = status.progress  # type: ignore[attr-defined]
        job.message = status.message  # type: ignore[attr-defined]
    return "poll_job"


async def _load_provider(
    spec: RequestSpec, edl_token: str | None = None
) -> RetrievalProvider:
    """Fetch fresh capabilities and build this spec's retrieval provider.

    The worker owns no provider state — it reconstructs the right provider on
    every task, bound to the collection's **freshly-fetched** capabilities so
    ``find_service`` re-checks the matched service rather than trusting the spec
    blindly. ``providers.build`` owns the spec -> provider mapping itself
    (Harmony, AppEEARS point→Parquet, or OPeNDAP DAP4 subset); an unknown
    provider fails loud there — we never silently fall back to Harmony for a
    job another provider planned. ``edl_token`` (PRD 022) is the job's own
    decrypted per-call credential — threaded into the capabilities fetch and
    the provider so every attempt runs as the caller's identity.
    """
    caps = await CMRProvider(edl_token=edl_token).collection_capabilities(spec.concept_id)
    return providers.build(spec, caps, edl_token=edl_token)


def _job_id_from_url(url: str | None) -> str | None:
    """Recover a Harmony job id from its status URL (``…/jobs/<id>?linktype=…``).

    The durable row stores only ``provider_job_url`` (the spec'd column set has no
    ``provider_job_id``), so on a resumed poll we recover the id from the URL's
    last *path* segment. We parse the path explicitly so a trailing query string
    (harmony-py appends ``?linktype=…``) or fragment never leaks into the id —
    passing ``<uuid>?linktype=https`` to ``client.status`` makes Harmony reject it.
    """
    if not url:
        return None
    path = urlsplit(url).path
    return path.rstrip("/").rsplit("/", 1)[-1] or None


def _sweep_minutes(every: int) -> set[int]:
    """The arq cron minute set for a sweep every ``every`` minutes.

    arq's cron takes the minutes it fires *on*, not an interval, so an interval
    that doesn't divide 60 would fire unevenly across the hour boundary (e.g. 7
    → …56, 00, a 4-minute gap). Rounded down to the nearest divisor of 60 so a
    misconfigured interval sweeps slightly more often rather than unevenly —
    the safe direction for a recovery path.
    """
    every = max(1, min(60, int(every)))
    while 60 % every:
        every -= 1
    return set(range(0, 60, every))


class WorkerSettings:
    """Arq worker configuration. Run with ``arq earthdata_mcp.jobs.worker.WorkerSettings``."""

    # Bounded retries (PRD 004): an unforeseen repeating crash degrades to a
    # logged dead task instead of retrying forever; the startup staleness sweep
    # then catches the job on the next reclaim pass.
    # ``keep_result=0`` pairs with :func:`lifecycle_job_id`. Arq refuses an
    # explicit ``_job_id`` that still has a *stored result*, not just a live job,
    # so a retained result would make a finished resume attempt block the next one
    # for the whole retention window — stranding a job that legitimately needs
    # reclaiming again. Dropping the result narrows the dedupe to exactly the
    # window we want ("already queued or running"). Nothing reads these results:
    # the Postgres row is the source of truth, and each task returns None.
    functions = [
        func(submit_job, max_tries=5, keep_result=0),
        func(poll_job, max_tries=5, keep_result=0),
        func(materialize_job, max_tries=5, keep_result=0),
        healthcheck,
    ]
    # The recovery path that runs *while* the worker runs. Without it, `startup`
    # is the only thing that ever re-enqueues a stranded job, so every stall
    # waits on a human noticing and restarting the process.
    # ``run_at_startup=False``: `on_startup` already reclaims, more thoroughly.
    # ``max_tries=1`` — a sweep that fails has nothing to salvage; the next tick
    # is a few minutes away and sees the same rows.
    cron_jobs = [
        cron(
            sweep_stalled_jobs,
            minute=_sweep_minutes(get_settings().job_reclaim_sweep_minutes),
            second=0,
            run_at_startup=False,
            max_tries=1,
        )
    ]
    on_startup = startup
    redis_settings = RedisSettings.from_dsn(get_settings().redis_url)
    # Harmony downloads can be large (100s of MB); give them up to 1 hour.
    job_timeout = 3600
