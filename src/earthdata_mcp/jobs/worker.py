"""Stateless Arq worker that drives the durable job lifecycle (PLAN.md §4.3).

The worker owns no state: the Postgres ``jobs`` table is the source of truth. It
exposes three tasks — ``submit_job`` → ``poll_job`` → ``materialize_job`` — that
walk a job through the state machine, each re-enqueueing the next, and a
``startup`` hook that **reclaims every non-terminal job on boot and re-enqueues
it from its current state**, so a process restart never strands an in-flight job.

Run with ``arq earthdata_mcp.jobs.worker.WorkerSettings``.

The harmony-py client, the EDL session, polling, and download all live in
:class:`~earthdata_mcp.providers.harmony.HarmonyProvider`; the worker only
sequences the durable transitions around it (we never hand-roll a Harmony client
— CLAUDE.md hard rule). The provider + plan are rebuilt from the durable
``request_spec`` on every task, so resumption after a restart needs nothing but
the row.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

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
from earthdata_mcp.providers.request_spec import InvalidSpecError, RequestSpec
from earthdata_mcp.redaction import redact
from earthdata_mcp.storage import get_storage_backend
from earthdata_mcp.workspace.models import ProvenanceEventType
from earthdata_mcp.workspace.provenance import ProvenanceStore

logger = logging.getLogger(__name__)

# Which task resumes a job found in each non-terminal state on startup (§4.3).
_RESUME_TASK: dict[JobState, str] = {
    JobState.PENDING: "submit_job",
    JobState.SUBMITTED: "poll_job",
    JobState.RUNNING: "poll_job",
    JobState.MATERIALIZING: "materialize_job",
}

# Delay (seconds) before the next poll while a job is still running.
_POLL_INTERVAL = 5


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
        service = EvictionService(
            _session_factory(ctx), get_storage_backend(), get_settings().earthdata_cache_max_bytes
        )
        ctx["eviction"] = service
    return service


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
    except Exception as exc:
        # PRD 022: a credential failure is first-class and terminates the job —
        # checked before the PRD 002 invalid/infrastructure split so an auth
        # rejection can never be misread as infrastructure and trigger the
        # OPeNDAP fallback under the wrong identity.
        credential_exc = classify_credential_failure(exc, identity)
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
    """Poll a ``SUBMITTED``/``RUNNING`` job once and advance per provider status."""
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

    identity = await _load_job_identity(ctx, job_id, spec, "poll", credential_encrypted)
    provider = await _load_provider(spec, edl_token=identity.token if identity else None)
    ref = JobRef(
        provider=spec.provider,
        provider_job_id=_job_id_from_url(url),
        provider_job_url=url,
    )
    try:
        status = await provider.poll(ref)
    except Exception as exc:
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
    read. The sweep is best-effort: it never fails the materialization.
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
        await crud.transition_state(session, job_id, JobState.READY, progress=100)

    await _sweep_eviction_safe(ctx, protected_handle=spec.obs_handle)


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
    session_factory = _session_factory(ctx)
    stale_before = datetime.now(timezone.utc) - timedelta(
        days=get_settings().job_stale_after_days
    )

    resumable: list[tuple[str, JobState]] = []
    async with session_factory() as session:
        jobs = await crud.reclaim_non_terminal(session)
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
        await session.commit()

    redis = ctx["redis"]
    for job_id, state in resumable:
        task = _RESUME_TASK.get(state)
        if task is not None:
            await redis.enqueue_job(task, job_id)
            logger.info("resuming job %s (%s) via %s", job_id, state.value, task)

    # PRD 015: converge a box restarted after heavy use below the cap without
    # waiting for the next retrieval.
    await _sweep_eviction_safe(ctx, protected_handle=None)


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


class WorkerSettings:
    """Arq worker configuration. Run with ``arq earthdata_mcp.jobs.worker.WorkerSettings``."""

    # Bounded retries (PRD 004): an unforeseen repeating crash degrades to a
    # logged dead task instead of retrying forever; the startup staleness sweep
    # then catches the job on the next reclaim pass.
    functions = [
        func(submit_job, max_tries=5),
        func(poll_job, max_tries=5),
        func(materialize_job, max_tries=5),
        healthcheck,
    ]
    on_startup = startup
    redis_settings = RedisSettings.from_dsn(get_settings().redis_url)
    # Harmony downloads can be large (100s of MB); give them up to 1 hour.
    job_timeout = 3600
