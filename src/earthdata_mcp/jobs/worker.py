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
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import TERMINAL_STATES, JobState
from earthdata_mcp.providers.base import JobRef, RetrievalProvider
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.providers.request_spec import InvalidSpecError, RequestSpec
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
    branch that led here (``invalid_request`` vs ``infrastructure``) so the
    provenance trail explains *why* fallback was or wasn't attempted.
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
    sentence instead.
    """
    if len(exc.args) > 1:
        return ": ".join(str(a) for a in exc.args)
    return str(exc) or type(exc).__name__


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

    provider = await _load_provider(spec)
    plan = spec.to_plan()
    try:
        ref = await provider.submit(plan)
    except Exception as exc:
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
    await _provenance(ctx).record_event(
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

    provider = await _load_provider(spec)
    ref = JobRef(
        provider=spec.provider,
        provider_job_id=_job_id_from_url(url),
        provider_job_url=url,
    )
    try:
        status = await provider.poll(ref)
    except Exception as exc:
        await _fail_job(ctx, job_id, spec, "poll", exc)
        raise

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
        if job is None or JobState(job.state) in TERMINAL_STATES:
            return  # cancelled out from under us, or already done
        next_task = _apply_poll(job, status)
        await session.commit()

    if next_task == "materialize_job":
        await ctx["redis"].enqueue_job("materialize_job", job_id)
    elif next_task == "poll_job":
        await ctx["redis"].enqueue_job("poll_job", job_id, _defer_by=_POLL_INTERVAL)


async def materialize_job(ctx: dict[str, Any], job_id: str) -> None:
    """Persist a ``MATERIALIZING`` job's result and advance to ``READY``."""
    session_factory = _session_factory(ctx)
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
        if job is None or JobState(job.state) is not JobState.MATERIALIZING:
            return
        spec = await _load_spec_or_fail(session, job_id, job.request_spec)
        if spec is None:
            return
        url = job.provider_job_url

    provider = await _load_provider(spec)
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

    await _provenance(ctx).record_event(
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

    # Still running: move SUBMITTED -> RUNNING on first signal, update progress.
    if current is JobState.SUBMITTED:
        crud.set_state(job, JobState.RUNNING, progress=status.progress, message=status.message)  # type: ignore[attr-defined]
    else:
        job.progress = status.progress  # type: ignore[attr-defined]
        job.message = status.message  # type: ignore[attr-defined]
    return "poll_job"


async def _load_provider(spec: RequestSpec) -> RetrievalProvider:
    """Fetch fresh capabilities and build this spec's retrieval provider.

    The worker owns no provider state — it reconstructs the right provider on
    every task, bound to the collection's **freshly-fetched** capabilities so
    ``find_service`` re-checks the matched service rather than trusting the spec
    blindly. ``providers.build`` owns the spec -> provider mapping itself
    (Harmony, AppEEARS point→Parquet, or OPeNDAP DAP4 subset); an unknown
    provider fails loud there — we never silently fall back to Harmony for a
    job another provider planned.
    """
    caps = await CMRProvider().collection_capabilities(spec.concept_id)
    return providers.build(spec, caps)


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
