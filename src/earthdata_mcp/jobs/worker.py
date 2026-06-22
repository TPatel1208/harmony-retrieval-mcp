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
from typing import Any
from urllib.parse import urlsplit

from arq.connections import RedisSettings

from earthdata_mcp.config import get_settings
from earthdata_mcp.db import create_engine, create_session_factory
from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import TERMINAL_STATES, JobState
from earthdata_mcp.providers.base import (
    AOI,
    JobRef,
    RetrievalPlan,
    TimeRange,
    TransformSpec,
)

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


# -- lifecycle tasks --------------------------------------------------------


async def submit_job(ctx: dict[str, Any], job_id: str) -> None:
    """Submit a ``PENDING`` job to its provider and advance to ``SUBMITTED``."""
    session_factory = _session_factory(ctx)
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
        if job is None or JobState(job.state) is not JobState.PENDING:
            return
        spec = dict(job.request_spec)

    provider = await _provider_for(spec)
    plan = _plan_from_spec(spec)
    ref = await provider.submit(plan)

    async with session_factory() as session:
        await crud.transition_state(
            session,
            job_id,
            JobState.SUBMITTED,
            provider_job_url=ref.provider_job_url,
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
        spec = dict(job.request_spec)
        url = job.provider_job_url

    provider = await _provider_for(spec)
    ref = JobRef(
        provider=spec.get("provider", "harmony"),
        provider_job_id=_job_id_from_url(url),
        provider_job_url=url,
    )
    status = await provider.poll(ref)

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
        spec = dict(job.request_spec)
        url = job.provider_job_url

    provider = await _provider_for(spec)
    ref = JobRef(
        provider=spec.get("provider", "harmony"),
        provider_job_id=_job_id_from_url(url),
        provider_job_url=url,
        job_handle=spec.get("job_handle"),
    )
    result = await provider.materialize(ref)

    # Resolve the pending obs_ handle to the durable storage key (not a URL).
    obs_handle = spec.get("obs_handle")
    if obs_handle:
        from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store

        store = _default_store()
        await store.update_handle(
            spec.get("workspace_id", DEFAULT_WORKSPACE),
            obs_handle,
            {
                "status": "ready",
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
    """Reclaim every non-terminal job and re-enqueue it from its current state.

    This is what makes a long Harmony job survive a worker restart (§4.3): the
    work list comes straight from Postgres, and each job is dispatched to the task
    that resumes its state — ``PENDING`` re-submits, ``SUBMITTED``/``RUNNING``
    re-poll, ``MATERIALIZING`` re-materializes. Terminal jobs are left alone.
    """
    session_factory = _session_factory(ctx)
    async with session_factory() as session:
        jobs = await crud.reclaim_non_terminal(session)
        # Read fields inside the session; the instances detach when it closes.
        resumable = [(job.job_id, JobState(job.state)) for job in jobs]

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
        crud.set_state(job, JobState.FAILED, error=status.error, progress=job.progress)  # type: ignore[attr-defined]
        return None
    if target is JobState.CANCELLED:
        crud.set_state(job, JobState.CANCELLED)
        return None

    if target is JobState.READY:
        if current is JobState.SUBMITTED:
            crud.set_state(job, JobState.RUNNING)
        crud.set_state(job, JobState.MATERIALIZING, progress=status.progress)  # type: ignore[attr-defined]
        return "materialize_job"

    # Still running: move SUBMITTED -> RUNNING on first signal, update progress.
    if current is JobState.SUBMITTED:
        crud.set_state(job, JobState.RUNNING, progress=status.progress)  # type: ignore[attr-defined]
    else:
        job.progress = status.progress  # type: ignore[attr-defined]
    return "poll_job"


async def _provider_for(spec: dict):
    """Rebuild the retrieval provider from a durable spec, keyed on ``provider``.

    The worker owns no provider state — it reconstructs the right provider on every
    task from ``request_spec["provider"]`` (the path the router chose at planning
    time), bound to the collection's **freshly-fetched** capabilities so
    ``find_service`` re-checks the matched service rather than trusting the spec
    blindly. This is the single switch that decides which provider drives
    submit→poll→materialize for a given job (Harmony, AppEEARS point→Parquet, or
    OPeNDAP DAP4 subset). An unknown provider fails loud — we never silently fall
    back to Harmony for a job another provider planned.
    """
    from earthdata_mcp.providers.appeears import AppEEARSProvider
    from earthdata_mcp.providers.cmr import CMRProvider
    from earthdata_mcp.providers.harmony import HarmonyProvider
    from earthdata_mcp.providers.opendap import OPeNDAPProvider

    provider = spec.get("provider", "harmony")
    caps = await CMRProvider().collection_capabilities(spec["concept_id"])
    if provider == "harmony":
        return HarmonyProvider(caps)
    if provider == "appeears":
        # AppEEARS builds its point-task body from the plan alone (no granule URL).
        return AppEEARSProvider(caps)
    if provider == "opendap":
        # The granule's OPeNDAP URL is discovered at planning time; carried on the
        # spec when present (OPeNDAP routing is still dormant — router.py step 3).
        return OPeNDAPProvider(caps, opendap_url=spec.get("opendap_url"))
    raise ValueError(f"no retrieval provider for spec provider {provider!r}")


def _plan_from_spec(spec: dict) -> RetrievalPlan:
    """Reconstruct the :class:`RetrievalPlan` from a durable request spec."""
    bbox = spec.get("aoi_bbox")
    variables = tuple(spec.get("variables") or ())
    fmt = spec["output_format"]
    return RetrievalPlan(
        output_format=fmt,
        needs_bbox=bool(spec.get("needs_bbox")),
        needs_variable=bool(spec.get("needs_variable")),
        needs_temporal=bool(spec.get("needs_temporal")),
        # Carried so a resumed AppEEARS job rebuilds a plan its provider can_handle.
        needs_point_sample=bool(spec.get("needs_point_sample")),
        concept_id=spec.get("concept_id"),
        short_name=spec.get("short_name"),
        aoi=AOI(bbox=tuple(bbox)) if bbox else None,
        time_range=TimeRange.from_cmr(spec["time_range"])
        if spec.get("time_range")
        else None,
        transform=TransformSpec(output_format=fmt, variables=variables)
        if variables
        else None,
    )


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

    functions = [submit_job, poll_job, materialize_job, healthcheck]
    on_startup = startup
    redis_settings = RedisSettings.from_dsn(get_settings().redis_url)
