"""CRUD over the durable ``jobs`` table (PLAN.md §4.3).

The Postgres ``jobs`` table is the source of truth: every read the tools and the
worker do goes through here, and every state change is funnelled through
``set_state``/``transition_state`` so the state machine in ``state.py`` is the
sole authority on what is legal. The worker is stateless — it owns no job state,
it only mutates these rows.

All functions take an :class:`~sqlalchemy.ext.asyncio.AsyncSession` so callers
control the transaction boundary; the mutating helpers ``commit`` before
returning so a crash mid-task never leaves an uncommitted transition.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import (
    TERMINAL_STATES,
    JobState,
    assert_legal,
)


class JobNotFoundError(KeyError):
    """No job with this id/handle exists."""


async def create_job(
    session: AsyncSession,
    *,
    job_id: str,
    job_handle: str,
    obs_handle: str | None,
    provider: str,
    request_spec: dict,
) -> Job:
    """Insert a fresh job in ``PENDING`` with its durable, re-materializable spec."""
    job = Job(
        job_id=job_id,
        job_handle=job_handle,
        obs_handle=obs_handle,
        provider=provider,
        request_spec=request_spec,
        state=JobState.PENDING.value,
        progress=0,
    )
    session.add(job)
    await session.commit()
    return job


async def get_job(session: AsyncSession, job_id: str) -> Job | None:
    """Load a job by primary key, or ``None``."""
    return await session.get(Job, job_id)


async def get_job_by_handle(session: AsyncSession, job_handle: str) -> Job | None:
    """Load a job by its public ``job_`` handle, or ``None``.

    This is the read path ``get_retrieval_status`` uses: state comes from Postgres,
    never from any in-memory bookkeeping (§4.3 hard rule).
    """
    stmt = select(Job).where(Job.job_handle == job_handle)
    return (await session.execute(stmt)).scalar_one_or_none()


async def reclaim_non_terminal(session: AsyncSession) -> list[Job]:
    """Return every job not in a terminal state — the restart-resume work list.

    The worker's startup hook reclaims these and re-enqueues each from its current
    state, so no in-flight job is lost to a process restart (§4.3).
    """
    terminal = [s.value for s in TERMINAL_STATES]
    stmt = select(Job).where(Job.state.not_in(terminal)).order_by(Job.created_at)
    return list((await session.execute(stmt)).scalars().all())


def set_state(job: Job, new_state: JobState, **extra: object) -> None:
    """Move a *loaded* job to ``new_state`` (guarded), setting any extra columns.

    Synchronous and commit-free so a single task can chain legal hops in one
    transaction (e.g. ``SUBMITTED -> RUNNING -> MATERIALIZING``) before committing.
    ``extra`` sets named columns (``provider_job_url``, ``progress``, ``error``,
    ``output_expires_at``) atomically with the state change.
    """
    assert_legal(JobState(job.state), new_state)
    job.state = JobState(new_state).value
    for key, value in extra.items():
        setattr(job, key, value)


async def transition_state(
    session: AsyncSession,
    job_id: str,
    new_state: JobState,
    **extra: object,
) -> Job:
    """Load ``job_id``, apply one guarded transition, and commit.

    Raises :class:`JobNotFoundError` if the job is gone and
    :class:`~earthdata_mcp.jobs.state.IllegalTransition` if the edge is illegal.
    """
    job = await session.get(Job, job_id)
    if job is None:
        raise JobNotFoundError(job_id)
    set_state(job, new_state, **extra)
    await session.commit()
    return job


def parse_expiration(value: str | datetime | None) -> datetime | None:
    """Coerce a Harmony ``data_expiration`` (ISO string or datetime) to a datetime."""
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
