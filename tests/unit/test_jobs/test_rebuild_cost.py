"""A job records what it cost to build, at the moment it becomes READY (PRD 024).

The materialization cache evicts by value density, so it has to know what a
result would cost to rebuild. That number is only cheaply knowable at the
``READY`` transition — afterwards ``updated_at`` has moved on — so every path to
READY funnels through ``crud.set_state`` and is stamped there.

Real Postgres, through the public ``crud`` interface: what these assert is the
durable column a later sweep reads, never how it was computed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import update

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import JobState


async def _pending_job(session_factory, *, created_ago: timedelta | None = None) -> str:
    job_id = uuid4().hex
    async with session_factory() as session:
        await crud.create_job(
            session,
            job_id=job_id,
            job_handle=f"job_{uuid4().hex[:16]}",
            obs_handle=f"obs_{uuid4().hex[:16]}",
            provider="harmony",
            request_spec={"concept_id": "C1-X"},
        )
        if created_ago is not None:
            await session.execute(
                update(Job)
                .where(Job.job_id == job_id)
                .values(created_at=datetime.now(timezone.utc) - created_ago)
            )
            await session.commit()
    return job_id


async def _walk_to_ready(session_factory, job_id: str) -> None:
    for state in (
        JobState.SUBMITTED,
        JobState.RUNNING,
        JobState.MATERIALIZING,
        JobState.READY,
    ):
        async with session_factory() as session:
            await crud.transition_state(session, job_id, state)


async def test_reaching_ready_records_the_build_wall_clock(session_factory) -> None:
    """A job that took fifteen minutes to build says so on its own row."""
    job_id = await _pending_job(session_factory, created_ago=timedelta(minutes=15))

    await _walk_to_ready(session_factory, job_id)

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.rebuild_cost_seconds is not None
    assert 15 * 60 <= job.rebuild_cost_seconds < 16 * 60


async def test_a_job_that_never_reached_ready_records_no_cost(session_factory) -> None:
    """Cost is an outcome of a completed build — a failed job has none to report,
    and must not be recorded as free (which would make it maximally evictable)."""
    job_id = await _pending_job(session_factory, created_ago=timedelta(minutes=15))

    async with session_factory() as session:
        await crud.transition_state(session, job_id, JobState.SUBMITTED)
    async with session_factory() as session:
        await crud.transition_state(session, job_id, JobState.FAILED, error="boom")

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.rebuild_cost_seconds is None


async def test_a_replay_can_carry_the_original_cost_forward(session_factory) -> None:
    """A rematerialization that already knows what the original build cost may
    pass it explicitly; the measured value must not overwrite it."""
    job_id = await _pending_job(session_factory)

    for state in (JobState.SUBMITTED, JobState.RUNNING, JobState.MATERIALIZING):
        async with session_factory() as session:
            await crud.transition_state(session, job_id, state)
    async with session_factory() as session:
        await crud.transition_state(
            session, job_id, JobState.READY, rebuild_cost_seconds=902.5
        )

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.rebuild_cost_seconds == 902.5
