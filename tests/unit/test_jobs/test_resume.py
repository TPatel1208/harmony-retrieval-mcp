"""Restart-resume: the worker reclaims non-terminal jobs from Postgres on boot.

The contract (PLAN.md §4.3): a job left mid-flight by a killed worker must be
re-enqueued from its *current* state when the worker restarts — none lost, none
double-handled into a terminal state. We exercise ``startup`` directly with the
real Postgres-backed ``session_factory`` and a mocked Redis, asserting each
seeded job is dispatched to the task that resumes *its* state, by ``job_id``.

Because the jobs table is shared across the suite, every assertion is keyed to
the specific ``job_id`` this test seeded — never a bare "was it called" — so
rows from other tests can't make a pass or a failure spurious.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.jobs.worker import startup


async def _seed_job(session_factory, state: JobState) -> str:
    """Insert one job in ``state`` and return its job_id."""
    job_id = uuid4().hex
    async with session_factory() as session:
        session.add(
            Job(
                job_id=job_id,
                job_handle=f"job_{uuid4().hex[:16]}",
                obs_handle=f"obs_{uuid4().hex[:16]}",
                provider="harmony",
                request_spec={"concept_id": "C1-X"},
                state=state.value,
                progress=0,
                provider_job_url="https://harmony.earthdata.nasa.gov/jobs/abc",
            )
        )
        await session.commit()
    return job_id


def _ctx(session_factory) -> dict:
    """A worker ctx with the real session factory and a mocked Redis pool."""
    return {"session_factory": session_factory, "redis": AsyncMock()}


def _enqueued(redis: AsyncMock) -> list[tuple]:
    """The positional args of every ``enqueue_job`` await: ``(task_name, job_id)``."""
    return [call.args for call in redis.enqueue_job.await_args_list]


# -- non-terminal states resume from their current state ------------------


async def test_startup_reclaims_pending(session_factory) -> None:
    job_id = await _seed_job(session_factory, JobState.PENDING)
    ctx = _ctx(session_factory)

    await startup(ctx)

    ctx["redis"].enqueue_job.assert_any_await("submit_job", job_id)


async def test_startup_reclaims_submitted(session_factory) -> None:
    job_id = await _seed_job(session_factory, JobState.SUBMITTED)
    ctx = _ctx(session_factory)

    await startup(ctx)

    ctx["redis"].enqueue_job.assert_any_await("poll_job", job_id)


async def test_startup_reclaims_running(session_factory) -> None:
    job_id = await _seed_job(session_factory, JobState.RUNNING)
    ctx = _ctx(session_factory)

    await startup(ctx)

    ctx["redis"].enqueue_job.assert_any_await("poll_job", job_id)


async def test_startup_reclaims_materializing(session_factory) -> None:
    job_id = await _seed_job(session_factory, JobState.MATERIALIZING)
    ctx = _ctx(session_factory)

    await startup(ctx)

    ctx["redis"].enqueue_job.assert_any_await("materialize_job", job_id)


async def test_startup_resumes_pending_with_correct_task_only(session_factory) -> None:
    """A PENDING job is dispatched to submit_job and to no other task."""
    job_id = await _seed_job(session_factory, JobState.PENDING)
    ctx = _ctx(session_factory)

    await startup(ctx)

    mine = [args for args in _enqueued(ctx["redis"]) if args[1] == job_id]
    assert mine == [("submit_job", job_id)]


# -- terminal states are never resumed ------------------------------------


@pytest.mark.parametrize(
    "state",
    [JobState.READY, JobState.FAILED, JobState.EXPIRED, JobState.CANCELLED],
)
async def test_startup_skips_terminal_states(session_factory, state) -> None:
    job_id = await _seed_job(session_factory, state)
    ctx = _ctx(session_factory)

    await startup(ctx)

    # This specific job id must appear in no enqueue call, whatever else is queued.
    enqueued_ids = [args[1] for args in _enqueued(ctx["redis"])]
    assert job_id not in enqueued_ids
