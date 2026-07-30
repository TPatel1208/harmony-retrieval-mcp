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

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.jobs.worker import lifecycle_job_id, startup


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

    assert ("submit_job", job_id) in _enqueued(ctx["redis"])


async def test_startup_reclaims_submitted(session_factory) -> None:
    job_id = await _seed_job(session_factory, JobState.SUBMITTED)
    ctx = _ctx(session_factory)

    await startup(ctx)

    assert ("poll_job", job_id) in _enqueued(ctx["redis"])


async def test_startup_reclaims_running(session_factory) -> None:
    job_id = await _seed_job(session_factory, JobState.RUNNING)
    ctx = _ctx(session_factory)

    await startup(ctx)

    assert ("poll_job", job_id) in _enqueued(ctx["redis"])


async def test_startup_reclaims_materializing(session_factory) -> None:
    job_id = await _seed_job(session_factory, JobState.MATERIALIZING)
    ctx = _ctx(session_factory)

    await startup(ctx)

    assert ("materialize_job", job_id) in _enqueued(ctx["redis"])


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


# -- PRD 004: startup reclaim gets a staleness + invalid-spec policy --------
#
# Blind re-enqueue-forever is what let 106 debris jobs sit non-terminal
# indefinitely; a non-terminal job that can never make progress (an unreadable
# spec, or one stale well past any real Harmony/AppEEARS run time) must be
# failed on reclaim instead of re-enqueued again.


async def _seed_job_with_spec_and_age(
    session_factory, state: JobState, request_spec: dict, created_at: datetime
) -> str:
    job_id = uuid4().hex
    async with session_factory() as session:
        session.add(
            Job(
                job_id=job_id,
                job_handle=f"job_{uuid4().hex[:16]}",
                obs_handle=f"obs_{uuid4().hex[:16]}",
                provider="harmony",
                request_spec=request_spec,
                state=state.value,
                progress=0,
                provider_job_url="https://harmony.earthdata.nasa.gov/jobs/abc",
                created_at=created_at,
            )
        )
        await session.commit()
    return job_id


async def test_startup_fails_stale_job_instead_of_reenqueuing(session_factory) -> None:
    """A non-terminal job far older than the staleness horizon (default 7 days)
    is failed as stale on reclaim, not re-enqueued forever."""
    stale_created_at = datetime.now(timezone.utc) - timedelta(days=10)
    job_id = await _seed_job_with_spec_and_age(
        session_factory, JobState.SUBMITTED, {"concept_id": "C1-X"}, stale_created_at
    )
    ctx = _ctx(session_factory)

    await startup(ctx)

    enqueued_ids = [args[1] for args in _enqueued(ctx["redis"])]
    assert job_id not in enqueued_ids

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert "stale" in job.error.lower()


async def test_startup_fails_invalid_spec_job_instead_of_reenqueuing(session_factory) -> None:
    """A non-terminal job whose spec can't be reconstructed is failed
    immediately on reclaim, not re-enqueued to crash again."""
    recent = datetime.now(timezone.utc)
    job_id = await _seed_job_with_spec_and_age(
        session_factory,
        JobState.RUNNING,
        {"concept_id": "C1-X", "lat_axis": {"name": "/product/latitude"}},
        recent,
    )
    ctx = _ctx(session_factory)

    await startup(ctx)

    enqueued_ids = [args[1] for args in _enqueued(ctx["redis"])]
    assert job_id not in enqueued_ids

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert "invalid job spec" in job.error


async def test_startup_still_reenqueues_valid_recent_job(session_factory) -> None:
    """A valid, recently-created non-terminal job is unaffected by the new
    policy — the existing resume behavior is preserved."""
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    job_id = await _seed_job_with_spec_and_age(
        session_factory, JobState.SUBMITTED, {"concept_id": "C1-X"}, recent
    )
    ctx = _ctx(session_factory)

    await startup(ctx)

    assert ("poll_job", job_id) in _enqueued(ctx["redis"])
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.SUBMITTED.value


# -- idempotent re-enqueue: a restart never multiplies in-flight copies ----


async def test_startup_resume_uses_a_stable_arq_job_id(session_factory) -> None:
    """Two reclaim passes enqueue the *same* arq job id for the same job.

    Regression (live wedge): every ``enqueue_job`` used a fresh random arq id, so
    a crash-looping worker left one more copy of each non-terminal job queued per
    restart — 4 stuck jobs became 20 in-flight arq entries, which filled arq's
    default 10 concurrency slots and starved every new submit/poll. Passing a
    deterministic ``_job_id`` makes the re-enqueue idempotent: arq drops the
    duplicate instead of stacking it.
    """
    job_id = await _seed_job(session_factory, JobState.MATERIALIZING)
    ctx = _ctx(session_factory)

    await startup(ctx)
    await startup(ctx)

    mine = [
        call
        for call in ctx["redis"].enqueue_job.await_args_list
        if call.args[1:] == (job_id,)
    ]
    assert len(mine) == 2, "both passes should attempt the resume"
    assert {call.kwargs.get("_job_id") for call in mine} == {
        lifecycle_job_id("materialize_job", job_id)
    }
