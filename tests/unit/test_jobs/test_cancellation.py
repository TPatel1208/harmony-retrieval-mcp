"""Arq's ``job_timeout`` cancels the task — and cancellation is not an ``Exception``.

``materialize_job`` catches ``BaseException`` on purpose, with a comment saying
why: a timeout firing inside ``asyncio.to_thread`` raises ``CancelledError``,
which ``except Exception`` does not see. ``submit_job`` and ``poll_job`` never
got the same treatment, so a cancellation there unwound straight past their
handlers — no ``_fail_job``, no reschedule, nothing written to the row. The job
was left exactly as stranded as the two live incidents that motivated all of
this, just through a third door.

The two tasks want opposite verdicts from the same event, and that is the point
of testing them separately:

* a cancelled **poll** learned nothing about the job — the retrieval is still
  running at the provider — so it must reschedule, exactly like any other
  no-answer fault;
* a cancelled **submit** may have reached the provider without us keeping the
  job URL, so there is no watchable job on our side; it fails, which is the
  honest terminal state and the one a user can retry from.

Both must still re-raise. Swallowing ``CancelledError`` breaks asyncio's
cancellation contract and leaves arq believing a timed-out task succeeded.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from earthdata_mcp.jobs import crud, worker as worker_mod
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.jobs.worker import poll_job, submit_job


async def _seed(session_factory, state: JobState) -> str:
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
    return {"session_factory": session_factory, "redis": AsyncMock()}


def _enqueued(redis: AsyncMock) -> list[tuple]:
    return [call.args for call in redis.enqueue_job.await_args_list]


@pytest.fixture
def _no_identity(monkeypatch):
    async def _none(*args, **kwargs):
        return None

    monkeypatch.setattr(worker_mod, "_load_job_identity", _none)


@pytest.fixture
def _cancelling_provider(monkeypatch):
    """A provider whose every call is cancelled, as arq's timeout would."""

    class _Cancelled:
        async def submit(self, plan):
            raise asyncio.CancelledError()

        async def poll(self, ref):
            raise asyncio.CancelledError()

    async def _build(*args, **kwargs):
        return _Cancelled()

    monkeypatch.setattr(worker_mod, "_load_provider", _build)


async def test_a_cancelled_poll_leaves_the_job_watched(
    session_factory, _no_identity, _cancelling_provider
) -> None:
    """A poll that ran out of time got no answer — and no answer is not a
    verdict. It must leave a poll queued, or nothing looks at this job again."""
    job_id = await _seed(session_factory, JobState.RUNNING)
    ctx = _ctx(session_factory)

    with pytest.raises(asyncio.CancelledError):
        await poll_job(ctx, job_id)

    assert ("poll_job", job_id) in _enqueued(ctx["redis"])
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.RUNNING.value
    assert not job.error, f"a cancelled poll wrote a verdict: {job.error!r}"


async def test_a_cancelled_submit_fails_the_job_rather_than_stranding_it(
    session_factory, _no_identity, _cancelling_provider
) -> None:
    """Nothing came back with a job URL, so there is nothing to poll. Better a
    FAILED row a user can see and retry than a PENDING one nobody will."""
    job_id = await _seed(session_factory, JobState.PENDING)
    ctx = _ctx(session_factory)

    with pytest.raises(asyncio.CancelledError):
        await submit_job(ctx, job_id)

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert "CancelledError" in job.error, (
        f"the row must say what killed it, not just that it died: {job.error!r}"
    )
