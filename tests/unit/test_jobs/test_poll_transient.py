"""A transient network fault during a poll must not decide the job's fate.

Live incident 2026-08-02. A running Harmony job was polled, the poll's
capabilities fetch hit ``ConnectError: [Errno -5] No address associated with
hostname``, and the exception escaped ``poll_job`` entirely -- it was raised
*before* the ``try`` that guards ``provider.poll``. Arq retried to
``max_tries`` and gave up. Because nothing ever reached ``_fail_job``, the row
was never written at all: it sat in ``RUNNING`` at 83% with an empty ``error``
column and no poll scheduled, and stayed there until the worker was restarted
45 minutes later -- at which point the reclaim resumed it and it completed in
seconds. The retrieval had been finished at the provider the whole time.

The rule these tests pin: a transient fault carries no information about the
job. It is not evidence the job failed, so it must not fail it; and it is not
permission to stop watching, so it must reschedule. Only a definite answer from
the provider -- or a definitely-permanent error -- may move a job to terminal.
"""

from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from earthdata_mcp.jobs import crud, worker as worker_mod
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.jobs.worker import poll_job

from unittest.mock import AsyncMock


async def _seed_running_job(session_factory) -> str:
    job_id = uuid4().hex
    async with session_factory() as session:
        session.add(
            Job(
                job_id=job_id,
                job_handle=f"job_{uuid4().hex[:16]}",
                obs_handle=f"obs_{uuid4().hex[:16]}",
                provider="harmony",
                request_spec={"concept_id": "C1-X"},
                state=JobState.RUNNING.value,
                progress=83,
                provider_job_url="https://harmony.earthdata.nasa.gov/jobs/abc",
            )
        )
        await session.commit()
    return job_id


def _ctx(session_factory) -> dict:
    return {"session_factory": session_factory, "redis": AsyncMock()}


def _enqueued(redis: AsyncMock) -> list[tuple]:
    return [call.args for call in redis.enqueue_job.await_args_list]


async def _state_of(session_factory, job_id: str) -> str:
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
        return job.state


@pytest.fixture
def _no_identity(monkeypatch):
    """Job identity is irrelevant to these tests; keep it out of the way."""
    async def _none(*args, **kwargs):
        return None

    monkeypatch.setattr(worker_mod, "_load_job_identity", _none)


async def test_a_transient_provider_load_failure_reschedules_the_poll(
    session_factory, monkeypatch, _no_identity
) -> None:
    """The exact shape of the live incident: the capabilities fetch that builds
    the provider fails with a connection error before the poll is even made."""
    job_id = await _seed_running_job(session_factory)

    async def _boom(*args, **kwargs):
        raise httpx.ConnectError("[Errno -5] No address associated with hostname")

    monkeypatch.setattr(worker_mod, "_load_provider", _boom)
    ctx = _ctx(session_factory)

    await poll_job(ctx, job_id)

    assert ("poll_job", job_id) in _enqueued(ctx["redis"]), (
        "a transient fault must leave a poll scheduled -- otherwise nothing "
        "ever looks at this job again"
    )
    assert await _state_of(session_factory, job_id) == JobState.RUNNING.value


async def test_a_transient_provider_load_failure_does_not_fail_the_job(
    session_factory, monkeypatch, _no_identity
) -> None:
    """A DNS blip is not evidence the retrieval failed."""
    job_id = await _seed_running_job(session_factory)

    async def _boom(*args, **kwargs):
        raise httpx.ConnectError("[Errno -5] No address associated with hostname")

    monkeypatch.setattr(worker_mod, "_load_provider", _boom)

    await poll_job(_ctx(session_factory), job_id)

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.RUNNING.value
    assert not job.error, f"transient fault wrote an error: {job.error!r}"


async def test_a_transient_poll_failure_reschedules_rather_than_failing(
    session_factory, monkeypatch, _no_identity
) -> None:
    """The same rule for the guarded call itself. Previously a single transient
    error here drove the job straight to FAILED, and the arq retry that followed
    was a no-op because the state guard at the top of poll_job now rejected it --
    so one blip permanently killed a healthy job."""
    job_id = await _seed_running_job(session_factory)

    class _FlakyProvider:
        async def poll(self, ref):
            raise httpx.ConnectError("[Errno -5] No address associated with hostname")

    async def _provider(*args, **kwargs):
        return _FlakyProvider()

    monkeypatch.setattr(worker_mod, "_load_provider", _provider)
    ctx = _ctx(session_factory)

    await poll_job(ctx, job_id)

    assert ("poll_job", job_id) in _enqueued(ctx["redis"])
    assert await _state_of(session_factory, job_id) == JobState.RUNNING.value


async def test_a_permanent_error_still_fails_the_job(
    session_factory, monkeypatch, _no_identity
) -> None:
    """The counterweight: making transient faults survivable must not make
    genuine failures survivable too, or a broken job polls forever."""
    job_id = await _seed_running_job(session_factory)

    class _BrokenProvider:
        async def poll(self, ref):
            raise ValueError("provider rejected the job id")

    async def _provider(*args, **kwargs):
        return _BrokenProvider()

    monkeypatch.setattr(worker_mod, "_load_provider", _provider)

    with pytest.raises(ValueError):
        await poll_job(_ctx(session_factory), job_id)

    assert await _state_of(session_factory, job_id) == JobState.FAILED.value
