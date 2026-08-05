"""The reclaim sweep has to run *while* the worker runs, not only at boot.

Until now the only thing that re-enqueued a stranded job was ``startup``. That
made every stall a silent hang of unbounded duration: the row sat non-terminal
with an empty ``error``, nothing queued, and no amount of waiting fixed it —
someone had to restart the worker. Three classes of stall are unreachable by
any amount of in-task error handling, because in each the process that would
have retried no longer knows the job exists:

* the gap between committing a state to Postgres and enqueueing the next task
  in Redis (the enqueue fails; the arq retry re-enters the task, sees a state
  its guard rejects, and returns early — permanently);
* Redis losing its queue (a container recreate empties the deferred-poll
  sorted set, stranding every in-flight job at once while the worker lives on);
* ``max_tries`` exhaustion, which by design leaves the row non-terminal.

The sweep is also what finally bounds the transient-fault re-poll loop, whose
docstring already claims to be "bounded in the end by the reclaim pass's
staleness horizon" — a horizon that, before this, only ever ran at boot.

The trap these tests exist to pin: the boot-time reclaim takes *every*
non-terminal job, but the healthy lifecycle chain enqueues itself under fresh
random arq ids. Running that same blanket pass on a timer would queue a second
poller alongside the chained one — reviving the exact double-handling that
produced ``IllegalTransition: submitted -> submitted``. A periodic sweep must
therefore select on *staleness*, never on non-terminality alone.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from unittest.mock import AsyncMock

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.jobs.worker import lifecycle_job_id, sweep_stalled_jobs


async def _seed(
    session_factory,
    state: JobState,
    *,
    last_polled_at: datetime | None = None,
) -> str:
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
                last_polled_at=last_polled_at,
            )
        )
        await session.commit()
    return job_id


def _ago(**kwargs) -> datetime:
    return datetime.now(timezone.utc) - timedelta(**kwargs)


def _ctx(session_factory) -> dict:
    return {"session_factory": session_factory, "redis": AsyncMock()}


def _enqueued(redis: AsyncMock) -> list[tuple]:
    return [call.args for call in redis.enqueue_job.await_args_list]


async def test_reclaim_stalled_selects_a_job_whose_last_poll_is_old(
    session_factory,
) -> None:
    """A RUNNING job last polled an hour ago has fallen out of the chain: at a
    five-second poll interval, that timestamp is proof nothing is watching it."""
    stalled = await _seed(session_factory, JobState.RUNNING, last_polled_at=_ago(hours=1))

    async with session_factory() as session:
        jobs = await crud.reclaim_stalled(session, stalled_before=_ago(minutes=5))

    assert stalled in [job.job_id for job in jobs]


async def test_reclaim_stalled_ignores_a_job_polled_moments_ago(
    session_factory,
) -> None:
    """The whole point of the staleness predicate. A job the chain polled a
    second ago already has a poll deferred; re-enqueueing it would put a second
    poller on the same row."""
    healthy = await _seed(
        session_factory, JobState.RUNNING, last_polled_at=datetime.now(timezone.utc)
    )

    async with session_factory() as session:
        jobs = await crud.reclaim_stalled(session, stalled_before=_ago(minutes=5))

    assert healthy not in [job.job_id for job in jobs]


async def test_a_materializing_job_gets_its_own_longer_horizon(
    session_factory,
) -> None:
    """A download is not a poll. ``job_timeout`` is an hour and a large Harmony
    result legitimately writes nothing to the row for most of it, so judging
    ``MATERIALIZING`` by the poll horizon would re-enqueue a job that is busy
    streaming bytes — and materialize_job is not idempotent-cheap."""
    downloading = await _seed(
        session_factory, JobState.MATERIALIZING, last_polled_at=_ago(minutes=20)
    )

    async with session_factory() as session:
        jobs = await crud.reclaim_stalled(
            session,
            stalled_before=_ago(minutes=5),
            materializing_stalled_before=_ago(hours=2),
        )

    assert downloading not in [job.job_id for job in jobs]


async def test_a_materializing_job_past_its_own_horizon_is_still_stalled(
    session_factory,
) -> None:
    """The counterweight: the longer horizon is patience, not exemption. Past
    twice ``job_timeout`` nothing is downloading — the task is gone."""
    abandoned = await _seed(
        session_factory, JobState.MATERIALIZING, last_polled_at=_ago(hours=5)
    )

    async with session_factory() as session:
        jobs = await crud.reclaim_stalled(
            session,
            stalled_before=_ago(minutes=5),
            materializing_stalled_before=_ago(hours=2),
        )

    assert abandoned in [job.job_id for job in jobs]


# -- the periodic task itself ---------------------------------------------


async def test_the_sweep_reenqueues_a_stalled_job_without_a_restart(
    session_factory,
) -> None:
    """The headline behavior: a stranded job gets picked back up by a worker
    that never restarted."""
    job_id = await _seed(session_factory, JobState.RUNNING, last_polled_at=_ago(hours=1))
    ctx = _ctx(session_factory)

    await sweep_stalled_jobs(ctx)

    assert ("poll_job", job_id) in _enqueued(ctx["redis"])


async def test_the_sweep_leaves_a_healthy_in_flight_job_alone(
    session_factory,
) -> None:
    """The regression this whole design is shaped around. The boot-time reclaim
    takes every non-terminal job; running *that* on a timer would enqueue a
    resume alongside the chained poll already deferred for this row — two
    pollers racing one job through the state guard, which is how
    ``IllegalTransition: submitted -> submitted`` happened live."""
    job_id = await _seed(
        session_factory, JobState.RUNNING, last_polled_at=datetime.now(timezone.utc)
    )
    ctx = _ctx(session_factory)

    await sweep_stalled_jobs(ctx)

    assert job_id not in [args[1] for args in _enqueued(ctx["redis"])]


async def test_the_sweep_resumes_under_the_deterministic_arq_id(
    session_factory,
) -> None:
    """Two sweeps over a still-stalled job must not stack two copies of the
    work — the same idempotence the boot reclaim relies on, for the same
    reason (a job that stays stalled is swept again every interval, so this
    path repeats far more often than a restart ever did)."""
    job_id = await _seed(session_factory, JobState.RUNNING, last_polled_at=_ago(hours=1))
    ctx = _ctx(session_factory)

    await sweep_stalled_jobs(ctx)
    await sweep_stalled_jobs(ctx)

    mine = [
        call
        for call in ctx["redis"].enqueue_job.await_args_list
        if call.args[1:] == (job_id,)
    ]
    assert len(mine) == 2
    assert {call.kwargs.get("_job_id") for call in mine} == {
        lifecycle_job_id("poll_job", job_id)
    }


async def test_the_sweep_fails_a_stalled_job_it_can_never_resume(
    session_factory,
) -> None:
    """PRD 004's dead-end policy has to hold on this path too, and more so: the
    boot reclaim met an unreadable spec once per restart, the sweep meets it
    every interval forever. One shared triage, so the two can't drift."""
    job_id = uuid4().hex
    async with session_factory() as session:
        session.add(
            Job(
                job_id=job_id,
                job_handle=f"job_{uuid4().hex[:16]}",
                obs_handle=f"obs_{uuid4().hex[:16]}",
                provider="harmony",
                request_spec={"concept_id": "C1-X", "lat_axis": {"name": "/lat"}},
                state=JobState.RUNNING.value,
                progress=0,
                last_polled_at=_ago(hours=1),
            )
        )
        await session.commit()
    ctx = _ctx(session_factory)

    await sweep_stalled_jobs(ctx)

    assert job_id not in [args[1] for args in _enqueued(ctx["redis"])]
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert "invalid job spec" in job.error


def test_the_worker_actually_runs_the_sweep_on_a_timer() -> None:
    """Built is not wired. Everything above is dead code unless the worker is
    configured to call it without being restarted — which was the entire bug."""
    from earthdata_mcp.jobs.worker import WorkerSettings

    assert any(
        cron_job.coroutine is sweep_stalled_jobs
        for cron_job in getattr(WorkerSettings, "cron_jobs", [])
    ), "WorkerSettings declares no cron entry for the reclaim sweep"
