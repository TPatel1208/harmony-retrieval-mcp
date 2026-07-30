"""Worker-side adjacent-granule prefetch triggering (PRD 025).

The worker is the only caller of :class:`PrefetchService` — one ask per
retrieval that reaches ``READY`` — and a prefetch that raises must never fail
the materialization that triggered it, mirroring the crash-proof convention
already established for provenance writes and the eviction sweep (prior art:
``test_worker_eviction.py``).

What the worker *hands* the service matters as much as that it calls it: the
originating job's own speculative flag (no chaining), what the retrieval
actually delivered (the size gate), and the credential read before the terminal
transition erased it (the identity rule).
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

from earthdata_mcp.config import Settings
from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs import worker as worker_mod
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.prefetch import PrefetchDecision, PrefetchService
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers.base import JobRef, MaterializedResult
from earthdata_mcp.tools import discovery as discovery_mod
from earthdata_mcp.workspace.models import HandleType

_DELIVERED_BYTES = 4096


class _RecordingPrefetch:
    """Stands in for the real service, capturing exactly what it was handed."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def maybe_prefetch(self, spec, **kwargs) -> PrefetchDecision:
        self.calls.append({"spec": spec, **kwargs})
        return PrefetchDecision("issued", job_id="j-prefetch")


class _MaterializingProvider:
    async def materialize(self, job: JobRef) -> MaterializedResult:
        return MaterializedResult(
            storage_key="harmony/result/new.nc",
            media_type="application/netcdf4",
            size_bytes=_DELIVERED_BYTES,
        )


async def _seed_materializing_job(
    session_factory,
    workspace_id: str,
    obs_handle: str,
    *,
    speculative: bool = False,
    credential_encrypted: str | None = None,
) -> str:
    job_id = uuid4().hex
    request_spec = {
        "concept_id": "C1-X",
        "provider": "harmony",
        "workspace_id": workspace_id,
        "obs_handle": obs_handle,
        "job_handle": f"job_{uuid4().hex[:16]}",
        "time_range": "2024-04-01T00:00:00Z/2024-04-08T00:00:00Z",
    }
    async with session_factory() as session:
        session.add(
            Job(
                job_id=job_id,
                job_handle=request_spec["job_handle"],
                obs_handle=obs_handle,
                provider="harmony",
                request_spec=request_spec,
                state=JobState.MATERIALIZING.value,
                progress=50,
                provider_job_url="https://example/jobs/j1",
                speculative=speculative,
                credential_encrypted=credential_encrypted,
            )
        )
        await session.commit()
    return job_id


def _ctx(session_factory, **extra) -> dict:
    return {
        "session_factory": session_factory,
        "redis": AsyncMock(),
        "eviction": _NoopEviction(),
        **extra,
    }


class _NoopEviction:
    async def sweep(self, **kwargs) -> None:
        return None


async def test_a_completed_retrieval_asks_for_a_prefetch(
    session_factory, workspace_store, workspace_id, monkeypatch
) -> None:
    obs_handle = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )
    job_id = await _seed_materializing_job(session_factory, workspace_id, obs_handle)
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_MaterializingProvider())
    )
    recorder = _RecordingPrefetch()

    await worker_mod.materialize_job(_ctx(session_factory, prefetch=recorder), job_id)

    assert len(recorder.calls) == 1
    call = recorder.calls[0]
    assert call["speculative_origin"] is False
    # The size gate is fed what this retrieval actually delivered — for a
    # same-length adjacent window that is the best estimate available, and it
    # costs no CMR round-trip.
    assert call["size_bytes"] == _DELIVERED_BYTES
    assert call["spec"].time_range == "2024-04-01T00:00:00Z/2024-04-08T00:00:00Z"


async def test_a_speculative_job_reports_itself_as_such(
    session_factory, workspace_store, workspace_id, monkeypatch
) -> None:
    """The no-chaining rule is only as good as what the worker reports: a
    speculative job reaching READY must tell the service it was one."""
    obs_handle = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )
    job_id = await _seed_materializing_job(
        session_factory, workspace_id, obs_handle, speculative=True
    )
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_MaterializingProvider())
    )
    recorder = _RecordingPrefetch()

    await worker_mod.materialize_job(_ctx(session_factory, prefetch=recorder), job_id)

    assert recorder.calls[0]["speculative_origin"] is True


async def test_the_prefetch_sees_the_credential_the_job_ran_under(
    session_factory, workspace_store, workspace_id, monkeypatch
) -> None:
    """``set_state`` erases a job's stored credential the instant it goes
    terminal, so the only way a prefetch can inherit the originating identity is
    for the worker to have read it *before* the READY transition."""
    obs_handle = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )
    job_id = await _seed_materializing_job(
        session_factory,
        workspace_id,
        obs_handle,
        credential_encrypted="gAAAAA-pretend-fernet-blob",
    )
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_MaterializingProvider())
    )
    # The blob is a stand-in, so the worker's own decrypt step is stubbed: what
    # is under test is which credential value reaches the prefetch, not Fernet.
    monkeypatch.setattr(worker_mod, "_load_job_identity", AsyncMock(return_value=None))
    recorder = _RecordingPrefetch()

    await worker_mod.materialize_job(_ctx(session_factory, prefetch=recorder), job_id)

    assert recorder.calls[0]["credential_encrypted"] == "gAAAAA-pretend-fernet-blob"
    # And the durable row really did have it erased by the transition.
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.credential_encrypted is None


async def test_a_speculative_job_reaching_ready_enqueues_no_further_prefetch(
    session_factory, workspace_store, workspace_id, monkeypatch
) -> None:
    """The composed no-chaining property, with the feature **on** and the real
    service wired: a speculative job completing must not spawn another. An
    unbounded cascade of speculative jobs is this feature's worst failure, so
    the guarantee is asserted through the whole path and not only at the gate.
    """
    obs_handle = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )
    job_id = await _seed_materializing_job(
        session_factory, workspace_id, obs_handle, speculative=True
    )
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_MaterializingProvider())
    )
    enqueue = AsyncMock()
    service = PrefetchService(
        session_factory,
        store=workspace_store,
        enqueue_fn=enqueue,
        settings=Settings(_env_file=None, earthdata_prefetch_enabled=True),
        # Would happily plan a window if it were ever reached — so a passing test
        # proves the chaining gate refused, not that planning failed.
        window_plan_fn=_always_plans,
    )

    await worker_mod.materialize_job(_ctx(session_factory, prefetch=service), job_id)

    enqueue.assert_not_awaited()
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.READY.value


async def _always_plans(spec, edl_token):
    from earthdata_mcp.jobs.prefetch import WindowPlan

    return WindowPlan(spec=spec, access_restricted=False, capabilities_version="2")


async def test_materialize_job_reaches_ready_even_when_prefetch_raises(
    session_factory, workspace_store, workspace_id, monkeypatch
) -> None:
    """Speculation is optional; the retrieval it followed is not."""
    obs_handle = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )
    job_id = await _seed_materializing_job(session_factory, workspace_id, obs_handle)
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_MaterializingProvider())
    )

    class _ThrowingPrefetch:
        async def maybe_prefetch(self, *args, **kwargs):
            raise RuntimeError("CMR unreachable")

    await worker_mod.materialize_job(
        _ctx(session_factory, prefetch=_ThrowingPrefetch()), job_id
    )

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.READY.value


async def test_with_prefetch_disabled_a_completed_retrieval_enqueues_nothing_extra(
    session_factory, workspace_store, workspace_id, monkeypatch
) -> None:
    """Story 6 end to end through the worker: with the feature off (the default),
    the whole path is inert — the only queue traffic is the retrieval's own."""
    obs_handle = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )
    job_id = await _seed_materializing_job(session_factory, workspace_id, obs_handle)
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_MaterializingProvider())
    )
    enqueue = AsyncMock()
    service = PrefetchService(
        session_factory,
        store=workspace_store,
        enqueue_fn=enqueue,
        settings=Settings(_env_file=None, earthdata_prefetch_enabled=False),
    )
    ctx = _ctx(session_factory, prefetch=service)

    await worker_mod.materialize_job(ctx, job_id)

    enqueue.assert_not_awaited()
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.READY.value
