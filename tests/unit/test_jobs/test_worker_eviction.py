"""Worker eviction-sweep triggers (PRD 015).

The worker is the only caller of ``EvictionService`` — every successful
materialization and worker startup runs one sweep — and a sweep that raises
must never fail the materialization or the startup that triggered it, mirroring
the crash-proof convention already established for provenance writes (prior
art: ``test_worker_provenance.py``).
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.eviction import EvictionService
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.jobs import worker as worker_mod
from earthdata_mcp.providers.base import JobRef, MaterializedResult
from earthdata_mcp.tools import discovery as discovery_mod
from earthdata_mcp.workspace.models import HandleType


def _ctx(session_factory, **extra) -> dict:
    return {"session_factory": session_factory, "redis": AsyncMock(), **extra}


class _MaterializingProvider:
    async def materialize(self, job: JobRef) -> MaterializedResult:
        return MaterializedResult(
            storage_key="harmony/result/new.nc",
            media_type="application/netcdf4",
            size_bytes=10,
        )


async def _seed_materializing_job(session_factory, workspace_id: str, obs_handle: str) -> str:
    job_id = uuid4().hex
    request_spec = {
        "concept_id": "C1-X",
        "provider": "harmony",
        "workspace_id": workspace_id,
        "obs_handle": obs_handle,
        "job_handle": f"job_{uuid4().hex[:16]}",
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
            )
        )
        await session.commit()
    return job_id


async def test_materialize_job_over_cap_evicts_older_result(
    session_factory, workspace_store, local_backend, workspace_id, monkeypatch
) -> None:
    # A pre-existing, older, over-cap result already sitting in the cache.
    old_key = f"harmony/{workspace_id}/old.nc"
    await local_backend.put(old_key, b"x" * 50)
    old_handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.OBS,
        {"status": "ready", "storage_key": old_key, "media_type": "application/x-netcdf"},
    )

    # The new obs_ handle this job is about to materialize.
    obs_handle = await workspace_store.put_handle(workspace_id, HandleType.OBS, {"status": "pending"})
    job_id = await _seed_materializing_job(session_factory, workspace_id, obs_handle)
    await local_backend.put("harmony/result/new.nc", b"x" * 10)  # what the fake provider "wrote"

    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(worker_mod, "_load_provider", AsyncMock(return_value=_MaterializingProvider()))

    ctx = _ctx(session_factory, eviction=EvictionService(session_factory, local_backend, max_bytes=10))

    await worker_mod.materialize_job(ctx, job_id)

    old_record = await workspace_store.get_handle(workspace_id, old_handle)
    assert old_record.payload["status"] == "expired"

    new_record = await workspace_store.get_handle(workspace_id, obs_handle)
    assert new_record.payload["status"] == "ready"


async def test_materialize_job_reaches_ready_even_when_eviction_sweep_raises(
    session_factory, workspace_store, workspace_id, monkeypatch
) -> None:
    obs_handle = await workspace_store.put_handle(workspace_id, HandleType.OBS, {"status": "pending"})
    job_id = await _seed_materializing_job(session_factory, workspace_id, obs_handle)

    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(worker_mod, "_load_provider", AsyncMock(return_value=_MaterializingProvider()))

    class _ThrowingEviction:
        async def sweep(self, *args, **kwargs) -> None:
            raise RuntimeError("eviction backend unreachable")

    ctx = _ctx(session_factory, eviction=_ThrowingEviction())

    await worker_mod.materialize_job(ctx, job_id)

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.READY.value


class _FakeSessionFactory:
    """A minimal async-context-manager session stub for ``startup``'s reclaim
    query — no non-terminal jobs, so the reclaim loop is a no-op and only the
    eviction-sweep trigger under test runs."""

    def __call__(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, *args, **kwargs):
        class _Result:
            def scalars(self):
                return self

            def all(self):
                return []

        return _Result()

    async def commit(self) -> None:
        return None


async def test_startup_runs_an_eviction_sweep() -> None:
    calls: list[str | None] = []

    class _RecordingEviction:
        async def sweep(self, *, protected_handle: str | None = None) -> None:
            calls.append(protected_handle)

    ctx = {
        "session_factory": _FakeSessionFactory(),
        "redis": AsyncMock(),
        "eviction": _RecordingEviction(),
    }

    await worker_mod.startup(ctx)

    assert calls == [None]


async def test_startup_swallows_a_raising_eviction_sweep() -> None:
    class _ThrowingEviction:
        async def sweep(self, **kwargs) -> None:
            raise RuntimeError("boom")

    ctx = {
        "session_factory": _FakeSessionFactory(),
        "redis": AsyncMock(),
        "eviction": _ThrowingEviction(),
    }

    await worker_mod.startup(ctx)  # must not raise
