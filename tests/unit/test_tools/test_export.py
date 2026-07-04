"""``export_result`` + ``rematerialize`` — the consumer data-egress contract (PRD 017).

Real Postgres-backed workspace/provenance/jobs fixtures (isolation and durability
are the point, per the prior art in ``test_tools/test_retrieval.py``); the local
filesystem backend for storage so ``export_result``'s ``storage_uri`` is a real
``file://`` URL over real bytes. The enqueue is a mock so no Redis is touched.
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import update

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.tools.export import export_result, rematerialize
from earthdata_mcp.workspace.models import Handle, HandleType, mint_handle


@pytest.fixture
def mock_enqueue() -> AsyncMock:
    return AsyncMock()


async def _seed_obs(
    workspace_store, local_backend, workspace_id, *, data=b"hello world", media_type="application/x-netcdf"
) -> tuple[str, str]:
    key = f"harmony/{workspace_id}/g.nc"
    await local_backend.put(key, data)
    handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.OBS,
        {"status": "ready", "storage_key": key, "media_type": media_type, "size_bytes": len(data)},
    )
    return handle, key


async def _seed_job_for_obs(session_factory, workspace_id, obs_handle, request_spec=None) -> Job:
    job_id = uuid4().hex
    spec = request_spec or {
        "concept_id": "C1-X",
        "workspace_id": workspace_id,
        "obs_handle": obs_handle,
        "provider": "harmony",
    }
    async with session_factory() as session:
        job = Job(
            job_id=job_id,
            job_handle=f"job_{uuid4().hex[:16]}",
            obs_handle=obs_handle,
            provider="harmony",
            request_spec=spec,
            state=JobState.READY.value,
            progress=100,
        )
        session.add(job)
        await session.commit()
    return job


# -- export_result -----------------------------------------------------------


async def test_export_result_returns_storage_uri_for_ready_handle(
    workspace_store, local_backend, workspace_id,
) -> None:
    handle, _key = await _seed_obs(workspace_store, local_backend, workspace_id)

    out = await export_result(
        handle, workspace_id=workspace_id, store=workspace_store, storage=local_backend,
    )

    assert out["handle"] == handle
    assert out["status"] == "ready"
    assert out["storage_uri"].startswith("file://")
    assert out["media_type"] == "application/x-netcdf"
    assert out["size_bytes"] == len(b"hello world")
    assert "rematerialize" in out["rematerialize_hint"]


async def test_export_result_pending_handle_returns_structured_not_ready(
    workspace_store, workspace_id,
) -> None:
    handle = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )

    out = await export_result(handle, workspace_id=workspace_id, store=workspace_store)

    assert out["handle"] == handle
    assert out["status"] == "pending"
    assert out["message"]


async def test_export_result_expired_handle_points_to_rematerialize(
    workspace_store, workspace_id,
) -> None:
    handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.OBS,
        {"status": "expired", "storage_key": None, "media_type": "application/x-netcdf"},
    )

    out = await export_result(handle, workspace_id=workspace_id, store=workspace_store)

    assert out["status"] == "expired"
    assert "rematerialize" in out["message"]


async def test_export_result_unknown_and_cross_workspace_are_byte_identical(
    workspace_store, local_backend, workspace_id,
) -> None:
    """PRD 017 story 6: a handle from another workspace collapses to the same
    not_found answer as a nonexistent handle — existence never leaks."""
    handle, _key = await _seed_obs(workspace_store, local_backend, workspace_id)
    never_minted = mint_handle(HandleType.OBS)

    cross = await export_result(
        handle, workspace_id="ws-intruder", store=workspace_store, storage=local_backend
    )
    unknown = await export_result(
        never_minted, workspace_id=workspace_id, store=workspace_store, storage=local_backend
    )

    assert cross["status"] == "not_found"
    assert unknown["status"] == "not_found"
    assert cross["message"].replace(handle, "<h>") == unknown["message"].replace(never_minted, "<h>")


async def test_export_result_rejects_job_handle(workspace_store, workspace_id) -> None:
    job_handle = mint_handle(HandleType.JOB)
    with pytest.raises(ValueError, match="obs_ or cube_"):
        await export_result(job_handle, workspace_id=workspace_id, store=workspace_store)


async def test_export_result_accepts_cube_handle(
    workspace_store, local_backend, workspace_id,
) -> None:
    key = f"transform/{workspace_id}/out.nc"
    await local_backend.put(key, b"cubedata")
    handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.CUBE,
        {"status": "ready", "storage_key": key, "media_type": "application/x-netcdf"},
    )

    out = await export_result(
        handle, workspace_id=workspace_id, store=workspace_store, storage=local_backend,
    )

    assert out["status"] == "ready"
    assert out["size_bytes"] == len(b"cubedata")  # stat() fallback: cube_ payload has no size_bytes


async def test_export_result_lru_touches_the_handle(
    workspace_store, local_backend, session_factory, workspace_id,
) -> None:
    """PRD 017 story 7: exporting a result bumps its ``updated_at`` so it is
    never the first thing the LRU eviction cap reclaims."""
    handle, _key = await _seed_obs(workspace_store, local_backend, workspace_id)
    async with session_factory() as session:
        await session.execute(
            update(Handle).where(Handle.handle == handle).values(
                updated_at=Handle.updated_at - timedelta(hours=1)
            )
        )
        await session.commit()
    before = (await workspace_store.get_handle(workspace_id, handle)).updated_at

    await export_result(handle, workspace_id=workspace_id, store=workspace_store, storage=local_backend)

    after = (await workspace_store.get_handle(workspace_id, handle)).updated_at
    assert after > before


# -- rematerialize -------------------------------------------------------------


async def test_rematerialize_cache_hit_returns_immediately_without_enqueue(
    workspace_store, local_backend, session_factory, workspace_id, mock_enqueue,
) -> None:
    handle, _key = await _seed_obs(workspace_store, local_backend, workspace_id)
    job = await _seed_job_for_obs(session_factory, workspace_id, handle)

    out = await rematerialize(
        handle, workspace_id=workspace_id,
        store=workspace_store, session_factory=session_factory, enqueue_fn=mock_enqueue,
    )

    assert out == {"job_handle": job.job_handle, "obs_handle": handle, "status": "ready"}
    mock_enqueue.assert_not_awaited()


async def test_rematerialize_evicted_key_re_enqueues_job_with_original_spec(
    workspace_store, provenance_store, session_factory, workspace_id, mock_enqueue,
) -> None:
    handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.OBS,
        {"status": "expired", "storage_key": None, "media_type": "application/x-netcdf"},
    )
    spec = {
        "concept_id": "C1-X",
        "workspace_id": workspace_id,
        "obs_handle": handle,
        "provider": "harmony",
        "time_range": "2024-01-01/2024-01-31",
    }
    old_job = await _seed_job_for_obs(session_factory, workspace_id, handle, request_spec=spec)
    # The old job is terminal EXPIRED (mirrors what the eviction sweep leaves behind).
    async with session_factory() as session:
        j = await crud.get_job(session, old_job.job_id)
        j.state = JobState.EXPIRED.value
        await session.commit()

    out = await rematerialize(
        handle, workspace_id=workspace_id,
        store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=mock_enqueue,
    )

    assert out["obs_handle"] == handle
    assert out["status"] == JobState.PENDING.value
    assert out["job_handle"].startswith("job_")
    assert out["job_handle"] != old_job.job_handle

    mock_enqueue.assert_awaited_once()
    assert mock_enqueue.await_args.args[0] == "submit_job"

    async with session_factory() as session:
        new_job = await crud.get_job_by_handle(session, out["job_handle"])
    assert new_job.request_spec == spec  # never invented, never overridden

    obs_record = await workspace_store.get_handle(workspace_id, handle)
    assert obs_record.payload["status"] == "pending"

    events = await provenance_store.events(workspace_id, handle)
    assert any(e.event_type == "re-materialized" for e in events)


async def test_rematerialize_handle_with_no_job_returns_not_found(
    workspace_store, provenance_store, session_factory, workspace_id, mock_enqueue,
) -> None:
    """A transform-owned cube_ handle (or an orphaned obs_) has no job behind
    it — it cannot be replayed, so this is a structured not_found, not a crash."""
    handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.CUBE,
        {"status": "expired", "storage_key": None, "media_type": "application/x-netcdf"},
    )

    out = await rematerialize(
        handle, workspace_id=workspace_id,
        store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=mock_enqueue,
    )

    assert out["status"] == "not_found"
    mock_enqueue.assert_not_awaited()


async def test_rematerialize_unknown_and_cross_workspace_are_byte_identical(
    workspace_store, session_factory, workspace_id, mock_enqueue,
) -> None:
    never_minted = mint_handle(HandleType.OBS)
    handle = await workspace_store.put_handle(
        "ws-owner", HandleType.OBS, {"status": "expired"}
    )

    cross = await rematerialize(
        handle, workspace_id="ws-intruder",
        store=workspace_store, session_factory=session_factory, enqueue_fn=mock_enqueue,
    )
    unknown = await rematerialize(
        never_minted, workspace_id="ws-owner",
        store=workspace_store, session_factory=session_factory, enqueue_fn=mock_enqueue,
    )

    assert cross["status"] == "not_found"
    assert unknown["status"] == "not_found"
    assert cross["message"].replace(handle, "<h>") == unknown["message"].replace(never_minted, "<h>")


async def test_rematerialize_rejects_job_handle(workspace_store, workspace_id) -> None:
    job_handle = mint_handle(HandleType.JOB)
    with pytest.raises(ValueError, match="obs_ or cube_"):
        await rematerialize(job_handle, workspace_id=workspace_id, store=workspace_store)
