"""EvictionService — enforces ``EARTHDATA_CACHE_MAX_BYTES`` over the
materialization cache (PRD 015).

Tests observe external behavior only: which blobs remain in the backend, what
handle payloads read back through the store, and which provenance events/job
states exist afterwards — never the sweep's internal ordering or queries. Real
Postgres (workspace/provenance/jobs fixtures) + the local filesystem backend,
following the prior art in ``test_storage_local.py`` and
``test_workspace_handles.py``.

Eviction is deliberately global (it scans handles across every workspace, per
the PRD's "the cap is global" decision), and the Postgres fixtures are real,
persistent rows with no per-test rollback — so every storage key here embeds
``workspace_id`` (itself a fresh uuid per test) to guarantee it can never alias
a debris row left behind by an earlier test run.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import update

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.eviction import EvictionService
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.workspace.models import Handle, HandleType, ProvenanceEventType
from earthdata_mcp.workspace.provenance import ProvenanceStore


def _key(workspace_id: str, name: str) -> str:
    return f"harmony/{workspace_id}/{name}"


async def _put_obs(
    workspace_store, workspace_id, storage_key, *, status="ready", media_type="application/x-netcdf"
):
    return await workspace_store.put_handle(
        workspace_id,
        HandleType.OBS,
        {"status": status, "storage_key": storage_key, "media_type": media_type},
    )


async def _age(session_factory, handle: str, when: datetime) -> None:
    """Backdate a handle's ``updated_at`` so LRU order is deterministic in tests."""
    async with session_factory() as session:
        await session.execute(
            update(Handle).where(Handle.handle == handle).values(updated_at=when)
        )
        await session.commit()


async def _seed_ready_job(session_factory, workspace_id: str, obs_handle: str) -> str:
    """Insert a READY job owning ``obs_handle`` — the row the sweep must walk
    across the ``ready -> expired`` edge when that handle is evicted."""
    job_id = uuid4().hex
    async with session_factory() as session:
        session.add(
            Job(
                job_id=job_id,
                job_handle=f"job_{uuid4().hex[:16]}",
                obs_handle=obs_handle,
                provider="harmony",
                request_spec={"concept_id": "C1-X", "workspace_id": workspace_id, "obs_handle": obs_handle},
                state=JobState.READY.value,
                progress=100,
            )
        )
        await session.commit()
    return job_id


def _service(session_factory, local_backend, max_bytes: int) -> EvictionService:
    return EvictionService(session_factory, local_backend, max_bytes)


async def test_under_cap_is_noop(workspace_store, local_backend, session_factory, workspace_id) -> None:
    key = _key(workspace_id, "x/g1.nc")
    await local_backend.put(key, b"1234567890")
    handle = await _put_obs(workspace_store, workspace_id, key)

    await _service(session_factory, local_backend, max_bytes=1_000_000).sweep()

    assert await local_backend.get(key) == b"1234567890"
    record = await workspace_store.get_handle(workspace_id, handle)
    assert record.payload["status"] == "ready"


async def test_over_cap_evicts_least_recently_used_first(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    now = datetime.now(timezone.utc)
    key_a, key_b, key_c = (_key(workspace_id, n) for n in ("a/g.nc", "b/g.nc", "c/g.nc"))

    oldest = await _put_obs(workspace_store, workspace_id, key_a)
    await local_backend.put(key_a, b"x" * 40)
    await _age(session_factory, oldest, now - timedelta(hours=2))

    middle = await _put_obs(workspace_store, workspace_id, key_b)
    await local_backend.put(key_b, b"x" * 40)
    await _age(session_factory, middle, now - timedelta(hours=1))

    newest = await _put_obs(workspace_store, workspace_id, key_c)
    await local_backend.put(key_c, b"x" * 40)
    await _age(session_factory, newest, now)

    # Total is 120 bytes; cap forces eviction of the single oldest handle (40
    # bytes) to bring the total (80) at or under the cap.
    await _service(session_factory, local_backend, max_bytes=100).sweep()

    oldest_record = await workspace_store.get_handle(workspace_id, oldest)
    assert oldest_record.payload["status"] == "expired"
    assert oldest_record.payload.get("storage_key") is None
    with pytest.raises(KeyError):
        await local_backend.get(key_a)

    for handle, key in [(middle, key_b), (newest, key_c)]:
        record = await workspace_store.get_handle(workspace_id, handle)
        assert record.payload["status"] == "ready"
        assert await local_backend.get(key) == b"x" * 40


async def test_triggering_result_is_protected_from_its_own_sweep(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """The result that just materialized and triggered this sweep must never be
    evicted in the same pass, even if it is the oldest (e.g. its ``updated_at``
    predates a handle touched moments before by an unrelated write)."""
    now = datetime.now(timezone.utc)
    key_a, key_b = (_key(workspace_id, n) for n in ("a/g.nc", "b/g.nc"))

    triggering = await _put_obs(workspace_store, workspace_id, key_a)
    await local_backend.put(key_a, b"x" * 40)
    await _age(session_factory, triggering, now - timedelta(hours=2))

    other = await _put_obs(workspace_store, workspace_id, key_b)
    await local_backend.put(key_b, b"x" * 40)
    await _age(session_factory, other, now - timedelta(hours=1))

    # Over the 50-byte cap; only "other" is evictable since "triggering" is protected.
    await _service(session_factory, local_backend, max_bytes=50).sweep(protected_handle=triggering)

    triggering_record = await workspace_store.get_handle(workspace_id, triggering)
    assert triggering_record.payload["status"] == "ready"
    assert await local_backend.get(key_a) == b"x" * 40

    other_record = await workspace_store.get_handle(workspace_id, other)
    assert other_record.payload["status"] == "expired"


async def test_orphan_blobs_reclaimed_before_any_live_handle_is_evicted(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """A blob under a materialization prefix with no referencing handle (debris
    from a crashed job) is deleted first — and if that alone brings the total
    back under the cap, no live handle is touched at all."""
    orphan_key = _key(workspace_id, "orphan/dangling.nc")
    await local_backend.put(orphan_key, b"x" * 60)

    live_key = _key(workspace_id, "live/g.nc")
    live = await _put_obs(workspace_store, workspace_id, live_key)
    await local_backend.put(live_key, b"x" * 40)

    # Total on disk is 100 bytes (orphan + live), over the 50-byte cap — but
    # reclaiming the 60-byte orphan alone is enough to get back under it.
    await _service(session_factory, local_backend, max_bytes=50).sweep()

    with pytest.raises(KeyError):
        await local_backend.get(orphan_key)

    live_record = await workspace_store.get_handle(workspace_id, live)
    assert live_record.payload["status"] == "ready"
    assert await local_backend.get(live_key) == b"x" * 40


async def test_oversized_single_result_tolerated_when_protected(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """A single result larger than the cap is still stored and is not rejected
    or force-evicted on the very sweep that materialized it — the cap bounds
    the cache over time, it does not reject a result outright."""
    key = _key(workspace_id, "big/g.nc")
    huge = await _put_obs(workspace_store, workspace_id, key)
    await local_backend.put(key, b"x" * 1000)

    await _service(session_factory, local_backend, max_bytes=10).sweep(protected_handle=huge)

    record = await workspace_store.get_handle(workspace_id, huge)
    assert record.payload["status"] == "ready"
    assert await local_backend.get(key) == b"x" * 1000


async def test_eviction_records_expired_event_and_walks_job_to_expired(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """Evicting a job-owned obs_ result: the handle flips to expired (storage
    key cleared, media type preserved), an ``expired`` provenance event carries
    the size + reason, and a READY job owning that handle walks the legal
    ``ready -> expired`` edge — provenance never loses the lineage/spec."""
    provenance = ProvenanceStore(session_factory)
    key = _key(workspace_id, "a/g.nc")
    obs_handle = await _put_obs(workspace_store, workspace_id, key)
    await local_backend.put(key, b"x" * 40)
    await provenance.record_edge(
        workspace_id, obs_handle, "dataset_src", request_spec={"concept_id": "C1-X"}
    )
    job_id = await _seed_ready_job(session_factory, workspace_id, obs_handle)

    await _service(session_factory, local_backend, max_bytes=0).sweep()

    record = await workspace_store.get_handle(workspace_id, obs_handle)
    assert record.payload["status"] == "expired"
    assert record.payload.get("storage_key") is None
    assert record.payload["media_type"] == "application/x-netcdf"

    events = await provenance.events(workspace_id, obs_handle)
    expired = [e for e in events if e.event_type == ProvenanceEventType.EXPIRED.value]
    assert len(expired) == 1
    assert expired[0].detail["size_bytes"] == 40
    assert expired[0].detail["reason"]

    ancestry = await provenance.ancestry(workspace_id, obs_handle)
    assert [a.handle for a in ancestry] == ["dataset_src"]

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.EXPIRED.value


async def test_eviction_of_cube_handle_with_no_job_row_does_not_error(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """A transform-owned cube_ handle has no jobs row at all — eviction must
    still succeed (handle + event carry the whole story)."""
    key = _key(workspace_id, "cube/y.nc")
    cube_handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.CUBE,
        {"status": "ready", "storage_key": key, "media_type": "application/x-netcdf"},
    )
    await local_backend.put(key, b"x" * 40)

    await _service(session_factory, local_backend, max_bytes=0).sweep()

    record = await workspace_store.get_handle(workspace_id, cube_handle)
    assert record.payload["status"] == "expired"
