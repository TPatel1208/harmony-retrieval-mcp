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
a debris row left behind by an earlier test run, and the shared
``session_factory`` fixture retires the suite's own leftover ``ready`` handles
around every test (see ``tests/conftest.py``) so each cap arithmetic here means
what it says.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import update

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.eviction import EvictionService
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.prefetch import PREFETCH_WASTED
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.workspace.models import Handle, HandleType, ProvenanceEventType
from earthdata_mcp.workspace.provenance import ProvenanceStore


def _key(workspace_id: str, name: str) -> str:
    return f"harmony/{workspace_id}/{name}"


async def _put_obs(
    workspace_store,
    workspace_id,
    storage_key,
    *,
    status="ready",
    media_type="application/x-netcdf",
    pinned=False,
    speculative=False,
):
    payload = {"status": status, "storage_key": storage_key, "media_type": media_type}
    if pinned:
        payload["pinned"] = True
    if speculative:
        payload["speculative"] = True
    return await workspace_store.put_handle(workspace_id, HandleType.OBS, payload)


async def _age(session_factory, handle: str, when: datetime) -> None:
    """Backdate a handle's last-use timestamps so eviction order is deterministic.

    Sets both ``last_accessed_at`` (what the score reads) and ``updated_at``
    (what it falls back to), so a test that says "this was last used two hours
    ago" means it on either axis.
    """
    async with session_factory() as session:
        await session.execute(
            update(Handle)
            .where(Handle.handle == handle)
            .values(updated_at=when, last_accessed_at=when)
        )
        await session.commit()


async def _seed_ready_job(
    session_factory,
    workspace_id: str,
    obs_handle: str,
    *,
    rebuild_cost_seconds: float | None = None,
) -> str:
    """Insert a READY job owning ``obs_handle`` — the row the sweep must walk
    across the ``ready -> expired`` edge when that handle is evicted.

    ``rebuild_cost_seconds`` is what that job cost to build; left ``None`` it
    stands in for a legacy row written before the column existed.
    """
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
                rebuild_cost_seconds=rebuild_cost_seconds,
            )
        )
        await session.commit()
    return job_id


def _service(
    session_factory,
    local_backend,
    max_bytes: int,
    *,
    default_rebuild_cost_seconds: float = 60.0,
    orphan_sweep_interval_seconds: float = 3600.0,
) -> EvictionService:
    return EvictionService(
        session_factory,
        local_backend,
        max_bytes,
        default_rebuild_cost_seconds=default_rebuild_cost_seconds,
        orphan_sweep_interval_seconds=orphan_sweep_interval_seconds,
    )


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


async def test_expensive_old_result_outlives_a_cheap_recent_one(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """The direct inversion of LRU (PRD 024): a result that took fifteen minutes
    and a provider job to build must outlive one of identical size that rebuilds
    in seconds, even though the expensive one was used longer ago. Pure LRU
    evicts the expensive one; value-density eviction evicts the cheap one."""
    now = datetime.now(timezone.utc)
    key_expensive, key_cheap = (_key(workspace_id, n) for n in ("exp/g.nc", "cheap/g.nc"))

    expensive = await _put_obs(workspace_store, workspace_id, key_expensive)
    await local_backend.put(key_expensive, b"x" * 40)
    await _age(session_factory, expensive, now - timedelta(hours=2))
    await _seed_ready_job(session_factory, workspace_id, expensive, rebuild_cost_seconds=900.0)

    cheap = await _put_obs(workspace_store, workspace_id, key_cheap)
    await local_backend.put(key_cheap, b"x" * 40)
    await _age(session_factory, cheap, now)
    await _seed_ready_job(session_factory, workspace_id, cheap, rebuild_cost_seconds=3.0)

    # 80 bytes held against a 50-byte cap: exactly one of the two must go.
    await _service(session_factory, local_backend, max_bytes=50).sweep()

    assert (await workspace_store.get_handle(workspace_id, cheap)).payload["status"] == "expired"
    expensive_record = await workspace_store.get_handle(workspace_id, expensive)
    assert expensive_record.payload["status"] == "ready"
    assert await local_backend.get(key_expensive) == b"x" * 40


async def test_a_status_write_does_not_make_a_handle_look_recently_used(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """Recency must mean "someone used this", not "this row changed" (PRD 024).

    ``update_handle`` bumps ``updated_at`` on any status write, so under
    ``updated_at``-keyed LRU a handle nobody has read in three hours looks
    fresher than one read an hour ago. Scoring on ``last_accessed_at`` — written
    only by ``touch_handle`` — inverts that.
    """
    now = datetime.now(timezone.utc)
    key_stale, key_used = (_key(workspace_id, n) for n in ("stale/g.nc", "used/g.nc"))

    stale = await _put_obs(workspace_store, workspace_id, key_stale)
    await local_backend.put(key_stale, b"x" * 40)
    await _age(session_factory, stale, now - timedelta(hours=3))
    # A status-only write: the row changes, but nobody read the result.
    await workspace_store.update_handle(workspace_id, stale, {"status": "ready"})

    used = await _put_obs(workspace_store, workspace_id, key_used)
    await local_backend.put(key_used, b"x" * 40)
    await _age(session_factory, used, now - timedelta(hours=1))

    await _service(session_factory, local_backend, max_bytes=50).sweep()

    assert (await workspace_store.get_handle(workspace_id, stale)).payload["status"] == "expired"
    assert (await workspace_store.get_handle(workspace_id, used)).payload["status"] == "ready"


async def test_touching_a_handle_makes_it_the_last_evicted(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """The other half of precise recency: a genuine read *does* count. Two
    identical, equally stale results; the one ``export_result`` touched survives."""
    now = datetime.now(timezone.utc)
    key_read, key_unread = (_key(workspace_id, n) for n in ("read/g.nc", "unread/g.nc"))

    read = await _put_obs(workspace_store, workspace_id, key_read)
    await local_backend.put(key_read, b"x" * 40)
    await _age(session_factory, read, now - timedelta(hours=3))

    unread = await _put_obs(workspace_store, workspace_id, key_unread)
    await local_backend.put(key_unread, b"x" * 40)
    await _age(session_factory, unread, now - timedelta(hours=3))

    await workspace_store.touch_handle(workspace_id, read)

    await _service(session_factory, local_backend, max_bytes=50).sweep()

    assert (await workspace_store.get_handle(workspace_id, read)).payload["status"] == "ready"
    assert (await workspace_store.get_handle(workspace_id, unread)).payload["status"] == "expired"


async def test_a_pinned_result_is_never_evicted_under_cap_pressure(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """A researcher's pin outranks the cap: two results identical in size, age
    and build cost, and only the unpinned one goes."""
    now = datetime.now(timezone.utc)
    key_pinned, key_loose = (_key(workspace_id, n) for n in ("pin/g.nc", "loose/g.nc"))

    pinned = await _put_obs(workspace_store, workspace_id, key_pinned, pinned=True)
    await local_backend.put(key_pinned, b"x" * 40)
    await _age(session_factory, pinned, now - timedelta(hours=3))

    loose = await _put_obs(workspace_store, workspace_id, key_loose)
    await local_backend.put(key_loose, b"x" * 40)
    await _age(session_factory, loose, now - timedelta(hours=1))

    await _service(session_factory, local_backend, max_bytes=50).sweep()

    pinned_record = await workspace_store.get_handle(workspace_id, pinned)
    assert pinned_record.payload["status"] == "ready"
    assert await local_backend.get(key_pinned) == b"x" * 40
    assert (await workspace_store.get_handle(workspace_id, loose)).payload["status"] == "expired"


async def test_unpinning_restores_eviction_eligibility(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """A pin is a decision the researcher can take back — once unpinned, the
    handle competes on score like anything else."""
    now = datetime.now(timezone.utc)
    key_a, key_b = (_key(workspace_id, n) for n in ("a/g.nc", "b/g.nc"))

    formerly_pinned = await _put_obs(workspace_store, workspace_id, key_a, pinned=True)
    await local_backend.put(key_a, b"x" * 40)
    await _age(session_factory, formerly_pinned, now - timedelta(hours=3))

    other = await _put_obs(workspace_store, workspace_id, key_b)
    await local_backend.put(key_b, b"x" * 40)
    await _age(session_factory, other, now - timedelta(hours=1))

    await workspace_store.update_handle(workspace_id, formerly_pinned, {"pinned": False})
    await _age(session_factory, formerly_pinned, now - timedelta(hours=3))

    await _service(session_factory, local_backend, max_bytes=50).sweep()

    assert (
        await workspace_store.get_handle(workspace_id, formerly_pinned)
    ).payload["status"] == "expired"
    assert (await workspace_store.get_handle(workspace_id, other)).payload["status"] == "ready"


async def test_pins_alone_over_the_cap_evict_nothing_and_warn(
    workspace_store, local_backend, session_factory, workspace_id, caplog
) -> None:
    """When pinned bytes alone exceed the cap there is nothing legitimate to
    reclaim. A full cache is recoverable; silently discarding data a user
    explicitly protected is not — so evict nothing and say so loudly."""
    key_a, key_b = (_key(workspace_id, n) for n in ("pin-a/g.nc", "pin-b/g.nc"))
    for key in (key_a, key_b):
        handle = await _put_obs(workspace_store, workspace_id, key, pinned=True)
        await local_backend.put(key, b"x" * 40)
        del handle

    unpinned_key = _key(workspace_id, "loose/g.nc")
    loose = await _put_obs(workspace_store, workspace_id, unpinned_key)
    await local_backend.put(unpinned_key, b"x" * 40)

    with caplog.at_level("WARNING"):
        await _service(session_factory, local_backend, max_bytes=50).sweep()

    # Nothing was evicted at all — not even the unpinned handle, because
    # evicting it could not bring the cache under the cap anyway.
    assert (await workspace_store.get_handle(workspace_id, loose)).payload["status"] == "ready"
    assert await local_backend.get(key_a) == b"x" * 40
    assert await local_backend.get(key_b) == b"x" * 40
    assert any("pinned" in record.message.lower() for record in caplog.records)


async def test_eviction_takes_from_the_workspace_furthest_over_its_fair_share(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """One user's heavy week must not evict another user's results (PRD 024).

    The heavy workspace holds three results; the light one holds a single result
    that is by every other measure the most evictable thing in the cache — the
    oldest, and just as cheap to rebuild. Global scoring would take it. Fair
    scoring takes from the workspace that is actually over its share.
    """
    now = datetime.now(timezone.utc)
    heavy_ws, light_ws = workspace_id, f"ws-{uuid4().hex}"

    heavy_handles = []
    for name in ("h1.nc", "h2.nc", "h3.nc"):
        key = _key(heavy_ws, name)
        handle = await _put_obs(workspace_store, heavy_ws, key)
        await local_backend.put(key, b"x" * 40)
        await _age(session_factory, handle, now - timedelta(hours=1))
        heavy_handles.append(handle)

    light_key = _key(light_ws, "l1.nc")
    light = await _put_obs(workspace_store, light_ws, light_key)
    await local_backend.put(light_key, b"x" * 40)
    await _age(session_factory, light, now - timedelta(hours=10))

    # 160 bytes held against a 120-byte cap. Fair share is 60 bytes each: the
    # heavy workspace is 60 over, the light one 20 under.
    await _service(session_factory, local_backend, max_bytes=120).sweep()

    assert (await workspace_store.get_handle(light_ws, light)).payload["status"] == "ready"
    assert await local_backend.get(light_key) == b"x" * 40
    heavy_states = [
        (await workspace_store.get_handle(heavy_ws, h)).payload["status"] for h in heavy_handles
    ]
    assert heavy_states.count("expired") == 1


async def test_a_single_workspace_deployment_evicts_purely_by_score(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """Fairness must be invisible to a single-tenant deployment: with one
    workspace every candidate shares the same share, so the order is exactly the
    score order and nothing else changes."""
    now = datetime.now(timezone.utc)
    key_old, key_new = (_key(workspace_id, n) for n in ("old/g.nc", "new/g.nc"))

    old = await _put_obs(workspace_store, workspace_id, key_old)
    await local_backend.put(key_old, b"x" * 40)
    await _age(session_factory, old, now - timedelta(hours=10))

    new = await _put_obs(workspace_store, workspace_id, key_new)
    await local_backend.put(key_new, b"x" * 40)
    await _age(session_factory, new, now - timedelta(hours=1))

    await _service(session_factory, local_backend, max_bytes=50).sweep()

    assert (await workspace_store.get_handle(workspace_id, old)).payload["status"] == "expired"
    assert (await workspace_store.get_handle(workspace_id, new)).payload["status"] == "ready"


class _CountingBackend:
    """A storage backend that counts the round-trips a sweep makes through it."""

    def __init__(self, inner) -> None:
        self._inner = inner
        self.stat_calls: list[str] = []
        self.list_calls: list[str] = []

    async def stat(self, key: str):
        self.stat_calls.append(key)
        return await self._inner.stat(key)

    async def list(self, prefix: str = "") -> list[str]:
        self.list_calls.append(prefix)
        return await self._inner.list(prefix)

    def __getattr__(self, name):
        return getattr(self._inner, name)


async def test_a_sweep_does_not_stat_results_that_persist_their_size(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """A sweep runs after every materialization, so its cost must not grow with
    the cache (PRD 024 story 7). The worker persists ``size_bytes`` beside the
    storage key; eviction reads that rather than issuing a stat per handle. Only
    a handle written before it did should cost a round-trip.
    """
    counting = _CountingBackend(local_backend)
    sized_keys = [_key(workspace_id, name) for name in ("s1.nc", "s2.nc", "s3.nc")]
    for key in sized_keys:
        await local_backend.put(key, b"x" * 40)
        await workspace_store.put_handle(
            workspace_id,
            HandleType.OBS,
            {
                "status": "ready",
                "storage_key": key,
                "media_type": "application/x-netcdf",
                "size_bytes": 40,
            },
        )

    legacy_key = _key(workspace_id, "legacy.nc")
    await local_backend.put(legacy_key, b"x" * 40)
    await _put_obs(workspace_store, workspace_id, legacy_key)

    await _service(session_factory, counting, max_bytes=1_000_000).sweep()

    # Asserted over this test's own keys: the sweep is global, and other
    # modules' handles (which predate ``size_bytes``) legitimately still stat.
    assert [k for k in counting.stat_calls if k in sized_keys] == []
    assert counting.stat_calls.count(legacy_key) == 1


async def test_a_legacy_result_is_not_preferentially_purged(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """Handles predating ``rebuild_cost_seconds``/``last_accessed_at`` are scored
    with the configured defaults, not as free-to-rebuild. Treating an absent cost
    as zero would purge exactly the oldest, most established data first."""
    now = datetime.now(timezone.utc)
    key_legacy, key_cheap = (_key(workspace_id, n) for n in ("legacy/g.nc", "cheap/g.nc"))

    legacy = await _put_obs(workspace_store, workspace_id, key_legacy)
    await local_backend.put(key_legacy, b"x" * 40)
    await _age(session_factory, legacy, now - timedelta(hours=1))

    cheap = await _put_obs(workspace_store, workspace_id, key_cheap)
    await local_backend.put(key_cheap, b"x" * 40)
    await _age(session_factory, cheap, now - timedelta(hours=1))
    await _seed_ready_job(session_factory, workspace_id, cheap, rebuild_cost_seconds=3.0)

    await _service(
        session_factory, local_backend, max_bytes=50, default_rebuild_cost_seconds=900.0
    ).sweep()

    assert (await workspace_store.get_handle(workspace_id, legacy)).payload["status"] == "ready"
    assert (await workspace_store.get_handle(workspace_id, cheap)).payload["status"] == "expired"


async def test_orphan_reclamation_does_not_run_on_every_sweep(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """Orphan reclamation lists every blob under four prefixes — debris cleanup,
    not admission control, and O(cache) either way. It belongs on a timer, not on
    the path of every materialization."""
    counting = _CountingBackend(local_backend)
    service = _service(session_factory, counting, max_bytes=1_000_000)

    await service.sweep()
    listed_on_first_sweep = len(counting.list_calls)
    await service.sweep()

    assert listed_on_first_sweep > 0
    assert len(counting.list_calls) == listed_on_first_sweep


async def test_orphan_reclamation_runs_again_once_the_interval_elapses(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """Debris still has to be collected — just on the timer's schedule."""
    counting = _CountingBackend(local_backend)
    service = _service(
        session_factory, counting, max_bytes=1_000_000, orphan_sweep_interval_seconds=0.0
    )

    await service.sweep()
    listed_on_first_sweep = len(counting.list_calls)
    await service.sweep()

    assert len(counting.list_calls) == 2 * listed_on_first_sweep


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


async def test_speculative_results_are_evicted_before_any_requested_one(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """PRD 025's central safety property: a prefetch may never cost a researcher
    their own results. The speculative candidate here wins on *every* other
    axis — it is smaller, newer, and more expensive to rebuild, so value-density
    scoring alone would keep it — and it still goes first.
    """
    now = datetime.now(timezone.utc)
    key_spec, key_real = (_key(workspace_id, n) for n in ("spec/g.nc", "real/g.nc"))

    speculative = await _put_obs(workspace_store, workspace_id, key_spec, speculative=True)
    await local_backend.put(key_spec, b"x" * 10)
    await _age(session_factory, speculative, now)
    await _seed_ready_job(session_factory, workspace_id, speculative, rebuild_cost_seconds=900.0)

    requested = await _put_obs(workspace_store, workspace_id, key_real)
    await local_backend.put(key_real, b"x" * 40)
    await _age(session_factory, requested, now - timedelta(hours=10))
    await _seed_ready_job(session_factory, workspace_id, requested, rebuild_cost_seconds=3.0)

    # 50 bytes held against a 40-byte cap: exactly one candidate must go.
    await _service(session_factory, local_backend, max_bytes=40).sweep()

    assert (
        await workspace_store.get_handle(workspace_id, speculative)
    ).payload["status"] == "expired"
    assert (await workspace_store.get_handle(workspace_id, requested)).payload["status"] == "ready"


async def test_a_speculative_result_evicted_unused_is_reported_as_waste(
    workspace_store, local_backend, session_factory, workspace_id, caplog
) -> None:
    """A feature that spends quota on guesses has to say what the wrong guesses
    cost, or its hit rate is unfalsifiable (PRD 025 story 5)."""
    key = _key(workspace_id, "spec/g.nc")
    handle = await _put_obs(workspace_store, workspace_id, key, speculative=True)
    await local_backend.put(key, b"x" * 40)

    with caplog.at_level("INFO"):
        await _service(session_factory, local_backend, max_bytes=0).sweep()

    wasted = [r for r in caplog.records if getattr(r, "event", None) == PREFETCH_WASTED]
    assert len(wasted) == 1
    assert wasted[0].bytes_reclaimed == 40
    assert wasted[0].handle == handle


async def test_promoting_a_speculative_result_restores_its_normal_standing(
    workspace_store, local_backend, session_factory, workspace_id
) -> None:
    """Once someone has asked for it, a prefetched result is not disposable any
    more — it competes on score like anything else (PRD 025 story 4)."""
    now = datetime.now(timezone.utc)
    key_promoted, key_other = (_key(workspace_id, n) for n in ("prom/g.nc", "other/g.nc"))

    promoted = await _put_obs(workspace_store, workspace_id, key_promoted, speculative=True)
    await local_backend.put(key_promoted, b"x" * 40)
    await _age(session_factory, promoted, now - timedelta(hours=1))

    other = await _put_obs(workspace_store, workspace_id, key_other)
    await local_backend.put(key_other, b"x" * 40)
    await _age(session_factory, other, now - timedelta(hours=10))

    await workspace_store.update_handle(workspace_id, promoted, {"speculative": False})
    await _age(session_factory, promoted, now - timedelta(hours=1))

    await _service(session_factory, local_backend, max_bytes=50).sweep()

    assert (await workspace_store.get_handle(workspace_id, promoted)).payload["status"] == "ready"
    assert (await workspace_store.get_handle(workspace_id, other)).payload["status"] == "expired"
