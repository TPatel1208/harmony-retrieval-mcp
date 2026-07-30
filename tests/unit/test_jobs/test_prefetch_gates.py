"""Adjacent-granule prefetch — the gates and the enqueue (PRD 025).

Prefetching spends provider quota, worker capacity and cache bytes on data
nobody asked for, so it is refused unless *every* gate passes. Each refusal
condition is asserted **in isolation** here — one test per gate, with every
other gate deliberately satisfied — so a future change to one gate cannot
silently disable the others. That isolation is the point of this module.

Real Postgres fixtures (the budget and fair-share gates are durable questions,
and the enqueue writes real handles and a real job row); the CMR/OPeNDAP
planning pass and the queue are injected, so nothing here touches the network.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import or_, update

from earthdata_mcp.config import Settings
from earthdata_mcp.jobs import crud, prefetch
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.prefetch import PrefetchService, WindowPlan
from earthdata_mcp.jobs.state import TERMINAL_STATES, JobState
from earthdata_mcp.providers.request_spec import RequestSpec, compute_request_signature
from earthdata_mcp.workspace.models import Handle, HandleType, ProvenanceEventType

#: Same isolation contract as ``test_eviction.py``: the fair-share reading is
#: global by design, so this suite's own workspaces are the only ``ws-`` rows and
#: leftovers from other modules are retired before each test.
_SUITE_WORKSPACE_PATTERN = "ws-%"

#: The origin window and what it predicts. Both are far enough in the past that
#: the NRT-latency gate passes for every test that isn't about that gate.
_ORIGIN_WINDOW = "2024-04-01T00:00:00Z/2024-04-08T00:00:00Z"
_PREDICTED_WINDOW = "2024-04-08T00:00:00Z/2024-04-15T00:00:00Z"

_BBOX = (-75.6, 38.9, -73.9, 41.4)
_VARIABLES = ("vertical_column_troposphere",)
_FORMAT = "application/netcdf4"


@pytest.fixture(autouse=True)
async def _isolate_cache_and_queue(session_factory):
    """Retire the test debris other modules left in the shared database.

    Two readings here are global by design — the PRD 024 fair share, and "is any
    user-requested job in flight" — so a ``ready`` handle or a non-terminal job
    row another module seeded would decide this module's gates instead of its own
    arithmetic.

    Handles are selected by ``ws-`` workspace (the shared ``workspace_id``
    fixture's pattern; a real deployment's are ``default`` or ``user-{id}``).
    Jobs are selected by that *or* by having no ``workspace_id`` in their durable
    spec at all: every spec ``RequestSpec.from_plan`` builds carries one, so a
    row without one can only have been seeded directly by a test. Nothing here
    may touch a real row. Prior art: ``test_eviction.py``'s ``_isolate_cache``.
    """
    async with session_factory() as session:
        await session.execute(
            update(Handle)
            .where(
                Handle.workspace_id.like(_SUITE_WORKSPACE_PATTERN),
                Handle.payload["status"].astext == "ready",
            )
            .values(payload=Handle.payload.concat({"status": "expired"}))
        )
        await session.execute(
            update(Job)
            .where(
                Job.state.not_in([s.value for s in TERMINAL_STATES]),
                or_(
                    Job.request_spec["workspace_id"].astext.like(
                        _SUITE_WORKSPACE_PATTERN
                    ),
                    Job.request_spec["workspace_id"].astext.is_(None),
                ),
            )
            .values(state=JobState.CANCELLED.value)
        )
        await session.commit()


def _settings(**overrides) -> Settings:
    base = dict(
        _env_file=None,
        earthdata_prefetch_enabled=True,
        earthdata_prefetch_max_bytes=1_000_000,
        earthdata_prefetch_budget_per_workspace_per_day=24,
        earthdata_prefetch_min_window_age_hours=48.0,
        # Generous by default so only the fair-share test's own arithmetic is
        # ever what refuses a prefetch here.
        earthdata_cache_max_bytes=100_000_000,
    )
    base.update(overrides)
    return Settings(**base)


def _spec(workspace_id: str, *, job_handle: str, obs_handle: str, **overrides) -> RequestSpec:
    base = dict(
        concept_id="C1-LPCLOUD",
        short_name="TEMPO_NO2_L3",
        output_format=_FORMAT,
        output_shape="grid",
        needs_bbox=True,
        needs_variable=True,
        needs_temporal=True,
        needs_point_sample=False,
        aoi_bbox=_BBOX,
        time_range=_ORIGIN_WINDOW,
        variables=_VARIABLES,
        provider="harmony",
        service_name="l3-subsetter",
        opendap_urls=(),
        coord_lat="lat",
        coord_lon="lon",
        coord_time="time",
        lat_axis=None,
        lon_axis=None,
        var_dims={},
        var_coords={},
        variable_expansions={},
        workspace_id=workspace_id,
        job_handle=job_handle,
        obs_handle=obs_handle,
        cache_key_value="originkey",
    )
    base.update(overrides)
    return RequestSpec(**base)


def _window_plan(*, access_restricted: bool | None = False, has_data: bool = True):
    """A stand-in for the fresh planning pass, so no test touches CMR.

    Returns the predicted spec back unchanged (plus a plausible cache key), which
    is exactly what a real pass does for a Harmony-routed collection with no
    OPeNDAP discovery to redo.
    """

    async def plan(spec: RequestSpec, edl_token: str | None) -> WindowPlan:
        if not has_data:
            return WindowPlan(spec=None, access_restricted=access_restricted)
        return WindowPlan(
            spec=spec, access_restricted=access_restricted, capabilities_version="2"
        )

    return plan


async def _seed_origin(workspace_store, workspace_id: str) -> tuple[str, str, str]:
    """The originating retrieval's three handles, as ``_submit_retrieval`` left them."""
    dataset_handle = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": "C1-LPCLOUD"}
    )
    obs_handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.OBS,
        {"status": "ready", "storage_key": f"harmony/{workspace_id}/origin.nc", "size_bytes": 100},
    )
    job_handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.JOB,
        {"obs_handle": obs_handle, "dataset_handle": dataset_handle},
    )
    return dataset_handle, obs_handle, job_handle


def _service(session_factory, workspace_store, provenance_store, enqueue, **kwargs):
    return PrefetchService(
        session_factory,
        store=workspace_store,
        provenance=provenance_store,
        enqueue_fn=enqueue,
        window_plan_fn=kwargs.pop("window_plan_fn", _window_plan()),
        settings=kwargs.pop("settings", _settings()),
        **kwargs,
    )


async def _maybe(service, spec, **kwargs):
    defaults = dict(speculative_origin=False, size_bytes=100)
    defaults.update(kwargs)
    return await service.maybe_prefetch(spec, **defaults)


# -- the tracer: everything passes -----------------------------------------


async def test_a_completed_retrieval_issues_one_speculative_job_for_the_next_window(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """With every gate satisfied, the next contiguous window is enqueued as a
    durable job like any other — marked speculative, on the same
    ``submit_job`` path, never an in-memory background task."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    enqueue = AsyncMock()
    spec = _spec(workspace_id, job_handle=job_handle, obs_handle="obs_origin")

    decision = await _maybe(
        _service(session_factory, workspace_store, provenance_store, enqueue), spec
    )

    assert decision.issued
    assert decision.time_range == _PREDICTED_WINDOW
    enqueue.assert_awaited_once_with("submit_job", decision.job_id)

    async with session_factory() as session:
        job = await crud.get_job(session, decision.job_id)
    assert job.state == JobState.PENDING.value
    assert job.speculative is True
    assert job.request_spec["time_range"] == _PREDICTED_WINDOW

    record = await workspace_store.get_handle(workspace_id, decision.obs_handle)
    assert record.payload["speculative"] is True
    assert record.payload["status"] == "pending"


async def test_the_prefetched_result_is_reachable_by_the_request_it_predicts(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """A prefetch nobody can find is pure waste: the new handle carries the
    canonical signature of the request it predicts, so PRD 020's reuse path
    matches it when the researcher actually asks."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    spec = _spec(workspace_id, job_handle=job_handle, obs_handle="obs_origin")

    decision = await _maybe(
        _service(session_factory, workspace_store, provenance_store, AsyncMock()), spec
    )

    expected = compute_request_signature(
        concept_id="C1-LPCLOUD",
        bbox=_BBOX,
        time_range=_PREDICTED_WINDOW,
        variables=_VARIABLES,
        output_format=_FORMAT,
    )
    record = await workspace_store.get_handle(workspace_id, decision.obs_handle)
    assert record.payload["signature"] == expected


async def test_the_speculative_result_keeps_its_lineage(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """A prefetched result is a materialization like any other and must be
    describable: it descends from the same dataset handle, keyed to its own
    durable spec, and says in its own lineage that it was speculative."""
    dataset_handle, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    spec = _spec(workspace_id, job_handle=job_handle, obs_handle="obs_origin")

    decision = await _maybe(
        _service(session_factory, workspace_store, provenance_store, AsyncMock()), spec
    )

    ancestry = await provenance_store.ancestry(workspace_id, decision.obs_handle)
    assert [a.handle for a in ancestry] == [dataset_handle]

    events = await provenance_store.events(workspace_id, decision.obs_handle)
    kinds = [e.event_type for e in events]
    assert ProvenanceEventType.PREFETCHED.value in kinds


# -- the gates, each in isolation -------------------------------------------


async def test_disabled_by_default_enqueues_nothing(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """Story 6: one setting turns the whole feature off, and off is the default."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    enqueue = AsyncMock()
    service = _service(
        session_factory,
        workspace_store,
        provenance_store,
        enqueue,
        settings=_settings(earthdata_prefetch_enabled=False),
    )

    decision = await _maybe(service, _spec(workspace_id, job_handle=job_handle, obs_handle="o"))

    assert decision.reason == prefetch.DISABLED
    enqueue.assert_not_awaited()


async def test_a_prefetch_never_chains(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """Story 7, and this feature's worst possible failure: a speculative job
    reaching READY must issue no further prefetch. One step, never a cascade."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    enqueue = AsyncMock()

    decision = await _maybe(
        _service(session_factory, workspace_store, provenance_store, enqueue),
        _spec(workspace_id, job_handle=job_handle, obs_handle="o"),
        speculative_origin=True,
    )

    assert decision.reason == prefetch.CHAINED
    enqueue.assert_not_awaited()


async def test_a_spec_with_no_prediction_enqueues_nothing(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    enqueue = AsyncMock()

    decision = await _maybe(
        _service(session_factory, workspace_store, provenance_store, enqueue),
        _spec(workspace_id, job_handle=job_handle, obs_handle="o", needs_point_sample=True),
    )

    assert decision.reason == prefetch.NO_PREDICTION
    enqueue.assert_not_awaited()


async def test_a_pending_user_job_drops_the_prefetch(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """Story 2: a researcher's job must never wait behind a guess. With real
    work in flight the prefetch is *dropped*, not queued behind it."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    await _seed_job(session_factory, workspace_id, state=JobState.RUNNING, speculative=False)
    enqueue = AsyncMock()

    decision = await _maybe(
        _service(session_factory, workspace_store, provenance_store, enqueue),
        _spec(workspace_id, job_handle=job_handle, obs_handle="o"),
    )

    assert decision.reason == prefetch.REAL_WORK_QUEUED
    enqueue.assert_not_awaited()


async def test_another_pending_prefetch_does_not_drop_this_one(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """The queue gate is about *user* work. A speculative job already in flight
    is not a researcher waiting, so it must not be read as one."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    await _seed_job(session_factory, workspace_id, state=JobState.RUNNING, speculative=True)

    decision = await _maybe(
        _service(session_factory, workspace_store, provenance_store, AsyncMock()),
        _spec(workspace_id, job_handle=job_handle, obs_handle="o"),
    )

    assert decision.issued


async def test_a_workspace_past_its_prefetch_budget_is_refused(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """Prefetch never chains, but a long sequential exploration would still spend
    unbounded quota one step at a time — the daily budget is that backstop."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    for _ in range(2):
        await _seed_job(
            session_factory, workspace_id, state=JobState.READY, speculative=True
        )
    enqueue = AsyncMock()
    service = _service(
        session_factory,
        workspace_store,
        provenance_store,
        enqueue,
        settings=_settings(earthdata_prefetch_budget_per_workspace_per_day=2),
    )

    decision = await _maybe(service, _spec(workspace_id, job_handle=job_handle, obs_handle="o"))

    assert decision.reason == prefetch.OVER_BUDGET
    enqueue.assert_not_awaited()


async def test_another_workspaces_prefetches_do_not_spend_this_ones_budget(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """The budget is per workspace: someone else's speculative week must not
    stop this researcher's next window from being warmed."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    other = f"ws-{uuid4().hex}"
    for _ in range(5):
        await _seed_job(session_factory, other, state=JobState.READY, speculative=True)
    service = _service(
        session_factory,
        workspace_store,
        provenance_store,
        AsyncMock(),
        settings=_settings(earthdata_prefetch_budget_per_workspace_per_day=2),
    )

    decision = await _maybe(service, _spec(workspace_id, job_handle=job_handle, obs_handle="o"))

    assert decision.issued


async def test_a_workspace_over_its_fair_share_is_refused(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """Never prefetch into a cache already under pressure: a workspace holding
    more than its PRD 024 fair share does not get to hold speculative bytes too."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    await _seed_ready_bytes(workspace_store, workspace_id, size=900)
    enqueue = AsyncMock()
    service = _service(
        session_factory,
        workspace_store,
        provenance_store,
        enqueue,
        settings=_settings(earthdata_cache_max_bytes=1000),
    )

    decision = await _maybe(
        service, _spec(workspace_id, job_handle=job_handle, obs_handle="o"), size_bytes=200
    )

    assert decision.reason == prefetch.OVER_FAIR_SHARE
    enqueue.assert_not_awaited()


async def test_a_predicted_window_too_recent_to_be_complete_is_refused(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """A window upstream has not finished filling would be re-fetched the moment
    the rest lands, so prefetching it spends the quota twice for one window."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    now = datetime.now(timezone.utc)
    # An origin window ending an hour ago predicts one running an hour into the
    # future — squarely inside any collection's latency.
    recent = (
        f"{(now - timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%SZ')}/"
        f"{(now - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')}"
    )
    enqueue = AsyncMock()

    decision = await _maybe(
        _service(session_factory, workspace_store, provenance_store, enqueue),
        _spec(workspace_id, job_handle=job_handle, obs_handle="o", time_range=recent),
    )

    assert decision.reason == prefetch.NRT_WINDOW
    enqueue.assert_not_awaited()


async def test_a_prefetch_larger_than_the_prefetch_cap_is_refused(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """A guess is not allowed to cost what a request a researcher made can."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    enqueue = AsyncMock()
    service = _service(
        session_factory,
        workspace_store,
        provenance_store,
        enqueue,
        settings=_settings(earthdata_prefetch_max_bytes=50),
    )

    decision = await _maybe(
        service, _spec(workspace_id, job_handle=job_handle, obs_handle="o"), size_bytes=500
    )

    assert decision.reason == prefetch.TOO_LARGE
    enqueue.assert_not_awaited()


async def test_a_window_the_workspace_already_holds_is_refused(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """The dedupe machinery decides sameness, not a second notion of it: if the
    predicted request's own signature is already in this workspace, there is
    nothing to warm."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    signature = compute_request_signature(
        concept_id="C1-LPCLOUD",
        bbox=_BBOX,
        time_range=_PREDICTED_WINDOW,
        variables=_VARIABLES,
        output_format=_FORMAT,
    )
    await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "ready", "signature": signature}
    )
    enqueue = AsyncMock()

    decision = await _maybe(
        _service(session_factory, workspace_store, provenance_store, enqueue),
        _spec(workspace_id, job_handle=job_handle, obs_handle="o"),
    )

    assert decision.reason == prefetch.ALREADY_HELD
    enqueue.assert_not_awaited()


async def test_a_window_with_no_data_is_not_enqueued(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """The prediction is a guess about *access*, not about existence — a window
    the archive has nothing in is a miss, not a job to submit."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    enqueue = AsyncMock()
    service = _service(
        session_factory,
        workspace_store,
        provenance_store,
        enqueue,
        window_plan_fn=_window_plan(has_data=False),
    )

    decision = await _maybe(service, _spec(workspace_id, job_handle=job_handle, obs_handle="o"))

    assert decision.reason == prefetch.NO_DATA_FOR_WINDOW
    enqueue.assert_not_awaited()


# -- identity ---------------------------------------------------------------


async def test_a_restricted_collection_with_no_identity_is_skipped(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """Story 8: speculation must never fetch anything the caller could not have
    fetched themselves. With no identity to run as, a restricted collection is
    skipped rather than attempted under the service account."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    enqueue = AsyncMock()
    service = _service(
        session_factory,
        workspace_store,
        provenance_store,
        enqueue,
        window_plan_fn=_window_plan(access_restricted=True),
    )

    decision = await _maybe(
        service,
        _spec(workspace_id, job_handle=job_handle, obs_handle="o"),
        credential_encrypted=None,
    )

    assert decision.reason == prefetch.RESTRICTED_NO_IDENTITY
    enqueue.assert_not_awaited()


async def test_a_prefetch_inherits_the_originating_jobs_credential(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """A prefetch runs as the identity that asked for the window before it —
    never as a broader one, and never as none when one was supplied."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)

    decision = await _maybe(
        _service(session_factory, workspace_store, provenance_store, AsyncMock()),
        _spec(workspace_id, job_handle=job_handle, obs_handle="o"),
        credential_encrypted="gAAAAA-pretend-fernet-blob",
    )

    assert decision.issued
    async with session_factory() as session:
        job = await crud.get_job(session, decision.job_id)
    assert job.credential_encrypted == "gAAAAA-pretend-fernet-blob"


# -- helpers ---------------------------------------------------------------


async def _seed_job(
    session_factory,
    workspace_id: str,
    *,
    state: JobState,
    speculative: bool,
    created_at: datetime | None = None,
) -> str:
    job_id = uuid4().hex
    async with session_factory() as session:
        session.add(
            Job(
                job_id=job_id,
                job_handle=f"job_{uuid4().hex[:16]}",
                obs_handle=f"obs_{uuid4().hex[:16]}",
                provider="harmony",
                request_spec={"concept_id": "C1-LPCLOUD", "workspace_id": workspace_id},
                state=state.value,
                progress=0,
                speculative=speculative,
            )
        )
        await session.commit()
        if created_at is not None:
            await session.execute(
                update(Job).where(Job.job_id == job_id).values(created_at=created_at)
            )
            await session.commit()
    return job_id


async def _seed_ready_bytes(workspace_store, workspace_id: str, *, size: int) -> str:
    return await workspace_store.put_handle(
        workspace_id,
        HandleType.OBS,
        {
            "status": "ready",
            "storage_key": f"harmony/{workspace_id}/{uuid4().hex}.nc",
            "size_bytes": size,
        },
    )


async def test_a_prefetch_older_than_the_budget_window_no_longer_counts(
    session_factory, workspace_store, provenance_store, workspace_id
) -> None:
    """The budget is per rolling day, not per lifetime — yesterday's speculation
    must not permanently disable today's."""
    _, _, job_handle = await _seed_origin(workspace_store, workspace_id)
    old = datetime.now(timezone.utc) - timedelta(days=3)
    for _ in range(5):
        await _seed_job(
            session_factory,
            workspace_id,
            state=JobState.READY,
            speculative=True,
            created_at=old,
        )
    service = _service(
        session_factory,
        workspace_store,
        provenance_store,
        AsyncMock(),
        settings=_settings(earthdata_prefetch_budget_per_workspace_per_day=2),
    )

    decision = await _maybe(service, _spec(workspace_id, job_handle=job_handle, obs_handle="o"))

    assert decision.issued
