"""Durable retrieval tools — planning, persistence, and the read-from-Postgres path.

CMR is faked at the object level (``collection_capabilities`` as an ``AsyncMock``);
the enqueue is a mock so no Redis is touched. Handles AND jobs live in the real
Postgres-backed fixtures, so workspace isolation and durable state are exercised
for real — these tools' contract is "plan, persist a durable job, hand back
handles," and durability is the point.

Two assertions are load-bearing (per the design review):
* ``test_get_status_reads_from_postgres`` mutates the jobs row *directly* after
  creation and re-reads through the tool — proving status comes from the DB, not
  from anything the submit call cached.
* ``test_retrieve_data_stores_request_spec_not_url`` asserts no spec value is a
  staged-output URL (CLAUDE.md hard rule: provenance stores the re-materializable
  spec, never an ephemeral URL).
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from earthdata_mcp.jobs.crud import get_job_by_handle
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers._capabilities import (
    CollectionCapabilities,
    ServiceCapability,
)
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.providers.router import NotRetrievable
from earthdata_mcp.tools.retrieval import (
    cancel_retrieval,
    get_retrieval_status,
    retrieve_data,
    retrieve_subset,
    retrieve_timeseries,
)
from earthdata_mcp.workspace.models import HandleType
from earthdata_mcp.workspace.store import CrossWorkspaceError

_CONCEPT_ID = "C1234567890-LPCLOUD"
_BBOX = [-105.0, 37.0, -104.0, 38.0]
_TIME = "2024-01-01/2024-03-31"


# -- capability fixtures ---------------------------------------------------


def _grid_caps() -> CollectionCapabilities:
    """A gridded collection whose one service does bbox+variable+temporal → Zarr."""
    svc = ServiceCapability(
        service_name="l3-subsetter",
        concept_id="S100-LPCLOUD",
        subset_bbox=True,
        subset_variable=True,
        subset_temporal=True,
        output_formats=frozenset({"application/zarr", "application/netcdf4"}),
    )
    return CollectionCapabilities(
        concept_id=_CONCEPT_ID,
        short_name="MOD13Q1",
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset(),
        direct_s3=None,
        services=[svc],
        capabilities_version="2",
        advisory=[],
    )


def _no_service_caps() -> CollectionCapabilities:
    """A collection with no Harmony service and no direct S3 — nothing fits."""
    return CollectionCapabilities(
        concept_id=_CONCEPT_ID,
        short_name="MOD13Q1",
        processing_level="2",
        output_shape="swath",
        native_formats=frozenset(),
        direct_s3=None,
        services=[],
        capabilities_version="1",
        advisory=[],
    )


def _make_cmr(caps: CollectionCapabilities) -> CMRProvider:
    """A ``CMRProvider`` whose ``collection_capabilities`` returns ``caps``.

    Built with ``__new__`` so no real ``__init__`` (settings/HTTP) runs.
    """
    cmr = CMRProvider.__new__(CMRProvider)
    cmr.collection_capabilities = AsyncMock(return_value=caps)
    return cmr


@pytest.fixture
def grid_cmr() -> CMRProvider:
    return _make_cmr(_grid_caps())


@pytest.fixture
def mock_enqueue() -> AsyncMock:
    return AsyncMock()


# -- seed helpers ----------------------------------------------------------


async def _seed_dataset(store, workspace_id: str) -> str:
    return await store.put_handle(
        workspace_id,
        HandleType.DATASET,
        {"concept_id": _CONCEPT_ID, "collection": {}, "search": {}},
    )


async def _seed_aoi(store, workspace_id: str) -> str:
    return await store.put_handle(
        workspace_id,
        HandleType.AOI,
        {"source": "bbox", "bbox": _BBOX, "geojson": None, "query": None},
    )


def _kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue):
    return dict(
        cmr=grid_cmr,
        store=workspace_store,
        provenance=provenance_store,
        session_factory=session_factory,
        enqueue_fn=mock_enqueue,
    )


# -- retrieve_data ---------------------------------------------------------


async def test_retrieve_data_returns_handles(
    grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue,
    workspace_id,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        **_kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue),
    )

    assert out["job_handle"].startswith("job_")
    assert out["obs_handle"].startswith("obs_")
    assert out["status"] == JobState.PENDING.value
    assert out["provider"] == "harmony"
    # The worker is kicked off via the queue, by the new job's id.
    mock_enqueue.assert_awaited_once()
    assert mock_enqueue.await_args.args[0] == "submit_job"


async def test_retrieve_data_creates_pending_job_in_db(
    grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue,
    workspace_id,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        **_kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue),
    )

    async with session_factory() as session:
        job = await get_job_by_handle(session, out["job_handle"])
    assert job is not None
    assert job.state == JobState.PENDING.value
    assert job.obs_handle == out["obs_handle"]
    assert job.provider == "harmony"


async def test_retrieve_data_stores_request_spec_not_url(
    grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue,
    workspace_id,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        **_kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue),
    )

    async with session_factory() as session:
        job = await get_job_by_handle(session, out["job_handle"])
    spec = job.request_spec
    # Re-materializable: the planning inputs are there...
    assert spec["concept_id"] == _CONCEPT_ID
    assert spec["time_range"] == _TIME
    assert spec["aoi_bbox"] == _BBOX
    assert spec["service_name"] == "l3-subsetter"
    # ...and nothing ephemeral: no value is a staged-output URL.
    for value in spec.values():
        if isinstance(value, str):
            assert not value.lower().startswith(("http://", "https://", "s3://"))


async def test_retrieve_data_defaults_zarr_for_grid(
    grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue,
    workspace_id,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id, output_format=None,
        **_kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue),
    )

    async with session_factory() as session:
        job = await get_job_by_handle(session, out["job_handle"])
    assert job.request_spec["output_format"] == "application/zarr"
    assert job.request_spec["output_shape"] == "grid"


async def test_retrieve_data_not_retrievable_raises(
    workspace_store, provenance_store, session_factory, mock_enqueue, workspace_id,
) -> None:
    cmr = _make_cmr(_no_service_caps())
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    with pytest.raises(NotRetrievable):
        await retrieve_data(
            ds, aoi, _TIME, workspace_id=workspace_id,
            cmr=cmr, store=workspace_store, provenance=provenance_store,
            session_factory=session_factory, enqueue_fn=mock_enqueue,
        )
    # Failed at planning time → nothing enqueued, no orphan job.
    mock_enqueue.assert_not_awaited()


# -- retrieve_subset -------------------------------------------------------


async def test_retrieve_subset_sets_variable_flag(
    grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue,
    workspace_id,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await retrieve_subset(
        ds, aoi, _TIME, ["NDVI"], workspace_id=workspace_id,
        **_kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue),
    )

    async with session_factory() as session:
        job = await get_job_by_handle(session, out["job_handle"])
    spec = job.request_spec
    assert spec["needs_variable"] is True
    assert spec["needs_bbox"] is True
    assert spec["variables"] == ["NDVI"]


# -- retrieve_timeseries ---------------------------------------------------


async def test_retrieve_timeseries_no_aoi_required(
    grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue,
    workspace_id,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)

    out = await retrieve_timeseries(
        ds, _TIME, ["NDVI"], workspace_id=workspace_id, aoi_handle=None,
        **_kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue),
    )

    async with session_factory() as session:
        job = await get_job_by_handle(session, out["job_handle"])
    spec = job.request_spec
    assert spec["needs_bbox"] is False
    assert spec["aoi_bbox"] is None
    assert spec["needs_variable"] is True


# -- get_retrieval_status --------------------------------------------------


async def test_get_status_reads_from_postgres(
    grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue,
    workspace_id,
) -> None:
    """Mutate the durable row directly, then read through the tool — it must
    reflect the DB, not anything the submit call returned."""
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)
    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        **_kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue),
    )
    job_handle = out["job_handle"]

    # Directly advance the row in the DB (bypassing the tools entirely).
    async with session_factory() as session:
        job = await get_job_by_handle(session, job_handle)
        job.state = JobState.RUNNING.value
        job.progress = 42
        job.error = None
        await session.commit()

    status = await get_retrieval_status(
        job_handle, workspace_id=workspace_id,
        store=workspace_store, session_factory=session_factory,
    )
    assert status["status"] == JobState.RUNNING.value
    assert status["progress"] == 42
    assert status["obs_handle"] == out["obs_handle"]


async def test_get_status_wrong_handle_type_raises(
    workspace_store, session_factory, workspace_id,
) -> None:
    aoi = await _seed_aoi(workspace_store, workspace_id)
    with pytest.raises(ValueError, match="job_"):
        await get_retrieval_status(
            aoi, workspace_id=workspace_id,
            store=workspace_store, session_factory=session_factory,
        )


async def test_get_status_cross_workspace_denied(
    grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue,
    workspace_id,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)
    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        **_kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue),
    )

    with pytest.raises(CrossWorkspaceError):
        await get_retrieval_status(
            out["job_handle"], workspace_id="ws-intruder",
            store=workspace_store, session_factory=session_factory,
        )


# -- cancel_retrieval ------------------------------------------------------


async def test_cancel_pending_job(
    grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue,
    workspace_id,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)
    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        **_kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue),
    )

    res = await cancel_retrieval(
        out["job_handle"], workspace_id=workspace_id,
        store=workspace_store, session_factory=session_factory,
    )
    assert res["status"] == JobState.CANCELLED.value

    async with session_factory() as session:
        job = await get_job_by_handle(session, out["job_handle"])
    assert job.state == JobState.CANCELLED.value


async def test_cancel_terminal_job_raises(
    grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue,
    workspace_id,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)
    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        **_kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue),
    )

    # Force the row to a terminal state, then cancelling must be illegal.
    async with session_factory() as session:
        job = await get_job_by_handle(session, out["job_handle"])
        job.state = JobState.READY.value
        await session.commit()

    with pytest.raises(ValueError):
        await cancel_retrieval(
            out["job_handle"], workspace_id=workspace_id,
            store=workspace_store, session_factory=session_factory,
        )


async def test_cancel_cross_workspace_denied(
    grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue,
    workspace_id,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)
    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        **_kwargs(grid_cmr, workspace_store, provenance_store, session_factory, mock_enqueue),
    )

    with pytest.raises(CrossWorkspaceError):
        await cancel_retrieval(
            out["job_handle"], workspace_id="ws-intruder",
            store=workspace_store, session_factory=session_factory,
        )
