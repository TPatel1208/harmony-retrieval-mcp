"""Coverage tools — handle resolution, CMR delegation, size aggregation.

CMR is faked at the object level (``AsyncMock``) rather than over HTTP: these
tools care about resolving the right ``concept_id``/bbox from the handles and
shaping the result, not about CMR's wire format (that is ``test_cmr.py``'s job).
Handles are seeded into the real Postgres-backed ``workspace_store`` so workspace
isolation is exercised for real.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools.coverage import (
    check_availability,
    check_coverage,
    estimate_retrieval_size,
    inspect_granules,
)
from earthdata_mcp.workspace.models import HandleType
from earthdata_mcp.workspace.store import CrossWorkspaceError

_CONCEPT_ID = "C1234567890-LPCLOUD"
_BBOX = [-105.0, 37.0, -104.0, 38.0]
_BBOX_STR = "-105.0,37.0,-104.0,38.0"
_TIME = "2024-01-01/2024-03-31"

_GRANULE_1 = {
    "concept_id": "G1-X",
    "granule_ur": "MOD13Q1.A2024001.h09v04.061.nc",
    "related_urls": [],
    "cloud_cover": None,
    "day_night_flag": "Day",
    "size_mb": 120.5,
}
_GRANULE_2 = {
    "concept_id": "G2-X",
    "granule_ur": "MOD13Q1.A2024009.h09v04.061.nc",
    "related_urls": [],
    "cloud_cover": None,
    "day_night_flag": "Day",
    "size_mb": 118.2,
}
_AVAIL_RESULT = {
    "collection_concept_id": _CONCEPT_ID,
    "granule_count": 12,
    "available": True,
}


def _make_cmr(*, granules=None, availability=None) -> CMRProvider:
    """A ``CMRProvider`` whose two network methods are ``AsyncMock``s.

    Built with ``__new__`` so no real ``__init__`` (settings/HTTP) runs.
    """
    cmr = CMRProvider.__new__(CMRProvider)
    cmr.search_granules = AsyncMock(
        return_value=[_GRANULE_1, _GRANULE_2] if granules is None else granules
    )
    cmr.check_availability = AsyncMock(
        return_value=_AVAIL_RESULT if availability is None else availability
    )
    return cmr


@pytest.fixture
def fake_cmr() -> CMRProvider:
    return _make_cmr()


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


# -- check_availability ---------------------------------------------------


async def test_check_availability_resolves_and_delegates(
    fake_cmr, workspace_store, workspace_id
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await check_availability(
        ds, aoi, _TIME, workspace_id=workspace_id, cmr=fake_cmr, store=workspace_store
    )

    assert out["available"] is True
    assert out["granule_count"] == 12
    assert out["dataset_handle"] == ds
    assert out["aoi_handle"] == aoi
    assert out["time_range"] == _TIME
    fake_cmr.check_availability.assert_awaited_once_with(
        _CONCEPT_ID, bounding_box=_BBOX_STR, temporal=_TIME
    )


# -- check_coverage -------------------------------------------------------


async def test_check_coverage_returns_sample_granules(
    fake_cmr, workspace_store, workspace_id
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await check_coverage(
        ds, aoi, _TIME, workspace_id=workspace_id, cmr=fake_cmr, store=workspace_store
    )

    assert out["covered"] is True
    assert out["granule_count"] == 2
    assert len(out["sample_granules"]) == 2
    assert out["sample_granules"][0]["concept_id"] == "G1-X"


async def test_check_coverage_empty_is_not_covered(
    workspace_store, workspace_id
) -> None:
    cmr = _make_cmr(granules=[])
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await check_coverage(
        ds, aoi, _TIME, workspace_id=workspace_id, cmr=cmr, store=workspace_store
    )
    assert out["covered"] is False
    assert out["granule_count"] == 0


# -- inspect_granules -----------------------------------------------------


async def test_inspect_granules_forwards_limit(
    fake_cmr, workspace_store, workspace_id
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await inspect_granules(
        ds,
        aoi,
        _TIME,
        workspace_id=workspace_id,
        limit=5,
        cmr=fake_cmr,
        store=workspace_store,
    )

    assert out["count"] == 2
    assert len(out["granules"]) == 2
    fake_cmr.search_granules.assert_awaited_once_with(
        _CONCEPT_ID, bounding_box=_BBOX_STR, temporal=_TIME, limit=5
    )


# -- estimate_retrieval_size ----------------------------------------------


async def test_estimate_retrieval_size_sums_sizes(
    fake_cmr, workspace_store, workspace_id
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await estimate_retrieval_size(
        ds, aoi, _TIME, workspace_id=workspace_id, cmr=fake_cmr, store=workspace_store
    )

    assert out["sampled_granules"] == 2
    assert out["total_size_mb"] == pytest.approx(238.7)
    assert out["avg_size_mb"] == pytest.approx(119.35)
    assert out["warning"] is None
    # Samples up to the CMR cap of 50.
    fake_cmr.search_granules.assert_awaited_once_with(
        _CONCEPT_ID, bounding_box=_BBOX_STR, temporal=_TIME, limit=50
    )


async def test_estimate_retrieval_size_zero_sizes_warns(
    workspace_store, workspace_id
) -> None:
    cmr = _make_cmr(
        granules=[{**_GRANULE_1, "size_mb": 0.0}, {**_GRANULE_2, "size_mb": 0.0}]
    )
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await estimate_retrieval_size(
        ds, aoi, _TIME, workspace_id=workspace_id, cmr=cmr, store=workspace_store
    )

    assert out["total_size_mb"] == 0.0
    assert out["warning"] is not None
    assert "size estimate unavailable" in out["warning"]


async def test_estimate_retrieval_size_no_granules_warns(
    workspace_store, workspace_id
) -> None:
    cmr = _make_cmr(granules=[])
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await estimate_retrieval_size(
        ds, aoi, _TIME, workspace_id=workspace_id, cmr=cmr, store=workspace_store
    )

    assert out["sampled_granules"] == 0
    assert out["total_size_mb"] == 0.0
    assert out["warning"] == "No granules found for this AOI and time range."


# -- handle resolution failures -------------------------------------------


async def test_wrong_dataset_handle_type_raises(
    fake_cmr, workspace_store, workspace_id
) -> None:
    aoi = await _seed_aoi(workspace_store, workspace_id)
    # Passing an aoi_ handle where a dataset_ handle is expected.
    with pytest.raises(ValueError, match="dataset_"):
        await check_availability(
            aoi,
            aoi,
            _TIME,
            workspace_id=workspace_id,
            cmr=fake_cmr,
            store=workspace_store,
        )
    fake_cmr.check_availability.assert_not_awaited()


async def test_wrong_aoi_handle_type_raises(
    fake_cmr, workspace_store, workspace_id
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    with pytest.raises(ValueError, match="aoi_"):
        await check_availability(
            ds,
            ds,
            _TIME,
            workspace_id=workspace_id,
            cmr=fake_cmr,
            store=workspace_store,
        )


async def test_cross_workspace_denied(
    fake_cmr, workspace_store, workspace_id
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    with pytest.raises(CrossWorkspaceError):
        await check_availability(
            ds,
            aoi,
            _TIME,
            workspace_id="ws-intruder",
            cmr=fake_cmr,
            store=workspace_store,
        )
