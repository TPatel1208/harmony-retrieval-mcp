"""Preview/inspection tools — GIBS reference, structural summary, descriptive stats.

Handles and provenance live in the real Postgres-backed fixtures; materialized
results live in the ``local_backend`` filesystem fixture, serialized through the
shared ``tools/_dataio`` route so these tools read exactly what the transform tools
(and the Phase 6 worker) write. ``preview_dataset`` makes no network call — it only
constructs a GIBS request — so it is fully exercised offline.
"""

from __future__ import annotations

from uuid import uuid4

import numpy as np
import pytest
import xarray as xr

from earthdata_mcp.tools._dataio import ZARR_MEDIA_TYPE, serialize_result
from earthdata_mcp.tools.preview import (
    inspect_statistics,
    preview_dataset,
    summarize_dataset,
)
from earthdata_mcp.workspace.models import HandleType
from earthdata_mcp.workspace.store import CrossWorkspaceError

_CONCEPT_ID = "C1234567890-LPCLOUD"
_BBOX = [-105.0, 37.0, -104.0, 38.0]


def _grid() -> xr.Dataset:
    """A tiny gridded cube with a known mean for stat assertions."""
    return xr.Dataset(
        {"ndvi": (("lat", "lon"), np.arange(6, dtype="float32").reshape(2, 3))},
        coords={"lat": [37.0, 38.0], "lon": [-105.0, -104.5, -104.0]},
        attrs={"crs": "EPSG:4326"},
    )


async def _seed_dataset(store, workspace_id: str) -> str:
    return await store.put_handle(
        workspace_id,
        HandleType.DATASET,
        {
            "concept_id": _CONCEPT_ID,
            "collection": {"short_name": "MOD13Q1", "processing_level": "3"},
        },
    )


async def _seed_aoi(store, workspace_id: str) -> str:
    return await store.put_handle(
        workspace_id, HandleType.AOI, {"source": "bbox", "bbox": _BBOX}
    )


async def _seed_cube(
    store, storage, workspace_id: str, ds: xr.Dataset, handle_type=HandleType.OBS
) -> str:
    name, data = serialize_result(ds, ZARR_MEDIA_TYPE)
    key = f"results/{uuid4().hex}/{name}"
    await storage.put(key, data)
    return await store.put_handle(
        workspace_id,
        handle_type,
        {"status": "ready", "storage_key": key, "media_type": ZARR_MEDIA_TYPE},
    )


# -- preview_dataset -------------------------------------------------------


async def test_preview_dataset_mints_preview_handle(workspace_store, workspace_id):
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await preview_dataset(
        ds, time_range="2024-01-01/2024-03-31", aoi_handle=aoi,
        workspace_id=workspace_id, store=workspace_store,
    )

    assert out["handle"].startswith("preview_")
    assert out["layer"] == "MOD13Q1"
    assert out["bbox"] == _BBOX  # AOI bbox wins over the collection extent
    assert out["time"] == "2024-01-01"  # start date of the range
    assert "gibs.earthdata.nasa.gov" in out["gibs_url"]
    assert "LAYERS=MOD13Q1" in out["gibs_url"]


async def test_preview_handle_payload_has_no_url_source_of_truth(
    workspace_store, workspace_id
):
    ds = await _seed_dataset(workspace_store, workspace_id)
    out = await preview_dataset(ds, workspace_id=workspace_id, store=workspace_store)

    record = await workspace_store.get_handle(workspace_id, out["handle"])
    # The durable payload is the re-constructible GIBS spec, not the URL.
    assert record.payload["source"] == "gibs"
    assert record.payload["layer"] == "MOD13Q1"
    for value in record.payload.values():
        if isinstance(value, str):
            assert not value.lower().startswith(("http://", "https://"))


async def test_preview_explicit_layer_overrides(workspace_store, workspace_id):
    ds = await _seed_dataset(workspace_store, workspace_id)
    out = await preview_dataset(
        ds, layer="MODIS_Terra_NDVI_8Day",
        workspace_id=workspace_id, store=workspace_store,
    )
    assert out["layer"] == "MODIS_Terra_NDVI_8Day"
    assert out["layer_is_guess"] is False


async def test_preview_rejects_non_dataset_handle(workspace_store, workspace_id):
    aoi = await _seed_aoi(workspace_store, workspace_id)
    with pytest.raises(ValueError, match="dataset_"):
        await preview_dataset(aoi, workspace_id=workspace_id, store=workspace_store)


# -- summarize_dataset -----------------------------------------------------


async def test_summarize_dataset_metadata(workspace_store, workspace_id):
    ds = await _seed_dataset(workspace_store, workspace_id)
    out = await summarize_dataset(ds, workspace_id=workspace_id, store=workspace_store)
    assert out["kind"] == "dataset"
    assert out["summary"]["short_name"] == "MOD13Q1"
    assert out["summary"]["processing_level"] == "3"


async def test_summarize_materialized_cube(
    workspace_store, local_backend, workspace_id
):
    cube = await _seed_cube(workspace_store, local_backend, workspace_id, _grid())
    out = await summarize_dataset(
        cube, workspace_id=workspace_id, store=workspace_store, storage=local_backend
    )
    assert out["summary"]["type"] == "grid"
    assert out["summary"]["dims"] == {"lat": 2, "lon": 3}
    assert "ndvi" in out["summary"]["data_vars"]


async def test_summarize_unmaterialized_obs_returns_status(
    workspace_store, workspace_id
):
    handle = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )
    out = await summarize_dataset(handle, workspace_id=workspace_id, store=workspace_store)
    assert out["status"] == "pending"
    assert out["summary"] is None


# -- inspect_statistics ----------------------------------------------------


async def test_inspect_statistics_descriptive(
    workspace_store, local_backend, workspace_id
):
    cube = await _seed_cube(workspace_store, local_backend, workspace_id, _grid())
    out = await inspect_statistics(
        cube, workspace_id=workspace_id, store=workspace_store, storage=local_backend
    )
    stats = out["statistics"]["ndvi"]
    assert stats["min"] == 0.0
    assert stats["max"] == 5.0
    assert stats["mean"] == pytest.approx(2.5)
    assert stats["count"] == 6


async def test_inspect_statistics_rejects_dataset_handle(
    workspace_store, workspace_id
):
    ds = await _seed_dataset(workspace_store, workspace_id)
    with pytest.raises(ValueError, match="obs_ or cube_"):
        await inspect_statistics(ds, workspace_id=workspace_id, store=workspace_store)


async def test_inspect_statistics_cross_workspace_denied(
    workspace_store, local_backend, workspace_id
):
    cube = await _seed_cube(workspace_store, local_backend, workspace_id, _grid())
    with pytest.raises(CrossWorkspaceError):
        await inspect_statistics(
            cube, workspace_id="ws-intruder",
            store=workspace_store, storage=local_backend,
        )
