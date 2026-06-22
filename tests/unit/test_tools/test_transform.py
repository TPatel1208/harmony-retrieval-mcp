"""Transform tools — the Phase 7.2 gate.

Two assertions are load-bearing per the prompt and CLAUDE.md:
* every transform's output handle is a ``cube_`` handle;
* every transform writes a provenance edge from its source(s) to the new cube
  (checked through ``ProvenanceStore.ancestry``) — no silent transforms.

Handles + provenance are real (Postgres fixtures); source/derived data live in the
``local_backend`` filesystem fixture, serialized through the same ``tools/_dataio``
route the tools use, so a materialized cube can be read back and inspected.
"""

from __future__ import annotations

from uuid import uuid4

import numpy as np
import pytest
import xarray as xr

from earthdata_mcp.tools._dataio import (
    PARQUET_MEDIA_TYPE,
    ZARR_MEDIA_TYPE,
    open_result,
    serialize_result,
)
from earthdata_mcp.tools.transform import (
    align,
    convert_format,
    reproject,
    resample,
    subset,
)
from earthdata_mcp.workspace.models import HandleType
from earthdata_mcp.workspace.store import CrossWorkspaceError

_BBOX = [-105.0, 37.5, -104.0, 38.5]


def _grid(var: str = "ndvi", lon=(-105.0, -104.5, -104.0)) -> xr.Dataset:
    lat = [37.0, 38.0, 39.0]
    data = np.arange(len(lat) * len(lon), dtype="float32").reshape(len(lat), len(lon))
    return xr.Dataset(
        {var: (("lat", "lon"), data)},
        coords={"lat": lat, "lon": list(lon)},
        attrs={"crs": "EPSG:4326"},
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


async def _ancestor_handles(provenance_store, workspace_id, cube_handle) -> set[str]:
    rows = await provenance_store.ancestry(workspace_id, cube_handle)
    return {a.handle for a in rows}


async def _read_cube(store, storage, workspace_id, cube_handle):
    record = await store.get_handle(workspace_id, cube_handle)
    data = await storage.get(record.payload["storage_key"])
    return open_result(data, record.payload["media_type"]), record.payload


def _deps(workspace_store, provenance_store, local_backend) -> dict:
    return dict(store=workspace_store, provenance=provenance_store, storage=local_backend)


# -- subset ----------------------------------------------------------------


async def test_subset_returns_cube_and_records_edge(
    workspace_store, provenance_store, local_backend, workspace_id
):
    src = await _seed_cube(workspace_store, local_backend, workspace_id, _grid())
    aoi = await _seed_aoi(workspace_store, workspace_id)

    out = await subset(
        src, aoi_handle=aoi, variables=["ndvi"], workspace_id=workspace_id,
        **_deps(workspace_store, provenance_store, local_backend),
    )

    assert out["handle"].startswith("cube_")
    assert out["status"] == "ready"
    assert src in await _ancestor_handles(provenance_store, workspace_id, out["handle"])
    # The bbox actually narrowed the lat axis (37.5..38.5 → lat 38 only).
    cube, _ = await _read_cube(workspace_store, local_backend, workspace_id, out["handle"])
    assert cube.sizes["lat"] == 1


# -- reproject -------------------------------------------------------------


async def test_reproject_returns_cube_records_edge_and_sets_crs(
    workspace_store, provenance_store, local_backend, workspace_id
):
    src = await _seed_cube(workspace_store, local_backend, workspace_id, _grid())

    out = await reproject(
        src, crs="EPSG:3857", workspace_id=workspace_id,
        **_deps(workspace_store, provenance_store, local_backend),
    )

    assert out["handle"].startswith("cube_")
    assert src in await _ancestor_handles(provenance_store, workspace_id, out["handle"])
    # Per the user's requirement: read the materialized cube back and assert the CRS
    # attribute equals the requested CRS — never a vacuous pass when rioxarray absent.
    cube, _ = await _read_cube(workspace_store, local_backend, workspace_id, out["handle"])
    assert cube.attrs["crs"] == "EPSG:3857"
    assert cube["ndvi"].attrs["spatial_ref"] == "EPSG:3857"


# -- resample --------------------------------------------------------------


async def test_resample_returns_cube_and_records_edge(
    workspace_store, provenance_store, local_backend, workspace_id
):
    src = await _seed_cube(workspace_store, local_backend, workspace_id, _grid())

    out = await resample(
        src, spatial_factor=2, workspace_id=workspace_id,
        **_deps(workspace_store, provenance_store, local_backend),
    )

    assert out["handle"].startswith("cube_")
    assert src in await _ancestor_handles(provenance_store, workspace_id, out["handle"])
    # coarsen(boundary="trim") halves the 3-wide axes down to 1.
    cube, _ = await _read_cube(workspace_store, local_backend, workspace_id, out["handle"])
    assert cube.sizes["lon"] == 1


async def test_resample_requires_a_parameter(
    workspace_store, provenance_store, local_backend, workspace_id
):
    src = await _seed_cube(workspace_store, local_backend, workspace_id, _grid())
    with pytest.raises(ValueError, match="time_freq"):
        await resample(
            src, workspace_id=workspace_id,
            **_deps(workspace_store, provenance_store, local_backend),
        )


# -- convert_format --------------------------------------------------------


async def test_convert_format_grid_to_parquet(
    workspace_store, provenance_store, local_backend, workspace_id
):
    src = await _seed_cube(workspace_store, local_backend, workspace_id, _grid())

    out = await convert_format(
        src, output_format=PARQUET_MEDIA_TYPE, workspace_id=workspace_id,
        **_deps(workspace_store, provenance_store, local_backend),
    )

    assert out["handle"].startswith("cube_")
    assert out["output_format"] == PARQUET_MEDIA_TYPE
    assert src in await _ancestor_handles(provenance_store, workspace_id, out["handle"])
    record = await workspace_store.get_handle(workspace_id, out["handle"])
    assert record.payload["media_type"] == PARQUET_MEDIA_TYPE


# -- align -----------------------------------------------------------------


async def test_align_returns_cube_report_and_edge_per_source(
    workspace_store, provenance_store, local_backend, workspace_id
):
    a = await _seed_cube(
        workspace_store, local_backend, workspace_id, _grid("ndvi")
    )
    # Second input on a different lon grid → alignment must reconcile them.
    b = await _seed_cube(
        workspace_store, local_backend, workspace_id,
        _grid("lst", lon=(-104.5, -104.0, -103.5)),
    )

    out = await align(
        [a, b], workspace_id=workspace_id,
        **_deps(workspace_store, provenance_store, local_backend),
    )

    assert out["handle"].startswith("cube_")
    report = out["alignment_report"]
    assert report["n_inputs"] == 2
    assert report["shape_after"]["lon"] >= 3
    # An edge from EVERY input is in the lineage (no silent input).
    ancestors = await _ancestor_handles(provenance_store, workspace_id, out["handle"])
    assert {a, b} <= ancestors


async def test_align_requires_two_sources(
    workspace_store, provenance_store, local_backend, workspace_id
):
    a = await _seed_cube(workspace_store, local_backend, workspace_id, _grid())
    with pytest.raises(ValueError, match="two source"):
        await align(
            [a], workspace_id=workspace_id,
            **_deps(workspace_store, provenance_store, local_backend),
        )


# -- shared contract -------------------------------------------------------


async def test_transform_spec_is_url_free(
    workspace_store, provenance_store, local_backend, workspace_id
):
    """The provenance edge stores a re-materializable spec, never a staged URL."""
    src = await _seed_cube(workspace_store, local_backend, workspace_id, _grid())
    out = await reproject(
        src, crs="EPSG:3857", workspace_id=workspace_id,
        **_deps(workspace_store, provenance_store, local_backend),
    )
    # If a URL had slipped into the spec, record_edge would have raised; assert the
    # durable payload is clean too.
    record = await workspace_store.get_handle(workspace_id, out["handle"])
    for value in record.payload.values():
        if isinstance(value, str):
            assert not value.lower().startswith(("http://", "https://", "s3://"))


async def test_subset_cross_workspace_source_denied(
    workspace_store, provenance_store, local_backend, workspace_id
):
    src = await _seed_cube(workspace_store, local_backend, workspace_id, _grid())
    with pytest.raises(CrossWorkspaceError):
        await subset(
            src, workspace_id="ws-intruder",
            **_deps(workspace_store, provenance_store, local_backend),
        )


async def test_non_materialized_source_rejected(
    workspace_store, provenance_store, local_backend, workspace_id
):
    pending = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )
    with pytest.raises(ValueError, match="not a materialized result"):
        await subset(
            pending, workspace_id=workspace_id,
            **_deps(workspace_store, provenance_store, local_backend),
        )
