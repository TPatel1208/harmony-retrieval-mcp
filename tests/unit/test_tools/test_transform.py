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

import os
import tempfile
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


def _time_grid(var: str, time_offset_ns: int = 0) -> xr.Dataset:
    """3×3 grid with a 3-step time axis, offset by ``time_offset_ns`` nanoseconds."""
    lat = [37.0, 38.0, 39.0]
    lon = [-105.0, -104.5, -104.0]
    # Nominal scan times at T=0, +1h, +2h; sub-second offset simulates
    # TEMPO products storing scan-center vs scan-start times differently.
    base_ns = np.array([0, 3_600_000_000_000, 7_200_000_000_000], dtype="int64")
    times = (base_ns + time_offset_ns).astype("datetime64[ns]")
    data = np.ones((3, len(lat), len(lon)), dtype="float32")
    return xr.Dataset(
        {var: (("time", "lat", "lon"), data)},
        coords={"time": times, "lat": lat, "lon": lon},
    )


async def test_align_sub_second_time_offset_does_not_produce_all_nan(
    workspace_store, provenance_store, local_backend, workspace_id
):
    """Sub-second timestamp offsets between TEMPO products must not produce NaN output.

    Without snapping, xr.align(join="outer") sees no exact time matches and
    creates 2× the expected steps — every variable is NaN at every peer's
    timestep.  The snap_time_freq parameter must collapse them back to 3 steps
    with finite values.
    """
    # 500 ms offset between the two products (sub-second, realistic TEMPO delta).
    delta_ns = 500_000_000
    no2 = _time_grid("no2", time_offset_ns=0)
    hcho = _time_grid("hcho", time_offset_ns=delta_ns)

    a = await _seed_cube(workspace_store, local_backend, workspace_id, no2)
    b = await _seed_cube(workspace_store, local_backend, workspace_id, hcho)

    out = await align(
        [a, b],
        method="outer",
        snap_time_freq=60 * 10**9,  # snap to nearest minute
        workspace_id=workspace_id,
        **_deps(workspace_store, provenance_store, local_backend),
    )

    cube, _ = await _read_cube(workspace_store, local_backend, workspace_id, out["handle"])
    # Must have 3 time steps, not 6.
    assert cube.sizes["time"] == 3, (
        f"expected 3 time steps after snapping, got {cube.sizes['time']}"
    )
    # Both variables must be finite at every timestep (no NaN from misalignment).
    assert not bool(np.isnan(cube["no2"].values).any()), "no2 has unexpected NaN"
    assert not bool(np.isnan(cube["hcho"].values).any()), "hcho has unexpected NaN"


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


# -- TEMPO L3 regression tests ---------------------------------------------
# TEMPO NO2/HCHO L3 data is read with decode_times=False, so:
#   • time coordinate is raw float64 (seconds since GPS epoch), not datetime64
#   • quality-flag variables stay as int16/int32 (no scale_factor to trigger decode)
# These two properties cause two distinct failures in align and resample.


def _tempo_like_grid(
    var_float: str,
    var_int: str | None = None,
    *,
    lon_offset: float = 0.0,
    include_time_units: bool = False,
) -> xr.Dataset:
    """TEMPO L3-like dataset: float32 science var, optional int16 quality var, float64 time.

    ``lon_offset`` shifts the lon axis so two grids can have partially different
    coverage (outer join then inserts NaN-padded positions into the other dataset).
    ``include_time_units`` adds a CF ``units`` attr; omit it to simulate the
    post-bundle-concat state where ``_strip_unsafe_coord_attrs`` has already removed it.
    """
    # Raw float64 seconds — exactly what xarray stores when decode_times=False and
    # no units attr survives to trigger open_zarr's CF decoder on round-trip.
    time_vals = np.array([0.0, 3_600.0, 7_200.0], dtype="float64")
    lat = [37.0, 38.0, 39.0]
    lon = [-105.0 + lon_offset, -104.5 + lon_offset, -104.0 + lon_offset]
    data_vars: dict = {
        var_float: (("time", "lat", "lon"), np.ones((3, 3, 3), dtype="float32")),
    }
    if var_int is not None:
        data_vars[var_int] = (("time", "lat", "lon"), np.zeros((3, 3, 3), dtype="int16"))
    time_attrs = {"units": "seconds since 1980-01-06T00:00:00Z"} if include_time_units else {}
    return xr.Dataset(
        data_vars,
        coords={"time": ("time", time_vals, time_attrs), "lat": lat, "lon": lon},
    )


async def test_align_integer_dtype_vars_survive_outer_join(
    workspace_store, provenance_store, local_backend, workspace_id
):
    """Outer align must not raise a divide error on int16 quality-flag variables.

    TEMPO NO2 and HCHO L3 both carry a main_data_quality_flag (int16) alongside
    float science variables.  The two lon grids are offset so the outer join must
    introduce NaN-padded positions into each dataset — that NaN fill on an int16
    array is what triggers the bug.  Before the fix, xr.align raised:
      ufunc 'divide' not supported for the input types...
    """
    # Offset lon by 1° so the grids overlap partially and outer join must pad with NaN.
    no2 = _tempo_like_grid("no2", var_int="main_data_quality_flag", lon_offset=0.0)
    hcho = _tempo_like_grid("hcho", var_int="main_data_quality_flag", lon_offset=1.0)

    a = await _seed_cube(workspace_store, local_backend, workspace_id, no2)
    b = await _seed_cube(workspace_store, local_backend, workspace_id, hcho)

    out = await align(
        [a, b], method="outer", workspace_id=workspace_id,
        **_deps(workspace_store, provenance_store, local_backend),
    )
    assert out["handle"].startswith("cube_")
    cube, _ = await _read_cube(workspace_store, local_backend, workspace_id, out["handle"])
    assert "main_data_quality_flag" in cube.data_vars
    # After the cast-to-float fix, integer quality flags must survive as float so
    # NaN-padded positions (where one dataset has no coverage) are preserved
    # correctly.  Before the fix they stayed int16 and NaN was silently truncated
    # to 0, producing corrupt fill values.
    assert not np.issubdtype(cube["main_data_quality_flag"].dtype, np.integer), (
        "quality flag must be float after alignment so NaN fill is lossless"
    )


async def test_resample_float64_time_works(
    workspace_store, provenance_store, local_backend, workspace_id
):
    """Temporal resample must succeed when time coord has CF units and is float64 in Zarr.

    For single NetCDF granules (non-bundle path) the ``units`` attr on the time
    coordinate survives the Zarr round-trip, so ``xr.open_zarr`` (decode_times=True)
    decodes it to datetime64.  For the bundle path the ``_open_netcdf_bundle`` fix
    re-attaches the units after concat so the same decode happens.  Both paths must
    allow resample to proceed.

    Time values [0, 3600, 7200] seconds since 1980-01-06 all fall on the same day,
    so resampling by "1D" must produce a single time step.
    """
    # include_time_units=True → units attr survives Zarr round-trip → open_zarr
    # decodes float64 to datetime64 → resample works.
    ds = _tempo_like_grid("no2", include_time_units=True)
    src = await _seed_cube(workspace_store, local_backend, workspace_id, ds)

    out = await resample(
        src, time_freq="1D", workspace_id=workspace_id,
        **_deps(workspace_store, provenance_store, local_backend),
    )
    assert out["handle"].startswith("cube_")
    cube, _ = await _read_cube(workspace_store, local_backend, workspace_id, out["handle"])
    # All 3 hourly steps are on 1980-01-06, so daily resample collapses to 1 step.
    assert cube.sizes["time"] == 1


async def test_resample_netcdf_obs_float64_time_decodes_and_resamples(
    workspace_store, provenance_store, local_backend, workspace_id
):
    """Resample on a netCDF obs must decode float64 time via CF units before resampling.

    Harmony delivers netCDF with time encoded as float64 seconds (CF convention).
    ``open_result`` reads it with ``decode_times=False``, so time stays float64 with
    a ``units`` attr.  Unlike the Zarr path (where ``open_zarr`` auto-decodes),
    the netCDF path reaches resample with raw float64 time — the
    ``_maybe_decode_float_time`` helper must bridge the gap.
    """
    # Write a TEMPO-like single-granule netCDF to storage directly (no Zarr round-trip)
    # so the decode_times=False netCDF reading path is exercised, not the Zarr path.
    time_vals = np.array([0.0, 3_600.0, 7_200.0], dtype="float64")
    ds = xr.Dataset(
        {"no2": (("time", "lat", "lon"), np.ones((3, 3, 3), dtype="float32"))},
        coords={
            "time": ("time", time_vals, {"units": "seconds since 1980-01-06T00:00:00Z"}),
            "lat": [37.0, 38.0, 39.0],
            "lon": [-105.0, -104.5, -104.0],
        },
    )
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        nc_path = os.path.join(tmp, "granule.nc")
        ds.to_netcdf(nc_path, engine="h5netcdf", mode="w")
        with open(nc_path, "rb") as f:
            nc_bytes = f.read()

    key = f"results/{uuid4().hex}/granule.nc"
    await local_backend.put(key, nc_bytes)
    src = await workspace_store.put_handle(
        workspace_id,
        HandleType.OBS,
        {"status": "ready", "storage_key": key, "media_type": "application/netcdf4"},
    )

    out = await resample(
        src, time_freq="1D", workspace_id=workspace_id,
        **_deps(workspace_store, provenance_store, local_backend),
    )
    assert out["handle"].startswith("cube_")
    cube, _ = await _read_cube(workspace_store, local_backend, workspace_id, out["handle"])
    assert cube.sizes["time"] == 1
