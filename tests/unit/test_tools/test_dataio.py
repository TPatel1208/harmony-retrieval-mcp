"""``_dataio`` read paths — netCDF group-flattening and the multi-granule bundle.

These cover the half of the format-by-shape vocabulary added so Harmony/OPeNDAP
netCDF results are consumable by preview/transform without changing those tools:

* a grouped netCDF (TEMPO/OMI keep science vars under ``/product``, ``/geolocation``)
  flattens into one dataset with group-prefixed names — a plain ``open_dataset`` would
  surface no data variables at all;
* a zip bundle of single-time granule subsets concatenates on the ``time`` axis, which
  is how a multi-day OPeNDAP retrieval is carried.

The Zarr/Parquet round-trips already had coverage in test_transform; these add the
netCDF reader. They need an h5netcdf engine (a project dependency).
"""

from __future__ import annotations

import io
import os
import tempfile
import zipfile

import numpy as np
import xarray as xr

from earthdata_mcp.tools._dataio import (
    NETCDF_BUNDLE_MEDIA_TYPE,
    open_result,
)


def _grouped_netcdf_bytes() -> bytes:
    """A netCDF-4 file with science vars under ``/product`` and ``/geolocation``.

    Multi-group writes need a real file (append mode), so we use a temp dir with
    ``ignore_cleanup_errors`` to tolerate Windows' lazy file-handle release.
    """
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        path = os.path.join(tmp, "granule.nc")
        # Root group carries nothing the consumer needs — the data is in subgroups,
        # which is exactly the TEMPO/OMI shape that defeats a plain open_dataset.
        xr.Dataset().to_netcdf(path, engine="h5netcdf", mode="w")
        xr.Dataset(
            {"vertical_column": ("time", np.array([1.0, 2.0], dtype="float64"))},
            coords={"time": np.array([0, 1], dtype="int64")},
        ).to_netcdf(path, group="product", engine="h5netcdf", mode="a")
        xr.Dataset(
            {"latitude": ("time", np.array([40.0, 41.0], dtype="float64"))},
            coords={"time": np.array([0, 1], dtype="int64")},
        ).to_netcdf(path, group="geolocation", engine="h5netcdf", mode="a")
        with open(path, "rb") as f:
            return f.read()


def _flat_netcdf_bytes(time_value: int, x_value: float) -> bytes:
    """A flat (groupless) single-time netCDF granule subset."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        path = os.path.join(tmp, "granule.nc")
        xr.Dataset(
            {"x": ("time", np.array([x_value], dtype="float64"))},
            coords={"time": np.array([time_value], dtype="int64")},
        ).to_netcdf(path, engine="h5netcdf", mode="w")
        with open(path, "rb") as f:
            return f.read()


def test_open_netcdf_flattens_groups_with_prefixed_names() -> None:
    ds = open_result(_grouped_netcdf_bytes(), "application/netcdf4")

    assert isinstance(ds, xr.Dataset)
    # Group path becomes a name prefix, so nothing is lost and nothing collides.
    assert "product__vertical_column" in ds.data_vars
    assert "geolocation__latitude" in ds.data_vars
    # Data survives the round-trip.
    assert ds["product__vertical_column"].values.tolist() == [1.0, 2.0]
    assert ds["geolocation__latitude"].values.tolist() == [40.0, 41.0]
    # The shared dimension coordinate stays unprefixed so axes still align.
    assert "time" in ds.coords


def test_open_netcdf_bundle_concats_on_time() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("g0.nc", _flat_netcdf_bytes(0, 10.0))
        zf.writestr("g1.nc", _flat_netcdf_bytes(1, 20.0))

    ds = open_result(buf.getvalue(), NETCDF_BUNDLE_MEDIA_TYPE)

    assert isinstance(ds, xr.Dataset)
    # Two single-time granules concatenate into a length-2 series in name order.
    assert ds.sizes["time"] == 2
    assert ds["x"].values.tolist() == [10.0, 20.0]


def test_open_netcdf_bundle_single_member_is_the_member() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("only.nc", _flat_netcdf_bytes(0, 5.0))

    ds = open_result(buf.getvalue(), NETCDF_BUNDLE_MEDIA_TYPE)

    assert ds.sizes["time"] == 1
    assert ds["x"].values.tolist() == [5.0]


def _cf_time_netcdf_bytes(time_value: int, x_value: float) -> bytes:
    """A flat single-time granule with a CF-encoded float64 ``time`` coordinate.

    Uses ``units: seconds since 2000-01-01T00:00:00Z`` to mimic the encoding
    TEMPO/OMI L3 products write — the same encoding that ``_strip_unsafe_coord_attrs``
    removes during multi-granule concat, and that ``_open_netcdf_bundle`` must
    re-attach so the downstream Zarr round-trip can decode ``time`` to datetime64.
    """
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        path = os.path.join(tmp, "granule.nc")
        time_arr = np.array([float(time_value)], dtype="float64")
        ds = xr.Dataset(
            {"x": ("time", np.array([x_value], dtype="float64"))},
            coords={
                "time": (
                    "time",
                    time_arr,
                    {"units": "seconds since 2000-01-01T00:00:00Z", "calendar": "standard"},
                )
            },
        )
        ds.to_netcdf(path, engine="h5netcdf", mode="w")
        with open(path, "rb") as f:
            return f.read()


def test_open_netcdf_bundle_preserves_time_units_after_concat() -> None:
    """Multi-granule bundle concat must re-attach the CF ``units`` attr on ``time``.

    ``_strip_unsafe_coord_attrs`` removes ``units`` so ``xr.concat`` doesn't choke
    on attribute-equality checks across granules.  Without the re-attach step the
    resulting dataset has a raw float64 ``time`` coordinate with no CF metadata —
    ``xr.open_zarr`` (decode_times=True) then cannot decode it, and any downstream
    ``resample(time=...)`` call raises a TypeError.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("g0.nc", _cf_time_netcdf_bytes(0, 10.0))
        zf.writestr("g1.nc", _cf_time_netcdf_bytes(86400, 20.0))  # +1 day in seconds

    ds = open_result(buf.getvalue(), NETCDF_BUNDLE_MEDIA_TYPE)

    assert ds.sizes["time"] == 2
    # The ``units`` attr must survive so Zarr round-trips can decode back to datetime64.
    assert "units" in ds["time"].attrs, (
        "time units attr must be re-attached after bundle concat"
    )
