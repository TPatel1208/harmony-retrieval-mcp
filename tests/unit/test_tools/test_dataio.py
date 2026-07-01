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

import pyarrow as pa
import pytest

from earthdata_mcp.tools._dataio import (
    CSV_MEDIA_TYPE,
    NETCDF_BUNDLE_MEDIA_TYPE,
    NETCDF_WRITE_MEDIA_TYPE,
    PARQUET_MEDIA_TYPE,
    UnsupportedMediaType,
    normalize_output_format,
    open_result,
    serialize_result,
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


def _cf_time_netcdf_bytes(
    time_value: int, x_value: float, units: str = "seconds since 2000-01-01T00:00:00Z"
) -> bytes:
    """A flat single-time granule with a CF-encoded float64 ``time`` coordinate.

    Defaults to ``seconds since 2000-01-01T00:00:00Z`` to mimic the encoding
    TEMPO/OMI L3 products write. ``units`` is overridable so a test can give
    different members different reference epochs (as MERRA-2 daily granules do).
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
                    {"units": units, "calendar": "standard"},
                )
            },
        )
        ds.to_netcdf(path, engine="h5netcdf", mode="w")
        with open(path, "rb") as f:
            return f.read()


def test_open_netcdf_bundle_decodes_time_to_datetime64_after_concat() -> None:
    """Multi-granule bundle concat must decode ``time`` to real datetime64 values.

    ``_strip_unsafe_coord_attrs`` removes the CF ``units``/``calendar`` attrs so
    ``xr.concat`` doesn't choke on attribute-equality checks across granules.
    ``_open_netcdf_bundle`` decodes each member against its own ``units`` *before*
    that strip runs (see ``_decode_member_time``), so the concatenated result
    already carries proper datetime64 values with no CF metadata left to lose.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("g0.nc", _cf_time_netcdf_bytes(0, 10.0))
        zf.writestr("g1.nc", _cf_time_netcdf_bytes(86400, 20.0))  # +1 day in seconds

    ds = open_result(buf.getvalue(), NETCDF_BUNDLE_MEDIA_TYPE)

    assert ds.sizes["time"] == 2
    assert np.issubdtype(ds["time"].dtype, np.datetime64)
    assert list(ds["time"].values) == [
        np.datetime64("2000-01-01T00:00:00"),
        np.datetime64("2000-01-02T00:00:00"),
    ]


def test_open_netcdf_bundle_decodes_each_member_against_its_own_epoch() -> None:
    """Members with different CF reference epochs must decode to distinct dates.

    MERRA-2 daily granules each encode ``time`` as "minutes since <that day's
    date> 00:00:00" — the raw values ``[180]``/``[180]`` are identical across
    days.  A prior version of ``_open_netcdf_bundle`` stripped every member's
    ``units`` before concat and re-attached only the *first* member's units to
    the whole stack afterward, silently reinterpreting every later member's
    day-relative offset under the first member's epoch and collapsing distinct
    days onto one. Decoding each member against its own units before concat
    (``_decode_member_time``) must keep them distinct.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "g0.nc",
            _cf_time_netcdf_bytes(180, 10.0, units="minutes since 2020-09-15 00:00:00"),
        )
        zf.writestr(
            "g1.nc",
            _cf_time_netcdf_bytes(180, 20.0, units="minutes since 2020-09-16 00:00:00"),
        )

    ds = open_result(buf.getvalue(), NETCDF_BUNDLE_MEDIA_TYPE)

    assert ds.sizes["time"] == 2
    times = list(ds["time"].values)
    assert len(set(times)) == 2, f"expected two distinct days, got {times}"
    assert times == [
        np.datetime64("2020-09-15T03:00:00"),
        np.datetime64("2020-09-16T03:00:00"),
    ]


def _no_time_coord_netcdf_bytes(range_beginning_date: str, x_value: float) -> bytes:
    """A granule with a singleton ``Time`` (capitalized) dim and no ``time`` coord.

    Mirrors OMI_MINDS_NO2d: the only per-granule date lives in the
    ``RangeBeginningDate``/``RangeBeginningTime`` global attrs (standard CMR/UMM-G
    granule temporal metadata), not in any coordinate variable.
    """
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        path = os.path.join(tmp, "granule.nc")
        ds = xr.Dataset(
            {"x": ("Time", np.array([x_value], dtype="float64"))},
            attrs={
                "RangeBeginningDate": range_beginning_date,
                "RangeBeginningTime": "00:00:00.000000Z",
            },
        )
        ds.to_netcdf(path, engine="h5netcdf", mode="w")
        with open(path, "rb") as f:
            return f.read()


def test_open_netcdf_bundle_synthesizes_time_from_range_beginning_date() -> None:
    """A granule with no ``time`` coordinate at all must still get a real,
    indexed ``time`` after bundle concat.

    Without this, ``xr.concat(dim="time")`` can't find ``time`` in the member
    (it only has a differently-cased ``Time`` dim) and fabricates a brand-new,
    *unindexed* stacking dimension instead. That later fails ``xr.align``
    against any peer dataset that does carry a real time index:
    ``AlignmentError: ... note: an index is found along that dimension``.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("g0.nc", _no_time_coord_netcdf_bytes("2020-09-15", 10.0))
        zf.writestr("g1.nc", _no_time_coord_netcdf_bytes("2020-09-16", 20.0))

    ds = open_result(buf.getvalue(), NETCDF_BUNDLE_MEDIA_TYPE)

    assert "time" in ds.coords, "time must be an indexed coordinate, not a bare dim"
    assert "time" in ds.indexes
    assert np.issubdtype(ds["time"].dtype, np.datetime64)
    assert list(ds["time"].values) == [
        np.datetime64("2020-09-15T00:00:00"),
        np.datetime64("2020-09-16T00:00:00"),
    ]
    assert ds["x"].values.tolist() == [10.0, 20.0]


# -- convert_format serializers ----------------------------------------------


def test_serialize_and_reopen_csv_round_trips_a_table() -> None:
    table = pa.table({"lat": [37.0, 38.0], "ndvi": [0.1, 0.2]})

    name, data = serialize_result(table, CSV_MEDIA_TYPE)
    reopened = open_result(data, CSV_MEDIA_TYPE)

    assert name == "cube.csv"
    assert reopened.column("ndvi").to_pylist() == [0.1, 0.2]


def test_serialize_csv_rejects_a_dataset() -> None:
    ds = xr.Dataset({"ndvi": ("lat", np.array([0.1, 0.2]))}, coords={"lat": [37.0, 38.0]})
    with pytest.raises(UnsupportedMediaType, match="tabular"):
        serialize_result(ds, CSV_MEDIA_TYPE)


def test_serialize_and_reopen_netcdf_round_trips_a_dataset() -> None:
    ds = xr.Dataset({"ndvi": ("lat", np.array([0.1, 0.2]))}, coords={"lat": [37.0, 38.0]})

    name, data = serialize_result(ds, NETCDF_WRITE_MEDIA_TYPE)
    reopened = open_result(data, NETCDF_WRITE_MEDIA_TYPE)

    assert name == "cube.nc"
    assert reopened["ndvi"].values.tolist() == [0.1, 0.2]


def test_serialize_netcdf_rejects_a_table() -> None:
    table = pa.table({"lat": [37.0, 38.0], "ndvi": [0.1, 0.2]})
    with pytest.raises(UnsupportedMediaType, match="gridded"):
        serialize_result(table, NETCDF_WRITE_MEDIA_TYPE)


@pytest.mark.parametrize(
    "alias,canonical",
    [
        ("csv", CSV_MEDIA_TYPE),
        ("CSV", CSV_MEDIA_TYPE),
        ("text/csv", CSV_MEDIA_TYPE),
        ("netcdf", NETCDF_WRITE_MEDIA_TYPE),
        ("NetCDF", NETCDF_WRITE_MEDIA_TYPE),
        ("application/x-netcdf4", NETCDF_WRITE_MEDIA_TYPE),
        ("parquet", PARQUET_MEDIA_TYPE),
        ("application/parquet", PARQUET_MEDIA_TYPE),
        (PARQUET_MEDIA_TYPE, PARQUET_MEDIA_TYPE),
    ],
)
def test_normalize_output_format_accepts_aliases(alias: str, canonical: str) -> None:
    assert normalize_output_format(alias) == canonical


def test_normalize_output_format_rejects_unknown() -> None:
    with pytest.raises(UnsupportedMediaType):
        normalize_output_format("geojson")
