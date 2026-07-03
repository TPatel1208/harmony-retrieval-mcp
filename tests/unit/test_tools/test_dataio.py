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
    NATIVE_ARCHIVE_MEDIA_TYPE,
    NETCDF_BUNDLE_MEDIA_TYPE,
    NETCDF_WRITE_MEDIA_TYPE,
    PARQUET_MEDIA_TYPE,
    UnsupportedMediaType,
    HDF4_MEDIA_TYPE,
    normalize_output_format,
    open_result,
    open_result_lazy,
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


def _grouped_bundle_zip_path(tmp_path, filenames_and_bytes: list[tuple[str, bytes]]):
    """Write a netCDF-bundle zip (of grouped members) to a real path for open_result_lazy."""
    path = tmp_path / "bundle.nc.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in filenames_and_bytes:
            zf.writestr(name, data)
    return path


def test_open_result_lazy_flattens_groups_in_bundle_single_member(tmp_path) -> None:
    """Regression: inspect_statistics returned ``{}`` on a materialized obs_ handle
    whose netCDF-bundle member was a grouped (TEMPO/OMI-shaped) file, because
    ``open_result_lazy``'s bundle branch opened each member's root group directly
    (no data vars live there) instead of flattening groups like the eager
    ``open_result``/``_open_netcdf_bundle`` path does."""
    path = _grouped_bundle_zip_path(
        tmp_path, [("only.nc", _grouped_netcdf_bytes())]
    )

    with open_result_lazy(path, NETCDF_BUNDLE_MEDIA_TYPE) as ds:
        assert "product__vertical_column" in ds.data_vars
        assert "geolocation__latitude" in ds.data_vars
        assert ds["product__vertical_column"].values.tolist() == [1.0, 2.0]


def test_open_result_lazy_flattens_groups_in_bundle_multi_member(tmp_path) -> None:
    path = _grouped_bundle_zip_path(
        tmp_path,
        [("g0.nc", _grouped_netcdf_bytes()), ("g1.nc", _grouped_netcdf_bytes())],
    )

    with open_result_lazy(path, NETCDF_BUNDLE_MEDIA_TYPE) as ds:
        assert "product__vertical_column" in ds.data_vars
        assert ds.sizes["time"] == 4  # two members x two time steps each, concatenated


def _cf_time_netcdf_bytes(
    time_value: int,
    x_value: float,
    units: str = "seconds since 2000-01-01T00:00:00Z",
    calendar: str = "standard",
) -> bytes:
    """A flat single-time granule with a CF-encoded float64 ``time`` coordinate.

    Defaults to ``seconds since 2000-01-01T00:00:00Z`` / ``standard`` to mimic the
    encoding TEMPO/OMI L3 products write. ``units``/``calendar`` are overridable so
    a test can give different members different reference epochs (as MERRA-2 daily
    granules do) or a non-standard CF calendar (julian, 360_day, noleap).
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
                    {"units": units, "calendar": calendar},
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


def test_open_netcdf_bundle_decodes_julian_calendar_to_cftime() -> None:
    """PRD 001: a julian-calendar bundle (e.g. GPM IMERG) must open, not crash.

    Today ``_decode_member_time`` hardcodes ``use_cftime=False``, which forces
    pandas datetime64 decoding — pandas raises ``OutOfBoundsDatetime`` (or, for
    small offsets, silently mis-decodes) for any non-standard calendar. The
    shared decode policy must route a ``julian`` calendar through
    ``use_cftime=True`` instead, yielding a correctly-valued cftime index.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "g0.nc",
            _cf_time_netcdf_bytes(
                0, 10.0, units="days since 2000-01-01T00:00:00", calendar="julian"
            ),
        )
        zf.writestr(
            "g1.nc",
            _cf_time_netcdf_bytes(
                1, 20.0, units="days since 2000-01-01T00:00:00", calendar="julian"
            ),
        )

    ds = open_result(buf.getvalue(), NETCDF_BUNDLE_MEDIA_TYPE)

    assert ds.sizes["time"] == 2
    import cftime

    assert all(isinstance(t, cftime.datetime) for t in ds["time"].values)
    assert [t.calendar for t in ds["time"].values] == ["julian", "julian"]


def test_open_netcdf_bundle_decodes_360_day_calendar_including_feb_30() -> None:
    """A 360_day calendar (climate-model convention: every month has 30 days) must
    decode correctly, including dates like Feb 30 that don't exist in any real
    calendar — the defining correctness check for this calendar."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "g0.nc",
            _cf_time_netcdf_bytes(
                # day 0 = Jan 1; day 59 (Jan's 30 days + Feb's next 29) in a
                # 360_day calendar lands on Feb 30 — a date only this calendar has.
                59, 10.0, units="days since 2000-01-01T00:00:00", calendar="360_day",
            ),
        )
        zf.writestr(
            "g1.nc",
            _cf_time_netcdf_bytes(
                60, 20.0, units="days since 2000-01-01T00:00:00", calendar="360_day",
            ),
        )

    ds = open_result(buf.getvalue(), NETCDF_BUNDLE_MEDIA_TYPE)

    assert ds.sizes["time"] == 2
    import cftime

    times = list(ds["time"].values)
    assert all(isinstance(t, cftime.Datetime360Day) for t in times)
    assert (times[0].month, times[0].day) == (2, 30)
    assert (times[1].month, times[1].day) == (3, 1)


def test_open_netcdf_bundle_decodes_noleap_calendar() -> None:
    """A noleap/365_day calendar (no Feb 29 ever) must decode correctly."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "g0.nc",
            _cf_time_netcdf_bytes(
                0, 10.0, units="days since 2001-01-01T00:00:00", calendar="noleap",
            ),
        )
        zf.writestr(
            "g1.nc",
            _cf_time_netcdf_bytes(
                365, 20.0, units="days since 2001-01-01T00:00:00", calendar="noleap",
            ),
        )

    ds = open_result(buf.getvalue(), NETCDF_BUNDLE_MEDIA_TYPE)

    assert ds.sizes["time"] == 2
    import cftime

    times = list(ds["time"].values)
    assert all(isinstance(t, cftime.DatetimeNoLeap) for t in times)
    # 365 days after 2001-01-01 with no leap day is exactly 2002-01-01.
    assert (times[1].year, times[1].month, times[1].day) == (2002, 1, 1)


def test_open_netcdf_bundle_standard_calendar_still_yields_datetime64() -> None:
    """Regression guard: the fast path (standard/absent calendar) must not
    change — the 95% case still decodes to real ``datetime64[ns]``, not cftime."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("g0.nc", _cf_time_netcdf_bytes(0, 10.0))
        zf.writestr("g1.nc", _cf_time_netcdf_bytes(86400, 20.0))

    ds = open_result(buf.getvalue(), NETCDF_BUNDLE_MEDIA_TYPE)

    assert np.issubdtype(ds["time"].dtype, np.datetime64)


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


# -- native-format honesty (PRD 008) ------------------------------------------


def test_open_result_native_hdf4_raises_actionable_error() -> None:
    """A materialized result honestly labeled x-hdf4 (PRD 008) must name every
    alternative a researcher has, not just say "cannot open"."""
    data = b"\x0e\x03\x13\x01" + b"native-hdf4-bytes"
    with pytest.raises(UnsupportedMediaType) as exc_info:
        open_result(data, HDF4_MEDIA_TYPE)
    message = str(exc_info.value)
    assert "HDF4" in message
    assert "AppEEARS" in message
    assert "output_format" in message
    assert "download" in message.lower()


def test_open_result_native_archive_bundle_raises_actionable_error() -> None:
    data = b"PK\x03\x04fake-zip-bytes"
    with pytest.raises(UnsupportedMediaType) as exc_info:
        open_result(data, NATIVE_ARCHIVE_MEDIA_TYPE)
    message = str(exc_info.value)
    assert "AppEEARS" in message
    assert "output_format" in message
    assert "download" in message.lower()


def test_open_result_lazy_native_hdf4_raises_actionable_error(tmp_path) -> None:
    """inspect_statistics reads through open_result_lazy (the disk-path route),
    so the same actionable message must fire there too (PRD 008 story 4)."""
    path = tmp_path / "granule.hdf"
    path.write_bytes(b"\x0e\x03\x13\x01" + b"native-hdf4-bytes")
    with pytest.raises(UnsupportedMediaType) as exc_info:
        with open_result_lazy(path, HDF4_MEDIA_TYPE):
            pass
    message = str(exc_info.value)
    assert "AppEEARS" in message
    assert "output_format" in message


def test_open_result_mislabeled_hdf4_raises_actionable_error() -> None:
    """PRD 008 story 8: a legacy row materialized *before* this fix shipped can
    still carry a netCDF media type on HDF4 bytes. The magic-byte defense must
    catch the mismatch and raise the same actionable message, not let h5netcdf
    fail with a raw parser error."""
    data = b"\x0e\x03\x13\x01" + b"actually-hdf4-bytes"
    with pytest.raises(UnsupportedMediaType) as exc_info:
        open_result(data, "application/netcdf4")
    message = str(exc_info.value)
    assert "AppEEARS" in message
    assert "before content sniffing existed" in message
