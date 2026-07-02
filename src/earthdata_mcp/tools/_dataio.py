"""Bytes ⇄ dataset I/O for the preview and transform tools (PLAN.md §4.4).

A single owner of the "open a materialized result / serialize a derived result"
mapping so preview and transform share **one** storage route (CLAUDE.md: don't
introduce a second materialization path). It speaks the same format-by-shape
vocabulary the Phase 6 retrieval engine uses:

* gridded → a **zipped Zarr store** carried as one opaque blob. Zarr is natively a
  directory of many small objects; zipping it keeps a cube as a single
  ``StorageBackend`` key, matching how the Phase 6 worker stores one object per
  result. We write to a temp directory and zip the tree (rather than zarr's
  append-only ``ZipStore``, which cannot overwrite the group attrs xarray writes).
* tabular → **Parquet**.

It also **reads** (never writes) netCDF-4: Harmony subsetters and OPeNDAP DAP4 both
deliver netCDF, and grid retrieval now stores it as-is (few Harmony services produce
Zarr natively). Grouped products (TEMPO/OMI keep science vars under ``product/…`` and
``geolocation/…``) are flattened into one dataset via ``xr.open_groups`` so the
existing preview/transform tools consume them unchanged. A multi-granule retrieval is
carried as a **zip bundle of netCDF members** and concatenated on read.

Only the guaranteed stack is used (``xarray`` + ``zarr`` + ``h5netcdf``, ``pyarrow`` +
``pandas``); an unopenable media type fails loudly rather than silently guessing.
"""

from __future__ import annotations

import contextlib
import io
import os
import tempfile
import zipfile
from pathlib import Path

import numpy as np
import zarr
import zarr.storage

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import xarray as xr

#: Media types this server can open and serialize in-process (§4.4 format-by-shape).
ZARR_MEDIA_TYPE = "application/zarr"
PARQUET_MEDIA_TYPE = "application/x-parquet"
CSV_MEDIA_TYPE = "text/csv"

#: netCDF-4 media types we can *read* (Harmony subset / OPeNDAP DAP4 outputs). Several
#: spellings reach us: harmony-py guesses from the filename, OPeNDAP sets x-netcdf.
NETCDF_MEDIA_TYPES = frozenset(
    {"application/netcdf4", "application/x-netcdf", "application/x-netcdf4"}
)

#: Canonical media type this server *writes* netCDF as (``convert_format`` target).
#: One spelling out of the several ``NETCDF_MEDIA_TYPES`` accepts on read.
NETCDF_WRITE_MEDIA_TYPE = "application/x-netcdf"

#: ``convert_format``'s caller-facing vocabulary: bare names and common media-type
#: spellings, normalized to the one canonical constant ``serialize_result``/``open_result``
#: dispatch on. Case-insensitive lookup — see :func:`normalize_output_format`.
_FORMAT_ALIASES = {
    "zarr": ZARR_MEDIA_TYPE,
    ZARR_MEDIA_TYPE: ZARR_MEDIA_TYPE,
    "parquet": PARQUET_MEDIA_TYPE,
    PARQUET_MEDIA_TYPE: PARQUET_MEDIA_TYPE,
    "application/parquet": PARQUET_MEDIA_TYPE,
    "csv": CSV_MEDIA_TYPE,
    CSV_MEDIA_TYPE: CSV_MEDIA_TYPE,
    "netcdf": NETCDF_WRITE_MEDIA_TYPE,
    "netcdf4": NETCDF_WRITE_MEDIA_TYPE,
    **{t: NETCDF_WRITE_MEDIA_TYPE for t in NETCDF_MEDIA_TYPES},
}


def normalize_output_format(output_format: str) -> str:
    """Map a caller-supplied format name/media-type (case-insensitive) to the
    canonical media type ``serialize_result``/``open_result`` understand.

    Raises :class:`UnsupportedMediaType` for anything not in ``_FORMAT_ALIASES``,
    same as an unrecognised media type reaching ``serialize_result`` directly.
    """
    canonical = _FORMAT_ALIASES.get(output_format.strip().lower())
    if canonical is None:
        raise UnsupportedMediaType(
            f"cannot serialize media type {output_format!r} (supported: "
            f"{sorted(set(_FORMAT_ALIASES))})"
        )
    return canonical

#: A multi-granule retrieval: a zip whose members are netCDF granule subsets, opened
#: and concatenated on the time axis. Produced by ``OPeNDAPProvider`` (Part 3).
NETCDF_BUNDLE_MEDIA_TYPE = "application/netcdf-bundle+zip"

#: Dimension concatenated across bundle members. TEMPO/OMI L3 carry a length-1 ``time``
#: per granule; stacking on it reconstructs the time series for the window.
_BUNDLE_CONCAT_DIM = "time"

#: netCDF read engine — pure-Python h5py-based, no C library (see pyproject).
_NETCDF_ENGINE = "h5netcdf"


class UnsupportedMediaType(ValueError):
    """A media type this server cannot open or serialize in-process."""


def open_result(data: bytes, media_type: str) -> xr.Dataset | pa.Table:
    """Open a materialized result blob into an ``xr.Dataset`` or ``pa.Table``.

    Raises :class:`UnsupportedMediaType` for anything but zipped-Zarr or Parquet —
    we never guess an engine we don't have.
    """
    if media_type == ZARR_MEDIA_TYPE:
        return _open_zarr_zip(data)
    if media_type == PARQUET_MEDIA_TYPE:
        return pq.read_table(io.BytesIO(data))
    if media_type == CSV_MEDIA_TYPE:
        return pa_csv.read_csv(io.BytesIO(data))
    if media_type in NETCDF_MEDIA_TYPES:
        return _open_netcdf_flattened(data)
    if media_type == NETCDF_BUNDLE_MEDIA_TYPE:
        return _open_netcdf_bundle(data)
    raise UnsupportedMediaType(
        f"cannot open media type {media_type!r} in-process (supported: "
        f"{ZARR_MEDIA_TYPE!r}, {PARQUET_MEDIA_TYPE!r}, {CSV_MEDIA_TYPE!r}, "
        f"{NETCDF_BUNDLE_MEDIA_TYPE!r}, and netCDF-4)"
    )


@contextlib.contextmanager
def open_result_lazy(
    path: Path, media_type: str, variables: list[str] | None = None
):
    """Open a stored result lazily (dask-backed) from a filesystem path.

    Context manager: keeps file handles and temp dirs alive while the caller
    computes statistics.  For Parquet, ``variables`` drives column projection at
    read time.  All other paths ignore ``variables`` — variable selection happens
    at the statistics layer on the already-lazy array.

    Raises :class:`UnsupportedMediaType` for unrecognised ``media_type``.
    """
    if media_type == ZARR_MEDIA_TYPE:
        # ZipStore in read mode: zarr reads individual chunk blobs on demand —
        # no full extraction to a temp dir.  The write path still avoids ZipStore
        # (it can't overwrite group attrs that xarray writes), but reading is fine.
        store = zarr.storage.ZipStore(str(path), mode="r")
        try:
            yield xr.open_zarr(store, chunks="auto", consolidated=False)
        finally:
            store.close()

    elif media_type in NETCDF_MEDIA_TYPES:
        # xr.open_groups propagates chunks= to each internal open_dataset call,
        # so both plain and grouped (TEMPO/OMI) netCDF come back dask-backed.
        groups = xr.open_groups(
            path, engine=_NETCDF_ENGINE, chunks="auto", decode_times=False
        )
        try:
            yield _merge_groups(groups)
        finally:
            for ds in groups.values():
                ds.close()

    elif media_type == NETCDF_BUNDLE_MEDIA_TYPE:
        # Extract to temp; each member is opened group-flattened (chunks="auto"
        # keeps it dask-backed) the same way the eager _open_netcdf_bundle path
        # does — a plain open_dataset/open_mfdataset here would read only each
        # member's root group and surface no data for grouped (TEMPO/OMI)
        # products, since their science variables live under /product,
        # /geolocation subgroups.
        with tempfile.TemporaryDirectory() as tmp:
            with zipfile.ZipFile(path) as zf:
                zf.extractall(tmp)
            member_paths = sorted(p for p in Path(tmp).iterdir() if p.is_file())
            if not member_paths:
                raise UnsupportedMediaType("netCDF bundle is empty")
            all_groups: list[dict[str, xr.Dataset]] = []
            try:
                members = []
                for member_path in member_paths:
                    groups = xr.open_groups(
                        member_path,
                        engine=_NETCDF_ENGINE,
                        chunks="auto",
                        decode_times=False,
                    )
                    all_groups.append(groups)
                    members.append(
                        _decode_member_time(
                            _synthesize_bundle_time_coord(_merge_groups(groups))
                        )
                    )
                if len(members) == 1:
                    yield members[0]
                else:
                    normalized = [_strip_unsafe_coord_attrs(ds) for ds in members]
                    yield xr.concat(normalized, dim=_BUNDLE_CONCAT_DIM)
            finally:
                for groups in all_groups:
                    for ds in groups.values():
                        ds.close()

    elif media_type == PARQUET_MEDIA_TYPE:
        yield pq.read_table(path, columns=variables)

    else:
        raise UnsupportedMediaType(f"cannot lazily open media type {media_type!r}")


def serialize_result(obj: xr.Dataset | pa.Table, media_type: str) -> tuple[str, bytes]:
    """Serialize a derived dataset/table to ``(filename, bytes)`` for the backend.

    A gridded :class:`xarray.Dataset` becomes a zipped Zarr store, netCDF-4, or (via
    flattening) Parquet/CSV; a :class:`pa.Table` becomes Parquet or CSV directly.
    CSV and netCDF are shape-restricted on purpose (see each branch below) rather than
    silently reshaping a table into a grid or exploding a grid into one giant row set.
    Raises :class:`UnsupportedMediaType` for any other target, or a shape mismatch.
    """
    if media_type == ZARR_MEDIA_TYPE:
        if not isinstance(obj, xr.Dataset):
            raise UnsupportedMediaType("zarr output requires an xarray.Dataset")
        return "cube.zarr.zip", _zarr_zip_bytes(obj)
    if media_type == PARQUET_MEDIA_TYPE:
        table = obj if isinstance(obj, pa.Table) else _dataset_to_table(obj)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        return "cube.parquet", buf.getvalue()
    if media_type == CSV_MEDIA_TYPE:
        if not isinstance(obj, pa.Table):
            raise UnsupportedMediaType(
                "convert_format: csv output requires a tabular (point) source; "
                "grid sources should convert to netcdf or parquet"
            )
        buf = io.BytesIO()
        pa_csv.write_csv(obj, buf)
        return "cube.csv", buf.getvalue()
    if media_type == NETCDF_WRITE_MEDIA_TYPE:
        if not isinstance(obj, xr.Dataset):
            raise UnsupportedMediaType(
                "convert_format: netcdf output requires a gridded source; "
                "point/tabular sources should convert to csv or parquet"
            )
        return "cube.nc", _netcdf_bytes(obj)
    raise UnsupportedMediaType(f"cannot serialize media type {media_type!r}")


def _netcdf_bytes(ds: xr.Dataset) -> bytes:
    """Write ``ds`` to netCDF-4 via h5netcdf (pure-Python, no C library) and read it back."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "cube.nc")
        ds.to_netcdf(path, engine=_NETCDF_ENGINE, format="NETCDF4")
        with open(path, "rb") as f:
            return f.read()


def _zarr_zip_bytes(ds: xr.Dataset) -> bytes:
    """Write ``ds`` to a temp Zarr directory, then zip the tree into bytes."""
    with tempfile.TemporaryDirectory() as tmp:
        store_dir = os.path.join(tmp, "store")
        ds.to_zarr(store_dir, mode="w", consolidated=False)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _dirs, files in os.walk(store_dir):
                for name in files:
                    path = os.path.join(root, name)
                    zf.write(path, os.path.relpath(path, store_dir))
        return buf.getvalue()


def _open_zarr_zip(data: bytes) -> xr.Dataset:
    """Inverse of :func:`_zarr_zip_bytes`: unzip to a temp dir and open eagerly."""
    with tempfile.TemporaryDirectory() as tmp:
        store_dir = os.path.join(tmp, "store")
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            zf.extractall(store_dir)
        # .load() pulls everything into memory before the temp dir is removed.
        return xr.open_zarr(store_dir, consolidated=False).load()


def _open_netcdf_flattened(data: bytes) -> xr.Dataset:
    """Open one netCDF-4 blob, flattening all groups into a single dataset.

    TEMPO/OMI L3 keep science variables in subgroups (``/product/vertical_column``,
    ``/geolocation/latitude``); a plain ``xr.open_dataset`` reads only the root group
    and surfaces no data. We open every group with ``xr.open_groups`` and merge them
    into one dataset, prefixing each non-root group's variables and non-dimension
    coordinates with the group path (``product__vertical_column``). The prefix both
    records provenance and prevents collisions (several groups define ``latitude``).
    """
    # h5netcdf opens an in-memory buffer directly, so no temp file (and no Windows
    # file-handle race on cleanup). Load eagerly, then close the group handles.
    groups = xr.open_groups(
        io.BytesIO(data), engine=_NETCDF_ENGINE, decode_times=False
    )
    try:
        return _merge_groups(groups).load()
    finally:
        for ds in groups.values():
            ds.close()


def _merge_groups(groups: dict[str, xr.Dataset]) -> xr.Dataset:
    """Merge an ``xr.open_groups`` mapping into one dataset with group-prefixed names.

    Root-group (``"/"``) names are kept verbatim; every other group contributes
    ``<group_path>__<name>`` for its data variables and non-dimension coordinates.
    Dimension coordinates are left unprefixed so shared axes still align on merge.
    """
    renamed: list[xr.Dataset] = []
    for group_path, ds in groups.items():
        prefix = group_path.strip("/").replace("/", "__")
        if not prefix:
            renamed.append(ds)
            continue
        mapping = {
            name: f"{prefix}__{name}"
            for name in (*ds.data_vars, *ds.coords)
            if name not in ds.dims  # keep dimension coords as shared axes
        }
        renamed.append(ds.rename(mapping))
    # compat="override" lets shared dimension coords (e.g. latitude/longitude grids
    # repeated per group) merge without an exact-equality check across groups.
    return xr.merge(renamed, compat="override", join="outer")


def _synthesize_bundle_time_coord(ds: xr.Dataset) -> xr.Dataset:
    """Give a granule a real, indexed ``time`` coordinate before bundle concat.

    Some daily L3 products (e.g. OMI_MINDS_NO2d) carry a differently-cased
    singleton time dimension (``Time``, not ``time``) with no coordinate variable
    at all — the granule's date lives only in the ``RangeBeginningDate``/
    ``RangeBeginningTime`` global attrs (standard CMR/UMM-G granule temporal
    metadata). Left alone, :func:`_open_netcdf_bundle`'s ``xr.concat(dim="time")``
    can't find that name in the member and fabricates a brand-new, unindexed
    stacking dimension instead of reusing it — which then fails ``xr.align``
    against any co-temporal dataset that *does* carry a real time index
    (``AlignmentError: ... note: an index is found along that dimension``).
    Renaming the singleton dim and attaching the granule-level date closes
    that gap. No-op when ``time`` already exists or the date attrs are absent.
    """
    if _BUNDLE_CONCAT_DIM in ds.dims:
        return ds
    candidates = [
        d for d in ds.dims
        if d.lower() == _BUNDLE_CONCAT_DIM and ds.sizes[d] == 1
    ]
    if not candidates:
        return ds
    date = ds.attrs.get("RangeBeginningDate")
    if not date:
        return ds
    time_str = f"{date}T{ds.attrs.get('RangeBeginningTime', '00:00:00').rstrip('Z')}"
    ds = ds.rename({candidates[0]: _BUNDLE_CONCAT_DIM})
    return ds.assign_coords({_BUNDLE_CONCAT_DIM: [np.datetime64(time_str)]})


def _decode_member_time(ds: xr.Dataset) -> xr.Dataset:
    """Decode one bundle member's own CF time units before cross-granule concat.

    Daily granules can each carry a *different* CF reference epoch — MERRA-2
    daily files encode ``time`` as "minutes since <that granule's date>
    00:00:00", so raw values like ``[0, 180, ..., 1260]`` repeat identically in
    every member. Concat has no way to reconcile that on raw encoded ints: the
    previous approach stripped each member's ``units``/``calendar`` (to dodge
    xarray's attribute-equality check across granules) and reattached only the
    *first* member's units to the whole concatenated array afterward — which
    silently reinterpreted every later member's day-relative offsets under the
    first member's epoch, collapsing 61 distinct days onto one. Decoding each
    member against its own units before concat sidesteps that: the values
    handed to ``xr.concat`` are already absolute and directly comparable.
    """
    if _BUNDLE_CONCAT_DIM not in ds.coords:
        return ds
    time_var = ds[_BUNDLE_CONCAT_DIM]
    if np.issubdtype(time_var.dtype, np.datetime64):
        return ds
    units = time_var.attrs.get("units")
    if not units:
        return ds
    decoded = xr.coding.times.decode_cf_datetime(
        time_var.values,
        units=units,
        calendar=time_var.attrs.get("calendar", "standard"),
        use_cftime=False,
    )
    return ds.assign_coords({_BUNDLE_CONCAT_DIM: (_BUNDLE_CONCAT_DIM, decoded)})


def _open_netcdf_bundle(data: bytes) -> xr.Dataset:
    """Open a zip of netCDF granule subsets and concat them on the time axis.

    Each member is flattened by :func:`_open_netcdf_flattened`, given a real
    ``time`` coordinate if it only has a bare singleton dim
    (:func:`_synthesize_bundle_time_coord`), and decoded to datetime64 against
    its own CF units (:func:`_decode_member_time`) — all before concat, since
    per-member epochs can differ. Members are then concatenated on ``time`` in
    filename order (granule names sort chronologically). Non-time coordinate
    ``units``/``calendar`` attrs are stripped before concat so granules written
    at different times don't trip xarray's attribute-equality check — the same
    append-safety guard TTA applies in ``zarr_normalization``.
    """
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = sorted(n for n in zf.namelist() if not n.endswith("/"))
        members = [
            _decode_member_time(
                _synthesize_bundle_time_coord(_open_netcdf_flattened(zf.read(n)))
            )
            for n in names
        ]
    if not members:
        raise UnsupportedMediaType("netCDF bundle is empty")
    if len(members) == 1:
        return members[0]
    normalized = [_strip_unsafe_coord_attrs(ds) for ds in members]
    return xr.concat(normalized, dim=_BUNDLE_CONCAT_DIM).load()


def _strip_unsafe_coord_attrs(ds: xr.Dataset) -> xr.Dataset:
    """Drop ``units``/``calendar`` from coords so cross-granule concat doesn't choke.

    Ported from TTA's ``_strip_zarr_unsafe_coord_attrs``
    (``Backend/preprocessing/zarr_normalization.py``): coordinate attribute drift
    across granules makes ``xr.concat`` raise on its equality check.
    """
    ds = ds.copy()
    for coord in ds.coords:
        for attr in ("units", "calendar"):
            ds[coord].attrs.pop(attr, None)
            ds[coord].encoding.pop(attr, None)
    return ds


def _dataset_to_table(obj: xr.Dataset | pa.Table) -> pa.Table:
    """Flatten a gridded dataset to a table (for grid → Parquet conversions)."""
    if isinstance(obj, pa.Table):
        return obj
    if isinstance(obj, xr.Dataset):
        return pa.Table.from_pandas(obj.to_dataframe().reset_index())
    raise UnsupportedMediaType("parquet output requires a table or xarray.Dataset")
