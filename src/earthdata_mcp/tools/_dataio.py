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

import io
import os
import tempfile
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr

#: Media types this server can open and serialize in-process (§4.4 format-by-shape).
ZARR_MEDIA_TYPE = "application/zarr"
PARQUET_MEDIA_TYPE = "application/x-parquet"

#: netCDF-4 media types we can *read* (Harmony subset / OPeNDAP DAP4 outputs). Several
#: spellings reach us: harmony-py guesses from the filename, OPeNDAP sets x-netcdf.
NETCDF_MEDIA_TYPES = frozenset(
    {"application/netcdf4", "application/x-netcdf", "application/x-netcdf4"}
)

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
    if media_type in NETCDF_MEDIA_TYPES:
        return _open_netcdf_flattened(data)
    if media_type == NETCDF_BUNDLE_MEDIA_TYPE:
        return _open_netcdf_bundle(data)
    raise UnsupportedMediaType(
        f"cannot open media type {media_type!r} in-process (supported: "
        f"{ZARR_MEDIA_TYPE!r}, {PARQUET_MEDIA_TYPE!r}, "
        f"{NETCDF_BUNDLE_MEDIA_TYPE!r}, and netCDF-4)"
    )


def serialize_result(obj: xr.Dataset | pa.Table, media_type: str) -> tuple[str, bytes]:
    """Serialize a derived dataset/table to ``(filename, bytes)`` for the backend.

    A gridded :class:`xarray.Dataset` becomes a zipped Zarr store; a table (or a
    dataset flattened to one) becomes Parquet. Raises :class:`UnsupportedMediaType`
    for any other target.
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
    raise UnsupportedMediaType(f"cannot serialize media type {media_type!r}")


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


def _open_netcdf_bundle(data: bytes) -> xr.Dataset:
    """Open a zip of netCDF granule subsets and concat them on the time axis.

    Each member is flattened by :func:`_open_netcdf_flattened`; members are then
    concatenated on ``time`` in filename order (granule names sort chronologically).
    Coordinate ``units``/``calendar`` attrs are stripped before concat so granules
    written at different times don't trip xarray's attribute-equality check — the
    same append-safety guard TTA applies in ``zarr_normalization``.
    """
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = sorted(n for n in zf.namelist() if not n.endswith("/"))
        members = [_open_netcdf_flattened(zf.read(n)) for n in names]
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
