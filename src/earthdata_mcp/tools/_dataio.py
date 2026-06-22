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

Only the guaranteed stack is used (``xarray`` + ``zarr``, ``pyarrow`` + ``pandas``);
no netCDF engine is assumed, so an unopenable media type fails loudly rather than
silently guessing.
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
    raise UnsupportedMediaType(
        f"cannot open media type {media_type!r} in-process "
        f"(supported: {ZARR_MEDIA_TYPE!r}, {PARQUET_MEDIA_TYPE!r})"
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


def _dataset_to_table(obj: xr.Dataset | pa.Table) -> pa.Table:
    """Flatten a gridded dataset to a table (for grid → Parquet conversions)."""
    if isinstance(obj, pa.Table):
        return obj
    if isinstance(obj, xr.Dataset):
        return pa.Table.from_pandas(obj.to_dataframe().reset_index())
    raise UnsupportedMediaType("parquet output requires a table or xarray.Dataset")
