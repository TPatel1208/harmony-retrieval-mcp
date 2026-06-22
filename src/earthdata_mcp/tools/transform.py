"""Transform tools (PLAN.md §6 Phase 7.2).

Five in-process transforms over a **materialized** source (``obs_``/``cube_``):
``subset``, ``reproject``, ``resample``, ``convert_format``, ``align``. Each one
resolves its source within the workspace, loads it through the same
``StorageBackend`` route the Phase 6 worker writes to (``tools/_dataio``), applies
the operation with xarray/pyarrow, writes the result back through the backend using
the §4.4 format-by-shape policy, mints a ``cube_`` handle, and — the load-bearing
contract — **records a provenance edge keyed to the durable transform spec**. No
transform is silent: every one appends to the DAG (CLAUDE.md hard rule).

The transform spec is re-materializable (operation + source handle + params), never
a URL or staged path; the ``storage_key`` lives on the ``cube_`` handle payload, not
in the provenance spec — mirroring how retrieval keeps the worker-written key off
the lineage record. ``ProvenanceStore`` rejects a URL-valued spec at write time.

Fidelity note: ``reproject`` records the requested CRS as a dataset attribute (and,
if ``rioxarray`` is importable, writes a real CRS too); a true raster warp is the
Harmony reproject *service*'s job at retrieval time. Recording the CRS transform and
its provenance edge is the contract here, and the attribute is written whether or not
rioxarray is present, so the result is never a vacuous no-op.
"""

from __future__ import annotations

import hashlib
import json

import pyarrow as pa
import xarray as xr

from earthdata_mcp.storage import StorageBackend, get_storage_backend
from earthdata_mcp.tools._dataio import (
    PARQUET_MEDIA_TYPE,
    ZARR_MEDIA_TYPE,
    open_result,
    serialize_result,
)
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.tools.retrieval import _default_provenance
from earthdata_mcp.workspace.models import HandleType, handle_type_of
from earthdata_mcp.workspace.provenance import ProvenanceStore
from earthdata_mcp.workspace.store import WorkspaceStore

#: §4.4 format-by-shape (mirror of ``tools/retrieval._FORMAT_BY_SHAPE``). A derived
#: gridded cube defaults to Zarr, a tabular one to Parquet.
_FORMAT_BY_SHAPE = {
    "grid": ZARR_MEDIA_TYPE,
    "point": PARQUET_MEDIA_TYPE,
}

_LON_NAMES = ("lon", "longitude", "x")
_LAT_NAMES = ("lat", "latitude", "y")

_storage: StorageBackend | None = None


def _default_storage() -> StorageBackend:
    """Process-wide storage backend, built lazily so import has no side effects."""
    global _storage
    if _storage is None:
        _storage = get_storage_backend()
    return _storage


# -- the five tools --------------------------------------------------------


async def subset(
    source_handle: str,
    aoi_handle: str | None = None,
    variables: list[str] | None = None,
    time_range: str | None = None,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    provenance: ProvenanceStore | None = None,
    storage: StorageBackend | None = None,
) -> dict:
    """Spatial/variable/temporal subset of a materialized result → a ``cube_``."""
    store, provenance, storage = _resolve_deps(store, provenance, storage)
    obj, _payload = await _load_source(source_handle, workspace_id, store, storage)

    bbox = await _aoi_bbox(aoi_handle, workspace_id, store)
    result = _apply_subset(obj, bbox, variables, time_range)

    spec = {
        "operation": "subset",
        "source_handle": source_handle,
        "aoi_handle": aoi_handle,
        "aoi_bbox": list(bbox) if bbox is not None else None,
        "variables": list(variables or []),
        "time_range": time_range,
    }
    return await _emit_cube(
        result, [source_handle], spec, workspace_id, store, provenance, storage
    )


async def reproject(
    source_handle: str,
    crs: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    provenance: ProvenanceStore | None = None,
    storage: StorageBackend | None = None,
) -> dict:
    """Tag a gridded result with a target CRS → a ``cube_``.

    The requested CRS is always written as a dataset attribute (and per-variable
    ``spatial_ref``), so the output records the transform regardless of whether
    ``rioxarray`` is available to write a formal CRS.
    """
    store, provenance, storage = _resolve_deps(store, provenance, storage)
    obj, _payload = await _load_source(source_handle, workspace_id, store, storage)
    if not isinstance(obj, xr.Dataset):
        raise ValueError("reproject requires a gridded (Zarr) source")

    result = obj.copy()
    try:  # Best effort: a real CRS if rioxarray is installed.
        import rioxarray  # noqa: F401

        result = result.rio.write_crs(crs)
    except Exception:  # noqa: BLE001 — rioxarray absent or input not rio-shaped.
        pass
    # Always record the CRS as an attribute, present with or without rioxarray.
    result.attrs["crs"] = crs
    for var in result.data_vars:
        result[var].attrs["spatial_ref"] = crs

    spec = {"operation": "reproject", "source_handle": source_handle, "crs": crs}
    out = await _emit_cube(
        result, [source_handle], spec, workspace_id, store, provenance, storage
    )
    out["crs"] = crs
    return out


async def resample(
    source_handle: str,
    time_freq: str | None = None,
    spatial_factor: int | None = None,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    provenance: ProvenanceStore | None = None,
    storage: StorageBackend | None = None,
) -> dict:
    """Temporal (``time_freq``, e.g. ``"1D"``) and/or spatial (integer ``spatial_factor``
    coarsening) resampling of a gridded result → a ``cube_``."""
    store, provenance, storage = _resolve_deps(store, provenance, storage)
    obj, _payload = await _load_source(source_handle, workspace_id, store, storage)
    if not isinstance(obj, xr.Dataset):
        raise ValueError("resample requires a gridded (Zarr) source")
    if not time_freq and not spatial_factor:
        raise ValueError("resample needs a time_freq and/or a spatial_factor")

    result = obj
    if time_freq and "time" in result.dims:
        result = result.resample(time=time_freq).mean()
    if spatial_factor:
        dims = {
            name: spatial_factor
            for name in (_lon_name(result), _lat_name(result))
            if name is not None
        }
        if dims:
            result = result.coarsen(boundary="trim", **dims).mean()

    spec = {
        "operation": "resample",
        "source_handle": source_handle,
        "time_freq": time_freq,
        "spatial_factor": spatial_factor,
    }
    return await _emit_cube(
        result, [source_handle], spec, workspace_id, store, provenance, storage
    )


async def convert_format(
    source_handle: str,
    output_format: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    provenance: ProvenanceStore | None = None,
    storage: StorageBackend | None = None,
) -> dict:
    """Re-serialize a materialized result to a different media type → a ``cube_``.

    Unlike the other transforms, the output format is the caller's explicit choice
    (not format-by-shape); it still travels the one storage route.
    """
    store, provenance, storage = _resolve_deps(store, provenance, storage)
    obj, _payload = await _load_source(source_handle, workspace_id, store, storage)

    spec = {
        "operation": "convert_format",
        "source_handle": source_handle,
        "output_format": output_format,
    }
    return await _emit_cube(
        obj,
        [source_handle],
        spec,
        workspace_id,
        store,
        provenance,
        storage,
        output_format=output_format,
    )


async def align(
    source_handles: list[str],
    method: str = "outer",
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    provenance: ProvenanceStore | None = None,
    storage: StorageBackend | None = None,
) -> dict:
    """Align ≥2 gridded results to a common grid → a ``cube_`` + an alignment report.

    Records **one provenance edge per source** (so every input is in the lineage),
    and returns an ``alignment_report`` describing the common grid and the shape
    change each input underwent.
    """
    if not source_handles or len(source_handles) < 2:
        raise ValueError("align needs at least two source handles")
    store, provenance, storage = _resolve_deps(store, provenance, storage)

    datasets = []
    for handle in source_handles:
        obj, _payload = await _load_source(handle, workspace_id, store, storage)
        if not isinstance(obj, xr.Dataset):
            raise ValueError("align requires gridded (Zarr) sources")
        datasets.append(obj)

    shapes_before = [{str(k): int(v) for k, v in d.sizes.items()} for d in datasets]
    aligned = xr.align(*datasets, join=method)
    combined = xr.merge(aligned, compat="override")
    report = {
        "n_inputs": len(datasets),
        "method": method,
        "common_dims": {str(k): int(v) for k, v in combined.sizes.items()},
        "shapes_before": shapes_before,
        "shape_after": {str(k): int(v) for k, v in combined.sizes.items()},
    }

    spec = {
        "operation": "align",
        "source_handles": list(source_handles),
        "method": method,
        "alignment_report": report,
    }
    out = await _emit_cube(
        combined, list(source_handles), spec, workspace_id, store, provenance, storage
    )
    out["alignment_report"] = report
    return out


# -- shared core -----------------------------------------------------------


def _resolve_deps(
    store: WorkspaceStore | None,
    provenance: ProvenanceStore | None,
    storage: StorageBackend | None,
) -> tuple[WorkspaceStore, ProvenanceStore, StorageBackend]:
    """Fill in the lazily-built process defaults for any dependency not injected."""
    return (
        store or _default_store(),
        provenance or _default_provenance(),
        storage or _default_storage(),
    )


async def _load_source(
    handle: str,
    workspace_id: str,
    store: WorkspaceStore,
    storage: StorageBackend,
) -> tuple[xr.Dataset | pa.Table, dict]:
    """Resolve a materialized ``obs_``/``cube_`` handle and open its data blob.

    Type-checks the prefix, resolves within the workspace (cross-workspace access
    raises), and requires the handle to be a ``ready`` result with a ``storage_key``.
    """
    htype = handle_type_of(handle)
    if htype not in (HandleType.OBS, HandleType.CUBE):
        raise ValueError(f"expected an obs_ or cube_ handle, got {handle!r}")
    record = await store.get_handle(workspace_id, handle)  # isolation gate
    payload = record.payload
    storage_key = payload.get("storage_key")
    media_type = payload.get("media_type")
    if payload.get("status") != "ready" or not storage_key or not media_type:
        raise ValueError(f"source handle {handle!r} is not a materialized result")
    data = await storage.get(storage_key)
    return open_result(data, media_type), payload


async def _emit_cube(
    result_obj: xr.Dataset | pa.Table,
    source_handles: list[str],
    transform_spec: dict,
    workspace_id: str,
    store: WorkspaceStore,
    provenance: ProvenanceStore,
    storage: StorageBackend,
    output_format: str | None = None,
) -> dict:
    """Serialize → store → mint a ``cube_`` → record a provenance edge per source.

    The output media type is the caller's ``output_format`` (``convert_format``) or,
    by default, the §4.4 format for the result's shape. The provenance ``request_spec``
    carries the durable transform recipe and never the ``storage_key`` (which is a
    backend key, kept on the handle payload).
    """
    media_type = output_format or _FORMAT_BY_SHAPE[_shape_of(result_obj)]
    name, data = serialize_result(result_obj, media_type)

    # The cache key is a content-addressed fingerprint of the spec + sources (§4.4),
    # so it keys the storage path without needing the not-yet-minted handle — letting
    # us mint the cube with its full payload in one write.
    cache_key = _cache_key(transform_spec, source_handles)
    storage_key = f"transform/{cache_key}/{name}"
    await storage.put(storage_key, data)

    cube_handle = await store.put_handle(
        workspace_id,
        HandleType.CUBE,
        payload={
            "status": "ready",
            "storage_key": storage_key,
            "media_type": media_type,
            "transform": transform_spec,
            "source_handles": list(source_handles),
            "cache_key": cache_key,
        },
    )

    # Durable, re-materializable spec — no storage_key, no URL (provenance rejects one).
    edge_spec = {
        **transform_spec,
        "workspace_id": workspace_id,
        "cube_handle": cube_handle,
        "output_format": media_type,
        "cache_key": cache_key,
    }
    for source_handle in source_handles:
        await provenance.record_edge(
            workspace_id,
            target_handle=cube_handle,
            source_handle=source_handle,
            request_spec=edge_spec,
        )

    return {
        "handle": cube_handle,
        "status": "ready",
        "output_format": media_type,
        "operation": transform_spec["operation"],
    }


def _shape_of(obj: xr.Dataset | pa.Table) -> str:
    """Result shape for format-by-shape: a Dataset is gridded, a Table is tabular."""
    return "grid" if isinstance(obj, xr.Dataset) else "point"


def _cache_key(transform_spec: dict, source_handles: list[str]) -> str:
    """A 24-char fingerprint of the operation + sources + params (cf. retrieval §4.4)."""
    raw = json.dumps(
        {"sources": list(source_handles), "spec": transform_spec},
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


async def _aoi_bbox(
    aoi_handle: str | None, workspace_id: str, store: WorkspaceStore
) -> tuple[float, float, float, float] | None:
    """Resolve an optional ``aoi_`` handle to a bbox (W,S,E,N)."""
    if aoi_handle is None:
        return None
    if handle_type_of(aoi_handle) is not HandleType.AOI:
        raise ValueError(f"expected an aoi_ handle, got {aoi_handle!r}")
    record = await store.get_handle(workspace_id, aoi_handle)
    bbox = record.payload.get("bbox")
    if not bbox or len(bbox) != 4:
        raise ValueError(f"aoi handle {aoi_handle!r} payload missing 'bbox'")
    return tuple(float(c) for c in bbox)


def _apply_subset(
    obj: xr.Dataset | pa.Table,
    bbox: tuple[float, float, float, float] | None,
    variables: list[str] | None,
    time_range: str | None,
) -> xr.Dataset | pa.Table:
    """Apply variable/spatial/temporal selection to a dataset or table."""
    if isinstance(obj, pa.Table):
        # Tabular: column (variable) selection only — bbox/time filtering on a flat
        # table would need known lat/lon/time columns, which we don't assume here.
        return obj.select(variables) if variables else obj

    result = obj
    if variables:
        result = result[variables]
    if bbox is not None:
        result = _bbox_sel(result, bbox)
    if time_range and "time" in result.dims:
        start, _, end = time_range.partition("/")
        result = result.sel(time=slice(start or None, end or None))
    return result


def _bbox_sel(ds: xr.Dataset, bbox: tuple[float, float, float, float]) -> xr.Dataset:
    """Select a W,S,E,N box, honoring a descending latitude axis."""
    west, south, east, north = bbox
    lon, lat = _lon_name(ds), _lat_name(ds)
    if lon is not None:
        ds = ds.sel({lon: slice(west, east)})
    if lat is not None:
        values = ds[lat].values
        if values.size > 1 and values[0] > values[-1]:
            ds = ds.sel({lat: slice(north, south)})
        else:
            ds = ds.sel({lat: slice(south, north)})
    return ds


def _lon_name(ds: xr.Dataset) -> str | None:
    return next((n for n in _LON_NAMES if n in ds.coords or n in ds.dims), None)


def _lat_name(ds: xr.Dataset) -> str | None:
    return next((n for n in _LAT_NAMES if n in ds.coords or n in ds.dims), None)
