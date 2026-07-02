"""Preview and inspection tools (PLAN.md §6 Phase 7.1).

Three read-only ways to *look at* a dataset or a materialized result without
re-retrieving it. None of them derives a new artifact, so — unlike the transform
tools — **none records a provenance edge**:

* ``preview_dataset`` — a GIBS visual reference (a re-constructible WMS request,
  not a network call). Mints a ``preview_`` handle whose payload is the durable
  GIBS spec (layer/bbox/time), never the URL as its source of truth.
* ``summarize_dataset`` — a structural summary: collection metadata for a
  ``dataset_`` handle, or dims/variables (gridded) / rows/columns (tabular) for a
  materialized ``obs_``/``cube_`` handle.
* ``inspect_statistics`` — descriptive per-variable statistics (min/max/mean/std/
  count) over a materialized result. Descriptive only — not an analysis tool
  (PLAN.md: no correlation/trend/anomaly).

Materialized results are read back through the same ``StorageBackend`` route the
transform tools write to (``tools/_dataio.open_result``); no second storage path.
"""

from __future__ import annotations

import math
import urllib.parse

import dask
import numpy as np

import pyarrow as pa
import pyarrow.compute as pc
import xarray as xr

from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.storage import StorageBackend, get_storage_backend
from earthdata_mcp.tools._dataio import open_result, open_result_lazy
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.workspace.handles import resolve_aoi, resolve_materialized
from earthdata_mcp.workspace.models import HandleType, handle_type_of
from earthdata_mcp.workspace.store import WorkspaceStore

#: GIBS WMS endpoint (EPSG:4326, "best" layers). A documented, stable URL; we only
#: *construct* a request here — the agent (or a browser) fetches the image.
_GIBS_WMS = "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi"
_GLOBAL_BBOX = (-180.0, -90.0, 180.0, 90.0)

# Coordinate name candidates, longest-/most-specific first.
_LON_NAMES = ("lon", "longitude", "x")
_LAT_NAMES = ("lat", "latitude", "y")

_storage: StorageBackend | None = None

# In-process GIBS layer cache: concept_id → authoritative layer name.
# Concurrent coroutines may race to populate the same key but will write the
# same value — worst outcome is one extra CMR call, never a correctness issue.
_gibs_layer_cache: dict[str, str] = {}


def _default_storage() -> StorageBackend:
    """Process-wide storage backend, built lazily so import has no side effects."""
    global _storage
    if _storage is None:
        _storage = get_storage_backend()
    return _storage


# -- GIBS layer resolution -------------------------------------------------


def _select_gibs_layer(layers: list[dict]) -> str | None:
    """Pick the best GIBS layer from a collection's EDSC tag data array.

    Preference order (highest first):
    1. Non-polar layers (arctic/antarctic flags absent or False) — polar-projection
       layers render blank at EPSG:4326 global extents.
    2. Within that pool, non-night layers (name does not contain ``_Night``) —
       daytime composites are the canonical preview product for most sensors.
    3. First entry as a last resort when all candidates share the same traits.

    Returns ``None`` when ``layers`` is empty or no entry has a ``"layer"`` field.
    """
    candidates = [l for l in layers if l.get("layer")]
    if not candidates:
        return None
    non_polar = [l for l in candidates if not (l.get("arctic") or l.get("antarctic"))]
    pool = non_polar if non_polar else candidates
    day_pool = [l for l in pool if "_Night" not in l["layer"]]
    chosen = day_pool[0] if day_pool else pool[0]
    return chosen["layer"]


async def _discover_gibs_layer(
    concept_id: str, cmr: CMRProvider
) -> tuple[str | None, str | None]:
    """Authoritative GIBS layer lookup via CMR EDSC tags.

    Returns ``(layer_name, failure_reason)``. ``layer_name`` is ``None`` when not found.
    Populates ``_gibs_layer_cache`` on success to avoid repeat CMR calls.
    """
    if concept_id in _gibs_layer_cache:
        return _gibs_layer_cache[concept_id], None
    layers = await cmr.fetch_gibs_layers(concept_id)
    if not layers:
        return None, "no edsc.extra.serverless.gibs tag on CMR record"
    layer_name = _select_gibs_layer(layers)
    if not layer_name:
        return None, "edsc.extra.serverless.gibs tag has no 'layer' field"
    _gibs_layer_cache[concept_id] = layer_name
    return layer_name, None


# -- preview_dataset -------------------------------------------------------


async def preview_dataset(
    dataset_handle: str,
    time_range: str | None = None,
    aoi_handle: str | None = None,
    layer: str | None = None,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    cmr: CMRProvider | None = None,
) -> dict:
    """Build a GIBS visual preview reference for a dataset and mint a ``preview_`` handle.

    Resolves the ``dataset_`` handle (and an optional ``aoi_`` for the bbox) within
    the workspace, then constructs a GIBS WMS ``GetMap`` request. No network call is
    made — the returned ``gibs_url`` is for the agent to fetch. The minted handle's
    payload stores the *re-constructible* GIBS spec (layer/bbox/time), not the URL.
    """
    if handle_type_of(dataset_handle) is not HandleType.DATASET:
        raise ValueError(
            f"preview_dataset expects a dataset_ handle, got {dataset_handle!r}"
        )
    store = store or _default_store()

    record = await store.get_handle(workspace_id, dataset_handle)  # isolation gate
    collection = record.payload.get("collection", {})
    short_name = collection.get("short_name") or record.payload.get("short_name")

    # Layer resolution priority: explicit param → handle payload cache →
    # authoritative CMR EDSC tag lookup → short_name heuristic (last resort).
    lookup_source: str
    lookup_failure_reason: str | None = None

    if layer is not None:
        resolved_layer = layer
        lookup_source = "explicit"
    elif "gibs_layer" in record.payload:
        resolved_layer = record.payload["gibs_layer"]
        lookup_source = "handle_payload"
    else:
        concept_id = record.payload.get("concept_id")
        if concept_id:
            _cmr = cmr or CMRProvider()
            cmr_layer, failure = await _discover_gibs_layer(concept_id, _cmr)
            if cmr_layer:
                resolved_layer = cmr_layer
                lookup_source = "cmr_tags"
                # Write-through: persist so future calls skip the CMR round-trip.
                await store.update_handle(
                    workspace_id, dataset_handle, {"gibs_layer": cmr_layer}
                )
            else:
                resolved_layer = short_name
                lookup_source = "short_name_guess"
                lookup_failure_reason = failure
        else:
            resolved_layer = short_name
            lookup_source = "short_name_guess"
            lookup_failure_reason = "handle has no concept_id"

    layer_is_guess = lookup_source == "short_name_guess"
    if not resolved_layer:
        raise ValueError(
            f"dataset handle {dataset_handle!r} has no GIBS layer — tried "
            f"{lookup_source}: {lookup_failure_reason}"
        )

    bbox = _preview_bbox(collection, await _aoi_bbox(aoi_handle, workspace_id, store))
    time = _preview_time(time_range)

    spec = {
        "source": "gibs",
        "layer": resolved_layer,
        "bbox": list(bbox),
        "time": time,
        "format": "image/png",
    }
    preview_handle = await store.put_handle(workspace_id, HandleType.PREVIEW, spec)

    return {
        "handle": preview_handle,
        "gibs_url": _gibs_url(resolved_layer, bbox, time),
        "layer": resolved_layer,
        "layer_is_guess": layer_is_guess,
        "lookup_source": lookup_source,
        "lookup_failure_reason": lookup_failure_reason,
        "bbox": list(bbox),
        "time": time,
        "format": "image/png",
    }


async def _aoi_bbox(
    aoi_handle: str | None, workspace_id: str, store: WorkspaceStore
) -> tuple[float, float, float, float] | None:
    """Resolve an optional ``aoi_`` handle to a bbox (W,S,E,N)."""
    if aoi_handle is None:
        return None
    return await resolve_aoi(store, workspace_id, aoi_handle)


def _preview_bbox(
    collection: dict, aoi_bbox: tuple[float, float, float, float] | None
) -> tuple[float, float, float, float]:
    """Prefer the AOI bbox, fall back to the collection extent, then to global."""
    if aoi_bbox is not None:
        return aoi_bbox
    extent = collection.get("bbox")
    if extent and len(extent) == 4:
        return tuple(float(c) for c in extent)
    return _GLOBAL_BBOX


def _preview_time(time_range: str | None) -> str | None:
    """GIBS snapshots are single-date; use the start of a ``"start/end"`` range."""
    if not time_range:
        return None
    start = time_range.split("/", 1)[0].strip()
    # A date is enough for GIBS daily imagery; keep a full instant if that's given.
    return start.split("T", 1)[0] if "T" in start else start


def _gibs_url(
    layer: str,
    bbox: tuple[float, float, float, float],
    time: str | None,
    width: int = 512,
    height: int = 512,
) -> str:
    """A GIBS WMS 1.3.0 ``GetMap`` URL. EPSG:4326 BBOX is ``ymin,xmin,ymax,xmax``."""
    w, s, e, n = bbox
    params = {
        "SERVICE": "WMS",
        "REQUEST": "GetMap",
        "VERSION": "1.3.0",
        "LAYERS": layer,
        "CRS": "EPSG:4326",
        "BBOX": f"{s},{w},{n},{e}",
        "WIDTH": str(width),
        "HEIGHT": str(height),
        "FORMAT": "image/png",
    }
    if time:
        params["TIME"] = time
    return f"{_GIBS_WMS}?{urllib.parse.urlencode(params)}"


# -- summarize_dataset -----------------------------------------------------


async def summarize_dataset(
    handle: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    storage: StorageBackend | None = None,
) -> dict:
    """Structural summary of a ``dataset_`` (metadata) or materialized ``obs_``/``cube_``.

    A ``dataset_`` summary comes from the collection metadata on the handle. An
    ``obs_``/``cube_`` summary opens the materialized blob through the storage
    backend and reports dims/variables (gridded) or rows/columns (tabular). A handle
    that exists but is not yet materialized returns ``status`` with a ``None`` summary
    rather than raising.
    """
    store = store or _default_store()
    htype = handle_type_of(handle)

    if htype is HandleType.DATASET:
        record = await store.get_handle(workspace_id, handle)  # isolation gate
        collection = record.payload.get("collection", {})
        return {
            "handle": handle,
            "kind": "dataset",
            "summary": {
                "short_name": collection.get("short_name")
                or record.payload.get("short_name"),
                "concept_id": record.payload.get("concept_id")
                or collection.get("concept_id"),
                "processing_level": collection.get("processing_level"),
                "bbox": collection.get("bbox"),
                "temporal": collection.get("temporal"),
                "native_formats": collection.get("native_formats"),
            },
        }

    if htype in (HandleType.OBS, HandleType.CUBE):
        try:
            storage_key, media_type, _payload = await resolve_materialized(
                store, workspace_id, handle
            )
        except ValueError:
            # Not yet materialized — isolation must still be enforced here too.
            record = await store.get_handle(workspace_id, handle)  # isolation gate
            return {
                "handle": handle,
                "kind": htype.value,
                "status": record.payload.get("status"),
                "summary": None,
            }
        storage = storage or _default_storage()
        data = await storage.get(storage_key)
        obj = open_result(data, media_type)
        return {
            "handle": handle,
            "kind": htype.value,
            "media_type": media_type,
            "summary": _summarize_obj(obj),
        }

    raise ValueError(
        f"summarize_dataset expects a dataset_, obs_, or cube_ handle, got {handle!r}"
    )


def _summarize_obj(obj: xr.Dataset | pa.Table) -> dict:
    """Structural summary of an opened dataset/table."""
    if isinstance(obj, xr.Dataset):
        return {
            "type": "grid",
            "dims": {str(k): int(v) for k, v in obj.sizes.items()},
            "data_vars": [str(v) for v in obj.data_vars],
            "coords": [str(c) for c in obj.coords],
            "attrs": {str(k): _json_safe(v) for k, v in obj.attrs.items()},
        }
    return {
        "type": "table",
        "num_rows": obj.num_rows,
        "columns": list(obj.column_names),
        "schema": {f.name: str(f.type) for f in obj.schema},
    }


def _json_safe(value):
    """Coerce a netCDF global-attribute value to a JSON-serializable type.

    netCDF attrs commonly come back as numpy scalars/arrays (``np.int32``,
    ``np.float64``, ...) via h5netcdf. Those aren't JSON-serializable, so left
    as-is they fail MCP output-schema serialization with a generic "Output
    validation error" that hides the real ``TypeError`` — every other summary
    field survives, only the attrs blob needs normalizing.
    """
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


# -- inspect_statistics ----------------------------------------------------


async def inspect_statistics(
    handle: str,
    variables: list[str] | None = None,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    storage: StorageBackend | None = None,
) -> dict:
    """Descriptive per-variable statistics over a materialized ``obs_``/``cube_``.

    Computes min/max/mean/std/count per variable (or per column, for tabular). This
    is inspection, not analysis: no correlation, trend, anomaly, or hotspot — those
    are out of scope by PLAN.md rule.
    """
    store = store or _default_store()
    storage = storage or _default_storage()

    storage_key, media_type, _payload = await resolve_materialized(store, workspace_id, handle)

    local_path = storage.path(storage_key)
    if local_path is not None:
        # Fast path: open lazily from disk, fuse all reductions into one dask.compute().
        with open_result_lazy(local_path, media_type, variables) as obj:
            stats = (
                _statistics_fused(obj, variables)
                if isinstance(obj, xr.Dataset)
                else _statistics(obj, variables)  # pa.Table Parquet path
            )
        return {"handle": handle, "statistics": stats}

    # Fallback: S3 or any non-path-addressable backend — bytes → eager open.
    data = await storage.get(storage_key)
    obj = open_result(data, media_type)
    return {
        "handle": handle,
        "statistics": _statistics(obj, variables),
    }


def _statistics_fused(obj: xr.Dataset, variables: list[str] | None) -> dict:
    """Per-variable stats via a single dask.compute() spanning all variables.

    Builds lazy xarray reductions (min/max/mean/std/count) for every numeric
    variable and lazy count for non-numeric ones, then calls dask.compute() once
    so the scheduler executes the entire graph in a single pass — each chunk of
    each dask-backed array is read from the storage layer only once per variable
    (the scheduler sees the shared source nodes and avoids redundant I/O).
    """
    names = variables or [str(v) for v in obj.data_vars]
    numeric = [n for n in names if np.issubdtype(obj[n].dtype, np.number)]
    other = [n for n in names if not np.issubdtype(obj[n].dtype, np.number)]

    lazy_vals: list = []
    schedule: list[tuple[str, str]] = []  # parallel to lazy_vals

    for name in numeric:
        da = obj[name]
        for stat, reduction in (
            ("min",   da.min()),
            ("max",   da.max()),
            ("mean",  da.mean()),
            ("std",   da.std()),
            ("count", da.count()),
        ):
            lazy_vals.append(reduction)
            schedule.append((name, stat))

    for name in other:
        lazy_vals.append(obj[name].count())
        schedule.append((name, "count"))

    computed = dask.compute(*lazy_vals)

    out: dict[str, dict] = {}
    for (name, stat), value in zip(schedule, computed):
        entry = out.setdefault(name, {})
        entry[stat] = int(value) if stat == "count" else _finite_scalar(value)

    for name in other:
        out[name]["dtype"] = str(obj[name].dtype)

    return out


def _finite_scalar(value) -> float | None:
    """Normalize any numeric scalar (numpy, Python, 0-d array) to ``float | None``.

    Accepts: numpy floats/ints, Python numbers, 0-d ndarrays, xr.DataArray scalars.
    Returns ``None`` for NaN.  Returns ``None`` (rather than raising) for any type
    that can't be coerced to float (complex, object) — callers should guard with
    ``np.issubdtype`` before passing non-numeric values.
    """
    try:
        v = float(np.asarray(value).flat[0])
    except (TypeError, ValueError):
        return None
    return None if math.isnan(v) else v


def _statistics(obj: xr.Dataset | pa.Table, variables: list[str] | None) -> dict:
    """Per-variable descriptive statistics; skips non-numeric columns."""
    stats: dict[str, dict] = {}
    if isinstance(obj, xr.Dataset):
        names = variables or [str(v) for v in obj.data_vars]
        for name in names:
            da = obj[name]
            if not np.issubdtype(da.dtype, np.number):
                # Non-numeric (datetime, string, object): count only.
                stats[name] = {"count": int(da.count()), "dtype": str(da.dtype)}
                continue
            stats[name] = {
                "min": _finite(da.min()),
                "max": _finite(da.max()),
                "mean": _finite(da.mean()),
                "std": _finite(da.std()),
                "count": int(da.count()),
            }
        return stats

    names = variables or list(obj.column_names)
    for name in names:
        column = obj[name]
        try:
            stats[name] = {
                "min": pc.min(column).as_py(),
                "max": pc.max(column).as_py(),
                "mean": pc.mean(column).as_py(),
                "std": pc.stddev(column).as_py(),
                "count": pc.count(column).as_py(),
            }
        except pa.lib.ArrowNotImplementedError:
            # Non-numeric column: count only.
            stats[name] = {"count": pc.count(column).as_py()}
    return stats


def _finite(value: xr.DataArray) -> float | None:
    """A scalar reduction as a plain float, with NaN normalized to ``None``."""
    result = float(value.values)
    return None if math.isnan(result) else result
