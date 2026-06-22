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

import pyarrow as pa
import pyarrow.compute as pc
import xarray as xr

from earthdata_mcp.storage import StorageBackend, get_storage_backend
from earthdata_mcp.tools._dataio import open_result
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
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


def _default_storage() -> StorageBackend:
    """Process-wide storage backend, built lazily so import has no side effects."""
    global _storage
    if _storage is None:
        _storage = get_storage_backend()
    return _storage


# -- preview_dataset -------------------------------------------------------


async def preview_dataset(
    dataset_handle: str,
    time_range: str | None = None,
    aoi_handle: str | None = None,
    layer: str | None = None,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
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

    # Layer: explicit > payload hint > short_name. Flag when it's a best guess so a
    # caller knows the GIBS layer name was not authoritatively resolved.
    resolved_layer = layer or record.payload.get("gibs_layer") or short_name
    layer_is_guess = layer is None and "gibs_layer" not in record.payload
    if not resolved_layer:
        raise ValueError(
            f"dataset handle {dataset_handle!r} has no GIBS layer, short_name, or "
            "explicit layer to preview"
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
    if handle_type_of(aoi_handle) is not HandleType.AOI:
        raise ValueError(f"expected an aoi_ handle, got {aoi_handle!r}")
    record = await store.get_handle(workspace_id, aoi_handle)
    bbox = record.payload.get("bbox")
    if not bbox or len(bbox) != 4:
        raise ValueError(f"aoi handle {aoi_handle!r} payload missing 'bbox'")
    return tuple(float(c) for c in bbox)


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
    record = await store.get_handle(workspace_id, handle)  # isolation gate

    if htype is HandleType.DATASET:
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
        payload = record.payload
        if payload.get("status") != "ready" or not payload.get("storage_key"):
            return {
                "handle": handle,
                "kind": htype.value,
                "status": payload.get("status"),
                "summary": None,
            }
        storage = storage or _default_storage()
        data = await storage.get(payload["storage_key"])
        obj = open_result(data, payload["media_type"])
        return {
            "handle": handle,
            "kind": htype.value,
            "media_type": payload["media_type"],
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
            "attrs": {str(k): obj.attrs[k] for k in obj.attrs},
        }
    return {
        "type": "table",
        "num_rows": obj.num_rows,
        "columns": list(obj.column_names),
        "schema": {f.name: str(f.type) for f in obj.schema},
    }


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
    htype = handle_type_of(handle)
    if htype not in (HandleType.OBS, HandleType.CUBE):
        raise ValueError(
            f"inspect_statistics expects an obs_ or cube_ handle, got {handle!r}"
        )
    store = store or _default_store()
    storage = storage or _default_storage()

    record = await store.get_handle(workspace_id, handle)  # isolation gate
    payload = record.payload
    if payload.get("status") != "ready" or not payload.get("storage_key"):
        raise ValueError(f"handle {handle!r} is not a materialized result")

    data = await storage.get(payload["storage_key"])
    obj = open_result(data, payload["media_type"])
    return {
        "handle": handle,
        "statistics": _statistics(obj, variables),
    }


def _statistics(obj: xr.Dataset | pa.Table, variables: list[str] | None) -> dict:
    """Per-variable descriptive statistics; skips non-numeric columns."""
    stats: dict[str, dict] = {}
    if isinstance(obj, xr.Dataset):
        names = variables or [str(v) for v in obj.data_vars]
        for name in names:
            da = obj[name]
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
