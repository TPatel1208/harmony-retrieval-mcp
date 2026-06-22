"""``define_area_of_interest`` — mint an ``aoi_`` handle (PLAN.md §6 Phase 6.1).

Accepts the forms an agent naturally has on hand and resolves each to a bbox
(plus the original geometry, when given GeoJSON):

* a bbox string ``"-105,37,-104,38"`` (W,S,E,N decimal degrees),
* a GeoJSON geometry/Feature (string or dict),
* a place name, HUC watershed, or FIPS code — geocoded via **Nominatim**.

The handle ``payload`` is the **re-materializable spec** (CLAUDE.md hard rule):
the resolved ``bbox``, the source kind, the original ``geojson`` when supplied,
and the original ``query`` when geocoded — never an ephemeral staged URL. This is
all metadata, fast, and auth-free: no Harmony, no downloads (PLAN.md §6).
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.workspace.models import HandleType
from earthdata_mcp.workspace.store import WorkspaceStore

#: Nominatim's public geocoder. Free, no key — but its usage policy *requires* a
#: descriptive ``User-Agent`` (it 403s without one) and rate-limits anonymous
#: callers to ~1 req/sec, which is fine for a per-invocation tool.
_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
_NOMINATIM_HEADERS = {"User-Agent": "earthdata-mcp/0.1 (NASA Earthdata MCP server)"}


async def define_area_of_interest(
    location: str | dict,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
) -> dict:
    """Resolve ``location`` to a bbox and mint an ``aoi_`` handle.

    ``location`` is one of: a dict (GeoJSON), a GeoJSON string, a ``"W,S,E,N"``
    bbox string, or a place/HUC/FIPS name to geocode. Returns
    ``{"handle", "bbox": [W,S,E,N], "geojson": dict|None, "source": str}``.
    """
    store = store or _default_store()

    bbox, geojson, source, query = await _resolve_location(location)

    payload = {
        "source": source,
        "bbox": list(bbox),
        "geojson": geojson,
        "query": query,
    }
    handle = await store.put_handle(workspace_id, HandleType.AOI, payload=payload)

    return {
        "handle": handle,
        "bbox": list(bbox),
        "geojson": geojson,
        "source": source,
    }


async def _resolve_location(
    location: str | dict,
) -> tuple[tuple[float, float, float, float], dict | None, str, str | None]:
    """Dispatch on input shape → ``(bbox, geojson, source, query)``.

    First match wins: dict/GeoJSON-string → geojson; 4-comma string → bbox
    (re-raises on bad values rather than guessing it's a place name); anything
    else → Nominatim.
    """
    if isinstance(location, dict):
        return _bbox_from_geojson(location), location, "geojson", None

    parsed = _try_parse_json(location)
    if isinstance(parsed, dict):
        return _bbox_from_geojson(parsed), parsed, "geojson", None

    # A string with exactly three commas looks like "W,S,E,N": parse it as a bbox
    # and surface a bad value rather than silently geocoding "Colorado,37,-104,38".
    if location.count(",") == 3:
        return _parse_bbox_string(location), None, "bbox", None

    bbox = await _geocode_nominatim(location)
    return bbox, None, "nominatim", location


def _try_parse_json(s: str) -> Any:
    """Return ``json.loads(s)`` or ``None`` if it isn't JSON.

    A bare bbox string ``"-105,37,-104,38"`` is not valid JSON, so this returns
    ``None`` for it and the bbox branch handles it.
    """
    try:
        return json.loads(s)
    except (ValueError, TypeError):
        return None


# -- bbox string -----------------------------------------------------------


def _parse_bbox_string(s: str) -> tuple[float, float, float, float]:
    """Parse ``"W,S,E,N"`` → ``(W, S, E, N)``. Raises ``ValueError`` on bad input."""
    parts = s.split(",")
    if len(parts) != 4:
        raise ValueError(
            f"expected 4 comma-separated floats (W,S,E,N), got {len(parts)}: {s!r}"
        )
    try:
        w, south, e, n = (float(p.strip()) for p in parts)
    except ValueError as exc:
        raise ValueError(f"non-numeric value in bbox string {s!r}") from exc
    _validate_bbox(w, south, e, n)
    return (w, south, e, n)


def _validate_bbox(w: float, s: float, e: float, n: float) -> None:
    """Validate a ``(W, S, E, N)`` bbox.

    Longitudes in ``[-180, 180]``, latitudes in ``[-90, 90]``, and ``S <= N``.
    ``W <= E`` is **not** required — an anti-meridian-crossing box (W > E) is
    valid and CMR accepts it.
    """
    if not (-180.0 <= w <= 180.0 and -180.0 <= e <= 180.0):
        raise ValueError(f"longitude out of range [-180, 180]: W={w}, E={e}")
    if not (-90.0 <= s <= 90.0 and -90.0 <= n <= 90.0):
        raise ValueError(f"latitude out of range [-90, 90]: S={s}, N={n}")
    if s > n:
        raise ValueError(f"south latitude {s} exceeds north latitude {n}")


# -- GeoJSON ---------------------------------------------------------------


def _bbox_from_geojson(geojson: dict) -> tuple[float, float, float, float]:
    """Compute ``(W, S, E, N)`` from any GeoJSON geometry/Feature/FeatureCollection.

    Always derived from the coordinates (a stale or differently-ordered ``bbox``
    member on the object is ignored). Raises ``ValueError`` if no coordinates are
    present.
    """
    coords = _flatten_coords(geojson)
    if not coords:
        raise ValueError("GeoJSON contains no coordinate pairs")
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return (min(lons), min(lats), max(lons), max(lats))


def _flatten_coords(obj: dict) -> list[list[float]]:
    """Collect every ``[lon, lat]`` pair from a GeoJSON object."""
    gtype = obj.get("type")
    if gtype == "Feature":
        return _flatten_coords(obj.get("geometry") or {})
    if gtype == "FeatureCollection":
        pairs: list[list[float]] = []
        for feature in obj.get("features", []):
            pairs.extend(_flatten_coords(feature))
        return pairs
    if gtype == "GeometryCollection":
        pairs = []
        for geom in obj.get("geometries", []):
            pairs.extend(_flatten_coords(geom))
        return pairs
    return _extract_pairs(obj.get("coordinates"))


def _extract_pairs(coords: Any) -> list[list[float]]:
    """Flatten arbitrarily-nested coordinate arrays into ``[[lon, lat], ...]``.

    A single ``[lon, lat]`` (or ``[lon, lat, alt]``) pair is detected by its first
    element being a number; altitude, if present, is dropped.
    """
    if not coords:
        return []
    if isinstance(coords[0], (int, float)):
        return [list(coords[:2])]
    pairs: list[list[float]] = []
    for item in coords:
        pairs.extend(_extract_pairs(item))
    return pairs


# -- Nominatim -------------------------------------------------------------


async def _geocode_nominatim(query: str) -> tuple[float, float, float, float]:
    """Geocode ``query`` to ``(W, S, E, N)`` via Nominatim. Raises on no match.

    Nominatim returns ``boundingbox`` as ``["S", "N", "W", "E"]`` strings, which
    we reorder to the ``(W, S, E, N)`` convention CMR and our ``AOI`` use.
    """
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            _NOMINATIM_URL,
            params={"q": query, "format": "json", "limit": 1},
            headers=_NOMINATIM_HEADERS,
        )
    response.raise_for_status()
    results = response.json()
    if not results:
        raise ValueError(f"Nominatim found no results for location {query!r}")
    box = results[0].get("boundingbox")
    if not box or len(box) != 4:
        raise ValueError(
            f"Nominatim result for {query!r} has no usable bounding box"
        )
    south, north, west, east = (float(v) for v in box)
    return (west, south, east, north)
