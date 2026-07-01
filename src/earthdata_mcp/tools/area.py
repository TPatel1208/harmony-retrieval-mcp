"""``define_area_of_interest`` — mint an ``aoi_`` handle (PLAN.md §6 Phase 6.1).

Accepts the forms an agent naturally has on hand and resolves each to a bbox
(plus the original geometry, when given GeoJSON):

* a bbox string ``"-105,37,-104,38"`` (W,S,E,N decimal degrees),
* a bare point string ``"-93.6,41.6"`` (lon,lat decimal degrees) — resolves to a
  degenerate point AOI (Point geojson + zero-area bbox), the point/station case
  ``retrieve_timeseries(point_sample=True)`` expects,
* a GeoJSON geometry/Feature (string or dict),
* a HUC watershed code (all-digit string, even length 2–12) — resolved via
  the **USGS Watershed Boundary Dataset (WBD) REST API** (auth-free).
* a place/watershed/basin name — tried first against **Nominatim** (OSM), then
  against the USGS WBD name search (HUC2 → HUC12); raises if both fail.

The handle ``payload`` is the **re-materializable spec** (CLAUDE.md hard rule):
the resolved ``bbox``, the source kind, the original ``geojson`` when supplied,
and the original ``query`` when geocoded — never an ephemeral staged URL. This is
all metadata, fast, and auth-free: no Harmony, no downloads (PLAN.md §6).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
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

#: Curated region lookup table — checked before Nominatim for continent-scale
#: and informal-region queries that the OSM geocoder mishandles silently.
_REGIONS_PATH = Path(__file__).parent / "data" / "regions.json"
_region_table: dict[str, tuple[float, float, float, float]] | None = None

#: USGS National Map Watershed Boundary Dataset (WBD) REST API — auth-free.
#: Layer N corresponds to HUC level 2N (e.g. layer 1 = HUC2, layer 4 = HUC8).
_WBD_BASE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer"
#: Valid HUC code lengths — even digits in [2, 12].
_HUC_LENGTHS: frozenset[int] = frozenset({2, 4, 6, 8, 10, 12})

#: Matches "HUC-2 10", "HUC2 14", "HUC 01010001", "huc-8 14080101", etc.
#: Stated level digits are advisory only; code length determines the WBD layer.
_HUC_PREFIX_RE = re.compile(r"(?i)^huc[-]?\d*\s+(\d+)$")

#: Stopwords stripped before AND-token WBD search.
_WBD_STOPWORDS: frozenset[str] = frozenset({"river", "basin", "watershed", "region"})


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


def _load_region_table() -> dict[str, tuple[float, float, float, float]]:
    """Load and cache the curated region lookup table from ``regions.json``.

    Primary keys and aliases are both stored in lowercase so lookup is
    case-insensitive after a single ``.lower()`` call on the query.
    The result is module-level cached after the first call.
    """
    global _region_table
    if _region_table is not None:
        return _region_table
    raw: dict = json.loads(_REGIONS_PATH.read_text(encoding="utf-8"))
    table: dict[str, tuple[float, float, float, float]] = {}
    for key, entry in raw.items():
        if key.startswith("_"):
            continue  # skip metadata keys such as "_comment"
        w, s, e, n = entry["bbox"]
        bbox: tuple[float, float, float, float] = (float(w), float(s), float(e), float(n))
        table[key] = bbox  # key is already lowercase in the JSON
        for alias in entry.get("aliases", []):
            table[alias.lower()] = bbox
    _region_table = table
    return table


def _lookup_region(query: str) -> tuple[float, float, float, float] | None:
    """Return the curated bbox for ``query`` (case-insensitive), or ``None``."""
    return _load_region_table().get(query.lower().strip())


def _is_huc_code(s: str) -> bool:
    """Return True iff ``s`` is an all-digit string whose length is in {2,4,6,8,10,12}."""
    return s.isdigit() and len(s) in _HUC_LENGTHS


def _normalize_huc_prefix(s: str) -> str | None:
    """Strip a HUC prefix from ``s`` and return the trailing digit string, or None.

    Accepts forms like ``"HUC-2 10"``, ``"HUC2 14"``, ``"HUC 01010001"``,
    ``"huc-8 14080101"`` (case-insensitive).  Leading zeros in the digit portion
    are preserved verbatim.  Returns ``None`` when no prefix pattern matches.
    """
    m = _HUC_PREFIX_RE.match(s.strip())
    return m.group(1) if m else None


async def _resolve_location(
    location: str | dict,
) -> tuple[tuple[float, float, float, float], dict | None, str, str | None]:
    """Dispatch on input shape → ``(bbox, geojson, source, query)``.

    Resolution order:
    1. dict / GeoJSON string → geojson branch.
    2. Four-comma string → bbox branch (raises on bad values; never geocodes).
    3. One-comma numeric string ``"lon,lat"`` → a degenerate point AOI (a Point
       geojson plus a zero-area bbox where W==E, S==N) — the point/station case
       ``retrieve_timeseries(point_sample=True)`` expects. A one-comma string
       that doesn't parse as two in-range floats (e.g. ``"Denver, CO"``) falls
       through to geocoding instead.
    4. All-digit even-length string (2–12 chars) → USGS WBD by HUC code; raises
       immediately if WBD returns nothing (no Nominatim fallback for HUC inputs).
    5. Curated region table (``tools/data/regions.json``) — continents and
       informal regions that Nominatim mishandles; matched case-insensitively
       including aliases.  Returns immediately; Nominatim is never called.
    6. Plain-English name → Nominatim first:
       - Polygon/MultiPolygon result → return immediately, source ``"nominatim"``.
       - Point/LineString result (``geo is None``) → try USGS WBD name search first:
         - WBD hit → return it, source ``"usgs_wbd"`` (fixes basin/watershed names).
         - WBD miss → return the Nominatim point bbox, source ``"nominatim_point"``.
       - Nominatim raises (no results at all) → try USGS WBD name search (HUC2 →
         HUC12, first non-empty layer); if both fail, raises ValueError naming both.
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

    # A string with exactly one comma may be a bare "lon,lat" point (the
    # point/station case retrieve_timeseries(point_sample=True) expects) — but
    # it may also be a "City, State" place name, so only claim it as a point
    # when both parts actually parse as in-range floats; otherwise fall through
    # to geocoding below.
    if location.count(",") == 1:
        point = _try_parse_point(location)
        if point is not None:
            lon, lat = point
            geometry = {"type": "Point", "coordinates": [lon, lat]}
            return (lon, lat, lon, lat), geometry, "point", None

    huc_digits = _normalize_huc_prefix(location)
    if huc_digits is not None and _is_huc_code(huc_digits):
        bbox, geometry = await _query_wbd_by_huc(huc_digits)
        return bbox, geometry, "usgs_wbd", huc_digits

    if _is_huc_code(location):
        bbox, geometry = await _query_wbd_by_huc(location)
        return bbox, geometry, "usgs_wbd", location

    region_bbox = _lookup_region(location)
    if region_bbox is not None:
        return region_bbox, None, "region_table", location

    nominatim_err: ValueError | None = None
    nominatim_point_bbox: tuple[float, float, float, float] | None = None
    try:
        bbox, geo = await _geocode_nominatim(location)
        if geo is not None:
            return bbox, geo, "nominatim", location
        # geo is None → Point or LineString; fall through to WBD before committing.
        nominatim_point_bbox = bbox
    except ValueError as exc:
        nominatim_err = exc

    result = await _query_wbd_by_name(location)
    if result is not None:
        wbd_bbox, geometry = result
        return wbd_bbox, geometry, "usgs_wbd", location

    if nominatim_point_bbox is not None:
        return nominatim_point_bbox, None, "nominatim_point", location

    raise ValueError(
        f"Neither Nominatim nor USGS WBD found results for location {location!r}"
    ) from nominatim_err


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


def _try_parse_point(s: str) -> tuple[float, float] | None:
    """Parse ``"lon,lat"`` → ``(lon, lat)``, or ``None`` if it isn't one.

    Returns ``None`` rather than raising so a one-comma place name like
    ``"Denver, CO"`` falls through to geocoding instead of being rejected.
    """
    parts = s.split(",")
    if len(parts) != 2:
        return None
    try:
        lon, lat = (float(p.strip()) for p in parts)
    except ValueError:
        return None
    if not (-180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0):
        return None
    return lon, lat


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


# -- USGS WBD REST API --------------------------------------------------------


async def _query_wbd_by_huc(
    huc_code: str,
) -> tuple[tuple[float, float, float, float], dict]:
    """Query USGS WBD by exact HUC code. Raises ``ValueError`` if not found.

    Leading zeros are preserved verbatim — never strip them before sending.
    """
    n = len(huc_code)
    layer = n // 2
    field = f"huc{n}"
    url = f"{_WBD_BASE_URL}/{layer}/query"
    params = {
        "f": "geojson",
        "where": f"{field}='{huc_code}'",
        "outFields": "*",
        "returnGeometry": "true",
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        fc = response.json()
    features = fc.get("features", [])
    if not features:
        raise ValueError(f"USGS WBD found no watershed for HUC code {huc_code!r}")
    geometry = features[0]["geometry"]
    return _bbox_from_geojson(geometry), geometry


async def _query_wbd_by_name(
    query: str,
) -> tuple[tuple[float, float, float, float], dict] | None:
    """Search USGS WBD by name: LIKE phase first, then token phase.

    LIKE returns on exactly 1 match at the coarsest non-empty layer; >1 or 0
    results fall through to token search.  Token search strips stopwords, builds
    an AND WHERE clause, and raises ``ValueError`` for multiple candidates.
    Returns ``None`` when both phases find nothing.
    """
    async with httpx.AsyncClient(timeout=15.0) as client:
        result = await _query_wbd_by_like(query, client)
        if result is not None:
            return result
        return await _query_wbd_by_tokens(query, client)


async def _query_wbd_by_like(
    query: str,
    client: httpx.AsyncClient,
) -> tuple[tuple[float, float, float, float], dict] | None:
    """LIKE substring search across HUC2→HUC12 layers, coarsest-first.

    Returns ``(bbox, geometry)`` only when exactly 1 feature matches at the
    first non-empty layer.  Returns ``None`` for 0 total matches or >1 at the
    first non-empty layer (caller falls through to token search).
    """
    safe_query = query.replace("'", "''")
    for layer in range(1, 7):
        url = f"{_WBD_BASE_URL}/{layer}/query"
        params = {
            "f": "geojson",
            "where": f"UPPER(name) LIKE UPPER('%{safe_query}%')",
            "outFields": "name",
            "returnGeometry": "true",
        }
        response = await client.get(url, params=params)
        response.raise_for_status()
        features = response.json().get("features", [])
        if features:
            if len(features) == 1:
                geometry = features[0]["geometry"]
                return _bbox_from_geojson(geometry), geometry
            return None  # >1 at first non-empty layer; fall through to token
    return None


async def _query_wbd_by_tokens(
    query: str,
    client: httpx.AsyncClient,
) -> tuple[tuple[float, float, float, float], dict] | None:
    """AND-token WBD search after stopword removal, coarsest-first.

    Strips ``{river, basin, watershed, region}`` and requires all remaining
    tokens to appear in the WBD name.  Stops at the first non-empty layer.
    Raises ``ValueError`` naming candidates when multiple features match.
    Returns ``None`` when the token list is empty or all layers return nothing.
    """
    tokens = [t for t in query.lower().split() if t not in _WBD_STOPWORDS]
    if not tokens:
        return None
    safe_tokens = [t.replace("'", "''") for t in tokens]
    conditions = " AND ".join(
        f"UPPER(name) LIKE UPPER('%{t}%')" for t in safe_tokens
    )
    for layer in range(1, 7):
        huc_level = layer * 2
        huc_field = f"huc{huc_level}"
        url = f"{_WBD_BASE_URL}/{layer}/query"
        params = {
            "f": "geojson",
            "where": conditions,
            "outFields": f"name,{huc_field}",
            "returnGeometry": "true",
        }
        response = await client.get(url, params=params)
        response.raise_for_status()
        features = response.json().get("features", [])
        if not features:
            continue
        if len(features) == 1:
            geometry = features[0]["geometry"]
            return _bbox_from_geojson(geometry), geometry
        candidates = [
            f"{f['properties'].get('name', '?')} ({f['properties'].get(huc_field, '?')})"
            for f in features
        ]
        first_code = features[0]["properties"].get(huc_field, "?")
        raise ValueError(
            f"Ambiguous location {query!r}: {len(candidates)} HUC-{huc_level}"
            f" regions match — {', '.join(candidates)}."
            f" Provide a HUC code directly (e.g. {first_code!r}) to be precise."
        )
    return None


# -- Nominatim -------------------------------------------------------------


async def _geocode_nominatim(
    query: str,
) -> tuple[tuple[float, float, float, float], dict | None]:
    """Geocode ``query`` via Nominatim. Returns ``((W, S, E, N), geojson|None)``.

    Sends two requests when necessary:
    1. ``featuretype=relation`` — prefers OSM administrative boundary relations
       (basins, national forests, country borders) over POI nodes.
    2. Unrestricted fallback — used when the first call returns nothing, so
       city names and landmarks that OSM stores as nodes or ways still resolve.

    ``polygon_geojson=1`` is included on both calls. The returned geojson is
    set only for ``Polygon`` / ``MultiPolygon`` results; ``Point`` and
    ``LineString`` results yield ``None`` so callers can treat ``None`` as
    "no polygon; use bbox only".

    Nominatim returns ``boundingbox`` as ``["S", "N", "W", "E"]`` strings,
    reordered here to the ``(W, S, E, N)`` convention used by CMR and AOI.
    """
    base_params: dict = {
        "q": query,
        "format": "json",
        "limit": 1,
        "polygon_geojson": 1,
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            _NOMINATIM_URL,
            params={**base_params, "featuretype": "relation"},
            headers=_NOMINATIM_HEADERS,
        )
        response.raise_for_status()
        results = response.json()

        if not results:
            # Fallback: unrestricted search for nodes/ways (cities, landmarks).
            response = await client.get(
                _NOMINATIM_URL,
                params=base_params,
                headers=_NOMINATIM_HEADERS,
            )
            response.raise_for_status()
            results = response.json()

    if not results:
        raise ValueError(f"Nominatim found no results for location {query!r}")

    result = results[0]
    box = result.get("boundingbox")
    if not box or len(box) != 4:
        raise ValueError(
            f"Nominatim result for {query!r} has no usable bounding box"
        )
    south, north, west, east = (float(v) for v in box)

    raw_geo = result.get("geojson")
    geojson = raw_geo if raw_geo and raw_geo.get("type") in ("Polygon", "MultiPolygon") else None

    return (west, south, east, north), geojson
