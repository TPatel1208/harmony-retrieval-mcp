"""Extract continent bounding boxes from Natural Earth admin-0 boundaries.

Downloads the Natural Earth 110m admin-0 countries GeoJSON from the public
GitHub mirror, groups features by continent, and prints the union bbox for
each continent in the (W, S, E, N) order used by this system.

Usage:
    python scripts/extract_natural_earth_bboxes.py

The printed values are the source for the continent entries in
src/earthdata_mcp/tools/data/regions.json.  Run this script to reproduce
or audit those values; copy the output into the JSON file by hand so the
per-region aliases and informal-region entries are preserved.

Requirements: only the Python standard library and ``urllib.request``.
No external packages needed.
"""

import json
import urllib.request

NE_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/"
    "geojson/ne_110m_admin_0_countries.geojson"
)

COORD_ORDER_NOTE = (
    "# Coordinate order: [W, S, E, N] "
    "(west longitude, south latitude, east longitude, north latitude)"
)


def bbox_union(
    bboxes: list[tuple[float, float, float, float]],
) -> tuple[float, float, float, float]:
    """Return the smallest axis-aligned bbox that contains all input bboxes."""
    w = min(b[0] for b in bboxes)
    s = min(b[1] for b in bboxes)
    e = max(b[2] for b in bboxes)
    n = max(b[3] for b in bboxes)
    return (w, s, e, n)


def feature_bbox(geometry: dict) -> tuple[float, float, float, float]:
    """Return (W, S, E, N) for a GeoJSON geometry by scanning all coordinates."""
    coords = list(flatten_coords(geometry.get("coordinates", [])))
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return (min(lons), min(lats), max(lons), max(lats))


def flatten_coords(obj):
    """Recursively yield [lon, lat] pairs from nested coordinate arrays."""
    if not obj:
        return
    if isinstance(obj[0], (int, float)):
        yield obj[:2]
    else:
        for item in obj:
            yield from flatten_coords(item)


def main() -> None:
    print(f"Fetching {NE_URL} …")
    with urllib.request.urlopen(NE_URL, timeout=30) as resp:
        fc = json.loads(resp.read())

    continent_bboxes: dict[str, list[tuple[float, float, float, float]]] = {}
    for feature in fc["features"]:
        continent = feature["properties"].get("CONTINENT", "Unknown")
        bbox = feature_bbox(feature["geometry"])
        continent_bboxes.setdefault(continent, []).append(bbox)

    print(COORD_ORDER_NOTE)
    print("Continent bounding boxes (W, S, E, N):")
    for continent in sorted(continent_bboxes):
        w, s, e, n = bbox_union(continent_bboxes[continent])
        print(f'  "{continent.lower()}": {{"bbox": [{w:.1f}, {s:.1f}, {e:.1f}, {n:.1f}]}}')


if __name__ == "__main__":
    main()
