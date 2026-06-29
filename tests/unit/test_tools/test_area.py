"""``define_area_of_interest`` — bbox/GeoJSON parsing, Nominatim, WBD, handle minting.

Pure parsing is tested without any DB or HTTP; the integration tests use the real
Postgres-backed ``workspace_store`` fixture and mock HTTP with pytest-httpx
(``httpx_mock``). The store payload is asserted to be the re-materializable spec.
"""

from __future__ import annotations

import json

import pytest

from earthdata_mcp.tools.area import (
    _bbox_from_geojson,
    _is_huc_code,
    _lookup_region,
    _normalize_huc_prefix,
    _parse_bbox_string,
    define_area_of_interest,
)
from earthdata_mcp.workspace.models import HandleType, handle_type_of
from earthdata_mcp.workspace.store import CrossWorkspaceError

# Nominatim returns boundingbox as ["S", "N", "W", "E"] strings.
# _NOMINATIM_RESPONSE includes a Polygon geojson as returned when polygon_geojson=1.
_NOMINATIM_POLYGON = {
    "type": "Polygon",
    "coordinates": [
        [
            [-116.0, 35.0],
            [-102.0, 35.0],
            [-102.0, 49.0],
            [-116.0, 49.0],
            [-116.0, 35.0],
        ]
    ],
}
_NOMINATIM_RESPONSE = [
    {
        "place_id": 12345,
        "display_name": "Rocky Mountains, Colorado, USA",
        "boundingbox": ["35.0", "49.0", "-116.0", "-102.0"],
        "lat": "42.0",
        "lon": "-109.0",
        "geojson": _NOMINATIM_POLYGON,
    }
]
# Reordered to (W, S, E, N): W=-116.0, S=35.0, E=-102.0, N=49.0
_EXPECTED_NOMINATIM_BBOX = [-116.0, 35.0, -102.0, 49.0]

# Fallback result: Point geometry (no polygon should be stored).
_NOMINATIM_FALLBACK_POINT = [
    {
        "place_id": 99999,
        "display_name": "Denver, Colorado, USA",
        "boundingbox": ["39.6", "39.9", "-105.1", "-104.7"],
        "lat": "39.7392",
        "lon": "-104.9903",
        "geojson": {"type": "Point", "coordinates": [-104.9903, 39.7392]},
    }
]
_EXPECTED_FALLBACK_BBOX = [-105.1, 39.6, -104.7, 39.9]

# Fallback result: Polygon geometry (polygon should be stored even via fallback).
_NOMINATIM_FALLBACK_POLYGON_GEO = {
    "type": "Polygon",
    "coordinates": [
        [
            [-105.1, 39.6],
            [-104.7, 39.6],
            [-104.7, 39.9],
            [-105.1, 39.9],
            [-105.1, 39.6],
        ]
    ],
}
_NOMINATIM_FALLBACK_POLYGON = [
    {
        "place_id": 88888,
        "display_name": "Denver, Colorado, USA",
        "boundingbox": ["39.6", "39.9", "-105.1", "-104.7"],
        "lat": "39.7392",
        "lon": "-104.9903",
        "geojson": _NOMINATIM_FALLBACK_POLYGON_GEO,
    }
]

# -- USGS WBD mock data -------------------------------------------------------
# Represents a GeoJSON FeatureCollection returned by the WBD REST API.
_WBD_POLYGON = {
    "type": "Polygon",
    "coordinates": [
        [
            [-108.0, 36.0],
            [-105.0, 36.0],
            [-105.0, 40.0],
            [-108.0, 40.0],
            [-108.0, 36.0],
        ]
    ],
}
_WBD_FEATURE_COLLECTION = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": _WBD_POLYGON,
            "properties": {"name": "Upper Colorado Region", "huc8": "14080101"},
        }
    ],
}
_WBD_EMPTY = {"type": "FeatureCollection", "features": []}
# Two-feature collection used to exercise LIKE→token ambiguity paths.
_WBD_LOWER_COLORADO_POLYGON = {
    "type": "Polygon",
    "coordinates": [
        [
            [-120.0, 31.0],
            [-109.0, 31.0],
            [-109.0, 37.0],
            [-120.0, 37.0],
            [-120.0, 31.0],
        ]
    ],
}
_WBD_FEATURE_COLLECTION_MULTI = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": _WBD_POLYGON,
            "properties": {"name": "Upper Colorado Region", "huc2": "14"},
        },
        {
            "type": "Feature",
            "geometry": _WBD_LOWER_COLORADO_POLYGON,
            "properties": {"name": "Lower Colorado Region", "huc2": "15"},
        },
    ],
}
# (W, S, E, N) derived from _WBD_POLYGON coordinates
_EXPECTED_WBD_BBOX = [-108.0, 36.0, -105.0, 40.0]


# -- bbox string parsing (no DB, no HTTP) ---------------------------------


def test_parse_bbox_string_valid() -> None:
    assert _parse_bbox_string("-105,37,-104,38") == (-105.0, 37.0, -104.0, 38.0)


def test_parse_bbox_string_strips_whitespace() -> None:
    assert _parse_bbox_string(" -105, 37, -104, 38 ") == (-105.0, 37.0, -104.0, 38.0)


def test_parse_bbox_string_wrong_count() -> None:
    with pytest.raises(ValueError, match="4 comma-separated"):
        _parse_bbox_string("-105,37,-104")


def test_parse_bbox_string_non_numeric() -> None:
    with pytest.raises(ValueError, match="non-numeric"):
        _parse_bbox_string("-105,north,-104,38")


def test_parse_bbox_string_south_exceeds_north() -> None:
    with pytest.raises(ValueError, match="south"):
        _parse_bbox_string("-105,50,-104,38")


def test_parse_bbox_string_latitude_out_of_range() -> None:
    with pytest.raises(ValueError, match="latitude out of range"):
        _parse_bbox_string("-105,37,-104,95")


def test_parse_bbox_string_anti_meridian_allowed() -> None:
    # W > E is valid (crosses the anti-meridian); must not raise.
    assert _parse_bbox_string("170,-10,-170,10") == (170.0, -10.0, -170.0, 10.0)


# -- GeoJSON bbox extraction (no DB, no HTTP) -----------------------------


def test_bbox_from_geojson_polygon() -> None:
    poly = {
        "type": "Polygon",
        "coordinates": [
            [
                [-105.0, 37.0],
                [-104.0, 37.0],
                [-104.0, 38.0],
                [-105.0, 38.0],
                [-105.0, 37.0],
            ]
        ],
    }
    assert _bbox_from_geojson(poly) == (-105.0, 37.0, -104.0, 38.0)


def test_bbox_from_geojson_feature_wrapper_drops_altitude() -> None:
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-104.5, 37.5, 1000.0]},
        "properties": {},
    }
    assert _bbox_from_geojson(feature) == (-104.5, 37.5, -104.5, 37.5)


def test_bbox_from_geojson_multipolygon_spans_regions() -> None:
    mp = {
        "type": "MultiPolygon",
        "coordinates": [
            [[[-105.0, 37.0], [-104.0, 37.0], [-104.0, 38.0], [-105.0, 37.0]]],
            [[[-110.0, 40.0], [-109.0, 40.0], [-109.0, 41.0], [-110.0, 40.0]]],
        ],
    }
    assert _bbox_from_geojson(mp) == (-110.0, 37.0, -104.0, 41.0)


def test_bbox_from_geojson_empty_raises() -> None:
    with pytest.raises(ValueError, match="no coordinate pairs"):
        _bbox_from_geojson({"type": "GeometryCollection", "geometries": []})


# -- handle minting + persistence (real store) ----------------------------


async def test_define_area_bbox_mints_aoi_handle(
    workspace_store, workspace_id
) -> None:
    out = await define_area_of_interest(
        "-105,37,-104,38", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "bbox"
    assert out["bbox"] == [-105.0, 37.0, -104.0, 38.0]
    assert out["geojson"] is None
    assert out["handle"].startswith("aoi_")
    assert handle_type_of(out["handle"]) is HandleType.AOI


async def test_define_area_payload_is_rematerializable(
    workspace_store, workspace_id
) -> None:
    out = await define_area_of_interest(
        "-105,37,-104,38", workspace_id=workspace_id, store=workspace_store
    )
    record = await workspace_store.get_handle(workspace_id, out["handle"])
    assert record.payload["source"] == "bbox"
    assert record.payload["bbox"] == [-105.0, 37.0, -104.0, 38.0]
    assert record.payload["geojson"] is None
    assert record.payload["query"] is None


async def test_define_area_geojson_string_stores_geometry(
    workspace_store, workspace_id
) -> None:
    geojson_str = json.dumps(
        {
            "type": "Polygon",
            "coordinates": [
                [
                    [-105.0, 37.0],
                    [-104.0, 37.0],
                    [-104.0, 38.0],
                    [-105.0, 38.0],
                    [-105.0, 37.0],
                ]
            ],
        }
    )
    out = await define_area_of_interest(
        geojson_str, workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "geojson"
    assert out["bbox"] == [-105.0, 37.0, -104.0, 38.0]

    record = await workspace_store.get_handle(workspace_id, out["handle"])
    assert record.payload["source"] == "geojson"
    assert record.payload["geojson"]["type"] == "Polygon"


async def test_define_area_nominatim(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # The only HTTP call here is to Nominatim (the store is Postgres), so a
    # url-less mock is unambiguous and dodges query-encoding fragility.
    # One request expected: relation call hits immediately, no fallback needed.
    httpx_mock.add_response(json=_NOMINATIM_RESPONSE)
    out = await define_area_of_interest(
        "Rocky Mountains", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "nominatim"
    assert out["bbox"] == _EXPECTED_NOMINATIM_BBOX
    assert out["geojson"] == _NOMINATIM_POLYGON

    record = await workspace_store.get_handle(workspace_id, out["handle"])
    assert record.payload["query"] == "Rocky Mountains"
    assert record.payload["geojson"] == _NOMINATIM_POLYGON

    # Exactly one HTTP request (no fallback triggered).
    requests = httpx_mock.get_requests()
    assert len(requests) == 1
    # The required descriptive User-Agent went out (Nominatim 403s without it).
    assert "earthdata-mcp" in requests[0].headers.get("User-Agent", "")


async def test_define_area_nominatim_polygon_stored(
    httpx_mock, workspace_store, workspace_id
) -> None:
    multipolygon = {
        "type": "MultiPolygon",
        "coordinates": [
            [[[-116.0, 35.0], [-102.0, 35.0], [-102.0, 49.0], [-116.0, 35.0]]],
            [[[-120.0, 30.0], [-110.0, 30.0], [-110.0, 40.0], [-120.0, 30.0]]],
        ],
    }
    response = [
        {
            "place_id": 77777,
            "display_name": "Amazon Rainforest",
            "boundingbox": ["35.0", "49.0", "-116.0", "-102.0"],
            "lat": "42.0",
            "lon": "-109.0",
            "geojson": multipolygon,
        }
    ]
    httpx_mock.add_response(json=response)
    out = await define_area_of_interest(
        "Amazon Rainforest", workspace_id=workspace_id, store=workspace_store
    )
    assert out["geojson"] == multipolygon
    record = await workspace_store.get_handle(workspace_id, out["handle"])
    assert record.payload["geojson"] == multipolygon


async def test_define_area_nominatim_fallback_when_no_relation(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # First call (featuretype=relation) returns nothing; fallback returns a Point.
    # Point result is low-confidence → tool runs LIKE (6 layers) then token (6
    # layers) before falling back to the nominatim_point result.
    httpx_mock.add_response(json=[])                            # Nominatim relation
    httpx_mock.add_response(json=_NOMINATIM_FALLBACK_POINT)    # Nominatim unrestricted (Point)
    for _ in range(6):                                          # WBD LIKE layers 1–6 all miss
        httpx_mock.add_response(json=_WBD_EMPTY)
    for _ in range(6):                                          # WBD token layers 1–6 all miss
        httpx_mock.add_response(json=_WBD_EMPTY)
    out = await define_area_of_interest(
        "Denver", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "nominatim_point"
    assert out["bbox"] == _EXPECTED_FALLBACK_BBOX
    # Point geometry must not be stored as polygon.
    assert out["geojson"] is None
    # 2 Nominatim + 6 LIKE + 6 token = 14 HTTP requests.
    assert len(httpx_mock.get_requests()) == 14


async def test_define_area_basin_name_wbd_after_nominatim_point(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # Nominatim relation returns nothing; unrestricted returns a Point (low-confidence).
    # WBD layer 1 (HUC2) has a match → source is "usgs_wbd", not "nominatim_point".
    httpx_mock.add_response(json=[])                           # Nominatim relation
    httpx_mock.add_response(json=_NOMINATIM_FALLBACK_POINT)   # Nominatim unrestricted (Point)
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION)     # WBD layer 1 hits
    out = await define_area_of_interest(
        "Colorado River Basin", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "usgs_wbd"
    assert out["bbox"] == _EXPECTED_WBD_BBOX
    assert out["geojson"] == _WBD_POLYGON

    record = await workspace_store.get_handle(workspace_id, out["handle"])
    assert record.payload["source"] == "usgs_wbd"
    assert record.payload["query"] == "Colorado River Basin"

    # 2 Nominatim + 1 WBD = 3 HTTP requests total.
    assert len(httpx_mock.get_requests()) == 3


async def test_define_area_nominatim_fallback_polygon_stored(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # First call (featuretype=relation) returns nothing; fallback returns a Polygon.
    httpx_mock.add_response(json=[])
    httpx_mock.add_response(json=_NOMINATIM_FALLBACK_POLYGON)
    out = await define_area_of_interest(
        "Denver", workspace_id=workspace_id, store=workspace_store
    )
    assert out["geojson"] == _NOMINATIM_FALLBACK_POLYGON_GEO
    record = await workspace_store.get_handle(workspace_id, out["handle"])
    assert record.payload["geojson"] == _NOMINATIM_FALLBACK_POLYGON_GEO


async def test_define_area_nominatim_no_results_raises(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # Both Nominatim calls return nothing; all 6 LIKE layers and all 6 token
    # layers also return nothing → raises with both resolver names mentioned.
    httpx_mock.add_response(json=[])  # Nominatim relation
    httpx_mock.add_response(json=[])  # Nominatim unrestricted fallback
    for _ in range(6):  # WBD LIKE layers 1–6
        httpx_mock.add_response(json=_WBD_EMPTY)
    for _ in range(6):  # WBD token layers 1–6
        httpx_mock.add_response(json=_WBD_EMPTY)
    with pytest.raises(ValueError, match="Nominatim"):
        await define_area_of_interest(
            "ZZZ-nowhere", workspace_id=workspace_id, store=workspace_store
        )


async def test_define_area_handle_is_workspace_scoped(
    workspace_store, workspace_id
) -> None:
    out = await define_area_of_interest(
        "-105,37,-104,38", workspace_id=workspace_id, store=workspace_store
    )
    with pytest.raises(CrossWorkspaceError):
        await workspace_store.get_handle("ws-intruder", out["handle"])


# -- HUC code detection (pure unit, no I/O) -----------------------------------


def test_is_huc_code_valid_all_levels() -> None:
    assert _is_huc_code("14") is True
    assert _is_huc_code("1408") is True
    assert _is_huc_code("140801") is True
    assert _is_huc_code("14080101") is True
    assert _is_huc_code("1408010101") is True
    assert _is_huc_code("140801010101") is True


def test_is_huc_code_leading_zeros_valid() -> None:
    assert _is_huc_code("01010001") is True


def test_is_huc_code_odd_length_rejected() -> None:
    assert _is_huc_code("1") is False
    assert _is_huc_code("123") is False
    assert _is_huc_code("12345") is False
    assert _is_huc_code("1234567") is False


def test_is_huc_code_too_long_rejected() -> None:
    # 14 digits is even but outside the {2,4,6,8,10,12} set.
    assert _is_huc_code("14081010101234") is False


def test_is_huc_code_non_digit_rejected() -> None:
    assert _is_huc_code("14080101x") is False
    assert _is_huc_code("Colorado") is False
    assert _is_huc_code("") is False


# -- WBD HUC code resolver (real store, HTTP mocked) -------------------------


async def test_define_area_wbd_huc8(
    httpx_mock, workspace_store, workspace_id
) -> None:
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION)
    out = await define_area_of_interest(
        "14080101", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "usgs_wbd"
    assert out["bbox"] == _EXPECTED_WBD_BBOX
    assert out["geojson"] == _WBD_POLYGON
    assert out["handle"].startswith("aoi_")
    assert handle_type_of(out["handle"]) is HandleType.AOI

    record = await workspace_store.get_handle(workspace_id, out["handle"])
    assert record.payload["source"] == "usgs_wbd"
    assert record.payload["query"] == "14080101"
    assert record.payload["bbox"] == _EXPECTED_WBD_BBOX
    assert record.payload["geojson"] == _WBD_POLYGON

    # Exactly one HTTP request — no Nominatim fallback attempted.
    assert len(httpx_mock.get_requests()) == 1


async def test_define_area_wbd_huc_leading_zeros_preserved(
    httpx_mock, workspace_store, workspace_id
) -> None:
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION)
    huc = "01010001"
    await define_area_of_interest(huc, workspace_id=workspace_id, store=workspace_store)

    request = httpx_mock.get_requests()[0]
    # Leading zeros must appear verbatim in the WHERE clause sent to WBD.
    assert "01010001" in str(request.url)


@pytest.mark.parametrize(
    "huc_code,expected_layer",
    [
        ("14", 1),
        ("1408", 2),
        ("140801", 3),
        ("14080101", 4),
        ("1408010101", 5),
        ("140801010101", 6),
    ],
)
async def test_define_area_wbd_all_huc_levels(
    huc_code: str,
    expected_layer: int,
    httpx_mock,
    workspace_store,
    workspace_id,
) -> None:
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION)
    out = await define_area_of_interest(
        huc_code, workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "usgs_wbd"

    request = httpx_mock.get_requests()[0]
    assert f"/{expected_layer}/query" in request.url.path


async def test_define_area_wbd_invalid_huc_raises(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # WBD returns an empty feature collection for an unknown HUC code.
    httpx_mock.add_response(json=_WBD_EMPTY)
    with pytest.raises(ValueError, match="USGS WBD"):
        await define_area_of_interest(
            "99999999", workspace_id=workspace_id, store=workspace_store
        )
    # No Nominatim fallback — exactly one HTTP request to WBD.
    assert len(httpx_mock.get_requests()) == 1


# -- WBD name search (plain-English fallback after Nominatim empty) -----------


async def test_define_area_name_nominatim_empty_wbd_hit(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # Nominatim returns nothing on both calls; WBD layer 1 (HUC2) has a match.
    httpx_mock.add_response(json=[])   # Nominatim relation
    httpx_mock.add_response(json=[])   # Nominatim unrestricted fallback
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION)  # WBD layer 1 hits
    out = await define_area_of_interest(
        "Colorado River Basin", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "usgs_wbd"
    assert out["bbox"] == _EXPECTED_WBD_BBOX
    assert out["geojson"] == _WBD_POLYGON

    record = await workspace_store.get_handle(workspace_id, out["handle"])
    assert record.payload["source"] == "usgs_wbd"
    assert record.payload["query"] == "Colorado River Basin"


async def test_define_area_name_wbd_scan_skips_empty_layers(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # WBD layers 1 and 2 return nothing; layer 3 (HUC6) has the first hit.
    httpx_mock.add_response(json=[])              # Nominatim relation
    httpx_mock.add_response(json=[])              # Nominatim fallback
    httpx_mock.add_response(json=_WBD_EMPTY)     # WBD layer 1 empty
    httpx_mock.add_response(json=_WBD_EMPTY)     # WBD layer 2 empty
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION)  # WBD layer 3 hits
    out = await define_area_of_interest(
        "Chesapeake Bay Watershed", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "usgs_wbd"
    assert out["bbox"] == _EXPECTED_WBD_BBOX
    # 2 Nominatim + 3 WBD = 5 requests total.
    assert len(httpx_mock.get_requests()) == 5


async def test_define_area_both_resolvers_fail_raises(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # Nominatim returns nothing; LIKE (6 layers) and token (6 layers) also find
    # nothing → raises with both resolver names mentioned.
    httpx_mock.add_response(json=[])  # Nominatim relation
    httpx_mock.add_response(json=[])  # Nominatim fallback
    for _ in range(6):  # WBD LIKE layers 1–6
        httpx_mock.add_response(json=_WBD_EMPTY)
    for _ in range(6):  # WBD token layers 1–6
        httpx_mock.add_response(json=_WBD_EMPTY)
    with pytest.raises(ValueError, match="(?i)(nominatim|usgs wbd)"):
        await define_area_of_interest(
            "ZZZ-nonexistent-basin", workspace_id=workspace_id, store=workspace_store
        )


# -- HUC prefix normalization (pure unit, no I/O) -----------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("HUC-2 10", "10"),
        ("HUC2 14", "14"),
        ("HUC 01010001", "01010001"),   # leading zeros preserved
        ("huc-8 14080101", "14080101"),  # lowercase
        ("HUC-4 1408", "1408"),
        ("HUC12 140801010101", "140801010101"),
        ("HUC 14", "14"),              # no hyphen, no level digits
    ],
)
def test_normalize_huc_prefix_matching_forms(raw: str, expected: str) -> None:
    assert _normalize_huc_prefix(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "huckleberry creek",   # not a HUC prefix
        "HUC14",               # no whitespace separator
        "14080101",            # bare digits (no prefix)
        "Colorado",
        "HUC-2:10",            # colon separator not supported
        "HUC",                 # prefix only, no digits
        "",
    ],
)
def test_normalize_huc_prefix_non_matching_returns_none(raw: str) -> None:
    assert _normalize_huc_prefix(raw) is None


# -- HUC-prefix routing (real store, HTTP mocked) -----------------------------


async def test_define_area_huc_prefix_routes_to_correct_layer(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # "HUC-2 14" → normalize to "14" (2 digits) → layer 1, exactly 1 WBD request.
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION)
    out = await define_area_of_interest(
        "HUC-2 14", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "usgs_wbd"
    assert out["bbox"] == _EXPECTED_WBD_BBOX

    requests = httpx_mock.get_requests()
    assert len(requests) == 1
    assert "/1/query" in requests[0].url.path  # layer 1 = HUC-2


async def test_define_area_huc_prefix_leading_zeros_preserved(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # "HUC 01010001" → normalize to "01010001"; leading zeros must reach WBD verbatim.
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION)
    await define_area_of_interest(
        "HUC 01010001", workspace_id=workspace_id, store=workspace_store
    )
    assert "01010001" in str(httpx_mock.get_requests()[0].url)


async def test_define_area_huc_prefix_lowercase(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # "huc-8 14080101" (lowercase) → normalize to "14080101" → layer 4.
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION)
    out = await define_area_of_interest(
        "huc-8 14080101", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "usgs_wbd"
    assert "/4/query" in httpx_mock.get_requests()[0].url.path  # layer 4 = HUC-8
    assert len(httpx_mock.get_requests()) == 1


# -- Two-phase WBD name search (real store, HTTP mocked) ----------------------


async def test_define_area_like_multi_falls_to_token_single_hit(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # LIKE layer 1 returns >1 features → falls through to token search.
    # Token layer 1 returns exactly 1 feature → source "usgs_wbd".
    httpx_mock.add_response(json=[])                         # Nominatim relation
    httpx_mock.add_response(json=[])                         # Nominatim fallback
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION_MULTI)  # LIKE layer 1: >1
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION)   # token layer 1: 1 hit
    out = await define_area_of_interest(
        "Colorado", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "usgs_wbd"
    assert out["bbox"] == _EXPECTED_WBD_BBOX
    assert out["geojson"] == _WBD_POLYGON
    # 2 Nominatim + 1 LIKE + 1 token = 4 requests.
    assert len(httpx_mock.get_requests()) == 4


async def test_define_area_token_multi_raises_ambiguity(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # LIKE layer 1 returns >1 → token layer 1 also returns >1 → ValueError naming
    # the candidates and suggesting a HUC code.
    httpx_mock.add_response(json=[])                              # Nominatim relation
    httpx_mock.add_response(json=[])                              # Nominatim fallback
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION_MULTI)  # LIKE layer 1: >1
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION_MULTI)  # token layer 1: >1
    with pytest.raises(ValueError) as exc_info:
        await define_area_of_interest(
            "Colorado", workspace_id=workspace_id, store=workspace_store
        )
    msg = str(exc_info.value)
    assert "Colorado" in msg
    assert "Upper Colorado Region" in msg
    assert "Lower Colorado Region" in msg
    assert "HUC code" in msg


async def test_define_area_token_all_miss_nominatim_point_fallback(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # Nominatim returns a Point (low-confidence); LIKE and token each scan all
    # 6 layers with no hits → nominatim_point fallback.
    httpx_mock.add_response(json=[])                         # Nominatim relation
    httpx_mock.add_response(json=_NOMINATIM_FALLBACK_POINT)  # Nominatim fallback (Point)
    for _ in range(6):                                       # LIKE layers 1–6: all miss
        httpx_mock.add_response(json=_WBD_EMPTY)
    for _ in range(6):                                       # token layers 1–6: all miss
        httpx_mock.add_response(json=_WBD_EMPTY)
    out = await define_area_of_interest(
        "someplace", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "nominatim_point"
    assert out["bbox"] == _EXPECTED_FALLBACK_BBOX
    # 2 Nominatim + 6 LIKE + 6 token = 14 requests.
    assert len(httpx_mock.get_requests()) == 14


async def test_define_area_all_stopword_query_skips_token(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # Query is all stopwords ("river basin") → token phase returns None immediately
    # without making any HTTP requests; LIKE runs 6 layers (all empty); result is
    # nominatim_point.  Request count (8) proves token phase was skipped (vs 14).
    httpx_mock.add_response(json=[])                         # Nominatim relation
    httpx_mock.add_response(json=_NOMINATIM_FALLBACK_POINT)  # Nominatim fallback (Point)
    for _ in range(6):                                       # LIKE layers 1–6: all miss
        httpx_mock.add_response(json=_WBD_EMPTY)
    out = await define_area_of_interest(
        "river basin", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "nominatim_point"
    # 2 Nominatim + 6 LIKE + 0 token = 8 requests (token skipped).
    assert len(httpx_mock.get_requests()) == 8


# -- Region lookup table (real JSON file, no HTTP) ----------------------------
#
# Tests use the real regions.json to catch data errors in the table itself.
# No HTTP mocking needed: region-table hits return immediately without calling
# Nominatim or USGS WBD.


def _contains(bbox: list[float], lon: float, lat: float) -> bool:
    """Return True if (lon, lat) falls inside (W, S, E, N) bbox."""
    w, s, e, n = bbox
    return w <= lon <= e and s <= lat <= n


# -- Pure lookup (no DB, no HTTP) ---------------------------------------------


def test_lookup_region_africa() -> None:
    assert _lookup_region("Africa") is not None


def test_lookup_region_case_insensitive() -> None:
    assert _lookup_region("africa") == _lookup_region("AFRICA") == _lookup_region("Africa")


def test_lookup_region_whitespace_trimmed() -> None:
    assert _lookup_region("  Africa  ") == _lookup_region("Africa")


def test_lookup_region_alias_mena() -> None:
    assert _lookup_region("MENA") == _lookup_region("Middle East")


def test_lookup_region_alias_se_asia() -> None:
    assert _lookup_region("SE Asia") == _lookup_region("Southeast Asia")


def test_lookup_region_unknown_returns_none() -> None:
    assert _lookup_region("Brazil") is None
    assert _lookup_region("Colorado River Basin") is None
    assert _lookup_region("14080101") is None


# -- Full pipeline (real store, no HTTP) --------------------------------------


@pytest.mark.parametrize(
    "query,landmark_lon,landmark_lat",
    [
        ("Africa", 3.4, 6.5),          # Lagos
        ("Europe", 2.3, 48.9),         # Paris
        ("Asia", 139.7, 35.7),         # Tokyo
        ("North America", -99.1, 19.4),  # Mexico City
        ("South America", -43.2, -22.9),  # Rio de Janeiro
        ("Oceania", 151.2, -33.9),     # Sydney
        ("Antarctica", 0.0, -75.0),    # generic polar point
        ("Middle East", 31.2, 30.1),   # Cairo
        ("Southeast Asia", 100.5, 13.8),  # Bangkok
        ("South Asia", 77.2, 28.6),    # New Delhi
        ("East Asia", 139.7, 35.7),    # Tokyo
        ("Central Asia", 71.4, 51.2),  # Astana (Nur-Sultan)
        ("Scandinavia", 10.7, 59.9),   # Oslo
        ("Nordic countries", 10.7, 59.9),  # Oslo
        ("Sub-Saharan Africa", 3.4, 6.5),  # Lagos
        ("Central America", -87.2, 14.1),  # Tegucigalpa
        ("Caribbean", -66.1, 18.5),    # San Juan, Puerto Rico
    ],
)
async def test_region_bbox_contains_landmark(
    query: str,
    landmark_lon: float,
    landmark_lat: float,
    workspace_store,
    workspace_id,
) -> None:
    out = await define_area_of_interest(query, workspace_id=workspace_id, store=workspace_store)
    assert out["source"] == "region_table"
    assert _contains(out["bbox"], landmark_lon, landmark_lat), (
        f"{query!r} bbox {out['bbox']} does not contain "
        f"({landmark_lon}, {landmark_lat})"
    )


async def test_region_mena_alias_same_as_middle_east(workspace_store, workspace_id) -> None:
    out_me = await define_area_of_interest(
        "Middle East", workspace_id=workspace_id, store=workspace_store
    )
    out_mena = await define_area_of_interest(
        "MENA", workspace_id=workspace_id, store=workspace_store
    )
    assert out_mena["bbox"] == out_me["bbox"]
    # Both must contain Cairo (31.2°E, 30.1°N).
    assert _contains(out_mena["bbox"], 31.2, 30.1)


async def test_region_se_asia_alias(workspace_store, workspace_id) -> None:
    out_full = await define_area_of_interest(
        "Southeast Asia", workspace_id=workspace_id, store=workspace_store
    )
    out_short = await define_area_of_interest(
        "SE Asia", workspace_id=workspace_id, store=workspace_store
    )
    assert out_full["bbox"] == out_short["bbox"]


async def test_region_scandinavia_contains_oslo(workspace_store, workspace_id) -> None:
    out = await define_area_of_interest(
        "Scandinavia", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "region_table"
    assert _contains(out["bbox"], 10.7, 59.9)  # Oslo


async def test_region_case_insensitive_sub_saharan_africa(
    workspace_store, workspace_id
) -> None:
    out_lower = await define_area_of_interest(
        "sub-saharan africa", workspace_id=workspace_id, store=workspace_store
    )
    out_title = await define_area_of_interest(
        "Sub-Saharan Africa", workspace_id=workspace_id, store=workspace_store
    )
    assert out_lower["source"] == "region_table"
    assert out_lower["bbox"] == out_title["bbox"]


async def test_region_geojson_is_none(workspace_store, workspace_id) -> None:
    out = await define_area_of_interest(
        "Africa", workspace_id=workspace_id, store=workspace_store
    )
    assert out["geojson"] is None


async def test_region_payload_is_rematerializable(workspace_store, workspace_id) -> None:
    out = await define_area_of_interest(
        "Africa", workspace_id=workspace_id, store=workspace_store
    )
    record = await workspace_store.get_handle(workspace_id, out["handle"])
    assert record.payload["source"] == "region_table"
    assert record.payload["query"] == "Africa"
    assert record.payload["geojson"] is None
    assert len(record.payload["bbox"]) == 4


async def test_region_no_http_calls(httpx_mock, workspace_store, workspace_id) -> None:
    # Region-table hits must never call Nominatim or USGS WBD.
    out = await define_area_of_interest(
        "Middle East", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "region_table"
    assert httpx_mock.get_requests() == []


async def test_country_still_uses_nominatim(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # Country-level queries (not in the region table) must fall through to Nominatim.
    httpx_mock.add_response(json=_NOMINATIM_RESPONSE)
    out = await define_area_of_interest(
        "Brazil", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "nominatim"
    assert len(httpx_mock.get_requests()) >= 1


async def test_huc_code_still_uses_wbd(
    httpx_mock, workspace_store, workspace_id
) -> None:
    # HUC watershed codes must still route to USGS WBD, not the region table.
    httpx_mock.add_response(json=_WBD_FEATURE_COLLECTION)
    out = await define_area_of_interest(
        "14080101", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "usgs_wbd"
    assert len(httpx_mock.get_requests()) == 1
