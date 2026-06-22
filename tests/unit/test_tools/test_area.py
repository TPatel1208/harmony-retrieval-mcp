"""``define_area_of_interest`` — bbox/GeoJSON parsing, Nominatim, handle minting.

Pure parsing is tested without any DB or HTTP; the integration tests use the real
Postgres-backed ``workspace_store`` fixture and mock Nominatim with pytest-httpx
(``httpx_mock``). The store payload is asserted to be the re-materializable spec.
"""

from __future__ import annotations

import json

import pytest

from earthdata_mcp.tools.area import (
    _bbox_from_geojson,
    _parse_bbox_string,
    define_area_of_interest,
)
from earthdata_mcp.workspace.models import HandleType, handle_type_of
from earthdata_mcp.workspace.store import CrossWorkspaceError

# Nominatim returns boundingbox as ["S", "N", "W", "E"] strings.
_NOMINATIM_RESPONSE = [
    {
        "place_id": 12345,
        "display_name": "Rocky Mountains, Colorado, USA",
        "boundingbox": ["35.0", "49.0", "-116.0", "-102.0"],
        "lat": "42.0",
        "lon": "-109.0",
    }
]
# Reordered to (W, S, E, N): W=-116.0, S=35.0, E=-102.0, N=49.0
_EXPECTED_NOMINATIM_BBOX = [-116.0, 35.0, -102.0, 49.0]


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
    httpx_mock.add_response(json=_NOMINATIM_RESPONSE)
    out = await define_area_of_interest(
        "Rocky Mountains", workspace_id=workspace_id, store=workspace_store
    )
    assert out["source"] == "nominatim"
    assert out["bbox"] == _EXPECTED_NOMINATIM_BBOX
    assert out["geojson"] is None

    record = await workspace_store.get_handle(workspace_id, out["handle"])
    assert record.payload["query"] == "Rocky Mountains"

    # The required descriptive User-Agent went out (Nominatim 403s without it).
    request = httpx_mock.get_requests()[0]
    assert "earthdata-mcp" in request.headers.get("User-Agent", "")


async def test_define_area_nominatim_no_results_raises(
    httpx_mock, workspace_store, workspace_id
) -> None:
    httpx_mock.add_response(json=[])
    with pytest.raises(ValueError, match="no results"):
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
