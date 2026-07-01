"""CollectionCapabilities + find_service — the TEMPO union-trap gate (PLAN.md §4.2).

Uses the saved real records:
  * tests/fixtures/tempo_no2_l2_capabilities.json  — two disjoint services
  * tests/fixtures/tempo_no2_l3_umm_c.json          — gridded, direct S3, no service
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from earthdata_mcp.providers._capabilities import (
    CollectionCapabilities,
    RetrievalPlan,
)

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.fixture
def l2_caps() -> CollectionCapabilities:
    data = json.loads((_FIXTURES / "tempo_no2_l2_capabilities.json").read_text())
    return CollectionCapabilities.from_harmony_capabilities(data)


@pytest.fixture
def l3_caps() -> CollectionCapabilities:
    umm_c = json.loads((_FIXTURES / "tempo_no2_l3_umm_c.json").read_text())
    return CollectionCapabilities.from_harmony_capabilities({}, umm_c)


def test_bbox_plus_png_satisfied_by_no_service(l2_caps: CollectionCapabilities) -> None:
    # The union advertises bbox AND png, but neither service does both.
    plan = RetrievalPlan(output_format="image/png", needs_bbox=True)
    assert l2_caps.find_service(plan) is None


def test_bbox_plus_netcdf_routes_to_subsetter(l2_caps: CollectionCapabilities) -> None:
    plan = RetrievalPlan(output_format="application/netcdf", needs_bbox=True)
    svc = l2_caps.find_service(plan)
    assert svc is not None
    assert svc.service_name == "l2-subsetter-batchee-stitchee-concise"


def test_variable_plus_png_routes_to_imagenator(l2_caps: CollectionCapabilities) -> None:
    plan = RetrievalPlan(output_format="image/png", needs_variable=True)
    svc = l2_caps.find_service(plan)
    assert svc is not None
    assert svc.service_name == "asdc/imagenator_l2"


def test_l3_is_gridded_with_direct_s3_and_provisional_advisory(
    l3_caps: CollectionCapabilities,
) -> None:
    assert l3_caps.output_shape == "grid"
    assert l3_caps.direct_s3 is not None
    assert l3_caps.direct_s3.region == "us-west-2"
    assert any("provisional" in note.lower() for note in l3_caps.advisory)


def test_two_services_parsed_from_l2_fixture(l2_caps: CollectionCapabilities) -> None:
    names = {s.service_name for s in l2_caps.services}
    assert names == {"l2-subsetter-batchee-stitchee-concise", "asdc/imagenator_l2"}


def test_collection_capabilities_carries_version() -> None:
    caps = CollectionCapabilities.from_harmony_capabilities(
        {}, {"Version": "061", "ShortName": "MOD13Q1"}
    )
    assert caps.version == "061"


def test_spatial_extent_parses_bounding_rectangle() -> None:
    umm_c = {
        "SpatialExtent": {
            "HorizontalSpatialDomain": {
                "Geometry": {
                    "BoundingRectangles": [
                        {
                            "WestBoundingCoordinate": -179.875,
                            "SouthBoundingCoordinate": -59.875,
                            "EastBoundingCoordinate": 179.875,
                            "NorthBoundingCoordinate": 89.875,
                        }
                    ]
                }
            }
        }
    }
    caps = CollectionCapabilities.from_harmony_capabilities({}, umm_c)
    assert caps.spatial_extent == (-179.875, -59.875, 179.875, 89.875)


def test_spatial_extent_falls_back_to_gpolygon_bbox(
    l3_caps: CollectionCapabilities,
) -> None:
    # tempo_no2_l3_umm_c.json has GPolygons, no BoundingRectangles (see module
    # docstring fixture list).
    assert l3_caps.spatial_extent == (-170.0, 10.0, -10.0, 80.0)


def test_spatial_extent_none_without_geometry() -> None:
    caps = CollectionCapabilities.from_harmony_capabilities({}, {"ShortName": "X"})
    assert caps.spatial_extent is None
