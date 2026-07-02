"""CollectionCapabilities + find_service — the TEMPO union-trap gate (PLAN.md §4.2).

Uses the saved real records:
  * tests/fixtures/tempo_no2_l2_capabilities.json        — two disjoint services
                                                            (the union trap)
  * tests/fixtures/tempo_no2_l3_umm_c.json                — gridded, direct S3, no
                                                            registered service
  * tests/fixtures/mcd19a2_lpcloud_capabilities.json      — no CMR-registered
                                                            services at all (the
                                                            LPCLOUD case from the
                                                            fallback-signal PRD)
  * tests/fixtures/swot_ssh_multi_service_capabilities.json — three services, each
                                                            satisfying a different
                                                            plan subset (partial
                                                            overlap, not a union
                                                            trap)
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


@pytest.fixture
def no_services_caps() -> CollectionCapabilities:
    """MCD19A2 (LPCLOUD): a real collection with zero CMR-registered services."""
    data = json.loads((_FIXTURES / "mcd19a2_lpcloud_capabilities.json").read_text())
    return CollectionCapabilities.from_harmony_capabilities(data)


@pytest.fixture
def multi_service_caps() -> CollectionCapabilities:
    """SWOT SSH (POCLOUD): three services, each satisfying a different plan
    subset — a subsetter (bbox+variable+temporal, netCDF), a reprojector
    (bbox+reproject, tiff/png), and a concatenator (netCDF only). No single
    service does everything, but this is heterogeneity, not a union trap."""
    data = json.loads((_FIXTURES / "swot_ssh_multi_service_capabilities.json").read_text())
    return CollectionCapabilities.from_harmony_capabilities(data)


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


# -- fixture corpus: heterogeneous real DAAC registrations ------------------


def test_no_registered_services_finds_no_service_for_any_plan(
    no_services_caps: CollectionCapabilities,
) -> None:
    # MCD19A2/LPCLOUD: zero services means every plan is unsatisfiable by any
    # single service — distinct from the union-trap case (a nonempty, disjoint
    # service list). Both correctly yield None from find_service.
    assert no_services_caps.services == []
    plan = RetrievalPlan(output_format="application/netcdf", needs_bbox=True)
    assert no_services_caps.find_service(plan) is None


def test_three_services_parsed_from_multi_service_fixture(
    multi_service_caps: CollectionCapabilities,
) -> None:
    names = {s.service_name for s in multi_service_caps.services}
    assert names == {"podaac/l2ss-py", "harmony/gdal", "podaac/concise"}


def test_multi_service_bbox_variable_temporal_netcdf_routes_to_subsetter(
    multi_service_caps: CollectionCapabilities,
) -> None:
    plan = RetrievalPlan(
        output_format="application/netcdf",
        needs_bbox=True,
        needs_variable=True,
        needs_temporal=True,
    )
    svc = multi_service_caps.find_service(plan)
    assert svc is not None
    assert svc.service_name == "podaac/l2ss-py"


def test_multi_service_bbox_reproject_tiff_routes_to_gdal(
    multi_service_caps: CollectionCapabilities,
) -> None:
    plan = RetrievalPlan(output_format="image/tiff", needs_bbox=True, needs_reproject=True)
    svc = multi_service_caps.find_service(plan)
    assert svc is not None
    assert svc.service_name == "harmony/gdal"


def test_multi_service_partial_overlap_satisfied_by_no_single_service(
    multi_service_caps: CollectionCapabilities,
) -> None:
    # Real heterogeneity, not a union trap: the subsetter does bbox+variable but
    # not reproject; gdal reprojects but doesn't variable-subset; concise doesn't
    # subset at all. No single service does bbox+variable+reproject together.
    plan = RetrievalPlan(
        output_format="application/netcdf",
        needs_bbox=True,
        needs_variable=True,
        needs_reproject=True,
    )
    assert multi_service_caps.find_service(plan) is None


def test_rolled_up_union_booleans_are_never_consulted(l2_caps: CollectionCapabilities) -> None:
    """The TEMPO L2 fixture's top-level booleans claim bbox+png support that no
    single service provides — the union trap `find_service` already declines.
    This test pins the *mechanism*: re-parse the same fixture with those exact
    top-level booleans flipped to the opposite lie (falsely claiming NO support
    at all), and the per-service truth still wins. If `find_service` or the merge
    ever started reading the rolled-up union instead of each service's own block,
    this would flip to a false negative and fail.
    """
    data = json.loads((_FIXTURES / "tempo_no2_l2_capabilities.json").read_text())
    lying_top_level = {
        **data,
        "bboxSubset": False,
        "variableSubset": False,
        "temporalSubset": False,
        "shapeSubset": False,
        "concatenate": False,
        "outputFormats": [],
    }
    caps = CollectionCapabilities.from_harmony_capabilities(lying_top_level)
    plan = RetrievalPlan(output_format="application/netcdf", needs_bbox=True)
    svc = caps.find_service(plan)
    assert svc is not None
    assert svc.service_name == "l2-subsetter-batchee-stitchee-concise"
