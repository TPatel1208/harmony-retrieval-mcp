"""``RequestSpec`` — the durable, re-materializable request spec value object.

This is the single new test surface the PRD calls for: it pins the persisted
JSONB shape (a golden dict), the round-trip through Postgres, ``to_plan()``
reconstruction, ``cache_key()`` production, and legacy tolerance — asserting
external behavior, not the names of internal helpers.
"""

from __future__ import annotations

from earthdata_mcp.providers._capabilities import (
    CollectionCapabilities,
    ServiceCapability,
)
from earthdata_mcp.providers.base import AOI, RetrievalPlan, TimeRange
from earthdata_mcp.providers.opendap import AxisGeometry
from earthdata_mcp.providers.request_spec import RequestSpec
from earthdata_mcp.providers.router import RoutingDecision

_CONCEPT_ID = "C1234567890-LPCLOUD"
_BBOX = (-105.0, 37.0, -104.0, 38.0)
_TIME = "2024-01-01/2024-03-31"


def _harmony_caps() -> CollectionCapabilities:
    return CollectionCapabilities(
        concept_id=_CONCEPT_ID,
        short_name="MOD13Q1",
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset(),
        direct_s3=None,
        services=[],
        capabilities_version="2",
    )


def _harmony_plan() -> RetrievalPlan:
    return RetrievalPlan(
        output_format="application/netcdf4",
        needs_bbox=True,
        needs_variable=False,
        needs_temporal=True,
        needs_point_sample=False,
        concept_id=_CONCEPT_ID,
        short_name="MOD13Q1",
        aoi=AOI(bbox=_BBOX),
        time_range=TimeRange.from_cmr(_TIME),
    )


def _harmony_decision() -> RoutingDecision:
    svc = ServiceCapability(service_name="l3-subsetter", concept_id="S100-LPCLOUD")
    return RoutingDecision(path="harmony", service=svc)


def test_from_plan_to_jsonb_produces_expected_durable_dict() -> None:
    spec = RequestSpec.from_plan(
        _harmony_plan(),
        decision=_harmony_decision(),
        caps=_harmony_caps(),
        workspace_id="ws-1",
        job_handle="job_abc",
        obs_handle="obs_abc",
        time_range=_TIME,
    )
    data = spec.to_jsonb()

    assert data["concept_id"] == _CONCEPT_ID
    assert data["short_name"] == "MOD13Q1"
    assert data["output_format"] == "application/netcdf4"
    assert data["output_shape"] == "grid"
    assert data["needs_bbox"] is True
    assert data["needs_variable"] is False
    assert data["needs_temporal"] is True
    assert data["needs_point_sample"] is False
    assert data["aoi_bbox"] == [-105.0, 37.0, -104.0, 38.0]
    # Byte-identical to the raw input string — NOT re-derived via TimeRange,
    # which would reformat "2024-01-01" into "2024-01-01T00:00:00Z".
    assert data["time_range"] == _TIME
    assert data["variables"] == []
    assert data["provider"] == "harmony"
    assert data["service_name"] == "l3-subsetter"
    assert data["opendap_urls"] is None
    assert data["opendap_url"] is None
    assert data["coord_lat"] is None
    assert data["coord_lon"] is None
    assert data["lat_axis"] is None
    assert data["lon_axis"] is None
    assert data["var_dims"] == {}
    assert data["workspace_id"] == "ws-1"
    assert data["job_handle"] == "job_abc"
    assert data["obs_handle"] == "obs_abc"
    assert isinstance(data["cache_key"], str) and len(data["cache_key"]) == 24

    # Re-materializable: never a staged-output URL (CLAUDE.md hard rule).
    for value in data.values():
        if isinstance(value, str):
            assert not value.lower().startswith(("http://", "https://", "s3://"))


def test_from_jsonb_round_trips_to_an_equal_spec() -> None:
    original = RequestSpec.from_plan(
        _harmony_plan(),
        decision=_harmony_decision(),
        caps=_harmony_caps(),
        workspace_id="ws-1",
        job_handle="job_abc",
        obs_handle="obs_abc",
        time_range=_TIME,
    )
    reconstructed = RequestSpec.from_jsonb(original.to_jsonb())
    assert reconstructed == original


def _grid_no_service_caps() -> CollectionCapabilities:
    return CollectionCapabilities(
        concept_id=_CONCEPT_ID,
        short_name="TEMPO_HCHO_L3",
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset(),
        direct_s3=None,
        services=[],
        capabilities_version="",
    )


def test_opendap_spec_round_trips_axis_geometry_var_dims_and_urls() -> None:
    """The DAP4 hyperslab inputs (axis geometry, per-variable dims, granule URLs)
    and the resolved (not bare-leaf) variable names all survive the round-trip —
    a resumed OPeNDAP job must reproduce the identical hyperslab constraint."""
    plan = RetrievalPlan(
        output_format="application/netcdf4",
        needs_bbox=True,
        needs_variable=True,
        needs_temporal=True,
        concept_id=_CONCEPT_ID,
        short_name="TEMPO_HCHO_L3",
        aoi=AOI(bbox=_BBOX),
        time_range=TimeRange.from_cmr(_TIME),
    )
    lat_axis = AxisGeometry(name="/product/latitude", origin=-59.875, step=0.25, length=600)
    lon_axis = AxisGeometry(name="/product/longitude", origin=-179.875, step=0.25, length=1440)
    var_dims = {"/product/vertical_column_total": (("latitude", None), ("longitude", None))}

    spec = RequestSpec.from_plan(
        plan,
        decision=RoutingDecision(path="opendap"),
        caps=_grid_no_service_caps(),
        workspace_id="ws-1",
        job_handle="job_abc",
        obs_handle="obs_abc",
        time_range=_TIME,
        # Resolved by CMR UMM-V lookup, distinct from plan.transform.variables
        # (which was built before resolution ran).
        variables=("/product/vertical_column_total",),
        opendap_urls=["https://opendap.example/g1", "https://opendap.example/g2"],
        coord_lat="/product/latitude",
        coord_lon="/product/longitude",
        lat_axis=lat_axis,
        lon_axis=lon_axis,
        var_dims=var_dims,
    )
    data = spec.to_jsonb()

    assert data["opendap_urls"] == ["https://opendap.example/g1", "https://opendap.example/g2"]
    assert data["opendap_url"] == "https://opendap.example/g1"
    assert data["coord_lat"] == "/product/latitude"
    assert data["coord_lon"] == "/product/longitude"
    assert data["lat_axis"] == {
        "name": "/product/latitude", "origin": -59.875, "step": 0.25, "length": 600,
    }
    assert data["lon_axis"] == {
        "name": "/product/longitude", "origin": -179.875, "step": 0.25, "length": 1440,
    }
    assert data["var_dims"] == {
        "/product/vertical_column_total": [["latitude", None], ["longitude", None]],
    }
    assert data["variables"] == ["/product/vertical_column_total"]

    reconstructed = RequestSpec.from_jsonb(data)
    assert reconstructed == spec
    assert reconstructed.lat_axis == lat_axis
    assert reconstructed.lon_axis == lon_axis
    assert reconstructed.var_dims == var_dims


# -- to_plan(): the worker's RetrievalPlan reconstruction --------------------


def test_to_plan_reconstructs_harmony_plan() -> None:
    spec = RequestSpec.from_plan(
        _harmony_plan(),
        decision=_harmony_decision(),
        caps=_harmony_caps(),
        workspace_id="ws-1",
        job_handle="job_abc",
        obs_handle="obs_abc",
        time_range=_TIME,
    )
    plan = RequestSpec.from_jsonb(spec.to_jsonb()).to_plan()

    assert plan.output_format == "application/netcdf4"
    assert plan.needs_bbox is True
    assert plan.needs_temporal is True
    assert plan.needs_point_sample is False
    assert plan.concept_id == _CONCEPT_ID
    assert plan.aoi.bbox == _BBOX
    assert plan.time_range == TimeRange.from_cmr(_TIME)
    assert plan.transform is None


def test_to_plan_reconstructs_opendap_plan_with_variables() -> None:
    plan_in = RetrievalPlan(
        output_format="application/netcdf4",
        needs_bbox=True,
        needs_variable=True,
        needs_temporal=True,
        concept_id=_CONCEPT_ID,
        short_name="TEMPO_HCHO_L3",
        aoi=AOI(bbox=_BBOX),
        time_range=TimeRange.from_cmr(_TIME),
    )
    spec = RequestSpec.from_plan(
        plan_in,
        decision=RoutingDecision(path="opendap"),
        caps=_grid_no_service_caps(),
        workspace_id="ws-1",
        job_handle="job_abc",
        obs_handle="obs_abc",
        time_range=_TIME,
        variables=("/product/vertical_column_total",),
        opendap_urls=["https://opendap.example/g1"],
    )
    plan = RequestSpec.from_jsonb(spec.to_jsonb()).to_plan()

    assert plan.needs_variable is True
    assert plan.transform.variables == ("/product/vertical_column_total",)
    assert plan.transform.output_format == "application/netcdf4"


def test_to_plan_reconstructs_appeears_point_sample_plan() -> None:
    plan_in = RetrievalPlan(
        output_format="application/x-parquet",
        needs_variable=True,
        needs_temporal=True,
        needs_point_sample=True,
        concept_id=_CONCEPT_ID,
        short_name="MOD13Q1",
        aoi=AOI(bbox=_BBOX),
        time_range=TimeRange.from_cmr(_TIME),
    )
    spec = RequestSpec.from_plan(
        plan_in,
        decision=RoutingDecision(path="appeears"),
        caps=_harmony_caps(),
        workspace_id="ws-1",
        job_handle="job_abc",
        obs_handle="obs_abc",
        time_range=_TIME,
        variables=("NDVI",),
    )
    plan = RequestSpec.from_jsonb(spec.to_jsonb()).to_plan()

    assert plan.needs_point_sample is True
    assert plan.output_format == "application/x-parquet"
    assert plan.transform.variables == ("NDVI",)


# -- cache_key(): golden value + round-trip stability ------------------------


def test_cache_key_matches_golden_value_and_is_stable_across_round_trip() -> None:
    spec = RequestSpec.from_plan(
        _harmony_plan(),
        decision=_harmony_decision(),
        caps=_harmony_caps(),
        workspace_id="ws-1",
        job_handle="job_abc",
        obs_handle="obs_abc",
        time_range=_TIME,
    )
    # SHA-256[:24] of "MOD13Q1:application/netcdf4:-105.0,37.0,-104.0,38.0:
    # 2024-01-01/2024-03-31::l3-subsetter:2" — same inputs/hash as the
    # pre-refactor tools.retrieval._cache_key.
    assert spec.cache_key() == "e2e12dc82576faf860c6597c"

    reconstructed = RequestSpec.from_jsonb(spec.to_jsonb())
    assert reconstructed.cache_key() == spec.cache_key()


# -- legacy tolerance ---------------------------------------------------------


def test_from_jsonb_missing_output_format_defaults_to_netcdf4() -> None:
    spec = RequestSpec.from_jsonb({"concept_id": _CONCEPT_ID, "provider": "harmony"})
    assert spec.output_format == "application/netcdf4"
    assert spec.to_plan().output_format == "application/netcdf4"


def test_from_jsonb_singular_opendap_url_promotes_to_list() -> None:
    spec = RequestSpec.from_jsonb(
        {
            "concept_id": _CONCEPT_ID,
            "provider": "opendap",
            "opendap_url": "https://opendap.example/granule",
        }
    )
    assert spec.opendap_urls == ("https://opendap.example/granule",)


def test_from_jsonb_missing_provider_defaults_to_harmony() -> None:
    spec = RequestSpec.from_jsonb({"concept_id": _CONCEPT_ID})
    assert spec.provider == "harmony"


def test_from_jsonb_missing_needs_flags_default_to_false() -> None:
    spec = RequestSpec.from_jsonb({"concept_id": _CONCEPT_ID, "provider": "harmony"})
    assert spec.needs_bbox is False
    assert spec.needs_variable is False
    assert spec.needs_temporal is False
    assert spec.needs_point_sample is False
