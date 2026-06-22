"""Capability-gated router decision tree (PLAN.md §4.2, Phase 4.5 gate).

Drives the router with the saved Phase-2 union-trap fixtures:
  * tests/fixtures/tempo_no2_l2_capabilities.json  — two disjoint services
  * tests/fixtures/tempo_no2_l3_umm_c.json          — gridded, direct S3, no service

The load-bearing assertions: bbox+png is NotRetrievable (neither service does
both) with both services reported as ``available``; bbox+netcdf routes to the
subsetter; the L3 direct-S3 case routes to direct fetch and **never** submits to
Harmony.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.base import RetrievalPlan
from earthdata_mcp.providers.router import NotRetrievable, Router

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"

SUBSETTER = "l2-subsetter-batchee-stitchee-concise"
IMAGENATOR = "asdc/imagenator_l2"


@pytest.fixture
def l2_caps() -> CollectionCapabilities:
    data = json.loads((_FIXTURES / "tempo_no2_l2_capabilities.json").read_text())
    return CollectionCapabilities.from_harmony_capabilities(data)


@pytest.fixture
def l3_caps() -> CollectionCapabilities:
    umm_c = json.loads((_FIXTURES / "tempo_no2_l3_umm_c.json").read_text())
    return CollectionCapabilities.from_harmony_capabilities({}, umm_c)


# -- the union trap: bbox + png is satisfiable by NEITHER service ----------


def test_bbox_plus_png_is_not_retrievable_and_never_submits(l2_caps) -> None:
    harmony = MagicMock()
    router = Router(l2_caps, harmony=harmony)
    plan = RetrievalPlan(output_format="image/png", needs_bbox=True)

    with pytest.raises(NotRetrievable) as exc:
        router.route(plan)

    # `available` reports BOTH services' real, disjoint capabilities so the agent
    # can relax the request — not the rolled-up union.
    available = {s.service_name: s for s in exc.value.available}
    assert set(available) == {SUBSETTER, IMAGENATOR}
    assert available[SUBSETTER].subset_bbox is True
    assert "application/netcdf" in available[SUBSETTER].output_formats
    assert "image/png" not in available[SUBSETTER].output_formats
    assert available[IMAGENATOR].subset_bbox is False
    assert available[IMAGENATOR].output_formats == frozenset({"image/png"})

    # Routing decided NOT to submit anything — the planning-time failure is the
    # whole point (no opaque submit-time failure, no Harmony fallback).
    harmony.submit.assert_not_called()


def test_bbox_plus_netcdf_routes_to_subsetter(l2_caps) -> None:
    harmony = MagicMock()
    router = Router(l2_caps, harmony=harmony)
    plan = RetrievalPlan(output_format="application/netcdf", needs_bbox=True)

    decision = router.route(plan)
    assert decision.path == "harmony"
    assert decision.service is not None
    assert decision.service.service_name == SUBSETTER
    assert decision.provider is harmony
    harmony.submit.assert_not_called()  # route decides, it does not submit


def test_variable_plus_png_routes_to_imagenator(l2_caps) -> None:
    router = Router(l2_caps, harmony=MagicMock())
    plan = RetrievalPlan(output_format="image/png", needs_variable=True)
    decision = router.route(plan)
    assert decision.path == "harmony"
    assert decision.service.service_name == IMAGENATOR


# -- direct-fetch path: L3, data as-is, in-region S3 -----------------------


def test_l3_data_as_is_routes_to_direct_fetch_no_harmony(l3_caps) -> None:
    harmony = MagicMock()
    router = Router(l3_caps, harmony=harmony)
    # "Data as-is": no transforms requested; the COMPLETE gridded netCDF-4 L3
    # collection with an S3 prefix is the direct-fetch case.
    plan = RetrievalPlan(output_format="application/netcdf")

    decision = router.route(plan)
    assert decision.path == "direct"
    assert decision.service is None
    harmony.submit.assert_not_called()


def test_l3_with_a_transform_need_is_not_direct(l3_caps) -> None:
    # A transform need disqualifies direct fetch; with no service and no OPeNDAP,
    # the plan is NotRetrievable rather than silently routed to direct.
    router = Router(l3_caps, harmony=MagicMock())
    plan = RetrievalPlan(output_format="application/netcdf", needs_bbox=True)
    with pytest.raises(NotRetrievable):
        router.route(plan)


# -- OPeNDAP structural hook (dormant until Phase 7) -----------------------


def test_opendap_path_taken_when_provider_can_handle(l3_caps) -> None:
    # No Harmony service and a transform need (so not direct) — but an OPeNDAP
    # provider that can_handle the plan is selected before NotRetrievable.
    opendap = MagicMock()
    opendap.can_handle.return_value = True
    router = Router(l3_caps, harmony=MagicMock(), opendap=opendap)
    plan = RetrievalPlan(output_format="application/netcdf", needs_variable=True)
    decision = router.route(plan)
    assert decision.path == "opendap"
    assert decision.provider is opendap


# -- AppEEARS point/area-sample path (Phase 7.4) ---------------------------


def test_point_sample_routes_to_appeears_before_harmony(l2_caps) -> None:
    # A point/area sample is tabular: it goes to AppEEARS even when a Harmony
    # service could match — it must not be forced through the gridded cube path.
    appeears = MagicMock()
    appeears.can_handle.return_value = True
    harmony = MagicMock()
    router = Router(l2_caps, harmony=harmony, appeears=appeears)
    plan = RetrievalPlan(
        output_format="application/x-parquet",
        needs_bbox=True,
        needs_point_sample=True,
    )

    decision = router.route(plan)
    assert decision.path == "appeears"
    assert decision.provider is appeears
    harmony.submit.assert_not_called()


def test_non_point_plan_does_not_hijack_to_appeears(l2_caps) -> None:
    # AppEEARS is consulted but declines a non-point plan, so routing falls
    # through to the normal Harmony decision tree.
    appeears = MagicMock()
    appeears.can_handle.return_value = False
    router = Router(l2_caps, harmony=MagicMock(), appeears=appeears)
    plan = RetrievalPlan(output_format="application/netcdf", needs_bbox=True)

    decision = router.route(plan)
    assert decision.path == "harmony"
    assert decision.service.service_name == SUBSETTER
