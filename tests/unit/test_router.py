"""Harmony-first router decision (Phase 4.5 gate).

Drives the router with the saved Phase-2 union-trap fixtures:
  * tests/fixtures/tempo_no2_l2_capabilities.json  — two disjoint services
  * tests/fixtures/tempo_no2_l3_umm_c.json          — gridded, direct S3, no service

Harmony is tried first for every transform plan when wired: bbox+netcdf pins the
subsetter; bbox+png — where no single service satisfies the whole plan — routes to
Harmony unpinned (server picks chain) rather than raising NotRetrievable. The
worker's Harmony→OPeNDAP fallback catches any runtime failure. The only non-Harmony
shortcut is direct-S3 for a "data as-is" plan, and only when actually connected to
the DAAC's S3 (in-region + enabled); otherwise even data-as-is goes to Harmony.
NotRetrievable is only raised when Harmony is not wired and no other path fits.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from earthdata_mcp.config import Settings
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


def test_bbox_plus_png_with_harmony_wired_routes_to_harmony_unpinned(l2_caps) -> None:
    # Neither service handles bbox+png, but Harmony is wired — step 1.5 routes to
    # Harmony unpinned (server picks its default chain) rather than failing at
    # planning time. The worker's Harmony→OPeNDAP fallback handles any runtime failure.
    harmony = MagicMock()
    router = Router(l2_caps, harmony=harmony)
    plan = RetrievalPlan(output_format="image/png", needs_bbox=True)
    decision = router.route(plan)
    assert decision.path == "harmony"
    assert decision.service is None  # no pinned service — server picks
    assert decision.provider is harmony
    harmony.submit.assert_not_called()  # route decides, does not submit


def test_bbox_plus_png_unpinned_trace_names_first_unmet_need_per_service(l2_caps) -> None:
    # The trace is the legible "why": neither service is disqualified by a missing
    # registration, each fails the plan at a different, named point — the subsetter
    # does bbox but not png; the imagenator does png but not bbox. This is the
    # union-trap shape, so the reason category says so, never the rolled-up booleans.
    router = Router(l2_caps, harmony=MagicMock())
    plan = RetrievalPlan(output_format="image/png", needs_bbox=True)
    decision = router.route(plan)
    assert decision.trace["path"] == "harmony"
    assert decision.trace["pinned_service"] is None
    assert decision.trace["reason"] == "no_single_service_satisfies"
    by_name = {s["service_name"]: s["first_unmet_need"] for s in decision.trace["services"]}
    assert by_name[SUBSETTER] == "output_format"
    assert by_name[IMAGENATOR] == "bbox"


def test_bbox_plus_png_without_harmony_is_not_retrievable(l2_caps) -> None:
    # Without Harmony wired, no path can satisfy bbox+png — fails at planning time.
    # ``available`` reports BOTH services' real, disjoint capabilities so the agent
    # can relax the request — not the rolled-up union.
    router = Router(l2_caps)
    plan = RetrievalPlan(output_format="image/png", needs_bbox=True)
    with pytest.raises(NotRetrievable) as exc:
        router.route(plan)
    available = {s.service_name: s for s in exc.value.available}
    assert set(available) == {SUBSETTER, IMAGENATOR}
    assert available[SUBSETTER].subset_bbox is True
    assert "application/netcdf" in available[SUBSETTER].output_formats
    assert "image/png" not in available[SUBSETTER].output_formats
    assert available[IMAGENATOR].subset_bbox is False
    assert available[IMAGENATOR].output_formats == frozenset({"image/png"})


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


def test_bbox_plus_netcdf_pinned_trace_names_service_and_satisfied_needs(l2_caps) -> None:
    router = Router(l2_caps, harmony=MagicMock())
    plan = RetrievalPlan(output_format="application/netcdf", needs_bbox=True)

    decision = router.route(plan)
    assert decision.trace["path"] == "harmony"
    assert decision.trace["pinned_service"] == SUBSETTER
    assert decision.trace["pinned_concept_id"] == decision.service.concept_id
    assert decision.trace["satisfied_needs"] == ["bbox", "output_format"]


def test_variable_plus_png_routes_to_imagenator(l2_caps) -> None:
    router = Router(l2_caps, harmony=MagicMock())
    plan = RetrievalPlan(output_format="image/png", needs_variable=True)
    decision = router.route(plan)
    assert decision.path == "harmony"
    assert decision.service.service_name == IMAGENATOR


# -- direct-fetch path: L3, data as-is, only when connected to S3 -----------


def test_l3_data_as_is_routes_to_harmony_when_s3_not_connected(l3_caps) -> None:
    # "Data as-is" L3 with an S3 prefix, but S3 direct is off by default (not
    # in-region) — so we do NOT take the direct path; Harmony is tried first.
    harmony = MagicMock()
    router = Router(l3_caps, harmony=harmony, settings=Settings(s3_direct_enabled=False))
    plan = RetrievalPlan(output_format="application/netcdf")

    decision = router.route(plan)
    assert decision.path == "harmony"
    assert decision.service is None  # no services on L3 — server picks the chain
    assert decision.provider is harmony
    harmony.submit.assert_not_called()


def test_l3_data_as_is_routes_to_direct_when_s3_connected(l3_caps, monkeypatch) -> None:
    # Same data-as-is plan, but now S3 direct is enabled AND we are in the bucket's
    # region — the direct-fetch shortcut fires and skips Harmony.
    region = l3_caps.direct_s3.region
    assert region  # fixture carries a DirectDistributionInformation region
    monkeypatch.setenv("AWS_REGION", region)
    harmony = MagicMock()
    router = Router(l3_caps, harmony=harmony, settings=Settings(s3_direct_enabled=True))
    plan = RetrievalPlan(output_format="application/netcdf")

    decision = router.route(plan)
    assert decision.path == "direct"
    assert decision.service is None
    harmony.submit.assert_not_called()
    # The trace records the gate facts that fired the shortcut (in-region + enabled).
    assert decision.trace["path"] == "direct"
    assert decision.trace["region"] == region
    assert decision.trace["s3_direct_enabled"] is True


def test_l3_with_a_transform_need_routes_to_harmony_when_wired(l3_caps) -> None:
    # A transform need disqualifies direct fetch; with Harmony wired, step 1.5
    # routes to Harmony unpinned (server picks chain) before OPeNDAP/NotRetrievable.
    harmony = MagicMock()
    router = Router(l3_caps, harmony=harmony)
    plan = RetrievalPlan(output_format="application/netcdf", needs_bbox=True)
    decision = router.route(plan)
    assert decision.path == "harmony"
    assert decision.service is None  # no pinned service — server picks
    assert decision.provider is harmony


def test_l3_no_registered_services_unpinned_trace_reason(l3_caps) -> None:
    # L3 has zero CMR-registered services — distinct reason category from the
    # union-trap case (a real, disjoint-but-nonempty service list).
    router = Router(l3_caps, harmony=MagicMock())
    plan = RetrievalPlan(output_format="application/netcdf", needs_bbox=True)
    decision = router.route(plan)
    assert decision.trace["pinned_service"] is None
    assert decision.trace["reason"] == "no_registered_services"
    assert decision.trace["services"] == []


def test_l3_with_a_transform_need_is_not_retrievable_without_harmony(l3_caps) -> None:
    # Without Harmony wired and no OPeNDAP, a transform need still fails fast.
    router = Router(l3_caps)
    plan = RetrievalPlan(output_format="application/netcdf", needs_bbox=True)
    with pytest.raises(NotRetrievable):
        router.route(plan)


# -- OPeNDAP structural hook (dormant until Phase 7) -----------------------


def test_opendap_path_taken_when_harmony_not_wired(l3_caps) -> None:
    # No Harmony wired and a transform need (so not direct) — OPeNDAP is reached.
    opendap = MagicMock()
    opendap.can_handle.return_value = True
    router = Router(l3_caps, opendap=opendap)
    plan = RetrievalPlan(output_format="application/netcdf", needs_variable=True)
    decision = router.route(plan)
    assert decision.path == "opendap"
    assert decision.provider is opendap


def test_harmony_preferred_over_opendap_when_no_services(l3_caps) -> None:
    # Harmony is wired and no single service matches — step 1.5 routes to Harmony
    # unpinned before OPeNDAP even when OPeNDAP can_handle the plan.
    harmony = MagicMock()
    opendap = MagicMock()
    opendap.can_handle.return_value = True
    router = Router(l3_caps, harmony=harmony, opendap=opendap)
    plan = RetrievalPlan(output_format="application/netcdf", needs_variable=True)
    decision = router.route(plan)
    assert decision.path == "harmony"
    assert decision.service is None
    assert decision.provider is harmony


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
    assert decision.trace["path"] == "appeears"
    assert decision.trace["reason"] == "point_sample_intent"
    assert decision.trace["needs_point_sample"] is True


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
