"""Harmony-first router.

Picks **one** retrieval path for a :class:`RetrievalPlan`, gating on the merged
:class:`CollectionCapabilities`. The gate consults
``CollectionCapabilities.find_service`` (one whole service or ``None``) and
``.direct_s3`` — this module composes them into the decision, it never
re-implements capability matching and never unions across services.

``route`` is a **pure planning decision**: it decides a path and returns a
:class:`RoutingDecision`. It never submits.

**Harmony is always tried first** for any plan that needs a transform: when a
single service matches it is pinned; when none matches (the union-trap case, or a
collection with no CMR-registered services) Harmony is submitted *unpinned* and the
server picks its default chain. OPeNDAP is **not** chosen at planning time in the
normal case — it is the worker's runtime fallback when a real Harmony submit fails.
The plan-time OPeNDAP and ``NotRetrievable`` paths below remain only as a structural
fallback for when Harmony is not wired at all.

The one non-Harmony shortcut is **direct-S3** for a "data as-is" (no-transform)
plan — and only when we are actually connected to the DAAC's S3 (in-region and
enabled). Otherwise even a data-as-is plan goes to Harmony.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal

from earthdata_mcp.config import Settings, get_settings
from earthdata_mcp.providers._capabilities import (
    CollectionCapabilities,
    S3DirectAccess,
    ServiceCapability,
)
from earthdata_mcp.providers.base import RetrievalPlan, RetrievalProvider

__all__ = ["NotRetrievable", "RoutingDecision", "Router"]


class NotRetrievable(Exception):
    """No path can satisfy the plan — raised at planning time.

    Carries ``available`` (the per-service capabilities) so the agent can relax
    the request ("png only without a bbox; want full-scene png or a bbox subset
    as netCDF?") instead of failing opaquely at submit time.
    """

    def __init__(self, reason: str, available: list[ServiceCapability]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.available = available


@dataclass(frozen=True)
class RoutingDecision:
    """The chosen retrieval path. ``service``/``provider`` set where they apply.

    ``trace`` explains *why* — computed from the capabilities already in hand, so
    it costs zero extra network calls and never changes the decision itself. For a
    pinned service it names the service and the plan needs it satisfied; when
    Harmony is submitted unpinned it lists, per registered service, the first plan
    need that service failed to satisfy, plus a reason category distinguishing
    "no single service matched" from "no services are registered at all". For the
    direct-S3 and AppEEARS shortcuts it records the gate facts that fired them.
    """

    path: Literal["harmony", "direct", "opendap", "appeears"]
    service: ServiceCapability | None = None
    provider: RetrievalProvider | None = None
    trace: dict = field(default_factory=dict)


class Router:
    """Routes a plan to exactly one path against one collection's capabilities."""

    def __init__(
        self,
        capabilities: CollectionCapabilities,
        *,
        harmony: RetrievalProvider | None = None,
        opendap: RetrievalProvider | None = None,
        appeears: RetrievalProvider | None = None,
        settings: Settings | None = None,
    ) -> None:
        self._caps = capabilities
        self._harmony = harmony
        self._opendap = opendap
        self._appeears = appeears
        self._settings = settings or get_settings()

    def route(self, plan: RetrievalPlan) -> RoutingDecision:
        """Decide one retrieval path — Harmony-first. Decide only, never submit."""
        # 0. A point/area sample is a tabular request: AppEEARS owns it, and it must
        #    not be forced through Harmony's gridded cube path. Gated on the plan's
        #    point-sample intent, so non-point plans skip it entirely.
        if self._appeears is not None and self._appeears.can_handle(plan):
            return RoutingDecision(
                "appeears",
                provider=self._appeears,
                trace={
                    "path": "appeears",
                    "reason": "point_sample_intent",
                    "needs_point_sample": plan.needs_point_sample,
                },
            )

        # 1. "Data as-is" (no transform) AND we are actually connected to the DAAC's
        #    S3 (in-region + enabled) → direct fetch, skip Harmony. When S3 is not
        #    reachable we fall through to Harmony rather than route to a path we
        #    cannot execute here.
        if (
            _is_data_as_is(plan)
            and self._caps.direct_s3 is not None
            and _s3_connected(self._caps.direct_s3, self._settings)
        ):
            return RoutingDecision(
                "direct",
                trace={
                    "path": "direct",
                    "reason": "data_as_is_in_region_s3",
                    "region": self._caps.direct_s3.region,
                    "s3_direct_enabled": self._settings.s3_direct_enabled,
                },
            )

        # 2. Harmony is always tried first for everything else. Pin the matched
        #    service when one satisfies the whole plan; otherwise submit unpinned and
        #    let the Harmony server pick its chain (covers the union-trap case and
        #    collections with no CMR-registered services). OPeNDAP is the worker's
        #    runtime fallback if this real submit fails.
        if self._harmony is not None:
            svc = self._caps.find_service(plan)
            trace = _service_trace(self._caps, plan, svc)
            trace["path"] = "harmony"
            return RoutingDecision("harmony", service=svc, provider=self._harmony, trace=trace)

        # --- Structural fallbacks below: only reachable when Harmony is NOT wired. ---

        # 3. A pinned single service still routes to Harmony's slot if one matches.
        svc = self._caps.find_service(plan)
        if svc is not None:
            trace = _service_trace(self._caps, plan, svc)
            trace["path"] = "harmony"
            return RoutingDecision("harmony", service=svc, provider=self._harmony, trace=trace)

        # 4. An OPeNDAP provider that can handle the plan.
        if self._opendap is not None and self._opendap.can_handle(plan):
            return RoutingDecision(
                "opendap",
                provider=self._opendap,
                trace={
                    "path": "opendap",
                    "reason": "harmony_not_wired",
                    "harmony_unmatched": _service_trace(self._caps, plan, None),
                },
            )

        # 5. Nothing satisfies it → fail fast at planning time.
        raise NotRetrievable(
            reason=(
                "no single service satisfies this request and no retrieval path "
                "is available; relax the request to match one service's capabilities"
            ),
            available=self._caps.services,
        )


def _service_trace(
    caps: CollectionCapabilities, plan: RetrievalPlan, svc: ServiceCapability | None
) -> dict:
    """Explain a Harmony service match/non-match, computed from data in hand.

    A match names the pinned service and the plan needs it satisfied. A non-match
    names, per registered service, the first plan need that service failed to
    satisfy (mirroring ``find_service``'s own check order — this never
    re-implements the gate, only narrates it), plus a reason distinguishing "no
    single service matched" (the union-trap shape) from "no services are
    registered at all". Never reads the rolled-up top-level union booleans.
    """
    if svc is not None:
        return {
            "pinned_service": svc.service_name,
            "pinned_concept_id": svc.concept_id,
            "satisfied_needs": _plan_need_names(plan),
        }
    reason = "no_registered_services" if not caps.services else "no_single_service_satisfies"
    return {
        "pinned_service": None,
        "reason": reason,
        "services": [
            {"service_name": s.service_name, "first_unmet_need": _first_unmet_need(s, plan)}
            for s in caps.services
        ],
    }


def _plan_need_names(plan: RetrievalPlan) -> list[str]:
    """The plan needs a matched service satisfied, in ``find_service``'s check order."""
    needs = []
    if plan.needs_bbox:
        needs.append("bbox")
    if plan.needs_variable:
        needs.append("variable")
    if plan.needs_temporal:
        needs.append("temporal")
    if plan.needs_shape:
        needs.append("shape")
    if plan.needs_dimension:
        needs.append("dimension")
    if plan.needs_reproject:
        needs.append("reproject")
    needs.append("output_format")
    return needs


def _first_unmet_need(service: ServiceCapability, plan: RetrievalPlan) -> str | None:
    """The first plan need ``service`` fails to satisfy, or ``None`` if it matches.

    Mirrors ``CollectionCapabilities.find_service``'s check order exactly — this
    is a narration of that gate, not a second implementation of it.
    """
    if plan.needs_bbox and not service.subset_bbox:
        return "bbox"
    if plan.needs_variable and not service.subset_variable:
        return "variable"
    if plan.needs_temporal and not service.subset_temporal:
        return "temporal"
    if plan.needs_shape and not service.subset_shape:
        return "shape"
    if plan.needs_dimension and not service.subset_dimension:
        return "dimension"
    if plan.needs_reproject and not service.reproject:
        return "reproject"
    if plan.output_format not in service.output_formats:
        return "output_format"
    return None


def _is_data_as_is(plan: RetrievalPlan) -> bool:
    """True only when the plan requests NO transforms — every ``needs_*`` is False.

    Any single transform need (bbox, variable, temporal, shape, dimension,
    reproject) disqualifies the direct-fetch path: direct fetch returns granules
    untouched, so a plan that needs *any* transform must go through a service.
    """
    return not (
        plan.needs_bbox
        or plan.needs_variable
        or plan.needs_temporal
        or plan.needs_shape
        or plan.needs_dimension
        or plan.needs_reproject
    )


def _s3_connected(direct_s3: S3DirectAccess, settings: Settings) -> bool:
    """True only when the direct-S3 shortcut is enabled AND we are in-region.

    DAAC direct-S3 reads are usable only from within the bucket's AWS region. We
    require both an explicit opt-in (``s3_direct_enabled``) and a region match
    between the collection's ``DirectDistributionInformation`` region and the
    process's AWS region (``AWS_REGION`` / ``AWS_DEFAULT_REGION``). A missing
    region on either side is treated as "not connected", so the default — off and
    not in-region — always routes through Harmony.
    """
    if not settings.s3_direct_enabled:
        return False
    want = (direct_s3.region or "").strip().lower()
    if not want:
        return False
    have = (os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "")
    return want == have.strip().lower()
