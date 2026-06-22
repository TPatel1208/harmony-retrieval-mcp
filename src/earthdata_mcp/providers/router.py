"""Capability-gated router (PLAN.md §4.2 decision tree). No Harmony fallback.

Picks **one** retrieval path for a :class:`RetrievalPlan`, gating on the merged
:class:`CollectionCapabilities`. The gate's source of truth is
``CollectionCapabilities.find_service`` (one whole service or ``None``) and
``.direct_s3`` — this module composes them into the §4.2 tree, it never
re-implements capability matching and never unions across services.

``route`` is a **pure planning decision**: it decides a path and returns a
:class:`RoutingDecision`, or raises :class:`NotRetrievable` at planning time. It
never submits — so an unserviceable plan can never reach a Harmony submit, and a
"data as-is" plan is dispatched to direct fetch without touching Harmony.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from earthdata_mcp.providers._capabilities import (
    CollectionCapabilities,
    ServiceCapability,
)
from earthdata_mcp.providers.base import RetrievalPlan, RetrievalProvider

__all__ = ["NotRetrievable", "RoutingDecision", "Router"]


class NotRetrievable(Exception):
    """No path can satisfy the plan — raised at planning time (PLAN.md §4.2).

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
    """The chosen retrieval path. ``service``/``provider`` set where they apply."""

    path: Literal["harmony", "direct", "opendap", "appeears"]
    service: ServiceCapability | None = None
    provider: RetrievalProvider | None = None


class Router:
    """Routes a plan to exactly one path against one collection's capabilities."""

    def __init__(
        self,
        capabilities: CollectionCapabilities,
        *,
        harmony: RetrievalProvider | None = None,
        opendap: RetrievalProvider | None = None,
        appeears: RetrievalProvider | None = None,
    ) -> None:
        self._caps = capabilities
        self._harmony = harmony
        self._opendap = opendap
        self._appeears = appeears

    def route(self, plan: RetrievalPlan) -> RoutingDecision:
        """Apply the §4.2 decision tree. Decide only — never submit."""
        # 0. A point/area sample is a tabular request: AppEEARS owns it, and it must
        #    not be forced through Harmony's gridded cube path (PLAN.md §4.4). Gated
        #    on the plan's point-sample intent, so non-point plans skip it entirely.
        if self._appeears is not None and self._appeears.can_handle(plan):
            return RoutingDecision("appeears", provider=self._appeears)

        # 1. A single Harmony service satisfies the whole plan → use that service.
        svc = self._caps.find_service(plan)
        if svc is not None:
            return RoutingDecision("harmony", service=svc, provider=self._harmony)

        # 2. "Data as-is" with in-region S3 direct access → direct fetch, skip
        #    Harmony. A COMPLETE gridded netCDF-4 L3 collection with an S3 prefix
        #    is this case.
        if _is_data_as_is(plan) and self._caps.direct_s3 is not None:
            return RoutingDecision("direct")

        # 3. An OPeNDAP provider that can handle the plan (Phase 7; dormant until
        #    one is wired). Structural hook, not a Harmony fallback.
        if self._opendap is not None and self._opendap.can_handle(plan):
            return RoutingDecision("opendap", provider=self._opendap)

        # 4. Nothing satisfies it → fail fast at planning time. Never fall back
        #    into Harmony with a request no single service can fulfill.
        raise NotRetrievable(
            reason=(
                "no single service satisfies this request and no direct-fetch path "
                "is available; relax the request to match one service's capabilities"
            ),
            available=self._caps.services,
        )


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
