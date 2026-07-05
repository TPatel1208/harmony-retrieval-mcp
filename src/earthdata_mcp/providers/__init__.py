"""Data providers: metadata (CMR) and retrieval (Harmony/OPeNDAP/AppEEARS)."""

from __future__ import annotations

from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.appeears import AppEEARSProvider
from earthdata_mcp.providers.auth import AuthError, EDLAuth
from earthdata_mcp.providers.base import (
    AOI,
    JobRef,
    JobStatus,
    MaterializedResult,
    MetadataProvider,
    ProviderCapabilities,
    RetrievalPlan,
    RetrievalProvider,
    TimeRange,
    TransformSpec,
)
from earthdata_mcp.providers.harmony import HarmonyProvider
from earthdata_mcp.providers.opendap import OPeNDAPProvider
from earthdata_mcp.providers.request_spec import RequestSpec
from earthdata_mcp.providers.router import NotRetrievable, RoutingDecision, Router

__all__ = [
    "AOI",
    "AppEEARSProvider",
    "AuthError",
    "EDLAuth",
    "HarmonyProvider",
    "JobRef",
    "JobStatus",
    "MaterializedResult",
    "MetadataProvider",
    "NotRetrievable",
    "OPeNDAPProvider",
    "ProviderCapabilities",
    "RequestSpec",
    "RetrievalPlan",
    "RetrievalProvider",
    "RoutingDecision",
    "Router",
    "TimeRange",
    "TransformSpec",
    "build",
]


def build(spec: RequestSpec, caps: CollectionCapabilities) -> RetrievalProvider:
    """The single spec -> :class:`RetrievalProvider` seam (durable worker side).

    Bound to the caller's freshly-fetched ``caps`` so ``find_service`` re-checks
    the matched service rather than trusting the spec blindly. An unknown
    ``spec.provider`` fails loud — a job is never silently mis-driven by a
    provider another provider planned it for.
    """
    if spec.provider == "harmony":
        return HarmonyProvider(caps, service_name_hint=spec.service_name)
    if spec.provider == "appeears":
        return AppEEARSProvider(caps)
    if spec.provider == "opendap":
        return OPeNDAPProvider(
            caps,
            opendap_urls=list(spec.opendap_urls),
            # ``None`` is a real, meaningful value here (the DMR-fallback case: no
            # single global coordinate pair, each variable's own is in
            # ``var_coords`` instead) — never coerce it to a literal "lat"/"lon",
            # which is wrong for any collection whose real coordinate variables
            # are named differently.
            coord_lat=spec.coord_lat,
            coord_lon=spec.coord_lon,
            coord_time=spec.coord_time,
            lat_axis=spec.lat_axis,
            lon_axis=spec.lon_axis,
            var_dims=spec.var_dims,
            var_coords=spec.var_coords,
        )
    raise ValueError(f"no retrieval provider for spec provider {spec.provider!r}")
