"""Data providers: metadata (CMR) and retrieval (Harmony/OPeNDAP/AppEEARS)."""

from __future__ import annotations

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
from earthdata_mcp.providers.router import NotRetrievable, RoutingDecision, Router

__all__ = [
    "AOI",
    "AuthError",
    "EDLAuth",
    "HarmonyProvider",
    "JobRef",
    "JobStatus",
    "MaterializedResult",
    "MetadataProvider",
    "NotRetrievable",
    "ProviderCapabilities",
    "RetrievalPlan",
    "RetrievalProvider",
    "RoutingDecision",
    "Router",
    "TimeRange",
    "TransformSpec",
]
