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

__all__ = [
    "AOI",
    "AuthError",
    "EDLAuth",
    "JobRef",
    "JobStatus",
    "MaterializedResult",
    "MetadataProvider",
    "ProviderCapabilities",
    "RetrievalPlan",
    "RetrievalProvider",
    "TimeRange",
    "TransformSpec",
]
