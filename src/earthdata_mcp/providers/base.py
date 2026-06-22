"""Provider Protocols and shared retrieval types (PLAN.md §4.1).

Two Protocols, not one (PLAN.md §2/§4.1):

* :class:`MetadataProvider` — discovery and capability metadata. ``CMRProvider``
  (Phase 2) satisfies this and **only** this: a metadata provider never grows a
  throwing ``retrieve`` stub.
* :class:`RetrievalProvider` — the durable submit → poll → materialize lifecycle.
  ``HarmonyProvider``/``OPeNDAPProvider``/``AppEEARSProvider`` (Phase 4.3/7)
  satisfy this; the router composes a metadata provider with the first retrieval
  provider whose ``can_handle(plan)`` is true.

Both Protocols are ``@runtime_checkable`` so conformance can be asserted with
``isinstance`` (see ``tests/unit/test_base_protocols.py``); structural typing
checks method *names*, and the static method signatures here document the
contract.

PLAN.md §4.1 sketches the Protocols with illustrative names
(``search(SearchSpec) -> list[CollectionMeta]``); the concrete contract is shaped
to ``CMRProvider``'s real surface, which returns plain ``dict``/``list[dict]``
(``SearchSpec``/``CollectionMeta``/``AvailabilitySpec`` are not among the shared
types Phase 4.1 calls for). The four ``RetrievalProvider`` methods match §4.1.

This module is the single owner of :class:`RetrievalPlan`;
``providers/_capabilities.py`` imports and re-exports it (the reconciliation its
Phase-2 docstring promised). Imports here stay shallow — only stdlib and
``jobs.state`` — so ``_capabilities`` → ``base`` is acyclic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol, runtime_checkable

from earthdata_mcp.jobs.state import JobState

# -- Shared value types ----------------------------------------------------


@dataclass(frozen=True)
class TimeRange:
    """A closed time interval. Serializes to CMR's ``"start/end"`` temporal form."""

    start: datetime
    end: datetime

    def to_cmr(self) -> str:
        """``"2024-01-01T00:00:00Z/2024-03-31T00:00:00Z"`` — CMR ``temporal`` param."""
        return f"{_iso_z(self.start)}/{_iso_z(self.end)}"

    @classmethod
    def from_cmr(cls, value: str) -> TimeRange:
        """Parse a CMR ``"start/end"`` range back into a :class:`TimeRange`."""
        start_s, _, end_s = value.partition("/")
        if not end_s:
            raise ValueError(f"not a CMR temporal range (no '/'): {value!r}")
        return cls(start=_parse_iso(start_s), end=_parse_iso(end_s))


@dataclass(frozen=True)
class AOI:
    """An area of interest: a bbox, a GeoJSON geometry, or both.

    ``bbox`` is ``(west, south, east, north)`` in decimal degrees — the order CMR
    and Harmony both use.
    """

    bbox: tuple[float, float, float, float] | None = None
    geojson: dict | None = None

    def to_bbox_str(self) -> str:
        """``"W,S,E,N"`` — CMR's ``bounding_box`` param. Raises if no bbox is set."""
        if self.bbox is None:
            raise ValueError("AOI has no bbox to format")
        return ",".join(str(c) for c in self.bbox)


@dataclass(frozen=True)
class TransformSpec:
    """The transformations a retrieval requests of a service.

    ``output_format`` is a media type (``"application/netcdf"``, ``"image/png"``,
    …) and is what the capability gate matches against a service's
    ``output_formats``.
    """

    output_format: str
    variables: tuple[str, ...] = ()
    reproject: str | None = None
    resample: str | None = None


@dataclass(frozen=True)
class ProviderCapabilities:
    """A provider-level descriptor — *what kind* of provider this is.

    Distinct from collection-level
    :class:`~earthdata_mcp.providers._capabilities.CollectionCapabilities`, which
    describes one dataset. ``kind`` is the half of the Protocol split a provider
    implements.
    """

    name: str
    kind: Literal["metadata", "retrieval"]
    output_formats: frozenset[str] = frozenset()


@dataclass(frozen=True)
class JobRef:
    """A reference to a provider-side job the worker can poll and materialize.

    Holds only durable coordinates (the provider's job id/url and our own
    ``job_`` handle) — never staged-output URLs, which expire (PLAN.md §4.5).
    """

    provider: str
    provider_job_id: str | None = None
    provider_job_url: str | None = None
    job_handle: str | None = None


@dataclass(frozen=True)
class JobStatus:
    """The result of polling a job. ``state`` reuses the durable :class:`JobState`."""

    state: JobState
    progress: int = 0
    message: str | None = None
    output_expires_at: datetime | None = None
    error: str | None = None


@dataclass(frozen=True)
class MaterializedResult:
    """A materialized result addressed by an opaque ``StorageBackend`` key.

    ``storage_key`` is a backend-agnostic key (local FS or object store, PLAN.md
    §4.4), never a staged-output URL.
    """

    storage_key: str
    media_type: str
    size_bytes: int | None = None
    extra: dict = field(default_factory=dict)


@dataclass(frozen=True)
class RetrievalPlan:
    """What a retrieval needs — the planning object the router gates and dispatches.

    The leading ``output_format`` + ``needs_*`` flags are the capability gate
    ``CollectionCapabilities.find_service`` reads (PLAN.md §4.2); they are kept
    exactly as Phase 2 defined them so the gate and its tests are unchanged. The
    trailing fields describe the concrete request and are all defaulted, so the
    minimal gate-only form ``RetrievalPlan(output_format=..., needs_bbox=True)``
    still constructs.
    """

    output_format: str
    needs_bbox: bool = False
    needs_variable: bool = False
    needs_temporal: bool = False
    needs_shape: bool = False
    needs_dimension: bool = False
    needs_reproject: bool = False
    # Point/area-sample intent (PLAN.md §4.4): the router gates the AppEEARS path
    # on this. Defaulted False, so it never affects `find_service` (Harmony
    # services don't sample points) nor any existing gate-only RetrievalPlan.
    needs_point_sample: bool = False
    # Concrete request description (used to build the provider request).
    concept_id: str | None = None
    short_name: str | None = None
    aoi: AOI | None = None
    time_range: TimeRange | None = None
    transform: TransformSpec | None = None


# -- Protocols -------------------------------------------------------------


@runtime_checkable
class MetadataProvider(Protocol):
    """Discovery + capability metadata. Metadata only — no retrieval lifecycle."""

    async def search_collections(self, **kwargs: object) -> list[dict]: ...

    async def search_granules(
        self, collection_concept_id: str, **kwargs: object
    ) -> list[dict]: ...

    async def get_variables(self, **kwargs: object) -> list[dict]: ...

    async def get_services(self, **kwargs: object) -> list[dict]: ...

    async def check_availability(
        self, collection_concept_id: str, **kwargs: object
    ) -> dict: ...

    async def collection_capabilities(self, concept_id: str) -> object: ...

    def capabilities(self) -> ProviderCapabilities: ...


@runtime_checkable
class RetrievalProvider(Protocol):
    """The durable submit → poll → materialize lifecycle (PLAN.md §4.1, §4.3)."""

    def can_handle(self, plan: RetrievalPlan) -> bool: ...

    async def submit(self, plan: RetrievalPlan) -> JobRef: ...

    async def poll(self, job: JobRef) -> JobStatus: ...

    async def materialize(self, job: JobRef) -> MaterializedResult: ...


# -- helpers ---------------------------------------------------------------


def _iso_z(dt: datetime) -> str:
    """Render a datetime as RFC3339 with a ``Z`` suffix for naive/UTC times."""
    text = dt.isoformat()
    if dt.tzinfo is None:
        return text + "Z"
    return text.replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 instant, accepting a trailing ``Z``."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
