"""Merged ``CollectionCapabilities`` view (PLAN.md §4.2).

Collection metadata is two layers from two fetches, merged into one view:

* **Layer 1 — UMM-C** (``cmr.collection_capabilities`` collection fetch): what the
  data *is* and how to get it *raw* — processing level (→ output shape), native
  formats, in-region S3 direct access, and maturity → advisory.
* **Layer 2 — Harmony ``/capabilities``** (the per-service capability block): what
  transforms are possible and *through which service*.

**The trap:** the rolled-up top-level booleans (``bboxSubset``, ``outputFormats``,
…) are an unsatisfiable *union* across disjoint services — a real TEMPO_NO2_L2
record advertises ``bbox`` + ``png`` that *no single service* can do. We never
read them. ``find_service`` matches **one whole service** or returns ``None``.

The per-service ``capabilities`` block maps 1:1 onto :class:`ServiceCapability`;
the top-level union booleans are deliberately ignored.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class S3DirectAccess:
    """In-region S3 direct access from UMM-C ``DirectDistributionInformation``."""

    region: str
    bucket_prefixes: tuple[str, ...]
    credentials_endpoint: str | None = None


@dataclass(frozen=True)
class ServiceCapability:
    """One Harmony service's capabilities, parsed per-service (never the union)."""

    service_name: str
    concept_id: str
    subset_bbox: bool = False
    subset_variable: bool = False
    subset_temporal: bool = False
    subset_shape: bool = False
    subset_dimension: bool = False
    concatenate: bool = False
    reproject: bool = False
    output_formats: frozenset[str] = frozenset()


@dataclass(frozen=True)
class RetrievalPlan:
    """What a retrieval needs, used to gate against a single service.

    Phase 4 owns the canonical ``RetrievalPlan`` in ``providers/base.py``; this is
    the minimal version the capability gate needs and is reconciled there.
    """

    output_format: str
    needs_bbox: bool = False
    needs_variable: bool = False
    needs_temporal: bool = False
    needs_shape: bool = False
    needs_dimension: bool = False
    needs_reproject: bool = False


@dataclass
class CollectionCapabilities:
    """UMM-C (Layer 1) merged with per-service Harmony capabilities (Layer 2)."""

    concept_id: str
    short_name: str
    processing_level: str
    output_shape: Literal["grid", "swath", "point"]
    native_formats: frozenset[str]
    direct_s3: S3DirectAccess | None
    services: list[ServiceCapability]
    capabilities_version: str = ""
    advisory: list[str] = field(default_factory=list)

    def find_service(self, plan: RetrievalPlan) -> ServiceCapability | None:
        """A SINGLE service must satisfy the ENTIRE plan. Never union across services."""
        for s in self.services:
            if plan.needs_bbox and not s.subset_bbox:
                continue
            if plan.needs_variable and not s.subset_variable:
                continue
            if plan.needs_temporal and not s.subset_temporal:
                continue
            if plan.needs_shape and not s.subset_shape:
                continue
            if plan.needs_dimension and not s.subset_dimension:
                continue
            if plan.needs_reproject and not s.reproject:
                continue
            if plan.output_format not in s.output_formats:
                continue
            return s
        return None

    @classmethod
    def from_harmony_capabilities(
        cls, harmony_caps: dict, umm_c: dict | None = None
    ) -> CollectionCapabilities:
        """Build the merged view from a Harmony ``/capabilities`` response.

        ``umm_c`` is the optional Layer-1 record; when absent (e.g. the L2
        union-trap fixture, which only exercises ``find_service``) the UMM-C
        layer falls back to defaults.
        """
        layer1 = umm_c_layer(umm_c or {})
        return cls(
            concept_id=harmony_caps.get("conceptId")
            or layer1["concept_id"],
            short_name=harmony_caps.get("shortName") or layer1["short_name"],
            processing_level=layer1["processing_level"],
            output_shape=layer1["output_shape"],
            native_formats=layer1["native_formats"],
            direct_s3=layer1["direct_s3"],
            services=parse_service_capabilities(harmony_caps),
            capabilities_version=str(harmony_caps.get("capabilitiesVersion", "")),
            advisory=layer1["advisory"],
        )


def parse_service_capabilities(harmony_caps: dict) -> list[ServiceCapability]:
    """Parse one :class:`ServiceCapability` per service from a Harmony response.

    Reads each service's own ``capabilities`` block; the top-level union booleans
    are ignored on purpose (see module docstring).
    """
    services: list[ServiceCapability] = []
    for svc in harmony_caps.get("services", []):
        caps = svc.get("capabilities", {})
        subset = caps.get("subsetting", {})
        services.append(
            ServiceCapability(
                service_name=svc.get("name", ""),
                concept_id=_concept_id_from_href(svc.get("href", "")),
                subset_bbox=bool(subset.get("bbox", False)),
                subset_variable=bool(subset.get("variable", False)),
                subset_temporal=bool(subset.get("temporal", False)),
                subset_shape=bool(subset.get("shape", False)),
                subset_dimension=bool(subset.get("dimension", False)),
                concatenate=bool(caps.get("concatenation", False)),
                reproject=bool(caps.get("reprojection", False)),
                output_formats=frozenset(caps.get("output_formats", [])),
            )
        )
    return services


def umm_c_layer(umm_c: dict) -> dict:
    """Extract the Layer-1 fields the merge needs from a UMM-C record.

    Reads the three fields NASA's ``normalize_collection_item`` omits at the
    pinned commit (``DirectDistributionInformation``, ``StandardProduct``,
    ``Purpose``) straight from UMM-C — canon is the schema, not NASA's repo.
    """
    processing_level = str(
        (umm_c.get("ProcessingLevel") or {}).get("Id", "") or ""
    )
    return {
        "concept_id": "",
        "short_name": umm_c.get("ShortName", ""),
        "processing_level": processing_level,
        "output_shape": _output_shape(processing_level),
        "native_formats": _native_formats(umm_c),
        "direct_s3": _direct_s3(umm_c),
        "advisory": _advisory(umm_c),
    }


def _output_shape(processing_level: str) -> Literal["grid", "swath", "point"]:
    """Heuristic: L2 → swath; L3/L4 → grid; default grid (PLAN.md §4.2)."""
    if processing_level == "2":
        return "swath"
    return "grid"


def _native_formats(umm_c: dict) -> frozenset[str]:
    info = umm_c.get("ArchiveAndDistributionInformation") or {}
    formats = {
        f.get("Format")
        for f in info.get("FileDistributionInformation", [])
        if f.get("Format")
    }
    return frozenset(formats)


def _direct_s3(umm_c: dict) -> S3DirectAccess | None:
    ddi = umm_c.get("DirectDistributionInformation")
    if not ddi:
        return None
    return S3DirectAccess(
        region=ddi.get("Region", ""),
        bucket_prefixes=tuple(ddi.get("S3BucketAndObjectPrefixNames", [])),
        credentials_endpoint=ddi.get("S3CredentialsAPIEndpoint"),
    )


def _advisory(umm_c: dict) -> list[str]:
    """Maturity/advisory notes read from UMM-C — not hand-curated (PLAN.md §4.2)."""
    notes: list[str] = []
    progress = umm_c.get("CollectionProgress")
    if progress:
        notes.append(f"CollectionProgress: {progress}")
    if umm_c.get("StandardProduct") is False:
        notes.append("Not flagged as the standard product (StandardProduct=false).")
    purpose = umm_c.get("Purpose")
    if purpose:
        notes.append(purpose)
    return notes


def _concept_id_from_href(href: str) -> str:
    """``…/search/concepts/S2940253910-LARC_CLOUD`` → ``S2940253910-LARC_CLOUD``."""
    return href.rstrip("/").rsplit("/", 1)[-1] if href else ""
