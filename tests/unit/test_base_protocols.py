"""Provider Protocols + shared types (PLAN.md §4.1).

Asserts the Protocol *split* is real: ``CMRProvider`` conforms to
``MetadataProvider`` with no throwing stubs, and is NOT a ``RetrievalProvider``.
Also pins the ``RetrievalPlan`` reconciliation (base owns it; ``_capabilities``
re-exports the same object) and the shared value-type round-trips.
"""

from __future__ import annotations

from datetime import datetime, timezone

from earthdata_mcp.config import Settings
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers import _capabilities
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
from earthdata_mcp.providers.cmr import CMRProvider


def _cmr() -> CMRProvider:
    return CMRProvider(Settings(_env_file=None))


# -- the Protocol split ----------------------------------------------------


def test_cmr_conforms_to_metadata_provider() -> None:
    assert isinstance(_cmr(), MetadataProvider)


def test_cmr_is_not_a_retrieval_provider() -> None:
    # CMR is metadata only — it must NOT structurally satisfy RetrievalProvider
    # (no can_handle/submit/poll/materialize), which is the whole point of the
    # two-Protocol split (no throwing retrieve stub).
    assert not isinstance(_cmr(), RetrievalProvider)


def test_cmr_capabilities_declares_metadata_only() -> None:
    caps = _cmr().capabilities()
    assert isinstance(caps, ProviderCapabilities)
    assert caps.kind == "metadata"
    assert caps.output_formats == frozenset()


def test_a_minimal_retrieval_provider_conforms() -> None:
    class FakeRetrieval:
        def can_handle(self, plan: RetrievalPlan) -> bool:
            return True

        async def submit(self, plan: RetrievalPlan) -> JobRef:
            return JobRef(provider="fake")

        async def poll(self, job: JobRef) -> JobStatus:
            return JobStatus(state=JobState.RUNNING)

        async def materialize(self, job: JobRef) -> MaterializedResult:
            return MaterializedResult(storage_key="k", media_type="application/netcdf")

    assert isinstance(FakeRetrieval(), RetrievalProvider)
    assert not isinstance(FakeRetrieval(), MetadataProvider)


# -- RetrievalPlan reconciliation -----------------------------------------


def test_retrieval_plan_is_the_same_object_in_capabilities() -> None:
    # base owns it; _capabilities re-exports the very same class.
    assert _capabilities.RetrievalPlan is RetrievalPlan


def test_gate_only_plan_still_constructs_and_gates() -> None:
    plan = RetrievalPlan(output_format="image/png", needs_bbox=True)
    assert plan.needs_bbox is True
    assert plan.concept_id is None  # appended fields default cleanly

    svc = _capabilities.ServiceCapability(
        service_name="s",
        concept_id="S1",
        subset_bbox=True,
        output_formats=frozenset({"image/png"}),
    )
    caps = _capabilities.CollectionCapabilities(
        concept_id="C1",
        short_name="X",
        processing_level="2",
        output_shape="swath",
        native_formats=frozenset(),
        direct_s3=None,
        services=[svc],
    )
    assert caps.find_service(plan) is svc
    assert caps.find_service(RetrievalPlan(output_format="application/netcdf")) is None


# -- shared value types ----------------------------------------------------


def test_time_range_cmr_round_trip() -> None:
    tr = TimeRange(
        start=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        end=datetime(2024, 3, 31, 0, 0, 0, tzinfo=timezone.utc),
    )
    assert tr.to_cmr() == "2024-01-01T00:00:00Z/2024-03-31T00:00:00Z"
    assert TimeRange.from_cmr(tr.to_cmr()) == tr


def test_aoi_to_bbox_str() -> None:
    assert AOI(bbox=(-105.0, 37.0, -104.0, 38.0)).to_bbox_str() == "-105.0,37.0,-104.0,38.0"


def test_job_status_accepts_a_job_state() -> None:
    status = JobStatus(state=JobState.READY, progress=100)
    assert status.state is JobState.READY


def test_transform_spec_and_plan_compose() -> None:
    plan = RetrievalPlan(
        output_format="application/netcdf",
        needs_bbox=True,
        needs_variable=True,
        concept_id="C123-PROV",
        aoi=AOI(bbox=(-105.0, 37.0, -104.0, 38.0)),
        transform=TransformSpec(
            output_format="application/netcdf", variables=("NO2",)
        ),
    )
    assert plan.transform is not None
    assert plan.transform.variables == ("NO2",)
