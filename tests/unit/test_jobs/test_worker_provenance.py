"""Worker job-lifecycle provenance events (PLAN.md §4.5, this PRD's core gap).

Before this, ``get_provenance`` returned an empty ``events`` list for every
completed job — the ``ProvenanceEvent`` machinery existed but nothing in the
worker's ``submit_job``/``materialize_job`` ever called ``record_event``. These
tests drive the real worker tasks against the real Postgres-backed
``ProvenanceStore`` (only the provider's network is faked, via the same
``_load_provider`` seam ``tests/integration/test_durable_pipeline.py`` patches),
and assert the event trail each lifecycle transition leaves behind.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.jobs import worker as worker_mod
from earthdata_mcp.providers.base import JobRef, JobStatus, MaterializedResult
from earthdata_mcp.tools import discovery as discovery_mod
from earthdata_mcp.workspace.models import HandleType


def _ctx(session_factory) -> dict:
    return {"session_factory": session_factory, "redis": AsyncMock()}


async def _seed_pending_job(
    session_factory, workspace_id: str, *, provider: str = "harmony", opendap_urls=None
) -> tuple[str, str]:
    """Insert a PENDING job with a durable spec carrying workspace_id/obs_handle."""
    job_id = uuid4().hex
    obs_handle = f"obs_{uuid4().hex[:16]}"
    request_spec = {
        "concept_id": "C1-X",
        "provider": provider,
        "workspace_id": workspace_id,
        "obs_handle": obs_handle,
        "job_handle": f"job_{uuid4().hex[:16]}",
    }
    if opendap_urls:
        request_spec["opendap_urls"] = opendap_urls
    async with session_factory() as session:
        session.add(
            Job(
                job_id=job_id,
                job_handle=request_spec["job_handle"],
                obs_handle=obs_handle,
                provider=provider,
                request_spec=request_spec,
                state=JobState.PENDING.value,
                progress=0,
            )
        )
        await session.commit()
    return job_id, obs_handle


class _SucceedingProvider:
    """Submits successfully on every call — the direct-Harmony-success path."""

    async def submit(self, plan) -> JobRef:
        return JobRef(
            provider="harmony",
            provider_job_id="j1",
            provider_job_url="https://example/jobs/j1",
        )


class _FlakyProvider:
    """Fails the first submit, succeeds on the retry — the fallback path."""

    def __init__(self) -> None:
        self.calls = 0

    async def submit(self, plan) -> JobRef:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("harmony 503")
        return JobRef(
            provider="opendap",
            provider_job_id="j2",
            provider_job_url="https://example/jobs/j2",
        )


class _MaterializingProvider:
    async def materialize(self, job: JobRef) -> MaterializedResult:
        return MaterializedResult(
            storage_key="harmony/result/g1.nc",
            media_type="application/netcdf4",
            size_bytes=1234,
        )


# -- SUBMITTED on direct-Harmony success ------------------------------------


async def test_submit_job_records_submitted_on_harmony_success(
    session_factory, provenance_store, workspace_id, monkeypatch
) -> None:
    job_id, obs_handle = await _seed_pending_job(session_factory, workspace_id)
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_SucceedingProvider())
    )

    await worker_mod.submit_job(_ctx(session_factory), job_id)

    events = await provenance_store.events(workspace_id, obs_handle)
    submitted = [e for e in events if e.event_type == "submitted"]
    assert len(submitted) == 1
    assert submitted[0].detail["provider"] == "harmony"
    assert [e for e in events if e.event_type == "provider-fallback"] == []


# -- PROVIDER_FALLBACK then SUBMITTED on Harmony-fails-then-OPeNDAP-succeeds -


async def test_submit_job_records_fallback_then_submitted_on_retry(
    session_factory, provenance_store, workspace_id, monkeypatch
) -> None:
    job_id, obs_handle = await _seed_pending_job(
        session_factory,
        workspace_id,
        provider="harmony",
        opendap_urls=["https://opendap.earthdata.nasa.gov/g1.nc"],
    )
    provider = _FlakyProvider()
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=provider)
    )
    ctx = _ctx(session_factory)

    await worker_mod.submit_job(ctx, job_id)  # fails -> records fallback, re-enqueues
    ctx["redis"].enqueue_job.assert_any_await("submit_job", job_id)
    await worker_mod.submit_job(ctx, job_id)  # retries -> succeeds via opendap

    events = await provenance_store.events(workspace_id, obs_handle)
    ordered = list(reversed(events))  # events() is newest-first; we want order-of-occurrence
    assert [e.event_type for e in ordered] == ["provider-fallback", "submitted"]

    fallback = ordered[0]
    assert fallback.detail["from_provider"] == "harmony"
    assert fallback.detail["to_provider"] == "opendap"
    assert fallback.detail["reason"]["error_type"] == "RuntimeError"
    assert "harmony 503" in fallback.detail["reason"]["message"]

    assert ordered[1].detail["provider"] == "opendap"


# -- MATERIALIZED detail fields ----------------------------------------------


async def test_materialize_job_records_materialized_with_detail_fields(
    session_factory, provenance_store, workspace_store, workspace_id, monkeypatch
) -> None:
    obs_handle = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )
    job_id = uuid4().hex
    request_spec = {
        "concept_id": "C1-X",
        "provider": "harmony",
        "workspace_id": workspace_id,
        "obs_handle": obs_handle,
        "job_handle": f"job_{uuid4().hex[:16]}",
    }
    async with session_factory() as session:
        session.add(
            Job(
                job_id=job_id,
                job_handle=request_spec["job_handle"],
                obs_handle=obs_handle,
                provider="harmony",
                request_spec=request_spec,
                state=JobState.MATERIALIZING.value,
                progress=50,
                provider_job_url="https://example/jobs/j1",
            )
        )
        await session.commit()

    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_MaterializingProvider())
    )

    await worker_mod.materialize_job(_ctx(session_factory), job_id)

    events = await provenance_store.events(workspace_id, obs_handle)
    materialized = [e for e in events if e.event_type == "materialized"]
    assert len(materialized) == 1
    detail = materialized[0].detail
    assert detail["provider"] == "harmony"
    assert detail["storage_key"] == "harmony/result/g1.nc"
    assert detail["media_type"] == "application/netcdf4"
    assert detail["size_bytes"] == 1234
