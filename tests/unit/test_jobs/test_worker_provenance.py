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

import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

from earthdata_mcp.jobs import crud
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


class _FailingProvider:
    """Fails every submit — the no-fallback-possible path."""

    async def submit(self, plan) -> JobRef:
        raise RuntimeError("variable subsetting on C1-X is unsupported")


class _MultiArgFailingProvider:
    """Fails every submit the way harmony-py does: ``Exception(reason, message)``,
    a bare two-positional-arg exception (see ``harmony.client``: ``raise
    Exception(response.reason, message)``)."""

    async def submit(self, plan) -> JobRef:
        raise Exception(
            "Unprocessable Entity",
            "Error: the requested combination of operations is not supported",
        )


class _InvalidRequestProvider:
    """Fails the way harmony-py does for a rejected (422) request — a real
    Harmony rejection phrase in the message, per ``harmony.client``'s
    ``raise Exception(response.reason, description)``."""

    async def submit(self, plan) -> JobRef:
        raise Exception(
            "Unprocessable Entity",
            "Error: Coverages were not found for the provided variables: "
            "definitely_not_real",
        )


class _ServiceUnavailableProvider:
    """Fails the way harmony-py does for a transient 5xx outage."""

    async def submit(self, plan) -> JobRef:
        raise Exception("Service Unavailable", "upstream request timed out")


class _MaterializingProvider:
    async def materialize(self, job: JobRef) -> MaterializedResult:
        return MaterializedResult(
            storage_key="harmony/result/g1.nc",
            media_type="application/netcdf4",
            size_bytes=1234,
        )


class _RunningPollProvider:
    """Reports a still-running status with progress and a provider message
    (PRD 007) — the poll_job success path a real Harmony/AppEEARS mid-run
    poll takes."""

    async def poll(self, job: JobRef) -> JobStatus:
        return JobStatus(state=JobState.RUNNING, progress=42, message="Downloading")


class _FailingPollProvider:
    """Fails every poll — a mid-flight Harmony poll error."""

    async def poll(self, job: JobRef):
        raise RuntimeError("harmony poll: job not found")


class _FailingMaterializeProvider:
    """Fails every materialize — a download/materialization error."""

    async def materialize(self, job: JobRef) -> MaterializedResult:
        raise RuntimeError("materialize: storage backend unreachable")


async def _seed_submitted_job(
    session_factory, workspace_id: str, *, provider: str = "harmony"
) -> tuple[str, str]:
    """Insert a SUBMITTED job with a durable spec, ready for ``poll_job``."""
    job_id = uuid4().hex
    obs_handle = f"obs_{uuid4().hex[:16]}"
    request_spec = {
        "concept_id": "C1-X",
        "provider": provider,
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
                provider=provider,
                request_spec=request_spec,
                state=JobState.SUBMITTED.value,
                progress=0,
                provider_job_url="https://example/jobs/j1",
            )
        )
        await session.commit()
    return job_id, obs_handle


async def _seed_materializing_job(
    session_factory, workspace_id: str, *, provider: str = "harmony"
) -> tuple[str, str]:
    """Insert a MATERIALIZING job with a durable spec, ready for ``materialize_job``."""
    job_id = uuid4().hex
    obs_handle = f"obs_{uuid4().hex[:16]}"
    request_spec = {
        "concept_id": "C1-X",
        "provider": provider,
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
                provider=provider,
                request_spec=request_spec,
                state=JobState.MATERIALIZING.value,
                progress=50,
                provider_job_url="https://example/jobs/j1",
            )
        )
        await session.commit()
    return job_id, obs_handle


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


# -- invalid-request classification skips the OPeNDAP fallback (PRD 002) ----


async def test_submit_job_invalid_request_fails_without_opendap_fallback(
    session_factory, provenance_store, workspace_id, monkeypatch
) -> None:
    """A Harmony rejection (bad variable) must not be retried against OPeNDAP —
    it fails immediately with Harmony's own message, verbatim."""
    job_id, obs_handle = await _seed_pending_job(
        session_factory,
        workspace_id,
        provider="harmony",
        opendap_urls=["https://opendap.earthdata.nasa.gov/g1.nc"],
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_InvalidRequestProvider())
    )
    ctx = _ctx(session_factory)

    try:
        await worker_mod.submit_job(ctx, job_id)
    except Exception:
        pass

    events = await provenance_store.events(workspace_id, obs_handle)
    assert [e for e in events if e.event_type == "provider-fallback"] == []
    ctx["redis"].enqueue_job.assert_not_awaited()

    failed = [e for e in events if e.event_type == "job-failed"]
    assert len(failed) == 1
    assert failed[0].detail["classification"] == "invalid_request"
    assert "Coverages were not found" in failed[0].detail["message"]

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert job.error.startswith("Harmony rejected the request:")
    assert "Coverages were not found" in job.error


async def test_submit_job_infrastructure_5xx_still_falls_back_to_opendap(
    session_factory, provenance_store, workspace_id, monkeypatch
) -> None:
    """A harmony-py-shaped 5xx exception is classified infrastructure — today's
    fallback behavior stays, unlike the invalid-request case above."""
    job_id, obs_handle = await _seed_pending_job(
        session_factory,
        workspace_id,
        provider="harmony",
        opendap_urls=["https://opendap.earthdata.nasa.gov/g1.nc"],
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_ServiceUnavailableProvider())
    )
    ctx = _ctx(session_factory)

    await worker_mod.submit_job(ctx, job_id)

    events = await provenance_store.events(workspace_id, obs_handle)
    fallback = [e for e in events if e.event_type == "provider-fallback"]
    assert len(fallback) == 1
    assert fallback[0].detail["reason"]["message"].startswith("Service Unavailable")
    ctx["redis"].enqueue_job.assert_any_await("submit_job", job_id)


# -- OPENDAP_NOT_APPLICABLE on Harmony-fails-with-no-opendap-urls -----------


async def test_submit_job_records_opendap_not_applicable_when_no_urls(
    session_factory, provenance_store, workspace_id, monkeypatch
) -> None:
    job_id, obs_handle = await _seed_pending_job(
        session_factory, workspace_id, provider="harmony", opendap_urls=None
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_FailingProvider())
    )
    ctx = _ctx(session_factory)

    try:
        await worker_mod.submit_job(ctx, job_id)
    except RuntimeError:
        pass

    events = await provenance_store.events(workspace_id, obs_handle)
    not_applicable = [e for e in events if e.event_type == "opendap-not-applicable"]
    assert len(not_applicable) == 1
    assert [e for e in events if e.event_type == "provider-fallback"] == []

    detail = not_applicable[0].detail
    assert detail["harmony_error"]["error_type"] == "RuntimeError"
    assert "unsupported" in detail["harmony_error"]["message"]
    assert detail["reason"] == "no_opendap_endpoint_discovered"

    # OPENDAP_NOT_APPLICABLE explains the fallback decision; JOB_FAILED records
    # the terminal outcome — both are recorded on this branch (WS3).
    failed = [e for e in events if e.event_type == "job-failed"]
    assert len(failed) == 1
    assert failed[0].detail == {
        "stage": "submit",
        "provider": "harmony",
        "error_type": "RuntimeError",
        "message": "variable subsetting on C1-X is unsupported",
        "classification": "infrastructure",
    }

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert "no OPeNDAP fallback is available" in job.error
    assert "unsupported" in job.error


async def test_submit_job_non_harmony_failure_records_job_failed_event(
    session_factory, provenance_store, workspace_id, monkeypatch
) -> None:
    job_id, obs_handle = await _seed_pending_job(
        session_factory, workspace_id, provider="opendap", opendap_urls=None
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_FailingProvider())
    )
    ctx = _ctx(session_factory)

    try:
        await worker_mod.submit_job(ctx, job_id)
    except RuntimeError:
        pass

    events = await provenance_store.events(workspace_id, obs_handle)
    assert [e for e in events if e.event_type == "opendap-not-applicable"] == []
    assert [e for e in events if e.event_type == "provider-fallback"] == []
    failed = [e for e in events if e.event_type == "job-failed"]
    assert len(failed) == 1
    assert failed[0].detail == {
        "stage": "submit",
        "provider": "opendap",
        "error_type": "RuntimeError",
        "message": "variable subsetting on C1-X is unsupported",
    }

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert job.error == "submit/opendap failed: variable subsetting on C1-X is unsupported"


async def test_submit_job_error_message_is_not_a_raw_args_tuple(
    session_factory, provenance_store, workspace_id, monkeypatch
) -> None:
    """Regression: a GPM variable-subset rejection surfaced as the raw Python
    tuple repr ``('Unprocessable Entity', 'Error: ...')`` instead of a message —
    harmony-py raises a bare multi-arg ``Exception``, and Python's default
    ``BaseException.__str__`` renders that as ``repr(args)`` rather than text."""
    job_id, obs_handle = await _seed_pending_job(
        session_factory, workspace_id, provider="opendap", opendap_urls=None
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_MultiArgFailingProvider())
    )
    ctx = _ctx(session_factory)

    try:
        await worker_mod.submit_job(ctx, job_id)
    except Exception:
        pass

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert not job.error.startswith("(")
    assert "Unprocessable Entity" in job.error
    assert "requested combination of operations" in job.error


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


# -- poll_job persists progress and message from the provider (PRD 007) ----


async def test_poll_job_persists_progress_and_message(
    session_factory, workspace_id, monkeypatch
) -> None:
    job_id, _obs_handle = await _seed_submitted_job(
        session_factory, workspace_id, provider="harmony"
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_RunningPollProvider())
    )
    ctx = _ctx(session_factory)

    await worker_mod.poll_job(ctx, job_id)

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.RUNNING.value
    assert job.progress == 42
    assert job.message == "Downloading"
    ctx["redis"].enqueue_job.assert_any_await("poll_job", job_id, _defer_by=5)


# -- last_polled_at is a durable proof-of-life, independent of row diffs ----
#
# PRD 012: an AppEEARS task queued at the provider reports the same
# state/progress/message every poll, so no column changes and a row-diffed
# `updated_at` (onupdate-triggered) freezes — the liveness signal must never be
# an artifact of whether the poll changed anything else.


async def _seed_running_job(
    session_factory, workspace_id: str, *, provider: str = "harmony"
) -> tuple[str, str]:
    """Insert a RUNNING job with a durable spec, ready for ``poll_job``."""
    job_id = uuid4().hex
    obs_handle = f"obs_{uuid4().hex[:16]}"
    request_spec = {
        "concept_id": "C1-X",
        "provider": provider,
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
                provider=provider,
                request_spec=request_spec,
                state=JobState.RUNNING.value,
                progress=10,
                message="processing",
                provider_job_url="https://example/jobs/j1",
            )
        )
        await session.commit()
    return job_id, obs_handle


class _UnchangedPollProvider:
    """Reports the exact same status on every poll — the AppEEARS
    still-queued-at-the-provider case that issues no UPDATE at all."""

    async def poll(self, job: JobRef) -> JobStatus:
        return JobStatus(state=JobState.RUNNING, progress=10, message="processing")


async def test_poll_job_stamps_last_polled_at_even_when_status_unchanged(
    session_factory, workspace_id, monkeypatch
) -> None:
    job_id, _obs_handle = await _seed_running_job(
        session_factory, workspace_id, provider="appeears"
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_UnchangedPollProvider())
    )
    ctx = _ctx(session_factory)

    await worker_mod.poll_job(ctx, job_id)
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    first_stamp = job.last_polled_at
    assert first_stamp is not None
    # The status is byte-identical to what's already stored — no other column
    # actually changes on this poll.
    assert job.state == JobState.RUNNING.value
    assert job.progress == 10
    assert job.message == "processing"

    await asyncio.sleep(0.01)
    await worker_mod.poll_job(ctx, job_id)  # identical status again
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.last_polled_at > first_stamp


class _QueuedPollProvider:
    """Reports AppEEARS' 'still queued' status: our state machine has no finer
    edge than SUBMITTED for pending/queued, so this is what a real
    AppEEARSProvider.poll() returns while a task waits in AppEEARS' own queue."""

    def __init__(self, message: str) -> None:
        self._message = message

    async def poll(self, job: JobRef) -> JobStatus:
        return JobStatus(state=JobState.SUBMITTED, progress=0, message=self._message)


class _ReadyPollProvider:
    """Reports success in one hop — the OPeNDAP/Harmony 'done' case the durable
    machine must still walk through the legal SUBMITTED -> RUNNING ->
    MATERIALIZING edges for."""

    async def poll(self, job: JobRef) -> JobStatus:
        return JobStatus(state=JobState.READY, progress=100, message="done")


async def test_poll_job_keeps_submitted_while_provider_reports_queued(
    session_factory, workspace_id, monkeypatch
) -> None:
    """AppEEARS 'pending'/'queued' both map onto JobState.SUBMITTED — a poll
    happening is not itself grounds to promote to RUNNING; only a
    running-class provider status may (PRD 012)."""
    job_id, _obs_handle = await _seed_submitted_job(
        session_factory, workspace_id, provider="appeears"
    )
    monkeypatch.setattr(
        worker_mod,
        "_load_provider",
        AsyncMock(return_value=_QueuedPollProvider("queued at AppEEARS")),
    )
    ctx = _ctx(session_factory)

    await worker_mod.poll_job(ctx, job_id)

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.SUBMITTED.value
    assert job.message == "queued at AppEEARS"
    ctx["redis"].enqueue_job.assert_any_await("poll_job", job_id, _defer_by=5)


async def test_poll_job_submitted_directly_to_ready_walks_legal_edges(
    session_factory, workspace_id, monkeypatch
) -> None:
    """A provider reporting success in one hop from SUBMITTED must still walk
    SUBMITTED -> RUNNING -> MATERIALIZING — never a direct jump the state
    machine forbids."""
    job_id, _obs_handle = await _seed_submitted_job(
        session_factory, workspace_id, provider="opendap"
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_ReadyPollProvider())
    )
    ctx = _ctx(session_factory)

    await worker_mod.poll_job(ctx, job_id)

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.MATERIALIZING.value
    ctx["redis"].enqueue_job.assert_any_await("materialize_job", job_id)


async def test_poll_job_failure_leaves_last_polled_at_unchanged(
    session_factory, workspace_id, monkeypatch
) -> None:
    """A failed poll records why through `_fail_job` (the one failure choke
    point) — it must not also stamp a successful-poll timestamp."""
    job_id, _obs_handle = await _seed_submitted_job(
        session_factory, workspace_id, provider="harmony"
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_FailingPollProvider())
    )
    ctx = _ctx(session_factory)

    try:
        await worker_mod.poll_job(ctx, job_id)
    except RuntimeError:
        pass

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.last_polled_at is None


# -- JOB_FAILED on poll/materialize stage failures (WS3 failure envelope) ---


async def test_poll_job_failure_records_job_failed_with_stage_and_provider(
    session_factory, provenance_store, workspace_id, monkeypatch
) -> None:
    job_id, obs_handle = await _seed_submitted_job(
        session_factory, workspace_id, provider="harmony"
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_FailingPollProvider())
    )
    ctx = _ctx(session_factory)

    try:
        await worker_mod.poll_job(ctx, job_id)
    except RuntimeError:
        pass

    events = await provenance_store.events(workspace_id, obs_handle)
    failed = [e for e in events if e.event_type == "job-failed"]
    assert len(failed) == 1
    assert failed[0].detail == {
        "stage": "poll",
        "provider": "harmony",
        "error_type": "RuntimeError",
        "message": "harmony poll: job not found",
    }

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert job.error == "poll/harmony failed: harmony poll: job not found"


async def test_materialize_job_failure_records_job_failed_with_stage_and_provider(
    session_factory, provenance_store, workspace_id, monkeypatch
) -> None:
    job_id, obs_handle = await _seed_materializing_job(
        session_factory, workspace_id, provider="opendap"
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_FailingMaterializeProvider())
    )
    ctx = _ctx(session_factory)

    try:
        await worker_mod.materialize_job(ctx, job_id)
    except RuntimeError:
        pass

    events = await provenance_store.events(workspace_id, obs_handle)
    failed = [e for e in events if e.event_type == "job-failed"]
    assert len(failed) == 1
    assert failed[0].detail == {
        "stage": "materialize",
        "provider": "opendap",
        "error_type": "RuntimeError",
        "message": "materialize: storage backend unreachable",
    }

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert job.error == "materialize/opendap failed: materialize: storage backend unreachable"


# -- PRD 004: the failure path must never fail ------------------------------
#
# Live evidence: 106 jobs stuck non-terminal with null workspace_id — the
# JOB_FAILED provenance INSERT violates a NOT NULL constraint before the state
# transition to FAILED, so the job that was trying to fail can never fail.


async def _seed_pending_job_without_workspace_coords(
    session_factory, *, provider: str = "opendap"
) -> str:
    """A legacy/malformed spec: workspace_id and obs_handle are both None."""
    job_id = uuid4().hex
    request_spec = {
        "concept_id": "C1-X",
        "provider": provider,
        "workspace_id": None,
        "obs_handle": None,
        "job_handle": f"job_{uuid4().hex[:16]}",
    }
    async with session_factory() as session:
        session.add(
            Job(
                job_id=job_id,
                job_handle=request_spec["job_handle"],
                obs_handle=None,
                provider=provider,
                request_spec=request_spec,
                state=JobState.PENDING.value,
                progress=0,
            )
        )
        await session.commit()
    return job_id


async def test_submit_job_with_null_workspace_id_still_reaches_failed(
    session_factory, monkeypatch
) -> None:
    """The literal live bug: a spec with no workspace_id/obs_handle must not
    prevent the job from reaching FAILED when its provider submit fails."""
    job_id = await _seed_pending_job_without_workspace_coords(session_factory)
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_FailingProvider())
    )
    ctx = _ctx(session_factory)

    try:
        await worker_mod.submit_job(ctx, job_id)
    except RuntimeError:
        pass

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert job.error == "submit/opendap failed: variable subsetting on C1-X is unsupported"


async def test_fail_job_reaches_failed_even_when_provenance_store_raises(
    session_factory, workspace_id, monkeypatch, caplog
) -> None:
    """Ordering guarantee: even with a *valid* spec, a provenance store that
    throws for any reason (DB hiccup, etc.) must never block the transition to
    FAILED — and the original failure must still be logged."""
    job_id, _obs_handle = await _seed_pending_job(
        session_factory, workspace_id, provider="opendap"
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_FailingProvider())
    )

    class _ThrowingProvenanceStore:
        async def record_event(self, *args, **kwargs) -> None:
            raise RuntimeError("provenance DB unreachable")

    ctx = _ctx(session_factory)
    ctx["provenance"] = _ThrowingProvenanceStore()

    with caplog.at_level("WARNING"):
        try:
            await worker_mod.submit_job(ctx, job_id)
        except RuntimeError:
            pass

    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert job.error == "submit/opendap failed: variable subsetting on C1-X is unsupported"
    assert any(job_id in record.getMessage() for record in caplog.records)


async def test_submit_job_with_malformed_spec_fails_without_calling_provider(
    session_factory, monkeypatch,
) -> None:
    """A field-stripped spec (bad axis geometry) must fail the job cleanly at
    the load seam — never reach the provider, never raise out of the task."""
    job_id = uuid4().hex
    request_spec = {
        "concept_id": "C1-X",
        "provider": "opendap",
        "lat_axis": {"name": "/product/latitude"},  # missing origin/step/length
        "job_handle": f"job_{uuid4().hex[:16]}",
    }
    async with session_factory() as session:
        session.add(
            Job(
                job_id=job_id,
                job_handle=request_spec["job_handle"],
                obs_handle=None,
                provider="opendap",
                request_spec=request_spec,
                state=JobState.PENDING.value,
                progress=0,
            )
        )
        await session.commit()

    class _MustNotBeCalledProvider:
        async def submit(self, plan):
            raise AssertionError("provider.submit must not be called for an invalid spec")

    load_provider = AsyncMock(return_value=_MustNotBeCalledProvider())
    monkeypatch.setattr(worker_mod, "_load_provider", load_provider)
    ctx = _ctx(session_factory)

    await worker_mod.submit_job(ctx, job_id)  # must not raise

    load_provider.assert_not_awaited()
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
    assert job.state == JobState.FAILED.value
    assert "invalid job spec" in job.error
