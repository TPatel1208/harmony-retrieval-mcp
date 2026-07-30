"""The worker records what a job *actually* retrieved (PRD 023).

Drives the real ``materialize_job`` task against real Postgres (only the
provider is faked, via the same ``_load_provider`` seam
``test_worker_provenance.py`` patches) and asserts the durable materialization
record it leaves behind — including the headline case no existing mechanism in
this server can see: a replay of an unchanged spec that resolved a *different*
granule set.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock
from uuid import uuid4

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs import worker as worker_mod
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers.base import MaterializedResult
from earthdata_mcp.providers.materialization import MaterializationRecord
from earthdata_mcp.tools import discovery as discovery_mod
from earthdata_mcp.tools.export import export_result
from earthdata_mcp.workspace.models import HandleType


def _ctx(session_factory) -> dict:
    return {"session_factory": session_factory, "redis": AsyncMock()}


class _Provider:
    """Materializes to a fixed storage key, reporting a given granule list."""

    def __init__(self, granule_ids=None, **extra) -> None:
        self._extra = dict(extra)
        if granule_ids is not None:
            self._extra["granule_ids"] = list(granule_ids)

    async def materialize(self, job) -> MaterializedResult:
        return MaterializedResult(
            storage_key=f"p/{job.job_handle}/result.nc",
            media_type="application/x-netcdf",
            size_bytes=1234,
            extra=self._extra,
        )


async def _seed_materializing_job(
    session_factory,
    workspace_store,
    workspace_id: str,
    *,
    provider: str = "harmony",
    obs_handle: str | None = None,
) -> tuple[str, str]:
    """Insert a MATERIALIZING job (and its pending obs_ handle) ready for
    ``materialize_job``."""
    job_id = uuid4().hex
    if obs_handle is None:
        obs_handle = await workspace_store.put_handle(
            workspace_id, HandleType.OBS, {"status": "pending"}
        )
    request_spec = {
        "concept_id": "C1-X",
        "provider": provider,
        "output_format": "application/netcdf4",
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


async def _materialization_of(session_factory, job_id: str) -> dict | None:
    async with session_factory() as session:
        job = await crud.get_job(session, job_id)
        return job.materialization


# -- the record is written at the transition into READY ----------------------


async def test_materialize_records_what_the_provider_actually_delivered(
    session_factory, workspace_store, workspace_id, monkeypatch
) -> None:
    job_id, _obs = await _seed_materializing_job(
        session_factory, workspace_store, workspace_id, provider="opendap"
    )
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(
        worker_mod,
        "_load_provider",
        AsyncMock(return_value=_Provider(granule_ids=["https://o/g2.nc", "https://o/g1.nc"])),
    )

    await worker_mod.materialize_job(_ctx(session_factory), job_id)

    record = MaterializationRecord.from_jsonb(
        await _materialization_of(session_factory, job_id)
    )
    assert record.provider_used == "opendap"
    assert set(record.granule_ids) == {"https://o/g1.nc", "https://o/g2.nc"}
    assert record.partial is False
    assert record.content_digest


# -- drift: a replay of an unchanged spec that delivered different data -------


async def test_a_replay_that_resolved_an_extra_granule_is_a_different_digest(
    session_factory, workspace_store, workspace_id, monkeypatch, caplog
) -> None:
    """The headline case: an NRT late-arrival makes a replay of the *same*
    persisted spec a strictly different result, and nothing else in this server
    can see that. The drift is logged with both digests and the handle."""
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    first_id, obs_handle = await _seed_materializing_job(
        session_factory, workspace_store, workspace_id
    )
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_Provider(granule_ids=["G1", "G2"]))
    )
    await worker_mod.materialize_job(_ctx(session_factory), first_id)

    replay_id, _ = await _seed_materializing_job(
        session_factory, workspace_store, workspace_id, obs_handle=obs_handle
    )
    monkeypatch.setattr(
        worker_mod,
        "_load_provider",
        AsyncMock(return_value=_Provider(granule_ids=["G1", "G2", "G3"])),
    )
    with caplog.at_level(logging.WARNING, logger="earthdata_mcp.jobs.worker"):
        await worker_mod.materialize_job(_ctx(session_factory), replay_id)

    original = MaterializationRecord.from_jsonb(
        await _materialization_of(session_factory, first_id)
    )
    replayed = MaterializationRecord.from_jsonb(
        await _materialization_of(session_factory, replay_id)
    )
    assert replayed.content_digest != original.content_digest

    drift = [r for r in caplog.records if "materialization-drift" in r.getMessage()]
    assert len(drift) == 1
    assert obs_handle in drift[0].getMessage()
    assert original.content_digest in drift[0].getMessage()
    assert replayed.content_digest in drift[0].getMessage()


async def test_a_replay_that_reproduced_its_original_logs_no_drift(
    session_factory, workspace_store, workspace_id, monkeypatch, caplog
) -> None:
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    provider = AsyncMock(return_value=_Provider(granule_ids=["G1", "G2"]))
    monkeypatch.setattr(worker_mod, "_load_provider", provider)

    first_id, obs_handle = await _seed_materializing_job(
        session_factory, workspace_store, workspace_id
    )
    await worker_mod.materialize_job(_ctx(session_factory), first_id)
    replay_id, _ = await _seed_materializing_job(
        session_factory, workspace_store, workspace_id, obs_handle=obs_handle
    )
    with caplog.at_level(logging.WARNING, logger="earthdata_mcp.jobs.worker"):
        await worker_mod.materialize_job(_ctx(session_factory), replay_id)

    assert not [r for r in caplog.records if "materialization-drift" in r.getMessage()]


async def test_a_replay_that_ran_via_a_different_provider_is_drift(
    session_factory, workspace_store, workspace_id, monkeypatch, caplog
) -> None:
    """A Harmony job that fell back to OPeNDAP on replay delivered its data by a
    different code path — same granules, different digest."""
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_Provider(granule_ids=["G1"]))
    )
    first_id, obs_handle = await _seed_materializing_job(
        session_factory, workspace_store, workspace_id, provider="harmony"
    )
    await worker_mod.materialize_job(_ctx(session_factory), first_id)

    replay_id, _ = await _seed_materializing_job(
        session_factory,
        workspace_store,
        workspace_id,
        provider="opendap",
        obs_handle=obs_handle,
    )
    with caplog.at_level(logging.WARNING, logger="earthdata_mcp.jobs.worker"):
        await worker_mod.materialize_job(_ctx(session_factory), replay_id)

    original = MaterializationRecord.from_jsonb(
        await _materialization_of(session_factory, first_id)
    )
    replayed = MaterializationRecord.from_jsonb(
        await _materialization_of(session_factory, replay_id)
    )
    assert original.provider_used == "harmony"
    assert replayed.provider_used == "opendap"
    assert replayed.content_digest != original.content_digest
    assert [r for r in caplog.records if "materialization-drift" in r.getMessage()]


# -- the record reaches the consumer -----------------------------------------


async def test_a_materialized_handle_exports_its_delivered_content_identity(
    session_factory, workspace_store, workspace_id, local_backend, monkeypatch
) -> None:
    """End to end: what the worker recorded is what ``export_result`` hands a
    consumer, without a second lookup."""
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(
        worker_mod,
        "_load_provider",
        AsyncMock(return_value=_Provider(granule_ids=["G2", "G1", "G2"])),
    )
    job_id, obs_handle = await _seed_materializing_job(
        session_factory, workspace_store, workspace_id, provider="harmony"
    )
    await local_backend.put(f"p/{await _job_handle(session_factory, job_id)}/result.nc", b"x")

    await worker_mod.materialize_job(_ctx(session_factory), job_id)

    out = await export_result(
        obs_handle,
        workspace_id=workspace_id,
        store=workspace_store,
        storage=local_backend,
    )
    record = MaterializationRecord.from_jsonb(
        await _materialization_of(session_factory, job_id)
    )
    assert out["content_digest"] == record.content_digest
    # Deduplicated: the tally is of distinct granules delivered, not of files.
    assert out["granule_count"] == 2
    assert out["partial"] is False


async def _job_handle(session_factory, job_id: str) -> str:
    async with session_factory() as session:
        return (await crud.get_job(session, job_id)).job_handle


async def test_a_point_sample_records_a_partial_but_reproducible_record(
    session_factory, workspace_store, workspace_id, monkeypatch, caplog
) -> None:
    """PRD 023: AppEEARS names no granules. The record says ``partial`` rather
    than claiming an empty granule list, and a replay that delivered the same
    thing still digests identically — so a partial digest is not noise."""
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)
    monkeypatch.setattr(
        worker_mod, "_load_provider", AsyncMock(return_value=_Provider())
    )
    first_id, obs_handle = await _seed_materializing_job(
        session_factory, workspace_store, workspace_id, provider="appeears"
    )
    await worker_mod.materialize_job(_ctx(session_factory), first_id)
    replay_id, _ = await _seed_materializing_job(
        session_factory, workspace_store, workspace_id,
        provider="appeears", obs_handle=obs_handle,
    )
    with caplog.at_level(logging.WARNING, logger="earthdata_mcp.jobs.worker"):
        await worker_mod.materialize_job(_ctx(session_factory), replay_id)

    original = MaterializationRecord.from_jsonb(
        await _materialization_of(session_factory, first_id)
    )
    replayed = MaterializationRecord.from_jsonb(
        await _materialization_of(session_factory, replay_id)
    )
    assert original.partial is True
    assert replayed.content_digest == original.content_digest
    assert not [r for r in caplog.records if "materialization-drift" in r.getMessage()]
