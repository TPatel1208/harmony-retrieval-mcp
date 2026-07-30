"""Durable retrieval pipeline end-to-end (PLAN.md §4.3, §6 Phase 8 gate).

Plan → persist a durable job → drive the real worker tasks (submit → poll →
materialize) → READY → obs handle resolved → provenance recorded → get_provenance.
Everything below the provider is real: the Postgres ``jobs`` table, the state
machine, handle resolution, the local storage backend, and the provenance DAG.
Only the provider's *network* is faked — via a fake ``RetrievalProvider`` injected
at the worker's ``_load_provider`` seam — so no EDL credentials or Harmony/AppEEARS
servers are needed. The credentialed real-Harmony version is
``tests/live/test_full_retrieval.py``.

Two paths are covered:
* **Harmony** (gridded subset → netCDF): asserts the job row records
  ``provider == "harmony"`` and the obs handle resolves to a netCDF result.
* **AppEEARS point sample** (→ Parquet): asserts BOTH that the materialized
  result is real Parquet *and* that the job row records ``provider == "appeears"``
  (the hold-firm assertion — never just READY).
"""

from __future__ import annotations

import asyncio
import io
import os
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import xarray as xr

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs import worker as worker_mod
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers._capabilities import (
    CollectionCapabilities,
    ServiceCapability,
)
from earthdata_mcp.providers.appeears import AppEEARSProvider
from earthdata_mcp.providers.base import JobRef, JobStatus, MaterializedResult
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools import discovery as discovery_mod
from earthdata_mcp.tools._dataio import NETCDF_BUNDLE_MEDIA_TYPE, open_result
from earthdata_mcp.tools.export import export_result, rematerialize
from earthdata_mcp.tools.provenance import get_provenance
from earthdata_mcp.tools.retrieval import (
    get_retrieval_status,
    retrieve_data,
    retrieve_subset,
    retrieve_timeseries,
)
from earthdata_mcp.workspace.models import HandleType

_CONCEPT_ID = "C1234567890-LPCLOUD"
_BBOX = [-105.0, 37.0, -104.0, 38.0]
_TIME = "2024-01-01/2024-03-31"


# -- capabilities + CMR stub ----------------------------------------------


def _grid_caps() -> CollectionCapabilities:
    svc = ServiceCapability(
        service_name="l3-subsetter",
        concept_id="S100-LPCLOUD",
        subset_bbox=True,
        subset_variable=True,
        subset_temporal=True,
        output_formats=frozenset({"application/zarr", "application/netcdf4"}),
    )
    return CollectionCapabilities(
        concept_id=_CONCEPT_ID,
        short_name="MOD13Q1",
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset(),
        direct_s3=None,
        services=[svc],
        capabilities_version="2",
        advisory=[],
    )


def _make_cmr() -> CMRProvider:
    cmr = CMRProvider.__new__(CMRProvider)
    cmr.collection_capabilities = AsyncMock(return_value=_grid_caps())
    # OPeNDAP discovery now runs for every grid+bbox retrieval (before routing picks
    # Harmony); an empty granule list keeps this collection on the Harmony path.
    cmr.search_granules = AsyncMock(return_value=[])
    return cmr


def _grid_no_service_caps() -> CollectionCapabilities:
    """A gridded L3 collection with no Harmony service — OPeNDAP is the only path."""
    return CollectionCapabilities(
        concept_id=_CONCEPT_ID,
        short_name="TEMPO_HCHO_L3",
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset(),
        direct_s3=None,
        services=[],
        capabilities_version="",
        advisory=[],
    )


_OPENDAP_URLS = [
    "https://opendap.earthdata.nasa.gov/collections/C1234567890-LPCLOUD"
    f"/granules/TEMPO_HCHO_L3_V03_2024010{day}T102914Z_S001.nc"
    for day in (1, 2)
]


def _make_cmr_opendap_window() -> CMRProvider:
    """CMR stub: no capable Harmony service, but two granules with OPeNDAP URLs."""
    cmr = CMRProvider.__new__(CMRProvider)
    cmr.collection_capabilities = AsyncMock(return_value=_grid_no_service_caps())
    cmr.search_granules = AsyncMock(
        return_value=[
            {"related_urls": [{"URL": url, "Subtype": "OPENDAP DATA"}]}
            for url in _OPENDAP_URLS
        ]
    )
    cmr.get_variables = AsyncMock(return_value=[])
    return cmr


def _netcdf_bundle_bytes() -> bytes:
    """A zip of two single-time netCDF granule subsets (what OPeNDAP materialises)."""

    def _granule(time_value: int, x_value: float) -> bytes:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "g.nc")
            xr.Dataset(
                {"x": ("time", np.array([x_value], dtype="float64"))},
                coords={"time": np.array([time_value], dtype="int64")},
            ).to_netcdf(path, engine="h5netcdf", mode="w")
            with open(path, "rb") as f:
                return f.read()

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("g0.nc", _granule(0, 10.0))
        zf.writestr("g1.nc", _granule(1, 20.0))
    return buf.getvalue()


# -- a fake RetrievalProvider that materialises real bytes to real storage --


class _FakeProvider:
    """Drives submit→poll(READY)→materialize without any network.

    Writes real bytes through a real :class:`LocalFilesystemBackend` so the obs
    handle resolves to a genuine storage key the test can read back.
    """

    def __init__(self, provider: str, media_type: str, data: bytes, storage) -> None:
        self._provider = provider
        self._media_type = media_type
        self._data = data
        self._storage = storage

    async def submit(self, plan) -> JobRef:
        return JobRef(
            provider=self._provider,
            provider_job_id="fake-1",
            provider_job_url="https://example/jobs/fake-1",
        )

    async def poll(self, job: JobRef) -> JobStatus:
        return JobStatus(state=JobState.READY, progress=100)

    async def materialize(self, job: JobRef) -> MaterializedResult:
        handle = job.job_handle or "result"
        key = f"{self._provider}/{handle}/result"
        await self._storage.put(key, self._data)
        return MaterializedResult(
            storage_key=key, media_type=self._media_type, size_bytes=len(self._data)
        )


async def _drive_to_ready(session_factory, job_id: str) -> None:
    """Run the three worker tasks in order, as the queue would (mocked Redis)."""
    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    await worker_mod.submit_job(ctx, job_id)
    await worker_mod.poll_job(ctx, job_id)
    await worker_mod.materialize_job(ctx, job_id)


@pytest.fixture
def patch_worker_seam(monkeypatch, workspace_store):
    """Patch the two process-default seams the worker reaches for.

    ``_load_provider`` (the ``providers.build`` seam's caller) is replaced
    per-test with a fake provider; the worker's materialize step resolves the
    obs handle through ``discovery._default_store``, which we point at the
    test's real Postgres-backed store.
    """
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)

    def _install(provider: _FakeProvider) -> None:
        async def _fake_load_provider(spec, edl_token=None) -> _FakeProvider:
            return provider

        monkeypatch.setattr(worker_mod, "_load_provider", _fake_load_provider)

    return _install


# -- seed helpers ----------------------------------------------------------


async def _seed_dataset(store, workspace_id: str) -> str:
    return await store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": _CONCEPT_ID}
    )


async def _seed_aoi(store, workspace_id: str) -> str:
    return await store.put_handle(
        workspace_id, HandleType.AOI, {"source": "bbox", "bbox": _BBOX}
    )


# -- the Harmony path ------------------------------------------------------


async def test_harmony_path_runs_to_ready_with_provenance(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)
    patch_worker_seam(
        _FakeProvider("harmony", "application/netcdf4", b"\x89HDF\r\n", local_backend)
    )

    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        cmr=_make_cmr(), store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=AsyncMock(),
    )
    assert out["provider"] == "harmony"

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
        job_id = job.job_id
    await _drive_to_ready(session_factory, job_id)

    # Durable job reached READY and the row records the Harmony provider.
    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
    assert job.state == JobState.READY.value
    assert job.provider == "harmony"

    # The pending obs handle resolved to a real storage key + media type.
    obs = await workspace_store.get_handle(workspace_id, out["obs_handle"])
    assert obs.payload["status"] == "ready"
    assert obs.payload["media_type"] == "application/netcdf4"
    assert await local_backend.get(obs.payload["storage_key"]) == b"\x89HDF\r\n"

    # Provenance ties the result back to the dataset (durable spec, no URL).
    prov = await get_provenance(
        out["obs_handle"], workspace_id=workspace_id,
        store=workspace_store, provenance=provenance_store,
    )
    assert {a["handle"] for a in prov["ancestors"]} == {ds}

    # The job-lifecycle event trail: a clean first-attempt Harmony success records
    # ROUTED -> SUBMITTED -> MATERIALIZED, with no phantom PROVIDER_FALLBACK event
    # (this is the gap this test previously missed: it asserted only on ancestry).
    assert prov["events"], "get_provenance returned no events for a completed job"
    ordered_types = [e["event_type"] for e in reversed(prov["events"])]
    assert ordered_types == ["routed", "submitted", "materialized"]

    routed = prov["events"][-1]
    assert routed["detail"]["path"] == "harmony"
    assert routed["detail"]["pinned_service"] == "l3-subsetter"

    submitted = prov["events"][1]
    assert submitted["detail"]["provider"] == "harmony"

    materialized = prov["events"][0]
    assert materialized["detail"]["provider"] == "harmony"
    assert materialized["detail"]["storage_key"] == obs.payload["storage_key"]
    assert materialized["detail"]["media_type"] == "application/netcdf4"
    assert materialized["detail"]["size_bytes"] == len(b"\x89HDF\r\n")


# -- the fallback path: Harmony fails, OPeNDAP succeeds --------------------


async def test_harmony_fallback_path_records_fallback_then_submitted_then_materialized(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id, monkeypatch,
) -> None:
    """A job that falls back from Harmony to OPeNDAP records the full trail:
    ROUTED -> PROVIDER_FALLBACK -> SUBMITTED -> MATERIALIZED, with the fallback's
    reason naming the Harmony failure and the eventual SUBMITTED/MATERIALIZED
    naming the provider that actually produced the result (opendap)."""
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    class _FailThenOpendapProvider:
        """Fails submit() once (as if it were the Harmony provider), then the
        worker's real fallback re-routes to OPeNDAP and ``_load_provider`` is
        asked again — we hand back a provider that succeeds from then on."""

        def __init__(self) -> None:
            self.calls = 0
            self._ok = _FakeProvider(
                "opendap", "application/netcdf4", b"\x89HDF\r\n", local_backend
            )

        async def submit(self, plan):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("harmony 503")
            return await self._ok.submit(plan)

        async def poll(self, job):
            return await self._ok.poll(job)

        async def materialize(self, job):
            return await self._ok.materialize(job)

    patch_worker_seam(_FailThenOpendapProvider())

    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        cmr=_make_cmr_opendap_window(), store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=AsyncMock(),
    )

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
        job_id = job.job_id

    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    await worker_mod.submit_job(ctx, job_id)  # Harmony fails -> re-routes to OPeNDAP
    await worker_mod.submit_job(ctx, job_id)  # retry succeeds via OPeNDAP
    await worker_mod.poll_job(ctx, job_id)
    await worker_mod.materialize_job(ctx, job_id)

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
    assert job.state == JobState.READY.value
    assert job.provider == "opendap"

    prov = await get_provenance(
        out["obs_handle"], workspace_id=workspace_id,
        store=workspace_store, provenance=provenance_store,
    )
    ordered_types = [e["event_type"] for e in reversed(prov["events"])]
    assert ordered_types == ["routed", "provider-fallback", "submitted", "materialized"]

    routed = prov["events"][-1]
    assert routed["detail"]["path"] == "harmony"
    assert routed["detail"]["pinned_service"] is None

    fallback = prov["events"][-2]
    assert fallback["detail"]["from_provider"] == "harmony"
    assert fallback["detail"]["to_provider"] == "opendap"
    assert fallback["detail"]["reason"]["error_type"] == "RuntimeError"
    assert "harmony 503" in fallback["detail"]["reason"]["message"]

    submitted = prov["events"][1]
    assert submitted["detail"]["provider"] == "opendap"

    materialized = prov["events"][0]
    assert materialized["detail"]["provider"] == "opendap"


# -- the no-fallback-possible path: Harmony fails, no OPeNDAP URLs discovered --


async def test_harmony_failure_with_no_opendap_urls_records_not_applicable(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id,
) -> None:
    """A collection with no OPeNDAP endpoint (empty ``opendap_urls`` at plan
    time) fails its Harmony submit and stays FAILED — but the failure is
    legible: get_provenance records OPENDAP_NOT_APPLICABLE and
    get_retrieval_status's error string says explicitly that no fallback was
    available, rather than surfacing a bare terminal exception."""
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    class _AlwaysFailsProvider:
        async def submit(self, plan):
            raise RuntimeError("variable subsetting on C1234567890-LPCLOUD is unsupported")

    patch_worker_seam(_AlwaysFailsProvider())

    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        cmr=_make_cmr(), store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=AsyncMock(),
    )
    assert out["provider"] == "harmony"

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
        job_id = job.job_id
        assert not job.request_spec.get("opendap_urls")

    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    with pytest.raises(RuntimeError):
        await worker_mod.submit_job(ctx, job_id)

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
    assert job.state == JobState.FAILED.value

    prov = await get_provenance(
        out["obs_handle"], workspace_id=workspace_id,
        store=workspace_store, provenance=provenance_store,
    )
    ordered_types = [e["event_type"] for e in reversed(prov["events"])]
    assert ordered_types == ["routed", "opendap-not-applicable", "job-failed"]

    routed = prov["events"][-1]
    assert routed["detail"]["path"] == "harmony"

    not_applicable = prov["events"][1]
    assert not_applicable["detail"]["reason"] == "no_opendap_endpoint_discovered"
    assert "unsupported" in not_applicable["detail"]["harmony_error"]["message"]

    job_failed = prov["events"][0]
    assert job_failed["detail"]["stage"] == "submit"
    assert job_failed["detail"]["provider"] == "harmony"
    assert job_failed["detail"]["classification"] == "infrastructure"
    assert "unsupported" in job_failed["detail"]["message"]

    status = await get_retrieval_status(
        out["job_handle"], workspace_id=workspace_id,
        store=workspace_store, session_factory=session_factory,
    )
    assert "no OPeNDAP fallback is available" in status["error"]
    assert "unsupported" in status["error"]


# -- the AppEEARS point path (hold-firm: Parquet AND provider==appeears) ----


async def test_appeears_point_path_materialises_parquet(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id, monkeypatch,
) -> None:
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    # Real Parquet bytes so the assertion is about genuine tabular output.
    table = pa.table({"date": ["2024-01-01"], "ndvi": [0.42]})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    patch_worker_seam(
        _FakeProvider("appeears", "application/x-parquet", buf.getvalue(), local_backend)
    )

    # retrieve_timeseries's planning step (_check_appeears_catalog) builds its own
    # real AppEEARSProvider and calls the product/layer lookup directly — a network
    # seam the worker-provider fake above doesn't reach. Fake it here too, so the
    # gate never needs live AppEEARS/EDL access.
    async def _fake_fetch_layer_map(self, product: str) -> dict:
        return {"NDVI": {}}

    monkeypatch.setattr(AppEEARSProvider, "_fetch_layer_map", _fake_fetch_layer_map)

    out = await retrieve_timeseries(
        ds, _TIME, ["NDVI"], workspace_id=workspace_id, aoi_handle=aoi,
        point_sample=True,
        cmr=_make_cmr(), store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=AsyncMock(),
    )
    assert out["provider"] == "appeears"

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
        job_id = job.job_id
    await _drive_to_ready(session_factory, job_id)

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
    # Hold-firm: the job row records the AppEEARS provider...
    assert job.state == JobState.READY.value
    assert job.provider == "appeears"
    assert job.request_spec["provider"] == "appeears"

    # ...and the materialized result is genuine Parquet, never a Zarr cube.
    obs = await workspace_store.get_handle(workspace_id, out["obs_handle"])
    assert obs.payload["media_type"] == "application/x-parquet"
    read_back = pq.read_table(io.BytesIO(await local_backend.get(obs.payload["storage_key"])))
    assert read_back.column_names == ["date", "ndvi"]


# -- the OPeNDAP bundle path (hold-firm: provider==opendap AND a readable bundle) --


async def test_opendap_bundle_path_runs_to_ready_and_opens(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id,
) -> None:
    """A no-Harmony-service grid collection still routes to Harmony (unpinned) at
    plan time — CLAUDE.md's hard rule: OPeNDAP is never a planning-time choice,
    only the worker's runtime fallback when a real Harmony submit fails (router.py
    step 2 always wires Harmony first; this collection's empty ``services`` only
    means the match is unpinned, per the union-trap/no-registered-services case).
    OPeNDAP URLs are still discovered and stored on the spec at plan time for any
    gridded+bbox collection, so once Harmony's submit fails, the worker's fallback
    re-routes to OPeNDAP, materialises a netCDF bundle, and the obs handle opens
    back into one time-concatenated dataset."""
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    class _FailThenBundleProvider:
        """Fails submit() once (as Harmony), then the worker's runtime fallback
        re-routes to OPeNDAP and asks ``_load_provider`` again — hand back a
        provider that materialises the multi-granule bundle from then on."""

        def __init__(self) -> None:
            self.calls = 0
            self._ok = _FakeProvider(
                "opendap", NETCDF_BUNDLE_MEDIA_TYPE, _netcdf_bundle_bytes(), local_backend
            )

        async def submit(self, plan):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("harmony 503")
            return await self._ok.submit(plan)

        async def poll(self, job):
            return await self._ok.poll(job)

        async def materialize(self, job):
            return await self._ok.materialize(job)

    patch_worker_seam(_FailThenBundleProvider())

    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        cmr=_make_cmr_opendap_window(), store=workspace_store,
        provenance=provenance_store, session_factory=session_factory,
        enqueue_fn=AsyncMock(),
    )
    # Harmony first, unpinned (no service matched) — never OPeNDAP at plan time.
    assert out["provider"] == "harmony"
    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
        job_id = job.job_id
        # OPeNDAP URLs are discovered regardless of the routing decision, so the
        # worker's runtime fallback has something to retry against.
        assert job.request_spec["opendap_urls"] == _OPENDAP_URLS

    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    await worker_mod.submit_job(ctx, job_id)  # Harmony fails -> re-routes to OPeNDAP
    await worker_mod.submit_job(ctx, job_id)  # retry succeeds via OPeNDAP
    await worker_mod.poll_job(ctx, job_id)
    await worker_mod.materialize_job(ctx, job_id)

    # Hold-firm: the durable row records OPeNDAP (post-fallback), not just READY.
    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
    assert job.state == JobState.READY.value
    assert job.provider == "opendap"

    # The obs handle resolves to a netCDF bundle that opens + concatenates on time.
    obs = await workspace_store.get_handle(workspace_id, out["obs_handle"])
    assert obs.payload["media_type"] == NETCDF_BUNDLE_MEDIA_TYPE
    bundle = await local_backend.get(obs.payload["storage_key"])
    result = open_result(bundle, NETCDF_BUNDLE_MEDIA_TYPE)
    assert result.sizes["time"] == 2
    assert result["x"].values.tolist() == [10.0, 20.0]


# -- export_result / rematerialize (PRD 017) -------------------------------


def _real_netcdf_bytes(value: float) -> bytes:
    """A minimal, genuinely-openable single-variable netCDF file."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "g.nc")
        xr.Dataset(
            {"x": ("time", np.array([value], dtype="float64"))},
            coords={"time": np.array([0], dtype="int64")},
        ).to_netcdf(path, engine="h5netcdf", mode="w")
        with open(path, "rb") as f:
            return f.read()


async def test_export_then_evict_then_rematerialize_then_export_again(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id,
) -> None:
    """The full recovery loop PRD 017 exists for: materialize → export → open
    with xarray from the returned URI; then simulate eviction (delete the blob,
    flip the handle to expired, as the eviction sweep would) → rematerialize →
    drive the new job to READY → export again → open again — no memory of the
    original retrieve_data arguments needed anywhere in the recovery half."""
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)
    patch_worker_seam(
        _FakeProvider("harmony", "application/netcdf4", _real_netcdf_bytes(1.0), local_backend)
    )

    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        cmr=_make_cmr(), store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=AsyncMock(),
    )
    obs_handle = out["obs_handle"]

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
        job_id = job.job_id
    await _drive_to_ready(session_factory, job_id)

    exported = await export_result(
        obs_handle, workspace_id=workspace_id, store=workspace_store, storage=local_backend,
    )
    assert exported["status"] == "ready"
    ds_opened = xr.open_dataset(Path.from_uri(exported["storage_uri"]))
    assert ds_opened["x"].values.tolist() == [1.0]

    # Simulate eviction: delete the blob, flip the handle to expired — exactly
    # what jobs/eviction.py's _evict_one does, without invoking the sweep itself.
    evicted_record = await workspace_store.get_handle(workspace_id, obs_handle)
    await local_backend.delete(evicted_record.payload["storage_key"])
    await workspace_store.update_handle(
        workspace_id, obs_handle, {"status": "expired", "storage_key": None}
    )

    evicted_export = await export_result(
        obs_handle, workspace_id=workspace_id, store=workspace_store, storage=local_backend,
    )
    assert evicted_export["status"] == "expired"

    re_enqueue = AsyncMock()
    rematerialized = await rematerialize(
        obs_handle, workspace_id=workspace_id,
        store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=re_enqueue,
    )
    assert rematerialized["status"] == JobState.PENDING.value
    assert rematerialized["obs_handle"] == obs_handle
    re_enqueue.assert_awaited_once()
    assert re_enqueue.await_args.args[0] == "submit_job"

    # A different fake provider instance so the recovered job's materialized
    # bytes are distinguishable from the first run's.
    patch_worker_seam(
        _FakeProvider("harmony", "application/netcdf4", _real_netcdf_bytes(2.0), local_backend)
    )
    async with session_factory() as session:
        new_job = await crud.get_job_by_handle(session, rematerialized["job_handle"])
        new_job_id = new_job.job_id
        assert new_job.request_spec["concept_id"] == _CONCEPT_ID
    await _drive_to_ready(session_factory, new_job_id)

    re_exported = await export_result(
        obs_handle, workspace_id=workspace_id, store=workspace_store, storage=local_backend,
    )
    assert re_exported["status"] == "ready"
    ds_reopened = xr.open_dataset(Path.from_uri(re_exported["storage_uri"]))
    assert ds_reopened["x"].values.tolist() == [2.0]


# -- retrieve_subset idempotent dedupe end-to-end (PRD 020) -----------------


async def test_retrieve_subset_dedupe_end_to_end_materialized_then_evicted(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id,
) -> None:
    """retrieve → await → retrieve identical (exactly one provider job ever
    ran, the second call returns in one round-trip); then evict the key →
    retrieve identical again (rematerialize-equivalent replay) — the two
    production surfaces PRD 020 exists for."""
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)
    patch_worker_seam(
        _FakeProvider("harmony", "application/netcdf4", _real_netcdf_bytes(1.0), local_backend)
    )
    enqueue = AsyncMock()

    first = await retrieve_subset(
        ds, aoi, _TIME, ["NDVI"], workspace_id=workspace_id,
        cmr=_make_cmr(), store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=enqueue,
    )
    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, first["job_handle"])
        job_id = job.job_id
    await _drive_to_ready(session_factory, job_id)

    second = await retrieve_subset(
        ds, aoi, _TIME, ["NDVI"], workspace_id=workspace_id,
        cmr=_make_cmr(), store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=enqueue,
    )
    assert second["reused"] is True
    assert second["obs_handle"] == first["obs_handle"]
    assert second["status"] == "ready"
    # Exactly one provider job ever ran — the second call cost no extra enqueue.
    enqueue.assert_awaited_once()

    exported = await export_result(
        first["obs_handle"], workspace_id=workspace_id,
        store=workspace_store, storage=local_backend,
    )
    assert exported["status"] == "ready"

    # Simulate eviction: delete the blob, flip the handle to expired — exactly
    # what jobs/eviction.py's _evict_one does.
    evicted_record = await workspace_store.get_handle(workspace_id, first["obs_handle"])
    await local_backend.delete(evicted_record.payload["storage_key"])
    await workspace_store.update_handle(
        workspace_id, first["obs_handle"], {"status": "expired", "storage_key": None},
    )

    # A different fake provider instance so the recovered job's materialized
    # bytes are distinguishable from the first run's.
    patch_worker_seam(
        _FakeProvider("harmony", "application/netcdf4", _real_netcdf_bytes(2.0), local_backend)
    )
    third = await retrieve_subset(
        ds, aoi, _TIME, ["NDVI"], workspace_id=workspace_id,
        cmr=_make_cmr(), store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=enqueue,
    )
    assert third["reused"] is True
    assert third["obs_handle"] == first["obs_handle"]
    assert third["job_handle"] != first["job_handle"]
    assert third["status"] == JobState.PENDING.value
    assert enqueue.await_count == 2

    async with session_factory() as session:
        new_job = await crud.get_job_by_handle(session, third["job_handle"])
        new_job_id = new_job.job_id
    await _drive_to_ready(session_factory, new_job_id)

    re_exported = await export_result(
        first["obs_handle"], workspace_id=workspace_id,
        store=workspace_store, storage=local_backend,
    )
    assert re_exported["status"] == "ready"
    ds_reopened = xr.open_dataset(Path.from_uri(re_exported["storage_uri"]))
    assert ds_reopened["x"].values.tolist() == [2.0]


async def test_retrieve_subset_dedupe_failed_job_falls_through_to_fresh_submission(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id,
) -> None:
    """End-to-end: retrieve_subset -> the real worker fails the job (Harmony
    submit raises, no OPeNDAP fallback discovered) -> the job row lands
    FAILED, but the obs_ handle payload stays frozen at ``{"status":
    "pending"}`` (only ``materialize_job`` ever writes ``"ready"``, only
    eviction writes ``"expired"`` — the failure path only ever touches the
    jobs row). An identical retrieve_subset must not hand back that dead job
    as a reuse hit: it submits a fresh job under the *same* obs_ handle
    identity, which the worker then drives to READY normally, exportable at
    the end — proving the signature was never permanently un-retryable."""
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)

    class _AlwaysFailsProvider:
        async def submit(self, plan):
            raise RuntimeError("harmony 503")

    patch_worker_seam(_AlwaysFailsProvider())
    enqueue = AsyncMock()

    first = await retrieve_subset(
        ds, aoi, _TIME, ["NDVI"], workspace_id=workspace_id,
        cmr=_make_cmr(), store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=enqueue,
    )
    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, first["job_handle"])
        job_id = job.job_id

    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    with pytest.raises(RuntimeError):
        await worker_mod.submit_job(ctx, job_id)

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, first["job_handle"])
    assert job.state == JobState.FAILED.value

    obs = await workspace_store.get_handle(workspace_id, first["obs_handle"])
    assert obs.payload["status"] == "pending"  # never touched by the failure path

    patch_worker_seam(
        _FakeProvider("harmony", "application/netcdf4", _real_netcdf_bytes(1.0), local_backend)
    )
    second = await retrieve_subset(
        ds, aoi, _TIME, ["NDVI"], workspace_id=workspace_id,
        cmr=_make_cmr(), store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=enqueue,
    )

    assert "reused" not in second
    assert second["obs_handle"] == first["obs_handle"]
    assert second["job_handle"] != first["job_handle"]
    # The dead job's own submit attempt plus this fresh retry.
    assert enqueue.await_count == 2

    async with session_factory() as session:
        new_job = await crud.get_job_by_handle(session, second["job_handle"])
        new_job_id = new_job.job_id
    await _drive_to_ready(session_factory, new_job_id)

    exported = await export_result(
        first["obs_handle"], workspace_id=workspace_id,
        store=workspace_store, storage=local_backend,
    )
    assert exported["status"] == "ready"


async def test_retrieve_subset_concurrent_identical_calls_collapse_onto_one_job(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id,
) -> None:
    """Story 6, driven through the real Postgres-backed stack: two concurrent
    identical retrieve_subset calls must collapse onto exactly one job. The
    earlier SELECT-based reuse check is a check-then-act read on its own —
    both calls can miss it before either has written a handle. The partial
    unique index on (workspace_id, signature) is what actually closes the
    window: whichever call's handle insert loses collapses onto the winner's
    handle instead of enqueueing a second job."""
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)
    patch_worker_seam(
        _FakeProvider("harmony", "application/netcdf4", _real_netcdf_bytes(1.0), local_backend)
    )
    enqueue = AsyncMock()

    results = await asyncio.gather(
        retrieve_subset(
            ds, aoi, _TIME, ["NDVI"], workspace_id=workspace_id,
            cmr=_make_cmr(), store=workspace_store, provenance=provenance_store,
            session_factory=session_factory, enqueue_fn=enqueue,
        ),
        retrieve_subset(
            ds, aoi, _TIME, ["NDVI"], workspace_id=workspace_id,
            cmr=_make_cmr(), store=workspace_store, provenance=provenance_store,
            session_factory=session_factory, enqueue_fn=enqueue,
        ),
    )

    obs_handles = {r["obs_handle"] for r in results}
    job_handles = {r["job_handle"] for r in results}
    assert len(obs_handles) == 1
    assert len(job_handles) == 1
    assert sum(1 for r in results if r.get("reused")) == 1
    enqueue.assert_awaited_once()

    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, next(iter(job_handles)))
        job_id = job.job_id
    await _drive_to_ready(session_factory, job_id)

    exported = await export_result(
        next(iter(obs_handles)), workspace_id=workspace_id,
        store=workspace_store, storage=local_backend,
    )
    assert exported["status"] == "ready"
