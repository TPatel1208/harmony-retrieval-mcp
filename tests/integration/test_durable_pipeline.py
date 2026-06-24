"""Durable retrieval pipeline end-to-end (PLAN.md §4.3, §6 Phase 8 gate).

Plan → persist a durable job → drive the real worker tasks (submit → poll →
materialize) → READY → obs handle resolved → provenance recorded → get_provenance.
Everything below the provider is real: the Postgres ``jobs`` table, the state
machine, handle resolution, the local storage backend, and the provenance DAG.
Only the provider's *network* is faked — via a fake ``RetrievalProvider`` injected
at the worker's ``_provider_for`` seam — so no EDL credentials or Harmony/AppEEARS
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

import io
import os
import tempfile
import zipfile
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
from earthdata_mcp.providers.base import JobRef, JobStatus, MaterializedResult
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools import discovery as discovery_mod
from earthdata_mcp.tools._dataio import NETCDF_BUNDLE_MEDIA_TYPE, open_result
from earthdata_mcp.tools.provenance import get_provenance
from earthdata_mcp.tools.retrieval import retrieve_data, retrieve_timeseries
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

    ``_provider_for`` is replaced per-test with a fake provider; the worker's
    materialize step resolves the obs handle through ``discovery._default_store``,
    which we point at the test's real Postgres-backed store.
    """
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)

    def _install(provider: _FakeProvider) -> None:
        async def _fake_provider_for(spec: dict):
            return provider

        monkeypatch.setattr(worker_mod, "_provider_for", _fake_provider_for)

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


# -- the AppEEARS point path (hold-firm: Parquet AND provider==appeears) ----


async def test_appeears_point_path_materialises_parquet(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id,
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
    """A no-Harmony-service grid collection routes to OPeNDAP, materialises a netCDF
    bundle, and the obs handle opens back into one time-concatenated dataset."""
    ds = await _seed_dataset(workspace_store, workspace_id)
    aoi = await _seed_aoi(workspace_store, workspace_id)
    patch_worker_seam(
        _FakeProvider(
            "opendap", NETCDF_BUNDLE_MEDIA_TYPE, _netcdf_bundle_bytes(), local_backend
        )
    )

    out = await retrieve_data(
        ds, aoi, _TIME, workspace_id=workspace_id,
        cmr=_make_cmr_opendap_window(), store=workspace_store,
        provenance=provenance_store, session_factory=session_factory,
        enqueue_fn=AsyncMock(),
    )
    # Routed to OPeNDAP at planning time, carrying every granule in the window.
    assert out["provider"] == "opendap"
    async with session_factory() as session:
        job = await crud.get_job_by_handle(session, out["job_handle"])
        job_id = job.job_id
        assert job.request_spec["opendap_urls"] == _OPENDAP_URLS
    await _drive_to_ready(session_factory, job_id)

    # Hold-firm: the durable row records OPeNDAP, not just READY.
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
