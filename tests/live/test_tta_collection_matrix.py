"""Live validation matrix over the eight TTA collections (PRD 019, ``@pytest.mark.live``).

Talking-to-Air Phase 1 needs evidence this server actually works against the real
collections it uses — the live suite until now has only ever touched UAT/test
collections and MOD13Q1. This walks the real end-to-end path for each collection
through the **public tool functions** (the same seam an MCP client calls):

    search_datasets -> describe_dataset -> define_area_of_interest ->
    retrieve_subset -> get_retrieval_status (poll to terminal) ->
    export_result -> open_result (xarray/pyarrow)

Submission goes through the real, already-running ``worker`` container (the
default ``enqueue_fn`` enqueues onto the real Arq/Redis queue — this test never
drives worker internals directly), against **production** CMR/Harmony/OPeNDAP —
not UAT. A tight AOI (the LA basin — dense signal for every one of these
products) and a short window (1 day for hourly/daily-cadence products, 1 month
for the monthly product) keep each run small.

Failures here are data, not blockers (PRD 019): a failing collection gets a
ledger entry in ``docs/live_matrix_quirk_ledger.md`` rather than silently
skipping. Run on demand / nightly, never in the default gate:

    docker compose exec mcp python -m pytest -m live tests/live/test_tta_collection_matrix.py -v

Running all nine back-to-back occasionally trips CMR/CloudFront's own rate
limiting (a bare, boilerplate 414 unrelated to request shape) — a collection
that fails this way in a full-file run has been confirmed to pass when run
alone (``pytest ...::test_live_matrix_collection[<label>]``); re-run the
specific id before filing a new ledger row for it.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import numpy as np
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from earthdata_mcp.config import Settings
from earthdata_mcp.db import create_session_factory
from earthdata_mcp.jobs.models import create_jobs_schema
from earthdata_mcp.jobs.state import TERMINAL_STATES, JobState
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools._dataio import (
    HDF4_MEDIA_TYPE,
    NATIVE_ARCHIVE_MEDIA_TYPE,
    open_result,
)
from earthdata_mcp.tools.area import define_area_of_interest
from earthdata_mcp.tools.discovery import search_datasets
from earthdata_mcp.tools.export import export_result
from earthdata_mcp.tools.retrieval import cancel_retrieval, get_retrieval_status, retrieve_subset
from earthdata_mcp.tools.understanding import describe_dataset
from earthdata_mcp.workspace import ProvenanceStore, WorkspaceStore, create_schema

pytestmark = pytest.mark.live

_HAS_CREDS = bool(
    os.getenv("EARTHDATA_TOKEN")
    or (os.getenv("EDL_USERNAME") and os.getenv("EDL_PASSWORD"))
)

#: LA basin — dense, land-based signal for every one of these products
#: (TEMPO's North-America-only domain included), small enough to keep each
#: retrieval fast.
_BBOX = "-118.6,33.6,-117.6,34.6"

POLL_TIMEOUT_S = 900
POLL_INTERVAL_S = 10


@dataclass(frozen=True)
class _Collection:
    label: str
    concept_id: str
    short_name: str
    #: Group-qualified where the product is grouped (TEMPO/OMHCHOd/OMDOAO3e) —
    #: the exact shape PRD 019's problem statement flags as a plausible breaker.
    variable: str
    #: "hourly"/"daily" -> 1-day window; "monthly" -> 1-month window.
    cadence: str


#: The eight TTA collections (PRD 019 user story 1). Concept ids + primary
#: variables confirmed against production CMR (UMM-C short_name/version lookup
#: + UMM-Var / granule DMR inspection) while writing this matrix.
_MATRIX = [
    _Collection(
        "TEMPO NO2 L3 V04", "C3685896708-LARC_CLOUD", "TEMPO_NO2_L3",
        "product/vertical_column_troposphere", "hourly",
    ),
    _Collection(
        "TEMPO O3TOT L3 V04", "C3685896625-LARC_CLOUD", "TEMPO_O3TOT_L3",
        "product/column_amount_o3", "hourly",
    ),
    _Collection(
        "TEMPO HCHO L3 V04", "C3685897141-LARC_CLOUD", "TEMPO_HCHO_L3",
        "product/vertical_column", "hourly",
    ),
    _Collection(
        "OMI_MINDS_NO2d", "C2215175232-GES_DISC", "OMI_MINDS_NO2d",
        "ColumnAmountNO2", "daily",
    ),
    _Collection(
        "OMDOAO3e", "C1266136037-GES_DISC", "OMDOAO3e",
        "HDFEOS/GRIDS/ColumnAmountO3/Data_Fields/ColumnAmountO3", "daily",
    ),
    _Collection(
        "OMHCHOd", "C4119459768-GES_DISC", "OMHCHOd",
        "key_science_data/column_amount", "daily",
    ),
    _Collection(
        "TROPOMI NO2 monthly", "C3087325222-GES_DISC", "HAQ_TROPOMI_NO2_GLOBAL_M_L3",
        "Tropospheric_NO2", "monthly",
    ),
    _Collection(
        "MODIS Terra AOD", "C1443727145-LAADS", "MOD08_D3",
        "AOD_550_Dark_Target_Deep_Blue_Combined_Mean", "daily",
    ),
    _Collection(
        "MODIS Aqua AOD", "C1443729298-LAADS", "MYD08_D3",
        "AOD_550_Dark_Target_Deep_Blue_Combined_Mean", "daily",
    ),
]


@pytest.mark.skipif(not _HAS_CREDS, reason="no EDL credentials in environment")
@pytest.mark.parametrize("entry", _MATRIX, ids=[c.label for c in _MATRIX])
def test_live_matrix_collection(entry: _Collection) -> None:
    asyncio.run(_run(entry))


@pytest.mark.skipif(not _HAS_CREDS, reason="no EDL credentials in environment")
def test_live_matrix_cancel_harmony_job() -> None:
    """PRD 021: submit a real Harmony job and cancel it mid-flight, confirming
    the provider-side cancel actually reaches Harmony's ``/jobs/{id}/cancel``
    endpoint, not just the local durable state. Uses the first (Harmony-routed)
    collection in the matrix — TEMPO NO2 L3."""
    asyncio.run(_run_cancel(_MATRIX[0]))


async def _run_cancel(entry: _Collection) -> None:
    settings = Settings()
    engine = create_async_engine(settings.database_url)
    await create_schema(engine)
    await create_jobs_schema(engine)
    session_factory = create_session_factory(engine)
    store = WorkspaceStore(session_factory)
    provenance = ProvenanceStore(session_factory)
    workspace_id = f"ws-live-cancel-{uuid4().hex}"
    cmr = CMRProvider()

    try:
        window = await _recent_window(cmr, entry)
        if window is None:
            pytest.skip(f"{entry.label}: no granule over {_BBOX} in CMR right now")

        search_out = await search_datasets(
            entry.short_name, filters={"concept_id": entry.concept_id},
            workspace_id=workspace_id, cmr=cmr, store=store,
        )
        assert search_out["count"] >= 1, f"{entry.label}: search_datasets found nothing"
        dataset_handle = search_out["datasets"][0]["handle"]

        aoi_out = await define_area_of_interest(_BBOX, workspace_id, store=store)
        aoi_handle = aoi_out["handle"]

        retrieve_out = await retrieve_subset(
            dataset_handle, aoi_handle, window, [entry.variable],
            workspace_id=workspace_id,
            cmr=cmr, store=store, provenance=provenance,
            session_factory=session_factory,
        )
        job_handle = retrieve_out["job_handle"]
        assert retrieve_out["provider"] == "harmony", (
            f"{entry.label}: expected a Harmony-routed job to exercise the real "
            f"cancel path, got {retrieve_out['provider']!r}"
        )

        # Wait for the real (already-running) worker to actually submit to
        # Harmony before cancelling — cancelling while still "pending" reports
        # upstream: "unsupported" (no provider_job_url yet) and never exercises
        # the real Harmony cancel endpoint this test is for.
        status = await _poll_until_submitted_or_terminal(job_handle, workspace_id, store, session_factory)
        if status["status"] in {s.value for s in TERMINAL_STATES}:
            pytest.skip(
                f"{entry.label}: job reached {status['status']!r} before it could "
                "be cancelled mid-flight"
            )

        cancel_out = await cancel_retrieval(
            job_handle, workspace_id, store=store, session_factory=session_factory,
        )
        assert cancel_out["cancelled"] is True
        assert cancel_out["upstream"] != "error", (
            f"{entry.label}: provider-side cancel failed — see logs for the "
            "classified exception"
        )

        final_status = await _poll_to_terminal(job_handle, workspace_id, store, session_factory)
        assert final_status["status"] in {s.value for s in TERMINAL_STATES}, (
            f"{entry.label}: cancelled job never reached a terminal state: {final_status}"
        )
    finally:
        await engine.dispose()


async def _poll_until_submitted_or_terminal(job_handle, workspace_id, store, session_factory) -> dict:
    """Poll until the worker has actually submitted the job upstream (state
    leaves ``pending``) or it reaches a terminal state on its own."""
    deadline = time.monotonic() + POLL_TIMEOUT_S
    status = await get_retrieval_status(
        job_handle, workspace_id, store=store, session_factory=session_factory,
    )
    terminal = {s.value for s in TERMINAL_STATES}
    while status["status"] == JobState.PENDING.value:
        if time.monotonic() > deadline:
            pytest.skip(f"{job_handle}: job never left pending in time to cancel mid-flight")
        await asyncio.sleep(POLL_INTERVAL_S)
        status = await get_retrieval_status(
            job_handle, workspace_id, store=store, session_factory=session_factory,
        )
        if status["status"] in terminal:
            break
    return status


async def _recent_window(cmr: CMRProvider, entry: _Collection) -> str | None:
    """The most recent day/month with a granule actually intersecting ``_BBOX``.

    ``None`` when the collection has no granule over this AOI at all — the
    caller skips rather than submitting a retrieval doomed to find nothing.
    """
    granules = await cmr.search_granules(
        entry.concept_id, bounding_box=_BBOX, sort_key="-start_date", limit=1,
    )
    if not granules or not granules[0]["time_start"]:
        return None
    start = granules[0]["time_start"].replace("Z", "+00:00")
    dt = datetime.fromisoformat(start)
    if entry.cadence == "monthly":
        month_start = dt.date().replace(day=1)
        next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        return f"{month_start.isoformat()}/{next_month.isoformat()}"
    day: date = dt.date()
    return f"{day.isoformat()}/{(day + timedelta(days=1)).isoformat()}"


async def _run(entry: _Collection) -> None:
    settings = Settings()
    engine = create_async_engine(settings.database_url)
    await create_schema(engine)
    await create_jobs_schema(engine)
    session_factory = create_session_factory(engine)
    store = WorkspaceStore(session_factory)
    provenance = ProvenanceStore(session_factory)
    workspace_id = f"ws-live-matrix-{uuid4().hex}"
    cmr = CMRProvider()

    try:
        window = await _recent_window(cmr, entry)
        if window is None:
            pytest.skip(f"{entry.label}: no granule over {_BBOX} in CMR right now")

        search_out = await search_datasets(
            entry.short_name, filters={"concept_id": entry.concept_id},
            workspace_id=workspace_id, cmr=cmr, store=store,
        )
        assert search_out["count"] >= 1, f"{entry.label}: search_datasets found nothing"
        dataset_handle = search_out["datasets"][0]["handle"]

        describe_out = await describe_dataset(
            dataset_handle, workspace_id=workspace_id, cmr=cmr, store=store,
        )
        var_names = {v.get("name") for v in describe_out["variables"]}
        # describe_dataset's default get_variables() call caps out at CMR's own
        # default page limit (50) — a collection with hundreds of UMM-Var
        # associations (MOD08_D3/MYD08_D3 carry 975) is legitimately truncated,
        # in no particular order, so the chosen primary variable can plausibly
        # be missing from this compact listing without anything being broken.
        # Only assert membership when the catalog wasn't at risk of truncation.
        if describe_out["variable_source"] == "umm-var" and describe_out["variable_count"] < 50:
            assert entry.variable in var_names, (
                f"{entry.label}: {entry.variable!r} not in describe_dataset's "
                f"UMM-Var catalog {sorted(var_names)}"
            )

        aoi_out = await define_area_of_interest(_BBOX, workspace_id, store=store)
        aoi_handle = aoi_out["handle"]

        retrieve_out = await retrieve_subset(
            dataset_handle, aoi_handle, window, [entry.variable],
            workspace_id=workspace_id,
            cmr=cmr, store=store, provenance=provenance,
            session_factory=session_factory,
        )
        job_handle = retrieve_out["job_handle"]
        obs_handle = retrieve_out["obs_handle"]

        status = await _poll_to_terminal(job_handle, workspace_id, store, session_factory)
        assert status["status"] == "ready", (
            f"{entry.label}: job ended in {status['status']!r}: {status.get('error')}"
        )

        export_out = await export_result(obs_handle, workspace_id, store=store)
        assert export_out["status"] == "ready"
        media_type = export_out["media_type"]

        if media_type in (HDF4_MEDIA_TYPE, NATIVE_ARCHIVE_MEDIA_TYPE):
            # PRD 008: untouched native HDF4 is an honest, documented outcome when
            # no reformatting service intervened, not a bug in this server.
            pytest.skip(
                f"{entry.label}: materialized as native HDF4 ({media_type}) — "
                "no reformatting service in the chain, per PRD 008"
            )

        data = _read_storage_uri(export_out["storage_uri"])
        ds = open_result(data, media_type)

        flattened = entry.variable.replace("/", "__")
        assert flattened in ds.variables, (
            f"{entry.label}: {flattened!r} missing from opened dataset "
            f"variables {sorted(ds.variables)}"
        )
        if "time" in ds.coords:
            assert np.issubdtype(ds["time"].dtype, np.datetime64), (
                f"{entry.label}: time coordinate did not CF-decode to datetime64 "
                f"(dtype={ds['time'].dtype})"
            )
    finally:
        await engine.dispose()


async def _poll_to_terminal(job_handle, workspace_id, store, session_factory) -> dict:
    deadline = time.monotonic() + POLL_TIMEOUT_S
    status = await get_retrieval_status(
        job_handle, workspace_id, store=store, session_factory=session_factory,
    )
    while status["status"] not in {s.value for s in TERMINAL_STATES}:
        if time.monotonic() > deadline:
            pytest.fail(f"job {job_handle} did not reach a terminal state within {POLL_TIMEOUT_S}s")
        await asyncio.sleep(POLL_INTERVAL_S)
        status = await get_retrieval_status(
            job_handle, workspace_id, store=store, session_factory=session_factory,
        )
    return status


def _read_storage_uri(storage_uri: str) -> bytes:
    """Read a ``file://`` storage URI directly — the shared ``/data`` volume this
    container and the worker container both mount (docker-compose.yml)."""
    assert storage_uri.startswith("file://"), f"unexpected storage_uri scheme: {storage_uri!r}"
    path = Path(storage_uri.removeprefix("file://"))
    return path.read_bytes()
