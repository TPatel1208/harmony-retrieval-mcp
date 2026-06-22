"""Live AppEEARS point task through AppEEARSProvider (``@pytest.mark.live``, opt-in).

The credentialed end-to-end of the AppEEARS point → Parquet path (PLAN.md §4.4,
Phase 7.4 gate). Skipped unless EDL credentials are present. AppEEARS point tasks
take minutes, so this is a nightly/on-demand test, never part of the commit gate:

    EARTHDATA_TOKEN=... docker compose exec mcp pytest -m live \
        tests/live/test_appeears_point.py -v

It submits a real one-point MOD13Q1.061 NDVI task, polls to completion, and
asserts the materialized result is **Parquet** (never a Zarr cube).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import io
import os
import time

import pyarrow.parquet as pq
import pytest

from earthdata_mcp.config import Settings
from earthdata_mcp.jobs.state import TERMINAL_STATES, JobState
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.appeears import PARQUET_MEDIA_TYPE, AppEEARSProvider
from earthdata_mcp.providers.base import AOI, JobRef, RetrievalPlan, TimeRange, TransformSpec

pytestmark = pytest.mark.live

PRODUCT = "MOD13Q1.061"
LAYER = "_250m_16_days_NDVI"
POLL_TIMEOUT_S = 1800
POLL_INTERVAL_S = 20

_HAS_CREDS = bool(
    os.getenv("EARTHDATA_TOKEN")
    or (os.getenv("EARTHDATA_USERNAME") and os.getenv("EARTHDATA_PASSWORD"))
)


def _caps() -> CollectionCapabilities:
    return CollectionCapabilities(
        concept_id="C-LPCLOUD",
        short_name=PRODUCT,
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset({"HDF-EOS"}),
        direct_s3=None,
        services=[],
    )


@pytest.mark.skipif(not _HAS_CREDS, reason="no EDL credentials in environment")
def test_live_appeears_point_to_parquet() -> None:
    provider = AppEEARSProvider(_caps(), settings=Settings())
    plan = RetrievalPlan(
        output_format=PARQUET_MEDIA_TYPE,
        needs_point_sample=True,
        short_name=PRODUCT,
        aoi=AOI(bbox=(-104.0, 38.0, -104.0, 38.0)),  # a single point
        time_range=TimeRange(
            start=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2023, 3, 1, tzinfo=dt.timezone.utc),
        ),
        transform=TransformSpec(output_format=PARQUET_MEDIA_TYPE, variables=(LAYER,)),
    )

    async def _run() -> tuple[str, bytes]:
        ref = await provider.submit(plan)
        assert ref.provider_job_id
        deadline = time.monotonic() + POLL_TIMEOUT_S
        status = await provider.poll(ref)
        while status.state not in TERMINAL_STATES and time.monotonic() < deadline:
            await asyncio.sleep(POLL_INTERVAL_S)
            status = await provider.poll(ref)
        assert status.state is JobState.READY, f"task ended in {status.state}"
        result = await provider.materialize(
            JobRef(
                provider="appeears",
                provider_job_id=ref.provider_job_id,
                job_handle="job_live_appeears",
            )
        )
        return result.media_type, await provider._storage_backend().get(
            result.storage_key
        )

    media_type, data = asyncio.run(_run())
    assert media_type == PARQUET_MEDIA_TYPE  # tabular, never Zarr
    table = pq.read_table(io.BytesIO(data))
    assert table.num_rows > 0
