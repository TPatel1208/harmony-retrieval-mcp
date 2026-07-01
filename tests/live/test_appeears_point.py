"""Live AppEEARS point tasks across several datasets (``@pytest.mark.live``, opt-in).

Exercises the full submit → poll → materialize path for two products that use
different AppEEARS layer naming conventions:

* **MOD13Q1.061** — layer keys carry a leading underscore (``_250m_16_days_NDVI``).
  The user supplies the UMM-V name ``"250m 16 days NDVI"``; the provider must
  normalize and resolve it.

* **MOD11A1.061** — layer keys have NO leading underscore (``LST_Day_1km``).
  The user supplies ``"LST Day 1km"``; same normalization, different pattern.

Both cases exercise the Bug 1 fix (versioned product id = ``short_name.version``)
and the Bug 2 fix (UMM-V → AppEEARS layer translation via normalized matching).

Run:

    EDL_USERNAME=<user> EDL_PASSWORD=<pass> \\
        docker compose exec mcp pytest -m live tests/live/test_appeears_point.py -v

Tasks take several minutes; the poll timeout is 30 min per case. The two cases
are submitted in parallel to keep wall-clock time reasonable.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import io
import os
import time
from dataclasses import dataclass

import pyarrow.parquet as pq
import pytest

from earthdata_mcp.config import Settings
from earthdata_mcp.jobs.state import TERMINAL_STATES, JobState
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.appeears import PARQUET_MEDIA_TYPE, AppEEARSProvider
from earthdata_mcp.providers.base import AOI, JobRef, RetrievalPlan, TimeRange, TransformSpec

pytestmark = pytest.mark.live

POLL_TIMEOUT_S = 1800
POLL_INTERVAL_S = 30

# A single well-known point in Colorado (centroid of a ~2° bbox).
POINT_AOI = AOI(bbox=(-105.0, 37.0, -103.0, 39.0))
TIME_RANGE = TimeRange(
    start=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
    end=dt.datetime(2023, 8, 1, tzinfo=dt.timezone.utc),
)

_HAS_CREDS = bool(os.getenv("EDL_USERNAME") and os.getenv("EDL_PASSWORD"))


@dataclass(frozen=True)
class _Case:
    """One dataset to submit as a point task."""
    short_name: str
    version: str
    umm_v_variable: str     # UMM-V Name field — what the caller supplies
    expected_layer: str     # AppEEARS layer id — what the provider must resolve to
    label: str


CASES = [
    _Case(
        short_name="MOD13Q1",
        version="061",
        umm_v_variable="250m 16 days NDVI",
        expected_layer="_250m_16_days_NDVI",   # leading-underscore convention
        label="MOD13Q1-NDVI",
    ),
    _Case(
        short_name="MOD11A1",
        version="061",
        umm_v_variable="LST Day 1km",
        expected_layer="LST_Day_1km",          # no-leading-underscore convention
        label="MOD11A1-LST",
    ),
]


def _caps(case: _Case) -> CollectionCapabilities:
    return CollectionCapabilities(
        concept_id=f"C-LPCLOUD-{case.short_name}",
        short_name=case.short_name,
        version=case.version,
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset({"HDF-EOS"}),
        direct_s3=None,
        services=[],
    )


def _plan(case: _Case) -> RetrievalPlan:
    return RetrievalPlan(
        output_format=PARQUET_MEDIA_TYPE,
        needs_point_sample=True,
        short_name=case.short_name,
        aoi=POINT_AOI,
        time_range=TIME_RANGE,
        transform=TransformSpec(
            output_format=PARQUET_MEDIA_TYPE,
            variables=(case.umm_v_variable,),
        ),
    )


async def _run_case(
    case: _Case, settings: Settings, tmp_dir: str
) -> tuple[str, str, int]:
    """Submit, poll to completion, materialize. Returns (label, media_type, num_rows)."""
    from earthdata_mcp.storage.local import LocalFilesystemBackend

    storage = LocalFilesystemBackend(tmp_dir)
    provider = AppEEARSProvider(_caps(case), settings=settings, storage=storage)
    plan = _plan(case)

    ref = await provider.submit(plan)
    assert ref.provider_job_id, f"{case.label}: submit returned no task_id"

    deadline = time.monotonic() + POLL_TIMEOUT_S
    status = await provider.poll(ref)
    while status.state not in TERMINAL_STATES and time.monotonic() < deadline:
        await asyncio.sleep(POLL_INTERVAL_S)
        status = await provider.poll(ref)

    assert status.state is JobState.READY, (
        f"{case.label}: task ended in {status.state} — {status.error or 'no error detail'}"
    )

    result = await provider.materialize(
        JobRef(
            provider="appeears",
            provider_job_id=ref.provider_job_id,
            job_handle=f"job_live_{case.short_name.lower()}",
        )
    )
    assert result.media_type == PARQUET_MEDIA_TYPE, (
        f"{case.label}: expected Parquet, got {result.media_type}"
    )

    raw = await storage.get(result.storage_key)
    table = pq.read_table(io.BytesIO(raw))
    return case.label, result.media_type, table.num_rows


@pytest.mark.skipif(not _HAS_CREDS, reason="EDL_USERNAME / EDL_PASSWORD not set")
def test_live_appeears_multiple_products(tmp_path) -> None:
    """Submit MOD13Q1 and MOD11A1 point tasks in parallel; both must reach READY
    and return Parquet rows. Confirms version suffix + layer resolution across the
    two AppEEARS layer naming conventions (leading-underscore vs plain)."""
    settings = Settings()

    async def _run_all():
        return await asyncio.gather(
            *[_run_case(c, settings, str(tmp_path / c.short_name)) for c in CASES]
        )

    results = asyncio.run(_run_all())
    for label, media_type, num_rows in results:
        assert media_type == PARQUET_MEDIA_TYPE, f"{label}: wrong media type"
        assert num_rows > 0, f"{label}: Parquet table has no rows"
