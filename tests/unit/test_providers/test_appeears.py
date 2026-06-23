"""AppEEARSProvider — point/area sample → Parquet glue (PLAN.md §4.4, Phase 7.4).

HTTP is mocked with pytest-httpx (``httpx_mock``); no network. We assert *our*
glue — the point-sample capability gate, the plan → AppEEARS task-body mapping, the
status → ``JobState`` mapping, and that ``materialize`` writes **Parquet** (never
Zarr) to ``StorageBackend``. The real AppEEARS task is the ``@live`` test.
"""

from __future__ import annotations

import datetime as dt
import io

import pyarrow.parquet as pq
import pytest

from earthdata_mcp.config import Settings
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.appeears import PARQUET_MEDIA_TYPE, AppEEARSProvider
from earthdata_mcp.providers.base import (
    AOI,
    JobRef,
    JobStatus,
    RetrievalPlan,
    TimeRange,
    TransformSpec,
)
from earthdata_mcp.tools import _dataio

BASE = "https://appeears.test/api"
PRODUCT = "MOD13Q1.061"
LAYER = "_250m_16_days_NDVI"


def _caps() -> CollectionCapabilities:
    return CollectionCapabilities(
        concept_id="C1-LPCLOUD",
        short_name=PRODUCT,
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset({"HDF-EOS"}),
        direct_s3=None,
        services=[],
    )


def _settings() -> Settings:
    return Settings(_env_file=None, appeears_url=BASE, earthdata_token="tok")


def _provider(*, storage=None, on_progress=None) -> AppEEARSProvider:
    return AppEEARSProvider(
        _caps(), storage=storage, settings=_settings(), on_progress=on_progress
    )


def _point_plan(**kw) -> RetrievalPlan:
    base = dict(
        output_format=PARQUET_MEDIA_TYPE,
        needs_point_sample=True,
        concept_id="C1-LPCLOUD",
        short_name=PRODUCT,
        aoi=AOI(bbox=(-105.0, 37.0, -103.0, 39.0)),  # centroid (38, -104)
        time_range=TimeRange(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
        ),
        transform=TransformSpec(output_format=PARQUET_MEDIA_TYPE, variables=(LAYER,)),
    )
    base.update(kw)
    return RetrievalPlan(**base)


# -- the Parquet media type stays in sync with the in-process I/O route ----


def test_parquet_media_type_matches_dataio() -> None:
    assert PARQUET_MEDIA_TYPE == _dataio.PARQUET_MEDIA_TYPE


# -- capability gate: point/area sample intent only ------------------------


def test_can_handle_true_for_point_sample() -> None:
    assert _provider().can_handle(_point_plan()) is True


def test_can_handle_false_without_point_sample() -> None:
    plan = _point_plan(needs_point_sample=False)
    assert _provider().can_handle(plan) is False


# -- submit: plan -> AppEEARS point task -----------------------------------


@pytest.mark.asyncio
async def test_submit_posts_point_task_and_returns_task_id(httpx_mock) -> None:
    httpx_mock.add_response(
        url=f"{BASE}/task",
        method="POST",
        json={"task_id": "abc123", "status": "pending"},
    )
    ref = await _provider().submit(_point_plan())

    assert ref.provider == "appeears"
    assert ref.provider_job_id == "abc123"
    assert ref.provider_job_url == f"{BASE}/task/abc123"

    req = httpx_mock.get_requests()[0]
    assert req.headers.get("Authorization") == "Bearer tok"
    import json

    body = json.loads(req.content)
    assert body["task_type"] == "point"
    params = body["params"]
    assert params["coordinates"][0]["latitude"] == 38.0
    assert params["coordinates"][0]["longitude"] == -104.0
    assert params["layers"] == [{"product": PRODUCT, "layer": LAYER}]
    assert params["dates"] == [{"startDate": "01-01-2024", "endDate": "03-01-2024"}]


@pytest.mark.asyncio
async def test_submit_raises_for_non_point_plan() -> None:
    with pytest.raises(ValueError):
        await _provider().submit(_point_plan(needs_point_sample=False))


@pytest.mark.asyncio
async def test_appeears_submit_raises_on_missing_token() -> None:
    """Missing earthdata_token must fail fast with a clear message, not a 403."""
    no_token = Settings(_env_file=None, appeears_url=BASE, earthdata_token="")
    p = AppEEARSProvider(_caps(), settings=no_token)
    with pytest.raises(ValueError, match="earthdata_token"):
        await p.submit(_point_plan())


# -- poll: AppEEARS status -> JobState -------------------------------------


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("pending", JobState.SUBMITTED),
        ("queued", JobState.SUBMITTED),
        ("processing", JobState.RUNNING),
        ("done", JobState.READY),
        ("error", JobState.FAILED),
        ("expired", JobState.EXPIRED),
        ("deleted", JobState.CANCELLED),
    ],
)
@pytest.mark.asyncio
async def test_poll_maps_status(httpx_mock, status, expected) -> None:
    httpx_mock.add_response(
        url=f"{BASE}/task/t1",
        json={"status": status, "progress": {"summary": 40}},
    )
    s = await _provider().poll(JobRef(provider="appeears", provider_job_id="t1"))
    assert s.state is expected


@pytest.mark.asyncio
async def test_poll_failed_surfaces_error_and_fires_on_progress(httpx_mock) -> None:
    seen: list[JobStatus] = []
    httpx_mock.add_response(
        url=f"{BASE}/task/t1", json={"status": "error", "error": "sampling failed"}
    )
    s = await _provider(on_progress=seen.append).poll(
        JobRef(provider="appeears", provider_job_id="t1")
    )
    assert s.state is JobState.FAILED
    assert s.error == "sampling failed"
    assert seen == [s]


# -- materialize: CSV bundle -> Parquet (never Zarr) -----------------------


@pytest.mark.asyncio
async def test_materialize_writes_parquet_not_zarr(httpx_mock, local_backend) -> None:
    csv = (
        b"Date,Latitude,Longitude,MOD13Q1_061__250m_16_days_NDVI\n"
        b"2024-01-01,38.0,-104.0,0.51\n2024-01-17,38.0,-104.0,0.63\n"
    )
    httpx_mock.add_response(
        url=f"{BASE}/bundle/abc123",
        json={
            "files": [
                {
                    "file_id": "g1",
                    "file_name": "task-granule-list.txt",
                    "file_type": "txt",
                },
                {
                    "file_id": "r1",
                    "file_name": "task-MOD13Q1-061-results.csv",
                    "file_type": "csv",
                },
            ]
        },
    )
    httpx_mock.add_response(url=f"{BASE}/bundle/abc123/r1", content=csv)

    p = _provider(storage=local_backend)
    ref = JobRef(provider="appeears", provider_job_id="abc123", job_handle="job_xyz")
    result = await p.materialize(ref)

    # The load-bearing constraint: tabular → Parquet, NEVER a Zarr cube.
    assert result.media_type == PARQUET_MEDIA_TYPE
    assert result.media_type != _dataio.ZARR_MEDIA_TYPE
    assert result.storage_key == "appeears/job_xyz/series.parquet"

    stored = await local_backend.get(result.storage_key)
    table = pq.read_table(io.BytesIO(stored))
    assert "MOD13Q1_061__250m_16_days_NDVI" in table.column_names
    assert table.num_rows == 2
