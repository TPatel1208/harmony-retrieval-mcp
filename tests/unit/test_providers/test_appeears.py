"""AppEEARSProvider — point/area sample → Parquet glue (PLAN.md §4.4, Phase 7.4).

HTTP is mocked with pytest-httpx (``httpx_mock``); no network. We assert *our*
glue — the point-sample capability gate, the plan → AppEEARS task-body mapping, the
status → ``JobState`` mapping, and that ``materialize`` writes **Parquet** (never
Zarr) to ``StorageBackend``. The real AppEEARS task is the ``@live`` test.
"""

from __future__ import annotations

import datetime as dt
import io
import json

import httpx
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
SHORT_NAME = "MOD13Q1"
VERSION = "061"
PRODUCT = f"{SHORT_NAME}.{VERSION}"   # "MOD13Q1.061" — the AppEEARS product identifier
UMM_V_LAYER = "250m 16 days NDVI"    # what CMR / UMM-V returns to the user
APPEEARS_LAYER = "_250m_16_days_NDVI" # AppEEARS internal layer identifier


def _caps(*, short_name: str = SHORT_NAME, version: str = VERSION) -> CollectionCapabilities:
    return CollectionCapabilities(
        concept_id="C1-LPCLOUD",
        short_name=short_name,
        version=version,
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset({"HDF-EOS"}),
        direct_s3=None,
        services=[],
    )


def _settings() -> Settings:
    return Settings(_env_file=None, appeears_url=BASE, edl_username="u", edl_password="p")


def _provider(*, storage=None, on_progress=None, caps=None) -> AppEEARSProvider:
    return AppEEARSProvider(
        caps or _caps(), storage=storage, settings=_settings(), on_progress=on_progress
    )


def _point_plan(**kw) -> RetrievalPlan:
    base = dict(
        output_format=PARQUET_MEDIA_TYPE,
        needs_point_sample=True,
        concept_id="C1-LPCLOUD",
        short_name=SHORT_NAME,
        aoi=AOI(bbox=(-105.0, 37.0, -103.0, 39.0)),  # centroid (38, -104)
        time_range=TimeRange(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
        ),
        transform=TransformSpec(output_format=PARQUET_MEDIA_TYPE, variables=(UMM_V_LAYER,)),
    )
    base.update(kw)
    return RetrievalPlan(**base)


def _mock_login(httpx_mock, token: str = "session-tok") -> None:
    httpx_mock.add_response(
        url=f"{BASE}/login", method="POST", json={"token": token}
    )


def _mock_layer_list(
    httpx_mock,
    product: str = PRODUCT,
    layers: dict | None = None,
) -> None:
    """Mock GET /product/{product} with a minimal AppEEARS layer map.

    AppEEARS returns the layer dict directly at /product/{productId} — there is
    no /layer sub-resource (that path returns 404).
    """
    if layers is None:
        layers = {APPEEARS_LAYER: {"Description": "NDVI 250m 16 days", "Units": "NDVI"}}
    httpx_mock.add_response(
        url=f"{BASE}/product/{product}",
        method="GET",
        json=layers,
    )


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
    """The task body must use the versioned product id and the AppEEARS layer identifier
    resolved from the UMM-V variable name supplied by the caller."""
    _mock_login(httpx_mock)
    _mock_layer_list(httpx_mock)
    httpx_mock.add_response(
        url=f"{BASE}/task",
        method="POST",
        json={"task_id": "abc123", "status": "pending"},
    )
    ref = await _provider().submit(_point_plan())

    assert ref.provider == "appeears"
    assert ref.provider_job_id == "abc123"
    assert ref.provider_job_url == f"{BASE}/task/abc123"

    # requests[0]=POST /login, [1]=GET /product/.../layer, [2]=POST /task
    task_req = httpx_mock.get_requests()[2]
    assert task_req.headers.get("Authorization") == "Bearer session-tok"

    body = json.loads(task_req.content)
    assert body["task_type"] == "point"
    params = body["params"]
    assert params["coordinates"][0]["latitude"] == 38.0
    assert params["coordinates"][0]["longitude"] == -104.0
    assert params["layers"] == [{"product": PRODUCT, "layer": APPEEARS_LAYER}]
    assert params["dates"] == [{"startDate": "01-01-2024", "endDate": "03-01-2024"}]


@pytest.mark.asyncio
async def test_submit_resolves_layer_with_no_leading_underscore(httpx_mock) -> None:
    """MOD11A1 layers don't use a leading underscore; normalization must handle both styles."""
    mod11_caps = _caps(short_name="MOD11A1", version="061")
    provider = AppEEARSProvider(mod11_caps, settings=_settings())
    plan = RetrievalPlan(
        output_format=PARQUET_MEDIA_TYPE,
        needs_point_sample=True,
        concept_id="C2-LPCLOUD",
        short_name="MOD11A1",
        aoi=AOI(bbox=(-105.0, 37.0, -103.0, 39.0)),
        time_range=TimeRange(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
        ),
        transform=TransformSpec(output_format=PARQUET_MEDIA_TYPE, variables=("LST Day 1km",)),
    )
    _mock_login(httpx_mock)
    _mock_layer_list(
        httpx_mock,
        product="MOD11A1.061",
        layers={"LST_Day_1km": {"Description": "LST Day"}, "LST_Night_1km": {"Description": "LST Night"}},
    )
    httpx_mock.add_response(url=f"{BASE}/task", method="POST", json={"task_id": "def456"})

    await provider.submit(plan)

    task_req = httpx_mock.get_requests()[2]
    body = json.loads(task_req.content)
    assert body["params"]["layers"] == [{"product": "MOD11A1.061", "layer": "LST_Day_1km"}]


@pytest.mark.asyncio
async def test_submit_raises_when_product_not_in_appeears_catalog(httpx_mock) -> None:
    """A 404 from GET /product/{product} means the dataset is not in AppEEARS's
    curated catalog — submit must raise RuntimeError immediately rather than
    forwarding an unknown product to POST /task and receiving a cryptic 400."""
    _mock_login(httpx_mock)
    httpx_mock.add_response(
        url=f"{BASE}/product/{PRODUCT}",
        method="GET",
        status_code=404,
    )

    with pytest.raises(RuntimeError, match="not in the AppEEARS product catalog"):
        await _provider().submit(_point_plan())


@pytest.mark.asyncio
async def test_submit_raises_on_unmatched_variable(httpx_mock) -> None:
    """When a UMM-V name has no normalized match in the AppEEARS layer list, submit
    must raise immediately naming the available layers — not send a broken layer
    name to POST /task, which AppEEARS rejects with an opaque 400."""
    _mock_login(httpx_mock)
    _mock_layer_list(httpx_mock)  # only contains APPEEARS_LAYER, not "Unknown Variable"
    plan = _point_plan(
        transform=TransformSpec(
            output_format=PARQUET_MEDIA_TYPE, variables=("Unknown Variable",)
        )
    )

    with pytest.raises(RuntimeError, match="Unknown Variable"):
        await _provider().submit(plan)

    # No /task request was ever sent — the bad request never left the client.
    assert len(httpx_mock.get_requests()) == 2


@pytest.mark.asyncio
async def test_submit_task_rejection_surfaces_response_body(httpx_mock) -> None:
    """A 400 from POST /task must surface AppEEARS's error body in the raised
    exception — a bare ``response.raise_for_status()`` drops it, leaving the
    caller with only "400 BAD REQUEST" and no clue what was wrong."""
    _mock_login(httpx_mock)
    _mock_layer_list(httpx_mock)
    httpx_mock.add_response(
        url=f"{BASE}/task",
        method="POST",
        status_code=400,
        json={"message": "startDate must precede endDate"},
    )

    with pytest.raises(httpx.HTTPStatusError, match="startDate must precede endDate"):
        await _provider().submit(_point_plan())


@pytest.mark.asyncio
async def test_submit_raises_for_non_point_plan() -> None:
    with pytest.raises(ValueError):
        await _provider().submit(_point_plan(needs_point_sample=False))


@pytest.mark.asyncio
async def test_appeears_submit_raises_on_missing_token() -> None:
    """Missing edl_username/edl_password must fail fast with a clear message, not a network call."""
    no_creds = Settings(
        _env_file=None, appeears_url=BASE, edl_username="", edl_password=""
    )
    p = AppEEARSProvider(_caps(), settings=no_creds)
    with pytest.raises(ValueError, match="EDL_USERNAME"):
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
    _mock_login(httpx_mock)
    httpx_mock.add_response(
        url=f"{BASE}/task/t1",
        json={"status": status, "progress": {"summary": 40}},
    )
    s = await _provider().poll(JobRef(provider="appeears", provider_job_id="t1"))
    assert s.state is expected


@pytest.mark.asyncio
async def test_poll_failed_surfaces_error_and_fires_on_progress(httpx_mock) -> None:
    seen: list[JobStatus] = []
    _mock_login(httpx_mock)
    httpx_mock.add_response(
        url=f"{BASE}/task/t1", json={"status": "error", "error": "sampling failed"}
    )
    s = await _provider(on_progress=seen.append).poll(
        JobRef(provider="appeears", provider_job_id="t1")
    )
    assert s.state is JobState.FAILED
    assert s.error == "sampling failed"
    assert seen == [s]


# -- retry: re-login on 401/403 mid-session --------------------------------


@pytest.mark.asyncio
async def test_poll_retries_on_403_with_new_token(httpx_mock) -> None:
    # Initial login
    _mock_login(httpx_mock, token="token-1")
    # First poll → 403 (session expired)
    httpx_mock.add_response(url=f"{BASE}/task/t1", method="GET", status_code=403)
    # Re-login issues a new token
    _mock_login(httpx_mock, token="token-2")
    # Retry succeeds
    httpx_mock.add_response(
        url=f"{BASE}/task/t1",
        json={"status": "done", "progress": {"summary": 100}},
    )
    s = await _provider().poll(JobRef(provider="appeears", provider_job_id="t1"))
    assert s.state is JobState.READY

    # requests: [0] login, [1] GET /task/t1 (403), [2] re-login, [3] GET /task/t1 (200)
    retry_req = httpx_mock.get_requests()[3]
    assert retry_req.headers.get("Authorization") == "Bearer token-2"


# -- materialize: CSV bundle -> Parquet (never Zarr) -----------------------


@pytest.mark.asyncio
async def test_materialize_raises_on_empty_bundle(httpx_mock, local_backend) -> None:
    """When AppEEARS returns no CSV (no valid data at the point/time range), materialize
    must raise RuntimeError with a message that names the cause — not a raw bundle dump."""
    _mock_login(httpx_mock)
    httpx_mock.add_response(
        url=f"{BASE}/bundle/no-data",
        json={
            "files": [
                {"file_id": "j1", "file_name": "task-request.json", "file_type": "json"},
                {"file_id": "x1", "file_name": "task-metadata.xml", "file_type": "xml"},
                {"file_id": "r1", "file_name": "README", "file_type": "txt"},
            ]
        },
    )
    p = _provider(storage=local_backend)
    ref = JobRef(provider="appeears", provider_job_id="no-data", job_handle="job_nd")
    with pytest.raises(RuntimeError, match="no tabular|no valid data"):
        await p.materialize(ref)


@pytest.mark.asyncio
async def test_materialize_writes_parquet_not_zarr(httpx_mock, local_backend) -> None:
    csv = (
        b"Date,Latitude,Longitude,MOD13Q1_061__250m_16_days_NDVI\n"
        b"2024-01-01,38.0,-104.0,0.51\n2024-01-17,38.0,-104.0,0.63\n"
    )
    _mock_login(httpx_mock)
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
