"""HarmonyProvider — wraps harmony-py (PLAN.md §4.3, Phase 4.5 tests).

The harmony-py ``Client`` is mocked throughout: we assert *our* glue — the
plan → ``harmony.Request`` mapping (esp. the matched ``service_id``), the
status → ``JobState`` mapping, the ``on_progress`` hook, and result persistence
through ``StorageBackend``. We never exercise the real Harmony network here (that
is the ``@live`` test).
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.base import (
    AOI,
    JobRef,
    JobStatus,
    RetrievalPlan,
    TimeRange,
    TransformSpec,
)
from earthdata_mcp.providers.harmony import HarmonyProvider

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"

SUBSETTER = "l2-subsetter-batchee-stitchee-concise"
IMAGENATOR = "asdc/imagenator_l2"
# Reusable placeholder UUID for tests that just need a structurally valid job ID.
_VALID_UUID = "550e8400-e29b-41d4-a716-446655440000"


@pytest.fixture
def l2_caps() -> CollectionCapabilities:
    data = json.loads((_FIXTURES / "tempo_no2_l2_capabilities.json").read_text())
    return CollectionCapabilities.from_harmony_capabilities(data)


def _provider(caps, client=None, **kw) -> HarmonyProvider:
    return HarmonyProvider(caps, client=client, **kw)


# -- capability gate -------------------------------------------------------


def test_can_handle_true_when_a_service_matches(l2_caps) -> None:
    p = _provider(l2_caps, client=MagicMock())
    plan = RetrievalPlan(output_format="application/netcdf", needs_bbox=True)
    assert p.can_handle(plan) is True


def test_can_handle_false_for_the_union_trap(l2_caps) -> None:
    # bbox + png: the union advertises it, but no single service does both.
    p = _provider(l2_caps, client=MagicMock())
    plan = RetrievalPlan(output_format="image/png", needs_bbox=True)
    assert p.can_handle(plan) is False


# -- submit: the plan -> harmony.Request mapping ---------------------------


@pytest.mark.asyncio
async def test_submit_maps_plan_to_request_with_matched_service(l2_caps) -> None:
    client = MagicMock()
    client.submit.return_value = "job-123"
    p = _provider(l2_caps, client=client)

    plan = RetrievalPlan(
        output_format="application/netcdf",
        needs_bbox=True,
        needs_variable=True,
        concept_id="C2930725014-LARC_CLOUD",
        aoi=AOI(bbox=(-105.0, 37.0, -104.0, 38.0)),
        time_range=TimeRange(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
        ),
        transform=TransformSpec(
            output_format="application/netcdf", variables=("NO2",)
        ),
    )
    ref = await p.submit(plan)

    assert isinstance(ref, JobRef)
    assert ref.provider == "harmony"
    assert ref.provider_job_id == "job-123"

    # The request handed to harmony-py pins the MATCHED service and carries the
    # plan's transforms — never the wrong service, never the union.
    client.submit.assert_called_once()
    request = client.submit.call_args.args[0]
    assert request.service_id == SUBSETTER
    assert request.format == "application/netcdf"
    assert request.variables == ["NO2"]
    assert (request.spatial.w, request.spatial.s, request.spatial.e, request.spatial.n) == (
        -105.0,
        37.0,
        -104.0,
        38.0,
    )
    assert request.temporal == {"start": plan.time_range.start, "stop": plan.time_range.end}


@pytest.mark.asyncio
async def test_submit_variable_png_pins_the_imagenator(l2_caps) -> None:
    client = MagicMock()
    client.submit.return_value = "job-png"
    p = _provider(l2_caps, client=client)
    plan = RetrievalPlan(
        output_format="image/png",
        needs_variable=True,
        concept_id="C2930725014-LARC_CLOUD",
        transform=TransformSpec(output_format="image/png", variables=("NO2",)),
    )
    await p.submit(plan)
    request = client.submit.call_args.args[0]
    assert request.service_id == IMAGENATOR


@pytest.mark.asyncio
async def test_submit_unpinned_when_no_service_satisfies(l2_caps) -> None:
    """Union trap (services exist, none match) → submit UNPINNED; server picks chain."""
    client = MagicMock()
    client.submit.return_value = "job-unpinned"
    p = _provider(l2_caps, client=client)
    plan = RetrievalPlan(output_format="image/png", needs_bbox=True)  # union trap

    ref = await p.submit(plan)

    assert ref.provider_job_id == "job-unpinned"
    client.submit.assert_called_once()
    request = client.submit.call_args.args[0]
    assert request.service_id is None  # no service pinned — Harmony picks the chain


@pytest.mark.asyncio
async def test_harmony_submit_uses_hint_when_caps_empty() -> None:
    """When caps.services is empty (failed fetch) and hint is set, synthesize and submit."""
    from earthdata_mcp.providers._capabilities import CollectionCapabilities

    empty_caps = CollectionCapabilities(
        concept_id="C2-TEST",
        short_name="TEST",
        processing_level="3",
        output_shape="swath",
        native_formats=frozenset(),
        direct_s3=None,
        services=[],
    )
    client = MagicMock()
    client.submit.return_value = "job-xyz"
    p = _provider(empty_caps, client=client, service_name_hint="my-service")
    plan = RetrievalPlan(output_format="application/netcdf4", concept_id="C2-TEST")
    ref = await p.submit(plan)

    assert ref.provider_job_id == "job-xyz"
    request = client.submit.call_args.args[0]
    assert request.service_id == "my-service"


@pytest.mark.asyncio
async def test_harmony_submit_ignores_hint_when_caps_present_but_no_match(
    l2_caps,
) -> None:
    """Union-trap with caps present: a stored hint is NOT reused (it only covers a
    failed caps fetch). We submit UNPINNED and let the server pick the chain."""
    client = MagicMock()
    client.submit.return_value = "job-unpinned"
    p = _provider(l2_caps, client=client, service_name_hint=SUBSETTER)
    plan = RetrievalPlan(output_format="image/png", needs_bbox=True)  # union trap

    await p.submit(plan)

    request = client.submit.call_args.args[0]
    assert request.service_id is None


@pytest.mark.asyncio
async def test_harmony_poll_rejects_non_uuid_job_id(l2_caps) -> None:
    p = _provider(l2_caps, client=MagicMock())
    for bad_id in ("abc", "None"):
        with pytest.raises(ValueError, match="expected UUID"):
            await p.poll(JobRef(provider="harmony", provider_job_id=bad_id))


# -- poll: Harmony status -> JobState --------------------------------------


@pytest.mark.parametrize(
    ("harmony_status", "expected"),
    [
        ("accepted", JobState.SUBMITTED),
        ("running", JobState.RUNNING),
        ("running_with_errors", JobState.RUNNING),
        ("paused", JobState.RUNNING),
        ("previewing", JobState.RUNNING),
        ("successful", JobState.READY),
        ("complete_with_errors", JobState.READY),
        ("failed", JobState.FAILED),
        ("canceled", JobState.CANCELLED),
    ],
)
@pytest.mark.asyncio
async def test_poll_maps_status(l2_caps, harmony_status, expected) -> None:
    expires = dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc)
    client = MagicMock()
    client.status.return_value = {
        "status": harmony_status,
        "progress": 42,
        "message": "working",
        "data_expiration": expires,
    }
    p = _provider(l2_caps, client=client)
    status = await p.poll(JobRef(provider="harmony", provider_job_id=_VALID_UUID))
    assert status.state is expected
    assert status.progress == 42
    assert status.output_expires_at == expires


@pytest.mark.asyncio
async def test_poll_failed_surfaces_error(l2_caps) -> None:
    client = MagicMock()
    client.status.return_value = {
        "status": "failed",
        "progress": 0,
        "message": "boom",
        "errors": ["service exploded"],
    }
    p = _provider(l2_caps, client=client)
    status = await p.poll(JobRef(provider="harmony", provider_job_id=_VALID_UUID))
    assert status.state is JobState.FAILED
    assert "service exploded" in status.error


@pytest.mark.asyncio
async def test_poll_invokes_on_progress(l2_caps) -> None:
    seen: list[JobStatus] = []
    client = MagicMock()
    client.status.return_value = {"status": "running", "progress": 10, "message": "m"}
    p = _provider(l2_caps, client=client, on_progress=seen.append)
    status = await p.poll(JobRef(provider="harmony", provider_job_id=_VALID_UUID))
    assert seen == [status]
    assert seen[0].progress == 10


# -- materialize: persist through StorageBackend ---------------------------


@pytest.mark.asyncio
async def test_materialize_persists_result_to_storage(
    l2_caps, local_backend, tmp_path
) -> None:
    # harmony-py downloads to a directory and returns Futures whose result() is the
    # downloaded filename. Mock that and assert the bytes land in StorageBackend.
    payload = b"\x89netcdf-bytes"
    downloaded = tmp_path / "TEMPO_NO2_L2.subset.nc4"
    downloaded.write_bytes(payload)

    future = MagicMock()
    future.result.return_value = str(downloaded)
    client = MagicMock()
    client.download_all.return_value = iter([future])

    p = _provider(l2_caps, client=client, storage=local_backend)
    result = await p.materialize(
        JobRef(provider="harmony", provider_job_id=_VALID_UUID, job_handle="job_abc")
    )

    assert result.size_bytes == len(payload)
    assert result.storage_key.startswith("harmony/job_abc/")
    assert await local_backend.get(result.storage_key) == payload


@pytest.mark.asyncio
async def test_materialize_bundles_multiple_granules(
    l2_caps, local_backend, tmp_path
) -> None:
    # A multi-granule Harmony result (one file per input granule) is bundled into a
    # single netCDF-bundle zip so the whole request materializes — not just the first
    # granule — matching OPeNDAPProvider and what tools/_dataio concatenates on read.
    import io
    import zipfile

    members = {
        "TEMPO_NO2_L3_S002.nc4": b"\x89granule-002",
        "TEMPO_NO2_L3_S003.nc4": b"\x89granule-003",
        "TEMPO_NO2_L3_S004.nc4": b"\x89granule-004",
    }
    futures = []
    for name, data in members.items():
        path = tmp_path / name
        path.write_bytes(data)
        future = MagicMock()
        future.result.return_value = str(path)
        futures.append(future)
    client = MagicMock()
    client.download_all.return_value = iter(futures)

    p = _provider(l2_caps, client=client, storage=local_backend)
    result = await p.materialize(
        JobRef(provider="harmony", provider_job_id=_VALID_UUID, job_handle="job_xyz")
    )

    assert result.media_type == "application/netcdf-bundle+zip"
    assert result.storage_key == "harmony/job_xyz/result.nc.zip"
    assert result.extra["granule_count"] == 3
    bundle = await local_backend.get(result.storage_key)
    with zipfile.ZipFile(io.BytesIO(bundle)) as zf:
        assert sorted(zf.namelist()) == sorted(members)
        for name, data in members.items():
            assert zf.read(name) == data
