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
async def test_submit_widens_degenerate_temporal_range(l2_caps) -> None:
    """A single-day request normalizes to start == stop (a date-only
    '2024-06-15/2024-06-15' parses to midnight on both ends). Harmony rejects a
    zero-width window ("start must be earlier than its stop datetime"), so submit
    nudges stop one second forward — the window is non-degenerate and a daily
    granule still overlaps it."""
    client = MagicMock()
    client.submit.return_value = "job-day"
    p = _provider(l2_caps, client=client)
    instant = dt.datetime(2024, 6, 15, tzinfo=dt.timezone.utc)
    plan = RetrievalPlan(
        output_format="application/netcdf",
        needs_variable=True,
        concept_id="C2930725014-LARC_CLOUD",
        time_range=TimeRange(start=instant, end=instant),
        transform=TransformSpec(output_format="application/netcdf", variables=("NO2",)),
    )
    await p.submit(plan)
    request = client.submit.call_args.args[0]
    assert request.temporal["start"] == instant
    assert request.temporal["stop"] == instant + dt.timedelta(seconds=1)
    assert request.temporal["start"] < request.temporal["stop"]


@pytest.mark.asyncio
async def test_submit_preserves_non_degenerate_temporal_range(l2_caps) -> None:
    """A normal multi-day window is passed through untouched — the widening only
    fires for a zero-width start == stop range."""
    client = MagicMock()
    client.submit.return_value = "job-range"
    p = _provider(l2_caps, client=client)
    start = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc)
    plan = RetrievalPlan(
        output_format="application/netcdf",
        needs_variable=True,
        concept_id="C2930725014-LARC_CLOUD",
        time_range=TimeRange(start=start, end=end),
        transform=TransformSpec(output_format="application/netcdf", variables=("NO2",)),
    )
    await p.submit(plan)
    request = client.submit.call_args.args[0]
    assert request.temporal == {"start": start, "stop": end}


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
async def test_materialize_single_hdf4_file_gets_native_media_type(
    l2_caps, local_backend, tmp_path
) -> None:
    # MOD13Q1-style result: Harmony returns the granule as-is, no reformatting
    # service ran. The filename extension guesses netCDF, but the bytes are
    # HDF4 — the honest media type must never be a netCDF one (PRD 008).
    payload = b"\x0e\x03\x13\x01" + b"hdf4-granule-bytes"
    downloaded = tmp_path / "MOD13Q1.A2024001.h08v05.hdf"
    downloaded.write_bytes(payload)

    future = MagicMock()
    future.result.return_value = str(downloaded)
    client = MagicMock()
    client.download_all.return_value = iter([future])

    p = _provider(l2_caps, client=client, storage=local_backend)
    result = await p.materialize(
        JobRef(provider="harmony", provider_job_id=_VALID_UUID, job_handle="job_hdf4")
    )

    assert result.media_type == "application/x-hdf4"
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

    _HDF5_MAGIC = b"\x89HDF\r\n\x1a\n"
    members = {
        "TEMPO_NO2_L3_S002.nc4": _HDF5_MAGIC + b"granule-002",
        "TEMPO_NO2_L3_S003.nc4": _HDF5_MAGIC + b"granule-003",
        "TEMPO_NO2_L3_S004.nc4": _HDF5_MAGIC + b"granule-004",
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


@pytest.mark.asyncio
async def test_materialize_bundle_with_hdf4_member_gets_native_archive_type(
    l2_caps, local_backend, tmp_path
) -> None:
    # MOD13Q1-style multi-granule result: every member is untouched native HDF4.
    # The bundle must never be labeled netcdf-bundle+zip (PRD 008) — it's a
    # native archive, and downstream tools must refuse to open it as netCDF.
    import io
    import zipfile

    members = {
        "MOD13Q1.A2024001.h08v05.hdf": b"\x0e\x03\x13\x01" + b"granule-1",
        "MOD13Q1.A2024009.h08v05.hdf": b"\x0e\x03\x13\x01" + b"granule-2",
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
        JobRef(provider="harmony", provider_job_id=_VALID_UUID, job_handle="job_native")
    )

    assert result.media_type == "application/x-native-archive+zip"
    bundle = await local_backend.get(result.storage_key)
    with zipfile.ZipFile(io.BytesIO(bundle)) as zf:
        assert sorted(zf.namelist()) == sorted(members)


@pytest.mark.asyncio
async def test_materialize_mixed_bundle_gets_native_archive_type(
    l2_caps, local_backend, tmp_path
) -> None:
    # A mixed bundle (some netCDF, some unknown/HDF4 members) must be treated
    # as native/unopenable, never half-opened as netCDF (PRD 008 story 7).
    _HDF5_MAGIC = b"\x89HDF\r\n\x1a\n"
    members = {
        "good.nc4": _HDF5_MAGIC + b"granule-good",
        "bad.hdf": b"\x0e\x03\x13\x01" + b"granule-bad",
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
        JobRef(provider="harmony", provider_job_id=_VALID_UUID, job_handle="job_mixed")
    )

    assert result.media_type == "application/x-native-archive+zip"
