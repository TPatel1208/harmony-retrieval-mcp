"""Live Harmony submit through HarmonyProvider (``@pytest.mark.live``, opt-in).

The credentialed end-to-end of our harmony-py wrapper (PLAN.md §4.5). Skipped
unless EDL credentials are present, so the default unit run never touches the
network. Run on demand / nightly CI:

    EARTHDATA_TOKEN=... docker compose exec mcp pytest -m live \
        tests/live/test_harmony_submit.py -v

Target: the small, known-serviceable UAT test collection ``C1234088182-EEDTEST``
on ``Environment.UAT``. The service is discovered at runtime from UAT Harmony's
``/capabilities`` (parsed by our own ``from_harmony_capabilities``) so the test
stays correct regardless of which service backs EEDTEST, and the plan is built to
match that service — exercising the real ``find_service`` → submit → poll path.
"""

from __future__ import annotations

import os
import time

import httpx
import pytest

from earthdata_mcp.config import Settings
from earthdata_mcp.jobs.state import TERMINAL_STATES, JobState
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.base import AOI, RetrievalPlan, TransformSpec
from earthdata_mcp.providers.harmony import HarmonyProvider

pytestmark = pytest.mark.live

COLLECTION = "C1234088182-EEDTEST"
UAT_CAPABILITIES = "https://harmony.uat.earthdata.nasa.gov/capabilities"
POLL_TIMEOUT_S = 300
POLL_INTERVAL_S = 5

_HAS_CREDS = bool(
    os.getenv("EARTHDATA_TOKEN")
    or (os.getenv("EARTHDATA_USERNAME") and os.getenv("EARTHDATA_PASSWORD"))
)


def _eedtest_capabilities() -> CollectionCapabilities:
    token = os.getenv("EARTHDATA_TOKEN")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    resp = httpx.get(
        UAT_CAPABILITIES,
        params={"collectionid": COLLECTION, "format": "json"},
        headers=headers,
        timeout=30.0,
        follow_redirects=True,
    )
    resp.raise_for_status()
    return CollectionCapabilities.from_harmony_capabilities(resp.json())


@pytest.mark.skipif(not _HAS_CREDS, reason="no EDL credentials in environment")
def test_live_harmony_submit_poll_to_ready() -> None:
    import harmony

    caps = _eedtest_capabilities()
    if not caps.services:
        pytest.skip(f"{COLLECTION} advertises no Harmony service in UAT right now")

    # Build a plan that the first real service can satisfy, so find_service picks it.
    svc = caps.services[0]
    fmt = next(iter(svc.output_formats), "application/netcdf")
    plan = RetrievalPlan(
        output_format=fmt,
        concept_id=COLLECTION,
        needs_bbox=svc.subset_bbox,
        aoi=AOI(bbox=(-140.0, 20.0, -50.0, 60.0)) if svc.subset_bbox else None,
        transform=TransformSpec(output_format=fmt),
    )
    assert caps.find_service(plan) is not None

    provider = HarmonyProvider(
        caps, settings=Settings(), env=harmony.Environment.UAT
    )

    import asyncio

    async def _run() -> JobState:
        ref = await provider.submit(plan)
        assert ref.provider_job_id
        deadline = time.monotonic() + POLL_TIMEOUT_S
        status = await provider.poll(ref)
        while status.state not in TERMINAL_STATES and time.monotonic() < deadline:
            await asyncio.sleep(POLL_INTERVAL_S)
            status = await provider.poll(ref)
        return status.state

    final = asyncio.run(_run())
    assert final is JobState.READY, f"job ended in {final}"
