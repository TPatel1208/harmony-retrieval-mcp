"""Full durable retrieval against REAL Harmony (``@pytest.mark.live``, opt-in).

This is the path the Phase 8 gate refuses to skip (PLAN.md §6 Phase 8, §4.3): the
durable submit → poll → materialize lifecycle driven end-to-end through the real
worker tasks, against a real Harmony job — not a mocked provider. It is the
credentialed sibling of ``tests/integration/test_durable_pipeline.py`` (which
fakes the provider) and of ``tests/live/test_harmony_submit.py`` (which drives the
provider directly, without the durable job table).

Skipped unless EDL credentials are present, so the default unit run never touches
the network. Run on demand / nightly / release with a **UAT** Earthdata token:

    docker compose exec -e EARTHDATA_TOKEN mcp pytest -m live \\
        tests/live/test_full_retrieval.py -v

Target: the small, known-serviceable **UAT** test collection ``C1234088182-EEDTEST``.
UAT (not production) because our provider pins the matched service via ``service_id``
(CLAUDE.md hard rule / PLAN.md §4.3) — production Harmony disables client-specified
serviceId, while UAT honours it, so UAT is where the "submit exactly the matched
service" contract is actually exercisable end-to-end. The backing service is
discovered at runtime from UAT Harmony's ``/capabilities`` (parsed by our own
``from_harmony_capabilities``) and the plan is built to match it. The worker's
``_load_provider`` seam is pointed at UAT, and its obs-handle resolution at this
test's real Postgres store; everything else — job table, state machine, storage,
provenance — is the real production code path.

The token must be a **UAT** Earthdata Login token (production is a separate system).
"""

from __future__ import annotations

import asyncio
import os
import time
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from earthdata_mcp.config import Settings
from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs import worker as worker_mod
from earthdata_mcp.jobs.models import create_jobs_schema
from earthdata_mcp.jobs.state import TERMINAL_STATES, JobState
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.base import AOI, RetrievalPlan, TransformSpec
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools import discovery as discovery_mod
from earthdata_mcp.tools.provenance import get_provenance
from earthdata_mcp.tools.retrieval import retrieve_data
from earthdata_mcp.workspace.models import HandleType

pytestmark = pytest.mark.live

COLLECTION = "C1234088182-EEDTEST"
UAT_CAPABILITIES = "https://harmony.uat.earthdata.nasa.gov/capabilities"
BBOX = (-140.0, 20.0, -50.0, 60.0)
POLL_TIMEOUT_S = 600
POLL_INTERVAL_S = 5

_HAS_CREDS = bool(
    os.getenv("EARTHDATA_TOKEN")
    or (os.getenv("EARTHDATA_USERNAME") and os.getenv("EARTHDATA_PASSWORD"))
)


def _capabilities() -> CollectionCapabilities:
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
    caps = CollectionCapabilities.from_harmony_capabilities(resp.json())
    caps.concept_id = COLLECTION
    return caps


@pytest.mark.skipif(not _HAS_CREDS, reason="no EDL credentials in environment")
def test_live_full_durable_retrieval_to_ready() -> None:
    caps = _capabilities()
    if not caps.services:
        pytest.skip(f"{COLLECTION} advertises no Harmony service in UAT right now")
    # retrieve_data builds a bbox plan, so pick a service that actually does bbox
    # (EEDTEST's first service is a reprojector that does not) and use its format.
    bbox_services = [s for s in caps.services if s.subset_bbox and s.output_formats]
    if not bbox_services:
        pytest.skip(f"{COLLECTION} has no bbox-subsetting service in UAT right now")
    svc = bbox_services[0]
    fmt = next(iter(svc.output_formats))

    # Sanity: the bbox plan retrieve_data builds resolves to that service.
    probe = RetrievalPlan(
        output_format=fmt,
        concept_id=COLLECTION,
        needs_bbox=True,
        aoi=AOI(bbox=BBOX),
        transform=TransformSpec(output_format=fmt),
    )
    assert caps.find_service(probe) is svc

    asyncio.run(_run(caps, fmt))


async def _run(caps: CollectionCapabilities, fmt: str) -> None:
    import harmony
    from sqlalchemy.ext.asyncio import create_async_engine

    from earthdata_mcp.db import create_session_factory
    from earthdata_mcp.providers.harmony import HarmonyProvider
    from earthdata_mcp.workspace import ProvenanceStore, WorkspaceStore, create_schema

    settings = Settings()
    engine = create_async_engine(settings.database_url)
    await create_schema(engine)
    await create_jobs_schema(engine)
    session_factory = create_session_factory(engine)
    store = WorkspaceStore(session_factory)
    provenance = ProvenanceStore(session_factory)
    workspace_id = f"ws-live-{uuid4().hex}"

    # CMR stub returning the runtime-discovered UAT caps (production CMR may not
    # serve the EEDTEST UAT collection).
    cmr = CMRProvider.__new__(CMRProvider)
    cmr.collection_capabilities = AsyncMock(return_value=caps)

    # The two seams a real worker reaches for: rebuild the provider against UAT
    # Harmony (where serviceId pinning is honoured), and resolve the obs handle
    # through this test's real store.
    async def _load_provider(spec) -> "HarmonyProvider":
        return HarmonyProvider(caps, settings=settings, env=harmony.Environment.UAT)

    orig_load_provider = worker_mod._load_provider
    orig_default_store = discovery_mod._default_store
    worker_mod._load_provider = _load_provider
    discovery_mod._default_store = lambda: store
    try:
        ds = await store.put_handle(
            workspace_id, HandleType.DATASET, {"concept_id": COLLECTION}
        )
        aoi = await store.put_handle(
            workspace_id, HandleType.AOI, {"source": "bbox", "bbox": list(BBOX)}
        )

        out = await retrieve_data(
            ds, aoi, "", workspace_id=workspace_id, output_format=fmt,
            cmr=cmr, store=store, provenance=provenance,
            session_factory=session_factory, enqueue_fn=AsyncMock(),
        )
        assert out["provider"] == "harmony"

        async with session_factory() as session:
            job = await crud.get_job_by_handle(session, out["job_handle"])
            job_id = job.job_id

        ctx = {"session_factory": session_factory, "redis": AsyncMock()}
        await worker_mod.submit_job(ctx, job_id)

        # Drive the real poll/materialize loop until the job reaches a terminal state.
        deadline = time.monotonic() + POLL_TIMEOUT_S
        while time.monotonic() < deadline:
            await worker_mod.poll_job(ctx, job_id)
            async with session_factory() as session:
                job = await crud.get_job(session, job_id)
            state = JobState(job.state)
            if state is JobState.MATERIALIZING:
                await worker_mod.materialize_job(ctx, job_id)
                async with session_factory() as session:
                    job = await crud.get_job(session, job_id)
                state = JobState(job.state)
            if state in TERMINAL_STATES:
                break
            await asyncio.sleep(POLL_INTERVAL_S)

        async with session_factory() as session:
            job = await crud.get_job(session, job_id)
        assert job.state == JobState.READY.value, f"job ended in {job.state}: {job.error}"

        obs = await store.get_handle(workspace_id, out["obs_handle"])
        assert obs.payload["status"] == "ready"
        assert obs.payload["storage_key"]

        prov = await get_provenance(
            out["obs_handle"], workspace_id=workspace_id,
            store=store, provenance=provenance,
        )
        assert ds in {a["handle"] for a in prov["ancestors"]}
    finally:
        worker_mod._load_provider = orig_load_provider
        discovery_mod._default_store = orig_default_store
        await engine.dispose()
