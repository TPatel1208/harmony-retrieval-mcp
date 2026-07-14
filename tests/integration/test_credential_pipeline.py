"""Per-call credential through the durable pipeline (PRD 022).

Prior art: ``test_durable_pipeline.py`` — everything below the provider is real
(Postgres jobs table, state machine, handle resolution, provenance DAG); only
the provider's network is faked at the worker's ``_load_provider`` seam. These
tests add the credential dimension:

* a job enqueued with a token persists it **encrypted** (never plaintext, never
  in the queue payload), the worker decrypts it **per attempt** — including
  across the Harmony→OPeNDAP fallback — and the column is **null** after any
  terminal state;
* auth failures surface as the three first-class classifications, from the
  worker as well as the tool boundary, and never trigger the OPeNDAP fallback;
* a EULA-classified failure durably flips the collection's cache scoping;
* no observability surface (logs, job error, provenance trail) ever carries a
  substring of the token.
"""

from __future__ import annotations

import base64
import json
import logging
import time
from unittest.mock import AsyncMock

import httpx
import pytest
from cryptography.fernet import Fernet

from earthdata_mcp import redaction
from earthdata_mcp.config import get_settings
from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs import worker as worker_mod
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers._capabilities import (
    CollectionCapabilities,
    ServiceCapability,
)
from earthdata_mcp.providers.base import JobRef, JobStatus, MaterializedResult
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.providers.identity import (
    decrypt_credential,
    encrypt_credential,
    reset_token_sessions,
)
from earthdata_mcp.tools import discovery as discovery_mod
from earthdata_mcp.tools.provenance import get_provenance
from earthdata_mcp.tools.retrieval import get_retrieval_status, retrieve_subset

_CONCEPT_ID = "C1234567890-LPCLOUD"
_BBOX = [-105.0, 37.0, -104.0, 38.0]
_TIME = "2024-01-01/2024-03-31"


def _jwt(uid: str = "alice", exp: float | None = None) -> str:
    def seg(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    claims: dict = {"uid": uid}
    if exp is None:
        exp = time.time() + 3600
    claims["exp"] = exp
    return f"{seg({'alg': 'RS256'})}.{seg(claims)}.per-call-signature"


@pytest.fixture(autouse=True)
def _fresh_identity_state():
    reset_token_sessions()
    redaction.clear_secrets()
    yield
    reset_token_sessions()
    redaction.clear_secrets()


@pytest.fixture
def credential_key(monkeypatch):
    """The repo-private Fernet key, present for the test's process settings."""
    key = Fernet.generate_key().decode()
    monkeypatch.setenv("EARTHDATA_CREDENTIAL_KEY", key)
    get_settings.cache_clear()
    yield key
    get_settings.cache_clear()


def _caps(access_restricted: bool | None = None) -> CollectionCapabilities:
    svc = ServiceCapability(
        service_name="l3-subsetter",
        concept_id="S100-LPCLOUD",
        subset_bbox=True,
        subset_variable=True,
        subset_temporal=True,
        output_formats=frozenset({"application/netcdf4"}),
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
        access_restricted=access_restricted,
    )


def _make_cmr(caps: CollectionCapabilities, opendap_urls: list[str] | None = None) -> CMRProvider:
    cmr = CMRProvider.__new__(CMRProvider)
    cmr.collection_capabilities = AsyncMock(return_value=caps)
    granules = [
        {"related_urls": [{"URL": url, "Subtype": "OPENDAP DATA"}]}
        for url in (opendap_urls or [])
    ]
    cmr.search_granules = AsyncMock(return_value=granules)
    cmr.get_variables = AsyncMock(return_value=[])
    cmr.get_dmr = AsyncMock(return_value=None)
    return cmr


class _FakeProvider:
    """submit→poll(READY)→materialize, writing real bytes to real storage."""

    def __init__(self, provider: str, storage) -> None:
        self._provider = provider
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
        key = f"{self._provider}/{job.job_handle or 'result'}/result"
        await self._storage.put(key, b"\x89HDF\r\n")
        return MaterializedResult(storage_key=key, media_type="application/netcdf4", size_bytes=6)


@pytest.fixture
def patch_worker_seam(monkeypatch, workspace_store):
    """Install a fake provider at ``_load_provider`` and record the per-attempt
    credential the worker decrypted and threaded in (the seam's new argument)."""
    monkeypatch.setattr(discovery_mod, "_default_store", lambda: workspace_store)

    def _install(provider) -> list[str | None]:
        seen_tokens: list[str | None] = []

        async def _fake_load_provider(spec, edl_token=None):
            seen_tokens.append(edl_token)
            return provider

        monkeypatch.setattr(worker_mod, "_load_provider", _fake_load_provider)
        return seen_tokens

    return _install


async def _submit(
    *, token, cmr, workspace_store, provenance_store, session_factory, enqueue, workspace_id,
    variables=("NDVI",),
):
    ds = await workspace_store.put_handle(
        workspace_id, discovery_mod.HandleType.DATASET, {"concept_id": _CONCEPT_ID}
    )
    aoi = await workspace_store.put_handle(
        workspace_id, discovery_mod.HandleType.AOI, {"source": "bbox", "bbox": _BBOX}
    )
    return await retrieve_subset(
        ds, aoi, _TIME, list(variables), workspace_id=workspace_id, edl_token=token,
        cmr=cmr, store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=enqueue,
    )


async def _job_row(session_factory, job_handle: str):
    async with session_factory() as session:
        return await crud.get_job_by_handle(session, job_handle)


# -- persistence: encrypted at rest, job_id-only queue, null at terminal -----


async def test_token_job_persists_encrypted_decrypts_per_attempt_and_erases(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id, credential_key,
) -> None:
    token = _jwt(uid="alice")
    seen_tokens = patch_worker_seam(_FakeProvider("harmony", local_backend))
    enqueue = AsyncMock()

    out = await _submit(
        token=token, cmr=_make_cmr(_caps()), workspace_store=workspace_store,
        provenance_store=provenance_store, session_factory=session_factory,
        enqueue=enqueue, workspace_id=workspace_id,
    )

    # The queue payload stays job_id-only: no credential ever enters Redis.
    enqueue.assert_awaited_once()
    assert enqueue.await_args.args[0] == "submit_job"
    assert all(token not in str(a) for a in enqueue.await_args.args)

    job = await _job_row(session_factory, out["job_handle"])
    # Encrypted at rest: present, never plaintext, decrypts back to the token.
    assert job.credential_encrypted
    assert token not in job.credential_encrypted
    assert decrypt_credential(job.credential_encrypted) == token
    # And never inside the spec, which flows to provenance and status enrichment.
    assert token not in json.dumps(job.request_spec)

    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    await worker_mod.submit_job(ctx, job.job_id)
    await worker_mod.poll_job(ctx, job.job_id)
    await worker_mod.materialize_job(ctx, job.job_id)

    # The worker decrypted the credential for every attempt (submit/poll/materialize).
    assert seen_tokens == [token, token, token]

    job = await _job_row(session_factory, out["job_handle"])
    assert job.state == JobState.READY.value
    # Terminal state erased the credential at rest (story 13).
    assert job.credential_encrypted is None


async def test_credential_survives_the_harmony_to_opendap_fallback(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id, credential_key,
) -> None:
    token = _jwt(uid="alice")

    class _FailThenOpendap:
        def __init__(self) -> None:
            self.calls = 0
            self._ok = _FakeProvider("opendap", local_backend)

        async def submit(self, plan):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("harmony 503")
            return await self._ok.submit(plan)

        async def poll(self, job):
            return await self._ok.poll(job)

        async def materialize(self, job):
            return await self._ok.materialize(job)

    seen_tokens = patch_worker_seam(_FailThenOpendap())
    urls = [f"https://opendap.test/granules/g{i}.nc" for i in (1, 2)]
    caps = _caps()
    caps.services = []  # no capable service; Harmony submits unpinned
    out = await _submit(
        token=token, cmr=_make_cmr(caps, opendap_urls=urls),
        workspace_store=workspace_store, provenance_store=provenance_store,
        session_factory=session_factory, enqueue=AsyncMock(), workspace_id=workspace_id,
    )
    job = await _job_row(session_factory, out["job_handle"])

    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    await worker_mod.submit_job(ctx, job.job_id)  # Harmony fails -> re-route
    # The re-routed row still carries the encrypted credential (only terminal
    # states erase it), so the fallback attempt runs as the same identity.
    job = await _job_row(session_factory, out["job_handle"])
    assert job.provider == "opendap"
    assert decrypt_credential(job.credential_encrypted) == token

    await worker_mod.submit_job(ctx, job.job_id)  # retry via OPeNDAP
    await worker_mod.poll_job(ctx, job.job_id)
    await worker_mod.materialize_job(ctx, job.job_id)

    assert all(seen == token for seen in seen_tokens)
    assert len(seen_tokens) == 4  # two submits + poll + materialize
    job = await _job_row(session_factory, out["job_handle"])
    assert job.state == JobState.READY.value
    assert job.credential_encrypted is None


async def test_no_token_call_needs_no_key_and_persists_no_credential(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id,
) -> None:
    """Regression guard (story 3): without ``edl_token`` the pipeline is
    byte-for-byte today's — no encryption key required, no credential column
    written, the worker builds providers with no per-call identity."""
    seen_tokens = patch_worker_seam(_FakeProvider("harmony", local_backend))
    out = await _submit(
        token=None, cmr=_make_cmr(_caps()), workspace_store=workspace_store,
        provenance_store=provenance_store, session_factory=session_factory,
        enqueue=AsyncMock(), workspace_id=workspace_id,
    )
    job = await _job_row(session_factory, out["job_handle"])
    assert job.credential_encrypted is None

    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    await worker_mod.submit_job(ctx, job.job_id)
    await worker_mod.poll_job(ctx, job.job_id)
    await worker_mod.materialize_job(ctx, job.job_id)

    assert seen_tokens == [None, None, None]
    job = await _job_row(session_factory, out["job_handle"])
    assert job.state == JobState.READY.value


# -- error classes: reachable from the worker, never a silent fallback --------


async def test_token_expired_mid_job_terminates_with_classification(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id, credential_key,
) -> None:
    """EDL tokens live 60 days; valid-at-enqueue but expired-at-retry is a real
    path. The job must terminate as TOKEN_EXPIRED — never silently degrade to
    the service credential, never fall back to OPeNDAP."""
    seen_tokens = patch_worker_seam(_FakeProvider("harmony", local_backend))
    out = await _submit(
        token=_jwt(uid="alice"), cmr=_make_cmr(_caps(), opendap_urls=["https://o.test/g.nc"]),
        workspace_store=workspace_store, provenance_store=provenance_store,
        session_factory=session_factory, enqueue=AsyncMock(), workspace_id=workspace_id,
    )
    job = await _job_row(session_factory, out["job_handle"])

    # Simulate the clock passing the token's exp between enqueue and the retry.
    expired = _jwt(uid="alice", exp=time.time() - 60)
    async with session_factory() as session:
        row = await crud.get_job(session, job.job_id)
        row.credential_encrypted = encrypt_credential(expired)
        await session.commit()

    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    with pytest.raises(Exception):
        await worker_mod.submit_job(ctx, job.job_id)

    job = await _job_row(session_factory, out["job_handle"])
    assert job.state == JobState.FAILED.value
    assert "TOKEN_EXPIRED" in job.error
    assert job.provider == "harmony"  # no fallback under the wrong identity
    assert job.credential_encrypted is None  # terminal erased it
    assert seen_tokens == []  # no provider was ever built with a dead credential

    status = await get_retrieval_status(
        out["job_handle"], workspace_id=workspace_id,
        store=workspace_store, session_factory=session_factory,
    )
    assert "TOKEN_EXPIRED" in status["error"]

    prov = await get_provenance(
        out["obs_handle"], workspace_id=workspace_id,
        store=workspace_store, provenance=provenance_store,
    )
    failed = [e for e in prov["events"] if e["event_type"] == "job-failed"]
    assert failed and failed[0]["detail"]["classification"] == "TOKEN_EXPIRED"


async def test_provider_401_classifies_token_invalid_and_blocks_fallback(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id, credential_key,
) -> None:
    class _Rejects401:
        async def submit(self, plan):
            request = httpx.Request("POST", "https://harmony.test/jobs")
            raise httpx.HTTPStatusError(
                "401", request=request,
                response=httpx.Response(401, request=request, text="invalid_credentials"),
            )

    patch_worker_seam(_Rejects401())
    out = await _submit(
        token=_jwt(uid="alice"),
        cmr=_make_cmr(_caps(), opendap_urls=["https://o.test/g.nc"]),
        workspace_store=workspace_store, provenance_store=provenance_store,
        session_factory=session_factory, enqueue=AsyncMock(), workspace_id=workspace_id,
    )
    job = await _job_row(session_factory, out["job_handle"])

    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    with pytest.raises(httpx.HTTPStatusError):
        await worker_mod.submit_job(ctx, job.job_id)

    job = await _job_row(session_factory, out["job_handle"])
    assert job.state == JobState.FAILED.value
    assert "TOKEN_INVALID" in job.error
    # An auth rejection is never misread as infrastructure: despite discovered
    # OPeNDAP URLs the job stays on harmony, terminally failed.
    assert job.provider == "harmony"

    prov = await get_provenance(
        out["obs_handle"], workspace_id=workspace_id,
        store=workspace_store, provenance=provenance_store,
    )
    types = [e["event_type"] for e in prov["events"]]
    assert "provider-fallback" not in types
    failed = [e for e in prov["events"] if e["event_type"] == "job-failed"]
    assert failed[0]["detail"]["classification"] == "TOKEN_INVALID"


async def test_eula_403_classifies_with_resolution_url_and_flips_scoping(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id, credential_key,
) -> None:
    resolution_url = "https://urs.earthdata.nasa.gov/approve_app?client_id=harmony"
    concept_id = f"C-eula-{workspace_id}"

    class _RejectsEula:
        async def submit(self, plan):
            # harmony-py's two-arg shape for a JSON-bodied HTTP error response.
            raise Exception(
                "Forbidden",
                json.dumps(
                    {"message": "EULA acceptance required", "resolution_url": resolution_url}
                ),
            )

    patch_worker_seam(_RejectsEula())
    caps = _caps(access_restricted=False)
    caps.concept_id = concept_id
    cmr = _make_cmr(caps)
    ds = await workspace_store.put_handle(
        workspace_id, discovery_mod.HandleType.DATASET, {"concept_id": concept_id}
    )
    aoi = await workspace_store.put_handle(
        workspace_id, discovery_mod.HandleType.AOI, {"source": "bbox", "bbox": _BBOX}
    )
    out = await retrieve_subset(
        ds, aoi, _TIME, ["NDVI"], workspace_id=workspace_id, edl_token=_jwt(uid="alice"),
        cmr=cmr, store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=AsyncMock(),
    )
    job = await _job_row(session_factory, out["job_handle"])

    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    with pytest.raises(Exception):
        await worker_mod.submit_job(ctx, job.job_id)

    job = await _job_row(session_factory, out["job_handle"])
    assert job.state == JobState.FAILED.value
    assert "EULA_NOT_ACCEPTED" in job.error
    assert resolution_url in job.error  # the actionable fix reaches the caller

    prov = await get_provenance(
        out["obs_handle"], workspace_id=workspace_id,
        store=workspace_store, provenance=provenance_store,
    )
    failed = [e for e in prov["events"] if e["event_type"] == "job-failed"]
    assert failed[0]["detail"]["classification"] == "EULA_NOT_ACCEPTED"
    assert failed[0]["detail"]["resolution_url"] == resolution_url

    # The observed EULA failure durably marks the collection restricted for
    # future cache-scoping decisions — even though CMR said unrestricted.
    async with session_factory() as session:
        assert await crud.collection_restriction(session, concept_id) is True


# -- cache scoping end-to-end ---------------------------------------------------


async def test_unrestricted_collection_reuses_across_identities(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id, credential_key,
) -> None:
    patch_worker_seam(_FakeProvider("harmony", local_backend))
    enqueue = AsyncMock()
    caps = _caps(access_restricted=False)
    caps.concept_id = f"C-open-{workspace_id}"
    cmr = _make_cmr(caps)
    ds = await workspace_store.put_handle(
        workspace_id, discovery_mod.HandleType.DATASET, {"concept_id": caps.concept_id}
    )
    aoi = await workspace_store.put_handle(
        workspace_id, discovery_mod.HandleType.AOI, {"source": "bbox", "bbox": _BBOX}
    )
    common = dict(
        cmr=cmr, store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=enqueue, workspace_id=workspace_id,
    )

    first = await retrieve_subset(ds, aoi, _TIME, ["NDVI"], edl_token=_jwt(uid="alice"), **common)
    job = await _job_row(session_factory, first["job_handle"])
    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    await worker_mod.submit_job(ctx, job.job_id)
    await worker_mod.poll_job(ctx, job.job_id)
    await worker_mod.materialize_job(ctx, job.job_id)

    second = await retrieve_subset(ds, aoi, _TIME, ["NDVI"], edl_token=_jwt(uid="bob"), **common)
    # Unrestricted: bob reuses alice's bytes — per-user identity doesn't destroy
    # the cache savings (story 11).
    assert second.get("reused") is True
    assert second["obs_handle"] == first["obs_handle"]
    enqueue.assert_awaited_once()


async def test_restricted_collection_never_reuses_across_identities(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id, credential_key,
) -> None:
    patch_worker_seam(_FakeProvider("harmony", local_backend))
    enqueue = AsyncMock()
    caps = _caps(access_restricted=True)
    caps.concept_id = f"C-gated-{workspace_id}"
    cmr = _make_cmr(caps)
    ds = await workspace_store.put_handle(
        workspace_id, discovery_mod.HandleType.DATASET, {"concept_id": caps.concept_id}
    )
    aoi = await workspace_store.put_handle(
        workspace_id, discovery_mod.HandleType.AOI, {"source": "bbox", "bbox": _BBOX}
    )
    common = dict(
        cmr=cmr, store=workspace_store, provenance=provenance_store,
        session_factory=session_factory, enqueue_fn=enqueue, workspace_id=workspace_id,
    )

    first = await retrieve_subset(ds, aoi, _TIME, ["NDVI"], edl_token=_jwt(uid="alice"), **common)
    job = await _job_row(session_factory, first["job_handle"])
    ctx = {"session_factory": session_factory, "redis": AsyncMock()}
    await worker_mod.submit_job(ctx, job.job_id)
    await worker_mod.poll_job(ctx, job.job_id)
    await worker_mod.materialize_job(ctx, job.job_id)

    other = await retrieve_subset(ds, aoi, _TIME, ["NDVI"], edl_token=_jwt(uid="bob"), **common)
    # Restricted: bob is never served bytes alice's entitlements fetched.
    assert "reused" not in other
    assert other["obs_handle"] != first["obs_handle"]
    assert enqueue.await_count == 2

    # Same identity still reuses its own result.
    again = await retrieve_subset(ds, aoi, _TIME, ["NDVI"], edl_token=_jwt(uid="alice"), **common)
    assert again.get("reused") is True
    assert again["obs_handle"] == first["obs_handle"]
    assert enqueue.await_count == 2


# -- secret hygiene: no surface carries the token -------------------------------


async def test_no_log_error_trail_or_provenance_carries_the_token(
    workspace_store, provenance_store, session_factory, patch_worker_seam,
    local_backend, workspace_id, credential_key, caplog,
) -> None:
    token = _jwt(uid="alice")

    class _LeakyProvider:
        """A worst-case provider whose failure message embeds the credential —
        exactly what the scrub chokepoints exist to catch."""

        async def submit(self, plan):
            raise RuntimeError(f"submit rejected: Authorization: Bearer {token}")

    patch_worker_seam(_LeakyProvider())
    with caplog.at_level(logging.DEBUG):
        out = await _submit(
            token=token, cmr=_make_cmr(_caps()), workspace_store=workspace_store,
            provenance_store=provenance_store, session_factory=session_factory,
            enqueue=AsyncMock(), workspace_id=workspace_id,
        )
        job = await _job_row(session_factory, out["job_handle"])
        ctx = {"session_factory": session_factory, "redis": AsyncMock()}
        with pytest.raises(RuntimeError):
            await worker_mod.submit_job(ctx, job.job_id)

    # Captured logs: nothing carries the token.
    assert token not in caplog.text
    assert all(token not in r.getMessage() for r in caplog.records)

    # The durable error string (what get_retrieval_status echoes) is scrubbed.
    job = await _job_row(session_factory, out["job_handle"])
    assert job.state == JobState.FAILED.value
    assert token not in (job.error or "")
    status = await get_retrieval_status(
        out["job_handle"], workspace_id=workspace_id,
        store=workspace_store, session_factory=session_factory,
    )
    assert token not in json.dumps(status)

    # The job trail / provenance records are scrubbed too.
    prov = await get_provenance(
        out["obs_handle"], workspace_id=workspace_id,
        store=workspace_store, provenance=provenance_store,
    )
    assert token not in json.dumps(prov)
