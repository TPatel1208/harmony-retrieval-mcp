"""``export_result`` + ``rematerialize`` — the consumer data-egress contract (PRD 017).

No tool hands a consumer bytes, a path, or a URL until now: the storage key lives
in a handle's payload and was only ever read by this server's own preview/transform
tools. These two read-side tools close that gap and make the "provenance records
the spec, never the ephemeral URL" hard rule cash out for consumers:

* :func:`export_result` resolves an ``obs_``/``cube_`` handle to a URI a consumer
  can open directly with xarray/pyarrow — ``file://`` from the local backend,
  a presigned ``https://`` GET URL from an S3 backend (never hard-coded; see
  :meth:`~earthdata_mcp.storage.backend.StorageBackend.uri`). It also LRU-touches
  the handle so a result a consumer is actively reading is never the first thing
  the eviction cap reclaims.
* :func:`rematerialize` re-enqueues the durable job behind an evicted ``obs_``
  handle from its **already-persisted** request spec — the same enqueue path a
  fresh ``retrieve_*`` call uses — so recovery never needs the original tool
  arguments. A handle whose result is still cached answers immediately (no
  provider round-trip); one derived by a transform (``cube_``, no job behind it)
  answers a structured ``not_found`` rather than inventing a spec.

Neither tool records a provenance edge (a read, not a transform); the ancestry
graph is left exactly as it was.
"""

from __future__ import annotations

from uuid import uuid4

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.storage import StorageBackend, get_storage_backend
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.tools.retrieval import (
    EnqueueFn,
    _default_enqueue,
    _default_provenance,
    _default_session_factory,
)
from earthdata_mcp.workspace.models import HandleType, ProvenanceEventType, handle_type_of
from earthdata_mcp.workspace.provenance import ProvenanceStore
from earthdata_mcp.workspace.store import CrossWorkspaceError, HandleNotFoundError, WorkspaceStore

#: PRD 014-style not-found message, byte-identical for an unknown and a
#: cross-workspace handle so existence never leaks (see retrieval.py's twin).
_NOT_FOUND_MESSAGE = "no result found for handle {handle!r}"

_REMATERIALIZE_HINT = (
    "storage_uri is cache-managed and may be evicted; if it stops resolving, "
    "call rematerialize(handle) to re-enqueue a durable job from the persisted "
    "request spec, then export_result again once it is ready"
)

_storage: StorageBackend | None = None


def _default_storage() -> StorageBackend:
    """Process-wide storage backend, built lazily so import has no side effects."""
    global _storage
    if _storage is None:
        _storage = get_storage_backend()
    return _storage


async def export_result(
    handle: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    storage: StorageBackend | None = None,
) -> dict:
    """Resolve a materialized ``obs_``/``cube_`` handle to an openable URI.

    Returns ``{handle, status: "ready", storage_uri, media_type, size_bytes,
    rematerialize_hint}`` for a materialized result — ``storage_uri`` is
    scheme-agnostic (``file://`` locally, a presigned ``https://`` URL for an S3
    backend), so a consumer's open-by-scheme logic survives a move to hosted
    deployment unchanged. LRU-touches the handle so a result being actively read
    is never the first the eviction cap reclaims.

    A pending (not yet materialized) or expired (evicted) handle answers with a
    structured ``{handle, status, message}`` instead of raising (PRD 014); an
    expired answer's ``message`` names ``rematerialize`` as the recovery path.
    An unknown or cross-workspace handle collapses to the same ``not_found``
    answer so existence never leaks. A ``job_`` handle (or any non-``obs_``/
    ``cube_`` prefix) raises ``ValueError`` — a caller bug, not a missing result.
    """
    if handle_type_of(handle) not in (HandleType.OBS, HandleType.CUBE):
        raise ValueError(f"export_result expects an obs_ or cube_ handle, got {handle!r}")
    store = store or _default_store()

    try:
        record = await store.get_handle(workspace_id, handle)
    except (HandleNotFoundError, CrossWorkspaceError):
        return {
            "handle": handle,
            "status": "not_found",
            "message": _NOT_FOUND_MESSAGE.format(handle=handle),
        }

    payload = record.payload
    status = payload.get("status")

    if status == "expired":
        return {
            "handle": handle,
            "status": "expired",
            "message": (
                f"handle {handle!r} was evicted from the materialization cache; "
                f"call rematerialize({handle!r}) to recover it"
            ),
        }

    if status != "ready" or not payload.get("storage_key") or not payload.get("media_type"):
        return {
            "handle": handle,
            "status": status or "pending",
            "message": f"handle {handle!r} is not yet materialized (status={status!r})",
        }

    storage_key = payload["storage_key"]
    storage = storage or _default_storage()
    size_bytes = payload.get("size_bytes")
    if size_bytes is None:
        size_bytes = (await storage.stat(storage_key)).size
    storage_uri = await storage.uri(storage_key)

    await store.touch_handle(workspace_id, handle)

    return {
        "handle": handle,
        "status": "ready",
        "storage_uri": storage_uri,
        "media_type": payload["media_type"],
        "size_bytes": size_bytes,
        "rematerialize_hint": _REMATERIALIZE_HINT,
    }


async def rematerialize(
    handle: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    provenance: ProvenanceStore | None = None,
    session_factory=None,
    enqueue_fn: EnqueueFn | None = None,
) -> dict:
    """Re-enqueue the durable job behind an evicted ``obs_``/``cube_`` handle.

    Reads the request spec **already persisted with the original job** and
    resubmits it unchanged via the same enqueue path a fresh ``retrieve_*`` call
    uses — never inventing a new spec, never accepting overrides (that would be a
    new retrieval, not a replay). Returns ``{job_handle, obs_handle, status}`` for
    the newly-enqueued job.

    A handle whose result is still cached (not evicted) answers immediately with
    its existing job/status — a cache hit costs no provider round-trip. A handle
    with no job behind it (a transform-derived ``cube_``, or an orphan) cannot be
    rematerialized and answers a structured ``{handle, status: "not_found",
    message}``, matching the unknown/cross-workspace handle answer (PRD 014) so
    none of these cases raises. A ``job_`` handle (or any non-``obs_``/``cube_``
    prefix) raises ``ValueError`` — a caller bug, not a missing result.
    """
    if handle_type_of(handle) not in (HandleType.OBS, HandleType.CUBE):
        raise ValueError(f"rematerialize expects an obs_ or cube_ handle, got {handle!r}")
    store = store or _default_store()
    provenance = provenance or _default_provenance()
    session_factory = session_factory or _default_session_factory()
    enqueue_fn = enqueue_fn or _default_enqueue

    try:
        record = await store.get_handle(workspace_id, handle)
    except (HandleNotFoundError, CrossWorkspaceError):
        return {
            "handle": handle,
            "status": "not_found",
            "message": _NOT_FOUND_MESSAGE.format(handle=handle),
        }

    async with session_factory() as session:
        job = await crud.get_job_by_obs_handle(session, handle)

    if record.payload.get("status") != "expired":
        # Cache hit (still ready) or already in flight (pending): no re-enqueue
        # needed, hand back what's already there — near-instant, no provider
        # round-trip.
        return {
            "job_handle": job.job_handle if job is not None else None,
            "obs_handle": handle,
            "status": record.payload.get("status", "unknown"),
        }

    if job is None:
        return {
            "handle": handle,
            "status": "not_found",
            "message": (
                f"handle {handle!r} has no job behind it — it cannot be "
                "rematerialized (only a job-backed obs_ result can be replayed)"
            ),
        }

    new_job_id = uuid4().hex
    new_job_handle = await store.put_handle(
        workspace_id, HandleType.JOB, payload={"obs_handle": handle}
    )
    await store.update_handle(workspace_id, handle, {"status": "pending"})
    async with session_factory() as session:
        await crud.create_job(
            session,
            job_id=new_job_id,
            job_handle=new_job_handle,
            obs_handle=handle,
            provider=job.provider,
            request_spec=job.request_spec,
        )
    await provenance.record_event(
        workspace_id,
        handle,
        ProvenanceEventType.RE_MATERIALIZED,
        detail={"job_handle": new_job_handle, "previous_job_handle": job.job_handle},
    )
    await enqueue_fn("submit_job", new_job_id)

    return {
        "job_handle": new_job_handle,
        "obs_handle": handle,
        "status": JobState.PENDING.value,
    }
