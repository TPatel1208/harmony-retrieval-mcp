"""``pin_result`` — exempt a result from cap-driven eviction (PRD 024).

The materialization cache is bounded by bytes, and until now the only thing that
kept a result alive was having been read recently. That is the wrong protection
for a dataset a researcher returns to across weeks: their own unrelated work
evicts it. A pin says "keep this", durably, in the handle's own payload — so it
survives a restart, and the eviction sweep simply never selects it.

Pinning is bounded by ``pinned_bytes_max_per_workspace``, because an unbounded
pin is a way to monopolise a shared cache. Past the ceiling the pin is
**refused** with a message that says so, never silently ignored: a researcher who
believes their data is protected when it is not is worse off than one who was
told no.

Same isolation contract as :func:`~earthdata_mcp.tools.export.export_result` —
an unknown and a cross-workspace handle collapse to one ``not_found`` answer, so
existence never leaks across workspaces (PRD 014).
"""

from __future__ import annotations

from earthdata_mcp.config import get_settings
from earthdata_mcp.providers.identity import parse_edl_token
from earthdata_mcp.storage import StorageBackend
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.tools.export import _default_storage
from earthdata_mcp.workspace.models import HandleType, handle_type_of
from earthdata_mcp.workspace.store import (
    CrossWorkspaceError,
    HandleNotFoundError,
    HandleRecord,
    WorkspaceStore,
)

#: Byte-identical for an unknown and a cross-workspace handle (see export.py's twin).
_NOT_FOUND_MESSAGE = "no result found for handle {handle!r}"


async def pin_result(
    handle: str,
    pinned: bool = True,
    workspace_id: str = DEFAULT_WORKSPACE,
    edl_token: str | None = None,
    *,
    store: WorkspaceStore | None = None,
    storage: StorageBackend | None = None,
    max_pinned_bytes: int | None = None,
) -> dict:
    """Mark an ``obs_``/``cube_`` handle exempt from cap-driven eviction, or clear it.

    Returns ``{handle, status, pinned, pinned_bytes, pinned_bytes_max}``.
    ``status`` is ``"pinned"``/``"unpinned"`` on success and ``"refused"`` when
    the pin would push this workspace past its pinned-bytes ceiling — in which
    case ``pinned`` stays ``False`` and ``message`` explains the limit.

    A speculative (prefetched, PRD 025) handle is likewise ``"refused"``: a
    result nobody has asked for is disposable by construction, and pinning it
    would exempt it from the very rule that keeps speculation from costing a
    researcher their own data. Requesting that window promotes the handle, and a
    promoted handle pins like any other.

    A handle whose result has been evicted answers ``{status: "expired"}``: there
    are no bytes left to protect, and ``rematerialize`` is the recovery path. An
    unknown or cross-workspace handle answers the same ``not_found`` as every
    other handle-resolving tool. A ``job_`` handle (or any non-``obs_``/``cube_``
    prefix) raises ``ValueError`` — a caller bug, not a missing result.

    Unpinning is always allowed and never consults the ceiling: taking a pin
    back can only free bytes.

    ``edl_token`` (PRD 022) is accepted for surface uniformity and validated at
    the boundary; this write touches only Postgres.
    """
    parse_edl_token(edl_token)
    if handle_type_of(handle) not in (HandleType.OBS, HandleType.CUBE):
        raise ValueError(f"pin_result expects an obs_ or cube_ handle, got {handle!r}")
    store = store or _default_store()
    storage = storage or _default_storage()
    if max_pinned_bytes is None:
        max_pinned_bytes = get_settings().earthdata_pinned_bytes_max_per_workspace

    try:
        record = await store.get_handle(workspace_id, handle)
    except (HandleNotFoundError, CrossWorkspaceError):
        return {
            "handle": handle,
            "status": "not_found",
            "message": _NOT_FOUND_MESSAGE.format(handle=handle),
        }

    if not pinned:
        await store.update_handle(workspace_id, handle, {"pinned": False})
        return {
            "handle": handle,
            "status": "unpinned",
            "pinned": False,
            "pinned_bytes": await _pinned_bytes(store, storage, workspace_id, exclude=handle),
            "pinned_bytes_max": max_pinned_bytes,
        }

    if record.payload.get("speculative"):
        # PRD 025: a prefetched result is disposable by construction, and the
        # eviction rule that discards it before any requested result is exactly
        # what stops speculation from costing a researcher their own data. A pin
        # would exempt it from that rule. Refused rather than ignored, and the
        # message names the one action that makes it pinnable: ask for it.
        return {
            "handle": handle,
            "status": "refused",
            "pinned": False,
            "message": (
                f"handle {handle!r} holds a speculative (prefetched) result "
                "nobody has requested yet, and speculative results cannot be "
                "pinned; retrieve this window explicitly — the matching request "
                "promotes it to an ordinary result, which can then be pinned"
            ),
        }

    if record.payload.get("status") != "ready":
        return {
            "handle": handle,
            "status": record.payload.get("status") or "pending",
            "pinned": False,
            "message": (
                f"handle {handle!r} holds no cached bytes to pin "
                f"(status={record.payload.get('status')!r}); call "
                f"rematerialize({handle!r}) first, then pin it once it is ready"
            ),
        }

    # Excluding this handle makes re-pinning what is already pinned idempotent
    # rather than double-counting it against the ceiling.
    others = await _pinned_bytes(store, storage, workspace_id, exclude=handle)
    size = await _size_of(storage, record)
    if others + size > max_pinned_bytes:
        return {
            "handle": handle,
            "status": "refused",
            "pinned": False,
            "pinned_bytes": others,
            "pinned_bytes_max": max_pinned_bytes,
            "message": (
                f"pinning {handle!r} ({size} bytes) would put workspace "
                f"{workspace_id!r} at {others + size} pinned bytes, over its "
                f"{max_pinned_bytes}-byte ceiling; unpin something first or raise "
                "EARTHDATA_PINNED_BYTES_MAX_PER_WORKSPACE"
            ),
        }

    await store.update_handle(workspace_id, handle, {"pinned": True})
    return {
        "handle": handle,
        "status": "pinned",
        "pinned": True,
        "pinned_bytes": others + size,
        "pinned_bytes_max": max_pinned_bytes,
    }


async def _pinned_bytes(
    store: WorkspaceStore, storage: StorageBackend, workspace_id: str, *, exclude: str
) -> int:
    """Bytes this workspace currently holds pinned, ignoring ``exclude``."""
    total = 0
    for handle_type in (HandleType.OBS, HandleType.CUBE):
        for record in await store.list_handles(workspace_id, handle_type):
            if record.handle == exclude or not record.payload.get("pinned"):
                continue
            if record.payload.get("status") != "ready":
                continue
            total += await _size_of(storage, record)
    return total


async def _size_of(storage: StorageBackend, record: HandleRecord) -> int:
    """A ready result's size — the persisted value, or ``stat`` as fallback.

    The worker persists ``size_bytes`` beside the storage key it describes, so
    the common path costs no storage round-trip; the fallback covers handles
    written before it did. A key that has vanished counts as zero rather than
    raising: a missing blob must not make pinning impossible.
    """
    size = record.payload.get("size_bytes")
    if isinstance(size, int):
        return size
    try:
        return (await storage.stat(record.payload["storage_key"])).size
    except KeyError:
        return 0
