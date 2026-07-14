"""``list_workspace`` — re-enumerate a workspace's handles (PRD 019 story 4).

A reconnecting consumer (after a restart or page reload) has no conversation
history to replay handles from. This is a thin read over the existing
``WorkspaceStore.list_handles`` seam — no new state, workspace-scoped like every
other handle-resolving tool.
"""

from __future__ import annotations

from earthdata_mcp.providers.identity import parse_edl_token
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.workspace.models import HandleType
from earthdata_mcp.workspace.store import HandleRecord, WorkspaceStore


async def list_workspace(
    workspace_id: str = DEFAULT_WORKSPACE,
    handle_type: str | None = None,
    edl_token: str | None = None,
    *,
    store: WorkspaceStore | None = None,
) -> dict:
    """List ``workspace_id``'s handles, oldest first, optionally filtered by type.

    Returns ``{handles: [{handle, type, created_at, summary}]}``. ``summary`` is a
    small, type-specific slice of the handle's payload — enough to recognize the
    handle without resolving it.

    ``edl_token`` (PRD 022) is accepted for surface uniformity and validated
    at the boundary; this read touches only Postgres.
    """
    parse_edl_token(edl_token)
    store = store or _default_store()
    records = await store.list_handles(
        workspace_id, HandleType(handle_type) if handle_type else None
    )
    return {"handles": [_to_entry(r) for r in records]}


def _to_entry(record: HandleRecord) -> dict:
    return {
        "handle": record.handle,
        "type": record.handle_type.value,
        "created_at": record.created_at.isoformat(),
        "summary": _summarize(record.handle_type, record.payload),
    }


def _summarize(handle_type: HandleType, payload: dict) -> dict:
    """A compact, type-specific slice of ``payload`` — never the full payload."""
    if handle_type is HandleType.DATASET:
        collection = payload.get("collection", {})
        return {
            "concept_id": payload.get("concept_id"),
            "short_name": collection.get("short_name"),
            "version": collection.get("version"),
        }
    if handle_type is HandleType.AOI:
        return {"source": payload.get("source"), "bbox": payload.get("bbox")}
    if handle_type in (HandleType.OBS, HandleType.CUBE):
        return {"status": payload.get("status"), "media_type": payload.get("media_type")}
    if handle_type is HandleType.JOB:
        return {
            "dataset_handle": payload.get("dataset_handle"),
            "obs_handle": payload.get("obs_handle"),
        }
    if handle_type is HandleType.PREVIEW:
        return {"layer": payload.get("layer"), "bbox": payload.get("bbox")}
    return {}
