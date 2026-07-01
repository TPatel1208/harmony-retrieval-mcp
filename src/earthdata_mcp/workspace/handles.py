"""Typed handle resolvers — one implementation of each resolution sequence.

Six tool modules (``coverage``, ``retrieval``, ``preview``, ``transform``,
``provenance``, ``understanding``) each used to hand-roll their own version of
"type-check the handle prefix -> resolve within the workspace (the isolation
gate) -> pull a field out of the payload." The shapes differ slightly
(``dataset_`` -> ``concept_id``, ``aoi_`` -> ``bbox``, ``obs_``/``cube_`` ->
``storage_key`` + ``media_type``), but the pattern — and the isolation gate
every one depends on — is identical. These three functions are the one place
that sequence lives; every tool call site is a thin caller of one or more of
them.

``store.get_handle`` is where ``CrossWorkspaceError``/``HandleNotFoundError``
are raised (PLAN.md §4.6) — these resolvers never catch or reshape that; they
only add the type-check before it and the payload-extraction after it.
"""

from __future__ import annotations

from earthdata_mcp.workspace.models import HandleType, handle_type_of
from earthdata_mcp.workspace.store import WorkspaceStore

__all__ = ["resolve_dataset", "resolve_aoi", "resolve_materialized"]


async def resolve_dataset(store: WorkspaceStore, workspace_id: str, handle: str) -> str:
    """Resolve a ``dataset_`` handle to its ``concept_id``."""
    if handle_type_of(handle) is not HandleType.DATASET:
        raise ValueError(f"expected a dataset_ handle, got {handle!r}")
    record = await store.get_handle(workspace_id, handle)  # isolation gate
    concept_id = record.payload.get("concept_id")
    if not concept_id:
        raise ValueError(f"dataset handle {handle!r} payload missing 'concept_id'")
    return concept_id


async def resolve_aoi(
    store: WorkspaceStore, workspace_id: str, handle: str
) -> tuple[float, float, float, float]:
    """Resolve an ``aoi_`` handle to its bbox as ``(west, south, east, north)``.

    A ``None`` handle (a caller's "no AOI given") is the caller's decision, not
    this resolver's — callers that accept an optional AOI must check for
    ``None`` themselves before calling.
    """
    if handle_type_of(handle) is not HandleType.AOI:
        raise ValueError(f"expected an aoi_ handle, got {handle!r}")
    record = await store.get_handle(workspace_id, handle)  # isolation gate
    bbox = record.payload.get("bbox")
    if not bbox or len(bbox) != 4:
        raise ValueError(f"aoi handle {handle!r} payload missing or malformed 'bbox'")
    return tuple(float(c) for c in bbox)


async def resolve_materialized(
    store: WorkspaceStore, workspace_id: str, handle: str
) -> tuple[str, str, dict]:
    """Resolve an ``obs_``/``cube_`` handle to ``(storage_key, media_type, payload)``.

    Requires the handle to be a materialized (``status == "ready"``) result
    with both fields present; a pending or malformed handle raises rather than
    returning a partial result.
    """
    if handle_type_of(handle) not in (HandleType.OBS, HandleType.CUBE):
        raise ValueError(f"expected an obs_ or cube_ handle, got {handle!r}")
    record = await store.get_handle(workspace_id, handle)  # isolation gate
    payload = record.payload
    storage_key = payload.get("storage_key")
    media_type = payload.get("media_type")
    if payload.get("status") != "ready" or not storage_key or not media_type:
        raise ValueError(f"handle {handle!r} is not a materialized result")
    return storage_key, media_type, payload
