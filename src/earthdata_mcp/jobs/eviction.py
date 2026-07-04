"""Materialization cache eviction — enforces ``EARTHDATA_CACHE_MAX_BYTES`` (PRD 015).

The one new seam this PRD adds: constructed from the session factory (workspace
+ provenance + jobs access) and the process's :class:`StorageBackend`, exposing
a single :meth:`EvictionService.sweep` entry point. The worker is the only
caller — no other module knows eviction exists.

A sweep has two passes, in order:

1. **Orphan reclamation** — blobs under a materialization prefix with no
   handle referencing them (debris from crashed jobs) are deleted first, before
   any live result is considered.
2. **LRU eviction** — if the summed size of tracked, ``ready`` ``obs_``/``cube_``
   handles still exceeds the cap, the least-recently-used ones (by the handle's
   own ``updated_at`` — the interface is never widened for storage mtime) are
   evicted until back under the cap. The result whose materialization triggered
   the sweep is excluded from that sweep so a retrieval can never evict its own
   output before it's read.

Evicting one result: delete its blob; flip its handle payload to ``expired``
with the storage key cleared (media type and other descriptive fields
survive); record the ``expired`` provenance event with size + reason; and, for
a job-owned ``obs_`` handle whose job is ``READY``, walk that job across the
legal ``ready -> expired`` edge. A transform-owned ``cube_`` handle has no job
row — the handle status and event carry the whole story.
"""

from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.storage.backend import StorageBackend
from earthdata_mcp.workspace.models import Handle, HandleType
from earthdata_mcp.workspace.provenance import ProvenanceStore
from earthdata_mcp.workspace.store import WorkspaceStore

logger = logging.getLogger(__name__)

#: Every provider's materialized-output prefix (harmony.py/appeears.py/
#: opendap/_runtime.py's ``PROVIDER`` constants) plus the transform emit path's
#: own prefix. A blob under one of these with no referencing handle is debris.
_MATERIALIZATION_PREFIXES: tuple[str, ...] = ("harmony/", "appeears/", "opendap/", "transform/")

_EVICTABLE_TYPES: tuple[str, ...] = (HandleType.OBS.value, HandleType.CUBE.value)


class EvictionService:
    """Enforces ``max_bytes`` over the materialization cache."""

    def __init__(
        self, session_factory: async_sessionmaker, storage: StorageBackend, max_bytes: int
    ) -> None:
        self._session_factory = session_factory
        self._store = WorkspaceStore(session_factory)
        self._provenance = ProvenanceStore(session_factory)
        self._storage = storage
        self._max_bytes = max_bytes

    async def sweep(self, *, protected_handle: str | None = None) -> None:
        """Reclaim orphan blobs, then evict LRU results if still over the cap.

        ``protected_handle`` (the result whose materialization triggered this
        sweep, if any) is never evicted in this pass.
        """
        await self._reclaim_orphans()
        await self._evict_over_cap(protected_handle)

    # -- orphan reclamation --------------------------------------------------

    async def _reclaim_orphans(self) -> None:
        referenced = await self._referenced_storage_keys()
        for prefix in _MATERIALIZATION_PREFIXES:
            for key in await self._storage.list(prefix):
                if key not in referenced:
                    await self._storage.delete(key)
                    logger.info("evicted orphan blob %s (no referencing handle)", key)

    async def _referenced_storage_keys(self) -> set[str]:
        async with self._session_factory() as session:
            stmt = select(Handle.payload).where(Handle.handle_type.in_(_EVICTABLE_TYPES))
            payloads = (await session.execute(stmt)).scalars().all()
        return {
            payload["storage_key"]
            for payload in payloads
            if payload and payload.get("storage_key")
        }

    # -- LRU eviction ---------------------------------------------------------

    async def _evict_over_cap(self, protected_handle: str | None) -> None:
        async with self._session_factory() as session:
            stmt = select(Handle).where(Handle.handle_type.in_(_EVICTABLE_TYPES))
            rows = (await session.execute(stmt)).scalars().all()
            candidates = [
                (row.handle, row.workspace_id, row.payload["storage_key"], row.updated_at)
                for row in rows
                if row.payload and row.payload.get("status") == "ready" and row.payload.get("storage_key")
            ]

        sized: list[tuple[str, str, str, object, int]] = []
        total = 0
        for handle, workspace_id, key, updated_at in candidates:
            try:
                stat = await self._storage.stat(key)
            except KeyError:
                continue
            total += stat.size
            sized.append((handle, workspace_id, key, updated_at, stat.size))

        if total <= self._max_bytes:
            return

        sized.sort(key=lambda item: item[3])  # oldest updated_at first == least-recently-used
        for handle, workspace_id, key, _updated_at, size in sized:
            if total <= self._max_bytes:
                break
            if handle == protected_handle:
                continue
            await self._evict_one(handle, workspace_id, key, size)
            total -= size

    async def _evict_one(self, handle: str, workspace_id: str, key: str, size: int) -> None:
        await self._storage.delete(key)
        await self._store.update_handle(workspace_id, handle, {"status": "expired", "storage_key": None})
        await self._provenance.mark_expired(
            workspace_id, handle, detail={"size_bytes": size, "reason": "cache_eviction_cap"}
        )
        async with self._session_factory() as session:
            job = await crud.get_job_by_obs_handle(session, handle)
            if job is not None and JobState(job.state) is JobState.READY:
                await crud.transition_state(session, job.job_id, JobState.EXPIRED)
        logger.info("evicted %s (%d bytes) under the materialization cache cap", handle, size)
