"""Materialization cache eviction — enforces ``EARTHDATA_CACHE_MAX_BYTES``
(PRD 015; policy replaced by PRD 024).

Constructed from the session factory (workspace + provenance + jobs access) and
the process's :class:`StorageBackend`, exposing a single
:meth:`EvictionService.sweep` entry point. The worker is the only caller — no
other module knows eviction exists.

A sweep has two passes, in order:

1. **Orphan reclamation** — blobs under a materialization prefix with no handle
   referencing them (debris from crashed jobs) are deleted. This lists every
   blob under four prefixes, so it runs on a timer rather than after every
   materialization: it is debris cleanup, not admission control.
2. **Cost-aware eviction** — if the summed size of tracked, ``ready``
   ``obs_``/``cube_`` handles exceeds the cap, results are evicted until back
   under it. What goes is decided by three things, in this order:

   * **Pins.** A pinned result is never selected. Its bytes still count toward
     the cap; if pinned bytes *alone* exceed the cap nothing is evicted and the
     situation is logged loudly — a full cache is recoverable, silently
     discarding data a user explicitly protected is not.
   * **Fairness.** Bytes are taken from the workspace furthest over its fair
     share (cap ÷ active workspaces), so one user's heavy week cannot evict
     another user's results. A single-workspace deployment is unaffected.
   * **Speculation.** Within a workspace, every result nobody asked for (a
     prefetch, PRD 025) is evicted before *any* result someone did, whatever
     their sizes, costs and recencies say. A prefetch that could cost a
     researcher their own data would be worse than no prefetch at all, so this
     is a lexicographic first key and not a weight.
   * **Value density.** Among equally-speculative candidates, the highest
     :meth:`_Candidate.score` goes first: the most bytes reclaimed for the
     least rebuild pain, least-recently-*read* first. This subsumes LRU — the
     thing worth minimising is time to rebuild what was thrown away, not bytes
     held, so a fifteen-minute Harmony result no longer loses to a
     three-second OPeNDAP subset merely for being slightly older.

   The result whose materialization triggered the sweep is excluded from it, so
   a retrieval can never evict its own output before it is read.

Sizes come from the ``size_bytes`` the worker persists beside the storage key,
not from a ``stat`` per handle — a sweep runs after every materialization and
must not cost a storage round-trip per cached result. The ``StorageBackend``
interface is never widened for this (no storage mtime, no bulk stat).

Evicting one result: delete its blob; flip its handle payload to ``expired``
with the storage key cleared (media type and other descriptive fields
survive); record the ``expired`` provenance event with size, reason, discarded
rebuild cost and score; and, for a job-owned ``obs_`` handle whose job is
``READY``, walk that job across the legal ``ready -> expired`` edge. A
transform-owned ``cube_`` handle has no job row — the handle status and event
carry the whole story.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from time import monotonic

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

#: Fallback build cost for a result whose job never recorded one — a legacy row,
#: or a transform-owned ``cube_`` with no job at all. Deliberately not zero:
#: zero cost makes a candidate infinitely evictable, which would purge exactly
#: the oldest and most established data first (PRD 024).
DEFAULT_REBUILD_COST_SECONDS = 60.0


async def under_fair_share(
    session_factory: async_sessionmaker,
    workspace_id: str,
    *,
    max_bytes: int,
    extra_bytes: int = 0,
) -> bool:
    """Would ``workspace_id`` still be within its fair share holding
    ``extra_bytes`` more (PRD 024's fairness rule, PRD 025's admission gate)?

    Fair share is the same quantity :meth:`EvictionService._evict_over_cap`
    takes bytes by — ``max_bytes ÷ active workspaces`` — and it lives here so
    there is exactly one definition of it rather than a second one drifting in
    the caller. ``extra_bytes`` is what the caller is *about to* add, so a
    workspace comfortably under its share today cannot admit something that
    puts it over.

    Read from the ``size_bytes`` handles persist and nothing else: no ``stat``
    fallback, no storage round-trip. An admission gate runs before the bytes
    exist and must not cost a walk of the cache; a handle too old to carry its
    own size simply doesn't count toward anyone's footprint here.
    """
    async with session_factory() as session:
        stmt = select(Handle.workspace_id, Handle.payload).where(
            Handle.handle_type.in_(_EVICTABLE_TYPES)
        )
        rows = (await session.execute(stmt)).all()

    held: dict[str, int] = {}
    for row_workspace, payload in rows:
        if not payload or payload.get("status") != "ready":
            continue
        size = payload.get("size_bytes")
        if not isinstance(size, int):
            continue
        held[row_workspace] = held.get(row_workspace, 0) + size

    # A workspace holding nothing yet is still an active workspace the moment it
    # asks for room, so it counts itself into the denominator.
    workspaces = set(held) | {workspace_id}
    fair_share = max_bytes / len(workspaces)
    return held.get(workspace_id, 0) + extra_bytes <= fair_share


@dataclass(frozen=True)
class _Candidate:
    """One ready result the sweep may evict, with everything scoring needs."""

    handle: str
    workspace_id: str
    storage_key: str
    size_bytes: int
    last_used: datetime
    rebuild_cost_seconds: float
    pinned: bool
    #: Nobody asked for this result — adjacent-granule prefetch guessed it
    #: (PRD 025). Sorted ahead of every requested candidate regardless of score.
    speculative: bool = False

    def eviction_key(self, now: datetime) -> tuple[bool, float]:
        """Where this candidate sits in its workspace's eviction order.

        Ascending; the queue is popped from the back, so the largest key goes
        first. ``speculative`` leads the tuple, which is what makes speculation
        strictly second-class: **every** speculative candidate is evicted before
        **any** requested one, whatever their sizes, costs and recencies say
        (PRD 025 story 3). A prefetch that could evict data a researcher asked
        for would be worse than no prefetch at all, so this cannot be a weight —
        it has to be a lexicographic first key.
        """
        return (self.speculative, self.score(now))

    def score(self, now: datetime) -> float:
        """Value density: bytes reclaimed per unit of rebuild pain, aged up.

        ``size_bytes / (rebuild_cost_seconds * recency_weight)`` with
        ``recency_weight = 1 / (1 + hours_since_last_use)``, i.e.

            score = size_bytes * (1 + hours_since_last_use) / rebuild_cost_seconds

        Highest score is evicted first: big, cheap to rebuild, and untouched
        for a long time. This subsumes LRU — recency is one factor rather than
        the sole key — so a fifteen-minute Harmony result no longer loses to a
        three-second OPeNDAP subset merely for being slightly older.
        """
        age_hours = max((now - _aware(self.last_used)).total_seconds(), 0.0) / 3600
        return self.size_bytes * (1 + age_hours) / self.rebuild_cost_seconds


def _aware(value: datetime) -> datetime:
    """Postgres hands back tz-aware datetimes; a naive one is read as UTC."""
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


class EvictionService:
    """Enforces ``max_bytes`` over the materialization cache."""

    def __init__(
        self,
        session_factory: async_sessionmaker,
        storage: StorageBackend,
        max_bytes: int,
        *,
        default_rebuild_cost_seconds: float = DEFAULT_REBUILD_COST_SECONDS,
        orphan_sweep_interval_seconds: float = 3600.0,
    ) -> None:
        self._session_factory = session_factory
        self._store = WorkspaceStore(session_factory)
        self._provenance = ProvenanceStore(session_factory)
        self._storage = storage
        self._max_bytes = max_bytes
        self._default_rebuild_cost = default_rebuild_cost_seconds
        self._orphan_interval = orphan_sweep_interval_seconds
        self._last_orphan_sweep: float | None = None

    async def sweep(self, *, protected_handle: str | None = None) -> None:
        """Reclaim orphan blobs if due, then evict over-cap results by value.

        ``protected_handle`` (the result whose materialization triggered this
        sweep, if any) is never evicted in this pass.
        """
        await self._reclaim_orphans_if_due()
        await self._evict_over_cap(protected_handle)

    # -- orphan reclamation --------------------------------------------------

    async def _reclaim_orphans_if_due(self) -> None:
        """Run orphan reclamation at most once per ``orphan_sweep_interval``.

        Reclamation lists every blob under four prefixes and materializes the
        full referenced-key set — O(cache), on a path that runs after *every*
        materialization. It is debris cleanup rather than admission control, so
        a timer is the right cadence; the cap is enforced on every sweep either
        way (PRD 024 story 7).

        The timestamp is in-process, and deliberately so: a restart simply runs
        one extra reclamation, which is harmless. Nothing durable depends on it
        — a blob missed this hour is collected the next.
        """
        now = monotonic()
        if self._last_orphan_sweep is not None and (
            now - self._last_orphan_sweep < self._orphan_interval
        ):
            return
        self._last_orphan_sweep = now
        await self._reclaim_orphans()

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

    # -- cost-aware eviction --------------------------------------------------

    async def _evict_over_cap(self, protected_handle: str | None) -> None:
        candidates = await self._candidates()
        total = sum(c.size_bytes for c in candidates)
        if total <= self._max_bytes:
            return

        # Pinned results count toward the cap but are never selected. If they
        # alone exceed it, no legitimate eviction can bring the cache under —
        # so evict nothing and say so loudly. A full cache is recoverable;
        # silently discarding data a user explicitly protected is not.
        pinned_bytes = sum(c.size_bytes for c in candidates if c.pinned)
        if pinned_bytes > self._max_bytes:
            logger.warning(
                "materialization cache is over its cap but pinned results alone "
                "hold %d bytes against a %d-byte cap — evicting nothing; raise "
                "EARTHDATA_CACHE_MAX_BYTES or unpin something",
                pinned_bytes,
                self._max_bytes,
                extra={"pinned_bytes": pinned_bytes, "max_bytes": self._max_bytes},
            )
            return

        now = datetime.now(timezone.utc)
        # Held per workspace, pinned bytes included: a workspace's footprint is
        # what it occupies, whether or not this sweep may reclaim it.
        held: dict[str, int] = {}
        for candidate in candidates:
            held[candidate.workspace_id] = held.get(candidate.workspace_id, 0) + candidate.size_bytes
        fair_share = self._max_bytes / len(held)

        # Within a workspace, highest value density first: the most bytes
        # reclaimed for the least rebuild pain, oldest first among otherwise
        # comparable candidates. Queues are popped from the back.
        queues: dict[str, list[_Candidate]] = {}
        for candidate in candidates:
            if candidate.pinned or candidate.handle == protected_handle:
                continue
            queues.setdefault(candidate.workspace_id, []).append(candidate)
        for queue in queues.values():
            queue.sort(key=lambda c: c.eviction_key(now))

        while total > self._max_bytes and queues:
            # Take from whoever is furthest over their fair share, so one
            # workspace's heavy week never evicts another's results. With a
            # single workspace every candidate shares one queue and this
            # reduces exactly to score order.
            workspace_id = max(queues, key=lambda ws: held[ws] - fair_share)
            candidate = queues[workspace_id].pop()
            if not queues[workspace_id]:
                del queues[workspace_id]
            await self._evict_one(candidate, candidate.score(now))
            total -= candidate.size_bytes
            held[workspace_id] -= candidate.size_bytes

    async def _candidates(self) -> list[_Candidate]:
        """Every ready, evictable result, with its size, recency and build cost."""
        async with self._session_factory() as session:
            stmt = select(Handle).where(Handle.handle_type.in_(_EVICTABLE_TYPES))
            rows = [
                row
                for row in (await session.execute(stmt)).scalars().all()
                if row.payload
                and row.payload.get("status") == "ready"
                and row.payload.get("storage_key")
            ]
            costs = await crud.rebuild_costs(session, [row.handle for row in rows])

        candidates: list[_Candidate] = []
        for row in rows:
            size = await self._size_of(row.payload)
            if size is None:
                continue
            candidates.append(
                _Candidate(
                    handle=row.handle,
                    workspace_id=row.workspace_id,
                    storage_key=row.payload["storage_key"],
                    size_bytes=size,
                    # A read if there has been one; otherwise the row's own last
                    # change, so a handle nobody has read since the column
                    # existed scores exactly as it did before (PRD 024).
                    last_used=row.last_accessed_at or row.updated_at,
                    rebuild_cost_seconds=costs.get(row.handle) or self._default_rebuild_cost,
                    pinned=bool(row.payload.get("pinned")),
                    speculative=bool(row.payload.get("speculative")),
                )
            )
        return candidates

    async def _size_of(self, payload: dict) -> int | None:
        """A result's size in bytes — from the payload, or ``stat`` as fallback.

        The worker persists ``size_bytes`` beside the storage key it describes,
        so a sweep over a healthy cache issues no storage round-trips at all
        (PRD 024 story 7); ``export_result`` already reads it the same way. The
        ``stat`` path remains for handles written before that, and a key that
        has vanished underneath us drops out of the candidate set entirely.
        """
        size = payload.get("size_bytes")
        if isinstance(size, int):
            return size
        try:
            return (await self._storage.stat(payload["storage_key"])).size
        except KeyError:
            return None

    async def _evict_one(self, candidate: _Candidate, score: float) -> None:
        handle, workspace_id = candidate.handle, candidate.workspace_id
        await self._storage.delete(candidate.storage_key)
        await self._store.update_handle(workspace_id, handle, {"status": "expired", "storage_key": None})
        await self._provenance.mark_expired(
            workspace_id,
            handle,
            detail={
                "size_bytes": candidate.size_bytes,
                "reason": "cache_eviction_cap",
                "rebuild_cost_seconds": candidate.rebuild_cost_seconds,
                "score": score,
                "speculative": candidate.speculative,
            },
        )
        async with self._session_factory() as session:
            job = await crud.get_job_by_obs_handle(session, handle)
            if job is not None and JobState(job.state) is JobState.READY:
                await crud.transition_state(session, job.job_id, JobState.EXPIRED)
        # Structured, so cap sizing becomes an evidence question rather than a
        # guess: what was reclaimed, and what rebuild cost was thrown away. A
        # speculative candidate discarded unused is additionally named as
        # prefetch waste — a feature that spends quota on guesses has to report
        # what the wrong guesses cost or its hit rate is unfalsifiable (PRD 025).
        # Imported here rather than at module scope: prefetch reads this module's
        # fair share, so a top-level import back the other way would be a cycle.
        from earthdata_mcp.jobs.prefetch import PREFETCH_WASTED

        event = PREFETCH_WASTED if candidate.speculative else "cache-eviction"
        logger.info(
            "evicted %s under the materialization cache cap",
            handle,
            extra={
                "event": event,
                "handle": handle,
                "workspace_id": workspace_id,
                "bytes_reclaimed": candidate.size_bytes,
                "rebuild_cost_seconds": candidate.rebuild_cost_seconds,
                "score": score,
                "speculative": candidate.speculative,
            },
        )
