"""Durable, spec-keyed provenance DAG (PLAN.md §4.5).

Lineage edges are keyed to **durable request specs and granule IDs**, never to
ephemeral staged-output URLs — a graph of dead links is worse than none, and
Harmony's staged outputs expire (~30 days). Because the spec is durable, an
``expired`` or evicted result can be re-materialized from its lineage.

Ancestry is computed with a **recursive CTE**, written deliberately here and
tested on a deep (≥20-hop) graph. The walk is workspace-scoped at every level so
lineage never crosses a workspace boundary, and a depth guard makes an accidental
cycle terminate instead of looping forever.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from earthdata_mcp.workspace.models import (
    ProvenanceEdge,
    ProvenanceEvent,
    ProvenanceEventType,
)

# A URL is never the source of truth for an edge. Spec keys that look like a
# staged-output URL are rejected at record time so the rule can't be bypassed.
_URL_PREFIXES = ("http://", "https://", "s3://", "gs://")

# Keys that may legitimately hold durable (re-materializable) URLs — OPeNDAP
# granule endpoints are permanent CMR-resolvable references, not staged outputs.
_DURABLE_URL_KEYS: frozenset[str] = frozenset({"opendap_url"})

# Walk ancestors target -> source. MIN(depth) collapses diamonds; the depth guard
# makes an accidental cycle terminate. Workspace-scoped at both levels.
_ANCESTRY_SQL = text(
    """
    WITH RECURSIVE ancestry(handle, depth) AS (
        SELECT source_handle, 1
        FROM provenance_edges
        WHERE target_handle = :start AND workspace_id = :ws
      UNION ALL
        SELECT e.source_handle, a.depth + 1
        FROM provenance_edges e
        JOIN ancestry a ON e.target_handle = a.handle
        WHERE e.workspace_id = :ws AND a.depth < :max_depth
    )
    SELECT handle, MIN(depth) AS depth
    FROM ancestry
    GROUP BY handle
    ORDER BY depth, handle
    """
)


class ProvenanceError(Exception):
    """A provenance operation could not be completed."""


@dataclass(frozen=True)
class Ancestor:
    """One ancestor of a handle and its shortest distance (in edges)."""

    handle: str
    depth: int


@dataclass(frozen=True)
class ProvenanceEventRecord:
    """One first-class lineage event read back for a handle."""

    event_type: str
    detail: dict
    created_at: object


class ProvenanceStore:
    """Records lineage edges and events; answers ancestry queries."""

    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory

    async def record_edge(
        self,
        workspace_id: str,
        target_handle: str,
        source_handle: str,
        request_spec: dict | None = None,
        granule_ids: list[str] | None = None,
    ) -> None:
        """Record that ``target_handle`` was derived from ``source_handle``.

        The edge carries the durable, re-materializable ``request_spec`` and the
        ``granule_ids`` consumed — not a staged-output URL. A spec that smuggles
        a URL in as its source of truth is rejected.
        """
        _reject_url_spec(request_spec)
        async with self._session_factory() as session:
            session.add(
                ProvenanceEdge(
                    workspace_id=workspace_id,
                    target_handle=target_handle,
                    source_handle=source_handle,
                    request_spec=request_spec,
                    granule_ids=granule_ids,
                )
            )
            await session.commit()

    async def record_event(
        self,
        workspace_id: str,
        handle: str,
        event_type: ProvenanceEventType,
        detail: dict | None = None,
    ) -> None:
        """Record a first-class lineage event against ``handle``."""
        async with self._session_factory() as session:
            session.add(
                ProvenanceEvent(
                    workspace_id=workspace_id,
                    handle=handle,
                    event_type=ProvenanceEventType(event_type).value,
                    detail=detail or {},
                )
            )
            await session.commit()

    async def mark_expired(
        self, workspace_id: str, handle: str, detail: dict | None = None
    ) -> None:
        """Record that ``handle``'s materialized output has expired."""
        await self.record_event(
            workspace_id, handle, ProvenanceEventType.EXPIRED, detail
        )

    async def ancestry(
        self, workspace_id: str, handle: str, max_depth: int = 10_000
    ) -> list[Ancestor]:
        """Return all ancestors of ``handle`` (shortest depth first).

        Workspace-scoped: edges in other workspaces are invisible. ``max_depth``
        guards against accidental cycles.
        """
        async with self._session_factory() as session:
            rows = await session.execute(
                _ANCESTRY_SQL,
                {"start": handle, "ws": workspace_id, "max_depth": max_depth},
            )
            return [Ancestor(handle=r.handle, depth=r.depth) for r in rows]

    async def events(
        self, workspace_id: str, handle: str
    ) -> list[ProvenanceEventRecord]:
        """Return a handle's first-class lineage events, newest first.

        Workspace-scoped like :meth:`ancestry` — events in other workspaces are
        invisible. The ``created``/``materialized``/``expired``/``re-materialized``
        timeline is what ``get_provenance`` surfaces alongside the ancestry graph.
        """
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    text(
                        """
                        SELECT event_type, detail, created_at
                        FROM provenance_events
                        WHERE handle = :h AND workspace_id = :ws
                        ORDER BY created_at DESC, id DESC
                        """
                    ),
                    {"h": handle, "ws": workspace_id},
                )
            ).all()
            return [
                ProvenanceEventRecord(
                    event_type=r.event_type,
                    detail=r.detail or {},
                    created_at=r.created_at,
                )
                for r in rows
            ]

    async def re_materialize(self, workspace_id: str, handle: str) -> dict:
        """Re-materialization stub: recover the durable spec for ``handle``.

        Reads the most recent edge's ``request_spec`` (the re-materializable
        recipe), records a ``re-materialized`` event, and returns the spec. The
        actual rebuild is wired to the retrieval engine in Phase 6; this proves
        the spec — not a dead URL — is what provenance hands back.
        """
        async with self._session_factory() as session:
            spec = (
                await session.execute(
                    text(
                        """
                        SELECT request_spec
                        FROM provenance_edges
                        WHERE target_handle = :h AND workspace_id = :ws
                          AND request_spec IS NOT NULL
                        ORDER BY created_at DESC, id DESC
                        LIMIT 1
                        """
                    ),
                    {"h": handle, "ws": workspace_id},
                )
            ).scalar_one_or_none()
            if spec is None:
                raise ProvenanceError(
                    f"no re-materializable spec for handle {handle!r} "
                    f"in workspace {workspace_id!r}"
                )
            session.add(
                ProvenanceEvent(
                    workspace_id=workspace_id,
                    handle=handle,
                    event_type=ProvenanceEventType.RE_MATERIALIZED.value,
                    detail={"from_spec": True},
                )
            )
            await session.commit()
            return spec


def _reject_url_spec(request_spec: dict | None) -> None:
    """Refuse a spec whose top-level values are staged-output URLs.

    Provenance must never store an ephemeral URL as the source of truth; the spec
    is the re-materializable recipe (AOI, time range, variables, transforms).
    """
    if not request_spec:
        return
    for key, value in request_spec.items():
        if key in _DURABLE_URL_KEYS:
            continue
        if isinstance(value, str) and value.lower().startswith(_URL_PREFIXES):
            raise ProvenanceError(
                "request_spec must be re-materializable, not a staged-output URL"
            )
