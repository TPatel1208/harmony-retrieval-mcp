"""Workspace store (Postgres) with ownership/isolation (PLAN.md §4.6).

Every handle belongs to exactly one ``workspace_id``. Reads and writes are scoped
to a workspace; a handle owned by another workspace is **denied**, not silently
returned. Auth identity (Phase 4) maps to workspace ownership.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from earthdata_mcp.workspace.models import Base, Handle, HandleType, mint_handle


class HandleNotFoundError(KeyError):
    """No handle with this id exists at all.

    Subclasses ``KeyError`` for callers that pattern-match on it, but
    ``KeyError.__str__`` renders as ``repr(args[0])`` regardless of subclass —
    left un-overridden, that bare ``'some_handle'`` is what reaches the MCP
    client as the tool error, indistinguishable from an unhandled ``KeyError``.
    """

    def __str__(self) -> str:
        return f"unknown handle: {self.args[0]!r}"


class CrossWorkspaceError(PermissionError):
    """The handle exists but is owned by a different workspace — access denied.

    Distinct from :class:`HandleNotFoundError` internally; callers that must not
    leak existence across workspaces can collapse both to a not-found response.
    """


@dataclass(frozen=True)
class HandleRecord:
    """A detached, read-only view of a stored handle."""

    handle: str
    workspace_id: str
    handle_type: HandleType
    payload: dict
    created_at: datetime
    updated_at: datetime


async def create_schema(engine: AsyncEngine) -> None:
    """Create the workspace + provenance tables (idempotent).

    Also creates a partial unique index on ``(workspace_id, payload->>'signature')``
    for ``handle_type = 'obs'`` handles carrying a signature — the database-level
    uniqueness guarantee behind :meth:`WorkspaceStore.put_handle_unique_signature`
    (PRD 020 story 6: two concurrent identical ``retrieve_subset`` calls must
    collapse onto one job, not just usually collapse onto one).
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_handles_workspace_signature "
                "ON handles (workspace_id, (payload->>'signature')) "
                "WHERE handle_type = 'obs' AND payload ? 'signature'"
            )
        )


async def drop_schema(engine: AsyncEngine) -> None:
    """Drop the workspace + provenance tables (for teardown)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


class WorkspaceStore:
    """Persists and resolves handles, enforcing workspace ownership."""

    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory

    async def put_handle(
        self,
        workspace_id: str,
        handle_type: HandleType,
        payload: dict | None = None,
    ) -> str:
        """Mint and persist a new handle owned by ``workspace_id``."""
        handle = mint_handle(handle_type)
        async with self._session_factory() as session:
            session.add(
                Handle(
                    handle=handle,
                    workspace_id=workspace_id,
                    handle_type=HandleType(handle_type).value,
                    payload=payload or {},
                )
            )
            await session.commit()
        return handle

    async def put_handle_unique_signature(
        self, workspace_id: str, handle_type: HandleType, payload: dict
    ) -> tuple[HandleRecord, bool]:
        """Insert a handle whose ``payload["signature"]`` is unique within
        ``workspace_id`` — or discover the row that already won the race.

        Backs ``retrieve_subset``'s exact-match dedupe (PRD 020 story 6). The
        earlier ``find_handle_by_payload_value`` lookup (:func:`_check_reuse` in
        ``tools/retrieval.py``) is a check-then-act read: two concurrent
        identical requests can both miss it and both proceed to insert a
        handle, which would enqueue two jobs for the same signature. This
        closes that window at the database, via the partial unique index
        ``create_schema`` creates on ``(workspace_id, payload->>'signature')``
        for ``handle_type = 'obs'`` handles that carry a signature.

        Returns ``(record, True)`` for the winner — the row this call just
        inserted. Returns ``(record, False)`` with the pre-existing row when a
        concurrent caller already won: the caller must treat that as a reuse
        hit, never as license to insert a second job for the same signature.
        """
        signature = payload["signature"]
        handle = mint_handle(handle_type)
        row = Handle(
            handle=handle,
            workspace_id=workspace_id,
            handle_type=HandleType(handle_type).value,
            payload=payload,
        )
        async with self._session_factory() as session:
            session.add(row)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
            else:
                return _to_record(row), True

        existing = await self.find_handle_by_payload_value(
            workspace_id, handle_type, "signature", signature
        )
        if existing is None:
            # The unique index only rejects an insert when a colliding row is
            # live, so this would mean it vanished between the failed commit
            # and this lookup — not possible under Postgres's transaction
            # isolation. Surface loudly rather than silently drop the guarantee.
            raise RuntimeError(
                f"unique-signature insert for {signature!r} conflicted but no "
                "existing handle with that signature was found"
            )
        return existing, False

    async def get_handle(self, workspace_id: str, handle: str) -> HandleRecord:
        """Resolve ``handle`` within ``workspace_id``.

        Raises :class:`HandleNotFoundError` if no such handle exists, or
        :class:`CrossWorkspaceError` if it belongs to another workspace.
        """
        async with self._session_factory() as session:
            row = await session.get(Handle, handle)
            if row is None:
                raise HandleNotFoundError(handle)
            if row.workspace_id != workspace_id:
                raise CrossWorkspaceError(
                    f"handle {handle!r} is not owned by workspace {workspace_id!r}"
                )
            return _to_record(row)

    async def update_handle(
        self, workspace_id: str, handle: str, payload: dict
    ) -> HandleRecord:
        """Merge ``payload`` into a handle's payload, scoped to ``workspace_id``.

        Used by the worker to resolve a pending ``obs_`` handle once its job
        materializes (it records the durable ``storage_key`` + media type — never
        a staged-output URL). Same isolation contract as :meth:`get_handle`:
        :class:`HandleNotFoundError` if absent, :class:`CrossWorkspaceError` if
        owned by another workspace.
        """
        async with self._session_factory() as session:
            row = await session.get(Handle, handle)
            if row is None:
                raise HandleNotFoundError(handle)
            if row.workspace_id != workspace_id:
                raise CrossWorkspaceError(
                    f"handle {handle!r} is not owned by workspace {workspace_id!r}"
                )
            # Reassign a new dict so SQLAlchemy detects the JSONB change.
            row.payload = {**(row.payload or {}), **payload}
            await session.commit()
            # ``updated_at`` carries ``onupdate=func.now()``, so the flush expires it
            # (the server computes the new value) regardless of ``expire_on_commit``.
            # Refresh here, inside the async context, so building the detached record
            # below never triggers a sync lazy-load (which raises ``MissingGreenlet``).
            await session.refresh(row)
            return _to_record(row)

    async def touch_handle(self, workspace_id: str, handle: str) -> None:
        """Bump ``handle``'s ``updated_at`` to now, without touching its payload.

        The materialization cache's LRU eviction (PRD 015) orders candidates by
        ``updated_at``; ``export_result`` (PRD 017) calls this so a result a
        consumer is actively reading is never the first evicted. Same isolation
        contract as :meth:`get_handle`.
        """
        async with self._session_factory() as session:
            row = await session.get(Handle, handle)
            if row is None:
                raise HandleNotFoundError(handle)
            if row.workspace_id != workspace_id:
                raise CrossWorkspaceError(
                    f"handle {handle!r} is not owned by workspace {workspace_id!r}"
                )
            row.updated_at = func.now()
            await session.commit()

    async def find_handle_by_payload_value(
        self, workspace_id: str, handle_type: HandleType, key: str, value: str
    ) -> HandleRecord | None:
        """The most recently created handle of ``handle_type`` in ``workspace_id``
        whose ``payload[key] == value``, or ``None``.

        Backs ``retrieve_subset``'s exact-match reuse lookup (PRD 020): the
        canonical request signature lives in an ``obs_`` handle's own payload
        at creation time, so dedupe reduces to a lookup over the workspace's
        existing handles rather than a parallel index. Same workspace-scoping
        as every other read here, so a match never crosses a workspace
        boundary.

        The ``payload[key] == value`` match is a JSONB predicate pushed into
        the query (``payload->>key = value``, ``LIMIT 1``) rather than a Python
        loop over every handle of this type — on the hot submit path this
        would otherwise grow linearly with the workspace's session history.
        """
        stmt = (
            select(Handle)
            .where(Handle.workspace_id == workspace_id)
            .where(Handle.handle_type == HandleType(handle_type).value)
            .where(Handle.payload[key].astext == value)
            .order_by(Handle.created_at.desc())
            .limit(1)
        )
        async with self._session_factory() as session:
            row = (await session.execute(stmt)).scalar_one_or_none()
        return _to_record(row) if row is not None else None

    async def list_handles(
        self, workspace_id: str, handle_type: HandleType | None = None
    ) -> list[HandleRecord]:
        """List a workspace's handles, optionally filtered by type."""
        stmt = select(Handle).where(Handle.workspace_id == workspace_id)
        if handle_type is not None:
            stmt = stmt.where(Handle.handle_type == HandleType(handle_type).value)
        stmt = stmt.order_by(Handle.created_at)
        async with self._session_factory() as session:
            rows = (await session.execute(stmt)).scalars().all()
            return [_to_record(r) for r in rows]


def _to_record(row: Handle) -> HandleRecord:
    return HandleRecord(
        handle=row.handle,
        workspace_id=row.workspace_id,
        handle_type=HandleType(row.handle_type),
        payload=dict(row.payload or {}),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
