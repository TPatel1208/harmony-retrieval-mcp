"""Workspace store (Postgres) with ownership/isolation (PLAN.md §4.6).

Every handle belongs to exactly one ``workspace_id``. Reads and writes are scoped
to a workspace; a handle owned by another workspace is **denied**, not silently
returned. Auth identity (Phase 4) maps to workspace ownership.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from earthdata_mcp.workspace.models import Base, Handle, HandleType, mint_handle


class HandleNotFoundError(KeyError):
    """No handle with this id exists at all."""


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
    """Create the workspace + provenance tables (idempotent)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


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
