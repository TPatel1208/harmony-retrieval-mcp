"""Async SQLAlchemy engine/session factory.

Phase 1 only provides the engine seam. Table creation is deliberately NOT done
here — workspace/provenance tables land in Phase 3 and the durable ``jobs`` table
in Phase 6 (via migrations, not ``create_all``).
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)

from earthdata_mcp.config import Settings, get_settings


def create_engine(settings: Settings | None = None) -> AsyncEngine:
    """Create an async engine from settings (default: the process settings)."""
    settings = settings or get_settings()
    return create_async_engine(settings.database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker:
    """Return an ``async_sessionmaker`` bound to ``engine``."""
    return async_sessionmaker(engine, expire_on_commit=False)
