"""Durable ``jobs`` table — SQLAlchemy model skeleton (PLAN.md §4.3).

Phase 1 lands the model definition only. There is intentionally NO ``create_all``
call and no migration here — table creation is owned by the migration step in
Phase 6 (and workspace/provenance tables in Phase 3).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for the durable job model.

    Independent of the workspace/provenance ``Base`` (``workspace/models.py``);
    the two metadatas create disjoint tables, so creating one schema never touches
    the other's.
    """


class Job(Base):
    """A durable retrieval job. State is persisted; the worker is stateless."""

    __tablename__ = "jobs"

    job_id: Mapped[str] = mapped_column(String, primary_key=True)
    job_handle: Mapped[str] = mapped_column(String, unique=True, index=True)
    obs_handle: Mapped[str | None] = mapped_column(String, index=True, default=None)
    provider: Mapped[str] = mapped_column(String)
    # The durable, re-materializable request spec (never an ephemeral URL).
    request_spec: Mapped[dict] = mapped_column(JSONB)
    state: Mapped[str] = mapped_column(String, index=True)
    provider_job_url: Mapped[str | None] = mapped_column(String, default=None)
    progress: Mapped[int] = mapped_column(Integer, default=0)
    output_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    error: Mapped[str | None] = mapped_column(String, default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


async def create_jobs_schema(engine: AsyncEngine) -> None:
    """Create the durable ``jobs`` table (idempotent).

    Phase 6 owns the jobs table's creation, separate from the workspace schema so
    each is created from its own ``metadata``.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def drop_jobs_schema(engine: AsyncEngine) -> None:
    """Drop the durable ``jobs`` table (for teardown)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
