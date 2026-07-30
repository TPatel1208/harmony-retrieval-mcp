"""Durable ``jobs`` table — SQLAlchemy model skeleton (PLAN.md §4.3).

Phase 1 lands the model definition only. There is intentionally NO ``create_all``
call and no migration here — table creation is owned by the migration step in
Phase 6 (and workspace/provenance tables in Phase 3).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, func, text
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
    # The provider's own status text for the current poll (e.g. Harmony's message
    # field, or AppEEARS' pending/queued/processing-step wording) — distinct from
    # `error`, which is terminal-failure-only (PRD 007).
    message: Mapped[str | None] = mapped_column(String, default=None)
    output_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    error: Mapped[str | None] = mapped_column(String, default=None)
    # Stamped unconditionally after every successful provider poll, independent
    # of whether the poll changed anything else — the durable proof-of-life a
    # row-diffed `updated_at` can't provide when a provider reports the same
    # queued state poll after poll (PRD 012). Nullable so pre-existing rows
    # (from before this column existed) fall back to `updated_at`.
    last_polled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    # The per-call EDL credential this job runs under (PRD 022), Fernet-encrypted
    # under the repo-private EARTHDATA_CREDENTIAL_KEY — written at enqueue when a
    # token was supplied, decrypted by the worker per attempt, and nulled by
    # ``crud.set_state`` the moment the job reaches any terminal state so tokens
    # never accumulate at rest. NULL means the env-credential fallback.
    credential_encrypted: Mapped[str | None] = mapped_column(String, default=None)
    # What this job *actually* delivered (PRD 023) — the resolved granule ids,
    # the provider that really ran, the realized service chain, and the
    # ``content_digest`` over them. Written once, by the worker, at the
    # transition into READY, never at planning time: this column's entire value
    # is that it is an outcome and not an intent (``request_spec`` above is the
    # intent). NULL for every handle materialized before this column existed —
    # legacy records are absent, never backfilled.
    materialization: Mapped[dict | None] = mapped_column(JSONB, default=None)
    # What this job cost to build, in wall-clock seconds from creation to READY
    # (PRD 024) — stamped once by ``crud.set_state`` at that transition. The
    # materialization cache evicts by value density, so it needs to know what it
    # would be throwing away: a fifteen-minute Harmony result and a three-second
    # OPeNDAP subset of the same size are not the same loss. NULL for every job
    # that reached READY before this column existed; the eviction score
    # substitutes a configured default rather than treating absent as free.
    rebuild_cost_seconds: Mapped[float | None] = mapped_column(Float, default=None)
    # Whether nobody asked for this job (PRD 025): it was issued by
    # adjacent-granule prefetch as a guess about the next request. Speculative
    # work is second-class *explicitly* — the eviction sweep discards it before
    # any user-requested result, it cannot be pinned, and it never chains (a
    # speculative job issues no prefetch of its own). FALSE for every job that
    # existed before this column, which is the honest default: those were all
    # asked for.
    speculative: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=text("false")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class CollectionAccess(Base):
    """Durable access-restriction memory per collection (PRD 022 cache scoping).

    A EULA-classified failure observed for a collection marks it restricted
    here, so future result-reuse/dedupe scoping decisions treat it as
    restricted even when CMR's own metadata doesn't say so.
    """

    __tablename__ = "collection_access"

    concept_id: Mapped[str] = mapped_column(String, primary_key=True)
    restricted: Mapped[bool] = mapped_column(Boolean, default=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


async def create_jobs_schema(engine: AsyncEngine) -> None:
    """Create the durable ``jobs`` (+ ``collection_access``) tables (idempotent).

    Phase 6 owns the jobs table's creation, separate from the workspace schema so
    each is created from its own ``metadata``. ``create_all`` only creates a
    missing table — it never alters an existing one — so a column added to the
    model after a table already exists (``last_polled_at``, PRD 012;
    ``credential_encrypted``, PRD 022; ``materialization``, PRD 023;
    ``speculative``, PRD 025) needs its own idempotent
    ``ADD COLUMN IF NOT EXISTS`` alongside it. A couple of columns don't warrant
    a migration framework.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_polled_at TIMESTAMPTZ")
        )
        await conn.execute(
            text(
                "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS credential_encrypted VARCHAR"
            )
        )
        await conn.execute(
            text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS materialization JSONB")
        )
        await conn.execute(
            text(
                "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS "
                "rebuild_cost_seconds DOUBLE PRECISION"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS "
                "speculative BOOLEAN NOT NULL DEFAULT FALSE"
            )
        )


async def drop_jobs_schema(engine: AsyncEngine) -> None:
    """Drop the durable ``jobs`` table (for teardown)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
