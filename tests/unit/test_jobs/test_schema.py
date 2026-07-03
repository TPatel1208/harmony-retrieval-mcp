"""Idempotent jobs-schema creation, including the additive ``last_polled_at``
column (PRD 012) — schema creation is ``create_all``-based and never adds a
column to an already-existing table, so ``create_jobs_schema`` gains a small
idempotent ``ADD COLUMN IF NOT EXISTS`` step alongside it.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from earthdata_mcp.config import Settings
from earthdata_mcp.jobs.models import create_jobs_schema, drop_jobs_schema


async def test_create_jobs_schema_twice_is_idempotent() -> None:
    settings = Settings()
    engine = create_async_engine(settings.database_url)
    try:
        await create_jobs_schema(engine)
        await create_jobs_schema(engine)  # must not raise
    finally:
        await drop_jobs_schema(engine)
        await engine.dispose()


async def test_create_jobs_schema_adds_last_polled_at_to_pre_column_table() -> None:
    """A ``jobs`` table created before this PRD (no ``last_polled_at`` column)
    gets the column added by a subsequent ``create_jobs_schema`` call, rather
    than ``create_jobs_schema`` silently leaving the pre-existing table as-is."""
    settings = Settings()
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS jobs"))
            await conn.execute(
                text(
                    "CREATE TABLE jobs ("
                    "job_id VARCHAR PRIMARY KEY, "
                    "job_handle VARCHAR UNIQUE, "
                    "provider VARCHAR, "
                    "request_spec JSONB, "
                    "state VARCHAR, "
                    "progress INTEGER, "
                    "created_at TIMESTAMPTZ DEFAULT now(), "
                    "updated_at TIMESTAMPTZ DEFAULT now()"
                    ")"
                )
            )

        await create_jobs_schema(engine)  # must add the missing column, not raise

        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'jobs' AND column_name = 'last_polled_at'"
                )
            )
            assert result.scalar_one_or_none() == "last_polled_at"
    finally:
        await drop_jobs_schema(engine)
        await engine.dispose()
