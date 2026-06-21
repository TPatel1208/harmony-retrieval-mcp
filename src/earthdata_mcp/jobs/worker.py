"""Arq worker runtime (PLAN.md §4.3).

The worker is stateless; the Postgres ``jobs`` table is the source of truth. Phase
1 wires the runtime skeleton: an Arq ``WorkerSettings`` with a health-check task
and a restart-resume hook stubbed for Phase 6 (reclaim non-terminal jobs and
resume polling from ``provider_job_url``).
"""

from __future__ import annotations

from typing import Any

from arq.connections import RedisSettings

from earthdata_mcp.config import get_settings


async def healthcheck(ctx: dict[str, Any]) -> str:
    """Trivial task proving the worker is alive. Replaced by real tasks later."""
    return "ok"


async def startup(ctx: dict[str, Any]) -> None:
    """Restart-resume hook. Phase 6: reclaim non-terminal jobs and resume polling."""
    # No-op in Phase 1 (no durable jobs yet).
    return None


class WorkerSettings:
    """Arq worker configuration. Run with ``arq earthdata_mcp.jobs.worker.WorkerSettings``."""

    functions = [healthcheck]
    on_startup = startup
    redis_settings = RedisSettings.from_dsn(get_settings().redis_url)
