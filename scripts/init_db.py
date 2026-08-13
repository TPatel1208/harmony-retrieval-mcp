"""Create the handles/provenance and durable-jobs Postgres schemas.

Idempotent — safe to re-run against an already-initialized database. Wraps
the same two calls the README used to paste inline as a `python -c` heredoc;
extracted here because a multi-line quoted `-c` argument is fragile outside
bash (shell quoting/line-continuation rules differ across sh, PowerShell,
cmd.exe), and the `scripts/` directory is already bind-mounted read-only into
the `mcp` container, so no image rebuild is needed to pick this up.

Usage:
    docker compose exec mcp python scripts/init_db.py
    uv run python scripts/init_db.py   # local (non-Docker) development
"""
import asyncio

from earthdata_mcp.db import create_engine
from earthdata_mcp.jobs.models import create_jobs_schema
from earthdata_mcp.workspace import create_schema


async def main() -> None:
    engine = create_engine()
    try:
        await create_schema(engine)  # handles + provenance tables
        await create_jobs_schema(engine)  # durable jobs table
    finally:
        await engine.dispose()
    print("schema ready")


if __name__ == "__main__":
    asyncio.run(main())
