"""Durable async job model: table, state machine, and stateless worker (§4.3)."""

from __future__ import annotations

from earthdata_mcp.jobs import crud
from earthdata_mcp.jobs.models import (
    Base,
    Job,
    create_jobs_schema,
    drop_jobs_schema,
)
from earthdata_mcp.jobs.state import (
    LEGAL_TRANSITIONS,
    TERMINAL_STATES,
    IllegalTransition,
    JobState,
    assert_legal,
)

__all__ = [
    "Base",
    "IllegalTransition",
    "Job",
    "JobState",
    "LEGAL_TRANSITIONS",
    "TERMINAL_STATES",
    "assert_legal",
    "create_jobs_schema",
    "crud",
    "drop_jobs_schema",
]
