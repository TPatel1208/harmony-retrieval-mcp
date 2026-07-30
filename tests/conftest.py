"""Shared test fixtures."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import or_, update
from sqlalchemy.ext.asyncio import create_async_engine

from earthdata_mcp.config import Settings
from earthdata_mcp.db import create_session_factory
from earthdata_mcp.jobs.models import Job, create_jobs_schema
from earthdata_mcp.jobs.state import TERMINAL_STATES, JobState
from earthdata_mcp.storage.local import LocalFilesystemBackend
from earthdata_mcp.workspace import (
    ProvenanceStore,
    WorkspaceStore,
    create_schema,
)
from earthdata_mcp.workspace.models import Handle


def pytest_configure(config: pytest.Config) -> None:
    """Register the ``live`` marker here too.

    It is declared canonically in ``pyproject.toml``, but that file is baked into
    the dev image (only ``src``/``tests`` are bind-mounted), so registering it in
    this mounted conftest keeps ``-m live`` warning-free without an image rebuild.
    """
    config.addinivalue_line(
        "markers",
        "live: needs real Earthdata Login credentials / network (run on demand, nightly CI)",
    )


@pytest.fixture
def local_settings(tmp_path: Path) -> Settings:
    """Settings pointing the local storage backend at an isolated tmp dir."""
    return Settings(
        _env_file=None,
        earthdata_storage="local",
        earthdata_data_dir=str(tmp_path / "data"),
    )


@pytest.fixture
def local_backend(tmp_path: Path) -> LocalFilesystemBackend:
    """A local filesystem backend rooted in a tmp dir."""
    return LocalFilesystemBackend(tmp_path / "store")


# --- Postgres-backed fixtures (workspace + provenance) -------------------
# These connect lazily — only tests that request them touch the DB. DATABASE_URL
# comes from the environment (set by docker-compose for the mcp container).


@pytest.fixture
async def pg_engine():
    """A real async engine with the workspace/provenance + jobs schemas created.

    The two declarative bases are independent and create disjoint tables; both
    ``create_*`` calls are idempotent.
    """
    settings = Settings()
    engine = create_async_engine(settings.database_url)
    await create_schema(engine)
    await create_jobs_schema(engine)
    try:
        yield engine
    finally:
        await engine.dispose()


#: Every workspace the suite mints is ``ws-<uuid4 hex>`` (the ``workspace_id``
#: fixture below) or an explicit ``ws-`` literal. A real deployment's workspaces
#: are ``default`` or ``user-{id}`` — and this database really does hold those —
#: so this pattern selects suite rows and nothing else. Nothing here may widen.
_TEST_WORKSPACE_PATTERN = "ws-%"


async def _retire_test_debris(factory) -> None:
    """Drive the suite's own leftover rows to a terminal/expired state.

    The unit suite runs against a **shared, persistent** database with no
    per-test rollback, and two of the system's readings are deliberately global:
    the PRD 024 cache fair share (every ``ready`` handle, whoever owns it) and
    PRD 025's "is any user-requested job in flight" gate (every non-terminal job
    row). A test that seeds either and walks away therefore decides a *later*
    test's behavior — that is exactly how ``test_prefetch_gates`` came to pass
    alone and fail after a full run. Leftover non-terminal jobs are not only a
    test problem: the worker's startup reclaim picks each one up on every boot
    and re-fails it as stale, which is how the 106 stuck rows behind PRD 004 got
    there in the first place.

    Run on both sides of every DB-touching test, because a crashed or
    interrupted run skips teardown and the next run must still start clean.

    Rows are **retired, not deleted**: this database is shared with real rows,
    and the discriminator above is the only thing standing between the suite and
    them. Terminal jobs and expired handles are inert — every other query is
    scoped by handle or by workspace — so retiring is enough. Two selectors:

    * jobs whose durable spec has a ``ws-`` ``workspace_id``, **or** none at all
      (every spec ``RequestSpec.from_plan`` builds carries one, so a row without
      one can only have been seeded directly by a test);
    * ``ready`` handles owned by a ``ws-`` workspace.

    A retired job also gives up its stored credential, since PRD 022's invariant
    is that no terminal job holds one at rest.
    """
    async with factory() as session:
        await session.execute(
            update(Job)
            .where(
                Job.state.not_in([state.value for state in TERMINAL_STATES]),
                or_(
                    Job.request_spec["workspace_id"].astext.like(
                        _TEST_WORKSPACE_PATTERN
                    ),
                    Job.request_spec["workspace_id"].astext.is_(None),
                ),
            )
            .values(
                state=JobState.CANCELLED.value,
                error="retired by the test suite's shared-database cleanup",
                credential_encrypted=None,
            )
        )
        await session.execute(
            update(Handle)
            .where(
                Handle.workspace_id.like(_TEST_WORKSPACE_PATTERN),
                Handle.payload["status"].astext == "ready",
            )
            .values(payload=Handle.payload.concat({"status": "expired"}))
        )
        await session.commit()


@pytest.fixture
async def session_factory(pg_engine):
    """The suite's session factory, with the shared database cleaned around each
    test — see :func:`_retire_test_debris` for why that is not optional."""
    factory = create_session_factory(pg_engine)
    await _retire_test_debris(factory)
    try:
        yield factory
    finally:
        await _retire_test_debris(factory)


@pytest.fixture
def workspace_store(session_factory) -> WorkspaceStore:
    return WorkspaceStore(session_factory)


@pytest.fixture
def provenance_store(session_factory) -> ProvenanceStore:
    return ProvenanceStore(session_factory)


@pytest.fixture
def workspace_id() -> str:
    """A unique workspace id, isolating each test's rows."""
    return f"ws-{uuid4().hex}"
