"""Shared test fixtures."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from earthdata_mcp.config import Settings
from earthdata_mcp.db import create_session_factory
from earthdata_mcp.storage.local import LocalFilesystemBackend
from earthdata_mcp.workspace import (
    ProvenanceStore,
    WorkspaceStore,
    create_schema,
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
    """A real async engine with the workspace/provenance schema created."""
    settings = Settings()
    engine = create_async_engine(settings.database_url)
    await create_schema(engine)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def session_factory(pg_engine):
    return create_session_factory(pg_engine)


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
