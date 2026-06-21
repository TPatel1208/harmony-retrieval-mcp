"""Shared test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from earthdata_mcp.config import Settings
from earthdata_mcp.storage.local import LocalFilesystemBackend


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
