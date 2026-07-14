"""Config + storage-backend selection (config-only seam, PLAN.md §4.4)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from earthdata_mcp.config import Settings
from earthdata_mcp.storage import get_storage_backend
from earthdata_mcp.storage.local import LocalFilesystemBackend


def test_defaults_are_local() -> None:
    settings = Settings(_env_file=None)
    assert settings.earthdata_storage == "local"
    assert settings.storage_kind == "local"


def test_s3_url_is_detected() -> None:
    settings = Settings(_env_file=None, earthdata_storage="s3://bucket/prefix")
    assert settings.storage_kind == "s3"


def test_get_storage_backend_defaults_to_local(tmp_path) -> None:
    settings = Settings(
        _env_file=None,
        earthdata_storage="local",
        earthdata_data_dir=str(tmp_path),
    )
    backend = get_storage_backend(settings)
    assert isinstance(backend, LocalFilesystemBackend)


def test_env_overrides_storage(monkeypatch) -> None:
    monkeypatch.setenv("EARTHDATA_STORAGE", "s3://my-bucket/data")
    settings = Settings(_env_file=None)
    assert settings.storage_kind == "s3"


def test_mcp_transport_defaults_to_stdio(monkeypatch) -> None:
    # Hermetic against the container's own ambient transport config: the mcp
    # service runs with EARTHDATA_MCP_TRANSPORT=http since the PRD 018 compose
    # topology, and this test asserts the *code* default, not the deployment's.
    for name in (
        "EARTHDATA_MCP_TRANSPORT",
        "EARTHDATA_MCP_HOST",
        "EARTHDATA_MCP_PORT",
        "EARTHDATA_MCP_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = Settings(_env_file=None)
    assert settings.mcp_transport == "stdio"
    assert settings.mcp_host == "0.0.0.0"
    assert settings.mcp_port == 8765
    assert settings.mcp_token == ""


def test_mcp_transport_env_overrides(monkeypatch) -> None:
    monkeypatch.setenv("EARTHDATA_MCP_TRANSPORT", "http")
    monkeypatch.setenv("EARTHDATA_MCP_HOST", "127.0.0.1")
    monkeypatch.setenv("EARTHDATA_MCP_PORT", "9000")
    monkeypatch.setenv("EARTHDATA_MCP_TOKEN", "s3cr3t")
    settings = Settings(_env_file=None)
    assert settings.mcp_transport == "http"
    assert settings.mcp_host == "127.0.0.1"
    assert settings.mcp_port == 9000
    assert settings.mcp_token == "s3cr3t"


def test_invalid_mcp_transport_rejected(monkeypatch) -> None:
    monkeypatch.setenv("EARTHDATA_MCP_TRANSPORT", "carrier-pigeon")
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
