"""Config + storage-backend selection (config-only seam, PLAN.md §4.4)."""

from __future__ import annotations

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
