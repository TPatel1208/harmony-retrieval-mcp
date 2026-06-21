"""Pluggable storage: ``StorageBackend`` + local (default) and object-store impls.

Selection is config-only; callers use ``get_storage_backend`` and never import a
concrete backend.
"""

from __future__ import annotations

from earthdata_mcp.config import Settings, get_settings
from earthdata_mcp.storage.backend import StatResult, StorageBackend
from earthdata_mcp.storage.local import LocalFilesystemBackend

__all__ = [
    "StorageBackend",
    "StatResult",
    "LocalFilesystemBackend",
    "get_storage_backend",
]


def get_storage_backend(settings: Settings | None = None) -> StorageBackend:
    """Return the configured backend: local FS by default, object store for s3://."""
    settings = settings or get_settings()
    if settings.storage_kind == "s3":
        # Imported lazily so the default path needs no s3 extra installed.
        from earthdata_mcp.storage.s3 import ObjectStoreBackend

        return ObjectStoreBackend(settings.earthdata_storage)
    return LocalFilesystemBackend(settings.earthdata_data_dir)
