"""The ``StorageBackend`` interface.

Everything above storage speaks to this interface and never imports a concrete
backend. Keys are opaque strings (use ``/`` as the separator). See PLAN.md §4.4.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StatResult:
    """Metadata for a stored object."""

    key: str
    size: int


class StorageBackend(ABC):
    """An opaque key/value object store: put / get / delete / list / stat."""

    @abstractmethod
    async def put(self, key: str, data: bytes) -> None:
        """Store ``data`` under ``key``, overwriting any existing object."""

    @abstractmethod
    async def get(self, key: str) -> bytes:
        """Return the bytes stored under ``key``; raise ``KeyError`` if absent."""

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete ``key``. Idempotent: deleting a missing key is a no-op."""

    @abstractmethod
    async def list(self, prefix: str = "") -> list[str]:
        """Return all keys beginning with ``prefix`` (sorted)."""

    @abstractmethod
    async def stat(self, key: str) -> StatResult:
        """Return metadata for ``key``; raise ``KeyError`` if absent."""

    def path(self, key: str) -> "Path | None":
        """Filesystem ``Path`` for ``key``, or ``None`` if not path-addressable (e.g. S3).

        The default returns ``None``; ``LocalFilesystemBackend`` overrides this so
        callers can open the file directly with xarray/dask without reading bytes first.
        """
        return None
