"""Object-store ``StorageBackend`` (S3 / S3-compatible) — present but off by default.

Selected only when ``EARTHDATA_STORAGE`` is an ``s3://bucket/prefix`` URL. The
implementation is intentionally a stub in Phase 1; it is fleshed out (and the
parametrized round-trip test enabled) when a deployment opts into object storage.
Requires the ``s3`` extra (``pip install earthdata-mcp[s3]``).
"""

from __future__ import annotations

from urllib.parse import urlparse

from earthdata_mcp.storage.backend import StatResult, StorageBackend


class ObjectStoreBackend(StorageBackend):
    """S3 / S3-compatible backend. Stub until a deployment opts in."""

    def __init__(self, url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme != "s3":
            raise ValueError(f"not an s3 url: {url!r}")
        self.bucket = parsed.netloc
        self.prefix = parsed.path.lstrip("/")

    def _key(self, key: str) -> str:
        return f"{self.prefix.rstrip('/')}/{key}" if self.prefix else key

    async def put(self, key: str, data: bytes) -> None:
        raise NotImplementedError("ObjectStoreBackend is enabled in a later phase")

    async def get(self, key: str) -> bytes:
        raise NotImplementedError("ObjectStoreBackend is enabled in a later phase")

    async def delete(self, key: str) -> None:
        raise NotImplementedError("ObjectStoreBackend is enabled in a later phase")

    async def list(self, prefix: str = "") -> list[str]:
        raise NotImplementedError("ObjectStoreBackend is enabled in a later phase")

    async def stat(self, key: str) -> StatResult:
        raise NotImplementedError("ObjectStoreBackend is enabled in a later phase")
