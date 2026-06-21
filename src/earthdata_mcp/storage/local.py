"""Local filesystem ``StorageBackend`` — the default for single-node research.

No cloud account or credentials needed to develop or test. Materializes objects
as files under a configured data root.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from earthdata_mcp.storage.backend import StatResult, StorageBackend


class LocalFilesystemBackend(StorageBackend):
    """Stores objects as files under ``root``."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        """Resolve ``key`` to a path under ``root``, rejecting traversal."""
        path = (self.root / key).resolve()
        if path != self.root and self.root not in path.parents:
            raise ValueError(f"key escapes storage root: {key!r}")
        return path

    async def put(self, key: str, data: bytes) -> None:
        def _write() -> None:
            path = self._path(key)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

        await asyncio.to_thread(_write)

    async def get(self, key: str) -> bytes:
        def _read() -> bytes:
            path = self._path(key)
            if not path.is_file():
                raise KeyError(key)
            return path.read_bytes()

        return await asyncio.to_thread(_read)

    async def delete(self, key: str) -> None:
        def _delete() -> None:
            self._path(key).unlink(missing_ok=True)

        await asyncio.to_thread(_delete)

    async def list(self, prefix: str = "") -> list[str]:
        def _list() -> list[str]:
            keys: list[str] = []
            for path in self.root.rglob("*"):
                if not path.is_file():
                    continue
                key = path.relative_to(self.root).as_posix()
                if key.startswith(prefix):
                    keys.append(key)
            return sorted(keys)

        return await asyncio.to_thread(_list)

    async def stat(self, key: str) -> StatResult:
        def _stat() -> StatResult:
            path = self._path(key)
            if not path.is_file():
                raise KeyError(key)
            return StatResult(key=key, size=path.stat().st_size)

        return await asyncio.to_thread(_stat)
