"""StorageBackend round-trip.

The local-filesystem case is the gate (no cloud account needed). A parametrized
S3 variant runs only when EARTHDATA_STORAGE points at MinIO/S3.
"""

from __future__ import annotations

import os

import pytest

from earthdata_mcp.storage.backend import StorageBackend
from earthdata_mcp.storage.local import LocalFilesystemBackend


def _make_local(tmp_path) -> LocalFilesystemBackend:
    return LocalFilesystemBackend(tmp_path / "store")


def _make_s3() -> StorageBackend:
    from earthdata_mcp.storage.s3 import ObjectStoreBackend

    return ObjectStoreBackend(os.environ["EARTHDATA_STORAGE"])


# The local backend is always exercised. The s3 backend is included only when
# EARTHDATA_STORAGE is configured to an object store (otherwise skipped).
_BACKENDS = ["local"]
if os.environ.get("EARTHDATA_STORAGE", "local").startswith("s3://"):
    _BACKENDS.append("s3")


@pytest.fixture(params=_BACKENDS)
def backend(request, tmp_path) -> StorageBackend:
    if request.param == "local":
        return _make_local(tmp_path)
    return _make_s3()


async def test_put_get_round_trip(backend: StorageBackend) -> None:
    await backend.put("a/b/obj.bin", b"hello world")
    assert await backend.get("a/b/obj.bin") == b"hello world"


async def test_stat_reports_size(backend: StorageBackend) -> None:
    await backend.put("sized.bin", b"12345")
    stat = await backend.stat("sized.bin")
    assert stat.key == "sized.bin"
    assert stat.size == 5


async def test_list_filters_by_prefix(backend: StorageBackend) -> None:
    await backend.put("p/one.bin", b"1")
    await backend.put("p/two.bin", b"2")
    await backend.put("other.bin", b"3")
    keys = await backend.list("p/")
    assert keys == ["p/one.bin", "p/two.bin"]


async def test_delete_is_idempotent(backend: StorageBackend) -> None:
    await backend.put("gone.bin", b"x")
    await backend.delete("gone.bin")
    with pytest.raises(KeyError):
        await backend.get("gone.bin")
    # Deleting a missing key must not raise.
    await backend.delete("gone.bin")


def test_local_rejects_path_traversal(tmp_path) -> None:
    backend = _make_local(tmp_path)
    with pytest.raises(ValueError):
        backend._path("../escape.bin")


def test_path_resolves_key_to_filesystem_path(tmp_path) -> None:
    b = _make_local(tmp_path)
    assert b.path("a/b.nc") == b.root / "a" / "b.nc"


def test_path_rejects_traversal(tmp_path) -> None:
    b = _make_local(tmp_path)
    with pytest.raises(ValueError):
        b.path("../escape.bin")


def test_base_backend_path_returns_none() -> None:
    class _Stub(StorageBackend):
        async def put(self, k, d): ...  # type: ignore[override]
        async def get(self, k): ...  # type: ignore[override]
        async def delete(self, k): ...  # type: ignore[override]
        async def list(self, prefix=""): ...  # type: ignore[override]
        async def stat(self, k): ...  # type: ignore[override]

    assert _Stub().path("any_key") is None
