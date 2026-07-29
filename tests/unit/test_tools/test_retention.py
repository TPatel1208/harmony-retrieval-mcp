"""``pin_result`` — exempting a result from cap-driven eviction (PRD 024).

Real Postgres-backed workspace fixtures plus the local filesystem backend, per
the prior art in ``test_tools/test_export.py``. Tests observe the tool's answer
and what the handle reads back as through the store — never how the pin is
stored — so the durable flag can move without rewriting them.
"""

from __future__ import annotations

import pytest

from earthdata_mcp.tools.retention import pin_result
from earthdata_mcp.workspace.models import HandleType


async def _seed_obs(workspace_store, local_backend, workspace_id, name="g.nc", *, size=40):
    key = f"harmony/{workspace_id}/{name}"
    await local_backend.put(key, b"x" * size)
    handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.OBS,
        {
            "status": "ready",
            "storage_key": key,
            "media_type": "application/x-netcdf",
            "size_bytes": size,
        },
    )
    return handle, key


async def test_pinning_a_ready_result_marks_it_pinned(
    workspace_store, local_backend, workspace_id
) -> None:
    handle, _ = await _seed_obs(workspace_store, local_backend, workspace_id)

    out = await pin_result(
        handle, True, workspace_id, store=workspace_store, storage=local_backend
    )

    assert out["handle"] == handle
    assert out["pinned"] is True
    record = await workspace_store.get_handle(workspace_id, handle)
    assert record.payload["pinned"] is True


async def test_unpinning_clears_the_pin(
    workspace_store, local_backend, workspace_id
) -> None:
    handle, _ = await _seed_obs(workspace_store, local_backend, workspace_id)
    await pin_result(handle, True, workspace_id, store=workspace_store, storage=local_backend)

    out = await pin_result(
        handle, False, workspace_id, store=workspace_store, storage=local_backend
    )

    assert out["pinned"] is False
    record = await workspace_store.get_handle(workspace_id, handle)
    assert record.payload["pinned"] is False


async def test_pinning_past_the_workspace_ceiling_is_refused_not_ignored(
    workspace_store, local_backend, workspace_id
) -> None:
    """A pin that would push the workspace past its pinned-bytes ceiling is
    refused with a message that says so — pinning must not be a way to
    monopolise the cache, and a silently-ignored pin is worse than a refusal
    because the researcher would believe their data was protected."""
    first, _ = await _seed_obs(workspace_store, local_backend, workspace_id, "a.nc", size=40)
    second, _ = await _seed_obs(workspace_store, local_backend, workspace_id, "b.nc", size=40)

    await pin_result(
        first, True, workspace_id, store=workspace_store, storage=local_backend, max_pinned_bytes=50
    )
    out = await pin_result(
        second, True, workspace_id, store=workspace_store, storage=local_backend, max_pinned_bytes=50
    )

    assert out["status"] == "refused"
    assert out["pinned"] is False
    assert "50" in out["message"]
    record = await workspace_store.get_handle(workspace_id, second)
    assert not record.payload.get("pinned")


async def test_repinning_an_already_pinned_result_is_idempotent(
    workspace_store, local_backend, workspace_id
) -> None:
    """Pinning what is already pinned must not double-count against the ceiling."""
    handle, _ = await _seed_obs(workspace_store, local_backend, workspace_id, size=40)

    await pin_result(
        handle, True, workspace_id, store=workspace_store, storage=local_backend, max_pinned_bytes=50
    )
    out = await pin_result(
        handle, True, workspace_id, store=workspace_store, storage=local_backend, max_pinned_bytes=50
    )

    assert out["pinned"] is True
    assert out["status"] != "refused"


async def test_unknown_and_cross_workspace_handles_answer_identically(
    workspace_store, local_backend, workspace_id
) -> None:
    """Same isolation contract as ``get_handle``/``export_result`` (PRD 014):
    a handle in someone else's workspace must be indistinguishable from one
    that does not exist, or existence leaks."""
    other_handle, _ = await _seed_obs(workspace_store, local_backend, "ws-somebody-else")

    unknown = await pin_result(
        "obs_deadbeefdeadbeef", True, workspace_id, store=workspace_store, storage=local_backend
    )
    cross = await pin_result(
        other_handle, True, workspace_id, store=workspace_store, storage=local_backend
    )

    assert unknown["status"] == "not_found"
    assert cross["status"] == "not_found"
    assert unknown["message"].replace("obs_deadbeefdeadbeef", "H") == cross[
        "message"
    ].replace(other_handle, "H")


async def test_pinning_an_evicted_result_is_refused(
    workspace_store, local_backend, workspace_id
) -> None:
    """An expired handle holds no bytes — there is nothing for a pin to protect,
    and answering "pinned" would promise a retention this cannot deliver."""
    handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.OBS,
        {"status": "expired", "storage_key": None, "media_type": "application/x-netcdf"},
    )

    out = await pin_result(
        handle, True, workspace_id, store=workspace_store, storage=local_backend
    )

    assert out["status"] == "expired"
    assert out["pinned"] is False
    assert "rematerialize" in out["message"]


async def test_pin_result_rejects_a_non_result_handle(
    workspace_store, local_backend, workspace_id
) -> None:
    with pytest.raises(ValueError):
        await pin_result(
            "job_1234567890abcdef",
            True,
            workspace_id,
            store=workspace_store,
            storage=local_backend,
        )
