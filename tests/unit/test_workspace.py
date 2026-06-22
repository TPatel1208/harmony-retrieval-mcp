"""Handles + workspace store: round-trip, typing, and cross-workspace denial.

Runs against the real Postgres in the docker stack (the gate). Each test uses a
unique ``workspace_id`` so rows never collide across tests or runs.
"""

from __future__ import annotations

import pytest

from earthdata_mcp.workspace import (
    CrossWorkspaceError,
    HandleNotFoundError,
    HandleType,
    WorkspaceStore,
    handle_type_of,
    mint_handle,
)


# --- handle types (no DB) ------------------------------------------------


def test_mint_handle_is_prefixed_and_opaque() -> None:
    handle = mint_handle(HandleType.JOB)
    assert handle.startswith("job_")
    assert handle != mint_handle(HandleType.JOB)  # opaque + unique


@pytest.mark.parametrize("ht", list(HandleType))
def test_every_handle_type_round_trips_its_prefix(ht: HandleType) -> None:
    assert handle_type_of(mint_handle(ht)) is ht


def test_job_handle_type_exists() -> None:
    assert HandleType.JOB.value == "job"
    assert mint_handle(HandleType.JOB).startswith("job_")


def test_handle_type_of_rejects_unprefixed() -> None:
    with pytest.raises(ValueError):
        handle_type_of("no-prefix-here")


# --- store round-trip ----------------------------------------------------


async def test_handle_round_trip(
    workspace_store: WorkspaceStore, workspace_id: str
) -> None:
    handle = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"short_name": "MOD13Q1"}
    )
    record = await workspace_store.get_handle(workspace_id, handle)

    assert record.handle == handle
    assert record.workspace_id == workspace_id
    assert record.handle_type is HandleType.DATASET
    assert record.payload == {"short_name": "MOD13Q1"}


async def test_get_missing_handle_raises(
    workspace_store: WorkspaceStore, workspace_id: str
) -> None:
    with pytest.raises(HandleNotFoundError):
        await workspace_store.get_handle(workspace_id, "dataset_doesnotexist")


async def test_list_handles_filters_by_type(
    workspace_store: WorkspaceStore, workspace_id: str
) -> None:
    await workspace_store.put_handle(workspace_id, HandleType.DATASET, {})
    await workspace_store.put_handle(workspace_id, HandleType.AOI, {})
    job = await workspace_store.put_handle(workspace_id, HandleType.JOB, {})

    jobs = await workspace_store.list_handles(workspace_id, HandleType.JOB)
    assert [r.handle for r in jobs] == [job]
    assert len(await workspace_store.list_handles(workspace_id)) == 3


# --- update_handle -------------------------------------------------------


async def test_update_handle_merges_and_reads_back(
    workspace_store: WorkspaceStore, workspace_id: str
) -> None:
    """update_handle must merge the payload and return the populated record.

    Regression for the MissingGreenlet bug: ``updated_at`` (``onupdate=func.now()``)
    is expired by the UPDATE flush, so building the detached record used to fire a
    sync lazy-load in the async context. The returned record — and a fresh read —
    must both reflect the merge without raising.
    """
    handle = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )

    returned = await workspace_store.update_handle(
        workspace_id, handle, {"status": "ready", "storage_key": "results/x.zarr"}
    )

    # The record returned straight from update_handle is fully populated.
    assert returned.payload == {"status": "ready", "storage_key": "results/x.zarr"}
    assert returned.updated_at is not None

    # And a fresh read sees the merged payload (original key kept, new keys added).
    reread = await workspace_store.get_handle(workspace_id, handle)
    assert reread.payload == {"status": "ready", "storage_key": "results/x.zarr"}


async def test_update_handle_cross_workspace_denied(
    workspace_store: WorkspaceStore,
) -> None:
    handle = await workspace_store.put_handle("ws-owner", HandleType.OBS, {})
    with pytest.raises(CrossWorkspaceError):
        await workspace_store.update_handle("ws-intruder", handle, {"x": 1})


# --- ownership / isolation ----------------------------------------------


async def test_cross_workspace_read_is_denied(
    workspace_store: WorkspaceStore,
) -> None:
    owner = "ws-owner"
    intruder = "ws-intruder"
    handle = await workspace_store.put_handle(
        owner, HandleType.OBS, {"secret": True}
    )

    # The owner reads it fine.
    assert (await workspace_store.get_handle(owner, handle)).handle == handle

    # A different workspace is denied — not silently handed the row.
    with pytest.raises(CrossWorkspaceError):
        await workspace_store.get_handle(intruder, handle)


async def test_list_is_workspace_scoped(
    workspace_store: WorkspaceStore,
) -> None:
    await workspace_store.put_handle("ws-a", HandleType.CUBE, {})
    await workspace_store.put_handle("ws-b", HandleType.CUBE, {})

    assert len(await workspace_store.list_handles("ws-a")) >= 1
    a_handles = await workspace_store.list_handles("ws-a")
    assert all(r.workspace_id == "ws-a" for r in a_handles)
