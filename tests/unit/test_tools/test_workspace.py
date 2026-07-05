"""``list_workspace`` — re-enumerate a workspace's handles (PRD 019 story 4).

A thin read over ``WorkspaceStore.list_handles``: no new state, workspace-scoped,
same isolation contract as every other handle-resolving tool.
"""

from __future__ import annotations

import pytest

from earthdata_mcp.tools.workspace import list_workspace
from earthdata_mcp.workspace.models import HandleType


async def test_list_workspace_returns_handle_with_type_and_created_at(
    workspace_store, workspace_id,
) -> None:
    handle = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": "C1-X"}
    )

    out = await list_workspace(workspace_id, store=workspace_store)

    assert len(out["handles"]) == 1
    entry = out["handles"][0]
    assert entry["handle"] == handle
    assert entry["type"] == "dataset"
    assert entry["created_at"] is not None


async def test_list_workspace_summary_is_type_specific(
    workspace_store, workspace_id,
) -> None:
    dataset = await workspace_store.put_handle(
        workspace_id,
        HandleType.DATASET,
        {"concept_id": "C1-X", "collection": {"short_name": "MOD13Q1", "version": "061"}},
    )
    aoi = await workspace_store.put_handle(
        workspace_id, HandleType.AOI, {"source": "bbox", "bbox": [-105.0, 37.0, -104.0, 38.0]}
    )

    out = await list_workspace(workspace_id, store=workspace_store)

    by_handle = {e["handle"]: e for e in out["handles"]}
    assert by_handle[dataset]["summary"] == {
        "concept_id": "C1-X", "short_name": "MOD13Q1", "version": "061",
    }
    assert by_handle[aoi]["summary"] == {
        "source": "bbox", "bbox": [-105.0, 37.0, -104.0, 38.0],
    }


async def test_list_workspace_filters_by_handle_type(
    workspace_store, workspace_id,
) -> None:
    await workspace_store.put_handle(workspace_id, HandleType.DATASET, {"concept_id": "C1-X"})
    aoi = await workspace_store.put_handle(
        workspace_id, HandleType.AOI, {"source": "bbox", "bbox": [-1, -1, 1, 1]}
    )

    out = await list_workspace(workspace_id, handle_type="aoi", store=workspace_store)

    assert [e["handle"] for e in out["handles"]] == [aoi]


async def test_list_workspace_is_scoped_to_the_workspace(
    workspace_store, workspace_id,
) -> None:
    await workspace_store.put_handle(workspace_id, HandleType.DATASET, {"concept_id": "C1-X"})

    out = await list_workspace("ws-someone-else", store=workspace_store)

    assert out["handles"] == []
