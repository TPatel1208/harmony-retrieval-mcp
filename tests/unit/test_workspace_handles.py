"""Typed handle resolvers — the one place each handle-resolution sequence lives.

``resolve_dataset``/``resolve_aoi``/``resolve_materialized`` each do the
type-check -> isolation-gated lookup -> payload-extraction sequence exactly
once, so the six tool modules that used to hand-roll this can become thin
callers. These tests assert each resolver's own contract directly against the
real Postgres-backed ``WorkspaceStore`` fixture: correct type + workspace ->
typed value; wrong handle type -> ValueError; cross-workspace access ->
CrossWorkspaceError; missing/malformed payload field -> ValueError.
"""

from __future__ import annotations

import pytest

from earthdata_mcp.workspace.handles import (
    resolve_aoi,
    resolve_dataset,
    resolve_materialized,
)
from earthdata_mcp.workspace.models import HandleType
from earthdata_mcp.workspace.store import CrossWorkspaceError

_CONCEPT_ID = "C1234567890-LPCLOUD"
_BBOX = [-105.0, 37.0, -104.0, 38.0]


# -- resolve_dataset ---------------------------------------------------------


async def test_resolve_dataset_returns_concept_id(workspace_store, workspace_id) -> None:
    handle = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": _CONCEPT_ID}
    )
    concept_id = await resolve_dataset(workspace_store, workspace_id, handle)
    assert concept_id == _CONCEPT_ID


async def test_resolve_dataset_wrong_type_raises(workspace_store, workspace_id) -> None:
    aoi = await workspace_store.put_handle(workspace_id, HandleType.AOI, {"bbox": _BBOX})
    with pytest.raises(ValueError, match="dataset_"):
        await resolve_dataset(workspace_store, workspace_id, aoi)


async def test_resolve_dataset_cross_workspace_denied(workspace_store, workspace_id) -> None:
    handle = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": _CONCEPT_ID}
    )
    with pytest.raises(CrossWorkspaceError):
        await resolve_dataset(workspace_store, "ws-intruder", handle)


async def test_resolve_dataset_missing_concept_id_raises(workspace_store, workspace_id) -> None:
    handle = await workspace_store.put_handle(workspace_id, HandleType.DATASET, {})
    with pytest.raises(ValueError, match="concept_id"):
        await resolve_dataset(workspace_store, workspace_id, handle)


# -- resolve_aoi --------------------------------------------------------------


async def test_resolve_aoi_returns_bbox_tuple(workspace_store, workspace_id) -> None:
    handle = await workspace_store.put_handle(workspace_id, HandleType.AOI, {"bbox": _BBOX})
    bbox = await resolve_aoi(workspace_store, workspace_id, handle)
    assert bbox == tuple(_BBOX)
    assert all(isinstance(c, float) for c in bbox)


async def test_resolve_aoi_wrong_type_raises(workspace_store, workspace_id) -> None:
    ds = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": _CONCEPT_ID}
    )
    with pytest.raises(ValueError, match="aoi_"):
        await resolve_aoi(workspace_store, workspace_id, ds)


async def test_resolve_aoi_cross_workspace_denied(workspace_store, workspace_id) -> None:
    handle = await workspace_store.put_handle(workspace_id, HandleType.AOI, {"bbox": _BBOX})
    with pytest.raises(CrossWorkspaceError):
        await resolve_aoi(workspace_store, "ws-intruder", handle)


async def test_resolve_aoi_malformed_bbox_raises(workspace_store, workspace_id) -> None:
    handle = await workspace_store.put_handle(
        workspace_id, HandleType.AOI, {"bbox": [-105.0, 37.0]}
    )
    with pytest.raises(ValueError, match="bbox"):
        await resolve_aoi(workspace_store, workspace_id, handle)


# -- resolve_materialized ------------------------------------------------------


async def test_resolve_materialized_returns_storage_key_media_type_payload(
    workspace_store, workspace_id
) -> None:
    payload = {
        "status": "ready",
        "storage_key": "results/x.zarr",
        "media_type": "application/vnd+zarr",
    }
    handle = await workspace_store.put_handle(workspace_id, HandleType.OBS, payload)
    storage_key, media_type, resolved_payload = await resolve_materialized(
        workspace_store, workspace_id, handle
    )
    assert storage_key == "results/x.zarr"
    assert media_type == "application/vnd+zarr"
    assert resolved_payload == payload


async def test_resolve_materialized_accepts_cube_handle(workspace_store, workspace_id) -> None:
    payload = {
        "status": "ready",
        "storage_key": "transform/x/y.zarr",
        "media_type": "application/vnd+zarr",
    }
    handle = await workspace_store.put_handle(workspace_id, HandleType.CUBE, payload)
    storage_key, media_type, _ = await resolve_materialized(workspace_store, workspace_id, handle)
    assert storage_key == "transform/x/y.zarr"


async def test_resolve_materialized_wrong_type_raises(workspace_store, workspace_id) -> None:
    ds = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": _CONCEPT_ID}
    )
    with pytest.raises(ValueError, match="obs_ or cube_"):
        await resolve_materialized(workspace_store, workspace_id, ds)


async def test_resolve_materialized_cross_workspace_denied(workspace_store, workspace_id) -> None:
    handle = await workspace_store.put_handle(
        workspace_id,
        HandleType.OBS,
        {"status": "ready", "storage_key": "results/x.zarr", "media_type": "application/x-netcdf"},
    )
    with pytest.raises(CrossWorkspaceError):
        await resolve_materialized(workspace_store, "ws-intruder", handle)


async def test_resolve_materialized_not_ready_raises(workspace_store, workspace_id) -> None:
    handle = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending"}
    )
    with pytest.raises(ValueError, match="not a materialized result"):
        await resolve_materialized(workspace_store, workspace_id, handle)
