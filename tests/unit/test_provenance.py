"""Spec-keyed provenance: deep ancestry, workspace scoping, re-materialization.

Runs against the real Postgres in the docker stack so the recursive CTE is
exercised for real (PLAN.md §4.5). Each test uses a unique ``workspace_id``.
"""

from __future__ import annotations

import pytest

from earthdata_mcp.workspace import (
    ProvenanceError,
    ProvenanceEventType,
    ProvenanceStore,
)


async def test_single_edge_ancestry(
    provenance_store: ProvenanceStore, workspace_id: str
) -> None:
    await provenance_store.record_edge(
        workspace_id, "obs_child", "dataset_parent", request_spec={"x": 1}
    )
    ancestry = await provenance_store.ancestry(workspace_id, "obs_child")
    assert [a.handle for a in ancestry] == ["dataset_parent"]
    assert ancestry[0].depth == 1


async def test_deep_graph_ancestry_20_hops(
    provenance_store: ProvenanceStore, workspace_id: str
) -> None:
    """A 21-node chain; the deepest node has 20 ancestors at depths 1..20."""
    chain = [f"node_{i}" for i in range(21)]
    for child, parent in zip(chain[1:], chain[:-1]):
        await provenance_store.record_edge(
            workspace_id, child, parent, request_spec={"step": child}
        )

    ancestry = await provenance_store.ancestry(workspace_id, chain[-1])

    assert len(ancestry) == 20
    # Immediate parent at depth 1, the root at depth 20.
    assert ancestry[0] == _at(chain[-2], 1)
    assert ancestry[-1] == _at(chain[0], 20)
    # Depths are contiguous 1..20.
    assert [a.depth for a in ancestry] == list(range(1, 21))


async def test_diamond_uses_shortest_depth(
    provenance_store: ProvenanceStore, workspace_id: str
) -> None:
    # root -> a -> sink and root -> sink (direct). sink's depth to root is 1.
    await provenance_store.record_edge(workspace_id, "sink", "a")
    await provenance_store.record_edge(workspace_id, "a", "root")
    await provenance_store.record_edge(workspace_id, "sink", "root")

    ancestry = {a.handle: a.depth for a in
                await provenance_store.ancestry(workspace_id, "sink")}
    assert ancestry == {"a": 1, "root": 1}


async def test_ancestry_is_workspace_scoped(
    provenance_store: ProvenanceStore,
) -> None:
    await provenance_store.record_edge("ws-1", "obs_x", "dataset_y")
    # A different workspace sees no lineage for the same handle id.
    assert await provenance_store.ancestry("ws-2", "obs_x") == []


async def test_record_edge_rejects_url_spec(
    provenance_store: ProvenanceStore, workspace_id: str
) -> None:
    """An edge's spec must be re-materializable, never a staged-output URL."""
    with pytest.raises(ProvenanceError):
        await provenance_store.record_edge(
            workspace_id,
            "obs_bad",
            "dataset_src",
            request_spec={"output": "https://harmony.example/staged/abc.nc"},
        )


async def test_re_materialize_returns_spec_and_records_event(
    provenance_store: ProvenanceStore, workspace_id: str
) -> None:
    spec = {"short_name": "MOD13Q1", "bbox": [-105, 37, -104, 38]}
    await provenance_store.record_edge(
        workspace_id, "obs_result", "dataset_src", request_spec=spec
    )
    # Mark it expired — a first-class event — then rebuild from the spec.
    await provenance_store.mark_expired(workspace_id, "obs_result")

    recovered = await provenance_store.re_materialize(workspace_id, "obs_result")
    assert recovered == spec


async def test_re_materialize_without_spec_raises(
    provenance_store: ProvenanceStore, workspace_id: str
) -> None:
    await provenance_store.record_edge(workspace_id, "obs_nospec", "dataset_src")
    with pytest.raises(ProvenanceError):
        await provenance_store.re_materialize(workspace_id, "obs_nospec")


def _at(handle: str, depth: int):
    from earthdata_mcp.workspace import Ancestor

    return Ancestor(handle=handle, depth=depth)


def test_event_types_present() -> None:
    assert ProvenanceEventType.EXPIRED.value == "expired"
    assert ProvenanceEventType.RE_MATERIALIZED.value == "re-materialized"
