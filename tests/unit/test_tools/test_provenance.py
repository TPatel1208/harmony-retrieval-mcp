"""get_provenance + cite_dataset (PLAN.md §6 Phase 8, §4.5).

get_provenance runs against the real Postgres-backed provenance/workspace
fixtures so the recursive-CTE ancestry and event reads are exercised for real and
workspace isolation is enforced. cite_dataset fakes CMR at the object level
(``get_citations`` as an ``AsyncMock``) — its contract is "resolve the dataset
handle, ask CMR, pass CMR's records straight through," with no hand-rolled strings.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools.provenance import cite_dataset, get_provenance
from earthdata_mcp.workspace.models import HandleType, ProvenanceEventType
from earthdata_mcp.workspace.store import CrossWorkspaceError

_CONCEPT_ID = "C2565788901-LPCLOUD"


# -- get_provenance --------------------------------------------------------


async def test_get_provenance_returns_ancestry_and_events(
    workspace_store, provenance_store, workspace_id,
) -> None:
    dataset = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": _CONCEPT_ID}
    )
    obs = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "ready"}
    )
    await provenance_store.record_edge(
        workspace_id, target_handle=obs, source_handle=dataset,
        request_spec={"concept_id": _CONCEPT_ID},
    )
    await provenance_store.record_event(
        workspace_id, obs, ProvenanceEventType.MATERIALIZED, {"size_bytes": 10}
    )

    out = await get_provenance(
        obs, workspace_id=workspace_id,
        store=workspace_store, provenance=provenance_store,
    )

    assert out["handle"] == obs
    assert out["ancestors"] == [{"handle": dataset, "depth": 1}]
    assert [e["event_type"] for e in out["events"]] == ["materialized"]
    assert out["events"][0]["detail"] == {"size_bytes": 10}
    assert out["events"][0]["created_at"] is not None


async def test_get_provenance_cross_workspace_denied(
    workspace_store, provenance_store, workspace_id,
) -> None:
    obs = await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "ready"}
    )
    with pytest.raises(CrossWorkspaceError):
        await get_provenance(
            obs, workspace_id="ws-intruder",
            store=workspace_store, provenance=provenance_store,
        )


# -- cite_dataset ----------------------------------------------------------


def _cmr_with_citations(payload: dict) -> CMRProvider:
    cmr = CMRProvider.__new__(CMRProvider)
    cmr.get_citations = AsyncMock(return_value=payload)
    return cmr


async def test_cite_dataset_passes_through_cmr_records(
    workspace_store, workspace_id,
) -> None:
    dataset = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": _CONCEPT_ID}
    )
    cmr = _cmr_with_citations(
        {
            "concept_id": _CONCEPT_ID,
            "doi": "10.5067/MODIS/MOD13A1.061",
            "doi_authority": "https://doi.org",
            "collection_citations": [{"Title": "MODIS/Terra ...", "Creator": "K. Didan"}],
            "reference_citation_count": 412,
        }
    )

    out = await cite_dataset(
        dataset, workspace_id=workspace_id, store=workspace_store, cmr=cmr,
    )

    assert out["handle"] == dataset
    assert out["concept_id"] == _CONCEPT_ID
    assert out["doi"] == "10.5067/MODIS/MOD13A1.061"
    assert out["collection_citations"][0]["Creator"] == "K. Didan"
    assert out["reference_citation_count"] == 412
    cmr.get_citations.assert_awaited_once_with(_CONCEPT_ID)


async def test_cite_dataset_graceful_when_no_citations(
    workspace_store, workspace_id,
) -> None:
    """A collection with no DOI / citations yields empty fields, not an error."""
    dataset = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": _CONCEPT_ID}
    )
    cmr = _cmr_with_citations(
        {
            "concept_id": _CONCEPT_ID,
            "doi": None,
            "doi_authority": None,
            "collection_citations": [],
            "reference_citation_count": 0,
        }
    )

    out = await cite_dataset(
        dataset, workspace_id=workspace_id, store=workspace_store, cmr=cmr,
    )
    assert out["doi"] is None
    assert out["collection_citations"] == []
    assert out["reference_citation_count"] == 0


async def test_cite_dataset_wrong_handle_type_raises(
    workspace_store, workspace_id,
) -> None:
    aoi = await workspace_store.put_handle(
        workspace_id, HandleType.AOI, {"bbox": [-105.0, 37.0, -104.0, 38.0]}
    )
    with pytest.raises(ValueError, match="dataset_"):
        await cite_dataset(aoi, workspace_id=workspace_id, store=workspace_store)


async def test_cite_dataset_cross_workspace_denied(
    workspace_store, workspace_id,
) -> None:
    dataset = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": _CONCEPT_ID}
    )
    cmr = _cmr_with_citations({"concept_id": _CONCEPT_ID})
    with pytest.raises(CrossWorkspaceError):
        await cite_dataset(
            dataset, workspace_id="ws-intruder", store=workspace_store, cmr=cmr,
        )
