"""``describe_dataset`` — handle resolution, variables, advisory flagging.

CMR HTTP is mocked (`httpx_mock`); the workspace store is the real Postgres
fixture. A ``dataset_`` handle is seeded directly via the store, mirroring what
``search_datasets`` persists (an enriched TEMPO_NO2_L3 collection).
"""

from __future__ import annotations

import pytest

from earthdata_mcp.catalog.enrichment import ADVISORY_FLAG, enrich_collection
from earthdata_mcp.config import Settings
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools.understanding import describe_dataset
from earthdata_mcp.workspace.models import HandleType
from earthdata_mcp.workspace.store import CrossWorkspaceError


@pytest.fixture
def provider() -> CMRProvider:
    return CMRProvider(Settings(_env_file=None))


async def _seed_dataset_handle(store, workspace_id: str) -> str:
    """Persist a dataset_ handle whose payload mirrors search_datasets' spec."""
    collection = enrich_collection(
        {
            "concept_id": "C1-X",
            "short_name": "TEMPO_NO2_L3",
            "version": "V03",
            "entry_title": "TEMPO NO2 V03",
            "processing_level": "3",
        }
    )
    payload = {"concept_id": "C1-X", "collection": collection, "search": {}}
    return await store.put_handle(workspace_id, HandleType.DATASET, payload=payload)


# get_variables is two-phase: collection fetch (associations) then variables fetch.
def _mock_get_variables(httpx_mock) -> None:
    httpx_mock.add_response(
        json={"items": [{"meta": {"associations": {"variables": ["V1-X"]}}}]}
    )
    httpx_mock.add_response(
        json={
            "items": [
                {
                    "meta": {"concept-id": "V1-X"},
                    "umm": {
                        "Name": "vertical_column_troposphere",
                        "LongName": "Tropospheric NO2 column",
                        "Units": "molecules/cm2",
                        "Scale": 1,
                    },
                }
            ]
        }
    )


async def test_describe_resolves_metadata_and_variables(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    handle = await _seed_dataset_handle(workspace_store, workspace_id)
    _mock_get_variables(httpx_mock)

    out = await describe_dataset(
        handle, workspace_id=workspace_id, cmr=provider, store=workspace_store
    )

    assert out["handle"] == handle
    assert out["concept_id"] == "C1-X"
    assert out["metadata"]["short_name"] == "TEMPO_NO2_L3"
    assert len(out["variables"]) == 1
    var = out["variables"][0]
    assert var["name"] == "vertical_column_troposphere"
    # QA fact is preserved from UMM-Var (authoritative).
    assert var["scale"] == 1


async def test_variable_advisory_notes_are_flagged(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    handle = await _seed_dataset_handle(workspace_store, workspace_id)
    _mock_get_variables(httpx_mock)

    out = await describe_dataset(
        handle, workspace_id=workspace_id, cmr=provider, store=workspace_store
    )

    var_notes = out["variables"][0]["advisory_notes"]
    assert var_notes  # vertical_column_troposphere is curated in variables.yaml
    for note in var_notes:
        assert note["advisory"] == ADVISORY_FLAG
        assert note["authoritative"] is False


async def test_collection_advisory_notes_are_flagged(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    handle = await _seed_dataset_handle(workspace_store, workspace_id)
    _mock_get_variables(httpx_mock)

    out = await describe_dataset(
        handle, workspace_id=workspace_id, cmr=provider, store=workspace_store
    )

    notes = out["advisory_notes"]
    assert notes
    for note in notes:
        assert note["advisory"] == ADVISORY_FLAG
        assert note["authoritative"] is False


async def test_rejects_non_dataset_handle(workspace_store, workspace_id) -> None:
    with pytest.raises(ValueError, match="dataset_ handle"):
        await describe_dataset(
            "aoi_deadbeefdeadbeef",
            workspace_id=workspace_id,
            store=workspace_store,
        )


async def test_describe_is_workspace_scoped(
    provider, workspace_store, workspace_id
) -> None:
    handle = await _seed_dataset_handle(workspace_store, workspace_id)

    with pytest.raises(CrossWorkspaceError):
        await describe_dataset(
            handle,
            workspace_id="ws-someone-else",
            cmr=provider,
            store=workspace_store,
        )
