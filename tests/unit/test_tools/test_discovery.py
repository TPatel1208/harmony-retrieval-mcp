"""``search_datasets`` — handle minting, workspace persistence, advisory flagging.

CMR HTTP is mocked with pytest-httpx (`httpx_mock`); the workspace store is the
real Postgres-backed fixture (`workspace_store`). KMS is a deterministic stub so
the test does not depend on the cached GCMD dump.
"""

from __future__ import annotations

import pytest

from earthdata_mcp.catalog.enrichment import ADVISORY_FLAG
from earthdata_mcp.config import Settings
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools.discovery import search_datasets
from earthdata_mcp.workspace.models import HandleType, handle_type_of
from earthdata_mcp.workspace.store import CrossWorkspaceError


class FakeKMS:
    """Deterministic KMS: always normalizes to the canonical GCMD keyword."""

    def __init__(self, terms: list[str]) -> None:
        self._terms = terms

    def normalize_keyword(self, term: str) -> list[str]:
        return list(self._terms)


@pytest.fixture
def provider() -> CMRProvider:
    return CMRProvider(Settings(_env_file=None))


# A TEMPO_NO2_L3 collection — curated in products.yaml, so enrichment attaches an
# advisory note we can assert is flagged.
_COLLECTION_ITEM = {
    "meta": {"concept-id": "C1-X", "provider-id": "LARC_CLOUD"},
    "umm": {
        "ShortName": "TEMPO_NO2_L3",
        "Version": "V03",
        "EntryTitle": "TEMPO NO2 tropospheric and stratospheric columns V03",
        "ProcessingLevel": {"Id": "3"},
    },
}


async def test_mints_dataset_handles_and_persists(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    httpx_mock.add_response(json={"items": [_COLLECTION_ITEM]})

    out = await search_datasets(
        "vegetation",
        workspace_id=workspace_id,
        cmr=provider,
        store=workspace_store,
        kms=FakeKMS(["VEGETATION INDEX"]),
    )

    assert out["count"] == 1
    assert len(out["datasets"]) == 1
    handle = out["datasets"][0]["handle"]
    # dataset_ prefix.
    assert handle.startswith("dataset_")
    assert handle_type_of(handle) is HandleType.DATASET

    summary = out["datasets"][0]["summary"]
    assert summary["short_name"] == "TEMPO_NO2_L3"
    assert summary["concept_id"] == "C1-X"

    # Workspace persistence: the handle resolves, scoped to this workspace, and its
    # payload carries the re-materializable spec (concept_id + search context).
    record = await workspace_store.get_handle(workspace_id, handle)
    assert handle_type_of(record.handle) is HandleType.DATASET
    assert record.payload["concept_id"] == "C1-X"
    assert record.payload["search"]["query"] == "vegetation"


async def test_advisory_notes_are_flagged(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    httpx_mock.add_response(json={"items": [_COLLECTION_ITEM]})

    out = await search_datasets(
        "vegetation",
        workspace_id=workspace_id,
        cmr=provider,
        store=workspace_store,
        kms=FakeKMS(["VEGETATION INDEX"]),
    )

    notes = out["datasets"][0]["summary"]["advisory_notes"]
    assert notes  # TEMPO_NO2_L3 is curated
    for note in notes:
        assert note["advisory"] == ADVISORY_FLAG
        assert note["authoritative"] is False


async def test_kms_normalization_feeds_cmr_keyword(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    httpx_mock.add_response(json={"items": []})

    await search_datasets(
        "veg",
        workspace_id=workspace_id,
        cmr=provider,
        store=workspace_store,
        kms=FakeKMS(["VEGETATION INDEX"]),
    )

    req = httpx_mock.get_requests()[0]
    assert "VEGETATION+INDEX" in str(req.url) or "VEGETATION%20INDEX" in str(req.url)


async def test_handles_are_workspace_scoped(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    httpx_mock.add_response(json={"items": [_COLLECTION_ITEM]})

    out = await search_datasets(
        "vegetation",
        workspace_id=workspace_id,
        cmr=provider,
        store=workspace_store,
        kms=FakeKMS(["VEGETATION INDEX"]),
    )
    handle = out["datasets"][0]["handle"]

    with pytest.raises(CrossWorkspaceError):
        await workspace_store.get_handle("ws-someone-else", handle)


async def test_filters_are_whitelisted(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    httpx_mock.add_response(json={"items": []})

    await search_datasets(
        "vegetation",
        filters={"provider": "LPCLOUD", "evil_key": "DROP TABLE"},
        workspace_id=workspace_id,
        cmr=provider,
        store=workspace_store,
        kms=FakeKMS(["VEGETATION INDEX"]),
    )

    url = str(httpx_mock.get_requests()[0].url)
    assert "provider=LPCLOUD" in url
    assert "evil_key" not in url
