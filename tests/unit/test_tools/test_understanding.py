"""``describe_dataset`` — handle resolution, variables, advisory flagging.

CMR HTTP is mocked (`httpx_mock`); the workspace store is the real Postgres
fixture. A ``dataset_`` handle is seeded directly via the store, mirroring what
``search_datasets`` persists (an enriched TEMPO_NO2_L3 collection).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from earthdata_mcp.catalog.enrichment import ADVISORY_FLAG, enrich_collection
from earthdata_mcp.config import Settings
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools.understanding import describe_dataset
from earthdata_mcp.workspace.models import HandleType
from earthdata_mcp.workspace.store import CrossWorkspaceError

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


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


# Collection with no UMM-Var associations at all — get_variables short-circuits
# after phase 1 (no second HTTP call is made).
def _mock_no_variable_associations(httpx_mock) -> None:
    httpx_mock.add_response(json={"items": [{"meta": {"associations": {}}}]})


_OPENDAP_GRANULE_URL = "https://opendap.example/data/GLDAS_NOAH025_3H.A20240101.0000.021"


def _mock_granule_search_with_opendap_url(httpx_mock, url: str = _OPENDAP_GRANULE_URL) -> None:
    httpx_mock.add_response(
        json={
            "items": [
                {
                    "meta": {"concept-id": "G1-X"},
                    "umm": {
                        "GranuleUR": "GLDAS_NOAH025_3H.A20240101.0000.021",
                        "RelatedUrls": [
                            {
                                "URL": f"{url}.html",
                                "Type": "USE SERVICE API",
                                "Subtype": "OPENDAP DATA",
                            }
                        ],
                    },
                }
            ]
        }
    )


def _mock_dmr_fetch(httpx_mock, fixture_name: str = "gldas_flat.dmr.xml") -> None:
    httpx_mock.add_response(text=(_FIXTURES / fixture_name).read_text())


# No granules at all in this window — the DMR fallback degrades to Task 1's
# "none" behavior without a second (get_dmr) HTTP call.
def _mock_no_granules(httpx_mock) -> None:
    httpx_mock.add_response(json={"items": []})


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
    assert out["variable_count"] == 1
    assert len(out["variables"]) == 1
    var = out["variables"][0]
    assert var["name"] == "vertical_column_troposphere"


async def test_describe_compact_by_default_trims_variable_and_metadata_fields(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    """Compact response: name/long_name/units/advisory_notes per variable, core
    identification fields only in the collection metadata echo — the shape a
    400-variable collection needs to stay usable."""
    handle = await _seed_dataset_handle(workspace_store, workspace_id)
    _mock_get_variables(httpx_mock)

    out = await describe_dataset(
        handle, workspace_id=workspace_id, cmr=provider, store=workspace_store
    )

    var = out["variables"][0]
    assert set(var) == {"name", "long_name", "units", "advisory_notes"}
    assert out["metadata"] == {
        "concept_id": "C1-X",
        "short_name": "TEMPO_NO2_L3",
        "version": "V03",
        "entry_title": "TEMPO NO2 V03",
        "processing_level": "3",
    }
    assert "abstract" not in out["metadata"]


async def test_describe_detail_true_returns_full_shape(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    """``detail=True`` opts back into today's full shape: every enriched QA fact
    per variable and the complete collection metadata blob."""
    handle = await _seed_dataset_handle(workspace_store, workspace_id)
    _mock_get_variables(httpx_mock)

    out = await describe_dataset(
        handle, workspace_id=workspace_id, detail=True,
        cmr=provider, store=workspace_store,
    )

    var = out["variables"][0]
    # QA fact is preserved from UMM-Var (authoritative) — full shape only.
    assert var["scale"] == 1
    assert out["variable_count"] == 1
    assert "entry_title" in out["metadata"]


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


async def test_describe_states_variable_source_umm_var_when_populated(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    handle = await _seed_dataset_handle(workspace_store, workspace_id)
    _mock_get_variables(httpx_mock)

    out = await describe_dataset(
        handle, workspace_id=workspace_id, cmr=provider, store=workspace_store
    )

    assert out["variable_source"] == "umm-var"
    assert "variables_note" not in out


async def test_describe_empty_catalog_states_source_none_with_note(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    """No UMM-Var *and* no granules to fall back to (DMR fail-soft: no granules
    found) — degrades all the way to the plain "none" honesty behavior."""
    handle = await _seed_dataset_handle(workspace_store, workspace_id)
    _mock_no_variable_associations(httpx_mock)
    _mock_no_granules(httpx_mock)

    out = await describe_dataset(
        handle, workspace_id=workspace_id, cmr=provider, store=workspace_store
    )

    assert out["variable_source"] == "none"
    assert out["variable_count"] == 0
    assert out["variables"] == []
    assert "UMM-Var" in out["variables_note"]


async def test_describe_empty_catalog_honesty_fields_survive_detail_mode(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    handle = await _seed_dataset_handle(workspace_store, workspace_id)
    _mock_no_variable_associations(httpx_mock)
    _mock_no_granules(httpx_mock)

    out = await describe_dataset(
        handle, workspace_id=workspace_id, detail=True,
        cmr=provider, store=workspace_store,
    )

    assert out["variable_source"] == "none"
    assert "UMM-Var" in out["variables_note"]


async def test_describe_falls_back_to_dmr_variable_listing_when_umm_var_empty(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    handle = await _seed_dataset_handle(workspace_store, workspace_id)
    _mock_no_variable_associations(httpx_mock)
    _mock_granule_search_with_opendap_url(httpx_mock)
    _mock_dmr_fetch(httpx_mock)

    out = await describe_dataset(
        handle, workspace_id=workspace_id, cmr=provider, store=workspace_store
    )

    assert out["variable_source"] == "opendap-dmr"
    assert out["variable_count"] == 4
    names = {v["name"] for v in out["variables"]}
    assert names == {"/lat", "/lon", "/time", "/precipitation"}
    assert set(out["variables"][0]) == {"name"}
    assert "one recent granule" in out["variables_note"] or "DMR" in out["variables_note"]


async def test_describe_dmr_fallback_degrades_on_fetch_error(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    """A granule with an OPeNDAP URL is found, but the DMR fetch itself fails
    (a 404) — degrades to Task 1's plain "none" behavior, never raises."""
    handle = await _seed_dataset_handle(workspace_store, workspace_id)
    _mock_no_variable_associations(httpx_mock)
    _mock_granule_search_with_opendap_url(httpx_mock)
    httpx_mock.add_response(status_code=404, text="Not Found")

    out = await describe_dataset(
        handle, workspace_id=workspace_id, cmr=provider, store=workspace_store
    )

    assert out["variable_source"] == "none"
    assert out["variable_count"] == 0
    assert "UMM-Var" in out["variables_note"]


async def test_describe_dmr_fallback_degrades_on_parse_error(
    httpx_mock, provider, workspace_store, workspace_id
) -> None:
    """The DMR fetch succeeds but returns malformed XML — degrades to Task 1's
    plain "none" behavior, never raises."""
    handle = await _seed_dataset_handle(workspace_store, workspace_id)
    _mock_no_variable_associations(httpx_mock)
    _mock_granule_search_with_opendap_url(httpx_mock)
    httpx_mock.add_response(text="<not valid xml")

    out = await describe_dataset(
        handle, workspace_id=workspace_id, cmr=provider, store=workspace_store
    )

    assert out["variable_source"] == "none"
    assert out["variable_count"] == 0
    assert "UMM-Var" in out["variables_note"]


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
