"""CMRProvider — normalization, retry policy, two-phase lookup, paging.

HTTP is mocked with pytest-httpx (`httpx_mock`); no network. Canon is CMR's
public API + UMM (docs/cmr_patterns.md).
"""

from __future__ import annotations

import pytest

from earthdata_mcp import providers
from earthdata_mcp.config import Settings
from earthdata_mcp.providers import cmr as cmr_mod
from earthdata_mcp.providers.cmr import CMRError, CMRProvider, normalize_collection_item


@pytest.fixture
def settings() -> Settings:
    return Settings(_env_file=None)


@pytest.fixture
def provider(settings: Settings) -> CMRProvider:
    return CMRProvider(settings)


def test_normalize_surfaces_the_three_omitted_umm_c_fields() -> None:
    # The three fields NASA's normalize_collection_item omits at the pinned commit.
    item = {
        "meta": {"concept-id": "C1-X", "provider-id": "X"},
        "umm": {
            "ShortName": "FOO",
            "ProcessingLevel": {"Id": "3"},
            "DirectDistributionInformation": {
                "Region": "us-west-2",
                "S3BucketAndObjectPrefixNames": ["s3://b/p"],
            },
            "StandardProduct": False,
            "Purpose": "PROVISIONAL; see known issues",
            "ArchiveAndDistributionInformation": {
                "FileDistributionInformation": [
                    {"Format": "netCDF-4", "Media": ["HTTPS"]}
                ]
            },
        },
    }
    out = normalize_collection_item(item)
    assert out["direct_distribution_information"]["Region"] == "us-west-2"
    assert out["standard_product"] is False
    assert out["purpose"] == "PROVISIONAL; see known issues"
    assert out["native_formats"] == [{"format": "netCDF-4", "media_type": ["HTTPS"]}]


async def test_retries_5xx_then_succeeds(httpx_mock, provider: CMRProvider) -> None:
    httpx_mock.add_response(status_code=503, text="upstream blip")
    httpx_mock.add_response(json={"items": []})
    result = await provider.search_collections(keyword="rain")
    assert result == []
    assert len(httpx_mock.get_requests()) == 2  # one retry


async def test_does_not_retry_4xx(httpx_mock, provider: CMRProvider) -> None:
    httpx_mock.add_response(
        status_code=400, json={"errors": ["bad parameter: nope"]}
    )
    with pytest.raises(CMRError, match="bad parameter"):
        await provider.search_collections(keyword="rain")
    assert len(httpx_mock.get_requests()) == 1  # no retry on 4xx


async def test_sends_our_client_id_header(httpx_mock, provider: CMRProvider) -> None:
    httpx_mock.add_response(json={"items": []})
    await provider.search_collections(keyword="rain")
    req = httpx_mock.get_requests()[0]
    assert req.headers["Client-Id"] == "earthdata-retrieval-mcp"


async def test_two_phase_variable_lookup(httpx_mock, provider: CMRProvider) -> None:
    # Phase 1: collection fetch exposes meta.associations.variables.
    httpx_mock.add_response(
        json={"items": [{"meta": {"associations": {"variables": ["V99-X"]}}}]}
    )
    # Phase 2: variables.umm_json by concept_id[].
    httpx_mock.add_response(
        json={
            "items": [
                {"meta": {"concept-id": "V99-X"}, "umm": {"Name": "no2", "Scale": 2}}
            ]
        }
    )
    variables = await provider.get_variables("C1-X")
    assert [v["name"] for v in variables] == ["no2"]
    assert variables[0]["scale"] == 2
    # Phase 2 request must carry the discovered concept id.
    phase2 = httpx_mock.get_requests()[1]
    assert "V99-X" in str(phase2.url)


async def test_get_variables_returns_empty_without_associations(
    httpx_mock, provider: CMRProvider
) -> None:
    httpx_mock.add_response(json={"items": [{"meta": {"associations": {}}}]})
    assert await provider.get_variables("C1-X") == []


async def test_check_availability_is_count_only(
    httpx_mock, provider: CMRProvider
) -> None:
    httpx_mock.add_response(json={"items": []}, headers={"CMR-Hits": "42"})
    out = await provider.check_availability("C1-X", temporal="2024-01-01T00:00:00Z,")
    assert out["granule_count"] == 42
    assert out["available"] is True
    req = httpx_mock.get_requests()[0]
    assert "page_size=0" in str(req.url)


async def test_search_after_pagination(
    httpx_mock, provider: CMRProvider, monkeypatch
) -> None:
    monkeypatch.setattr(cmr_mod, "_PAGE_SIZE", 2)
    httpx_mock.add_response(
        json={"items": [{"meta": {}, "umm": {}}, {"meta": {}, "umm": {}}]},
        headers={"CMR-Search-After": "tok1"},
    )
    httpx_mock.add_response(
        json={"items": [{"meta": {}, "umm": {}}, {"meta": {}, "umm": {}}]}
    )
    results = await provider.search_collections(keyword="rain", limit=4)
    assert len(results) == 4
    # The second request resends the cursor in the CMR-Search-After header.
    assert httpx_mock.get_requests()[1].headers.get("CMR-Search-After") == "tok1"


async def test_get_citations_surfaces_doi_and_collection_citations(
    httpx_mock, provider: CMRProvider
) -> None:
    """DOI + formal CollectionCitations come straight from UMM-C; the associated
    citation concepts (works that cite the dataset) are counted, not fetched."""
    httpx_mock.add_response(
        json={
            "items": [
                {
                    "meta": {
                        "concept-id": "C2-X",
                        "associations": {"citations": ["CIT1-E", "CIT2-E", "CIT3-E"]},
                    },
                    "umm": {
                        "DOI": {
                            "DOI": "10.5067/MODIS/MOD13A1.061",
                            "Authority": "https://doi.org",
                        },
                        "CollectionCitations": [
                            {"Title": "MODIS/Terra Vegetation Indices", "Creator": "K. Didan"}
                        ],
                    },
                }
            ]
        }
    )
    out = await provider.get_citations("C2-X")
    assert out["concept_id"] == "C2-X"
    assert out["doi"] == "10.5067/MODIS/MOD13A1.061"
    assert out["doi_authority"] == "https://doi.org"
    assert out["collection_citations"][0]["Creator"] == "K. Didan"
    # Counted, not fetched (only the one collection request was made).
    assert out["reference_citation_count"] == 3
    assert len(httpx_mock.get_requests()) == 1


async def test_get_citations_graceful_without_doi_or_citations(
    httpx_mock, provider: CMRProvider
) -> None:
    """A collection with no DOI and no citations returns empty fields, not an error."""
    httpx_mock.add_response(
        json={"items": [{"meta": {"concept-id": "C3-X"}, "umm": {}}]}
    )
    out = await provider.get_citations("C3-X")
    assert out["doi"] is None
    assert out["doi_authority"] is None
    assert out["collection_citations"] == []
    assert out["reference_citation_count"] == 0


async def test_get_citations_graceful_when_collection_missing(
    httpx_mock, provider: CMRProvider
) -> None:
    """An unknown concept id (no items) yields empty fields, never an error."""
    httpx_mock.add_response(json={"items": []})
    out = await provider.get_citations("C-NOPE")
    assert out == {
        "concept_id": "C-NOPE",
        "doi": None,
        "doi_authority": None,
        "collection_citations": [],
        "reference_citation_count": 0,
    }


def test_providers_package_importable() -> None:
    assert providers.__doc__  # sanity: package docstring intact
