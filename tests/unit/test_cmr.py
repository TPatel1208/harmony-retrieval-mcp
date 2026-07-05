"""CMRProvider — normalization, retry policy, two-phase lookup, paging.

HTTP is mocked with pytest-httpx (`httpx_mock`); no network. Canon is CMR's
public API + UMM (docs/cmr_patterns.md).
"""

from __future__ import annotations

import httpx
import pytest

from earthdata_mcp import providers
from earthdata_mcp.config import Settings
from earthdata_mcp.providers import cmr as cmr_mod
from earthdata_mcp.providers.cmr import (
    CMRError,
    CMRProvider,
    normalize_collection_item,
    normalize_granule_item,
)


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


def test_normalize_granule_item_range_date_time() -> None:
    item = {
        "meta": {"concept-id": "G1-X"},
        "umm": {
            "GranuleUR": "MOD13Q1.A2024001.h09v04.061.nc",
            "TemporalExtent": {
                "RangeDateTime": {
                    "BeginningDateTime": "2024-01-01T00:00:00.000Z",
                    "EndingDateTime": "2024-01-01T00:05:00.000Z",
                }
            },
        },
    }
    out = normalize_granule_item(item)
    assert out["time_start"] == "2024-01-01T00:00:00.000Z"
    assert out["time_end"] == "2024-01-01T00:05:00.000Z"


def test_normalize_granule_item_single_date_time() -> None:
    item = {
        "meta": {"concept-id": "G2-X"},
        "umm": {
            "GranuleUR": "GPM.A2024001.nc",
            "TemporalExtent": {"SingleDateTime": "2024-01-01T00:00:00.000Z"},
        },
    }
    out = normalize_granule_item(item)
    assert out["time_start"] == "2024-01-01T00:00:00.000Z"
    assert out["time_end"] == "2024-01-01T00:00:00.000Z"


def test_normalize_granule_item_no_temporal_extent() -> None:
    item = {"meta": {"concept-id": "G3-X"}, "umm": {"GranuleUR": "X.nc"}}
    out = normalize_granule_item(item)
    assert out["time_start"] is None
    assert out["time_end"] is None


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


async def test_get_dmr_fetches_dmr_xml_from_granule_url(
    httpx_mock, provider: CMRProvider
) -> None:
    httpx_mock.add_response(text="<Dataset/>")
    xml = await provider.get_dmr("https://opendap.earthdata.nasa.gov/granules/ATL08.h5")
    assert xml == "<Dataset/>"
    req = httpx_mock.get_requests()[0]
    assert str(req.url) == "https://opendap.earthdata.nasa.gov/granules/ATL08.h5.dmr.xml"


async def test_get_dmr_sends_bearer_token_when_configured(httpx_mock) -> None:
    provider = CMRProvider(Settings(_env_file=None, earthdata_token="tok"))
    httpx_mock.add_response(text="<Dataset/>")
    await provider.get_dmr("https://opendap.earthdata.nasa.gov/granules/ATL08.h5")
    req = httpx_mock.get_requests()[0]
    assert req.headers["Authorization"] == "Bearer tok"


async def test_get_dmr_returns_none_on_4xx(httpx_mock, provider: CMRProvider) -> None:
    httpx_mock.add_response(status_code=404, text="not found")
    xml = await provider.get_dmr("https://opendap.earthdata.nasa.gov/granules/ATL08.h5")
    assert xml is None


async def test_get_dmr_returns_none_on_network_error(
    httpx_mock, provider: CMRProvider
) -> None:
    httpx_mock.add_exception(httpx.ConnectError("connection refused"))
    xml = await provider.get_dmr("https://opendap.earthdata.nasa.gov/granules/ATL08.h5")
    assert xml is None


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


async def test_get_variables_carries_dimensions_name_and_size(
    httpx_mock, provider: CMRProvider
) -> None:
    """UMM-V ``Dimensions`` is normalized to a lowercase name/size list — the
    data geometry discovery needs to derive an axis's cell count from it."""
    httpx_mock.add_response(
        json={"items": [{"meta": {"associations": {"variables": ["V1-X"]}}}]}
    )
    httpx_mock.add_response(
        json={
            "items": [
                {
                    "meta": {"concept-id": "V1-X"},
                    "umm": {
                        "Name": "lat",
                        "Dimensions": [{"Name": "lat", "Size": 600}],
                    },
                }
            ]
        }
    )
    variables = await provider.get_variables("C1-X")
    assert variables[0]["dimensions"] == [{"name": "lat", "size": 600}]


async def test_get_variables_returns_empty_without_associations(
    httpx_mock, provider: CMRProvider
) -> None:
    httpx_mock.add_response(json={"items": [{"meta": {"associations": {}}}]})
    assert await provider.get_variables("C1-X") == []


async def test_get_variables_batches_a_large_association_list(
    httpx_mock, provider: CMRProvider
) -> None:
    """A collection with hundreds of UMM-Var associations (MOD08_D3 carries 975)
    must never put every id on one query string — CloudFront returns a bare 414
    (Request-URI Too Long) well before CMR's own limits kick in, and that 414's
    boilerplate HTML gives no hint it's a batching problem. The complete list
    must come back too, never silently truncated to the default page limit:
    describe_dataset's ``variable_count`` promises "always the true total," and
    retrieve_subset's plan-time ``validate_variables`` must be able to confirm
    a real variable name that simply isn't among the first 50 associations in
    CMR's (arbitrary) ordering."""
    ids = [f"V{i}-X" for i in range(cmr_mod._VARIABLE_ID_BATCH_SIZE + 1)]
    httpx_mock.add_response(
        json={"items": [{"meta": {"associations": {"variables": ids}}}]}
    )
    httpx_mock.add_response(
        json={
            "items": [
                {"meta": {"concept-id": v}, "umm": {"Name": v}}
                for v in ids[: cmr_mod._VARIABLE_ID_BATCH_SIZE]
            ]
        }
    )
    httpx_mock.add_response(
        json={
            "items": [
                {"meta": {"concept-id": v}, "umm": {"Name": v}}
                for v in ids[cmr_mod._VARIABLE_ID_BATCH_SIZE :]
            ]
        }
    )
    variables = await provider.get_variables("C1-X")
    assert len(variables) == len(ids)
    assert {v["name"] for v in variables} == set(ids)


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


async def test_harmony_capabilities_5xx_returns_empty_dict(
    httpx_mock, provider: CMRProvider
) -> None:
    """A Harmony /capabilities 500 must not crash collection_capabilities.

    Harmony returns 500 when a collection has bogus service associations pointing
    at a non-existent provider (e.g. XYZ_PROV). The fix catches RetryableError
    and returns {} so the router can decide from an empty services list.
    """
    # CMR collection fetch (needed by collection_capabilities)
    httpx_mock.add_response(
        json={
            "items": [
                {
                    "meta": {"concept-id": "C1-X", "provider-id": "X", "associations": {}},
                    "umm": {"ShortName": "TEMPO_NO2_L3", "ProcessingLevel": {"Id": "3"}},
                }
            ]
        }
    )
    # Harmony /capabilities returns 500 four times (retried then re-raised).
    # No url= filter: pytest-httpx exact-matches including query params, so
    # collectionid=C1-X&format=json would break it — consume in order instead.
    for _ in range(4):
        httpx_mock.add_response(
            status_code=500,
            json={"code": "harmony.ServerError", "description": "Error: Internal server error"},
        )
    caps = await provider.collection_capabilities("C1-X")
    # Must not raise; services list is empty.
    assert caps.services == []
    assert caps.concept_id == "C1-X"


async def test_harmony_capabilities_4xx_returns_empty_dict(
    httpx_mock, provider: CMRProvider
) -> None:
    """A Harmony /capabilities 404 (collection unknown to Harmony) must also return {}."""
    httpx_mock.add_response(
        json={
            "items": [
                {
                    "meta": {"concept-id": "C2-X", "provider-id": "X", "associations": {}},
                    "umm": {"ShortName": "UNKNOWN_COL", "ProcessingLevel": {"Id": "3"}},
                }
            ]
        }
    )
    # No url= filter for same reason as the 5xx test above.
    httpx_mock.add_response(
        status_code=404,
        json={"code": "harmony.collectionNotFound"},
    )
    caps = await provider.collection_capabilities("C2-X")
    assert caps.services == []


def test_providers_package_importable() -> None:
    assert providers.__doc__  # sanity: package docstring intact
