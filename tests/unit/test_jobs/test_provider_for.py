"""worker._provider_for rebuilds the right provider from the durable spec.

The worker owns no provider state — on every task it reconstructs the retrieval
provider from ``request_spec["provider"]`` (the path the router chose), bound to
freshly-fetched capabilities. This is the carry-forward fix that lets an AppEEARS
or OPeNDAP job be driven by its own provider rather than always Harmony.

CMR is faked at the object level (``collection_capabilities`` as an ``AsyncMock``)
so no network is touched — the assertion is purely about which provider class the
switch returns for each spec.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.appeears import AppEEARSProvider
from earthdata_mcp.providers.harmony import HarmonyProvider
from earthdata_mcp.providers.opendap import AxisGeometry, OPeNDAPProvider
from earthdata_mcp.jobs.worker import _job_id_from_url, _plan_from_spec, _provider_for


def _caps() -> CollectionCapabilities:
    return CollectionCapabilities(
        concept_id="C1-X",
        short_name="MOD13Q1",
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset(),
        direct_s3=None,
        services=[],
    )


def _spec(provider: str, **extra) -> dict:
    return {"concept_id": "C1-X", "provider": provider, **extra}


async def _provider_for_with_mock_caps(spec: dict):
    """Call _provider_for with CMR.collection_capabilities stubbed to fixed caps."""
    with patch(
        "earthdata_mcp.providers.cmr.CMRProvider.collection_capabilities",
        new=AsyncMock(return_value=_caps()),
    ):
        return await _provider_for(spec)


async def test_provider_for_harmony() -> None:
    provider = await _provider_for_with_mock_caps(_spec("harmony"))
    assert isinstance(provider, HarmonyProvider)


async def test_provider_for_appeears() -> None:
    provider = await _provider_for_with_mock_caps(_spec("appeears"))
    assert isinstance(provider, AppEEARSProvider)


async def test_provider_for_opendap_passes_urls() -> None:
    spec = _spec(
        "opendap",
        opendap_urls=["https://hyrax.example/g1", "https://hyrax.example/g2"],
    )
    provider = await _provider_for_with_mock_caps(spec)
    assert isinstance(provider, OPeNDAPProvider)
    assert provider._opendap_urls == [
        "https://hyrax.example/g1",
        "https://hyrax.example/g2",
    ]


async def test_provider_for_opendap_falls_back_to_singular_url() -> None:
    """A spec written before multi-granule support carries only ``opendap_url``."""
    spec = _spec("opendap", opendap_url="https://hyrax.example/granule")
    provider = await _provider_for_with_mock_caps(spec)
    assert isinstance(provider, OPeNDAPProvider)
    assert provider._opendap_urls == ["https://hyrax.example/granule"]


async def test_provider_for_opendap_rebuilds_axis_geometry() -> None:
    """A spec carrying discovered grid geometry rebuilds identical AxisGeometry
    objects — a resumed job must reproduce the same hyperslab constraint."""
    spec = _spec(
        "opendap",
        opendap_urls=["https://hyrax.example/g1"],
        lat_axis={"name": "lat", "origin": -59.875, "step": 0.25, "length": 600},
        lon_axis={"name": "lon", "origin": -179.875, "step": 0.25, "length": 1440},
    )
    provider = await _provider_for_with_mock_caps(spec)
    assert isinstance(provider, OPeNDAPProvider)
    assert provider._lat_axis == AxisGeometry(
        name="lat", origin=-59.875, step=0.25, length=600
    )
    assert provider._lon_axis == AxisGeometry(
        name="lon", origin=-179.875, step=0.25, length=1440
    )


async def test_provider_for_opendap_rebuilds_var_dims() -> None:
    """A spec carrying per-variable dim plans rebuilds them as tuples-of-tuples —
    a resumed job must bracket each variable exactly as it was originally."""
    spec = _spec(
        "opendap",
        opendap_urls=["https://hyrax.example/g1"],
        var_dims={"Rainf_tavg": [["time", 1], ["lat", None], ["lon", None]]},
    )
    provider = await _provider_for_with_mock_caps(spec)
    assert provider._var_dims == {
        "Rainf_tavg": (("time", 1), ("lat", None), ("lon", None))
    }


async def test_provider_for_opendap_without_geometry_in_spec() -> None:
    """A spec with no lat_axis/lon_axis key (or an explicit None) rebuilds a
    provider with no geometry — the pre-existing whole-array behavior."""
    spec = _spec("opendap", opendap_urls=["https://hyrax.example/g1"])
    provider = await _provider_for_with_mock_caps(spec)
    assert provider._lat_axis is None
    assert provider._lon_axis is None
    assert provider._var_dims == {}


async def test_provider_for_defaults_to_harmony_when_absent() -> None:
    """A legacy spec without a 'provider' key resolves to Harmony (its old default)."""
    provider = await _provider_for_with_mock_caps({"concept_id": "C1-X"})
    assert isinstance(provider, HarmonyProvider)


async def test_provider_for_unknown_raises() -> None:
    with pytest.raises(ValueError, match="mystery"):
        await _provider_for_with_mock_caps(_spec("mystery"))


# -- _job_id_from_url: strip query string so the recovered id is clean ------


def test_job_id_from_url_strips_query_string() -> None:
    """harmony-py status URLs carry a ``?linktype=…`` query — it must not leak into
    the id passed to ``client.status`` (which Harmony then rejects)."""
    url = "https://harmony.uat.earthdata.nasa.gov/jobs/abc-123?linktype=https"
    assert _job_id_from_url(url) == "abc-123"


def test_job_id_from_url_plain_path() -> None:
    assert _job_id_from_url("https://harmony.earthdata.nasa.gov/jobs/xyz-9") == "xyz-9"


def test_job_id_from_url_none() -> None:
    assert _job_id_from_url(None) is None


# -- _plan_from_spec: tolerates missing optional fields ---------------------


def test_plan_from_spec_tolerates_missing_output_format() -> None:
    """Old specs without output_format must not KeyError; default to netcdf4."""
    spec = {"concept_id": "C1-X", "provider": "harmony"}
    plan = _plan_from_spec(spec)
    assert plan.output_format == "application/netcdf4"
