"""``providers.build(spec, caps)`` — the single spec -> RetrievalProvider seam.

Replaces the worker's in-process ``_provider_for`` switch: ``submit_job`` /
``poll_job`` / ``materialize_job`` / ``startup`` all reconstruct their provider
through this one call. Capabilities are fetched by the caller (unchanged;
capability re-checking is intentional) and passed in directly — ``build``
itself does no CMR I/O, so these tests assert purely on which provider class
it returns and how it is constructed.
"""

from __future__ import annotations

import pytest

from earthdata_mcp.providers import build
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.appeears import AppEEARSProvider
from earthdata_mcp.providers.harmony import HarmonyProvider
from earthdata_mcp.providers.opendap import AxisGeometry, OPeNDAPProvider
from earthdata_mcp.providers.request_spec import RequestSpec


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


def _spec(provider: str, **extra) -> RequestSpec:
    return RequestSpec.from_jsonb({"concept_id": "C1-X", "provider": provider, **extra})


def test_build_harmony() -> None:
    provider = build(_spec("harmony"), _caps())
    assert isinstance(provider, HarmonyProvider)


def test_build_appeears() -> None:
    provider = build(_spec("appeears"), _caps())
    assert isinstance(provider, AppEEARSProvider)


def test_build_opendap_passes_urls() -> None:
    spec = _spec(
        "opendap",
        opendap_urls=["https://hyrax.example/g1", "https://hyrax.example/g2"],
    )
    provider = build(spec, _caps())
    assert isinstance(provider, OPeNDAPProvider)
    assert provider._opendap_urls == [
        "https://hyrax.example/g1",
        "https://hyrax.example/g2",
    ]


def test_build_opendap_singular_url_promoted_via_spec() -> None:
    """A legacy spec dict carrying only ``opendap_url`` still builds a provider
    with that one URL — the promotion happens in ``RequestSpec.from_jsonb``,
    ``build`` just consumes the already-resolved ``opendap_urls``."""
    spec = RequestSpec.from_jsonb(
        {"concept_id": "C1-X", "provider": "opendap", "opendap_url": "https://hyrax.example/g"}
    )
    provider = build(spec, _caps())
    assert isinstance(provider, OPeNDAPProvider)
    assert provider._opendap_urls == ["https://hyrax.example/g"]


def test_build_opendap_rebuilds_axis_geometry() -> None:
    """A spec carrying discovered grid geometry rebuilds identical AxisGeometry
    objects — a resumed job must reproduce the same hyperslab constraint."""
    spec = _spec(
        "opendap",
        opendap_urls=["https://hyrax.example/g1"],
        lat_axis={"name": "lat", "origin": -59.875, "step": 0.25, "length": 600},
        lon_axis={"name": "lon", "origin": -179.875, "step": 0.25, "length": 1440},
    )
    provider = build(spec, _caps())
    assert provider._lat_axis == AxisGeometry(
        name="lat", origin=-59.875, step=0.25, length=600
    )
    assert provider._lon_axis == AxisGeometry(
        name="lon", origin=-179.875, step=0.25, length=1440
    )


def test_build_opendap_rebuilds_var_dims() -> None:
    spec = _spec(
        "opendap",
        opendap_urls=["https://hyrax.example/g1"],
        var_dims={"Rainf_tavg": [["time", 1], ["lat", None], ["lon", None]]},
    )
    provider = build(spec, _caps())
    assert provider._var_dims == {
        "Rainf_tavg": (("time", 1), ("lat", None), ("lon", None))
    }


def test_build_opendap_without_geometry_in_spec() -> None:
    """No lat_axis/lon_axis in the spec rebuilds a provider with no geometry —
    the pre-existing whole-array behavior."""
    spec = _spec("opendap", opendap_urls=["https://hyrax.example/g1"])
    provider = build(spec, _caps())
    assert provider._lat_axis is None
    assert provider._lon_axis is None
    assert provider._var_dims == {}


def test_build_defaults_to_harmony_when_provider_absent() -> None:
    """A legacy spec without a 'provider' key resolves to Harmony (its old default)."""
    spec = RequestSpec.from_jsonb({"concept_id": "C1-X"})
    provider = build(spec, _caps())
    assert isinstance(provider, HarmonyProvider)


def test_build_unknown_provider_raises() -> None:
    with pytest.raises(ValueError, match="mystery"):
        build(_spec("mystery"), _caps())
