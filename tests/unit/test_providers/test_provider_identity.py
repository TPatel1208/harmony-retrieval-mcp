"""Per-call ``edl_token`` threading through the providers (PRD 022).

Asserts *which identity a NASA-facing call would use* at each provider's
existing injectable seam: the Harmony client is built from harmony-py's
explicit ``token=`` (never process env), AppEEARS sends the per-call bearer
directly (and never re-logs in as the service account on a rejection),
OPeNDAP's auth header prefers the per-call token, and CMR's Bearer-gated
fetches do too. Absent token = the env-credential path, byte-for-byte.
"""

from __future__ import annotations

import base64
import json
import os

import pytest

from earthdata_mcp import redaction
from earthdata_mcp.config import Settings
from earthdata_mcp.providers import build as build_provider
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.appeears import AppEEARSProvider
from earthdata_mcp.providers.harmony import HarmonyProvider
from earthdata_mcp.providers.identity import (
    CredentialError,
    reset_token_sessions,
)
from earthdata_mcp.providers.opendap import OPeNDAPProvider
from earthdata_mcp.providers.request_spec import RequestSpec

APPEEARS_BASE = "https://appeears.test/api"
CMR_BASE = "https://cmr.test"
HARMONY_BASE = "https://harmony.test"


def _jwt(uid: str = "alice") -> str:
    def seg(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{seg({'alg': 'RS256'})}.{seg({'uid': uid})}.sig"


def _caps() -> CollectionCapabilities:
    return CollectionCapabilities(
        concept_id="C1-TEST",
        short_name="MOD13Q1",
        version="061",
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset(),
        direct_s3=None,
        services=[],
    )


@pytest.fixture(autouse=True)
def _clean_sessions():
    reset_token_sessions()
    redaction.clear_secrets()
    yield
    reset_token_sessions()
    redaction.clear_secrets()


# -- Harmony: harmony-py's explicit token=, cached per identity ---------------


class _FakeHarmonyClient:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs


def test_harmony_client_built_from_per_call_token_without_env_mutation(
    monkeypatch,
) -> None:
    import harmony

    monkeypatch.setattr(harmony, "Client", _FakeHarmonyClient)
    token = _jwt()
    before = dict(os.environ)
    provider = HarmonyProvider(
        _caps(),
        settings=Settings(_env_file=None, earthdata_token="env-tok"),
        edl_token=token,
    )
    client = provider._client()
    assert isinstance(client, _FakeHarmonyClient)
    assert client.kwargs["token"] == token  # the per-call identity, not env's
    assert dict(os.environ) == before  # no process-env mutation, ever


def test_harmony_env_credential_path_unchanged_without_token(monkeypatch) -> None:
    import harmony

    monkeypatch.setattr(harmony, "Client", _FakeHarmonyClient)
    provider = HarmonyProvider(
        _caps(), settings=Settings(_env_file=None, earthdata_token="env-tok")
    )
    assert provider._client().kwargs["token"] == "env-tok"


def test_harmony_clients_cached_per_identity(monkeypatch) -> None:
    import harmony

    monkeypatch.setattr(harmony, "Client", _FakeHarmonyClient)
    token_a, token_b = _jwt(uid="a"), _jwt(uid="b")
    settings = Settings(_env_file=None)
    first = HarmonyProvider(_caps(), settings=settings, edl_token=token_a)._client()
    again = HarmonyProvider(_caps(), settings=settings, edl_token=token_a)._client()
    other = HarmonyProvider(_caps(), settings=settings, edl_token=token_b)._client()
    assert again is first  # same identity, one session (story 15)
    assert other is not first  # two tokens, two sessions


# -- AppEEARS: per-call bearer, never a service-account re-login --------------


def _appeears(edl_token: str | None) -> AppEEARSProvider:
    # Deliberately NO service username/password: the per-call bearer must be
    # sufficient on its own.
    return AppEEARSProvider(
        _caps(),
        settings=Settings(_env_file=None, appeears_url=APPEEARS_BASE),
        edl_token=edl_token,
    )


async def test_appeears_sends_the_per_call_bearer(httpx_mock) -> None:
    token = _jwt()
    httpx_mock.add_response(
        url=f"{APPEEARS_BASE}/product/MOD13Q1.061", method="GET", json={"NDVI": {}}
    )
    await _appeears(token)._fetch_layer_map("MOD13Q1.061")
    requests = httpx_mock.get_requests()
    assert len(requests) == 1  # no POST /login round-trip at all
    assert requests[0].headers["Authorization"] == f"Bearer {token}"


async def test_appeears_rejection_is_classified_not_relogged_in(httpx_mock) -> None:
    httpx_mock.add_response(
        url=f"{APPEEARS_BASE}/product/MOD13Q1.061", method="GET", status_code=401
    )
    with pytest.raises(CredentialError):
        await _appeears(_jwt())._fetch_layer_map("MOD13Q1.061")
    # One request only: the service-account re-login retry must never fire for
    # a per-call identity (that would be silent identity substitution).
    assert len(httpx_mock.get_requests()) == 1


# -- OPeNDAP: per-instance bearer wins over the env token ---------------------


def test_opendap_auth_header_prefers_per_call_token() -> None:
    settings = Settings(_env_file=None, earthdata_token="env-tok")
    token = _jwt()
    with_token = OPeNDAPProvider(
        _caps(), opendap_urls=["https://opendap.test/g1.nc"],
        settings=settings, edl_token=token,
    )
    without = OPeNDAPProvider(
        _caps(), opendap_urls=["https://opendap.test/g1.nc"], settings=settings
    )
    assert with_token._auth_headers() == {"Authorization": f"Bearer {token}"}
    assert without._auth_headers() == {"Authorization": "Bearer env-tok"}


# -- CMR: bearer on searches and the Bearer-gated fetches ---------------------


def _cmr_settings() -> Settings:
    return Settings(
        _env_file=None,
        cmr_url=CMR_BASE,
        harmony_url=HARMONY_BASE,
        earthdata_token="env-tok",
    )


async def test_cmr_search_carries_the_per_call_bearer(httpx_mock) -> None:
    from earthdata_mcp.providers.cmr import CMRProvider

    token = _jwt()
    httpx_mock.add_response(json={"items": []})
    await CMRProvider(_cmr_settings(), edl_token=token).search_collections(keyword="x")
    assert httpx_mock.get_requests()[0].headers["Authorization"] == f"Bearer {token}"


async def test_cmr_search_stays_anonymous_without_a_token(httpx_mock) -> None:
    from earthdata_mcp.providers.cmr import CMRProvider

    httpx_mock.add_response(json={"items": []})
    await CMRProvider(_cmr_settings()).search_collections(keyword="x")
    assert "Authorization" not in httpx_mock.get_requests()[0].headers


async def test_cmr_harmony_capabilities_prefers_per_call_token(httpx_mock) -> None:
    from earthdata_mcp.providers.cmr import CMRProvider

    token = _jwt()
    httpx_mock.add_response(json={"services": []})
    await CMRProvider(_cmr_settings(), edl_token=token)._fetch_harmony_capabilities("C1-TEST")
    assert httpx_mock.get_requests()[0].headers["Authorization"] == f"Bearer {token}"


async def test_cmr_harmony_capabilities_env_token_without_override(httpx_mock) -> None:
    from earthdata_mcp.providers.cmr import CMRProvider

    httpx_mock.add_response(json={"services": []})
    await CMRProvider(_cmr_settings())._fetch_harmony_capabilities("C1-TEST")
    assert httpx_mock.get_requests()[0].headers["Authorization"] == "Bearer env-tok"


# -- providers.build threads the token into every provider --------------------


def _spec(provider: str) -> RequestSpec:
    return RequestSpec.from_jsonb(
        {
            "concept_id": "C1-TEST",
            "output_format": "application/netcdf4",
            "provider": provider,
            "opendap_urls": ["https://opendap.test/g1.nc"],
            "workspace_id": "ws",
            "job_handle": "job_x",
            "obs_handle": "obs_x",
        }
    )


def test_build_threads_token_into_each_provider() -> None:
    token = _jwt()
    harmony_p = build_provider(_spec("harmony"), _caps(), edl_token=token)
    appeears_p = build_provider(_spec("appeears"), _caps(), edl_token=token)
    opendap_p = build_provider(_spec("opendap"), _caps(), edl_token=token)
    assert harmony_p._edl_token == token
    assert appeears_p._edl_token == token
    assert opendap_p._edl_token == token
