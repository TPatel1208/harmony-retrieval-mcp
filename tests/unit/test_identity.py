"""Per-call EDL identity layer (PRD 022) — parse, sessions, S3 creds, crypto.

No network and no real credentials anywhere: tokens are synthesized JWTs, HTTP
is an injected ``httpx.MockTransport``, and encryption keys are generated per
test. The assertions target external behavior at the module's seams — which
identity a NASA-facing call would use, what classification a failure surfaces
as — never session-cache internals (PRD 022 Testing Decisions).
"""

from __future__ import annotations

import base64
import json
import os
import time
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from cryptography.fernet import Fernet

from earthdata_mcp import redaction
from earthdata_mcp.config import Settings
from earthdata_mcp.providers import identity as identity_mod
from earthdata_mcp.providers.identity import (
    EULA_NOT_ACCEPTED,
    TOKEN_EXPIRED,
    TOKEN_INVALID,
    CredentialKeyUnavailable,
    EulaNotAcceptedError,
    TokenExpiredError,
    TokenInvalidError,
    classify_credential_failure,
    decrypt_credential,
    encrypt_credential,
    ensure_credential_key,
    get_token_session,
    parse_edl_token,
    reset_token_sessions,
    service_identity_scope,
)


def _jwt(uid: str | None = "alice", exp: float | None = None) -> str:
    """A synthetic EDL-shaped JWT (three base64url segments, uid/exp claims)."""

    def seg(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    claims: dict = {}
    if uid is not None:
        claims["uid"] = uid
    if exp is not None:
        claims["exp"] = exp
    return f"{seg({'alg': 'RS256'})}.{seg(claims)}.signature-bits"


@pytest.fixture(autouse=True)
def _clean_identity_state():
    reset_token_sessions()
    redaction.clear_secrets()
    yield
    reset_token_sessions()
    redaction.clear_secrets()


# -- parse_edl_token: the one boundary --------------------------------------


def test_absent_token_means_no_identity() -> None:
    assert parse_edl_token(None) is None
    assert parse_edl_token("") is None
    assert parse_edl_token("   ") is None


def test_valid_token_yields_username_and_expiry() -> None:
    exp = time.time() + 3600
    identity = parse_edl_token(_jwt(uid="bob", exp=exp))
    assert identity.username == "bob"
    assert identity.expires_at == datetime.fromtimestamp(exp, tz=timezone.utc)
    assert not identity.is_expired()
    assert identity.scope_key == "bob"


def test_token_without_uid_scopes_by_digest_not_raw_token() -> None:
    token = _jwt(uid=None, exp=time.time() + 3600)
    identity = parse_edl_token(token)
    assert identity.username is None
    assert identity.scope_key.startswith("token:")
    assert token not in identity.scope_key


def test_malformed_token_is_token_invalid_at_the_boundary() -> None:
    with pytest.raises(TokenInvalidError) as excinfo:
        parse_edl_token("not-a-jwt-at-all")
    assert str(excinfo.value).startswith(TOKEN_INVALID)


def test_expired_token_is_token_expired_at_the_boundary() -> None:
    with pytest.raises(TokenExpiredError) as excinfo:
        parse_edl_token(_jwt(exp=time.time() - 60))
    assert str(excinfo.value).startswith(TOKEN_EXPIRED)


def test_parse_never_touches_process_env() -> None:
    before = dict(os.environ)
    parse_edl_token(_jwt())
    assert dict(os.environ) == before


def test_identity_repr_never_leaks_the_token() -> None:
    token = _jwt()
    identity = parse_edl_token(token)
    assert token not in repr(identity)
    assert token not in str(identity)


def test_parse_registers_token_for_redaction() -> None:
    token = _jwt()
    parse_edl_token(token)
    assert token not in redaction.redact(f"submit failed with header Bearer {token}")


# -- per-identity session cache (bounded, expiring) --------------------------


def test_same_token_reuses_one_session_two_tokens_get_two() -> None:
    a1 = get_token_session(parse_edl_token(_jwt(uid="a")))
    a2 = get_token_session(parse_edl_token(_jwt(uid="a")))
    b = get_token_session(parse_edl_token(_jwt(uid="b")))
    assert a1 is a2
    assert b is not a1


def test_session_cache_expires_by_ttl() -> None:
    identity = parse_edl_token(_jwt(uid="a"))
    first = get_token_session(identity, now=0.0)
    still = get_token_session(identity, now=identity_mod._SESSION_TTL_SECONDS - 1)
    expired = get_token_session(identity, now=identity_mod._SESSION_TTL_SECONDS + 1)
    assert still is first
    assert expired is not first


def test_session_cache_is_bounded_lru() -> None:
    first = get_token_session(parse_edl_token(_jwt(uid="user0")), now=0.0)
    for i in range(1, identity_mod._SESSION_CACHE_MAX + 1):
        get_token_session(parse_edl_token(_jwt(uid=f"user{i}")), now=0.0)
    evicted = get_token_session(parse_edl_token(_jwt(uid="user0")), now=0.0)
    assert evicted is not first  # user0 was the oldest entry and fell out


def test_session_construction_never_touches_process_env() -> None:
    before = dict(os.environ)
    get_token_session(parse_edl_token(_jwt()))
    assert dict(os.environ) == before


def test_cached_client_is_built_once_per_key() -> None:
    session = get_token_session(parse_edl_token(_jwt()))
    builds: list[int] = []

    def build() -> object:
        builds.append(1)
        return object()

    first = session.client_for("harmony:PROD", build)
    second = session.client_for("harmony:PROD", build)
    assert first is second
    assert len(builds) == 1


# -- direct bearer-header S3 temporary credentials ----------------------------


def _mock_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_s3_credentials_fetched_with_per_token_bearer() -> None:
    token = _jwt(uid="alice")
    session = get_token_session(parse_edl_token(token))
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "ST"},
        )

    creds = await session.get_s3_credentials(daac="LPDAAC", http_client=_mock_client(handler))
    assert creds["accessKeyId"] == "AK"
    assert len(seen) == 1
    assert seen[0].headers["Authorization"] == f"Bearer {token}"
    # The endpoint came from earthaccess's own DAAC registry, not a hand-rolled URL.
    assert "s3credentials" in str(seen[0].url)


async def test_s3_credentials_cached_per_endpoint() -> None:
    session = get_token_session(parse_edl_token(_jwt()))
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(200, json={"accessKeyId": "AK", "secretAccessKey": "SK"})

    await session.get_s3_credentials(daac="LPDAAC", http_client=_mock_client(handler))
    await session.get_s3_credentials(daac="LPDAAC", http_client=_mock_client(handler))
    assert len(calls) == 1


async def test_s3_credentials_401_classifies_token_invalid() -> None:
    session = get_token_session(parse_edl_token(_jwt(exp=time.time() + 3600)))

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text="bad credentials")

    with pytest.raises(TokenInvalidError):
        await session.get_s3_credentials(daac="LPDAAC", http_client=_mock_client(handler))


async def test_s3_credentials_403_eula_classifies_with_resolution_url() -> None:
    session = get_token_session(parse_edl_token(_jwt()))
    url = "https://urs.earthdata.nasa.gov/approve_app?client_id=xyz"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            json={"error_description": "EULA Acceptance Failure", "resolution_url": url},
        )

    with pytest.raises(EulaNotAcceptedError) as excinfo:
        await session.get_s3_credentials(daac="LPDAAC", http_client=_mock_client(handler))
    assert excinfo.value.resolution_url == url
    assert str(excinfo.value).startswith(EULA_NOT_ACCEPTED)


def test_unknown_daac_has_no_endpoint() -> None:
    with pytest.raises(ValueError):
        identity_mod.s3_credentials_endpoint(daac="NOT_A_DAAC")


# -- failure classification (worker + tool boundary share this) --------------


def test_classify_passes_credential_errors_through() -> None:
    original = TokenExpiredError("boom")
    assert classify_credential_failure(original) is original


def test_classify_httpx_401_expired_token_is_token_expired() -> None:
    identity = identity_mod.EDLIdentity(
        token="tok-tok-tok", username="a",
        expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
    )
    request = httpx.Request("GET", "https://harmony.test/jobs")
    exc = httpx.HTTPStatusError(
        "401", request=request, response=httpx.Response(401, request=request)
    )
    classified = classify_credential_failure(exc, identity)
    assert isinstance(classified, TokenExpiredError)
    assert classified.classification == TOKEN_EXPIRED


def test_classify_harmony_two_arg_unauthorized_is_token_invalid() -> None:
    exc = Exception("Unauthorized", "invalid_credentials")
    classified = classify_credential_failure(exc, None)
    assert isinstance(classified, TokenInvalidError)


def test_classify_harmony_forbidden_eula_extracts_resolution_url() -> None:
    url = "https://urs.earthdata.nasa.gov/approve_app?client_id=abc"
    exc = Exception(
        "Forbidden", f'{{"message": "EULA acceptance required", "resolution_url": "{url}"}}'
    )
    classified = classify_credential_failure(exc, None)
    assert isinstance(classified, EulaNotAcceptedError)
    assert classified.resolution_url == url


def test_classify_leaves_non_auth_failures_alone() -> None:
    assert classify_credential_failure(RuntimeError("harmony 503"), None) is None
    assert classify_credential_failure(Exception("Bad Request", "no such variable"), None) is None
    request = httpx.Request("GET", "https://x.test/")
    exc = httpx.HTTPStatusError(
        "503", request=request, response=httpx.Response(503, request=request)
    )
    assert classify_credential_failure(exc, None) is None


# -- durable credential encryption (Fernet/MultiFernet) -----------------------


def _key() -> str:
    return Fernet.generate_key().decode()


def test_encrypt_decrypt_roundtrip_and_no_plaintext_at_rest() -> None:
    settings = Settings(_env_file=None, earthdata_credential_key=_key())
    token = _jwt(uid="alice")
    blob = encrypt_credential(token, settings)
    assert token not in blob
    assert decrypt_credential(blob, settings) == token


def test_multifernet_rotation_decrypts_under_the_old_key() -> None:
    old = _key()
    blob = encrypt_credential("tok-value-123", Settings(_env_file=None, earthdata_credential_key=old))
    rotated = Settings(_env_file=None, earthdata_credential_key=f"{_key()},{old}")
    assert decrypt_credential(blob, rotated) == "tok-value-123"


def test_missing_key_raises_credential_key_unavailable() -> None:
    settings = Settings(_env_file=None, earthdata_credential_key="")
    with pytest.raises(CredentialKeyUnavailable):
        ensure_credential_key(settings)
    with pytest.raises(CredentialKeyUnavailable):
        encrypt_credential("tok", settings)


def test_wrong_key_fails_decrypt_loud() -> None:
    blob = encrypt_credential("tok", Settings(_env_file=None, earthdata_credential_key=_key()))
    with pytest.raises(ValueError):
        decrypt_credential(blob, Settings(_env_file=None, earthdata_credential_key=_key()))


def test_decrypt_registers_plaintext_for_redaction() -> None:
    settings = Settings(_env_file=None, earthdata_credential_key=_key())
    token = _jwt(uid="worker-side")
    blob = encrypt_credential(token, settings)
    redaction.clear_secrets()
    decrypt_credential(blob, settings)
    assert token not in redaction.redact(f"error mentioning {token}")


# -- service identity scope ----------------------------------------------------


def test_service_scope_prefers_configured_username() -> None:
    assert service_identity_scope(Settings(_env_file=None, edl_username="svc")) == "svc"


def test_service_scope_derives_from_env_token_uid() -> None:
    settings = Settings(
        _env_file=None, edl_username="", earthdata_token=_jwt(uid="svc-uid")
    )
    assert service_identity_scope(settings) == "svc-uid"


def test_service_scope_falls_back_to_literal_service() -> None:
    settings = Settings(_env_file=None, edl_username="", earthdata_token="")
    assert service_identity_scope(settings) == "service"


# -- redaction chokepoint ------------------------------------------------------


def test_redact_replaces_registered_secret_everywhere() -> None:
    redaction.register_secret("super-secret-token-value")
    assert (
        redaction.redact("Authorization: Bearer super-secret-token-value failed")
        == f"Authorization: Bearer {redaction.REDACTED} failed"
    )
    scrubbed = redaction.redact_obj(
        {"nested": ["ok", {"deep": "x super-secret-token-value y"}], "n": 3}
    )
    assert scrubbed["n"] == 3
    assert "super-secret-token-value" not in json.dumps(scrubbed)


def test_short_values_are_never_registered() -> None:
    redaction.register_secret("abc")
    assert redaction.redact("abc def") == "abc def"


def test_log_records_are_redacted_at_the_factory(caplog) -> None:
    import logging

    token = _jwt(uid="loggy")
    parse_edl_token(token)  # registers + installs the factory
    logger = logging.getLogger("earthdata_mcp.test_redaction")
    with caplog.at_level(logging.INFO, logger=logger.name):
        logger.info("submitting with token %s for job %s", token, "job-1")
    combined = " ".join(r.getMessage() for r in caplog.records)
    assert token not in combined
    assert redaction.REDACTED in combined
