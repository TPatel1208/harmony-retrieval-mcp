"""EDLAuth — session lifecycle, identity, S3 creds (PLAN.md §4.2, §4.6).

The ``earthaccess`` module is mocked (injected), so these tests do no network
I/O and need no real credentials. The ``@live`` check in
``tests/live/test_edl_auth.py`` exercises a real token.
"""

from __future__ import annotations

import pytest

from earthdata_mcp.config import Settings
from earthdata_mcp.providers.auth import AuthError, EDLAuth


class FakeAuth:
    """Stand-in for an ``earthaccess`` ``Auth`` session."""

    def __init__(self, username: str = "alice", authenticated: bool = True) -> None:
        self.username = username
        self.authenticated = authenticated
        self.s3_calls: list[tuple[str | None, str | None]] = []

    def get_s3_credentials(
        self, daac: str | None = None, provider: str | None = None
    ) -> dict[str, str]:
        self.s3_calls.append((daac, provider))
        return {"accessKeyId": "AK", "secretAccessKey": "SK"}


class FakeEarthaccess:
    """Mock of the ``earthaccess`` module recording how ``login`` was called."""

    def __init__(self, auth: FakeAuth | None = None) -> None:
        self._auth = auth if auth is not None else FakeAuth()
        self.login_calls: list[str] = []

    def login(self, strategy: str = "all") -> FakeAuth:
        self.login_calls.append(strategy)
        return self._auth


@pytest.fixture
def clean_edl_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure no ambient ``EARTHDATA_*`` vars leak into export assertions."""
    for name in ("EARTHDATA_USERNAME", "EARTHDATA_PASSWORD", "EARTHDATA_TOKEN"):
        monkeypatch.delenv(name, raising=False)


def _settings(**kw: object) -> Settings:
    return Settings(_env_file=None, **kw)


def test_login_uses_environment_strategy(clean_edl_env: None) -> None:
    fake = FakeEarthaccess()
    auth = EDLAuth(_settings(edl_username="alice", edl_password="pw"), earthaccess_module=fake)
    auth.login()
    # earthaccess 0.18's single "environment" strategy covers user/pass AND token.
    assert fake.login_calls == ["environment"]


def test_credentials_exported_to_env(clean_edl_env: None) -> None:
    import os

    fake = FakeEarthaccess()
    auth = EDLAuth(
        _settings(edl_username="alice", edl_password="pw", earthdata_token="tok123"),
        earthaccess_module=fake,
    )
    auth.login()
    assert os.environ["EARTHDATA_USERNAME"] == "alice"
    assert os.environ["EARTHDATA_PASSWORD"] == "pw"
    assert os.environ["EARTHDATA_TOKEN"] == "tok123"


def test_ambient_env_is_not_overwritten(
    clean_edl_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A real environment must win over configured defaults.
    monkeypatch.setenv("EARTHDATA_TOKEN", "real-token")
    fake = FakeEarthaccess()
    auth = EDLAuth(_settings(earthdata_token="config-token"), earthaccess_module=fake)
    auth.login()
    import os

    assert os.environ["EARTHDATA_TOKEN"] == "real-token"


def test_session_is_cached(clean_edl_env: None) -> None:
    fake = FakeEarthaccess()
    auth = EDLAuth(_settings(earthdata_token="tok"), earthaccess_module=fake)
    auth.login()
    auth.login()
    auth.identity()
    assert len(fake.login_calls) == 1  # logged in exactly once


def test_force_relogs(clean_edl_env: None) -> None:
    fake = FakeEarthaccess()
    auth = EDLAuth(_settings(earthdata_token="tok"), earthaccess_module=fake)
    auth.login()
    auth.login(force=True)
    assert len(fake.login_calls) == 2


def test_identity_maps_to_username(clean_edl_env: None) -> None:
    fake = FakeEarthaccess(FakeAuth(username="bob"))
    auth = EDLAuth(_settings(edl_username="bob", edl_password="pw"), earthaccess_module=fake)
    assert auth.identity() == "bob"


def test_get_s3_credentials_delegates(clean_edl_env: None) -> None:
    fake_auth = FakeAuth()
    fake = FakeEarthaccess(fake_auth)
    auth = EDLAuth(_settings(earthdata_token="tok"), earthaccess_module=fake)
    creds = auth.get_s3_credentials(daac="LPCLOUD")
    assert creds == {"accessKeyId": "AK", "secretAccessKey": "SK"}
    assert fake_auth.s3_calls == [("LPCLOUD", None)]


def test_unauthenticated_session_raises(clean_edl_env: None) -> None:
    fake = FakeEarthaccess(FakeAuth(authenticated=False))
    auth = EDLAuth(_settings(earthdata_token="tok"), earthaccess_module=fake)
    with pytest.raises(AuthError):
        auth.login()
    assert auth.authenticated is False


def test_reset_clears_cached_session(clean_edl_env: None) -> None:
    fake = FakeEarthaccess()
    auth = EDLAuth(_settings(earthdata_token="tok"), earthaccess_module=fake)
    auth.login()
    auth.reset()
    auth.login()
    assert len(fake.login_calls) == 2
