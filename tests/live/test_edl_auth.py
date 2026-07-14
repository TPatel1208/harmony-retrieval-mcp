"""Live EDL auth check (``@pytest.mark.live``, opt-in).

Confirms a real Earthdata Login token authenticates and yields in-region S3
credentials. Skipped unless credentials are present in the environment, so the
default unit run never touches the network. Run on demand / nightly CI:

    EARTHDATA_TOKEN=... docker compose exec mcp pytest -m live \
        tests/live/test_edl_auth.py -v
"""

from __future__ import annotations

import os

import pytest

from earthdata_mcp.providers.auth import EDLAuth
from earthdata_mcp.providers.identity import get_token_session, parse_edl_token

pytestmark = pytest.mark.live

_HAS_CREDS = bool(
    os.getenv("EARTHDATA_TOKEN")
    or (os.getenv("EARTHDATA_USERNAME") and os.getenv("EARTHDATA_PASSWORD"))
)

_HAS_TOKEN = bool(os.getenv("EARTHDATA_TOKEN"))


@pytest.mark.skipif(not _HAS_CREDS, reason="no EDL credentials in environment")
def test_real_login_authenticates() -> None:
    auth = EDLAuth()
    session = auth.login()
    assert auth.authenticated
    assert getattr(session, "authenticated", False)


@pytest.mark.skipif(not _HAS_CREDS, reason="no EDL credentials in environment")
def test_real_login_yields_s3_credentials() -> None:
    # A known DAAC with in-region S3 direct access (LP DAAC cloud). earthaccess
    # 0.18 keys its registry lookup on the DAAC short name ("LPDAAC") and
    # silently returns {} for the provider spelling ("LPCLOUD").
    auth = EDLAuth()
    creds = auth.get_s3_credentials(daac="LPDAAC")
    assert creds.get("accessKeyId")
    assert creds.get("secretAccessKey")


@pytest.mark.skipif(not _HAS_TOKEN, reason="no EARTHDATA_TOKEN in environment")
async def test_per_call_token_yields_s3_credentials() -> None:
    """PRD 022: a real token driven through the *per-call* path — parsed at the
    boundary into an identity, per-identity session, direct bearer-header HTTP
    to the DAAC's s3credentials endpoint — yields in-region S3 credentials
    without earthaccess's env-bound login and without touching process env."""
    before = dict(os.environ)
    identity = parse_edl_token(os.environ["EARTHDATA_TOKEN"])
    session = get_token_session(identity)
    creds = await session.get_s3_credentials(daac="LPCLOUD")
    assert creds.get("accessKeyId")
    assert creds.get("secretAccessKey")
    assert dict(os.environ) == before
