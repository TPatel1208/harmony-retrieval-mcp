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

pytestmark = pytest.mark.live

_HAS_CREDS = bool(
    os.getenv("EARTHDATA_TOKEN")
    or (os.getenv("EARTHDATA_USERNAME") and os.getenv("EARTHDATA_PASSWORD"))
)


@pytest.mark.skipif(not _HAS_CREDS, reason="no EDL credentials in environment")
def test_real_login_authenticates() -> None:
    auth = EDLAuth()
    session = auth.login()
    assert auth.authenticated
    assert getattr(session, "authenticated", False)


@pytest.mark.skipif(not _HAS_CREDS, reason="no EDL credentials in environment")
def test_real_login_yields_s3_credentials() -> None:
    # A known DAAC with in-region S3 direct access (LP DAAC cloud).
    auth = EDLAuth()
    creds = auth.get_s3_credentials(daac="LPCLOUD")
    assert creds.get("accessKeyId")
    assert creds.get("secretAccessKey")
