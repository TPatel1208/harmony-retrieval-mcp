"""``worker._job_id_from_url`` — recovers a Harmony job id from its status URL.

The durable row stores only ``provider_job_url`` (no ``provider_job_id`` column),
so a resumed poll must recover the id from the URL's last path segment, stripping
any query string harmony-py appends.

(Provider reconstruction from a durable spec — formerly ``_provider_for`` — now
lives behind ``providers.build``; see ``tests/unit/test_providers/test_build.py``
and ``tests/unit/test_providers/test_request_spec.py``.)
"""

from __future__ import annotations

from earthdata_mcp.jobs.worker import _job_id_from_url


def test_job_id_from_url_strips_query_string() -> None:
    """harmony-py status URLs carry a ``?linktype=…`` query — it must not leak into
    the id passed to ``client.status`` (which Harmony then rejects)."""
    url = "https://harmony.uat.earthdata.nasa.gov/jobs/abc-123?linktype=https"
    assert _job_id_from_url(url) == "abc-123"


def test_job_id_from_url_plain_path() -> None:
    assert _job_id_from_url("https://harmony.earthdata.nasa.gov/jobs/xyz-9") == "xyz-9"


def test_job_id_from_url_none() -> None:
    assert _job_id_from_url(None) is None
