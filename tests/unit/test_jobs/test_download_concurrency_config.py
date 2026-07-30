"""``worker._configure_harmony_download_concurrency`` — wires harmony-py's
download thread pool size via ``NUM_REQUESTS_WORKERS``.

Deliberately a plain function called from :func:`worker.startup`, not a
module-level statement: ``get_settings()`` is ``@lru_cache``'d, so calling it
at import time would permanently freeze the process-wide Settings singleton
the moment anything merely imports ``worker`` (a test reaching for an
unrelated helper, e.g. ``_job_id_from_url``), rather than only when the
worker actually starts. No DB fixtures needed — this is pure env-var wiring.
"""

from __future__ import annotations

import os

import pytest

from earthdata_mcp.jobs.worker import _configure_harmony_download_concurrency


@pytest.fixture(autouse=True)
def _clean_env():
    os.environ.pop("NUM_REQUESTS_WORKERS", None)
    yield
    os.environ.pop("NUM_REQUESTS_WORKERS", None)


def test_sets_num_requests_workers_from_settings() -> None:
    from earthdata_mcp.config import get_settings

    _configure_harmony_download_concurrency()

    assert os.environ["NUM_REQUESTS_WORKERS"] == str(get_settings().retrieval_download_concurrency)


def test_does_not_clobber_an_operators_explicit_override() -> None:
    os.environ["NUM_REQUESTS_WORKERS"] = "99"

    _configure_harmony_download_concurrency()

    assert os.environ["NUM_REQUESTS_WORKERS"] == "99"
