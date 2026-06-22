"""Live OPeNDAP DAP4 subset through OPeNDAPProvider (``@pytest.mark.live``, opt-in).

The credentialed end-to-end of our Hyrax/DAP4 wrapper (PLAN.md §4.2 step 3, Phase
7.3 gate). Skipped unless EDL credentials are present, so the default unit run
never touches the network. Run on demand / nightly CI:

    EARTHDATA_TOKEN=... docker compose exec mcp pytest -m live \
        tests/live/test_opendap_subset.py -v

Target: a known GES_DISC gridded collection. The granule's OPeNDAP URL is
discovered at runtime from CMR (so the test follows real granule rotation), and a
single variable is projected into a DAP4 constraint — exercising the real
submit → materialize path against Hyrax.
"""

from __future__ import annotations

import asyncio
import os

import pytest

from earthdata_mcp.config import Settings
from earthdata_mcp.providers.base import JobRef, RetrievalPlan, TransformSpec
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.providers.opendap import OPeNDAPProvider

pytestmark = pytest.mark.live

SHORT_NAME = "GLDAS_NOAH025_3H"
PROVIDER = "GES_DISC"
VARIABLE = "Rainf_tavg"
NETCDF = "application/x-netcdf"

_HAS_CREDS = bool(
    os.getenv("EARTHDATA_TOKEN")
    or (os.getenv("EARTHDATA_USERNAME") and os.getenv("EARTHDATA_PASSWORD"))
)


def _opendap_url(related_urls: list[dict]) -> str | None:
    """Pull a granule's OPeNDAP access URL from its RelatedUrls (sans .html)."""
    for entry in related_urls:
        url = str(entry.get("URL", ""))
        subtype = str(entry.get("Subtype", "")).upper()
        if "OPENDAP" in subtype or "opendap" in url.lower():
            return url[: -len(".html")] if url.endswith(".html") else url
    return None


@pytest.mark.skipif(not _HAS_CREDS, reason="no EDL credentials in environment")
def test_live_opendap_dap4_subset() -> None:
    async def _run() -> bytes:
        cmr = CMRProvider()
        collections = await cmr.search_collections(
            short_name=SHORT_NAME, provider=PROVIDER, limit=1
        )
        if not collections:
            pytest.skip(f"{SHORT_NAME} not found in CMR right now")
        concept_id = collections[0]["concept_id"]
        caps = await cmr.collection_capabilities(concept_id)

        granules = await cmr.search_granules(concept_id, limit=1)
        if not granules:
            pytest.skip(f"{SHORT_NAME} has no granules right now")
        url = _opendap_url(granules[0].get("related_urls", []))
        if not url:
            pytest.skip(f"{SHORT_NAME} granule advertises no OPeNDAP URL")

        provider = OPeNDAPProvider(caps, opendap_url=url, settings=Settings())
        plan = RetrievalPlan(
            output_format=NETCDF,
            needs_variable=True,
            concept_id=concept_id,
            transform=TransformSpec(output_format=NETCDF, variables=(VARIABLE,)),
        )
        assert provider.can_handle(plan)

        ref = await provider.submit(plan)
        assert ref.provider_job_url and ".dap.nc4?dap4.ce=" in ref.provider_job_url
        status = await provider.poll(ref)
        assert status.state.value == "ready"  # OPeNDAP is synchronous
        # materialize reads the constraint URL back off the JobRef.
        result = await provider.materialize(
            JobRef(
                provider="opendap",
                provider_job_url=ref.provider_job_url,
                job_handle="job_live_opendap",
            )
        )
        return await provider._storage_backend().get(result.storage_key)

    data = asyncio.run(_run())
    assert data, "OPeNDAP subset returned no bytes"
