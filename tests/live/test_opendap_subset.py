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
from earthdata_mcp.providers.base import AOI, JobRef, RetrievalPlan, TransformSpec
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.providers.opendap import (
    OPeNDAPProvider,
    _discover_grid_geometry,
    _resolve_from_cmr,
)
from earthdata_mcp.tools._dataio import NETCDF_BUNDLE_MEDIA_TYPE, open_result

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

        # Pull several granules so the multi-granule bundle path is exercised.
        granules = await cmr.search_granules(concept_id, limit=3)
        if not granules:
            pytest.skip(f"{SHORT_NAME} has no granules right now")
        urls = [
            u
            for u in (_opendap_url(g.get("related_urls", [])) for g in granules)
            if u
        ]
        if not urls:
            pytest.skip(f"{SHORT_NAME} granules advertise no OPeNDAP URL")

        provider = OPeNDAPProvider(caps, opendap_urls=urls, settings=Settings())
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
        # materialize reads the constraint URLs back off the JobRef and bundles them.
        result = await provider.materialize(
            JobRef(
                provider="opendap",
                provider_job_url=ref.provider_job_url,
                job_handle="job_live_opendap",
            )
        )
        assert result.media_type == NETCDF_BUNDLE_MEDIA_TYPE
        bundle = await provider._storage_backend().get(result.storage_key)
        return bundle, len(urls)

    bundle, n_granules = asyncio.run(_run())
    assert bundle, "OPeNDAP subset returned no bytes"
    # The bundle opens and concatenates the granules on the time axis.
    ds = open_result(bundle, NETCDF_BUNDLE_MEDIA_TYPE)
    assert ds.sizes.get("time", 0) >= n_granules


# ~5x5deg window over the US Midwest — the same case the PRD's production
# benchmark used to confirm OPeNDAP was serving whole-globe payloads.
_SMALL_BBOX = (-95.0, 38.0, -90.0, 43.0)


@pytest.mark.skipif(not _HAS_CREDS, reason="no EDL credentials in environment")
def test_live_opendap_bbox_hyperslab_reduces_bytes() -> None:
    """A small-bbox DAP4 subset must return materially fewer bytes than a
    whole-globe request for the same collection/variable/granule — the
    end-to-end regression guard for the bbox hyperslab (never silently regress
    back to whole-globe transfer)."""

    async def _run() -> tuple[int, int]:
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
        urls = [
            u
            for u in (_opendap_url(g.get("related_urls", [])) for g in granules)
            if u
        ]
        if not urls:
            pytest.skip(f"{SHORT_NAME} granules advertise no OPeNDAP URL")

        coord_lat, coord_lon, resolved = await _resolve_from_cmr(cmr, concept_id, (VARIABLE,))
        lat_axis, lon_axis, var_dims = await _discover_grid_geometry(
            cmr, concept_id, caps.spatial_extent, resolved, coord_lat, coord_lon
        )
        if lat_axis is None or lon_axis is None:
            pytest.skip(f"{SHORT_NAME} grid geometry could not be discovered from CMR")

        provider = OPeNDAPProvider(
            caps,
            opendap_urls=urls,
            coord_lat=coord_lat,
            coord_lon=coord_lon,
            lat_axis=lat_axis,
            lon_axis=lon_axis,
            var_dims=var_dims,
            settings=Settings(),
        )

        small_plan = RetrievalPlan(
            output_format=NETCDF,
            needs_variable=True,
            needs_bbox=True,
            concept_id=concept_id,
            aoi=AOI(bbox=_SMALL_BBOX),
            transform=TransformSpec(output_format=NETCDF, variables=resolved),
        )
        small_ref = await provider.submit(small_plan)
        small_result = await provider.materialize(
            JobRef(
                provider="opendap",
                provider_job_url=small_ref.provider_job_url,
                job_handle="job_live_opendap_small_bbox",
            )
        )

        whole_plan = RetrievalPlan(
            output_format=NETCDF,
            needs_variable=True,
            concept_id=concept_id,
            transform=TransformSpec(output_format=NETCDF, variables=resolved),
        )
        whole_ref = await provider.submit(whole_plan)
        whole_result = await provider.materialize(
            JobRef(
                provider="opendap",
                provider_job_url=whole_ref.provider_job_url,
                job_handle="job_live_opendap_whole_globe",
            )
        )
        return small_result.size_bytes, whole_result.size_bytes

    small_bytes, whole_bytes = asyncio.run(_run())
    assert small_bytes < whole_bytes, (
        f"bbox subset ({small_bytes} bytes) should be materially smaller than "
        f"whole-globe ({whole_bytes} bytes) — the hyperslab may not have taken effect"
    )
