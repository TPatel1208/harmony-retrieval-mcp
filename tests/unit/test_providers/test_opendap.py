"""OPeNDAPProvider — Hyrax/DAP4 subset glue (PLAN.md §4.2 step 3, Phase 7.3 tests).

HTTP is mocked with pytest-httpx (``httpx_mock``); no network. We assert *our*
glue — the capability gate, the plan → DAP4 constraint-URL mapping (and that the
URL is the durable coordinate carried on the JobRef), the synchronous ``poll``,
and result persistence through ``StorageBackend``. The real Hyrax fetch is the
``@live`` test.
"""

from __future__ import annotations

from urllib.parse import unquote

import pytest

from earthdata_mcp.config import Settings
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.base import (
    AOI,
    JobRef,
    RetrievalPlan,
    TransformSpec,
)
from earthdata_mcp.providers.opendap import OPeNDAPProvider

OPENDAP_URL = (
    "https://opendap.earthdata.nasa.gov/collections/C1-GES_DISC/granules/"
    "GLDAS_NOAH025_3H.A20240101.0000.021.nc4"
)
NETCDF = "application/x-netcdf"


def _grid_caps() -> CollectionCapabilities:
    """A minimal gridded (L3) collection capability view — no Harmony service."""
    return CollectionCapabilities(
        concept_id="C1-GES_DISC",
        short_name="GLDAS_NOAH025_3H",
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset({"netCDF-4"}),
        direct_s3=None,
        services=[],
    )


def _swath_caps() -> CollectionCapabilities:
    caps = _grid_caps()
    caps.output_shape = "swath"
    return caps


def _point_caps() -> CollectionCapabilities:
    caps = _grid_caps()
    caps.output_shape = "point"
    return caps


def _settings() -> Settings:
    return Settings(_env_file=None, earthdata_token="tok")


def _provider(caps=None, *, opendap_url=OPENDAP_URL, storage=None) -> OPeNDAPProvider:
    return OPeNDAPProvider(
        caps or _grid_caps(),
        opendap_url=opendap_url,
        storage=storage,
        settings=_settings(),
    )


def _subset_plan(**kw) -> RetrievalPlan:
    base = dict(
        output_format=NETCDF,
        needs_variable=True,
        concept_id="C1-GES_DISC",
        transform=TransformSpec(output_format=NETCDF, variables=("precipitation",)),
    )
    base.update(kw)
    return RetrievalPlan(**base)


# -- capability gate -------------------------------------------------------


def test_can_handle_true_for_gridded_variable_subset() -> None:
    assert _provider().can_handle(_subset_plan()) is True


def test_can_handle_true_for_swath_bbox_subset() -> None:
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_bbox=True,
        aoi=AOI(bbox=(-105.0, 37.0, -104.0, 38.0)),
    )
    assert _provider(_swath_caps()).can_handle(plan) is True


def test_can_handle_false_for_png() -> None:
    plan = _subset_plan(output_format="image/png")
    assert _provider().can_handle(plan) is False


def test_can_handle_false_for_data_as_is() -> None:
    # No transform need → direct fetch territory, not OPeNDAP.
    plan = RetrievalPlan(output_format=NETCDF)
    assert _provider().can_handle(plan) is False


def test_can_handle_false_for_point_sample() -> None:
    # A point/area sample belongs to AppEEARS, never OPeNDAP.
    plan = _subset_plan(needs_point_sample=True)
    assert _provider().can_handle(plan) is False


def test_can_handle_false_for_point_shaped_collection() -> None:
    assert _provider(_point_caps()).can_handle(_subset_plan()) is False


def test_can_handle_false_without_opendap_endpoint() -> None:
    assert _provider(opendap_url=None).can_handle(_subset_plan()) is False


# -- submit: plan -> DAP4 constraint URL on the JobRef ---------------------


@pytest.mark.asyncio
async def test_submit_builds_dap4_url_with_projected_variables() -> None:
    plan = _subset_plan(
        needs_temporal=True,
        needs_bbox=True,
        aoi=AOI(bbox=(-105.0, 37.0, -104.0, 38.0)),
    )
    ref = await _provider().submit(plan)

    assert ref.provider == "opendap"
    assert ref.provider_job_id is None  # OPeNDAP has no provider-side job id
    assert ref.provider_job_url is not None
    assert ref.provider_job_url.startswith(OPENDAP_URL + ".dap.nc4?dap4.ce=")

    # The constraint expression projects the requested variable + needed coords.
    ce = unquote(ref.provider_job_url.split("dap4.ce=", 1)[1])
    assert "/precipitation" in ce
    assert "/lat" in ce and "/lon" in ce  # needs_bbox
    assert "/time" in ce  # needs_temporal


@pytest.mark.asyncio
async def test_submit_raises_when_it_cannot_handle() -> None:
    plan = RetrievalPlan(output_format="image/png", needs_variable=True)
    with pytest.raises(ValueError):
        await _provider().submit(plan)


# -- poll: synchronous service is ready immediately ------------------------


@pytest.mark.asyncio
async def test_poll_is_ready_immediately() -> None:
    status = await _provider().poll(JobRef(provider="opendap", provider_job_url="u"))
    assert status.state is JobState.READY
    assert status.progress == 100


# -- materialize: fetch off the JobRef URL, persist to storage -------------


@pytest.mark.asyncio
async def test_materialize_reads_url_off_jobref_and_persists(
    httpx_mock, local_backend
) -> None:
    payload = b"CDF\x01netcdf-subset-bytes"
    url = OPENDAP_URL + ".dap.nc4?dap4.ce=%2Fprecipitation"
    httpx_mock.add_response(url=url, content=payload)

    p = _provider(storage=local_backend)
    # The constraint URL is supplied on the JobRef — materialize must NOT rebuild it.
    ref = JobRef(provider="opendap", provider_job_url=url, job_handle="job_abc")
    result = await p.materialize(ref)

    assert result.media_type == NETCDF
    assert result.size_bytes == len(payload)
    assert (
        result.storage_key == "opendap/job_abc/GLDAS_NOAH025_3H.A20240101.0000.021.nc4"
    )
    assert await local_backend.get(result.storage_key) == payload

    # Bearer token from settings was sent.
    assert httpx_mock.get_requests()[0].headers.get("Authorization") == "Bearer tok"


@pytest.mark.asyncio
async def test_materialize_raises_without_constraint_url() -> None:
    with pytest.raises(ValueError, match="provider_job_url"):
        await _provider().materialize(JobRef(provider="opendap"))
