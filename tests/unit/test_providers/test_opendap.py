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
from earthdata_mcp.providers.opendap import AxisGeometry, OPeNDAPProvider

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


def _provider(caps=None, *, opendap_urls=(OPENDAP_URL,), storage=None) -> OPeNDAPProvider:
    return OPeNDAPProvider(
        caps or _grid_caps(),
        opendap_urls=list(opendap_urls),
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
    assert _provider(opendap_urls=[]).can_handle(_subset_plan()) is False


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
    # /time is NOT projected — temporal filtering is at CMR granule-search level;
    # many L3 files have no time variable at all (time is in the filename).
    ce = unquote(ref.provider_job_url.split("dap4.ce=", 1)[1])
    assert "precipitation" in ce
    assert "lat" in ce and "lon" in ce  # needs_bbox
    assert "/time" not in ce


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


# -- submit: multiple granules -> newline-joined constraint URLs -----------


@pytest.mark.asyncio
async def test_submit_builds_one_constraint_url_per_granule() -> None:
    url2 = (
        "https://opendap.earthdata.nasa.gov/collections/C1-GES_DISC/granules/"
        "GLDAS_NOAH025_3H.A20240101.0300.021.nc4"
    )
    p = _provider(opendap_urls=[OPENDAP_URL, url2])
    ref = await p.submit(_subset_plan())

    parts = ref.provider_job_url.split("\n")
    assert len(parts) == 2
    assert parts[0].startswith(OPENDAP_URL + ".dap.nc4?dap4.ce=")
    assert parts[1].startswith(url2 + ".dap.nc4?dap4.ce=")


# -- materialize: fetch each subset off the JobRef, persist one zip bundle --


@pytest.mark.asyncio
async def test_materialize_bundles_granules_into_one_zip(
    httpx_mock, local_backend
) -> None:
    import zipfile
    from io import BytesIO

    url1 = OPENDAP_URL + ".dap.nc4?dap4.ce=precipitation"
    url2 = (
        "https://opendap.earthdata.nasa.gov/collections/C1-GES_DISC/granules/"
        "GLDAS_NOAH025_3H.A20240101.0300.021.nc4.dap.nc4?dap4.ce=precipitation"
    )
    httpx_mock.add_response(url=url1, content=b"granule-one")
    httpx_mock.add_response(url=url2, content=b"granule-two")

    p = _provider(storage=local_backend)
    # Both constraint URLs are supplied on the JobRef — materialize must NOT rebuild.
    ref = JobRef(
        provider="opendap",
        provider_job_url=f"{url1}\n{url2}",
        job_handle="job_abc",
    )
    result = await p.materialize(ref)

    assert result.media_type == "application/netcdf-bundle+zip"
    assert result.storage_key == "opendap/job_abc/subset.nc.zip"
    assert result.extra["granule_count"] == 2

    # The stored object is a zip carrying both granule subsets under granule names.
    bundle = await local_backend.get(result.storage_key)
    with zipfile.ZipFile(BytesIO(bundle)) as zf:
        contents = {n: zf.read(n) for n in zf.namelist()}
    assert set(contents.values()) == {b"granule-one", b"granule-two"}

    # Bearer token from settings was sent on each fetch.
    assert all(
        r.headers.get("Authorization") == "Bearer tok"
        for r in httpx_mock.get_requests()
    )


@pytest.mark.asyncio
async def test_materialize_raises_without_constraint_url() -> None:
    with pytest.raises(ValueError, match="provider_job_url"):
        await _provider().materialize(JobRef(provider="opendap"))


# -- _resolve_from_cmr: variable path resolution ---------------------------


class _CmrStub:
    """Minimal CMR stub: returns a fixed variable list or raises on demand."""

    def __init__(self, vars_or_exc: list[dict] | BaseException) -> None:
        self._vars = vars_or_exc

    async def get_variables(self, concept_id: str, **_: object) -> list[dict]:
        if isinstance(self._vars, BaseException):
            raise self._vars
        return list(self._vars)


from earthdata_mcp.providers.opendap import (  # noqa: E402
    _constraint_expression,
    _discover_grid_geometry,
    _resolve_from_cmr,
)


# -- _constraint_expression: DAP4 FQN serialization ------------------------


def test_constraint_expression_root_variable_gets_leading_slash() -> None:
    """Root-level variable names are prefixed with '/' to form a DAP4 FQN."""
    plan = _subset_plan()  # variables=("precipitation",)
    ce = _constraint_expression(plan)
    assert ce == "/precipitation"


def test_constraint_expression_grouped_variable_preserves_existing_slash() -> None:
    """A variable already carrying a group path (leading '/') is used as-is."""
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        concept_id="C1-GES_DISC",
        transform=TransformSpec(
            output_format=NETCDF, variables=("/product/vertical_column_total",)
        ),
    )
    ce = _constraint_expression(plan)
    assert ce == "/product/vertical_column_total"


def test_constraint_expression_bbox_emits_fqn_for_flat_coords() -> None:
    """Flat collection coord names (bare 'lat'/'lon') get a leading slash."""
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        concept_id="C1-GES_DISC",
        aoi=AOI(bbox=(-105.0, 37.0, -104.0, 38.0)),
        transform=TransformSpec(output_format=NETCDF, variables=("precipitation",)),
    )
    ce = _constraint_expression(plan, coord_lat="lat", coord_lon="lon")
    parts = ce.split(";")
    assert "/lat" in parts
    assert "/lon" in parts
    assert "/precipitation" in parts


def test_constraint_expression_bbox_emits_fqn_for_grouped_coords() -> None:
    """Grouped collection coord names already start with '/' and pass through."""
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        concept_id="C1-TEMPO",
        aoi=AOI(bbox=(-105.0, 37.0, -104.0, 38.0)),
        transform=TransformSpec(
            output_format=NETCDF, variables=("/product/vertical_column_total",)
        ),
    )
    ce = _constraint_expression(
        plan,
        coord_lat="/product/latitude",
        coord_lon="/product/longitude",
    )
    parts = ce.split(";")
    assert "/product/latitude" in parts
    assert "/product/longitude" in parts
    assert "/product/vertical_column_total" in parts


# -- _constraint_expression: DAP4 hyperslab (bbox spatial subsetting) ------


def test_constraint_expression_bbox_with_geometry_emits_hyperslab() -> None:
    """Ascending lat/lon grid + known box -> correct inclusive index ranges on
    both the coordinate arrays and the projected data variable."""
    lat_axis = AxisGeometry(name="lat", origin=-59.875, step=0.25, length=600)
    lon_axis = AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        concept_id="C1-GES_DISC",
        aoi=AOI(bbox=(-95.0, 38.0, -90.0, 43.0)),
        transform=TransformSpec(output_format=NETCDF, variables=("precipitation",)),
    )
    ce = _constraint_expression(
        plan, coord_lat="lat", coord_lon="lon", lat_axis=lat_axis, lon_axis=lon_axis
    )
    parts = ce.split(";")
    assert "/lat[391:412]" in parts
    assert "/lon[339:360]" in parts
    assert "/precipitation[391:412][339:360]" in parts


def test_constraint_expression_descending_latitude_orders_indices_correctly() -> None:
    """A north-to-south (descending) latitude axis still yields low <= high,
    covering the requested window rather than being flipped or dropped."""
    lat_axis = AxisGeometry(name="lat", origin=89.875, step=-0.25, length=600)
    lon_axis = AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        concept_id="C1-GES_DISC",
        aoi=AOI(bbox=(-95.0, 38.0, -90.0, 43.0)),
        transform=TransformSpec(output_format=NETCDF, variables=("precipitation",)),
    )
    ce = _constraint_expression(
        plan, coord_lat="lat", coord_lon="lon", lat_axis=lat_axis, lon_axis=lon_axis
    )
    parts = ce.split(";")
    assert "/lat[187:208]" in parts
    assert "/precipitation[187:208][339:360]" in parts


def test_constraint_expression_grouped_coords_compose_with_hyperslab() -> None:
    """A grouped (TEMPO-style) collection's FQN coords/vars still get hyperslabs."""
    lat_axis = AxisGeometry(name="/product/latitude", origin=-59.875, step=0.25, length=600)
    lon_axis = AxisGeometry(name="/product/longitude", origin=-179.875, step=0.25, length=1440)
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        concept_id="C1-TEMPO",
        aoi=AOI(bbox=(-95.0, 38.0, -90.0, 43.0)),
        transform=TransformSpec(
            output_format=NETCDF, variables=("/product/vertical_column_total",)
        ),
    )
    ce = _constraint_expression(
        plan,
        coord_lat="/product/latitude",
        coord_lon="/product/longitude",
        lat_axis=lat_axis,
        lon_axis=lon_axis,
    )
    parts = ce.split(";")
    assert "/product/latitude[391:412]" in parts
    assert "/product/longitude[339:360]" in parts
    assert "/product/vertical_column_total[391:412][339:360]" in parts


def test_constraint_expression_box_exceeding_extent_is_clamped() -> None:
    """A box wider than the grid's extent clamps to the axis's valid indices."""
    lat_axis = AxisGeometry(name="lat", origin=-59.875, step=0.25, length=600)
    lon_axis = AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        concept_id="C1-GES_DISC",
        aoi=AOI(bbox=(-200.0, -90.0, 200.0, 100.0)),
        transform=TransformSpec(output_format=NETCDF, variables=("precipitation",)),
    )
    ce = _constraint_expression(
        plan, coord_lat="lat", coord_lon="lon", lat_axis=lat_axis, lon_axis=lon_axis
    )
    parts = ce.split(";")
    assert "/lat[0:599]" in parts
    assert "/lon[0:1439]" in parts


def test_constraint_expression_degenerate_point_box_yields_one_cell() -> None:
    """A point box (west == east, south == north) still returns at least one cell."""
    lat_axis = AxisGeometry(name="lat", origin=-59.875, step=0.25, length=600)
    lon_axis = AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        concept_id="C1-GES_DISC",
        aoi=AOI(bbox=(-90.125, 40.125, -90.125, 40.125)),
        transform=TransformSpec(output_format=NETCDF, variables=("precipitation",)),
    )
    ce = _constraint_expression(
        plan, coord_lat="lat", coord_lon="lon", lat_axis=lat_axis, lon_axis=lon_axis
    )
    parts = ce.split(";")
    assert "/lat[400:400]" in parts
    assert "/lon[359:359]" in parts


def test_constraint_expression_antimeridian_falls_back_to_whole_longitude() -> None:
    """west > east (a ±180deg wrap) skips the longitude hyperslab (v1: correct, not
    minimal) but still clips latitude."""
    lat_axis = AxisGeometry(name="lat", origin=-59.875, step=0.25, length=600)
    lon_axis = AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        concept_id="C1-GES_DISC",
        aoi=AOI(bbox=(170.0, 38.0, -170.0, 43.0)),
        transform=TransformSpec(output_format=NETCDF, variables=("precipitation",)),
    )
    ce = _constraint_expression(
        plan, coord_lat="lat", coord_lon="lon", lat_axis=lat_axis, lon_axis=lon_axis
    )
    parts = ce.split(";")
    assert "/lat[391:412]" in parts
    assert "/lon" in parts  # whole-longitude, no bracket
    assert "/precipitation[391:412]" in parts  # no lon bracket on the data var either


@pytest.mark.asyncio
async def test_submit_hyperslabs_when_provider_has_geometry_and_is_gridded() -> None:
    """A grid-shaped collection with axis geometry gets a real hyperslab on submit."""
    lat_axis = AxisGeometry(name="lat", origin=-59.875, step=0.25, length=600)
    lon_axis = AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
    provider = OPeNDAPProvider(
        _grid_caps(),
        opendap_urls=[OPENDAP_URL],
        storage=None,
        settings=_settings(),
        lat_axis=lat_axis,
        lon_axis=lon_axis,
    )
    plan = _subset_plan(needs_bbox=True, aoi=AOI(bbox=(-95.0, 38.0, -90.0, 43.0)))
    ref = await provider.submit(plan)
    ce = unquote(ref.provider_job_url.split("dap4.ce=", 1)[1])
    assert "[391:412]" in ce  # lat clipped
    assert "[339:360]" in ce  # lon clipped


@pytest.mark.asyncio
async def test_submit_swath_collection_ignores_geometry() -> None:
    """A swath (curvilinear) collection never hyperslabs, even with geometry set —
    a 1D index range cannot express a bbox on a 2D lat/lon field."""
    lat_axis = AxisGeometry(name="lat", origin=-59.875, step=0.25, length=600)
    lon_axis = AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
    provider = OPeNDAPProvider(
        _swath_caps(),
        opendap_urls=[OPENDAP_URL],
        storage=None,
        settings=_settings(),
        lat_axis=lat_axis,
        lon_axis=lon_axis,
    )
    plan = _subset_plan(needs_bbox=True, aoi=AOI(bbox=(-95.0, 38.0, -90.0, 43.0)))
    ref = await provider.submit(plan)
    ce = unquote(ref.provider_job_url.split("dap4.ce=", 1)[1])
    assert "[" not in ce  # whole-array, no hyperslab brackets at all


def test_constraint_expression_var_dims_full_ranges_non_spatial_dimension() -> None:
    """A variable carrying a non-spatial dimension (e.g. GLDAS's per-granule
    ``time``, size 1) gets a full-range bracket for it, in its own dimension
    order, alongside the lat/lon hyperslab — this is what actually shrinks the
    payload for the real target collection (verified against production Cloud
    OPeNDAP: a bracket-count mismatch is a 400 from Hyrax, not a safe no-op)."""
    lat_axis = AxisGeometry(name="lat", origin=-59.875, step=0.25, length=600)
    lon_axis = AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
    var_dims = {"precipitation": (("time", 1), ("lat", None), ("lon", None))}
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        concept_id="C1-GES_DISC",
        aoi=AOI(bbox=(-95.0, 38.0, -90.0, 43.0)),
        transform=TransformSpec(output_format=NETCDF, variables=("precipitation",)),
    )
    ce = _constraint_expression(
        plan,
        coord_lat="lat",
        coord_lon="lon",
        lat_axis=lat_axis,
        lon_axis=lon_axis,
        var_dims=var_dims,
    )
    parts = ce.split(";")
    assert "/precipitation[0:0][391:412][339:360]" in parts


def test_constraint_expression_var_dims_unresolved_variable_is_whole_array() -> None:
    """A variable absent from var_dims (its shape couldn't be verified) gets no
    bracket at all, even though geometry is available for other variables."""
    lat_axis = AxisGeometry(name="lat", origin=-59.875, step=0.25, length=600)
    lon_axis = AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        concept_id="C1-GES_DISC",
        aoi=AOI(bbox=(-95.0, 38.0, -90.0, 43.0)),
        transform=TransformSpec(output_format=NETCDF, variables=("mystery_var",)),
    )
    ce = _constraint_expression(
        plan,
        coord_lat="lat",
        coord_lon="lon",
        lat_axis=lat_axis,
        lon_axis=lon_axis,
        var_dims={},  # discovery ran but could not resolve this variable
    )
    parts = ce.split(";")
    assert "/mystery_var" in parts
    assert "/lat[391:412]" in parts  # coords still hyperslab regardless


@pytest.mark.asyncio
async def test_resolve_from_cmr_grouped_file() -> None:
    """A bare leaf name is resolved to its full grouped CMR path (case-insensitive)."""
    cmr = _CmrStub([{"name": "/product/ScienceData", "standard_name": None}])
    lat, lon, resolved = await _resolve_from_cmr(cmr, "C1", ("sciencedata",))
    assert resolved == ("/product/ScienceData",)


@pytest.mark.asyncio
async def test_resolve_from_cmr_exact_path_passthrough() -> None:
    """A variable name that already starts with '/' is used as-is, no CMR lookup."""
    cmr = _CmrStub([{"name": "/product/ScienceData", "standard_name": None}])
    lat, lon, resolved = await _resolve_from_cmr(cmr, "C1", ("/product/ScienceData",))
    assert resolved == ("/product/ScienceData",)


@pytest.mark.asyncio
async def test_resolve_from_cmr_ambiguity_raises() -> None:
    """A bare name matched by multiple CMR paths raises ValueError naming all paths."""
    cmr = _CmrStub([
        {"name": "/a/ndvi", "standard_name": None},
        {"name": "/b/ndvi", "standard_name": None},
    ])
    with pytest.raises(ValueError) as exc_info:
        await _resolve_from_cmr(cmr, "C1", ("ndvi",))
    msg = str(exc_info.value)
    assert "/a/ndvi" in msg and "/b/ndvi" in msg


@pytest.mark.asyncio
async def test_resolve_from_cmr_cmr_failure_fallback() -> None:
    """On any CMR exception the bare name flows through and no exception propagates."""
    cmr = _CmrStub(RuntimeError("network down"))
    lat, lon, resolved = await _resolve_from_cmr(cmr, "C1", ("precipitation",))
    assert resolved == ("precipitation",)
    assert lat == "lat" and lon == "lon"


@pytest.mark.asyncio
async def test_resolve_from_cmr_not_found_passthrough() -> None:
    """A bare name absent from CMR variables passes through unchanged."""
    cmr = _CmrStub([{"name": "/product/other_var", "standard_name": None}])
    lat, lon, resolved = await _resolve_from_cmr(cmr, "C1", ("myvar",))
    assert resolved == ("myvar",)


# -- _discover_grid_geometry: plan-time axis geometry -----------------------

# UMM-C reports the grid's outer *edges* (confirmed against production GLDAS:
# published west/south is exactly a half-cell short of the real lon[0]/lat[0]).
_GLDAS_EXTENT = (-180.0, -60.0, 180.0, 90.0)


def _gldas_cmr_vars() -> list[dict]:
    """A clean regular grid: lat/lon self-dims + one 2D (lat, lon) data var."""
    return [
        {"name": "lat", "dimensions": [{"name": "lat", "size": 600}]},
        {"name": "lon", "dimensions": [{"name": "lon", "size": 1440}]},
        {
            "name": "precipitation",
            "dimensions": [{"name": "lat", "size": 600}, {"name": "lon", "size": 1440}],
        },
    ]


@pytest.mark.asyncio
async def test_discover_grid_geometry_clean_regular_grid() -> None:
    """A well-formed UMM-C extent + UMM-V dims derives matching axis geometry,
    and a clean 2-D (lat, lon) variable gets a var_dims entry with no extra dims."""
    cmr = _CmrStub(_gldas_cmr_vars())
    lat_axis, lon_axis, var_dims = await _discover_grid_geometry(
        cmr, "C1", _GLDAS_EXTENT, ("precipitation",), "lat", "lon"
    )
    assert lat_axis == AxisGeometry(name="lat", origin=-59.875, step=0.25, length=600)
    assert lon_axis == AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
    assert var_dims == {"precipitation": (("lat", None), ("lon", None))}


@pytest.mark.asyncio
async def test_discover_grid_geometry_no_extent_fails_soft() -> None:
    """No UMM-C spatial extent -> (None, None, {}), never raises."""
    cmr = _CmrStub(_gldas_cmr_vars())
    result = await _discover_grid_geometry(
        cmr, "C1", None, ("precipitation",), "lat", "lon"
    )
    assert result == (None, None, {})


@pytest.mark.asyncio
async def test_discover_grid_geometry_cmr_error_fails_soft() -> None:
    """A CMR/network error during variable lookup -> (None, None, {}), never raises."""
    cmr = _CmrStub(RuntimeError("network down"))
    result = await _discover_grid_geometry(
        cmr, "C1", _GLDAS_EXTENT, ("precipitation",), "lat", "lon"
    )
    assert result == (None, None, {})


@pytest.mark.asyncio
async def test_discover_grid_geometry_missing_dimension_size_fails_soft() -> None:
    """lat/lon dimension sizes not resolvable from UMM-V -> (None, None, {})."""
    cmr = _CmrStub([{"name": "lat"}, {"name": "lon"}])  # no "dimensions" at all
    result = await _discover_grid_geometry(
        cmr, "C1", _GLDAS_EXTENT, ("precipitation",), "lat", "lon"
    )
    assert result == (None, None, {})


@pytest.mark.asyncio
async def test_discover_grid_geometry_extra_variable_dimension_gets_full_range() -> None:
    """A requested variable with a non-spatial 3rd dimension (e.g. GLDAS's
    per-granule ``time``, size 1) still gets real axis geometry — the extra
    dimension is resolved to its own full-range slot in var_dims, verified
    against production Cloud OPeNDAP (a positional CE needs one bracket per
    dimension or Hyrax rejects it outright)."""
    cmr_vars = _gldas_cmr_vars()
    cmr_vars[-1]["dimensions"].insert(0, {"name": "time", "size": 1})
    cmr = _CmrStub(cmr_vars)
    lat_axis, lon_axis, var_dims = await _discover_grid_geometry(
        cmr, "C1", _GLDAS_EXTENT, ("precipitation",), "lat", "lon"
    )
    assert lat_axis == AxisGeometry(name="lat", origin=-59.875, step=0.25, length=600)
    assert lon_axis == AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
    assert var_dims == {
        "precipitation": (("time", 1), ("lat", None), ("lon", None))
    }


@pytest.mark.asyncio
async def test_discover_grid_geometry_unresolvable_variable_omitted_from_var_dims() -> None:
    """A variable whose dims can't be safely bracketed (missing size on a
    non-spatial dim) is simply absent from var_dims — axis geometry for the
    rest of the request is unaffected."""
    cmr_vars = _gldas_cmr_vars()
    cmr_vars[-1]["dimensions"].insert(0, {"name": "time"})  # no "size"
    cmr = _CmrStub(cmr_vars)
    lat_axis, lon_axis, var_dims = await _discover_grid_geometry(
        cmr, "C1", _GLDAS_EXTENT, ("precipitation",), "lat", "lon"
    )
    assert lat_axis is not None and lon_axis is not None
    assert var_dims == {}
