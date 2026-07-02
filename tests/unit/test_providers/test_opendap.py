"""OPeNDAPProvider — Hyrax/DAP4 subset glue (PLAN.md §4.2 step 3, Phase 7.3 tests).

HTTP is mocked with pytest-httpx (``httpx_mock``); no network. We assert *our*
glue — the capability gate, the plan → DAP4 constraint-URL mapping (and that the
URL is the durable coordinate carried on the JobRef), the synchronous ``poll``,
and result persistence through ``StorageBackend``. The real Hyrax fetch is the
``@live`` test.
"""

from __future__ import annotations

from dataclasses import dataclass
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
    # /time is NOT projected when no real time coordinate was resolved — many
    # L3 files have no time variable at all (time is in the filename), and
    # this provider was built without a coord_time.
    ce = unquote(ref.provider_job_url.split("dap4.ce=", 1)[1])
    assert "precipitation" in ce
    assert "lat" in ce and "lon" in ce  # needs_bbox
    assert "/time" not in ce


@pytest.mark.asyncio
async def test_submit_projects_time_when_coord_time_resolved() -> None:
    """A collection with a real time coordinate (e.g. TROPOMI's per-scanline
    ``time``) must have it projected — otherwise Hyrax returns the data
    variable's ``time`` dimension without coordinate values, and xarray
    degrades it to a plain RangeIndex, breaking any downstream time-based
    resample."""
    provider = OPeNDAPProvider(
        _swath_caps(),
        opendap_urls=[OPENDAP_URL],
        coord_time="/product/time",
        settings=_settings(),
    )
    plan = _subset_plan(needs_temporal=True)
    ref = await provider.submit(plan)

    ce = unquote(ref.provider_job_url.split("dap4.ce=", 1)[1])
    assert "/product/time" in ce.split(";")


@pytest.mark.asyncio
async def test_submit_omits_time_when_coord_time_not_resolved() -> None:
    """No behavior change for collections with no UMM-V time variable at all."""
    ref = await _provider(_swath_caps()).submit(_subset_plan(needs_temporal=True))
    ce = unquote(ref.provider_job_url.split("dap4.ce=", 1)[1])
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
    _discover_grid_geometry,
    _resolve_from_cmr,
    build_constraint_expression,
)


# -- collection-archetype corpus: DAP4 constraint-expression serialization --
#
# Each row pins one reverse-engineered Hyrax/DAP4 quirk against the pure,
# public build_constraint_expression() core — see docs/opendap_quirk_ledger.md
# for the quirk each row id cross-references. Every assertion here previously
# lived in its own per-bug test function; none was dropped, only tabulated.


@dataclass(frozen=True)
class _CEArchetypeCase:
    """One collection-archetype row: inputs to build_constraint_expression()
    plus how to check its output. ``expect_equals`` asserts the whole CE
    string; ``expect_contains``/``expect_absent`` check individual ``;``-split
    tokens (or, for ``expect_absent``, a raw substring) — whichever the
    original per-bug test asserted."""

    id: str
    archetype: str
    plan: RetrievalPlan
    coord_lat: str = "lat"
    coord_lon: str = "lon"
    coord_time: str | None = None
    lat_axis: AxisGeometry | None = None
    lon_axis: AxisGeometry | None = None
    var_dims: dict | None = None
    expect_equals: str | None = None
    expect_contains: tuple[str, ...] = ()
    expect_absent: tuple[str, ...] = ()


def _bbox_plan(
    variables: tuple[str, ...], bbox: tuple[float, float, float, float], concept_id: str
) -> RetrievalPlan:
    return RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        concept_id=concept_id,
        aoi=AOI(bbox=bbox),
        transform=TransformSpec(output_format=NETCDF, variables=variables),
    )


_GRID_LAT_AXIS = AxisGeometry(name="lat", origin=-59.875, step=0.25, length=600)
_GRID_LON_AXIS = AxisGeometry(name="lon", origin=-179.875, step=0.25, length=1440)
_GROUPED_LAT_AXIS = AxisGeometry(name="/product/latitude", origin=-59.875, step=0.25, length=600)
_GROUPED_LON_AXIS = AxisGeometry(name="/product/longitude", origin=-179.875, step=0.25, length=1440)

_CE_ARCHETYPE_CORPUS: tuple[_CEArchetypeCase, ...] = (
    # -- flat / root-level (GLDAS-style): no group path, no leading slash yet.
    _CEArchetypeCase(
        id="flat_root_variable_gets_leading_slash",
        archetype="flat",
        plan=_subset_plan(),  # variables=("precipitation",)
        expect_equals="/precipitation",
    ),
    _CEArchetypeCase(
        id="flat_bbox_emits_fqn_for_flat_coords_no_geometry",
        archetype="flat",
        plan=_bbox_plan(("precipitation",), (-105.0, 37.0, -104.0, 38.0), "C1-GES_DISC"),
        expect_contains=("/lat", "/lon", "/precipitation"),
    ),
    _CEArchetypeCase(
        id="flat_names_with_spaces_pass_through_verbatim",
        archetype="names_with_spaces",
        # MOD13Q1-style: real MODIS variable names carry embedded spaces
        # (e.g. "250m 16 days NDVI"). Verbatim leading-slash FQN, no splitting
        # or sanitizing of the space.
        plan=_subset_plan(
            transform=TransformSpec(output_format=NETCDF, variables=("250m 16 days NDVI",))
        ),
        expect_equals="/250m 16 days NDVI",
    ),
    # -- grouped (TEMPO-style): variables/coords already carry a group path.
    _CEArchetypeCase(
        id="grouped_variable_preserves_existing_slash",
        archetype="grouped",
        plan=_subset_plan(
            transform=TransformSpec(
                output_format=NETCDF, variables=("/product/vertical_column_total",)
            )
        ),
        expect_equals="/product/vertical_column_total",
    ),
    _CEArchetypeCase(
        id="grouped_bbox_emits_fqn_for_grouped_coords_no_geometry",
        archetype="grouped",
        plan=_bbox_plan(
            ("/product/vertical_column_total",), (-105.0, 37.0, -104.0, 38.0), "C1-TEMPO"
        ),
        coord_lat="/product/latitude",
        coord_lon="/product/longitude",
        expect_contains=(
            "/product/latitude",
            "/product/longitude",
            "/product/vertical_column_total",
        ),
    ),
    _CEArchetypeCase(
        id="grouped_coords_compose_with_hyperslab",
        archetype="grouped",
        plan=_bbox_plan(
            ("/product/vertical_column_total",), (-95.0, 38.0, -90.0, 43.0), "C1-TEMPO"
        ),
        coord_lat="/product/latitude",
        coord_lon="/product/longitude",
        lat_axis=_GROUPED_LAT_AXIS,
        lon_axis=_GROUPED_LON_AXIS,
        expect_contains=(
            "/product/latitude[391:412]",
            "/product/longitude[339:360]",
            "/product/vertical_column_total[391:412][339:360]",
        ),
    ),
    # -- swath collection with a real per-scanline time coordinate (TROPOMI):
    # projected whole-array (no axis geometry narrows a 2D swath's time either)
    # so the response's time dimension keeps coordinate values downstream.
    _CEArchetypeCase(
        id="swath_time_coordinate_projected_when_resolved",
        archetype="swath_time",
        plan=_subset_plan(
            transform=TransformSpec(
                output_format=NETCDF, variables=("nitrogendioxide_tropospheric_column",)
            )
        ),
        coord_time="/product/time",
        expect_equals="/product/time;/nitrogendioxide_tropospheric_column",
    ),
    # -- grid-edge-bbox geometry (UMM-C convention): AxisGeometry.origin is
    # already the half-cell-offset *value* derived from the grid's outer
    # *edge* (see _discover_grid_geometry) — these rows pin the hyperslab
    # index math a corpus of that geometry shape must produce.
    _CEArchetypeCase(
        id="grid_edge_bbox_ascending_axis_hyperslab",
        archetype="grid_edge_bbox",
        plan=_bbox_plan(("precipitation",), (-95.0, 38.0, -90.0, 43.0), "C1-GES_DISC"),
        lat_axis=_GRID_LAT_AXIS,
        lon_axis=_GRID_LON_AXIS,
        expect_contains=(
            "/lat[391:412]",
            "/lon[339:360]",
            "/precipitation[391:412][339:360]",
        ),
    ),
    _CEArchetypeCase(
        id="grid_edge_bbox_descending_latitude_orders_indices_correctly",
        archetype="grid_edge_bbox",
        plan=_bbox_plan(("precipitation",), (-95.0, 38.0, -90.0, 43.0), "C1-GES_DISC"),
        lat_axis=AxisGeometry(name="lat", origin=89.875, step=-0.25, length=600),
        lon_axis=_GRID_LON_AXIS,
        expect_contains=("/lat[187:208]", "/precipitation[187:208][339:360]"),
    ),
    _CEArchetypeCase(
        id="grid_edge_bbox_box_exceeding_extent_is_clamped",
        archetype="grid_edge_bbox",
        plan=_bbox_plan(("precipitation",), (-200.0, -90.0, 200.0, 100.0), "C1-GES_DISC"),
        lat_axis=_GRID_LAT_AXIS,
        lon_axis=_GRID_LON_AXIS,
        expect_contains=("/lat[0:599]", "/lon[0:1439]"),
    ),
    _CEArchetypeCase(
        id="grid_edge_bbox_degenerate_point_box_yields_one_cell",
        archetype="grid_edge_bbox",
        plan=_bbox_plan(
            ("precipitation",), (-90.125, 40.125, -90.125, 40.125), "C1-GES_DISC"
        ),
        lat_axis=_GRID_LAT_AXIS,
        lon_axis=_GRID_LON_AXIS,
        expect_contains=("/lat[400:400]", "/lon[359:359]"),
    ),
    _CEArchetypeCase(
        id="grid_edge_bbox_antimeridian_falls_back_to_whole_longitude",
        archetype="grid_edge_bbox",
        plan=_bbox_plan(("precipitation",), (170.0, 38.0, -170.0, 43.0), "C1-GES_DISC"),
        lat_axis=_GRID_LAT_AXIS,
        lon_axis=_GRID_LON_AXIS,
        expect_contains=(
            "/lat[391:412]",
            "/lon",  # whole-longitude, no bracket
            "/precipitation[391:412]",  # no lon bracket on the data var either
        ),
    ),
    _CEArchetypeCase(
        id="grid_edge_bbox_var_dims_full_ranges_non_spatial_dimension",
        archetype="grid_edge_bbox",
        plan=_bbox_plan(("precipitation",), (-95.0, 38.0, -90.0, 43.0), "C1-GES_DISC"),
        lat_axis=_GRID_LAT_AXIS,
        lon_axis=_GRID_LON_AXIS,
        var_dims={"precipitation": (("time", 1), ("lat", None), ("lon", None))},
        expect_contains=("/precipitation[0:0][391:412][339:360]",),
    ),
    _CEArchetypeCase(
        id="grid_edge_bbox_var_dims_unresolved_variable_is_whole_array",
        archetype="grid_edge_bbox",
        plan=_bbox_plan(("mystery_var",), (-95.0, 38.0, -90.0, 43.0), "C1-GES_DISC"),
        lat_axis=_GRID_LAT_AXIS,
        lon_axis=_GRID_LON_AXIS,
        var_dims={},  # discovery ran but could not resolve this variable
        expect_contains=("/mystery_var", "/lat[391:412]"),
    ),
)


@pytest.mark.parametrize("case", _CE_ARCHETYPE_CORPUS, ids=lambda c: c.id)
def test_constraint_expression_archetype_corpus(case: _CEArchetypeCase) -> None:
    ce = build_constraint_expression(
        case.plan,
        coord_lat=case.coord_lat,
        coord_lon=case.coord_lon,
        coord_time=case.coord_time,
        lat_axis=case.lat_axis,
        lon_axis=case.lon_axis,
        var_dims=case.var_dims,
    )
    if case.expect_equals is not None:
        assert ce == case.expect_equals
    parts = ce.split(";")
    for token in case.expect_contains:
        assert token in parts
    for absent in case.expect_absent:
        assert absent not in ce


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


@pytest.mark.asyncio
async def test_resolve_from_cmr_grouped_file() -> None:
    """A bare leaf name is resolved to its full grouped CMR path (case-insensitive)."""
    cmr = _CmrStub([{"name": "/product/ScienceData", "standard_name": None}])
    lat, lon, time, resolved = await _resolve_from_cmr(cmr, "C1", ("sciencedata",))
    assert resolved == ("/product/ScienceData",)


@pytest.mark.asyncio
async def test_resolve_from_cmr_exact_path_passthrough() -> None:
    """A variable name that already starts with '/' is used as-is, no CMR lookup."""
    cmr = _CmrStub([{"name": "/product/ScienceData", "standard_name": None}])
    lat, lon, time, resolved = await _resolve_from_cmr(cmr, "C1", ("/product/ScienceData",))
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
    lat, lon, time, resolved = await _resolve_from_cmr(cmr, "C1", ("precipitation",))
    assert resolved == ("precipitation",)
    assert lat == "lat" and lon == "lon"
    assert time is None


@pytest.mark.asyncio
async def test_resolve_from_cmr_not_found_passthrough() -> None:
    """A bare name absent from CMR variables passes through unchanged."""
    cmr = _CmrStub([{"name": "/product/other_var", "standard_name": None}])
    lat, lon, time, resolved = await _resolve_from_cmr(cmr, "C1", ("myvar",))
    assert resolved == ("myvar",)


@pytest.mark.asyncio
async def test_resolve_from_cmr_discovers_time_by_standard_name() -> None:
    cmr = _CmrStub([{"name": "/product/time", "standard_name": "time"}])
    _lat, _lon, time, _resolved = await _resolve_from_cmr(cmr, "C1", ("no2",))
    assert time == "/product/time"


@pytest.mark.asyncio
async def test_resolve_from_cmr_discovers_time_by_leaf_name() -> None:
    cmr = _CmrStub([{"name": "/product/time", "standard_name": None}])
    _lat, _lon, time, _resolved = await _resolve_from_cmr(cmr, "C1", ("no2",))
    assert time == "/product/time"


@pytest.mark.asyncio
async def test_resolve_from_cmr_no_time_variable_stays_none() -> None:
    """L3 collections with no UMM-V time variable resolve time to None — the
    fail-soft default that keeps /time out of the CE."""
    cmr = _CmrStub([{"name": "/precipitation", "standard_name": None}])
    _lat, _lon, time, _resolved = await _resolve_from_cmr(cmr, "C1", ("precipitation",))
    assert time is None


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


# -- plan_subset: the OPeNDAP planning seam ---------------------------------
#
# plan_subset composes granule-URL discovery + _resolve_from_cmr +
# _discover_grid_geometry behind one public entry point. These tests assert
# the *composition* (what plan_subset returns for a given fake CMR + inputs),
# not the internals each helper already has its own exhaustive coverage for.

from earthdata_mcp.providers.opendap import plan_subset  # noqa: E402

_PLAN_BBOX = (-105.0, 37.0, -104.0, 38.0)

_GROUPED_VARS = [
    {"name": "/product/vertical_column_total", "standard_name": None},
    {"name": "/product/latitude", "standard_name": "latitude"},
    {"name": "/product/longitude", "standard_name": "longitude"},
]

# TROPOMI-shaped: a swath (L2) collection whose science variable carries a
# real per-scanline time coordinate — the case _resolve_from_cmr must surface
# via coord_time so the OPeNDAP CE actually projects it.
_SWATH_VARS_WITH_TIME = [
    {"name": "/PRODUCT/nitrogendioxide_tropospheric_column", "standard_name": None},
    {"name": "/PRODUCT/latitude", "standard_name": "latitude"},
    {"name": "/PRODUCT/longitude", "standard_name": "longitude"},
    {"name": "/PRODUCT/time", "standard_name": "time"},
]

_AMBIGUOUS_VARS = [
    {"name": "/a/ndvi", "standard_name": None},
    {"name": "/b/ndvi", "standard_name": None},
]

# UMM-C reports the grid's outer *edges*, not the first cell's value (see
# _discover_grid_geometry's docstring).
_GRID_EXTENT = (-180.0, -60.0, 180.0, 90.0)

_GRID_GEOMETRY_VARS = [
    {
        "name": "/product/latitude",
        "standard_name": "latitude",
        "dimensions": [{"name": "latitude", "size": 600}],
    },
    {
        "name": "/product/longitude",
        "standard_name": "longitude",
        "dimensions": [{"name": "longitude", "size": 1440}],
    },
    {
        "name": "/product/vertical_column_total",
        "standard_name": None,
        "dimensions": [
            {"name": "latitude", "size": 600},
            {"name": "longitude", "size": 1440},
        ],
    },
]


def _granule_with_opendap_url(url: str) -> dict:
    return {
        "related_urls": [
            {"URL": url, "Type": "USE SERVICE API", "Subtype": "OPENDAP DATA"}
        ]
    }


class _PlanCmrStub:
    """A CMR stub whose ``search_granules`` and ``get_variables`` are both
    independently configurable — ``plan_subset`` calls both."""

    def __init__(
        self,
        *,
        granules: list[dict] | None = None,
        variables: list[dict] | BaseException | None = None,
    ) -> None:
        self._granules = granules if granules is not None else []
        self._variables = variables
        self.get_variables_called = False

    async def search_granules(
        self, concept_id: str, *, bounding_box=None, temporal=None, limit=None
    ) -> list[dict]:
        return self._granules

    async def get_variables(self, concept_id: str, **_: object) -> list[dict]:
        self.get_variables_called = True
        if isinstance(self._variables, BaseException):
            raise self._variables
        return list(self._variables or [])


@pytest.mark.asyncio
async def test_plan_subset_discovers_granule_urls() -> None:
    cmr = _PlanCmrStub(granules=[_granule_with_opendap_url(OPENDAP_URL)])
    plan = await plan_subset(cmr, _grid_caps(), "C1-GES_DISC", _PLAN_BBOX, "", ("precipitation",))
    assert plan.opendap_urls == [OPENDAP_URL]


@pytest.mark.asyncio
async def test_plan_subset_resolves_bare_leaf_to_grouped_path() -> None:
    """When granule URLs are found, a bare leaf variable name is resolved to its
    full UMM-V group path (the TEMPO case), and the resolved coordinate names
    are carried on the plan too."""
    cmr = _PlanCmrStub(
        granules=[_granule_with_opendap_url(OPENDAP_URL)], variables=_GROUPED_VARS
    )
    plan = await plan_subset(
        cmr, _grid_caps(), "C1-GES_DISC", _PLAN_BBOX, "", ("vertical_column_total",)
    )
    assert plan.variables == ("/product/vertical_column_total",)
    assert plan.coord_lat == "/product/latitude"
    assert plan.coord_lon == "/product/longitude"


@pytest.mark.asyncio
async def test_plan_subset_already_qualified_variable_passes_through() -> None:
    cmr = _PlanCmrStub(
        granules=[_granule_with_opendap_url(OPENDAP_URL)], variables=_GROUPED_VARS
    )
    plan = await plan_subset(
        cmr, _grid_caps(), "C1-GES_DISC", _PLAN_BBOX, "",
        ("/product/vertical_column_total",),
    )
    assert plan.variables == ("/product/vertical_column_total",)


@pytest.mark.asyncio
async def test_plan_subset_ambiguous_variable_raises() -> None:
    """An ambiguous bare leaf name raises outside the fail-soft path — it is
    never swallowed by plan_subset's CMR-error handling."""
    cmr = _PlanCmrStub(
        granules=[_granule_with_opendap_url(OPENDAP_URL)], variables=_AMBIGUOUS_VARS
    )
    with pytest.raises(ValueError) as exc_info:
        await plan_subset(cmr, _grid_caps(), "C1-GES_DISC", _PLAN_BBOX, "", ("ndvi",))
    msg = str(exc_info.value)
    assert "/a/ndvi" in msg and "/b/ndvi" in msg


@pytest.mark.asyncio
async def test_plan_subset_resolves_time_coordinate_for_swath_collection() -> None:
    """TROPOMI-shaped case: a swath collection whose science variable has a
    real per-scanline time coordinate must have that coordinate carried on the
    plan (and then projected by OPeNDAPProvider) — otherwise the resulting
    netCDF's ``time`` dimension has no coordinate values and degrades to a
    plain integer index, breaking downstream ``resample``."""
    cmr = _PlanCmrStub(
        granules=[_granule_with_opendap_url(OPENDAP_URL)],
        variables=_SWATH_VARS_WITH_TIME,
    )
    plan = await plan_subset(
        cmr,
        _swath_caps(),
        "C1-TROPOMI",
        None,
        "",
        ("nitrogendioxide_tropospheric_column",),
    )
    assert plan.coord_time == "/PRODUCT/time"


@pytest.mark.asyncio
async def test_plan_subset_no_granules_skips_resolution() -> None:
    """No OPeNDAP granule found -> defaults, and get_variables is never called
    (fail-soft, but also no unnecessary CMR round-trip)."""
    cmr = _PlanCmrStub(granules=[])
    plan = await plan_subset(cmr, _grid_caps(), "C1-GES_DISC", _PLAN_BBOX, "", ("precipitation",))
    assert plan.opendap_urls == []
    assert plan.coord_lat == "lat"
    assert plan.coord_lon == "lon"
    assert plan.lat_axis is None and plan.lon_axis is None
    assert plan.var_dims == {}
    assert plan.variables == ("precipitation",)
    assert cmr.get_variables_called is False


@pytest.mark.asyncio
async def test_plan_subset_cmr_error_falls_back_gracefully() -> None:
    """A CMR/network error resolving variables still yields the discovered
    granule URLs, with coordinate names and variables at their fail-soft
    defaults — never raises."""
    cmr = _PlanCmrStub(
        granules=[_granule_with_opendap_url(OPENDAP_URL)],
        variables=RuntimeError("network error"),
    )
    plan = await plan_subset(cmr, _grid_caps(), "C1-GES_DISC", _PLAN_BBOX, "", ("precipitation",))
    assert plan.opendap_urls == [OPENDAP_URL]
    assert plan.coord_lat == "lat"
    assert plan.coord_lon == "lon"
    assert plan.variables == ("precipitation",)


@pytest.mark.asyncio
async def test_plan_subset_discovers_grid_geometry_for_grid_shape() -> None:
    cmr = _PlanCmrStub(
        granules=[_granule_with_opendap_url(OPENDAP_URL)], variables=_GRID_GEOMETRY_VARS
    )
    caps = _grid_caps()
    caps.spatial_extent = _GRID_EXTENT
    plan = await plan_subset(
        cmr, caps, "C1-GES_DISC", _PLAN_BBOX, "", ("vertical_column_total",)
    )
    assert plan.lat_axis is not None
    assert plan.lon_axis is not None
    assert plan.var_dims


@pytest.mark.asyncio
async def test_plan_subset_swath_shape_never_discovers_grid_geometry() -> None:
    """A swath collection never gets axis geometry, even with granule URLs and
    resolvable variables — a 1D index hyperslab cannot express a bbox on a
    swath's curvilinear geolocation."""
    cmr = _PlanCmrStub(
        granules=[_granule_with_opendap_url(OPENDAP_URL)], variables=_GRID_GEOMETRY_VARS
    )
    caps = _swath_caps()
    caps.spatial_extent = _GRID_EXTENT
    plan = await plan_subset(
        cmr, caps, "C1-GES_DISC", _PLAN_BBOX, "", ("vertical_column_total",)
    )
    assert plan.lat_axis is None
    assert plan.lon_axis is None
    assert plan.var_dims == {}
