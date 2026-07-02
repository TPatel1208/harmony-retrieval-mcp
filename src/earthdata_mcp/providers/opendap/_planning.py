"""CMR-facing OPeNDAP planning — the public :func:`plan_subset` entry point
and its discovery/resolution helpers (PLAN.md §4.2 step 3).

Everything here talks to CMR (granule search, UMM-V lookup) to resolve what
:mod:`earthdata_mcp.providers.opendap._serialization`'s pure builder needs:
granule OPeNDAP URLs, coordinate variable names, and (for a ``"grid"``
collection) regular-grid axis geometry. Every CMR call is fail-soft — a
network or lookup error degrades to the untouched inputs rather than raising,
mirroring the no-op behaviour this planning seam replaces.
"""

from __future__ import annotations

from dataclasses import dataclass

from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.opendap._serialization import AxisGeometry, VarDimPlan, _leaf


async def _resolve_from_cmr(
    cmr: object,
    concept_id: str,
    variables: tuple[str, ...],
) -> tuple[str, str, str | None, tuple[str, ...]]:
    """Resolve variable names and discover coordinate names from CMR UMM-V.

    Makes exactly one ``get_variables()`` call for the collection. Returns
    ``(lat_name, lon_name, time_name, resolved_variables)`` where each
    resolved path is the full CMR UMM-V ``Name`` value for that variable.
    ``time_name`` is ``None`` when the collection has no UMM-V variable whose
    standard_name/leaf name marks it as time — many L3 products encode time in
    the filename instead of a variable, and there's nothing to project there.

    Variable matching:
    * Already a full path (starts with ``/``) → used as-is, no lookup.
    * Bare name → case-insensitive match against the last segment of each CMR
      variable's ``Name``.  Zero matches → pass through unchanged.
      Exactly one match → substitute the full CMR path.
      More than one match → raise ``ValueError`` naming all conflicting paths.

    Falls back to ``("lat", "lon", None, variables_as_passed)`` on any network
    or CMR error. The ambiguity ``ValueError`` is raised *after* the try block
    so it is never swallowed by the network-error handler.
    """
    try:
        cmr_vars = await cmr.get_variables(concept_id)  # type: ignore[attr-defined]
    except Exception:
        return ("lat", "lon", None, variables)

    # Discover coordinate variable names from CMR UMM-V standard_name or
    # canonical leaf name.
    lat_name = "lat"
    lon_name = "lon"
    time_name: str | None = None
    for var in cmr_vars:
        name: str = var.get("name") or ""
        std = (var.get("standard_name") or "").lower()
        leaf = name.rsplit("/", 1)[-1].lower()
        if std == "latitude" or leaf in ("lat", "latitude"):
            lat_name = name
        elif std == "longitude" or leaf in ("lon", "longitude"):
            lon_name = name
        elif std == "time" or leaf == "time":
            time_name = name

    # Resolve data variable names: exact full paths pass through; bare names
    # are matched against CMR leaf segments (case-insensitive).
    resolved: list[str] = []
    for user_var in variables:
        if user_var.startswith("/"):
            resolved.append(user_var)
            continue
        leaf_lower = user_var.lower()
        matches = [
            v["name"]
            for v in cmr_vars
            if (v.get("name") or "").rsplit("/", 1)[-1].lower() == leaf_lower
        ]
        if len(matches) == 1:
            resolved.append(matches[0])
        elif not matches:
            resolved.append(user_var)
        else:
            raise ValueError(
                f"Variable {user_var!r} is ambiguous — it appears at multiple "
                f"paths: {', '.join(matches)}"
            )

    return (lat_name, lon_name, time_name, tuple(resolved))


def _find_cmr_var(cmr_vars: list[dict], name: str) -> dict | None:
    leaf = _leaf(name)
    for var in cmr_vars:
        if _leaf(str(var.get("name") or "")) == leaf:
            return var
    return None


def _dimension_length(cmr_vars: list[dict], coord_name: str) -> int | None:
    """The size of ``coord_name``'s own (self-referencing) UMM-V dimension."""
    var = _find_cmr_var(cmr_vars, coord_name)
    if var is None:
        return None
    leaf = _leaf(coord_name)
    for dim in var.get("dimensions") or []:
        if _leaf(str(dim.get("name") or "")) == leaf:
            size = dim.get("size")
            return int(size) if size else None
    return None


def _variable_dim_plan(
    cmr_vars: list[dict], var_name: str, lat_leaf: str, lon_leaf: str
) -> VarDimPlan | None:
    """``var_name``'s own UMM-V dimensions, in order, as a :data:`VarDimPlan`.

    DAP4 hyperslabs are positional — a variable's *every* dimension needs a
    bracket, in the variable's own order, or Hyrax rejects the request outright
    (confirmed against production Cloud OPeNDAP: a 2-bracket CE on a 3-D
    variable is a 400, not a silently-misapplied slice). ``None`` when the
    variable's dimensions are unknown, don't include both a lat and a lon
    dimension, or include a non-spatial dimension whose size UMM-V doesn't
    report — any of which makes a safe per-dimension bracket list impossible,
    so the caller falls back to a whole-array projection for just this variable.
    """
    var = _find_cmr_var(cmr_vars, var_name)
    if var is None:
        return None
    dims = var.get("dimensions") or []
    if not dims:
        return None
    leaves = [_leaf(str(d.get("name") or "")) for d in dims]
    if lat_leaf not in leaves or lon_leaf not in leaves:
        return None
    plan: list[tuple[str, int | None]] = []
    for d, leaf in zip(dims, leaves):
        if leaf in (lat_leaf, lon_leaf):
            plan.append((leaf, None))
            continue
        size = d.get("size")
        if not size:
            return None
        plan.append((leaf, int(size)))
    return tuple(plan)


async def _discover_grid_geometry(
    cmr: object,
    concept_id: str,
    spatial_extent: tuple[float, float, float, float] | None,
    variables: tuple[str, ...],
    coord_lat: str,
    coord_lon: str,
) -> tuple[AxisGeometry | None, AxisGeometry | None, dict[str, VarDimPlan]]:
    """Derive regular lat/lon axis geometry from UMM-C extent + UMM-V dimensions.

    Mirrors :func:`_resolve_from_cmr`'s fail-soft posture and reuses the same
    ``get_variables()`` call — one extra CMR round trip, made in the same
    planning pass that already resolves coordinate names. Any error, missing
    extent, or unresolvable axis length yields ``(None, None, {})`` — the
    caller's whole-array fallback. Never raises into the retrieval path.

    Each requested variable is independently resolved to a :data:`VarDimPlan`
    (see :func:`_variable_dim_plan`); a variable that fails resolution is
    simply absent from the returned map (whole-array for that one variable),
    it does not disable geometry for the rest of the request.

    UMM-C's bounding coordinates describe the grid's outer *edges*, not the
    first cell's coordinate value — confirmed against production Cloud OPeNDAP
    (GLDAS's published west/south edge is exactly a half-cell short of its
    real ``lon[0]``/``lat[0]``). So each axis's ``step`` is the extent span
    divided by ``length`` (not ``length - 1``), and ``origin`` (a *value*, not
    an edge) is the edge offset by half a step.
    """
    if spatial_extent is None:
        return (None, None, {})
    try:
        cmr_vars = await cmr.get_variables(concept_id)  # type: ignore[attr-defined]
    except Exception:
        return (None, None, {})

    lat_len = _dimension_length(cmr_vars, coord_lat)
    lon_len = _dimension_length(cmr_vars, coord_lon)
    if not lat_len or not lon_len:
        return (None, None, {})

    west, south, east, north = spatial_extent
    lat_step = (north - south) / lat_len
    lon_step = (east - west) / lon_len
    if lat_step == 0 or lon_step == 0:
        return (None, None, {})

    lat_axis = AxisGeometry(
        name=coord_lat, origin=south + lat_step / 2, step=lat_step, length=lat_len
    )
    lon_axis = AxisGeometry(
        name=coord_lon, origin=west + lon_step / 2, step=lon_step, length=lon_len
    )

    lat_leaf, lon_leaf = _leaf(coord_lat), _leaf(coord_lon)
    var_dims: dict[str, VarDimPlan] = {}
    for v in variables:
        plan = _variable_dim_plan(cmr_vars, v, lat_leaf, lon_leaf)
        if plan is not None:
            var_dims[v] = plan

    return (lat_axis, lon_axis, var_dims)


#: Cap on granules pulled into a single OPeNDAP bundle (one DAP4 bbox subset each).
#: Matches CMR's per-request granule limit; a wider window is truncated to this.
_OPENDAP_GRANULE_LIMIT = 50


@dataclass(frozen=True)
class OpendapPlan:
    """The OPeNDAP planning outputs :func:`plan_subset` resolves for one request.

    Carries exactly what ``RequestSpec.from_plan`` needs (PLAN.md §4.3):
    the window's granule OPeNDAP URLs, the resolved coordinate names, the
    optional regular-grid axis geometry, the per-variable dimension plans, and
    the resolved variable names (bare leaf names substituted with their full
    UMM-V group path where CMR resolves them unambiguously). ``opendap_urls``
    empty means no OPeNDAP endpoint was found for this window — every other
    field is then the untouched default (fail-soft, never raises).

    ``coord_time`` is ``None`` unless UMM-V resolves a real time coordinate
    variable for the collection — most L3 products encode time in the
    filename instead, and there's nothing to project there.
    """

    opendap_urls: list[str]
    coord_lat: str
    coord_lon: str
    coord_time: str | None
    lat_axis: AxisGeometry | None
    lon_axis: AxisGeometry | None
    var_dims: dict[str, VarDimPlan]
    variables: tuple[str, ...]


async def plan_subset(
    cmr: object,
    caps: CollectionCapabilities,
    concept_id: str,
    bbox: tuple[float, float, float, float] | None,
    time_range: str,
    variables: tuple[str, ...],
) -> OpendapPlan:
    """The public OPeNDAP planning entry point (PLAN.md §4.2 step 3).

    Composes granule-URL discovery, coordinate/variable-name resolution
    (:func:`_resolve_from_cmr`), and grid-geometry discovery
    (:func:`_discover_grid_geometry`) — everything a caller previously had to
    reach into this module's private helpers to assemble. When no granule in
    the window advertises an OPeNDAP URL, resolution is skipped entirely and
    the plan carries the untouched inputs (fail-soft, mirrors the no-op this
    replaces). Grid-geometry discovery only runs for a ``"grid"`` collection —
    a swath's curvilinear geolocation cannot use a 1D index hyperslab.
    """
    urls = await _discover_opendap_urls(cmr, concept_id, bbox, time_range)
    if not urls:
        return OpendapPlan(
            opendap_urls=[],
            coord_lat="lat",
            coord_lon="lon",
            coord_time=None,
            lat_axis=None,
            lon_axis=None,
            var_dims={},
            variables=tuple(variables),
        )

    coord_lat, coord_lon, coord_time, resolved_variables = await _resolve_from_cmr(
        cmr, concept_id, variables
    )
    lat_axis: AxisGeometry | None = None
    lon_axis: AxisGeometry | None = None
    var_dims: dict[str, VarDimPlan] = {}
    if caps.output_shape == "grid":
        lat_axis, lon_axis, var_dims = await _discover_grid_geometry(
            cmr, concept_id, caps.spatial_extent, resolved_variables, coord_lat, coord_lon
        )
    return OpendapPlan(
        opendap_urls=urls,
        coord_lat=coord_lat,
        coord_lon=coord_lon,
        coord_time=coord_time,
        lat_axis=lat_axis,
        lon_axis=lon_axis,
        var_dims=var_dims,
        variables=resolved_variables,
    )


async def _discover_opendap_urls(
    cmr: object,
    concept_id: str,
    bbox: tuple[float, float, float, float] | None,
    time_range: str,
) -> list[str]:
    """Search the window's granules and collect each one's OPeNDAP access URL.

    Over every granule in the AOI+time window (up to
    :data:`_OPENDAP_GRANULE_LIMIT`) so a multi-day request covers the whole
    span. Returns ``[]`` if no granules are found or none advertise an
    OPeNDAP URL.
    """
    bbox_str = ",".join(str(c) for c in bbox) if bbox is not None else None
    granules = await cmr.search_granules(  # type: ignore[attr-defined]
        concept_id,
        bounding_box=bbox_str,
        temporal=time_range or None,
        limit=_OPENDAP_GRANULE_LIMIT,
    )
    urls: list[str] = []
    for granule in granules:
        url = _opendap_url_of(granule)
        if url:
            urls.append(url)
    return urls


def _opendap_url_of(granule: dict) -> str | None:
    """Extract a granule's OPeNDAP access URL (sans trailing ``.html``) or ``None``."""
    for entry in granule.get("related_urls", []):
        url = str(entry.get("URL", ""))
        subtype = str(entry.get("Subtype", "")).upper()
        if "OPENDAP" in subtype or "opendap" in url.lower():
            return url[: -len(".html")] if url.endswith(".html") else url
    return None
