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

import difflib
from dataclasses import dataclass
from xml.etree import ElementTree as ET

from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.opendap._dmr import group_geolocation, parse_dmr, resolve_variables
from earthdata_mcp.providers.opendap._serialization import AxisGeometry, VarDimPlan, _leaf


async def _resolve_from_cmr(
    cmr: object,
    concept_id: str,
    variables: tuple[str, ...],
    *,
    cmr_vars: list[dict] | None = None,
) -> tuple[str | None, str | None, str | None, tuple[str, ...]]:
    """Resolve variable names and discover coordinate names from CMR UMM-V.

    Makes exactly one ``get_variables()`` call for the collection, unless the
    caller already fetched it (``cmr_vars`` given — :func:`plan_subset` shares
    its one fetch with :func:`_discover_grid_geometry` and plan-time variable
    validation this way). Returns ``(lat_name, lon_name, time_name,
    resolved_variables)`` where each resolved path is the full CMR UMM-V
    ``Name`` value for that variable. ``time_name`` is ``None`` when the
    collection has no UMM-V variable whose standard_name/leaf name marks it as
    time — many L3 products encode time in the filename instead of a variable,
    and there's nothing to project there.

    Variable matching:
    * Already a full path (starts with ``/``) → used as-is, no lookup.
    * Bare name → case-insensitive match against the last segment of each CMR
      variable's ``Name``.  Zero matches → pass through unchanged.
      Exactly one match → substitute the full CMR path.
      More than one match → raise ``ValueError`` naming all conflicting paths.

    ``lat_name``/``lon_name`` are ``None`` — never a guessed ``"lat"``/``"lon"``
    (PRD 009's never-guess rule) — unless UMM-V actually confirms a coordinate
    variable by standard_name or canonical leaf name. Falls back to
    ``(None, None, None, variables_as_passed)`` on any network or CMR error.
    The ambiguity ``ValueError`` is raised *after* the try block so it is never
    swallowed by the network-error handler.
    """
    if cmr_vars is None:
        try:
            cmr_vars = await cmr.get_variables(concept_id)  # type: ignore[attr-defined]
        except Exception:
            return (None, None, None, variables)

    # Discover coordinate variable names from CMR UMM-V standard_name or
    # canonical leaf name. Stay None (never a guessed default) until UMM-V
    # actually confirms one.
    lat_name: str | None = None
    lon_name: str | None = None
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
    coord_lat: str | None,
    coord_lon: str | None,
    *,
    cmr_vars: list[dict] | None = None,
) -> tuple[AxisGeometry | None, AxisGeometry | None, dict[str, VarDimPlan]]:
    """Derive regular lat/lon axis geometry from UMM-C extent + UMM-V dimensions.

    Mirrors :func:`_resolve_from_cmr`'s fail-soft posture. Reuses the caller's
    already-fetched ``cmr_vars`` when given (:func:`plan_subset` shares its one
    fetch across coordinate resolution, grid geometry, and plan-time variable
    validation); otherwise makes its own ``get_variables()`` call. Any error,
    missing extent, an unconfirmed coordinate name, or unresolvable axis length
    yields ``(None, None, {})`` — the caller's whole-array fallback. Never
    raises into the retrieval path.

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
    if spatial_extent is None or coord_lat is None or coord_lon is None:
        return (None, None, {})
    if cmr_vars is None:
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


async def _resolve_from_dmr(
    cmr: object,
    opendap_url: str,
    variables: tuple[str, ...],
) -> tuple[tuple[str, ...], dict[str, tuple[str, str]], dict[str, tuple[str, ...]]] | None:
    """DMR-backed structure discovery — the fallback when UMM-Var is empty (PRD 009).

    Fetches the first window granule's DMR and resolves each requested name
    against its real variable tree (:func:`~earthdata_mcp.providers.opendap._dmr.resolve_variables`):
    a bare leaf under multiple parallel sibling groups (ICESat-2's six beams)
    expands to all of them. Each resolved variable's own group-local lat/lon
    is looked up via :func:`~earthdata_mcp.providers.opendap._dmr.group_geolocation`.

    Returns ``None`` — never raises — only when the DMR itself could not be
    fetched or parsed (network hiccup, malformed XML): the caller degrades to
    variables-only planning rather than guessing a coordinate. An unresolvable
    *variable name* is a different failure — the DMR was fetched fine and is
    authoritative ground truth for this granule, so :func:`resolve_variables`'s
    ``ValueError`` propagates uncaught, rejecting the plan before any job or
    handle is created (PRD 009 user story 6).
    """
    try:
        xml_text = await cmr.get_dmr(opendap_url)  # type: ignore[attr-defined]
    except Exception:
        return None
    if not xml_text:
        return None
    try:
        tree = parse_dmr(xml_text)
    except ET.ParseError:
        return None

    resolved, expansions = resolve_variables(tree, variables)
    var_coords: dict[str, tuple[str, str]] = {}
    for var in resolved:
        lat, lon = group_geolocation(tree, var)
        if lat is not None and lon is not None:
            var_coords[var] = (lat, lon)
    return (resolved, var_coords, expansions)


async def _reconcile_variables_with_dmr(
    cmr: object,
    opendap_url: str,
    variables: tuple[str, ...],
) -> tuple[str, ...]:
    """Remap UMM-V group-qualified variable paths onto the granule DMR's real paths.

    UMM-V records a variable's ``Name`` as its archive-native (HDF-EOS5)
    group-qualified path — e.g. ``HDFEOS/GRIDS/ColumnAmountNO2/Data_Fields/
    ColumnAmountNO2Trop``. But a Hyrax endpoint that serves the granule through
    an NcML aggregation (GES DISC's ``.he5.ncml`` L3 products — OMNO2d)
    *flattens* that hierarchy: the variable is exposed at the DMR root under its
    bare leaf (``/ColumnAmountNO2Trop``). Projecting the UMM-V path then draws a
    400 from Hyrax ("the constraint expression referenced a variable that was
    not found in the dataset"), and because UMM-V *is* present the empty-UMM-V
    DMR fallback (:func:`_resolve_from_dmr`) never runs to catch it.

    So we verify — never guess (CLAUDE.md) — each name against the granule's own
    DMR, the one authority for what the endpoint actually accepts. A path
    present verbatim is kept (the endpoint preserved groups — TEMPO). A path the
    DMR lacks is remapped to the DMR path whose leaf matches, but only when
    exactly one does; a leaf with zero or several DMR matches is left untouched
    (the flattened DMR is an endpoint quirk, not authority to *reject* a
    variable UMM-V already confirmed exists — leaving it reproduces today's
    behaviour rather than newly blocking the job). Fail-soft: a DMR that can't
    be fetched or parsed returns the names unchanged.
    """
    try:
        xml_text = await cmr.get_dmr(opendap_url)  # type: ignore[attr-defined]
    except Exception:
        return variables
    if not xml_text:
        return variables
    try:
        tree = parse_dmr(xml_text)
    except ET.ParseError:
        return variables

    reconciled: list[str] = []
    for var in variables:
        fqn = var if var.startswith("/") else f"/{var}"
        if fqn in tree.variables:
            reconciled.append(var)
            continue
        leaf = _leaf(var)
        matches = [p for p in tree.variables if _leaf(p) == leaf]
        reconciled.append(matches[0] if len(matches) == 1 else var)
    return tuple(reconciled)


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
    coord_lat: str | None
    coord_lon: str | None
    coord_time: str | None
    lat_axis: AxisGeometry | None
    lon_axis: AxisGeometry | None
    var_dims: dict[str, VarDimPlan]
    variables: tuple[str, ...]
    #: The raw UMM-V records fetched for this collection (``[]`` when no granule
    #: advertised an OPeNDAP URL, or the fetch failed). Exposed so a caller can
    #: run plan-time variable validation (:func:`validate_variables`) without a
    #: second ``get_variables`` round trip (PRD 002).
    cmr_vars: list[dict]
    #: Each DMR-resolved variable's own group-local ``(lat_path, lon_path)`` —
    #: populated only via the DMR fallback (PRD 009), since a grouped/swath
    #: product with no UMM-Var catalog has no single global coordinate pair.
    #: Empty when UMM-Var resolved (the single ``coord_lat``/``coord_lon`` pair
    #: applies to every variable) or the DMR could not be consulted.
    var_coords: dict[str, tuple[str, str]]
    #: Bare names the DMR expanded to multiple parallel-sibling-group paths
    #: (ICESat-2's six beams), keyed by the original requested name — carried
    #: through to the durable spec/provenance so the expansion is never a
    #: surprise (PRD 009 user story 3).
    variable_expansions: dict[str, tuple[str, ...]]


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

    Fetches ``get_variables()`` exactly once here and passes it into both
    ``_resolve_from_cmr`` and ``_discover_grid_geometry`` (previously two
    separate fetches for a grid collection) — the single fetch this module's
    docstring promises.
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
            cmr_vars=[],
            var_coords={},
            variable_expansions={},
        )

    try:
        cmr_vars = await cmr.get_variables(concept_id)  # type: ignore[attr-defined]
    except Exception:
        cmr_vars = []

    coord_lat, coord_lon, coord_time, resolved_variables = await _resolve_from_cmr(
        cmr, concept_id, variables, cmr_vars=cmr_vars
    )

    # UMM-Var is the first source (one round-trip we already make); the DMR is
    # only consulted when it came back empty — the ATL08 case, where plan-time
    # variable/coordinate resolution would otherwise silently no-op (PRD 009).
    var_coords: dict[str, tuple[str, str]] = {}
    variable_expansions: dict[str, tuple[str, ...]] = {}
    if not cmr_vars:
        dmr_result = await _resolve_from_dmr(cmr, urls[0], resolved_variables)
        if dmr_result is not None:
            resolved_variables, var_coords, variable_expansions = dmr_result
    elif any("/" in v for v in resolved_variables):
        # UMM-V is present, but its variable Names are archive-native paths an
        # NcML-aggregating endpoint flattens (OMNO2d) — verify each against the
        # granule DMR and remap a flattened one to its real root path, so the CE
        # never projects a variable the endpoint 400s on. Only a group-qualified
        # name can be flattened, so a flat collection (GLDAS) skips this round-trip.
        resolved_variables = await _reconcile_variables_with_dmr(
            cmr, urls[0], resolved_variables
        )

    lat_axis: AxisGeometry | None = None
    lon_axis: AxisGeometry | None = None
    var_dims: dict[str, VarDimPlan] = {}
    if caps.output_shape == "grid":
        lat_axis, lon_axis, var_dims = await _discover_grid_geometry(
            cmr,
            concept_id,
            caps.spatial_extent,
            resolved_variables,
            coord_lat,
            coord_lon,
            cmr_vars=cmr_vars,
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
        cmr_vars=cmr_vars,
        var_coords=var_coords,
        variable_expansions=variable_expansions,
    )


def validate_variables(cmr_vars: list[dict], variables: tuple[str, ...]) -> None:
    """Raise ``ValueError`` if any requested variable doesn't resolve against
    ``cmr_vars`` — the same normalization :func:`_resolve_from_cmr` applies
    (full slash-path match, or unambiguous bare-leaf match). A no-op when
    ``cmr_vars`` is empty: an unregistered or unavailable UMM-Var catalog must
    never block retrieval (PRD 002 user story 10).

    Lists up to a handful of closest-matching leaf names (difflib) per unknown
    variable, so a typo (``"vertical_column_totl"``) is correctable without a
    separate ``describe_dataset`` call.
    """
    if not cmr_vars or not variables:
        return
    full_names = [str(v.get("name") or "") for v in cmr_vars if v.get("name")]
    if not full_names:
        return
    full_lower = {n.lower() for n in full_names}
    leaves_lower = {n.rsplit("/", 1)[-1].lower() for n in full_names}

    unknown = [
        v
        for v in variables
        if v.lower() not in full_lower and v.lstrip("/").lower() not in leaves_lower
    ]
    if not unknown:
        return

    all_leaves = sorted({n.rsplit("/", 1)[-1] for n in full_names})
    parts = []
    for v in unknown:
        matches = difflib.get_close_matches(v.rsplit("/", 1)[-1], all_leaves, n=5)
        parts.append(f"{v!r} (closest matches: {matches})" if matches else repr(v))
    raise ValueError(
        f"Unknown variable(s) for this collection: {'; '.join(parts)}. "
        "Variable names are collection-specific and often group-qualified "
        "(e.g. 'product/vertical_column_troposphere') — do not guess from the "
        "variable's common/scientific name. Call describe_dataset on this "
        "dataset handle to get the exact list."
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
