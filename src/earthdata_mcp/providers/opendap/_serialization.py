"""Pure DAP4 serialization core — no network, no CMR, no filesystem I/O.

Everything here is a pure function of already-resolved inputs (variable paths,
axis geometry, bbox): fully-qualified names, hyperslab index-range math, and
the constraint-expression (CE) builder that composes them. This is the module
the collection-archetype corpus in ``tests/unit/test_providers/test_opendap.py``
exercises directly — see ``docs/opendap_quirk_ledger.md`` for the reverse-
engineered Hyrax quirk each corpus row pins.

:func:`build_constraint_expression` is this core's one public, promoted name;
everything else here stays private to the ``opendap`` package.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from earthdata_mcp.providers.base import RetrievalPlan


@dataclass(frozen=True)
class AxisGeometry:
    """A regular 1D coordinate axis, described without fetching its values.

    ``origin`` is the axis's first value (index 0); ``step`` is the signed
    spacing between consecutive cells (negative for a descending axis, e.g.
    north-to-south latitude); ``length`` is the axis's cell count. This is the
    minimal data needed to map a degree value to a cell index.
    """

    name: str
    origin: float
    step: float
    length: int


#: One data variable's own dimensions, in order, as ``(leaf_name, size)`` pairs.
#: ``size`` is ``None`` for the two spatial (lat/lon) dims — their range comes
#: from the axis, not a stored size — and the dimension's true size for
#: anything else (e.g. a per-granule ``time`` dimension of length 1).
VarDimPlan = tuple[tuple[str, "int | None"], ...]


def _fqn(name: str) -> str:
    """Return ``name`` as a DAP4 fully-qualified name (leading slash)."""
    return name if name.startswith("/") else f"/{name}"


def _leaf(name: str) -> str:
    return name.rsplit("/", 1)[-1].lower()


def _index_range(axis: AxisGeometry, lo: float, hi: float) -> tuple[int, int] | None:
    """Map a ``[lo, hi]`` degree window onto an inclusive ``[low, high]`` cell
    index range on ``axis``, clamped to ``[0, length - 1]``.

    Uses floor/ceil rather than round so a box edge that falls between two
    cells still includes the cell it touches (never clipped by an off-by-one).
    ``axis.step``'s sign carries the axis's direction — a descending axis
    (e.g. north-to-south latitude) still yields ``low <= high``. A degenerate
    window (``lo == hi``) always resolves to at least one cell. Returns
    ``None`` for a malformed axis (zero step or non-positive length).
    """
    if axis.step == 0 or axis.length <= 0:
        return None
    i_lo = (lo - axis.origin) / axis.step
    i_hi = (hi - axis.origin) / axis.step
    low_f, high_f = (i_lo, i_hi) if axis.step > 0 else (i_hi, i_lo)
    low = max(0, min(math.floor(low_f), axis.length - 1))
    high = max(0, min(math.ceil(high_f), axis.length - 1))
    return (low, high) if high >= low else (low, low)


def _bracket(index_range: tuple[int, int] | None) -> str:
    return f"[{index_range[0]}:{index_range[1]}]" if index_range is not None else ""


def _fqn_sliced(name: str, *ranges: tuple[int, int] | None) -> str:
    return _fqn(name) + "".join(_bracket(r) for r in ranges)


def _var_bracket_ranges(
    dims: VarDimPlan,
    lat_leaf: str,
    lon_leaf: str,
    lat_range: tuple[int, int] | None,
    lon_range: tuple[int, int] | None,
) -> tuple[tuple[int, int] | None, ...]:
    """One bracket range per dimension, in the variable's own dimension order.

    DAP4 hyperslabs are positional: a variable's *every* dimension needs a
    bracket, in order, or Hyrax rejects the request outright (verified against
    production Cloud OPeNDAP — a 2-bracket CE on a 3-D variable is a 400, not a
    silently-misapplied slice). A non-spatial dimension (e.g. GLDAS's
    per-granule ``time`` dimension, size 1) gets its own full ``(0, size - 1)``
    range so the variable still opens correctly.
    """
    ranges: list[tuple[int, int] | None] = []
    for leaf, size in dims:
        if leaf == lat_leaf:
            ranges.append(lat_range)
        elif leaf == lon_leaf:
            ranges.append(lon_range)
        else:
            ranges.append((0, size - 1) if size else None)
    return tuple(ranges)


def build_constraint_expression(
    plan: RetrievalPlan,
    *,
    coord_lat: str = "lat",
    coord_lon: str = "lon",
    coord_time: str | None = None,
    lat_axis: AxisGeometry | None = None,
    lon_axis: AxisGeometry | None = None,
    var_dims: dict[str, VarDimPlan] | None = None,
) -> str:
    """Build a DAP4 projection CE from the plan's variables + coordinate variables.

    Every projected name is emitted as a DAP4 fully-qualified name (leading slash):
    ``/var`` for a root variable; ``/<group>/<leaf>`` for a grouped variable.
    This is the canonical form for both flat and grouped netCDF4 collections.

    ``coord_lat`` and ``coord_lon`` are the actual variable names in this
    collection's netCDF files — they vary (``lat``/``lon`` for GLDAS;
    ``/product/latitude``/``/product/longitude`` for TEMPO L2 grouped).

    ``lat_axis``/``lon_axis`` are optional regular-grid descriptors. When both
    are given and the plan needs a bbox, the coordinate arrays *and* every
    projected data variable are clipped to inclusive DAP4 index hyperslabs
    covering the requested box — the variable hyperslab is what actually
    shrinks the payload, since projecting a clipped coordinate alone does not.
    A box crossing the antimeridian (west > east) falls back to whole-longitude
    projection rather than emitting a wrong/split slab. When either axis is
    absent (no geometry, or a swath collection the caller never threads
    geometry for), output is identical to a plain whole-array projection.

    ``var_dims`` maps a variable name to its own ordered dimension plan (see
    :data:`VarDimPlan`), discovered alongside the axes. When given, each
    variable is bracketed dimension-by-dimension using its *own* shape — the
    only safe way to hyperslab a variable that carries an extra non-spatial
    dimension (e.g. GLDAS's per-granule ``time``); a variable absent from the
    map gets no bracket at all (whole-array — its shape could not be verified).
    When ``var_dims`` is not given at all, every variable is bracketed as a
    plain ``[lat][lon]`` pair (the caller is asserting it already knows the
    variable is exactly 2-D over these two axes).

    Temporal *filtering* (which granules to fetch) happens at the CMR
    granule-search level, not within a file via DAP4 CE. Many L3 monthly
    products have no ``time`` variable at all (time is encoded in the
    filename) — projecting a nonexistent ``/time`` causes a 400 from Hyrax —
    so ``coord_time`` is only projected when the caller resolved one from
    UMM-V (``None`` by default, matching that L3 case). When a collection
    does carry a real ``time`` coordinate (e.g. TROPOMI's per-scanline
    ``time``), omitting it would still leave a data variable's ``time``
    *dimension* in the response but without coordinate values, degrading it
    to a plain integer index downstream — so it is projected whole-array,
    same as lat/lon when no axis geometry narrows it.
    """
    # No variables requested → no CE; OPeNDAP returns the full file.
    if plan.transform is None or not plan.transform.variables:
        return ""

    lat_range: tuple[int, int] | None = None
    lon_range: tuple[int, int] | None = None
    if (
        plan.needs_bbox
        and lat_axis is not None
        and lon_axis is not None
        and plan.aoi is not None
        and plan.aoi.bbox is not None
    ):
        west, south, east, north = plan.aoi.bbox
        lat_range = _index_range(lat_axis, south, north)
        if west <= east:
            lon_range = _index_range(lon_axis, west, east)
        # else: antimeridian wrap (v1) — leave lon_range None, whole-longitude.

    lat_leaf = _leaf(coord_lat) if lat_axis is not None else ""
    lon_leaf = _leaf(coord_lon) if lon_axis is not None else ""

    projected: list[str] = []
    seen: set[str] = set()

    def _add(name: str, *ranges: tuple[int, int] | None) -> None:
        if name in seen:
            return
        seen.add(name)
        projected.append(_fqn_sliced(name, *ranges))

    if plan.needs_bbox:
        _add(coord_lat, lat_range)
        _add(coord_lon, lon_range)
    if coord_time:
        _add(coord_time)
    for var in plan.transform.variables:
        if not plan.needs_bbox:
            _add(var)
        elif var_dims is not None:
            dims = var_dims.get(var)
            if dims is None:
                _add(var)  # shape unverified — whole-array for this variable only
            else:
                _add(var, *_var_bracket_ranges(dims, lat_leaf, lon_leaf, lat_range, lon_range))
        else:
            _add(var, lat_range, lon_range)
    return ";".join(projected)
