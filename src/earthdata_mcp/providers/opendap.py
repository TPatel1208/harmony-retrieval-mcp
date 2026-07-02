"""OPeNDAPProvider — Hyrax/DAP4 subset for gridded collections (PLAN.md §4.2 step 3).

A :class:`~earthdata_mcp.providers.base.RetrievalProvider` for collections served
through an OPeNDAP (Hyrax/DAP4) endpoint — the §4.2 decision-tree path taken for a
**gridded variable/bbox subset** when no single Harmony service satisfies the plan
and the data is not "as-is" direct-fetchable.

Like :class:`~earthdata_mcp.providers.harmony.HarmonyProvider`, the provider is
**bound to one collection** — its :class:`CollectionCapabilities` (for the
output-shape gate) plus the granule's OPeNDAP access URL (discovered from the
granule's RelatedUrls in real use; injected in tests). The only logic we own is the
:class:`RetrievalPlan` → **DAP4 constraint-expression URL** mapping and persisting
the subset through :class:`~earthdata_mcp.storage.backend.StorageBackend`.

OPeNDAP is a **synchronous** service: there is no provider-side job to poll. We
still honour the durable submit → poll → materialize seam (PLAN.md §4.3 sync path):
``submit`` builds **one constraint URL per granule** in the window — the durable,
re-materializable coordinates — and stores them newline-joined on
``JobRef.provider_job_url``; ``poll`` reports ``READY`` immediately; ``materialize``
reads the URLs **back off the JobRef** (never rebuilds them), fetches each subset, and
**bundles** them into one zip. The constraint URLs are the *spec*, not ephemeral
staged-output URLs (PLAN.md §4.5): re-running the same GETs re-materialises the subset.

Gridded output stays netCDF here. A single-granule request yields a one-member bundle;
a multi-day request yields all the window's granule subsets in one zip, which
``tools/_dataio`` opens and concatenates on the time axis. No Zarr conversion happens
in the provider — the read path understands netCDF directly.
"""

from __future__ import annotations

import asyncio
import io
import logging
import math
import zipfile
from dataclasses import dataclass
from urllib.parse import quote, urlsplit

import httpx
import tenacity

from earthdata_mcp.config import Settings, get_settings
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.base import (
    JobRef,
    JobStatus,
    MaterializedResult,
    RetrievalPlan,
)
from earthdata_mcp.providers.ratelimit import get_limiter
from earthdata_mcp.storage.backend import StorageBackend
from earthdata_mcp.storage.local import LocalFilesystemBackend

logger = logging.getLogger(__name__)

PROVIDER = "opendap"

#: DAP4 binary-subset response suffix Hyrax appends to a granule URL.
_DAP4_SUFFIX = ".dap.nc4"
_NETCDF_MEDIA_TYPE = "application/x-netcdf"
#: Multi-granule result: a zip of netCDF subsets. Must match ``tools/_dataio``.
_NETCDF_BUNDLE_MEDIA_TYPE = "application/netcdf-bundle+zip"
_GRIDDED_SHAPES = frozenset({"grid", "swath"})
#: Separator for the per-granule constraint URLs carried on ``provider_job_url``.
_URL_SEP = "\n"


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


class OPeNDAPProvider:
    """A ``RetrievalProvider`` that subsets one collection's granules over DAP4."""

    def __init__(
        self,
        capabilities: CollectionCapabilities,
        *,
        opendap_urls: list[str] | None = None,
        coord_lat: str = "lat",
        coord_lon: str = "lon",
        coord_time: str | None = None,
        lat_axis: AxisGeometry | None = None,
        lon_axis: AxisGeometry | None = None,
        var_dims: dict[str, VarDimPlan] | None = None,
        storage: StorageBackend | None = None,
        settings: Settings | None = None,
        timeout: float = 300.0,
    ) -> None:
        self._caps = capabilities
        # The window's granule OPeNDAP base URLs (sans the .dap.nc4 suffix). In
        # production these come from the granules' RelatedUrls; injected in tests.
        self._opendap_urls = [u.rstrip("/") for u in (opendap_urls or []) if u]
        # Coordinate variable names vary by collection (GLDAS uses "lat"/"lon";
        # TEMPO uses "latitude"/"longitude"). Discovered from CMR UMM-V at planning
        # time and injected here so the CE uses the correct names.
        self._coord_lat = coord_lat
        self._coord_lon = coord_lon
        # Resolved from UMM-V the same way as coord_lat/coord_lon; None when
        # the collection has no real time variable (time-in-filename L3).
        self._coord_time = coord_time
        # Regular-grid axis geometry, discovered at planning time the same way as
        # coord_lat/coord_lon. Only threaded into the CE for a "grid" collection —
        # a 1D index hyperslab cannot express a bbox on a swath's 2D geolocation.
        self._lat_axis = lat_axis
        self._lon_axis = lon_axis
        # Per-variable dimension plans discovered alongside the axes — lets a
        # variable with a non-spatial dimension (e.g. a per-granule time axis)
        # still be hyperslabbed correctly. A variable absent from the map falls
        # back to whole-array (see _constraint_expression).
        self._var_dims = var_dims
        self._storage = storage
        self._settings = settings or get_settings()
        self._timeout = timeout

    # -- capability gate --------------------------------------------------

    def can_handle(self, plan: RetrievalPlan) -> bool:
        """True for a gridded variable/bbox/temporal subset to netCDF over OPeNDAP.

        False for: a point/area sample (AppEEARS owns that), a non-gridded
        collection, a "data as-is" plan (no subset → direct fetch), a non-netCDF
        output (png belongs to a Harmony imagenator), or a collection with no
        OPeNDAP endpoints bound.
        """
        if plan.needs_point_sample:
            return False
        if not self._opendap_urls:
            return False
        if self._caps.output_shape not in _GRIDDED_SHAPES:
            return False
        if not (plan.needs_variable or plan.needs_bbox or plan.needs_temporal):
            return False
        return _is_netcdf(plan.output_format)

    # -- lifecycle --------------------------------------------------------

    async def submit(self, plan: RetrievalPlan) -> JobRef:
        """Build one DAP4 constraint URL per granule and carry them on the JobRef.

        The router gates this, but we re-check: a provider must never build a
        request it cannot satisfy. The constraint URLs are the durable coordinates —
        they flow newline-joined through ``provider_job_url`` so ``materialize`` reads
        them back rather than recomputing them.
        """
        if not self.can_handle(plan):
            raise ValueError(
                "OPeNDAPProvider cannot handle this plan — the router must not "
                "dispatch it here (PLAN.md §4.2, no fallback)"
            )
        urls = [self._build_dap4_url(plan, base) for base in self._opendap_urls]
        logger.info("OPeNDAP subset request built: %d granule(s)", len(urls))
        return JobRef(provider=PROVIDER, provider_job_url=_URL_SEP.join(urls))

    async def poll(self, job: JobRef) -> JobStatus:
        """OPeNDAP is synchronous — the subset is ready as soon as it is built."""
        return JobStatus(state=JobState.READY, progress=100)

    async def materialize(self, job: JobRef) -> MaterializedResult:
        """Fetch each DAP4 subset off the JobRef and persist them as one zip bundle.

        Reads ``provider_job_url`` straight off the :class:`JobRef` (the durable
        coordinates ``submit`` stored) — it never rebuilds the constraints, so a
        resumed job materialises from exactly what was persisted. The members are
        bundled into a single :class:`StorageBackend` object so one obs handle maps to
        one key, matching how every other provider stores one result; ``tools/_dataio``
        opens the bundle and concatenates the granules on read.
        """
        joined = job.provider_job_url
        if not joined:
            raise ValueError(
                "OPeNDAP JobRef carries no constraint URL — submit must store it on "
                "provider_job_url (the durable coordinate, PLAN.md §4.5)"
            )
        urls = [u for u in joined.split(_URL_SEP) if u]
        # Fetch all granules concurrently; order is preserved by asyncio.gather.
        chunks = await asyncio.gather(*[self._fetch(u) for u in urls])
        buf = io.BytesIO()
        total = 0
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for url, data in zip(urls, chunks):
                total += len(data)
                zf.writestr(_filename_from_url(url), data)
        bundle = buf.getvalue()

        storage = self._storage_backend()
        handle = job.job_handle or "result"
        key = f"{PROVIDER}/{handle}/subset.nc.zip"
        await storage.put(key, bundle)
        return MaterializedResult(
            storage_key=key,
            media_type=_NETCDF_BUNDLE_MEDIA_TYPE,
            size_bytes=len(bundle),
            extra={"granule_count": len(urls), "uncompressed_bytes": total},
        )

    # -- DAP4 request mapping (the only logic we own) ---------------------

    def _build_dap4_url(self, plan: RetrievalPlan, base_url: str) -> str:
        """Map a plan + one granule onto ``<granule>.dap.nc4?dap4.ce=<projection>``.

        The constraint expression projects the requested variables (and the
        spatial/temporal coordinate variables they need), so only the requested
        subset crosses the wire — the whole point of an OPeNDAP fetch. Axis
        geometry is only passed through for a "grid" collection; a swath's
        curvilinear 2D geolocation keeps the whole-array projection.
        """
        is_grid = self._caps.output_shape == "grid"
        ce = _constraint_expression(
            plan,
            coord_lat=self._coord_lat,
            coord_lon=self._coord_lon,
            coord_time=self._coord_time,
            lat_axis=self._lat_axis if is_grid else None,
            lon_axis=self._lon_axis if is_grid else None,
            var_dims=self._var_dims if is_grid else None,
        )
        url = f"{base_url}{_DAP4_SUFFIX}"
        if ce:
            url = f"{url}?dap4.ce={quote(ce, safe='')}"
        return url

    # -- HTTP / storage construction --------------------------------------

    async def _fetch(self, url: str) -> bytes:
        """GET the DAP4 subset bytes (Bearer-authenticated when a token is set).

        Retries up to 3× on transient 5xx / 429 with exponential backoff (2 s,
        4 s, 8 s). A persistent 503 after all retries surfaces a message that
        names the direct-download alternative so the caller is not left guessing.
        """
        await get_limiter(PROVIDER).acquire()

        @tenacity.retry(
            retry=tenacity.retry_if_exception(_is_transient_http_error),
            wait=tenacity.wait_exponential(multiplier=2, min=2, max=30),
            stop=tenacity.stop_after_attempt(4),
            reraise=True,
        )
        async def _do_get() -> bytes:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(
                    url, headers=self._auth_headers(), follow_redirects=True
                )
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code in (502, 503, 504, 429):
                        raise
                    if exc.response.status_code == 503:
                        raise RuntimeError(
                            f"OPeNDAP server returned 503 for {url!r} — the DAP4 "
                            "endpoint for this collection appears persistently "
                            "unavailable. Consider direct HTTPS/S3 download of the "
                            "full granule file and subsetting locally."
                        ) from exc
                    raise
                return response.content

        try:
            return await _do_get()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (502, 503, 504):
                raise RuntimeError(
                    f"OPeNDAP server returned {exc.response.status_code} for "
                    f"{url!r} after retries — the DAP4 endpoint for this collection "
                    "appears persistently unavailable. Consider direct HTTPS/S3 "
                    "download of the full granule file and subsetting locally."
                ) from exc
            raise

    def _auth_headers(self) -> dict[str, str]:
        token = self._settings.earthdata_token or None
        return {"Authorization": f"Bearer {token}"} if token else {}

    def _storage_backend(self) -> StorageBackend:
        if self._storage is None:
            self._storage = LocalFilesystemBackend(self._settings.earthdata_data_dir)
        return self._storage


# -- helpers ---------------------------------------------------------------


def _fqn(name: str) -> str:
    """Return ``name`` as a DAP4 fully-qualified name (leading slash)."""
    return name if name.startswith("/") else f"/{name}"


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


#: One data variable's own dimensions, in order, as ``(leaf_name, size)`` pairs.
#: ``size`` is ``None`` for the two spatial (lat/lon) dims — their range comes
#: from the axis, not a stored size — and the dimension's true size for
#: anything else (e.g. a per-granule ``time`` dimension of length 1).
VarDimPlan = tuple[tuple[str, "int | None"], ...]


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


def _constraint_expression(
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


def _leaf(name: str) -> str:
    return name.rsplit("/", 1)[-1].lower()


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


def _is_netcdf(output_format: str) -> bool:
    """True for any netCDF media type (the only shape OPeNDAP materialises here)."""
    return "netcdf" in output_format.lower()


def _is_transient_http_error(exc: BaseException) -> bool:
    """True for HTTP errors that are worth retrying (5xx gateway errors, 429)."""
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response.status_code in (429, 502, 503, 504)
    )


def _filename_from_url(url: str) -> str:
    """``…/GRANULE.dap.nc4?…`` → ``GRANULE.nc4`` (fallback ``subset.nc4``)."""
    path = urlsplit(url).path
    last = path.rsplit("/", 1)[-1]
    if last.endswith(_DAP4_SUFFIX):
        # The DAP4 response is netCDF-4. The granule name usually already ends in
        # .nc4 (…granule.nc4 + .dap.nc4); only append when it does not.
        stem = last[: -len(_DAP4_SUFFIX)]
        return stem if stem.endswith(".nc4") else stem + ".nc4"
    return last or "subset.nc4"
