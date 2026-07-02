"""Hyrax-facing OPeNDAP runtime — :class:`OPeNDAPProvider` (PLAN.md §4.2 step 3).

A :class:`~earthdata_mcp.providers.base.RetrievalProvider` for collections served
through an OPeNDAP (Hyrax/DAP4) endpoint — the §4.2 decision-tree path taken for a
**gridded variable/bbox subset** when no single Harmony service satisfies the plan
and the data is not "as-is" direct-fetchable.

Like :class:`~earthdata_mcp.providers.harmony.HarmonyProvider`, the provider is
**bound to one collection** — its :class:`CollectionCapabilities` (for the
output-shape gate) plus the granule's OPeNDAP access URL (discovered from the
granule's RelatedUrls in real use; injected in tests). The only logic we own is the
:class:`RetrievalPlan` → **DAP4 constraint-expression URL** mapping (built by
:func:`~earthdata_mcp.providers.opendap._serialization.build_constraint_expression`)
and persisting the subset through :class:`~earthdata_mcp.storage.backend.StorageBackend`.

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
import zipfile
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
from earthdata_mcp.providers.opendap._serialization import (
    AxisGeometry,
    VarDimPlan,
    build_constraint_expression,
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
        # back to whole-array (see build_constraint_expression).
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
        ce = build_constraint_expression(
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
