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
import logging
import tempfile
import zipfile
from pathlib import Path
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
        coord_lat: str | None = "lat",
        coord_lon: str | None = "lon",
        coord_time: str | None = None,
        lat_axis: AxisGeometry | None = None,
        lon_axis: AxisGeometry | None = None,
        var_dims: dict[str, VarDimPlan] | None = None,
        var_coords: dict[str, tuple[str, str]] | None = None,
        storage: StorageBackend | None = None,
        settings: Settings | None = None,
        timeout: float = 300.0,
        edl_token: str | None = None,
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
        # Per-variable, DMR-resolved group-local geolocation (ATL08's per-beam
        # lat/lon; PRD 009) — projected alongside its variable regardless of
        # collection shape, unlike lat_axis/lon_axis/var_dims which only apply
        # to a "grid" collection's index hyperslab.
        self._var_coords = var_coords
        self._storage = storage
        self._settings = settings or get_settings()
        self._timeout = timeout
        # Per-call identity (PRD 022): a per-instance bearer that wins over the
        # env token in _auth_headers — no process-env mutation, no earthaccess.
        self._edl_token = edl_token

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

    async def cancel(self, job: JobRef) -> None:
        """Documented no-op (PRD 021): OPeNDAP is synchronous — each granule fetch
        completes (or fails) within a single ``materialize`` call, so there is no
        durable upstream job for a provider-side cancel to reach. Present only to
        satisfy the ``RetrievalProvider`` Protocol uniformly."""
        return None

    async def materialize(self, job: JobRef) -> MaterializedResult:
        """Fetch each DAP4 subset off the JobRef and persist them as one zip bundle.

        Reads ``provider_job_url`` straight off the :class:`JobRef` (the durable
        coordinates ``submit`` stored) — it never rebuilds the constraints, so a
        resumed job materialises from exactly what was persisted. The members are
        bundled into a single :class:`StorageBackend` object so one obs handle maps to
        one key, matching how every other provider stores one result; ``tools/_dataio``
        opens the bundle and concatenates the granules on read.

        Every granule streams straight to a scratch file on disk
        (:meth:`_fetch_all`) and the zip is built member-by-member from those
        files — mirrors
        :meth:`~earthdata_mcp.providers.harmony.HarmonyProvider.materialize`
        (fixed by commit 9802d84 after an in-RAM-bundled 50-granule Harmony
        request OOM-killed the process): nothing here ever holds a whole
        granule, let alone the whole bundle, in memory.
        """
        joined = job.provider_job_url
        if not joined:
            raise ValueError(
                "OPeNDAP JobRef carries no constraint URL — submit must store it on "
                "provider_job_url (the durable coordinate, PLAN.md §4.5)"
            )
        urls = [u for u in joined.split(_URL_SEP) if u]

        with tempfile.TemporaryDirectory() as tmp:
            fetched = await self._fetch_all(urls, Path(tmp))

            bundle_path = Path(tmp) / "_bundle.nc.zip"
            total = 0

            def _write_bundle() -> None:
                nonlocal total
                with zipfile.ZipFile(bundle_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    for path, arcname in fetched:
                        total += path.stat().st_size
                        zf.write(path, arcname=arcname)

            # Off the event loop, same as HarmonyProvider.materialize's own
            # disk-based bundle build — ZIP_DEFLATED compression is CPU-bound
            # and would otherwise block every other concurrent task/job in
            # this worker process for its duration.
            await asyncio.to_thread(_write_bundle)

            storage = self._storage_backend()
            handle = job.job_handle or "result"
            key = f"{PROVIDER}/{handle}/subset.nc.zip"
            await storage.put_file(key, bundle_path)
            return MaterializedResult(
                storage_key=key,
                media_type=_NETCDF_BUNDLE_MEDIA_TYPE,
                size_bytes=bundle_path.stat().st_size,
                extra={
                    "granule_count": len(urls),
                    "uncompressed_bytes": total,
                    # PRD 023: which granules this run actually fetched. The
                    # DAP4 constraint is derived from the spec and would only
                    # restate the intent, so it is stripped — what varies
                    # between two runs of the same spec is the granule set.
                    "granule_ids": [_granule_of(u) for u in urls],
                },
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
            var_coords=self._var_coords,
        )
        url = f"{base_url}{_DAP4_SUFFIX}"
        if ce:
            url = f"{url}?dap4.ce={quote(ce, safe='')}"
        return url

    # -- HTTP / storage construction --------------------------------------

    async def _fetch_all(self, urls: list[str], dest_dir: Path) -> list[tuple[Path, str]]:
        """Fetch every URL to a file under ``dest_dir``, concurrently.

        Returns ``(disk_path, arcname)`` pairs, in ``urls`` order. ``disk_path``
        is unique by *position*, never by the granule's own name — two URLs
        whose ``_filename_from_url`` collide (a duplicate/overlapping CMR
        result, or two granules that both fall through to the same fallback
        name) would otherwise both stream into the identical on-disk path at
        once with no coordination, silently corrupting one or both. This is a
        real risk specifically because fetches run *concurrently* here — the
        old code only ever built the zip (via an in-memory ``zf.writestr``
        keyed by name) sequentially, after every fetch had already finished, so
        a name collision there just produced two same-named zip members rather
        than interleaved bytes on disk. ``arcname`` (the plain granule name)
        is what a caller building the zip should use instead, so a genuine
        collision still reproduces that old, merely-odd outcome.

        Bounded by ``retrieval_download_concurrency`` — a semaphore local to
        this call, deliberately not process-wide (cross-job concurrency across
        simultaneous retrievals is a separate, unaddressed question) — and
        fail-fast: the first granule to error cancels every other in-flight
        fetch via :class:`asyncio.TaskGroup`, instead of letting them run to
        completion in the background the way a plain ``asyncio.gather`` would.

        ``TaskGroup`` wraps whatever failed in an ``ExceptionGroup``. Every
        exception in it is logged here (so a second, concurrent failure is
        never silently dropped) and only the first is re-raised bare — matching
        the old sequential ``asyncio.gather``'s "first exception wins" shape, so
        ``materialize_job``'s credential/error classification (which switches on
        the caught exception's own type) keeps working unchanged. A cancellation
        reaching in from *outside* this call (e.g. Arq's job_timeout firing
        mid-fetch) is a pure ``BaseExceptionGroup`` of ``CancelledError`` and
        is intentionally left unwrapped — that shape already reaches
        ``materialize_job``'s ``except BaseException`` handler correctly.
        """
        sem = asyncio.Semaphore(self._settings.retrieval_download_concurrency)
        try:
            async with asyncio.TaskGroup() as tg:
                tasks = [
                    tg.create_task(self._fetch_to_file(url, dest_dir, sem, index))
                    for index, url in enumerate(urls)
                ]
        except* Exception as eg:
            for exc in eg.exceptions:
                logger.error("OPeNDAP granule fetch failed: %s", exc, exc_info=exc)
            raise eg.exceptions[0] from None
        return [t.result() for t in tasks]

    async def _fetch_to_file(
        self, url: str, dest_dir: Path, sem: asyncio.Semaphore, index: int
    ) -> tuple[Path, str]:
        """Stream one DAP4 subset straight to a file under ``dest_dir``; return
        ``(disk_path, arcname)`` — see :meth:`_fetch_all` for why the two names
        differ.

        Retries up to 3× on transient 5xx / 429 with exponential backoff (2 s,
        4 s, 8 s). Both the concurrency semaphore and the HTTP client are
        scoped to a single attempt — acquired/constructed fresh inside the
        retried function, not around the whole retry loop — so a granule
        sitting in backoff sleep releases its concurrency slot for another
        pending granule instead of holding it idle for up to ~14s across
        retries, and every attempt gets an isolated connection/cookie jar
        instead of reusing whatever a failed attempt left behind (relevant
        given EDL's redirect-based SSO can set session cookies). Each attempt
        reopens the destination file in ``"wb"`` (truncating any partial write
        from a prior attempt), so a retry never appends onto a half-written
        file. A persistent 503 after all retries surfaces a message that names
        the direct-download alternative so the caller is not left guessing.
        """
        arcname = _filename_from_url(url)
        dest = dest_dir / f"{index:04d}_{arcname}"
        await get_limiter(PROVIDER).acquire()

        @tenacity.retry(
            retry=tenacity.retry_if_exception(_is_transient_http_error),
            wait=tenacity.wait_exponential(multiplier=2, min=2, max=30),
            stop=tenacity.stop_after_attempt(4),
            reraise=True,
        )
        async def _do_get() -> None:
            async with sem, httpx.AsyncClient(timeout=self._timeout) as client:
                async with client.stream(
                    "GET", url, headers=self._auth_headers(), follow_redirects=True
                ) as response:
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
                    with open(dest, "wb") as f:
                        async for chunk in response.aiter_bytes():
                            f.write(chunk)

        try:
            await _do_get()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (502, 503, 504):
                raise RuntimeError(
                    f"OPeNDAP server returned {exc.response.status_code} for "
                    f"{url!r} after retries — the DAP4 endpoint for this collection "
                    "appears persistently unavailable. Consider direct HTTPS/S3 "
                    "download of the full granule file and subsetting locally."
                ) from exc
            raise
        return dest, arcname

    def _auth_headers(self) -> dict[str, str]:
        token = self._edl_token or self._settings.earthdata_token or None
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


def _granule_of(url: str) -> str:
    """``…/GRANULE.nc4.dap.nc4?dap4.ce=…`` → ``…/GRANULE.nc4`` (PRD 023).

    The delivered-content identifier for one OPeNDAP granule: the granule's own
    endpoint, with this request's DAP4 constraint and the ``.dap.nc4`` response
    suffix removed. CMR granule concept-ids aren't in hand here — the planner
    persists endpoints, not ids — and the endpoint is the provider's own stable
    identifier for the same thing.
    """
    base = url.split("?", 1)[0]
    return base[: -len(_DAP4_SUFFIX)] if base.endswith(_DAP4_SUFFIX) else base


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
