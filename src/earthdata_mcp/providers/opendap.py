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
``submit`` builds the constraint URL — the durable, re-materializable coordinate —
and stores it on ``JobRef.provider_job_url``; ``poll`` reports ``READY``
immediately; ``materialize`` reads the URL **back off the JobRef** (never rebuilds
it) and fetches the bytes. The constraint URL is the *spec*, not an ephemeral
staged-output URL (PLAN.md §4.5): re-running the same GET re-materialises the
subset.

Gridded output stays netCDF here, exactly as ``HarmonyProvider.materialize`` leaves
its result — the format-by-shape Zarr conversion is the Phase 6 worker's job, not
the provider's.
"""

from __future__ import annotations

import logging
from urllib.parse import quote, urlsplit

import httpx

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
_GRIDDED_SHAPES = frozenset({"grid", "swath"})


class OPeNDAPProvider:
    """A ``RetrievalProvider`` that subsets one collection's granules over DAP4."""

    def __init__(
        self,
        capabilities: CollectionCapabilities,
        *,
        opendap_url: str | None = None,
        coord_lat: str = "lat",
        coord_lon: str = "lon",
        storage: StorageBackend | None = None,
        settings: Settings | None = None,
        timeout: float = 300.0,
    ) -> None:
        self._caps = capabilities
        # The granule's OPeNDAP base URL (sans the .dap.nc4 suffix). In production
        # this comes from the granule RelatedUrls; injected directly in tests.
        self._opendap_url = opendap_url.rstrip("/") if opendap_url else None
        # Coordinate variable names vary by collection (GLDAS uses "lat"/"lon";
        # TEMPO uses "latitude"/"longitude"). Discovered from CMR UMM-V at planning
        # time and injected here so the CE uses the correct names.
        self._coord_lat = coord_lat
        self._coord_lon = coord_lon
        self._storage = storage
        self._settings = settings or get_settings()
        self._timeout = timeout

    # -- capability gate --------------------------------------------------

    def can_handle(self, plan: RetrievalPlan) -> bool:
        """True for a gridded variable/bbox/temporal subset to netCDF over OPeNDAP.

        False for: a point/area sample (AppEEARS owns that), a non-gridded
        collection, a "data as-is" plan (no subset → direct fetch), a non-netCDF
        output (png belongs to a Harmony imagenator), or a collection with no
        OPeNDAP endpoint bound.
        """
        if plan.needs_point_sample:
            return False
        if self._opendap_url is None:
            return False
        if self._caps.output_shape not in _GRIDDED_SHAPES:
            return False
        if not (plan.needs_variable or plan.needs_bbox or plan.needs_temporal):
            return False
        return _is_netcdf(plan.output_format)

    # -- lifecycle --------------------------------------------------------

    async def submit(self, plan: RetrievalPlan) -> JobRef:
        """Build the DAP4 constraint URL and carry it on the JobRef.

        The router gates this, but we re-check: a provider must never build a
        request it cannot satisfy. The constraint URL is the durable coordinate —
        it flows through ``provider_job_url`` so ``materialize`` reads it back
        rather than recomputing it.
        """
        if not self.can_handle(plan):
            raise ValueError(
                "OPeNDAPProvider cannot handle this plan — the router must not "
                "dispatch it here (PLAN.md §4.2, no fallback)"
            )
        url = self._build_dap4_url(plan)
        logger.info("OPeNDAP subset request built: %s", url)
        return JobRef(provider=PROVIDER, provider_job_url=url)

    async def poll(self, job: JobRef) -> JobStatus:
        """OPeNDAP is synchronous — the subset is ready as soon as it is built."""
        return JobStatus(state=JobState.READY, progress=100)

    async def materialize(self, job: JobRef) -> MaterializedResult:
        """Fetch the DAP4 subset off the JobRef's URL and persist it.

        Reads ``provider_job_url`` straight off the :class:`JobRef` (the durable
        coordinate ``submit`` stored) — it never rebuilds the constraint, so a
        resumed job materialises from exactly what was persisted.
        """
        url = job.provider_job_url
        if not url:
            raise ValueError(
                "OPeNDAP JobRef carries no constraint URL — submit must store it on "
                "provider_job_url (the durable coordinate, PLAN.md §4.5)"
            )
        data = await self._fetch(url)
        storage = self._storage_backend()
        handle = job.job_handle or "result"
        name = _filename_from_url(url)
        key = f"{PROVIDER}/{handle}/{name}"
        await storage.put(key, data)
        return MaterializedResult(
            storage_key=key, media_type=_NETCDF_MEDIA_TYPE, size_bytes=len(data)
        )

    # -- DAP4 request mapping (the only logic we own) ---------------------

    def _build_dap4_url(self, plan: RetrievalPlan) -> str:
        """Map a plan onto ``<granule>.dap.nc4?dap4.ce=<projection>``.

        The constraint expression projects the requested variables (and the
        spatial/temporal coordinate variables they need), so only the requested
        subset crosses the wire — the whole point of an OPeNDAP fetch.
        """
        ce = _constraint_expression(plan, coord_lat=self._coord_lat, coord_lon=self._coord_lon)
        url = f"{self._opendap_url}{_DAP4_SUFFIX}"
        if ce:
            url = f"{url}?dap4.ce={quote(ce, safe='')}"
        return url

    # -- HTTP / storage construction --------------------------------------

    async def _fetch(self, url: str) -> bytes:
        """GET the DAP4 subset bytes (Bearer-authenticated when a token is set)."""
        await get_limiter(PROVIDER).acquire()
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(
                url, headers=self._auth_headers(), follow_redirects=True
            )
            response.raise_for_status()
            return response.content

    def _auth_headers(self) -> dict[str, str]:
        token = self._settings.earthdata_token or None
        return {"Authorization": f"Bearer {token}"} if token else {}

    def _storage_backend(self) -> StorageBackend:
        if self._storage is None:
            self._storage = LocalFilesystemBackend(self._settings.earthdata_data_dir)
        return self._storage


# -- helpers ---------------------------------------------------------------


def _constraint_expression(
    plan: RetrievalPlan,
    *,
    coord_lat: str = "lat",
    coord_lon: str = "lon",
) -> str:
    """Build a DAP4 projection CE from the plan's variables + coordinate variables.

    ``coord_lat`` and ``coord_lon`` are the actual variable names in this
    collection's netCDF files — they vary (``lat``/``lon`` for GLDAS;
    ``latitude``/``longitude`` for TEMPO L3; ``Latitude``/``Longitude`` for
    GES DISC monthly products).

    ``/time`` is intentionally omitted: temporal filtering happens at the CMR
    granule-search level (selecting which files to fetch), not within a file via
    DAP4 CE. Many L3 monthly products have no ``time`` variable at all (time is
    encoded in the filename), so projecting it causes a 400 from Hyrax.
    """
    projected: list[str] = []
    if plan.needs_bbox:
        projected.extend([f"/{coord_lat}", f"/{coord_lon}"])
    if plan.transform is not None:
        projected.extend(f"/{v}" for v in plan.transform.variables)
    # Preserve order while dropping duplicate coordinate projections.
    seen: set[str] = set()
    unique = [p for p in projected if not (p in seen or seen.add(p))]
    return ";".join(unique)


def _is_netcdf(output_format: str) -> bool:
    """True for any netCDF media type (the only shape OPeNDAP materialises here)."""
    return "netcdf" in output_format.lower()


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
