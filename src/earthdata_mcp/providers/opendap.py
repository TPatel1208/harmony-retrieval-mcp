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
import zipfile
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
        subset crosses the wire — the whole point of an OPeNDAP fetch.
        """
        ce = _constraint_expression(plan, coord_lat=self._coord_lat, coord_lon=self._coord_lon)
        url = f"{base_url}{_DAP4_SUFFIX}"
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


def _fqn(name: str) -> str:
    """Return ``name`` as a DAP4 fully-qualified name (leading slash)."""
    return name if name.startswith("/") else f"/{name}"


def _constraint_expression(
    plan: RetrievalPlan,
    *,
    coord_lat: str = "lat",
    coord_lon: str = "lon",
) -> str:
    """Build a DAP4 projection CE from the plan's variables + coordinate variables.

    Every projected name is emitted as a DAP4 fully-qualified name (leading slash):
    ``/var`` for a root variable; ``/<group>/<leaf>`` for a grouped variable.
    This is the canonical form for both flat and grouped netCDF4 collections.

    ``coord_lat`` and ``coord_lon`` are the actual variable names in this
    collection's netCDF files — they vary (``lat``/``lon`` for GLDAS;
    ``/product/latitude``/``/product/longitude`` for TEMPO L2 grouped).

    ``/time`` is intentionally omitted: temporal filtering happens at the CMR
    granule-search level (selecting which files to fetch), not within a file via
    DAP4 CE. Many L3 monthly products have no ``time`` variable at all (time is
    encoded in the filename), so projecting it causes a 400 from Hyrax.
    """
    # No variables requested → no CE; OPeNDAP returns the full file.
    if plan.transform is None or not plan.transform.variables:
        return ""

    projected: list[str] = []
    if plan.needs_bbox:
        projected.extend([coord_lat, coord_lon])
    projected.extend(plan.transform.variables)
    # Preserve order while dropping duplicate coordinate projections.
    seen: set[str] = set()
    unique = [p for p in projected if not (p in seen or seen.add(p))]
    return ";".join(_fqn(v) for v in unique)


async def _resolve_from_cmr(
    cmr: object,
    concept_id: str,
    variables: tuple[str, ...],
) -> tuple[str, str, tuple[str, ...]]:
    """Resolve variable names and discover coordinate names from CMR UMM-V.

    Makes exactly one ``get_variables()`` call for the collection. Returns
    ``(lat_name, lon_name, resolved_variables)`` where each resolved path is
    the full CMR UMM-V ``Name`` value for that variable.

    Variable matching:
    * Already a full path (starts with ``/``) → used as-is, no lookup.
    * Bare name → case-insensitive match against the last segment of each CMR
      variable's ``Name``.  Zero matches → pass through unchanged.
      Exactly one match → substitute the full CMR path.
      More than one match → raise ``ValueError`` naming all conflicting paths.

    Falls back to ``("lat", "lon", variables_as_passed)`` on any network or
    CMR error. The ambiguity ``ValueError`` is raised *after* the try block so
    it is never swallowed by the network-error handler.
    """
    try:
        cmr_vars = await cmr.get_variables(concept_id)  # type: ignore[attr-defined]
    except Exception:
        return ("lat", "lon", variables)

    # Discover coordinate variable names from CMR UMM-V standard_name or
    # canonical leaf name.
    lat_name = "lat"
    lon_name = "lon"
    for var in cmr_vars:
        name: str = var.get("name") or ""
        std = (var.get("standard_name") or "").lower()
        leaf = name.rsplit("/", 1)[-1].lower()
        if std == "latitude" or leaf in ("lat", "latitude"):
            lat_name = name
        elif std == "longitude" or leaf in ("lon", "longitude"):
            lon_name = name

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

    return (lat_name, lon_name, tuple(resolved))


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
