"""AppEEARSProvider — point/area sample tasks → Parquet (PLAN.md §4.4, Phase 7.4).

A :class:`~earthdata_mcp.providers.base.RetrievalProvider` for NASA's AppEEARS
(Application for Extracting and Exploring Analysis Ready Samples) point/area sample
service. AppEEARS samples gridded products at coordinates and returns a **tabular
time-series**, so its output flows to **Parquet** — never the gridded Zarr cube
path (forcing a point time-series into a cube is the impedance mismatch PLAN.md §4.4
calls out). The resulting ``obs_`` handle therefore carries
``application/x-parquet``, never ``application/zarr``.

The provider is bound to one collection's :class:`CollectionCapabilities` and honours
the durable submit → poll → materialize seam:

* ``submit`` POSTs a point task (coordinates + product layers + dates) to
  ``<appeears>/task`` and returns the AppEEARS ``task_id`` as the durable
  coordinate. The request body is the re-materializable spec (PLAN.md §4.5).
* ``poll`` maps the AppEEARS task status onto our :class:`JobState`.
* ``materialize`` downloads the task's CSV results bundle, builds a ``pyarrow``
  table, and persists it **as Parquet** through :class:`StorageBackend`.

The AppEEARS API is Bearer-authenticated with the same EDL token Harmony uses.
"""

from __future__ import annotations

import io
import logging

import httpx
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from earthdata_mcp.config import Settings, get_settings
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.base import (
    AOI,
    JobRef,
    JobStatus,
    MaterializedResult,
    RetrievalPlan,
)
from earthdata_mcp.providers.ratelimit import get_limiter
from earthdata_mcp.storage.backend import StorageBackend
from earthdata_mcp.storage.local import LocalFilesystemBackend

logger = logging.getLogger(__name__)

PROVIDER = "appeears"

#: Tabular media type — must match ``tools/_dataio.PARQUET_MEDIA_TYPE`` so the
#: materialized obs_ handle opens through the same in-process I/O route. Defined
#: locally to keep the heavy xarray/zarr import out of the providers layer; the
#: unit test asserts the two stay in sync.
PARQUET_MEDIA_TYPE = "application/x-parquet"

# AppEEARS task status -> our durable JobState (PLAN.md §4.3 state machine).
_STATUS_MAP: dict[str, JobState] = {
    "pending": JobState.SUBMITTED,
    "queued": JobState.SUBMITTED,
    "processing": JobState.RUNNING,
    "done": JobState.READY,
    "error": JobState.FAILED,
    "expired": JobState.EXPIRED,
    "deleted": JobState.CANCELLED,
}


class AppEEARSProvider:
    """A ``RetrievalProvider`` for AppEEARS point/area sample tasks → Parquet."""

    def __init__(
        self,
        capabilities: CollectionCapabilities,
        *,
        storage: StorageBackend | None = None,
        settings: Settings | None = None,
        timeout: float = 60.0,
        on_progress: object | None = None,
    ) -> None:
        self._caps = capabilities
        self._storage = storage
        self._settings = settings or get_settings()
        self._base = self._settings.appeears_url.rstrip("/")
        self._timeout = timeout
        self._on_progress = on_progress

    # -- capability gate --------------------------------------------------

    def can_handle(self, plan: RetrievalPlan) -> bool:
        """True iff the plan is a point/area sample task (PLAN.md §4.4).

        AppEEARS is selected by the *intent* to sample points, not by collection
        shape — many gridded products are sampleable. The router gates on this so
        a point time-series never gets forced through Harmony's gridded cube path.
        """
        return bool(plan.needs_point_sample)

    # -- lifecycle --------------------------------------------------------

    async def submit(self, plan: RetrievalPlan) -> JobRef:
        """POST a point task; return its ``task_id`` as the durable coordinate."""
        if not self.can_handle(plan):
            raise ValueError(
                "AppEEARSProvider only handles point/area sample plans "
                "(needs_point_sample); the router must not dispatch this here"
            )
        body = self._build_task(plan)
        await get_limiter(PROVIDER).acquire()
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                f"{self._base}/task", json=body, headers=self._auth_headers()
            )
            response.raise_for_status()
            payload = response.json()
        task_id = str(payload.get("task_id", ""))
        if not task_id:
            raise RuntimeError(f"AppEEARS task submit returned no task_id: {payload!r}")
        logger.info("AppEEARS point task submitted: task_id=%s", task_id)
        return JobRef(
            provider=PROVIDER,
            provider_job_id=task_id,
            provider_job_url=f"{self._base}/task/{task_id}",
        )

    async def poll(self, job: JobRef) -> JobStatus:
        """One status check against ``<appeears>/task/<id>`` (worker drives the loop)."""
        await get_limiter(PROVIDER).acquire()
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(
                f"{self._base}/task/{job.provider_job_id}",
                headers=self._auth_headers(),
            )
            response.raise_for_status()
            raw = response.json()
        status = self._to_job_status(raw)
        if callable(self._on_progress):
            self._on_progress(status)
        return status

    async def materialize(self, job: JobRef) -> MaterializedResult:
        """Download the task's CSV results and persist them **as Parquet**.

        Point time-series are tabular: we read the bundle's results CSV into a
        ``pyarrow`` table and write Parquet (never Zarr). The obs_ handle this
        resolves to carries ``application/x-parquet`` (PLAN.md §4.4 hard rule).
        """
        await get_limiter(PROVIDER).acquire()
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            headers = self._auth_headers()
            bundle = await client.get(
                f"{self._base}/bundle/{job.provider_job_id}", headers=headers
            )
            bundle.raise_for_status()
            file_id, file_name = _results_file(bundle.json())
            results = await client.get(
                f"{self._base}/bundle/{job.provider_job_id}/{file_id}",
                headers=headers,
            )
            results.raise_for_status()
            csv_bytes = results.content

        parquet = _csv_to_parquet(csv_bytes)
        storage = self._storage_backend()
        handle = job.job_handle or job.provider_job_id or "result"
        key = f"{PROVIDER}/{handle}/series.parquet"
        await storage.put(key, parquet)
        logger.info("AppEEARS results materialized to Parquet: %s (%s)", key, file_name)
        return MaterializedResult(
            storage_key=key,
            media_type=PARQUET_MEDIA_TYPE,
            size_bytes=len(parquet),
            extra={"source_file": file_name},
        )

    # -- request mapping (the only logic we own) --------------------------

    def _build_task(self, plan: RetrievalPlan) -> dict:
        """Map a plan onto an AppEEARS point-task body (coordinates + layers + dates)."""
        lat, lon = _point_from_aoi(plan.aoi)
        product = plan.short_name or plan.concept_id or ""
        layers = [
            {"product": product, "layer": v}
            for v in (plan.transform.variables if plan.transform else ())
        ]
        params: dict[str, object] = {
            "coordinates": [
                {"latitude": lat, "longitude": lon, "id": "0", "category": "site"}
            ],
            "layers": layers,
        }
        if plan.time_range is not None:
            params["dates"] = [
                {
                    "startDate": _appeears_date(plan.time_range.start),
                    "endDate": _appeears_date(plan.time_range.end),
                }
            ]
        return {
            "task_type": "point",
            "task_name": f"earthdata-mcp {product}".strip(),
            "params": params,
        }

    # -- status mapping ---------------------------------------------------

    def _to_job_status(self, raw: dict) -> JobStatus:
        state = _STATUS_MAP.get(str(raw.get("status", "")).lower(), JobState.RUNNING)
        progress = raw.get("progress")
        pct = int(progress.get("summary", 0)) if isinstance(progress, dict) else 0
        error = (
            raw.get("error") or raw.get("message") if state is JobState.FAILED else None
        )
        return JobStatus(
            state=state,
            progress=pct,
            message=raw.get("message"),
            error=error,
        )

    # -- HTTP / storage construction --------------------------------------

    def _auth_headers(self) -> dict[str, str]:
        token = self._settings.earthdata_token or None
        return {"Authorization": f"Bearer {token}"} if token else {}

    def _storage_backend(self) -> StorageBackend:
        if self._storage is None:
            self._storage = LocalFilesystemBackend(self._settings.earthdata_data_dir)
        return self._storage


# -- helpers ---------------------------------------------------------------


def _point_from_aoi(aoi: AOI | None) -> tuple[float, float]:
    """Extract a representative ``(lat, lon)`` from an AOI for a point sample.

    A GeoJSON Point is used directly; a bbox uses its centroid (a degenerate
    point bbox — ``w==e, s==n`` — is the common point-sample case).
    """
    if aoi is None:
        raise ValueError("AppEEARS point task needs an AOI (a point or bbox)")
    if aoi.geojson is not None:
        geom = aoi.geojson.get("geometry", aoi.geojson)
        if geom.get("type") == "Point":
            lon, lat = geom["coordinates"][:2]
            return float(lat), float(lon)
    if aoi.bbox is not None:
        west, south, east, north = aoi.bbox
        return (south + north) / 2.0, (west + east) / 2.0
    raise ValueError("AOI has neither a Point geometry nor a bbox")


def _appeears_date(dt) -> str:
    """AppEEARS dates are ``MM-DD-YYYY``."""
    return dt.strftime("%m-%d-%Y")


def _results_file(bundle: dict) -> tuple[str, str]:
    """Pick the point-results CSV from a bundle listing → ``(file_id, file_name)``."""
    files = bundle.get("files", [])
    csvs = [f for f in files if str(f.get("file_type", "")).lower() == "csv"]
    # Prefer the per-sample results CSV over the granule-list / stats CSVs.
    preferred = [f for f in csvs if "results" in str(f.get("file_name", "")).lower()]
    chosen = preferred or csvs
    if not chosen:
        raise RuntimeError(f"AppEEARS bundle has no CSV results file: {bundle!r}")
    f = chosen[0]
    return str(f.get("file_id", "")), str(f.get("file_name", ""))


def _csv_to_parquet(csv_bytes: bytes) -> bytes:
    """Read AppEEARS results CSV into a table and serialize it as Parquet."""
    table = pacsv.read_csv(io.BytesIO(csv_bytes))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()
