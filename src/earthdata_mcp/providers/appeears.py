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

AppEEARS does NOT accept EDL JWTs. Auth flow: ``POST /login`` with
``Authorization: Basic <base64(edl_username:edl_password)>`` returns a short-lived
session token; all subsequent requests carry ``Authorization: Bearer <session_token>``.
On ``401``/``403``, the provider re-logs in once and retries automatically.
"""

from __future__ import annotations

import base64
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

# "pending" and "queued" both collapse onto JobState.SUBMITTED (our state machine
# has no finer edge); AppEEARS' own distinction between them is preserved here
# instead, since it's the only queue-side signal available before "processing"
# starts reporting a real `progress` object (PRD 007 diagnosis — confirmed
# against the AppEEARS API docs: no `progress` field exists before processing).
_QUEUE_STATUS_MESSAGE: dict[str, str] = {
    "pending": "pending at AppEEARS",
    "queued": "queued at AppEEARS",
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
        edl_token: str | None = None,
    ) -> None:
        self._caps = capabilities
        self._storage = storage
        self._settings = settings or get_settings()
        self._base = self._settings.appeears_url.rstrip("/")
        self._timeout = timeout
        self._on_progress = on_progress
        self._appeears_token: str | None = None
        # Per-call identity (PRD 022): when set, every request carries this
        # bearer directly and the Basic-login flow below is never entered — a
        # rejected per-call credential is a classified failure, never a silent
        # re-login as the service account (identity substitution).
        self._edl_token = edl_token

    # -- capability gate --------------------------------------------------

    def can_handle(self, plan: RetrievalPlan) -> bool:
        """True iff the plan is a point/area sample task (PLAN.md §4.4).

        AppEEARS is selected by the *intent* to sample points, not by collection
        shape — many gridded products are sampleable. The router gates on this so
        a point time-series never gets forced through Harmony's gridded cube path.
        """
        return bool(plan.needs_point_sample)

    # -- lifecycle --------------------------------------------------------

    def product_id(self) -> str:
        """The AppEEARS product identifier: ``short_name`` plus version when present."""
        product = self._caps.short_name
        if self._caps.version:
            product = f"{product}.{self._caps.version}"
        return product

    async def check_product_in_catalog(self) -> None:
        """Raise if this collection's product isn't in the AppEEARS catalog.

        Reuses the same ``GET /product/{id}`` lookup :meth:`submit` performs, so
        a planning-time caller (the shared retrieval planning core, for a
        point-sample plan routed here) can fail fast — before any job row or
        handle is minted — with the identical clear message. ``submit``'s own
        check stays, so a resumed job remains safe even if this planning-time
        call was skipped or the catalog changed since.
        """
        await self._fetch_layer_map(self.product_id())

    async def submit(self, plan: RetrievalPlan) -> JobRef:
        """POST a point task; return its ``task_id`` as the durable coordinate."""
        if not self.can_handle(plan):
            raise ValueError(
                "AppEEARSProvider only handles point/area sample plans "
                "(needs_point_sample); the router must not dispatch this here"
            )
        if not self._edl_token and not (
            self._settings.edl_username and self._settings.edl_password
        ):
            raise ValueError(
                "AppEEARS submission requires EDL credentials; "
                "set EDL_USERNAME and EDL_PASSWORD environment variables"
            )
        product = self.product_id()
        layer_map = await self._fetch_layer_map(product)
        variables = plan.transform.variables if plan.transform else ()
        resolved = self._resolve_layers(product, layer_map, variables)
        body = self._build_task(plan, product, resolved)
        await get_limiter(PROVIDER).acquire()
        response = await self._request("POST", f"{self._base}/task", json=body)
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
        response = await self._request(
            "GET", f"{self._base}/task/{job.provider_job_id}"
        )
        raw = response.json()
        status = self._to_job_status(raw)
        if callable(self._on_progress):
            self._on_progress(status)
        return status

    async def cancel(self, job: JobRef) -> None:
        """Best-effort ``DELETE {base}/task/{task_id}`` (PLAN.md's AppEEARS API).

        A missing task id is a no-op — there is nothing upstream to cancel. A
        404 (already deleted/unknown) or 409 (AppEEARS refuses to delete a
        task in a state it considers final) is exactly the "already-terminal
        or unknown" case the Protocol says must never raise, so both are
        logged and swallowed. Any other HTTP failure propagates — the caller
        (``cancel_retrieval``) treats it as non-fatal but reports it.
        """
        if not job.provider_job_id:
            return
        await get_limiter(PROVIDER).acquire()
        try:
            await self._request("DELETE", f"{self._base}/task/{job.provider_job_id}")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (404, 409):
                logger.info(
                    "AppEEARS cancel: task %s already gone or non-cancellable (%s)",
                    job.provider_job_id,
                    exc.response.status_code,
                )
                return
            raise

    async def materialize(self, job: JobRef) -> MaterializedResult:
        """Download the task's CSV results and persist them **as Parquet**.

        Point time-series are tabular: we read the bundle's results CSV into a
        ``pyarrow`` table and write Parquet (never Zarr). The obs_ handle this
        resolves to carries ``application/x-parquet`` (PLAN.md §4.4 hard rule).
        """
        await get_limiter(PROVIDER).acquire()
        bundle = await self._request(
            "GET", f"{self._base}/bundle/{job.provider_job_id}"
        )
        file_id, file_name = _results_file(bundle.json())
        results = await self._request(
            "GET", f"{self._base}/bundle/{job.provider_job_id}/{file_id}"
        )
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

    def _build_task(self, plan: RetrievalPlan, product: str, layers: list[dict]) -> dict:
        """Map a plan onto an AppEEARS point-task body (coordinates + layers + dates).

        ``layers`` is a pre-resolved list of ``{"product": ..., "layer": ...}`` dicts
        built by ``submit``; ``_build_task`` is pure and owns no I/O.
        """
        lat, lon = _point_from_aoi(plan.aoi)
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

    async def _fetch_layer_map(self, product: str) -> dict:
        """GET /product/{product} → dict of AppEEARS layer identifiers.

        The AppEEARS product endpoint (``/product/{productId}``) returns the layer
        map directly as a JSON object keyed by layer identifier — there is no
        separate ``/layer`` sub-resource. Raises ``RuntimeError`` when AppEEARS
        returns a 404 (product not in catalog) so ``submit`` fails immediately with
        a clear message rather than sending an unknown product to ``POST /task``
        and getting a cryptic 400 back. Other HTTP errors are re-raised as-is.
        """
        try:
            response = await self._request("GET", f"{self._base}/product/{product}")
            return response.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise RuntimeError(
                    f"Dataset {product!r} is not in the AppEEARS product catalog — "
                    "AppEEARS only serves a curated subset of NASA products. "
                    "Use retrieve_subset (Harmony/OPeNDAP path) or direct download instead."
                ) from exc
            raise

    def _resolve_layers(
        self, product: str, layer_map: dict, variables: tuple[str, ...]
    ) -> list[dict]:
        """Translate UMM-V variable names to AppEEARS layer identifiers.

        Normalized comparison: strip a leading underscore, lowercase, spaces → underscores.
        An unmatched name is never sent raw: AppEEARS validates every layer in the
        task body and rejects the *whole* task with an opaque 400 if any one of
        them is invalid, so silently forwarding a broken name only trades a clear
        error here for a cryptic one from the API. Raise immediately instead,
        naming the layers that were actually available.
        """
        norm_to_key = {_normalize_layer(k): k for k in layer_map}
        resolved = []
        for var in variables:
            matched = norm_to_key.get(_normalize_layer(var))
            if matched is None:
                available = ", ".join(sorted(layer_map)) or "(none)"
                raise RuntimeError(
                    f"No AppEEARS layer matches variable {var!r} in product "
                    f"{product!r}; available layers: {available}"
                )
            resolved.append({"product": product, "layer": matched})
        return resolved

    # -- status mapping ---------------------------------------------------

    def _to_job_status(self, raw: dict) -> JobStatus:
        status_key = str(raw.get("status", "")).lower()
        state = _STATUS_MAP.get(status_key, JobState.RUNNING)
        progress = raw.get("progress")
        pct = int(progress.get("summary", 0)) if isinstance(progress, dict) else 0
        message = _current_step_message(progress) if isinstance(progress, dict) else None
        if message is None:
            message = _QUEUE_STATUS_MESSAGE.get(status_key)
        error = (
            raw.get("error") or raw.get("message") if state is JobState.FAILED else None
        )
        return JobStatus(
            state=state,
            progress=pct,
            message=message,
            error=error,
        )

    # -- AppEEARS auth ----------------------------------------------------

    async def _login(self) -> str:
        """POST /login with HTTP Basic (EDL user:pass); cache and return the session token."""
        credentials = base64.b64encode(
            f"{self._settings.edl_username}:{self._settings.edl_password}".encode()
        ).decode()
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                f"{self._base}/login",
                headers={"Authorization": f"Basic {credentials}"},
            )
            response.raise_for_status()
            payload = response.json()
            token = str(payload.get("token", ""))
        if not token:
            raise RuntimeError(f"AppEEARS /login returned no token: {payload!r}")
        self._appeears_token = token
        return token

    async def _ensure_token(self) -> str:
        if self._appeears_token:
            return self._appeears_token
        return await self._login()

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Send an authenticated request; retry once on 401/403 after re-login.

        With a per-call ``edl_token`` (PRD 022), that bearer rides every
        request directly and a 401/403 is raised as a classified
        :class:`~earthdata_mcp.providers.identity.CredentialError` — the
        service-account re-login below must never quietly substitute the
        server's identity for the caller's.

        ``follow_redirects=True`` is required: AppEEARS bundle file endpoints
        return 302 redirects to pre-signed S3/CloudFront URLs for the actual
        CSV download, so the client must follow them automatically.
        """
        per_call = bool(self._edl_token)
        token = self._edl_token if per_call else await self._ensure_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        async with httpx.AsyncClient(
            timeout=self._timeout, follow_redirects=True
        ) as client:
            response = await client.request(method, url, headers=headers, **kwargs)
            if response.status_code in (401, 403):
                if per_call:
                    from earthdata_mcp.providers.identity import (
                        classify_http_response,
                        parse_edl_token,
                    )

                    raise classify_http_response(
                        response.status_code,
                        response.text,
                        identity=parse_edl_token(self._edl_token),
                    )
                self._appeears_token = None
                token = await self._login()
                headers["Authorization"] = f"Bearer {token}"
                response = await client.request(method, url, headers=headers, **kwargs)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                # response.text carries AppEEARS's actual validation message (e.g.
                # a bad date range or layer); a bare raise_for_status() drops it,
                # leaving only "400 Bad Request" for the caller to go on.
                raise httpx.HTTPStatusError(
                    f"{exc}\nResponse body: {response.text}",
                    request=exc.request,
                    response=exc.response,
                ) from exc
        return response

    # -- storage construction ---------------------------------------------

    def _storage_backend(self) -> StorageBackend:
        if self._storage is None:
            self._storage = LocalFilesystemBackend(self._settings.earthdata_data_dir)
        return self._storage


# -- helpers ---------------------------------------------------------------


def _normalize_layer(name: str) -> str:
    """Canonical form for fuzzy-matching UMM-V names against AppEEARS layer keys.

    Rules: strip one leading ``/`` or ``_`` (MOD13Q1's leading-underscore style,
    or a UMM-V group path's leading slash), lowercase, then map both spaces and
    ``/`` to ``_``. UMM-Var group/leaf names, OPeNDAP paths, and AppEEARS layer
    keys are three spellings of the same variable (e.g.
    ``Soil_Moisture_Retrieval_Data_AM/soil_moisture`` vs.
    ``Soil_Moisture_Retrieval_Data_AM_soil_moisture``); applying the same rule to
    both sides collapses all three onto one canonical form.
    """
    return name.lstrip("/_").lower().replace(" ", "_").replace("/", "_")


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


def _current_step_message(progress: dict) -> str | None:
    """The description of the step currently in flight, from a ``processing``
    task's ``progress.details`` array (``[{"step", "desc", "pct_complete"}, ...]``).

    Picks the first step not yet at 100% — the one actually running — falling
    back to the last step's description if every listed step is complete
    (a task about to transition to ``done``). Returns ``None`` for a payload
    with no ``details`` (AppEEARS may report only ``summary``).
    """
    details = progress.get("details")
    if not isinstance(details, list) or not details:
        return None
    for step in details:
        if isinstance(step, dict) and step.get("pct_complete", 100) < 100:
            desc = step.get("desc")
            return str(desc) if desc is not None else None
    desc = details[-1].get("desc") if isinstance(details[-1], dict) else None
    return str(desc) if desc is not None else None


def _appeears_date(dt) -> str:
    """AppEEARS dates are ``MM-DD-YYYY``."""
    return dt.strftime("%m-%d-%Y")


def _results_file(bundle: dict) -> tuple[str, str]:
    """Pick the point-results CSV from a bundle listing → ``(file_id, file_name)``.

    AppEEARS consistently names the data file ``*results*.csv``; the granule-list
    and statistics files use ``*granule-list*`` / ``*statistics*`` names. We require
    "results" in the filename rather than falling back to any CSV, so a bundle that
    contains only a granule-list CSV does not silently produce wrong data.
    """
    files = bundle.get("files", [])
    csvs = [f for f in files if str(f.get("file_type", "")).lower() == "csv"]
    chosen = [f for f in csvs if "results" in str(f.get("file_name", "")).lower()]
    if not chosen:
        file_names = [str(f.get("file_name", "?")) for f in files]
        summary = ", ".join(file_names[:6]) + ("…" if len(file_names) > 6 else "")
        raise RuntimeError(
            f"AppEEARS returned no tabular results — the bundle has {len(files)} "
            f"file(s) ({summary}) but no CSV results file. "
            f"The point/time range may have no valid data (cloud-masked, outside "
            f"the collection's spatial coverage, or no granules in the time window)."
        )
    f = chosen[0]
    return str(f.get("file_id", "")), str(f.get("file_name", ""))


def _csv_to_parquet(csv_bytes: bytes) -> bytes:
    """Read AppEEARS results CSV into a table and serialize it as Parquet."""
    table = pacsv.read_csv(io.BytesIO(csv_bytes))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()
