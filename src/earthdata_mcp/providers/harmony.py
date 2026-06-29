"""HarmonyProvider — wraps the official harmony-py client (CLAUDE.md hard rule).

A :class:`~earthdata_mcp.providers.base.RetrievalProvider`. Our code is *only* the
:class:`RetrievalPlan` → :class:`harmony.Request` mapping, the Harmony-status →
:class:`JobState` mapping, the ``on_progress`` glue, and persisting the result
through :class:`~earthdata_mcp.storage.backend.StorageBackend`. **harmony-py owns
request construction, the EDL session, polling, and Zarr** — we never hand-roll a
Harmony client (CLAUDE.md hard rule).

The provider is **bound to one collection's** :class:`CollectionCapabilities` so
that ``can_handle``/``submit`` can consult ``find_service`` (the Protocol methods
take only a ``plan``). It submits **exactly** the service ``find_service`` returns,
passing its ``service_name`` as harmony-py's ``service_id`` (which accepts a
service chain name) so Harmony invokes the matched service and never the wrong one.

Scope is Phase 4: the submit/poll/materialize *seam* the durable worker (Phase 6)
will drive. Format-by-shape (Zarr/Parquet) selection, the materialization cache
key (§4.4), and worker wiring are deliberately **not** built here.
"""

from __future__ import annotations

import asyncio
import io
import logging
import mimetypes
import re
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from earthdata_mcp.config import Settings, get_settings
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers._capabilities import (
    CollectionCapabilities,
    ServiceCapability,
)
from earthdata_mcp.providers.auth import EDLAuth
from earthdata_mcp.providers.base import (
    JobRef,
    JobStatus,
    MaterializedResult,
    RetrievalPlan,
)
from earthdata_mcp.providers.ratelimit import get_limiter
from earthdata_mcp.storage.backend import StorageBackend
from earthdata_mcp.storage.local import LocalFilesystemBackend

if TYPE_CHECKING:  # pragma: no cover - typing only
    import harmony

logger = logging.getLogger(__name__)

PROVIDER = "harmony"

# mimetypes.guess_type doesn't know NetCDF/HDF5 extensions on Windows or most
# Linux systems without a MIME database — patch the gap so Harmony .nc outputs
# aren't stored as opaque octet-stream blobs that open_result() can't read.
_EXT_MEDIA_TYPES: dict[str, str] = {
    ".nc": "application/netcdf4",
    ".nc4": "application/netcdf4",
    ".h5": "application/netcdf4",
    ".hdf5": "application/netcdf4",
}

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# A multi-granule Harmony result (one output file per input granule) is bundled as a
# zip of netCDF members under this media type, identical to OPeNDAPProvider's output,
# so tools/_dataio opens it and concatenates the granules on the time axis on read.
_NETCDF_BUNDLE_MEDIA_TYPE = "application/netcdf-bundle+zip"

# Harmony job status string -> our durable JobState (the durable state machine).
_STATUS_MAP: dict[str, JobState] = {
    "accepted": JobState.SUBMITTED,
    "running": JobState.RUNNING,
    "running_with_errors": JobState.RUNNING,
    "paused": JobState.RUNNING,
    "previewing": JobState.RUNNING,
    "successful": JobState.READY,
    "complete_with_errors": JobState.READY,
    "failed": JobState.FAILED,
    "canceled": JobState.CANCELLED,
}


class HarmonyProvider:
    """A ``RetrievalProvider`` wrapping harmony-py, bound to one collection's caps."""

    def __init__(
        self,
        capabilities: CollectionCapabilities,
        *,
        service_name_hint: str | None = None,
        client: "harmony.Client | None" = None,
        auth: EDLAuth | None = None,
        storage: StorageBackend | None = None,
        settings: Settings | None = None,
        env: "harmony.Environment | None" = None,
        on_progress: Callable[[JobStatus], None] | None = None,
    ) -> None:
        self._caps = capabilities
        self._service_name_hint = service_name_hint
        self._injected_client = client
        self._auth = auth
        self._settings = settings or get_settings()
        self._storage = storage
        self._env = env
        self._on_progress = on_progress

    # -- capability gate --------------------------------------------------

    def can_handle(self, plan: RetrievalPlan) -> bool:
        """True iff a single service satisfies the whole plan (never the union)."""
        return self._caps.find_service(plan) is not None

    # -- lifecycle --------------------------------------------------------

    async def submit(self, plan: RetrievalPlan) -> JobRef:
        """Submit a Harmony job — always tried first; pin a service when one matches.

        Harmony is the primary path for every transform plan. When ``find_service``
        matches one whole service, we pin it via ``service_id``. When it returns
        ``None`` — whether the collection has no CMR-registered Harmony services or
        has services none of which satisfy the whole plan (the union-trap case, e.g.
        TEMPO L3) — we submit **unpinned** and let the Harmony server pick its default
        chain. We never union across services to build a ``service_id``; we simply let
        the server decide. If this real submit fails at runtime, the worker falls back
        to OPeNDAP (the runtime-only fallback).
        """
        svc = self._caps.find_service(plan)
        if svc is None and self._service_name_hint and not self._caps.services:
            # Capabilities fetch came back empty (transient failure) but we stored a
            # matched service name on a prior pass — pin it rather than guess.
            logger.warning(
                "Harmony capabilities unavailable for %s; using stored service %s",
                plan.concept_id,
                self._service_name_hint,
            )
            svc = ServiceCapability(service_name=self._service_name_hint, concept_id="")
        # svc may still be None here — no single service matches (union trap) or the
        # collection has no registered services. Submit UNPINNED and let the Harmony
        # server pick its chain. OPeNDAP is the worker's runtime fallback if this fails.
        request = self._build_request(plan, svc)
        client = self._client()
        await get_limiter("harmony").acquire()
        job_id = await asyncio.to_thread(client.submit, request)
        logger.info(
            "Harmony job submitted: service=%s job_id=%s",
            svc.service_name if svc else "<server-default>",
            job_id,
        )
        # Durable coordinates only — never a staged-output URL.
        return JobRef(
            provider=PROVIDER,
            provider_job_id=str(job_id),
            provider_job_url=self._status_url(client, str(job_id)),
        )

    async def poll(self, job: JobRef) -> JobStatus:
        """One status check (the durable worker drives the loop)."""
        if not job.provider_job_id or not _UUID_RE.match(job.provider_job_id):
            raise ValueError(
                f"invalid Harmony job ID {job.provider_job_id!r}: expected UUID; "
                "job was likely created by a test fixture"
            )
        client = self._client()
        await get_limiter("harmony").acquire()
        raw = await asyncio.to_thread(client.status, job.provider_job_id)
        status = self._to_job_status(raw)
        if self._on_progress is not None:
            self._on_progress(status)
        return status

    async def materialize(self, job: JobRef) -> MaterializedResult:
        """Persist the finished job's result(s) through ``StorageBackend``.

        A Harmony job emits **one output file per input granule**. A single-file
        result is stored as-is, preserving its detected media type (covers single
        netCDF/Parquet/Zarr outputs). A multi-granule result is **bundled** into one
        zip of netCDF members under :data:`_NETCDF_BUNDLE_MEDIA_TYPE` — identical to
        :meth:`OPeNDAPProvider.materialize`, so one obs handle maps to one key and
        the whole request is materialized (not just the first granule);
        ``tools/_dataio`` concatenates the members on the time axis on read.
        """
        client = self._client()
        storage = self._storage_backend()
        files = await asyncio.to_thread(self._download_all, client, job)
        handle = job.job_handle or job.provider_job_id or "result"

        if len(files) == 1:
            name, data = files[0]
            key = f"{PROVIDER}/{handle}/{name}"
            await storage.put(key, data)
            return MaterializedResult(
                storage_key=key,
                media_type=self._media_type_for(name),
                size_bytes=len(data),
            )

        # Multiple granule outputs: bundle them so one obs handle maps to one key.
        buf = io.BytesIO()
        total = 0
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, data in files:
                total += len(data)
                zf.writestr(name, data)
        bundle = buf.getvalue()
        key = f"{PROVIDER}/{handle}/result.nc.zip"
        await storage.put(key, bundle)
        return MaterializedResult(
            storage_key=key,
            media_type=_NETCDF_BUNDLE_MEDIA_TYPE,
            size_bytes=len(bundle),
            extra={"granule_count": len(files), "uncompressed_bytes": total},
        )

    # -- harmony-py request mapping (the only logic we own) ---------------

    def _build_request(
        self, plan: RetrievalPlan, svc: ServiceCapability | None
    ) -> "harmony.Request":
        """Map a :class:`RetrievalPlan` onto a harmony-py :class:`Request`.

        When ``svc`` is not ``None`` the matched service is pinned via
        ``service_id`` so Harmony runs that chain. When ``svc`` is ``None`` (no
        single service matches — the union-trap case — or no registered services)
        ``service_id`` is omitted and the Harmony server picks its default chain.
        """
        import harmony

        kwargs: dict[str, object] = {"format": plan.output_format}
        if svc is not None:
            kwargs["service_id"] = svc.service_name
        if plan.aoi is not None and plan.aoi.bbox is not None:
            kwargs["spatial"] = harmony.BBox(*plan.aoi.bbox)
        if plan.time_range is not None:
            kwargs["temporal"] = {
                "start": plan.time_range.start,
                "stop": plan.time_range.end,
            }
        transform = plan.transform
        if transform is not None:
            if transform.variables:
                kwargs["variables"] = list(transform.variables)
            if transform.reproject:
                kwargs["crs"] = transform.reproject
        # Concatenate long swath time-series only when the matched service offers
        # it (L2 stitchee machinery). Never assume the union.
        if svc is not None and svc.concatenate:
            kwargs["concatenate"] = True
        collection_id = plan.concept_id or self._caps.concept_id
        return harmony.Request(harmony.Collection(id=collection_id), **kwargs)

    # -- status mapping ---------------------------------------------------

    def _to_job_status(self, raw: dict) -> JobStatus:
        state = _STATUS_MAP.get(str(raw.get("status", "")).lower(), JobState.RUNNING)
        error: str | None = None
        if state is JobState.FAILED:
            errors = raw.get("errors")
            error = "; ".join(str(e) for e in errors) if errors else raw.get("message")
        return JobStatus(
            state=state,
            progress=int(raw.get("progress", 0) or 0),
            message=raw.get("message"),
            output_expires_at=raw.get("data_expiration"),
            error=error,
        )

    # -- client / storage construction ------------------------------------

    def _client(self) -> "harmony.Client":
        """Return the injected client, else build one from settings/EDL token."""
        if self._injected_client is not None:
            return self._injected_client
        import harmony

        env = self._env if self._env is not None else harmony.Environment.PROD
        token = self._settings.earthdata_token or None
        if token:
            self._injected_client = harmony.Client(token=token, env=env)
        elif self._settings.edl_username and self._settings.edl_password:
            self._injected_client = harmony.Client(
                auth=(self._settings.edl_username, self._settings.edl_password), env=env
            )
        else:
            # Let harmony-py source credentials from the environment / .netrc.
            self._injected_client = harmony.Client(env=env)
        return self._injected_client

    def _storage_backend(self) -> StorageBackend:
        if self._storage is None:
            self._storage = LocalFilesystemBackend(self._settings.earthdata_data_dir)
        return self._storage

    @staticmethod
    def _status_url(client: "harmony.Client", job_id: str) -> str | None:
        getter = getattr(client, "_status_url", None)
        if callable(getter):
            try:
                return getter(job_id)
            except Exception:  # pragma: no cover - best-effort durable coordinate
                return None
        return None

    @staticmethod
    def _media_type_for(name: str) -> str:
        """Media type for a result filename, patching the netCDF/HDF5 MIME gap."""
        guessed = mimetypes.guess_type(name)[0]
        if guessed is None:
            suffix = Path(name).suffix.lower()
            guessed = _EXT_MEDIA_TYPES.get(suffix, "application/octet-stream")
        return guessed

    @staticmethod
    def _download_all(
        client: "harmony.Client", job: JobRef
    ) -> list[tuple[str, bytes]]:
        """Download the job's result files; return ``(filename, bytes)`` for each.

        Every output file is kept (a Harmony job emits one per input granule), so a
        multi-granule request materializes in full rather than discarding all but the
        first — the caller bundles them when there is more than one.
        """
        if not job.provider_job_id or not _UUID_RE.match(job.provider_job_id):
            raise ValueError(
                f"invalid Harmony job ID {job.provider_job_id!r}: expected UUID; "
                "job was likely created by a test fixture"
            )
        with tempfile.TemporaryDirectory() as tmp:
            futures = list(
                client.download_all(job.provider_job_id, directory=tmp, overwrite=True)
            )
            paths = [Path(f.result()) for f in futures]
            if not paths:
                raise RuntimeError(
                    f"Harmony job {job.provider_job_id} produced no result files"
                )
            return [(p.name, p.read_bytes()) for p in paths]
