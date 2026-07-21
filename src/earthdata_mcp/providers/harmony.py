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
import logging
import mimetypes
import re
import tempfile
import zipfile
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import requests

from earthdata_mcp.config import Settings, get_settings
from earthdata_mcp.formats import NETCDF_FAMILY_FORMATS, sniff_format
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

# Honest media types for a materialize result whose bytes are NOT netCDF-family
# (PRD 008): native HDF4/HDF-EOS2 granules Harmony returned as-is because no
# reformatting service ran, or bytes we can't identify at all. Never a netCDF type.
_HDF4_MEDIA_TYPE = "application/x-hdf4"
_UNKNOWN_MEDIA_TYPE = "application/octet-stream"
_NATIVE_ARCHIVE_MEDIA_TYPE = "application/x-native-archive+zip"

#: Bytes to read off the front of a result file to classify it. ``sniff_format``
#: only ever inspects leading magic numbers (the longest is 8 bytes), so a short
#: header read answers exactly as well as the whole file — and lets a multi-GB
#: granule be identified without ever being resident in memory.
_MAGIC_PREFIX_BYTES = 16


def _read_magic(path: Path) -> bytes:
    """The leading bytes of ``path``, enough for :func:`sniff_format` to classify it."""
    with path.open("rb") as fh:
        return fh.read(_MAGIC_PREFIX_BYTES)


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
        edl_token: str | None = None,
    ) -> None:
        self._caps = capabilities
        self._service_name_hint = service_name_hint
        self._injected_client = client
        self._auth = auth
        self._settings = settings or get_settings()
        self._storage = storage
        self._env = env
        self._on_progress = on_progress
        # Per-call identity (PRD 022): when set, the client is built with
        # harmony-py's explicit ``token=`` argument (no process-env mutation)
        # and cached per identity; when absent, the env-credential path below
        # is byte-for-byte today's behavior.
        self._edl_token = edl_token

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

    async def cancel(self, job: JobRef) -> None:
        """Best-effort Harmony cancel via harmony-py's ``client.cancel(job_id)``.

        A missing/malformed job id (never actually submitted, or a test
        fixture id) is a no-op — there is nothing upstream to cancel.
        harmony-py raises a two-arg ``Exception(response.reason, message)``
        for a 409 (job already in a terminal state — the only case its own
        cancel() method special-cases) and ``requests.HTTPError`` for a 404
        (unknown job id); both are exactly the "already-terminal or unknown"
        cases the Protocol says must never raise, so both are logged and
        swallowed here. Any other failure (auth, network, 5xx) propagates —
        the caller (``cancel_retrieval``) treats it as non-fatal but reports it.
        """
        if not job.provider_job_id or not _UUID_RE.match(job.provider_job_id):
            return
        client = self._client()
        await get_limiter("harmony").acquire()
        try:
            await asyncio.to_thread(client.cancel, job.provider_job_id)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                logger.info("Harmony cancel: job %s not found (already gone)", job.provider_job_id)
                return
            raise
        except Exception as exc:
            # harmony-py's own 409-conflict branch raises exactly this shape
            # (see harmony.Client.cancel) — never any other exception's shape.
            if type(exc) is Exception and len(exc.args) == 2:
                logger.info(
                    "Harmony cancel: job %s already in a terminal state (%s)",
                    job.provider_job_id,
                    exc.args[1],
                )
                return
            raise

    async def materialize(self, job: JobRef) -> MaterializedResult:
        """Persist the finished job's result(s) through ``StorageBackend``.

        A Harmony job emits **one output file per input granule**. A single-file
        result is stored as-is, preserving its detected media type (covers single
        netCDF/Parquet/Zarr outputs). A multi-granule result is **bundled** into one
        zip of netCDF members under :data:`_NETCDF_BUNDLE_MEDIA_TYPE` — identical to
        :meth:`OPeNDAPProvider.materialize`, so one obs handle maps to one key and
        the whole request is materialized (not just the first granule);
        ``tools/_dataio`` concatenates the members on the time axis on read.

        Everything moves **file-to-file**: granules are downloaded to a scratch
        directory, zipped member-by-member into an archive on disk, and handed to
        the backend with ``put_file``. Nothing here ever holds a whole granule (or
        the whole bundle) in memory, so peak RSS stays flat in the granule count
        instead of scaling with the result. That is not a micro-optimization: the
        earlier read-everything-then-zip-in-RAM version needed ~2x the uncompressed
        bundle and was OOM-killed by the kernel on a 50-granule TEMPO request —
        and since a SIGKILL leaves the job row non-terminal, the worker resumed it
        on restart straight back into the same kill.
        """
        client = self._client()
        storage = self._storage_backend()
        handle = job.job_handle or job.provider_job_id or "result"

        with tempfile.TemporaryDirectory() as tmp:
            paths = await asyncio.to_thread(self._download_all, client, job, tmp)

            if len(paths) == 1:
                path = paths[0]
                key = f"{PROVIDER}/{handle}/{path.name}"
                await storage.put_file(key, path)
                return MaterializedResult(
                    storage_key=key,
                    media_type=self._media_type_for(path.name, _read_magic(path)),
                    size_bytes=path.stat().st_size,
                )

            # Multiple granule outputs: bundle them so one obs handle maps to one key.
            # Sniffed per-member: all netCDF-family -> today's bundle type; any
            # HDF4/unknown member -> the whole bundle is a native archive (PRD 008 —
            # a mixed bundle is never half-opened, so one bad member taints the lot).
            # The archive is built beside the granules rather than in a BytesIO, and
            # each member is sniffed from its leading bytes only.
            bundle_path = Path(tmp) / "_bundle.nc.zip"
            total = 0
            formats: dict[str, int] = {}
            def _write_bundle() -> None:
                nonlocal total
                with zipfile.ZipFile(bundle_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    for path in paths:
                        total += path.stat().st_size
                        detected = sniff_format(_read_magic(path))
                        formats[detected] = formats.get(detected, 0) + 1
                        zf.write(path, arcname=path.name)

            await asyncio.to_thread(_write_bundle)
            all_netcdf = all(fmt in NETCDF_FAMILY_FORMATS for fmt in formats)
            media_type = (
                _NETCDF_BUNDLE_MEDIA_TYPE if all_netcdf else _NATIVE_ARCHIVE_MEDIA_TYPE
            )
            key = f"{PROVIDER}/{handle}/result.nc.zip"
            await storage.put_file(key, bundle_path)
            return MaterializedResult(
                storage_key=key,
                media_type=media_type,
                size_bytes=bundle_path.stat().st_size,
                extra={
                    "granule_count": len(paths),
                    "uncompressed_bytes": total,
                    "detected_formats": formats,
                },
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
            start, stop = plan.time_range.start, plan.time_range.end
            # Harmony rejects a zero-width temporal window ("The temporal range's
            # start must be earlier than its stop datetime"), but a single-day
            # request normalizes to start == stop (a date-only '2024-06-15/
            # 2024-06-15' parses to midnight on both ends). Nudge stop one second
            # forward so the window is non-degenerate: a daily granule still
            # overlaps it and a genuine instant request is unchanged in practice.
            # Only Harmony is this strict — CMR granule search (the OPeNDAP path)
            # accepts start == stop — so this widening belongs here at submit
            # time, not in the shared time_range grammar.
            if stop <= start:
                stop = start + timedelta(seconds=1)
            kwargs["temporal"] = {"start": start, "stop": stop}
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
        """Return the injected client, else build one from the per-call token
        (cached per identity, PRD 022) or from settings/EDL env credentials."""
        if self._injected_client is not None:
            return self._injected_client
        import harmony

        env = self._env if self._env is not None else harmony.Environment.PROD
        if self._edl_token:
            from earthdata_mcp.providers.identity import get_token_session, parse_edl_token

            identity = parse_edl_token(self._edl_token)
            session = get_token_session(identity)
            self._injected_client = session.client_for(
                f"harmony:{getattr(env, 'name', env)}",
                lambda: harmony.Client(token=identity.token, env=env),
            )
            return self._injected_client
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
    def _media_type_for(name: str, data: bytes) -> str:
        """Media type for a result file, sniffed by content first (PRD 008).

        netCDF-family bytes (HDF5/netCDF-4, netCDF classic) keep today's
        extension-based media type. HDF4 or unrecognized bytes get an honest
        native media type instead — extension-based guessing never overrides
        what the bytes actually are, so a mislabeled ``.nc4`` that is really
        HDF4 (the MOD13Q1 failure mode) can never be stored as netCDF.
        """
        detected = sniff_format(data)
        if detected == "hdf4":
            return _HDF4_MEDIA_TYPE
        if detected not in NETCDF_FAMILY_FORMATS:
            return _UNKNOWN_MEDIA_TYPE
        guessed = mimetypes.guess_type(name)[0]
        if guessed is None:
            suffix = Path(name).suffix.lower()
            guessed = _EXT_MEDIA_TYPES.get(suffix, "application/octet-stream")
        return guessed

    @staticmethod
    def _download_all(
        client: "harmony.Client", job: JobRef, directory: str
    ) -> list[Path]:
        """Download the job's result files into ``directory``; return their paths.

        Every output file is kept (a Harmony job emits one per input granule), so a
        multi-granule request materializes in full rather than discarding all but the
        first — the caller bundles them when there is more than one.

        Returns *paths*, never bytes: the caller owns ``directory``'s lifetime and
        streams from these files, so a large result is never resident in memory.
        """
        if not job.provider_job_id or not _UUID_RE.match(job.provider_job_id):
            raise ValueError(
                f"invalid Harmony job ID {job.provider_job_id!r}: expected UUID; "
                "job was likely created by a test fixture"
            )
        futures = list(
            client.download_all(job.provider_job_id, directory=directory, overwrite=True)
        )
        paths = [Path(f.result()) for f in futures]
        if not paths:
            raise RuntimeError(
                f"Harmony job {job.provider_job_id} produced no result files"
            )
        return paths
