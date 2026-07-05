"""``CMRProvider`` — metadata only (PLAN.md §1, §4.1; docs/cmr_patterns.md).

Canon is CMR's public API + the UMM schemas; NASA's ``nasa/earthdata-mcp`` is a
pinned worked example only. This provider hits ``cmr.earthdata.nasa.gov``
directly for collection/granule/variable/service metadata and merges the
Harmony ``/capabilities`` view (Layer 2) for ``collection_capabilities``.

**Metadata only — there is deliberately no ``retrieve`` here** (a retrieval
method would be a leaky abstraction; retrieval lives behind ``RetrievalProvider``
in Phase 4). Retry policy: 5xx and timeouts are retried, **4xx never is** (a 4xx
is a malformed query — retrying just wastes calls).
"""

from __future__ import annotations

from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from earthdata_mcp.config import Settings, get_settings
from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.base import ProviderCapabilities
from earthdata_mcp.providers.ratelimit import get_limiter

# UMM-JSON endpoints (docs/cmr_patterns.md). All five search tools hit the
# ``.umm_json`` variants so the response carries typed ``umm`` + CMR ``meta``.
_COLLECTIONS = "/search/collections.umm_json"
_GRANULES = "/search/granules.umm_json"
_VARIABLES = "/search/variables.umm_json"
_SERVICES = "/search/services.umm_json"

# Legacy .json feed — the only CMR endpoint that exposes EDSC tags such as
# edsc.extra.serverless.gibs (docs/cmr_patterns.md §Common HTTP conventions).
_COLLECTIONS_JSON = "/search/collections.json"

# CMR caps user-facing limits low; mirror NASA's defaults.
_DEFAULT_LIMIT = 10
_MAX_LIMIT = 50
_PAGE_SIZE = 500

# ``concept_id[]`` entries per ``get_variables`` batch — comfortably under the
# URL length CloudFront starts rejecting with a bare 414 (docs/cmr_patterns.md).
_VARIABLE_ID_BATCH_SIZE = 100


class CMRError(Exception):
    """A non-retryable CMR error (4xx): a malformed query, not a transient fault."""


class RetryableError(Exception):
    """A transient CMR fault (5xx) worth retrying."""


def _message_from_body(response: httpx.Response) -> str:
    """Pull a concise message from a CMR error body: errors[] → message/error → text."""
    try:
        body = response.json()
    except ValueError:
        return response.text.strip()
    if isinstance(body, dict):
        errors = body.get("errors")
        if isinstance(errors, list) and errors:
            return "; ".join(str(e) for e in errors)
        for key in ("message", "error"):
            if body.get(key):
                return str(body[key])
    return response.text.strip()


class CMRProvider:
    """A ``MetadataProvider`` over CMR's public search API. Metadata only."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._cmr_base = self._settings.cmr_url.rstrip("/")
        self._harmony_base = self._settings.harmony_url.rstrip("/")
        self._headers = {"Client-Id": self._settings.cmr_client_id}

    def capabilities(self) -> ProviderCapabilities:
        """CMR is a **metadata-only** provider — it mints no retrieval formats.

        Declaring this (rather than leaving it implicit) is what lets
        ``CMRProvider`` satisfy :class:`~earthdata_mcp.providers.base.MetadataProvider`
        without a single throwing stub.
        """
        return ProviderCapabilities(
            name="cmr", kind="metadata", output_formats=frozenset()
        )

    # -- HTTP -------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type((RetryableError, httpx.TimeoutException)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=0.5, max=8),
        reraise=True,
    )
    async def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 60.0,
    ) -> httpx.Response:
        """Issue one request. Retries 5xx/timeout; raises ``CMRError`` on 4xx."""
        merged = dict(self._headers)
        if headers:
            merged.update(headers)
        # Polite per-provider rate limiting at the HTTP boundary (§8). Each retry
        # attempt takes a token too, so a retry storm is throttled as well.
        await get_limiter("cmr").acquire()
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(
                method, url, params=params, headers=merged
            )
        if 500 <= response.status_code < 600:
            raise RetryableError(
                f"CMR {response.status_code}: {_message_from_body(response)}"
            )
        if 400 <= response.status_code < 500:
            raise CMRError(
                f"CMR {response.status_code}: {_message_from_body(response)}"
            )
        return response

    async def _search(
        self, endpoint: str, params: dict[str, Any], *, limit: int
    ) -> list[dict]:
        """Page a ``.umm_json`` search via ``CMR-Search-After`` up to ``limit`` items.

        The query params are byte-stable across pages; the cursor lives only in the
        ``CMR-Search-After`` request/response header (docs/cmr_patterns.md).
        """
        url = f"{self._cmr_base}{endpoint}"
        query = dict(params)
        query["page_size"] = min(limit, _PAGE_SIZE)
        items: list[dict] = []
        search_after: str | None = None
        while len(items) < limit:
            headers = {"CMR-Search-After": search_after} if search_after else None
            response = await self._request("GET", url, params=query, headers=headers)
            page = response.json().get("items", [])
            items.extend(page)
            search_after = response.headers.get("CMR-Search-After")
            if not search_after or len(page) < query["page_size"]:
                break
        return items[:limit]

    async def _count(self, endpoint: str, params: dict[str, Any]) -> int:
        """Count-only query: ``page_size=0`` returns the total via ``CMR-Hits``."""
        url = f"{self._cmr_base}{endpoint}"
        query = dict(params)
        query["page_size"] = 0
        response = await self._request("GET", url, params=query)
        return int(response.headers.get("CMR-Hits", 0))

    # -- Collections ------------------------------------------------------

    async def search_collections(
        self,
        *,
        keyword: str | None = None,
        concept_id: str | None = None,
        short_name: str | None = None,
        provider: str | None = None,
        temporal: str | None = None,
        bounding_box: str | None = None,
        processing_level_id: list[str] | None = None,
        has_granules: bool | None = None,
        limit: int = _DEFAULT_LIMIT,
    ) -> list[dict]:
        """Search collections (UMM-C); returns normalized records."""
        params = _clean(
            {
                "keyword": keyword,
                "concept_id": concept_id,
                "short_name": short_name,
                "provider": provider,
                "temporal": temporal,
                "bounding_box": bounding_box,
                "processing_level_id[]": processing_level_id,
                "has_granules": _bool(has_granules),
            }
        )
        items = await self._search(
            _COLLECTIONS, params, limit=min(limit, _MAX_LIMIT)
        )
        return [normalize_collection_item(i) for i in items]

    async def _fetch_collection(self, concept_id: str) -> dict | None:
        """One-shot collection fetch by concept id (``page_size=1``)."""
        items = await self._search(
            _COLLECTIONS, {"concept_id": concept_id}, limit=1
        )
        return items[0] if items else None

    async def fetch_gibs_layers(self, concept_id: str) -> list[dict]:
        """Return GIBS layer objects from the CMR EDSC tag for one collection.

        Hits the legacy ``.json`` endpoint with ``include_tags=edsc.extra.serverless.gibs``
        (the only CMR endpoint that exposes EDSC tags; docs/cmr_patterns.md §endpoints).
        Parses ``feed.entry[0].tags["edsc.extra.serverless.gibs"].data``.
        Returns ``[]`` on any failure — 4xx, 5xx, timeout, or missing tag.
        """
        url = f"{self._cmr_base}{_COLLECTIONS_JSON}"
        params = {"concept_id": concept_id, "include_tags": "edsc.extra.serverless.gibs"}
        try:
            response = await self._request("GET", url, params=params, timeout=30.0)
        except (CMRError, RetryableError, httpx.TimeoutException):
            return []
        try:
            feed = response.json()
        except ValueError:
            return []
        entries = feed.get("feed", {}).get("entry", [])
        if not entries:
            return []
        tags = entries[0].get("tags", {})
        data = tags.get("edsc.extra.serverless.gibs", {}).get("data", [])
        return data if isinstance(data, list) else []

    # -- Granules ---------------------------------------------------------

    async def search_granules(
        self,
        collection_concept_id: str,
        *,
        temporal: str | None = None,
        bounding_box: str | None = None,
        cloud_cover: str | None = None,
        day_night_flag: str | None = None,
        sort_key: str | None = None,
        limit: int = _DEFAULT_LIMIT,
    ) -> list[dict]:
        """Search granules (UMM-G) within one parent collection."""
        params = _clean(
            {
                "collection_concept_id": collection_concept_id,
                "temporal": temporal,
                "bounding_box": bounding_box,
                "cloud_cover": cloud_cover,
                "day_night_flag": day_night_flag,
                "sort_key": sort_key,
            }
        )
        items = await self._search(
            _GRANULES, params, limit=min(limit, _MAX_LIMIT)
        )
        return [normalize_granule_item(i) for i in items]

    async def check_availability(
        self,
        collection_concept_id: str,
        *,
        temporal: str | None = None,
        bounding_box: str | None = None,
    ) -> dict:
        """Count-only granule query — confirms data actually exists for an AOI+time."""
        params = _clean(
            {
                "collection_concept_id": collection_concept_id,
                "temporal": temporal,
                "bounding_box": bounding_box,
            }
        )
        total = await self._count(_GRANULES, params)
        return {
            "collection_concept_id": collection_concept_id,
            "granule_count": total,
            "available": total > 0,
        }

    # -- Variables & services (two-phase via collection associations) -----

    async def _association_ids(self, collection_concept_id: str, kind: str) -> list[str]:
        """Phase 1: read ``meta.associations.<kind>`` off the collection record."""
        item = await self._fetch_collection(collection_concept_id)
        if not item:
            return []
        return item.get("meta", {}).get("associations", {}).get(kind, [])

    async def get_variables(
        self,
        collection_concept_id: str | None = None,
        *,
        keyword: str | None = None,
        limit: int = _MAX_LIMIT,
    ) -> list[dict]:
        """Two-phase lookup: collection associations → ``variables.umm_json``.

        A keyword-only call (no collection) skips phase 1, searches globally, and
        is capped at ``limit`` (an open-ended search needs a cap). A collection-
        scoped call ignores ``limit`` and always returns the *complete*
        association list instead — describe_dataset's ``variable_count`` promises
        "always the true total," and retrieve_subset's plan-time
        ``validate_variables`` must be able to confirm a real variable that
        simply isn't among the first ``limit`` associations in CMR's (arbitrary)
        ordering. The list is batched into groups of :data:`_VARIABLE_ID_BATCH_SIZE`
        ``concept_id[]`` entries per request — some real collections (MOD08_D3/
        MYD08_D3 carry 975 UMM-Var associations) put enough ids on one query
        string that CloudFront rejects it with a bare 414 well before CMR's own
        limits would.
        """
        if collection_concept_id:
            ids = await self._association_ids(collection_concept_id, "variables")
            if not ids:
                return []
            items: list[dict] = []
            for i in range(0, len(ids), _VARIABLE_ID_BATCH_SIZE):
                batch = ids[i : i + _VARIABLE_ID_BATCH_SIZE]
                params: dict[str, Any] = {"concept_id[]": batch}
                if keyword:
                    params["keyword"] = keyword
                items.extend(await self._search(_VARIABLES, params, limit=len(batch)))
            return [normalize_variable_item(i) for i in items]
        if not keyword:
            return []
        items = await self._search(_VARIABLES, {"keyword": keyword}, limit=limit)
        return [normalize_variable_item(i) for i in items]

    async def get_services(
        self,
        collection_concept_id: str | None = None,
        *,
        keyword: str | None = None,
        type: str | None = None,
        limit: int = _MAX_LIMIT,
    ) -> list[dict]:
        """Two-phase lookup: collection associations → ``services.umm_json``."""
        params = _clean({"keyword": keyword, "type": type})
        if collection_concept_id:
            ids = await self._association_ids(collection_concept_id, "services")
            if not ids:
                return []
            params["concept_id[]"] = ids
        if not params:
            return []
        items = await self._search(_SERVICES, params, limit=limit)
        return [normalize_service_item(i) for i in items]

    # -- Citations (NASA get_citations pattern; canon = UMM-C) ------------

    async def get_citations(self, collection_concept_id: str) -> dict:
        """Official DOI + formal citation strings for a collection.

        Mirrors NASA's ``get_citations`` intent — "how do I cite this dataset" —
        but reads the authoritative records straight from UMM-C (canon, not the
        NASA repo): the collection's own ``DOI`` and ``CollectionCitations`` (the
        formal citation strings CMR publishes). It also counts the works that
        *cite* the dataset via ``meta.associations.citations`` (CMR's citation
        concepts are publications-that-cite, a different thing from the dataset's
        own citation), exposed as a count rather than fetching hundreds of records.

        Graceful by contract: a collection with no DOI and no citation records
        returns empty fields and a zero count — never an error.
        """
        item = await self._fetch_collection(collection_concept_id)
        if not item:
            return {
                "concept_id": collection_concept_id,
                "doi": None,
                "doi_authority": None,
                "collection_citations": [],
                "reference_citation_count": 0,
            }
        meta = item.get("meta", {})
        umm = item.get("umm", {})
        doi = umm.get("DOI") or {}
        reference_ids = meta.get("associations", {}).get("citations", []) or []
        return {
            "concept_id": meta.get("concept-id", collection_concept_id),
            "doi": doi.get("DOI"),
            "doi_authority": doi.get("Authority"),
            # The formal "how to cite" strings, verbatim from CMR's UMM-C record.
            "collection_citations": umm.get("CollectionCitations") or [],
            # Count of works citing the dataset (associated citation concepts).
            "reference_citation_count": len(reference_ids),
        }

    # -- Merged capability view (Layer 1 + Layer 2) -----------------------

    async def collection_capabilities(self, concept_id: str) -> CollectionCapabilities:
        """Merge UMM-C (CMR) with the per-service Harmony ``/capabilities`` view.

        The service layer comes from Harmony's **public** capabilities endpoint
        (no auth); per-service blocks are parsed in ``_capabilities`` and the
        rolled-up union booleans are ignored (PLAN.md §4.2).
        """
        item = await self._fetch_collection(concept_id)
        umm_c = item.get("umm", {}) if item else {}
        harmony_caps = await self._fetch_harmony_capabilities(concept_id)
        caps = CollectionCapabilities.from_harmony_capabilities(harmony_caps, umm_c)
        # The collection's real concept id is authoritative over the Harmony echo.
        if item:
            caps.concept_id = item.get("meta", {}).get("concept-id", concept_id)
        else:
            caps.concept_id = caps.concept_id or concept_id
        return caps

    async def _fetch_harmony_capabilities(self, concept_id: str) -> dict:
        """Fetch Harmony's ``/capabilities`` JSON (Bearer-authenticated; empty dict if 4xx/none).

        Harmony redirects unauthenticated requests to Earthdata Login (returns HTML
        with status 200), so we must always send the Bearer token.
        """
        url = f"{self._harmony_base}/capabilities"
        params = {"collectionid": concept_id, "format": "json"}
        headers: dict[str, str] = {}
        token = self._settings.earthdata_token
        if token:
            headers["Authorization"] = f"Bearer {token.strip()}"
        try:
            response = await self._request("GET", url, params=params, headers=headers, timeout=30.0)
        except (CMRError, RetryableError, httpx.TimeoutException):
            # 4xx → collection unknown to Harmony; 5xx → Harmony internal error;
            # timeout → Harmony unreachable. Treat all as "no capabilities" and
            # let the router decide. A collection with no Harmony services returns
            # 200 with "services":[] — that is the normal path, not this branch.
            return {}
        try:
            return response.json()
        except ValueError:
            return {}

    async def get_dmr(self, opendap_url: str) -> str | None:
        """Fetch a granule's DAP4 DMR (Dataset Metadata Response) XML, or ``None``.

        The one non-CMR fetch this provider makes: OPeNDAP planning's structure-
        discovery fallback (PRD 009) when a collection's UMM-Var catalog is empty
        or insufficient. Bearer-authenticated the same way as the Harmony
        capabilities fetch; fail-soft on any error (network, timeout, 4xx/5xx) —
        the caller degrades to variables-only planning rather than raising, the
        same posture as every other CMR-facing planning call.
        """
        url = f"{opendap_url.rstrip('/')}.dmr.xml"
        headers: dict[str, str] = {}
        token = self._settings.earthdata_token
        if token:
            headers["Authorization"] = f"Bearer {token.strip()}"
        try:
            response = await self._request("GET", url, headers=headers, timeout=30.0)
        except (CMRError, RetryableError, httpx.TimeoutException, httpx.HTTPError):
            return None
        return response.text


# -- UMM-JSON normalization -----------------------------------------------


def normalize_collection_item(item: dict) -> dict:
    """Normalize a UMM-C item, surfacing the fields the capability merge needs.

    Includes the three NASA's ``normalize_collection_item`` omits at the pinned
    commit — ``DirectDistributionInformation``, ``StandardProduct``, ``Purpose`` —
    read straight from the UMM-C record (docs/cmr_patterns.md).
    """
    meta = item.get("meta", {})
    umm = item.get("umm", {})
    info = umm.get("ArchiveAndDistributionInformation") or {}
    native_formats = [
        {"format": f.get("Format"), "media_type": f.get("Media")}
        for f in info.get("FileDistributionInformation", [])
    ]
    return {
        "concept_id": meta.get("concept-id"),
        "provider_id": meta.get("provider-id"),
        "revision_id": meta.get("revision-id"),
        "associations": meta.get("associations", {}),
        "short_name": umm.get("ShortName"),
        "version": umm.get("Version"),
        "entry_title": umm.get("EntryTitle"),
        "abstract": umm.get("Abstract"),
        "processing_level": (umm.get("ProcessingLevel") or {}).get("Id"),
        "doi": (umm.get("DOI") or {}).get("DOI"),
        "science_keywords": umm.get("ScienceKeywords", []),
        "temporal_extents": umm.get("TemporalExtents", []),
        "native_formats": native_formats,
        # Three fields NASA omits at the pinned commit — we read them ourselves:
        "direct_distribution_information": umm.get("DirectDistributionInformation"),
        "standard_product": umm.get("StandardProduct"),
        "purpose": umm.get("Purpose"),
        # Maturity NASA does surface:
        "collection_progress": umm.get("CollectionProgress"),
    }


def normalize_granule_item(item: dict) -> dict:
    """Normalize a UMM-G item (subset relevant to coverage/size estimation)."""
    meta = item.get("meta", {})
    umm = item.get("umm", {})
    data_granule = umm.get("DataGranule", {})
    size_mb = 0.0
    for adi in data_granule.get("ArchiveAndDistributionInformation", []) or []:
        size = adi.get("SizeInBytes")
        if size:
            size_mb += size / (1024**2)
        elif adi.get("Size") and adi.get("SizeUnit") == "MB":
            size_mb += float(adi["Size"])
    time_start, time_end = _granule_temporal_extent(umm.get("TemporalExtent", {}))
    return {
        "concept_id": meta.get("concept-id"),
        "granule_ur": umm.get("GranuleUR"),
        "related_urls": umm.get("RelatedUrls", []),
        "cloud_cover": umm.get("CloudCover"),
        "day_night_flag": data_granule.get("DayNightFlag"),
        "size_mb": size_mb,
        "time_start": time_start,
        "time_end": time_end,
    }


def _granule_temporal_extent(temporal_extent: dict) -> tuple[str | None, str | None]:
    """A granule's ``(time_start, time_end)`` from UMM-G ``TemporalExtent``.

    ``RangeDateTime`` (Beginning/EndingDateTime) is most common; a granule with
    ``SingleDateTime`` instead reports the same instant for both ends.
    """
    range_dt = temporal_extent.get("RangeDateTime")
    if range_dt:
        return range_dt.get("BeginningDateTime"), range_dt.get("EndingDateTime")
    single = temporal_extent.get("SingleDateTime")
    return single, single


def normalize_variable_item(item: dict) -> dict:
    """Normalize a UMM-V item. Scale/offset/fill/valid-range feed enrichment."""
    meta = item.get("meta", {})
    umm = item.get("umm", {})
    return {
        "concept_id": meta.get("concept-id"),
        "name": umm.get("Name"),
        "long_name": umm.get("LongName"),
        "definition": umm.get("Definition"),
        "data_type": umm.get("DataType"),
        "units": umm.get("Units"),
        "scale": umm.get("Scale"),
        "offset": umm.get("Offset"),
        "fill_values": umm.get("FillValues", []),
        "valid_ranges": umm.get("ValidRanges", []),
        "standard_name": umm.get("StandardName"),
        "dimensions": [
            {"name": d.get("Name"), "size": d.get("Size")}
            for d in umm.get("Dimensions", []) or []
        ],
    }


def normalize_service_item(item: dict) -> dict:
    """Normalize a UMM-S item. ``ServiceOptions`` is passed through raw.

    We do not decompose ``ServiceOptions`` into capability booleans here — the
    per-service capability decomposition is done in ``_capabilities`` from the
    Harmony ``/capabilities`` view (PLAN.md §4.2).
    """
    meta = item.get("meta", {})
    umm = item.get("umm", {})
    return {
        "concept_id": meta.get("concept-id"),
        "name": umm.get("Name"),
        "long_name": umm.get("LongName"),
        "type": umm.get("Type"),
        "version": umm.get("Version"),
        "service_options": umm.get("ServiceOptions", {}),
    }


# -- helpers ---------------------------------------------------------------


def _clean(params: dict[str, Any]) -> dict[str, Any]:
    """Drop ``None`` values so they never reach the query string."""
    return {k: v for k, v in params.items() if v is not None}


def _bool(value: bool | None) -> str | None:
    """CMR wants lowercase ``true``/``false`` strings."""
    if value is None:
        return None
    return "true" if value else "false"
