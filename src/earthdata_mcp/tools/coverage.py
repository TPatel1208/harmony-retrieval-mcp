"""Coverage and granule-inspection tools (PLAN.md §6 Phase 6.2).

Four metadata-only tools that answer "is the data there, and how much of it?" for
a ``dataset_`` + ``aoi_`` + time window. All delegate to ``CMRProvider``'s granule
search/count — no Harmony, no downloads (PLAN.md §6 constraint):

* ``check_availability`` — count-only (CMR-Hits header); the fast yes/no.
* ``check_coverage`` — the same true count plus a small sample of granules to
  eyeball; ``granule_count`` always agrees with ``check_availability``.
* ``inspect_granules`` — the granule records themselves (URs, URLs, sizes).
* ``estimate_retrieval_size`` — extrapolate the true total from a sampled
  average granule size; the gate a later phase uses to refuse a huge request
  before any byte moves.

Each resolves both handles within the caller's workspace (cross-workspace access
is denied, not silently served) and rebuilds the CMR query from the durable
payloads — never from anything ephemeral.
"""

from __future__ import annotations

from earthdata_mcp.config import Settings, get_settings
from earthdata_mcp.providers.base import normalize_time_range
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.workspace.handles import resolve_aoi, resolve_dataset
from earthdata_mcp.workspace.store import WorkspaceStore

#: Granules sampled to estimate total retrieval size. CMR caps the per-request
#: limit at 50; the estimate extrapolates from this sample (see the warning path).
_SIZE_SAMPLE_LIMIT = 50

#: Sample size for ``check_coverage``'s eyeball list.
_COVERAGE_SAMPLE_LIMIT = 10

#: The literal that opts a coverage tool into querying the whole archive
#: (PRD 003) — any other empty/whitespace ``time_range`` is a validation error,
#: never a silent "all time".
_ALL_TIME_TOKEN = "all"


def _coverage_time_window(time_range: str) -> tuple[str | None, bool]:
    """Resolve a coverage tool's ``time_range`` into ``(cmr_temporal, all_time)``.

    The literal ``"all"`` (case-insensitive) is the one documented escape
    hatch for "no time bound" — every other value goes through
    :func:`normalize_time_range`, which raises before any CMR call rather than
    silently treating empty/whitespace as "all time".
    """
    if time_range.strip().lower() == _ALL_TIME_TOKEN:
        return None, True
    return normalize_time_range(time_range), False


async def check_coverage(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    cmr: CMRProvider | None = None,
    store: WorkspaceStore | None = None,
) -> dict:
    """Is there data for this dataset/AOI/time? Returns the true count + a sample.

    ``granule_count`` is the true total (CMR-Hits, via the same query
    ``check_availability`` uses) — never capped at the eyeball sample size, so
    it agrees with ``check_availability`` for the same dataset/AOI/time.
    ``sample_granules`` is a slim eyeball projection (granule identifier +
    temporal extent only) capped at :data:`_COVERAGE_SAMPLE_LIMIT`; its size is
    reported as ``sampled_granules``. Full granule records (URs, URLs, sizes)
    are ``inspect_granules``'s job — this tool answers "is it there", not "what
    is it".

    ``time_range`` accepts ``start/end`` or ``start,end`` with ISO-8601
    endpoints (date-only or full timestamp, open ranges allowed); empty input
    is rejected before any CMR call. Pass the literal ``"all"`` to query the
    whole archive deliberately — the response then carries ``all_time: true``.
    """
    cmr = cmr or CMRProvider()
    store = store or _default_store()
    temporal, all_time = _coverage_time_window(time_range)

    concept_id, bbox_str = await _resolve_handles(
        dataset_handle, aoi_handle, workspace_id, store
    )
    availability = await cmr.check_availability(
        concept_id, bounding_box=bbox_str, temporal=temporal
    )
    granules = await cmr.search_granules(
        concept_id,
        bounding_box=bbox_str,
        temporal=temporal,
        limit=_COVERAGE_SAMPLE_LIMIT,
    )
    sample = [
        {
            "concept_id": g.get("concept_id"),
            "granule_ur": g.get("granule_ur"),
            "time_start": g.get("time_start"),
            "time_end": g.get("time_end"),
        }
        for g in granules
    ]
    return {
        "dataset_handle": dataset_handle,
        "aoi_handle": aoi_handle,
        "time_range": time_range,
        "all_time": all_time,
        "granule_count": availability["granule_count"],
        "covered": availability["available"],
        "sampled_granules": len(sample),
        "sample_granules": sample,
    }


async def check_availability(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    cmr: CMRProvider | None = None,
    store: WorkspaceStore | None = None,
) -> dict:
    """Fast count-only availability check — uses CMR-Hits, fetches no granule data.

    ``time_range`` accepts ``start/end`` or ``start,end`` with ISO-8601
    endpoints (date-only or full timestamp, open ranges allowed); empty input
    is rejected before any CMR call. Pass the literal ``"all"`` to query the
    whole archive deliberately — the response then carries ``all_time: true``.
    """
    cmr = cmr or CMRProvider()
    store = store or _default_store()
    temporal, all_time = _coverage_time_window(time_range)

    concept_id, bbox_str = await _resolve_handles(
        dataset_handle, aoi_handle, workspace_id, store
    )
    result = await cmr.check_availability(
        concept_id, bounding_box=bbox_str, temporal=temporal
    )
    return {
        "dataset_handle": dataset_handle,
        "aoi_handle": aoi_handle,
        "time_range": time_range,
        "all_time": all_time,
        "available": result["available"],
        "granule_count": result["granule_count"],
    }


async def inspect_granules(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    limit: int = 10,
    *,
    cmr: CMRProvider | None = None,
    store: WorkspaceStore | None = None,
) -> dict:
    """Return up to ``limit`` granule records (CMR caps the effective limit at 50).

    ``time_range`` accepts ``start/end`` or ``start,end`` with ISO-8601
    endpoints (date-only or full timestamp, open ranges allowed); empty input
    is rejected before any CMR call. Pass the literal ``"all"`` to query the
    whole archive deliberately — the response then carries ``all_time: true``.
    """
    cmr = cmr or CMRProvider()
    store = store or _default_store()
    temporal, all_time = _coverage_time_window(time_range)

    concept_id, bbox_str = await _resolve_handles(
        dataset_handle, aoi_handle, workspace_id, store
    )
    granules = await cmr.search_granules(
        concept_id,
        bounding_box=bbox_str,
        temporal=temporal,
        limit=limit,
    )
    return {
        "dataset_handle": dataset_handle,
        "aoi_handle": aoi_handle,
        "time_range": time_range,
        "all_time": all_time,
        "granules": granules,
        "count": len(granules),
    }


async def estimate_retrieval_size(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    cmr: CMRProvider | None = None,
    store: WorkspaceStore | None = None,
    settings: Settings | None = None,
) -> dict:
    """Estimate total retrieval size by extrapolating from a sample of granules.

    Averages ``size_mb`` over a sample of up to 50 granules, then multiplies by
    the true granule count (CMR-Hits, the same count ``check_availability``
    reports) to produce ``total_size_mb`` for the *whole* AOI+time window — not
    just the sum of the sampled subset, which stays constant regardless of AOI
    size since granule files are global and subsetting happens downstream.
    Granules that report no size (UMM-G may omit it) are excluded from the
    average; ``warning`` is set when no granules match or none report a size,
    so a caller never mistakes a metadata gap for "0 MB to download". When the
    extrapolated total exceeds ``settings.size_warning_threshold_mb`` (default
    ~10 GB), ``warning`` instead advises narrowing the AOI, time window, or
    variables before any byte moves — the no-granules/no-sizes warnings above
    take priority since they are mutually exclusive with a large estimate.

    ``time_range`` accepts ``start/end`` or ``start,end`` with ISO-8601
    endpoints (date-only or full timestamp, open ranges allowed); empty input
    is rejected before any CMR call. Pass the literal ``"all"`` to query the
    whole archive deliberately — the response then carries ``all_time: true``
    (and, being unbounded, is exactly the case this tool's size warning exists
    to catch).
    """
    cmr = cmr or CMRProvider()
    store = store or _default_store()
    settings = settings or get_settings()
    temporal, all_time = _coverage_time_window(time_range)

    concept_id, bbox_str = await _resolve_handles(
        dataset_handle, aoi_handle, workspace_id, store
    )
    availability = await cmr.check_availability(
        concept_id, bounding_box=bbox_str, temporal=temporal
    )
    total_granules = availability["granule_count"]
    granules = await cmr.search_granules(
        concept_id,
        bounding_box=bbox_str,
        temporal=temporal,
        limit=_SIZE_SAMPLE_LIMIT,
    )

    sized = [g for g in granules if g.get("size_mb", 0.0) > 0.0]
    avg_size_mb = sum(g["size_mb"] for g in sized) / len(sized) if sized else 0.0
    total_size_mb = avg_size_mb * total_granules

    warning: str | None = None
    if not granules:
        warning = "No granules found for this AOI and time range."
    elif not sized:
        warning = (
            f"None of the {len(granules)} sampled granules reported a size; "
            "size estimate unavailable."
        )
    elif total_size_mb > settings.size_warning_threshold_mb:
        warning = (
            f"Estimated total is ~{total_size_mb:,.0f} MB (~{total_size_mb / 1024:.1f} GB) — "
            "narrow the AOI, time window, or variables before retrieving."
        )

    return {
        "dataset_handle": dataset_handle,
        "aoi_handle": aoi_handle,
        "time_range": time_range,
        "all_time": all_time,
        "total_granules": total_granules,
        "sampled_granules": len(granules),
        "avg_size_mb": round(avg_size_mb, 3),
        "total_size_mb": round(total_size_mb, 3),
        "warning": warning,
    }


# -- handle resolution -----------------------------------------------------


async def _resolve_handles(
    dataset_handle: str,
    aoi_handle: str,
    workspace_id: str,
    store: WorkspaceStore,
) -> tuple[str, str]:
    """Resolve a ``dataset_`` + ``aoi_`` pair → ``(concept_id, bbox_str)``.

    A thin composition of the two typed resolvers; the CMR ``"W,S,E,N"``
    bounding-box string is rebuilt here from the bbox tuple ``resolve_aoi``
    returns, since that formatting is specific to this caller.
    """
    concept_id = await resolve_dataset(store, workspace_id, dataset_handle)
    bbox = await resolve_aoi(store, workspace_id, aoi_handle)
    bbox_str = ",".join(str(c) for c in bbox)
    return concept_id, bbox_str
