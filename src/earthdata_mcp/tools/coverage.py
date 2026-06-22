"""Coverage and granule-inspection tools (PLAN.md §6 Phase 6.2).

Four metadata-only tools that answer "is the data there, and how much of it?" for
a ``dataset_`` + ``aoi_`` + time window. All delegate to ``CMRProvider``'s granule
search/count — no Harmony, no downloads (PLAN.md §6 constraint):

* ``check_availability`` — count-only (CMR-Hits header); the fast yes/no.
* ``check_coverage`` — count plus a small sample of granules to eyeball.
* ``inspect_granules`` — the granule records themselves (URs, URLs, sizes).
* ``estimate_retrieval_size`` — sum CMR-reported granule sizes; the gate a later
  phase uses to refuse a huge request before any byte moves.

Each resolves both handles within the caller's workspace (cross-workspace access
is denied, not silently served) and rebuilds the CMR query from the durable
payloads — never from anything ephemeral.
"""

from __future__ import annotations

from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.workspace.models import HandleType, handle_type_of
from earthdata_mcp.workspace.store import WorkspaceStore

#: Granules sampled to estimate total retrieval size. CMR caps the per-request
#: limit at 50; the estimate extrapolates from this sample (see the warning path).
_SIZE_SAMPLE_LIMIT = 50

#: Sample size for ``check_coverage``'s eyeball list.
_COVERAGE_SAMPLE_LIMIT = 10


async def check_coverage(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    cmr: CMRProvider | None = None,
    store: WorkspaceStore | None = None,
) -> dict:
    """Is there data for this dataset/AOI/time? Returns a count + sample granules."""
    cmr = cmr or CMRProvider()
    store = store or _default_store()

    concept_id, bbox_str = await _resolve_handles(
        dataset_handle, aoi_handle, workspace_id, store
    )
    granules = await cmr.search_granules(
        concept_id,
        bounding_box=bbox_str,
        temporal=time_range,
        limit=_COVERAGE_SAMPLE_LIMIT,
    )
    return {
        "dataset_handle": dataset_handle,
        "aoi_handle": aoi_handle,
        "time_range": time_range,
        "granule_count": len(granules),
        "covered": len(granules) > 0,
        "sample_granules": granules,
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
    """Fast count-only availability check — uses CMR-Hits, fetches no granule data."""
    cmr = cmr or CMRProvider()
    store = store or _default_store()

    concept_id, bbox_str = await _resolve_handles(
        dataset_handle, aoi_handle, workspace_id, store
    )
    result = await cmr.check_availability(
        concept_id, bounding_box=bbox_str, temporal=time_range
    )
    return {
        "dataset_handle": dataset_handle,
        "aoi_handle": aoi_handle,
        "time_range": time_range,
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
    """Return up to ``limit`` granule records (CMR caps the effective limit at 50)."""
    cmr = cmr or CMRProvider()
    store = store or _default_store()

    concept_id, bbox_str = await _resolve_handles(
        dataset_handle, aoi_handle, workspace_id, store
    )
    granules = await cmr.search_granules(
        concept_id,
        bounding_box=bbox_str,
        temporal=time_range,
        limit=limit,
    )
    return {
        "dataset_handle": dataset_handle,
        "aoi_handle": aoi_handle,
        "time_range": time_range,
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
) -> dict:
    """Estimate total retrieval size by summing CMR-reported granule sizes.

    Sums ``size_mb`` over a sample of up to 50 granules. Granules that report no
    size (UMM-G may omit it) are excluded from the average; ``warning`` is set
    when no granules match or none report a size, so a caller never mistakes a
    metadata gap for "0 MB to download".
    """
    cmr = cmr or CMRProvider()
    store = store or _default_store()

    concept_id, bbox_str = await _resolve_handles(
        dataset_handle, aoi_handle, workspace_id, store
    )
    granules = await cmr.search_granules(
        concept_id,
        bounding_box=bbox_str,
        temporal=time_range,
        limit=_SIZE_SAMPLE_LIMIT,
    )

    sized = [g for g in granules if g.get("size_mb", 0.0) > 0.0]
    total_size_mb = sum(g["size_mb"] for g in sized)
    avg_size_mb = total_size_mb / len(sized) if sized else 0.0

    warning: str | None = None
    if not granules:
        warning = "No granules found for this AOI and time range."
    elif not sized:
        warning = (
            f"None of the {len(granules)} sampled granules reported a size; "
            "size estimate unavailable."
        )

    return {
        "dataset_handle": dataset_handle,
        "aoi_handle": aoi_handle,
        "time_range": time_range,
        "sampled_granules": len(granules),
        "total_size_mb": round(total_size_mb, 3),
        "avg_size_mb": round(avg_size_mb, 3),
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

    Type-checks both handle prefixes up front (a wrong-typed handle is a
    ``ValueError`` before any DB hit), then resolves each within ``workspace_id``
    — ``store.get_handle`` raises ``CrossWorkspaceError``/``HandleNotFoundError``,
    which propagate. The bbox string is rebuilt from the AOI payload in the
    ``"W,S,E,N"`` order CMR's ``bounding_box`` param expects.
    """
    if handle_type_of(dataset_handle) is not HandleType.DATASET:
        raise ValueError(
            f"expected a dataset_ handle, got {dataset_handle!r}"
        )
    if handle_type_of(aoi_handle) is not HandleType.AOI:
        raise ValueError(f"expected an aoi_ handle, got {aoi_handle!r}")

    dataset_record = await store.get_handle(workspace_id, dataset_handle)
    aoi_record = await store.get_handle(workspace_id, aoi_handle)

    concept_id = dataset_record.payload.get("concept_id")
    if not concept_id:
        raise ValueError(
            f"dataset handle {dataset_handle!r} payload missing 'concept_id'"
        )

    bbox = aoi_record.payload.get("bbox")
    if not bbox or len(bbox) != 4:
        raise ValueError(
            f"aoi handle {aoi_handle!r} payload missing or malformed 'bbox'"
        )
    bbox_str = ",".join(str(c) for c in bbox)

    return concept_id, bbox_str
