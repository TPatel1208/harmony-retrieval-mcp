"""MCP server entry point.

Phase 5 registered the two handle-minting discovery tools (``search_datasets`` +
``describe_dataset``); Phase 6.1–6.2 added the area + coverage surface; Phase 6.3
adds the durable retrieval tools (``retrieve_data``/``retrieve_subset``/
``retrieve_timeseries``/``get_retrieval_status``/``cancel_retrieval``); Phase 7.1–7.2
adds the preview tools (``preview_dataset``/``summarize_dataset``/
``inspect_statistics``) and the transform tools (``subset``/``reproject``/
``resample``/``convert_format``/``align``); Phase 8 completes the v1 surface with
the provenance tools (``get_provenance``/``cite_dataset``). Importing this module
must have no DB, network, or credential side effects — the tools build their
dependencies lazily, on first call, never at import.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastmcp import FastMCP
from pydantic import Field
from starlette.middleware import Middleware

from earthdata_mcp.config import get_settings
from earthdata_mcp.http_auth import BearerTokenMiddleware
from earthdata_mcp.tools import (
    area,
    coverage,
    discovery,
    export,
    preview,
    provenance,
    retrieval,
    transform,
    understanding,
    workspace,
)

mcp = FastMCP("earthdata-mcp")

#: The uniform per-call identity parameter (PRD 022), advertised on every
#: workspace-scoped tool's input schema so a consumer can feature-detect
#: support at connect time. Consumer-supplied, never model-supplied (the
#: consumer strips it from model-facing schemas, like ``workspace_id``);
#: absent/empty means the server's environment credential — byte-for-byte
#: today's behavior.
EdlToken = Annotated[
    str | None,
    Field(
        description=(
            "Optional per-user Earthdata Login bearer token. Consumer-supplied, "
            "never model-supplied. When present, this call (and any durable job "
            "it spawns) authenticates to NASA as that token's identity; absent "
            "means the server's environment credential."
        )
    ),
]


@mcp.tool
async def search_datasets(
    query: str,
    filters: dict[str, Any] | None = None,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Search NASA Earthdata collections; mint a ``dataset_`` handle per result.

    Returns ``{"datasets": [{"handle", "summary"}], "count": n}``. ``filters`` is
    an optional dict of CMR collection-search params (e.g. ``temporal``,
    ``bounding_box``, ``provider``, ``limit``).
    """
    return await discovery.search_datasets(query, filters, workspace_id, edl_token=edl_token)


@mcp.tool
async def describe_dataset(
    dataset_handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    detail: bool = False,
    edl_token: EdlToken = None,
) -> dict:
    """Resolve a ``dataset_`` handle to its metadata, variables, and advisory notes.

    Compact by default (name/long_name/units/fill_values/valid_ranges/
    advisory_notes per variable — a variable with neither fill values nor a
    valid range in UMM-Var carries a ``mask_metadata_note`` flagging the gap;
    core collection identification fields only). Pass ``detail=True`` for the
    full per-variable UMM-Var enrichment and the complete collection metadata.
    """
    return await understanding.describe_dataset(dataset_handle, workspace_id, detail, edl_token=edl_token)


@mcp.tool
async def define_area_of_interest(
    location: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Define an area of interest and mint an ``aoi_`` handle.

    ``location`` may be a bbox string ``"-105,37,-104,38"`` (W,S,E,N decimal
    degrees), a bare point ``"-93.6,41.6"`` (lon,lat), a GeoJSON geometry/Feature
    as a JSON string, a HUC watershed code (``"14080101"``, or explicit ``"HUC
    14080101"``), a US FIPS state/county code (``"06001"``, or explicit ``"FIPS
    06001"``; 2-digit state, 5-digit county), or a place/watershed name to
    geocode. A bare 2-digit code is ambiguous (valid as both HUC2 and a FIPS
    state code) and raises a disambiguation error naming both prefixed forms —
    use ``"HUC <code>"`` or ``"FIPS <code>"`` explicitly. Returns
    ``{"handle", "bbox", "geometry_summary", "source"}`` — ``geometry_summary``
    is ``{"type", "vertex_count"}`` (or ``None``); resolve the handle for the
    full geometry.
    """
    return await area.define_area_of_interest(location, workspace_id, edl_token=edl_token)


@mcp.tool
async def check_coverage(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Check granule coverage for a dataset/AOI/time. Returns a count + sample granules."""
    return await coverage.check_coverage(
        dataset_handle, aoi_handle, time_range, workspace_id, edl_token=edl_token
    )


@mcp.tool
async def check_availability(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Fast count-only availability check — no granule data fetched."""
    return await coverage.check_availability(
        dataset_handle, aoi_handle, time_range, workspace_id, edl_token=edl_token
    )


@mcp.tool
async def inspect_granules(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    limit: int = 10,
    edl_token: EdlToken = None,
) -> dict:
    """Return granule records for a dataset/AOI/time (up to ``limit``, CMR-capped at 50)."""
    return await coverage.inspect_granules(
        dataset_handle, aoi_handle, time_range, workspace_id, limit=limit, edl_token=edl_token
    )


@mcp.tool
async def estimate_retrieval_size(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Estimate total retrieval size by sampling up to 50 granules' reported sizes."""
    return await coverage.estimate_retrieval_size(
        dataset_handle, aoi_handle, time_range, workspace_id, edl_token=edl_token
    )


@mcp.tool
async def retrieve_data(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    output_format: str | None = None,
    edl_token: EdlToken = None,
) -> dict:
    """Retrieve a dataset over an AOI + time window as a durable job.

    Returns ``{job_handle, obs_handle, status, provider}`` immediately; poll the
    ``job_`` handle with ``get_retrieval_status``. Output format defaults by the
    collection's shape (gridded → Zarr) unless ``output_format`` is given.
    """
    return await retrieval.retrieve_data(
        dataset_handle, aoi_handle, time_range, workspace_id, output_format, edl_token=edl_token
    )


@mcp.tool
async def retrieve_subset(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    variables: list[str],
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    output_format: str | None = None,
    edl_token: EdlToken = None,
) -> dict:
    """Retrieve a variable + bbox + temporal subset as a durable job.

    Harmony is tried first: a single service that does all the requested transforms
    is pinned; if none does, Harmony is submitted unpinned (server picks the chain).
    OPeNDAP is the worker's runtime fallback. Returns the job/obs handles.
    """
    return await retrieval.retrieve_subset(
        dataset_handle, aoi_handle, time_range, variables, workspace_id, output_format, edl_token=edl_token
    )


@mcp.tool
async def retrieve_timeseries(
    dataset_handle: str,
    time_range: str,
    variables: list[str],
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    output_format: str | None = None,
    aoi_handle: str | None = None,
    point_sample: bool = False,
    edl_token: EdlToken = None,
) -> dict:
    """Retrieve a variable time-series as a durable job; the AOI is optional.

    Set ``point_sample=True`` to sample at a point/area: the request routes to
    AppEEARS and returns a tabular series as Parquet (never a Zarr cube).
    """
    return await retrieval.retrieve_timeseries(
        dataset_handle,
        time_range,
        variables,
        workspace_id,
        output_format,
        aoi_handle,
        point_sample, edl_token=edl_token
    )


@mcp.tool
async def get_retrieval_status(
    job_handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Read a retrieval job's durable state from Postgres (status/progress/error).

    Also carries a curated slice of what the job pertains to — ``short_name``,
    ``variables``, ``aoi_bbox``, ``time_range``, ``provider``,
    ``output_format``, and ``granule_count`` — whichever of those the job's
    request actually set; never the raw internal spec.

    An unknown, cross-workspace, or orphaned ``job_`` handle is a structured
    ``{job_handle, status: "not_found", message}`` answer, not an error — a
    missing job is a valid answer to "what's this job's status?".
    """
    return await retrieval.get_retrieval_status(job_handle, workspace_id, edl_token=edl_token)


@mcp.tool
async def cancel_retrieval(
    job_handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Cancel a non-terminal retrieval job.

    Flips the local durable state to cancelled (the worker stops
    polling/materializing) and then best-effort cancels the job at its
    provider too — real for Harmony and AppEEARS, a no-op for the synchronous
    OPeNDAP path. The response carries ``upstream``: ``"requested"`` (issued
    to the provider), ``"unsupported"`` (OPeNDAP, or a job never submitted
    upstream), ``"already_terminal"`` (the job had already finished before
    this call landed), or ``"error"`` (the provider cancel failed — logged,
    never fatal to this call; the local cancel still succeeds).

    Idempotent: a job that is already terminal (ready/failed/expired/cancelled)
    is left as-is and its current status is returned rather than raising — the
    caller's intent ("stop this job") is already satisfied by the time the call
    lands, since a job's state can change between when a caller decides to
    cancel and when the request arrives.

    An unknown, cross-workspace, or orphaned ``job_`` handle is likewise not an
    error: it returns ``{job_handle, status: "not_found", cancelled: False,
    message}`` — "stop this job" on a gone job is already satisfied.
    """
    return await retrieval.cancel_retrieval(job_handle, workspace_id, edl_token=edl_token)


@mcp.tool
async def preview_dataset(
    dataset_handle: str,
    time_range: str | None = None,
    aoi_handle: str | None = None,
    layer: str | None = None,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Build a GIBS visual preview reference for a dataset; mint a ``preview_`` handle.

    Returns ``{handle, gibs_url, layer, bbox, time, format}``. No network call is
    made — ``gibs_url`` is a request the agent or a browser can fetch.
    """
    return await preview.preview_dataset(
        dataset_handle, time_range, aoi_handle, layer, workspace_id, edl_token=edl_token
    )


@mcp.tool
async def summarize_dataset(
    handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Structural summary of a ``dataset_`` (metadata) or materialized ``obs_``/``cube_``."""
    return await preview.summarize_dataset(handle, workspace_id, edl_token=edl_token)


@mcp.tool
async def inspect_statistics(
    handle: str,
    variables: list[str] | None = None,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Descriptive per-variable statistics (min/max/mean/std/count) over a result."""
    return await preview.inspect_statistics(handle, variables, workspace_id, edl_token=edl_token)


@mcp.tool
async def subset(
    source_handle: str,
    aoi_handle: str | None = None,
    variables: list[str] | None = None,
    time_range: str | None = None,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Spatial/variable/temporal subset of a materialized result → a ``cube_`` handle."""
    return await transform.subset(
        source_handle, aoi_handle, variables, time_range, workspace_id, edl_token=edl_token
    )


@mcp.tool
async def reproject(
    source_handle: str,
    crs: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Tag a gridded result with a target CRS → a ``cube_`` handle."""
    return await transform.reproject(source_handle, crs, workspace_id, edl_token=edl_token)


@mcp.tool
async def resample(
    source_handle: str,
    time_freq: str | None = None,
    spatial_factor: int | None = None,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Temporal and/or spatial resampling of a gridded result → a ``cube_`` handle."""
    return await transform.resample(
        source_handle, time_freq, spatial_factor, workspace_id, edl_token=edl_token
    )


@mcp.tool
async def convert_format(
    source_handle: str,
    output_format: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Re-serialize a materialized result to a different media type → a ``cube_`` handle.

    ``output_format`` accepts ``"csv"``, ``"netcdf"``, ``"parquet"``, ``"zarr"`` (or
    their media-type spellings), case-insensitive. CSV requires a tabular (point)
    source; netCDF requires a gridded source — each raises a clear error otherwise.
    """
    return await transform.convert_format(source_handle, output_format, workspace_id, edl_token=edl_token)


@mcp.tool
async def align(
    source_handles: list[str],
    method: str = "outer",
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Align ≥2 gridded results to a common grid → a ``cube_`` handle + alignment report."""
    return await transform.align(source_handles, method, workspace_id=workspace_id, edl_token=edl_token)


@mcp.tool
async def export_result(
    handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Resolve a materialized ``obs_``/``cube_`` handle to an openable URI.

    Returns ``{handle, status: "ready", storage_uri, media_type, size_bytes,
    rematerialize_hint}``. ``storage_uri`` is scheme-agnostic — ``file://``
    locally, a presigned ``https://`` URL for an S3 backend — so open-by-scheme
    logic survives a move to hosted deployment unchanged. Exporting LRU-touches
    the handle so an actively-read result is never the first thing evicted.

    A pending or expired (evicted) handle answers ``{handle, status, message}``
    instead of raising; an unknown or cross-workspace handle collapses to the
    same ``not_found`` answer so existence never leaks.
    """
    return await export.export_result(handle, workspace_id, edl_token=edl_token)


@mcp.tool
async def rematerialize(
    handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Re-enqueue the durable job behind an evicted ``obs_`` handle.

    Resubmits the request spec already persisted with the original job — never a
    new or overridden spec — via the same enqueue path a fresh retrieval uses.
    Returns ``{job_handle, obs_handle, status}``; poll ``job_handle`` with
    ``get_retrieval_status`` and call ``export_result`` again once it is ready.

    A handle whose result is still cached answers immediately with no provider
    round-trip. A handle with no job behind it (e.g. a transform-derived
    ``cube_``) or an unknown/cross-workspace handle answers a structured
    ``{status: "not_found"}`` rather than raising.
    """
    return await export.rematerialize(handle, workspace_id, edl_token=edl_token)


@mcp.tool
async def get_provenance(
    handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Return a handle's lineage: its ancestry graph (recursive CTE) and events.

    Events are ``created`` (a ``cube_`` handle's mint) or
    ``routed``/``submitted``/``provider-fallback``/``opendap-not-applicable``/
    ``job-failed``/``materialized`` (an ``obs_`` handle's retrieval lifecycle).
    A ``job_`` handle redirects to its ``obs_`` handle's lineage (the response
    then adds ``resolved_handle``); a handle with no lineage (e.g. ``dataset_``/
    ``aoi_``) gets an explanatory ``note`` instead of bare empty arrays.
    Returns ``{handle, ancestors: [{handle, depth}], events: [...],
    resolved_handle?, note?}`` scoped to the workspace — lineage never crosses a
    workspace boundary.
    """
    return await provenance.get_provenance(handle, workspace_id, edl_token=edl_token)


@mcp.tool
async def cite_dataset(
    dataset_handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    edl_token: EdlToken = None,
) -> dict:
    """Official DOI + formal citation strings for a ``dataset_`` handle.

    Citations come from CMR's own records (UMM-C DOI + CollectionCitations), never
    hand-rolled. Returns ``{handle, concept_id, doi, doi_authority,
    collection_citations, reference_citation_count}``.
    """
    return await provenance.cite_dataset(dataset_handle, workspace_id, edl_token=edl_token)


@mcp.tool
async def list_workspace(
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    handle_type: str | None = None,
    edl_token: EdlToken = None,
) -> dict:
    """Re-enumerate a workspace's handles — for a consumer reconnecting after a
    restart or page reload, with no conversation history to replay them from.

    ``handle_type`` optionally filters to one kind (``dataset``, ``aoi``, ``query``,
    ``obs``, ``cube``, ``preview``, ``job``). Returns ``{handles: [{handle, type,
    created_at, summary}]}``, oldest first; ``summary`` is a small type-specific
    slice of the handle's payload, not the full payload.
    """
    return await workspace.list_workspace(workspace_id, handle_type, edl_token=edl_token)


def main() -> None:
    """Run the MCP server: stdio by default, streamable HTTP when configured."""
    settings = get_settings()
    if settings.mcp_transport == "http":
        middleware = []
        if settings.mcp_token:
            middleware.append(Middleware(BearerTokenMiddleware, token=settings.mcp_token))
        mcp.run(
            transport="http",
            host=settings.mcp_host,
            port=settings.mcp_port,
            middleware=middleware,
            # Uvicorn's default access logger crashes formatting the access line
            # for sse_starlette's streamed responses (record.args is None on the
            # final send() after a client disconnects mid-stream), which breaks
            # the SSE connection and sends clients into a reconnect storm. Access
            # logging brings no operational value here (structured app logs cover
            # request handling); disabling it removes the crash entirely.
            uvicorn_config={"access_log": False},
        )
    else:
        mcp.run()


if __name__ == "__main__":
    main()
