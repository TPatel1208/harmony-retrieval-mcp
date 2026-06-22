"""MCP server entry point.

Phase 5 registered the two handle-minting discovery tools (``search_datasets`` +
``describe_dataset``); Phase 6.1–6.2 added the area + coverage surface; Phase 6.3
adds the durable retrieval tools (``retrieve_data``/``retrieve_subset``/
``retrieve_timeseries``/``get_retrieval_status``/``cancel_retrieval``); Phase 7.1–7.2
adds the preview tools (``preview_dataset``/``summarize_dataset``/
``inspect_statistics``) and the transform tools (``subset``/``reproject``/
``resample``/``convert_format``/``align``). The rest arrives in its respective
phase. Importing this module must have no DB, network,
or credential side effects — the tools build their dependencies lazily, on first
call, never at import.
"""

from __future__ import annotations

from typing import Any

from fastmcp import FastMCP

from earthdata_mcp.tools import (
    area,
    coverage,
    discovery,
    preview,
    retrieval,
    transform,
    understanding,
)

mcp = FastMCP("earthdata-mcp")


@mcp.tool
async def search_datasets(
    query: str,
    filters: dict[str, Any] | None = None,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Search NASA Earthdata collections; mint a ``dataset_`` handle per result.

    Returns ``{"datasets": [{"handle", "summary"}], "count": n}``. ``filters`` is
    an optional dict of CMR collection-search params (e.g. ``temporal``,
    ``bounding_box``, ``provider``, ``limit``).
    """
    return await discovery.search_datasets(query, filters, workspace_id)


@mcp.tool
async def describe_dataset(
    dataset_handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Resolve a ``dataset_`` handle to its metadata, variables, and advisory notes."""
    return await understanding.describe_dataset(dataset_handle, workspace_id)


@mcp.tool
async def define_area_of_interest(
    location: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Define an area of interest and mint an ``aoi_`` handle.

    ``location`` may be a bbox string ``"-105,37,-104,38"`` (W,S,E,N decimal
    degrees), a GeoJSON geometry/Feature as a JSON string, or a place name / HUC
    watershed / FIPS code to geocode. Returns
    ``{"handle", "bbox", "geojson", "source"}``.
    """
    return await area.define_area_of_interest(location, workspace_id)


@mcp.tool
async def check_coverage(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Check granule coverage for a dataset/AOI/time. Returns a count + sample granules."""
    return await coverage.check_coverage(
        dataset_handle, aoi_handle, time_range, workspace_id
    )


@mcp.tool
async def check_availability(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Fast count-only availability check — no granule data fetched."""
    return await coverage.check_availability(
        dataset_handle, aoi_handle, time_range, workspace_id
    )


@mcp.tool
async def inspect_granules(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    limit: int = 10,
) -> dict:
    """Return granule records for a dataset/AOI/time (up to ``limit``, CMR-capped at 50)."""
    return await coverage.inspect_granules(
        dataset_handle, aoi_handle, time_range, workspace_id, limit=limit
    )


@mcp.tool
async def estimate_retrieval_size(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Estimate total retrieval size by sampling up to 50 granules' reported sizes."""
    return await coverage.estimate_retrieval_size(
        dataset_handle, aoi_handle, time_range, workspace_id
    )


@mcp.tool
async def retrieve_data(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    output_format: str | None = None,
) -> dict:
    """Retrieve a dataset over an AOI + time window as a durable job.

    Returns ``{job_handle, obs_handle, status, provider}`` immediately; poll the
    ``job_`` handle with ``get_retrieval_status``. Output format defaults by the
    collection's shape (gridded → Zarr) unless ``output_format`` is given.
    """
    return await retrieval.retrieve_data(
        dataset_handle, aoi_handle, time_range, workspace_id, output_format
    )


@mcp.tool
async def retrieve_subset(
    dataset_handle: str,
    aoi_handle: str,
    time_range: str,
    variables: list[str],
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    output_format: str | None = None,
) -> dict:
    """Retrieve a variable + bbox + temporal subset as a durable job.

    Routes only to a single service that does all the requested transforms; fails
    fast if none can (no Harmony fallback). Returns the job/obs handles.
    """
    return await retrieval.retrieve_subset(
        dataset_handle, aoi_handle, time_range, variables, workspace_id, output_format
    )


@mcp.tool
async def retrieve_timeseries(
    dataset_handle: str,
    time_range: str,
    variables: list[str],
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
    output_format: str | None = None,
    aoi_handle: str | None = None,
) -> dict:
    """Retrieve a variable time-series as a durable job; the AOI is optional."""
    return await retrieval.retrieve_timeseries(
        dataset_handle,
        time_range,
        variables,
        workspace_id,
        output_format,
        aoi_handle,
    )


@mcp.tool
async def get_retrieval_status(
    job_handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Read a retrieval job's durable state from Postgres (status/progress/error)."""
    return await retrieval.get_retrieval_status(job_handle, workspace_id)


@mcp.tool
async def cancel_retrieval(
    job_handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Cancel a non-terminal retrieval job (illegal once it is already terminal)."""
    return await retrieval.cancel_retrieval(job_handle, workspace_id)


@mcp.tool
async def preview_dataset(
    dataset_handle: str,
    time_range: str | None = None,
    aoi_handle: str | None = None,
    layer: str | None = None,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Build a GIBS visual preview reference for a dataset; mint a ``preview_`` handle.

    Returns ``{handle, gibs_url, layer, bbox, time, format}``. No network call is
    made — ``gibs_url`` is a request the agent or a browser can fetch.
    """
    return await preview.preview_dataset(
        dataset_handle, time_range, aoi_handle, layer, workspace_id
    )


@mcp.tool
async def summarize_dataset(
    handle: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Structural summary of a ``dataset_`` (metadata) or materialized ``obs_``/``cube_``."""
    return await preview.summarize_dataset(handle, workspace_id)


@mcp.tool
async def inspect_statistics(
    handle: str,
    variables: list[str] | None = None,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Descriptive per-variable statistics (min/max/mean/std/count) over a result."""
    return await preview.inspect_statistics(handle, variables, workspace_id)


@mcp.tool
async def subset(
    source_handle: str,
    aoi_handle: str | None = None,
    variables: list[str] | None = None,
    time_range: str | None = None,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Spatial/variable/temporal subset of a materialized result → a ``cube_`` handle."""
    return await transform.subset(
        source_handle, aoi_handle, variables, time_range, workspace_id
    )


@mcp.tool
async def reproject(
    source_handle: str,
    crs: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Tag a gridded result with a target CRS → a ``cube_`` handle."""
    return await transform.reproject(source_handle, crs, workspace_id)


@mcp.tool
async def resample(
    source_handle: str,
    time_freq: str | None = None,
    spatial_factor: int | None = None,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Temporal and/or spatial resampling of a gridded result → a ``cube_`` handle."""
    return await transform.resample(
        source_handle, time_freq, spatial_factor, workspace_id
    )


@mcp.tool
async def convert_format(
    source_handle: str,
    output_format: str,
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Re-serialize a materialized result to a different media type → a ``cube_`` handle."""
    return await transform.convert_format(source_handle, output_format, workspace_id)


@mcp.tool
async def align(
    source_handles: list[str],
    method: str = "outer",
    workspace_id: str = discovery.DEFAULT_WORKSPACE,
) -> dict:
    """Align ≥2 gridded results to a common grid → a ``cube_`` handle + alignment report."""
    return await transform.align(source_handles, method, workspace_id)


def main() -> None:
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
