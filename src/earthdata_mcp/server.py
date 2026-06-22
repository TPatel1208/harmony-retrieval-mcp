"""MCP server entry point.

Phase 5 registered the two handle-minting discovery tools (``search_datasets`` +
``describe_dataset``); Phase 6.1–6.2 adds the area + coverage surface
(``define_area_of_interest`` and the four metadata-only coverage tools). The rest
arrives in its respective phase. Importing this module must have no DB, network,
or credential side effects — the tools build their dependencies lazily, on first
call, never at import.
"""

from __future__ import annotations

from typing import Any

from fastmcp import FastMCP

from earthdata_mcp.tools import area, coverage, discovery, understanding

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


def main() -> None:
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
