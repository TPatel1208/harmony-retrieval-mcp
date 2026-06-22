"""MCP server entry point.

Phase 5: registers exactly the two handle-minting discovery tools
(``search_datasets`` + ``describe_dataset``). The remaining tool surface is added
in its respective phase. Importing this module must have no DB, network, or
credential side effects — the tools build their dependencies lazily, on first
call, never at import.
"""

from __future__ import annotations

from typing import Any

from fastmcp import FastMCP

from earthdata_mcp.tools import discovery, understanding

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


def main() -> None:
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
