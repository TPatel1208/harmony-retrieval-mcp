"""MCP server entry point.

Phase 1: constructs the FastMCP app with **zero tools registered**. Tools are
added in their respective phases. Importing this module must have no DB, network,
or credential side effects.
"""

from __future__ import annotations

from fastmcp import FastMCP

mcp = FastMCP("earthdata-mcp")


def main() -> None:
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
