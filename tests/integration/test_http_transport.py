"""HTTP transport + bearer token smoke test (PRD 018).

Starts the real FastMCP app under streamable-HTTP with the bearer-token
middleware wired in — the same construction ``server.main()`` uses under
``EARTHDATA_MCP_TRANSPORT=http`` — and drives it with a real client over a
real socket. Only the transport is under test: a missing/wrong token must get
a 401, and a correct token must get full service (list tools, call one
network-free tool end-to-end against the real Postgres-backed default store).
No test asserts transport internals.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator

import httpx
import pytest
import uvicorn
from fastmcp import Client
from starlette.middleware import Middleware

from earthdata_mcp.http_auth import BearerTokenMiddleware
from earthdata_mcp.server import mcp

_TOKEN = "test-bearer-token"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def http_server_url() -> AsyncIterator[str]:
    """Run the real FastMCP app under streamable HTTP with the token middleware."""
    port = _free_port()
    app = mcp.http_app(
        transport="streamable-http",
        middleware=[Middleware(BearerTokenMiddleware, token=_TOKEN)],
    )
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.02)
    else:
        raise RuntimeError("HTTP server did not start in time")

    try:
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        server.should_exit = True
        await task


_MCP_HEADERS = {
    "accept": "application/json, text/event-stream",
    "content-type": "application/json",
}


async def test_missing_token_yields_401(http_server_url: str) -> None:
    async with httpx.AsyncClient() as client:
        resp = await client.post(http_server_url, json={}, headers=_MCP_HEADERS)
    assert resp.status_code == 401


async def test_wrong_token_yields_401(http_server_url: str) -> None:
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            http_server_url,
            json={},
            headers={**_MCP_HEADERS, "authorization": "Bearer wrong-token"},
        )
    assert resp.status_code == 401


async def test_correct_token_lists_tools_and_calls_cheap_tool(
    http_server_url: str,
) -> None:
    async with Client(http_server_url, auth=_TOKEN) as client:
        tools = await client.list_tools()
        assert any(t.name == "define_area_of_interest" for t in tools)

        result = await client.call_tool(
            "define_area_of_interest", {"location": "-105,37,-104,38"}
        )
        assert result.data["bbox"] == [-105.0, 37.0, -104.0, 38.0]
        assert result.data["handle"].startswith("aoi_")


@pytest.fixture
async def http_server_url_no_token() -> AsyncIterator[str]:
    """No token configured: the middleware is never wired in, so plain HTTP works."""
    port = _free_port()
    app = mcp.http_app(transport="streamable-http", middleware=[])
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.02)
    else:
        raise RuntimeError("HTTP server did not start in time")

    try:
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        server.should_exit = True
        await task


async def test_no_token_configured_serves_without_authorization_header(
    http_server_url_no_token: str,
) -> None:
    async with Client(http_server_url_no_token) as client:
        tools = await client.list_tools()
        assert any(t.name == "define_area_of_interest" for t in tools)
