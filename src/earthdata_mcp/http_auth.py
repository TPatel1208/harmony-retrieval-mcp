"""Bearer-token gate for the HTTP transport (PRD 018).

A tripwire, not real authn: one static token, constant-time compared, checked on
every request when configured. stdio never sees this — it's wired in only when
``mcp_transport == "http"`` and ``mcp_token`` is set.
"""

from __future__ import annotations

import hmac

from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send


class BearerTokenMiddleware:
    """Rejects any HTTP request whose ``Authorization`` header isn't ``Bearer <token>``.

    401 with no body on mismatch or absence — never leaks which part was wrong.
    """

    def __init__(self, app: ASGIApp, token: str) -> None:
        self.app = app
        self._token = token

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        header = request.headers.get("authorization", "")
        scheme, _, presented = header.partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(presented, self._token):
            response = Response(status_code=401)
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)
