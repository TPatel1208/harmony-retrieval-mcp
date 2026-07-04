# PRD 018 — HTTP transport, bearer token, and cross-stack compose topology

**Repo:** harmony-retrieval-mcp · **Session scope:** one session, one commit · **Label:** ready-for-agent
**Depends on:** PRD 016. Pairs with PRD 017 as the consumer contract; either order works after 016.

## Problem Statement

As the Talking-to-Air backend — a separate container in a separate compose stack — I cannot connect to this server at all: it speaks stdio only, which requires being the consumer's child process, and its compose service runs an idle placeholder with no port exposed. There is also no authentication story, and the project's end goal is a hosted deployment, so building the consumer against an unauthenticated endpoint would bake in trust-the-network assumptions that later have to be hunted down.

## Solution

Env-selectable streamable-HTTP transport with a static bearer token, plus the compose topology that lets a second stack attach: the server joins an external Docker network and shares its materialization volume so `file://` URIs from `export_result` resolve identically in consumer containers. Posture: contract-proof now, harden later — token now (cheap, keeps consumers honest), real authn/S3/quotas deferred to a pre-hosting phase.

## User Stories

1. As a consumer stack, I want the server reachable at a stable HTTP URL on a shared Docker network, so that my backend connects like any other service dependency.
2. As an operator, I want transport selected by environment (stdio default, HTTP opt-in with host/port), so that local MCP-client use and cross-container use are both one config away.
3. As an operator, I want a static bearer token required on every HTTP request when configured, so that a published port is not an open door and consumers send credentials from day one.
4. As the TTA backend, I want `file://` URIs from `export_result` to resolve at the same absolute path inside my container, so that opening exported data is a plain filesystem read.
5. As an operator, I want the compose service to actually run the server (not an idle placeholder) when HTTP transport is configured, so that `docker compose up` yields a working endpoint.
6. As a security-conscious future maintainer, I want the current posture documented (private-network lab box; token is a tripwire, not real authn), so that the pre-hosting hardening scope is explicit rather than forgotten.

## Implementation Decisions

- Config gains transport, host, port, and token settings, all env-driven with the existing settings prefix and defaults preserving today's stdio behavior.
- The server entrypoint selects the transport at startup: stdio when unset, streamable HTTP (FastMCP ≥2 native) when configured.
- Token enforcement at the HTTP layer (middleware/auth hook on the FastMCP app), applied only when a token is configured; stdio ignores it. Constant-time comparison; 401 with no detail on mismatch.
- Compose: the mcp service runs the server command under HTTP config; the port is exposed on an **external** Docker network (created out-of-band, both stacks join it); the materialization volume becomes a named volume mounted at an identical absolute path in any consumer that attaches.
- Document in the README/compose comments: which env vars, how a second stack attaches, and the security posture + deferred hardening list (real authn, S3 + presigned URLs, per-workspace quotas).

## Testing Decisions

- Unit: config parsing (defaults, env overrides, invalid transport rejected) at the existing config test seam.
- Integration: in-container smoke — start the server under HTTP config, connect with an MCP client over streamable HTTP, list tools, call one cheap tool end-to-end; assert a missing/wrong bearer token yields 401 and a correct one succeeds. Prior art: the integration suite's use of the real compose services.
- No test asserts transport internals — only that a client can or cannot get service.

## Out of Scope

- Real authentication (OAuth/JWT), token rotation, per-user identity — pre-hosting phase.
- S3 backend / presigned URLs — the PRD 017 contract already accommodates them.
- The TTA-side connection code (Talking-to-Air PRD T02).

## Further Notes

Decision provenance: bearer-token-now was decided in the 2026-07-04 grilling session (deployment end goal is hosted/public; current scope is a lab box).
