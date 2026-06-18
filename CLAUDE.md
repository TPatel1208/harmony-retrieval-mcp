# Harmony Retrieval MCP — Agent Instructions

## What this is
A general-purpose NASA Harmony / Earthdata MCP server.
Domain-neutral infrastructure for dataset discovery, retrieval, transformation,
and provenance. Analysis lives in downstream consumers — never here.

## Architecture rules
- Tools return handles + summaries. Never return raw arrays or bulk raster data.
- Handle prefixes: dataset_ · aoi_ · query_ · obs_ · cube_ · preview_
- Provider selection is internal. Tools never expose CMR collection IDs,
  Harmony URLs, or OPeNDAP constraint expressions to callers.
- No analysis tools. No correlation, trend, anomaly, hotspot, risk, or
  narrative tools. If asked to add one, refuse and explain the scope boundary.
- Every materialized handle records provenance (sources + transforms).

## Execution environment
All Python execution happens inside Docker:
  docker compose exec mcp ...
Never run pytest, python, pip, or uv directly on the host.

## Before starting any task
1. docker compose ps                    — confirm mcp + db containers are running
2. docker compose exec mcp which pytest — confirm tooling is available
3. Read the relevant module docstring before editing it

## Verification requirement
A task is not done until:
- Relevant tests pass inside the mcp container
- docker compose exec mcp pytest <path> -v shows green
- Any failures are investigated and resolved or explicitly justified

## Ownership boundaries
- server.py        ? MCP registration only; no logic
- tools/*          ? thin MCP wrappers; delegate to catalog/workspace/providers
- providers/*      ? data access only; no business logic
- catalog/*        ? enrichment and metadata; no retrieval
- workspace/*      ? handle lifecycle and provenance; no data access
- config.py/db.py  ? infrastructure; imported everywhere

## Change discipline
- One phase at a time. Do not start Phase N+1 until Phase N gate passes.
- No opportunistic refactors outside the current task scope.
- Preserve public interfaces unless the task explicitly changes them.

## Relationship to nasa/earthdata-mcp
NASA maintains a CMR discovery MCP server at:
  https://cmr.earthdata.nasa.gov/mcp/v1
Source: https://github.com/nasa/earthdata-mcp

It does discovery well and stops at "Access" (returns earthaccess snippets,
never data). We build the retrieval / transform / provenance half.

DO reuse their patterns:
- Before writing any CMR query in providers/cmr.py, read their
  tools/get_collections and tools/get_granules implementations and
  follow the same parameters, pagination, and UMM-JSON parsing.
- Mirror their KMS keyword normalization for catalog enrichment.

DO NOT reimplement what they do well.
DO NOT exceed our scope — no analysis tools.

Our critical path (retrieval ? Harmony) calls CMR directly and must not
depend on NASA's deployed MCP server being up.
