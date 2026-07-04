# PRD 017 — `export_result` + `rematerialize`: the consumer data-egress contract

**Repo:** harmony-retrieval-mcp · **Session scope:** one session, one commit · **Label:** ready-for-agent
**Depends on:** PRD 016 (green gate).

## Problem Statement

As an external consumer (Talking-to-Air's analysis layer), I cannot reach the data this server materializes: no tool returns bytes, a path, or a URL — the storage key lives in the handle payload and is only read by the server's own preview/transform tools. And when a cached result is evicted, I have no durable way to get it back: the request spec is persisted server-side, but no tool replays it, so recovery currently depends on the originating conversation still being in someone's context window.

## Solution

Two new read-side tools that together form the complete egress contract: `export_result` hands the consumer a URI it can open directly, and `rematerialize` re-enqueues the durable job from the persisted spec when that URI has been evicted. The pair makes the "provenance records the spec, never the ephemeral URL" hard rule actually cash out for consumers: URIs are explicitly disposable because re-derivation is one tool call.

## User Stories

1. As a consumer application, I want `export_result(handle, workspace_id)` to return `{handle, storage_uri, media_type, size_bytes, rematerialize_hint}`, so that I can open materialized Zarr/Parquet/NetCDF with xarray/pyarrow.
2. As a consumer, I want the response shape identical whether the backend serves `file://` (local filesystem) or a presigned `https://` URL (S3), so that my open-by-scheme logic survives the move to hosted deployment unchanged.
3. As a consumer whose exported URI stopped resolving, I want `rematerialize(handle, workspace_id)` to return `{job_handle, obs_handle, status}` for a re-enqueued durable job, so that recovery is export → rematerialize → await → export, with no memory of the original tool arguments required.
4. As a consumer, I want a cache hit during rematerialization to complete near-instantly, so that recovering an evicted-but-recently-regenerable result doesn't cost a full provider round-trip.
5. As a consumer polling a still-running retrieval, I want `export_result` on a pending handle to answer with a structured not-ready status instead of raising, so that my agent loop treats it as a state, not a crash.
6. As a workspace owner, I want a handle from another workspace to collapse to the same not-found answer as a nonexistent handle, so that handle existence never leaks across workspace boundaries.
7. As the cache eviction policy, I want exports to LRU-touch the underlying key, so that results a consumer is actively reading are not the first evicted.
8. As a provenance consumer, I want exports to leave the ancestry graph untouched (a read, not a transform), so that lineage stays a faithful record of data derivation.

## Implementation Decisions

- Both tools resolve `obs_`/`cube_` handles through the existing materialized-resolution seam; `job_` handles are not accepted (callers await the job first — the status tool already hands them the `obs_` handle).
- `storage_uri` comes from the StorageBackend interface: local backend yields `file://`; an S3 backend yields a presigned GET URL. Never hard-code a backend (CLAUDE.md hard rule). The contract is scheme-agnostic by construction; consumers branch on scheme.
- `rematerialize_hint` is a fixed advisory string stating the URI is cache-managed and may be evicted, and naming `rematerialize` as the recovery path.
- `rematerialize` re-enqueues the durable job from the request spec already persisted with the job/handle — same enqueue path as the original retrieval, so restart-resume, provenance events, and the cache key all behave identically to a first run. It never invents a new spec and never accepts spec overrides (that would be a new retrieval, not a replay).
- No new provenance edges from either tool. At most an `exported` event outside the ancestry graph; nothing that changes lineage.
- Not-found / not-ready / evicted all follow the PRD 014 pattern: structured answers with a `status` discriminator, exceptions reserved for caller bugs (malformed handle prefixes).
- Register both tools alongside the existing tool registrations; docstrings follow the house style (contract-first, answer shapes spelled out).

## Testing Decisions

- Seam: the existing tool-function seam with injectable store/provenance/session/enqueue fakes — identical to how every other tool is unit-tested in this repo (prior art: the retrieval and provenance tool test files).
- Unit: local-path round-trip (export → path exists → contents readable); pending handle → structured not-ready; unknown and cross-workspace handles → byte-identical not-found; rematerialize on an evicted key → job re-enqueued with the original spec; rematerialize cache hit → immediate materialized status; LRU touch observable via the eviction-order seam.
- Integration (durable-pipeline harness): materialize via fake provider → export → open with xarray from the returned URI; then delete the key to simulate eviction → rematerialize → await → export again → open again.
- Gates: unit + integration suites fully green in the compose stack.

## Out of Scope

- HTTP transport and auth (PRD 018).
- `list_workspace` (PRD 019).
- Any S3 backend implementation — only the contract must be scheme-agnostic; the local backend is the one that ships.
- Consumer-side open/retry logic (Talking-to-Air PRD T03).

## Further Notes

Decision provenance: the rematerialize tool and the scheme-agnostic contract were decided in the 2026-07-04 grilling session (see the decision record in the research-platform plan, §8) after discovering `get_provenance` exposes ancestry and events but not the spec.
