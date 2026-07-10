# PRD 020 — Idempotent `retrieve_subset`: request-signature dedupe

**Repo:** harmony-retrieval-mcp · **Session scope:** one session, one commit · **Label:** ready-for-agent
**Depends on:** PRD 017 (`export_result`/`rematerialize` — the recovery path a deduped handle leans on), PRD 019 (`list_workspace`).

## Problem Statement

As a researcher using a consumer app (Talking-to-Air), asking the same question in a new conversation refetches data the server already holds. My workspace persists across threads (`user-{id}`), and the handle from last Tuesday's "NO2 over New Jersey, April 1–7" retrieval is still materialized — but the consumer's model has no memory of handles across conversations, so it innocently submits an identical `retrieve_subset`, launches a new provider job, and I wait minutes for bytes that are already on disk.

As the server, I persist every request spec (for `rematerialize`) but never *read* the specs at submit time, so I cannot recognize a repeat. The knowledge to dedupe is already stored; no code path consults it.

## Solution

`retrieve_subset` becomes idempotent per workspace: at submit, compute a canonical signature of the request, look it up among the workspace's existing handles, and on an **exact match** return the existing handles immediately with `"reused": true` instead of enqueueing a new job. A miss proceeds exactly as today. If a matched handle's data was evicted, reuse still returns the same handle identity and re-enqueues via the same durable-replay path `rematerialize` uses — the caller sees one consistent contract either way.

Exact match only (decision 2026-07-10): a requested time range that is a strict subset of an existing handle's range is a **miss**. Subset-reuse changes `n_granules` and provenance in user-visible ways for a marginal saving; it is deliberately out.

## User Stories

1. As a researcher, I want a repeat of a prior retrieval (same dataset, AOI, time range, variables, format) to come back near-instantly with the existing handle, so that repeated analysis on repeated datasets never re-waits on the provider.
2. As a consumer application, I want the reused response shape to be a superset of the normal response (same handle fields, plus `"reused": true`), so that my composites need no branching to benefit.
3. As a provenance consumer, I want reuse recorded as an event on the existing handle — never a new ancestry node — so that lineage stays a faithful record of derivation and I can see the data was served from cache.
4. As a researcher whose matched handle was evicted, I want the reuse path to re-enqueue the durable job from the persisted spec (rematerialize semantics) and hand me the same handle, so that eviction is invisible to my workflow.
5. As a workspace owner, I want signatures scoped to my workspace, so that another user's identical request never hands them my handle (and never leaks that it exists).
6. As a consumer agent that accidentally double-fires the same retrieval within one conversation, I want the second call to collapse onto the first (including while the first job is still running), so that duplicate jobs never race.
7. As a researcher requesting a *near*-miss (subset time range, one variable fewer), I want a genuinely new retrieval, so that my provenance reflects exactly what I asked for.

## Implementation Decisions

- **Canonical signature** — a hash over: dataset concept-id (resolved, not the handle string), AOI geometry (canonical WKB of the *resolved* geometry, so two `aoi_` handles for the same polygon match), normalized time bounds (UTC ISO-8601, one inclusive/exclusive convention), sorted deduplicated variable list, output format. Workspace id scopes the lookup, not the hash input.
- **Audit before building:** the durable-job pipeline already has a cache key on the request spec (PRD 017 noted rematerialize cache hits). If that key is (or can be made) equivalent to the signature above, the index reduces to a handle-level lookup over existing state rather than a parallel structure. Prefer extending it over adding a second notion of request identity.
- **Match → reuse, three states:** materialized → return existing `obs_`/`cube_` handle, `status: ready`, `reused: true`; job still running → return the existing `job_handle`, `reused: true` (story 6); evicted → same handle identity, re-enqueue via the rematerialize path, return the new `job_handle` with `reused: true`.
- **Exact match only.** No subset/superset reasoning, no geometry tolerance beyond canonicalization. Near-misses are misses.
- **Provenance:** a `reused` event on the existing handle (like `exported` — outside the ancestry graph). Never a new lineage node for a reuse.
- **No opt-out parameter for v1.** If a forced refetch is ever needed (upstream data corrected), that is a deliberate new tool argument in a future PRD, not a default-off flag nobody discovers.

## Testing Decisions

- Seam: the existing tool-function seam with injectable store/enqueue/provenance fakes, as in every retrieval-tool test.
- Unit — signature canonicalization is the load-bearing surface: variable order/duplication irrelevant; equivalent time-bound spellings collide; same polygon via two different `aoi_` handles collides; differing output format does not collide; strict-subset time range does **not** collide (the exact-match rule, pinned by test).
- Unit — reuse states: materialized hit returns ready + `reused: true` with no enqueue call observed; running hit returns the in-flight job handle; evicted hit re-enqueues with the *original* spec (spec equality asserted) and keeps handle identity; cross-workspace identical request observes zero reuse.
- Integration (durable-pipeline harness): retrieve → await → retrieve identical → assert exactly one provider job ran and the second call returned in one round-trip; then evict the key → retrieve identical → assert rematerialize-equivalent replay.
- Gates: unit + integration suites fully green in the compose stack.

## Out of Scope

- Subset/superset reuse (time or spatial containment) — explicitly rejected for v1, revisit only with evidence the exact-match hit rate is too low.
- Cross-workspace/global dedupe (shared immutable data pool) — a storage-architecture change, not a tool change.
- Consumer-side handle discovery (`list_workspace`-based recall in the consumer's prompt) — the consumer may add it later; this PRD makes it unnecessary for the repeat-request case.
- `retrieve_timeseries` dedupe — same pattern, separate follow-up once this proves out; point-sample signatures need their own canonicalization thought.
- Any TTL/eviction policy change.

## Further Notes

Decision provenance: 2026-07-09/10 grilling session (Talking-to-Air repo). The consumer's original framing was "move file storage client-side so TTA can cache"; the session established that bytes are already cached server-side and keyed by handle — the missing piece is request-identity recognition, which belongs next to the workspace that owns the requests. The scheme-agnostic egress work (S3 backend, non-`file://` consumer opens) remains parked until a split deployment actually triggers it.
