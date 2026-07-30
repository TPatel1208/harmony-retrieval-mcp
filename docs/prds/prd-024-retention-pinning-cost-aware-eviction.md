# PRD 024 — Retention: pinning, cost-aware eviction, and per-workspace fairness

**Repo:** harmony-retrieval-mcp · **Session scope:** one session, one commit · **Label:** ready-for-agent
**Depends on:** PRD 015 (the eviction service this replaces the policy inside), PRD 017 (`export_result`'s LRU touch), PRD 020 (reuse — which this makes far more likely to hit).
**Consumer:** requested by Talking-To-Air during its cache design (2026-07-28): "longer TTLs, pinning frequently accessed granules, smarter eviction (LRU/LFU)."

## Problem Statement

The materialization cache evicts the results that are most expensive to rebuild, as readily as the ones that are cheapest, and one workspace's activity evicts another's data.

`EvictionService` ([src/earthdata_mcp/jobs/eviction.py](../../src/earthdata_mcp/jobs/eviction.py)) enforces a single global byte cap (`earthdata_cache_max_bytes`, default 5 GiB, [config.py:46-47](../../src/earthdata_mcp/config.py)) by evicting least-recently-used `ready` `obs_`/`cube_` handles, ordered by `updated_at`, until back under the cap. It is correct and clearly written. Its policy has four gaps:

1. **Eviction is cost-blind.** A 2 GiB Harmony result that took fifteen minutes and a provider job to build is evicted on exactly the same footing as a 2 MiB OPeNDAP subset that rebuilds in seconds. The cap measures *bytes held*; the thing worth minimising is *time to rebuild what we threw away*. Evicting the expensive one to keep the cheap one is a strictly worse trade, and it is the current behavior whenever the expensive one is slightly older.
2. **Nothing can be pinned.** The only protection is `protected_handle` — the single result whose own materialization triggered the sweep ([eviction.py:64-71](../../src/earthdata_mcp/jobs/eviction.py)) — plus `export_result`'s `touch_handle`, which only bumps recency. A result a researcher returns to weekly has no way to say so, and loses to anything touched more recently.
3. **The cap is global, so eviction is unfair.** Handles are selected across all workspaces ([eviction.py:97-103](../../src/earthdata_mcp/jobs/eviction.py)) with no per-workspace accounting. One user running a heavy week evicts every other user's results. With per-user workspaces (`user-{id}`) and per-user credentials (PRD 022), that is a cross-tenant effect nothing else in this server permits.
4. **Recency is imprecise.** LRU orders by `Handle.updated_at`, which `touch_handle` bumps on read ([workspace/store.py:201-218](../../src/earthdata_mcp/workspace/store.py)) — but which `update_handle` also bumps on any *status write*. A status transition therefore looks identical to a read, so a handle can be "recently used" without anyone having used it.

There is also a scalability cost worth fixing while the policy is open: every sweep calls `storage.stat(key)` for **every** ready handle ([eviction.py:107-113](../../src/earthdata_mcp/jobs/eviction.py)), and `_reclaim_orphans` lists every blob under four prefixes and materialises the full referenced-key set ([eviction.py:75-91](../../src/earthdata_mcp/jobs/eviction.py)), on every sweep. Both are O(cache) per materialization.

## Solution

Replace the eviction *policy* — not the service's shape, which stays a single `sweep()` the worker alone calls — with a cost-aware score, an explicit pin, and per-workspace fairness.

**Eviction score.** Rank candidates by *value density*: how much is reclaimed per unit of rebuild cost, discounted by how long since last genuine use.

```
score = size_bytes / (rebuild_cost_seconds * recency_weight)
```

Evict the highest score first — biggest reclaim for the least rebuild pain, oldest first among comparable candidates. `rebuild_cost_seconds` is the original job's measured wall-clock from submission to `READY`, already knowable at the transition and cheap to persist. This subsumes LRU (recency is a factor, not the sole key) and is a better answer than LFU, which would need a new access counter and still would not distinguish a cheap frequent read from an expensive rare one.

**Explicit pinning.** A `pin_result(handle, pinned: bool)` tool marking a handle exempt from cap-driven eviction, with a per-workspace pinned-bytes ceiling so pinning cannot be used to monopolise the cache. Pinned bytes still count toward the global cap; they are simply never selected. If pinned bytes alone exceed the cap, log loudly and evict nothing — a full cache is recoverable, silently discarding data a user explicitly protected is not.

**Per-workspace fairness.** Evict from the workspace furthest over its fair share (cap ÷ active workspaces) before touching a workspace under it. A single-tenant deployment behaves exactly as today; a multi-tenant one stops letting one user evict another.

**Precise recency.** Add `last_accessed_at`, written only by `touch_handle`, and score on that. `updated_at` keeps meaning "row changed."

**A raised cap is the trivial part.** 5 GiB is small for this workload and should be raised — but raising it without fixing the policy just means the wrong things get evicted later. The cap is a setting; this PRD is about what happens when it is reached.

## User Stories

1. As a researcher, I want the result that took fifteen minutes to build to outlive the one that takes three seconds, so that eviction costs me the least possible time.
2. As a researcher with a dataset I return to across weeks, I want to pin it, so that it is there when I come back rather than evicted by my own unrelated work.
3. As a researcher sharing a deployment, I want another user's heavy week to not evict my results, so that cache behavior does not depend on strangers.
4. As a researcher, I want a pinned result to be reported as pinned, so that "why is this still here" and "why is this gone" both have answers.
5. As an operator, I want the cap raisable by configuration, so that a deployment with disk can simply use it.
6. As an operator, I want every eviction to record what it cost — bytes reclaimed and the rebuild seconds discarded — so that cap sizing is driven by data.
7. As an operator, I want a sweep's cost to not grow linearly with cache size, so that a large healthy cache does not slow every materialization.
8. As a consumer, I want fewer evictions overall, so that PRD 020's reuse path and `export_result` hit far more often than they rematerialize.

## Implementation Decisions

- Persist `rebuild_cost_seconds` on the job row at the `READY` transition (`updated_at - created_at`), and `size_bytes` on the handle payload at the same moment. The latter removes the per-sweep `storage.stat` storm — `export_result` already prefers a persisted `size_bytes` and only stats as a fallback ([tools/export.py:127-129](../../src/earthdata_mcp/tools/export.py)); eviction should do the same.
- Missing `rebuild_cost_seconds` (legacy rows) defaults to a configured median rather than zero — zero would make legacy results infinitely evictable, silently purging exactly the oldest and most established data.
- `last_accessed_at` is a new nullable column, written only by `touch_handle`. Null falls back to `updated_at`, so behavior is unchanged until reads start recording.
- Pinning is a durable flag in the handle payload, so it survives restarts, plus `pinned_bytes_max_per_workspace` in config. `list_workspace` reports `pinned` per handle.
- `pin_result` is workspace-scoped with the same isolation contract as `get_handle`/`touch_handle` — an unknown or cross-workspace handle collapses to the same `not_found` answer so existence never leaks (PRD 014).
- Orphan reclamation moves off the per-materialization path onto a periodic sweep (it is debris cleanup, not admission control), so a normal sweep no longer lists every blob under four prefixes.
- New settings: `earthdata_cache_max_bytes` (existing, raise the default), `earthdata_pinned_bytes_max_per_workspace`, `earthdata_rebuild_cost_default_seconds`, `earthdata_orphan_sweep_interval_seconds`.
- Structured eviction log/metric carrying handle, workspace, bytes reclaimed, `rebuild_cost_seconds` discarded, and score — so cap sizing becomes an evidence question.

## Testing Decisions

- **Cost awareness:** given an expensive-old and a cheap-new candidate that cannot both fit, the cheap one is evicted — the direct inversion of today's behavior, asserted head-on.
- **Pinning:** a pinned handle is never evicted under cap pressure; an unpinned one of identical size and age is; unpinning restores eligibility.
- **Pin ceiling:** pinning past `pinned_bytes_max_per_workspace` is refused with a structured message, not silently ignored.
- **Pins exceed cap:** nothing is evicted, a loud warning is logged, and no data is lost.
- **Fairness:** with two workspaces over cap, eviction takes from the one furthest over its share; a single-workspace deployment evicts identically to today.
- **Recency precision:** a status-only `update_handle` does **not** make a handle look recently used; a `touch_handle` does.
- **Legacy rows:** handles with no `rebuild_cost_seconds`/`last_accessed_at` are scored via the configured defaults and are not preferentially purged.
- **Protection preserved:** the in-flight `protected_handle` is still never evicted by its own sweep (PRD 015's invariant).
- **Sweep cost:** a sweep over N ready handles issues no `storage.stat` call for any handle with a persisted `size_bytes`.
- Per [CLAUDE.md](../../CLAUDE.md), tests run inside the `mcp` container. **Do not run `pytest tests/unit` casually — it executes `drop_all` against the configured database and has destroyed live job rows.**

## Out of Scope

Materialization identity / `content_digest` (PRD 023). Granule prefetching (PRD 025). A true TTL — expiry by age is the wrong axis here; the cache is bounded by bytes and should evict by value, and an unused-for-N-days sweep can be added later if evidence calls for it. Tiered or cold storage. Cross-workspace deduplication of identical materializations. Changing the `StorageBackend` interface — LRU explicitly does not widen it for storage mtime, and this PRD keeps that discipline.

## Further Notes

The consumer's own cube cache (Talking-To-Air PRD T52) makes the same trade in the opposite direction: it caches *derived* artifacts locally so that an eviction here costs less. The two are complementary, and this PRD's success metric is visible from there — a drop in `rematerialize` calls and a rise in `export_result` ready-first-try. PRD 020's reuse path also benefits directly: a request signature can only be reused while the handle it points at is still materialized, so every eviction avoided is a potential reuse gained.
