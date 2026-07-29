# PRD 025 — Adjacent-granule prefetch: warm the next request before it is made

**Repo:** harmony-retrieval-mcp · **Session scope:** one session, one commit · **Label:** ready-for-agent
**Depends on:** PRD 023 (materialization identity — prefetch must record what it fetched like any other job), PRD 024 (retention — prefetched data competes for the cap and must be scored *below* user-requested data). **Build last of the three.**
**Consumer:** requested by Talking-To-Air during its cache design (2026-07-28): "prefetching adjacent granules."

## Problem Statement

Research access is sequential and predictable, and the server does nothing with that.

A researcher who retrieves NO₂ over New Jersey for 1–7 April very often asks for 8–14 April next, or for the adjacent variable, or for the same week on the neighbouring collection. Every one of those is a full cold path: `retrieve_subset` → plan → provider job → poll → materialize → export. PRD 020's dedupe helps only when a request is *identical* to one already held — exact match only, by explicit decision, and a strict subset is a deliberate miss. The near-miss case, which is the common one in exploratory work, always pays full price.

The server has the information to anticipate this and does not use it: every `request_spec` records dataset, AOI, time range and variables, and the provenance graph records the order in which a workspace's handles were created.

The risk is equally clear, which is why this PRD is last of the three: prefetching spends provider quota, worker capacity and cache bytes on data nobody has asked for. Done carelessly it evicts results a researcher *did* ask for in order to hold speculative ones — strictly worse than not prefetching at all.

## Solution

After a retrieval reaches `READY`, enqueue **at most one** low-priority speculative job for the most likely next request, under strict, individually-checkable conditions.

**The single prediction: the next contiguous time window, same everything else.** A retrieval of `[T0, T1)` for a given collection, AOI and variable set predicts `[T1, T1 + (T1 - T0))`. Same length, immediately adjacent, nothing else varied. This is the pattern with the strongest signal and the least ambiguity, and it is deliberately the *only* prediction in v1 — adjacent-variable and adjacent-collection prediction are far weaker and belong behind evidence from this one.

**Prefetch is refused unless every gate passes:**

- The workspace is **under** its retention fair share (PRD 024) — never prefetch into a cache already under pressure.
- The predicted window is fully in the past relative to the collection's latency, so it is not an NRT window that will be re-fetched anyway when data lands.
- The predicted request's signature (`compute_request_signature`) is not already present in the workspace — reuse the dedupe machinery rather than a second notion of sameness.
- The estimated size is under a prefetch-specific cap, well below `size_warning_threshold_mb`.
- This workspace has not exceeded its prefetch budget for the current window.
- The originating job was **user-requested, not itself a prefetch.** No chaining: one speculative step, never a cascade.

**Prefetched results are second-class, explicitly.** They are marked `speculative: true`, scored **strictly below** any user-requested result in PRD 024's eviction ranking regardless of size or cost, and cannot be pinned. A prefetch may never evict something a researcher asked for. When a user request later matches a prefetched handle, PRD 020's reuse path returns it and the handle is **promoted** to ordinary status — at that moment it stops being speculative, because someone asked for it.

**Prefetch jobs run at the back of the queue** and are cancelled, not queued, whenever real work is waiting. A researcher's job must never wait behind a guess.

## User Stories

1. As a researcher stepping forward through consecutive time windows, I want the next window to often be ready when I ask for it, so that sequential exploration stops paying full provider latency every step.
2. As a researcher, I want a prefetch to never delay a request I actually made, so that speculation is free from my point of view.
3. As a researcher, I want speculative data evicted before anything I asked for, so that prefetching cannot cost me my own results.
4. As a researcher, I want a prefetched result I then request to become an ordinary result, so that it is no longer treated as disposable once it is real.
5. As an operator, I want a hit rate for prefetch, so that a feature that spends quota on guesses must justify itself with numbers.
6. As an operator, I want prefetch disabled by a single setting, so that a deployment with tight provider quota can simply turn it off — and so that it can ship **off by default**.
7. As an operator, I want prefetch to never chain, so that one retrieval cannot trigger an unbounded sequence of speculative jobs.
8. As a researcher on a restricted collection, I want prefetch to run under the same identity rules as any other retrieval, so that speculation cannot fetch anything I could not have fetched myself.

## Implementation Decisions

- **Ships disabled** (`earthdata_prefetch_enabled: bool = False`). A feature that spends provider quota on predictions earns its default by measurement, not by argument.
- Prediction lives in one pure, testable function: `predict_next(spec: RequestSpec) -> RequestSpec | None`. No I/O, no provider knowledge — trivially unit-testable and trivially extensible when evidence supports a second prediction.
- Enqueued through the existing durable job path (an Arq job like any other), never an in-memory background task — CLAUDE.md's hard rule that anything that matters is a durable job applies to speculation too.
- `speculative: true` in the handle payload and on the job row. PRD 024's scorer sorts all speculative candidates ahead of (i.e. evicted before) all non-speculative ones, before any cost or recency comparison.
- **Identity:** a prefetch runs under the originating job's credential scope, or as the service account where none survives (a terminal job's credential is erased — see PRD 022 and `_reenqueue_replay`). It must never grant access the original caller did not have; where identity is unavailable and the collection is restricted, the prefetch is simply skipped.
- Promotion on reuse: `_check_reuse`'s hit path clears `speculative` when a user request matches a prefetched handle, and records a provenance event so the transition is legible.
- Prefetch jobs carry a queue priority below all user work, and are dropped rather than deferred when the queue is non-empty.
- Metrics: `prefetch_issued`, `prefetch_hit` (a user request matched a prefetched handle), `prefetch_wasted` (evicted unused), plus bytes and provider-seconds spent. `prefetch_hit / prefetch_issued` is the number that decides whether this stays on.
- Settings: `earthdata_prefetch_enabled`, `earthdata_prefetch_max_bytes`, `earthdata_prefetch_budget_per_workspace_per_day`.

## Testing Decisions

- **Prediction:** a `[T0, T1)` spec predicts exactly `[T1, T1 + (T1 - T0))` with all other fields identical; a spec with no time range predicts `None`; a point-sample spec predicts `None`.
- **No chaining:** a prefetch job reaching `READY` issues no further prefetch — asserted directly, since an unbounded cascade is this feature's worst failure.
- **Gates, individually:** each of the six refusal conditions is asserted in isolation, so a future change to one cannot silently disable the others.
- **Never evicts real data:** under cap pressure with a speculative and a user-requested candidate, the speculative one goes first regardless of size, cost and recency.
- **Cannot be pinned:** `pin_result` on a speculative handle is refused with a structured message.
- **Promotion:** a user request matching a prefetched handle returns it via PRD 020's reuse path, clears `speculative`, and records the provenance event.
- **Never delays real work:** with a user job queued, a pending prefetch is dropped rather than run ahead of or alongside it.
- **Identity:** a prefetch never runs with a credential the originating request did not carry; a restricted collection with no available identity is skipped, not attempted.
- **Disabled by default:** with `earthdata_prefetch_enabled` unset, a completed retrieval enqueues nothing and every existing test passes unchanged.
- Per [CLAUDE.md](../../CLAUDE.md), tests run inside the `mcp` container. **Do not run `pytest tests/unit` casually — it executes `drop_all` against the configured database and has destroyed live job rows.**

## Out of Scope

Materialization identity (PRD 023) and retention policy (PRD 024) — both are prerequisites, not part of this. Adjacent-*variable* and adjacent-*collection* prediction; multi-step or ML-driven prediction; cross-workspace prediction. Prefetching into a cold tier. Speculative *transforms* (regrid/compare) on prefetched data — speculation stops at retrieval. Consumer-side prefetch hinting: if Talking-To-Air later wants to declare intent explicitly, that is a much stronger signal than inference and deserves its own PRD rather than being smuggled in here.

## Further Notes

Order matters: PRD 024's eviction scoring must exist before this ships, or speculative data competes with user data on equal terms and the first thing prefetching does is evict something a researcher asked for. PRD 023 matters because a prefetched result is a materialization like any other and must be describable — including for the case where the predicted window turns out to overlap late-arriving NRT granules, which is exactly the drift `content_digest` makes visible.
