# PRD 021 — Job metadata enrichment + provider-side cancel

**Repo:** harmony-retrieval-mcp · **Session scope:** one session, one commit · **Label:** ready-for-agent
**Depends on:** PRD 019 (`list_workspace`, job state machine), PRD 017 (materialization/export). Consumed by Talking-to-Air T27; follows TTA T26 (which repairs the consumer against today's contract and adds the live contract test this PRD's shapes must satisfy). Phase 2 of 3 (TTA T26 → PRD 021 → TTA T27).

## Problem Statement

As the jobs-panel consumer, I can list a workspace's jobs and read their live status, but I cannot tell a researcher *what a job is* or *stop it for real*. Two gaps:

1. **`get_retrieval_status` is status-only.** It returns state/progress/phase/message/handles, but not what the job pertains to — dataset, variables, area, time window, provider, output format, size. All of that already sits in the durable `jobs.request_spec` column the tool loads anyway (`crud.get_job_by_handle`), but none of it is surfaced. The consumer is forced to render a job card with a bare handle and a spinner.
2. **`cancel_retrieval` never reaches the provider.** It flips the durable state to `CANCELLED` and the worker stops polling/materializing — a real local resource release — but it makes no provider-side cancel call. The upstream Harmony (and AppEEARS) job keeps computing on NASA's servers to completion. The `RetrievalProvider` Protocol has no `cancel` method at all. "Stop this job" today means "stop chasing it," not "stop it."

## Solution

Enrich `get_retrieval_status` with a curated, immutable slice of `request_spec`, and add a `cancel` seam to the `RetrievalProvider` Protocol so `cancel_retrieval` propagates a best-effort provider-side cancel after it flips local state — real for Harmony and AppEEARS, a documented no-op for the synchronous OPeNDAP provider.

## User Stories

1. As the jobs-panel consumer, I want `get_retrieval_status` to include the dataset short name, variables, AOI bbox, time range, provider, output format, and granule count, so that a job card says what the job pertains to without me resolving other handles.
2. As a researcher who cancels a running Harmony job, I want the actual Harmony job cancelled at NASA (not just locally forgotten), so that I stop paying for compute I no longer want.
3. As a researcher who cancels a running AppEEARS task, I want the task deleted at AppEEARS, so that the same intent holds across providers.
4. As the consumer, I want the cancel response to say what happened upstream (requested / unsupported / already-terminal / error), so that I can honestly tell the user whether provider compute was released — without the local cancel ever failing on an upstream hiccup.
5. As a maintainer, I want OPeNDAP (a synchronous provider with no durable upstream job) to conform to the same `cancel` seam as a documented no-op, so that the Protocol stays uniform and future providers have one contract to satisfy.

## Implementation Decisions

- **`get_retrieval_status` enrichment (`tools/retrieval.py`):** add a curated slice derived from the already-loaded `job.request_spec` — `short_name`, `variables`, `aoi_bbox`, `time_range`, `provider`, `output_format`, and `granule_count` (= `len(request_spec["opendap_urls"])`, or the provider's equivalent granule tally). Never the raw spec: the `opendap_urls` list alone can carry dozens of URLs and must not cross the wire. Fields absent from a given spec are simply omitted. `list_workspace` stays a thin handle-enumerator — it does *not* grow a jobs-table join.
- **`cancel` on the Protocol (`providers/base.py`):** add `async def cancel(self, job: JobRef) -> None` to `RetrievalProvider`. Contract: best-effort, idempotent, must not raise on an already-terminal or unknown upstream job.
  - **Harmony (`providers/harmony.py`):** call the official harmony-py client's `cancel(job_id)` (CLAUDE.md hard rule: never hand-roll a Harmony client). The provider owns only the JobRef → `job_id` mapping and error classification.
  - **AppEEARS (`providers/appeears.py`):** `DELETE {base}/task/{provider_job_id}` through the existing authed `_request` path.
  - **OPeNDAP (`providers/opendap/_runtime.py`):** documented no-op — the service is synchronous, there is no durable upstream job to cancel; the docstring states so.
- **`cancel_retrieval` ordering (`tools/retrieval.py`):** keep today's local-state semantics (flip to `CANCELLED`, worker stops), then attempt `provider.cancel(job_ref)` **best-effort**. A provider-cancel failure never fails the tool — the user's local intent is already satisfied. Return a new `upstream` field alongside the existing shape: `"requested"` (provider cancel issued), `"unsupported"` (OPeNDAP / no upstream job), `"already_terminal"` (job finished/failed before the call landed), or `"error"` (provider cancel raised — logged, surfaced, non-fatal). The existing `{job_handle, status, cancelled, ...}` contract and the PRD 014 not-found/terminal/non-cancellable answers are unchanged.

## Testing Decisions

- **Enrichment:** hermetic tool-seam test with a job whose `request_spec` carries the full field set — assert the curated keys appear and `opendap_urls` (raw) does not; assert a spec missing a field omits it rather than emitting null.
- **Provider cancel:** per-provider unit tests with a fake client/HTTP — Harmony asserts `client.cancel(job_id)` is called with the mapped id; AppEEARS asserts a `DELETE /task/{id}`; OPeNDAP asserts the no-op returns cleanly. Each asserts idempotence on an already-terminal job.
- **`cancel_retrieval` orchestration:** local state flips to `CANCELLED` *and* the provider cancel is attempted; a raising provider cancel yields `upstream: "error"` with the local cancel still succeeding; a terminal-before-call job yields `upstream: "already_terminal"`.
- **Live matrix (opt-in, credentialed):** extend the PRD 019 matrix so at least one Harmony collection is submitted, cancelled mid-flight, and observed to reach a cancelled/terminal state at the provider.

## Out of Scope

- Pause/resume (harmony-py exposes them, but the consumer asked only for cancel).
- Surfacing per-granule progress or byte counts beyond the granule tally.
- Any consumer-side rendering — Talking-to-Air T27 owns the card, the result jump-off, and the cancel UX.
- Changing `list_workspace`'s summary shape (TTA T26's live contract test pins it; leave it thin).

## Further Notes

The enrichment is nearly free — the row is already loaded in `get_retrieval_status`; this is a curated projection, not new I/O. The cancel seam is the substantive change: it introduces the first Protocol method that mutates provider-side state, so the best-effort/idempotent contract in `base.py`'s docstring is the load-bearing decision — every future provider must honor it. Land after TTA T26 so its live contract test is green against the enriched shape.
