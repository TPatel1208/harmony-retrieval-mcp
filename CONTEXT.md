# Domain Glossary

## Durable request spec / `RequestSpec`

A retrieval's lifecycle splits across two halves: the **planning half** (the
`retrieve_*` tools in `tools/retrieval.py`) and the **durable half** (the
stateless Arq worker in `jobs/worker.py`). The **durable request spec** is the
re-materializable record that bridges them — everything the worker needs to
resume a job from Postgres alone, with no in-memory state (CLAUDE.md hard
rule). It is never a staged-output URL; it is the request's *inputs*
(collection, format, AOI, time range, variables, routing decision, OPeNDAP
discovery outputs), re-derivable and safe to persist indefinitely.

`RequestSpec` (`providers/request_spec.py`) is the typed value object that
owns this contract:

- `RequestSpec.from_plan(...)` — built from a routed `RetrievalPlan` plus the
  router's `RoutingDecision` and the OPeNDAP discovery outputs (axis geometry,
  var-dims, resolved variable names, granule URLs).
- `to_jsonb()` / `from_jsonb()` — the durable (de)serialization to/from the
  `jobs.request_spec` JSONB column and the provenance edge, tolerant of
  already-persisted legacy specs (missing `output_format`, singular
  `opendap_url`, missing `provider`).
- `to_plan()` — reconstructs the `RetrievalPlan` a resumed job was built from.
- `cache_key()` — the materialization cache key, computed once and carried
  through the round-trip so already-materialized results keep resolving.

`providers.build(spec, caps)` (`providers/__init__.py`) is the paired seam:
the single spec → `RetrievalProvider` mapping (Harmony / OPeNDAP / AppEEARS),
consumed by the worker's `submit_job` / `poll_job` / `materialize_job` and by
`startup` resume. An unknown `spec.provider` raises rather than silently
falling back to Harmony.

## Failure legibility convention

Explanation facts are persisted at plan time; failure time only formats them.
A terminal failure must never re-derive anything with a fresh CMR/Harmony call
— everything it needs to explain itself (stage, provider, the caught
exception) is already on the durable `RequestSpec` or in hand at the point of
failure. `jobs/worker.py`'s `_fail_job` is the one choke point all three
lifecycle tasks (`submit_job`/`poll_job`/`materialize_job`) route through on
their way to `FAILED`: it builds the stage- and provider-prefixed stored error
string and records a `JOB_FAILED` provenance event with structured detail
(`stage`, `provider`, `error_type`, `message`). Branch-specific explanations —
like `OPENDAP_NOT_APPLICABLE`'s no-fallback-available event and its verbatim
error prefix — are recorded *in addition to* `JOB_FAILED`, not instead of it:
they explain a decision, `JOB_FAILED` records the terminal outcome.

**The failure path must never fail (PRD 004).** The state transition to
`FAILED` happens *first* and needs nothing but the job id; every provenance
write that explains the failure happens after, through `_record_event_safe`,
which skips (with a log line) when the spec has no `workspace_id`/handle to
key the event to, and otherwise wraps the write so any exception is logged and
swallowed rather than propagated. A secondary write can never re-create the
"job that was trying to fail can never fail" bug. Deserialization is hardened
the same way: `RequestSpec.from_jsonb` raises one typed `InvalidSpecError` for
a malformed nested payload instead of leaking a raw `KeyError`/`TypeError`
from whichever helper first touches the missing field, and every lifecycle
task (plus startup reclaim) catches it at the load seam and fails the job
cleanly instead of crashing the task. Startup reclaim also fails (rather than
re-enqueues) a non-terminal job older than `job_stale_after_days` — blind
re-enqueue-forever is how debris jobs accumulate.

## Speculative result / promotion

A **speculative result** is one nobody asked for: adjacent-granule prefetch
(`jobs/prefetch.py`, PRD 025) guessed that the next contiguous time window would
be requested and retrieved it in advance. It is marked `speculative: true` in
both places that matter — the `obs_` handle payload (what the cache and
`pin_result` read) and the `jobs.speculative` column (what the worker reads to
know a job must not prefetch again).

Speculative is a *standing*, not a flavour, and it is deliberately second-class:

- the eviction sweep (`jobs/eviction.py`) discards **every** speculative
  candidate before **any** requested one — a lexicographic first key ahead of
  size, rebuild cost and recency, never a weight;
- `pin_result` refuses it;
- it never chains: a speculative job reaching `READY` issues no prefetch of its
  own, so one retrieval can trigger at most one speculative step;
- it is dropped rather than deferred whenever user-requested work is in flight.

**Promotion** is the one transition out of that standing. When a user request
matches a speculative handle, `tools/retrieval._promote_if_speculative` clears
the flag on PRD 020's reuse path and records a `promoted` provenance event: the
guess turned out to be a real request, so the result is no more disposable than
one retrieved directly. What promotes a handle is *someone having asked*, not the
bytes having landed — a request that collapses onto a speculative job still in
flight promotes it too.

The pair `prefetch-issued` / `prefetch-hit` (plus `prefetch-wasted` for a
speculative result evicted unused) are structured log events, and their ratio is
the only honest answer to whether the feature earns its quota. That is why
promotion has to be recorded rather than merely applied.
