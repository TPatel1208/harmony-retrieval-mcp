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
