# PRD: Signal when OPeNDAP fallback is unavailable, not just when it fires

> Triage label to apply when published: `ready-for-agent`

## Problem Statement

CLAUDE.md's hard rule states OPeNDAP is "the worker's runtime fallback when a real
Harmony submit fails." A researcher reading that rule reasonably expects any Harmony
submit failure to at least *attempt* OPeNDAP. In practice, whether that happens
depends on a planning-time fact the researcher never sees: whether `_submit_retrieval`
discovered an OPeNDAP granule URL for the collection at plan time
(`src/earthdata_mcp/tools/retrieval.py:339-346`, gated on `output_shape in ("grid",
"swath")` and a bbox being present).

A variable-subset request against MCD19A2 (MODIS AOD, LPCLOUD) failed with "variable
subsetting on C2324689816-LPCLOUD is unsupported" and the job died — no fallback
attempt, no explanation. The same shape of request against MERRA-2 (GES_DISC)
correctly fell back through OPeNDAP/Hyrax. From a researcher's perspective this reads
as "fallback is broken" or "inconsistent across collections."

It isn't broken: LPCLOUD's granules genuinely have no Hyrax/OPeNDAP endpoint, so
`opendap_urls` is legitimately empty and `worker.py:106`'s fallback condition
(`spec.provider == "harmony" and spec.opendap_urls`) correctly declines to retry
something that cannot succeed. The actual gap is diagnostic: when that condition is
false, the job's `FAILED` state stores only the raw Harmony exception
(`worker.py:132-136`, `error=str(exc)`) with nothing recording *that OPeNDAP was
considered and found unavailable for this collection*, or why. A researcher comparing
two failed/succeeded jobs side by side has no way to tell "OPeNDAP wasn't tried
because none exists here" apart from "OPeNDAP wasn't tried because of a bug."

## Solution

When a Harmony submit fails and the worker's existing fallback condition
(`spec.provider == "harmony" and spec.opendap_urls`) evaluates false — i.e., no
OPeNDAP URL was discovered at plan time — record a provenance event and enrich the
job's stored error so both `get_provenance` and `get_retrieval_status` make the
non-fallback explicit and explain why, using the same information already computed at
plan time (collection shape, bbox presence, granule search result) rather than
re-deriving anything new.

## User Stories

1. As a researcher, I want a Harmony failure on a collection with no OPeNDAP endpoint
   to say so explicitly, so that I don't mistake "no fallback exists here" for "the
   fallback path is broken."
2. As a researcher, I want the same failure signal whether the cause is "no OPeNDAP
   endpoint for this collection" or "OPeNDAP was never checked because the plan
   wasn't gridded/swath+bbox," so that I get an accurate reason either way.
3. As a researcher comparing a failed MCD19A2 job against a successful MERRA-2 job, I
   want `get_provenance` to show me the fallback decision was made (attempted vs. not
   applicable) rather than a bare terminal exception, so that I can trust the
   fallback rule is being applied consistently even when it doesn't apply.
4. As a researcher, I want `get_retrieval_status` on the failed job to include the
   same "OPeNDAP not available for this collection" context inline, so that I don't
   have to cross-reference `get_provenance` just to understand why my job died.
5. As a maintainer, I want the new signal to reuse `spec.opendap_urls`'s existing
   plan-time discovery result rather than re-querying CMR or re-running
   `plan_subset` at failure time, so that a failing job doesn't cost an extra
   round-trip on top of the failure it's already reporting.
6. As a maintainer, I want a job that fails with `spec.provider != "harmony"` (e.g. a
   direct AppEEARS or OPeNDAP-primary failure) to be unaffected by this change, so
   that the new signal is scoped to the Harmony-then-maybe-OPeNDAP path only, not
   every failure path in the worker.
7. As a researcher, I want a job that *does* successfully fall back to OPeNDAP to
   continue producing the existing `PROVIDER_FALLBACK` → `SUBMITTED` trail unchanged,
   so that this PRD only adds a new outcome, not alters the one that already works.

## Implementation Decisions

- **New enum value, no migration.** Add `OPENDAP_NOT_APPLICABLE` to
  `ProvenanceEventType` (`src/earthdata_mcp/workspace/models.py:56-64`). Same as the
  prior `SUBMITTED`/`PROVIDER_FALLBACK` addition: `ProvenanceEvent.event_type` is a
  plain `String` column, so this requires no Alembic migration. Update the class's
  docstring listing to include the new value.
- **Fires in `submit_job`'s existing exception handler, on the branch that currently
  falls through to `FAILED`.** In `worker.py:105-136`, the `if spec.provider ==
  "harmony" and spec.opendap_urls:` branch is unchanged. Add an `elif spec.provider ==
  "harmony":` branch (i.e., Harmony failed and `spec.opendap_urls` is falsy) that
  records `OPENDAP_NOT_APPLICABLE` before falling through to the existing
  `transition_state(..., JobState.FAILED, ...)` call. Jobs where `spec.provider !=
  "harmony"` are untouched — they fall through to the same unconditional `FAILED`
  transition as today.
- **Event detail carries the Harmony error plus the plan-time shape/bbox facts
  already on `spec`**, not a new CMR lookup: `detail={"harmony_error": {"error_type":
  type(exc).__name__, "message": str(exc)}, "reason": "no_opendap_endpoint_discovered",
  "output_shape": spec.output_shape, "had_bbox": spec.aoi is not None}`. The exact
  field names for shape/bbox should match whatever `RequestSpec` already exposes
  (`spec.to_plan()`'s `RetrievalPlan` carries `concept_id`, `aoi`, and the collection's
  `output_shape` is available via the same `caps` used in `_submit_retrieval`; thread
  through whichever of these is already present on `RequestSpec` rather than adding a
  new field to carry it, since `opendap_urls` being empty is itself computed from
  these same inputs at plan time).
- **`crud.transition_state`'s stored `error` string gets a human-readable prefix** on
  this branch only: instead of bare `str(exc)`, store `f"Harmony failed and no
  OPeNDAP fallback is available for this collection: {exc}"`. This is the string
  `get_retrieval_status` already surfaces for a `FAILED` job, so this single change
  satisfies user story 4 without `get_retrieval_status` itself needing new code.
- **No change to the fallback condition itself.** `spec.opendap_urls` remains the
  single source of truth for whether OPeNDAP is attempted; this PRD only makes the
  "false" branch legible, it does not change when the branch is taken. In particular
  this PRD does not attempt to discover OPeNDAP endpoints for collections whose
  `output_shape` isn't `"grid"`/`"swath"`, or for point-sample plans — that discovery
  gap (if it is one) is out of scope, see below.

## Testing Decisions

- Good tests here assert on the durable record (`get_provenance`'s `events`, the
  job's stored `error` string) — external behavior a caller can observe — not on
  internal call counts or mocks of `plan_subset`.
- **Unit: `OPENDAP_NOT_APPLICABLE` recorded when Harmony fails with no
  `opendap_urls`.** A job whose `spec.provider == "harmony"` and `spec.opendap_urls ==
  []` fails its submit and records exactly one `OPENDAP_NOT_APPLICABLE` event (no
  `PROVIDER_FALLBACK`), then transitions to `FAILED`.
- **Unit: existing `PROVIDER_FALLBACK` path is unaffected.** A job whose
  `spec.opendap_urls` is non-empty still records `PROVIDER_FALLBACK` → `SUBMITTED` as
  today, with no `OPENDAP_NOT_APPLICABLE` event. Guards against the new `elif`
  accidentally widening the existing condition.
- **Unit: non-Harmony provider failure is unaffected.** A job with `spec.provider ==
  "opendap"` (or `"appeears"`) that fails submit records neither
  `PROVIDER_FALLBACK` nor `OPENDAP_NOT_APPLICABLE`, and transitions to `FAILED` with
  the bare `str(exc)` error string, unchanged from current behavior.
- **Unit: `FAILED` job's `error` field carries the human-readable prefix** on the
  `OPENDAP_NOT_APPLICABLE` branch, and the raw Harmony exception text is still
  present somewhere in the string (so nothing informational is lost, only prefixed).
- **Integration: extend the existing Harmony-failure integration test** (the
  fallback-path variant referenced in
  `docs/prd/record-provenance-events-and-validate-resample-time-freq.md`'s testing
  section, `tests/integration/test_durable_pipeline.py`) with a sibling case: Harmony
  submit fails, `spec.opendap_urls` is empty, assert the job ends `FAILED` with
  `get_provenance` showing `OPENDAP_NOT_APPLICABLE` and `get_retrieval_status`
  showing the prefixed error string.
- Run via the project's Docker test workflow per project rules: `docker compose exec
  mcp python -m pytest tests/unit -v` for unit tests, equivalent invocation against
  `tests/integration` for the integration test. The local Python environment lacks
  dependencies and DB access.

## Out of Scope

- **Actually discovering an OPeNDAP endpoint for collections that currently have
  none** (e.g. investigating whether LPCLOUD collections have any Hyrax presence at
  all, or extending discovery to point-sample/non-grid-swath plans). This PRD only
  makes the current "not applicable" outcome legible; it does not change which
  collections get OPeNDAP discovery.
- **`cancel_retrieval`'s no-op-on-terminal-job behavior.** Re-confirmed intentional
  and already documented; not touched here (see prior PRD's Out of Scope for the
  same conclusion).
- **`get_provenance` events being empty.** Already fixed (commit `45febb4`,
  `SUBMITTED`/`PROVIDER_FALLBACK`/`MATERIALIZED` are fully wired); this PRD adds one
  more event type to that same machinery, it does not re-fix the original gap.
- **The three other correctness items from the same triage round**
  (`estimate_retrieval_size`'s missing size-threshold warning,
  `retrieve_timeseries(point_sample=True)`'s post-job-creation AppEEARS catalog
  check, `preview_dataset`'s GIBS layer-guessing heuristic) — explicitly deferred by
  the reporting user to their own scoping, not bundled into this PRD.
- **The four usability/performance items from the same triage round** (AOI
  geocoding payload size, `describe_dataset` citation/variable bloat,
  `check_coverage`/`inspect_granules` duplication, unhelpful job-not-found error) —
  heterogeneous in owner/severity, to be split into their own ticket(s) rather than
  bundled here, matching the project's existing precedent of not bundling
  design/performance issues with correctness fixes.

## Further Notes

- Source: bug-report triage session (`/diagnosing-bugs` + `/grill-me`) covering 10
  findings across correctness and usability; this PRD covers exactly one of the
  correctness findings (item 1, "retrieve_subset doesn't fall back to OPeNDAP on
  Harmony failure") as scoped by the reporting user, 2026-07-02.
- Per project hard rules: OPeNDAP remains the worker's runtime fallback, never a
  planning-time choice; provenance records the re-materializable spec and, per the
  prior PRD, first-class lifecycle events — this PRD extends that event vocabulary,
  it does not introduce a new provenance mechanism.
