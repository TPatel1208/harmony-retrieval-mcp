# PRD: Record job-lifecycle provenance events and validate `resample`'s `time_freq`

> Triage label to apply when published: `ready-for-agent`

## Problem Statement

Two bugs surfaced during air-quality-researcher testing of the Earthdata MCP across gridded L3/L4 (MERRA-2, TROPOMI CONUS), swath L2 (MODIS MOD04_L2), and tabular (AppEEARS) datasets. The original report listed 11 findings; #1 and #2 were confirmed already fixed (in `cedf573`/`2f18855`) before this PRD was scoped, and #4 was confirmed to be intentional behavior, not a bug — both are covered under Out of Scope, along with the six design/performance issues (#6-11), which are heterogeneous enough to warrant their own tickets rather than bundling here.

**`get_provenance` returns an empty `events` list for every completed job, including ones that fell back from Harmony to OPeNDAP.** CLAUDE.md's hard rule is that provenance must be durable and re-materializable, and specifically records the request spec. Today that's honored at the edge level (`record_edge`, called once when a job is created) but the `ProvenanceEvent` machinery — the table, the `ProvenanceEventType` enum, `ProvenanceStore.record_event` — exists and is fully wired for read access (`get_provenance`), yet nothing in the worker's job lifecycle (`src/earthdata_mcp/jobs/worker.py`) ever calls `record_event`. A job that silently fell back from Harmony to OPeNDAP — the exact scenario CLAUDE.md names as the expected, common case under unpinned submission — has no record of that decision: not that it happened, not why, not which provider ultimately produced the result. From a researcher's perspective: "my subset succeeded, but I have no way to find out whether it came from Harmony or OPeNDAP, or why."

**`resample`'s `time_freq` parameter leaks a raw pandas exception with no guidance.** `src/earthdata_mcp/tools/transform.py:165` calls `result.resample(time=time_freq).mean()` directly against the caller-supplied string. An invalid or deprecated alias (e.g. `"M"`, deprecated in favor of `"ME"`) raises pandas' own internal error message verbatim, and neither the tool's docstring nor its MCP schema documents what frequency strings are accepted. A caller has no way to know "monthly" → `"ME"` without trial and error.

## Solution

**Provenance events:** add `SUBMITTED` and `PROVIDER_FALLBACK` to `ProvenanceEventType` (`src/earthdata_mcp/workspace/models.py:56-62`) alongside the existing `CREATED`/`MATERIALIZED`/`EXPIRED`/`RE_MATERIALIZED`. Wire three `record_event` calls into the worker's job lifecycle (`src/earthdata_mcp/jobs/worker.py`):
- `SUBMITTED`, fired at the point `submit_job` transitions a job's state to `JobState.SUBMITTED` (i.e., on the pass whose `provider.submit()` call actually succeeds — whichever provider that turns out to be).
- `PROVIDER_FALLBACK`, fired in `submit_job`'s exception handler at the moment a Harmony submit failure triggers the OPeNDAP re-route, before the job is re-enqueued.
- `MATERIALIZED`, fired in `materialize_job` immediately before the job transitions to `JobState.READY`.

The worker gains a `ctx`-cached `_provenance(ctx)` helper mirroring the existing `_session_factory(ctx)` pattern, rather than reusing `tools/retrieval.py`'s module-global singleton.

**`resample` validation:** wrap the `result.resample(time=time_freq).mean()` call in a `try`/`except`, catching pandas' exception and re-raising a `ValueError` with a clear message and 2-3 example valid frequency strings. Update the tool's docstring to mention the same examples.

## User Stories

1. As a researcher, I want `get_provenance` on a job that fell back from Harmony to OPeNDAP to show me that the fallback happened, so that I understand why my result came from OPeNDAP instead of Harmony.
2. As a researcher, I want the fallback event to include the actual error that triggered it, so that I can tell a transient Harmony outage apart from a genuinely unsupported request.
3. As a researcher, I want `get_provenance` on any completed job to tell me which provider actually produced the materialized output, so that I don't have to guess or re-derive it.
4. As a researcher, I want `get_provenance` on a job that succeeded via Harmony on the first attempt (no fallback) to show a clean `SUBMITTED → MATERIALIZED` trail with no phantom fallback event, so that the event log matches what actually happened.
5. As a maintainer, I want provenance-event writes to be append-only with no dedup logic, so that a worker crash-and-restart mid-job produces an honest (if repetitive) history rather than silently swallowed evidence of the restart.
6. As a maintainer, I want the worker's access to `ProvenanceStore` to follow the same `ctx`-caching convention `_session_factory(ctx)` already established, so that the worker module doesn't mix two different lifecycle-caching strategies.
7. As a researcher, I want an invalid `time_freq` (e.g. a deprecated pandas alias like `"M"`) to raise a clear error naming a couple of valid alternatives, so that I don't have to read pandas source or trial-and-error my way to a working frequency string.
8. As a maintainer, I want the `time_freq` fix to catch and re-wrap pandas' own exception rather than maintain a separate allowlist of valid frequency strings, so that this codebase never has to track pandas' frequency-alias table as it changes across versions.

## Implementation Decisions

### Provenance events

- **New enum values only — no DB migration.** `ProvenanceEvent.event_type` (`models.py:122`) is a plain `String` column, not a native Postgres enum, so adding `SUBMITTED` and `PROVIDER_FALLBACK` to the `ProvenanceEventType` `StrEnum` requires no Alembic migration. Update the `ProvenanceEvent` docstring (`models.py:114-115`) to list the two new event kinds alongside the existing four.
- **`SUBMITTED` fires once, at actual submission success.** In `worker.py`'s `submit_job`, record `SUBMITTED` at the existing `transition_state(session, job_id, JobState.SUBMITTED, ...)` call (currently `worker.py:112-118`), with `detail={"provider": spec.provider}`. This is reached only on a successful `provider.submit()` — whichever pass that is, first-attempt or post-fallback-retry — so it fires exactly once per job regardless of whether a fallback occurred.
- **`PROVIDER_FALLBACK` fires in the exception handler, before re-enqueue.** In `submit_job`'s `except Exception as exc:` branch (currently `worker.py:89-105`), when the Harmony-failed/OPeNDAP-available condition (`spec.provider == "harmony" and spec.opendap_urls`) is met, record `PROVIDER_FALLBACK` with `detail={"from_provider": "harmony", "to_provider": "opendap", "reason": {"error_type": type(exc).__name__, "message": str(exc)}}` before rewriting `spec.provider` and re-enqueuing. This means a fallback job's real event order is `PROVIDER_FALLBACK → SUBMITTED → MATERIALIZED` (fallback necessarily precedes the eventual successful `SUBMITTED`, since `SUBMITTED` can only fire on success and the first, failed attempt never reaches it).
- **`MATERIALIZED` fires just before the `READY` transition.** In `materialize_job` (currently `worker.py:163-208`), record `MATERIALIZED` immediately before `transition_state(session, job_id, JobState.READY, progress=100)` (currently `worker.py:207-208`), with `detail={"provider": spec.provider, "storage_key": result.storage_key, "media_type": result.media_type, "size_bytes": result.size_bytes}`. This is the field that closes the original bug's gap in isolation — the last event in any job's trail names the provider that actually produced the output, without needing to cross-reference `PROVIDER_FALLBACK`.
- **Append-only, no idempotency guard.** `record_event` is never called with a check-then-skip against existing rows for the same `(job, event_type)`. A worker crash-restart producing a second `SUBMITTED` (or any duplicate) is acceptable and meaningful history, not noise to be suppressed. This matches the store's existing append-only posture (edges and events are never updated or deleted).
- **New `_provenance(ctx)` worker helper.** Add a helper alongside `_session_factory(ctx)` (currently `worker.py:57-64`) that lazily builds `ProvenanceStore(_session_factory(ctx))` and caches it on `ctx["provenance"]`. Do not reuse `tools/retrieval.py`'s `_default_provenance()` module-global singleton (`retrieval.py:80-85`) — the worker's existing convention is `ctx`-scoped caching, and mixing the two strategies in the same call path adds cross-module coupling for no benefit.
- **Handle/workspace targeting.** All three `record_event` calls target `(spec.workspace_id, spec.obs_handle)`, both of which are non-optional fields already present on every `RequestSpec` (`providers/request_spec.py:62-64`).
- **`CREATED` stays unwired — explicitly out of scope** (see below).

### `resample` time_freq validation

- **Try/except, not an allowlist.** Wrap `result.resample(time=time_freq).mean()` (`transform.py:165`) in a `try`/`except`, catching the exception pandas raises for a bad/deprecated frequency alias and re-raising a `ValueError` with a message that names the bad value, states pandas rejected it, and gives 2-3 example valid aliases (e.g. `"D"` daily, `"h"` hourly, `"ME"` month-end) plus a pointer to pandas' offset-alias documentation.
- **Docstring update.** `resample`'s docstring (`transform.py:153-154`) currently gives only one example (`"1D"`). Add the same 2-3 example aliases used in the error message so the schema-level guidance and the error-message guidance agree.

## Testing Decisions

- **Unit: `SUBMITTED` on direct-Harmony success.** A job whose provider is Harmony throughout records exactly one `SUBMITTED` event with `detail.provider == "harmony"`, no `PROVIDER_FALLBACK` event.
- **Unit: `SUBMITTED` + `PROVIDER_FALLBACK` on Harmony-fails-then-OPeNDAP-succeeds.** A job that fails its first Harmony submit and successfully retries via OPeNDAP records, in order, one `PROVIDER_FALLBACK` (`from_provider="harmony"`, `to_provider="opendap"`, `reason.error_type`/`reason.message` matching the raised exception) followed by one `SUBMITTED` (`detail.provider == "opendap"`).
- **Unit: `MATERIALIZED` detail fields.** A successfully materialized job records one `MATERIALIZED` event whose `detail` contains the correct `provider`, `storage_key`, `media_type`, and `size_bytes`.
- **Unit: `resample` invalid `time_freq`.** Calling `resample` with a deprecated alias (e.g. `"M"`) raises a `ValueError` (not a raw pandas exception) whose message names the bad value and does not simply forward pandas' internal wording verbatim.
- **Integration (hard requirement): update `test_harmony_path_runs_to_ready_with_provenance`.** This existing test (`tests/integration/test_durable_pipeline.py:225-264`) currently asserts only on ancestry. Extend it to assert `events` is non-empty and contains the expected `SUBMITTED`/`MATERIALIZED` (and, for a fallback-path variant, `PROVIDER_FALLBACK`) trail. This is the test that should have caught the original gap — leaving it unchanged would mean a future regression here goes undetected the same way this one did.
- **Run via the project's Docker test workflow**, per project rules: `docker compose exec mcp python -m pytest tests/unit -v` for the unit tests, and the equivalent invocation against `tests/integration` for the updated integration test. The local Python environment lacks dependencies and DB access.

## Out of Scope

- **Bugs #1 and #2 from the original report** (OPeNDAP time-coordinate degradation to `RangeIndex`; raw `TypeError` on re-applied `time_range`) — both confirmed already fixed, in `cedf573` and `2f18855` respectively, before this PRD was scoped.
- **Bug #4 from the original report** (`cancel_retrieval` on an already-terminal job) — confirmed not a bug. `retrieval.py:265-289`'s no-op-on-terminal behavior is intentional and documented (canceling an already-terminal job is a no-op, not an illegal request), and is covered by `test_cancel_terminal_job_is_noop`. The original report mischaracterized the docstring's intent.
- **Design/performance issues #6-11 from the original report** (AOI geometry payload size, citation-ID noise, `related_urls` payload size, whole-file size estimates, GIBS layer guessing, AppEEARS-eligibility discoverability) — real findings, but heterogeneous in owner/severity/tool; each should get its own ticket rather than being bundled here.
- **`CREATED` provenance events.** The enum value exists but is never written; job creation is already durably recorded via `record_edge` (which carries the re-materializable request spec — the thing CLAUDE.md actually requires). Adding a redundant `CREATED` event for the same moment doesn't close any gap the original bug report identified.
- **Direct-S3 provenance instrumentation.** The direct-S3 "data as-is" path is not yet wired to the worker (`providers/__init__.py:49-72`'s `build()` has no `"direct"` case; a `spec.provider == "direct"` job would fail at submit time today). Once implemented, it will flow through the same `submit_job`/`materialize_job` functions this PRD instruments, so no separate work is needed here — noted for awareness, not actioned.
- **A structured error taxonomy for `PROVIDER_FALLBACK.reason`.** `reason` is `{"error_type": type(exc).__name__, "message": str(exc)}`, not a classified taxonomy (e.g. "timeout" vs. "validation" vs. "quota"). Building a taxonomy would require categorizing every possible harmony-py exception speculatively; revisit only if a concrete reporting/analytics requirement emerges.
- **Any change to `resample`'s or `subset`'s functional behavior beyond the `time_freq` error message.** This PRD does not touch `spatial_factor` coarsening, the `_maybe_decode_float_time` fix already shipped in `2f18855`, or any other part of the resample/subset pipeline.

## Further Notes

- Source: `/grill-me` session on an air-quality-researcher bug report covering gridded L3/L4 (MERRA-2, TROPOMI CONUS), swath L2 (MODIS MOD04_L2), and tabular (AppEEARS) datasets, 2026-07-01.
- `ProvenanceEventType`'s docstring (`models.py:57`) cites "PLAN.md §4.5" as its canonical spec; no `PLAN.md` currently exists anywhere in the repo (confirmed via repo-wide search). This PRD updates the enum and its inline docstring directly since there's no external doc to reconcile against — worth a maintainer follow-up to either restore or retire that citation, independent of this PRD.
- Per project hard rules: all retrieval remains a durable job persisted in Postgres, resumable on restart; provenance records the re-materializable spec, never an ephemeral staged-output URL; Harmony remains primary, OPeNDAP the worker's runtime fallback only.
