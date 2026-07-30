# PRD 023 — Materialization identity: record and expose what was *actually* retrieved

**Repo:** harmony-retrieval-mcp · **Session scope:** one session, one commit · **Label:** ready-for-agent
**Depends on:** PRD 017 (`export_result`/`rematerialize` — this adds a field to the former and a durable record behind the latter), PRD 020 (request-signature dedupe — deliberately *distinct* from this; see below), PRD 021 (job metadata enrichment — same `request_spec` projection seam).
**Consumer:** Talking-To-Air PRD T54 is blocked on this. It is the reason this PRD exists.

## Problem Statement

The server records what was **asked for**, never what was **delivered**, so no consumer — and no part of this server — can tell whether two materializations of the same handle contain the same data.

`RequestSpec` ([src/earthdata_mcp/providers/request_spec.py:50-84](../../src/earthdata_mcp/providers/request_spec.py)) is durable and complete about *intent*: `concept_id`, `aoi_bbox`, `time_range`, `variables`, `output_format`, `service_name`, and the planning-time `opendap_urls`. `rematerialize` replays that spec unchanged ([tools/retrieval.py:1212-1268](../../src/earthdata_mcp/tools/retrieval.py) — `request_spec=job.request_spec`, no re-planning), and the `obs_` handle identity never changes.

But an unchanged spec does **not** guarantee unchanged bytes, for three reasons that are all deliberate design in this repo:

1. **Harmony resolves granules server-side at submit time.** The spec pins the collection, bbox and time range; Harmony decides which granules satisfy them, *when the job runs*. For an NRT collection whose range touches the present, a late-arriving granule makes a replay a strictly different result.
2. **Harmony is often submitted UNPINNED** — a hard rule in [CLAUDE.md](../../CLAUDE.md): when no single service satisfies the plan, submit unpinned and let the server pick the chain. `compute_request_signature`'s own docstring says it plainly: *"an unpinned Harmony submit lets the server pick a different chain, a service's capabilities can change"* ([request_spec.py:362-366](../../src/earthdata_mcp/providers/request_spec.py)).
3. **OPeNDAP is a runtime fallback, not a planning-time choice** (CLAUDE.md again). A job that ran via Harmony originally can run via OPeNDAP on replay if the Harmony submit fails that day — a different code path, potentially a different output shape, from the same spec.

So `job.request_spec` answers "what was this job asked to fetch?" and nothing in the system answers "what did it actually fetch?" `_status_enrichment` even derives `granule_count` from `opendap_urls` ([tools/retrieval.py:355-364](../../src/earthdata_mcp/tools/retrieval.py)) — the docstring is candid that this is "the only granule tally the durable spec carries," and it is a *planning-time* tally that no provider is obliged to have honored.

The concrete consequence downstream: Talking-To-Air wants to cache a derived Zarr cube keyed on the identity of its source, and to serve that cube without an `export_result` round-trip (its PRD T54). With no delivered-content identity to key on, it must either re-verify upstream on every lookup — losing the ability to answer while this server is restarting or has evicted the source — or guess. Neither is acceptable in a tool whose scientific claims are supposed to be grounded.

## Solution

Record, at materialization, a durable **materialization record** describing what the job actually produced, and expose its digest through `export_result`.

Per completed job, persist:

- `granule_ids` — the resolved granule identifiers actually retrieved (CMR granule concept-ids where available, otherwise the provider's own identifiers/URLs)
- `provider_used` — the provider that actually ran, which may differ from `request_spec.provider`
- `service_chain` — the Harmony service(s) that actually processed it, when the provider reports them (an unpinned submit's realized chain)
- `content_digest` — a stable hash over the above plus `output_format`, canonicalized (granule ids sorted and deduplicated)

`export_result`'s ready response gains `content_digest` (and `granule_count` derived from the record, superseding the planning-time tally for jobs that have one). Existing fields are untouched, so every current consumer is unaffected.

**This is deliberately not `compute_request_signature`.** That signature hashes *the user's intent* — dataset, AOI, time bounds, variables, format — and exists so a repeat ask can reuse an existing handle (PRD 020). Its docstring already draws the line this PRD depends on: routing details must never decide whether two requests are "the same ask." `content_digest` is the opposite question — whether two materializations are *the same bytes* — and routing is exactly what it must capture. Two requests can share a signature and differ in digest; that is the whole point, and it is the fact PRD 020's reuse path quietly relies on today without being able to state.

**This is also not `cache_key_value`.** That field folds in `service_name`/`service_version` and, per its own comment, "backs no lookup at all — nothing reads it back" ([request_spec.py:360-362](../../src/earthdata_mcp/providers/request_spec.py)). It is planning-time routing intent, not delivered content. Leave it alone; do not overload it.

Once a handle carries a `content_digest`, a consumer can key derived artifacts on delivered content and detect a changed rematerialization instead of assuming one never happens — and this server gains, for the first time, the ability to say whether a replay reproduced its original.

## User Stories

1. As a consumer caching derived artifacts, I want a stable identifier for what a handle actually contains, so that I can key a cache on delivered bytes rather than on filesystem metadata that changes on every replay.
2. As a consumer, I want a rematerialization that produced *different* data to be detectable, so that an NRT late-arrival or a different service chain cannot silently change my analysis.
3. As a researcher reading provenance, I want to see which granules actually went into my result, so that "what is this made of" has an answer that does not depend on planning-time guesses.
4. As a researcher, I want to know which provider and service chain actually ran, so that an unpinned Harmony submit's realized routing is a recorded fact rather than an inference.
5. As the server, I want `granule_count` in `get_retrieval_status` to reflect what was retrieved once that is known, so that a consumer is not shown a planning-time tally as though it were an outcome.
6. As an operator, I want a replay whose digest differs from the original to be logged, so that upstream data changes are observable rather than silent.
7. As a consumer of an older handle with no materialization record, I want the field simply absent, so that adoption is additive and nothing breaks.

## Implementation Decisions

- New durable column (or JSONB block) on the `jobs` row — `materialization` — written by the worker at the transition into `READY`, alongside the existing `storage_key`/`size_bytes` writes. Never written at planning time: this field's entire value is that it is an outcome.
- `content_digest = sha256(canonical_json({granule_ids: sorted(set(...)), provider_used, service_chain, output_format}))`. Sorting and deduplication mirror `compute_request_signature`'s canonicalization discipline so equivalent results collide and reordering never matters.
- **Absent, not empty, when unknown.** A provider that does not report granule identifiers (an AppEEARS point sample) records `provider_used` and `output_format` and omits `granule_ids`; the digest is then computed over what is known and flagged `partial: true`. A consumer must be able to tell "no granule list available" from "an empty granule list," and must be able to decline to trust a partial digest.
- Legacy handles (materialized before this PRD) have no record. `export_result` omits `content_digest` entirely for them — never a fabricated or backfilled value. `from_jsonb`'s existing legacy tolerance ([request_spec.py:192-235](../../src/earthdata_mcp/providers/request_spec.py)) is the precedent for this posture.
- On replay, compare the new digest to the previous one and log a named event when they differ, with both digests and the handle. This is the first time this server can observe upstream drift; it should say so.
- `_status_enrichment` prefers the materialization record's granule count when present, falling back to the `opendap_urls` tally otherwise — and the docstring is updated to say which it returned.
- No provenance-graph change: this is descriptive metadata on an existing node, not a new edge. Consistent with PRD 017's stance that reads record no edge.

## Testing Decisions

- **Digest stability:** the same granule set in different order, or with duplicates, yields the same digest; a different granule set does not.
- **Provider-drift detection:** a job whose replay runs via a different `provider_used` produces a different digest and logs the drift event.
- **NRT drift:** a replay that resolves an extra granule produces a different digest (simulated by a fake provider returning a longer granule list on the second run) — the headline scenario, since this is the case no existing mechanism can see.
- **Partial digests:** an AppEEARS-style job with no granule identifiers records `partial: true` and still produces a stable digest across replays.
- **Legacy tolerance:** a handle materialized before this PRD exports successfully with `content_digest` absent, and every existing `export_result` field byte-identical.
- **Signature independence:** two requests with the same `compute_request_signature` but different realized routing get different digests — asserted directly, since this distinction is the PRD's thesis.
- **Enrichment:** `get_retrieval_status` reports the materialized granule count when a record exists and the planning-time tally when it does not.
- Per [CLAUDE.md](../../CLAUDE.md), tests run inside the `mcp` container. **Do not run `pytest tests/unit` casually — it executes `drop_all` against the configured database and has destroyed live job rows.**

## Out of Scope

Retention, pinning and eviction policy (PRD 024). Granule prefetching (PRD 025). Changing `compute_request_signature` or PRD 020's reuse semantics — reuse continues to match on intent, and this PRD only makes the resulting materialization describable. Changing `cache_key_value`. Backfilling records for already-materialized handles. Verifying that a replay *should* have reproduced the original, or blocking a replay that did not — this PRD makes drift observable; deciding what to do about it belongs to the consumer.

## Further Notes

Talking-To-Air's PRD T52 defines its cube-cache key as `SHA256(source_identity || OPEN_PIPELINE_VERSION || engine)`, where `source_identity` is deliberately a *function* rather than a fixed tuple — today `(path, st_size, st_mtime_ns)` of the exported file — precisely so a stronger upstream identifier can be adopted in one line. `content_digest` is that identifier. Shipping this PRD lets T54 take its branch A (serve a cached cube without an `export_result` round-trip) on a proven identity rather than on an assumption about handle immutability that, per the Problem Statement above, does not hold.
