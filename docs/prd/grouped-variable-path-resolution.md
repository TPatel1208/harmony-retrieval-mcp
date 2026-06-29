# PRD: Resolve grouped-collection variable paths so TEMPO (and other grouped netCDF4) subsets succeed

> Triage label to apply when published: `ready-for-agent`

## Problem Statement

When I request a variable/bbox subset for a grouped netCDF4 collection (e.g. TEMPO NO2 L2/L3) by its bare leaf variable name — `vertical_column_total` — the retrieval job fails. Harmony fails the submit, the worker silently falls back to OPeNDAP, and Hyrax rejects the OPeNDAP request with HTTP 400 because the DAP4 constraint expression carries the bare leaf name (`dap4.ce=vertical_column_total`) instead of the variable's fully-qualified group path. Even passing an explicit `product/vertical_column_total` still 400s, because the emitted constraint lacks the leading slash that DAP4 fully-qualified names require for grouped datasets.

Flat-file collections (GLDAS, MOD13Q1) are not affected by the grouped-path problem — their variables live at the root — so the symptom is specific to collections whose netCDF4 files organize variables into groups.

From my perspective: "I asked for a real variable in a real TEMPO granule, in a window that has data, and the job failed with a 400 I can't act on."

## Solution

When I request a variable for any collection, the system should resolve my bare leaf name to the collection's actual variable path from CMR UMM-V *at planning time*, before the job is persisted — so that whichever provider ultimately runs the job (Harmony first, OPeNDAP as the worker's runtime fallback) receives the correct, fully-qualified variable name. For grouped collections, the OPeNDAP constraint expression should emit the variable as a proper DAP4 fully-qualified name (leading slash, full group path). Bare leaf names for grouped collections should then succeed end to end, and an ambiguous leaf name (one that exists at more than one path) should fail fast with a clear message telling me the conflicting paths so I can disambiguate.

## User Stories

1. As an Earthdata MCP user, I want to request a TEMPO variable by its bare leaf name and have it resolve to the correct group path, so that my subset succeeds without my needing to know the collection's internal netCDF4 group layout.
2. As an Earthdata MCP user, I want variable-path resolution to happen once, at planning time, so that both the primary Harmony submit and the OPeNDAP fallback use the same resolved name and I get consistent behavior regardless of which path runs.
3. As an Earthdata MCP user requesting a grouped collection, I want the OPeNDAP DAP4 constraint expression to use the fully-qualified name with a leading slash, so that Hyrax accepts the request instead of returning HTTP 400.
4. As an Earthdata MCP user requesting a flat-file collection (GLDAS, MOD13Q1), I want variable resolution to leave my root-level variable names working as before, so that the fix for grouped collections does not regress flat collections.
5. As an Earthdata MCP user, I want my coordinate variable names (lat/lon) to continue being discovered from CMR UMM-V as they are today, so that the bbox projection keeps working across collections that name their coordinates differently.
6. As an Earthdata MCP user who passes a variable that is ambiguous across multiple group paths, I want the request to fail fast at planning time with a message naming all the conflicting paths, so that I can pick the exact one I want instead of getting a silent wrong answer or an opaque downstream 400.
7. As an Earthdata MCP user who passes an already-fully-qualified variable path, I want it used as-is without a second lookup, so that I retain an escape hatch when I already know the exact path.
8. As an Earthdata MCP user whose collection has no UMM-V variable metadata, or when the CMR lookup fails, I want the system to fall back gracefully to passing my variable through unchanged (and default coordinate names), so that a metadata gap degrades rather than hard-fails.
9. As an Earthdata MCP user, I want the resolved variable paths recorded in the durable request spec, so that a job resumed after a restart re-materializes from the same resolved coordinates rather than re-deriving (or mis-deriving) them.
10. As an Earthdata MCP user, I want provenance to keep recording the re-materializable request spec (resolved variables included) and never an ephemeral staged-output URL, so that my retrieval stays reproducible.
11. As an Earthdata MCP user submitting a multi-day window for a grouped collection, I want every granule's OPeNDAP constraint expression to carry the resolved group path, so that the whole window's bundle materializes rather than only the granules that happened to work.
12. As an Earthdata MCP user, I want the primary Harmony submit to receive resolved variable names so that it succeeds outright when it can, rather than failing and forcing the silent OPeNDAP fallback for a problem that was only ever a naming issue.
13. As a maintainer, I want the existing variable-resolution helper to actually be wired into the planning path rather than sitting as dead code, so that the behavior we tested at the unit level is the behavior users get.
14. As a maintainer, I want a single resolution step that produces both coordinate names and resolved data-variable names from one CMR `get_variables` call, so that we don't make redundant CMR round-trips per request.
15. As a maintainer, I want the grouped-vs-flat distinction handled by the resolved path's shape (group prefix present or not) rather than per-collection special casing, so that new grouped collections work without code changes.

## Implementation Decisions

- **Wire variable resolution into the planning path.** The retrieval planning core (the shared core of the three `retrieve_*` tools) currently discovers only coordinate names from CMR UMM-V and passes data variables through unresolved. It must instead resolve the data variables to their full UMM-V paths *before* constructing the `RetrievalPlan` and the durable request spec.
- **Reuse the existing resolver, retire the coordinate-only path.** A resolver that performs both coordinate-name discovery and bare-leaf → full-path data-variable resolution from a single `get_variables` call already exists in the OPeNDAP provider module but is currently unreferenced by production code (dead code). Adopt it as the single resolution entry point and remove the now-redundant coordinate-only discovery helper, so there is exactly one CMR variables lookup and one resolution policy.
- **Resolution policy (from the existing resolver, treated as the contract):**
  - A variable already given as a fully-qualified path (leading `/`) is used as-is, no lookup.
  - A bare leaf name is matched case-insensitively against the last path segment of each UMM-V variable `Name`.
    - Exactly one match → substitute the full UMM-V path.
    - Zero matches → pass the name through unchanged (degrade, do not fail).
    - More than one match → raise an error naming all conflicting paths. This ambiguity error must be raised outside the network-error try/except so it is never swallowed by the CMR-error fallback.
  - On any CMR/network error, fall back to default coordinate names and pass variables through unchanged.
- **Resolution feeds both providers.** Because resolution happens at planning time and the resolved names land in the durable spec, the primary Harmony request (which passes `variables` to harmony-py) and the OPeNDAP worker fallback (which builds the DAP4 CE) both consume the resolved names. No per-provider resolution.
- **DAP4 fully-qualified-name serialization.** The OPeNDAP constraint-expression builder must emit each projected variable (and the projected coordinate variables, when a bbox is present) as a DAP4 fully-qualified name with a leading slash. For a grouped variable this yields `/<group>/<leaf>`; for a root variable `/<leaf>`. The recent change that stripped the leading slash fixed flat files at the cost of grouped files; the correct form is the FQN-with-leading-slash for both. `/time` projection stays omitted (temporal filtering is done at CMR granule-search level; many L3 products have no `time` variable and projecting it 400s).
- **Durable spec carries resolved names.** The resolved data-variable paths and the discovered coordinate names are persisted on the request spec (alongside the existing `opendap_urls`, `coord_lat`, `coord_lon`) so a resumed job rebuilds identical constraint expressions. Provider reconstruction in the worker reads these off the spec and does not re-resolve.
- **Harmony-first routing is unchanged.** Routing still tries Harmony first and treats OPeNDAP strictly as the worker's runtime fallback when a real Harmony submit fails. This PRD changes *what names the providers receive*, not the routing decision.
- **No new CMR round-trips beyond the one already made.** The combined resolver performs a single `get_variables(concept_id)` call; coordinate discovery and variable resolution share that one result.

## Testing Decisions

- **What makes a good test here:** assert external, observable behavior — the resolved variable names that land in the durable request spec at the tool boundary, and the exact DAP4 constraint-expression string emitted for a grouped variable — not the internals of how resolution is implemented. Tests drive real public entry points with a fake CMR (UMM-V variables stubbed) and fake storage/enqueue, exactly as the existing retrieval tests do.
- **Seam 1 — retrieval tool entry (highest seam).** Test the shared planning core of the `retrieve_*` tools. With a fake CMR whose `get_variables` returns grouped UMM-V variables, assert the persisted request spec carries the resolved full group paths for the data variables (not just the lat/lon coordinates). Cover: bare leaf → resolved group path; already-qualified path passes through; zero-match passes through unchanged; ambiguous leaf raises with all conflicting paths named; CMR-error path falls back to defaults. Prior art: `tests/unit/test_tools/test_retrieval.py` — `test_retrieve_data_stores_request_spec_not_url`, `test_retrieve_data_collects_all_window_granules_for_opendap_fallback`, and the existing `cmr.get_variables = AsyncMock(...)` stubbing pattern.
- **Seam 2 — OPeNDAP constraint-expression serialization.** Test the constraint-expression / DAP4 URL builder in the OPeNDAP provider directly. Assert a resolved grouped variable serializes to a fully-qualified name with a leading slash (`/product/vertical_column_total`), a root variable to `/<leaf>`, and that projected coordinate variables follow the same FQN form when a bbox is present. Prior art: `tests/unit/test_providers/test_opendap.py` (existing `_constraint_expression` coverage).
- **No new seams.** These two existing seams are preferred over introducing new ones; they test genuinely distinct behaviors (planning-time resolution vs. CE wire format), so they are not collapsed into one.
- **Run via the project's Docker test workflow** (`docker compose exec mcp python -m pytest tests/unit -v`), per project rules — the local Python environment lacks dependencies and DB access.

## Out of Scope

- The GLDAS Harmony subsetter "index 0 is out of bounds" error — a separate Harmony-side subsetting bug, unrelated to variable-name resolution.
- MOD13Q1 variable names containing spaces (e.g. "250m 16 days blue reflectance") and their URL-encoding in DAP4 — a distinct encoding concern, not grouped-path resolution.
- Any change to the Harmony-first routing decision itself, or to the worker's fallback mechanism. This PRD changes the variable names providers receive, not when/whether the fallback fires.
- Multi-variable OPeNDAP request behavior beyond ensuring each requested variable is resolved and serialized correctly (no batching/optimization work).
- Conversion to Zarr or any change to the read/concatenation path in `_dataio`.

## Further Notes

- Root-cause trace from the investigation that produced this PRD: the `_resolve_from_cmr` helper that performs grouped-path resolution exists in the OPeNDAP provider module but is never called from production code; the planning path instead calls a coordinate-only discovery helper, leaving data variables unresolved. Separately, a recent change removed the leading slash from emitted DAP4 constraint expressions, which fixes flat collections but breaks grouped FQNs. The two together produce the TEMPO 400.
- DAP4 fully-qualified names are rooted with a leading slash: `/var` at the root, `/group/var` within a group. This is the canonical form to standardize on for both flat and grouped collections.
- Validation should reproduce against TEMPO in a window known to have data (e.g. June 2024 had ~353 granules), since the original QA pass hit empty windows for some season/region combinations and mis-attributed SKIPs.
- Per project hard rules: Harmony remains primary and is always tried first; OPeNDAP is the worker's runtime fallback, not a planning-time choice; provenance records the re-materializable spec, never an ephemeral staged-output URL.
