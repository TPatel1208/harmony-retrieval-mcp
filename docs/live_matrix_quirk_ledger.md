# Live TTA-collection matrix quirk ledger (PRD 019)

Every quirk the live validation matrix
(`tests/live/test_tta_collection_matrix.py`) surfaces against the eight real
Talking-to-Air collections, so a fix landing today never un-fixes a past one
and Talking-to-Air's own prompt guidance (T04) can cite a concrete row instead
of a vague "TEMPO is weird" note.

This ledger is broader than `docs/opendap_quirk_ledger.md` on purpose: that one
is scoped to Hyrax/DAP4 constraint-expression *emission* only (its own "out of
scope" section says so). This one covers the whole pipeline the matrix walks —
search, describe, Harmony/OPeNDAP routing, materialization, and response
decoding — for whichever real collection surfaced the quirk.

**Convention:** a new quirk gets a row here, naming the collection, the
symptom, the root cause (once known), and whether it was fixed in the matrix's
own session or spawned a follow-up (PRD 019 §Solution: "failures are data, not
blockers").

| Collection | Symptom | Root cause / status | Session |
|---|---|---|---|
| MOD08_D3 / MYD08_D3 (975 UMM-Var associations) | `describe_dataset`/`retrieve_subset` raised `CMR 414` (CloudFront "Request could not be satisfied") instead of any application-level error. | `CMRProvider.get_variables` put every associated variable id on one `concept_id[]` query string; CloudFront rejects the URL as too long well before CMR's own limits engage. **Fixed this session**: batched into groups of 100 (`_VARIABLE_ID_BATCH_SIZE`, `providers/cmr.py`). |  |
| MOD08_D3 / MYD08_D3 (same root cause, second-order) | `retrieve_subset` raised `Unknown variable(s) for this collection` for `AOD_550_Dark_Target_Deep_Blue_Combined_Mean` — a real variable name — because it wasn't among the *first 50* associations `get_variables()` silently truncated to. Contradicted `describe_dataset`'s own documented `variable_count` contract ("always the true total"). | `get_variables(collection_concept_id=...)` capped the batched-but-still-collection-scoped list at the default page `limit` (50), meant only for the open-ended keyword-search path. **Fixed this session**: a collection-scoped call now returns the complete batched list, ignoring `limit` (`providers/cmr.py`). |  |
| OMHCHOd (no UMM-Var; DMR-fallback path, PRD 009) | `retrieve_subset` raised `Variable 'key_science_data/column_amount' not found in this granule's structure`, even though that is the *exact* string `describe_dataset` had just returned as the variable's UMM-Var `Name`. | `resolve_variables` (`providers/opendap/_dmr.py`) only treated a name as a full DAP4 path when it started with `/`; UMM-Var's own `Name` field for a grouped variable never carries a leading slash, so it fell through to bare-leaf (last-segment-only) matching and found nothing. **Fixed this session**: a slash-containing name is now checked as a full path with an assumed leading `/` before falling back to bare-leaf matching. |  |
| Any DMR-fallback (no-UMM-Var) grouped/swath collection with per-variable coordinates (e.g. OMHCHOd; originally the ATL08 six-beam case) | The materialized OPeNDAP request always projected the literal `/lat`/`/lon` coordinate names, 400ing against any collection whose real coordinates are named differently (OMHCHOd's are `/latitude`/`/longitude`) — even though plan-time DMR resolution had already discovered the *correct* per-variable coordinate paths. | `providers/__init__.py`'s `build()` — the single spec→provider seam the durable worker uses at submit/resume time — coerced `spec.coord_lat`/`spec.coord_lon` from `None` to the literal `"lat"`/`"lon"` (`spec.coord_lat or "lat"`) and never forwarded `spec.var_coords` to `OPeNDAPProvider` at all, even though both were already correctly planned and durably persisted on the spec. **Fixed this session**: `build()` now passes `coord_lat`/`coord_lon` through verbatim (`None` is a real, meaningful "no single global coordinate" signal) and forwards `var_coords`. |  |
| OMDOAO3e (classic HDF-EOS5 grid, real UMM-Var catalog) | OPeNDAP fallback 400'd: `dap4.ce=/lat;/lon;/HDFEOS/GRIDS/ColumnAmountO3/Data_Fields/ColumnAmountO3`. | UMM-Var's `Name` for this product is the *original* nested HDF-EOS5 path (`HDFEOS/GRIDS/.../ColumnAmountO3`), but the real GES DISC OPeNDAP endpoint serves this collection through an NcML wrapper that **flattens** the hierarchy to root-level names (confirmed via the granule's own DMR: a plain `ColumnAmountO3` variable with root dimensions `/lat`,`/lon` sized 720×1440, `fullnamepath` attribute recording the original nested path). Since this collection *does* have a registered UMM-Var catalog, the DMR-fallback resolution path (`_resolve_from_dmr`) never engages to catch the mismatch — planning trusts UMM-Var's `Name` verbatim. **Not fixed — structural, follow-up session.** Fixing correctly means deciding when/how to cross-check a UMM-Var-resolved name against a live granule DMR even when UMM-Var itself isn't empty, for exactly this classic-HDF-EOS-via-NcML shape. |  |
| OMHCHOd, TROPOMI NO2 monthly (`HAQ_TROPOMI_NO2_GLOBAL_M_L3`), MOD08_D3, MYD08_D3 — 4 of 9 matrix collections | Harmony rejected the submit with a 422 ("the requested combination of operations: variable subsetting [and reformatting to application/netcdf] ... is unsupported"), and the job failed outright — OPeNDAP never got a chance, even though a direct OPeNDAP request against the same collection/variable/AOI/window (verified by hand for OMDOAO3e/OMHCHOd) is capable of serving the data. | `jobs/worker.py`'s `_classify_submit_failure` buckets *any* Harmony 422 containing the phrase `"the requested combination of operations"` as `invalid_request`, and `submit_job` only attempts the OPeNDAP fallback when the classification is `"infrastructure"`. This phrase means "no chain in Harmony's own service registry can do this," which is a Harmony-catalog fact, not a fact about whether the request itself is valid for *any* provider — conflating the two skips a fallback that would otherwise work. This is deliberate, tested behavior (`tests/unit/test_jobs/test_worker_provenance.py::test_submit_job_invalid_request_fails_without_opendap_fallback` locks in today's classification with this exact phrase). **Not fixed — structural, follow-up session**, and the single highest-impact finding of this matrix (affects 4 of 9 real collections tested): reclassifying "unsupported combination of operations" as fallback-eligible (while still refusing fallback for a genuinely bad variable name, e.g. "coverages were not found") needs a deliberate routing-policy decision plus updating the test that currently pins the old behavior. |  |

## Collections validated end-to-end this session

| Collection | Concept id | Primary variable | Result |
|---|---|---|---|
| TEMPO NO2 L3 V04 | C3685896708-LARC_CLOUD | `product/vertical_column_troposphere` | pass |
| TEMPO O3TOT L3 V04 | C3685896625-LARC_CLOUD | `product/column_amount_o3` | pass |
| TEMPO HCHO L3 V04 | C3685897141-LARC_CLOUD | `product/vertical_column` | pass |
| OMI_MINDS_NO2d | C2215175232-GES_DISC | `ColumnAmountNO2` | pass |
| OMDOAO3e | C1266136037-GES_DISC | `HDFEOS/GRIDS/ColumnAmountO3/Data_Fields/ColumnAmountO3` | fail — see ledger row above |
| OMHCHOd | C4119459768-GES_DISC | `key_science_data/column_amount` | fail — see ledger row above |
| TROPOMI NO2 monthly (`HAQ_TROPOMI_NO2_GLOBAL_M_L3`) | C3087325222-GES_DISC | `Tropospheric_NO2` | fail — see ledger row above |
| MODIS Terra AOD (`MOD08_D3`) | C1443727145-LAADS | `AOD_550_Dark_Target_Deep_Blue_Combined_Mean` | fail — see ledger row above |
| MODIS Aqua AOD (`MYD08_D3`) | C1443729298-LAADS | `AOD_550_Dark_Target_Deep_Blue_Combined_Mean` | fail — see ledger row above |

Nine collections total (the problem statement's "eight" underquotes its own
user story 1's enumeration — TEMPO NO2/O3TOT/HCHO, three OMI products, one
TROPOMI product, and MODIS Terra/Aqua both count separately).
