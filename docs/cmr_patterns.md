# CMR query patterns reference (Phase 2.1)

Research notes for `providers/cmr.py` (Phase 2.2) and `providers/_capabilities.py`
(Phase 2.3). The goal is that the provider code is right the first time: correct
endpoints, parameters, pagination, and UMM-JSON parsing.

## Canon vs. worked example

**Canon is CMR's public API docs + the UMM schemas.** NASA's `nasa/earthdata-mcp`
is a *worked example to re-verify against*, never a contract — it is young and
actively refactoring (the commit read here is itself a "v1.0 cleanup and
pagination" rewrite that deleted the old Lambda/RAG/Redis pipeline). Where this
doc borrows a pattern from their code, the **pinned commit** is cited; treat
anything not confirmable against CMR's docs/UMM as advisory.

- CMR Search API docs: <https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html>
- UMM schema CDN: `https://cdn.earthdata.nasa.gov/umm/<concept>/v<version>`

### Pinned reference

- Repo: `github.com/nasa/earthdata-mcp`
- **Commit: `bb0ac5d5b2f59593659c6f8fbf02c3b12ff890df`** ("CMRNLP-103: Remove empty
  resource statement", 2026-05-21; the tip of the `CMRNLP-103: v1.0 MCP server
  cleanup and pagination (#48)` work that bumped the server/tools to v1.0.0).
- Files read:
  - `util/cmr/client.py` — HTTP client, endpoints, `search_after` pagination
  - `util/cmr/search_tools.py` — UMM-JSON `normalize_*` parsers, `fetch_association_ids`,
    spatial/temporal/cloud-cover formatting
  - `tools/get_collections/tool.py`, `tools/get_granules/tool.py`,
    `tools/get_variables/tool.py`, `tools/get_services/tool.py`
  - `util/pagination.py` — opaque cursor encode/decode
  - `docs/consumers/SUPPORTED_PARAMETERS.md` — their own MCP-arg → CMR-param → UMM-path map

The UMM schema versions quoted below are the ones NASA's `SUPPORTED_PARAMETERS.md`
pins at this commit. **Re-verify against the live schema at implementation time** —
versions move.

| Concept | UMM schema (per NASA doc @ pinned commit) |
|---|---|
| UMM-C (collection) | v1.18.3 |
| UMM-G (granule)    | v1.6.5 |
| UMM-V (variable)   | v1.9.0 |
| UMM-S (service)    | v1.5.3 |

---

## Common HTTP / client conventions (`util/cmr/client.py`)

- **Base URL:** `https://cmr.earthdata.nasa.gov` (env-overridable via `CMR_URL`).
- **Format:** all five search tools hit the **`.umm_json`** variants, so the
  response carries `{"items": [{"meta": {...}, "umm": {...}}, ...]}`. Parse the
  typed UMM record from `umm`, and CMR-managed identifiers from `meta`.
- **Endpoints used:**

  | concept_type | endpoint |
  |---|---|
  | collection | `/search/collections.umm_json` |
  | granule    | `/search/granules.umm_json` |
  | variable   | `/search/variables.umm_json` |
  | service    | `/search/services.umm_json` |
  | tool       | `/search/tools.umm_json` |
  | citation   | `/search/citations.umm_json` |

  Two non-`.umm_json` endpoints are also used for special cases:
  `/search/collections.json?include_tags=edsc.*` (the legacy `.json` feed is the
  only one exposing EDSC tags such as `edsc.extra.serverless.gibs`), and
  `/search/concepts/{concept_id}/{revision_id}.umm_json` (single-revision fetch).

- **Header:** every request sends a `Client-Id` header. CMR asks all clients to
  identify themselves; **we must set our own** `Client-Id` (e.g.
  `earthdata-retrieval-mcp`). Do *not* copy NASA's id.
- **Timeouts:** NASA uses 30 s for single-concept fetches, 60 s for paged search.
- **Method:** GET by default. **Spatial queries POST** a GeoJSON shapefile as
  multipart `files={"shapefile": (...)}` instead of a query param (see Spatial
  below).
- **Error handling (pattern to mirror with httpx + tenacity in 2.2):** NASA
  raises on `response.raise_for_status()` and pulls a concise message from the
  JSON body (`errors[]`, then `message`/`error`, then raw text). Our hard rule:
  **retry 5xx/timeouts, never 4xx** — a 4xx is a malformed query and retrying
  just wastes calls.

### Pagination — `search_after` (the only correct way past 1M / 2k results)

NASA uses CMR's **`search_after`** scheme, not `page_num`/offset (deep offset
paging is capped and discouraged by CMR). Mechanics, straight from `client.py`:

1. First request: send query params + `page_size` (their client default 500;
   the MCP tools cap user `limit` at **50**, default **10**). No special header.
2. CMR returns a page and three response **headers**:
   - `CMR-Hits` → total matching results (→ `total_hits`)
   - `CMR-Took` → server processing time in ms
   - `CMR-Search-After` → opaque token for the *next* page
3. Next request: resend the **identical** query params and pass the token back as
   the request **header** `CMR-Search-After: <token>`.
4. Stop when there is no `CMR-Search-After` header or the page returned fewer
   items than `page_size`.
5. `page_size=0` is a **count-only** query: one page, empty `items`, `total_hits`
   populated from `CMR-Hits`. Useful for `estimate_retrieval_size` later.

**Cursor opacity (their MCP layer, `util/pagination.py`):** the raw
`CMR-Search-After` token is wrapped together with the original query params into a
URL-safe base64 cursor (`{"backend": "cmr", "value": {"token", "params", ...}}`).
Cursors are **query-scoped** — on each page they re-validate that the caller's
params match the params frozen in the cursor and reject a mismatch ("cannot change
search parameters when paginating"). That UX wrapper is *their* concern; for our
internal provider we can hold the raw `CMR-Search-After` token directly. The
load-bearing CMR fact is: **token lives in the `CMR-Search-After` request/response
header, and the query params must be byte-stable across pages.**

### Spatial & temporal formatting (`search_tools.py`)

- **Temporal** → a single `temporal` param formatted `"<start>,<end>"` with ISO
  8601 `Z` timestamps; either side may be empty (`"2020-01-01T00:00:00Z,"`).
- **Spatial** → accept WKT (`POLYGON`, `POINT`, `LINESTRING`), convert to GeoJSON
  and **POST as a multipart `shapefile`** (`application/geo+json`). CMR limits
  enforced client-side before submit: **1,000,000 bytes, 500 features, 5,000
  points** — oversized geometry raises rather than 4xx-ing at CMR. Polygons are
  reoriented (`orient_polygons`) because CMR is winding-order sensitive; NASA's
  prompt guidance separately warns to prefer `ENVELOPE`/bbox over hand-written
  `POLYGON` to dodge winding bugs. (CMR also supports `bounding_box=W,S,E,N`,
  `point=lon,lat`, `polygon=lon,lat,...` as plain GET params — simpler for our
  bbox-driven AOIs in Phase 6 and worth using instead of the shapefile POST when
  the AOI is a simple rectangle.)

---

## Tool 1 — `get_collections` (UMM-C)

- **Endpoint:** `/search/collections.umm_json`
- **Purpose for us:** the Layer-1 fetch in the capability merge (§4.2). One-shot
  by `concept_id` for `collection_capabilities()`, or keyword/filter search for
  `search_collections`.

### Request parameters (MCP arg → CMR param)

| CMR param | Notes |
|---|---|
| `keyword` | Free text. **AND logic** across all space-separated words; every word must appear *somewhere* in indexed metadata (title, summary, short name, GCMD keywords, platform/instrument, processing level, archive center…). More words = stricter. Wrap in escaped quotes for an exact phrase; `*`/`?` wildcards allowed. |
| `concept_id` | Exact `C<number>-<PROVIDER>` lookup. |
| `short_name` | Exact by default; `*`/`?` allowed. |
| `provider` | DAAC short name. **Caveat:** providers are migrating to cloud IDs (`LPDAAC_ECS`→`LPCLOUD`, `PODAAC`→`POCLOUD`); a stale provider silently returns 0 hits. Omit when you know `short_name`. |
| `temporal` | `start,end` ISO 8601 (overlap filter on declared extent). |
| `polygon`/`point`/`bounding_box` (or shapefile POST) | Spatial overlap. |
| `platform[]` | Repeatable. |
| `instrument[]` | Repeatable. |
| `processing_level_id[]` | Repeatable — **the L2-vs-L3 discriminator** that drives `output_shape`. |
| `has_granules` | `true` filters out metadata-only shells. |
| `page_size`, `CMR-Search-After` | Pagination as above. |

CMR supports more we will likely want and NASA does *not* expose: `doi`,
`project`, `data_center`, `science_keywords[]` hierarchy, `updated_since`. Pull
exact names from the CMR collection-search docs, not from memory.

### UMM-JSON parsing (`normalize_collection_item`)

`meta.*` → `concept-id`, `native-id`, `revision-id`, `provider-id`,
`associations` (see variables/services below).

`umm.*` fields NASA extracts:
`ShortName`, `Version`, `EntryTitle`, `Abstract`, temporal extent
(`TemporalExtents.RangeDateTimes` → start/end + `is_ongoing`),
`Platforms[].ShortName` + nested `Instruments[].ShortName`,
**`ProcessingLevel.Id`**, `DOI.DOI`, `CollectionDataType`,
**`CollectionProgress`**, `ScienceKeywords`,
`SpatialExtent…BoundingRectangles` (→ `[W,S,E,N]`),
`DataCenters[]` (→ `{role, short_name}`), and
**`ArchiveAndDistributionInformation.FileDistributionInformation[]`** (→
`{format, media_type}` — the native-format list).

**Gap to close in 2.2 (important):** at this commit NASA does **not** parse
`DirectDistributionInformation`, `StandardProduct`, or `Purpose` from UMM-C. Our
capability merge needs all three (see the UMM-C field list below), so we read them
straight from the UMM-C record — a concrete case of "canon is UMM, not their repo."

---

## Tool 2 — `get_granules` (UMM-G)

- **Endpoint:** `/search/granules.umm_json`
- **Purpose for us:** `search_granules` / `check_availability` / size estimation
  (Phase 6 coverage). Confirms data *actually exists* for an AOI+time window —
  a collection appearing in `get_collections` only means its declared extent
  overlaps, not that granules exist.

### Request parameters

| CMR param | Notes |
|---|---|
| `collection_concept_id` | **Required** — scopes the search to one parent collection. |
| `temporal` | `start,end` ISO 8601. |
| `polygon`/`point`/`bounding_box` (or shapefile POST) | Spatial. |
| `cloud_cover` | `"min,max"` (0–100). Only meaningful for optical collections (Landsat/MODIS/VIIRS/Sentinel-2); omit for SAR/altimetry. |
| `day_night_flag` | `DAY` / `NIGHT` / `UNSPECIFIED`. |
| `sort_key` | e.g. `-start_date` (newest first). For NRT missions set this explicitly — CMR defaults to relevance, which can surface old granules first. |
| `page_size`, `CMR-Search-After` | Pagination. |

### UMM-JSON parsing (`normalize_granule_item`)

`meta.*` → ids as above. `umm.*`: `GranuleUR`, `RelatedUrls` (→ access URLs —
download requires EDL auth), `CloudCover`, `DataFormat`,
`DataGranule.DayNightFlag`, `DataGranule.ProducerGranuleId`,
`DataGranule.ProductionDateTime`, temporal `RangeDateTime`,
`SpatialExtent…BoundingRectangles` (MBR — encloses swath data, corners may be
empty), `OrbitCalculatedSpatialDomains` (→ orbit info),
`AdditionalAttributes[]` (→ `{name, values}`), and
**`DataGranule.ArchiveAndDistributionInformation` → size** (summed bytes → MB;
feeds `estimate_retrieval_size`).

---

## Tool 3 — `get_variables` (UMM-V)

- **Endpoint:** `/search/variables.umm_json`
- **Two-phase lookup** (pattern to reuse in 2.2; `fetch_association_ids`):
  1. **Phase 1 — discover association ids.** If a `collection_concept_id` is
     given, fetch the *collection* (`/search/collections.umm_json?concept_id=…`,
     `page_size=1`) and read **`items[0].meta.associations.variables`** — a list
     of variable concept-ids. (Collections own the variable↔collection linkage in
     `meta.associations`, so you go through the collection record, not a
     `collection_concept_id` filter on the variable endpoint.)
  2. **Phase 2 — fetch the variables.** Search `variables.umm_json` with
     `concept_id[]=<ids from phase 1>` and/or a free-text `keyword`.
- A keyword-only call (no collection) skips Phase 1 and searches globally.

### UMM-JSON parsing (`normalize_variable_item`)

`meta.concept-id` plus UMM-V: `Name`, `LongName`, `Definition`, `DataType`,
`Units`, **`Scale`**, **`Offset`**, **`FillValues`**, **`ValidRanges`**,
`Dimensions`, `StandardName` (CF), `ScienceKeywords`, `VariableType`,
`VariableSubType`, `Sets`, `MeasurementIdentifiers`, `SamplingIdentifiers`,
`RelatedUrls`. (Scale/offset/fill/valid-range are exactly what `catalog/enrichment.py`
pulls "UMM-Var first" in 2.5.)

---

## Tool 4 — `get_services` (UMM-S) — capability source of truth

- **Endpoint:** `/search/services.umm_json`
- **Same two-phase association lookup** as variables, via
  `fetch_association_ids(collection_concept_id, "services")` →
  `items[0].meta.associations.services` → then `services.umm_json?concept_id[]=…`.
  Also searchable by `keyword` and `type`.

### Request parameters

| CMR param | Notes |
|---|---|
| `concept_id[]` | Service ids discovered from the collection's `meta.associations.services`. |
| `keyword` | Free text. |
| `type` | Service type filter (e.g. Harmony / ESI / OPeNDAP / WMS). |
| `page_size`, `CMR-Search-After` | Pagination. |

### UMM-JSON parsing (`normalize_service_item`) — and the trap

NASA's parser passes the capability block **through raw**:
`Name`, `LongName`, **`Type`**, `Version`, `Description`, `URL`, `RelatedURLs`,
`AccessConstraints`, `UseConstraints`, `ServiceKeywords`,
**`ServiceOptions`** (subset types, supported projections, output formats —
*not* decomposed into booleans), `ServiceOrganizations` (→ `{roles, short_name}`),
`OperationMetadata`.

**This is the crux of Phase 2.3.** NASA does not compute per-capability flags at
all — it hands `ServiceOptions` to the agent verbatim. So **our** `_capabilities.py`
must parse, *per service*, from the UMM-S `ServiceOptions` block:
subsetting types (spatial/bbox, variable, temporal, shape, dimension),
concatenation/aggregation, reprojection, and supported output formats — building
one `ServiceCapability` per service. Per the hard rules and §4.2 we **ignore any
rolled-up top-level capability booleans** (CMR's collection-level `has_*` flags
are an unsatisfiable union across disjoint services — the TEMPO L2 trap), and
`find_service(plan)` must match **one whole service or return `None`**, never a
union. The exact `ServiceOptions` sub-field names (`Subset`,
`SupportedReformattings`, `SupportedOutputProjections`, `Aggregation`, …) come
from the **pinned UMM-S schema (v1.5.3 at this commit) — read the schema directly
in 2.3**, since NASA's code gives us no parsing to copy here.

---

## UMM-C fields the capability merge (§4.2/2.3) will need

Layer-1 (UMM-C) fields that drive routing, output shape, and advisory notes.
"Surfaced by NASA?" = whether `normalize_collection_item` extracts it at the
pinned commit; **No** means we must read it from the UMM-C record ourselves.

| UMM-C field | Used for | Surfaced by NASA @ `bb0ac5d`? |
|---|---|---|
| `ProcessingLevel` (`.Id`) | output shape heuristic: L2→swath, L3/L4→grid | **Yes** (`ProcessingLevel.Id`) |
| `ArchiveAndDistributionInformation` | native formats (`FileDistributionInformation[].Format` + `Media`) → "data as-is" / direct download path | **Yes** (format + media_type) |
| `DirectDistributionInformation` | in-region **S3** direct access (`Region`, `S3BucketAndObjectPrefixNames`, `S3CredentialsAPIEndpoint`) → skip-Harmony direct-fetch path | **No — we add it** |
| `CollectionProgress` | maturity → advisory (ACTIVE/COMPLETE/DEPRECATED/PLANNED) | **Yes** |
| `StandardProduct` | maturity/advisory (is this the standard product?) | **No — we add it** |
| `Purpose` | advisory text (e.g. "PROVISIONAL; see known issues") | **No — we add it** |

Three of the six (`DirectDistributionInformation`, `StandardProduct`, `Purpose`)
are not in NASA's normalized output at this commit — read them from UMM-C
directly. This is precisely why canon is the UMM-C schema, not the NASA repo.

---

## Carry-forward checklist for Phase 2.2 / 2.3

- [ ] httpx + tenacity client; **our own** `Client-Id`; retry 5xx/timeout, never 4xx.
- [ ] `.umm_json` endpoints; parse `meta` (ids, `associations`) + `umm` (typed record).
- [ ] `search_after` paging via `CMR-Search-After` header; `CMR-Hits` for totals;
      `page_size=0` for count-only.
- [ ] `temporal=start,end` (ISO-Z); bbox/point/polygon GET params for simple AOIs,
      GeoJSON shapefile POST for complex ones (respect 1MB/500-feature/5k-point limits).
- [ ] Variables & services go through the **collection's `meta.associations`**
      (two-phase), not a direct collection filter on those endpoints.
- [ ] `collection_capabilities()` reads all six UMM-C fields above — adding the
      three NASA omits — and parses `ServiceOptions` **per service** into
      `ServiceCapability`, ignoring rolled-up booleans.
</content>
</invoke>
