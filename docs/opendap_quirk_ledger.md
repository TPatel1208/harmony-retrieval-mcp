# Hyrax/DAP4 quirk ledger

Every reverse-engineered Hyrax/DAP4 quirk the OPeNDAP provider handles, pinned
by a named row in the collection-archetype corpus
(`test_constraint_expression_archetype_corpus` in
`tests/unit/test_providers/test_opendap.py`). Before this ledger the knowledge
was trapped in commit messages — a fix landing today risked un-fixing a past
one because nothing named the full set of quirks in one place.

**Convention:** a new quirk gets a ledger entry here plus a corpus row (or a
new archetype) in `test_opendap.py` — never an ad-hoc, un-cross-referenced
test. See `_serialization.py`'s `build_constraint_expression` for the pure
core every row exercises directly (no network, no CMR, no filesystem I/O).

| Quirk | What Hyrax does | DAAC / collection it surfaced on | Corpus row(s) |
|---|---|---|---|
| Grouped netCDF4 fully-qualified names | A grouped variable/coordinate must be projected by its full `/group/leaf` path, and even a root-level variable needs a leading `/` — an unqualified name is a 400. | TEMPO L2 (grouped); GLDAS (flat, still needs the leading slash) | `flat_root_variable_gets_leading_slash`, `grouped_variable_preserves_existing_slash`, `flat_bbox_emits_fqn_for_flat_coords_no_geometry`, `grouped_bbox_emits_fqn_for_grouped_coords_no_geometry` |
| Coordinate-aware DAP4 hyperslabs | A bbox subset must clip *both* the lat/lon coordinate arrays *and* every projected data variable to matching inclusive index ranges — projecting a clipped coordinate alone does not shrink the payload. | GLDAS (flat), TEMPO (grouped) | `grid_edge_bbox_ascending_axis_hyperslab`, `grouped_coords_compose_with_hyperslab` |
| Descending-axis index ordering | A north-to-south (descending) latitude axis must still yield `low <= high` in the DAP4 bracket — a naive division without accounting for `step`'s sign flips or drops the range. | GLDAS-style descending latitude grids | `grid_edge_bbox_descending_latitude_orders_indices_correctly` |
| Grid-edge bbox translation (UMM-C convention) | UMM-C's bounding-coordinate extent describes the grid's outer *edges*, not the first cell's coordinate value — GLDAS's published west/south edge is exactly a half-cell short of its real `lon[0]`/`lat[0]`. `AxisGeometry.origin` must be the edge offset by half a step, not the raw edge. | GLDAS (confirmed against production Cloud OPeNDAP) | All `grid_edge_bbox_*` rows (the axis fixtures they share, `_GRID_LAT_AXIS`/`_GRID_LON_AXIS`, are pre-computed via this edge→origin translation) |
| Bracket-count-must-match-dimension-count | A variable's *every* dimension needs a positional bracket, in the variable's own order, or Hyrax rejects the whole request outright (a 2-bracket CE on a 3-D variable is a 400, not a silently-misapplied slice). A non-spatial dimension (e.g. a per-granule `time` of length 1) needs its own full-range bracket. | GLDAS (per-granule `time` dimension alongside `lat`/`lon`) | `grid_edge_bbox_var_dims_full_ranges_non_spatial_dimension` |
| Unverified-shape fallback | When a variable's dimension shape can't be safely resolved from UMM-V, it must be projected whole-array (no bracket at all) rather than guessing a bracket count — geometry for the rest of the request is unaffected. | Any collection with a variable UMM-V under-reports | `grid_edge_bbox_var_dims_unresolved_variable_is_whole_array` |
| Antimeridian wrap | A bbox crossing ±180° (`west > east`) must fall back to whole-longitude projection rather than emitting a wrong or split slab (v1: correct, not minimal). Latitude still clips normally. | Any grid collection queried across the dateline | `grid_edge_bbox_antimeridian_falls_back_to_whole_longitude` |
| Box-exceeds-extent clamping | A requested box wider than the grid's own extent must clamp to the axis's valid `[0, length-1]` indices instead of producing an out-of-range or inverted bracket. | Any grid collection with a global or near-global query | `grid_edge_bbox_box_exceeding_extent_is_clamped` |
| Degenerate point box | A zero-area box (`west == east`, `south == north`) must still resolve to at least one cell, not an empty or inverted range. | Any point-like bbox query against a grid collection | `grid_edge_bbox_degenerate_point_box_yields_one_cell` |
| CF-time projection for a real time coordinate | A collection with a real per-scanline/per-record `time` coordinate variable (as opposed to time-in-filename L3 products) must have it projected whole-array — omitting it leaves the response's `time` *dimension* present but without coordinate values, which xarray then degrades to a plain integer `RangeIndex`, breaking downstream time-based resampling. Projecting a nonexistent `/time` on a collection that has none is itself a 400, so this is opt-in per the resolved `coord_time`. | TROPOMI (swath, per-scanline `time`) | `swath_time_coordinate_projected_when_resolved` (pure CE); `test_submit_projects_time_when_coord_time_resolved` / `test_submit_omits_time_when_coord_time_not_resolved` (provider wiring); `test_plan_subset_resolves_time_coordinate_for_swath_collection` (CMR resolution) |
| Variable names with embedded spaces | Real MODIS variable names carry embedded spaces (e.g. `"250m 16 days NDVI"`); the DAP4 FQN must pass the name through verbatim — the space is percent-encoded later, once, when the whole constraint expression is URL-quoted (`_runtime.py`'s `_build_dap4_url`), never split or sanitized during CE assembly. | MOD13Q1 | `flat_names_with_spaces_pass_through_verbatim` |

## Out of scope for this ledger

Per the WS1 decision, this ledger and its corpus cover request *emission*
(the DAP4 constraint expression), not response *decoding*. Two related quirks
live on the read/concatenation path instead, pinned by their own existing
tests (unchanged by this ledger):

- **CF-time decode-before-subset** — a data variable's `time` coordinate must
  be CF-decoded to real datetimes *before* any time-range selection is
  applied on read, or the selection compares against raw encoded integers.
- **Grouped lazy-read flattening** — a grouped netCDF4 file's group hierarchy
  must be flattened before concatenation across granules on read.
