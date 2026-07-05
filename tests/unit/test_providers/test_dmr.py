"""Pure DMR (DAP4 Dataset Metadata Response) parser tests (PRD 009).

No network, no CMR — every test loads a trimmed-but-real fixture from
``tests/fixtures/*.dmr.xml`` and asserts on the parsed tree, mirroring the
collection-archetype corpus style in ``test_opendap.py``. See
``docs/opendap_quirk_ledger.md`` for the quirk each fixture pins.
"""

from __future__ import annotations

from pathlib import Path

from earthdata_mcp.providers.opendap._dmr import group_geolocation, parse_dmr, resolve_variables

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


def _load(name: str) -> str:
    return (_FIXTURES / name).read_text()


def test_parse_extracts_flat_variable_paths_and_dims() -> None:
    tree = parse_dmr(_load("gldas_flat.dmr.xml"))
    assert set(tree.variables) == {"/lat", "/lon", "/time", "/precipitation"}
    assert tree.variables["/precipitation"].dim_paths == ("/time", "/lat", "/lon")


def test_parse_extracts_grouped_variable_paths_across_parallel_beams() -> None:
    """ATL08-style: each beam is a parallel sibling group, group-local lat/lon,
    no root coordinates at all."""
    tree = parse_dmr(_load("atl08_grouped.dmr.xml"))
    assert "/gt1l/land_segments/terrain/h_te_best_fit" in tree.variables
    assert "/gt1r/land_segments/terrain/h_te_best_fit" in tree.variables
    assert "/gt2l/land_segments/terrain/h_te_best_fit" in tree.variables
    assert "/gt1l/land_segments/latitude" in tree.variables
    assert "/lat" not in tree.variables and "/lon" not in tree.variables


def test_parse_captures_standard_name_attribute() -> None:
    tree = parse_dmr(_load("atl08_grouped.dmr.xml"))
    assert tree.variables["/gt1l/land_segments/latitude"].standard_name == "latitude"
    assert tree.variables["/gt1l/land_segments/terrain/h_te_best_fit"].standard_name is None


# -- group_geolocation: nearest-ancestor-group lat/lon lookup ---------------


def test_group_geolocation_finds_lat_lon_two_levels_above_variable() -> None:
    """ATL08: h_te_best_fit's own group (.../terrain) has no coordinates —
    they live in the grandparent group (.../land_segments)."""
    tree = parse_dmr(_load("atl08_grouped.dmr.xml"))
    lat, lon = group_geolocation(tree, "/gt1l/land_segments/terrain/h_te_best_fit")
    assert lat == "/gt1l/land_segments/latitude"
    assert lon == "/gt1l/land_segments/longitude"


def test_group_geolocation_does_not_cross_sibling_beam_groups() -> None:
    """gt1r's variable must resolve to gt1r's own coordinates, never gt1l's."""
    tree = parse_dmr(_load("atl08_grouped.dmr.xml"))
    lat, lon = group_geolocation(tree, "/gt1r/land_segments/terrain/h_te_best_fit")
    assert lat == "/gt1r/land_segments/latitude"
    assert lon == "/gt1r/land_segments/longitude"


def test_group_geolocation_falls_back_to_root_group() -> None:
    """SMAP: sm_surface lives under Geophysical_Data, but lat/lon are at the
    dataset root — the nearest ancestor *with* coordinates, not the nearest
    group overall."""
    tree = parse_dmr(_load("smap_single_group.dmr.xml"))
    lat, lon = group_geolocation(tree, "/Geophysical_Data/sm_surface")
    assert lat == "/latitude"
    assert lon == "/longitude"


def test_group_geolocation_none_when_no_ancestor_has_coordinates() -> None:
    tree = parse_dmr(_load("gldas_flat.dmr.xml"))
    lat, lon = group_geolocation(tree, "/nonexistent_var")
    assert (lat, lon) == (None, None)


# -- resolve_variables: full-path / bare-leaf / expansion / not-found -------


def test_resolve_full_path_verbatim() -> None:
    tree = parse_dmr(_load("atl08_grouped.dmr.xml"))
    resolved, expansions = resolve_variables(
        tree, ("/gt1l/land_segments/terrain/h_te_best_fit",)
    )
    assert resolved == ("/gt1l/land_segments/terrain/h_te_best_fit",)
    assert expansions == {}


def test_resolve_unknown_full_path_raises_with_close_matches() -> None:
    tree = parse_dmr(_load("atl08_grouped.dmr.xml"))
    try:
        resolve_variables(tree, ("/gt1l/land_segments/terrain/h_te_best_ft",))
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "h_te_best_fit" in str(exc)


def test_resolve_group_qualified_path_without_leading_slash() -> None:
    """UMM-Var's own ``Name`` field for a grouped variable (e.g. CMR's
    ``key_science_data/column_amount`` for OMHCHOd) never carries a leading
    slash — describe_dataset hands that string back to a consumer verbatim, so
    passing it straight into retrieve_subset must resolve exactly like the
    full ``/``-prefixed path, not fall through to bare-leaf (last-segment-only)
    matching, which would find zero matches and raise."""
    tree = parse_dmr(_load("atl08_grouped.dmr.xml"))
    resolved, expansions = resolve_variables(
        tree, ("gt1l/land_segments/terrain/h_te_best_fit",)
    )
    assert resolved == ("/gt1l/land_segments/terrain/h_te_best_fit",)
    assert expansions == {}


def test_resolve_unknown_group_qualified_path_without_leading_slash_raises() -> None:
    tree = parse_dmr(_load("atl08_grouped.dmr.xml"))
    try:
        resolve_variables(tree, ("gt1l/land_segments/terrain/h_te_best_ft",))
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "h_te_best_fit" in str(exc)


def test_resolve_bare_leaf_unique_match() -> None:
    tree = parse_dmr(_load("smap_single_group.dmr.xml"))
    resolved, expansions = resolve_variables(tree, ("sm_surface",))
    assert resolved == ("/Geophysical_Data/sm_surface",)
    assert expansions == {}


def test_resolve_bare_leaf_expands_across_parallel_sibling_groups() -> None:
    """The ATL08 case: h_te_best_fit is ambiguous by design — the user gets
    every beam's copy, and the expansion is recorded for provenance."""
    tree = parse_dmr(_load("atl08_grouped.dmr.xml"))
    resolved, expansions = resolve_variables(tree, ("h_te_best_fit",))
    assert set(resolved) == {
        "/gt1l/land_segments/terrain/h_te_best_fit",
        "/gt1r/land_segments/terrain/h_te_best_fit",
        "/gt2l/land_segments/terrain/h_te_best_fit",
    }
    assert set(expansions["h_te_best_fit"]) == set(resolved)


def test_resolve_bare_leaf_no_match_raises() -> None:
    tree = parse_dmr(_load("atl08_grouped.dmr.xml"))
    try:
        resolve_variables(tree, ("totally_made_up_variable",))
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "totally_made_up_variable" in str(exc)
