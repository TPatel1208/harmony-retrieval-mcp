"""Thin advisory enrichment (PLAN.md task 2.5).

UMM facts are authoritative; curated notes are advisory and never override them.
"""

from __future__ import annotations

from earthdata_mcp.catalog.enrichment import (
    ADVISORY_FLAG,
    enrich_collection,
    enrich_variable,
)


def test_variable_facts_come_from_umm_var() -> None:
    out = enrich_variable(
        {
            "Name": "no2",
            "Units": "molecules/cm2",
            "Scale": 0.1,
            "Offset": 5,
            "FillValues": [{"Value": -1.0}],
            "ValidRanges": [{"Min": 0, "Max": 100}],
        }
    )
    assert out["scale"] == 0.1
    assert out["offset"] == 5
    assert out["units"] == "molecules/cm2"
    assert out["fill_values"] == [{"Value": -1.0}]


def test_curated_collection_note_is_flagged_advisory() -> None:
    out = enrich_collection({"short_name": "TEMPO_NO2_L3"})
    assert out["advisory_notes"]
    note = out["advisory_notes"][0]
    assert note["authoritative"] is False
    assert note["advisory"] == ADVISORY_FLAG
    assert note["owner"]
    assert note["last_reviewed"]


def test_uncurated_collection_passes_through_cleanly() -> None:
    out = enrich_collection({"short_name": "NOT_CURATED_XYZ"})
    assert out["advisory_notes"] == []
    # Original fields are untouched.
    assert out["short_name"] == "NOT_CURATED_XYZ"


def test_curated_note_never_overrides_umm_facts() -> None:
    # A curated entry that tries to smuggle in a fact key must be ignored.
    out = enrich_variable(
        {"Name": "no2", "Scale": 0.1},
        curated={"note": "advisory text", "scale": 999, "owner": "x@y.z"},
    )
    assert out["scale"] == 0.1  # UMM wins
    assert out["advisory_notes"][0]["note"] == "advisory text"
    assert out["advisory_notes"][0]["authoritative"] is False
