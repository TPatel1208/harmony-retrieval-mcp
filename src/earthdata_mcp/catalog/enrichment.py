"""Thin, advisory enrichment (PLAN.md §4.2, §4.4; task 2.5).

QA facts — scale, offset, fill values, valid range, units — are pulled from
**UMM-Var first** (authoritative). The curated YAML
(``catalog/data/products.yaml`` / ``variables.yaml``) adds only genuinely-additive
notes, each carrying ``owner`` + ``last_reviewed`` and flagged
**advisory/non-authoritative**. Curated notes **never override UMM facts** — the
merge is structured so curated input can only contribute advisory text, never a
fact key. Uncurated collections/variables pass through cleanly (no notes).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_PRODUCTS_YAML = Path(__file__).parent / "data" / "products.yaml"
_VARIABLES_YAML = Path(__file__).parent / "data" / "variables.yaml"

#: Marker attached to every curated note so consumers know it is not authoritative.
ADVISORY_FLAG = "advisory/non-authoritative"


def enrich_variable(
    umm_var: dict,
    *,
    short_name: str | None = None,
    curated: dict | None = None,
) -> dict:
    """Extract authoritative QA facts from a UMM-V record + advisory curated notes.

    ``umm_var`` is the typed UMM-V record (the ``umm`` block). Facts come straight
    from it; ``curated`` (or a lookup in ``variables.yaml`` by ``short_name`` +
    variable name) can only add an advisory note.
    """
    name = umm_var.get("Name")
    facts = {
        "name": name,
        "long_name": umm_var.get("LongName"),
        "data_type": umm_var.get("DataType"),
        "units": umm_var.get("Units"),
        # QA facts — UMM-Var is authoritative:
        "scale": umm_var.get("Scale"),
        "offset": umm_var.get("Offset"),
        "fill_values": umm_var.get("FillValues", []),
        "valid_ranges": umm_var.get("ValidRanges", []),
        "standard_name": umm_var.get("StandardName"),
    }
    if curated is None and short_name and name:
        curated = _load_variables().get(short_name, {}).get(name)
    facts["advisory_notes"] = _variable_notes(curated)
    return facts


def enrich_collection(
    collection: dict,
    *,
    curated: dict | None = None,
) -> dict:
    """Pass a normalized collection through, attaching advisory product notes.

    Uncurated collections get an empty ``advisory_notes`` list (clean passthrough).
    The collection's own UMM-derived fields are never modified.
    """
    short_name = collection.get("short_name")
    if curated is None and short_name:
        curated = _load_products().get(short_name)
    enriched = dict(collection)
    enriched["advisory_notes"] = _collection_notes(curated)
    return enriched


# -- note construction (curated → advisory, never facts) ------------------


def _collection_notes(curated: dict | None) -> list[dict[str, Any]]:
    if not curated:
        return []
    owner = curated.get("owner")
    last_reviewed = curated.get("last_reviewed")
    return [
        {
            "note": note,
            "owner": owner,
            "last_reviewed": last_reviewed,
            "authoritative": False,
            "advisory": ADVISORY_FLAG,
        }
        for note in curated.get("advisory", [])
    ]


def _variable_notes(curated: dict | None) -> list[dict[str, Any]]:
    if not curated or not curated.get("note"):
        return []
    return [
        {
            "note": curated["note"],
            "owner": curated.get("owner"),
            "last_reviewed": curated.get("last_reviewed"),
            "authoritative": False,
            "advisory": ADVISORY_FLAG,
        }
    ]


# -- curated YAML loaders --------------------------------------------------


def _load_products() -> dict[str, dict]:
    return _load_yaml(_PRODUCTS_YAML, "products")


def _load_variables() -> dict[str, dict]:
    return _load_yaml(_VARIABLES_YAML, "variables")


def _load_yaml(path: Path, key: str) -> dict[str, dict]:
    if not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text()) or {}
    return data.get(key) or {}
