"""Pure DMR (DAP4 Dataset Metadata Response) parser — no network, no CMR.

Structure discovery of last resort: when a granule's collection has no UMM-Var
catalog (ATL08 and other true per-beam-group products), the granule's own DMR
is the one authoritative source for what variables and coordinates actually
exist. This module turns DMR XML text into a :class:`DmrTree` — full variable
paths, each variable's own dimensions, and enough to look up a variable's
group-local geolocation — and nothing else: it never fetches, never talks to
CMR, and never guesses a name the XML didn't confirm.

See ``docs/opendap_quirk_ledger.md`` for the never-guess-coordinates quirk this
module exists to serve, and ``tests/unit/test_providers/test_dmr.py`` for the
fixture corpus (one row per real-product layout: ATL08 grouped/parallel-beam,
SMAP single-group, GLDAS flat, TEMPO grouped).
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass
from xml.etree import ElementTree as ET

#: DMR element tags that are never a variable, even though some (Attribute)
#: carry a ``name`` attribute the way a variable does.
_SKIP_TAGS = frozenset({"Dimension", "Attribute"})

_LAT_LEAF_NAMES = ("lat", "latitude", "y")
_LON_LEAF_NAMES = ("lon", "longitude", "x")


@dataclass(frozen=True)
class DmrVariable:
    """One DMR variable: its full slash path, own dimensions, and standard_name."""

    path: str
    dim_paths: tuple[str, ...]
    standard_name: str | None = None


@dataclass(frozen=True)
class DmrTree:
    """The granule's structure: every variable, keyed by its full DAP4 path."""

    variables: dict[str, DmrVariable]


def parse_dmr(xml_text: str) -> DmrTree:
    """Parse DMR XML text into a :class:`DmrTree`.

    Any element that isn't a ``Group``, ``Dimension``, or ``Attribute`` and
    carries a ``name`` is treated as a variable (DAP4's scalar/array type tags
    — ``Float32``, ``Int16``, ``String``, etc. — are numerous and collection-
    specific; matching everything else is more robust than enumerating them).
    """
    root = ET.fromstring(xml_text)
    variables: dict[str, DmrVariable] = {}
    _walk(root, "", variables)
    return DmrTree(variables=variables)


def _local_tag(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _walk(node: ET.Element, group_path: str, variables: dict[str, DmrVariable]) -> None:
    for child in node:
        tag = _local_tag(child.tag)
        name = child.get("name")
        if name is None:
            continue
        if tag == "Group":
            _walk(child, f"{group_path}/{name}", variables)
        elif tag in _SKIP_TAGS:
            continue
        else:
            path = f"{group_path}/{name}"
            variables[path] = DmrVariable(
                path=path,
                dim_paths=_dim_paths(child),
                standard_name=_standard_name(child),
            )


def _dim_paths(var_node: ET.Element) -> tuple[str, ...]:
    names = (d.get("name") for d in var_node if _local_tag(d.tag) == "Dim")
    return tuple(name for name in names if name is not None)


def _standard_name(var_node: ET.Element) -> str | None:
    for attr in var_node:
        if _local_tag(attr.tag) == "Attribute" and attr.get("name") == "standard_name":
            value = next((v for v in attr if _local_tag(v.tag) == "Value"), None)
            if value is not None and value.text:
                return value.text
    return None


def group_geolocation(tree: DmrTree, var_path: str) -> tuple[str | None, str | None]:
    """The nearest ancestor group (walking up from ``var_path``'s own group to
    the root) whose direct children include both a latitude and a longitude
    array. ``(None, None)`` when ``var_path`` is unknown or no ancestor group
    has both — the caller must never guess a coordinate name in that case.
    """
    if var_path not in tree.variables:
        return (None, None)
    segments = var_path.strip("/").split("/")[:-1]
    for depth in range(len(segments), -1, -1):
        group = "/" + "/".join(segments[:depth]) if depth else ""
        lat = _direct_child_coord(tree, group, _LAT_LEAF_NAMES, "latitude")
        lon = _direct_child_coord(tree, group, _LON_LEAF_NAMES, "longitude")
        if lat is not None and lon is not None:
            return (lat, lon)
    return (None, None)


def _direct_child_coord(
    tree: DmrTree, group: str, leaf_names: tuple[str, ...], standard_name: str
) -> str | None:
    for path, var in tree.variables.items():
        parent = path.rsplit("/", 1)[0]
        if parent != group:
            continue
        leaf = path.rsplit("/", 1)[-1].lower()
        if leaf in leaf_names or (var.standard_name or "").lower() == standard_name:
            return path
    return None


def resolve_variables(
    tree: DmrTree, variables: tuple[str, ...]
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    """Resolve each requested name against the DMR tree's real paths.

    A full path (starts with ``/``, or contains ``/`` at all — UMM-Var's own
    ``Name`` field for a grouped variable, e.g. ``key_science_data/column_amount``,
    is group-qualified but never carries a leading slash, and describe_dataset
    hands that string back to a consumer verbatim) must exist verbatim once a
    leading ``/`` is assumed. A bare leaf name (no ``/`` at all) is matched
    case-insensitively against every path's last segment: one match resolves to
    it; more than one match — the parallel-sibling-group case (ICESat-2's six
    beams) — expands to *all* of them, recorded in the returned expansion map
    (``original name -> resolved paths``) so the caller can surface it in
    provenance. Zero matches, in either case, raises ``ValueError`` with difflib
    close matches over the tree's full paths — unlike CMR UMM-V resolution, the
    DMR is authoritative ground truth for this granule, so an unresolvable name
    must never reach OPeNDAP.
    """
    all_paths = sorted(tree.variables)
    resolved: list[str] = []
    expansions: dict[str, tuple[str, ...]] = {}
    for user_var in variables:
        if "/" in user_var:
            full_path = user_var if user_var.startswith("/") else f"/{user_var}"
            if full_path in tree.variables:
                resolved.append(full_path)
                continue
            raise ValueError(_unknown_variable_message(user_var, all_paths))
        leaf_lower = user_var.lower()
        matches = [p for p in all_paths if p.rsplit("/", 1)[-1].lower() == leaf_lower]
        if not matches:
            raise ValueError(_unknown_variable_message(user_var, all_paths))
        if len(matches) == 1:
            resolved.append(matches[0])
        else:
            resolved.extend(matches)
            expansions[user_var] = tuple(matches)
    return tuple(resolved), expansions


def _unknown_variable_message(name: str, all_paths: list[str]) -> str:
    matches = difflib.get_close_matches(name, all_paths, n=5)
    suffix = f" (closest matches: {matches})" if matches else ""
    return f"Variable {name!r} not found in this granule's structure{suffix}"
