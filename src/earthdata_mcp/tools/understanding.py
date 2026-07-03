"""``describe_dataset`` — resolve a ``dataset_`` handle to a description.

The second of v1's two discovery primitives (PLAN.md §6 Phase 5). It resolves a
``dataset_`` handle within its workspace, returns the collection metadata stored
on the handle, fetches the collection's variables (``get_variables``), and
attaches **advisory** enrichment notes — QA facts come from UMM-Var first; curated
notes are always flagged ``advisory/non-authoritative`` and never override a fact.

The response is **compact by default**: a 400-variable collection's full
UMM-Var enrichment plus the whole collection metadata blob dwarfs what an agent
needs to decide what to request next. ``detail=True`` opts into today's full
shape (every enriched QA fact, the complete metadata echo).
"""

from __future__ import annotations

from xml.etree import ElementTree as ET

from earthdata_mcp.catalog.enrichment import enrich_variable
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.providers.opendap._dmr import parse_dmr
from earthdata_mcp.providers.opendap._planning import _opendap_url_of
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.workspace.handles import resolve_dataset
from earthdata_mcp.workspace.store import WorkspaceStore

#: Explains an empty UMM-Var catalog with no DMR fallback available either: the
#: fact, the retrieve-time consequence, and where a legal variable name comes
#: from instead.
_NO_UMM_VAR_NOTE = (
    "This collection has no UMM-Var records associated in CMR. Retrieve-time "
    "variable-name validation will fall back to granule-structure discovery "
    "where available, or be skipped entirely. Legal variable names come from "
    "the product's data dictionary or its granule structure — a full "
    "group-qualified path is the safest form to pass to retrieve_subset."
)

#: Explains an empty UMM-Var catalog when a granule DMR filled the gap: the
#: entries below are real paths from one granule's structure, not CMR's
#: curated catalog, so no units/long_name/fill-value facts are attached.
_DMR_SOURCE_NOTE = (
    "This collection has no UMM-Var records associated in CMR. The variables "
    "below are the full group-qualified paths discovered from one recent "
    "granule's DMR (DAP4 structure) — real paths from a real file, not CMR's "
    "curated catalog, so no units/long_name/fill-value facts are attached. "
    "Retrieve-time variable-name validation will rely on this same "
    "granule-structure discovery. Pass a path shown here as-is to "
    "retrieve_subset."
)

#: Core identification fields kept in the compact ``metadata`` echo.
_COMPACT_METADATA_FIELDS = (
    "concept_id",
    "short_name",
    "version",
    "entry_title",
    "processing_level",
)


async def describe_dataset(
    dataset_handle: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    detail: bool = False,
    *,
    cmr: CMRProvider | None = None,
    store: WorkspaceStore | None = None,
) -> dict:
    """Describe the dataset a ``dataset_`` handle refers to.

    Resolution is workspace-scoped: a handle owned by another workspace raises
    (``CrossWorkspaceError``), an unknown handle raises (``HandleNotFoundError``),
    and a non-``dataset_`` handle raises ``ValueError`` — this tool resolves
    dataset handles only.

    Compact by default: each variable trims to name/long_name/units/
    advisory_notes, and ``metadata`` trims to core identification fields
    (:data:`_COMPACT_METADATA_FIELDS`). Pass ``detail=True`` for the full
    per-variable UMM-Var enrichment and the complete collection metadata.
    ``variable_count`` is always the true total, in both modes, so no
    trimming is ever silent.
    """
    cmr = cmr or CMRProvider()
    store = store or _default_store()

    concept_id = await resolve_dataset(store, workspace_id, dataset_handle)
    record = await store.get_handle(workspace_id, dataset_handle)
    collection = record.payload.get("collection", {})
    short_name = collection.get("short_name")

    variables: list[dict] = [
        enrich_variable(_to_umm_var(var), short_name=short_name)
        for var in await cmr.get_variables(concept_id)
    ]

    metadata = collection if detail else _compact_metadata(collection)
    variable_source = "umm-var" if variables else "none"

    dmr_paths = await _dmr_variable_paths(cmr, concept_id) if not variables else None

    if dmr_paths is not None:
        variable_source = "opendap-dmr"
        out_variables: list[dict] = [{"name": p} for p in dmr_paths]
        variable_count = len(dmr_paths)
    else:
        out_variables = variables if detail else [_compact_variable(v) for v in variables]
        variable_count = len(variables)

    out = {
        "handle": dataset_handle,
        "concept_id": concept_id,
        "metadata": metadata,
        "variables": out_variables,
        "variable_count": variable_count,
        # Collection-level advisory notes, already flagged advisory by enrichment.
        "advisory_notes": collection.get("advisory_notes", []),
        "variable_source": variable_source,
    }
    if variable_source == "opendap-dmr":
        out["variables_note"] = _DMR_SOURCE_NOTE
    elif variable_source != "umm-var":
        out["variables_note"] = _NO_UMM_VAR_NOTE
    return out


async def _dmr_variable_paths(cmr: CMRProvider, concept_id: str) -> list[str] | None:
    """Variable paths from one recent granule's DMR, or ``None`` on any fail-soft
    condition (no granules, no OPeNDAP URL, fetch error, malformed XML) — the
    caller then keeps the plain "no UMM-Var, no fallback either" note.

    The one source of last resort when the collection has no UMM-Var catalog
    (PRD 010 Task 2, reusing PRD 009's pure DMR parser and granule search).
    Never raises: an unresolvable structure here is a description gap, not a
    plan-time rejection like :func:`~earthdata_mcp.providers.opendap._planning.plan_subset`.
    """
    try:
        granules = await cmr.search_granules(concept_id, limit=1)
    except Exception:
        return None
    if not granules:
        return None
    url = _opendap_url_of(granules[0])
    if not url:
        return None
    try:
        xml_text = await cmr.get_dmr(url)
    except Exception:
        return None
    if not xml_text:
        return None
    try:
        tree = parse_dmr(xml_text)
    except ET.ParseError:
        return None
    return sorted(tree.variables)


def _compact_metadata(collection: dict) -> dict:
    """Core identification fields only — never the full metadata blob."""
    return {k: collection.get(k) for k in _COMPACT_METADATA_FIELDS}


def _compact_variable(var: dict) -> dict:
    """Name/long_name/units/advisory_notes only — never the full QA enrichment."""
    return {
        "name": var.get("name"),
        "long_name": var.get("long_name"),
        "units": var.get("units"),
        "advisory_notes": var.get("advisory_notes", []),
    }


def _to_umm_var(var: dict) -> dict:
    """Re-shape a normalized variable record into the UMM-V keys ``enrich_variable``
    reads, so QA facts are pulled from UMM-Var and curated notes stay advisory."""
    return {
        "Name": var.get("name"),
        "LongName": var.get("long_name"),
        "DataType": var.get("data_type"),
        "Units": var.get("units"),
        "Scale": var.get("scale"),
        "Offset": var.get("offset"),
        "FillValues": var.get("fill_values", []),
        "ValidRanges": var.get("valid_ranges", []),
        "StandardName": var.get("standard_name"),
    }
