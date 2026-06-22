"""``describe_dataset`` — resolve a ``dataset_`` handle to a full description.

The second of v1's two discovery primitives (PLAN.md §6 Phase 5). It resolves a
``dataset_`` handle within its workspace, returns the collection metadata stored
on the handle, fetches the collection's variables (``get_variables``), and
attaches **advisory** enrichment notes — QA facts come from UMM-Var first; curated
notes are always flagged ``advisory/non-authoritative`` and never override a fact.
"""

from __future__ import annotations

from earthdata_mcp.catalog.enrichment import enrich_variable
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.workspace.models import HandleType, handle_type_of
from earthdata_mcp.workspace.store import WorkspaceStore


async def describe_dataset(
    dataset_handle: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    cmr: CMRProvider | None = None,
    store: WorkspaceStore | None = None,
) -> dict:
    """Describe the dataset a ``dataset_`` handle refers to.

    Resolution is workspace-scoped: a handle owned by another workspace raises
    (``CrossWorkspaceError``), an unknown handle raises (``HandleNotFoundError``),
    and a non-``dataset_`` handle raises ``ValueError`` — this tool resolves
    dataset handles only.
    """
    if handle_type_of(dataset_handle) is not HandleType.DATASET:
        raise ValueError(
            f"describe_dataset expects a dataset_ handle, got {dataset_handle!r}"
        )

    cmr = cmr or CMRProvider()
    store = store or _default_store()

    record = await store.get_handle(workspace_id, dataset_handle)
    collection = record.payload.get("collection", {})
    concept_id = record.payload.get("concept_id") or collection.get("concept_id")
    short_name = collection.get("short_name")

    variables: list[dict] = []
    if concept_id:
        for var in await cmr.get_variables(concept_id):
            variables.append(
                enrich_variable(_to_umm_var(var), short_name=short_name)
            )

    return {
        "handle": dataset_handle,
        "concept_id": concept_id,
        "metadata": collection,
        "variables": variables,
        # Collection-level advisory notes, already flagged advisory by enrichment.
        "advisory_notes": collection.get("advisory_notes", []),
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
