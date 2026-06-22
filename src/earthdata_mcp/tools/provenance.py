"""Provenance + citation tools (PLAN.md §6 Phase 8, §4.5).

Two read-only tools that close the v1 surface:

* :func:`get_provenance` — a handle's lineage, answered by the durable,
  spec-keyed provenance DAG: its ancestry (the recursive CTE, §4.5) plus its
  first-class events (``created``/``materialized``/``expired``/
  ``re-materialized``). Every read is workspace-scoped, so lineage never crosses a
  workspace boundary.
* :func:`cite_dataset` — the official DOI and formal citation strings for a
  ``dataset_`` handle, read from **CMR's own records** (UMM-C ``DOI`` +
  ``CollectionCitations``), never hand-rolled (CLAUDE.md constraint). Reuses
  NASA's ``get_citations`` intent via :meth:`CMRProvider.get_citations`.

Both are dependency-injectable (tests pass stores/CMR directly) and otherwise
build lazy process defaults on first use, so importing this module — and
``server.py`` — touches no DB or network.
"""

from __future__ import annotations

from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.tools.discovery import DEFAULT_WORKSPACE, _default_store
from earthdata_mcp.tools.retrieval import _default_provenance
from earthdata_mcp.workspace.models import HandleType, handle_type_of
from earthdata_mcp.workspace.provenance import ProvenanceStore
from earthdata_mcp.workspace.store import WorkspaceStore


async def get_provenance(
    handle: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    provenance: ProvenanceStore | None = None,
) -> dict:
    """Return ``handle``'s lineage: its ancestry (recursive CTE) + its events.

    Resolves ``handle`` within the workspace first (cross-workspace access is
    denied, §4.6), then walks ancestors shortest-depth-first and lists the
    handle's first-class provenance events newest-first. Returns
    ``{handle, ancestors: [{handle, depth}], events: [{event_type, detail, created_at}]}``.
    """
    store = store or _default_store()
    provenance = provenance or _default_provenance()

    # Isolation gate: raises CrossWorkspaceError / HandleNotFoundError.
    await store.get_handle(workspace_id, handle)

    ancestors = await provenance.ancestry(workspace_id, handle)
    events = await provenance.events(workspace_id, handle)
    return {
        "handle": handle,
        "ancestors": [{"handle": a.handle, "depth": a.depth} for a in ancestors],
        "events": [
            {
                "event_type": e.event_type,
                "detail": e.detail,
                "created_at": _isoformat(e.created_at),
            }
            for e in events
        ],
    }


async def cite_dataset(
    dataset_handle: str,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    store: WorkspaceStore | None = None,
    cmr: CMRProvider | None = None,
) -> dict:
    """Return the official DOI + formal citation strings for a ``dataset_`` handle.

    Resolves the ``dataset_`` handle → ``concept_id`` (workspace-scoped), then asks
    CMR for the dataset's own citation records (DOI + ``CollectionCitations``). The
    strings come from CMR's records, never hand-rolled. Graceful when a collection
    publishes no DOI or citations — empty fields, never an error.
    """
    store = store or _default_store()
    cmr = cmr or CMRProvider()

    if handle_type_of(dataset_handle) is not HandleType.DATASET:
        raise ValueError(f"expected a dataset_ handle, got {dataset_handle!r}")

    record = await store.get_handle(workspace_id, dataset_handle)  # isolation gate
    concept_id = record.payload.get("concept_id")
    if not concept_id:
        raise ValueError(
            f"dataset handle {dataset_handle!r} payload missing 'concept_id'"
        )

    citations = await cmr.get_citations(concept_id)
    return {
        "handle": dataset_handle,
        "concept_id": citations.get("concept_id", concept_id),
        "doi": citations.get("doi"),
        "doi_authority": citations.get("doi_authority"),
        "collection_citations": citations.get("collection_citations", []),
        "reference_citation_count": citations.get("reference_citation_count", 0),
    }


def _isoformat(value: object) -> str | None:
    """Render a timestamp as ISO-8601 when present, else ``None``."""
    return value.isoformat() if hasattr(value, "isoformat") else value
