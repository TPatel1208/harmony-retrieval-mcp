"""``search_datasets`` — the handle-minting discovery primitive (PLAN.md §6 Phase 5).

v1 ships exactly two discovery tools; this is the first. It does **not** try to
out-search CMR — it KMS-normalizes the query, runs one ``search_collections``,
enriches each result with advisory notes, and mints a ``dataset_`` handle saved
to the caller's workspace. A capable agent composes the rest (the deferred
``discover_datasets``/``recommend_datasets``/… surface) from this primitive plus
NASA's server.

The handle ``payload`` is the **re-materializable spec** (CLAUDE.md hard rule):
the collection's ``concept_id``, its enriched metadata, and the search context —
never an ephemeral staged-output URL.
"""

from __future__ import annotations

from typing import Any

from earthdata_mcp.catalog.enrichment import enrich_collection
from earthdata_mcp.catalog.kms import KMSCatalog
from earthdata_mcp.db import create_engine, create_session_factory
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.workspace.models import HandleType
from earthdata_mcp.workspace.store import WorkspaceStore

#: Workspace used when a caller does not scope the call to one of its own.
DEFAULT_WORKSPACE = "default"

#: Filter keys we forward to ``CMRProvider.search_collections``. Anything else a
#: caller passes is dropped so junk never reaches the CMR query string.
_ALLOWED_FILTERS = frozenset(
    {
        "concept_id",
        "short_name",
        "provider",
        "temporal",
        "bounding_box",
        "processing_level_id",
        "has_granules",
        "limit",
    }
)

# Lazily-built default store. Stays ``None`` until the first un-injected call, so
# importing this module (and ``server.py``) constructs no engine and touches no
# DB or network — server.py's import contract.
_store: WorkspaceStore | None = None


def _default_store() -> WorkspaceStore:
    """Return the process default :class:`WorkspaceStore`, building it on first use."""
    global _store
    if _store is None:
        _store = WorkspaceStore(create_session_factory(create_engine()))
    return _store


async def search_datasets(
    query: str,
    filters: dict[str, Any] | None = None,
    workspace_id: str = DEFAULT_WORKSPACE,
    *,
    cmr: CMRProvider | None = None,
    store: WorkspaceStore | None = None,
    kms: KMSCatalog | None = None,
) -> dict:
    """Search collections and mint a ``dataset_`` handle per result.

    Returns ``{"datasets": [{"handle", "summary"}], "count": n}``. ``filters`` is
    an optional dict of CMR collection-search params (see ``_ALLOWED_FILTERS``).
    """
    cmr = cmr or CMRProvider()
    kms = kms or KMSCatalog()
    store = store or _default_store()

    keyword = _normalize_query(kms, query)
    collections = await cmr.search_collections(
        keyword=keyword, **_allowed(filters)
    )

    datasets: list[dict] = []
    for collection in collections:
        enriched = enrich_collection(collection)
        spec = {
            "concept_id": enriched.get("concept_id"),
            "collection": enriched,
            "search": {
                "query": query,
                "normalized": kms.normalize_keyword(query),
                "filters": _allowed(filters),
            },
        }
        handle = await store.put_handle(
            workspace_id, HandleType.DATASET, payload=spec
        )
        datasets.append({"handle": handle, "summary": _summary(enriched)})

    return {"datasets": datasets, "count": len(datasets)}


def _normalize_query(kms: KMSCatalog, query: str) -> str:
    """KMS-normalize ``query`` to a CMR keyword (first canonical term; passthrough)."""
    terms = kms.normalize_keyword(query)
    return terms[0] if terms else query


def _allowed(filters: dict[str, Any] | None) -> dict[str, Any]:
    """Keep only the filter keys ``search_collections`` accepts."""
    if not filters:
        return {}
    return {k: v for k, v in filters.items() if k in _ALLOWED_FILTERS}


def _summary(collection: dict) -> dict:
    """A compact, agent-facing summary of one enriched collection."""
    return {
        "concept_id": collection.get("concept_id"),
        "short_name": collection.get("short_name"),
        "version": collection.get("version"),
        "entry_title": collection.get("entry_title"),
        "processing_level": collection.get("processing_level"),
        "advisory_notes": collection.get("advisory_notes", []),
    }
