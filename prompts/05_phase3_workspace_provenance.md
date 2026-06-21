# Session 5 — Phase 3: Workspace, handles, provenance

**Read first:** `PLAN.md` Phase 3, §4.5 (provenance), §4.6 (workspace scoping);
`CLAUDE.md`.

## Goal
Our core that NASA's server has no equivalent for: typed handles, workspace
persistence with ownership/isolation, and a spec-keyed provenance DAG.

## Tasks
1. **`workspace/models.py`** — handle types: `dataset_`, `aoi_`, `query_`,
   `obs_`, `cube_`, `preview_`, and **`job_`**. Opaque, prefixed, stable.
2. **`workspace/store.py`** — Postgres persistence. Every handle belongs to a
   `workspace_id`; enforce **ownership/isolation** — cross-workspace reads denied.
3. **`workspace/provenance.py`** — lineage edges keyed to **durable request specs
   and granule IDs**, never to ephemeral URLs. Ancestry via a **recursive CTE**.
   First-class `expired` and `re-materialized` events.

## Constraints
- Provenance must never store a staged-output URL as the source of truth.
- Recursive ancestry must be written deliberately and tested on a deep graph.

## Gate
```bash
docker compose exec mcp pytest tests/unit/test_workspace.py \
  tests/unit/test_provenance.py -v
```
Tests must cover: handle round-trip; **cross-workspace denial**; **deep-graph
ancestry (≥20 hops)**; and a spec-based re-materialization stub.

## Commit
`feat: workspace, handles, spec-keyed provenance (Phase 3)`
