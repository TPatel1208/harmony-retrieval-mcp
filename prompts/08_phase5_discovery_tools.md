# Session 8 — Phase 5: Discovery & understanding tools (trimmed)

**Read first:** `PLAN.md` Phase 5 and §2 (discovery trim); `CLAUDE.md`.

## Goal
The **two** handle-minting MCP tools — nothing more. A capable agent composes the
rest from these plus NASA's server.

## Tasks
1. **`tools/discovery.py`** — `search_datasets(query, filters, workspace_id)`:
   KMS-normalize → `cmr.search_collections` → enrich → mint `dataset_` handles,
   saved to the workspace. Return `{datasets: [{handle, summary}], count}`.
2. **`tools/understanding.py`** — `describe_dataset(dataset_)`: resolve handle →
   collection metadata + `get_variables` + **advisory** enrichment notes.
3. Register exactly these two tools in `server.py`.

## Constraints
- **Do not** build `discover_datasets`, `recommend_datasets`, `list_variables`,
  `explain_variable`, or `compare_datasets` — they are deferred post-v1.
- No analysis tools, ever.
- Enrichment notes must be flagged advisory in the tool output.

## Gate
```bash
docker compose exec mcp pytest tests/unit/test_tools/test_discovery.py \
  tests/unit/test_tools/test_understanding.py -v
```
Mocked-CMR tests assert `dataset_` handle prefixes, advisory-note flagging, and
workspace persistence.

## Commit
`feat: search_datasets + describe_dataset (Phase 5)`
