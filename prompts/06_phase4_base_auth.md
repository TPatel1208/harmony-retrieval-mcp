# Session 6 — Phase 4.1–4.2: Provider Protocols + EDL auth

**Read first:** `PLAN.md` Phase 4 (tasks 4.1–4.2), §4.1 (provider split);
`CLAUDE.md`.

## Goal
The two-Protocol provider abstraction and working Earthdata Login — so the next
session can submit a real Harmony job.

## Tasks
1. **`providers/base.py`** — define `MetadataProvider` and `RetrievalProvider`
   (§4.1) plus shared types (`RetrievalPlan`, `JobRef`, `JobStatus`,
   `TransformSpec`, `AOI`, `TimeRange`, `ProviderCapabilities`,
   `MaterializedResult`). `CMRProvider` already satisfies `MetadataProvider` from
   Phase 2 — confirm it conforms; **no throwing stubs**.
2. **`providers/auth.py`** — EDL via `earthaccess`: token/session lifecycle and
   in-region S3 credentials. Auth identity maps to **workspace ownership**
   (Phase 3). Read credentials from the environment; never from committed files.

## Constraints
- A provider must not implement a method it can't honor. CMR = metadata only.
- No secrets in the repo. `.env`/credential files stay git-ignored.

## Gate
```bash
docker compose exec mcp pytest tests/unit/test_base_protocols.py \
  tests/unit/test_auth.py -v
# Optional live auth check (needs EDL creds):
EARTHDATA_TOKEN=... docker compose exec mcp pytest -m live tests/live/test_edl_auth.py -v
```
Unit tests mock the EDL session; the `@live` check confirms a real token
authenticates and yields S3 creds.

## Commit
`feat: provider Protocols + EDL auth (Phase 4.1-4.2)`
