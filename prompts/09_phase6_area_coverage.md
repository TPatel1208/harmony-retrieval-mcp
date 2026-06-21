# Session 9 — Phase 6.1–6.2: Area & coverage

**Read first:** `PLAN.md` Phase 6 (tasks 6.1–6.2); `CLAUDE.md`.

## Goal
Define AOIs and answer coverage/availability questions — all metadata-only and
fast (no retrieval yet).

## Tasks
1. **`tools/area.py`** — `define_area_of_interest`: accept place name (via
   Nominatim), bbox, GeoJSON, HUC watershed, or FIPS admin → mint an `aoi_`
   handle.
2. **`tools/coverage.py`** — `check_coverage`, `check_availability`,
   `inspect_granules`, and `estimate_retrieval_size` (from CMR granule sizes).
   All delegate to `cmr.search_granules`. Metadata-only.

## Constraints
- No Harmony, no downloads this session. Coverage is pure CMR metadata.
- `estimate_retrieval_size` exists to gate huge requests later — make it real.

## Gate
```bash
docker compose exec mcp pytest tests/unit/test_tools/test_area.py \
  tests/unit/test_tools/test_coverage.py -v
# Real availability through the stack (no auth):
docker compose exec mcp python -c "
import asyncio
from earthdata_mcp.tools.area import define_area_of_interest
from earthdata_mcp.tools.coverage import check_availability
from earthdata_mcp.tools.discovery import search_datasets
async def go():
    aoi=await define_area_of_interest('-105,37,-104,38')
    ds=(await search_datasets('vegetation'))['datasets'][0]['handle']
    print(await check_availability(ds, aoi['handle'], '2024-01-01/2024-03-31'))
asyncio.run(go())"
```

## Commit
`feat: area + coverage tools (Phase 6.1-6.2)`
