# Build prompts for Claude Code

These are **driver prompts** — each one runs a single Claude Code session against
`PLAN.md` and ends at a green gate + a commit. They deliberately do **not**
restate the plan; they point Claude Code at the right `PLAN.md` section and
enforce the gate and guardrails. `PLAN.md` stays the single source of truth.

**Read `CLAUDE_CODE_SETUP.md` first** (install, CLAUDE.md, permissions, the
working loop).

## Order

| # | Prompt | Phase | Ends with |
|---|---|---|---|
| 01 | `01_phase0_tta_audit.md` | 0 | `docs/tta_audit.md` |
| 02 | `02_phase1_scaffold.md` | 1 | Docker + DB + storage + worker up |
| 03 | `03_phase2_cmr_patterns.md` | 2.1 | `docs/cmr_patterns.md` |
| 04 | `04_phase2_cmr_capabilities.md` | 2.2–2.5 | CMR provider + CollectionCapabilities + KMS + enrichment |
| 05 | `05_phase3_workspace_provenance.md` | 3 | Workspace + handles + provenance |
| 06 | `06_phase4_base_auth.md` | 4.1–4.2 | Provider Protocols + EDL auth |
| 07 | `07_phase4_harmony_router.md` | 4.3–4.5 | Harmony (harmony-py) + router + live test |
| 08 | `08_phase5_discovery_tools.md` | 5 | `search_datasets` + `describe_dataset` |
| 09 | `09_phase6_area_coverage.md` | 6.1–6.2 | Area + coverage tools |
| 10 | `10_phase6_retrieval.md` | 6.3 | Durable async retrieval + resume |
| 11 | `11_phase7_preview_transform.md` | 7.1–7.2 | Preview + transform tools |
| 12 | `12_phase7_opendap_appeears.md` | 7.3–7.4 | OPeNDAP + AppEEARS (Parquet path) |
| 13 | `13_phase8_provenance_hardening.md` | 8 | Provenance tools + citations + hardening |

## How to run one

In the repo, with Claude Code open:

```
/clear
Follow prompts/02_phase1_scaffold.md. Read PLAN.md and CLAUDE.md first.
Do not commit until the gate passes; show me the gate output.
```

Then **run the gate yourself**, confirm green, and let it commit. `/clear` before
the next prompt.

## Rules every prompt assumes (also in CLAUDE.md)

- PLAN.md wins over a prompt if they conflict — flag it, don't silently choose.
- Do not skip, weaken, or fake a gate. Never commit red.
- Stay in the current phase; don't build ahead.
- The hard rules in CLAUDE.md (harmony-py, capability gating, durable jobs,
  spec-based provenance, StorageBackend, CMR canon, no analysis tools) apply
  everywhere.
