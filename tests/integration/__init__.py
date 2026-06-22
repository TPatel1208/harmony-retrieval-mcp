"""End-to-end integration tests (PLAN.md §6 Phase 8 gate).

These exercise the full durable pipeline — plan → persist → worker submit/poll/
materialize → ready → provenance — against the real Postgres job table and real
storage, with only the provider's network faked. The live Harmony flow lives in
``tests/live/test_full_retrieval.py`` (credentialed, opt-in).
"""
