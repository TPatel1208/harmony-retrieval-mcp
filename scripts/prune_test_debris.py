"""Delete the unit suite's leftover rows from the shared development database.

The unit suite runs against the same Postgres the worker uses (``DATABASE_URL``)
with no per-test rollback, so every run leaves handles, provenance rows and job
rows behind. ``tests/conftest.py``'s ``session_factory`` fixture *retires* them
(non-terminal jobs → ``cancelled``, ``ready`` handles → ``expired``) so they stop
influencing later tests and the worker's startup reclaim, but retired rows still
accumulate. This script is the periodic sweep that removes them.

**The discriminator is the only thing standing between this script and real
data, and it is deliberately narrow.** Only workspaces matching ``ws-%`` are
touched — that is the pattern of ``tests/conftest.py``'s ``workspace_id``
fixture, which mints ``ws-<uuid4 hex>`` per test. Plus job rows whose durable
spec carries no ``workspace_id`` at all: every spec ``RequestSpec.from_plan``
builds has one, so a row without one can only have been seeded directly by a
test.

Everything else is left alone and reported instead, including workspaces that
*look* like debris (``t54_verify_*``, ``debug_repro_*``, ``probe_*``,
``user-test``, …). Those came from manual live-debugging sessions rather than the
suite fixture, and deciding whether they are still wanted is an operator call,
not this script's.

Dry run by default — it prints what it would delete and exits. Pass ``--apply``
to actually delete.

    docker compose exec mcp python scripts/prune_test_debris.py
    docker compose exec mcp python scripts/prune_test_debris.py --apply

``scripts/`` is bind-mounted into the ``mcp`` container, so a container created
before that mount existed needs one ``docker compose up -d mcp``. To run without
recreating anything, pipe the script in instead:

    docker compose exec -T mcp python - --apply < scripts/prune_test_debris.py
"""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy import delete, func, or_, select

from earthdata_mcp.db import create_engine, create_session_factory
from earthdata_mcp.jobs.models import Job
from earthdata_mcp.workspace.models import Handle, ProvenanceEdge, ProvenanceEvent

#: Mirrors ``tests/conftest.py``'s ``_TEST_WORKSPACE_PATTERN``. Never widen this:
#: real workspaces are ``default`` and ``user-{id}``, and this database holds
#: thousands of their rows.
TEST_WORKSPACE_PATTERN = "ws-%"


def _workspace_scoped(model):
    """Rows this script may delete from a table with a ``workspace_id`` column."""
    return model.workspace_id.like(TEST_WORKSPACE_PATTERN)


def _suite_jobs():
    """Job rows this script may delete.

    Either the durable spec names a ``ws-`` workspace, or it names none at all —
    see the module docstring for why the second case is exclusively test-seeded.
    """
    return or_(
        Job.request_spec["workspace_id"].astext.like(TEST_WORKSPACE_PATTERN),
        Job.request_spec["workspace_id"].astext.is_(None),
    )


async def _report(session) -> dict[str, int]:
    """Count what would be deleted, per table."""
    counts = {}
    for label, model, predicate in (
        ("provenance_events", ProvenanceEvent, _workspace_scoped(ProvenanceEvent)),
        ("provenance_edges", ProvenanceEdge, _workspace_scoped(ProvenanceEdge)),
        ("handles", Handle, _workspace_scoped(Handle)),
        ("jobs", Job, _suite_jobs()),
    ):
        stmt = select(func.count()).select_from(model).where(predicate)
        counts[label] = (await session.execute(stmt)).scalar_one()
    return counts


async def _job_selector_breakdown(session) -> list[tuple[str, int]]:
    """Job counts per selector, plus what survives — the check that matters.

    A surviving count of zero is not automatically wrong (this database may
    simply hold no real retrievals), but it is the number to look at before
    passing ``--apply``.
    """
    ws_only = Job.request_spec["workspace_id"].astext.like(TEST_WORKSPACE_PATTERN)
    no_workspace = Job.request_spec["workspace_id"].astext.is_(None)
    out = []
    for label, predicate in (
        ("spec workspace_id ws-*", ws_only),
        ("spec has no workspace_id", no_workspace),
        ("SURVIVING (real workspaces)", ~_suite_jobs()),
    ):
        stmt = select(func.count()).select_from(Job).where(predicate)
        out.append((label, (await session.execute(stmt)).scalar_one()))
    return out


async def _no_workspace_collections(session) -> list[tuple[str, int]]:
    """Which collections the no-``workspace_id`` job rows name."""
    concept_id = Job.request_spec["concept_id"].astext
    stmt = (
        select(concept_id, func.count())
        .where(Job.request_spec["workspace_id"].astext.is_(None))
        .group_by(concept_id)
        .order_by(func.count().desc())
    )
    return [(row[0], row[1]) for row in (await session.execute(stmt)).all()]


async def _survivors(session) -> list[tuple[str, int]]:
    """Workspaces this script refuses to touch, with their handle counts."""
    stmt = (
        select(Handle.workspace_id, func.count())
        .where(~Handle.workspace_id.like(TEST_WORKSPACE_PATTERN))
        .group_by(Handle.workspace_id)
        .order_by(func.count().desc())
    )
    return [(row[0], row[1]) for row in (await session.execute(stmt)).all()]


async def main(apply: bool) -> None:
    session_factory = create_session_factory(create_engine())
    async with session_factory() as session:
        counts = await _report(session)
        survivors = await _survivors(session)

        total = sum(counts.values())
        print(f"suite debris matching workspace {TEST_WORKSPACE_PATTERN!r}:")
        for label, count in counts.items():
            print(f"  {label:20s} {count:>7,}")
        print(f"  {'TOTAL':20s} {total:>7,}")

        # The two job selectors, broken out: the ``ws-`` half is self-evidently
        # the fixture's, the "no workspace_id at all" half is the one an operator
        # should be able to eyeball, and the surviving count is the proof that
        # real job rows are not in scope.
        jobs_by_selector = await _job_selector_breakdown(session)
        print("\n  jobs, by selector:")
        for label, count in jobs_by_selector:
            print(f"    {label:38s} {count:>7,}")

        # The corroborating evidence for the no-workspace_id selector: a
        # directly-seeded test row names a fixture collection, never a real CMR
        # concept id. If a real short_name ever shows up here, stop and
        # investigate before applying.
        print("\n  collections in the no-workspace_id bucket:")
        for concept_id, count in await _no_workspace_collections(session):
            print(f"    {str(concept_id):38s} {count:>7,}")

        print("\nleft untouched (not the suite fixture's pattern):")
        for workspace_id, count in survivors:
            print(f"  {str(workspace_id):42s} {count:>7,} handles")

        if not apply:
            print("\ndry run — nothing deleted. Re-run with --apply to delete.")
            return
        if total == 0:
            print("\nnothing to delete.")
            return

        # Provenance first, then handles, then jobs: nothing here is enforced by
        # a foreign key (these are plain string columns), but deleting the
        # referring rows before the referenced ones means an interrupted run
        # leaves no lineage pointing at a handle that is already gone.
        for model, predicate in (
            (ProvenanceEvent, _workspace_scoped(ProvenanceEvent)),
            (ProvenanceEdge, _workspace_scoped(ProvenanceEdge)),
            (Handle, _workspace_scoped(Handle)),
            (Job, _suite_jobs()),
        ):
            await session.execute(delete(model).where(predicate))
        await session.commit()
        print(f"\ndeleted {total:,} rows.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually delete (default is a dry run that only reports)",
    )
    asyncio.run(main(parser.parse_args().apply))
