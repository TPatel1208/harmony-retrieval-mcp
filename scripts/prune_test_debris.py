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

Everything else is left alone and reported instead. Workspaces from manual
live-debugging sessions (``t54_verify_*``, ``debug_repro_*``, ``probe_*``,
``user-test``, …) are a separate, **literally enumerated** list —
:data:`RETIRED_DEBUG_WORKSPACES`, deleted only under
``--include-retired-debug``. They are not matched by pattern because three of
them are spelled ``user-…``, which is also the shape of a real consumer identity.

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

#: Workspaces left behind by manual live-debugging sessions, retired on operator
#: instruction (2026-07-29). Enumerated **literally**, never matched by pattern:
#: three of them are spelled ``user-…``, which is also the shape of a real
#: consumer identity, so a pattern here could delete someone's research. Adding a
#: name to this list is a deliberate, reviewable act.
#:
#: Provenance of each group, for anyone auditing the list later:
#:   * ``t54_*``/``user-t54*`` — PRD 019's live TTA matrix verification runs
#:   * ``debug_repro_*``/``probe_*``/``t35_task_zero`` — one-off crash repros
#:   * ``user-test``/``user-stress-user`` — hand-driven smoke/stress sessions
#:
#: Gated behind ``--include-retired-debug`` because, unlike the uuid-per-test
#: ``ws-`` pattern, these are *stable* names a future debugging session could
#: legitimately reuse — a routine sweep must not silently delete live work under
#: a name someone picked again.
RETIRED_DEBUG_WORKSPACES: tuple[str, ...] = (
    "user-test",
    "user-stress-user",
    "t54_verify_f1734690",
    "t54_verify_46e3082b",
    "user-t54verify02570471",
    "user-t54verify212cc885",
    "t54_probe_2781d2f8",
    "user-t54probe",
    "debug_repro_22294622",
    "debug_repro_3f60f6fe",
    "t35_task_zero",
    "probe_c2c979fc",
)


def _workspace_scoped(model, *, include_retired_debug: bool):
    """Rows this script may delete from a table with a ``workspace_id`` column."""
    predicate = model.workspace_id.like(TEST_WORKSPACE_PATTERN)
    if include_retired_debug:
        predicate = or_(predicate, model.workspace_id.in_(RETIRED_DEBUG_WORKSPACES))
    return predicate


def _suite_jobs(*, include_retired_debug: bool):
    """Job rows this script may delete.

    Either the durable spec names a ``ws-`` workspace, or it names none at all —
    see the module docstring for why the second case is exclusively test-seeded.

    The retired debugging workspaces have to be selected here too when they are
    selected for handles: four of them own real ``expired`` job rows, and
    deleting their handles while leaving those behind would strand jobs pointing
    at handles that no longer exist.
    """
    workspace_id = Job.request_spec["workspace_id"].astext
    predicate = or_(
        workspace_id.like(TEST_WORKSPACE_PATTERN),
        workspace_id.is_(None),
    )
    if include_retired_debug:
        predicate = or_(predicate, workspace_id.in_(RETIRED_DEBUG_WORKSPACES))
    return predicate


def _targets(*, include_retired_debug: bool):
    """The ``(label, model, predicate)`` triples this run may delete, in the
    order they must be deleted in."""
    return (
        (
            "provenance_events",
            ProvenanceEvent,
            _workspace_scoped(ProvenanceEvent, include_retired_debug=include_retired_debug),
        ),
        (
            "provenance_edges",
            ProvenanceEdge,
            _workspace_scoped(ProvenanceEdge, include_retired_debug=include_retired_debug),
        ),
        (
            "handles",
            Handle,
            _workspace_scoped(Handle, include_retired_debug=include_retired_debug),
        ),
        ("jobs", Job, _suite_jobs(include_retired_debug=include_retired_debug)),
    )


async def _report(session, *, include_retired_debug: bool) -> dict[str, int]:
    """Count what would be deleted, per table."""
    counts = {}
    for label, model, predicate in _targets(include_retired_debug=include_retired_debug):
        stmt = select(func.count()).select_from(model).where(predicate)
        counts[label] = (await session.execute(stmt)).scalar_one()
    return counts


async def _job_selector_breakdown(
    session, *, include_retired_debug: bool
) -> list[tuple[str, int]]:
    """Job counts per selector, plus what survives — the check that matters.

    A surviving count of zero is not automatically wrong (this database may
    simply hold no real retrievals), but it is the number to look at before
    passing ``--apply``.
    """
    workspace_id = Job.request_spec["workspace_id"].astext
    selectors = [
        ("spec workspace_id ws-*", workspace_id.like(TEST_WORKSPACE_PATTERN)),
        ("spec has no workspace_id", workspace_id.is_(None)),
    ]
    if include_retired_debug:
        selectors.append(
            ("retired debug workspaces", workspace_id.in_(RETIRED_DEBUG_WORKSPACES))
        )
    selectors.append(
        (
            "SURVIVING (real workspaces)",
            ~_suite_jobs(include_retired_debug=include_retired_debug),
        )
    )
    out = []
    for label, predicate in selectors:
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


async def _survivors(session, *, include_retired_debug: bool) -> list[tuple[str, int]]:
    """Workspaces this run refuses to touch, with their handle counts."""
    stmt = (
        select(Handle.workspace_id, func.count())
        .where(
            ~_workspace_scoped(Handle, include_retired_debug=include_retired_debug)
        )
        .group_by(Handle.workspace_id)
        .order_by(func.count().desc())
    )
    return [(row[0], row[1]) for row in (await session.execute(stmt)).all()]


async def main(apply: bool, include_retired_debug: bool) -> None:
    session_factory = create_session_factory(create_engine())
    async with session_factory() as session:
        counts = await _report(session, include_retired_debug=include_retired_debug)
        survivors = await _survivors(
            session, include_retired_debug=include_retired_debug
        )

        total = sum(counts.values())
        scope = f"workspace {TEST_WORKSPACE_PATTERN!r}"
        if include_retired_debug:
            scope += f" + {len(RETIRED_DEBUG_WORKSPACES)} retired debug workspaces"
        print(f"suite debris matching {scope}:")
        for label, count in counts.items():
            print(f"  {label:20s} {count:>7,}")
        print(f"  {'TOTAL':20s} {total:>7,}")

        # The job selectors, broken out: the ``ws-`` one is self-evidently the
        # fixture's, the "no workspace_id at all" one is what an operator should
        # eyeball, and the surviving count is the proof that real job rows are
        # not in scope.
        jobs_by_selector = await _job_selector_breakdown(
            session, include_retired_debug=include_retired_debug
        )
        print("\n  jobs, by selector:")
        for label, count in jobs_by_selector:
            print(f"    {label:38s} {count:>7,}")

        if include_retired_debug:
            print("\n  retired debug workspaces in scope (literal list, not a pattern):")
            for workspace_id in RETIRED_DEBUG_WORKSPACES:
                stmt = (
                    select(func.count())
                    .select_from(Handle)
                    .where(Handle.workspace_id == workspace_id)
                )
                held = (await session.execute(stmt)).scalar_one()
                print(f"    {workspace_id:38s} {held:>7,} handles")

        # The corroborating evidence for the no-workspace_id selector: a
        # directly-seeded test row names a fixture collection, never a real CMR
        # concept id. If a real short_name ever shows up here, stop and
        # investigate before applying.
        print("\n  collections in the no-workspace_id bucket:")
        for concept_id, count in await _no_workspace_collections(session):
            print(f"    {str(concept_id):38s} {count:>7,}")

        print("\nleft untouched:")
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
        for _label, model, predicate in _targets(
            include_retired_debug=include_retired_debug
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
    parser.add_argument(
        "--include-retired-debug",
        action="store_true",
        help=(
            "also delete the enumerated RETIRED_DEBUG_WORKSPACES — stable names a "
            "future debugging session could reuse, so never part of a routine sweep"
        ),
    )
    args = parser.parse_args()
    asyncio.run(main(args.apply, args.include_retired_debug))
