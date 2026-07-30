"""CRUD over the durable ``jobs`` table (PLAN.md §4.3).

The Postgres ``jobs`` table is the source of truth: every read the tools and the
worker do goes through here, and every state change is funnelled through
``set_state``/``transition_state`` so the state machine in ``state.py`` is the
sole authority on what is legal. The worker is stateless — it owns no job state,
it only mutates these rows.

All functions take an :class:`~sqlalchemy.ext.asyncio.AsyncSession` so callers
control the transaction boundary; the mutating helpers ``commit`` before
returning so a crash mid-task never leaves an uncommitted transition.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from earthdata_mcp.jobs.models import CollectionAccess, Job
from earthdata_mcp.jobs.state import (
    TERMINAL_STATES,
    JobState,
    assert_legal,
)


class JobNotFoundError(KeyError):
    """No job with this id/handle exists.

    Subclasses ``KeyError`` for callers that pattern-match on it, but
    ``KeyError.__str__`` renders as ``repr(args[0])`` regardless of subclass —
    left un-overridden, that bare ``'some_job'`` is what reaches the MCP
    client as the tool error, indistinguishable from an unhandled ``KeyError``.
    """

    def __str__(self) -> str:
        return f"unknown job: {self.args[0]!r}"


async def create_job(
    session: AsyncSession,
    *,
    job_id: str,
    job_handle: str,
    obs_handle: str | None,
    provider: str,
    request_spec: dict,
    credential_encrypted: str | None = None,
    speculative: bool = False,
) -> Job:
    """Insert a fresh job in ``PENDING`` with its durable, re-materializable spec.

    ``credential_encrypted`` (PRD 022) is the per-call EDL credential the job
    runs under, already Fernet-encrypted — never plaintext, never inside
    ``request_spec`` (which flows to provenance edges and status enrichment).

    ``speculative`` (PRD 025) marks a job nobody asked for — issued by
    adjacent-granule prefetch. Defaults to ``False`` so every existing caller
    keeps minting user-requested work; only the prefetch path passes ``True``.
    """
    job = Job(
        job_id=job_id,
        job_handle=job_handle,
        obs_handle=obs_handle,
        provider=provider,
        request_spec=request_spec,
        state=JobState.PENDING.value,
        progress=0,
        credential_encrypted=credential_encrypted,
        speculative=speculative,
    )
    session.add(job)
    await session.commit()
    return job


async def get_job(session: AsyncSession, job_id: str) -> Job | None:
    """Load a job by primary key, or ``None``."""
    return await session.get(Job, job_id)


async def get_job_by_handle(session: AsyncSession, job_handle: str) -> Job | None:
    """Load a job by its public ``job_`` handle, or ``None``.

    This is the read path ``get_retrieval_status`` uses: state comes from Postgres,
    never from any in-memory bookkeeping (§4.3 hard rule).
    """
    stmt = select(Job).where(Job.job_handle == job_handle)
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_job_by_obs_handle(session: AsyncSession, obs_handle: str) -> Job | None:
    """Load the most recently created job that produced ``obs_handle``, or ``None``.

    An ``obs_`` handle can have more than one job row behind it over its
    lifetime — each rematerialize/reuse re-enqueue (PRD 017, PRD 020) mints a
    fresh job row while keeping the same handle identity — so this orders by
    ``created_at`` and takes the latest; that is always the live job. Used by
    the eviction service (PRD 015) to walk a job across the legal
    ``ready -> expired`` edge when its result is evicted; a transform-owned
    ``cube_`` handle has no job row, so callers must treat ``None`` as fine.
    """
    stmt = (
        select(Job)
        .where(Job.obs_handle == obs_handle)
        .order_by(Job.created_at.desc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def previous_materialization(
    session: AsyncSession, obs_handle: str, *, before_job_id: str
) -> dict | None:
    """The most recent materialization record for ``obs_handle`` from an
    *earlier* job than ``before_job_id``, or ``None`` (PRD 023).

    An ``obs_`` handle keeps its identity across every replay while each replay
    mints a fresh job row (see :func:`get_job_by_obs_handle`), so this is what
    a materializing job compares itself against to notice that the same
    persisted spec just delivered different data. ``None`` covers both the
    first materialization of a handle and any earlier one that predates the
    column — an absent record is never grounds to claim drift.
    """
    stmt = (
        select(Job.materialization)
        .where(
            Job.obs_handle == obs_handle,
            Job.job_id != before_job_id,
            Job.materialization.is_not(None),
        )
        .order_by(Job.created_at.desc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalars().first()


async def rebuild_costs(session: AsyncSession, obs_handles: list[str]) -> dict[str, float]:
    """The measured build cost (seconds) of the latest job behind each handle.

    One query for the whole eviction candidate set rather than one per handle —
    a sweep runs after every materialization, so this read must not grow a
    round-trip per cached result (PRD 024 story 7). Handles with no job row (a
    transform-owned ``cube_``) and jobs that reached READY before the column
    existed are simply absent from the mapping; the caller substitutes its
    configured default rather than reading absent as zero.
    """
    if not obs_handles:
        return {}
    stmt = (
        select(Job.obs_handle, Job.rebuild_cost_seconds)
        .where(
            Job.obs_handle.in_(obs_handles),
            Job.rebuild_cost_seconds.is_not(None),
        )
        .order_by(Job.created_at.desc())
    )
    costs: dict[str, float] = {}
    for obs_handle, cost in (await session.execute(stmt)).all():
        # Ordered newest-first, so the first row seen for a handle is its latest
        # job — the cost of the build that actually produced what is cached now.
        costs.setdefault(obs_handle, float(cost))
    return costs


async def user_work_in_flight(session: AsyncSession) -> bool:
    """Is any *user-requested* job non-terminal right now (PRD 025)?

    The gate behind "a researcher's job must never wait behind a guess": a
    pending prefetch is dropped, not deferred, whenever this is true. Asked of
    Postgres rather than of the queue because the ``jobs`` table is the source of
    truth and Arq exposes no priority lanes — dropping is the only honest way to
    keep speculation off a busy worker.

    Speculative jobs are excluded deliberately: another guess already in flight
    is not a researcher waiting.
    """
    terminal = [s.value for s in TERMINAL_STATES]
    stmt = (
        select(Job.job_id)
        .where(Job.state.not_in(terminal), Job.speculative.is_(False))
        .limit(1)
    )
    return (await session.execute(stmt)).scalars().first() is not None


async def speculative_jobs_since(
    session: AsyncSession, workspace_id: str, since: datetime
) -> int:
    """How many speculative jobs this workspace has been issued since ``since``.

    Backs the per-workspace prefetch budget (PRD 025). Prefetch never chains, so
    no single retrieval can cascade — but a long sequential exploration would
    still spend unbounded provider quota one step at a time, and this is that
    backstop. Scoped by the ``workspace_id`` carried in the durable spec (the
    ``jobs`` table has no workspace column of its own), so one workspace's
    speculation never spends another's budget.
    """
    stmt = select(func.count()).select_from(Job).where(
        Job.speculative.is_(True),
        Job.request_spec["workspace_id"].astext == workspace_id,
        Job.created_at >= since,
    )
    return int((await session.execute(stmt)).scalar_one())


async def reclaim_non_terminal(session: AsyncSession) -> list[Job]:
    """Return every job not in a terminal state — the restart-resume work list.

    The worker's startup hook reclaims these and re-enqueues each from its current
    state, so no in-flight job is lost to a process restart (§4.3).
    """
    terminal = [s.value for s in TERMINAL_STATES]
    stmt = select(Job).where(Job.state.not_in(terminal)).order_by(Job.created_at)
    return list((await session.execute(stmt)).scalars().all())


def set_state(job: Job, new_state: JobState, **extra: object) -> None:
    """Move a *loaded* job to ``new_state`` (guarded), setting any extra columns.

    Synchronous and commit-free so a single task can chain legal hops in one
    transaction (e.g. ``SUBMITTED -> RUNNING -> MATERIALIZING``) before committing.
    ``extra`` sets named columns (``provider_job_url``, ``progress``, ``error``,
    ``output_expires_at``) atomically with the state change.
    """
    assert_legal(JobState(job.state), new_state)
    job.state = JobState(new_state).value
    for key, value in extra.items():
        setattr(job, key, value)
    # PRD 024: what this job cost to build, measured at the one moment it is
    # knowable — every path to READY funnels through here, so the eviction
    # score never has to reconstruct it later from a row that has since been
    # updated. An explicit `extra` wins (a replay may carry the original cost).
    if JobState(new_state) is JobState.READY and "rebuild_cost_seconds" not in extra:
        job.rebuild_cost_seconds = _elapsed_since(job.created_at)
    # PRD 022: the persisted per-call credential exists for the job's
    # operational life only — every transition funnels through here, so any
    # terminal state (success, failure, cancel, expiry) erases it at rest.
    if JobState(new_state) in TERMINAL_STATES:
        job.credential_encrypted = None


def _elapsed_since(created_at: datetime | None) -> float | None:
    """Wall-clock seconds from ``created_at`` to now, or ``None`` if unknowable.

    ``created_at`` is a server default, so it is only ``None`` for a job that has
    never been flushed — in which case there is no elapsed time to report and an
    absent cost (which the eviction score reads as "use the default") is the
    honest answer, not a fabricated zero.
    """
    if created_at is None:
        return None
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return max((datetime.now(timezone.utc) - created_at).total_seconds(), 0.0)


async def transition_state(
    session: AsyncSession,
    job_id: str,
    new_state: JobState,
    **extra: object,
) -> Job:
    """Load ``job_id``, apply one guarded transition, and commit.

    Raises :class:`JobNotFoundError` if the job is gone and
    :class:`~earthdata_mcp.jobs.state.IllegalTransition` if the edge is illegal.
    """
    job = await session.get(Job, job_id)
    if job is None:
        raise JobNotFoundError(job_id)
    set_state(job, new_state, **extra)
    await session.commit()
    return job


async def mark_collection_restricted(session: AsyncSession, concept_id: str) -> None:
    """Durably record that ``concept_id`` is access-restricted (PRD 022).

    Written when a EULA-classified failure is observed for the collection, so
    future dedupe/reuse scoping treats it as restricted even when CMR's own
    metadata says nothing. Upsert + commit; idempotent.
    """
    existing = await session.get(CollectionAccess, concept_id)
    if existing is None:
        session.add(CollectionAccess(concept_id=concept_id, restricted=True))
    else:
        existing.restricted = True
    await session.commit()


async def collection_restriction(
    session: AsyncSession, concept_id: str
) -> bool | None:
    """The durably-observed restriction flag for ``concept_id``, or ``None``."""
    row = await session.get(CollectionAccess, concept_id)
    return None if row is None else bool(row.restricted)


def parse_expiration(value: str | datetime | None) -> datetime | None:
    """Coerce a Harmony ``data_expiration`` (ISO string or datetime) to a datetime."""
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
