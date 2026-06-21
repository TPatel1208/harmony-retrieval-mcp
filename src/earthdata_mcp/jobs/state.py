"""Job state machine.

The set of legal states and transitions lives here, in one place (PLAN.md §4.3).
Phase 1 declares the states and a skeleton transition map; the transition guard
is enforced when the durable job model is built in Phase 6.
"""

from __future__ import annotations

from enum import StrEnum


class JobState(StrEnum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    RUNNING = "running"
    MATERIALIZING = "materializing"
    READY = "ready"
    FAILED = "failed"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


TERMINAL_STATES: frozenset[JobState] = frozenset(
    {JobState.READY, JobState.FAILED, JobState.EXPIRED, JobState.CANCELLED}
)

# Legal transitions. Filled out / enforced in Phase 6.
LEGAL_TRANSITIONS: dict[JobState, frozenset[JobState]] = {
    JobState.PENDING: frozenset({JobState.SUBMITTED, JobState.FAILED, JobState.CANCELLED}),
    JobState.SUBMITTED: frozenset({JobState.RUNNING, JobState.FAILED, JobState.CANCELLED}),
    JobState.RUNNING: frozenset(
        {JobState.MATERIALIZING, JobState.FAILED, JobState.CANCELLED}
    ),
    JobState.MATERIALIZING: frozenset({JobState.READY, JobState.FAILED}),
    JobState.READY: frozenset({JobState.EXPIRED}),
    JobState.FAILED: frozenset(),
    JobState.EXPIRED: frozenset(),
    JobState.CANCELLED: frozenset(),
}
