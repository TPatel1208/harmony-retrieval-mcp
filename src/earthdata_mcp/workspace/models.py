"""Handle types and workspace/provenance ORM models (PLAN.md §4.5–4.6).

Handles are **opaque, prefixed, stable** identifiers the agent reasons in
(datasets, areas, jobs, results). The prefix names the kind; the suffix is
random and carries no meaning — callers must never parse anything but the
prefix.

All workspace and provenance tables share one declarative ``Base`` so the schema
is created from a single ``metadata``. The durable ``jobs`` table is owned by
``jobs/models.py`` (Phase 6) on its own base.
"""

from __future__ import annotations

import secrets
from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class HandleType(StrEnum):
    """The kinds of handle the server mints. The value is the prefix."""

    DATASET = "dataset"
    AOI = "aoi"
    QUERY = "query"
    OBS = "obs"
    CUBE = "cube"
    PREVIEW = "preview"
    JOB = "job"


def mint_handle(handle_type: HandleType) -> str:
    """Mint a fresh opaque handle, e.g. ``job_3f9c1a...``.

    The suffix is 16 hex chars (64 bits) of randomness — collision-free in
    practice and meaningless on purpose.
    """
    return f"{HandleType(handle_type).value}_{secrets.token_hex(8)}"


def handle_type_of(handle: str) -> HandleType:
    """Return the :class:`HandleType` encoded in ``handle``'s prefix.

    Raises ``ValueError`` for a malformed or unknown prefix.
    """
    prefix, sep, _ = handle.partition("_")
    if not sep:
        raise ValueError(f"not a handle (no prefix): {handle!r}")
    return HandleType(prefix)


class ProvenanceEventType(StrEnum):
    """First-class provenance events (PLAN.md §4.5)."""

    CREATED = "created"
    ROUTED = "routed"
    SUBMITTED = "submitted"
    PROVIDER_FALLBACK = "provider-fallback"
    OPENDAP_NOT_APPLICABLE = "opendap-not-applicable"
    JOB_FAILED = "job-failed"
    MATERIALIZED = "materialized"
    EXPIRED = "expired"
    RE_MATERIALIZED = "re-materialized"


class Base(DeclarativeBase):
    """Declarative base for workspace + provenance tables."""


class Handle(Base):
    """A handle owned by exactly one workspace.

    ``payload`` is the durable content the handle resolves to (a search spec, an
    AOI geometry, a retrieval spec, …) — never an ephemeral staged-output URL.
    """

    __tablename__ = "handles"

    handle: Mapped[str] = mapped_column(String, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(String, index=True)
    handle_type: Mapped[str] = mapped_column(String, index=True)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ProvenanceEdge(Base):
    """A lineage edge: ``target_handle`` was derived from ``source_handle``.

    The edge is keyed to the **durable, re-materializable request spec** and the
    **granule IDs** it consumed — never a staged-output URL, which expires. This
    is what lets an expired or evicted result be rebuilt (PLAN.md §4.5).
    """

    __tablename__ = "provenance_edges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[str] = mapped_column(String, index=True)
    target_handle: Mapped[str] = mapped_column(String, index=True)
    source_handle: Mapped[str] = mapped_column(String, index=True)
    # The re-materializable spec for `target_handle` (durable, not a URL).
    request_spec: Mapped[dict | None] = mapped_column(JSONB, default=None)
    # The granule IDs consumed to produce `target_handle`.
    granule_ids: Mapped[list | None] = mapped_column(JSONB, default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ProvenanceEvent(Base):
    """A first-class lineage event (``created``/``submitted``/``provider-fallback``/
    ``opendap-not-applicable``/``job-failed``/``materialized``/``expired``/
    ``re-materialized``) attached to a handle."""

    __tablename__ = "provenance_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[str] = mapped_column(String, index=True)
    handle: Mapped[str] = mapped_column(String, index=True)
    event_type: Mapped[str] = mapped_column(String, index=True)
    detail: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
