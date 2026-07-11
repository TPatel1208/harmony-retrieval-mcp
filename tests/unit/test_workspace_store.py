"""``WorkspaceStore`` lookup/insert primitives that back PRD 020's dedupe.

Direct, low-level tests against the real Postgres-backed ``WorkspaceStore``
fixture (not through a ``retrieve_*`` tool) for the two methods the dedupe
review flagged:

* ``find_handle_by_payload_value`` — now a JSONB predicate pushed into SQL
  (``payload->>key = value``, ``LIMIT 1``) instead of a Python loop over every
  handle of that type; this asserts it still returns the same "most recently
  created match" semantics.
* ``put_handle_unique_signature`` — the atomic insert-or-discover primitive
  backed by the partial unique index ``create_schema`` creates on
  ``(workspace_id, payload->>'signature')`` for ``handle_type = 'obs'``
  handles, which is what actually closes the check-then-act race a
  SELECT-based dedupe check alone cannot.
"""

from __future__ import annotations

from earthdata_mcp.workspace.models import HandleType

_CONCEPT_ID = "C1234567890-LPCLOUD"


# -- find_handle_by_payload_value --------------------------------------------


async def test_find_handle_by_payload_value_returns_most_recent_match(
    workspace_store, workspace_id,
) -> None:
    """Two handles share the same payload value for an arbitrary key; the
    lookup must return the most recently created one, matching the
    pre-SQL-pushdown Python-loop semantics (``order_by(created_at.desc())``,
    first hit).

    Uses ``HandleType.DATASET`` with a ``tag`` key rather than an OBS handle
    with ``signature`` — two OBS handles sharing a signature in the same
    workspace is exactly what the new partial unique index (PRD 020 story 6)
    now forbids, so that combination can no longer occur outside the
    ``put_handle_unique_signature`` race-loser case exercised elsewhere.
    """
    older = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": "C1-OLD", "tag": "dup"}
    )
    newer = await workspace_store.put_handle(
        workspace_id, HandleType.DATASET, {"concept_id": "C1-NEW", "tag": "dup"}
    )
    assert older != newer

    match = await workspace_store.find_handle_by_payload_value(
        workspace_id, HandleType.DATASET, "tag", "dup"
    )

    assert match is not None
    assert match.handle == newer


async def test_find_handle_by_payload_value_no_match_returns_none(
    workspace_store, workspace_id,
) -> None:
    await workspace_store.put_handle(
        workspace_id, HandleType.OBS, {"status": "pending", "signature": "sig-a"}
    )

    match = await workspace_store.find_handle_by_payload_value(
        workspace_id, HandleType.OBS, "signature", "sig-does-not-exist"
    )

    assert match is None


async def test_find_handle_by_payload_value_scoped_to_workspace(
    workspace_store, workspace_id,
) -> None:
    """A signature match in another workspace is invisible — same isolation
    contract as every other read on this store."""
    other_workspace_id = f"{workspace_id}-other"
    await workspace_store.put_handle(
        other_workspace_id, HandleType.OBS, {"status": "pending", "signature": "sig-a"}
    )

    match = await workspace_store.find_handle_by_payload_value(
        workspace_id, HandleType.OBS, "signature", "sig-a"
    )

    assert match is None


# -- put_handle_unique_signature ---------------------------------------------


async def test_put_handle_unique_signature_first_caller_wins(
    workspace_store, workspace_id,
) -> None:
    record, created = await workspace_store.put_handle_unique_signature(
        workspace_id, HandleType.OBS, {"status": "pending", "signature": "sig-a"}
    )

    assert created is True
    assert record.handle.startswith("obs_")
    assert record.payload["signature"] == "sig-a"


async def test_put_handle_unique_signature_second_caller_discovers_the_winner(
    workspace_store, workspace_id,
) -> None:
    """The database-level uniqueness guarantee behind PRD 020 story 6: a
    second insert for the same ``(workspace_id, signature)`` never creates a
    second row — it discovers and returns the first one instead."""
    first, first_created = await workspace_store.put_handle_unique_signature(
        workspace_id, HandleType.OBS, {"status": "pending", "signature": "sig-a"}
    )
    second, second_created = await workspace_store.put_handle_unique_signature(
        workspace_id, HandleType.OBS, {"status": "pending", "signature": "sig-a"}
    )

    assert first_created is True
    assert second_created is False
    assert second.handle == first.handle

    all_obs = await workspace_store.list_handles(workspace_id, HandleType.OBS)
    assert len(all_obs) == 1


async def test_put_handle_unique_signature_different_signatures_both_created(
    workspace_store, workspace_id,
) -> None:
    """The uniqueness guarantee is scoped to a matching signature — two
    genuinely different requests never collide."""
    first, first_created = await workspace_store.put_handle_unique_signature(
        workspace_id, HandleType.OBS, {"status": "pending", "signature": "sig-a"}
    )
    second, second_created = await workspace_store.put_handle_unique_signature(
        workspace_id, HandleType.OBS, {"status": "pending", "signature": "sig-b"}
    )

    assert first_created is True
    assert second_created is True
    assert first.handle != second.handle


async def test_put_handle_unique_signature_scoped_to_workspace(
    workspace_store, workspace_id,
) -> None:
    """The same signature in two different workspaces is not a collision —
    the unique index is scoped on ``(workspace_id, signature)``."""
    other_workspace_id = f"{workspace_id}-other"

    first, first_created = await workspace_store.put_handle_unique_signature(
        workspace_id, HandleType.OBS, {"status": "pending", "signature": "sig-a"}
    )
    second, second_created = await workspace_store.put_handle_unique_signature(
        other_workspace_id, HandleType.OBS, {"status": "pending", "signature": "sig-a"}
    )

    assert first_created is True
    assert second_created is True
    assert first.handle != second.handle
