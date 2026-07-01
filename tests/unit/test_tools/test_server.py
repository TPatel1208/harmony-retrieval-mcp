"""``server.py`` MCP tool wrappers — thin pass-throughs to ``earthdata_mcp.tools.*``.

Each ``@mcp.tool`` function must forward its arguments to the module-level function
it delegates to without letting positional order drift. A wrapper that calls its
target positionally still "works" at import/type-check time even after the target
gains a new parameter — it just silently shifts every later argument into the wrong
slot. That is exactly what happened to ``align``: ``transform.align`` gained a
``snap_time_freq`` parameter ahead of ``workspace_id``, but this wrapper still called
``transform.align(source_handles, method, workspace_id)`` positionally, so the
workspace id landed in ``snap_time_freq`` — which then got divided into a real
dataset's time values inside ``_snap_time``, raising a ``TypeError`` for every align
call regardless of join method or handle order.
"""

from __future__ import annotations

from earthdata_mcp import server


async def test_align_forwards_workspace_id_by_keyword(monkeypatch) -> None:
    captured: dict = {}

    async def fake_align(
        source_handles, method="outer", snap_time_freq=None, workspace_id="default", **kwargs
    ):
        captured.update(
            source_handles=source_handles,
            method=method,
            snap_time_freq=snap_time_freq,
            workspace_id=workspace_id,
        )
        return {"handle": "cube_fake"}

    monkeypatch.setattr(server.transform, "align", fake_align)

    result = await server.align(["obs_a", "obs_b"], method="outer", workspace_id="ws-123")

    assert result == {"handle": "cube_fake"}
    # The bug: workspace_id ("ws-123") ends up here instead.
    assert captured["snap_time_freq"] is None
    # The bug's mirror image: workspace_id falls back to the default instead.
    assert captured["workspace_id"] == "ws-123"
