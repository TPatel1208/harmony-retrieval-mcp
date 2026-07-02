"""``JobNotFoundError``'s string form — a shared-root-cause sibling of
``HandleNotFoundError`` (see ``tests/unit/test_workspace.py``): both subclass
``KeyError``, whose ``__str__`` renders as bare ``repr(args[0])`` unless
overridden, leaking as an unlabeled string through the MCP tool-error path.
"""

from __future__ import annotations

from earthdata_mcp.jobs.crud import JobNotFoundError


def test_job_not_found_error_str_is_not_a_bare_key_repr() -> None:
    err = JobNotFoundError("job_nonexistent123")
    assert str(err) != "'job_nonexistent123'"
    assert "job_nonexistent123" in str(err)
