"""Every module under ``tests/live`` must at least import.

The live suite is opt-in (``-m live``, real Earthdata credentials), and the
benchmark scripts in there are not even pytest modules — so nothing in the
default gate ever loads any of it. That is how ``bench_cloud_opendap_vs_harmony``
came to sit broken at *module scope*: ``_opendap_url_of`` had moved out of
``tools/retrieval`` into ``providers/opendap/_planning``, and the stale import
raised ``ImportError`` before the benchmark could take a single measurement.
Nobody found out until someone opened the file.

This is the cheapest guard that would have caught it: import each module and
nothing else. It needs no credentials and no network, so it belongs in the
default run — which is why it lives here, in ``tests/unit``, rather than beside
the modules it checks. Under ``tests/live`` it would carry the ``live`` marker's
fate and never run.

What it does **not** do is run anything. A live test whose body drifted is still
only caught by running it against the real services; this catches the strictly
cheaper class of rot — a moved symbol, a renamed module, a signature that changed
arity where the call sits at import time.

A module that needs credentials or network merely to be *imported* will fail
here, and that is the right outcome: import-time side effects are what make a
suite impossible to check cheaply. Move the side effect into a fixture or the
test body rather than exempting the module.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

_LIVE_DIR = Path(__file__).resolve().parents[1] / "live"


def _live_modules() -> list[str]:
    """Every importable module name in ``tests/live``, ``__init__`` aside."""
    return sorted(path.stem for path in _LIVE_DIR.glob("*.py") if path.stem != "__init__")


def test_the_live_suite_is_discoverable() -> None:
    """A guard on the guard.

    ``parametrize`` over an empty list is a *passing* test run with zero cases —
    so a moved directory or a renamed package would silently turn the check
    below into nothing at all while still reporting green.
    """
    assert _live_modules(), f"no live modules found under {_LIVE_DIR}"


@pytest.mark.parametrize("module_name", _live_modules())
def test_live_module_imports(module_name: str) -> None:
    importlib.import_module(f"tests.live.{module_name}")
