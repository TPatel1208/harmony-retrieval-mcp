"""OPeNDAPProvider — Hyrax/DAP4 subset for gridded collections (PLAN.md §4.2 step 3).

This package is split along its three responsibilities:

* :mod:`._serialization` — pure DAP4 serialization (fully-qualified names,
  hyperslab index math, the constraint-expression builder). No I/O.
* :mod:`._planning` — CMR-facing planning (:func:`plan_subset` and its
  discovery/resolution helpers).
* :mod:`._runtime` — Hyrax-facing runtime (:class:`OPeNDAPProvider`'s
  submit/poll/materialize).

The package's public face — :class:`OPeNDAPProvider`, :func:`plan_subset`,
:class:`OpendapPlan`, :class:`AxisGeometry`, :data:`VarDimPlan`, and the
promoted :func:`build_constraint_expression` — is unchanged by this split;
every import of ``earthdata_mcp.providers.opendap`` from outside this package
keeps working exactly as before.

Every reverse-engineered Hyrax/DAP4 quirk this package handles (grouped FQN
leading slash, coordinate-aware hyperslabs, grid-edge bbox translation, CF-time
projection) is pinned by a named row in the collection-archetype corpus in
``tests/unit/test_providers/test_opendap.py``. See
``docs/opendap_quirk_ledger.md`` for the ledger cross-referencing each quirk to
its corpus row — a new quirk gets a ledger entry plus a corpus row, not an
ad-hoc test.
"""

from __future__ import annotations

from earthdata_mcp.providers.opendap._planning import (
    OpendapPlan,
    _discover_grid_geometry,
    _discover_opendap_urls,
    _opendap_url_of,
    _resolve_from_cmr,
    plan_subset,
)
from earthdata_mcp.providers.opendap._runtime import PROVIDER, OPeNDAPProvider
from earthdata_mcp.providers.opendap._serialization import (
    AxisGeometry,
    VarDimPlan,
    build_constraint_expression,
)

__all__ = [
    "PROVIDER",
    "AxisGeometry",
    "OPeNDAPProvider",
    "OpendapPlan",
    "VarDimPlan",
    "build_constraint_expression",
    "plan_subset",
]
