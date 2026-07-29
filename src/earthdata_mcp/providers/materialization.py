"""``MaterializationRecord`` — what a job *actually* delivered (PRD 023).

:class:`~earthdata_mcp.providers.request_spec.RequestSpec` is complete about
**intent** and says nothing about **outcome**, and in this repo the two come
apart by design: Harmony resolves granules server-side at submit time (an NRT
late-arrival changes a replay's result), Harmony is frequently submitted
unpinned so the server picks the chain, and OPeNDAP is a *runtime* fallback
rather than a planning-time choice. An unchanged spec therefore does not
promise unchanged bytes. This record is the other half: written once, by the
worker, at the transition into ``READY``, describing what the job produced.

``content_digest`` is deliberately **not**
:func:`~earthdata_mcp.providers.request_spec.compute_request_signature`. That
signature hashes the user's ask so a repeat ask can reuse a handle (PRD 020),
and its docstring is explicit that routing must never decide whether two
requests are "the same ask". This digest asks the opposite question — are two
materializations the same bytes? — so routing is exactly what it must capture.
Two jobs can share a signature and differ in digest; that is the point.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from earthdata_mcp.providers.base import MaterializedResult
    from earthdata_mcp.providers.request_spec import RequestSpec

__all__ = ["MaterializationRecord"]

#: The key a provider uses to report the granule identifiers it actually
#: retrieved, on :attr:`MaterializedResult.extra`. Absent means the provider
#: does not name its inputs (AppEEARS) — never that it retrieved none.
GRANULE_IDS_KEY = "granule_ids"

#: The key a provider uses to report the service chain that actually processed
#: the job, on :attr:`MaterializedResult.extra`.
SERVICE_CHAIN_KEY = "service_chain"


@dataclass(frozen=True)
class MaterializationRecord:
    """The durable description of one job's delivered content.

    ``granule_ids`` is ``None`` — *absent*, never empty — when the provider
    reports no granule identifiers (an AppEEARS point sample). A consumer must
    be able to tell "no granule list available" from "an empty granule list",
    and to decline to trust a digest computed over an incomplete description;
    :attr:`partial` says which it is holding.
    """

    granule_ids: tuple[str, ...] | None
    provider_used: str
    service_chain: tuple[str, ...] | None
    output_format: str

    # -- construction -------------------------------------------------------

    @classmethod
    def from_result(
        cls, result: MaterializedResult, spec: RequestSpec
    ) -> MaterializationRecord:
        """Describe what ``result`` delivered for ``spec``.

        ``provider_used`` comes from ``spec.provider`` **as the worker read it
        at materialize time**, not from the routing decision that planned the
        job: the runtime OPeNDAP fallback rewrites that field in Postgres
        before re-submitting, so a Harmony-planned job that fell back records
        ``opendap`` — which is the whole point of recording it.

        Granule identifiers and the realized service chain are whatever the
        provider chose to report on :attr:`MaterializedResult.extra`; a
        provider that reports neither yields a :attr:`partial` record rather
        than an empty one.
        """
        extra = result.extra or {}
        granule_ids = extra.get(GRANULE_IDS_KEY)
        service_chain = extra.get(SERVICE_CHAIN_KEY)
        return cls(
            granule_ids=tuple(granule_ids) if granule_ids is not None else None,
            provider_used=spec.provider,
            service_chain=tuple(service_chain) if service_chain is not None else None,
            output_format=spec.output_format,
        )

    # -- durable (de)serialization -------------------------------------------

    def to_jsonb(self) -> dict:
        """The durable dict stored in ``jobs.materialization``.

        ``content_digest``/``partial`` are stored alongside the inputs rather
        than recomputed on read: a consumer keying a cache on the digest needs
        the value this materialization actually published, even if this
        module's canonicalization is ever revised. An absent granule list is
        omitted, never written as ``[]`` (see the class docstring).
        """
        data: dict[str, object] = {
            "provider_used": self.provider_used,
            "output_format": self.output_format,
            "content_digest": self.content_digest,
            "partial": self.partial,
        }
        if self.granule_ids is not None:
            data[GRANULE_IDS_KEY] = list(self.granule_ids)
        if self.service_chain is not None:
            data[SERVICE_CHAIN_KEY] = list(self.service_chain)
        return data

    @classmethod
    def from_jsonb(cls, data: dict | None) -> MaterializationRecord | None:
        """Reconstruct a stored record, or ``None`` for a job that has none.

        Every handle materialized before PRD 023 has no record, and this
        answers ``None`` for them rather than fabricating or backfilling one —
        the same legacy-tolerance posture as
        :meth:`~earthdata_mcp.providers.request_spec.RequestSpec.from_jsonb`.
        """
        if not data:
            return None
        granule_ids = data.get(GRANULE_IDS_KEY)
        service_chain = data.get(SERVICE_CHAIN_KEY)
        return cls(
            granule_ids=tuple(granule_ids) if granule_ids is not None else None,
            provider_used=data.get("provider_used") or "",
            service_chain=tuple(service_chain) if service_chain is not None else None,
            output_format=data.get("output_format") or "",
        )

    # -- identity ------------------------------------------------------------

    @property
    def partial(self) -> bool:
        """True when the digest was computed over an incomplete description."""
        return self.granule_ids is None

    @property
    def content_digest(self) -> str:
        """A stable hash over the delivered-content description.

        Granule ids are sorted and deduplicated — the same delivered set is the
        same content however the provider happened to order or repeat it —
        mirroring ``compute_request_signature``'s canonicalization discipline.
        An unknown component is omitted from the hashed object rather than
        hashed as ``null``, so a partial record's digest is a hash of what is
        actually known.
        """
        payload: dict[str, object] = {
            "provider_used": self.provider_used,
            "output_format": self.output_format,
        }
        if self.granule_ids is not None:
            payload["granule_ids"] = sorted(set(self.granule_ids))
        if self.service_chain is not None:
            payload["service_chain"] = list(self.service_chain)
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
