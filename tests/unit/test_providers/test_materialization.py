"""``MaterializationRecord`` — delivered-content identity (PRD 023).

Pure, DB-free tests of the digest itself: what it collides on, what it
separates, and what it refuses to claim when a provider reports no granule
identifiers. The worker-side persistence and drift detection live in
``tests/unit/test_jobs/test_materialization.py``.
"""

from __future__ import annotations

from earthdata_mcp.providers.base import MaterializedResult
from earthdata_mcp.providers.materialization import MaterializationRecord
from earthdata_mcp.providers.request_spec import RequestSpec, compute_request_signature


def _spec(**overrides) -> RequestSpec:
    """A durable spec with only the fields the record is built from."""
    return RequestSpec.from_jsonb({"output_format": "application/netcdf4", **overrides})


def _record(**overrides) -> MaterializationRecord:
    base = {
        "granule_ids": ("G2-X", "G1-X"),
        "provider_used": "harmony",
        "service_chain": None,
        "output_format": "application/netcdf4",
    }
    base.update(overrides)
    return MaterializationRecord(**base)


def test_granule_order_and_duplicates_never_change_the_digest() -> None:
    """The same delivered granule set is the same content, however it is spelled."""
    reordered = _record(granule_ids=("G1-X", "G2-X"))
    duplicated = _record(granule_ids=("G2-X", "G1-X", "G2-X"))

    assert reordered.content_digest == _record().content_digest
    assert duplicated.content_digest == _record().content_digest


def test_a_different_granule_set_is_a_different_digest() -> None:
    extra_granule = _record(granule_ids=("G1-X", "G2-X", "G3-X"))

    assert extra_granule.content_digest != _record().content_digest


def test_a_provider_reporting_no_granule_ids_is_partial_but_still_stable() -> None:
    """An AppEEARS point sample names no granules: the record says so rather
    than pretending the granule list was empty, and still digests stably."""
    first = _record(granule_ids=None, provider_used="appeears")
    replay = _record(granule_ids=None, provider_used="appeears")
    empty = _record(granule_ids=(), provider_used="appeears")

    assert first.partial is True
    assert first.content_digest == replay.content_digest
    # "no granule list available" is not "an empty granule list".
    assert empty.partial is False
    assert first.content_digest != empty.content_digest


def test_same_request_signature_different_routing_is_a_different_digest() -> None:
    """The PRD's thesis, asserted directly: a signature answers "the same ask?"
    and a digest answers "the same bytes?" — an unpinned Harmony submit that
    fell back to OPeNDAP is the same ask and different bytes."""
    ask = {
        "concept_id": "C1-X",
        "bbox": (-10.0, 0.0, 10.0, 20.0),
        "time_range": "2024-01-01/2024-01-02",
        "variables": ("no2",),
        "output_format": "application/netcdf4",
    }
    assert compute_request_signature(**ask) == compute_request_signature(**ask)

    via_harmony = _record(provider_used="harmony")
    via_opendap = _record(provider_used="opendap")

    assert via_harmony.content_digest != via_opendap.content_digest


def test_the_realized_service_chain_separates_two_otherwise_identical_results() -> None:
    """An unpinned submit's realized chain is part of delivered-content identity."""
    unreported = _record()
    l2ss = _record(service_chain=("l2-subsetter",))
    stitchee = _record(service_chain=("l2-subsetter", "stitchee"))

    assert len({unreported.content_digest, l2ss.content_digest, stitchee.content_digest}) == 3


# -- durable (de)serialization + construction from a provider result ----------


def test_jsonb_round_trip_preserves_identity_and_carries_the_digest() -> None:
    record = _record(service_chain=("l2-subsetter",))

    stored = record.to_jsonb()

    assert stored["content_digest"] == record.content_digest
    assert stored["partial"] is False
    assert MaterializationRecord.from_jsonb(stored) == record


def test_an_absent_granule_list_stays_absent_through_storage() -> None:
    """Never round-trips as ``[]`` — a partial record must stay legible as one."""
    stored = _record(granule_ids=None).to_jsonb()

    assert "granule_ids" not in stored
    assert stored["partial"] is True
    assert MaterializationRecord.from_jsonb(stored).granule_ids is None


def test_a_legacy_job_with_no_record_reads_back_as_none() -> None:
    """Handles materialized before this PRD have no block — never a fabricated one."""
    assert MaterializationRecord.from_jsonb(None) is None
    assert MaterializationRecord.from_jsonb({}) is None


def test_built_from_what_the_provider_reported_not_from_the_plan() -> None:
    """``provider_used`` is the provider that actually ran — the spec's own
    ``provider`` field is rewritten to ``opendap`` by the worker's runtime
    fallback, so a Harmony-planned job that fell back records ``opendap``."""
    result = MaterializedResult(
        storage_key="opendap/job_x/subset.nc.zip",
        media_type="application/zip",
        extra={"granule_ids": ["https://o/g2.nc", "https://o/g1.nc"], "granule_count": 2},
    )
    spec = _spec(provider="opendap")

    record = MaterializationRecord.from_result(result, spec)

    assert record.provider_used == "opendap"
    assert record.granule_ids == ("https://o/g2.nc", "https://o/g1.nc")
    assert record.output_format == "application/netcdf4"
    assert record.partial is False


def test_a_provider_that_reports_no_granule_ids_yields_a_partial_record() -> None:
    result = MaterializedResult(
        storage_key="appeears/job_x/series.parquet", media_type="application/x-parquet"
    )

    record = MaterializationRecord.from_result(result, _spec(provider="appeears"))

    assert record.partial is True
    assert record.to_jsonb()["content_digest"]
