"""Adjacent-granule prefetch — the prediction itself (PRD 025).

``predict_next`` is pure: a spec in, a spec (or ``None``) out, no I/O and no
provider knowledge. These tests read it as a specification of *what* the server
guesses next, never how — the whole point of isolating it is that a second
prediction can be added later without touching the gates that decide whether to
act on one.

The gates and the enqueue live in ``test_prefetch_gates.py``.
"""

from __future__ import annotations

from earthdata_mcp.jobs.prefetch import predict_next
from earthdata_mcp.providers.request_spec import RequestSpec


def _spec(**overrides) -> RequestSpec:
    """A minimal durable spec — the fields prediction actually reads, plus the
    ones an equality assertion needs to prove nothing *else* moved."""
    base = dict(
        concept_id="C1-LPCLOUD",
        short_name="TEMPO_NO2_L3",
        output_format="application/netcdf4",
        output_shape="grid",
        needs_bbox=True,
        needs_variable=True,
        needs_temporal=True,
        needs_point_sample=False,
        aoi_bbox=(-75.6, 38.9, -73.9, 41.4),
        time_range="2024-04-01/2024-04-08",
        variables=("vertical_column_troposphere",),
        provider="harmony",
        service_name="l3-subsetter",
        opendap_urls=(),
        coord_lat="lat",
        coord_lon="lon",
        coord_time="time",
        lat_axis=None,
        lon_axis=None,
        var_dims={},
        var_coords={},
        variable_expansions={},
        workspace_id="ws-1",
        job_handle="job_abc",
        obs_handle="obs_abc",
        cache_key_value="deadbeef",
    )
    base.update(overrides)
    return RequestSpec(**base)


def test_predicts_the_next_contiguous_window_of_the_same_length() -> None:
    """The single prediction this feature makes: ``[T0, T1)`` predicts
    ``[T1, T1 + (T1 - T0))`` — same length, immediately adjacent."""
    predicted = predict_next(_spec(time_range="2024-04-01/2024-04-08"))

    assert predicted is not None
    assert predicted.time_range == "2024-04-08T00:00:00Z/2024-04-15T00:00:00Z"


def test_nothing_but_the_window_changes() -> None:
    """Same collection, same AOI, same variables, same format, same provider —
    varying anything else would be a different (and much weaker) prediction."""
    original = _spec()
    predicted = predict_next(original)

    assert predicted is not None
    for field in (
        "concept_id",
        "short_name",
        "output_format",
        "aoi_bbox",
        "variables",
        "provider",
        "service_name",
        "workspace_id",
    ):
        assert getattr(predicted, field) == getattr(original, field), field


def test_a_spec_with_no_time_range_predicts_nothing() -> None:
    """There is no "next window" without a window."""
    assert predict_next(_spec(time_range=None)) is None


def test_a_point_sample_spec_predicts_nothing() -> None:
    """A point sample is a series at a location, not a step through windows —
    stepping it forward is not the pattern this feature observed."""
    assert predict_next(_spec(needs_point_sample=True)) is None


def test_a_zero_length_window_predicts_nothing() -> None:
    """A degenerate window has no length to advance by; predicting the same
    instant again would be a guaranteed duplicate."""
    assert predict_next(_spec(time_range="2024-04-01/2024-04-01")) is None


def test_the_prediction_carries_no_granule_urls_from_the_old_window() -> None:
    """OPeNDAP URLs are granule URLs for the *old* window. Carrying them into
    the next window's spec would fetch April 1–8's granules and file them under
    April 8–15's signature — the one failure that would poison the cache."""
    predicted = predict_next(
        _spec(provider="opendap", opendap_urls=("https://opendap/g1.nc", "https://opendap/g2.nc"))
    )

    assert predicted is not None
    assert predicted.opendap_urls == ()


def test_the_prediction_belongs_to_no_job_yet() -> None:
    """A predicted spec is not the originating job's spec: its handles are the
    caller's to mint, and its cache key cannot be honestly reconstructed here
    (the service-capabilities version it folds in is not persisted)."""
    predicted = predict_next(_spec())

    assert predicted is not None
    assert not predicted.job_handle
    assert not predicted.obs_handle
    assert not predicted.cache_key_value
