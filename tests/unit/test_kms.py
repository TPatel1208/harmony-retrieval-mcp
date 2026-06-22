"""KMS keyword normalization (PLAN.md task 2.4)."""

from __future__ import annotations

from pathlib import Path

import pytest

from earthdata_mcp.config import Settings
from earthdata_mcp.catalog.kms import KMSCatalog


@pytest.fixture
def settings() -> Settings:
    return Settings(_env_file=None)


@pytest.fixture
def catalog(settings: Settings, tmp_path: Path) -> KMSCatalog:
    return KMSCatalog(settings, cache_dir=tmp_path)


def test_curated_synonym_maps_to_gcmd_keywords(catalog: KMSCatalog) -> None:
    assert "PRECIPITATION AMOUNT" in catalog.normalize_keyword("rain")


def test_curated_lookup_is_case_insensitive(catalog: KMSCatalog) -> None:
    assert catalog.normalize_keyword("Rain") == catalog.normalize_keyword("rain")


def test_unknown_term_passes_through(catalog: KMSCatalog) -> None:
    assert catalog.normalize_keyword("zzz-unmapped") == ["zzz-unmapped"]


def test_refresh_caches_dump_and_matches_labels(
    httpx_mock, catalog: KMSCatalog
) -> None:
    httpx_mock.add_response(
        json={
            "concepts": [
                {"prefLabel": "SEA SURFACE TEMPERATURE"},
                {"prefLabel": "SOIL MOISTURE"},
            ]
        }
    )
    assert catalog.refresh(force=True) is True
    assert catalog._cache_path.is_file()
    # A term not in the curated map now resolves via the cached KMS dump.
    assert catalog.normalize_keyword("soil moisture") == ["SOIL MOISTURE"]


def test_refresh_skips_when_cache_is_fresh(httpx_mock, catalog: KMSCatalog) -> None:
    httpx_mock.add_response(json={"concepts": [{"prefLabel": "SNOW DEPTH"}]})
    assert catalog.refresh(force=True) is True
    # Second call sees a fresh cache and must not fetch again.
    assert catalog.refresh() is False
    assert len(httpx_mock.get_requests()) == 1
