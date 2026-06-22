"""KMS keyword normalization, mirroring NASA's ``get_keywords`` (PLAN.md §1, 2.4).

Keyword normalization ("rain" → "PRECIPITATION AMOUNT") is genuinely hard, so we
mirror NASA's KMS approach rather than inventing our own vocabulary:

1. **Curated synonyms** (``catalog/data/concepts.yaml``) map colloquial terms to
   canonical GCMD science keywords — the authoritative mapping for our queries.
2. **Cached KMS dump** — a refresh-on-schedule snapshot of GCMD's science-keyword
   labels, used to expand/validate a term against the real vocabulary.
3. **Passthrough** — an unknown term is returned unchanged so search still works.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import httpx
import yaml

from earthdata_mcp.config import Settings, get_settings

_CONCEPTS_YAML = Path(__file__).parent / "data" / "concepts.yaml"
_KMS_CACHE_FILE = "kms_keywords.json"
# GCMD science-keyword concept scheme.
_KMS_SCHEME_PATH = "/concepts/concept_scheme/sciencekeywords"


class KMSCatalog:
    """Normalizes colloquial terms to GCMD science keywords."""

    def __init__(
        self, settings: Settings | None = None, cache_dir: str | Path | None = None
    ) -> None:
        self._settings = settings or get_settings()
        self._cache_dir = Path(cache_dir or self._settings.earthdata_data_dir)
        self._concepts = _load_concepts()
        self._labels: list[str] | None = None  # lazily loaded KMS dump

    # -- public API -------------------------------------------------------

    def normalize_keyword(self, term: str) -> list[str]:
        """Curated synonyms → KMS-dump matches → passthrough ``[term]``."""
        key = term.strip().lower()
        if key in self._concepts:
            return list(self._concepts[key])
        matches = self._match_kms(key)
        if matches:
            return matches
        return [term]

    def refresh(self, *, force: bool = False) -> bool:
        """Refresh the cached KMS dump if missing or stale. Returns True if fetched."""
        if not force and self._cache_fresh():
            return False
        labels = self._fetch_kms_labels()
        self._write_cache(labels)
        self._labels = labels
        return True

    # -- KMS dump cache ---------------------------------------------------

    @property
    def _cache_path(self) -> Path:
        return self._cache_dir / _KMS_CACHE_FILE

    def _cache_fresh(self) -> bool:
        path = self._cache_path
        if not path.is_file():
            return False
        try:
            payload = json.loads(path.read_text())
        except (ValueError, OSError):
            return False
        age = time.time() - payload.get("fetched_at", 0)
        return age < self._settings.kms_cache_ttl_seconds

    def _write_cache(self, labels: list[str]) -> None:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache_path.write_text(
            json.dumps({"fetched_at": time.time(), "labels": labels})
        )

    def _load_cache(self) -> list[str]:
        path = self._cache_path
        if not path.is_file():
            return []
        try:
            return json.loads(path.read_text()).get("labels", [])
        except (ValueError, OSError):
            return []

    def _fetch_kms_labels(self) -> list[str]:
        url = f"{self._settings.kms_url.rstrip('/')}{_KMS_SCHEME_PATH}"
        response = httpx.get(url, params={"format": "json"}, timeout=60.0)
        response.raise_for_status()
        return _extract_labels(response.json())

    def _match_kms(self, key: str) -> list[str]:
        if self._labels is None:
            self._labels = self._load_cache()
        if not self._labels:
            return []
        # Exact (case-insensitive) hit first, then substring containment.
        exact = [lbl for lbl in self._labels if lbl.lower() == key]
        if exact:
            return exact
        return [lbl for lbl in self._labels if key in lbl.lower()]


def _load_concepts() -> dict[str, list[str]]:
    """Load the curated colloquial → GCMD-keyword synonyms (keys lower-cased)."""
    if not _CONCEPTS_YAML.is_file():
        return {}
    data = yaml.safe_load(_CONCEPTS_YAML.read_text()) or {}
    concepts = data.get("concepts") or {}
    return {str(k).lower(): list(v) for k, v in concepts.items()}


def _extract_labels(payload: object) -> list[str]:
    """Pull GCMD keyword labels from a KMS JSON payload (tolerant of shape)."""
    labels: list[str] = []
    if isinstance(payload, dict):
        concepts = payload.get("concepts")
        if isinstance(concepts, list):
            for c in concepts:
                if isinstance(c, dict):
                    label = c.get("prefLabel") or c.get("label") or c.get("text")
                    if label:
                        labels.append(str(label))
                elif isinstance(c, str):
                    labels.append(c)
    elif isinstance(payload, list):
        for c in payload:
            if isinstance(c, str):
                labels.append(c)
            elif isinstance(c, dict):
                label = c.get("prefLabel") or c.get("label")
                if label:
                    labels.append(str(label))
    return labels


def normalize_keyword(term: str) -> list[str]:
    """Module-level convenience wrapper over a default :class:`KMSCatalog`."""
    return KMSCatalog().normalize_keyword(term)
