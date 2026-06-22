"""Runtime settings loaded from the environment via pydantic-settings.

Storage backend selection is config-only (``EARTHDATA_STORAGE``): the rest of the
system speaks to the ``StorageBackend`` interface and never imports a concrete
backend. See PLAN.md §4.4.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process configuration. Field names map to upper-cased env vars."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # CMR / Harmony / KMS (metadata services, Phase 2) --------------------
    # Canon is CMR's public API + UMM schemas (PLAN.md §1).
    cmr_url: str = "https://cmr.earthdata.nasa.gov"
    harmony_url: str = "https://harmony.earthdata.nasa.gov"
    # CMR asks every client to identify itself — set OUR own id, never NASA's.
    cmr_client_id: str = "earthdata-retrieval-mcp"
    # GCMD Keyword Management Service (KMS) base for keyword normalization.
    kms_url: str = "https://gcmd.earthdata.nasa.gov/kms"
    # How long a cached KMS dump stays fresh before a refresh (default 7 days).
    kms_cache_ttl_seconds: int = 7 * 24 * 3600

    # Storage --------------------------------------------------------------
    # `local` (default) or an `s3://bucket/prefix` URL.
    earthdata_storage: str = "local"
    # Root for the local filesystem backend.
    earthdata_data_dir: str = "./data"
    # Materialization cache eviction cap (bytes); default ~5 GiB (§4.4).
    earthdata_cache_max_bytes: int = 5 * 1024**3

    # Database -------------------------------------------------------------
    database_url: str = (
        "postgresql+asyncpg://postgres:postgres@localhost:5432/earthdata_mcp"
    )

    # Worker broker (Arq / Redis) -----------------------------------------
    redis_url: str = "redis://localhost:6379/0"

    # Earthdata Login (used from Phase 4) ---------------------------------
    edl_username: str = ""
    edl_password: str = ""
    earthdata_token: str = ""

    # Logging --------------------------------------------------------------
    log_level: str = "INFO"

    @property
    def storage_kind(self) -> str:
        """``"s3"`` when ``earthdata_storage`` is an s3 URL, else ``"local"``."""
        return "s3" if self.earthdata_storage.startswith("s3://") else "local"


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings singleton."""
    return Settings()
