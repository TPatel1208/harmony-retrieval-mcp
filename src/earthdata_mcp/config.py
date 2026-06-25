"""Runtime settings loaded from the environment via pydantic-settings.

Storage backend selection is config-only (``EARTHDATA_STORAGE``): the rest of the
system speaks to the ``StorageBackend`` interface and never imports a concrete
backend.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process configuration. Field names map to upper-cased env vars."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # CMR / Harmony / KMS (metadata services, Phase 2) --------------------
    # Canon is CMR's public API + UMM schemas.
    cmr_url: str = "https://cmr.earthdata.nasa.gov"
    harmony_url: str = "https://harmony.earthdata.nasa.gov"
    # AppEEARS point/area sample API (Phase 7.4; Bearer-authenticated with the EDL
    # token below). Trailing path segments (``/task``, ``/bundle/...``) are appended.
    appeears_url: str = "https://appeears.earthdatacloud.nasa.gov/api"
    # CMR asks every client to identify itself — set OUR own id, never NASA's.
    cmr_client_id: str = "earthdata-retrieval-mcp"
    # GCMD Keyword Management Service (KMS) base for keyword normalization.
    kms_url: str = "https://gcmd.earthdata.nasa.gov/kms"
    # How long a cached KMS dump stays fresh before a refresh (default 7 days).
    kms_cache_ttl_seconds: int = 7 * 24 * 3600

    # Storage --------------------------------------------------------------
    # `local` (default) or an `s3://bucket/prefix` URL.
    earthdata_storage: str = "local"
    # Enable the direct-S3 fetch shortcut for "data as-is" plans. Off by default:
    # direct S3 reads work only from within the DAAC's AWS region, so we route to
    # Harmony unless this is explicitly turned on AND we are in-region. Env:
    # EARTHDATA_S3_DIRECT.
    s3_direct_enabled: bool = False
    # Root for the local filesystem backend.
    earthdata_data_dir: str = "./data"
    # Materialization cache eviction cap (bytes); default ~5 GiB
    earthdata_cache_max_bytes: int = 5 * 1024**3

    # Database -------------------------------------------------------------
    database_url: str = (
        "postgresql+asyncpg://postgres:postgres@localhost:5432/earthdata_mcp"
    )

    # Worker broker (Arq / Redis) -----------------------------------------
    redis_url: str = "redis://localhost:6379/0"

    # Earthdata Login ---------------------------------
    edl_username: str = ""
    edl_password: str = ""
    earthdata_token: str = ""

    # Per-provider rate limiting ------------------
    # Token-bucket refill rate in requests/sec at each provider's HTTP boundary.
    # Generous by default so normal traffic is never delayed — a backstop against
    # a runaway poll loop, not a throttle on ordinary use.
    cmr_rate_per_sec: float = 20.0
    harmony_rate_per_sec: float = 10.0
    appeears_rate_per_sec: float = 10.0
    opendap_rate_per_sec: float = 10.0

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
