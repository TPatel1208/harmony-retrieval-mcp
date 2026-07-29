"""Runtime settings loaded from the environment via pydantic-settings.

Storage backend selection is config-only (``EARTHDATA_STORAGE``): the rest of the
system speaks to the ``StorageBackend`` interface and never imports a concrete
backend.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
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
    # Materialization cache eviction cap (bytes); default ~50 GiB. Raised from
    # 5 GiB with PRD 024: a single Harmony result can be gigabytes, and a
    # research box has disk. The cap is what bounds the cache; *what* it evicts
    # when it is reached is the eviction score's job (jobs/eviction.py).
    earthdata_cache_max_bytes: int = 50 * 1024**3
    # Ceiling on bytes any one workspace may hold pinned (PRD 024). Pinned
    # results are exempt from cap-driven eviction, so this is what stops pinning
    # from being a way to monopolise a shared cache. Default ~10 GiB.
    earthdata_pinned_bytes_max_per_workspace: int = 10 * 1024**3
    # Build cost assumed for a result whose job never recorded one (a legacy row,
    # or a transform-derived cube_ with no job). Deliberately non-zero: zero cost
    # makes a result infinitely evictable, which would purge exactly the oldest
    # and most established data first.
    earthdata_rebuild_cost_default_seconds: float = 60.0
    # How often orphan-blob reclamation runs. It is debris cleanup, not admission
    # control, so it no longer runs on every materialization — listing every blob
    # under four prefixes is O(cache) and belongs on a timer. Default 1 hour.
    earthdata_orphan_sweep_interval_seconds: float = 3600.0
    # estimate_retrieval_size warns when the extrapolated total exceeds this
    # (MB); default ~10 GB — the gate the tool's docstring promises.
    size_warning_threshold_mb: float = 10 * 1024
    # Worker startup reclaim (PRD 004): a non-terminal job older than this is
    # failed as stale instead of re-enqueued forever. Generous by default — real
    # Harmony/AppEEARS jobs can legitimately run for hours.
    job_stale_after_days: float = 7.0

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
    # Per-call credential encryption (PRD 022): the repo-private Fernet key the
    # durable job row's encrypted-credential column is written under — explicitly
    # a different key than any consumer holds. Comma-separate keys to rotate
    # (MultiFernet: first key encrypts, all keys decrypt). Empty means per-call
    # edl_token support is off; the env-credential path needs no key.
    earthdata_credential_key: str = ""

    # Per-provider rate limiting ------------------
    # Token-bucket refill rate in requests/sec at each provider's HTTP boundary.
    # Generous by default so normal traffic is never delayed — a backstop against
    # a runaway poll loop, not a throttle on ordinary use.
    cmr_rate_per_sec: float = 20.0
    harmony_rate_per_sec: float = 10.0
    appeears_rate_per_sec: float = 10.0
    opendap_rate_per_sec: float = 10.0

    # Concurrent in-flight granule downloads during materialize, shared by both
    # providers: bounds OPeNDAPProvider's per-materialize asyncio.Semaphore
    # (opendap/_runtime.py) and is exported as harmony-py's own NUM_REQUESTS_WORKERS
    # env var (jobs/worker.py, read by harmony.Client at construction). Both
    # providers stream granules straight to disk, so this trades network/decompress
    # parallelism against disk I/O and open file descriptors, not RAM — raising it
    # doesn't multiply peak memory the way concurrent in-RAM buffering would.
    retrieval_download_concurrency: int = 6

    # MCP transport / bearer token (PRD 018) --------------------------------
    # stdio (default) requires being the consumer's child process; http is the
    # opt-in for a separate-container consumer (e.g. a second compose stack).
    mcp_transport: Literal["stdio", "http"] = Field(
        default="stdio", validation_alias="EARTHDATA_MCP_TRANSPORT"
    )
    mcp_host: str = Field(default="0.0.0.0", validation_alias="EARTHDATA_MCP_HOST")
    mcp_port: int = Field(default=8765, validation_alias="EARTHDATA_MCP_PORT")
    # Static bearer token required on every HTTP request when set; ignored under
    # stdio. A tripwire, not real authn — see README's security posture note.
    mcp_token: str = Field(default="", validation_alias="EARTHDATA_MCP_TOKEN")

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
