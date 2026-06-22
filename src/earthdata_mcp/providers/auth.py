"""Earthdata Login (EDL) via ``earthaccess`` (PLAN.md §4.2, §4.6).

EDL is the credential every Harmony transform and data download requires, which
is why PLAN.md pulls auth forward to Phase 4 — before the first real submit.

Adapted from TTA's ``utils/earthaccess_client.py`` (audit decision: *reuse,
light adaptation*, ``docs/tta_audit.md``): the lazy, thread-safe, cached-session
bootstrap is kept; ``config.settings`` is swapped for our :func:`get_settings`;
and the ``earthaccess`` module is injectable so unit tests can mock the session
without a network round-trip.

**Credentials come only from the environment / :class:`Settings`** (which loads a
git-ignored ``.env``) — never from a committed file (PLAN.md §4.2). The pinned
``earthaccess`` (>=0.18, see ``pyproject.toml``) folds both username/password and
``EARTHDATA_TOKEN`` into its single ``"environment"`` login strategy, so we set
the ``EARTHDATA_*`` env names and call that one strategy rather than juggling a
separate (non-existent) "token" strategy.

The authenticated **identity maps to workspace ownership** (PLAN.md §4.6):
:meth:`EDLAuth.identity` returns the EDL username a workspace is owned by.
"""

from __future__ import annotations

import logging
import os
import threading
from types import ModuleType

from earthdata_mcp.config import Settings, get_settings

logger = logging.getLogger(__name__)


class AuthError(RuntimeError):
    """EDL login failed or was attempted without usable credentials."""


class EDLAuth:
    """Lazy, thread-safe Earthdata Login session backed by ``earthaccess``.

    Constructing this does **not** authenticate; the first :meth:`login` (or any
    call that needs a session) performs environment-based login and caches it.
    Later calls reuse the cached session unless ``force=True``.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        earthaccess_module: ModuleType | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        # Imported lazily and injectable so unit tests can pass a mock module.
        if earthaccess_module is None:
            import earthaccess

            earthaccess_module = earthaccess
        self._ea = earthaccess_module
        self._auth: object | None = None
        self._lock = threading.Lock()

    # -- session lifecycle ------------------------------------------------

    def login(self, force: bool = False) -> object:
        """Return an authenticated ``earthaccess`` auth, logging in if needed.

        Double-checked under a lock so concurrent callers authenticate once.
        """
        if self._auth is not None and not force:
            return self._auth
        with self._lock:
            if self._auth is not None and not force:
                return self._auth
            self._export_credentials_to_env()
            auth = self._ea.login(strategy="environment")
            if not auth or not getattr(auth, "authenticated", False):
                raise AuthError("earthaccess login did not return an authenticated session")
            self._auth = auth
            logger.info("EDL authenticated as %s", getattr(auth, "username", "?"))
            return self._auth

    @property
    def authenticated(self) -> bool:
        """Whether a session is cached and reports itself authenticated."""
        return bool(self._auth is not None and getattr(self._auth, "authenticated", False))

    def identity(self) -> str:
        """The EDL username this session owns — maps to workspace ownership (§4.6)."""
        auth = self.login()
        return getattr(auth, "username", "") or ""

    def get_s3_credentials(
        self, daac: str | None = None, provider: str | None = None
    ) -> dict[str, str]:
        """Fetch in-region S3 credentials for a DAAC/provider (PLAN.md §4.2).

        Delegates to the ``earthaccess`` session, which returns short-lived keys
        usable only from within the matching AWS region.
        """
        auth = self.login()
        return auth.get_s3_credentials(daac=daac, provider=provider)

    def reset(self) -> None:
        """Drop the cached session (test helper / forced re-auth)."""
        with self._lock:
            self._auth = None

    # -- internals --------------------------------------------------------

    def _export_credentials_to_env(self) -> None:
        """Copy EDL settings into the ``EARTHDATA_*`` env names earthaccess reads.

        Only fills names that are absent, so a real environment always wins over
        configured defaults. Never reads or writes a credential file.
        """
        s = self._settings
        if s.edl_username and not os.getenv("EARTHDATA_USERNAME"):
            os.environ["EARTHDATA_USERNAME"] = s.edl_username
        if s.edl_password and not os.getenv("EARTHDATA_PASSWORD"):
            os.environ["EARTHDATA_PASSWORD"] = s.edl_password
        if s.earthdata_token and not os.getenv("EARTHDATA_TOKEN"):
            os.environ["EARTHDATA_TOKEN"] = s.earthdata_token
