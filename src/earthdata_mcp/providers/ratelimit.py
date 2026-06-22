"""Per-provider async rate limiting (PLAN.md §8 hardening).

A small token-bucket limiter applied at each provider's outbound-HTTP boundary, so
a burst of tool calls (or a tight poll loop across many jobs) cannot hammer CMR /
Harmony / AppEEARS / OPeNDAP past a polite rate. The bucket refills continuously at
``rate_per_sec`` up to ``burst`` tokens; ``async with limiter`` (or ``acquire``)
takes one token, awaiting **only** when the bucket is empty — so normal, low-volume
traffic is never delayed. No new dependencies: just ``asyncio`` and a monotonic
clock.

Limiters are process-global, one per provider name, with rates read from config
(generous defaults). :func:`get_limiter` is the single place a provider looks up
its limiter, so wiring a new provider is one call.
"""

from __future__ import annotations

import asyncio
import time

from earthdata_mcp.config import get_settings


class AsyncRateLimiter:
    """A token-bucket limiter: ``rate_per_sec`` refill, ``burst`` capacity."""

    def __init__(self, rate_per_sec: float, burst: float | None = None) -> None:
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be positive")
        self._rate = float(rate_per_sec)
        # Default burst = one second's worth of tokens (min 1), so a single call is
        # always immediately serviceable from a full bucket.
        self._capacity = float(burst) if burst is not None else max(rate_per_sec, 1.0)
        self._tokens = self._capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Take one token, sleeping until the bucket has refilled enough.

        The lock serializes acquirers, which is the point — it enforces the rate
        across concurrent callers rather than letting them all pass at once.
        """
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(
                    self._capacity, self._tokens + (now - self._updated) * self._rate
                )
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Sleep just long enough for the one missing token to refill.
                await asyncio.sleep((1.0 - self._tokens) / self._rate)

    async def __aenter__(self) -> AsyncRateLimiter:
        await self.acquire()
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


#: Process-global per-provider limiters, built lazily from config.
_limiters: dict[str, AsyncRateLimiter] = {}

#: Fallback rate for a provider with no dedicated config field (req/sec).
_DEFAULT_RATE = 10.0


def get_limiter(provider: str) -> AsyncRateLimiter:
    """Return the process-wide limiter for ``provider`` (built on first use).

    The rate comes from a ``<provider>_rate_per_sec`` settings field when present
    (e.g. ``cmr_rate_per_sec``), else :data:`_DEFAULT_RATE`. The same instance is
    returned on every call so the bucket state is shared across the process.
    """
    limiter = _limiters.get(provider)
    if limiter is None:
        settings = get_settings()
        rate = getattr(settings, f"{provider}_rate_per_sec", _DEFAULT_RATE)
        limiter = AsyncRateLimiter(rate)
        _limiters[provider] = limiter
    return limiter


def reset_limiters() -> None:
    """Drop all cached limiters (test helper; never called in production)."""
    _limiters.clear()
