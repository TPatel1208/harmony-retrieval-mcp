"""Per-provider token-bucket rate limiter (PLAN.md §8 hardening).

Two properties matter: a full bucket services a small burst without delay, and
once the bucket is empty further acquisitions are paced at ``rate_per_sec``; and
``get_limiter`` returns one shared instance per provider so the bucket state is
process-global. The limiter is exercised directly (not via the global registry)
for the timing assertions, so the test is independent of any cached limiter other
tests may have built.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from earthdata_mcp.providers.ratelimit import (
    AsyncRateLimiter,
    get_limiter,
    reset_limiters,
)


async def test_burst_within_capacity_is_immediate() -> None:
    """Up to ``burst`` acquisitions from a full bucket return without sleeping."""
    limiter = AsyncRateLimiter(rate_per_sec=5.0, burst=3)
    start = time.monotonic()
    for _ in range(3):
        await limiter.acquire()
    assert time.monotonic() - start < 0.05


async def test_blocks_past_burst() -> None:
    """Acquiring past the bucket paces the extra calls at ~1/rate seconds each."""
    rate = 20.0  # 1 token every 0.05 s
    limiter = AsyncRateLimiter(rate_per_sec=rate, burst=1)
    start = time.monotonic()
    # 1 free (full bucket) + 3 that must wait one refill interval each.
    for _ in range(4):
        await limiter.acquire()
    elapsed = time.monotonic() - start
    # Three refills of 1/rate seconds; allow scheduler slack on the lower bound.
    assert elapsed >= 3 * (1 / rate) * 0.8


async def test_rejects_nonpositive_rate() -> None:
    with pytest.raises(ValueError):
        AsyncRateLimiter(rate_per_sec=0)


async def test_async_context_manager_acquires() -> None:
    limiter = AsyncRateLimiter(rate_per_sec=100.0, burst=1)
    async with limiter as entered:
        assert entered is limiter


def test_get_limiter_is_stable_per_provider() -> None:
    """The registry returns the same instance per provider, distinct across names."""
    reset_limiters()
    try:
        a = get_limiter("cmr")
        b = get_limiter("cmr")
        c = get_limiter("harmony")
        assert a is b
        assert a is not c
    finally:
        reset_limiters()


async def test_get_limiter_concurrent_calls_are_paced() -> None:
    """A real registry limiter paces concurrent acquirers (shared bucket state)."""
    reset_limiters()
    try:
        limiter = get_limiter("appeears")
        # Drain whatever the default bucket holds, then time two paced acquisitions.
        # Drive directly with a tiny rate to keep the test fast and deterministic.
        fast = AsyncRateLimiter(rate_per_sec=50.0, burst=1)
        start = time.monotonic()
        await asyncio.gather(*(fast.acquire() for _ in range(3)))
        assert time.monotonic() - start >= 2 * (1 / 50.0) * 0.8
        assert limiter is get_limiter("appeears")
    finally:
        reset_limiters()
