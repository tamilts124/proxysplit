"""
proxy/fetchers/chunk.py
fetch_chunk — byte-range chunk fetcher with per-proxy adaptive timeouts,
shared retry-budget enforcement, and circuit-breaker awareness.
"""

import asyncio
import time
from typing import Optional

import aiohttp

from proxy.logging_setup import log
from proxy.stats import STATS
from proxy.rate_limiter import get_limiter
from proxy.session_pool import http_proxy_param, pick_proxy, record_failure, record_success

# Injected by proxy_server.main()
CHUNK_SIZER = None      # AdaptiveChunkSizer instance or None
SESSION_POOL = None     # SessionPool instance

# Score-adaptive timeout scaling bounds
_MIN_TIMEOUT_FACTOR = 0.6   # fast proxies → tighter timeout
_MAX_TIMEOUT_FACTOR = 2.5   # slow/unknown proxies → more slack


def _adaptive_timeout(proxy_url: str, base_timeout: float) -> float:
    """Return a per-proxy chunk timeout scaled inversely to the proxy score.

    Uses a floor of 0.1 on the score so recently-recovered proxies don't
    receive astronomically long timeouts while their stats rebuild.

    score ≈ 1.0 (excellent) → factor ≈ MIN_TIMEOUT_FACTOR (tight)
    score ≈ 0.5 (unknown)   → factor ≈ 1.0 (unchanged)
    score ≈ 0.1 (floor)     → factor = MAX_TIMEOUT_FACTOR
    """
    score  = max(0.1, STATS.score(proxy_url))
    factor = max(_MIN_TIMEOUT_FACTOR, min(_MAX_TIMEOUT_FACTOR, 0.5 / score))
    return base_timeout * factor


async def fetch_chunk(
    url: str, start: int, end: int, index: int,
    proxy_url: str, chunk_retries: int, chunk_proxy: bool,
    chunk_timeout: float, connect_timeout: float,
    pool: Optional[str], weighted: bool,
    retry_budget: Optional[asyncio.Semaphore] = None,
    request_id: str = "-",
) -> tuple[int, bytes, int, str]:
    """
    Fetch one byte-range chunk with per-chunk size validation.
    Retries with a fresh proxy on size mismatch or any exception.
    Circuit breaker is checked before every attempt.

    retry_budget: shared asyncio.Semaphore across all chunks in one request;
                  each retry attempt (beyond the first) consumes one slot so
                  total retries are bounded even when many chunks fail at once.
    """
    from proxy.circuit_breaker import BREAKER

    headers       = {"Range": f"bytes={start}-{end}"}
    expected_size = end - start + 1
    last_exc: Exception = RuntimeError("no attempts")
    current = proxy_url

    for attempt in range(1, chunk_retries + 1):
        if BREAKER and not BREAKER.allow_request(current):
            alt = pick_proxy(pool=pool, exclude_set={current}, weighted=weighted)
            if alt:
                current = alt

        # First attempt is free; subsequent attempts consume budget
        if retry_budget and attempt > 1:
            try:
                await asyncio.wait_for(retry_budget.acquire(), timeout=30)
            except asyncio.TimeoutError:
                raise RuntimeError(
                    f"[{request_id}] Chunk #{index} timed out waiting for retry budget "
                    f"(all {retry_budget._value + (retry_budget._bound - retry_budget._value) if hasattr(retry_budget, '_bound') else '?'} "
                    f"retry slots are in use)"
                )
            except Exception as exc:
                raise RuntimeError(
                    f"[{request_id}] Chunk #{index} retry budget error: {exc}"
                )

        log.debug(
            f"  [{request_id}] Chunk #{index:03d} [{start:,}-{end:,}] "
            f"via {current} attempt {attempt}/{chunk_retries}"
        )
        await get_limiter(current).acquire()
        t0      = time.monotonic()
        timeout = _adaptive_timeout(current, chunk_timeout)
        try:
            session = SESSION_POOL.get(current)
            async with session.get(
                url, headers=headers, proxy=http_proxy_param(current),
                timeout=aiohttp.ClientTimeout(total=timeout, connect=connect_timeout),
            ) as resp:
                data = await resp.read()
                lat  = (time.monotonic() - t0) * 1000

                if len(data) != expected_size:
                    raise ValueError(
                        f"Chunk #{index} size mismatch: expected {expected_size}, "
                        f"got {len(data)} (status={resp.status})"
                    )

                record_success(current, len(data), lat)
                if CHUNK_SIZER:
                    CHUNK_SIZER.record(lat)
                return index, data, resp.status, current

        except aiohttp.ClientConnectorError as exc:
            # Connector-level error (e.g. SOCKS server restarted).
            # Drop the cached session so the next attempt rebuilds it (fix #7).
            last_exc = exc
            SESSION_POOL.invalidate_sync(current)
            record_failure(current)
            log.warning(
                f"  [{request_id}] Chunk #{index:03d} attempt {attempt} "
                f"via {current}: connector error (session invalidated): {exc}"
            )
            if attempt < chunk_retries and chunk_proxy:
                nxt = pick_proxy(pool=pool, exclude_set={current}, weighted=weighted)
                if nxt:
                    current = nxt

        except Exception as exc:
            last_exc = exc
            record_failure(current)
            log.warning(
                f"  [{request_id}] Chunk #{index:03d} attempt {attempt} "
                f"via {current}: {exc}"
            )
            if attempt < chunk_retries and chunk_proxy:
                nxt = pick_proxy(pool=pool, exclude_set={current}, weighted=weighted)
                if nxt:
                    current = nxt

    raise last_exc
