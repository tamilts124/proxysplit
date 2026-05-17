"""
proxy/session_pool.py
SessionPool: one aiohttp.ClientSession per proxy URL,
with TCPConnector(limit=30) per session to prevent fd exhaustion.

Also: connector helpers, proxy selection, and distribute_proxies.

All singletons (SESSION_POOL, BREAKER) are set by proxy_server.main()
after construction; modules read them at call time, not import time.
"""

import random
from typing import Optional

import aiohttp
from aiohttp_socks import ProxyConnector

SUPPORTED_SCHEMES = ("http://", "https://", "socks4://", "socks4a://", "socks5://")

# ── Singleton set by proxy_server.main() ─────────────────────────────────────
SESSION_POOL: Optional["SessionPool"] = None

# Set by proxy_server.main() so circular imports never trigger at import time
BREAKER = None


# ── Connector / proxy-param helpers ──────────────────────────────────────────

def make_connector(proxy_url: Optional[str]) -> aiohttp.BaseConnector:
    """TCPConnector(limit=30) for HTTP/direct; ProxyConnector for SOCKS."""
    if proxy_url and proxy_url.startswith("socks"):
        return ProxyConnector.from_url(proxy_url, limit=30)
    return aiohttp.TCPConnector(limit=30)


def http_proxy_param(proxy_url: Optional[str]) -> Optional[str]:
    """Only HTTP(S) proxies use the proxy= kwarg; SOCKS are connector-level."""
    if proxy_url and proxy_url.startswith("http"):
        return proxy_url
    return None


# ── Proxy selection ───────────────────────────────────────────────────────────

def pick_proxy(
    pool: Optional[str] = None,
    exclude_set: Optional[set] = None,
    pin: Optional[str] = None,
    weighted: bool = True,
) -> Optional[str]:
    """Select one proxy from the live registry, honouring circuit breaker."""
    from proxy.registry import REGISTRY
    if pin:
        return pin
    if not REGISTRY:
        return None
    if weighted:
        return REGISTRY.score_choice(pool=pool, exclude_set=exclude_set)
    return REGISTRY.random_choice(pool=pool, exclude_set=exclude_set)


def record_failure(proxy_url: str):
    """Record a proxy failure in stats and notify the circuit breaker."""
    from proxy.stats import STATS
    from proxy.circuit_breaker import BREAKER
    consec = STATS.record_failure(proxy_url)
    if BREAKER:
        BREAKER.record_failure(proxy_url, consec)


def record_success(proxy_url: str, nbytes: int, latency_ms: float):
    """Record a proxy success in stats and notify the circuit breaker."""
    from proxy.stats import STATS
    from proxy.circuit_breaker import BREAKER
    STATS.record_success(proxy_url, nbytes, latency_ms)
    if BREAKER:
        BREAKER.record_success(proxy_url)


def distribute_proxies(
    num_chunks: int,
    pool: Optional[str] = None,
    pin: Optional[str] = None,
    weighted: bool = True,
    exclude_window: int = 3,
) -> list[str]:
    """
    Assign one proxy per chunk.
    Consecutive chunks avoid the last min(pool_size-1, exclude_window) proxies
    so load is spread evenly across the whole pool.

    Falls back to the first available proxy when the exclude window exhausts
    all candidates, and logs a warning so the operator knows the pool is small.
    """
    from proxy.registry import REGISTRY
    from proxy.logging_setup import log

    if pin:
        return [pin] * num_chunks
    available = REGISTRY.available(pool) if REGISTRY else []
    if not available:
        return []
    if len(available) == 1:
        return [available[0]] * num_chunks

    win = min(len(available) - 1, exclude_window)
    assigned: list[str] = []
    fallback_count = 0
    # Pre-compute scores once for the fallback (fix #4: pick best, not first)
    _score_cache: dict[str, float] = {}

    def _best_available() -> str:
        nonlocal _score_cache
        if not _score_cache:
            from proxy.stats import STATS
            _score_cache = STATS.scores_snapshot()
        return max(available, key=lambda p: _score_cache.get(p, 0.5))

    for _ in range(num_chunks):
        excl   = set(assigned[-win:]) if assigned else set()
        choice = pick_proxy(pool=pool, exclude_set=excl, weighted=weighted)
        if choice is None:
            # exclude_window exhausted all candidates — fall back to best proxy
            fallback_count += 1
            choice = _best_available()
        assigned.append(choice)

    if fallback_count:
        log.warning(
            f"distribute_proxies: exclude_window={win} exhausted candidates "
            f"{fallback_count}/{num_chunks} time(s); pool has only {len(available)} proxies. "
            "Consider adding more proxies or reducing exclude_window."
        )
    return assigned


# ── SessionPool ───────────────────────────────────────────────────────────────

class SessionPool:
    """One aiohttp.ClientSession per proxy URL, lazily created and cached."""

    def __init__(self):
        self._sessions: dict[str, aiohttp.ClientSession] = {}

    def get(self, proxy_url: Optional[str]) -> aiohttp.ClientSession:
        key = proxy_url or "__direct__"
        s = self._sessions.get(key)
        if s is None or s.closed:
            self._sessions[key] = aiohttp.ClientSession(
                connector=make_connector(proxy_url)
            )
        return self._sessions[key]

    def invalidate_sync(self, proxy_url: Optional[str]):
        """Synchronously drop a session from the pool without closing it.
        Safe to call from a non-async context (e.g. inside fetch_chunk).
        The old session will be garbage-collected; aiohttp will close its
        underlying connector when the last reference drops."""
        key = proxy_url or "__direct__"
        self._sessions.pop(key, None)

    async def prewarm(self, proxy_urls: list[str], target_url: str, timeout: float = 8.0):
        """Open keep-alive connections to the given proxies without waiting for
        a real request.  A lightweight HEAD against target_url exercises the
        TCP stack so the first chunk fetch hits a warmed connection.
        Errors are silently ignored — pre-warming is best-effort.
        """
        import asyncio

        async def _warm_one(proxy_url: str):
            try:
                session     = self.get(proxy_url)
                proxy_param = http_proxy_param(proxy_url)
                async with session.head(
                    target_url,
                    proxy=proxy_param,
                    timeout=aiohttp.ClientTimeout(total=timeout, connect=timeout / 2),
                    allow_redirects=False,
                ):
                    pass
            except Exception:
                pass

        await asyncio.gather(*[_warm_one(p) for p in proxy_urls], return_exceptions=True)

    async def invalidate(self, proxy_url: Optional[str]):
        key = proxy_url or "__direct__"
        s   = self._sessions.pop(key, None)
        if s and not s.closed:
            await s.close()

    async def close_all(self):
        for s in self._sessions.values():
            if not s.closed:
                await s.close()
        self._sessions.clear()
