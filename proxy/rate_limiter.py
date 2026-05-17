"""
proxy/rate_limiter.py
Token-bucket rate limiter, one bucket per proxy URL.
"""

import asyncio
import time

_RATE_LIMITERS: dict[str, "TokenBucket"] = {}
_GLOBAL_RPS: float = 0.0


class TokenBucket:
    """Async token-bucket rate limiter.  rps=0 means unlimited."""

    def __init__(self, rps: float):
        self.rps = rps
        self._tokens = float(rps) if rps > 0 else 0.0
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        if self.rps <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(self.rps, self._tokens + (now - self._last) * self.rps)
            self._last = now
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self.rps
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


def get_limiter(proxy_url: str) -> TokenBucket:
    """Return (creating if needed) the rate-limiter bucket for proxy_url."""
    if proxy_url not in _RATE_LIMITERS:
        _RATE_LIMITERS[proxy_url] = TokenBucket(_GLOBAL_RPS)
    return _RATE_LIMITERS[proxy_url]


def remove_limiter(proxy_url: str):
    """Remove the bucket when a proxy is removed from the pool."""
    _RATE_LIMITERS.pop(proxy_url, None)


def set_global_rps(rps: float):
    """Called once at startup from proxy_server.main()."""
    global _GLOBAL_RPS
    _GLOBAL_RPS = rps
