"""
proxy/cache.py
LRU in-memory response cache for GET requests.

Cache key  : URL (query-string included)
Cache entry: (status, body_bytes, headers_dict, cached_at)
TTL        : derived from Cache-Control max-age or a configured default.
             Entries with no-store / no-cache directives are never stored.

The module exposes a single module-level CACHE singleton initialised by
proxy_server.main() via init().  max_size=0 disables caching entirely.

Thread-safety: all public methods hold a threading.Lock so the cache is
safe to call from asyncio coroutines running in the same event loop thread
as well as from background threads.
"""

import threading
import time
from collections import OrderedDict
from typing import Optional

from proxy.logging_setup import log


class ResponseCache:
    """LRU cache for full HTTP responses.

    Parameters
    ----------
    max_size   : maximum number of cached entries (0 = disabled)
    default_ttl: TTL in seconds when no Cache-Control max-age is present
    max_body   : bodies larger than this (bytes) are not stored (default 64 MB)
    """

    def __init__(
        self,
        max_size: int = 256,
        default_ttl: float = 300.0,
        max_body: int = 64 * 1024 * 1024,
    ):
        self.max_size    = max_size
        self.default_ttl = default_ttl
        self.max_body    = max_body
        self._lock       = threading.Lock()
        # OrderedDict used as LRU: most-recent at the end
        self._store: OrderedDict[str, dict] = OrderedDict()
        self._hits   = 0
        self._misses = 0

    @property
    def enabled(self) -> bool:
        return self.max_size > 0

    # ── public API ────────────────────────────────────────────────────────────

    def get(self, url: str) -> Optional[tuple[int, bytes, dict]]:
        """Return (status, body, headers) for a cached URL, or None on miss."""
        if not self.enabled:
            return None
        with self._lock:
            entry = self._store.get(url)
            if entry is None:
                self._misses += 1
                return None
            if time.monotonic() > entry["expires"]:
                del self._store[url]
                self._misses += 1
                return None
            # Move to end (most-recently-used)
            self._store.move_to_end(url)
            self._hits += 1
            log.debug(f"Cache HIT  {url}  ({self._hits} hits / {self._misses} misses)")
            return entry["status"], entry["body"], entry["headers"]

    def get_validators(self, url: str) -> Optional[tuple[Optional[str], Optional[str]]]:
        """Return (etag, last_modified) for a cached URL so the caller can send a
        conditional request to the origin (fix #10).  Returns None on cache miss."""
        if not self.enabled:
            return None
        with self._lock:
            entry = self._store.get(url)
            if entry is None:
                return None
            headers = entry["headers"]
            return headers.get("ETag"), headers.get("Last-Modified")

    def put(self, url: str, status: int, body: bytes, headers: dict):
        """Store a response.  Skips if disabled, body is too large, or
        Cache-Control says not to cache."""
        if not self.enabled:
            return
        if len(body) > self.max_body:
            log.debug(f"Cache SKIP (body {len(body)//1024}KB > max {self.max_body//1024}KB) {url}")
            return

        cc = headers.get("Cache-Control", "").lower()
        if "no-store" in cc or "no-cache" in cc or "private" in cc:
            log.debug(f"Cache SKIP (Cache-Control: {cc!r}) {url}")
            return

        ttl = self.default_ttl
        for part in cc.split(","):
            part = part.strip()
            if part.startswith("max-age="):
                try:
                    ttl = float(part[8:])
                except ValueError:
                    pass
                break

        if ttl <= 0:
            return

        with self._lock:
            if url in self._store:
                self._store.move_to_end(url)
            self._store[url] = {
                "status":  status,
                "body":    body,
                "headers": dict(headers),
                "expires": time.monotonic() + ttl,
                "cached_at": time.time(),
            }
            # Evict oldest when over capacity
            while len(self._store) > self.max_size:
                evicted, _ = self._store.popitem(last=False)
                log.debug(f"Cache EVICT (LRU) {evicted}")
        log.debug(f"Cache STORE ttl={ttl:.0f}s  {url}")

    def invalidate(self, url: str):
        with self._lock:
            self._store.pop(url, None)

    def clear(self):
        with self._lock:
            self._store.clear()
            self._hits   = 0
            self._misses = 0

    @property
    def stats(self) -> dict:
        with self._lock:
            return {
                "size":     len(self._store),
                "max_size": self.max_size,
                "hits":     self._hits,
                "misses":   self._misses,
                "hit_rate": round(self._hits / max(1, self._hits + self._misses), 3),
                "enabled":  self.enabled,
            }


# ── module-level singleton ────────────────────────────────────────────────────

CACHE: ResponseCache = ResponseCache(max_size=0)   # disabled until init() called


def init(max_size: int = 256, default_ttl: float = 300.0, max_body: int = 64 * 1024 * 1024):
    """Called once from proxy_server.main() with config values."""
    global CACHE
    CACHE = ResponseCache(max_size=max_size, default_ttl=default_ttl, max_body=max_body)
    if CACHE.enabled:
        log.info(
            f"ResponseCache: max_size={max_size}  "
            f"default_ttl={default_ttl:.0f}s  "
            f"max_body={max_body // (1024*1024)}MB"
        )
    else:
        log.info("ResponseCache: disabled (cache_size=0)")
