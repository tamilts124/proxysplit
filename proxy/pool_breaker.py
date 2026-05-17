"""
proxy/pool_breaker.py
Per-pool circuit breakers.

The global CircuitBreaker in circuit_breaker.py bans a proxy server-wide.
This module lets a proxy that is failing for one named pool (e.g. "cdn")
remain in service for another pool (e.g. "general").

Usage
-----
    from proxy.pool_breaker import POOL_BREAKER

    # Record a failure for a specific pool
    POOL_BREAKER.record_failure("http://1.2.3.4:8080", pool="cdn", consecutive=6)

    # Check before dispatching
    if not POOL_BREAKER.allow_request("http://1.2.3.4:8080", pool="cdn"):
        alt = pick_proxy(pool="cdn", exclude_set={"http://1.2.3.4:8080"})

The pool_breaker is independent of the global BREAKER — operators who want
pool-aware isolation import and wire it themselves; everyone else is unaffected.

A pool=None key is equivalent to the global breaker and is deliberately
*not* shared with it; they operate independently.
"""

import threading
import time
from typing import Optional

from proxy.logging_setup import log

_CB_CLOSED    = "CLOSED"
_CB_OPEN      = "OPEN"
_CB_HALF_OPEN = "HALF_OPEN"


class PoolCircuitBreaker:
    """
    One circuit-breaker state machine per (proxy_url, pool) pair.

    Parameters mirror CircuitBreaker in circuit_breaker.py.
    """

    def __init__(
        self,
        threshold: int   = 5,
        base_delay: float = 60.0,
        max_delay: float  = 3600.0,
    ):
        self.threshold  = threshold
        self.base_delay = base_delay
        self.max_delay  = max_delay
        self._lock      = threading.Lock()
        # key: (proxy_url, pool_name_or_None)
        self._breakers: dict[tuple[str, Optional[str]], dict] = {}

    def _key(self, url: str, pool: Optional[str]) -> tuple[str, Optional[str]]:
        return (url, pool)

    def _entry(self, url: str, pool: Optional[str]) -> dict:
        k = self._key(url, pool)
        if k not in self._breakers:
            self._breakers[k] = {
                "state": _CB_CLOSED, "strike": 0,
                "open_until": 0.0, "banned_at": None,
            }
        return self._breakers[k]

    def record_failure(self, url: str, pool: Optional[str], consecutive: int):
        """Open the breaker for (url, pool) if consecutive >= threshold."""
        if consecutive < self.threshold:
            return
        with self._lock:
            e = self._entry(url, pool)
            if e["state"] in (_CB_CLOSED, _CB_HALF_OPEN):
                e["strike"] += 1
                delay = min(self.base_delay * (2 ** (e["strike"] - 1)), self.max_delay)
                e["state"]      = _CB_OPEN
                e["open_until"] = time.monotonic() + delay
                e["banned_at"]  = time.time()
                log.warning(
                    f"   ⛔ PoolBreaker OPEN url={url} pool={pool!r} "
                    f"(strike={e['strike']}, delay={delay:.0f}s)"
                )

    def record_success(self, url: str, pool: Optional[str]):
        """Close the breaker for (url, pool) on success."""
        with self._lock:
            e = self._breakers.get(self._key(url, pool))
            if e and e["state"] == _CB_HALF_OPEN:
                e["state"]  = _CB_CLOSED
                e["strike"] = 0
                log.info(f"   ✓ PoolBreaker CLOSED url={url} pool={pool!r}")

    def allow_request(self, url: str, pool: Optional[str] = None) -> bool:
        """
        Return True if the request may proceed for this (url, pool) pair.
        Transitions OPEN → HALF_OPEN when the timer expires.
        """
        with self._lock:
            e = self._breakers.get(self._key(url, pool))
            if not e or e["state"] == _CB_CLOSED:
                return True
            if e["state"] == _CB_OPEN:
                if time.monotonic() < e["open_until"]:
                    return False
                e["state"] = _CB_HALF_OPEN
                log.info(f"   ↗ PoolBreaker HALF-OPEN url={url} pool={pool!r}")
                return True
            # HALF_OPEN — allow one probe
            return True

    def blocked_for(self, pool: Optional[str]) -> set[str]:
        """Return proxy URLs currently blocked for the given pool."""
        now = time.monotonic()
        with self._lock:
            return {
                url for (url, p), e in self._breakers.items()
                if p == pool and e["state"] == _CB_OPEN and now < e["open_until"]
            }

    def release(self, url: str, pool: Optional[str] = None):
        """Manually reset a (url, pool) breaker to CLOSED."""
        with self._lock:
            e = self._breakers.get(self._key(url, pool))
            if e:
                e["state"]  = _CB_CLOSED
                e["strike"] = 0
        log.info(f"   ✓ PoolBreaker manually reset: url={url} pool={pool!r}")

    def snapshot(self) -> dict:
        now = time.monotonic()
        with self._lock:
            return {
                f"{url}@{pool}": {
                    "url":   url, "pool": pool,
                    "state": e["state"],
                    "strike": e["strike"],
                    "remaining_s": max(0.0, round(e["open_until"] - now, 1)),
                    "banned_at": e.get("banned_at"),
                }
                for (url, pool), e in self._breakers.items()
                if e["state"] != _CB_CLOSED
            }


# ── Module-level singleton ────────────────────────────────────────────────────
POOL_BREAKER: Optional[PoolCircuitBreaker] = None
