"""
proxy/circuit_breaker.py
Circuit breaker: CLOSED → OPEN (on threshold failures) → HALF_OPEN → CLOSED.
Exponential backoff per proxy with configurable max delay.

Fix: HALF_OPEN allow_request() now uses a single consistent state read after
the event wait, preventing the window where a proxy that just recovered still
returned False to the thread that woke up from the event wait.
"""

import threading
import time
from typing import Optional

from proxy.logging_setup import log

_CB_CLOSED    = "CLOSED"
_CB_OPEN      = "OPEN"
_CB_HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    def __init__(self, threshold: int = 5, base_delay: float = 60.0, max_delay: float = 3600.0):
        self._lock = threading.Lock()
        self.threshold  = threshold
        self.base_delay = base_delay
        self.max_delay  = max_delay
        self._breakers: dict[str, dict] = {}
        # One threading.Event per URL in HALF_OPEN state so that exactly
        # one probe request passes through while all others wait.
        self._probe_events: dict[str, threading.Event] = {}

    def _entry(self, url: str) -> dict:
        if url not in self._breakers:
            self._breakers[url] = {
                "state": _CB_CLOSED, "strike": 0,
                "open_until": 0.0, "banned_at": None, "reason": "",
            }
        return self._breakers[url]

    def _open(self, url: str, reason: str):
        e = self._entry(url)
        e["strike"] += 1
        delay = min(self.base_delay * (2 ** (e["strike"] - 1)), self.max_delay)
        e["state"]      = _CB_OPEN
        e["open_until"] = time.monotonic() + delay
        e["banned_at"]  = time.time()
        e["reason"]     = reason
        log.warning(
            f"   ⛔ CircuitBreaker OPEN {url} (strike={e['strike']}, delay={delay:.0f}s)")

    def record_failure(self, url: str, consecutive: int, reason: str = "consecutive failures"):
        if consecutive < self.threshold:
            return
        evt = None
        with self._lock:
            e = self._entry(url)
            if e["state"] in (_CB_CLOSED, _CB_HALF_OPEN):
                self._open(url, reason)
                evt = self._probe_events.pop(url, None)
        if evt:
            evt.set()

    def record_success(self, url: str):
        evt = None
        with self._lock:
            e = self._breakers.get(url)
            if e and e["state"] == _CB_HALF_OPEN:
                e["state"]  = _CB_CLOSED
                e["strike"] = 0
                e["reason"] = ""
                evt = self._probe_events.pop(url, None)
                from proxy.stats import STATS
                STATS.reset_consecutive_failures(url)
                log.info(f"   ✓ CircuitBreaker CLOSED {url}")
        if evt:
            evt.set()

    def allow_request(self, url: str) -> bool:
        """
        CLOSED    → always True.
        OPEN      → True only once the delay expires, then transitions to HALF_OPEN.
        HALF_OPEN → exactly ONE probe passes through; all others block on the
                    threading.Event until the probe finishes.  After the event
                    fires we re-read state under the lock so we never return a
                    stale True/False.
        """
        evt_to_wait: Optional[threading.Event] = None

        with self._lock:
            e = self._breakers.get(url)
            if not e or e["state"] == _CB_CLOSED:
                return True

            if e["state"] == _CB_OPEN:
                if time.monotonic() < e["open_until"]:
                    return False
                # Timer expired — become the probe
                e["state"] = _CB_HALF_OPEN
                evt = threading.Event()
                self._probe_events[url] = evt
                log.info(f"   ↗ CircuitBreaker HALF-OPEN {url} (probing)")
                return True

            if e["state"] == _CB_HALF_OPEN:
                evt = self._probe_events.get(url)
                if evt is None:
                    # Probe already finished; state should now be CLOSED or OPEN
                    return e["state"] == _CB_CLOSED
                evt_to_wait = evt
                # Fall through to wait outside the lock

        if evt_to_wait is not None:
            evt_to_wait.wait(timeout=30)

        # Re-read state after the event fires — single authoritative check
        with self._lock:
            e = self._breakers.get(url)
            return (not e) or e["state"] == _CB_CLOSED

    def blocked_urls(self) -> set[str]:
        now = time.monotonic()
        with self._lock:
            return {u for u, e in self._breakers.items()
                    if e["state"] == _CB_OPEN and now < e["open_until"]}

    def snapshot(self) -> dict:
        now = time.monotonic()
        with self._lock:
            return {
                url: {
                    "state":       e["state"],
                    "strike":      e["strike"],
                    "reason":      e["reason"],
                    "banned_at":   e.get("banned_at"),
                    "remaining_s": max(0.0, round(e["open_until"] - now, 1)),
                }
                for url, e in self._breakers.items() if e["state"] != _CB_CLOSED
            }

    def release(self, url: str):
        evt = None
        with self._lock:
            e = self._breakers.get(url)
            if e:
                e["state"]  = _CB_CLOSED
                e["strike"] = 0
            evt = self._probe_events.pop(url, None)
        if evt:
            evt.set()
        from proxy.stats import STATS
        STATS.reset_consecutive_failures(url)
        log.info(f"   ✓ CircuitBreaker manually reset: {url}")

    def expired_open(self) -> list[str]:
        now = time.monotonic()
        with self._lock:
            return [u for u, e in self._breakers.items()
                    if e["state"] == _CB_OPEN and now >= e["open_until"]]

    def state_for(self, url: str) -> str:
        with self._lock:
            return self._breakers.get(url, {}).get("state", _CB_CLOSED)


# ── Module-level singleton — set by proxy_server.main() ──────────────────────
BREAKER: Optional[CircuitBreaker] = None


def compat_quarantine_snapshot() -> dict:
    if not BREAKER:
        return {}
    snap = BREAKER.snapshot()
    out  = {}
    for url, v in snap.items():
        out[url] = {
            "reason":    v["reason"],
            "strike":    v["strike"],
            "state":     v["state"],
            "banned_at": v.get("banned_at"),
            "remaining_s": v["remaining_s"],
            "active":    v["state"] != _CB_CLOSED,
        }
    return out
