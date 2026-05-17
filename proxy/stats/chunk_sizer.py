"""
proxy/stats/chunk_sizer.py
AdaptiveChunkSizer — adjusts chunk size to hit a target completion time.
Separated from proxy_stats so fetchers can import only what they need.
"""

import collections
import threading
from typing import Optional


class AdaptiveChunkSizer:
    """
    Tracks recent chunk completion times via EMA and adjusts chunk size to
    hit a target completion time.  Thread-safe via a simple lock.

    Tuning:
      - alpha=0.25 gives moderate smoothing (~4-sample memory)
      - ratio thresholds (1.5 / 0.6) prevent thrashing on a single outlier
      - step multipliers (×1.5 / ×0.6) allow fast convergence while staying stable
    """

    def __init__(self, initial: int, minimum: int, maximum: int, target_ms: float):
        self._lock    = threading.Lock()
        self.current  = initial
        self.minimum  = minimum
        self.maximum  = maximum
        self.target_ms = target_ms
        self._ema_ms: Optional[float] = None
        self._alpha   = 0.25
        self._window: collections.deque = collections.deque(maxlen=10)

    def record(self, elapsed_ms: float):
        from proxy.logging_setup import log
        with self._lock:
            self._window.append(elapsed_ms)
            if self._ema_ms is None:
                self._ema_ms = elapsed_ms
            else:
                self._ema_ms = self._alpha * elapsed_ms + (1 - self._alpha) * self._ema_ms
            self._adjust(log)

    def _adjust(self, log):
        if self._ema_ms is None or self._ema_ms <= 0:
            return
        ratio = self.target_ms / self._ema_ms
        if ratio > 1.5:
            new = min(int(self.current * 1.5), self.maximum)
        elif ratio < 0.6:
            new = max(int(self.current * 0.6), self.minimum)
        else:
            return
        if new != self.current:
            log.info(
                f"AdaptiveChunk: {self.current // 1024}KB → {new // 1024}KB "
                f"(ema={self._ema_ms:.0f}ms, target={self.target_ms:.0f}ms)"
            )
            self.current = new

    @property
    def size(self) -> int:
        with self._lock:
            return self.current

    @property
    def ema_ms(self) -> Optional[float]:
        with self._lock:
            return self._ema_ms
