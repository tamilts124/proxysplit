"""
proxy/registry.py
ProxyRegistry: thread-safe live proxy pool with score-weighted,
circuit-breaker-aware selection.
Also holds the _PROXY_TAGS dict and named-pool helpers.
"""

import random
import threading
from typing import Optional

from proxy.logging_setup import log
from proxy.stats import STATS
from proxy.rate_limiter import remove_limiter

# { proxy_url: {tag, ...} }
PROXY_TAGS: dict[str, set[str]] = {}

# Module-level singleton (set in main)
REGISTRY: Optional["ProxyRegistry"] = None


def urls_for_pool(pool: Optional[str], all_urls: list[str]) -> list[str]:
    if not pool:
        return all_urls
    return [u for u in all_urls if pool in PROXY_TAGS.get(u, set())]


class ProxyRegistry:
    """Thread-safe live pool with score-weighted, circuit-breaker-aware selection."""

    def __init__(self, initial: list[str], tor_urls: Optional[set[str]] = None):
        self._lock = threading.Lock()
        self._all: list[str] = list(initial)
        self._tor_urls: set[str] = tor_urls or set()

    def all(self) -> list[str]:
        with self._lock:
            return list(self._all)

    def available(self, pool: Optional[str] = None) -> list[str]:
        from proxy.circuit_breaker import BREAKER
        blocked = BREAKER.blocked_urls() if BREAKER else set()
        with self._lock:
            urls = [p for p in self._all if p not in blocked]
        return urls_for_pool(pool, urls)

    def add(self, url: str, tags: Optional[list[str]] = None) -> bool:
        with self._lock:
            if url in self._all:
                return False
            self._all.append(url)
        if tags:
            PROXY_TAGS[url] = set(tags)
        log.info(f"   + Added proxy: {url}  tags={tags or []}")
        return True

    def remove(self, url: str) -> bool:
        with self._lock:
            if url not in self._all:
                return False
            self._all.remove(url)
            self._tor_urls.discard(url)
        PROXY_TAGS.pop(url, None)
        remove_limiter(url)
        log.info(f"   - Removed proxy: {url}")
        return True

    def replace(self, new_list: list[str]):
        with self._lock:
            old = len(self._all)
            self._all = list(new_list)
        log.info(f"   ↺ Pool replaced: {old} → {len(new_list)}")

    def score_choice(self, pool: Optional[str] = None,
                     exclude_set: Optional[set[str]] = None) -> Optional[str]:
        """Weighted by composite score; respects circuit breakers.
        Uses a single scores_snapshot() call to avoid N lock acquisitions."""
        candidates = self.available(pool)
        if not candidates:
            return None
        if exclude_set and len(candidates) > len(exclude_set):
            candidates = [p for p in candidates if p not in exclude_set]
        if not candidates:
            return None
        # One lock acquisition for all scores (fix #1)
        all_scores = STATS.scores_snapshot()
        weights = [max(0.01, all_scores.get(p, 0.5)) for p in candidates]
        total = sum(weights)
        r = random.random() * total
        cumulative = 0.0
        for p, w in zip(candidates, weights):
            cumulative += w
            if r <= cumulative:
                return p
        return candidates[-1]

    def random_choice(self, pool: Optional[str] = None,
                      exclude_set: Optional[set[str]] = None) -> Optional[str]:
        candidates = self.available(pool)
        if not candidates:
            return None
        if exclude_set and len(candidates) > len(exclude_set):
            candidates = [p for p in candidates if p not in exclude_set]
        return random.choice(candidates) if candidates else None

    def __len__(self):
        with self._lock:
            return len(self._all)
