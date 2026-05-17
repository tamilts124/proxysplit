"""
proxy/stats/proxy_stats.py
Per-proxy counters, latency tracking, composite scoring, and Prometheus export.

score() blends success-rate, avg latency, and p95 latency so proxies that
are usually fast but occasionally stall are penalised appropriately.
"""

import collections
import math
import threading
import time
from typing import Optional


class ProxyStats:
    """
    Thread-safe per-proxy counters.

    score() = success_rate / (1 + blend_latency_s)
    blend   = 0.6 * avg_ms + 0.4 * p95_ms  (falls back to avg when < 5 samples)

    Using p95 penalises proxies that are usually fast but occasionally stall,
    which would otherwise hold up the write loop on their slow chunks.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._data: dict[str, dict] = {}

    # ── internal helpers ──────────────────────────────────────────────────────

    # Half-life for time-weighted latency decay (seconds).  Samples older
    # than ~3 half-lives contribute < 12 % of their original weight.
    _LATENCY_HALFLIFE_S: float = 300.0  # 5 minutes

    def _entry(self, url: str) -> dict:
        if url not in self._data:
            self._data[url] = {
                "success": 0,
                "failure": 0,
                "bytes_transferred": 0,
                "consecutive_failures": 0,
                # Each element is (timestamp_s, latency_ms)
                "latency_ms": collections.deque(maxlen=100),
                "events":     collections.deque(maxlen=1000),
                # Cached score; invalidated on every write
                "_score_cache": None,
            }
        return self._data[url]

    def _decay_weight(self, ts: float, now: float) -> float:
        """Exponential decay weight for a sample recorded at *ts*."""
        age_s = max(0.0, now - ts)
        return math.exp(-age_s * math.log(2) / self._LATENCY_HALFLIFE_S)

    def _score(self, e: dict) -> float:
        """Score calculation that works on a pre-fetched entry dict.
        Caller must hold the lock (or pass a snapshot).
        Result is memoised in e['_score_cache'] and cleared on every write."""
        cached = e.get("_score_cache")
        if cached is not None:
            return cached

        total = e["success"] + e["failure"]
        sr    = e["success"] / total if total else 0.5

        samples = list(e["latency_ms"])   # list of (ts, lat_ms)
        if not samples:
            result = sr / 2.0
            e["_score_cache"] = result
            return result

        now = time.time()
        weights = [self._decay_weight(ts, now) for ts, _ in samples]
        lats_ms = [lat for _, lat in samples]
        total_w = sum(weights)

        if total_w <= 0:
            avg_ms   = sum(lats_ms) / len(lats_ms)
            blend_ms = avg_ms
        else:
            avg_ms = sum(w * l for w, l in zip(weights, lats_ms)) / total_w
            if len(samples) >= 5:
                # Weighted p95: sort by latency, accumulate weight until 95 %
                pairs = sorted(zip(lats_ms, weights), key=lambda x: x[0])
                threshold = 0.95 * total_w
                cumulative = 0.0
                p95_ms = pairs[-1][0]
                for lat, w in pairs:
                    cumulative += w
                    if cumulative >= threshold:
                        p95_ms = lat
                        break
                blend_ms = 0.6 * avg_ms + 0.4 * p95_ms
            else:
                blend_ms = avg_ms

        result = sr / (1.0 + blend_ms / 1000.0)
        e["_score_cache"] = result
        return result

    # ── write API ─────────────────────────────────────────────────────────────

    def _invalidate_score(self, e: dict):
        """Called under the lock after any write that affects score."""
        e["_score_cache"] = None

    def record_success(self, url: str, nbytes: int, latency_ms: float):
        with self._lock:
            e = self._entry(url)
            e["success"] += 1
            e["bytes_transferred"] += nbytes
            now = time.time()
            e["latency_ms"].append((now, latency_ms))
            e["consecutive_failures"] = 0
            e["events"].append((now, True, latency_ms))
            self._invalidate_score(e)

    def record_failure(self, url: str) -> int:
        with self._lock:
            e = self._entry(url)
            e["failure"] += 1
            e["consecutive_failures"] += 1
            e["events"].append((time.time(), False, 0.0))
            self._invalidate_score(e)
            return e["consecutive_failures"]

    def seed_latency(self, url: str, latency_ms: float, nbytes: int):
        with self._lock:
            e = self._entry(url)
            now = time.time()
            e["latency_ms"].append((now, latency_ms))
            e["bytes_transferred"] += nbytes
            e["success"] += 1
            e["events"].append((now, True, latency_ms))
            self._invalidate_score(e)

    def reset_consecutive_failures(self, url: str):
        with self._lock:
            self._entry(url)["consecutive_failures"] = 0

    # ── read API ──────────────────────────────────────────────────────────────

    def get_consecutive_failures(self, url: str) -> int:
        with self._lock:
            return self._entry(url)["consecutive_failures"]

    def score(self, url: str) -> float:
        with self._lock:
            e = self._data.get(url)
            if not e:
                return 0.5
            return self._score(e)

    def success_rate(self, url: str) -> float:
        with self._lock:
            e = self._data.get(url)
            if not e:
                return 0.5
            total = e["success"] + e["failure"]
            return e["success"] / total if total else 0.5

    def throughput(self, url: str) -> float:
        """Bytes per second estimate based on cumulative stats."""
        with self._lock:
            e = self._data.get(url)
            if not e:
                return 0.0
            lats = [lat for _, lat in e["latency_ms"]]
            if not lats:
                return 0.0
            total_s = sum(lats) / 1000.0
            return e["bytes_transferred"] / (total_s + 1e-9)

    def per_proxy_chunk_size(
        self,
        url: str,
        base_size: int,
        minimum: int,
        maximum: int,
    ) -> int:
        """Return a chunk size scaled to this proxy's relative score.

        Faster proxies (higher score) get proportionally larger chunks so they
        stay busy; slower proxies get smaller chunks so they finish sooner and
        don't stall the write loop.  The result is clamped to [minimum, maximum].

        Scaling uses the ratio of this proxy's score to the average score across
        all tracked proxies.  When only one proxy exists or scores are identical,
        every proxy gets base_size.
        """
        with self._lock:
            all_scores = [self._score(e) for e in self._data.values()]
            my_score   = self._score(self._entry(url))
        if not all_scores:
            return base_size
        avg_score = sum(all_scores) / len(all_scores)
        if avg_score <= 0:
            return base_size
        ratio  = my_score / avg_score
        scaled = int(base_size * ratio)
        return max(minimum, min(maximum, scaled))

    def _windowed(self, e: dict, since: float) -> dict:
        evts  = [(ts, ok, lat) for ts, ok, lat in e["events"] if ts >= since]
        succ  = sum(1 for _, ok, _ in evts if ok)
        fail  = sum(1 for _, ok, _ in evts if not ok)
        lats  = [lat for _, ok, lat in evts if ok and lat > 0]
        total = succ + fail
        return {
            "success":        succ,
            "failure":        fail,
            "success_rate":   round(succ / total, 3) if total else None,
            "avg_latency_ms": round(sum(lats) / len(lats), 1) if lats else None,
            "events_in_window": len(evts),
        }

    def scores_snapshot(self) -> dict[str, float]:
        """Return {url: score} for all tracked proxies under a single lock.
        Used by score_choice() to avoid N sequential lock acquisitions."""
        with self._lock:
            return {url: self._score(e) for url, e in self._data.items()}

    def snapshot(self, window_minutes: Optional[float] = None) -> dict:
        since = time.time() - window_minutes * 60 if window_minutes else 0.0
        with self._lock:
            out = {}
            for url, d in self._data.items():
                raw_lats = list(d["latency_ms"])  # list of (ts, lat_ms)
                lats  = [lat for _, lat in raw_lats]
                total = d["success"] + d["failure"]
                base  = {
                    "success":              d["success"],
                    "failure":              d["failure"],
                    "success_rate":         round(d["success"] / total, 3) if total else None,
                    "consecutive_failures": d["consecutive_failures"],
                    "bytes_transferred":    d["bytes_transferred"],
                    "avg_latency_ms":       round(sum(lats) / len(lats), 1) if lats else None,
                    "p95_latency_ms": (
                        round(sorted(lats)[int(len(lats) * 0.95)], 1)
                        if len(lats) >= 5 else None
                    ),
                    "score":       round(self._score(d), 4),
                    "throughput_bps": round(
                        d["bytes_transferred"] / (sum(lats) / 1000.0 + 1e-9), 0
                    ) if lats else 0,
                }
                if window_minutes:
                    base["window"] = self._windowed(d, since)
                out[url] = base
            return out

    def prometheus_lines(self, banned_urls: set) -> str:
        lines = [
            "# HELP proxy_requests_total Total requests per proxy and result",
            "# TYPE proxy_requests_total counter",
            "# HELP proxy_bytes_total Bytes transferred per proxy",
            "# TYPE proxy_bytes_total counter",
            "# HELP proxy_latency_avg_ms Average latency per proxy in ms",
            "# TYPE proxy_latency_avg_ms gauge",
            "# HELP proxy_score Composite proxy score (0-1)",
            "# TYPE proxy_score gauge",
            "# HELP proxy_throughput_bps Estimated throughput in bytes/s",
            "# TYPE proxy_throughput_bps gauge",
            "# HELP proxy_banned Whether the proxy is currently banned",
            "# TYPE proxy_banned gauge",
        ]
        with self._lock:
            for url, d in self._data.items():
                lbl  = f'proxy="{url}"'
                lats = [lat for _, lat in d["latency_ms"]]
                avg  = sum(lats) / len(lats) if lats else 0
                tput = d["bytes_transferred"] / (sum(lats) / 1000.0 + 1e-9) if lats else 0
                lines += [
                    f'proxy_requests_total{{{lbl},result="success"}} {d["success"]}',
                    f'proxy_requests_total{{{lbl},result="failure"}} {d["failure"]}',
                    f'proxy_bytes_total{{{lbl}}} {d["bytes_transferred"]}',
                    f'proxy_latency_avg_ms{{{lbl}}} {round(avg, 1)}',
                    f'proxy_score{{{lbl}}} {round(self._score(d), 4)}',
                    f'proxy_throughput_bps{{{lbl}}} {round(tput, 0)}',
                    f'proxy_banned{{{lbl}}} {1 if url in banned_urls else 0}',
                ]
        return "\n".join(lines) + "\n"
