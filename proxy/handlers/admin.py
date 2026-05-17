"""
proxy/handlers/admin.py
All administrative/utility route handlers:
  /health, /status, /proxy/stats, /proxy/banned, /proxy/test,
  /proxy/add, /proxy/remove, /proxy/reload, /metrics,
  /tor/refresh, /tor/status
"""

import json as _json_mod
import time
from typing import Optional

import aiohttp
from aiohttp import web

from proxy.logging_setup import log, _START_TIME
from proxy.stats import STATS
from proxy.circuit_breaker import BREAKER, compat_quarantine_snapshot, _CB_CLOSED
from proxy.registry import REGISTRY, PROXY_TAGS
from proxy.session_pool import (
    SESSION_POOL, make_connector, http_proxy_param,
    pick_proxy, distribute_proxies,
)
from proxy.fetchers.chunk import CHUNK_SIZER
from proxy.fetchers.head import fetch_whole
from proxy.fetchers.health import filter_live_proxies_async, get_own_ip
from proxy.cache import CACHE


# ── /health ───────────────────────────────────────────────────────────────────

def make_health_handler():
    async def h(req: web.Request) -> web.Response:
        if REGISTRY and REGISTRY.available():
            return web.Response(text="ok", content_type="text/plain")
        return web.Response(status=503, text="no proxies available",
                            content_type="text/plain")
    return h


# ── /status ───────────────────────────────────────────────────────────────────

def make_status_handler(config: dict):
    async def h(req: web.Request) -> web.Response:
        pool_all   = REGISTRY.all() if REGISTRY else []
        pool_avail = REGISTRY.available() if REGISTRY else []
        blocked    = len(BREAKER.blocked_urls()) if BREAKER else 0
        sizer      = ({"current_kb": CHUNK_SIZER.size // 1024,
                       "ema_ms":     CHUNK_SIZER.ema_ms}
                      if CHUNK_SIZER else None)
        from proxy.tor_manager import TOR_MANAGER
        body = {
            "uptime_seconds":     int(time.monotonic() - _START_TIME),
            "proxy_count":        len(pool_all),
            "available_count":    len(pool_avail),
            "blocked_count":      blocked,
            "chunk_proxy":        config.get("chunk_proxy", True),
            "weighted_selection": config.get("weighted_selection", True),
            "adaptive_chunk":     sizer,
            "race_first_chunk":   config.get("race_first_chunk", False),
            "exclude_window":     config.get("exclude_window", 3),
            "proxies":            pool_all,
            "available":          pool_avail,
            "tags":               {u: list(t) for u, t in PROXY_TAGS.items()},
            "chunk_size":         config.get("chunk_size", 512 * 1024),
            "max_workers":        config.get("max_workers", 10),
            "chunk_retries":      config.get("chunk_retries", 2),
            "tor":                TOR_MANAGER.status if TOR_MANAGER else None,
            "cache":              CACHE.stats,
        }
        return web.Response(text=_json_mod.dumps(body, indent=2),
                            content_type="application/json")
    return h


# ── /proxy/stats ──────────────────────────────────────────────────────────────

def make_stats_handler():
    async def h(req: web.Request) -> web.Response:
        q = req.rel_url.query

        # ?window=5m / 1h / 300 (seconds)
        window = q.get("window")
        window_min: Optional[float] = None
        if window:
            try:
                if window.endswith("h"):   window_min = float(window[:-1]) * 60
                elif window.endswith("m"): window_min = float(window[:-1])
                else:                      window_min = float(window)
            except ValueError:
                pass

        blocked = BREAKER.blocked_urls() if BREAKER else set()
        snap = STATS.snapshot(window_minutes=window_min)
        for url, entry in snap.items():
            entry["circuit_state"] = BREAKER.state_for(url) if BREAKER else _CB_CLOSED
            entry["blocked"] = url in blocked

        # ?filter=blocked  — show only blocked proxies
        filter_by = q.get("filter", "").strip().lower()
        if filter_by == "blocked":
            snap = {u: e for u, e in snap.items() if e.get("blocked")}
        elif filter_by == "available":
            snap = {u: e for u, e in snap.items() if not e.get("blocked")}

        # ?sort=score | latency | failures | url
        sort_by = q.get("sort", "").strip().lower()
        _SORT_KEYS = {
            "score":    lambda e: -(e.get("score") or 0),
            "latency":  lambda e:  (e.get("avg_latency_ms") or float("inf")),
            "failures": lambda e: -(e.get("failure") or 0),
            "url":      lambda e:  e,          # sorted by dict key, see below
            "success":  lambda e: -(e.get("success") or 0),
        }
        if sort_by in _SORT_KEYS:
            if sort_by == "url":
                snap = dict(sorted(snap.items(), key=lambda kv: kv[0]))
            else:
                snap = dict(sorted(snap.items(), key=lambda kv: _SORT_KEYS[sort_by](kv[1])))

        # ?limit=N  — return at most N entries (useful for large pools)
        limit_str = q.get("limit", "")
        if limit_str.isdigit():
            limit = int(limit_str)
            snap = dict(list(snap.items())[:limit])

        return web.Response(text=_json_mod.dumps(snap, indent=2),
                            content_type="application/json")
    return h


# ── /proxy/banned ─────────────────────────────────────────────────────────────

def make_banned_handler():
    async def h(req: web.Request) -> web.Response:
        return web.Response(text=_json_mod.dumps(compat_quarantine_snapshot(), indent=2),
                            content_type="application/json")
    return h


# ── /proxy/test ───────────────────────────────────────────────────────────────

def make_proxy_test_handler():
    async def h(req: web.Request) -> web.Response:
        try:
            data = await req.json()
        except Exception:
            return web.Response(status=400, text='{"error":"JSON body required"}',
                                content_type="application/json")
        proxy_url = data.get("url", "").strip()
        target    = data.get("target", "https://httpbin.org/ip").strip()
        if not proxy_url:
            return web.Response(status=400, text='{"error":"url required"}',
                                content_type="application/json")
        own_ip = await get_own_ip()
        t0     = time.monotonic()
        try:
            connector   = make_connector(proxy_url)
            proxy_param = http_proxy_param(proxy_url)
            async with aiohttp.ClientSession(connector=connector) as s:
                async with s.get(target, proxy=proxy_param,
                                 timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    body_j   = await resp.json()
                    lat      = round((time.monotonic() - t0) * 1000, 1)
                    seen_ip  = body_j.get("origin", "")
                    result   = {
                        "alive": True, "latency_ms": lat, "seen_ip": seen_ip,
                        "transparent": bool(own_ip and own_ip in seen_ip),
                        "status": resp.status,
                    }
        except Exception as exc:
            result = {"alive": False, "error": str(exc),
                      "latency_ms": round((time.monotonic() - t0) * 1000, 1)}
        return web.Response(text=_json_mod.dumps(result, indent=2),
                            content_type="application/json")
    return h


# ── /proxy/add ────────────────────────────────────────────────────────────────

def make_proxy_add_handler():
    from proxy.session_pool import SUPPORTED_SCHEMES

    async def h(req: web.Request) -> web.Response:
        try:
            data = await req.json()
        except Exception:
            return web.Response(status=400, text='{"error":"JSON body required"}',
                                content_type="application/json")
        url  = data.get("url", "").strip()
        tags = data.get("tags", [])
        if not url:
            return web.Response(status=400, text='{"error":"url required"}',
                                content_type="application/json")
        if not any(url.startswith(s) for s in SUPPORTED_SCHEMES):
            return web.Response(status=400,
                                text=f'{{"error":"unsupported scheme: {url}"}}',
                                content_type="application/json")
        added = REGISTRY.add(url, tags=tags) if REGISTRY else False
        return web.Response(
            text=_json_mod.dumps({"added": added, "url": url, "tags": tags,
                                  "pool_size": len(REGISTRY) if REGISTRY else 0}),
            content_type="application/json")
    return h


# ── /proxy/remove ─────────────────────────────────────────────────────────────

def make_proxy_remove_handler():
    async def h(req: web.Request) -> web.Response:
        try:
            data = await req.json()
        except Exception:
            return web.Response(status=400, text='{"error":"JSON body required"}',
                                content_type="application/json")
        url = data.get("url", "").strip()
        if not url:
            return web.Response(status=400, text='{"error":"url required"}',
                                content_type="application/json")
        removed = REGISTRY.remove(url) if REGISTRY else False
        if removed and SESSION_POOL:
            await SESSION_POOL.invalidate(url)
        return web.Response(
            text=_json_mod.dumps({"removed": removed, "url": url,
                                  "pool_size": len(REGISTRY) if REGISTRY else 0}),
            content_type="application/json")
    return h


# ── /proxy/reload ─────────────────────────────────────────────────────────────

def make_proxy_reload_handler(args, config: dict):
    async def h(req: web.Request) -> web.Response:
        from proxy.config_loader import load_config, collect_proxies
        log.info("/proxy/reload triggered")
        try:
            fresh = load_config(args.config)
        except Exception as exc:
            return web.Response(status=500, text=_json_mod.dumps({"error": str(exc)}),
                                content_type="application/json")
        raw = collect_proxies(args, fresh)
        from proxy.tor_manager import TOR_MANAGER
        tor_urls: set[str] = set()
        if TOR_MANAGER:
            tor_urls = set(TOR_MANAGER.proxy_urls)
            seen = set(raw)
            for p in tor_urls:
                if p not in seen:
                    raw.append(p); seen.add(p)
        static   = [p for p in raw if p not in tor_urls]
        live     = await filter_live_proxies_async(static)
        new_pool = live + list(tor_urls)
        if REGISTRY:
            REGISTRY.replace(new_pool)
        PROXY_TAGS.clear()
        for entry in fresh.get("proxies", []):
            if isinstance(entry, dict):
                PROXY_TAGS[entry["url"]] = set(entry.get("tags", []))
        return web.Response(
            text=_json_mod.dumps({"reloaded": True, "pool_size": len(new_pool),
                                   "proxies": new_pool}, indent=2),
            content_type="application/json")
    return h


# ── /metrics ──────────────────────────────────────────────────────────────────

def make_metrics_handler():
    async def h(req: web.Request) -> web.Response:
        blocked   = BREAKER.blocked_urls() if BREAKER else set()
        pool_size = len(REGISTRY.all()) if REGISTRY else 0
        avail     = len(REGISTRY.available()) if REGISTRY else 0
        uptime    = int(time.monotonic() - _START_TIME)
        chunk_kb  = CHUNK_SIZER.size // 1024 if CHUNK_SIZER else 0
        cs        = CACHE.stats
        lines     = STATS.prometheus_lines(blocked)
        lines += (
            f"\n# HELP proxy_server_uptime_seconds Uptime\n"
            f"# TYPE proxy_server_uptime_seconds counter\n"
            f"proxy_server_uptime_seconds {uptime}\n"
            f"# HELP proxy_pool_size Total proxies\n"
            f"# TYPE proxy_pool_size gauge\n"
            f"proxy_pool_size {pool_size}\n"
            f"# HELP proxy_pool_available Available proxies\n"
            f"# TYPE proxy_pool_available gauge\n"
            f"proxy_pool_available {avail}\n"
            f"# HELP proxy_pool_blocked Circuit-broken proxies\n"
            f"# TYPE proxy_pool_blocked gauge\n"
            f"proxy_pool_blocked {len(blocked)}\n"
            f"# HELP proxy_chunk_size_kb Current adaptive chunk size in KB\n"
            f"# TYPE proxy_chunk_size_kb gauge\n"
            f"proxy_chunk_size_kb {chunk_kb}\n"
            f"# HELP proxy_cache_hits Total cache hits\n"
            f"# TYPE proxy_cache_hits counter\n"
            f"proxy_cache_hits {cs['hits']}\n"
            f"# HELP proxy_cache_misses Total cache misses\n"
            f"# TYPE proxy_cache_misses counter\n"
            f"proxy_cache_misses {cs['misses']}\n"
            f"# HELP proxy_cache_size Current cached entries\n"
            f"# TYPE proxy_cache_size gauge\n"
            f"proxy_cache_size {cs['size']}\n"
        )
        return web.Response(text=lines, content_type="text/plain; version=0.0.4")
    return h


# ── /tor/* ────────────────────────────────────────────────────────────────────

def make_tor_refresh_handler():
    async def h(req: web.Request) -> web.Response:
        from proxy.tor_manager import TOR_MANAGER
        if not TOR_MANAGER:
            return web.Response(status=404, content_type="application/json",
                                text='{"error":"Tor not enabled"}')
        result = await TOR_MANAGER.refresh_all_circuits()
        ok = sum(1 for v in result.values() if v)
        return web.Response(
            text=_json_mod.dumps({"refreshed": ok, "total": len(result),
                                   "details": {str(k): v for k, v in result.items()}},
                                 indent=2),
            content_type="application/json")
    return h


def make_tor_status_handler():
    async def h(req: web.Request) -> web.Response:
        from proxy.tor_manager import TOR_MANAGER
        if not TOR_MANAGER:
            return web.Response(status=404, content_type="application/json",
                                text='{"error":"Tor not enabled"}')
        return web.Response(text=_json_mod.dumps(TOR_MANAGER.status, indent=2),
                            content_type="application/json")
    return h
