"""
proxy/handlers/request.py
make_request_handler — the main /proxy route handler.

Owns the RequestConfig dataclass, cache check, and dispatch to handle_inner.
"""

import asyncio
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Optional

from aiohttp import web

from proxy.logging_setup import log
from proxy.cache import CACHE
from proxy.disk_cache import DISK_CACHE
from proxy.stats import STATS
from proxy.fetchers.chunk import CHUNK_SIZER
from proxy.streaming import _ProgressiveStream
from proxy.handlers.inner import handle_inner


def _make_request_id() -> str:
    return uuid.uuid4().hex[:8]


@dataclass
class RequestConfig:
    """All per-handler configuration values in one place."""
    static_chunk_size:  int   = 512 * 1024
    max_workers:        int   = 10
    chunk_retries:      int   = 2
    chunk_proxy:        bool  = True
    chunk_timeout:      float = 60.0
    connect_timeout:    float = 10.0
    head_timeout:       float = 15.0
    weighted:           bool  = True
    use_streaming:      bool  = True
    prefetch_ahead:     int   = 2
    race_first_chunk:   bool  = False
    exclude_window:     int   = 3
    retry_budget_total: int   = 0
    per_proxy_chunks:   bool  = True
    chunk_size_min:     int   = 65536
    chunk_size_max:     int   = 4 * 1024 * 1024
    prewarm_url:        str   = ""
    prewarm_top_n:      int   = 5

    @classmethod
    def from_config(cls, config: dict) -> "RequestConfig":
        return cls(
            static_chunk_size  = config.get("chunk_size", 512 * 1024),
            max_workers        = config.get("max_workers", 10),
            chunk_retries      = config.get("chunk_retries", 2),
            chunk_proxy        = config.get("chunk_proxy", True),
            chunk_timeout      = config.get("chunk_timeout", 60),
            connect_timeout    = config.get("connect_timeout", 10),
            head_timeout       = config.get("head_timeout", 15),
            weighted           = config.get("weighted_selection", True),
            use_streaming      = config.get("streaming", True),
            prefetch_ahead     = config.get("prefetch_ahead", 2),
            race_first_chunk   = config.get("race_first_chunk", False),
            exclude_window     = config.get("exclude_window", 3),
            retry_budget_total = config.get("retry_budget", 0),
            per_proxy_chunks   = config.get("per_proxy_chunk_size", True),
            chunk_size_min     = config.get("adaptive_chunk_min", 65536),
            chunk_size_max     = config.get("adaptive_chunk_max", 4 * 1024 * 1024),
            prewarm_url        = config.get("prewarm_url", config.get("warmup_url", "")),
            prewarm_top_n      = config.get("prewarm_top_n", 5),
        )


def make_request_handler(config: dict):
    rc = RequestConfig.from_config(config)

    async def handle_request(request: web.Request) -> web.StreamResponse | web.Response:
        rid = _make_request_id()
        url = request.rel_url.query.get("url")
        if not url:
            return web.Response(status=400, content_type="text/plain",
                                text="Missing ?url= parameter\n")
        if not url.startswith(("http://", "https://")):
            return web.Response(status=400, text=f"Unsupported scheme: {url}")

        # Cache check (GET only; ?no-cache=1 to bypass)
        # L1: in-memory LRU (fast, bounded RAM)
        # L2: disk-backed sqlite (large files, cross-restart)
        # On a cache hit we also perform ETag/Last-Modified revalidation
        # against the origin (fix #10) so stale entries are refreshed when
        # the content actually changes, rather than relying purely on TTL.
        no_cache = request.rel_url.query.get("no-cache", "0") not in ("0", "")
        if request.method == "GET" and not no_cache:
            cached = CACHE.get(url)
            if cached is not None:
                status, body, hdrs = cached
                ct = hdrs.get("Content-Type", "application/octet-stream").split(";")[0].strip()

                # ── ETag / Last-Modified revalidation (fix #10) ───────────────
                validators = CACHE.get_validators(url)
                cond_headers: dict = {}
                if validators:
                    etag, last_mod = validators
                    if etag:     cond_headers["If-None-Match"]     = etag
                    if last_mod: cond_headers["If-Modified-Since"] = last_mod
                if cond_headers:
                    try:
                        import aiohttp as _aiohttp
                        from proxy.session_pool import SESSION_POOL as _SP, pick_proxy as _pp
                        _prx   = _pp(pool=request.rel_url.query.get("pool"),
                                     weighted=config.get("weighted_selection", True))
                        _sess  = _SP.get(_prx)
                        from proxy.session_pool import http_proxy_param as _hpp
                        async with _sess.get(
                            url, headers=cond_headers,
                            proxy=_hpp(_prx),
                            timeout=_aiohttp.ClientTimeout(
                                total=config.get("head_timeout", 15)),
                            allow_redirects=True,
                        ) as _r:
                            if _r.status == 304:
                                # Content unchanged — serve from cache as-is
                                log.info(f"[{rid}] Cache HIT (L1, 304 revalidated) {url}")
                                return web.Response(
                                    status=status, body=body, content_type=ct,
                                    headers={"X-Cache": "HIT-L1-REVALIDATED",
                                             "X-Request-ID": rid})
                            elif _r.status == 200:
                                # Fresh response — update cache and fall through
                                fresh_body = await _r.read()
                                fresh_hdrs = dict(_r.headers)
                                CACHE.put(url, 200, fresh_body, fresh_hdrs)
                                fresh_ct = fresh_hdrs.get(
                                    "Content-Type", "application/octet-stream"
                                ).split(";")[0].strip()
                                log.info(
                                    f"[{rid}] Cache MISS (revalidated 200 — updated) {url}")
                                return web.Response(
                                    status=200, body=fresh_body, content_type=fresh_ct,
                                    headers={"X-Cache": "REVALIDATED",
                                             "X-Request-ID": rid})
                            # Other status: fall through to cached copy
                    except Exception as _rv_exc:
                        log.debug(
                            f"[{rid}] Revalidation failed ({_rv_exc}) — serving cached copy")

                log.info(f"[{rid}] Cache HIT (L1) {url}")
                return web.Response(status=status, body=body, content_type=ct,
                                    headers={"X-Cache": "HIT-L1", "X-Request-ID": rid})
            # L2 disk cache
            if DISK_CACHE is not None:
                disk_hit = await asyncio.get_event_loop().run_in_executor(
                    None, DISK_CACHE.get, url)
                if disk_hit is not None:
                    status, body, hdrs = disk_hit
                    ct = hdrs.get("Content-Type", "application/octet-stream").split(";")[0].strip()
                    log.info(f"[{rid}] Cache HIT (L2/disk) {url}")
                    # Promote to L1
                    CACHE.put(url, status, body, hdrs)
                    return web.Response(status=status, body=body, content_type=ct,
                                        headers={"X-Cache": "HIT-L2", "X-Request-ID": rid})

        pool = request.rel_url.query.get("pool")
        pin  = request.headers.get("X-Proxy-Pin")
        mode = "chunk-proxy" if rc.chunk_proxy else "request-proxy"
        log.info(f"[{rid}] ─── [{mode}] {request.method} {url} pool={pool} pin={pin}")

        req_headers: dict = dict(request.headers)
        body: Optional[bytes] = None
        if request.method in ("POST", "PUT", "PATCH"):
            body = await request.read()

        # Pass the client's Range header through so partial-content resume works
        # (fix #9).  If present we honour it in handle_inner.
        client_range: Optional[str] = request.headers.get("Range")

        chunk_size = CHUNK_SIZER.size if CHUNK_SIZER else rc.static_chunk_size

        # Per-request hard deadline (fix #12).  0 = disabled.
        request_timeout: float = config.get("request_timeout", 0)

        try:
            inner_coro = handle_inner(
                url, req_headers, body, request.method,
                chunk_size, rc.max_workers, rc.chunk_retries, rc.chunk_proxy,
                rc.chunk_timeout, rc.connect_timeout, rc.head_timeout,
                pool, pin, rc.weighted, rc.use_streaming, rc.prefetch_ahead,
                rc.race_first_chunk, rc.exclude_window, config,
                rc.retry_budget_total, rc.per_proxy_chunks,
                rc.chunk_size_min, rc.chunk_size_max,
                rc.prewarm_url, rc.prewarm_top_n,
                rid, client_range=client_range,
            )
            if request_timeout > 0:
                result = await asyncio.wait_for(inner_coro, timeout=request_timeout)
            else:
                result = await inner_coro
            if isinstance(result, _ProgressiveStream):
                resp = await result.send(request)
                resp.headers["X-Request-ID"] = rid
                return resp
            if request.method == "GET" and isinstance(result, web.Response):
                CACHE.put(url, result.status, result.body or b"", dict(result.headers))
                if DISK_CACHE is not None:
                    body_bytes = result.body or b""
                    hdrs       = dict(result.headers)
                    asyncio.get_event_loop().run_in_executor(
                        None, lambda: DISK_CACHE.put(url, result.status, body_bytes, hdrs))
            if isinstance(result, web.Response):
                result.headers["X-Request-ID"] = rid
            return result
        except asyncio.TimeoutError:
            log.error(f"[{rid}] Request timed out after {request_timeout}s")
            return web.Response(status=504, content_type="text/plain",
                                text=f"Request timeout after {request_timeout}s",
                                headers={"X-Request-ID": rid})
        except Exception:
            log.error(f"[{rid}] Unhandled error:\n{traceback.format_exc()}")
            return web.Response(status=502, content_type="text/plain",
                                text="Proxy error — see server log",
                                headers={"X-Request-ID": rid})

    return handle_request
