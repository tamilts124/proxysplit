"""
proxy/fetchers/head.py
get_head_info  — two-stage HEAD / range-probe to discover Content-Length
                 and range-support for a URL.
fetch_whole    — single-request fallback fetcher (no chunking).
"""

import asyncio
from typing import Optional

import aiohttp

from proxy.logging_setup import log
from proxy.session_pool import http_proxy_param, pick_proxy, record_failure, record_success


async def get_head_info(
    url: str, req_headers: dict,
    head_timeout: float, connect_timeout: float,
    pool: Optional[str], weighted: bool,
    head_retries: int = 3,
    request_id: str = "-",
) -> tuple[Optional[int], bool, str]:
    """
    Two-stage probe.
    Stage 1: Concurrent HEAD requests (up to head_retries in parallel) so
             a single slow/flaky proxy doesn't block TTFB for up to
             head_retries × head_timeout seconds (fix #8).
    Stage 2: Range: bytes=0-0 GET fallback for CDNs that block HEAD.
    Returns (content_length, supports_ranges, content_type).
    """
    from proxy.session_pool import SESSION_POOL

    forward = {k: v for k, v in req_headers.items()
               if k.lower() in ("accept", "user-agent", "accept-language")}

    # Pick up to head_retries distinct proxies for parallel probing
    probe_proxies: list[str] = []
    excluded: set[str] = set()
    for _ in range(head_retries):
        p = pick_proxy(pool=pool, exclude_set=excluded if excluded else None, weighted=weighted)
        if not p:
            break
        probe_proxies.append(p)
        excluded.add(p)

    async def _try_head(proxy_url: str) -> Optional[tuple[int, bool, str]]:
        """Returns (content_length, supports_ranges, content_type) or None."""
        try:
            session = SESSION_POOL.get(proxy_url)
            async with session.head(
                url, headers=forward, proxy=http_proxy_param(proxy_url),
                timeout=aiohttp.ClientTimeout(total=head_timeout, connect=connect_timeout),
                allow_redirects=True,
            ) as resp:
                cl = resp.headers.get("Content-Length")
                ar = resp.headers.get("Accept-Ranges", "none").lower()
                ct = resp.headers.get("Content-Type", "application/octet-stream")
                log.info(f"[{request_id}] HEAD {url} → {resp.status} CL={cl} AR={ar} CT={ct}")
                if cl:
                    return int(cl), ar == "bytes", ct
                return None   # HEAD succeeded but no CL — try range probe
        except Exception as exc:
            record_failure(proxy_url)
            log.warning(f"[{request_id}] HEAD via {proxy_url}: {exc}")
            return None

    # Run all probes concurrently; take the first non-None result
    if probe_proxies:
        tasks = [asyncio.ensure_future(_try_head(p)) for p in probe_proxies]
        content_length_result: Optional[tuple[int, bool, str]] = None
        try:
            pending = set(tasks)
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for t in done:
                    try:
                        r = t.result()
                    except Exception:
                        continue
                    if r is not None:
                        content_length_result = r
                        # Cancel remaining probes — we have our answer
                        for rem in pending:
                            rem.cancel()
                        await asyncio.gather(*pending, return_exceptions=True)
                        pending = set()
                        break
        except Exception:
            pass
        if content_length_result is not None:
            return content_length_result

    # Stage 2: minimal GET bytes=0-0 probe
    log.info(f"[{request_id}] HEAD fallback: probing range support via GET bytes=0-0 for {url}")
    probe_proxy = pick_proxy(pool=pool, weighted=weighted)
    if probe_proxy:
        try:
            from proxy.session_pool import SESSION_POOL
            session = SESSION_POOL.get(probe_proxy)
            async with session.get(
                url,
                headers={**forward, "Range": "bytes=0-0"},
                proxy=http_proxy_param(probe_proxy),
                timeout=aiohttp.ClientTimeout(total=head_timeout, connect=connect_timeout),
                allow_redirects=True,
            ) as resp:
                ct = resp.headers.get("Content-Type", "application/octet-stream")
                if resp.status == 206:
                    cr = resp.headers.get("Content-Range", "")   # "bytes 0-0/123456"
                    if "/" in cr:
                        total_str = cr.split("/")[-1].strip()
                        if total_str.isdigit():
                            total = int(total_str)
                            log.info(f"[{request_id}] Range-probe → CL={total} CT={ct} (206 fallback)")
                            return total, True, ct
                elif resp.status == 200:
                    cl = resp.headers.get("Content-Length")
                    log.info(f"[{request_id}] Range-probe → 200 (no range support) CL={cl}")
                    return (int(cl) if cl else None), False, ct
        except Exception as exc:
            log.warning(f"[{request_id}] Range-probe failed via {probe_proxy}: {exc}")

    raise RuntimeError(f"Could not determine Content-Length for {url}")


async def fetch_whole(
    url: str, req_headers: dict, body: Optional[bytes] = None,
    method: str = "GET", request_proxy: Optional[str] = None,
    retries: int = 3, chunk_timeout: float = 120, connect_timeout: float = 10,
    pool: Optional[str] = None, weighted: bool = True,
    request_id: str = "-",
) -> tuple[int, bytes, dict]:
    """Single-request fallback; honours the circuit breaker on every attempt."""
    from proxy.circuit_breaker import BREAKER
    from proxy.session_pool import SESSION_POOL

    forward = {k: v for k, v in req_headers.items()
               if k.lower() in ("accept", "user-agent", "accept-language",
                                "accept-encoding", "content-type")}
    last_exc: Exception = RuntimeError("no attempts")
    current = request_proxy or pick_proxy(pool=pool, weighted=weighted)

    for attempt in range(1, retries + 1):
        if BREAKER and current and not BREAKER.allow_request(current):
            alt = pick_proxy(pool=pool, exclude_set={current}, weighted=weighted)
            if alt:
                current = alt
        log.info(f"[{request_id}] Fallback {method} attempt {attempt} via {current}")
        try:
            session = SESSION_POOL.get(current)
            async with session.request(
                method, url, headers=forward, proxy=http_proxy_param(current),
                data=body,
                timeout=aiohttp.ClientTimeout(total=chunk_timeout, connect=connect_timeout),
            ) as resp:
                data = await resp.read()
                record_success(current, len(data), 0)
                return resp.status, data, dict(resp.headers)
        except Exception as exc:
            last_exc = exc
            record_failure(current)
            log.warning(f"[{request_id}] Fallback attempt {attempt} failed: {exc}")
            if attempt < retries and request_proxy is None:
                nxt = pick_proxy(pool=pool, exclude_set={current}, weighted=weighted)
                if nxt:
                    current = nxt
    raise last_exc
