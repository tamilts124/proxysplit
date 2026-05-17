"""
proxy/fetchers/health.py
Health-check helpers: liveness probes, filter_live_proxies_async,
background_health_checker, warmup benchmark, validate_proxies.
"""

import asyncio
import time
from typing import Optional

import aiohttp

from proxy.logging_setup import log
from proxy.stats import STATS
from proxy.session_pool import make_connector, http_proxy_param

SUPPORTED_SCHEMES = ("http://", "https://", "socks4://", "socks4a://", "socks5://")


async def get_own_ip(timeout: int = 5) -> Optional[str]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://httpbin.org/ip",
                             timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                return (await r.json()).get("origin")
    except Exception:
        return None


async def check_proxy_async(proxy_url: str, own_ip: Optional[str],
                             timeout: int = 10) -> bool:
    try:
        connector   = make_connector(proxy_url)
        proxy_param = http_proxy_param(proxy_url)
        async with aiohttp.ClientSession(connector=connector) as s:
            async with s.get("https://httpbin.org/ip", proxy=proxy_param,
                             timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status != 200:
                    return False
                data = await r.json()
                seen = data.get("origin", "")
                if own_ip and own_ip in seen:
                    log.info(f"   ~ transparent {proxy_url}")
                    return False
                return True
    except Exception:
        return False


def filter_supported(proxies: list[str]) -> tuple[list[str], int]:
    ok, skip = [], 0
    for p in proxies:
        if any(p.startswith(s) for s in SUPPORTED_SCHEMES):
            ok.append(p)
        else:
            skip += 1
            log.info(f"   - skip {p} (unsupported scheme)")
    return ok, skip


async def filter_live_proxies_async(proxies: list[str]) -> list[str]:
    supported, skipped = filter_supported(proxies)
    log.info(f"Checking {len(supported)} proxies ({skipped} skipped)…")
    own_ip = await get_own_ip()
    log.info(f"   Own IP: {own_ip or 'unknown'}")
    results = await asyncio.gather(
        *[check_proxy_async(p, own_ip) for p in supported], return_exceptions=True)
    live = []
    for p, ok in zip(supported, results):
        if isinstance(ok, Exception):
            ok = False
        log.info(f"   {'✓ alive' if ok else '✗ dead '}  {p}")
        if ok:
            live.append(p)
    log.info(f"   {len(live)}/{len(supported)} alive")
    return live


def validate_proxies(proxies: list[str], tor_only: bool = False):
    if tor_only:
        if not proxies:
            raise SystemExit("\n❌ ERROR: No Tor instances running.\n")
        return
    if len(proxies) < 2:
        raise SystemExit(
            f"\n❌ ERROR: At least 2 proxies required. Loaded: {len(proxies)}\n"
            "   Add via config.yaml, --proxy-file, or --tor\n"
        )


async def warmup_proxy(proxy_url: str, url: str, timeout: float = 20.0):
    t0 = time.monotonic()
    try:
        connector   = make_connector(proxy_url)
        proxy_param = http_proxy_param(proxy_url)
        async with aiohttp.ClientSession(connector=connector) as s:
            async with s.get(url, proxy=proxy_param,
                             timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                data        = await resp.read()
                latency_ms  = (time.monotonic() - t0) * 1000
                STATS.seed_latency(proxy_url, latency_ms, len(data))
                log.info(
                    f"   Warmup {proxy_url}: {latency_ms:.0f}ms "
                    f"{len(data) // 1024}KB score={STATS.score(proxy_url):.3f}"
                )
    except Exception as exc:
        log.warning(f"   Warmup failed {proxy_url}: {exc}")


async def run_warmup(proxies: list[str], url: str):
    log.info(f"Proxy warm-up benchmark using {url} …")
    await asyncio.gather(*[warmup_proxy(p, url) for p in proxies], return_exceptions=True)
    log.info("Warm-up complete")


async def background_health_checker(interval: int):
    """Periodically retests expired-ban proxies; reinstates those that pass."""
    from proxy.circuit_breaker import BREAKER
    from proxy.registry import REGISTRY

    log.info(f"Background health checker: every {interval}s")
    own_ip = await get_own_ip()
    while True:
        await asyncio.sleep(interval)
        if not BREAKER or not REGISTRY:
            continue
        candidates = BREAKER.expired_open()
        if not candidates:
            continue
        log.info(f"Background probe: {len(candidates)} proxy(ies)…")
        for url in candidates:
            ok = await check_proxy_async(url, own_ip, timeout=15)
            if ok:
                BREAKER.release(url)
                if url not in REGISTRY.all():
                    REGISTRY.add(url)
                log.info(f"   ✓ Reinstated: {url}")
            else:
                consec = STATS.get_consecutive_failures(url)
                BREAKER.record_failure(url, max(consec, BREAKER.threshold), "retest failed")
                log.info(f"   ✗ Still dead: {url}")
