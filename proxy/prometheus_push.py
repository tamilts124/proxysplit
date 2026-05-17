"""
proxy/prometheus_push.py
Optional Prometheus Pushgateway client.

Pushes the /metrics payload to a configured Pushgateway endpoint on a
fixed interval, enabling monitoring in environments that cannot scrape
the pull endpoint (batch jobs, short-lived containers, etc.).

Configuration (config.yaml)
---------------------------
prometheus_push_url:      "http://pushgateway:9091"   # required to enable
prometheus_push_interval: 15                          # seconds, default 15
prometheus_push_job:      "proxy_server"              # job label, default "proxy_server"

The push uses the Prometheus text format — no external library needed.

Usage (called from proxy_server.main)
-------------------------------------
    from proxy.prometheus_push import maybe_start_pusher
    maybe_start_pusher(config)
"""

import asyncio
from typing import Optional

from proxy.logging_setup import log


async def _push_loop(url: str, interval: int, job: str):
    """Periodically POST metrics to the Pushgateway."""
    import aiohttp
    from proxy.stats import STATS
    from proxy.circuit_breaker import BREAKER
    from proxy.registry import REGISTRY

    push_url = f"{url.rstrip('/')}/metrics/job/{job}"
    log.info(f"Prometheus push: → {push_url} every {interval}s")

    while True:
        await asyncio.sleep(interval)
        try:
            blocked   = BREAKER.blocked_urls() if BREAKER else set()
            pool_size = len(REGISTRY.all()) if REGISTRY else 0
            avail     = len(REGISTRY.available()) if REGISTRY else 0
            lines     = STATS.prometheus_lines(blocked)
            lines    += (
                f"proxy_pool_size {pool_size}\n"
                f"proxy_pool_available {avail}\n"
                f"proxy_pool_blocked {len(blocked)}\n"
            )
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    push_url,
                    data=lines.encode(),
                    headers={"Content-Type": "text/plain; version=0.0.4"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status not in (200, 202, 204):
                        log.warning(f"Prometheus push returned {resp.status}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(f"Prometheus push failed: {exc}")


def maybe_start_pusher(config: dict) -> Optional[asyncio.Task]:
    """
    Start the push loop if prometheus_push_url is configured.
    Returns the asyncio.Task or None.
    Call this after the event loop is running (inside an async context or
    via asyncio.ensure_future inside _start()).
    """
    push_url = config.get("prometheus_push_url", "").strip()
    if not push_url:
        return None
    interval = int(config.get("prometheus_push_interval", 15))
    job      = config.get("prometheus_push_job", "proxy_server")
    return asyncio.ensure_future(_push_loop(push_url, interval, job))
