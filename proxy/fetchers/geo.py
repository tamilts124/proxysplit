"""
proxy/fetchers/geo.py
geo_tag_proxies — query ip-api.com batch endpoint to tag each proxy with
country and ASN codes.  Uses the free batch API (up to 100 IPs per request,
no key required).  The lookup goes direct (not through the proxy) so it
never skews proxy-health stats.
"""

import asyncio
import json as _j
import urllib.parse
from typing import Optional

import aiohttp

from proxy.logging_setup import log


async def geo_tag_proxies(proxies: list[str], timeout: int = 6) -> None:
    """Tag each proxy URL with 'country:<CC>' and 'asn:<ASN>' in PROXY_TAGS."""
    from proxy.registry import PROXY_TAGS

    if not proxies:
        return

    def _extract_ip(proxy_url: str) -> Optional[str]:
        try:
            return urllib.parse.urlparse(proxy_url).hostname
        except Exception:
            return None

    ip_to_urls: dict[str, list[str]] = {}
    for p in proxies:
        ip = _extract_ip(p)
        if ip:
            ip_to_urls.setdefault(ip, []).append(p)

    unique_ips = list(ip_to_urls.keys())
    log.info(f"Geo-tagging {len(unique_ips)} proxy IPs via ip-api.com batch…")

    async with aiohttp.ClientSession() as sess:
        for i in range(0, len(unique_ips), 100):
            batch   = unique_ips[i:i + 100]
            payload = _j.dumps(
                [{"query": ip, "fields": "query,countryCode,as"} for ip in batch]
            )
            try:
                async with sess.post(
                    "http://ip-api.com/batch",
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as resp:
                    if resp.status != 200:
                        log.warning(f"   ip-api.com batch returned {resp.status}")
                        continue
                    results = await resp.json(content_type=None)
                    for entry in results:
                        ip      = entry.get("query", "")
                        cc      = entry.get("countryCode", "").upper()
                        asn_raw = entry.get("as", "")   # e.g. "AS12345 Some ISP"
                        asn     = asn_raw.split()[0] if asn_raw else ""
                        if not ip or not cc:
                            continue
                        for proxy_url in ip_to_urls.get(ip, []):
                            tags = PROXY_TAGS.setdefault(proxy_url, set())
                            tags.add(f"country:{cc}")
                            if asn:
                                tags.add(f"asn:{asn}")
                        log.debug(f"   Geo {ip} → {cc} {asn}")
            except Exception as exc:
                log.warning(f"   Geo batch failed: {exc}")

    tagged = sum(1 for p in proxies if PROXY_TAGS.get(p))
    log.info(f"   Geo-tagged {tagged}/{len(proxies)} proxies")
