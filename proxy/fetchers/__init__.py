"""
proxy/fetchers/__init__.py
Public re-exports so existing callers keep working unchanged:

    from proxy.fetchers import fetch_chunk, get_head_info, fetch_whole, ...
    from proxy.fetchers import AdaptiveChunkSizer, CHUNK_SIZER, SESSION_POOL
"""

from proxy.fetchers.chunk   import fetch_chunk, CHUNK_SIZER, SESSION_POOL
from proxy.fetchers.head    import get_head_info, fetch_whole
from proxy.fetchers.health  import (
    filter_live_proxies_async, run_warmup, validate_proxies,
    background_health_checker, get_own_ip,
)
from proxy.fetchers.geo     import geo_tag_proxies
from proxy.stats            import AdaptiveChunkSizer

import proxy.fetchers.chunk as _chunk_mod


def _set_chunk_sizer(sizer):
    """Called by proxy_server.main() after constructing the AdaptiveChunkSizer."""
    _chunk_mod.CHUNK_SIZER = sizer


def _set_session_pool(pool):
    """Called by proxy_server.main() after constructing the SessionPool."""
    _chunk_mod.SESSION_POOL = pool


__all__ = [
    "fetch_chunk",
    "get_head_info",
    "fetch_whole",
    "filter_live_proxies_async",
    "run_warmup",
    "validate_proxies",
    "background_health_checker",
    "get_own_ip",
    "geo_tag_proxies",
    "AdaptiveChunkSizer",
    "CHUNK_SIZER",
    "SESSION_POOL",
]
