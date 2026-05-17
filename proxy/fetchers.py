"""
proxy/fetchers.py  (compatibility shim)
All logic has moved to proxy/fetchers/ sub-package.
Re-exports everything so existing callers keep working unchanged.
"""

from proxy.fetchers.chunk  import fetch_chunk                          # noqa: F401
from proxy.fetchers.head   import get_head_info, fetch_whole           # noqa: F401
from proxy.fetchers.health import (                                    # noqa: F401
    filter_live_proxies_async, run_warmup, validate_proxies,
    background_health_checker, get_own_ip,
)
from proxy.fetchers.geo    import geo_tag_proxies                      # noqa: F401
from proxy.stats           import AdaptiveChunkSizer                   # noqa: F401
import proxy.fetchers.chunk as _chunk_mod

# Module-level references read by proxy_server.main()
CHUNK_SIZER  = _chunk_mod.CHUNK_SIZER
SESSION_POOL = _chunk_mod.SESSION_POOL

__all__ = [
    "fetch_chunk", "get_head_info", "fetch_whole",
    "filter_live_proxies_async", "run_warmup", "validate_proxies",
    "background_health_checker", "get_own_ip", "geo_tag_proxies",
    "AdaptiveChunkSizer", "CHUNK_SIZER", "SESSION_POOL",
]
