"""
proxy/stats.py  (compatibility shim)
All logic has moved to proxy/stats/ sub-package.
This file re-exports the public API so any code that does
    from proxy.stats import STATS, AdaptiveChunkSizer
continues to work without change.
"""

from proxy.stats.chunk_sizer import AdaptiveChunkSizer   # noqa: F401
from proxy.stats.proxy_stats import ProxyStats           # noqa: F401
from proxy.stats import STATS                            # noqa: F401

__all__ = ["STATS", "AdaptiveChunkSizer", "ProxyStats"]
