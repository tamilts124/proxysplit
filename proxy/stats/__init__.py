"""
proxy/stats/__init__.py
Public re-exports so existing code can still do:
    from proxy.stats import STATS, AdaptiveChunkSizer
"""

from proxy.stats.chunk_sizer import AdaptiveChunkSizer
from proxy.stats.proxy_stats import ProxyStats

STATS = ProxyStats()

__all__ = ["STATS", "AdaptiveChunkSizer", "ProxyStats"]
