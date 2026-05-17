"""
proxy/streaming.py  (compatibility shim)
All logic has moved to proxy/streaming/ sub-package.
Re-exports _ProgressiveStream, is_excluded, make_raw_handler.
"""

from proxy.streaming import _ProgressiveStream, is_excluded, make_raw_handler  # noqa: F401

__all__ = ["_ProgressiveStream", "is_excluded", "make_raw_handler"]
