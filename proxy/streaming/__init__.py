"""
proxy/streaming/__init__.py
Public re-exports — existing imports stay unchanged:

    from proxy.streaming import _ProgressiveStream, is_excluded, make_raw_handler
"""

import posixpath
from urllib.parse import urlparse

from proxy.logging_setup import log
from proxy.streaming.progressive import _ProgressiveStream
from proxy.streaming.tunnel import make_raw_handler
from proxy.streaming.websocket import handle_websocket_upgrade


def is_excluded(url: str, ct: str, config: dict) -> bool:
    ext = posixpath.splitext(urlparse(url).path.lower())[1]
    if ext and ext in config.get("no_chunk_extensions", []):
        log.info(f"    ext {ext!r} excluded")
        return True
    for mime in config.get("no_chunk_mimetypes", []):
        if ct.lower().startswith(mime.lower()):
            log.info(f"    mime {ct!r} excluded")
            return True
    return False


__all__ = ["_ProgressiveStream", "is_excluded", "make_raw_handler", "handle_websocket_upgrade"]
