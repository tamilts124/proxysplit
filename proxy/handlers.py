"""
proxy/handlers.py  (compatibility shim)
All logic has moved to proxy/handlers/ sub-package.
Re-exports all make_*_handler functions.
"""

from proxy.handlers import (                # noqa: F401
    make_request_handler,
    make_health_handler,
    make_status_handler,
    make_stats_handler,
    make_banned_handler,
    make_proxy_test_handler,
    make_proxy_add_handler,
    make_proxy_remove_handler,
    make_proxy_reload_handler,
    make_metrics_handler,
    make_tor_refresh_handler,
    make_tor_status_handler,
)

__all__ = [
    "make_request_handler",
    "make_health_handler",
    "make_status_handler",
    "make_stats_handler",
    "make_banned_handler",
    "make_proxy_test_handler",
    "make_proxy_add_handler",
    "make_proxy_remove_handler",
    "make_proxy_reload_handler",
    "make_metrics_handler",
    "make_tor_refresh_handler",
    "make_tor_status_handler",
]
