"""
proxy/handlers/__init__.py
Public re-exports — existing imports in proxy_server.py stay unchanged.
"""

from proxy.handlers.request import make_request_handler
from proxy.handlers.admin import (
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
