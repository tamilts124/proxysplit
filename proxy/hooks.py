"""
proxy/hooks.py
Lightweight plugin/middleware hook registry.

Drop callables into ON_REQUEST or ON_RESPONSE from any module imported
at startup (e.g. proxy/plugins/my_plugin.py) to intercept every request
without touching core files.

ON_REQUEST hooks receive (url, headers, method) and may return a modified
headers dict or None (meaning "no change").

ON_RESPONSE hooks receive (url, status, headers) and may return a modified
headers dict or None.

Example — inject an auth header for a specific domain:

    # proxy/plugins/auth_inject.py
    from proxy.hooks import on_request

    @on_request
    def inject_auth(url: str, headers: dict, method: str):
        if "example.com" in url:
            return {**headers, "Authorization": "Bearer SECRET"}

Load at startup by importing in proxy_server.py:
    import proxy.plugins.auth_inject  # noqa: F401
"""

from typing import Callable, Optional

ON_REQUEST:  list[Callable] = []
ON_RESPONSE: list[Callable] = []


def on_request(fn: Callable) -> Callable:
    """Decorator: register fn as a request hook."""
    ON_REQUEST.append(fn)
    return fn


def on_response(fn: Callable) -> Callable:
    """Decorator: register fn as a response hook."""
    ON_RESPONSE.append(fn)
    return fn


def run_request_hooks(url: str, headers: dict, method: str) -> dict:
    """Run all ON_REQUEST hooks and return the (possibly modified) headers."""
    for hook in ON_REQUEST:
        try:
            result = hook(url, headers, method)
            if isinstance(result, dict):
                headers = result
        except Exception as exc:
            from proxy.logging_setup import log
            log.warning(f"[hooks] ON_REQUEST hook {hook.__name__!r} raised: {exc}")
    return headers


def run_response_hooks(url: str, status: int, headers: dict) -> dict:
    """Run all ON_RESPONSE hooks and return the (possibly modified) headers."""
    for hook in ON_RESPONSE:
        try:
            result = hook(url, status, headers)
            if isinstance(result, dict):
                headers = result
        except Exception as exc:
            from proxy.logging_setup import log
            log.warning(f"[hooks] ON_RESPONSE hook {hook.__name__!r} raised: {exc}")
    return headers
