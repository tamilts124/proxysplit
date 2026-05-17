"""
proxy/config_watcher.py
Automatic config-file reload when config.yaml changes on disk.

Uses watchfiles if available (cross-platform, inotify-backed on Linux),
falling back to a simple polling loop every `poll_interval` seconds.

Usage in proxy_server.py:
    from proxy.config_watcher import start_config_watcher, LIVE_KEYS
    asyncio.ensure_future(start_config_watcher(args.config, reload_fn))

reload_fn(new_config) is called with the freshly-loaded config dict whenever
the file changes.  Only keys listed in LIVE_KEYS are merged into the running
config; anything else (host, port, proxies, etc.) is logged as a warning so
the operator knows a restart is required for those changes to take effect.
"""

import asyncio
import os
from typing import Awaitable, Callable, Optional, Union

from proxy.logging_setup import log


ReloadCallback = Callable[[dict], Union[None, Awaitable[None]]]

# Keys that are safe to apply at runtime without restarting the server.
# Anything not in this set requires a restart; the watcher will log a warning
# for each such key found in the reloaded config so the operator is aware.
LIVE_KEYS: frozenset[str] = frozenset({
    "proxy_rps",
    "adaptive_target_ms",
    "adaptive_chunk_min",
    "adaptive_chunk_max",
    "chunk_timeout",
    "connect_timeout",
    "head_timeout",
    "request_timeout",
    "chunk_retries",
    "max_workers",
    "ban_threshold",
    "ban_base_delay",
    "ban_max_delay",
    "check_interval",
    "cache_ttl",
    "cache_max_body_mb",
    "prefetch_ahead",
    "exclude_window",
    "race_first_chunk",
    "weighted_selection",
    "per_proxy_chunk_size",
    "retry_budget",
    "log_level",
    "prewarm_top_n",
    "prewarm_url",
    "warmup_url",
})

# Keys that cannot be hot-applied; require a server restart.
_RESTART_REQUIRED_KEYS: frozenset[str] = frozenset({
    "host", "port", "proxies", "proxy_files",
    "tor", "tor_only", "tor_instances", "tor_refresh_interval",
    "disk_cache_path", "disk_cache_max_size_mb",
    "streaming",         # changes the response path, unsafe mid-flight
    "chunk_proxy",       # changes mode for all new requests — restart cleaner
    "cache_size",        # would require re-initialising the LRU
    "prometheus_push_url", "prometheus_push_interval",
})


async def start_config_watcher(
    config_path: str,
    reload_fn: ReloadCallback,
    poll_interval: float = 5.0,
) -> None:
    """Start an async config-file watcher.

    Tries watchfiles first (pip install watchfiles); if not installed falls back
    to a polling loop.  Either way reload_fn is called only with the subset of
    keys that are safe to hot-apply (LIVE_KEYS).
    """
    try:
        import watchfiles  # noqa: F401
        await _watch_with_watchfiles(config_path, reload_fn)
    except ImportError:
        log.info(
            "config_watcher: 'watchfiles' not installed; "
            f"falling back to polling every {poll_interval:.0f}s"
        )
        await _watch_with_polling(config_path, reload_fn, poll_interval)


async def _watch_with_watchfiles(config_path: str, reload_fn: ReloadCallback) -> None:
    import watchfiles

    log.info(
        f"config_watcher: watching {config_path} "
        "via watchfiles (inotify/kqueue/ReadDirChanges)"
    )
    async for changes in watchfiles.awatch(config_path):
        for _change_type, path in changes:
            if os.path.abspath(path) == os.path.abspath(config_path):
                await _trigger_reload(config_path, reload_fn)
                break


async def _watch_with_polling(
    config_path: str,
    reload_fn: ReloadCallback,
    poll_interval: float,
) -> None:
    last_mtime: Optional[float] = _safe_mtime(config_path)
    log.info(f"config_watcher: polling {config_path} every {poll_interval:.0f}s")
    while True:
        await asyncio.sleep(poll_interval)
        current = _safe_mtime(config_path)
        if current is not None and current != last_mtime:
            last_mtime = current
            await _trigger_reload(config_path, reload_fn)


async def _trigger_reload(config_path: str, reload_fn: ReloadCallback) -> None:
    log.info(f"config_watcher: {config_path} changed \u2014 reloading")
    try:
        from proxy.config_loader import load_config
        new_cfg = load_config(config_path)
    except Exception as exc:
        log.error(f"config_watcher: failed to parse {config_path}: {exc}")
        return

    # Warn about keys that need a restart; only pass live-safe keys to callback.
    restart_needed = [k for k in new_cfg if k in _RESTART_REQUIRED_KEYS]
    if restart_needed:
        log.warning(
            f"config_watcher: the following changed keys require a restart to take effect: "
            + ", ".join(sorted(restart_needed))
        )

    unknown = [k for k in new_cfg if k not in LIVE_KEYS and k not in _RESTART_REQUIRED_KEYS]
    if unknown:
        log.debug(f"config_watcher: unknown keys (ignored): {', '.join(sorted(unknown))}")

    live_cfg = {k: v for k, v in new_cfg.items() if k in LIVE_KEYS}
    if not live_cfg:
        log.info("config_watcher: no live-reloadable keys changed")
        return

    try:
        result = reload_fn(live_cfg)
        if asyncio.iscoroutine(result):
            await result
        log.info(f"config_watcher: applied live keys: {', '.join(sorted(live_cfg))}")
    except Exception as exc:
        log.error(f"config_watcher: reload_fn raised: {exc}")


def _safe_mtime(path: str) -> Optional[float]:
    try:
        return os.path.getmtime(path)
    except OSError:
        return None
