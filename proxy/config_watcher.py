"""
proxy/config_watcher.py
Automatic config-file reload when config.yaml changes on disk (fix #14).

Uses watchfiles if available (cross-platform, inotify-backed on Linux),
falling back to a simple polling loop every `poll_interval` seconds.

Usage in proxy_server.py:
    from proxy.config_watcher import start_config_watcher
    asyncio.ensure_future(start_config_watcher(args.config, reload_fn))

reload_fn(new_config) is called with the freshly-loaded config dict whenever
the file changes.  Errors in loading or applying the config are logged and
suppressed so the watcher never crashes the server.
"""

import asyncio
import os
import time
from typing import Awaitable, Callable, Optional, Union

from proxy.logging_setup import log


ReloadCallback = Callable[[dict], Union[None, Awaitable[None]]]


async def start_config_watcher(
    config_path: str,
    reload_fn: ReloadCallback,
    poll_interval: float = 5.0,
) -> None:
    """Start an async config-file watcher.

    Tries watchfiles first (pip install watchfiles); if not installed falls back
    to a polling loop.  Either way reload_fn is called with the new config dict
    whenever the file's mtime changes.
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

    log.info(f"config_watcher: watching {config_path} via watchfiles (inotify/kqueue/ReadDirChanges)")
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
    try:
        result = reload_fn(new_cfg)
        if asyncio.iscoroutine(result):
            await result
        log.info("config_watcher: reload applied successfully")
    except Exception as exc:
        log.error(f"config_watcher: reload_fn raised: {exc}")


def _safe_mtime(path: str) -> Optional[float]:
    try:
        return os.path.getmtime(path)
    except OSError:
        return None
