"""
proxy/dedup.py
In-flight request deduplication (coalescing).

When two clients request the same URL at the same time, the second
waits for the first's result and shares it — avoiding double fetching.

Usage:
    from proxy.dedup import dedup_get, dedup_put, dedup_done

    async def handle(url):
        future, is_new = dedup_get(url)
        if not is_new:
            return await future          # wait for the first caller
        try:
            result = await do_fetch(url)
            dedup_put(url, result)
            return result
        except Exception as exc:
            dedup_done(url, exc=exc)
            raise
        finally:
            dedup_done(url)

Only GET-like idempotent requests should be deduplicated.
"""

import asyncio
from typing import Optional

from proxy.logging_setup import log

# url → asyncio.Future
_INFLIGHT: dict[str, asyncio.Future] = {}


def dedup_get(url: str) -> tuple[Optional[asyncio.Future], bool]:
    """Return (future, is_new).

    If is_new=True, this caller is the leader — it must call dedup_put()
    or dedup_done(exc=...) when done.
    If is_new=False, the future resolves to the leader's result.
    """
    loop = asyncio.get_event_loop()
    if url in _INFLIGHT:
        log.debug(f"[dedup] coalesced {url}")
        return _INFLIGHT[url], False
    fut = loop.create_future()
    _INFLIGHT[url] = fut
    return fut, True


def dedup_put(url: str, result) -> None:
    """Resolve the in-flight future with result and clean up."""
    fut = _INFLIGHT.pop(url, None)
    if fut and not fut.done():
        fut.set_result(result)


def dedup_done(url: str, exc: Optional[BaseException] = None) -> None:
    """Resolve the in-flight future with an exception (or remove if already resolved)."""
    fut = _INFLIGHT.pop(url, None)
    if fut and not fut.done():
        if exc:
            fut.set_exception(exc)
        else:
            fut.cancel()


def inflight_count() -> int:
    return len(_INFLIGHT)
