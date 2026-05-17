"""
proxy/handlers/inner.py
Core request-processing logic extracted from the monolithic _handle_inner.

Responsibilities:
  - Content-info resolution (HEAD/range probe)
  - Exclusion check
  - Range and proxy assignment
  - Per-proxy chunk sizing
  - Retry budget construction
  - Dispatch to _ProgressiveStream or gather-mode
"""

import asyncio
import re
from typing import Optional

from aiohttp import web

from proxy.logging_setup import log
from proxy.stats import STATS
from proxy.fetchers.chunk import fetch_chunk
from proxy.fetchers.head import get_head_info, fetch_whole
from proxy.session_pool import pick_proxy, distribute_proxies
from proxy.streaming import _ProgressiveStream, is_excluded

# Pre-compiled range pattern used for client Range: header parsing.
_RANGE_PAT = re.compile(r"bytes=(\d+)-(\d*)$")


async def resolve_content_info(
    url: str,
    req_headers: dict,
    config: dict,
    head_timeout: float,
    connect_timeout: float,
    pool: Optional[str],
    weighted: bool,
    rid: str,
) -> tuple[Optional[int], bool, str]:
    """Run the HEAD/range probe; return (content_length, supports_ranges, content_type).
    Returns (None, False, 'application/octet-stream') on failure."""
    content_length  = None
    supports_ranges = False
    content_type    = "application/octet-stream"
    try:
        content_length, supports_ranges, content_type = await get_head_info(
            url, req_headers, head_timeout, connect_timeout, pool, weighted,
            request_id=rid,
        )
    except Exception as exc:
        log.warning(f"[{rid}] HEAD/probe failed ({exc}) \u2014 fallback to single fetch")
    return content_length, supports_ranges, content_type


def build_ranges(
    content_length: int,
    chunk_size: int,
    start_offset: int = 0,
    end_offset: Optional[int] = None,
) -> list[tuple[int, int, int]]:
    """Split [start_offset, end_offset] into (start, end, index) tuples."""
    if end_offset is None:
        end_offset = content_length - 1
    ranges: list[tuple[int, int, int]] = []
    start = start_offset
    idx   = 0
    while start <= end_offset:
        end = min(start + chunk_size - 1, end_offset)
        ranges.append((start, end, idx))
        start = end + 1
        idx  += 1
    return ranges


def apply_per_proxy_chunk_sizes(
    ranges: list[tuple[int, int, int]],
    assigned: list[str],
    effective_start: int,
    effective_end: int,
    base_chunk_size: int,
    chunk_size_min: int,
    chunk_size_max: int,
) -> tuple[list[tuple[int, int, int]], list[str]]:
    """Rebalance ranges so fast proxies get bigger chunks.

    Fix #2: new_assigned is trimmed to len(per_proxy_ranges) before returning
    so that the zip() in _ProgressiveStream is never misaligned.
    """
    if len(set(assigned)) <= 1:
        return ranges, assigned

    per_proxy_ranges: list[tuple[int, int, int]] = []
    new_assigned = list(assigned)
    cur = effective_start

    for i, proxy_url in enumerate(assigned):
        if cur > effective_end:
            break
        pcs = STATS.per_proxy_chunk_size(
            proxy_url, base_chunk_size, chunk_size_min, chunk_size_max
        )
        end = min(cur + pcs - 1, effective_end)
        per_proxy_ranges.append((cur, end, i))
        cur = end + 1

    # Fill any remaining bytes with the last proxy.
    while cur <= effective_end:
        end = min(cur + base_chunk_size - 1, effective_end)
        per_proxy_ranges.append((cur, end, len(per_proxy_ranges)))
        new_assigned.append(new_assigned[-1])
        cur = end + 1

    # Trim new_assigned to the actual number of ranges produced.
    # When the early-break fires, new_assigned still has the original length,
    # which would give wrong proxy assignments to trailing chunks in the zip().
    new_assigned = new_assigned[: len(per_proxy_ranges)]

    return per_proxy_ranges, new_assigned


async def handle_inner(
    url: str,
    req_headers: dict,
    body: Optional[bytes],
    method: str,
    chunk_size: int,
    max_workers: int,
    chunk_retries: int,
    chunk_proxy: bool,
    chunk_timeout: float,
    connect_timeout: float,
    head_timeout: float,
    pool: Optional[str],
    pin: Optional[str],
    weighted: bool,
    use_streaming: bool,
    prefetch_ahead: int,
    race_first_chunk: bool,
    exclude_window: int,
    config: dict,
    retry_budget_total: int,
    per_proxy_chunks: bool,
    chunk_size_min: int,
    chunk_size_max: int,
    prewarm_url: str,
    prewarm_top_n: int,
    rid: str,
    client_range: Optional[str] = None,
):
    from proxy.session_pool import SESSION_POOL
    from proxy.registry import REGISTRY

    request_proxy = pin if pin else (
        None if chunk_proxy else pick_proxy(pool=pool, weighted=weighted)
    )

    content_length, supports_ranges, content_type = await resolve_content_info(
        url, req_headers, config, head_timeout, connect_timeout, pool, weighted, rid
    )

    if is_excluded(url, content_type, config):
        status, data, hdrs = await fetch_whole(
            url, req_headers, body, method, request_proxy,
            chunk_timeout=chunk_timeout, connect_timeout=connect_timeout,
            pool=pool, weighted=weighted, request_id=rid,
        )
        ct = hdrs.get("Content-Type", content_type).split(";")[0].strip()
        return web.Response(status=status, body=data, content_type=ct)

    if not content_length or not supports_ranges:
        reason = "no Content-Length" if not content_length else "no Accept-Ranges"
        log.info(f"    [{rid}] Fallback: {reason}")
        status, data, hdrs = await fetch_whole(
            url, req_headers, body, method, request_proxy,
            chunk_timeout=chunk_timeout, connect_timeout=connect_timeout,
            pool=pool, weighted=weighted, request_id=rid,
        )
        ct = hdrs.get("Content-Type", content_type).split(";")[0].strip()
        return web.Response(status=status, body=data, content_type=ct)

    # ── Partial-content resume (fix #9) ───────────────────────────────────────
    # If the client sent a Range: header, honour it by fetching only the
    # requested byte window and responding with 206.  Supports "bytes=N-M"
    # and "bytes=N-" forms; anything else falls through to a full 200 fetch.
    effective_start = 0
    effective_end   = content_length - 1
    is_partial      = False
    if client_range and content_length and supports_ranges:
        m = _RANGE_PAT.match(client_range.strip())
        if m:
            rs     = int(m.group(1))
            re_raw = m.group(2)
            re_    = int(re_raw) if re_raw else content_length - 1
            re_    = min(re_, content_length - 1)
            if rs <= re_ < content_length:
                effective_start = rs
                effective_end   = re_
                is_partial      = True
                log.info(
                    f"    [{rid}] Partial-content resume: "
                    f"{effective_start}-{effective_end}/{content_length}"
                )

    effective_length = effective_end - effective_start + 1

    # Connection pre-warm (best-effort, fire-and-forget).
    if prewarm_url and SESSION_POOL and REGISTRY:
        available = REGISTRY.available(pool)
        if available and prewarm_top_n > 0:
            top = sorted(
                available, key=lambda p: STATS.score(p), reverse=True
            )[:prewarm_top_n]
            asyncio.ensure_future(SESSION_POOL.prewarm(top, prewarm_url))

    # Build ranges and assign proxies.
    ranges = build_ranges(
        content_length, chunk_size,
        start_offset=effective_start,
        end_offset=effective_end,
    )
    assigned = distribute_proxies(
        len(ranges), pool=pool, pin=pin, weighted=weighted,
        exclude_window=exclude_window,
    )
    if not assigned:
        from proxy.registry import REGISTRY
        pool_size = len(REGISTRY.all()) if REGISTRY else 0
        return web.Response(
            status=503,
            content_type="application/json",
            headers={"Retry-After": "5"},
            text=(
                f'{{"error": "no proxies available", '
                f'"pool": {pool!r}, '
                f'"pool_size": {pool_size}}}'
            ),
        )

    if per_proxy_chunks:
        ranges, assigned = apply_per_proxy_chunk_sizes(
            ranges, assigned, effective_start, effective_end,
            chunk_size, chunk_size_min, chunk_size_max,
        )

    num_chunks = len(ranges)
    _mode_tag    = "[pin]" if pin else "[chunk-proxy]" if chunk_proxy else "[request-proxy]"
    _partial_tag = "partial " if is_partial else ""
    log.info(
        f"    [{rid}] {_mode_tag} "
        f"{effective_length:,}B ({_partial_tag}of {content_length:,}B) "
        f"\u2192 {num_chunks} chunks  "
        f"chunk_size={chunk_size // 1024}KB  "
        f"proxies_used={len(set(assigned))}"
    )

    ct_clean = content_type.split(";")[0].strip()

    race_proxy: Optional[str] = None
    if race_first_chunk and num_chunks >= 1:
        race_proxy = pick_proxy(pool=pool, exclude_set={assigned[0]}, weighted=weighted)
        if race_proxy:
            log.info(f"    [{rid}] Chunk-0 race: {assigned[0]} vs {race_proxy}")

    # Retry budget: total retries across all chunks in this request.
    budget = (
        retry_budget_total if retry_budget_total > 0
        else num_chunks * max(chunk_retries - 1, 1)
    )
    retry_budget = asyncio.Semaphore(budget)

    if use_streaming:
        return _ProgressiveStream(
            url=url,
            ranges=ranges,
            assigned=assigned,
            num_chunks=num_chunks,
            content_length=effective_length,
            ct_clean=ct_clean,
            chunk_retries=chunk_retries,
            chunk_proxy=chunk_proxy,
            chunk_timeout=chunk_timeout,
            connect_timeout=connect_timeout,
            pool=pool,
            weighted=weighted,
            max_workers=max_workers,
            prefetch_ahead=prefetch_ahead,
            race_first_chunk=race_first_chunk and race_proxy is not None,
            race_proxy=race_proxy,
            retry_budget=retry_budget,
            request_id=rid,
            is_partial=is_partial,
            partial_start=effective_start,
            partial_end=effective_end,
            full_content_length=content_length,
            max_buffer_bytes=config.get("stream_buffer_mb", 64) * 1024 * 1024,
        )

    # ── Non-streaming: gather all chunks, then assemble ───────────────────────
    sem = asyncio.Semaphore(max_workers)

    async def bounded(s: int, e: int, i: int, p: str):
        async with sem:
            return await fetch_chunk(
                url, s, e, i, p, chunk_retries, chunk_proxy,
                chunk_timeout, connect_timeout, pool, weighted,
                retry_budget=retry_budget, request_id=rid,
            )

    results = await asyncio.gather(
        *[bounded(s, e, i, assigned[i]) for s, e, i in ranges],
        return_exceptions=True,
    )

    chunks: dict[int, bytes] = {}
    for res in results:
        if isinstance(res, Exception):
            return web.Response(status=502, text=f"Chunk failed: {res}")
        cidx, data, _st, _p = res
        chunks[cidx] = data

    for s, e, i in ranges:
        expected = e - s + 1
        actual   = len(chunks.get(i, b""))
        if actual != expected:
            return web.Response(
                status=502,
                text=f"[{rid}] Chunk #{i} size mismatch: expected {expected}, got {actual}",
            )

    assembled = b"".join(chunks[i] for i in range(num_chunks))
    if len(assembled) != effective_length:
        return web.Response(
            status=502,
            text=f"[{rid}] Reassembly error: expected {effective_length}, got {len(assembled)}",
        )
    log.info(f"    [{rid}] Reassembled {len(assembled):,}B from {num_chunks} chunks \u2713")

    status_code  = 206 if is_partial else 200
    resp_headers: dict = {}
    if is_partial:
        resp_headers["Content-Range"] = (
            f"bytes {effective_start}-{effective_end}/{content_length}"
        )
    return web.Response(
        status=status_code, body=assembled,
        content_type=ct_clean, headers=resp_headers,
    )
