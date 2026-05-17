"""
proxy/streaming/progressive.py
_ProgressiveStream: decoupled fetch/write pipeline with sliding-window
prefetch, per-chunk asyncio.Event readiness signals, and optional
Happy-Eyeballs chunk-0 race.

v7 architecture:
  fetch_sem (Semaphore) — bounds simultaneous network I/O.
  ready[i] (Event)     — set when chunk i data is available; write loop
                          awaits it then advances without waiting on others.
  prefetch_gate[i]     — released by write loop after writing chunk i,
                          allowing fetch tasks beyond prefetch_ahead to start.
  retry_budget         — shared Semaphore limits total retries per request.
  _buf_bytes           — asyncio.Lock-guarded counter caps total buffered bytes
                          so a slow client cannot cause unbounded RAM growth.
"""

import asyncio
from typing import Optional

from aiohttp import web

from proxy.logging_setup import log
from proxy.fetchers.chunk import fetch_chunk
from proxy.session_pool import pick_proxy


class _ProgressiveStream:
    def __init__(
        self, url, ranges, assigned, num_chunks, content_length,
        ct_clean, chunk_retries, chunk_proxy, chunk_timeout,
        connect_timeout, pool, weighted, max_workers, prefetch_ahead,
        race_first_chunk: bool = False,
        race_proxy: Optional[str] = None,
        retry_budget: Optional[asyncio.Semaphore] = None,
        request_id: str = "-",
        is_partial: bool = False,
        partial_start: int = 0,
        partial_end: int = 0,
        full_content_length: int = 0,
        max_buffer_bytes: int = 64 * 1024 * 1024,  # 64 MB hard cap on buffered chunks
    ):
        self.url                 = url
        self.ranges              = ranges
        self.assigned            = assigned
        self.num_chunks          = num_chunks
        self.content_length      = content_length
        self.ct_clean            = ct_clean
        self.chunk_retries       = chunk_retries
        self.chunk_proxy         = chunk_proxy
        self.chunk_timeout       = chunk_timeout
        self.connect_timeout     = connect_timeout
        self.pool                = pool
        self.weighted            = weighted
        self.max_workers         = max_workers
        self.prefetch_ahead      = prefetch_ahead
        self.race_first_chunk    = race_first_chunk
        self.race_proxy          = race_proxy
        self.retry_budget        = retry_budget
        self.request_id          = request_id
        self.is_partial          = is_partial
        self.partial_start       = partial_start
        self.partial_end         = partial_end
        self.full_content_length = full_content_length
        self.max_buffer_bytes    = max_buffer_bytes

    async def send(self, request: web.Request) -> web.StreamResponse:
        status_code = 206 if self.is_partial else 200
        resp = web.StreamResponse(status=status_code)
        resp.content_type   = self.ct_clean
        resp.content_length = self.content_length
        resp.headers["Accept-Ranges"]    = "bytes"
        resp.headers["X-Proxy-Chunks"]   = str(self.num_chunks)
        resp.headers["X-Proxy-Mode"]     = "chunk-proxy" if self.chunk_proxy else "request-proxy"
        resp.headers["X-Prefetch-Ahead"] = str(self.prefetch_ahead)
        if self.race_first_chunk:
            resp.headers["X-Race-Chunk0"] = "true"
        if self.is_partial:
            resp.headers["Content-Range"] = (
                f"bytes {self.partial_start}-{self.partial_end}/{self.full_content_length}"
            )
        await resp.prepare(request)

        results: dict[int, tuple | BaseException] = {}
        ready:   list[asyncio.Event] = [asyncio.Event() for _ in range(self.num_chunks)]
        fetch_sem = asyncio.Semaphore(self.max_workers)

        # Memory backpressure: track total bytes sitting in results[] awaiting
        # the write loop.  Fetch tasks wait here if the buffer is full; the
        # write loop decrements the counter after each resp.write(), unblocking
        # the next fetch.  This prevents a slow client from accumulating
        # unbounded in-memory chunk data.
        _buf_bytes     = 0
        _buf_lock      = asyncio.Lock()
        _buf_has_space = asyncio.Event()
        _buf_has_space.set()  # initially empty — space available

        prefetch_gate: list[asyncio.Event] = [asyncio.Event() for _ in range(self.num_chunks)]
        for i in range(min(self.prefetch_ahead, self.num_chunks)):
            prefetch_gate[i].set()

        async def _do_fetch(s, e, i, proxy):
            return await fetch_chunk(
                self.url, s, e, i, proxy,
                self.chunk_retries, self.chunk_proxy,
                self.chunk_timeout, self.connect_timeout,
                self.pool, self.weighted,
                retry_budget=self.retry_budget,
                request_id=self.request_id,
            )

        async def _fetch_task(s, e, i, proxy):
            gate_idx = max(0, i - self.prefetch_ahead)
            await prefetch_gate[gate_idx].wait()
            async with fetch_sem:
                try:
                    res = await _do_fetch(s, e, i, proxy)
                    # Wait for buffer space before storing the result so a slow
                    # client doesn't let in-memory chunk data grow unboundedly.
                    nonlocal _buf_bytes
                    if not isinstance(res, Exception):
                        chunk_sz = len(res[1])
                        while True:
                            async with _buf_lock:
                                if _buf_bytes + chunk_sz <= self.max_buffer_bytes:
                                    _buf_bytes += chunk_sz
                                    break
                                _buf_has_space.clear()
                            await _buf_has_space.wait()
                    results[i] = res
                except Exception as exc:
                    results[i] = exc
                finally:
                    ready[i].set()

        async def _race_task(s, e, i, proxy_a, proxy_b):
            t_a = asyncio.ensure_future(_do_fetch(s, e, i, proxy_a))
            t_b = asyncio.ensure_future(_do_fetch(s, e, i, proxy_b))
            pending = {t_a, t_b}
            winner  = None
            try:
                while pending and winner is None:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        if task.exception() is None:
                            winner = task.result(); break
                        else:
                            log.debug(f"  Race chunk#0 candidate failed: {task.exception()}")
            except asyncio.CancelledError:
                # The outer task list was cancelled (stream aborted).
                # Ensure both sub-tasks are cleaned up before propagating.
                for t in (t_a, t_b):
                    if not t.done():
                        t.cancel()
                        try:
                            await t
                        except Exception:
                            pass
                raise
            finally:
                # Cancel any still-pending sub-task (normal winner path).
                for t in pending:
                    t.cancel()
                    try: await t
                    except Exception: pass
            if winner is not None:
                results[i] = winner
            else:
                try:
                    results[i] = t_a.exception() or RuntimeError("race: both proxies failed")
                except Exception as ex:
                    results[i] = ex
            ready[i].set()

        tasks: list[asyncio.Task] = []
        for s, e, i in self.ranges:
            if i == 0 and self.race_first_chunk and self.race_proxy:
                t = asyncio.ensure_future(
                    _race_task(s, e, i, self.assigned[i], self.race_proxy))
            else:
                t = asyncio.ensure_future(_fetch_task(s, e, i, self.assigned[i]))
            tasks.append(t)

        trace_parts: list[str] = []
        bytes_sent  = 0
        failed_at: Optional[int] = None

        try:
            for i in range(self.num_chunks):
                await ready[i].wait()
                result = results.get(i)
                if isinstance(result, BaseException):
                    failed_at = i
                    log.error(f"Stream failed at chunk #{i}: {result}")
                    break
                idx, data, _status, used_proxy = result
                await resp.write(data)
                bytes_sent += len(data)
                # Release buffer quota so stalled fetch tasks can proceed.
                nonlocal _buf_bytes  # noqa: F821 (defined in enclosing send())
                async with _buf_lock:
                    _buf_bytes = max(0, _buf_bytes - len(data))
                    _buf_has_space.set()
                trace_parts.append(f"{i}:{used_proxy}")
                next_gate = i + 1
                if next_gate < self.num_chunks:
                    prefetch_gate[next_gate].set()
        except Exception as exc:
            failed_at = -1
            log.error(f"Unexpected stream error: {exc}")
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            await resp.write_eof()
        except Exception:
            pass

        # Emit X-Proxy-Trace header so operators can see chunk routing without
        # requiring DEBUG logging (fix #15). Format: "0:proxyA,1:proxyB,..."
        trace_header = ",".join(trace_parts)
        try:
            resp.headers["X-Proxy-Trace"] = trace_header[:4096]  # cap at 4 KB
        except Exception:
            pass  # headers already sent; best-effort only

        if failed_at is None:
            log.info(
                f"    ✓ Streamed {bytes_sent:,}B in {self.num_chunks} chunks"
                f"  race={'on' if self.race_first_chunk else 'off'}"
            )
        else:
            log.warning(
                f"    ✗ Stream aborted after {bytes_sent:,}B (chunk #{failed_at})")
        log.debug(f"    Trace: {', '.join(trace_parts)}")
        return resp
