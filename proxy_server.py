#!/usr/bin/env python3
"""
proxy_server.py  — Entry point (v6 refactored)
All logic lives in the proxy/ package.  This file only wires things together.
"""

import asyncio
import signal
import sys

from aiohttp import web

import proxy.circuit_breaker as cb_mod
import proxy.fetchers       as fetchers_mod
import proxy.registry       as reg_mod
import proxy.session_pool   as sp_mod
import proxy.stats          as stats_mod
import proxy.tor_manager    as tor_mod
from proxy.config_loader    import load_config, collect_proxies, parse_args
from proxy.config_watcher   import start_config_watcher
from proxy.cache            import init as init_cache
from proxy.disk_cache       import init as init_disk_cache
from proxy.fetchers         import (
    filter_live_proxies_async, run_warmup, validate_proxies,
    background_health_checker, AdaptiveChunkSizer,
)
from proxy.handlers         import (
    make_request_handler,
    make_health_handler, make_status_handler, make_stats_handler,
    make_banned_handler, make_proxy_test_handler,
    make_proxy_add_handler, make_proxy_remove_handler,
    make_proxy_reload_handler, make_metrics_handler,
    make_tor_refresh_handler, make_tor_status_handler,
)
from proxy.logging_setup    import log, configure as configure_logging, suppress_connection_reset
from proxy.rate_limiter     import set_global_rps
from proxy.registry         import ProxyRegistry, PROXY_TAGS
from proxy.session_pool     import SessionPool
from proxy.streaming        import make_raw_handler
from proxy.tor_manager      import TorInstanceManager
from proxy.circuit_breaker  import CircuitBreaker


def main():
    args   = parse_args()
    config = load_config(args.config)

    # ── Logging ───────────────────────────────────────────────────────────────
    log_fmt = args.log_format or config.get("log_format", "text")
    configure_logging(log_fmt, args.log_level)
    log.info(f"Config: {args.config}  log_format={log_fmt}")

    # ── CLI overrides ─────────────────────────────────────────────────────────
    if args.chunk_proxy is not None:
        config["chunk_proxy"] = args.chunk_proxy
    elif "chunk_proxy" not in config:
        config["chunk_proxy"] = True
    if args.check_interval is not None:
        config["check_interval"] = args.check_interval

    max_workers = config.get("max_workers", 10)
    use_tor     = args.tor or config.get("tor", False)
    tor_only    = args.tor_only or config.get("tor_only", False)
    if args.tor_instances is not None:        config["tor_instances"] = args.tor_instances
    if args.tor_refresh_interval is not None: config["tor_refresh_interval"] = args.tor_refresh_interval
    num_tor = config.get("tor_instances", 0) or max_workers

    # ── Response cache ─────────────────────────────────────────────────────────
    init_cache(
        max_size    = config.get("cache_size", 256),
        default_ttl = config.get("cache_ttl", 300),
        max_body    = config.get("cache_max_body_mb", 64) * 1024 * 1024,
    )

    # ── Disk-backed cache (large bodies, cross-restart) ────────────────────────
    disk_cache_path = config.get("disk_cache_path", "").strip()
    if disk_cache_path:
        init_disk_cache(
            db_path      = disk_cache_path,
            max_size_mb  = config.get("disk_cache_max_size_mb", 4096),
            default_ttl  = config.get("disk_cache_default_ttl", 86400),
            max_body_mb  = config.get("disk_cache_max_body_mb", 2048),
        )
        log.info(f"DiskCache enabled: {disk_cache_path}")

    # ── Rate limiter ──────────────────────────────────────────────────────────
    global_rps = float(config.get("proxy_rps", 0))
    set_global_rps(global_rps)
    if global_rps > 0:
        log.info(f"Per-proxy rate limit: {global_rps} req/s")

    # ── Adaptive chunk sizer ─────────────────────────────────────────────────
    if config.get("adaptive_chunk", True):
        sizer = AdaptiveChunkSizer(
            initial   = config.get("chunk_size", 512 * 1024),
            minimum   = config.get("adaptive_chunk_min", 65536),
            maximum   = config.get("adaptive_chunk_max", 4 * 1024 * 1024),
            target_ms = config.get("adaptive_target_ms", 800),
        )
        fetchers_mod.CHUNK_SIZER = sizer
        log.info(
            f"AdaptiveChunk: initial={sizer.current//1024}KB "
            f"min={sizer.minimum//1024}KB max={sizer.maximum//1024}KB "
            f"target={sizer.target_ms}ms"
        )

    # ── Circuit breaker ───────────────────────────────────────────────────────
    breaker = CircuitBreaker(
        threshold  = config.get("ban_threshold", 5),
        base_delay = config.get("ban_base_delay", 60),
        max_delay  = config.get("ban_max_delay", 3600),
    )
    cb_mod.BREAKER = breaker
    # Propagate to modules that import BREAKER at call time (they use cb_mod.BREAKER)
    from proxy import session_pool as _sp
    _sp.BREAKER = breaker
    from proxy import fetchers as _fe
    _fe.BREAKER = breaker
    log.info(
        f"CircuitBreaker: threshold={breaker.threshold} "
        f"base_delay={breaker.base_delay}s max_delay={breaker.max_delay}s"
    )

    # ── Tor ───────────────────────────────────────────────────────────────────
    if use_tor:
        log.info(f"Tor: launching {num_tor} instance(s)")
        mgr = TorInstanceManager(config, num_tor)
        try:
            mgr.start_all()
        except Exception as exc:
            log.error(f"Tor start failed: {exc}"); sys.exit(1)
        tor_mod.TOR_MANAGER = mgr
    tor_manager = tor_mod.TOR_MANAGER

    # ── Collect + health-check proxies ────────────────────────────────────────
    proxies  = collect_proxies(args, config)
    tor_urls: set[str] = set()
    if tor_manager:
        tor_urls = set(tor_manager.proxy_urls)
        seen = set(proxies)
        for p in tor_urls:
            if p not in seen: proxies.append(p); seen.add(p)

    loop = asyncio.new_event_loop()
    loop.set_exception_handler(suppress_connection_reset)
    asyncio.set_event_loop(loop)

    static_proxies = [p for p in proxies if p not in tor_urls]
    live_static    = loop.run_until_complete(filter_live_proxies_async(static_proxies)) if static_proxies else []
    proxies        = live_static + list(tor_urls)
    validate_proxies(proxies, tor_only=tor_only)

    log.info(f"✓  {len(proxies)} proxies ready")
    for i, p in enumerate(proxies):
        tags    = list(PROXY_TAGS.get(p, set()))
        tor_tag = " [tor]" if p in tor_urls else ""
        log.info(f"   [{i+1}] {p}{tor_tag}  tags={tags}")

    # ── Warm-up ───────────────────────────────────────────────────────────────
    warmup_url = config.get("warmup_url", "")
    if warmup_url:
        loop.run_until_complete(run_warmup(proxies, warmup_url))

    # ── Global singletons ─────────────────────────────────────────────────────
    registry     = ProxyRegistry(proxies, tor_urls=tor_urls)
    session_pool = SessionPool()
    reg_mod.REGISTRY       = registry
    sp_mod.SESSION_POOL    = session_pool
    fetchers_mod.SESSION_POOL = session_pool  # fetchers import SESSION_POOL at call time

    # ── App + routes ──────────────────────────────────────────────────────────
    app     = web.Application()
    handler = make_request_handler(config)
    for m in ("GET", "POST", "PUT", "PATCH", "DELETE"):
        app.router.add_route(m, "/proxy", handler)

    app.router.add_get("/health",        make_health_handler())
    app.router.add_get("/status",        make_status_handler(config))
    app.router.add_get("/proxy/stats",   make_stats_handler())
    app.router.add_get("/proxy/banned",  make_banned_handler())
    app.router.add_post("/proxy/test",   make_proxy_test_handler())
    app.router.add_post("/proxy/add",    make_proxy_add_handler())
    app.router.add_post("/proxy/remove", make_proxy_remove_handler())
    app.router.add_post("/proxy/reload", make_proxy_reload_handler(args, config))
    app.router.add_get("/metrics",       make_metrics_handler())
    app.router.add_get("/tor/refresh",   make_tor_refresh_handler())
    app.router.add_post("/tor/refresh",  make_tor_refresh_handler())
    app.router.add_get("/tor/status",    make_tor_status_handler())

    host          = config.get("host", "0.0.0.0")
    port          = config.get("port", 8888)
    check_iv      = config.get("check_interval", 0)
    drain_timeout = config.get("drain_timeout", 30)

    log.info(f"🚀 {host}:{port}")
    log.info(f"   chunk_proxy={config['chunk_proxy']}  weighted={config.get('weighted_selection', True)}")
    log.info(f"   adaptive_chunk={'on' if fetchers_mod.CHUNK_SIZER else 'off'}  prefetch_ahead={config.get('prefetch_ahead', 2)}")
    log.info(f"   race_first_chunk={config.get('race_first_chunk', False)}  exclude_window={config.get('exclude_window', 3)}")
    log.info(f"   proxy_rps={global_rps or 'unlimited'}  drain_timeout={drain_timeout}s")
    log.info(f"   check_interval={check_iv}s")
    if tor_manager:
        log.info(f"   tor={tor_manager.num_instances} instances  refresh={tor_manager.refresh_iv}s")

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    _shutting_down = False

    def _sigterm(sig, frame):
        nonlocal _shutting_down
        log.info(f"Signal {sig} — draining in-flight requests (timeout={drain_timeout}s)…")
        _shutting_down = True
        loop.call_soon_threadsafe(_do_drain)

    def _do_drain():
        async def _drain_coro():
            loop.stop()
        asyncio.ensure_future(_drain_coro())

    signal.signal(signal.SIGINT,  _sigterm)
    signal.signal(signal.SIGTERM, _sigterm)

    class _FilteredAccessLogger(web.AccessLogger):
        def log(self, request, response, time):
            if request.method in ("UNKNOWN", "CONNECT"): return
            super().log(request, response, time)

    async def _start():
        if tor_manager:  await tor_manager.start_auto_refresh()
        if check_iv > 0: asyncio.ensure_future(background_health_checker(check_iv))

        from proxy.prometheus_push import maybe_start_pusher
        maybe_start_pusher(config)

        # Config hot-reload watcher (fix #14).
        # When config.yaml changes on disk, live settings that don't require a
        # restart (rate-limit, chunk sizes, timeouts, etc.) are updated in-place.
        if config.get("config_autoreload", True):
            def _apply_config_update(new_cfg: dict):
                # Update mutable runtime settings without restarting.
                # Pool changes still require /proxy/reload or a restart.
                if "proxy_rps" in new_cfg:
                    set_global_rps(float(new_cfg["proxy_rps"]))
                if fetchers_mod.CHUNK_SIZER and "adaptive_target_ms" in new_cfg:
                    fetchers_mod.CHUNK_SIZER.target_ms = float(new_cfg["adaptive_target_ms"])
                # Merge updated keys into the live config dict so all handlers
                # using config.get(...) see the new values immediately.
                config.update(new_cfg)
                log.info("config_watcher: live config updated")

            asyncio.ensure_future(
                start_config_watcher(args.config, _apply_config_update)
            )
            log.info(f"config_watcher: auto-reload enabled for {args.config}")
        else:
            log.info("config_watcher: disabled (set config_autoreload: true to enable)")

        runner = web.AppRunner(app, access_log_class=_FilteredAccessLogger)
        await runner.setup()
        internal_site = web.TCPSite(runner, "127.0.0.1", 0)
        await internal_site.start()
        int_port = internal_site._server.sockets[0].getsockname()[1]
        log.info(f"   Internal aiohttp on 127.0.0.1:{int_port}")

        raw_server = await asyncio.start_server(make_raw_handler(int_port), host, port)
        async with raw_server:
            try:
                await loop.create_future()
            except asyncio.CancelledError:
                pass

        if tor_manager: tor_manager.cancel_auto_refresh()
        await runner.cleanup()
        await session_pool.close_all()

    try:
        loop.run_until_complete(_start())
    except RuntimeError:
        pass
    finally:
        if tor_manager: tor_manager.stop_all()


if __name__ == "__main__":
    main()
