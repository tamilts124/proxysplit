# proxysplit

**Parallel chunk proxy that splits any HTTP download across multiple proxies simultaneously.**

Each request is broken into byte-range chunks, every chunk is fetched through a different proxy in parallel, and the result is streamed directly to your client. Text-based content types (HTML, JSON, JS, CSS, etc.) are detected automatically and routed through a fast single-fetch path instead.

```
Client ──► Proxy Server ──► Chunk 0 → Proxy A ──► Target
                        ──► Chunk 1 → Proxy B ──► Target
                        ──► Chunk 2 → Proxy C ──► Target
                        ──► Chunk 3 → Proxy D ──► Target
                             Stream in order ──► Client
```

---

## Features

- **Parallel chunked fetching** — splits requests by byte range, each chunk on a different proxy
- **True progressive streaming** — each chunk is written to the client socket *as it arrives*, not after all chunks finish
- **Partial-content resume** — honours the client `Range:` header and responds 206; clients can resume interrupted downloads from the byte they left off
- **Weighted proxy selection** — proxies with better success rates and lower latency are chosen more often; single `scores_snapshot()` call keeps the hot path to one lock acquisition regardless of pool size
- **Adaptive chunk sizing** — chunk size auto-adjusts toward a target completion time using an EMA; per-proxy chunk sizes scale to each proxy's relative speed
- **Time-weighted latency scoring** — latency samples decay exponentially (5-minute half-life) so a proxy that was fast three days ago but is now slow is penalised correctly
- **Proxy quarantine (circuit breaker)** — after N consecutive failures a proxy is auto-banned with exponential-backoff duration; reinstated automatically by background health checks
- **Per-pool circuit breakers** — a proxy failing for one named pool (e.g. `cdn`) stays in service for others (e.g. `general`)
- **In-flight request deduplication** — two clients requesting the same URL simultaneously share one fetch instead of double-fetching
- **Two-tier response cache** — L1 in-memory LRU for hot small responses; L2 disk-backed sqlite3 (WAL mode) for large files and cross-restart persistence
- **ETag / Last-Modified cache revalidation** — on a cache hit the server sends `If-None-Match` / `If-Modified-Since` to the origin; a 304 serves the cached copy instantly, a 200 refreshes it
- **Config hot-reload** — `config.yaml` is watched via `watchfiles` (inotify/kqueue) or 5-second polling; live settings update without a restart or HTTP call
- **Request-level timeout budget** — configurable hard ceiling (`request_timeout`) on total request time; returns 504 if exceeded
- **Parallel HEAD probes** — up to `head_retries` proxies are probed concurrently at startup; first success wins, eliminating serial HEAD timeouts on flaky proxies
- **Stale SOCKS session recovery** — connector-level errors automatically drop the cached session so the next attempt rebuilds it rather than reusing a dead connection
- **Plugin/middleware hooks** — `ON_REQUEST` / `ON_RESPONSE` hook lists; drop a file in `proxy/plugins/` to inject auth headers, rewrite URLs, or log externally without touching core code
- **WebSocket proxying** — detects `Upgrade: websocket` and performs a bidirectional relay through the selected proxy
- **CONNECT tunnel support** — HTTPS and raw TCP traffic tunnelled correctly through HTTP and SOCKS proxies
- **Live proxy management** — add, remove, or reload the pool at runtime via `/proxy/add`, `/proxy/remove`, `/proxy/reload`
- **Background health checker** — periodically retests expired-ban proxies and reinstates them automatically
- **Prometheus pull + push** — `/metrics` endpoint for pull-based scraping, plus optional push to a Pushgateway for batch/short-lived environments
- **Tor multi-instance support** — one Tor process per worker, each with its own SOCKS + control port
- **Free proxy integration** — downloads a fresh list from the ProxyScrape JSON API; headless-safe (no TTY prompt in Docker/systemd)
- **Startup health + anonymity check** — dead and transparent (IP-leaking) proxies filtered out before the server starts
- **SOCKS4 / SOCKS5 / HTTP proxy support** with optional username:password auth
- **Chunk-level retry with shared budget** — failed chunks retried with a fresh proxy; total retries per request are bounded
- **Happy Eyeballs for chunk 0** — optional race between two proxies on the first chunk to minimise TTFB
- **POST / PUT / PATCH body forwarding**

---

## Install

```bash
pip install -r requirements.txt
```

**Requirements** (all in `requirements.txt`):

| Package | Purpose |
|---|---|
| `aiohttp>=3.9.0` | Async HTTP client/server |
| `aiohttp-socks>=0.8.4` | SOCKS proxy connector for aiohttp |
| `pyyaml>=6.0` | Config file parsing |
| `requests[socks]>=2.31.0` | Sync fallback for `--use-free-list` path |
| `PySocks>=1.7.1` | SOCKS support |
| `python-socks[asyncio]>=2.4.3` | CONNECT tunnel + WebSocket relay through SOCKS5 |
| `watchfiles>=0.21.0` | *(optional)* inotify/kqueue config hot-reload; falls back to 5s polling if absent |

All other features (sqlite3 disk cache, Prometheus push, deduplication, hooks) use Python standard library only — no extra packages needed.

---

## Quick Start

```bash
# 1. Copy the example config
cp config.example.yaml config.yaml

# 2. Add proxies to config.yaml (or use --use-free-list)

# 3. Start the server
python proxy_server.py
```

Then send requests with the target URL as a query parameter:

```bash
# Download a large file through the chunk proxy
curl "http://localhost:8888/proxy?url=https://example.com/largefile.zip" -o output.zip

# Resume an interrupted download from byte 1048576 onward
curl "http://localhost:8888/proxy?url=https://example.com/largefile.zip" \
     -H "Range: bytes=1048576-" -o output.zip

# Simple page fetch
curl "http://localhost:8888/proxy?url=https://httpbin.org/get"

# Force-bypass cache for a fresh fetch
curl "http://localhost:8888/proxy?url=https://example.com/file.zip&no-cache=1" -o file.zip

# Pin all chunks to one specific proxy
curl "http://localhost:8888/proxy?url=https://example.com/file.zip" \
     -H "X-Proxy-Pin: http://1.2.3.4:8080" -o file.zip

# Route through a named proxy pool
curl "http://localhost:8888/proxy?url=https://example.com/file.zip&pool=cdn" -o file.zip
```

---

## Configuration

`config.yaml` (copy from `config.example.yaml`):

```yaml
host: "0.0.0.0"
port: 8888

chunk_size: 524288       # 512 KB per chunk (adaptive will adjust from here)
max_workers: 10          # max parallel chunk fetches
chunk_retries: 2         # retry attempts per failed chunk

# Timeouts
chunk_timeout: 60        # per-chunk read timeout in seconds
connect_timeout: 10      # TCP connect timeout in seconds
head_timeout: 15         # HEAD request timeout in seconds

streaming: true          # true progressive streaming
weighted_selection: true # better proxies chosen more often

# Adaptive chunk sizing
adaptive_chunk: true
adaptive_chunk_min: 65536       # 64 KB floor
adaptive_chunk_max: 4194304     # 4 MB ceiling
adaptive_target_ms: 800         # target chunk completion time in ms

# Per-proxy chunk sizing (fast proxies get bigger chunks)
per_proxy_chunk_size: true

# Proxy quarantine / circuit breaker
ban_threshold: 5         # consecutive failures before quarantine
ban_base_delay: 60       # initial ban duration in seconds
ban_max_delay: 3600      # max ban duration (1 hour)

# Background retest of banned proxies (0 = off)
check_interval: 300

# In-memory LRU cache (L1)
cache_size: 256           # max entries (0 = disabled)
cache_ttl: 300            # default TTL in seconds
cache_max_body_mb: 64     # bodies larger than this skip L1

# Disk-backed sqlite3 cache (L2) — uncomment to enable
# disk_cache_path: "/var/cache/proxy/disk_cache.db"
# disk_cache_max_size_mb: 4096
# disk_cache_default_ttl: 86400   # 24 h
# disk_cache_max_body_mb: 2048

# Prometheus push gateway — uncomment to enable
# prometheus_push_url:      "http://pushgateway:9091"
# prometheus_push_interval: 15   # seconds
# prometheus_push_job:      "proxy_server"

# Per-request hard deadline (0 = disabled)
request_timeout: 0         # seconds; returns 504 if the full request exceeds this

# Config hot-reload (watchfiles or polling)
config_autoreload: true    # set false to disable

# ETag / Last-Modified cache revalidation
# (no config knob — follows upstream Cache-Control headers automatically)

proxies:
  - "http://1.2.3.4:8080"
  - "socks5://9.10.11.12:1080"
  # Tagged pools — route specific requests via ?pool=cdn
  - url: "http://5.6.7.8:3128"
    tags: ["cdn"]
```

At least **2 proxies** must be live at startup (unless `tor_only: true`).

---

## CLI

```bash
python proxy_server.py
python proxy_server.py --config custom.yaml
python proxy_server.py --proxy-file proxies.txt
python proxy_server.py --proxy-file http.txt --proxy-file socks5.txt
python proxy_server.py --use-free-list
python proxy_server.py --check-interval 300
python proxy_server.py --log-level DEBUG
python proxy_server.py --tor --tor-instances 4 --tor-refresh-interval 300
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--config FILE` | `-c` | `config.yaml` | YAML config file |
| `--proxy-file FILE` | `-f` | — | Text file of proxy URLs. Repeatable. |
| `--use-free-list` | | off | Download proxies from ProxyScrape API |
| `--check-interval N` | | 0 | Background health recheck interval in seconds |
| `--chunk-proxy` | | on | Per-chunk proxy rotation (default) |
| `--no-chunk-proxy` | | — | Single proxy per full request |
| `--log-level` | | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `--log-format` | | `text` | `text` or `json` |
| `--tor` | | off | Spawn Tor instances |
| `--tor-only` | | off | Use only Tor, skip static proxies |
| `--tor-instances N` | | max_workers | Override Tor instance count |
| `--tor-refresh-interval N` | | 0 | Auto-refresh Tor circuits every N seconds |

---

## Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | `200 ok` when proxies available, `503` otherwise |
| `/status` | GET | JSON: uptime, pool size, available/banned counts, config snapshot |
| `/proxy/stats` | GET | Per-proxy counters: success/failure/latency/score/throughput. `?window=5m` or `?window=1h` for a rolling window. `?sort=score\|latency\|failures\|success\|url` to sort. `?filter=blocked\|available` to filter. `?limit=N` to cap output. |
| `/proxy/banned` | GET | Quarantined proxies with reason, strike count, remaining ban time |
| `/proxy/test` | POST | Live-test a proxy URL. Body: `{"url": "http://ip:port", "target": "https://..."}` |
| `/proxy/add` | POST | Add a proxy at runtime. Body: `{"url": "http://ip:port", "tags": ["cdn"]}` |
| `/proxy/remove` | POST | Remove a proxy at runtime. Body: `{"url": "http://ip:port"}` |
| `/proxy/reload` | POST | Re-read config + proxy files, health-check, hot-swap pool |
| `/metrics` | GET | Prometheus text format metrics (pull) |
| `/tor/refresh` | GET/POST | Send NEWNYM to all Tor instances (new circuits) |
| `/tor/status` | GET | Per-Tor-instance alive/port/last-refresh info |

### Response headers

| Header | Example | Meaning |
|---|---|---|
| `X-Request-ID` | `a3f7c912` | Unique ID for this request (appears in logs too) |
| `X-Cache` | `HIT-L1` / `HIT-L2` / `HIT-L1-REVALIDATED` / `REVALIDATED` | Cache tier and revalidation result |
| `X-Proxy-Chunks` | `8` | Number of chunks the file was split into |
| `X-Proxy-Mode` | `chunk-proxy` | `chunk-proxy` or `request-proxy` |
| `X-Race-Chunk0` | `true` | Whether chunk 0 was raced between two proxies |
| `X-Proxy-Trace` | `0:proxy-a,1:proxy-b,...` | Which proxy served each chunk (streaming path only; capped at 4 KB) |
| `Content-Range` | `bytes 0-1048575/10485760` | Present on 206 responses when the client sent a `Range:` header |

### Examples

```bash
# Per-proxy stats sorted by score, top 10 only
curl "http://localhost:8888/proxy/stats?sort=score&limit=10" | python -m json.tool

# Show only blocked proxies
curl "http://localhost:8888/proxy/stats?filter=blocked" | python -m json.tool

# Per-proxy stats with 5-minute rolling window
curl "http://localhost:8888/proxy/stats?window=5m" | python -m json.tool

# Add a proxy with a pool tag
curl -X POST http://localhost:8888/proxy/add \
     -H "Content-Type: application/json" \
     -d '{"url": "socks5://1.2.3.4:1080", "tags": ["cdn"]}'

# Remove a proxy
curl -X POST http://localhost:8888/proxy/remove \
     -H "Content-Type: application/json" \
     -d '{"url": "http://5.6.7.8:8080"}'

# Hot-reload pool from config + proxy files
curl -X POST http://localhost:8888/proxy/reload

# Check bans
curl http://localhost:8888/proxy/banned | python -m json.tool

# Scrape Prometheus metrics
curl http://localhost:8888/metrics
```

---

## Caching (Two-Tier)

### L1 — In-memory LRU

Fast, RAM-bounded cache for small/hot responses. Controlled by `cache_size`, `cache_ttl`, and `cache_max_body_mb` in config. Bodies over the limit skip L1 but can still land in L2. Disabled with `cache_size: 0`.

### L2 — Disk-backed sqlite3

Survives server restarts. Uses WAL mode for safe concurrent reads. Suited for large files (model weights, large archives, media). On a cache hit the body is also promoted back to L1 for subsequent fast access.

Enable by setting `disk_cache_path` in config:

```yaml
disk_cache_path: "/var/cache/proxy/disk_cache.db"
disk_cache_max_size_mb: 4096     # 4 GB cap; LRU eviction when exceeded
disk_cache_default_ttl: 86400    # 24 hours
disk_cache_max_body_mb: 2048     # per-entry cap
```

Cache behaviour is controlled by upstream `Cache-Control` headers (`no-store`, `no-cache`, `private`, `max-age`). Both tiers respect the same rules.

### ETag / Last-Modified revalidation

When a cached entry has an `ETag` or `Last-Modified` header, a cache hit triggers a conditional request to the origin (`If-None-Match` / `If-Modified-Since`) before serving:

- **304 Not Modified** — origin confirms the content is unchanged; the cached copy is served immediately with `X-Cache: HIT-L1-REVALIDATED`.
- **200 OK** — origin returned fresh content; the cache is updated and the new body is returned with `X-Cache: REVALIDATED`.
- **Any error / timeout** — the cached copy is served as a safe fallback (revalidation is best-effort).

This gives you stale-while-still-fresh correctness for assets with short `max-age` but infrequent actual changes, at the cost of one cheap conditional HEAD-equivalent per cache hit.

---

## Proxy Quarantine (Circuit Breaker)

Bad proxies are auto-quarantined after `ban_threshold` consecutive failures:

- **Strike 1** → banned for `ban_base_delay` seconds (default 60s)
- **Strike 2** → banned for `ban_base_delay × 2`
- **Strike N** → banned for `min(ban_base_delay × 2^(N-1), ban_max_delay)`

If `check_interval > 0` the background health checker periodically retests proxies whose ban has elapsed. On success they re-enter the pool; on failure they receive another strike.

```bash
curl http://localhost:8888/proxy/banned
```

```json
{
  "http://1.2.3.4:8080": {
    "reason": "consecutive failures",
    "strike": 2,
    "state": "OPEN",
    "banned_at": 1717612800.0,
    "remaining_s": 87.4,
    "active": true
  }
}
```

### Per-pool circuit breakers

If a proxy is misbehaving for one specific pool but fine for others, use `POOL_BREAKER` from `proxy/pool_breaker.py`. It tracks state per `(proxy_url, pool)` pair independently of the global breaker. This is an opt-in API for advanced use — see the module docstring.

---

## Request Deduplication

When two clients request the same URL at the same time, the second waits for the first result and shares it — avoiding a double fetch. Only GET-like idempotent requests are deduplicated. The API is in `proxy/dedup.py` and is used automatically by the request handler.

---

## Plugin Hooks

Drop a file in `proxy/plugins/` and register callables into `ON_REQUEST` or `ON_RESPONSE` without touching core files:

```python
# proxy/plugins/auth_inject.py
from proxy.hooks import on_request

@on_request
def inject_auth(url: str, headers: dict, method: str) -> dict:
    if "internal.example.com" in url:
        return {**headers, "Authorization": "Bearer MY_SECRET"}
```

Then import it at startup in `proxy_server.py`:

```python
import proxy.plugins.auth_inject  # noqa: F401
```

`ON_REQUEST` hooks receive `(url, headers, method)` and may return a modified headers dict or `None` (no change). `ON_RESPONSE` hooks receive `(url, status, headers)` and work the same way.

---

## WebSocket Proxying

The proxy auto-detects `Upgrade: websocket` in incoming requests and performs a bidirectional relay through the selected proxy — no extra config needed. HTTP and SOCKS proxies are both supported (SOCKS uses `python-socks`). WebSocket connections use the same pool selection and tag routing as regular requests.

---

## Prometheus / Grafana

### Pull (default)

Metrics at `/metrics` in Prometheus text format:

```
proxy_requests_total{proxy="http://1.2.3.4:8080",result="success"} 142
proxy_requests_total{proxy="http://1.2.3.4:8080",result="failure"} 3
proxy_bytes_total{proxy="http://1.2.3.4:8080"} 4831488
proxy_latency_avg_ms{proxy="http://1.2.3.4:8080"} 312.5
proxy_score{proxy="http://1.2.3.4:8080"} 0.8741
proxy_throughput_bps{proxy="http://1.2.3.4:8080"} 2048000
proxy_banned{proxy="http://1.2.3.4:8080"} 0
proxy_server_uptime_seconds 3601
proxy_pool_size 8
proxy_pool_available 7
proxy_pool_blocked 1
proxy_chunk_size_kb 512
proxy_cache_hits 240
proxy_cache_misses 18
proxy_cache_size 47
```

```yaml
# prometheus.yml
scrape_configs:
  - job_name: chunk-proxy
    static_configs:
      - targets: ['localhost:8888']
    metrics_path: /metrics
```

### Push (optional)

For batch jobs or short-lived containers that can't be scraped:

```yaml
# config.yaml
prometheus_push_url:      "http://pushgateway:9091"
prometheus_push_interval: 15    # seconds
prometheus_push_job:      "proxy_server"
```

---

## How It Works

1. **HEAD probe** — up to `head_retries` proxies probed **concurrently**; first success wins (eliminates serial timeout stacking on flaky proxies)
2. **Cache check** — L1 memory → L2 disk; ETag/Last-Modified revalidation on hit; returns immediately on confirmed hit
3. **Partial-content check** — client `Range:` header parsed; if present, only the requested byte window is fetched and a 206 is returned
4. **Exclusion check** — URL extension or `Content-Type` in the exclusion list → single-fetch path
5. **Fallback** — no `Content-Length` or `Accept-Ranges: bytes` → single-fetch path
6. **Split** — file divided into N chunks of `chunk_size` bytes using byte ranges
7. **Proxy assignment** — proxies assigned using score-weighted selection; consecutive chunks avoid the same proxy (`exclude_window`); overflow falls back to the highest-scored available proxy
8. **Per-proxy chunk sizing** — faster proxies receive proportionally larger chunks; `new_assigned` trimmed to match range count
9. **Parallel fetch** — all chunks fetched concurrently, bounded by `max_workers`; optional chunk-0 race for lowest TTFB
10. **Retry budget** — failed chunks retried with a fresh proxy; total retries per request bounded by a shared semaphore; connector errors invalidate the cached session before retry
11. **Request timeout** — optional hard deadline wraps the entire pipeline; returns 504 on expiry
12. **Progressive stream** — each chunk is written to the client socket *as it completes*, without waiting for all chunks; `X-Proxy-Trace` header records which proxy served each chunk
13. **Cache store** — response stored to L1 and L2 for future requests

---

## Package Layout

```
proxy_server.py          Entry point — wires all singletons, starts aiohttp + raw TCP server

proxy/
  handlers/
    request.py           Main /proxy route handler, RequestConfig dataclass, cache logic
    admin.py             All admin endpoints (/health, /status, /proxy/*, /metrics, /tor/*)
    inner.py             Core dispatch: HEAD probe, range split, proxy assign, stream/gather
  fetchers/
    chunk.py             fetch_chunk — byte-range fetcher with adaptive timeouts
    head.py              get_head_info, fetch_whole — HEAD probe and single-fetch fallback
    health.py            filter_live_proxies_async, background_health_checker, warmup
    geo.py               geo_tag_proxies — ip-api.com batch geo-tagging
  streaming/
    progressive.py       _ProgressiveStream — sliding-window prefetch pipeline
    tunnel.py            CONNECT tunnel handler, make_raw_handler (WebSocket-aware)
    websocket.py         WebSocket upgrade detection and bidirectional relay
  stats/
    proxy_stats.py       ProxyStats — per-proxy counters, scoring, Prometheus export
    chunk_sizer.py       AdaptiveChunkSizer — EMA-based chunk size tuning

  cache.py               In-memory LRU response cache (L1)
  disk_cache.py          Disk-backed sqlite3 response cache (L2)
  circuit_breaker.py     Global circuit breaker with HALF_OPEN probe state machine
  pool_breaker.py        Per-pool circuit breakers (opt-in)
  dedup.py               In-flight request deduplication
  hooks.py               ON_REQUEST / ON_RESPONSE plugin hook registry
  prometheus_push.py     Prometheus Pushgateway client (optional)
  session_pool.py        SessionPool, proxy selection, distribute_proxies
  registry.py            ProxyRegistry — thread-safe pool with score-weighted selection
  rate_limiter.py        Per-proxy token-bucket rate limiter
  config_loader.py       YAML loading, proxy file loading, CLI argument parsing
  config_watcher.py      File-change watcher for automatic config hot-reload
  logging_setup.py       Structured text/JSON logging setup
  tor_manager.py         Tor process management (multi-instance, auto-refresh)

  # Compatibility shims (re-export from sub-packages, zero-impact on callers)
  handlers.py  fetchers.py  streaming.py  stats.py
```

---

## Proxy File Format

One URL per line. Blank lines and `#` comments ignored:

```
# HTTP
http://1.2.3.4:8080
http://user:pass@5.6.7.8:3128

# SOCKS
socks4://9.10.11.12:1080
socks5://13.14.15.16:1080
socks5://user:pass@17.18.19.20:1080
```

Supported schemes: `http://`, `https://`, `socks4://`, `socks4a://`, `socks5://`

---

## Headless / Docker / Daemon Usage

The server is fully non-interactive when run without a TTY. The `--use-free-list` prompt is automatically suppressed — the existing list is reused. To force a re-download in headless mode set `free_proxy_redownload: true` in config.

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "proxy_server.py", "--config", "config.yaml"]
```

---

## Changelog

### v7 (current)
- **Partial-content resume** — honours client `Range:` header; responds 206 with `Content-Range`; clients can resume interrupted downloads from the exact byte they left off (streaming and gather paths both support this)
- **ETag / Last-Modified cache revalidation** — `cache.get_validators()` returns stored validators; on a cache hit the handler sends `If-None-Match`/`If-Modified-Since` to the origin; 304 → serve cached copy instantly, 200 → refresh cache; revalidation errors fall back silently to the cached copy
- **Config hot-reload** — new `proxy/config_watcher.py`; uses `watchfiles` (inotify/kqueue/ReadDirChanges) when installed, falls back to 5-second polling otherwise; live settings (`proxy_rps`, `adaptive_target_ms`, timeouts, etc.) applied without a restart; disabled with `config_autoreload: false`
- **Request-level timeout budget** — `request_timeout` config key wraps `handle_inner` in `asyncio.wait_for`; returns 504 on expiry; 0 = disabled (default)
- **Parallel HEAD probes** — `get_head_info` now launches up to `head_retries` concurrent HEAD requests and takes the first success, eliminating serial timeout stacking (worst-case TTFB improvement: `head_retries × head_timeout` → `head_timeout`)
- **`/proxy/stats` sort + filter** — new query params: `?sort=score|latency|failures|success|url`, `?filter=blocked|available`, `?limit=N`
- **`X-Proxy-Trace` response header** — `_ProgressiveStream` emits `0:proxyA,1:proxyB,...` after streaming completes; shows chunk routing without requiring DEBUG logging; capped at 4 KB
- **Single-lock score reads** — `score_choice()` and `distribute_proxies()` call `STATS.scores_snapshot()` (one lock acquisition) instead of N sequential `STATS.score()` calls; meaningful improvement on large pools
- **Time-weighted latency decay** — `ProxyStats` stores `(timestamp, latency_ms)` pairs; `_score()` applies `exp(-age/halflife)` weights (5-minute half-life); old samples decay gracefully rather than dragging scores indefinitely; score memoised in `_score_cache` and cleared on every write
- **Stale SOCKS session invalidation** — `fetch_chunk` catches `aiohttp.ClientConnectorError` and calls `SESSION_POOL.invalidate_sync()` before retry; prevents repeated failures against a dead SOCKS connector
- **`distribute_proxies` fallback picks best proxy** — overflow chunks now go to the highest-scored available proxy rather than `available[0]` (insertion order)
- **`apply_per_proxy_chunk_sizes` alignment fix** — `new_assigned` trimmed to `len(per_proxy_ranges)` before return; previously the extra entries caused wrong proxy assignments in the `_ProgressiveStream` zip loop
- **Retry budget wait clarified** — timeout raised from 5 s → 30 s; error message distinguishes "timed out waiting for a retry slot" from "budget actually exhausted"
- **`_race_task` cancellation fix** — `_race_task` now handles `asyncio.CancelledError` explicitly and cancels + awaits both `t_a` and `t_b` sub-tasks when cancelled from outside, preventing leaked tasks on stream abort
- **`watchfiles` added to `requirements.txt`** as an optional dependency with a fallback note

### v6
- **Refactored package layout** — monolithic files split into focused sub-packages (`handlers/`, `fetchers/`, `stats/`, `streaming/`); compatibility shims keep all existing imports working unchanged
- **`RequestConfig` dataclass** — all per-handler config in one place; replaces 15-variable closure
- **Two-tier cache** — L1 in-memory LRU + L2 disk-backed sqlite3 (WAL); L2 survives restarts, handles large files
- **`cache.init()` wired at startup** — cache was silently disabled in previous versions; now properly initialised from config
- **Per-pool circuit breakers** (`pool_breaker.py`) — proxy failing for one pool stays in service for others
- **In-flight request deduplication** (`dedup.py`) — coalesces concurrent identical requests
- **Plugin/middleware hooks** (`hooks.py`) — `ON_REQUEST`/`ON_RESPONSE` without touching core
- **WebSocket proxying** — `Upgrade: websocket` detected and relayed bidirectionally through HTTP/SOCKS proxies
- **Prometheus push gateway** (`prometheus_push.py`) — push metrics on a configurable interval alongside the existing pull endpoint
- **`_ProgressiveStream` retry budget** — `retry_budget` semaphore and `request_id` threaded into chunk fetch tasks; bounded total retries per request
- **`_adaptive_timeout` score floor** — score clamped to 0.1 before division; prevents explosion for recently-recovered proxies
- **`distribute_proxies` fallback warning** — logs when the exclude window exhausts all candidates
- **`_score()` private helper** — single shared implementation eliminates `_calc_score_locked` duplication
- **HALF_OPEN circuit breaker fix** — single consistent state read after event wait; eliminates stale-False window
- **Headless stdin gate** — `--use-free-list` prompt suppressed when no TTY; configurable via `free_proxy_redownload`
- **Bandwidth-aware scoring** — `throughput()` added to `ProxyStats`; surfaced in `/proxy/stats` and Prometheus metrics
- **`X-Cache` response header** — `HIT-L1` or `HIT-L2` identifies which cache tier served the response

### v5
- **Adaptive chunk sizing** — EMA tracks completion time; adjusts chunk size toward `adaptive_target_ms`
- **Per-proxy chunk sizing** — faster proxies assigned proportionally larger chunks
- **Race first chunk** — optional Happy Eyeballs race on chunk 0 to minimise TTFB
- **Shared retry budget** — semaphore bounds total retries across all chunks in one request
- **Proxy pool tags + named pools** — tag proxies in config; route via `?pool=name`
- **Geo-tagging** — `ip-api.com` batch lookup tags proxies with `country:CC` and `asn:AS*`
- **`X-Proxy-Pin` header** — pin all chunks to one specific proxy per request

### v4
- **Weighted proxy selection** — success-rate weights; poor proxies deprioritised automatically
- **Proxy quarantine** — exponential-backoff auto-ban; configurable threshold/delays
- **Live proxy management** — `/proxy/add`, `/proxy/remove`, `/proxy/reload`
- **Background health recheck** — `check_interval`; auto-reinstates banned proxies
- **True progressive streaming** — `_ProgressiveStream`; writes each chunk immediately on arrival
- **`/proxy/banned`** endpoint — inspect quarantine state
- **`/metrics`** endpoint — Prometheus text format

### v3
- True browser streaming via `StreamResponse`
- Extension + MIME exclusion list
- ProxyScrape JSON API integration

### v2
- Session pool — `ClientSession` reused per proxy URL
- HEAD retries across multiple proxies
- Async startup health + anonymity check
- POST / PUT / PATCH body forwarding
- `/health` and `/status` endpoints
